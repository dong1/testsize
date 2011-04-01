/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */

/*
 * catalog_class.c - catalog class
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "storage_common.h"
#include "error_manager.h"
#include "system_catalog.h"
#include "heap_file.h"
#include "btree.h"
#include "oid.h"
#include "object_representation.h"
#include "object_representation_sr.h"
#include "transform.h"
#include "set_object.h"
#include "locator_sr.h"
#include "memory_hash.h"
#include "system_parameter.h"
#include "class_object.h"
#include "critical_section.h"
#include "xserver_interface.h"
#include "memory_alloc.h"
#include "language_support.h"
#include "numeric_opfunc.h"
#include "string_opfunc.h"
#include "dbtype.h"
#include "db_date.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define IS_SUBSET(value)        (value).sub.count >= 0

#define EXCHANGE_OR_VALUE(a,b) \
  do { \
    OR_VALUE t; \
    t = a; \
    a = b; \
    b = t; \
  } while (0)

#define CATCLS_INDEX_NAME "i__db_class_class_name"
#define CATCLS_INDEX_KEY   9

#define CATCLS_OID_TABLE_SIZE   1024

typedef struct or_value OR_VALUE;
typedef struct catcls_entry CATCLS_ENTRY;
typedef struct catcls_property CATCLS_PROPERTY;
typedef int (*CREADER) (THREAD_ENTRY * thread_p, OR_BUF * buf,
			OR_VALUE * value_p);

struct or_value
{
  union or_id
  {
    OID classoid;
    ATTR_ID attrid;
  } id;
  DB_VALUE value;
  struct or_sub
  {
    struct or_value *value;
    int count;
  } sub;
};

struct catcls_entry
{
  OID class_oid;
  OID oid;
  CATCLS_ENTRY *next;
};

struct catcls_property
{
  const char *name;
  DB_SEQ *seq;
  int size;
  int is_unique;
  int is_reverse;
  int is_primary_key;
  int is_foreign_key;
};

/* TODO: add to ct_class.h */
bool catcls_Enable = false;

static BTID catcls_Btid;
static CATCLS_ENTRY *catcls_Free_entry_list = NULL;
static MHT_TABLE *catcls_Class_oid_to_oid_hash_table = NULL;

/* TODO: move to ct_class.h */
extern int catcls_compile_catalog_classes (THREAD_ENTRY * thread_p);
extern int catcls_insert_catalog_classes (THREAD_ENTRY * thread_p,
					  RECDES * record);
extern int catcls_delete_catalog_classes (THREAD_ENTRY * thread_p,
					  const char *name, OID * class_oid);
extern int catcls_update_catalog_classes (THREAD_ENTRY * thread_p,
					  const char *name, RECDES * record);
extern int catcls_finalize_class_oid_to_oid_hash_table (void);
extern int catcls_remove_entry (OID * class_oid);

static int catcls_initialize_class_oid_to_oid_hash_table (int num_entry);
static int catcls_get_or_value_from_class (THREAD_ENTRY * thread_p,
					   OR_BUF * buf_p,
					   OR_VALUE * value_p);
static int catcls_get_or_value_from_attribute (THREAD_ENTRY * thread_p,
					       OR_BUF * buf_p,
					       OR_VALUE * value_p);
static int ct_get_or_value_from_attrid (THREAD_ENTRY * thread_p,
					OR_BUF * buf_p, OR_VALUE * value_p);
static int catcls_get_or_value_from_domain (THREAD_ENTRY * thread_p,
					    OR_BUF * buf_p,
					    OR_VALUE * value_p);
static int catcls_get_or_value_from_method (THREAD_ENTRY * thread_p,
					    OR_BUF * buf_p,
					    OR_VALUE * value_p);
static int catcls_get_or_value_from_method_signiture (THREAD_ENTRY * thread_p,
						      OR_BUF * buf_p,
						      OR_VALUE * value_p);
static int catcls_get_or_value_from_method_argument (THREAD_ENTRY * thread_p,
						     OR_BUF * buf_p,
						     OR_VALUE * value_p);
static int catcls_get_or_value_from_method_file (THREAD_ENTRY * thread_p,
						 OR_BUF * buf_p,
						 OR_VALUE * value_p);
static int catcls_get_or_value_from_resolution (THREAD_ENTRY * thread_p,
						OR_BUF * buf_p,
						OR_VALUE * value_p);
static int catcls_get_or_value_from_query_spec (THREAD_ENTRY * thread_p,
						OR_BUF * buf_p,
						OR_VALUE * value_p);

static int catcls_get_or_value_from_indexes (DB_SEQ * seq,
					     OR_VALUE * subset,
					     int is_unique,
					     int is_reverse,
					     int is_primary_key,
					     int is_foreign_key,
					     int is_global,
					     const char *node_name);
static int catcls_get_subset (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
			      int expected_size, OR_VALUE * value_p,
			      CREADER reader);
static int catcls_get_object_set (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				  int expected_size, OR_VALUE * value);
static int catcls_get_property_set (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				    int expected_size, OR_VALUE * value_p,
				    int is_global, const char *node_name);
static int catcls_reorder_attributes_by_repr (THREAD_ENTRY * thread_p,
					      OR_VALUE * value_p);
static int catcls_expand_or_value_by_repr (OR_VALUE * value_p,
					   OID * class_oid, DISK_REPR * rep);
static void catcls_expand_or_value_by_subset (THREAD_ENTRY * thread_p,
					      OR_VALUE * value_p);

static int catcls_get_or_value_from_buffer (THREAD_ENTRY * thread_p,
					    OR_BUF * buf_p,
					    OR_VALUE * value_p,
					    DISK_REPR * rep);
static int catcls_put_or_value_into_buffer (OR_VALUE * value_p, int chn,
					    OR_BUF * buf_p, OID * class_oid,
					    DISK_REPR * rep);

static OR_VALUE *catcls_get_or_value_from_class_record (THREAD_ENTRY *
							thread_p,
							RECDES * record);
static OR_VALUE *catcls_get_or_value_from_record (THREAD_ENTRY * thread_p,
						  RECDES * record);
static int catcls_put_or_value_into_record (THREAD_ENTRY * thread_p,
					    OR_VALUE * value_p, int chn,
					    RECDES * record, OID * class_oid);

static int catcls_insert_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
				 OID * root_oid);
static int catcls_delete_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p);
static int catcls_update_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
				 OR_VALUE * old_value, int *uflag);

static int catcls_insert_instance (THREAD_ENTRY * thread_p,
				   OR_VALUE * value_p, OID * oid,
				   OID * root_oid, OID * class_oid,
				   HFID * hfid, HEAP_SCANCACHE * scan);
static int catcls_delete_instance (THREAD_ENTRY * thread_p, OID * oid,
				   OID * class_oid, HFID * hfid,
				   HEAP_SCANCACHE * scan);
static int catcls_update_instance (THREAD_ENTRY * thread_p,
				   OR_VALUE * value_p, OID * oid,
				   OID * class_oid, HFID * hfid,
				   HEAP_SCANCACHE * scan);
static OID *catcls_find_oid (OID * class_oid);
static int catcls_put_entry (CATCLS_ENTRY * entry);
static char *catcls_unpack_allocator (int size);
static OR_VALUE *catcls_allocate_or_value (int size);
static void catcls_free_sub_value (OR_VALUE * values, int count);
static void catcls_free_or_value (OR_VALUE * value);
static int catcls_expand_or_value_by_def (OR_VALUE * value_p, CT_CLASS * def);
static int catcls_guess_record_length (OR_VALUE * value_p);
static int catcls_find_class_oid_by_class_name (THREAD_ENTRY * thread_p,
						const char *name,
						OID * class_oid);
static int catcls_find_btid_of_class_name (THREAD_ENTRY * thread_p,
					   BTID * btid);
static int catcls_find_oid_by_class_name (THREAD_ENTRY * thread_p,
					  const char *name, OID * oid);
static int catcls_convert_class_oid_to_oid (THREAD_ENTRY * thread_p,
					    DB_VALUE * oid_val);
static int catcls_convert_attr_id_to_name (THREAD_ENTRY * thread_p,
					   OR_BUF * orbuf_p,
					   OR_VALUE * value_p);
static void catcls_apply_component_type (OR_VALUE * value_p, int type);
static int catcls_resolution_space (int name_space);
static void catcls_apply_resolutions (OR_VALUE * value_p,
				      OR_VALUE * resolution_p);

#define VALCNV_TOO_BIG_TO_MATTER   1024

typedef struct valcnv_buffer VALCNV_BUFFER;
struct valcnv_buffer
{
  size_t length;
  unsigned char *bytes;
};

static int valcnv_Max_set_elements = 10;

static int valcnv_convert_value_to_string (DB_VALUE * value);
static VALCNV_BUFFER *valcnv_append_bytes (VALCNV_BUFFER * old_string,
					   const char *new_tail,
					   const size_t new_tail_length);
static VALCNV_BUFFER *valcnv_append_string (VALCNV_BUFFER * old_string,
					    const char *new_tail);
static VALCNV_BUFFER *valcnv_convert_float_to_string (VALCNV_BUFFER * buf,
						      const float value);
static VALCNV_BUFFER *valcnv_convert_double_to_string (VALCNV_BUFFER * buf,
						       const double value);
static VALCNV_BUFFER *valcnv_convert_bit_to_string (VALCNV_BUFFER * buf,
						    const DB_VALUE * value);
static VALCNV_BUFFER *valcnv_convert_set_to_string (VALCNV_BUFFER * buf,
						    DB_SET * set);
static VALCNV_BUFFER *valcnv_convert_money_to_string (const double value);
static VALCNV_BUFFER *valcnv_convert_data_to_string (VALCNV_BUFFER * buf,
						     const DB_VALUE * value);
static VALCNV_BUFFER *valcnv_convert_db_value_to_string (VALCNV_BUFFER * buf,
							 const DB_VALUE *
							 value);

/*
 * ct_alloc_entry () -
 *   return:
 *   void(in):
 */
static CATCLS_ENTRY *
catcls_allocate_entry (void)
{
  CATCLS_ENTRY *entry_p;
  if (catcls_Free_entry_list != NULL)
    {
      entry_p = catcls_Free_entry_list;
      catcls_Free_entry_list = catcls_Free_entry_list->next;
    }
  else
    {
      entry_p = (CATCLS_ENTRY *) malloc (sizeof (CATCLS_ENTRY));
      if (entry_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (CATCLS_ENTRY));
	  return NULL;
	}
    }

  entry_p->next = NULL;
  return entry_p;
}

/*
 * ct_free_entry () -
 *   return:
 *   key(in):
 *   data(in):
 *   args(in):
 */
static int
catcls_free_entry (const void *key, void *data, void *args)
{
  CATCLS_ENTRY *entry_p = (CATCLS_ENTRY *) data;
  entry_p->next = catcls_Free_entry_list;
  catcls_Free_entry_list = entry_p;

  return NO_ERROR;
}

/*
 * ct_init_class_oid_to_oid_ht () -
 *   return:
 *   num_entry(in):
 */
static int
catcls_initialize_class_oid_to_oid_hash_table (int num_entry)
{
  catcls_Class_oid_to_oid_hash_table =
    mht_create ("Class OID to OID", num_entry, oid_hash, oid_compare_equals);

  if (catcls_Class_oid_to_oid_hash_table == NULL)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * catcls_finalize_class_oid_to_oid_hash_table () -
 *   return:
 *   void(in):
 */
int
catcls_finalize_class_oid_to_oid_hash_table (void)
{
  CATCLS_ENTRY *entry_p, *next_p;

  if (catcls_Class_oid_to_oid_hash_table)
    {
      mht_map (catcls_Class_oid_to_oid_hash_table, catcls_free_entry, NULL);
      mht_destroy (catcls_Class_oid_to_oid_hash_table);
    }

  for (entry_p = catcls_Free_entry_list; entry_p; entry_p = next_p)
    {
      next_p = entry_p->next;
      free_and_init (entry_p);
    }
  catcls_Free_entry_list = NULL;

  catcls_Class_oid_to_oid_hash_table = NULL;
  return NO_ERROR;
}

/*
 * ct_find_oid () -
 *   return:
 *   class_oid(in):
 */
static OID *
catcls_find_oid (OID * class_oid_p)
{
  CATCLS_ENTRY *entry_p;

  if (catcls_Class_oid_to_oid_hash_table)
    {
      entry_p =
	(CATCLS_ENTRY *) mht_get (catcls_Class_oid_to_oid_hash_table,
				  (void *) class_oid_p);
      if (entry_p != NULL)
	{
	  return &entry_p->oid;
	}
      else
	{
	  return NULL;
	}
    }

  return NULL;
}

/*
 * ct_put_entry () -
 *   return:
 *   entry(in):
 */
static int
catcls_put_entry (CATCLS_ENTRY * entry_p)
{
  if (catcls_Class_oid_to_oid_hash_table)
    {
      if (mht_put
	  (catcls_Class_oid_to_oid_hash_table, &entry_p->class_oid,
	   entry_p) == NULL)
	{
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

/*
 * catcls_remove_entry () -
 *   return:
 *   class_oid(in):
 */
int
catcls_remove_entry (OID * class_oid_p)
{
  if (catcls_Class_oid_to_oid_hash_table)
    {
      mht_rem (catcls_Class_oid_to_oid_hash_table, class_oid_p,
	       catcls_free_entry, NULL);
    }

  return NO_ERROR;
}

/*
 * ct_unpack_allocator () -
 *   return:
 *   size(in):
 */
static char *
catcls_unpack_allocator (int size)
{
  return ((char *) malloc (size));
}

/*
 * ct_alloc_or_value () -
 *   return:
 *   size(in):
 */
static OR_VALUE *
catcls_allocate_or_value (int size)
{
  OR_VALUE *value_p;
  int msize, i;

  msize = size * sizeof (OR_VALUE);
  value_p = (OR_VALUE *) malloc (msize);
  if (value_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      msize);
    }
  else
    {
      for (i = 0; i < size; i++)
	{
	  db_value_put_null (&value_p[i].value);
	  value_p[i].sub.value = NULL;
	  value_p[i].sub.count = -1;
	}
    }

  return (value_p);
}

/*
 * cr_free_sub_value () -
 *   return:
 *   values(in):
 *   count(in):
 */
static void
catcls_free_sub_value (OR_VALUE * values, int count)
{
  int i;

  if (values != NULL)
    {
      for (i = 0; i < count; i++)
	{
	  pr_clear_value (&values[i].value);
	  catcls_free_sub_value (values[i].sub.value, values[i].sub.count);
	}
      free_and_init (values);
    }
}

/*
 * ct_free_or_value () -
 *   return:
 *   value(in):
 */
static void
catcls_free_or_value (OR_VALUE * value_p)
{
  if (value_p != NULL)
    {
      pr_clear_value (&value_p->value);
      catcls_free_sub_value (value_p->sub.value, value_p->sub.count);
      free_and_init (value_p);
    }
}

/*
 * ct_expand_or_value_by_def () -
 *   return:
 *   value(in):
 *   def(in):
 */
static int
catcls_expand_or_value_by_def (OR_VALUE * value_p, CT_CLASS * def_p)
{
  OR_VALUE *attrs_p;
  int n_attrs;
  CT_ATTR *att_def_p;
  int i;
  int error;

  if (value_p != NULL)
    {
      /* index_of */
      COPY_OID (&value_p->id.classoid, &def_p->classoid);

      n_attrs = def_p->n_atts;
      attrs_p = catcls_allocate_or_value (n_attrs);
      if (attrs_p == NULL)
	{
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      value_p->sub.value = attrs_p;
      value_p->sub.count = n_attrs;

      att_def_p = def_p->atts;
      for (i = 0; i < n_attrs; i++)
	{
	  attrs_p[i].id.attrid = att_def_p[i].id;
	  error = db_value_domain_init (&attrs_p[i].value, att_def_p[i].type,
					DB_DEFAULT_PRECISION,
					DB_DEFAULT_SCALE);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
    }

  return NO_ERROR;
}

/*
 * ct_guess_record_length () -
 *   return:
 *   value(in):
 */
static int
catcls_guess_record_length (OR_VALUE * value_p)
{
  int length;
  DB_TYPE data_type;
  PR_TYPE *map_p;
  OR_VALUE *attrs_p;
  int n_attrs, i;

  attrs_p = value_p->sub.value;
  n_attrs = value_p->sub.count;

  length = OR_HEADER_SIZE + OR_VAR_TABLE_SIZE (n_attrs)
    + OR_BOUND_BIT_BYTES (n_attrs);

  for (i = 0; i < n_attrs; i++)
    {

      data_type = DB_VALUE_DOMAIN_TYPE (&attrs_p[i].value);
      map_p = tp_Type_id_map[data_type];

      if (map_p->lengthval != NULL)
	{
	  length += (*(map_p->lengthval)) (&attrs_p[i].value, 1);
	}
      else if (map_p->disksize)
	{
	  length += map_p->disksize;
	}
    }

  return (length);
}

/*
 * ct_find_classoid_by_classname () -
 *   return:
 *   name(in):
 *   class_oid(in):
 */
static int
catcls_find_class_oid_by_class_name (THREAD_ENTRY * thread_p,
				     const char *name_p, OID * class_oid_p)
{
  LC_FIND_CLASSNAME status;

  status = xlocator_find_class_oid (thread_p, name_p, class_oid_p, NULL_LOCK);

  if (status == LC_CLASSNAME_ERROR)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_LC_UNKNOWN_CLASSNAME, 1, name_p);
      return ER_FAILED;
    }
  else if (status == LC_CLASSNAME_DELETED)
    {
      /* not found the class */
      OID_SET_NULL (class_oid_p);
    }

  return NO_ERROR;
}

/*
 * ct_find_btid_of_classname () -
 *   return:
 *   btid(in):
 */
static int
catcls_find_btid_of_class_name (THREAD_ENTRY * thread_p, BTID * btid_p)
{
  DISK_REPR *repr_p = NULL;
  DISK_ATTR *att_repr_p;
  REPR_ID repr_id;
  OID *index_class_p;
  ATTR_ID index_key;
  int error = NO_ERROR;

  index_class_p = &ct_Class.classoid;
  index_key = (ct_Class.atts)[CATCLS_INDEX_KEY].id;

  error =
    catalog_get_last_representation_id (thread_p, index_class_p, &repr_id);
  if (error != NO_ERROR)
    {
      goto error;
    }
  else
    {
      repr_p = catalog_get_representation (thread_p, index_class_p, repr_id);
      if (repr_p == NULL)
	{
	  error = er_errid ();
	  goto error;
	}
    }

  for (att_repr_p = repr_p->variable; att_repr_p->id != index_key;
       att_repr_p++)
    {
      ;
    }

  if (att_repr_p->bt_stats == NULL)
    {
      error = ER_SM_NO_INDEX;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, CATCLS_INDEX_NAME);
      goto error;

    }
  else
    {
      BTID_COPY (btid_p, &(att_repr_p->bt_stats->btid));
    }

  catalog_free_representation (repr_p);
  return NO_ERROR;

error:

  if (repr_p)
    {
      catalog_free_representation (repr_p);
    }

  return error;
}

/*
 * ct_find_oid_by_classname () - Get an instance oid in the ct_Class using the
 *                               index for classname
 *   return:
 *   name(in):
 *   oid(in):
 */
static int
catcls_find_oid_by_class_name (THREAD_ENTRY * thread_p, const char *name_p,
			       OID * oid_p)
{
  DB_VALUE key_val;
  int error = NO_ERROR;

  error = db_make_varchar (&key_val, DB_MAX_IDENTIFIER_LENGTH,
			   (char *) name_p, strlen (name_p));
  if (error != NO_ERROR)
    {
      return error;
    }

  error =
    xbtree_find_unique (thread_p, &catcls_Btid, true, &key_val,
			&ct_Class.classoid, oid_p, false);
  if (error == BTREE_ERROR_OCCURRED)
    {
      pr_clear_value (&key_val);
      return error;
    }
  else if (error == BTREE_KEY_NOTFOUND)
    {
      OID_SET_NULL (oid_p);
    }

  pr_clear_value (&key_val);
  return NO_ERROR;
}

/*
 * ct_convert_classoid_to_oid () -
 *   return:
 *   oid_val(in):
 */
static int
catcls_convert_class_oid_to_oid (THREAD_ENTRY * thread_p,
				 DB_VALUE * oid_val_p)
{
  char *name_p = NULL;
  OID oid_buf;
  OID *class_oid_p, *oid_p;
  CATCLS_ENTRY *entry_p;

  if (DB_IS_NULL (oid_val_p))
    {
      return NO_ERROR;
    }

  class_oid_p = DB_PULL_OID (oid_val_p);

  if (csect_enter_as_reader (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      return ER_FAILED;
    }

  oid_p = catcls_find_oid (class_oid_p);

  csect_exit (CSECT_CT_OID_TABLE);

  if (oid_p == NULL)
    {
      oid_p = &oid_buf;
      name_p = heap_get_class_name (thread_p, class_oid_p);
      if (name_p == NULL)
	{
	  return NO_ERROR;
	}

      if (catcls_find_oid_by_class_name (thread_p, name_p, oid_p) != NO_ERROR)
	{
	  free_and_init (name_p);
	  return er_errid ();
	}

      if (!OID_ISNULL (oid_p) && (entry_p = catcls_allocate_entry ()) != NULL)
	{
	  COPY_OID (&entry_p->class_oid, class_oid_p);
	  COPY_OID (&entry_p->oid, oid_p);
	  if (csect_enter (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) !=
	      NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	  catcls_put_entry (entry_p);
	  csect_exit (CSECT_CT_OID_TABLE);
	}
    }

  db_push_oid (oid_val_p, oid_p);

  if (name_p)
    {
      free_and_init (name_p);
    }

  return NO_ERROR;
}

/*
 * ct_convert_attrid_to_name () -
 *   return:
 *   obuf(in):
 *   value(in):
 */
static int
catcls_convert_attr_id_to_name (THREAD_ENTRY * thread_p, OR_BUF * orbuf_p,
				OR_VALUE * value_p)
{
  OR_BUF *buf_p, orep;
  OR_VALUE *indexes, *keys;
  OR_VALUE *index_atts, *key_atts;
  OR_VALUE *id_val_p = NULL, *id_atts;
  OR_VALUE *ids;
  OR_VARINFO *vars = NULL;
  int id;
  int size;
  int i, j, k;
  int error = NO_ERROR;

  buf_p = &orep;
  or_init (buf_p, orbuf_p->buffer, (int) (orbuf_p->endptr - orbuf_p->buffer));

  or_advance (buf_p, OR_HEADER_SIZE);

  size = tf_Metaclass_class.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);

      return error;
    }

  /* jump to the 'attributes' and extract its id/name.
   * there are no indexes for shared or class attributes,
   * so we need only id/name for 'attributes'.
   */
  or_seek (buf_p, vars[ORC_ATTRIBUTES_INDEX].offset);

  id_val_p = catcls_allocate_or_value (1);
  if (id_val_p == NULL)
    {
      free_and_init (vars);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  error = catcls_get_subset (thread_p, buf_p,
			     vars[ORC_ATTRIBUTES_INDEX].length,
			     id_val_p, ct_get_or_value_from_attrid);
  if (error != NO_ERROR)
    {
      free_and_init (vars);
      free_and_init (id_val_p);
      return error;
    }

  /* replace id with name for each key attribute */
  for (indexes = value_p->sub.value, i = 0; i < value_p->sub.count; i++)
    {
      index_atts = indexes[i].sub.value;

      for (keys = (index_atts[4]).sub.value,
	   j = 0; j < (index_atts[4]).sub.count; j++)
	{
	  key_atts = keys[j].sub.value;
	  id = DB_GET_INT (&key_atts[1].value);

	  for (ids = id_val_p->sub.value, k = 0; k < id_val_p->sub.count; k++)
	    {
	      id_atts = ids[k].sub.value;
	      if (id == DB_GET_INT (&id_atts[0].value))
		{
		  pr_clear_value (&key_atts[1].value);
		  pr_clone_value (&id_atts[1].value, &key_atts[1].value);
		}
	    }
	}
    }

  catcls_free_or_value (id_val_p);
  free_and_init (vars);

  return NO_ERROR;
}

/*
 * ct_apply_component_type () -
 *   return:
 *   value(in):
 *   type(in):
 */
static void
catcls_apply_component_type (OR_VALUE * value_p, int type)
{
  OR_VALUE *subset_p, *attrs;
  int i;

  for (subset_p = value_p->sub.value, i = 0; i < value_p->sub.count; i++)
    {
      attrs = subset_p[i].sub.value;
      /* assume that the attribute values of xxx are ordered by
         { class_of, xxx_name, xxx_type, from_xxx_name, ... } */
      db_make_int (&attrs[2].value, type);
    }
}

/*
 * ct_resolution_space() - modified of sm_resolution_space()
 *   return:
 *   name_space(in):
 *
 * TODO: need to integrate
 */
static int
catcls_resolution_space (int name_space)
{
  int res_space = 5;		/* ID_INSTANCE */

  /* TODO: is ID_CLASS_ATTRIBUTE corret?? */
  if (name_space == 1)		/* ID_SHARED_ATTRIBUTE */
    {
      res_space = 6;		/* ID_CLASS */
    }

  return res_space;
}

/*
 * ct_apply_resolutions () -
 *   return:
 *   value(in):
 *   res(in):
 */
static void
catcls_apply_resolutions (OR_VALUE * value_p, OR_VALUE * resolution_p)
{
  OR_VALUE *subset_p, *resolution_subset_p;
  OR_VALUE *attrs, *res_attrs;
  int i, j;
  int attr_name_space;

  for (resolution_subset_p = resolution_p->sub.value, i = 0;
       i < resolution_p->sub.count; i++)
    {
      res_attrs = resolution_subset_p[i].sub.value;

      for (subset_p = value_p->sub.value, j = 0; j < value_p->sub.count; j++)
	{
	  attrs = subset_p[j].sub.value;

	  /* assume that the attribute values of xxx are ordered by
	     { class_of, xxx_name, xxx_type, from_xxx_name, ... } */

	  /* compare component name & name space */
	  if (tp_value_compare (&attrs[1].value, &res_attrs[1].value, 1, 0)
	      == DB_EQ)
	    {
	      attr_name_space =
		catcls_resolution_space (DB_GET_INT (&attrs[2].value));
	      if (attr_name_space == DB_GET_INT (&res_attrs[2].value))
		{
		  /* set the value as 'from_xxx_name' */
		  pr_clear_value (&attrs[3].value);
		  pr_clone_value (&res_attrs[3].value, &attrs[3].value);
		}
	    }
	}
    }
}

/*
 * ct_get_or_value_from_class () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_class (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				OR_VALUE * value_p)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  int size;
  OR_VALUE *resolution_p = NULL;
  OID class_oid;
  int error = NO_ERROR;
  int is_global;
  char *node_name;

  error = catcls_expand_or_value_by_def (value_p, &ct_Class);
  if (error != NO_ERROR)
    {
      goto error;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_class.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      goto error;
    }

  /* fixed */

  or_advance (buf_p, ORC_ATT_COUNT_OFFSET);

  /* attribute_count */
  (*(tp_Integer.readval)) (buf_p, &attrs[1].value, NULL, -1, true, NULL, 0);

  /* object_size */
  or_advance (buf_p, OR_INT_SIZE);

  /* shared_count */
  (*(tp_Integer.readval)) (buf_p, &attrs[2].value, NULL, -1, true, NULL, 0);

  /* method_count */
  (*(tp_Integer.readval)) (buf_p, &attrs[3].value, NULL, -1, true, NULL, 0);

  /* class_method_count */
  (*(tp_Integer.readval)) (buf_p, &attrs[4].value, NULL, -1, true, NULL, 0);

  /* class_att_count */
  (*(tp_Integer.readval)) (buf_p, &attrs[5].value, NULL, -1, true, NULL, 0);

  /* flags */
  (*(tp_Integer.readval)) (buf_p, &attrs[6].value, NULL, -1, true, NULL, 0);
  is_global = db_get_int (&attrs[6].value) & SM_CLASSFLAG_GLOBAL;
  is_global = is_global ? 1 : 0;

  /* class_type */
  (*(tp_Integer.readval)) (buf_p, &attrs[7].value, NULL, -1, true, NULL, 0);

  /* owner */
  (*(tp_Object.readval)) (buf_p, &attrs[8].value, NULL, -1, true, NULL, 0);

  /*real_oid */
  or_advance (buf_p, OR_OID_SIZE);

  /*real heap file id */
  or_advance (buf_p, 3 * OR_INT_SIZE);

  /* variable */

  /* name */
  attr_val_p = &attrs[9].value;
  (*(tp_String.readval)) (buf_p, attr_val_p, NULL, vars[0].length, true, NULL,
			  0);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* (class_of) */
  if (catcls_find_class_oid_by_class_name (thread_p,
					   DB_GET_STRING (&attrs[9].value),
					   &class_oid) != NO_ERROR)
    {
      error = er_errid ();
      goto error;
    }
  db_push_oid (&attrs[0].value, &class_oid);

  /* loader_commands */
  or_advance (buf_p, vars[1].length);

  /* representations */
  or_advance (buf_p, vars[2].length);

  /* sub_classes */
  error = catcls_get_object_set (thread_p, buf_p, vars[3].length, &attrs[10]);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* super_classes */
  error = catcls_get_object_set (thread_p, buf_p, vars[4].length, &attrs[11]);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* attributes */
  error = catcls_get_subset (thread_p, buf_p, vars[5].length, &attrs[12],
			     catcls_get_or_value_from_attribute);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* shared_attributes */
  error = catcls_get_subset (thread_p, buf_p, vars[6].length, &attrs[13],
			     catcls_get_or_value_from_attribute);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* class_attributes */
  error = catcls_get_subset (thread_p, buf_p, vars[7].length, &attrs[14],
			     catcls_get_or_value_from_attribute);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* methods */
  error = catcls_get_subset (thread_p, buf_p, vars[8].length, &attrs[15],
			     catcls_get_or_value_from_method);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* class_methods */
  error = catcls_get_subset (thread_p, buf_p, vars[9].length, &attrs[16],
			     catcls_get_or_value_from_method);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* (apply attribute & method type) */
  catcls_apply_component_type (&attrs[12], 0);
  catcls_apply_component_type (&attrs[13], 2);
  catcls_apply_component_type (&attrs[14], 1);
  catcls_apply_component_type (&attrs[15], 0);
  catcls_apply_component_type (&attrs[16], 1);

  /* method_files */
  error = catcls_get_subset (thread_p, buf_p, vars[10].length, &attrs[17],
			     catcls_get_or_value_from_method_file);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* (resolutions) */
  if (vars[11].length > 0)
    {
      resolution_p = catcls_allocate_or_value (1);
      if (resolution_p == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto error;
	}

      error = catcls_get_subset (thread_p, buf_p, vars[11].length,
				 resolution_p,
				 catcls_get_or_value_from_resolution);
      if (error != NO_ERROR)
	{
	  goto error;
	}

      catcls_apply_resolutions (&attrs[12], resolution_p);
      catcls_apply_resolutions (&attrs[13], resolution_p);
      catcls_apply_resolutions (&attrs[14], resolution_p);
      catcls_apply_resolutions (&attrs[15], resolution_p);
      catcls_apply_resolutions (&attrs[16], resolution_p);
      catcls_free_or_value (resolution_p);
      resolution_p = NULL;
    }

  /* query_spec */
  error = catcls_get_subset (thread_p, buf_p, vars[12].length, &attrs[18],
			     catcls_get_or_value_from_query_spec);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* triggers */
  or_advance (buf_p, vars[13].length);

  /* node_name */
  /* jump the properties, get the node name firstly */
  or_seek (buf_p, vars[15].offset);
  attr_val_p = &attrs[21].value;
  (*(tp_String.readval)) (buf_p, attr_val_p, NULL, vars[15].length, true,
			  NULL, 0);
  db_string_truncate (attr_val_p, 64);
  node_name = db_get_string (attr_val_p);

  /* properties */
  /* go back, get the properties */
  or_seek (buf_p, vars[14].offset);
  error = catcls_get_property_set (thread_p, buf_p, vars[14].length,
				   &attrs[19], is_global, node_name);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* is_global */
  attr_val_p = &attrs[20].value;
  db_make_int (attr_val_p, is_global);

  /* jump the node name information */
  or_advance (buf_p, vars[15].length);

  if (vars)
    {
      free_and_init (vars);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  if (resolution_p)
    {
      catcls_free_or_value (resolution_p);
    }

  return error;
}

/*
 * ct_get_or_value_from_attribute () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_attribute (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				    OR_VALUE * value_p)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  int size;
  int error = NO_ERROR;

  error = catcls_expand_or_value_by_def (value_p, &ct_Attribute);
  if (error != NO_ERROR)
    {
      goto error;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_attribute.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      goto error;
    }

  /* id */
  or_advance (buf_p, OR_INT_SIZE);

  /* type */
  (*(tp_Integer.readval)) (buf_p, &attrs[4].value, NULL, -1, true, NULL, 0);

  /* offset */
  or_advance (buf_p, OR_INT_SIZE);

  /* order */
  (*(tp_Integer.readval)) (buf_p, &attrs[5].value, NULL, -1, true, NULL, 0);

  /* class */
  attr_val_p = &attrs[6].value;
  (*(tp_Object.readval)) (buf_p, attr_val_p, NULL, -1, true, NULL, 0);
  error = catcls_convert_class_oid_to_oid (thread_p, attr_val_p);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* flag */
  attr_val_p = &attrs[7].value;
  (*(tp_Integer.readval)) (buf_p, attr_val_p, NULL, -1, true, NULL, 0);

  /* for 'is_nullable', reverse NON_NULL flag */
  db_make_int (attr_val_p,
	       (DB_GET_INT (attr_val_p) & SM_ATTFLAG_NON_NULL) ? false :
	       true);

  /* index_file_id */
  or_advance (buf_p, OR_INT_SIZE);

  /* index_root_pageid */
  or_advance (buf_p, OR_INT_SIZE);

  /* index_volid_key */
  or_advance (buf_p, OR_INT_SIZE);

  /** variable **/

  /* name */
  attr_val_p = &attrs[1].value;
  (*(tp_String.readval)) (buf_p, attr_val_p, NULL, vars[0].length, true, NULL,
			  0);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* default value */
  attr_val_p = &attrs[8].value;
  or_get_value (buf_p, attr_val_p, NULL, vars[1].length, true);
  valcnv_convert_value_to_string (attr_val_p);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* original value */
  or_advance (buf_p, vars[2].length);

  /* domain */
  error = catcls_get_subset (thread_p, buf_p, vars[3].length, &attrs[9],
			     catcls_get_or_value_from_domain);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* triggers */
  or_advance (buf_p, vars[4].length);

  /* properties */
  or_advance (buf_p, vars[5].length);

  if (vars)
    {
      free_and_init (vars);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * ct_get_or_value_from_attrid () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
ct_get_or_value_from_attrid (THREAD_ENTRY * thread_p, OR_BUF * buf,
			     OR_VALUE * value)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val;
  OR_VARINFO *vars = NULL;
  int size;
  char *start_ptr;
  int error = NO_ERROR;

  error = catcls_expand_or_value_by_def (value, &ct_Attrid);
  if (error != NO_ERROR)
    {
      goto error;
    }

  attrs = value->sub.value;

  /* variable offset */
  start_ptr = buf->ptr;

  size = tf_Metaclass_attribute.n_variable;
  vars = or_get_var_table (buf, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      goto error;
    }

  /* id */
  (*(tp_Integer.readval)) (buf, &attrs[0].value, NULL, -1, true, NULL, 0);

  or_advance (buf, (int) (start_ptr - buf->ptr) + vars[0].offset);

  /* name */
  attr_val = &attrs[1].value;
  (*(tp_String.readval)) (buf, attr_val, NULL, vars[0].length, true, NULL, 0);
  db_string_truncate (attr_val, DB_MAX_IDENTIFIER_LENGTH);

  /* go to the end */
  or_advance (buf, (int) (start_ptr - buf->ptr)
	      + (vars[5].offset + vars[5].length));

  if (vars)
    {
      free_and_init (vars);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * ct_get_or_value_from_domain () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_domain (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				 OR_VALUE * value_p)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  int size;
  int error = NO_ERROR;

  error = catcls_expand_or_value_by_def (value_p, &ct_Domain);
  if (error != NO_ERROR)
    {
      goto error;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_domain.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      goto error;
    }

  /* type */
  (*(tp_Integer.readval)) (buf_p, &attrs[1].value, NULL, -1, true, NULL, 0);

  /* precision */
  (*(tp_Integer.readval)) (buf_p, &attrs[2].value, NULL, -1, true, NULL, 0);

  /* scale */
  (*(tp_Integer.readval)) (buf_p, &attrs[3].value, NULL, -1, true, NULL, 0);

  /* codeset */
  (*(tp_Integer.readval)) (buf_p, &attrs[4].value, NULL, -1, true, NULL, 0);

  /* class */
  attr_val_p = &attrs[5].value;
  (*(tp_Object.readval)) (buf_p, attr_val_p, NULL, -1, true, NULL, 0);

  if (!DB_IS_NULL (attr_val_p))
    {
      error = catcls_convert_class_oid_to_oid (thread_p, attr_val_p);
      if (error != NO_ERROR)
	{
	  goto error;
	}

      if (DB_IS_NULL (attr_val_p))
	{
	  /* if self reference for example, "class x (a x)"
	     set an invalid data type, and fill its value later */
	  error = db_value_domain_init (attr_val_p, DB_TYPE_VARIABLE,
					DB_DEFAULT_PRECISION,
					DB_DEFAULT_SCALE);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }
	}
    }

  /* set_domain */
  error = catcls_get_subset (thread_p, buf_p, vars[0].length, &attrs[6],
			     catcls_get_or_value_from_domain);
  if (error != NO_ERROR)
    {
      goto error;
    }

  if (vars)
    {
      free_and_init (vars);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * ct_get_or_value_from_method () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_method (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				 OR_VALUE * value_p)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  int size;
  int error = NO_ERROR;

  error = catcls_expand_or_value_by_def (value_p, &ct_Method);
  if (error != NO_ERROR)
    {
      goto error;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_method.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      goto error;
    }

  /* class */
  attr_val_p = &attrs[4].value;
  (*(tp_Object.readval)) (buf_p, attr_val_p, NULL, -1, true, NULL, 0);
  error = catcls_convert_class_oid_to_oid (thread_p, attr_val_p);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* id */
  or_advance (buf_p, OR_INT_SIZE);

  /* name */
  attr_val_p = &attrs[1].value;
  (*(tp_String.readval)) (buf_p, attr_val_p, NULL, vars[0].length, true, NULL,
			  0);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* signatures */
  error = catcls_get_subset (thread_p, buf_p, vars[1].length, &attrs[5],
			     catcls_get_or_value_from_method_signiture);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* properties */
  or_advance (buf_p, vars[2].length);

  if (vars)
    {
      free_and_init (vars);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * ct_get_or_value_from_methsig () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_method_signiture (THREAD_ENTRY * thread_p,
					   OR_BUF * buf_p, OR_VALUE * value_p)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  int size;
  int error = NO_ERROR;

  error = catcls_expand_or_value_by_def (value_p, &ct_Methsig);
  if (error != NO_ERROR)
    {
      goto error;
    }

  attrs = value_p->sub.value;

  /* variable offset */
  size = tf_Metaclass_methsig.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      goto error;
    }

  /* arg_count */
  (*(tp_Integer.readval)) (buf_p, &attrs[1].value, NULL, -1, true, NULL, 0);

  /* function_name */
  attr_val_p = &attrs[2].value;
  (*(tp_String.readval)) (buf_p, attr_val_p, NULL, vars[0].length, true, NULL,
			  0);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* string_def */
  or_advance (buf_p, vars[1].length);

  /* return_value */
  error = catcls_get_subset (thread_p, buf_p, vars[2].length, &attrs[3],
			     catcls_get_or_value_from_method_argument);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* arguments */
  error = catcls_get_subset (thread_p, buf_p, vars[3].length, &attrs[4],
			     catcls_get_or_value_from_method_argument);
  if (error != NO_ERROR)
    {
      goto error;
    }

  if (vars)
    {
      free_and_init (vars);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * ct_get_or_value_from_metharg () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_method_argument (THREAD_ENTRY * thread_p,
					  OR_BUF * buf_p, OR_VALUE * value_p)
{
  OR_VALUE *attrs;
  OR_VARINFO *vars = NULL;
  int size;
  int error;

  error = catcls_expand_or_value_by_def (value_p, &ct_Metharg);
  if (error != NO_ERROR)
    {
      goto error;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_metharg.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      goto error;
    }

  /* type */
  (*(tp_Integer.readval)) (buf_p, &attrs[1].value, NULL, -1, true, NULL, 0);

  /* index */
  (*(tp_Integer.readval)) (buf_p, &attrs[2].value, NULL, -1, true, NULL, 0);

  /* domain */
  error = catcls_get_subset (thread_p, buf_p, vars[0].length, &attrs[3],
			     catcls_get_or_value_from_domain);
  if (error != NO_ERROR)
    {
      goto error;
    }

  if (vars)
    {
      free_and_init (vars);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * ct_get_or_value_from_methfile () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_method_file (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				      OR_VALUE * value_p)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  int size;
  int error = NO_ERROR;

  error = catcls_expand_or_value_by_def (value_p, &ct_Methfile);
  if (error != NO_ERROR)
    {
      goto error;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_methfile.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      goto error;
    }

  /* class */
  attr_val_p = &attrs[1].value;
  (*(tp_Object.readval)) (buf_p, attr_val_p, NULL, -1, true, NULL, 0);
  error = catcls_convert_class_oid_to_oid (thread_p, attr_val_p);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* name */
  attr_val_p = &attrs[2].value;
  (*(tp_String.readval)) (buf_p, attr_val_p, NULL, vars[0].length, true, NULL,
			  0);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* properties */
  or_advance (buf_p, vars[1].length);

  if (vars)
    {
      free_and_init (vars);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * ct_get_or_value_from_resolution () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_resolution (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				     OR_VALUE * value_p)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  int size;
  int error = NO_ERROR;

  error = catcls_expand_or_value_by_def (value_p, &ct_Resolution);
  if (error != NO_ERROR)
    {
      goto error;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_resolution.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      goto error;
    }

  /* class */
  attr_val_p = &attrs[0].value;
  (*(tp_Object.readval)) (buf_p, attr_val_p, NULL, -1, true, NULL, 0);
  error = catcls_convert_class_oid_to_oid (thread_p, attr_val_p);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* type */
  (*(tp_Integer.readval)) (buf_p, &attrs[2].value, NULL, -1, true, NULL, 0);

  /* name */
  attr_val_p = &attrs[3].value;
  (*(tp_String.readval)) (buf_p, attr_val_p, NULL, vars[0].length, true, NULL,
			  0);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* alias */
  attr_val_p = &attrs[1].value;
  (*(tp_String.readval)) (buf_p, attr_val_p, NULL, vars[1].length, true, NULL,
			  0);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  if (vars)
    {
      free_and_init (vars);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * ct_get_or_value_from_queryspec () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_query_spec (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				     OR_VALUE * value_p)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  int size;
  int error = NO_ERROR;

  error = catcls_expand_or_value_by_def (value_p, &ct_Queryspec);
  if (error != NO_ERROR)
    {
      goto error;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_query_spec.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      goto error;
    }

  /* specification */
  attr_val_p = &attrs[1].value;
  (*(tp_String.readval)) (buf_p, attr_val_p, NULL, vars[0].length, true, NULL,
			  0);
  db_string_truncate (attr_val_p, DB_MAX_SPEC_LENGTH);

  if (vars)
    {
      free_and_init (vars);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * ct_get_or_value_from_indexes () -
 *   return:
 *   seq(in):
 *   values(in):
 *   is_unique(in):
 *   is_reverse(in):
 *   is_primary_key(in):
 *   is_foreign_key(in):
 */
static int
catcls_get_or_value_from_indexes (DB_SEQ * seq_p, OR_VALUE * values,
				  int is_unique, int is_reverse,
				  int is_primary_key, int is_foreign_key,
				  int is_global, const char *node_name)
{
  int seq_size;
  DB_VALUE keys, prefix_val;
  DB_SEQ *key_seq_p = NULL, *prefix_seq;
  int key_size, att_cnt;
  OR_VALUE *attrs, *key_attrs;
  DB_VALUE *attr_val_p;
  OR_VALUE *subset_p;
  int e, i, j, k;
  int error = NO_ERROR;

  db_value_put_null (&keys);
  db_value_put_null (&prefix_val);

  seq_size = set_size (seq_p);
  for (i = 0, j = 0; i < seq_size; i += 2, j++)
    {
      error = catcls_expand_or_value_by_def (&values[j], &ct_Index);
      if (error != NO_ERROR)
	{
	  goto error;
	}

      attrs = values[j].sub.value;

      /* index_name */
      attr_val_p = &attrs[1].value;
      error = set_get_element (seq_p, i, attr_val_p);
      if (error != NO_ERROR)
	{
	  goto error;
	}
      db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

      /* (is_unique) */
      db_make_int (&attrs[2].value, is_unique);

      error = set_get_element (seq_p, i + 1, &keys);
      if (error != NO_ERROR)
	{
	  goto error;
	}

      if (DB_VALUE_TYPE (&keys) == DB_TYPE_SEQUENCE)
	{
	  key_seq_p = DB_GET_SEQUENCE (&keys);
	}
      else
	{
	  assert (0);
	  error = ER_SM_INVALID_PROPERTY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  goto error;
	}

      key_size = set_size (key_seq_p);
      att_cnt = (key_size - 1) / 2;

      /* key_count */
      db_make_int (&attrs[3].value, att_cnt);

      subset_p = catcls_allocate_or_value (att_cnt);
      if (subset_p == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto error;
	}

      attrs[4].sub.value = subset_p;
      attrs[4].sub.count = att_cnt;

      /* key_attrs */
      e = 1;
      for (k = 0; k < att_cnt; k++)	/* for each [attrID, asc_desc]+ */
	{
	  error = catcls_expand_or_value_by_def (&subset_p[k], &ct_Indexkey);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }

	  key_attrs = subset_p[k].sub.value;

	  /* key_attr_id */
	  attr_val_p = &key_attrs[1].value;
	  error = set_get_element (key_seq_p, e++, attr_val_p);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }

	  /* key_order */
	  db_make_int (&key_attrs[2].value, k);

	  /* asc_desc */
	  attr_val_p = &key_attrs[3].value;
	  error = set_get_element (key_seq_p, e++, attr_val_p);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }

	  /* prefix_length */
	  db_make_int (&key_attrs[4].value, -1);
	}

      if (!is_primary_key && !is_foreign_key)
	{
	  /* prefix_length */
	  error = set_get_element (key_seq_p, key_size - 1, &prefix_val);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }

	  if (DB_VALUE_TYPE (&prefix_val) == DB_TYPE_SEQUENCE)
	    {
	      prefix_seq = DB_GET_SEQUENCE (&prefix_val);
	      assert (set_size (prefix_seq) == att_cnt);

	      for (k = 0; k < att_cnt; k++)
		{
		  key_attrs = subset_p[k].sub.value;
		  attr_val_p = &key_attrs[4].value;

		  error = set_get_element (prefix_seq, k, attr_val_p);
		  if (error != NO_ERROR)
		    {
		      pr_clear_value (&prefix_val);
		      goto error;
		    }
		}

	      pr_clear_value (&prefix_val);
	    }
	  else
	    {
	      assert (DB_VALUE_TYPE (&prefix_val) == DB_TYPE_INTEGER);
	    }
	}

      pr_clear_value (&keys);

      /* is_reverse */
      db_make_int (&attrs[5].value, is_reverse);

      /* is_primary_key */
      db_make_int (&attrs[6].value, is_primary_key);

      /* is_foreign_key */
      db_make_int (&attrs[7].value, is_foreign_key);

      /* is_global */
      db_make_int (&attrs[8].value, is_global);

      /* node_name */
      db_make_string (&attrs[9].value, node_name);
    }

  return NO_ERROR;

error:

  pr_clear_value (&keys);
  return error;
}

/*
 * ct_get_subset () -
 *   return:
 *   buf(in):
 *   expected_size(in):
 *   value(in):
 *   reader(in):
 */
static int
catcls_get_subset (THREAD_ENTRY * thread_p, OR_BUF * buf_p, int expected_size,
		   OR_VALUE * value_p, CREADER reader)
{
  OR_VALUE *subset_p;
  int count, i;
  int error = NO_ERROR;

  if (expected_size == 0)
    {
      value_p->sub.count = 0;
      return NO_ERROR;
    }

  count = or_skip_set_header (buf_p);
  subset_p = catcls_allocate_or_value (count);
  if (subset_p == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  value_p->sub.value = subset_p;
  value_p->sub.count = count;

  for (i = 0; i < count; i++)
    {
      error = (*reader) (thread_p, buf_p, &subset_p[i]);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  return NO_ERROR;
}

/*
 * ct_get_objset () -
 *   return:
 *   buf(in):
 *   expected_size(in):
 *   value(in):
 */
static int
catcls_get_object_set (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
		       int expected_size, OR_VALUE * value_p)
{
  DB_SET *oid_set_p = NULL;
  DB_VALUE oid_val;
  int count, i;
  int error = NO_ERROR;

  if (expected_size == 0)
    {
      return NO_ERROR;
    }

  count = or_skip_set_header (buf_p);
  oid_set_p = set_create_sequence (count);
  if (oid_set_p == NULL)
    {
      error = er_errid ();
      goto error;
    }

  for (i = 0; i < count; i++)
    {
      (*(tp_Object.readval)) (buf_p, &oid_val, NULL, -1, true, NULL, 0);

      error = catcls_convert_class_oid_to_oid (thread_p, &oid_val);
      if (error != NO_ERROR)
	{
	  goto error;
	}

      error = set_put_element (oid_set_p, i, &oid_val);
      if (error != NO_ERROR)
	{
	  goto error;
	}
    }

  db_make_sequence (&value_p->value, oid_set_p);
  return NO_ERROR;

error:

  if (oid_set_p)
    {
      set_free (oid_set_p);
    }

  return error;
}

/*
 * ct_get_propset () -
 *   return:
 *   buf(in):
 *   expected_size(in):
 *   value(in):
 */
static int
catcls_get_property_set (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
			 int expected_size, OR_VALUE * value_p,
			 int is_global, const char *node_name)
{
  DB_VALUE prop_val;
  DB_SEQ *prop_seq_p = NULL;
  int n_size = 0;
  CATCLS_PROPERTY property_vars[SM_PROPERTY_NUM_INDEX_FAMILY] = {
    {SM_PROPERTY_INDEX, NULL, 0, false, false, false, false},
    {SM_PROPERTY_UNIQUE, NULL, 0, true, false, false, false},
    {SM_PROPERTY_REVERSE_INDEX, NULL, 0, false, true, false, false},
    {SM_PROPERTY_REVERSE_UNIQUE, NULL, 0, true, true, false, false},
    {SM_PROPERTY_PRIMARY_KEY, NULL, 0, true, false, true, false},
    {SM_PROPERTY_FOREIGN_KEY, NULL, 0, false, false, false, true},
  };

  DB_VALUE vals[SM_PROPERTY_NUM_INDEX_FAMILY];
  OR_VALUE *subset_p = NULL;
  int error = NO_ERROR;
  int i, idx;

  if (expected_size == 0)
    {
      value_p->sub.count = 0;
      return NO_ERROR;
    }

  db_value_put_null (&prop_val);
  for (i = 0; i < SM_PROPERTY_NUM_INDEX_FAMILY; i++)
    {
      db_value_put_null (&vals[i]);
    }

  (*(tp_Sequence.readval)) (buf_p, &prop_val, NULL, expected_size, true, NULL,
			    0);
  prop_seq_p = DB_GET_SEQUENCE (&prop_val);

  for (i = 0; i < SM_PROPERTY_NUM_INDEX_FAMILY; i++)
    {
      if (prop_seq_p != NULL &&
	  (classobj_get_prop (prop_seq_p, property_vars[i].name, &vals[i]) >
	   0))
	{

	  if (DB_VALUE_TYPE (&vals[i]) == DB_TYPE_SEQUENCE)
	    {
	      property_vars[i].seq = DB_GET_SEQUENCE (&vals[i]);
	    }

	  if (property_vars[i].seq != NULL)
	    {
	      property_vars[i].size = set_size (property_vars[i].seq) / 2;
	      n_size += property_vars[i].size;
	    }
	}
    }

  if (n_size > 0)
    {
      subset_p = catcls_allocate_or_value (n_size);
      if (subset_p == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto error;
	}

      value_p->sub.value = subset_p;
      value_p->sub.count = n_size;
    }
  else
    {
      value_p->sub.value = NULL;
      value_p->sub.count = 0;
    }

  idx = 0;
  for (i = 0; i < SM_PROPERTY_NUM_INDEX_FAMILY; i++)
    {
      if (property_vars[i].seq != NULL)
	{
	  error = catcls_get_or_value_from_indexes (property_vars[i].seq,
						    &subset_p[idx],
						    property_vars[i].
						    is_unique,
						    property_vars[i].
						    is_reverse,
						    property_vars[i].
						    is_primary_key,
						    property_vars[i].
						    is_foreign_key,
						    is_global, node_name);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }
	}

      idx += property_vars[i].size;
    }

  error = catcls_convert_attr_id_to_name (thread_p, buf_p, value_p);
  if (error != NO_ERROR)
    {
      goto error;
    }

  pr_clear_value (&prop_val);
  for (i = 0; i < SM_PROPERTY_NUM_INDEX_FAMILY; i++)
    {
      pr_clear_value (&vals[i]);
    }

  return NO_ERROR;

error:
  pr_clear_value (&prop_val);
  for (i = 0; i < SM_PROPERTY_NUM_INDEX_FAMILY; i++)
    {
      pr_clear_value (&vals[i]);
    }

  return error;
}

/*
 * ct_reorder_attrs_by_rep () -
 *   return:
 *   value(in):
 */
static int
catcls_reorder_attributes_by_repr (THREAD_ENTRY * thread_p,
				   OR_VALUE * value_p)
{
  OR_VALUE *attrs, *var_attrs;
  int n_attrs;
  DISK_ATTR *fixed_p, *variable_p;
  int n_fixed, n_variable;
  DISK_REPR *repr_p = NULL;
  REPR_ID repr_id;
  OID *class_oid_p;
  int i, j;
  int error = NO_ERROR;

  class_oid_p = &value_p->id.classoid;
  error =
    catalog_get_last_representation_id (thread_p, class_oid_p, &repr_id);
  if (error != NO_ERROR)
    {
      goto error;
    }
  else
    {
      repr_p = catalog_get_representation (thread_p, class_oid_p, repr_id);
      if (repr_p == NULL)
	{
	  error = er_errid ();
	  goto error;
	}
    }

  fixed_p = repr_p->fixed;
  n_fixed = repr_p->n_fixed;
  variable_p = repr_p->variable;
  n_variable = repr_p->n_variable;

  attrs = value_p->sub.value;
  n_attrs = n_fixed + n_variable;

  for (i = 0; i < n_fixed; i++)
    {
      for (j = i; j < n_attrs; j++)
	{
	  if (fixed_p[i].id == attrs[j].id.attrid)
	    {
	      EXCHANGE_OR_VALUE (attrs[i], attrs[j]);
	      break;
	    }
	}
    }

  var_attrs = &attrs[n_fixed];
  for (i = 0; i < n_variable; i++)
    {
      for (j = i; j < n_variable; j++)
	{
	  if (variable_p[i].id == var_attrs[j].id.attrid)
	    {
	      EXCHANGE_OR_VALUE (var_attrs[i], var_attrs[j]);
	      break;
	    }
	}
    }
  catalog_free_representation (repr_p);

  return NO_ERROR;

error:

  if (repr_p)
    {
      catalog_free_representation (repr_p);
    }

  return error;
}

/*
 * ct_expand_or_value_by_rep () -
 *   return:
 *   value(in):
 *   class_oid(in):
 *   rep(in):
 */
static int
catcls_expand_or_value_by_repr (OR_VALUE * value_p, OID * class_oid_p,
				DISK_REPR * repr_p)
{
  OR_VALUE *attrs, *var_attrs;
  int n_attrs;
  DISK_ATTR *fixed_p, *variable_p;
  int n_fixed, n_variable;
  int i;
  int error = NO_ERROR;

  fixed_p = repr_p->fixed;
  n_fixed = repr_p->n_fixed;
  variable_p = repr_p->variable;
  n_variable = repr_p->n_variable;

  n_attrs = n_fixed + n_variable;
  attrs = catcls_allocate_or_value (n_attrs);
  if (attrs == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  value_p->sub.value = attrs;
  value_p->sub.count = n_attrs;

  COPY_OID (&value_p->id.classoid, class_oid_p);

  for (i = 0; i < n_fixed; i++)
    {
      attrs[i].id.attrid = fixed_p[i].id;
      error = db_value_domain_init (&attrs[i].value, fixed_p[i].type,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  var_attrs = &attrs[n_fixed];
  for (i = 0; i < n_variable; i++)
    {
      var_attrs[i].id.attrid = variable_p[i].id;
      error = db_value_domain_init (&var_attrs[i].value, variable_p[i].type,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  return NO_ERROR;
}

/*
 * ct_expand_or_value_by_subset () -
 *   return:
 *   value(in):
 */
static void
catcls_expand_or_value_by_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p)
{
  DB_SET *set_p;
  int size, i;
  DB_VALUE element;
  OID *oid_p, class_oid;
  OR_VALUE *subset_p;

  if (pr_is_set_type (DB_VALUE_TYPE (&value_p->value)))
    {
      set_p = DB_PULL_SET (&value_p->value);
      size = set_size (set_p);
      if (size > 0)
	{
	  set_get_element (set_p, 0, &element);
	  if (DB_VALUE_TYPE (&element) == DB_TYPE_OID)
	    {
	      oid_p = DB_PULL_OID (&element);
	      heap_get_class_oid (thread_p, oid_p, &class_oid);

	      if (!OID_EQ (&class_oid, &ct_Class.classoid))
		{
		  subset_p = catcls_allocate_or_value (size);
		  if (subset_p != NULL)
		    {
		      value_p->sub.value = subset_p;
		      value_p->sub.count = size;

		      for (i = 0; i < size; i++)
			{
			  COPY_OID (&((subset_p[i]).id.classoid), &class_oid);
			}
		    }
		}
	    }
	}
    }
}

/*
 * ct_put_or_value_into_buf () -
 *   return:
 *   value(in):
 *   chn(in):
 *   buf(in):
 *   class_oid(in):
 *   rep(in):
 */
static int
catcls_put_or_value_into_buffer (OR_VALUE * value_p, int chn, OR_BUF * buf_p,
				 OID * class_oid_p, DISK_REPR * repr_p)
{
  OR_VALUE *attrs, *var_attrs;
  int n_attrs;
  DISK_ATTR *fixed_p, *variable_p;
  int n_fixed, n_variable;
  DB_TYPE data_type;
  unsigned int repr_id_bits;
  char *bound_bits = NULL;
  int bound_size;
  char *offset_p, *start_p;
  int i, pad, offset;
  int error = NO_ERROR;

  fixed_p = repr_p->fixed;
  n_fixed = repr_p->n_fixed;
  variable_p = repr_p->variable;
  n_variable = repr_p->n_variable;

  attrs = value_p->sub.value;
  n_attrs = n_fixed + n_variable;

  bound_size = OR_BOUND_BIT_BYTES (n_fixed);
  bound_bits = (char *) malloc (bound_size);
  if (bound_bits == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, bound_size);
      goto error;
    }
  memset (bound_bits, 0, bound_size);

  /* header */
  or_put_oid (buf_p, class_oid_p);
  repr_id_bits = repr_p->id;
  if (n_fixed)
    {
      repr_id_bits |= OR_BOUND_BIT_FLAG;
    }

  or_put_int (buf_p, repr_id_bits);
  or_put_int (buf_p, chn);
  or_put_int (buf_p, 0);

  /* offset table */
  offset_p = buf_p->ptr;
  or_advance (buf_p, OR_VAR_TABLE_SIZE (n_variable));

  /* fixed */
  start_p = buf_p->ptr;
  for (i = 0; i < n_fixed; i++)
    {
      data_type = fixed_p[i].type;

      if (DB_IS_NULL (&attrs[i].value))
	{
	  or_advance (buf_p, (*tp_Type_id_map[data_type]).disksize);
	  OR_CLEAR_BOUND_BIT (bound_bits, i);
	}
      else
	{
	  (*((*tp_Type_id_map[data_type]).writeval)) (buf_p, &attrs[i].value);
	  OR_ENABLE_BOUND_BIT (bound_bits, i);
	}
    }

  pad = (int) (buf_p->ptr - start_p);
  if (pad < repr_p->fixed_length)
    {
      or_pad (buf_p, repr_p->fixed_length - pad);
    }
  else if (pad > repr_p->fixed_length)
    {
      error = ER_SM_CORRUPTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto error;
    }

  /* bound bits */
  if (n_fixed)
    {
      or_put_data (buf_p, bound_bits, bound_size);
    }

  /* variable */
  var_attrs = &attrs[n_fixed];
  for (i = 0; i < n_variable; i++)
    {
      offset = (int) (buf_p->ptr - buf_p->buffer);

      data_type = variable_p[i].type;
      (*((*tp_Type_id_map[data_type]).writeval)) (buf_p, &var_attrs[i].value);

      OR_PUT_INT (offset_p, offset);
      offset_p += OR_INT_SIZE;
    }

  /* put last offset */
  offset = (int) (buf_p->ptr - buf_p->buffer);
  OR_PUT_INT (offset_p, offset);

  if (bound_bits)
    {
      free_and_init (bound_bits);
    }

  return NO_ERROR;

error:

  if (bound_bits)
    {
      free_and_init (bound_bits);
    }

  return error;
}

/*
 * ct_get_or_value_from_buf () -
 *   return:
 *   buf(in):
 *   value(in):
 *   rep(in):
 */
static int
catcls_get_or_value_from_buffer (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				 OR_VALUE * value_p, DISK_REPR * repr_p)
{
  OR_VALUE *attrs, *var_attrs;
  int n_attrs;
  DISK_ATTR *fixed_p, *variable_p;
  int n_fixed, n_variable;
  DB_TYPE data_type;
  OR_VARINFO *vars = NULL;
  unsigned int repr_id_bits;
  char *bound_bits = NULL;
  int bound_bits_flag = false;
  char *start_p;
  int i, pad, size, rc;
  int error = NO_ERROR;

  fixed_p = repr_p->fixed;
  n_fixed = repr_p->n_fixed;
  variable_p = repr_p->variable;
  n_variable = repr_p->n_variable;

  attrs = value_p->sub.value;
  n_attrs = n_fixed + n_variable;

  /* header */
  or_advance (buf_p, OR_OID_SIZE);	/* class_oid */
  repr_id_bits = or_get_int (buf_p, &rc);	/* repid */
  bound_bits_flag = repr_id_bits & OR_BOUND_BIT_FLAG;
  or_advance (buf_p, OR_INT_SIZE);	/* chn */
  or_advance (buf_p, OR_INT_SIZE);	/* not used */

  if (bound_bits_flag)
    {
      size = OR_BOUND_BIT_BYTES (n_fixed);
      bound_bits = (char *) malloc (size);
      if (bound_bits == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, size);
	  goto error;
	}
      memset (bound_bits, 0, size);
    }

  /* offset table */
  vars = or_get_var_table (buf_p, n_variable, catcls_unpack_allocator);
  if (vars == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      n_variable * sizeof (OR_VARINFO));
      goto error;
    }

  /* fixed */
  start_p = buf_p->ptr;

  /* read bound bits before accessing fixed attributes */
  buf_p->ptr += repr_p->fixed_length;
  if (bound_bits_flag)
    {
      or_get_data (buf_p, bound_bits, OR_BOUND_BIT_BYTES (n_fixed));
    }

  buf_p->ptr = start_p;
  for (i = 0; i < n_fixed; i++)
    {
      data_type = fixed_p[i].type;

      if (bound_bits_flag && OR_GET_BOUND_BIT (bound_bits, i))
	{
	  (*((*tp_Type_id_map[data_type]).readval)) (buf_p, &attrs[i].value,
						     NULL, -1, true, NULL, 0);
	}
      else
	{
	  db_value_put_null (&attrs[i].value);
	  or_advance (buf_p, (*tp_Type_id_map[data_type]).disksize);
	}
    }

  pad = (int) (buf_p->ptr - start_p);
  if (pad < repr_p->fixed_length)
    {
      or_advance (buf_p, repr_p->fixed_length - pad);
    }
  else if (pad > repr_p->fixed_length)
    {
      error = ER_SM_CORRUPTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto error;
    }

  /* bound bits */
  if (bound_bits_flag)
    {
      or_advance (buf_p, OR_BOUND_BIT_BYTES (n_fixed));
    }

  /* variable */
  var_attrs = &attrs[n_fixed];
  for (i = 0; i < n_variable; i++)
    {
      data_type = variable_p[i].type;
      (*((*tp_Type_id_map[data_type]).readval)) (buf_p, &var_attrs[i].value,
						 NULL, vars[i].length, true,
						 NULL, 0);
      catcls_expand_or_value_by_subset (thread_p, &var_attrs[i]);
    }

  if (vars)
    {
      free_and_init (vars);
    }

  if (bound_bits)
    {
      free_and_init (bound_bits);
    }

  return NO_ERROR;

error:

  if (vars)
    {
      free_and_init (vars);
    }

  if (bound_bits)
    {
      free_and_init (bound_bits);
    }

  return error;
}

/*
 * ct_put_or_value_into_record () -
 *   return:
 *   value(in):
 *   chn(in):
 *   record(in):
 *   class_oid(in):
 */
static int
catcls_put_or_value_into_record (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
				 int chn, RECDES * record_p,
				 OID * class_oid_p)
{
  OR_BUF *buf_p, repr_buffer;
  DISK_REPR *repr_p = NULL;
  REPR_ID repr_id;
  int error = NO_ERROR;

  error =
    catalog_get_last_representation_id (thread_p, class_oid_p, &repr_id);
  if (error != NO_ERROR)
    {
      return error;
    }
  else
    {
      repr_p = catalog_get_representation (thread_p, class_oid_p, repr_id);
      if (repr_p == NULL)
	{
	  error = er_errid ();
	  return error;
	}
    }

  buf_p = &repr_buffer;
  or_init (buf_p, record_p->data, record_p->length);

  error =
    catcls_put_or_value_into_buffer (value_p, chn, buf_p, class_oid_p,
				     repr_p);
  if (error != NO_ERROR)
    {
      catalog_free_representation (repr_p);
      return error;
    }

  record_p->length = (int) (buf_p->ptr - buf_p->buffer);
  catalog_free_representation (repr_p);

  return NO_ERROR;
}

/*
 * ct_get_or_value_from_class_record () -
 *   return:
 *   record(in):
 */
static OR_VALUE *
catcls_get_or_value_from_class_record (THREAD_ENTRY * thread_p,
				       RECDES * record_p)
{
  OR_VALUE *value_p = NULL;
  OR_BUF *buf_p, repr_buffer;

  value_p = catcls_allocate_or_value (1);
  if (value_p == NULL)
    {
      return (NULL);
    }

  buf_p = &repr_buffer;
  or_init (buf_p, record_p->data, record_p->length);

  or_advance (buf_p, OR_HEADER_SIZE);
  if (catcls_get_or_value_from_class (thread_p, buf_p, value_p) != NO_ERROR)
    {
      catcls_free_or_value (value_p);
      return (NULL);
    }

  return (value_p);
}

/*
 * ct_get_or_value_from_record () -
 *   return:
 *   record(in):
 */
static OR_VALUE *
catcls_get_or_value_from_record (THREAD_ENTRY * thread_p, RECDES * record_p)
{
  OR_VALUE *value_p = NULL;
  OR_BUF *buf_p, repr_buffer;
  OID class_oid;
  REPR_ID repr_id;
  DISK_REPR *repr_p = NULL;
  int error;

  or_class_oid (record_p, &class_oid);
  error = catalog_get_last_representation_id (thread_p, &class_oid, &repr_id);
  if (error != NO_ERROR)
    {
      goto error;
    }
  else
    {
      repr_p = catalog_get_representation (thread_p, &class_oid, repr_id);
      if (repr_p == NULL)
	{
	  error = er_errid ();
	  goto error;
	}
    }

  value_p = catcls_allocate_or_value (1);
  if (value_p == NULL)
    {
      goto error;
    }
  else if (catcls_expand_or_value_by_repr (value_p, &class_oid, repr_p) !=
	   NO_ERROR)
    {
      goto error;
    }

  buf_p = &repr_buffer;
  or_init (buf_p, record_p->data, record_p->length);

  if (catcls_get_or_value_from_buffer (thread_p, buf_p, value_p, repr_p) !=
      NO_ERROR)
    {
      goto error;
    }

  catalog_free_representation (repr_p);
  return (value_p);

error:

  if (value_p)
    {
      catcls_free_or_value (value_p);
    }

  if (repr_p)
    {
      catalog_free_representation (repr_p);
    }

  return (NULL);
}

/*
 * ct_insert_subset () -
 *   return:
 *   value(in):
 *   root_oid(in):
 */
static int
catcls_insert_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
		      OID * root_oid_p)
{
  OR_VALUE *subset_p;
  int n_subset;
  OID *class_oid_p, oid;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  DB_SET *oid_set_p = NULL;
  DB_VALUE oid_val;
  int i;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;
  int error = NO_ERROR;

  subset_p = value_p->sub.value;
  n_subset = value_p->sub.count;

  if (n_subset == 0)
    {
      return NO_ERROR;
    }

  oid_set_p = set_create_sequence (n_subset);
  if (oid_set_p == NULL)
    {
      error = er_errid ();
      goto error;
    }

  class_oid_p = &subset_p[0].id.classoid;
  cls_info_p = catalog_get_class_info (thread_p, class_oid_p);
  if (cls_info_p == NULL)
    {
      error = er_errid ();
      goto error;
    }

  hfid_p = &cls_info_p->hfid;
  if (heap_scancache_start_modify (thread_p, &scan, hfid_p, class_oid_p,
				   MULTI_ROW_UPDATE) != NO_ERROR)
    {
      error = er_errid ();
      goto error;
    }

  is_scan_inited = true;

  for (i = 0; i < n_subset; i++)
    {
      error = catcls_insert_instance (thread_p, &subset_p[i], &oid,
				      root_oid_p, class_oid_p, hfid_p, &scan);
      if (error != NO_ERROR)
	{
	  goto error;
	}

      db_push_oid (&oid_val, &oid);
      error = set_put_element (oid_set_p, i, &oid_val);
      if (error != NO_ERROR)
	{
	  goto error;
	}
    }

  db_make_sequence (&value_p->value, oid_set_p);

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);

  return NO_ERROR;

error:

  if (oid_set_p)
    {
      set_free (oid_set_p);
    }

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  return error;
}

/*
 * ct_delete_subset () -
 *   return:
 *   value(in):
 */
static int
catcls_delete_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p)
{
  OR_VALUE *subset_p;
  int n_subset;
  OID *class_oid_p, *oid_p;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  DB_SET *oid_set_p;
  DB_VALUE oid_val;
  int i;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;
  int error = NO_ERROR;

  subset_p = value_p->sub.value;
  n_subset = value_p->sub.count;

  if (n_subset == 0)
    {
      return NO_ERROR;
    }

  oid_set_p = DB_GET_SET (&value_p->value);
  class_oid_p = &subset_p[0].id.classoid;

  cls_info_p = catalog_get_class_info (thread_p, class_oid_p);
  if (cls_info_p == NULL)
    {
      error = er_errid ();
      goto error;
    }

  hfid_p = &cls_info_p->hfid;
  if (heap_scancache_start_modify (thread_p, &scan, hfid_p, class_oid_p,
				   MULTI_ROW_DELETE) != NO_ERROR)
    {
      error = er_errid ();
      goto error;
    }

  is_scan_inited = true;

  for (i = 0; i < n_subset; i++)
    {
      error = set_get_element (oid_set_p, i, &oid_val);
      if (error != NO_ERROR)
	{
	  goto error;
	}

      oid_p = DB_GET_OID (&oid_val);
      error =
	catcls_delete_instance (thread_p, oid_p, class_oid_p, hfid_p, &scan);
      if (error != NO_ERROR)
	{
	  goto error;
	}
    }

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);

  return NO_ERROR;

error:

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  return error;
}

/*
 * ct_update_subset () -
 *   return:
 *   value(in):
 *   old_value(in):
 *   uflag(in):
 */
static int
catcls_update_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
		      OR_VALUE * old_value_p, int *update_flag_p)
{
  OR_VALUE *subset_p, *old_subset_p;
  int n_subset, n_old_subset;
  int n_min_subset;
  OID *class_oid_p;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  OID *oid_p, tmp_oid;
  DB_SET *old_oid_set_p = NULL;
  DB_VALUE oid_val;
  int i;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;
  int error = NO_ERROR;

  if ((value_p->sub.count > 0) &&
      ((old_value_p->sub.count < 0) || DB_IS_NULL (&old_value_p->value)))
    {
      old_oid_set_p = set_create_sequence (0);
      db_make_sequence (&old_value_p->value, old_oid_set_p);
      old_value_p->sub.count = 0;
    }

  subset_p = value_p->sub.value;
  n_subset = value_p->sub.count;

  old_subset_p = old_value_p->sub.value;
  n_old_subset = old_value_p->sub.count;

  n_min_subset = (n_subset > n_old_subset) ? n_old_subset : n_subset;

  if (subset_p != NULL)
    {
      class_oid_p = &subset_p[0].id.classoid;
    }
  else if (old_subset_p != NULL)
    {
      class_oid_p = &old_subset_p[0].id.classoid;
    }
  else
    {
      return NO_ERROR;
    }

  old_oid_set_p = DB_PULL_SET (&old_value_p->value);
  cls_info_p = catalog_get_class_info (thread_p, class_oid_p);
  if (cls_info_p == NULL)
    {
      error = er_errid ();
      goto error;
    }

  hfid_p = &cls_info_p->hfid;
  if (heap_scancache_start_modify (thread_p, &scan, hfid_p, class_oid_p,
				   MULTI_ROW_UPDATE) != NO_ERROR)
    {
      goto error;
    }

  is_scan_inited = true;

  /* update components */
  for (i = 0; i < n_min_subset; i++)
    {
      error = set_get_element (old_oid_set_p, i, &oid_val);
      if (error != NO_ERROR)
	{
	  goto error;
	}

      if (DB_VALUE_TYPE (&oid_val) != DB_TYPE_OID)
	{
	  goto error;
	}

      oid_p = DB_PULL_OID (&oid_val);
      error = catcls_update_instance (thread_p, &subset_p[i], oid_p,
				      class_oid_p, hfid_p, &scan);
      if (error != NO_ERROR)
	{
	  goto error;
	}
    }

  /* drop components */
  if (n_old_subset > n_subset)
    {
      for (i = n_old_subset - 1; i >= n_min_subset; i--)
	{
	  error = set_get_element (old_oid_set_p, i, &oid_val);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }

	  if (DB_VALUE_TYPE (&oid_val) != DB_TYPE_OID)
	    {
	      goto error;
	    }

	  oid_p = DB_PULL_OID (&oid_val);
	  error = catcls_delete_instance (thread_p, oid_p, class_oid_p,
					  hfid_p, &scan);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }

	  error = set_drop_seq_element (old_oid_set_p, i);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }
	}

      if (set_size (old_oid_set_p) == 0)
	{
	  pr_clear_value (&old_value_p->value);
	}

      *update_flag_p = true;
    }
  /* add components */
  else if (n_old_subset < n_subset)
    {
      OID root_oid = { NULL_PAGEID, NULL_SLOTID, NULL_VOLID };
      for (i = n_min_subset, oid_p = &tmp_oid; i < n_subset; i++)
	{
	  error = catcls_insert_instance (thread_p, &subset_p[i], oid_p,
					  &root_oid, class_oid_p, hfid_p,
					  &scan);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }

	  db_push_oid (&oid_val, oid_p);
	  error = set_add_element (old_oid_set_p, &oid_val);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }
	}
      *update_flag_p = true;
    }

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);

  return NO_ERROR;

error:

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  return error;
}

/*
 * ct_insert_instance () -
 *   return:
 *   value(in):
 *   oid(in):
 *   root_oid(in):
 *   class_oid(in):
 *   hfid(in):
 *   scan(in):
 */
static int
catcls_insert_instance (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
			OID * oid_p, OID * root_oid_p, OID * class_oid_p,
			HFID * hfid_p, HEAP_SCANCACHE * scan_p)
{
  RECDES record;
  OR_VALUE *attrs;
  OR_VALUE *subset_p, *attr_p;
  bool old;
  int i, j, k;
  bool is_lock_inited = false;
  int error = NO_ERROR;

  record.data = NULL;

  if (heap_assign_address_with_class_oid (thread_p, hfid_p, oid_p, 0,
					  class_oid_p) != NO_ERROR)
    {
      error = er_errid ();
      goto error;
    }

  is_lock_inited = true;

  if (OID_ISNULL (root_oid_p))
    {
      COPY_OID (root_oid_p, oid_p);
    }

  for (attrs = value_p->sub.value, i = 0; i < value_p->sub.count; i++)
    {
      if (IS_SUBSET (attrs[i]))
	{
	  /* set backward oid */
	  for (subset_p = attrs[i].sub.value,
	       j = 0; j < attrs[i].sub.count; j++)
	    {
	      /* assume that the attribute values of xxx are ordered by
	         { class_of, xxx_name, xxx_type, from_xxx_name, ... } */

	      attr_p = subset_p[j].sub.value;
	      db_push_oid (&attr_p[0].value, oid_p);

	      if (OID_EQ (class_oid_p, &ct_Class.classoid))
		{
		  /* if root node, eliminate self references */
		  for (k = 1; k < subset_p[j].sub.count; k++)
		    {
		      if (DB_VALUE_TYPE (&attr_p[k].value) == DB_TYPE_OID)
			{
			  if (OID_EQ (oid_p, DB_PULL_OID (&attr_p[k].value)))
			    {
			      db_value_put_null (&attr_p[k].value);
			    }
			}
		    }
		}
	    }

	  error = catcls_insert_subset (thread_p, &attrs[i], root_oid_p);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }
	}
      else if (DB_VALUE_DOMAIN_TYPE (&attrs[i].value) == DB_TYPE_VARIABLE)
	{
	  /* set a self referenced oid */
	  db_push_oid (&attrs[i].value, root_oid_p);
	}
    }

  error = catcls_reorder_attributes_by_repr (thread_p, value_p);
  if (error != NO_ERROR)
    {
      goto error;
    }

  record.length = catcls_guess_record_length (value_p);
  record.area_size = record.length;
  record.type = REC_HOME;
  record.data = (char *) malloc (record.length);

  if (record.data == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, record.length);
      goto error;
    }

  error = catcls_put_or_value_into_record (thread_p, value_p, 0, &record,
					   class_oid_p);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* for replication */
  if (locator_add_or_remove_index (thread_p, &record, oid_p, class_oid_p,
				   true, SINGLE_ROW_INSERT, scan_p,
				   false, false, hfid_p) != NO_ERROR)
    {
      error = er_errid ();
      goto error;
    }

  if (heap_update (thread_p, hfid_p, oid_p, &record, &old, scan_p) == NULL)
    {
      error = er_errid ();
      goto error;
    }

#if defined(SERVER_MODE)
  lock_unlock_object (thread_p, oid_p, class_oid_p, X_LOCK, false);
#endif /* SERVER_MODE */
  free_and_init (record.data);

  return NO_ERROR;

error:

#if defined(SERVER_MODE)
  if (is_lock_inited)
    {
      lock_unlock_object (thread_p, oid_p, class_oid_p, X_LOCK, false);
    }
#endif /* SERVER_MODE */

  if (record.data)
    {
      free_and_init (record.data);
    }

  return error;
}

/*
 * ct_delete_instance () -
 *   return:
 *   oid(in):
 *   class_oid(in):
 *   hfid(in):
 *   scan(in):
 */
static int
catcls_delete_instance (THREAD_ENTRY * thread_p, OID * oid_p,
			OID * class_oid_p, HFID * hfid_p,
			HEAP_SCANCACHE * scan_p)
{
  RECDES record;
  OR_VALUE *value_p = NULL;
  OR_VALUE *attrs;
  int i;
#if defined(SERVER_MODE)
  bool is_lock_inited = false;
#endif /* SERVER_MODE */
  int error = NO_ERROR;

  record.data = NULL;

#if defined(SERVER_MODE)
  if (lock_object (thread_p, oid_p, class_oid_p, X_LOCK, LK_UNCOND_LOCK) !=
      LK_GRANTED)
    {
      error = er_errid ();
      goto error;
    }
  is_lock_inited = true;
#endif /* SERVER_MODE */

  if (heap_get (thread_p, oid_p, &record, scan_p, COPY, NULL_CHN) !=
      S_SUCCESS)
    {
      error = er_errid ();
      goto error;
    }

  value_p = catcls_get_or_value_from_record (thread_p, &record);
  if (value_p == NULL)
    {
      error = er_errid ();
      goto error;
    }

  for (attrs = value_p->sub.value, i = 0; i < value_p->sub.count; i++)
    {
      if (IS_SUBSET (attrs[i]))
	{
	  error = catcls_delete_subset (thread_p, &attrs[i]);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }
	}
    }

  /* for replication */
  if (locator_add_or_remove_index (thread_p, &record, oid_p, class_oid_p,
				   false, SINGLE_ROW_DELETE, scan_p,
				   false, false, hfid_p) != NO_ERROR)
    {
      error = er_errid ();
      goto error;
    }

  if (heap_delete (thread_p, hfid_p, oid_p, scan_p) == NULL)
    {
      error = er_errid ();
      goto error;
    }

#if defined(SERVER_MODE)
  lock_unlock_object (thread_p, oid_p, class_oid_p, X_LOCK, false);
#endif /* SERVER_MODE */
  catcls_free_or_value (value_p);

  return NO_ERROR;

error:

#if defined(SERVER_MODE)
  if (is_lock_inited)
    {
      lock_unlock_object (thread_p, oid_p, class_oid_p, X_LOCK, false);
    }
#endif /* SERVER_MODE */

  if (value_p)
    {
      catcls_free_or_value (value_p);
    }

  return error;
}

/*
 * ct_update_instance () -
 *   return:
 *   value(in):
 *   oid(in):
 *   class_oid(in):
 *   hfid(in):
 *   scan(in):
 */
static int
catcls_update_instance (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
			OID * oid_p, OID * class_oid_p, HFID * hfid_p,
			HEAP_SCANCACHE * scan_p)
{
  RECDES record, old_record;
  OR_VALUE *old_value_p = NULL;
  OR_VALUE *attrs, *old_attrs;
  OR_VALUE *subset_p, *attr_p;
  int old_chn;
  int uflag = false;
  bool old;
  int i, j, k;
#if defined(SERVER_MODE)
  bool is_lock_inited = false;
#endif /* SERVER_MODE */
  int error = NO_ERROR;

  record.data = NULL;
  old_record.data = NULL;
#if defined(SERVER_MODE)
  if (lock_object (thread_p, oid_p, class_oid_p, X_LOCK, LK_UNCOND_LOCK) !=
      LK_GRANTED)
    {
      error = er_errid ();
      goto error;
    }
  is_lock_inited = true;
#endif /* SERVER_MODE */

  if (heap_get (thread_p, oid_p, &old_record, scan_p, COPY, NULL_CHN) !=
      S_SUCCESS)
    {
      error = er_errid ();
      goto error;
    }

  old_chn = or_chn (&old_record);
  old_value_p = catcls_get_or_value_from_record (thread_p, &old_record);
  if (old_value_p == NULL)
    {
      error = er_errid ();
      goto error;
    }

  error = catcls_reorder_attributes_by_repr (thread_p, value_p);
  if (error != NO_ERROR)
    {
      goto error;
    }

  /* update old_value */
  for (attrs = value_p->sub.value, old_attrs = old_value_p->sub.value,
       i = 0; i < value_p->sub.count; i++)
    {
      if (IS_SUBSET (attrs[i]))
	{
	  /* set backward oid */
	  for (subset_p = attrs[i].sub.value,
	       j = 0; j < attrs[i].sub.count; j++)
	    {
	      /* assume that the attribute values of xxx are ordered by
	         { class_of, xxx_name, xxx_type, from_xxx_name, ... } */
	      attr_p = subset_p[j].sub.value;
	      db_push_oid (&attr_p[0].value, oid_p);

	      if (OID_EQ (class_oid_p, &ct_Class.classoid))
		{
		  /* if root node, eliminate self references */
		  for (k = 1; k < subset_p[j].sub.count; k++)
		    {
		      if (DB_VALUE_TYPE (&attr_p[k].value) == DB_TYPE_OID)
			{
			  if (OID_EQ (oid_p, DB_PULL_OID (&attr_p[k].value)))
			    {
			      db_value_put_null (&attr_p[k].value);
			    }
			}
		    }
		}
	    }

	  error = catcls_update_subset (thread_p, &attrs[i], &old_attrs[i],
					&uflag);
	  if (error != NO_ERROR)
	    {
	      goto error;
	    }
	}
      else
	{
	  if (tp_value_compare (&old_attrs[i].value, &attrs[i].value, 1, 1) !=
	      DB_EQ)
	    {
	      pr_clear_value (&old_attrs[i].value);
	      pr_clone_value (&attrs[i].value, &old_attrs[i].value);
	      uflag = true;
	    }
	}
    }

  if (uflag == true)
    {
      record.length = catcls_guess_record_length (old_value_p);
      record.area_size = record.length;
      record.type = REC_HOME;
      record.data = (char *) malloc (record.length);

      if (record.data == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, record.length);
	  goto error;
	}

      error = catcls_put_or_value_into_record (thread_p, old_value_p,
					       old_chn + 1, &record,
					       class_oid_p);
      if (error != NO_ERROR)
	{
	  goto error;
	}

      /* give up setting updated attr info */
      if (locator_update_index (thread_p, &record, &old_record, NULL, 0,
				oid_p, class_oid_p,
				SINGLE_ROW_UPDATE, scan_p, false,
				false,
				REPL_INFO_TYPE_STMT_NORMAL) != NO_ERROR)
	{
	  error = er_errid ();
	  goto error;
	}

      if (heap_update (thread_p, hfid_p, oid_p, &record, &old, scan_p) ==
	  NULL)
	{
	  error = er_errid ();
	  goto error;
	}

      free_and_init (record.data);
    }

#if defined(SERVER_MODE)
  lock_unlock_object (thread_p, oid_p, class_oid_p, X_LOCK, false);
#endif /* SERVER_MODE */
  catcls_free_or_value (old_value_p);

  return NO_ERROR;

error:

#if defined(SERVER_MODE)
  if (is_lock_inited)
    {
      lock_unlock_object (thread_p, oid_p, class_oid_p, X_LOCK, false);
    }
#endif /* SERVER_MODE */

  if (record.data)
    {
      free_and_init (record.data);
    }

  if (old_value_p)
    {
      catcls_free_or_value (old_value_p);
    }

  return error;
}

/*
 * catcls_insert_catalog_classes () -
 *   return:
 *   record(in):
 */
int
catcls_insert_catalog_classes (THREAD_ENTRY * thread_p, RECDES * record_p)
{
  OR_VALUE *value_p = NULL;
  OID oid, *class_oid_p;
  OID root_oid = { NULL_PAGEID, NULL_SLOTID, NULL_VOLID };
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;

  value_p = catcls_get_or_value_from_class_record (thread_p, record_p);
  if (value_p == NULL)
    {
      goto error;
    }

  class_oid_p = &ct_Class.classoid;
  cls_info_p = catalog_get_class_info (thread_p, class_oid_p);
  if (cls_info_p == NULL)
    {
      goto error;
    }

  hfid_p = &cls_info_p->hfid;
  if (heap_scancache_start_modify (thread_p, &scan, hfid_p, class_oid_p,
				   SINGLE_ROW_UPDATE) != NO_ERROR)
    {
      goto error;
    }

  is_scan_inited = true;

  if (catcls_insert_instance (thread_p, value_p, &oid, &root_oid, class_oid_p,
			      hfid_p, &scan) != NO_ERROR)
    {
      goto error;
    }

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);
  catcls_free_or_value (value_p);

  return NO_ERROR;

error:

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  if (value_p)
    {
      catcls_free_or_value (value_p);
    }

  return ER_FAILED;
}

/*
 * catcls_delete_catalog_classes () -
 *   return:
 *   name(in):
 *   class_oid(in):
 */
int
catcls_delete_catalog_classes (THREAD_ENTRY * thread_p, const char *name_p,
			       OID * class_oid_p)
{
  OID oid, *ct_class_oid_p;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;

  if (catcls_find_oid_by_class_name (thread_p, name_p, &oid) != NO_ERROR)
    {
      goto error;
    }

  ct_class_oid_p = &ct_Class.classoid;
  cls_info_p = catalog_get_class_info (thread_p, ct_class_oid_p);
  if (cls_info_p == NULL)
    {
      goto error;
    }

  hfid_p = &cls_info_p->hfid;
  if (heap_scancache_start_modify (thread_p, &scan, hfid_p,
				   ct_class_oid_p,
				   SINGLE_ROW_DELETE) != NO_ERROR)
    {
      goto error;
    }

  is_scan_inited = true;

  if (catcls_delete_instance (thread_p, &oid, ct_class_oid_p, hfid_p, &scan)
      != NO_ERROR)
    {
      goto error;
    }

  if (csect_enter (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  if (catcls_remove_entry (class_oid_p) != NO_ERROR)
    {
      csect_exit (CSECT_CT_OID_TABLE);
      return ER_FAILED;
    }

  csect_exit (CSECT_CT_OID_TABLE);

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);

  return NO_ERROR;

error:

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  return ER_FAILED;
}

/*
 * catcls_update_catalog_classes () -
 *   return:
 *   name(in):
 *   record(in):
 */
int
catcls_update_catalog_classes (THREAD_ENTRY * thread_p, const char *name_p,
			       RECDES * record_p)
{
  OR_VALUE *value_p = NULL;
  OID oid, *class_oid_p;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;

  if (catcls_find_oid_by_class_name (thread_p, name_p, &oid) != NO_ERROR)
    {
      goto error;
    }

  if (OID_ISNULL (&oid))
    {
      return (catcls_insert_catalog_classes (thread_p, record_p));
    }

  value_p = catcls_get_or_value_from_class_record (thread_p, record_p);
  if (value_p == NULL)
    {
      goto error;
    }

  class_oid_p = &ct_Class.classoid;
  cls_info_p = catalog_get_class_info (thread_p, class_oid_p);
  if (cls_info_p == NULL)
    {
      goto error;
    }

  hfid_p = &cls_info_p->hfid;
  if (heap_scancache_start_modify (thread_p, &scan, hfid_p,
				   class_oid_p,
				   SINGLE_ROW_UPDATE) != NO_ERROR)
    {
      goto error;
    }

  is_scan_inited = true;

  if (catcls_update_instance (thread_p, value_p, &oid, class_oid_p, hfid_p,
			      &scan) != NO_ERROR)
    {
      goto error;
    }

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);
  catcls_free_or_value (value_p);

  return NO_ERROR;

error:

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  if (value_p)
    {
      catcls_free_or_value (value_p);
    }

  return ER_FAILED;
}

/*
 * catcls_compile_catalog_classes () -
 *   return:
 *   void(in):
 */
int
catcls_compile_catalog_classes (THREAD_ENTRY * thread_p)
{
  RECDES class_record;
  OID *class_oid_p, tmp_oid;
  const char *class_name_p;
  const char *attr_name_p;
  CT_ATTR *atts;
  int n_atts;
  int c, a, i;
  HEAP_SCANCACHE scan;

  /* check if an old version database */
  if (catcls_find_class_oid_by_class_name (thread_p, CT_CLASS_NAME, &tmp_oid)
      != NO_ERROR)
    {
      return ER_FAILED;
    }
  else if (OID_ISNULL (&tmp_oid))
    {
      /* no catalog classes */
      return NO_ERROR;
    }

  /* fill classoid and attribute ids for each meta catalog classes */
  for (c = 0; ct_Classes[c] != NULL; c++)
    {
      class_name_p = ct_Classes[c]->name;
      class_oid_p = &ct_Classes[c]->classoid;

      if (catcls_find_class_oid_by_class_name (thread_p, class_name_p,
					       class_oid_p) != NO_ERROR)
	{
	  return ER_FAILED;
	}

      atts = ct_Classes[c]->atts;
      n_atts = ct_Classes[c]->n_atts;

      if (heap_scancache_quick_start (&scan) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      if (heap_get (thread_p, class_oid_p, &class_record, &scan, PEEK,
		    NULL_CHN) != S_SUCCESS)
	{
	  return ER_FAILED;
	}

      for (i = 0; i < n_atts; i++)
	{
	  attr_name_p = or_get_attrname (&class_record, i);
	  if (attr_name_p == NULL)
	    {
	      (void) heap_scancache_end (thread_p, &scan);
	      return ER_FAILED;
	    }

	  for (a = 0; a < n_atts; a++)
	    {
	      if (strcmp (atts[a].name, attr_name_p) == 0)
		{
		  atts[a].id = i;
		  break;
		}
	    }
	}
      if (heap_scancache_end (thread_p, &scan) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  catcls_Enable = true;

  if (catcls_find_btid_of_class_name (thread_p, &catcls_Btid) != NO_ERROR)
    {
      return ER_FAILED;
    }

  if (catcls_initialize_class_oid_to_oid_hash_table (CATCLS_OID_TABLE_SIZE) !=
      NO_ERROR)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * vc_append_bytes(): append a string to string buffer
 *
 *   returns: on success, ptr to concatenated string. otherwise, NULL.
 *   old_string(IN/OUT): original string
 *   new_tail(IN): string to be appended
 *   new_tail_length(IN): length of the string to be appended
 *
 */
static VALCNV_BUFFER *
valcnv_append_bytes (VALCNV_BUFFER * buffer_p, const char *new_tail_p,
		     const size_t new_tail_length)
{
  size_t old_length;

  if (new_tail_p == NULL)
    {
      return buffer_p;
    }
  else if (buffer_p == NULL)
    {
      buffer_p = (VALCNV_BUFFER *) malloc (sizeof (VALCNV_BUFFER));
      if (buffer_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (VALCNV_BUFFER));
	  return NULL;
	}

      buffer_p->length = 0;
      buffer_p->bytes = NULL;
    }

  old_length = buffer_p->length;
  buffer_p->length += new_tail_length;

  buffer_p->bytes = (unsigned char *) realloc (buffer_p->bytes,
					       buffer_p->length);
  if (buffer_p->bytes == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, buffer_p->length);
      return NULL;
    }

  memcpy (&buffer_p->bytes[old_length], new_tail_p, new_tail_length);
  return buffer_p;
}

/*
 * vc_append_string(): append a string to string buffer
 *
 *   returns: on success, ptr to concatenated string. otherwise, NULL.
 *   old_string(IN/OUT): original string
 *   new_tail(IN): string to be appended
 *
 */
static VALCNV_BUFFER *
valcnv_append_string (VALCNV_BUFFER * buffer_p, const char *new_tail_p)
{
  return valcnv_append_bytes (buffer_p, new_tail_p, strlen (new_tail_p));
}

/*
 * vc_float_to_string(): append float value to string buffer
 *
 *   returns: on success, ptr to converted string. otherwise, NULL.
 *   buf(IN/OUT): buffer
 *   value(IN): floating point value which is to be converted
 *
 */
static VALCNV_BUFFER *
valcnv_convert_float_to_string (VALCNV_BUFFER * buffer_p, const float value)
{
  char tbuf[24];

  sprintf (tbuf, "%.17g", value);

#if defined(HPUX)
  /* workaround for HP's broken printf */
  if (strstr (tbuf, "++") || strstr (tbuf, "--"))
#else /* HPUX */
  if (strstr (tbuf, "Inf"))
#endif /* HPUX */
    {
      sprintf (tbuf, "%.17g", (value > 0 ? FLT_MAX : -FLT_MAX));
    }

  return valcnv_append_string (buffer_p, tbuf);
}

/*
 * vc_double_to_string(): append double value to string buffer
 *
 *   returns: on success, ptr to converted string. otherwise, NULL.
 *   buf : buffer
 *   value(IN): double value which is to be converted
 *
 */
static VALCNV_BUFFER *
valcnv_convert_double_to_string (VALCNV_BUFFER * buffer_p, const double value)
{
  char tbuf[24];

  sprintf (tbuf, "%.17g", value);

#if defined(HPUX)
  /* workaround for HP's broken printf */
  if (strstr (tbuf, "++") || strstr (tbuf, "--"))
#else /* HPUX */
  if (strstr (tbuf, "Inf"))
#endif /* HPUX */
    {
      sprintf (tbuf, "%.17g", (value > 0 ? DBL_MAX : -DBL_MAX));
    }

  return valcnv_append_string (buffer_p, tbuf);
}

/*
 * vc_bit_to_string(): append bit value to string buffer
 *
 *   returns: on success, ptr to converted string. otherwise, NULL.
 *   buf(IN/OUT): buffer
 *   value(IN): BIT value which is to be converted
 *
 */
static VALCNV_BUFFER *
valcnv_convert_bit_to_string (VALCNV_BUFFER * buffer_p,
			      const DB_VALUE * value_p)
{
  unsigned char *bit_string_p;
  int nibble_len, nibbles, count;
  char tbuf[10];

  bit_string_p = (unsigned char *) DB_PULL_STRING (value_p);
  nibble_len = (DB_GET_STRING_LENGTH (value_p) + 3) / 4;

  for (nibbles = 0, count = 0; nibbles < nibble_len - 1;
       count++, nibbles += 2)
    {
      sprintf (tbuf, "%02x", bit_string_p[count]);
      tbuf[2] = '\0';
      buffer_p = valcnv_append_string (buffer_p, tbuf);
      if (buffer_p == NULL)
	{
	  return NULL;
	}
    }

  if (nibbles < nibble_len)
    {
      sprintf (tbuf, "%1x", bit_string_p[count]);
      tbuf[1] = '\0';
      buffer_p = valcnv_append_string (buffer_p, tbuf);
      if (buffer_p == NULL)
	{
	  return NULL;
	}
    }

  return buffer_p;
}

/*
 * vc_set_to_string(): append set value to string buffer
 *
 *   returns: on success, ptr to converted string. otherwise, NULL.
 *   buf(IN/OUT): buffer
 *   set(IN): SET value which is to be converted
 *
 */
static VALCNV_BUFFER *
valcnv_convert_set_to_string (VALCNV_BUFFER * buffer_p, DB_SET * set_p)
{
  DB_VALUE value;
  int err, size, max_n, i;

  if (set_p == NULL)
    {
      return buffer_p;
    }

  buffer_p = valcnv_append_string (buffer_p, "{");
  if (buffer_p == NULL)
    {
      return NULL;
    }

  size = set_size (set_p);
  if (valcnv_Max_set_elements == 0)
    {
      max_n = size;
    }
  else
    {
      max_n = MIN (size, valcnv_Max_set_elements);
    }

  for (i = 0; i < max_n; i++)
    {
      err = set_get_element (set_p, i, &value);
      if (err < 0)
	{
	  return NULL;
	}

      buffer_p = valcnv_convert_db_value_to_string (buffer_p, &value);
      pr_clear_value (&value);
      if (i < size - 1)
	{
	  buffer_p = valcnv_append_string (buffer_p, ", ");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }
	}
    }

  if (i < size)
    {
      buffer_p = valcnv_append_string (buffer_p, "...");
      if (buffer_p == NULL)
	{
	  return NULL;
	}
    }

  buffer_p = valcnv_append_string (buffer_p, "}");
  if (buffer_p == NULL)
    {
      return NULL;
    }

  return buffer_p;
}

/*
 * vc_money_to_string(): append monetary value to string buffer
 *
 *   returns: on success, ptr to converted string. otherwise, NULL.
 *   value(IN): monetary value which is to be converted
 *
 */
static VALCNV_BUFFER *
valcnv_convert_money_to_string (const double value)
{
  char cbuf[LDBL_MAX_10_EXP + 20];	/* 20 == floating fudge factor */

  sprintf (cbuf, "%.2f", value);

#if defined(HPUX)
  /* workaround for HP's broken printf */
  if (strstr (cbuf, "++") || strstr (cbuf, "--"))
#else /* HPUX */
  if (strstr (cbuf, "Inf"))
#endif /* HPUX */
    {
      sprintf (cbuf, "%.2f", (value > 0 ? DBL_MAX : -DBL_MAX));
    }

  return valcnv_append_string (NULL, cbuf);
}

/*
 * vc_data_to_string(): append a value to string buffer
 *
 *   returns: on success, ptr to converted string. otherwise, NULL.
 *   buf(IN/OUT): buffer
 *   value(IN): a value which is to be converted
 *
 */
static VALCNV_BUFFER *
valcnv_convert_data_to_string (VALCNV_BUFFER * buffer_p,
			       const DB_VALUE * value_p)
{
  OID *oid_p;
  DB_SET *set_p;
  DB_ELO *elo_p;
  char *src_p, *end_p, *p;
  ptrdiff_t len;

  DB_MONETARY *money_p;
  VALCNV_BUFFER *money_string_p;
  const char *currency_symbol_p;
  double amount;

  char line[1025];

  int err;

  if (DB_IS_NULL (value_p))
    {
      buffer_p = valcnv_append_string (buffer_p, "NULL");
    }
  else
    {
      switch (DB_VALUE_TYPE (value_p))
	{
	case DB_TYPE_INTEGER:
	  sprintf (line, "%d", DB_GET_INTEGER (value_p));
	  buffer_p = valcnv_append_string (buffer_p, line);
	  break;

	case DB_TYPE_BIGINT:
	  sprintf (line, "%lld", (long long) DB_GET_BIGINT (value_p));
	  buffer_p = valcnv_append_string (buffer_p, line);
	  break;

	case DB_TYPE_SHORT:
	  sprintf (line, "%d", (int) DB_GET_SHORT (value_p));
	  buffer_p = valcnv_append_string (buffer_p, line);
	  break;

	case DB_TYPE_FLOAT:
	  buffer_p =
	    valcnv_convert_float_to_string (buffer_p, DB_GET_FLOAT (value_p));
	  break;

	case DB_TYPE_DOUBLE:
	  buffer_p =
	    valcnv_convert_double_to_string (buffer_p,
					     DB_GET_DOUBLE (value_p));
	  break;

	case DB_TYPE_NUMERIC:
	  buffer_p =
	    valcnv_append_string (buffer_p,
				  numeric_db_value_print ((DB_VALUE *)
							  value_p));
	  break;

	case DB_TYPE_BIT:
	case DB_TYPE_VARBIT:
	  buffer_p = valcnv_convert_bit_to_string (buffer_p, value_p);
	  break;

	case DB_TYPE_CHAR:
	case DB_TYPE_NCHAR:
	case DB_TYPE_VARCHAR:
	case DB_TYPE_VARNCHAR:
	  src_p = DB_PULL_STRING (value_p);
	  end_p = src_p + DB_GET_STRING_SIZE (value_p);
	  while (src_p < end_p)
	    {
	      for (p = src_p; p < end_p && *p != '\''; p++)
		{
		  ;
		}

	      if (p < end_p)
		{
		  len = p - src_p + 1;
		  buffer_p = valcnv_append_bytes (buffer_p, src_p, len);
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }

		  buffer_p = valcnv_append_string (buffer_p, "'");
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }
		}
	      else
		{
		  buffer_p =
		    valcnv_append_bytes (buffer_p, src_p, end_p - src_p);
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }
		}

	      src_p = p + 1;
	    }
	  break;

	case DB_TYPE_OID:
	  oid_p = (OID *) DB_PULL_OID (value_p);

	  sprintf (line, "%d", (int) oid_p->volid);
	  buffer_p = valcnv_append_string (buffer_p, line);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, "|");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  sprintf (line, "%d", (int) oid_p->pageid);
	  buffer_p = valcnv_append_string (buffer_p, line);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, "|");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  sprintf (line, "%d", (int) oid_p->slotid);
	  buffer_p = valcnv_append_string (buffer_p, line);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  break;

	case DB_TYPE_SET:
	case DB_TYPE_MULTI_SET:
	case DB_TYPE_SEQUENCE:
	  set_p = DB_PULL_SET (value_p);
	  if (set_p == NULL)
	    {
	      buffer_p = valcnv_append_string (buffer_p, "NULL");
	    }
	  else
	    {
	      return valcnv_convert_set_to_string (buffer_p, set_p);
	    }

	  break;

	case DB_TYPE_ELO:
	  elo_p = DB_PULL_ELO (value_p);
	  if (elo_p == NULL)
	    {
	      buffer_p = valcnv_append_string (buffer_p, "NULL");
	    }
	  else
	    {
	      if (elo_p->pathname != NULL)
		{
		  buffer_p = valcnv_append_string (buffer_p, elo_p->pathname);
		}
	      else if (elo_p->type == ELO_LO)
		{
		  sprintf (line, "%d", (int) elo_p->loid.vfid.volid);
		  buffer_p = valcnv_append_string (buffer_p, line);
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }

		  buffer_p = valcnv_append_string (buffer_p, "|");
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }

		  sprintf (line, "%d", (int) elo_p->loid.vfid.fileid);
		  buffer_p = valcnv_append_string (buffer_p, line);
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }

		  buffer_p = valcnv_append_string (buffer_p, "|");
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }

		  sprintf (line, "%d", (int) elo_p->loid.vpid.volid);
		  buffer_p = valcnv_append_string (buffer_p, line);
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }

		  buffer_p = valcnv_append_string (buffer_p, "|");
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }

		  sprintf (line, "%d", (int) elo_p->loid.vpid.pageid);
		  buffer_p = valcnv_append_string (buffer_p, line);
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }
		}
	      else
		{
		  sprintf (line, "%p", elo_p);
		  buffer_p = valcnv_append_string (buffer_p, line);
		  if (buffer_p == NULL)
		    {
		      return NULL;
		    }
		}
	    }
	  break;

	case DB_TYPE_TIME:
	  err = db_time_to_string (line, VALCNV_TOO_BIG_TO_MATTER,
				   DB_GET_TIME (value_p));
	  if (err == 0)
	    {
	      return NULL;
	    }
	  buffer_p = valcnv_append_string (buffer_p, line);
	  break;

	case DB_TYPE_TIMESTAMP:
	  err = db_timestamp_to_string (line, VALCNV_TOO_BIG_TO_MATTER,
					DB_GET_TIMESTAMP (value_p));
	  if (err == 0)
	    {
	      return NULL;
	    }
	  buffer_p = valcnv_append_string (buffer_p, line);
	  break;

	case DB_TYPE_DATETIME:
	  err = db_datetime_to_string (line, VALCNV_TOO_BIG_TO_MATTER,
				       DB_GET_DATETIME (value_p));
	  if (err == 0)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, line);
	  break;

	case DB_TYPE_DATE:
	  err = db_date_to_string (line, VALCNV_TOO_BIG_TO_MATTER,
				   DB_GET_DATE (value_p));
	  if (err == 0)
	    {
	      return NULL;
	    }
	  buffer_p = valcnv_append_string (buffer_p, line);
	  break;

	case DB_TYPE_MONETARY:
	  money_p = DB_GET_MONETARY (value_p);
	  OR_MOVE_DOUBLE (&money_p->amount, &amount);
	  money_string_p = valcnv_convert_money_to_string (amount);
	  if (money_string_p == NULL)
	    {
	      return NULL;
	    }

	  currency_symbol_p = lang_currency_symbol (money_p->type);
	  strncpy (line, currency_symbol_p, strlen (currency_symbol_p));
	  strncpy (line + strlen (currency_symbol_p),
		   (char *) money_string_p->bytes, money_string_p->length);
	  line[strlen (currency_symbol_p) + money_string_p->length] = '\0';

	  free_and_init (money_string_p->bytes);
	  free_and_init (money_string_p);
	  buffer_p = valcnv_append_string (buffer_p, line);
	  break;

	default:
	  break;
	}
    }

  return buffer_p;
}

/*
 * vc_db_value_to_string(): append a value to string buffer with a type prefix
 *
 *   returns: on success, ptr to converted string. otherwise, NULL.
 *   buf(IN/OUT): buffer
 *   value(IN): a value which is to be converted
 *
 */
static VALCNV_BUFFER *
valcnv_convert_db_value_to_string (VALCNV_BUFFER * buffer_p,
				   const DB_VALUE * value_p)
{
  if (DB_IS_NULL (value_p))
    {
      buffer_p = valcnv_append_string (buffer_p, "NULL");
    }
  else
    {
      switch (DB_VALUE_TYPE (value_p))
	{
	case DB_TYPE_CHAR:
	case DB_TYPE_VARCHAR:
	  buffer_p = valcnv_append_string (buffer_p, "'");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_convert_data_to_string (buffer_p, value_p);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, "'");
	  break;

	case DB_TYPE_DATE:
	  buffer_p = valcnv_append_string (buffer_p, "date '");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_convert_data_to_string (buffer_p, value_p);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, "'");
	  break;

	case DB_TYPE_TIME:
	  buffer_p = valcnv_append_string (buffer_p, "time '");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_convert_data_to_string (buffer_p, value_p);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, "'");
	  break;

	case DB_TYPE_TIMESTAMP:
	  buffer_p = valcnv_append_string (buffer_p, "timestamp '");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_convert_data_to_string (buffer_p, value_p);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, "'");
	  break;

	case DB_TYPE_DATETIME:
	  buffer_p = valcnv_append_string (buffer_p, "datetime '");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_convert_data_to_string (buffer_p, value_p);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, "'");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }
	  break;

	case DB_TYPE_NCHAR:
	case DB_TYPE_VARNCHAR:
	  buffer_p = valcnv_append_string (buffer_p, "N'");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_convert_data_to_string (buffer_p, value_p);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, "'");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }
	  break;

	case DB_TYPE_BIT:
	case DB_TYPE_VARBIT:
	  buffer_p = valcnv_append_string (buffer_p, "X'");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_convert_data_to_string (buffer_p, value_p);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, "'");
	  break;

	case DB_TYPE_ELO:
	  buffer_p = valcnv_append_string (buffer_p, "ELO'");
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_convert_data_to_string (buffer_p, value_p);
	  if (buffer_p == NULL)
	    {
	      return NULL;
	    }

	  buffer_p = valcnv_append_string (buffer_p, "'");
	  break;

	default:
	  buffer_p = valcnv_convert_data_to_string (buffer_p, value_p);
	  break;
	}
    }

  return buffer_p;
}

/*
 * valcnv_convert_value_to_string(): convert a value to a string type value
 *
 *   returns: on success, NO_ERROR. otherwise, ER_FAILED.
 *   value(IN/OUT): a value which is to be converted to string
 *                  Note that the value is cleaned up during conversion.
 *
 */
static int
valcnv_convert_value_to_string (DB_VALUE * value_p)
{
  VALCNV_BUFFER buffer = { 0, NULL };
  VALCNV_BUFFER *buf_p;
  DB_VALUE src_value;

  if (!DB_IS_NULL (value_p))
    {
      buf_p = &buffer;
      buf_p = valcnv_convert_db_value_to_string (buf_p, value_p);
      if (buf_p == NULL)
	{
	  return ER_FAILED;
	}

      DB_MAKE_VARCHAR (&src_value, DB_MAX_STRING_LENGTH,
		       (char *) buf_p->bytes, CAST_STRLEN (buf_p->length));

      pr_clear_value (value_p);
      (*(tp_String.setval)) (value_p, &src_value, true);

      pr_clear_value (&src_value);
      free_and_init (buf_p->bytes);
    }

  return NO_ERROR;
}
