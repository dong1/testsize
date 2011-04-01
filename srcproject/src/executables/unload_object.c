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
 * unload_object.c: Utility that emits database object definitions in database
 *               object loader format.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <ctype.h>

#include <sys/stat.h>
#if defined(WINDOWS)
#include <sys/timeb.h>
#include <time.h>
#include <direct.h>
#define	SIGALRM	14
#endif /* WINDOWS */

#include "utility.h"
#include "load_object.h"
#include "file_hash.h"
#include "db.h"
#include "glo_class.h"
#include "memory_hash.h"
#include "memory_alloc.h"
#include "locator_cl.h"
#include "locator_sr.h"
#include "schema_manager.h"
#include "heap_file.h"
#include "locator.h"
#include "slotted_page.h"
#include "transform_cl.h"
#include "object_accessor.h"
#include "set_object.h"

#include "message_catalog.h"
#include "server_interface.h"
#include "porting.h"
#include "unloaddb.h"

#include "system_parameter.h"
#include "transform.h"
#include "execute_schema.h"
#include "network_interface_cl.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define MARK_CLASS_REQUESTED(cl_no) \
  (class_requested[cl_no / 8] |= 1 << cl_no % 8)
#define MARK_CLASS_REFERENCED(cl_no) \
  (class_referenced[cl_no / 8] |= 1 << cl_no % 8)
#define MARK_CLASS_PROCESSED(cl_no) \
  (class_processed[cl_no / 8] |= 1 << cl_no % 8)
#define IS_CLASS_REQUESTED(cl_no) \
  (class_requested[cl_no / 8] & 1 << cl_no % 8)
#define IS_CLASS_REFERENCED(cl_no) \
  (class_referenced[cl_no / 8] & 1 << cl_no % 8)
#define IS_CLASS_PROCESSED(cl_no) \
  (class_processed[cl_no / 8] & 1 << cl_no % 8)

#define GAUGE_INTERVAL	1

static char *output_filename = NULL;

static int output_number = 0;

static FH_TABLE *obj_table = NULL;
static FH_TABLE *cl_table = NULL;

static char *class_requested = NULL;
static char *class_referenced = NULL;
static char *class_processed = NULL;

static OID null_oid;

static const char *prohibited_classes[] = {
  "db_authorizations",		/* old name for db_root */
  "db_root",
  "db_user",
  "db_authorization",
  "db_password",
  "db_trigger",
  "db_serial",
  "db_ha_apply_info",
  /* catalog classes */
  CT_CLASS_NAME,
  CT_ATTRIBUTE_NAME,
  CT_DOMAIN_NAME,
  CT_METHOD_NAME,
  CT_METHSIG_NAME,
  CT_METHARG_NAME,
  CT_METHFILE_NAME,
  CT_QUERYSPEC_NAME,
  CT_RESOLUTION_NAME,		/* currently, not implemented */
  CT_INDEX_NAME,
  CT_INDEXKEY_NAME,
  CT_CLASSAUTH_NAME,
  CT_DATATYPE_NAME,
  CT_STORED_PROC_NAME,
  CT_STORED_PROC_ARGS_NAME,
  CT_PARTITION_NAME,
  CT_CLUSTER_NODE_NAME,
  /* catalog vclasses */
  CTV_CLASS_NAME,
  CTV_SUPER_CLASS_NAME,
  CTV_VCLASS_NAME,
  CTV_ATTRIBUTE_NAME,
  CTV_ATTR_SD_NAME,
  CTV_METHOD_NAME,
  CTV_METHARG_NAME,
  CTV_METHARG_SD_NAME,
  CTV_METHFILE_NAME,
  CTV_INDEX_NAME,
  CTV_INDEXKEY_NAME,
  CTV_AUTH_NAME,
  CTV_TRIGGER_NAME,
  CTV_STORED_PROC_NAME,
  CTV_STORED_PROC_ARGS_NAME,
  CTV_PARTITION_NAME,
  NULL
};

static int class_objects = 0;
static int total_objects = 0;
static int failed_objects = 0;

static int approximate_class_objects = 0;
static char *gauge_class_name;
static int total_approximate_class_objects = 0;


#define OBJECT_SUFFIX "_objects"

#define HEADER_FORMAT 	"-------------------------------+--------------------------------\n""    %-25s  |  %23s \n""-------------------------------+--------------------------------\n"
#define MSG_FORMAT 		"    %-25s  |  %10d (%3d%% / %3d%%)"
static FILE *unloadlog_file = NULL;


static int get_estimated_objs (HFID * hfid, int *est_objects);
static int set_referenced_subclasses (DB_OBJECT * class_);
static bool check_referenced_domain (DB_DOMAIN * dom_list, bool set_cls_ref,
				     int *num_cls_refp);
static void extractobjects_cleanup (void);
static void extractobjects_term_handler (int sig);
static bool mark_referenced_domain (SM_CLASS * class_ptr, int *num_set);
static void gauge_alarm_handler (int sig);
static int process_class (int cl_no);
static int process_object (DESC_OBJ * desc_obj, OID * obj_oid,
			   int referenced_class);
static int process_set (DB_SET * set);
static int process_value (DB_VALUE * value);
static void update_hash (OID * object_oid, OID * class_oid, int *data);
static DB_OBJECT *is_class (OID * obj_oid, OID * class_oid);
static int all_classes_processed (void);

/*
 * get_estimated_objs - get the estimated number of object reside in file heap
 *    return: NO_ERROR if success, error code otherwise
 *    hfid(in): file heap id
 *    est_objects(out): estimated number of object
 */
static int
get_estimated_objs (HFID * hfid, int *est_objects)
{
  int ignore_npages;
  int nobjs = 0;
  int error = NO_ERROR;

  error = heap_get_class_num_objects_pages (hfid, 1, &nobjs, &ignore_npages);
  if (error < 0)
    return error;

  *est_objects += nobjs;

  return nobjs;
}

/*
 * set_referenced_subclasses - set class as referenced
 *    return: NO_ERROR, if successful, error number, if not successful.
 *    class(in): root class
 * Note:
 *    CURRENTLY, ALWAYS RETURN NO_ERROR
 */
static int
set_referenced_subclasses (DB_OBJECT * class_)
{
  int error = NO_ERROR;
  int *cls_no_ptr;
  SM_CLASS *class_ptr;
  DB_OBJLIST *u;
  bool check_reference_chain = false;
  int num_set;
  int error_code;

  error_code = fh_get (cl_table, ws_oid (class_), (FH_DATA *) (&cls_no_ptr));
  if (error_code == NO_ERROR && cls_no_ptr != NULL)
    {
      if (input_filename)
	{
	  if (include_references || is_req_class (class_))
	    {
	      if (!IS_CLASS_REFERENCED (*cls_no_ptr)
		  && !IS_CLASS_REQUESTED (*cls_no_ptr))
		{
		  check_reference_chain = true;
		}
	      MARK_CLASS_REFERENCED (*cls_no_ptr);
	    }
	}
      else
	{
	  if (!IS_CLASS_REFERENCED (*cls_no_ptr)
	      && !IS_CLASS_REQUESTED (*cls_no_ptr))
	    {
	      check_reference_chain = true;
	    }
	  MARK_CLASS_REFERENCED (*cls_no_ptr);
	}
    }
  else
    {
#if defined(CUBRID_DEBUG)
      fprintf (stdout, "cls_no_ptr is NULL\n");
#endif /* CUBRID_DEBUG */
    }

  ws_find (class_, (MOBJ *) & class_ptr);
  if (class_ptr == NULL)
    {
      goto exit_on_error;
    }

  if (check_reference_chain == true)
    {
      mark_referenced_domain (class_ptr, &num_set);
    }

  /* dive to the bottom */
  for (u = class_ptr->users; u != NULL && error == NO_ERROR; u = u->next)
    {
      error = set_referenced_subclasses (u->op);
    }

exit_on_end:
  return error;

exit_on_error:
  CHECK_EXIT_ERROR (error);
  goto exit_on_end;
}


/*
 * check_referenced_domain - check for OBJECT domain as referenced
 *    return: true, if found referened OBJECT domain. false, if not
 *            found referenced OBJECT domain.
 *    dom_list(in): domain list of class attributes
 *    set_cls_ref(in): for true, do checking. for false, do marking
 *    num_cls_refp(out): number of referenced classes.
 * Note:
 *    for referenced CLASS domain, mark the CLASS and set the number of
 *    referenced classes.
 */
static bool
check_referenced_domain (DB_DOMAIN * dom_list,
			 bool set_cls_ref, int *num_cls_refp)
{
  bool found_object_dom;
  DB_DOMAIN *dom;
  DB_TYPE type;
  DB_OBJECT *class_;

  found_object_dom = false;	/* init */

  for (dom = dom_list; dom && !found_object_dom; dom = db_domain_next (dom))
    {
      type = db_domain_type (dom);
      switch (type)
	{
	case DB_TYPE_OBJECT:
	  class_ = db_domain_class (dom);
	  if (class_ == NULL)
	    {
	      return true;	/* found object domain */
	    }

	  *num_cls_refp += 1;	/* increase number of reference to class */

	  if (set_cls_ref)
	    {
	      if (set_referenced_subclasses (class_) != NO_ERROR)
		{
		  /* cause error - currently, not happened */
		  return true;
		}
	    }
	  break;
	case DB_TYPE_SET:
	case DB_TYPE_MULTISET:
	case DB_TYPE_SEQUENCE:
	  found_object_dom = check_referenced_domain (db_domain_set (dom),
						      set_cls_ref,
						      num_cls_refp);
	  break;
	default:
	  break;
	}
    }
  return found_object_dom;
}


/*
 * extractobjects_cleanup - do cleanup task
 *    return: void
 */
static void
extractobjects_cleanup (void)
{
  if (obj_out)
    {
      if (obj_out->buffer != NULL)
	free_and_init (obj_out->buffer);
      if (obj_out->fp != NULL)
	fclose (obj_out->fp);
    }

  if (obj_table != NULL)
    {
      if (debug_flag)
	{
	  fh_dump (obj_table);
	}
      fh_destroy (obj_table);
    }
  if (cl_table != NULL)
    fh_destroy (cl_table);

  free_and_init (output_filename);
  free_and_init (class_requested);
  free_and_init (class_referenced);
  free_and_init (class_processed);
  return;
}

/*
 * extractobjects_term_handler - extractobject terminate handler
 *    return: void
 *    sig(in): not used
 */
static void
extractobjects_term_handler (int sig)
{
  extractobjects_cleanup ();
  /* terminate a program */
  _exit (1);
}

/*
 * mark_referenced_domain - mark given SM_CLASS closure
 *    return: true if no error, false otherwise
 *    class_ptr(in): SM_CLASS
 *    num_set(out): amortized marking number of SM_CLASS closure
 */
static bool
mark_referenced_domain (SM_CLASS * class_ptr, int *num_set)
{
  SM_ATTRIBUTE *attribute;

  if (class_ptr == NULL)
    return true;

  for (attribute = class_ptr->shared; attribute != NULL;
       attribute = (SM_ATTRIBUTE *) attribute->header.next)
    {
      if (check_referenced_domain (attribute->domain, true /* do marking */ ,
				   num_set) != false)
	{
	  return false;
	}
    }

  for (attribute = class_ptr->class_attributes; attribute != NULL;
       attribute = (SM_ATTRIBUTE *) attribute->header.next)
    {
      if (check_referenced_domain (attribute->domain, true /* do marking */ ,
				   num_set) != false)
	{
	  return false;
	}
    }

  for (attribute = class_ptr->ordered_attributes;
       attribute; attribute = attribute->order_link)
    {
      if (attribute->header.name_space != ID_ATTRIBUTE)
	{
	  continue;
	}
      if (check_referenced_domain (attribute->domain, true /* do marking */ ,
				   num_set) != false)
	{
	  return false;
	}
    }
  return true;
}


/*
 * extractobjects - dump the database in loader format.
 *    return: 0 for success. 1 for error
 *    exec_name(in): utility name
 */
int
extractobjects (const char *exec_name)
{
  int i;
  HFID *hfid;
  int est_objects = 0;
  int cache_size;
  SM_CLASS *class_ptr;
  const char **cptr;
  int status = 0;
  int num_unload_classes = 0;
  DB_OBJECT **unload_class_table = NULL;
  bool has_obj_ref;
  int num_cls_ref;
  SM_ATTRIBUTE *attribute;
  void (*prev_intr_handler) (int sig);
  void (*prev_term_handler) (int sig);
#if !defined (WINDOWS)
  void (*prev_quit_handler) (int sig);
#endif
  LOG_LSA lsa;
  char unloadlog_filename[PATH_MAX];

  /* register new signal handlers */
  prev_intr_handler =
    os_set_signal_handler (SIGINT, extractobjects_term_handler);
  prev_term_handler =
    os_set_signal_handler (SIGTERM, extractobjects_term_handler);
#if !defined(WINDOWS)
  prev_quit_handler =
    os_set_signal_handler (SIGQUIT, extractobjects_term_handler);
#endif

  if (cached_pages <= 0)
    {
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_UNLOADDB,
				       UNLOADDB_MSG_INVALID_CACHED_PAGES));
      return 1;
    }
  if (page_size < (ssize_t) (sizeof (OID) + sizeof (int)))
    {
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_UNLOADDB,
				       UNLOADDB_MSG_INVALID_CACHED_PAGE_SIZE));
      return 1;
    }

  /*
   * Open output file
   */
  if (output_dirname == NULL)
    output_dirname = ".";
  if (strlen (output_dirname) > PATH_MAX - 8)
    {
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_UNLOADDB,
				       UNLOADDB_MSG_INVALID_DIR_NAME));
      return 1;
    }

  output_filename = (char *) malloc (PATH_MAX);
  if (output_filename == NULL)
    return 1;

  snprintf (output_filename, PATH_MAX - 1, "%s/%s%s",
	    output_dirname, output_prefix, OBJECT_SUFFIX);
  if ((obj_out->fp = fopen_ex (output_filename, "wb")) == NULL)
    {				/* binary */
      fprintf (stderr, "%s: %s.\n\n", exec_name, strerror (errno));
      free_and_init (output_filename);
      return errno;
    }

  {
    struct stat stbuf;
    int blksize;

    blksize = 4096;		/* init */
    if (stat (output_dirname, &stbuf) == -1)
      {
	;			/* nop */
      }
    else
      {
#if defined (WINDOWS)
	blksize = 4096;
#else /* !WINDOWS */
	blksize = stbuf.st_blksize;
#endif /* WINDOWS */
      }

    /*
     * Determine the IO buffer size by specifying a multiple of the
     * natural block size for the device.
     * NEED FUTURE OPTIMIZATION
     */
    obj_out->iosize = 1024 * 1024;	/* 1 Mbyte */
    obj_out->iosize -= (obj_out->iosize % blksize);

    obj_out->buffer = (char *) malloc (obj_out->iosize);

    obj_out->ptr = obj_out->buffer;	/* init */
    obj_out->count = 0;		/* init */
  }

  /*
   * The user indicates which classes are to be processed by
   * using -i with a file that contains a list of classes.
   * If the -i option is not used, it means process all classes.
   * Thus if input_filename is null, it means process all classes.
   * Three bit arrays are allocated to indicate whether a class
   * is requested, is referenced or is processed.  The index
   * into these arrays is the same as the index into class_table->mops.
   */
  if ((unload_class_table =
       (DB_OBJECT **) malloc (DB_SIZEOF (void *) * class_table->num)) == NULL)
    {
      status = 1;
      goto end;
    }
  for (i = 0; i < class_table->num; ++i)
    {
      unload_class_table[i] = NULL;
    }
  if ((class_requested =
       (char *) malloc ((class_table->num + 7) / 8)) == NULL)
    {
      status = 1;
      goto end;
    }
  if ((class_referenced =
       (char *) malloc ((class_table->num + 7) / 8)) == NULL)
    {
      status = 1;
      goto end;
    }
  if ((class_processed =
       (char *) malloc ((class_table->num + 7) / 8)) == NULL)
    {
      status = 1;
      goto end;
    }

  memset (class_requested, 0, (class_table->num + 7) / 8);
  memset (class_referenced, 0, (class_table->num + 7) / 8);
  memset (class_processed, 0, (class_table->num + 7) / 8);

  /*
   * Create the class hash table
   * Its purpose is to hash a class OID to the index into the
   * class_table->mops array.
   */
  cl_table = fh_create ("class hash", 4096, 1024, 4,
			NULL, FH_OID_KEY, DB_SIZEOF (int),
			oid_hash, oid_compare_equals);
  if (cl_table == NULL)
    {
      status = 1;
      goto end;
    }

  has_obj_ref = false;		/* init */
  num_cls_ref = 0;		/* init */

  /*
   * Total the number of objects & mark requested classes.
   */
#if defined(CUBRID_DEBUG)
  fprintf (stdout, "----- all class dump -----\n");
#endif /* CUBRID_DEBUG */
  for (i = 0; i < class_table->num; i++)
    {
      if (!WS_MARKED_DELETED (class_table->mops[i]) &&
	  class_table->mops[i] != sm_Root_class_mop)
	{
	  ws_find (class_table->mops[i], (MOBJ *) & class_ptr);
	  if (class_ptr == NULL)
	    {
	      status = 1;
	      goto end;
	    }

	  for (cptr = prohibited_classes; *cptr; ++cptr)
	    {
	      if (strcmp (*cptr, class_ptr->header.name) == 0)
		break;
	    }
	  if (*cptr == NULL)
	    {
#if defined(CUBRID_DEBUG)
	      fprintf (stdout, "%s%s%s\n",
		       PRINT_IDENTIFIER (class_ptr->header.name));
#endif /* CUBRID_DEBUG */

	      fh_put (cl_table, ws_oid (class_table->mops[i]), &i);
	      if (input_filename)
		{
		  if (is_req_class (class_table->mops[i])
		      || (!required_class_only
			  && sm_is_system_class (class_table->mops[i])))
		    MARK_CLASS_REQUESTED (i);
		}
	      else
		MARK_CLASS_REQUESTED (i);

	      if (!required_class_only || IS_CLASS_REQUESTED (i))
		{
		  if (text_print (obj_out, NULL, 0, "%cid %s%s%s %d\n", '%',
				  PRINT_IDENTIFIER (class_ptr->header.name),
				  i) != NO_ERROR)
		    {
		      status = 1;
		      goto end;
		    }
		}
	      if (IS_CLASS_REQUESTED (i))
		{
		  if (!has_obj_ref)
		    {		/* not found object domain */
		      for (attribute = class_ptr->shared; attribute != NULL;
			   attribute =
			   (SM_ATTRIBUTE *) attribute->header.next)
			{
			  /* false -> don't set */
			  if ((has_obj_ref =
			       check_referenced_domain (attribute->domain,
							false,
							&num_cls_ref)) ==
			      true)
			    {
#if defined(CUBRID_DEBUG)
			      fprintf (stdout,
				       "found OBJECT domain: %s%s%s->%s\n",
				       PRINT_IDENTIFIER (class_ptr->header.
							 name),
				       db_attribute_name (attribute));
#endif /* CUBRID_DEBUG */
			      break;
			    }
			}
		    }

		  if (!has_obj_ref)
		    {		/* not found object domain */
		      for (attribute = class_ptr->class_attributes;
			   attribute != NULL;
			   attribute =
			   (SM_ATTRIBUTE *) attribute->header.next)
			{
			  /* false -> don't set */
			  if ((has_obj_ref =
			       check_referenced_domain (attribute->domain,
							false,
							&num_cls_ref)) ==
			      true)
			    {
#if defined(CUBRID_DEBUG)
			      fprintf (stdout,
				       "found OBJECT domain: %s%s%s->%s\n",
				       PRINT_IDENTIFIER (class_ptr->header.
							 name),
				       db_attribute_name (attribute));
#endif /* CUBRID_DEBUG */
			      break;
			    }
			}
		    }

		  if (!has_obj_ref)
		    {		/* not found object domain */
		      for (attribute = class_ptr->ordered_attributes;
			   attribute; attribute = attribute->order_link)
			{
			  if (attribute->header.name_space != ID_ATTRIBUTE)
			    {
			      continue;
			    }
			  has_obj_ref =
			    check_referenced_domain (attribute->domain, false
						     /* don't set */ ,
						     &num_cls_ref);
			  if (has_obj_ref == true)
			    {
#if defined(CUBRID_DEBUG)
			      fprintf (stdout,
				       "found OBJECT domain: %s%s%s->%s\n",
				       PRINT_IDENTIFIER (class_ptr->header.
							 name),
				       db_attribute_name (attribute));
#endif /* CUBRID_DEBUG */
			      break;
			    }
			}
		    }
		  unload_class_table[num_unload_classes] =
		    class_table->mops[i];
		  num_unload_classes++;
		}

	      hfid = sm_heap ((MOBJ) class_ptr);
	      if (!HFID_IS_NULL (hfid))
		{
		  if (get_estimated_objs (hfid, &est_objects) < 0)
		    {
		      status = 1;
		      goto end;
		    }
		}
	    }
	}
    }

  OR_PUT_NULL_OID (&null_oid);

#if defined(CUBRID_DEBUG)
  fprintf (stdout, "has_obj_ref = %d, num_cls_ref = %d\n",
	   has_obj_ref, num_cls_ref);
#endif /* CUBRID_DEBUG */

  if (has_obj_ref || num_cls_ref > 0)
    {				/* found any referenced domain */
      int num_set;

      num_set = 0;		/* init */

      for (i = 0; i < class_table->num; i++)
	{
	  if (!IS_CLASS_REQUESTED (i))
	    {
	      continue;
	    }

	  /* check for emptyness, but not implemented
	   * NEED FUTURE WORk
	   */

	  if (has_obj_ref)
	    {
	      MARK_CLASS_REFERENCED (i);
	      continue;
	    }

	  ws_find (class_table->mops[i], (MOBJ *) & class_ptr);
	  if (class_ptr == NULL)
	    {
	      status = 1;
	      goto end;
	    }

	  if (mark_referenced_domain (class_ptr, &num_set) == false)
	    {
	      status = 1;
	      goto end;
	    }


	}			/* for (i = 0; i < class_table->num; i++) */

      if (has_obj_ref)
	{
	  ;			/* nop */
	}
      else
	{
	  if (num_cls_ref != num_set)
	    {
#if defined(CUBRID_DEBUG)
	      fprintf (stdout, "num_cls_ref = %d, num_set = %d\n",
		       num_cls_ref, num_set);
#endif /* CUBRID_DEBUG */
	      status = 1;
	      goto end;
	    }
	}
    }

#if defined(CUBRID_DEBUG)
  {
    int total_req_cls = 0;
    int total_ref_cls = 0;

    fprintf (stdout, "----- referenced class dump -----\n");
    for (i = 0; i < class_table->num; i++)
      {
	if (!IS_CLASS_REQUESTED (i))
	  {
	    continue;
	  }
	total_req_cls++;
	if (IS_CLASS_REFERENCED (i))
	  {
	    ws_find (class_table->mops[i], (MOBJ *) & class_ptr);
	    if (class_ptr == NULL)
	      {
		status = 1;
		goto end;
	      }
	    fprintf (stdout, "%s%s%s\n",
		     PRINT_IDENTIFIER (class_ptr->header.name));
	    total_ref_cls++;
	  }
      }
    fprintf (stdout,
	     "class_table->num = %d, total_req_cls = %d, total_ref_cls = %d\n",
	     class_table->num, total_req_cls, total_ref_cls);
  }
#endif /* CUBRID_DEBUG */

  /*
   * Lock all unloaded classes with S_LOCK
   */
  if (locator_fetch_set (num_unload_classes, unload_class_table,
			 DB_FETCH_QUERY_READ, DB_FETCH_QUERY_READ,
			 true) == NULL)
    {
      status = 1;
      goto end;
    }

  locator_get_append_lsa (&lsa);

  /*
   * Estimate the number of objects.
   */

  if (est_size == 0)
    {
      est_size = est_objects;
    }

  cache_size = cached_pages * page_size / (DB_SIZEOF (OID) + DB_SIZEOF (int));
  est_size = est_size > cache_size ? est_size : cache_size;

  /*
   * Create the hash table
   */
  if (has_obj_ref || num_cls_ref > 0)
    {				/* found any referenced domain */
      obj_table = fh_create ("object hash", est_size, page_size, cached_pages,
			     hash_filename, FH_OID_KEY, DB_SIZEOF (int),
			     oid_hash, oid_compare_equals);

      if (obj_table == NULL)
	{
	  status = 1;
	  goto end;
	}
    }

  /*
   * Dump the object definitions
   */
  total_approximate_class_objects = est_objects;
  snprintf (unloadlog_filename, sizeof (unloadlog_filename) - 1,
	    "%s_unloaddb.log", output_prefix);
  unloadlog_file = fopen (unloadlog_filename, "w+");
  if (unloadlog_file != NULL)
    {
      fprintf (unloadlog_file, HEADER_FORMAT, "Class Name",
	       "Total Instances");
    }
  if (verbose_flag)
    {
      fprintf (stdout, HEADER_FORMAT, "Class Name", "Total Instances");
    }

  do
    {
      for (i = 0; i < class_table->num; i++)
	{
	  if (!WS_MARKED_DELETED (class_table->mops[i]) &&
	      class_table->mops[i] != sm_Root_class_mop)
	    if (process_class (i) != NO_ERROR)
	      {
		if (!ignore_err_flag)
		  {
		    status = 1;
		    goto end;
		  }
	      }
	}
    }
  while (!all_classes_processed ());

  if (failed_objects != 0)
    {
      status = 1;
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_UNLOADDB,
				       UNLOADDB_MSG_OBJECTS_FAILED),
	       total_objects - failed_objects, total_objects);
      if (unloadlog_file != NULL)
	{
	  fprintf (unloadlog_file, msgcat_message (MSGCAT_CATALOG_UTILS,
						   MSGCAT_UTIL_SET_UNLOADDB,
						   UNLOADDB_MSG_OBJECTS_FAILED),
		   total_objects - failed_objects, total_objects);
	}
    }
  else if (verbose_flag)
    {
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_UNLOADDB,
				       UNLOADDB_MSG_OBJECTS_DUMPED),
	       total_objects);
    }

  if (unloadlog_file != NULL)
    {
      if (failed_objects == 0)
	{
	  fprintf (unloadlog_file, msgcat_message (MSGCAT_CATALOG_UTILS,
						   MSGCAT_UTIL_SET_UNLOADDB,
						   UNLOADDB_MSG_OBJECTS_DUMPED),
		   total_objects);
	}
      fprintf (unloadlog_file, msgcat_message (MSGCAT_CATALOG_UTILS,
					       MSGCAT_UTIL_SET_UNLOADDB,
					       UNLOADDB_MSG_LOG_LSA),
	       lsa.pageid, lsa.offset);
    }
  /* flush remaining buffer */
  if (text_print_flush (obj_out) != NO_ERROR)
    {
      status = 1;
    }

/* in case of both normal and error */
end:
  if (unloadlog_file != NULL)
    {
      fclose (unloadlog_file);
    }
  /*
   * Cleanup
   */
  free_and_init (unload_class_table);
  extractobjects_cleanup ();

  /* restore previous signal handlers */
  (void) os_set_signal_handler (SIGINT, prev_intr_handler);
  (void) os_set_signal_handler (SIGTERM, prev_term_handler);
#if !defined (WINDOWS)
  (void) os_set_signal_handler (SIGQUIT, prev_quit_handler);
#endif

  return (status);
}


/*
 * gauge_alarm_handler - signal handler
 *    return: void
 *    sig(in): singal number
 */
static void
gauge_alarm_handler (int sig)
{
  if (sig == SIGALRM)
    {
      fprintf (stdout, MSG_FORMAT "\r",
	       gauge_class_name, class_objects,
	       (class_objects > 0
		&& approximate_class_objects >=
		class_objects) ? (int) (100 * ((float) class_objects /
					       (float)
					       approximate_class_objects)) :
	       100,
	       (int) (100 *
		      ((float) total_objects /
		       (float) total_approximate_class_objects)));
      fflush (stdout);
    }
  else
    {
      ;
    }
#if !defined(WINDOWS)
  alarm (GAUGE_INTERVAL);
#endif
  return;
}


/*
 * process_class - dump one class in loader format
 *    return: NO_ERROR, if successful, error number, if not successful.
 *    cl_no(in): class object index for class_table
 */
static int
process_class (int cl_no)
{
  int error = NO_ERROR;
  DB_OBJECT *class_ = class_table->mops[cl_no];
  int i = 0;
  int v = 0;
  SM_CLASS *class_ptr;
  SM_ATTRIBUTE *attribute;
  LC_COPYAREA *fetch_area;	/* Area where objects are received */
  HFID *hfid;
  OID *class_oid;
  OID last_oid;
  LOCK lock = S_LOCK;		/* Lock to acquire for the above purpose */
  int nobjects, nfetched;
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in area */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area        */
  RECDES recdes;		/* Record descriptor */
  DESC_OBJ *desc_obj = NULL;	/* The object described by obj       */
  int requested_class = 0;
  int referenced_class = 0;
  void (*prev_handler) (int sig) = NULL;
  unsigned int prev_alarm = 0;
#if defined(WINDOWS)
  struct _timeb timebuffer;
  time_t start = 0;
#endif
  int total;

  /*
   * Only process classes that were requested or classes that were
   * referenced via requested classes.
   */
  if (IS_CLASS_PROCESSED (cl_no))
    {
      goto exit_on_end;		/* do nothing successfully */
    }

  if (IS_CLASS_REQUESTED (cl_no))
    requested_class = 1;
  if (IS_CLASS_REFERENCED (cl_no))
    referenced_class = 1;

  if (!requested_class && !referenced_class)
    {
      goto exit_on_end;		/* do nothing successfully */
    }

  class_objects = 0;
  MARK_CLASS_PROCESSED (cl_no);

  /* Get the class data */
  ws_find (class_, (MOBJ *) & class_ptr);
  if (class_ptr == NULL)
    {
      goto exit_on_error;
    }

  class_oid = ws_oid (class_);

  v = 0;
  for (attribute = class_ptr->shared; attribute != NULL;
       attribute = (SM_ATTRIBUTE *) attribute->header.next)
    {

      if (DB_VALUE_TYPE (&attribute->value) == DB_TYPE_NULL)
	continue;
      if (v == 0)
	{
	  CHECK_PRINT_ERROR (text_print (obj_out,
					 NULL, 0,
					 "%cclass %s%s%s shared (%s%s%s", '%',
					 PRINT_IDENTIFIER (class_ptr->header.
							   name),
					 PRINT_IDENTIFIER (attribute->header.
							   name)));
	}
      else
	{
	  CHECK_PRINT_ERROR (text_print (obj_out,
					 NULL, 0, ", %s%s%s",
					 PRINT_IDENTIFIER (attribute->header.
							   name)));
	}

      ++v;
    }
  if (v)
    {
      CHECK_PRINT_ERROR (text_print (obj_out, ")\n", 2, NULL));
    }

  v = 0;
  for (attribute = class_ptr->shared; attribute != NULL;
       attribute = (SM_ATTRIBUTE *) attribute->header.next)
    {
      if (DB_VALUE_TYPE (&attribute->value) == DB_TYPE_NULL)
	{
	  continue;
	}
      if (v)
	{
	  CHECK_PRINT_ERROR (text_print (obj_out, " ", 1, NULL));
	}
      error = process_value (&attribute->value);
      if (error != NO_ERROR)
	{
	  if (!ignore_err_flag)
	    goto exit_on_error;
	}

      ++v;
    }
  if (v)
    {
      CHECK_PRINT_ERROR (text_print (obj_out, "\n", 1, NULL));
    }

  v = 0;
  for (attribute = class_ptr->class_attributes; attribute != NULL;
       attribute = (SM_ATTRIBUTE *) attribute->header.next)
    {
      if (DB_VALUE_TYPE (&attribute->value) == DB_TYPE_NULL)
	{
	  continue;
	}
      if (v == 0)
	{
	  CHECK_PRINT_ERROR (text_print (obj_out,
					 NULL, 0,
					 "%cclass %s%s%s class (%s%s%s", '%',
					 PRINT_IDENTIFIER (class_ptr->header.
							   name),
					 PRINT_IDENTIFIER (attribute->header.
							   name)));
	}
      else
	{
	  CHECK_PRINT_ERROR (text_print (obj_out,
					 NULL, 0, ", %s%s%s",
					 PRINT_IDENTIFIER (attribute->header.
							   name)));
	}
      ++v;
    }
  if (v)
    {
      CHECK_PRINT_ERROR (text_print (obj_out, ")\n", 2, NULL));
    }

  v = 0;
  for (attribute = class_ptr->class_attributes; attribute != NULL;
       attribute = (SM_ATTRIBUTE *) attribute->header.next)
    {

      if (DB_VALUE_TYPE (&attribute->value) == DB_TYPE_NULL)
	continue;
      if (v)
	CHECK_PRINT_ERROR (text_print (obj_out, " ", 1, NULL));
      if ((error = process_value (&attribute->value)) != NO_ERROR)
	{
	  if (!ignore_err_flag)
	    goto exit_on_error;
	}

      ++v;
    }

  CHECK_PRINT_ERROR (text_print (obj_out, NULL, 0, (v) ? "\n%cclass %s%s%s ("	/* new line */
				 : "%cclass %s%s%s (",
				 '%',
				 PRINT_IDENTIFIER (class_ptr->header.name)));

  v = 0;
  attribute = class_ptr->ordered_attributes;
  while (attribute)
    {
      if (attribute->header.name_space == ID_ATTRIBUTE)
	{
	  CHECK_PRINT_ERROR (text_print (obj_out, NULL, 0, (v) ? " %s%s%s"	/* space */
					 : "%s%s%s",
					 PRINT_IDENTIFIER (attribute->header.
							   name)));
	  ++v;
	}
      attribute = (SM_ATTRIBUTE *) attribute->order_link;
    }
  CHECK_PRINT_ERROR (text_print (obj_out, ")\n", 2, NULL));

  /* Find the heap where the instances are stored */
  hfid = sm_heap ((MOBJ) class_ptr);
  if (hfid->vfid.fileid == NULL_FILEID)
    {
      if (total_objects == total_approximate_class_objects)
	{
	  total = 100;
	}
      else
	{
	  total = 100 *
	    ((float) total_objects / (float) total_approximate_class_objects);
	}
      fprintf (unloadlog_file, MSG_FORMAT "\n", class_ptr->header.name, 0,
	       100, total);
      fflush (unloadlog_file);
      if (verbose_flag)
	{
	  fprintf (stdout, MSG_FORMAT "\n", class_ptr->header.name, 0,
		   100, total);
	  fflush (stdout);
	}
      goto exit_on_end;
    }

  /* Flush all the instances */

  if (locator_flush_all_instances (class_, false) != NO_ERROR)
    {
      if (total_objects == total_approximate_class_objects)
	{
	  total = 100;
	}
      else
	{
	  total = 100 *
	    ((float) total_objects / (float) total_approximate_class_objects);
	}
      fprintf (unloadlog_file, MSG_FORMAT "\n", class_ptr->header.name, 0,
	       100, total);
      fflush (unloadlog_file);
      if (verbose_flag)
	{
	  fprintf (stdout, MSG_FORMAT "\n", class_ptr->header.name, 0,
		   100, total);
	  fflush (stdout);
	}
      goto exit_on_end;
    }

  nobjects = 0;
  nfetched = -1;
  OID_SET_NULL (&last_oid);

  /* Now start fetching all the instances */

  approximate_class_objects = 0;
  if (get_estimated_objs (hfid, &approximate_class_objects) < 0)
    {
      if (!ignore_err_flag)
	goto exit_on_error;
    }

  if (verbose_flag)
    {
      gauge_class_name = (char *) class_ptr->header.name;
#if !defined (WINDOWS)
      prev_handler = os_set_signal_handler (SIGALRM, gauge_alarm_handler);
      prev_alarm = alarm (GAUGE_INTERVAL);
#endif
    }

  desc_obj = make_desc_obj (class_ptr);

  while (nobjects != nfetched)
    {

      if (locator_fetch_all (hfid, &lock, class_oid, &nobjects, &nfetched,
			     &last_oid, &fetch_area) == NO_ERROR)
	{
	  if (fetch_area != NULL)
	    {
	      mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (fetch_area);
	      obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);

	      for (i = 0; i < mobjs->num_objs; ++i)
		{
		  /*
		   * Process all objects for a requested class, but
		   * only referenced objects for a referenced class.
		   */
		  ++class_objects;
		  ++total_objects;
		  LC_RECDES_TO_GET_ONEOBJ (fetch_area, obj, &recdes);
		  if ((error =
		       desc_disk_to_obj (class_, class_ptr, &recdes,
					 desc_obj)) == NO_ERROR)
		    {
		      if ((error = process_object (desc_obj, &obj->oid,
						   referenced_class)) !=
			  NO_ERROR)
			{
			  if (!ignore_err_flag)
			    {
			      desc_free (desc_obj);
			      locator_free_copy_area (fetch_area);
			      goto exit_on_error;
			    }
			}
		    }
		  else
		    {
		      if (error == ER_TF_BUFFER_UNDERFLOW)
			{
			  desc_free (desc_obj);
			  goto exit_on_error;
			}
		      ++failed_objects;
		    }
		  obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);
#if defined(WINDOWS)
		  if (verbose_flag && (i % 10 == 0))
		    {
		      _ftime (&timebuffer);
		      if (start == 0)
			{
			  start = timebuffer.time;
			}
		      else
			{
			  if ((timebuffer.time - start) > GAUGE_INTERVAL)
			    {
			      gauge_alarm_handler (SIGALRM);
			      start = timebuffer.time;
			    }
			}
		    }
#endif
		}
	      locator_free_copy_area (fetch_area);
	    }
	  else
	    {
	      /* No more objects */
	      break;
	    }
	}
      else
	{
	  /* some error was occurred */
	  if (!ignore_err_flag)
	    {
	      desc_free (desc_obj);
	      goto exit_on_error;
	    }
	  else
	    ++failed_objects;
	}
    }

  desc_free (desc_obj);

  total_approximate_class_objects +=
    (class_objects - approximate_class_objects);
  if (total_objects == total_approximate_class_objects)
    {
      total = 100;
    }
  else
    {
      total = 100 *
	((float) total_objects / (float) total_approximate_class_objects);
    }
  if (verbose_flag)
    {
#if !defined(WINDOWS)
      alarm (prev_alarm);
      (void) os_set_signal_handler (SIGALRM, prev_handler);
#endif

      fprintf (stdout, MSG_FORMAT "\n", class_ptr->header.name,
	       class_objects, 100, total);
      fflush (stdout);
    }
  fprintf (unloadlog_file, MSG_FORMAT "\n", class_ptr->header.name,
	   class_objects, 100, total);

exit_on_end:

  return error;

exit_on_error:

  CHECK_EXIT_ERROR (error);
  goto exit_on_end;

}

/*
 * process_object - dump one object in loader format
 *    return: NO_ERROR, if successful, error number, if not successful.
 *    desc_obj(in): object data
 *    obj_oid(in): object oid
 *    referenced_class(in): is referenced ?
 */
static int
process_object (DESC_OBJ * desc_obj, OID * obj_oid, int referenced_class)
{
  int error = NO_ERROR;
  SM_CLASS *class_ptr;
  SM_ATTRIBUTE *attribute;
  DB_VALUE *value;
  OID *class_oid;
  int data;
  int v = 0;

  class_ptr = desc_obj->class_;
  class_oid = ws_oid (desc_obj->classop);
  if (referenced_class)
    {				/* need to hash OID */
      update_hash (obj_oid, class_oid, &data);
      if (debug_flag)
	{
	  CHECK_PRINT_ERROR (text_print (obj_out,
					 NULL, 0, "%d/*%d.%d.%d*/: ", data,
					 obj_oid->volid, obj_oid->pageid,
					 obj_oid->slotid));
	}
      else
	{
	  CHECK_PRINT_ERROR (text_print (obj_out, NULL, 0, "%d: ", data));
	}
    }

  attribute = class_ptr->ordered_attributes;
  for (attribute = class_ptr->ordered_attributes;
       attribute; attribute = attribute->order_link)
    {

      if (attribute->header.name_space != ID_ATTRIBUTE)
	continue;

      if (v)
	CHECK_PRINT_ERROR (text_print (obj_out, " ", 1, NULL));

      value = &desc_obj->values[attribute->storage_order];

      if ((error = process_value (value)) != NO_ERROR)
	{
	  if (!ignore_err_flag)
	    goto exit_on_error;
	}

      ++v;
    }
  CHECK_PRINT_ERROR (text_print (obj_out, "\n", 1, NULL));

exit_on_end:

  return error;

exit_on_error:

  CHECK_EXIT_ERROR (error);
  goto exit_on_end;

}

/*
 * process_set - dump a set in loader format
 *    return: NO_ERROR, if successful, error number, if not successful.
 *    set(in): set
 * Note:
 *    Should only get here for class and shared attributes that have
 *    default values.
 */
static int
process_set (DB_SET * set)
{
  int error = NO_ERROR;
  SET_ITERATOR *it = NULL;
  DB_VALUE *element_value;
  int check_nelem = 0;

  CHECK_PRINT_ERROR (text_print (obj_out, "{", 1, NULL));

  it = set_iterate (set);
  while ((element_value = set_iterator_value (it)) != NULL)
    {

      if ((error = process_value (element_value)) != NO_ERROR)
	{
	  if (!ignore_err_flag)
	    goto exit_on_error;
	}

      check_nelem++;

      if (set_iterator_next (it))
	{
	  CHECK_PRINT_ERROR (text_print (obj_out, ", ", 2, NULL));
	  if (check_nelem >= 10)
	    {			/* set New-Line for each 10th elements */
	      CHECK_PRINT_ERROR (text_print (obj_out, "\n", 1, NULL));
	      check_nelem -= 10;
	    }
	}
    }
  CHECK_PRINT_ERROR (text_print (obj_out, "}", 1, NULL));

exit_on_end:
  if (it != NULL)
    {
      set_iterator_free (it);
    }
  return error;

exit_on_error:

  CHECK_EXIT_ERROR (error);
  goto exit_on_end;
}


/*
 * process_value - dump one value in loader format
 *    return: NO_ERROR, if successful, error number, if not successful.
 *    value(in): the value to process
 */
static int
process_value (DB_VALUE * value)
{
  int error = NO_ERROR;

  switch (DB_VALUE_TYPE (value))
    {
    case DB_TYPE_OID:
    case DB_TYPE_OBJECT:
      {
	OID *ref_oid;
	int ref_data;
	OID ref_class_oid;
	DB_OBJECT *classop;
	SM_CLASS *class_ptr;
	int *cls_no_ptr, cls_no;

	if (DB_VALUE_TYPE (value) == DB_TYPE_OID)
	  {
	    ref_oid = DB_GET_OID (value);
	  }
	else
	  {
	    ref_oid = WS_OID (DB_PULL_OBJECT (value));
	  }

	if (required_class_only || (ref_oid == (OID *) 0)
	    || (OID_EQ (ref_oid, &null_oid)))
	  {
	    CHECK_PRINT_ERROR (text_print (obj_out, "NULL", 4, NULL));
	    break;
	  }

	OID_SET_NULL (&ref_class_oid);

	if ((error = locator_does_exist (ref_oid, NULL_CHN, IS_LOCK,
					 &ref_class_oid, NULL_CHN,
					 false, false, NULL,
					 DB_CLUSTER_NODE_LOCAL)) == LC_EXIST)
	  {
	    if ((classop = is_class (ref_oid, &ref_class_oid)))
	      {
		ws_find (classop, (MOBJ *) & class_ptr);
		if (class_ptr == NULL)
		  {
		    goto exit_on_error;
		  }
		CHECK_PRINT_ERROR (text_print (obj_out, NULL, 0, "@%s",
					       class_ptr->header.name));
		break;
	      }

	    /*
	     * Lock referenced class with S_LOCK
	     */
	    error = NO_ERROR;	/* clear */
	    if ((classop =
		 is_class (&ref_class_oid, WS_OID (sm_Root_class_mop))))
	      {
		if (locator_fetch_class (classop, DB_FETCH_QUERY_READ) ==
		    NULL)
		  {
		    error = LC_ERROR;
		  }
	      }
	    else
	      {
		error = LC_ERROR;
	      }
	  }
	else if (error != LC_DOESNOT_EXIST)
	  {
	    error = LC_ERROR;
	  }
	else
	  {
	    CHECK_PRINT_ERROR (text_print (obj_out, "NULL", 4, NULL));
	    break;
	  }

	if (error != NO_ERROR)
	  {
	    if (!ignore_err_flag)
	      goto exit_on_error;
	    else
	      {
		(void) text_print (obj_out, "NULL", 4, NULL);
		break;
	      }
	  }

	/*
	 * Output a reference indication if all classes are being processed,
	 * or if a class_list is being used and references are being included,
	 * or if a class_list is being used and the referenced class is a
	 * requested class.  Otherwise, output "NULL".
	 */

	/* figure out what it means for this to be NULL, I think
	   this happens only for the reserved system classes
	   like db_user that are not dumped.  This is a problem because
	   trigger objects for one, like to point directly at the
	   user object.   There will probably be others in time.
	 */
	error = fh_get (cl_table, &ref_class_oid, (FH_DATA *) (&cls_no_ptr));
	if (error != NO_ERROR || cls_no_ptr == NULL)
	  {
	    CHECK_PRINT_ERROR (text_print (obj_out, "NULL", 4, NULL));
	  }
	else
	  {
	    cls_no = *cls_no_ptr;
	    if (!input_filename || include_references
		|| (IS_CLASS_REQUESTED (cls_no)))
	      {
		update_hash (ref_oid, &ref_class_oid, &ref_data);
		if (debug_flag)
		  {
		    int *temp;
		    error =
		      fh_get (cl_table, &ref_class_oid, (FH_DATA *) (&temp));
		    if (error != NO_ERROR || temp == NULL)
		      {
			CHECK_PRINT_ERROR (text_print
					   (obj_out, "NULL", 4, NULL));
		      }
		    else
		      {
			CHECK_PRINT_ERROR (text_print (obj_out,
						       NULL, 0,
						       "@%d|%d/*%d.%d.%d*/",
						       *temp,
						       ref_data,
						       ref_oid->volid,
						       ref_oid->pageid,
						       ref_oid->slotid));
		      }
		  }
		else
		  {
		    int *temp;
		    error =
		      fh_get (cl_table, &ref_class_oid, (FH_DATA *) (&temp));
		    if (error != NO_ERROR || temp == NULL)
		      {
			CHECK_PRINT_ERROR (text_print
					   (obj_out, "NULL", 4, NULL));
		      }
		    else
		      {
			CHECK_PRINT_ERROR (text_print (obj_out,
						       NULL, 0, "@%d|%d",
						       *temp, ref_data));
		      }
		  }
	      }
	    else
	      {
		CHECK_PRINT_ERROR (text_print (obj_out, "NULL", 4, NULL));
	      }
	  }
	break;
      }

    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
    case DB_TYPE_SET:
      CHECK_PRINT_ERROR (process_set (DB_GET_SET (value)));
      break;

    case DB_TYPE_ELO:
      {
	DB_ELO *elo;

	elo = DB_GET_ELO (value);
	if (elo)
	  {
	    switch (elo->type)
	      {
	      case ELO_LO:
		{
		  char filename[DB_MAX_IDENTIFIER_LENGTH];
		  if (LOID_IS_NULL (&elo->loid))
		    {
		      CHECK_PRINT_ERROR (text_print
					 (obj_out, "NULL", 4, NULL));
		      break;
		    }
		  if (lo_count > 0)
		    {
		      snprintf (filename, sizeof (filename) - 1,
				"%s/%s_lo%d/%s_%d.lo", output_dirname,
				output_prefix, output_number / lo_count,
				output_prefix, output_number);
		      if (!(output_number % lo_count))
			{
			  char lo_dirname[DB_MAX_IDENTIFIER_LENGTH];
			  snprintf (lo_dirname, sizeof (lo_dirname) - 1,
				    "%s/%s_lo%d",
				    output_dirname, output_prefix,
				    output_number / lo_count);
			  if (mkdir (lo_dirname, S_IRWXU))
			    {
			      goto exit_on_error;
			    }
			}
		      output_number++;
		    }
		  else
		    {
		      snprintf (filename, sizeof (filename) - 1, "%s_%d.lo",
				output_prefix, ++output_number);
		    }
		  CHECK_PRINT_ERROR (text_print (obj_out, NULL, 0, "^I'%s'",
						 filename));
		  if (lo_migrate_out (&elo->loid, filename) != 0)
		    goto exit_on_error;
		  break;
		}
	      case ELO_FBO:
		CHECK_PRINT_ERROR (text_print (obj_out, NULL, 0, "^E'%s'",
					       elo->original_pathname));
		break;
	      default:
		CHECK_PRINT_ERROR (text_print
				   (obj_out, "<ELO NULL>", 10, NULL));
		break;
	      }
	  }
	else
	  CHECK_PRINT_ERROR (desc_value_special_fprint (obj_out, value));
	break;
      }

    default:
      CHECK_PRINT_ERROR (desc_value_special_fprint (obj_out, value));
      break;
    }

exit_on_end:

  return error;

exit_on_error:

  CHECK_EXIT_ERROR (error);
  goto exit_on_end;

}


/*
 * update_hash - update obj_table hash
 *    return: void
 *    object_oid(in): the object oid used as hash key
 *    class_oid(in): the oid of the object's class
 *    data(out): the value to associate with the oid
 * Note:
 *    If the object oid exists, return the data.  Otherwise, get the value for
 *    the class oid, increment it and use it for the data.  Store the data
 *    with the object oid  and class oid.  The data for the class id is the
 *    next number to use for the class.  If the class oid doesn't exist,
 *    initialize the data to 1.
 */
static void
update_hash (OID * object_oid, OID * class_oid, int *data)
{
  int *d;
  int error;

  error = fh_get (obj_table, object_oid, (FH_DATA *) (&d));
  if (error != NO_ERROR || d == NULL)
    {
      error = fh_get (obj_table, class_oid, (FH_DATA *) (&d));
      if (error != NO_ERROR || d == NULL)
	{
	  *data = 1;
	}
      else
	{
	  *data = *d + 1;
	}
      if (fh_put (obj_table, class_oid, data) != NO_ERROR)
	{
	  perror
	    ("SYSTEM ERROR related with hash-file\n==>unloaddb is NOT completed");
	  exit (1);
	}
      if (fh_put (obj_table, object_oid, data) != NO_ERROR)
	{
	  perror
	    ("SYSTEM ERROR related with hash-file\n==>unloaddb is NOT completed");
	  exit (1);
	}
    }
  else
    {
      *data = *d;
    }
}


/*
 * is_class - determine whether the object is actually a class.
 *    return: MOP for the object
 *    obj_oid(in): the object oid
 *    class_oid(in): the class oid
 */
static DB_OBJECT *
is_class (OID * obj_oid, OID * class_oid)
{
  if (OID_EQ (class_oid, WS_OID (sm_Root_class_mop)))
    {
      return ws_mop (obj_oid, NULL);
    }
  return 0;
}


/*
 * is_req_class - determine whether the class was requested in the input file
 *    return: 1 if true, otherwise 0.
 *    class(in): the class object
 */
int
is_req_class (DB_OBJECT * class_)
{
  int n;

  for (n = 0; n < class_table->num; ++n)
    {
      if (req_class_table[n] == class_)
	return 1;
    }
  return 0;
}


/*
 * all_classes_processed - compares class_requested with class_processed.
 *    return: 1 if true, otherwise 0.
 */
static int
all_classes_processed (void)
{
  int n;

  for (n = 0; n < (class_table->num + 7) / 8; ++n)
    {
      if ((class_requested[n] | class_referenced[n]) != class_processed[n])
	return 0;
    }
  return 1;
}

/*
 * ltrim - trim a given string.
 *    return: pointer to the trimed string.
 */
static char *
ltrim (char *s)
{
  char *begin;

  assert (s != NULL);

  begin = s;
  while (*begin != '\0')
    {
      if (isspace (*begin))
	{
	  begin++;
	}
      else
	{
	  break;
	}
    }
  s = begin;

  return s;
}

/*
 * get_requested_classes - read the class names from input_filename
 *    return: 0 for success, non-zero otherwise
 *    input_filename(in): the name of the input_file
 *    class_list(out): the class table to store classes in
 */
int
get_requested_classes (const char *input_filename, DB_OBJECT * class_list[])
{
  int i, j, is_partition = 0, error;
  int len_clsname = 0;
  FILE *input_file;
  char buffer[DB_MAX_IDENTIFIER_LENGTH];
  char class_name[DB_MAX_IDENTIFIER_LENGTH];
  char downcase_class_name[SM_MAX_IDENTIFIER_LENGTH];
  MOP *sub_partitions = NULL;
  char scan_format[16];
  char *trimmed_buf;

  if (input_filename == NULL)
    {
      return 0;
    }

  input_file = fopen (input_filename, "r");
  if (input_file == NULL)
    {
      perror (input_filename);
      return 1;
    }
  snprintf (scan_format, sizeof (scan_format), "%%%ds\n",
	    (int) (sizeof (buffer) - 1));
  i = 0;
  while (fgets ((char *) buffer, DB_MAX_IDENTIFIER_LENGTH,
		input_file) != NULL)
    {
      DB_OBJECT *class_;

      /* trim left */
      trimmed_buf = ltrim (buffer);
      len_clsname = strlen (trimmed_buf);

      /* get rid of \n at end of line */
      if (len_clsname > 0 && trimmed_buf[len_clsname - 1] == '\n')
	{
	  trimmed_buf[len_clsname - 1] = 0;
	  len_clsname--;
	}

      if (len_clsname >= 1)
	{
	  sscanf ((char *) buffer, scan_format, (char *) class_name);

	  sm_downcase_name (class_name, downcase_class_name,
			    SM_MAX_IDENTIFIER_LENGTH);

	  class_ = locator_find_class (downcase_class_name);
	  if (class_ != NULL)
	    {
	      class_list[i] = class_;
	      error =
		do_is_partitioned_classobj (&is_partition, class_, NULL,
					    &sub_partitions);
	      if (is_partition == 1 && sub_partitions != NULL)
		{
		  for (j = 0; sub_partitions[j]; j++)
		    {
		      i++;
		      class_list[i] = sub_partitions[j];
		    }
		}
	      if (sub_partitions != NULL)
		{
		  free_and_init (sub_partitions);
		}
	      i++;
	    }
	}
    }				/* while */

  fclose (input_file);

  return 0;
}