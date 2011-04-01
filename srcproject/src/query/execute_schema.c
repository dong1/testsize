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
 * execute_schema.c
 */

#ident "$Id$"

#include "config.h"

#include <stdarg.h>
#include <ctype.h>

#include "error_manager.h"
#include "parser.h"
#include "parser_message.h"
#include "db.h"
#include "dbi.h"
#include "semantic_check.h"
#include "execute_schema.h"
#include "execute_statement.h"
#include "schema_manager.h"
#include "transaction_cl.h"
#include "system_parameter.h"
#include "dbval.h"
#include "cluster_config.h"
#if defined(WINDOWS)
#include "misc_string.h"
#endif /* WINDOWS */
#include "semantic_check.h"
#include "xasl_generation.h"
#include "memory_alloc.h"
#include "transform.h"
#include "set_object.h"
#include "object_accessor.h"
#include "memory_hash.h"
#include "locator_cl.h"
#include "xasl_support.h"
#include "network_interface_cl.h"
#include "view_transform.h"

#define UNIQUE_SAVEPOINT_ADD_ATTR_MTHD "aDDaTTRmTHD"
#define UNIQUE_SAVEPOINT_CREATE_ENTITY "cREATEeNTITY"
#define UNIQUE_SAVEPOINT_MULTIPLE_RENAME "mULTIPLErENAME"
#define UNIQUE_SAVEPOINT_MULTIPLE_ALTER "mULTIPLEaLTER"
#define UNIQUE_SAVEPOINT_TRUNCATE "tRUnCATE"

typedef enum
{
  DO_INDEX_CREATE, DO_INDEX_DROP
} DO_INDEX;

static int drop_class_name (const char *name);

static int do_alter_one_clause_with_template (PARSER_CONTEXT * parser,
					      PT_NODE * alter);
static int do_alter_clause_rename_entity (PARSER_CONTEXT * const parser,
					  PT_NODE * const alter);
static int do_alter_clause_drop_index (PARSER_CONTEXT * const parser,
				       PT_NODE * const alter);
static int do_rename_internal (const char *const old_name,
			       const char *const new_name);
static DB_CONSTRAINT_TYPE get_reverse_unique_index_type (const bool
							 is_reverse,
							 const bool
							 is_unique);
static int create_or_drop_index_helper (const PARSER_CONTEXT * const parser,
					const char *const constraint_name,
					const bool is_reverse,
					const bool is_unique,
					const PT_NODE * const column_names,
					const PT_NODE *
					const column_prefix_length,
					DB_OBJECT * const obj,
					const DO_INDEX do_index);
static int update_locksets_for_multiple_rename (const char *class_name,
						int *num_mops, MOP * mop_set,
						int *num_names,
						char **name_set,
						bool error_on_misssing_class);
static int acquire_locks_for_multiple_rename (const PT_NODE * statement);

static int validate_attribute_domain (PARSER_CONTEXT * parser,
				      PT_NODE * attribute,
				      const bool check_zero_precision);
static SM_FOREIGN_KEY_ACTION map_pt_to_sm_action (const PT_MISC_TYPE action);
static int add_foreign_key (DB_CTMPL * ctemplate, const PT_NODE * cnstr,
			    const char **att_names);

static int add_union_query (PARSER_CONTEXT * parser,
			    DB_CTMPL * ctemplate, const PT_NODE * query);
static int add_query_to_virtual_class (PARSER_CONTEXT * parser,
				       DB_CTMPL * ctemplate,
				       const PT_NODE * queries);
static int do_copy_indexes (MOP classmop, SM_CLASS * src_class);

/*
 * Function Group :
 * DO functions for alter statement
 *
 */

/*
 * do_alter_one_clause_with_template() - Executes the operations required by a
 *                                       single ALTER clause.
 *   return: Error code
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of a single clause of an ALTER statement. Not
 *                  all possible clauses are handled by this function; see the
 *                  note below and the do_alter() function.
 *
 * Note: This function handles clauses that require class template operations.
 *       It always calls dbt_edit_class(). Other ALTER clauses might have
 *       dedicated processing functions. See do_alter() for details.
 */
static int
do_alter_one_clause_with_template (PARSER_CONTEXT * parser, PT_NODE * alter)
{
  const char *entity_name, *new_query;
  const char *attr_name, *mthd_name, *mthd_file, *attr_mthd_name;
  const char *new_name, *old_name, *domain;
  DB_CTMPL *ctemplate = NULL;
  DB_OBJECT *vclass, *sup_class;
  int error = NO_ERROR;
  DB_ATTRIBUTE *found_attr, *def_attr;
  DB_METHOD *found_mthd;
  DB_DOMAIN *def_domain;
  DB_VALUE src_val, dest_val;
  DB_TYPE db_desired_type;
  int query_no, class_attr;
  PT_NODE *vlist, *p, *n, *d;
  PT_NODE *node, *nodelist;
  PT_NODE *data_type, *data_default, *path;
  PT_NODE *slist, *parts, *coalesce_list, *names, *delnames;
  PT_NODE *create_index = NULL;
  PT_TYPE_ENUM pt_desired_type;
#if 0
  HFID *hfid;
#endif
  char keycol[DB_MAX_IDENTIFIER_LENGTH], partnum_str[32];
  MOP classop;
  SM_CLASS *class_, *subcls;
  DB_OBJLIST *objs;
  SM_CLASS_CONSTRAINT *cons;
  SM_ATTRIBUTE **attp;
  char **namep = NULL, **attrnames;
  int *asc_desc = NULL;
  int i, partnum = 0, coalesce_num = 0;
  SM_CLASS *smclass;
  TP_DOMAIN *key_type;
  bool partition_savepoint = false;
  const PT_ALTER_CODE alter_code = alter->info.alter.code;
  bool need_partition_post_work = false;

  entity_name = alter->info.alter.entity_name->info.name.original;
  if (entity_name == NULL)
    {
      return er_errid ();
    }

  vclass = db_find_class (entity_name);
  if (vclass == NULL)
    {
      return er_errid ();
    }

  /* If this is a global partition table, we need get information on remote node. */
  error = sm_get_remote_info_if_need (vclass);
  if (error != NO_ERROR)
    {
      return error;
    }

  db_make_null (&src_val);
  db_make_null (&dest_val);

  ctemplate = dbt_edit_class (vclass);
  if (ctemplate == NULL)
    {
      /* when dbt_edit_class fails (e.g. because the server unilaterally
         aborts us), we must record the associated error message into the
         parser.  Otherwise, we may get a confusing error msg of the form:
         "so_and_so is not a class". */
      pt_record_error (parser, parser->statement_number - 1,
		       alter->line_number, alter->column_number, er_msg ());
      return er_errid ();
    }

  switch (alter_code)
    {
    case PT_ADD_QUERY:
      error = do_add_queries (parser, ctemplate,
			      alter->info.alter.alter_clause.query.query);
      break;

    case PT_DROP_QUERY:
      vlist = alter->info.alter.alter_clause.query.query_no_list;
      if (vlist == NULL)
	{
	  error = dbt_drop_query_spec (ctemplate, 1);
	}
      else if (vlist->next == NULL)
	{			/* ie, only one element in list */
	  error = dbt_drop_query_spec (ctemplate,
				       vlist->info.value.data_value.i);
	}
      else
	{
	  slist = pt_sort_in_desc_order (vlist);
	  for (; slist; slist = slist->next)
	    {
	      error = dbt_drop_query_spec (ctemplate,
					   slist->info.value.data_value.i);
	      if (error != NO_ERROR)
		{
		  break;
		}
	    }
	}
      break;

    case PT_MODIFY_QUERY:
      if (alter->info.alter.alter_clause.query.query_no_list)
	{
	  query_no = alter->info.alter.alter_clause.query.query_no_list->
	    info.value.data_value.i;
	}
      else
	{
	  query_no = 1;
	}
      new_query = parser_print_tree (parser,
				     alter->info.alter.alter_clause.query.
				     query);
      error = dbt_change_query_spec (ctemplate, new_query, query_no);
      break;

    case PT_ADD_ATTR_MTHD:
#if 0
      /* we currently core dump when adding a unique
         constraint at the same time as an attribute, whether the unique
         constraint is on the new attribute or another.
         Therefore we temporarily disallow adding a unique constraint
         and an attribute in the same alter statement if the class has
         or has had any instances.
         Note that we should be checking for instances in the entire
         subhierarchy, not just the current class. */
      if ((hfid = sm_get_heap (vclass)) && !HFID_IS_NULL (hfid)
	  && alter->info.alter.alter_clause.attr_mthd.attr_def_list)
	{
	  for (p = alter->info.alter.constraint_list; p != NULL; p = p->next)
	    {
	      if (p->info.constraint.type == PT_CONSTRAIN_UNIQUE)
		{
		  error = ER_DO_ALTER_ADD_WITH_UNIQUE;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
		  (void) dbt_abort_class (ctemplate);
		  return error;
		}
	    }
	  PT_END;
	}
#endif
      error = tran_savepoint (UNIQUE_SAVEPOINT_ADD_ATTR_MTHD, false);
      if (error == NO_ERROR)
	{
	  error = do_add_attributes (parser, ctemplate,
				     alter->info.alter.alter_clause.attr_mthd.
				     attr_def_list, NULL);
	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (ctemplate);
	      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_ADD_ATTR_MTHD);
	      return error;
	    }

	  error = do_add_foreign_key_objcache_attr (ctemplate,
						    alter->info.alter.
						    constraint_list);
	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (ctemplate);
	      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_ADD_ATTR_MTHD);
	      return error;
	    }

	  vclass = dbt_finish_class (ctemplate);
	  if (vclass == NULL)
	    {
	      error = er_errid ();
	      dbt_abort_class (ctemplate);
	      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_ADD_ATTR_MTHD);
	      return error;
	    }

	  ctemplate = dbt_edit_class (vclass);
	  if (ctemplate == NULL)
	    {
	      error = er_errid ();
	      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_ADD_ATTR_MTHD);
	      return error;
	    }

	  error = do_add_constraints (ctemplate,
				      alter->info.alter.constraint_list);
	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (ctemplate);
	      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_ADD_ATTR_MTHD);
	      return error;
	    }

	  error = do_check_fk_constraints (ctemplate,
					   alter->info.alter.constraint_list);
	  if (error != NO_ERROR)
	    {
	      (void) dbt_abort_class (ctemplate);
	      (void)
		tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_ADD_ATTR_MTHD);
	      return error;
	    }

	  if (alter->info.alter.alter_clause.attr_mthd.mthd_def_list != NULL)
	    {
	      error = do_add_methods (parser,
				      ctemplate,
				      alter->info.alter.alter_clause.
				      attr_mthd.mthd_def_list);
	    }
	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (ctemplate);
	      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_ADD_ATTR_MTHD);
	      return error;
	    }

	  if (alter->info.alter.alter_clause.attr_mthd.mthd_file_list != NULL)
	    {
	      error = do_add_method_files (parser, ctemplate,
					   alter->info.alter.alter_clause.
					   attr_mthd.mthd_file_list);
	    }
	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (ctemplate);
	      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_ADD_ATTR_MTHD);
	      return error;
	    }

	  create_index = alter->info.alter.create_index;
	}
      break;

    case PT_DROP_ATTR_MTHD:
      p = alter->info.alter.alter_clause.attr_mthd.attr_mthd_name_list;
      for (; p && p->node_type == PT_NAME; p = p->next)
	{
	  attr_mthd_name = p->info.name.original;
	  if (p->info.name.meta_class == PT_META_ATTR)
	    {
	      found_attr = db_get_class_attribute (vclass, attr_mthd_name);
	      if (found_attr)
		{
		  error = dbt_drop_class_attribute (ctemplate,
						    attr_mthd_name);
		}
	      else
		{
		  found_mthd = db_get_class_method (vclass, attr_mthd_name);
		  if (found_mthd)
		    {
		      error = dbt_drop_class_method (ctemplate,
						     attr_mthd_name);
		    }
		}
	    }
	  else
	    {
	      found_attr = db_get_attribute (vclass, attr_mthd_name);
	      if (found_attr)
		{
		  error = dbt_drop_attribute (ctemplate, attr_mthd_name);
		}
	      else
		{
		  found_mthd = db_get_method (vclass, attr_mthd_name);
		  if (found_mthd)
		    {
		      error = dbt_drop_method (ctemplate, attr_mthd_name);
		    }
		}
	    }

	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (ctemplate);
	      return error;
	    }
	}

      p = alter->info.alter.alter_clause.attr_mthd.mthd_file_list;
      for (; p
	   && p->node_type == PT_FILE_PATH
	   && (path = p->info.file_path.string) != NULL
	   && path->node_type == PT_VALUE
	   && (path->type_enum == PT_TYPE_VARCHAR
	       || path->type_enum == PT_TYPE_CHAR
	       || path->type_enum == PT_TYPE_NCHAR
	       || path->type_enum == PT_TYPE_VARNCHAR); p = p->next)
	{
	  mthd_file = (char *) path->info.value.data_value.str->bytes;
	  error = dbt_drop_method_file (ctemplate, mthd_file);
	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (ctemplate);
	      return error;
	    }
	}

      break;

    case PT_MODIFY_ATTR_MTHD:
      p = alter->info.alter.alter_clause.attr_mthd.attr_def_list;
      for (; p && p->node_type == PT_ATTR_DEF; p = p->next)
	{
	  attr_name = p->info.attr_def.attr_name->info.name.original;
	  if (p->info.attr_def.attr_type == PT_META_ATTR)
	    {
	      class_attr = 1;
	    }
	  else
	    {
	      class_attr = 0;
	    }
	  data_type = p->data_type;

	  domain = pt_node_to_db_domain_name (p);
	  error = dbt_change_domain (ctemplate, attr_name,
				     class_attr, domain);

	  if (data_type && pt_is_set_type (p))
	    {
	      nodelist = data_type->data_type;
	      for (node = nodelist; node != NULL; node = node->next)
		{
		  domain = pt_data_type_to_db_domain_name (node);
		  error = dbt_add_set_attribute_domain (ctemplate,
							attr_name, class_attr,
							domain);
		  if (error != NO_ERROR)
		    {
		      dbt_abort_class (ctemplate);
		      return error;
		    }
		}
	    }

	  data_default = p->info.attr_def.data_default;
	  if (data_default != NULL
	      && data_default->node_type == PT_DATA_DEFAULT)
	    {
	      pt_desired_type = p->type_enum;

	      /* try to coerce the default value into the attribute's type */
	      d = data_default->info.data_default.default_value;
	      d = pt_semantic_check (parser, d);
	      if (pt_has_error (parser))
		{
		  pt_report_to_ersys (parser, PT_SEMANTIC);
		  dbt_abort_class (ctemplate);
		  return er_errid ();
		}

	      if (d != NULL)
		{
		  error = pt_coerce_value (parser, d, d, pt_desired_type,
					   p->data_type);
		  if (error != NO_ERROR)
		    {
		      break;
		    }
		}

	      pt_evaluate_tree (parser, d, &dest_val);
	      if (pt_has_error (parser))
		{
		  pt_report_to_ersys (parser, PT_SEMANTIC);
		  dbt_abort_class (ctemplate);
		  return er_errid ();
		}

	      error = dbt_change_default (ctemplate, attr_name, class_attr,
					  &dest_val);
	      if (error != NO_ERROR)
		{
		  dbt_abort_class (ctemplate);
		  return error;
		}
	    }
	}

      /* the order in which methods are defined will change;
         currently there's no way around this problem. */
      p = alter->info.alter.alter_clause.attr_mthd.mthd_def_list;
      for (; p && p->node_type == PT_METHOD_DEF; p = p->next)
	{
	  mthd_name = p->info.method_def.method_name->info.name.original;
	  error = dbt_drop_method (ctemplate, mthd_name);
	  if (error == NO_ERROR)
	    {
	      error = do_add_methods (parser,
				      ctemplate,
				      alter->info.alter.alter_clause.
				      attr_mthd.mthd_def_list);
	    }
	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (ctemplate);
	      return error;
	    }
	}
      break;

    case PT_ADD_SUPCLASS:
      error = do_add_supers (parser,
			     ctemplate,
			     alter->info.alter.super.sup_class_list);
      break;

    case PT_DROP_SUPCLASS:
      for (p = alter->info.alter.super.sup_class_list;
	   p && p->node_type == PT_NAME; p = p->next)
	{
	  sup_class = db_find_class (p->info.name.original);
	  if (sup_class == NULL)
	    {
	      error = er_errid ();
	    }
	  else
	    {
	      error = dbt_drop_super (ctemplate, sup_class);
	    }
	  if (error != NO_ERROR)
	    {
	      (void) dbt_abort_class (ctemplate);
	      return error;
	    }
	}
      break;

    case PT_DROP_RESOLUTION:
      for (p = alter->info.alter.super.resolution_list;
	   p && p->node_type == PT_RESOLUTION; p = p->next)
	{
	  sup_class = db_find_class (p->info.resolution.of_sup_class_name->
				     info.name.original);
	  attr_mthd_name = p->info.resolution.attr_mthd_name->
	    info.name.original;
	  error = dbt_drop_resolution (ctemplate, sup_class, attr_mthd_name);
	  if (error != NO_ERROR)
	    {
	      (void) dbt_abort_class (ctemplate);
	      return error;
	    }
	}
      break;

    case PT_MODIFY_DEFAULT:
    case PT_ALTER_DEFAULT:
      n = alter->info.alter.alter_clause.ch_attr_def.attr_name_list;
      d = alter->info.alter.alter_clause.ch_attr_def.data_default_list;
      for (; n && d; n = n->next, d = d->next)
	{
	  /* try to coerce the default value into the attribute's type */
	  d = pt_semantic_check (parser, d);
	  if (d == NULL)
	    {
	      if (pt_has_error (parser))
		{
		  pt_report_to_ersys (parser, PT_SEMANTIC);
		  error = er_errid ();
		}
	      else
		{
		  error = ER_GENERIC_ERROR;
		}
	      break;
	    }

	  pt_evaluate_tree (parser, d, &src_val);
	  if (pt_has_error (parser))
	    {
	      pt_report_to_ersys (parser, PT_SEMANTIC);
	      error = er_errid ();
	      break;
	    }

	  attr_name = n->info.name.original;
	  if (n->info.name.meta_class == PT_META_ATTR)
	    {
	      def_attr = db_get_class_attribute (vclass, attr_name);
	    }
	  else
	    {
	      def_attr = db_get_attribute (vclass, attr_name);
	    }
	  if (!def_attr || !(def_domain = db_attribute_domain (def_attr)))
	    {
	      error = er_errid ();
	      break;
	    }
	  db_desired_type = db_domain_type (def_domain);

	  error = tp_value_coerce (&src_val, &dest_val, def_domain);
	  if (error != NO_ERROR)
	    {
	      DB_OBJECT *desired_class;
	      const char *desired_type;

	      if ((db_desired_type == DB_TYPE_OBJECT)
		  && (desired_class = db_domain_class (def_domain)))
		{
		  desired_type = db_get_class_name (desired_class);
		}
	      else
		{
		  desired_type = db_get_type_name (db_desired_type);
		}

	      if (error == DOMAIN_OVERFLOW)
		{
		  PT_ERRORmf2 (parser, d, MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_OVERFLOW_COERCING_TO,
			       pt_short_print (parser, d), desired_type);
		}
	      else
		{
		  PT_ERRORmf2 (parser, d, MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_CANT_COERCE_TO,
			       pt_short_print (parser, d), desired_type);
		}
	      break;
	    }

	  if (n->info.name.meta_class == PT_META_ATTR)
	    {
	      error = dbt_change_default (ctemplate, attr_name, 1, &dest_val);
	    }
	  else
	    {
	      error = dbt_change_default (ctemplate, attr_name, 0, &dest_val);
	    }
	  if (error != NO_ERROR)
	    {
	      break;
	    }

	  pr_clear_value (&src_val);
	  pr_clear_value (&dest_val);
	}
      break;

      /* If merely renaming resolution,  will be done after switch statement */
    case PT_RENAME_RESOLUTION:
      break;

    case PT_RENAME_ATTR_MTHD:
      if (alter->info.alter.alter_clause.rename.old_name)
	{
	  old_name = alter->info.alter.alter_clause.rename.old_name->
	    info.name.original;
	}
      else
	{
	  old_name = NULL;
	}

      new_name = alter->info.alter.alter_clause.rename.new_name->
	info.name.original;

      if (alter->info.alter.alter_clause.rename.meta == PT_META_ATTR)
	{
	  class_attr = 1;
	}
      else
	{
	  class_attr = 0;
	}

      switch (alter->info.alter.alter_clause.rename.element_type)
	{
	case PT_ATTRIBUTE:
	case PT_METHOD:
	  error = dbt_rename (ctemplate, old_name, class_attr, new_name);
	  break;

	case PT_FUNCTION_RENAME:
	  mthd_name = alter->info.alter.alter_clause.rename.mthd_name->
	    info.name.original;
	  error = dbt_change_method_implementation (ctemplate, mthd_name,
						    class_attr, new_name);
	  break;

	  /* the following case is not yet supported,
	     but hey, when it is, there'll code for it :-) */

	  /* There's code now. this drops the old file name and
	     puts the new file name in its place., I took out the
	     class_attr, since for our purpose we don't need it */

	case PT_FILE_RENAME:
	  old_name = (char *) alter->info.alter.alter_clause.rename.old_name->
	    info.file_path.string->info.value.data_value.str->bytes;
	  new_name = (char *) alter->info.alter.alter_clause.rename.new_name->
	    info.file_path.string->info.value.data_value.str->bytes;
	  error = dbt_rename_method_file (ctemplate, old_name, new_name);
	  break;

	default:
	  /* actually, it means that a wrong thing is being
	     renamed, and is really an error condition. */
	  assert (false);
	  break;
	}
      break;

    case PT_DROP_CONSTRAINT:
    case PT_DROP_FK_CLAUSE:
    case PT_DROP_PRIMARY_CLAUSE:
      {
	SM_CLASS_CONSTRAINT *cons = NULL;
	const char *constraint_name = NULL;

	if (alter_code == PT_DROP_PRIMARY_CLAUSE)
	  {
	    assert (alter->info.alter.constraint_list == NULL);
	    cons = classobj_find_class_primary_key (ctemplate->current);
	    if (cons != NULL)
	      {
		assert (cons->type == SM_CONSTRAINT_PRIMARY_KEY);
		constraint_name = cons->name;
	      }
	    else
	      {
		/* We set a name to print the error message. */
		constraint_name = "primary key";
	      }
	  }
	else
	  {
	    assert (alter->info.alter.constraint_list->next == NULL);
	    assert (alter->info.alter.constraint_list->node_type == PT_NAME);
	    constraint_name = alter->info.alter.constraint_list->info.name.
	      original;
	    assert (constraint_name != NULL);
	    cons = classobj_find_class_index (ctemplate->current,
					      constraint_name);
	  }

	if (cons != NULL)
	  {
	    const DB_CONSTRAINT_TYPE constraint_type =
	      db_constraint_type (cons);

	    if (alter_code == PT_DROP_FK_CLAUSE &&
		constraint_type != DB_CONSTRAINT_FOREIGN_KEY)
	      {
		er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			ER_SM_CONSTRAINT_HAS_DIFFERENT_TYPE, 1,
			constraint_name);
		error = er_errid ();
	      }
	    else
	      {
		if (alter_code == PT_DROP_FK_CLAUSE &&
		    PRM_COMPAT_MODE == COMPAT_MYSQL)
		  {
		    /* We warn the user that dropping a foreign key behaves
		       differently in CUBRID (the associated index is also
		       dropped while MySQL's associated index is kept and only
		       the foreign key constraint is dropped). This difference
		       is not important enough to be an error but a warning or
		       a notification might help. */
		    er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
			    ER_SM_FK_MYSQL_DIFFERENT, 0);
		  }
		error = dbt_drop_constraint (ctemplate, constraint_type,
					     constraint_name, NULL, 0);
	      }
	  }
	else
	  {
	    er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		    ER_SM_CONSTRAINT_NOT_FOUND, 1, constraint_name);
	    error = er_errid ();
	  }
      }
      break;

    case PT_APPLY_PARTITION:
    case PT_REMOVE_PARTITION:
    case PT_ADD_PARTITION:
    case PT_ADD_HASHPARTITION:
    case PT_COALESCE_PARTITION:
    case PT_REORG_PARTITION:
    case PT_ANALYZE_PARTITION:

      error = tran_savepoint (UNIQUE_PARTITION_SAVEPOINT_ALTER, false);
      if (error != NO_ERROR)
	{
	  break;
	}
      partition_savepoint = true;

      switch (alter_code)
	{
	case PT_APPLY_PARTITION:
	case PT_ADD_PARTITION:
	case PT_ADD_HASHPARTITION:
	  need_partition_post_work = true;

	  error = do_create_partition (parser, alter, vclass, ctemplate);
	  if (error == NO_ERROR)
	    {
	      error = do_check_fk_constraints (ctemplate, alter->info.alter.
					       constraint_list);
	    }
	  break;

	case PT_REORG_PARTITION:
	  need_partition_post_work = true;

	  error = do_create_partition (parser, alter, vclass, ctemplate);
	  if (error == NO_ERROR)
	    {
	      coalesce_num = 0;
	      names = alter->info.alter.alter_clause.partition.name_list;
	      for (; names; names = names->next)
		{
		  if (names->partition_pruned)
		    {
		      coalesce_num++;
		    }
		}
	      sprintf (partnum_str, "$%d", coalesce_num);
	      error = do_remove_partition_pre (ctemplate, keycol,
					       partnum_str);
	    }
	  break;

	case PT_REMOVE_PARTITION:
	  need_partition_post_work = true;

	  error = do_remove_partition_pre (ctemplate, keycol, "*");
	  break;

	case PT_COALESCE_PARTITION:
	  need_partition_post_work = true;

	  error = do_get_partition_keycol (keycol, vclass);
	  if (error != NO_ERROR)
	    {
	      break;
	    }

	  error = do_get_partition_size (vclass);
	  if (error < 0)
	    {
	      break;
	    }
	  partnum = error;
	  coalesce_num = (partnum
			  - (alter->info.alter.alter_clause.partition.size->
			     info.value.data_value.i));
	  sprintf (partnum_str, "#%d", coalesce_num);

	  error = do_remove_partition_pre (ctemplate, keycol, partnum_str);
	  break;

	case PT_ANALYZE_PARTITION:
	  names = alter->info.alter.alter_clause.partition.name_list;
	  if (names == NULL)
	    {			/* ALL */
	      error = au_fetch_class (vclass, &class_, AU_FETCH_READ,
				      AU_SELECT);
	      if (error != NO_ERROR)
		{
		  break;
		}

	      error = sm_update_statistics (vclass);
	      if (error != NO_ERROR)
		{
		  break;
		}

	      for (objs = class_->users; objs; objs = objs->next)
		{
		  error = au_fetch_class (objs->op, &subcls,
					  AU_FETCH_READ, AU_SELECT);
		  if (error != NO_ERROR)
		    {
		      break;
		    }

		  if (!subcls->partition_of)
		    {
		      continue;	/* not partitioned */
		    }

		  error = sm_update_statistics (objs->op);
		  if (error != NO_ERROR)
		    {
		      break;
		    }
		}
	    }
	  else
	    {
	      for (; names; names = names->next)
		{
		  if (!names->info.name.db_object)
		    {
		      break;
		    }

		  error = sm_update_statistics (names->info.name.db_object);
		  if (error != NO_ERROR)
		    {
		      break;
		    }
		}
	    }
	  break;

	default:
	  break;
	}
      break;

    case PT_DROP_PARTITION:	/* post work */
      need_partition_post_work = true;
      break;

    default:
      assert (false);
      dbt_abort_class (ctemplate);
      return error;
    }

  /* Process resolution list if appropriate */
  if (error == NO_ERROR)
    {
      if (alter->info.alter.super.resolution_list != NULL
	  && alter->info.alter.code != PT_DROP_RESOLUTION)
	{
	  error = do_add_resolutions (parser, ctemplate,
				      alter->info.alter.super.
				      resolution_list);
	}
    }

  if (error != NO_ERROR)
    {
      dbt_abort_class (ctemplate);
      if (partition_savepoint)
	{
	  goto alter_partition_fail;
	}
      return error;
    }

  vclass = dbt_finish_class (ctemplate);

  /* the dbt_finish_class() failed, the template was not freed */
  if (vclass == NULL)
    {
      error = er_errid ();
      dbt_abort_class (ctemplate);
      if (partition_savepoint)
	{
	  goto alter_partition_fail;
	}
      return error;
    }

  for (; create_index != NULL; create_index = create_index->next)
    {
      create_index->info.index.indexed_class =
	parser_copy_tree (parser, alter->info.alter.entity_name);
      if (create_index->info.index.indexed_class == NULL)
	{
	  return ER_FAILED;
	}
      error = do_create_index (parser, create_index);
      if (error != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  if (need_partition_post_work == false)
    {
      return NO_ERROR;
    }

  switch (alter_code)
    {
    case PT_APPLY_PARTITION:
    case PT_ADD_HASHPARTITION:
    case PT_ADD_PARTITION:
    case PT_REORG_PARTITION:
      if (alter_code == PT_APPLY_PARTITION)
	{
	  error = do_update_partition_newly (entity_name,
					     alter->info.alter.
					     alter_clause.partition.info->
					     info.partition.keycol->info.
					     name.original);
	}
      else if (alter_code == PT_ADD_HASHPARTITION
	       || alter_code == PT_REORG_PARTITION)
	{
	  error = do_get_partition_keycol (keycol, vclass);
	  if (error == NO_ERROR)
	    {
	      error = do_update_partition_newly (entity_name, keycol);
	    }
	}

      if (error == NO_ERROR)
	{
	  /* index propagate */
	  classop = db_find_class (entity_name);
	  if (classop == NULL)
	    {
	      error = er_errid ();
	      goto fail_end;
	    }
	  if (au_fetch_class (classop, &class_, AU_FETCH_READ,
			      AU_SELECT) != NO_ERROR)
	    {
	      error = er_errid ();
	      goto fail_end;
	    }

	  smclass = sm_get_class_with_statistics (classop);
	  if (smclass == NULL)
	    {
	      if (error == NO_ERROR && (error = er_errid ()) == NO_ERROR)
		{
		  error = ER_PARTITION_WORK_FAILED;
		}
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_PARTITION_WORK_FAILED, 0);
	      goto fail_end;
	    }

	  if (smclass->stats == NULL)
	    {
	      if (error == NO_ERROR && (error = er_errid ()) == NO_ERROR)
		{
		  error = ER_PARTITION_WORK_FAILED;
		}
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_PARTITION_WORK_FAILED, 0);
	      goto fail_end;
	    }

	  for (cons = class_->constraints; cons; cons = cons->next)
	    {
	      if (cons->type != SM_CONSTRAINT_INDEX
		  && cons->type != SM_CONSTRAINT_REVERSE_INDEX)
		{
		  continue;
		}

	      attp = cons->attributes;
	      i = 0;
	      while (*attp)
		{
		  attp++;
		  i++;
		}

	      if (i <= 0
		  || ((namep = (char **) malloc (sizeof (char *) * (i + 1)))
		      == NULL)
		  || (asc_desc = (int *) malloc (sizeof (int) * (i))) == NULL)
		{
		  if (error == NO_ERROR && (error = er_errid ()) == NO_ERROR)
		    {
		      error = ER_PARTITION_WORK_FAILED;
		    }
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_PARTITION_WORK_FAILED, 0);
		  goto fail_end;
		}

	      attp = cons->attributes;
	      attrnames = namep;

	      /* need to get asc/desc info */
	      key_type = classobj_find_cons_index2_col_type_list (cons,
								  smclass->
								  stats);
	      if (key_type == NULL)
		{
		  if (error == NO_ERROR && (error = er_errid ()) == NO_ERROR)
		    {
		      error = ER_PARTITION_WORK_FAILED;
		    }
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_PARTITION_WORK_FAILED, 0);
		  goto fail_end;
		}

	      i = 0;
	      while (*attp && key_type)
		{
		  *attrnames = (char *) (*attp)->header.name;
		  attrnames++;

		  asc_desc[i] = 0;	/* guess as Asc */
		  if (DB_IS_CONSTRAINT_REVERSE_INDEX_FAMILY (cons->type)
		      || key_type->is_desc)
		    {
		      asc_desc[i] = 1;	/* Desc */
		    }
		  i++;

		  attp++;
		  key_type = key_type->next;
		}

	      if (*attp || key_type)
		{
		  if (error == NO_ERROR && (error = er_errid ()) == NO_ERROR)
		    {
		      error = ER_PARTITION_WORK_FAILED;
		    }
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_PARTITION_WORK_FAILED, 0);
		  goto fail_end;
		}

	      *attrnames = NULL;

	      for (objs = class_->users; objs; objs = objs->next)
		{
		  error = au_fetch_class (objs->op, &subcls,
					  AU_FETCH_READ, AU_SELECT);
		  if (error != NO_ERROR)
		    {
		      error = er_errid ();
		      goto fail_end;
		    }

		  if (!subcls->partition_of)
		    {
		      continue;	/* not partitioned */
		    }

		  if (alter_code == PT_ADD_PARTITION
		      || alter_code == PT_REORG_PARTITION
		      || alter_code == PT_ADD_HASHPARTITION)
		    {
		      parts = alter->info.alter.alter_clause.partition.parts;
		      for (; parts; parts = parts->next)
			{
			  if (alter_code == PT_REORG_PARTITION
			      && parts->partition_pruned)
			    {
			      continue;	/* reused partition */
			    }
			  if (ws_mop_compare (objs->op,
					      parts->info.parts.name->
					      info.name.db_object) == 0)
			    {
			      break;
			    }
			}
		      if (parts == NULL)
			{
			  continue;
			}
		    }
		  error = sm_add_index (objs->op,
					db_constraint_type (cons),
					cons->name, (const char **) namep,
					asc_desc, cons->attrs_prefix_length);
		  if (error != NO_ERROR)
		    {
		      break;
		    }
		}

	      free_and_init (namep);
	      free_and_init (asc_desc);
	    }

	fail_end:

	  if (namep != NULL)
	    {
	      free_and_init (namep);
	    }
	  if (asc_desc != NULL)
	    {
	      free_and_init (asc_desc);
	    }
	}

      if (error != NO_ERROR)
	{
	  goto alter_partition_fail;
	}

      if (alter_code == PT_REORG_PARTITION)
	{
	  delnames = NULL;
	  names = alter->info.alter.alter_clause.partition.name_list;
	  for (; names; names = parts)
	    {
	      parts = names->next;
	      if (names->partition_pruned)
		{		/* for delete partition */
		  if (delnames != NULL)
		    {
		      delnames->next = names;
		    }
		  else
		    {
		      alter->info.alter.alter_clause.partition.name_list =
			names;
		    }
		  delnames = names;
		  delnames->next = NULL;
		}
	      else
		{
		  names->next = NULL;
		  parser_free_tree (parser, names);
		}
	    }

	  if (delnames != NULL)
	    {
	      error = do_drop_partition_list (vclass, alter->
					      info.alter.alter_clause.
					      partition.name_list);
	      if (error != NO_ERROR)
		{
		  goto alter_partition_fail;
		}
	    }
	}
      break;

    case PT_COALESCE_PARTITION:
      error = do_update_partition_newly (entity_name, keycol);
      if (error != NO_ERROR)
	{
	  goto alter_partition_fail;
	}

      slist = NULL;
      coalesce_list = NULL;
      for (; coalesce_num < partnum; coalesce_num++)
	{
	  sprintf (partnum_str, "p%d", coalesce_num);
	  parts = pt_name (parser, partnum_str);
	  if (parts == NULL)
	    {
	      goto alter_partition_fail;
	    }
	  parts->next = NULL;
	  if (coalesce_list == NULL)
	    {
	      coalesce_list = parts;
	    }
	  else
	    {
	      slist->next = parts;
	    }
	  slist = parts;
	}

      error = do_drop_partition_list (vclass, coalesce_list);
      parser_free_tree (parser, coalesce_list);

      if (error != NO_ERROR)
	{
	  goto alter_partition_fail;
	}

      break;

    case PT_REMOVE_PARTITION:
      error = do_remove_partition_post (parser, entity_name, keycol);
      if (error != NO_ERROR)
	{
	  goto alter_partition_fail;
	}
      break;

    case PT_DROP_PARTITION:
      error = do_drop_partition_list (vclass, alter->
				      info.alter.alter_clause.partition.
				      name_list);
      if (error != NO_ERROR)
	{
	  goto alter_partition_fail;
	}
      break;

    default:
      break;
    }

  return NO_ERROR;

alter_partition_fail:
  if (partition_savepoint && error != NO_ERROR
      && error != ER_LK_UNILATERALLY_ABORTED)
    {
      (void) tran_abort_upto_savepoint (UNIQUE_PARTITION_SAVEPOINT_ALTER);
    }
  return error;
}

/*
 * do_alter_clause_rename_entity() - Executes an ALTER TABLE RENAME TO clause
 *   return: Error code
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of a PT_RENAME_ENTITY clause potentially
 *                  followed by the rest of the clauses in the ALTER
 *                  statement.
 * Note: The clauses following the PT_RENAME_ENTITY clause will be updated to
 *       the new name of the class.
 */
static int
do_alter_clause_rename_entity (PARSER_CONTEXT * const parser,
			       PT_NODE * const alter)
{
  int error_code = NO_ERROR;
  const PT_ALTER_CODE alter_code = alter->info.alter.code;
  const char *const old_name =
    alter->info.alter.entity_name->info.name.original;
  const char *const new_name =
    alter->info.alter.alter_clause.rename.new_name->info.name.original;
  PT_NODE *tmp_clause = NULL;

  assert (alter_code == PT_RENAME_ENTITY);
  assert (alter->info.alter.super.resolution_list == NULL);

  error_code = do_rename_internal (old_name, new_name);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  /* We now need to update the current name of the class for the rest of the
     ALTER clauses. */
  for (tmp_clause = alter->next; tmp_clause != NULL;
       tmp_clause = tmp_clause->next)
    {
      parser_free_tree (parser, tmp_clause->info.alter.entity_name);
      tmp_clause->info.alter.entity_name =
	parser_copy_tree (parser,
			  alter->info.alter.alter_clause.rename.new_name);
      if (tmp_clause->info.alter.entity_name == NULL)
	{
	  error_code = ER_FAILED;
	  goto error_exit;
	}
    }

  return error_code;

error_exit:
  return error_code;
}

/*
 * do_alter_clause_rename_entity() - Executes an ALTER TABLE DROP INDEX clause
 *   return: Error code
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of a PT_DROP_INDEX_CLAUSE clause potentially
 *                  followed by the rest of the clauses in the ALTER
 *                  statement.
 * Note: The clauses following the PT_DROP_INDEX_CLAUSE clause are not
 *       affected in any way.
 */
static int
do_alter_clause_drop_index (PARSER_CONTEXT * const parser,
			    PT_NODE * const alter)
{
  int error_code = NO_ERROR;
  const PT_ALTER_CODE alter_code = alter->info.alter.code;
  DB_OBJECT *obj = NULL;

  assert (alter_code == PT_DROP_INDEX_CLAUSE);
  assert (alter->info.alter.constraint_list != NULL);
  assert (alter->info.alter.constraint_list->next == NULL);
  assert (alter->info.alter.constraint_list->node_type == PT_NAME);

  obj = db_find_class (alter->info.alter.entity_name->info.name.original);
  if (obj == NULL)
    {
      error_code = er_errid ();
    }
  error_code =
    create_or_drop_index_helper (parser,
				 alter->info.alter.constraint_list->info.name.
				 original,
				 alter->info.alter.alter_clause.index.reverse,
				 alter->info.alter.alter_clause.index.unique,
				 NULL, NULL, obj, DO_INDEX_DROP);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  return error_code;

error_exit:
  return error_code;
}

/*
 * do_alter() -
 *   return: Error code
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of an alter statement
 */
int
do_alter (PARSER_CONTEXT * parser, PT_NODE * alter)
{
  int error_code = NO_ERROR;
  PT_NODE *crt_clause = NULL;
  bool do_semantic_checks = false;
  bool do_rollback = false;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      error_code = ER_AU_AUTHORIZATION_FAILURE;
      goto error_exit;
    }

  /* Multiple alter operations in a single statement need to be atomic. */
  error_code = tran_savepoint (UNIQUE_SAVEPOINT_MULTIPLE_ALTER, false);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }
  do_rollback = true;

  for (crt_clause = alter; crt_clause != NULL; crt_clause = crt_clause->next)
    {
      PT_NODE *const save_next = crt_clause->next;
      const PT_ALTER_CODE alter_code = crt_clause->info.alter.code;

      /* The first ALTER clause has already been checked, we call the semantic
         check starting with the second clause. */
      if (do_semantic_checks)
	{
	  PT_NODE *crt_result = NULL;

	  crt_clause->next = NULL;
	  crt_result = pt_compile (parser, crt_clause);
	  crt_clause->next = save_next;
	  if (!crt_result || pt_has_error (parser))
	    {
	      pt_report_to_ersys_with_statement (parser, PT_SEMANTIC,
						 crt_clause);
	      error_code = er_errid ();
	      goto error_exit;
	    }
	  assert (crt_result == crt_clause);
	}

      switch (alter_code)
	{
	case PT_RENAME_ENTITY:
	  error_code = do_alter_clause_rename_entity (parser, crt_clause);
	  break;
	case PT_DROP_INDEX_CLAUSE:
	  error_code = do_alter_clause_drop_index (parser, crt_clause);
	  break;
	default:
	  /* This code might not correctly handle a list of ALTER clauses so
	     we keep crt_clause->next to NULL during its execution just to be
	     on the safe side. */
	  crt_clause->next = NULL;
	  error_code = do_alter_one_clause_with_template (parser, crt_clause);
	  crt_clause->next = save_next;
	}

      if (error_code != NO_ERROR)
	{
	  goto error_exit;
	}
      do_semantic_checks = true;
    }

  return error_code;

error_exit:
  if (do_rollback)
    {
      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_MULTIPLE_ALTER);
    }
  return error_code;
}




/*
 * Function Group :
 * DO functions for user management
 *
 */

#define IS_NAME(n)      ((n)->node_type == PT_NAME)
#define IS_STRING(n)    ((n)->node_type == PT_VALUE &&          \
                         ((n)->type_enum == PT_TYPE_VARCHAR  || \
                          (n)->type_enum == PT_TYPE_CHAR     || \
                          (n)->type_enum == PT_TYPE_VARNCHAR || \
                          (n)->type_enum == PT_TYPE_NCHAR))
#define GET_NAME(n)     ((char *) (n)->info.name.original)
#define GET_STRING(n)   ((char *) (n)->info.value.data_value.str->bytes)

/*
 * do_grant() - Grants priviledges
 *   return: Error code if grant fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a grant statement
 */
int
do_grant (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  PT_NODE *user, *user_list;
  DB_OBJECT *user_obj, *class_mop;
  PT_NODE *auth_cmd_list, *auth_list, *auth;
  DB_AUTH db_auth;
  PT_NODE *spec_list, *s_list, *spec;
  PT_NODE *entity_list, *entity;
  int grant_option;

  CHECK_MODIFICATION_ERROR ();

  user_list = statement->info.grant.user_list;
  auth_cmd_list = statement->info.grant.auth_cmd_list;
  spec_list = statement->info.grant.spec_list;

  if (statement->info.grant.grant_option == PT_GRANT_OPTION)
    {
      grant_option = true;
    }
  else
    {
      grant_option = false;
    }

  for (user = user_list; user != NULL; user = user->next)
    {
      user_obj = db_find_user (user->info.name.original);
      if (user_obj == NULL)
	{
	  return er_errid ();
	}

      auth_list = auth_cmd_list;
      for (auth = auth_list; auth != NULL; auth = auth->next)
	{
	  db_auth = pt_auth_to_db_auth (auth);

	  s_list = spec_list;
	  for (spec = s_list; spec != NULL; spec = spec->next)
	    {
	      entity_list = spec->info.spec.flat_entity_list;
	      for (entity = entity_list; entity != NULL;
		   entity = entity->next)
		{
		  class_mop = db_find_class (entity->info.name.original);
		  if (class_mop == NULL)
		    {
		      return er_errid ();
		    }

		  error = db_grant (user_obj, class_mop, db_auth,
				    grant_option);
		  if (error != NO_ERROR)
		    {
		      return error;
		    }
		}
	    }
	}
    }

  return error;
}

/*
 * do_revoke() - Revokes priviledges
 *   return: Error code if revoke fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a revoke statement
 */
int
do_revoke (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;

  PT_NODE *user, *user_list;
  DB_OBJECT *user_obj, *class_mop;
  PT_NODE *auth_cmd_list, *auth_list, *auth;
  DB_AUTH db_auth;
  PT_NODE *spec_list, *s_list, *spec;
  PT_NODE *entity_list, *entity;

  CHECK_MODIFICATION_ERROR ();

  user_list = statement->info.revoke.user_list;
  auth_cmd_list = statement->info.revoke.auth_cmd_list;
  spec_list = statement->info.revoke.spec_list;

  for (user = user_list; user != NULL; user = user->next)
    {
      user_obj = db_find_user (user->info.name.original);
      if (user_obj == NULL)
	{
	  return er_errid ();
	}

      auth_list = auth_cmd_list;
      for (auth = auth_list; auth != NULL; auth = auth->next)
	{
	  db_auth = pt_auth_to_db_auth (auth);

	  s_list = spec_list;
	  for (spec = s_list; spec != NULL; spec = spec->next)
	    {
	      entity_list = spec->info.spec.flat_entity_list;
	      for (entity = entity_list; entity != NULL;
		   entity = entity->next)
		{
		  class_mop = db_find_class (entity->info.name.original);
		  if (class_mop == NULL)
		    {
		      return er_errid ();
		    }

		  error = db_revoke (user_obj, class_mop, db_auth);
		  if (error != NO_ERROR)
		    {
		      return error;
		    }
		}
	    }
	}
    }

  return error;
}

/*
 * do_create_user() - Create a user
 *   return: Error code if creation fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a create user statement
 */
int
do_create_user (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  DB_OBJECT *user, *group, *member;
  int exists;
  PT_NODE *node, *node2;
  const char *user_name, *password;
  const char *group_name, *member_name;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      return ER_AU_AUTHORIZATION_FAILURE;
    }

  if (statement == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  user = NULL;
  node = statement->info.create_user.user_name;
  if (node == NULL || node->node_type != PT_NAME
      || node->info.name.original == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_AU_MISSING_OR_INVALID_USER, 0);
      return ER_AU_MISSING_OR_INVALID_USER;
    }

  user_name = node->info.name.original;

  /* first, check if user_name is in group or member clause */
  for (node = statement->info.create_user.groups;
       node != NULL; node = node->next)
    {
      if (node == NULL || node->node_type != PT_NAME
	  || node->info.name.original == NULL)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_AU_MISSING_OR_INVALID_USER, 0);
	  return ER_AU_MISSING_OR_INVALID_USER;
	}

      group_name = node->info.name.original;
      if (strcasecmp (user_name, group_name) == 0)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_AU_MEMBER_CAUSES_CYCLES, 0);
	  return ER_AU_MEMBER_CAUSES_CYCLES;
	}
    }

  for (node = statement->info.create_user.members;
       node != NULL; node = node->next)
    {
      member_name = (node && IS_NAME (node)) ? GET_NAME (node) : NULL;
      if (member_name == NULL)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return ER_OBJ_INVALID_ARGUMENTS;
	}

      if (strcasecmp (user_name, member_name) == 0
	  || strcasecmp (member_name, AU_PUBLIC_USER_NAME) == 0)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_AU_MEMBER_CAUSES_CYCLES, 0);
	  return ER_AU_MEMBER_CAUSES_CYCLES;
	}
    }

  /* second, check if group name is in member clause */
  for (node = statement->info.create_user.groups;
       node != NULL; node = node->next)
    {
      group_name = (node && IS_NAME (node)) ? GET_NAME (node) : NULL;
      if (group_name == NULL)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return ER_OBJ_INVALID_ARGUMENTS;
	}

      for (node2 = statement->info.create_user.members;
	   node2 != NULL; node2 = node2->next)
	{
	  member_name = (node2 && IS_NAME (node2)) ? GET_NAME (node2) : NULL;
	  if (member_name == NULL)
	    {
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_OBJ_INVALID_ARGUMENTS, 0);
	      return ER_OBJ_INVALID_ARGUMENTS;
	    }

	  if (strcasecmp (group_name, member_name) == 0)
	    {
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_AU_MEMBER_CAUSES_CYCLES, 0);
	      return ER_AU_MEMBER_CAUSES_CYCLES;
	    }
	}
    }

  if (parser == NULL || statement == NULL || user_name == NULL)
    {
      error = ER_AU_INVALID_USER;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }
  else
    {
      exists = 0;

      user = db_add_user (user_name, &exists);
      if (user == NULL)
	{
	  error = er_errid ();
	}
      else if (exists)
	{
	  error = ER_AU_USER_EXISTS;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, user_name);
	}
      else
	{
	  node = statement->info.create_user.password;
	  password = (node && IS_STRING (node)) ? GET_STRING (node) : NULL;
	  if (error == NO_ERROR && password)
	    {
	      error = au_set_password (user, password);
	    }

	  node = statement->info.create_user.groups;
	  group_name = (node && IS_NAME (node)) ? GET_NAME (node) : NULL;
	  if (error == NO_ERROR && group_name)
	    do
	      {
		group = db_find_user (group_name);

		if (group == NULL)
		  {
		    error = er_errid ();
		  }
		else
		  {
		    error = db_add_member (group, user);
		  }

		node = node->next;
		group_name = (node
			      && IS_NAME (node)) ? GET_NAME (node) : NULL;
	      }
	    while (error == NO_ERROR && group_name);

	  node = statement->info.create_user.members;
	  member_name = (node && IS_NAME (node)) ? GET_NAME (node) : NULL;
	  if (error == NO_ERROR && member_name != NULL)
	    {
	      do
		{
		  member = db_find_user (member_name);

		  if (member == NULL)
		    {
		      error = er_errid ();
		    }
		  else
		    {
		      error = db_add_member (user, member);
		    }

		  node = node->next;
		  member_name = (node
				 && IS_NAME (node)) ? GET_NAME (node) : NULL;
		}
	      while (error == NO_ERROR && member_name != NULL);
	    }
	}

      if (error != NO_ERROR)
	{
	  if (user && exists == 0)
	    {
	      er_stack_push ();
	      db_drop_user (user);
	      er_stack_pop ();
	    }
	}
    }

  return error;
}

/*
 * do_drop_user() - Drop the user
 *   return: Error code if dropping fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a drop user statement
 */
int
do_drop_user (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  DB_OBJECT *user;
  PT_NODE *node;
  const char *user_name;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      return ER_AU_AUTHORIZATION_FAILURE;
    }

  if (statement == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  node = statement->info.create_user.user_name;
  user_name = (node && IS_NAME (node)) ? GET_NAME (node) : NULL;

  if (!parser || !statement || !user_name)
    {
      error = ER_AU_INVALID_USER;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }
  else
    {
      user = db_find_user (user_name);

      if (user == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = db_drop_user (user);
	}
    }

  return error;
}

/*
 * do_alter_user() - Change the user's password
 *   return: Error code if alter fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of an alter statement
 */
int
do_alter_user (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  DB_OBJECT *user;
  PT_NODE *node;
  const char *user_name, *password;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      return ER_AU_AUTHORIZATION_FAILURE;
    }

  if (statement == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  node = statement->info.alter_user.user_name;
  user_name = (node && IS_NAME (node)) ? GET_NAME (node) : NULL;

  if (!parser || !statement || !user_name)
    {
      error = ER_AU_INVALID_USER;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }
  else
    {
      user = db_find_user (user_name);

      if (user == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  node = statement->info.alter_user.password;
	  password = (node && IS_STRING (node)) ? GET_STRING (node) : NULL;

	  error = au_set_password (user, password);
	}
    }

  return error;
}

/*
 * Function Group :
 * Code for dropping a Classes by Parse Tree descriptions.
 *
 */

/*
 * drop_class_name() - This static routine drops a class by name.
 *   return: Error code
 *   name(in): Class name to drop
 */
static int
drop_class_name (const char *name)
{
  DB_OBJECT *class_mop;

  class_mop = db_find_class (name);

  if (class_mop)
    {
      return db_drop_class (class_mop);
    }
  else
    {
      /* if class is null, return the global error. */
      return er_errid ();
    }
}

/*
 * do_drop() - Drops a vclass, class
 *   return: Error code if a vclass is not deleted.
 *   parser(in): Parser context
 *   statement(in/out): Parse tree of a drop statement
 */
int
do_drop (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  int error = NO_ERROR;
  PT_NODE *entity_spec_list, *entity_spec;
  PT_NODE *entity;
  PT_NODE *entity_list;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      return ER_AU_AUTHORIZATION_FAILURE;
    }

  /* partitioned sub-class check */
  entity_spec_list = statement->info.drop.spec_list;
  for (entity_spec = entity_spec_list; entity_spec != NULL;
       entity_spec = entity_spec->next)
    {
      entity_list = entity_spec->info.spec.flat_entity_list;
      for (entity = entity_list; entity != NULL; entity = entity->next)
	{
	  if (do_is_partitioned_subclass (NULL, entity->info.name.original,
					  NULL))
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_INVALID_PARTITION_REQUEST, 0);
	      return er_errid ();
	    }
	}
    }

  entity_spec_list = statement->info.drop.spec_list;
  for (entity_spec = entity_spec_list; entity_spec != NULL;
       entity_spec = entity_spec->next)
    {
      entity_list = entity_spec->info.spec.flat_entity_list;
      for (entity = entity_list; entity != NULL; entity = entity->next)
	{
	  error = drop_class_name (entity->info.name.original);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
    }

  return error;
}

/*
 * update_locksets_for_multiple_rename() - Adds a class name to one of the two
 *                                         sets: either names to be reserved
 *                                         or classes to be locked
 *   return: Error code
 *   class_name(in): A class name involved in a rename operation
 *   num_mops(in/out): The number of MOPs
 *   mop_set(in/out): The MOPs to lock before the rename operation
 *   num_names(in/out): The number of class names
 *   name_set(in/out): The class names to reserve before the rename operation
 *   error_on_misssing_class(in/out): Whether to return an error if a class
 *                                    with the given class_name is not found
 */
int
update_locksets_for_multiple_rename (const char *class_name, int *num_mops,
				     MOP * mop_set, int *num_names,
				     char **name_set,
				     bool error_on_misssing_class)
{
  DB_OBJECT *class_mop = NULL;
  char realname[SM_MAX_IDENTIFIER_LENGTH];
  int i = 0;

  sm_downcase_name (class_name, realname, SM_MAX_IDENTIFIER_LENGTH);

  class_mop = db_find_class (realname);
  if (class_mop == NULL && error_on_misssing_class)
    {
      return er_errid ();
    }

  if (class_mop != NULL)
    {
      /* Classes that exist are locked. */
      /* Duplicates are harmless; they are handled by locator_fetch_set ()
         anyway. */
      mop_set[*num_mops] = class_mop;
      ++(*num_mops);
    }
  else
    {
      /* Class names that don't yet exist are reserved. */
      for (i = 0; i < *num_names; ++i)
	{
	  if (intl_mbs_casecmp (name_set[i], realname) == 0)
	    {
	      /* The class name is used more than once, we ignore its current
	         occurence. */
	      return NO_ERROR;
	    }
	}
      name_set[*num_names] = strdup (realname);
      if (name_set[*num_names] == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (strlen (realname) + 1) * sizeof (char));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      ++(*num_names);
    }
  return NO_ERROR;
}

/*
 * acquire_locks_for_multiple_rename() - Performs the necessary locking for an
 *                                       atomic multiple rename operation
 *   return: Error code
 *   statement(in): Parse tree of a rename statement
 *
 * Note: We need to lock all the classes and vclasses involved in the rename
 *       operation. When doing multiple renames we preventively lock all the
 *       names involved in the rename operation. For statements such as:
 *           RENAME A to tmp, B to A, tmp to B;
 *       "A" and "B" will be exclusively locked (locator_fetch_set ())
 *       and the name "tmp" will be reserved for renaming operations
 *       (locator_reserve_class_names ()).
 */
int
acquire_locks_for_multiple_rename (const PT_NODE * statement)
{
  int error = NO_ERROR;
  const PT_NODE *current_rename = NULL;
  int num_rename = 0;
  int num_mops = 0;
  MOP *mop_set = NULL;
  int num_names = 0;
  char **name_set = NULL;
  OID *oid_set = NULL;
  MOBJ fetch_result = NULL;
  LC_FIND_CLASSNAME reserve_result = LC_CLASSNAME_ERROR;
  int i = 0;

  num_rename = pt_length_of_list (statement);

  mop_set = (MOP *) malloc (2 * num_rename * sizeof (MOP));
  if (mop_set == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      2 * num_rename * sizeof (MOP));
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error_exit;
    }
  num_mops = 0;

  name_set = (char **) malloc (2 * num_rename * sizeof (char *));
  if (name_set == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      2 * num_rename * sizeof (char *));
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error_exit;
    }
  num_names = 0;

  for (current_rename = statement;
       current_rename != NULL; current_rename = current_rename->next)
    {
      const bool is_first_rename = current_rename == statement ? true : false;
      const char *old_name =
	current_rename->info.rename.old_name->info.name.original;
      const char *new_name =
	current_rename->info.rename.new_name->info.name.original;

      bool found = false;

      for (i = 0; i < num_names; i++)
	{
	  if (strcmp (name_set[i], old_name) == 0)
	    {
	      found = true;
	      break;
	    }
	}

      if (!found)
	{
	  error =
	    update_locksets_for_multiple_rename (old_name, &num_mops, mop_set,
						 &num_names, name_set, true);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }

	  if (is_first_rename)
	    {
	      /* We have made sure the first class to be renamed can be locked. */
	      assert (num_mops == 1);
	    }
	}

      error =
	update_locksets_for_multiple_rename (new_name, &num_mops, mop_set,
					     &num_names, name_set, false);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
      if (is_first_rename && num_names != 1)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_CLASSNAME_EXIST, 1,
		  new_name);
	  error = ER_LC_CLASSNAME_EXIST;
	  goto error_exit;
	}
      /* We have made sure the first name to be used can be reserved. */
    }

  assert (num_mops != 0 && num_names != 0);

  fetch_result = locator_fetch_set (num_mops, mop_set, DB_FETCH_WRITE,
				    DB_FETCH_WRITE, 1);
  if (fetch_result == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CANNOT_GET_LOCK, 0);
      error = ER_CANNOT_GET_LOCK;
      goto error_exit;
    }

  oid_set = (OID *) malloc (num_names * sizeof (OID));
  if (oid_set == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      num_names * sizeof (OID));
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error_exit;
    }

  for (i = 0; i < num_names; ++i)
    {
      /* Each reserved name will point to the OID of the first class to be
         renamed. This is ok as the associated transient table entries will
         only be used for the multiple rename operation. */
      COPY_OID (&oid_set[i], ws_oid (mop_set[0]));
    }

  reserve_result =
    locator_reserve_class_names (num_names, (const char **) name_set,
				 oid_set, DB_CLUSTER_NODE_LOCAL);
  if (reserve_result != LC_CLASSNAME_RESERVED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CANNOT_GET_LOCK, 0);
      error = ER_CANNOT_GET_LOCK;
      goto error_exit;
    }

error_exit:

  if (oid_set != NULL)
    {
      assert (num_names > 0);
      free_and_init (oid_set);
    }
  if (name_set != NULL)
    {
      for (i = 0; i < num_names; ++i)
	{
	  assert (name_set[i] != NULL);
	  free_and_init (name_set[i]);
	}
      free_and_init (name_set);
    }
  if (mop_set != NULL)
    {
      free_and_init (mop_set);
    }
  return error;
}

/*
 * do_rename() - Renames several vclasses or classes
 *   return: Error code
 *   parser(in): Parser context
 *   statement(in): Parse tree of a rename statement
 */
int
do_rename (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  const PT_NODE *current_rename = NULL;
  bool do_rollback = false;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      error = ER_AU_AUTHORIZATION_FAILURE;
      goto error_exit;
    }

  if (statement->next != NULL)
    {
      /* Multiple renaming operations in a single statement need to be
         atomic. */
      error = tran_savepoint (UNIQUE_SAVEPOINT_MULTIPLE_RENAME, false);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
      do_rollback = true;

      error = acquire_locks_for_multiple_rename (statement);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  for (current_rename = statement;
       current_rename != NULL; current_rename = current_rename->next)
    {
      const char *old_name =
	current_rename->info.rename.old_name->info.name.original;
      const char *new_name =
	current_rename->info.rename.new_name->info.name.original;

      error = do_rename_internal (old_name, new_name);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  return error;

error_exit:
  if (do_rollback)
    {
      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_MULTIPLE_RENAME);
    }
  return error;
}

static int
do_rename_internal (const char *const old_name, const char *const new_name)
{
  DB_OBJECT *old_class = NULL;

  if (do_is_partitioned_subclass (NULL, old_name, NULL))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INVALID_PARTITION_REQUEST,
	      0);
      return er_errid ();
    }

  old_class = db_find_class (old_name);
  if (old_class == NULL)
    {
      return er_errid ();
    }

  return db_rename_class (old_class, new_name);
}

/*
 * Function Group :
 * Parse tree to index commands translation.
 *
 */

static DB_CONSTRAINT_TYPE
get_reverse_unique_index_type (const bool is_reverse, const bool is_unique)
{
  if (is_unique)
    {
      return is_reverse ? DB_CONSTRAINT_REVERSE_UNIQUE : DB_CONSTRAINT_UNIQUE;
    }
  else
    {
      return is_reverse ? DB_CONSTRAINT_REVERSE_INDEX : DB_CONSTRAINT_INDEX;
    }
}

/*
 * create_or_drop_index_helper()
 *   return: Error code
 *   parser(in): Parser context
 *   constraint_name(in): If NULL the default constraint name is used;
 *                        column_names must be non-NULL in this case.
 *   is_reverse(in):
 *   is_unique(in):
 *   column_names(in): Can be NULL if dropping a constraint and providing the
 *                     constraint name.
 *   obj(in): Class object
 *   do_index(in) : The operation to be performed (creating or dropping)
 */
static int
create_or_drop_index_helper (const PARSER_CONTEXT * const parser,
			     const char *const constraint_name,
			     const bool is_reverse,
			     const bool is_unique,
			     const PT_NODE * const column_names,
			     const PT_NODE * const column_prefix_length,
			     DB_OBJECT * const obj, const DO_INDEX do_index)
{
  int error = NO_ERROR;
  int i = 0, nnames = 0;
  DB_CONSTRAINT_TYPE ctype = -1;
  const PT_NODE *c = NULL, *n = NULL;
  char **attnames = NULL;
  int *asc_desc = NULL;
  int *attrs_prefix_length = NULL;
  char *cname = NULL;
  char const *colname = NULL;
  bool mysql_index_name = false;

  nnames = pt_length_of_list (column_names);

  if (do_index == DO_INDEX_CREATE && nnames == 1 && column_prefix_length)
    {
      n = column_names->info.sort_spec.expr;
      if (n)
	{
	  colname = n->info.name.original;
	}

      if (colname && (sm_att_unique_constrained (obj, colname) ||
		      sm_att_fk_constrained (obj, colname)))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_SM_INDEX_PREFIX_LENGTH_ON_UNIQUE_FOREIGN, 0);

	  return ER_SM_INDEX_PREFIX_LENGTH_ON_UNIQUE_FOREIGN;
	}
    }

  attnames = (char **) malloc ((nnames + 1) * sizeof (const char *));
  if (attnames == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (nnames + 1) * sizeof (const char *));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  asc_desc = (int *) malloc ((nnames) * sizeof (int));
  if (asc_desc == NULL)
    {
      free_and_init (attnames);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      nnames * sizeof (int));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  error = sm_get_remote_info_if_need (obj);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (do_index == DO_INDEX_CREATE)
    {
      attrs_prefix_length = (int *) malloc ((nnames) * sizeof (int));
      if (attrs_prefix_length == NULL)
	{
	  free_and_init (attnames);
	  free_and_init (asc_desc);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, nnames * sizeof (int));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
    }

  for (c = column_names, i = 0; c != NULL; c = c->next, i++)
    {
      asc_desc[i] = c->info.sort_spec.asc_or_desc == PT_ASC ? 0 : 1;
      /* column name node */
      n = c->info.sort_spec.expr;
      attnames[i] = (char *) n->info.name.original;
      if (do_index == DO_INDEX_CREATE)
	{
	  attrs_prefix_length[i] = -1;
	}
    }
  attnames[i] = NULL;

  if (do_index == DO_INDEX_CREATE && nnames == 1 &&
      attrs_prefix_length && column_prefix_length)
    {
      attrs_prefix_length[0] = column_prefix_length->info.value.data_value.i;
    }

  ctype = get_reverse_unique_index_type (is_reverse, is_unique);

  if (PRM_COMPAT_MODE == COMPAT_MYSQL && ctype == DB_CONSTRAINT_INDEX
      && constraint_name != NULL && nnames == 0)
    {
      mysql_index_name = true;
    }

  cname = sm_produce_constraint_name (sm_class_name (obj), ctype,
				      (const char **) attnames,
				      asc_desc, constraint_name);
  if (cname == NULL)
    {
      error = er_errid ();
    }
  else
    {
      if (do_index == DO_INDEX_CREATE)
	{
	  error = sm_add_constraint (obj, ctype, cname,
				     (const char **) attnames,
				     asc_desc, attrs_prefix_length, false);
	}
      else
	{
	  assert (do_index == DO_INDEX_DROP);
	  error = sm_drop_constraint (obj, ctype, cname,
				      (const char **) attnames,
				      false, mysql_index_name);
	}
      sm_free_constraint_name (cname);
    }

  free_and_init (attnames);
  free_and_init (asc_desc);
  if (attrs_prefix_length)
    {
      free_and_init (attrs_prefix_length);
    }

  return error;
}

/*
 * do_create_index() - Creates an index
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   statement(in) : Parse tree of a create index statement
 */
int
do_create_index (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  PT_NODE *cls;
  DB_OBJECT *obj;
  const char *index_name = NULL;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      return ER_AU_AUTHORIZATION_FAILURE;
    }

  cls = statement->info.index.indexed_class;

  obj = db_find_class (cls->info.name.original);
  if (obj == NULL)
    {
      return er_errid ();
    }

  index_name = statement->info.index.index_name ?
    statement->info.index.index_name->info.name.original : NULL;
  return
    create_or_drop_index_helper (parser, index_name,
				 statement->info.index.reverse,
				 statement->info.index.unique,
				 statement->info.index.column_names,
				 statement->info.index.prefix_length,
				 obj, DO_INDEX_CREATE);
}

/*
 * do_drop_index() - Drops an index on a class.
 *   return: Error code if it fails
 *   parser(in) : Parser context
 *   statement(in): Parse tree of a drop index statement
 */
int
do_drop_index (PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  PT_NODE *cls = NULL;
  DB_OBJECT *obj = NULL;
  bool free_cls = false;
  const char *index_name = NULL;
  int error_code = NO_ERROR;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      error_code = ER_AU_AUTHORIZATION_FAILURE;
      goto error_exit;
    }

  index_name = statement->info.index.index_name ?
    statement->info.index.index_name->info.name.original : NULL;

  cls = statement->info.index.indexed_class;
  if (cls == NULL)
    {
      DB_CONSTRAINT_TYPE index_type;

      if (index_name == NULL)
	{
	  error_code = ER_SM_INVALID_DEF_CONSTRAINT_NAME_PARAMS;
	  goto error_exit;
	}
      index_type =
	get_reverse_unique_index_type (statement->info.index.reverse,
				       statement->info.index.unique);
      cls = pt_find_class_of_index (parser, index_name, index_type);

      if (cls == NULL)
	{
	  error_code = er_errid ();
	  goto error_exit;
	}
      free_cls = true;
    }

  obj = db_find_class (cls->info.name.original);
  if (obj == NULL)
    {
      error_code = er_errid ();
      goto error_exit;
    }

  cls->info.name.db_object = obj;
  pt_check_user_owns_class (parser, cls);

  if (free_cls)
    {
      parser_free_tree (parser, cls);
      cls = NULL;
      free_cls = false;
    }
  error_code =
    create_or_drop_index_helper (parser, index_name,
				 statement->info.index.reverse,
				 statement->info.index.unique,
				 statement->info.index.column_names,
				 statement->info.index.prefix_length,
				 obj, DO_INDEX_DROP);
  return error_code;

error_exit:
  if (free_cls)
    {
      assert (cls != NULL);
      parser_free_tree (parser, cls);
      cls = NULL;
      free_cls = false;
    }
  return error_code;
}

/*
 * do_alter_index() - Alters an index on a class.
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a alter index statement
 */
int
do_alter_index (PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  DB_OBJECT *obj;
  PT_NODE *n, *c;
  PT_NODE *cls = NULL;
  bool free_cls = false;
  int i, nnames;
  DB_CONSTRAINT_TYPE ctype;
  char **attnames = NULL;
  int *asc_desc = NULL;
  int *attrs_prefix_length = NULL;
  char *cname;
  SM_CLASS *smcls;
  SM_CLASS_CONSTRAINT *idx;
  SM_ATTRIBUTE **attp;
  int attnames_allocated = 0;
  const char *index_name = NULL;

  /* TODO refactor this code, the code in create_or_drop_index_helper and the
     code in do_drop_index in order to remove duplicate code */

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      error = ER_AU_AUTHORIZATION_FAILURE;
      goto error_exit;
    }

  index_name = statement->info.index.index_name ?
    statement->info.index.index_name->info.name.original : NULL;

  cls = statement->info.index.indexed_class;
  if (cls == NULL)
    {
      if (index_name == NULL)
	{
	  error = ER_SM_INVALID_DEF_CONSTRAINT_NAME_PARAMS;
	  goto error_exit;
	}
      ctype = get_reverse_unique_index_type (statement->info.index.reverse,
					     statement->info.index.unique);
      cls = pt_find_class_of_index (parser, index_name, ctype);

      if (cls == NULL)
	{
	  error = er_errid ();
	  goto error_exit;
	}
      free_cls = true;
    }

  obj = db_find_class (cls->info.name.original);
  if (obj == NULL)
    {
      error = er_errid ();
      goto error_exit;
    }
  cls->info.name.db_object = obj;
  pt_check_user_owns_class (parser, cls);
  if (free_cls)
    {
      parser_free_tree (parser, cls);
      cls = NULL;
      free_cls = false;
    }

  error = sm_get_remote_info_if_need (obj);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  ctype = get_reverse_unique_index_type (statement->info.index.reverse,
					 statement->info.index.unique);

  if (statement->info.index.column_names == NULL)
    {
      /* find the attributes of the index */
      idx = NULL;

      if (au_fetch_class (obj, &smcls, AU_FETCH_READ, AU_SELECT) != NO_ERROR)
	{
	  error = er_errid ();
	  goto error_exit;
	}

      if ((idx = classobj_find_class_index (smcls, index_name)) == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_SM_NO_INDEX, 1, index_name);
	  return er_errid ();
	}

      attp = idx->attributes;
      if (attp == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ATTRIBUTE, 1, "unknown");
	  return er_errid ();
	}

      nnames = 0;
      while (*attp++)
	{
	  nnames++;
	}

      attnames = (char **) malloc ((nnames + 1) * sizeof (const char *));
      if (attnames == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1,
		  (nnames + 1) * sizeof (const char *));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      attnames_allocated = 1;

      for (i = 0, attp = idx->attributes; *attp; i++, attp++)
	{
	  attnames[i] = strdup ((*attp)->header.name);
	  if (attnames[i] == NULL)
	    {
	      int j;
	      for (j = 0; j < i; ++j)
		{
		  free_and_init (attnames[j]);
		}
	      free_and_init (attnames);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      (nnames + 1) * sizeof (const char *));
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	}
      attnames[i] = NULL;

      if (idx->asc_desc)
	{
	  asc_desc = (int *) malloc ((nnames) * sizeof (int));
	  if (asc_desc == NULL)
	    {
	      free_and_init (attnames);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, nnames * sizeof (int));
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }

	  for (i = 0; i < nnames; i++)
	    {
	      asc_desc[i] = idx->asc_desc[i];
	    }
	}

      if (ctype == DB_CONSTRAINT_INDEX)
	{
	  assert (idx->attrs_prefix_length);

	  attrs_prefix_length = (int *) malloc ((nnames) * sizeof (int));
	  if (attrs_prefix_length == NULL)
	    {
	      free_and_init (attnames);
	      free_and_init (asc_desc);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, nnames * sizeof (int));
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }

	  for (i = 0; i < nnames; i++)
	    {
	      attrs_prefix_length[i] = idx->attrs_prefix_length[i];
	    }
	}
    }
  else
    {
      nnames = pt_length_of_list (statement->info.index.column_names);
      attnames = (char **) malloc ((nnames + 1) * sizeof (const char *));
      if (attnames == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (nnames + 1) * sizeof (const char *));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      asc_desc = (int *) malloc ((nnames) * sizeof (int));
      if (asc_desc == NULL)
	{
	  free_and_init (attnames);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, nnames * sizeof (int));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      for (c = statement->info.index.column_names, i = 0; c != NULL;
	   c = c->next, i++)
	{
	  asc_desc[i] = c->info.sort_spec.asc_or_desc == PT_ASC ? 0 : 1;
	  n = c->info.sort_spec.expr;	/* column name node */
	  attnames[i] = (char *) n->info.name.original;
	}
      attnames[i] = NULL;
    }

  cname = sm_produce_constraint_name (sm_class_name (obj), ctype,
				      (const char **) attnames,
				      asc_desc, index_name);
  if (cname == NULL)
    {
      if (error == NO_ERROR && (error = er_errid ()) == NO_ERROR)
	{
	  error = ER_GENERIC_ERROR;
	}
    }
  else
    {
      /* preserve prefix index when only the column names are specified */
      if (ctype == DB_CONSTRAINT_INDEX && !attrs_prefix_length &&
	  statement->info.index.column_names && !index_name)
	{
	  if (au_fetch_class (obj, &smcls, AU_FETCH_READ, AU_SELECT) !=
	      NO_ERROR)
	    {
	      error = er_errid ();
	    }
	  else
	    {
	      if ((idx = classobj_find_class_index (smcls, cname)) == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_SM_NO_INDEX, 1, cname);
		  error = er_errid ();
		}
	      else
		{
		  assert (idx->attrs_prefix_length);

		  attrs_prefix_length =
		    (int *) malloc ((nnames) * sizeof (int));
		  if (attrs_prefix_length == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY,
			      1, nnames * sizeof (int));
		      error = er_errid ();
		    }
		  else
		    {
		      for (i = 0; i < nnames; i++)
			{
			  attrs_prefix_length[i] =
			    idx->attrs_prefix_length[i];
			}
		    }
		}
	    }
	}

      if (error == NO_ERROR)
	{
	  error = sm_drop_constraint (obj, ctype, cname,
				      (const char **) attnames, false, false);
	  if (error == NO_ERROR)
	    {
	      error = sm_add_constraint (obj, ctype, cname,
					 (const char **) attnames,
					 asc_desc, attrs_prefix_length,
					 false);
	    }
	  sm_free_constraint_name (cname);
	}
    }

  if (attnames_allocated)
    {
      for (i = 0; attnames[i]; i++)
	{
	  free_and_init (attnames[i]);
	}
    }

  if (attnames)
    {
      free_and_init (attnames);
    }
  if (asc_desc)
    {
      free_and_init (asc_desc);
    }
  if (attrs_prefix_length)
    {
      free_and_init (attrs_prefix_length);
    }

  return error;

error_exit:
  if (free_cls)
    {
      assert (cls != NULL);
      parser_free_tree (parser, cls);
      cls = NULL;
      free_cls = false;
    }
  return error;
}

/*
 * Function Group :
 * Code for partition
 *
 */

enum ATTR_FOUND
{
  PATTR_NOT_FOUND = 0,
  PATTR_NAME,
  PATTR_VALUE,
  PATTR_NAME_VALUE,
  PATTR_COLUMN = 4,
  PATTR_KEY = 8
};

typedef struct part_class_info PART_CLASS_INFO;
struct part_class_info
{
  char *pname;
  DB_CTMPL *temp;
  DB_OBJECT *obj;
  PART_CLASS_INFO *next;
};

typedef struct pruning_info PRUNING_INFO;
struct pruning_info
{
  PARSER_CONTEXT *parser;
  PT_NODE *expr;
  DB_VALUE *attr;
  PT_NODE *ppart;		/* PT_NAME, original, db_object, location(temporary use) */
  SM_CLASS *smclass;
  int type, size, wrkmap, expr_cnt, and_or;
  UINTPTR spec;
};

typedef struct db_value_slist DB_VALUE_SLIST;
struct db_value_slist
{
  struct db_value_slist *next;
  MOP partition_of;
  DB_VALUE *min;
  DB_VALUE *max;
};

typedef enum
{
  RANGES_LESS = -1,		/* less than */
  RANGES_EQUAL = 0,		/* equal */
  RANGES_GREATER = 1,		/* greater than */
  RANGES_ERROR = 2		/* error */
} MERGE_CHECK_RESULT;

static int insert_partition_catalog (PARSER_CONTEXT * parser,
				     DB_CTMPL * clstmpl, PT_NODE * node,
				     char *base_obj, char *cata_obj,
				     DB_VALUE * minval);
static PT_NODE *replace_name_with_value (PARSER_CONTEXT * parser,
					 PT_NODE * node, void *void_arg,
					 int *continue_walk);
static PT_NODE *adjust_name_with_type (PARSER_CONTEXT * parser,
				       PT_NODE * node, void *void_arg,
				       int *continue_walk);
static DB_VALUE *evaluate_partition_expr (DB_VALUE * expr, DB_VALUE * ival);
static int apply_partition_list_search (SM_CLASS * smclass, DB_VALUE * sval,
					char *retbuf);
static int apply_partition_range_search (SM_CLASS * smclass, DB_VALUE * sval,
					 char *retbuf);
static PT_NODE *find_partition_attr (PARSER_CONTEXT * parser, PT_NODE * node,
				     void *void_arg, int *continue_walk);
static int check_same_expr (PARSER_CONTEXT * parser, PT_NODE * p,
			    PT_NODE * q);
static int evaluate_partition_range (PARSER_CONTEXT * parser, PT_NODE * expr);
static PT_NODE *convert_expr_to_constant (PARSER_CONTEXT * parser,
					  PT_NODE * node, void *void_arg,
					  int *continue_walk);
static PT_NODE *get_pruned_partition_spec (PRUNING_INFO * ppi, MOP subobj);
static void add_pruned_partition_part (PT_NODE * subspec, PRUNING_INFO * ppi,
				       MOP subcls, char *cname);
static int adjust_pruned_partition (PT_NODE * spec, PRUNING_INFO * ppi);
static int increase_value (DB_VALUE * val);
static int decrease_value (DB_VALUE * val);
static int check_hash_range (PRUNING_INFO * ppi, char *partmap,
			     PT_OP_TYPE op, PT_NODE * from_expr,
			     PT_NODE * to_expr, int setval);
static int select_hash_partition (PRUNING_INFO * ppi, PT_NODE * expr);
static int select_range_partition (PRUNING_INFO * ppi, PT_NODE * expr);
static int select_list_partition (PRUNING_INFO * ppi, PT_NODE * expr);
static bool select_range_list (PRUNING_INFO * ppi, PT_NODE * cond);
static bool make_attr_search_value (int and_or, PT_NODE * incond,
				    PRUNING_INFO * ppi);
static PT_NODE *apply_no_pruning (PT_NODE * spec, PRUNING_INFO * ppi);
static MERGE_CHECK_RESULT check_range_merge (DB_VALUE * val1, PT_OP_TYPE op1,
					     DB_VALUE * val2, PT_OP_TYPE op2);
static int is_ranges_meetable (DB_VALUE * aval1, PT_OP_TYPE aop1,
			       DB_VALUE * aval2, PT_OP_TYPE aop2,
			       DB_VALUE * bval1, PT_OP_TYPE bop1,
			       DB_VALUE * bval2, PT_OP_TYPE bop2);
static int is_in_range (DB_VALUE * aval1, PT_OP_TYPE aop1,
			DB_VALUE * aval2, PT_OP_TYPE aop2, DB_VALUE * bval);
static int adjust_partition_range (DB_OBJLIST * objs);
static int adjust_partition_size (MOP class_);
static const char *get_attr_name (PT_NODE * attribute);
static int do_add_attribute (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
			     PT_NODE * atts, bool error_on_not_normal);
static int do_add_attribute_from_select_column (DB_CTMPL * ctemplate,
						DB_QUERY_TYPE * column);
static DB_QUERY_TYPE *query_get_column_with_name (DB_QUERY_TYPE *
						  query_columns,
						  const char *name);
static PT_NODE *get_attribute_with_name (PT_NODE * atts, const char *name);
static PT_NODE *create_select_to_insert_into (PARSER_CONTEXT * parser,
					      const char *class_name,
					      PT_NODE * create_select,
					      PT_CREATE_SELECT_ACTION
					      create_select_action,
					      DB_QUERY_TYPE * query_columns);
static int execute_create_select_query (PARSER_CONTEXT * parser,
					const char *const class_name,
					PT_NODE * create_select,
					PT_CREATE_SELECT_ACTION
					create_select_action,
					DB_QUERY_TYPE * query_columns,
					PT_NODE * flagged_statement);

/*
 * do_create_partition() -  Creates partitions
 *   return: Error code if partitions are not created
 *   parser(in): Parser context
 *   node(in): The parse tree of a create class
 *   class_obj(in):
 *   clstmpl(in):
 *
 * Note:
 */
int
do_create_partition (PARSER_CONTEXT * parser, PT_NODE * node,
		     DB_OBJECT * class_obj, DB_CTMPL * clstmpl)
{
  int error;
  PT_NODE *pinfo, *hash_parts, *newparts, *hashtail;
  PT_NODE *parts, *parts_save, *fmin;
  PT_NODE *parttemp, *names;
  PART_CLASS_INFO pci = { NULL, NULL, NULL, NULL };
  PART_CLASS_INFO *newpci, *wpci;
  char class_name[DB_MAX_IDENTIFIER_LENGTH];
  DB_VALUE *minval, *parts_val, *fmin_val, partsize, delval;
  int part_cnt = 0, part_add = -1;
  int size;
  int save;
  SM_CLASS *smclass;
  bool reuse_oid = false;
  bool is_global;
  PT_NODE *node_info, *cur_node_info;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      return ER_AU_AUTHORIZATION_FAILURE;
    }

  pinfo = hash_parts = newparts = hashtail = NULL;
  parts = parts_save = fmin = NULL;
  node_info = cur_node_info = NULL;

  is_global =
    node->info.create_entity.global_entity == PT_GLOBAL ? true : false;

  if (node->node_type == PT_ALTER)
    {
      if (is_global)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_INVALID_PARTITION_REQUEST, 0);
	  return er_errid ();
	}

      pinfo = node->info.alter.alter_clause.partition.info;
      if (node->info.alter.code == PT_ADD_PARTITION
	  || node->info.alter.code == PT_REORG_PARTITION)
	{
	  parts = node->info.alter.alter_clause.partition.parts;
	  part_add = parts->info.parts.type;
	}
      else if (node->info.alter.code == PT_ADD_HASHPARTITION)
	{
	  part_add = PT_PARTITION_HASH;
	}

      intl_mbs_lower ((char *) node->info.alter.entity_name->info.name.
		      original, class_name);
    }
  else if (node->node_type == PT_CREATE_ENTITY)
    {
      pinfo = node->info.create_entity.partition_info;
      intl_mbs_lower ((char *) node->info.create_entity.entity_name->info.
		      name.original, class_name);
    }
  else
    {
      return NO_ERROR;
    }

  if (is_global)
    {
      node_info = pinfo == NULL ? NULL : pinfo->info.partition.on_node;
      if (node_info == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_INVALID_PARTITION_REQUEST, 0);
	  return ER_INVALID_PARTITION_REQUEST;
	}
    }

  if (part_add == -1)
    {				/* create or apply partition */
      if (!pinfo)
	{
	  return NO_ERROR;
	}

      parts = pinfo->info.partition.parts;
    }

  parts_save = parts;
  parttemp = parser_new_node (parser, PT_CREATE_ENTITY);
  if (parttemp == NULL)
    {
      error = er_errid ();
      goto end_create;
    }

  error = au_fetch_class (class_obj, &smclass, AU_FETCH_READ, AU_SELECT);
  if (error != NO_ERROR)
    {
      error = er_errid ();
      goto end_create;
    }

  reuse_oid = (smclass->flags & SM_CLASSFLAG_REUSE_OID) ? true : false;

  parttemp->info.create_entity.entity_type = PT_CLASS;
  parttemp->info.create_entity.global_entity = is_global ? PT_GLOBAL : 0;
  parttemp->info.create_entity.entity_name =
    parser_new_node (parser, PT_NAME);
  parttemp->info.create_entity.supclass_list =
    parser_new_node (parser, PT_NAME);
  if (parttemp->info.create_entity.entity_name == NULL
      || parttemp->info.create_entity.supclass_list == NULL)
    {
      error = er_errid ();
      goto end_create;
    }
  parttemp->info.create_entity.supclass_list->info.name.db_object = class_obj;

  error = NO_ERROR;
  if (part_add == PT_PARTITION_HASH
      || (pinfo
	  && pinfo->node_type != PT_VALUE
	  && pinfo->info.partition.type == PT_PARTITION_HASH))
    {
      int pi, org_hashsize, new_hashsize;

      hash_parts = parser_new_node (parser, PT_PARTS);
      if (hash_parts == NULL)
	{
	  error = er_errid ();
	  goto end_create;
	}
      hash_parts->info.parts.name = parser_new_node (parser, PT_NAME);
      if (hash_parts->info.parts.name == NULL)
	{
	  error = er_errid ();
	  goto end_create;
	}

      hash_parts->info.parts.type = PT_PARTITION_HASH;
      if (part_add == PT_PARTITION_HASH)
	{
	  org_hashsize = do_get_partition_size (class_obj);
	  if (org_hashsize < 0)
	    {
	      error = er_errid ();
	      goto end_create;
	    }
	  new_hashsize
	    = node->info.alter.alter_clause.partition.size->info.value.
	    data_value.i;
	}
      else
	{
	  org_hashsize = 0;
	  new_hashsize =
	    pinfo->info.partition.hashsize->info.value.data_value.i;
	}

      for (pi = 0; pi < new_hashsize; pi++)
	{
	  newpci = (PART_CLASS_INFO *) malloc (sizeof (PART_CLASS_INFO));
	  if (newpci == NULL)
	    {
	      error = er_errid ();
	      goto end_create;
	    }

	  memset (newpci, 0x0, sizeof (PART_CLASS_INFO));

	  newpci->next = pci.next;
	  pci.next = newpci;

	  newpci->pname = (char *) malloc (strlen (class_name) + 5 + 13);
	  if (newpci->pname == NULL)
	    {
	      error = er_errid ();
	      goto end_create;
	    }

	  sprintf (newpci->pname, "%s" PARTITIONED_SUB_CLASS_TAG "p%d",
		   class_name, pi + org_hashsize);
	  if (strlen (newpci->pname) >= PARTITION_VARCHAR_LEN)
	    {
	      error = ER_INVALID_PARTITION_REQUEST;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	      goto end_create;
	    }
	  newpci->temp = dbt_create_class (newpci->pname);
	  if (newpci->temp == NULL)
	    {
	      error = er_errid ();
	      goto end_create;
	    }

	  parttemp->info.create_entity.entity_name->info.name.original
	    = newpci->pname;
	  parttemp->info.create_entity.supclass_list->info.name.original
	    = class_name;

	  if (cur_node_info == NULL)
	    {
	      cur_node_info = node_info;
	    }

	  parttemp->info.create_entity.on_node = cur_node_info;
	  cur_node_info = cur_node_info == NULL ? NULL : cur_node_info->next;

	  error = do_create_local (parser, newpci->temp, parttemp, NULL);
	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (newpci->temp);
	      goto end_create;
	    }

	  newpci->temp->partition_parent_atts = smclass->attributes;
	  newpci->obj = dbt_finish_class (newpci->temp);
	  if (newpci->obj == NULL)
	    {
	      dbt_abort_class (newpci->temp);
	      error = er_errid ();
	      goto end_create;
	    }

	  if (!is_global)
	    {
	      if (reuse_oid)
		{
		  error =
		    sm_set_class_flag (newpci->obj, SM_CLASSFLAG_REUSE_OID,
				       1);
		  if (error != NO_ERROR)
		    {
		      goto end_create;
		    }
		}

	      if (locator_create_heap_if_needed (newpci->obj, reuse_oid) ==
		  NULL)
		{
		  error = (er_errid () != NO_ERROR) ? er_errid () : ER_FAILED;
		  goto end_create;
		}
	    }

	  hash_parts->info.parts.name->info.name.original
	    = strstr (newpci->pname, PARTITIONED_SUB_CLASS_TAG)
	    + strlen (PARTITIONED_SUB_CLASS_TAG);
	  hash_parts->info.parts.values = NULL;

	  error = insert_partition_catalog (parser, NULL, hash_parts,
					    class_name, newpci->pname, NULL);
	  if (error != NO_ERROR)
	    {
	      goto end_create;
	    }
	  if (part_add == PT_PARTITION_HASH)
	    {
	      hash_parts->next = NULL;
	      hash_parts->info.parts.name->info.name.db_object = newpci->obj;
	      newparts = parser_copy_tree (parser, hash_parts);
	      if (node->info.alter.alter_clause.partition.parts == NULL)
		{
		  node->info.alter.alter_clause.partition.parts = newparts;
		}
	      else
		{
		  if (hashtail != NULL)
		    {
		      hashtail->next = newparts;
		    }
		}

	      hashtail = newparts;
	    }
	  error = NO_ERROR;
	  parttemp->info.create_entity.on_node = NULL;
	}
    }
  else
    {				/* RANGE or LIST */
      char *part_name;

      for (; parts; parts = parts->next, part_cnt++)
	{
	  newpci = (PART_CLASS_INFO *) malloc (sizeof (PART_CLASS_INFO));
	  if (newpci == NULL)
	    {
	      error = er_errid ();
	      goto end_create;
	    }

	  memset (newpci, 0x0, sizeof (PART_CLASS_INFO));

	  newpci->next = pci.next;
	  pci.next = newpci;

	  part_name = (char *) parts->info.parts.name->info.name.original;
	  size = strlen (class_name) + 5 + 1 + strlen (part_name);

	  newpci->pname = (char *) malloc (size);
	  if (newpci->pname == NULL)
	    {
	      error = er_errid ();
	      goto end_create;
	    }
	  sprintf (newpci->pname, "%s" PARTITIONED_SUB_CLASS_TAG "%s",
		   class_name, part_name);

	  if (strlen (newpci->pname) >= PARTITION_VARCHAR_LEN)
	    {
	      error = ER_INVALID_PARTITION_REQUEST;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	      goto end_create;
	    }

	  if (node->info.alter.code == PT_REORG_PARTITION
	      && parts->partition_pruned)
	    {			/* reused partition */
	      error = insert_partition_catalog (parser, NULL, parts,
						class_name,
						newpci->pname, NULL);
	      if (error != NO_ERROR)
		{
		  goto end_create;	/* reorg partition info update */
		}
	      error = NO_ERROR;
	      continue;
	    }

	  newpci->temp = dbt_create_class (newpci->pname);
	  if (newpci->temp == NULL)
	    {
	      error = er_errid ();
	      goto end_create;
	    }

	  parttemp->info.create_entity.entity_name->info.name.original
	    = newpci->pname;
	  parttemp->info.create_entity.supclass_list->info.name.original
	    = class_name;

	  error = do_create_local (parser, newpci->temp, parttemp, NULL);
	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (newpci->temp);
	      goto end_create;
	    }

	  newpci->temp->partition_parent_atts = smclass->attributes;
	  newpci->obj = dbt_finish_class (newpci->temp);
	  if (newpci->obj == NULL)
	    {
	      dbt_abort_class (newpci->temp);
	      error = er_errid ();
	      goto end_create;
	    }

	  if (reuse_oid)
	    {
	      error = sm_set_class_flag (newpci->obj, SM_CLASSFLAG_REUSE_OID,
					 1);
	      if (error != NO_ERROR)
		{
		  error = er_errid ();
		  goto end_create;
		}
	    }
	  if (locator_create_heap_if_needed (newpci->obj, reuse_oid) == NULL
	      || locator_flush_class (newpci->obj) != NO_ERROR)
	    {
	      error = er_errid ();
	      goto end_create;
	    }

	  /* RANGE-MIN VALUE search */
	  minval = NULL;
	  if ((pinfo
	       && pinfo->node_type != PT_VALUE
	       && pinfo->info.partition.type == PT_PARTITION_RANGE)
	      || part_add == PT_PARTITION_RANGE)
	    {
	      parts_val = pt_value_to_db (parser, parts->info.parts.values);
	      for (fmin = parts_save; fmin; fmin = fmin->next)
		{
		  if (fmin == parts)
		    {
		      continue;
		    }
		  if (fmin->info.parts.values == NULL)
		    {
		      continue;	/* RANGE-MAXVALUE */
		    }
		  fmin_val = pt_value_to_db (parser, fmin->info.parts.values);
		  if (fmin_val == NULL)
		    {
		      continue;
		    }
		  if (parts->info.parts.values == NULL
		      || db_value_compare (parts_val, fmin_val) == DB_GT)
		    {
		      if (minval == NULL)
			{
			  minval = fmin_val;
			}
		      else
			{
			  if (db_value_compare (minval, fmin_val) == DB_LT)
			    {
			      minval = fmin_val;
			    }
			}
		    }
		}
	    }
	  if (part_add == PT_PARTITION_RANGE
	      && minval == NULL && pinfo && pinfo->node_type == PT_VALUE)
	    {
	      /* set in pt_check_alter_partition */
	      minval = pt_value_to_db (parser, pinfo);
	    }
	  parts->info.parts.name->info.name.db_object = newpci->obj;
	  error = insert_partition_catalog (parser, NULL, parts,
					    class_name, newpci->pname,
					    minval);
	  if (error != NO_ERROR)
	    {
	      goto end_create;
	    }
	  error = NO_ERROR;
	}
    }

  if (part_add != -1)
    {				/* partition size update */
      adjust_partition_size (class_obj);

      if (node->info.alter.code == PT_REORG_PARTITION)
	{
	  AU_DISABLE (save);
	  db_make_string (&delval, "DEL");
	  for (names = node->info.alter.alter_clause.partition.name_list;
	       names; names = names->next)
	    {
	      if (names->partition_pruned)
		{		/* for delete partition */
		  error =
		    db_put_internal (names->info.name.db_object,
				     PARTITION_ATT_PEXPR, &delval);
		  if (error != NO_ERROR)
		    {
		      break;
		    }
		}
	    }
	  pr_clear_value (&delval);
	  AU_ENABLE (save);
	  if (error != NO_ERROR)
	    {
	      goto end_create;
	    }
	  if (part_add == PT_PARTITION_RANGE)
	    {
	      error
		= au_fetch_class (class_obj, &smclass, AU_FETCH_READ,
				  AU_SELECT);
	      if (error != NO_ERROR)
		{
		  goto end_create;
		}
	      adjust_partition_range (smclass->users);
	    }
	}
    }
  else
    {				/* set parent's partition info */
      db_make_int (&partsize, part_cnt);
      error = insert_partition_catalog (parser, clstmpl, pinfo, class_name,
					class_name, &partsize);
    }

end_create:
  for (wpci = pci.next; wpci;)
    {
      if (wpci->pname)
	{
	  free_and_init (wpci->pname);
	}
      newpci = wpci;
      wpci = wpci->next;
      free_and_init (newpci);
    }
  if (parttemp != NULL)
    {
      parser_free_tree (parser, parttemp);
    }
  if (error != NO_ERROR)
    {
      return error;
    }
  return NO_ERROR;
}

/*
 * insert_partition_catalog() -
 *   return: Error code
 *   parser(in): Parser context
 *   clstmpl(in/out): Template of sm_class
 *   node(in): Parser tree of an partition class
 *   base_obj(in): Partition class name
 *   cata_obj(in): Catalog class name
 *   minval(in):
 *
 * Note:
 */
static int
insert_partition_catalog (PARSER_CONTEXT * parser, DB_CTMPL * clstmpl,
			  PT_NODE * node, char *base_obj,
			  char *cata_obj, DB_VALUE * minval)
{
  MOP partcata, classcata, newpart, newclass, base_in_ctclass, base_class;
  DB_OTMPL *otmpl = NULL;
  DB_CTMPL *ctmpl;
  DB_VALUE val, *ptval, *hashsize;
  PT_NODE *parts;
  char *query, *query_str = NULL, *p;
  DB_COLLECTION *dbc = NULL;
  int save;
  bool au_disable_flag = false, is_global;

  AU_DISABLE (save);
  au_disable_flag = true;


  base_class = sm_find_class (base_obj);
  if (base_class == NULL)
    {
      goto fail_return;
    }

  is_global = sm_is_global_class (base_class);
  if (is_global)
    {
      MOBJ smclass;
      const char *class_name;
      MOP catalog_class;
      DB_VALUE name_val;

      if (ws_find (base_class, &smclass) == WS_FIND_MOP_DELETED)
	{
	  goto fail_return;
	}

      catalog_class = sm_find_class (CT_CLASS_NAME);
      if (catalog_class == NULL)
	{
	  goto fail_return;
	}

      class_name = sm_classobj_name ((MOBJ) smclass);
      db_make_string (&name_val, class_name);
      base_in_ctclass =
	obj_find_oids_in_catalog_global (catalog_class, CLASS_ATT_NAME,
					 &name_val);
      pr_clear_value (&name_val);
    }
  else
    {
      classcata = sm_find_class (CT_CLASS_NAME);
      if (classcata == NULL)
	{
	  goto fail_return;
	}
      db_make_varchar (&val, PARTITION_VARCHAR_LEN, base_obj,
		       strlen (base_obj));
      base_in_ctclass = db_find_unique (classcata, CLASS_ATT_NAME, &val);
      pr_clear_value (&val);
    }

  if (base_in_ctclass == NULL)
    {
      goto fail_return;
    }

  ws_set_remote (base_in_ctclass, DB_CLUSTER_NODE_LOCAL);

  partcata = sm_find_class (PARTITION_CATALOG_CLASS);
  if (partcata == NULL)
    {
      goto fail_return;
    }
  otmpl = dbt_create_object_internal (partcata);
  if (otmpl == NULL)
    {
      goto fail_return;
    }
  db_make_object (&val, base_in_ctclass);
  if (dbt_put_internal (otmpl, PARTITION_ATT_CLASSOF, &val) < 0)
    {
      goto fail_return;
    }
  pr_clear_value (&val);

  if (node->node_type == PT_PARTITION)
    {
      db_make_null (&val);
    }
  else
    {
      p = (char *) node->info.parts.name->info.name.original;
      db_make_varchar (&val, PARTITION_VARCHAR_LEN, p, strlen (p));
    }
  if (dbt_put_internal (otmpl, PARTITION_ATT_PNAME, &val) < 0)
    {
      goto fail_return;
    }
  pr_clear_value (&val);

  if (node->node_type == PT_PARTITION)
    {
      db_make_int (&val, node->info.partition.type);
    }
  else
    {
      db_make_int (&val, node->info.parts.type);
    }
  if (dbt_put_internal (otmpl, PARTITION_ATT_PTYPE, &val) < 0)
    {
      goto fail_return;
    }
  pr_clear_value (&val);

  if (node->node_type == PT_PARTITION)
    {
      query = parser_print_tree (parser, node->info.partition.expr);
      if (query == NULL)
	{
	  goto fail_return;
	}

      query_str = (char *) malloc (strlen (query) + strlen (base_obj) +
				   7 /* strlen("SELECT ") */  +
				   6 /* strlen(" FROM ") */  +
				   2 /* [] */  +
				   1 /* terminating null */ );
      if (query_str == NULL)
	{
	  goto fail_return;
	}
      sprintf (query_str, "SELECT %s FROM [%s]", query, base_obj);
      db_make_varchar (&val, PARTITION_VARCHAR_LEN, query_str,
		       strlen (query_str));
    }
  else
    {
      db_make_null (&val);
    }
  if (dbt_put_internal (otmpl, PARTITION_ATT_PEXPR, &val) < 0)
    {
      goto fail_return;
    }
  pr_clear_value (&val);
  if (query_str)
    {
      free_and_init (query_str);
    }

  dbc = set_create_sequence (0);
  if (dbc == NULL)
    {
      goto fail_return;
    }
  if (node->node_type == PT_PARTITION)
    {
      p = (char *) node->info.partition.keycol->info.name.original;
      db_make_varchar (&val, PARTITION_VARCHAR_LEN, p, strlen (p));
      set_add_element (dbc, &val);
      if (node->info.partition.type == PT_PARTITION_HASH)
	{
	  hashsize = pt_value_to_db (parser, node->info.partition.hashsize);
	  set_add_element (dbc, hashsize);
	}
      else
	{
	  set_add_element (dbc, minval);
	}
    }
  else
    {
      if (node->info.parts.type == PT_PARTITION_RANGE)
	{
	  if (minval == NULL)
	    {
	      db_make_null (&val);
	      set_add_element (dbc, &val);
	    }
	  else
	    {
	      set_add_element (dbc, minval);
	    }
	}
      if (node->info.parts.values == NULL)
	{			/* RANGE-MAXVALUE */
	  db_make_null (&val);
	  set_add_element (dbc, &val);
	}
      else
	{
	  for (parts = node->info.parts.values; parts; parts = parts->next)
	    {
	      ptval = pt_value_to_db (parser, parts);
	      if (ptval == NULL)
		{
		  goto fail_return;
		}
	      set_add_element (dbc, ptval);
	    }
	}
    }
  db_make_sequence (&val, dbc);
  if (dbt_put_internal (otmpl, PARTITION_ATT_PVALUES, &val) < 0)
    {
      goto fail_return;
    }

  if (is_global)
    {
      otmpl->discard_on_finish = 0;
    }

  newpart = dbt_finish_object (otmpl);

  if (is_global && newpart != NULL)
    {
      /* dbt_finish_object on every node. */
      DB_OBJLIST *node_list;
      MOP my_node, cur_node;
      int cur_ip;
      OID temp_oid;

      OID_ASSIGN_TEMPOID (&temp_oid);
      if (OID_ISNULL (&temp_oid))
	{
	  if (locator_all_flush () != NO_ERROR)
	    {
	      goto fail_return;
	    }
	  OID_INIT_TEMPID ();
	  OID_ASSIGN_TEMPOID (&temp_oid);
	}

      if (!ws_set_rmt_oid (newpart, &temp_oid, DB_CLUSTER_NODE_ALL))
	{
	  goto fail_return;
	}

      my_node = ccf_get_connected_node ();
      node_list = ccf_get_node_list ();
      if (my_node == NULL || node_list == NULL)
	{
	  goto fail_return;
	}

      for (; node_list != NULL; node_list = node_list->next)
	{
	  cur_node = node_list->op;
	  if (ws_mop_compare (my_node, cur_node) == 0)
	    {
	      continue;
	    }

	  cur_ip = ccf_get_node_ip (cur_node);

	  if (!ws_set_remote (base_in_ctclass, cur_ip)
	      || !ws_set_remote (newpart, cur_ip))
	    {
	      goto fail_return;
	    }

	  /* reuse the newpart. */
	  otmpl->object = newpart;

	  if (obt_update (otmpl, &newpart) != NO_ERROR)
	    {
	      goto fail_return;
	    }
	}

      /* reuse the newpart. */
      otmpl->object = newpart;
      otmpl->discard_on_finish = 1;

      ws_set_remote (base_in_ctclass, DB_CLUSTER_NODE_LOCAL);
      ws_set_remote (newpart, DB_CLUSTER_NODE_LOCAL);
    }

  if (newpart == NULL)
    {
      goto fail_return;
    }


  /* SM_CLASS's partition_of update */
  if (clstmpl)
    {
      clstmpl->partition_of = newpart;
    }
  else
    {
      newclass = sm_find_class (cata_obj);
      if (newclass == NULL)
	{
	  goto fail_return;
	}
      ctmpl = dbt_edit_class (newclass);
      if (ctmpl == NULL)
	{
	  goto fail_return;
	}
      ctmpl->partition_of = newpart;
      if (dbt_finish_class (ctmpl) == NULL)
	{
	  dbt_abort_class (ctmpl);
	  goto fail_return;
	}
    }

  AU_ENABLE (save);
  au_disable_flag = false;
  set_free (dbc);

  if (otmpl != NULL)
    {
      dbt_abort_object (otmpl);
    }

  return NO_ERROR;

fail_return:
  if (otmpl != NULL)
    {
      dbt_abort_object (otmpl);
    }

  if (base_in_ctclass != NULL && WS_IS_REMOTE (base_in_ctclass))
    {
      ws_set_remote (base_in_ctclass, DB_CLUSTER_NODE_LOCAL);
    }
  if (newpart != NULL && WS_IS_REMOTE (newpart))
    {
      ws_set_remote (newpart, DB_CLUSTER_NODE_LOCAL);
    }

  if (au_disable_flag == true)
    {
      AU_ENABLE (save);
    }
  if (dbc != NULL)
    {
      set_free (dbc);
    }
  return er_errid ();
}

/*
 * replace_name_with_value() -
 *   return: PT_NODE pointer
 *   parser(in): Parser context
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 *
 * Note:
 */

static PT_NODE *
replace_name_with_value (PARSER_CONTEXT * parser, PT_NODE * node,
			 void *void_arg, int *continue_walk)
{
  PT_NODE *newval;
  DB_VALUE *ival = (DB_VALUE *) void_arg;
  *continue_walk = PT_CONTINUE_WALK;

  if (node->node_type == PT_NAME)
    {
      newval = pt_dbval_to_value (parser, ival);
      if (newval)
	{
	  newval->next = node->next;
	  node->next = NULL;
	  parser_free_tree (parser, node);
	  node = newval;
	  *continue_walk = PT_STOP_WALK;
	}
    }

  return node;
}

/*
 * adjust_name_with_type() -
 *   return: PT_NODE pointer
 *   parser(in): Parser context
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 *
 * Note:
 */

static PT_NODE *
adjust_name_with_type (PARSER_CONTEXT * parser, PT_NODE * node,
		       void *void_arg, int *continue_walk)
{
  PT_TYPE_ENUM *key_type = (PT_TYPE_ENUM *) void_arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (node->node_type == PT_NAME)
    {
      node->type_enum = (PT_TYPE_ENUM) * key_type;
      node->data_type = pt_domain_to_data_type (parser,
						pt_type_enum_to_db_domain
						(node->type_enum));
    }

  return node;
}

/*
 * evaluate_partition_expr() -
 *   return: DB_VALUE pointer
 *   expr(in): Expression to evaluate
 *   ival(in):
 *
 * Note:
 */
static DB_VALUE *
evaluate_partition_expr (DB_VALUE * expr, DB_VALUE * ival)
{
  PT_NODE **newnode;
  PT_NODE *rstnode, *pcol, *expr_type = NULL;
  PARSER_CONTEXT *expr_parser = NULL;

  if (expr == NULL || ival == NULL)
    return NULL;

  expr_parser = parser_create_parser ();
  if (expr_parser == NULL)
    {
      return NULL;
    }

  newnode = parser_parse_string (expr_parser, DB_GET_STRING (expr));
  if (newnode && *newnode)
    {
      pcol = (*newnode)->info.query.q.select.list;
      if (pcol->node_type == PT_NAME)
	{
	  parser_free_parser (expr_parser);
	  return ival;
	}

      rstnode =
	parser_walk_tree (expr_parser, pcol, replace_name_with_value, ival,
			  NULL, NULL);

      /* expression type check and constant evaluation */
      expr_type = pt_semantic_type (expr_parser, pcol, NULL);
      if (!expr_type)
	{
	  parser_free_parser (expr_parser);
	  return NULL;
	}

      pr_clear_value (ival);
      if (expr_type->node_type == PT_EXPR)
	{
	  pt_evaluate_tree (expr_parser, pcol, ival);
	}
      else
	{
	  db_value_clone (pt_value_to_db (expr_parser, expr_type), ival);
	}

      parser_free_parser (expr_parser);
      return ival;
    }

  parser_free_parser (expr_parser);
  return NULL;
}

/*
 * apply_partition_list_search() -
 *   return: Error code
 *   smclass(in):
 *   sval(in):
 *   retbuf(in):
 *
 * Note:
 */
static int
apply_partition_list_search (SM_CLASS * smclass, DB_VALUE * sval,
			     char *retbuf)
{
  int error = NO_ERROR;
  DB_OBJLIST *objs;
  DB_VALUE pname, pval, element;
  int setsize, i1;
  SM_CLASS *subcls;
  char *pname_str;

  db_make_null (&pname);
  db_make_null (&pval);
  db_make_null (&element);

  for (objs = smclass->users; objs; objs = objs->next)
    {
      error = au_fetch_class (objs->op, &subcls, AU_FETCH_READ, AU_SELECT);
      if (error != NO_ERROR)
	{
	  goto end_return;
	}
      if (!subcls->partition_of)
	{
	  continue;		/* not partitioned */
	}

      error = db_get (subcls->partition_of, PARTITION_ATT_PVALUES, &pval);
      if (error != NO_ERROR)
	{
	  goto end_return;
	}
      error = db_get (subcls->partition_of, PARTITION_ATT_PNAME, &pname);
      if (error != NO_ERROR
	  || DB_IS_NULL (&pname)
	  || (pname_str = DB_GET_STRING (&pname)) == NULL)
	{
	  goto end_return;
	}

      setsize = set_size (pval.data.set);
      if (setsize <= 0)
	{
	  error = -1;
	  goto end_return;
	}

      for (i1 = 0; i1 < setsize; i1++)
	{
	  error = set_get_element (pval.data.set, i1, &element);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }

	  /* null element matching */
	  if ((DB_IS_NULL (sval) && DB_IS_NULL (&element))
	      || db_value_compare (sval, &element) == DB_EQ)
	    {
	      strcpy (retbuf, pname_str);
	      error = NO_ERROR;
	      goto end_return;
	    }
	  pr_clear_value (&element);
	}
      pr_clear_value (&pname);
      pr_clear_value (&pval);
    }

  error = -1;			/* not found */

end_return:
  pr_clear_value (&pname);
  pr_clear_value (&pval);
  pr_clear_value (&element);

  return error;
}

/*
 * apply_partition_range_search() -
 *   return: Error code
 *   smclass(in):
 *   sval(in):
 *   retbuf(in):
 *
 * Note:
 */
static int
apply_partition_range_search (SM_CLASS * smclass, DB_VALUE * sval,
			      char *retbuf)
{
  MOP max = NULL, fit = NULL;
  int error = NO_ERROR;
  DB_OBJLIST *objs;
  DB_VALUE pname, pval, minele, maxele, *fitval = NULL;
  SM_CLASS *subcls;
  char *p = NULL;

  db_make_null (&pname);
  db_make_null (&pval);
  db_make_null (&minele);
  db_make_null (&maxele);

  for (objs = smclass->users; objs; objs = objs->next)
    {
      error = au_fetch_class (objs->op, &subcls, AU_FETCH_READ, AU_SELECT);
      if (error != NO_ERROR)
	{
	  goto clear_end;
	}
      if (!subcls->partition_of)
	{
	  continue;		/* not partitioned */
	}

      error = db_get (subcls->partition_of, PARTITION_ATT_PVALUES, &pval);
      if (error != NO_ERROR)
	{
	  goto clear_end;
	}
      error = set_get_element (pval.data.set, 0, &minele);
      if (error != NO_ERROR)
	{
	  goto clear_end;
	}
      error = set_get_element (pval.data.set, 1, &maxele);
      if (error != NO_ERROR)
	{
	  goto clear_end;
	}

      if (DB_IS_NULL (&maxele))
	{			/* MAXVALUE */
	  max = subcls->partition_of;
	}
      else
	{
	  if (DB_IS_NULL (sval) || db_value_compare (sval, &maxele) == DB_LT)
	    {
	      if (fit == NULL)
		{
		  fit = subcls->partition_of;
		  fitval = db_value_copy (&maxele);
		}
	      else
		{
		  if (db_value_compare (fitval, &maxele) == DB_GT)
		    {
		      db_value_free (fitval);
		      fit = subcls->partition_of;
		      fitval = db_value_copy (&maxele);
		    }
		}
	    }
	}

      pr_clear_value (&pval);
      pr_clear_value (&minele);
      pr_clear_value (&maxele);
    }

  if (fit == NULL)
    {
      if (max == NULL)
	{
	  error = -1;
	  goto clear_end;
	}
      fit = max;
    }

  error = db_get (fit, PARTITION_ATT_PNAME, &pname);
  if (error != NO_ERROR
      || DB_IS_NULL (&pname) || (p = DB_GET_STRING (&pname)) == NULL)
    {
      goto clear_end;
    }
  strcpy (retbuf, p);

  error = NO_ERROR;

clear_end:
  pr_clear_value (&pname);
  pr_clear_value (&pval);
  pr_clear_value (&minele);
  pr_clear_value (&maxele);
  if (fitval)
    {
      db_value_free (fitval);
    }

  return error;
}

/*
 * get_partition_parts() -
 *   return: Error code
 *   class_obj(out):
 *   smclass(in):
 *   ptype(in):
 *   pattr(in):
 *   sval(in):
 *
 * Note:
 */

static int
get_partition_parts (MOP * class_obj, SM_CLASS * smclass, int ptype,
		     DB_VALUE * pattr, DB_VALUE * sval)
{
  DB_VALUE ele;
  char pname[PARTITION_VARCHAR_LEN + 1];
  char pclass[PARTITION_VARCHAR_LEN + 1];
  int error = NO_ERROR;

  if (smclass == NULL ||
      ptype < PT_PARTITION_HASH || ptype > PT_PARTITION_LIST ||
      pattr == NULL || sval == NULL)
    {
      *class_obj = NULL;
      return error;
    }

  switch (ptype)
    {
    case PT_PARTITION_HASH:
      error = set_get_element (pattr->data.set, 1, &ele);
      if (error != NO_ERROR)
	{
	  *class_obj = NULL;
	  return error;
	}
      if (ele.data.i <= 0)
	{
	  pr_clear_value (&ele);
	  *class_obj = NULL;
	  return error;
	}

      sprintf (pname, "p%d", mht_get_hash_number (ele.data.i, sval));
      pr_clear_value (&ele);
      break;
    case PT_PARTITION_LIST:
      error = apply_partition_list_search (smclass, sval, pname);
      if (error != NO_ERROR)
	{
	  *class_obj = NULL;
	  return error;
	}
      break;
    case PT_PARTITION_RANGE:
      error = apply_partition_range_search (smclass, sval, pname);
      if (error != NO_ERROR)
	{
	  *class_obj = NULL;
	  return error;
	}
      break;
    }

  sprintf (pclass, "%s" PARTITIONED_SUB_CLASS_TAG "%s",
	   smclass->header.name, pname);

  *class_obj = sm_find_class (pclass);
  return NO_ERROR;
}

/*
 * do_insert_partition_cache() -
 *   return: Error code
 *   pic(in):
 *   attr(in):
 *   desc(in):
 *   val(in):
 *
 * Note:
 */
int
do_insert_partition_cache (PARTITION_INSERT_CACHE ** pic, PT_NODE * attr,
			   DB_ATTDESC * desc, DB_VALUE * val)
{
  PARTITION_INSERT_CACHE *picnext;

  if (*pic == NULL)
    {
      *pic =
	(PARTITION_INSERT_CACHE *) malloc (sizeof (PARTITION_INSERT_CACHE));
      if (*pic == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED,
		  0);
	  return er_errid ();
	}
      picnext = *pic;
    }
  else
    {
      for (picnext = *pic; picnext && picnext->next; picnext = picnext->next)
	;

      picnext->next =
	(PARTITION_INSERT_CACHE *) malloc (sizeof (PARTITION_INSERT_CACHE));
      if (picnext->next == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED,
		  0);
	  return er_errid ();
	}
      picnext = picnext->next;
    }

  picnext->next = NULL;
  picnext->attr = attr;
  picnext->desc = desc;
  picnext->val = pr_copy_value (val);

  return NO_ERROR;
}

/*
 * do_insert_partition_cache() -
 *   return: Error code
 *   pic(in):
 *   attr(in):
 *   desc(in):
 *   val(in):
 *
 * Note:
 */
void
do_clear_partition_cache (PARTITION_INSERT_CACHE * pic)
{
  PARTITION_INSERT_CACHE *picnext, *tmp;

  picnext = pic;
  while (picnext)
    {
      if (picnext->val)
	{
	  pr_free_value (picnext->val);
	}
      tmp = picnext;
      picnext = picnext->next;
      if (tmp)
	{
	  free_and_init (tmp);
	}
    }
}

/*
 * do_init_partition_select() -
 *   return: Error code
 *   classobj(in):
 *   psi(in):
 *
 * Note:
 */
int
do_init_partition_select (MOP classobj, PARTITION_SELECT_INFO ** psi)
{
  DB_VALUE ptype, pname, pexpr, pattr;
  int error = NO_ERROR;
  SM_CLASS *smclass;
  int au_save;
  bool au_disable_flag = false;

  if (classobj == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED, 0);
      return er_errid ();
    }

  db_make_null (&ptype);
  db_make_null (&pname);
  db_make_null (&pexpr);
  db_make_null (&pattr);

  AU_DISABLE (au_save);
  au_disable_flag = true;

  error = au_fetch_class (classobj, &smclass, AU_FETCH_READ, AU_SELECT);
  if (error != NO_ERROR)
    {
      goto end_partition;
    }

  if (smclass->partition_of == NULL)
    {
      error = ER_PARTITION_WORK_FAILED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto end_partition;
    }

  error = db_get (smclass->partition_of, PARTITION_ATT_PNAME, &pname);
  if (error != NO_ERROR)
    {
      goto end_partition;
    }

  /* adjust only partition parent class */
  if (DB_IS_NULL (&pname))
    {
      error = db_get (smclass->partition_of, PARTITION_ATT_PTYPE, &ptype);
      if (error != NO_ERROR
	  || ((error = db_get (smclass->partition_of, PARTITION_ATT_PEXPR,
			       &pexpr)) != NO_ERROR)
	  || ((error = db_get (smclass->partition_of, PARTITION_ATT_PVALUES,
			       &pattr)) != NO_ERROR))
	{
	  goto end_partition;
	}

      *psi =
	(PARTITION_SELECT_INFO *) malloc (sizeof (PARTITION_SELECT_INFO));
      if (*psi == NULL)
	{
	  error = ER_PARTITION_WORK_FAILED;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	}
      else
	{
	  error = NO_ERROR;
	  (*psi)->ptype = pr_copy_value (&ptype);
	  (*psi)->pexpr = pr_copy_value (&pexpr);
	  (*psi)->pattr = pr_copy_value (&pattr);
	  (*psi)->smclass = smclass;
	}
    }
  else
    {
      error = ER_PARTITION_WORK_FAILED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }

  AU_ENABLE (au_save);
  au_disable_flag = false;

end_partition:
  pr_clear_value (&ptype);
  pr_clear_value (&pname);
  pr_clear_value (&pexpr);
  pr_clear_value (&pattr);

  if (au_disable_flag == true)
    AU_ENABLE (au_save);

  return error;
}

/*
 * do_clear_partition_select() -
 *   return: Error code
 *   psi(in):
 *
 * Note:
 */
void
do_clear_partition_select (PARTITION_SELECT_INFO * psi)
{
  if (psi == NULL)
    {
      return;
    }

  pr_free_value (psi->ptype);
  pr_free_value (psi->pattr);
  pr_free_value (psi->pexpr);

  free_and_init (psi);
}

/*
 * do_select_partition() -
 *   return: Error code
 *   psi(in):
 *   val(in):
 *   retobj(in):
 *
 * Note:
 */
int
do_select_partition (PARTITION_SELECT_INFO * psi, DB_VALUE * val,
		     MOP * retobj)
{
  int error = NO_ERROR;
  DB_VALUE retval;
  int au_save;

  /* expr eval */
  db_make_null (&retval);
  error = db_value_clone (val, &retval);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (evaluate_partition_expr (psi->pexpr, &retval) == NULL)
    {
      pr_clear_value (&retval);
      error = ER_PARTITION_WORK_FAILED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }

  AU_DISABLE (au_save);

  /* _db_partition object search */
  error = get_partition_parts (retobj, psi->smclass, psi->ptype->data.i,
			       psi->pattr, &retval);
  if (*retobj == NULL)
    {
      pr_clear_value (&retval);
      AU_ENABLE (au_save);
      error = ER_PARTITION_NOT_EXIST;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }

  pr_clear_value (&retval);

  AU_ENABLE (au_save);
  return NO_ERROR;
}

/*
 * find_partition_attr() -
 *   return: PT_NODE pointer
 *   parser(in): Parser context
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 * Note:
 */
static PT_NODE *
find_partition_attr (PARSER_CONTEXT * parser, PT_NODE * node,
		     void *void_arg, int *continue_walk)
{
  PRUNING_INFO *ppi = (PRUNING_INFO *) void_arg;
  *continue_walk = PT_CONTINUE_WALK;

  if (node->node_type == PT_NAME)
    {
      if (node->info.name.spec_id == ppi->spec)
	{
	  const char *mbs2 = DB_GET_STRING (ppi->attr);
	  if (mbs2 == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_PARTITION_WORK_FAILED, 0);
	      return NULL;
	    }

	  if (intl_mbs_casecmp (node->info.name.original, mbs2) == 0)
	    {
	      ppi->wrkmap |= PATTR_KEY;
	    }
	  else
	    {
	      ppi->wrkmap |= PATTR_COLUMN;
	    }
	}
      else
	{
	  ppi->wrkmap |= PATTR_NAME;
	}
    }
  else if (node->node_type == PT_VALUE)
    {
      ppi->wrkmap |= PATTR_VALUE;
    }

  return node;
}

/*
 * find_partition_attr() -
 *   return: 1 if it is same, else 0
 *   parser(in): Parser context
 *   p(in):
 *   q(in):
 *
 * Note:
 */
static int
check_same_expr (PARSER_CONTEXT * parser, PT_NODE * p, PT_NODE * q)
{
  DB_VALUE *vp, *vq;

  if (!p || !q || !parser)
    {
      return 0;
    }

  if (p->node_type != q->node_type)
    {
      return 0;
    }

  switch (p->node_type)
    {
    case PT_EXPR:
      if (p->info.expr.op != q->info.expr.op)
	{
	  return 0;
	}

      if (p->info.expr.arg1)
	{
	  if (check_same_expr
	      (parser, p->info.expr.arg1, q->info.expr.arg1) == 0)
	    {
	      return 0;
	    }
	}
      if (p->info.expr.arg2)
	{
	  if (check_same_expr
	      (parser, p->info.expr.arg2, q->info.expr.arg2) == 0)
	    {
	      return 0;
	    }
	}
      break;

    case PT_VALUE:
      vp = pt_value_to_db (parser, p);
      vq = pt_value_to_db (parser, q);
      if (!vp || !vq)
	{
	  return 0;
	}
      if (!tp_value_equal (vp, vq, 1))
	{
	  return 0;
	}
      break;

    case PT_NAME:
      if (intl_mbs_casecmp (p->info.name.original, q->info.name.original) !=
	  0)
	{
	  return 0;
	}
      break;

    default:
      break;
    }

  return 1;			/* same expr */
}

/*
 * evaluate_partition_range() -
 *   return:
 *   parser(in): Parser context
 *   expr(in):
 *
 * Note:
 */
static int
evaluate_partition_range (PARSER_CONTEXT * parser, PT_NODE * expr)
{
  int cmprst = 0, optype;
  PT_NODE *elem, *llim, *ulim;
  DB_VALUE *orgval, *llimval, *ulimval;
  DB_VALUE_COMPARE_RESULT cmp1 = DB_UNK;
  DB_VALUE_COMPARE_RESULT cmp2 = DB_UNK;

  orgval = llimval = ulimval = NULL;

  if (!expr || expr->node_type != PT_EXPR
      || expr->info.expr.op != PT_RANGE
      || expr->info.expr.arg1->node_type != PT_VALUE
      || expr->info.expr.arg2->node_type != PT_EXPR)
    {
      return 0;
    }

  for (elem = expr->info.expr.arg2; elem; elem = elem->or_next)
    {
      optype = elem->info.expr.op;

      switch (optype)
	{
	case PT_BETWEEN_EQ_NA:
	  llim = elem->info.expr.arg1;
	  ulim = llim;
	  break;

	case PT_BETWEEN_INF_LE:
	case PT_BETWEEN_INF_LT:
	  llim = NULL;
	  ulim = elem->info.expr.arg1;
	  break;

	case PT_BETWEEN_GE_INF:
	case PT_BETWEEN_GT_INF:
	  llim = elem->info.expr.arg1;
	  ulim = NULL;
	  break;

	default:
	  llim = elem->info.expr.arg1;
	  ulim = elem->info.expr.arg2;
	  break;
	}

      if (llim != NULL
	  && (llim->node_type != PT_VALUE
	      || (llimval = pt_value_to_db (parser, llim)) == NULL))
	{
	  return 0;
	}

      if (ulim != NULL
	  && (ulim->node_type != PT_VALUE
	      || (ulimval = pt_value_to_db (parser, ulim)) == NULL))
	{
	  return 0;
	}

      orgval = pt_value_to_db (parser, expr->info.expr.arg1);
      if (orgval == NULL)
	{
	  return 0;
	}

      if (llim != NULL)
	{
	  cmp1 = (DB_VALUE_COMPARE_RESULT) db_value_compare (llimval, orgval);
	}
      if (ulim != NULL)
	{
	  cmp2 = (DB_VALUE_COMPARE_RESULT) db_value_compare (orgval, ulimval);
	}

      switch (elem->info.expr.op)
	{
	case PT_BETWEEN_EQ_NA:
	  if (cmp1 == DB_EQ)
	    {
	      cmprst = 1;
	    }
	  break;
	case PT_BETWEEN_INF_LE:
	  if (cmp2 == DB_EQ || cmp2 == DB_LT)
	    {
	      cmprst = 1;
	    }
	  break;
	case PT_BETWEEN_INF_LT:
	  if (cmp2 == DB_LT)
	    {
	      cmprst = 1;
	    }
	  break;
	case PT_BETWEEN_GE_INF:
	  if (cmp1 == DB_EQ || cmp1 == DB_LT)
	    {
	      cmprst = 1;
	    }
	  break;
	case PT_BETWEEN_GT_INF:
	  if (cmp1 == DB_LT)
	    {
	      cmprst = 1;
	    }
	  break;
	default:
	  if ((optype == PT_BETWEEN_GE_LE || optype == PT_BETWEEN_GE_LT)
	      && cmp1 != DB_EQ && cmp1 != DB_LT)
	    {
	      break;
	    }

	  if ((optype == PT_BETWEEN_GE_LE || optype == PT_BETWEEN_GT_LE)
	      && cmp2 != DB_EQ && cmp2 != DB_LT)
	    {
	      break;
	    }

	  if ((optype == PT_BETWEEN_GT_LE || optype == PT_BETWEEN_GT_LT)
	      && cmp1 != DB_LT)
	    {
	      break;
	    }

	  if ((optype == PT_BETWEEN_GE_LT || optype == PT_BETWEEN_GT_LT)
	      && cmp2 != DB_LT)
	    {
	      break;
	    }

	  cmprst = 1;
	  break;
	}

      if (cmprst)
	break;			/* true find */
    }

  return cmprst;
}

/*
 * convert_expr_to_constant() -
 *   return: PT_NODE pointer
 *   parser(in): Parser context
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in)
 *
 * Note:
 */
static PT_NODE *
convert_expr_to_constant (PARSER_CONTEXT * parser, PT_NODE * node,
			  void *void_arg, int *continue_walk)
{
  DB_VALUE retval, *host_var, *castval;
  PT_NODE *newval;
  bool *support_op = (bool *) void_arg;
  *continue_walk = PT_CONTINUE_WALK;

  if (node->node_type == PT_EXPR)
    {
      switch (node->info.expr.op)
	{
	case PT_SYS_DATE:
	  db_sys_date (&retval);
	  break;
	case PT_SYS_TIME:
	  db_sys_time (&retval);
	  break;
	case PT_SYS_TIMESTAMP:
	  db_sys_timestamp (&retval);
	  break;
	case PT_SYS_DATETIME:
	  db_sys_datetime (&retval);
	  break;
	case PT_PLUS:
	case PT_MINUS:
	case PT_MODULUS:
	case PT_TIMES:
	case PT_DIVIDE:
	case PT_UNARY_MINUS:
	case PT_POSITION:
	case PT_SUBSTRING:
	case PT_OCTET_LENGTH:
	case PT_BIT_LENGTH:
	case PT_CHAR_LENGTH:
	case PT_LOWER:
	case PT_UPPER:
	case PT_TRIM:
	case PT_LTRIM:
	case PT_RTRIM:
	case PT_LPAD:
	case PT_RPAD:
	case PT_REPLACE:
	case PT_TRANSLATE:
	case PT_ADD_MONTHS:
	case PT_LAST_DAY:
	case PT_MONTHS_BETWEEN:
	case PT_TO_DATE:
	case PT_TO_NUMBER:
	case PT_TO_TIME:
	case PT_TO_TIMESTAMP:
	case PT_TO_DATETIME:
	case PT_EXTRACT:
	case PT_TO_CHAR:
	case PT_STRCAT:
	case PT_FLOOR:
	case PT_CEIL:
	case PT_POWER:
	case PT_ROUND:
	case PT_ABS:
	case PT_LOG:
	case PT_EXP:
	case PT_SQRT:
	case PT_ACOS:
	case PT_ASIN:
	case PT_ATAN:
	case PT_ATAN2:
	case PT_SIN:
	case PT_COS:
	case PT_TAN:
	case PT_COT:
	case PT_DEGREES:
	case PT_RADIANS:
	case PT_LN:
	case PT_LOG2:
	case PT_LOG10:
	case PT_FORMAT:
	case PT_DATE_FORMAT:
	case PT_STR_TO_DATE:
	case PT_TRUNC:
	  /* PT_RANGE - sub type */
	case PT_BETWEEN_GE_LE:
	case PT_BETWEEN_GE_LT:
	case PT_BETWEEN_GT_LE:
	case PT_BETWEEN_GT_LT:
	case PT_BETWEEN_EQ_NA:
	case PT_BETWEEN_INF_LE:
	case PT_BETWEEN_INF_LT:
	case PT_BETWEEN_GE_INF:
	case PT_BETWEEN_GT_INF:
	case PT_BETWEEN_AND:
	case PT_INCR:
	case PT_DECR:
	case PT_PRIOR:
	case PT_CONNECT_BY_ROOT:
	case PT_QPRIOR:
	case PT_BIT_NOT:
	case PT_BIT_AND:
	case PT_BIT_OR:
	case PT_BIT_XOR:
	case PT_BITSHIFT_LEFT:
	case PT_BITSHIFT_RIGHT:
	case PT_DIV:
	case PT_MOD:
	case PT_CONCAT:
	case PT_CONCAT_WS:
	case PT_FIELD:
	case PT_LEFT:
	case PT_RIGHT:
	case PT_LOCATE:
	case PT_MID:
	case PT_STRCMP:
	case PT_REVERSE:
	case PT_BIT_COUNT:
	case PT_DATEF:
	case PT_DATEDIFF:
	case PT_SCHEMA:
	case PT_DATABASE:
	case PT_TIME_FORMAT:
	case PT_TIMESTAMP:
	case PT_UNIX_TIMESTAMP:
	  return node;

	case PT_CAST:
	  castval = pt_value_to_db (parser, node->info.expr.arg1);
	  if (castval != NULL)
	    {
	      if (tp_value_cast (castval, &retval,
				 pt_type_enum_to_db_domain (node->info.expr.
							    cast_type->
							    type_enum),
				 false) == NO_ERROR)
		{
		  break;
		}
	    }

	default:
	  *support_op = false;
	  return node;
	}

      newval = pt_dbval_to_value (parser, &retval);
      if (newval)
	{
	  newval->next = node->next;
	  node->next = NULL;
	  parser_free_tree (parser, node);
	  node = newval;
	}
    }
  else if (node->node_type == PT_HOST_VAR)
    {
      host_var = pt_host_var_db_value (parser, node);
      if (host_var)
	{
	  parser_free_tree (parser, node);
	  node = pt_dbval_to_value (parser, host_var);
	}
    }
  return node;
}

/*
 * get_pruned_partition_spec() -
 *   return: PT_NODE pointer
 *   ppi(in):
 *   subobj(in):
 *
 * Note:
 */
static PT_NODE *
get_pruned_partition_spec (PRUNING_INFO * ppi, MOP subobj)
{
  PT_NODE *subspec;

  for (subspec = ppi->ppart; subspec; subspec = subspec->next)
    {
      if (ws_mop_compare (subspec->info.name.db_object, subobj) == 0)
	{
	  return subspec;
	}
    }
  return NULL;
}

/*
 * add_pruned_partition_part() -
 *   return: PT_NODE pointer
 *   ppi(in):
 *   subobj(in):
 *
 * Note:
 */
static void
add_pruned_partition_part (PT_NODE * subspec, PRUNING_INFO * ppi, MOP subcls,
			   char *cname)
{
  if (subspec == NULL)
    {				/* new node */
      subspec = pt_name (ppi->parser, cname);
      subspec->info.name.db_object = subcls;
      subspec->info.name.location = 1;
      subspec->next = NULL;
      if (ppi->ppart == NULL)
	{
	  ppi->ppart = subspec;
	}
      else
	{
	  parser_append_node (subspec, ppi->ppart);
	}
    }
  else
    {
      subspec->info.name.location++;
    }
}

/*
 * adjust_pruned_partition() -
 *   return: Number of parts
 *   spec(in):
 *   ppi(in):
 *
 * Note:
 */
static int
adjust_pruned_partition (PT_NODE * spec, PRUNING_INFO * ppi)
{
  PT_NODE *subspec, *pre, *tmp;
  int partcnt = 0;

  pre = NULL;
  for (subspec = ppi->ppart; subspec;)
    {
      if (subspec->info.name.location == ppi->expr_cnt)
	{
	  partcnt++;

	  if (spec)
	    {
	      subspec->line_number = spec->line_number;
	      subspec->column_number = spec->column_number;
	      subspec->info.name.spec_id = spec->info.spec.id;
	      subspec->info.name.meta_class = spec->info.spec.meta_class;
	      subspec->info.name.partition_of = NULL;
	      subspec->info.name.location = 0;
	    }
	  pre = subspec;
	  subspec = subspec->next;
	}
      else
	{
	  tmp = subspec->next;
	  if (pre != NULL)
	    {
	      pre->next = tmp;
	    }
	  else
	    {
	      ppi->ppart = tmp;
	    }
	  subspec->next = NULL;
	  parser_free_tree (ppi->parser, subspec);
	  subspec = tmp;
	}
    }

  return partcnt;
}

/*
 * increase_value() -
 *   return: 1 if it is increased, else 0
 *   val(in):
 *
 * Note:
 */
static int
increase_value (DB_VALUE * val)
{
  int month, day, year;

  if (!val || DB_IS_NULL (val))
    {
      return 0;
    }

  switch (DB_VALUE_TYPE (val))
    {
    case DB_TYPE_INTEGER:
      val->data.i++;
      break;
    case DB_TYPE_BIGINT:
      val->data.bigint++;
      break;
    case DB_TYPE_SHORT:
      val->data.i++;
      break;
    case DB_TYPE_TIME:
      val->data.time++;
      break;
    case DB_TYPE_UTIME:
      val->data.utime++;
      break;
    case DB_TYPE_DATETIME:
      if (val->data.datetime.time == MILLISECONDS_OF_ONE_DAY - 1)
	{
	  val->data.datetime.date++;
	  val->data.datetime.time = 0;
	}
      else
	{
	  val->data.datetime.time++;
	}
      break;
    case DB_TYPE_DATE:
      val->data.date++;
      db_date_decode (&val->data.date, &month, &day, &year);
      db_make_date (val, month, day, year);
      break;
    default:
      return 0;
    }

  return 1;
}

/*
 * decrease_value() -
 *   return: 1 if it is increased, else 0
 *   val(in):
 *
 * Note:
 */
static int
decrease_value (DB_VALUE * val)
{
  int month, day, year;

  if (!val || DB_IS_NULL (val))
    {
      return 0;
    }

  switch (DB_VALUE_TYPE (val))
    {
    case DB_TYPE_INTEGER:
      val->data.i--;
      break;
    case DB_TYPE_BIGINT:
      val->data.bigint--;
      break;
    case DB_TYPE_SHORT:
      val->data.i--;
      break;
    case DB_TYPE_TIME:
      val->data.time--;
      break;
    case DB_TYPE_UTIME:
      val->data.utime--;
      break;
    case DB_TYPE_DATETIME:
      if (val->data.datetime.time == 0)
	{
	  val->data.datetime.date--;
	  val->data.datetime.time = MILLISECONDS_OF_ONE_DAY - 1;
	}
      else
	{
	  val->data.datetime.time--;
	}
      break;
    case DB_TYPE_DATE:
      val->data.date--;
      db_date_decode (&val->data.date, &month, &day, &year);
      db_make_date (val, month, day, year);
      break;
    default:
      return 0;
    }

  return 1;
}

/*
 * check_hash_range() -
 *   return:
 *   ppi(in):
 *   partmap(in):
 *   op(in):
 *   from_expr(in):
 *   to_expr(in):
 *   setval(in):
 *
 * Note:
 */
static int
check_hash_range (PRUNING_INFO * ppi, char *partmap, PT_OP_TYPE op,
		  PT_NODE * from_expr, PT_NODE * to_expr, int setval)
{
  int addcnt = 0, ret, hashnum;
  DB_VALUE *fromval, *toval;

  if (!from_expr
      || (from_expr->type_enum != PT_TYPE_INTEGER
	  && from_expr->type_enum != PT_TYPE_BIGINT
	  && from_expr->type_enum != PT_TYPE_SMALLINT
	  && !PT_IS_DATE_TIME_TYPE (from_expr->type_enum)))
    {
      return -1;
    }

  if (!to_expr
      || (to_expr->type_enum != PT_TYPE_INTEGER
	  && to_expr->type_enum != PT_TYPE_BIGINT
	  && to_expr->type_enum != PT_TYPE_SMALLINT
	  && !PT_IS_DATE_TIME_TYPE (to_expr->type_enum)))
    {
      return -1;
    }

  /* GE_LT adjust */
  fromval = pt_value_to_db (ppi->parser, from_expr);
  if (fromval == NULL)
    {
      return -1;
    }
  if (op == PT_BETWEEN_GT_LE || op == PT_BETWEEN_GT_LT)
    {
      if (!increase_value (fromval))
	{
	  return -1;
	}
    }

  toval = pt_value_to_db (ppi->parser, to_expr);
  if (toval == NULL)
    {
      return -1;
    }
  if (op == PT_BETWEEN_GE_LE || op == PT_BETWEEN_GT_LE)
    {
      if (!increase_value (toval))
	{
	  return -1;
	}
    }

  while (1)
    {
      ret = db_value_compare (fromval, toval);
      if (ret == DB_EQ || ret == DB_GT)
	{
	  break;
	}
      hashnum = mht_get_hash_number (ppi->size, fromval);
      if (partmap[hashnum] != setval)
	{
	  partmap[hashnum] = setval;
	  addcnt++;
	  if (addcnt >= ppi->size)
	    {
	      return -1;	/* all partitions */
	    }
	}
      if (!increase_value (fromval))
	{
	  return -1;
	}
    }

  return addcnt;
}

/*
 * select_hash_partition() -
 *   return:
 *   ppi(in):
 *   expr(in):
 *
 * Note:
 */
static int
select_hash_partition (PRUNING_INFO * ppi, PT_NODE * expr)
{
  DB_OBJLIST *objs;
  PT_NODE *elem, *pruned;
  int rst = 0, setsize, i1, hashnum, sval, target_cnt;
  int ret;
  char *partmap;
  DB_VALUE *hval, ele;
  SM_CLASS *subcls;
  DB_VALUE temp;
  db_make_null (&temp);

  pt_evaluate_tree (ppi->parser, expr->info.expr.arg2, &temp);
  if (pt_has_error (ppi->parser))
    {
      pt_report_to_ersys (ppi->parser, PT_SEMANTIC);
      return 0;
    }
  hval = &temp;

  partmap = (char *) malloc (ppi->size);
  if (partmap == NULL)
    {
      db_value_clear (&temp);
      return 0;
    }

  memset (partmap, 0, ppi->size);

  switch (expr->info.expr.op)
    {
    case PT_RANGE:
      for (rst = 0, elem = expr->info.expr.arg2; elem; elem = elem->or_next)
	{
	  if (elem->info.expr.op == PT_BETWEEN_EQ_NA)
	    {
	      hval = pt_value_to_db (ppi->parser, elem->info.expr.arg1);
	      if (hval == NULL)
		{
		  continue;
		}

	      hashnum = mht_get_hash_number (ppi->size, hval);
	      if (partmap[hashnum] != 1)
		{
		  partmap[hashnum] = 1;
		  rst++;
		}
	    }
	  else
	    {
	      switch (elem->info.expr.op)
		{
		case PT_BETWEEN_INF_LE:
		case PT_BETWEEN_INF_LT:
		case PT_BETWEEN_GE_INF:
		case PT_BETWEEN_GT_INF:
		  ret = -1;
		default:
		  ret = check_hash_range (ppi, partmap, elem->info.expr.op,
					  elem->info.expr.arg1,
					  elem->info.expr.arg2, 1);
		  break;
		}

	      if (ret == -1)
		{
		  rst = -1;	/* range -> no pruning */
		  break;
		}
	      else
		{
		  rst += ret;
		}
	    }
	}
      break;

    case PT_BETWEEN:
      rst = check_hash_range (ppi, partmap, PT_BETWEEN_GE_LE,
			      expr->info.expr.arg2->info.expr.arg1,
			      expr->info.expr.arg2->info.expr.arg2, 1);
      break;

    case PT_GE:
    case PT_GT:
    case PT_LT:
    case PT_LE:
      rst = -1;			/* range -> no pruning */
      break;

    case PT_IS_IN:
      setsize = set_size (hval->data.set);
      if (setsize <= 0)
	{
	  rst = -1;
	  break;
	}

      for (i1 = 0, rst = 0; i1 < setsize; i1++)
	{
	  if (set_get_element (hval->data.set, i1, &ele) != NO_ERROR)
	    {
	      rst = -1;
	      break;
	    }

	  hashnum = mht_get_hash_number (ppi->size, &ele);
	  if (partmap[hashnum] != 0)
	    {
	      partmap[hashnum] = 1;
	      rst++;
	    }

	  pr_clear_value (&ele);
	}
      break;

    case PT_IS_NULL:
      partmap[0] = 1;		/* first partition */
      rst = 1;
      break;
    case PT_EQ:
      hashnum = mht_get_hash_number (ppi->size, hval);
      partmap[hashnum] = 1;
      rst = 1;
      break;

    default:
      break;
    }

  if (rst <= 0)
    {
      free_and_init (partmap);
      db_value_clear (&temp);
      return 0;
    }

  target_cnt = ppi->expr_cnt + 1;
  for (hashnum = 0, sval = 0, objs = ppi->smclass->users;
       objs && sval < rst; objs = objs->next, hashnum++)
    {
      if (!partmap[hashnum])
	{
	  continue;
	}

      sval++;

      pruned = get_pruned_partition_spec (ppi, objs->op);
      if (ppi->expr_cnt == 0)
	{
	  if (pruned != NULL)
	    {
	      continue;
	    }
	}
      else
	{
	  if (pruned == NULL)
	    {
	      continue;
	    }
	  if (pruned->info.name.location == target_cnt)
	    {
	      continue;
	    }
	}

      if (pruned)
	{
	  add_pruned_partition_part (pruned, ppi, objs->op, NULL);
	}
      else
	{
	  if (au_fetch_class (objs->op, &subcls, AU_FETCH_READ, AU_SELECT) !=
	      NO_ERROR)
	    {
	      continue;
	    }
	  if (!subcls->partition_of)
	    {
	      continue;
	    }
	  add_pruned_partition_part (pruned, ppi, objs->op,
				     (char *) subcls->header.name);
	}
    }

  free_and_init (partmap);
  db_value_clear (&temp);
  return 1;
}

/*
 * select_range_partition() -
 *   return:
 *   ppi(in):
 *   expr(in):
 *
 * Note:
 */
static int
select_range_partition (PRUNING_INFO * ppi, PT_NODE * expr)
{
  DB_OBJLIST *objs;
  DB_VALUE pval, minele, maxele;
  SM_CLASS *subcls;
  PT_NODE *elem, *pruned;
  DB_VALUE *minval, *maxval, *lval = NULL, *uval, ele;
  PT_OP_TYPE minop, maxop, lop, uop;
  int rst, optype, setsize, i1, target_cnt;
  DB_TYPE range_type;

  target_cnt = ppi->expr_cnt + 1;
  if (expr->info.expr.arg2 && expr->info.expr.arg2->node_type == PT_VALUE)
    {
      lval = pt_value_to_db (ppi->parser, expr->info.expr.arg2);
      if (lval == NULL)
	{
	  return 0;		/* expr skip */
	}
    }

  db_make_null (&maxele);
  db_make_null (&minele);

  for (objs = ppi->smclass->users; objs; objs = objs->next)
    {

      pruned = get_pruned_partition_spec (ppi, objs->op);
      if (ppi->expr_cnt == 0)
	{
	  if (pruned != NULL)
	    {
	      continue;
	    }
	}
      else
	{
	  if (pruned == NULL)
	    {
	      continue;
	    }
	  if (pruned->info.name.location == target_cnt)
	    {
	      continue;
	    }
	}

      if (au_fetch_class (objs->op, &subcls, AU_FETCH_READ, AU_SELECT) !=
	  NO_ERROR || !subcls->partition_of
	  || db_get (subcls->partition_of, PARTITION_ATT_PVALUES,
		     &pval) != NO_ERROR)
	{
	  continue;
	}

      pr_clear_value (&maxele);
      pr_clear_value (&minele);

      if (set_get_element (pval.data.set, 0, &minele) != NO_ERROR ||
	  set_get_element (pval.data.set, 1, &maxele) != NO_ERROR)
	{
	  continue;
	}

      pr_clear_value (&pval);

      /* min/max conversion for is_ranges_meetable */
      if (DB_IS_NULL (&minele))
	{
	  minval = NULL;
	  minop = PT_GT_INF;
	}
      else
	{
	  minval = &minele;
	  minop = PT_GE;
	}

      if (DB_IS_NULL (&maxele))
	{
	  maxval = NULL;
	  maxop = PT_LT_INF;
	}
      else
	{
	  maxval = &maxele;
	  maxop = (decrease_value (maxval)) ? PT_LE : PT_LT;
	}

      rst = 0;

      /* expr's op conversion for is_ranges_meetable */
      switch (expr->info.expr.op)
	{
	case PT_RANGE:
	  for (elem = expr->info.expr.arg2; elem; elem = elem->or_next)
	    {

	      optype = elem->info.expr.op;
	      if (optype == PT_BETWEEN_EQ_NA)
		{
		  lval = pt_value_to_db (ppi->parser, elem->info.expr.arg1);
		  if (lval == NULL)
		    {
		      continue;
		    }
		  if (is_in_range (minval, minop, maxval, maxop, lval))
		    {
		      break;
		    }
		  else
		    {
		      continue;
		    }
		}

	      if (pt_between_to_comp_op ((PT_OP_TYPE) optype, &lop, &uop) !=
		  0)
		{
		  continue;
		}

	      switch (optype)
		{
		case PT_BETWEEN_INF_LE:
		case PT_BETWEEN_INF_LT:
		  lval = NULL;
		  uval = pt_value_to_db (ppi->parser, elem->info.expr.arg1);
		  if (uval == NULL)
		    {
		      continue;
		    }
		  break;

		case PT_BETWEEN_GE_INF:
		case PT_BETWEEN_GT_INF:
		  lval = pt_value_to_db (ppi->parser, elem->info.expr.arg1);
		  if (lval == NULL)
		    {
		      continue;
		    }
		  uval = NULL;
		  break;

		default:
		  lval = pt_value_to_db (ppi->parser, elem->info.expr.arg1);
		  if (lval == NULL)
		    {
		      continue;
		    }
		  uval = pt_value_to_db (ppi->parser, elem->info.expr.arg2);
		  if (uval == NULL)
		    {
		      continue;
		    }
		  break;
		}		/* switch (op_type) */

	      if (is_ranges_meetable (minval, minop, maxval, maxop,
				      lval, lop, uval, uop))
		{
		  break;
		}
	    }
	  rst = (elem == NULL) ? 0 : 1;
	  break;

	case PT_BETWEEN:
	case PT_NOT_BETWEEN:
	  if (expr->info.expr.arg2 == NULL)
	    {
	      return 0;
	    }

	  lval = pt_value_to_db (ppi->parser,	/* BETWEEN .. AND */
				 expr->info.expr.arg2->info.expr.arg1);
	  if (lval == NULL)
	    {
	      return 0;
	    }
	  uval = pt_value_to_db (ppi->parser,
				 expr->info.expr.arg2->info.expr.arg2);
	  if (uval == NULL)
	    {
	      return 0;
	    }
	  if (expr->info.expr.op == PT_BETWEEN)
	    {
	      rst = is_ranges_meetable (minval, minop, maxval, maxop,
					lval, PT_GE, uval, PT_LE);
	    }
	  else
	    {
	      rst = 0;
	      if (is_ranges_meetable (minval, minop, maxval, maxop,
				      NULL, PT_GT_INF, lval, PT_LT) ||
		  is_ranges_meetable (minval, minop, maxval, maxop,
				      uval, PT_GT, NULL, PT_LT_INF))
		{
		  rst = 1;
		}
	    }
	  break;

	case PT_GE:
	case PT_GT:
	  rst = is_ranges_meetable (minval, minop, maxval, maxop,
				    lval, expr->info.expr.op, NULL,
				    PT_LT_INF);
	  break;

	case PT_LT:
	case PT_LE:
	  rst = is_ranges_meetable (minval, minop, maxval, maxop,
				    NULL, PT_GT_INF, lval,
				    expr->info.expr.op);
	  break;

	case PT_IS_IN:
	  if (lval == NULL)
	    {
	      return 0;
	    }

	  setsize = set_size (lval->data.set);
	  if (setsize <= 0)
	    {
	      return 0;
	    }

	  for (i1 = 0; i1 < setsize; i1++)
	    {
	      if (set_get_element (lval->data.set, i1, &ele) != NO_ERROR)
		{
		  return 0;
		}
	      if (is_in_range (minval, minop, maxval, maxop, &ele))
		{
		  pr_clear_value (&ele);
		  break;
		}
	      pr_clear_value (&ele);
	    }

	  if (i1 >= setsize)
	    {			/* not found */
	      rst = 0;
	    }
	  else
	    {
	      rst = 1;
	    }
	  break;

	case PT_IS_NOT_IN:
	  if (maxval == NULL || minval == NULL)
	    {
	      rst = 1;		/* not prune : min/max-infinite */
	      break;
	    }

	  range_type = DB_VALUE_TYPE (maxval);
	  if (range_type != DB_TYPE_INTEGER
	      && range_type != DB_TYPE_SMALLINT
	      && range_type != DB_TYPE_BIGINT
	      && range_type != DB_TYPE_DATE
	      && range_type != DB_TYPE_TIME
	      && range_type != DB_TYPE_TIMESTAMP
	      && range_type != DB_TYPE_DATETIME)
	    {
	      rst = 1;
	      break;
	    }

	  rst = 1;
	  while (1)
	    {
	      if (db_value_compare (minval, maxval) == DB_GT)
		{
		  rst = 0;
		  break;
		}

	      if (lval == NULL)
		{
		  break;
		}

	      if (set_find_seq_element (lval->data.set, minval, 0) < 0)
		{
		  break;	/* not found */
		}
	      if (!increase_value (minval))
		{
		  break;
		}
	    }
	  break;

	case PT_IS_NULL:
	  rst = (minval == NULL) ? 1 : 0;
	  break;

	case PT_NULLSAFE_EQ:
	case PT_EQ:
	  rst = is_in_range (minval, minop, maxval, maxop, lval);
	  break;

	case PT_NE:
	  rst = 0;
	  if (is_ranges_meetable (minval, minop, maxval, maxop,
				  NULL, PT_GT_INF, lval, PT_LT) ||
	      is_ranges_meetable (minval, minop, maxval, maxop,
				  lval, PT_GT, NULL, PT_LT_INF))
	    {
	      rst = 1;
	    }
	  break;

	default:
	  break;
	}

      if (rst)
	{
	  add_pruned_partition_part (pruned, ppi, objs->op,
				     (char *) subcls->header.name);
	}
    }				/* end of for */

  pr_clear_value (&maxele);
  pr_clear_value (&minele);

  return 1;
}

/*
 * select_list_partition() -
 *   return:
 *   ppi(in):
 *   expr(in):
 *
 * Note:
 */
static int
select_list_partition (PRUNING_INFO * ppi, PT_NODE * expr)
{
  DB_OBJLIST *objs;
  DB_VALUE pval, ele;
  int setsize, i1, rst, target_cnt, check_all_flag, check_cnt;
  SM_CLASS *subcls;
  PT_NODE *actexpr, *actval, *pruned;

  target_cnt = ppi->expr_cnt + 1;
  db_make_null (&pval);

  check_all_flag = (expr->info.expr.op == PT_NOT_BETWEEN
		    || expr->info.expr.op == PT_IS_NOT_IN
		    || expr->info.expr.op == PT_IS_NOT_NULL
		    || expr->info.expr.op == PT_NE) ? 1 : 0;
  for (objs = ppi->smclass->users; objs; objs = objs->next)
    {
      pruned = get_pruned_partition_spec (ppi, objs->op);

      if (ppi->expr_cnt == 0)
	{
	  if (pruned != NULL)
	    {
	      continue;
	    }
	}
      else
	{
	  if (pruned == NULL)
	    {
	      continue;
	    }
	  if (pruned->info.name.location == target_cnt)
	    {
	      continue;
	    }
	}

      if (au_fetch_class (objs->op, &subcls, AU_FETCH_READ, AU_SELECT)
	  != NO_ERROR || !subcls->partition_of
	  || db_get (subcls->partition_of, PARTITION_ATT_PVALUES,
		     &pval) != NO_ERROR)
	{
	  continue;
	}

      setsize = set_size (pval.data.set);
      if (setsize <= 0)
	{
	  pr_clear_value (&pval);
	  continue;
	}

      check_cnt = 0;
      for (i1 = 0; i1 < setsize; i1++)
	{
	  if (set_get_element (pval.data.set, i1, &ele) != NO_ERROR)
	    {
	      continue;
	    }

	  actexpr = parser_copy_tree_list (ppi->parser, expr);
	  if (actexpr == NULL)
	    {
	      pr_clear_value (&ele);
	      continue;
	    }
	  actval = pt_dbval_to_value (ppi->parser, &ele);
	  if (actval == NULL)
	    {
	      pr_clear_value (&ele);
	      continue;
	    }

	  actval->next = expr->info.expr.arg1->next;
	  actexpr->info.expr.arg1->next = NULL;
	  parser_free_tree (ppi->parser, actexpr->info.expr.arg1);
	  actexpr->info.expr.arg1 = actval;

	  if (actexpr->info.expr.op == PT_RANGE)
	    {
	      rst = evaluate_partition_range (ppi->parser, actexpr);
	      parser_free_tree (ppi->parser, actexpr);
	    }
	  else
	    {
	      actval = pt_semantic_type (ppi->parser, actexpr, NULL);
	      if (actval == NULL)
		{
		  pr_clear_value (&ele);
		  continue;
		}
	      rst = actval->info.value.data_value.i;
	      parser_free_tree (ppi->parser, actval);
	    }
	  pr_clear_value (&ele);

	  if (check_all_flag)
	    {
	      if (rst)
		{
		  check_cnt++;
		}
	      if (expr->info.expr.op == PT_IS_NOT_NULL
		  || expr->info.expr.op == PT_NE || check_cnt > 0)
		{
		  break;
		}
	    }
	  else
	    {
	      if (rst)
		{
		  add_pruned_partition_part (pruned, ppi, objs->op,
					     (char *) subcls->header.name);
		  break;
		}
	    }
	}			/* for i1 */
      pr_clear_value (&pval);

      if (check_all_flag)
	{
	  if ((expr->info.expr.op == PT_IS_NOT_NULL
	       || expr->info.expr.op == PT_NE))
	    {
	      if (setsize != 1 || check_cnt > 0)
		{
		  add_pruned_partition_part (pruned, ppi, objs->op,
					     (char *) subcls->header.name);
		}
	    }
	  else
	    {
	      if (check_cnt > 0)
		{
		  add_pruned_partition_part (pruned, ppi, objs->op,
					     (char *) subcls->header.name);
		}
	    }
	}
    }				/* for objs */

  return 1;
}

/*
 * select_range_list() -
 *   return:
 *   ppi(in):
 *   cond(in):
 *
 * Note:
 */
static bool
select_range_list (PRUNING_INFO * ppi, PT_NODE * cond)
{
  PT_NODE *condeval = NULL;
  bool support_op = true;
  int num_markers;
  bool unbound_hostvar = false;

  condeval = parser_copy_tree_list (ppi->parser, cond);
  if (condeval == NULL)
    {
      return false;
    }

  if (condeval->info.expr.arg2
      && condeval->info.expr.arg2->node_type != PT_VALUE)
    {
      condeval->info.expr.arg2 = parser_walk_tree (ppi->parser,	/*  SYS_DATE... */
						   condeval->info.expr.arg2,
						   NULL, NULL,
						   convert_expr_to_constant,
						   &support_op);
      if (support_op == false)
	{
	  goto exit_on_end;
	}
      if (condeval->info.expr.arg2->node_type != PT_VALUE)
	{
	  PT_NODE *node;
	  node = pt_semantic_type (ppi->parser,
				   condeval->info.expr.arg2, NULL);
	  if (node == NULL)
	    {
	      goto exit_on_end;
	    }

	  condeval->info.expr.arg2 = node;
	}

      num_markers = 0;
      (void) parser_walk_leaves (ppi->parser, condeval->info.expr.arg2,
				 pt_count_input_markers,
				 &num_markers, NULL, NULL);
      if (num_markers > 0)
	{			/* found input marker, give up */
	  unbound_hostvar = true;
	  goto exit_on_end;
	}
    }

  /* eval. fail-ignore constant type mismatch etc... */
  if (ppi->parser->error_msgs)
    {
      parser_free_tree (ppi->parser, ppi->parser->error_msgs);
      ppi->parser->error_msgs = NULL;

      goto exit_on_end;
    }

  if (condeval->info.expr.arg2
      && (condeval->info.expr.arg1->type_enum
	  != condeval->info.expr.arg2->type_enum)
      && condeval->info.expr.arg2->type_enum != PT_TYPE_SET
      && (!TP_IS_CHAR_TYPE (condeval->info.expr.arg1->type_enum)
	  || !TP_IS_CHAR_TYPE (condeval->info.expr.arg2->type_enum)))
    {
      if (pt_coerce_value (ppi->parser, condeval->info.expr.arg2,
			   condeval->info.expr.arg2,
			   condeval->info.expr.arg1->type_enum,
			   condeval->info.expr.arg1->data_type) != NO_ERROR)
	{
	  goto exit_on_end;
	}
    }

  switch (ppi->type)
    {
    case PT_PARTITION_RANGE:
      if (select_range_partition (ppi, condeval) && !ppi->and_or)
	{
	  ppi->expr_cnt++;
	}
      break;
    case PT_PARTITION_LIST:
      if (select_list_partition (ppi, condeval) && !ppi->and_or)
	{
	  ppi->expr_cnt++;
	}
      break;
    case PT_PARTITION_HASH:
      if (select_hash_partition (ppi, condeval) && !ppi->and_or)
	{
	  ppi->expr_cnt++;
	}
      break;
    }

exit_on_end:

  if (condeval)
    {
      parser_free_tree (ppi->parser, condeval);
    }

  return unbound_hostvar;
}

/*
 * make_attr_search_value() -
 *   return:
 *   and_or(in):
 *   incond(in):
 *   ppi(in):
 *
 * Note:
 */
static bool
make_attr_search_value (int and_or, PT_NODE * incond, PRUNING_INFO * ppi)
{
  int a1, a2, befcnt;
  PT_NODE *cond;
  bool unbound_hostvar = false;

  if (incond == NULL || incond->node_type != PT_EXPR)
    {
      return unbound_hostvar;
    }

  if (incond->or_next)
    {				/* OR link */
      if (make_attr_search_value (1, incond->or_next, ppi))
	{
	  unbound_hostvar = true;
	}
    }

  befcnt = ppi->expr_cnt;
  cond = parser_copy_tree (ppi->parser, incond);
  if (cond == NULL)
    {
      return unbound_hostvar;
    }

  switch (cond->info.expr.op)
    {
    case PT_NOT_BETWEEN:
    case PT_IS_NOT_IN:
    case PT_IS_NOT_NULL:
    case PT_NE:
      if (ppi->type == PT_PARTITION_HASH
	  || (cond->info.expr.op == PT_IS_NOT_NULL
	      && ppi->type == PT_PARTITION_RANGE))
	{
	  break;		/* not prune */
	}

    case PT_BETWEEN:
    case PT_RANGE:
    case PT_GE:
    case PT_GT:
    case PT_LT:
    case PT_LE:
    case PT_IS_IN:
    case PT_IS_NULL:
    case PT_NULLSAFE_EQ:
    case PT_EQ:
      /* key column-constant search */
      ppi->wrkmap = 0;
      parser_walk_tree (ppi->parser, cond->info.expr.arg1,
			find_partition_attr, ppi, NULL, NULL);
      a1 = ppi->wrkmap;

      ppi->wrkmap = 0;
      parser_walk_tree (ppi->parser, cond->info.expr.arg2,
			find_partition_attr, ppi, NULL, NULL);
      a2 = ppi->wrkmap;

      if (a1 == PATTR_NOT_FOUND	/* not prune */
	  || !(a1 & PATTR_KEY) || (a1 & (PATTR_NAME | PATTR_COLUMN)))
	{
	  break;
	}
      if ((a2 != PATTR_NOT_FOUND && a2 != PATTR_VALUE))
	{
	  break;
	}

      ppi->and_or = and_or;
      if (ppi->expr->node_type == PT_NAME)
	{
	  if (cond->info.expr.arg1->node_type == PT_EXPR)
	    {
	      break;
	    }
	  if (select_range_list (ppi, cond))
	    {
	      unbound_hostvar = true;
	    }
	}
      else
	{			/* expression matching */
	  if (cond->info.expr.arg1->node_type == PT_EXPR)
	    {
	      if (check_same_expr (ppi->parser, ppi->expr,
				   cond->info.expr.arg1))
		{
		  if (select_range_list (ppi, cond))
		    {
		      unbound_hostvar = true;
		    }
		}
	      else
		{
		  break;	/* different expr - not prune */
		}
	    }
	  else
	    {
	      break;
	    }
	}
      break;

    default:
      break;

    }				/* switch */

  if (cond != NULL)
    {
      parser_free_tree (ppi->parser, cond);
    }

  if (and_or)
    {
      return unbound_hostvar;	/* OR node */
    }

  if (ppi->expr_cnt > 0 && befcnt != ppi->expr_cnt)
    {
      if (!adjust_pruned_partition (NULL, ppi))
	{
	  return unbound_hostvar;	/* No partition */
	}
    }

  if (incond->next)
    {				/* AND link */
      if (make_attr_search_value (0, incond->next, ppi))
	{
	  unbound_hostvar = true;
	}
    }

  return unbound_hostvar;
}

/*
 * make_attr_search_value() -
 *   return:
 *   spec(in):
 *   ppi(in):
 *
 * Note:
 */
static PT_NODE *
apply_no_pruning (PT_NODE * spec, PRUNING_INFO * ppi)
{
  DB_OBJLIST *objs;
  PT_NODE *rst = NULL, *newname;
  SM_CLASS *subcls;

  for (objs = ppi->smclass->users; objs; objs = objs->next)
    {
      if (au_fetch_class (objs->op, &subcls, AU_FETCH_READ, AU_SELECT)
	  != NO_ERROR)
	{
	  continue;
	}
      if (!subcls->partition_of)
	{
	  continue;
	}

      newname = pt_name (ppi->parser, subcls->header.name);
      newname->info.name.db_object = objs->op;
      newname->info.name.location = 0;
      newname->line_number = spec->line_number;
      newname->column_number = spec->column_number;
      newname->info.name.spec_id = spec->info.spec.id;
      newname->info.name.meta_class = spec->info.spec.meta_class;
      newname->info.name.partition_of = NULL;
      newname->next = NULL;

      if (rst == NULL)
	{
	  rst = newname;
	}
      else
	{
	  parser_append_node (newname, rst);
	}
    }

  return rst;
}

/*
 * do_apply_partition_pruning() -
 *   return:
 *   parser(in): Parser context
 *   stmt(in):
 *
 * Note:
 */
void
do_apply_partition_pruning (PARSER_CONTEXT * parser, PT_NODE * stmt)
{
  PRUNING_INFO pi = { NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0L };
  PT_NODE *spec, *cond, *name, *retflat;
  PT_NODE **enode;
  DB_VALUE ptype, pname, pexpr, pattr;
  DB_VALUE attr, hashsize;
  int is_all = 0, au_save;
  MOP classop;
  PARSER_CONTEXT *expr_parser = NULL;

  AU_DISABLE (au_save);

  spec = cond = name = retflat = NULL;
  switch (stmt->node_type)
    {
    case PT_SELECT:
      spec = stmt->info.query.q.select.from;
      cond = stmt->info.query.q.select.where;
      break;
    case PT_UPDATE:
      spec = stmt->info.update.spec;
      cond = stmt->info.update.search_cond;
      break;
    case PT_DELETE:
      spec = stmt->info.delete_.spec;
      cond = stmt->info.delete_.search_cond;
      break;
    case PT_SPEC:		/* path expression */
      spec = stmt;
      cond = NULL;
      break;
    default:
      break;
    }

  if (spec == NULL)
    {
      AU_ENABLE (au_save);
      return;
    }

  db_make_null (&ptype);
  db_make_null (&pname);
  db_make_null (&pexpr);
  db_make_null (&pattr);

  /* partitioned table search */
  for (; spec; spec = spec->next)
    {
      for (name = spec->info.spec.flat_entity_list; name; name = name->next)
	{
	  if (name->info.name.partition_of == NULL)
	    {
	      continue;
	    }

	  classop = db_find_class (name->info.name.original);
	  if (classop != NULL)
	    {
	      if (au_fetch_class
		  (classop, &pi.smclass, AU_FETCH_READ,
		   AU_SELECT) != NO_ERROR)
		{
		  goto work_failed;
		}
	    }
	  else
	    {
	      goto work_failed;
	    }

	  db_make_null (&ptype);
	  db_make_null (&pname);
	  db_make_null (&pexpr);
	  db_make_null (&pattr);
	  db_make_null (&attr);
	  db_make_null (&hashsize);

	  if (db_get
	      (name->info.name.partition_of, PARTITION_ATT_PNAME,
	       &pname) != NO_ERROR)
	    {
	      continue;
	    }
	  if (!DB_IS_NULL (&pname))
	    {
	      goto clear_loop;	/* partitioned sub-class */
	    }

	  if (db_get
	      (name->info.name.partition_of, PARTITION_ATT_PTYPE,
	       &ptype) != NO_ERROR)
	    {
	      goto clear_loop;
	    }

	  if (db_get
	      (name->info.name.partition_of, PARTITION_ATT_PEXPR,
	       &pexpr) != NO_ERROR)
	    {
	      goto clear_loop;
	    }
	  if (DB_IS_NULL (&pexpr))
	    {
	      goto clear_loop;
	    }

	  if (db_get
	      (name->info.name.partition_of, PARTITION_ATT_PVALUES,
	       &pattr) != NO_ERROR)
	    {
	      goto clear_loop;
	    }
	  if (DB_IS_NULL (&pattr))
	    {
	      goto clear_loop;
	    }
	  if (set_get_element (pattr.data.set, 0, &attr) != NO_ERROR)
	    {
	      goto clear_loop;
	    }

	  if (ptype.data.i == PT_PARTITION_HASH)
	    {
	      if (set_get_element (pattr.data.set, 1, &hashsize) != NO_ERROR)
		{
		  goto clear_loop;
		}
	      pi.size = hashsize.data.i;
	    }
	  else
	    {
	      pi.size = 0;
	    }

	  expr_parser = parser_create_parser ();
	  if (expr_parser == NULL)
	    {
	      goto clear_loop;
	    }

	  enode = parser_parse_string (expr_parser, DB_GET_STRING (&pexpr));
	  if (enode == NULL)
	    {
	      goto clear_loop;
	    }


	  if (*enode != NULL)
	    {
	      pi.expr = (*enode)->info.query.q.select.list;
	    }

	  if (pi.expr)
	    {
	      pi.parser = parser;
	      pi.attr = &attr;
	      pi.ppart = NULL;
	      pi.type = ptype.data.i;
	      pi.spec = name->info.name.spec_id;
	      pi.expr_cnt = 0;

	      /* search condition search & value list make */
	      if (cond != NULL && cond->node_type == PT_EXPR)
		{
		  if (make_attr_search_value (0, cond, &pi))
		    {
		      stmt->cannot_prepare = 1;	/* unbound HOSTVAR exists */
		      goto clear_loop;
		    }
		}
	      else
		{
		  is_all = 1;
		}

	      if (pi.expr_cnt <= 0)
		{
		  is_all = 1;
		}

	      if (!is_all)
		{		/* pruned partition adjust */
		  if (pi.expr_cnt > 0 && pi.ppart == NULL)
		    {
		      is_all = -1;	/* no partitions */
		    }
		  else
		    {
		      if (!adjust_pruned_partition (spec, &pi))
			{
			  is_all = -1;
			}
		    }
		}

	      if (is_all != -1)
		{
		  if (is_all)
		    {
		      retflat = apply_no_pruning (spec, &pi);
		    }
		  else
		    {
		      retflat = pi.ppart;
		    }
		  parser_append_node (retflat,
				      spec->info.spec.flat_entity_list);
		  spec->partition_pruned = 1;
		  stmt->partition_pruned = 1;
		  if (cond != NULL)
		    {
		      cond->partition_pruned = 1;
		    }
		}
	    }

	clear_loop:
	  pr_clear_value (&ptype);
	  pr_clear_value (&pname);
	  pr_clear_value (&pexpr);
	  pr_clear_value (&pattr);
	  pr_clear_value (&attr);
	  pr_clear_value (&hashsize);

	  if (expr_parser)
	    {
	      parser_free_parser (expr_parser);
	      expr_parser = NULL;
	    }
	}

      for (name = spec->info.spec.path_entities; name; name = name->next)
	{
	  if (name->info.spec.meta_class == PT_PATH_OUTER ||
	      name->info.spec.meta_class == PT_PATH_INNER)
	    {
	      do_apply_partition_pruning (parser, name);
	      if (name->partition_pruned)
		{
		  stmt->partition_pruned = 1;
		}
	    }
	}
    }

  AU_ENABLE (au_save);
  return;

work_failed:
  AU_ENABLE (au_save);

  pr_clear_value (&ptype);
  pr_clear_value (&pname);
  pr_clear_value (&pexpr);
  pr_clear_value (&pattr);

  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED, 0);
}

/*
 * check_range_merge() - Compare two DB_VALUEs specified by range operator
 *   return:
 *   val1(in):
 *   op1(in):
 *   val2(in):
 *   op2(in):
 */
static MERGE_CHECK_RESULT
check_range_merge (DB_VALUE * val1, PT_OP_TYPE op1,
		   DB_VALUE * val2, PT_OP_TYPE op2)
{
  DB_VALUE_COMPARE_RESULT rc;

  switch (op1)
    {
    case PT_EQ:
    case PT_GE:
    case PT_GT:
    case PT_LT:
    case PT_LE:
    case PT_GT_INF:
    case PT_LT_INF:
      break;
    default:
      return RANGES_ERROR;
    }

  switch (op2)
    {
    case PT_EQ:
    case PT_GE:
    case PT_GT:
    case PT_LT:
    case PT_LE:
    case PT_GT_INF:
    case PT_LT_INF:
      break;
    default:
      return RANGES_ERROR;
    }

  if (op1 == PT_GT_INF)		/* val1 is -INF */
    {
      return (op1 == op2) ? RANGES_EQUAL : RANGES_LESS;
    }
  if (op1 == PT_LT_INF)		/* val1 is +INF */
    {
      return (op1 == op2) ? RANGES_EQUAL : RANGES_GREATER;
    }
  if (op2 == PT_GT_INF)		/* val2 is -INF */
    {
      return (op2 == op1) ? RANGES_EQUAL : RANGES_GREATER;
    }
  if (op2 == PT_LT_INF)		/* va2 is +INF */
    {
      return (op2 == op1) ? RANGES_EQUAL : RANGES_LESS;
    }

  rc = (DB_VALUE_COMPARE_RESULT) tp_value_compare (val1, val2, 1, 1);
  if (rc == DB_EQ)
    {

      if (op1 == op2)
	{
	  return RANGES_EQUAL;
	}
      if (op1 == PT_EQ || op1 == PT_GE || op1 == PT_LE)
	{
	  if (op2 == PT_EQ || op2 == PT_GE || op2 == PT_LE)
	    {
	      return RANGES_EQUAL;
	    }
	  if (op2 == PT_GT)
	    {
	      return RANGES_LESS;
	    }
	  if (op2 == PT_LT)
	    {
	      return RANGES_GREATER;
	    }
	  return RANGES_EQUAL;
	}
      if (op1 == PT_GT)
	{
	  if (op2 == PT_GT)
	    {
	      return RANGES_EQUAL;
	    }
	  return RANGES_GREATER;
	}
      if (op1 == PT_LT)
	{
	  if (op2 == PT_LT)
	    {
	      return RANGES_EQUAL;
	    }
	  return RANGES_LESS;
	}
    }
  else if (rc == DB_LT)
    {
      return RANGES_LESS;
    }
  else if (rc == DB_GT)
    {
      return RANGES_GREATER;
    }

  return RANGES_ERROR;
}

/*
 * check_range_merge() -
 *   return:
 *   aval1(in):
 *   aop1(in):
 *   aval2(in):
 *   aop2(in):
 *   bval1(in):
 *   bop1(in):
 *   bval2(in):
 *   bop2(in):
 *
 * Note:
 */
static int
is_ranges_meetable (DB_VALUE * aval1, PT_OP_TYPE aop1,
		    DB_VALUE * aval2, PT_OP_TYPE aop2,
		    DB_VALUE * bval1, PT_OP_TYPE bop1,
		    DB_VALUE * bval2, PT_OP_TYPE bop2)
{
  MERGE_CHECK_RESULT cmp1, cmp2, cmp3, cmp4;

  cmp1 = check_range_merge (aval1, aop1, bval1, bop1);
  cmp2 = check_range_merge (aval1, aop1, bval2, bop2);
  cmp3 = check_range_merge (aval2, aop2, bval1, bop1);
  cmp4 = check_range_merge (aval2, aop2, bval2, bop2);

  if (cmp1 == RANGES_ERROR || cmp2 == RANGES_ERROR
      || cmp3 == RANGES_ERROR || cmp4 == RANGES_ERROR)
    {
      return 0;
    }

  if ((cmp1 == RANGES_LESS || cmp1 == RANGES_GREATER)
      && cmp1 == cmp2 && cmp1 == cmp3 && cmp1 == cmp4)
    {
      /* they are disjoint ranges */
      return 0;
    }

  return 1;
}

/*
 * is_in_range() - Check if the value is in range
 *   return:
 *   aval1(in):
 *   aop1(in):
 *   aval2(in):
 *   aop2(in):
 *   bval(in):
 *
 * Note:
 */
static int
is_in_range (DB_VALUE * aval1, PT_OP_TYPE aop1,
	     DB_VALUE * aval2, PT_OP_TYPE aop2, DB_VALUE * bval)
{
  MERGE_CHECK_RESULT cmp1, cmp2;

  cmp1 = check_range_merge (aval1, aop1, bval, PT_EQ);
  cmp2 = check_range_merge (aval2, aop2, bval, PT_EQ);

  if (cmp1 == RANGES_ERROR || cmp2 == RANGES_ERROR)
    {
      return 0;
    }

  if ((cmp1 == RANGES_LESS || cmp1 == RANGES_GREATER) && cmp1 == cmp2)
    {
      /* the value is not in range */
      return 0;
    }

  return 1;
}

/*
 * do_build_partition_xasl() -
 *   return: Error code
 *   parser(in): Parser context
 *   xasl(in/out): XASL tree to be built
 *   class_obj(in):
 *   idx(in):
 *
 * Note:
 */
int
do_build_partition_xasl (PARSER_CONTEXT * parser, XASL_NODE * xasl,
			 MOP class_obj, int idx)
{
  DB_VALUE ptype, pname, pexpr, pattr, pval, partname;
  PT_NODE **enode, *expr;
  DB_OBJLIST *objs;
  SM_CLASS *smclass, *subcls;
  DB_VALUE attr, hashsize;
  int is_error = 1, pi, au_save, partition_remove_mode = 0;
  int partition_coalesce_mode = 0, coalesce_part, partnum;
  int partition_reorg_mode = 0;
  XASL_PARTITION_INFO *xpi = NULL;
  OID *class_oid, *proxy_class_oid;
  HFID *hfid;
  int node_id;
  MOBJ class_;
  PT_TYPE_ENUM key_type;
  PARSER_CONTEXT *expr_parser = NULL;
  int delete_flag;

  if (!parser || !xasl || !class_obj)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED, 0);
      return ER_PARTITION_WORK_FAILED;
    }

  if (au_fetch_class (class_obj, &smclass, AU_FETCH_READ, AU_SELECT)
      != NO_ERROR)
    {
      return er_errid ();
    }

  db_make_null (&ptype);
  db_make_null (&pname);
  db_make_null (&pexpr);
  db_make_null (&pattr);
  db_make_null (&attr);
  db_make_null (&hashsize);
  db_make_null (&pval);
  db_make_null (&partname);

  AU_DISABLE (au_save);

  /* partitioned sub-class */
  if (db_get (smclass->partition_of, PARTITION_ATT_PNAME, &pname) != NO_ERROR
      || !DB_IS_NULL (&pname))
    {
      goto work_end;
    }

  if (db_get (smclass->partition_of, PARTITION_ATT_PTYPE, &ptype) != NO_ERROR)
    {
      goto work_end;
    }

  if (db_get (smclass->partition_of, PARTITION_ATT_PEXPR, &pexpr) != NO_ERROR
      || DB_IS_NULL (&pexpr))
    {
      goto work_end;
    }

  if (db_get (smclass->partition_of, PARTITION_ATT_PVALUES, &pattr) !=
      NO_ERROR || DB_IS_NULL (&pattr))
    {
      goto work_end;
    }

  if (set_size (pattr.data.set) >= 3)
    {
      char *p = NULL;

      if (set_get_element (pattr.data.set, 2, &attr) != NO_ERROR
	  || DB_IS_NULL (&attr) || (p = DB_GET_STRING (&attr)) == NULL)
	{
	  goto work_end;
	}
      if (p[0] == '*')
	{
	  partition_remove_mode = 1;
	}
      else if (p[0] == '#')
	{
	  partition_coalesce_mode = 1;
	  coalesce_part = atoi (&p[1]);
	  if (coalesce_part <= 0 ||
	      set_drop_seq_element (pattr.data.set, 2) != NO_ERROR)
	    {
	      goto work_end;
	    }
	  if (db_put_internal (smclass->partition_of,
			       PARTITION_ATT_PVALUES, &pattr) != NO_ERROR)
	    {
	      goto work_end;
	    }
	}
      else if (p[0] == '$')
	{
	  partition_reorg_mode = 1;
	  coalesce_part = atoi (&p[1]);
	  if (coalesce_part < 0
	      || set_drop_seq_element (pattr.data.set, 2) != NO_ERROR)
	    {
	      goto work_end;
	    }
	  if (db_put_internal (smclass->partition_of,
			       PARTITION_ATT_PVALUES, &pattr) != NO_ERROR)
	    goto work_end;
	}
    }

  if (set_get_element (pattr.data.set, 0, &attr) != NO_ERROR
      || set_get_element (pattr.data.set, 1, &hashsize) != NO_ERROR)
    {
      goto work_end;
    }

  if (hashsize.data.i <= 0)
    {
      goto work_end;
    }

  expr_parser = parser_create_parser ();
  if (expr_parser == NULL)
    {
      goto work_end;
    }

  enode = parser_parse_string (expr_parser, DB_GET_STRING (&pexpr));
  if (enode == NULL || *enode == NULL)
    {
      goto work_end;
    }

  expr = (*enode)->info.query.q.select.list;
  if (expr == NULL)
    {
      goto work_end;
    }

  key_type =
    pt_db_to_type_enum (sm_att_type_id (class_obj, DB_GET_STRING (&attr)));

  parser_walk_tree (expr_parser, expr, adjust_name_with_type, &key_type, NULL,
		    NULL);
  pt_semantic_type (expr_parser, expr, NULL);

  xpi = regu_partition_info_alloc ();
  if (xpi == NULL)
    {
      goto work_end;
    }

  xpi->no_parts = hashsize.data.i;
  if (partition_coalesce_mode)
    {
      xpi->act_parts = coalesce_part;
    }
  else if (partition_reorg_mode)
    {
      xpi->act_parts = hashsize.data.i - coalesce_part;
    }
  else
    {
      xpi->act_parts = hashsize.data.i;
    }
  xpi->type = ptype.data.i;
  db_make_null (&pval);

  /* partition key to NULL-value replace */
  if (expr->node_type == PT_NAME)
    {
      parser_free_tree (expr_parser, expr);
      expr = pt_dbval_to_value (parser, &pval);
    }
  else
    {
      parser_walk_tree (expr_parser, expr, replace_name_with_value, &pval,
			NULL, NULL);
    }

  xpi->expr = pt_to_regu_variable (parser, expr, UNBOX_AS_VALUE);
  xpi->parts = regu_parts_array_alloc (xpi->no_parts);
  if (xpi->parts == NULL)
    {
      goto work_end;
    }
  if (partition_remove_mode)
    {
      xpi->key_attr = -1;
    }
  else
    {
      xpi->key_attr = sm_att_id (class_obj, DB_GET_STRING (&attr));
    }

  for (pi = 0, objs = smclass->users; objs; objs = objs->next)
    {
      bool reuse_oid = false;

      if (au_fetch_class (objs->op, &subcls, AU_FETCH_READ, AU_SELECT)
	  != NO_ERROR)
	{
	  goto work_end;
	}

      if (!subcls->partition_of)
	{
	  continue;
	}

      delete_flag = 0;
      if (partition_coalesce_mode)
	{
	  if (db_get (subcls->partition_of, PARTITION_ATT_PNAME, &partname)
	      != NO_ERROR)
	    {
	      goto work_end;
	    }
	  partnum = atoi (DB_GET_STRING (&partname) + 1);
	  pr_clear_value (&partname);
	  if (partnum >= coalesce_part)
	    {
	      delete_flag = 1;
	    }
	}

      if (partition_reorg_mode)
	{
	  if (db_get (subcls->partition_of, PARTITION_ATT_PEXPR, &pval)
	      != NO_ERROR)
	    {
	      goto work_end;
	    }
	  if (!DB_IS_NULL (&pval))
	    {
	      delete_flag = 1;
	    }
	  pr_clear_value (&pval);
	}

      reuse_oid = (subcls->flags & SM_CLASSFLAG_REUSE_OID) ? true : false;
      class_ = locator_create_heap_if_needed (objs->op, reuse_oid);
      if (class_ == NULL || (hfid = sm_heap (class_)) == NULL
	  || (locator_flush_class (objs->op) != NO_ERROR))
	{
	  goto work_end;
	}

      proxy_class_oid = class_oid = ws_identifier (objs->op);
      if (!class_oid)
	{
	  goto work_end;
	}

      node_id = DB_CLUSTER_NODE_LOCAL;
      if (sm_get_class_type ((SM_CLASS *) class_) == SM_PCLASS_CT)
	{
	  class_oid = &(((SM_CLASS *) class_)->real_oid);
	  hfid = &(((SM_CLASS *) class_)->real_hfid);
	  node_id = db_find_node_ip (((SM_CLASS *) class_)->node_name);
	}

      if (delete_flag)
	{
	  db_make_null (&pval);
	}
      else
	{
	  if (db_get (subcls->partition_of, PARTITION_ATT_PVALUES, &pval)
	      != NO_ERROR)
	    {
	      goto work_end;
	    }

	  if (DB_IS_NULL (&pval) || set_size (pval.data.set) <= 0)
	    {
	      goto work_end;
	    }
	}

      xpi->parts[pi] = regu_parts_info_alloc ();
      if (xpi->parts[pi] == NULL)
	{
	  goto work_end;
	}

      xpi->parts[pi]->class_oid = *class_oid;
      xpi->parts[pi]->proxy_class_oid = *proxy_class_oid;
      xpi->parts[pi]->class_hfid = *hfid;
      xpi->parts[pi]->node_id = node_id;
      xpi->parts[pi]->vals = regu_dbval_alloc ();
      regu_dbval_type_init (xpi->parts[pi]->vals, DB_VALUE_TYPE (&pval));
      db_value_clone (&pval, xpi->parts[pi]->vals);
      pr_clear_value (&pval);
      pi++;
    }

  is_error = 0;
  if (idx <= 0)
    {
      xasl->proc.insert.partition = xpi;
    }
  else
    {
      xasl->proc.update.partition[idx - 1] = xpi;
    }

work_end:
  AU_ENABLE (au_save);

  pr_clear_value (&ptype);
  pr_clear_value (&pname);
  pr_clear_value (&pexpr);
  pr_clear_value (&pattr);
  pr_clear_value (&attr);
  pr_clear_value (&hashsize);
  pr_clear_value (&pval);
  if (expr_parser)
    parser_free_parser (expr_parser);

  if (is_error)
    {
      if (er_errid ())
	{
	  return er_errid ();
	}
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED, 0);
      return ER_PARTITION_WORK_FAILED;
    }
  else
    {
      return NO_ERROR;
    }
}

/*
 * do_check_partitioned_class() - Checks partitioned class
 *   return: Error code if check_map or keyattr is checked
 *   classop(in): MOP of class
 *   class_map(in/out): Checking method(CHECK_PARTITION_NONE, _PARTITION_PARENT,
 *			 _PARTITION_SUBS)
 *   keyattr(in): Partition key attribute to check
 *
 * Note:
 */
int
do_check_partitioned_class (DB_OBJECT * classop, int check_map, char *keyattr)
{
  int error = NO_ERROR;
  int is_partition = 0;
  char attr_name[DB_MAX_IDENTIFIER_LENGTH];

  error = do_is_partitioned_classobj (&is_partition, classop,
				      (keyattr) ? attr_name : NULL, NULL);
  if (error != NO_ERROR)
    return error;

  if (is_partition > 0)
    {
      if (((check_map & CHECK_PARTITION_PARENT) && is_partition == 1)
	  || ((check_map & CHECK_PARTITION_SUBS) && is_partition == 2))
	{
	  error = ER_NOT_ALLOWED_ACCESS_TO_PARTITION;
	}
      else if (keyattr)
	{
	  if (intl_mbs_casecmp (keyattr, attr_name) == 0)
	    {
	      error = ER_NOT_ALLOWED_ACCESS_TO_PARTITION;
	    }
	}

      if (error != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_NOT_ALLOWED_ACCESS_TO_PARTITION, 0);
	}
    }

  return error;
}

/*
 * do_get_partition_parent () -
 *   return: NO_ERROR or error code
 *   classop(in): MOP of class
 *   parentop(out): MOP of the parent of the sub-partition or NULL if not a
 *                  sub-partition
 */
int
do_get_partition_parent (DB_OBJECT * const classop, MOP * const parentop)
{
  int is_partition = NOT_PARTITION_CLASS;
  int error_code = NO_ERROR;
  int au_save = 0;
  SM_CLASS *smclass = NULL;
  DB_VALUE pname;
  DB_VALUE pclassof;
  DB_VALUE classobj;

  db_make_null (&pname);
  db_make_null (&pclassof);
  db_make_null (&classobj);

  assert (classop != NULL);
  assert (parentop != NULL && *parentop == NULL);
  *parentop = NULL;

  AU_DISABLE (au_save);

  error_code = au_fetch_class (classop, &smclass, AU_FETCH_READ, AU_SELECT);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }
  if (smclass->partition_of == NULL)
    {
      goto end;
    }

  error_code = db_get (smclass->partition_of, PARTITION_ATT_PNAME, &pname);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  is_partition = DB_IS_NULL (&pname) ? PARTITIONED_CLASS : PARTITION_CLASS;
  if (is_partition != PARTITION_CLASS)
    {
      goto end;
    }

  error_code = db_get (smclass->partition_of, PARTITION_ATT_CLASSOF,
		       &pclassof);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  error_code = db_get (DB_GET_OBJECT (&pclassof), PARTITION_ATT_CLASSOF,
		       &classobj);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  *parentop = DB_PULL_OBJECT (&classobj);
  assert (*parentop != NULL);

end:
  AU_ENABLE (au_save);
  pr_clear_value (&pname);
  pr_clear_value (&pclassof);
  pr_clear_value (&classobj);
  smclass = NULL;

  return error_code;

error_exit:
  AU_ENABLE (au_save);
  pr_clear_value (&pname);
  pr_clear_value (&pclassof);
  pr_clear_value (&classobj);
  smclass = NULL;
  *parentop = NULL;

  return error_code;
}

/*
 * do_is_partitioned_classobj() -
 *   return: NO_ERROR or error code
 *   is_partition(in/out): 0 if not partition, 1 if partition and parent,
 *                         2 if sub-partition
 *   classop(in): MOP of class
 *   keyattr(in): Partition key attribute to check
 *   sub_partitions(in): MOP of sub class
 *
 * Note:
 */
int
do_is_partitioned_classobj (int *is_partition,
			    DB_OBJECT * classop, char *keyattr,
			    MOP ** sub_partitions)
{
  DB_OBJLIST *objs;
  SM_CLASS *smclass, *subcls;
  DB_VALUE pname, pattr, psize, attrname, pclassof, classobj;
  int au_save, pcnt, i;
  MOP *subobjs = NULL;
  int error;

  assert (classop != NULL);
  assert (is_partition != NULL);

  *is_partition = NOT_PARTITION_CLASS;

  AU_DISABLE (au_save);

  error = au_fetch_class (classop, &smclass, AU_FETCH_READ, AU_SELECT);
  if (error != NO_ERROR)
    {
      AU_ENABLE (au_save);
      return error;
    }
  if (!smclass->partition_of)
    {
      AU_ENABLE (au_save);
      return NO_ERROR;
    }

  db_make_null (&pname);
  db_make_null (&pattr);
  db_make_null (&psize);
  db_make_null (&attrname);
  db_make_null (&pclassof);
  db_make_null (&classobj);

  if (db_get (smclass->partition_of, PARTITION_ATT_PNAME, &pname) != NO_ERROR)
    {
      goto partition_failed;
    }
  *is_partition = (DB_IS_NULL (&pname) ? PARTITIONED_CLASS : PARTITION_CLASS);

  if (keyattr || sub_partitions)
    {
      if (*is_partition == PARTITION_CLASS)
	{			/* sub-partition */
	  if (db_get (smclass->partition_of, PARTITION_ATT_CLASSOF, &pclassof)
	      != NO_ERROR
	      || db_get (DB_GET_OBJECT (&pclassof), PARTITION_ATT_CLASSOF,
			 &classobj) != NO_ERROR
	      || au_fetch_class (DB_PULL_OBJECT (&classobj), &smclass,
				 AU_FETCH_READ, AU_SELECT) != NO_ERROR)
	    {
	      goto partition_failed;
	    }
	}

      if (db_get (smclass->partition_of, PARTITION_ATT_PVALUES, &pattr)
	  != NO_ERROR)
	{
	  goto partition_failed;
	}
      if (set_get_element (pattr.data.set, 0, &attrname) != NO_ERROR)
	{
	  goto partition_failed;
	}
      if (set_get_element (pattr.data.set, 1, &psize) != NO_ERROR)
	{
	  goto partition_failed;
	}

      pcnt = psize.data.i;
      if (keyattr)
	{
	  char *p = NULL;

	  keyattr[0] = 0;
	  if (DB_IS_NULL (&attrname)
	      || (p = DB_GET_STRING (&attrname)) == NULL)
	    {
	      goto partition_failed;
	    }
	  strncpy (keyattr, p, DB_MAX_IDENTIFIER_LENGTH);
	}

      if (sub_partitions)
	{
	  subobjs = (MOP *) malloc (sizeof (MOP) * (pcnt + 1));
	  if (subobjs == NULL)
	    {
	      goto partition_failed;
	    }
	  memset (subobjs, 0, sizeof (MOP) * (pcnt + 1));

	  for (objs = smclass->users, i = 0; objs && i < pcnt;
	       objs = objs->next)
	    {
	      if (au_fetch_class (objs->op, &subcls, AU_FETCH_READ, AU_SELECT)
		  != NO_ERROR)
		{
		  goto partition_failed;
		}
	      if (!subcls->partition_of)
		{
		  continue;
		}
	      subobjs[i++] = objs->op;
	    }
	  if (i < pcnt)
	    {
	      goto partition_failed;
	    }

	  *sub_partitions = subobjs;
	}
    }

  AU_ENABLE (au_save);

  pr_clear_value (&pname);
  pr_clear_value (&pattr);
  pr_clear_value (&psize);
  pr_clear_value (&attrname);
  pr_clear_value (&pclassof);
  pr_clear_value (&classobj);

  return NO_ERROR;

partition_failed:

  AU_ENABLE (au_save);

  if (subobjs)
    {
      free_and_init (subobjs);
    }

  pr_clear_value (&pname);
  pr_clear_value (&pattr);
  pr_clear_value (&psize);
  pr_clear_value (&attrname);
  pr_clear_value (&pclassof);
  pr_clear_value (&classobj);

  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED, 0);

  return ER_PARTITION_WORK_FAILED;
}

/*
 * do_is_partitioned_subclass() -
 *   return: 1 if success, else error code
 *   is_partitioned(in/out):
 *   classname(in):
 *   keyattr(in):
 *
 * Note:
 */
int
do_is_partitioned_subclass (int *is_partitioned,
			    const char *classname, char *keyattr)
{
  MOP classop;
  SM_CLASS *smclass;
  DB_VALUE pname, pattr, attrname;
  int au_save, ret = 0;

  if (!classname)
    {
      return 0;
    }
  if (is_partitioned)
    {
      *is_partitioned = 0;
    }

  classop = db_find_class (classname);
  if (classop == NULL)
    {
      return 0;
    }

  AU_DISABLE (au_save);

  if (au_fetch_class (classop, &smclass, AU_FETCH_READ, AU_SELECT)
      != NO_ERROR || !smclass->partition_of)
    {
      AU_ENABLE (au_save);
      return 0;
    }

  db_make_null (&pname);
  if (db_get (smclass->partition_of, PARTITION_ATT_PNAME, &pname) != NO_ERROR)
    {
      AU_ENABLE (au_save);
      return 0;
    }

  if (!DB_IS_NULL (&pname))
    {
      ret = 1;			/* partitioned sub-class */
    }
  else
    {
      if (is_partitioned)
	{
	  *is_partitioned = 1;
	}

      if (keyattr)
	{
	  char *p = NULL;

	  keyattr[0] = 0;
	  db_make_null (&pattr);

	  if (db_get (smclass->partition_of, PARTITION_ATT_PVALUES,
		      &pattr) == NO_ERROR
	      && set_get_element (pattr.data.set, 0, &attrname) == NO_ERROR
	      && !DB_IS_NULL (&attrname) && (p = DB_GET_STRING (&attrname)))
	    {
	      strncpy (keyattr, p, DB_MAX_IDENTIFIER_LENGTH);

	      pr_clear_value (&pattr);
	      pr_clear_value (&attrname);
	    }
	}
    }

  pr_clear_value (&pname);
  AU_ENABLE (au_save);

  return ret;
}

/*
 * do_drop_partition() -
 *   return: Error code
 *   class(in):
 *   drop_sub_flag(in):
 *
 * Note:
 */
int
do_drop_partition (MOP class_, int drop_sub_flag)
{
  DB_OBJLIST *objs;
  SM_CLASS *smclass, *subclass;
  DB_VALUE pname;
  int au_save;
  MOP delobj, delpart, super_in_ctclass;
  int error = NO_ERROR;
  bool is_global;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      return ER_AU_AUTHORIZATION_FAILURE;
    }

  if (!class_)
    {
      return -1;
    }

  AU_DISABLE (au_save);

  db_make_null (&pname);
  error = au_fetch_class (class_, &smclass, AU_FETCH_READ, AU_SELECT);
  if (error != NO_ERROR)
    {
      goto fail_return;
    }
  if (!smclass->partition_of)
    {
      error = ER_FAILED;
      goto fail_return;
    }

  error = db_get (smclass->partition_of, PARTITION_ATT_PNAME, &pname);
  if (error != NO_ERROR)
    {
      goto fail_return;
    }
  if (!DB_IS_NULL (&pname))
    {
      error = ER_FAILED;
      goto fail_return;		/* partitioned sub-class */
    }

  is_global = sm_is_global_class (class_);
  if (is_global)
    {
      const char *class_name;
      MOP catalog_class;
      DB_VALUE name_val;

      catalog_class = sm_find_class (CT_CLASS_NAME);
      if (catalog_class == NULL)
	{
	  error = er_errid ();
	  goto fail_return;
	}

      class_name = sm_classobj_name ((MOBJ) smclass);
      db_make_string (&name_val, class_name);

      super_in_ctclass =
	obj_find_oids_in_catalog_global (catalog_class, CLASS_ATT_NAME,
					 &name_val);
      pr_clear_value (&name_val);
      if (super_in_ctclass == NULL)
	{
	  error = er_errid ();
	  goto fail_return;
	}

      /* step 1: get the remote partition_of. */
      error = obj_find_partition_global (class_, smclass, super_in_ctclass);
      if (error != NO_ERROR)
	{
	  goto fail_return;
	}

      /* step 2: get the remote subclasses->partiton_of. */
      for (objs = smclass->users; objs; objs = objs->next)
	{
	  if (NO_ERROR !=
	      au_fetch_class (objs->op, &subclass, AU_FETCH_READ, AU_SELECT))
	    {
	      error = ER_FAILED;
	      goto fail_return;
	    }

	  error =
	    obj_find_partition_global (objs->op, subclass, super_in_ctclass);
	  if (error != NO_ERROR)
	    {
	      goto fail_return;
	    }
	}

      error = obj_delete_global (smclass->partition_of);
    }
  else
    {
      error = obj_delete (smclass->partition_of);
    }

  if (error != NO_ERROR)
    {
      goto fail_return;
    }

  for (objs = smclass->users; objs;)
    {
      error = au_fetch_class (objs->op, &subclass, AU_FETCH_READ, AU_SELECT);
      if (error != NO_ERROR)
	{
	  goto fail_return;
	}
      if (subclass->partition_of)
	{
	  delpart = subclass->partition_of;
	  delobj = objs->op;
	  objs = objs->next;
	  if (drop_sub_flag)
	    {
	      error = sm_delete_class_mop (delobj);
	      if (error != NO_ERROR)
		{
		  goto fail_return;
		}
	    }

	  if (is_global)
	    {
	      error = obj_delete_global (delpart);
	    }
	  else
	    {
	      error = obj_delete (delpart);
	    }

	  if (error != NO_ERROR)
	    {
	      goto fail_return;
	    }
	}
      else
	{
	  objs = objs->next;
	}
    }

  error = NO_ERROR;

fail_return:
  AU_ENABLE (au_save);

  pr_clear_value (&pname);
  return error;
}

/*
 * partition_rename() -
 *   return: Error code
 *   old_class(in):
 *   newname(in):
 *
 * Note:
 */
int
do_rename_partition (MOP old_class, const char *newname)
{
  DB_OBJLIST *objs;
  SM_CLASS *smclass, *subclass;
  int au_save, newlen;
  int error;
  char new_subname[PARTITION_VARCHAR_LEN + 1], *ptr;

  if (!old_class || !newname)
    {
      return -1;
    }

  newlen = strlen (newname);

  AU_DISABLE (au_save);

  error = au_fetch_class (old_class, &smclass, AU_FETCH_READ, AU_SELECT);
  if (error != NO_ERROR)
    {
      goto end_rename;
    }

  for (objs = smclass->users; objs; objs = objs->next)
    {
      if ((au_fetch_class (objs->op, &subclass, AU_FETCH_READ, AU_SELECT)
	   == NO_ERROR) && subclass->partition_of)
	{
	  ptr = strstr ((char *) subclass->header.name,
			PARTITIONED_SUB_CLASS_TAG);
	  if (ptr == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_PARTITION_WORK_FAILED, 0);
	      goto end_rename;
	    }

	  if ((newlen + strlen (ptr)) >= PARTITION_VARCHAR_LEN)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_PARTITION_WORK_FAILED, 0);
	      goto end_rename;
	    }
	  sprintf (new_subname, "%s%s", newname, ptr);

	  error = sm_rename_class (objs->op, new_subname);
	  if (error != NO_ERROR)
	    {
	      break;
	    }
	}
    }

end_rename:
  AU_ENABLE (au_save);

  return error;
}

/*
 * do_is_partition_changed() -
 *   return: MOP of class
 *   parser(in): Parser context
 *   smclass(in):
 *   editobj(in):
 *   list_column_names(in):
 *   list_values(in):
 *   const_column_names(in):
 *   const_values(in):
 *
 * Note:
 */
MOP
do_is_partition_changed (PARSER_CONTEXT * parser, SM_CLASS * smclass,
			 MOP editobj, PT_NODE * list_column_names,
			 DB_VALUE * list_values, PT_NODE * const_column_names,
			 DB_VALUE * const_values)
{
  SM_CLASS *supclass;
  DB_VALUE ptype, pname, pexpr, pattr, *retval, attrname, *valptr;
  MOP chgobj = NULL;
  PT_NODE *name;
  char flag_prc, *nameptr;
  int au_save;

  if (!smclass || !editobj || !smclass->partition_of || !smclass->inheritance)
    {
      return NULL;
    }
  if (au_fetch_class (smclass->inheritance->op, &supclass, AU_FETCH_READ,
		      AU_SELECT) != NO_ERROR)
    {
      return NULL;
    }

  db_make_null (&ptype);
  db_make_null (&pname);
  db_make_null (&pexpr);
  db_make_null (&pattr);
  db_make_null (&attrname);

  AU_DISABLE (au_save);

  if (db_get (supclass->partition_of, PARTITION_ATT_PNAME, &pname) !=
      NO_ERROR)
    {
      goto end_partition;
    }

  /* adjust only partition parent class */
  if (DB_IS_NULL (&pname))
    {
      if (db_get (supclass->partition_of, PARTITION_ATT_PTYPE, &ptype)
	  != NO_ERROR
	  || db_get (supclass->partition_of, PARTITION_ATT_PEXPR,
		     &pexpr) != NO_ERROR
	  || db_get (supclass->partition_of, PARTITION_ATT_PVALUES,
		     &pattr) != NO_ERROR)
	{
	  goto end_partition;
	}

      if (set_get_element (pattr.data.set, 0, &attrname) != NO_ERROR)
	{
	  goto end_partition;
	}

      if (DB_IS_NULL (&attrname))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED,
		  0);
	  goto end_partition;
	}

      nameptr = DB_PULL_STRING (&attrname);
      /* partition key column search */
      valptr = list_values;
      flag_prc = 0;

      for (name = list_column_names; name != NULL; name = name->next)
	{
	  if (SM_COMPARE_NAMES (nameptr, name->info.name.original) == 0)
	    {
	      flag_prc = 1;
	      break;
	    }
	  valptr++;
	}

      if (!flag_prc)
	{
	  valptr = const_values;
	  for (name = const_column_names; name != NULL; name = name->next)
	    {
	      if (SM_COMPARE_NAMES (nameptr, name->info.name.original) == 0)
		{
		  flag_prc = 1;
		  break;
		}
	      valptr++;
	    }
	}
      if (!flag_prc)		/* partition key column not found! */
	{
	  goto end_partition;
	}

      retval = evaluate_partition_expr (&pexpr, valptr);
      if (retval == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED,
		  0);
	  goto end_partition;
	}
      get_partition_parts (&chgobj, supclass, ptype.data.i, &pattr, retval);
      if (chgobj && ws_mop_compare (editobj, chgobj) == 0)
	{
	  chgobj = NULL;	/* same partition */
	}
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED, 0);
      goto end_partition;
    }

end_partition:
  pr_clear_value (&ptype);
  pr_clear_value (&pname);
  pr_clear_value (&pexpr);
  pr_clear_value (&pattr);
  pr_clear_value (&attrname);

  AU_ENABLE (au_save);

  return chgobj;
}

/*
 * do_update_partition_newly() -
 *   return: Error code
 *   classname(in):
 *   keyname(in):
 *
 * Note:
 */
int
do_update_partition_newly (const char *classname, const char *keyname)
{
  int error = NO_ERROR;
  DB_QUERY_RESULT *query_result;
  DB_QUERY_ERROR query_error;
  char *sqlbuf;

  sqlbuf = (char *) malloc (20 + strlen (classname) + strlen (keyname) * 2);
  if (sqlbuf == NULL)
    {
      return -1;
    }
  sprintf (sqlbuf, "UPDATE %s SET %s=%s;", classname, keyname, keyname);

  error = db_execute (sqlbuf, &query_result, &query_error);
  if (error >= 0)
    {
      error = NO_ERROR;
      db_query_end (query_result);
    }
  free_and_init (sqlbuf);

  return error;
}

/*
 * do_remove_partition_pre() -
 *   return: Error code
 *   clstmp(in):
 *   keynattr(in):
 *   magic_word(in):
 *
 * Note:
 */
int
do_remove_partition_pre (DB_CTMPL * clstmpl, char *keyattr,
			 const char *magic_word)
{
  int error = NO_ERROR;
  DB_VALUE pattr, attrname, star;
  int au_save;

  if (clstmpl && keyattr)
    {
      if (clstmpl->partition_of)
	{
	  char *p = NULL;

	  AU_DISABLE (au_save);

	  keyattr[0] = 0;
	  db_make_null (&pattr);
	  error = db_get (clstmpl->partition_of, PARTITION_ATT_PVALUES,
			  &pattr);
	  if (error == NO_ERROR
	      && ((error = set_get_element (pattr.data.set, 0, &attrname)) ==
		  NO_ERROR) && !DB_IS_NULL (&attrname)
	      && (p = DB_GET_STRING (&attrname)))
	    {
	      strncpy (keyattr, p, DB_MAX_IDENTIFIER_LENGTH);

	      /* '*' set to 3rd element - partition remove mode update */
	      /* '#Number' set to 3rd element - partition coalesce mode update */
	      db_make_string (&star, magic_word);
	      error = set_add_element (pattr.data.set, &star);
	      if (error == NO_ERROR)
		{
		  error = db_put_internal (clstmpl->partition_of,
					   PARTITION_ATT_PVALUES, &pattr);
		}

	      pr_clear_value (&pattr);
	      pr_clear_value (&attrname);
	      pr_clear_value (&star);
	    }

	  AU_ENABLE (au_save);
	}
    }

  return error;
}

/*
 * do_remove_partition_post() -
 *   return: Error code
 *   parser(in): Parser context
 *   classname(in):
 *   keyname(in):
 *
 * Note:
 */
int
do_remove_partition_post (PARSER_CONTEXT * parser, const char *classname,
			  const char *keyname)
{
  int error = NO_ERROR;
  DB_CTMPL *ctmpl;
  MOP vclass;

  error = do_update_partition_newly (classname, keyname);
  if (error == NO_ERROR)
    {
      vclass = db_find_class (classname);
      if (vclass == NULL)
	{
	  error = er_errid ();
	  return error;
	}

      error = do_drop_partition (vclass, 1);
      if (error != NO_ERROR)
	{
	  return error;
	}

      ctmpl = dbt_edit_class (vclass);
      if (ctmpl != NULL)
	{
	  ctmpl->partition_of = NULL;

	  if (dbt_finish_class (ctmpl) == NULL)
	    {
	      error = er_errid ();
	      dbt_abort_class (ctmpl);
	    }
	  else if (locator_flush_class (vclass) != NO_ERROR)
	    {
	      error = er_errid ();
	    }

	}
      else
	{
	  error = er_errid ();
	}
    }

  return error;
}

/*
 * adjust_partition_range() -
 *   return: Error code
 *   objs(in):
 *
 * Note:
 */
static int
adjust_partition_range (DB_OBJLIST * objs)
{
  DB_OBJLIST *subs;
  SM_CLASS *subclass;
  DB_VALUE ptype, pexpr, pattr, minval, maxval, seqval, *wrtval;
  int error = NO_ERROR;
  int au_save;
  char check_flag = 1;
  DB_VALUE_SLIST *ranges = NULL, *rfind, *new_range, *prev_range;
  DB_COLLECTION *dbc = NULL;

  db_make_null (&ptype);
  db_make_null (&pattr);
  db_make_null (&minval);
  db_make_null (&maxval);

  AU_DISABLE (au_save);
  for (subs = objs; subs; subs = subs->next)
    {
      error = au_fetch_class (subs->op, &subclass, AU_FETCH_READ, AU_SELECT);
      if (error != NO_ERROR)
	{
	  break;
	}
      if (!subclass->partition_of)
	{
	  continue;
	}

      if (check_flag)
	{			/* RANGE check */
	  error = db_get (subclass->partition_of, PARTITION_ATT_PTYPE,
			  &ptype);
	  if (error != NO_ERROR)
	    {
	      break;
	    }
	  if (ptype.data.i != PT_PARTITION_RANGE)
	    {
	      break;
	    }
	  check_flag = 0;
	}

      error = db_get (subclass->partition_of, PARTITION_ATT_PVALUES, &pattr);
      if (error != NO_ERROR)
	{
	  break;
	}
      error = db_get (subclass->partition_of, PARTITION_ATT_PEXPR, &pexpr);
      if (error != NO_ERROR)
	{
	  break;
	}
      if (!DB_IS_NULL (&pexpr))
	{
	  pr_clear_value (&pattr);
	  pr_clear_value (&pexpr);
	  continue;		/* reorg deleted partition */
	}

      error = set_get_element (pattr.data.set, 0, &minval);
      if (error != NO_ERROR)
	{
	  break;
	}
      error = set_get_element (pattr.data.set, 1, &maxval);
      if (error != NO_ERROR)
	{
	  break;
	}
      if ((new_range =
	   (DB_VALUE_SLIST *) malloc (sizeof (DB_VALUE_SLIST))) == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  sizeof (DB_VALUE_SLIST));
	  break;
	}
      new_range->partition_of = subclass->partition_of;
      new_range->min = db_value_copy (&minval);
      new_range->max = db_value_copy (&maxval);
      new_range->next = NULL;
      pr_clear_value (&minval);
      pr_clear_value (&maxval);
      pr_clear_value (&pattr);

      if (ranges == NULL)
	{
	  ranges = new_range;
	}
      else
	{			/* sort ranges */
	  for (rfind = ranges, prev_range = NULL; rfind; rfind = rfind->next)
	    {
	      if (DB_IS_NULL (rfind->max)
		  || db_value_compare (rfind->max, new_range->max) == DB_GT)
		{
		  if (prev_range == NULL)
		    {
		      new_range->next = ranges;
		      ranges = new_range;
		    }
		  else
		    {
		      new_range->next = prev_range->next;
		      prev_range->next = new_range;
		    }
		  break;
		}
	      prev_range = rfind;
	    }

	  if (rfind == NULL)
	    {
	      prev_range->next = new_range;
	    }
	}
    }

  for (rfind = ranges, prev_range = NULL; rfind; rfind = rfind->next)
    {
      wrtval = NULL;
      if (prev_range == NULL)
	{			/* Min value of first range is low infinite */
	  if (!DB_IS_NULL (rfind->min))
	    {
	      db_make_null (&minval);
	      wrtval = &minval;
	    }
	}
      else
	{
	  if (db_value_compare (prev_range->max, rfind->min) != DB_EQ)
	    {
	      wrtval = prev_range->max;
	    }
	}
      if (wrtval != NULL)
	{			/* adjust min value of range */
	  dbc = set_create_sequence (0);
	  if (dbc != NULL)
	    {
	      set_add_element (dbc, wrtval);
	      set_add_element (dbc, rfind->max);
	      db_make_sequence (&seqval, dbc);
	      error = db_put_internal (rfind->partition_of,
				       PARTITION_ATT_PVALUES, &seqval);
	      set_free (dbc);
	    }
	  if (error != NO_ERROR)
	    {
	      break;
	    }
	}
      prev_range = rfind;
    }

  for (rfind = ranges; rfind;)
    {
      db_value_free (rfind->min);
      db_value_free (rfind->max);
      prev_range = rfind->next;
      free_and_init (rfind);
      rfind = prev_range;
    }
  pr_clear_value (&ptype);
  pr_clear_value (&pattr);
  pr_clear_value (&minval);
  pr_clear_value (&maxval);
  AU_ENABLE (au_save);
  return error;
}

/*
 * adjust_partition_size() -
 *   return: Error code
 *   class(in):
 *
 * Note:
 */
static int
adjust_partition_size (MOP class_)
{
  int error = NO_ERROR;
  SM_CLASS *smclass, *subclass;
  DB_VALUE pattr, keyname, psize;
  DB_OBJLIST *subs;
  int au_save, partcnt;

  if (!class_)
    {
      return -1;
    }
  error = au_fetch_class (class_, &smclass, AU_FETCH_READ, AU_SELECT);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (!smclass->partition_of)
    {
      error = ER_INVALID_PARTITION_REQUEST;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }

  db_make_null (&psize);
  db_make_null (&pattr);
  db_make_null (&keyname);

  AU_DISABLE (au_save);
  for (subs = smclass->users, partcnt = 0; subs; subs = subs->next)
    {
      error = au_fetch_class (subs->op, &subclass, AU_FETCH_READ, AU_SELECT);
      if (error != NO_ERROR)
	{
	  goto fail_end;
	}
      if (!subclass->partition_of)
	{
	  continue;
	}
      partcnt++;
    }
  error = db_get (smclass->partition_of, PARTITION_ATT_PVALUES, &pattr);
  if (error != NO_ERROR)
    {
      goto fail_end;
    }
  error = set_get_element (pattr.data.set, 0, &keyname);
  if (error != NO_ERROR)
    {
      goto fail_end;
    }
  error = set_get_element (pattr.data.set, 1, &psize);
  if (error != NO_ERROR)
    {
      goto fail_end;
    }
  if (psize.data.i != partcnt)
    {
      psize.data.i = partcnt;
      error = set_put_element (pattr.data.set, 1, &psize);
      if (error != NO_ERROR)
	{
	  goto fail_end;
	}
      error =
	db_put_internal (smclass->partition_of, PARTITION_ATT_PVALUES,
			 &pattr);
      if (error != NO_ERROR)
	{
	  goto fail_end;
	}
    }
  error = NO_ERROR;

fail_end:
  pr_clear_value (&keyname);
  pr_clear_value (&psize);
  pr_clear_value (&pattr);
  AU_ENABLE (au_save);
  return error;
}

/*
 * do_get_partition_size() -
 *   return: Size if success, else error code
 *   class(in):
 *
 * Note:
 */
int
do_get_partition_size (MOP class_)
{
  int error = NO_ERROR;
  SM_CLASS *smclass;
  DB_VALUE pattr, psize;
  int au_save;

  if (!class_)
    {
      return -1;
    }
  error = au_fetch_class (class_, &smclass, AU_FETCH_READ, AU_SELECT);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (!smclass->partition_of)
    {
      error = ER_INVALID_PARTITION_REQUEST;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }

  db_make_null (&psize);
  db_make_null (&pattr);

  AU_DISABLE (au_save);
  error = db_get (smclass->partition_of, PARTITION_ATT_PVALUES, &pattr);
  if (error != NO_ERROR)
    {
      goto fail_end;
    }
  error = set_get_element (pattr.data.set, 1, &psize);
  if (error != NO_ERROR)
    {
      goto fail_end;
    }
  error = psize.data.i;
  if (error == 0)
    {
      error = -1;
    }

fail_end:
  pr_clear_value (&psize);
  pr_clear_value (&pattr);
  AU_ENABLE (au_save);
  return error;
}

/*
 * do_get_partition_keycol() -
 *   return: Error code
 *   keycol(out):
 *   class(in):
 *
 * Note:
 */
int
do_get_partition_keycol (char *keycol, MOP class_)
{
  int error = NO_ERROR;
  SM_CLASS *smclass;
  DB_VALUE pattr, keyname;
  int au_save;
  char *keyname_str;

  if (!class_ || !keycol)
    {
      return -1;
    }
  error = au_fetch_class (class_, &smclass, AU_FETCH_READ, AU_SELECT);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (!smclass->partition_of)
    {
      error = ER_INVALID_PARTITION_REQUEST;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }

  db_make_null (&keyname);
  db_make_null (&pattr);

  AU_DISABLE (au_save);
  error = db_get (smclass->partition_of, PARTITION_ATT_PVALUES, &pattr);
  if (error != NO_ERROR)
    {
      goto fail_end;
    }
  error = set_get_element (pattr.data.set, 0, &keyname);
  if (error != NO_ERROR)
    {
      goto fail_end;
    }

  if (DB_IS_NULL (&keyname))
    {
      goto fail_end;
    }
  keyname_str = DB_PULL_STRING (&keyname);
  strncpy (keycol, keyname_str, DB_MAX_IDENTIFIER_LENGTH);
  error = NO_ERROR;

fail_end:
  pr_clear_value (&keyname);
  pr_clear_value (&pattr);
  AU_ENABLE (au_save);
  return error;
}

/*
 * do_get_partition_keycol() -
 *   return: Error code
 *   class(in):
 *   name_list(in):
 *
 * Note:
 */
int
do_drop_partition_list (MOP class_, PT_NODE * name_list)
{
  PT_NODE *names;
  int error = NO_ERROR;
  char subclass_name[DB_MAX_IDENTIFIER_LENGTH];
  SM_CLASS *smclass, *subclass;
  int au_save;
  MOP delpart, classcata;

  if (!class_ || !name_list)
    {
      return -1;
    }

  error = au_fetch_class (class_, &smclass, AU_FETCH_READ, AU_SELECT);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (!smclass->partition_of)
    {
      error = ER_INVALID_PARTITION_REQUEST;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }

  for (names = name_list; names; names = names->next)
    {
      sprintf (subclass_name, "%s" PARTITIONED_SUB_CLASS_TAG "%s",
	       smclass->header.name, names->info.name.original);
      classcata = sm_find_class (subclass_name);
      if (classcata == NULL)
	{
	  return er_errid ();
	}

      error = au_fetch_class (classcata, &subclass, AU_FETCH_READ, AU_SELECT);
      if (error != NO_ERROR)
	{
	  return error;
	}
      if (subclass->partition_of)
	{
	  delpart = subclass->partition_of;
	  error = sm_delete_class_mop (classcata);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	  AU_DISABLE (au_save);
	  error = obj_delete (delpart);
	  if (error != NO_ERROR)
	    {
	      AU_ENABLE (au_save);
	      return error;
	    }
	  AU_ENABLE (au_save);
	}
      else
	{
	  error = ER_PARTITION_NOT_EXIST;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  return error;
	}
    }

  adjust_partition_range (smclass->users);
  adjust_partition_size (class_);
  return NO_ERROR;
}

/*
 * validate_attribute_domain() - This checks an attribute to make sure
 *                                  that it makes sense
 *   return: Error code
 *   parser(in): Parser context
 *   attribute(in): Parse tree of an attribute or method
 *   check_zero_precision(in): Do not permit zero precision if true
 *
 * Note: Error reporting system
 */
static int
validate_attribute_domain (PARSER_CONTEXT * parser,
			   PT_NODE * attribute,
			   const bool check_zero_precision)
{
  int error = NO_ERROR;

  if (attribute == NULL)
    {
      pt_record_error (parser,
		       parser->statement_number,
		       __LINE__, 0, "system error - NULL attribute node");
    }
  else
    {
      if (attribute->type_enum == PT_TYPE_NONE)
	{
	  pt_record_error (parser,
			   parser->statement_number,
			   attribute->line_number,
			   attribute->column_number,
			   "system error - attribute type not set");
	}
      else
	{
	  PT_NODE *dtyp = attribute->data_type;

	  if (dtyp)
	    {
	      int p = attribute->data_type->info.data_type.precision;
	      int s = attribute->data_type->info.data_type.dec_precision;

	      switch (attribute->type_enum)
		{
		case PT_TYPE_FLOAT:
		case PT_TYPE_DOUBLE:
		  if (p != DB_DEFAULT_PRECISION
		      && (p < 0 || p > DB_MAX_NUMERIC_PRECISION))
		    {
		      PT_ERRORmf3 (parser, attribute,
				   MSGCAT_SET_PARSER_SEMANTIC,
				   MSGCAT_SEMANTIC_INV_PREC, p, 0,
				   DB_MAX_NUMERIC_PRECISION);
		    }
		  break;

		case PT_TYPE_NUMERIC:
		  if (p != DB_DEFAULT_PRECISION
		      && (p < 0 || (p == 0 && check_zero_precision)
			  || p > DB_MAX_NUMERIC_PRECISION))
		    {
		      PT_ERRORmf3 (parser, attribute,
				   MSGCAT_SET_PARSER_SEMANTIC,
				   MSGCAT_SEMANTIC_INV_PREC, p, 0,
				   DB_MAX_NUMERIC_PRECISION);
		    }
		  break;

		case PT_TYPE_BIT:
		  if (p != DB_DEFAULT_PRECISION
		      && (p < 0 || (p == 0 && check_zero_precision)
			  || p > DB_MAX_BIT_PRECISION))
		    {
		      PT_ERRORmf3 (parser, attribute,
				   MSGCAT_SET_PARSER_SEMANTIC,
				   MSGCAT_SEMANTIC_INV_PREC, p, 0,
				   DB_MAX_BIT_PRECISION);
		    }
		  break;

		case PT_TYPE_VARBIT:
		  if (p != DB_DEFAULT_PRECISION
		      && (p < 0 || (p == 0 && check_zero_precision)
			  || p > DB_MAX_VARBIT_PRECISION))
		    {
		      PT_ERRORmf3 (parser, attribute,
				   MSGCAT_SET_PARSER_SEMANTIC,
				   MSGCAT_SEMANTIC_INV_PREC, p, 0,
				   DB_MAX_VARBIT_PRECISION);
		    }
		  break;

		case PT_TYPE_CHAR:
		  if (p != DB_DEFAULT_PRECISION
		      && (p < 0 || (p == 0 && check_zero_precision)
			  || p > DB_MAX_CHAR_PRECISION))
		    {
		      PT_ERRORmf3 (parser, attribute,
				   MSGCAT_SET_PARSER_SEMANTIC,
				   MSGCAT_SEMANTIC_INV_PREC, p, 0,
				   DB_MAX_CHAR_PRECISION);
		    }
		  break;

		case PT_TYPE_NCHAR:
		  if (p != DB_DEFAULT_PRECISION
		      && (p < 0 || (p == 0 && check_zero_precision)
			  || p > DB_MAX_NCHAR_PRECISION))
		    {
		      PT_ERRORmf3 (parser, attribute,
				   MSGCAT_SET_PARSER_SEMANTIC,
				   MSGCAT_SEMANTIC_INV_PREC, p, 0,
				   DB_MAX_NCHAR_PRECISION);
		    }
		  break;

		case PT_TYPE_VARCHAR:
		  if (p != DB_DEFAULT_PRECISION
		      && (p < 0 || (p == 0 && check_zero_precision)
			  || p > DB_MAX_VARCHAR_PRECISION))
		    {
		      PT_ERRORmf3 (parser, attribute,
				   MSGCAT_SET_PARSER_SEMANTIC,
				   MSGCAT_SEMANTIC_INV_PREC, p, 0,
				   DB_MAX_VARCHAR_PRECISION);
		    }
		  break;

		case PT_TYPE_VARNCHAR:
		  if (p != DB_DEFAULT_PRECISION
		      && (p < 0 || (p == 0 && check_zero_precision)
			  || p > DB_MAX_VARNCHAR_PRECISION))
		    {
		      PT_ERRORmf3 (parser, attribute,
				   MSGCAT_SET_PARSER_SEMANTIC,
				   MSGCAT_SEMANTIC_INV_PREC, p, 0,
				   DB_MAX_VARNCHAR_PRECISION);
		    }
		  break;

		default:
		  break;
		}
	    }
	}
    }

  if (error == NO_ERROR)
    {
      if (pt_has_error (parser))
	{
	  error = ER_PT_SEMANTIC;
	}
    }

  return error;

}

static const char *
get_attr_name (PT_NODE * attribute)
{
  /* First try the derived name and then the original name. For example:
     create view a_view as select a av1, a av2, b bv from a_tbl;
   */
  return attribute->info.attr_def.attr_name->alias_print
    ? attribute->info.attr_def.attr_name->alias_print
    : attribute->info.attr_def.attr_name->info.name.original;
}

/*
 * do_add_attribute() - Adds an attribute to a class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   attribute(in/out): Attribute to add
 *   error_on_not_normal(in): whether to flag an error on class and shared
 *                            attributes or not
 *
 * Note : The class object is modified
 */
static int
do_add_attribute (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
		  PT_NODE * attribute, bool error_on_not_normal)
{
  const char *attr_name;
  int meta, shared;
  DB_VALUE *default_value = NULL;
  DB_VALUE stack_value;
  int error = NO_ERROR;
  PT_TYPE_ENUM desired_type;
  PT_NODE *def_val;
  DB_DOMAIN *attr_db_domain;
  MOP auto_increment_obj = NULL;
  SM_ATTRIBUTE *att;
  SM_NAME_SPACE name_space = ID_NULL;
  bool add_first = false;
  const char *add_after_attr = NULL;
  PT_NODE *const ordering_info = attribute->info.attr_def.ordering_info;

  attr_name = get_attr_name (attribute);

  meta = (attribute->info.attr_def.attr_type == PT_META_ATTR);
  shared = (attribute->info.attr_def.attr_type == PT_SHARED);

  if (error_on_not_normal && attribute->info.attr_def.attr_type != PT_NORMAL)
    {
      ERROR1 (error, ER_SM_ONLY_NORMAL_ATTRIBUTES, attr_name);
      return error;
    }

  if (validate_attribute_domain (parser, attribute,
				 smt_get_class_type (ctemplate) ==
				 SM_CLASS_CT ? true : false))
    {
      /* validate_attribute_domain() is assumed to issue whatever
         messages are pertinent. */
      return ER_GENERIC_ERROR;
    }

  if (attribute->info.attr_def.data_default == NULL)
    {
      default_value = NULL;
    }
  else
    {
      desired_type = attribute->type_enum;

      /* try to coerce the default value into the attribute's type */
      def_val =
	attribute->info.attr_def.data_default->info.data_default.
	default_value;
      def_val = pt_semantic_check (parser, def_val);
      if (pt_has_error (parser) || def_val == NULL)
	{
	  pt_report_to_ersys (parser, PT_SEMANTIC);
	  return er_errid ();
	}

      error = pt_coerce_value (parser, def_val, def_val, desired_type,
			       attribute->data_type);
      if (error != NO_ERROR)
	{
	  return error;
	}

      default_value = &stack_value;
      pt_evaluate_tree (parser, def_val, default_value);
      if (pt_has_error (parser))
	{
	  pt_report_to_ersys (parser, PT_SEMANTIC);
	  return er_errid ();
	}
    }

  attr_db_domain = pt_node_to_db_domain (parser, attribute, ctemplate->name);
  if (attr_db_domain == NULL)
    {
      return (er_errid ());
    }

  if (ordering_info != NULL)
    {
      assert (ordering_info->node_type == PT_ATTR_ORDERING);

      add_first = ordering_info->info.attr_ordering.first;

      if (ordering_info->info.attr_ordering.after != NULL)
	{
	  PT_NODE *const after_name = ordering_info->info.attr_ordering.after;

	  assert (after_name->node_type == PT_NAME);
	  add_after_attr = after_name->info.name.original;
	  assert (add_first == false);
	}
      else
	{
	  add_after_attr = NULL;
	  /*
	   * If we have no "AFTER name" then this must have been a "FIRST"
	   * token
	   */
	  assert (add_first == true);
	}
    }
  else
    {
      add_first = false;
      add_after_attr = NULL;
    }

  if (meta)
    {
      name_space = ID_CLASS_ATTRIBUTE;
    }
  else if (shared)
    {
      name_space = ID_SHARED_ATTRIBUTE;
    }
  else
    {
      name_space = ID_ATTRIBUTE;
    }

  error = smt_add_attribute_w_dflt_w_order (ctemplate, attr_name, NULL,
					    attr_db_domain, default_value,
					    name_space, add_first,
					    add_after_attr);

  if (default_value)
    {
      db_value_clear (default_value);
    }

  /* Does the attribute belong to a NON_NULL constraint? */
  if (error == NO_ERROR)
    {
      if (attribute->info.attr_def.constrain_not_null)
	{
	  error = dbt_constrain_non_null (ctemplate, attr_name, meta, 1);
	}
    }

  /* create & set auto_increment attribute's serial object */
  if (error == NO_ERROR && !meta && !shared)
    {
      if (attribute->info.attr_def.auto_increment)
	{
	  if (db_Enable_replications <= 0)
	    {
	      error = do_create_auto_increment_serial (parser,
						       &auto_increment_obj,
						       ctemplate->name,
						       attribute);
	    }
	  if (error == NO_ERROR)
	    {
	      if (smt_find_attribute (ctemplate, attr_name, 0, &att)
		  == NO_ERROR)
		{
		  att->auto_increment = auto_increment_obj;
		  att->flags |= SM_ATTFLAG_AUTO_INCREMENT;
		}
	    }
	}
    }
  return error;
}

/*
 * do_add_attribute_from_select_column() - Adds an attribute to a class object
 *   return: Error code
 *   ctemplate(in/out): Class template
 *   column(in): Attribute to add, as specified by a SELECT column in a
 *               CREATE ... AS SELECT statement. The original SELECT column's
 *               NOT NULL and default value need to be copied.
 *
 * Note : The class object is modified
 */
static int
do_add_attribute_from_select_column (DB_CTMPL * ctemplate,
				     DB_QUERY_TYPE * column)
{
  DB_VALUE default_value;
  int error = NO_ERROR;
  const char *attr_name;
  MOP class_obj = NULL;

  DB_MAKE_NULL (&default_value);

  if (column == NULL || column->domain == NULL)
    {
      error = ER_FAILED;
      goto error_exit;
    }

  if (column->domain->type->id == DB_TYPE_NULL)
    {
      error = ER_CREATE_AS_SELECT_NULL_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto error_exit;
    }

  attr_name = db_query_format_name (column);

  if (column->spec_name != NULL)
    {
      class_obj = sm_find_class (column->spec_name);
      if (class_obj == NULL)
	{
	  goto error_exit;
	}

      error = sm_att_default_value (class_obj, column->attr_name,
				    &default_value);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  error = smt_add_attribute_w_dflt (ctemplate, attr_name, NULL,
				    column->domain, &default_value,
				    ID_ATTRIBUTE);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  if (class_obj != NULL)
    {
      if (sm_att_constrained (class_obj, column->attr_name,
			      SM_ATTFLAG_NON_NULL))
	{
	  error = dbt_constrain_non_null (ctemplate, attr_name, 0, 1);
	}
    }

  return error;

error_exit:
  db_value_clear (&default_value);
  return error;
}

static DB_QUERY_TYPE *
query_get_column_with_name (DB_QUERY_TYPE * query_columns, const char *name)
{
  DB_QUERY_TYPE *column;
  char real_name[SM_MAX_IDENTIFIER_LENGTH];
  char column_name[SM_MAX_IDENTIFIER_LENGTH];

  if (query_columns == NULL)
    {
      return NULL;
    }

  sm_downcase_name (name, real_name, SM_MAX_IDENTIFIER_LENGTH);
  for (column = query_columns;
       column != NULL; column = db_query_format_next (column))
    {
      sm_downcase_name (db_query_format_name (column), column_name,
			SM_MAX_IDENTIFIER_LENGTH);
      if (intl_mbs_casecmp (real_name, column_name) == 0)
	{
	  return column;
	}
    }
  return NULL;
}

static PT_NODE *
get_attribute_with_name (PT_NODE * atts, const char *name)
{
  PT_NODE *crt_attr;
  char real_name[SM_MAX_IDENTIFIER_LENGTH];
  char attribute_name[SM_MAX_IDENTIFIER_LENGTH];

  if (atts == NULL)
    {
      return NULL;
    }

  sm_downcase_name (name, real_name, SM_MAX_IDENTIFIER_LENGTH);
  for (crt_attr = atts; crt_attr != NULL; crt_attr = crt_attr->next)
    {
      sm_downcase_name (get_attr_name (crt_attr), attribute_name,
			SM_MAX_IDENTIFIER_LENGTH);
      if (intl_mbs_casecmp (real_name, attribute_name) == 0)
	{
	  return crt_attr;
	}
    }
  return NULL;
}

/*
 * do_add_attributes() - Adds attributes to a class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   atts(in/out): Attributes to add
 *   create_select_columns(in): the column list of a select for
 *                              CREATE ... AS SELECT statements
 *
 * Note : The class object is modified
 */
int
do_add_attributes (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
		   PT_NODE * atts, DB_QUERY_TYPE * create_select_columns)
{
  PT_NODE *crt_attr;
  DB_QUERY_TYPE *column;
  int error = NO_ERROR;

  crt_attr = atts;
  while (crt_attr)
    {
      const char *const attr_name = get_attr_name (crt_attr);
      if (query_get_column_with_name (create_select_columns, attr_name) ==
	  NULL)
	{
	  error = do_add_attribute (parser, ctemplate, crt_attr, false);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
      crt_attr = crt_attr->next;
    }

  for (column = create_select_columns;
       column != NULL; column = db_query_format_next (column))
    {
      const char *const col_name = db_query_format_name (column);
      crt_attr = get_attribute_with_name (atts, col_name);
      if (crt_attr != NULL)
	{
	  error = do_add_attribute (parser, ctemplate, crt_attr, true);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
      else
	{
	  error = do_add_attribute_from_select_column (ctemplate, column);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
    }

  return error;
}


/*
 * map_pt_to_sm_action() -
 *   return: SM_FOREIGN_KEY_ACTION
 *   action(in): Action to map
 *
 * Note:
 */
static SM_FOREIGN_KEY_ACTION
map_pt_to_sm_action (const PT_MISC_TYPE action)
{
  switch (action)
    {
    case PT_RULE_CASCADE:
      return SM_FOREIGN_KEY_CASCADE;

    case PT_RULE_RESTRICT:
      return SM_FOREIGN_KEY_RESTRICT;

    case PT_RULE_NO_ACTION:
      return SM_FOREIGN_KEY_NO_ACTION;

    case PT_RULE_SET_NULL:
      return SM_FOREIGN_KEY_SET_NULL;

    default:
      break;
    }

  return SM_FOREIGN_KEY_NO_ACTION;
}

/*
 * add_foreign_key() -
 *   return: Error code
 *   ctemplate(in/out): Class template
 *   cnstr(in): Constraint name
 *   att_names(in): Key attribute names
 *
 * Note:
 */
static int
add_foreign_key (DB_CTMPL * ctemplate, const PT_NODE * cnstr,
		 const char **att_names)
{
  PT_FOREIGN_KEY_INFO *fk_info;
  const char *constraint_name = NULL;
  char **ref_attrs = NULL;
  int i, n_atts, n_ref_atts;
  PT_NODE *p;
  int error = NO_ERROR;
  const char *cache_attr = NULL;

  fk_info = (PT_FOREIGN_KEY_INFO *) & (cnstr->info.constraint.un.foreign_key);

  n_atts = pt_length_of_list (fk_info->attrs);
  i = 0;
  for (p = fk_info->attrs; p; p = p->next)
    {
      att_names[i++] = p->info.name.original;
    }
  att_names[i] = NULL;

  if (fk_info->referenced_attrs != NULL)
    {
      n_ref_atts = pt_length_of_list (fk_info->referenced_attrs);

      ref_attrs = (char **) malloc ((n_ref_atts + 1) * sizeof (char *));
      if (ref_attrs == NULL)
	{
	  return er_errid ();
	}

      i = 0;
      for (p = fk_info->referenced_attrs; p; p = p->next)
	{
	  ref_attrs[i++] = (char *) p->info.name.original;
	}
      ref_attrs[i] = NULL;
    }

  /* Get the constraint name (if supplied) */
  if (cnstr->info.constraint.name)
    {
      constraint_name = cnstr->info.constraint.name->info.name.original;
    }

  if (fk_info->cache_attr)
    {
      cache_attr = fk_info->cache_attr->info.name.original;
    }

  error = dbt_add_foreign_key (ctemplate, constraint_name, att_names,
			       fk_info->referenced_class->info.name.original,
			       (const char **) ref_attrs,
			       map_pt_to_sm_action (fk_info->delete_action),
			       map_pt_to_sm_action (fk_info->update_action),
			       cache_attr);
  free_and_init (ref_attrs);
  return error;
}

/*
 * add_foreign_key_objcache_attr() -
 *   return: Error code
 *   ctemplate(in/out): Class template
 *   constraint(in): Constraints in the class
 *
 * Note:
 */
int
do_add_foreign_key_objcache_attr (DB_CTMPL * ctemplate, PT_NODE * constraints)
{
  PT_NODE *cnstr;
  PT_FOREIGN_KEY_INFO *fk_info;
  SM_ATTRIBUTE *cache_attr;
  int error;
  MOP ref_clsop;
  char *ref_cls_name;

  for (cnstr = constraints; cnstr != NULL; cnstr = cnstr->next)
    {
      if (cnstr->info.constraint.type != PT_CONSTRAIN_FOREIGN_KEY)
	{
	  continue;
	}

      fk_info = &(cnstr->info.constraint.un.foreign_key);
      ref_cls_name = (char *) fk_info->referenced_class->info.name.original;

      if (fk_info->cache_attr)
	{
	  error = smt_find_attribute (ctemplate,
				      fk_info->cache_attr->info.name.original,
				      false, &cache_attr);

	  if (error == NO_ERROR)
	    {
	      ref_clsop = sm_find_class (ref_cls_name);

	      if (ref_clsop == NULL)
		{
		  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			  ER_FK_UNKNOWN_REF_CLASSNAME, 1, ref_cls_name);
		  return er_errid ();
		}

	      if (cache_attr->type->id != DB_TYPE_OBJECT
		  || !OID_EQ (&cache_attr->domain->class_oid,
			      ws_oid (ref_clsop)))
		{
		  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			  ER_SM_INVALID_NAME, 1, ref_cls_name);
		}
	    }
	  else if (error == ER_SM_INVALID_NAME)
	    {
	      return error;
	    }
	  else
	    {
	      er_clear ();

	      if (smt_add_attribute
		  (ctemplate, fk_info->cache_attr->info.name.original,
		   ref_cls_name, NULL) != NO_ERROR)
		{
		  return er_errid ();
		}
	    }
	}
    }

  return NO_ERROR;
}

/*
 * do_add_constraints() - This extern routine adds constraints
 *			  to a class object.
 *   return: Error code
 *   ctemplate(in/out): Class template
 *   constraints(in): Constraints to add
 *
 * Note : Class object is modified
 */
int
do_add_constraints (DB_CTMPL * ctemplate, PT_NODE * constraints)
{
  int error = NO_ERROR;
  PT_NODE *cnstr;
  int max_attrs = 0;
  char **att_names = NULL;

  /*  Find the size of the largest UNIQUE constraint list and allocate
     a character array large enough to contain it. */
  for (cnstr = constraints; cnstr != NULL; cnstr = cnstr->next)
    {
      if (cnstr->info.constraint.type == PT_CONSTRAIN_UNIQUE)
	{
	  max_attrs = MAX (max_attrs,
			   pt_length_of_list (cnstr->info.constraint.un.
					      unique.attrs));
	}
      if (cnstr->info.constraint.type == PT_CONSTRAIN_PRIMARY_KEY)
	{
	  max_attrs = MAX (max_attrs,
			   pt_length_of_list (cnstr->info.constraint.un.
					      primary_key.attrs));
	}
      if (cnstr->info.constraint.type == PT_CONSTRAIN_FOREIGN_KEY)
	{
	  max_attrs = MAX (max_attrs,
			   pt_length_of_list (cnstr->info.constraint.un.
					      foreign_key.attrs));
	}
    }

  if (max_attrs > 0)
    {
      att_names = (char **) malloc ((max_attrs + 1) * sizeof (char *));

      if (att_names == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  for (cnstr = constraints; cnstr != NULL; cnstr = cnstr->next)
	    {
	      if (cnstr->info.constraint.type == PT_CONSTRAIN_UNIQUE)
		{
		  PT_NODE *p;
		  int i, n_atts;
		  int class_attributes = 0;
		  char *constraint_name = NULL;
		  DB_CONSTRAINT_TYPE constraint_type = DB_CONSTRAINT_UNIQUE;
		  int *asc_desc = NULL;

		  asc_desc = (int *) malloc ((max_attrs) * sizeof (int));
		  if (asc_desc == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1,
			      (max_attrs) * sizeof (int));
		      error = ER_OUT_OF_VIRTUAL_MEMORY;
		      goto constraint_error;
		    }

		  n_atts =
		    pt_length_of_list (cnstr->info.constraint.un.unique.
				       attrs);
		  if (PT_NAME_INFO_IS_FLAGED
		      (cnstr->info.constraint.un.unique.attrs,
		       PT_NAME_INFO_DESC))
		    {
		      constraint_type = DB_CONSTRAINT_REVERSE_UNIQUE;
		    }

		  i = 0;
		  for (p = cnstr->info.constraint.un.unique.attrs; p;
		       p = p->next)
		    {
		      asc_desc[i] =
			PT_NAME_INFO_IS_FLAGED (p, PT_NAME_INFO_DESC) ? 1 : 0;
		      att_names[i++] = (char *) p->info.name.original;

		      /* Determine if the unique constraint is being applied
		         to class or normal attributes.  The way the parser
		         currently works, all multi-column constraints will be
		         on normal attributes and it's therefore impossible
		         for a constraint to contain both class and normal
		         attributes. */
		      if (p->info.name.meta_class == PT_META_ATTR)
			{
			  class_attributes = 1;
			}

		      /* We keep DB_CONSTRAINT_REVERSE_UNIQUE only if all
		         columns are marked as DESC. */
		      if (!PT_NAME_INFO_IS_FLAGED (p, PT_NAME_INFO_DESC))
			{
			  constraint_type = DB_CONSTRAINT_UNIQUE;
			}
		    }
		  att_names[i] = NULL;

		  /* Get the constraint name (if supplied) */
		  if (cnstr->info.constraint.name)
		    {
		      constraint_name =
			cnstr->info.constraint.name->info.name.original;
		    }

		  constraint_name =
		    sm_produce_constraint_name_tmpl (ctemplate,
						     constraint_type,
						     (const char **)
						     att_names, asc_desc,
						     constraint_name);
		  error =
		    smt_add_constraint (ctemplate, constraint_type,
					constraint_name,
					(const char **) att_names,
					(constraint_type ==
					 DB_CONSTRAINT_UNIQUE) ? asc_desc :
					NULL, class_attributes, NULL);

		  sm_free_constraint_name (constraint_name);
		  free_and_init (asc_desc);
		  if (error != NO_ERROR)
		    {
		      goto constraint_error;
		    }
		}

	      if (cnstr->info.constraint.type == PT_CONSTRAIN_PRIMARY_KEY)
		{
		  PT_NODE *p;
		  int i, n_atts;
		  int class_attributes = 0;
		  char *constraint_name = NULL;

		  n_atts =
		    pt_length_of_list (cnstr->info.constraint.un.primary_key.
				       attrs);
		  i = 0;
		  for (p = cnstr->info.constraint.un.primary_key.attrs; p;
		       p = p->next)
		    {
		      att_names[i++] = (char *) p->info.name.original;

		      /* Determine if the unique constraint is being applied
		         to class or normal attributes.  The way the parser
		         currently works, all multi-column constraints will be
		         on normal attributes and it's therefore impossible
		         for a constraint to contain both class and normal
		         attributes. */
		      if (p->info.name.meta_class == PT_META_ATTR)
			{
			  class_attributes = 1;
			}
		    }
		  att_names[i] = NULL;

		  /* Get the constraint name (if supplied) */
		  if (cnstr->info.constraint.name)
		    {
		      constraint_name =
			cnstr->info.constraint.name->info.name.original;
		    }

		  error = dbt_add_constraint (ctemplate,
					      DB_CONSTRAINT_PRIMARY_KEY,
					      constraint_name,
					      (const char **) att_names,
					      class_attributes);
		  if (error != NO_ERROR)
		    {
		      goto constraint_error;
		    }
		}

	      if (cnstr->info.constraint.type == PT_CONSTRAIN_FOREIGN_KEY)
		{
		  error = add_foreign_key (ctemplate, cnstr,
					   (const char **) att_names);
		  if (error != NO_ERROR)
		    {
		      goto constraint_error;
		    }
		}
	    }

	  free_and_init (att_names);
	}
    }

  return (error);

/* error handler */
constraint_error:
  if (att_names)
    {
      free_and_init (att_names);
    }
  return (error);
}

/*
 * do_check_fk_constraints() - Checks that foreign key constraints are
 *                             consistent with the schema.
 * The routine only works when a new class is created or when it is altered
 * with a single change; it might not work in the future if a class will be
 * altered with multiple changes in a single call.
 *
 * Currently the following checks are performed:
 *   SET NULL referential actions must not contradict the attributes' domains
 *   (the attributes cannot have a NOT NULL constraint as they cannot be
 *   NULL).
 *
 *   SET NULL actions are not yet supported on partitioned tables.
 *
 * In the future the function should also check for foreign keys that have
 * cascading referential actions and either represent cycles in the schema or
 * represent "race" updates (the same attribute can be affected on two
 * separate cascading action paths; the results are undefined).
 *
 *   return: Error code
 *   ctemplate(in/out): Class template
 *   constraints(in): List of all the class constraints that have been added.
 *                    Currently the function does not support checking for
 *                    consistency when NOT NULL constraints are added.
 *
 * Note : The class object is not modified
 */
int
do_check_fk_constraints (DB_CTMPL * ctemplate, PT_NODE * constraints)
{
  int error = NO_ERROR;
  PT_NODE *cnstr;

  for (cnstr = constraints; cnstr != NULL; cnstr = cnstr->next)
    {
      PT_NODE *attr;
      PT_FOREIGN_KEY_INFO *fk_info;

      if (cnstr->info.constraint.type != PT_CONSTRAIN_FOREIGN_KEY)
	{
	  continue;
	}

      fk_info =
	(PT_FOREIGN_KEY_INFO *) & cnstr->info.constraint.un.foreign_key;
      if (ctemplate->partition_of != NULL
	  && (fk_info->delete_action == PT_RULE_SET_NULL
	      || fk_info->update_action == PT_RULE_SET_NULL))
	{
	  PT_NODE *const constr_name = cnstr->info.constraint.name;
	  ERROR2 (error, ER_FK_CANT_ON_PARTITION,
		  constr_name ? constr_name->info.name.original : "",
		  ctemplate->name);
	  goto error_exit;
	}

      for (attr = fk_info->attrs; attr; attr = attr->next)
	{
	  const char *const att_name = attr->info.name.original;
	  SM_ATTRIBUTE *attp = NULL;

	  error = smt_find_attribute (ctemplate, att_name, 0, &attp);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }

	  /* FK cannot be defined on shared attribute. */
	  if (db_attribute_is_shared (attp))
	    {
	      PT_NODE *const constr_name = cnstr->info.constraint.name;
	      ERROR2 (error, ER_FK_CANT_ON_SHARED_ATTRIBUTE,
		      constr_name ? constr_name->info.name.original : "",
		      att_name);
	      goto error_exit;
	    }
	  if ((fk_info->delete_action == PT_RULE_SET_NULL ||
	       fk_info->update_action == PT_RULE_SET_NULL) &&
	      db_attribute_is_non_null (attp))
	    {
	      PT_NODE *const constr_name = cnstr->info.constraint.name;
	      ERROR2 (error, ER_FK_MUST_NOT_BE_NOT_NULL,
		      constr_name ? constr_name->info.name.original : "",
		      att_name);
	      goto error_exit;
	    }
	}
    }

  if (ctemplate->current != NULL)
    {
      SM_CLASS_CONSTRAINT *c;

      for (c = ctemplate->current->constraints; c != NULL; c = c->next)
	{
	  SM_ATTRIBUTE **attribute_p = NULL;

	  if (c->type != SM_CONSTRAINT_FOREIGN_KEY)
	    {
	      continue;
	    }
	  if (ctemplate->partition_of != NULL &&
	      (c->fk_info->delete_action == SM_FOREIGN_KEY_SET_NULL ||
	       c->fk_info->update_action == SM_FOREIGN_KEY_SET_NULL))
	    {
	      ERROR2 (error, ER_FK_CANT_ON_PARTITION, c->name ? c->name : "",
		      ctemplate->name);
	      goto error_exit;
	    }
	  if (c->fk_info->delete_action != SM_FOREIGN_KEY_SET_NULL &&
	      c->fk_info->update_action != SM_FOREIGN_KEY_SET_NULL)
	    {
	      continue;
	    }
	  for (attribute_p = c->attributes; *attribute_p; ++attribute_p)
	    {
	      const char *const att_name = (*attribute_p)->header.name;
	      SM_ATTRIBUTE *attp = NULL;

	      smt_find_attribute (ctemplate, att_name, 0, &attp);
	      if (db_attribute_is_non_null (attp))
		{
		  ERROR2 (error, ER_FK_MUST_NOT_BE_NOT_NULL,
			  c->name ? c->name : "", att_name);
		  goto error_exit;
		}
	    }
	}
    }
  return error;

error_exit:
  return error;
}


/*
 * do_add_methods() - Adds methods to a class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   methods(in): Methods to add
 *
 * Note : Class object is modified
 */
int
do_add_methods (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
		PT_NODE * methods)
{
  const char *method_name, *method_impl;
  PT_NODE *args_list, *type, *type_list;
  PT_NODE *data_type;
  int arg_num;
  int is_meta;
  int error = NO_ERROR;
  DB_DOMAIN *arg_db_domain;

  /* add each method listed in the class definition */

  while (methods && (error == NO_ERROR))
    {
      method_name = methods->info.method_def.method_name->info.name.original;

      if (methods->info.method_def.function_name != NULL)
	{
	  method_impl =
	    methods->info.method_def.function_name->info.name.original;
	}
      else
	{
	  method_impl = NULL;
	}

      if (methods->info.method_def.mthd_type == PT_META_ATTR)
	{
	  error = dbt_add_class_method (ctemplate, method_name, method_impl);
	}
      else
	{
	  error = dbt_add_method (ctemplate, method_name, method_impl);
	}
      if (error != NO_ERROR)
	{
	  return error;
	}

      /* if the result of the method is declared, then add it! */

      arg_num = 0;
      is_meta = methods->info.method_def.mthd_type == PT_META_ATTR;

      if (methods->type_enum != PT_TYPE_NONE)
	{
	  if (PT_IS_COLLECTION_TYPE (methods->type_enum))
	    {
	      arg_db_domain = pt_node_to_db_domain (parser, methods,
						    ctemplate->name);
	      if (arg_db_domain == NULL)
		{
		  return (er_errid ());
		}

	      error = smt_assign_argument_domain (ctemplate, method_name,
						  is_meta, NULL, arg_num,
						  NULL, arg_db_domain);
	      if (error != NO_ERROR)
		{
		  return error;
		}

	      type_list = methods->data_type;
	      for (type = type_list; type != NULL; type = type->next)
		{
		  arg_db_domain = pt_data_type_to_db_domain (parser, type,
							     ctemplate->name);
		  if (arg_db_domain == NULL)
		    {
		      return (er_errid ());
		    }

		  error = smt_add_set_argument_domain (ctemplate,
						       method_name,
						       is_meta, NULL,
						       arg_num, NULL,
						       arg_db_domain);
		  if (error != NO_ERROR)
		    {
		      return error;
		    }
		}
	    }
	  else
	    {
	      if (validate_attribute_domain (parser, methods, false))
		{
		  return ER_GENERIC_ERROR;
		}
	      arg_db_domain = pt_node_to_db_domain (parser, methods,
						    ctemplate->name);
	      if (arg_db_domain == NULL)
		{
		  return (er_errid ());
		}

	      error = smt_assign_argument_domain (ctemplate, method_name,
						  is_meta, NULL, arg_num,
						  NULL, arg_db_domain);
	      if (error != NO_ERROR)
		{
		  return error;
		}
	    }
	}

      /* add each argument of the method that is declared. */

      args_list = methods->info.method_def.method_args_list;
      for (data_type = args_list; data_type != NULL;
	   data_type = data_type->next)
	{
	  arg_num++;

	  if (PT_IS_COLLECTION_TYPE (data_type->type_enum))
	    {
	      arg_db_domain = pt_data_type_to_db_domain (parser, data_type,
							 ctemplate->name);
	      if (arg_db_domain == NULL)
		{
		  return (er_errid ());
		}

	      error = smt_assign_argument_domain (ctemplate, method_name,
						  is_meta, NULL, arg_num,
						  NULL, arg_db_domain);
	      if (error != NO_ERROR)
		{
		  return error;
		}

	      type_list = data_type->data_type;
	      for (type = type_list; type != NULL; type = type->next)
		{
		  arg_db_domain = pt_data_type_to_db_domain (parser, type,
							     ctemplate->name);
		  if (arg_db_domain == NULL)
		    {
		      return (er_errid ());
		    }

		  error = smt_add_set_argument_domain (ctemplate,
						       method_name,
						       is_meta, NULL,
						       arg_num, NULL,
						       arg_db_domain);
		  if (error != NO_ERROR)
		    {
		      return error;
		    }
		}
	    }
	  else
	    {
	      if (validate_attribute_domain (parser, data_type, false))
		{
		  return ER_GENERIC_ERROR;
		}
	      arg_db_domain = pt_node_to_db_domain (parser, data_type,
						    ctemplate->name);
	      if (arg_db_domain == NULL)
		{
		  return (er_errid ());
		}

	      error = smt_assign_argument_domain (ctemplate, method_name,
						  is_meta, NULL, arg_num,
						  NULL, arg_db_domain);
	      if (error != NO_ERROR)
		{
		  return error;
		}
	    }
	}

      methods = methods->next;
    }
  return (error);
}

/*
 * do_add_method_files() - Adds method files to a class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   method_files(in): Method files to add
 *
 * Note : Class object is modified
 */
int
do_add_method_files (const PARSER_CONTEXT * parser,
		     DB_CTMPL * ctemplate, PT_NODE * method_files)
{
  const char *method_file_name;
  int error = NO_ERROR;
  PT_NODE *path, *mf;

  /* add each method_file listed in the class definition */

  for (mf = method_files; mf && error == NO_ERROR; mf = mf->next)
    {
      if (mf->node_type == PT_FILE_PATH
	  && (path = mf->info.file_path.string) != NULL
	  && path->node_type == PT_VALUE
	  && (path->type_enum == PT_TYPE_VARCHAR
	      || path->type_enum == PT_TYPE_CHAR
	      || path->type_enum == PT_TYPE_NCHAR
	      || path->type_enum == PT_TYPE_VARNCHAR))
	{
	  method_file_name = (char *) path->info.value.data_value.str->bytes;
	  error = dbt_add_method_file (ctemplate, method_file_name);
	}
      else
	{
	  break;
	}
    }

  return (error);
}

/*
 * do_add_supers() - Adds super-classes to a class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   supers(in): Superclasses to add
 *
 * Note : Class object is modified
 */
int
do_add_supers (const PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
	       const PT_NODE * supers)
{
  MOP super_class;
  int error = NO_ERROR;


  /* Add each superclass listed in the class definition.
     Each superclass must already exist inthe database before
     it can be added. */

  while (supers && (error == NO_ERROR))
    {
      super_class = db_find_class (supers->info.name.original);
      if (super_class == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = dbt_add_super (ctemplate, super_class);
	}

      supers = supers->next;
    }

  return (error);
}

/*
 * do_add_resolutions() - Adds resolutions to a class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   supers(in): Resolution to add
 *
 * Note : Class object is modified
 */
int
do_add_resolutions (const PARSER_CONTEXT * parser,
		    DB_CTMPL * ctemplate, const PT_NODE * resolution)
{
  int error = NO_ERROR;
  DB_OBJECT *resolution_super_mop;
  const char *resolution_attr_mthd_name, *resolution_as_attr_mthd_name;

  /* add each conflict resolution listed in the
     class definition */

  while (resolution && (error == NO_ERROR))
    {
      resolution_super_mop =
	db_find_class (resolution->info.resolution.of_sup_class_name->info.
		       name.original);

      if (resolution_super_mop == NULL)
	{
	  error = er_errid ();
	  break;
	}

      resolution_attr_mthd_name =
	resolution->info.resolution.attr_mthd_name->info.name.original;
      if (resolution->info.resolution.as_attr_mthd_name == NULL)
	{
	  resolution_as_attr_mthd_name = NULL;
	}
      else
	{
	  resolution_as_attr_mthd_name =
	    resolution->info.resolution.as_attr_mthd_name->info.name.original;
	}

      if (resolution->info.resolution.attr_type == PT_META_ATTR)
	{
	  error = dbt_add_class_resolution (ctemplate, resolution_super_mop,
					    resolution_attr_mthd_name,
					    resolution_as_attr_mthd_name);
	}
      else
	{
	  error = dbt_add_resolution (ctemplate, resolution_super_mop,
				      resolution_attr_mthd_name,
				      resolution_as_attr_mthd_name);
	}

      resolution = resolution->next;
    }

  return (error);
}

/*
 * add_query_to_virtual_class() - Adds a query to a virtual class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   supers(in): Queries to add
 *
 * Note : Class object is modified
 */
static int
add_query_to_virtual_class (PARSER_CONTEXT * parser,
			    DB_CTMPL * ctemplate, const PT_NODE * queries)
{
  const char *query;
  int error = NO_ERROR;

  query = parser_print_tree (parser, queries);
  error = dbt_add_query_spec (ctemplate, query);

  return (error);
}

/*
 * add_union_query() - Adds a query to a virtual class
 *			  object. If the query is a union all query, it is
 * 			  divided into its component queries
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   supers(in): Union queries to add
 *
 * Note : class object modified
 */
static int
add_union_query (PARSER_CONTEXT * parser,
		 DB_CTMPL * ctemplate, const PT_NODE * query)
{
  int error = NO_ERROR;

  /* Add each query listed in the virtual class definition. */

  if (query->node_type == PT_UNION
      && query->info.query.all_distinct == PT_ALL)
    {
      error = add_union_query
	(parser, ctemplate, query->info.query.q.union_.arg1);

      if (error == NO_ERROR)
	{
	  error = add_union_query
	    (parser, ctemplate, query->info.query.q.union_.arg2);
	}
    }
  else
    {
      error = add_query_to_virtual_class (parser, ctemplate, query);
    }

  return (error);
}

/*
 * do_add_queries() - Adds a list of queries to a virtual class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   queries(in): Queries to add
 *
 * Note : Class object is modified
 */
int
do_add_queries (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
		const PT_NODE * queries)
{
  int error = NO_ERROR;

  while (queries && (error == NO_ERROR))
    {
      error = add_union_query (parser, ctemplate, queries);

      queries = queries->next;
    }

  return (error);
}

/*
 * do_set_object_id() - Sets the object_id for a class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   queries(in): Object ids to set
 *
 * Note : Class object is modified
 */
int
do_set_object_id (const PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
		  PT_NODE * object_id_list)
{
  int error = NO_ERROR;
  PT_NODE *object_id;
  int total_ids = 0;
  DB_NAMELIST *id_list = NULL;
  const char *att_name;

  object_id = object_id_list;
  while (object_id)
    {
      att_name = object_id->info.name.original;
      if (att_name)
	{
	  (void) db_namelist_append (&id_list, att_name);
	}
      ++total_ids;
      object_id = object_id->next;
    }
  if (total_ids == 0)
    {
      if (id_list)
	{
	  db_namelist_free (id_list);
	}
      return (error);
    }

  error = dbt_set_object_id (ctemplate, id_list);
  db_namelist_free (id_list);

  return (error);
}

/*
 * do_create_local() - Creates a new class or vclass
 *   return: Error code if the class/vclass is not created
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   pt_node(in): Parse tree of a create class/vclass
 */
int
do_create_local (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
		 PT_NODE * pt_node, DB_QUERY_TYPE * create_select_columns)
{
  int error = NO_ERROR;

  /* create a MOP for the ctemplate, extracting its name
     from the parse tree. */

  if (pt_node->info.create_entity.global_entity == PT_GLOBAL
      && pt_node->info.create_entity.entity_type == PT_CLASS
      && pt_node->info.create_entity.partition_info == NULL)
    {
      /* This is a global table with non-partition, it should have node_name. */
      assert (pt_node->info.create_entity.on_node->info.value.text);
      ctemplate->node_name =
	ws_copy_string (pt_node->info.create_entity.on_node->info.value.text);
      if (ctemplate->node_name == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  return er_errid ();
	}
    }

  if (pt_node->info.create_entity.global_entity == PT_GLOBAL)
    {
      ctemplate->is_global = true;
    }

  error = do_add_attributes (parser, ctemplate,
			     pt_node->info.create_entity.attr_def_list,
			     create_select_columns);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_add_attributes (parser, ctemplate,
			     pt_node->info.create_entity.class_attr_def_list,
			     NULL);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_add_constraints (ctemplate,
			      pt_node->info.create_entity.constraint_list);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_check_fk_constraints (ctemplate,
				   pt_node->info.create_entity.
				   constraint_list);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_add_methods (parser, ctemplate,
			  pt_node->info.create_entity.method_def_list);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_add_method_files (parser, ctemplate,
			       pt_node->info.create_entity.method_file_list);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_add_resolutions (parser, ctemplate,
			      pt_node->info.create_entity.resolution_list);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_add_supers (parser, ctemplate,
			 pt_node->info.create_entity.supclass_list);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_add_queries (parser, ctemplate,
			  pt_node->info.create_entity.as_query_list);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_set_object_id (parser, ctemplate,
			    pt_node->info.create_entity.object_id_list);
  if (error != NO_ERROR)
    {
      return error;
    }

  return (error);
}

/*
 * create_select_to_insert_into() - An "INSERT INTO ... SELECT" statement is
 *                                  built from a simple SELECT statement to be
 *                                  used for "CREATE ... AS SELECT" execution
 *   return: The INSERT statement or NULL on error
 *   parser(in): Parser context
 *   class_name(in): the name of the table created by the CREATE statement
 *   create_select(in): the select statement parse tree
 *   query_columns(in): the columns of the select statement
 */
static PT_NODE *
create_select_to_insert_into (PARSER_CONTEXT * parser, const char *class_name,
			      PT_NODE * create_select,
			      PT_CREATE_SELECT_ACTION create_select_action,
			      DB_QUERY_TYPE * query_columns)
{
  PT_NODE *ins = NULL;
  PT_NODE *ocs = NULL;
  PT_NODE *nls = NULL;
  DB_QUERY_TYPE *column = NULL;
  char real_name[SM_MAX_IDENTIFIER_LENGTH] = { 0 };
  PT_NODE *name = NULL;

  /* TODO The generated nodes have incorrect line and column information. */
  ins = parser_new_node (parser, PT_INSERT);
  if (ins == NULL)
    {
      goto error_exit;
    }

  if (create_select_action == PT_CREATE_SELECT_REPLACE)
    {
      ins->info.insert.do_replace = true;
    }
  else
    {
      /* PT_CREATE_SELECT_IGNORE is not yet implemented */
      assert (create_select_action == PT_CREATE_SELECT_NO_ACTION);
    }

  ins->info.insert.spec = ocs = parser_new_node (parser, PT_SPEC);
  if (ocs == NULL)
    {
      goto error_exit;
    }

  ocs->info.spec.only_all = PT_ONLY;
  ocs->info.spec.meta_class = PT_CLASS;
  ocs->info.spec.entity_name = pt_name (parser, class_name);
  if (ocs->info.spec.entity_name == NULL)
    {
      goto error_exit;
    }

  for (column = query_columns;
       column != NULL; column = db_query_format_next (column))
    {
      sm_downcase_name (db_query_format_name (column), real_name,
			SM_MAX_IDENTIFIER_LENGTH);

      name = pt_name (parser, real_name);
      if (name == NULL)
	{
	  goto error_exit;
	}
      ins->info.insert.attr_list =
	parser_append_node (name, ins->info.insert.attr_list);
    }

  ins->info.insert.value_clauses = nls = pt_node_list (parser, PT_IS_SUBQUERY,
						       create_select);
  if (nls == NULL)
    {
      goto error_exit;
    }

  return ins;

error_exit:
  parser_free_tree (parser, ins);
  return NULL;
}

/*
 * execute_create_select_query() - Executes an "INSERT INTO ... SELECT"
 *                                 statement built from a SELECT statement to
 *                                 be used for "CREATE ... AS SELECT"
 *                                 execution
 *   return: NO_ERROR on success, non-zero for ERROR
 *   parser(in): Parser context
 *   class_name(in): the name of the table created by the CREATE statement
 *   create_select(in): the select statement parse tree
 *   query_columns(in): the columns of the select statement
 *   flagged_statement(in): a node to copy the special statement flags from;
 *                          flags such as recompile will be used for the
 *                          INSERT statement
 */
static int
execute_create_select_query (PARSER_CONTEXT * parser,
			     const char *const class_name,
			     PT_NODE * create_select,
			     PT_CREATE_SELECT_ACTION create_select_action,
			     DB_QUERY_TYPE * query_columns,
			     PT_NODE * flagged_statement)
{
  PT_NODE *insert_into = NULL;
  PT_NODE *create_select_copy = parser_copy_tree (parser, create_select);
  int error = NO_ERROR;

  if (create_select_copy == NULL)
    {
      error = ER_FAILED;
      goto error_exit;
    }
  insert_into = create_select_to_insert_into (parser, class_name,
					      create_select_copy,
					      create_select_action,
					      query_columns);
  if (insert_into == NULL)
    {
      error = er_errid ();
      goto error_exit;
    }
  pt_copy_statement_flags (flagged_statement, insert_into);
  create_select_copy = NULL;

  insert_into = pt_compile (parser, insert_into);
  if (!insert_into || pt_has_error (parser))
    {
      pt_report_to_ersys_with_statement (parser, PT_SEMANTIC, insert_into);
      error = er_errid ();
      goto error_exit;
    }

  insert_into = mq_translate (parser, insert_into);
  if (!insert_into || pt_has_error (parser))
    {
      pt_report_to_ersys_with_statement (parser, PT_SEMANTIC, insert_into);
      error = er_errid ();
      goto error_exit;
    }

  error = do_statement (parser, insert_into);
  if (insert_into->xasl_id)
    {
      free_and_init (insert_into->xasl_id);
    }
  if (error < 0)
    {
      goto error_exit;
    }
  else
    {
      error = 0;
    }

  parser_free_tree (parser, insert_into);
  insert_into = NULL;

  return error;

error_exit:
  if (create_select_copy != NULL)
    {
      parser_free_tree (parser, create_select_copy);
      create_select_copy = NULL;
    }
  if (insert_into != NULL)
    {
      parser_free_tree (parser, insert_into);
      insert_into = NULL;
    }
  return error;
}

/*
 * do_create_entity() - Creates a new class/vclass
 *   return: Error code if the class/vclass is not created
 *   parser(in): Parser context
 *   node(in/out): Parse tree of a create class/vclass
 */
int
do_create_entity (PARSER_CONTEXT * parser, PT_NODE * node)
{
  int error = NO_ERROR;
  DB_CTMPL *ctemplate = NULL;
  DB_OBJECT *class_obj = NULL;
  const char *class_name = NULL;
  const char *create_like = NULL;
  SM_CLASS *class_ = NULL;
  PT_NODE *create_select = NULL;
  PT_NODE *create_index = NULL;
  DB_QUERY_TYPE *query_columns = NULL;
  bool do_rollback_on_error = false;
  bool do_abort_class_on_error = false;
  bool is_global_class = false;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      error = ER_AU_AUTHORIZATION_FAILURE;
      goto error_exit;
    }

  class_name = node->info.create_entity.entity_name->info.name.original;

  if (node->info.create_entity.create_like != NULL)
    {
      create_like = node->info.create_entity.create_like->info.name.original;
    }

  create_select = node->info.create_entity.create_select;
  if (create_select != NULL)
    {
      error = pt_get_select_query_columns (parser, create_select,
					   &query_columns);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }
  assert (!(create_like != NULL && create_select != NULL));

  if (node->info.create_entity.global_entity == PT_GLOBAL)
    {
      is_global_class = true;
      if (create_like != NULL || create_select != NULL
	  || !db_is_in_cluster ())
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_GENERIC_FUNCTION_FAILURE, 0);
	  error = er_errid ();
	  goto error_exit;
	}
    }

  switch (node->info.create_entity.entity_type)
    {
    case PT_CLASS:
      if (node->info.create_entity.partition_info != NULL
	  || create_like != NULL || create_select != NULL
	  || node->info.create_entity.create_index != NULL)
	{
	  error = tran_savepoint (UNIQUE_SAVEPOINT_CREATE_ENTITY, false);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	  do_rollback_on_error = true;
	}
      if (create_like)
	{
	  ctemplate = dbt_copy_class (class_name, create_like, &class_);
	}
      else
	{
	  ctemplate = dbt_create_class (class_name);
	}
      break;

    case PT_VCLASS:
      if (node->info.create_entity.or_replace && db_find_class (class_name))
	{
	  /* drop existing view */
	  if (do_is_partitioned_subclass (NULL, class_name, NULL))
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_INVALID_PARTITION_REQUEST, 0);
	      error = er_errid ();
	      goto error_exit;
	    }

	  error = tran_savepoint (UNIQUE_SAVEPOINT_CREATE_ENTITY, false);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	  do_rollback_on_error = true;

	  error = drop_class_name (class_name);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	}

      ctemplate = dbt_create_vclass (class_name);
      break;

    default:
      error = ER_GENERIC_ERROR;	/* a system error */
      break;
    }

  if (ctemplate == NULL)
    {
      if (error == NO_ERROR)
	{
	  error = er_errid ();
	}
      goto error_exit;
    }
  do_abort_class_on_error = true;

  if (create_like != NULL)
    {
      /* Nothing left to do, we already have the template filled in. */
    }
  else
    {
      error = do_create_local (parser, ctemplate, node, query_columns);
    }

  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  class_obj = dbt_finish_class (ctemplate);

  if (class_obj == NULL)
    {
      error = er_errid ();
      goto error_exit;
    }
  do_abort_class_on_error = false;
  ctemplate = NULL;

  switch (node->info.create_entity.entity_type)
    {
    case PT_VCLASS:
      if (node->info.create_entity.with_check_option == PT_CASCADED_CHECK_OPT)
	error =
	  sm_set_class_flag (class_obj, SM_CLASSFLAG_WITHCHECKOPTION, 1);
      else if (node->info.create_entity.with_check_option ==
	       PT_LOCAL_CHECK_OPT)
	{
	  error =
	    sm_set_class_flag (class_obj, SM_CLASSFLAG_LOCALCHECKOPTION, 1);
	}
      break;
    case PT_CLASS:
      {
	PT_NODE *tbl_opt = NULL;
	bool reuse_oid = false;

	if (!is_global_class)
	  {
	    /* For global class, following operation has been executed. */
	    for (tbl_opt = node->info.create_entity.table_option_list;
		 tbl_opt != NULL; tbl_opt = tbl_opt->next)
	      {
		assert (tbl_opt->node_type == PT_TABLE_OPTION);
		if (tbl_opt->info.table_option.option
		    == PT_TABLE_OPTION_REUSE_OID)
		  {
		    reuse_oid = true;
		  }
	      }
	    if (locator_create_heap_if_needed (class_obj, reuse_oid) == NULL)
	      {
		error = er_errid ();
		break;
	      }
	    if (reuse_oid)
	      {
		error =
		  sm_set_class_flag (class_obj, SM_CLASSFLAG_REUSE_OID, 1);
	      }
	  }
	break;
      }

    default:
      break;
    }

  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  if (node->info.create_entity.partition_info != NULL)
    {
      error = do_create_partition (parser, node, class_obj, NULL);
      if (error != NO_ERROR)
	{
	  if (error == ER_LK_UNILATERALLY_ABORTED)
	    {
	      do_rollback_on_error = false;
	    }
	  goto error_exit;
	}
    }

  if (create_like)
    {
      error = do_copy_indexes (class_obj, class_);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  if (create_select)
    {
      if (db_Enable_replications <= 0)
	{
	  error = do_replicate_schema (parser, node);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }

	  error = execute_create_select_query (parser, class_name,
					       create_select,
					       node->info.create_entity.
					       create_select_action,
					       query_columns, node);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	}

      db_free_query_format (query_columns);
      query_columns = NULL;
    }
  assert (query_columns == NULL);

  for (create_index = node->info.create_entity.create_index;
       create_index != NULL; create_index = create_index->next)
    {
      create_index->info.index.indexed_class =
	parser_copy_tree (parser, node->info.create_entity.entity_name);
      if (create_index->info.index.indexed_class == NULL)
	{
	  error = ER_FAILED;
	  goto error_exit;
	}
      error = do_create_index (parser, create_index);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  return error;

error_exit:
  if (query_columns != NULL)
    {
      db_free_query_format (query_columns);
      query_columns = NULL;
    }
  if (do_abort_class_on_error)
    {
      (void) dbt_abort_class (ctemplate);
    }
  if (do_rollback_on_error)
    {
      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_CREATE_ENTITY);
    }
  return error;
}


/*
 * do_copy_indexes() - Copies all the indexes of a given class to another
 *                     class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classmop(in): the class to copy the indexes to
 *   class_(in): the class to copy the indexes from
 */
static int
do_copy_indexes (MOP classmop, SM_CLASS * src_class)
{
  int error = NO_ERROR;
  const char **att_names = NULL;
  SM_CLASS_CONSTRAINT *c;
  char *auto_cons_name = NULL;
  char *new_cons_name = NULL;
  DB_CONSTRAINT_TYPE constraint_type;

  assert (src_class != NULL);

  if (src_class->constraints == NULL)
    {
      return NO_ERROR;
    }

  for (c = src_class->constraints; c; c = c->next)
    {
      if (c->type != SM_CONSTRAINT_INDEX &&
	  c->type != SM_CONSTRAINT_REVERSE_INDEX)
	{
	  /* These should have been copied already. */
	  continue;
	}

      att_names = classobj_point_at_att_names (c, NULL);
      if (att_names == NULL)
	{
	  return er_errid ();
	}

      constraint_type = db_constraint_type (c);
      auto_cons_name = sm_produce_constraint_name (src_class->header.name,
						   constraint_type, att_names,
						   c->asc_desc, NULL);

      /* check if constraint's name was generated automatically */
      if (auto_cons_name != NULL && strcmp (auto_cons_name, c->name) == 0)
	{
	  /* regenerate name automatically for new class */
	  new_cons_name = sm_produce_constraint_name_mop (classmop,
							  constraint_type,
							  att_names,
							  c->asc_desc, NULL);
	}
      else
	{
	  /* use name given by user */
	  new_cons_name = (char *) c->name;
	}

      if (auto_cons_name != NULL)
	{
	  sm_free_constraint_name (auto_cons_name);
	}

      error = sm_add_index (classmop, constraint_type, new_cons_name,
			    att_names, c->asc_desc, c->attrs_prefix_length);

      free_and_init (att_names);

      if (new_cons_name != NULL && new_cons_name != c->name)
	{
	  sm_free_constraint_name (new_cons_name);
	}

      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  return error;
}





/*
 * Function Group :
 * Code for truncating Classes by Parse Tree descriptions.
 *
 */

static int truncate_class_name (const char *name);

/*
 * truncate_class_name() - This static routine truncates a class by name.
 *   return: Error code
 *   name(in): Class name to truncate
 */
static int
truncate_class_name (const char *name)
{
  DB_OBJECT *class_mop;

  class_mop = db_find_class (name);

  if (class_mop)
    {
      return db_truncate_class (class_mop);
    }
  else
    {
      /* if class is null, return the global error. */
      return er_errid ();
    }
}

/*
 * do_truncate() - Truncates a class
 *   return: Error code if truncation fails.
 *   parser(in): Parser context
 *   statement(in/out): Parse tree of the statement
 */
int
do_truncate (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  int error = NO_ERROR;
  PT_NODE *entity_spec = NULL;
  PT_NODE *entity = NULL;
  PT_NODE *entity_list = NULL;

  CHECK_MODIFICATION_ERROR ();

  entity_spec = statement->info.truncate.spec;
  if (entity_spec == NULL)
    {
      return NO_ERROR;
    }

  entity_list = entity_spec->info.spec.flat_entity_list;
  for (entity = entity_list; entity != NULL; entity = entity->next)
    {
      /* partitioned sub-class check */
      if (do_is_partitioned_subclass (NULL, entity->info.name.original, NULL))
	{
	  error = ER_INVALID_PARTITION_REQUEST;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  return error;
	}
    }

  error = tran_savepoint (UNIQUE_SAVEPOINT_TRUNCATE, false);
  if (error != NO_ERROR)
    {
      return error;
    }

  for (entity = entity_list; entity != NULL; entity = entity->next)
    {
      error = truncate_class_name (entity->info.name.original);
      if (error != NO_ERROR)
	{
	  (void) tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_TRUNCATE);
	  return error;
	}
    }

  return error;
}

/*
* Function Group :
* DO functions for register node
*
*/

/*
 * do_register_node() - register a new node into cluster
 *   return: Error code
 *   parser(in): Parser context
 *   statement(in/out): Parse tree of the statement
 */
int
do_register_node (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  PT_NODE *node, *host, *port;
  char *node_name, *db_host;
  int cubrid_port = PRM_TCP_PORT_ID;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      return ER_AU_AUTHORIZATION_FAILURE;
    }

  if (statement == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  node = statement->info.register_node.on_node;
  host = statement->info.register_node.host_name;
  port = statement->info.register_node.port;

  if (!parser || node == NULL || host == NULL || !IS_STRING (node)
      || !IS_STRING (host))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  node_name = (char *) node->info.value.data_value.str->bytes;
  db_host = (char *) host->info.value.data_value.str->bytes;

  if (port != NULL)
    {
      if (!
	  (port->node_type == PT_VALUE && port->type_enum == PT_TYPE_INTEGER))
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return ER_OBJ_INVALID_ARGUMENTS;
	}
      cubrid_port = port->info.value.data_value.i;
    }
  return db_register_cluster_node (node_name, db_host, cubrid_port);
}

/*
 * do_unregister_node() - unregister an exist node from cluster
 *   return: Error code
 *   parser(in): Parser context
 *   statement(in/out): Parse tree of the statement
 */
int
do_unregister_node (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  PT_NODE *node;
  char *node_name;

  CHECK_MODIFICATION_ERROR ();

  if (PRM_BLOCK_DDL_STATEMENT)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_AUTHORIZATION_FAILURE,
	      0);
      return ER_AU_AUTHORIZATION_FAILURE;
    }

  if (statement == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  node = statement->info.register_node.on_node;
  if (!parser || !node || !IS_STRING (node))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  node_name = (char *) node->info.value.data_value.str->bytes;
  return db_unregister_cluster_node (node_name);
}

/*
 * do_schema_sync() - replicate global schema to another node
 *   return: Error code
 *   parser(in): Parser context
 *   statement(in/out): Parse tree of the statement
 */
int
do_schema_sync (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  PT_NODE *node;
  MOP node_obj;
  DB_OBJLIST *classes;
  unsigned int remote_node_id;
  char *node_name;
  int error_code = NO_ERROR;

  CHECK_MODIFICATION_ERROR ();

  node = statement->info.schema_sync.node_name;
  if (!parser || !node || !IS_STRING (node))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  if (!au_is_dba_group_member (Au_user))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_AU_DBA_ONLY, 1,
	      "schema_sync");
      return ER_AU_DBA_ONLY;
    }

  node_name = (char *) node->info.value.data_value.str->bytes;
  node_obj = ccf_find_node (node_name);
  if (node_obj == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_CCF_NOT_FIND_NODE, 0);
      return ER_CCF_NOT_FIND_NODE;
    }
  remote_node_id = ccf_get_node_ip (node_obj);

  sm_Do_schema_sync = true;

  classes = sm_fetch_all_classes (1, DB_FETCH_READ);
  for (; classes != NULL; classes = classes->next)
    {
      if (!sm_is_global_class (classes->op))
	{
	  continue;
	}

      error_code = sm_sync_global_schema (classes->op, remote_node_id);
      if (error_code != NO_ERROR)
	{
	  goto error_exit;
	}
    }
  ml_ext_free (classes);

error_exit:
  sm_Do_schema_sync = false;
  return error_code;
}
