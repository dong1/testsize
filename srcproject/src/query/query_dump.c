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
 * query_dump.c - Query processor printer
 */

#ident "$Id$"

#include "config.h"
#include <stdio.h>

#include "error_manager.h"
#include "object_representation.h"
#include "query_executor.h"
#include "class_object.h"

#define foutput stdout

enum
{
  ARITH_EXP = 0,
  LARITH_EXP = 1,
  AGG_EXP = 2,
  LAGG_EXP = 3
};

#define HASH_NUMBER 128

typedef struct qdump_xasl_check_node QDUMP_XASL_CHECK_NODE;
struct qdump_xasl_check_node
{
  QDUMP_XASL_CHECK_NODE *next;
  UINTPTR xasl_addr;
  PROC_TYPE xasl_type;
  int referenced;
  int reachable;
};

static bool qdump_print_xasl_type (XASL_NODE * xasl);
static bool qdump_print_db_value_array (DB_VALUE ** array, int cnt);
static bool qdump_print_column (const char *title_p, int col_count,
				int *column_p);
static bool qdump_print_list_merge_info (QFILE_LIST_MERGE_INFO * ptr);
static bool qdump_print_merge_list_proc_node (MERGELIST_PROC_NODE * ptr);
static bool qdump_print_update_proc_node (UPDATE_PROC_NODE * ptr);
static bool qdump_print_delete_proc_node (DELETE_PROC_NODE * ptr);
static bool qdump_print_insert_proc_node (INSERT_PROC_NODE * ptr);
static const char *qdump_target_type_string (TARGET_TYPE type);
static const char *qdump_access_method_string (ACCESS_METHOD access);
static bool qdump_print_access_spec (ACCESS_SPEC_TYPE * spec_list);
static const char *qdump_key_range_string (RANGE range);
static bool qdump_print_key_info (KEY_INFO * key_info);
static const char *qdump_range_type_string (RANGE_TYPE range_type);
static bool qdump_print_index (INDX_INFO * indexptr);
static bool qdump_print_index_id (INDX_ID id);
static bool qdump_print_btid (BTID id);
static bool qdump_print_class (CLS_SPEC_TYPE * ptr);
static bool qdump_print_hfid (HFID id);
static bool qdump_print_vfid (VFID id);
static bool qdump_print_list (LIST_SPEC_TYPE * ptr);
static bool qdump_print_outlist (const char *title, OUTPTR_LIST * outlist);
static bool qdump_print_listi_d (QFILE_LIST_ID * idptr);
static bool qdump_print_type_list (QFILE_TUPLE_VALUE_TYPE_LIST * typeptr);
static bool qdump_print_domain_list (int cnt, TP_DOMAIN ** ptr);
static bool qdump_print_sort_list (SORT_LIST * sorting_list);
static bool qdump_print_attribute_id (ATTR_DESCR attr);
static bool qdump_print_tuple_value_position (QFILE_TUPLE_VALUE_POSITION pos);
static bool qdump_print_value_list (VAL_LIST * vallist);
static bool qdump_print_regu_variable_list (REGU_VARIABLE_LIST varlist);
static const char *qdump_option_string (int option);
static bool qdump_print_db_value (DB_VALUE * value);
static const char *qdump_regu_type_string (REGU_DATATYPE type);
static bool qdump_print_regu_type (REGU_VARIABLE * value);
static const char *qdump_data_type_string (DB_TYPE type);
static bool qdump_print_value (REGU_VARIABLE * value);
static const char *qdump_function_type_string (FUNC_TYPE ftype);
static bool qdump_print_function_value (REGU_VARIABLE * regu);
static bool qdump_print_value_type_addr (REGU_VARIABLE * value);
static bool qdump_print_oid (OID * oidptr);
static bool qdump_print_predicate (PRED_EXPR * predptr);
static const char *qdump_relation_operator_string (int op);
static const char *qdump_arith_operator_string (OPERATOR_TYPE opcode);
static bool qdump_print_arith_expression (int type, ARITH_TYPE * arith_p);
static bool qdump_print_aggregate_expression (int type,
					      AGGREGATE_TYPE * aggptr);
static bool qdump_print_arith (int type, void *ptr);
static bool qdump_print_term (PRED_EXPR * pred_ptr);
static const char *qdump_bool_operator_string (BOOL_OP bool_op);
static bool qdump_print_lhs_predicate (PRED_EXPR * pred_p);
#if defined(CUBRID_DEBUG)
static QDUMP_XASL_CHECK_NODE *qdump_find_check_node_for (XASL_NODE * xasl,
							 QDUMP_XASL_CHECK_NODE
							 *
							 chk_nodes
							 [HASH_NUMBER]);
static void qdump_check_node (XASL_NODE * xasl,
			      QDUMP_XASL_CHECK_NODE * chk_nodes[HASH_NUMBER]);
static int qdump_print_inconsistencies (QDUMP_XASL_CHECK_NODE *
					chk_nodes[HASH_NUMBER]);
#endif /* CUBRID_DEBUG */

/*
 * qdump_print_xasl_type () -
 *   return:
 *   xasl(in):
 */
static bool
qdump_print_xasl_type (XASL_NODE * xasl_p)
{
  const char *type_string_p;

  switch (xasl_p->type)
    {
    case BUILDLIST_PROC:
      type_string_p = "buildlist_proc";
      break;
    case BUILDVALUE_PROC:
      type_string_p = "buildvalue_proc";
      break;
    case UNION_PROC:
      type_string_p = "union_proc";
      break;
    case DIFFERENCE_PROC:
      type_string_p = "difference_proc";
      break;
    case INTERSECTION_PROC:
      type_string_p = "intersection_proc";
      break;
    case OBJFETCH_PROC:
      type_string_p = "objfetch_proc";
      break;
    case SETFETCH_PROC:
      type_string_p = "setfetch_proc";
      break;
    case SCAN_PROC:
      type_string_p = "scan_proc";
      break;
    case MERGELIST_PROC:
      type_string_p = "mergelist_proc";
      break;
    case UPDATE_PROC:
      type_string_p = "update_proc";
      break;
    case DELETE_PROC:
      type_string_p = "delete_proc";
      break;
    case INSERT_PROC:
      type_string_p = "insert_proc";
      break;
    case CONNECTBY_PROC:
      type_string_p = "connectby_proc";
      break;
    default:
      return false;
    }

  fprintf (foutput, "[%s:%p]\n", type_string_p, xasl_p);
  return true;
}

/*
 * qdump_print_db_value_array () -
 *   return:
 *   array(in)  :
 *   int cnt(in):
 */
static bool
qdump_print_db_value_array (DB_VALUE ** array_p, int count)
{
  int i;

  if (array_p == NULL)
    {
      return true;
    }

  for (i = 0; i < count; i++, array_p++)
    {
      if (!qdump_print_db_value (*array_p))
	{
	  return false;
	}
      fprintf (foutput, "; ");
    }

  return true;
}

static bool
qdump_print_column (const char *title_p, int col_count, int *column_p)
{
  int i;

  fprintf (foutput, "[%s:", title_p);

  for (i = 0; i < col_count; i++)
    {
      fprintf (foutput, "%s%d", (i ? "|" : ""), *(column_p + i));
    }

  fprintf (foutput, "]");
  return true;
}

/*
 * qdump_print_list_merge_info () -
 *   return:
 *   ptr(in):
 */
static bool
qdump_print_list_merge_info (QFILE_LIST_MERGE_INFO * merge_info_p)
{
  if (merge_info_p == NULL)
    {
      return true;
    }

  fprintf (foutput, "[join type:%d]", merge_info_p->join_type);
  fprintf (foutput, "[single fetch:%d]\n", merge_info_p->single_fetch);

  qdump_print_column ("outer column position", merge_info_p->ls_column_cnt,
		      merge_info_p->ls_outer_column);
  qdump_print_column ("outer column is unique", merge_info_p->ls_column_cnt,
		      merge_info_p->ls_outer_unique);
  qdump_print_column ("inner column position", merge_info_p->ls_column_cnt,
		      merge_info_p->ls_inner_column);
  qdump_print_column ("inner column is unique", merge_info_p->ls_column_cnt,
		      merge_info_p->ls_inner_unique);

  fprintf (foutput, "\n[output column count:%d]", merge_info_p->ls_pos_cnt);

  qdump_print_column ("output columns", merge_info_p->ls_pos_cnt,
		      merge_info_p->ls_pos_list);
  qdump_print_column ("outer/inner indicators", merge_info_p->ls_pos_cnt,
		      merge_info_p->ls_outer_inner_list);

  return true;
}

/*
 * qdump_print_merge_list_proc_node () -
 *   return:
 *   ptr(in):
 */
static bool
qdump_print_merge_list_proc_node (MERGELIST_PROC_NODE * node_p)
{
  fprintf (foutput, "[outer xasl:%p]\n", node_p->outer_xasl);
  if (node_p->outer_spec_list)
    {
      fprintf (foutput, "-->outer access spec:");
      qdump_print_access_spec (node_p->outer_spec_list);
      fprintf (foutput, "\n");
    }

  if (node_p->outer_val_list)
    {
      fprintf (foutput, "-->outer val_list:");
      qdump_print_value_list (node_p->outer_val_list);
      fprintf (foutput, "\n");
    }

  fprintf (foutput, "[inner xasl:%p]\n", node_p->inner_xasl);

  if (node_p->inner_spec_list)
    {
      fprintf (foutput, "-->inner access spec:");
      qdump_print_access_spec (node_p->inner_spec_list);
      fprintf (foutput, "\n");
    }

  if (node_p->inner_val_list)
    {
      fprintf (foutput, "-->inner val_list:");
      qdump_print_value_list (node_p->inner_val_list);
      fprintf (foutput, "\n");
    }

  qdump_print_list_merge_info (&node_p->ls_merge);
  fprintf (foutput, "\n");

  return true;
}

static bool
qdump_print_attribute (const char *action_p, int attr_count, int *attr_ids_p)
{
  int i;

  fprintf (foutput, "[number of attributes to %s:%d]", action_p, attr_count);
  fprintf (foutput, "[ID's of attributes for %s:", action_p);

  for (i = 0; i < attr_count; i++)
    {
      fprintf (foutput, "%d%c", attr_ids_p[i],
	       i == attr_count - 1 ? ']' : ',');
    }

  return true;
}

/*
 * qdump_print_update_proc_node () -
 *   return:
 *   ptr(in):
 */
static bool
qdump_print_update_proc_node (UPDATE_PROC_NODE * node_p)
{
  int i;

  fprintf (foutput, "[number of HFID's to use:%d]", node_p->no_classes);

  for (i = 0; i < node_p->no_classes; ++i)
    {
      qdump_print_oid (&node_p->class_oid[i]);
    }

  for (i = 0; i < node_p->no_classes; ++i)
    {
      qdump_print_hfid (node_p->class_hfid[i]);
    }

  for (i = 0; i < node_p->no_classes; ++i)
    {
      fprintf (foutput, "node id:%d", node_p->node_ids[i]);
    }

  qdump_print_attribute ("update", node_p->no_vals, node_p->att_id);

  fprintf (foutput, "[number of constant values:%d]", node_p->no_consts);
  fprintf (foutput, "[constant values:");
  qdump_print_db_value_array (node_p->consts, node_p->no_consts);
  fprintf (foutput, "]");

  return true;
}

/*
 * qdump_print_delete_proc_node () -
 *   return:
 *   ptr(in):
 */
static bool
qdump_print_delete_proc_node (DELETE_PROC_NODE * node_p)
{
  int i;

  fprintf (foutput, "[number of HFID's to use:%d]", node_p->no_classes);

  for (i = 0; i < node_p->no_classes; ++i)
    {
      qdump_print_oid (&node_p->class_oid[i]);
    }

  for (i = 0; i < node_p->no_classes; ++i)
    {
      qdump_print_hfid (node_p->class_hfid[i]);
    }

  for (i = 0; i < node_p->no_classes; ++i)
    {
      fprintf (foutput, "node id:%d", node_p->node_ids[i]);
    }

  return true;
}

/*
 * qdump_print_insert_proc_node () -
 *   return:
 *   ptr(in):
 */
static bool
qdump_print_insert_proc_node (INSERT_PROC_NODE * node_p)
{
  fprintf (foutput, "class oid[%d %d %d]",
	   node_p->class_oid.pageid, node_p->class_oid.slotid,
	   node_p->class_oid.volid);

  qdump_print_hfid (node_p->class_hfid);
  qdump_print_attribute ("insert", node_p->no_vals, node_p->att_id);

  return true;
}

/*
 * qdump_target_type_string () -
 *   return:
 *   type(in):
 */
static const char *
qdump_target_type_string (TARGET_TYPE type)
{
  switch (type)
    {
    case TARGET_CLASS:
      return "class";
    case TARGET_CLASS_ATTR:
      return "class_attr";
    case TARGET_LIST:
      return "list";
    case TARGET_SET:
      return "set";
    case TARGET_METHOD:
      return "method";
    default:
      return "undefined";
    }
}

/*
 * qdump_access_method_string () -
 *   return:
 *   access(in):
 */
static const char *
qdump_access_method_string (ACCESS_METHOD access)
{
  switch (access)
    {
    case SEQUENTIAL:
      return "sequential";
    case INDEX:
      return "index";
    default:
      return "undefined";
    }
}

/*
 * qdump_print_access_spec () -
 *   return:
 *   spec_list(in):
 */
static bool
qdump_print_access_spec (ACCESS_SPEC_TYPE * spec_list_p)
{
  TARGET_TYPE type;

  if (spec_list_p == NULL)
    {
      return true;
    }

  type = spec_list_p->type;
  fprintf (foutput, " %s", qdump_target_type_string (type));

  fprintf (foutput, ",%s", qdump_access_method_string (spec_list_p->access));

  if (spec_list_p->access == INDEX)
    {
      if (qdump_print_index (spec_list_p->indexptr) == false)
	{
	  return false;
	}
    }

  fprintf (foutput, "\n	");

  if (type == TARGET_CLASS)
    {
      qdump_print_class (&ACCESS_SPEC_CLS_SPEC (spec_list_p));
    }
  else if (type == TARGET_SET)
    {
      qdump_print_value (ACCESS_SPEC_SET_PTR (spec_list_p));
    }
  else if (type == TARGET_LIST)
    {
      qdump_print_list (&ACCESS_SPEC_LIST_SPEC (spec_list_p));
    }

  if (spec_list_p->where_key)
    {
      fprintf (foutput, "\n      key filter:");
      qdump_print_predicate (spec_list_p->where_key);
    }

  if (spec_list_p->where_pred)
    {
      fprintf (foutput, "\n      access pred:");
      qdump_print_predicate (spec_list_p->where_pred);
    }

  fprintf (foutput, "\n  grouped scan=%d", spec_list_p->grouped_scan);
  fprintf (foutput, ",fixed scan=%d", spec_list_p->fixed_scan);
  fprintf (foutput, ",qualified block=%d", spec_list_p->qualified_block);
  fprintf (foutput, ",single fetch=%d", spec_list_p->single_fetch);

  if (spec_list_p->s_dbval)
    {
      fprintf (foutput, "\n      s_dbval:");
      qdump_print_db_value (spec_list_p->s_dbval);
    }

  fprintf (foutput, "\n-->next access spec:");
  qdump_print_access_spec (spec_list_p->next);
  fprintf (foutput, "\n");

  return true;
}

static const char *
qdump_key_range_string (RANGE range)
{
  switch (range)
    {
    case NA_NA:
      return "N/A";
    case GE_LE:
      return "GE_LE";
    case GE_LT:
      return "GE_LT";
    case GT_LE:
      return "GT_LE";
    case GT_LT:
      return "GT_LT";
    case GE_INF:
      return "GE_INF";
    case GT_INF:
      return "GT_INF";
    case INF_LT:
      return "INF_LT";
    case INF_LE:
      return "INF_LE";
    case INF_INF:
      return "INF_INF";
    case EQ_NA:
      return "EQ";
    default:
      return "undefined";
    }
}

/*
 * qdump_print_key_info () -
 *   return:
 *   key_info(in):
 */
static bool
qdump_print_key_info (KEY_INFO * key_info_p)
{
  int i;

  fprintf (foutput, "<key cnt:%d>", key_info_p->key_cnt);
  fprintf (foutput, "key ranges:");
  for (i = 0; i < key_info_p->key_cnt; i++)
    {
      fprintf (foutput, "<%s>",
	       qdump_key_range_string (key_info_p->key_ranges[i].range));

      fprintf (foutput, "[");
      if (!qdump_print_value (key_info_p->key_ranges[i].key1))
	{
	  return false;
	}

      fprintf (foutput, "][");

      if (!qdump_print_value (key_info_p->key_ranges[i].key2))
	{
	  return false;
	}
      fprintf (foutput, "]");
    }
  fprintf (foutput, "<is constant:%d>", key_info_p->is_constant);

  return true;
}

static const char *
qdump_range_type_string (RANGE_TYPE range_type)
{
  switch (range_type)
    {
    case R_KEY:
      return "R_KEY";
    case R_RANGE:
      return "R_RANGE";
    case R_KEYLIST:
      return "R_KEYLIST";
    case R_RANGELIST:
      return "R_RANGELIST";
    default:
      return "undefined";
    }
}

/*
 * qdump_print_index () -
 *   return:
 *   index_ptr(in):
 */
static bool
qdump_print_index (INDX_INFO * index_p)
{
  if (index_p == NULL)
    {
      return true;
    }

  fprintf (foutput, "<index id:");
  if (!qdump_print_index_id (index_p->indx_id))
    {
      return false;
    }
  fprintf (foutput, ">");

  fprintf (foutput, "<%s>", qdump_range_type_string (index_p->range_type));

  fprintf (foutput, "key info:");
  if (!qdump_print_key_info (&index_p->key_info))
    {
      return false;
    }
  fprintf (foutput, ">");

  return true;
}

/*
 * qdump_print_index_id () -
 *   return:
 *   id(in):
 */
static bool
qdump_print_index_id (INDX_ID id)
{
  if (id.type == T_BTID)
    {
      fprintf (foutput, "<type: Btree>");
      fprintf (foutput, "(%d;%d)", id.i.btid.vfid.fileid,
	       id.i.btid.vfid.volid);
    }
  else
    {
      fprintf (foutput, "<type: Extendible Hashing>");
      fprintf (foutput, "<%d;%d;%d>", id.i.ehid.vfid.volid,
	       id.i.ehid.vfid.fileid, id.i.ehid.pageid);
    }

  return true;
}

/*
 * qdump_print_btid () -
 *   return:
 *   id(in):
 */
static bool
qdump_print_btid (BTID id)
{
  fprintf (foutput, "<Btree:(%d;%d;%d)>", id.vfid.fileid,
	   id.vfid.volid, id.root_pageid);
  return true;
}

/*
 * qdump_print_class () -
 *   return:
 *   ptr(in):
 */
static bool
qdump_print_class (CLS_SPEC_TYPE * class_p)
{
  qdump_print_hfid (class_p->hfid);
  fprintf (foutput, "oid[%d %d %d]",
	   class_p->cls_oid.pageid, class_p->cls_oid.slotid,
	   class_p->cls_oid.volid);
  fprintf (foutput, "\n	regu_list_key:");
  qdump_print_regu_variable_list (class_p->cls_regu_list_key);
  fprintf (foutput, "\n	regu_list_pred:");
  qdump_print_regu_variable_list (class_p->cls_regu_list_pred);
  fprintf (foutput, "\n	regu_list_rest:");
  qdump_print_regu_variable_list (class_p->cls_regu_list_rest);
  return true;
}

/*
 * qdump_print_hfid () -
 *   return:
 *   id(in):
 */
static bool
qdump_print_hfid (HFID id)
{
  fprintf (foutput, "hfid:");
  qdump_print_vfid (id.vfid);
  fprintf (foutput, ":%d", id.hpgid);
  return true;
}

/*
 * qdump_print_vfid () -
 *   return:
 *   id(in):
 */
static bool
qdump_print_vfid (VFID id)
{
  fprintf (foutput, "vfid(%d;%d)", id.fileid, id.volid);
  return true;
}

/*
 * qdump_print_list () -
 *   return:
 *   ptr(in):
 */
static bool
qdump_print_list (LIST_SPEC_TYPE * list_p)
{
  fprintf (foutput, "list=");
  fprintf (foutput, "xasl:%p", list_p->xasl_node);
  fprintf (foutput, "\n	regu_list_pred:");
  qdump_print_regu_variable_list (list_p->list_regu_list_pred);
  fprintf (foutput, "\n	regu_list_rest:");
  qdump_print_regu_variable_list (list_p->list_regu_list_rest);
  return true;
}

/*
 * qdump_print_outlist () -
 *   return:
 *   title(in):
 *   outlist(in):
 */
static bool
qdump_print_outlist (const char *title_p, OUTPTR_LIST * outlist_p)
{
  REGU_VARIABLE_LIST nextptr;

  if (outlist_p == NULL)
    {
      return true;
    }

  nextptr = outlist_p->valptrp;
  fprintf (foutput, "-->%s:", title_p);

  while (nextptr)
    {
      fprintf (foutput, "[addr:%p]", &nextptr->value);
      if (!qdump_print_value (&nextptr->value))
	{
	  return false;
	}

      fprintf (foutput, "; ");
      nextptr = nextptr->next;
    }

  fprintf (foutput, "\n");
  return true;
}

/*
 * qdump_print_listi_d () -
 *   return:
 *   idptr(in):
 */
static bool
qdump_print_listi_d (QFILE_LIST_ID * list_id_p)
{
  if (list_id_p == NULL)
    {
      return true;
    }

  fprintf (foutput, "(address:%p)", list_id_p);
  fprintf (foutput, "(type_list:");

  if (!qdump_print_type_list (&list_id_p->type_list))
    {
      return false;
    }

  fprintf (foutput, ")(tuple_cnt:%d)", list_id_p->tuple_cnt);
  return true;
}

/*
 * qdump_print_type_list () -
 *   return:
 *   typeptr(in):
 */
static bool
qdump_print_type_list (QFILE_TUPLE_VALUE_TYPE_LIST * type_list_p)
{
  fprintf (foutput, "<type_cnt:%d>", type_list_p->type_cnt);
  if (!qdump_print_domain_list (type_list_p->type_cnt, type_list_p->domp))
    {
      return false;
    }
  return true;
}

/*
 * qdump_print_domain_list () -
 *   return:
 *   cnt(in):
 *   ptr(in):
 */
static bool
qdump_print_domain_list (int cnt, TP_DOMAIN ** domains_p)
{
  int i;

  if (domains_p == NULL)
    {
      return true;
    }

  for (i = 0; i < cnt; i++)
    {
      fprintf (foutput, "%s; ",
	       qdump_data_type_string (domains_p[i]->type->id));
    }

  return true;
}

/*
 * qdump_print_sort_list () -
 *   return:
 *   sorting_list(in):
 */
static bool
qdump_print_sort_list (SORT_LIST * sort_list_p)
{
  if (sort_list_p == NULL)
    {
      return true;
    }

  fprintf (foutput, "<sorting field(POS):");
  if (!qdump_print_tuple_value_position (sort_list_p->pos_descr))
    {
      return false;
    }

  fprintf (foutput, ">");
  fprintf (foutput, "<sorting order:");
  if (sort_list_p->s_order == S_ASC)
    {
      fprintf (foutput, "ascending>");
    }
  else
    {
      fprintf (foutput, "descending>");
    }

  if (!qdump_print_sort_list (sort_list_p->next))
    {
      return false;
    }
  return true;
}

/*
 * qdump_print_attribute_id () -
 *   return:
 *   attr(in):
 */
static bool
qdump_print_attribute_id (ATTR_DESCR attr)
{
  fprintf (foutput, "attr_id:%d|db_type:", (int) attr.id);
  fprintf (foutput, "%s", qdump_data_type_string (attr.type));

  return true;
}

/*
 * qdump_print_tuple_value_position () -
 *   return:
 *   pos(in):
 */
static bool
qdump_print_tuple_value_position (QFILE_TUPLE_VALUE_POSITION pos)
{
  fprintf (foutput, "(position %d) (db_type ", pos.pos_no);
  fprintf (foutput, "%s", qdump_data_type_string (pos.dom->type->id));
  fprintf (foutput, ")");

  return true;
}

/*
 * qdump_print_value_list () -
 *   return:
 *   vallist(in):
 */
static bool
qdump_print_value_list (VAL_LIST * value_list_p)
{
  QPROC_DB_VALUE_LIST dbval_list;

  if (value_list_p == NULL)
    {
      return true;
    }

  dbval_list = value_list_p->valp;
  fprintf (foutput, "(values %d <", value_list_p->val_cnt);

  while (dbval_list != NULL)
    {
      fprintf (foutput, "addr:%p|", dbval_list->val);
      fprintf (foutput, "type:%s",
	       qdump_data_type_string (DB_VALUE_DOMAIN_TYPE
				       (dbval_list->val)));
      fprintf (foutput, "|value:");

      if (!qdump_print_db_value (dbval_list->val))
	{
	  return false;
	}

      fprintf (foutput, "; ");
      dbval_list = dbval_list->next;
    }

  fprintf (foutput, ">)");
  return true;
}

/*
 * qdump_print_regu_variable_list () -
 *   return:
 *   varlist(in):
 */
static bool
qdump_print_regu_variable_list (REGU_VARIABLE_LIST var_list)
{
  if (var_list == NULL)
    {
      return true;
    }

  while (var_list != NULL)
    {
      if (!qdump_print_value (&var_list->value))
	{
	  return false;
	}

      fprintf (foutput, "; ");
      var_list = var_list->next;
    }

  return true;
}

/*
 * qdump_option_string () -
 *   return:
 *   option(in):
 */
static const char *
qdump_option_string (int option)
{
  switch (option)
    {
    case Q_DISTINCT:
      return "DISTINCT";
    case Q_ALL:
      return "ALL";
    default:
      return "undefined";
    }
}

/*
 * qdump_print_db_value () -
 *   return:
 *   value(in):
 */
static bool
qdump_print_db_value (DB_VALUE * value_p)
{
  db_value_print (value_p);
  return true;
}

static const char *
qdump_regu_type_string (REGU_DATATYPE type)
{
  switch (type)
    {
    case TYPE_DBVAL:
      return "TYPE_DBVAL";
    case TYPE_CONSTANT:
      return "TYPE_CONSTANT";
    case TYPE_ORDERBY_NUM:
      return "TYPE_ORDERBY_NUM";
    case TYPE_AGGREGATE:
      return "TYPE_AGGREGATE";
    case TYPE_INARITH:
      return "TYPE_INARITH";
    case TYPE_OUTARITH:
      return "TYPE_OUTARITH";
    case TYPE_LIST_ID:
      return "TYPE_LIST_ID";
    case TYPE_ATTR_ID:
      return "TYPE_ATTR_ID";
    case TYPE_SHARED_ATTR_ID:
      return "TYPE_SHARED_ATTR_ID";
    case TYPE_CLASS_ATTR_ID:
      return "TYPE_CLASS_ATTR_ID";
    case TYPE_POSITION:
      return "TYPE_POSITION";
    case TYPE_POS_VALUE:
      return "TYPE_POS_VALUE";
    case TYPE_OID:
      return "TYPE_OID";
    case TYPE_CLASSOID:
      return "TYPE_CLASSOID";
    case TYPE_FUNC:
      return "TYPE_FUNC";
    default:
      return "undefined";
    }
}

/*
 * qdump_print_regu_type () -
 *   return:
 *   value(in):
 */
static bool
qdump_print_regu_type (REGU_VARIABLE * value_p)
{
  DB_TYPE type;

  if (value_p->type == TYPE_DBVAL)
    {
      type = DB_VALUE_DOMAIN_TYPE (&(value_p->value.dbval));
      fprintf (foutput, "%s", qdump_data_type_string (type));
    }
  else
    {
      fprintf (foutput, "%s", qdump_regu_type_string (value_p->type));
    }

  return true;
}

/*
 * qdump_data_type_string () -
 *   return:
 *   type(in):
 */

static const char *
qdump_data_type_string (DB_TYPE type)
{
  switch (type)
    {
    case DB_TYPE_NULL:
      return "NULL";
    case DB_TYPE_INTEGER:
      return "INTEGER";
    case DB_TYPE_BIGINT:
      return "BIGINT";
    case DB_TYPE_FLOAT:
      return "FLOAT";
    case DB_TYPE_DOUBLE:
      return "DOUBLE";
    case DB_TYPE_VARCHAR:
      return "VARCHAR";
    case DB_TYPE_OBJECT:
      return "OBJECT";
    case DB_TYPE_SET:
      return "SET";
    case DB_TYPE_MULTISET:
      return "MULTISET";
    case DB_TYPE_SEQUENCE:
      return "SEQUENCE";
    case DB_TYPE_ELO:
      return "ELO";
    case DB_TYPE_TIME:
      return "TIME";
    case DB_TYPE_TIMESTAMP:
      return "TIMESTAMP";
    case DB_TYPE_DATETIME:
      return "DATETIME";
    case DB_TYPE_DATE:
      return "DATE";
    case DB_TYPE_MONETARY:
      return "MONETARY";
    case DB_TYPE_VARIABLE:
      return "VARIABLE";
    case DB_TYPE_SUB:
      return "SUB";
    case DB_TYPE_POINTER:
      return "POINTER";
    case DB_TYPE_ERROR:
      return "ERROR";
    case DB_TYPE_SMALLINT:
      return "SMALLINT";
    case DB_TYPE_VOBJ:
      return "VOBJ";
    case DB_TYPE_OID:
      return "OID";
    case DB_TYPE_NUMERIC:
      return "NUMERIC";
    case DB_TYPE_BIT:
      return "BIT";
    case DB_TYPE_VARBIT:
      return "VARBIT";
    case DB_TYPE_CHAR:
      return "CHAR";
    case DB_TYPE_NCHAR:
      return "NCHAR";
    case DB_TYPE_VARNCHAR:
      return "VARNCHAR";
    case DB_TYPE_DB_VALUE:
      return "DB_VALUE";
    case DB_TYPE_RESULTSET:
      return "DB_RESULTSET";
    case DB_TYPE_MIDXKEY:
      return "DB_MIDXKEY";
    case DB_TYPE_TABLE:
      return "DB_TABLE";
    default:
      return "[***UNKNOWN***]";
    }
}

/*
 * qdump_print_value () -
 *   return:
 *   value(in):
 */
static bool
qdump_print_value (REGU_VARIABLE * value_p)
{
  XASL_NODE *xasl_p;

  if (value_p == NULL)
    {
      fprintf (foutput, "NIL");
      return true;
    }

  if (value_p->hidden_column)
    {
      fprintf (foutput, "[HIDDEN_COLUMN]");
    }
  xasl_p = REGU_VARIABLE_XASL (value_p);
  if (xasl_p)
    {
      fprintf (foutput, "[xasl:%p]", xasl_p);
    }

  fprintf (foutput, "[");
  qdump_print_value_type_addr (value_p);
  fprintf (foutput, "]");

  switch (value_p->type)
    {
    case TYPE_DBVAL:
      qdump_print_db_value (&value_p->value.dbval);
      return true;

    case TYPE_CONSTANT:
    case TYPE_ORDERBY_NUM:
      qdump_print_db_value (value_p->value.dbvalptr);
      return true;

    case TYPE_AGGREGATE:
      qdump_print_arith (AGG_EXP, (void *) value_p->value.aggptr);
      return true;

    case TYPE_INARITH:
    case TYPE_OUTARITH:
      if (!qdump_print_arith (ARITH_EXP, (void *) value_p->value.arithptr))
	{
	  return false;
	}
      return true;
    case TYPE_ATTR_ID:
      if (!qdump_print_attribute_id (value_p->value.attr_descr))
	{
	  return false;
	}
      return true;

    case TYPE_LIST_ID:
      if (value_p->value.srlist_id->sorted)
	{
	  fprintf (foutput, "[SORTED]");
	}
      else
	{
	  fprintf (foutput, "[NOT SORTED]");
	}

      if (!qdump_print_listi_d (value_p->value.srlist_id->list_id))
	{
	  return false;
	}

      return true;

    case TYPE_POSITION:
      if (!qdump_print_tuple_value_position (value_p->value.pos_descr))
	{
	  return false;
	}

      return true;

    case TYPE_POS_VALUE:
    case TYPE_OID:
      return true;

    case TYPE_FUNC:
      qdump_print_function_value (value_p);
      return true;

    default:
      return true;
    }
}

static const char *
qdump_function_type_string (FUNC_TYPE ftype)
{
  switch (ftype)
    {
    case PT_MIN:
      return "MIN";
    case PT_MAX:
      return "MAX";
    case PT_SUM:
      return "SUM";
    case PT_AVG:
      return "AVG";
    case PT_STDDEV:
      return "STDDEV";
    case PT_VARIANCE:
      return "VARIANCE";
    case PT_COUNT:
      return "COUNT";
    case PT_COUNT_STAR:
      return "COUNT_STAR";
    case PT_GROUPBY_NUM:
      return "GROUPBY_NUM";
    case PT_AGG_BIT_AND:
      return "BIT_AND";
    case PT_AGG_BIT_OR:
      return "BIT_OR";
    case PT_AGG_BIT_XOR:
      return "BIT_XOR";
    case PT_TOP_AGG_FUNC:
      return "TOP_AGG_FUNC";
    case PT_GENERIC:
      return "GENERIC";
    case F_TABLE_SET:
      return "F_TABLE_SET";
    case F_TABLE_MULTISET:
      return "F_TABLE_MULTISET";
    case F_TABLE_SEQUENCE:
      return "F_TABLE_SEQUENCE";
    case F_TOP_TABLE_FUNC:
      return "F_TOP_TABLE_FUNC";
    case F_MIDXKEY:
      return "F_MIDXKEY";
    case F_SET:
      return "F_SET";
    case F_MULTISET:
      return "F_MULTISET";
    case F_SEQUENCE:
      return "F_SEQUENCE";
    case F_VID:
      return "F_VID";
    case F_GENERIC:
      return "F_GENERIC";
    case F_CLASS_OF:
      return "F_CLASS_OF";
    default:
      return "***UNKNOWN***";
    }
}

/*
 * qdump_print_function_value () -
 *   return:
 *   regu(in):
 */
static bool
qdump_print_function_value (REGU_VARIABLE * regu_var_p)
{
  if (regu_var_p == NULL)
    {
      fprintf (foutput, "NIL");
      return true;
    }

  if (regu_var_p->hidden_column)
    {
      fprintf (foutput, "[HIDDEN_COLUMN]");
    }

  fprintf (foutput, "[TYPE_FUNC]");
  fprintf (foutput, "[%s]",
	   qdump_function_type_string (regu_var_p->value.funcp->ftype));
  fprintf (foutput, "operand-->");
  qdump_print_regu_variable_list (regu_var_p->value.funcp->operand);

  return true;
}

/*
 * qdump_print_value_type_addr () -
 *   return:
 *   value(in):
 */
static bool
qdump_print_value_type_addr (REGU_VARIABLE * regu_var_p)
{
  void *addr;

  qdump_print_regu_type (regu_var_p);

  switch (regu_var_p->type)
    {
    case TYPE_DBVAL:
      addr = (void *) &regu_var_p->value.dbval;
      break;

    case TYPE_CONSTANT:
    case TYPE_ORDERBY_NUM:
      addr = (void *) regu_var_p->value.dbvalptr;
      break;

    case TYPE_AGGREGATE:
      addr = (void *) regu_var_p->value.aggptr;
      break;

    case TYPE_INARITH:
    case TYPE_OUTARITH:
      addr = (void *) regu_var_p->value.arithptr;
      break;

    case TYPE_LIST_ID:
      addr = (void *) regu_var_p->value.srlist_id->list_id;
      break;

    case TYPE_ATTR_ID:
    case TYPE_SHARED_ATTR_ID:
    case TYPE_CLASS_ATTR_ID:
      addr = (void *) &regu_var_p->value.attr_descr;
      break;

    case TYPE_POSITION:
      addr = (void *) &regu_var_p->value.pos_descr;
      break;

    case TYPE_POS_VALUE:
      addr = (void *) &regu_var_p->value.val_pos;
      break;

    case TYPE_OID:
    case TYPE_CLASSOID:
    case TYPE_FUNC:
      return true;

    default:
      return false;
    }

  fprintf (foutput, ":%p", addr);

  return true;
}


/*
 * qdump_print_oid () -
 *   return:
 *   id(in):
 */
static bool
qdump_print_oid (OID * oid_p)
{
  fprintf (foutput, "[OID:%d,%d,%d]",
	   oid_p->pageid, oid_p->slotid, oid_p->volid);
  return true;
}

static bool
qdump_print_comp_eval_term (EVAL_TERM * term_p)
{
  COMP_EVAL_TERM *et_comp_p = &term_p->et.et_comp;

  fprintf (foutput, "[TYPE:%s]", qdump_data_type_string (et_comp_p->type));

  qdump_print_value (et_comp_p->lhs);
  fprintf (foutput, " %s ",
	   qdump_relation_operator_string (et_comp_p->rel_op));

  if (et_comp_p->rhs != NULL)
    {
      qdump_print_value (et_comp_p->rhs);
    }

  return true;
}

static bool
qdump_print_alsm_eval_term (EVAL_TERM * term_p)
{
  ALSM_EVAL_TERM *et_alsm_p = &term_p->et.et_alsm;

  fprintf (foutput, "[ITEM TYPE:%s]",
	   qdump_data_type_string (et_alsm_p->item_type));

  qdump_print_value (et_alsm_p->elem);
  fprintf (foutput, " %s ",
	   qdump_relation_operator_string (et_alsm_p->rel_op));

  switch (et_alsm_p->eq_flag)
    {
    case F_SOME:
      fprintf (foutput, "some ");
      break;
    case F_ALL:
      fprintf (foutput, "all ");
      break;
    default:
      return false;
    }

  qdump_print_value (et_alsm_p->elemset);

  return true;
}

static bool
qdump_print_like_eval_term (EVAL_TERM * term_p)
{
  LIKE_EVAL_TERM *et_like_p = &term_p->et.et_like;

  fprintf (foutput, "SOURCE");
  qdump_print_value (et_like_p->src);
  fprintf (foutput, "PATTERN:");

  if (!qdump_print_value (et_like_p->pattern))
    {
      return false;
    }

  if (et_like_p->esc_char)
    {
      if (!qdump_print_value (et_like_p->esc_char))
	{
	  return false;
	}
    }

  return true;
}

static bool
qdump_print_eval_term (PRED_EXPR * pred_p)
{
  EVAL_TERM *term = &pred_p->pe.eval_term;

  switch (term->et_type)
    {
    case T_COMP_EVAL_TERM:
      return qdump_print_comp_eval_term (term);

    case T_ALSM_EVAL_TERM:
      return qdump_print_alsm_eval_term (term);

    case T_LIKE_EVAL_TERM:
      return qdump_print_like_eval_term (term);

    default:
      return false;
    }
}

/*
 * qdump_print_term () -
 *   return:
 *   term(in):
 */
static bool
qdump_print_term (PRED_EXPR * pred_p)
{
  if (pred_p == NULL)
    {
      return true;
    }

  switch (pred_p->type)
    {
    case T_EVAL_TERM:
      return qdump_print_eval_term (pred_p);

    case T_NOT_TERM:
      fprintf (foutput, "(NOT ");

      if (!qdump_print_predicate (pred_p->pe.not_term))
	{
	  return false;
	}
      fprintf (foutput, ")");

      return true;

    default:
      return false;
    }
}

static const char *
qdump_bool_operator_string (BOOL_OP bool_op)
{
  if (bool_op == B_AND)
    {
      return "AND";
    }
  else if (bool_op == B_OR)
    {
      return "OR";
    }
  else if (bool_op == B_XOR)
    {
      return "XOR";
    }
  else if (bool_op == B_IS)
    {
      return "IS";
    }
  else if (bool_op == B_IS_NOT)
    {
      return "IS NOT";
    }
  else
    {
      return "undefined";
    }
}

static bool
qdump_print_lhs_predicate (PRED_EXPR * pred_p)
{
  fprintf (foutput, "(");

  if (!qdump_print_predicate (pred_p->pe.pred.lhs))
    {
      return false;
    }

  fprintf (foutput, " %s ",
	   qdump_bool_operator_string (pred_p->pe.pred.bool_op));

  return true;
}

/*
 * qdump_print_predicate () -
 *   return:
 *   predptr(in):
 */
static bool
qdump_print_predicate (PRED_EXPR * pred_p)
{
  int parn_cnt;

  if (pred_p == NULL)
    {
      return true;
    }

  switch (pred_p->type)
    {
    case T_PRED:
      if (qdump_print_lhs_predicate (pred_p) == false)
	{
	  return false;
	}

      parn_cnt = 1;

      /* Traverse right-linear chains of AND/OR terms */
      for (pred_p = pred_p->pe.pred.rhs; pred_p->type == T_PRED;
	   pred_p = pred_p->pe.pred.rhs)
	{
	  if (qdump_print_lhs_predicate (pred_p) == false)
	    {
	      return false;
	    }

	  parn_cnt++;
	}

      /* rhs */
      switch (pred_p->type)
	{
	case T_EVAL_TERM:
	case T_NOT_TERM:
	  if (!qdump_print_term (pred_p))
	    {
	      return false;
	    }
	  break;
	default:
	  return false;
	}

      while (parn_cnt > 0)
	{
	  fprintf (foutput, ")");
	  parn_cnt--;
	}

      return true;

    case T_EVAL_TERM:
    case T_NOT_TERM:
      return qdump_print_term (pred_p);

    default:
      return false;
    }
}

/*
 * qdump_relation_operator_string () -
 *   return:
 *   op(in):
 */
static const char *
qdump_relation_operator_string (int op)
{
  switch (op)
    {
    case R_EQ:
      return "=";
    case R_NE:
      return "<>";
    case R_GT:
      return ">";
    case R_GE:
      return ">=";
    case R_LT:
      return "<";
    case R_LE:
      return "<=";
    case R_NULL:
      return "IS NULL";
    case R_EXISTS:
      return "EXISTS";
    case R_NULLSAFE_EQ:
      return "<=>";
    default:
      return "undefined";
    }
}

static const char *
qdump_arith_operator_string (OPERATOR_TYPE opcode)
{
  switch (opcode)
    {
    case T_ADD:
      return "+";
    case T_SUB:
      return "-";
    case T_MUL:
      return "*";
    case T_DIV:
      return "/";
    case T_STRCAT:
      return "||";
    case T_BIT_NOT:
      return "~";
    case T_BIT_AND:
      return "&";
    case T_BIT_OR:
      return "|";
    case T_BIT_XOR:
      return "^";
    case T_BITSHIFT_LEFT:
      return "<<";
    case T_BITSHIFT_RIGHT:
      return ">>";
    case T_INTDIV:
      return "div";
    case T_INTMOD:
      return "mod";
    default:
      return "undefined";
    }
}

static bool
qdump_print_arith_expression (int type, ARITH_TYPE * arith_p)
{
  fprintf (foutput, "[%s]",
	   qdump_data_type_string (DB_VALUE_DOMAIN_TYPE (arith_p->value)));

  if (arith_p->opcode == T_UNMINUS
#if defined(ENABLE_UNUSED_FUNCTION)
      || arith_p->opcode == T_UNPLUS
#endif /* ENABLE_UNUSED_FUNCTION */
    )
    {
      fprintf (foutput, "(");
      if (arith_p->opcode == T_UNMINUS)
	{
	  fprintf (foutput, "-");
	}
#if defined(ENABLE_UNUSED_FUNCTION)
      else
	{
	  fprintf (foutput, "+");
	}
#endif /* ENABLE_UNUSED_FUNCTION */

      if (!qdump_print_value (arith_p->rightptr))
	{
	  return false;
	}
      fprintf (foutput, ")");
    }
  else
    {
      /*binary op */

      fprintf (foutput, "(");
      if (!qdump_print_value (arith_p->leftptr))
	{
	  return false;
	}

      fprintf (foutput, "%s", qdump_arith_operator_string (arith_p->opcode));

      if (!qdump_print_value (arith_p->rightptr))
	{
	  return false;
	}
      fprintf (foutput, ")");
    }

  if (type == LARITH_EXP)
    {
      fprintf (foutput, "; ");
      if (!qdump_print_arith (LARITH_EXP, (void *) arith_p->next))
	{
	  return false;
	}
    }

  return true;
}

static bool
qdump_print_aggregate_expression (int type, AGGREGATE_TYPE * aggptr)
{
  fprintf (foutput, "[%s]",
	   qdump_data_type_string (DB_VALUE_DOMAIN_TYPE (aggptr->value)));

  fprintf (foutput, "%s(", qdump_function_type_string (aggptr->function));

  fprintf (foutput, "%s ", qdump_option_string (aggptr->option));

  if (!qdump_print_value (&aggptr->operand))
    {
      return false;
    }

  if (!qdump_print_listi_d (aggptr->list_id))
    {
      return false;
    }

  fprintf (foutput, "(optimize:%d)", aggptr->flag_agg_optimize);

  if (!qdump_print_btid (aggptr->btid))
    {
      return false;
    }

  fprintf (foutput, ")");

  if (type == LAGG_EXP)
    {
      fprintf (foutput, "; ");
      if (!qdump_print_arith (LAGG_EXP, aggptr->next))
	{
	  return false;
	}
    }

  return true;
}

/*
 * qdump_print_arith () -
 *   return:
 *   type(in):
 *   ptr(in):
 */
static bool
qdump_print_arith (int type, void *ptr)
{
  if (ptr == NULL)
    {
      return true;
    }

  if (type == ARITH_EXP || type == LARITH_EXP)
    {
      return qdump_print_arith_expression (type, (ARITH_TYPE *) ptr);
    }
  else if (type == AGG_EXP || type == LAGG_EXP)
    {
      return qdump_print_aggregate_expression (type, (AGGREGATE_TYPE *) ptr);
    }

  return true;
}

#if defined(CUBRID_DEBUG)
/*
 * qdump_check_xasl_tree () -
 *   return:
 *   xasl(in):
 */
bool
qdump_check_xasl_tree (XASL_NODE * xasl_p)
{
  QDUMP_XASL_CHECK_NODE *chk_nodes[HASH_NUMBER] = { NULL };

  if (xasl_p == NULL)
    {
      return true;
    }

  /* recursively check the tree */
  qdump_check_node (xasl_p, chk_nodes);

  /* print any inconsistencies in the tree */
  return qdump_print_inconsistencies (chk_nodes);
}

/*
 * qdump_find_check_node_for () -
 *   return:
 *   xasl(in):
 *   chk_nodes(in):
 */
static QDUMP_XASL_CHECK_NODE *
qdump_find_check_node_for (XASL_NODE * xasl_p,
			   QDUMP_XASL_CHECK_NODE * chk_nodes[HASH_NUMBER])
{
  UINTPTR access_node_hash;
  QDUMP_XASL_CHECK_NODE *check_node_p;

  access_node_hash = (UINTPTR) xasl_p % HASH_NUMBER;

  for (check_node_p = chk_nodes[access_node_hash]; check_node_p;
       check_node_p = check_node_p->next)
    {
      if (check_node_p->xasl_addr == (UINTPTR) xasl_p)
	{
	  break;
	}
    }

  if (!check_node_p)
    {
      /* forward reference */
      check_node_p =
	(QDUMP_XASL_CHECK_NODE *) malloc (sizeof (QDUMP_XASL_CHECK_NODE));
      if (check_node_p == NULL)
	{
	  return NULL;
	}
      check_node_p->next = chk_nodes[access_node_hash];
      chk_nodes[access_node_hash] = check_node_p;
      check_node_p->xasl_addr = (UINTPTR) xasl_p;
      check_node_p->xasl_type = xasl_p->type;
      check_node_p->referenced = 0;
      check_node_p->reachable = 0;
    }

  return check_node_p;
}

/*
 * qdump_check_node () -
 *   return:
 *   xasl(in):
 *   chk_nodes(in):
 */
static void
qdump_check_node (XASL_NODE * xasl_p,
		  QDUMP_XASL_CHECK_NODE * chk_nodes[HASH_NUMBER])
{
  UINTPTR addr_hash;
  QDUMP_XASL_CHECK_NODE *check_node_p, *check_node1_p;
  ACCESS_SPEC_TYPE *spec_p;

  if (!xasl_p)
    {
      return;
    }

  /* get hash number */
  addr_hash = (UINTPTR) xasl_p % HASH_NUMBER;

  check_node_p = qdump_find_check_node_for (xasl_p, chk_nodes);
  if (check_node_p == NULL)
    {
      return;
    }

  if (check_node_p->reachable)
    {
      return;
    }

  check_node_p->reachable = 1;

  /*
   * Mark the node its access spec references.  You may need to create
   * it if it is a forward reference.
   */
  for (spec_p = xasl_p->spec_list; spec_p; spec_p = spec_p->next)
    {
      if (spec_p->type == TARGET_LIST)
	{
	  check_node1_p =
	    qdump_find_check_node_for (ACCESS_SPEC_XASL_NODE (spec_p),
				       chk_nodes);
	  /* mark as referenced */
	  if (check_node1_p)
	    {
	      check_node1_p->referenced = 1;
	    }
	}
    }

  /* recursively check the children of this node */
  switch (xasl_p->type)
    {
    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:
      check_node1_p =
	qdump_find_check_node_for (xasl_p->proc.union_.left, chk_nodes);
      if (check_node1_p)
	{
	  check_node1_p->referenced = 1;
	}

      check_node1_p =
	qdump_find_check_node_for (xasl_p->proc.union_.right, chk_nodes);
      if (check_node1_p)
	{
	  check_node1_p->referenced = 1;
	}
      break;

    case MERGELIST_PROC:
      check_node1_p =
	qdump_find_check_node_for (xasl_p->proc.mergelist.inner_xasl,
				   chk_nodes);
      if (check_node1_p)
	{
	  check_node1_p->referenced = 1;
	}

      check_node1_p =
	qdump_find_check_node_for (xasl_p->proc.mergelist.inner_xasl,
				   chk_nodes);
      if (check_node1_p)
	{
	  check_node1_p->referenced = 1;
	}
      break;

    case BUILDLIST_PROC:
      qdump_check_node (xasl_p->proc.buildlist.eptr_list, chk_nodes);

    default:
      break;
    }

  qdump_check_node (xasl_p->aptr_list, chk_nodes);
  qdump_check_node (xasl_p->bptr_list, chk_nodes);
  qdump_check_node (xasl_p->scan_ptr, chk_nodes);
  qdump_check_node (xasl_p->dptr_list, chk_nodes);
  qdump_check_node (xasl_p->fptr_list, chk_nodes);
  qdump_check_node (xasl_p->connect_by_ptr, chk_nodes);
  qdump_check_node (xasl_p->next, chk_nodes);
}

/*
 * qdump_print_inconsistencies () -
 *   return:
 *   chk_nodes(in):
 */
static int
qdump_print_inconsistencies (QDUMP_XASL_CHECK_NODE * chk_nodes[HASH_NUMBER])
{
  int i, error = 0;
  QDUMP_XASL_CHECK_NODE *check_node_p, *tmp_node_p;

  for (i = 0; i < HASH_NUMBER; i++)
    {
      for (check_node_p = chk_nodes[i]; check_node_p;
	   check_node_p = check_node_p->next)
	{
	  /* any buildlist procs that are referenced must be reachable */
	  if (check_node_p->referenced && !check_node_p->reachable)
	    {
	      if (!error)
		{
		  fprintf (stdout,
			   "\nSYSTEM ERROR--INCONSISTENT XASL TREE\n\n");
		}

	      fprintf (stdout,
		       "Referenced node [%lld] is not reachable in the tree\n",
		       (long long) check_node_p->xasl_addr);
	      error = 1;
	    }
	}
    }

  /* clean up our mallocs */
  for (i = 0; i < HASH_NUMBER; i++)
    {
      for (check_node_p = chk_nodes[i]; check_node_p; /* do nothing */ )
	{
	  tmp_node_p = check_node_p;
	  check_node_p = check_node_p->next;
	  free_and_init (tmp_node_p);
	}
    }

  if (error)
    {
      fprintf (stdout, "\n");
    }

  return !error;
}
#endif /* CUBRID_DEBUG */

static bool
qdump_print_fetch_node (XASL_NODE * xasl_p)
{
  FETCH_PROC_NODE *node_p = &xasl_p->proc.fetch;

  fprintf (foutput, "-->fetch  <addr:%p><type:", node_p->arg);
  fprintf (foutput, "%s",
	   qdump_data_type_string (DB_VALUE_DOMAIN_TYPE (node_p->arg)));
  fprintf (foutput, ">\n fetch_res (%d)\n", (int) node_p->fetch_res);

  if (node_p->set_pred)
    {
      fprintf (foutput, "-->set predicate:");
      qdump_print_predicate (node_p->set_pred);
      fprintf (foutput, "\n");
    }

  if (node_p->ql_flag)
    {
      fprintf (foutput, "-->ql_flag on (no null paths)\n");
    }

  return true;
}

static bool
qdump_print_build_list_node (XASL_NODE * xasl_p)
{
  BUILDLIST_PROC_NODE *node_p = &xasl_p->proc.buildlist;

  if (xasl_p->outptr_list != NULL)
    {
      fprintf (foutput, "-->output columns:");
      qdump_print_db_value_array (node_p->output_columns,
				  xasl_p->outptr_list->valptr_cnt);
      fprintf (foutput, "\n");
    }

  if (node_p->g_outptr_list)
    {
      qdump_print_outlist ("group by output ptrlist", node_p->g_outptr_list);
      fprintf (foutput, "\n");
    }

  if (node_p->groupby_list)
    {
      fprintf (foutput, "-->group by list:");
      qdump_print_sort_list (node_p->groupby_list);
      fprintf (foutput, "\n");
    }

  if (node_p->g_regu_list)
    {
      fprintf (foutput, "-->group by regu list:");
      qdump_print_regu_variable_list (node_p->g_regu_list);
      fprintf (foutput, "\n");
    }

  if (node_p->g_val_list)
    {
      fprintf (foutput, "-->group by val_list:");
      qdump_print_value_list (node_p->g_val_list);
      fprintf (foutput, "\n");
    }

  if (node_p->g_having_pred)
    {
      fprintf (foutput, "-->having predicate:");
      qdump_print_predicate (node_p->g_having_pred);
      fprintf (foutput, "\n");
    }

  if (node_p->g_grbynum_val)
    {
      fprintf (foutput, "-->grbynum val:");
      fprintf (foutput, "<addr:%p|", node_p->g_grbynum_val);
      fprintf (foutput, "type:%s",
	       qdump_data_type_string (DB_VALUE_DOMAIN_TYPE
				       (node_p->g_grbynum_val)));
      fprintf (foutput, "|value:");
      qdump_print_db_value (node_p->g_grbynum_val);
      fprintf (foutput, ">\n");
    }

  if (node_p->g_grbynum_pred)
    {
      fprintf (foutput, "-->grbynum predicate:");
      qdump_print_predicate (node_p->g_grbynum_pred);
      fprintf (foutput, "\n");

      if (node_p->g_grbynum_flag == XASL_G_GRBYNUM_FLAG_SCAN_CONTINUE)
	{
	  fprintf (foutput, "-->grbynum CONTINUE\n");
	}
    }

  if (node_p->g_agg_list)
    {
      fprintf (foutput, "-->having agg list:");
      qdump_print_arith (LAGG_EXP, (void *) node_p->g_agg_list);
      fprintf (foutput, "\n");
    }

  if (node_p->g_outarith_list)
    {
      fprintf (foutput, "-->having outarith list:");
      qdump_print_arith (ARITH_EXP, (void *) node_p->g_outarith_list);
      fprintf (foutput, "\n");
    }

  if (node_p->eptr_list)
    {
      fprintf (foutput, "-->EPTR LIST:%p\n", node_p->eptr_list);
    }

  if (node_p->g_with_rollup)
    {
      fprintf (foutput, "-->WITH ROLLUP\n");
    }

  return true;
}

static bool
qdump_print_build_value_node (XASL_NODE * xasl_p)
{
  BUILDVALUE_PROC_NODE *node_p = &xasl_p->proc.buildvalue;
  if (xasl_p->proc.buildvalue.having_pred)
    {
      fprintf (foutput, "-->having predicate:");
      qdump_print_predicate (node_p->having_pred);
      fprintf (foutput, "\n");
    }

  if (node_p->grbynum_val)
    {
      fprintf (foutput, "-->grbynum val:");
      fprintf (foutput, "<addr:%p|", node_p->grbynum_val);
      fprintf (foutput, "type:%s",
	       qdump_data_type_string (DB_VALUE_DOMAIN_TYPE
				       (node_p->grbynum_val)));
      fprintf (foutput, "|value:");
      qdump_print_db_value (node_p->grbynum_val);
      fprintf (foutput, ">\n");
    }

  if (node_p->agg_list)
    {
      fprintf (foutput, "-->agg list:");
      qdump_print_arith (LAGG_EXP, (void *) node_p->agg_list);
      fprintf (foutput, "\n");
    }

  if (node_p->outarith_list)
    {
      fprintf (foutput, "-->outarith list:");
      qdump_print_arith (ARITH_EXP, (void *) node_p->outarith_list);
    }

  if (node_p->is_always_false)
    {
      fprintf (foutput, "-->always-false\n");
    }

  return true;
}

static bool
qdump_print_connect_by_proc_node (XASL_NODE * xasl_p)
{
  CONNECTBY_PROC_NODE *node_p = &xasl_p->proc.connect_by;

  if (node_p->start_with_pred)
    {
      fprintf (foutput, "-->start with predicate:");
      qdump_print_predicate (node_p->start_with_pred);
      fprintf (foutput, "\n");
    }

  if (node_p->after_connect_by_pred)
    {
      fprintf (foutput, "-->after connect by predicate:");
      qdump_print_predicate (node_p->after_connect_by_pred);
      fprintf (foutput, "\n");
    }

  if (node_p->input_list_id)
    {
      fprintf (foutput, "-->input list id:");
      qdump_print_listi_d (node_p->input_list_id);
      fprintf (foutput, "\n");
    }

  if (node_p->start_with_list_id)
    {
      fprintf (foutput, "-->start with list id:");
      qdump_print_listi_d (node_p->start_with_list_id);
      fprintf (foutput, "\n");
    }

  if (node_p->regu_list_pred)
    {
      fprintf (foutput, "-->connect by regu list pred:");
      qdump_print_regu_variable_list (node_p->regu_list_pred);
      fprintf (foutput, "\n");
    }

  if (node_p->regu_list_rest)
    {
      fprintf (foutput, "-->connect by regu list rest:");
      qdump_print_regu_variable_list (node_p->regu_list_rest);
      fprintf (foutput, "\n");
    }

  if (node_p->prior_val_list)
    {
      fprintf (foutput, "-->prior val list:");
      qdump_print_value_list (node_p->prior_val_list);
      fprintf (foutput, "\n");
    }

  if (node_p->prior_outptr_list)
    {
      qdump_print_outlist ("prior output ptrlist", node_p->prior_outptr_list);
      fprintf (foutput, "\n");
    }

  if (node_p->prior_regu_list_pred)
    {
      fprintf (foutput, "-->prior regu list pred:");
      qdump_print_regu_variable_list (node_p->prior_regu_list_pred);
      fprintf (foutput, "\n");
    }

  if (node_p->prior_regu_list_rest)
    {
      fprintf (foutput, "-->prior regu list rest:");
      qdump_print_regu_variable_list (node_p->prior_regu_list_rest);
      fprintf (foutput, "\n");
    }

  if (node_p->after_cb_regu_list_pred)
    {
      fprintf (foutput, "-->after connect by regu list pred:");
      qdump_print_regu_variable_list (node_p->after_cb_regu_list_pred);
      fprintf (foutput, "\n");
    }

  if (node_p->after_cb_regu_list_rest)
    {
      fprintf (foutput, "-->after connect by regu list rest:");
      qdump_print_regu_variable_list (node_p->after_cb_regu_list_rest);
      fprintf (foutput, "\n");
    }

  return true;
}

/*
 * qdump_print_xasl () -
 *   return:
 *   xasl(in):
 */
bool
qdump_print_xasl (XASL_NODE * xasl_p)
{
  VAL_LIST *single_tuple_p;
  QPROC_DB_VALUE_LIST value_list;
  int i;

  if (xasl_p == NULL)
    {
      return true;
    }

  fprintf (foutput, "\n<start of xasl structure %p>\n", xasl_p);
  qdump_print_xasl_type (xasl_p);

  if (xasl_p->flag)
    {
      int save_flag, nflag;

      save_flag = xasl_p->flag;
      nflag = 0;

      fprintf (foutput, "-->[flag=");

      if (XASL_IS_FLAGED (xasl_p, XASL_LINK_TO_REGU_VARIABLE))
	{
	  XASL_CLEAR_FLAG (xasl_p, XASL_LINK_TO_REGU_VARIABLE);
	  fprintf (foutput, "%sXASL_LINK_TO_REGU_VARIABLE",
		   (nflag ? "|" : ""));
	  nflag++;
	}

      if (XASL_IS_FLAGED (xasl_p, XASL_SKIP_ORDERBY_LIST))
	{
	  XASL_CLEAR_FLAG (xasl_p, XASL_SKIP_ORDERBY_LIST);
	  fprintf (foutput, "%sXASL_SKIP_ORDERBY_LIST", (nflag ? "|" : ""));
	  nflag++;
	}

      if (XASL_IS_FLAGED (xasl_p, XASL_ZERO_CORR_LEVEL))
	{
	  XASL_CLEAR_FLAG (xasl_p, XASL_ZERO_CORR_LEVEL);
	  fprintf (foutput, "%sXASL_ZERO_CORR_LEVEL", (nflag ? "|" : ""));
	  nflag++;
	}

      if (XASL_IS_FLAGED (xasl_p, XASL_TOP_MOST_XASL))
	{
	  XASL_CLEAR_FLAG (xasl_p, XASL_TOP_MOST_XASL);
	  fprintf (foutput, "%sXASL_TOP_MOST_XASL", (nflag ? "|" : ""));
	  nflag++;
	}

      if (xasl_p->flag)
	{
	  fprintf (foutput, "%d%s", xasl_p->flag, (nflag ? "|" : ""));
	  nflag++;
	}

      fprintf (foutput, "]\n");

      xasl_p->flag = save_flag;
    }

  if (xasl_p->next)
    {
      fprintf (foutput, "-->next:%p\n", xasl_p->next);
    }

  if (xasl_p->list_id)
    {
      fprintf (foutput, "-->list id:");
      qdump_print_listi_d (xasl_p->list_id);
      fprintf (foutput, "\n");
    }

  if (xasl_p->orderby_list)
    {
      fprintf (foutput, "-->order by list:");
      qdump_print_sort_list (xasl_p->orderby_list);
      fprintf (foutput, "\n");
    }

  if (xasl_p->ordbynum_val)
    {
      fprintf (foutput, "-->ordbynum val:");
      fprintf (foutput, "<addr:%p|", xasl_p->ordbynum_val);
      fprintf (foutput, "type:%s",
	       qdump_data_type_string (DB_VALUE_DOMAIN_TYPE
				       (xasl_p->ordbynum_val)));
      fprintf (foutput, "|value:");
      qdump_print_db_value (xasl_p->ordbynum_val);
      fprintf (foutput, ">\n");
    }

  if (xasl_p->ordbynum_pred)
    {
      fprintf (foutput, "-->ordbynum predicate:");
      qdump_print_predicate (xasl_p->ordbynum_pred);
      fprintf (foutput, "\n");
      if (xasl_p->ordbynum_flag == XASL_ORDBYNUM_FLAG_SCAN_CONTINUE)
	fprintf (foutput, "-->ordbynum CONTINUE\n");
    }

  if (xasl_p->is_single_tuple)
    {
      fprintf (foutput, "-->single tuple:");
      single_tuple_p = xasl_p->single_tuple;
      if (single_tuple_p)
	{
	  fprintf (foutput, "[value list]:");
	  for (value_list = single_tuple_p->valp, i = 0;
	       i < single_tuple_p->val_cnt;
	       value_list = value_list->next, i++)
	    {
	      qdump_print_db_value (value_list->val);
	      fprintf (foutput, "\t");
	    }
	}
      fprintf (foutput, "\n");
    }

  if (xasl_p->option == Q_DISTINCT)
    {
      fprintf (foutput, "-->query DISTINCT\n");
    }

  if (xasl_p->outptr_list)
    {
      qdump_print_outlist ("outptr list", xasl_p->outptr_list);
      fprintf (foutput, "\n");
    }

  if (xasl_p->remote_outptr_list)
    {
      qdump_print_outlist ("remote outptr list", xasl_p->remote_outptr_list);
      fprintf (foutput, "\n");
    }

  if (xasl_p->spec_list)
    {
      fprintf (foutput, "-->access spec:");
      qdump_print_access_spec (xasl_p->spec_list);
      fprintf (foutput, "\n");
    }

  if (xasl_p->merge_spec)
    {
      fprintf (foutput, "-->merge spec:");
      qdump_print_access_spec (xasl_p->merge_spec);
      fprintf (foutput, "\n");
    }

  if (xasl_p->val_list)
    {
      fprintf (foutput, "-->val_list:");
      qdump_print_value_list (xasl_p->val_list);
      fprintf (foutput, "\n");
    }

  if (xasl_p->aptr_list)
    {
      fprintf (foutput, "-->aptr list:%p\n", xasl_p->aptr_list);
    }

  if (xasl_p->bptr_list)
    {
      fprintf (foutput, "-->bptr list:%p\n", xasl_p->bptr_list);
    }

  if (xasl_p->scan_ptr)
    {
      fprintf (foutput, "-->scan ptr:%p\n", xasl_p->scan_ptr);
    }

  if (xasl_p->dptr_list)
    {
      fprintf (foutput, "-->dptr list:%p\n", xasl_p->dptr_list);
    }

  if (xasl_p->fptr_list)
    {
      fprintf (foutput, "-->fptr list:%p\n", xasl_p->fptr_list);
    }

  if (xasl_p->connect_by_ptr)
    {
      fprintf (foutput, "-->connect_by ptr:%p\n", xasl_p->connect_by_ptr);
    }

  if (xasl_p->after_join_pred)
    {
      fprintf (foutput, "-->after_join predicate:");
      qdump_print_predicate (xasl_p->after_join_pred);
      fprintf (foutput, "\n");
    }

  if (xasl_p->if_pred)
    {
      fprintf (foutput, "-->if predicate:");
      qdump_print_predicate (xasl_p->if_pred);
      fprintf (foutput, "\n");
    }

  if (xasl_p->instnum_val)
    {
      fprintf (foutput, "-->instnum val:");
      fprintf (foutput, "<addr:%p|", xasl_p->instnum_val);
      fprintf (foutput, "type:%s",
	       qdump_data_type_string (DB_VALUE_DOMAIN_TYPE
				       (xasl_p->instnum_val)));
      fprintf (foutput, "|value:");
      qdump_print_db_value (xasl_p->instnum_val);
      fprintf (foutput, ">\n");
    }

  if (xasl_p->instnum_pred)
    {
      fprintf (foutput, "-->instnum predicate:");
      qdump_print_predicate (xasl_p->instnum_pred);
      fprintf (foutput, "\n");
      if (xasl_p->instnum_flag == XASL_INSTNUM_FLAG_SCAN_CONTINUE)
	{
	  fprintf (foutput, "-->instnum CONTINUE\n");
	}
    }

  if (xasl_p->level_val)
    {
      fprintf (foutput, "-->level val:");
      fprintf (foutput, "<addr:%p|", xasl_p->level_val);
      fprintf (foutput, "type:%s",
	       qdump_data_type_string (DB_VALUE_DOMAIN_TYPE
				       (xasl_p->level_val)));
      fprintf (foutput, "|value:");
      qdump_print_db_value (xasl_p->level_val);
      fprintf (foutput, ">\n");
    }

  if (xasl_p->level_regu)
    {
      fprintf (foutput, "-->level regu:");
      qdump_print_value (xasl_p->level_regu);
      fprintf (foutput, "\n");
    }

  if (xasl_p->isleaf_val)
    {
      fprintf (foutput, "-->isleaf val:");
      fprintf (foutput, "<addr:%p|", xasl_p->isleaf_val);
      fprintf (foutput, "type:%s",
	       qdump_data_type_string (DB_VALUE_DOMAIN_TYPE
				       (xasl_p->isleaf_val)));
      fprintf (foutput, "|value:");
      qdump_print_db_value (xasl_p->isleaf_val);
      fprintf (foutput, ">\n");
    }

  if (xasl_p->isleaf_regu)
    {
      fprintf (foutput, "-->isleaf regu:");
      qdump_print_value (xasl_p->isleaf_regu);
      fprintf (foutput, "\n");
    }

  if (xasl_p->iscycle_val)
    {
      fprintf (foutput, "-->iscycle val:");
      fprintf (foutput, "<addr:%p|", xasl_p->iscycle_val);
      fprintf (foutput, "type:%s",
	       qdump_data_type_string (DB_VALUE_DOMAIN_TYPE
				       (xasl_p->iscycle_val)));
      fprintf (foutput, "|value:");
      qdump_print_db_value (xasl_p->iscycle_val);
      fprintf (foutput, ">\n");
    }

  if (xasl_p->iscycle_regu)
    {
      fprintf (foutput, "-->iscycle regu:");
      qdump_print_value (xasl_p->iscycle_regu);
      fprintf (foutput, "\n");
    }

  fprintf (foutput, "-->current spec:");
  qdump_print_access_spec (xasl_p->curr_spec);
  fprintf (foutput, "\n");
  fprintf (foutput, "-->[next scan on=%d]", xasl_p->next_scan_on);
  fprintf (foutput, "[next scan block on=%d]", xasl_p->next_scan_block_on);
  fprintf (foutput, "-->[cat fetched=%d]", xasl_p->cat_fetched);
  fprintf (foutput, "\n");

  switch (xasl_p->type)
    {
    case OBJFETCH_PROC:
    case SETFETCH_PROC:
      qdump_print_fetch_node (xasl_p);
      break;

    case BUILDLIST_PROC:
      qdump_print_build_list_node (xasl_p);
      break;

    case BUILDVALUE_PROC:
      qdump_print_build_value_node (xasl_p);
      break;

    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:
      fprintf (foutput, "left xasl:%p\n", xasl_p->proc.union_.left);
      fprintf (foutput, "right xasl:%p\n", xasl_p->proc.union_.right);
      break;

    case MERGELIST_PROC:
      fprintf (foutput, "inner xasl:%p\n", xasl_p->proc.mergelist.inner_xasl);
      fprintf (foutput, "outer xasl:%p\n", xasl_p->proc.mergelist.outer_xasl);
      qdump_print_merge_list_proc_node (&xasl_p->proc.mergelist);
      break;

    case CONNECTBY_PROC:
      qdump_print_connect_by_proc_node (xasl_p);
      break;

    case SCAN_PROC:
      break;

    case UPDATE_PROC:
      fprintf (foutput, "-->update info:");
      qdump_print_update_proc_node (&xasl_p->proc.update);
      fprintf (foutput, "\n");
      break;

    case DELETE_PROC:
      fprintf (foutput, "-->delete info:");
      qdump_print_delete_proc_node (&xasl_p->proc.delete_);
      fprintf (foutput, "\n");
      break;

    case INSERT_PROC:
      fprintf (foutput, "-->insert info:");
      qdump_print_insert_proc_node (&xasl_p->proc.insert);
      fprintf (foutput, "\n");
      break;

    default:
      return false;
    }

  fprintf (foutput, "end of internals of ");
  qdump_print_xasl_type (xasl_p);
  qdump_print_xasl (xasl_p->aptr_list);
  qdump_print_xasl (xasl_p->bptr_list);
  qdump_print_xasl (xasl_p->scan_ptr);
  qdump_print_xasl (xasl_p->dptr_list);
  qdump_print_xasl (xasl_p->fptr_list);
  qdump_print_xasl (xasl_p->connect_by_ptr);

  if (xasl_p->type == BUILDLIST_PROC)
    {
      qdump_print_xasl (xasl_p->proc.buildlist.eptr_list);
    }

  qdump_print_xasl (xasl_p->next);
  fprintf (foutput, "creator OID:");
  qdump_print_oid (&xasl_p->creator_oid);
  fprintf (foutput, "\nclass OID/reprid list:%d ", xasl_p->n_oid_list);

  for (i = 0; i < xasl_p->n_oid_list; ++i)
    {
      qdump_print_oid (&xasl_p->class_oid_list[i]);
      fprintf (foutput, "/%d ", xasl_p->repr_id_list[i]);
    }

  fprintf (foutput, "\ndbval_cnt:%d", xasl_p->dbval_cnt);
  fprintf (foutput, "\nqstmt:%s\n", xasl_p->qstmt ? xasl_p->qstmt : "(NULL)");
  fprintf (foutput, "<end of xasl structure %p>\n", xasl_p);
  fflush (foutput);

  return true;
}
