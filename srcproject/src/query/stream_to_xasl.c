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
 * stream_to_xasl.c - XASL tree restorer
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "error_manager.h"
#include "query_executor.h"
#include "server_interface.h"
#include "class_object.h"
#include "object_primitive.h"
#include "work_space.h"
#include "memory_alloc.h"
#include "heap_file.h"
#include "release_string.h"
#if defined(SERVER_MODE)
#include "thread_impl.h"
#endif /* SERVER_MODE */

/* memory alignment unit - to align stored XASL tree nodes */
#define	ALIGN_UNIT	sizeof(double)
#define	ALIGN_MASK	(ALIGN_UNIT - 1)
#define MAKE_ALIGN(x)	(((x) & ~ALIGN_MASK) + \
                         (((x) & ALIGN_MASK) ? ALIGN_UNIT : 0))

/* to limit size of XASL trees */
#define	OFFSETS_PER_BLOCK	256
#define	START_PTR_PER_BLOCK	15
#define MAX_PTR_BLOCKS		256

#define PTR_BLOCK(ptr)  (((UINTPTR) ptr) / sizeof(UINTPTR)) % MAX_PTR_BLOCKS

/*
 * the linear byte stream for store the given XASL tree is allocated
 * and expanded dynamically on demand by the following amount of bytes
 */
#define	STREAM_EXPANSION_UNIT	(OFFSETS_PER_BLOCK * sizeof(int))
#define BUFFER_EXPANSION 4

#define BOUND_VAL (1 << ((OR_INT_SIZE * 8) - 2))

/* structure of a visited pointer constant */
typedef struct visited_ptr VISITED_PTR;
struct visited_ptr
{
  const void *ptr;		/* a pointer constant */
  void *str;			/* where the struct pointed by 'ptr'
				   is stored */
};

/* structure to hold information needed during packing */
typedef struct xasl_unpack_info XASL_UNPACK_INFO;
struct xasl_unpack_info
{
  char *packed_xasl;		/* ptr to packed xasl tree */
#if defined (SERVER_MODE)
  THREAD_ENTRY *thrd;		/* used for private allocation */
#endif				/* SERVER_MODE */

  /* blocks of visited pointer constants */
  VISITED_PTR *ptr_blocks[MAX_PTR_BLOCKS];

  char *alloc_buf;		/* alloced buf */

  int packed_size;		/* packed xasl tree size */

  /* low-water-mark of visited pointers */
  int ptr_lwm[MAX_PTR_BLOCKS];

  /* max number of visited pointers */
  int ptr_max[MAX_PTR_BLOCKS];

  int alloc_size;		/* alloced buf size */
};

#if !defined(SERVER_MODE)
static XASL_UNPACK_INFO *xasl_unpack_info;
static int stx_Xasl_errcode = NO_ERROR;
#endif /* !SERVER_MODE */

static int stx_get_xasl_errcode (THREAD_ENTRY * thread_p);
static void stx_set_xasl_errcode (THREAD_ENTRY * thread_p, int errcode);
static XASL_UNPACK_INFO *stx_get_xasl_unpack_info_ptr (THREAD_ENTRY *
						       thread_p);
#if defined(SERVER_MODE)
static void stx_set_xasl_unpack_info_ptr (THREAD_ENTRY * thread_p,
					  XASL_UNPACK_INFO * ptr);
#endif /* SERVER_MODE */

static ACCESS_SPEC_TYPE *stx_restore_access_spec_type (THREAD_ENTRY *
						       thread_p, char **ptr);
static AGGREGATE_TYPE *stx_restore_aggregate_type (THREAD_ENTRY * thread_p,
						   char *ptr);
static FUNCTION_TYPE *stx_restore_function_type (THREAD_ENTRY * thread_p,
						 char *ptr);
static QFILE_SORTED_LIST_ID *stx_restore_srlist_id (THREAD_ENTRY * thread_p,
						    char *ptr);
static QFILE_LIST_ID *stx_restore_list_id (THREAD_ENTRY * thread_p,
					   char *ptr);
static ARITH_TYPE *stx_restore_arith_type (THREAD_ENTRY * thread_p,
					   char *ptr);
static INDX_INFO *stx_restore_indx_info (THREAD_ENTRY * thread_p, char *ptr);
static OUTPTR_LIST *stx_restore_outptr_list (THREAD_ENTRY * thread_p,
					     char *ptr);
static SELUPD_LIST *stx_restore_selupd_list (THREAD_ENTRY * thread_p,
					     char *ptr);
static PRED_EXPR *stx_restore_pred_expr (THREAD_ENTRY * thread_p, char *ptr);
static XASL_PARTITION_INFO *stx_restore_partition_info (THREAD_ENTRY *
							thread_p, char *ptr);
static XASL_PARTS_INFO *stx_restore_parts_info (THREAD_ENTRY * thread_p,
						char *ptr);
static XASL_PARTS_INFO **stx_restore_parts_array (THREAD_ENTRY * thread_p,
						  char *ptr, int size);
static XASL_PARTITION_INFO **stx_restore_partition_array (THREAD_ENTRY *
							  thread_p, char *ptr,
							  int size);
static REGU_VARIABLE *stx_restore_regu_variable (THREAD_ENTRY * thread_p,
						 char *ptr);
static REGU_VARIABLE_LIST stx_restore_regu_variable_list (THREAD_ENTRY *
							  thread_p,
							  char *ptr);
static REGU_VARLIST_LIST stx_restore_regu_varlist_list (THREAD_ENTRY *
							thread_p, char *ptr);
static SORT_LIST *stx_restore_sort_list (THREAD_ENTRY * thread_p, char *ptr);
static char *stx_restore_string (THREAD_ENTRY * thread_p, char *ptr);
static VAL_LIST *stx_restore_val_list (THREAD_ENTRY * thread_p, char *ptr);
static DB_VALUE *stx_restore_db_value (THREAD_ENTRY * thread_p, char *ptr);
static QPROC_DB_VALUE_LIST stx_restore_db_value_list (THREAD_ENTRY * thread_p,
						      char *ptr);
static XASL_NODE *stx_restore_xasl_node (THREAD_ENTRY * thread_p, char *ptr);
static HEAP_CACHE_ATTRINFO *stx_restore_cache_attrinfo (THREAD_ENTRY *
							thread_p, char *ptr);
static DB_VALUE **stx_restore_db_value_array_extra (THREAD_ENTRY * thread_p,
						    char *ptr, int size,
						    int total_size);
static int *stx_restore_int_array (THREAD_ENTRY * thread_p, char *ptr,
				   int size);
static OID *stx_restore_OID_array (THREAD_ENTRY * thread_p, char *ptr,
				   int size);
static METHOD_SIG_LIST *stx_restore_method_sig_list (THREAD_ENTRY * thread_p,
						     char *ptr);
static KEY_RANGE *stx_restore_key_range_array (THREAD_ENTRY * thread_p,
					       char *ptr, int size);

static char *stx_build_xasl_node (THREAD_ENTRY * thread_p, char *tmp,
				  XASL_NODE * ptr);
static char *stx_build_cache_attrinfo (char *tmp);
static char *stx_build_union_proc (THREAD_ENTRY * thread_p, char *tmp,
				   UNION_PROC_NODE * ptr);
static char *stx_build_fetch_proc (THREAD_ENTRY * thread_p, char *tmp,
				   FETCH_PROC_NODE * ptr);
static char *stx_build_buildlist_proc (THREAD_ENTRY * thread_p, char *tmp,
				       BUILDLIST_PROC_NODE * ptr);
static char *stx_build_buildvalue_proc (THREAD_ENTRY * thread_p, char *tmp,
					BUILDVALUE_PROC_NODE * ptr);
static char *stx_build_mergelist_proc (THREAD_ENTRY * thread_p, char *tmp,
				       MERGELIST_PROC_NODE * ptr);
static char *stx_build_ls_merge_info (THREAD_ENTRY * thread_p, char *tmp,
				      QFILE_LIST_MERGE_INFO * ptr);
static char *stx_build_update_proc (THREAD_ENTRY * thread_p, char *tmp,
				    UPDATE_PROC_NODE * ptr);
static char *stx_build_delete_proc (THREAD_ENTRY * thread_p, char *tmp,
				    DELETE_PROC_NODE * ptr);
static char *stx_build_insert_proc (THREAD_ENTRY * thread_p, char *tmp,
				    INSERT_PROC_NODE * ptr);
static char *stx_build_outptr_list (THREAD_ENTRY * thread_p, char *tmp,
				    OUTPTR_LIST * ptr);
static char *stx_build_selupd_list (THREAD_ENTRY * thread_p, char *tmp,
				    SELUPD_LIST * ptr);
static char *stx_build_pred_expr (THREAD_ENTRY * thread_p, char *tmp,
				  PRED_EXPR * ptr);
static char *stx_build_partition_info (THREAD_ENTRY * thread_p, char *tmp,
				       XASL_PARTITION_INFO * ptr);
static char *stx_build_parts_info (THREAD_ENTRY * thread_p, char *tmp,
				   XASL_PARTS_INFO * ptr);
static char *stx_build_pred (THREAD_ENTRY * thread_p, char *tmp, PRED * ptr);
static char *stx_build_eval_term (THREAD_ENTRY * thread_p, char *tmp,
				  EVAL_TERM * ptr);
static char *stx_build_comp_eval_term (THREAD_ENTRY * thread_p, char *tmp,
				       COMP_EVAL_TERM * ptr);
static char *stx_build_alsm_eval_term (THREAD_ENTRY * thread_p, char *tmp,
				       ALSM_EVAL_TERM * ptr);
static char *stx_build_like_eval_term (THREAD_ENTRY * thread_p, char *tmp,
				       LIKE_EVAL_TERM * ptr);
static char *stx_build_access_spec_type (THREAD_ENTRY * thread_p, char *tmp,
					 ACCESS_SPEC_TYPE * ptr);
static char *stx_build_indx_info (THREAD_ENTRY * thread_p, char *tmp,
				  INDX_INFO * ptr);
static char *stx_build_indx_id (THREAD_ENTRY * thread_p, char *tmp,
				INDX_ID * ptr);
static char *stx_build_key_info (THREAD_ENTRY * thread_p, char *tmp,
				 KEY_INFO * ptr);
static char *stx_build_cls_spec_type (THREAD_ENTRY * thread_p, char *tmp,
				      CLS_SPEC_TYPE * ptr);
static char *stx_build_list_spec_type (THREAD_ENTRY * thread_p, char *tmp,
				       LIST_SPEC_TYPE * ptr);
static char *stx_build_set_spec_type (THREAD_ENTRY * thread_p, char *tmp,
				      SET_SPEC_TYPE * ptr);
static char *stx_build_method_spec_type (THREAD_ENTRY * thread_p, char *tmp,
					 METHOD_SPEC_TYPE * ptr);
static char *stx_build_val_list (THREAD_ENTRY * thread_p, char *tmp,
				 VAL_LIST * ptr);
static char *stx_build_db_value_list (THREAD_ENTRY * thread_p, char *tmp,
				      QPROC_DB_VALUE_LIST ptr);
static char *stx_build_regu_variable (THREAD_ENTRY * thread_p, char *tmp,
				      REGU_VARIABLE * ptr);
static char *stx_build_attr_descr (THREAD_ENTRY * thread_p, char *tmp,
				   ATTR_DESCR * ptr);
static char *stx_build_pos_descr (char *tmp,
				  QFILE_TUPLE_VALUE_POSITION * ptr);
static char *stx_build_db_value (THREAD_ENTRY * thread_p, char *tmp,
				 DB_VALUE * ptr);
static char *stx_build_arith_type (THREAD_ENTRY * thread_p, char *tmp,
				   ARITH_TYPE * ptr);
static char *stx_build_aggregate_type (THREAD_ENTRY * thread_p, char *tmp,
				       AGGREGATE_TYPE * ptr);
static char *stx_build_function_type (THREAD_ENTRY * thread_p, char *tmp,
				      FUNCTION_TYPE * ptr);
static char *stx_build_srlist_id (THREAD_ENTRY * thread_p, char *tmp,
				  QFILE_SORTED_LIST_ID * ptr);
static char *stx_build_sort_list (THREAD_ENTRY * thread_p, char *tmp,
				  SORT_LIST * ptr);
static char *stx_build_connectby_proc (THREAD_ENTRY * thread_p, char *tmp,
				       CONNECTBY_PROC_NODE * ptr);

static int stx_mark_struct_visited (THREAD_ENTRY * thread_p, const void *ptr,
				    void *str);
static void *stx_get_struct_visited_ptr (THREAD_ENTRY * thread_p,
					 const void *ptr);
static void stx_free_visited_ptrs (THREAD_ENTRY * thread_p);
static char *stx_alloc_struct (THREAD_ENTRY * thread_p, int size);
static int stx_init_xasl_unpack_info (THREAD_ENTRY * thread_p,
				      char *xasl_stream,
				      int xasl_stream_size);
#if defined(ENABLE_UNUSED_FUNCTION)
static char *stx_unpack_char (char *tmp, char *ptr);
static char *stx_unpack_long (char *tmp, long *ptr);
#endif

/*
 * stx_map_stream_to_xasl () -
 *   return: if successful, return 0, otherwise non-zero error code
 *   xasl_tree(in)      : pointer to where to return the
 *                        root of the unpacked XASL tree
 *   xasl_stream(in)    : pointer to xasl stream
 *   xasl_stream_size(in)       : # of bytes in xasl_stream
 *   xasl_unpack_info_ptr(in)   : pointer to where to return the pack info
 *
 * Note: map the linear byte stream in disk representation to an XASL tree.
 *
 * Note: the caller is responsible for freeing the memory of
 * xasl_unpack_info_ptr. The free function is stx_free_xasl_unpack_info().
 */
int
stx_map_stream_to_xasl (THREAD_ENTRY * thread_p, XASL_NODE ** xasl_tree,
			char *xasl_stream, int xasl_stream_size,
			void **xasl_unpack_info_ptr)
{
  XASL_NODE *xasl;
  char *p;
  int header_size;
  int offset;

  if (!xasl_tree || !xasl_stream || !xasl_unpack_info_ptr
      || xasl_stream_size <= 0)
    {
      return ER_QPROC_INVALID_XASLNODE;
    }

  stx_set_xasl_errcode (thread_p, NO_ERROR);
  stx_init_xasl_unpack_info (thread_p, xasl_stream, xasl_stream_size);

  /* calculate offset to XASL tree in the stream buffer */
  p = or_unpack_int (xasl_stream, &header_size);
  offset = sizeof (int)		/* [size of header data] */
    + header_size		/* [header data] */
    + sizeof (int);		/* [size of body data] */
  offset = MAKE_ALIGN (offset);

  /* restore XASL tree from body data of the stream buffer */
  xasl = stx_restore_xasl_node (thread_p, xasl_stream + offset);
  if (!xasl)
    {
      stx_free_xasl_unpack_info (stx_get_xasl_unpack_info_ptr (thread_p));
      goto end;
    }

  /* set result */
  *xasl_tree = xasl;
  *xasl_unpack_info_ptr = stx_get_xasl_unpack_info_ptr (thread_p);

  /* restore header data of new XASL format */
  p = or_unpack_int (p, &xasl->dbval_cnt);
  OID_SET_NULL (&xasl->creator_oid);
  xasl->n_oid_list = 0;
  xasl->class_oid_list = NULL;
  xasl->repr_id_list = NULL;

  /* initialize the query in progress flag to FALSE.  Note that this flag
     is not packed/unpacked.  It is strictly a server side flag. */
  xasl->query_in_progress = false;

end:
  stx_free_visited_ptrs (thread_p);
#if defined(SERVER_MODE)
  stx_set_xasl_unpack_info_ptr (thread_p, NULL);
#endif /* SERVER_MODE */
  return stx_get_xasl_errcode (thread_p);
}

/*
 * stx_free_xasl_unpack_info () -
 *   return:
 *   xasl_unpack_info(in): unpack info returned by stx_map_stream_to_xasl ()
 *
 * Note: free the memory used for unpacking the xasl tree.
 */
void
stx_free_xasl_unpack_info (void *xasl_unpack_info)
{
#if defined (SERVER_MODE)
  if (xasl_unpack_info)
    {
      ((XASL_UNPACK_INFO *) xasl_unpack_info)->thrd = NULL;
    }
#endif /* SERVER_MODE */
}

/*
 * stx_restore_func_postfix () -
 *   return: if successful, return the offset of position
 *           in disk object where the node is stored, otherwise
 *           return ER_FAILED and error code is set to xasl_errcode.
 *   ptr(in): pointer to an XASL tree node whose type is return type
 *
 * Note: store the XASL tree node pointed by 'ptr' into disk
 * object with the help of stx_build_func_postfix to process
 * the members of the node.
 */

static AGGREGATE_TYPE *
stx_restore_aggregate_type (THREAD_ENTRY * thread_p, char *ptr)
{
  AGGREGATE_TYPE *aggregate;

  if (ptr == NULL)
    {
      return NULL;
    }

  aggregate = (AGGREGATE_TYPE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (aggregate != NULL)
    {
      return aggregate;
    }

  aggregate =
    (AGGREGATE_TYPE *) stx_alloc_struct (thread_p, sizeof (*aggregate));
  if (aggregate == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, aggregate) == ER_FAILED
      || stx_build_aggregate_type (thread_p, ptr, aggregate) == NULL)
    {
      return NULL;
    }

  return aggregate;
}

static FUNCTION_TYPE *
stx_restore_function_type (THREAD_ENTRY * thread_p, char *ptr)
{
  FUNCTION_TYPE *function;

  if (ptr == NULL)
    {
      return NULL;
    }

  function = (FUNCTION_TYPE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (function != NULL)
    {
      return function;
    }

  function =
    (FUNCTION_TYPE *) stx_alloc_struct (thread_p, sizeof (*function));
  if (function == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, function) == ER_FAILED
      || stx_build_function_type (thread_p, ptr, function) == NULL)
    {
      return NULL;
    }

  return function;
}

static QFILE_SORTED_LIST_ID *
stx_restore_srlist_id (THREAD_ENTRY * thread_p, char *ptr)
{
  QFILE_SORTED_LIST_ID *sort_list_id;

  if (ptr == NULL)
    {
      return NULL;
    }

  sort_list_id =
    (QFILE_SORTED_LIST_ID *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (sort_list_id != NULL)
    {
      return sort_list_id;
    }

  sort_list_id = (QFILE_SORTED_LIST_ID *)
    stx_alloc_struct (thread_p, sizeof (*sort_list_id));
  if (sort_list_id == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, sort_list_id) == ER_FAILED
      || stx_build_srlist_id (thread_p, ptr, sort_list_id) == NULL)
    {
      return NULL;
    }

  return sort_list_id;
}

static ARITH_TYPE *
stx_restore_arith_type (THREAD_ENTRY * thread_p, char *ptr)
{
  ARITH_TYPE *arithmetic;

  if (ptr == NULL)
    {
      return NULL;
    }

  arithmetic = (ARITH_TYPE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (arithmetic != NULL)
    {
      return arithmetic;
    }

  arithmetic =
    (ARITH_TYPE *) stx_alloc_struct (thread_p, sizeof (*arithmetic));
  if (arithmetic == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, arithmetic) == ER_FAILED
      || stx_build_arith_type (thread_p, ptr, arithmetic) == NULL)
    {
      return NULL;
    }

  return arithmetic;
}

static INDX_INFO *
stx_restore_indx_info (THREAD_ENTRY * thread_p, char *ptr)
{
  INDX_INFO *indx_info;

  if (ptr == NULL)
    {
      return NULL;
    }

  indx_info = (INDX_INFO *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (indx_info != NULL)
    {
      return indx_info;
    }

  indx_info = (INDX_INFO *) stx_alloc_struct (thread_p, sizeof (*indx_info));
  if (indx_info == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, indx_info) == ER_FAILED
      || stx_build_indx_info (thread_p, ptr, indx_info) == NULL)
    {
      return NULL;
    }

  return indx_info;
}

static OUTPTR_LIST *
stx_restore_outptr_list (THREAD_ENTRY * thread_p, char *ptr)
{
  OUTPTR_LIST *outptr_list;

  if (ptr == NULL)
    {
      return NULL;
    }

  outptr_list = (OUTPTR_LIST *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (outptr_list != NULL)
    {
      return outptr_list;
    }

  outptr_list =
    (OUTPTR_LIST *) stx_alloc_struct (thread_p, sizeof (*outptr_list));
  if (outptr_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, outptr_list) == ER_FAILED
      || stx_build_outptr_list (thread_p, ptr, outptr_list) == NULL)
    {
      return NULL;
    }

  return outptr_list;
}

static SELUPD_LIST *
stx_restore_selupd_list (THREAD_ENTRY * thread_p, char *ptr)
{
  SELUPD_LIST *selupd_list;

  if (ptr == NULL)
    {
      return NULL;
    }

  selupd_list = (SELUPD_LIST *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (selupd_list != NULL)
    {
      return selupd_list;
    }

  selupd_list =
    (SELUPD_LIST *) stx_alloc_struct (thread_p, sizeof (*selupd_list));
  if (selupd_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, selupd_list) == ER_FAILED
      || stx_build_selupd_list (thread_p, ptr, selupd_list) == NULL)
    {
      return NULL;
    }

  return selupd_list;
}

static PRED_EXPR *
stx_restore_pred_expr (THREAD_ENTRY * thread_p, char *ptr)
{
  PRED_EXPR *pred_expr;

  if (ptr == NULL)
    {
      return NULL;
    }

  pred_expr = (PRED_EXPR *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (pred_expr != NULL)
    {
      return pred_expr;
    }

  pred_expr = (PRED_EXPR *) stx_alloc_struct (thread_p, sizeof (*pred_expr));
  if (pred_expr == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, pred_expr) == ER_FAILED
      || stx_build_pred_expr (thread_p, ptr, pred_expr) == NULL)
    {
      return NULL;
    }

  return pred_expr;
}

static XASL_PARTITION_INFO *
stx_restore_partition_info (THREAD_ENTRY * thread_p, char *ptr)
{
  XASL_PARTITION_INFO *partition_info;

  if (ptr == NULL)
    {
      return NULL;
    }

  partition_info =
    (XASL_PARTITION_INFO *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (partition_info != NULL)
    {
      return partition_info;
    }

  partition_info = (XASL_PARTITION_INFO *)
    stx_alloc_struct (thread_p, sizeof (*partition_info));
  if (partition_info == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, partition_info) == ER_FAILED
      || stx_build_partition_info (thread_p, ptr, partition_info) == NULL)
    {
      return NULL;
    }

  return partition_info;
}

static XASL_PARTS_INFO *
stx_restore_parts_info (THREAD_ENTRY * thread_p, char *ptr)
{
  XASL_PARTS_INFO *parts_info;

  if (ptr == NULL)
    {
      return NULL;
    }

  parts_info = (XASL_PARTS_INFO *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (parts_info != NULL)
    {
      return parts_info;
    }

  parts_info =
    (XASL_PARTS_INFO *) stx_alloc_struct (thread_p, sizeof (*parts_info));
  if (parts_info == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, parts_info) == ER_FAILED
      || stx_build_parts_info (thread_p, ptr, parts_info) == NULL)
    {
      return NULL;
    }

  return parts_info;
}

static REGU_VARIABLE *
stx_restore_regu_variable (THREAD_ENTRY * thread_p, char *ptr)
{
  REGU_VARIABLE *regu_var;

  if (ptr == NULL)
    {
      return NULL;
    }

  regu_var = (REGU_VARIABLE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (regu_var != NULL)
    {
      return regu_var;
    }

  regu_var =
    (REGU_VARIABLE *) stx_alloc_struct (thread_p, sizeof (*regu_var));
  if (regu_var == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, regu_var) == ER_FAILED
      || stx_build_regu_variable (thread_p, ptr, regu_var) == NULL)
    {
      return NULL;
    }

  return regu_var;
}

static SORT_LIST *
stx_restore_sort_list (THREAD_ENTRY * thread_p, char *ptr)
{
  SORT_LIST *sort_list;

  if (ptr == NULL)
    {
      return NULL;
    }

  sort_list = (SORT_LIST *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (sort_list != NULL)
    {
      return sort_list;
    }

  sort_list = (SORT_LIST *) stx_alloc_struct (thread_p, sizeof (*sort_list));
  if (sort_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, sort_list) == ER_FAILED
      || stx_build_sort_list (thread_p, ptr, sort_list) == NULL)
    {
      return NULL;
    }

  return sort_list;
}

static VAL_LIST *
stx_restore_val_list (THREAD_ENTRY * thread_p, char *ptr)
{
  VAL_LIST *val_list;

  if (ptr == NULL)
    {
      return NULL;
    }

  val_list = (VAL_LIST *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (val_list != NULL)
    {
      return val_list;
    }

  val_list = (VAL_LIST *) stx_alloc_struct (thread_p, sizeof (*val_list));
  if (val_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, val_list) == ER_FAILED
      || stx_build_val_list (thread_p, ptr, val_list) == NULL)
    {
      return NULL;
    }

  return val_list;
}

static DB_VALUE *
stx_restore_db_value (THREAD_ENTRY * thread_p, char *ptr)
{
  DB_VALUE *value;

  if (ptr == NULL)
    {
      return NULL;
    }

  value = (DB_VALUE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (value != NULL)
    {
      return value;
    }

  value = (DB_VALUE *) stx_alloc_struct (thread_p, sizeof (*value));
  if (value == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, value) == ER_FAILED
      || stx_build_db_value (thread_p, ptr, value) == NULL)
    {
      return NULL;
    }

  return value;
}

static QPROC_DB_VALUE_LIST
stx_restore_db_value_list (THREAD_ENTRY * thread_p, char *ptr)
{
  QPROC_DB_VALUE_LIST value_list;

  if (ptr == NULL)
    {
      return NULL;
    }

  value_list =
    (QPROC_DB_VALUE_LIST) stx_get_struct_visited_ptr (thread_p, ptr);
  if (value_list != NULL)
    {
      return value_list;
    }

  value_list =
    (QPROC_DB_VALUE_LIST) stx_alloc_struct (thread_p, sizeof (*value_list));
  if (value_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, value_list) == ER_FAILED
      || stx_build_db_value_list (thread_p, ptr, value_list) == NULL)
    {
      return NULL;
    }

  return value_list;
}

static XASL_NODE *
stx_restore_xasl_node (THREAD_ENTRY * thread_p, char *ptr)
{
  XASL_NODE *xasl;

  if (ptr == NULL)
    {
      return NULL;
    }

  xasl = (XASL_NODE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (xasl != NULL)
    {
      return xasl;
    }

  xasl = (XASL_NODE *) stx_alloc_struct (thread_p, sizeof (*xasl));
  if (xasl == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, xasl) == ER_FAILED
      || stx_build_xasl_node (thread_p, ptr, xasl) == NULL)
    {
      return NULL;
    }

  return xasl;
}

static HEAP_CACHE_ATTRINFO *
stx_restore_cache_attrinfo (THREAD_ENTRY * thread_p, char *ptr)
{
  HEAP_CACHE_ATTRINFO *cache_attrinfo;

  if (ptr == NULL)
    {
      return NULL;
    }

  cache_attrinfo =
    (HEAP_CACHE_ATTRINFO *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (cache_attrinfo != NULL)
    {
      return cache_attrinfo;
    }

  cache_attrinfo = (HEAP_CACHE_ATTRINFO *)
    stx_alloc_struct (thread_p, sizeof (*cache_attrinfo));
  if (cache_attrinfo == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, cache_attrinfo) == ER_FAILED
      || stx_build_cache_attrinfo (ptr) == NULL)
    {
      return NULL;
    }

  return cache_attrinfo;
}

#if 0
/* there are currently no pointers to these type of structures in xasl
 * so there is no need to have a separate restore function.
 */

static MERGELIST_PROC_NODE *
stx_restore_merge_list_info (char *ptr)
{
  MERGELIST_PROC_NODE *merge_list_info;

  if (ptr == NULL)
    {
      return NULL;
    }

  merge_list_info = stx_get_struct_visited_ptr (thread_p, ptr);
  if (merge_list_info != NULL)
    {
      return merge_list_info;
    }

  merge_list_info =
    (MERGELIST_PROC_NODE *) stx_alloc_struct (sizeof (*merge_list_info));
  if (merge_list_info == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (ptr, merge_list_info) == ER_FAILED
      || stx_build_merge_list_info (ptr, merge_list_info) == NULL)
    {
      return NULL;
    }

  return merge_list_info;
}

static QFILE_LIST_MERGE_INFO *
stx_restore_ls_merge_info (char *ptr)
{
  QFILE_LIST_MERGE_INFO *list_merge_info;

  if (ptr == NULL)
    {
      return NULL;
    }

  list_merge_info = stx_get_struct_visited_ptr (thread_p, ptr);
  if (list_merge_info != NULL)
    {
      return list_merge_info;
    }

  list_merge_info = (QFILE_LIST_MERGE_INFO *)
    stx_alloc_struct (sizeof (*list_merge_info));
  if (list_merge_info == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (ptr, list_merge_info) == ER_FAILED
      || stx_build_ls_merge_info (ptr, list_merge_info) == NULL)
    {
      return NULL;
    }

  return list_merge_info;
}

static UPDATE_PROC_NODE *
stx_restore_update_info (char *ptr)
{
  UPDATE_PROC_NODE *update_info;

  if (ptr == NULL)
    {
      return NULL;
    }

  update_info = stx_get_struct_visited_ptr (thread_p, ptr);
  if (update_info != NULL)
    {
      return update_info;
    }

  update_info = (UPDATE_PROC_NODE *) stx_alloc_struct (sizeof (*update_info));
  if (update_info == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (ptr, update_info) == ER_FAILED
      || stx_build_update_info (ptr, update_info) == NULL)
    {
      return NULL;
    }

  return update_info;
}

static DELETE_PROC_NODE *
stx_restore_delete_info (char *ptr)
{
  DELETE_PROC_NODE *delete_info;

  if (ptr == NULL)
    {
      return NULL;
    }

  delete_info = stx_get_struct_visited_ptr (thread_p, ptr);
  if (delete_info != NULL)
    {
      return delete_info;
    }

  delete_info = (DELETE_PROC_NODE *) stx_alloc_struct (sizeof (*delete_info));
  if (delete_info == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (ptr, delete_info) == ER_FAILED
      || stx_build_delete_info (ptr, delete_info) == NULL)
    {
      return NULL;
    }

  return delete_info;
}

static INSERT_PROC_NODE *
stx_restore_insert_info (char *ptr)
{
  INSERT_PROC_NODE *insert_info;

  if (ptr == NULL)
    {
      return NULL;
    }

  insert_info = stx_get_struct_visited_ptr (thread_p, ptr);
  if (insert_info != NULL)
    {
      return insert_info;
    }

  insert_info = (INSERT_PROC_NODE *) stx_alloc_struct (sizeof (*insert_info));
  if (insert_info == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (ptr, insert_info) == ER_FAILED
      || stx_build_insert_info (ptr, insert_info) == NULL)
    {
      return NULL;
    }

  return insert_info;
}
#endif /* 0 */

static QFILE_LIST_ID *
stx_restore_list_id (THREAD_ENTRY * thread_p, char *ptr)
{
  QFILE_LIST_ID *list_id;

  if (ptr == NULL)
    {
      return NULL;
    }

  list_id = (QFILE_LIST_ID *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (list_id != NULL)
    {
      return list_id;
    }

  or_unpack_listid (ptr, (void **) &list_id);
  if (list_id == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  if (stx_mark_struct_visited (thread_p, ptr, list_id) == ER_FAILED)
    {
      return NULL;
    }

  return list_id;
}

static METHOD_SIG_LIST *
stx_restore_method_sig_list (THREAD_ENTRY * thread_p, char *ptr)
{
  METHOD_SIG_LIST *method_sig_list;

  if (ptr == NULL)
    {
      return NULL;
    }

  method_sig_list =
    (METHOD_SIG_LIST *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (method_sig_list != NULL)
    {
      return method_sig_list;
    }

  or_unpack_method_sig_list (ptr, (void **) &method_sig_list);
  if (method_sig_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  if (stx_mark_struct_visited (thread_p, ptr, method_sig_list) == ER_FAILED)
    {
      return NULL;
    }

  return method_sig_list;
}

static char *
stx_restore_string (THREAD_ENTRY * thread_p, char *ptr)
{
  char *string;

  string = (char *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (string != NULL)
    {
      return string;
    }

  or_unpack_string (ptr, &string);
  if (string == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  if (stx_mark_struct_visited (thread_p, ptr, string) == ER_FAILED)
    {
      return NULL;
    }

  return string;
}

static DB_VALUE **
stx_restore_db_value_array_extra (THREAD_ENTRY * thread_p, char *ptr,
				  int nelements, int total_nelements)
{
  DB_VALUE **value_array;
  int *ints;
  int i;
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  value_array = (DB_VALUE **)
    stx_alloc_struct (thread_p, sizeof (DB_VALUE *) * total_nelements);
  if (value_array == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  ints = stx_restore_int_array (thread_p, ptr, nelements);
  if (ints == NULL)
    {
      return NULL;
    }

  for (i = 0; i < nelements; i++)
    {
      offset = ints[i];
      value_array[i] =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (value_array[i] == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  for (; i < total_nelements; ++i)
    {
      value_array[i] = NULL;
    }
  return value_array;
}

#if defined(ENABLE_UNUSED_FUNCTION)
static TP_DOMAIN **
stx_restore_domain_array (THREAD_ENTRY * thread_p, char *ptr, int nelements)
{
  int i;
  TP_DOMAIN **domains;

  domains =
    (TP_DOMAIN **) stx_alloc_struct (thread_p,
				     nelements * sizeof (TP_DOMAIN *));
  if (domains == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  for (i = 0; i < nelements; ++i)
    {
      ptr = or_unpack_domain (ptr, &domains[i], NULL);
    }

  return domains;
}
#endif

static int *
stx_restore_int_array (THREAD_ENTRY * thread_p, char *ptr, int nelements)
{
  int *int_array;
  int i;

  int_array = (int *) stx_alloc_struct (thread_p, sizeof (int) * nelements);
  if (int_array == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  for (i = 0; i < nelements; ++i)
    {
      ptr = or_unpack_int (ptr, &int_array[i]);
    }

  return int_array;
}

static HFID *
stx_restore_hfid_array (THREAD_ENTRY * thread_p, char *ptr, int nelements)
{
  HFID *hfid_array;
  int i;

  hfid_array =
    (HFID *) stx_alloc_struct (thread_p, sizeof (HFID) * nelements);
  if (hfid_array == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  for (i = 0; i < nelements; ++i)
    {
      ptr = or_unpack_hfid (ptr, &hfid_array[i]);
    }

  return hfid_array;
}

static OID *
stx_restore_OID_array (THREAD_ENTRY * thread_p, char *ptr, int nelements)
{
  OID *oid_array;
  int i;

  oid_array = (OID *) stx_alloc_struct (thread_p, sizeof (OID) * nelements);
  if (oid_array == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  for (i = 0; i < nelements; i++)
    {
      ptr = or_unpack_oid (ptr, &oid_array[i]);
    }

  return oid_array;
}

#if defined(ENABLE_UNUSED_FUNCTION)
static char *
stx_restore_input_vals (THREAD_ENTRY * thread_p, char *ptr, int nelements)
{
  char *input_vals;

  input_vals = stx_alloc_struct (thread_p, nelements);
  if (input_vals == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  memmove (input_vals, ptr, nelements);

  return input_vals;
}
#endif

static XASL_PARTS_INFO **
stx_restore_parts_array (THREAD_ENTRY * thread_p, char *ptr, int size)
{
  XASL_PARTS_INFO **parts_info_array;
  int *offset_array;
  int i;
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  parts_info_array =
    (XASL_PARTS_INFO **) stx_alloc_struct (thread_p,
					   sizeof (XASL_PARTS_INFO *) * size);
  if (parts_info_array == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  offset_array = stx_restore_int_array (thread_p, ptr, size);
  if (offset_array == NULL)
    {
      return NULL;
    }

  for (i = 0; i < size; i++)
    {
      offset = offset_array[i];
      parts_info_array[i] =
	stx_restore_parts_info (thread_p,
				&xasl_unpack_info->packed_xasl[offset]);
      if (parts_info_array[i] == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return parts_info_array;
}

static XASL_PARTITION_INFO **
stx_restore_partition_array (THREAD_ENTRY * thread_p, char *ptr, int size)
{
  XASL_PARTITION_INFO **partition_info_array;
  int *offset_array;
  int i;
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  partition_info_array =
    (XASL_PARTITION_INFO **) stx_alloc_struct (thread_p,
					       sizeof (XASL_PARTITION_INFO *)
					       * size);
  if (partition_info_array == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  offset_array = stx_restore_int_array (thread_p, ptr, size);
  if (offset_array == NULL)
    {
      return NULL;
    }

  for (i = 0; i < size; i++)
    {
      offset = offset_array[i];
      if (offset <= 0)
	{
	  partition_info_array[i] = NULL;
	  continue;
	}
      partition_info_array[i] =
	stx_restore_partition_info (thread_p,
				    &xasl_unpack_info->packed_xasl[offset]);
      if (partition_info_array[i] == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return (partition_info_array);
}

/*
 * Restore the regu_variable_list as an array to avoid recursion in the server.
 * The array size is restored first, then the array.
 */
static REGU_VARIABLE_LIST
stx_restore_regu_variable_list (THREAD_ENTRY * thread_p, char *ptr)
{
  REGU_VARIABLE_LIST regu_var_list;
  int offset;
  int total;
  int i;
  char *ptr2;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  regu_var_list =
    (REGU_VARIABLE_LIST) stx_get_struct_visited_ptr (thread_p, ptr);
  if (regu_var_list != NULL)
    {
      return regu_var_list;
    }

  ptr2 = or_unpack_int (ptr, &total);
  regu_var_list = (REGU_VARIABLE_LIST)
    stx_alloc_struct (thread_p,
		      sizeof (struct regu_variable_list_node) * total);
  if (regu_var_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  if (stx_mark_struct_visited (thread_p, ptr, regu_var_list) == ER_FAILED)
    {
      return NULL;
    }

  ptr = ptr2;
  for (i = 0; i < total; i++)
    {
      ptr = or_unpack_int (ptr, &offset);
      if (stx_build_regu_variable
	  (thread_p, &xasl_unpack_info->packed_xasl[offset],
	   &regu_var_list[i].value) == NULL)
	{
	  return NULL;
	}

      if (i < total - 1)
	{
	  regu_var_list[i].next = (struct regu_variable_list_node *)
	    &regu_var_list[i + 1];
	}
      else
	{
	  regu_var_list[i].next = NULL;
	}
    }

  return regu_var_list;
}

static REGU_VARLIST_LIST
stx_restore_regu_varlist_list (THREAD_ENTRY * thread_p, char *ptr)
{
  REGU_VARLIST_LIST regu_var_list_list;
  int offset;
  int total;
  int i;
  char *ptr2;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  regu_var_list_list =
    (REGU_VARLIST_LIST) stx_get_struct_visited_ptr (thread_p, ptr);
  if (regu_var_list_list != NULL)
    {
      return regu_var_list_list;
    }

  ptr2 = or_unpack_int (ptr, &total);
  regu_var_list_list = (REGU_VARLIST_LIST)
    stx_alloc_struct (thread_p,
		      sizeof (struct regu_varlist_list_node) * total);
  if (regu_var_list_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  if (stx_mark_struct_visited (thread_p, ptr, regu_var_list_list) ==
      ER_FAILED)
    {
      return NULL;
    }

  ptr = ptr2;
  for (i = 0; i < total; i++)
    {
      ptr = or_unpack_int (ptr, &offset);
      regu_var_list_list[i].list =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (i < total - 1)
	{
	  regu_var_list_list[i].next =
	    (REGU_VARLIST_LIST) (&regu_var_list_list[i + 1]);
	}
      else
	{
	  regu_var_list_list[i].next = NULL;
	}
    }

  return regu_var_list_list;
}

static KEY_RANGE *
stx_restore_key_range_array (THREAD_ENTRY * thread_p, char *ptr,
			     int nelements)
{
  KEY_RANGE *key_range_array;
  int *ints;
  int i, j, offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  key_range_array = (KEY_RANGE *)
    stx_alloc_struct (thread_p, sizeof (KEY_RANGE) * nelements);
  if (key_range_array == NULL)
    {
      goto error;
    }

  ints = stx_restore_int_array (thread_p, ptr, 3 * nelements);
  if (ints == NULL)
    {
      return NULL;
    }

  for (i = 0, j = 0; i < nelements; i++, j++)
    {
      key_range_array[i].range = (RANGE) ints[j];

      offset = ints[++j];
      if (offset)
	{
	  key_range_array[i].key1 =
	    stx_restore_regu_variable (thread_p, &xasl_unpack_info->
				       packed_xasl[offset]);
	  if (key_range_array[i].key1 == NULL)
	    {
	      goto error;
	    }
	}
      else
	{
	  key_range_array[i].key1 = NULL;
	}

      offset = ints[++j];
      if (offset)
	{
	  key_range_array[i].key2 =
	    stx_restore_regu_variable (thread_p, &xasl_unpack_info->
				       packed_xasl[offset]);
	  if (key_range_array[i].key2 == NULL)
	    {
	      goto error;
	    }
	}
      else
	{
	  key_range_array[i].key2 = NULL;
	}
    }

  return key_range_array;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

/*
 * Restore access spec type list as an array to avoid recursion in the server.
 * The array size is restored first, then the array.
 */
static ACCESS_SPEC_TYPE *
stx_restore_access_spec_type (THREAD_ENTRY * thread_p, char **ptr)
{
  ACCESS_SPEC_TYPE *access_spec_type = NULL;
  int total, i;

  *ptr = or_unpack_int (*ptr, &total);
  if (total > 0)
    {
      access_spec_type = (ACCESS_SPEC_TYPE *)
	stx_alloc_struct (thread_p, sizeof (ACCESS_SPEC_TYPE) * total);
      if (access_spec_type == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  for (i = 0; i < total; i++)
    {
      *ptr =
	stx_build_access_spec_type (thread_p, *ptr, &access_spec_type[i]);
      if (*ptr == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
      if (i < total - 1)
	{
	  access_spec_type[i].next =
	    (ACCESS_SPEC_TYPE *) (&access_spec_type[i + 1]);
	}
      else
	{
	  access_spec_type[i].next = NULL;
	}
    }

  return access_spec_type;
}

static char *
stx_build_xasl_node (THREAD_ENTRY * thread_p, char *ptr, XASL_NODE * xasl)
{
  int offset;
  int tmp;

  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  /* initialize query_in_progress flag */
  xasl->query_in_progress = false;

  ptr = or_unpack_int (ptr, (int *) &xasl->type);

  ptr = or_unpack_int (ptr, (int *) &xasl->flag);

  /* initialize xasl status */
  xasl->status = XASL_CLEARED;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->list_id = NULL;
    }
  else
    {
      xasl->list_id =
	stx_restore_list_id (thread_p,
			     &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->list_id == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->after_iscan_list = NULL;
    }
  else
    {
      xasl->after_iscan_list =
	stx_restore_sort_list (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->after_iscan_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->orderby_list = NULL;
    }
  else
    {
      xasl->orderby_list =
	stx_restore_sort_list (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->orderby_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->ordbynum_pred = NULL;
    }
  else
    {
      xasl->ordbynum_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->ordbynum_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->ordbynum_val = NULL;
    }
  else
    {
      xasl->ordbynum_val =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->ordbynum_val == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &xasl->ordbynum_flag);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->single_tuple = NULL;
    }
  else
    {
      xasl->single_tuple =
	stx_restore_val_list (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->single_tuple == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &xasl->is_single_tuple);

  ptr = or_unpack_int (ptr, (int *) &xasl->option);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->outptr_list = NULL;
    }
  else
    {
      xasl->outptr_list =
	stx_restore_outptr_list (thread_p,
				 &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->outptr_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->remote_outptr_list = NULL;
    }
  else
    {
      xasl->remote_outptr_list =
	stx_restore_outptr_list (thread_p,
				 &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->remote_outptr_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->selected_upd_list = NULL;
    }
  else
    {
      xasl->selected_upd_list =
	stx_restore_selupd_list (thread_p,
				 &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->selected_upd_list == NULL)
	{
	  goto error;
	}
    }

  xasl->spec_list = stx_restore_access_spec_type (thread_p, &ptr);

  xasl->merge_spec = stx_restore_access_spec_type (thread_p, &ptr);
  if (ptr == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->val_list = NULL;
    }
  else
    {
      xasl->val_list =
	stx_restore_val_list (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->val_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->merge_val_list = NULL;
    }
  else
    {
      xasl->merge_val_list =
	stx_restore_val_list (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->merge_val_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->aptr_list = NULL;
    }
  else
    {
      xasl->aptr_list =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->aptr_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->bptr_list = NULL;
    }
  else
    {
      xasl->bptr_list =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->bptr_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->dptr_list = NULL;
    }
  else
    {
      xasl->dptr_list =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->dptr_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->after_join_pred = NULL;
    }
  else
    {
      xasl->after_join_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->after_join_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->if_pred = NULL;
    }
  else
    {
      xasl->if_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->if_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->instnum_pred = NULL;
    }
  else
    {
      xasl->instnum_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->instnum_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->instnum_val = NULL;
    }
  else
    {
      xasl->instnum_val =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->instnum_val == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &xasl->instnum_flag);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->fptr_list = NULL;
    }
  else
    {
      xasl->fptr_list =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->fptr_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->scan_ptr = NULL;
    }
  else
    {
      xasl->scan_ptr =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->scan_ptr == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->connect_by_ptr = NULL;
    }
  else
    {
      xasl->connect_by_ptr =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->connect_by_ptr == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->level_val = NULL;
    }
  else
    {
      xasl->level_val =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->level_val == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->level_regu = NULL;
    }
  else
    {
      xasl->level_regu =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->level_regu == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->isleaf_val = NULL;
    }
  else
    {
      xasl->isleaf_val =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->isleaf_val == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->isleaf_regu = NULL;
    }
  else
    {
      xasl->isleaf_regu =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->isleaf_regu == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->iscycle_val = NULL;
    }
  else
    {
      xasl->iscycle_val =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->iscycle_val == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->iscycle_regu = NULL;
    }
  else
    {
      xasl->iscycle_regu =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->iscycle_regu == NULL)
	{
	  goto error;
	}
    }

  xasl->curr_spec = stx_restore_access_spec_type (thread_p, &ptr);

  ptr = or_unpack_int (ptr, &xasl->next_scan_on);

  ptr = or_unpack_int (ptr, &xasl->next_scan_block_on);

  ptr = or_unpack_int (ptr, &xasl->cat_fetched);

  ptr = or_unpack_int (ptr, &xasl->composite_locking);

  /*
   * Note that the composite lock block is strictly a server side block
   * and was not packed.  We'll simply clear the memory.
   */
  memset (&xasl->composite_lock, 0, sizeof (LK_COMPOSITE_LOCK));

  switch (xasl->type)
    {
    case CONNECTBY_PROC:
      ptr = stx_build_connectby_proc (thread_p, ptr, &xasl->proc.connect_by);
      break;

    case BUILDLIST_PROC:
      ptr = stx_build_buildlist_proc (thread_p, ptr, &xasl->proc.buildlist);
      break;

    case BUILDVALUE_PROC:
      ptr = stx_build_buildvalue_proc (thread_p, ptr, &xasl->proc.buildvalue);
      break;

    case MERGELIST_PROC:
      ptr = stx_build_mergelist_proc (thread_p, ptr, &xasl->proc.mergelist);
      break;

    case UPDATE_PROC:
      ptr = stx_build_update_proc (thread_p, ptr, &xasl->proc.update);
      break;

    case DELETE_PROC:
      ptr = stx_build_delete_proc (thread_p, ptr, &xasl->proc.delete_);
      break;

    case INSERT_PROC:
      ptr = stx_build_insert_proc (thread_p, ptr, &xasl->proc.insert);
      break;

    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:
      ptr = stx_build_union_proc (thread_p, ptr, &xasl->proc.union_);
      break;

    case OBJFETCH_PROC:
    case SETFETCH_PROC:
      ptr = stx_build_fetch_proc (thread_p, ptr, &xasl->proc.fetch);
      break;

    case SCAN_PROC:
      break;

    case DO_PROC:
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_unpack_int (ptr, (int *) &xasl->projected_size);
  ptr = or_unpack_double (ptr, (double *) &xasl->cardinality);

  ptr = or_unpack_int (ptr, &tmp);
  xasl->iscan_oid_order = (bool) tmp;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->qstmt = NULL;
    }
  else
    {
      xasl->qstmt =
	stx_restore_string (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->qstmt == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->next = NULL;
    }
  else
    {
      xasl->next =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->next == NULL)
	{
	  goto error;
	}
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_cache_attrinfo (char *ptr)
{
  int dummy;

  /* unpack the zero int that is sent mainly as a placeholder */
  ptr = or_unpack_int (ptr, &dummy);

  return ptr;
}

static char *
stx_build_union_proc (THREAD_ENTRY * thread_p, char *ptr,
		      UNION_PROC_NODE * union_proc)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      union_proc->left = NULL;
    }
  else
    {
      union_proc->left =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (union_proc->left == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      union_proc->right = NULL;
    }
  else
    {
      union_proc->right =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (union_proc->right == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return ptr;
}

static char *
stx_build_fetch_proc (THREAD_ENTRY * thread_p, char *ptr,
		      FETCH_PROC_NODE * obj_set_fetch_proc)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);
  int i;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      obj_set_fetch_proc->arg = NULL;
    }
  else
    {
      obj_set_fetch_proc->arg =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (obj_set_fetch_proc->arg == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &i);
  obj_set_fetch_proc->fetch_res = (bool) i;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      obj_set_fetch_proc->set_pred = NULL;
    }
  else
    {
      obj_set_fetch_proc->set_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (obj_set_fetch_proc->set_pred == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &i);
  obj_set_fetch_proc->ql_flag = (bool) i;

  return ptr;
}

static char *
stx_build_buildlist_proc (THREAD_ENTRY * thread_p, char *ptr,
			  BUILDLIST_PROC_NODE * stx_build_list_proc)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  stx_build_list_proc->output_columns = (DB_VALUE **) 0;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->eptr_list = NULL;
    }
  else
    {
      stx_build_list_proc->eptr_list =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->eptr_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->groupby_list = NULL;
    }
  else
    {
      stx_build_list_proc->groupby_list =
	stx_restore_sort_list (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->groupby_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->after_groupby_list = NULL;
    }
  else
    {
      stx_build_list_proc->after_groupby_list =
	stx_restore_sort_list (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->after_groupby_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_outptr_list = NULL;
    }
  else
    {
      stx_build_list_proc->g_outptr_list =
	stx_restore_outptr_list (thread_p,
				 &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_outptr_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_regu_list = NULL;
    }
  else
    {
      stx_build_list_proc->g_regu_list =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (stx_build_list_proc->g_regu_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_val_list = NULL;
    }
  else
    {
      stx_build_list_proc->g_val_list =
	stx_restore_val_list (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_val_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_having_pred = NULL;
    }
  else
    {
      stx_build_list_proc->g_having_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_having_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_grbynum_pred = NULL;
    }
  else
    {
      stx_build_list_proc->g_grbynum_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_grbynum_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_grbynum_val = NULL;
    }
  else
    {
      stx_build_list_proc->g_grbynum_val =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_grbynum_val == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &stx_build_list_proc->g_grbynum_flag);
  ptr = or_unpack_int (ptr, (int *) &stx_build_list_proc->g_with_rollup);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_agg_list = NULL;
    }
  else
    {
      stx_build_list_proc->g_agg_list =
	stx_restore_aggregate_type (thread_p,
				    &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_agg_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_outarith_list = NULL;
    }
  else
    {
      stx_build_list_proc->g_outarith_list =
	stx_restore_arith_type (thread_p,
				&xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_outarith_list == NULL)
	{
	  goto error;
	}
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_buildvalue_proc (THREAD_ENTRY * thread_p, char *ptr,
			   BUILDVALUE_PROC_NODE * stx_build_value_proc)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_value_proc->having_pred = NULL;
    }
  else
    {
      stx_build_value_proc->having_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_value_proc->having_pred == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_value_proc->grbynum_val = NULL;
    }
  else
    {
      stx_build_value_proc->grbynum_val =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_value_proc->grbynum_val == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_value_proc->agg_list = NULL;
    }
  else
    {
      stx_build_value_proc->agg_list =
	stx_restore_aggregate_type (thread_p,
				    &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_value_proc->agg_list == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_value_proc->outarith_list = NULL;
    }
  else
    {
      stx_build_value_proc->outarith_list =
	stx_restore_arith_type (thread_p,
				&xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_value_proc->outarith_list == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &stx_build_value_proc->is_always_false);

  return ptr;
}

static char *
stx_build_mergelist_proc (THREAD_ENTRY * thread_p, char *ptr,
			  MERGELIST_PROC_NODE * merge_list_info)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      merge_list_info->outer_xasl = NULL;
    }
  else
    {
      merge_list_info->outer_xasl =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (merge_list_info->outer_xasl == NULL)
	{
	  goto error;
	}
    }

  merge_list_info->outer_spec_list =
    stx_restore_access_spec_type (thread_p, &ptr);
  if (ptr == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      merge_list_info->outer_val_list = NULL;
    }
  else
    {
      merge_list_info->outer_val_list =
	stx_restore_val_list (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (merge_list_info->outer_val_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      merge_list_info->inner_xasl = NULL;
    }
  else
    {
      merge_list_info->inner_xasl =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (merge_list_info->inner_xasl == NULL)
	{
	  goto error;
	}
    }

  merge_list_info->inner_spec_list =
    stx_restore_access_spec_type (thread_p, &ptr);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      merge_list_info->inner_val_list = NULL;
    }
  else
    {
      merge_list_info->inner_val_list =
	stx_restore_val_list (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (merge_list_info->inner_val_list == NULL)
	{
	  goto error;
	}
    }

  ptr = stx_build_ls_merge_info (thread_p, ptr, &merge_list_info->ls_merge);

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_ls_merge_info (THREAD_ENTRY * thread_p, char *ptr,
			 QFILE_LIST_MERGE_INFO * list_merge_info)
{
  int offset;
  int single_fetch;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, (int *) &list_merge_info->join_type);

  ptr = or_unpack_int (ptr, &single_fetch);
  list_merge_info->single_fetch = (QPROC_SINGLE_FETCH) single_fetch;

  ptr = or_unpack_int (ptr, &list_merge_info->ls_column_cnt);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || list_merge_info->ls_column_cnt == 0)
    {
      list_merge_info->ls_outer_column = NULL;
    }
  else
    {
      list_merge_info->ls_outer_column =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       list_merge_info->ls_column_cnt);
      if (list_merge_info->ls_outer_column == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || list_merge_info->ls_column_cnt == 0)
    {
      list_merge_info->ls_outer_unique = NULL;
    }
  else
    {
      list_merge_info->ls_outer_unique =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       list_merge_info->ls_column_cnt);
      if (list_merge_info->ls_outer_unique == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || list_merge_info->ls_column_cnt == 0)
    {
      list_merge_info->ls_inner_column = NULL;
    }
  else
    {
      list_merge_info->ls_inner_column =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       list_merge_info->ls_column_cnt);
      if (list_merge_info->ls_inner_column == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if ((offset == 0) || (list_merge_info->ls_column_cnt == 0))
    {
      list_merge_info->ls_inner_unique = NULL;
    }
  else
    {
      list_merge_info->ls_inner_unique = stx_restore_int_array
	(thread_p, &xasl_unpack_info->packed_xasl[offset],
	 list_merge_info->ls_column_cnt);
      if (list_merge_info->ls_inner_unique == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &list_merge_info->ls_pos_cnt);

  ptr = or_unpack_int (ptr, &offset);
  if ((offset == 0) || (list_merge_info->ls_pos_cnt == 0))
    {
      list_merge_info->ls_pos_list = NULL;
    }
  else
    {
      list_merge_info->ls_pos_list =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       list_merge_info->ls_pos_cnt);
      if (list_merge_info->ls_pos_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || list_merge_info->ls_pos_cnt == 0)
    {
      list_merge_info->ls_outer_inner_list = NULL;
    }
  else
    {
      list_merge_info->ls_outer_inner_list =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       list_merge_info->ls_pos_cnt);
      if (list_merge_info->ls_outer_inner_list == NULL)
	{
	  goto error;
	}
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_update_proc (THREAD_ENTRY * thread_p, char *ptr,
		       UPDATE_PROC_NODE * update_info)
{
  int offset;
  int i;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &update_info->no_classes);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || update_info->no_classes == 0)
    {
      update_info->class_oid = NULL;
    }
  else
    {
      update_info->class_oid =
	stx_restore_OID_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       update_info->no_classes);
      if (update_info->class_oid == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || update_info->no_classes == 0)
    {
      update_info->class_hfid = NULL;
    }
  else
    {
      update_info->class_hfid =
	stx_restore_hfid_array (thread_p,
				&xasl_unpack_info->packed_xasl[offset],
				update_info->no_classes);
      if (update_info->class_hfid == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || update_info->no_classes == 0)
    {
      update_info->node_ids = NULL;
    }
  else
    {
      update_info->node_ids =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       update_info->no_classes);
      if (update_info->node_ids == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &update_info->no_vals);

  ptr = or_unpack_int (ptr, &update_info->no_consts);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0
      || update_info->no_vals == 0 || update_info->no_classes == 0)
    {
      update_info->att_id = NULL;
    }
  else
    {
      update_info->att_id =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       update_info->no_classes *
			       update_info->no_vals);
      if (update_info->att_id == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      update_info->consts = (DB_VALUE **)
	stx_alloc_struct (thread_p,
			  sizeof (DB_VALUE *) * update_info->no_vals);
      if (update_info->consts == NULL)
	{
	  goto error;
	}
      for (i = 0; i < update_info->no_vals; ++i)
	{
	  update_info->consts[i] = (DB_VALUE *) 0;
	}
    }
  else
    {
      /* Make extra space after the constants for the attribute expression
         values. */
      update_info->consts =
	stx_restore_db_value_array_extra (thread_p, &xasl_unpack_info->
					  packed_xasl[offset],
					  update_info->no_consts,
					  update_info->no_vals);
      if (update_info->consts == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      update_info->cons_pred = NULL;
    }
  else
    {
      update_info->cons_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (update_info->cons_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &update_info->has_uniques);

  ptr = or_unpack_int (ptr, &update_info->waitsecs);
  ptr = or_unpack_int (ptr, &update_info->no_logging);
  ptr = or_unpack_int (ptr, &update_info->release_lock);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || update_info->no_classes == 0)
    {
      update_info->partition = NULL;
    }
  else
    {
      update_info->partition =
	stx_restore_partition_array (thread_p,
				     &xasl_unpack_info->packed_xasl[offset],
				     update_info->no_classes);
      if (update_info->partition == NULL)
	{
	  goto error;
	}
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_delete_proc (THREAD_ENTRY * thread_p, char *ptr,
		       DELETE_PROC_NODE * delete_info)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &delete_info->no_classes);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || delete_info->no_classes == 0)
    {
      delete_info->class_oid = NULL;
    }
  else
    {
      delete_info->class_oid =
	stx_restore_OID_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       delete_info->no_classes);
      if (delete_info->class_oid == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || delete_info->no_classes == 0)
    {
      delete_info->class_hfid = NULL;
    }
  else
    {
      delete_info->class_hfid =
	stx_restore_hfid_array (thread_p,
				&xasl_unpack_info->packed_xasl[offset],
				delete_info->no_classes);
      if (delete_info->class_hfid == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || delete_info->no_classes == 0)
    {
      delete_info->node_ids = NULL;
    }
  else
    {
      delete_info->node_ids =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       delete_info->no_classes);
      if (delete_info->node_ids == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &delete_info->waitsecs);
  ptr = or_unpack_int (ptr, &delete_info->no_logging);
  ptr = or_unpack_int (ptr, &delete_info->release_lock);

  return ptr;
}

static char *
stx_build_insert_proc (THREAD_ENTRY * thread_p, char *ptr,
		       INSERT_PROC_NODE * insert_info)
{
  int offset;
  int i;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_oid (ptr, &insert_info->class_oid);

  ptr = or_unpack_oid (ptr, &insert_info->proxy_oid);

  ptr = or_unpack_hfid (ptr, &insert_info->class_hfid);

  ptr = or_unpack_int (ptr, &insert_info->node_id);

  ptr = or_unpack_int (ptr, &insert_info->no_vals);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || insert_info->no_vals == 0)
    {
      insert_info->att_id = NULL;
    }
  else
    {
      insert_info->att_id =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       insert_info->no_vals);
      if (insert_info->att_id == NULL)
	{
	  goto error;
	}
    }

  /* Make space for the subquery values. */
  insert_info->vals =
    (DB_VALUE **) stx_alloc_struct (thread_p, sizeof (DB_VALUE *) *
				    insert_info->no_vals);
  if (insert_info->no_vals)
    {
      if (insert_info->vals == NULL)
	{
	  goto error;
	}
      for (i = 0; i < insert_info->no_vals; ++i)
	{
	  insert_info->vals[i] = (DB_VALUE *) 0;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      insert_info->cons_pred = NULL;
    }
  else
    {
      insert_info->cons_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (insert_info->cons_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &insert_info->has_uniques);
  ptr = or_unpack_int (ptr, &insert_info->waitsecs);
  ptr = or_unpack_int (ptr, &insert_info->no_logging);
  ptr = or_unpack_int (ptr, &insert_info->release_lock);
  ptr = or_unpack_int (ptr, &insert_info->do_replace);
  ptr = or_unpack_int (ptr, &insert_info->dup_key_oid_var_index);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      insert_info->partition = NULL;
    }
  else
    {
      insert_info->partition =
	stx_restore_partition_info (thread_p,
				    &xasl_unpack_info->packed_xasl[offset]);
      if (insert_info->partition == NULL)
	{
	  goto error;
	}
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_outptr_list (THREAD_ENTRY * thread_p, char *ptr,
		       OUTPTR_LIST * outptr_list)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &outptr_list->valptr_cnt);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      outptr_list->valptrp = NULL;
    }
  else
    {
      outptr_list->valptrp =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (outptr_list->valptrp == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return ptr;
}

static char *
stx_build_selupd_list (THREAD_ENTRY * thread_p, char *ptr,
		       SELUPD_LIST * selupd_list)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_oid (ptr, &selupd_list->class_oid);
  ptr = or_unpack_hfid (ptr, &selupd_list->class_hfid);
  ptr = or_unpack_int (ptr, &selupd_list->select_list_size);
  ptr = or_unpack_int (ptr, &selupd_list->waitsecs);

  ptr = or_unpack_int (ptr, &offset);
  selupd_list->select_list =
    stx_restore_regu_varlist_list (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      selupd_list->next = NULL;
    }
  else
    {
      selupd_list->next =
	stx_restore_selupd_list (thread_p,
				 &xasl_unpack_info->packed_xasl[offset]);
      if (selupd_list->next == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return ptr;
}

static char *
stx_build_pred_expr (THREAD_ENTRY * thread_p, char *ptr,
		     PRED_EXPR * pred_expr)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, (int *) &pred_expr->type);

  switch (pred_expr->type)
    {
    case T_PRED:
      ptr = stx_build_pred (thread_p, ptr, &pred_expr->pe.pred);
      break;

    case T_EVAL_TERM:
      ptr = stx_build_eval_term (thread_p, ptr, &pred_expr->pe.eval_term);
      break;

    case T_NOT_TERM:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
	{
	  pred_expr->pe.not_term = NULL;
	}
      else
	{
	  pred_expr->pe.not_term =
	    stx_restore_pred_expr (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
	  if (pred_expr->pe.not_term == NULL)
	    {
	      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	      return NULL;
	    }
	}
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  return ptr;
}

static char *
stx_build_partition_info (THREAD_ENTRY * thread_p, char *ptr,
			  XASL_PARTITION_INFO * partition_info)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, (int *) &partition_info->key_attr);
  ptr = or_unpack_int (ptr, (int *) &partition_info->type);
  ptr = or_unpack_int (ptr, (int *) &partition_info->no_parts);
  ptr = or_unpack_int (ptr, (int *) &partition_info->act_parts);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      partition_info->expr = NULL;
    }
  else
    {
      partition_info->expr =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (partition_info->expr == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || partition_info->no_parts == 0)
    {
      partition_info->parts = NULL;
    }
  else
    {
      partition_info->parts =
	stx_restore_parts_array (thread_p,
				 &xasl_unpack_info->packed_xasl[offset],
				 partition_info->no_parts);
      if (partition_info->parts == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return ptr;
}

static char *
stx_build_parts_info (THREAD_ENTRY * thread_p, char *ptr,
		      XASL_PARTS_INFO * parts_info)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_oid (ptr, &parts_info->class_oid);
  ptr = or_unpack_oid (ptr, &parts_info->proxy_class_oid);
  ptr = or_unpack_hfid (ptr, &parts_info->class_hfid);
  ptr = or_unpack_int (ptr, &parts_info->node_id);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      parts_info->vals = NULL;
    }
  else
    {
      parts_info->vals =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (parts_info->vals == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return ptr;
}

static char *
stx_build_pred (THREAD_ENTRY * thread_p, char *ptr, PRED * pred)
{
  int offset;
  int rhs_type;
  PRED_EXPR *rhs;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  /* lhs */
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      pred->lhs = NULL;
    }
  else
    {
      pred->lhs =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (pred->lhs == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &pred->bool_op);	/* bool_op */

  ptr = or_unpack_int (ptr, &rhs_type);	/* rhs-type */

  /* Traverse right-linear chains of AND/OR terms */
  while (rhs_type == T_PRED)
    {
      pred->rhs =
	(PRED_EXPR *) stx_alloc_struct (thread_p, sizeof (PRED_EXPR));
      if (pred->rhs == NULL)
	{
	  goto error;
	}

      rhs = pred->rhs;

      rhs->type = T_PRED;

      pred = &rhs->pe.pred;

      /* lhs */
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
	{
	  pred->lhs = NULL;
	}
      else
	{
	  pred->lhs =
	    stx_restore_pred_expr (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
	  if (pred->lhs == NULL)
	    {
	      goto error;
	    }
	}

      ptr = or_unpack_int (ptr, (int *) &pred->bool_op);	/* bool_op */

      ptr = or_unpack_int (ptr, &rhs_type);	/* rhs-type */
    }

  /* rhs */
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      pred->rhs = NULL;
    }
  else
    {
      pred->rhs =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (pred->rhs == NULL)
	{
	  goto error;
	}
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_eval_term (THREAD_ENTRY * thread_p, char *ptr,
		     EVAL_TERM * eval_term)
{
  ptr = or_unpack_int (ptr, (int *) &eval_term->et_type);

  switch (eval_term->et_type)
    {
    case T_COMP_EVAL_TERM:
      ptr = stx_build_comp_eval_term (thread_p, ptr, &eval_term->et.et_comp);
      break;

    case T_ALSM_EVAL_TERM:
      ptr = stx_build_alsm_eval_term (thread_p, ptr, &eval_term->et.et_alsm);
      break;

    case T_LIKE_EVAL_TERM:
      ptr = stx_build_like_eval_term (thread_p, ptr, &eval_term->et.et_like);
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  return ptr;
}

static char *
stx_build_comp_eval_term (THREAD_ENTRY * thread_p, char *ptr,
			  COMP_EVAL_TERM * comp_eval_term)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      comp_eval_term->lhs = NULL;
    }
  else
    {
      comp_eval_term->lhs =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (comp_eval_term->lhs == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      comp_eval_term->rhs = NULL;
    }
  else
    {
      comp_eval_term->rhs =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (comp_eval_term->rhs == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &comp_eval_term->rel_op);

  ptr = or_unpack_int (ptr, (int *) &comp_eval_term->type);

  return ptr;
}

static char *
stx_build_alsm_eval_term (THREAD_ENTRY * thread_p, char *ptr,
			  ALSM_EVAL_TERM * alsm_eval_term)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      alsm_eval_term->elem = NULL;
    }
  else
    {
      alsm_eval_term->elem =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (alsm_eval_term->elem == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      alsm_eval_term->elemset = NULL;
    }
  else
    {
      alsm_eval_term->elemset =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (alsm_eval_term->elemset == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &alsm_eval_term->eq_flag);
  ptr = or_unpack_int (ptr, (int *) &alsm_eval_term->rel_op);
  ptr = or_unpack_int (ptr, (int *) &alsm_eval_term->item_type);

  return ptr;
}

static char *
stx_build_like_eval_term (THREAD_ENTRY * thread_p, char *ptr,
			  LIKE_EVAL_TERM * like_eval_term)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      like_eval_term->src = NULL;
    }
  else
    {
      like_eval_term->src =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (like_eval_term->src == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      like_eval_term->pattern = NULL;
    }
  else
    {
      like_eval_term->pattern =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (like_eval_term->pattern == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      like_eval_term->esc_char = NULL;
    }
  else
    {
      like_eval_term->esc_char =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (like_eval_term->esc_char == NULL)
	{
	  goto error;
	}
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_access_spec_type (THREAD_ENTRY * thread_p, char *ptr,
			    ACCESS_SPEC_TYPE * access_spec)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, (int *) &access_spec->type);
  ptr = or_unpack_int (ptr, (int *) &access_spec->access);
  ptr = or_unpack_int (ptr, (int *) &access_spec->lock_hint);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      access_spec->indexptr = NULL;
    }
  else
    {
      access_spec->indexptr =
	stx_restore_indx_info (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (access_spec->indexptr == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      access_spec->where_key = NULL;
    }
  else
    {
      access_spec->where_key =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (access_spec->where_key == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      access_spec->where_pred = NULL;
    }
  else
    {
      access_spec->where_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (access_spec->where_pred == NULL)
	{
	  goto error;
	}
    }

  switch (access_spec->type)
    {
    case TARGET_CLASS:
    case TARGET_CLASS_ATTR:
      ptr = stx_build_cls_spec_type (thread_p, ptr,
				     &ACCESS_SPEC_CLS_SPEC (access_spec));
      break;

    case TARGET_LIST:
      ptr = stx_build_list_spec_type (thread_p, ptr,
				      &ACCESS_SPEC_LIST_SPEC (access_spec));
      break;

    case TARGET_SET:
      ptr = stx_build_set_spec_type (thread_p, ptr,
				     &ACCESS_SPEC_SET_SPEC (access_spec));
      break;

    case TARGET_METHOD:
      ptr = stx_build_method_spec_type (thread_p, ptr,
					&ACCESS_SPEC_METHOD_SPEC
					(access_spec));
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  if (ptr == NULL)
    {
      return NULL;
    }

  /* access_spec_type->s_id not sent to server */
  memset (&access_spec->s_id, '\0', sizeof (SCAN_ID));
  access_spec->s_id.status = S_CLOSED;

  ptr = or_unpack_int (ptr, &access_spec->grouped_scan);
  ptr = or_unpack_int (ptr, &access_spec->fixed_scan);
  ptr = or_unpack_int (ptr, &access_spec->qualified_block);
  ptr = or_unpack_int (ptr, (int *) &access_spec->single_fetch);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      access_spec->s_dbval = NULL;
    }
  else
    {
      access_spec->s_dbval =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (access_spec->s_dbval == NULL)
	{
	  goto error;
	}
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_indx_info (THREAD_ENTRY * thread_p, char *ptr,
		     INDX_INFO * indx_info)
{
  ptr = stx_build_indx_id (thread_p, ptr, &indx_info->indx_id);
  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_unpack_int (ptr, (int *) &indx_info->range_type);

  ptr = stx_build_key_info (thread_p, ptr, &indx_info->key_info);
  if (ptr == NULL)
    {
      return NULL;
    }

  return ptr;
}

static char *
stx_build_indx_id (THREAD_ENTRY * thread_p, char *ptr, INDX_ID * indx_id)
{
  ptr = or_unpack_int (ptr, (int *) &indx_id->type);
  if (ptr == NULL)
    {
      return NULL;
    }

  switch (indx_id->type)
    {
    case T_BTID:
      ptr = or_unpack_btid (ptr, &indx_id->i.btid);
      break;

    case T_EHID:
      ptr = or_unpack_ehid (ptr, &indx_id->i.ehid);
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  return ptr;
}

static char *
stx_build_key_info (THREAD_ENTRY * thread_p, char *ptr, KEY_INFO * key_info)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &key_info->key_cnt);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || key_info->key_cnt == 0)
    {
      key_info->key_ranges = NULL;
    }
  else
    {
      key_info->key_ranges =
	stx_restore_key_range_array (thread_p,
				     &xasl_unpack_info->packed_xasl[offset],
				     key_info->key_cnt);
      if (key_info->key_ranges == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &key_info->is_constant);

  return ptr;
}

static char *
stx_build_cls_spec_type (THREAD_ENTRY * thread_p, char *ptr,
			 CLS_SPEC_TYPE * cls_spec)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_hfid (ptr, &cls_spec->hfid);
  ptr = or_unpack_oid (ptr, &cls_spec->cls_oid);
  ptr = or_unpack_int (ptr, &cls_spec->node_id);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cls_regu_list_key = NULL;
    }
  else
    {
      cls_spec->cls_regu_list_key =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (cls_spec->cls_regu_list_key == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cls_regu_list_pred = NULL;
    }
  else
    {
      cls_spec->cls_regu_list_pred =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (cls_spec->cls_regu_list_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cls_regu_list_rest = NULL;
    }
  else
    {
      cls_spec->cls_regu_list_rest =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (cls_spec->cls_regu_list_rest == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &cls_spec->num_attrs_key);
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || cls_spec->num_attrs_key == 0)
    {
      cls_spec->attrids_key = NULL;
    }
  else
    {
      cls_spec->attrids_key =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       cls_spec->num_attrs_key);
      if (cls_spec->attrids_key == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cache_key = NULL;
    }
  else
    {
      cls_spec->cache_key =
	stx_restore_cache_attrinfo (thread_p,
				    &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cache_key == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &cls_spec->num_attrs_pred);
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || cls_spec->num_attrs_pred == 0)
    {
      cls_spec->attrids_pred = NULL;
    }
  else
    {
      cls_spec->attrids_pred =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       cls_spec->num_attrs_pred);
      if (cls_spec->attrids_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cache_pred = NULL;
    }
  else
    {
      cls_spec->cache_pred =
	stx_restore_cache_attrinfo (thread_p,
				    &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cache_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &cls_spec->num_attrs_rest);
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || cls_spec->num_attrs_rest == 0)
    {
      cls_spec->attrids_rest = NULL;
    }
  else
    {
      cls_spec->attrids_rest =
	stx_restore_int_array (thread_p,
			       &xasl_unpack_info->packed_xasl[offset],
			       cls_spec->num_attrs_rest);
      if (cls_spec->attrids_rest == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cache_rest = NULL;
    }
  else
    {
      cls_spec->cache_rest =
	stx_restore_cache_attrinfo (thread_p,
				    &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cache_rest == NULL)
	{
	  goto error;
	}
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_list_spec_type (THREAD_ENTRY * thread_p, char *ptr,
			  LIST_SPEC_TYPE * list_spec_type)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      list_spec_type->xasl_node = NULL;
    }
  else
    {
      list_spec_type->xasl_node =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (list_spec_type->xasl_node == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      list_spec_type->list_regu_list_pred = NULL;
    }
  else
    {
      list_spec_type->list_regu_list_pred =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (list_spec_type->list_regu_list_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      list_spec_type->list_regu_list_rest = NULL;
    }
  else
    {
      list_spec_type->list_regu_list_rest =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (list_spec_type->list_regu_list_rest == NULL)
	{
	  goto error;
	}
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_set_spec_type (THREAD_ENTRY * thread_p, char *ptr,
			 SET_SPEC_TYPE * set_spec)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      set_spec->set_ptr = NULL;
    }
  else
    {
      set_spec->set_ptr =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (set_spec->set_ptr == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      set_spec->set_regu_list = NULL;
    }
  else
    {
      set_spec->set_regu_list =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (set_spec->set_regu_list == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return ptr;
}

static char *
stx_build_method_spec_type (THREAD_ENTRY * thread_p, char *ptr,
			    METHOD_SPEC_TYPE * method_spec)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      method_spec->xasl_node = NULL;
    }
  else
    {
      method_spec->xasl_node =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (method_spec->xasl_node == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      method_spec->method_regu_list = NULL;
    }
  else
    {
      method_spec->method_regu_list =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (method_spec->method_regu_list == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      method_spec->method_sig_list = NULL;
    }
  else
    {
      method_spec->method_sig_list =
	stx_restore_method_sig_list (thread_p,
				     &xasl_unpack_info->packed_xasl[offset]);
      if (method_spec->method_sig_list == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return ptr;
}

static char *
stx_build_val_list (THREAD_ENTRY * thread_p, char *ptr, VAL_LIST * val_list)
{
  int offset, i;
  QPROC_DB_VALUE_LIST value_list = NULL;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &val_list->val_cnt);

  value_list = (QPROC_DB_VALUE_LIST)
    stx_alloc_struct (thread_p, sizeof (struct qproc_db_value_list) *
		      val_list->val_cnt);
  if (val_list->val_cnt)
    {
      if (value_list == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  for (i = 0; i < val_list->val_cnt; i++)
    {
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
	{
	  value_list[i].val = NULL;
	}
      else
	{
	  value_list[i].val =
	    stx_restore_db_value (thread_p,
				  &xasl_unpack_info->packed_xasl[offset]);
	  if (value_list[i].val == NULL)
	    {
	      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	      return NULL;
	    }
	}

      if (i < val_list->val_cnt - 1)
	{
	  value_list[i].next = (QPROC_DB_VALUE_LIST) & value_list[i + 1];
	}
      else
	{
	  value_list[i].next = NULL;
	}
    }

  val_list->valp = value_list;

  return ptr;
}

static char *
stx_build_db_value_list (THREAD_ENTRY * thread_p, char *ptr,
			 QPROC_DB_VALUE_LIST value_list)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      value_list->next = NULL;
    }
  else
    {
      value_list->next =
	stx_restore_db_value_list (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (value_list->next == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      value_list->val = NULL;
    }
  else
    {
      value_list->val =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (value_list->val == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return ptr;
}

static char *
stx_build_regu_variable (THREAD_ENTRY * thread_p, char *ptr,
			 REGU_VARIABLE * regu_var)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_domain (ptr, &regu_var->domain, NULL);
  ptr = or_unpack_int (ptr, (int *) &regu_var->type);
  ptr = or_unpack_int (ptr, &regu_var->hidden_column);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      regu_var->vfetch_to = NULL;
    }
  else
    {
      regu_var->vfetch_to =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (regu_var->vfetch_to == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      regu_var->xasl = NULL;
    }
  else
    {
      regu_var->xasl =
	stx_restore_xasl_node (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (regu_var->xasl == NULL)
	{
	  goto error;
	}
    }

  switch (regu_var->type)
    {
    case TYPE_DBVAL:
      ptr = stx_build_db_value (thread_p, ptr, &regu_var->value.dbval);
      break;

    case TYPE_CONSTANT:
    case TYPE_ORDERBY_NUM:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
	{
	  regu_var->value.dbvalptr = NULL;
	}
      else
	{
	  regu_var->value.dbvalptr =
	    stx_restore_db_value (thread_p,
				  &xasl_unpack_info->packed_xasl[offset]);
	  if (regu_var->value.dbvalptr == NULL)
	    {
	      goto error;
	    }
	}
      break;

    case TYPE_INARITH:
    case TYPE_OUTARITH:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
	{
	  regu_var->value.arithptr = NULL;
	}
      else
	{
	  regu_var->value.arithptr =
	    stx_restore_arith_type (thread_p,
				    &xasl_unpack_info->packed_xasl[offset]);
	  if (regu_var->value.arithptr == NULL)
	    {
	      goto error;
	    }
	}
      break;

    case TYPE_AGGREGATE:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
	{
	  regu_var->value.aggptr = NULL;
	}
      else
	{
	  regu_var->value.aggptr =
	    stx_restore_aggregate_type (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
	  if (regu_var->value.aggptr == NULL)
	    {
	      goto error;
	    }
	}
      break;

    case TYPE_FUNC:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
	{
	  regu_var->value.funcp = NULL;
	}
      else
	{
	  regu_var->value.funcp =
	    stx_restore_function_type (thread_p, &xasl_unpack_info->
				       packed_xasl[offset]);
	  if (regu_var->value.funcp == NULL)
	    {
	      goto error;
	    }
	}
      break;

    case TYPE_ATTR_ID:
    case TYPE_SHARED_ATTR_ID:
    case TYPE_CLASS_ATTR_ID:
      ptr = stx_build_attr_descr (thread_p, ptr, &regu_var->value.attr_descr);
      break;

    case TYPE_LIST_ID:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
	{
	  regu_var->value.srlist_id = NULL;
	}
      else
	{
	  regu_var->value.srlist_id =
	    stx_restore_srlist_id (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
	  if (regu_var->value.srlist_id == NULL)
	    {
	      goto error;
	    }
	}
      break;

    case TYPE_POSITION:
      ptr = stx_build_pos_descr (ptr, &regu_var->value.pos_descr);
      break;

    case TYPE_POS_VALUE:
      ptr = or_unpack_int (ptr, &regu_var->value.val_pos);
      break;

    case TYPE_OID:
    case TYPE_CLASSOID:
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_attr_descr (THREAD_ENTRY * thread_p, char *ptr,
		      ATTR_DESCR * attr_descr)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, (int *) &attr_descr->id);
  ptr = or_unpack_int (ptr, (int *) &attr_descr->type);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      attr_descr->cache_attrinfo = NULL;
    }
  else
    {
      attr_descr->cache_attrinfo =
	stx_restore_cache_attrinfo (thread_p,
				    &xasl_unpack_info->packed_xasl[offset]);
      if (attr_descr->cache_attrinfo == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  attr_descr->cache_dbvalp = NULL;

  return ptr;
}

static char *
stx_build_pos_descr (char *ptr, QFILE_TUPLE_VALUE_POSITION * position_descr)
{
  ptr = or_unpack_int (ptr, &position_descr->pos_no);
  ptr = or_unpack_domain (ptr, &position_descr->dom, NULL);

  return ptr;
}

static char *
stx_build_db_value (THREAD_ENTRY * thread_p, char *ptr, DB_VALUE * value)
{
  ptr = or_unpack_db_value (ptr, value);

  return ptr;
}

static char *
stx_build_arith_type (THREAD_ENTRY * thread_p, char *ptr,
		      ARITH_TYPE * arith_type)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_domain (ptr, &arith_type->domain, NULL);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      arith_type->value = NULL;
    }
  else
    {
      arith_type->value =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (arith_type->value == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &arith_type->opcode);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      arith_type->next = NULL;
    }
  else
    {
      arith_type->next =
	stx_restore_arith_type (thread_p,
				&xasl_unpack_info->packed_xasl[offset]);
      if (arith_type->next == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      arith_type->leftptr = NULL;
    }
  else
    {
      arith_type->leftptr =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (arith_type->leftptr == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      arith_type->rightptr = NULL;
    }
  else
    {
      arith_type->rightptr =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (arith_type->rightptr == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      arith_type->thirdptr = NULL;
    }
  else
    {
      arith_type->thirdptr =
	stx_restore_regu_variable (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
      if (arith_type->thirdptr == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &arith_type->misc_operand);
  if (arith_type->opcode == T_CASE || arith_type->opcode == T_DECODE
      || arith_type->opcode == T_IF)
    {
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
	{
	  arith_type->pred = NULL;
	}
      else
	{
	  arith_type->pred =
	    stx_restore_pred_expr (thread_p,
				   &xasl_unpack_info->packed_xasl[offset]);
	  if (arith_type->pred == NULL)
	    {
	      goto error;
	    }
	}
    }
  else
    {
      arith_type->pred = NULL;
    }

  /* This member is only used on server internally. */
  arith_type->rand_seed = NULL;

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_aggregate_type (THREAD_ENTRY * thread_p, char *ptr,
			  AGGREGATE_TYPE * aggregate)
{
  int offset;
  int type;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_domain (ptr, &aggregate->domain, NULL);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      aggregate->value = NULL;
    }
  else
    {
      aggregate->value =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (aggregate->value == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      aggregate->value2 = NULL;
    }
  else
    {
      aggregate->value2 =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (aggregate->value2 == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &aggregate->curr_cnt);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      aggregate->next = NULL;
    }
  else
    {
      aggregate->next =
	stx_restore_aggregate_type (thread_p,
				    &xasl_unpack_info->packed_xasl[offset]);
      if (aggregate->next == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &aggregate->function);
  ptr = or_unpack_int (ptr, (int *) &aggregate->option);
  ptr = or_unpack_int (ptr, (int *) &type);
  aggregate->opr_dbtype = (DB_TYPE) type;

  ptr = stx_build_regu_variable (thread_p, ptr, &aggregate->operand);
  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      aggregate->list_id = NULL;
    }
  else
    {
      aggregate->list_id =
	stx_restore_list_id (thread_p,
			     &xasl_unpack_info->packed_xasl[offset]);
      if (aggregate->list_id == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &aggregate->flag_agg_optimize);
  ptr = or_unpack_btid (ptr, &aggregate->btid);

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_function_type (THREAD_ENTRY * thread_p, char *ptr,
			 FUNCTION_TYPE * function)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      function->value = NULL;
    }
  else
    {
      function->value =
	stx_restore_db_value (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (function->value == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = or_unpack_int (ptr, (int *) &function->ftype);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      function->operand = NULL;
    }
  else
    {
      function->operand =
	stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->
					packed_xasl[offset]);
      if (function->operand == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return ptr;
}

static char *
stx_build_srlist_id (THREAD_ENTRY * thread_p, char *ptr,
		     QFILE_SORTED_LIST_ID * sort_list_id)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &sort_list_id->sorted);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      sort_list_id->list_id = NULL;
    }
  else
    {
      sort_list_id->list_id =
	stx_restore_list_id (thread_p,
			     &xasl_unpack_info->packed_xasl[offset]);
      if (sort_list_id->list_id == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  return ptr;
}

static char *
stx_build_sort_list (THREAD_ENTRY * thread_p, char *ptr,
		     SORT_LIST * sort_list)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      sort_list->next = NULL;
    }
  else
    {
      sort_list->next =
	stx_restore_sort_list (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (sort_list->next == NULL)
	{
	  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
	  return NULL;
	}
    }

  ptr = stx_build_pos_descr (ptr, &sort_list->pos_descr);
  if (ptr == NULL)
    {
      return NULL;
    }
  ptr = or_unpack_int (ptr, (int *) &sort_list->s_order);

  return ptr;
}

static char *
stx_build_connectby_proc (THREAD_ENTRY * thread_p, char *ptr,
			  CONNECTBY_PROC_NODE * stx_connectby_proc)
{
  int offset;
  int tmp;

  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->start_with_pred = NULL;
    }
  else
    {
      stx_connectby_proc->start_with_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (stx_connectby_proc->start_with_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->after_connect_by_pred = NULL;
    }
  else
    {
      stx_connectby_proc->after_connect_by_pred =
	stx_restore_pred_expr (thread_p,
			       &xasl_unpack_info->packed_xasl[offset]);
      if (stx_connectby_proc->after_connect_by_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->input_list_id = NULL;
    }
  else
    {
      stx_connectby_proc->input_list_id =
	stx_restore_list_id (thread_p,
			     &xasl_unpack_info->packed_xasl[offset]);
      if (stx_connectby_proc->input_list_id == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->start_with_list_id = NULL;
    }
  else
    {
      stx_connectby_proc->start_with_list_id =
	stx_restore_list_id (thread_p,
			     &xasl_unpack_info->packed_xasl[offset]);
      if (stx_connectby_proc->start_with_list_id == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->regu_list_pred = NULL;
    }
  else
    {
      stx_connectby_proc->regu_list_pred =
	stx_restore_regu_variable_list (thread_p,
					&xasl_unpack_info->
					packed_xasl[offset]);
      if (stx_connectby_proc->regu_list_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->regu_list_rest = NULL;
    }
  else
    {
      stx_connectby_proc->regu_list_rest =
	stx_restore_regu_variable_list (thread_p,
					&xasl_unpack_info->
					packed_xasl[offset]);
      if (stx_connectby_proc->regu_list_rest == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->prior_val_list = NULL;
    }
  else
    {
      stx_connectby_proc->prior_val_list =
	stx_restore_val_list (thread_p,
			      &xasl_unpack_info->packed_xasl[offset]);
      if (stx_connectby_proc->prior_val_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->prior_outptr_list = NULL;
    }
  else
    {
      stx_connectby_proc->prior_outptr_list =
	stx_restore_outptr_list (thread_p,
				 &xasl_unpack_info->packed_xasl[offset]);
      if (stx_connectby_proc->prior_outptr_list == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->prior_regu_list_pred = NULL;
    }
  else
    {
      stx_connectby_proc->prior_regu_list_pred =
	stx_restore_regu_variable_list (thread_p,
					&xasl_unpack_info->
					packed_xasl[offset]);
      if (stx_connectby_proc->prior_regu_list_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->prior_regu_list_rest = NULL;
    }
  else
    {
      stx_connectby_proc->prior_regu_list_rest =
	stx_restore_regu_variable_list (thread_p,
					&xasl_unpack_info->
					packed_xasl[offset]);
      if (stx_connectby_proc->prior_regu_list_rest == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->after_cb_regu_list_pred = NULL;
    }
  else
    {
      stx_connectby_proc->after_cb_regu_list_pred =
	stx_restore_regu_variable_list (thread_p,
					&xasl_unpack_info->
					packed_xasl[offset]);
      if (stx_connectby_proc->after_cb_regu_list_pred == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_connectby_proc->after_cb_regu_list_rest = NULL;
    }
  else
    {
      stx_connectby_proc->after_cb_regu_list_rest =
	stx_restore_regu_variable_list (thread_p,
					&xasl_unpack_info->
					packed_xasl[offset]);
      if (stx_connectby_proc->after_cb_regu_list_rest == NULL)
	{
	  goto error;
	}
    }

  ptr = or_unpack_int (ptr, &tmp);
  stx_connectby_proc->single_table_opt = (bool) tmp;

  stx_connectby_proc->curr_tuple = NULL;

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

/*
 * stx_mark_struct_visited () -
 *   return: if successful, return NO_ERROR, otherwise
 *           ER_FAILED and error code is set to xasl_errcode
 *   ptr(in)    : pointer constant to be marked visited
 *   str(in)    : where the struct pointed by 'ptr' is stored
 *
 * Note: mark the given pointer constant as visited to avoid
 * duplicated storage of a struct which is pointed by more than one node
 */
static int
stx_mark_struct_visited (THREAD_ENTRY * thread_p, const void *ptr, void *str)
{
  int new_lwm;
  int block_no;
#if defined(SERVER_MODE)
  THREAD_ENTRY *thrd;
#else /* SERVER_MODE */
  void *thrd = NULL;
#endif /* !SERVER_MODE */
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  thrd = thread_p;

  block_no = PTR_BLOCK (ptr);
  new_lwm = xasl_unpack_info->ptr_lwm[block_no];

  if (xasl_unpack_info->ptr_max[block_no] == 0)
    {
      xasl_unpack_info->ptr_max[block_no] = START_PTR_PER_BLOCK;
      xasl_unpack_info->ptr_blocks[block_no] = (VISITED_PTR *)
	db_private_alloc (thrd,
			  sizeof (VISITED_PTR) *
			  xasl_unpack_info->ptr_max[block_no]);
    }
  else if (xasl_unpack_info->ptr_max[block_no] <= new_lwm)
    {
      xasl_unpack_info->ptr_max[block_no] *= 2;
      xasl_unpack_info->ptr_blocks[block_no] = (VISITED_PTR *)
	db_private_realloc (thrd, xasl_unpack_info->ptr_blocks[block_no],
			    sizeof (VISITED_PTR) *
			    xasl_unpack_info->ptr_max[block_no]);
    }

  if (xasl_unpack_info->ptr_blocks[block_no] == (VISITED_PTR *) NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return ER_FAILED;
    }

  xasl_unpack_info->ptr_blocks[block_no][new_lwm].ptr = ptr;
  xasl_unpack_info->ptr_blocks[block_no][new_lwm].str = str;

  xasl_unpack_info->ptr_lwm[block_no]++;

  return NO_ERROR;
}

/*
 * stx_get_struct_visited_ptr () -
 *   return: if the ptr is already visited, the offset of
 *           position where the node pointed by 'ptr' is stored,
 *           otherwise, ER_FAILED (xasl_errcode is NOT set)
 *   ptr(in)    : pointer constant to be checked if visited or not
 *
 * Note: check if the node pointed by `ptr` is already stored or
 * not to avoid multiple store of the same node
 */
static void *
stx_get_struct_visited_ptr (THREAD_ENTRY * thread_p, const void *ptr)
{
  int block_no;
  int element_no;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  block_no = PTR_BLOCK (ptr);

  if (xasl_unpack_info->ptr_lwm[block_no] <= 0)
    {
      return NULL;
    }

  for (element_no = 0;
       element_no < xasl_unpack_info->ptr_lwm[block_no]; element_no++)
    {
      if (ptr == xasl_unpack_info->ptr_blocks[block_no][element_no].ptr)
	{
	  return (xasl_unpack_info->ptr_blocks[block_no][element_no].str);
	}
    }

  return NULL;
}

/*
 * stx_free_visited_ptrs () -
 *   return:
 *
 * Note: free memory allocated to manage visited ptr constants
 */
static void
stx_free_visited_ptrs (THREAD_ENTRY * thread_p)
{
  int i;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  for (i = 0; i < MAX_PTR_BLOCKS; i++)
    {
      xasl_unpack_info->ptr_lwm[i] = 0;
      xasl_unpack_info->ptr_max[i] = 0;
      if (xasl_unpack_info->ptr_blocks[i])
	{
	  db_private_free_and_init (thread_p,
				    xasl_unpack_info->ptr_blocks[i]);
	  xasl_unpack_info->ptr_blocks[i] = (VISITED_PTR *) 0;
	}
    }
}

/*
 * stx_alloc_struct () -
 *   return:
 *   size(in)   : # of bytes of the node
 *
 * Note: allocate storage for structures pointed to from the xasl tree.
 */
static char *
stx_alloc_struct (THREAD_ENTRY * thread_p, int size)
{
  char *ptr;
  XASL_UNPACK_INFO *xasl_unpack_info =
    stx_get_xasl_unpack_info_ptr (thread_p);

  if (!size)
    {
      return NULL;
    }

  size = MAKE_ALIGN (size);	/* alignment */
  if (size > xasl_unpack_info->alloc_size)
    {				/* need to alloc */
      int p_size;

      p_size = MAX (size, xasl_unpack_info->packed_size);
      p_size = MAKE_ALIGN (p_size);	/* alignment */
      ptr = db_private_alloc (thread_p, p_size);
      if (ptr == NULL)
	{
	  return NULL;		/* error */
	}
      xasl_unpack_info->alloc_size = p_size;
      xasl_unpack_info->alloc_buf = ptr;
    }

  /* consume alloced buffer */
  ptr = xasl_unpack_info->alloc_buf;
  xasl_unpack_info->alloc_size -= size;
  xasl_unpack_info->alloc_buf += size;

  return ptr;
}

/*
 * stx_init_xasl_unpack_info () -
 *   return:
 *   xasl_stream(in)    : pointer to xasl stream
 *   xasl_stream_size(in)       :
 *
 * Note: initialize the xasl pack information.
 */
static int
stx_init_xasl_unpack_info (THREAD_ENTRY * thread_p, char *xasl_stream,
			   int xasl_stream_size)
{
  int n;
#if defined(SERVER_MODE)
  XASL_UNPACK_INFO *xasl_unpack_info;
#endif /* SERVER_MODE */
  int head_offset, body_offset;

#define UNPACK_SCALE 3		/* TODO: assume */

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  head_offset = sizeof (XASL_UNPACK_INFO);
  head_offset = MAKE_ALIGN (head_offset);
  body_offset = xasl_stream_size * UNPACK_SCALE;
  body_offset = MAKE_ALIGN (body_offset);
#if defined(SERVER_MODE)
  xasl_unpack_info = db_private_alloc (thread_p, head_offset + body_offset);
  stx_set_xasl_unpack_info_ptr (thread_p, xasl_unpack_info);
#else /* SERVER_MODE */
  xasl_unpack_info = db_private_alloc (NULL, head_offset + body_offset);
#endif /* SERVER_MODE */
  if (xasl_unpack_info == NULL)
    {
      return ER_FAILED;
    }
  xasl_unpack_info->packed_xasl = xasl_stream;
  xasl_unpack_info->packed_size = xasl_stream_size;
  for (n = 0; n < MAX_PTR_BLOCKS; ++n)
    {
      xasl_unpack_info->ptr_blocks[n] = (VISITED_PTR *) 0;
      xasl_unpack_info->ptr_lwm[n] = 0;
      xasl_unpack_info->ptr_max[n] = 0;
    }
  xasl_unpack_info->alloc_size = xasl_stream_size * UNPACK_SCALE;
  xasl_unpack_info->alloc_buf = (char *) xasl_unpack_info + head_offset;
#if defined (SERVER_MODE)
  xasl_unpack_info->thrd = thread_p;
#endif /* SERVER_MODE */

  return NO_ERROR;
}

/*
 * stx_get_xasl_unpack_info_ptr () -
 *   return:
 */
static XASL_UNPACK_INFO *
stx_get_xasl_unpack_info_ptr (THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  return (XASL_UNPACK_INFO *) thread_p->xasl_unpack_info_ptr;
#else /* SERVER_MODE */
  return (XASL_UNPACK_INFO *) xasl_unpack_info;
#endif /* SERVER_MODE */
}

#if defined(SERVER_MODE)
/*
 * stx_set_xasl_unpack_info_ptr () -
 *   return:
 *   ptr(in)    :
 */
static void
stx_set_xasl_unpack_info_ptr (THREAD_ENTRY * thread_p, XASL_UNPACK_INFO * ptr)
{
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  thread_p->xasl_unpack_info_ptr = ptr;
}
#endif /* SERVER_MODE */

/*
 * stx_get_xasl_errcode () -
 *   return:
 */
static int
stx_get_xasl_errcode (THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  return thread_p->xasl_errcode;
#else /* SERVER_MODE */
  return stx_Xasl_errcode;
#endif /* SERVER_MODE */
}

/*
 * stx_set_xasl_errcode () -
 *   return:
 *   errcode(in)        :
 */
static void
stx_set_xasl_errcode (THREAD_ENTRY * thread_p, int errcode)
{
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  thread_p->xasl_errcode = errcode;
#else /* SERVER_MODE */
  stx_Xasl_errcode = errcode;
#endif /* SERVER_MODE */
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * stx_unpack_char () -
 *   return:
 *   tmp(in)    :
 *   ptr(in)    :
 */
static char *
stx_unpack_char (char *tmp, char *ptr)
{
  int i;

  tmp = or_unpack_int (tmp, &i);
  *ptr = i;

  return tmp;
}

/*
 * stx_unpack_long () -
 *   return:
 *   tmp(in)    :
 *   ptr(in)    :
 */
static char *
stx_unpack_long (char *tmp, long *ptr)
{
  int i;

  tmp = or_unpack_int (tmp, &i);
  *ptr = i;

  return tmp;
}
#endif
