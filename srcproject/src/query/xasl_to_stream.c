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
 * xasl_to_stream.c - XASL tree storer
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
#include "memory_alloc.h"
#include "heap_file.h"

/* memory alignment unit - to align stored XASL tree nodes */
#define	ALIGN_UNIT	sizeof(double)
#define	ALIGN_MASK	(ALIGN_UNIT - 1)
#define MAKE_ALIGN(x)	(((x) & ~ALIGN_MASK) + \
			 (((x) & ALIGN_MASK) ? ALIGN_UNIT : 0 ))

/* to limit size of XASL trees */
#define	OFFSETS_PER_BLOCK	4096
#define	START_PTR_PER_BLOCK	15
#define MAX_PTR_BLOCKS		256
#define PTR_BLOCK(ptr)  (((UINTPTR) ptr) / __WORDSIZE) % MAX_PTR_BLOCKS

/*
 * the linear byte stream for store the given XASL tree is allocated
 * and expanded dynamically on demand by the following amount of bytes
 */
#define	STREAM_EXPANSION_UNIT	(OFFSETS_PER_BLOCK * sizeof(int))

#define    BYTE_SIZE        OR_INT_SIZE
#define    LONG_SIZE        OR_INT_SIZE
#define    PTR_SIZE         OR_INT_SIZE
#define    pack_char        or_pack_int
#define    pack_long        or_pack_int

/* structure of a visited pointer constant */
typedef struct visited_ptr VISITED_PTR;
struct visited_ptr
{
  const void *ptr;		/* a pointer constant */
  int offset;			/* offset where the node pointed by 'ptr'
				   is stored */
};

/* structure to hold information needed during packing */
typedef struct xasl_pack_info XASL_PACK_INFO;
struct xasl_pack_info
{
  /* linear byte stream to store the given XASL tree */
  char *_xts_Stream_buffer;	/* pointer to the stream */
  int _xts_Stream_size;		/* # of bytes allocated */
  int _xts_Free_offset_in_stream;

  /* blocks of visited pointer constants */
  VISITED_PTR *_xts_Ptr_blocks[MAX_PTR_BLOCKS];

  /* low-water-mark of visited pointers */
  int _xts_Ptr_lwm[MAX_PTR_BLOCKS];
  int _xts_Ptr_max[MAX_PTR_BLOCKS];
};

#if !defined(SERVER_MODE)
static void *xasl_pack_info_ptr = NULL;
/* error code specific to this file */
static int _xts_Xasl_errcode = NO_ERROR;

#define xts_pack_info_ptr(p) (xasl_pack_info_ptr)
#define xts_Xasl_errcode(p) (_xts_Xasl_errcode)
#else
#define xts_pack_info_ptr(p) ((p)->xasl_pack_info_ptr)
#define xts_Xasl_errcode(p) ((p)->xasl_errcode)
#endif

#define xts_Stream_buffer(p) (((XASL_PACK_INFO*)xts_pack_info_ptr(p))->_xts_Stream_buffer)
#define xts_Stream_size(p) (((XASL_PACK_INFO*)xts_pack_info_ptr(p))->_xts_Stream_size)
#define xts_Free_offset_in_stream(p) (((XASL_PACK_INFO*)xts_pack_info_ptr(p))->_xts_Free_offset_in_stream)
#define xts_Ptr_blocks(p) (((XASL_PACK_INFO*)xts_pack_info_ptr(p))->_xts_Ptr_blocks)
#define xts_Ptr_lwm(p) (((XASL_PACK_INFO*)xts_pack_info_ptr(p))->_xts_Ptr_lwm)
#define xts_Ptr_max(p) (((XASL_PACK_INFO*)xts_pack_info_ptr(p))->_xts_Ptr_max)

static int xts_save_aggregate_type (THREAD_ENTRY * thread_p,
				    const AGGREGATE_TYPE * aggregate);
static int xts_save_function_type (THREAD_ENTRY * thread_p,
				   const FUNCTION_TYPE * function);
static int xts_save_srlist_id (THREAD_ENTRY * thread_p,
			       const QFILE_SORTED_LIST_ID * sort_list_id);
static int xts_save_list_id (THREAD_ENTRY * thread_p,
			     const QFILE_LIST_ID * list_id);
static int xts_save_arith_type (THREAD_ENTRY * thread_p,
				const ARITH_TYPE * arithmetic);
static int xts_save_indx_info (THREAD_ENTRY * thread_p,
			       const INDX_INFO * indx_info);
static int xts_save_outptr_list (THREAD_ENTRY * thread_p,
				 const OUTPTR_LIST * outptr_list);
static int xts_save_selupd_list (THREAD_ENTRY * thread_p,
				 const SELUPD_LIST * selupd_list);
static int xts_save_pred_expr (THREAD_ENTRY * thread_p,
			       const PRED_EXPR * ptr);
static int xts_save_partition_info (THREAD_ENTRY * thread_p,
				    const XASL_PARTITION_INFO * ptr);
static int xts_save_parts_info (THREAD_ENTRY * thread_p,
				const XASL_PARTS_INFO * ptr);
static int xts_save_parts_array (THREAD_ENTRY * thread_p,
				 XASL_PARTS_INFO ** ptr, int size);
static int xts_save_partition_array (THREAD_ENTRY * thread_p,
				     XASL_PARTITION_INFO ** ptr, int size);
static int xts_save_regu_variable (THREAD_ENTRY * thread_p,
				   const REGU_VARIABLE * ptr);
static int xts_save_regu_variable_list (THREAD_ENTRY * thread_p,
					const REGU_VARIABLE_LIST ptr);
static int xts_save_regu_varlist_list (THREAD_ENTRY * thread_p,
				       const REGU_VARLIST_LIST ptr);
static int xts_save_sort_list (THREAD_ENTRY * thread_p,
			       const SORT_LIST * ptr);
static int xts_save_string (THREAD_ENTRY * thread_p, const char *str);
static int xts_save_val_list (THREAD_ENTRY * thread_p, const VAL_LIST * ptr);
static int xts_save_db_value (THREAD_ENTRY * thread_p, const DB_VALUE * ptr);
static int xts_save_xasl_node (THREAD_ENTRY * thread_p,
			       const XASL_NODE * ptr);
static int xts_save_cache_attrinfo (THREAD_ENTRY * thread_p,
				    const HEAP_CACHE_ATTRINFO * ptr);
#if 0
/* there are currently no pointers to these type of structures in xasl
 * so there is no need to have a separate restore function.
 */
static int xts_save_merge_list_info (THREAD_ENTRY * thread_p,
				     const MERGELIST_PROC_NODE * ptr);
static int xts_save_ls_merge_info (THREAD_ENTRY * thread_p,
				   const QFILE_LIST_MERGE_INFO * ptr);
static int xts_save_update_info (THREAD_ENTRY * thread_p,
				 const UPDATE_PROC_NODE * ptr);
static int xts_save_delete_info (THREAD_ENTRY * thread_p,
				 const DELETE_PROC_NODE * ptr);
static int xts_save_insert_info (THREAD_ENTRY * thread_p,
				 const INSERT_PROC_NODE * ptr);
#endif
static int xts_save_db_value_array (THREAD_ENTRY * thread_p, DB_VALUE ** ptr,
				    int size);
static int xts_save_int_array (THREAD_ENTRY * thread_p, int *ptr, int size);
static int xts_save_hfid_array (THREAD_ENTRY * thread_p, HFID * ptr,
				int size);
static int xts_save_oid_array (THREAD_ENTRY * thread_p, OID * ptr, int size);
static int xts_save_method_sig_list (THREAD_ENTRY * thread_p,
				     const METHOD_SIG_LIST * ptr);
static int xts_save_key_range_array (THREAD_ENTRY * thread_p,
				     const KEY_RANGE * ptr, int size);

static char *xts_process_xasl_node (THREAD_ENTRY * thread_p, char *ptr,
				    const XASL_NODE * xasl);
static char *xts_process_cache_attrinfo (THREAD_ENTRY * thread_p, char *ptr);
static char *xts_process_union_proc (THREAD_ENTRY * thread_p, char *ptr,
				     const UNION_PROC_NODE * union_proc);
static char *xts_process_fetch_proc (THREAD_ENTRY * thread_p, char *ptr,
				     const FETCH_PROC_NODE *
				     obj_set_fetch_proc);
static char *xts_process_buildlist_proc (THREAD_ENTRY * thread_p, char *ptr,
					 const BUILDLIST_PROC_NODE *
					 build_list_proc);
static char *xts_process_buildvalue_proc (THREAD_ENTRY * thread_p, char *ptr,
					  const BUILDVALUE_PROC_NODE *
					  build_value_proc);
static char *xts_process_mergelist_proc (THREAD_ENTRY * thread_p, char *ptr,
					 const MERGELIST_PROC_NODE *
					 merge_list_info);
static char *xts_process_ls_merge_info (THREAD_ENTRY * thread_p, char *ptr,
					const QFILE_LIST_MERGE_INFO *
					qfile_list_merge_info);
static char *xts_process_update_proc (THREAD_ENTRY * thread_p, char *ptr,
				      const UPDATE_PROC_NODE * update_info);
static char *xts_process_delete_proc (THREAD_ENTRY * thread_p, char *ptr,
				      const DELETE_PROC_NODE * delete_proc);
static char *xts_process_insert_proc (THREAD_ENTRY * thread_p, char *ptr,
				      const INSERT_PROC_NODE * insert_proc);
static char *xts_process_outptr_list (THREAD_ENTRY * thread_p, char *ptr,
				      const OUTPTR_LIST * outptr_list);
static char *xts_process_selupd_list (THREAD_ENTRY * thread_p, char *ptr,
				      const SELUPD_LIST * selupd_list);
static char *xts_process_pred_expr (THREAD_ENTRY * thread_p, char *ptr,
				    const PRED_EXPR * pred_expr);
static char *xts_process_partition_info (THREAD_ENTRY * thread_p, char *ptr,
					 const XASL_PARTITION_INFO *
					 partition_info);
static char *xts_process_parts_info (THREAD_ENTRY * thread_p, char *ptr,
				     const XASL_PARTS_INFO * parts_info);
static char *xts_process_pred (THREAD_ENTRY * thread_p, char *ptr,
			       const PRED * pred);
static char *xts_process_eval_term (THREAD_ENTRY * thread_p, char *ptr,
				    const EVAL_TERM * eval_term);
static char *xts_process_comp_eval_term (THREAD_ENTRY * thread_p, char *ptr,
					 const COMP_EVAL_TERM *
					 comp_eval_term);
static char *xts_process_alsm_eval_term (THREAD_ENTRY * thread_p, char *ptr,
					 const ALSM_EVAL_TERM *
					 alsm_eval_term);
static char *xts_process_like_eval_term (THREAD_ENTRY * thread_p, char *ptr,
					 const LIKE_EVAL_TERM *
					 like_eval_term);
static char *xts_process_access_spec_type (THREAD_ENTRY * thread_p, char *ptr,
					   const ACCESS_SPEC_TYPE *
					   access_spec);
static char *xts_process_indx_info (THREAD_ENTRY * thread_p, char *ptr,
				    const INDX_INFO * indx_info);
static char *xts_process_indx_id (THREAD_ENTRY * thread_p, char *ptr,
				  const INDX_ID * indx_id);
static char *xts_process_key_info (THREAD_ENTRY * thread_p, char *ptr,
				   const KEY_INFO * key_info);
static char *xts_process_cls_spec_type (THREAD_ENTRY * thread_p, char *ptr,
					const CLS_SPEC_TYPE * cls_spec);
static char *xts_process_list_spec_type (THREAD_ENTRY * thread_p, char *ptr,
					 const LIST_SPEC_TYPE * list_spec);
static char *xts_process_set_spec_type (THREAD_ENTRY * thread_p, char *ptr,
					const SET_SPEC_TYPE * set_spec);
static char *xts_process_method_spec_type (THREAD_ENTRY * thread_p, char *ptr,
					   const METHOD_SPEC_TYPE *
					   method_spec);
static char *xts_process_list_id (THREAD_ENTRY * thread_p, char *ptr,
				  const QFILE_LIST_ID * list_id);
static char *xts_process_val_list (THREAD_ENTRY * thread_p, char *ptr,
				   const VAL_LIST * val_list);
static char *xts_process_regu_variable (THREAD_ENTRY * thread_p, char *ptr,
					const REGU_VARIABLE * regu_var);
static char *xts_process_attr_descr (THREAD_ENTRY * thread_p, char *ptr,
				     const ATTR_DESCR * attr_descr);
static char *xts_process_pos_descr (THREAD_ENTRY * thread_p, char *ptr,
				    const QFILE_TUPLE_VALUE_POSITION *
				    position_descr);
static char *xts_process_db_value (THREAD_ENTRY * thread_p, char *ptr,
				   const DB_VALUE * value);
static char *xts_process_arith_type (THREAD_ENTRY * thread_p, char *ptr,
				     const ARITH_TYPE * arith);
static char *xts_process_aggregate_type (THREAD_ENTRY * thread_p, char *ptr,
					 const AGGREGATE_TYPE * aggregate);
static char *xts_process_function_type (THREAD_ENTRY * thread_p, char *ptr,
					const FUNCTION_TYPE * function);
static char *xts_process_srlist_id (THREAD_ENTRY * thread_p, char *ptr,
				    const QFILE_SORTED_LIST_ID *
				    sort_list_id);
static char *xts_process_sort_list (THREAD_ENTRY * thread_p, char *ptr,
				    const SORT_LIST * sort_list);
static char *xts_process_method_sig_list (THREAD_ENTRY * thread_p, char *ptr,
					  const METHOD_SIG_LIST *
					  method_sig_list);
static char *xts_process_connectby_proc (THREAD_ENTRY * thread_p, char *ptr,
					 const CONNECTBY_PROC_NODE *
					 connectby_proc);

static int xts_sizeof_xasl_node (THREAD_ENTRY * thread_p,
				 const XASL_NODE * ptr);
static int xts_sizeof_cache_attrinfo (const HEAP_CACHE_ATTRINFO * ptr);
static int xts_sizeof_union_proc (const UNION_PROC_NODE * ptr);
static int xts_sizeof_fetch_proc (const FETCH_PROC_NODE * ptr);
static int xts_sizeof_buildlist_proc (const BUILDLIST_PROC_NODE * ptr);
static int xts_sizeof_buildvalue_proc (const BUILDVALUE_PROC_NODE * ptr);
static int xts_sizeof_mergelist_proc (THREAD_ENTRY * thread_p,
				      const MERGELIST_PROC_NODE * ptr);
static int xts_sizeof_ls_merge_info (const QFILE_LIST_MERGE_INFO * ptr);
static int xts_sizeof_update_proc (const UPDATE_PROC_NODE * ptr);
static int xts_sizeof_delete_proc (const DELETE_PROC_NODE * ptr);
static int xts_sizeof_insert_proc (const INSERT_PROC_NODE * ptr);
static int xts_sizeof_outptr_list (const OUTPTR_LIST * ptr);
static int xts_sizeof_selupd_list (const SELUPD_LIST * ptr);
static int xts_sizeof_pred_expr (THREAD_ENTRY * thread_p,
				 const PRED_EXPR * ptr);
static int xts_sizeof_partition_info (const XASL_PARTITION_INFO * ptr);
static int xts_sizeof_parts_info (const XASL_PARTS_INFO * ptr);
static int xts_sizeof_pred (THREAD_ENTRY * thread_p, const PRED * ptr);
static int xts_sizeof_eval_term (THREAD_ENTRY * thread_p,
				 const EVAL_TERM * ptr);
static int xts_sizeof_comp_eval_term (const COMP_EVAL_TERM * ptr);
static int xts_sizeof_alsm_eval_term (const ALSM_EVAL_TERM * ptr);
static int xts_sizeof_like_eval_term (const LIKE_EVAL_TERM * ptr);
static int xts_sizeof_access_spec_type (THREAD_ENTRY * thread_p,
					const ACCESS_SPEC_TYPE * ptr);
static int xts_sizeof_indx_info (THREAD_ENTRY * thread_p,
				 const INDX_INFO * ptr);
static int xts_sizeof_indx_id (THREAD_ENTRY * thread_p, const INDX_ID * ptr);
static int xts_sizeof_key_info (const KEY_INFO * ptr);
static int xts_sizeof_cls_spec_type (const CLS_SPEC_TYPE * ptr);
static int xts_sizeof_list_spec_type (const LIST_SPEC_TYPE * ptr);
static int xts_sizeof_set_spec_type (const SET_SPEC_TYPE * ptr);
static int xts_sizeof_method_spec_type (const METHOD_SPEC_TYPE * ptr);
static int xts_sizeof_list_id (const QFILE_LIST_ID * ptr);
static int xts_sizeof_val_list (const VAL_LIST * ptr);
static int xts_sizeof_regu_variable (THREAD_ENTRY * thread_p,
				     const REGU_VARIABLE * ptr);
static int xts_sizeof_attr_descr (const ATTR_DESCR * ptr);
static int xts_sizeof_pos_descr (const QFILE_TUPLE_VALUE_POSITION * ptr);
static int xts_sizeof_db_value (const DB_VALUE * ptr);
static int xts_sizeof_arith_type (const ARITH_TYPE * ptr);
static int xts_sizeof_aggregate_type (THREAD_ENTRY * thread_p,
				      const AGGREGATE_TYPE * ptr);
static int xts_sizeof_function_type (const FUNCTION_TYPE * ptr);
static int xts_sizeof_srlist_id (const QFILE_SORTED_LIST_ID * ptr);
static int xts_sizeof_sort_list (const SORT_LIST * ptr);
static int xts_sizeof_method_sig_list (const METHOD_SIG_LIST * ptr);
static int xts_sizeof_connectby_proc (const CONNECTBY_PROC_NODE * ptr);

static int xts_mark_ptr_visited (THREAD_ENTRY * thread_p, const void *ptr,
				 int offset);
static int xts_get_offset_visited_ptr (THREAD_ENTRY * thread_p,
				       const void *ptr);
static void xts_free_visited_ptrs (THREAD_ENTRY * thread_p);
static int xts_reserve_location_in_stream (THREAD_ENTRY * thread_p, int size);

/*
 * xts_map_xasl_to_stream () -
 *   return: if successful, return 0, otherwise non-zero error code
 *   xasl_tree(in)      : pointer to root of XASL tree
 *   xasl_stream(out)   : pointer to xasl stream
 *   xasl_stream_size(out): # of bytes in xasl_stream
 *
 * Note: map the XASL tree into linear byte stream of disk
 * representation. On successful return, `*xasl_stream'
 * will have the address of memory containing the linearly
 * mapped xasl stream, `*xasl_stream_size' will have the
 * # of bytes allocated for the stream
 *
 * Note: the caller should be responsible for free the memory of
 * xasl_stream. the free function should be free_and_init().
 */
int
xts_map_xasl_to_stream (THREAD_ENTRY * thread_p, const XASL_NODE * xasl_tree,
			char **xasl_stream, int *xasl_stream_size)
{
  int offset;
  int header_size, body_size;
  char *p;
  int i;

  if (!xasl_tree || !xasl_stream || !xasl_stream_size)
    {
      return ER_QPROC_INVALID_XASLNODE;
    }

  if (xts_pack_info_ptr (thread_p) == NULL)
    {
      void *pack_info_p;
      pack_info_p = db_private_alloc (thread_p, sizeof (XASL_PACK_INFO));
      if (pack_info_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (XASL_PACK_INFO));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      memset (pack_info_p, 0, sizeof (XASL_PACK_INFO));
      xts_pack_info_ptr (thread_p) = pack_info_p;
    }

  xts_Xasl_errcode (thread_p) = NO_ERROR;

  /* reserve space for new XASL format */
  header_size = sizeof (int)	/* xasl->dbval_cnt */
    + sizeof (OID)		/* xasl->creator_oid */
    + sizeof (int)		/* xasl->n_oid_list */
    + sizeof (OID) * xasl_tree->n_oid_list	/* xasl->class_oid_list */
    + sizeof (int) * xasl_tree->n_oid_list;	/* xasl->repr_id_list */

  offset = sizeof (int)		/* [size of header data] */
    + header_size		/* [header data] */
    + sizeof (int);		/* [size of body data] */
  offset = MAKE_ALIGN (offset);

  xts_reserve_location_in_stream (thread_p, offset);

  /* save XASL tree into body data of the stream buffer */
  if (xts_save_xasl_node (thread_p, xasl_tree) == ER_FAILED)
    {
      if (xts_Stream_buffer (thread_p))
	{
	  db_private_free_and_init (thread_p, xts_Stream_buffer (thread_p));
	}
      goto end;
    }

  /* make header size and data of new XASL format */
  p = or_pack_int (xts_Stream_buffer (thread_p), header_size);
  p = or_pack_int (p, xasl_tree->dbval_cnt);
  p = or_pack_oid (p, (OID *) (&xasl_tree->creator_oid));
  p = or_pack_int (p, xasl_tree->n_oid_list);
  for (i = 0; i < xasl_tree->n_oid_list; i++)
    {
      p = or_pack_oid (p, &xasl_tree->class_oid_list[i]);
    }
  for (i = 0; i < xasl_tree->n_oid_list; i++)
    {
      p = or_pack_int (p, xasl_tree->repr_id_list[i]);
    }

  /* set body size of new XASL format */
  body_size = xts_Free_offset_in_stream (thread_p) - offset;
  p = or_pack_int (p, body_size);

  /* set result */
  *xasl_stream = xts_Stream_buffer (thread_p);
  *xasl_stream_size = xts_Free_offset_in_stream (thread_p);

end:

#if defined(SERVER_MODE)
  xts_final (thread_p);
#else
  /* free all memories */
  xts_free_visited_ptrs (thread_p);

  xts_Stream_buffer (thread_p) = NULL;
  xts_Stream_size (thread_p) = 0;
  xts_Free_offset_in_stream (thread_p) = 0;
#endif

  return xts_Xasl_errcode (thread_p);
}

/*
 * xts_final () -
 *   return:
 *
 * Note: Added for the PC so we have a way to free up all the storage that
 * is currently in use when a database "connection" is closed.
 * Called by qp_final() on the PC but not for the workstations.
 */
void
xts_final (THREAD_ENTRY * thread_p)
{
  int i;

  if (xts_pack_info_ptr (thread_p) != NULL)
    {
      for (i = 0; i < MAX_PTR_BLOCKS; i++)
	{
	  if (xts_Ptr_blocks (thread_p)[i] != NULL)
	    {
	      db_private_free_and_init (thread_p,
					xts_Ptr_blocks (thread_p)[i]);
	    }
	  xts_Ptr_blocks (thread_p)[i] = NULL;
	  xts_Ptr_lwm (thread_p)[i] = 0;
	  xts_Ptr_max (thread_p)[i] = 0;
	}

      db_private_free_and_init (thread_p, xts_pack_info_ptr (thread_p));
    }
}

static int
xts_save_aggregate_type (THREAD_ENTRY * thread_p,
			 const AGGREGATE_TYPE * aggregate)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*aggregate) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (aggregate == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, aggregate);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_aggregate_type (thread_p, aggregate);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, aggregate, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_aggregate_type (thread_p, buf_p, aggregate) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_function_type (THREAD_ENTRY * thread_p,
			const FUNCTION_TYPE * function)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*function) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (function == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, function);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_function_type (function);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, function, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_function_type (thread_p, buf_p, function) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_srlist_id (THREAD_ENTRY * thread_p,
		    const QFILE_SORTED_LIST_ID * sort_list_id)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*sort_list_id) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (sort_list_id == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, sort_list_id);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_srlist_id (sort_list_id);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, sort_list_id, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_srlist_id (thread_p, buf_p, sort_list_id) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_list_id (THREAD_ENTRY * thread_p, const QFILE_LIST_ID * list_id)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*list_id) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (list_id == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, list_id);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_list_id (list_id);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, list_id, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  buf = xts_process_list_id (thread_p, buf_p, list_id);
  if (buf == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }
  assert (buf <= buf_p + size);

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_arith_type (THREAD_ENTRY * thread_p, const ARITH_TYPE * arithmetic)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*arithmetic) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (arithmetic == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, arithmetic);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_arith_type (arithmetic);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, arithmetic, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_arith_type (thread_p, buf_p, arithmetic) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_indx_info (THREAD_ENTRY * thread_p, const INDX_INFO * indx_info)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*indx_info) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (indx_info == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, indx_info);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_indx_info (thread_p, indx_info);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, indx_info, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_indx_info (thread_p, buf_p, indx_info) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_outptr_list (THREAD_ENTRY * thread_p,
		      const OUTPTR_LIST * outptr_list)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*outptr_list) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (outptr_list == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, outptr_list);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_outptr_list (outptr_list);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, outptr_list, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  buf = xts_process_outptr_list (thread_p, buf_p, outptr_list);
  if (buf == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }
  assert (buf <= buf_p + size);

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_selupd_list (THREAD_ENTRY * thread_p,
		      const SELUPD_LIST * selupd_list)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*selupd_list) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (selupd_list == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, selupd_list);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_selupd_list (selupd_list);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, selupd_list, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  buf = xts_process_selupd_list (thread_p, buf_p, selupd_list);
  if (buf == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }
  assert (buf <= buf_p + size);

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_pred_expr (THREAD_ENTRY * thread_p, const PRED_EXPR * pred_expr)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*pred_expr) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (pred_expr == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, pred_expr);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_pred_expr (thread_p, pred_expr);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, pred_expr, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  buf = xts_process_pred_expr (thread_p, buf_p, pred_expr);
  if (buf == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }
  assert (buf <= buf_p + size);

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_partition_info (THREAD_ENTRY * thread_p,
			 const XASL_PARTITION_INFO * partition_info)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*partition_info) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (partition_info == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, partition_info);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_partition_info (partition_info);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, partition_info, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_partition_info (thread_p, buf_p, partition_info) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_parts_info (THREAD_ENTRY * thread_p,
		     const XASL_PARTS_INFO * parts_info)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*parts_info) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (parts_info == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, parts_info);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_parts_info (parts_info);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, parts_info, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_parts_info (thread_p, buf_p, parts_info) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_parts_array (THREAD_ENTRY * thread_p,
		      XASL_PARTS_INFO ** parts_info_array, int nelements)
{
  int offset;
  int *offset_array;
  int i;

  offset_array =
    (int *) db_private_alloc (thread_p, sizeof (int) * nelements);
  if (offset_array == NULL)
    {
      return ER_FAILED;
    }

  for (i = 0; i < nelements; i++)
    {
      offset_array[i] = xts_save_parts_info (thread_p, parts_info_array[i]);
      if (offset_array[i] == ER_FAILED)
	{
	  db_private_free_and_init (thread_p, offset_array);
	  return ER_FAILED;
	}
    }

  offset = xts_save_int_array (thread_p, offset_array, nelements);

  db_private_free_and_init (thread_p, offset_array);
  return offset;
}

static int
xts_save_partition_array (THREAD_ENTRY * thread_p,
			  XASL_PARTITION_INFO ** partition_info_array,
			  int nelements)
{
  int offset;
  int *offset_array;
  int i;

  offset_array =
    (int *) db_private_alloc (thread_p, sizeof (int) * nelements);
  if (offset_array == NULL)
    {
      return ER_FAILED;
    }

  for (i = 0; i < nelements; i++)
    {
      offset_array[i] = ((partition_info_array[i] == NULL) ?
			 0 :
			 xts_save_partition_info (thread_p,
						  partition_info_array[i]));
      if (offset_array[i] == ER_FAILED)
	{
	  db_private_free_and_init (thread_p, offset_array);
	  return ER_FAILED;
	}
    }

  offset = xts_save_int_array (thread_p, offset_array, nelements);

  db_private_free_and_init (thread_p, offset_array);
  return offset;
}

static int
xts_save_regu_variable (THREAD_ENTRY * thread_p,
			const REGU_VARIABLE * regu_var)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*regu_var) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (regu_var == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, regu_var);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_regu_variable (thread_p, regu_var);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, regu_var, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_regu_variable (thread_p, buf_p, regu_var) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_sort_list (THREAD_ENTRY * thread_p, const SORT_LIST * sort_list)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*sort_list) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (sort_list == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, sort_list);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_sort_list (sort_list);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, sort_list, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  buf = xts_process_sort_list (thread_p, buf_p, sort_list);
  if (buf == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }
  assert (buf <= buf_p + size);

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_val_list (THREAD_ENTRY * thread_p, const VAL_LIST * val_list)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*val_list) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (val_list == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, val_list);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_val_list (val_list);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, val_list, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  buf = xts_process_val_list (thread_p, buf_p, val_list);
  if (buf == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }
  assert (buf <= buf_p + size);

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_db_value (THREAD_ENTRY * thread_p, const DB_VALUE * value)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*value) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (value == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, value);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_db_value (value);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, value, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  buf = xts_process_db_value (thread_p, buf_p, value);
  if (buf == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }
  assert (buf <= buf_p + size);

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_xasl_node (THREAD_ENTRY * thread_p, const XASL_NODE * xasl)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*xasl) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (xasl == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, xasl);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_xasl_node (thread_p, xasl);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, xasl, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  buf = xts_process_xasl_node (thread_p, buf_p, xasl);
  if (buf == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }
  assert (buf <= buf_p + size);

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_cache_attrinfo (THREAD_ENTRY * thread_p,
			 const HEAP_CACHE_ATTRINFO * attrinfo)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*attrinfo) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (attrinfo == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, attrinfo);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_cache_attrinfo (attrinfo);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, attrinfo, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_cache_attrinfo (thread_p, buf_p) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

#if 0
/*
 * there are currently no pointers to these type of structures in xasl
 * so there is no need to have a separate restore function.
 */

static int
xts_save_merge_list_info (THREAD_ENTRY * thread_p,
			  const MERGELIST_PROC_NODE * mergelist_proc)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*mergelist_proc) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (mergelist_proc == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (mergelist_proc);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_merge_list_info (mergelist_proc);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (mergelist_proc, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_merge_list_info (buf_p, mergelist_proc) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_ls_merge_info (THREAD_ENTRY * thread_p,
			const QFILE_LIST_MERGE_INFO * list_merge_info)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*list_merge_info) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (list_merge_info == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (list_merge_info);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_ls_merge_info (list_merge_info);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (list_merge_info, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_ls_merge_info (buf_p, list_merge_info) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_update_info (THREAD_ENTRY * thread_p,
		      const UPDATE_PROC_NODE * update_proc)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*update_proc) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (update_proc == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (update_proc);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_update_info (update_proc);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (update_proc, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_update_info (buf_p, update_proc) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_delete_info (THREAD_ENTRY * thread_p,
		      const DELETE_PROC_NODE * delete_proc)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*delete_proc) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (delete_proc == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (delete_proc);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_delete_info (delete_proc);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (delete_proc, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_delete_info (buf_p, delete_proc) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_insert_info (THREAD_ENTRY * thread_p,
		      const INSERT_PROC_NODE * insert_proc)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*insert_proc) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (insert_proc == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (insert_proc);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_insert_proc (insert_proc);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (insert_proc, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_insert_info (buf_p, insert_proc) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}
#endif

static int
xts_save_method_sig_list (THREAD_ENTRY * thread_p,
			  const METHOD_SIG_LIST * method_sig_list)
{
  int offset;
  int size;
  OR_ALIGNED_BUF (sizeof (*method_sig_list) * 2) a_buf;
  char *buf = OR_ALIGNED_BUF_START (a_buf);
  char *buf_p = NULL;
  bool is_buf_alloced = false;

  if (method_sig_list == NULL)
    {
      return NO_ERROR;
    }

  offset = xts_get_offset_visited_ptr (thread_p, method_sig_list);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  size = xts_sizeof_method_sig_list (method_sig_list);
  if (size == ER_FAILED)
    {
      return ER_FAILED;
    }

  offset = xts_reserve_location_in_stream (thread_p, size);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, method_sig_list,
			       offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  if (size <= (int) OR_ALIGNED_BUF_SIZE (a_buf))
    {
      buf_p = buf;
    }
  else
    {
      buf_p = (char *) db_private_alloc (thread_p, size);
      if (buf_p == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}

      is_buf_alloced = true;
    }

  if (xts_process_method_sig_list (thread_p, buf_p, method_sig_list) == NULL)
    {
      offset = ER_FAILED;
      goto end;
    }

  memcpy (&xts_Stream_buffer (thread_p)[offset], buf_p, size);

end:
  if (is_buf_alloced)
    {
      db_private_free_and_init (thread_p, buf_p);
    }

  return offset;
}

static int
xts_save_string (THREAD_ENTRY * thread_p, const char *string)
{
  int offset;
  int string_length;

  offset = xts_get_offset_visited_ptr (thread_p, string);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  string_length = or_packed_string_length (string);

  offset = xts_reserve_location_in_stream (thread_p, string_length);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, string, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  or_pack_string (&xts_Stream_buffer (thread_p)[offset], string);

  return offset;
}

#if defined(ENABLE_UNUSED_FUNCTION)
static int
xts_save_string_with_length (THREAD_ENTRY * thread_p, const char *string,
			     int length)
{
  int offset;

  offset = xts_get_offset_visited_ptr (thread_p, string);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  offset =
    xts_reserve_location_in_stream (thread_p, or_align_length (length));
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, string, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  or_pack_string_with_length (&xts_Stream_buffer (thread_p)[offset],
			      (char *) string, length);

  return offset;
}

static int
xts_save_input_vals (THREAD_ENTRY * thread_p, const char *input_vals_p,
		     int length)
{
  int offset;

  offset = xts_get_offset_visited_ptr (thread_p, input_vals_p);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  offset = xts_reserve_location_in_stream (thread_p, length);
  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, input_vals_p, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  memmove (&xts_Stream_buffer (thread_p)[offset], (char *) input_vals_p,
	   length);

  return offset;
}
#endif

static int
xts_save_db_value_array (THREAD_ENTRY * thread_p,
			 DB_VALUE ** db_value_array_p, int nelements)
{
  int offset;
  int *offset_array;
  int i;

  offset_array =
    (int *) db_private_alloc (thread_p, sizeof (int) * nelements);
  if (offset_array == NULL)
    {
      return ER_FAILED;
    }

  for (i = 0; i < nelements; i++)
    {
      offset_array[i] = xts_save_db_value (thread_p, db_value_array_p[i]);
      if (offset_array[i] == ER_FAILED)
	{
	  offset = ER_FAILED;
	  goto end;
	}
    }

  offset = xts_save_int_array (thread_p, offset_array, nelements);

end:
  db_private_free_and_init (thread_p, offset_array);

  return offset;
}

#if defined (ENABLE_UNUSED_FUNCTION)
static int
xts_save_domain_array (THREAD_ENTRY * thread_p, TP_DOMAIN ** domain_array_p,
		       int nelements)
{
  int offset, i, len;
  char *ptr;

  if (domain_array_p == NULL)
    {
      return 0;
    }

  for (len = 0, i = 0; i < nelements; i++)
    {
      len += or_packed_domain_size (domain_array_p[i], 0);
    }

  offset = xts_reserve_location_in_stream (thread_p, len);
  if (offset == ER_FAILED)
    {
      return ER_FAILED;
    }

  ptr = &xts_Stream_buffer (thread_p)[offset];
  for (i = 0; i < nelements; i++)
    {
      ptr = or_pack_domain (ptr, domain_array_p[i], 0, 0);
    }

  return offset;
}
#endif

static int
xts_save_int_array (THREAD_ENTRY * thread_p, int *int_array, int nelements)
{
  int offset, i;
  char *ptr;

  if (int_array == NULL)
    {
      return 0;
    }

  offset = xts_reserve_location_in_stream (thread_p, OR_INT_SIZE * nelements);
  if (offset == ER_FAILED)
    {
      return ER_FAILED;
    }

  ptr = &xts_Stream_buffer (thread_p)[offset];
  for (i = 0; i < nelements; ++i)
    {
      ptr = or_pack_int (ptr, int_array[i]);
    }

  return offset;
}

static int
xts_save_hfid_array (THREAD_ENTRY * thread_p, HFID * hfid_array,
		     int nelements)
{
  int offset, i;
  char *ptr;

  if (hfid_array == NULL)
    {
      return 0;
    }

  offset =
    xts_reserve_location_in_stream (thread_p, OR_HFID_SIZE * nelements);
  if (offset == ER_FAILED)
    {
      return ER_FAILED;
    }

  ptr = &xts_Stream_buffer (thread_p)[offset];
  for (i = 0; i < nelements; ++i)
    {
      ptr = or_pack_hfid (ptr, &hfid_array[i]);
    }

  return offset;
}

static int
xts_save_oid_array (THREAD_ENTRY * thread_p, OID * oid_array, int nelements)
{
  int offset, i;
  char *ptr;

  if (oid_array == NULL)
    {
      return 0;
    }

  offset = xts_reserve_location_in_stream (thread_p, OR_OID_SIZE * nelements);
  if (offset == ER_FAILED)
    {
      return ER_FAILED;
    }

  ptr = &xts_Stream_buffer (thread_p)[offset];
  for (i = 0; i < nelements; ++i)
    {
      ptr = or_pack_oid (ptr, &oid_array[i]);
    }

  return offset;
}

/*
 * Save the regu_variable_list as an array to avoid recursion in the server.
 * Pack the array size first, then the array.
 */
#define OFFSET_BUFFER_SIZE 32

static int
xts_save_regu_variable_list (THREAD_ENTRY * thread_p,
			     const REGU_VARIABLE_LIST regu_var_list)
{
  int offset;
  int *regu_var_offset_table;
  int offset_local_buffer[OFFSET_BUFFER_SIZE];
  REGU_VARIABLE_LIST regu_var_p;
  int nelements, i;

  if (regu_var_list == NULL)
    {
      return 0;
    }

  offset = xts_get_offset_visited_ptr (thread_p, regu_var_list);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  nelements = 0;
  for (regu_var_p = regu_var_list; regu_var_p; regu_var_p = regu_var_p->next)
    {
      ++nelements;
    }

  if (OFFSET_BUFFER_SIZE <= nelements)
    {
      regu_var_offset_table =
	(int *) db_private_alloc (thread_p, sizeof (int) * (nelements + 1));
      if (regu_var_offset_table == NULL)
	{
	  return ER_FAILED;
	}
    }
  else
    {
      regu_var_offset_table = offset_local_buffer;
    }

  i = 0;
  regu_var_offset_table[i++] = nelements;
  for (regu_var_p = regu_var_list; regu_var_p;
       ++i, regu_var_p = regu_var_p->next)
    {
      regu_var_offset_table[i] =
	xts_save_regu_variable (thread_p, &regu_var_p->value);
      if (regu_var_offset_table[i] == ER_FAILED)
	{
	  if (regu_var_offset_table != offset_local_buffer)
	    {
	      db_private_free_and_init (thread_p, regu_var_offset_table);
	    }
	  return ER_FAILED;
	}
    }

  offset =
    xts_save_int_array (thread_p, regu_var_offset_table, nelements + 1);

  if (regu_var_offset_table != offset_local_buffer)
    {
      db_private_free_and_init (thread_p, regu_var_offset_table);
    }

  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, regu_var_list, offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  return offset;
}

static int
xts_save_regu_varlist_list (THREAD_ENTRY * thread_p,
			    const REGU_VARLIST_LIST regu_var_list_list)
{
  int offset;
  int *regu_var_list_offset_table;
  int offset_local_buffer[OFFSET_BUFFER_SIZE];
  REGU_VARLIST_LIST regu_var_list_p;
  int nelements, i;

  if (regu_var_list_list == NULL)
    {
      return 0;
    }

  offset = xts_get_offset_visited_ptr (thread_p, regu_var_list_list);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  nelements = 0;
  for (regu_var_list_p = regu_var_list_list; regu_var_list_p;
       regu_var_list_p = regu_var_list_p->next)
    {
      ++nelements;
    }

  if (OFFSET_BUFFER_SIZE <= nelements)
    {
      regu_var_list_offset_table =
	(int *) db_private_alloc (thread_p, sizeof (int) * (nelements + 1));
      if (regu_var_list_offset_table == NULL)
	{
	  return ER_FAILED;
	}
    }
  else
    {
      regu_var_list_offset_table = offset_local_buffer;
    }

  i = 0;
  regu_var_list_offset_table[i++] = nelements;
  for (regu_var_list_p = regu_var_list_list; regu_var_list_p;
       ++i, regu_var_list_p = regu_var_list_p->next)
    {
      regu_var_list_offset_table[i] =
	xts_save_regu_variable_list (thread_p, regu_var_list_p->list);
      if (regu_var_list_offset_table[i] == ER_FAILED)
	{
	  if (regu_var_list_offset_table != offset_local_buffer)
	    {
	      db_private_free_and_init (thread_p, regu_var_list_offset_table);
	    }
	  return ER_FAILED;
	}
    }

  offset =
    xts_save_int_array (thread_p, regu_var_list_offset_table, nelements + 1);

  if (regu_var_list_offset_table != offset_local_buffer)
    {
      db_private_free_and_init (thread_p, regu_var_list_offset_table);
    }

  if (offset == ER_FAILED
      || xts_mark_ptr_visited (thread_p, regu_var_list_list,
			       offset) == ER_FAILED)
    {
      return ER_FAILED;
    }

  return offset;
}

static int
xts_save_key_range_array (THREAD_ENTRY * thread_p,
			  const KEY_RANGE * key_range_array, int nelements)
{
  int offset, i, j;
  int *key_range_offset_table;

  if (key_range_array == NULL)
    {
      return 0;
    }

  offset = xts_get_offset_visited_ptr (thread_p, key_range_array);
  if (offset != ER_FAILED)
    {
      return offset;
    }

  key_range_offset_table =
    (int *) db_private_alloc (thread_p, sizeof (int) * 3 * nelements);
  if (key_range_offset_table == NULL)
    {
      return ER_FAILED;
    }

  for (i = 0, j = 0; i < nelements; i++, j++)
    {
      key_range_offset_table[j] = key_range_array[i].range;

      if (key_range_array[i].key1)
	{
	  key_range_offset_table[++j] =
	    xts_save_regu_variable (thread_p, key_range_array[i].key1);
	  if (key_range_offset_table[j] == ER_FAILED)
	    {
	      db_private_free_and_init (thread_p, key_range_offset_table);
	      return ER_FAILED;
	    }
	}
      else
	{
	  key_range_offset_table[++j] = 0;
	}

      if (key_range_array[i].key2)
	{
	  key_range_offset_table[++j] =
	    xts_save_regu_variable (thread_p, key_range_array[i].key2);
	  if (key_range_offset_table[j] == ER_FAILED)
	    {
	      db_private_free_and_init (thread_p, key_range_offset_table);
	      return ER_FAILED;
	    }
	}
      else
	{
	  key_range_offset_table[++j] = 0;
	}
    }

  offset =
    xts_save_int_array (thread_p, key_range_offset_table, 3 * nelements);

  db_private_free_and_init (thread_p, key_range_offset_table);

  return offset;
}

static char *
xts_process_xasl_node (THREAD_ENTRY * thread_p, char *ptr,
		       const XASL_NODE * xasl)
{
  int offset;
  int cnt;
  ACCESS_SPEC_TYPE *access_spec = NULL;

  assert (PTR_ALIGN (ptr, MAX_ALIGNMENT) == ptr);

  ptr = or_pack_int (ptr, xasl->type);

  ptr = or_pack_int (ptr, xasl->flag);

  if (xasl->list_id == NULL)
    {
      ptr = or_pack_int (ptr, 0);
    }
  else
    {
      offset = xts_save_list_id (thread_p, xasl->list_id);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
    }

  offset = xts_save_sort_list (thread_p, xasl->after_iscan_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_sort_list (thread_p, xasl->orderby_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_pred_expr (thread_p, xasl->ordbynum_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_db_value (thread_p, xasl->ordbynum_val);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, xasl->ordbynum_flag);

  offset = xts_save_val_list (thread_p, xasl->single_tuple);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, xasl->is_single_tuple);

  ptr = or_pack_int (ptr, xasl->option);

  offset = xts_save_outptr_list (thread_p, xasl->outptr_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_outptr_list (thread_p, xasl->remote_outptr_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_selupd_list (thread_p, xasl->selected_upd_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  for (cnt = 0, access_spec = xasl->spec_list; access_spec;
       access_spec = access_spec->next, cnt++)
    ;				/* empty */
  ptr = or_pack_int (ptr, cnt);

  for (access_spec = xasl->spec_list; access_spec;
       access_spec = access_spec->next)
    {
      ptr = xts_process_access_spec_type (thread_p, ptr, access_spec);
    }

  for (cnt = 0, access_spec = xasl->merge_spec; access_spec;
       access_spec = access_spec->next, cnt++)
    ;				/* empty */
  ptr = or_pack_int (ptr, cnt);

  for (access_spec = xasl->merge_spec; access_spec;
       access_spec = access_spec->next)
    {
      ptr = xts_process_access_spec_type (thread_p, ptr, access_spec);
    }

  offset = xts_save_val_list (thread_p, xasl->val_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_val_list (thread_p, xasl->merge_val_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_xasl_node (thread_p, xasl->aptr_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_xasl_node (thread_p, xasl->bptr_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_xasl_node (thread_p, xasl->dptr_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_pred_expr (thread_p, xasl->after_join_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_pred_expr (thread_p, xasl->if_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_pred_expr (thread_p, xasl->instnum_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_db_value (thread_p, xasl->instnum_val);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, xasl->instnum_flag);

  offset = xts_save_xasl_node (thread_p, xasl->fptr_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_xasl_node (thread_p, xasl->scan_ptr);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_xasl_node (thread_p, xasl->connect_by_ptr);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_db_value (thread_p, xasl->level_val);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable (thread_p, xasl->level_regu);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_db_value (thread_p, xasl->isleaf_val);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable (thread_p, xasl->isleaf_regu);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_db_value (thread_p, xasl->iscycle_val);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable (thread_p, xasl->iscycle_regu);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  for (cnt = 0, access_spec = xasl->curr_spec; access_spec;
       access_spec = access_spec->next, cnt++)
    ;				/* empty */
  ptr = or_pack_int (ptr, cnt);

  for (access_spec = xasl->curr_spec; access_spec;
       access_spec = access_spec->next)
    {
      ptr = xts_process_access_spec_type (thread_p, ptr, access_spec);
    }

  ptr = or_pack_int (ptr, xasl->next_scan_on);

  ptr = or_pack_int (ptr, xasl->next_scan_block_on);

  ptr = or_pack_int (ptr, xasl->cat_fetched);

  ptr = or_pack_int (ptr, xasl->composite_locking);

  /*
   * NOTE that the composite lock block is strictly a server side block
   * and is not packed.
   */

  switch (xasl->type)
    {
    case BUILDLIST_PROC:
      ptr = xts_process_buildlist_proc (thread_p, ptr, &xasl->proc.buildlist);
      break;

    case BUILDVALUE_PROC:
      ptr =
	xts_process_buildvalue_proc (thread_p, ptr, &xasl->proc.buildvalue);
      break;

    case MERGELIST_PROC:
      ptr = xts_process_mergelist_proc (thread_p, ptr, &xasl->proc.mergelist);
      break;

    case CONNECTBY_PROC:
      ptr =
	xts_process_connectby_proc (thread_p, ptr, &xasl->proc.connect_by);
      break;

    case UPDATE_PROC:
      ptr = xts_process_update_proc (thread_p, ptr, &xasl->proc.update);
      break;

    case DELETE_PROC:
      ptr = xts_process_delete_proc (thread_p, ptr, &xasl->proc.delete_);
      break;

    case INSERT_PROC:
      ptr = xts_process_insert_proc (thread_p, ptr, &xasl->proc.insert);
      break;

    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:
      ptr = xts_process_union_proc (thread_p, ptr, &xasl->proc.union_);
      break;

    case OBJFETCH_PROC:
    case SETFETCH_PROC:
      ptr = xts_process_fetch_proc (thread_p, ptr, &xasl->proc.fetch);
      break;

    case SCAN_PROC:
      break;

    case DO_PROC:
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return NULL;
    }

  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_pack_int (ptr, xasl->projected_size);
  ptr = or_pack_double (ptr, xasl->cardinality);

  ptr = or_pack_int (ptr, (int) xasl->iscan_oid_order);

  if (xasl->qstmt)
    {
      offset = xts_save_string (thread_p, xasl->qstmt);
    }
  else
    {
      offset = xts_save_string (thread_p, "");	/* because restore_xxx() cannot handle
						   NULL pointer */
    }
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_xasl_node (thread_p, xasl->next);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_cache_attrinfo (THREAD_ENTRY * thread_p, char *ptr)
{
  /*
   * We don't need to pack anything here, it is strictly a server side
   * structure.  Unfortunately, we must send something or else the ptrs
   * to this structure might conflict with a structure that might be
   * packed after it.  To avoid this we'll pack a single zero.
   */
  ptr = or_pack_int (ptr, 0);

  return ptr;
}

static char *
xts_process_union_proc (THREAD_ENTRY * thread_p, char *ptr,
			const UNION_PROC_NODE * union_proc)
{
  int offset;

  offset = xts_save_xasl_node (thread_p, union_proc->left);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_xasl_node (thread_p, union_proc->right);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_fetch_proc (THREAD_ENTRY * thread_p, char *ptr,
			const FETCH_PROC_NODE * obj_set_fetch_proc)
{
  int offset;

  offset = xts_save_db_value (thread_p, obj_set_fetch_proc->arg);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, (int) obj_set_fetch_proc->fetch_res);

  offset = xts_save_pred_expr (thread_p, obj_set_fetch_proc->set_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, (int) obj_set_fetch_proc->ql_flag);

  return ptr;
}

static char *
xts_process_buildlist_proc (THREAD_ENTRY * thread_p, char *ptr,
			    const BUILDLIST_PROC_NODE * build_list_proc)
{
  int offset;

  /* ptr->output_columns not sent to server */

  offset = xts_save_xasl_node (thread_p, build_list_proc->eptr_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_sort_list (thread_p, build_list_proc->groupby_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_sort_list (thread_p, build_list_proc->after_groupby_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_outptr_list (thread_p, build_list_proc->g_outptr_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p, build_list_proc->g_regu_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_val_list (thread_p, build_list_proc->g_val_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_pred_expr (thread_p, build_list_proc->g_having_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_pred_expr (thread_p, build_list_proc->g_grbynum_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_db_value (thread_p, build_list_proc->g_grbynum_val);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, build_list_proc->g_grbynum_flag);
  ptr = or_pack_int (ptr, build_list_proc->g_with_rollup);

  offset = xts_save_aggregate_type (thread_p, build_list_proc->g_agg_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_arith_type (thread_p, build_list_proc->g_outarith_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_buildvalue_proc (THREAD_ENTRY * thread_p, char *ptr,
			     const BUILDVALUE_PROC_NODE * build_value_proc)
{
  int offset;

  offset = xts_save_pred_expr (thread_p, build_value_proc->having_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_db_value (thread_p, build_value_proc->grbynum_val);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_aggregate_type (thread_p, build_value_proc->agg_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_arith_type (thread_p, build_value_proc->outarith_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, build_value_proc->is_always_false);

  return ptr;
}

static char *
xts_process_mergelist_proc (THREAD_ENTRY * thread_p, char *ptr,
			    const MERGELIST_PROC_NODE * merge_list_info)
{
  int offset;
  int cnt;
  ACCESS_SPEC_TYPE *access_spec = NULL;

  offset = xts_save_xasl_node (thread_p, merge_list_info->outer_xasl);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  for (cnt = 0, access_spec = merge_list_info->outer_spec_list; access_spec;
       access_spec = access_spec->next, cnt++)
    ;				/* empty */
  ptr = or_pack_int (ptr, cnt);

  for (access_spec = merge_list_info->outer_spec_list; access_spec;
       access_spec = access_spec->next)
    {
      ptr = xts_process_access_spec_type (thread_p, ptr, access_spec);
    }

  offset = xts_save_val_list (thread_p, merge_list_info->outer_val_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_xasl_node (thread_p, merge_list_info->inner_xasl);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  for (cnt = 0, access_spec = merge_list_info->inner_spec_list; access_spec;
       access_spec = access_spec->next, cnt++)
    ;				/* empty */
  ptr = or_pack_int (ptr, cnt);

  for (access_spec = merge_list_info->inner_spec_list; access_spec;
       access_spec = access_spec->next)
    {
      ptr = xts_process_access_spec_type (thread_p, ptr, access_spec);
    }

  offset = xts_save_val_list (thread_p, merge_list_info->inner_val_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return xts_process_ls_merge_info (thread_p, ptr,
				    &merge_list_info->ls_merge);
}

static char *
xts_process_ls_merge_info (THREAD_ENTRY * thread_p, char *ptr,
			   const QFILE_LIST_MERGE_INFO *
			   qfile_list_merge_info)
{
  int offset;

  ptr = or_pack_int (ptr, qfile_list_merge_info->join_type);

  ptr = or_pack_int (ptr, qfile_list_merge_info->single_fetch);

  ptr = or_pack_int (ptr, qfile_list_merge_info->ls_column_cnt);

  offset =
    xts_save_int_array (thread_p, qfile_list_merge_info->ls_outer_column,
			qfile_list_merge_info->ls_column_cnt);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_int_array (thread_p, qfile_list_merge_info->ls_outer_unique,
			qfile_list_merge_info->ls_column_cnt);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_int_array (thread_p, qfile_list_merge_info->ls_inner_column,
			qfile_list_merge_info->ls_column_cnt);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_int_array (thread_p, qfile_list_merge_info->ls_inner_unique,
			qfile_list_merge_info->ls_column_cnt);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, qfile_list_merge_info->ls_pos_cnt);

  offset = xts_save_int_array (thread_p, qfile_list_merge_info->ls_pos_list,
			       qfile_list_merge_info->ls_pos_cnt);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_int_array (thread_p, qfile_list_merge_info->ls_outer_inner_list,
			qfile_list_merge_info->ls_pos_cnt);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_update_proc (THREAD_ENTRY * thread_p, char *ptr,
			 const UPDATE_PROC_NODE * update_info)
{
  int offset;

  ptr = or_pack_int (ptr, update_info->no_classes);

  offset =
    xts_save_oid_array (thread_p, update_info->class_oid,
			update_info->no_classes);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_hfid_array (thread_p, update_info->class_hfid,
			 update_info->no_classes);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_int_array (thread_p, update_info->node_ids,
			update_info->no_classes);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, update_info->no_vals);

  ptr = or_pack_int (ptr, update_info->no_consts);

  offset = xts_save_int_array (thread_p, update_info->att_id,
			       update_info->no_classes *
			       update_info->no_vals);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  if (update_info->no_consts > 0)
    {
      offset = xts_save_db_value_array (thread_p, update_info->consts,
					update_info->no_consts);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
    }
  else
    {
      ptr = or_pack_int (ptr, 0);
    }

  offset = xts_save_pred_expr (thread_p, update_info->cons_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, update_info->has_uniques);

  ptr = or_pack_int (ptr, update_info->waitsecs);

  ptr = or_pack_int (ptr, update_info->no_logging);

  ptr = or_pack_int (ptr, update_info->release_lock);

  offset = xts_save_partition_array (thread_p, update_info->partition,
				     update_info->no_classes);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_delete_proc (THREAD_ENTRY * thread_p, char *ptr,
			 const DELETE_PROC_NODE * delete_info)
{
  int offset;

  ptr = or_pack_int (ptr, delete_info->no_classes);

  offset =
    xts_save_oid_array (thread_p, delete_info->class_oid,
			delete_info->no_classes);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_hfid_array (thread_p, delete_info->class_hfid,
			 delete_info->no_classes);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_int_array (thread_p, delete_info->node_ids,
			delete_info->no_classes);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, delete_info->waitsecs);

  ptr = or_pack_int (ptr, delete_info->no_logging);

  ptr = or_pack_int (ptr, delete_info->release_lock);

  return ptr;
}

static char *
xts_process_insert_proc (THREAD_ENTRY * thread_p, char *ptr,
			 const INSERT_PROC_NODE * insert_info)
{
  int offset;

  ptr = or_pack_oid (ptr, (OID *) & insert_info->class_oid);

  ptr = or_pack_oid (ptr, (OID *) & insert_info->proxy_oid);

  ptr = or_pack_hfid (ptr, &insert_info->class_hfid);

  ptr = or_pack_int (ptr, insert_info->node_id);

  ptr = or_pack_int (ptr, insert_info->no_vals);

  offset =
    xts_save_int_array (thread_p, insert_info->att_id, insert_info->no_vals);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_pred_expr (thread_p, insert_info->cons_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, insert_info->has_uniques);

  ptr = or_pack_int (ptr, insert_info->waitsecs);

  ptr = or_pack_int (ptr, insert_info->no_logging);

  ptr = or_pack_int (ptr, insert_info->release_lock);

  ptr = or_pack_int (ptr, insert_info->do_replace);

  ptr = or_pack_int (ptr, insert_info->dup_key_oid_var_index);

  offset = xts_save_partition_info (thread_p, insert_info->partition);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_outptr_list (THREAD_ENTRY * thread_p, char *ptr,
			 const OUTPTR_LIST * outptr_list)
{
  int offset;

  ptr = or_pack_int (ptr, outptr_list->valptr_cnt);

  offset = xts_save_regu_variable_list (thread_p, outptr_list->valptrp);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_selupd_list (THREAD_ENTRY * thread_p, char *ptr,
			 const SELUPD_LIST * selupd_list)
{
  int offset;

  ptr = or_pack_oid (ptr, (OID *) & selupd_list->class_oid);

  ptr = or_pack_hfid (ptr, &selupd_list->class_hfid);

  ptr = or_pack_int (ptr, selupd_list->select_list_size);

  ptr = or_pack_int (ptr, selupd_list->waitsecs);

  offset = xts_save_regu_varlist_list (thread_p, selupd_list->select_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_selupd_list (thread_p, selupd_list->next);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_pred_expr (THREAD_ENTRY * thread_p, char *ptr,
		       const PRED_EXPR * pred_expr)
{
  int offset;

  ptr = or_pack_int (ptr, pred_expr->type);

  switch (pred_expr->type)
    {
    case T_PRED:
      ptr = xts_process_pred (thread_p, ptr, &pred_expr->pe.pred);
      break;

    case T_EVAL_TERM:
      ptr = xts_process_eval_term (thread_p, ptr, &pred_expr->pe.eval_term);
      break;

    case T_NOT_TERM:
      offset = xts_save_pred_expr (thread_p, pred_expr->pe.not_term);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return NULL;
    }

  return ptr;
}

static char *
xts_process_partition_info (THREAD_ENTRY * thread_p, char *ptr,
			    const XASL_PARTITION_INFO * partition_info)
{
  int offset;

  ptr = or_pack_int (ptr, partition_info->key_attr);

  ptr = or_pack_int (ptr, partition_info->type);

  ptr = or_pack_int (ptr, partition_info->no_parts);

  ptr = or_pack_int (ptr, partition_info->act_parts);

  offset = xts_save_regu_variable (thread_p, partition_info->expr);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_parts_array (thread_p, partition_info->parts,
				 partition_info->no_parts);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_parts_info (THREAD_ENTRY * thread_p, char *ptr,
			const XASL_PARTS_INFO * parts_info)
{
  int offset;

  ptr = or_pack_oid (ptr, (OID *) & parts_info->class_oid);

  ptr = or_pack_oid (ptr, (OID *) & parts_info->proxy_class_oid);

  ptr = or_pack_hfid (ptr, &parts_info->class_hfid);

  ptr = or_pack_int (ptr, parts_info->node_id);

  offset = xts_save_db_value (thread_p, parts_info->vals);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_pred (THREAD_ENTRY * thread_p, char *ptr, const PRED * pred)
{
  int offset;
  PRED_EXPR *rhs;

  offset = xts_save_pred_expr (thread_p, pred->lhs);	/* lhs */
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, pred->bool_op);	/* bool_op */

  rhs = pred->rhs;
  ptr = or_pack_int (ptr, rhs->type);	/* rhs-type */

  /* Traverse right-linear chains of AND/OR terms */
  while (rhs->type == T_PRED)
    {
      pred = &rhs->pe.pred;

      offset = xts_save_pred_expr (thread_p, pred->lhs);	/* lhs */
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);

      ptr = or_pack_int (ptr, pred->bool_op);	/* bool_op */

      rhs = pred->rhs;
      ptr = or_pack_int (ptr, rhs->type);	/* rhs-type */
    }

  offset = xts_save_pred_expr (thread_p, pred->rhs);	/* rhs */
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_eval_term (THREAD_ENTRY * thread_p, char *ptr,
		       const EVAL_TERM * eval_term)
{
  ptr = or_pack_int (ptr, eval_term->et_type);

  switch (eval_term->et_type)
    {
    case T_COMP_EVAL_TERM:
      ptr =
	xts_process_comp_eval_term (thread_p, ptr, &eval_term->et.et_comp);
      break;

    case T_ALSM_EVAL_TERM:
      ptr =
	xts_process_alsm_eval_term (thread_p, ptr, &eval_term->et.et_alsm);
      break;

    case T_LIKE_EVAL_TERM:
      ptr =
	xts_process_like_eval_term (thread_p, ptr, &eval_term->et.et_like);
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return NULL;
    }

  return ptr;
}

static char *
xts_process_comp_eval_term (THREAD_ENTRY * thread_p, char *ptr,
			    const COMP_EVAL_TERM * comp_eval_term)
{
  int offset;

  offset = xts_save_regu_variable (thread_p, comp_eval_term->lhs);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable (thread_p, comp_eval_term->rhs);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, comp_eval_term->rel_op);

  ptr = or_pack_int (ptr, comp_eval_term->type);

  return ptr;
}

static char *
xts_process_alsm_eval_term (THREAD_ENTRY * thread_p, char *ptr,
			    const ALSM_EVAL_TERM * alsm_eval_term)
{
  int offset;

  offset = xts_save_regu_variable (thread_p, alsm_eval_term->elem);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable (thread_p, alsm_eval_term->elemset);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, alsm_eval_term->eq_flag);

  ptr = or_pack_int (ptr, alsm_eval_term->rel_op);

  ptr = or_pack_int (ptr, alsm_eval_term->item_type);

  return ptr;
}

static char *
xts_process_like_eval_term (THREAD_ENTRY * thread_p, char *ptr,
			    const LIKE_EVAL_TERM * like_eval_term)
{
  int offset;

  offset = xts_save_regu_variable (thread_p, like_eval_term->src);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable (thread_p, like_eval_term->pattern);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable (thread_p, like_eval_term->esc_char);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_access_spec_type (THREAD_ENTRY * thread_p, char *ptr,
			      const ACCESS_SPEC_TYPE * access_spec)
{
  int offset;

  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_pack_int (ptr, access_spec->type);

  ptr = or_pack_int (ptr, access_spec->access);

  ptr = or_pack_int (ptr, access_spec->lock_hint);

  if (access_spec->access == SEQUENTIAL)
    {
      ptr = or_pack_int (ptr, 0);
    }
  else
    {
      offset = xts_save_indx_info (thread_p, access_spec->indexptr);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}

      ptr = or_pack_int (ptr, offset);
    }

  offset = xts_save_pred_expr (thread_p, access_spec->where_key);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_pred_expr (thread_p, access_spec->where_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  switch (access_spec->type)
    {
    case TARGET_CLASS:
    case TARGET_CLASS_ATTR:
      ptr = xts_process_cls_spec_type (thread_p, ptr,
				       &ACCESS_SPEC_CLS_SPEC (access_spec));
      break;

    case TARGET_LIST:
      ptr = xts_process_list_spec_type (thread_p, ptr,
					&ACCESS_SPEC_LIST_SPEC (access_spec));
      break;

    case TARGET_SET:
      ptr = xts_process_set_spec_type (thread_p, ptr,
				       &ACCESS_SPEC_SET_SPEC (access_spec));
      break;

    case TARGET_METHOD:
      ptr = xts_process_method_spec_type (thread_p, ptr,
					  &ACCESS_SPEC_METHOD_SPEC
					  (access_spec));
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return NULL;
    }

  if (ptr == NULL)
    {
      return NULL;
    }

  /* ptr->s_id not sent to server */

  ptr = or_pack_int (ptr, access_spec->grouped_scan);

  ptr = or_pack_int (ptr, access_spec->fixed_scan);

  ptr = or_pack_int (ptr, access_spec->qualified_block);

  ptr = or_pack_int (ptr, access_spec->single_fetch);

  offset = xts_save_db_value (thread_p, access_spec->s_dbval);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_indx_info (THREAD_ENTRY * thread_p, char *ptr,
		       const INDX_INFO * indx_info)
{
  ptr = xts_process_indx_id (thread_p, ptr, &indx_info->indx_id);
  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_pack_int (ptr, indx_info->range_type);

  ptr = xts_process_key_info (thread_p, ptr, &indx_info->key_info);

  return ptr;
}

static char *
xts_process_indx_id (THREAD_ENTRY * thread_p, char *ptr,
		     const INDX_ID * indx_id)
{
  ptr = or_pack_int (ptr, indx_id->type);

  switch (indx_id->type)
    {
    case T_BTID:
      ptr = or_pack_btid (ptr, (BTID *) & indx_id->i.btid);
      break;

    case T_EHID:
      ptr = or_pack_ehid (ptr, (EHID *) & indx_id->i.ehid);
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return NULL;
    }

  return ptr;
}

static char *
xts_process_key_info (THREAD_ENTRY * thread_p, char *ptr,
		      const KEY_INFO * key_info)
{
  int offset;

  ptr = or_pack_int (ptr, key_info->key_cnt);

  if (key_info->key_cnt > 0)
    {
      offset = xts_save_key_range_array (thread_p, key_info->key_ranges,
					 key_info->key_cnt);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
    }
  else
    {
      ptr = or_pack_int (ptr, 0);
    }

  ptr = or_pack_int (ptr, key_info->is_constant);

  return ptr;
}

static char *
xts_process_cls_spec_type (THREAD_ENTRY * thread_p, char *ptr,
			   const CLS_SPEC_TYPE * cls_spec)
{
  int offset;

  ptr = or_pack_hfid (ptr, &cls_spec->hfid);

  ptr = or_pack_oid (ptr, (OID *) & cls_spec->cls_oid);
  ptr = or_pack_int (ptr, cls_spec->node_id);

  offset =
    xts_save_regu_variable_list (thread_p, cls_spec->cls_regu_list_key);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p, cls_spec->cls_regu_list_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p, cls_spec->cls_regu_list_rest);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, cls_spec->num_attrs_key);

  offset = xts_save_int_array (thread_p, cls_spec->attrids_key,
			       cls_spec->num_attrs_key);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_cache_attrinfo (thread_p, cls_spec->cache_key);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, cls_spec->num_attrs_pred);

  offset = xts_save_int_array (thread_p, cls_spec->attrids_pred,
			       cls_spec->num_attrs_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_cache_attrinfo (thread_p, cls_spec->cache_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, cls_spec->num_attrs_rest);

  offset = xts_save_int_array (thread_p, cls_spec->attrids_rest,
			       cls_spec->num_attrs_rest);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_cache_attrinfo (thread_p, cls_spec->cache_rest);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_list_spec_type (THREAD_ENTRY * thread_p, char *ptr,
			    const LIST_SPEC_TYPE * list_spec)
{
  int offset;

  offset = xts_save_xasl_node (thread_p, list_spec->xasl_node);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p, list_spec->list_regu_list_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p, list_spec->list_regu_list_rest);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_set_spec_type (THREAD_ENTRY * thread_p, char *ptr,
			   const SET_SPEC_TYPE * set_spec)
{
  int offset;

  offset = xts_save_regu_variable (thread_p, set_spec->set_ptr);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable_list (thread_p, set_spec->set_regu_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_method_spec_type (THREAD_ENTRY * thread_p, char *ptr,
			      const METHOD_SPEC_TYPE * method_spec)
{
  int offset;

  offset = xts_save_xasl_node (thread_p, method_spec->xasl_node);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p, method_spec->method_regu_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_method_sig_list (thread_p, method_spec->method_sig_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_list_id (THREAD_ENTRY * thread_p, char *ptr,
		     const QFILE_LIST_ID * list_id)
{
  return or_pack_listid (ptr, (void *) list_id);
}

static char *
xts_process_val_list (THREAD_ENTRY * thread_p, char *ptr,
		      const VAL_LIST * val_list)
{
  int offset;
  int i;
  QPROC_DB_VALUE_LIST p;

  ptr = or_pack_int (ptr, val_list->val_cnt);

  for (i = 0, p = val_list->valp; i < val_list->val_cnt; i++, p = p->next)
    {
      offset = xts_save_db_value (thread_p, p->val);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}

      ptr = or_pack_int (ptr, offset);
    }

  return ptr;
}

static char *
xts_process_regu_variable (THREAD_ENTRY * thread_p, char *ptr,
			   const REGU_VARIABLE * regu_var)
{
  int offset;

  /* we prepend the domain before we pack the regu_variable */
  ptr = OR_PACK_DOMAIN_OBJECT_TO_OID (ptr, regu_var->domain, 0, 0);

  ptr = or_pack_int (ptr, regu_var->type);

  ptr = or_pack_int (ptr, regu_var->hidden_column);

  offset = xts_save_db_value (thread_p, regu_var->vfetch_to);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_xasl_node (thread_p, REGU_VARIABLE_XASL (regu_var));
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  switch (regu_var->type)
    {
    case TYPE_DBVAL:
      ptr = xts_process_db_value (thread_p, ptr, &regu_var->value.dbval);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      break;

    case TYPE_CONSTANT:
    case TYPE_ORDERBY_NUM:
      offset = xts_save_db_value (thread_p, regu_var->value.dbvalptr);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
      break;

    case TYPE_INARITH:
    case TYPE_OUTARITH:
      offset = xts_save_arith_type (thread_p, regu_var->value.arithptr);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
      break;

    case TYPE_AGGREGATE:
      offset = xts_save_aggregate_type (thread_p, regu_var->value.aggptr);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
      break;

    case TYPE_FUNC:
      offset = xts_save_function_type (thread_p, regu_var->value.funcp);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
      break;

    case TYPE_ATTR_ID:
    case TYPE_SHARED_ATTR_ID:
    case TYPE_CLASS_ATTR_ID:
      ptr =
	xts_process_attr_descr (thread_p, ptr, &regu_var->value.attr_descr);
      if (ptr == NULL)
	{
	  return NULL;
	}
      break;

    case TYPE_LIST_ID:
      offset = xts_save_srlist_id (thread_p, regu_var->value.srlist_id);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
      break;

    case TYPE_POSITION:
      ptr = xts_process_pos_descr (thread_p, ptr, &regu_var->value.pos_descr);
      if (ptr == NULL)
	{
	  return NULL;
	}
      break;

    case TYPE_POS_VALUE:
      ptr = or_pack_int (ptr, regu_var->value.val_pos);
      break;

    case TYPE_OID:
    case TYPE_CLASSOID:
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return NULL;
    }

  return ptr;
}

static char *
xts_process_attr_descr (THREAD_ENTRY * thread_p, char *ptr,
			const ATTR_DESCR * attr_descr)
{
  int offset;

  ptr = or_pack_int (ptr, attr_descr->id);

  ptr = or_pack_int (ptr, attr_descr->type);

  offset = xts_save_cache_attrinfo (thread_p, attr_descr->cache_attrinfo);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_pos_descr (THREAD_ENTRY * thread_p, char *ptr,
		       const QFILE_TUPLE_VALUE_POSITION * position_descr)
{
  ptr = or_pack_int (ptr, position_descr->pos_no);
  ptr = OR_PACK_DOMAIN_OBJECT_TO_OID (ptr, position_descr->dom, 0, 0);

  return ptr;
}

static char *
xts_process_db_value (THREAD_ENTRY * thread_p, char *ptr,
		      const DB_VALUE * value)
{
  ptr = or_pack_db_value (ptr, (DB_VALUE *) value);

  return ptr;
}

static char *
xts_process_arith_type (THREAD_ENTRY * thread_p, char *ptr,
			const ARITH_TYPE * arith)
{
  int offset;

  ptr = OR_PACK_DOMAIN_OBJECT_TO_OID (ptr, arith->domain, 0, 0);

  offset = xts_save_db_value (thread_p, arith->value);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, arith->opcode);

  offset = xts_save_arith_type (thread_p, arith->next);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable (thread_p, arith->leftptr);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable (thread_p, arith->rightptr);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_regu_variable (thread_p, arith->thirdptr);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, arith->misc_operand);

  if (arith->opcode == T_CASE || arith->opcode == T_DECODE
      || arith->opcode == T_IF)
    {
      offset = xts_save_pred_expr (thread_p, arith->pred);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
    }

  return ptr;
}

static char *
xts_process_aggregate_type (THREAD_ENTRY * thread_p, char *ptr,
			    const AGGREGATE_TYPE * aggregate)
{
  int offset;

  ptr = OR_PACK_DOMAIN_OBJECT_TO_OID (ptr, aggregate->domain, 0, 0);

  offset = xts_save_db_value (thread_p, aggregate->value);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_db_value (thread_p, aggregate->value2);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, aggregate->curr_cnt);

  offset = xts_save_aggregate_type (thread_p, aggregate->next);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, aggregate->function);

  ptr = or_pack_int (ptr, aggregate->option);

  ptr = or_pack_int (ptr, (int) aggregate->opr_dbtype);

  ptr = xts_process_regu_variable (thread_p, ptr, &aggregate->operand);
  if (ptr == NULL)
    {
      return NULL;
    }

  if (aggregate->list_id == NULL)
    {
      ptr = or_pack_int (ptr, 0);
    }
  else
    {
      offset = xts_save_list_id (thread_p, aggregate->list_id);
      if (offset == ER_FAILED)
	{
	  return NULL;
	}
      ptr = or_pack_int (ptr, offset);
    }

  ptr = or_pack_int (ptr, aggregate->flag_agg_optimize);

  ptr = or_pack_btid (ptr, (BTID *) (&aggregate->btid));
  if (ptr == NULL)
    {
      return NULL;
    }

  return ptr;
}

static char *
xts_process_function_type (THREAD_ENTRY * thread_p, char *ptr,
			   const FUNCTION_TYPE * function)
{
  int offset;

  offset = xts_save_db_value (thread_p, function->value);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, function->ftype);

  offset = xts_save_regu_variable_list (thread_p, function->operand);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_srlist_id (THREAD_ENTRY * thread_p, char *ptr,
		       const QFILE_SORTED_LIST_ID * sort_list_id)
{
  int offset;

  ptr = or_pack_int (ptr, sort_list_id->sorted);

  offset = xts_save_list_id (thread_p, sort_list_id->list_id);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  return ptr;
}

static char *
xts_process_sort_list (THREAD_ENTRY * thread_p, char *ptr,
		       const SORT_LIST * sort_list)
{
  int offset;

  offset = xts_save_sort_list (thread_p, sort_list->next);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = xts_process_pos_descr (thread_p, ptr, &sort_list->pos_descr);
  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_pack_int (ptr, sort_list->s_order);

  /* others (not sent to server) */

  return ptr;
}

static char *
xts_process_method_sig_list (THREAD_ENTRY * thread_p, char *ptr,
			     const METHOD_SIG_LIST * method_sig_list)
{
  return or_pack_method_sig_list (ptr, (void *) method_sig_list);
}

static char *
xts_process_connectby_proc (THREAD_ENTRY * thread_p, char *ptr,
			    const CONNECTBY_PROC_NODE * connectby_proc)
{
  int offset;

  offset = xts_save_pred_expr (thread_p, connectby_proc->start_with_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_pred_expr (thread_p, connectby_proc->after_connect_by_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_list_id (thread_p, connectby_proc->input_list_id);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_list_id (thread_p, connectby_proc->start_with_list_id);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p, connectby_proc->regu_list_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p, connectby_proc->regu_list_rest);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_val_list (thread_p, connectby_proc->prior_val_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset = xts_save_outptr_list (thread_p, connectby_proc->prior_outptr_list);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p,
				 connectby_proc->prior_regu_list_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p,
				 connectby_proc->prior_regu_list_rest);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p,
				 connectby_proc->after_cb_regu_list_pred);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  offset =
    xts_save_regu_variable_list (thread_p,
				 connectby_proc->after_cb_regu_list_rest);
  if (offset == ER_FAILED)
    {
      return NULL;
    }
  ptr = or_pack_int (ptr, offset);

  ptr = or_pack_int (ptr, (int) connectby_proc->single_table_opt);

  return ptr;
}

/*
 * xts_sizeof_xasl_node () -
 *   return:
 *   xasl(in)    :
 */
static int
xts_sizeof_xasl_node (THREAD_ENTRY * thread_p, const XASL_NODE * xasl)
{
  int size = 0;
  int tmp_size = 0;
  ACCESS_SPEC_TYPE *access_spec = NULL;

  size += OR_INT_SIZE +		/* type */
    OR_INT_SIZE +		/* flag */
    PTR_SIZE +			/* list_id */
    PTR_SIZE +			/* after_iscan_list */
    PTR_SIZE +			/* orderby_list */
    PTR_SIZE +			/* ordbynum_pred */
    PTR_SIZE +			/* ordbynum_val */
    OR_INT_SIZE +		/* ordbynum_flag */
    PTR_SIZE +			/* single_tuple */
    OR_INT_SIZE +		/* is_single_tuple */
    OR_INT_SIZE +		/* option */
    PTR_SIZE +			/* outptr_list */
    PTR_SIZE +			/* selected_upd_list */
    PTR_SIZE +			/* val_list */
    PTR_SIZE +			/* merge_val_list */
    PTR_SIZE +			/* aptr_list */
    PTR_SIZE +			/* bptr_list */
    PTR_SIZE +			/* dptr_list */
    PTR_SIZE +			/* after_join_pred */
    PTR_SIZE +			/* if_pred */
    PTR_SIZE +			/* instnum_pred */
    PTR_SIZE +			/* instnum_val */
    OR_INT_SIZE +		/* instnum_flag */
    PTR_SIZE +			/* fptr_list */
    PTR_SIZE +			/* scan_ptr */
    PTR_SIZE +			/* connect_by_ptr */
    PTR_SIZE +			/* level_val */
    PTR_SIZE +			/* level_regu */
    PTR_SIZE +			/* isleaf_val */
    PTR_SIZE +			/* isleaf_regu */
    PTR_SIZE +			/* iscycle_val */
    PTR_SIZE +			/* iscycle_regu */
    OR_INT_SIZE +		/* next_scan_on */
    OR_INT_SIZE +		/* next_scan_block_on */
    OR_INT_SIZE +		/* cat_fetched */
    OR_INT_SIZE;		/* composite_locking */

  size += OR_INT_SIZE;		/* number of access specs in spec_list */
  for (access_spec = xasl->spec_list; access_spec;
       access_spec = access_spec->next)
    {
      size += xts_sizeof_access_spec_type (thread_p, access_spec);
    }

  size += OR_INT_SIZE;		/* number of access specs in merge_spec */
  for (access_spec = xasl->merge_spec; access_spec;
       access_spec = access_spec->next)
    {
      size += xts_sizeof_access_spec_type (thread_p, access_spec);
    }

  size += OR_INT_SIZE;		/* number of access specs in curr_spec */
  for (access_spec = xasl->curr_spec; access_spec;
       access_spec = access_spec->next)
    {
      size += xts_sizeof_access_spec_type (thread_p, access_spec);
    }

  switch (xasl->type)
    {
    case BUILDLIST_PROC:
      tmp_size = xts_sizeof_buildlist_proc (&xasl->proc.buildlist);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case BUILDVALUE_PROC:
      tmp_size = xts_sizeof_buildvalue_proc (&xasl->proc.buildvalue);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case MERGELIST_PROC:
      tmp_size = xts_sizeof_mergelist_proc (thread_p, &xasl->proc.mergelist);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case CONNECTBY_PROC:
      tmp_size = xts_sizeof_connectby_proc (&xasl->proc.connect_by);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case UPDATE_PROC:
      tmp_size = xts_sizeof_update_proc (&xasl->proc.update);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case DELETE_PROC:
      tmp_size = xts_sizeof_delete_proc (&xasl->proc.delete_);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case INSERT_PROC:
      tmp_size = xts_sizeof_insert_proc (&xasl->proc.insert);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:
      tmp_size = xts_sizeof_union_proc (&xasl->proc.union_);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case OBJFETCH_PROC:
    case SETFETCH_PROC:
      tmp_size = xts_sizeof_fetch_proc (&xasl->proc.fetch);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case SCAN_PROC:
      break;

    case DO_PROC:
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return ER_FAILED;
    }

  size += OR_INT_SIZE;		/* projected_size */

  size += OR_DOUBLE_ALIGNED_SIZE;	/* cardinality */

  size += OR_INT_SIZE;		/* iscan_oid_order */

  size += PTR_SIZE;		/* qstmt */

  size += PTR_SIZE;		/* next */
  return size;
}

/*
 * xts_sizeof_cache_attrinfo () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_cache_attrinfo (const HEAP_CACHE_ATTRINFO * cache_attrinfo)
{
  return OR_INT_SIZE;		/* dummy 0 */
}

/*
 * xts_sizeof_union_proc () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_union_proc (const UNION_PROC_NODE * union_proc)
{
  int size = 0;

  size += PTR_SIZE +		/* left */
    PTR_SIZE;			/* right */

  return size;
}

/*
 * xts_sizeof_fetch_proc () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_fetch_proc (const FETCH_PROC_NODE * obj_set_fetch_proc)
{
  int size = 0;

  size += PTR_SIZE +		/* arg */
    OR_INT_SIZE +		/* fetch_res */
    PTR_SIZE +			/* set_pred */
    OR_INT_SIZE;		/* ql_flag */

  return size;
}

/*
 * xts_sizeof_buildlist_proc () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_buildlist_proc (const BUILDLIST_PROC_NODE * build_list)
{
  int size = 0;

  size +=
    /* output_columns (not sent to serve) */
    PTR_SIZE +			/* eptr_list */
    PTR_SIZE +			/* groupby_list */
    PTR_SIZE +			/* after_groupby_list */
    PTR_SIZE +			/* g_outptr_list */
    PTR_SIZE +			/* g_regu_list */
    PTR_SIZE +			/* g_val_list */
    PTR_SIZE +			/* g_having_pred */
    PTR_SIZE +			/* g_grbynum_pred */
    PTR_SIZE +			/* g_grbynum_val */
    OR_INT_SIZE +		/* g_grbynum_flag */
    OR_INT_SIZE +		/* g_with_rollup */
    PTR_SIZE +			/* g_agg_list */
    PTR_SIZE;			/* g_outarith_list */

  return size;
}

/*
 * xts_sizeof_buildvalue_proc () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_buildvalue_proc (const BUILDVALUE_PROC_NODE * build_value)
{
  int size = 0;

  size += PTR_SIZE +		/* having_pred */
    PTR_SIZE +			/* grbynum_val */
    PTR_SIZE +			/* agg_list */
    PTR_SIZE +			/* outarith_list */
    OR_INT_SIZE;		/* is_always_false */

  return size;
}

/*
 * xts_sizeof_ls_merge_info () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_ls_merge_info (const QFILE_LIST_MERGE_INFO * qfile_list_merge_info)
{
  int size = 0;

  size += OR_INT_SIZE +		/* join_type */
    OR_INT_SIZE +		/* single_fetch */
    OR_INT_SIZE +		/* ls_column_cnt */
    PTR_SIZE +			/* ls_outer_column */
    PTR_SIZE +			/* ls_outer_unique */
    PTR_SIZE +			/* ls_inner_column */
    PTR_SIZE +			/* ls_inner_unique */
    OR_INT_SIZE +		/* ls_pos_cnt */
    PTR_SIZE +			/* ls_outer_inner_list */
    PTR_SIZE;			/* ls_pos_list */

  return size;
}

/*
 * xts_sizeof_mergelist_proc () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_mergelist_proc (THREAD_ENTRY * thread_p,
			   const MERGELIST_PROC_NODE * merge_list_info)
{
  int size = 0;
  ACCESS_SPEC_TYPE *access_spec = NULL;

  size += PTR_SIZE +		/* outer_xasl */
    PTR_SIZE +			/* outer_val_list */
    PTR_SIZE +			/* inner_xasl */
    PTR_SIZE +			/* inner_val_list */
    xts_sizeof_ls_merge_info (&merge_list_info->ls_merge);	/* ls_merge_info */

  size += OR_INT_SIZE;		/* count of access specs in outer_spec_list */
  for (access_spec = merge_list_info->outer_spec_list; access_spec;
       access_spec = access_spec->next)
    {
      size += xts_sizeof_access_spec_type (thread_p, access_spec);
    }

  size += OR_INT_SIZE;		/* count of access specs in inner_spec_list */
  for (access_spec = merge_list_info->inner_spec_list; access_spec;
       access_spec = access_spec->next)
    {
      size += xts_sizeof_access_spec_type (thread_p, access_spec);
    }

  return size;
}

/*
 * xts_sizeof_update_proc () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_update_proc (const UPDATE_PROC_NODE * update_info)
{
  int size = 0;

  size += OR_INT_SIZE +		/* no_classes */
    PTR_SIZE +			/* class_oid */
    PTR_SIZE +			/* class_hfid */
    PTR_SIZE +			/* node_ids */
    OR_INT_SIZE +		/* no_vals */
    OR_INT_SIZE +		/* no_consts */
    2 * PTR_SIZE +		/* 2 array pointers: att_id & consts */
    PTR_SIZE +			/* constraint predicate: cons_pred */
    OR_INT_SIZE +		/* has_uniques */
    OR_INT_SIZE +		/* waitsecs */
    OR_INT_SIZE +		/* no_logging */
    OR_INT_SIZE +		/* release_lock */
    PTR_SIZE;			/* PARTITION INFO array */

  return size;
}

/*
 * xts_sizeof_delete_proc () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_delete_proc (const DELETE_PROC_NODE * delete_info)
{
  int size = 0;

  size += OR_INT_SIZE +		/* no_classes */
    PTR_SIZE +			/* class_oid */
    PTR_SIZE +			/* class_hfid */
    PTR_SIZE +			/* nodes_ids */
    OR_INT_SIZE +		/* waitsecs */
    OR_INT_SIZE +		/* no_logging */
    OR_INT_SIZE;		/* release_lock */

  return size;
}

/*
 * xts_sizeof_insert_proc () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_insert_proc (const INSERT_PROC_NODE * insert_info)
{
  int size = 0;

  size += OR_OID_SIZE +		/* class_oid */
    OR_OID_SIZE +		/* proxy_oid */
    OR_HFID_SIZE +		/* class_hfid */
    OR_INT_SIZE +		/* node_id */
    OR_INT_SIZE +		/* no_vals */
    PTR_SIZE +			/* array pointer: att_id */
    PTR_SIZE +			/* constraint predicate: cons_pred */
    OR_INT_SIZE +		/* has_uniques */
    OR_INT_SIZE +		/* waitsecs */
    OR_INT_SIZE +		/* no_logging */
    OR_INT_SIZE +		/* release_lock */
    OR_INT_SIZE +		/* do_replace */
    OR_INT_SIZE +		/* dup_key_oid_var_index */
    PTR_SIZE;			/* partition_info */

  return size;
}

/*
 * xts_sizeof_outptr_list () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_outptr_list (const OUTPTR_LIST * outptr_list)
{
  int size = 0;

  size += OR_INT_SIZE +		/* valptr_cnt */
    PTR_SIZE;			/* valptrp */

  return size;
}

/*
 * xts_sizeof_pred_expr () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_pred_expr (THREAD_ENTRY * thread_p, const PRED_EXPR * pred_expr)
{
  int size = 0;
  int tmp_size = 0;

  size += OR_INT_SIZE;		/* type_node */
  switch (pred_expr->type)
    {
    case T_PRED:
      tmp_size = xts_sizeof_pred (thread_p, &pred_expr->pe.pred);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case T_EVAL_TERM:
      tmp_size = xts_sizeof_eval_term (thread_p, &pred_expr->pe.eval_term);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case T_NOT_TERM:
      size += PTR_SIZE;
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return ER_FAILED;
    }

  return size;
}

/*
 * xts_sizeof_partition_info () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_partition_info (const XASL_PARTITION_INFO * partition_info)
{
  int size = 0;

  size = OR_INT_SIZE +		/* key_attr */
    OR_INT_SIZE +		/* type */
    OR_INT_SIZE +		/* no_parts */
    OR_INT_SIZE +		/* act_parts */
    PTR_SIZE +			/* expr */
    PTR_SIZE;			/* parts */

  return size;
}

/*
 * xts_sizeof_parts_info () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_parts_info (const XASL_PARTS_INFO * parts_info)
{
  int size = 0;

  size = OR_OID_SIZE +		/* cls_oid */
    OR_OID_SIZE +		/* proxy_cls_oid */
    OR_HFID_SIZE +		/* hfid */
    OR_INT_SIZE +		/* node_id */
    PTR_SIZE;			/* vals */

  return size;
}

/*
 * xts_sizeof_selupd_list () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_selupd_list (const SELUPD_LIST * selupd_list)
{
  int size = 0;

  size = OR_OID_SIZE +		/* class_oid */
    OR_HFID_SIZE +		/* class_hfid */
    OR_INT_SIZE +		/* select_list_size */
    PTR_SIZE +			/* select_list */
    OR_INT_SIZE +		/* waitsecs */
    PTR_SIZE;			/* next */

  return size;
}

/*
 * xts_sizeof_pred () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_pred (THREAD_ENTRY * thread_p, const PRED * pred)
{
  int size = 0;
  int tmp_size = 0;
  PRED_EXPR *rhs;

  size += PTR_SIZE +		/* lhs */
    OR_INT_SIZE;		/* bool_op */

  rhs = pred->rhs;
  size += OR_INT_SIZE;		/* rhs-type */

  /* Traverse right-linear chains of AND/OR terms */
  while (rhs->type == T_PRED)
    {
      pred = &rhs->pe.pred;

      size += PTR_SIZE +	/* lhs */
	OR_INT_SIZE;		/* bool_op */

      rhs = pred->rhs;
      size += OR_INT_SIZE;	/* rhs-type */
    }

  switch (rhs->type)
    {
    case T_EVAL_TERM:
      tmp_size = xts_sizeof_eval_term (thread_p, &rhs->pe.eval_term);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;		/* rhs */
      break;

    case T_NOT_TERM:
      size += PTR_SIZE;		/* pe.not_term */
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return ER_FAILED;
    }

  return size;
}

/*
 * xts_sizeof_eval_term () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_eval_term (THREAD_ENTRY * thread_p, const EVAL_TERM * eval_term)
{
  int size = 0;
  int tmp_size = 0;

  size += OR_INT_SIZE;		/* et_type */
  switch (eval_term->et_type)
    {
    case T_COMP_EVAL_TERM:
      tmp_size = xts_sizeof_comp_eval_term (&eval_term->et.et_comp);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case T_ALSM_EVAL_TERM:
      tmp_size = xts_sizeof_alsm_eval_term (&eval_term->et.et_alsm);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case T_LIKE_EVAL_TERM:
      tmp_size = xts_sizeof_like_eval_term (&eval_term->et.et_like);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return ER_FAILED;
    }

  return size;
}

/*
 * xts_sizeof_comp_eval_term () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_comp_eval_term (const COMP_EVAL_TERM * comp_eval_term)
{
  int size = 0;

  size += PTR_SIZE +		/* lhs */
    PTR_SIZE +			/* rhs */
    OR_INT_SIZE +		/* rel_op */
    OR_INT_SIZE;		/* type */

  return size;
}

/*
 * xts_sizeof_alsm_eval_term () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_alsm_eval_term (const ALSM_EVAL_TERM * alsm_eval_term)
{
  int size = 0;

  size += PTR_SIZE +		/* elem */
    PTR_SIZE +			/* elemset */
    OR_INT_SIZE +		/* eq_flag */
    OR_INT_SIZE +		/* rel_op */
    OR_INT_SIZE;		/* item_type */

  return size;
}

/*
 * xts_sizeof_like_eval_term () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_like_eval_term (const LIKE_EVAL_TERM * like_eval_term)
{
  int size = 0;

  size += PTR_SIZE +		/* src */
    PTR_SIZE +			/* pattern */
    PTR_SIZE;			/* esc_char */

  return size;
}

/*
 * xts_sizeof_access_spec_type () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_access_spec_type (THREAD_ENTRY * thread_p,
			     const ACCESS_SPEC_TYPE * access_spec)
{
  int size = 0;
  int tmp_size = 0;

  size += OR_INT_SIZE +		/* type */
    OR_INT_SIZE +		/* access */
    OR_INT_SIZE +		/* lock_hint */
    PTR_SIZE +			/* index_ptr */
    PTR_SIZE +			/* where_key */
    PTR_SIZE;			/* where_pred */

  switch (access_spec->type)
    {
    case TARGET_CLASS:
    case TARGET_CLASS_ATTR:
      tmp_size =
	xts_sizeof_cls_spec_type (&ACCESS_SPEC_CLS_SPEC (access_spec));
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case TARGET_LIST:
      tmp_size =
	xts_sizeof_list_spec_type (&ACCESS_SPEC_LIST_SPEC (access_spec));
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case TARGET_SET:
      tmp_size =
	xts_sizeof_set_spec_type (&ACCESS_SPEC_SET_SPEC (access_spec));
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case TARGET_METHOD:
      tmp_size =
	xts_sizeof_method_spec_type (&ACCESS_SPEC_METHOD_SPEC (access_spec));
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return ER_FAILED;
    }

  size +=			/* s_id (not sent to server) */
    OR_INT_SIZE +		/* grouped_scan */
    OR_INT_SIZE +		/* fixed_scan */
    OR_INT_SIZE +		/* qualified_scan */
    OR_INT_SIZE +		/* single_fetch */
    PTR_SIZE;			/* s_dbval */

  return size;
}

/*
 * xts_sizeof_indx_info () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_indx_info (THREAD_ENTRY * thread_p, const INDX_INFO * indx_info)
{
  int size = 0;
  int tmp_size = 0;

  tmp_size = xts_sizeof_indx_id (thread_p, &indx_info->indx_id);
  if (tmp_size == ER_FAILED)
    {
      return ER_FAILED;
    }
  size += tmp_size;

  size += OR_INT_SIZE;		/* range_type */

  tmp_size = xts_sizeof_key_info (&indx_info->key_info);
  if (tmp_size == ER_FAILED)
    {
      return ER_FAILED;
    }
  size += tmp_size;

  return size;
}

/*
 * xts_sizeof_indx_id () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_indx_id (THREAD_ENTRY * thread_p, const INDX_ID * indx_id)
{
  int size = 0;

  size += OR_INT_SIZE;		/* type */
  switch (indx_id->type)
    {
    case T_BTID:
      size += OR_BTID_ALIGNED_SIZE;
      break;

    case T_EHID:
      size += OR_EHID_SIZE;
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return ER_FAILED;
    }

  return size;
}

/*
 * xts_sizeof_key_info () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_key_info (const KEY_INFO * key_info)
{
  int size = 0;

  size += OR_INT_SIZE +		/* key_cnt */
    PTR_SIZE +			/* key_ranges */
    OR_INT_SIZE;		/* is_constant */

  return size;
}

/*
 * xts_sizeof_cls_spec_type () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_cls_spec_type (const CLS_SPEC_TYPE * cls_spec)
{
  int size = 0;

  size += PTR_SIZE +		/* cls_regu_list_key */
    PTR_SIZE +			/* cls_regu_list_pred */
    PTR_SIZE +			/* cls_regu_list_rest */
    OR_HFID_SIZE +		/* hfid */
    OR_OID_SIZE +		/* cls_oid */
    OR_INT_SIZE +		/* node_id */
    OR_INT_SIZE +		/* num_attrs_key */
    PTR_SIZE +			/* attrids_key */
    PTR_SIZE +			/* cache_key */
    OR_INT_SIZE +		/* num_attrs_pred */
    PTR_SIZE +			/* attrids_pred */
    PTR_SIZE +			/* cache_pred */
    OR_INT_SIZE +		/* num_attrs_rest */
    PTR_SIZE +			/* attrids_rest */
    PTR_SIZE;			/* cache_rest */

  return size;
}

/*
 * xts_sizeof_list_spec_type () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_list_spec_type (const LIST_SPEC_TYPE * list_spec)
{
  int size = 0;

  size += PTR_SIZE +		/* list_regu_list_pred */
    PTR_SIZE +			/* list_regu_list_rest */
    PTR_SIZE;			/* xasl_node */

  return size;
}

/*
 * xts_sizeof_set_spec_type () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_set_spec_type (const SET_SPEC_TYPE * set_spec)
{
  int size = 0;

  size += PTR_SIZE +		/* set_regu_list */
    PTR_SIZE;			/* set_ptr */

  return size;
}

/*
 * xts_sizeof_method_spec_type () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_method_spec_type (const METHOD_SPEC_TYPE * method_spec)
{
  int size = 0;

  size += PTR_SIZE +		/* method_regu_list */
    PTR_SIZE +			/* xasl_node */
    PTR_SIZE;			/* method_sig_list */

  return size;
}

/*
 * xts_sizeof_list_id () -
 *   return:xts_process_db_value
 *   ptr(in)    :
 */
static int
xts_sizeof_list_id (const QFILE_LIST_ID * list_id)
{
  int size = 0;

  size = or_listid_length ((void *) list_id);

  return size;
}

/*
 * xts_sizeof_val_list () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_val_list (const VAL_LIST * val_list)
{
  int size = 0;
  QPROC_DB_VALUE_LIST p = NULL;

  size += OR_INT_SIZE;		/* val_cnt */

  for (p = val_list->valp; p; p = p->next)
    {
      size += PTR_SIZE;		/* p->val */
    }

  return size;
}

/*
 * xts_sizeof_regu_variable () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_regu_variable (THREAD_ENTRY * thread_p,
			  const REGU_VARIABLE * regu_var)
{
  int size = 0;
  int tmp_size = 0;

  /* we prepend the domain before we pack the regu_variable */
  size += or_packed_domain_size (regu_var->domain, 0);
  size += OR_INT_SIZE;		/* type */
  size += OR_INT_SIZE;		/* hidden_column */
  size += PTR_SIZE;		/* vfetch_to */
  size += PTR_SIZE;		/* REGU_VARIABLE_XASL */

  switch (regu_var->type)
    {
    case TYPE_DBVAL:
      tmp_size =
	OR_VALUE_ALIGNED_SIZE ((DB_VALUE *) (&regu_var->value.dbval));
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case TYPE_CONSTANT:
    case TYPE_ORDERBY_NUM:
      size += PTR_SIZE;		/* dbvalptr */
      break;

    case TYPE_INARITH:
    case TYPE_OUTARITH:
      size += PTR_SIZE;		/* arithptr */
      break;

    case TYPE_AGGREGATE:
      size += PTR_SIZE;		/* aggptr */
      break;

    case TYPE_FUNC:
      size += PTR_SIZE;		/* funcp */
      break;

    case TYPE_ATTR_ID:
    case TYPE_SHARED_ATTR_ID:
    case TYPE_CLASS_ATTR_ID:
      tmp_size = xts_sizeof_attr_descr (&regu_var->value.attr_descr);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case TYPE_LIST_ID:
      size += PTR_SIZE;		/* srlist_id */
      break;

    case TYPE_POSITION:
      tmp_size = xts_sizeof_pos_descr (&regu_var->value.pos_descr);
      if (tmp_size == ER_FAILED)
	{
	  return ER_FAILED;
	}
      size += tmp_size;
      break;

    case TYPE_POS_VALUE:
      size += OR_INT_SIZE;	/* val_pos */
      break;

    case TYPE_OID:
    case TYPE_CLASSOID:
      break;

    default:
      xts_Xasl_errcode (thread_p) = ER_QPROC_INVALID_XASLNODE;
      return ER_FAILED;
    }

  return size;
}

/*
 * xts_sizeof_attr_descr () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_attr_descr (const ATTR_DESCR * attr_descr)
{
  int size = 0;

  size += OR_INT_SIZE +		/* id */
    OR_INT_SIZE +		/* type */
    PTR_SIZE;			/* cache_attrinfo */

  return size;
}

/*
 * xts_sizeof_pos_descr () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_pos_descr (const QFILE_TUPLE_VALUE_POSITION * position_descr)
{
  int size = 0;

  size += OR_INT_SIZE +		/* pos_no */
    or_packed_domain_size (position_descr->dom, 0);	/* type */

  return size;
}

/*
 * xts_sizeof_db_value () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_db_value (const DB_VALUE * value)
{
  return or_db_value_size ((DB_VALUE *) value);
}

/*
 * xts_sizeof_arith_type () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_arith_type (const ARITH_TYPE * arith)
{
  int size = 0;

  size += PTR_SIZE +		/* next */
    PTR_SIZE +			/* value */
    OR_INT_SIZE +		/* operator */
    PTR_SIZE +			/* leftptr */
    PTR_SIZE +			/* rightptr */
    PTR_SIZE +			/* thirdptr */
    OR_INT_SIZE +		/* misc_operand */
    ((arith->opcode == T_CASE || arith->opcode == T_DECODE
      || arith->opcode == T_IF) ? PTR_SIZE : 0 /* case pred */ ) +
    or_packed_domain_size (arith->domain, 0);

  return size;
}

/*
 * xts_sizeof_aggregate_type () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_aggregate_type (THREAD_ENTRY * thread_p,
			   const AGGREGATE_TYPE * aggregate)
{
  int size = 0;
  int tmp_size = 0;

  size += or_packed_domain_size (aggregate->domain, 0) + PTR_SIZE +	/* next */
    PTR_SIZE +			/* value */
    PTR_SIZE +			/* value2 */
    OR_INT_SIZE +		/* curr_cnt */
    OR_INT_SIZE +		/* function */
    OR_INT_SIZE +		/* option */
    OR_INT_SIZE;		/* opr_dbtype */

  tmp_size = xts_sizeof_regu_variable (thread_p, &aggregate->operand);
  if (tmp_size == ER_FAILED)
    {
      return ER_FAILED;
    }
  size += tmp_size;

  size += PTR_SIZE		/* list_id */
    + OR_INT_SIZE + OR_BTID_ALIGNED_SIZE;

  return size;
}

/*
 * xts_sizeof_function_type () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_function_type (const FUNCTION_TYPE * function)
{
  int size = 0;

  size += PTR_SIZE +		/* value */
    OR_INT_SIZE +		/* ftype */
    PTR_SIZE;			/* operand */

  return size;
}

/*
 * xts_sizeof_srlist_id () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_srlist_id (const QFILE_SORTED_LIST_ID * sort_list_id)
{
  int size = 0;

  size += OR_INT_SIZE +		/* sorted */
    PTR_SIZE;			/* list_id */

  return size;
}

/*
 * xts_sizeof_sort_list () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_sort_list (const SORT_LIST * sort_lis)
{
  int size = 0;
  int tmp_size = 0;

  size += PTR_SIZE;		/* next */

  tmp_size = xts_sizeof_pos_descr (&sort_lis->pos_descr);
  if (tmp_size == ER_FAILED)
    {
      return ER_FAILED;
    }
  size += tmp_size;

  size +=
    /* other (not sent to server) */
    OR_INT_SIZE;		/* s_order */

  return size;
}

/*
 * xts_sizeof_method_sig_list () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_method_sig_list (const METHOD_SIG_LIST * method_sig_lis)
{
  return or_method_sig_list_length ((void *) method_sig_lis);
}

/*
 * xts_sizeof_connectby_proc () -
 *   return:
 *   ptr(in)    :
 */
static int
xts_sizeof_connectby_proc (const CONNECTBY_PROC_NODE * connectby)
{
  int size = 0;

  size += PTR_SIZE +		/* start_with_pred */
    PTR_SIZE +			/* after_connect_by_pred */
    PTR_SIZE +			/* input_list_id */
    PTR_SIZE +			/* start_with_list_id */
    PTR_SIZE +			/* regu_list_pred */
    PTR_SIZE +			/* regu_list_rest */
    PTR_SIZE +			/* prior_val_list */
    PTR_SIZE +			/* prior_outptr_list */
    PTR_SIZE +			/* prior_regu_list_pred */
    PTR_SIZE +			/* prior_regu_list_rest */
    PTR_SIZE +			/* after_cb_regu_list_pred */
    PTR_SIZE +			/* after_cb_regu_list_rest */
    OR_INT_SIZE;		/* single_table_opt */

  return size;
}

/*
 * xts_mark_ptr_visited () -
 *   return: if successful, return NO_ERROR, otherwise
 *           ER_FAILED and error code is set to xasl_errcode
 *   ptr(in)    : pointer constant to be marked visited
 *   offset(in) : where the node pointed by 'ptr' is stored
 *
 * Note: mark the given pointer constant as visited to avoid
 * duplicated stored of a node which is pointed by more	than one node
 */
static int
xts_mark_ptr_visited (THREAD_ENTRY * thread_p, const void *ptr, int offset)
{
  int new_lwm;
  int block_no;

  block_no = PTR_BLOCK (ptr);

  new_lwm = xts_Ptr_lwm (thread_p)[block_no];

  if (xts_Ptr_max (thread_p)[block_no] == 0)
    {
      xts_Ptr_max (thread_p)[block_no] = START_PTR_PER_BLOCK;
      xts_Ptr_blocks (thread_p)[block_no] = (VISITED_PTR *)
	db_private_alloc (thread_p,
			  sizeof (VISITED_PTR) *
			  xts_Ptr_max (thread_p)[block_no]);
    }
  else if (xts_Ptr_max (thread_p)[block_no] <= new_lwm)
    {
      xts_Ptr_max (thread_p)[block_no] *= 2;
      xts_Ptr_blocks (thread_p)[block_no] = (VISITED_PTR *)
	db_private_realloc (thread_p, xts_Ptr_blocks (thread_p)[block_no],
			    sizeof (VISITED_PTR) *
			    xts_Ptr_max (thread_p)[block_no]);
    }

  if (xts_Ptr_blocks (thread_p)[block_no] == (VISITED_PTR *) NULL)
    {
      xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
      return ER_FAILED;
    }

  xts_Ptr_blocks (thread_p)[block_no][new_lwm].ptr = ptr;
  xts_Ptr_blocks (thread_p)[block_no][new_lwm].offset = offset;

  xts_Ptr_lwm (thread_p)[block_no]++;

  return NO_ERROR;
}

/*
 * xts_get_offset_visited_ptr () -
 *   return: if the ptr is already visited, the offset of
 *           position where the node pointed by 'ptr' is stored,
 *           otherwise, ER_FAILED (xasl_errcode is NOT set)
 *   ptr(in)    : pointer constant to be checked if visited or not
 *
 * Note: check if the node pointed by `ptr` is already stored or
 * not to avoid multiple store of the same node
 */
static int
xts_get_offset_visited_ptr (THREAD_ENTRY * thread_p, const void *ptr)
{
  int block_no;
  int element_no;

  block_no = PTR_BLOCK (ptr);

  if (xts_Ptr_lwm (thread_p)[block_no] <= 0)
    {
      return ER_FAILED;
    }

  for (element_no = 0; element_no < xts_Ptr_lwm (thread_p)[block_no];
       element_no++)
    {
      if (ptr == xts_Ptr_blocks (thread_p)[block_no][element_no].ptr)
	{
	  return xts_Ptr_blocks (thread_p)[block_no][element_no].offset;
	}
    }

  return ER_FAILED;
}

/*
 * xts_free_visited_ptrs () -
 *   return:
 *
 * Note: free memory allocated to manage visited ptr constants
 */
static void
xts_free_visited_ptrs (THREAD_ENTRY * thread_p)
{
  int i;

  for (i = 0; i < MAX_PTR_BLOCKS; i++)
    {
      xts_Ptr_lwm (thread_p)[i] = 0;
    }
}

/*
 * xts_reserve_location_in_stream () -
 *   return: if successful, return the offset of position
 *           where the given item is to be stored, otherwise ER_FAILED
 *           and error code is set to xasl_errcode
 *   size(in)   : # of bytes of the node
 *
 * Note: reserve size bytes in the stream
 */
static int
xts_reserve_location_in_stream (THREAD_ENTRY * thread_p, int size)
{
  int needed;
  int grow;

  size = MAKE_ALIGN (size);
  needed =
    size - (xts_Stream_size (thread_p) -
	    xts_Free_offset_in_stream (thread_p));

  if (needed >= 0)
    {
      /* expansion is needed */
      grow = needed;

      if (grow < (int) STREAM_EXPANSION_UNIT)
	{
	  grow = STREAM_EXPANSION_UNIT;
	}
      if (grow < (xts_Stream_size (thread_p) / 2))
	{
	  grow = xts_Stream_size (thread_p) / 2;
	}

      xts_Stream_size (thread_p) += grow;

      if (xts_Stream_buffer (thread_p) == NULL)
	{
	  xts_Stream_buffer (thread_p) =
	    (char *) db_private_alloc (thread_p, xts_Stream_size (thread_p));
	}
      else
	{
	  xts_Stream_buffer (thread_p) =
	    (char *) db_private_realloc (thread_p,
					 xts_Stream_buffer (thread_p),
					 xts_Stream_size (thread_p));
	}

      if (xts_Stream_buffer (thread_p) == NULL)
	{
	  xts_Xasl_errcode (thread_p) = ER_OUT_OF_VIRTUAL_MEMORY;
	  return ER_FAILED;
	}
    }

  xts_Free_offset_in_stream (thread_p) += size;
  assert ((xts_Free_offset_in_stream (thread_p) - size) % MAX_ALIGNMENT == 0);
  return (xts_Free_offset_in_stream (thread_p) - size);
}
