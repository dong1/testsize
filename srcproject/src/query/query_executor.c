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
 * query_executor.c - Query evaluator module
 */

#ident "$Id$"


#include "config.h"

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <search.h>
#include <sys/timeb.h>

#include "porting.h"
#include "error_manager.h"
#include "memory_alloc.h"
#include "oid.h"
#include "slotted_page.h"
#include "heap_file.h"
#include "page_buffer.h"
#include "extendible_hash.h"
#include "locator_sr.h"
#include "btree.h"
#include "replication.h"
#include "xserver_interface.h"

#if defined(SERVER_MODE)
#include "connection_error.h"
#include "thread_impl.h"
#endif /* SERVER_MODE */

#include "query_manager.h"
#include "fetch.h"
#include "transaction_sr.h"
#include "system_parameter.h"
#include "memory_hash.h"
#include "parser.h"
#include "set_object.h"
#if defined (SERVER_MODE)
#include "remote_query.h"
#endif
#include "cluster_config.h"

#if defined(CUBRID_DEBUG)
#include "environment_variable.h"
#endif /* CUBRID_DEBUG */

#if defined(SERVER_MODE) && defined(DIAG_DEVEL)
#include "perf_monitor.h"
#endif

#define GOTO_EXIT_ON_ERROR \
  do \
    { \
      qexec_failure_line (__LINE__, xasl_state); \
      goto exit_on_error; \
    } \
  while (0)

/* Page buffer int keep free ratios: for the cases of
 * client/server - minimal page buffer pool
 * client/server - large page buffer pool
 * standalone    - minimal page buffer pool
 * standalone    - large page buffer pool
 * respectively,
 */

/* Keep the scans grouped only for that many classes scans for join queries.*/
#define QPROC_MAX_GROUPED_SCAN_CNT       (4)

/* if 1, single class query scans are done in a grouped manner. */
#define QPROC_SINGLE_CLASS_GROUPED_SCAN  (0)

/* if 1, multi class query scans are done in a grouped manner. */
#define QPROC_MULTI_CLASS_GROUPED_SCAN   (1)

/* used for tuple string id */
#define CONNECTBY_TUPLE_INDEX_STRING_MEM  64


#define QEXEC_CLEAR_AGG_LIST_VALUE(agg_list) \
  do \
    { \
      AGGREGATE_TYPE *agg_ptr; \
      for (agg_ptr = (agg_list); agg_ptr; agg_ptr = agg_ptr->next) \
	{ \
	  if (agg_ptr->function == PT_GROUPBY_NUM) \
	    continue; \
	  pr_clear_value (agg_ptr->value); \
	} \
    } \
  while (0)

#define QEXEC_EMPTY_ACCESS_SPEC_SCAN(specp) \
  ((specp)->type == TARGET_CLASS \
    && ((ACCESS_SPEC_HFID((specp)).vfid.fileid == NULL_FILEID \
         || ACCESS_SPEC_HFID((specp)).vfid.volid == NULL_VOLID)))

#define QEXEC_EMPTY_SPEC_INDEX(specp) \
  ((((specp)->type == TARGET_CLASS) || (specp)->type == TARGET_CLASS_ATTR) \
  && ((specp)->indexptr) && ((specp)->indexptr->indx_id.type == T_BTID) \
                            && ((specp)->indexptr->indx_id.i.btid.root_pageid == NULL_PAGEID))

#define XASL_SPEC_INIT_FLAG(tmp_spec)\
  do\
  {\
    while ((tmp_spec))\
    {\
      (tmp_spec)->flag = 0;\
      (tmp_spec) = (tmp_spec)->next;\
    }\
  }while(0)

/* Note: the following macro is used just for replacement of a repetitive
 * text in order to improve the readability.
 */

#if !defined(SERVER_MODE)
#undef MUTEX_LOCK
#define MUTEX_LOCK(a, b)
#undef MUTEX_UNLOCK
#define MUTEX_UNLOCK(a)
#endif

#define QEXEC_INITIALIZE_XASL_CACHE_CLO(c, e) \
  do \
    { \
      (c)->next = NULL; \
      (c)->LRU_prev = (c)->LRU_next = NULL; \
      (c)->ent_ptr = (e);	/* save entry pointer */ \
      (c)->xasl = NULL; \
      (c)->xasl_buf_info = NULL; \
    } \
  while (0)

/* XASL scan block function */
typedef SCAN_CODE (*XSAL_SCAN_FUNC) (THREAD_ENTRY * thread_p, XASL_NODE *,
				     XASL_STATE *, QFILE_TUPLE_RECORD *,
				     void *);

/* pointer to XASL scan function */
typedef XSAL_SCAN_FUNC *XASL_SCAN_FNC_PTR;

typedef struct groupby_state GROUPBY_STATE;
struct groupby_state
{
  int state;

  SORTKEY_INFO key_info;
  QFILE_LIST_SCAN_ID *input_scan;
#if 0				/* SortCache */
  VPID fixed_vpid;		/* current fixed page info of  */
  PAGE_PTR fixed_page;		/* input list file             */
#endif
  QFILE_LIST_ID *output_file;

  PRED_EXPR *having_pred;
  PRED_EXPR *grbynum_pred;
  DB_VALUE *grbynum_val;
  int grbynum_flag;
  XASL_NODE *eptr_list;
  AGGREGATE_TYPE *g_agg_list;
  AGGREGATE_TYPE **g_rollup_agg_list;
  REGU_VARIABLE_LIST g_regu_list;
  VAL_LIST *g_val_list;
  OUTPTR_LIST *g_outptr_list;
  XASL_STATE *xasl_state;

  RECDES current_key;
  RECDES gby_rec;
  QFILE_TUPLE_RECORD input_tpl;
  QFILE_TUPLE_RECORD *output_tplrec;
  int input_recs;
  int rollup_levels;

  SORT_CMP_FUNC *cmp_fn;
};

/*
 * Information required for processing the ORDBY_NUM() function. See
 * qexec_eval_ordbynum_pred ().
 */
typedef struct ordbynum_info ORDBYNUM_INFO;
struct ordbynum_info
{
  XASL_STATE *xasl_state;
  PRED_EXPR *ordbynum_pred;
  DB_VALUE *ordbynum_val;
  int ordbynum_flag;
  int ordbynum_pos_cnt;
  int *ordbynum_pos;
  int reserved[2];
};

/* parent pos info stack */
typedef struct parent_pos_info PARENT_POS_INFO;
struct parent_pos_info
{
  QFILE_TUPLE_POSITION tpl_pos;
  PARENT_POS_INFO *stack;
};

/* XASL cache related things */

/* counters */
typedef struct xasl_cache_counter XASL_CACHE_COUNTER;
struct xasl_cache_counter
{
  unsigned int lookup;		/* counter of cache lookup */
  unsigned int hit;		/* counter of cache hit */
  unsigned int miss;		/* counter of cache miss */
  unsigned int full;		/* counter of cache full */
};

/* candidate/victim info */
typedef struct xasl_cache_ent_cv_info XASL_CACHE_ENT_CV_INFO;
struct xasl_cache_ent_cv_info
{
  bool include_in_use;
  /* candidate */
  float c_ratio;		/* candidate ratio such as 5% */
  int c_num;			/* number of candidates */
  int c_idx;			/* index of candidates */
  int c_selcnt;			/* candidates select counter */
  XASL_CACHE_ENTRY **c_time;	/* candidates who are old aged */
  XASL_CACHE_ENTRY **c_ref;	/* candidates who are recently used */
  /* victim */
  float v_ratio;		/* victim ratio such as 2% */
  int v_num;			/* number of victims */
  int v_idx;			/* index of victims */
  XASL_CACHE_ENTRY **victim;	/* victims to be deleted */
};

/* cache entries info */
typedef struct xasl_cache_ent_info XASL_CACHE_ENT_INFO;
struct xasl_cache_ent_info
{
  int max_entries;		/* max number of cache entries */
  int num;			/* number of cache entries in use */
  XASL_CACHE_COUNTER counter;	/* counter of cache entry */
  MHT_TABLE *qstr_ht;		/* memory hash table for XASL stream cache
				   referencing by query string */
  MHT_TABLE *xid_ht;		/* memory hash table for XASL stream cache
				   referencing by xasl file id (XASL_ID) */
  MHT_TABLE *oid_ht;		/* memory hash table for XASL stream cache
				   referencing by class oid */
  XASL_CACHE_ENT_CV_INFO cv_info;	/* candidate/victim info */
};

/* cache clones info */
typedef struct xasl_cache_clo_info XASL_CACHE_CLO_INFO;
struct xasl_cache_clo_info
{
  int max_clones;		/* max number of cache clones */
  int num;			/* number of cache clones in use */
  XASL_CACHE_COUNTER counter;	/* counter of cache clone */
  XASL_CACHE_CLONE *head;	/* LRU head of cache clones in use */
  XASL_CACHE_CLONE *tail;	/* LRU tail of cache clones in use */
  XASL_CACHE_CLONE *free_list;	/* cache clones in free */
  int n_alloc;			/* number of alloc_arr */
  XASL_CACHE_CLONE **alloc_arr;	/* alloced cache clones */
};

/* XASL cache entry pooling */
#define FIXED_SIZE_OF_POOLED_XASL_CACHE_ENTRY   4096
#define ADDITION_FOR_POOLED_XASL_CACHE_ENTRY    offsetof(POOLED_XASL_CACHE_ENTRY, s.entry)	/* s.next field */
#define POOLED_XASL_CACHE_ENTRY_FROM_XASL_CACHE_ENTRY(p) \
        ((POOLED_XASL_CACHE_ENTRY *) ((char*) p - ADDITION_FOR_POOLED_XASL_CACHE_ENTRY))

typedef union pooled_xasl_cache_entry POOLED_XASL_CACHE_ENTRY;
union pooled_xasl_cache_entry
{
  struct
  {
    int next;			/* next entry in the free list */
    XASL_CACHE_ENTRY entry;	/* XASL cache entry data */
  } s;
  char dummy[FIXED_SIZE_OF_POOLED_XASL_CACHE_ENTRY];
  /* 4K size including XASL cache entry itself
   *   and reserved spaces for
   *   xasl_cache_ent.query_string,
   *   xasl_cache_ent.class_oid_list, and
   *   xasl_cache_ent.repr_id_list */
};

typedef struct xasl_cache_entry_pool XASL_CACHE_ENTRY_POOL;
struct xasl_cache_entry_pool
{
  POOLED_XASL_CACHE_ENTRY *pool;	/* array of POOLED_XASL_CACHE_ENTRY */
  int n_entries;		/* number of entries in the pool */
  int free_list;		/* the head(first entry) of the free list */
};

static const int RESERVED_SIZE_FOR_XASL_CACHE_ENTRY =
  (FIXED_SIZE_OF_POOLED_XASL_CACHE_ENTRY -
   ADDITION_FOR_POOLED_XASL_CACHE_ENTRY);

/* XASL cache related things */

/* XASL entry cache and related information */
static XASL_CACHE_ENT_INFO xasl_ent_cache = {
  0,				/*max_entries */
  0,				/*num */
  {0,				/*lookup */
   0,				/*hit */
   0,				/*miss */
   0 /*full */ },		/*counter */
  NULL,				/*qstr_ht */
  NULL,				/*xid_ht */
  NULL,				/*oid_ht */
/* information of cacndidates to be removed from XASL cache */
  {false,			/*include_in_use */
   0.0,				/*c_ratio */
   0,				/*c_num */
   0,				/*c_idx */
   0,				/*c_selcnt */
   NULL,			/*c_time */
   NULL,			/*c_ref */
   0.0,				/*v_ratio */
   0,				/*v_num */
   0,				/*v_idx */
   NULL /*v */ }		/*cv_info */
};
static XASL_CACHE_ENT_CV_INFO *xasl_ent_cv = &xasl_ent_cache.cv_info;
#if defined (ENABLE_UNUSED_FUNCTION)
/* XASL clone cache and related information */
static XASL_CACHE_CLO_INFO xasl_clo_cache = {
  0,				/*max_clones */
  0,				/*num */
  {0,				/*lookup */
   0,				/*hit */
   0,				/*miss */
   0 /*full */ },		/*counter */
  NULL,				/*head */
  NULL,				/*tail */
  NULL,				/*free_list */
  0,				/*n_alloc */
  NULL				/*alloc_arr */
};
#endif /* ENABLE_UNUSED_FUNCTION */

/* XASL cache entry pool */
static XASL_CACHE_ENTRY_POOL xasl_cache_entry_pool = { NULL, 0, -1 };

/*
 *  XASL_CACHE_ENTRY memory structure :=
 *      [|ent structure itself|TRANID array(tran_id_array)
 *       |OID array(class_oid_ilst)|int array(repr_id_list)
 *       |char array(query_string)|]
 *  ; malloc all in one memory block
*/
#if defined(SERVER_MODE)

#define XASL_CACHE_ENTRY_ALLOC_SIZE(qlen, noid) \
        (sizeof(XASL_CACHE_ENTRY)       /* space for structure */ \
         + sizeof(int) * MAX_NTRANS/* space for tran_index_array */ \
         + sizeof(OID) * (noid)         /* space for class_oid_list */ \
         + sizeof(int) * (noid)    /* space for repr_id_list */ \
         + (qlen))		/* space for query_string */
#define XASL_CACHE_ENTRY_TRAN_INDEX_ARRAY(ent) \
        (int *) ((char *) ent + sizeof(XASL_CACHE_ENTRY))
#define XASL_CACHE_ENTRY_CLASS_OID_LIST(ent) \
        (OID *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                 sizeof(TRANID) * MAX_NTRANS)
#define XASL_CACHE_ENTRY_REPR_ID_LIST(ent) \
        (int *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                      sizeof(int) * MAX_NTRANS + \
                      sizeof(OID) * ent->n_oid_list)
#define XASL_CACHE_ENTRY_QUERY_STRING(ent) \
        (char *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                  sizeof(int) * MAX_NTRANS + \
                  sizeof(OID) * ent->n_oid_list + \
                  sizeof(int) * ent->n_oid_list)

#else /* SA_MODE */

#define XASL_CACHE_ENTRY_ALLOC_SIZE(qlen, noid) \
        (sizeof(XASL_CACHE_ENTRY)       /* space for structure */ \
         + sizeof(OID) * (noid)         /* space for class_oid_list */ \
         + sizeof(int) * (noid)    /* space for repr_id_list */ \
         + (qlen))		/* space for query_string */
#define XASL_CACHE_ENTRY_CLASS_OID_LIST(ent) \
        (OID *) ((char *) ent + sizeof(XASL_CACHE_ENTRY))
#define XASL_CACHE_ENTRY_REPR_ID_LIST(ent) \
        (int *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                      sizeof(OID) * ent->n_oid_list)
#define XASL_CACHE_ENTRY_QUERY_STRING(ent) \
        (char *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                  sizeof(OID) * ent->n_oid_list + \
                  sizeof(int) * ent->n_oid_list)

#endif /* SERVER_MODE */

static DB_LOGICAL qexec_eval_instnum_pred (THREAD_ENTRY * thread_p,
					   XASL_NODE * xasl,
					   XASL_STATE * xasl_state);
static int qexec_end_one_iteration (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				    XASL_STATE * xasl_state,
				    QFILE_TUPLE_RECORD * tplrec);
static void qexec_failure_line (int line, XASL_STATE * xasl_state);
static int qexec_clear_xasl_head (THREAD_ENTRY * thread_p, XASL_NODE * xasl);
static int qexec_clear_arith_list (XASL_NODE * xasl_p, ARITH_TYPE * list,
				   int final);
static int qexec_clear_regu_var (XASL_NODE * xasl_p, REGU_VARIABLE * regu_var,
				 int final);
static int qexec_clear_regu_list (XASL_NODE * xasl_p, REGU_VARIABLE_LIST list,
				  int final);
static void qexec_clear_db_val_list (QPROC_DB_VALUE_LIST list);
static int qexec_clear_pred (XASL_NODE * xasl_p, PRED_EXPR * pr, int final);
static int qexec_clear_access_spec_list (XASL_NODE * xasl_p,
					 THREAD_ENTRY * thread_p,
					 ACCESS_SPEC_TYPE * list, int final);
static int qexec_clear_agg_list (XASL_NODE * xasl_p, AGGREGATE_TYPE * list,
				 int final);
static void qexec_clear_head_lists (THREAD_ENTRY * thread_p,
				    XASL_NODE * xasl_list);
static void qexec_clear_scan_all_lists (THREAD_ENTRY * thread_p,
					XASL_NODE * xasl_list);
static void qexec_clear_all_lists (THREAD_ENTRY * thread_p,
				   XASL_NODE * xasl_list);
static DB_LOGICAL qexec_eval_ordbynum_pred (THREAD_ENTRY * thread_p,
					    ORDBYNUM_INFO * ordby_info);
static int qexec_ordby_put_next (THREAD_ENTRY * thread_p,
				 const RECDES * recdes, void *arg);
static int qexec_orderby_distinct (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				   QUERY_OPTIONS option,
				   XASL_STATE * xasl_state);
static DB_LOGICAL qexec_eval_grbynum_pred (THREAD_ENTRY * thread_p,
					   GROUPBY_STATE * gbstate);
static GROUPBY_STATE *qexec_initialize_groupby_state (GROUPBY_STATE * gbstate,
						      SORT_LIST *
						      groupby_list,
						      PRED_EXPR * having_pred,
						      PRED_EXPR *
						      grbynum_pred,
						      DB_VALUE * grbynum_val,
						      int grbynum_flag,
						      XASL_NODE * eptr_list,
						      AGGREGATE_TYPE *
						      g_agg_list,
						      REGU_VARIABLE_LIST
						      g_regu_list,
						      VAL_LIST * g_val_list,
						      OUTPTR_LIST *
						      g_outptr_list,
						      int with_rollup,
						      XASL_STATE * xasl_state,
						      QFILE_TUPLE_VALUE_TYPE_LIST
						      * type_list,
						      QFILE_TUPLE_RECORD *
						      tplrec);
static void qexec_clear_groupby_state (THREAD_ENTRY * thread_p,
				       GROUPBY_STATE * gbstate);
static int qexec_initialize_groupby_rollup (GROUPBY_STATE * gbstate);
static void qexec_clear_groupby_rollup (THREAD_ENTRY * thread_p,
					GROUPBY_STATE * gbstate);
static void qexec_gby_start_group (THREAD_ENTRY * thread_p,
				   GROUPBY_STATE * gbstate,
				   const RECDES * key);
static void qexec_gby_agg_tuple (THREAD_ENTRY * thread_p,
				 GROUPBY_STATE * gbstate, QFILE_TUPLE tpl,
				 int peek);
static void qexec_gby_finalize_group (THREAD_ENTRY * thread_p,
				      GROUPBY_STATE * gbstate);
static void qexec_gby_start_rollup_group (THREAD_ENTRY * thread_p,
					  GROUPBY_STATE * gbstate,
					  const RECDES * key,
					  int rollup_level);
static void qexec_gby_finalize_rollup_group (THREAD_ENTRY * thread_p,
					     GROUPBY_STATE * gbstate,
					     int rollup_level);
static SORT_STATUS qexec_gby_get_next (THREAD_ENTRY * thread_p,
				       RECDES * recdes, void *arg);
static int qexec_gby_put_next (THREAD_ENTRY * thread_p, const RECDES * recdes,
			       void *arg);
static int qexec_groupby (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			  XASL_STATE * xasl_state,
			  QFILE_TUPLE_RECORD * tplrec);
static int qexec_collection_has_null (DB_VALUE * colval);
static DB_VALUE_COMPARE_RESULT qexec_cmp_tpl_vals_merge (QFILE_TUPLE *
							 left_tval,
							 TP_DOMAIN **
							 left_dom,
							 QFILE_TUPLE *
							 rght_tval,
							 TP_DOMAIN **
							 rght_dom,
							 int tval_cnt);
static long qexec_size_remaining (QFILE_TUPLE_RECORD * tplrec1,
				  QFILE_TUPLE_RECORD * tplrec2,
				  QFILE_LIST_MERGE_INFO * merge_info, int k);
static int qexec_merge_tuple (QFILE_TUPLE_RECORD * tplrec1,
			      QFILE_TUPLE_RECORD * tplrec2,
			      QFILE_LIST_MERGE_INFO * merge_info,
			      QFILE_TUPLE_RECORD * tplrec);
static int qexec_merge_tuple_add_list (THREAD_ENTRY * thread_p,
				       QFILE_LIST_ID * list_id,
				       QFILE_TUPLE_RECORD * tplrec1,
				       QFILE_TUPLE_RECORD * tplrec2,
				       QFILE_LIST_MERGE_INFO * merge_info,
				       QFILE_TUPLE_RECORD * tplrec);
static QFILE_LIST_ID *qexec_merge_list (THREAD_ENTRY * thread_p,
					QFILE_LIST_ID * outer_list_idp,
					QFILE_LIST_ID * inner_list_idp,
					QFILE_LIST_MERGE_INFO * merge_infop,
					int ls_flag);
static QFILE_LIST_ID *qexec_merge_list_outer (THREAD_ENTRY * thread_p,
					      SCAN_ID * outer_sid,
					      SCAN_ID * inner_sid,
					      QFILE_LIST_MERGE_INFO *
					      merge_infop,
					      PRED_EXPR *
					      other_outer_join_pred,
					      XASL_STATE * xasl_state,
					      int ls_flag);
static int qexec_merge_listfiles (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				  XASL_STATE * xasl_state);
static int qexec_open_scan (THREAD_ENTRY * thread_p,
			    ACCESS_SPEC_TYPE * curr_spec, VAL_LIST * val_list,
			    VAL_DESCR * vd, int readonly_scan, int fixed,
			    int grouped, bool iscan_oid_order,
			    SCAN_ID * s_id);
static SCAN_CODE qexec_next_merge_block (THREAD_ENTRY * thread_p,
					 ACCESS_SPEC_TYPE ** spec);

static SCAN_CODE qexec_next_scan_block_iterations (THREAD_ENTRY * thread_p,
						   XASL_NODE * xasl);
static SCAN_CODE qexec_execute_scan (THREAD_ENTRY * thread_p,
				     XASL_NODE * xasl,
				     XASL_STATE * xasl_state,
				     QFILE_TUPLE_RECORD * ignore,
				     XASL_SCAN_FNC_PTR next_scan_fnc);
static SCAN_CODE qexec_intprt_fnc (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				   XASL_STATE * xasl_state,
				   QFILE_TUPLE_RECORD * tplrec,
				   XASL_SCAN_FNC_PTR next_scan_fnc);
static SCAN_CODE qexec_merge_fnc (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				  XASL_STATE * xasl_state,
				  QFILE_TUPLE_RECORD * tplrec,
				  XASL_SCAN_FNC_PTR ignore);
static int qexec_setup_list_id (XASL_NODE * xasl);
static int qexec_execute_update (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				 XASL_STATE * xasl_state);
static int qexec_execute_delete (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				 XASL_STATE * xasl_state);
static int qexec_execute_obj_set_fetch (THREAD_ENTRY * thread_p,
					XASL_NODE * xasl,
					XASL_STATE * xasl_state);
static int qexec_execute_increment (THREAD_ENTRY * thread_p, OID * oid,
				    OID * class_oid, HFID * class_hfid,
				    ATTR_ID attrid, int n_increment);
static int qexec_execute_selupd_list (THREAD_ENTRY * thread_p,
				      XASL_NODE * xasl,
				      XASL_STATE * xasl_state);
static int qexec_start_connect_by_lists (THREAD_ENTRY * thread_p,
					 XASL_NODE * xasl,
					 XASL_STATE * xasl_state);
static int qexec_update_connect_by_lists (THREAD_ENTRY * thread_p,
					  XASL_NODE * xasl,
					  XASL_STATE * xasl_state,
					  QFILE_TUPLE_RECORD * tplrec);
static void qexec_end_connect_by_lists (THREAD_ENTRY * thread_p,
					XASL_NODE * xasl);
static void qexec_clear_connect_by_lists (XASL_NODE * xasl);
static int qexec_execute_connect_by (THREAD_ENTRY * thread_p,
				     XASL_NODE * xasl,
				     XASL_STATE * xasl_state,
				     QFILE_TUPLE_RECORD * tplrec);
static int qexec_iterate_connect_by_results (THREAD_ENTRY * thread_p,
					     XASL_NODE * xasl,
					     XASL_STATE * xasl_state,
					     QFILE_TUPLE_RECORD * tplrec);
static int qexec_insert_tuple_into_list (THREAD_ENTRY * thread_p,
					 QFILE_LIST_ID * list_id,
					 OUTPTR_LIST * outptr_list,
					 XASL_STATE * xasl_state,
					 QFILE_TUPLE_RECORD * tplrec);
static int qexec_check_for_cycle (THREAD_ENTRY * thread_p,
				  OUTPTR_LIST * outptr_list, QFILE_TUPLE tpl,
				  QFILE_TUPLE_VALUE_TYPE_LIST * type_list,
				  QFILE_LIST_ID * list_id_p, int *iscycle);
static int qexec_compare_valptr_with_tuple (OUTPTR_LIST * outptr_list,
					    QFILE_TUPLE tpl,
					    QFILE_TUPLE_VALUE_TYPE_LIST *
					    type_list, int *are_equal);
static int qexec_listfile_orderby (THREAD_ENTRY * thread_p,
				   QFILE_LIST_ID * list_file,
				   SORT_LIST * orderby_list,
				   XASL_STATE * xasl_state,
				   OUTPTR_LIST * outptr_list);
static int compute_son_index (THREAD_ENTRY * thread_p,
			      char **son_index, char *father_index,
			      int *len_son_index, int cnt);
static int qexec_end_buildvalueblock_iterations (THREAD_ENTRY * thread_p,
						 XASL_NODE * xasl,
						 XASL_STATE * xasl_state,
						 QFILE_TUPLE_RECORD * tplrec);
static int qexec_end_mainblock_iterations (THREAD_ENTRY * thread_p,
					   XASL_NODE * xasl,
					   XASL_STATE * xasl_state,
					   QFILE_TUPLE_RECORD * tplrec);
#if defined(SERVER_MODE)
#if defined (ENABLE_UNUSED_FUNCTION)
static int tranid_compare (const void *t1, const void *t2);	/* TODO: put to header ?? */
#endif
#endif
static unsigned int xasl_id_hash (const void *key, unsigned int htsize);
static int qexec_print_xasl_cache_ent (FILE * fp, const void *key,
				       void *data, void *args);
static XASL_CACHE_ENTRY *qexec_alloc_xasl_cache_ent (int req_size);
#if defined (ENABLE_UNUSED_FUNCTION)
static XASL_CACHE_CLONE *qexec_expand_xasl_cache_clo_arr (int n_exp);
static XASL_CACHE_CLONE *qexec_alloc_xasl_cache_clo (XASL_CACHE_ENTRY * ent);
static int qexec_append_LRU_xasl_cache_clo (XASL_CACHE_CLONE * clo);
static int qexec_delete_LRU_xasl_cache_clo (XASL_CACHE_CLONE * clo);
#endif
static int qexec_free_xasl_cache_ent (THREAD_ENTRY * thread_p, void *data,
				      void *args);
static int qexec_select_xasl_cache_ent (THREAD_ENTRY * thread_p, void *data,
					void *args);
#if defined(SERVER_MODE)
static int qexec_remove_my_transaction_id (THREAD_ENTRY * thread_p,
					   XASL_CACHE_ENTRY * ent);
#endif
static int qexec_delete_xasl_cache_ent (THREAD_ENTRY * thread_p, void *data,
					void *args);

static int qexec_partition_select (THREAD_ENTRY * thread_p, HFID * hfid,
				   OID * oid, int *partition_node_id,
				   XASL_STATE * xasl_state,
				   HEAP_CACHE_ATTRINFO * attr_info,
				   XASL_PARTITION_INFO * xpi,
				   REGU_VARIABLE ** rv);
static int qexec_partition_chg_select (THREAD_ENTRY * thread_p,
				       XASL_STATE * xasl_state,
				       HEAP_CACHE_ATTRINFO * attr_info,
				       XASL_PARTITION_INFO * xpi,
				       REGU_VARIABLE ** rv);
static int qexec_partition_list_search (XASL_PARTITION_INFO * xpi,
					DB_VALUE * sval);
static int qexec_partition_range_search (XASL_PARTITION_INFO * xpi,
					 DB_VALUE * sval);
static REGU_VARIABLE *replace_null_arith (REGU_VARIABLE * regu_var,
					  DB_VALUE * set_dbval);
static REGU_VARIABLE *replace_null_dbval (REGU_VARIABLE * regu_var,
					  DB_VALUE * set_dbval);
static int qexec_partition_get_parts (XASL_PARTITION_INFO * xpi,
				      DB_VALUE * sval);
static void qexec_replace_prior_regu_vars (THREAD_ENTRY * thread_p,
					   REGU_VARIABLE * regu,
					   XASL_NODE * xasl);
static void qexec_replace_prior_regu_vars_pred (THREAD_ENTRY * thread_p,
						PRED_EXPR * pred,
						XASL_NODE * xasl);
static int qexec_init_index_pseudocolumn_strings (THREAD_ENTRY * thread_p,
						  char **father_index,
						  int *len_father_index,
						  char **son_index,
						  int *len_son_index);
static void qexec_set_pseudocolumns_val_pointers (XASL_NODE * xasl,
						  DB_VALUE ** level_valp,
						  DB_VALUE ** isleaf_valp,
						  DB_VALUE ** iscycle_valp,
						  DB_VALUE ** parent_pos_valp,
						  DB_VALUE ** index_valp);
static int qexec_get_index_pseudocolumn_value_from_tuple (THREAD_ENTRY *
							  thread_p,
							  XASL_NODE * xasl,
							  QFILE_TUPLE tpl,
							  DB_VALUE **
							  index_valp,
							  char **index_value,
							  int *index_len);
static int qexec_recalc_tuples_parent_pos_in_list (THREAD_ENTRY * thread_p,
						   QFILE_LIST_ID * list_id_p);
static int qexec_remove_duplicates_for_replace (THREAD_ENTRY * thread_p,
						HEAP_SCANCACHE * scan_cache,
						HEAP_CACHE_ATTRINFO *
						attr_info,
						HEAP_CACHE_ATTRINFO *
						index_attr_info,
						const HEAP_IDX_ELEMENTS_INFO *
						idx_info,
						XASL_PARTITION_INFO *
						partition_info);
static int qexec_oid_of_duplicate_key_update (THREAD_ENTRY * thread_p,
					      HEAP_SCANCACHE * scan_cache,
					      HEAP_CACHE_ATTRINFO * attr_info,
					      HEAP_CACHE_ATTRINFO *
					      index_attr_info,
					      const HEAP_IDX_ELEMENTS_INFO *
					      idx_info, OID * unique_oid);
static int qexec_fill_oid_of_duplicate_key (THREAD_ENTRY * thread_p,
					    XASL_NODE * xasl,
					    XASL_STATE * xasl_state,
					    OID * unique_oid);
static int qexec_execute_duplicate_key_update (THREAD_ENTRY * thread_p,
					       XASL_NODE * xasl,
					       XASL_STATE * xasl_state,
					       OID * unique_oid,
					       int *force_count);
static int qexec_execute_do_stmt (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				  XASL_STATE * xasl_state);

static int *tranid_lsearch (const int *key, int *base, int *nmemb);
static int *tranid_lfind (const int *key, const int *base, int *nmemb);

/*
 * Utility routines
 */

/*
 * qexec_eval_instnum_pred () -
 *   return:
 *   xasl(in)   :
 *   xasl_state(in)     :
 */
static DB_LOGICAL
qexec_eval_instnum_pred (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			 XASL_STATE * xasl_state)
{
  DB_LOGICAL ev_res;

  /* instant numbering; increase the value of inst_num() by 1 */
  if (xasl->instnum_val)
    {
      xasl->instnum_val->data.i++;
    }

  if (xasl->instnum_pred)
    {
      /* evaluate predicate */
      ev_res = eval_pred (thread_p, xasl->instnum_pred,
			  &xasl_state->vd, NULL);
      switch (ev_res)
	{
	case V_FALSE:
	  /* evaluation is false; if check flag was set, stop scan */
	  if (xasl->instnum_flag & XASL_INSTNUM_FLAG_SCAN_CHECK)
	    {
	      xasl->instnum_flag |= XASL_INSTNUM_FLAG_SCAN_STOP;
	    }
	  break;
	case V_TRUE:
	  /* evaluation is true; if not continue scan mode, set scan check flag */
	  if (!(xasl->instnum_flag & XASL_INSTNUM_FLAG_SCAN_CONTINUE)
	      && !(xasl->instnum_flag & XASL_INSTNUM_FLAG_SCAN_CHECK))
	    {
	      xasl->instnum_flag |= XASL_INSTNUM_FLAG_SCAN_CHECK;
	    }
	  break;
	case V_ERROR:
	  break;
	default:		/* V_UNKNOWN */
	  break;
	}
    }
  else
    {
      /* no predicate; always true */
      ev_res = V_TRUE;
    }

  return ev_res;
}

/*
 * qexec_end_one_iteration () -
 *   return: NO_ERROR or ER_code
 *   xasl(in)   :
 *   xasl_state(in)     :
 *   tplrec(in) :
 *
 * Note: Processing to be accomplished when a candidate row has been qualified.
 */
static int
qexec_end_one_iteration (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			 XASL_STATE * xasl_state, QFILE_TUPLE_RECORD * tplrec)
{
  DB_VALUE *dbval, element;
  REGU_VARIABLE_LIST reg_varptr;
  OID instance_oid, class_oid;
  DB_TYPE typ;
  QFILE_TUPLE_DESCRIPTOR *tdp;
  QFILE_LIST_ID *xlist_id;
  QPROC_TPLDESCR_STATUS tpldescr_status;
  int ret = NO_ERROR;

  if (xasl->composite_locking)
    {
      /* By convention, the first value in the outptr list is the instance
       * OID, the second value in the outptr list is the class OID.
       */
      reg_varptr = xasl->outptr_list->valptrp;
      if (xasl->outptr_list->valptr_cnt < 2)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      ret = fetch_peek_dbval (thread_p, &reg_varptr->value, &xasl_state->vd,
			      NULL, NULL, NULL, &dbval);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      typ = db_value_domain_type (dbval);
      if (typ == DB_TYPE_VOBJ)
	{
	  /* grab the real oid */
	  ret = db_seq_get (DB_GET_SEQUENCE (dbval), 2, &element);
	  if (ret != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  dbval = &element;
	  typ = db_value_domain_type (dbval);
	}

      if (typ != DB_TYPE_OID)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      COPY_OID (&instance_oid, DB_GET_OID (dbval));

      reg_varptr = reg_varptr->next;
      ret = fetch_peek_dbval (thread_p, &reg_varptr->value, &xasl_state->vd,
			      NULL, NULL, NULL, &dbval);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      typ = db_value_domain_type (dbval);
      if (typ == DB_TYPE_VOBJ)
	{
	  /* grab the real oid */
	  ret = db_seq_get (DB_GET_SEQUENCE (dbval), 2, &element);
	  if (ret != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  dbval = &element;
	  typ = db_value_domain_type (dbval);
	}

      if (typ != DB_TYPE_OID)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      COPY_OID (&class_oid, DB_GET_OID (dbval));

      ret =
	lock_add_composite_lock (thread_p, &xasl->composite_lock,
				 &instance_oid, &class_oid);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  if (xasl->type == BUILDLIST_PROC)
    {
      /* make f_valp array */
      xlist_id = xasl->list_id;
      if ((xlist_id->tpl_descr.f_valp == NULL)
	  && (xlist_id->type_list.type_cnt > 0))
	{
	  size_t size = xlist_id->type_list.type_cnt * DB_SIZEOF (DB_VALUE *);
	  xlist_id->tpl_descr.f_valp = (DB_VALUE **) malloc (size);
	  if (xlist_id->tpl_descr.f_valp == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      /* build tuple descriptor */
      tdp = &((xasl->list_id)->tpl_descr);
      tpldescr_status =
	qdata_generate_tuple_desc_for_valptr_list (thread_p,
						   xasl->outptr_list,
						   &xasl_state->vd, tdp);
      if (tpldescr_status == QPROC_TPLDESCR_FAILURE)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      switch (tpldescr_status)
	{
	case QPROC_TPLDESCR_SUCCESS:
	  /* generate tuple into list file page */
	  if (qfile_generate_tuple_into_list (thread_p, xasl->list_id,
					      T_NORMAL) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  break;

	case QPROC_TPLDESCR_RETRY_SET_TYPE:
	case QPROC_TPLDESCR_RETRY_BIG_REC:
	  /* BIG QFILE_TUPLE or a SET-field is included */
	  if (tplrec->tpl == NULL)
	    {
	      /* allocate tuple descriptor */
	      tplrec->size = DB_PAGESIZE;
	      tplrec->tpl = (QFILE_TUPLE) db_private_alloc (thread_p,
							    DB_PAGESIZE);
	      if (tplrec->tpl == NULL)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	  if ((qdata_copy_valptr_list_to_tuple (thread_p, xasl->outptr_list,
						&xasl_state->vd,
						tplrec) != NO_ERROR)
	      || (qfile_add_tuple_to_list (thread_p, xasl->list_id,
					   tplrec->tpl) != NO_ERROR))
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  break;

	default:
	  break;
	}
    }
  else if (xasl->type == BUILDVALUE_PROC)
    {
      if (xasl->proc.buildvalue.agg_list != NULL)
	{
	  if (qdata_evaluate_aggregate_list (thread_p,
					     xasl->proc.buildvalue.agg_list,
					     &xasl_state->vd) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * Clean_up processing routines
 */

/*
 * qexec_failure_line () -
 *   return: int
 *   line(in)   :
 *   xasl_state(in)     :
 */
static void
qexec_failure_line (int line, XASL_STATE * xasl_state)
{
  if (!xasl_state->qp_xasl_line)
    {
      xasl_state->qp_xasl_line = line;
    }
}

/*
 * qexec_clear_xasl_head () -
 *   return: int
 *   xasl(in)   : XASL Tree procedure block
 *
 * Note: Clear XASL head node by destroying the resultant list file,
 * if any, and also resultant single values, if any. Return the
 * number of total pages deallocated.
 */
static int
qexec_clear_xasl_head (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  int pg_cnt = 0;
  VAL_LIST *single_tuple;
  QPROC_DB_VALUE_LIST value_list;
  int i;

  if (xasl->list_id)
    {				/* destroy list file */
      (void) qfile_close_list (thread_p, xasl->list_id);
      qfile_destroy_list (thread_p, xasl->list_id);
    }

  single_tuple = xasl->single_tuple;
  if (single_tuple)
    {
      /* clear result value */
      for (value_list = single_tuple->valp, i = 0;
	   i < single_tuple->val_cnt; value_list = value_list->next, i++)
	{
	  pr_clear_value (value_list->val);
	}
    }

  if (XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
    {
      qexec_clear_xasl_head (thread_p, xasl->connect_by_ptr);
    }

  xasl->status = XASL_CLEARED;

  return pg_cnt;
}

/*
 * qexec_clear_arith_list () - clear the db_values in the db_val list
 *   return:
 *   xasl_p(in) :
 *   list(in)   :
 *   final(in)  :
 */
static int
qexec_clear_arith_list (XASL_NODE * xasl_p, ARITH_TYPE * list, int final)
{
  ARITH_TYPE *p;
  int pg_cnt;

  pg_cnt = 0;
  for (p = list; p; p = p->next)
    {
      pr_clear_value (p->value);
      pg_cnt += qexec_clear_regu_var (xasl_p, p->leftptr, final);
      pg_cnt += qexec_clear_regu_var (xasl_p, p->rightptr, final);
      pg_cnt += qexec_clear_regu_var (xasl_p, p->thirdptr, final);
      pg_cnt += qexec_clear_pred (xasl_p, p->pred, final);

      if (p->rand_seed != NULL)
	{
	  free_and_init (p->rand_seed);
	}
    }

  return pg_cnt;
}

/*
 * qexec_clear_regu_var () - clear the db_values in the regu_variable
 *   return:
 *   xasl_p(in) :
 *   regu_var(in) :      :
 *   final(in)  :
 */
static int
qexec_clear_regu_var (XASL_NODE * xasl_p, REGU_VARIABLE * regu_var, int final)
{
  int pg_cnt;

  pg_cnt = 0;
  if (!regu_var)
    {
      return pg_cnt;
    }

  switch (regu_var->type)
    {
    case TYPE_ATTR_ID:		/* fetch object attribute value */
    case TYPE_SHARED_ATTR_ID:
    case TYPE_CLASS_ATTR_ID:
      regu_var->value.attr_descr.cache_dbvalp = NULL;
      break;
    case TYPE_CONSTANT:
      pr_clear_value (regu_var->value.dbvalptr);
      break;
    case TYPE_INARITH:
    case TYPE_OUTARITH:
      pg_cnt +=
	qexec_clear_arith_list (xasl_p, regu_var->value.arithptr, final);
      break;
    case TYPE_AGGREGATE:
      pr_clear_value (regu_var->value.aggptr->value);
      pg_cnt +=
	qexec_clear_regu_var (xasl_p, &regu_var->value.aggptr->operand,
			      final);
      break;
    case TYPE_FUNC:
      pr_clear_value (regu_var->value.funcp->value);
      pg_cnt +=
	qexec_clear_regu_list (xasl_p, regu_var->value.funcp->operand, final);
      break;
    case TYPE_DBVAL:
      /* FIXME::
       * Though regu_var->value.dbval should be freed,
       * there are a complicated issue on asynchronous query execution.
       * During executing an asynchrouos query, private heap id is changed
       * and this brings the problem. We do not know the heap id of
       * the allocator thread, so we cannot free it at this time.
       * Memory leak will break out when a query which has set
       * (ie, select set{} from ...) is executed under asynchronous mode.
       */
      if (!XASL_IS_FLAGED (xasl_p, XASL_QEXEC_MODE_ASYNC))
	{
	  pr_clear_value (&regu_var->value.dbval);
	}
      break;
    default:
      break;
    }

  return pg_cnt;
}


/*
 * qexec_clear_regu_list () - clear the db_values in the regu list
 *   return:
 *   xasl_p(in) :
 *   list(in)   :
 *   final(in)  :
 */
static int
qexec_clear_regu_list (XASL_NODE * xasl_p, REGU_VARIABLE_LIST list, int final)
{
  REGU_VARIABLE_LIST p;
  int pg_cnt;

  pg_cnt = 0;
  for (p = list; p; p = p->next)
    {
      pg_cnt += qexec_clear_regu_var (xasl_p, &p->value, final);
    }

  return pg_cnt;
}

/*
 * qexec_clear_db_val_list () - clear the db_values in the db_val list
 *   return:
 *   list(in)   :
 */
static void
qexec_clear_db_val_list (QPROC_DB_VALUE_LIST list)
{
  QPROC_DB_VALUE_LIST p;

  for (p = list; p; p = p->next)
    {
      pr_clear_value (p->val);
    }

}

/*
 * qexec_clear_pred () - clear the db_values in a predicate
 *   return:
 *   xasl_p(in) :
 *   pr(in)     :
 *   final(in)  :
 */
static int
qexec_clear_pred (XASL_NODE * xasl_p, PRED_EXPR * pr, int final)
{
  int pg_cnt;
  PRED_EXPR *expr;

  pg_cnt = 0;
  if (!pr)
    {
      return pg_cnt;
    }

  switch (pr->type)
    {
    case T_PRED:
      pg_cnt += qexec_clear_pred (xasl_p, pr->pe.pred.lhs, final);
      for (expr = pr->pe.pred.rhs;
	   expr && expr->type == T_PRED; expr = expr->pe.pred.rhs)
	{
	  pg_cnt += qexec_clear_pred (xasl_p, expr->pe.pred.lhs, final);
	}
      pg_cnt += qexec_clear_pred (xasl_p, expr, final);
      break;
    case T_EVAL_TERM:
      switch (pr->pe.eval_term.et_type)
	{
	case T_COMP_EVAL_TERM:
	  {
	    COMP_EVAL_TERM *et_comp = &pr->pe.eval_term.et.et_comp;

	    pg_cnt += qexec_clear_regu_var (xasl_p, et_comp->lhs, final);
	    pg_cnt += qexec_clear_regu_var (xasl_p, et_comp->rhs, final);
	  }
	  break;
	case T_ALSM_EVAL_TERM:
	  {
	    ALSM_EVAL_TERM *et_alsm = &pr->pe.eval_term.et.et_alsm;

	    pg_cnt += qexec_clear_regu_var (xasl_p, et_alsm->elem, final);
	    pg_cnt += qexec_clear_regu_var (xasl_p, et_alsm->elemset, final);
	  }
	  break;
	case T_LIKE_EVAL_TERM:
	  {
	    LIKE_EVAL_TERM *et_like = &pr->pe.eval_term.et.et_like;

	    pg_cnt += qexec_clear_regu_var (xasl_p, et_like->src, final);
	    pg_cnt += qexec_clear_regu_var (xasl_p, et_like->pattern, final);
	  }
	  break;
	}
      break;
    case T_NOT_TERM:
      pg_cnt += qexec_clear_pred (xasl_p, pr->pe.not_term, final);
      break;
    }

  return pg_cnt;
}

/*
 * qexec_clear_access_spec_list () - clear the db_values in the access spec list
 *   return:
 *   xasl_p(in) :
 *   list(in)   :
 *   final(in)  :
 */
static int
qexec_clear_access_spec_list (XASL_NODE * xasl_p, THREAD_ENTRY * thread_p,
			      ACCESS_SPEC_TYPE * list, int final)
{
  ACCESS_SPEC_TYPE *p;
  HEAP_SCAN_ID *hsidp;
  INDX_SCAN_ID *isidp;
  int pg_cnt;

  /* I'm not sure this access structure could be anymore complicated
   * (surely some of these dbvalues are redundant)
   */

  pg_cnt = 0;
  for (p = list; p; p = p->next)
    {
      pr_clear_value (p->s_dbval);
      pg_cnt += qexec_clear_pred (xasl_p, p->where_pred, final);
      pg_cnt += qexec_clear_pred (xasl_p, p->where_key, final);
      pr_clear_value (p->s_id.join_dbval);
      switch (p->s_id.type)
	{
	case S_HEAP_SCAN:
	case S_CLASS_ATTR_SCAN:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.hsid.scan_pred.regu_list,
				   final);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.hsid.rest_regu_list,
				   final);
	  hsidp = &p->s_id.s.hsid;
	  if (hsidp->caches_inited)
	    {
	      heap_attrinfo_end (thread_p, hsidp->pred_attrs.attr_cache);
	      heap_attrinfo_end (thread_p, hsidp->rest_attrs.attr_cache);
	      hsidp->caches_inited = false;
	    }
	  break;
	case S_INDX_SCAN:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.isid.key_pred.regu_list,
				   final);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.isid.scan_pred.regu_list,
				   final);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.isid.rest_regu_list,
				   final);
	  isidp = &p->s_id.s.isid;
	  if (isidp->caches_inited)
	    {
	      if (isidp->key_pred.regu_list)
		{
		  heap_attrinfo_end (thread_p, isidp->key_attrs.attr_cache);
		}
	      heap_attrinfo_end (thread_p, isidp->pred_attrs.attr_cache);
	      heap_attrinfo_end (thread_p, isidp->rest_attrs.attr_cache);
	      isidp->caches_inited = false;
	    }
	  break;
	case S_LIST_SCAN:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p,
				   p->s_id.s.llsid.scan_pred.regu_list,
				   final);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.llsid.rest_regu_list,
				   final);
	  break;
	case S_SET_SCAN:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.ssid.scan_pred.regu_list,
				   final);
	  break;
	case S_METHOD_SCAN:
	  break;
	}
      if (p->s_id.val_list)
	{
	  qexec_clear_db_val_list (p->s_id.val_list->valp);
	}
      switch (p->type)
	{
	case TARGET_CLASS:
	case TARGET_CLASS_ATTR:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.cls_node.cls_regu_list_key,
				   final);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.cls_node.cls_regu_list_pred,
				   final);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.cls_node.cls_regu_list_rest,
				   final);
	  if (p->access == INDEX)
	    {
	      INDX_INFO *indx_info;

	      indx_info = p->indexptr;
	      if (indx_info)
		{
		  int i, N;

		  N = indx_info->key_info.key_cnt;
		  for (i = 0; i < N; i++)
		    {
		      pg_cnt +=
			qexec_clear_regu_var (xasl_p, indx_info->key_info.
					      key_ranges[i].key1, final);
		      pg_cnt +=
			qexec_clear_regu_var (xasl_p, indx_info->key_info.
					      key_ranges[i].key2, final);
		    }
		}
	    }
	  break;
	case TARGET_LIST:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.list_node.list_regu_list_pred,
				   final);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.list_node.list_regu_list_rest,
				   final);
	  break;
	case TARGET_SET:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.set_node.set_regu_list,
				   final);
	  pg_cnt +=
	    qexec_clear_regu_var (xasl_p, p->s_id.s.ssid.set_ptr, final);
	  pr_clear_value (&p->s_id.s.ssid.set);
	  break;
	case TARGET_METHOD:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.method_node.method_regu_list,
				   final);
	  break;
	}
    }

  return pg_cnt;
}

/*
 * qexec_clear_agg_list () - clear the db_values in the agg list
 *   return:
 *   xasl_p(in) :
 *   list(in)   :
 *   final(in)  :
 */
static int
qexec_clear_agg_list (XASL_NODE * xasl_p, AGGREGATE_TYPE * list, int final)
{
  AGGREGATE_TYPE *p;
  int pg_cnt;

  pg_cnt = 0;
  for (p = list; p; p = p->next)
    {
      pr_clear_value (p->value);
      pg_cnt += qexec_clear_regu_var (xasl_p, &p->operand, final);
    }

  return pg_cnt;
}

/*
 * qexec_clear_xasl () -
 *   return: int
 *   xasl(in)   : XASL Tree procedure block
 *   final(in)  : true iff DB_VALUES, etc should be whacked
 *                (i.e., if this XASL tree will ***NEVER*** be used again)
 *
 * Note: Destroy all the list files (temporary or result list files)
 * created during interpretation of XASL Tree procedure block
 * and return the number of total pages deallocated.
 */
int
qexec_clear_xasl (THREAD_ENTRY * thread_p, XASL_NODE * xasl, bool final)
{
  int i, j;
  int pg_cnt;
  int query_save_state;

  pg_cnt = 0;
  if (xasl == NULL)
    {
      return pg_cnt;
    }

  /*
   ** We set this because in some M paths (e.g. when a driver crashes)
   ** the function qexec_clear_xasl() can be called recursively. By setting
   ** the query_in_progress flag, we prevent qmgr_clear_trans_wakeup() from
   ** clearing the xasl structure; thus preventing a core at the
   ** primary calling level.
   */
  query_save_state = xasl->query_in_progress;

  xasl->query_in_progress = true;

  /* clear the head node */
  pg_cnt += qexec_clear_xasl_head (thread_p, xasl);

  /* abort the composite locking */
  if (xasl->composite_locking)
    {
      lock_abort_composite_lock (&xasl->composite_lock);
      (void) lock_demote_all_update_inst_locks (thread_p);
    }

  /* clear the body node */
  if (xasl->aptr_list)
    {
      if (XASL_IS_FLAGED (xasl, XASL_QEXEC_MODE_ASYNC))
	{
	  /* propagate XASL_QEXEC_MODE_ASYNC flag */
	  XASL_SET_FLAG (xasl->aptr_list, XASL_QEXEC_MODE_ASYNC);
	}
      pg_cnt += qexec_clear_xasl (thread_p, xasl->aptr_list, final);
    }
  if (xasl->bptr_list)
    {
      if (XASL_IS_FLAGED (xasl, XASL_QEXEC_MODE_ASYNC))
	{
	  /* propagate XASL_QEXEC_MODE_ASYNC flag */
	  XASL_SET_FLAG (xasl->bptr_list, XASL_QEXEC_MODE_ASYNC);
	}
      pg_cnt += qexec_clear_xasl (thread_p, xasl->bptr_list, final);
    }
  if (xasl->dptr_list)
    {
      if (XASL_IS_FLAGED (xasl, XASL_QEXEC_MODE_ASYNC))
	{
	  /* propagate XASL_QEXEC_MODE_ASYNC flag */
	  XASL_SET_FLAG (xasl->dptr_list, XASL_QEXEC_MODE_ASYNC);
	}
      pg_cnt += qexec_clear_xasl (thread_p, xasl->dptr_list, final);
    }
  if (xasl->fptr_list)
    {
      if (XASL_IS_FLAGED (xasl, XASL_QEXEC_MODE_ASYNC))
	{
	  /* propagate XASL_QEXEC_MODE_ASYNC flag */
	  XASL_SET_FLAG (xasl->fptr_list, XASL_QEXEC_MODE_ASYNC);
	}
      pg_cnt += qexec_clear_xasl (thread_p, xasl->fptr_list, final);
    }
  if (xasl->scan_ptr)
    {
      if (XASL_IS_FLAGED (xasl, XASL_QEXEC_MODE_ASYNC))
	{
	  /* propagate XASL_QEXEC_MODE_ASYNC flag */
	  XASL_SET_FLAG (xasl->scan_ptr, XASL_QEXEC_MODE_ASYNC);
	}
      pg_cnt += qexec_clear_xasl (thread_p, xasl->scan_ptr, final);
    }

  /* clear the CONNECT BY node */
  if (XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
    {
      if (xasl->connect_by_ptr
	  && XASL_IS_FLAGED (xasl, XASL_QEXEC_MODE_ASYNC))
	{
	  /* propagate XASL_QEXEC_MODE_ASYNC flag */
	  XASL_SET_FLAG (xasl->connect_by_ptr, XASL_QEXEC_MODE_ASYNC);
	}
      pg_cnt += qexec_clear_xasl (thread_p, xasl->connect_by_ptr, final);
    }

  if (final)
    {
      /* clear the db_values in the tree */
      if (xasl->outptr_list)
	{
	  pg_cnt +=
	    qexec_clear_regu_list (xasl, xasl->outptr_list->valptrp, final);
	}
      pg_cnt +=
	qexec_clear_access_spec_list (xasl, thread_p, xasl->spec_list, final);
      pg_cnt +=
	qexec_clear_access_spec_list (xasl, thread_p, xasl->merge_spec,
				      final);
      if (xasl->val_list)
	{
	  qexec_clear_db_val_list (xasl->val_list->valp);
	}
      if (xasl->merge_val_list)
	{
	  qexec_clear_db_val_list (xasl->merge_val_list->valp);
	}
      pg_cnt += qexec_clear_pred (xasl, xasl->after_join_pred, final);
      pg_cnt += qexec_clear_pred (xasl, xasl->if_pred, final);
      if (xasl->instnum_val)
	{
	  pr_clear_value (xasl->instnum_val);
	}
      pg_cnt += qexec_clear_pred (xasl, xasl->instnum_pred, final);
      if (xasl->ordbynum_val)
	{
	  pr_clear_value (xasl->ordbynum_val);
	}
      pg_cnt += qexec_clear_pred (xasl, xasl->ordbynum_pred, final);

      if (xasl->level_val)
	{
	  pr_clear_value (xasl->level_val);
	}
      if (xasl->isleaf_val)
	{
	  pr_clear_value (xasl->isleaf_val);
	}
      if (xasl->iscycle_val)
	{
	  pr_clear_value (xasl->iscycle_val);
	}
    }

  switch (xasl->type)
    {

    case BUILDLIST_PROC:
      {
	BUILDLIST_PROC_NODE *buildlist = &xasl->proc.buildlist;

	if (buildlist->eptr_list)
	  {
	    if (XASL_IS_FLAGED (xasl, XASL_QEXEC_MODE_ASYNC))
	      {
		/* propagate XASL_QEXEC_MODE_ASYNC flag */
		XASL_SET_FLAG (buildlist->eptr_list, XASL_QEXEC_MODE_ASYNC);
	      }
	    pg_cnt +=
	      qexec_clear_xasl (thread_p, buildlist->eptr_list, final);
	  }

	if (xasl->curr_spec)
	  {
	    scan_end_scan (thread_p, &xasl->curr_spec->s_id);
	    scan_close_scan (thread_p, &xasl->curr_spec->s_id);
	  }
	if (xasl->merge_spec)
	  {
	    scan_end_scan (thread_p, &xasl->merge_spec->s_id);
	    scan_close_scan (thread_p, &xasl->merge_spec->s_id);
	  }
	if (final)
	  {
	    if (buildlist->g_outptr_list)
	      {
		pg_cnt +=
		  qexec_clear_regu_list (xasl,
					 buildlist->g_outptr_list->valptrp,
					 final);
	      }
	    pg_cnt +=
	      qexec_clear_regu_list (xasl, buildlist->g_regu_list, final);
	    if (buildlist->g_val_list)
	      {
		qexec_clear_db_val_list (buildlist->g_val_list->valp);
	      }
	    pg_cnt +=
	      qexec_clear_agg_list (xasl, buildlist->g_agg_list, final);
	    pg_cnt +=
	      qexec_clear_arith_list (xasl, buildlist->g_outarith_list,
				      final);
	    pg_cnt +=
	      qexec_clear_pred (xasl, buildlist->g_having_pred, final);
	    pg_cnt +=
	      qexec_clear_pred (xasl, buildlist->g_grbynum_pred, final);
	    if (buildlist->g_grbynum_val)
	      {
		pr_clear_value (buildlist->g_grbynum_val);
	      }
	  }
      }
      break;

    case OBJFETCH_PROC:
    case SETFETCH_PROC:
      if (final)
	{
	  FETCH_PROC_NODE *fetch = &xasl->proc.fetch;

	  pg_cnt += qexec_clear_pred (xasl, fetch->set_pred, final);
	  pr_clear_value (fetch->arg);
	}
      break;

    case BUILDVALUE_PROC:
      {
	BUILDVALUE_PROC_NODE *buildvalue = &xasl->proc.buildvalue;

	if (xasl->curr_spec)
	  {
	    scan_end_scan (thread_p, &xasl->curr_spec->s_id);
	    scan_close_scan (thread_p, &xasl->curr_spec->s_id);
	  }
	if (xasl->merge_spec)
	  {
	    scan_end_scan (thread_p, &xasl->merge_spec->s_id);
	    scan_close_scan (thread_p, &xasl->merge_spec->s_id);
	  }
	if (final)
	  {
	    pg_cnt +=
	      qexec_clear_agg_list (xasl, buildvalue->agg_list, final);
	    pg_cnt +=
	      qexec_clear_arith_list (xasl, buildvalue->outarith_list, final);
	    pg_cnt += qexec_clear_pred (xasl, buildvalue->having_pred, final);
	    if (buildvalue->grbynum_val)
	      {
		pr_clear_value (buildvalue->grbynum_val);
	      }
	  }
      }
      break;

    case SCAN_PROC:
      if (xasl->curr_spec)
	{
	  scan_end_scan (thread_p, &xasl->curr_spec->s_id);
	  scan_close_scan (thread_p, &xasl->curr_spec->s_id);
	}
      break;

    case UPDATE_PROC:
      if (final)
	{
	  UPDATE_PROC_NODE *update = &xasl->proc.update;

	  for (i = 0; i < update->no_classes; i++)
	    {
	      if (update->partition[i])
		{
		  for (j = 0; j < update->partition[i]->no_parts; j++)
		    {
		      pr_clear_value (update->partition[i]->parts[j]->vals);
		    }
		}
	    }
	}
      break;

    case INSERT_PROC:
      if (final)
	{
	  INSERT_PROC_NODE *insert = &xasl->proc.insert;

	  if (insert->partition)
	    {
	      for (i = 0; i < insert->partition->no_parts; i++)
		{
		  pr_clear_value (insert->partition->parts[i]->vals);
		}
	    }
	}

    default:
      break;
    }				/* switch */

  /* Note: Here reset the current pointer to access spacification nodes.
   *       This is needed beause this XASL tree may be used again if
   *       this thread is suspended and restarted.
   */
  xasl->curr_spec = NULL;

  /* clear the next xasl node */

  if (xasl->next)
    {
      if (XASL_IS_FLAGED (xasl, XASL_QEXEC_MODE_ASYNC))
	{
	  /* propagate XASL_QEXEC_MODE_ASYNC flag */
	  XASL_SET_FLAG (xasl->next, XASL_QEXEC_MODE_ASYNC);
	}
      pg_cnt += qexec_clear_xasl (thread_p, xasl->next, final);
    }

  xasl->query_in_progress = query_save_state;

  return pg_cnt;
}

/*
 * qexec_clear_head_lists () -
 *   return:
 *   xasl_list(in)      : List of XASL procedure blocks
 *
 * Note: Traverse through the given list of XASL procedure blocks and
 * clean/destroy results generated by interpretation of these
 * blocks such as list files generated.
 */
static void
qexec_clear_head_lists (THREAD_ENTRY * thread_p, XASL_NODE * xasl_list)
{
  XASL_NODE *xasl;

  for (xasl = xasl_list; xasl != NULL; xasl = xasl->next)
    {
      if (XASL_IS_FLAGED (xasl, XASL_ZERO_CORR_LEVEL))
	{
	  /* skip out zero correlation-level uncorrelated subquery */
	  continue;
	}
      /* clear XASL head node */
      (void) qexec_clear_xasl_head (thread_p, xasl);
    }
}

/*
 * qexec_clear_scan_all_lists () -
 *   return:
 *   xasl_list(in)      :
 */
static void
qexec_clear_scan_all_lists (THREAD_ENTRY * thread_p, XASL_NODE * xasl_list)
{
  XASL_NODE *xasl;

  for (xasl = xasl_list; xasl != NULL; xasl = xasl->scan_ptr)
    {
      if (xasl->bptr_list)
	{
	  qexec_clear_head_lists (thread_p, xasl->bptr_list);
	}
      if (xasl->fptr_list)
	{
	  qexec_clear_head_lists (thread_p, xasl->fptr_list);
	}
      if (xasl->dptr_list)
	{
	  qexec_clear_head_lists (thread_p, xasl->dptr_list);
	}
    }
}

/*
 * qexec_clear_all_lists () -
 *   return:
 *   xasl_list(in)      :
 */
static void
qexec_clear_all_lists (THREAD_ENTRY * thread_p, XASL_NODE * xasl_list)
{
  XASL_NODE *xasl;

  for (xasl = xasl_list; xasl != NULL; xasl = xasl->next)
    {
      /* check for NULL pointers before doing pointless procedure calls.
       * This procedure is called once per row, and typically will
       * have many or all of these xasl sublists NULL.
       * In the limiting case of scanning rows as fast as possible,
       * these empty procedure calls amounted to several percent
       * of the cpu time.
       */
      if (xasl->bptr_list)
	{
	  qexec_clear_all_lists (thread_p, xasl->bptr_list);
	}
      if (xasl->fptr_list)
	{
	  qexec_clear_all_lists (thread_p, xasl->fptr_list);
	}

      /* Note: Dptr lists are only procedure blocks (other than aptr_list)
       * which can produce a LIST FILE. Therefore, we are trying to clear
       * all the dptr_list result LIST FILES in the XASL tree per iteration.
       */
      if (xasl->dptr_list)
	{
	  qexec_clear_head_lists (thread_p, xasl->dptr_list);
	}

      if (xasl->scan_ptr)
	{
	  qexec_clear_scan_all_lists (thread_p, xasl->scan_ptr);
	}
    }
}

/*
 * qexec_get_xasl_list_id () -
 *   return: QFILE_LIST_ID *, or NULL
 *   xasl(in)   : XASL Tree procedure block
 *
 * Note: Extract the list file identifier from the head node of the
 * specified XASL tree procedure block. This represents the
 * result of the interpretation of the block.
 */
QFILE_LIST_ID *
qexec_get_xasl_list_id (XASL_NODE * xasl)
{
  QFILE_LIST_ID *list_id = (QFILE_LIST_ID *) NULL;
  VAL_LIST *single_tuple;
  QPROC_DB_VALUE_LIST value_list;
  int i;

  if (xasl->list_id)
    {
      /* allocate region for list file identifier */
      list_id = (QFILE_LIST_ID *) malloc (sizeof (QFILE_LIST_ID));
      if (list_id == NULL)
	{
	  return (QFILE_LIST_ID *) NULL;
	}

      QFILE_CLEAR_LIST_ID (list_id);
      if (qfile_copy_list_id (list_id, xasl->list_id, true) != NO_ERROR)
	{
	  qfile_free_list_id (list_id);
	  return (QFILE_LIST_ID *) NULL;
	}
      qfile_clear_list_id (xasl->list_id);
    }

  single_tuple = xasl->single_tuple;
  if (single_tuple)
    {
      /* clear result value */
      for (value_list = single_tuple->valp, i = 0;
	   i < single_tuple->val_cnt; value_list = value_list->next, i++)
	{
	  pr_clear_value (value_list->val);
	}
    }

  return list_id;
}

/*
 * qexec_eval_ordbynum_pred () -
 *   return:
 *   ordby_info(in)     :
 */
static DB_LOGICAL
qexec_eval_ordbynum_pred (THREAD_ENTRY * thread_p, ORDBYNUM_INFO * ordby_info)
{
  DB_LOGICAL ev_res;

  if (ordby_info->ordbynum_val)
    {
      /* Increment the value of orderby_num() used for "order by" numbering */
      ordby_info->ordbynum_val->data.i++;
    }

  if (ordby_info->ordbynum_pred)
    {
      /*
       * Evaluate the predicate.
       * CUBRID does not currently support such predicates in WHERE condition
       * lists but might support them in future versions (see the usage of
       * MSGCAT_SEMANTIC_ORDERBYNUM_SELECT_LIST_ERR). Currently such
       * predicates must only be used with the "order by [...] for [...]"
       * syntax.
       * Sample query:
       *   select * from participant
       *   order by silver for orderby_num() between 1 and 10;
       * Invalid query (at present):
       *   select * from participant where orderby_num() between 1 and 10
       *   order by silver;
       */
      ev_res =
	eval_pred (thread_p, ordby_info->ordbynum_pred,
		   &ordby_info->xasl_state->vd, NULL);
      switch (ev_res)
	{
	case V_FALSE:
	  if (ordby_info->ordbynum_flag & XASL_ORDBYNUM_FLAG_SCAN_CHECK)
	    {
	      /* If in the "scan check" mode then signal that the scan should
	       * stop, as there will be no more tuples to return.
	       */
	      ordby_info->ordbynum_flag |= XASL_ORDBYNUM_FLAG_SCAN_STOP;
	    }
	  break;
	case V_TRUE:
	  /* The predicate evaluated as true. It is possible that we are in
	   * the "continue scan" mode, indicated by
	   * XASL_ORDBYNUM_FLAG_SCAN_CONTINUE. This mode means we should
	   * continue evaluating the predicate for all the other tuples
	   * because the predicate is complex and we cannot predict its vale.
	   * If the predicate is very simple we can predict that it will be
	   * true for a single range of tuples, like the range in the
	   * following example:
	   * Tuple1 Tuple2 Tuple3 Tuple4 Tuple5 Tuple6 Tuple7 Tuple8 Tuple9
	   * False  False  False  True   True   True   True   False  False
	   * When we find the first true predicate we set the "scan check"
	   * mode.
	   */
	  if (!(ordby_info->ordbynum_flag & XASL_ORDBYNUM_FLAG_SCAN_CONTINUE)
	      && !(ordby_info->ordbynum_flag & XASL_ORDBYNUM_FLAG_SCAN_CHECK))
	    {
	      ordby_info->ordbynum_flag |= XASL_ORDBYNUM_FLAG_SCAN_CHECK;
	    }
	  break;
	case V_ERROR:
	  break;
	case V_UNKNOWN:
	default:
	  break;
	}
    }
  else
    {
      /* No predicate was given so no filtering is required. */
      ev_res = V_TRUE;
    }

  return ev_res;
}

/*
 * qexec_ordby_put_next () -
 *   return:
 *   recdes(in) :
 *   arg(in)    :
 */
static int
qexec_ordby_put_next (THREAD_ENTRY * thread_p, const RECDES * recdes,
		      void *arg)
{
  SORT_INFO *info;
  SORT_REC *key;
  char *data, *tvalhp;
  int tval_size;
  ORDBYNUM_INFO *ordby_info;
  PAGE_PTR page;
  VPID ovfl_vpid;
  DB_LOGICAL ev_res;
  int error;
  int i;
  VPID vpid;
  QFILE_LIST_ID *list_idp;
  QFILE_TUPLE_RECORD tplrec;

  error = NO_ERROR;

  info = (SORT_INFO *) arg;
  ordby_info = (ORDBYNUM_INFO *) info->extra_arg;

  /* Traverse next link */
  for (key = (SORT_REC *) recdes->data; key && error == NO_ERROR;
       key = key->next)
    {
      ev_res = V_TRUE;
      if (ordby_info != NULL && ordby_info->ordbynum_val)
	{
	  /* evaluate orderby_num predicates */
	  ev_res = qexec_eval_ordbynum_pred (thread_p, ordby_info);
	  if (ev_res == V_ERROR)
	    {
	      return er_errid ();
	    }
	  if (ordby_info->ordbynum_flag & XASL_ORDBYNUM_FLAG_SCAN_STOP)
	    {
	      /* reset ordbynum_val for next use */
	      DB_MAKE_INT (ordby_info->ordbynum_val, 0);
	      /* setting SORT_PUT_STOP will make 'sr_in_sort()' stop processing;
	         the caller, 'qexec_gby_put_next()', returns 'gbstate->state' */
	      return SORT_PUT_STOP;
	    }
	}

      if (ordby_info != NULL && ev_res == V_TRUE)
	{
	  if (info->key_info.use_original)
	    {			/* P_sort_key */
	      /* We need to consult the original file for the bonafide tuple.
	         The SORT_REC only kept the keys that we needed so that we
	         wouldn't have to drag them around while we were sorting. */

	      list_idp = &(info->s_id->s_id->list_id);
	      vpid.pageid = key->s.original.pageid;
	      vpid.volid = key->s.original.volid;

#if 0				/* SortCache */
	      /* check if page is already fixed */
	      if (VPID_EQ (&(info->fixed_vpid), &vpid))
		{
		  /* use cached page pointer */
		  page = info->fixed_page;
		}
	      else
		{
		  /* free currently fixed page */
		  if (info->fixed_page != NULL)
		    {
		      qmgr_free_old_page (info->fixed_page,
					  list_idp->tfile_vfid);
		      info->fixed_page = NULL;
		    }

		  /* fix page and cache fixed vpid */
		  page = qmgr_get_old_page (&vpid, list_idp->tfile_vfid);
		  if (page == NULL)
		    {
		      return er_errid ();
		    }

		  /* cache page pointer */
		  info->fixed_vpid = vpid;
		  info->fixed_page = page;
		}		/* else */
#else
	      page =
		qmgr_get_old_page (thread_p, &vpid, list_idp->tfile_vfid);
	      if (page == NULL)
		{
		  return er_errid ();
		}
#endif

	      QFILE_GET_OVERFLOW_VPID (&ovfl_vpid, page);

	      if (ovfl_vpid.pageid == NULL_PAGEID ||
		  ovfl_vpid.pageid == NULL_PAGEID_ASYNC)
		{
		  /* This is the normal case of a non-overflow tuple. We can
		     use the page image directly, since we know that the
		     tuple resides entirely on that page. */
		  data = page + key->s.original.offset;
		  /* update orderby_num() in the tuple */
		  for (i = 0; ordby_info && i < ordby_info->ordbynum_pos_cnt;
		       i++)
		    {
		      QFILE_GET_TUPLE_VALUE_HEADER_POSITION (data,
							     ordby_info->
							     ordbynum_pos[i],
							     tvalhp);
		      (void) qdata_copy_db_value_to_tuple_value (ordby_info->
								 ordbynum_val,
								 tvalhp,
								 &tval_size);
		    }
		  error =
		    qfile_add_tuple_to_list (thread_p, info->output_file,
					     data);
		}
	      else
		{
		  /* Rats; this tuple requires overflow pages. We need to
		     copy all of the pages from the input file to the output
		     file. */
		  if (ordby_info && ordby_info->ordbynum_pos_cnt > 0)
		    {
		      /* I think this way is very inefficient. */
		      tplrec.size = 0;
		      tplrec.tpl = NULL;
		      qfile_get_tuple (thread_p, page,
				       page + key->s.original.offset, &tplrec,
				       list_idp);
		      data = tplrec.tpl;
		      /* update orderby_num() in the tuple */
		      for (i = 0;
			   ordby_info && i < ordby_info->ordbynum_pos_cnt;
			   i++)
			{
			  QFILE_GET_TUPLE_VALUE_HEADER_POSITION (data,
								 ordby_info->
								 ordbynum_pos
								 [i], tvalhp);
			  (void)
			    qdata_copy_db_value_to_tuple_value (ordby_info->
								ordbynum_val,
								tvalhp,
								&tval_size);
			}
		      error =
			qfile_add_tuple_to_list (thread_p, info->output_file,
						 data);
		      db_private_free_and_init (thread_p, tplrec.tpl);
		    }
		  else
		    {
		      error =
			qfile_add_overflow_tuple_to_list (thread_p,
							  info->output_file,
							  page, list_idp);
		    }
		}
#if 1				/* SortCache */
	      qmgr_free_old_page (thread_p, page, list_idp->tfile_vfid);
#endif
	    }
	  else
	    {			/* A_sort_key */
	      /* We didn't record the original vpid, and we should just
	         reconstruct the original record from this sort key (rather
	         than pressure the page buffer pool by reading in the original
	         page to get the original tuple) */

	      if (qfile_generate_sort_tuple (&info->key_info, key,
					     &info->output_recdes) == NULL)
		{
		  error = ER_FAILED;
		}
	      else
		{
		  data = info->output_recdes.data;
		  /* update orderby_num() in the tuple */
		  for (i = 0; ordby_info && i < ordby_info->ordbynum_pos_cnt;
		       i++)
		    {
		      QFILE_GET_TUPLE_VALUE_HEADER_POSITION (data,
							     ordby_info->
							     ordbynum_pos[i],
							     tvalhp);
		      (void) qdata_copy_db_value_to_tuple_value (ordby_info->
								 ordbynum_val,
								 tvalhp,
								 &tval_size);
		    }
		  error =
		    qfile_add_tuple_to_list (thread_p, info->output_file,
					     data);
		}
	    }

	}			/* if (ev_res == V_TRUE) */

    }				/* for (key = (SORT_REC *) recdes->data; ...) */

  return (error == NO_ERROR) ? NO_ERROR : er_errid ();
}

/*
 * qexec_orderby_distinct () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   :
 *   option(in) : Distinct/All indication flag
 *   xasl_state(in)     : Ptr to the XASL_STATE for this tree
 *
 * Note: Depending on the indicated set of sorting items and the value
 * of the distinct/all option, the given list file is sorted on
 * several columns and/or duplications are eliminated. If only
 * duplication elimination is specified and all the colums of the
 * list file contains orderable types (non-sets), first the list
 * file is sorted on all colums and then duplications are
 * eliminated on the fly, thus causing and ordered-distinct list file output.
 */
static int
qexec_orderby_distinct (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			QUERY_OPTIONS option, XASL_STATE * xasl_state)
{
  QFILE_LIST_ID *list_id = xasl->list_id;
  SORT_LIST *order_list = xasl->orderby_list;
  PRED_EXPR *ordbynum_pred = xasl->ordbynum_pred;
  DB_VALUE *ordbynum_val = xasl->ordbynum_val;
  int ordbynum_flag = xasl->ordbynum_flag;
  OUTPTR_LIST *outptr_list;
  SORT_LIST *orderby_ptr, *order_ptr, *orderby_list;
  SORT_LIST *order_ptr2, temp_ord;
  bool orderby_alloc;
  int k, n, i, ls_flag;
  ORDBYNUM_INFO ordby_info;
  REGU_VARIABLE_LIST regu_list;
  SORT_PUT_FUNC *put_fn;

  outptr_list = (xasl->type == BUILDLIST_PROC
		 && xasl->proc.buildlist.groupby_list) ? xasl->proc.buildlist.
    g_outptr_list : xasl->outptr_list;

  if (order_list != NULL || option == Q_DISTINCT)
    {

      /* sort the result list file */
      /* form the linked list of sort type items */
      if (option != Q_DISTINCT)
	{
	  orderby_list = order_list;
	  orderby_alloc = false;
	}
      else
	{
	  /* allocate space for  sort list */
	  orderby_list =
	    qfile_allocate_sort_list (list_id->type_list.type_cnt);
	  if (orderby_list == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  /* form an order_by list including all list file positions */
	  orderby_alloc = true;
	  for (k = 0, order_ptr = orderby_list;
	       k < list_id->type_list.type_cnt;
	       k++, order_ptr = order_ptr->next)
	    {
	      order_ptr->s_order = S_ASC;
	      order_ptr->pos_descr.dom = list_id->type_list.domp[k];
	      order_ptr->pos_descr.pos_no = k;
	    }			/* for */

	  /* put the original order_by specifications, if any,
	   * to the beginning of the order_by list.
	   */
	  for (orderby_ptr = order_list, order_ptr = orderby_list;
	       orderby_ptr != NULL;
	       orderby_ptr = orderby_ptr->next, order_ptr = order_ptr->next)
	    {
	      /* save original content */
	      temp_ord.s_order = order_ptr->s_order;
	      temp_ord.pos_descr.dom = order_ptr->pos_descr.dom;
	      temp_ord.pos_descr.pos_no = order_ptr->pos_descr.pos_no;

	      /* put originial order_by node */
	      order_ptr->s_order = orderby_ptr->s_order;
	      order_ptr->pos_descr.dom = orderby_ptr->pos_descr.dom;
	      order_ptr->pos_descr.pos_no = orderby_ptr->pos_descr.pos_no;

	      /* put temporary node into old order_by node position */
	      for (order_ptr2 = order_ptr->next; order_ptr2 != NULL;
		   order_ptr2 = order_ptr2->next)
		{
		  if (orderby_ptr->pos_descr.pos_no ==
		      order_ptr2->pos_descr.pos_no)
		    {
		      order_ptr2->s_order = temp_ord.s_order;
		      order_ptr2->pos_descr.dom = temp_ord.pos_descr.dom;
		      order_ptr2->pos_descr.pos_no =
			temp_ord.pos_descr.pos_no;
		      break;	/* immediately exit inner loop */
		    };
		}
	    }

	}			/* if-else */

      /* sort the list file */
      ordby_info.ordbynum_pos_cnt = 0;
      ordby_info.ordbynum_pos = ordby_info.reserved;
      if (outptr_list)
	{
	  for (n = 0, regu_list = outptr_list->valptrp; regu_list;
	       regu_list = regu_list->next)
	    {
	      if (regu_list->value.type == TYPE_ORDERBY_NUM)
		{
		  n++;
		}
	    }
	  ordby_info.ordbynum_pos_cnt = n;
	  if (n > 2)
	    {
	      ordby_info.ordbynum_pos = (int *) db_private_alloc (thread_p,
								  sizeof (int)
								  * n);
	      if (ordby_info.ordbynum_pos == NULL)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	  for (n = 0, i = 0, regu_list = outptr_list->valptrp; regu_list;
	       regu_list = regu_list->next, i++)
	    {
	      if (regu_list->value.type == TYPE_ORDERBY_NUM)
		{
		  ordby_info.ordbynum_pos[n++] = i;
		}
	    }
	}

      ordby_info.xasl_state = xasl_state;
      ordby_info.ordbynum_pred = ordbynum_pred;
      ordby_info.ordbynum_val = ordbynum_val;
      ordby_info.ordbynum_flag = ordbynum_flag;
      put_fn = (ordbynum_val) ? &qexec_ordby_put_next : NULL;

      if (ordbynum_val == NULL
	  && orderby_list
	  && qfile_is_sort_list_covered (list_id->sort_list,
					 orderby_list) == true)
	{
	  /* no need to sort here
	   */
	}
      else
	{
	  ls_flag = ((option == Q_DISTINCT) ? QFILE_FLAG_DISTINCT
		     : QFILE_FLAG_ALL);
	  /* If this is the top most XASL, then the list file to be open will be
	     the last result file.
	     (Note that 'order by' is the last processing.) */
	  if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL)
	      && XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED))
	    {
	      QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
	    }

	  list_id = qfile_sort_list_with_func (thread_p, list_id,
					       orderby_list, option, ls_flag,
					       NULL, put_fn, NULL,
					       &ordby_info);
	  if (list_id == NULL)
	    {
	      if (orderby_alloc == true)
		{
		  qfile_free_sort_list (orderby_list);
		}
	      GOTO_EXIT_ON_ERROR;
	    }
	}
      if (ordby_info.ordbynum_pos != ordby_info.reserved)
	{
	  db_private_free_and_init (thread_p, ordby_info.ordbynum_pos);
	}

      /* free temporarily allocated areas */
      if (orderby_alloc == true)
	{
	  qfile_free_sort_list (orderby_list);
	}
    }				/* if */

  /* duplicates elimination has already been done at qfile_sort_list_with_func()
   */

  return NO_ERROR;

exit_on_error:

  return ER_FAILED;
}

/*
 * qexec_eval_grbynum_pred () -
 *   return:
 *   gbstate(in)        :
 */
static DB_LOGICAL
qexec_eval_grbynum_pred (THREAD_ENTRY * thread_p, GROUPBY_STATE * gbstate)
{
  DB_LOGICAL ev_res;

  /* groupby numbering; increase the value of groupby_num() by 1 */
  if (gbstate->grbynum_val)
    {
      gbstate->grbynum_val->data.i++;
    }

  if (gbstate->grbynum_pred)
    {
      /* evaluate predicate */
      ev_res = eval_pred (thread_p, gbstate->grbynum_pred,
			  &gbstate->xasl_state->vd, NULL);
      switch (ev_res)
	{
	case V_FALSE:
	  /* evaluation is false; if check flag was set, stop scan */
	  if (gbstate->grbynum_flag & XASL_G_GRBYNUM_FLAG_SCAN_CHECK)
	    {
	      gbstate->grbynum_flag |= XASL_G_GRBYNUM_FLAG_SCAN_STOP;
	    }
	  break;

	case V_TRUE:
	  /* evaluation is true; if not continue scan mode, set scan check flag */
	  if (!(gbstate->grbynum_flag & XASL_G_GRBYNUM_FLAG_SCAN_CONTINUE)
	      && !(gbstate->grbynum_flag & XASL_G_GRBYNUM_FLAG_SCAN_CHECK))
	    {
	      gbstate->grbynum_flag |= XASL_G_GRBYNUM_FLAG_SCAN_CHECK;
	    }
	  break;

	case V_ERROR:
	  break;

	case V_UNKNOWN:
	default:
	  break;
	}
    }
  else
    {
      /* no predicate; always true */
      ev_res = V_TRUE;
    }

  return ev_res;
}

/*
 * qexec_initialize_groupby_state () -
 *   return:
 *   gbstate(in)        :
 *   groupby_list(in)   : Group_by sorting list specification
 *   having_pred(in)    : Having predicate expression
 *   grbynum_pred(in)   :
 *   grbynum_val(in)    :
 *   grbynum_flag(in)   :
 *   eptr_list(in)      : Having subquery list
 *   g_agg_list(in)     : Group_by aggregation list
 *   g_regu_list(in)    : Regulator Variable List
 *   g_val_list(in)     : Value List
 *   g_outptr_list(in)  : Output pointer list
 *   g_with_rollup(in)	: Has WITH ROLLUP clause
 *   xasl_state(in)     : XASL tree state information
 *   type_list(in)      :
 *   tplrec(out) 	: Tuple record descriptor to store result tuples
 */
static GROUPBY_STATE *
qexec_initialize_groupby_state (GROUPBY_STATE * gbstate,
				SORT_LIST * groupby_list,
				PRED_EXPR * having_pred,
				PRED_EXPR * grbynum_pred,
				DB_VALUE * grbynum_val, int grbynum_flag,
				XASL_NODE * eptr_list,
				AGGREGATE_TYPE * g_agg_list,
				REGU_VARIABLE_LIST g_regu_list,
				VAL_LIST * g_val_list,
				OUTPTR_LIST * g_outptr_list, int with_rollup,
				XASL_STATE * xasl_state,
				QFILE_TUPLE_VALUE_TYPE_LIST * type_list,
				QFILE_TUPLE_RECORD * tplrec)
{
  gbstate->state = NO_ERROR;

  gbstate->input_scan = NULL;
#if 0				/* SortCache */
  VPID_SET_NULL (&(gbstate->fixed_vpid));
  gbstate->fixed_page = NULL;
#endif
  gbstate->output_file = NULL;

  gbstate->having_pred = having_pred;
  gbstate->grbynum_pred = grbynum_pred;
  gbstate->grbynum_val = grbynum_val;
  gbstate->grbynum_flag = grbynum_flag;
  gbstate->eptr_list = eptr_list;
  gbstate->g_agg_list = g_agg_list;
  gbstate->g_regu_list = g_regu_list;
  gbstate->g_val_list = g_val_list;
  gbstate->g_outptr_list = g_outptr_list;
  gbstate->xasl_state = xasl_state;

  gbstate->current_key.area_size = 0;
  gbstate->current_key.length = 0;
  gbstate->current_key.type = 0;	/* Unused */
  gbstate->current_key.data = NULL;
  gbstate->gby_rec.area_size = 0;
  gbstate->gby_rec.length = 0;
  gbstate->gby_rec.type = 0;	/* Unused */
  gbstate->gby_rec.data = NULL;
  gbstate->output_tplrec = NULL;
  gbstate->input_tpl.size = 0;
  gbstate->input_tpl.tpl = 0;
  gbstate->input_recs = 0;

  gbstate->rollup_levels = 0;
  gbstate->g_rollup_agg_list = NULL;

  if (qfile_initialize_sort_key_info (&gbstate->key_info, groupby_list,
				      type_list) == NULL)
    {
      return NULL;
    }

  gbstate->current_key.data = (char *) db_private_alloc (NULL, DB_PAGESIZE);
  if (gbstate->current_key.data == NULL)
    {
      return NULL;
    }
  gbstate->current_key.area_size = DB_PAGESIZE;

  gbstate->output_tplrec = tplrec;

  if (with_rollup)
    {
      /* initialize rollup aggregate lists */
      if (qexec_initialize_groupby_rollup (gbstate) != NO_ERROR)
	{
	  return NULL;
	}
    }

  return gbstate;
}

/*
 * qexec_clear_groupby_state () -
 *   return:
 *   gbstate(in)        :
 */
static void
qexec_clear_groupby_state (THREAD_ENTRY * thread_p, GROUPBY_STATE * gbstate)
{
#if 0				/* SortCache */
  QFILE_LIST_ID *list_idp;
#endif

  QEXEC_CLEAR_AGG_LIST_VALUE (gbstate->g_agg_list);
  if (gbstate->eptr_list)
    {
      qexec_clear_head_lists (thread_p, gbstate->eptr_list);
    }
  if (gbstate->current_key.data)
    {
      db_private_free_and_init (thread_p, gbstate->current_key.data);
      gbstate->current_key.area_size = 0;
    }
  if (gbstate->gby_rec.data)
    {
      db_private_free_and_init (thread_p, gbstate->gby_rec.data);
      gbstate->gby_rec.area_size = 0;
    }
  gbstate->output_tplrec = NULL;
  /*
   * Don't cleanup gbstate->input_tpl; the memory it points to was
   * managed by the listfile manager (via input_scan), and it's not
   * ours to free.
   */
#if 0				/* SortCache */
  list_idp = &(gbstate->input_scan->list_id);
  /* free currently fixed page */
  if (gbstate->fixed_page != NULL)
    {
      qmgr_free_old_page (gbstate->fixed_page, list_idp->tfile_vfid);
      gbstate->fixed_page = NULL;
    }
#endif

  qfile_clear_sort_key_info (&gbstate->key_info);
  if (gbstate->input_scan)
    {
      qfile_close_scan (thread_p, gbstate->input_scan);
      gbstate->input_scan = NULL;
    }
  if (gbstate->output_file)
    {
      qfile_close_list (thread_p, gbstate->output_file);
      qfile_free_list_id (gbstate->output_file);
      gbstate->output_file = NULL;
    }

  /* cleanup rollup aggregates lists */
  qexec_clear_groupby_rollup (thread_p, gbstate);
}

/*
 * qexec_gby_start_group () -
 *   return:
 *   gbstate(in)        :
 *   key(in)    :
 */
static void
qexec_gby_start_group (THREAD_ENTRY * thread_p, GROUPBY_STATE * gbstate,
		       const RECDES * key)
{
  XASL_STATE *xasl_state = gbstate->xasl_state;
  int error;

  if (gbstate->state != NO_ERROR)
    {
      return;
    }

  /*
   * Record the new key; keep it in SORT_KEY format so we can continue
   * to use the SORTKEY_INFO version of the comparison functions.
   *
   * WARNING: the sort module doesn't seem to set key->area_size
   * reliably, so the only thing we can rely on is key->length.
   */
  if (gbstate->current_key.area_size < key->length)
    {
      void *tmp;

      tmp = db_private_realloc (thread_p, gbstate->current_key.data,
				key->area_size);
      if (tmp == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      gbstate->current_key.data = (char *) tmp;
      gbstate->current_key.area_size = key->area_size;
    }
  memcpy (gbstate->current_key.data, key->data, key->length);
  gbstate->current_key.length = key->length;

  /*
   * (Re)initialize the various accumulator variables...
   */
  error = qdata_initialize_aggregate_list (thread_p, gbstate->g_agg_list,
					   gbstate->xasl_state->query_id);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

wrapup:
  return;

exit_on_error:
  gbstate->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_gby_agg_tuple () -
 *   return:
 *   gbstate(in)        :
 *   tpl(in)    :
 *   peek(in)   :
 */
static void
qexec_gby_agg_tuple (THREAD_ENTRY * thread_p, GROUPBY_STATE * gbstate,
		     QFILE_TUPLE tpl, int peek)
{
  XASL_STATE *xasl_state = gbstate->xasl_state;

  if (gbstate->state != NO_ERROR)
    {
      return;
    }

  /*
   * Read the incoming tuple into DB_VALUEs and do the necessary
   * aggregation...
   */
  if (fetch_val_list (thread_p, gbstate->g_regu_list,
		      &gbstate->xasl_state->vd, NULL, NULL,
		      tpl, peek) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  if (qdata_evaluate_aggregate_list (thread_p, gbstate->g_agg_list,
				     &gbstate->xasl_state->vd) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* evaluate rollup aggregates lists */
  if (gbstate->g_rollup_agg_list)
    {
      int i;
      for (i = 0; i < gbstate->rollup_levels; i++)
	{
	  if (qdata_evaluate_aggregate_list (thread_p,
					     gbstate->g_rollup_agg_list[i],
					     &gbstate->xasl_state->vd)
	      != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

wrapup:
  return;

exit_on_error:
  gbstate->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_gby_finalize_group () -
 *   return:
 *   gbstate(in)        :
 */
static void
qexec_gby_finalize_group (THREAD_ENTRY * thread_p, GROUPBY_STATE * gbstate)
{
  XASL_NODE *xptr;
  DB_LOGICAL ev_res;
  QFILE_TUPLE_DESCRIPTOR *tdp;
  QFILE_LIST_ID *xlist_id;
  QPROC_TPLDESCR_STATUS tpldescr_status;
  XASL_STATE *xasl_state = gbstate->xasl_state;

  if (gbstate->state != NO_ERROR)
    {
      return;
    }

  /*
   * See if the currently accumulated row qualifies (i.e., satisfies
   * any HAVING predicate) and if so, spit it out.
   */
  if (qdata_finalize_aggregate_list (thread_p, gbstate->g_agg_list) !=
      NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  for (xptr = gbstate->eptr_list; xptr; xptr = xptr->next)
    {
      if (qexec_execute_mainblock (thread_p, xptr, gbstate->xasl_state) !=
	  NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  ev_res = (gbstate->having_pred
	    ? eval_pred (thread_p, gbstate->having_pred,
			 &gbstate->xasl_state->vd, NULL) : V_TRUE);
  if (ev_res == V_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (ev_res == V_TRUE)
    {
      if (gbstate->grbynum_val)
	{
	  /* evaluate groupby_num predicates */
	  ev_res = qexec_eval_grbynum_pred (thread_p, gbstate);
	  if (ev_res == V_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  if (gbstate->grbynum_flag & XASL_G_GRBYNUM_FLAG_SCAN_STOP)
	    {
	      /* reset grbynum_val for next use */
	      DB_MAKE_INT (gbstate->grbynum_val, 0);
	      /* setting SORT_PUT_STOP will make 'sr_in_sort()' stop processing;
	         the caller, 'qexec_gby_put_next()', returns 'gbstate->state' */
	      gbstate->state = SORT_PUT_STOP;
	    }
	}
    }

  if (ev_res == V_TRUE)
    {
      /* make f_valp array */
      xlist_id = gbstate->output_file;
      if ((xlist_id->tpl_descr.f_valp == NULL)
	  && (xlist_id->type_list.type_cnt > 0))
	{
	  size_t size = xlist_id->type_list.type_cnt * sizeof (DB_VALUE *);
	  xlist_id->tpl_descr.f_valp = (DB_VALUE **) malloc (size);
	  if (xlist_id->tpl_descr.f_valp == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      /* build tuple descriptor */
      tdp = &(gbstate->output_file->tpl_descr);
      tpldescr_status =
	qdata_generate_tuple_desc_for_valptr_list (thread_p,
						   gbstate->g_outptr_list,
						   &gbstate->xasl_state->vd,
						   tdp);

      if (tpldescr_status == QPROC_TPLDESCR_FAILURE)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      switch (tpldescr_status)
	{
	case QPROC_TPLDESCR_SUCCESS:
	  /* generate tuple into list file page */
	  if (qfile_generate_tuple_into_list
	      (thread_p, gbstate->output_file, T_NORMAL) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  break;

	case QPROC_TPLDESCR_RETRY_SET_TYPE:
	case QPROC_TPLDESCR_RETRY_BIG_REC:
	  /* BIG QFILE_TUPLE or a SET-field is included */
	  if (gbstate->output_tplrec->tpl == NULL)
	    {
	      /* allocate tuple descriptor */
	      gbstate->output_tplrec->size = DB_PAGESIZE;
	      gbstate->output_tplrec->tpl =
		(QFILE_TUPLE) db_private_alloc (thread_p, DB_PAGESIZE);
	      if (gbstate->output_tplrec->tpl == NULL)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	  if (qdata_copy_valptr_list_to_tuple
	      (thread_p, gbstate->g_outptr_list, &gbstate->xasl_state->vd,
	       gbstate->output_tplrec) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  if (qfile_add_tuple_to_list (thread_p, gbstate->output_file,
				       gbstate->output_tplrec->tpl) !=
	      NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  break;

	default:
	  break;
	}
    }

wrapup:
  QEXEC_CLEAR_AGG_LIST_VALUE (gbstate->g_agg_list);
  if (gbstate->eptr_list)
    {
      qexec_clear_head_lists (thread_p, gbstate->eptr_list);
    }
  return;

exit_on_error:
  gbstate->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_gby_get_next () -
 *   return:
 *   recdes(in) :
 *   arg(in)    :
 */
static SORT_STATUS
qexec_gby_get_next (THREAD_ENTRY * thread_p, RECDES * recdes, void *arg)
{
  GROUPBY_STATE *gbstate;

  gbstate = (GROUPBY_STATE *) arg;

  return qfile_make_sort_key (thread_p, &gbstate->key_info,
			      recdes, gbstate->input_scan,
			      &gbstate->input_tpl);
}

/*
 * qexec_gby_put_next () -
 *   return:
 *   recdes(in) :
 *   arg(in)    :
 */
static int
qexec_gby_put_next (THREAD_ENTRY * thread_p, const RECDES * recdes, void *arg)
{
  GROUPBY_STATE *info;
  SORT_REC *key;
  char *data;
  PAGE_PTR page;
  VPID vpid;
  int peek;
  QFILE_LIST_ID *list_idp;

  QFILE_TUPLE_RECORD dummy;
  int status;
  int i, j, nkeys;

  info = (GROUPBY_STATE *) arg;
  list_idp = &(info->input_scan->list_id);

  data = NULL;
  page = NULL;

  /* Traverse next link */
  for (key = (SORT_REC *) recdes->data; key; key = key->next)
    {
      if (info->state != NO_ERROR)
	{
	  goto exit_on_error;
	}

      peek = COPY;		/* default */
      if (info->key_info.use_original)
	{			/* P_sort_key */
	  /*
	   * Retrieve the original tuple.  This will be the case if the
	   * original tuple had more fields than we were sorting on.
	   */
	  vpid.pageid = key->s.original.pageid;
	  vpid.volid = key->s.original.volid;

#if 0				/* SortCache */
	  /* check if page is already fixed */
	  if (VPID_EQ (&(info->fixed_vpid), &vpid))
	    {
	      /* use cached page pointer */
	      page = info->fixed_page;
	    }
	  else
	    {
	      /* free currently fixed page */
	      if (info->fixed_page != NULL)
		{
		  qmgr_free_old_page (info->fixed_page, list_idp->tfile_vfid);
		  info->fixed_page = NULL;
		}

	      /* fix page and cache fixed vpid */
	      page = qmgr_get_old_page (&vpid, list_idp->tfile_vfid);
	      if (page == NULL)
		{
		  goto exit_on_error;
		}

	      /* save page pointer */
	      info->fixed_vpid = vpid;
	      info->fixed_page = page;
	    }			/* else */
#else
	  page = qmgr_get_old_page (thread_p, &vpid, list_idp->tfile_vfid);
	  if (page == NULL)
	    {
	      goto exit_on_error;
	    }
#endif

	  QFILE_GET_OVERFLOW_VPID (&vpid, page);
	  data = page + key->s.original.offset;
	  if (vpid.pageid != NULL_PAGEID)
	    {
	      /*
	       * This sucks; why do we need two different structures to
	       * accomplish exactly the same goal?
	       */
	      dummy.size = info->gby_rec.area_size;
	      dummy.tpl = info->gby_rec.data;
	      status =
		qfile_get_tuple (thread_p, page, data, &dummy, list_idp);

	      if (dummy.tpl != info->gby_rec.data)
		{
		  /*
		   * DON'T FREE THE BUFFER!  qfile_get_tuple() already did
		   * that, and what you have here in gby_rec is a dangling
		   * pointer.
		   */
		  info->gby_rec.area_size = dummy.size;
		  info->gby_rec.data = dummy.tpl;
		}
	      if (status != NO_ERROR)
		{
		  goto exit_on_error;
		}

	      data = info->gby_rec.data;
	    }
	  else
	    {
	      peek = PEEK;	/* avoid unnecessary COPY */
	    }
	}
      else
	{			/* A_sort_key */
	  /*
	   * We didn't record the original vpid, and we should just
	   * reconstruct the original record from this sort key (rather
	   * than pressure the page buffer pool by reading in the original
	   * page to get the original tuple).
	   */
	  if (qfile_generate_sort_tuple (&info->key_info,
					 key, &info->gby_rec) == NULL)
	    {
	      goto exit_on_error;
	    }
	  data = info->gby_rec.data;
	}

      if (info->input_recs == 0)
	{
	  /*
	   * First record we've seen; put it out and set up the group
	   * comparison key(s).
	   */
	  qexec_gby_start_group (thread_p, info, recdes);

	  /* start all rollup groups */
	  if (info->g_rollup_agg_list)
	    {
	      for (i = 0; i < info->rollup_levels; i++)
		{
		  qexec_gby_start_rollup_group (thread_p, info, recdes, i);
		}
	    }

	  qexec_gby_agg_tuple (thread_p, info, data, peek);
	}
      else if ((*info->cmp_fn) (&info->current_key.data, &key,
				&info->key_info) == 0)
	{
	  /*
	   * Still in the same group; accumulate the tuple and proceed,
	   * leaving the group key the same.
	   */
	  qexec_gby_agg_tuple (thread_p, info, data, peek);
	}
      else
	{
	  /*
	   * We got a new group; finalize the group we were accumulating,
	   * and start a new group using the current key as the group key.
	   */
	  qexec_gby_finalize_group (thread_p, info);

	  if (info->state == SORT_PUT_STOP)
	    {
	      goto wrapup;
	    }

	  /* handle the rollup groups */
	  if (info->g_rollup_agg_list)
	    {
	      nkeys = info->key_info.nkeys;

	      /*
	       * find the first key that fails comparison;
	       * the rollup level will be key number
	       */
	      for (i = 1; i < nkeys; i++)
		{
		  info->key_info.nkeys = i;

		  if ((*info->cmp_fn) (&info->current_key.data, &key,
				       &info->key_info) != 0)
		    {
		      /* finalize rollup groups */
		      for (j = nkeys - 1; j >= i; j--)
			{
			  qexec_gby_finalize_rollup_group (thread_p, info, j);
			  qexec_gby_start_rollup_group (thread_p, info,
							recdes, j);
			}
		      break;
		    }
		}

	      info->key_info.nkeys = nkeys;
	    }

	  qexec_gby_start_group (thread_p, info, recdes);
	  qexec_gby_agg_tuple (thread_p, info, data, peek);
	}
      info->input_recs++;

#if 1				/* SortCache */
      if (page)
	{
	  qmgr_free_old_page (thread_p, page, list_idp->tfile_vfid);
	  page = NULL;
	}
#endif

    }				/* for (key = (SORT_REC *) recdes->data; ...) */

wrapup:
#if 1				/* SortCache */
  if (page)
    {
      qmgr_free_old_page (thread_p, page, list_idp->tfile_vfid);
    }
#endif

  return info->state;

exit_on_error:
  info->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_groupby () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   :
 *   xasl_state(in)     : XASL tree state information
 *   tplrec(out) : Tuple record descriptor to store result tuples
 *
 * Note: Apply the group_by clause to the given list file to group it
 * using the specified group_by parameters.
 */
static int
qexec_groupby (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
	       XASL_STATE * xasl_state, QFILE_TUPLE_RECORD * tplrec)
{
  QFILE_LIST_ID *list_id = xasl->list_id;
  BUILDLIST_PROC_NODE *buildlist = &xasl->proc.buildlist;
  GROUPBY_STATE gbstate;
  QFILE_LIST_SCAN_ID input_scan_id;
  int ls_flag = 0;

  if (buildlist->groupby_list == NULL)
    {
      return NO_ERROR;
    }

  /* initialize groupby_num() value */
  if (buildlist->g_grbynum_val && DB_IS_NULL (buildlist->g_grbynum_val))
    {
      DB_MAKE_INT (buildlist->g_grbynum_val, 0);
    }

  if (qexec_initialize_groupby_state (&gbstate, buildlist->groupby_list,
				      buildlist->g_having_pred,
				      buildlist->g_grbynum_pred,
				      buildlist->g_grbynum_val,
				      buildlist->g_grbynum_flag,
				      buildlist->eptr_list,
				      buildlist->g_agg_list,
				      buildlist->g_regu_list,
				      buildlist->g_val_list,
				      buildlist->g_outptr_list,
				      buildlist->g_with_rollup,
				      xasl_state,
				      &list_id->type_list, tplrec) == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /*
   * Create a new listfile to receive the results.
   */
  {
    QFILE_TUPLE_VALUE_TYPE_LIST output_type_list;
    QFILE_LIST_ID *output_list_id;

    if (qdata_get_valptr_type_list (buildlist->g_outptr_list,
				    &output_type_list) != NO_ERROR)
      {
	GOTO_EXIT_ON_ERROR;
      }
    /* If it does not have 'order by'(xasl->orderby_list),
       then the list file to be open at here will be the last one.
       Otherwise, the last list file will be open at
       qexec_orderby_distinct().
       (Note that only one that can have 'group by' is BUILDLIST_PROC type.)
       And, the top most XASL is the other condition for the list file
       to be the last result file. */

    QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);
    if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL)
	&& XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED)
	&& (xasl->orderby_list == NULL
	    || XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST))
	&& xasl->option != Q_DISTINCT)
      {
	QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
      }

    output_list_id = qfile_open_list (thread_p, &output_type_list,
				      buildlist->after_groupby_list,
				      xasl_state->query_id, ls_flag);
    if (output_list_id == NULL)
      {
	if (output_type_list.domp)
	  {
	    db_private_free_and_init (thread_p, output_type_list.domp);
	  }

	GOTO_EXIT_ON_ERROR;
      }

    if (output_type_list.domp)
      {
	db_private_free_and_init (thread_p, output_type_list.domp);
      }

    gbstate.output_file = output_list_id;
  }

  if (list_id->tuple_cnt == 0)
    {
      /* empty unsorted list file, no need to proceed */
      qfile_destroy_list (thread_p, list_id);
      qfile_close_list (thread_p, gbstate.output_file);
      qfile_copy_list_id (list_id, gbstate.output_file, true);
      qexec_clear_groupby_state (thread_p, &gbstate);	/* will free gbstate.output_file */

      return NO_ERROR;
    }

  /*
   * Open a scan on the unsorted input file
   */
  if (qfile_open_list_scan (list_id, &input_scan_id) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  gbstate.input_scan = &input_scan_id;

  /*
   * Now load up the sort module and set it off...
   */
  gbstate.key_info.use_original =
    (gbstate.key_info.nkeys != list_id->type_list.type_cnt);
  gbstate.cmp_fn = (gbstate.key_info.use_original == 1
		    ? &qfile_compare_partial_sort_record
		    : &qfile_compare_all_sort_record);

  if (sort_listfile (thread_p, NULL_VOLID,
		     qfile_get_estimated_pages_for_sorting (list_id,
							    &gbstate.
							    key_info),
		     &qexec_gby_get_next, &gbstate, &qexec_gby_put_next,
		     &gbstate, gbstate.cmp_fn, &gbstate.key_info,
		     SORT_DUP) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /*
   * There may be one unfinished group in the output, since the sort_listfile
   * interface doesn't include a finalization function.  If so, finish
   * off that group.
   */
  if (gbstate.input_recs != 0)
    {
      qexec_gby_finalize_group (thread_p, &gbstate);
      if (gbstate.g_rollup_agg_list)
	{
	  int i;
	  for (i = gbstate.rollup_levels - 1; i >= 0; i--)
	    {
	      qexec_gby_finalize_rollup_group (thread_p, &gbstate, i);
	    }
	}
    }

  qfile_close_list (thread_p, gbstate.output_file);
#if 0				/* SortCache */
  /* free currently fixed page */
  if (gbstate.fixed_page != NULL)
    {
      QFILE_LIST_ID *list_idp;

      list_idp = &(gbstate.input_scan->list_id);
      qmgr_free_old_page (gbstate.fixed_page, list_idp->tfile_vfid);
      gbstate.fixed_page = NULL;
    }
#endif
  qfile_destroy_list (thread_p, list_id);
  qfile_copy_list_id (list_id, gbstate.output_file, true);
  /* qexec_clear_groupby_state() will free gbstate.output_file */

wrapup:
  {
    int result;
    /* SORT_PUT_STOP set by 'qexec_gby_finalize_group()' isn't error */
    result = (gbstate.state == NO_ERROR || gbstate.state == SORT_PUT_STOP)
      ? NO_ERROR : ER_FAILED;
    qexec_clear_groupby_state (thread_p, &gbstate);
    return result;
  }

exit_on_error:

  gbstate.state = er_errid ();
  if (gbstate.state == NO_ERROR || gbstate.state == SORT_PUT_STOP)
    {
      gbstate.state = ER_FAILED;
    }

  goto wrapup;
}

/*
 * qexec_collection_has_null () -
 *   return: 1 iff collection has an element with a NULL in it
 *   colval(in) : DB_VALUE of a collection
 */
static int
qexec_collection_has_null (DB_VALUE * colval)
{
  DB_VALUE elem;
  DB_COLLECTION *col;
  long i;
  int result = 0;

  col = db_get_set (colval);
  for (i = 0; i < db_set_size (col) && !result; i++)
    {
      if (db_set_get (col, i, &elem) < NO_ERROR)
	{
	  return 1;
	  /* flag an error as a NULL, DON'T clear elem with unknown state */
	}
      if (PRIM_IS_NULL (&elem))
	{
	  return 1;		/* found a NULL, can stop looking, clear unecessary */
	}

      if (pr_is_set_type (PRIM_TYPE (&elem))
	  && qexec_collection_has_null (&elem))
	{
	  /* this is for nested set types, need to fall thru to clear */
	  result = 1;
	}

      pr_clear_value (&elem);
    }

  return 0;
}

/*
 * qexec_cmp_tpl_vals_merge () -
 *   return:
 *        DB_UNK: return error
 *   left_tval(in)      : left tuple values
 *   left_dom(in)       : Domains of left_tval
 *   rght_tval(in)      : right tuple values
 *   rght_dom(in)       : Domains of rght_tval
 *   tval_cnt(in)       : tuple values count
 *
 * Note: This routine checks if two tuple values are equal. Coercion
 * is done if necessary. This must give a totally
 * ordered result, but never return equal for
 * a NULL comparison. The result of this comparison
 * will tell the merge routine whether to advance the
 * left or right file to the next tuple. DB_UNK
 * cannot be returned because it is ambiguous as
 * to which side is collated later. If two
 * NULL's or two collections both containing NULL
 * are encountered, then it may pick either side
 * as the lessor side, that tuple will be discarded,
 * then the next comparison will discard the other side.
 */
static DB_VALUE_COMPARE_RESULT
qexec_cmp_tpl_vals_merge (QFILE_TUPLE * left_tval, TP_DOMAIN ** left_dom,
			  QFILE_TUPLE * rght_tval, TP_DOMAIN ** rght_dom,
			  int tval_cnt)
{
  OR_BUF buf;
  DB_VALUE left_dbval, rght_dbval;
  int i, cmp, left_len, rght_len;
  bool left_is_set, rght_is_set;

  cmp = DB_UNK;			/* init */

  for (i = 0; i < tval_cnt; i++)
    {
      /* get tpl values into db_values for the comparison
       */

      /* zero length means NULL */
      if ((left_len = QFILE_GET_TUPLE_VALUE_LENGTH (left_tval[i])) == 0)
	{
	  cmp = DB_LT;
	  break;
	}
      rght_len = QFILE_GET_TUPLE_VALUE_LENGTH (rght_tval[i]);
      if (rght_len == 0)
	{
	  cmp = DB_GT;
	  break;
	}

      or_init (&buf, (char *) (left_tval[i] + QFILE_TUPLE_VALUE_HEADER_SIZE),
	       left_len);
      /* Do not copy the string--just use the pointer.  The pr_ routines
       * for strings and sets have different semantics for length.
       */
      left_is_set = pr_is_set_type (left_dom[i]->type->id) ? true : false;
      if ((*(left_dom[i]->type->readval)) (&buf, &left_dbval, left_dom[i], -1,
					   left_is_set, NULL, 0) != NO_ERROR)
	{
	  cmp = DB_UNK;		/* is error */
	  break;
	}
      if (DB_IS_NULL (&left_dbval))
	{
	  cmp = DB_LT;
	  break;
	}

      or_init (&buf, (char *) (rght_tval[i] + QFILE_TUPLE_VALUE_HEADER_SIZE),
	       rght_len);
      /* Do not copy the string--just use the pointer.  The pr_ routines
       * for strings and sets have different semantics for length.
       */
      rght_is_set = pr_is_set_type (rght_dom[i]->type->id) ? true : false;
      if ((*(rght_dom[i]->type->readval)) (&buf, &rght_dbval, rght_dom[i], -1,
					   rght_is_set, NULL, 0) != NO_ERROR)
	{
	  cmp = DB_UNK;		/* is error */
	  break;
	}
      if (DB_IS_NULL (&rght_dbval))
	{
	  cmp = DB_GT;
	  break;
	}

      /* both left_dbval, rght_dbval is non-null */
      cmp = tp_value_compare (&left_dbval, &rght_dbval, 1, 0);

      if (left_is_set)
	{
	  if (cmp == DB_UNK && qexec_collection_has_null (&left_dbval))
	    {
	      cmp = DB_LT;
	    }
	  pr_clear_value (&left_dbval);
	}
      if (rght_is_set)
	{
	  if (cmp == DB_UNK && qexec_collection_has_null (&rght_dbval))
	    {
	      cmp = DB_GT;
	    }
	  pr_clear_value (&rght_dbval);
	}

      if (cmp == DB_EQ)
	{
	  continue;		/* step into the next tval */
	}

      if (cmp == DB_LT || cmp == DB_GT)
	{
	  ;			/* OK */
	}
      else
	{			/* is error */
	  cmp = DB_UNK;
	}

      /* at here, immediately return */
      break;
    }

  return (DB_VALUE_COMPARE_RESULT) cmp;
}

/*
 * qexec_size_remaining () -
 *   return: int
 *   tplrec1(in)        : First tuple descriptor
 *   tplrec2(in)        : Second tuple descriptor
 *   merge_info(in)     : Tuple merge information
 *   k(in)      : column to start at
 *
 * Note: This routine calculates the size needed to store the
 *  remaining tuple to copy.
 * If either tuple is a NULL pointer, assume that the space
 * for an UNBOUND (header) will be needed.
 */
static long
qexec_size_remaining (QFILE_TUPLE_RECORD * tplrec1,
		      QFILE_TUPLE_RECORD * tplrec2,
		      QFILE_LIST_MERGE_INFO * merge_info, int k)
{
  int i, tpl_size;
  char *t_valhp;

  tpl_size = 0;
  for (i = k; i < merge_info->ls_pos_cnt; i++)
    {
      tpl_size += QFILE_TUPLE_VALUE_HEADER_SIZE;
      if (merge_info->ls_outer_inner_list[i] == QFILE_OUTER_LIST)
	{
	  if (tplrec1)
	    {
	      QFILE_GET_TUPLE_VALUE_HEADER_POSITION (tplrec1->tpl,
						     merge_info->
						     ls_pos_list[i], t_valhp);
	      tpl_size += QFILE_GET_TUPLE_VALUE_LENGTH (t_valhp);
	    }
	}
      else
	{
	  if (tplrec2)
	    {
	      QFILE_GET_TUPLE_VALUE_HEADER_POSITION (tplrec2->tpl,
						     merge_info->
						     ls_pos_list[i], t_valhp);
	      tpl_size += QFILE_GET_TUPLE_VALUE_LENGTH (t_valhp);
	    }
	}
    }

  return tpl_size;
}

/*
 * qexec_merge_tuple () -
 *   return: NO_ERROR, or ER_code
 *   tplrec1(in)        : First tuple descriptor
 *   tplrec2(in)        : Second tuple descriptor
 *   merge_info(in)     : Tuple merge information
 *   tplrec(in) : Result tuple descriptor
 *
 * Note: This routine merges the given two list files tuples using
 * the given list merge information and stores the result into
 * result tuple descriptor.
 */
static int
qexec_merge_tuple (QFILE_TUPLE_RECORD * tplrec1, QFILE_TUPLE_RECORD * tplrec2,
		   QFILE_LIST_MERGE_INFO * merge_info,
		   QFILE_TUPLE_RECORD * tplrec)
{
  QFILE_TUPLE tplp;
  char *t_valhp;
  int t_val_size;
  int tpl_size, offset;
  int k;
  INT32 ls_unbound[2] = { 0, 0 };

  /* merge two tuples, and form a new tuple */
  tplp = tplrec->tpl;
  offset = 0;
  QFILE_PUT_TUPLE_LENGTH (tplp, QFILE_TUPLE_LENGTH_SIZE);	/* set tuple length */
  tplp += QFILE_TUPLE_LENGTH_SIZE;
  offset += QFILE_TUPLE_LENGTH_SIZE;

  QFILE_PUT_TUPLE_VALUE_FLAG ((char *) ls_unbound, V_UNBOUND);
  QFILE_PUT_TUPLE_VALUE_LENGTH ((char *) ls_unbound, 0);

  /* copy tuple values from the first and second list file tuples */
  for (k = 0; k < merge_info->ls_pos_cnt; k++)
    {

      if (merge_info->ls_outer_inner_list[k] == QFILE_OUTER_LIST)
	{
	  if (tplrec1)
	    {
	      QFILE_GET_TUPLE_VALUE_HEADER_POSITION (tplrec1->tpl,
						     merge_info->
						     ls_pos_list[k], t_valhp);
	    }
	  else
	    {
	      t_valhp = (char *) ls_unbound;
	    }
	}
      else
	{			/* copy from the second tuple */
	  if (tplrec2)
	    {
	      QFILE_GET_TUPLE_VALUE_HEADER_POSITION (tplrec2->tpl,
						     merge_info->
						     ls_pos_list[k], t_valhp);
	    }
	  else
	    {
	      t_valhp = (char *) ls_unbound;
	    }
	}

      t_val_size =
	QFILE_TUPLE_VALUE_HEADER_SIZE +
	QFILE_GET_TUPLE_VALUE_LENGTH (t_valhp);
      if ((tplrec->size - offset) < t_val_size)
	{			/* no space left */
	  tpl_size = offset +
	    qexec_size_remaining (tplrec1, tplrec2, merge_info, k);
	  if (qfile_reallocate_tuple (tplrec, tpl_size) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	  tplp = (QFILE_TUPLE) tplrec->tpl + offset;
	}

      memcpy (tplp, t_valhp, t_val_size);
      tplp += t_val_size;
      offset += t_val_size;
    }				/* for */

  /* set tuple length */
  QFILE_PUT_TUPLE_LENGTH (tplrec->tpl, offset);

  return NO_ERROR;
}

/*
 * qexec_merge_tuple_add_list () - Merge a tuple, and add it to the list file
 *   return: NO_ERROR, or ER_code
 *   list_id(in)        : List file to insert into
 *   tplrec1(in)        : First tuple descriptor
 *   tplrec2(in)        : Second tuple descriptor
 *   merge_info(in)     : Tuple merge information
 *   tplrec(in) : Result tuple descriptor
 */
static int
qexec_merge_tuple_add_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id,
			    QFILE_TUPLE_RECORD * tplrec1,
			    QFILE_TUPLE_RECORD * tplrec2,
			    QFILE_LIST_MERGE_INFO * merge_info,
			    QFILE_TUPLE_RECORD * tplrec)
{
  int ret;
  QFILE_TUPLE_DESCRIPTOR *tdp;
  int tplrec1_max_size;
  int tplrec2_max_size;

  /* get tuple descriptor */
  tdp = &(list_id->tpl_descr);

  if (tplrec1)
    {
      tplrec1_max_size = QFILE_GET_TUPLE_LENGTH (tplrec1->tpl);
    }
  else
    {
      tplrec1_max_size =
	QFILE_TUPLE_VALUE_HEADER_SIZE * (merge_info->ls_pos_cnt);
    }

  if (tplrec2)
    {
      tplrec2_max_size = QFILE_GET_TUPLE_LENGTH (tplrec2->tpl);
    }
  else
    {
      tplrec2_max_size =
	QFILE_TUPLE_VALUE_HEADER_SIZE * (merge_info->ls_pos_cnt);
    }

  tdp->tpl_size =
    DB_ALIGN (tplrec1_max_size + tplrec2_max_size, MAX_ALIGNMENT);

  if (tdp->tpl_size < QFILE_MAX_TUPLE_SIZE_IN_PAGE)
    {				/* SMALL QFILE_TUPLE */
      /* set tuple descriptor */
      tdp->tplrec1 = tplrec1;
      tdp->tplrec2 = tplrec2;
      tdp->merge_info = merge_info;

      /* build merged tuple into the list file page */
      ret = qfile_generate_tuple_into_list (thread_p, list_id, T_MERGE);
    }
  else
    {				/* BIG QFILE_TUPLE */
      /* merge two tuples, and form a new tuple */
      ret = qexec_merge_tuple (tplrec1, tplrec2, merge_info, tplrec);
      if (ret != NO_ERROR)
	{
	  return ret;
	}

      /* add merged tuple to the resultant list file */
      ret = qfile_add_tuple_to_list (thread_p, list_id, tplrec->tpl);
    }

  return ret;
}

/* pre-defined vars:    list_idp,
 *                      merge_infop,
 *                      nvals
 *                      tplrec
 *                      indp
 *                      valp
 *                      scan
 *                      sid
 */
/****************************** COMMON MACRO ********************************/
#define QEXEC_MERGE_ADD_MERGETUPLE(thread_p, t1, t2)                         \
    do {                                                                     \
        if (qexec_merge_tuple_add_list((thread_p), list_idp, (t1), (t2),     \
                              merge_infop, &tplrec) != NO_ERROR) {           \
            goto exit_on_error;                                              \
        }                                                                    \
    } while (0)

#define QEXEC_MERGE_PVALS(pre)                                               \
    do {                                                                     \
        int _v;                                                              \
        for (_v = 0; _v < nvals; _v++) {                                     \
            do {                                                             \
                QFILE_GET_TUPLE_VALUE_HEADER_POSITION((pre##_tplrec).tpl,    \
                                        (pre##_indp)[_v],                    \
                                        (pre##_valp)[_v]);                   \
            } while (0);                                                     \
        }                                                                    \
    } while (0)

/**************************** INNER MERGE MACRO *****************************/
#define QEXEC_MERGE_NEXT_SCAN(thread_p, pre, e)                              \
    do {                                                                     \
        pre##_scan = qfile_scan_list_next((thread_p), &(pre##_sid), &(pre##_tplrec), PEEK); \
        if ((e) && pre##_scan == S_END)                                      \
            goto exit_on_end;                                                \
        if (pre##_scan == S_ERROR)                                           \
            goto exit_on_error;                                              \
    } while (0)

#define QEXEC_MERGE_PREV_SCAN(thread_p, pre)                                 \
    do {                                                                     \
        pre##_scan = qfile_scan_list_prev((thread_p), &(pre##_sid), &(pre##_tplrec), PEEK); \
        if (pre##_scan == S_ERROR)                                           \
            goto exit_on_error;                                              \
    } while (0)

#define QEXEC_MERGE_NEXT_SCAN_PVALS(thread_p, pre, e)                        \
    do {                                                                     \
        QEXEC_MERGE_NEXT_SCAN((thread_p), pre, e);                           \
        if (pre##_scan == S_SUCCESS) {                                       \
            QEXEC_MERGE_PVALS(pre);                                          \
        }                                                                    \
    } while (0)

#define QEXEC_MERGE_REV_SCAN_PVALS(thread_p, pre)                            \
    do {                                                                     \
        QEXEC_MERGE_PREV_SCAN((thread_p), pre);                                          \
        if (pre##_scan == S_SUCCESS) {                                       \
            QEXEC_MERGE_PVALS(pre);                                          \
        }                                                                    \
    } while (0)

/*  rename scan_next_scan to scan_next_scan_proxy */
/**************************** OUTER MERGE MACRO *****************************/
#define QEXEC_MERGE_OUTER_NEXT_SCAN(thread_p, pre, e)                        \
    do {                                                                     \
        pre##_sid->qualification = QPROC_QUALIFIED_OR_NOT; /* init */        \
        pre##_scan = scan_next_scan((thread_p), pre##_sid);                  \
        if ((e) && pre##_scan == S_END)                                      \
            goto exit_on_end;                                                \
        if (pre##_scan == S_ERROR)                                           \
            goto exit_on_error;                                              \
    } while (0)

#define QEXEC_MERGE_OUTER_PREV_SCAN(thread_p, pre)                           \
    do {                                                                     \
        pre##_sid->qualification = QPROC_QUALIFIED_OR_NOT; /* init */        \
        pre##_scan = scan_prev_scan((thread_p), pre##_sid);                  \
        if (pre##_scan == S_ERROR)                                           \
            goto exit_on_error;                                              \
    } while (0)

#define QEXEC_MERGE_OUTER_NEXT_SCAN_PVALS(thread_p, pre, e)                  \
    do {                                                                     \
        QEXEC_MERGE_OUTER_NEXT_SCAN((thread_p), pre, e);                     \
        if (pre##_scan == S_SUCCESS) {                                       \
            QEXEC_MERGE_PVALS(pre);                                          \
        }                                                                    \
    } while (0)

#define QEXEC_MERGE_OUTER_PREV_SCAN_PVALS(thread_p, pre)                     \
    do {                                                                     \
        QEXEC_MERGE_OUTER_PREV_SCAN((thread_p), pre);                        \
        if (pre##_scan == S_SUCCESS) {                                       \
            QEXEC_MERGE_PVALS(pre);                                          \
        }                                                                    \
    } while (0)

/*
 * qexec_merge_list () -
 *   return: QFILE_LIST_ID *, or NULL
 *   outer_list_idp(in) : First (left) list file to be merged
 *   inner_list_idp(in) : Second (right) list file to be merged
 *   merge_infop(in)    : List file merge information
 *   ls_flag(in)        :
 *
 * Note: This routine merges the given two sorted list files using
 * the given list file merge information and returns the result
 * list file identifier.
 *
 * Note: The routine assumes that the join column data types for both list
 * files are same.
 */
static QFILE_LIST_ID *
qexec_merge_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * outer_list_idp,
		  QFILE_LIST_ID * inner_list_idp,
		  QFILE_LIST_MERGE_INFO * merge_infop, int ls_flag)
{
  /* outer -> left scan, inner -> right scan */

  /* pred-efined vars */
  QFILE_LIST_ID *list_idp = NULL;
  int nvals;
  QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
  QFILE_TUPLE_RECORD outer_tplrec = { NULL, 0 };
  QFILE_TUPLE_RECORD inner_tplrec = { NULL, 0 };
  int *outer_indp, *inner_indp;
  char **outer_valp = NULL, **inner_valp = NULL;
  SCAN_CODE outer_scan = S_END, inner_scan = S_END;
  QFILE_LIST_SCAN_ID outer_sid, inner_sid;

  TP_DOMAIN **outer_domp = NULL, **inner_domp = NULL;
  QFILE_TUPLE_VALUE_TYPE_LIST type_list;
  int k, cnt, group_cnt, already_compared;
  SCAN_DIRECTION direction;
  QFILE_TUPLE_POSITION inner_tplpos;
  DB_VALUE_COMPARE_RESULT val_cmp;

  /* get merge columns count */
  nvals = merge_infop->ls_column_cnt;

  /* get indicator of merge columns */
  outer_indp = merge_infop->ls_outer_column;
  inner_indp = merge_infop->ls_inner_column;

  /* form the typelist for the resultant list file */
  type_list.type_cnt = merge_infop->ls_pos_cnt;
  type_list.domp = (TP_DOMAIN **) malloc (type_list.type_cnt *
					  sizeof (TP_DOMAIN *));
  if (type_list.domp == NULL)
    {
      goto exit_on_error;
    }

  for (k = 0; k < type_list.type_cnt; k++)
    {
      type_list.domp[k] =
	(merge_infop->ls_outer_inner_list[k] == QFILE_OUTER_LIST)
	? outer_list_idp->type_list.domp[merge_infop->ls_pos_list[k]]
	: inner_list_idp->type_list.domp[merge_infop->ls_pos_list[k]];
    }

  outer_sid.status = S_CLOSED;
  inner_sid.status = S_CLOSED;

  /* open a scan on the outer(inner) list file */
  if (qfile_open_list_scan (outer_list_idp, &outer_sid) != NO_ERROR
      || qfile_open_list_scan (inner_list_idp, &inner_sid) != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* open the result list file; same query id with outer(inner) list file */
  list_idp = qfile_open_list (thread_p, &type_list, NULL,
			      outer_list_idp->query_id, ls_flag);
  if (list_idp == NULL)
    {
      goto exit_on_error;
    }

  if (outer_list_idp->tuple_cnt == 0 || inner_list_idp->tuple_cnt == 0)
    {
      goto exit_on_end;
    }

  /* allocate the area to store the merged tuple */
  if (qfile_reallocate_tuple (&tplrec, DB_PAGESIZE) != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* merge column domain info */
  outer_domp = (TP_DOMAIN **) db_private_alloc (thread_p,
						nvals * sizeof (TP_DOMAIN *));
  if (outer_domp == NULL)
    {
      goto exit_on_error;
    }

  inner_domp = (TP_DOMAIN **) db_private_alloc (thread_p,
						nvals * sizeof (TP_DOMAIN *));
  if (inner_domp == NULL)
    {
      goto exit_on_error;
    }

  for (k = 0; k < nvals; k++)
    {
      outer_domp[k] =
	outer_list_idp->type_list.domp[merge_infop->ls_outer_column[k]];
      inner_domp[k] =
	inner_list_idp->type_list.domp[merge_infop->ls_inner_column[k]];
    }

  /* merge column val pointer */
  outer_valp = (char **) db_private_alloc (thread_p, nvals * sizeof (char *));
  if (outer_valp == NULL)
    {
      goto exit_on_error;
    }

  inner_valp = (char **) db_private_alloc (thread_p, nvals * sizeof (char *));
  if (inner_valp == NULL)
    {
      goto exit_on_error;
    }

  /* When a list file is sorted on a column, all the NULL values appear at
     the beginning of the list. So, we know that all the following values
     in the inner/outer column are BOUND(not NULL) values.
     Depending on the join type, we must skip or join with a NULL opposite
     row, when a NULL is encountered. */

  /* move the outer(left) scan to the first tuple */
  while (1)
    {
      /* move to the next outer tuple and
         position tuple values (to merge columns) */
      QEXEC_MERGE_NEXT_SCAN_PVALS (thread_p, outer, true);

      for (k = 0; k < nvals; k++)
	{
	  if (QFILE_GET_TUPLE_VALUE_FLAG (outer_valp[k]) == V_UNBOUND)
	    {
	      break;
	    }
	}
      if (k >= nvals)
	{			/* not found V_UNBOUND. exit loop */
	  break;
	}
    }

  /* move the inner(right) scan to the first tuple */
  while (1)
    {
      /* move to the next inner tuple and
         position tuple values (to merge columns) */
      QEXEC_MERGE_NEXT_SCAN_PVALS (thread_p, inner, true);

      for (k = 0; k < nvals; k++)
	{
	  if (QFILE_GET_TUPLE_VALUE_FLAG (inner_valp[k]) == V_UNBOUND)
	    {
	      break;
	    }
	}
      if (k >= nvals)
	{			/* not found V_UNBOUND. exit loop */
	  break;
	}

    }

  /* set the comparison function to be called */
  direction = S_FORWARD;
  group_cnt = 0;
  already_compared = false;
  val_cmp = DB_UNK;

  while (1)
    {
      /* compare two tuple values, if they have not been compared yet */
      if (!already_compared)
	{
	  val_cmp = qexec_cmp_tpl_vals_merge (outer_valp, outer_domp,
					      inner_valp, inner_domp, nvals);
	  if (val_cmp == DB_UNK)
	    {			/* is error */
	      goto exit_on_error;
	    }
	}
      already_compared = false;	/* re-init */

      /* value of the outer is less than value of the inner */
      if (val_cmp == DB_LT)
	{
	  /* move the outer(left) scan to the next tuple and
	     position tuple values (to merge columns) */
	  QEXEC_MERGE_NEXT_SCAN_PVALS (thread_p, outer, true);
	  direction = S_FORWARD;
	  group_cnt = 0;

	  continue;
	}

      /* value of the outer is greater than value of the inner */
      if (val_cmp == DB_GT)
	{
	  /* move the inner(right) scan to the next tuple and
	     position tuple values (to merge columns) */
	  QEXEC_MERGE_NEXT_SCAN_PVALS (thread_p, inner, true);
	  direction = S_FORWARD;
	  group_cnt = 0;

	  continue;
	}

      if (val_cmp != DB_EQ)
	{			/* error ? */
	  goto exit_on_error;
	}

      /* values of the outer and inner are equal, do a scan group processing
       */
      if (direction == S_FORWARD)
	{
	  /* move forwards within a group
	   */
	  cnt = 0;
	  /* group_cnt has the number of tuples in the group */
	  while (1)
	    {
	      /* merge the fetched tuples(left and right) */
	      QEXEC_MERGE_ADD_MERGETUPLE (thread_p, &outer_tplrec,
					  &inner_tplrec);

	      cnt++;		/* increase the counter of processed tuples */

	      /* if the group is formed for the first time */
	      if (group_cnt == 0)
		{
		  /* move the inner(right) scan to the next tuple and
		     position tuple values (to merge columns) */
		  QEXEC_MERGE_NEXT_SCAN_PVALS (thread_p, inner,
					       false /* do not exit */ );
		  if (inner_scan == S_END)
		    {
		      break;
		    }

		  /* and compare */
		  val_cmp = qexec_cmp_tpl_vals_merge (outer_valp,
						      outer_domp,
						      inner_valp,
						      inner_domp, nvals);
		  if (val_cmp != DB_EQ)
		    {
		      if (val_cmp == DB_UNK)
			{	/* is error */
			  goto exit_on_error;
			}

		      break;	/* found the bottom of the group */
		    }
		}
	      else
		{		/* group_cnt > 0 */
		  if (cnt >= group_cnt)
		    {
		      break;	/* reached the bottom of the group */
		    }

		  /* move the inner(right) scan to the next tuple */
		  QEXEC_MERGE_NEXT_SCAN (thread_p, inner, true);
		}
	    }			/* while (1) */

	  /* move the outer to the next tuple and position tuple values */
	  QEXEC_MERGE_NEXT_SCAN_PVALS (thread_p, outer, true);

	  /* if the group is formed for the first time */
	  if (group_cnt == 0)
	    {
	      /* save the position of inner scan;
	         it is the bottom of the group */
	      qfile_save_current_scan_tuple_position (&inner_sid,
						      &inner_tplpos);

	      if (inner_scan == S_END)
		{
		  /* move the inner to the previous tuple and
		     position tuple values */
		  QEXEC_MERGE_REV_SCAN_PVALS (thread_p, inner);

		  /* set group count and direction */
		  group_cnt = cnt;
		  direction = S_BACKWARD;
		}
	      else
		{
		  /* and compare */
		  val_cmp = qexec_cmp_tpl_vals_merge (outer_valp, outer_domp,
						      inner_valp, inner_domp,
						      nvals);
		  if (val_cmp == DB_UNK)
		    {		/* is error */
		      goto exit_on_error;
		    }

		  if (val_cmp == DB_LT)
		    {
		      /* move the inner to the previous tuple and
		         position tuple values */
		      QEXEC_MERGE_REV_SCAN_PVALS (thread_p, inner);

		      /* and compare */
		      val_cmp =
			qexec_cmp_tpl_vals_merge (outer_valp, outer_domp,
						  inner_valp, inner_domp,
						  nvals);
		      if (val_cmp == DB_UNK)
			{	/* is error */
			  goto exit_on_error;
			}

		      if (val_cmp == DB_EQ)
			{
			  /* next value is the same, so prepare for further
			     group scan operations */

			  /* set group count and direction */
			  group_cnt = cnt;
			  direction = S_BACKWARD;
			}
		      else
			{
			  /* move the inner to the current tuple and
			     position tuple values */
			  QEXEC_MERGE_NEXT_SCAN_PVALS (thread_p, inner, true);

			  val_cmp = DB_LT;	/* restore comparison */
			}
		    }

		  /* comparison has already been done */
		  already_compared = true;
		}
	    }
	  else
	    {
	      /* set further scan direction */
	      direction = S_BACKWARD;
	    }
	}
      else
	{			/* (direction == S_BACKWARD) */
	  /* move backwards within a group
	   */
	  cnt = group_cnt;
	  /* group_cnt has the number of tuples in the group */
	  while (1)
	    {
	      /* merge the fetched tuples(left and right) */
	      QEXEC_MERGE_ADD_MERGETUPLE (thread_p, &outer_tplrec,
					  &inner_tplrec);

	      cnt--;		/* decrease the counter of the processed tuples */

	      if (cnt <= 0)
		{
		  break;	/* finish the group */
		}

	      /* if not yet reached the top of the group */
	      /* move the inner(right) scan to the previous tuple */
	      QEXEC_MERGE_PREV_SCAN (thread_p, inner);

	      /* all of the inner tuples in the group have the same
	         value at the merge column, so we don't need to
	         compare with the value of the outer one; just count
	         the number of the tuples in the group */
	    }			/* while (1) */

	  /* position tuple values (to merge columns) */
	  QEXEC_MERGE_PVALS (inner);

	  /* move the outer(left) scan to the next tuple and
	     position tuple values */
	  QEXEC_MERGE_NEXT_SCAN_PVALS (thread_p, outer, true);

	  /* and compare */
	  val_cmp = qexec_cmp_tpl_vals_merge (outer_valp, outer_domp,
					      inner_valp, inner_domp, nvals);
	  if (val_cmp == DB_UNK)
	    {			/* is error */
	      goto exit_on_error;
	    }

	  if (val_cmp != DB_EQ)
	    {
	      /* jump to the previously set scan position */
	      inner_scan =
		qfile_jump_scan_tuple_position (thread_p, &inner_sid,
						&inner_tplpos, &inner_tplrec,
						PEEK);
	      /* is saved position the end of scan? */
	      if (inner_scan == S_END)
		{
		  goto exit_on_end;
		}

	      /* and position tuple values */
	      QEXEC_MERGE_PVALS (inner);

	      /* reset group count */
	      group_cnt = 0;
	    }
	  else
	    {
	      /* comparison has already been done */
	      already_compared = true;
	    }

	  /* set further scan direction */
	  direction = S_FORWARD;

	}			/* (direction == S_BACKWARD) */

    }				/* while (1) */

exit_on_end:
  free_and_init (type_list.domp);
  qfile_close_scan (thread_p, &outer_sid);
  qfile_close_scan (thread_p, &inner_sid);

  if (tplrec.tpl)
    {
      db_private_free_and_init (thread_p, tplrec.tpl);
    }

  if (outer_domp)
    {
      db_private_free_and_init (thread_p, outer_domp);
    }
  if (outer_valp)
    {
      db_private_free_and_init (thread_p, outer_valp);
    }

  if (inner_domp)
    {
      db_private_free_and_init (thread_p, inner_domp);
    }
  if (inner_valp)
    {
      db_private_free_and_init (thread_p, inner_valp);
    }

  if (list_idp)
    {
      qfile_close_list (thread_p, list_idp);
    }

  return list_idp;

exit_on_error:
  if (list_idp)
    {
      qfile_close_list (thread_p, list_idp);
      qfile_free_list_id (list_idp);
    }

  list_idp = NULL;
  goto exit_on_end;
}

/*
 * qexec_merge_list_outer () -
 *   return:
 *   xasl(in)           :
 *   merge_infop(in)    :
 *   other_outer_join_pred(in)  :
 *   xasl_state(in)     :
 *   ls_flag(in)        :
 */
static QFILE_LIST_ID *
qexec_merge_list_outer (THREAD_ENTRY * thread_p, SCAN_ID * outer_sid,
			SCAN_ID * inner_sid,
			QFILE_LIST_MERGE_INFO * merge_infop,
			PRED_EXPR * other_outer_join_pred,
			XASL_STATE * xasl_state, int ls_flag)
{
  /* outer -> left scan, inner -> right scan */

  /* pre-defined vars: */
  QFILE_LIST_ID *list_idp = NULL;
  int nvals;
  QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
  QFILE_TUPLE_RECORD outer_tplrec = { NULL, 0 };
  QFILE_TUPLE_RECORD inner_tplrec = { NULL, 0 };
  int *outer_indp, *inner_indp;
  char **outer_valp = NULL, **inner_valp = NULL;
  SCAN_CODE outer_scan = S_END, inner_scan = S_END;

  TP_DOMAIN **outer_domp = NULL, **inner_domp = NULL;
  QFILE_LIST_ID *outer_list_idp, *inner_list_idp;
  QFILE_TUPLE_VALUE_TYPE_LIST type_list;
  int k, cnt, group_cnt, merge_cnt, already_compared;
  SCAN_DIRECTION direction;
  SCAN_POS inner_scanpos;
  bool all_lefts, all_rghts;
  DB_LOGICAL ev_res;
  DB_VALUE_COMPARE_RESULT val_cmp;

  direction = S_FORWARD;	/* init */
  val_cmp = DB_NE;		/* init - mark as not yet compared */

  /* determine all lefts or all rights option depending on join type */
  all_lefts = (merge_infop->join_type == JOIN_LEFT ||
	       merge_infop->join_type == JOIN_OUTER) ? true : false;
  all_rghts = (merge_infop->join_type == JOIN_RIGHT ||
	       merge_infop->join_type == JOIN_OUTER) ? true : false;

  /* QFILE_LIST_ID pointer from SCAN_ID */
  outer_list_idp = outer_sid->s.llsid.list_id;
  inner_list_idp = inner_sid->s.llsid.list_id;

  /* get merge columns count */
  nvals = merge_infop->ls_column_cnt;

  /* get indicator of merge columns */
  outer_indp = merge_infop->ls_outer_column;
  inner_indp = merge_infop->ls_inner_column;

  /* form the typelist for the resultant list file */
  type_list.type_cnt = merge_infop->ls_pos_cnt;
  type_list.domp = (TP_DOMAIN **) malloc (type_list.type_cnt *
					  sizeof (TP_DOMAIN *));
  if (type_list.domp == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  for (k = 0; k < type_list.type_cnt; k++)
    {
      type_list.domp[k] =
	(merge_infop->ls_outer_inner_list[k] == QFILE_OUTER_LIST)
	? outer_list_idp->type_list.domp[merge_infop->ls_pos_list[k]]
	: inner_list_idp->type_list.domp[merge_infop->ls_pos_list[k]];
    }

  /* open the result list file; same query id with outer(inner) list file */
  list_idp =
    qfile_open_list (thread_p, &type_list, NULL, outer_list_idp->query_id,
		     ls_flag);
  if (list_idp == NULL)
    {
      goto exit_on_error;
    }

  if (all_lefts && outer_list_idp->tuple_cnt == 0)
    {
      all_lefts = false;
      goto exit_on_end;
    }
  if (all_rghts && inner_list_idp->tuple_cnt == 0)
    {
      all_rghts = false;
      goto exit_on_end;
    }

  /* allocate the area to store the merged tuple */
  if (qfile_reallocate_tuple (&tplrec, DB_PAGESIZE) != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* merge column domain info */
  outer_domp = (TP_DOMAIN **) db_private_alloc (thread_p,
						nvals * sizeof (TP_DOMAIN *));
  if (outer_domp == NULL)
    {
      goto exit_on_error;
    }

  inner_domp = (TP_DOMAIN **) db_private_alloc (thread_p,
						nvals * sizeof (TP_DOMAIN *));
  if (inner_domp == NULL)
    {
      goto exit_on_error;
    }

  for (k = 0; k < nvals; k++)
    {
      outer_domp[k] =
	outer_list_idp->type_list.domp[merge_infop->ls_outer_column[k]];
      inner_domp[k] =
	inner_list_idp->type_list.domp[merge_infop->ls_inner_column[k]];
    }

  /* merge column val pointer */
  outer_valp = (char **) db_private_alloc (thread_p, nvals * sizeof (char *));
  if (outer_valp == NULL)
    {
      goto exit_on_error;
    }

  inner_valp = (char **) db_private_alloc (thread_p, nvals * sizeof (char *));
  if (inner_valp == NULL)
    {
      goto exit_on_error;
    }

  /* start scans */
  if (scan_start_scan (thread_p, outer_sid) != NO_ERROR
      || scan_start_scan (thread_p, inner_sid) != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* set tuple record pointer in QFILE_LIST_SCAN_ID */
  outer_sid->s.llsid.tplrecp = &outer_tplrec;
  inner_sid->s.llsid.tplrecp = &inner_tplrec;


  /* When a list file is sorted on a column, all the NULL value appear at
     the beginning of the list. So, we know that all the following values
     in the outer/inner column are BOUND(not NULL) value. And we can process
     all NULL values before the merging process. */

  /* move the outer(left) scan to the first tuple */
  while (1)
    {
      /* fetch a next tuple from outer(left) list file and
         position tuple values (to merge columns) */
      QEXEC_MERGE_OUTER_NEXT_SCAN_PVALS (thread_p, outer, true);

      if (outer_sid->qualification == QPROC_QUALIFIED)
	{
	  for (k = 0; k < nvals; k++)
	    {
	      if (QFILE_GET_TUPLE_VALUE_FLAG (outer_valp[k]) == V_UNBOUND)
		{
		  break;
		}
	    }
	  if (k >= nvals)
	    {			/* not found V_UNBOUND */
	      break;		/* found valid tuple. exit loop */
	    }
	}

      /* depending on the join type, join with a NULL opposite row
         when a NULL or a not-qualified is encountered, or skip it. */
      if (all_lefts)
	{
	  QEXEC_MERGE_ADD_MERGETUPLE (thread_p, &outer_tplrec, NULL);
	}
    }

  /* move the inner(right) scan to the first tuple */
  while (1)
    {
      /* move the inner(right) scan to the first tuple and
         position tuple values (to merge columns) */
      QEXEC_MERGE_OUTER_NEXT_SCAN_PVALS (thread_p, inner, true);

      if (inner_sid->qualification == QPROC_QUALIFIED)
	{
	  for (k = 0; k < nvals; k++)
	    {
	      if (QFILE_GET_TUPLE_VALUE_FLAG (inner_valp[k]) == V_UNBOUND)
		{
		  break;
		}
	    }
	  if (k >= nvals)
	    {			/* not found V_UNBOUND */
	      break;		/* found valid tuple. exit loop */
	    }
	}

      /* depending on the join type, join with a NULL opposite row
         when a NULL is encountered, or skip it. */
      if (all_rghts)
	{
	  QEXEC_MERGE_ADD_MERGETUPLE (thread_p, NULL, &inner_tplrec);
	}
    }

  /* set the comparison function to be called */
  direction = S_FORWARD;
  group_cnt = 0;
  already_compared = false;
  val_cmp = DB_UNK;

  while (1)
    {
      /* compare two tuple values, if they have not been compared yet */
      if (!already_compared)
	{
	  val_cmp = qexec_cmp_tpl_vals_merge (outer_valp, outer_domp,
					      inner_valp, inner_domp, nvals);
	  if (val_cmp == DB_UNK)
	    {			/* is error */
	      goto exit_on_error;
	    }
	}
      already_compared = false;	/* re-init */

      /* value of the outer is less than value of the inner */
      if (val_cmp == DB_LT)
	{
	  /* depending on the join type, join with a NULL opposite row
	     when it does not match */
	  if (all_lefts)
	    {
	      /* merge the fetched tuple(left) and NULL tuple(right) */
	      QEXEC_MERGE_ADD_MERGETUPLE (thread_p, &outer_tplrec, NULL);
	    }

	  /* move the outer(left) scan to the next tuple and
	     position tuple values (to merge columns) */
	  QEXEC_MERGE_OUTER_NEXT_SCAN_PVALS (thread_p, outer, true);
	  direction = S_FORWARD;
	  group_cnt = 0;

	  continue;
	}

      /* value of the outer is greater than value of the inner */
      if (val_cmp == DB_GT)
	{
	  /* depending on the join type, join with a NULL opposite row
	     when a NULL is encountered, or skip it. */
	  if (all_rghts)
	    {
	      /* merge the fetched tuple(right) and NULL tuple(left) */
	      QEXEC_MERGE_ADD_MERGETUPLE (thread_p, NULL, &inner_tplrec);
	    }

	  /* move the inner(right) scan to the next tuple and
	     position tuple values (to merge columns) */
	  QEXEC_MERGE_OUTER_NEXT_SCAN_PVALS (thread_p, inner, true);
	  direction = S_FORWARD;
	  group_cnt = 0;

	  continue;
	}

      if (val_cmp != DB_EQ)
	{			/* error ? */
	  goto exit_on_error;
	}

      /* values of the outer and inner are equal, do a scan group processing
       */
      if (direction == S_FORWARD)
	{
	  /* move forwards within a group
	   */
	  merge_cnt = cnt = 0;
	  /* group_cnt has the number of tuples in the group */
	  while (1)
	    {
	      /* evaluate other outer join predicate */
	      ev_res = V_UNKNOWN;
	      if (other_outer_join_pred != NULL)
		{
		  ev_res = eval_pred (thread_p, other_outer_join_pred,
				      &xasl_state->vd, NULL);
		  if (ev_res == V_ERROR)
		    {
		      goto exit_on_error;
		    }
		}

	      /* is qualified */
	      if (other_outer_join_pred == NULL || ev_res == V_TRUE)
		{
		  /* merge the fetched tuples(left and right) */
		  QEXEC_MERGE_ADD_MERGETUPLE (thread_p, &outer_tplrec,
					      &inner_tplrec);

		  merge_cnt++;	/* increase the counter of merged tuples */

		  /* if scan works in a single_fetch mode and first
		   * qualified scan item has now been fetched,
		   * return immediately.
		   */
		  if (merge_infop->single_fetch == QPROC_SINGLE_OUTER)
		    {
		      goto exit_on_end;
		    }
		}

	      cnt++;		/* increase the counter of processed tuples */

	      /* if the group is formed for the first time */
	      if (group_cnt == 0)
		{
		  /* not qualified */
		  if (!(other_outer_join_pred == NULL || ev_res == V_TRUE))
		    {
		      /* depending on the join type, join with a NULL
		         opposite row when a NULL or a not-qualified is
		         encountered, or skip it. */
		      if (all_rghts)
			{
			  /* merge the fetched tuple(right) and NULL
			     tuple(left) */
			  QEXEC_MERGE_ADD_MERGETUPLE (thread_p, NULL,
						      &inner_tplrec);
			}
		    }

		  /* move the inner(right) scan to the next tuple and
		     position tuple values (to merge columns) */
		  QEXEC_MERGE_OUTER_NEXT_SCAN_PVALS (thread_p, inner, false	/* do not exit */
		    );
		  if (inner_scan == S_END)
		    {
		      if (merge_cnt == 0)
			{	/* not merged */
			  /* depending on the join type, join with a NULL
			     opposite row when a NULL or a not-qualified is
			     encountered, or skip it. */
			  if (all_lefts)
			    {
			      /* merge the fetched tuple(left) and NULL
			         tuple(right) */
			      QEXEC_MERGE_ADD_MERGETUPLE (thread_p,
							  &outer_tplrec,
							  NULL);
			    }
			}
		      break;
		    }

		  /* and compare */
		  val_cmp = qexec_cmp_tpl_vals_merge (outer_valp,
						      outer_domp,
						      inner_valp,
						      inner_domp, nvals);
		  if (val_cmp != DB_EQ)
		    {
		      if (val_cmp == DB_UNK)
			{	/* is error */
			  goto exit_on_error;
			}

		      if (merge_cnt == 0)
			{	/* not merged */
			  /* depending on the join type, join with a NULL
			     opposite row when a NULL or a not-qualified is
			     encountered, or skip it. */
			  if (all_lefts)
			    {
			      /* merge the fetched tuple(left) and NULL
			         tuple(right) */
			      QEXEC_MERGE_ADD_MERGETUPLE (thread_p,
							  &outer_tplrec,
							  NULL);
			    }
			}

		      break;	/* found the bottom of the group */
		    }
		}
	      else
		{		/* group_cnt > 0 */
		  if (cnt >= group_cnt)
		    {
		      break;	/* reached the bottom of the group */
		    }

		  /* move the inner(right) scan to the next tuple */
		  QEXEC_MERGE_OUTER_NEXT_SCAN (thread_p, inner, true);
		}
	    }			/* while (1) */

	  /* move the outer to the next tuple and position tuple values */
	  QEXEC_MERGE_OUTER_NEXT_SCAN_PVALS (thread_p, outer, true);

	  /* if the group is formed for the first time */
	  if (group_cnt == 0)
	    {
	      /* save the position of inner scan;
	         it is the bottom of the group */
	      scan_save_scan_pos (inner_sid, &inner_scanpos);

	      if (inner_scan == S_END)
		{
		  /* move the inner to the previous tuple and
		     position tuple values */
		  QEXEC_MERGE_OUTER_PREV_SCAN_PVALS (thread_p, inner);

		  /* set group count and direction */
		  group_cnt = cnt;
		  direction = S_BACKWARD;
		}
	      else
		{
		  /* and compare */
		  val_cmp = qexec_cmp_tpl_vals_merge (outer_valp, outer_domp,
						      inner_valp, inner_domp,
						      nvals);
		  if (val_cmp == DB_UNK)
		    {		/* is error */
		      goto exit_on_error;
		    }

		  if (val_cmp == DB_LT)
		    {
		      /* move the inner to the previous tuple and
		         position tuple values */
		      QEXEC_MERGE_OUTER_PREV_SCAN_PVALS (thread_p, inner);

		      /* and compare */
		      val_cmp =
			qexec_cmp_tpl_vals_merge (outer_valp, outer_domp,
						  inner_valp, inner_domp,
						  nvals);
		      if (val_cmp == DB_UNK)
			{	/* is error */
			  goto exit_on_error;
			}

		      if (val_cmp == DB_EQ)
			{
			  /* next value is the same, so prepare for further
			     group scan operations */

			  /* set group count and direction */
			  group_cnt = cnt;
			  direction = S_BACKWARD;
			}
		      else
			{
			  /* move the inner to the current tuple and
			     position tuple values */
			  QEXEC_MERGE_OUTER_NEXT_SCAN_PVALS (thread_p, inner,
							     true);

			  val_cmp = DB_LT;	/* restore comparison */
			}
		    }

		  /* comparison has already been done */
		  already_compared = true;
		}
	    }
	  else
	    {
	      /* set further scan direction */
	      direction = S_BACKWARD;
	    }
	}
      else
	{			/* (direction == S_BACKWARD) */
	  /* move backwards within a group
	   */
	  cnt = group_cnt;
	  /* group_cnt has the number of tuples in the group */
	  while (1)
	    {
	      /* evaluate other outer join predicate */
	      ev_res = V_UNKNOWN;
	      if (other_outer_join_pred != NULL)
		{
		  ev_res = eval_pred (thread_p, other_outer_join_pred,
				      &xasl_state->vd, NULL);
		  if (ev_res == V_ERROR)
		    {
		      goto exit_on_error;
		    }
		}

	      /* is qualified */
	      if (other_outer_join_pred == NULL || ev_res == V_TRUE)
		{
		  /* merge the fetched tuples(left and right) */
		  QEXEC_MERGE_ADD_MERGETUPLE (thread_p, &outer_tplrec,
					      &inner_tplrec);
		}

	      cnt--;		/* decrease the counter of the processed tuples */

	      if (cnt <= 0)
		{
		  break;	/* finish the group */
		}

	      /* if not yet reached the top of the group */
	      /* move the inner(right) scan to the previous tuple */
	      QEXEC_MERGE_OUTER_PREV_SCAN (thread_p, inner);

	      /* all of the inner tuples in the group have the same
	         value at the merge column, so we don't need to
	         compare with the value of the outer one; just count
	         the number of the tuples in the group */
	    }			/* while (1) */

	  /* position tuple values (to merge columns) */
	  QEXEC_MERGE_PVALS (inner);

	  /* move the outer(left) scan to the next tuple and
	     position tuple values */
	  QEXEC_MERGE_OUTER_NEXT_SCAN_PVALS (thread_p, outer, true);

	  /* and compare */
	  val_cmp = qexec_cmp_tpl_vals_merge (outer_valp, outer_domp,
					      inner_valp, inner_domp, nvals);
	  if (val_cmp == DB_UNK)
	    {			/* is error */
	      goto exit_on_error;
	    }

	  if (val_cmp != DB_EQ)
	    {
	      /* jump to the previously set scan position */
	      inner_scan =
		scan_jump_scan_pos (thread_p, inner_sid, &inner_scanpos);
	      /* is saved position the end of scan? */
	      if (inner_scan == S_END)
		{
		  goto exit_on_end;	/* inner(right) is exhausted */
		}

	      /* and position tuple values */
	      QEXEC_MERGE_PVALS (inner);

	      /* reset group count */
	      group_cnt = 0;
	    }
	  else
	    {
	      /* comparison has already been done */
	      already_compared = true;
	    }

	  /* set further scan direction */
	  direction = S_FORWARD;

	}			/* (direction == S_BACKWARD) */

    }				/* while (1) */

exit_on_end:
  /* inner(right) is at the end. is there more to the outer(left) ? */
  if (all_lefts)
    {
      while (outer_scan != S_END)
	{
	  /* merge the fetched tuple(left) and NULL tuple(right) */
	  QEXEC_MERGE_ADD_MERGETUPLE (thread_p, &outer_tplrec, NULL);

	  /* move the outer to the next tuple */
	  QEXEC_MERGE_OUTER_NEXT_SCAN (thread_p, outer,
				       false /* do not exit */ );
	}
    }				/* if (all_lefts) */

  /* outer(left) is at the end. is there more to the inner(right) ? */
  if (all_rghts)
    {
      if (direction == S_FORWARD)
	{
	  if (val_cmp == DB_NE)
	    {			/* mark as not yet compared */
	      val_cmp = DB_UNK;	/* clear */

	      /* move the inner(right) scan to the first tuple and
	         position tuple values (to merge columns) */
	      QEXEC_MERGE_OUTER_NEXT_SCAN_PVALS (thread_p, inner, true);
	    }
	}
      else
	{			/* direction == S_BACKWARD */
	  inner_scan =
	    scan_jump_scan_pos (thread_p, inner_sid, &inner_scanpos);
	}
      while (inner_scan != S_END)
	{
	  /* merge the fetched tuple(right) and NULL tuple(left) */
	  QEXEC_MERGE_ADD_MERGETUPLE (thread_p, NULL, &inner_tplrec);

	  /* move the inner to the next tuple */
	  QEXEC_MERGE_OUTER_NEXT_SCAN (thread_p, inner,
				       false /* do not exit */ );
	}
    }				/* if (all_rghts) */

  free_and_init (type_list.domp);
  scan_end_scan (thread_p, outer_sid);
  scan_end_scan (thread_p, inner_sid);
  outer_sid->s.llsid.tplrecp = NULL;
  inner_sid->s.llsid.tplrecp = NULL;

  if (tplrec.tpl)
    {
      db_private_free_and_init (thread_p, tplrec.tpl);
    }

  if (outer_domp)
    {
      db_private_free_and_init (thread_p, outer_domp);
    }
  if (outer_valp)
    {
      db_private_free_and_init (thread_p, outer_valp);
    }

  if (inner_domp)
    {
      db_private_free_and_init (thread_p, inner_domp);
    }
  if (inner_valp)
    {
      db_private_free_and_init (thread_p, inner_valp);
    }

  if (list_idp)
    {
      qfile_close_list (thread_p, list_idp);
    }

  return list_idp;

exit_on_error:
  if (list_idp)
    {
      qfile_close_list (thread_p, list_idp);
      qfile_free_list_id (list_idp);
    }

  list_idp = NULL;

  direction = S_FORWARD;
  val_cmp = DB_UNK;		/* mark as error */
  all_lefts = all_rghts = false;

  goto exit_on_end;
}

/*
 * qexec_merge_listfiles () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     :
 *
 * Note: This function is used for a direct merge of two LIST files
 * during XASL tree interpretation.
 *
 * Note: For a direct list file merge, currently the outer and inner columns
 * should have the same data type.
 */
static int
qexec_merge_listfiles (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		       XASL_STATE * xasl_state)
{
  QFILE_LIST_ID *list_id = NULL;
  ACCESS_SPEC_TYPE *outer_spec = NULL;	/* left  */
  ACCESS_SPEC_TYPE *inner_spec = NULL;	/* right */
  QFILE_LIST_MERGE_INFO *merge_infop = &(xasl->proc.mergelist.ls_merge);
  XASL_NODE *outer_xasl, *inner_xasl;
  int ls_flag = 0;

  outer_xasl = xasl->proc.mergelist.outer_xasl;
  inner_xasl = xasl->proc.mergelist.inner_xasl;

  /* open the empty list file if not already open */

  if (outer_xasl->list_id->type_list.type_cnt == 0)
    {
      /* start main block iterations */
      if (qexec_start_mainblock_iterations (thread_p, outer_xasl,
					    xasl_state) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }
  if (inner_xasl->list_id->type_list.type_cnt == 0)
    {
      /* start main block iterations */
      if (qexec_start_mainblock_iterations (thread_p, inner_xasl,
					    xasl_state) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* If MERGELIST_PROC does not have 'order by' (xasl->orderby_list),
     then the list file to be open at here will be the last one.
     Otherwise, the last list file will be open at
     qexec_orderby_distinct().
     (Note that only one that can have 'group by' is BUILDLIST_PROC type.)
     And, the top most XASL is the other condition for the list file
     to be the last result file. */

  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);
  if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL)
      && XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED)
      && (xasl->orderby_list == NULL
	  || XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST))
      && xasl->option != Q_DISTINCT)
    {
      QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
    }

  if (merge_infop->join_type == JOIN_INNER)
    {
      /* call list file merge routine */
      list_id = qexec_merge_list (thread_p, outer_xasl->list_id,
				  inner_xasl->list_id, merge_infop, ls_flag);
    }
  else
    {
      outer_spec = xasl->proc.mergelist.outer_spec_list;
      inner_spec = xasl->proc.mergelist.inner_spec_list;

      if (qexec_open_scan (thread_p, outer_spec,
			   xasl->proc.mergelist.outer_val_list,
			   &xasl_state->vd,
			   true,
			   outer_spec->fixed_scan,
			   outer_spec->grouped_scan,
			   true, &outer_spec->s_id) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (qexec_open_scan (thread_p, inner_spec,
			   xasl->proc.mergelist.inner_val_list,
			   &xasl_state->vd,
			   true,
			   inner_spec->fixed_scan,
			   inner_spec->grouped_scan,
			   true, &inner_spec->s_id) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* call outer join merge routine */
      list_id = qexec_merge_list_outer (thread_p, &outer_spec->s_id,
					&inner_spec->s_id,
					merge_infop,
					xasl->after_join_pred, xasl_state,
					ls_flag);

      qexec_close_scan (thread_p, outer_spec);
      outer_spec = NULL;

      qexec_close_scan (thread_p, inner_spec);
      inner_spec = NULL;
    }

  if (list_id == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* make this the resultant list file */
  qfile_copy_list_id (xasl->list_id, list_id, true);
  qfile_free_list_id (list_id);

  return NO_ERROR;

exit_on_error:

  if (outer_spec)
    {
      qexec_close_scan (thread_p, outer_spec);
    }
  if (inner_spec)
    {
      qexec_close_scan (thread_p, inner_spec);
    }

  return ER_FAILED;
}

/*
 * Interpreter routines
 */

/*
 * qexec_open_scan () -
 *   return: NO_ERROR, or ER_code
 *   curr_spec(in)      : Access Specification Node
 *   val_list(in)       : Value list pointer
 *   vd(in)     : Value descriptor
 *   readonly_scan(in)  :
 *   fixed(in)  : Fixed scan flag
 *   grouped(in)        : Grouped scan flag
 *   iscan_oid_order(in)       :
 *   s_id(out)   : Set to the scan identifier
 *
 * Note: This routine is used to open a scan on an access specification
 * node. A scan identifier is created with the given parameters.
 */
static int
qexec_open_scan (THREAD_ENTRY * thread_p, ACCESS_SPEC_TYPE * curr_spec,
		 VAL_LIST * val_list, VAL_DESCR * vd, int readonly_scan,
		 int fixed, int grouped, bool iscan_oid_order, SCAN_ID * s_id)
{
  SCAN_TYPE scan_type;
  INDX_INFO *indx_info;

  switch (curr_spec->type)
    {
    case TARGET_CLASS:
      if (curr_spec->access == SEQUENTIAL)
	{
	  /* open a sequential heap file scan */
	  scan_type = S_HEAP_SCAN;
	  indx_info = NULL;
	}
      else if (curr_spec->access == INDEX)
	{
	  /* open an indexed heap file scan */
	  scan_type = S_INDX_SCAN;
	  indx_info = curr_spec->indexptr;
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE,
		  0);
	  return ER_FAILED;
	}			/* if */
      if (scan_type == S_HEAP_SCAN)
	{
	  if (scan_open_heap_scan (thread_p, s_id,
				   readonly_scan,
				   fixed,
				   curr_spec->lock_hint,
				   grouped,
				   curr_spec->single_fetch,
				   curr_spec->s_dbval,
				   val_list,
				   vd,
				   &ACCESS_SPEC_CLS_OID (curr_spec),
				   &ACCESS_SPEC_HFID (curr_spec),
				   curr_spec->s.cls_node.cls_regu_list_pred,
				   curr_spec->where_pred,
				   curr_spec->s.cls_node.cls_regu_list_rest,
				   curr_spec->s.cls_node.num_attrs_pred,
				   curr_spec->s.cls_node.attrids_pred,
				   curr_spec->s.cls_node.cache_pred,
				   curr_spec->s.cls_node.num_attrs_rest,
				   curr_spec->s.cls_node.attrids_rest,
				   curr_spec->s.cls_node.cache_rest) !=
	      NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
      else
	{
	  if (scan_open_index_scan (thread_p, s_id,
				    readonly_scan,
				    fixed,
				    curr_spec->lock_hint,
				    grouped,
				    curr_spec->single_fetch,
				    curr_spec->s_dbval,
				    val_list,
				    vd,
				    indx_info,
				    &ACCESS_SPEC_CLS_OID (curr_spec),
				    &ACCESS_SPEC_HFID (curr_spec),
				    curr_spec->s.cls_node.cls_regu_list_key,
				    curr_spec->where_key,
				    curr_spec->s.cls_node.cls_regu_list_pred,
				    curr_spec->where_pred,
				    curr_spec->s.cls_node.cls_regu_list_rest,
				    curr_spec->s.cls_node.num_attrs_key,
				    curr_spec->s.cls_node.attrids_key,
				    curr_spec->s.cls_node.cache_key,
				    curr_spec->s.cls_node.num_attrs_pred,
				    curr_spec->s.cls_node.attrids_pred,
				    curr_spec->s.cls_node.cache_pred,
				    curr_spec->s.cls_node.num_attrs_rest,
				    curr_spec->s.cls_node.attrids_rest,
				    curr_spec->s.cls_node.cache_rest,
				    iscan_oid_order) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	  /* monitor */
	  mnt_qm_iscans (thread_p);
	}
      break;

    case TARGET_CLASS_ATTR:
      if (scan_open_class_attr_scan (thread_p, s_id,
				     grouped,
				     curr_spec->single_fetch,
				     curr_spec->s_dbval,
				     val_list,
				     vd,
				     &ACCESS_SPEC_CLS_OID (curr_spec),
				     &ACCESS_SPEC_HFID (curr_spec),
				     curr_spec->s.cls_node.cls_regu_list_pred,
				     curr_spec->where_pred,
				     curr_spec->s.cls_node.cls_regu_list_rest,
				     curr_spec->s.cls_node.num_attrs_pred,
				     curr_spec->s.cls_node.attrids_pred,
				     curr_spec->s.cls_node.cache_pred,
				     curr_spec->s.cls_node.num_attrs_rest,
				     curr_spec->s.cls_node.attrids_rest,
				     curr_spec->s.cls_node.cache_rest) !=
	  NO_ERROR)
	{
	  goto exit_on_error;
	}
      break;

    case TARGET_LIST:
      /* open a list file scan */
      if (scan_open_list_scan (thread_p, s_id,
			       grouped,
			       curr_spec->single_fetch,
			       curr_spec->s_dbval,
			       val_list,
			       vd,
			       ACCESS_SPEC_LIST_ID (curr_spec),
			       curr_spec->s.list_node.list_regu_list_pred,
			       curr_spec->where_pred,
			       curr_spec->s.list_node.list_regu_list_rest) !=
	  NO_ERROR)
	{
	  goto exit_on_error;
	}
      break;

    case TARGET_SET:
      /* open a set based derived table scan */
      if (scan_open_set_scan (thread_p, s_id,
			      grouped,
			      curr_spec->single_fetch,
			      curr_spec->s_dbval,
			      val_list,
			      vd,
			      ACCESS_SPEC_SET_PTR (curr_spec),
			      ACCESS_SPEC_SET_REGU_LIST (curr_spec),
			      curr_spec->where_pred) != NO_ERROR)
	{
	  goto exit_on_error;
	}
      break;

    case TARGET_METHOD:
      if (scan_open_method_scan (thread_p, s_id,
				 grouped,
				 curr_spec->single_fetch,
				 curr_spec->s_dbval,
				 val_list,
				 vd,
				 ACCESS_SPEC_METHOD_LIST_ID (curr_spec),
				 ACCESS_SPEC_METHOD_SIG_LIST (curr_spec)) !=
	  NO_ERROR)
	{
	  goto exit_on_error;
	}
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      goto exit_on_error;
    }				/* switch */

  return NO_ERROR;

exit_on_error:

  return ER_FAILED;
}

/*
 * qexec_close_scan () -
 *   return:
 *   curr_spec(in)      : Access Specification Node
 *
 * Note: This routine is used to close the access specification node scan.
 */
void
qexec_close_scan (THREAD_ENTRY * thread_p, ACCESS_SPEC_TYPE * curr_spec)
{
  if (curr_spec)
    {
      /* monitoring */
      switch (curr_spec->type)
	{
	case TARGET_CLASS:
	  if (curr_spec->access == SEQUENTIAL)
	    {
	      mnt_qm_sscans (thread_p);
	    }
	  else if (curr_spec->access == INDEX)
	    {
	      mnt_qm_iscans (thread_p);
	    }
	  break;
	case TARGET_CLASS_ATTR:
	  break;
	case TARGET_LIST:
	  mnt_qm_lscans (thread_p);
	  break;
	case TARGET_SET:
	  mnt_qm_setscans (thread_p);
	  break;
	case TARGET_METHOD:
	  mnt_qm_methscans (thread_p);
	  break;
	}
      scan_close_scan (thread_p, &curr_spec->s_id);
    }
}

/*
 * qexec_end_scan () -
 *   return:
 *   curr_spec(in)      : Access Specification Node
 *
 * Note: This routine is used to end the access specification node scan.
 *
 * Note: This routine is called for ERROR CASE scan end operation.
 */
void
qexec_end_scan (THREAD_ENTRY * thread_p, ACCESS_SPEC_TYPE * curr_spec)
{
  if (curr_spec)
    {
      scan_end_scan (thread_p, &curr_spec->s_id);
    }
}

/*
 * qexec_next_merge_block () -
 *   return:
 *   spec(in)   :
 */
static SCAN_CODE
qexec_next_merge_block (THREAD_ENTRY * thread_p, ACCESS_SPEC_TYPE ** spec)
{
  SCAN_CODE sb_scan;

  if (scan_start_scan (thread_p, &(*spec)->s_id) != NO_ERROR)
    {
      return S_ERROR;
    }

  do
    {
      sb_scan = scan_next_scan_block (thread_p, &(*spec)->s_id);
      if (sb_scan == S_SUCCESS)
	{
	  return S_SUCCESS;
	}
      else if (sb_scan == S_END)
	{
	  /* close old scan */
	  scan_end_scan (thread_p, &(*spec)->s_id);

	  /* move to the following access specifications left */
	  *spec = (*spec)->next;

	  /* check for and skip the case of empty heap files */
	  while (*spec != NULL && QEXEC_EMPTY_ACCESS_SPEC_SCAN (*spec))
	    {
	      *spec = (*spec)->next;
	    }

	  if (*spec == NULL)
	    {
	      return S_END;
	    }

	  /* initialize scan */
	  if (scan_start_scan (thread_p, &(*spec)->s_id) != NO_ERROR)
	    {
	      return S_ERROR;
	    }
	}
      else
	{
	  return S_ERROR;
	}

    }
  while (1);
}

/*
 * qexec_next_scan_block () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   xasl(in)   : XASL Tree pointer
 *
 * Note: This function is used to move the current access specification
 * node scan identifier for the given XASL block to the next
 * scan block. If there are no more scan blocks for the current
 * access specfication node, it moves to the next access
 * specification, if any, and starts the new scan block for that
 * node. If there are no more access specification nodes left,
 * it returns S_END.
 */
SCAN_CODE
qexec_next_scan_block (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  SCAN_CODE sb_scan;

  if (xasl->curr_spec == NULL)
    {
      /* initialize scan id */
      xasl->curr_spec = xasl->spec_list;

      /* check for and skip the case of empty heap file cases */
      while (xasl->curr_spec != NULL
	     && QEXEC_EMPTY_ACCESS_SPEC_SCAN (xasl->curr_spec))
	{
	  xasl->curr_spec = xasl->curr_spec->next;
	}

      if (xasl->curr_spec == NULL)
	{
	  return S_END;
	}

      /* initialilize scan */
      if (scan_start_scan_proxy (thread_p, xasl, xasl->curr_spec) != NO_ERROR)
	{
	  return S_ERROR;
	}
    }

  do
    {

      sb_scan = scan_next_scan_block_proxy (thread_p, xasl->curr_spec);
      if (sb_scan == S_SUCCESS)
	{
	  return S_SUCCESS;
	}
      else if (sb_scan == S_END)
	{
	  /* close old scan */
	  scan_end_scan_proxy (thread_p, xasl, xasl->curr_spec);

	  /* move to the following access specifications left */
	  xasl->curr_spec = xasl->curr_spec->next;

	  /* check for and skip the case of empty heap files */
	  while (xasl->curr_spec != NULL
		 && QEXEC_EMPTY_ACCESS_SPEC_SCAN (xasl->curr_spec))
	    {
	      xasl->curr_spec = xasl->curr_spec->next;
	    }

	  if (xasl->curr_spec == NULL)
	    {
	      return S_END;
	    }

	  /* initialize scan */
	  if (scan_start_scan_proxy (thread_p, xasl, xasl->curr_spec) !=
	      NO_ERROR)
	    {
	      return S_ERROR;
	    }
	}
      else
	{
	  return S_ERROR;
	}
    }
  while (1);

}

/*
 * qexec_next_scan_block_iterations () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   xasl(in)   : XASL Tree pointer
 *
 */
static SCAN_CODE
qexec_next_scan_block_iterations (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  SCAN_CODE sb_next;
  SCAN_CODE xs_scan;
  SCAN_CODE xs_scan2;
  XASL_NODE *last_xptr;
  XASL_NODE *prev_xptr;
  XASL_NODE *xptr2, *xptr3;

  /* first find the last scan block to be moved */
  for (last_xptr = xasl; last_xptr->scan_ptr; last_xptr = last_xptr->scan_ptr)
    {
      if (!last_xptr->next_scan_block_on
	  || (last_xptr->curr_spec
	      && last_xptr->curr_spec->s_id.status == S_STARTED
	      && !last_xptr->curr_spec->s_id.qualified_block))
	{
	  break;
	}
    }

  /* move the last scan block and reset further scans */

  /* if there are no qualified items in the current scan block,
   * this scan block will make no contribution with other possible
   * scan block combinations from following classes. Thus, directly
   * move to the next scan block in this class.
   */
  if (last_xptr->curr_spec
      && last_xptr->curr_spec->s_id.status == S_STARTED
      && !last_xptr->curr_spec->s_id.qualified_block)
    {
      if ((xs_scan = qexec_next_scan_block (thread_p, last_xptr)) == S_END)
	{
	  /* close following scan procedures if they are still active */
	  for (xptr2 = last_xptr; xptr2; xptr2 = xptr2->scan_ptr)
	    {
	      if (xptr2->scan_ptr && xptr2->next_scan_block_on)
		{
		  if (xptr2->scan_ptr->curr_spec)
		    {
		      scan_end_scan_proxy (thread_p, xptr2->scan_ptr,
					   xptr2->scan_ptr->curr_spec);
		    }
		  xptr2->scan_ptr->curr_spec = NULL;
		  xptr2->next_scan_block_on = false;
		}
	    }
	}
      else if (xs_scan == S_ERROR)
	{
	  return S_ERROR;
	}
    }
  else if ((xs_scan = qexec_next_scan_block (thread_p, last_xptr)) ==
	   S_SUCCESS)
    {				/* reset all the futher scans */
      for (xptr2 = last_xptr; xptr2; xptr2 = xptr2->scan_ptr)
	{
	  if (xptr2->scan_ptr)
	    {
	      sb_next = qexec_next_scan_block (thread_p, xptr2->scan_ptr);
	      if (sb_next == S_SUCCESS)
		{
		  xptr2->next_scan_block_on = true;
		}
	      else if (sb_next == S_END)
		{
		  /* close all preceding scan procedures and return */
		  for (xptr3 = xasl; xptr3 && xptr3 != xptr2->scan_ptr;
		       xptr3 = xptr3->scan_ptr)
		    {
		      if (xptr3->curr_spec)
			{
			  scan_end_scan_proxy (thread_p, xptr3,
					       xptr3->curr_spec);
			}
		      xptr3->curr_spec = NULL;
		      xptr3->next_scan_block_on = false;
		    }
		  return S_END;
		}
	      else
		{
		  return S_ERROR;
		}
	    }
	}
    }
  else if (xs_scan == S_ERROR)
    {
      return S_ERROR;
    }

  /* now move backwards, resetting all the previous scans */
  while (last_xptr != xasl)
    {

      /* find the previous to last xptr */
      for (prev_xptr = xasl; prev_xptr->scan_ptr != last_xptr;
	   prev_xptr = prev_xptr->scan_ptr)
	;

      /* set previous scan according to the last scan status */
      if (last_xptr->curr_spec == NULL)
	{			/* last scan ended */
	  prev_xptr->next_scan_block_on = false;

	  /* move the scan block of the previous scan */
	  xs_scan2 = qexec_next_scan_block (thread_p, prev_xptr);
	  if (xs_scan2 == S_SUCCESS)
	    {
	      /* move all the further scan blocks */
	      for (xptr2 = prev_xptr; xptr2; xptr2 = xptr2->scan_ptr)
		{
		  if (xptr2->scan_ptr)
		    {
		      sb_next = qexec_next_scan_block (thread_p,
						       xptr2->scan_ptr);
		      if (sb_next == S_SUCCESS)
			{
			  xptr2->next_scan_block_on = true;
			}
		      else if (sb_next == S_END)
			{
			  /* close all preceding scan procedures and return */
			  for (xptr3 = xasl;
			       xptr3 && xptr3 != xptr2->scan_ptr;
			       xptr3 = xptr3->scan_ptr)
			    {
			      if (xptr3->curr_spec)
				{
				  scan_end_scan_proxy (thread_p, xptr3,
						       xptr3->curr_spec);
				}
			      xptr3->curr_spec = NULL;
			      xptr3->next_scan_block_on = false;
			    }
			  return S_END;
			}
		      else
			{
			  return S_ERROR;
			}
		    }
		}
	    }
	  else if (xs_scan2 == S_ERROR)
	    {
	      return S_ERROR;
	    }

	}
      else			/* last scan successfully moved */
	{
	  if (scan_reset_scan_block_proxy (thread_p, prev_xptr) == S_ERROR)
	    {
	      return S_ERROR;
	    }
	}

      /* make previous scan the last scan ptr */
      last_xptr = prev_xptr;
    }				/* while */

  /* return the status of the first XASL block */
  return (xasl->curr_spec) ? S_SUCCESS : S_END;
}

/*
 * qexec_execute_scan () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   xasl(in)   : XASL Tree block
 *   xasl_state(in)     : XASL tree state information
 *   next_scan_fnc(in)  : Function to interpret following scan block
 *
 * Note: This routine executes one iteration on a scan operation on
 * the given XASL tree block. If end_of_scan is reached, it
 * return S_END. If an error occurs, it returns
 * S_ERROR. Each scan procedure block may have its own scan
 * procedures forming a path of scan procedure blocks. Thus, for
 * each scan procedure block interpretation, if there are already
 * active scan procedures started from that block, first their
 * execution is requested. Only if all of the following scan
 * procedures come to an end returning S_END, then the
 * current scan procedure scan item is advanced. When this scan
 * procedure too come to an end, it returns S_END to the
 * caller, indicating that there are no more scan items in the
 * path of the scan procedure blocks.
 *
 * Note: This function is the general scan block interpretation function.
 */
static SCAN_CODE
qexec_execute_scan (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		    XASL_STATE * xasl_state, QFILE_TUPLE_RECORD * ignore,
		    XASL_SCAN_FNC_PTR next_scan_fnc)
{
  XASL_NODE *xptr;
  SCAN_CODE sc_scan;
  SCAN_CODE xs_scan;
  DB_LOGICAL ev_res;
  int qualified;

  /* check if further scan procedure are still active */
  if (xasl->scan_ptr && xasl->next_scan_on)
    {
      xs_scan = (*next_scan_fnc) (thread_p, xasl->scan_ptr, xasl_state,
				  ignore, next_scan_fnc + 1);
      if (xs_scan != S_END)
	{
	  return xs_scan;
	}

      xasl->next_scan_on = false;
    }

  do
    {
      sc_scan =
	scan_next_scan_proxy (thread_p, xasl, xasl->curr_spec, S_FORWARD,
			      false);
      if (sc_scan != S_SUCCESS)
	{
	  return sc_scan;
	}

      /* set scan item as qualified */
      qualified = true;

      if (qualified)
	{
	  /* clear bptr subquery list files */
	  if (xasl->bptr_list)
	    {
	      qexec_clear_head_lists (thread_p, xasl->bptr_list);
	    }
	  /* evaluate bptr list */
	  for (xptr = xasl->bptr_list; qualified && xptr != NULL;
	       xptr = xptr->next)
	    {
	      if (qexec_execute_obj_set_fetch (thread_p, xptr, xasl_state) !=
		  NO_ERROR)
		{
		  return S_ERROR;
		}
	      else if (xptr->proc.fetch.fetch_res == false)
		{
		  qualified = false;
		}
	    }
	}			/* if (qualified) */

      if (qualified)
	{
	  /* evaluate dptr list */
	  for (xptr = xasl->dptr_list; xptr != NULL; xptr = xptr->next)
	    {
	      /* clear correlated subquery list files */
	      qexec_clear_head_lists (thread_p, xptr);
	      if (XASL_IS_FLAGED (xptr, XASL_LINK_TO_REGU_VARIABLE))
		{
		  /* skip if linked to regu var */
		  continue;
		}
	      if (qexec_execute_mainblock (thread_p, xptr, xasl_state) !=
		  NO_ERROR)
		{
		  return S_ERROR;
		}
	    }
	}			/* if (qualified) */

      if (qualified)
	{
	  /* evaluate after join predicate */
	  ev_res = V_UNKNOWN;
	  if (xasl->after_join_pred != NULL)
	    {
	      ev_res = eval_pred (thread_p, xasl->after_join_pred,
				  &xasl_state->vd, NULL);
	      if (ev_res == V_ERROR)
		{
		  return S_ERROR;
		}
	    }
	  qualified = (xasl->after_join_pred == NULL || ev_res == V_TRUE);
	}			/* if (qualified) */

      if (qualified)
	{
	  /* evaluate if predicate */
	  ev_res = V_UNKNOWN;
	  if (xasl->if_pred != NULL)
	    {
	      ev_res =
		eval_pred (thread_p, xasl->if_pred, &xasl_state->vd, NULL);
	      if (ev_res == V_ERROR)
		{
		  return S_ERROR;
		}
	    }
	  qualified = (xasl->if_pred == NULL || ev_res == V_TRUE);
	}			/* if (qualified) */

      if (qualified)
	{
	  /* clear fptr subquery list files */
	  if (xasl->fptr_list)
	    {
	      qexec_clear_head_lists (thread_p, xasl->fptr_list);
	    }
	  /* evaluate fptr list */
	  for (xptr = xasl->fptr_list; qualified && xptr != NULL;
	       xptr = xptr->next)
	    {
	      if (qexec_execute_obj_set_fetch (thread_p, xptr, xasl_state) !=
		  NO_ERROR)
		{
		  return S_ERROR;
		}
	      else if (xptr->proc.fetch.fetch_res == false)
		{
		  qualified = false;
		}
	    }
	}			/* if (qualified) */

      if (qualified)
	{
	  if (!xasl->scan_ptr)
	    {
	      /* no scan procedure block */
	      return S_SUCCESS;
	    }
	  else
	    {
	      /* current scan block has at least one qualified item */
	      xasl->curr_spec->s_id.qualified_block = true;

	      /* start following scan procedure */
	      xasl->scan_ptr->next_scan_on = false;
	      if (scan_reset_scan_block_proxy
		  (thread_p, xasl->scan_ptr) == S_ERROR)
		{
		  return S_ERROR;
		}
	      xasl->next_scan_on = true;

	      /* execute following scan procedure */
	      xs_scan =
		(*next_scan_fnc) (thread_p, xasl->scan_ptr, xasl_state,
				  ignore, next_scan_fnc + 1);
	      if (xs_scan == S_END)
		{
		  xasl->next_scan_on = false;
		}
	      else
		{
		  return xs_scan;
		}
	    }
	}			/* if (qualified) */

    }
  while (1);

}

/*
 * qexec_intprt_fnc () -
 *   return: scan code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL Tree state information
 *   tplrec(out) : Tuple record descriptor to store result tuples
 *   next_scan_fnc(in)  : Function to interpret following XASL scan block
 *
 * Note: This function is the main function used to interpret an XASL
 * tree block. That is, it assumes a general format XASL block
 * with all possible representations.
 */
static SCAN_CODE
qexec_intprt_fnc (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		  XASL_STATE * xasl_state, QFILE_TUPLE_RECORD * tplrec,
		  XASL_SCAN_FNC_PTR next_scan_fnc)
{
  XASL_NODE *xptr;
  SCAN_CODE xs_scan;
  SCAN_CODE xb_scan;
  SCAN_CODE ls_scan;
  DB_LOGICAL ev_res;
  int qualified;
  AGGREGATE_TYPE *agg_ptr;
  bool count_star_with_iscan_opt = false;

  if (xasl->type == BUILDVALUE_PROC)
    {
      BUILDVALUE_PROC_NODE *buildvalue = &xasl->proc.buildvalue;

      /* If it is possible to evaluate aggregation using index, do it */
      if (buildvalue->agg_list != NULL)
	{
	  bool flag_scan_needed;

	  flag_scan_needed = false;	/* init */

	  if (buildvalue->is_always_false != true)
	    {
	      for (agg_ptr = buildvalue->agg_list; agg_ptr;
		   agg_ptr = agg_ptr->next)
		{
		  if (!agg_ptr->flag_agg_optimize)
		    {
		      flag_scan_needed = true;
		      break;
		    }
		}

	      for (agg_ptr = buildvalue->agg_list; agg_ptr;
		   agg_ptr = agg_ptr->next)
		{
		  if (agg_ptr->flag_agg_optimize)
		    {
		      if (agg_ptr->function == PT_COUNT_STAR
			  && flag_scan_needed)
			{
			  /* If scan is needed, do not optimize PT_COUNT_STAR. */
			  agg_ptr->flag_agg_optimize = false;
			  continue;
			}
		      if (qdata_evaluate_aggregate_optimize (thread_p,
							     agg_ptr,
							     &xasl->
							     spec_list->s.
							     cls_node.
							     hfid,
							     xasl->spec_list->
							     s.cls_node.
							     node_id) !=
			  NO_ERROR)
			{
			  agg_ptr->flag_agg_optimize = false;
			  flag_scan_needed = true;
			}
		    }
		}
	    }

	  if (!flag_scan_needed)
	    {
	      return S_SUCCESS;
	    }

	  agg_ptr = buildvalue->agg_list;
	  if (!xasl->scan_ptr	/* no scan procedure */
	      && !xasl->fptr_list	/* no path expressions */
	      && !xasl->if_pred	/* no if predicates */
	      && !xasl->instnum_pred	/* no instnum predicate */
	      && agg_ptr->next == NULL	/* no other aggregate functions */
	      && agg_ptr->function == PT_COUNT_STAR)
	    {
	      /* only one count(*) function */
	      ACCESS_SPEC_TYPE *specp = xasl->spec_list;
	      if (specp->next == NULL
		  && specp->access == INDEX
		  && specp->s.cls_node.cls_regu_list_pred == NULL
		  && specp->where_pred == NULL)
		{
		  /* count(*) query will scan an index
		   * but does not have a data-filter
		   */
		  specp->s_id.s.isid.need_count_only = true;
		  count_star_with_iscan_opt = true;
		}
	    }
	}
    }
  else if (xasl->type == BUILDLIST_PROC)
    {
      /* If it is BUILDLIST, do not optimize aggregation */
      if (xasl->proc.buildlist.g_agg_list != NULL)
	{
	  for (agg_ptr = xasl->proc.buildlist.g_agg_list; agg_ptr;
	       agg_ptr = agg_ptr->next)
	    {
	      agg_ptr->flag_agg_optimize = false;
	    }
	}
    }

  while ((xb_scan =
	  qexec_next_scan_block_iterations (thread_p, xasl)) == S_SUCCESS)
    {
      while ((ls_scan =
	      scan_next_scan_proxy (thread_p, xasl, xasl->curr_spec,
				    S_FORWARD,
				    count_star_with_iscan_opt)) == S_SUCCESS)
	{
	  if (count_star_with_iscan_opt)
	    {
	      xasl->proc.buildvalue.agg_list->curr_cnt +=
		(&xasl->curr_spec->s_id)->s.isid.oid_list.oid_cnt;
	      /* may have more scan ranges */
	      continue;
	    }
	  /* set scan item as qualified */
	  qualified = true;


	  /* evaluate bptr list */
	  /* if path expression fetch fails, this instance disqualifies */
	  for (xptr = xasl->bptr_list;
	       qualified && xptr != NULL; xptr = xptr->next)
	    {
	      if (qexec_execute_obj_set_fetch (thread_p, xptr, xasl_state) !=
		  NO_ERROR)
		{
		  return S_ERROR;
		}
	      else if (xptr->proc.fetch.fetch_res == false)
		{
		  qualified = false;
		}
	    }

	  if (qualified)
	    {
	      /* evaluate dptr list */
	      for (xptr = xasl->dptr_list; xptr != NULL; xptr = xptr->next)
		{
		  /* clear correlated subquery list files */
		  qexec_clear_head_lists (thread_p, xptr);
		  if (XASL_IS_FLAGED (xptr, XASL_LINK_TO_REGU_VARIABLE))
		    {
		      /* skip if linked to regu var */
		      continue;
		    }
		  if (qexec_execute_mainblock (thread_p, xptr, xasl_state) !=
		      NO_ERROR)
		    {
		      return S_ERROR;
		    }
		}

	      /* evaluate after join predicate */
	      ev_res = V_UNKNOWN;
	      if (xasl->after_join_pred != NULL)
		{
		  ev_res = eval_pred (thread_p, xasl->after_join_pred,
				      &xasl_state->vd, NULL);
		  if (ev_res == V_ERROR)
		    {
		      return S_ERROR;
		    }
		}
	      qualified = (xasl->after_join_pred == NULL || ev_res == V_TRUE);

	      if (qualified)
		{
		  /* evaluate if predicate */
		  ev_res = V_UNKNOWN;
		  if (xasl->if_pred != NULL)
		    {
		      ev_res = eval_pred (thread_p, xasl->if_pred,
					  &xasl_state->vd, NULL);
		      if (ev_res == V_ERROR)
			{
			  return S_ERROR;
			}
		    }
		  qualified = (xasl->if_pred == NULL || ev_res == V_TRUE);
		}

	      if (qualified)
		{
		  /* evaluate fptr list */
		  for (xptr = xasl->fptr_list;
		       qualified && xptr != NULL; xptr = xptr->next)
		    {
		      if (qexec_execute_obj_set_fetch (thread_p, xptr,
						       xasl_state)
			  != NO_ERROR)
			{
			  return S_ERROR;
			}
		      else if (xptr->proc.fetch.fetch_res == false)
			{
			  qualified = false;
			}
		    }

		  if (qualified)
		    {

		      if (!xasl->scan_ptr)
			{	/* no scan procedure block */

			  /* if hierarchical query do special processing */
			  if (XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
			    {
			      if (qexec_update_connect_by_lists (thread_p,
								 xasl->
								 connect_by_ptr,
								 xasl_state,
								 tplrec)
				  != NO_ERROR)
				{
				  return S_ERROR;
				}
			    }
			  else
			    {
			      /* evaluate inst_num predicate */
			      if (xasl->instnum_val)
				{
				  ev_res = qexec_eval_instnum_pred (thread_p,
								    xasl,
								    xasl_state);
				  if (ev_res == V_ERROR)
				    {
				      return S_ERROR;
				    }
				  if ((xasl->instnum_flag
				       & XASL_INSTNUM_FLAG_SCAN_STOP))
				    {
				      return S_SUCCESS;
				    }
				}
			      qualified = (xasl->instnum_pred == NULL
					   || ev_res == V_TRUE);
			      if (qualified
				  && (qexec_end_one_iteration (thread_p, xasl,
							       xasl_state,
							       tplrec)
				      != NO_ERROR))
				{
				  return S_ERROR;
				}
			    }

			}
		      else
			{	/* handle the scan procedure */
			  /* current scan block has at least one qualified item */
			  xasl->curr_spec->s_id.qualified_block = true;

			  /* handle the scan procedure */
			  xasl->scan_ptr->next_scan_on = false;
			  if (scan_reset_scan_block_proxy
			      (thread_p, xasl->scan_ptr) == S_ERROR)
			    {
			      return S_ERROR;
			    }
			  xasl->next_scan_on = true;


			  while ((xs_scan = (*next_scan_fnc) (thread_p,
							      xasl->scan_ptr,
							      xasl_state,
							      tplrec,
							      next_scan_fnc +
							      1)) ==
				 S_SUCCESS)
			    {

			      /* if hierarchical query do special processing */
			      if (XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
				{
				  if (qexec_update_connect_by_lists (thread_p,
								     xasl->
								     connect_by_ptr,
								     xasl_state,
								     tplrec)
				      != NO_ERROR)
				    {
				      return S_ERROR;
				    }
				}
			      else
				{
				  /* evaluate inst_num predicate */
				  if (xasl->instnum_val)
				    {
				      ev_res =
					qexec_eval_instnum_pred (thread_p,
								 xasl,
								 xasl_state);
				      if (ev_res == V_ERROR)
					{
					  return S_ERROR;
					}
				      if (xasl->instnum_flag
					  & XASL_INSTNUM_FLAG_SCAN_STOP)
					{
					  return S_SUCCESS;
					}
				    }
				  qualified = (xasl->instnum_pred == NULL
					       || ev_res == V_TRUE);

				  if (qualified
				      /* one iteration successfully completed */
				      && qexec_end_one_iteration (thread_p,
								  xasl,
								  xasl_state,
								  tplrec) !=
				      NO_ERROR)
				    {
				      return S_ERROR;
				    }
				}

			    }

			  if (xs_scan != S_END)	/* an error happened */
			    {
			      return S_ERROR;
			    }
			}
		    }
		}
	    }

	  qexec_clear_all_lists (thread_p, xasl);
	}

      if (ls_scan != S_END)	/* an error happened */
	{
	  return S_ERROR;
	}
    }

  if (xb_scan != S_END)		/* an error happened */
    {
      return S_ERROR;
    }

  return S_SUCCESS;
}

/*
 * qexec_merge_fnc () -
 *   return: scan code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL Tree state information
 *   tplrec(out) : Tuple record descriptor to store result tuples
 *
 * Note: This function is used to interpret an XASL merge command
 * tree block. It assumes a general format XASL block
 * with all possible representations.
 */
static SCAN_CODE
qexec_merge_fnc (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		 XASL_STATE * xasl_state, QFILE_TUPLE_RECORD * tplrec,
		 XASL_SCAN_FNC_PTR ignore)
{
  XASL_NODE *xptr;
  DB_LOGICAL ev_res;
  int qualified;
  SCAN_CODE ls_scan1;		/* first list file scan counter */
  SCAN_CODE ls_scan2;		/* second list file scan counter */
  SCAN_ID *s_id1;		/* first list file scan identifier */
  SCAN_ID *s_id2;		/* second list file scan identifier */
  ACCESS_SPEC_TYPE *spec;

  /* set first scan parameters */
  s_id1 = &xasl->spec_list->s_id;
  /* set second scan parameters */
  s_id2 = &xasl->merge_spec->s_id;

  if ((!s_id1)
      || ((s_id1->type == S_LIST_SCAN)
	  && ((!s_id1->s.llsid.list_id)
	      || (s_id1->s.llsid.list_id->type_list.type_cnt == 0))))
    {
      GOTO_EXIT_ON_ERROR;
    }

  if ((!s_id2)
      || ((s_id2->type == S_LIST_SCAN)
	  && ((!s_id2->s.llsid.list_id)
	      || (s_id2->s.llsid.list_id->type_list.type_cnt == 0))))
    {
      GOTO_EXIT_ON_ERROR;
    }

  spec = xasl->merge_spec;
  if (qexec_next_merge_block (thread_p, &spec) != S_SUCCESS)
    {
      GOTO_EXIT_ON_ERROR;
    }

  while ((ls_scan1 = qexec_next_scan_block (thread_p, xasl)) == S_SUCCESS)
    {
      while ((ls_scan1 = scan_next_scan (thread_p, s_id1)) == S_SUCCESS)
	{
	  ls_scan2 = scan_next_scan (thread_p, s_id2);
	  if (ls_scan2 == S_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (ls_scan2 == S_END)
	    {
	      if (qexec_next_merge_block (thread_p, &spec) != S_SUCCESS)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      ls_scan2 = scan_next_scan (thread_p, s_id2);
	      if (ls_scan2 == S_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }

	  /* set scan item as qualified */
	  qualified = true;

	  /* evaluate bptr list */
	  /* if path expression fetch fails, this instance disqualifies */
	  for (xptr = xasl->bptr_list; qualified && xptr != NULL;
	       xptr = xptr->next)
	    {
	      if (qexec_execute_obj_set_fetch (thread_p, xptr, xasl_state) !=
		  NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      else if (xptr->proc.fetch.fetch_res == false)
		{
		  qualified = false;
		}
	    }

	  if (qualified)
	    {
	      /* evaluate dptr list */
	      for (xptr = xasl->dptr_list; xptr != NULL; xptr = xptr->next)
		{
		  /* clear correlated subquery list files */
		  qexec_clear_head_lists (thread_p, xptr);
		  if (XASL_IS_FLAGED (xptr, XASL_LINK_TO_REGU_VARIABLE))
		    {
		      /* skip if linked to regu var */
		      continue;
		    }
		  if (qexec_execute_mainblock (thread_p, xptr, xasl_state) !=
		      NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      /* evaluate if predicate */
	      ev_res = V_UNKNOWN;
	      if (xasl->if_pred != NULL)
		{
		  ev_res = eval_pred (thread_p, xasl->if_pred,
				      &xasl_state->vd, NULL);
		  if (ev_res == V_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      qualified = (xasl->if_pred == NULL || ev_res == V_TRUE);
	      if (qualified)
		{
		  /* evaluate fptr list */
		  for (xptr = xasl->fptr_list;
		       qualified && xptr != NULL; xptr = xptr->next)
		    {
		      if (qexec_execute_obj_set_fetch (thread_p, xptr,
						       xasl_state) !=
			  NO_ERROR)
			{
			  GOTO_EXIT_ON_ERROR;
			}
		      else if (xptr->proc.fetch.fetch_res == false)
			{
			  qualified = false;
			}
		    }

		  if (qualified)
		    {
		      if (!xasl->scan_ptr)
			{	/* no scan procedure block */
			  /* evaluate inst_num predicate */
			  if (xasl->instnum_val)
			    {
			      ev_res = qexec_eval_instnum_pred (thread_p,
								xasl,
								xasl_state);
			      if (ev_res == V_ERROR)
				{
				  return S_ERROR;
				}
			      if (xasl->
				  instnum_flag & XASL_INSTNUM_FLAG_SCAN_STOP)
				{
				  scan_end_scan (thread_p, s_id1);
				  scan_end_scan (thread_p, s_id2);
				  return S_SUCCESS;
				}
			    }	/* if (xasl->instnum_val) */
			  qualified = (xasl->instnum_pred == NULL
				       || ev_res == V_TRUE);

			  if (qualified
			      && qexec_end_one_iteration (thread_p, xasl,
							  xasl_state,
							  tplrec) != NO_ERROR)
			    {
			      return S_ERROR;
			    }
			}	/* handle the scan procedure */
		    }
		}
	    }
	}

      if (ls_scan1 == S_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  if (ls_scan1 == S_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  scan_end_scan (thread_p, s_id1);
  scan_end_scan (thread_p, s_id2);
  return S_SUCCESS;

exit_on_error:
  scan_end_scan (thread_p, s_id1);
  scan_end_scan (thread_p, s_id2);
  scan_close_scan (thread_p, s_id1);
  scan_close_scan (thread_p, s_id2);
  return S_ERROR;
}

/*
 * qexec_setup_list_id () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree block
 *
 * Note: This routine is used by update/delete/insert to set up a
 * type_list. Copying a list_id structure fails unless it has a type list.
 */
static int
qexec_setup_list_id (XASL_NODE * xasl)
{
  QFILE_LIST_ID *list_id;

  list_id = xasl->list_id;
  list_id->tuple_cnt = 0;
  list_id->page_cnt = 0;

  /* For streaming queries, set last_pgptr->next_vpid to NULL */
  if (list_id->last_pgptr != NULL)
    {
      QFILE_PUT_NEXT_VPID_NULL (list_id->last_pgptr);
    }

  list_id->last_pgptr = NULL;	/* don't want qfile_close_list() to free this
				 * bogus listid
				 */
  list_id->type_list.type_cnt = 1;
  list_id->type_list.domp =
    (TP_DOMAIN **) malloc (list_id->type_list.type_cnt *
			   sizeof (TP_DOMAIN *));
  if (list_id->type_list.domp == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      list_id->type_list.type_cnt * sizeof (TP_DOMAIN *));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  /* set up to return object domains in case we want to return
   * the updated/inserted/deleted oid's
   */
  list_id->type_list.domp[0] = &tp_Object_domain;

  return NO_ERROR;
}

/*
 * qexec_execute_update () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree block
 *   xasl_state(in)     :
 */
static int
qexec_execute_update (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		      XASL_STATE * xasl_state)
{
  UPDATE_PROC_NODE *update = &xasl->proc.update;
  SCAN_CODE xb_scan;
  SCAN_CODE ls_scan;
  XASL_NODE *aptr;
  DB_VALUE *valp;
  QPROC_DB_VALUE_LIST vallist;
  int k, i1, i2, parts_idx;
  int val_index;
  int rc;
  int no_vals;
  int cl_index = 0;
  OID *oid = NULL;
  OID *class_oid = NULL;
  OID prev_class_oid;
  HFID *hfid = NULL;
  ACCESS_SPEC_TYPE *specp = NULL;
  SCAN_ID *s_id;
  HEAP_CACHE_ATTRINFO attr_info;
  int attr_info_inited = 0;
  HEAP_SCANCACHE scan_cache;
  bool scan_cache_inited = false;
  LOG_LSA lsa;
  int savepoint_used = 0;
  int satisfies_constraints;
  int force_count;
  int op_type;
  int s, t;
  int num_unique_btrees = 0, max_unique_btrees = 0;
  int malloc_size;
  BTREE_UNIQUE_STATS *unique_stat_info = NULL;
  BTREE_UNIQUE_STATS *temp_info;
  char *ptr, *adjmap;
  REGU_VARIABLE *rvsave = NULL;
  int retidx, partition_unchanged;
  OID prev_oid = { 0, 0, 0 };
  REPR_ID new_reprid = 0;
  int tuple_cnt;
  REPL_INFO_TYPE repl_info;

  OID_SET_NULL (&prev_class_oid);

  aptr = xasl->aptr_list;

  for (specp = aptr->spec_list; specp; specp = specp->next)
    {
      if (specp->type == TARGET_CLASS && specp->access == SEQUENTIAL
	  && specp->where_key == NULL && specp->where_pred == NULL)
	{
	  if (lock_object (thread_p, &(specp->s.cls_node.cls_oid),
			   oid_Root_class_oid, X_LOCK,
			   LK_UNCOND_LOCK) != LK_GRANTED)
	    {
	      qexec_failure_line (__LINE__, xasl_state);
	      return ER_FAILED;
	    }
	}
    }

  if (qexec_execute_mainblock (thread_p, aptr, xasl_state) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }

  /* This guarantees that the result list file will have a type list.
     Copying a list_id structure fails unless it has a type list. */
  if (qexec_setup_list_id (xasl) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }

  /* Initialize operation type and other necessary things. */
  if (aptr->list_id->tuple_cnt > 1)
    {
      /* When multiple instances are updated,
       * statement level uniqueness checking should be performed.
       * In this case, uniqueness checking is performed by using statistical
       * information generated by the execution of UPDATE statement.
       */
      op_type = MULTI_ROW_UPDATE;

      /* If unique indexes do not exist, uniqueness checking is not needed. */
      if (update->has_uniques)
	{
	  num_unique_btrees = 0;
	  max_unique_btrees = UNIQUE_STAT_INFO_INCREMENT;

	  malloc_size =
	    sizeof (BTREE_UNIQUE_STATS) * UNIQUE_STAT_INFO_INCREMENT;
	  unique_stat_info =
	    (BTREE_UNIQUE_STATS *) db_private_alloc (thread_p, malloc_size);
	  if (unique_stat_info == NULL)
	    {
	      qexec_failure_line (__LINE__, xasl_state);
	      return ER_FAILED;
	    }
	}
    }
  else
    {				/* tuple_cnt <= 1 */
      /* When single instance is updated,
       * instance level uniqueness checking is performed.
       * In this case, uniqueness checking is performed by the server
       * when the key of the instance is inserted into an unique index.
       */
      op_type = SINGLE_ROW_UPDATE;
    }

  /* need to start a topop to ensure statement atomicity.
     One update statement might update several disk images.
     For example, one row update might update zero or more index keys,
     one heap record, and other things.
     So, the update statement must be performed atomically.
   */
  if (xtran_server_start_topop (thread_p, &lsa) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      db_private_free (thread_p, unique_stat_info);
      return ER_FAILED;
    }
  savepoint_used = 1;

  no_vals = update->no_vals;

  specp = xasl->spec_list;
  /* readonly_scan = true */
  if (qexec_open_scan (thread_p, specp, xasl->val_list, &xasl_state->vd, true,
		       specp->fixed_scan, specp->grouped_scan, true,
		       &specp->s_id) != NO_ERROR)
    {
      if (savepoint_used)
	{
	  xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
	}
      qexec_failure_line (__LINE__, xasl_state);
      db_private_free (thread_p, unique_stat_info);
      return ER_FAILED;
    }

  /* sub-class' partition information */
  adjmap = (char *) db_private_alloc (thread_p, update->no_classes);
  if (adjmap == NULL)
    {
      if (savepoint_used)
	{
	  xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
	}
      qexec_failure_line (__LINE__, xasl_state);
      db_private_free (thread_p, unique_stat_info);
      return ER_FAILED;
    }

  memset (adjmap, 0, update->no_classes);

  for (i1 = 0; i1 < update->no_classes; i1++)
    {
      if (!adjmap[i1] && update->partition[i1])
	{
	  adjmap[i1] = 1;
	  for (parts_idx = 0;
	       parts_idx < update->partition[i1]->no_parts; parts_idx++)
	    {
	      for (i2 = 0; i2 < update->no_classes; i2++)
		{
		  if (!adjmap[i2]
		      && OID_EQ (&update->class_oid[i2],
				 &update->partition[i1]->parts[parts_idx]->
				 class_oid))
		    {
		      update->partition[i2] = update->partition[i1];
		      if (update->partition[i1]->key_attr == -1)
			{	/* remove partition */
			  update->partition[i2]->parts[0]->class_oid = update->class_oid[i1];	/* save parent CLASS_OID */
			  update->partition[i2]->parts[0]->class_hfid =
			    update->class_hfid[i1];
			}
		      adjmap[i2] = 1;
		    }
		}
	    }
	}
    }
  db_private_free_and_init (thread_p, adjmap);

  tuple_cnt = 1;
  while ((xb_scan =
	  qexec_next_scan_block_iterations (thread_p, xasl)) == S_SUCCESS)
    {
      s_id = &xasl->curr_spec->s_id;
      while ((ls_scan = scan_next_scan (thread_p, s_id)) == S_SUCCESS)
	{
	  if (op_type == MULTI_ROW_UPDATE)
	    {
	      if (tuple_cnt == 1)
		{
		  repl_info = REPL_INFO_TYPE_STMT_START;
		}
	      else if (tuple_cnt == aptr->list_id->tuple_cnt)
		{
		  repl_info = REPL_INFO_TYPE_STMT_END;
		}
	      else
		{
		  repl_info = REPL_INFO_TYPE_STMT_NORMAL;
		}
	    }
	  else
	    {
	      repl_info = REPL_INFO_TYPE_STMT_NORMAL;
	    }
	  tuple_cnt++;

	  /* Append attribute expression values after the constants. */
	  for (k = 0, vallist = s_id->val_list->valp,
	       val_index = update->no_consts;
	       k < s_id->val_list->val_cnt; k++, vallist = vallist->next)
	    {
	      valp = vallist->val;
	      if (valp == NULL)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      if (k == 0)
		{
		  oid = DB_GET_OID (valp);
		}
	      else if (k == 1)
		{
		  class_oid = DB_GET_OID (valp);
		}
	      else
		{
		  update->consts[val_index++] = valp;
		}
	    }

	  /* evaluate constraint predicate */
	  satisfies_constraints = V_UNKNOWN;
	  if (update->cons_pred != NULL)
	    {
	      satisfies_constraints = eval_pred (thread_p, update->cons_pred,
						 &xasl_state->vd, NULL);
	      if (satisfies_constraints == V_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }

	  if (update->cons_pred != NULL && satisfies_constraints != V_TRUE)
	    {
	      /* currently there are only NOT NULL constraints */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_NULL_CONSTRAINT_VIOLATION, 0);
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (class_oid && !OID_EQ (&prev_class_oid, class_oid))
	    {
	      for (cl_index = 0, hfid = NULL;
		   cl_index < update->no_classes; ++cl_index)
		{
		  if (OID_EQ (&update->class_oid[cl_index], class_oid))
		    {
		      hfid = &update->class_hfid[cl_index];
		      break;
		    }
		}

	      if (hfid == NULL)
		{
		  /* matching class oid does not exist... error */
		  /* er_set(.....) needed */
		  er_log_debug (ARG_FILE_LINE,
				"qexec_execute_update: class OID is not correct\n");
		  GOTO_EXIT_ON_ERROR;
		}

	      if (!OID_ISNULL (&prev_class_oid) && attr_info_inited)
		{
		  (void) heap_attrinfo_end (thread_p, &attr_info);
		  attr_info_inited = 0;
		}

	      if (heap_attrinfo_start (thread_p, class_oid, -1, NULL,
				       &attr_info) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      attr_info_inited = 1;

	      if (scan_cache_inited)
		{
		  if (update->has_uniques && op_type == MULTI_ROW_UPDATE)
		    {
		      /* In this case, consider class hierarchy as well as single
		       * class.
		       * Therefore, construct the local statistical information
		       * by collecting the statistical information
		       * during scanning on each class of class hierarchy.
		       */
		      for (s = 0; s < scan_cache.num_btids; s++)
			{
			  temp_info = &(scan_cache.index_stat_info[s]);
			  if (temp_info->num_nulls == 0
			      && temp_info->num_keys == 0
			      && temp_info->num_oids == 0)
			    {
			      continue;
			    }
			  /* non-unique index would be filtered out at above
			   * statement. */

			  for (t = 0; t < num_unique_btrees; t++)
			    {
			      if (BTID_IS_EQUAL
				  (&temp_info->btid,
				   &unique_stat_info[t].btid))
				{
				  break;
				}
			    }
			  if (t < num_unique_btrees)
			    {
			      /* The same unique index has been found */
			      unique_stat_info[t].num_nulls +=
				temp_info->num_nulls;
			      unique_stat_info[t].num_keys +=
				temp_info->num_keys;
			      unique_stat_info[t].num_oids +=
				temp_info->num_oids;
			    }
			  else
			    {
			      /* The same unique index has not been found */
			      if (num_unique_btrees == max_unique_btrees)
				{
				  /* need more space for storing the local stat info */
				  max_unique_btrees +=
				    UNIQUE_STAT_INFO_INCREMENT;
				  malloc_size =
				    sizeof (BTREE_UNIQUE_STATS) *
				    max_unique_btrees;
				  ptr =
				    (char *) db_private_realloc (thread_p,
								 unique_stat_info,
								 malloc_size);
				  if (ptr == NULL)
				    {
				      GOTO_EXIT_ON_ERROR;
				    }
				  unique_stat_info =
				    (BTREE_UNIQUE_STATS *) ptr;
				}
			      t = num_unique_btrees;
			      BTID_COPY (&unique_stat_info[t].btid,
					 &temp_info->btid);
			      unique_stat_info[t].num_nulls =
				temp_info->num_nulls;
			      unique_stat_info[t].num_keys =
				temp_info->num_keys;
			      unique_stat_info[t].num_oids =
				temp_info->num_oids;
			      num_unique_btrees++;	/* increment */
			    }
			}
		    }
		  (void) locator_end_force_scan_cache (thread_p, &scan_cache);
		}
	      scan_cache_inited = false;

	      if (locator_start_force_scan_cache (thread_p, &scan_cache,
						  hfid, class_oid,
						  op_type) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      scan_cache_inited = true;

	      COPY_OID (&prev_class_oid, class_oid);
	    }

	  if (hfid == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (!HFID_IS_NULL (hfid))
	    {
	      if (heap_attrinfo_clear_dbvalues (&attr_info) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      for (k = 0; k < val_index; ++k)
		{
		  rc = heap_attrinfo_set (oid,
					  update->att_id[cl_index * no_vals +
							 k],
					  update->consts[k], &attr_info);
		  if (rc != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      force_count = 0;

	      partition_unchanged = 1;
	      if (update->partition[cl_index])
		{		/* partition adjust */
		  struct xasl_partition_info *p = update->partition[cl_index];

		  if (p->key_attr == -1)
		    {
		      /* partition remove mode */
		      if (locator_other_insert_delete (thread_p, hfid, oid,
						       &p->parts[0]->
						       class_hfid,
						       &p->parts[0]->
						       class_oid, &attr_info,
						       &scan_cache,
						       &force_count,
						       &prev_oid,
						       &new_reprid) !=
			  NO_ERROR)
			{
			  GOTO_EXIT_ON_ERROR;
			}
		      partition_unchanged = 0;
		    }
		  else
		    {
		      retidx = qexec_partition_chg_select (thread_p,
							   xasl_state,
							   &attr_info,
							   p, &rvsave);
		      if (retidx < 0)
			{
			  GOTO_EXIT_ON_ERROR;
			}

		      if (retidx-- > 0
			  && !OID_EQ (class_oid,
				      &p->parts[retidx]->class_oid))
			{
			  XASL_PARTS_INFO *parts = p->parts[retidx];

			  if (locator_other_insert_delete (thread_p, hfid,
							   oid,
							   &parts->class_hfid,
							   &parts->class_oid,
							   &attr_info,
							   &scan_cache,
							   &force_count,
							   &prev_oid,
							   &new_reprid)
			      != NO_ERROR)
			    {
			      GOTO_EXIT_ON_ERROR;
			    }
			  partition_unchanged = 0;
			}
		    }
		}

	      if (partition_unchanged)
		{
		  int idx = cl_index * no_vals;

		  if (locator_attribute_info_force (thread_p, hfid, oid, 0,
						    &attr_info,
						    &update->att_id[idx],
						    val_index,
						    LC_FLUSH_UPDATE, op_type,
						    &scan_cache, &force_count,
						    false,
						    repl_info) != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      /* Instances are not put into the result list file, but are counted. */
	      if (force_count)
		{
		  xasl->list_id->tuple_cnt++;
		}
	    }
	}
      if (ls_scan != S_END)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }
  if (xb_scan != S_END)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* sub-class' partition information clear */
  for (i1 = 0; i1 < update->no_classes; i1++)
    {
      if (update->partition[i1])
	{
	  for (i2 = i1 + 1; i2 < update->no_classes; i2++)
	    {
	      if (update->partition[i1] == update->partition[i2])
		{
		  update->partition[i2] = NULL;
		}
	    }
	}
    }

  /* check uniques */
  if (update->has_uniques && op_type == MULTI_ROW_UPDATE && scan_cache_inited)
    {
      /* In this case, consider class hierarchy as well as single class.
       * Therefore, construct the local statistical information
       * by collecting the statistical information
       * during scanning on each class of class hierarchy.
       */
      for (s = 0; s < scan_cache.num_btids; s++)
	{
	  temp_info = &(scan_cache.index_stat_info[s]);
	  if (temp_info->num_nulls == 0 && temp_info->num_keys == 0
	      && temp_info->num_oids == 0)
	    {
	      continue;
	    }
	  /* non-unique index would be filtered out at above statement. */

	  for (t = 0; t < num_unique_btrees; t++)
	    {
	      if (BTID_IS_EQUAL (&temp_info->btid, &unique_stat_info[t].btid))
		{
		  break;
		}
	    }
	  if (t < num_unique_btrees)
	    {
	      /* The same unique index has been found */
	      unique_stat_info[t].num_nulls += temp_info->num_nulls;
	      unique_stat_info[t].num_keys += temp_info->num_keys;
	      unique_stat_info[t].num_oids += temp_info->num_oids;
	    }
	  else
	    {
	      /* The same unique index has not been found */
	      if (num_unique_btrees == max_unique_btrees)
		{
		  /* need more space for storing the local stat info */
		  max_unique_btrees += UNIQUE_STAT_INFO_INCREMENT;
		  malloc_size =
		    sizeof (BTREE_UNIQUE_STATS) * max_unique_btrees;
		  ptr =
		    (char *) db_private_realloc (thread_p, unique_stat_info,
						 malloc_size);
		  if (ptr == NULL)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		  unique_stat_info = (BTREE_UNIQUE_STATS *) ptr;
		}
	      t = num_unique_btrees;
	      BTID_COPY (&unique_stat_info[t].btid, &temp_info->btid);
	      unique_stat_info[t].num_nulls = temp_info->num_nulls;
	      unique_stat_info[t].num_keys = temp_info->num_keys;
	      unique_stat_info[t].num_oids = temp_info->num_oids;
	      num_unique_btrees++;	/* increment */
	    }
	}

      /* When uniqueness checking based on each local statistical information
       * turns out as valid, the local statistical information must be
       * reflected into the global statistical information kept in the
       * root page of corresponding unique index.
       */
      for (s = 0; s < num_unique_btrees; s++)
	{
	  /* If local statistical information is not valid, skip it. */
	  if (unique_stat_info[s].num_nulls == 0
	      && unique_stat_info[s].num_keys == 0
	      && unique_stat_info[s].num_oids == 0)
	    {
	      continue;		/* no modification : non-unique index */
	    }

	  /* uniqueness checking based on local statistical information */
	  if ((unique_stat_info[s].num_nulls + unique_stat_info[s].num_keys)
	      != unique_stat_info[s].num_oids)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_BTREE_UNIQUE_FAILED, 0);
	      GOTO_EXIT_ON_ERROR;
	    }

	  /* (num_nulls + num_keys) == num_oids */
	  /* reflect the local information into the global information. */
	  if (btree_reflect_unique_statistics (thread_p, &unique_stat_info[s])
	      != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

  qexec_close_scan (thread_p, specp);
  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }

  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }

  if (unique_stat_info != NULL)
    {
      db_private_free_and_init (thread_p, unique_stat_info);
    }

  if (savepoint_used)
    {
      if (xtran_server_end_topop
	  (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER, &lsa) != TRAN_ACTIVE)
	{
	  qexec_failure_line (__LINE__, xasl_state);
	  return ER_FAILED;
	}
    }
  /* release all locks if the hint was given */
  if (update->release_lock)
    {
      lock_unlock_all (thread_p);
    }

#if 0				/* yaw */
  /* remove query result cache entries which are relevant with this class */
  class_oid = update->class_oid;
  if (!QFILE_IS_LIST_CACHE_DISABLED && class_oid)
    {
      for (s = 0; s < update->no_classes; s++, class_oid++)
	{
	  if (qexec_clear_list_cache_by_class (class_oid) != NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "qexec_execute_update: qexec_clear_list_cache_by_class failed for"
			    " class { %d %d %d }\n",
			    class_oid->pageid, class_oid->slotid,
			    class_oid->volid);
	    }
	}
      qmgr_add_modified_class (class_oid);
    }
#endif

  return NO_ERROR;

exit_on_error:
  /* sub-class' partition information clear */
  for (i1 = 0; i1 < update->no_classes; i1++)
    {
      if (update->partition[i1])
	{
	  for (i2 = i1 + 1; i2 < update->no_classes; i2++)
	    {
	      if (update->partition[i1] == update->partition[i2])
		{
		  update->partition[i2] = NULL;
		}
	    }
	}
    }

  qexec_end_scan (thread_p, specp);
  qexec_close_scan (thread_p, specp);
  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }
  if (unique_stat_info != NULL)
    {
      db_private_free_and_init (thread_p, unique_stat_info);
    }
  if (savepoint_used)
    {
      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
    }
  return ER_FAILED;
}

/*
 * qexec_execute_delete () -
 *   return: NO_ERROR or ER_code
 *   xasl(in)   : XASL Tree block
 *   xasl_state(in)     :
 */
static int
qexec_execute_delete (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		      XASL_STATE * xasl_state)
{
#define MIN_NUM_ROWS_FOR_MULTI_DELETE   20

  DELETE_PROC_NODE *delete_ = &xasl->proc.delete_;
  SCAN_CODE xb_scan;
  SCAN_CODE ls_scan;
  XASL_NODE *aptr;
  DB_VALUE *valp;
  OID *oid = NULL;
  OID *class_oid = NULL;
  OID prev_class_oid;
  HFID *hfid = NULL;
  ACCESS_SPEC_TYPE *specp = NULL;
  SCAN_ID *s_id;
  LOG_LSA lsa;
  int savepoint_used = 0;
  HEAP_SCANCACHE scan_cache;
  bool scan_cache_inited = false;
  int cl_index;
  int force_count;
  int op_type;
  int s, t;
  int num_unique_btrees, max_unique_btrees;
  int malloc_size;
  BTREE_UNIQUE_STATS *unique_stat_info = NULL;
  BTREE_UNIQUE_STATS *temp_info;
  char *ptr;


  OID_SET_NULL (&prev_class_oid);

  aptr = xasl->aptr_list;

  for (specp = aptr->spec_list; specp; specp = specp->next)
    {
      if (specp->type == TARGET_CLASS && specp->access == SEQUENTIAL
	  && specp->where_key == NULL && specp->where_pred == NULL)
	{
	  if (lock_object (thread_p, &(specp->s.cls_node.cls_oid),
			   oid_Root_class_oid, X_LOCK,
			   LK_UNCOND_LOCK) != LK_GRANTED)
	    {
	      qexec_failure_line (__LINE__, xasl_state);
	      return ER_FAILED;
	    }
	}
    }

  if (qexec_execute_mainblock (thread_p, aptr, xasl_state) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }

  /* This guarantees that the result list file will have a type list.
     Copying a list_id structure fails unless it has a type list. */
  if ((qexec_setup_list_id (xasl) != NO_ERROR)
      || (aptr->list_id->type_list.type_cnt != 2))
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }

  /* Initialize operation type and other necessary things. */
  if (aptr->list_id->tuple_cnt > MIN_NUM_ROWS_FOR_MULTI_DELETE)
    {
      op_type = MULTI_ROW_DELETE;

      num_unique_btrees = 0;
      max_unique_btrees = UNIQUE_STAT_INFO_INCREMENT;

      malloc_size = sizeof (BTREE_UNIQUE_STATS) * UNIQUE_STAT_INFO_INCREMENT;
      unique_stat_info = (BTREE_UNIQUE_STATS *) db_private_alloc (thread_p,
								  malloc_size);
      if (unique_stat_info == NULL)
	{
	  qexec_failure_line (__LINE__, xasl_state);
	  return ER_FAILED;
	}
    }
  else
    {
      /* When the number of instances to be deleted is small,
       * SINGLE_ROW_DELETE operation would be better, I guess..
       */
      op_type = SINGLE_ROW_DELETE;
    }

  /* need to start a topop to ensure statement atomicity.
   * One delete statement might update several disk images.
   * For example, one row delete might update
   * zero or more index keys, one heap record,
   * catalog info of object count, and other things.
   * So, the delete statement must be performed atomically.
   */
  if (xtran_server_start_topop (thread_p, &lsa) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      db_private_free (thread_p, unique_stat_info);
      return ER_FAILED;
    }
  savepoint_used = 1;

  specp = xasl->spec_list;
  /* readonly_scan = true */
  if (qexec_open_scan (thread_p, specp, xasl->val_list,
		       &xasl_state->vd, true,
		       specp->fixed_scan,
		       specp->grouped_scan, true, &specp->s_id) != NO_ERROR)
    {
      if (savepoint_used)
	{
	  xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
	}
      qexec_failure_line (__LINE__, xasl_state);
      db_private_free (thread_p, unique_stat_info);
      return ER_FAILED;
    }

  while ((xb_scan =
	  qexec_next_scan_block_iterations (thread_p, xasl)) == S_SUCCESS)
    {

      s_id = &xasl->curr_spec->s_id;
      while ((ls_scan = scan_next_scan (thread_p, s_id)) == S_SUCCESS)
	{

	  valp = s_id->val_list->valp->val;
	  if (valp == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  oid = DB_GET_OID (valp);
	  valp = s_id->val_list->valp->next->val;
	  if (valp == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  class_oid = DB_GET_OID (valp);

	  if (!OID_EQ (&prev_class_oid, class_oid))
	    {
	      for (cl_index = 0, hfid = NULL;
		   cl_index < delete_->no_classes; ++cl_index)
		{
		  if (OID_EQ (&delete_->class_oid[cl_index], class_oid))
		    {
		      hfid = &delete_->class_hfid[cl_index];
		      break;
		    }
		}

	      if (hfid == NULL)
		{
		  /* class oid does not exist... error */
		  /* er_set(...) needed */
		  er_log_debug (ARG_FILE_LINE,
				"qexec_execute_delete: class OID is not correct\n");
		  GOTO_EXIT_ON_ERROR;
		}

	      if (scan_cache_inited)
		{
		  if (op_type == MULTI_ROW_DELETE)
		    {
		      /* In this case, consider class hierarchy as well as single
		       * class.
		       * Therefore, construct the local statistical information
		       * by collecting the statistical information
		       * during scanning on each class of class hierarchy.
		       */
		      for (s = 0; s < scan_cache.num_btids; s++)
			{
			  temp_info = &(scan_cache.index_stat_info[s]);
			  if (temp_info->num_nulls == 0
			      && temp_info->num_keys == 0
			      && temp_info->num_oids == 0)
			    {
			      continue;
			    }
			  /* non-unique index would be filtered out at above statement. */

			  for (t = 0; t < num_unique_btrees; t++)
			    {
			      if (BTID_IS_EQUAL
				  (&temp_info->btid,
				   &unique_stat_info[t].btid))
				{
				  break;
				}
			    }
			  if (t < num_unique_btrees)
			    {
			      /* The same unique index has been found */
			      unique_stat_info[t].num_nulls +=
				temp_info->num_nulls;
			      unique_stat_info[t].num_keys +=
				temp_info->num_keys;
			      unique_stat_info[t].num_oids +=
				temp_info->num_oids;
			    }
			  else
			    {
			      /* The same unique index has not been found */
			      if (num_unique_btrees == max_unique_btrees)
				{
				  /* need more space for storing the local stat info */
				  max_unique_btrees +=
				    UNIQUE_STAT_INFO_INCREMENT;
				  malloc_size =
				    sizeof (BTREE_UNIQUE_STATS) *
				    max_unique_btrees;
				  ptr =
				    (char *) db_private_realloc (thread_p,
								 unique_stat_info,
								 malloc_size);
				  if (ptr == NULL)
				    {
				      GOTO_EXIT_ON_ERROR;
				    }
				  unique_stat_info =
				    (BTREE_UNIQUE_STATS *) ptr;
				}
			      t = num_unique_btrees;
			      BTID_COPY (&unique_stat_info[t].btid,
					 &temp_info->btid);
			      unique_stat_info[t].num_nulls =
				temp_info->num_nulls;
			      unique_stat_info[t].num_keys =
				temp_info->num_keys;
			      unique_stat_info[t].num_oids =
				temp_info->num_oids;
			      num_unique_btrees++;	/* increment */
			    }
			}	/* for */
		    }
		  (void) locator_end_force_scan_cache (thread_p, &scan_cache);
		}
	      scan_cache_inited = false;

	      if (locator_start_force_scan_cache (thread_p, &scan_cache, hfid,
						  class_oid, op_type)
		  != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      scan_cache_inited = true;

	      COPY_OID (&prev_class_oid, class_oid);
	    }

	  force_count = 0;
	  if (locator_attribute_info_force
	      (thread_p, hfid, oid, 0, NULL, NULL, 0, LC_FLUSH_DELETE,
	       op_type, &scan_cache, &force_count, false,
	       REPL_INFO_TYPE_STMT_NORMAL) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (force_count)
	    {
	      xasl->list_id->tuple_cnt++;
	    }
	}
      if (ls_scan != S_END)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }
  if (xb_scan != S_END)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* reflect local statistical information into the root page */
  if (op_type == MULTI_ROW_DELETE && scan_cache_inited)
    {
      /* In this case, consider class hierarchy as well as single class.
       * Therefore, construct the local statistical information
       * by collecting the statistical information
       * during scanning on each class of class hierarchy.
       */
      for (s = 0; s < scan_cache.num_btids; s++)
	{
	  temp_info = &(scan_cache.index_stat_info[s]);
	  if (temp_info->num_nulls == 0 && temp_info->num_keys == 0
	      && temp_info->num_oids == 0)
	    {
	      continue;
	    }
	  /* non-unique index would be filtered out at above statement. */

	  for (t = 0; t < num_unique_btrees; t++)
	    {
	      if (BTID_IS_EQUAL (&temp_info->btid, &unique_stat_info[t].btid))
		{
		  break;
		}
	    }
	  if (t < num_unique_btrees)
	    {
	      /* The same unique index has been found */
	      unique_stat_info[t].num_nulls += temp_info->num_nulls;
	      unique_stat_info[t].num_keys += temp_info->num_keys;
	      unique_stat_info[t].num_oids += temp_info->num_oids;
	    }
	  else
	    {
	      /* The same unique index has not been found */
	      if (num_unique_btrees == max_unique_btrees)
		{
		  /* need more space for storing unique index stat info */
		  max_unique_btrees += UNIQUE_STAT_INFO_INCREMENT;
		  malloc_size =
		    sizeof (BTREE_UNIQUE_STATS) * max_unique_btrees;
		  ptr =
		    (char *) db_private_realloc (thread_p, unique_stat_info,
						 malloc_size);
		  if (ptr == NULL)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		  unique_stat_info = (BTREE_UNIQUE_STATS *) ptr;
		}
	      t = num_unique_btrees;
	      BTID_COPY (&unique_stat_info[t].btid, &temp_info->btid);
	      unique_stat_info[t].num_nulls = temp_info->num_nulls;
	      unique_stat_info[t].num_keys = temp_info->num_keys;
	      unique_stat_info[t].num_oids = temp_info->num_oids;
	      num_unique_btrees++;	/* increment */
	    }
	}
      /* reflect local statistical information into the root page. */
      for (s = 0; s < num_unique_btrees; s++)
	{
	  if (unique_stat_info[s].num_nulls == 0
	      && unique_stat_info[s].num_keys == 0
	      && unique_stat_info[s].num_oids == 0)
	    {
	      continue;		/* no modification : non-unique index */
	    }
	  if (btree_reflect_unique_statistics (thread_p, &unique_stat_info[s])
	      != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

  qexec_close_scan (thread_p, specp);

  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }

  if (unique_stat_info != NULL)
    {
      db_private_free_and_init (thread_p, unique_stat_info);
    }

  if (savepoint_used)
    {
      if (xtran_server_end_topop
	  (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER, &lsa) != TRAN_ACTIVE)
	{
	  qexec_failure_line (__LINE__, xasl_state);
	  return ER_FAILED;
	}
    }
  /* release all locks if the hint was given */
  if (delete_->release_lock)
    {
      lock_unlock_all (thread_p);
    }

#if 0				/* yaw */
  /* remove query result cache entries which are relevant with this class */
  class_oid = XASL_DELETE_CLASS_OID (xasl);
  if (!QFILE_IS_LIST_CACHE_DISABLED && class_oid)
    {
      for (s = 0; s < XASL_DELETE_NO_CLASSES (xasl); s++, class_oid++)
	{
	  if (qexec_clear_list_cache_by_class (class_oid) != NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "qexec_execute_delete: qexec_clear_list_cache_by_class failed for"
			    " class { %d %d %d }\n",
			    class_oid->pageid, class_oid->slotid,
			    class_oid->volid);
	    }
	}
      qmgr_add_modified_class (class_oid);
    }
#endif

  return NO_ERROR;

exit_on_error:
  qexec_end_scan (thread_p, specp);
  qexec_close_scan (thread_p, specp);

  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }

  if (unique_stat_info != NULL)
    {
      db_private_free_and_init (thread_p, unique_stat_info);
    }

  if (savepoint_used)
    {
      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
    }
  return ER_FAILED;
}


/*
 * qexec_remove_duplicates_for_replace () - Removes the objects that would
 *       generate unique index violations when inserting the given attr_info
 *       (This is used for executing REPLACE statements)
 *   return: NO_ERROR or ER_code
 *   scan_cache(in):
 *   attr_info(in/out): The attribute information that will be inserted
 *   index_attr_info(in/out):
 *   idx_info(in):
 *   partition_info(in/out):
 */
static int
qexec_remove_duplicates_for_replace (THREAD_ENTRY * thread_p,
				     HEAP_SCANCACHE * scan_cache,
				     HEAP_CACHE_ATTRINFO * attr_info,
				     HEAP_CACHE_ATTRINFO * index_attr_info,
				     const HEAP_IDX_ELEMENTS_INFO * idx_info,
				     XASL_PARTITION_INFO * partition_info)
{
  LC_COPYAREA *copyarea = NULL;
  RECDES new_recdes;
  int i = 0;
  int error_code = NO_ERROR;
  char buf[DBVAL_BUFSIZE + MAX_ALIGNMENT];
  char *const aligned_buf = PTR_ALIGN (buf, MAX_ALIGNMENT);
  DB_VALUE dbvalue;
  DB_VALUE *key_dbvalue = NULL;
  int force_count = 0;

  if (heap_attrinfo_clear_dbvalues (index_attr_info) != NO_ERROR)
    {
      goto error_exit;
    }

  copyarea = locator_allocate_copy_area_by_attr_info (thread_p, attr_info,
						      NULL, &new_recdes, -1);
  if (copyarea == NULL)
    {
      goto error_exit;
    }

  if (idx_info->has_single_col)
    {
      error_code = heap_attrinfo_read_dbvalues (thread_p, &oid_Null_oid,
						&new_recdes, index_attr_info);
      if (error_code != NO_ERROR)
	{
	  goto error_exit;
	}
    }
  assert (index_attr_info->last_classrepr != NULL);

  for (i = 0; i < idx_info->num_btids; ++i)
    {
      BTID btid;
      OR_INDEX *const index = &(index_attr_info->last_classrepr->indexes[i]);
      OID unique_oid;
      OID class_oid;

      COPY_OID (&class_oid, &attr_info->class_oid);

      if (!btree_is_unique_type (index->type))
	{
	  continue;
	}

      key_dbvalue = heap_attrvalue_get_key (thread_p, i, index_attr_info,
					    &new_recdes, &btid, &dbvalue,
					    aligned_buf);
      /* TODO: unique with prefix length */

      if (key_dbvalue == NULL)
	{
	  goto error_exit;
	}

      if (xbtree_find_unique (thread_p, &index->btid, false, key_dbvalue,
			      &class_oid, &unique_oid, true)
	  == BTREE_KEY_FOUND)
	{
	  HFID class_hfid;

	  if (partition_info != NULL)
	    {
	      int partidx = 0;
	      bool found_partition = false;
	      if (heap_get_class_oid (thread_p, &unique_oid, &class_oid)
		  == NULL)
		{
		  goto error_exit;
		}
	      for (partidx = 0; partidx < partition_info->no_parts; ++partidx)
		{
		  XASL_PARTS_INFO *crt_part_info =
		    partition_info->parts[partidx];
		  if (OID_EQ (&class_oid, &crt_part_info->class_oid))
		    {
		      found_partition = true;
		      HFID_COPY (&class_hfid, &crt_part_info->class_hfid);
		      break;
		    }
		}
	      if (!found_partition)
		{
		  goto error_exit;
		}
	    }
	  else
	    {
	      HFID_COPY (&class_hfid, &scan_cache->hfid);
	    }

	  /* xbtree_find_unique () has set an U_LOCK on the instance. We need
	   * to get an X_LOCK in order to perform the delete.
	   */
	  if (lock_object (thread_p, &unique_oid, &class_oid, X_LOCK,
			   LK_UNCOND_LOCK) != LK_GRANTED)
	    {
	      goto error_exit;
	    }

	  force_count = 0;
	  if (locator_attribute_info_force (thread_p, &class_hfid,
					    &unique_oid, 0, NULL, NULL, 0,
					    LC_FLUSH_DELETE, MULTI_ROW_DELETE,
					    scan_cache, &force_count, false,
					    REPL_INFO_TYPE_STMT_NORMAL)
	      != NO_ERROR)
	    {
	      goto error_exit;
	    }
	  if (force_count != 0)
	    {
	      assert (force_count == 1);
	      /* MySQL has the special mysql_affected_rows() function
	       * that would return the total number of affected rows.
	       * CUBRID does not yet implement that function.
	       */
	      force_count = 0;
	    }
	}
      if (key_dbvalue == &dbvalue)
	{
	  pr_clear_value (&dbvalue);
	  key_dbvalue = NULL;
	}
    }

  if (copyarea != NULL)
    {
      locator_free_copy_area (copyarea);
      copyarea = NULL;
      new_recdes.data = NULL;
      new_recdes.area_size = 0;
    }

  return NO_ERROR;

error_exit:
  if (key_dbvalue == &dbvalue)
    {
      pr_clear_value (&dbvalue);
      key_dbvalue = NULL;
    }
  if (copyarea != NULL)
    {
      locator_free_copy_area (copyarea);
      copyarea = NULL;
      new_recdes.data = NULL;
      new_recdes.area_size = 0;
    }
  return ER_FAILED;
}

/*
 * qexec_oid_of_duplicate_key_update () - Finds an OID of an object that would
 *       generate unique index violations when inserting the given attr_info
 *       (This is used for executing INSERT ON DUPLICATE KEY UPDATE
 *        statements)
 *   return: NO_ERROR or ER_code
 *   scan_cache(in):
 *   attr_info(in/out): The attribute information that will be inserted
 *   index_attr_info(in/out):
 *   idx_info(in):
 *   unique_oid_p(out): the OID of one object to be updated or a NULL OID if
 *                      there are no potential unique index violations
 * Note: A single OID is returned even if there are several objects that would
 *       generate unique index violations (this can only happen if there are
 *       several unique indexes).
 */
static int
qexec_oid_of_duplicate_key_update (THREAD_ENTRY * thread_p,
				   HEAP_SCANCACHE * scan_cache,
				   HEAP_CACHE_ATTRINFO * attr_info,
				   HEAP_CACHE_ATTRINFO * index_attr_info,
				   const HEAP_IDX_ELEMENTS_INFO * idx_info,
				   OID * unique_oid_p)
{
  LC_COPYAREA *copyarea = NULL;
  RECDES new_recdes;
  int i = 0;
  int error_code = NO_ERROR;
  char buf[DBVAL_BUFSIZE + MAX_ALIGNMENT];
  char *const aligned_buf = PTR_ALIGN (buf, MAX_ALIGNMENT);
  DB_VALUE dbvalue;
  DB_VALUE *key_dbvalue = NULL;
  bool found_duplicate = false;

  OID_SET_NULL (unique_oid_p);

  if (heap_attrinfo_clear_dbvalues (index_attr_info) != NO_ERROR)
    {
      goto error_exit;
    }

  copyarea = locator_allocate_copy_area_by_attr_info (thread_p, attr_info,
						      NULL, &new_recdes, -1);
  if (copyarea == NULL)
    {
      goto error_exit;
    }

  if (idx_info->has_single_col)
    {
      error_code = heap_attrinfo_read_dbvalues (thread_p, &oid_Null_oid,
						&new_recdes, index_attr_info);
      if (error_code != NO_ERROR)
	{
	  goto error_exit;
	}
    }
  assert (index_attr_info->last_classrepr != NULL);

  for (i = 0; i < idx_info->num_btids && !found_duplicate; ++i)
    {
      BTID btid;
      OR_INDEX *const index = &(index_attr_info->last_classrepr->indexes[i]);
      OID unique_oid;
      OID class_oid;

      COPY_OID (&class_oid, &attr_info->class_oid);

      if (!btree_is_unique_type (index->type))
	{
	  continue;
	}

      key_dbvalue = heap_attrvalue_get_key (thread_p, i, index_attr_info,
					    &new_recdes, &btid, &dbvalue,
					    aligned_buf);
      if (key_dbvalue == NULL)
	{
	  goto error_exit;
	}

      if (xbtree_find_unique (thread_p, &index->btid, false, key_dbvalue,
			      &class_oid, &unique_oid, true)
	  == BTREE_KEY_FOUND)
	{
	  /* We now hold an U_LOCK on the instance. It will be upgraded to an
	   * X_LOCK when the update is executed.
	   */
	  found_duplicate = true;
	  COPY_OID (unique_oid_p, &unique_oid);
	}
      if (key_dbvalue == &dbvalue)
	{
	  pr_clear_value (&dbvalue);
	  key_dbvalue = NULL;
	}
    }

  if (copyarea != NULL)
    {
      locator_free_copy_area (copyarea);
      copyarea = NULL;
      new_recdes.data = NULL;
      new_recdes.area_size = 0;
    }

  return NO_ERROR;

error_exit:
  if (key_dbvalue == &dbvalue)
    {
      pr_clear_value (&dbvalue);
      key_dbvalue = NULL;
    }
  if (copyarea != NULL)
    {
      locator_free_copy_area (copyarea);
      copyarea = NULL;
      new_recdes.data = NULL;
      new_recdes.area_size = 0;
    }
  return ER_FAILED;
}

/*
 * qexec_fill_oid_of_duplicate_key () - Fills in the required values for the
 *       execution of the ON DUPLICATE KEY UPDATE XASL node
 *   return: NO_ERROR or ER_code
 *   xasl(in):
 *   xasl_state(in/out):
 *   unique_oid(in): the OID of the object that will be updated
 * Note: Also see pt_dup_key_update_stmt ()
 */
static int
qexec_fill_oid_of_duplicate_key (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				 XASL_STATE * xasl_state, OID * unique_oid)
{
  XASL_NODE *update_xasl = NULL;
  XASL_NODE *scan_xasl = NULL;
  int oid_index = -1;

  if (xasl == NULL || xasl->type != INSERT_PROC)
    {
      goto error_exit;
    }

  oid_index = xasl->proc.insert.dup_key_oid_var_index;
  if (oid_index < 0 || oid_index >= xasl_state->vd.dbval_cnt)
    {
      goto error_exit;
    }

  update_xasl = xasl->dptr_list;
  if (update_xasl == NULL || update_xasl->type != UPDATE_PROC ||
      update_xasl->next != NULL)
    {
      goto error_exit;
    }

  scan_xasl = update_xasl->aptr_list;
  if (scan_xasl == NULL || scan_xasl->type != BUILDLIST_PROC ||
      scan_xasl->next != NULL)
    {
      goto error_exit;
    }

  DB_MAKE_OID (&xasl_state->vd.dbval_ptr[oid_index], unique_oid);

  return NO_ERROR;

error_exit:
  return ER_FAILED;
}

/*
 * qexec_execute_duplicate_key_update () - Executes an update on a given OID
 *       (required by INSERT ON DUPLICATE KEY UPDATE processing)
 *   return: NO_ERROR or ER_code
 *   xasl(in):
 *   xasl_state(in/out):
 *   unique_oid(in): the OID of the object that will be updated
 *   force_count(out): the number of objects that have been updated; it should
 *                     always be 1 on success and 0 on error
 */
static int
qexec_execute_duplicate_key_update (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				    XASL_STATE * xasl_state, OID * unique_oid,
				    int *force_count)
{
  XASL_NODE *const update_xasl = xasl->dptr_list;
  int stat = NO_ERROR;

  *force_count = 0;

  if (update_xasl == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (qexec_fill_oid_of_duplicate_key (thread_p, xasl, xasl_state, unique_oid)
      != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  update_xasl->query_in_progress = true;
  stat = qexec_execute_mainblock (thread_p, update_xasl, xasl_state);
  update_xasl->query_in_progress = false;

  if (stat != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  if (update_xasl->list_id->tuple_cnt != 1)
    {
      GOTO_EXIT_ON_ERROR;
    }

  *force_count = 1;
  (void) qexec_clear_xasl (thread_p, update_xasl, false);

  return NO_ERROR;

exit_on_error:
  (void) qexec_clear_xasl (thread_p, update_xasl, true);
  return ER_FAILED;
}

/*
 * qexec_execute_insert () -
 *   return: NO_ERROR or ER_code
 *   xasl(in)   : XASL Tree block
 *   xasl_state(in)     :
 *   remote_insert(in):
 */
int
qexec_execute_insert (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		      XASL_STATE * xasl_state, bool remote_insert)
{
  INSERT_PROC_NODE *insert = &xasl->proc.insert;
  SCAN_CODE xb_scan;
  SCAN_CODE ls_scan;
  XASL_NODE *aptr;
  DB_VALUE *valp;
  QPROC_DB_VALUE_LIST vallist;
  int k;
  int val_no;
  int rc;
  OID oid;
  OID class_oid;
  ACCESS_SPEC_TYPE *specp = NULL;
  SCAN_ID *s_id;
  HEAP_CACHE_ATTRINFO attr_info;
  bool attr_info_inited = false;
  HEAP_CACHE_ATTRINFO index_attr_info;
  HEAP_IDX_ELEMENTS_INFO idx_info;
  bool index_attr_info_inited = false;
  LOG_LSA lsa;
  int savepoint_used = 0;
  int satisfies_constraints;
  HEAP_SCANCACHE scan_cache;
  bool scan_cache_inited = false;
  int scan_cache_op_type = 0;
  int force_count = 0;
  REGU_VARIABLE *rvsave = NULL;
  bool skip_insertion = false;
  int partition_node_id = 0;
  int flag = 0;			/* 0: local class, 1: not local class */

  aptr = xasl->aptr_list;
  val_no = insert->no_vals;


  if (aptr
      && qexec_execute_mainblock (thread_p, aptr, xasl_state) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }

  /* This guarantees that the result list file will have a type list.
     Copying a list_id structure fails unless it has a type list. */
  if (qexec_setup_list_id (xasl) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }

  /* need to start a topop to ensure statement atomicity.
     One insert statement might update several disk images.
     For example, one row insert might update
     one heap record, zero or more index keys,
     catalog info of object count, and other things.
     So, the insert statement must be performed atomically.
   */
  if (xtran_server_start_topop (thread_p, &lsa) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }
  savepoint_used = 1;

  COPY_OID (&class_oid, &insert->proxy_oid);
  if (insert->has_uniques && (insert->do_replace || xasl->dptr_list != NULL))
    {
      if (heap_attrinfo_start_with_index (thread_p, &class_oid, NULL,
					  &index_attr_info, &idx_info) < 0)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      index_attr_info_inited = true;
    }

  if (heap_attrinfo_start (thread_p, &class_oid, -1, NULL, &attr_info) !=
      NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  COPY_OID (&attr_info.class_oid, &insert->class_oid);
  attr_info_inited = true;

  specp = xasl->spec_list;
  if (specp || ((insert->do_replace || (xasl->dptr_list != NULL))
		&& insert->has_uniques))
    {
      scan_cache_op_type = MULTI_ROW_INSERT;
    }
  else
    {
      scan_cache_op_type = SINGLE_ROW_INSERT;
    }
  if (specp)
    {
      XASL_PARTS_INFO **parts = NULL;
      int i;

      if (insert->node_id != 0)
	{
	  flag = 1;
	}

      if (!flag && insert->partition)
	{
	  parts = insert->partition->parts;
	  for (i = 0; i < insert->partition->no_parts; i++)
	    {
	      if ((*parts)->node_id != 0)
		{
		  flag = 1;
		  break;
		}
	      parts++;
	    }
	}

      if (parts)
	{
	  parts = insert->partition->parts;
	}

      while (parts)
	{
	  if ((*parts)->node_id == 0)
	    {
	      class_oid.pageid = (*parts)->class_oid.pageid;
	      class_oid.slotid = (*parts)->class_oid.slotid;
	      class_oid.volid = (*parts)->class_oid.volid;

	      insert->class_hfid.hpgid = (*parts)->class_hfid.hpgid;
	      insert->class_hfid.vfid.fileid =
		(*parts)->class_hfid.vfid.fileid;
	      insert->class_hfid.vfid.volid = (*parts)->class_hfid.vfid.volid;

	      break;
	    }
	  else
	    {
	      parts++;
	    }
	}
      /* we are inserting multiple values ...
       * ie. insert into foo select ... */
      if (flag == 1 && parts == NULL)
	{
	  /* nothing */
	}
      else
	{
	  if (locator_start_force_scan_cache (thread_p, &scan_cache,
					      &insert->class_hfid, &class_oid,
					      scan_cache_op_type) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  scan_cache_inited = true;
	}

      /* readonly_scan = true */
      if (qexec_open_scan (thread_p, specp, xasl->val_list,
			   &xasl_state->vd, true,
			   specp->fixed_scan,
			   specp->grouped_scan, true,
			   &specp->s_id) != NO_ERROR)
	{
	  if (savepoint_used)
	    {
	      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
	    }
	  qexec_failure_line (__LINE__, xasl_state);
	  return ER_FAILED;
	}

      if (flag == 1 && parts == NULL)
	{
	  /* nothing */
	}
      else
	{
	  if (heap_hint_expected_num_objects (thread_p, &scan_cache,
					      specp->s.list_node.
					      xasl_node->list_id->tuple_cnt,
					      -1) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      while ((xb_scan = qexec_next_scan_block_iterations (thread_p,
							  xasl)) == S_SUCCESS)
	{
	  s_id = &xasl->curr_spec->s_id;
	  while ((ls_scan =
		  scan_next_scan_proxy (thread_p, xasl, xasl->curr_spec,
					1, 0)) == S_SUCCESS)
	    {
	      skip_insertion = false;

	      for (k = 0, vallist = s_id->val_list->valp;
		   k < val_no; k++, vallist = vallist->next)
		{
		  valp = vallist->val;
		  if (valp == NULL)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }

		  insert->vals[k] = valp;
		}

	      /* evaluate constraint predicate */
	      satisfies_constraints = V_UNKNOWN;
	      if (insert->cons_pred != NULL)
		{
		  satisfies_constraints = eval_pred (thread_p,
						     insert->cons_pred,
						     &xasl_state->vd, NULL);
		  if (satisfies_constraints == V_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      if (insert->cons_pred != NULL
		  && satisfies_constraints != V_TRUE)
		{
		  /* currently there are only NOT NULL constraints */
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_NULL_CONSTRAINT_VIOLATION, 0);
		  GOTO_EXIT_ON_ERROR;
		}

	      if (heap_attrinfo_clear_dbvalues (&attr_info) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      for (k = 0; k < val_no; ++k)
		{
		  if (DB_IS_NULL (insert->vals[k]))
		    {
		      OR_ATTRIBUTE *attr =
			heap_locate_last_attrepr (insert->att_id[k],
						  &attr_info);
		      if (attr == NULL)
			{
			  GOTO_EXIT_ON_ERROR;
			}

		      if (attr->is_autoincrement)
			{
			  continue;
			}
		    }
		  rc = heap_attrinfo_set (NULL, insert->att_id[k],
					  insert->vals[k], &attr_info);
		  if (rc != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      if (heap_set_autoincrement_value (thread_p, &attr_info,
						&scan_cache) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      if (insert->do_replace && insert->has_uniques)
		{
		  assert (index_attr_info_inited == true);
		  if (qexec_remove_duplicates_for_replace (thread_p,
							   &scan_cache,
							   &attr_info,
							   &index_attr_info,
							   &idx_info,
							   insert->
							   partition) !=
		      NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      if (xasl->dptr_list && insert->has_uniques)
		{
		  OID unique_oid;

		  assert (index_attr_info_inited == true);
		  OID_SET_NULL (&unique_oid);

		  if (qexec_oid_of_duplicate_key_update (thread_p,
							 &scan_cache,
							 &attr_info,
							 &index_attr_info,
							 &idx_info,
							 &unique_oid)
		      != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }

		  force_count = 0;
		  if (!OID_ISNULL (&unique_oid))
		    {
		      if (qexec_execute_duplicate_key_update (thread_p, xasl,
							      xasl_state,
							      &unique_oid,
							      &force_count)
			  != NO_ERROR)
			{
			  GOTO_EXIT_ON_ERROR;
			}
		    }
		  if (force_count)
		    {
		      assert (force_count == 1);
		      xasl->list_id->tuple_cnt += force_count * 2;
		      skip_insertion = true;
		    }
		}

	      if (skip_insertion)
		{
		  continue;
		}

	      if (insert->partition)
		{
		  OID partition_oid;
		  if (qexec_partition_select (thread_p, &insert->class_hfid,
					      &partition_oid,
					      &partition_node_id, xasl_state,
					      &attr_info, insert->partition,
					      &rvsave) != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}
	      else
		{
		  partition_node_id = insert->node_id;
		}

	      force_count = 0;
	      if (locator_attribute_info_force (thread_p, &insert->class_hfid,
						&oid, partition_node_id,
						&attr_info, NULL, 0,
						LC_FLUSH_INSERT,
						scan_cache_op_type,
						&scan_cache, &force_count,
						false,
						REPL_INFO_TYPE_STMT_NORMAL) !=
		  NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      /* Instances are not put into the result list file,
	       * but are counted. */
	      if (force_count)
		{
		  assert (force_count == 1);
		  xasl->list_id->tuple_cnt += force_count;
		}
	    }

	  if (ls_scan != S_END)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      if (xb_scan != S_END)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      qexec_close_scan (thread_p, specp);
    }
  else
    {
      /* we are inserting a single row
       * ie. insert into foo values(...) */
      REGU_VARIABLE_LIST regu_list = NULL;

      if (locator_start_force_scan_cache (thread_p, &scan_cache,
					  &insert->class_hfid, &class_oid,
					  scan_cache_op_type) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      scan_cache_inited = true;
      skip_insertion = false;

      if (xasl->outptr_list)
	{
	  regu_list = xasl->outptr_list->valptrp;
	  vallist = xasl->val_list->valp;
	  for (k = 0;
	       k < val_no;
	       k++, regu_list = regu_list->next, vallist = vallist->next)
	    {

	      if (fetch_peek_dbval (thread_p, &regu_list->value,
				    &xasl_state->vd, &class_oid, NULL, NULL,
				    &valp) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      if (!qdata_copy_db_value (vallist->val, valp))
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      insert->vals[k] = valp;
	    }
	}

      /* evaluate constraint predicate */
      satisfies_constraints = V_UNKNOWN;
      if (insert->cons_pred != NULL)
	{
	  satisfies_constraints = eval_pred (thread_p, insert->cons_pred,
					     &xasl_state->vd, NULL);
	  if (satisfies_constraints == V_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      if (insert->cons_pred != NULL && satisfies_constraints != V_TRUE)
	{
	  /* currently there are only NOT NULL constraints */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_NULL_CONSTRAINT_VIOLATION, 0);
	  GOTO_EXIT_ON_ERROR;
	}

      if (heap_attrinfo_clear_dbvalues (&attr_info) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      for (k = 0; k < val_no; ++k)
	{
	  if (DB_IS_NULL (insert->vals[k]))
	    {
	      OR_ATTRIBUTE *attr =
		heap_locate_last_attrepr (insert->att_id[k], &attr_info);
	      if (attr == NULL)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      if (attr->is_autoincrement)
		{
		  continue;
		}
	    }
	  rc = heap_attrinfo_set (NULL, insert->att_id[k],
				  insert->vals[k], &attr_info);
	  if (rc != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      if (heap_set_autoincrement_value (thread_p, &attr_info, &scan_cache) !=
	  NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (insert->do_replace && insert->has_uniques)
	{
	  assert (index_attr_info_inited == true);
	  if (qexec_remove_duplicates_for_replace (thread_p, &scan_cache,
						   &attr_info,
						   &index_attr_info,
						   &idx_info,
						   insert->partition)
	      != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      if (xasl->dptr_list && insert->has_uniques)
	{
	  OID unique_oid;

	  assert (index_attr_info_inited == true);
	  OID_SET_NULL (&unique_oid);

	  if (qexec_oid_of_duplicate_key_update (thread_p, &scan_cache,
						 &attr_info, &index_attr_info,
						 &idx_info, &unique_oid)
	      != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  force_count = 0;
	  if (!OID_ISNULL (&unique_oid))
	    {
	      if (qexec_execute_duplicate_key_update (thread_p, xasl,
						      xasl_state, &unique_oid,
						      &force_count)
		  != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	  if (force_count)
	    {
	      assert (force_count == 1);
	      xasl->list_id->tuple_cnt += force_count * 2;
	      skip_insertion = true;
	    }
	}

      if (!skip_insertion)
	{
	  if (insert->partition)
	    {
	      OID partition_oid;
	      if (qexec_partition_select (thread_p, &insert->class_hfid,
					  &partition_oid, &partition_node_id,
					  xasl_state, &attr_info,
					  insert->partition,
					  &rvsave) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }

	  force_count = 0;
	  if (locator_attribute_info_force (thread_p, &insert->class_hfid,
					    &oid, partition_node_id,
					    &attr_info, NULL, 0,
					    LC_FLUSH_INSERT,
					    scan_cache_op_type, &scan_cache,
					    &force_count, false,
					    REPL_INFO_TYPE_STMT_NORMAL) !=
	      NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  /* Instances are not put into the result list file, but are counted. */
	  if (force_count)
	    {
	      assert (force_count == 1);
	      xasl->list_id->tuple_cnt += force_count;
	    }
	}
    }

  /* check uniques */
  if (insert->has_uniques && scan_cache_op_type == MULTI_ROW_INSERT && !flag)
    {
      /* In this case, consider only single class.
       * Therefore, uniqueness checking is performed based on
       * the local statistical information kept in scan_cache.
       * And then, it is reflected into the global statistical information.
       */
      for (k = 0; k < scan_cache.num_btids; k++)
	{
	  if (scan_cache.index_stat_info[k].num_nulls == 0
	      && scan_cache.index_stat_info[k].num_keys == 0
	      && scan_cache.index_stat_info[k].num_oids == 0)
	    {
	      /* No modification to be done. Either a non-unique index or
	         the delete/insert operations cancelled each other out. */
	      continue;
	    }
	  else
	    {
	      assert (scan_cache.index_stat_info[k].num_nulls +
		      scan_cache.index_stat_info[k].num_keys ==
		      scan_cache.index_stat_info[k].num_oids);

	      if (btree_reflect_unique_statistics
		  (thread_p, &(scan_cache.index_stat_info[k])) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	}
    }

  if (index_attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &index_attr_info);
    }
  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }
  if (savepoint_used)
    {
      if (xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER,
				  &lsa) != TRAN_ACTIVE)
	{
	  qexec_failure_line (__LINE__, xasl_state);
	  return ER_FAILED;
	}
    }

  /* release all locks if the hint was given */
  if (insert->release_lock)
    {
      lock_unlock_all (thread_p);
    }

#if 0				/* yaw */
  /* remove query result cache entries which are relevant with this class */
  COPY_OID (&class_oid, &XASL_INSERT_CLASS_OID (xasl));
  if (!QFILE_IS_LIST_CACHE_DISABLED && !OID_ISNULL (&class_oid))
    {
      if (qexec_clear_list_cache_by_class (&class_oid) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_execute_insert: qexec_clear_list_cache_by_class failed for "
			"class { %d %d %d }\n",
			class_oid.pageid, class_oid.slotid, class_oid.volid);
	}
      qmgr_add_modified_class (&class_oid);
    }
#endif

  return NO_ERROR;

exit_on_error:
  qexec_end_scan (thread_p, specp);
  qexec_close_scan (thread_p, specp);
  if (index_attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &index_attr_info);
    }
  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }
  if (savepoint_used)
    {
      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
    }

  return ER_FAILED;
}

/*
 * qexec_execute_obj_set_fetch () -
 *   return: NO_ERROR or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL tree state information
 *
 * Note: This routines interpretes an XASL procedure block that denotes
 * an object/set fetch operation, one of the basic operations
 * of a path expression evaluation. A path expression evaluation
 * consists of fetching the objects linked in a path expression
 * and performing value fetch or predicate evalution operations
 * on them. A path expression evaluation successfully completes
 * if all the objects in the path are succesfully fetched and
 * qualified imposed predicates. Some of the objects in a path
 * expression may be non-existent. If that
 * happens, path expression evaluation succeeds and the value
 * list for the procedure block is filled with NULLs. A path
 * evaluation may consist of an ordered list of object_fetch,
 * set_fetch or x_fetch operations. To conform to the above
 * mentioned semantics, an object/set fetch may succeed or fail
 * depending whether the objects succesfully fetched and/or
 * qualified. The result of the object/set fetch operation is
 * indicated by setting a flag in the procedure block head node.
 * The objects to be fetched are indicated by object identifiers
 * already set in the procedure block head node.
 */
static int
qexec_execute_obj_set_fetch (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			     XASL_STATE * xasl_state)
{
  XASL_NODE *xptr;
  DB_LOGICAL ev_res;
  DB_LOGICAL ev_res2;
  RECDES oRec;
  HEAP_SCANCACHE scan_cache;
  ACCESS_SPEC_TYPE *specp = NULL;
  OID cls_oid;
  int dead_end = false;
  int unqualified_dead_end = false;
  FETCH_PROC_NODE *fetch = &xasl->proc.fetch;
  OID *dbvaloid;

  dbvaloid = DB_GET_OID (fetch->arg);

  /* the fetch_res represents whether current node in a path expression is
   * successfully completed to the end, or failed
   */
  fetch->fetch_res = false;

  /* check if corresponding object/set exists */
  if (xasl->type == OBJFETCH_PROC)
    {				/* an object fetch operation */
      /* check for virtual objects */
      if (!PRIM_IS_NULL (fetch->arg)
	  && PRIM_TYPE (fetch->arg) == DB_TYPE_VOBJ)
	{
	  DB_SET *setp = DB_GET_SET (fetch->arg);
	  DB_VALUE dbval, dbval1;

	  if ((db_set_size (setp) == 3)
	      && (db_set_get (setp, 1, &dbval) == NO_ERROR)
	      && (db_set_get (setp, 2, &dbval1) == NO_ERROR)
	      && (PRIM_IS_NULL (&dbval)
		  || ((PRIM_TYPE (&dbval) == DB_TYPE_OID)
		      && OID_ISNULL (DB_GET_OID (&dbval))))
	      && (PRIM_TYPE (&dbval1) == DB_TYPE_OID))
	    {
	      dbvaloid = DB_GET_OID (&dbval1);
	    }
	  else
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      /* object is non_existent ? */
      if (PRIM_IS_NULL (fetch->arg) || OID_ISNULL (dbvaloid))
	{
	  dead_end = true;
	}
    }
  else
    {				/* a set fetch operation */
      if (PRIM_IS_NULL (fetch->arg))
	{
	  return NO_ERROR;
	}
    }

  /*
   * Pre_processing
   */

  /* evaluate aptr list */
  for (xptr = xasl->aptr_list; xptr != NULL; xptr = xptr->next)
    {
      if (XASL_IS_FLAGED (xptr, XASL_LINK_TO_REGU_VARIABLE))
	{
	  /* skip if linked to regu var */
	  continue;
	}

      if (xptr->status == XASL_CLEARED)
	{
	  if (qexec_execute_mainblock (thread_p, xptr, xasl_state) !=
	      NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
      else
	{			/* already executed. success or failure */
	  if (xptr->status != XASL_SUCCESS)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

  /*
   * Processing
   */

  if (xasl->type == OBJFETCH_PROC)
    {				/* an object fetch operation */

      if (!dead_end)
	{
	  int cache_pred_end_needed = false;
	  int cache_rest_end_needed = false;
	  int scan_cache_end_needed = false;
	  int status = NO_ERROR;

	  /* Start heap file scan operation */
	  /* A new argument(is_indexscan = false) is appended */
	  (void) heap_scancache_start (thread_p, &scan_cache, NULL, NULL,
				       true, false, LOCKHINT_NONE);
	  scan_cache_end_needed = true;

	  /* fetch the object and the class oid */
	  if (heap_get_with_class_oid (thread_p, dbvaloid, &oRec, &scan_cache,
				       &cls_oid, PEEK) != S_SUCCESS)
	    {
	      if (er_errid () == ER_HEAP_UNKNOWN_OBJECT)
		{
		  /* dangling object reference */
		  dead_end = true;
		}

	      else if (er_errid () == ER_HEAP_NODATA_NEWADDRESS)
		{
		  dead_end = true;
		  unqualified_dead_end = true;
		  er_clear ();	/* clear ER_HEAP_NODATA_NEWADDRESS */
		}
	      else
		{
		  status = ER_FAILED;
		  goto wrapup;
		}
	    }
	  else
	    {
	      /* check to see if the object is one of the classes that
	       * we are interested in.  This can only fail if there was
	       * a selector variable in the query.  we can optimize this
	       * further to pass from the compiler whether this check
	       * is necessary or not.
	       */
	      for (specp = xasl->spec_list;
		   specp && specp->type == TARGET_CLASS
		   && (!OID_EQ (&ACCESS_SPEC_CLS_OID (specp), &cls_oid));
		   specp = specp->next)
		{
		  ;		/* NULL */
		}

	      if (specp == NULL)
		{
		  /* no specification contains the class oid, this is a
		   * possible situation for object domain definitions.
		   * It just causes the object fetch result to fail.
		   */
		  fetch->fetch_res = false;
		  dead_end = true;
		  unqualified_dead_end = true;
		}
	    }

	  if (!dead_end)
	    {
	      /* set up the attribute cache info */
	      status =
		heap_attrinfo_start (thread_p, &cls_oid,
				     specp->s.cls_node.num_attrs_pred,
				     specp->s.cls_node.attrids_pred,
				     specp->s.cls_node.cache_pred);
	      if (status != NO_ERROR)
		{
		  goto wrapup;
		}
	      cache_pred_end_needed = true;

	      status =
		heap_attrinfo_start (thread_p, &cls_oid,
				     specp->s.cls_node.num_attrs_rest,
				     specp->s.cls_node.attrids_rest,
				     specp->s.cls_node.cache_rest);
	      if (status != NO_ERROR)
		{
		  goto wrapup;
		}
	      cache_rest_end_needed = true;

	      fetch_init_val_list (specp->s.cls_node.cls_regu_list_pred);
	      fetch_init_val_list (specp->s.cls_node.cls_regu_list_rest);

	      /* read the predicate values from the heap into the scancache */
	      status =
		heap_attrinfo_read_dbvalues (thread_p, dbvaloid, &oRec,
					     specp->s.cls_node.cache_pred);
	      if (status != NO_ERROR)
		{
		  goto wrapup;
		}

	      /* fetch the values for the predicate from the object */
	      if (xasl->val_list != NULL)
		{
		  status =
		    fetch_val_list (thread_p,
				    specp->s.cls_node.cls_regu_list_pred,
				    &xasl_state->vd, NULL, dbvaloid, NULL,
				    COPY);
		  if (status != NO_ERROR)
		    {
		      goto wrapup;
		    }
		}

	      /* evaluate where predicate, if any */
	      ev_res = V_UNKNOWN;
	      if (specp->where_pred)
		{
		  ev_res = eval_pred (thread_p, specp->where_pred,
				      &xasl_state->vd, dbvaloid);
		  if (ev_res == V_ERROR)
		    {
		      status = ER_FAILED;
		      goto wrapup;
		    }
		}

	      if (specp->where_pred != NULL && ev_res != V_TRUE)
		{
		  /* the object is a disqualified one */
		  fetch->fetch_res = false;
		}
	      else
		{
		  /* the object is a qualified */
		  fetch->fetch_res = true;
		  /* read the rest of the values from the heap */
		  status = heap_attrinfo_read_dbvalues (thread_p, dbvaloid,
							&oRec,
							specp->s.cls_node.
							cache_rest);
		  if (status != NO_ERROR)
		    {
		      goto wrapup;
		    }

		  /* fetch the rest of the values from the object */
		  if (xasl->val_list != NULL)
		    {
		      status = fetch_val_list (thread_p, specp->s.cls_node.
					       cls_regu_list_rest,
					       &xasl_state->vd, NULL,
					       dbvaloid, NULL, COPY);
		      if (status != NO_ERROR)
			{
			  goto wrapup;
			}
		    }
		}
	    }

	wrapup:
	  if (cache_pred_end_needed)
	    {
	      heap_attrinfo_end (thread_p, specp->s.cls_node.cache_pred);
	    }
	  if (cache_rest_end_needed)
	    {
	      heap_attrinfo_end (thread_p, specp->s.cls_node.cache_rest);
	    }
	  if (scan_cache_end_needed)
	    {
	      (void) heap_scancache_end (thread_p, &scan_cache);
	    }
	  if (status == ER_FAILED)
	    {
	      return ER_FAILED;
	    }
	}			/* if !dead_end */

      if (dead_end)
	{
	  /* set values to null */
	  if (unqualified_dead_end || fetch->ql_flag == true)
	    {
	      /* the object is unqualified */
	      fetch->fetch_res = false;
	    }
	  else
	    {
	      qdata_set_value_list_to_null (xasl->val_list);
	      fetch->fetch_res = true;	/* the object is qualified */
	    }
	}

    }
  else
    {				/* wrong procedure block */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      GOTO_EXIT_ON_ERROR;
    }				/* if-else */

  if (fetch->fetch_res)
    {
      /* evaluate bptr list */
      for (xptr = xasl->bptr_list; fetch->fetch_res == true && xptr != NULL;
	   xptr = xptr->next)
	{
	  if (qexec_execute_obj_set_fetch (thread_p, xptr, xasl_state) !=
	      NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  else if (xptr->proc.fetch.fetch_res == false)
	    {
	      fetch->fetch_res = false;
	    }
	}

      if (fetch->fetch_res)
	{

	  /* evaluate dptr list */
	  for (xptr = xasl->dptr_list; xptr != NULL; xptr = xptr->next)
	    {
	      /* clear correlated subquery list files */
	      qexec_clear_head_lists (thread_p, xptr);
	      if (XASL_IS_FLAGED (xptr, XASL_LINK_TO_REGU_VARIABLE))
		{
		  /* skip if linked to regu var */
		  continue;
		}
	      if (qexec_execute_mainblock (thread_p, xptr, xasl_state) !=
		  NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }

	  /* evaluate constant (if) predicate */
	  if (fetch->fetch_res && xasl->if_pred != NULL)
	    {
	      ev_res2 =
		eval_pred (thread_p, xasl->if_pred, &xasl_state->vd, NULL);
	      if (ev_res2 == V_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      else if (ev_res2 != V_TRUE)
		{
		  fetch->fetch_res = false;
		}
	    }			/*if */

	  if (fetch->fetch_res)
	    {
	      /* evaluate fptr list */
	      for (xptr = xasl->fptr_list;
		   fetch->fetch_res == true && xptr != NULL;
		   xptr = xptr->next)
		{
		  if (qexec_execute_obj_set_fetch (thread_p, xptr, xasl_state)
		      != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		  else if (xptr->proc.fetch.fetch_res == false)
		    {
		      fetch->fetch_res = false;
		    }
		}

	      /* NO SCAN PROCEDURES are supported for these blocks! */
	    }			/* if */

	}			/* if */

    }				/* if */

  /* clear uncorrelated subquery list files */
  if (xasl->aptr_list)
    {
      qexec_clear_head_lists (thread_p, xasl->aptr_list);
    }

  return NO_ERROR;

exit_on_error:

  return ER_FAILED;
}

/*
 * qexec_execute_increment () -
 *   return:
 *   oid(in)    :
 *   class_oid(in)      :
 *   class_hfid(in)     :
 *   attrid(in) :
 *   n_increment(in)    :
 */
static int
qexec_execute_increment (THREAD_ENTRY * thread_p, OID * oid, OID * class_oid,
			 HFID * class_hfid, ATTR_ID attrid, int n_increment)
{
  HEAP_CACHE_ATTRINFO attr_info;
  int attr_info_inited = 0;
  HEAP_SCANCACHE scan_cache;
  bool scan_cache_inited = false;
  HEAP_ATTRVALUE *value = NULL;
  int force_count;
  int error = NO_ERROR;

  error = heap_attrinfo_start (thread_p, class_oid, -1, NULL, &attr_info);
  if (error != NO_ERROR)
    {
      goto wrapup;
    }
  attr_info_inited = 1;

  error =
    locator_start_force_scan_cache (thread_p, &scan_cache, class_hfid,
				    class_oid, SINGLE_ROW_UPDATE);
  if (error != NO_ERROR)
    {
      goto wrapup;
    }
  scan_cache_inited = true;

  if (!class_hfid)
    {
      error = ER_FAILED;
      goto wrapup;
    }

  if (!HFID_IS_NULL (class_hfid))
    {
      if (heap_attrinfo_clear_dbvalues (&attr_info) != NO_ERROR)
	{
	  error = ER_FAILED;
	  goto wrapup;
	}

      /* set increment operation for the attr */
      value = heap_attrvalue_locate (attrid, &attr_info);
      if (value == NULL)
	{
	  error = ER_FAILED;
	  goto wrapup;
	}

      value->do_increment = n_increment;

      force_count = 0;
      if (locator_attribute_info_force
	  (thread_p, class_hfid, oid, 0, &attr_info, &attrid, 1,
	   LC_FLUSH_UPDATE, SINGLE_ROW_UPDATE, &scan_cache, &force_count,
	   false, REPL_INFO_TYPE_STMT_NORMAL) != NO_ERROR)
	{
	  error = ER_FAILED;
	  goto wrapup;
	}
    }

#if 0				/* yaw */
  /* remove query result cache entries which are relevant with this class */
  if (!QFILE_IS_LIST_CACHE_DISABLED)
    {
      if (qexec_clear_list_cache_by_class (class_oid) != NO_ERROR)
	{
	  OID *o = class_oid;
	  er_log_debug (ARG_FILE_LINE,
			"qexec_execute_increment: qexec_clear_list_cache_by_class failed for"
			" class { %d %d %d }\n",
			o->pageid, o->slotid, o->volid);
	}
      qmgr_add_modified_class (class_oid);
    }
#endif

wrapup:

  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }

  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }

  return error;
}

/*
 * qexec_execute_selupd_list () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree
 *   xasl_state(in)     :
 * Note: This routine executes update for a selected tuple
 */
static int
qexec_execute_selupd_list (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			   XASL_STATE * xasl_state)
{
  SELUPD_LIST *list, *selupd;
  REGU_VARLIST_LIST outptr;
  REGU_VARIABLE *varptr;
  DB_VALUE *rightvalp, *thirdvalp;
  LOG_LSA lsa;
  CL_ATTR_ID attrid;
  int n_increment;
  int savepoint_used = 0;
  OID *oid, *class_oid, class_oid_buf;
  HFID *class_hfid;
  int lock_ret;
  int tran_index;
  int err = NO_ERROR;

  list = xasl->selected_upd_list;

  /* in this function,
     several instances can be updated, so it need to be atomic */
  if (xtran_server_start_topop (thread_p, &lsa) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }
  savepoint_used = 1;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  lock_start_instant_lock_mode (tran_index);

  if (db_Enable_replications > 0 && !LOG_CHECK_LOG_APPLIER (thread_p))
    {
      repl_start_flush_mark (thread_p);
    }
  /* do increment operation */
  for (selupd = list; selupd; selupd = selupd->next)
    {
      for (outptr = selupd->select_list; outptr; outptr = outptr->next)
	{
	  if (outptr->list == NULL)
	    {
	      goto exit_on_error;
	    }

	  /* pointer to the regu variable */
	  varptr = &(outptr->list->value);

	  /* check something */
	  if (!((varptr->type == TYPE_INARITH
		 || varptr->type == TYPE_OUTARITH)
		&& (varptr->value.arithptr->opcode == T_INCR
		    || varptr->value.arithptr->opcode == T_DECR)))
	    {
	      goto exit_on_error;
	    }
	  if (!(varptr->value.arithptr->leftptr->type == TYPE_CONSTANT
		&& varptr->value.arithptr->rightptr->type == TYPE_CONSTANT))
	    {
	      goto exit_on_error;
	    }

	  /* get oid and attrid to be fetched last at scan */
	  rightvalp = varptr->value.arithptr->value;
	  if (fetch_peek_dbval (thread_p, varptr->value.arithptr->thirdptr,
				NULL, NULL, NULL, NULL,
				&thirdvalp) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  /* we need the OID and the attribute id to perform increment */
	  oid = DB_GET_OID (rightvalp);
	  attrid = DB_GET_INTEGER (thirdvalp);
	  n_increment = (varptr->value.arithptr->opcode == T_INCR ? 1 : -1);

	  if (OID_ISNULL (oid))
	    {
	      /* in some cases, a query returns no result even if it should
	         have an result on dirty read mode. it may be caused by
	         index scan failure for index to be updated frequently
	         (hot spot index).
	         if this case is fixed, it does not need to be checked */
	      er_log_debug (ARG_FILE_LINE,
			    "qexec_execute_selupd_list: OID is null\n");
	      continue;
	    }

	  /* check if class oid/hfid does not set, find class oid/hfid to access */
	  if (!OID_EQ (&selupd->class_oid, &oid_Null_oid))
	    {
	      class_oid = &selupd->class_oid;
	      class_hfid = &selupd->class_hfid;
	    }
	  else
	    {
	      ACCESS_SPEC_TYPE *specp;

	      (void) heap_get_class_oid (thread_p, oid, &class_oid_buf);

	      class_oid = &class_oid_buf;
	      for (specp = xasl->spec_list; specp; specp = specp->next)
		{
		  if (specp->type == TARGET_CLASS
		      && OID_EQ (&specp->s.cls_node.cls_oid, class_oid))
		    {
		      class_hfid = &specp->s.cls_node.hfid;
		      break;
		    }
		}
	      if (!specp)
		{		/* not found hfid */
		  er_log_debug (ARG_FILE_LINE,
				"qexec_execute_selupd_list: class hfid to access is null\n");
		  goto exit_on_error;
		}
	    }

	  lock_ret = lock_object (thread_p, oid, class_oid, X_LOCK,
				  LK_UNCOND_LOCK);
	  switch (lock_ret)
	    {
	    case LK_GRANTED:
	      /* normal case */
	      break;

	    case LK_NOTGRANTED_DUE_ABORTED:
	      /* error, deadlock or something */
	      goto exit_on_error;

	    case LK_NOTGRANTED_DUE_TIMEOUT:
	      /* ignore lock timeout for click counter,
	         and skip this increment operation */
	      er_log_debug (ARG_FILE_LINE,
			    "qexec_execute_selupd_list: lock(X_LOCK) timed out "
			    "for OID { %d %d %d } class OID { %d %d %d }\n",
			    oid->pageid, oid->slotid, oid->volid,
			    class_oid->pageid, class_oid->slotid,
			    class_oid->volid);
	      er_clear ();
	      continue;

	    default:
	      /* simply, skip this increment operation */
	      er_log_debug (ARG_FILE_LINE,
			    "qexec_execute_selupd_list: skip for OID "
			    "{ %d %d %d } class OID { %d %d %d } lock_ret %d\n",
			    oid->pageid, oid->slotid, oid->volid,
			    class_oid->pageid, class_oid->slotid,
			    class_oid->volid, lock_ret);
	      continue;
	    }

	  if (qexec_execute_increment (thread_p, oid, class_oid, class_hfid,
				       attrid, n_increment) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
    }

  if (db_Enable_replications > 0 && !LOG_CHECK_LOG_APPLIER (thread_p))
    {
      repl_end_flush_mark (thread_p, false);
    }
  if (savepoint_used)
    {
      if (lock_is_instant_lock_mode (tran_index))
	{
	  if (xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_COMMIT, &lsa)
	      != TRAN_UNACTIVE_COMMITTED)
	    {
	      goto exit_on_error;
	    }
	}
      else
	{
	  if (xtran_server_end_topop
	      (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER,
	       &lsa) != TRAN_ACTIVE)
	    {
	      goto exit_on_error;
	    }
	}
    }

exit:
  lock_stop_instant_lock_mode (thread_p, tran_index, true);

  if (err != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }
  else
    {
      return NO_ERROR;
    }

exit_on_error:

  err = ER_FAILED;

  if (db_Enable_replications > 0 && !LOG_CHECK_LOG_APPLIER (thread_p))
    {
      repl_end_flush_mark (thread_p, true);
    }

  if (savepoint_used)
    {
      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
    }

  /* clear some kinds of error code; it's click counter! */
  err = er_errid ();
  if (err == ER_LK_UNILATERALLY_ABORTED ||
      err == ER_LK_OBJECT_TIMEOUT_CLASSOF_MSG ||
      err == ER_LK_OBJECT_DL_TIMEOUT_CLASSOF_MSG ||
      err == ER_LK_OBJECT_DL_TIMEOUT_SIMPLE_MSG ||
      err == ER_LK_OBJECT_TIMEOUT_SIMPLE_MSG)
    {
      er_log_debug (ARG_FILE_LINE,
		    "qexec_execute_selupd_list: ignore error %d\n", err);
      er_clear ();
      err = NO_ERROR;
    }

  goto exit;
}

/*
 * qexec_start_mainblock_iterations () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL tree state information
 *
 * Note: This routines performs the start-up operations for a main
 * procedure block iteration. The main procedure block nodes can
 * be of type BUILDLIST_PROC, BUILDVALUE, UNION_PROC,
 * DIFFERENCE_PROC and INTERSECTION_PROC.
 */
int
qexec_start_mainblock_iterations (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				  XASL_STATE * xasl_state)
{
  QFILE_TUPLE_VALUE_TYPE_LIST type_list;
  QFILE_LIST_ID *t_list_id = NULL;
  int ls_flag = 0;

  switch (xasl->type)
    {
    case CONNECTBY_PROC:
      {
	if (xasl->list_id->type_list.type_cnt == 0)
	  {
	    if (qdata_get_valptr_type_list (xasl->outptr_list,
					    &type_list) != NO_ERROR)
	      {
		if (type_list.domp)
		  {
		    db_private_free_and_init (thread_p, type_list.domp);
		  }
		GOTO_EXIT_ON_ERROR;
	      }

	    QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);
	    t_list_id = qfile_open_list (thread_p, &type_list,
					 NULL, xasl_state->query_id, ls_flag);
	    if (t_list_id == NULL)
	      {
		if (type_list.domp)
		  {
		    db_private_free_and_init (thread_p, type_list.domp);
		  }
		if (t_list_id)
		  {
		    qfile_free_list_id (t_list_id);
		  }
		GOTO_EXIT_ON_ERROR;
	      }

	    if (type_list.domp)
	      {
		db_private_free_and_init (thread_p, type_list.domp);
	      }

	    if (qfile_copy_list_id (xasl->list_id, t_list_id, true) !=
		NO_ERROR)
	      {
		qfile_free_list_id (t_list_id);
		GOTO_EXIT_ON_ERROR;
	      }			/* if */
	    qfile_free_list_id (t_list_id);
	  }
      }
      break;

    case BUILDLIST_PROC:	/* start BUILDLIST_PROC iterations */
      {
	BUILDLIST_PROC_NODE *buildlist = &xasl->proc.buildlist;

	/* initialize groupby_num() value for BUILDLIST_PROC */
	if (buildlist->g_grbynum_val)
	  {
	    DB_MAKE_INT (buildlist->g_grbynum_val, 0);
	  }

	if (xasl->list_id->type_list.type_cnt == 0)
	  {
	    if (qdata_get_valptr_type_list (xasl->outptr_list,
					    &type_list) != NO_ERROR)
	      {
		if (type_list.domp)
		  {
		    db_private_free_and_init (thread_p, type_list.domp);
		  }
		GOTO_EXIT_ON_ERROR;
	      }


	    QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);
	    if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL)
		&& XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED)
		&& buildlist->groupby_list == NULL
		&& (xasl->orderby_list == NULL
		    || XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST))
		&& xasl->option != Q_DISTINCT)
	      {
		QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
	      }

	    t_list_id = qfile_open_list (thread_p, &type_list,
					 xasl->after_iscan_list,
					 xasl_state->query_id, ls_flag);
	    if (t_list_id == NULL)
	      {
		if (type_list.domp)
		  {
		    db_private_free_and_init (thread_p, type_list.domp);
		  }
		if (t_list_id)
		  {
		    qfile_free_list_id (t_list_id);
		  }
		GOTO_EXIT_ON_ERROR;
	      }

	    if (type_list.domp)
	      {
		db_private_free_and_init (thread_p, type_list.domp);
	      }

	    if (qfile_copy_list_id (xasl->list_id, t_list_id, true) !=
		NO_ERROR)
	      {
		qfile_free_list_id (t_list_id);
		GOTO_EXIT_ON_ERROR;
	      }			/* if */

	    qfile_free_list_id (t_list_id);
	  }
	break;
      }

    case BUILDVALUE_PROC:	/* start BUILDVALUE_PROC iterations */
      {
	BUILDVALUE_PROC_NODE *buildvalue = &xasl->proc.buildvalue;

	/* set groupby_num() value as 1 for BUILDVALUE_PROC */
	if (buildvalue->grbynum_val)
	  {
	    DB_MAKE_INT (buildvalue->grbynum_val, 1);
	  }

	/* initialize aggregation list */
	if (qdata_initialize_aggregate_list
	    (thread_p, buildvalue->agg_list,
	     xasl_state->query_id) != NO_ERROR)
	  {
	    GOTO_EXIT_ON_ERROR;
	  }
	break;
      }

    case MERGELIST_PROC:
    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:	/* start SET block iterations */
      {
	break;
      }

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      GOTO_EXIT_ON_ERROR;
    }				/* switch */

  /* initialize inst_num() value, instnum_flag */
  if (xasl->instnum_val)
    {
      DB_MAKE_INT (xasl->instnum_val, 0);
      xasl->instnum_flag &=
	(0xff - (XASL_INSTNUM_FLAG_SCAN_CHECK + XASL_INSTNUM_FLAG_SCAN_STOP));
    }

  /* initialize orderby_num() value */
  if (xasl->ordbynum_val)
    {
      DB_MAKE_INT (xasl->ordbynum_val, 0);
    }

  if (XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
    {
      /* initialize level_val value */
      if (xasl->level_val)
	{
	  DB_MAKE_INT (xasl->level_val, 0);
	}
      /* initialize isleaf_val value */
      if (xasl->isleaf_val)
	{
	  DB_MAKE_INT (xasl->isleaf_val, 0);
	}
      /* initialize iscycle_val value */
      if (xasl->iscycle_val)
	{
	  DB_MAKE_INT (xasl->iscycle_val, 0);
	}
    }

  return NO_ERROR;

exit_on_error:

  return ER_FAILED;
}

/*
 * qexec_end_buildvalueblock_iterations () -
 *   return: NO_ERROR or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL tree state information
 *   tplrec(out) : Tuple record descriptor to store result tuples
 *
 * Note: This routines performs the finish-up operations for BUILDVALUE
 * block iteration.
 */
static int
qexec_end_buildvalueblock_iterations (THREAD_ENTRY * thread_p,
				      XASL_NODE * xasl,
				      XASL_STATE * xasl_state,
				      QFILE_TUPLE_RECORD * tplrec)
{
  QFILE_LIST_ID *t_list_id = NULL;
  int status = NO_ERROR;
  int ls_flag = 0;
  QFILE_TUPLE_DESCRIPTOR *tdp;
  QFILE_LIST_ID *xlist_id;
  QPROC_TPLDESCR_STATUS tpldescr_status;
  DB_LOGICAL ev_res = V_UNKNOWN;
  QFILE_LIST_ID *output = NULL;
  QFILE_TUPLE_VALUE_TYPE_LIST type_list;
  BUILDVALUE_PROC_NODE *buildvalue = &xasl->proc.buildvalue;

  /* make final pass on aggregate list nodes */
  if (buildvalue->agg_list
      && qdata_finalize_aggregate_list (thread_p,
					buildvalue->agg_list) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* evaluate having predicate */
  if (buildvalue->having_pred != NULL)
    {
      ev_res = eval_pred (thread_p, buildvalue->having_pred,
			  &xasl_state->vd, NULL);
      if (ev_res == V_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      else if (ev_res != V_TRUE
	       && qdata_set_valptr_list_unbound (thread_p, xasl->outptr_list,
						 &xasl_state->vd) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* a list of one tuple with a single value needs to be produced */
  if (qdata_get_valptr_type_list (xasl->outptr_list, &type_list) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* If BUILDVALUE_PROC does not have 'order by'(xasl->orderby_list),
     then the list file to be open at here will be the last one.
     Otherwise, the last list file will be open at
     qexec_orderby_distinct().
     (Note that only one that can have 'group by' is BUILDLIST_PROC type.)
     And, the top most XASL is the other condition for the list file
     to be the last result file. */
  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);
  if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL) &&
      XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED) &&
      (xasl->orderby_list == NULL ||
       XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST)) &&
      xasl->option != Q_DISTINCT)
    {
      QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
    }
  t_list_id =
    qfile_open_list (thread_p, &type_list, NULL, xasl_state->query_id,
		     ls_flag);
  if (t_list_id == NULL)
    {
      if (type_list.domp)
	{
	  db_private_free_and_init (thread_p, type_list.domp);
	}
      GOTO_EXIT_ON_ERROR;
    }

      /***** WHAT IN THE WORLD IS THIS? *****/
  if (type_list.domp)
    {
      db_private_free_and_init (thread_p, type_list.domp);
    }

  if (qfile_copy_list_id (xasl->list_id, t_list_id, true) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  output = xasl->list_id;
  qfile_free_list_id (t_list_id);
  t_list_id = NULL;

  if (buildvalue->having_pred == NULL || ev_res == V_TRUE)
    {

      /* make f_valp array */
      xlist_id = xasl->list_id;
      if ((xlist_id->tpl_descr.f_valp == NULL)
	  && (xlist_id->type_list.type_cnt > 0))
	{
	  size_t size = xlist_id->type_list.type_cnt * sizeof (DB_VALUE *);
	  xlist_id->tpl_descr.f_valp = (DB_VALUE **) malloc (size);
	  if (xlist_id->tpl_descr.f_valp == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      /* build tuple descriptor */
      tdp = &((xasl->list_id)->tpl_descr);
      tpldescr_status =
	qdata_generate_tuple_desc_for_valptr_list (thread_p,
						   xasl->outptr_list,
						   &xasl_state->vd, tdp);
      if (tpldescr_status == QPROC_TPLDESCR_FAILURE)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      switch (tpldescr_status)
	{
	case QPROC_TPLDESCR_SUCCESS:
	  /* build tuple into the list file page */
	  if (qfile_generate_tuple_into_list
	      (thread_p, xasl->list_id, T_NORMAL) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  break;

	case QPROC_TPLDESCR_RETRY_SET_TYPE:
	case QPROC_TPLDESCR_RETRY_BIG_REC:
	  if (tplrec->tpl == NULL)
	    {
	      /* allocate tuple descriptor */
	      tplrec->size = DB_PAGESIZE;
	      tplrec->tpl =
		(QFILE_TUPLE) db_private_alloc (thread_p, DB_PAGESIZE);
	      if (tplrec->tpl == NULL)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	  if (qdata_copy_valptr_list_to_tuple (thread_p, xasl->outptr_list,
					       &xasl_state->vd,
					       tplrec) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  if (qfile_add_tuple_to_list (thread_p, xasl->list_id, tplrec->tpl)
	      != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  break;

	default:
	  break;
	}
    }

end:

  QEXEC_CLEAR_AGG_LIST_VALUE (buildvalue->agg_list);
  if (t_list_id)
    {
      qfile_free_list_id (t_list_id);
      t_list_id = NULL;
    }
  if (output)
    {
      qfile_close_list (thread_p, output);
    }

  return status;

exit_on_error:

  status = ER_FAILED;
  goto end;
}

/*
 * qexec_end_mainblock_iterations () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL tree state information
 *   tplrec(out) : Tuple record descriptor to store result tuples
 *
 * Note: This routines performs the finish-up operations for a main
 * procedure block iteration. The main procedure block nodes can
 * be of type BUILDLIST_PROC, BUILDVALUE, UNION_PROC,
 * DIFFERENCE_PROC and INTERSECTION_PROC.
 */
static int
qexec_end_mainblock_iterations (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				XASL_STATE * xasl_state,
				QFILE_TUPLE_RECORD * tplrec)
{
  QFILE_LIST_ID *t_list_id = NULL;
  int status = NO_ERROR;
  bool distinct_needed;
  int ls_flag = 0;

  distinct_needed = (xasl->option == Q_DISTINCT) ? true : false;

  /* Acquire the lockset if composite locking is enabled. */
  if (xasl->composite_locking)
    {
      if (lock_finalize_composite_lock (thread_p, &xasl->composite_lock) !=
	  LK_GRANTED)
	{
	  return ER_FAILED;
	}
      (void) lock_demote_all_update_inst_locks (thread_p);
    }

  switch (xasl->type)
    {

    case CONNECTBY_PROC:
    case BUILDLIST_PROC:	/* end BUILDLIST_PROC iterations */
      /* close the list file */
      qfile_close_list (thread_p, xasl->list_id);
      break;

    case MERGELIST_PROC:
      /* do a direct list file merge to generate resultant list file */
      if (qexec_merge_listfiles (thread_p, xasl, xasl_state) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      break;

    case BUILDVALUE_PROC:	/* end BUILDVALUE_PROC iterations */
      status =
	qexec_end_buildvalueblock_iterations (thread_p, xasl, xasl_state,
					      tplrec);
      break;

    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:
      if (xasl->type == UNION_PROC)
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_UNION);
	}
      else if (xasl->type == DIFFERENCE_PROC)
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_DIFFERENCE);
	}
      else
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_INTERSECT);
	}

      if (distinct_needed)
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_DISTINCT);
	}
      else
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);
	}

      /* For UNION_PROC, DIFFERENCE_PROC, and INTERSECTION_PROC,
         if they do not have 'order by'(xasl->orderby_list),
         then the list file to be open at here will be the last one.
         Otherwise, the last list file will be open at qexec_groupby()
         or qexec_orderby_distinct().
         (Note that only one that can have 'group by' is BUILDLIST_PROC type.)
         And, the top most XASL is the other condition for the list file
         to be the last result file. */

      if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL)
	  && XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED)
	  && (xasl->orderby_list == NULL
	      || XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST)))
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
	}

      t_list_id = qfile_combine_two_list (thread_p,
					  xasl->proc.union_.left->list_id,
					  xasl->proc.union_.right->list_id,
					  ls_flag);
      distinct_needed = false;
      if (!t_list_id)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      if (qfile_copy_list_id (xasl->list_id, t_list_id, true) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      qfile_free_list_id (t_list_id);
      t_list_id = NULL;
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      GOTO_EXIT_ON_ERROR;
    }				/* switch */

  /* DISTINCT processing (i.e, duplicates elimination) is performed at
   * qexec_orderby_distinct() after GROUP BY processing
   */

success:
  if (t_list_id)
    {
      qfile_free_list_id (t_list_id);
    }
  return status;

exit_on_error:
  status = ER_FAILED;
  goto success;

}



/*
 *    qexec_execute_cleanup :
 *    thread_p  [in]  :
*/
static int
qexec_execute_cleanup (THREAD_ENTRY * thread_p)
{
  return NO_ERROR;
};


/*
 * qexec_execute_mainblock () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL state information
 *
 */
int
qexec_execute_mainblock (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			 XASL_STATE * xasl_state)
{
  XASL_NODE *xptr, *xptr2;
  QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
  SCAN_CODE qp_scan;
  int level;
  int spec_level;
  ACCESS_SPEC_TYPE *spec_ptr[2];
  ACCESS_SPEC_TYPE *specp;
  XASL_SCAN_FNC_PTR func_vector = (XASL_SCAN_FNC_PTR) NULL;
  int readonly_scan = true;
  int fixed_scan_flag;
  QFILE_LIST_MERGE_INFO *merge_infop;
  XASL_NODE *outer_xasl = NULL, *inner_xasl = NULL;
  int old_waitsecs;
  int error;
  ACCESS_SPEC_TYPE *tmp_spec = NULL;

  /* create new instant heap memory and save old */

  /*
   * Pre_processing
   */

  switch (xasl->type)
    {
    case CONNECTBY_PROC:
      break;

    case UPDATE_PROC:
      if (!LOG_CHECK_LOG_APPLIER (thread_p))
	{
	  CHECK_MODIFICATION_NO_RETURN (error);
	  if (error != NO_ERROR)
	    return error;
	}

      old_waitsecs = XASL_WAITSECS_NOCHANGE;
      if (xasl->proc.update.waitsecs != XASL_WAITSECS_NOCHANGE)
	{
	  old_waitsecs =
	    xlogtb_reset_wait_secs (thread_p, xasl->proc.update.waitsecs);
	}
      error = qexec_execute_update (thread_p, xasl, xasl_state);
      if (old_waitsecs != XASL_WAITSECS_NOCHANGE)
	{
	  (void) xlogtb_reset_wait_secs (thread_p, old_waitsecs);
	}
      if (error != NO_ERROR)
	{
	  return error;
	}
      /* monitor */
      mnt_qm_updates (thread_p);
      break;

    case DELETE_PROC:
      if (!LOG_CHECK_LOG_APPLIER (thread_p))
	{
	  CHECK_MODIFICATION_NO_RETURN (error);
	  if (error != NO_ERROR)
	    return error;
	}

      old_waitsecs = XASL_WAITSECS_NOCHANGE;
      if (xasl->proc.delete_.waitsecs != XASL_WAITSECS_NOCHANGE)
	{
	  old_waitsecs =
	    xlogtb_reset_wait_secs (thread_p, xasl->proc.delete_.waitsecs);
	}
      error = qexec_execute_delete (thread_p, xasl, xasl_state);
      if (old_waitsecs != XASL_WAITSECS_NOCHANGE)
	{
	  (void) xlogtb_reset_wait_secs (thread_p, old_waitsecs);
	}
      if (error != NO_ERROR)
	{
	  return error;
	}
      /* monitor */
      mnt_qm_deletes (thread_p);
      break;

    case INSERT_PROC:
      if (!LOG_CHECK_LOG_APPLIER (thread_p))
	{
	  CHECK_MODIFICATION_NO_RETURN (error);
	  if (error != NO_ERROR)
	    return error;
	}

      old_waitsecs = XASL_WAITSECS_NOCHANGE;
      if (xasl->proc.insert.waitsecs != XASL_WAITSECS_NOCHANGE)
	{
	  old_waitsecs =
	    xlogtb_reset_wait_secs (thread_p, xasl->proc.insert.waitsecs);
	}
      error = qexec_execute_insert (thread_p, xasl, xasl_state, false);
      if (old_waitsecs != XASL_WAITSECS_NOCHANGE)
	{
	  (void) xlogtb_reset_wait_secs (thread_p, old_waitsecs);
	}
      if (error != NO_ERROR)
	{
	  return error;
	}
      /* monitor */
      mnt_qm_inserts (thread_p);
      break;

    case DO_PROC:
      error = qexec_execute_do_stmt (thread_p, xasl, xasl_state);
      if (error != NO_ERROR)
	{
	  return error;
	}
      break;

    default:

      if (xasl->composite_locking)
	{
	  readonly_scan = false;
	  if (lock_initialize_composite_lock (thread_p, &xasl->composite_lock)
	      != NO_ERROR)
	    {
	      qexec_failure_line (__LINE__, xasl_state);
	      return ER_FAILED;
	    }
	}

      /* evaluate all the aptr lists in all scans */
      for (xptr = xasl; xptr; xptr = xptr->scan_ptr)
	{

	  merge_infop = NULL;	/* init */

	  if (xptr->type == MERGELIST_PROC)
	    {
	      merge_infop = &(xptr->proc.mergelist.ls_merge);

	      outer_xasl = xptr->proc.mergelist.outer_xasl;
	      inner_xasl = xptr->proc.mergelist.inner_xasl;
	    }

	  for (xptr2 = xptr->aptr_list; xptr2; xptr2 = xptr2->next)
	    {
	      if (merge_infop)
		{
		  if (merge_infop->join_type == JOIN_INNER
		      || merge_infop->join_type == JOIN_LEFT)
		    {
		      if (outer_xasl->list_id->type_list.type_cnt > 0
			  && outer_xasl->list_id->tuple_cnt == 0)
			{
			  /* outer is empty; skip inner */
			  if (inner_xasl == xptr2)
			    {
			      continue;
			    }
			}
		    }

		  if (merge_infop->join_type == JOIN_INNER
		      || merge_infop->join_type == JOIN_RIGHT)
		    {
		      if (inner_xasl->list_id->type_list.type_cnt > 0
			  && inner_xasl->list_id->tuple_cnt == 0)
			{
			  /* inner is empty; skip outer */
			  if (outer_xasl == xptr2)
			    {
			      continue;
			    }
			}
		    }
		}

	      if (XASL_IS_FLAGED (xptr2, XASL_LINK_TO_REGU_VARIABLE))
		{
		  /* skip if linked to regu var */
		  continue;
		}

	      if (xptr2->status == XASL_CLEARED)
		{
		  if (qexec_execute_mainblock (thread_p, xptr2, xasl_state) !=
		      NO_ERROR)
		    {
		      if (tplrec.tpl)
			{
			  db_private_free_and_init (thread_p, tplrec.tpl);
			}
		      qexec_failure_line (__LINE__, xasl_state);
		      return ER_FAILED;
		    }
		}
	      else
		{		/* already executed. success or failure */
		  if (xptr2->status != XASL_SUCCESS)
		    {
		      if (tplrec.tpl)
			{
			  db_private_free_and_init (thread_p, tplrec.tpl);
			}
		      qexec_failure_line (__LINE__, xasl_state);
		      return ER_FAILED;
		    }
		}
	    }
	}


      /* start main block iterations */
      if (qexec_start_mainblock_iterations (thread_p, xasl, xasl_state) !=
	  NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /*
       * Processing
       */
      /* Block out main part of query processing for performance profiling of
       * JDBC driver and CAS side. Main purpose of this modification is
       * to pretend that the server's scan time is very fast so that it affect
       * only little portion of whole turnaround time in the point of view
       * of the JDBC driver.
       */

      /* iterative processing is done only for XASL blocks that has
       * access specification list blocks.
       */
      if (xasl->spec_list)
	{
	  /* do locking on each instance instead of composite locking */
	  if (xasl->composite_locking)
	    {
	      fixed_scan_flag = false;
	    }
	  else
	    {
	      fixed_scan_flag = true;
	      for (xptr = xasl; xptr; xptr = xptr->scan_ptr)
		{
		  specp = xptr->spec_list;
		  for (; specp; specp = specp->next)
		    {
		      if (specp->type == TARGET_CLASS
			  && specp->access == INDEX)
			{
			  fixed_scan_flag = false;
			  break;
			}
		    }
		  if (fixed_scan_flag == false)
		    {
		      break;
		    }
		  specp = xptr->merge_spec;
		  if (specp)
		    {
		      if (specp->type == TARGET_CLASS
			  && specp->access == INDEX)
			{
			  fixed_scan_flag = false;
			  break;
			}
		    }
		}
	    }

	  /* open all the scans that are involved within the query,
	   * for SCAN blocks
	   */

	  level = qexec_open_scans (thread_p, xasl, xasl_state,
				    fixed_scan_flag, readonly_scan);
	  if (level == ER_FAILED)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  /* allocate xasl scan function vector */
	  func_vector = (XASL_SCAN_FNC_PTR)
	    db_private_alloc (thread_p, level * sizeof (XSAL_SCAN_FUNC));
	  if (func_vector == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  /* determine the type of XASL block associated functions */
	  if (xasl->merge_spec)
	    {
	      func_vector[0] = (XSAL_SCAN_FUNC) qexec_merge_fnc;
	      /* monitor */
	      mnt_qm_mjoins (thread_p);
	    }
	  else
	    {
	      for (xptr = xasl, level = 0; xptr;
		   xptr = xptr->scan_ptr, level++)
		{

		  /* set all the following scan blocks to off */
		  xasl->next_scan_block_on = false;

		  /* set the associated function with the scan */

		  /* Having more than one interpreter function was a bad
		   * idea, so I've removed the specialized ones. dkh.
		   */
		  if (level == 0)
		    {
		      func_vector[level] = (XSAL_SCAN_FUNC) qexec_intprt_fnc;
		    }
		  else
		    {
		      func_vector[level] =
			(XSAL_SCAN_FUNC) qexec_execute_scan;
		      /* monitor */
		      mnt_qm_nljoins (thread_p);
		    }
		}
	    }

	  if (XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
	    {
	      if (xasl->connect_by_ptr == NULL)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      /* initialize CONNECT BY internal lists */
	      if (qexec_start_connect_by_lists (thread_p,
						xasl->connect_by_ptr,
						xasl_state) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	  tmp_spec = xasl->spec_list;
	  XASL_SPEC_INIT_FLAG (tmp_spec);
	  /* call the first xasl interpreter function */
	  qp_scan = (*func_vector[0]) (thread_p, xasl, xasl_state, &tplrec,
				       &func_vector[1]);

	  if (XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
	    {
	      /* close CONNECT BY internal lists */
	      qexec_end_connect_by_lists (thread_p, xasl->connect_by_ptr);
	    }

	  /* free the function vector */
	  db_private_free_and_init (thread_p, func_vector);

	  /* close all the scans that are involved within the query */
	  for (xptr = xasl, level = 0; xptr; xptr = xptr->scan_ptr, level++)
	    {
	      spec_ptr[0] = xptr->spec_list;
	      spec_ptr[1] = xptr->merge_spec;
	      for (spec_level = 0; spec_level < 2; ++spec_level)
		{
		  for (specp = spec_ptr[spec_level]; specp;
		       specp = specp->next)
		    {
		      scan_end_scan_proxy (thread_p, xptr, specp);
		      qexec_close_scan_proxy (thread_p, xptr, specp);
		    }
		}
	      xptr->curr_spec = NULL;
	    }

	  if (qp_scan != S_SUCCESS)	/* error case */
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  /* process CONNECT BY xasl */
	  if (XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
	    {
	      if (qexec_execute_connect_by (thread_p, xasl->connect_by_ptr,
					    xasl_state, &tplrec) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      /* scan CONNECT BY results, apply WHERE, add to xasl->list_id */
	      if (qexec_iterate_connect_by_results
		  (thread_p, xasl, xasl_state, &tplrec) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      /* clear CONNECT BY internal lists */
	      qexec_clear_connect_by_lists (xasl->connect_by_ptr);
	    }
	}

      if (xasl->selected_upd_list)
	{
	  if (xasl->list_id->tuple_cnt > 1)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_QPROC_INVALID_QRY_SINGLE_TUPLE, 0);
	      GOTO_EXIT_ON_ERROR;
	    }
	  else if (xasl->list_id->tuple_cnt == 1)
	    {
	      old_waitsecs = XASL_WAITSECS_NOCHANGE;
	      if (xasl->selected_upd_list->waitsecs != XASL_WAITSECS_NOCHANGE)
		{
		  old_waitsecs =
		    xlogtb_reset_wait_secs (thread_p,
					    xasl->selected_upd_list->
					    waitsecs);
		}
	      error = qexec_execute_selupd_list (thread_p, xasl, xasl_state);
	      if (old_waitsecs != XASL_WAITSECS_NOCHANGE)
		{
		  (void) xlogtb_reset_wait_secs (thread_p, old_waitsecs);
		}
	      if (error != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	}

      /* end main block iterations */
      if (qexec_end_mainblock_iterations (thread_p, xasl, xasl_state, &tplrec)
	  != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /*
       * Post_processing
       */

      /*
       * DISTINCT processing caused by statement set operators(UNION,
       * DIFFERENCE, INTERSECTION) has already taken place now.
       * But, in the other cases, DISTINCT are not processed yet.
       * qexec_orderby_distinct() will handle it.
       */

      /* GROUP BY processing */
      if (xasl->type == BUILDLIST_PROC	/* it is SELECT query */
	  && xasl->proc.buildlist.groupby_list)	/* it has GROUP BY clause */
	{
	  if (qexec_groupby (thread_p, xasl, xasl_state, &tplrec) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

#if 0				/* DO NOT DELETE ME !!! - yaw: for future work */
      if (xasl->list_id->tuple_cnt == 0)
	{
	  /* skip post processing for empty list file */

	  /* monitor */
	  mnt_qm_selects (thread_p);
	  break;
	}
#endif


      /* ORDER BY and DISTINCT processing */
      if (xasl->type == UNION_PROC
	  || xasl->type == DIFFERENCE_PROC || xasl->type == INTERSECTION_PROC)
	{
	  /* DISTINCT was already processed in these cases. Consider only
	     ORDER BY */
	  if (xasl->orderby_list	/* it has ORDER BY clause */
	      && (xasl->list_id->tuple_cnt > 1	/* the result has more than one tuple */
		  || xasl->ordbynum_val != NULL))	/* ORDERBY_NUM() is used */
	    {
	      /* It has ORDER BY clause and the result has more than one
	         tuple. We cannot skip the processing some cases such as
	         'orderby_num() < 1', for example. */
	      if (qexec_orderby_distinct (thread_p, xasl, Q_ALL, xasl_state)
		  != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	}
      else
	{
	  /* DISTINCT & ORDER BY
	     check orderby_list flag for skipping order by */
	  if ((xasl->orderby_list	/* it has ORDER BY clause */
	       && !XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST)	/* cannot skip */
	       && (xasl->list_id->tuple_cnt > 1	/* the result has more than one tuple */
		   || xasl->ordbynum_val != NULL))	/* ORDERBY_NUM() is used */
	      || (xasl->option == Q_DISTINCT))	/* DISTINCT must be go on */
	    {
	      if (qexec_orderby_distinct (thread_p, xasl, xasl->option,
					  xasl_state) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	}

      /* monitor */
      mnt_qm_selects (thread_p);
      break;
    }

  if (xasl->is_single_tuple)
    {
      if (xasl->list_id->tuple_cnt > 1)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_INVALID_QRY_SINGLE_TUPLE, 0);
	  GOTO_EXIT_ON_ERROR;
	}

      if (xasl->single_tuple
	  && (qdata_get_single_tuple_from_list_id (thread_p, xasl->list_id,
						   xasl->single_tuple) !=
	      NO_ERROR))
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }


  /*
   * Cleanup and Exit processing
   */
  /* clear only non-zero correlation-level uncorrelated subquery list files */
  for (xptr = xasl; xptr; xptr = xptr->scan_ptr)
    {
      if (xptr->aptr_list)
	{
	  qexec_clear_head_lists (thread_p, xptr->aptr_list);
	}
    }
  if (tplrec.tpl)
    {
      db_private_free_and_init (thread_p, tplrec.tpl);
    }

  xasl->status = XASL_SUCCESS;

  return NO_ERROR;

  /*
   * Error processing
   */
exit_on_error:
#if defined(SERVER_MODE)
  /* query execution error must be set up before qfile_close_list(). */
  if (er_errid () < 0)
    {
      qmgr_set_query_error (thread_p, xasl_state->query_id);
    }
#endif
  qfile_close_list (thread_p, xasl->list_id);
  if (func_vector)
    {
      db_private_free_and_init (thread_p, func_vector);
    }

  if (tplrec.tpl)
    {
      db_private_free_and_init (thread_p, tplrec.tpl);
    }

  if (XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
    {
      qexec_clear_connect_by_lists (xasl->connect_by_ptr);
    }

  xasl->status = XASL_FAILURE;

  qexec_failure_line (__LINE__, xasl_state);
  return ER_FAILED;
}


#if 0
#define QP_MAX_RE_EXECUTES_UNDER_DEADLOCKS 10
#endif

QFILE_LIST_ID *
qexec_execute_query (THREAD_ENTRY * thread_p, XASL_NODE * xasl, int dbval_cnt,
		     const DB_VALUE * dbval_ptr, QUERY_ID query_id)
{
  int re_execute;
  int stat;
  QFILE_LIST_ID *list_id = NULL;
  XASL_STATE xasl_state;
  struct timeb tloc;
  struct tm *c_time_struct;
#if defined(SERVER_MODE)
  int rv;
#endif
  int tran_index;
#if defined(CUBRID_DEBUG)
  static int trace = -1;
  static FILE *fp = NULL;
  struct timeval s_tv, e_tv;
#endif /* CUBRID_DEBUG */
  QMGR_QUERY_ENTRY *query_entryp;

#if defined(CUBRID_DEBUG)
  {
    /* check the consistency of the XASL tree */
    if (!qdump_check_xasl_tree (xasl))
      {
	if (xasl)
	  {
	    qdump_print_xasl (xasl);
	  }
	else
	  {
	    printf ("<NULL XASL tree>\n");
	  }
      }
  }

  {
    if (trace == -1)
      {
	char *file;

	file = envvar_get ("QUERY_TRACE_FILE");
	if (file)
	  {
	    trace = 1;
	    if (!strcmp (file, "stdout"))
	      {
		fp = stdout;
	      }
	    else if (!strcmp (file, "stderr"))
	      {
		fp = stderr;
	      }
	    else
	      {
		fp = fopen (file, "a");
		if (!fp)
		  {
		    fprintf (stderr, "Error: QUERY_TRACE_FILE '%s'\n", file);
		    trace = 0;
		  }
	      }
	  }
	else
	  {
	    trace = 0;
	  }
      }

    if (trace && fp)
      {
	time_t loc;
	char str[19];

	time (&loc);
	strftime (str, 19, "%x %X", localtime (&loc));
	fprintf (fp, "start %s tid %d qid %ld query %s\n",
		 str, LOG_FIND_THREAD_TRAN_INDEX (thread_p), query_id,
		 (xasl->qstmt ? xasl->qstmt : "<NULL>"));
	gettimeofday (&s_tv, NULL);
      }
  }
#endif /* CUBRID_DEBUG */

  /* this routine should not be called if an outstanding error condition
   * already exists.
   */
  er_clear ();

  /* form the value descriptor to represent positional values */
  xasl_state.vd.dbval_cnt = dbval_cnt;
  xasl_state.vd.dbval_ptr = (DB_VALUE *) dbval_ptr;
  ftime (&tloc);
  c_time_struct = localtime (&tloc.time);

  if (c_time_struct != NULL)
    {
      db_datetime_encode (&xasl_state.vd.sys_datetime,
			  c_time_struct->tm_mon + 1, c_time_struct->tm_mday,
			  c_time_struct->tm_year + 1900,
			  c_time_struct->tm_hour, c_time_struct->tm_min,
			  c_time_struct->tm_sec, tloc.millitm);
    }

  xasl_state.vd.lrand = lrand48 ();
  xasl_state.vd.drand = drand48 ();
  xasl_state.vd.xasl_state = &xasl_state;

  /* save the query_id into the XASL state struct */
  xasl_state.query_id = query_id;

  /* initialize error line */
  xasl_state.qp_xasl_line = 0;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);

  do
    {
      re_execute = false;

      /* execute the query
       *
       * set the query in progress flag so that qmgr_clear_trans_wakeup() will
       * not remove our XASL tree out from under us in the event the
       * transaction is unilaterally aborted during query execution.
       */

      xasl->query_in_progress = true;

      QPARM_PTR (thread_p)->xasl_state = &xasl_state;

      stat = qexec_execute_mainblock (thread_p, xasl, &xasl_state);

      qexec_execute_cleanup (thread_p);

      xasl->query_in_progress = false;

      if (stat != NO_ERROR)
	{

	  /* Don't reexecute the query when temp file is not available. */
#if 0
#if defined(SERVER_MODE)
	  if (er_errid () == ER_QPROC_NOMORE_QFILE_PAGES
	      && num_deadlock_reexecute <= QP_MAX_RE_EXECUTES_UNDER_DEADLOCKS)
	    {

	      tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
	      if (qmgr_get_tran_status (tran_index) ==
		  QMGR_TRAN_RESUME_DUE_DEADLOCK)
		{
		  num_deadlock_reexecute++;
		}

	      /*
	       * no more pages left in the query file. deallocate all the
	       * pages and goto sleep.
	       */
#if defined(QP_DEBUG)
	      (void) fprintf (stderr, "*WARNING* qexec_execute_query: "
			      "No more pages left in the query area.\n"
			      " Query execution falls into sleep for transaction"
			      " index %d with %d deadlocks. \n\n",
			      tran_index, num_deadlock_reexecute);
#endif
	      /*
	       * Do ***NOT*** clear out the DB_VALUEs in this tree: if you
	       * do, you'll get incorrect results when you restart the
	       * query, because all of your "constants" will have turned to
	       * NULL.
	       */
	      (void) qexec_clear_xasl (thread_p, xasl, false);
	      (void) qmgr_free_query_temp_file (query_id);

	      /*
	       * Wait until some of the holders finish
	       */

	      if (qm_addtg_waiter (tran_index, QMGR_TRAN_WAITING) != NO_ERROR)
		{
		  return (QFILE_LIST_ID *) NULL;
		}

	      qmgr_set_tran_status (tran_index, QMGR_TRAN_RUNNING);
	      re_execute = true;	/* resume execution */
	    }
	  else
#endif
#endif
	    {
	      switch (er_errid ())
		{
		case NO_ERROR:
		  {
		    char buf[512];

		    /* if this query was interrupted by user
		     * then, set error ER_QM_EXECUTION_INTERRUPTED
		     */
#if defined(SERVER_MODE)
		    if (qmgr_is_async_query_interrupted (thread_p, query_id))
		      {
			er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				ER_QM_EXECUTION_INTERRUPTED, 0);
		      }
		    /* Make sure this does NOT return error indication without
		     * setting an error message and code. If we get here,
		     * we most likely have a system error. qp_xasl_line
		     * is the first line to set an error condition.
		     */
		    else
#endif
		      {
			snprintf (buf, 511, "Query execution failure #%d.",
				  xasl_state.qp_xasl_line);
			er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				ER_PT_EXECUTE, 2, buf, "");
		      }
		    break;
		  }
		case ER_INTERRUPTED:
		  {
		    /*
		     * Most of the cleanup that's about to happen will get
		     * screwed up if the interrupt is still in effect (e.g.,
		     * someone will do a pb_fetch, which will quit early, and
		     * so they'll bail without actually finishing their
		     * cleanup), so disable it.
		     */
		    xlogtb_set_interrupt (thread_p, false);
		    break;
		  }
		}		/* switch */

	      qmgr_set_query_error (thread_p, query_id);	/* propagate error */

	      if (xasl->list_id)
		{
		  qfile_close_list (thread_p, xasl->list_id);
		}

	      query_entryp = qmgr_get_query_entry (thread_p, query_id,
						   tran_index);
	      if (query_entryp != NULL)
		{
		  MUTEX_LOCK (rv, query_entryp->lock);
		  list_id = qexec_get_xasl_list_id (xasl);
		  query_entryp->list_id = list_id;
		  MUTEX_UNLOCK (query_entryp->lock);
		}

	      (void) qexec_clear_xasl (thread_p, xasl, true);

	      /* caller will detect the error condition and free the listid */
	      return list_id;
	    }			/* if-else */
	}			/* if */
      /* for async query, clean error */
      else
	{
	  /* async query executed successfully */
	  er_clear ();
	}

    }
  while (re_execute);

  /* get query result list file identifier */
  query_entryp = qmgr_get_query_entry (thread_p, query_id, tran_index);

  if (query_entryp != NULL)
    {
      MUTEX_LOCK (rv, query_entryp->lock);
      list_id = qexec_get_xasl_list_id (xasl);
      query_entryp->list_id = list_id;
      MUTEX_UNLOCK (query_entryp->lock);
    }

  /* set last_pgptr->next_vpid to NULL */
  if (list_id && list_id->last_pgptr != NULL)
    {
      QFILE_PUT_NEXT_VPID_NULL (list_id->last_pgptr);
    }

#if defined(CUBRID_DEBUG)
  {
    if (trace && fp)
      {
	time_t loc;
	char str[19];
	float elapsed;

	gettimeofday (&e_tv, NULL);
	elapsed = (float) (e_tv.tv_sec - s_tv.tv_sec) * 1000000;
	elapsed += (float) (e_tv.tv_usec - s_tv.tv_usec);
	elapsed /= 1000000;
	time (&loc);
	strftime (str, 19, "%x %X", localtime (&loc));
	fprintf (fp, "end %s tid %d qid %d elapsed %.6f\n",
		 str, LOG_FIND_THREAD_TRAN_INDEX (thread_p), query_id,
		 elapsed);
	fflush (fp);
      }
  }
#endif /* CUBRID_DEBUG */

  /* clear XASL tree */
  (void) qexec_clear_xasl (thread_p, xasl, true);

  return list_id;
}

#if defined(CUBRID_DEBUG)
/*
 * get_xasl_dumper_linked_in () -
 *   return:
 */
void
get_xasl_dumper_linked_in ()
{
  XASL_NODE *xasl = NULL;

  qdump_print_xasl (xasl);
}
#endif

#if defined(SERVER_MODE)
#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * tranid_compare () -
 *   return:
 *   t1(in)     :
 *   t2(in)     :
 */
static int
tranid_compare (const void *t1, const void *t2)
{
  return *((int *) t1) - *((int *) t2);
}
#endif /* ENABLE_UNUSED_FUNCTION */
#endif

/*
 * Generation of a pseudo value from the XASL_ID.
 * It is used for hashing purposes.
 */
#define XASL_ID_PSEUDO_KEY(xasl_id) \
  ((((xasl_id)->first_vpid.pageid) | ((xasl_id)->first_vpid.volid) << 24) ^ \
   (((xasl_id)->temp_vfid.fileid) | ((xasl_id)->temp_vfid.volid) >> 8))

/*
 * xasl_id_hash () - Hash an XASL_ID (XASL file identifier)
 *   return:
 *   key(in)    :
 *   htsize(in) :
 */
static unsigned int
xasl_id_hash (const void *key, unsigned int htsize)
{
  unsigned int hash;
  const XASL_ID *xasl_id = (const XASL_ID *) key;

  hash = XASL_ID_PSEUDO_KEY (xasl_id);
  return (hash % htsize);
}

/*
 * xasl_id_hash_cmpeq () - Compare two XASL_IDs for hash purpose
 *   return:
 *   key1(in)   :
 *   key2(in)   :
 */
int
xasl_id_hash_cmpeq (const void *key1, const void *key2)
{
  const XASL_ID *xasl_id1 = (const XASL_ID *) key1;
  const XASL_ID *xasl_id2 = (const XASL_ID *) key2;

  return XASL_ID_EQ (xasl_id1, xasl_id2);
}

/*
 * qexec_initialize_xasl_cache () - Initialize XASL cache
 *   return: NO_ERROR, or ER_code
 */
int
qexec_initialize_xasl_cache (THREAD_ENTRY * thread_p)
{
  int i;
  POOLED_XASL_CACHE_ENTRY *pent;

  if (PRM_XASL_MAX_PLAN_CACHE_ENTRIES <= 0)
    {
      return NO_ERROR;
    }

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* init cache entry info */
  xasl_ent_cache.max_entries = PRM_XASL_MAX_PLAN_CACHE_ENTRIES;
  xasl_ent_cache.num = 0;
  xasl_ent_cache.counter.lookup = 0;
  xasl_ent_cache.counter.hit = 0;
  xasl_ent_cache.counter.miss = 0;
  xasl_ent_cache.counter.full = 0;

  /* memory hash table for XASL stream cache referencing by query string */
  if (xasl_ent_cache.qstr_ht)
    {
      /* if the hash table already exist, clear it out */
      (void) mht_map_no_key (thread_p, xasl_ent_cache.qstr_ht,
			     qexec_free_xasl_cache_ent, NULL);
      (void) mht_clear (xasl_ent_cache.qstr_ht);
    }
  else
    {
      /* create */
      xasl_ent_cache.qstr_ht = mht_create ("XASL stream cache (query string)",
					   xasl_ent_cache.max_entries,
					   mht_1strhash, mht_strcmpeq);
    }
  /* memory hash table for XASL stream cache referencing by xasl file id */
  if (xasl_ent_cache.xid_ht)
    {
      /* if the hash table already exist, clear it out */
      /*(void) mht_map_no_key(xasl_ent_cache.xid_ht, NULL, NULL); */
      (void) mht_clear (xasl_ent_cache.xid_ht);
    }
  else
    {
      /* create */
      xasl_ent_cache.xid_ht = mht_create ("XASL stream cache (xasl file id)",
					  xasl_ent_cache.max_entries,
					  xasl_id_hash, xasl_id_hash_cmpeq);
    }
  /* memory hash table for XASL stream cache referencing by class oid */
  if (xasl_ent_cache.oid_ht)
    {
      /* if the hash table already exist, clear it out */
      /*(void) mht_map_no_key(xasl_ent_cache.oid_ht, NULL, NULL); */
      (void) mht_clear (xasl_ent_cache.oid_ht);
    }
  else
    {
      /* create */
      xasl_ent_cache.oid_ht = mht_create ("XASL stream cache (class oid)",
					  xasl_ent_cache.max_entries,
					  oid_hash, oid_compare_equals);
    }

  /* information of candidates to be removed from XASL cache */
  xasl_ent_cv->include_in_use = true;

#define ENT_C_RATIO 0.05f	/* candidate ratio such as 5% */
  xasl_ent_cv->c_ratio = ENT_C_RATIO;
  xasl_ent_cv->c_num =
    (int) ceil (xasl_ent_cache.max_entries * xasl_ent_cv->c_ratio);
  xasl_ent_cv->c_idx = 0;
  xasl_ent_cv->c_time = (XASL_CACHE_ENTRY **)
    malloc (sizeof (XASL_CACHE_ENTRY *) * xasl_ent_cv->c_num);
  xasl_ent_cv->c_ref = (XASL_CACHE_ENTRY **)
    malloc (sizeof (XASL_CACHE_ENTRY *) * xasl_ent_cv->c_num);

#define ENT_V_RATIO 0.02f	/* victim ratio such as 2% */
  xasl_ent_cv->v_ratio = ENT_V_RATIO;
  xasl_ent_cv->v_num =
    (int) ceil (xasl_ent_cache.max_entries * xasl_ent_cv->v_ratio);
  xasl_ent_cv->v_idx = 0;
  xasl_ent_cv->victim = (XASL_CACHE_ENTRY **)
    malloc (sizeof (XASL_CACHE_ENTRY *) * xasl_ent_cv->v_num);

#if defined (ENABLE_UNUSED_FUNCTION)
  /* init cache clone info */
  xasl_clo_cache.max_clones = PRM_XASL_MAX_PLAN_CACHE_CLONES;
  xasl_clo_cache.num = 0;
  xasl_clo_cache.counter.lookup = 0;
  xasl_clo_cache.counter.hit = 0;
  xasl_clo_cache.counter.miss = 0;
  xasl_clo_cache.counter.full = 0;
  xasl_clo_cache.head = NULL;
  xasl_clo_cache.tail = NULL;
  xasl_clo_cache.free_list = NULL;

  /* if cache clones already exist, free it */
  for (i = 0; i < xasl_clo_cache.n_alloc; i++)
    {
      free_and_init (xasl_clo_cache.alloc_arr[i]);
    }
  free_and_init (xasl_clo_cache.alloc_arr);
  xasl_clo_cache.n_alloc = 0;

  /* now, alloc clones array */
  if (xasl_clo_cache.max_clones > 0)
    {
      xasl_clo_cache.free_list = qexec_expand_xasl_cache_clo_arr (1);
      if (!xasl_clo_cache.free_list)
	{
	  xasl_clo_cache.max_clones = 0;
	}
    }
#endif /* ENABLE_UNUSED_FUNCTION */

  /* XASL cache entry pool */
  if (xasl_cache_entry_pool.pool)
    {
      free_and_init (xasl_cache_entry_pool.pool);
    }

  xasl_cache_entry_pool.n_entries = PRM_XASL_MAX_PLAN_CACHE_ENTRIES + 10;
  xasl_cache_entry_pool.pool = (POOLED_XASL_CACHE_ENTRY *)
    calloc (xasl_cache_entry_pool.n_entries,
	    sizeof (POOLED_XASL_CACHE_ENTRY));

  if (xasl_cache_entry_pool.pool != NULL)
    {
      xasl_cache_entry_pool.free_list = 0;
      for (pent = xasl_cache_entry_pool.pool, i = 0;
	   pent && i < xasl_cache_entry_pool.n_entries - 1; pent++, i++)
	{
	  pent->s.next = i + 1;
	}

      if (pent != NULL)
	{
	  pent->s.next = -1;
	}
    }

  csect_exit (CSECT_QPROC_XASL_CACHE);

  return ((xasl_ent_cache.qstr_ht && xasl_ent_cache.xid_ht
	   && xasl_ent_cache.oid_ht && xasl_cache_entry_pool.pool
	   && xasl_ent_cv->c_time && xasl_ent_cv->c_ref
	   && xasl_ent_cv->victim) ? NO_ERROR : ER_FAILED);
}

/*
 * qexec_finalize_xasl_cache () - Final XASL cache
 *   return: NO_ERROR, or ER_code
 */
int
qexec_finalize_xasl_cache (THREAD_ENTRY * thread_p)
{
  int ret = NO_ERROR;
#if defined (ENABLE_UNUSED_FUNCTION)
  int i;
#endif /* ENABLE_UNUSED_FUNCTION */

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NO_ERROR;
    }

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* memory hash table for XASL stream cache referencing by query string */
  if (xasl_ent_cache.qstr_ht)
    {
      (void) mht_map_no_key (thread_p, xasl_ent_cache.qstr_ht,
			     qexec_free_xasl_cache_ent, NULL);
      mht_destroy (xasl_ent_cache.qstr_ht);
      xasl_ent_cache.qstr_ht = NULL;
    }

  /* memory hash table for XASL stream cache referencing by xasl file id */
  if (xasl_ent_cache.xid_ht)
    {
      mht_destroy (xasl_ent_cache.xid_ht);
      xasl_ent_cache.xid_ht = NULL;
    }

  /* memory hash table for XASL stream cache referencing by class oid */
  if (xasl_ent_cache.oid_ht)
    {
      mht_destroy (xasl_ent_cache.oid_ht);
      xasl_ent_cache.oid_ht = NULL;
    }

  free_and_init (xasl_ent_cv->c_time);
  free_and_init (xasl_ent_cv->c_ref);
  free_and_init (xasl_ent_cv->victim);

#if defined (ENABLE_UNUSED_FUNCTION)
  /* free all cache clone and XASL tree */
  if (xasl_clo_cache.head)
    {
      XASL_CACHE_CLONE *clo;

      while ((clo = xasl_clo_cache.head) != NULL)
	{
	  clo->next = NULL;	/* cut-off */
	  /* delete from LRU list */
	  (void) qexec_delete_LRU_xasl_cache_clo (clo);

	  /* add clone to free_list */
	  ret = qexec_free_xasl_cache_clo (clo);
	}			/* while */
    }

  for (i = 0; i < xasl_clo_cache.n_alloc; i++)
    {
      free_and_init (xasl_clo_cache.alloc_arr[i]);
    }
  free_and_init (xasl_clo_cache.alloc_arr);
  xasl_clo_cache.n_alloc = 0;
#endif /* ENABLE_UNUSED_FUNCTION */

  /* XASL cache entry pool */
  if (xasl_cache_entry_pool.pool)
    {
      free_and_init (xasl_cache_entry_pool.pool);
    }

  csect_exit (CSECT_QPROC_XASL_CACHE);

  return NO_ERROR;
}

/*
 * qexec_print_xasl_cache_ent () - Print the entry
 *                              Will be used by mht_dump() function
 *   return:
 *   fp(in)     :
 *   key(in)    :
 *   data(in)   :
 *   args(in)   :
 */
static int
qexec_print_xasl_cache_ent (FILE * fp, const void *key, void *data,
			    void *args)
{
  XASL_CACHE_ENTRY *ent = (XASL_CACHE_ENTRY *) data;
#if defined (ENABLE_UNUSED_FUNCTION)
  XASL_CACHE_CLONE *clo;
#endif /* ENABLE_UNUSED_FUNCTION */
  int i;
  const OID *o;
  char str[20];
  time_t tmp_time;
  struct tm *c_time_struct;

  if (!ent)
    {
      return false;
    }
  if (fp == NULL)
    {
      fp = stdout;
    }

  fprintf (fp, "XASL_CACHE_ENTRY (%p) {\n", data);
  fprintf (fp, "  query_string = %s\n", ent->query_string);
  fprintf (fp,
	   "  xasl_id = { first_vpid = { %d %d } temp_vfid = { %d %d } }\n",
	   ent->xasl_id.first_vpid.pageid, ent->xasl_id.first_vpid.volid,
	   ent->xasl_id.temp_vfid.fileid, ent->xasl_id.temp_vfid.volid);
#if defined(SERVER_MODE)
  fprintf (fp, "  tran_index_array = [");
  for (i = 0; (unsigned int) i < ent->last_ta_idx; i++)
    {
      fprintf (fp, " %d", ent->tran_index_array[i]);
    }
  fprintf (fp, " ]\n");
  fprintf (fp, "  last_ta_idx = %lld\n", (long long) ent->last_ta_idx);
#endif
  fprintf (fp, "  creator_oid = { %d %d %d }\n", ent->creator_oid.pageid,
	   ent->creator_oid.slotid, ent->creator_oid.volid);
  fprintf (fp, "  n_oid_list = %d\n", ent->n_oid_list);
  fprintf (fp, "  class_oid_list = [");
  for (i = 0, o = ent->class_oid_list; i < ent->n_oid_list; i++, o++)
    {
      fprintf (fp, " { %d %d %d }", ent->class_oid_list[i].pageid,
	       ent->class_oid_list[i].slotid, ent->class_oid_list[i].volid);
    }
  fprintf (fp, " ]\n");
  fprintf (fp, "  repr_id_list = [");
  for (i = 0; i < ent->n_oid_list; i++)
    {
      fprintf (fp, " %d", ent->repr_id_list[i]);
    }
  fprintf (fp, " ]\n");

  tmp_time = ent->time_created.tv_sec;
  c_time_struct = localtime (&tmp_time);
  if (c_time_struct == NULL)
    {
      fprintf (fp, "  ent->time_created.tv_sec is invalid (%ld)\n",
	       ent->time_created.tv_sec);
    }
  else
    {
      (void) strftime (str, sizeof (str), "%x %X", c_time_struct);
      fprintf (fp, "  time_created = %s.%d\n", str,
	       (int) ent->time_created.tv_usec);
    }

  tmp_time = ent->time_last_used.tv_sec;
  c_time_struct = localtime (&tmp_time);
  if (c_time_struct == NULL)
    {
      fprintf (fp, "  ent->time_last_used.tv_sec is invalid (%ld)\n",
	       ent->time_last_used.tv_sec);
    }
  else
    {
      (void) strftime (str, sizeof (str), "%x %X", c_time_struct);
      fprintf (fp, "  time_last_used = %s.%d\n", str,
	       (int) ent->time_last_used.tv_usec);
      fprintf (fp, "  ref_count = %d\n", ent->ref_count);
      fprintf (fp, "  deletion_marker = %s\n",
	       (ent->deletion_marker) ? "true" : "false");
      fprintf (fp, "  dbval_cnt = %d\n", ent->dbval_cnt);
      fprintf (fp, "  list_ht_no = %d\n", ent->list_ht_no);
#if defined (ENABLE_UNUSED_FUNCTION)
      fprintf (fp, "  clo_list = [");
      for (clo = ent->clo_list; clo; clo = clo->next)
	{
	  fprintf (fp, " %p", (void *) clo);
	}
      fprintf (fp, " ]\n");
#endif /* ENABLE_UNUSED_FUNCTION */
      fprintf (fp, "}\n");
    }

  return true;
}

/*
 * qexec_dump_xasl_cache_internal () -
 *   return: NO_ERROR, or ER_code
 *   fp(in)     :
 *   mask(in)   :
 */
int
qexec_dump_xasl_cache_internal (THREAD_ENTRY * thread_p, FILE * fp, int mask)
{
  if (!xasl_ent_cache.qstr_ht || !xasl_ent_cache.xid_ht
      || !xasl_ent_cache.oid_ht)
    {
      return ER_FAILED;
    }
  if (xasl_ent_cache.max_entries <= 0)
    {
      return ER_FAILED;
    }

  if (!fp)
    {
      fp = stdout;
    }

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  fprintf (fp, "\n");
  fprintf (fp,
	   "CACHE        MAX        NUM     LOOKUP        HIT       MISS       FULL\n");
  fprintf (fp, "entry %10d %10d %10d %10d %10d %10d\n",
	   xasl_ent_cache.max_entries, xasl_ent_cache.num,
	   xasl_ent_cache.counter.lookup, xasl_ent_cache.counter.hit,
	   xasl_ent_cache.counter.miss, xasl_ent_cache.counter.full);
#if defined (ENABLE_UNUSED_FUNCTION)
  fprintf (fp, "clone %10d %10d %10d %10d %10d %10d\n",
	   xasl_clo_cache.max_clones, xasl_clo_cache.num,
	   xasl_clo_cache.counter.lookup, xasl_clo_cache.counter.hit,
	   xasl_clo_cache.counter.miss, xasl_clo_cache.counter.full);
#endif /* EANBLE_UNUSED_FUNCTION */
  fprintf (fp, "\n");

#if defined (ENABLE_UNUSED_FUNCTION)	/* DO NOT DELETE ME */
  {
    int i, j, k;
    XASL_CACHE_CLONE *clo;

    for (i = 0, clo = xasl_clo_cache.head; clo; clo = clo->LRU_next)
      {
	i++;
      }
    for (j = 0, clo = xasl_clo_cache.tail; clo; clo = clo->LRU_prev)
      {
	j++;
      }
    for (k = 0, clo = xasl_clo_cache.free_list; clo; clo = clo->next)
      {
	k++;
      }
    fprintf (fp, "CACHE  HEAD_LIST  TAIL_LIST  FREE_LIST    N_ALLOC\n");
    fprintf (fp, "clone %10d %10d %10d %10d\n", i, j, k,
	     xasl_clo_cache.n_alloc);
    fprintf (fp, "\n");
  }
#endif /* ENABLE_UNUSED_FUNCTION */

  if (mask & 1)
    {
      (void) mht_dump (fp, xasl_ent_cache.qstr_ht, true,
		       qexec_print_xasl_cache_ent, NULL);
    }
  if (mask & 2)
    {
      (void) mht_dump (fp, xasl_ent_cache.xid_ht, true,
		       qexec_print_xasl_cache_ent, NULL);
    }
  if (mask & 4)
    {
      (void) mht_dump (fp, xasl_ent_cache.oid_ht, true,
		       qexec_print_xasl_cache_ent, NULL);
    }

  csect_exit (CSECT_QPROC_XASL_CACHE);

  return NO_ERROR;
}

#if defined (CUBRID_DEBUG)
/*
 * qexec_dump_xasl_cache () -
 *   return: NO_ERROR, or ER_code
 *   fname(in)  :
 *   mask(in)   :
 */
int
qexec_dump_xasl_cache (THREAD_ENTRY * thread_p, const char *fname, int mask)
{
  int rc;
  FILE *fp;

  fp = (fname) ? fopen (fname, "a") : stdout;
  if (!fp)
    {
      fp = stdout;
    }
  rc = qexec_dump_xasl_cache_internal (thread_p, fp, mask);
  if (fp != stdout)
    {
      fclose (fp);
    }

  return rc;
}
#endif

/*
 * qexec_alloc_xasl_cache_ent () - Allocate the entry or get one from the pool
 *   return:
 *   req_size(in)       :
 */
static XASL_CACHE_ENTRY *
qexec_alloc_xasl_cache_ent (int req_size)
{
  /* this function should be called within CSECT_QP_XASL_CACHE */
  POOLED_XASL_CACHE_ENTRY *pent = NULL;

  if (req_size > RESERVED_SIZE_FOR_XASL_CACHE_ENTRY ||
      xasl_cache_entry_pool.free_list == -1)
    {
      /* malloc from the heap if required memory size is bigger than reserved,
         or the pool is exhausted */
      pent =
	(POOLED_XASL_CACHE_ENTRY *) malloc (req_size +
					    ADDITION_FOR_POOLED_XASL_CACHE_ENTRY);
      if (pent != NULL)
	{
	  /* mark as to be freed rather than returning back to the pool */
	  pent->s.next = -2;
	}
      else
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_alloc_xasl_cache_ent: allocation failed\n");
	}
    }
  else
    {
      /* get one from the pool */
      pent = &xasl_cache_entry_pool.pool[xasl_cache_entry_pool.free_list];
      xasl_cache_entry_pool.free_list = pent->s.next;
      pent->s.next = -1;
    }
  /* initialize */
  if (pent)
    {
      (void) memset ((void *) &pent->s.entry, 0, req_size);
    }

  return (pent ? &pent->s.entry : NULL);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qexec_expand_xasl_cache_clo_arr () - Expand alloced clone array
 *   return:
 *   n_exp(in)  :
 */
static XASL_CACHE_CLONE *
qexec_expand_xasl_cache_clo_arr (int n_exp)
{
  XASL_CACHE_CLONE **alloc_arr = NULL, *clo = NULL;
  int i, j, s, n, size;

  size = xasl_clo_cache.max_clones;
  if (size <= 0)
    {
      return xasl_clo_cache.free_list;	/* do nothing */
    }

  n = xasl_clo_cache.n_alloc + n_exp;	/* total number */

  if (xasl_clo_cache.n_alloc == 0)
    {
      s = 0;			/* start */
      alloc_arr = (XASL_CACHE_CLONE **)
	calloc (n, sizeof (XASL_CACHE_CLONE *));
    }
  else
    {
      s = xasl_clo_cache.n_alloc;	/* start */
      alloc_arr = (XASL_CACHE_CLONE **)
	realloc (xasl_clo_cache.alloc_arr, sizeof (XASL_CACHE_CLONE *) * n);

      if (alloc_arr != NULL)
	{
	  memset (alloc_arr + xasl_clo_cache.n_alloc, 0x00,
		  sizeof (XASL_CACHE_CLONE *) * n_exp);
	}

    }

  if (alloc_arr == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (XASL_CACHE_CLONE *) * n);
      return NULL;
    }

  /* alloc blocks */
  for (i = s; i < n; i++)
    {
      alloc_arr[i] = (XASL_CACHE_CLONE *)
	calloc (size, sizeof (XASL_CACHE_CLONE));
      if (alloc_arr[i] == NULL)
	{
	  int k;

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (XASL_CACHE_CLONE) * size);

	  /* free alloced memory */
	  for (k = s; k < i; k++)
	    {
	      free_and_init (alloc_arr[k]);
	    }
	  if (s == 0)
	    {			/* is alloced( not realloced) */
	      free_and_init (alloc_arr);
	    }

	  return NULL;
	}
    }

  /* init link */
  for (i = s; i < n; i++)
    {
      for (j = 0; j < size; j++)
	{
	  clo = &alloc_arr[i][j];

	  /* initialize */
	  QEXEC_INITIALIZE_XASL_CACHE_CLO (clo, NULL);
	  clo->next = &alloc_arr[i][j + 1];
	}
      if (i + 1 < n)
	{
	  clo->next = &alloc_arr[i + 1][0];	/* link to next block */
	}
    }

  if (clo != NULL)
    {
      clo->next = NULL;		/* last link */
    }

  xasl_clo_cache.n_alloc = n;
  xasl_clo_cache.alloc_arr = alloc_arr;

  return &xasl_clo_cache.alloc_arr[s][0];
}

/*
 * qexec_alloc_xasl_cache_clo () - Pop the clone from the free_list, or alloc it
 *   return:
 *   ent(in)    :
 */
static XASL_CACHE_CLONE *
qexec_alloc_xasl_cache_clo (XASL_CACHE_ENTRY * ent)
{
  XASL_CACHE_CLONE *clo;

  if (xasl_clo_cache.free_list == NULL && xasl_clo_cache.max_clones > 0)
    {
      /* need more clones; expand alloced clones */
      xasl_clo_cache.free_list = qexec_expand_xasl_cache_clo_arr (1);
    }

  clo = xasl_clo_cache.free_list;
  if (clo)
    {
      /* delete from free_list */
      xasl_clo_cache.free_list = clo->next;

      /* initialize */
      QEXEC_INITIALIZE_XASL_CACHE_CLO (clo, ent);
    }

  return clo;
}

/*
 * qexec_free_xasl_cache_clo () - Push the clone to free_list and free XASL tree
 *   return: NO_ERROR, or ER_code
 *   cache_clone_p(in)    :
 */
int
qexec_free_xasl_cache_clo (XASL_CACHE_CLONE * clo)
{
  if (!clo)
    {
      return ER_FAILED;
    }

  /* free XASL tree */
  stx_free_xasl_unpack_info (clo->xasl_buf_info);

  /* initialize */
  QEXEC_INITIALIZE_XASL_CACHE_CLO (clo, NULL);

  /* add to free_list */
  clo->next = xasl_clo_cache.free_list;
  xasl_clo_cache.free_list = clo;

  return NO_ERROR;
}

/*
 * qexec_append_LRU_xasl_cache_clo () - Append the clone to LRU list tail
 *   return: NO_ERROR, or ER_code
 *   cache_clone_p(in)    :
 */
static int
qexec_append_LRU_xasl_cache_clo (XASL_CACHE_CLONE * clo)
{
  int ret = NO_ERROR;

  /* check the number of XASL cache clones */
  if (xasl_clo_cache.num >= xasl_clo_cache.max_clones)
    {
      XASL_CACHE_ENTRY *ent;
      XASL_CACHE_CLONE *del, *pre, *cur;

      xasl_clo_cache.counter.full++;	/* counter */

      del = xasl_clo_cache.head;	/* get LRU head as victim */
      ent = del->ent_ptr;	/* get entry pointer */

      pre = NULL;
      for (cur = ent->clo_list; cur; cur = cur->next)
	{
	  if (cur == del)
	    {			/* found victim */
	      break;
	    }
	  pre = cur;
	}

      if (!cur)
	{			/* unknown error */
	  er_log_debug (ARG_FILE_LINE,
			"qexec_append_LRU_xasl_cache_clo: not found victim for qstr %s xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
			ent->query_string,
			ent->xasl_id.first_vpid.pageid,
			ent->xasl_id.first_vpid.volid,
			ent->xasl_id.temp_vfid.fileid,
			ent->xasl_id.temp_vfid.volid);
	  er_log_debug (ARG_FILE_LINE, "\tdel = %p, clo_list = [", del);
	  for (cur = ent->clo_list; cur; cur = cur->next)
	    {
	      er_log_debug (ARG_FILE_LINE, " %p", clo);
	    }
	  er_log_debug (ARG_FILE_LINE, " ]\n");
	  return ER_FAILED;
	}

      /* delete from entry's clone list */
      if (pre == NULL)
	{			/* the first */
	  ent->clo_list = del->next;
	}
      else
	{
	  pre->next = del->next;
	}
      del->next = NULL;		/* cut-off */

      /* delete from LRU list */
      (void) qexec_delete_LRU_xasl_cache_clo (del);

      /* add clone to free_list */
      ret = qexec_free_xasl_cache_clo (del);
    }

  clo->LRU_prev = clo->LRU_next = NULL;	/* init */

  /* append to LRU list */
  if (xasl_clo_cache.head == NULL)
    {				/* the first */
      xasl_clo_cache.head = xasl_clo_cache.tail = clo;
    }
  else
    {
      clo->LRU_prev = xasl_clo_cache.tail;
      xasl_clo_cache.tail->LRU_next = clo;

      xasl_clo_cache.tail = clo;	/* move tail */
    }

  xasl_clo_cache.num++;

  return NO_ERROR;
}

/*
 * qexec_delete_LRU_xasl_cache_clo () - Delete the clone from LRU list
 *   return: NO_ERROR, or ER_code
 *   cache_clone_p(in)    :
 */
static int
qexec_delete_LRU_xasl_cache_clo (XASL_CACHE_CLONE * clo)
{
  if (xasl_clo_cache.head == NULL)
    {				/* is empty LRU list */
      return ER_FAILED;
    }

  /* delete from LRU list */
  if (xasl_clo_cache.head == clo)
    {				/* the first */
      xasl_clo_cache.head = clo->LRU_next;	/* move head */
    }
  else
    {
      clo->LRU_prev->LRU_next = clo->LRU_next;
    }

  if (xasl_clo_cache.tail == clo)
    {				/* the last */
      xasl_clo_cache.tail = clo->LRU_prev;	/* move tail */
    }
  else
    {
      clo->LRU_next->LRU_prev = clo->LRU_prev;
    }
  clo->LRU_prev = clo->LRU_next = NULL;	/* cut-off */

  xasl_clo_cache.num--;

  return NO_ERROR;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * qexec_free_xasl_cache_ent () - Remove the entry from the hash and free it
 *                             Can be used by mht_map_no_key() function
 *   return:
 *   data(in)   :
 *   args(in)   :
 */
static int
qexec_free_xasl_cache_ent (THREAD_ENTRY * thread_p, void *data, void *args)
{
  /* this function should be called within CSECT_QP_XASL_CACHE */
  int ret = NO_ERROR;
  POOLED_XASL_CACHE_ENTRY *pent;
  XASL_CACHE_ENTRY *ent = (XASL_CACHE_ENTRY *) data;

  if (!ent)
    {
      return ER_FAILED;
    }

#if defined (ENABLE_UNUSED_FUNCTION)
  /* add clones to free_list */
  if (ent->clo_list)
    {
      XASL_CACHE_CLONE *clo, *next;

      for (clo = ent->clo_list; clo; clo = next)
	{
	  next = clo->next;	/* save next link */
	  clo->next = NULL;	/* cut-off */
	  if (xasl_clo_cache.max_clones > 0)
	    {			/* enable cache clone */
	      /* delete from LRU list */
	      (void) qexec_delete_LRU_xasl_cache_clo (clo);
	    }
	  /* add clone to free_list */
	  ret = qexec_free_xasl_cache_clo (clo);
	}			/* for (cache_clone_p = ent->clo_list; ...) */
    }
#endif /* ENABLE_UNUSED_FUNCTION */

  /* if this entry is from the pool return it, else free it */
  pent = POOLED_XASL_CACHE_ENTRY_FROM_XASL_CACHE_ENTRY (ent);
  if (pent->s.next == -2)
    {
      free_and_init (pent);
    }
  else
    {
      /* return it back to the pool */
      (void) memset (&pent->s.entry, 0, sizeof (XASL_CACHE_ENTRY));
      pent->s.next = xasl_cache_entry_pool.free_list;
      xasl_cache_entry_pool.free_list =
	CAST_BUFLEN (pent - xasl_cache_entry_pool.pool);
    }

  return NO_ERROR;
}				/* qexec_free_xasl_cache_ent() */

/*
 * qexec_lookup_xasl_cache_ent () - Lookup the XASL cache with the query string
 *   return:
 *   qstr(in)   :
 *   user_oid(in)       :
 */
XASL_CACHE_ENTRY *
qexec_lookup_xasl_cache_ent (THREAD_ENTRY * thread_p, const char *qstr,
			     const OID * user_oid)
{
  XASL_CACHE_ENTRY *ent;
  const OID *oidp;
  const int *rep_idp;
  int id;
  int i;
#if defined(SERVER_MODE)
  int tran_index;
  int num_elements;
#endif

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NULL;
    }

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return NULL;
    }

  /* look up the hash table with the key */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.qstr_ht, qstr);
  xasl_ent_cache.counter.lookup++;	/* counter */
  if (ent)
    {
      /* check if it is marked to be deleted */
      if (ent->deletion_marker)
	{
	  (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
	  ent = NULL;
	  goto end;
	}

      /* check ownership */
      if (ent && !OID_EQ (&ent->creator_oid, user_oid))
	{
	  ent = NULL;
	}

      /* check age - timeout */
      if (ent && PRM_XASL_PLAN_CACHE_TIMEOUT >= 0
	  && (difftime (time (NULL),
			ent->time_created.tv_sec) >
	      PRM_XASL_PLAN_CACHE_TIMEOUT))
	{
	  /* delete the entry which is timed out */
	  (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
	  ent = NULL;
	}

      /* check referenced classes using representation id - validation */
      if (ent)
	{
	  for (i = 0, oidp = ent->class_oid_list, rep_idp = ent->repr_id_list;
	       ent && i < ent->n_oid_list; i++, oidp++, rep_idp++)
	    {
	      if (catalog_get_last_representation_id (thread_p,
						      (OID *) oidp,
						      &id) != NO_ERROR
		  || id != *rep_idp)
		{
		  /* delete the entry if any referenced class was changed */
		  (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
		  ent = NULL;
		}
	    }
	}

      /* finally, we found an useful cache entry to reuse */
      if (ent)
	{
	  /* record my transaction id into the entry
	     and adjust timestamp and reference counter */
#if defined(SERVER_MODE)
	  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
	  if ((ssize_t) ent->last_ta_idx < MAX_NTRANS)
	    {
	      num_elements = (int) ent->last_ta_idx;
	      (void) tranid_lsearch (&tran_index, ent->tran_index_array,
				     &num_elements);
	      ent->last_ta_idx = num_elements;
	    }
#endif
	  (void) gettimeofday (&ent->time_last_used, NULL);
	  ent->ref_count++;

	}
    }

  if (ent)
    {
      xasl_ent_cache.counter.hit++;	/* counter */
    }
  else
    {
      xasl_ent_cache.counter.miss++;	/* counter */
    }

end:
  csect_exit (CSECT_QPROC_XASL_CACHE);

  return ent;
}

/*
 * qexec_select_xasl_cache_ent () - Select candidates to remove from the XASL cache
 *                               Will be used by mht_map_no_key() function
 *   return:
 *   data(in)   :
 *   args(in)   :
 */
static int
qexec_select_xasl_cache_ent (THREAD_ENTRY * thread_p, void *data, void *args)
{
  XASL_CACHE_ENTRY *ent = (XASL_CACHE_ENTRY *) data;
  XASL_CACHE_ENT_CV_INFO *info = (XASL_CACHE_ENT_CV_INFO *) args;
  XASL_CACHE_ENTRY **p, **q;
  int i;
  size_t n;

#define timevalcmp(a, CMP, b) \
    (((a).tv_sec == (b).tv_sec) ? \
     ((a).tv_usec CMP (b).tv_usec) : \
     ((a).tv_sec CMP (b).tv_sec))

#if defined(SERVER_MODE)
  if (info->include_in_use == false && ent->last_ta_idx > 0)
    {
      /* do not select one that is in use */
      return NO_ERROR;
    }
  if (info->include_in_use == true && ent->last_ta_idx == 0)
    {
      /* this entry may be selected in the previous selection step */
      return NO_ERROR;
    }
#endif /* SERVER_MODE */

  p = info->c_time;
  q = info->c_ref;
  i = 0;

  while (i < info->c_idx && (p || q))
    {
      if (p && timevalcmp (ent->time_created, <, (*p)->time_created))
	{
	  if (info->c_idx < info->c_num)
	    {
	      n = sizeof (XASL_CACHE_ENTRY *) * (info->c_idx - i);
	    }
	  else
	    {
	      n = sizeof (XASL_CACHE_ENTRY *) * (info->c_idx - i - 1);
	    }

	  (void) memmove (p + 1, p, n);

	  *p = ent;
	  p = NULL;
	}
      else if (p)
	{
	  p++;
	}

      if (q && ent->ref_count < (*q)->ref_count)
	{
	  if (info->c_idx < info->c_num)
	    {
	      n = sizeof (XASL_CACHE_ENTRY *) * (info->c_idx - i);
	    }
	  else
	    {
	      n = sizeof (XASL_CACHE_ENTRY *) * (info->c_idx - i - 1);
	    }

	  (void) memmove (q + 1, q, n);

	  *q = ent;
	  q = NULL;
	}
      else if (q)
	{
	  q++;
	}

      i++;
    }

  if (info->c_idx < info->c_num)
    {
      if (p)
	{
	  *p = ent;
	}
      if (q)
	{
	  *q = ent;
	}
      info->c_idx++;
    }

  if (--info->c_selcnt <= 0)
    {
      /* due to the performance reason, stop traversing here */
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * qexec_update_xasl_cache_ent () -
 *   return:
 *   qstr(in)   :
 *   xasl_id(in)        :
 *   oid(in)    :
 *   n_oids(in) :
 *   class_oids(in)     :
 *   repr_ids(in)       :
 *   dbval_cnt(in)      :
 *
 * Note: Update XASL cache entry if exist or create new one
 * As a side effect, the given 'xasl_id' can be change if the entry which has
 * the same query is found in the cache
 */
XASL_CACHE_ENTRY *
qexec_update_xasl_cache_ent (THREAD_ENTRY * thread_p, const char *qstr,
			     XASL_ID * xasl_id,
			     const OID * oid, int n_oids,
			     const OID * class_oids,
			     const int *repr_ids, int dbval_cnt)
{
  XASL_CACHE_ENTRY *ent, **p, **q, **r;
  const OID *o;
  int len, i, j, k;
#if defined(SERVER_MODE)
  int tran_index;
  int num_elements;
#endif /* SERVER_MODE */

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NULL;
    }

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return NULL;
    }

  /* check again whether the entry is in the cache */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.qstr_ht, qstr);
  if (ent != NULL && OID_EQ (&ent->creator_oid, oid))
    {
      if (ent->deletion_marker)
	{
	  (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
	  ent = NULL;
	  goto end;
	}

      /* the other competing thread which is running the same query
         already updated this entry after that this and the thread had failed
         to find the query in the cache;
         change the given XASL_ID to force to use the cached entry */
      XASL_ID_COPY (xasl_id, &(ent->xasl_id));

      /* record my transaction id into the entry
         and adjust timestamp and reference counter */

#if defined(SERVER_MODE)
      tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
      if ((ssize_t) ent->last_ta_idx < MAX_NTRANS)
	{
	  num_elements = (int) ent->last_ta_idx;
	  (void) tranid_lsearch (&tran_index, ent->tran_index_array,
				 &num_elements);
	  ent->last_ta_idx = num_elements;
	}
#endif

      (void) gettimeofday (&ent->time_last_used, NULL);
      ent->ref_count++;

      goto end;
    }

  if ((XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.xid_ht, xasl_id) != NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "qexec_update_xasl_cache_ent: duplicated xasl_id "
		    "{ first_vpid { %d %d } temp_vfid { %d %d } }\n",
		    xasl_id->first_vpid.pageid, xasl_id->first_vpid.volid,
		    xasl_id->temp_vfid.fileid, xasl_id->temp_vfid.volid);
      ent = NULL;
      goto end;
    }

  /* check the number of XASL cache entries; compare with qstr hash entries */
  if ((int) mht_count (xasl_ent_cache.qstr_ht) >= xasl_ent_cache.max_entries)
    {
      /* Cache full!
         We need to remove some entries. Select candidates that are old aged
         and recently used. Number of candidates is 5% of total entries.
         At first, we make two candidate groups, one by time condition and
         other by frequency. If a candidate belongs to both group, it becomes
         a victim. If the number of victims is insufficient, that is lower
         than 2% or none, select what are most recently used within the old
         aged candiates and what are old aged within the recently used
         candidates. */

      xasl_ent_cache.counter.full++;	/* counter */

      /* STEP 1: examine hash entries to selet candidates */
      xasl_ent_cv->c_idx = 0;
      xasl_ent_cv->v_idx = 0;
      /* at first, try to find candidates within entries that is not in use */
      xasl_ent_cv->c_selcnt = xasl_ent_cv->c_num * 2;
      xasl_ent_cv->include_in_use = false;
      (void) mht_map_no_key (thread_p, xasl_ent_cache.qstr_ht,
			     qexec_select_xasl_cache_ent, xasl_ent_cv);
      if (xasl_ent_cv->c_idx < xasl_ent_cv->c_num)
	{
	  /* insufficient candidates; try once more */
	  xasl_ent_cv->include_in_use = true;
	  (void) mht_map_no_key (thread_p, xasl_ent_cache.qstr_ht,
				 qexec_select_xasl_cache_ent, xasl_ent_cv);
	}

      /* STEP 2: find victims who appears in both groups */
      k = xasl_ent_cv->v_idx;
      r = xasl_ent_cv->victim;
      for (i = 0, p = xasl_ent_cv->c_time; i < xasl_ent_cv->c_num; i++, p++)
	{
	  if (*p == NULL)
	    {
	      continue;		/* skip out */
	    }
	  for (j = 0, q = xasl_ent_cv->c_ref; j < xasl_ent_cv->c_num;
	       j++, q++)
	    {
	      if (*p == *q && k < xasl_ent_cv->v_num)
		{
		  *r++ = *p;
		  k++;
		  *p = *q = NULL;
		  break;
		}
	    }
	  if (k >= xasl_ent_cv->v_num)
	    {
	      break;
	    }
	}

      /* STEP 3: select more victims if insufficient */
      if (k < xasl_ent_cv->v_num)
	{
	  /* The above victim selection algorithm is not completed yet.
	     Two double linked lists for XASL cache entries are needed to
	     implement the algorithm efficiently. One for creation time, and
	     the other one for referencing.
	     Instead, select from most significant members from each groups. */
	  p = xasl_ent_cv->c_time;
	  q = xasl_ent_cv->c_ref;
	  for (i = 0; i < xasl_ent_cv->c_num; i++)
	    {
	      if (*p)
		{
		  *r++ = *p;
		  k++;
		  *p = NULL;
		  if (k >= xasl_ent_cv->v_num)
		    {
		      break;
		    }
		}
	      if (*q)
		{
		  *r++ = *q;
		  k++;
		  *q = NULL;
		  if (k >= xasl_ent_cv->v_num)
		    {
		      break;
		    }
		}
	      p++, q++;
	    }
	}			/* if (k < xasl_ent_cv->v_num) */
      xasl_ent_cv->c_idx = 0;	/* clear */
      xasl_ent_cv->v_idx = k;

      /* STEP 4: now, delete victims from the cache */
      for (k = 0, r = xasl_ent_cv->victim; k < xasl_ent_cv->v_idx; k++, r++)
	{
	  (void) qexec_delete_xasl_cache_ent (thread_p, *r, NULL);
	}
      xasl_ent_cv->v_idx = 0;	/* clear */

    }				/* if */

  /* make new XASL_CACHE_ENTRY */
  len = strlen (qstr) + 1;
  /* get new entry from the XASL_CACHE_ENTRY_POOL */
  ent =
    qexec_alloc_xasl_cache_ent (XASL_CACHE_ENTRY_ALLOC_SIZE (len, n_oids));
  if (ent == NULL)
    {
      goto end;
    }
  /* initialize the entry */
#if defined(SERVER_MODE)
  ent->last_ta_idx = 0;
  ent->tran_index_array =
    (int *) memset (XASL_CACHE_ENTRY_TRAN_INDEX_ARRAY (ent),
		    0, MAX_NTRANS * sizeof (int));
#endif
  ent->n_oid_list = n_oids;

  if (class_oids != NULL)
    {
      ent->class_oid_list =
	(OID *) memcpy (XASL_CACHE_ENTRY_CLASS_OID_LIST (ent),
			(void *) class_oids, n_oids * sizeof (OID));
    }

  if (repr_ids != NULL)
    {
      ent->repr_id_list =
	(int *) memcpy (XASL_CACHE_ENTRY_REPR_ID_LIST (ent),
			(void *) repr_ids, n_oids * sizeof (int));
    }

  ent->query_string =
    (char *) memcpy (XASL_CACHE_ENTRY_QUERY_STRING (ent), (void *) qstr, len);
  XASL_ID_COPY (&ent->xasl_id, xasl_id);
  COPY_OID (&ent->creator_oid, oid);
  (void) gettimeofday (&ent->time_created, NULL);
  (void) gettimeofday (&ent->time_last_used, NULL);
  ent->ref_count = 0;
  ent->deletion_marker = false;
  ent->dbval_cnt = dbval_cnt;
  ent->list_ht_no = -1;
  ent->clo_list = NULL;
  /* record my transaction id into the entry */
#if defined(SERVER_MODE)
  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  if ((ssize_t) ent->last_ta_idx < MAX_NTRANS)
    {
      num_elements = (int) ent->last_ta_idx;
      (void) tranid_lsearch (&tran_index, ent->tran_index_array,
			     &num_elements);
      ent->last_ta_idx = num_elements;
    }
#endif

  /* insert (or update) the entry into the query string hash table */
  if (mht_put_new (xasl_ent_cache.qstr_ht, ent->query_string, ent) == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "qexec_update_xasl_cache_ent: mht_put failed for qstr %s\n",
		    ent->query_string);
      (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
      ent = NULL;
      goto end;
    }
  /* insert (or update) the entry into the xasl file id hash table */
  if (mht_put_new (xasl_ent_cache.xid_ht, &ent->xasl_id, ent) == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "qexec_update_xasl_cache_ent: mht_put failed for xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
		    ent->xasl_id.first_vpid.pageid,
		    ent->xasl_id.first_vpid.volid,
		    ent->xasl_id.temp_vfid.fileid,
		    ent->xasl_id.temp_vfid.volid);
      (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
      ent = NULL;
      goto end;
    }
  /* insert the entry into the class oid hash table
     Note that mht_put2() allows mutiple data with the same key */
  for (i = 0, o = ent->class_oid_list; i < n_oids; i++, o++)
    {
      if (mht_put2_new (xasl_ent_cache.oid_ht, o, ent) == NULL)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_update_xasl_cache_ent: mht_put2 failed for class_oid { %d %d %d }\n",
			o->pageid, o->slotid, o->volid);
	  (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
	  ent = NULL;
	  goto end;
	}
    }				/* for (i = 0, ...) */

  xasl_ent_cache.num++;

end:
  csect_exit (CSECT_QPROC_XASL_CACHE);

  return ent;
}

#if defined(SERVER_MODE)
/*
 * qexec_remove_my_transaction_id () -
 *   return: NO_ERROR, or ER_code
 *   ent(in)    :
 */
static int
qexec_remove_my_transaction_id (THREAD_ENTRY * thread_p,
				XASL_CACHE_ENTRY * ent)
{
  int tran_index, *p, *r;
  int num_elements;

  /* remove my transaction id from the entry and do compaction */
  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  r = &ent->tran_index_array[ent->last_ta_idx];
  do
    {
      /* find my tran_id */
      num_elements = (int) ent->last_ta_idx;
      p = tranid_lfind (&tran_index, ent->tran_index_array, &num_elements);
      ent->last_ta_idx = num_elements;
      if (p)
	{
	  *p = 0;
	  /* do compaction */
	  if (p + 1 < r)
	    {
	      (void) memcpy (p, p + 1, sizeof (int) * (r - p - 1));
	    }
	  ent->last_ta_idx--;
	  r--;
	}
    }
  while (p && p < r);

  return NO_ERROR;
}
#endif /* SERVER_MODE */

/*
 * qexec_end_use_of_xasl_cache_ent () - End use of XASL cache entry
 *   return: NO_ERROR, or ER_code
 *   xasl_id(in)        :
 *   marker(in) :
 */
int
qexec_end_use_of_xasl_cache_ent (THREAD_ENTRY * thread_p,
				 const XASL_ID * xasl_id, bool marker)
{
  XASL_CACHE_ENTRY *ent;
  int rc;

  if (xasl_ent_cache.max_entries <= 0)	/* check this condition first */
    {
      /* XASL cache was disabled so that we are free to delete the XASL file
         whose use is ended */
      return file_destroy (thread_p, &xasl_id->temp_vfid);
    }

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  rc = ER_FAILED;
  /* look up the hast table with the key */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.xid_ht, xasl_id);
  if (ent)
    {
      /* remove my transaction id from the entry and do compaction */
#if defined(SERVER_MODE)
      rc = qexec_remove_my_transaction_id (thread_p, ent);
#else /* SA_MODE */
      rc = NO_ERROR;
#endif /* SERVER_MODE */

      if (marker)
	{
	  /* mark it to be deleted */
	  ent->deletion_marker = true;
	}

#if defined(SERVER_MODE)
      if (ent->deletion_marker && ent->last_ta_idx == 0)
#else /* SA_MODE */
      if (ent->deletion_marker)
#endif /* SERVER_MODE */
	{
	  (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
	}
    }				/* if (ent) */
  else
    {
      er_log_debug (ARG_FILE_LINE,
		    "qexec_end_use_of_xasl_cache_ent: mht_get failed for xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
		    xasl_id->first_vpid.pageid, xasl_id->first_vpid.volid,
		    xasl_id->temp_vfid.fileid, xasl_id->temp_vfid.volid);
#if 0
      /* Hmm, this XASL_ID was not cached due to some reason.
         We are safe to delete this XASL file because it was used only
         by this query */
      if (file_destroy (&xasl_id->temp_vfid) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_end_use_of_xasl_cache_ent: fl_destroy failed for vfid { %d %d }\n",
			xasl_id->temp_vfid.fileid, xasl_id->temp_vfid.volid);
	}
#endif
    }

  csect_exit (CSECT_QPROC_XASL_CACHE);
  return rc;
}

/*
 * qexec_check_xasl_cache_ent_by_xasl () - Check the XASL cache with the XASL ID
 *   return:
 *   xasl_id(in)        :
 *   dbval_cnt(in)      :
 *   clop(in)   :
 */
XASL_CACHE_ENTRY *
qexec_check_xasl_cache_ent_by_xasl (THREAD_ENTRY * thread_p,
				    const XASL_ID * xasl_id, int dbval_cnt,
				    XASL_CACHE_CLONE ** clop)
{
  XASL_CACHE_ENTRY *ent;
#if defined (ENABLE_UNUSED_FUNCTION)
  XASL_CACHE_CLONE *clo;
#endif /* ENABLE_UNUSED_FUNCTION */

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NULL;
    }

  ent = NULL;			/* init */

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return NULL;
    }

  /* look up the hash table with the key, which is XASL ID */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.xid_ht, xasl_id);

  if (ent)
    {
      if (ent->deletion_marker)
	{
	  (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
	  ent = NULL;
	}

      /* check the stored time of the XASL */
      if (ent
	  && !CACHE_TIME_EQ (&(ent->xasl_id.time_stored),
			     &(xasl_id->time_stored)))
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_check_xasl_cache_ent_by_xasl: store time "
			"mismatch %d sec %d usec vs %d sec %d usec\n",
			ent->xasl_id.time_stored.sec,
			ent->xasl_id.time_stored.usec,
			xasl_id->time_stored.sec, xasl_id->time_stored.usec);
	  ent = NULL;
	}

      /* check the number of parameters of the XASL */
      if (ent && dbval_cnt > 0 && ent->dbval_cnt > dbval_cnt)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_check_xasl_cache_ent_by_xasl: dbval_cnt "
			"mismatch %d vs %d\n", ent->dbval_cnt, dbval_cnt);
	  ent = NULL;
	}
    }

#if defined (ENABLE_UNUSED_FUNCTION)
  /* check for cache clone */
  if (clop)
    {
      clo = *clop;
      /* check for cache clone */
      if (ent)
	{
	  if (clo)
	    {			/* push clone back to free_list */
	      /* append to LRU list */
	      if (xasl_clo_cache.max_clones > 0	/* enable cache clone */
		  && qexec_append_LRU_xasl_cache_clo (clo) == NO_ERROR)
		{
		  /* add to clone list */
		  clo->next = ent->clo_list;
		  ent->clo_list = clo;
		}
	      else
		{
		  /* give up; add to free_list */
		  (void) qexec_free_xasl_cache_clo (clo);
		}
	    }
	  else
	    {			/* pop clone from free_list */
	      xasl_clo_cache.counter.lookup++;	/* counter */

	      clo = ent->clo_list;
	      if (clo)
		{		/* already cloned */
		  xasl_clo_cache.counter.hit++;	/* counter */

		  /* delete from clone list */
		  ent->clo_list = clo->next;
		  clo->next = NULL;	/* cut-off */

		  /* delete from LRU list */
		  (void) qexec_delete_LRU_xasl_cache_clo (clo);
		}
	      else
		{
		  xasl_clo_cache.counter.miss++;	/* counter */

		  if (xasl_clo_cache.max_clones > 0)
		    {
		      clo = qexec_alloc_xasl_cache_clo (ent);
		    }
		}
	    }
	}
      else
	{
	  if (clo)
	    {			/* push clone back to free_list */
	      /* give up; add to free_list */
	      (void) qexec_free_xasl_cache_clo (clo);
	    }
	  else
	    {			/* pop clone from free_list */
	      xasl_clo_cache.counter.lookup++;	/* counter */

	      xasl_clo_cache.counter.miss++;	/* counter */
	    }
	}

      *clop = clo;
    }
#endif /* ENABLE_UNUSED_FUNCTION */

  csect_exit (CSECT_QPROC_XASL_CACHE);

  return ent;
}

/*
 * qexec_remove_xasl_cache_ent_by_class () - Remove the XASL cache entries by
 *                                        class OID
 *   return: NO_ERROR, or ER_code
 *   class_oid(in)      :
 */
int
qexec_remove_xasl_cache_ent_by_class (THREAD_ENTRY * thread_p,
				      const OID * class_oid)
{
  XASL_CACHE_ENTRY *ent;
  void *last;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NO_ERROR;
    }

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* for all entries in the class oid hash table
     Note that mht_put2() allows mutiple data with the same key,
     so we have to use mht_get2() */
  last = NULL;
  do
    {
      /* look up the hash table with the key */
      ent = (XASL_CACHE_ENTRY *) mht_get2 (xasl_ent_cache.oid_ht, class_oid,
					   &last);
      if (ent)
	{
#if defined(SERVER_MODE)
	  /* remove my transaction id from the entry and do compaction */
	  (void) qexec_remove_my_transaction_id (thread_p, ent);
#endif
	  if (qexec_delete_xasl_cache_ent (thread_p, ent, NULL) == NO_ERROR)
	    {
	      last = NULL;	/* for mht_get2() */
	    }
	}
    }
  while (ent);

  csect_exit (CSECT_QPROC_XASL_CACHE);

  return NO_ERROR;
}

/*
 * qexec_remove_xasl_cache_ent_by_qstr () - Remove the XASL cache entries by
 *                                       query string
 *   return: NO_ERROR, or ER_code
 *   qstr(in)   :
 *   user_oid(in)       :
 */
int
qexec_remove_xasl_cache_ent_by_qstr (THREAD_ENTRY * thread_p,
				     const char *qstr, const OID * user_oid)
{
  XASL_CACHE_ENTRY *ent;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NO_ERROR;
    }

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* look up the hash table with the key, which is query string */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.qstr_ht, qstr);
  if (ent)
    {
      /* check ownership */
      if (OID_EQ (&ent->creator_oid, user_oid))
	{
#if defined(SERVER_MODE)
	  /* remove my transaction id from the entry and do compaction */
	  (void) qexec_remove_my_transaction_id (thread_p, ent);
#endif
	  (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
	}
    }

  csect_exit (CSECT_QPROC_XASL_CACHE);

  return NO_ERROR;
}

/*
 * qexec_remove_xasl_cache_ent_by_xasl () - Remove the XASL cache entries by
 *                                       XASL ID
 *   return: NO_ERROR, or ER_code
 *   xasl_id(in)        :
 */
int
qexec_remove_xasl_cache_ent_by_xasl (THREAD_ENTRY * thread_p,
				     const XASL_ID * xasl_id)
{
  XASL_CACHE_ENTRY *ent;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NO_ERROR;
    }

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* look up the hash table with the key, which is XASL ID */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.xid_ht, xasl_id);
  if (ent)
    {
#if defined(SERVER_MODE)
      /* remove my transaction id from the entry and do compaction */
      (void) qexec_remove_my_transaction_id (thread_p, ent);
#endif
      (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
    }

  csect_exit (CSECT_QPROC_XASL_CACHE);

  return NO_ERROR;
}

/*
 * qexec_delete_xasl_cache_ent () - Delete a XASL cache entry
 *                               Can be used by mht_map_no_key() function
 *   return:
 *   data(in)   :
 *   args(in)   :
 */
static int
qexec_delete_xasl_cache_ent (THREAD_ENTRY * thread_p, void *data, void *args)
{
  XASL_CACHE_ENTRY *ent = (XASL_CACHE_ENTRY *) data;
  int rc = ER_FAILED;
  const OID *o;
  int i;

  if (!ent)
    {
      return ER_FAILED;
    }

  /* mark it to be deleted */
  ent->deletion_marker = true;
#if defined(SERVER_MODE)
  if (ent->deletion_marker && ent->last_ta_idx == 0)
#else /* SA_MODE */
  if (ent->deletion_marker)
#endif /* SERVER_MODE */
    {
      /* remove the entry from query string hash table */
      if (mht_rem2 (xasl_ent_cache.qstr_ht, ent->query_string, ent,
		    NULL, NULL) != NO_ERROR)
	{
	  if (!ent->deletion_marker)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "qexec_delete_xasl_cache_ent: mht_rem2 failed for qstr %s\n",
			    ent->query_string);
	    }
	}
      /* remove the entry from xasl file id hash table */
      if (mht_rem2 (xasl_ent_cache.xid_ht, &ent->xasl_id, ent,
		    NULL, NULL) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_delete_xasl_cache_ent: mht_rem failed for"
			" xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
			ent->xasl_id.first_vpid.pageid,
			ent->xasl_id.first_vpid.volid,
			ent->xasl_id.temp_vfid.fileid,
			ent->xasl_id.temp_vfid.volid);
	}
      /* remove the entries from class oid hash table */
      for (i = 0, o = ent->class_oid_list; i < ent->n_oid_list; i++, o++)
	{
	  if (mht_rem2 (xasl_ent_cache.oid_ht, o, ent, NULL, NULL) !=
	      NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "qexec_delete_xasl_cache_ent: mht_rem failed for"
			    " class_oid { %d %d %d }\n",
			    ent->class_oid_list[i].pageid,
			    ent->class_oid_list[i].slotid,
			    ent->class_oid_list[i].volid);
	    }
	}
      /* destroy the temp file of XASL_ID */
      if (file_destroy (thread_p, &(ent->xasl_id.temp_vfid)) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_delete_xasl_cache_ent: fl_destroy failed for vfid { %d %d }\n",
			ent->xasl_id.temp_vfid.fileid,
			ent->xasl_id.temp_vfid.volid);
	}

      /* clear out list cache */
      if (ent->list_ht_no >= 0)
	{
	  (void) qfile_clear_list_cache (thread_p, ent->list_ht_no, true);
	}
      rc = qexec_free_xasl_cache_ent (thread_p, ent, NULL);
      xasl_ent_cache.num--;	/* counter */
    }
  else
    {
      /* remove from the query string hash table to allow
         new XASL with the same query string to be registered */
      if (mht_rem2 (xasl_ent_cache.qstr_ht, ent->query_string, ent,
		    NULL, NULL) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_delete_xasl_cache_ent: mht_rem2 failed for qstr %s\n",
			ent->query_string);
	}
    }

  return rc;
}

/*
 * qexec_remove_all_xasl_cache_ent_by_xasl () - Remove all XASL cache entries
 *   return: NO_ERROR, or ER_code
 */
int
qexec_remove_all_xasl_cache_ent_by_xasl (THREAD_ENTRY * thread_p)
{
  int rc;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return ER_FAILED;
    }

  rc = NO_ERROR;		/* init */

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  if (!mht_map_no_key (thread_p, xasl_ent_cache.qstr_ht,
		       qexec_delete_xasl_cache_ent, NULL))
    {
      rc = ER_FAILED;
    }

  csect_exit (CSECT_QPROC_XASL_CACHE);

  return rc;
}

/*
 * qexec_clear_list_cache_by_class () - Clear the list cache entries of the XASL
 *                                   by class OID
 *   return: NO_ERROR, or ER_code
 *   class_oid(in)      :
 */
int
qexec_clear_list_cache_by_class (THREAD_ENTRY * thread_p,
				 const OID * class_oid)
{
  XASL_CACHE_ENTRY *ent;
  void *last;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NO_ERROR;
    }

  if (csect_enter (thread_p, CSECT_QPROC_XASL_CACHE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* for all entries in the class oid hash table
     Note that mht_put2() allows mutiple data with the same key,
     so we have to use mht_get2() */
  last = NULL;
  do
    {
      /* look up the hash table with the key */
      ent = (XASL_CACHE_ENTRY *) mht_get2 (xasl_ent_cache.oid_ht, class_oid,
					   &last);
      if (ent && ent->list_ht_no >= 0)
	{
	  (void) qfile_clear_list_cache (thread_p, ent->list_ht_no, false);
	}
    }
  while (ent);

  csect_exit (CSECT_QPROC_XASL_CACHE);

  return NO_ERROR;
}

/*
 * qexec_partition_select () -
 *   return:
 *   hfid(in)   :
 *   oid(in)    :
 *   xasl_state(in)     :
 *   attr_info(in)      :
 *   xpi(in)    :
 *   rv(in)     :
 */
static int
qexec_partition_select (THREAD_ENTRY * thread_p, HFID * hfid, OID * oid,
			int *node_id, XASL_STATE * xasl_state,
			HEAP_CACHE_ATTRINFO * attr_info,
			XASL_PARTITION_INFO * xpi, REGU_VARIABLE ** rv)
{
  DB_VALUE *retval;
  int idx, partidx;
  int err;

  for (idx = 0; idx < attr_info->num_values; idx++)
    {
      if (xpi->key_attr == attr_info->values[idx].attrid)
	{
	  break;
	}
    }				/* key attr search */

  if (idx >= attr_info->num_values)
    {
      err = ER_PARTITION_WORK_FAILED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, err, 0);
      return err;
    }

  /* key-value set */
  if (*rv == NULL)
    {
      *rv = replace_null_dbval (xpi->expr, &attr_info->values[idx].dbvalue);
      if (*rv == NULL)
	{
	  err = ER_PARTITION_WORK_FAILED;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, err, 0);
	  return err;
	}
    }
  else
    {
      (*rv)->value.dbval = attr_info->values[idx].dbvalue;
    }

  if (fetch_peek_dbval (thread_p, xpi->expr, &xasl_state->vd, NULL, NULL,
			NULL, &retval) != NO_ERROR)
    {
      err = ER_PARTITION_WORK_FAILED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, err, 0);
      return err;
    }

  partidx = qexec_partition_get_parts (xpi, retval);
  if (partidx >= 0 && partidx < xpi->no_parts)
    {
      attr_info->class_oid = xpi->parts[partidx]->class_oid;
      attr_info->last_classrepr->id =
	heap_get_class_repr_id (thread_p,
				&xpi->parts[partidx]->proxy_class_oid);
      if (attr_info->last_classrepr->id <= 0)
	{
	  return ((err = er_errid ()) < 0) ? err : ER_GENERIC_ERROR;
	}

      *oid = xpi->parts[partidx]->class_oid;
      *hfid = xpi->parts[partidx]->class_hfid;
      *node_id = xpi->parts[partidx]->node_id;

      return NO_ERROR;
    }
  else
    {
      err = ER_PARTITION_NOT_EXIST;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, err, 0);
      return err;
    }
}

/*
 * qexec_partition_chg_select () -
 *   return:
 *   xasl_state(in)     :
 *   attr_info(in)      :
 *   xpi(in)    :
 *   rv(in)     :
 */
static int
qexec_partition_chg_select (THREAD_ENTRY * thread_p, XASL_STATE * xasl_state,
			    HEAP_CACHE_ATTRINFO * attr_info,
			    XASL_PARTITION_INFO * xpi, REGU_VARIABLE ** rv)
{
  DB_VALUE *retval;
  int idx, partidx;

  for (idx = 0; idx < attr_info->num_values; idx++)
    {
      if (xpi->key_attr == attr_info->values[idx].attrid)
	{
	  if (attr_info->values[idx].state == HEAP_UNINIT_ATTRVALUE)
	    {
	      return 0;
	    }
	  else
	    {
	      break;
	    }
	}
    }				/* key attr search */

  if (idx >= attr_info->num_values)
    {
      return 0;
    }

  /* key-value set */
  if (*rv == NULL)
    {
      *rv = replace_null_dbval (xpi->expr, &attr_info->values[idx].dbvalue);
      if (*rv == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED,
		  0);
	  return -1;
	}
    }
  else
    {
      (*rv)->value.dbval = attr_info->values[idx].dbvalue;
    }

  if (fetch_peek_dbval (thread_p, xpi->expr, &xasl_state->vd, NULL,
			NULL, NULL, &retval) != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_WORK_FAILED, 0);
      return -1;
    }

  partidx = qexec_partition_get_parts (xpi, retval);
  if (partidx >= 0 && partidx < xpi->no_parts)
    {
      return partidx + 1;
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PARTITION_NOT_EXIST, 0);
      return -1;
    }
}

/*
 * qexec_partition_list_search () -
 *   return:
 *   xpi(in)    :
 *   sval(in)   :
 */
static int
qexec_partition_list_search (XASL_PARTITION_INFO * xpi, DB_VALUE * sval)
{
  DB_VALUE *pval, element;
  int setsize, i1, i2;

  for (i1 = 0; i1 < xpi->no_parts; i1++)
    {
      pval = xpi->parts[i1]->vals;
      if (DB_IS_NULL (pval))
	{
	  continue;
	}

      setsize = set_size (pval->data.set);
      if (setsize <= 0)
	{
	  return -1;
	}

      for (i2 = 0; i2 < setsize; i2++)
	{
	  if (set_get_element_nocopy (pval->data.set, i2, &element) !=
	      NO_ERROR)
	    {
	      return -1;
	    }

	  if ((DB_IS_NULL (sval) && DB_IS_NULL (&element))
	      || db_value_compare (sval, &element) == DB_EQ)
	    {
	      return i1;
	    }
	}
    }

  return -1;
}

/*
 * qexec_partition_range_search () -
 *   return:
 *   xpi(in)    :
 *   sval(in)   :
 */
static int
qexec_partition_range_search (XASL_PARTITION_INFO * xpi, DB_VALUE * sval)
{
  int i1, max = -1, fit = -1;
  DB_VALUE *pval, min_element, max_element, *fitval = NULL;

  for (i1 = 0; i1 < xpi->no_parts; i1++)
    {
      pval = xpi->parts[i1]->vals;
      if (DB_IS_NULL (pval))
	{
	  continue;
	}
      if (set_get_element_nocopy (pval->data.set, 0, &min_element) != NO_ERROR
	  || set_get_element_nocopy (pval->data.set, 1,
				     &max_element) != NO_ERROR)
	{
	  return -1;
	}

      if (DB_IS_NULL (&max_element))
	{
	  max = i1;
	  continue;
	}

      if (DB_IS_NULL (sval) || db_value_compare (sval, &max_element) == DB_LT)
	{
	  if (fit == -1)
	    {
	      fit = i1;
	      fitval = db_value_copy (&max_element);
	    }
	  else
	    {
	      if (db_value_compare (fitval, &max_element) == DB_GT)
		{
		  db_value_free (fitval);
		  fit = i1;
		  fitval = db_value_copy (&max_element);
		}
	    }
	}
    }

  if (fit == -1)
    {
      if (max == -1)
	{
	  if (fitval)
	    {
	      db_value_free (fitval);
	    }
	  return -1;
	}
      fit = max;
    }

  if (fitval)
    {
      db_value_free (fitval);
    }

  return fit;
}

/*
 * replace_null_arith () -
 *   return:
 *   regu_var(in)       :
 *   set_dbval(in)      :
 */
static REGU_VARIABLE *
replace_null_arith (REGU_VARIABLE * regu_var, DB_VALUE * set_dbval)
{
  REGU_VARIABLE *ret;

  if (!regu_var || !regu_var->value.arithptr)
    {
      return NULL;
    }

  ret = replace_null_dbval (regu_var->value.arithptr->leftptr, set_dbval);
  if (ret)
    {
      return ret;
    }

  ret = replace_null_dbval (regu_var->value.arithptr->rightptr, set_dbval);
  if (ret)
    {
      return ret;
    }

  ret = replace_null_dbval (regu_var->value.arithptr->thirdptr, set_dbval);
  if (ret)
    {
      return ret;
    }

  return NULL;
}

/*
 * replace_null_dbval () -
 *   return:
 *   regu_var(in)       :
 *   set_dbval(in)      :
 */
static REGU_VARIABLE *
replace_null_dbval (REGU_VARIABLE * regu_var, DB_VALUE * set_dbval)
{
  if (!regu_var)
    {
      return NULL;
    }

  if (regu_var->type == TYPE_DBVAL)
    {
      regu_var->value.dbval = *set_dbval;
      return regu_var;
    }
  else if (regu_var->type == TYPE_INARITH || regu_var->type == TYPE_OUTARITH)
    {
      return replace_null_arith (regu_var, set_dbval);
    }

  return NULL;
}

/*
 * qexec_partition_get_parts () -
 *   return:
 *   xpi(in)    :
 *   sval(in)   :
 */
static int
qexec_partition_get_parts (XASL_PARTITION_INFO * xpi, DB_VALUE * sval)
{
  switch (xpi->type)
    {
    case PT_PARTITION_HASH:
      return mht_get_hash_number (xpi->act_parts, sval);

    case PT_PARTITION_LIST:
      return qexec_partition_list_search (xpi, sval);

    case PT_PARTITION_RANGE:
      return qexec_partition_range_search (xpi, sval);
    }

  return -1;			/* abnormal state */
}

static int *
tranid_lsearch (const int *key, int *base, int *nmemb)
{
  int *result;

  result = tranid_lfind (key, base, nmemb);
  if (result == NULL)
    {
      result = &base[*nmemb++];
      *result = *key;
    }

  return result;
}

static int *
tranid_lfind (const int *key, const int *base, int *nmemb)
{
  const int *result = base;
  int cnt = 0;

  while (cnt < *nmemb && *key != *result)
    {
      result++;
      cnt++;
    }

  return ((cnt < *nmemb) ? (int *) result : NULL);
}

/*
 * qexec_execute_connect_by () - CONNECT BY execution main function
 *  return:
 *  xasl(in):
 *  xasl_state(in):
 */
static int
qexec_execute_connect_by (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			  XASL_STATE * xasl_state,
			  QFILE_TUPLE_RECORD * tplrec)
{
  QFILE_LIST_ID *listfile0 = NULL, *listfile1 = NULL, *listfile2 = NULL;
  QFILE_LIST_ID *listfile2_tmp = NULL;	/* for order siblings by */
  QFILE_TUPLE_VALUE_TYPE_LIST type_list = { NULL, 0 };
  QFILE_TUPLE_POSITION parent_pos;
  QFILE_LIST_SCAN_ID lfscan_id_lst2tmp, input_lfscan_id;
  QFILE_TUPLE_RECORD tpl_lst2tmp = { (QFILE_TUPLE) NULL, 0 };
  QFILE_TUPLE_RECORD temp_tuple_rec = { (QFILE_TUPLE) NULL, 0 };

  SCAN_CODE qp_lfscan_lst2tmp;
  SORT_LIST sl_bf2df, *orderby_list;
  CONNECTBY_PROC_NODE *connect_by;

  DB_VALUE *level_valp = NULL, *isleaf_valp = NULL, *iscycle_valp = NULL;
  DB_VALUE *parent_pos_valp = NULL, *index_valp = NULL;

  int level_value = 0, isleaf_value = 0, iscycle_value = 0, i = 0;
  char *son_index = NULL, *father_index = NULL;	/* current index and father */
  int len_son_index = 0, len_father_index = 0;
  int index = 0, index_father = 0;
  int has_order_siblings_by;

  int j, key_ranges_cnt;
  KEY_INFO *key_info_p;

  /* scanners vars */
  QFILE_LIST_SCAN_ID lfscan_id;
  QFILE_TUPLE_RECORD tuple_rec;
  QFILE_TUPLE_RECORD input_tuple_rec;
  SCAN_CODE qp_lfscan, qp_input_lfscan;

  DB_LOGICAL ev_res;
  QFILE_TUPLE parent_tpl_in_list;	/* points to parent tuple in result list */
  bool parent_tuple_added;
  int cycle;

  has_order_siblings_by = xasl->orderby_list ? 1 : 0;
  connect_by = &xasl->proc.connect_by;
  input_lfscan_id.status = S_CLOSED;
  lfscan_id.status = S_CLOSED;

  if (qexec_init_index_pseudocolumn_strings (thread_p, &father_index,
					     &len_father_index, &son_index,
					     &len_son_index) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  qexec_set_pseudocolumns_val_pointers (xasl, &level_valp, &isleaf_valp,
					&iscycle_valp, &parent_pos_valp,
					&index_valp);

  /* create the node's output list file */
  if (qexec_start_mainblock_iterations (thread_p, xasl, xasl_state) !=
      NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* replace PRIOR argument constant regu vars values pointers in if_pred
   * with the ones from the current parents list scan
   */
  qexec_replace_prior_regu_vars_pred (thread_p, xasl->if_pred, xasl);

  /* special case for single table query */
  if (connect_by->single_table_opt && xasl->spec_list)
    {
      assert (xasl->spec_list->s_id.status == S_CLOSED);
      qexec_replace_prior_regu_vars_pred (thread_p,
					  xasl->spec_list->where_pred, xasl);
      qexec_replace_prior_regu_vars_pred (thread_p,
					  xasl->spec_list->where_key, xasl);
      if (xasl->spec_list->access == INDEX && xasl->spec_list->indexptr)
	{
	  key_info_p = &xasl->spec_list->indexptr->key_info;
	  key_ranges_cnt = key_info_p->key_cnt;

	  for (j = 0; j < key_ranges_cnt; j++)
	    {
	      qexec_replace_prior_regu_vars (thread_p,
					     key_info_p->key_ranges[j].key1,
					     xasl);
	      qexec_replace_prior_regu_vars (thread_p,
					     key_info_p->key_ranges[j].key2,
					     xasl);
	    }
	}
    }

  /* get the domains for the list files */
  if (qdata_get_valptr_type_list (xasl->outptr_list, &type_list) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* listfile0: output list */
  listfile0 = xasl->list_id;
  if (listfile0 == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* listfile1: current parents list, initialized with START WITH list */
  listfile1 = connect_by->start_with_list_id;
  if (listfile1 == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* listfile2: current children list */
  listfile2 = qfile_open_list (thread_p,
			       &type_list, NULL, xasl_state->query_id, 0);
  if (listfile2 == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (has_order_siblings_by)
    {
      /* listfile2_tmp: current children list temporary (to apply order siblings by) */
      listfile2_tmp = qfile_open_list (thread_p,
				       &type_list,
				       NULL, xasl_state->query_id, 0);
      if (listfile2_tmp == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* sort the start with according to order siblings by */
  if (has_order_siblings_by)
    {
      if (qexec_listfile_orderby (thread_p,
				  listfile1,
				  xasl->orderby_list,
				  xasl_state, xasl->outptr_list) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* we have all list files, let's begin */

  while (listfile1->tuple_cnt > 0)
    {
      tuple_rec.tpl = (QFILE_TUPLE) NULL;
      tuple_rec.size = 0;

      input_tuple_rec.tpl = (QFILE_TUPLE) NULL;
      input_tuple_rec.size = 0;

      qp_input_lfscan = S_ERROR;

      /* calculate LEVEL pseudocolumn value */
      level_value++;
      DB_MAKE_INT (level_valp, level_value);

      /* start parents list scanner */
      if (qfile_open_list_scan (listfile1, &lfscan_id) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      while (1)
	{
	  isleaf_value = 1;
	  iscycle_value = 0;
	  parent_tpl_in_list = NULL;

	  qp_lfscan = qfile_scan_list_next (thread_p, &lfscan_id, &tuple_rec,
					    PEEK);
	  if (qp_lfscan != S_SUCCESS)
	    {
	      break;
	    }

	  parent_tuple_added = false;

	  /* reset parent tuple position pseudocolumn value */
	  DB_MAKE_BIT (parent_pos_valp, DB_DEFAULT_PRECISION, NULL, 8);

	  /* fetch regu_variable values from parent tuple; obs: prior_regu_list
	   * was split into pred and rest for possible future optimizations.
	   */
	  if (fetch_val_list (thread_p,
			      connect_by->prior_regu_list_pred,
			      &xasl_state->vd,
			      NULL, NULL, tuple_rec.tpl, PEEK) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  if (fetch_val_list (thread_p,
			      connect_by->prior_regu_list_rest,
			      &xasl_state->vd,
			      NULL, NULL, tuple_rec.tpl, PEEK) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  /* if START WITH list, we don't have the string index in the tuple
	   * so we create a fictional one with index_father. The column in
	   * the START WITH list will be written afterwards, when we insert
	   * tuples from list1 to list0.
	   */
	  if (listfile1 == connect_by->start_with_list_id)	/*is START WITH list? */
	    {
	      index_father++;
	      father_index[0] = 0;
	      if (compute_son_index (thread_p, &father_index,
				     NULL,
				     &len_father_index,
				     index_father) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	  else
	    {
	      /* not START WITH tuples but a previous generation of children,
	       * now parents. They have the index string column written.
	       */

	      if (qexec_get_index_pseudocolumn_value_from_tuple (thread_p,
								 xasl,
								 tuple_rec.
								 tpl,
								 &index_valp,
								 &father_index,
								 &len_father_index)
		  != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }

	  /* start the scanner on "input" */
	  if (connect_by->single_table_opt)
	    {
	      /* heap/index scan for single table query optimization */
	      if (qexec_open_scan (thread_p, xasl->spec_list, xasl->val_list,
				   &xasl_state->vd, true, true, false, false,
				   &xasl->spec_list->s_id) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      xasl->next_scan_block_on = false;
	    }
	  else
	    {
	      /* otherwise scan input listfile */
	      if (qfile_open_list_scan (connect_by->input_list_id,
					&input_lfscan_id) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }

	  index = 1;

	  if (connect_by->single_table_opt)
	    {
	      /* for single table */
	      qp_input_lfscan =
		qexec_next_scan_block_iterations (thread_p, xasl);
	    }

	  while (1)
	    {
	      if (connect_by->single_table_opt)
		{
		  if (qp_input_lfscan != S_SUCCESS)
		    {
		      break;
		    }
		  /* advance scanner on single table */
		  qp_input_lfscan =
		    scan_next_scan (thread_p, &xasl->curr_spec->s_id);
		}
	      else
		{
		  /* advance scanner on input list file */
		  qp_input_lfscan =
		    qfile_scan_list_next (thread_p, &input_lfscan_id,
					  &input_tuple_rec, PEEK);
		}

	      if (qp_input_lfscan != S_SUCCESS)
		{
		  break;
		}

	      if (!connect_by->single_table_opt)
		{
		  /* fetch pred regu_variable values from input list tuple */
		  if (fetch_val_list (thread_p, connect_by->regu_list_pred,
				      &xasl_state->vd, NULL, NULL,
				      input_tuple_rec.tpl, PEEK) != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      ev_res = V_UNKNOWN;

	      /* evaluate CONNECT BY predicate */
	      if (xasl->if_pred != NULL)
		{
		  ev_res = eval_pred (thread_p,
				      xasl->if_pred, &xasl_state->vd, NULL);
		  if (ev_res == V_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}
	      else
		{
		  ev_res = V_TRUE;
		}

	      if (ev_res == V_TRUE)
		{
		  if (!connect_by->single_table_opt)
		    {
		      /* fetch rest of regu_var values from input list tuple */
		      if (fetch_val_list (thread_p,
					  connect_by->regu_list_rest,
					  &xasl_state->vd,
					  NULL, NULL,
					  input_tuple_rec.tpl,
					  PEEK) != NO_ERROR)
			{
			  GOTO_EXIT_ON_ERROR;
			}
		    }

		  cycle = 0;
		  /* we found a qualified tuple; now check for cycle */
		  if (qexec_check_for_cycle (thread_p,
					     xasl->outptr_list,
					     tuple_rec.tpl,
					     &type_list,
					     listfile0, &cycle) != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }

		  if (cycle == 0)
		    {
		      isleaf_value = 0;
		    }

		  /* found ISLEAF, and we already know LEVEL;
		   * we need to add the parent tuple into result list ASAP,
		   * because we need the information about its position into
		   * the list to be kept into each child tuple
		   */
		  if (!parent_tuple_added)
		    {
		      if (listfile1 == connect_by->start_with_list_id)
			{
			  /* set index string pseudocolumn value to tuples
			   * from START WITH list
			   */
			  DB_MAKE_STRING (index_valp, father_index);
			}

		      /* set CONNECT_BY_ISLEAF pseudocolumn value;
		       * this is only for completion, we don't know its final
		       * value yet
		       */
		      DB_MAKE_INT (isleaf_valp, isleaf_value);

		      /* preserve the parent position pseudocolumn value */
		      if (qexec_get_tuple_column_value (tuple_rec.tpl,
							xasl->outptr_list->
							valptr_cnt -
							PCOL_PARENTPOS_TUPLE_OFFSET,
							parent_pos_valp,
							&tp_Bit_domain) !=
			  NO_ERROR)
			{
			  GOTO_EXIT_ON_ERROR;
			}

		      /* make the "final" parent tuple */
		      tuple_rec = temp_tuple_rec;
		      if (qdata_copy_valptr_list_to_tuple (thread_p,
							   connect_by->
							   prior_outptr_list,
							   &xasl_state->vd,
							   &tuple_rec) !=
			  NO_ERROR)
			{
			  GOTO_EXIT_ON_ERROR;
			}
		      temp_tuple_rec = tuple_rec;

		      /* add parent tuple to output list file, and
		       * get its position into the list
		       */
		      if (qfile_add_tuple_get_pos_in_list (thread_p,
							   listfile0,
							   tuple_rec.tpl,
							   &parent_pos)
			  != NO_ERROR)
			{
			  GOTO_EXIT_ON_ERROR;
			}

		      /* set parent tuple pointer in list */
		      parent_tpl_in_list = parent_pos.tpl;

		      /* set parent tuple position pseudocolumn value */
		      DB_MAKE_BIT (parent_pos_valp, DB_DEFAULT_PRECISION,
				   (void *) &parent_pos,
				   sizeof (parent_pos) * 8);

		      parent_tuple_added = true;
		    }

		  /* only add a child if it doesnt create a cycle */
		  if (cycle == 0)
		    {
		      if (has_order_siblings_by)
			{
			  if (qexec_insert_tuple_into_list (thread_p,
							    listfile2_tmp,
							    xasl->
							    outptr_list,
							    xasl_state,
							    tplrec) !=
			      NO_ERROR)
			    {
			      GOTO_EXIT_ON_ERROR;
			    }
			}
		      else
			{
			  if (compute_son_index (thread_p, &son_index,
						 father_index,
						 &len_son_index,
						 index) != NO_ERROR)
			    {
			      GOTO_EXIT_ON_ERROR;
			    }
			  index++;

			  DB_MAKE_STRING (index_valp, son_index);

			  if (qexec_insert_tuple_into_list (thread_p,
							    listfile2,
							    xasl->
							    outptr_list,
							    xasl_state,
							    tplrec) !=
			      NO_ERROR)
			    {
			      GOTO_EXIT_ON_ERROR;
			    }
			}
		    }
		  else if (!XASL_IS_FLAGED (xasl, XASL_HAS_NOCYCLE))
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_QPROC_CYCLE_DETECTED, 0);

		      GOTO_EXIT_ON_ERROR;
		    }
		  else
		    {
		      iscycle_value = 1;
		    }
		}
	    }
	  xasl->curr_spec = NULL;

	  if (qp_input_lfscan != S_END)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (has_order_siblings_by)
	    {
	      qfile_close_list (thread_p, listfile2_tmp);
	    }

	  if (connect_by->single_table_opt)
	    {
	      qexec_end_scan (thread_p, xasl->spec_list);
	      qexec_close_scan (thread_p, xasl->spec_list);
	    }
	  else
	    {
	      qfile_close_scan (thread_p, &input_lfscan_id);
	    }

	  if (!parent_tuple_added)
	    {
	      /* this parent node wasnt added above because it's a leaf node */

	      if (listfile1 == connect_by->start_with_list_id)
		{
		  DB_MAKE_STRING (index_valp, father_index);
		}

	      DB_MAKE_INT (isleaf_valp, isleaf_value);

	      if (qexec_get_tuple_column_value (tuple_rec.tpl,
						(xasl->outptr_list->
						 valptr_cnt -
						 PCOL_PARENTPOS_TUPLE_OFFSET),
						parent_pos_valp,
						&tp_Bit_domain) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      tuple_rec = temp_tuple_rec;
	      if (qdata_copy_valptr_list_to_tuple (thread_p,
						   connect_by->
						   prior_outptr_list,
						   &xasl_state->vd,
						   &tuple_rec) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      temp_tuple_rec = tuple_rec;

	      if (qfile_add_tuple_get_pos_in_list (thread_p,
						   listfile0,
						   tuple_rec.tpl,
						   &parent_pos) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      parent_tpl_in_list = parent_pos.tpl;
	    }

	  /* set CONNECT_BY_ISCYCLE pseudocolumn value */
	  DB_MAKE_INT (iscycle_valp, iscycle_value);
	  /* it is fixed size data, so we can set it in this fashion */
	  if (qexec_set_tuple_column_value (parent_tpl_in_list,
					    xasl->outptr_list->valptr_cnt -
					    PCOL_ISCYCLE_TUPLE_OFFSET,
					    iscycle_valp,
					    &tp_Integer_domain) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  /* set CONNECT_BY_ISLEAF pseudocolumn value */
	  DB_MAKE_INT (isleaf_valp, isleaf_value);
	  if (qexec_set_tuple_column_value (parent_tpl_in_list,
					    xasl->outptr_list->valptr_cnt -
					    PCOL_ISLEAF_TUPLE_OFFSET,
					    isleaf_valp,
					    &tp_Integer_domain) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (has_order_siblings_by)
	    {
	      /* sort the listfile2_tmp according to orderby lists */
	      index = 1;
	      if (qexec_listfile_orderby (thread_p,
					  listfile2_tmp,
					  xasl->orderby_list,
					  xasl_state,
					  xasl->outptr_list) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      /* scan listfile2_tmp and add indexes to tuples,
	       * then add them to listfile2
	       */
	      if (qfile_open_list_scan (listfile2_tmp, &lfscan_id_lst2tmp)
		  != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      while (1)
		{
		  qp_lfscan_lst2tmp = qfile_scan_list_next (thread_p,
							    &lfscan_id_lst2tmp,
							    &tpl_lst2tmp,
							    PEEK);
		  if (qp_lfscan_lst2tmp != S_SUCCESS)
		    {
		      break;
		    }

		  if (compute_son_index (thread_p, &son_index,
					 father_index,
					 &len_son_index, index) != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }

		  index++;

		  DB_MAKE_STRING (index_valp, son_index);

		  if (fetch_val_list (thread_p,
				      connect_by->prior_regu_list_pred,
				      &xasl_state->vd,
				      NULL, NULL,
				      tpl_lst2tmp.tpl, PEEK) != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		  if (fetch_val_list (thread_p,
				      connect_by->prior_regu_list_rest,
				      &xasl_state->vd,
				      NULL, NULL,
				      tpl_lst2tmp.tpl, PEEK) != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }

		  /* preserve iscycle, isleaf and parent_pos pseudocolumns */
		  if (qexec_get_tuple_column_value (tpl_lst2tmp.tpl,
						    xasl->outptr_list->
						    valptr_cnt -
						    PCOL_ISCYCLE_TUPLE_OFFSET,
						    iscycle_valp,
						    &tp_Integer_domain) !=
		      NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		  if (qexec_get_tuple_column_value (tpl_lst2tmp.tpl,
						    xasl->outptr_list->
						    valptr_cnt -
						    PCOL_ISLEAF_TUPLE_OFFSET,
						    isleaf_valp,
						    &tp_Integer_domain) !=
		      NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		  if (qexec_get_tuple_column_value (tpl_lst2tmp.tpl,
						    xasl->outptr_list->
						    valptr_cnt -
						    PCOL_PARENTPOS_TUPLE_OFFSET,
						    parent_pos_valp,
						    &tp_Bit_domain) !=
		      NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }

		  if (qexec_insert_tuple_into_list (thread_p,
						    listfile2,
						    connect_by->
						    prior_outptr_list,
						    xasl_state,
						    tplrec) != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      qfile_close_scan (thread_p, &lfscan_id_lst2tmp);
	      qfile_close_list (thread_p, listfile2_tmp);
	      qfile_destroy_list (thread_p, listfile2_tmp);
	      qfile_free_list_id (listfile2_tmp);

	      listfile2_tmp = qfile_open_list (thread_p,
					       &type_list,
					       NULL, xasl_state->query_id, 0);
	    }
	}

      qfile_close_scan (thread_p, &lfscan_id);

      if (qp_lfscan != S_END)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (listfile1 != connect_by->start_with_list_id)
	{
	  qfile_close_list (thread_p, listfile1);
	  qfile_destroy_list (thread_p, listfile1);
	  qfile_free_list_id (listfile1);
	}
      listfile1 = listfile2;

      listfile2 = qfile_open_list (thread_p,
				   &type_list, NULL, xasl_state->query_id, 0);
    }

  if (listfile1 != connect_by->start_with_list_id)
    {
      qfile_close_list (thread_p, listfile1);
      qfile_destroy_list (thread_p, listfile1);
      qfile_free_list_id (listfile1);
    }

  qfile_close_list (thread_p, listfile2);
  qfile_destroy_list (thread_p, listfile2);
  qfile_free_list_id (listfile2);

  if (has_order_siblings_by)
    {
      qfile_close_list (thread_p, listfile2_tmp);
      qfile_destroy_list (thread_p, listfile2_tmp);
      qfile_free_list_id (listfile2_tmp);
    }

  if (type_list.domp)
    {
      db_private_free_and_init (thread_p, type_list.domp);
    }

  if (qexec_end_mainblock_iterations (thread_p, xasl, xasl_state, tplrec)
      != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (son_index)
    {
      db_private_free_and_init (thread_p, son_index);
    }
  if (father_index)
    {
      db_private_free_and_init (thread_p, father_index);
    }

  /* sort result (BF to DF) */
  sl_bf2df.next = NULL;
  sl_bf2df.s_order = S_ASC;
  sl_bf2df.pos_descr.pos_no =
    xasl->outptr_list->valptr_cnt - PCOL_INDEX_STRING_TUPLE_OFFSET;
  sl_bf2df.pos_descr.dom = &tp_String_domain;

  orderby_list = xasl->orderby_list;

  /* TODO: need to clarify */
  xasl->orderby_list = &sl_bf2df;

  if (qexec_listfile_orderby (thread_p, xasl->list_id, xasl->orderby_list,
			      xasl_state, xasl->outptr_list) != NO_ERROR)
    {
      xasl->orderby_list = orderby_list;
      GOTO_EXIT_ON_ERROR;
    }

  xasl->orderby_list = orderby_list;

  /* after sort, parent_pos doesnt indicate the correct position of the parent
   * any more; recalculate the parent positions
   */
  if (qexec_recalc_tuples_parent_pos_in_list (thread_p, xasl->list_id) !=
      NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (temp_tuple_rec.tpl)
    {
      db_private_free_and_init (thread_p, temp_tuple_rec.tpl);
    }

  if (xasl->list_id->sort_list)
    {
      qfile_free_sort_list (xasl->list_id->sort_list);
      xasl->list_id->sort_list = NULL;
    }

  if (has_order_siblings_by)
    {
      if (connect_by->start_with_list_id->sort_list)
	{
	  qfile_free_sort_list (connect_by->start_with_list_id->sort_list);
	  connect_by->start_with_list_id->sort_list = NULL;
	}
    }

  xasl->status = XASL_SUCCESS;

  return NO_ERROR;

exit_on_error:

  if (connect_by->single_table_opt)
    {
      qexec_end_scan (thread_p, xasl->spec_list);
      qexec_close_scan (thread_p, xasl->spec_list);
    }
  else
    {
      qfile_close_scan (thread_p, &input_lfscan_id);
    }

  if (type_list.domp)
    {
      db_private_free_and_init (thread_p, type_list.domp);
    }

  if (listfile1 && (listfile1 != connect_by->start_with_list_id))
    {
      qfile_close_list (thread_p, listfile1);
      qfile_destroy_list (thread_p, listfile1);
      qfile_free_list_id (listfile1);
    }

  if (listfile2)
    {
      qfile_close_list (thread_p, listfile2);
      qfile_destroy_list (thread_p, listfile2);
      qfile_free_list_id (listfile2);
    }

  if (listfile2_tmp)
    {
      qfile_close_list (thread_p, listfile2_tmp);
      qfile_destroy_list (thread_p, listfile2_tmp);
      qfile_free_list_id (listfile2_tmp);
    }

  if (son_index)
    {
      db_private_free_and_init (thread_p, son_index);
    }

  if (father_index)
    {
      db_private_free_and_init (thread_p, father_index);
    }

  if (temp_tuple_rec.tpl)
    {
      db_private_free_and_init (thread_p, temp_tuple_rec.tpl);
    }

  if (xasl->list_id->sort_list)
    {
      qfile_free_sort_list (xasl->list_id->sort_list);
      xasl->list_id->sort_list = NULL;
    }

  if (has_order_siblings_by)
    {
      if (connect_by->start_with_list_id->sort_list)
	{
	  qfile_free_sort_list (connect_by->start_with_list_id->sort_list);
	  connect_by->start_with_list_id->sort_list = NULL;
	}
    }

  qfile_close_scan (thread_p, &lfscan_id);

  xasl->status = XASL_FAILURE;

  return ER_FAILED;
}

/*
 * qexec_replace_prior_regu_vars_prior_expr () - replaces values of the
 *    constant regu vars (these are part of the PRIOR argument) with values
 *    fetched from the parent tuple
 *  return:
 *  regu(in):
 *  xasl(in):
 */
void
qexec_replace_prior_regu_vars_prior_expr (THREAD_ENTRY * thread_p,
					  REGU_VARIABLE * regu,
					  XASL_NODE * xasl,
					  XASL_NODE * connect_by_ptr)
{
  if (regu == NULL)
    {
      return;
    }

  switch (regu->type)
    {
    case TYPE_CONSTANT:
      {
	int i;
	QPROC_DB_VALUE_LIST vl, vl_prior;

	for (i = 0, vl = xasl->val_list->valp,
	     vl_prior = connect_by_ptr->proc.connect_by.prior_val_list->valp;
	     i < xasl->val_list->val_cnt &&
	     i < connect_by_ptr->proc.connect_by.prior_val_list->val_cnt;
	     i++, vl = vl->next, vl_prior = vl_prior->next)
	  {
	    if (regu->value.dbvalptr == vl->val)
	      regu->value.dbvalptr = vl_prior->val;
	  }
      }
      break;

    case TYPE_INARITH:
    case TYPE_OUTARITH:
      qexec_replace_prior_regu_vars_prior_expr (thread_p,
						regu->value.arithptr->leftptr,
						xasl, connect_by_ptr);
      qexec_replace_prior_regu_vars_prior_expr (thread_p,
						regu->value.arithptr->
						rightptr, xasl,
						connect_by_ptr);
      qexec_replace_prior_regu_vars_prior_expr (thread_p,
						regu->value.arithptr->
						thirdptr, xasl,
						connect_by_ptr);
      break;

    case TYPE_AGGREGATE:
      qexec_replace_prior_regu_vars_prior_expr (thread_p,
						&regu->value.aggptr->operand,
						xasl, connect_by_ptr);
      break;

    case TYPE_FUNC:
      {
	REGU_VARIABLE_LIST *r = &regu->value.funcp->operand;
	while (*r)
	  {
	    qexec_replace_prior_regu_vars_prior_expr (thread_p,
						      &(*r)->value, xasl,
						      connect_by_ptr);
	    *r = (*r)->next;
	  }
      }
      break;

    default:
      break;
    }
}

/*
 * qexec_replace_prior_regu_vars () - looks for the PRIOR argument, and replaces
 *    the constant regu vars values with values fetched from the parent tuple
 *  return:
 *  regu(in):
 *  xasl(in):
 */
static void
qexec_replace_prior_regu_vars (THREAD_ENTRY * thread_p, REGU_VARIABLE * regu,
			       XASL_NODE * xasl)
{
  if (regu == NULL)
    {
      return;
    }

  switch (regu->type)
    {
    case TYPE_INARITH:
    case TYPE_OUTARITH:
      if (regu->value.arithptr->opcode == T_PRIOR)
	{
	  qexec_replace_prior_regu_vars_prior_expr (thread_p,
						    regu->value.arithptr->
						    rightptr, xasl, xasl);
	}
      else
	{
	  qexec_replace_prior_regu_vars (thread_p,
					 regu->value.arithptr->leftptr, xasl);
	  qexec_replace_prior_regu_vars (thread_p,
					 regu->value.arithptr->rightptr,
					 xasl);
	  qexec_replace_prior_regu_vars (thread_p,
					 regu->value.arithptr->thirdptr,
					 xasl);
	}
      break;

    case TYPE_AGGREGATE:
      qexec_replace_prior_regu_vars (thread_p,
				     &regu->value.aggptr->operand, xasl);
      break;

    case TYPE_FUNC:
      {
	REGU_VARIABLE_LIST *r = &regu->value.funcp->operand;
	while (*r)
	  {
	    qexec_replace_prior_regu_vars (thread_p, &(*r)->value, xasl);
	    *r = (*r)->next;
	  }
      }
      break;

    default:
      break;
    }
}

/*
 * qexec_replace_prior_regu_vars_pred () - replaces the values of constant
 *    regu variables which are part of PRIOR arguments within the predicate,
 *     with values fetched from the parent tuple
 *  return:
 *  pred(in):
 *  xasl(in):
 */
static void
qexec_replace_prior_regu_vars_pred (THREAD_ENTRY * thread_p, PRED_EXPR * pred,
				    XASL_NODE * xasl)
{
  if (pred == NULL)
    {
      return;
    }

  switch (pred->type)
    {
    case T_PRED:
      qexec_replace_prior_regu_vars_pred (thread_p, pred->pe.pred.lhs, xasl);
      qexec_replace_prior_regu_vars_pred (thread_p, pred->pe.pred.rhs, xasl);
      break;

    case T_EVAL_TERM:
      switch (pred->pe.eval_term.et_type)
	{
	case T_COMP_EVAL_TERM:
	  qexec_replace_prior_regu_vars (thread_p,
					 pred->pe.eval_term.et.et_comp.lhs,
					 xasl);
	  qexec_replace_prior_regu_vars (thread_p,
					 pred->pe.eval_term.et.et_comp.rhs,
					 xasl);
	  break;

	case T_ALSM_EVAL_TERM:
	  qexec_replace_prior_regu_vars (thread_p,
					 pred->pe.eval_term.et.et_alsm.elem,
					 xasl);
	  qexec_replace_prior_regu_vars (thread_p,
					 pred->pe.eval_term.et.et_alsm.
					 elemset, xasl);
	  break;

	case T_LIKE_EVAL_TERM:
	  qexec_replace_prior_regu_vars (thread_p,
					 pred->pe.eval_term.et.et_like.
					 pattern, xasl);
	  qexec_replace_prior_regu_vars (thread_p,
					 pred->pe.eval_term.et.et_like.src,
					 xasl);
	  break;
	}
      break;

    case T_NOT_TERM:
      qexec_replace_prior_regu_vars_pred (thread_p, pred->pe.not_term, xasl);
      break;
    }
}

/*
 * qexec_insert_tuple_into_list () - helper function for inserting a tuple
 *    into a list file
 *  return:
 *  list_id(in/out):
 *  xasl(in):
 *  xasl_state(in):
 *  tplrec(in):
 */
static int
qexec_insert_tuple_into_list (THREAD_ENTRY * thread_p,
			      QFILE_LIST_ID * list_id,
			      OUTPTR_LIST * outptr_list,
			      XASL_STATE * xasl_state,
			      QFILE_TUPLE_RECORD * tplrec)
{
  QFILE_TUPLE_DESCRIPTOR *tdp;
  QPROC_TPLDESCR_STATUS tpldescr_status;

  /* make f_valp array */
  if ((list_id->tpl_descr.f_valp == NULL)
      && (list_id->type_list.type_cnt > 0))
    {
      list_id->tpl_descr.f_valp =
	(DB_VALUE **) malloc (list_id->type_list.type_cnt *
			      DB_SIZEOF (DB_VALUE *));

      if (list_id->tpl_descr.f_valp == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1,
		  list_id->type_list.type_cnt * DB_SIZEOF (DB_VALUE *));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
    }

  /* build tuple descriptor */
  tdp = &(list_id->tpl_descr);
  tpldescr_status =
    qdata_generate_tuple_desc_for_valptr_list (thread_p,
					       outptr_list,
					       &xasl_state->vd, tdp);
  if (tpldescr_status == QPROC_TPLDESCR_FAILURE)
    {
      return ER_FAILED;
    }

  switch (tpldescr_status)
    {
    case QPROC_TPLDESCR_SUCCESS:
      /* generate tuple into list file page */
      if (qfile_generate_tuple_into_list (thread_p, list_id, T_NORMAL)
	  != NO_ERROR)
	{
	  return ER_FAILED;
	}
      break;

    case QPROC_TPLDESCR_RETRY_SET_TYPE:
    case QPROC_TPLDESCR_RETRY_BIG_REC:
      /* BIG QFILE_TUPLE or a SET-field is included */
      if (tplrec->tpl == NULL)
	{
	  /* allocate tuple descriptor */
	  tplrec->size = DB_PAGESIZE;
	  tplrec->tpl =
	    (QFILE_TUPLE) db_private_alloc (thread_p, DB_PAGESIZE);
	  if (tplrec->tpl == NULL)
	    {
	      return ER_FAILED;
	    }
	}
      if ((qdata_copy_valptr_list_to_tuple (thread_p, outptr_list,
					    &xasl_state->vd,
					    tplrec) != NO_ERROR)
	  || (qfile_add_tuple_to_list (thread_p, list_id,
				       tplrec->tpl) != NO_ERROR))
	{
	  return ER_FAILED;
	}
      break;

    default:
      break;
    }

  return NO_ERROR;
}

/*
 * qexec_set_tuple_column_value () - helper function for writing a column
 *    value into a tuple
 *  return:
 *  tpl(in/out):
 *  index(in):
 *  valp(in):
 *  domain(in):
 */
int
qexec_set_tuple_column_value (QFILE_TUPLE tpl, int index,
			      DB_VALUE * valp, TP_DOMAIN * domain)
{
  QFILE_TUPLE_VALUE_FLAG flag;
  char *ptr;
  int length;
  PR_TYPE *pr_type;
  OR_BUF buf;

  flag = (QFILE_TUPLE_VALUE_FLAG) qfile_locate_tuple_value (tpl, index,
							    &ptr, &length);
  if (flag == V_BOUND)
    {
      pr_type = domain->type;
      if (pr_type == NULL)
	{
	  return ER_FAILED;
	}

      OR_BUF_INIT (buf, ptr, length);

      if ((*(pr_type->writeval)) (&buf, valp) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  else
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * qexec_get_tuple_column_value () - helper function for reading a column
 *    value from a tuple
 *  return:
 *  tpl(in):
 *  index(in):
 *  valp(out):
 *  domain(in):
 */
int
qexec_get_tuple_column_value (QFILE_TUPLE tpl, int index,
			      DB_VALUE * valp, TP_DOMAIN * domain)
{
  QFILE_TUPLE_VALUE_FLAG flag;
  char *ptr;
  int length;
  PR_TYPE *pr_type;
  OR_BUF buf;

  flag = (QFILE_TUPLE_VALUE_FLAG) qfile_locate_tuple_value (tpl, index,
							    &ptr, &length);
  if (flag == V_BOUND)
    {
      pr_type = domain->type;
      if (pr_type == NULL)
	{
	  return ER_FAILED;
	}

      OR_BUF_INIT (buf, ptr, length);

      if ((*(pr_type->readval)) (&buf, valp, domain,
				 -1, false, NULL, 0) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  else
    {
      DB_MAKE_NULL (valp);
    }

  return NO_ERROR;
}

/*
 * qexec_check_for_cycle () - check the tuple described by the outptr_list
 *    to see if it is ancestor of tpl
 *  return:
 *  outptr_list(in):
 *  tpl(in):
 *  type_list(in):
 *  list_id_p(in):
 *  iscycle(out):
 */
static int
qexec_check_for_cycle (THREAD_ENTRY * thread_p, OUTPTR_LIST * outptr_list,
		       QFILE_TUPLE tpl,
		       QFILE_TUPLE_VALUE_TYPE_LIST * type_list,
		       QFILE_LIST_ID * list_id_p, int *iscycle)
{
  DB_VALUE p_pos_dbval;
  QFILE_LIST_SCAN_ID s_id;
  QFILE_TUPLE_RECORD tuple_rec = { (QFILE_TUPLE) NULL, 0 };
  QFILE_TUPLE_POSITION p_pos, *bitval;
  int length;

  if (qfile_open_list_scan (list_id_p, &s_id) != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* we start with tpl itself */
  tuple_rec.tpl = tpl;

  do
    {
      if (qexec_compare_valptr_with_tuple (outptr_list, tuple_rec.tpl,
					   type_list, iscycle) != NO_ERROR)
	{
	  qfile_close_scan (thread_p, &s_id);
	  return ER_FAILED;
	}

      if (*iscycle)
	{
	  break;
	}

      /* get the parent node */
      if (qexec_get_tuple_column_value (tuple_rec.tpl,
					outptr_list->valptr_cnt -
					PCOL_PARENTPOS_TUPLE_OFFSET,
					&p_pos_dbval,
					&tp_Bit_domain) != NO_ERROR)
	{
	  qfile_close_scan (thread_p, &s_id);
	  return ER_FAILED;
	}

      bitval = (QFILE_TUPLE_POSITION *) DB_GET_BIT (&p_pos_dbval, &length);

      if (bitval)
	{
	  p_pos.status = s_id.status;
	  p_pos.position = S_ON;
	  p_pos.vpid = bitval->vpid;
	  p_pos.offset = bitval->offset;
	  p_pos.tpl = NULL;
	  p_pos.tplno = bitval->tplno;

	  if (qfile_jump_scan_tuple_position (thread_p, &s_id, &p_pos,
					      &tuple_rec, PEEK) != S_SUCCESS)
	    {
	      qfile_close_scan (thread_p, &s_id);
	      return ER_FAILED;
	    }
	}
    }
  while (bitval);		/* the parent tuple pos is null for the root node */

  qfile_close_scan (thread_p, &s_id);

  return NO_ERROR;

}

/*
 * qexec_compare_valptr_with_tuple () - compare the tuple described by
 *    outptr_list to see if it is equal to tpl; ignore pseudo-columns
 *  return:
 *  outptr_list(in):
 *  tpl(in):
 *  type_list(in):
 *  are_equal(out):
 */
static int
qexec_compare_valptr_with_tuple (OUTPTR_LIST * outptr_list, QFILE_TUPLE tpl,
				 QFILE_TUPLE_VALUE_TYPE_LIST * type_list,
				 int *are_equal)
{
  REGU_VARIABLE_LIST regulist;
  QFILE_TUPLE tuple;
  OR_BUF buf;
  DB_VALUE dbval1, *dbvalp2;
  PR_TYPE *pr_type_p;
  DB_TYPE type;
  TP_DOMAIN *domp;
  int length1, length2, equal, copy, i;

  *are_equal = 1;

  tuple = tpl + QFILE_TUPLE_LENGTH_SIZE;
  regulist = outptr_list->valptrp;
  i = 0;

  while (regulist && i < outptr_list->valptr_cnt - PCOL_FIRST_TUPLE_OFFSET)
    {
      /* compare regulist->value.value.dbvalptr to the DB_VALUE from tuple */

      dbvalp2 = regulist->value.value.dbvalptr;
      length2 = (dbvalp2->domain.general_info.is_null != 0) ? 0 : -1;

      domp = type_list->domp[i];
      type = domp->type->id;
      copy = pr_is_set_type (type);
      pr_type_p = domp->type;

      length1 = QFILE_GET_TUPLE_VALUE_LENGTH (tuple);

      /* zero length means NULL */
      if (length1 == 0)
	{
	  db_make_null (&dbval1);
	}
      else
	{
	  or_init (&buf, (char *) tuple + QFILE_TUPLE_VALUE_HEADER_SIZE,
		   length1);
	  if ((*(pr_type_p->readval)) (&buf, &dbval1, domp, -1, copy,
				       NULL, 0) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	}

      if (length1 == 0 && length2 == 0)
	{
	  equal = 1;
	}
      else if (length1 == 0)
	{
	  equal = 0;
	}
      else if (length2 == 0)
	{
	  equal = 0;
	}
      else
	{
	  equal = ((*(pr_type_p->cmpval)) (&dbval1, dbvalp2, NULL, 0, 0,
					   1, NULL) == DB_EQ);
	}

      if (copy)
	{
	  pr_clear_value (&dbval1);
	}

      if (!equal)
	{
	  *are_equal = 0;
	  break;
	}

      tuple +=
	QFILE_TUPLE_VALUE_HEADER_SIZE + QFILE_GET_TUPLE_VALUE_LENGTH (tuple);
      regulist = regulist->next;
      i++;
    }

  if (i < outptr_list->valptr_cnt - PCOL_FIRST_TUPLE_OFFSET)
    *are_equal = 0;

  return NO_ERROR;
}

/*
 * qexec_init_index_pseudocolumn () - index pseudocolumn strings initialization
 *   return:
 *  father_index(out): father index string
 *  len_father_index(out): father index string allocation length
 *  son_index(out): son index string
 *  len_son_index(out): son index string allocation length
 */
static int
qexec_init_index_pseudocolumn_strings (THREAD_ENTRY * thread_p,
				       char **father_index,
				       int *len_father_index,
				       char **son_index, int *len_son_index)
{
  *len_father_index = CONNECTBY_TUPLE_INDEX_STRING_MEM;
  *father_index = (char *) db_private_alloc (thread_p, *len_father_index);

  if ((*father_index) == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  memset (*father_index, 0, *len_father_index);

  *len_son_index = CONNECTBY_TUPLE_INDEX_STRING_MEM;
  *son_index = (char *) db_private_alloc (thread_p, *len_son_index);

  if ((*son_index) == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  memset (*son_index, 0, *len_son_index);

  return NO_ERROR;
}

/*
 * compute_son_index () -
 *   return:
 *  son_index(out): son index string which will be father_index + "." + cnt
 *  father_index(in): father's index string
 *  len_son_index(in/out): current son's index string allocation length
 *  cnt(in):
 */
static int
compute_son_index (THREAD_ENTRY * thread_p,
		   char **son_index, char *father_index,
		   int *len_son_index, int cnt)
{
  char counter[32];
  int n = father_index ? strlen (father_index) : 0;

  snprintf (counter, 32, "%d", cnt);

  /* double the size, if more space needed */
  while (n + 1 + strlen (counter) > *len_son_index)
    {
      *len_son_index += CONNECTBY_TUPLE_INDEX_STRING_MEM;
      db_private_free_and_init (thread_p, *son_index);
      *son_index = (char *) db_private_alloc (thread_p, *len_son_index);
      if ((*son_index) == NULL)
	{
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      memset (*son_index, 0, *len_son_index);
    }

  if (father_index)
    {
      strcpy (*son_index, father_index);
    }
  if (n > 0)
    {
      strcat (*son_index, ".");	/* '.' < '0'...'9' */
    }
  strcat (*son_index, counter);

  return NO_ERROR;
}

/*
 * qexec_listfile_orderby () - sorts a listfile according to a orderby list
 *    return: NO_ERROR, or ER_code
 *   list_file(in): listfile to sort
 *   orderby_list(in): orderby list with sort columns
 *   xasl_state(in): xasl state
 *   outptr_list(in): xasl outptr list
 */
static int
qexec_listfile_orderby (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_file,
			SORT_LIST * orderby_list, XASL_STATE * xasl_state,
			OUTPTR_LIST * outptr_list)
{
  QFILE_LIST_ID *list_id = list_file;
  int n, i;
  ORDBYNUM_INFO ordby_info;
  REGU_VARIABLE_LIST regu_list;

  if (orderby_list != NULL)
    {
      if (orderby_list && qfile_is_sort_list_covered (list_id->sort_list,
						      orderby_list) == true)
	{
	  /* no need to sort here */
	}
      else
	{
	  /* sort the list file */
	  ordby_info.ordbynum_pos_cnt = 0;
	  ordby_info.ordbynum_pos = ordby_info.reserved;
	  if (outptr_list)
	    {
	      for (n = 0, regu_list = outptr_list->valptrp; regu_list;
		   regu_list = regu_list->next)
		{
		  if (regu_list->value.type == TYPE_ORDERBY_NUM)
		    {
		      n++;
		    }
		}
	      ordby_info.ordbynum_pos_cnt = n;
	      if (n > 2)
		{
		  ordby_info.ordbynum_pos =
		    (int *) db_private_alloc (thread_p, sizeof (int) * n);
		  if (ordby_info.ordbynum_pos == NULL)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}
	      for (n = 0, i = 0, regu_list = outptr_list->valptrp; regu_list;
		   regu_list = regu_list->next, i++)
		{
		  if (regu_list->value.type == TYPE_ORDERBY_NUM)
		    {
		      ordby_info.ordbynum_pos[n++] = i;
		    }
		}
	    }

	  ordby_info.xasl_state = xasl_state;
	  ordby_info.ordbynum_pred = NULL;
	  ordby_info.ordbynum_val = NULL;
	  ordby_info.ordbynum_flag = 0;

	  list_id = qfile_sort_list_with_func (thread_p, list_id,
					       orderby_list, Q_ALL,
					       QFILE_FLAG_ALL,
					       NULL, NULL, NULL, &ordby_info);

	  if (ordby_info.ordbynum_pos != ordby_info.reserved)
	    {
	      db_private_free_and_init (thread_p, ordby_info.ordbynum_pos);
	    }

	  if (list_id == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }				/* if */

  return NO_ERROR;

exit_on_error:

  return ER_FAILED;
}

/*
 * qexec_set_pseudocolumns_val_pointers () - setup pseudocolumns value pointers
 *    return:
 *  xasl(in):
 *  level_valp(out):
 *  isleaf_valp(out):
 *  iscycle_valp(out):
 *  parent_pos_valp(out):
 *  index_valp(out):
 */
static void
qexec_set_pseudocolumns_val_pointers (XASL_NODE * xasl,
				      DB_VALUE ** level_valp,
				      DB_VALUE ** isleaf_valp,
				      DB_VALUE ** iscycle_valp,
				      DB_VALUE ** parent_pos_valp,
				      DB_VALUE ** index_valp)
{
  REGU_VARIABLE_LIST regulist;
  int i, n;

  i = 0;
  n = xasl->outptr_list->valptr_cnt;
  regulist = xasl->outptr_list->valptrp;

  while (regulist)
    {
      if (i == n - PCOL_PARENTPOS_TUPLE_OFFSET)
	{
	  *parent_pos_valp = regulist->value.value.dbvalptr;
	}

      if (i == n - PCOL_LEVEL_TUPLE_OFFSET)
	{
	  *level_valp = regulist->value.value.dbvalptr;
	}

      if (i == n - PCOL_ISLEAF_TUPLE_OFFSET)
	{
	  *isleaf_valp = regulist->value.value.dbvalptr;
	}

      if (i == n - PCOL_ISCYCLE_TUPLE_OFFSET)
	{
	  *iscycle_valp = regulist->value.value.dbvalptr;
	}

      if (i == n - PCOL_INDEX_STRING_TUPLE_OFFSET)
	{
	  *index_valp = regulist->value.value.dbvalptr;
	}

      regulist = regulist->next;
      i++;
    }

  i = 0;
  n = xasl->proc.connect_by.prior_outptr_list->valptr_cnt;
  regulist = xasl->proc.connect_by.prior_outptr_list->valptrp;

  while (regulist)
    {
      if (i == n - PCOL_PARENTPOS_TUPLE_OFFSET)
	{
	  regulist->value.value.dbvalptr = *parent_pos_valp;
	}

      if (i == n - PCOL_LEVEL_TUPLE_OFFSET)
	{
	  regulist->value.value.dbvalptr = *level_valp;
	}

      if (i == n - PCOL_ISLEAF_TUPLE_OFFSET)
	{
	  regulist->value.value.dbvalptr = *isleaf_valp;
	}

      if (i == n - PCOL_ISCYCLE_TUPLE_OFFSET)
	{
	  regulist->value.value.dbvalptr = *iscycle_valp;
	}

      if (i == n - PCOL_INDEX_STRING_TUPLE_OFFSET)
	{
	  regulist->value.value.dbvalptr = *index_valp;
	}

      regulist = regulist->next;
      i++;
    }
}

/*
 * qexec_get_index_pseudocolumn_value_from_tuple () -
 *    return:
 *  xasl(in):
 *  tpl(in):
 *  index_valp(out):
 *  index_value(out):
 *  index_len(out):
 */
static int
qexec_get_index_pseudocolumn_value_from_tuple (THREAD_ENTRY * thread_p,
					       XASL_NODE * xasl,
					       QFILE_TUPLE tpl,
					       DB_VALUE ** index_valp,
					       char **index_value,
					       int *index_len)
{
  if (qexec_get_tuple_column_value (tpl,
				    xasl->outptr_list->valptr_cnt -
				    PCOL_INDEX_STRING_TUPLE_OFFSET,
				    *index_valp,
				    &tp_String_domain) != NO_ERROR)
    {
      return ER_FAILED;
    }

  if (!db_value_is_null (*index_valp))
    {
      /* increase the size if more space needed */
      while (strlen ((*index_valp)->data.ch.medium.buf) + 1 > *index_len)
	{
	  (*index_len) += CONNECTBY_TUPLE_INDEX_STRING_MEM;
	  db_private_free_and_init (thread_p, *index_value);
	  *index_value = (char *) db_private_alloc (thread_p, *index_len);

	  if (!(*index_value))
	    {
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	}

      strcpy (*index_value, (*index_valp)->data.ch.medium.buf);
    }

  return NO_ERROR;
}

/*
 * qexec_recalc_tuples_parent_pos_in_list () - recalculate the parent position
 *	in list for each tuple and update the parent_pos pseudocolumn
 *    return:
 *  list_id_p(in): The list file.
 *
 * Note: We need the parent positions for:
 *	- supporting PRIOR operator in SELECT list
 *	- SYS_CONNECT_BY_PATH()
 *	- CONNECT_BY_ROOT
 */
static int
qexec_recalc_tuples_parent_pos_in_list (THREAD_ENTRY * thread_p,
					QFILE_LIST_ID * list_id_p)
{
  PARENT_POS_INFO *pos_info_p, *prev_pos_info_p;
  DB_VALUE level_dbval, parent_pos_dbval;
  QFILE_LIST_SCAN_ID s_id, prev_s_id;
  QFILE_TUPLE_RECORD tuple_rec = { (QFILE_TUPLE) NULL, 0 };
  QFILE_TUPLE_RECORD prev_tuple_rec = { (QFILE_TUPLE) NULL, 0 };
  SCAN_CODE scan, prev_scan;
  int level, prev_level, i;
  bool started;

  prev_s_id.status = S_CLOSED;

  /* always empty bottom of the stack, just to be there */
  pos_info_p =
    (PARENT_POS_INFO *) db_private_alloc (thread_p, sizeof (PARENT_POS_INFO));
  if (pos_info_p == NULL)
    {
      goto exit_on_error;
    }
  memset ((void *) pos_info_p, 0, sizeof (PARENT_POS_INFO));

  if (qfile_open_list_scan (list_id_p, &s_id) != NO_ERROR)
    {
      goto exit_on_error;
    }
  if (qfile_open_list_scan (list_id_p, &prev_s_id) != NO_ERROR)
    {
      goto exit_on_error;
    }

  prev_level = 1;
  started = false;
  prev_scan = S_END;

  while (1)
    {
      scan = qfile_scan_list_next (thread_p, &s_id, &tuple_rec, PEEK);
      if (scan != S_SUCCESS)
	{
	  break;
	}

      if (started)
	{
	  prev_scan = qfile_scan_list_next (thread_p, &prev_s_id,
					    &prev_tuple_rec, PEEK);
	  if (prev_scan != S_SUCCESS)
	    {
	      break;
	    }
	}
      else
	{
	  started = true;
	}

      if (qexec_get_tuple_column_value (tuple_rec.tpl,
					list_id_p->type_list.type_cnt -
					PCOL_LEVEL_TUPLE_OFFSET, &level_dbval,
					&tp_Integer_domain) != NO_ERROR)
	{
	  goto exit_on_error;
	}

      level = DB_GET_INTEGER (&level_dbval);

      if (level == prev_level)
	{
	  /* the tuple is on the same level as the prev tuple */

	  if (!pos_info_p)
	    {
	      goto exit_on_error;
	    }

	  if (level > 1)
	    {
	      /* set parent position pseudocolumn value */
	      DB_MAKE_BIT (&parent_pos_dbval, DB_DEFAULT_PRECISION,
			   (void *) &pos_info_p->tpl_pos,
			   sizeof (pos_info_p->tpl_pos) * 8);

	      if (qexec_set_tuple_column_value (tuple_rec.tpl,
						list_id_p->type_list.
						type_cnt -
						PCOL_PARENTPOS_TUPLE_OFFSET,
						&parent_pos_dbval,
						&tp_Bit_domain) != NO_ERROR)
		{
		  goto exit_on_error;
		}
	    }
	}
      else if (level > prev_level)
	{
	  /* the tuple is child of the previous one */

	  if (prev_scan == S_END)
	    {
	      goto exit_on_error;	/* this should not be possible */
	    }

	  prev_pos_info_p = pos_info_p;
	  pos_info_p =
	    (PARENT_POS_INFO *) db_private_alloc (thread_p,
						  sizeof (PARENT_POS_INFO));
	  if (pos_info_p == NULL)
	    {
	      pos_info_p = prev_pos_info_p;
	      goto exit_on_error;
	    }
	  pos_info_p->stack = prev_pos_info_p;

	  qfile_save_current_scan_tuple_position (&prev_s_id,
						  &pos_info_p->tpl_pos);

	  DB_MAKE_BIT (&parent_pos_dbval, DB_DEFAULT_PRECISION,
		       (void *) &pos_info_p->tpl_pos,
		       sizeof (pos_info_p->tpl_pos) * 8);

	  if (qexec_set_tuple_column_value (tuple_rec.tpl,
					    list_id_p->type_list.type_cnt -
					    PCOL_PARENTPOS_TUPLE_OFFSET,
					    &parent_pos_dbval,
					    &tp_Bit_domain) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
      else
	{
	  /* level < prev_level */

	  for (i = level; i < prev_level; i++)
	    {
	      if (pos_info_p)
		{
		  prev_pos_info_p = pos_info_p->stack;
		  db_private_free_and_init (thread_p, pos_info_p);
		  pos_info_p = prev_pos_info_p;
		}
	      else
		{
		  goto exit_on_error;
		}
	    }

	  if (pos_info_p == NULL)
	    {
	      goto exit_on_error;
	    }

	  if (level > 1)
	    {
	      DB_MAKE_BIT (&parent_pos_dbval, DB_DEFAULT_PRECISION,
			   (void *) &pos_info_p->tpl_pos,
			   sizeof (pos_info_p->tpl_pos) * 8);

	      if (qexec_set_tuple_column_value (tuple_rec.tpl,
						list_id_p->type_list.
						type_cnt -
						PCOL_PARENTPOS_TUPLE_OFFSET,
						&parent_pos_dbval,
						&tp_Bit_domain) != NO_ERROR)
		{
		  goto exit_on_error;
		}
	    }
	}

      prev_level = level;
    }

  if (scan != S_END)
    {
      goto exit_on_error;
    }
  if (prev_scan != S_END && prev_scan != S_SUCCESS)
    {
      goto exit_on_error;
    }

  qfile_close_scan (thread_p, &s_id);
  qfile_close_scan (thread_p, &prev_s_id);

  while (pos_info_p)
    {
      prev_pos_info_p = pos_info_p->stack;
      db_private_free_and_init (thread_p, pos_info_p);
      pos_info_p = prev_pos_info_p;
    }

  return NO_ERROR;

exit_on_error:

  qfile_close_scan (thread_p, &s_id);
  qfile_close_scan (thread_p, &prev_s_id);

  while (pos_info_p)
    {
      prev_pos_info_p = pos_info_p->stack;
      db_private_free_and_init (thread_p, pos_info_p);
      pos_info_p = prev_pos_info_p;
    }

  return ER_FAILED;
}

/*
 * qexec_start_connect_by_lists () - initializes the START WITH list file and
 *	the CONNECT BY input list file
 *    return:
 *  xasl(in): CONNECT BY xasl
 *  xasl_state(in):
 */
static int
qexec_start_connect_by_lists (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			      XASL_STATE * xasl_state)
{
  QFILE_TUPLE_VALUE_TYPE_LIST type_list;
  QFILE_LIST_ID *t_list_id = NULL;
  CONNECTBY_PROC_NODE *connect_by = &xasl->proc.connect_by;
  REGU_VARIABLE_LIST regulist;
  int i;

  if (qdata_get_valptr_type_list (xasl->outptr_list, &type_list) != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (connect_by->start_with_list_id->type_list.type_cnt == 0)
    {
      t_list_id = qfile_open_list (thread_p, &type_list, NULL,
				   xasl_state->query_id, 0);
      if (t_list_id == NULL)
	{
	  goto exit_on_error;
	}

      if (qfile_copy_list_id (connect_by->start_with_list_id, t_list_id,
			      true) != NO_ERROR)
	{
	  goto exit_on_error;
	}

      qfile_free_list_id (t_list_id);
    }

  if (connect_by->input_list_id->type_list.type_cnt == 0)
    {
      t_list_id = qfile_open_list (thread_p, &type_list, NULL,
				   xasl_state->query_id, 0);
      if (t_list_id == NULL)
	{
	  goto exit_on_error;
	}

      if (qfile_copy_list_id (connect_by->input_list_id, t_list_id, true)
	  != NO_ERROR)
	{
	  goto exit_on_error;
	}

      qfile_free_list_id (t_list_id);
    }

  if (type_list.domp)
    {
      db_private_free_and_init (thread_p, type_list.domp);
    }

  /* reset pseudocolumn values */
  i = 0;
  regulist = xasl->outptr_list->valptrp;
  while (regulist)
    {
      /* parent position pseudocolumn */
      if (i == xasl->outptr_list->valptr_cnt - PCOL_PARENTPOS_TUPLE_OFFSET)
	{
	  DB_MAKE_BIT (regulist->value.value.dbvalptr, DB_DEFAULT_PRECISION,
		       NULL, 8);
	}
      /* string index pseudocolumn */
      if (i == xasl->outptr_list->valptr_cnt - PCOL_INDEX_STRING_TUPLE_OFFSET)
	{
	  DB_MAKE_STRING (regulist->value.value.dbvalptr, "");
	}
      /* level pseudocolumn */
      if (i == xasl->outptr_list->valptr_cnt - PCOL_LEVEL_TUPLE_OFFSET)
	{
	  DB_MAKE_INT (regulist->value.value.dbvalptr, 0);
	}
      /* connect_by_isleaf pseudocolumn */
      if (i == xasl->outptr_list->valptr_cnt - PCOL_ISLEAF_TUPLE_OFFSET)
	{
	  DB_MAKE_INT (regulist->value.value.dbvalptr, 0);
	}
      /* connect_by_iscycle pseudocolumn */
      if (i == xasl->outptr_list->valptr_cnt - PCOL_ISCYCLE_TUPLE_OFFSET)
	{
	  DB_MAKE_INT (regulist->value.value.dbvalptr, 0);
	}

      regulist = regulist->next;
      i++;
    }

  return NO_ERROR;

exit_on_error:

  if (type_list.domp)
    {
      db_private_free_and_init (thread_p, type_list.domp);
    }

  if (t_list_id)
    {
      qfile_free_list_id (t_list_id);
    }

  return ER_FAILED;
}

/*
 * qexec_update_connect_by_lists () - updates the START WITH list file and
 *	the CONNECT BY input list file with new data
 *    return:
 *  xasl(in): CONNECT BY xasl
 *  xasl_state(in):
 *  tplrec(in):
 */
static int
qexec_update_connect_by_lists (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			       XASL_STATE * xasl_state,
			       QFILE_TUPLE_RECORD * tplrec)
{
  DB_LOGICAL ev_res;
  CONNECTBY_PROC_NODE *connect_by = &xasl->proc.connect_by;

  /* evaluate START WITH predicate */
  ev_res = V_UNKNOWN;
  if (connect_by->start_with_pred != NULL)
    {
      ev_res = eval_pred (thread_p, connect_by->start_with_pred,
			  &xasl_state->vd, NULL);
      if (ev_res == V_ERROR)
	{
	  return ER_FAILED;
	}
    }

  if (connect_by->start_with_pred == NULL || ev_res == V_TRUE)
    {
      /* create tuple and add it to both input_list_id and start_with_list_id */
      if (qdata_copy_valptr_list_to_tuple (thread_p,
					   xasl->outptr_list,
					   &xasl_state->vd,
					   tplrec) != NO_ERROR)
	{
	  return ER_FAILED;
	}

      if (!connect_by->single_table_opt)
	{
	  if (qfile_add_tuple_to_list (thread_p, connect_by->input_list_id,
				       tplrec->tpl) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	}

      if (qfile_add_tuple_to_list (thread_p, connect_by->start_with_list_id,
				   tplrec->tpl) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  else
    {
      /* create tuple only in input_list_id */
      if (qexec_insert_tuple_into_list (thread_p, connect_by->input_list_id,
					xasl->outptr_list, xasl_state,
					tplrec) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

/*
 * qexec_end_connect_by_lists () - closes the START WITH list file and
 *	the CONNECT BY input list file
 *    return:
 *  xasl(in): CONNECT BY xasl
 */
static void
qexec_end_connect_by_lists (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  CONNECTBY_PROC_NODE *connect_by = &xasl->proc.connect_by;

  qfile_close_list (thread_p, connect_by->start_with_list_id);
  qfile_close_list (thread_p, connect_by->input_list_id);
}

/*
 * qexec_clear_connect_by_lists () - clears the START WITH list file and
 *	the CONNECT BY input list file
 *    return:
 *  xasl(in): CONNECT BY xasl
 */
static void
qexec_clear_connect_by_lists (XASL_NODE * xasl)
{
  CONNECTBY_PROC_NODE *connect_by = &xasl->proc.connect_by;

  qfile_clear_list_id (connect_by->input_list_id);
  qfile_clear_list_id (connect_by->start_with_list_id);
}

/*
 * qexec_iterate_connect_by_results () - scan CONNECT BY results, apply WHERE,
 *					 add to xasl->list_id
 *    return:
 *  xasl(in): SELECT xasl
 *  xasl_state(in):
 */
static int
qexec_iterate_connect_by_results (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				  XASL_STATE * xasl_state,
				  QFILE_TUPLE_RECORD * tplrec)
{
  CONNECTBY_PROC_NODE *connect_by = &xasl->connect_by_ptr->proc.connect_by;
  QFILE_LIST_SCAN_ID s_id;
  QFILE_TUPLE_RECORD tuple_rec = { (QFILE_TUPLE) NULL, 0 };
  SCAN_CODE scan;
  DB_VALUE *dbvalp;
  DB_LOGICAL ev_res;
  bool qualified;

  if (qfile_open_list_scan (xasl->connect_by_ptr->list_id, &s_id) != NO_ERROR)
    {
      return ER_FAILED;
    }

  while (1)
    {
      scan = qfile_scan_list_next (thread_p, &s_id, &tuple_rec, PEEK);
      if (scan != S_SUCCESS)
	{
	  break;
	}

      connect_by->curr_tuple = tuple_rec.tpl;

      /* fetch LEVEL pseudocolumn value */
      if (xasl->level_val)
	{
	  if (fetch_peek_dbval (thread_p, xasl->level_regu, &xasl_state->vd,
				NULL, NULL, tuple_rec.tpl,
				&dbvalp) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}

      /* fetch CONNECT_BY_ISLEAF pseudocolumn value */
      if (xasl->isleaf_val)
	{
	  if (fetch_peek_dbval (thread_p, xasl->isleaf_regu, &xasl_state->vd,
				NULL, NULL, tuple_rec.tpl,
				&dbvalp) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}

      /* fetch CONNECT_BY_ISCYCLE pseudocolumn value */
      if (xasl->iscycle_val)
	{
	  if (fetch_peek_dbval (thread_p, xasl->iscycle_regu, &xasl_state->vd,
				NULL, NULL, tuple_rec.tpl,
				&dbvalp) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}

      /* fetch pred part of xasl->connect_by_ptr->val_list from the tuple */
      if (fetch_val_list (thread_p, connect_by->after_cb_regu_list_pred,
			  &xasl_state->vd,
			  NULL, NULL, tuple_rec.tpl, PEEK) != NO_ERROR)
	{
	  goto exit_on_error;
	}

      /* evaluate after_connect_by predicate */
      ev_res = V_UNKNOWN;
      if (connect_by->after_connect_by_pred != NULL)
	{
	  ev_res = eval_pred (thread_p, connect_by->after_connect_by_pred,
			      &xasl_state->vd, NULL);
	  if (ev_res == V_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
      qualified = (connect_by->after_connect_by_pred == NULL
		   || ev_res == V_TRUE);

      if (qualified)
	{
	  /* fetch the rest of xasl->connect_by_ptr->val_list from the tuple */
	  if (fetch_val_list (thread_p, connect_by->after_cb_regu_list_rest,
			      &xasl_state->vd,
			      NULL, NULL, tuple_rec.tpl, PEEK) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  /* evaluate inst_num predicate */
	  if (xasl->instnum_val)
	    {
	      ev_res = qexec_eval_instnum_pred (thread_p, xasl, xasl_state);
	      if (ev_res == V_ERROR)
		{
		  goto exit_on_error;
		}

	      if (xasl->instnum_flag & XASL_INSTNUM_FLAG_SCAN_STOP)
		{
		  qfile_close_scan (thread_p, &s_id);
		  return NO_ERROR;
		}
	    }
	  qualified = (xasl->instnum_pred == NULL || ev_res == V_TRUE);

	  if (qualified
	      && qexec_end_one_iteration (thread_p, xasl, xasl_state,
					  tplrec) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}

    }

  if (scan != S_END)
    {
      goto exit_on_error;
    }

  qfile_close_scan (thread_p, &s_id);

  return NO_ERROR;

exit_on_error:
  qfile_close_scan (thread_p, &s_id);
  return ER_FAILED;
}

/*
 * qexec_gby_finalize_rollup_group () -
 *   return:
 *   gbstate(in):
 *   rollup_level(in):
 */
static void
qexec_gby_finalize_rollup_group (THREAD_ENTRY * thread_p,
				 GROUPBY_STATE * gbstate, int rollup_level)
{
  QFILE_TUPLE_DESCRIPTOR *tdp;
  QFILE_LIST_ID *xlist_id;
  QPROC_TPLDESCR_STATUS tpldescr_status;
  XASL_NODE *xptr;
  DB_LOGICAL ev_res;
  XASL_STATE *xasl_state = gbstate->xasl_state;

  if (gbstate->state != NO_ERROR)
    {
      return;
    }

  if (gbstate->g_rollup_agg_list == NULL
      || rollup_level >= gbstate->rollup_levels
      || gbstate->g_rollup_agg_list[rollup_level] == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (qdata_finalize_aggregate_list (thread_p,
				     gbstate->g_rollup_agg_list[rollup_level])
      != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* evaluate subqueries in HAVING predicate */
  for (xptr = gbstate->eptr_list; xptr; xptr = xptr->next)
    {
      if (qexec_execute_mainblock (thread_p, xptr, gbstate->xasl_state) !=
	  NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* evaluate HAVING preadicate */
  ev_res = (gbstate->having_pred
	    ? eval_pred (thread_p, gbstate->having_pred,
			 &gbstate->xasl_state->vd, NULL) : V_TRUE);
  if (ev_res == V_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* evaluate groupby_num predicates */
  if (ev_res == V_TRUE)
    {
      if (gbstate->grbynum_val)
	{
	  ev_res = qexec_eval_grbynum_pred (thread_p, gbstate);
	  if (ev_res == V_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

  if (ev_res != V_TRUE)
    {
      goto wrapup;
    }

  /* make f_valp array */
  xlist_id = gbstate->output_file;
  if ((xlist_id->tpl_descr.f_valp == NULL)
      && (xlist_id->type_list.type_cnt > 0))
    {
      xlist_id->tpl_descr.f_valp =
	(DB_VALUE **) malloc (xlist_id->type_list.type_cnt *
			      sizeof (DB_VALUE *));
      if (xlist_id->tpl_descr.f_valp == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /*
   * bind constant regu variables corresponding to aggregates in g_outptr_list
   * to db values computed by rollup aggregates
   */
  if (gbstate->g_outptr_list)	/* sanity check */
    {
      AGGREGATE_TYPE *agg_list;
      AGGREGATE_TYPE *rollup_agg_list;
      REGU_VARIABLE_LIST list = gbstate->g_outptr_list->valptrp;

      while (list)
	{
	  if (list->value.type == TYPE_CONSTANT)
	    {
	      agg_list = gbstate->g_agg_list;
	      rollup_agg_list = gbstate->g_rollup_agg_list[rollup_level];

	      while (agg_list && rollup_agg_list)
		{
		  if (list->value.value.dbvalptr == agg_list->value)
		    {
		      list->value.value.dbvalptr = rollup_agg_list->value;
		      break;
		    }
		  else if (list->value.value.dbvalptr == agg_list->value2)
		    {
		      list->value.value.dbvalptr = rollup_agg_list->value2;
		      break;
		    }

		  agg_list = agg_list->next;
		  rollup_agg_list = rollup_agg_list->next;
		}
	    }

	  list = list->next;
	}
    }

  /* set to NULL (in the rollup tuple) the columns that failed comparison */
  if (gbstate->g_val_list)
    {
      int i = 0;
      QPROC_DB_VALUE_LIST gby_vallist = gbstate->g_val_list->valp;

      while (gby_vallist)
	{
	  if (i >= rollup_level)
	    {
	      DB_MAKE_NULL (gby_vallist->val);
	    }
	  i++;
	  gby_vallist = gby_vallist->next;
	}
    }

  /* build tuple descriptor */
  tdp = &(gbstate->output_file->tpl_descr);
  tpldescr_status =
    qdata_generate_tuple_desc_for_valptr_list (thread_p,
					       gbstate->g_outptr_list,
					       &gbstate->xasl_state->vd, tdp);

  if (tpldescr_status == QPROC_TPLDESCR_FAILURE)
    {
      GOTO_EXIT_ON_ERROR;
    }

  switch (tpldescr_status)
    {
    case QPROC_TPLDESCR_SUCCESS:
      /* generate tuple into list file page */
      if (qfile_generate_tuple_into_list
	  (thread_p, gbstate->output_file, T_NORMAL) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      break;

    case QPROC_TPLDESCR_RETRY_SET_TYPE:
    case QPROC_TPLDESCR_RETRY_BIG_REC:
      /* BIG QFILE_TUPLE or a SET-field is included */
      if (gbstate->output_tplrec->tpl == NULL)
	{
	  /* allocate tuple descriptor */
	  gbstate->output_tplrec->size = DB_PAGESIZE;
	  gbstate->output_tplrec->tpl =
	    (QFILE_TUPLE) db_private_alloc (thread_p, DB_PAGESIZE);
	  if (gbstate->output_tplrec->tpl == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
      if (qdata_copy_valptr_list_to_tuple
	  (thread_p, gbstate->g_outptr_list, &gbstate->xasl_state->vd,
	   gbstate->output_tplrec) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      if (qfile_add_tuple_to_list (thread_p, gbstate->output_file,
				   gbstate->output_tplrec->tpl) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      break;

    default:
      break;
    }

wrapup:
  /*
   * restore db values bindings for constant regu variables corresponding to
   * aggregates in g_outptr_list
   */
  if (gbstate->g_outptr_list)	/* sanity check */
    {
      AGGREGATE_TYPE *agg_list;
      AGGREGATE_TYPE *rollup_agg_list;
      REGU_VARIABLE_LIST list = gbstate->g_outptr_list->valptrp;

      while (list)
	{
	  if (list->value.type == TYPE_CONSTANT)
	    {
	      agg_list = gbstate->g_agg_list;
	      rollup_agg_list = gbstate->g_rollup_agg_list[rollup_level];

	      while (agg_list && rollup_agg_list)
		{
		  if (list->value.value.dbvalptr == rollup_agg_list->value)
		    {
		      list->value.value.dbvalptr = agg_list->value;
		      break;
		    }
		  else if (list->value.value.dbvalptr ==
			   rollup_agg_list->value2)
		    {
		      list->value.value.dbvalptr = agg_list->value2;
		      break;
		    }

		  agg_list = agg_list->next;
		  rollup_agg_list = rollup_agg_list->next;
		}
	    }

	  list = list->next;
	}
    }

  QEXEC_CLEAR_AGG_LIST_VALUE (gbstate->g_rollup_agg_list[rollup_level]);
  return;

exit_on_error:
  gbstate->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_gby_start_rollup_group () -
 *   return:
 *   gbstate(in):
 *   key(in):
 *   rollup_level(in):
 */
static void
qexec_gby_start_rollup_group (THREAD_ENTRY * thread_p,
			      GROUPBY_STATE * gbstate, const RECDES * key,
			      int rollup_level)
{
  XASL_STATE *xasl_state = gbstate->xasl_state;
  int error;

  if (gbstate->state != NO_ERROR)
    {
      return;
    }

  if (gbstate->g_rollup_agg_list == NULL
      || rollup_level >= gbstate->rollup_levels
      || gbstate->g_rollup_agg_list[rollup_level] == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* (Re)initialize the various accumulator variables... */
  error =
    qdata_initialize_aggregate_list (thread_p,
				     gbstate->g_rollup_agg_list[rollup_level],
				     gbstate->xasl_state->query_id);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

wrapup:
  return;

exit_on_error:
  gbstate->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_initialize_groupby_rollup () - initialize rollup aggregates lists
 *   return:
 *   gbstate(in):
 */
static int
qexec_initialize_groupby_rollup (GROUPBY_STATE * gbstate)
{
  int i;
  AGGREGATE_TYPE *agg, *aggp, *aggr;

  if (gbstate == NULL)		/* sanity check */
    {
      return ER_FAILED;
    }

  gbstate->rollup_levels = gbstate->key_info.nkeys;

  if (gbstate->rollup_levels > 0)
    {
      gbstate->g_rollup_agg_list =
	(AGGREGATE_TYPE **) db_private_alloc (NULL,
					      gbstate->rollup_levels *
					      sizeof (AGGREGATE_TYPE *));
      if (gbstate->g_rollup_agg_list == NULL)
	{
	  return ER_FAILED;
	}

      if (gbstate->g_agg_list)
	{
	  for (i = 0; i < gbstate->rollup_levels; i++)
	    {
	      agg = gbstate->g_agg_list;
	      gbstate->g_rollup_agg_list[i] = aggp =
		(AGGREGATE_TYPE *) db_private_alloc (NULL,
						     sizeof (AGGREGATE_TYPE));
	      if (aggp == NULL)
		{
		  return ER_FAILED;
		}
	      memcpy (gbstate->g_rollup_agg_list[i], agg,
		      sizeof (AGGREGATE_TYPE));
	      gbstate->g_rollup_agg_list[i]->value =
		db_value_copy (agg->value);
	      gbstate->g_rollup_agg_list[i]->value2 =
		db_value_copy (agg->value2);

	      while ((agg = agg->next))
		{
		  aggr =
		    (AGGREGATE_TYPE *) db_private_alloc (NULL,
							 sizeof
							 (AGGREGATE_TYPE));
		  if (aggr == NULL)
		    {
		      return ER_FAILED;
		    }
		  memcpy (aggr, agg, sizeof (AGGREGATE_TYPE));
		  aggr->value = db_value_copy (agg->value);
		  aggr->value2 = db_value_copy (agg->value2);
		  aggp->next = aggr;
		  aggp = aggr;
		}
	    }
	}
      else
	{
	  for (i = 0; i < gbstate->rollup_levels; i++)
	    {
	      gbstate->g_rollup_agg_list[i] = NULL;
	    }
	}
    }
  else
    {
      gbstate->g_rollup_agg_list = NULL;
    }

  return NO_ERROR;
}

/*
 * qexec_clear_groupby_rollup () - clears rollup aggregates lists
 *   return:
 *   gbstate(in):
 */
static void
qexec_clear_groupby_rollup (THREAD_ENTRY * thread_p, GROUPBY_STATE * gbstate)
{
  int i;
  AGGREGATE_TYPE *agg, *next_agg;

  if (gbstate && gbstate->g_rollup_agg_list)
    {
      for (i = 0; i < gbstate->rollup_levels; i++)
	{
	  agg = gbstate->g_rollup_agg_list[i];
	  while (agg)
	    {
	      next_agg = agg->next;
	      pr_free_ext_value (agg->value);
	      pr_free_ext_value (agg->value2);
	      db_private_free (NULL, agg);
	      agg = next_agg;
	    }
	}
      db_private_free_and_init (NULL, gbstate->g_rollup_agg_list);
    }
}

/*
 * qexec_execute_do_stmt () - Execution function for DO statement
 *   return:
 *   xasl(in):
 *   xasl_state(in):
 */
static int
qexec_execute_do_stmt (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		       XASL_STATE * xasl_state)
{
  REGU_VARIABLE_LIST valptr_p = xasl->outptr_list->valptrp;
  DB_VALUE *dbval_p;
  int error = NO_ERROR;

  while (valptr_p)
    {
      error =
	fetch_peek_dbval (thread_p, &valptr_p->value, &xasl_state->vd, NULL,
			  NULL, NULL, &dbval_p);
      if (error != NO_ERROR)
	{
	  break;
	}
      else
	{
	  if (dbval_p)
	    {
	      db_value_clear (dbval_p);
	    }
	  valptr_p = valptr_p->next;
	}
    }

  return error;
}

/*
 * qexec_open_scans () -
 *   return: int
 *   xasl(in) : the BUILDLIST proc
 *   fixed_scan_flag  : the fixed scan flaged decided before calling
 *   readonly_scan : the readonly scan mark decided before calling 
 *
 * Note: Code copied out of qexec_execute_mainblock
 *       for remote scan
 */
int
qexec_open_scans (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		  XASL_STATE * xasl_state, int fixed_scan_flag,
		  int readonly_scan)
{
  int level, spec_level, iscan_oid_order;
  XASL_NODE *xptr;
  ACCESS_SPEC_TYPE *specp, *spec_ptr[2];

  for (xptr = xasl, level = 0; xptr; xptr = xptr->scan_ptr, level++)
    {
      /* consider all the access specification nodes */
      spec_ptr[0] = xptr->spec_list;
      spec_ptr[1] = xptr->merge_spec;

      for (spec_level = 0; spec_level < 2; ++spec_level)
	{
	  for (specp = spec_ptr[spec_level]; specp; specp = specp->next)
	    {
#if defined (SERVER_MODE)
	      /* for remote scan */
	      if ((specp->type == TARGET_CLASS
		   || specp->type == TARGET_CLASS_ATTR)
		  && specp->s.cls_node.node_id != DB_CLUSTER_NODE_LOCAL)
		{
		  continue;
		}

	      if (QEXEC_EMPTY_SPEC_INDEX (specp))
		{
		  continue;
		}
#endif
	      /* we must make scans fixed because non-fixed scans
	       * have never been tested.  There is also some
	       * discussion as to the utility of non fixed scans.
	       */
	      specp->fixed_scan = fixed_scan_flag;

	      /* set if the scan will be done in a grouped manner */
	      if ((level == 0 && xptr->scan_ptr == NULL)
		  && (QPROC_MAX_GROUPED_SCAN_CNT > 0))
		{
		  /* single class query */
		  specp->grouped_scan =
		    ((QPROC_SINGLE_CLASS_GROUPED_SCAN == 1) ? true : false);
		}
	      else if (level < QPROC_MAX_GROUPED_SCAN_CNT)
		{
		  specp->grouped_scan =
		    ((QPROC_MULTI_CLASS_GROUPED_SCAN == 1) ? true : false);
		}
	      else
		{
		  specp->grouped_scan = false;
		}

	      /* a class attribute scan cannot be grouped */
	      if (specp->grouped_scan && specp->type == TARGET_CLASS_ATTR)
		{
		  specp->grouped_scan = false;
		}

	      /* an index scan currently can be grouped, only if
	       * it contains only constant key values
	       */
	      if (specp->grouped_scan
		  && specp->type == TARGET_CLASS
		  && specp->access == INDEX
		  && specp->indexptr->key_info.is_constant == false)
		{
		  specp->grouped_scan = false;
		}

	      /* inner scan of outer join cannot be grouped */
	      if (specp->grouped_scan
		  && specp->single_fetch == QPROC_NO_SINGLE_OUTER)
		{
		  specp->grouped_scan = false;
		}

	      if (specp->grouped_scan && specp->fixed_scan == false)
		{
		  specp->grouped_scan = false;
		}

	      iscan_oid_order = xptr->iscan_oid_order;

	      /* open the scan for this access specification node */
	      if (level == 0 && spec_level == 1)
		{
#if defined(DIAG_DEVEL) && defined(SERVER_MODE)
		  SET_DIAG_VALUE_FULL_SCAN (diag_executediag, 1,
					    DIAG_VAL_SETTYPE_INC,
					    NULL, xasl, specp);
#if 0				/* ACTIVITY PROFILE */
		  ADD_ACTIVITY_DATA (diag_executediag,
				     DIAG_EVENTCLASS_TYPE_SERVER_QUERY_FULL_SCAN,
				     xasl->qstmt, "", 0);
#endif
#endif
		  if (qexec_open_scan (thread_p, specp, xptr->merge_val_list,
				       &xasl_state->vd, readonly_scan,
				       specp->fixed_scan, specp->grouped_scan,
				       iscan_oid_order,
				       &specp->s_id) != NO_ERROR)
		    {
		      return ER_FAILED;
		    }
		}
	      else
		{
		  if (specp->type == TARGET_CLASS
		      && specp->access == INDEX
		      && (qfile_is_sort_list_covered (xptr->after_iscan_list,
						      xptr->orderby_list) ==
			  true))
		    {
		      specp->grouped_scan = false;
		      iscan_oid_order = false;
		    }

#if defined(DIAG_DEVEL) && defined(SERVER_MODE)
		  SET_DIAG_VALUE_FULL_SCAN (diag_executediag, 1,
					    DIAG_VAL_SETTYPE_INC,
					    NULL, xasl, specp);
#if 0				/* ACTIVITY PROFILE */
		  ADD_ACTIVITY_DATA (diag_executediag,
				     DIAG_EVENTCLASS_TYPE_SERVER_QUERY_FULL_SCAN,
				     xasl->qstmt, "", 0);
#endif
#endif
		  if (qexec_open_scan (thread_p, specp, xptr->val_list,
				       &xasl_state->vd, readonly_scan,
				       specp->fixed_scan, specp->grouped_scan,
				       iscan_oid_order,
				       &specp->s_id) != NO_ERROR)
		    {
		      return ER_FAILED;
		    }
		}
	    }
	}
    }

  return level;
}

int
qexec_open_merge_scans (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			XASL_STATE * xasl_state)
{
  int rc = NO_ERROR;
  ACCESS_SPEC_TYPE *inner_spec = xasl->proc.mergelist.inner_spec_list;
  ACCESS_SPEC_TYPE *outer_spec = xasl->proc.mergelist.outer_spec_list;

#if defined (SERVER_MODE)
  if ((outer_spec->type != TARGET_CLASS
       && outer_spec->type != TARGET_CLASS_ATTR)
      || outer_spec->s.cls_node.node_id == 0)
#endif
    {
      rc = qexec_open_scan (thread_p, outer_spec,
			    xasl->proc.mergelist.outer_val_list,
			    &xasl_state->vd,
			    true,
			    outer_spec->fixed_scan,
			    outer_spec->grouped_scan,
			    true, &outer_spec->s_id);
      if (rc != NO_ERROR)
	{
	  return rc;
	}
    }

#if defined (SERVER_MODE)
  if ((inner_spec->type != TARGET_CLASS
       && inner_spec->type != TARGET_CLASS_ATTR)
      || inner_spec->s.cls_node.node_id == 0)
#endif
    {
      rc = qexec_open_scan (thread_p, inner_spec,
			    xasl->proc.mergelist.inner_val_list,
			    &xasl_state->vd,
			    true,
			    inner_spec->fixed_scan,
			    inner_spec->grouped_scan,
			    true, &inner_spec->s_id);
    }

  return rc;
}
