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
 * Scan (Server Side)
 */

#include "btree.h"		/* TODO: for BTREE_SCAN */

#ifndef _SCAN_MANAGER_H_
#define _SCAN_MANAGER_H_

#ident "$Id$"

#include "oid.h"		/* for OID */
#include "storage_common.h"	/* for PAGEID */
#include "heap_file.h"		/* for HEAP_SCANCACHE */
#include "method_scan.h"	/* for METHOD_SCAN_BUFFER */

/*
 *       	TYPEDEFS RELATED TO THE SCAN DATA STRUCTURES
 */

typedef enum
{
  S_HEAP_SCAN = 1,
  S_CLASS_ATTR_SCAN,
  S_INDX_SCAN,
  S_LIST_SCAN,
  S_SET_SCAN,
  S_METHOD_SCAN
} SCAN_TYPE;

typedef struct heap_scan_id HEAP_SCAN_ID;
struct heap_scan_id
{
  OID curr_oid;			/* current object identifier */
  OID cls_oid;			/* class object identifier */
  HFID hfid;			/* heap file identifier */
  HEAP_SCANCACHE scan_cache;	/* heap file scan_cache */
  HEAP_SCANRANGE scan_range;	/* heap file scan range */
  SCAN_PRED scan_pred;		/* scan predicates(filters) */
  SCAN_ATTRS pred_attrs;	/* attr info from predicates */
  REGU_VARIABLE_LIST rest_regu_list;	/* regulator variable list */
  SCAN_ATTRS rest_attrs;	/* attr info from other than preds */
  bool caches_inited;		/* are the caches initialized?? */
  bool scancache_inited;
  bool scanrange_inited;
  int lock_hint;		/* lock hint */
};				/* Regular Heap File Scan Identifier */

typedef struct key_val_range KEY_VAL_RANGE;
struct key_val_range
{
  RANGE range;
  DB_VALUE key1;
  DB_VALUE key2;
};

typedef struct indx_scan_id INDX_SCAN_ID;
struct indx_scan_id
{
  INDX_INFO *indx_info;		/* index information */
  BTREE_TYPE bt_type;		/* index type */
  int bt_num_attrs;		/* num of attributes of the index key */
  ATTR_ID *bt_attr_ids;		/* attr id array of the index key */
  int *bt_attrs_prefix_length;	/* attr prefix length */
  ATTR_ID *vstr_ids;		/* attr id array of variable string */
  int num_vstr;			/* num of variable string attrs */
  BTREE_SCAN bt_scan;		/* index scan info. structure */
  int one_range;		/* a single range? */
  int curr_keyno;		/* current key number */
  int curr_oidno;		/* current oid number */
  OID *curr_oidp;		/* current oid pointer */
  char *copy_buf;		/* index key copy_buf pointer info */
  OID_LIST oid_list;		/* list of object identifiers */
  OID cls_oid;			/* class object identifier */
  int copy_buf_len;		/* index key copy_buf length info */
  HFID hfid;			/* heap file identifier */
  HEAP_SCANCACHE scan_cache;	/* heap file scan_cache */
  SCAN_PRED key_pred;		/* key predicates(filters) */
  SCAN_ATTRS key_attrs;		/* attr info from key filter */
  SCAN_PRED scan_pred;		/* scan predicates(filters) */
  SCAN_ATTRS pred_attrs;	/* attr info from predicates */
  REGU_VARIABLE_LIST rest_regu_list;	/* regulator variable list */
  SCAN_ATTRS rest_attrs;	/* attr info from other than preds */
  KEY_VAL_RANGE *key_vals;	/* for eliminating duplicate ranges */
  int key_cnt;			/* number of valid ranges */
  int lock_hint;		/* lock hint */
  bool iscan_oid_order;		/* index_scan_oid_order flag */
  bool need_count_only;		/* get count only, no OIDs are copied */
  bool caches_inited;		/* are the caches initialized?? */
  bool scancache_inited;
};

typedef struct llist_scan_id LLIST_SCAN_ID;
struct llist_scan_id
{
  QFILE_LIST_ID *list_id;	/* Points to XASL tree */
  QFILE_LIST_SCAN_ID lsid;	/* List File Scan Identifier */
  SCAN_PRED scan_pred;		/* scan predicates(filters) */
  REGU_VARIABLE_LIST rest_regu_list;	/* regulator variable list */
  QFILE_TUPLE_RECORD *tplrecp;	/* tuple record pointer; output param */
};

typedef struct set_scan_id SET_SCAN_ID;
struct set_scan_id
{
  REGU_VARIABLE *set_ptr;	/* Points to XASL tree */
  REGU_VARIABLE_LIST operand;	/* operand points current element */
  DB_VALUE set;			/* set we will scan */
  int set_card;			/* cardinality of the set */
  int cur_index;		/* current element index */
  SCAN_PRED scan_pred;		/* scan predicates(filters) */
};

typedef struct va_scan_id VA_SCAN_ID;
struct va_scan_id
{
  METHOD_SCAN_BUFFER scan_buf;	/* value array buffer */
};

/* Note: Scan position is currently supported only for list file scans. */
typedef struct scan_pos SCAN_POS;
struct scan_pos
{
  SCAN_STATUS status;		/* Scan status                    */
  SCAN_POSITION position;	/* Scan position                  */
  QFILE_TUPLE_POSITION ls_tplpos;	/* List file index scan position  */
};				/* Scan position structure */


/*  adds for returning multiple value lists
    when scanning for more than one result, the values are packed 
    directly into cache. On the remote server that is doing
    the scan, cache.curr points to the  packed values, 
    On the result receiver side, cache.curr is the next read position,
    and cache.size is still the buffer size and the buffer size is equal to 
    the total length of packed values.
 */
typedef struct scan_result_cache_struct SCAN_RESULT_CACHE;
struct scan_result_cache_struct
{
  char *buffer;
  char *current;
  int size;
};

#define SCAN_CACHE_DATA_SIZE(c) ((c).current - PTR_ALIGN((c).buffer,MAX_ALIGNMENT))
#define SCAN_CACHE_FREE_SIZE(c) (SCAN_CACHE_END(c)-(c).current)
#define SCAN_CACHE_END(c)       ((c).buffer + (c).size)
#define SCAN_CACHE_IS_EMPTY(c)  ((c).buffer == NULL )
#define SCAN_CACHE_RESET(c)     ((c).buffer=NULL,(c).current=NULL,(c).size=0)

typedef struct scan_id_struct SCAN_ID;
struct scan_id_struct
{
  SCAN_TYPE type;		/* Scan Type */
  SCAN_STATUS status;		/* Scan Status */
  SCAN_POSITION position;	/* Scan Position */
  SCAN_DIRECTION direction;	/* Forward/Backward Direction */
  int readonly_scan;

  int fixed;			/* if true, pages containing scan
				   items in a group keep fixed */
  int grouped;			/* if true, the scan items are
				   accessed group by group,
				   instead of a whole single
				   scan from beginning to end. */
  int qualified_block;		/* scan block has qualified items,
				   initially set to true */
  QPROC_SINGLE_FETCH single_fetch;	/* scan fetch mode */
  int single_fetched;		/* if true, first qualified scan
				   item already fetched. */
  int null_fetched;		/* if true, null-padding scan item
				   already fetched. used in outer join */
  QPROC_QUALIFICATION qualification;	/* see QPROC_QUALIFICATION;
					   used for both input and output
					   parameter */
  DB_VALUE *join_dbval;		/* A dbval from another table for
				   simple JOIN terms.
				   if set, and unbound, no rows can match.
				   row or end of scan will be returned
				   with no actual SCAN. */
  VAL_LIST *val_list;		/* value list */
  VAL_DESCR *vd;		/* value descriptor */
  SCAN_RESULT_CACHE cache;	/* for scan cache */
  QFILE_LIST_SCAN_ID *scan_id;	/* for list scan */

  union
  {
    LLIST_SCAN_ID llsid;	/* List File Scan Identifier */
    HEAP_SCAN_ID hsid;		/* Regular Heap File Scan Identifier */
    INDX_SCAN_ID isid;		/* Indexed Heap File Scan Identifier */
    SET_SCAN_ID ssid;		/* Set Scan Identifier */
    VA_SCAN_ID vaid;		/* Value Array Identifier */
  } s;
};				/* Scan Identifier */

extern int scan_open_heap_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
				/* fields of SCAN_ID */
				int readonly_scan,
				int fixed,
				int lock_hint,
				int grouped,
				QPROC_SINGLE_FETCH single_fetch,
				DB_VALUE * join_dbval,
				VAL_LIST * val_list, VAL_DESCR * vd,
				/* fields of HEAP_SCAN_ID */
				OID * cls_oid,
				HFID * hfid,
				REGU_VARIABLE_LIST regu_list_pred,
				PRED_EXPR * pr,
				REGU_VARIABLE_LIST regu_list_rest,
				int num_attrs_pred,
				ATTR_ID * attrids_pred,
				HEAP_CACHE_ATTRINFO * cache_pred,
				int num_attrs_rest,
				ATTR_ID * attrids_rest,
				HEAP_CACHE_ATTRINFO * cache_rest);
extern int scan_open_class_attr_scan (THREAD_ENTRY * thread_p,
				      SCAN_ID * scan_id,
				      /* fields of SCAN_ID */
				      int grouped,
				      QPROC_SINGLE_FETCH single_fetch,
				      DB_VALUE * join_dbval,
				      VAL_LIST * val_list, VAL_DESCR * vd,
				      /* fields of HEAP_SCAN_ID */
				      OID * cls_oid,
				      HFID * hfid,
				      REGU_VARIABLE_LIST regu_list_pred,
				      PRED_EXPR * pr,
				      REGU_VARIABLE_LIST regu_list_rest,
				      int num_attrs_pred,
				      ATTR_ID * attrids_pred,
				      HEAP_CACHE_ATTRINFO * cache_pred,
				      int num_attrs_rest,
				      ATTR_ID * attrids_rest,
				      HEAP_CACHE_ATTRINFO * cache_rest);
extern int scan_open_index_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
				 /* fields of SCAN_ID */
				 int readonly_scan,
				 int fixed,
				 int lock_hint,
				 int grouped,
				 QPROC_SINGLE_FETCH single_fetch,
				 DB_VALUE * join_dbval,
				 VAL_LIST * val_list, VAL_DESCR * vd,
				 /* fields of INDX_SCAN_ID */
				 INDX_INFO * indx_info,
				 OID * cls_oid,
				 HFID * hfid,
				 REGU_VARIABLE_LIST regu_list_key,
				 PRED_EXPR * pr_key,
				 REGU_VARIABLE_LIST regu_list_pred,
				 PRED_EXPR * pr,
				 REGU_VARIABLE_LIST regu_list_rest,
				 int num_attrs_key,
				 ATTR_ID * attrids_key,
				 HEAP_CACHE_ATTRINFO * cache_key,
				 int num_attrs_pred,
				 ATTR_ID * attrids_pred,
				 HEAP_CACHE_ATTRINFO * cache_pred,
				 int num_attrs_rest,
				 ATTR_ID * attrids_rest,
				 HEAP_CACHE_ATTRINFO * cache_rest,
				 bool iscan_oid_order);
extern int scan_open_list_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
				/* fields of SCAN_ID */
				int grouped,
				QPROC_SINGLE_FETCH single_fetch,
				DB_VALUE * join_dbval,
				VAL_LIST * val_list, VAL_DESCR * vd,
				/* fields of LLIST_SCAN_ID */
				QFILE_LIST_ID * list_id,
				REGU_VARIABLE_LIST regu_list_pred,
				PRED_EXPR * pr,
				REGU_VARIABLE_LIST regu_list_rest);
extern int scan_open_set_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
			       /* fields of SCAN_ID */
			       int grouped,
			       QPROC_SINGLE_FETCH single_fetch,
			       DB_VALUE * join_dbval,
			       VAL_LIST * val_list, VAL_DESCR * vd,
			       /* fields of SET_SCAN_ID */
			       REGU_VARIABLE * set_ptr,
			       REGU_VARIABLE_LIST regu_list_pred,
			       PRED_EXPR * pr);
extern int scan_open_method_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
				  /* fields of SCAN_ID */
				  int grouped,
				  QPROC_SINGLE_FETCH single_fetch,
				  DB_VALUE * join_dbval,
				  VAL_LIST * val_list, VAL_DESCR * vd,
				  /* */
				  QFILE_LIST_ID * list_id,
				  METHOD_SIG_LIST * meth_sig_list);
extern int scan_start_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id);
extern SCAN_CODE scan_reset_scan_block (THREAD_ENTRY * thread_p,
					SCAN_ID * s_id);
extern SCAN_CODE scan_next_scan_block (THREAD_ENTRY * thread_p,
				       SCAN_ID * s_id);
extern void scan_end_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id);
extern void scan_close_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id);
extern SCAN_CODE scan_next_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id);
extern SCAN_CODE scan_prev_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id);
extern void scan_save_scan_pos (SCAN_ID * s_id, SCAN_POS * scan_pos);
extern SCAN_CODE scan_jump_scan_pos (THREAD_ENTRY * thread_p, SCAN_ID * s_id,
				     SCAN_POS * scan_pos);
extern void scan_initialize (void);
extern void scan_finalize (void);

#endif /* _SCAN_MANAGER_H_ */
