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
 * heap_file.h: Heap file ojbect manager (at Server)
 */

#ifndef _HEAP_FILE_H_
#define _HEAP_FILE_H_

#ident "$Id$"

#include "config.h"

#include "error_manager.h"
#include "storage_common.h"
#include "locator.h"
#include "file_manager.h"
#include "disk_manager.h"
#include "slotted_page.h"
#include "oid.h"
#include "object_representation_sr.h"
#include "thread_impl.h"
#include "system_catalog.h"

#define HFID_EQ(hfid_ptr1, hfid_ptr2) \
  ((hfid_ptr1) == (hfid_ptr2) || \
   ((hfid_ptr1)->hpgid == (hfid_ptr2)->hpgid && \
    VFID_EQ(&((hfid_ptr1)->vfid), &((hfid_ptr2)->vfid))))

/*
 * Heap scan structures
 */



typedef struct heap_bestspace HEAP_BESTSPACE;
struct heap_bestspace
{
  VPID vpid;			/* Vpid of one of the best pages */
  int freespace;		/* Estimated free space in this page */
#if 1				/* TODO: for future use - DO NOT DELETE ME */
  TRANID last_tranid;
#endif
};


typedef struct heap_scancache HEAP_SCANCACHE;
struct heap_scancache
{				/* Define a scan over the whole heap file  */
  int debug_initpattern;	/* A pattern which indicates that the
				 * structure has been initialized
				 */
  HFID hfid;			/* Heap file of scan                   */
  OID class_oid;		/* Class oid of scanned instances       */
  int unfill_space;		/* Stop inserting when page has run
				 * below this value. Remaining space
				 * is left for updates. This is used only
				 * for insertions.
				 */
  LOCK page_latch;		/* Indicates the latch/lock to be acquired
				 * on heap pages. Its value may be
				 * NULL_LOCK when it is secure to skip
				 * lock on heap pages. For example, the class
				 * of the heap has been locked with either
				 * S_LOCK, SIX_LOCK, or X_LOCK
				 */
  int cache_last_fix_page;	/* Indicates if page buffers and memory
				 * are cached (left fixed)
				 */
  PAGE_PTR pgptr;		/* Page pointer to last left fixed page */
  char *area;			/* Pointer to last left fixed memory
				 * allocated
				 */
  int area_size;		/* Size of allocated area               */
  int known_nrecs;		/* Cached number of records (known)     */
  int known_nbest;		/* Cached number of best pages (Known)  */
  int known_nother_best;	/* Cached number of other best pages    */
  VPID collect_nxvpid;		/* Next page where statistics are used  */
  int collect_npages;		/* Total number of pages that were
				 * scanned
				 */
  int collect_nrecs;		/* Total num of rec on scanned pages    */
  float collect_recs_sumlen;	/* Total len of recs on scanned pages   */
  int collect_nbest;		/* Total of best pages that were found
				 * by scanning the heap. This is a hint
				 */
  int collect_maxbest;		/* Maximum num of elements in collect_best
				 * array
				 */
  HEAP_BESTSPACE *collect_best;	/* Best pages that were found during the
				 * scan
				 */
  int num_btids;		/* Total number of indexes defined
				 * on the scanning class
				 */
  BTREE_UNIQUE_STATS *index_stat_info;	/* unique-related stat info
					 * <btid,num_nulls,num_keys,num_oids>
					 */
  int scanid_bit;
  FILE_TYPE file_type;		/* The file type of the heap file being
				 * scanned. Can be FILE_HEAP or
				 * FILE_HEAP_REUSE_SLOTS
				 */
};

typedef struct heap_scanrange HEAP_SCANRANGE;
struct heap_scanrange
{				/* Define a scanrange over a set of objects resident on the
				 * same page. It can be used for evaluation of nested joins
				 */
  OID first_oid;		/* First OID in scan range object                */
  OID last_oid;			/* Last OID in scan range object                 */
  HEAP_SCANCACHE scan_cache;	/* Current cached information from previous scan */
};

typedef enum
{
  HEAP_READ_ATTRVALUE,
  HEAP_WRITTEN_ATTRVALUE,
  HEAP_UNINIT_ATTRVALUE
} HEAP_ATTRVALUE_STATE;

typedef enum
{
  HEAP_INSTANCE_ATTR,
  HEAP_SHARED_ATTR,
  HEAP_CLASS_ATTR
} HEAP_ATTR_TYPE;

typedef struct heap_attrvalue HEAP_ATTRVALUE;
struct heap_attrvalue
{
  ATTR_ID attrid;		/* attribute identifier                       */
  HEAP_ATTRVALUE_STATE state;	/* State of the attribute value. Either of
				 * has been read, has been updated, or is
				 * unitialized
				 */
  int do_increment;
  HEAP_ATTR_TYPE attr_type;	/* Instance, class, or shared attribute */
  OR_ATTRIBUTE *last_attrepr;	/* Used for default values                    */
  OR_ATTRIBUTE *read_attrepr;	/* Pointer to a desired attribute information */
  DB_VALUE dbvalue;		/* DB values of the attribute in memory       */
};

typedef struct heap_cache_attrinfo HEAP_CACHE_ATTRINFO;
struct heap_cache_attrinfo
{
  OID class_oid;		/* Class object identifier               */
  int last_cacheindex;		/* An index identifier when the
				 * last_classrepr was obtained from the
				 * classrepr cache. Otherwise, -1
				 */
  int read_cacheindex;		/* An index identifier when the
				 * read_classrepr was obtained from the
				 * classrepr cache. Otherwise, -1
				 */
  OR_CLASSREP *last_classrepr;	/* Currently cached catalog attribute
				 * info.
				 */
  OR_CLASSREP *read_classrepr;	/* Currently cached catalog attribute
				 * info.
				 */
  OID inst_oid;			/* Instance Object identifier            */
  int inst_chn;			/* Current chn of instance object        */
  int num_values;		/* Number of desired attribute values    */
  HEAP_ATTRVALUE *values;	/* Value for the attributes              */
};

#if 0				/* TODO: check not use - ksseo */
typedef struct heap_spacecache HEAP_SPACECACHE;
struct heap_spacecache
{				/* Define an alter space cache for heap file  */

  float remain_sumlen;		/* Total new length of records that it
				 * is predicted for the rest of space
				 * cache. If it is unknown -1 is stored.
				 * This value is used to estimate the
				 * number of pages to allocate at a
				 * particular time in space cache.
				 * If the value is < pagesize, only one
				 * page at a time is allocated.
				 */
};
#endif

enum
{ END_SCAN, CONTINUE_SCAN };

typedef struct heap_idx_elements_info HEAP_IDX_ELEMENTS_INFO;
struct heap_idx_elements_info
{
  int num_btids;		/* class has # of btids          */
  int has_single_col;		/* class has single column index */
  int has_multi_col;		/* class has multi-column index  */
};

extern int heap_classrepr_decache (THREAD_ENTRY * thread_p,
				   const OID * class_oid);
#ifdef DEBUG_CLASSREPR_CACHE
extern int heap_classrepr_dump_anyfixed (void);
#endif /* DEBUG_CLASSREPR_CACHE */
extern int heap_manager_initialize (void);
extern int heap_manager_finalize (void);
extern int heap_assign_address (THREAD_ENTRY * thread_p, const HFID * hfid,
				OID * oid, int expected_length);
extern int heap_assign_address_with_class_oid (THREAD_ENTRY * thread_p,
					       const HFID * hfid, OID * oid,
					       int expected_length,
					       OID * class_oid);
extern OID *heap_insert (THREAD_ENTRY * thread_p, const HFID * hfid,
			 OID * oid, RECDES * recdes,
			 HEAP_SCANCACHE * scan_cache);
extern const OID *heap_update (THREAD_ENTRY * thread_p, const HFID * hfid,
			       const OID * oid, RECDES * recdes, bool * old,
			       HEAP_SCANCACHE * scan_cache);
extern const OID *heap_delete (THREAD_ENTRY * thread_p, const HFID * hfid,
			       const OID * oid, HEAP_SCANCACHE * scan_cache);
extern void heap_flush (THREAD_ENTRY * thread_p, const OID * oid);
extern int xheap_reclaim_addresses (THREAD_ENTRY * thread_p,
				    const HFID * hfid);
extern int heap_scancache_start (THREAD_ENTRY * thread_p,
				 HEAP_SCANCACHE * scan_cache,
				 const HFID * hfid, const OID * class_oid,
				 int cache_last_fix_page, int is_indexscan,
				 int lock_hint);
extern int heap_scancache_start_modify (THREAD_ENTRY * thread_p,
					HEAP_SCANCACHE * scan_cache,
					const HFID * hfid,
					const OID * class_oid, int op_type);
extern int heap_scancache_quick_start (HEAP_SCANCACHE * scan_cache);
extern int heap_scancache_end (THREAD_ENTRY * thread_p,
			       HEAP_SCANCACHE * scan_cache);
extern int heap_scancache_end_when_scan_will_resume (THREAD_ENTRY * thread_p,
						     HEAP_SCANCACHE *
						     scan_cache);
extern void heap_scancache_end_modify (THREAD_ENTRY * thread_p,
				       HEAP_SCANCACHE * scan_cache);
extern int heap_hint_expected_num_objects (THREAD_ENTRY * thread_p,
					   HEAP_SCANCACHE * scan_cache,
					   int nobjs, int avg_objsize);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int heap_get_chn (THREAD_ENTRY * thread_p, const OID * oid);
#endif
extern SCAN_CODE heap_get (THREAD_ENTRY * thread_p, const OID * oid,
			   RECDES * recdes, HEAP_SCANCACHE * scan_cache,
			   int ispeeking, int chn);
extern SCAN_CODE heap_get_with_class_oid (THREAD_ENTRY * thread_p,
					  const OID * oid, RECDES * recdes,
					  HEAP_SCANCACHE * scan_cache,
					  OID * class_oid, int ispeeking);
extern SCAN_CODE heap_next (THREAD_ENTRY * thread_p, const HFID * hfid,
			    OID * class_oid, OID * next_oid, RECDES * recdes,
			    HEAP_SCANCACHE * scan_cache, int ispeeking);
extern SCAN_CODE heap_prev (THREAD_ENTRY * thread_p, const HFID * hfid,
			    OID * class_oid, OID * prev_oid, RECDES * recdes,
			    HEAP_SCANCACHE * scan_cache, int ispeeking);
extern SCAN_CODE heap_first (THREAD_ENTRY * thread_p, const HFID * hfid,
			     OID * class_oid, OID * oid, RECDES * recdes,
			     HEAP_SCANCACHE * scan_cache, int ispeeking);
extern SCAN_CODE heap_last (THREAD_ENTRY * thread_p, const HFID * hfid,
			    OID * class_oid, OID * oid, RECDES * recdes,
			    HEAP_SCANCACHE * scan_cache, int ispeeking);
extern int heap_get_alloc (THREAD_ENTRY * thread_p, const OID * oid,
			   RECDES * recdes);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int heap_cmp (THREAD_ENTRY * thread_p, const OID * oid,
		     RECDES * recdes);
#endif
extern int heap_scanrange_start (THREAD_ENTRY * thread_p,
				 HEAP_SCANRANGE * scan_range,
				 const HFID * hfid, const OID * class_oid,
				 int lock_hint);
extern void heap_scanrange_end (THREAD_ENTRY * thread_p,
				HEAP_SCANRANGE * scan_range);
extern SCAN_CODE heap_scanrange_to_following (THREAD_ENTRY * thread_p,
					      HEAP_SCANRANGE * scan_range,
					      OID * start_oid);
extern SCAN_CODE heap_scanrange_to_prior (THREAD_ENTRY * thread_p,
					  HEAP_SCANRANGE * scan_range,
					  OID * last_oid);
extern SCAN_CODE heap_scanrange_next (THREAD_ENTRY * thread_p, OID * next_oid,
				      RECDES * recdes,
				      HEAP_SCANRANGE * scan_range,
				      int ispeeking);
extern SCAN_CODE heap_scanrange_prev (THREAD_ENTRY * thread_p, OID * prev_oid,
				      RECDES * recdes,
				      HEAP_SCANRANGE * scan_range,
				      int ispeeking);
extern SCAN_CODE heap_scanrange_first (THREAD_ENTRY * thread_p,
				       OID * first_oid, RECDES * recdes,
				       HEAP_SCANRANGE * scan_range,
				       int ispeeking);
extern SCAN_CODE heap_scanrange_last (THREAD_ENTRY * thread_p, OID * last_oid,
				      RECDES * recdes,
				      HEAP_SCANRANGE * scan_range,
				      int ispeeking);

extern bool heap_does_exist (THREAD_ENTRY * thread_p, const OID * oid,
			     OID * class_oid);
extern int heap_get_num_objects (THREAD_ENTRY * thread_p, const HFID * hfid,
				 int *npages, int *nobjs, int *avg_length);

#if defined (SERVER_MODE)
extern int heap_get_num_objects_proxy (THREAD_ENTRY * thread_p, int node_id,
				       const HFID * hfid, int *npages,
				       int *nobjs, int *avg_length);
#else
#define heap_get_num_objects_proxy(thread_p, node_id, hfid, npages, nobjs, avg_length)\
        heap_get_num_objects      (thread_p, hfid, npages, nobjs, avg_length)
#endif

extern int heap_estimate (THREAD_ENTRY * thread_p, const HFID * hfid,
			  int *npages, int *nobjs, int *avg_length);
extern int heap_estimate_num_objects (THREAD_ENTRY * thread_p,
				      const HFID * hfid);
extern INT32 heap_estimate_num_pages_needed (THREAD_ENTRY * thread_p,
					     int total_nobjs,
					     int avg_obj_size, int num_attrs,
					     int num_var_attrs);

extern OID *heap_get_class_oid (THREAD_ENTRY * thread_p, const OID * oid,
				OID * class_oid);
extern char *heap_get_class_name (THREAD_ENTRY * thread_p,
				  const OID * class_oid);
extern char *heap_get_class_name_alloc_if_diff (THREAD_ENTRY * thread_p,
						const OID * class_oid,
						char *guess_classname);
extern char *heap_get_class_name_of_instance (THREAD_ENTRY * thread_p,
					      const OID * inst_oid);
#if defined (ENABLE_UNUSED_FUNCTION)
extern char *heap_get_class_name_with_is_class (THREAD_ENTRY * thread_p,
						const OID * oid,
						int *isclass);
#endif
extern int heap_attrinfo_start (THREAD_ENTRY * thread_p,
				const OID * class_oid,
				int requested_num_attrs,
				const ATTR_ID * attrid,
				HEAP_CACHE_ATTRINFO * attr_info);
extern void heap_attrinfo_end (THREAD_ENTRY * thread_p,
			       HEAP_CACHE_ATTRINFO * attr_info);
extern int heap_attrinfo_clear_dbvalues (HEAP_CACHE_ATTRINFO * attr_info);
extern int heap_attrinfo_read_dbvalues (THREAD_ENTRY * thread_p,
					const OID * inst_oid, RECDES * recdes,
					HEAP_CACHE_ATTRINFO * attr_info);
extern DB_VALUE *heap_attrinfo_access (ATTR_ID attrid,
				       HEAP_CACHE_ATTRINFO * attr_info);
extern int heap_attrinfo_set (const OID * inst_oid, ATTR_ID attrid,
			      DB_VALUE * attr_val,
			      HEAP_CACHE_ATTRINFO * attr_info);
extern SCAN_CODE heap_attrinfo_transform_to_disk (THREAD_ENTRY * thread_p,
						  HEAP_CACHE_ATTRINFO *
						  attr_info,
						  RECDES * old_recdes,
						  RECDES * new_recdes);
extern DB_VALUE *heap_attrinfo_generate_key (THREAD_ENTRY * thread_p,
					     int n_atts, int *att_ids,
					     int *atts_prefix_length,
					     HEAP_CACHE_ATTRINFO * attr_info,
					     RECDES * recdes,
					     DB_VALUE * dbvalue, char *buf);
extern int heap_attrinfo_start_with_index (THREAD_ENTRY * thread_p,
					   OID * class_oid,
					   RECDES * class_recdes,
					   HEAP_CACHE_ATTRINFO * attr_info,
					   HEAP_IDX_ELEMENTS_INFO * idx_info);
extern int heap_attrinfo_start_with_btid (THREAD_ENTRY * thread_p,
					  OID * class_oid, BTID * btid,
					  HEAP_CACHE_ATTRINFO * attr_info);

#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_VALUE *heap_attrvalue_get_index (int value_index,
					   ATTR_ID * attrid, int *n_btids,
					   BTID ** btids,
					   HEAP_CACHE_ATTRINFO *
					   idx_attrinfo);
#endif
extern HEAP_ATTRVALUE *heap_attrvalue_locate (ATTR_ID attrid,
					      HEAP_CACHE_ATTRINFO *
					      attr_info);
extern OR_ATTRIBUTE *heap_locate_last_attrepr (ATTR_ID attrid,
					       HEAP_CACHE_ATTRINFO *
					       attr_info);
extern DB_VALUE *heap_attrvalue_get_key (THREAD_ENTRY * thread_p,
					 int btid_index,
					 HEAP_CACHE_ATTRINFO * idx_attrinfo,
					 RECDES * recdes, BTID * btid,
					 DB_VALUE * db_value, char *buf);

extern BTID *heap_indexinfo_get_btid (int btid_index,
				      HEAP_CACHE_ATTRINFO * attrinfo);
extern int heap_indexinfo_get_num_attrs (int btid_index,
					 HEAP_CACHE_ATTRINFO * attrinfo);
extern int heap_indexinfo_get_attrids (int btid_index,
				       HEAP_CACHE_ATTRINFO * attrinfo,
				       ATTR_ID * attrids);
extern int heap_indexinfo_get_attrs_prefix_length (int btid_index,
						   HEAP_CACHE_ATTRINFO *
						   attrinfo,
						   int *attrs_prefix_length,
						   int
						   len_attrs_prefix_length);
extern int heap_get_indexinfo_of_btid (THREAD_ENTRY * thread_p,
				       OID * class_oid, BTID * btid,
				       BTREE_TYPE * type, int *num_attrs,
				       ATTR_ID ** attr_ids,
				       int **attrs_prefix_length,
				       char **btnamepp);
extern int heap_get_referenced_by (THREAD_ENTRY * thread_p,
				   const OID * obj_oid, RECDES * obj,
				   int *max_oid_cnt, OID ** oid_list);

extern int heap_prefetch (THREAD_ENTRY * thread_p, const OID * oid,
			  OID * class_oid, LC_COPYAREA_DESC * prefetch);
extern DISK_ISVALID heap_check_all_pages (THREAD_ENTRY * thread_p,
					  HFID * hfid);
extern DISK_ISVALID heap_check_all_heaps (THREAD_ENTRY * thread_p);

extern int heap_chnguess_get (THREAD_ENTRY * thread_p, const OID * oid,
			      int tran_index);
extern int heap_chnguess_put (THREAD_ENTRY * thread_p, const OID * oid,
			      int tran_index, int chn);
extern void heap_chnguess_clear (THREAD_ENTRY * thread_p, int tran_index);

/* Misc */
extern int xheap_get_class_num_objects_pages (THREAD_ENTRY * thread_p,
					      const HFID * hfid,
					      int approximation, int *nobjs,
					      int *npages);

extern int xheap_has_instance (THREAD_ENTRY * thread_p, const HFID * hfid,
			       OID * class_oid);


/* auto-increment */
extern int heap_set_autoincrement_value (THREAD_ENTRY * thread_p,
					 HEAP_CACHE_ATTRINFO * attr_info,
					 HEAP_SCANCACHE * scan_cache);

extern void heap_dump (THREAD_ENTRY * thread_p, FILE * fp, HFID * hfid,
		       bool rec_p);
extern void heap_dump_all (THREAD_ENTRY * thread_p, FILE * fp, bool rec_p);
extern void heap_attrinfo_dump (THREAD_ENTRY * thread_p, FILE * fp,
				HEAP_CACHE_ATTRINFO * attr_info,
				bool dump_schema);
#if defined (CUBRID_DEBUG)
extern void heap_chnguess_dump (FILE * fp);
#endif /* CUBRID_DEBUG */
extern void heap_dump_all_capacities (THREAD_ENTRY * thread_p, FILE * fp);

/* partition-support */
extern REPR_ID heap_get_class_repr_id (THREAD_ENTRY * thread_p,
				       OID * class_oid);
extern int heap_attrinfo_set_uninitialized_global (THREAD_ENTRY * thread_p,
						   OID * inst_oid,
						   RECDES * recdes,
						   HEAP_CACHE_ATTRINFO *
						   attr_info);

/* Recovery functions */
extern int heap_rv_redo_newpage (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_redo_newpage_reuse_oid (THREAD_ENTRY * thread_p,
					   LOG_RCV * rcv);
extern int heap_rv_undoredo_pagehdr (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern void heap_rv_dump_statistics (FILE * fp, int ignore_length,
				     void *data);
extern void heap_rv_dump_chain (FILE * fp, int ignore_length, void *data);
extern int heap_rv_undo_insert (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_redo_insert (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_undo_delete (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_redo_delete (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_redo_delete_newhome (THREAD_ENTRY * thread_p,
					LOG_RCV * rcv);
extern int heap_rv_redo_mark_reusable_slot (THREAD_ENTRY * thread_p,
					    LOG_RCV * rcv);
extern int heap_rv_undoredo_update (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_undoredo_update_type (THREAD_ENTRY * thread_p,
					 LOG_RCV * rcv);
extern int heap_rv_redo_reuse_page (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_redo_reuse_page_reuse_oid (THREAD_ENTRY * thread_p,
					      LOG_RCV * rcv);
extern void heap_rv_dump_reuse_page (FILE * fp, int ignore_length,
				     void *data);

extern int heap_get_hfid_from_class_oid (THREAD_ENTRY * thread_p,
					 OID * class_oid, HFID * hfid);
extern int heap_compact_pages (THREAD_ENTRY * thread_p, OID * class_oid);

extern void heap_classrepr_dump_all (THREAD_ENTRY * thread_p, FILE * fp,
				     OID * class_oid);

#endif /* _HEAP_FILE_H_ */
