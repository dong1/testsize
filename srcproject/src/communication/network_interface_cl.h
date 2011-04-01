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
 * network_interface_cl.h - Definitions for client network support
 */

#ifndef _NETWORK_INTERFACE_CL_H_
#define _NETWORK_INTERFACE_CL_H_

#ident "$Id$"

#include <stdio.h>

#include "dbdef.h"
#include "replication.h"
#include "server_interface.h"
#include "perf_monitor.h"
#include "storage_common.h"
#include "object_domain.h"
#include "query_list.h"
#include "statistics.h"
#include "connection_defs.h"
#include "log_writer.h"

typedef struct server_info SERVER_INFO;
struct server_info
{
  int info_bits;
  DB_VALUE *value[SI_CNT];
};

/* killtran supporting structures and functions */
typedef struct one_tran_info ONE_TRAN_INFO;
struct one_tran_info
{
  int tran_index;
  int state;
  int process_id;
  char *db_user;
  char *program_name;
  char *login_name;
  char *host_name;
};

typedef struct trans_info TRANS_INFO;
struct trans_info
{
  int num_trans;
  ONE_TRAN_INFO tran[1];	/* really [num_trans] */
};

#define TRAN_STATE_CHAR(STATE)					\
	((STATE == TRAN_ACTIVE) ? '+' 				\
	: (info->tran[i].state == TRAN_UNACTIVE_ABORTED) ? '-'	\
	: ('A' + info->tran[i].state))

extern int locator_fetch (OID * oidp, int chn, LOCK lock, OID * class_oid,
			  int class_chn, int prefetch,
			  LC_COPYAREA ** fetch_copyarea, int node);
extern int locator_get_class (OID * class_oid, int class_chn, const OID * oid,
			      LOCK lock, int prefetching,
			      LC_COPYAREA ** fetch_copyarea);
extern int locator_fetch_all (const HFID * hfid, LOCK * lock,
			      OID * class_oidp, int *nobjects, int *nfetched,
			      OID * last_oidp, LC_COPYAREA ** fetch_copyarea);
extern int locator_does_exist (OID * oidp, int chn, LOCK lock,
			       OID * class_oid, int class_chn,
			       int need_fetching, int prefetch,
			       LC_COPYAREA ** fetch_copyarea, int node_id);
extern int locator_notify_isolation_incons (LC_COPYAREA ** synch_copyarea);
extern int locator_force (LC_COPYAREA * copy_area, int node_id);
extern int locator_fetch_lockset (LC_LOCKSET * lockset,
				  LC_COPYAREA ** fetch_copyarea);
extern int locator_fetch_all_reference_lockset (OID * oid, int chn,
						OID * class_oid,
						int class_chn, LOCK lock,
						int quit_on_errors,
						int prune_level,
						LC_LOCKSET ** lockset,
						LC_COPYAREA **
						fetch_copyarea);
extern LC_FIND_CLASSNAME locator_find_class_oid (const char *class_name,
						 OID * class_oid, LOCK lock,
						 int node);
extern LC_FIND_CLASSNAME locator_reserve_class_names (const int num_classes,
						      const char
						      **class_names,
						      OID * class_oids,
						      int node);
extern LC_FIND_CLASSNAME locator_delete_class_name (const char *class_name,
						    int node);
extern LC_FIND_CLASSNAME locator_rename_class_name (const char *old_name,
						    const char *new_name,
						    OID * class_oid,
						    int node);
extern int locator_assign_oid (const HFID * hfid, OID * perm_oid,
			       int expected_length, OID * class_oid,
			       const char *class_name, int node);
extern int locator_assign_oid_batch (LC_OIDSET * oidset);
extern LC_FIND_CLASSNAME
locator_find_lockhint_class_oids (int num_classes,
				  const char **many_classnames,
				  LOCK * many_locks,
				  int *many_need_subclasses,
				  OID * guessed_class_oids,
				  int *guessed_class_chns,
				  int quit_on_errors,
				  LC_LOCKHINT ** lockhint,
				  LC_COPYAREA ** fetch_copyarea, int node_id);
extern int locator_fetch_lockhint_classes (LC_LOCKHINT * lockhint,
					   LC_COPYAREA ** fetch_area,
					   int node_id);
extern int locator_build_fk_obj_cache (OID * cls_oid, HFID * hfid,
				       TP_DOMAIN * key_type, int n_attrs,
				       int *attr_ids, OID * pk_cls_oid,
				       BTID * pk_btid, int cache_attr_id,
				       char *fk_name);
extern int heap_create (HFID * hfid, const OID * class_oid, bool reuse_oid,
			int node);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int heap_destroy (const HFID * hfid);
#endif
extern int heap_destroy_newly_created (const HFID * hfid, int node);
extern int heap_reclaim_addresses (const HFID * hfid);
extern DKNPAGES disk_get_total_numpages (VOLID volid);
extern DKNPAGES disk_get_free_numpages (VOLID volid);
extern char *disk_get_remarks (VOLID volid);
#if defined (ENABLE_UNUSED_FUNCTION)
extern DISK_VOLPURPOSE disk_get_purpose (VOLID volid);
#endif
extern VOLID
disk_get_purpose_and_total_free_numpages (VOLID volid,
					  DISK_VOLPURPOSE * vol_purpose,
					  DKNPAGES * vol_ntotal_pages,
					  DKNPAGES * vol_nfree_pages);
extern char *disk_get_fullname (VOLID volid, char *vol_fullname);
extern LOG_COPY *log_client_get_first_postpone (LOG_LSA * next_lsa);
extern LOG_COPY *log_client_get_next_postpone (LOG_LSA * next_lsa);
extern LOG_COPY *log_client_get_first_undo (LOG_LSA * next_lsa);
extern LOG_COPY *log_client_get_next_undo (LOG_LSA * next_lsa);
extern LOG_COPY *log_client_unknown_state_abort_get_first_undo (LOG_LSA *
								next_lsa);
extern void log_append_client_undo (LOG_RCVCLIENT_INDEX rcv_index, int length,
				    void *data);
extern void log_append_client_postpone (LOG_RCVCLIENT_INDEX rcv_index,
					int length, void *data);
extern TRAN_STATE log_has_finished_client_postpone (void);
extern TRAN_STATE log_has_finished_client_undo (void);
extern int log_reset_waitsecs (int waitsecs);
extern int log_reset_isolation (TRAN_ISOLATION isolation,
				bool unlock_by_isolation);
extern void log_set_interrupt (int set);
extern void log_checkpoint (void);
extern void log_dump_stat (FILE * outfp);
extern void log_set_suppress_repl_on_transaction (int set);

extern TRAN_STATE tran_server_commit (bool retain_lock);
extern TRAN_STATE tran_server_abort (void);
#if defined(ENABLE_UNUSED_FUNCTION)
extern bool tran_is_blocked (int tran_index);
#endif
extern int tran_server_has_updated (void);
extern int tran_server_is_active_and_has_updated (void);
extern int tran_wait_server_active_trans (void);
extern int tran_server_set_global_tran_info (int gtrid, void *info, int size);
extern int tran_server_get_global_tran_info (int gtrid, void *buffer,
					     int size);
extern int tran_server_2pc_start (void);
extern TRAN_STATE tran_server_2pc_prepare (void);
extern int tran_server_2pc_recovery_prepared (int gtrids[], int size);
extern int tran_server_2pc_attach_global_tran (int gtrid);
extern TRAN_STATE tran_server_2pc_prepare_global_tran (int gtrid);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int tran_server_start_topop (LOG_LSA * topop_lsa);
extern TRAN_STATE tran_server_end_topop (LOG_RESULT_TOPOP result,
					 LOG_LSA * topop_lsa);
#endif
extern int tran_server_savepoint (const char *savept_name,
				  LOG_LSA * savept_lsa);
extern TRAN_STATE tran_server_partial_abort (const char *savept_name,
					     LOG_LSA * savept_lsa);
extern void lock_dump (FILE * outfp);

int boot_initialize_server (const BOOT_CLIENT_CREDENTIAL * client_credential,
			    BOOT_DB_PATH_INFO * db_path_info,
			    bool db_overwrite, const char *file_addmore_vols,
			    DKNPAGES db_npages, PGLENGTH db_desired_pagesize,
			    DKNPAGES log_npages,
			    PGLENGTH db_desired_log_page_size,
			    OID * rootclass_oid, HFID * rootclass_hfid,
			    int client_lock_wait,
			    TRAN_ISOLATION client_isolation);
int boot_register_client (const BOOT_CLIENT_CREDENTIAL * client_credential,
			  int client_lock_wait,
			  TRAN_ISOLATION client_isolation,
			  TRAN_STATE * tran_state,
			  BOOT_SERVER_CREDENTIAL * server_credential);
extern int boot_unregister_client (int tran_index);
extern int boot_backup (const char *backup_path,
			FILEIO_BACKUP_LEVEL backup_level,
			bool delete_unneeded_logarchives,
			const char *backup_verbose_file, int num_threads,
			FILEIO_ZIP_METHOD zip_method,
			FILEIO_ZIP_LEVEL zip_level, int skip_activelog,
			PAGEID safe_pageid);
extern VOLID boot_add_volume_extension (const char *ext_path,
					const char *ext_name,
					const char *ext_comments,
					DKNPAGES ext_npages,
					DISK_VOLPURPOSE ext_purpose,
					int ext_overwrite);
extern int boot_check_db_consistency (int check_flag);
extern int boot_find_number_permanent_volumes (void);
extern int boot_find_number_temp_volumes (void);
extern int boot_find_last_temp (void);
extern int boot_delete (const char *db_name, bool force_delete);
extern int boot_restart_from_backup (int print_restart, const char *db_name,
				     BO_RESTART_ARG * r_args);
extern bool boot_shutdown_server (bool iserfinal);
extern int boot_soft_rename (const char *olddb_name,
			     const char *newdb_name, const char *newdb_path,
			     const char *newlog_path,
			     const char *newdb_server_host,
			     const char *new_volext_path,
			     const char *fileof_vols_and_renamepaths,
			     bool newdb_overwrite, bool extern_rename,
			     bool force_delete);
extern int boot_copy (const char *from_dbname, const char *newdb_name,
		      const char *newdb_path, const char *newlog_path,
		      const char *newdb_server_host,
		      const char *new_volext_path,
		      const char *fileof_vols_and_copypaths,
		      bool newdb_overwrite);
extern int boot_emergency_patch (const char *db_name, bool recreate_log);
extern HA_SERVER_STATE boot_change_ha_mode (HA_SERVER_STATE state,
					    bool force, bool wait);
extern int boot_notify_ha_log_applier_state (HA_LOG_APPLIER_STATE state);
extern LOID *largeobjmgr_create (LOID * loid, int length, char *buffer,
				 int est_lo_length, OID * oid);
extern int largeobjmgr_destroy (LOID * loid);
extern int largeobjmgr_read (LOID * loid, INT64 offset, int length,
			     char *buffer);
extern int largeobjmgr_write (LOID * loid, INT64 offset, int length,
			      char *buffer);
extern int largeobjmgr_insert (LOID * loid, INT64 offset,
			       int length, char *buffer);
extern INT64 largeobjmgr_delete (LOID * loid, INT64 offset, INT64 length);
extern int largeobjmgr_append (LOID * loid, int length, char *buffer);
extern INT64 largeobjmgr_truncate (LOID * loid, INT64 offset);
extern int largeobjmgr_compress (LOID * loid);
extern INT64 largeobjmgr_length (LOID * loid);
extern char *stats_get_statistics_from_server (OID * classoid,
					       unsigned int timestamp,
					       int *length_ptr, int node_id);
extern int stats_update_class_statistics (OID * classoid, int node_id);
extern int stats_update_statistics (void);

extern int btree_add_index (BTID * btid, TP_DOMAIN * key_type,
			    OID * class_oid, int attr_id, int unique_btree,
			    int reverse_btree, int node);
extern int btree_load_index (BTID * btid, TP_DOMAIN * key_type,
			     OID * class_oids, int n_classes, int n_attrs,
			     int *attr_ids, int *attrs_prefix_length,
			     HFID * hfids, int unique_flag,
			     int reverse_flag, int last_key_desc,
			     OID * fk_refcls_oid, BTID * fk_refcls_pk_btid,
			     int cache_attr_id, const char *fk_name,
			     int node_id);
extern int btree_delete_index (BTID * btid, int node);
extern int locator_log_force_nologging (void);
extern int locator_remove_class_from_index (OID * oid, BTID * btid,
					    HFID * hfid);
extern BTREE_SEARCH
btree_find_unique (BTID * btid, DB_VALUE * key, OID * class_oid, OID * oid,
		   int node);
extern BTREE_SEARCH repl_btree_find_unique (BTID * btid, DB_VALUE * key,
					    OID * class_oid, OID * oid);
extern int btree_class_test_unique (char *buf, int buf_size);
extern int qfile_get_list_file_page (QUERY_ID query_id, VOLID volid,
				     PAGEID pageid, char *buffer,
				     int *buffer_size);
extern XASL_ID *qmgr_prepare_query (const char *query_str,
				    const OID * user_oid,
				    const char *xasl_buffer, int size,
				    XASL_ID * xasl_id);
extern QFILE_LIST_ID *qmgr_execute_query (const XASL_ID * xasl_id,
					  QUERY_ID * query_idp, int dbval_cnt,
					  const DB_VALUE * dbvals,
					  QUERY_FLAG flag,
					  CACHE_TIME * clt_cache_time,
					  CACHE_TIME * srv_cache_time);
extern QFILE_LIST_ID *qmgr_prepare_and_execute_query (char *xasl_buffer,
						      int xasl_size,
						      QUERY_ID * query_id,
						      int dbval_cnt,
						      DB_VALUE * dbval_ptr,
						      QUERY_FLAG flag);
extern int qmgr_end_query (QUERY_ID query_id);
extern int qmgr_drop_query_plan (const char *query_str, const OID * user_oid,
				 const XASL_ID * xasl_id, bool drop);
extern int qmgr_drop_all_query_plans (void);
extern void qmgr_dump_query_plans (FILE * outfp);
extern void qmgr_dump_query_cache (FILE * outfp);
extern int qmgr_get_query_info (DB_QUERY_RESULT * query_result, int *done,
				int *count, int *error, char **error_string);
extern int qmgr_sync_query (DB_QUERY_RESULT * query_result, int wait);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int qp_get_sys_timestamp (DB_VALUE * value);
#endif
extern int serial_get_next_value (DB_VALUE * value, DB_VALUE * oid);
extern int serial_get_current_value (DB_VALUE * value, DB_VALUE * oid);
extern int serial_decache (OID * oid, int node_id);

extern int mnt_server_start_stats (bool for_all_trans);
extern int mnt_server_stop_stats (void);
extern void mnt_server_reset_stats (void);
extern void mnt_server_reset_global_stats (void);
extern void mnt_server_copy_stats (MNT_SERVER_EXEC_STATS * to_stats);
extern void mnt_server_copy_global_stats (MNT_SERVER_EXEC_GLOBAL_STATS *
					  to_stats);
extern int catalog_is_acceptable_new_representation (OID * class_id,
						     HFID * hfid,
						     int *can_accept);
extern int thread_kill_tran_index (int kill_tran_index, char *kill_user,
				   char *kill_host, int kill_pid);
extern void thread_dump_cs_stat (FILE * outfp);

extern int logtb_get_pack_tran_table (char **buffer_p, int *size_p);
extern void logtb_free_trans_info (TRANS_INFO * info);
extern TRANS_INFO *logtb_get_trans_info (void);
extern void logtb_dump_trantable (FILE * outfp);

extern int heap_get_class_num_objects_pages (HFID * hfid, int approximation,
					     int *nobjs, int *npages);

extern int btree_get_statistics (BTID * btid, BTREE_STATS * stat_info);
extern int db_local_transaction_id (DB_VALUE * trid);
extern int qp_get_server_info (SERVER_INFO * server_info);
extern int heap_has_instance (HFID * hfid, OID * class_oid, int node);

extern int jsp_get_server_port (void);
extern int repl_log_get_append_lsa (LOG_LSA * lsa);
extern int repl_set_info (REPL_INFO * repl_info);

extern int logwr_get_log_pages (LOGWR_CONTEXT * ctx_ptr);

extern bool histo_is_supported (void);
extern int histo_start (bool for_all_trans);
extern int histo_stop (void);
extern void histo_print (FILE * stream);
extern void histo_print_global_stats (FILE * stream);
extern void histo_clear (void);
extern void histo_clear_global_stats (void);

extern int net_histo_start (bool for_all_trans);
extern int net_histo_stop (void);
extern void net_histo_print (FILE * stream);
extern void net_histo_print_global_stats (FILE * stream);
extern void net_histo_clear (void);
extern void net_histo_clear_global_stats (void);

extern int net_client_request_no_reply (int request, char *argbuf,
					int argsize);
extern int net_client_request (int request, char *argbuf, int argsize,
			       char *replybuf, int replysize, char *databuf,
			       int datasize, char *replydata,
			       int replydatasize);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int net_client_request_send_large_data (int request, char *argbuf,
					       int argsize, char *replybuf,
					       int replysize, char *databuf,
					       INT64 datasize,
					       char *replydata,
					       int replydatasize);
#endif
extern int net_client_request_via_oob (int request, char *argbuf, int argsize,
				       char *replybuf, int replysize,
				       char *databuf, int datasize,
				       char *replydata, int replydatasize);
extern int net_client_request2 (int request, char *argbuf, int argsize,
				char *replybuf, int replysize, char *databuf,
				int datasize, char **replydata_ptr,
				int *replydatasize_ptr);
extern int net_client_request2_no_malloc (int request, char *argbuf,
					  int argsize, char *replybuf,
					  int replysize, char *databuf,
					  int datasize, char *replydata,
					  int *replydatasize_ptr);
extern int net_client_request_3_data (int request, char *argbuf, int argsize,
				      char *databuf1, int datasize1,
				      char *databuf2, int datasize2,
				      char *replydata0, int replydatasize0,
				      char *replydata1, int replydatasize1,
				      char *replydata2, int replydatasize2);
extern int net_client_request_with_callback (int request, char *argbuf,
					     int argsize, char *replybuf,
					     int replysize, char *databuf1,
					     int datasize1, char *databuf2,
					     int datasize2,
					     char **replydata_ptr1,
					     int *replydatasize_ptr1,
					     char **replydata_ptr2,
					     int *replydatasize_ptr2);
extern int net_client_request_with_logwr_context (LOGWR_CONTEXT * ctx_ptr,
						  int request, char *argbuf,
						  int argsize, char *replybuf,
						  int replysize,
						  char *databuf1,
						  int datasize1,
						  char *databuf2,
						  int datasize2,
						  char **replydata_ptr1,
						  int *replydatasize_ptr1,
						  char **replydata_ptr2,
						  int *replydatasize_ptr2);
extern int net_client_get_next_log_pages (int rc, char *replybuf,
					  int replysize, int length);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int net_client_request3 (int request, char *argbuf, int argsize,
				char *replybuf, int replysize, char *databuf,
				int datasize, char **replydata_ptr,
				int *replydatasize_ptr, char **replydata_ptr2,
				int *replydatasize_ptr2);
#endif

extern int net_client_request_recv_copyarea (int request, char *argbuf,
					     int argsize, char *replybuf,
					     int replysize,
					     LC_COPYAREA ** reply_copy_area);
extern int net_client_request_recv_logarea (int request, char *argbuf,
					    int argsize, char *replybuf,
					    int replysize,
					    LOG_COPY ** reply_log_area);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int net_client_request_recv_large_data (int request, char *argbuf,
					       int argsize, char *replybuf,
					       int replysize, char *databuf,
					       int datasize, char *replydata,
					       INT64 * replydatasize_ptr);
#endif
extern int net_client_request_2recv_copyarea (int request, char *argbuf,
					      int argsize, char *replybuf,
					      int replysize, char *databuf,
					      int datasize, char *recvbuffer,
					      int recvbuffer_size,
					      LC_COPYAREA ** reply_copy_area,
					      int *eid);
extern int net_client_recv_copyarea (int request, char *replybuf,
				     int replysize, char *recvbuffer,
				     int recvbuffer_size,
				     LC_COPYAREA ** reply_copy_area, int eid);
extern int net_client_request_3recv_copyarea (int request, char *argbuf,
					      int argsize, char *replybuf,
					      int replysize, char *databuf,
					      int datasize, char **recvbuffer,
					      int *recvbuffer_size,
					      LC_COPYAREA ** reply_copy_area);
extern int net_client_request_recv_stream (int request, char *argbuf,
					   int argsize, char *replybuf,
					   int replybuf_size, char *databuf,
					   int datasize, FILE * outfp);
extern int net_client_ping_server (int client_val, int *server_val);
extern int net_client_ping_server_with_handshake (bool check_capabilities);

/* Startup/Shutdown */
#if defined(ENABLE_UNUSED_FUNCTION)
extern void net_client_shutdown_server (void);
#endif
extern int net_client_init (const char *dbname, const char *hostname);
extern int net_client_final (void);

extern void net_cleanup_client_queues (void);
extern int net_client_send_data (char *host, unsigned int rc, char *databuf,
				 int datasize);
extern int net_client_receive_action (int rc, int *action);

extern char *net_client_get_server_host (void);

extern int boot_compact_classes (OID ** class_oids,
				 int num_classes, int space_to_process,
				 int instance_lock_timeout,
				 int class_lock_timeout,
				 bool delete_old_repr,
				 OID * last_processed_class_oid,
				 OID * last_processed_oid,
				 int *total_objects, int *failed_objects,
				 int *modified_objects, int *big_objects,
				 int *ids_repr);

extern int boot_heap_compact (OID * class_oid);

extern int compact_db_start (void);
extern int compact_db_stop (void);

extern int net_connection_switch_to_server (int storage_id);
extern int net_connection_switch_back (void);

#endif /* _NETWORK_INTERFACE_CL_H_ */
