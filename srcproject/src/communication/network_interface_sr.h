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
 *      network_interface_sr.h -Definitions for server network support.
 */

#ifndef _NETWORK_INTERFACE_SR_H_
#define _NETWORK_INTERFACE_SR_H_

#ident "$Id$"

#include "query_opfunc.h"	/* for VACOMM stuff */
#include "thread_impl.h"

extern void return_error_to_client (THREAD_ENTRY * thread_p,
				    unsigned int rid);
extern int server_ping_with_handshake (THREAD_ENTRY * thread_p,
				       unsigned int rid, char *request,
				       int reqlen);
extern void server_ping (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, int reqlen);
extern void slocator_fetch (THREAD_ENTRY * thrd, unsigned int rid,
			    char *request, int reqlen);
extern void slocator_get_class (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);

extern void slocator_fetch_all (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);
extern void slocator_does_exist (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void slocator_notify_isolation_incons (THREAD_ENTRY * thread_p,
					      unsigned int rid, char *request,
					      int reqlen);
extern void slocator_force (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, int reqlen);
extern void slocator_fetch_lockset (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void slocator_fetch_all_reference_lockset (THREAD_ENTRY * thread_p,
						  unsigned int rid,
						  char *request, int reqlen);
extern void slocator_find_class_oid (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void slocator_reserve_classnames (THREAD_ENTRY * thread_p,
					 unsigned int rid, char *request,
					 int reqlen);
extern void slocator_delete_class_name (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void slocator_rename_class_name (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void slocator_assign_oid (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void sqst_server_get_statistics (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void slog_client_get_first_postpone (THREAD_ENTRY * thread_p,
					    unsigned int rid, char *request,
					    int reqlen);
extern void slog_client_get_next_postpone (THREAD_ENTRY * thread_p,
					   unsigned int rid, char *request,
					   int reqlen);
extern void slog_client_get_first_undo (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void slog_client_get_next_undo (THREAD_ENTRY * thread_p,
				       unsigned int rid, char *request,
				       int reqlen);
extern void slog_client_unknown_state_abort_get_first_undo (THREAD_ENTRY *
							    thread_p,
							    unsigned int rid,
							    char *request,
							    int reqlen);
extern void slog_append_client_undo (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void slog_append_client_postpone (THREAD_ENTRY * thread_p,
					 unsigned int rid, char *request,
					 int reqlen);
extern void slog_client_complete_postpone (THREAD_ENTRY * thread_p,
					   unsigned int rid, char *request,
					   int reqlen);
extern void slog_client_complete_undo (THREAD_ENTRY * thread_p,
				       unsigned int rid, char *request,
				       int reqlen);
extern void slog_checkpoint (THREAD_ENTRY * thread_p, unsigned int rid,
			     char *request, int reqlen);
#if defined(ENABLE_UNUSED_FUNCTION)
extern void slogtb_has_updated (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);
#endif
extern void slogtb_set_interrupt (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void slogtb_set_suppress_repl_on_transaction (THREAD_ENTRY * thread_p,
						     unsigned int rid,
						     char *request,
						     int reqlen);
extern void slogtb_reset_wait_secs (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void slogtb_reset_isolation (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void slogpb_dump_stat (THREAD_ENTRY * thread_p, unsigned int rid,
			      char *request, int reqlen);
extern void slock_dump (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, int reqlen);
extern void shf_create (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, int reqlen);
extern void shf_destroy (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, int reqlen);
extern void shf_destroy_when_new (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void shf_heap_reclaim_addresses (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void stran_server_commit (THREAD_ENTRY * thrd, unsigned int rid,
				 char *request, int reqlen);
extern void stran_server_abort (THREAD_ENTRY * thrd, unsigned int rid,
				char *request, int reqlen);
extern void stran_server_has_updated (THREAD_ENTRY * thread_p,
				      unsigned int rid, char *request,
				      int reqlen);
extern void stran_server_start_topop (THREAD_ENTRY * thread_p,
				      unsigned int rid, char *request,
				      int reqlen);
extern void stran_server_end_topop (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void stran_server_savepoint (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void stran_server_partial_abort (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void stran_server_is_active_and_has_updated (THREAD_ENTRY * thread_p,
						    unsigned int rid,
						    char *request,
						    int reqlen);
extern void stran_wait_server_active_trans (THREAD_ENTRY * thread_p,
					    unsigned int rid, char *request,
					    int reqlen);
extern void stran_is_blocked (THREAD_ENTRY * thread_p, unsigned int rid,
			      char *request, int reqlen);
extern void stran_server_set_global_tran_info (THREAD_ENTRY * thread_p,
					       unsigned int rid,
					       char *request, int reqlen);
extern void stran_server_get_global_tran_info (THREAD_ENTRY * thread_p,
					       unsigned int rid,
					       char *request, int reqlen);
extern void stran_server_2pc_start (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void stran_server_2pc_prepare (THREAD_ENTRY * thread_p,
				      unsigned int rid, char *request,
				      int reqlen);
extern void stran_server_2pc_recovery_prepared (THREAD_ENTRY * thread_p,
						unsigned int rid,
						char *request, int reqlen);
extern void stran_server_2pc_attach_global_tran (THREAD_ENTRY * thread_p,
						 unsigned int rid,
						 char *request, int reqlen);
extern void stran_server_2pc_prepare_global_tran (THREAD_ENTRY * thread_p,
						  unsigned int rid,
						  char *request, int reqlen);
extern void sboot_initialize_server (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void sboot_register_client (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);
extern void sboot_notify_unregister_client (THREAD_ENTRY * thread_p,
					    unsigned int rid, char *request,
					    int reqlen);
extern void sboot_backup (THREAD_ENTRY * thread_p, unsigned int rid,
			  char *request, int reqlen);
extern void sboot_add_volume_extension (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void sboot_check_db_consistency (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void sboot_find_number_permanent_volumes (THREAD_ENTRY * thread_p,
						 unsigned int rid,
						 char *request, int reqlen);
extern void sboot_find_number_temp_volumes (THREAD_ENTRY * thread_p,
					    unsigned int rid, char *request,
					    int reqlen);
extern void sboot_find_last_temp (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void sboot_change_ha_mode (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void sboot_notify_ha_log_applier_state (THREAD_ENTRY * thread_p,
					       unsigned int rid,
					       char *request, int reqlen);
extern void slargeobjmgr_create (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void slargeobjmgr_destroy (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void slargeobjmgr_read (THREAD_ENTRY * thread_p, unsigned int rid,
			       char *request, int reqlen);
extern void slargeobjmgr_write (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);
extern void slargeobjmgr_insert (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void slargeobjmgr_delete (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void slargeobjmgr_append (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void slargeobjmgr_truncate (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);
extern void slargeobjmgr_compress (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);
extern void slargeobjmgr_length (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void sqst_update_class_statistics (THREAD_ENTRY * thread_p,
					  unsigned int rid, char *request,
					  int reqlen);
extern void sqst_update_statistics (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void sbtree_add_index (THREAD_ENTRY * thread_p, unsigned int rid,
			      char *request, int reqlen);
extern void sbtree_load_index (THREAD_ENTRY * thread_p, unsigned int rid,
			       char *request, int reqlen);
extern void sbtree_delete_index (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void slocator_remove_class_from_index (THREAD_ENTRY * thread_p,
					      unsigned int rid, char *request,
					      int reqlen);
extern void sbtree_find_unique (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);
extern void srepl_btree_find_unique (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void sbtree_class_test_unique (THREAD_ENTRY * thread_p,
				      unsigned int rid, char *request,
				      int reqlen);
extern void sdk_totalpgs (THREAD_ENTRY * thread_p, unsigned int rid,
			  char *request, int reqlen);
extern void sdk_freepgs (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, int reqlen);
extern void sdk_remarks (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, int reqlen);
extern void sdk_purpose (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, int reqlen);
extern void sdk_purpose_totalpgs_and_freepgs (THREAD_ENTRY * thread_p,
					      unsigned int rid, char *request,
					      int reqlen);
extern void sdk_vlabel (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, int reqlen);
extern void sqfile_get_list_file_page (THREAD_ENTRY * thread_p,
				       unsigned int rid, char *request,
				       int reqlen);
extern void sqmgr_prepare_query (THREAD_ENTRY * thrd, unsigned int rid,
				 char *request, int reqlen);
extern void sqmgr_execute_query (THREAD_ENTRY * thrd, unsigned int rid,
				 char *request, int reqlen);
extern void sqmgr_prepare_and_execute_query (THREAD_ENTRY * thrd,
					     unsigned int rid, char *request,
					     int reqlen);
extern void sqmgr_end_query (THREAD_ENTRY * thrd, unsigned int rid,
			     char *request, int reqlen);
extern void sqmgr_drop_query_plan (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);
extern void sqmgr_drop_all_query_plans (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void sqmgr_dump_query_plans (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void sqmgr_dump_query_cache (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void sqmgr_get_query_info (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void sqmgr_sync_query (THREAD_ENTRY * thread_p, unsigned int rid,
			      char *request, int reqlen);
extern void sqp_get_sys_timestamp (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);
extern void sserial_get_current_value (THREAD_ENTRY * thread_p,
				       unsigned int rid, char *request,
				       int reqlen);
extern void sserial_get_next_value (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void sserial_decache (THREAD_ENTRY * thread_p, unsigned int rid,
			     char *request, int reqlen);
extern void sserial_get_real_oid (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void sserial_get_cache_range (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void smnt_server_start_stats (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void smnt_server_stop_stats (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void smnt_server_reset_stats (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void smnt_server_reset_global_stats (THREAD_ENTRY * thread_p,
					    unsigned int rid, char *request,
					    int reqlen);
extern void smnt_server_copy_stats (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void smnt_server_copy_global_stats (THREAD_ENTRY * thread_p,
					   unsigned int rid, char *request,
					   int reqlen);
extern void sct_can_accept_new_repr (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern int xs_send_method_call_info_to_client (THREAD_ENTRY * thread_p,
					       QFILE_LIST_ID * list_id,
					       METHOD_SIG_LIST *
					       method_sig_list);
extern int xs_receive_data_from_client (THREAD_ENTRY * thread_p, char **area,
					int *datasize);
extern int xs_send_action_to_client (THREAD_ENTRY * thread_p,
				     VACOMM_BUFFER_CLIENT_ACTION action);
extern void stest_performance (THREAD_ENTRY * thread_p, unsigned int rid,
			       char *request, int reqlen);
extern void slocator_assign_oid_batch (THREAD_ENTRY * thread_p,
				       unsigned int rid, char *request,
				       int reqlen);
extern void slocator_find_lockhint_class_oids (THREAD_ENTRY * thread_p,
					       unsigned int rid,
					       char *request, int reqlen);
extern void slocator_fetch_lockhint_classes (THREAD_ENTRY * thread_p,
					     unsigned int rid, char *request,
					     int reqlen);
extern void tm_restart_event_handler (unsigned int, char *, int);
extern void sthread_kill_tran_index (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void sthread_dump_cs_stat (THREAD_ENTRY * thread_p,
				  unsigned int rid, char *request,
				  int reqlen);
extern void slogtb_get_pack_tran_table (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void slogtb_dump_trantable (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);

extern int xio_send_user_prompt_to_client (THREAD_ENTRY * thread_p,
					   FILEIO_REMOTE_PROMPT_TYPE
					   prompt_id, const char *buffer,
					   const char *failure_prompt,
					   int range_low, int range_high,
					   const char *secondary_prompt,
					   int reprompt_value);
extern int xlog_send_log_pages_to_client (THREAD_ENTRY * thread_p,
					  char *logpb_area, int area_size,
					  LOGWR_MODE mode);
extern int xlog_get_page_request_with_reply (THREAD_ENTRY * thread_p,
					     PAGEID * fpageid_ptr,
					     LOGWR_MODE * mode_ptr);
extern void shf_get_class_num_objs_and_pages (THREAD_ENTRY * thread_p,
					      unsigned int rid, char *request,
					      int reqlen);
extern void sbtree_get_statistics (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);
extern void sqp_get_server_info (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void sprm_server_change_parameters (THREAD_ENTRY * thread_p,
					   unsigned int rid, char *request,
					   int reqlen);
extern void sprm_server_obtain_parameters (THREAD_ENTRY * thread_p,
					   unsigned int rid, char *request,
					   int reqlen);
extern void sprm_server_dump_parameters (THREAD_ENTRY * thread_p,
					 unsigned int rid, char *request,
					 int reqlen);
extern void shf_has_instance (THREAD_ENTRY * thread_p, unsigned int rid,
			      char *request, int reqlen);
extern void stran_get_local_transaction_id (THREAD_ENTRY * thread_p,
					    unsigned int rid, char *request,
					    int reqlen);
extern void sjsp_get_server_port (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void srepl_set_info (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, int reqlen);
extern void srepl_log_get_append_lsa (THREAD_ENTRY * thread_p,
				      unsigned int rid, char *request,
				      int reqlen);
extern void slocator_build_fk_object_cache (THREAD_ENTRY * thread_p,
					    unsigned int rid, char *request,
					    int reqlen);
extern void slogwr_get_log_pages (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);

extern void net_cleanup_server_queues (unsigned int rid);

extern void sboot_compact_db (THREAD_ENTRY * thread_p, unsigned int rid,
			      char *request, int reqlen);

extern void sboot_heap_compact (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);

extern void sboot_compact_start (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void sboot_compact_stop (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);

#endif /* _NETWORK_INTERFACE_SR_H_ */
