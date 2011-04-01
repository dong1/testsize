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
 * system_parameter.c - system parameters
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <float.h>
#include <errno.h>
#include <time.h>
#if defined (WINDOWS)
#include <io.h>
#include <direct.h>
#else
#include <unistd.h>
#endif /* !WINDOWS */
#include <sys/types.h>
#include <sys/stat.h>
#if !defined(WINDOWS)
#include <sys/param.h>
#endif
#include <assert.h>

#include "porting.h"
#include "chartype.h"
#include "misc_string.h"
#include "error_manager.h"
#include "storage_common.h"
#include "system_parameter.h"
#include "xserver_interface.h"
#include "util_func.h"
#include "log_comm.h"
#include "log_impl.h"
#include "memory_alloc.h"
#include "environment_variable.h"
#include "intl_support.h"
#include "log_manager.h"
#include "message_catalog.h"
#include "language_support.h"
#include "connection_defs.h"
#if defined (SERVER_MODE)
#include "server_support.h"
#endif /* SERVER_MODE */
#if defined (LINUX)
#include "stack_dump.h"
#endif
#include "ini_parser.h"
#if defined(WINDOWS)
#include "wintcp.h"
#else /* WINDOWS */
#include "tcp.h"
#endif /* WINDOWS */
#include "heartbeat.h"


#if defined (WINDOWS)
#define PATH_SEPARATOR  '\\'
#else
#define PATH_SEPARATOR  '/'
#endif
#define PATH_CURRENT    '.'

#define ER_LOG_FILE_DIR	"server"
#if !defined (CS_MODE)
static const char sysprm_error_log_file[] = "cub_server.err";
#else /* CS_MODE */
static const char sysprm_error_log_file[] = "cub_client.err";
#endif /* CS_MODE */
static const char sysprm_conf_file_name[] = "cubrid.conf";


/*
 * Note about ERROR_LIST and INTEGER_LIST type
 * ERROR_LIST type is an array of bool type with the size of -(ER_LAST_ERROR)
 * INTEGER_LIST type is an array of int type where the first element is
 * the size of the array. The max size of INTEGER_LIST is 255.
 */

/*
 * Bit masks for flag representing status words
 */

#define PRM_REQUIRED        0x00000001	/* Must be set with default or by user */
#define PRM_SET             0x00000002	/* has been set */
#define PRM_STRING          0x00000004	/* is string value */
#define PRM_INTEGER         0x00000008	/* is integer value */
#define PRM_FLOAT           0x00000010	/* is float value */
#define PRM_BOOLEAN         0x00000020	/* is boolean value */
#define PRM_KEYWORD         0x00000040	/* is keyword value */
#define PRM_ERROR_LIST      0x00000080	/* is error list value */
#define PRM_DEFAULT         0x00000100	/* has system default */
#define PRM_USER_CHANGE     0x00000200	/* user can change, not implemented */
#define PRM_ALLOCATED       0x00000400	/* storage has been malloc'd */
#define PRM_LOWER_LIMIT     0x00000800	/* has lower limit */
#define PRM_UPPER_LIMIT     0x00001000	/* has upper limit */
#define PRM_DEFAULT_USED    0x00002000	/* Default value has been used */
#define PRM_FOR_CLIENT      0x00004000	/* is for client parameter */
#define PRM_FOR_SERVER      0x00008000	/* is for server parameter */
#define PRM_HIDDEN          0x00010000	/* is hidden */
#define PRM_RELOADABLE      0x00020000	/* is reloadable */
#define PRM_INTEGER_LIST    0x00040000	/* is integer list value */
#define PRM_COMPOUND        0x00080000	/* sets the value of several others */
#define PRM_TEST_CHANGE     0x00100000	/* can only be changed in the test mode */

/*
 * Macros to access bit fields
 */

#define PRM_IS_REQUIRED(x)        (x & PRM_REQUIRED)
#define PRM_IS_SET(x)             (x & PRM_SET)
#define PRM_IS_STRING(x)          (x & PRM_STRING)
#define PRM_IS_INTEGER(x)         (x & PRM_INTEGER)
#define PRM_IS_FLOAT(x)           (x & PRM_FLOAT)
#define PRM_IS_BOOLEAN(x)         (x & PRM_BOOLEAN)
#define PRM_IS_KEYWORD(x)         (x & PRM_KEYWORD)
#define PRM_IS_ERROR_LIST(x)      (x & PRM_ERROR_LIST)
#define PRM_IS_INTEGER_LIST(x)    (x & PRM_INTEGER_LIST)
#define PRM_HAS_DEFAULT(x)        (x & PRM_DEFAULT)
#define PRM_USER_CAN_CHANGE(x)    (x & PRM_USER_CHANGE)
#define PRM_IS_ALLOCATED(x)       (x & PRM_ALLOCATED)
#define PRM_HAS_LOWER_LIMIT(x)    (x & PRM_LOWER_LIMIT)
#define PRM_HAS_UPPER_LIMIT(x)    (x & PRM_UPPER_LIMIT)
#define PRM_DEFAULT_VAL_USED(x)   (x & PRM_DEFAULT_USED)
#define PRM_IS_FOR_CLIENT(x)      (x & PRM_FOR_CLIENT)
#define PRM_IS_FOR_SERVER(x)      (x & PRM_FOR_SERVER)
#define PRM_IS_HIDDEN(x)          (x & PRM_HIDDEN)
#define PRM_IS_RELOADABLE(x)      (x & PRM_RELOADABLE)
#define PRM_IS_COMPOUND(x)        (x & PRM_COMPOUND)
#define PRM_TEST_CHANGE_ONLY(x)   (x & PRM_TEST_CHANGE)

/*
 * Macros to manipulate bit fields
 */

#define PRM_CLEAR_BIT(this, here)  (here &= ~this)
#define PRM_SET_BIT(this, here)    (here |= this)

/*
 * Macros to get values
 */

#define PRM_GET_INT(x)      (*((int *) (x)))
#define PRM_GET_FLOAT(x)    (*((float *) (x)))
#define PRM_GET_STRING(x)   (*((char **) (x)))
#define PRM_GET_BOOL(x)     (*((bool *) (x)))
#define PRM_GET_ERROR_LIST(x)   (*((bool **) (x)))
#define PRM_GET_INTEGER_LIST(x) (*((int **) (x)))

static bool error_list_intial[1] = { false };
static int int_list_initial[1] = { 0 };

/*
 * Global variables of parameters' value
 * Default values for the parameters
 * Upper and lower bounds for the parameters
 */
bool PRM_ER_LOG_DEBUG = false;
#if defined(NDEBUG)
static bool prm_er_log_debug_default = false;
#else /* NDEBUG */
static bool prm_er_log_debug_default = true;
#endif /*!NDEBUG */

int PRM_ER_LOG_LEVEL = ER_SYNTAX_ERROR_SEVERITY;
static int prm_er_log_level_default = ER_SYNTAX_ERROR_SEVERITY;
static int prm_er_log_level_lower = ER_FATAL_ERROR_SEVERITY;
static int prm_er_log_level_upper = ER_NOTIFICATION_SEVERITY;

bool PRM_ER_LOG_WARNING = false;
static bool prm_er_log_warning_default = false;

int PRM_ER_EXIT_ASK = ER_EXIT_DEFAULT;
static int prm_er_exit_ask_default = ER_EXIT_DEFAULT;

int PRM_ER_LOG_SIZE = INT_MIN;
static int prm_er_log_size_default = (100000 * 80L);
static int prm_er_log_size_lower = (100 * 80);

const char *PRM_ER_LOG_FILE = sysprm_error_log_file;
static const char *prm_er_log_file_default = sysprm_error_log_file;

bool PRM_IO_LOCKF_ENABLE = false;
static bool prm_io_lockf_enable_default = true;

int PRM_SR_NBUFFERS = INT_MIN;
static int prm_sr_nbuffers_default = 16;
static int prm_sr_nbuffers_lower = 1;

int PRM_PB_NBUFFERS = INT_MIN;
static int prm_pb_nbuffers_default = 25000;
static int prm_pb_nbuffers_lower = 1;

float PRM_HF_UNFILL_FACTOR = FLT_MIN;
static float prm_hf_unfill_factor_default = 0.10f;
static float prm_hf_unfill_factor_lower = 0.0f;
static float prm_hf_unfill_factor_upper = 0.3f;

float PRM_BT_UNFILL_FACTOR = FLT_MIN;
static float prm_bt_unfill_factor_default = 0.20f;
static float prm_bt_unfill_factor_lower = 0.0f;
static float prm_bt_unfill_factor_upper = 0.35f;

float PRM_BT_OID_NBUFFERS = FLT_MIN;
static float prm_bt_oid_nbuffers_default = 4.0f;
static float prm_bt_oid_nbuffers_lower = 0.05f;
static float prm_bt_oid_nbuffers_upper = 16.0f;

bool PRM_BT_INDEX_SCAN_OID_ORDER = false;
static bool prm_bt_index_scan_oid_order_default = false;

int PRM_BOSR_MAXTMP_PAGES = INT_MIN;
static int prm_bosr_maxtmp_pages = -1;	/* Infinite */

int PRM_LK_TIMEOUT_MESSAGE_DUMP_LEVEL = INT_MIN;
static int prm_lk_timeout_message_dump_level_default = 0;
static int prm_lk_timeout_message_dump_level_lower = 0;
static int prm_lk_timeout_message_dump_level_upper = 2;

int PRM_LK_ESCALATION_AT = INT_MIN;
static int prm_lk_escalation_at_default = 100000;
static int prm_lk_escalation_at_lower = 5;

int PRM_LK_TIMEOUT_SECS = INT_MIN;
static int prm_lk_timeout_secs_default = -1;	/* Infinite */
static int prm_lk_timeout_secs_lower = -1;

int PRM_LK_RUN_DEADLOCK_INTERVAL = INT_MIN;
static int prm_lk_run_deadlock_interval_default = 1;
static int prm_lk_run_deadlock_interval_lower = 1;

int PRM_LOG_NBUFFERS = INT_MIN;
static int prm_log_nbuffers_default = 50;
static int prm_log_nbuffers_lower = 3;

int PRM_LOG_CHECKPOINT_NPAGES = INT_MIN;
static int prm_log_checkpoint_npages_default = 10000;
static int prm_log_checkpoint_npages_lower = 10;

int PRM_LOG_CHECKPOINT_INTERVAL_MINUTES = INT_MIN;
static int prm_log_checkpoint_interval_minutes_default = 720;
static int prm_log_checkpoint_interval_minutes_lower = 1;

bool PRM_LOG_BACKGROUND_ARCHIVING = true;
static bool prm_log_background_archiving_default = true;

int PRM_LOG_ISOLATION_LEVEL = INT_MIN;
static int prm_log_isolation_level_default = TRAN_REP_CLASS_UNCOMMIT_INSTANCE;
static int prm_log_isolation_level_lower =
  TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE;
static int prm_log_isolation_level_upper = TRAN_SERIALIZABLE;

bool PRM_LOG_MEDIA_FAILURE_SUPPORT = false;
static bool prm_log_media_failure_support_default = true;

bool PRM_COMMIT_ON_SHUTDOWN = false;
static bool prm_commit_on_shutdown_default = false;

bool PRM_CSQL_AUTO_COMMIT = false;
static bool prm_csql_auto_commit_default = true;

bool PRM_LOG_SWEEP_CLEAN = true;
static bool prm_log_sweep_clean_default = true;

int PRM_WS_HASHTABLE_SIZE = INT_MIN;
static int prm_ws_hashtable_size_default = 4096;
static int prm_ws_hashtable_size_lower = 1024;

bool PRM_WS_MEMORY_REPORT = false;
static bool prm_ws_memory_report_default = false;

bool PRM_GC_ENABLE = false;
static bool prm_gc_enable_default = false;

int PRM_TCP_PORT_ID = INT_MIN;
static int prm_tcp_port_id_default = 1523;

int PRM_TCP_CONNECTION_TIMEOUT = INT_MIN;
static int prm_tcp_connection_timeout_default = 5;
static int prm_tcp_connection_timeout_lower = -1;

int PRM_OPTIMIZATION_LEVEL = INT_MIN;
static int prm_optimization_level_default = 1;

bool PRM_QO_DUMP = false;
static bool prm_qo_dump_default = false;

int PRM_CSS_MAX_CLIENTS = INT_MIN;
static int prm_css_max_clients_default = 50;
static int prm_css_max_clients_lower = 10;
static int prm_css_max_clients_upper = 1024;

int PRM_THREAD_STACKSIZE = INT_MIN;
static int prm_thread_stacksize_default = (100 * 1024);
static int prm_thread_stacksize_lower = 64 * 1024;

const char *PRM_CFG_DB_HOSTS = "";
static const char *prm_cfg_db_hosts_default = NULL;

int PRM_RESET_TR_PARSER = INT_MIN;
static int prm_reset_tr_parser_default = 10;

int PRM_IO_BACKUP_NBUFFERS = INT_MIN;
static int prm_io_backup_nbuffers_default = 256;
static int prm_io_backup_nbuffers_lower = 256;

int PRM_IO_BACKUP_MAX_VOLUME_SIZE = INT_MIN;
static int prm_io_backup_max_volume_size_default = -1;
static int prm_io_backup_max_volume_size_lower = 1024 * 32;

int PRM_MAX_PAGES_IN_TEMP_FILE_CACHE = INT_MIN;
static int prm_max_pages_in_temp_file_cache_default = 1000;	/* pages */
static int prm_max_pages_in_temp_file_cache_lower = 100;

int PRM_MAX_ENTRIES_IN_TEMP_FILE_CACHE = INT_MIN;
static int prm_max_entries_in_temp_file_cache_default = 512;
static int prm_max_entries_in_temp_file_cache_lower = 10;

bool PRM_PTHREAD_SCOPE_PROCESS = false;
static bool prm_pthread_scope_process_default = true;

int PRM_TEMP_MEM_BUFFER_PAGES = INT_MIN;
static int prm_temp_mem_buffer_pages_default = 4;
static int prm_temp_mem_buffer_pages_lower = 0;
static int prm_temp_mem_buffer_pages_upper = 20;

bool PRM_DONT_REUSE_HEAP_FILE = false;
static bool prm_dont_reuse_heap_file_default = false;

bool PRM_QUERY_MODE_SYNC = false;
static bool prm_query_mode_sync_default = false;

int PRM_INSERT_MODE = INT_MIN;
static int prm_insert_mode_default = 1 + 8 + 16;
static int prm_insert_mode_lower = 0;
static int prm_insert_mode_upper = 31;

int PRM_LK_MAX_SCANID_BIT = INT_MIN;
static int prm_lk_max_scanid_bit_default = 32;
static int prm_lk_max_scanid_bit_lower = 32;
static int prm_lk_max_scanid_bit_upper = 128;

bool PRM_HOSTVAR_LATE_BINDING = false;
static bool prm_hostvar_late_binding_default = false;

bool PRM_ENABLE_HISTO = false;
static bool prm_enable_histo_default = false;

int PRM_MUTEX_BUSY_WAITING_CNT = INT_MIN;
static int prm_mutex_busy_waiting_cnt_default = 0;

int PRM_PB_NUM_LRU_CHAINS = INT_MIN;
static int prm_pb_num_LRU_chains_default = 0;	/* system define */
static int prm_pb_num_LRU_chains_lower = 0;
static int prm_pb_num_LRU_chains_upper = 1000;

int PRM_PAGE_BG_FLUSH_INTERVAL_MSEC = INT_MIN;
static int prm_page_bg_flush_interval_msec_default = 0;
static int prm_page_bg_flush_interval_msec_lower = -1;

bool PRM_ADAPTIVE_FLUSH_CONTROL = false;
static bool prm_adaptive_flush_control_default = true;

int PRM_MAX_FLUSH_PAGES_PER_SECOND = INT_MAX;
static int prm_max_flush_pages_per_second_default = 10000;
static int prm_max_flush_pages_per_second_lower = 1;
static int prm_max_flush_pages_per_second_upper = INT_MAX;

int PRM_PB_SYNC_ON_NFLUSH = INT_MAX;
static int prm_pb_sync_on_nflush_default = 200;
static int prm_pb_sync_on_nflush_lower = 1;
static int prm_pb_sync_on_nflush_upper = INT_MAX;

bool PRM_ORACLE_STYLE_OUTERJOIN = false;
static bool prm_oracle_style_outerjoin_default = false;

int PRM_COMPAT_MODE = COMPAT_CUBRID;
static int prm_compat_mode_default = COMPAT_CUBRID;
static int prm_compat_mode_lower = COMPAT_CUBRID;
static int prm_compat_mode_upper = COMPAT_ORACLE;

bool PRM_ANSI_QUOTES = true;
static bool prm_ansi_quotes_default = true;

bool PRM_TEST_MODE = false;
static bool prm_test_mode_default = false;

bool PRM_ONLY_FULL_GROUP_BY = false;
static bool prm_only_full_group_by_default = false;

bool PRM_PIPES_AS_CONCAT = true;
static bool prm_pipes_as_concat_default = true;

bool PRM_MYSQL_TRIGGER_CORRELATION_NAMES = false;
static bool prm_mysql_trigger_correlation_names_default = false;

int PRM_COMPACTDB_PAGE_RECLAIM_ONLY = INT_MIN;
static int prm_compactdb_page_reclaim_only_default = 0;

float PRM_LIKE_TERM_SELECTIVITY = 0;
static float prm_like_term_selectivity_default = 0.1f;
static float prm_like_term_selectivity_upper = 1.0f;
static float prm_like_term_selectivity_lower = 0.0f;

int PRM_MAX_OUTER_CARD_OF_IDXJOIN = INT_MIN;
static int prm_max_outer_card_of_idxjoin_default = 0;
static int prm_max_outer_card_of_idxjoin_lower = 0;

bool PRM_ORACLE_STYLE_EMPTY_STRING = false;
static bool prm_oracle_style_empty_string_default = false;

int PRM_SUPPRESS_FSYNC = INT_MIN;
static int prm_suppress_fsync_default = 0;
static int prm_suppress_fsync_upper = 100;
static int prm_suppress_fsync_lower = 0;

bool PRM_CALL_STACK_DUMP_ON_ERROR = false;
static bool prm_call_stack_dump_on_error_default = false;

bool *PRM_CALL_STACK_DUMP_ACTIVATION = error_list_intial;
static bool *prm_call_stack_dump_activation_default = NULL;

bool *PRM_CALL_STACK_DUMP_DEACTIVATION = error_list_intial;
static bool *prm_call_stack_dump_deactivation_default = NULL;

bool PRM_COMPAT_NUMERIC_DIVISION_SCALE = false;
static bool prm_compat_numeric_division_scale_default = false;

bool PRM_DBFILES_PROTECT = false;
static bool prm_dbfiles_protect_default = false;

bool PRM_AUTO_RESTART_SERVER = false;
static bool prm_auto_restart_server_default = true;

int PRM_XASL_MAX_PLAN_CACHE_ENTRIES = INT_MIN;
static int prm_xasl_max_plan_cache_entries_default = 1000;

#if defined (ENABLE_UNUSED_FUNCTION)
int PRM_XASL_MAX_PLAN_CACHE_CLONES = INT_MIN;
static int prm_xasl_max_plan_cache_clones_default = -1;	/* disabled */
#endif /* ENABLE_UNUSED_FUNCTION */

int PRM_XASL_PLAN_CACHE_TIMEOUT = INT_MIN;
static int prm_xasl_plan_cache_timeout_default = -1;	/* infinity */

int PRM_LIST_QUERY_CACHE_MODE = INT_MIN;
static int prm_list_query_cache_mode_default = 0;	/* disabled */
static int prm_list_query_cache_mode_upper = 2;
static int prm_list_query_cache_mode_lower = 0;

int PRM_LIST_MAX_QUERY_CACHE_ENTRIES = INT_MIN;
static int prm_list_max_query_cache_entries_default = -1;	/* disabled */

int PRM_LIST_MAX_QUERY_CACHE_PAGES = INT_MIN;
static int prm_list_max_query_cache_pages_default = -1;	/* infinity */

bool PRM_REPLICATION_MODE = false;
static bool prm_replication_mode_default = false;

int PRM_HA_MODE = INT_MIN;
static int prm_ha_mode_default = HA_MODE_OFF;
static int prm_ha_mode_upper = HA_MODE_ROLE_CHANGE;
static int prm_ha_mode_lower = HA_MODE_OFF;

int PRM_HA_SERVER_STATE = INT_MIN;
static int prm_ha_server_state_default = HA_SERVER_STATE_IDLE;
static int prm_ha_server_state_upper = HA_SERVER_STATE_DEAD;
static int prm_ha_server_state_lower = HA_SERVER_STATE_IDLE;

int PRM_HA_LOG_APPLIER_STATE = INT_MIN;
static int prm_ha_log_applier_state_default =
  HA_LOG_APPLIER_STATE_UNREGISTERED;
static int prm_ha_log_applier_state_upper = HA_LOG_APPLIER_STATE_ERROR;
static int prm_ha_log_applier_state_lower = HA_LOG_APPLIER_STATE_UNREGISTERED;

const char *PRM_HA_NODE_LIST = "";
static const char *prm_ha_node_list_default = NULL;

int PRM_HA_PORT_ID = INT_MIN;
static int prm_ha_port_id_default = HB_DEFAULT_HA_PORT_ID;

int PRM_HA_INIT_TIMER_IN_MSECS = INT_MIN;
static int prm_ha_init_timer_im_msecs_default =
  HB_DEFAULT_INIT_TIMER_IN_MSECS;

int PRM_HA_HEARTBEAT_INTERVAL_IN_MSECS = INT_MIN;
static int prm_ha_heartbeat_interval_in_msecs_default =
  HB_DEFAULT_HEARTBEAT_INTERVAL_IN_MSECS;

int PRM_HA_CALC_SCORE_INTERVAL_IN_MSECS = INT_MIN;
static int prm_ha_calc_score_interval_in_msecs_default =
  HB_DEFAULT_CALC_SCORE_INTERVAL_IN_MSECS;

int PRM_HA_FAILOVER_WAIT_TIME_IN_MSECS = INT_MIN;
static int prm_ha_failover_wait_time_in_msecs_default =
  HB_DEFAULT_FAILOVER_WAIT_TIME_IN_MSECS;

int PRM_HA_PROCESS_START_CONFIRM_INTERVAL_IN_MSECS = INT_MIN;
static int prm_ha_process_start_confirm_interval_in_msecs_default =
  HB_DEFAULT_START_CONFIRM_INTERVAL_IN_MSECS;

int PRM_HA_PROCESS_DEREG_CONFIRM_INTERVAL_IN_MSECS = INT_MIN;
static int prm_ha_process_dereg_confirm_interval_in_msecs_default =
  HB_DEFAULT_DEREG_CONFIRM_INTERVAL_IN_MSECS;

int PRM_HA_MAX_PROCESS_START_CONFIRM = INT_MIN;
static int prm_ha_max_process_start_confirm_default =
  HB_DEFAULT_MAX_PROCESS_START_CONFIRM;

int PRM_HA_MAX_PROCESS_DEREG_CONFIRM = INT_MIN;
static int prm_ha_max_process_dereg_confirm_default =
  HB_DEFAULT_MAX_PROCESS_DEREG_CONFIRM;

int PRM_HA_CHANGEMODE_INTERVAL_IN_MSECS = INT_MIN;
static int prm_ha_changemode_interval_in_msecs_default =
  HB_DEFAULT_CHANGEMODE_INTERVAL_IN_MSECS;

int PRM_HA_MAX_HEARTBEAT_GAP = INT_MIN;
static int prm_ha_max_heartbeat_gap_default = HB_DEFAULT_MAX_HEARTBEAT_GAP;

bool PRM_JAVA_STORED_PROCEDURE = false;
static bool prm_java_stored_procedure_default = false;

bool PRM_COMPAT_PRIMARY_KEY = false;
static bool prm_compat_primary_key_default = false;

int PRM_LOG_HEADER_FLUSH_INTERVAL = INT_MIN;
static int prm_log_header_flush_interval_default = 5;
static int prm_log_header_flush_interval_lower = 1;

bool PRM_LOG_ASYNC_COMMIT = false;
static bool prm_log_async_commit_default = false;

int PRM_LOG_GROUP_COMMIT_INTERVAL_MSECS = INT_MIN;
static int prm_log_group_commit_interval_msecs_default = 0;
static int prm_log_group_commit_interval_msecs_lower = 0;

int PRM_LOG_BG_FLUSH_INTERVAL_MSECS = INT_MIN;
static int prm_log_bg_flush_interval_msecs_default = 1000;
static int prm_log_bg_flush_interval_msecs_lower = 0;

int PRM_LOG_BG_FLUSH_NUM_PAGES = INT_MIN;
static int prm_log_bg_flush_num_pages_default = 0;
static int prm_log_bg_flush_num_pages_lower = 0;

bool PRM_INTL_MBS_SUPPORT = false;
static bool prm_intl_mbs_support_default = false;

bool PRM_LOG_COMPRESS = false;
static bool prm_log_compress_default = true;

bool PRM_BLOCK_NOWHERE_STATEMENT = false;
static bool prm_block_nowhere_statement_default = false;

bool PRM_BLOCK_DDL_STATEMENT = false;
static bool prm_block_ddl_statement_default = false;

bool PRM_SINGLE_BYTE_COMPARE = false;
static bool prm_single_byte_compare_default = false;

int PRM_CSQL_HISTORY_NUM = INT_MIN;
static int prm_csql_history_num_default = 50;
static int prm_csql_history_num_upper = 200;
static int prm_csql_history_num_lower = 1;

bool PRM_LOG_TRACE_DEBUG = false;
static bool prm_log_trace_debug_default = false;

const char *PRM_DL_FORK = "";
static const char *prm_dl_fork_default = NULL;

bool PRM_ER_PRODUCTION_MODE = false;
static bool prm_er_production_mode_default = true;

int PRM_ER_STOP_ON_ERROR = INT_MIN;
static int prm_er_stop_on_error_default = 0;
static int prm_er_stop_on_error_upper = 0;

int PRM_TCP_RCVBUF_SIZE = INT_MIN;
static int prm_tcp_rcvbuf_size_default = -1;

int PRM_TCP_SNDBUF_SIZE = INT_MIN;
static int prm_tcp_sndbuf_size_default = -1;

int PRM_TCP_NODELAY = INT_MIN;
static int prm_tcp_nodelay_default = -1;

bool PRM_CSQL_SINGLE_LINE_MODE = false;
static bool prm_csql_single_line_mode_default = false;

bool PRM_XASL_DEBUG_DUMP = false;
static bool prm_xasl_debug_dump_default = false;

int PRM_LOG_MAX_ARCHIVES = INT_MIN;
static int prm_log_max_archives_default = INT_MAX;
static int prm_log_max_archives_lower = 0;

bool PRM_LOG_NO_LOGGING = false;
static bool prm_log_no_logging_default = false;

bool PRM_UNLOADDB_IGNORE_ERROR = false;
static bool prm_unloaddb_ignore_error_default = false;

int PRM_UNLOADDB_LOCK_TIMEOUT = INT_MIN;
static int prm_unloaddb_lock_timeout_default = -1;
static int prm_unloaddb_lock_timeout_lower = -1;

int PRM_LOADDB_FLUSH_INTERVAL = INT_MIN;
static int prm_loaddb_flush_interval_default = 1000;
static int prm_loaddb_flush_interval_lower = 0;

const char *PRM_IO_TEMP_VOLUME_PATH = "";
static char *prm_io_temp_volume_path_default = NULL;

const char *PRM_IO_VOLUME_EXT_PATH = "";
static char *prm_io_volume_ext_path_default = NULL;

bool PRM_UNIQUE_ERROR_KEY_VALUE = false;
static bool prm_unique_error_key_value_default = false;

bool PRM_USE_SYSTEM_MALLOC = false;
static bool prm_use_system_malloc_default = false;

const char *PRM_EVENT_HANDLER = "";
static const char *prm_event_handler_default = NULL;

bool *PRM_EVENT_ACTIVATION = error_list_intial;
static bool *prm_event_activation_default = NULL;

bool PRM_READ_ONLY_MODE = false;
static bool prm_read_only_mode_default = false;

int PRM_MNT_WAITING_THREAD = INT_MIN;
static int prm_mnt_waiting_thread_default = 0;
static int prm_mnt_waiting_thread_lower = 0;

int *PRM_MNT_STATS_THRESHOLD = int_list_initial;
static int *prm_mnt_stats_threshold_default = NULL;

const char *PRM_SERVICE_SERVICE_LIST = "";
static const char *prm_service_service_list_default = NULL;

const char *PRM_SERVICE_SERVER_LIST = "";
static const char *prm_service_server_list_default = NULL;

int PRM_MAX_CONN_POOL_SIZE = INT_MIN;
static int prm_max_conn_pool_size_default = 8;
static int prm_max_conn_pool_size_upper = 64;
static int prm_max_conn_pool_size_lower = 4;

typedef struct sysprm_param SYSPRM_PARAM;
struct sysprm_param
{
  const char *name;		/* the keyword expected */
  unsigned int flag;		/* bitmask flag representing status words */
  void *default_value;		/* address of (pointer to) default value */
  void *value;			/* address of (pointer to) current value */
  void *upper_limit;		/* highest allowable value */
  void *lower_limit;		/* lowest allowable value */
  char *force_value;		/* address of (pointer to) force value string */
};

static SYSPRM_PARAM prm_Def[] = {
  {PRM_NAME_ER_LOG_DEBUG,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER |
    PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_er_log_debug_default,
   (void *) &PRM_ER_LOG_DEBUG,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_ER_LOG_LEVEL,
   (PRM_KEYWORD | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_er_log_level_default,
   (void *) &PRM_ER_LOG_LEVEL,
   (void *) &prm_er_log_level_upper, (void *) &prm_er_log_level_lower,
   (char *) NULL},
  {PRM_NAME_ER_LOG_WARNING,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_er_log_warning_default,
   (void *) &PRM_ER_LOG_WARNING,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_ER_EXIT_ASK,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER |
    PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_er_exit_ask_default,
   (void *) &PRM_ER_EXIT_ASK,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_ER_LOG_SIZE,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_er_log_size_default,
   (void *) &PRM_ER_LOG_SIZE,
   (void *) NULL, (void *) &prm_er_log_size_lower,
   (char *) NULL},
  {PRM_NAME_ER_LOG_FILE,
   (PRM_REQUIRED | PRM_STRING | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_FOR_SERVER),
   (void *) &prm_er_log_file_default,
   (void *) &PRM_ER_LOG_FILE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_IO_LOCKF_ENABLE,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_io_lockf_enable_default,
   (void *) &PRM_IO_LOCKF_ENABLE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_SR_NBUFFERS,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_sr_nbuffers_default,
   (void *) &PRM_SR_NBUFFERS,
   (void *) NULL, (void *) &prm_sr_nbuffers_lower,
   (char *) NULL},
  {PRM_NAME_PB_NBUFFERS,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_pb_nbuffers_default,
   (void *) &PRM_PB_NBUFFERS,
   (void *) NULL, (void *) &prm_pb_nbuffers_lower,
   (char *) NULL},
  {PRM_NAME_HF_UNFILL_FACTOR,
   (PRM_REQUIRED | PRM_FLOAT | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_hf_unfill_factor_default,
   (void *) &PRM_HF_UNFILL_FACTOR,
   (void *) &prm_hf_unfill_factor_upper, (void *) &prm_hf_unfill_factor_lower,
   (char *) NULL},
  {PRM_NAME_BT_UNFILL_FACTOR,
   (PRM_REQUIRED | PRM_FLOAT | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_bt_unfill_factor_default,
   (void *) &PRM_BT_UNFILL_FACTOR,
   (void *) &prm_bt_unfill_factor_upper, (void *) &prm_bt_unfill_factor_lower,
   (char *) NULL},
  {PRM_NAME_BT_OID_NBUFFERS,
   (PRM_REQUIRED | PRM_FLOAT | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_bt_oid_nbuffers_default,
   (void *) &PRM_BT_OID_NBUFFERS,
   (void *) &prm_bt_oid_nbuffers_upper, (void *) &prm_bt_oid_nbuffers_lower,
   (char *) NULL},
  {PRM_NAME_BT_INDEX_SCAN_OID_ORDER,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE),
   (void *) &prm_bt_index_scan_oid_order_default,
   (void *) &PRM_BT_INDEX_SCAN_OID_ORDER,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_BOSR_MAXTMP_PAGES,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_bosr_maxtmp_pages,
   (void *) &PRM_BOSR_MAXTMP_PAGES,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_LK_TIMEOUT_MESSAGE_DUMP_LEVEL,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_lk_timeout_message_dump_level_default,
   (void *) &PRM_LK_TIMEOUT_MESSAGE_DUMP_LEVEL,
   (void *) &prm_lk_timeout_message_dump_level_upper,
   (void *) &prm_lk_timeout_message_dump_level_lower,
   (char *) NULL},
  {PRM_NAME_LK_ESCALATION_AT,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_lk_escalation_at_default,
   (void *) &PRM_LK_ESCALATION_AT,
   (void *) NULL, (void *) &prm_lk_escalation_at_lower,
   (char *) NULL},
  {PRM_NAME_LK_TIMEOUT_SECS,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE),
   (void *) &prm_lk_timeout_secs_default,
   (void *) &PRM_LK_TIMEOUT_SECS,
   (void *) NULL, (void *) &prm_lk_timeout_secs_lower,
   (char *) NULL},
  {PRM_NAME_LK_RUN_DEADLOCK_INTERVAL,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_lk_run_deadlock_interval_default,
   (void *) &PRM_LK_RUN_DEADLOCK_INTERVAL,
   (void *) NULL, (void *) &prm_lk_run_deadlock_interval_lower,
   (char *) NULL},
  {PRM_NAME_LOG_NBUFFERS,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_log_nbuffers_default,
   (void *) &PRM_LOG_NBUFFERS,
   (void *) NULL, (void *) &prm_log_nbuffers_lower,
   (char *) NULL},
  {PRM_NAME_LOG_CHECKPOINT_NPAGES,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_log_checkpoint_npages_default,
   (void *) &PRM_LOG_CHECKPOINT_NPAGES,
   (void *) NULL, (void *) &prm_log_checkpoint_npages_lower,
   (char *) NULL},
  {PRM_NAME_LOG_CHECKPOINT_INTERVAL_MINUTES,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_log_checkpoint_interval_minutes_default,
   (void *) &PRM_LOG_CHECKPOINT_INTERVAL_MINUTES,
   (void *) NULL, (void *) &prm_log_checkpoint_interval_minutes_lower,
   (char *) NULL},
  {PRM_NAME_LOG_BACKGROUND_ARCHIVING,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_log_background_archiving_default,
   (void *) &PRM_LOG_BACKGROUND_ARCHIVING,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_LOG_ISOLATION_LEVEL,
   (PRM_REQUIRED | PRM_KEYWORD | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE),
   (void *) &prm_log_isolation_level_default,
   (void *) &PRM_LOG_ISOLATION_LEVEL,
   (void *) &prm_log_isolation_level_upper,
   (void *) &prm_log_isolation_level_lower,
   (char *) NULL},
  {PRM_NAME_LOG_MEDIA_FAILURE_SUPPORT,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_log_media_failure_support_default,
   (void *) &PRM_LOG_MEDIA_FAILURE_SUPPORT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_COMMIT_ON_SHUTDOWN,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE),
   (void *) &prm_commit_on_shutdown_default,
   (void *) &PRM_COMMIT_ON_SHUTDOWN,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_CSQL_AUTO_COMMIT,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE),
   (void *) &prm_csql_auto_commit_default,
   (void *) &PRM_CSQL_AUTO_COMMIT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_LOG_SWEEP_CLEAN,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_log_sweep_clean_default,
   (void *) &PRM_LOG_SWEEP_CLEAN,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_WS_HASHTABLE_SIZE,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_ws_hashtable_size_default,
   (void *) &PRM_WS_HASHTABLE_SIZE,
   (void *) NULL, (void *) &prm_ws_hashtable_size_lower,
   (char *) NULL},
  {PRM_NAME_WS_MEMORY_REPORT,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_ws_memory_report_default,
   (void *) &PRM_WS_MEMORY_REPORT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_GC_ENABLE,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE),
   (void *) &prm_gc_enable_default,
   (void *) &PRM_GC_ENABLE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_TCP_PORT_ID,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_FOR_SERVER),
   (void *) &prm_tcp_port_id_default,
   (void *) &PRM_TCP_PORT_ID,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_TCP_CONNECTION_TIMEOUT,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE),
   (void *) &prm_tcp_connection_timeout_default,
   (void *) &PRM_TCP_CONNECTION_TIMEOUT,
   (void *) NULL, (void *) &prm_tcp_connection_timeout_lower,
   (char *) NULL},
  {PRM_NAME_OPTIMIZATION_LEVEL,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE),
   (void *) &prm_optimization_level_default,
   (void *) &PRM_OPTIMIZATION_LEVEL,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_QO_DUMP,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_qo_dump_default,
   (void *) &PRM_QO_DUMP,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_CSS_MAX_CLIENTS,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_css_max_clients_default,
   (void *) &PRM_CSS_MAX_CLIENTS,
   (void *) NULL, (void *) &prm_css_max_clients_lower,
   (char *) NULL},
  {PRM_NAME_THREAD_STACKSIZE,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_thread_stacksize_default,
   (void *) &PRM_THREAD_STACKSIZE,
   (void *) NULL, (void *) &prm_thread_stacksize_lower,
   (char *) NULL},
  {PRM_NAME_CFG_DB_HOSTS,
   (PRM_REQUIRED | PRM_STRING | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE),
   (void *) &prm_cfg_db_hosts_default,
   (void *) &PRM_CFG_DB_HOSTS,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_RESET_TR_PARSER,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_reset_tr_parser_default,
   (void *) &PRM_RESET_TR_PARSER,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_IO_BACKUP_NBUFFERS,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_io_backup_nbuffers_default,
   (void *) &PRM_IO_BACKUP_NBUFFERS,
   (void *) NULL, (void *) &prm_io_backup_nbuffers_lower,
   (char *) NULL},
  {PRM_NAME_IO_BACKUP_MAX_VOLUME_SIZE,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_io_backup_max_volume_size_default,
   (void *) &PRM_IO_BACKUP_MAX_VOLUME_SIZE,
   (void *) NULL, (void *) &prm_io_backup_max_volume_size_lower,
   (char *) NULL},
  {PRM_NAME_MAX_PAGES_IN_TEMP_FILE_CACHE,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_max_pages_in_temp_file_cache_default,
   (void *) &PRM_MAX_PAGES_IN_TEMP_FILE_CACHE,
   (void *) NULL, (void *) &prm_max_pages_in_temp_file_cache_lower,
   (char *) NULL},
  {PRM_NAME_MAX_ENTRIES_IN_TEMP_FILE_CACHE,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_max_entries_in_temp_file_cache_default,
   (void *) &PRM_MAX_ENTRIES_IN_TEMP_FILE_CACHE,
   (void *) NULL, (void *) &prm_max_entries_in_temp_file_cache_lower,
   (char *) NULL},
  {PRM_NAME_PTHREAD_SCOPE_PROCESS,	/* AIX only */
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_pthread_scope_process_default,
   (void *) &PRM_PTHREAD_SCOPE_PROCESS,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_TEMP_MEM_BUFFER_PAGES,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_temp_mem_buffer_pages_default,
   (void *) &PRM_TEMP_MEM_BUFFER_PAGES,
   (void *) &prm_temp_mem_buffer_pages_upper,
   (void *) &prm_temp_mem_buffer_pages_lower,
   (char *) NULL},
  {PRM_NAME_DONT_REUSE_HEAP_FILE,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER |
    PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_dont_reuse_heap_file_default,
   (void *) &PRM_DONT_REUSE_HEAP_FILE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_QUERY_MODE_SYNC,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_query_mode_sync_default,
   (void *) &PRM_QUERY_MODE_SYNC,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_INSERT_MODE,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_insert_mode_default,
   (void *) &PRM_INSERT_MODE,
   (void *) &prm_insert_mode_upper,
   (void *) &prm_insert_mode_lower,
   (char *) NULL},
  {PRM_NAME_LK_MAX_SCANID_BIT,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_lk_max_scanid_bit_default,
   (void *) &PRM_LK_MAX_SCANID_BIT,
   (void *) &prm_lk_max_scanid_bit_upper,
   (void *) &prm_lk_max_scanid_bit_lower,
   (char *) NULL},
  {PRM_NAME_HOSTVAR_LATE_BINDING,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_hostvar_late_binding_default,
   (void *) &PRM_HOSTVAR_LATE_BINDING,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_ENABLE_HISTO,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE),
   (void *) &prm_enable_histo_default,
   (void *) &PRM_ENABLE_HISTO,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_MUTEX_BUSY_WAITING_CNT,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_mutex_busy_waiting_cnt_default,
   (void *) &PRM_MUTEX_BUSY_WAITING_CNT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_PB_NUM_LRU_CHAINS,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_pb_num_LRU_chains_default,
   (void *) &PRM_PB_NUM_LRU_CHAINS,
   (void *) &prm_pb_num_LRU_chains_upper,
   (void *) &prm_pb_num_LRU_chains_lower,
   (char *) NULL},
  {PRM_NAME_PAGE_BG_FLUSH_INTERVAL_MSECS,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE),
   (void *) &prm_page_bg_flush_interval_msec_default,
   (void *) &PRM_PAGE_BG_FLUSH_INTERVAL_MSEC,
   (void *) NULL, (void *) &prm_page_bg_flush_interval_msec_lower,
   (char *) NULL},
  {PRM_NAME_ADAPTIVE_FLUSH_CONTROL,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE),
   (void *) &prm_adaptive_flush_control_default,
   (void *) &PRM_ADAPTIVE_FLUSH_CONTROL,
   (void *) NULL,
   (void *) NULL,
   (char *) NULL},
  {PRM_NAME_MAX_FLUSH_PAGES_PER_SECOND,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE),
   (void *) &prm_max_flush_pages_per_second_default,
   (void *) &PRM_MAX_FLUSH_PAGES_PER_SECOND,
   (void *) &prm_max_flush_pages_per_second_upper,
   (void *) &prm_max_flush_pages_per_second_lower,
   (char *) NULL},
  {PRM_NAME_PB_SYNC_ON_NFLUSH,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE),
   (void *) &prm_pb_sync_on_nflush_default,
   (void *) &PRM_PB_SYNC_ON_NFLUSH,
   (void *) &prm_pb_sync_on_nflush_upper,
   (void *) &prm_pb_sync_on_nflush_lower,
   (char *) NULL},
  {PRM_NAME_ORACLE_STYLE_OUTERJOIN,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_oracle_style_outerjoin_default,
   (void *) &PRM_ORACLE_STYLE_OUTERJOIN,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_ANSI_QUOTES,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_TEST_CHANGE),
   (void *) &prm_ansi_quotes_default,
   (void *) &PRM_ANSI_QUOTES,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_TEST_MODE,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER
    | PRM_HIDDEN),
   (void *) &prm_test_mode_default,
   (void *) &PRM_TEST_MODE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_ONLY_FULL_GROUP_BY,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE | PRM_TEST_CHANGE),
   (void *) &prm_only_full_group_by_default,
   (void *) &PRM_ONLY_FULL_GROUP_BY,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_PIPES_AS_CONCAT,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT |
    PRM_FOR_CLIENT | PRM_TEST_CHANGE),
   (void *) &prm_pipes_as_concat_default,
   (void *) &PRM_PIPES_AS_CONCAT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_MYSQL_TRIGGER_CORRELATION_NAMES,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT |
    PRM_FOR_CLIENT | PRM_TEST_CHANGE),
   (void *) &prm_mysql_trigger_correlation_names_default,
   (void *) &PRM_MYSQL_TRIGGER_CORRELATION_NAMES,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_COMPACTDB_PAGE_RECLAIM_ONLY,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT),
   (void *) &prm_compactdb_page_reclaim_only_default,
   (void *) &PRM_COMPACTDB_PAGE_RECLAIM_ONLY,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_LIKE_TERM_SELECTIVITY,
   (PRM_FLOAT | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_like_term_selectivity_default,
   (void *) &PRM_LIKE_TERM_SELECTIVITY,
   (void *) &prm_like_term_selectivity_upper,
   (void *) &prm_like_term_selectivity_lower,
   (char *) NULL},
  {PRM_NAME_MAX_OUTER_CARD_OF_IDXJOIN,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_max_outer_card_of_idxjoin_default,
   (void *) &PRM_MAX_OUTER_CARD_OF_IDXJOIN,
   (void *) NULL,
   (void *) &prm_max_outer_card_of_idxjoin_lower,
   (char *) NULL},
  {PRM_NAME_ORACLE_STYLE_EMPTY_STRING,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_FOR_SERVER),
   (void *) &prm_oracle_style_empty_string_default,
   (void *) &PRM_ORACLE_STYLE_EMPTY_STRING,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_SUPPRESS_FSYNC,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_suppress_fsync_default,
   (void *) &PRM_SUPPRESS_FSYNC,
   (void *) &prm_suppress_fsync_upper,
   (void *) &prm_suppress_fsync_lower,
   (char *) NULL},
  {PRM_NAME_CALL_STACK_DUMP_ON_ERROR,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_call_stack_dump_on_error_default,
   (void *) &PRM_CALL_STACK_DUMP_ON_ERROR,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_CALL_STACK_DUMP_ACTIVATION,
   (PRM_ERROR_LIST | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER),
   (void *) &prm_call_stack_dump_activation_default,
   (void *) &PRM_CALL_STACK_DUMP_ACTIVATION,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_CALL_STACK_DUMP_DEACTIVATION,
   (PRM_ERROR_LIST | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER),
   (void *) &prm_call_stack_dump_deactivation_default,
   (void *) &PRM_CALL_STACK_DUMP_DEACTIVATION,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_COMPAT_NUMERIC_DIVISION_SCALE,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_FOR_SERVER | PRM_USER_CHANGE),
   (void *) &prm_compat_numeric_division_scale_default,
   (void *) &PRM_COMPAT_NUMERIC_DIVISION_SCALE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_DBFILES_PROTECT,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_dbfiles_protect_default,
   (void *) &PRM_DBFILES_PROTECT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_AUTO_RESTART_SERVER,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_auto_restart_server_default,
   (void *) &PRM_AUTO_RESTART_SERVER,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_XASL_MAX_PLAN_CACHE_ENTRIES,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_FOR_SERVER),
   (void *) &prm_xasl_max_plan_cache_entries_default,
   (void *) &PRM_XASL_MAX_PLAN_CACHE_ENTRIES,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
#if defined (ENABLE_UNUSED_FUNCTION)
  {PRM_NAME_XASL_MAX_PLAN_CACHE_CLONES,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_xasl_max_plan_cache_clones_default,
   (void *) &PRM_XASL_MAX_PLAN_CACHE_CLONES,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
#endif /* ENABLE_UNUSED_FUNCTION */
  {PRM_NAME_XASL_PLAN_CACHE_TIMEOUT,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_xasl_plan_cache_timeout_default,
   (void *) &PRM_XASL_PLAN_CACHE_TIMEOUT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_LIST_QUERY_CACHE_MODE,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_list_query_cache_mode_default,
   (void *) &PRM_LIST_QUERY_CACHE_MODE,
   (void *) &prm_list_query_cache_mode_upper,
   (void *) &prm_list_query_cache_mode_lower,
   (char *) NULL},
  {PRM_NAME_LIST_MAX_QUERY_CACHE_ENTRIES,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_list_max_query_cache_entries_default,
   (void *) &PRM_LIST_MAX_QUERY_CACHE_ENTRIES,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_LIST_MAX_QUERY_CACHE_PAGES,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_list_max_query_cache_pages_default,
   (void *) &PRM_LIST_MAX_QUERY_CACHE_PAGES,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_REPLICATION_MODE,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_replication_mode_default,
   (void *) &PRM_REPLICATION_MODE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_MODE,
   (PRM_KEYWORD | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_ha_mode_default,
   (void *) &PRM_HA_MODE,
   (void *) &prm_ha_mode_upper,
   (void *) &prm_ha_mode_lower,
   (char *) NULL},
  {PRM_NAME_HA_SERVER_STATE,
   (PRM_KEYWORD | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_ha_server_state_default,
   (void *) &PRM_HA_SERVER_STATE,
   (void *) &prm_ha_server_state_upper,
   (void *) &prm_ha_server_state_lower,
   (char *) NULL},
  {PRM_NAME_HA_LOG_APPLIER_STATE,
   (PRM_KEYWORD | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_ha_log_applier_state_default,
   (void *) &PRM_HA_LOG_APPLIER_STATE,
   (void *) &prm_ha_log_applier_state_upper,
   (void *) &prm_ha_log_applier_state_lower,
   (char *) NULL},
  {PRM_NAME_HA_NODE_LIST,
   (PRM_STRING | PRM_DEFAULT | PRM_FOR_SERVER | PRM_FOR_CLIENT |
    PRM_USER_CHANGE | PRM_RELOADABLE),
   (void *) &prm_ha_node_list_default,
   (void *) &PRM_HA_NODE_LIST,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_PORT_ID,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT),
   (void *) &prm_ha_port_id_default,
   (void *) &PRM_HA_PORT_ID,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_INIT_TIMER_IN_MSECS,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_ha_init_timer_im_msecs_default,
   (void *) &PRM_HA_INIT_TIMER_IN_MSECS,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_HEARTBEAT_INTERVAL_IN_MSECS,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_ha_heartbeat_interval_in_msecs_default,
   (void *) &PRM_HA_HEARTBEAT_INTERVAL_IN_MSECS,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_CALC_SCORE_INTERVAL_IN_MSECS,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_ha_calc_score_interval_in_msecs_default,
   (void *) &PRM_HA_CALC_SCORE_INTERVAL_IN_MSECS,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_FAILOVER_WAIT_TIME_IN_MSECS,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_ha_failover_wait_time_in_msecs_default,
   (void *) &PRM_HA_FAILOVER_WAIT_TIME_IN_MSECS,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_PROCESS_START_CONFIRM_INTERVAL_IN_MSECS,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_ha_process_start_confirm_interval_in_msecs_default,
   (void *) &PRM_HA_PROCESS_START_CONFIRM_INTERVAL_IN_MSECS,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_PROCESS_DEREG_CONFIRM_INTERVAL_IN_MSECS,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_ha_process_dereg_confirm_interval_in_msecs_default,
   (void *) &PRM_HA_PROCESS_DEREG_CONFIRM_INTERVAL_IN_MSECS,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_MAX_PROCESS_START_CONFIRM,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_ha_max_process_start_confirm_default,
   (void *) &PRM_HA_MAX_PROCESS_START_CONFIRM,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_MAX_PROCESS_DEREG_CONFIRM,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_ha_max_process_dereg_confirm_default,
   (void *) &PRM_HA_MAX_PROCESS_DEREG_CONFIRM,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_CHANGEMODE_INTERVAL_IN_MSEC,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT),
   (void *) &prm_ha_changemode_interval_in_msecs_default,
   (void *) &PRM_HA_CHANGEMODE_INTERVAL_IN_MSECS,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_HA_MAX_HEARTBEAT_GAP,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_ha_max_heartbeat_gap_default,
   (void *) &PRM_HA_MAX_HEARTBEAT_GAP,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_JAVA_STORED_PROCEDURE,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_java_stored_procedure_default,
   (void *) &PRM_JAVA_STORED_PROCEDURE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_COMPAT_PRIMARY_KEY,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE),
   (void *) &prm_compat_primary_key_default,
   (void *) &PRM_COMPAT_PRIMARY_KEY,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_LOG_HEADER_FLUSH_INTERVAL,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_log_header_flush_interval_default,
   (void *) &PRM_LOG_HEADER_FLUSH_INTERVAL,
   (void *) NULL, (void *) &prm_log_header_flush_interval_lower,
   (char *) NULL},
  {PRM_NAME_LOG_ASYNC_COMMIT,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_log_async_commit_default,
   (void *) &PRM_LOG_ASYNC_COMMIT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_LOG_GROUP_COMMIT_INTERVAL_MSECS,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER |
    PRM_USER_CHANGE),
   (void *) &prm_log_group_commit_interval_msecs_default,
   (void *) &PRM_LOG_GROUP_COMMIT_INTERVAL_MSECS,
   (void *) NULL, (void *) &prm_log_group_commit_interval_msecs_lower,
   (char *) NULL},
  {PRM_NAME_LOG_BG_FLUSH_INTERVAL_MSECS,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_log_bg_flush_interval_msecs_default,
   (void *) &PRM_LOG_BG_FLUSH_INTERVAL_MSECS,
   (void *) NULL, (void *) &prm_log_bg_flush_interval_msecs_lower,
   (char *) NULL},
  {PRM_NAME_LOG_BG_FLUSH_NUM_PAGES,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_log_bg_flush_num_pages_default,
   (void *) &PRM_LOG_BG_FLUSH_NUM_PAGES,
   (void *) NULL, (void *) &prm_log_bg_flush_num_pages_lower,
   (char *) NULL},
  {PRM_NAME_INTL_MBS_SUPPORT,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT),
   (void *) &prm_intl_mbs_support_default,
   (void *) &PRM_INTL_MBS_SUPPORT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_LOG_COMPRESS,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE),
   (void *) &prm_log_compress_default,
   (void *) &PRM_LOG_COMPRESS,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_BLOCK_NOWHERE_STATEMENT,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE),
   (void *) &prm_block_nowhere_statement_default,
   (void *) &PRM_BLOCK_NOWHERE_STATEMENT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_BLOCK_DDL_STATEMENT,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE),
   (void *) &prm_block_ddl_statement_default,
   (void *) &PRM_BLOCK_DDL_STATEMENT,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_SINGLE_BYTE_COMPARE,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_FOR_SERVER),
   (void *) &prm_single_byte_compare_default,
   (void *) &PRM_SINGLE_BYTE_COMPARE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_CSQL_HISTORY_NUM,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE),
   (void *) &prm_csql_history_num_default,
   (void *) &PRM_CSQL_HISTORY_NUM,
   (void *) &prm_csql_history_num_upper,
   (void *) &prm_csql_history_num_lower,
   (char *) NULL},
  {PRM_NAME_LOG_TRACE_DEBUG,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_log_trace_debug_default,
   (void *) &PRM_LOG_TRACE_DEBUG,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_DL_FORK,
   (PRM_STRING | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_dl_fork_default,
   (void *) &PRM_DL_FORK,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_ER_PRODUCTION_MODE,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER |
    PRM_FOR_CLIENT | PRM_USER_CHANGE),
   (void *) &prm_er_production_mode_default,
   (void *) &PRM_ER_PRODUCTION_MODE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_ER_STOP_ON_ERROR,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_FOR_CLIENT |
    PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_er_stop_on_error_default,
   (void *) &PRM_ER_STOP_ON_ERROR,
   (void *) &prm_er_stop_on_error_upper, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_TCP_RCVBUF_SIZE,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_tcp_rcvbuf_size_default,
   (void *) &PRM_TCP_RCVBUF_SIZE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_TCP_SNDBUF_SIZE,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_tcp_sndbuf_size_default,
   (void *) &PRM_TCP_SNDBUF_SIZE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_TCP_NODELAY,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_HIDDEN),
   (void *) &prm_tcp_nodelay_default,
   (void *) &PRM_TCP_NODELAY,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_CSQL_SINGLE_LINE_MODE,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT |
    PRM_USER_CHANGE),
   (void *) &prm_csql_single_line_mode_default,
   (void *) &PRM_CSQL_SINGLE_LINE_MODE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_XASL_DEBUG_DUMP,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_xasl_debug_dump_default,
   (void *) &PRM_XASL_DEBUG_DUMP,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_LOG_MAX_ARCHIVES,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_log_max_archives_default,
   (void *) &PRM_LOG_MAX_ARCHIVES,
   (void *) NULL, (void *) &prm_log_max_archives_lower,
   (char *) NULL},
  {PRM_NAME_LOG_NO_LOGGING,
   (PRM_REQUIRED | PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER |
    PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_log_no_logging_default,
   (void *) &PRM_LOG_NO_LOGGING,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_UNLOADDB_IGNORE_ERROR,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_unloaddb_ignore_error_default,
   (void *) &PRM_UNLOADDB_IGNORE_ERROR,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_UNLOADDB_LOCK_TIMEOUT,
   (PRM_INTEGER | PRM_DEFAULT | PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_unloaddb_lock_timeout_default,
   (void *) &PRM_UNLOADDB_LOCK_TIMEOUT,
   (void *) NULL, (void *) &prm_unloaddb_lock_timeout_lower,
   (char *) NULL},
  {PRM_NAME_LOADDB_FLUSH_INTERVAL,
   (PRM_INTEGER | PRM_DEFAULT | PRM_USER_CHANGE | PRM_HIDDEN),
   (void *) &prm_loaddb_flush_interval_default,
   (void *) &PRM_LOADDB_FLUSH_INTERVAL,
   (void *) NULL, (void *) &prm_loaddb_flush_interval_lower,
   (char *) NULL},
  {PRM_NAME_IO_TEMP_VOLUME_PATH,
   (PRM_REQUIRED | PRM_STRING | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_io_temp_volume_path_default,
   (void *) &PRM_IO_TEMP_VOLUME_PATH,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_IO_VOLUME_EXT_PATH,
   (PRM_REQUIRED | PRM_STRING | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_io_volume_ext_path_default,
   (void *) &PRM_IO_VOLUME_EXT_PATH,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_UNIQUE_ERROR_KEY_VALUE,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_unique_error_key_value_default,
   (void *) &PRM_UNIQUE_ERROR_KEY_VALUE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_USE_SYSTEM_MALLOC,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_use_system_malloc_default,
   (void *) &PRM_USE_SYSTEM_MALLOC,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_EVENT_HANDLER,
   (PRM_STRING | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER),
   (void *) &prm_event_handler_default,
   (void *) &PRM_EVENT_HANDLER,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_EVENT_ACTIVATION,
   (PRM_ERROR_LIST | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER),
   (void *) &prm_event_activation_default,
   (void *) &PRM_EVENT_ACTIVATION,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_READ_ONLY_MODE,
   (PRM_BOOLEAN | PRM_DEFAULT | PRM_FOR_SERVER | PRM_FOR_CLIENT),
   (void *) &prm_read_only_mode_default,
   (void *) &PRM_READ_ONLY_MODE,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_MNT_WAITING_THREAD,
   (PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER | PRM_USER_CHANGE |
    PRM_HIDDEN),
   (void *) &prm_mnt_waiting_thread_default,
   (void *) &PRM_MNT_WAITING_THREAD,
   (void *) NULL, (void *) &prm_mnt_waiting_thread_lower,
   (char *) NULL},
  {PRM_NAME_MNT_STATS_THRESHOLD,
   (PRM_INTEGER_LIST | PRM_DEFAULT | PRM_FOR_SERVER | PRM_HIDDEN),
   (void *) &prm_mnt_stats_threshold_default,
   (void *) &PRM_MNT_STATS_THRESHOLD,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_SERVICE_SERVICE_LIST,
   (PRM_STRING | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_service_service_list_default,
   (void *) &PRM_SERVICE_SERVICE_LIST,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_SERVICE_SERVER_LIST,
   (PRM_STRING | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_service_server_list_default,
   (void *) &PRM_SERVICE_SERVER_LIST,
   (void *) NULL, (void *) NULL,
   (char *) NULL},
  {PRM_NAME_MAX_CONN_POOL_SIZE,
   (PRM_REQUIRED | PRM_INTEGER | PRM_DEFAULT | PRM_FOR_SERVER),
   (void *) &prm_max_conn_pool_size_default,
   (void *) &PRM_MAX_CONN_POOL_SIZE,
   (void *) &prm_max_conn_pool_size_upper,
   (void *) &prm_max_conn_pool_size_lower,
   (char *) NULL},
  /* All the compound parameters *must* be at the end of the array so that the
     changes they cause are not overridden by other parameters (for example in
     sysprm_load_and_init the parameters are set to their default in the order
     they are found in this array). */
  {PRM_NAME_COMPAT_MODE,
   (PRM_REQUIRED | PRM_KEYWORD | PRM_DEFAULT | PRM_FOR_CLIENT | PRM_FOR_SERVER
    | PRM_TEST_CHANGE | PRM_COMPOUND),
   (void *) &prm_compat_mode_default,
   (void *) &PRM_COMPAT_MODE,
   (void *) &prm_compat_mode_upper, (void *) &prm_compat_mode_lower,
   (char *) NULL}
};

#define NUM_PRM ((int)(sizeof(prm_Def)/sizeof(prm_Def[0])))


/*
 * Keyword searches do a intl_mbs_ncasecmp(), using the LENGTH OF THE TABLE KEY
 * as the limit, so make sure that overlapping keywords are ordered
 * correctly.  For example, make sure that "yes" precedes "y".
 */

typedef struct keyval KEYVAL;
struct keyval
{
  const char *key;
  int val;
};

static KEYVAL boolean_words[] = {
  {"yes", 1},
  {"y", 1},
  {"1", 1},
  {"true", 1},
  {"on", 1},
  {"no", 0},
  {"n", 0},
  {"0", 0},
  {"false", 0},
  {"off", 0}
};

static KEYVAL er_log_level_words[] = {
  {"fatal", ER_FATAL_ERROR_SEVERITY},
  {"error", ER_ERROR_SEVERITY},
  {"syntax", ER_SYNTAX_ERROR_SEVERITY},
  {"warning", ER_WARNING_SEVERITY},
  {"notification", ER_NOTIFICATION_SEVERITY}
};

static KEYVAL isolation_level_words[] = {
  {"tran_serializable", TRAN_SERIALIZABLE},
  {"tran_no_phantom_read", TRAN_SERIALIZABLE},

  {"tran_rep_class_rep_instance", TRAN_REP_CLASS_REP_INSTANCE},
  {"tran_rep_read", TRAN_REP_CLASS_REP_INSTANCE},
  {"tran_rep_class_commit_instance", TRAN_REP_CLASS_COMMIT_INSTANCE},
  {"tran_read_committed", TRAN_REP_CLASS_COMMIT_INSTANCE},
  {"tran_cursor_stability", TRAN_REP_CLASS_COMMIT_INSTANCE},
  {"tran_rep_class_uncommit_instance", TRAN_REP_CLASS_UNCOMMIT_INSTANCE},
  /*
   * This silly spelling has to hang around because it was in there
   * once upon a time and users may have come to depend on it.
   */
  {"tran_read_uncommited", TRAN_REP_CLASS_UNCOMMIT_INSTANCE},
  {"tran_read_uncommitted", TRAN_REP_CLASS_UNCOMMIT_INSTANCE},
  {"tran_commit_class_commit_instance", TRAN_COMMIT_CLASS_COMMIT_INSTANCE},
  {"tran_commit_class_uncommit_instance",
   TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE},

  /*
   * Why be so fascict about the "tran_" prefix?  Are we afraid someone
   * is going to use these gonzo words?
   */
  {"serializable", TRAN_SERIALIZABLE},
  {"no_phantom_read", TRAN_SERIALIZABLE},

  {"rep_class_rep_instance", TRAN_REP_CLASS_REP_INSTANCE},
  {"rep_read", TRAN_REP_CLASS_REP_INSTANCE},
  {"rep_class_commit_instance", TRAN_REP_CLASS_COMMIT_INSTANCE},
  {"read_committed", TRAN_REP_CLASS_COMMIT_INSTANCE},
  {"cursor_stability", TRAN_REP_CLASS_COMMIT_INSTANCE},
  {"rep_class_uncommit_instance", TRAN_REP_CLASS_UNCOMMIT_INSTANCE},
  {"read_uncommited", TRAN_REP_CLASS_UNCOMMIT_INSTANCE},
  {"commit_class_commit_instance", TRAN_COMMIT_CLASS_COMMIT_INSTANCE},
  {"commit_class_uncommit_instance", TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE}
};

static KEYVAL null_words[] = {
  {"null", 0},
  {"0", 0}
};

static KEYVAL ha_mode_words[] = {
  {HA_MODE_OFF_STR, HA_MODE_OFF},
  {"no", HA_MODE_OFF},
  {"n", HA_MODE_OFF},
  {"0", HA_MODE_OFF},
  {"false", HA_MODE_OFF},
  {"off", HA_MODE_OFF},
  {"yes", HA_MODE_FAIL_BACK},
  {"y", HA_MODE_FAIL_BACK},
  {"1", HA_MODE_FAIL_BACK},
  {"true", HA_MODE_FAIL_BACK},
  {"on", HA_MODE_FAIL_BACK},
  /*{HA_MODE_FAIL_OVER_STR, HA_MODE_FAIL_OVER}, *//* unused */
  {HA_MODE_FAIL_BACK_STR, HA_MODE_FAIL_BACK},
  /*{HA_MODE_LAZY_BACK_STR, HA_MODE_LAZY_BACK}, *//* not implemented yet */
  {HA_MODE_ROLE_CHANGE_STR, HA_MODE_ROLE_CHANGE}
};

static KEYVAL ha_server_state_words[] = {
  {HA_SERVER_STATE_IDLE_STR, HA_SERVER_STATE_IDLE},
  {HA_SERVER_STATE_ACTIVE_STR, HA_SERVER_STATE_ACTIVE},
  {HA_SERVER_STATE_TO_BE_ACTIVE_STR, HA_SERVER_STATE_TO_BE_ACTIVE},
  {HA_SERVER_STATE_STANDBY_STR, HA_SERVER_STATE_STANDBY},
  {HA_SERVER_STATE_TO_BE_STANDBY_STR, HA_SERVER_STATE_TO_BE_STANDBY},
  {HA_SERVER_STATE_MAINTENANCE_STR, HA_SERVER_STATE_MAINTENANCE},
  {HA_SERVER_STATE_DEAD_STR, HA_SERVER_STATE_DEAD}
};

static KEYVAL ha_log_applier_state_words[] = {
  {HA_LOG_APPLIER_STATE_UNREGISTERED_STR, HA_LOG_APPLIER_STATE_UNREGISTERED},
  {HA_LOG_APPLIER_STATE_RECOVERING_STR, HA_LOG_APPLIER_STATE_RECOVERING},
  {HA_LOG_APPLIER_STATE_WORKING_STR, HA_LOG_APPLIER_STATE_WORKING},
  {HA_LOG_APPLIER_STATE_DONE_STR, HA_LOG_APPLIER_STATE_DONE},
  {HA_LOG_APPLIER_STATE_ERROR_STR, HA_LOG_APPLIER_STATE_ERROR}
};


static KEYVAL compat_words[] = {
  {"cubrid", COMPAT_CUBRID},
  {"default", COMPAT_CUBRID},
  {"mysql", COMPAT_MYSQL},
  {"oracle", COMPAT_ORACLE}
};

static const char *compat_mode_values_PRM_ANSI_QUOTES[COMPAT_ORACLE + 2] = {
  NULL,				/* COMPAT_CUBRID     */
  "no",				/* COMPAT_MYSQL      */
  NULL,				/* COMPAT_ORACLE     */
  PRM_NAME_ANSI_QUOTES
};

static const char
  *compat_mode_values_PRM_ORACLE_STYLE_EMPTY_STRING[COMPAT_ORACLE + 2] = {
  NULL,				/* COMPAT_CUBRID     */
  NULL,				/* COMPAT_MYSQL      */
  "yes",			/* COMPAT_ORACLE     */
  PRM_NAME_ORACLE_STYLE_EMPTY_STRING
};

static const char
  *compat_mode_values_PRM_ORACLE_STYLE_OUTERJOIN[COMPAT_ORACLE + 2] = {
  NULL,				/* COMPAT_CUBRID     */
  NULL,				/* COMPAT_MYSQL      */
  "yes",			/* COMPAT_ORACLE     */
  PRM_NAME_ORACLE_STYLE_OUTERJOIN
};

static const char *compat_mode_values_PRM_PIPES_AS_CONCAT[COMPAT_ORACLE + 2] = {
  NULL,				/* COMPAT_CUBRID     */
  "no",				/* COMPAT_MYSQL      */
  NULL,				/* COMPAT_ORACLE     */
  PRM_NAME_PIPES_AS_CONCAT
};

/* Oracle's trigger correlation names are not yet supported. */
static const char
  *compat_mode_values_PRM_MYSQL_TRIGGER_CORRELATION_NAMES[COMPAT_ORACLE + 2] =
{
  NULL,				/* COMPAT_CUBRID     */
  "yes",			/* COMPAT_MYSQL      */
  NULL,				/* COMPAT_ORACLE     */
  PRM_NAME_MYSQL_TRIGGER_CORRELATION_NAMES
};

static const char **compat_mode_values[] = {
  compat_mode_values_PRM_ANSI_QUOTES,
  compat_mode_values_PRM_ORACLE_STYLE_EMPTY_STRING,
  compat_mode_values_PRM_ORACLE_STYLE_OUTERJOIN,
  compat_mode_values_PRM_PIPES_AS_CONCAT,
  compat_mode_values_PRM_MYSQL_TRIGGER_CORRELATION_NAMES
};


/*
 * Message id in the set MSGCAT_SET_PARAMETER
 * in the message catalog MSGCAT_CATALOG_CUBRID (file cubrid.msg).
 */
#define MSGCAT_PARAM_NOT_DIRECTORY              2
#define MSGCAT_PARAM_INIT_FILE_NOT_CREATED      3
#define MSGCAT_PARAM_CANT_WRITE                 4
#define MSGCAT_PARAM_CANT_ACCESS                6
#define MSGCAT_PARAM_NO_HOME                    7
#define MSGCAT_PARAM_NO_VALUE                   8
#define MSGCAT_PARAM_CANT_OPEN_INIT             9
#define MSGCAT_PARAM_BAD_LINE                   10
#define MSGCAT_PARAM_BAD_ENV_VAR                11
#define MSGCAT_PARAM_BAD_KEYWORD                12
#define MSGCAT_PARAM_BAD_VALUE                  13
#define MSGCAT_PARAM_NO_MEM                     14
#define MSGCAT_PARAM_BAD_STRING                 15
#define MSGCAT_PARAM_BAD_RANGE                  16
#define MSGCAT_PARAM_UNIX_ERROR                 17
#define MSGCAT_PARAM_NO_MSG                     18
#define MSGCAT_PARAM_RESET_BAD_RANGE            19
#define MSGCAT_PARAM_KEYWORD_INFO_INT           20
#define MSGCAT_PARAM_KEYWORD_INFO_FLOAT         21

static void prm_the_file_has_been_loaded (const char *path);
static int prm_print_value (const SYSPRM_PARAM * prm, char *buf, size_t len);
static int prm_print (const SYSPRM_PARAM * prm, char *buf, size_t len,
		      bool print_name);
static int sysprm_load_and_init_internal (const char *db_name,
					  const char *conf_file, bool reload);
static void prm_check_environment (void);
static void prm_load_by_section (INI_TABLE * ini, const char *section,
				 bool ignore_section, bool reload,
				 const char *file);
static int prm_read_and_parse_ini_file (const char *prm_file_name,
					const char *db_name,
					const bool reload);
static void prm_report_bad_entry (int line, int err, const char *where);
static int prm_set (SYSPRM_PARAM * prm, const char *value);
static int prm_set_force (SYSPRM_PARAM * prm, const char *value);
static int prm_set_default (SYSPRM_PARAM * prm);
static SYSPRM_PARAM *prm_find (const char *pname, const char *section);
static const KEYVAL *prm_keyword (int val, const char *name,
				  const KEYVAL * tbl, int dim);
static void prm_tune_parameters (void);
static int prm_compound_has_changed (SYSPRM_PARAM * prm);
static void prm_set_compound (SYSPRM_PARAM * param,
			      const char **compound_param_values[],
			      const int values_count);

/* conf files that have been loaded */
#define MAX_NUM_OF_PRM_FILES_LOADED	10
static struct
{
  char *conf_path;
  char *db_name;
} prm_Files_loaded[MAX_NUM_OF_PRM_FILES_LOADED];

/*
 * prm_file_has_been_loaded - Record the file path that has been loaded
 *   return: none
 *   conf_path(in): path of the conf file to be recorded
 *   db_name(in): db name to be recorded
 */
static void
prm_file_has_been_loaded (const char *conf_path, const char *db_name)
{
  int i;
  assert (conf_path != NULL);

  for (i = 0; i < MAX_NUM_OF_PRM_FILES_LOADED; i++)
    {
      if (prm_Files_loaded[i].conf_path == NULL)
	{
	  prm_Files_loaded[i].conf_path = strdup (conf_path);
	  prm_Files_loaded[i].db_name = db_name ? strdup (db_name) : NULL;
	  return;
	}
    }
}


/*
 * sysprm_dump_parameters - Print out current system parameters
 *   return: none
 *   fp(in):
 */
void
sysprm_dump_parameters (FILE * fp)
{
  char buf[LINE_MAX];
  int i;
  const SYSPRM_PARAM *prm;

  fprintf (fp, "#\n# cubrid.conf\n#\n\n");
  fprintf (fp,
	   "# system parameters were loaded from the files ([@section])\n");
  for (i = 0; i < MAX_NUM_OF_PRM_FILES_LOADED; i++)
    {
      if (prm_Files_loaded[i].conf_path != NULL)
	{
	  fprintf (fp, "# %s", prm_Files_loaded[i].conf_path);
	  if (prm_Files_loaded[i].db_name)
	    {
	      fprintf (fp, " [@%s]\n", prm_Files_loaded[i].db_name);
	    }
	  else
	    {
	      fprintf (fp, "\n");
	    }
	}
    }

  fprintf (fp, "\n# system parameters\n");
  for (i = 0; i < NUM_PRM; i++)
    {
      prm = &prm_Def[i];
      if (PRM_IS_HIDDEN (prm->flag))
	{
	  continue;
	}
      prm_print (prm, buf, LINE_MAX, true);
      fprintf (fp, "%s\n", buf);
    }

  return;
}

/*
 * sysprm_load_and_init_internal - Read system parameters from the init files
 *   return: NO_ERROR or ER_FAILED
 *   db_name(in): database name
 *   conf_file(in): config file
 *   reload(in):
 *
 * Note: Parameters would be tuned and forced according to the internal rules.
 */
static int
sysprm_load_and_init_internal (const char *db_name, const char *conf_file,
			       bool reload)
{
  char *base_db_name;
  char file_being_dealt_with[PATH_MAX];
  unsigned int i;
  struct stat stat_buf;
  int r;

  if (db_name == NULL)
    {
      /* intialize message catalog at here because there could be a code path
       * that did not call msgcat_init() before */
      if (msgcat_init () != NO_ERROR)
	{
	  return ER_FAILED;
	}
      base_db_name = NULL;
    }
  else
    {
      base_db_name = basename ((char *) db_name);
    }

#if !defined (CS_MODE)
  if (base_db_name != NULL && reload == false)
    {
      time_t log_time;
      struct tm log_tm, *log_tm_p = &log_tm;
      char error_log_name[PATH_MAX];

      log_time = time (NULL);
#if defined (SERVER_MODE) && !defined (WINDOWS)
      log_tm_p = localtime_r (&log_time, &log_tm);
#else
      log_tm_p = localtime (&log_time);
#endif /* SERVER_MODE && !WINDOWS */
      if (log_tm_p != NULL)
	{
	  snprintf (error_log_name, PATH_MAX - 1,
		    "%s%c%s_%04d%02d%02d_%02d%02d.err", ER_LOG_FILE_DIR,
		    PATH_SEPARATOR, base_db_name, log_tm_p->tm_year + 1900,
		    log_tm_p->tm_mon + 1, log_tm_p->tm_mday,
		    log_tm_p->tm_hour, log_tm_p->tm_min);
	  (void) prm_set (prm_find (PRM_NAME_ER_LOG_FILE, NULL),
			  error_log_name);
	}
    }
#endif /* !CS_MODE */

  /*
   * Read installation configuration file - $CUBRID/conf/cubrid.conf
   * or use conf_file if exist
   */

  if (conf_file == NULL)
    {
      /* use environment variable's value if exist */
      conf_file = envvar_get ("CONF_FILE");
      if (conf_file != NULL && *conf_file == '\0')
	{
	  conf_file = NULL;
	}
    }

  if (conf_file != NULL)
    {
      /* use user specified config path and file */
      strncpy (file_being_dealt_with, conf_file,
	       sizeof (file_being_dealt_with) - 1);
    }
  else
    {
      envvar_confdir_file (file_being_dealt_with, PATH_MAX,
			   sysprm_conf_file_name);
    }

  if (stat (file_being_dealt_with, &stat_buf) != 0)
    {
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_PARAMETERS,
				       MSGCAT_PARAM_CANT_ACCESS),
	       file_being_dealt_with, strerror (errno));
    }
  else
    {
#if !defined(CS_MODE)
      r = prm_read_and_parse_ini_file (file_being_dealt_with,
				       base_db_name, reload);
#else /* !CS_MODE */
      r = prm_read_and_parse_ini_file (file_being_dealt_with, NULL, reload);
#endif /* CS_MODE */
    }

#if !defined (SERVER_MODE)
  /*
   * Read $PWD/cubrid.conf if exist; not for server
   */
  if (conf_file == NULL)
    {
      snprintf (file_being_dealt_with, sizeof (file_being_dealt_with) - 1,
		"%s", sysprm_conf_file_name);
      if (stat (file_being_dealt_with, &stat_buf) == 0)
	{
#if !defined(CS_MODE)
	  r = prm_read_and_parse_ini_file (file_being_dealt_with,
					   base_db_name, reload);
#else /* !CS_MODE */
	  r = prm_read_and_parse_ini_file (file_being_dealt_with,
					   NULL, reload);
#endif /* CS_MODE */
	}
    }
#endif /* !SERVER_MODE */

  /*
   * If a parameter is not given, set it by default
   */
  for (i = 0; i < NUM_PRM; i++)
    {
      if (!PRM_IS_SET (prm_Def[i].flag))
	{
	  if (PRM_HAS_DEFAULT (prm_Def[i].flag))
	    {
	      (void) prm_set_default (&prm_Def[i]);
	    }
	  else
	    {
	      fprintf (stderr,
		       msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_PARAMETERS,
				       MSGCAT_PARAM_NO_VALUE),
		       prm_Def[i].name);
	    }
	}
    }

  /*
   * Perform system parameter check and tuning.
   */
  prm_check_environment ();
  prm_tune_parameters ();

  /*
   * Perform forced system parameter setting.
   */
  for (i = 0; i < DIM (prm_Def); i++)
    {
      if (prm_Def[i].force_value)
	{
	  prm_set (&prm_Def[i], prm_Def[i].force_value);
	}
    }

#if 0
  if (envvar_get ("PARAM_DUMP"))
    {
      sysprm_dump_parameters (stdout);
    }
#endif

  intl_Mbs_support = PRM_INTL_MBS_SUPPORT;

  return NO_ERROR;
}

/*
 * sysprm_load_and_init - Read system parameters from the init files
 *   return: NO_ERROR or ER_FAILED
 *   db_name(in): database name
 *   conf_file(in): config file
 *
 */
int
sysprm_load_and_init (const char *db_name, const char *conf_file)
{
  return sysprm_load_and_init_internal (db_name, conf_file, false);
}

/*
 * sysprm_reload_and_init - Read system parameters from the init files
 *   return: NO_ERROR or ER_FAILED
 *   db_name(in): database name
 *   conf_file(in): config file
 *
 */
int
sysprm_reload_and_init (const char *db_name, const char *conf_file)
{
  return sysprm_load_and_init_internal (db_name, conf_file, true);
}

/*
 * prm_load_by_section - Set system parameters from a file
 *   return: void
 *   ini(in):
 *   section(in):
 *   ignore_section(in):
 *   reload(in):
 *   file(in):
 */
static void
prm_load_by_section (INI_TABLE * ini, const char *section,
		     bool ignore_section, bool reload, const char *file)
{
  int i, error;
  int sec_len;
  const char *sec_p;
  const char *key, *value;
  SYSPRM_PARAM *prm;

  sec_p = (ignore_section) ? NULL : section;

  for (i = 0; i < ini->size; i++)
    {
      if (ini->key[i] == NULL || ini->val[i] == NULL)
	{
	  continue;
	}

      key = ini->key[i];
      value = ini->val[i];

      if (ini_hassec (key))
	{
	  sec_len = ini_seccmp (key, section);
	  if (!sec_len)
	    {
	      continue;
	    }
	  sec_len++;
	}
      else
	{
	  if (ignore_section)
	    {
	      sec_len = 1;
	    }
	  else
	    {
	      continue;
	    }
	}

      prm = prm_find (key + sec_len, sec_p);
      if (reload && (prm && !PRM_IS_RELOADABLE (prm->flag)))
	{
	  continue;
	}

      error = prm_set (prm, value);
      if (error != NO_ERROR)
	{
	  prm_report_bad_entry (ini->lineno[i], error, file);
	}
    }
}

/*
 * prm_read_and_parse_ini_file - Set system parameters from a file
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   prm_file_name(in):
 *   db_name(in):
 *   reload(in):
 */
static int
prm_read_and_parse_ini_file (const char *prm_file_name, const char *db_name,
			     const bool reload)
{
  INI_TABLE *ini;
  char sec_name[LINE_MAX];

  ini = ini_parser_load (prm_file_name);
  if (ini == NULL)
    {
      fprintf (stderr,
	       msgcat_message (MSGCAT_CATALOG_CUBRID, MSGCAT_SET_PARAMETERS,
			       MSGCAT_PARAM_CANT_OPEN_INIT), prm_file_name,
	       strerror (errno));
      return PRM_ERR_FILE_ERR;
    }

  prm_load_by_section (ini, "common", true, reload, prm_file_name);
  if (db_name != NULL && *db_name != '\0')
    {
      snprintf (sec_name, LINE_MAX, "@%s", db_name);
      prm_load_by_section (ini, sec_name, true, reload, prm_file_name);
    }
  prm_load_by_section (ini, "service", false, reload, prm_file_name);

  ini_parser_free (ini);

  prm_file_has_been_loaded (prm_file_name, db_name);

  return NO_ERROR;
}

/*
 * prm_check_environment -
 *   return: none
 */
static void
prm_check_environment (void)
{
  int i;
  char buf[256];

  for (i = 0; i < NUM_PRM; i++)
    {
      SYSPRM_PARAM *prm;
      const char *str;

      prm = &prm_Def[i];
      strncpy (buf, prm->name, sizeof (buf) - 1);
      buf[sizeof (buf) - 1] = '\0';
      ustr_upper (buf);

      str = envvar_get (buf);
      if (str && str[0])
	{
	  int error;
	  error = prm_set (prm, str);
	  if (error != 0)
	    {
	      prm_report_bad_entry (-1, error, buf);
	    }
	}
    }
}


/*
 * prm_change - Set the values of parameters
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   data(in): parameter setting line, e.g. "name=value"
 *   check(in): check PRM_USER_CAN_CHANGE, PRM_IS_FOR_CLIENT
 */
static int
prm_change (const char *data, bool check)
{
  char buf[256], *p, *name, *value;
  SYSPRM_PARAM *prm;
  int err = PRM_ERR_NO_ERROR;

  if (!data || *data == '\0')
    {
      return PRM_ERR_BAD_VALUE;
    }

  if (intl_mbs_ncpy (buf, data, 255) == NULL)
    {
      return PRM_ERR_BAD_VALUE;
    }

  p = buf;
  do
    {
      while (char_isspace (*p))
	{
	  p++;
	}
      if (*p == '\0')
	{
	  break;
	}
      name = p;

      while (*p && !char_isspace (*p) && *p != '=')
	{
	  p++;
	}
      if (*p == '\0')
	{
	  return PRM_ERR_BAD_VALUE;
	}
      else if (*p == '=')
	{
	  *p++ = '\0';
	}
      else
	{
	  *p++ = '\0';
	  while (char_isspace (*p))
	    {
	      p++;
	    }
	  if (*p == '=')
	    {
	      p++;
	    }
	}

      while (char_isspace (*p))
	{
	  p++;
	}
      if (*p == '\0')
	{
	  break;
	}
      value = p;

      if (*p == '"' || *p == '\'')
	{
	  char *t, delim;

	  delim = *p++;
	  value = t = p;
	  while (*t && *t != delim)
	    {
	      if (*t == '\\')
		{
		  t++;
		}
	      *p++ = *t++;
	    }
	  if (*t != delim)
	    {
	      return PRM_ERR_BAD_STRING;
	    }
	}
      else
	{
	  while (*p && !char_isspace (*p) && *p != ';')
	    {
	      p++;
	    }
	}

      if (*p)
	{
	  *p++ = '\0';
	  while (char_isspace (*p))
	    {
	      p++;
	    }
	  if (*p == ';')
	    {
	      p++;
	    }
	}

      prm = prm_find (name, NULL);
      if (prm == NULL)
	{
	  return PRM_ERR_UNKNOWN_PARAM;
	}

      if (!check
	  || PRM_USER_CAN_CHANGE (prm->flag)
	  || (PRM_TEST_CHANGE_ONLY (prm->flag) && PRM_TEST_MODE))
	{
	  /* We allow changing the parameter value. */
	}
      else
	{
	  return PRM_ERR_CANNOT_CHANGE;
	}
#if defined (SA_MODE)
      err = prm_set (prm, value);
#endif /* SA_MODE */
#if defined(CS_MODE)
      if (check == true)
	{
	  if (PRM_IS_FOR_CLIENT (prm->flag))
	    {
	      err = prm_set (prm, value);
	      if (PRM_IS_FOR_SERVER (prm->flag))
		{
		  /* for both client and server, return not for client after set it */
		  return PRM_ERR_NOT_FOR_CLIENT;
		}
	    }
	  else
	    {
	      if (PRM_IS_FOR_SERVER (prm->flag))
		{
		  /* for server only, return not for client */
		  return PRM_ERR_NOT_FOR_CLIENT;
		}
	      else
		{
		  /* not for both client and server, return error */
		  return PRM_ERR_CANNOT_CHANGE;
		}
	    }
	}
      else
	{
	  err = prm_set (prm, value);
	}
#endif /* CS_MODE */
#if defined (SERVER_MODE)
      if (!PRM_IS_FOR_SERVER (prm->flag))
	{
	  return PRM_ERR_NOT_FOR_SERVER;
	}
      err = prm_set (prm, value);
#endif /* SERVER_MODE */
    }
  while (err == PRM_ERR_NO_ERROR && *p);

  return err;
}

/*
 * sysprm_change_parameters - Set the values of parameters
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   data(in): parameter setting line, e.g. "name=value"
 *
 * Note: Mutiple parameters can be changed at a time by providing a string
 *       line which is like "param1=value1; param2=value2; ...".
 */
int
sysprm_change_parameters (const char *data)
{
  return prm_change (data, true);
}

/*
 * prm_print - Print a parameter to the buffer
 *   return: number of chars printed
 *   prm(in): parameter
 *   buf(out): print buffer
 *   len(in): length of the buffer
 *   print_name(in): print name of the parameter
 */
static int
prm_print (const SYSPRM_PARAM * prm, char *buf, size_t len, bool print_name)
{
  int n = 0;

  assert (prm != NULL && buf != NULL && len > 0);

  if (PRM_IS_INTEGER (prm->flag))
    {
      if (print_name)
	{
	  n =
	    snprintf (buf, len, "%s=%d", prm->name, PRM_GET_INT (prm->value));
	}
      else
	{
	  n = snprintf (buf, len, "%d", PRM_GET_INT (prm->value));
	}
    }
  else if (PRM_IS_BOOLEAN (prm->flag))
    {
      n = snprintf (buf, len, "%s=%c", prm->name,
		    (PRM_GET_BOOL (prm->value) ? 'y' : 'n'));
    }
  else if (PRM_IS_FLOAT (prm->flag))
    {
      n = snprintf (buf, len, "%s=%f", prm->name, PRM_GET_FLOAT (prm->value));
    }
  else if (PRM_IS_STRING (prm->flag))
    {
      n = snprintf (buf, len, "%s=\"%s\"", prm->name,
		    (PRM_GET_STRING (prm->value) ?
		     PRM_GET_STRING (prm->value) : ""));
    }
  else if (PRM_IS_KEYWORD (prm->flag))
    {
      const KEYVAL *keyvalp = NULL;
      if (intl_mbs_casecmp (prm->name, PRM_NAME_ER_LOG_LEVEL) == 0)
	{
	  keyvalp = prm_keyword (PRM_GET_INT (prm->value),
				 NULL, er_log_level_words,
				 DIM (er_log_level_words));
	}
      else if (intl_mbs_casecmp (prm->name, PRM_NAME_LOG_ISOLATION_LEVEL) ==
	       0)
	{
	  keyvalp = prm_keyword (PRM_GET_INT (prm->value),
				 NULL, isolation_level_words,
				 DIM (isolation_level_words));
	}
      else if (intl_mbs_casecmp (prm->name, PRM_NAME_HA_MODE) == 0)
	{
	  keyvalp = prm_keyword (PRM_GET_INT (prm->value),
				 NULL, ha_mode_words, DIM (ha_mode_words));
	}
      else if (intl_mbs_casecmp (prm->name, PRM_NAME_HA_SERVER_STATE) == 0)
	{
	  keyvalp = prm_keyword (PRM_GET_INT (prm->value),
				 NULL, ha_server_state_words,
				 DIM (ha_server_state_words));
	}
      else if (intl_mbs_casecmp (prm->name, PRM_NAME_HA_LOG_APPLIER_STATE) ==
	       0)
	{
	  keyvalp = prm_keyword (PRM_GET_INT (prm->value),
				 NULL, ha_log_applier_state_words,
				 DIM (ha_log_applier_state_words));
	}
      else if (intl_mbs_casecmp (prm->name, PRM_NAME_COMPAT_MODE) == 0)
	{
	  keyvalp = prm_keyword (PRM_GET_INT (prm->value),
				 NULL, compat_words, DIM (compat_words));
	}
      else
	{
	  assert (false);
	}
      if (keyvalp)
	{
	  n = snprintf (buf, len, "%s=\"%s\"", prm->name, keyvalp->key);
	}
      else
	{
	  n = snprintf (buf, len, "%s=%d", prm->name,
			PRM_GET_INT (prm->value));
	}
    }
  else if (PRM_IS_ERROR_LIST (prm->flag))
    {
      bool *error_list;
      int err_id;
      char *s;

      error_list = PRM_GET_ERROR_LIST (prm->value);
      if (error_list)
	{
	  s = buf;
	  n = snprintf (s, len, "%s= ", prm->name);
	  n -= 1;		/* to overwrite at the position of " " */
	  s += n;
	  len -= n;
	  for (err_id = 1; err_id <= -(ER_LAST_ERROR); err_id++)
	    {
	      if (error_list[err_id])
		{
		  n = snprintf (s, len, "%d,", err_id);
		  n -= 1;	/* to overwrite at the position of "," */
		  s += n;
		  len -= n;
		}
	    }
	  snprintf (s, len, " ");
	}
      else
	{
	  n = snprintf (buf, len, "%s=", prm->name);
	}
    }
  else if (PRM_IS_INTEGER_LIST (prm->flag))
    {
      int *int_list, list_size, i;
      char *s;

      int_list = PRM_GET_INTEGER_LIST (prm->value);
      if (int_list)
	{
	  list_size = int_list[0];
	  s = buf;
	  n = snprintf (s, len, "%s= ", prm->name);
	  n -= 1;		/* to overwrite at the position of " " */
	  s += n;
	  len -= n;
	  for (i = 1; i <= list_size; i++)
	    {
	      n = snprintf (s, len, "%d,", int_list[i]);
	      n -= 1;		/* to overwrite at the position of "," */
	      s += n;
	      len -= n;
	    }
	  snprintf (s, len, " ");
	}
      else
	{
	  n = snprintf (buf, len, "%s=", prm->name);
	}
    }
  else
    {
      n = snprintf (buf, len, "%s=?", prm->name);
    }

  return n;
}

/*
 * sysprm_obtain_parameters - Get parameters as the form of setting line,
 *                         e.g. "name=value"
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   data(in/out): names of parameters to get
 *                 and as buffer where the output to be stored
 *   len(in): size of the data buffer
 *
 * Note: Mutiple parameters can be ontained at a time by providing a string
 *       line which is like "param1; param2; ...".
 */
int
sysprm_obtain_parameters (char *data, int len)
{
  char buf[LINE_MAX], *p, *name, *t;
  int n;
  SYSPRM_PARAM *prm;

  if (!data || *data == '\0' || len <= 0)
    {
      return PRM_ERR_BAD_VALUE;
    }

  if (intl_mbs_ncpy (buf, data, LINE_MAX - 1) == NULL)
    {
      return PRM_ERR_BAD_VALUE;
    }

  *(t = data) = '\0';
  p = buf;
  do
    {
      while (char_isspace (*p))
	{
	  p++;
	}
      if (*p == '\0')
	{
	  break;
	}
      name = p;

      while (*p && !char_isspace (*p) && *p != ';')
	{
	  p++;
	}

      if (*p)
	{
	  *p++ = '\0';
	  while (char_isspace (*p))
	    {
	      p++;
	    }
	  if (*p == ';')
	    {
	      p++;
	    }
	}

      if ((prm = prm_find (name, NULL)) == NULL)
	{
	  return PRM_ERR_UNKNOWN_PARAM;
	}

      if (t != data)
	{
	  *t++ = ';';
	  len--;
	}

#ifdef SA_MODE
      n = prm_print (prm, t, len, true);
#endif
#if defined(CS_MODE) || defined(CS_MODE_MT)
      if (PRM_IS_FOR_CLIENT (prm->flag))
	{
	  n = prm_print (prm, t, len, true);
	  if (PRM_IS_FOR_SERVER (prm->flag))
	    {
	      /* for both client and server, return not for client after set it */
	      return PRM_ERR_NOT_FOR_CLIENT;
	    }
	}
      else
	{
	  if (PRM_IS_FOR_SERVER (prm->flag))
	    {
	      /* for server only, return not for client */
	      return PRM_ERR_NOT_FOR_CLIENT;
	    }
	  else
	    {
	      /* not for both client and server, return error */
	      return PRM_ERR_CANNOT_CHANGE;
	    }
	}
#endif
#ifdef SERVER_MODE
      if (!PRM_IS_FOR_SERVER (prm->flag))
	{
	  return PRM_ERR_NOT_FOR_SERVER;
	}
      n = prm_print (prm, t, len, true);
#endif
      len -= n;
      t += n;
    }
  while (*p && len > 0);

  return NO_ERROR;
}


#if !defined(CS_MODE)
/*
 * xsysprm_change_server_parameters -
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   arg1(in):
 *   arg2(in):
 */
int
xsysprm_change_server_parameters (const char *data)
{
  return sysprm_change_parameters (data);
}


/*
 * xsysprm_change_server_parameters -
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   data(data):
 *   len(in):
 */
int
xsysprm_obtain_server_parameters (char *data, int len)
{
  return sysprm_obtain_parameters (data, len);
}


/*
 * xsysprm_dump_server_parameters -
 *   return: none
 *   fp(in):
 */
void
xsysprm_dump_server_parameters (FILE * outfp)
{
  sysprm_dump_parameters (outfp);
}
#endif /* !CS_MODE */


/*
 * prm_set - Set the value of a parameter
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   prm(in):
 *   value(in):
 */
static int
prm_set (SYSPRM_PARAM * prm, const char *value)
{
  char *end;
  int warning_status = NO_ERROR;

  if (prm == NULL)
    {
      return PRM_ERR_UNKNOWN_PARAM;
    }

  if (value == NULL || strlen (value) == 0)
    {
      return PRM_ERR_BAD_VALUE;
    }

  if (PRM_IS_INTEGER (prm->flag))
    {
      int val, *valp;
      val = strtol (value, &end, 10);
      if (end == value)
	{
	  return PRM_ERR_BAD_VALUE;
	}
      if ((prm->upper_limit && PRM_GET_INT (prm->upper_limit) < val)
	  || (prm->lower_limit && PRM_GET_INT (prm->lower_limit) > val))
	{
	  if (PRM_HAS_DEFAULT (prm->flag))
	    {
	      if (val != PRM_GET_INT (prm->default_value))
		{
		  fprintf (stderr,
			   msgcat_message (MSGCAT_CATALOG_CUBRID,
					   MSGCAT_SET_PARAMETERS,
					   MSGCAT_PARAM_KEYWORD_INFO_INT),
			   prm->name, val, PRM_GET_INT (prm->default_value));
		  val = PRM_GET_INT (prm->default_value);
		  warning_status = PRM_ERR_RESET_BAD_RANGE;
		}
	    }
	  else
	    {
	      return PRM_ERR_BAD_RANGE;
	    }
	}
      valp = (int *) prm->value;
      *valp = val;
    }
  else if (PRM_IS_FLOAT (prm->flag))
    {
      float val, *valp;

      val = (float) strtod (value, &end);
      if (end == value)
	{
	  return PRM_ERR_BAD_VALUE;
	}
      if ((prm->upper_limit && PRM_GET_FLOAT (prm->upper_limit) < val)
	  || (prm->lower_limit && PRM_GET_FLOAT (prm->lower_limit) > val))
	{
	  if (PRM_HAS_DEFAULT (prm->flag))
	    {
	      if (val != PRM_GET_FLOAT (prm->default_value))
		{
		  fprintf (stderr,
			   msgcat_message (MSGCAT_CATALOG_CUBRID,
					   MSGCAT_SET_PARAMETERS,
					   MSGCAT_PARAM_KEYWORD_INFO_FLOAT),
			   prm->name, val,
			   PRM_GET_FLOAT (prm->default_value));
		  val = PRM_GET_FLOAT (prm->default_value);
		  warning_status = PRM_ERR_RESET_BAD_RANGE;
		}
	    }
	  else
	    {
	      return PRM_ERR_BAD_RANGE;
	    }
	}
      valp = (float *) prm->value;
      *valp = val;
    }
  else if (PRM_IS_BOOLEAN (prm->flag))
    {
      bool *valp;
      const KEYVAL *keyvalp;

      keyvalp = prm_keyword (-1, value, boolean_words, DIM (boolean_words));
      if (keyvalp == NULL)
	{
	  return PRM_ERR_BAD_VALUE;
	}
      valp = (bool *) prm->value;
      *valp = (bool) keyvalp->val;
    }
  else if (PRM_IS_STRING (prm->flag))
    {
      char *val, **valp;

      if (PRM_IS_ALLOCATED (prm->flag))
	{
	  char *str = PRM_GET_STRING (prm->value);
	  free_and_init (str);
	  PRM_CLEAR_BIT (PRM_ALLOCATED, prm->flag);
	}
      valp = (char **) prm->value;
      /* check if the value is represented as a null keyword */
      if (prm_keyword (-1, value, null_words, DIM (null_words)) != NULL)
	{
	  val = NULL;
	}
      else
	{
	  val = strdup (value);
	  if (val == NULL)
	    {
	      return (PRM_ERR_NO_MEM_FOR_PRM);
	    }
	  PRM_SET_BIT (PRM_ALLOCATED, prm->flag);
	}
      *valp = val;
    }
  else if (PRM_IS_KEYWORD (prm->flag))
    {
      int val, *valp;
      const KEYVAL *keyvalp = NULL;

      if (intl_mbs_casecmp (prm->name, PRM_NAME_ER_LOG_LEVEL) == 0)
	{
	  keyvalp = prm_keyword (-1, value, er_log_level_words,
				 DIM (er_log_level_words));
	}
      else if (intl_mbs_casecmp (prm->name, PRM_NAME_LOG_ISOLATION_LEVEL) ==
	       0)
	{
	  keyvalp = prm_keyword (-1, value, isolation_level_words,
				 DIM (isolation_level_words));
	}
      else if (intl_mbs_casecmp (prm->name, PRM_NAME_HA_MODE) == 0)
	{
	  keyvalp = prm_keyword (-1, value, ha_mode_words,
				 DIM (ha_mode_words));
	}
      else if (intl_mbs_casecmp (prm->name, PRM_NAME_HA_SERVER_STATE) == 0)
	{
	  keyvalp = prm_keyword (-1, value, ha_server_state_words,
				 DIM (ha_server_state_words));
	}
      else if (intl_mbs_casecmp (prm->name, PRM_NAME_HA_LOG_APPLIER_STATE) ==
	       0)
	{
	  keyvalp = prm_keyword (-1, value, ha_log_applier_state_words,
				 DIM (ha_log_applier_state_words));
	}
      else if (intl_mbs_casecmp (prm->name, PRM_NAME_COMPAT_MODE) == 0)
	{
	  keyvalp = prm_keyword (-1, value, compat_words, DIM (compat_words));
	}
      else
	{
	  assert (false);
	}
      if (keyvalp)
	{
	  val = (int) keyvalp->val;
	}
      else
	{
	  val = strtol (value, &end, 10);
	  if (end == value)
	    {
	      return PRM_ERR_BAD_VALUE;
	    }
	}
      if ((prm->upper_limit && PRM_GET_INT (prm->upper_limit) < val)
	  || (prm->lower_limit && PRM_GET_INT (prm->lower_limit) > val))
	{
	  if (PRM_HAS_DEFAULT (prm->flag))
	    {
	      if (val != PRM_GET_INT (prm->default_value))
		{
		  fprintf (stderr,
			   msgcat_message (MSGCAT_CATALOG_CUBRID,
					   MSGCAT_SET_PARAMETERS,
					   MSGCAT_PARAM_KEYWORD_INFO_INT),
			   prm->name, val, PRM_GET_INT (prm->default_value));
		  val = PRM_GET_INT (prm->default_value);
		  warning_status = PRM_ERR_RESET_BAD_RANGE;
		}
	    }
	  else
	    {
	      return PRM_ERR_BAD_RANGE;
	    }
	}
      valp = (int *) prm->value;
      *valp = val;
    }
  else if (PRM_IS_ERROR_LIST (prm->flag))
    {
      bool *val, **valp;

      if (PRM_IS_ALLOCATED (prm->flag))
	{
	  bool *error_list = PRM_GET_ERROR_LIST (prm->value);
	  free_and_init (error_list);
	  PRM_CLEAR_BIT (PRM_ALLOCATED, prm->flag);
	}
      valp = (bool **) prm->value;
      /* check if the value is represented as a null keyword */
      if (prm_keyword (-1, value, null_words, DIM (null_words)) != NULL)
	{
	  val = NULL;
	}
      else
	{
	  char *s, *p;
	  int err_id;

	  val = calloc (-(ER_LAST_ERROR) + 1, sizeof (bool));
	  if (val == NULL)
	    {
	      return (PRM_ERR_NO_MEM_FOR_PRM);
	    }
	  PRM_SET_BIT (PRM_ALLOCATED, prm->flag);
	  s = (char *) value;
	  p = s;
	  while (*s)
	    {
	      if (*s == ',')
		{
		  *s = '\0';
		  err_id = abs (atoi (p));
		  if (err_id != 0 && err_id <= -(ER_LAST_ERROR))
		    {
		      val[err_id] = true;
		    }
		  p = s + 1;
		  *s = ',';
		}
	      s++;
	    }
	  err_id = abs (atoi (p));
	  if (err_id != 0)
	    {
	      val[err_id] = true;
	    }
	}
      *valp = val;
    }
  else if (PRM_IS_INTEGER_LIST (prm->flag))
    {
      int *val, **valp;

      if (PRM_IS_ALLOCATED (prm->flag))
	{
	  int *int_list = PRM_GET_INTEGER_LIST (prm->value);
	  free_and_init (int_list);
	  PRM_CLEAR_BIT (PRM_ALLOCATED, prm->flag);
	}
      valp = (int **) prm->value;
      /* check if the value is represented as a null keyword */
      if (prm_keyword (-1, value, null_words, DIM (null_words)) != NULL)
	{
	  val = NULL;
	}
      else
	{
	  char *s, *p;
	  int list_size;

	  val = calloc (1024, sizeof (int));	/* max size is 1023 */
	  if (val == NULL)
	    {
	      return (PRM_ERR_NO_MEM_FOR_PRM);
	    }
	  PRM_SET_BIT (PRM_ALLOCATED, prm->flag);
	  list_size = 0;
	  s = (char *) value;
	  p = s;
	  while (*s)
	    {
	      if (*s == ',')
		{
		  *s = '\0';
		  val[++list_size] = atoi (p);
		  p = s + 1;
		  *s = ',';
		}
	      s++;
	    }
	  val[++list_size] = atoi (p);
	  val[0] = list_size;
	}
      *valp = val;
    }
  else
    {
      assert (false);
    }

  if (PRM_IS_COMPOUND (prm->flag))
    {
      prm_compound_has_changed (prm);
    }

  PRM_SET_BIT (PRM_SET, prm->flag);
  /* Indicate that the default value was not used */
  PRM_CLEAR_BIT (PRM_DEFAULT_USED, prm->flag);
  return warning_status;
}

static int
prm_compound_has_changed (SYSPRM_PARAM * prm)
{
  assert (PRM_IS_COMPOUND (prm->flag));

  if (prm == prm_find (PRM_NAME_COMPAT_MODE, NULL))
    {
      prm_set_compound (prm, compat_mode_values, DIM (compat_mode_values));
    }
  else
    {
      assert (false);
    }
  return NO_ERROR;
}


/*
 * prm_set_force -
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   prm(in):
 *   value(in):
 */
static int
prm_set_force (SYSPRM_PARAM * prm, const char *value)
{
  if (prm->force_value)
    {
      free_and_init (PRM_GET_STRING (&prm->force_value));
    }

  prm->force_value = strdup (value);
  if (prm->force_value == NULL)
    {
      return PRM_ERR_NO_MEM_FOR_PRM;
    }

  return NO_ERROR;
}

/*
 * prm_set_default -
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   prm(in):
 */
static int
prm_set_default (SYSPRM_PARAM * prm)
{
  if (prm == NULL)
    {
      return ER_FAILED;
    }

  if (PRM_IS_INTEGER (prm->flag) || PRM_IS_KEYWORD (prm->flag))
    {
      int val, *valp;
      val = PRM_GET_INT (prm->default_value);
      valp = (int *) prm->value;
      *valp = val;
    }
  else if (PRM_IS_BOOLEAN (prm->flag))
    {
      bool val, *valp;
      val = PRM_GET_BOOL (prm->default_value);
      valp = (bool *) prm->value;
      *valp = val;
    }
  else if (PRM_IS_FLOAT (prm->flag))
    {
      float val, *valp;
      val = PRM_GET_FLOAT (prm->default_value);
      valp = (float *) prm->value;
      *valp = val;
    }
  else if (PRM_IS_STRING (prm->flag))
    {
      char *val, **valp;
      if (PRM_IS_ALLOCATED (prm->flag))
	{
	  char *str = PRM_GET_STRING (prm->value);
	  free_and_init (str);
	  PRM_CLEAR_BIT (PRM_ALLOCATED, prm->flag);
	}
      val = *(char **) prm->default_value;
      valp = (char **) prm->value;
      *valp = val;
    }
  else if (PRM_IS_ERROR_LIST (prm->flag))
    {
      bool *val, **valp;
      if (PRM_IS_ALLOCATED (prm->flag))
	{
	  bool *error_list = PRM_GET_ERROR_LIST (prm->value);
	  free_and_init (error_list);
	  PRM_CLEAR_BIT (PRM_ALLOCATED, prm->flag);
	}
      val = *(bool **) prm->default_value;
      valp = (bool **) prm->value;
      *valp = val;
    }
  else if (PRM_IS_INTEGER_LIST (prm->flag))
    {
      int *val, **valp;
      if (PRM_IS_ALLOCATED (prm->flag))
	{
	  int *int_list = PRM_GET_INTEGER_LIST (prm->value);
	  free_and_init (int_list);
	  PRM_CLEAR_BIT (PRM_ALLOCATED, prm->flag);
	}
      val = *(int **) prm->default_value;
      valp = (int **) prm->value;
      *valp = val;
    }

  PRM_SET_BIT (PRM_SET, prm->flag);
  /* Indicate that the default value was used */
  PRM_SET_BIT (PRM_DEFAULT_USED, prm->flag);
  return NO_ERROR;
}

/*
 * prm_find -
 *   return: NULL or found parameter
 *   pname(in): parameter name to find
 */
static SYSPRM_PARAM *
prm_find (const char *pname, const char *section)
{
  unsigned int i;
  char *key;
  char buf[4096];

  if (pname == NULL)
    {
      return NULL;
    }

  if (section != NULL)
    {
      snprintf (buf, sizeof (buf) - 1, "%s::%s", section, pname);
      key = buf;
    }
  else
    {
      key = (char *) pname;
    }

  for (i = 0; i < DIM (prm_Def); i++)
    {
      if (intl_mbs_casecmp (prm_Def[i].name, key) == 0)
	{
	  return &prm_Def[i];
	}
    }

  return NULL;
}

/*
 * sysprm_set_force -
 *   return: NO_ERROR or error code
 *   pname(in): parameter name to set
 *   pvalue(in): value to be set to the parameter
 */
int
sysprm_set_force (const char *pname, const char *pvalue)
{
  SYSPRM_PARAM *prm;

  if (pname == NULL || pvalue == NULL)
    {
      return ER_PRM_BAD_VALUE;
    }

  prm = prm_find (pname, NULL);
  if (prm == NULL)
    {
      return ER_PRM_BAD_VALUE;
    }
  if (prm_set_force (prm, pvalue) != NO_ERROR)
    {
      return ER_PRM_CANNOT_CHANGE;
    }
  return NO_ERROR;
}

/*
 * sysprm_set_to_default -
 *   return: NO_ERROR or error code
 *   pname(in): parameter name to set to default value
 */
int
sysprm_set_to_default (const char *pname, bool set_to_force)
{
  SYSPRM_PARAM *prm;
  char val[LINE_MAX];

  if (pname == NULL)
    {
      return ER_PRM_BAD_VALUE;
    }

  prm = prm_find (pname, NULL);
  if (prm == NULL)
    {
      return ER_PRM_BAD_VALUE;
    }
  if (prm_set_default (prm) != NO_ERROR)
    {
      return ER_PRM_CANNOT_CHANGE;
    }
  if (set_to_force)
    {
      prm_print (prm, val, LINE_MAX, false);
      prm_set_force (prm, val);
    }
  return NO_ERROR;
}


/*
 * prm_keyword - Search a keyword within the keyword table
 *   return: NULL or found keyword
 *   val(in): keyword value
 *   name(in): keyword name
 *   tbl(in): keyword table
 *   dim(in): size of the table
 */
static const KEYVAL *
prm_keyword (int val, const char *name, const KEYVAL * tbl, int dim)
{
  int i;

  if (name != NULL)
    {
      for (i = 0; i < dim; i++)
	{
	  if (intl_mbs_casecmp (name, tbl[i].key) == 0)
	    {
	      return &tbl[i];
	    }
	}
    }
  else
    {
      for (i = 0; i < dim; i++)
	{
	  if (tbl[i].val == val)
	    {
	      return &tbl[i];
	    }
	}
    }
  return NULL;
}


/*
 * prm_report_bad_entry -
 *   return:
 *   line(in):
 *   err(in):
 *   where(in):
 */
static void
prm_report_bad_entry (int line, int err, const char *where)
{
  if (line >= 0)
    {
      fprintf (stderr,
	       msgcat_message (MSGCAT_CATALOG_CUBRID, MSGCAT_SET_PARAMETERS,
			       MSGCAT_PARAM_BAD_LINE), line, where);
    }
  else
    {
      fprintf (stderr,
	       msgcat_message (MSGCAT_CATALOG_CUBRID, MSGCAT_SET_PARAMETERS,
			       MSGCAT_PARAM_BAD_ENV_VAR), where);
    }

  if (err > 0)
    {
      switch (err)
	{
	case PRM_ERR_UNKNOWN_PARAM:
	  {
	    fprintf (stderr, "%s\n",
		     msgcat_message (MSGCAT_CATALOG_CUBRID,
				     MSGCAT_SET_PARAMETERS,
				     MSGCAT_PARAM_BAD_KEYWORD));
	    break;
	  }
	case PRM_ERR_BAD_VALUE:
	  {
	    fprintf (stderr, "%s\n",
		     msgcat_message (MSGCAT_CATALOG_CUBRID,
				     MSGCAT_SET_PARAMETERS,
				     MSGCAT_PARAM_BAD_VALUE));
	    break;
	  }
	case PRM_ERR_NO_MEM_FOR_PRM:
	  {
	    fprintf (stderr, "%s\n",
		     msgcat_message (MSGCAT_CATALOG_CUBRID,
				     MSGCAT_SET_PARAMETERS,
				     MSGCAT_PARAM_NO_MEM));
	    break;
	  }

	case PRM_ERR_BAD_STRING:
	  fprintf (stderr, "%s\n",
		   msgcat_message (MSGCAT_CATALOG_CUBRID,
				   MSGCAT_SET_PARAMETERS,
				   MSGCAT_PARAM_BAD_STRING));
	  break;

	case PRM_ERR_BAD_RANGE:
	  fprintf (stderr, "%s\n",
		   msgcat_message (MSGCAT_CATALOG_CUBRID,
				   MSGCAT_SET_PARAMETERS,
				   MSGCAT_PARAM_BAD_RANGE));
	  break;

	case PRM_ERR_RESET_BAD_RANGE:
	  fprintf (stderr, "%s\n",
		   msgcat_message (MSGCAT_CATALOG_CUBRID,
				   MSGCAT_SET_PARAMETERS,
				   MSGCAT_PARAM_RESET_BAD_RANGE));
	  break;

	default:
	  break;
	}
    }
  else
    {
      fprintf (stderr,
	       msgcat_message (MSGCAT_CATALOG_CUBRID, MSGCAT_SET_PARAMETERS,
			       MSGCAT_PARAM_UNIX_ERROR), strerror (err));
    }

  fflush (stderr);
}


/*
 * sysprm_final - Clean up the storage allocated during parameter parsing
 *   return: none
 */
void
sysprm_final (void)
{
  SYSPRM_PARAM *prm;
  char **valp;
  int i;

  for (i = 0; i < NUM_PRM; i++)
    {
      prm = &prm_Def[i];
      if (PRM_IS_ALLOCATED (prm->flag) && PRM_IS_STRING (prm->flag))
	{
	  char *str = PRM_GET_STRING (prm->value);
	  free_and_init (str);
	  PRM_CLEAR_BIT (PRM_ALLOCATED, prm->flag);
	  valp = (char **) prm->value;
	  *valp = NULL;
	}
    }

  for (i = 0; i < MAX_NUM_OF_PRM_FILES_LOADED; i++)
    {
      if (prm_Files_loaded[i].conf_path != NULL)
	{
	  free_and_init (prm_Files_loaded[i].conf_path);
	}
      if (prm_Files_loaded[i].db_name != NULL)
	{
	  free_and_init (prm_Files_loaded[i].db_name);
	}
    }
}


/*
 * prm_tune_parameters - Sets the values of various system parameters
 *                       depending on the value of other parameters
 *   return: none
 *
 * Note: Used for providing a mechanism for tuning various system parameters.
 *       The parameters are only tuned if the user has not set them
 *       explictly, this can be ascertained by checking if the default
 *       value has been used.
 */
#if defined (SA_MODE) || defined (SERVER_MODE)
static void
prm_tune_parameters (void)
{
  SYSPRM_PARAM *max_clients_prm;
  SYSPRM_PARAM *max_scanid_bit_prm;
  SYSPRM_PARAM *max_plan_cache_entries_prm;
#if defined (ENABLE_UNUSED_FUNCTION)
  SYSPRM_PARAM *max_plan_cache_clones_prm;
#endif /* ENABLE_UNUSED_FUNCTION */
  SYSPRM_PARAM *query_cache_mode_prm;
  SYSPRM_PARAM *max_query_cache_entries_prm;
  SYSPRM_PARAM *query_cache_size_in_pages_prm;
  SYSPRM_PARAM *ha_mode_prm;
  SYSPRM_PARAM *ha_server_state_prm;
  SYSPRM_PARAM *replication_prm;
  SYSPRM_PARAM *auto_restart_server_prm;
  SYSPRM_PARAM *log_background_archiving_prm;
  SYSPRM_PARAM *log_media_failure_support_prm;
  SYSPRM_PARAM *ha_node_list_prm;

  char newval[LINE_MAX];
  char host_name[MAXHOSTNAMELEN];
  int max_clients;

  /* Find the parameters that require tuning */
  max_clients_prm = prm_find (PRM_NAME_CSS_MAX_CLIENTS, NULL);
  max_scanid_bit_prm = prm_find (PRM_NAME_LK_MAX_SCANID_BIT, NULL);
  max_plan_cache_entries_prm =
    prm_find (PRM_NAME_XASL_MAX_PLAN_CACHE_ENTRIES, NULL);
#if defined (ENABLE_UNUSED_FUNCTION)
  max_plan_cache_clones_prm =
    prm_find (PRM_NAME_XASL_MAX_PLAN_CACHE_CLONES, NULL);
#endif /* ENABLE_UNUSED_FUNCTION */
  query_cache_mode_prm = prm_find (PRM_NAME_LIST_QUERY_CACHE_MODE, NULL);
  max_query_cache_entries_prm =
    prm_find (PRM_NAME_LIST_MAX_QUERY_CACHE_ENTRIES, NULL);
  query_cache_size_in_pages_prm =
    prm_find (PRM_NAME_LIST_MAX_QUERY_CACHE_PAGES, NULL);

  /* temporarily modifies the query result cache feature to be disabled
   * in RB-8.2.2. because it is not verified on 64 bit environment.
   */
  if (query_cache_mode_prm != NULL)
    {
      prm_set (query_cache_mode_prm, "0");
    }

  ha_mode_prm = prm_find (PRM_NAME_HA_MODE, NULL);
  ha_server_state_prm = prm_find (PRM_NAME_HA_SERVER_STATE, NULL);
  replication_prm = prm_find (PRM_NAME_REPLICATION_MODE, NULL);
  auto_restart_server_prm = prm_find (PRM_NAME_AUTO_RESTART_SERVER, NULL);
  log_background_archiving_prm =
    prm_find (PRM_NAME_LOG_BACKGROUND_ARCHIVING, NULL);
  log_media_failure_support_prm =
    prm_find (PRM_NAME_LOG_MEDIA_FAILURE_SUPPORT, NULL);
  ha_node_list_prm = prm_find (PRM_NAME_HA_NODE_LIST, NULL);

  /* Check that max clients has been set */
  assert (max_clients_prm != NULL);
  if (max_clients_prm == NULL)
    {
      return;
    }

#if 0
  if (!(PRM_IS_SET (max_clients_prm->flag)))
    {
      if (PRM_HAS_DEFAULT (max_clients_prm->flag))
	{
	  (void) prm_set_default (max_clients_prm);
	}
      else
	{
	  fprintf (stderr,
		   msgcat_message (MSGCAT_CATALOG_CUBRID,
				   MSGCAT_SET_PARAMETERS,
				   MSGCAT_PARAM_NO_VALUE),
		   max_clients_prm->name);
	  return;
	}
    }
  else
#endif
    {
      max_clients =
	MIN (prm_css_max_clients_upper, css_get_max_socket_fds ());
      if (PRM_GET_INT (max_clients_prm->value) > max_clients)
	{
	  sprintf (newval, "%d", max_clients);
	  (void) prm_set (max_clients_prm, newval);
	}
    }

#if defined (SERVER_MODE)
  assert (max_scanid_bit_prm != NULL);
  if (max_scanid_bit_prm == NULL)
    {
      return;
    }

  if (PRM_GET_INT (max_scanid_bit_prm->value) % 32)
    {
      sprintf (newval, "%d",
	       ((PRM_GET_INT (max_scanid_bit_prm->value)) +
		32 - (PRM_GET_INT (max_scanid_bit_prm->value)) % 32));
      (void) prm_set (max_scanid_bit_prm, newval);
    }
#endif /* SERVER_MODE */

  /* check Plan Cache and Query Cache parameters */
  assert (max_plan_cache_entries_prm != NULL);
  assert (query_cache_mode_prm != NULL);
  assert (max_query_cache_entries_prm != NULL);
  assert (query_cache_size_in_pages_prm != NULL);
#if defined (ENABLE_UNUSED_FUNCTION)
  assert (max_plan_cache_clones_prm != NULL);
#endif /* ENABLE_UNUSED_FUNCTION */

#if defined (ENABLE_UNUSED_FUNCTION)
  if (max_plan_cache_entries_prm == NULL || max_plan_cache_clones_prm == NULL
      || query_cache_mode_prm == NULL || max_query_cache_entries_prm == NULL)
    {
      return;
    }
#else /* ENABLE_UNUSED_FUNCTION */
  if (max_plan_cache_entries_prm == NULL
      || query_cache_mode_prm == NULL || max_query_cache_entries_prm == NULL)
    {
      return;
    }
#endif /* !ENABLE_UNUSED_FUNCTION */

  if (PRM_GET_INT (max_plan_cache_entries_prm->value) == 0)
    {
      /* 0 means disable plan cache */
      (void) prm_set (max_plan_cache_entries_prm, "-1");
    }

  if (PRM_GET_INT (max_plan_cache_entries_prm->value) <= 0)
    {
      /* disable all by default */
#if defined (ENABLE_UNUSED_FUNCTION)
      (void) prm_set_default (max_plan_cache_clones_prm);
#endif /* ENABLE_UNUSED_FUNCTION */
      (void) prm_set_default (query_cache_mode_prm);
      (void) prm_set_default (max_query_cache_entries_prm);
      (void) prm_set_default (query_cache_size_in_pages_prm);
    }
  else
    {
#if defined (ENABLE_UNUSED_FUNCTION)
#if 1				/* block XASL clone feature because of new heap layer */
      if (PRM_GET_INT (max_plan_cache_clones_prm->value) >=
	  PRM_GET_INT (max_plan_cache_entries_prm->value))
#else
      if ((PRM_GET_INT (max_plan_cache_clones_prm->value) >=
	   PRM_GET_INT (max_plan_cache_entries_prm->value))
	  || (PRM_GET_INT (max_plan_cache_clones_prm->value) < 0))
#endif
	{
	  sprintf (newval, "%d",
		   PRM_GET_INT (max_plan_cache_entries_prm->value));
	  (void) prm_set (max_plan_cache_clones_prm, newval);
	}
#endif /* ENABLE_UNUSED_FUNCTION */
      if (PRM_GET_INT (query_cache_mode_prm->value) == 0)
	{
	  (void) prm_set_default (max_query_cache_entries_prm);
	  (void) prm_set_default (query_cache_size_in_pages_prm);
	}
      else
	{
	  if (PRM_GET_INT (max_query_cache_entries_prm->value) <= 0)
	    {
	      sprintf (newval, "%d",
		       PRM_GET_INT (max_plan_cache_entries_prm->value));
	      (void) prm_set (max_query_cache_entries_prm, newval);
	    }
	}
    }

  /* check HA related parameters */
  assert (ha_mode_prm != NULL);
  assert (replication_prm != NULL);
  assert (auto_restart_server_prm != NULL);
  if (ha_mode_prm == NULL || ha_server_state_prm == NULL
      || replication_prm == NULL || auto_restart_server_prm == NULL)
    {
      return;
    }

#if !defined (SERVER_MODE)
  /* reset to default 'active mode' for SA */
  (void) prm_set_default (ha_mode_prm);
  (void) prm_set (ha_server_state_prm, HA_SERVER_STATE_ACTIVE_STR);
  /* reset to default 'replication' for SA */
  (void) prm_set_default (replication_prm);
#else /* !SERVER_MODE */
  if (PRM_GET_INT (ha_mode_prm->value) != HA_MODE_OFF)
    {
      if (PRM_DEFAULT_VAL_USED (ha_server_state_prm->flag))
	{
	  sprintf (newval, "%s", HA_SERVER_STATE_STANDBY_STR);
	  prm_set (ha_server_state_prm, newval);
	}
      prm_set (replication_prm, "yes");
      prm_set (auto_restart_server_prm, "no");
    }
  else
    {
      sprintf (newval, "%s", HA_SERVER_STATE_ACTIVE_STR);
      prm_set (ha_server_state_prm, newval);
    }
#if defined (WINDOWS)
  /* reset to default 'replication' for WIDOWS */
  (void) prm_set_default (replication_prm);
#endif
#endif /* SERVER_MODE */

  if (!PRM_GET_BOOL (log_media_failure_support_prm->value))
    {
      prm_set (log_background_archiving_prm, "no");
    }

  if (ha_node_list_prm == NULL
      || PRM_DEFAULT_VAL_USED (ha_node_list_prm->flag))
    {
      if (GETHOSTNAME (host_name, sizeof (host_name)))
	{
	  strncpy (host_name, "localhost", sizeof (host_name) - 1);
	}

      snprintf (newval, sizeof (newval) - 1, "%s@%s", host_name, host_name);
      prm_set (ha_node_list_prm, newval);
    }

  return;
}
#else /* SA_MODE || SERVER_MODE */
static void
prm_tune_parameters (void)
{
  SYSPRM_PARAM *max_plan_cache_entries_prm;
  SYSPRM_PARAM *ha_node_list_prm;

  char newval[LINE_MAX];
  char host_name[MAXHOSTNAMELEN];

  /* Find the parameters that require tuning */
  max_plan_cache_entries_prm =
    prm_find (PRM_NAME_XASL_MAX_PLAN_CACHE_ENTRIES, NULL);
  ha_node_list_prm = prm_find (PRM_NAME_HA_NODE_LIST, NULL);

  assert (max_plan_cache_entries_prm != NULL);
  if (max_plan_cache_entries_prm == NULL)
    {
      return;
    }

  /* check Plan Cache and Query Cache parameters */
  if (PRM_GET_INT (max_plan_cache_entries_prm->value) == 0)
    {
      /* 0 means disable plan cache */
      (void) prm_set (max_plan_cache_entries_prm, "-1");
    }

  if (ha_node_list_prm == NULL
      || PRM_DEFAULT_VAL_USED (ha_node_list_prm->flag))
    {
      if (GETHOSTNAME (host_name, sizeof (host_name)))
	{
	  strncpy (host_name, "localhost", sizeof (host_name) - 1);
	}

      snprintf (newval, sizeof (newval) - 1, "%s@%s", host_name, host_name);
      prm_set (ha_node_list_prm, newval);
    }
}
#endif /* CS_MODE */

#if defined (CS_MODE)
/*
 * sysprm_tune_client_parameters - Sets the values of various client system
 *                          parameters depending on the value from the server
 *   return: none
 */
void
sysprm_tune_client_parameters (void)
{
  char data[LINE_MAX], *newval;

  /* those parameters should be same to them of server's */
  snprintf (data, LINE_MAX, "%s;%s;%s;%s;%s;%s;",
	    PRM_NAME_XASL_MAX_PLAN_CACHE_ENTRIES,
	    PRM_NAME_ORACLE_STYLE_EMPTY_STRING,
	    PRM_NAME_COMPAT_NUMERIC_DIVISION_SCALE,
	    PRM_NAME_SINGLE_BYTE_COMPARE,
	    PRM_NAME_HA_MODE, PRM_NAME_REPLICATION_MODE);
  if (sysprm_obtain_server_parameters (data, LINE_MAX) == NO_ERROR)
    {
      newval = strtok (data, ";");
      while (newval != NULL)
	{
	  prm_change (newval, false);
	  newval = strtok (NULL, ";");
	}
    }
}
#endif /* CS_MODE */

int
prm_get_master_port_id (void)
{
  return PRM_TCP_PORT_ID;
}

bool
prm_get_commit_on_shutdown (void)
{
  return PRM_COMMIT_ON_SHUTDOWN;
}

bool
prm_get_query_mode_sync (void)
{
  return PRM_QUERY_MODE_SYNC;
}

/*
 * prm_set_compound - Sets the values of various system parameters based on
 *                    the value of a "compound" parameter.
 *   return: none
 *   param(in): the compound parameter whose value has changed
 *   compound_param_values(in): an array of arrays that indicate the name of
 *                              the parameter to change and its new value.
 *                              NULL indicates resetting the parameter to its
 *                              default. The name of the parameter is the last
 *                              element of the array, 1 past the upper limit
 *                              of the compound parameter.
 *   values_count(in): the number of elements in the compound_param_values
 *                     array
 */
static void
prm_set_compound (SYSPRM_PARAM * param, const char **compound_param_values[],
		  const int values_count)
{
  int i = 0;
  const int param_value = *(int *) param->value;
  const int param_upper_limit = *(int *) param->upper_limit;

  assert (PRM_IS_INTEGER (param->flag) || PRM_IS_KEYWORD (param->flag));
  assert (0 == *(int *) param->lower_limit);
  assert (param_value <= param_upper_limit);

  for (i = 0; i < values_count; ++i)
    {
      const char *compound_param_name =
	compound_param_values[i][param_upper_limit + 1];
      const char *compound_param_value =
	compound_param_values[i][param_value];

      assert (compound_param_name != NULL);

      if (compound_param_value == NULL)
	{
	  prm_set_default (prm_find (compound_param_name, NULL));
	}
      else
	{
	  prm_set (prm_find (compound_param_name, NULL),
		   compound_param_value);
	}
    }
}
