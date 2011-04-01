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
 * utility.h : Message constant definitions used by the utility
 *
 */

#ifndef _UTILITY_H_
#define _UTILITY_H_

#include <config.h>
#include <stdio.h>

/*
 * UTILITY MESSAGE SETS
 */

/*
 * Message set id in the message catalog MSGCAT_CATALOG_UTILS.
 * These define the $set numbers within the catalog file of the message
 * for each utility.
 */
typedef enum
{
  MSGCAT_UTIL_SET_GENERIC = 1,
  MSGCAT_UTIL_SET_BACKUPDB = 2,
  MSGCAT_UTIL_SET_COPYDB = 3,
  MSGCAT_UTIL_SET_CREATEDB = 4,
  MSGCAT_UTIL_SET_DELETEDB = 6,
  MSGCAT_UTIL_SET_RENAMEDB = 7,
  MSGCAT_UTIL_SET_MASTER = 9,
  MSGCAT_UTIL_SET_OPTIMIZEDB = 10,
  MSGCAT_UTIL_SET_RESTOREDB = 11,
  MSGCAT_UTIL_SET_LOADDB = 12,
  MSGCAT_UTIL_SET_UNLOADDB = 13,
  MSGCAT_UTIL_SET_COMPACTDB = 14,
  MSGCAT_UTIL_SET_COMMDB = 15,
  MSGCAT_UTIL_SET_PATCHDB = 16,
  MSGCAT_UTIL_SET_ADDVOLDB = 17,
  MSGCAT_UTIL_SET_CHECKDB = 18,
  MSGCAT_UTIL_SET_SPACEDB = 19,
  MSGCAT_UTIL_SET_ESTIMATEDB_DATA = 20,
  MSGCAT_UTIL_SET_ESTIMATEDB_INDEX = 21,
  MSGCAT_UTIL_SET_INSTALLDB = 22,
  MSGCAT_UTIL_SET_MIGDB = 23,
  MSGCAT_UTIL_SET_DIAGDB = 24,
  MSGCAT_UTIL_SET_LOCKDB = 25,
  MSGCAT_UTIL_SET_KILLTRAN = 26,
  MSGCAT_UTIL_SET_ALTERDBHOST = 33,
  MSGCAT_UTIL_SET_LOADJAVA = 34,
  MSGCAT_UTIL_SET_REPLSERVER = 35,
  MSGCAT_UTIL_SET_REPLAGENT = 36,
  MSGCAT_UTIL_SET_PLANDUMP = 37,
  MSGCAT_UTIL_SET_PARAMDUMP = 38,
  MSGCAT_UTIL_SET_CHANGEMODE = 39,
  MSGCAT_UTIL_SET_COPYLOGDB = 40,
  MSGCAT_UTIL_SET_APPLYLOGDB = 41,
  MSGCAT_UTIL_SET_LOGFILEDUMP = 42,
  MSGCAT_UTIL_SET_STATDUMP = 43
} MSGCAT_UTIL_SET;

/* Message id in the set MSGCAT_UTIL_SET_GENERIC */
typedef enum
{
  MSGCAT_UTIL_GENERIC_BAD_DATABASE_NAME = 1,
  MSGCAT_UTIL_GENERIC_BAD_OUTPUT_FILE = 2,
  MSGCAT_UTIL_GENERIC_BAD_VOLUME_NAME = 6,
  MSGCAT_UTIL_GENERIC_VERSION = 9,
  MSGCAT_UTIL_GENERIC_ADMIN_USAGE = 10,
  MSGCAT_UTIL_GENERIC_SERVICE_INVALID_NAME = 12,
  MSGCAT_UTIL_GENERIC_SERVICE_INVALID_CMD = 13,
  MSGCAT_UTIL_GENERIC_SERVICE_PROPERTY_FAIL = 14,
  MSGCAT_UTIL_GENERIC_START_STOP_3S = 15,
  MSGCAT_UTIL_GENERIC_START_STOP_2S = 16,
  MSGCAT_UTIL_GENERIC_NOT_RUNNING_2S = 17,
  MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S = 18,
  MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_2S = 19,
  MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S = 20,
  MSGCAT_UTIL_GENERIC_RESULT = 21,
  MSGCAT_UTIL_GENERIC_MISS_ARGUMENT = 22,
  MSGCAT_UTIL_GENERIC_CUBRID_USAGE = 23,
  MSGCAT_UTIL_GENERIC_ARGS_OVER = 31,
  MSGCAT_UTIL_GENERIC_MISS_DBNAME = 32,
  MSGCAT_UTIL_GENRIC_REPL_NOT_SUPPORTED = 33
} MSGCAT_UTIL_GENERIC_MSG;

/* Message id in the set MSGCAT_UTIL_SET_DELETEDB */
typedef enum
{
  DELETEDB_MSG_USAGE = 60
} MSGCAT_DELETEDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_BACKUPDB */
typedef enum
{
  BACKUPDB_INVALID_THREAD_NUM_OPT = 30,
  BACKUPDB_MSG_USAGE = 60
} MSGCAT_BACKUPDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_RENAMEDB */
typedef enum
{
  RENAMEDB_MSG_USAGE = 60
} MSGCAT_RENAMEDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_OPTIMIZEDB */
typedef enum
{
  OPTIMIZEDB_MSG_USAGE = 60
} MSGCAT_OPTIMIZEDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_COMMDB */
typedef enum
{
  COMMDB_STRING1 = 21,
  COMMDB_STRING2 = 22,
  COMMDB_STRING3 = 23,
  COMMDB_STRING4 = 24,
  COMMDB_STRING5 = 25,
  COMMDB_STRING6 = 26,
  COMMDB_STRING7 = 27,
  COMMDB_STRING8 = 28,
  COMMDB_STRING9 = 29,
  COMMDB_STRING10 = 30,
  COMMDB_STRING11 = 31,
  COMMDB_STRING12 = 32,
  COMMDB_STRING13 = 33,
  COMMDB_STRING14 = 34,
} MSGCAT_COMMDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_COPYDB */
typedef enum
{
  COPYDB_MSG_IDENTICAL = 30,
  COPYDB_VOLEXT_PATH_INVALID = 31,
  COPYDB_VOLS_TOFROM_PATHS_FILE_INVALID = 32,
  COPYDB_MSG_USAGE = 60
} MSGCAT_COPYDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_CREATEDB */
typedef enum
{
  CREATEDB_MSG_MISSING_USER = 41,
  CREATEDB_MSG_UNKNOWN_CMD = 42,
  CREATEDB_MSG_BAD_OUTPUT = 43,
  CREATEDB_MSG_CREATING = 45,
  CREATEDB_MSG_FAILURE = 46,
  CREATEDB_MSG_BAD_USERFILE = 47,
  CREATEDB_MSG_FEW_PAGES = 48,
  CREATEDB_MSG_USAGE = 60
} MSGCAT_CREATEDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_MASTER */
typedef enum
{
  MASTER_MSG_DUPLICATE = 11,
  MASTER_MSG_STARTING = 12,
  MASTER_MSG_EXITING = 13,
  MASTER_MSG_NO_PARAMETERS = 15,
  MASTER_MSG_PROCESS_ERROR = 16,
  MASTER_MSG_SERVER_STATUS = 17,
  MASTER_MSG_SERVER_NOTIFIED = 18,
  MASTER_MSG_SERVER_NOT_FOUND = 19,
  MASTER_MSG_GOING_DOWN = 20,
  MASTER_MSG_REPL_SERVER_NOTIFIED = 21,
  MASTER_MSG_REPL_AGENT_NOTIFIED = 22
} MSGCAT_MASTER_MSG;

/* Message id in the set MSGCAT_UTIL_SET_RESTOREDB */
typedef enum
{
  RESTOREDB_MSG_BAD_DATE = 19,
  RESTOREDB_MSG_FAILURE = 20,
  RESTOREDB_MSG_USAGE = 60
} MSGCAT_RESTOREDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_ADDVOLDB */
typedef enum
{
  ADDVOLDB_MSG_BAD_NPAGES = 20,
  ADDVOLDB_MSG_BAD_PURPOSE = 21,
  ADDVOLDB_MSG_USAGE = 60
} MSGCAT_ADDVOLDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_CHECKDB */
typedef enum
{
  CHECKDB_MSG_INCONSISTENT = 20,
  CHECKDB_MSG_USAGE = 60
} MSGCAT_CHECKDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_SPACEDB */
typedef enum
{
  SPACEDB_OUTPUT_SUMMARY = 15,
  SPACEDB_OUTPUT_FORMAT = 16,
  SPACEDB_MSG_BAD_OUTPUT = 17,
  SPACEDB_OUTPUT_SUMMARY_TMP_VOL = 18,
  SPACEDB_OUTPUT_TITLE_PAGE = 19,
  SPACEDB_OUTPUT_TITLE_SIZE = 20,
  SPACEDB_MSG_USAGE = 60
} MSGCAT_SPACEDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_ESTIMATEDB_DATA */
typedef enum
{
  ESTIMATEDB_DATA_MSG_NPAGES = 15,
  ESTIMATEDB_DATA_MSG_USAGE = 60
} MSGCAT_ESTIMATEDB_DATA_MSG;

/* Message id in the set MSGCAT_UTIL_SET_ESTIMATEDB_INDEX */
typedef enum
{
  ESTIMATEDB_INDEX_BAD_KEYTYPE = 15,
  ESTIMATEDB_INDEX_BAD_KEYLENGTH = 16,
  ESTIMATEDB_INDEX_BAD_ARGUMENTS = 17,
  ESTIMATEDB_INDEX_MSG_NPAGES = 20,
  ESTIMATEDB_INDEX_MSG_BLT_NPAGES = 21,
  ESTIMATEDB_INDEX_MSG_BLT_WRS_NPAGES = 22,
  ESTIMATEDB_INDEX_MSG_INPUT = 23,
  ESTIMATEDB_INDEX_MSG_INSTANCES = 24,
  ESTIMATEDB_INDEX_MSG_NUMBER_KEYS = 25,
  ESTIMATEDB_INDEX_MSG_AVG_KEYSIZE = 26,
  ESTIMATEDB_INDEX_MSG_KEYTYPE = 27,
  ESTIMATEDB_INDEX_MSG_USAGE = 60
} MSGCAT_ESTIMATEDB_INDEX_MSG;

/* Message id in the set MSGCAT_UTIL_SET_DIAGDB */
typedef enum
{
  DIAGDB_MSG_USAGE = 60
} MSGCAT_DIAGDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_ALTERDBHOST */
typedef enum
{
  ALTERDBHOST_MSG_USAGE = 60
} MSGCAT_ALTERDBHOST_MSG;

/* Message id in the set MSGCAT_UTIL_SET_PATCHDB */
typedef enum
{
  PATCHDB_MSG_USAGE = 60
} MSGCAT_PATCHDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_INSTALLDB */
typedef enum
{
  INSTALLDB_MSG_USAGE = 60
} MSGCAT_INSTALLDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_LOCKDB */
typedef enum
{
  LOCKDB_MSG_BAD_OUTPUT = 15,
  LOCKDB_MSG_NOT_IN_STANDALONE = 59,
  LOCKDB_MSG_USAGE = 60
} MSGCAT_LOCKDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_KILLTRAN */
typedef enum
{
  KILLTRAN_MSG_MANY_ARGS = 20,
  KILLTRAN_MSG_DBA_PASSWORD = 21,
  KILLTRAN_MSG_TABLE_HEADER = 22,
  KILLTRAN_MSG_TABLE_UNDERSCORE = 23,
  KILLTRAN_MSG_TABLE_ENTRY = 24,
  KILLTRAN_MSG_NONE_TABLE_ENTRIES = 25,
  KILLTRAN_MSG_NO_MATCHES = 26,
  KILLTRAN_MSG_READY_TO_KILL = 27,
  KILLTRAN_MSG_VERIFY = 28,
  KILLTRAN_MSG_KILLING = 29,
  KILLTRAN_MSG_KILL_FAILED = 30,
  KILLTRAN_MSG_KILL_TIMEOUT = 31,
  KILLTRAN_MSG_NOT_IN_STANDALONE = 59,
  KILLTRAN_MSG_USAGE = 60
} MSGCAT_KILLTRAN_MSG;

/* Message id in the set MSGCAT_UTIL_SET_PLANDUMP */
typedef enum
{
  PLANDUMP_MSG_BAD_OUTPUT = 15,
  PLANDUMP_MSG_NOT_IN_STANDALONE = 59,
  PLANDUMP_MSG_USAGE = 60
} MSGCAT_PLANDUMP_MSG;

/* Message id in the set MSGCAT_UTIL_SET_LOADJAVA */
typedef enum
{
  LOADJAVA_ARG_FORCE_OVERWRITE = 5,
  LOADJAVA_ARG_FORCE_OVERWRITE_HELP = 6
} MSGCAT_LOADJAVA_MSG;

/* Message id in the set MSGCAT_UTIL_SET_REPLSERVER */
typedef enum
{
  REPL_SERVER_SOCK_ERROR = 11,
  REPL_SERVER_MEMORY_ERROR = 12,
  REPL_SERVER_IO_ERROR = 13,
  REPL_SERVER_INTERNAL_ERROR = 14,
  REPL_SERVER_CANT_OPEN_ACTIVE = 21,
  REPL_SERVER_CANT_OPEN_DBINFO = 22,
  REPL_SERVER_CANT_FIND_DBINFO = 23,
  REPL_SERVER_CANT_OPEN_CATALOG = 24,
  REPL_SERVER_INVALID_ARGUMENT = 25,
  REPL_SERVER_CANT_CONNECT_TO_MASTER = 26,
  REPL_SERVER_CANT_BIND_SOCKET = 27,
  REPL_SERVER_EXCEED_MAXIMUM_CONNECTION = 28,
  REPL_SERVER_CLIENT_CONNECTION_FAIL = 29,
  REPL_SERVER_REQ_QUEUE_IS_FULL = 30,
  REPL_SERVER_CANT_OPEN_ARCHIVE = 31,
  REPL_SERVER_CANT_READ_ARCHIVE = 32
} MSGCAT_REPL_SERVER_MSG;

/* Message id in the set MSGCAT_UTIL_SET_REPLAGENT */
typedef enum
{
  REPL_AGENT_SOCK_ERROR = 11,
  REPL_AGENT_MEMORY_ERROR = 12,
  REPL_AGENT_IO_ERROR = 13,
  REPL_AGENT_INTERNAL_ERROR = 14,
  REPL_AGENT_UNZIP_ERROR = 15,
  REPL_AGENT_QUERY_ERROR = 16,
  REPL_AGENT_REPL_SERVER_CONNECT = 21,
  REPL_AGENT_COPY_LOG_OPEN_ERROR = 22,
  REPL_AGENT_CANT_CREATE_ARCHIVE = 23,
  REPL_AGENT_CANT_OPEN_CATALOG = 24,
  REPL_AGENT_CANT_READ_TRAIL_LOG = 25,
  REPL_AGENT_CANT_CONNECT_TO_MASTER = 26,
  REPL_AGENT_CANT_LOGIN_TO_SLAVE = 27,
  REPL_AGENT_CANT_CONNECT_TO_SLAVE = 28,
  REPL_AGENT_SLAVE_STOP = 29,
  REPL_AGENT_REPLICATION_BROKEN = 30,
  REPL_AGENT_GET_ID_FAIL = 31,
  REPL_AGENT_GET_LOG_HDR_FAIL = 32,
  REPL_AGENT_GET_LOG_PAGE_FAIL = 33,
  REPL_AGENT_GET_PARARMETER_FAIL = 34,
  REPL_AGENT_CANT_LOGIN_TO_DIST = 35,
  REPL_AGENT_CANT_CONNECT_TO_DIST = 36,
  REPL_AGENT_TOO_MANY_SLAVES = 37,
  REPL_AGENT_SLAVE_HAS_NO_MASTER = 38,
  REPL_AGENT_TOO_MANY_MASTERS = 39,
  REPL_AGENT_INVALID_TRAIL_INFO = 40,
  REPL_AGENT_NEED_MORE_WS = 41,
  REPL_AGENT_CANT_READ_STATUS_LOG = 42,
  REPL_AGENT_RESTART_MESSAGE = 51,
  REPL_AGENT_RECORD_TYPE_ERROR = 52,
  REPL_AGENT_INFO_MSG = 53
} MSGCAT_REPL_AGENT_MSG;

/* Message id in the set MSGCAT_UTIL_SET_COMPACTDB */
typedef enum
{
  COMPACTDB_MSG_PASS1 = 11,
  COMPACTDB_MSG_PROCESSED = 12,
  COMPACTDB_MSG_PASS2 = 13,
  COMPACTDB_MSG_CLASS = 14,
  COMPACTDB_MSG_OID = 15,
  COMPACTDB_MSG_INSTANCES = 16,
  COMPACTDB_MSG_UPDATING = 17,
  COMPACTDB_MSG_REFOID = 18,
  COMPACTDB_MSG_CANT_TRANSFORM = 19,
  COMPACTDB_MSG_NO_HEAP = 20,
  COMPACTDB_MSG_CANT_UPDATE = 21,
  COMPACTDB_MSG_FAILED = 22,
  COMPACTDB_MSG_ALREADY_STARTED = 23,
  COMPACTDB_MSG_OUT_OF_RANGE_PAGES = 24,
  COMPACTDB_MSG_OUT_OF_RANGE_INSTANCE_LOCK_TIMEOUT = 25,
  COMPACTDB_MSG_TOTAL_OBJECTS = 26,
  COMPACTDB_MSG_FAILED_OBJECTS = 27,
  COMPACTDB_MSG_MODIFIED_OBJECTS = 28,
  COMPACTDB_MSG_BIG_OBJECTS = 29,
  COMPACTDB_MSG_REPR_DELETED = 30,
  COMPACTDB_MSG_REPR_CANT_DELETE = 31,
  COMPACTDB_MSG_ISOLATION_LEVEL_FAILURE = 32,
  COMPACTDB_MSG_FAILURE = 33,
  COMPACTDB_MSG_OUT_OF_RANGE_CLASS_LOCK_TIMEOUT = 34,
  COMPACTDB_MSG_LOCKED_CLASS = 35,
  COMPACTDB_MSG_INVALID_CLASS = 36,
  COMPACTDB_MSG_PROCESS_CLASS_ERROR = 37,
  COMPACTDB_MSG_NOTHING_TO_PROCESS = 38,
  COMPACTDB_MSG_INVALID_PARAMETERS = 39,
  COMPACTDB_MSG_UNKNOWN_CLASS_NAME = 40,
  COMPACTDB_MSG_RECLAIMED = 41,
  COMPACTDB_MSG_RECLAIM_SKIPPED = 42,
  COMPACTDB_MSG_RECLAIM_ERROR = 43,
  COMPACTDB_MSG_PASS3 = 44,
  COMPACTDB_MSG_HEAP_COMPACT_FAILED = 45,
  COMPACTDB_MSG_HEAP_COMPACT_SUCCEEDED = 46
} MSGCAT_COMPACTDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_UNLOADDB */
typedef enum
{
  UNLOADDB_MSG_INVALID_CACHED_PAGES = 41,
  UNLOADDB_MSG_INVALID_CACHED_PAGE_SIZE = 42,
  UNLOADDB_MSG_OBJECTS_DUMPED = 43,
  UNLOADDB_MSG_OBJECTS_FAILED = 46,
  UNLOADDB_MSG_INVALID_DIR_NAME = 47,
  UNLOADDB_MSG_LOG_LSA = 48
} MSGCAT_UNLOADDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_LOADDB */
typedef enum
{
  LOADDB_MSG_MISSING_DBNAME = 19,
  LOADDB_MSG_UNREACHABLE_LINE = 20,
  LOADDB_MSG_SIG1 = 21,
  LOADDB_MSG_INTERRUPTED_COMMIT = 22,
  LOADDB_MSG_INTERRUPTED_ABORT = 23,
  LOADDB_MSG_BAD_INFILE = 24,
  LOADDB_MSG_CHECKING = 25,
  LOADDB_MSG_ERROR_COUNT = 26,
  LOADDB_MSG_INSERTING = 27,
  LOADDB_MSG_OBJECT_COUNT = 28,
  LOADDB_MSG_DEFAULT_COUNT = 29,
  LOADDB_MSG_COMMITTING = 30,
  LOADDB_MSG_CLOSING = 31,
  LOADDB_MSG_LINE = 32,
  LOADDB_MSG_PARSE_ERROR = 33,
  LOADDB_MSG_MISSING_DOMAIN = 34,
  LOADDB_MSG_SET_DOMAIN_ERROR = 35,
  LOADDB_MSG_UNEXPECTED_SET = 36,
  LOADDB_MSG_UNEXPECTED_TYPE = 37,
  LOADDB_MSG_UNKNOWN_ATT_CLASS = 38,
  LOADDB_MSG_UNKNOWN_CLASS = 39,
  LOADDB_MSG_UNKNOWN_CLASS_ID = 40,
  LOADDB_MSG_UNAUTHORIZED_CLASS = 41,
  LOADDB_MSG_STOPPED = 42,
  LOADDB_MSG_UPDATE_WARNING = 43,
  LOADDB_MSG_REDEFINING_INSTANCE = 44,
  LOADDB_MSG_INSTANCE_DEFINED = 45,
  LOADDB_MSG_INSTANCE_RESERVED = 46,
  LOADDB_MSG_UNIQUE_VIOLATION_NULL = 47,
  LOADDB_MSG_INSTANCE_COUNT = 48,
  LOADDB_MSG_CLASS_TITLE = 49,
  LOADDB_MSG_PASSWORD_PROMPT = 50,
  LOADDB_MSG_UPDATING_STATISTICS = 51,
  LOADDB_MSG_STD_ERR = 52,
  LOADDB_MSG_LEX_ERROR = 53,
  LOADDB_MSG_SYNTAX_ERR = 54,
  LOADDB_MSG_SYNTAX_MISSING = 55,
  LOADDB_MSG_SYNTAX_IN = 56,
  LOADDB_MSG_INCOMPATIBLE_ARGS = 57,
  LOADDB_MSG_COMMITTED_INSTANCES = 58,
#ifndef LDR_OLD_LOADDB
  LOADDB_MSG_NOPT_ERR = 59,
#endif
  LOADDB_MSG_CONVERSION_ERROR = 60,
#ifndef DISABLE_TTA_FIX
  LOADDB_MSG_INSTANCE_COUNT_EX = 112,
#endif
  LOADDB_MSG_LAST_COMMITTED_LINE = 113,
  LOADDB_MSG_USAGE = 120
} MSGCAT_LOADDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_MIGDB */
typedef enum
{
  MIGDB_MSG_TEMPORARY_CLASS_OID = 1,
  MIGDB_MSG_CANT_PRINT_ELO = 2,
  MIGDB_MSG_CANT_ACCESS_LO = 3,
  MIGDB_MSG_CANT_OPEN_LO_FILE = 4,
  MIGDB_MSG_READ_ERROR = 5,
  MIGDB_MSG_WRITE_ERROR = 6,
  MIGDB_MSG_CANT_OPEN_ELO = 7,
  MIGDB_MSG_FH_HASH_FILENAME = 9,
  MIGDB_MSG_FH_NAME = 10,
  MIGDB_MSG_FH_SIZE = 11,
  MIGDB_MSG_FH_PAGE_SIZE = 12,
  MIGDB_MSG_FH_DATA_SIZE = 13,
  MIGDB_MSG_FH_ENTRY_SIZE = 14,
  MIGDB_MSG_FH_ENTRIES_PER_PAGE = 15,
  MIGDB_MSG_FH_CACHED_PAGES = 16,
  MIGDB_MSG_FH_NUM_ENTRIES = 17,
  MIGDB_MSG_FH_NUM_COLLISIONS = 18,
  MIGDB_MSG_FH_HASH_FILENAME2 = 19,
  MIGDB_MSG_FH_NEXT_OVERFLOW_ENTRY = 20,
  MIGDB_MSG_FH_KEY_TYPE = 21,
  MIGDB_MSG_FH_PAGE_HEADERS = 22,
  MIGDB_MSG_FH_LAST_PAGE_HEADER = 23,
  MIGDB_MSG_FH_FREE_PAGE_HEADER = 24,
  MIGDB_MSG_FH_PAGE_BITMAP = 25,
  MIGDB_MSG_FH_PAGE_BITMAP_SIZE = 26
} MSGCAT_MIGDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_PARAMDUMP */
typedef enum
{
  PARAMDUMP_MSG_BAD_OUTPUT = 11,
  PARAMDUMP_MSG_CLIENT_PARAMETER = 21,
  PARAMDUMP_MSG_SERVER_PARAMETER = 22,
  PARAMDUMP_MSG_STANDALONE_PARAMETER = 23,
  PARAMDUMP_MSG_USAGE = 60
} MSGCAT_PARAMDUMP_MSG;

/* Message id in the set MSGCAT_UTIL_SET_CHANGEMODE */
typedef enum
{
  CHANGEMODE_MSG_BAD_MODE = 11,
  CHANGEMODE_MSG_CANNOT_CHANGE = 12,
  CHANGEMODE_MSG_DBA_PASSWORD = 21,
  CHANGEMODE_MSG_SERVER_MODE = 22,
  CHANGEMODE_MSG_SERVER_MODE_CHANGED = 23,
  CHANGEMODE_MSG_NOT_HA_MODE = 24,
  CHANGEMODE_MSG_HA_NOT_SUPPORT = 58,
  CHANGEMODE_MSG_NOT_IN_STANDALONE = 59,
  CHANGEMODE_MSG_USAGE = 60
} MSGCAT_CHANGEMODE_MSG;

/* Message id in the set MSGCAT_UTIL_SET_COPYLOGDB */
typedef enum
{
  COPYLOGDB_MSG_BAD_MODE = 11,
  COPYLOGDB_MSG_DBA_PASSWORD = 21,
  COPYLOGDB_MSG_NOT_HA_MODE = 22,
  COPYLOGDB_MSG_HA_NOT_SUPPORT = 58,
  COPYLOGDB_MSG_NOT_IN_STANDALONE = 59,
  COPYLOGDB_MSG_USAGE = 60
} MSGCAT_COPYLOGDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_APPLYLOGDB */
typedef enum
{
  APPLYLOGDB_MSG_DBA_PASSWORD = 21,
  APPLYLOGDB_MSG_NOT_HA_MODE = 22,
  APPLYLOGDB_MSG_HA_NOT_SUPPORT = 58,
  APPLYLOGDB_MSG_NOT_IN_STANDALONE = 59,
  APPLYLOGDB_MSG_USAGE = 60
} MSGCAT_APPLYLOGDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_STATMDUMP */
typedef enum
{
  STATDUMP_MSG_BAD_OUTPUT = 11,
  STATDUMP_MSG_NOT_IN_STANDALONE = 59,
  STATDUMP_MSG_USAGE = 60
} MSGCAT_STATDUMP_MSG;

typedef void *DSO_HANDLE;

typedef enum
{
  CREATEDB,
  RENAMEDB,
  COPYDB,
  DELETEDB,
  BACKUPDB,
  RESTOREDB,
  ADDVOLDB,
  SPACEDB,
  LOCKDB,
  KILLTRAN,
  OPTIMIZEDB,
  INSTALLDB,
  DIAGDB,
  PATCHDB,
  CHECKDB,
  ALTERDBHOST,
  PLANDUMP,
  ESTIMATE_DATA,
  ESTIMATE_INDEX,
  LOADDB,
  UNLOADDB,
  COMPACTDB,
  PARAMDUMP,
  STATDUMP,
  CHANGEMODE,
  COPYLOGDB,
  APPLYLOGDB,
  LOGFILEDUMP
} UTIL_INDEX;

typedef enum
{
  SA_ONLY,
  CS_ONLY,
  SA_CS
} UTIL_MODE;

typedef enum
{
  ARG_INTEGER,
  ARG_STRING,
  ARG_BOOLEAN
} UTIL_ARG_TYPE;

typedef struct option GETOPT_LONG;

typedef struct
{
  int arg_ch;
  union
  {
    int value_type;		/* if arg_ch is not OPTION_STRING_TABLE */
    int num_strings;		/* if arg_ch is OPTION_STRING_TABLE */
  } value_info;
  union
  {
    int i;
    void *p;
  } arg_value;
} UTIL_ARG_MAP;

typedef struct
{
  int utility_index;
  int utility_type;
  int need_args_num;
  const char *utility_name;
  const char *function_name;
  GETOPT_LONG *getopt_long;
  UTIL_ARG_MAP *arg_map;
} UTIL_MAP;

#define OPTION_STRING_TABLE                     10000

#if defined(WINDOWS)
#define UTIL_EXE_EXT            ".exe"
#else
#define UTIL_EXE_EXT            ""
#endif

#if defined(WINDOWS)
#define UTIL_WIN_SERVICE_CONTROLLER_NAME	"ctrlService" UTIL_EXE_EXT
#endif

#define UTIL_MASTER_NAME        "cub_master" UTIL_EXE_EXT
#define UTIL_COMMDB_NAME        "cub_commdb" UTIL_EXE_EXT
#define UTIL_CUBRID_NAME        "cub_server" UTIL_EXE_EXT
#define UTIL_BROKER_NAME        "cubrid_broker" UTIL_EXE_EXT
#define UTIL_MONITOR_NAME       "broker_monitor" UTIL_EXE_EXT
#define UTIL_CUB_AUTO_NAME      "cub_auto" UTIL_EXE_EXT
#define UTIL_CUB_JS_NAME        "cub_js" UTIL_EXE_EXT
#define UTIL_REPL_SERVER_NAME   "repl_server" UTIL_EXE_EXT
#define UTIL_REPL_AGENT_NAME    "repl_agent" UTIL_EXE_EXT
#define UTIL_ADMIN_NAME         "cub_admin" UTIL_EXE_EXT
#define UTIL_SQLX_NAME          "sqlx" UTIL_EXE_EXT
#define UTIL_CSQL_NAME          "csql" UTIL_EXE_EXT
#define UTIL_CUBRID_REL_NAME    "cubrid_rel" UTIL_EXE_EXT
#define UTIL_OLD_COMMDB_NAME    "commdb" UTIL_EXE_EXT
#define UTIL_CUBRID             "cubrid" UTIL_EXE_EXT

#define PROPERTY_ON             "on"
#define PROPERTY_OFF            "off"

#if defined(WINDOWS)
#define PRINT_SERVICE_NAME	"cubrid service"
#endif

#define PRINT_MASTER_NAME       "cubrid master"
#define PRINT_SERVER_NAME       "cubrid server"
#define PRINT_BROKER_NAME       "cubrid broker"
#define PRINT_MANAGER_NAME      "cubrid manager server"
#define PRINT_REPL_NAME         "cubrid replication"
#define PRINT_REPL_SERVER_NAME  "cubrid replication server"
#define PRINT_REPL_AGENT_NAME   "cubrid replication agent"
#define PRINT_HEARTBEAT_NAME    "cubrid heartbeat"

#define PRINT_CMD_SERVER        "server"
#define PRINT_CMD_START         "start"
#define PRINT_CMD_STOP          "stop"
#define PRINT_CMD_STATUS        "status"
#define PRINT_CMD_DEACTIVATE    "deact"
#define PRINT_CMD_ACTIVATE      "act"
#define PRINT_CMD_DEREG         "deregister"
#define PRINT_CMD_LIST          "list"
#define PRINT_CMD_RELOAD        "reload"

#define PRINT_RESULT_SUCCESS    "success"
#define PRINT_RESULT_FAIL       "fail"

#define CHECK_SERVER            "Server"
#define CHECK_HA_SERVER         "HA-Server"
#define CHECK_REPL_SERVER       "repl_server"
#define CHECK_REPL_AGENT        "repl_agent"

#define COMMDB_SERVER_STOP      "-S"
#define COMMDB_SERVER_STATUS    "-P"
#define COMMDB_REPL_STATUS      "-R"
#define COMMDB_ALL_STATUS       "-O"
#define COMMDB_ALL_STOP         "-A"
#define COMMDB_HA_DEREG         "-D"
#define COMMDB_HA_NODE_LIST     "-N"
#define COMMDB_HA_PROC_LIST     "-L"
#define COMMDB_HA_RELOAD        "-F"
#define COMMDB_HA_DEACTIVATE    "-U"
#define COMMDB_HA_ACTIVATE      "-T"

#define MASK_ALL                0xFF
#define MASK_SERVICE            0x01
#define MASK_SERVER             0x02
#define MASK_BROKER             0x04
#define MASK_MANAGER            0x08
#define MASK_REPL               0x10
#define MASK_ADMIN              0x20
#define MASK_HEARTBEAT          0x40

/* utility option list */
#define UTIL_OPTION_CREATEDB                    "createdb"
#define UTIL_OPTION_RENAMEDB                    "renamedb"
#define UTIL_OPTION_COPYDB                      "copydb"
#define UTIL_OPTION_DELETEDB                    "deletedb"
#define UTIL_OPTION_BACKUPDB                    "backupdb"
#define UTIL_OPTION_RESTOREDB                   "restoredb"
#define UTIL_OPTION_ADDVOLDB                    "addvoldb"
#define UTIL_OPTION_SPACEDB                     "spacedb"
#define UTIL_OPTION_LOCKDB                      "lockdb"
#define UTIL_OPTION_KILLTRAN                    "killtran"
#define UTIL_OPTION_OPTIMIZEDB                  "optimizedb"
#define UTIL_OPTION_INSTALLDB                   "installdb"
#define UTIL_OPTION_DIAGDB                      "diagdb"
#define UTIL_OPTION_PATCHDB                     "emergency_patchlog"
#define UTIL_OPTION_CHECKDB                     "checkdb"
#define UTIL_OPTION_ALTERDBHOST                 "alterdbhost"
#define UTIL_OPTION_PLANDUMP                    "plandump"
#define UTIL_OPTION_ESTIMATE_DATA               "estimate_data"
#define UTIL_OPTION_ESTIMATE_INDEX              "estimate_index"
#define UTIL_OPTION_LOADDB                      "loaddb"
#define UTIL_OPTION_UNLOADDB                    "unloaddb"
#define UTIL_OPTION_COMPACTDB                   "compactdb"
#define UTIL_OPTION_PARAMDUMP                   "paramdump"
#define UTIL_OPTION_STATDUMP                    "statdump"
#define UTIL_OPTION_CHANGEMODE                  "changemode"
#define UTIL_OPTION_COPYLOGDB                   "copylogdb"
#define UTIL_OPTION_APPLYLOGDB                  "applylogdb"
#define UTIL_OPTION_LOGFILEDUMP                 "logfiledump"

/* createdb option list */
#define CREATE_PAGES_S                          'p'
#define CREATE_PAGES_L                          "pages"
#define CREATE_COMMENT_S                        10102
#define CREATE_COMMENT_L                        "comment"
#define CREATE_FILE_PATH_S                      'F'
#define CREATE_FILE_PATH_L                      "file-path"
#define CREATE_LOG_PATH_S                       'L'
#define CREATE_LOG_PATH_L                       "log-path"
#define CREATE_SERVER_NAME_S                    10105
#define CREATE_SERVER_NAME_L                    "server-name"
#define CREATE_REPLACE_S                        'r'
#define CREATE_REPLACE_L                        "replace"
#define CREATE_MORE_VOLUME_FILE_S               10107
#define CREATE_MORE_VOLUME_FILE_L               "more-volume-file"
#define CREATE_USER_DEFINITION_FILE_S           10108
#define CREATE_USER_DEFINITION_FILE_L           "user-definition-file"
#define CREATE_CSQL_INITIALIZATION_FILE_S       10109
#define CREATE_CSQL_INITIALIZATION_FILE_L       "csql-initialization-file"
#define CREATE_OUTPUT_FILE_S                    'o'
#define CREATE_OUTPUT_FILE_L                    "output-file"
#define CREATE_VERBOSE_S                        'v'
#define CREATE_VERBOSE_L                        "verbose"
#define CREATE_CHARSET_S                        10112
#define CREATE_CHARSET_L                        "charset"
#define CREATE_LOG_PAGE_COUNT_S                 'l'
#define CREATE_LOG_PAGE_COUNT_L                 "log-page-count"
#define CREATE_PAGE_SIZE_S                      's'
#define CREATE_PAGE_SIZE_L                      "page-size"
#define CREATE_LOG_PAGE_SIZE_S                  10113
#define CREATE_LOG_PAGE_SIZE_L                  "log-page-size"

/* renamedb option list */
#define RENAME_EXTENTED_VOLUME_PATH_S           'E'
#define RENAME_EXTENTED_VOLUME_PATH_L           "extended-volume-path"
#define RENAME_CONTROL_FILE_S                   'i'
#define RENAME_CONTROL_FILE_L                   "control-file"
#define RENAME_DELETE_BACKUP_S                  'd'
#define RENAME_DELETE_BACKUP_L                  "delete-backup"

/* copydb option list */
#define COPY_SERVER_NAME_S                      10300
#define COPY_SERVER_NAME_L                      "server-name"
#define COPY_FILE_PATH_S                        'F'
#define COPY_FILE_PATH_L                        "file-path"
#define COPY_LOG_PATH_S                         'L'
#define COPY_LOG_PATH_L                         "log-path"
#define COPY_EXTENTED_VOLUME_PATH_S             'E'
#define COPY_EXTENTED_VOLUME_PATH_L             "extended-volume-path"
#define COPY_CONTROL_FILE_S                     'i'
#define COPY_CONTROL_FILE_L                     "control-file"
#define COPY_REPLACE_S                          'r'
#define COPY_REPLACE_L                          "replace"
#define COPY_DELETE_SOURCE_S                    'd'
#define COPY_DELETE_SOURCE_L                    "delete-source"

/* deletedb option list */
#define DELETE_OUTPUT_FILE_S                    'o'
#define DELETE_OUTPUT_FILE_L                    "output-file"
#define DELETE_DELETE_BACKUP_S                  'd'
#define DELETE_DELETE_BACKUP_L                  "delete-backup"

/* backupdb option list */
#define BACKUP_DESTINATION_PATH_S               'D'
#define BACKUP_DESTINATION_PATH_L		"destination-path"
#define BACKUP_REMOVE_ARCHIVE_S                 'r'
#define BACKUP_REMOVE_ARCHIVE_L                 "remove-archive"
#define BACKUP_LEVEL_S                          'l'
#define BACKUP_LEVEL_L                          "level"
#define BACKUP_OUTPUT_FILE_S                    'o'
#define BACKUP_OUTPUT_FILE_L                    "output-file"
#define BACKUP_SA_MODE_S                        'S'
#define BACKUP_SA_MODE_L                        "SA-mode"
#define BACKUP_CS_MODE_S                        'C'
#define BACKUP_CS_MODE_L                        "CS-mode"
#define BACKUP_NO_CHECK_S                       10506
#define BACKUP_NO_CHECK_L                       "no-check"
#define BACKUP_THREAD_COUNT_S                   't'
#define BACKUP_THREAD_COUNT_L                   "thread-count"
#define BACKUP_COMPRESS_S                       'z'
#define BACKUP_COMPRESS_L                       "compress"
#define BACKUP_EXCEPT_ACTIVE_LOG_S              'e'
#define BACKUP_EXCEPT_ACTIVE_LOG_L              "except-active-log"
#define BACKUP_SAFE_PAGE_ID_S                    10510
#define BACKUP_SAFE_PAGE_ID_L                    "safe-page-id"

/* restoredb option list */
#define RESTORE_UP_TO_DATE_S                    'd'
#define RESTORE_UP_TO_DATE_L                    "up-to-date"
#define RESTORE_LIST_S                          10601
#define RESTORE_LIST_L                          "list"
#define RESTORE_BACKUP_FILE_PATH_S              'B'
#define RESTORE_BACKUP_FILE_PATH_L              "backup-file-path"
#define RESTORE_LEVEL_S                         'l'
#define RESTORE_LEVEL_L                         "level"
#define RESTORE_PARTIAL_RECOVERY_S              'p'
#define RESTORE_PARTIAL_RECOVERY_L              "partial-recovery"
#define RESTORE_OUTPUT_FILE_S                   'o'
#define RESTORE_OUTPUT_FILE_L                   "output-file"
#define RESTORE_REPLICATION_MODE_S              'r'
#define RESTORE_REPLICATION_MODE_L              "replication-mode"
#define RESTORE_USE_DATABASE_LOCATION_PATH_S    'u'
#define RESTORE_USE_DATABASE_LOCATION_PATH_L    "use-database-location-path"

/* addvoldb option list */
#define ADDVOL_VOLUME_NAME_S                    'n'
#define ADDVOL_VOLUME_NAME_L                    "volume-name"
#define ADDVOL_FILE_PATH_S                      'F'
#define ADDVOL_FILE_PATH_L                      "file-path"
#define ADDVOL_COMMENT_S                        10702
#define ADDVOL_COMMENT_L                        "comment"
#define ADDVOL_PURPOSE_S                        'p'
#define ADDVOL_PURPOSE_L                        "purpose"
#define ADDVOL_SA_MODE_S                        'S'
#define ADDVOL_SA_MODE_L                        "SA-mode"
#define ADDVOL_CS_MODE_S                        'C'
#define ADDVOL_CS_MODE_L                        "CS-mode"

/* spacedb option list */
#define SPACE_OUTPUT_FILE_S                     'o'
#define SPACE_OUTPUT_FILE_L                     "output-file"
#define SPACE_SA_MODE_S                         'S'
#define SPACE_SA_MODE_L                         "SA-mode"
#define SPACE_CS_MODE_S                         'C'
#define SPACE_CS_MODE_L                         "CS-mode"
#define SPACE_SIZE_UNIT_S                       10803
#define SPACE_SIZE_UNIT_L                       "size-unit"

/* lockdb option list */
#define LOCK_OUTPUT_FILE_S                      'o'
#define LOCK_OUTPUT_FILE_L                      "output-file"

/* optimizedb option list */
#define OPTIMIZE_CLASS_NAME_S                   'n'
#define OPTIMIZE_CLASS_NAME_L                   "class-name"

/* installdb option list */
#define INSTALL_SERVER_NAME_S                   11100
#define INSTALL_SERVER_NAME_L                   "server-name"
#define INSTALL_FILE_PATH_S                     'F'
#define INSTALL_FILE_PATH_L                     "file-path"
#define INSTALL_LOG_PATH_S                      'L'
#define INSTALL_LOG_PATH_L                      "log-path"

/* diagdb option list */
#define DIAG_DUMP_TYPE_S                        'd'
#define DIAG_DUMP_TYPE_L                        "dump-type"
#define DIAG_DUMP_RECORDS_S                     11201
#define DIAG_DUMP_RECORDS_L                     "dump-records"

/* patch option list */
#define PATCH_RECREATE_LOG_S                    'r'
#define PATCH_RECREATE_LOG_L                    "recreate-log"

/* alterdbhost option list */
#define ALTERDBHOST_HOST_S                      'h'
#define ALTERDBHOST_HOST_L                      "host"

/* checkdb option list */
#define CHECK_SA_MODE_S                         'S'
#define CHECK_SA_MODE_L                         "SA-mode"
#define CHECK_CS_MODE_S                         'C'
#define CHECK_CS_MODE_L                         "CS-mode"
#define CHECK_REPAIR_S                          'r'
#define CHECK_REPAIR_L                          "repair"

/* plandump option list */
#define PLANDUMP_DROP_S			        'd'
#define PLANDUMP_DROP_L                         "drop"
#define PLANDUMP_OUTPUT_FILE_S		        'o'
#define PLANDUMP_OUTPUT_FILE_L                  "output-file"

/* killtran option list */
#define KILLTRAN_KILL_TRANSACTION_INDEX_S       'i'
#define KILLTRAN_KILL_TRANSACTION_INDEX_L       "kill-transaction-index"
#define KILLTRAN_KILL_USER_NAME_S               11701
#define KILLTRAN_KILL_USER_NAME_L               "kill-user-name"
#define KILLTRAN_KILL_HOST_NAME_S               11702
#define KILLTRAN_KILL_HOST_NAME_L               "kill-host-name"
#define KILLTRAN_KILL_PROGRAM_NAME_S            11703
#define KILLTRAN_KILL_PROGRAM_NAME_L            "kill-program-name"
#define KILLTRAN_DBA_PASSWORD_S                 'p'
#define KILLTRAN_DBA_PASSWORD_L                 "dba-password"
#define KILLTRAN_DISPLAY_INFORMATION_S          'd'
#define KILLTRAN_DISPLAY_INFORMATION_L          "display-information"
#define KILLTRAN_FORCE_S                        'f'
#define KILLTRAN_FORCE_L                        "force"

/* loaddb option list */
#define LOAD_USER_S                             'u'
#define LOAD_USER_L                             "user"
#define LOAD_PASSWORD_S                         'p'
#define LOAD_PASSWORD_L                         "password"
#define LOAD_CHECK_ONLY_S                       11802
#define LOAD_CHECK_ONLY_L                       "data-file-check-only"
#define LOAD_LOAD_ONLY_S                        'l'
#define LOAD_LOAD_ONLY_L                        "load-only"
#define LOAD_ESTIMATED_SIZE_S                   11804
#define LOAD_ESTIMATED_SIZE_L                   "estimated-size"
#define LOAD_VERBOSE_S                          'v'
#define LOAD_VERBOSE_L                          "verbose"
#define LOAD_NO_STATISTICS_S                    11806
#define LOAD_NO_STATISTICS_L                    "no-statistics"
#define LOAD_PERIODIC_COMMIT_S                  'c'
#define LOAD_PERIODIC_COMMIT_L                  "periodic-commit"
#define LOAD_NO_OID_S                           11808
#define LOAD_NO_OID_L                           "no-oid"
#define LOAD_SCHEMA_FILE_S                      's'
#define LOAD_SCHEMA_FILE_L                      "schema-file"
#define LOAD_INDEX_FILE_S                       'i'
#define LOAD_INDEX_FILE_L                       "index-file"
#define LOAD_IGNORE_LOGGING_S                   11811
#define LOAD_IGNORE_LOGGING_L                   "no-logging"
#define LOAD_DATA_FILE_S                        'd'
#define LOAD_DATA_FILE_L                        "data-file"
#define LOAD_ERROR_CONTROL_FILE_S               'e'
#define LOAD_ERROR_CONTROL_FILE_L               "error-control-file"
#define LOAD_IGNORE_CLASS_S                     11814
#define LOAD_IGNORE_CLASS_L                     "ignore-class-file"
#define LOAD_SA_MODE_S                          'S'
#define LOAD_SA_MODE_L                          "SA-mode"
#define LOAD_CS_MODE_S                          11815
#define LOAD_CS_MODE_L                          "CS-hidden"

/* unloaddb option list */
#define UNLOAD_INPUT_CLASS_FILE_S               'i'
#define UNLOAD_INPUT_CLASS_FILE_L               "input-class-file"
#define UNLOAD_INCLUDE_REFERENCE_S              11901
#define UNLOAD_INCLUDE_REFERENCE_L              "include-reference"
#define UNLOAD_INPUT_CLASS_ONLY_S               11902
#define UNLOAD_INPUT_CLASS_ONLY_L               "input-class-only"
#define UNLOAD_LO_COUNT_S                       11903
#define UNLOAD_LO_COUNT_L                       "lo-count"
#define UNLOAD_ESTIMATED_SIZE_S                 11904
#define UNLOAD_ESTIMATED_SIZE_L                 "estimated-size"
#define UNLOAD_CACHED_PAGES_S                   11905
#define UNLOAD_CACHED_PAGES_L                   "cached-pages"
#define UNLOAD_OUTPUT_PATH_S                    'O'
#define UNLOAD_OUTPUT_PATH_L                    "output-path"
#define UNLOAD_SCHEMA_ONLY_S                    's'
#define UNLOAD_SCHEMA_ONLY_L                    "schema-only"
#define UNLOAD_DATA_ONLY_S                      'd'
#define UNLOAD_DATA_ONLY_L                      "data-only"
#define UNLOAD_OUTPUT_PREFIX_S                  11909
#define UNLOAD_OUTPUT_PREFIX_L                  "output-prefix"
#define UNLOAD_HASH_FILE_S                      11910
#define UNLOAD_HASH_FILE_L                      "hash-file"
#define UNLOAD_VERBOSE_S                        'v'
#define UNLOAD_VERBOSE_L                        "verbose"
#define UNLOAD_USE_DELIMITER_S                  11912
#define UNLOAD_USE_DELIMITER_L                  "use-delimiter"
#define UNLOAD_SA_MODE_S                        'S'
#define UNLOAD_SA_MODE_L                        "SA-mode"
#define UNLOAD_CS_MODE_S                        'C'
#define UNLOAD_CS_MODE_L                        "CS-mode"

/* compactdb option list */
#define COMPACT_VERBOSE_S                       'v'
#define COMPACT_VERBOSE_L                       "verbose"
#define COMPACT_INPUT_CLASS_FILE_S              'i'
#define COMPACT_INPUT_CLASS_FILE_L              "input-class-file"
#define COMPACT_CS_MODE_S			'C'
#define COMPACT_CS_MODE_L			"CS-mode"
#define COMPACT_SA_MODE_S			'S'
#define COMPACT_SA_MODE_L			"SA-mode"
#define COMPACT_PAGES_COMMITED_ONCE_S		'p'
#define COMPACT_PAGES_COMMITED_ONCE_L		"pages-commited-once"
#define COMPACT_DELETE_OLD_REPR_S		'd'
#define COMPACT_DELETE_OLD_REPR_L		"delete-old-repr"
#define COMPACT_INSTANCE_LOCK_TIMEOUT_S		'I'
#define COMPACT_INSTANCE_LOCK_TIMEOUT_L		"Instance-lock-timeout"
#define COMPACT_CLASS_LOCK_TIMEOUT_S		'c'
#define COMPACT_CLASS_LOCK_TIMEOUT_L		"class-lock-timeout"

/* sqlx option list */
#define CSQL_SA_MODE_S                          'S'
#define CSQL_SA_MODE_L                          "SA-mode"
#define CSQL_CS_MODE_S                          'C'
#define CSQL_CS_MODE_L                          "CS-mode"
#define CSQL_USER_S                             'u'
#define CSQL_USER_L                             "user"
#define CSQL_PASSWORD_S                         'p'
#define CSQL_PASSWORD_L                         "password"
#define CSQL_ERROR_CONTINUE_S                   'e'
#define CSQL_ERROR_CONTINUE_L                   "error-continue"
#define CSQL_INPUT_FILE_S                       'i'
#define CSQL_INPUT_FILE_L                       "input-file"
#define CSQL_OUTPUT_FILE_S                      'o'
#define CSQL_OUTPUT_FILE_L                      "output-file"
#define CSQL_SINGLE_LINE_S                      's'
#define CSQL_SINGLE_LINE_L                      "single-line"
#define CSQL_COMMAND_S                          'c'
#define CSQL_COMMAND_L                          "command"
#define CSQL_LINE_OUTPUT_S                      'l'
#define CSQL_LINE_OUTPUT_L                      "line-output"
#define CSQL_READ_ONLY_S                        'r'
#define CSQL_READ_ONLY_L                        "read-only"
#define CSQL_NO_AUTO_COMMIT_S                   12010
#define CSQL_NO_AUTO_COMMIT_L                   "no-auto-commit"
#define CSQL_NO_PAGER_S                         12011
#define CSQL_NO_PAGER_L                         "no-pager"
#define CSQL_SYSADM_S                           12012
#define CSQL_SYSADM_L                           "sysadm"

#define COMMDB_SERVER_LIST_S                    'P'
#define COMMDB_SERVER_LIST_L                    "server-list"
#define COMMDB_REPL_LIST_S                      'R'
#define COMMDB_REPL_LIST_L                      "repl-list"
#define COMMDB_ALL_LIST_S                       'O'
#define COMMDB_ALL_LIST_L                       "all-list"
#define COMMDB_SHUTDOWN_SERVER_S                'S'
#define COMMDB_SHUTDOWN_SERVER_L                "shutdown-server"
#define COMMDB_SHUTDOWN_REPL_SERVER_S           'K'
#define COMMDB_SHUTDOWN_REPL_SERVER_L           "shutdown-repl-server"
#define COMMDB_SHUTDOWN_REPL_AGENT_S            'k'
#define COMMDB_SHUTDOWN_REPL_AGENT_L            "shutdown-repl-agent"
#define COMMDB_SHUTDOWN_ALL_S                   'A'
#define COMMDB_SHUTDOWN_ALL_L                   "shutdown-all"
#define COMMDB_HOST_S                           'h'
#define COMMDB_HOST_L                           "host"
#define COMMDB_SERVER_MODE_S                    'c'
#define COMMDB_SERVER_MODE_L                    "server-mode"
#define COMMDB_HA_NODE_LIST_S                   'N'
#define COMMDB_HA_NODE_LIST_L                   "node-list"
#define COMMDB_HA_PROCESS_LIST_S                'L'
#define COMMDB_HA_PROCESS_LIST_L                "process-list"
#define COMMDB_DEREG_HA_PROCESS_S               'D'
#define COMMDB_DEREG_HA_PROCESS_L               "dereg-process"
#define COMMDB_RECONFIG_HEARTBEAT_S             'F'
#define COMMDB_RECONFIG_HEARTBEAT_L             "reconfig-node-list"
#define COMMDB_DEACTIVATE_HEARTBEAT_S           'U'
#define COMMDB_DEACTIVATE_HEARTBEAT_L           "deactivate-heartbeat"
#define COMMDB_ACTIVATE_HEARTBEAT_S             'T'
#define COMMDB_ACTIVATE_HEARTBEAT_L             "activate-heartbeat"
#define COMMDB_VERBOSE_OUTPUT_S                 'V'
#define COMMDB_VERBOSE_OUTPUT_L	                "verbose"

/* paramdump option list */
#define PARAMDUMP_OUTPUT_FILE_S                 'o'
#define PARAMDUMP_OUTPUT_FILE_L                 "output-file"
#define PARAMDUMP_BOTH_S                        'b'
#define PARAMDUMP_BOTH_L                        "both"
#define PARAMDUMP_SA_MODE_S                     'S'
#define PARAMDUMP_SA_MODE_L                     "SA-mode"
#define PARAMDUMP_CS_MODE_S                     'C'
#define PARAMDUMP_CS_MODE_L                     "CS-mode"

/* statdump option list */
#define STATDUMP_OUTPUT_FILE_S                  'o'
#define STATDUMP_OUTPUT_FILE_L                  "output-file"
#define STATDUMP_INTERVAL_S                     'i'
#define STATDUMP_INTERVAL_L                     "interval"
#define STATDUMP_CUMULATIVE_S                   'c'
#define STATDUMP_CUMULATIVE_L                   "cumulative"

/* changemode option list */
#define CHANGEMODE_MODE_S                       'm'
#define CHANGEMODE_MODE_L                       "mode"
#define CHANGEMODE_WAIT_S                       'w'
#define CHANGEMODE_WAIT_L                       "wait"
#define CHANGEMODE_FORCE_S                      'f'
#define CHANGEMODE_FORCE_L                      "force"

/* copylogdb option list */
#define COPYLOG_LOG_PATH_S                      'L'
#define COPYLOG_LOG_PATH_L                      "log-path"
#define COPYLOG_MODE_S                          'm'
#define COPYLOG_MODE_L                          "mode"

/* applylogdb option list */
#define APPLYLOG_LOG_PATH_S                     'L'
#define APPLYLOG_LOG_PATH_L                     "log-path"
#define APPLYLOG_TEST_LOG_S                     't'
#define APPLYLOG_TEST_LOG_L                     "test-log"
#define APPLYLOG_MAX_MEM_SIZE_S			12401
#define APPLYLOG_MAX_MEM_SIZE_L			"max-mem-size"

#define VERSION_S                               20000
#define VERSION_L                               "version"

#if defined(WINDOWS)
#define LIB_UTIL_CS_NAME                "cubridcs.dll"
#define LIB_UTIL_SA_NAME                "cubridsa.dll"
#else
#define LIB_UTIL_CS_NAME                "libcubridcs.so"
#define LIB_UTIL_SA_NAME                "libcubridsa.so"
#endif

#define UTILITY_GENERIC_MSG_FUNC_NAME	"utility_get_generic_message"
#define UTILITY_INIT_FUNC_NAME	        "utility_initialize"
#define UTILITY_ADMIN_USAGE_FUNC_NAME   "util_admin_usage"
#define UTILITY_ADMIN_VERSION_FUNC_NAME "util_admin_version"
typedef int (*UTILITY_INIT_FUNC) (void);

/* extern functions */
extern int utility_initialize (void);
extern const char *utility_get_generic_message (int message_index);
extern int check_database_name (const char *name);
extern int check_new_database_name (const char *name);
extern int check_volume_name (const char *name);
extern int utility_get_option_int_value (UTIL_ARG_MAP * arg_map, int arg_ch);
extern bool utility_get_option_bool_value (UTIL_ARG_MAP * arg_map,
					   int arg_ch);
extern char *utility_get_option_string_value (UTIL_ARG_MAP * arg_map,
					      int arg_ch, int index);
extern int utility_get_option_string_table_size (UTIL_ARG_MAP * arg_map);

extern FILE *fopen_ex (const char *filename, const char *type);

typedef struct
{
  int keyval;
  const char *keystr;
} UTIL_KEYWORD;

extern int utility_keyword_value (UTIL_KEYWORD * keywords,
				  int *keyval_p, char **keystr_p);
extern int utility_keyword_search (UTIL_KEYWORD * keywords, int *keyval_p,
				   char **keystr_p);

extern int utility_localtime (const time_t * ts, struct tm *result);

/* admin utility main functions */
typedef struct
{
  UTIL_ARG_MAP *arg_map;
  const char *command_name;
  char *argv0;
  char **argv;
} UTIL_FUNCTION_ARG;
typedef int (*UTILITY_FUNCTION) (UTIL_FUNCTION_ARG *);

extern int compactdb (UTIL_FUNCTION_ARG * arg_map);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int loaddb_dba (UTIL_FUNCTION_ARG * arg_map);
#endif
extern int loaddb_user (UTIL_FUNCTION_ARG * arg_map);
extern int unloaddb (UTIL_FUNCTION_ARG * arg_map);
extern int backupdb (UTIL_FUNCTION_ARG * arg_map);
extern int addvoldb (UTIL_FUNCTION_ARG * arg_map);
extern int checkdb (UTIL_FUNCTION_ARG * arg_map);
extern int spacedb (UTIL_FUNCTION_ARG * arg_map);
extern int lockdb (UTIL_FUNCTION_ARG * arg_map);
extern int killtran (UTIL_FUNCTION_ARG * arg_map);
extern int restartevnt (UTIL_FUNCTION_ARG * arg_map);
extern int prestartldb (UTIL_FUNCTION_ARG * arg_map);
extern int shutdownldb (UTIL_FUNCTION_ARG * arg_map);
extern int mqueueldb (UTIL_FUNCTION_ARG * arg_map);
extern int plandump (UTIL_FUNCTION_ARG * arg_map);
extern int createdb (UTIL_FUNCTION_ARG * arg_map);
extern int deletedb (UTIL_FUNCTION_ARG * arg_map);
extern int restoredb (UTIL_FUNCTION_ARG * arg_map);
extern int renamedb (UTIL_FUNCTION_ARG * arg_map);
extern int installdb (UTIL_FUNCTION_ARG * arg_map);
extern int copydb (UTIL_FUNCTION_ARG * arg_map);
extern int optimizedb (UTIL_FUNCTION_ARG * arg_map);
extern int diagdb (UTIL_FUNCTION_ARG * arg_map);
extern int patchdb (UTIL_FUNCTION_ARG * arg_map);
extern int estimatedb_data (UTIL_FUNCTION_ARG * arg_map);
extern int estimatedb_index (UTIL_FUNCTION_ARG * arg_map);
extern int estimatedb_hash (UTIL_FUNCTION_ARG * arg_map);
extern int alterdbhost (UTIL_FUNCTION_ARG * arg_map);
extern int paramdump (UTIL_FUNCTION_ARG * arg_map);
extern int statdump (UTIL_FUNCTION_ARG * arg_map);
extern int changemode (UTIL_FUNCTION_ARG * arg_map);
extern int copylogdb (UTIL_FUNCTION_ARG * arg_map);
extern int applylogdb (UTIL_FUNCTION_ARG * arg_map);

extern void util_admin_usage (const char *argv0);
extern void util_admin_version (const char *argv0);
#endif /* _UTILITY_H_ */
