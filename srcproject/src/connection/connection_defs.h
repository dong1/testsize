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
 * connection_defs.h - all the #define, the structure defs and the typedefs
 *          for the client/server implementation
 */

#ifndef _CONNECTION_DEFS_H_
#define _CONNECTION_DEFS_H_

#ident "$Id$"

#include <stdio.h>
#if defined(WINDOWS)
#include <dos.h>
#include <process.h>
#endif /* WINDOWS */
#if !defined(WINDOWS) && defined(SERVER_MODE)
#include <pthread.h>
#endif /* !WINDOWS && SERVER_MODE */

#include "porting.h"
#include "memory_alloc.h"
#include "error_manager.h"
#if defined(SERVER_MODE)
#include "connection_list_sr.h"
#include "critical_section.h"
#endif
#include "thread_impl.h"

/*
 * These are the types of top-level commands sent to the master server
 * from the client when initiating a connection. They distinguish the
 * difference between an information connection and a user connection.
 */
enum css_command_type
{
  INFO_REQUEST = 1,		/* get runtime info from the master server */
  DATA_REQUEST = 2,		/* get data from the database server */
  SERVER_REQUEST = 3,		/* let new server attach */
  MSQL_REQUEST = 4,		/* A request to start a new M driver. */
  SERVER_REQUEST_NEW = 5	/* new-style server request */
};

/*
 * These are the responses from the master to a server
 * when it is trying to connect and register itself.
 */
enum css_master_response
{
  SERVER_ALREADY_EXISTS = 0,
  SERVER_REQUEST_ACCEPTED = 1,
  DRIVER_NOT_FOUND = 2,
  SERVER_REQUEST_ACCEPTED_NEW = 3
};

/*
 * These are the types of requests sent by the information client to
 * the master.
 */
enum css_client_request
{
  GET_START_TIME = 1,
  GET_SERVER_COUNT = 2,
  GET_REQUEST_COUNT = 3,
  START_MASTER_TRACING = 4,
  STOP_MASTER_TRACING = 5,
  START_SLAVE_TRACING = 6,
  STOP_SLAVE_TRACING = 7,
  SET_SERVER_LIMIT = 8,
  STOP_SERVER = 9,
  START_SERVER = 10,
  GET_SERVER_LIST = 11,
  KILL_MASTER_SERVER = 12,
  KILL_SLAVE_SERVER = 13,
  START_SHUTDOWN = 14,
  CANCEL_SHUTDOWN = 15,
  GET_SHUTDOWN_TIME = 16,
  KILL_SERVER_IMMEDIATE = 17,
  GET_REPL_LIST = 20,		/* REPL: get the info. for a process */
  GET_ALL_LIST = 21,		/* REPL: get the info. for all processes */
  GET_REPL_COUNT = 22,		/* REPL: get the # of repl processes */
  GET_ALL_COUNT = 23,		/* REPL: get the # of all processes */
  KILL_REPL_SERVER = 24,	/* REPL: kill the repl process */
  GET_SERVER_HA_MODE = 25,	/* HA: get server ha mode */
  GET_HA_NODE_LIST = 26,	/* HA: get ha node list */
  GET_HA_NODE_LIST_VERBOSE = 27,	/* HA: get ha node list verbose */
  GET_HA_PROCESS_LIST = 28,	/* HA: get ha process list */
  GET_HA_PROCESS_LIST_VERBOSE = 29,	/* HA: get ha process list verbose */
  DEREGISTER_HA_PROCESS = 30,	/* HA: deregister ha process */
  RECONFIG_HEARTBEAT = 31,	/* HA: reconfigure ha node */
  DEACTIVATE_HEARTBEAT = 32,	/* HA: deactivate */
  ACTIVATE_HEARTBEAT = 33	/* HA: activate */
};

/*
 * These are the types of requests sent between the master and the servers.
 */
enum css_server_request
{
  SERVER_START_TRACING = 1,
  SERVER_STOP_TRACING = 2,
  SERVER_HALT_EXECUTION = 3,
  SERVER_RESUME_EXECUTION = 4,
  SERVER_START_NEW_CLIENT = 5,
  SERVER_START_SHUTDOWN = 6,
  SERVER_STOP_SHUTDOWN = 7,
  SERVER_SHUTDOWN_IMMEDIATE = 8,
  SERVER_GET_HA_MODE = 9,
  SERVER_REGISTER_HA_PROCESS = 10,
  SERVER_CHANGE_HA_MODE = 11
};

/*
 * These are the status codes for the connection structure which represent
 * the state of the connection.
 */
enum css_conn_status
{
  CONN_OPEN = 1,
  CONN_CLOSED = 2,
  CONN_CLOSING = 3
};

/*
 * These are the types of fds in the socket queue.
 */
enum
{
  READ_WRITE = 0,
  READ_ONLY = 1,
  WRITE_ONLY = 2
};

/*
 * These are the types of "packets" that can be sent over the comm interface.
 */
enum css_packet_type
{
  COMMAND_TYPE = 1,
  DATA_TYPE = 2,
  ABORT_TYPE = 3,
  CLOSE_TYPE = 4,
  ERROR_TYPE = 5,
};

/*
 * These are the status conditions that can be returned when a client
 * is trying to get a connection.
 */
enum css_status
{
  SERVER_CONNECTED = 0,
  SERVER_NOT_FOUND = 1,
  SERVER_STARTED = 2,
  SERVER_IS_RECOVERING = 3,	/* not used */
  SERVER_HAS_SHUT_DOWN = 4,	/* not used */
  ERROR_MESSAGE_FROM_MASTER = 5,	/* an error message is returned */
  SERVER_CONNECTED_NEW = 6,
  SERVER_CLIENTS_EXCEEDED = 7
};

/*
 * These are the error values returned by the client and server interfaces
 */
enum css_error_code
{
  NO_ERRORS = 1,
  CONNECTION_CLOSED = 2,
  REQUEST_REFUSED = 3,
  ERROR_ON_READ = 4,
  ERROR_ON_WRITE = 5,
  RECORD_TRUNCATED = 6,
  ERROR_WHEN_READING_SIZE = 7,
  READ_LENGTH_MISMATCH = 8,
  ERROR_ON_COMMAND_READ = 9,
  NO_DATA_AVAILABLE = 10,
  WRONG_PACKET_TYPE = 11,
  SERVER_WAS_NOT_FOUND = 12,
  SERVER_ABORTED = 13,
  INTERRUPTED_READ = 14,
  CANT_ALLOC_BUFFER = 15,
  OS_ERROR = 16
};

/*
 * Server's request_handler status codes.
 * Assigned to error_p in current socket queue entry.
 */
enum css_status_code
{
  CSS_NO_ERRORS = 0,
  CSS_UNPLANNED_SHUTDOWN = 1,
  CSS_PLANNED_SHUTDOWN = 2
};

/*
 * HA mode
 */
typedef enum ha_mode HA_MODE;
enum ha_mode
{
  HA_MODE_OFF = 0,
  HA_MODE_FAIL_OVER = 1,	/* unused */
  HA_MODE_FAIL_BACK = 2,
  HA_MODE_LAZY_BACK = 3,	/* not implemented yet */
  HA_MODE_ROLE_CHANGE = 4
};
#define HA_MODE_OFF_STR		"off"
#define HA_MODE_FAIL_OVER_STR	"fail-over"
#define HA_MODE_FAIL_BACK_STR	"fail-back"
#define HA_MODE_LAZY_BACK_STR	"lazy-back"
#define HA_MODE_ROLE_CHANGE_STR	"role-change"

/*
 * HA server mode
 */
typedef enum ha_server_mode HA_SERVER_MODE;
enum ha_server_mode
{
  HA_SERVER_MODE_NA = -1,
  HA_SERVER_MODE_ACTIVE = 0,
  HA_SERVER_MODE_STANDBY = 1,
  HA_SERVER_MODE_BACKUP = 2,
  HA_SERVER_MODE_PRIMARY = 0,	/* alias of active */
  HA_SERVER_MODE_SECONDARY = 1,	/* alias of standby */
  HA_SERVER_MODE_TERNARY = 2	/* alias of backup */
};
#define HA_SERVER_MODE_ACTIVE_STR      "active"
#define HA_SERVER_MODE_STANDBY_STR     "standby"
#define HA_SERVER_MODE_BACKUP_STR      "backup"
#define HA_SERVER_MODE_PRIMARY_STR      "primary"
#define HA_SERVER_MODE_SECONDARY_STR    "secondary"
#define HA_SERVER_MODE_TERNARY_STR      "ternary"

/*
 * HA server state
 */
typedef enum ha_server_state HA_SERVER_STATE;
enum ha_server_state
{
  HA_SERVER_STATE_NA = -1,	/* N/A */
  HA_SERVER_STATE_IDLE = 0,	/* initial state */
  HA_SERVER_STATE_ACTIVE = 1,
  HA_SERVER_STATE_TO_BE_ACTIVE = 2,
  HA_SERVER_STATE_STANDBY = 3,
  HA_SERVER_STATE_TO_BE_STANDBY = 4,
  HA_SERVER_STATE_MAINTENANCE = 5,	/* maintenance mode */
  HA_SERVER_STATE_DEAD = 6	/* server is dead - virtual state; not exists */
};
#define HA_SERVER_STATE_IDLE_STR                "idle"
#define HA_SERVER_STATE_ACTIVE_STR              "active"
#define HA_SERVER_STATE_TO_BE_ACTIVE_STR        "to-be-active"
#define HA_SERVER_STATE_STANDBY_STR             "standby"
#define HA_SERVER_STATE_TO_BE_STANDBY_STR       "to-be-standby"
#define HA_SERVER_STATE_MAINTENANCE_STR         "maintenance"
#define HA_SERVER_STATE_DEAD_STR                "dead"

/*
 * HA log applier state
 */
typedef enum ha_log_applier_state HA_LOG_APPLIER_STATE;
enum ha_log_applier_state
{
  HA_LOG_APPLIER_STATE_NA = -1,
  HA_LOG_APPLIER_STATE_UNREGISTERED = 0,
  HA_LOG_APPLIER_STATE_RECOVERING = 1,
  HA_LOG_APPLIER_STATE_WORKING = 2,
  HA_LOG_APPLIER_STATE_DONE = 3,
  HA_LOG_APPLIER_STATE_ERROR = 4
};
#define HA_LOG_APPLIER_STATE_UNREGISTERED_STR   "unregistered"
#define HA_LOG_APPLIER_STATE_RECOVERING_STR     "recovering"
#define HA_LOG_APPLIER_STATE_WORKING_STR        "working"
#define HA_LOG_APPLIER_STATE_DONE_STR           "done"
#define HA_LOG_APPLIER_STATE_ERROR_STR          "error"

/*
 * This constant defines the maximum size of a msg from the master to the
 * server.  Every msg between the master and the server will transmit this
 * many bytes.  A constant msg size is necessary since the protocol does
 * not pre-send the msg length to the server before sending the actual msg.
 */
#define MASTER_TO_SRV_MSG_SIZE 1024

#ifdef PRINTING
#define TPRINTF(error_string, arg) \
  do \
    { \
      fprintf (stderr, error_string, (arg)); \
      fflush (stderr); \
    } \
  while (0)

#define TPRINTF2(error_string, arg1, arg2) \
  do \
    { \
      fprintf (stderr, error_string, (arg1), (arg2)); \
      fflush (stderr); \
    } \
  while (0)
#else /* PRINTING */
#define TPRINTF(error_string, arg)
#define TPRINTF2(error_string, arg1, arg2)
#endif /* PRINTING */

/* TODO: 64Bit porting */
#define HIGH16BITS(X) (((X) >> 16) & 0xffffL)
#define LOW16BITS(X)  ((X) & 0xffffL)
#define DEFAULT_HEADER_DATA {0,0,0,0,0,0,0,0,0}

#define CSS_RID_FROM_EID(eid)           ((unsigned short) LOW16BITS(eid))
#define CSS_ENTRYID_FROM_EID(eid)       ((unsigned short) HIGH16BITS(eid))


/*
 * This is the format of the header for each command packet that is sent
 * across the network.
 */
typedef struct packet_header NET_HEADER;
struct packet_header
{
  int type;
  int version;
  int node_id;
  int transaction_id;
  int request_id;
  int db_error;
  short function_code;
  short dummy;
  int buffer_size;
};

/*
 * These are the data definitions for the queuing routines.
 */
typedef struct css_queue_entry CSS_QUEUE_ENTRY;
struct css_queue_entry
{
  CSS_QUEUE_ENTRY *next;
  char *buffer;

#if !defined(SERVER_MODE)
  unsigned int key;
#else
  int key;
#endif

  int size;
  int rc;
  int transaction_id;
  int db_error;

#if !defined(SERVER_MODE)
  char lock;
#endif
};

/*
 * This data structure is the interface between the client and the
 * communication software to identify the data connection.
 */
typedef struct css_conn_entry CSS_CONN_ENTRY;
struct css_conn_entry
{
  SOCKET fd;
  unsigned short request_id;
  int status;			/* CONN_OPEN, CONN_CLOSED, CONN_CLOSING = 3 */
  int transaction_id;
  int client_id;
  int db_error;
  bool in_transaction;		/* this client is in-transaction or out-of- */
  bool reset_on_commit;		/* set reset_on_commit when commit/abort */

#if defined(SERVER_MODE)
  int idx;			/* connection index */
  CSS_CRITICAL_SECTION csect;
  bool stop_talk;		/* block and stop this connection */
  unsigned short stop_phase;

  char *version_string;		/* client version string */

  CSS_QUEUE_ENTRY *free_queue_list;
  struct css_wait_queue_entry *free_wait_queue_list;
  char *free_net_header_list;
  int free_queue_count;
  int free_wait_queue_count;
  int free_net_header_count;

  CSS_LIST request_queue;	/* list of requests    */
  CSS_LIST data_queue;		/* list of data packets    */
  CSS_LIST data_wait_queue;	/* list of waiters */
  CSS_LIST abort_queue;		/* list of aborted requests         */
  CSS_LIST buffer_queue;	/* list of buffers queued for data */
  CSS_LIST error_queue;		/* list of (server) error messages  */
  CSS_CONN_ENTRY *next_remote_server;	/* remote server conn entry list */
  int node_id;			/* host id of remote server */
  int remote_status;		/* 0 : free,  1: in using */
  CSS_CONN_ENTRY *trans_conn;	/*the conn transmit the package */
  struct css_conn_table *conn_table;

#else
  FILE *file;
  CSS_QUEUE_ENTRY *request_queue;	/* the header for unseen requests    */
  CSS_QUEUE_ENTRY *data_queue;	/* header for unseen data packets    */
  CSS_QUEUE_ENTRY *abort_queue;	/* queue of aborted requests         */
  CSS_QUEUE_ENTRY *buffer_queue;	/* header of buffers queued for data */
  CSS_QUEUE_ENTRY *error_queue;	/* queue of (server) error messages  */
  void *cnxn;
#endif

  CSS_CONN_ENTRY *next;
};

/*
 * This is the mapping entry from a host/key to/from the entry id.
 */
typedef struct css_mapping_entry CSS_MAP_ENTRY;
struct css_mapping_entry
{
  char *key;			/* host name (or some such) */
  CSS_CONN_ENTRY *conn;		/* the connection */
#if !defined(SERVER_MODE)
  CSS_MAP_ENTRY *next;
#endif
  unsigned short id;		/* host id to help identify the connection */
};
#if defined (ENABLE_UNUSED_FUNCTION)
extern void css_shutdown (int exit_reason);
#endif

/* 
 * Buffer object for S2S
 */
typedef struct css_buffer_struct CSS_BUFFER;
struct css_buffer_struct
{
  CSS_BUFFER *next;
  char *buff;
  unsigned alloc_id;
  unsigned data_size;
};
#define CSS_BUFFER_ALLOC_SIZE(b)  (1 << (b)->alloc_id)

#endif /* _CONNECTION_DEFS_H_ */
