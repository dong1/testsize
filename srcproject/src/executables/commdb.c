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
 * commdb.c - commdb main
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#else
#include "getopt.h"
#endif

#if defined(WINDOWS)
#include <winsock2.h>
#else /* ! WINDOWS */
#include <sys/types.h>
#include <netinet/in.h>
#include <unistd.h>
#endif /* ! WINDOWS */

#if defined(SOLARIS) || defined(LINUX)
#include <netdb.h>
#endif /* SOLARIS || LINUX */

#include "connection_defs.h"
#include "connection_cl.h"
#if defined(WINDOWS)
#include "wintcp.h"
#endif /* WINDOWS */
#include "error_manager.h"
#include "porting.h"
#include "master_util.h"
#include "message_catalog.h"
#include "utility.h"
#include "util_support.h"
#include "porting.h"

typedef enum
{
  COMM_SERVER,
  COMM_REPL_SERVER,
  COMM_REPL_AGENT,
  COMM_ALL = 99
} COMM_SERVER_TYPE;

static int send_for_server_stats (CSS_CONN_ENTRY * conn);
static int send_for_repl_stats (CSS_CONN_ENTRY * conn);
static int send_for_all_stats (CSS_CONN_ENTRY * conn);
#if defined (ENABLE_UNUSED_FUNCTION)
static int send_for_server_downtime (CSS_CONN_ENTRY * conn);
#endif
static int return_integer_data (CSS_CONN_ENTRY * conn,
				unsigned short request_id);
static int send_for_request_count (CSS_CONN_ENTRY * conn);
static void process_status_query (CSS_CONN_ENTRY * conn, int server_type,
				  char **server_info);
static void process_master_kill (CSS_CONN_ENTRY * conn);
static void process_master_stop_shutdown (CSS_CONN_ENTRY * conn);
static void process_master_shutdown (CSS_CONN_ENTRY * conn, int minutes);
static void process_repl_kill (CSS_CONN_ENTRY * conn, char *slave_name,
			       int pid);
static void process_slave_kill (CSS_CONN_ENTRY * conn, char *slave_name,
				int minutes, int pid);
static void process_immediate_kill (CSS_CONN_ENTRY * conn, char *slave_name);
static int process_server_info_pid (CSS_CONN_ENTRY * conn, const char *server,
				    int server_type);
static void process_ha_server_mode (CSS_CONN_ENTRY * conn, char *server_name);
static void process_ha_node_info_query (CSS_CONN_ENTRY * conn,
					int verbose_yn);
static void process_ha_process_info_query (CSS_CONN_ENTRY * conn,
					   int verbose_yn);
static void process_dereg_ha_process (CSS_CONN_ENTRY * conn,
				      char *pid_string);


static void process_batch_command (CSS_CONN_ENTRY * conn);

static char *commdb_Arg_server_name = NULL;
static bool commdb_Arg_halt_shutdown = false;
static int commdb_Arg_shutdown_time = 0;
static bool commdb_Arg_kill_all = false;
static bool commdb_Arg_print_info = false;
static bool commdb_Arg_print_repl_info = false;
static bool commdb_Arg_print_all_info = false;
static char *commdb_Arg_repl_server_name = NULL;
static char *commdb_Arg_repl_agent_name = NULL;
static bool commdb_Arg_ha_mode_server_info = false;
static char *commdb_Arg_ha_mode_server_name = NULL;
static bool commdb_Arg_print_ha_node_info = false;
static bool commdb_Arg_print_ha_process_info = false;
static bool commdb_Arg_dereg_ha_process = false;
static char *commdb_Arg_dereg_ha_process_pid = NULL;
static bool commdb_Arg_reconfig_heartbeat = false;
static bool commdb_Arg_deactivate_heartbeat = false;
static bool commdb_Arg_activate_heartbeat = false;
static bool commdb_Arg_verbose_output = false;

/*
 * send_request_no_args() - send request without argument
 *   return: request id if success, otherwise 0
 *   conn(in): connection entry pointer
 *   command(in): request command
 */
static unsigned short
send_request_no_args (CSS_CONN_ENTRY * conn, int command)
{
  unsigned short request_id;

  if (css_send_request (conn, command, &request_id, NULL, 0) == NO_ERRORS)
    return (request_id);
  else
    return (0);
}

/*
 * send_request_one_arg() - send request with one argument
 *   return: request id if success, otherwise 0
 *   conn(in): connection info
 *   command(in): request command
 *   buffer(in): buffer pointer of request argument
 *   size(in): size of argument
 */
static unsigned short
send_request_one_arg (CSS_CONN_ENTRY * conn, int command, char *buffer,
		      int size)
{
  unsigned short request_id;

  if (css_send_request (conn, command, &request_id, buffer, size)
      == NO_ERRORS)
    return (request_id);
  else
    return (0);
}

/*
 * send_request_two_args() - send request with two arguments
 *   return: request id if success, otherwise 0
 *   conn(in): connection info
 *   command(in): request command
 *   buffer1(in): buffer pointer of first request argument
 *   size1(in): size of first argument
 *   buffer2(in): buffer pointer of first request argument
 *   size2(in): size of first argument
 */
static unsigned short
send_request_two_args (CSS_CONN_ENTRY * conn, int command,
		       char *buffer1, int size1, char *buffer2, int size2)
{
  unsigned short request_id;

  if (css_send_request (conn, command, &request_id, buffer1, size1)
      == NO_ERRORS)
    if (css_send_data (conn, request_id, buffer2, size2) == NO_ERRORS)
      return (request_id);
  return (0);
}

/*
 * send_for_start_time() - send request for master start time
 *   return: request id if success, otherwise 0
 *   conn(in): connection info
 */
static unsigned short
send_for_start_time (CSS_CONN_ENTRY * conn)
{
  return (send_request_no_args (conn, GET_START_TIME));
}

/*
 * return_string() - receive string data response
 *   return: none
 *   conn(in): connection info
 *   request_id(in): request id
 *   buffer(out): response output buffer
 *   buffer_size(out): size of data received
 */
static void
return_string (CSS_CONN_ENTRY * conn, unsigned short request_id,
	       char **buffer, int *buffer_size)
{
  css_receive_data (conn, request_id, buffer, buffer_size);
}

/*
 * send_for_server_count() - send request for database server count
 *   return: request id
 *   conn(in): connection info
 */
static int
send_for_server_count (CSS_CONN_ENTRY * conn)
{
  return (send_request_no_args (conn, GET_SERVER_COUNT));
}

/*
 * send_for_repl_count() - send request for replication server and agent count
 *   return: request id
 *   conn(in): connection info
 */
static int
send_for_repl_count (CSS_CONN_ENTRY * conn)
{
  return (send_request_no_args (conn, GET_REPL_COUNT));
}

/*
 * send_for_all_count() - send request for all processes count
 *   return: request id
 *   conn(in): connection info
 */
static int
send_for_all_count (CSS_CONN_ENTRY * conn)
{
  return (send_request_no_args (conn, GET_ALL_COUNT));
}

/*
 * send_for_server_stats() - send request for server processes info
 *   return: request id
 *   conn(in): connection info
 */
static int
send_for_server_stats (CSS_CONN_ENTRY * conn)
{
  return (send_request_no_args (conn, GET_SERVER_LIST));
}

/*
 * send_for_repl_stats() - send request for replication processes info
 *   return: request id
 *   conn(in): connection info
 */
static int
send_for_repl_stats (CSS_CONN_ENTRY * conn)
{
  return (send_request_no_args (conn, GET_REPL_LIST));
}

/*
 * send_for_all_stats() - send request for all processes info
 *   return: request id
 *   conn(in): connection info
 */
static int
send_for_all_stats (CSS_CONN_ENTRY * conn)
{
  return (send_request_no_args (conn, GET_ALL_LIST));
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * send_for_server_downtime() - send request for master shutdown time or
 *                              release string
 *   return: request id
 *   conn(in): connection info
 */
static int
send_for_server_downtime (CSS_CONN_ENTRY * conn)
{
  return (send_request_no_args (conn, GET_SHUTDOWN_TIME));
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * return_integer_data() - receive integer data response
 *   return: response value
 *   conn(in): connection info
 *   request_id(in): request id
 */
static int
return_integer_data (CSS_CONN_ENTRY * conn, unsigned short request_id)
{
  int size;
  int *buffer = NULL;

  if (css_receive_data (conn, request_id, (char **) &buffer, &size)
      == NO_ERRORS)
    {
      if (size == sizeof (int))
	{
	  size = ntohl (*buffer);
	  free_and_init (buffer);
	  return (size);
	}
    }
  if (buffer != NULL)
    {
      free_and_init (buffer);
    }
  return (0);
}

/*
 * send_for_request_count() - send request for serviced master request count
 *   return: request id
 *   conn(in): connection info
 */
static int
send_for_request_count (CSS_CONN_ENTRY * conn)
{
  return (send_request_no_args (conn, GET_REQUEST_COUNT));
}

/*
 * process_status_query() - print or get process status
 *   return: none
 *   conn(in): connection info
 *   server_type(in): COMM_SERVER_TYPE
 *   server_info(out): process info output pointer. If server_info is NULL,
 *                     process status is displayed to stdout
 */
static void
process_status_query (CSS_CONN_ENTRY * conn, int server_type,
		      char **server_info)
{
  int buffer_size;
  int server_count, requests_serviced;
  char *buffer1 = NULL, *buffer2 = NULL;
  unsigned short rid1, rid2, rid3, rid4;

  if (server_info != NULL)
    *server_info = NULL;

  rid2 = send_for_request_count (conn);
  switch (server_type)
    {
    case COMM_SERVER:
      rid3 = send_for_server_count (conn);
      rid1 = send_for_start_time (conn);
      rid4 = send_for_server_stats (conn);
      break;
    case COMM_REPL_SERVER:
    case COMM_REPL_AGENT:
      rid3 = send_for_repl_count (conn);
      rid1 = send_for_start_time (conn);
      rid4 = send_for_repl_stats (conn);
      break;
    case COMM_ALL:
      rid3 = send_for_all_count (conn);
      rid1 = send_for_start_time (conn);
      rid4 = send_for_all_stats (conn);
      break;
    default:
      rid1 = rid3 = rid4 = 0;
      break;
    }

  /* check for errors on the read */
  if (!rid1 || !rid2 || !rid3 || !rid4)
    return;

  return_string (conn, rid1, &buffer1, &buffer_size);
  requests_serviced = return_integer_data (conn, rid2);
  server_count = return_integer_data (conn, rid3);

  if (server_count)
    {
      return_string (conn, rid4, &buffer2, &buffer_size);
      if (server_info == NULL)
	{
	  printf (msgcat_message (MSGCAT_CATALOG_UTILS,
				  MSGCAT_UTIL_SET_COMMDB,
				  COMMDB_STRING4), buffer2);
	}
      else
	{
	  *server_info = buffer2;
	  buffer2 = NULL;
	}
    }

  free_and_init (buffer1);
  free_and_init (buffer2);
}

/*
 * process_master_kill() - send request to kill master
 *   return: none
 *   conn(in): connection info
 */
static void
process_master_kill (CSS_CONN_ENTRY * conn)
{
  while (send_request_no_args (conn, KILL_MASTER_SERVER) != 0)
    {
      ;				/* wait to master kill */
    }
}

/*
 * process_master_stop_shutdown() - send request to cancel shutdown
 *   return: none
 *   conn(in): connection info
 */
static void
process_master_stop_shutdown (CSS_CONN_ENTRY * conn)
{
  send_request_no_args (conn, CANCEL_SHUTDOWN);
}

/*
 * process_master_shutdown() - send request to shut down master and
 *                             replication processes
 *   return: none
 *   conn(in): connection info
 *   minutes(in): shutdown timeout in minutes
 */
static void
process_master_shutdown (CSS_CONN_ENTRY * conn, int minutes)
{
  int down;

  /*
   * kill all the replication process..
   * The replication processes are not listening from the master..
   */
  down = htonl (minutes);
  while (send_request_one_arg (conn,
			       START_SHUTDOWN,
			       (char *) &down, sizeof (int)) != 0)
    {
      ;				/* wait to master shutdown */
    }
}

/*
 * process_repl_kill() - process request to kill replication server
 *   return:  none
 *   conn(in): connection info
 *   slave_name(in): target process name
 *   pid(in): process id
 */
static void
process_repl_kill (CSS_CONN_ENTRY * conn, char *slave_name, int pid)
{
  char *reply_buffer = NULL;
  int size = 0;
  unsigned short rid;

  rid = send_request_one_arg (conn, KILL_REPL_SERVER,
			      slave_name, strlen (slave_name) + 1);
  return_string (conn, rid, &reply_buffer, &size);
  if (size)
    {
      printf ("\n%s\n", reply_buffer);

      if (pid > 0)
	{
	  master_util_wait_proc_terminate (pid);
	}
    }
  if (reply_buffer != NULL)
    {
      free_and_init (reply_buffer);
    }
}

/*
 * process_slave_kill() - process request to kill server process
 *   return:  none
 *   conn(in): connection info
 *   slave_name(in): target process name
 *   minutes(in): shutdown timeout in minutes
 *   pid(in): process id
 */
static void
process_slave_kill (CSS_CONN_ENTRY * conn, char *slave_name, int minutes,
		    int pid)
{
  int net_minutes;
  char *reply_buffer = NULL;
  int size = 0;
  unsigned short rid;

  net_minutes = htonl (minutes);
  rid = send_request_two_args (conn, KILL_SLAVE_SERVER,
			       slave_name, strlen (slave_name) + 1,
			       (char *) &net_minutes, sizeof (int));
  return_string (conn, rid, &reply_buffer, &size);
  if (size)
    {
      printf ("\n%s\n", reply_buffer);

      if (pid > 0)
	{
	  master_util_wait_proc_terminate (pid);
	}
    }
  if (reply_buffer != NULL)
    {
      free_and_init (reply_buffer);
    }
}

/*
 * process_ha_server_mode() - process ha server mode
 *   return:  none
 *   conn(in): connection info
 *   server_name(in): target process name
 */
static void
process_ha_server_mode (CSS_CONN_ENTRY * conn, char *server_name)
{
  char *reply_buffer = NULL;
  int size = 0;
  unsigned short rid;

  rid = send_request_one_arg (conn, GET_SERVER_HA_MODE,
			      server_name, strlen (server_name) + 1);
  return_string (conn, rid, &reply_buffer, &size);

  if (size)
    {
      printf ("\n%s\n", reply_buffer);
    }

  if (reply_buffer != NULL)
    {
      free_and_init (reply_buffer);
    }
}


/*
 * process_server_info_pid() - find process id from server status
 *   return: process id. 0 if server not found.
 *   conn(in): connection info
 *   server(in): database name
 *   server_type(in): COMM_SERVER_TYPE
 */
static int
process_server_info_pid (CSS_CONN_ENTRY * conn,
			 const char *server, int server_type)
{
  char search_pattern[256];
  char *p = NULL;
  int pid = 0;
  char *server_info = NULL;

  if (server == NULL)
    return 0;

  process_status_query (conn, server_type, &server_info);

  if (server_info)
    {
      switch (server_type)
	{
	case COMM_REPL_SERVER:
	  sprintf (search_pattern, "repl_server %s (", server);
	  break;
	case COMM_REPL_AGENT:
	  sprintf (search_pattern, "repl_agent %s (", server);
	  break;
	default:
	  sprintf (search_pattern, "Server %s (", server);
	  break;
	}
      p = strstr (server_info, search_pattern);
      if (p)
	{
	  p = strstr (p + strlen (search_pattern), "pid");
	  if (p)
	    {
	      p += 4;
	    }
	}
      if (p)
	pid = atoi (p);

      free_and_init (server_info);
    }

  return pid;
}

/*
 * process_ha_node_info_query() - process heartbeat node list
 *   return:  none
 *   conn(in): connection info
 *   verbose_yn(in): 
 */
static void
process_ha_node_info_query (CSS_CONN_ENTRY * conn, int verbose_yn)
{
  char *reply_buffer = NULL;
  int size = 0;
#if !defined(WINDOWS)
  unsigned short rid;
#endif /* !WINDOWS */

#if !defined(WINDOWS)
  rid = send_request_no_args (conn,
			      (verbose_yn) ?
			      GET_HA_NODE_LIST_VERBOSE : GET_HA_NODE_LIST);
  return_string (conn, rid, &reply_buffer, &size);
#endif

  if (size)
    {
      printf ("\n%s\n", reply_buffer);
    }

  if (reply_buffer != NULL)
    {
      free_and_init (reply_buffer);
    }
}

/*
 process_ha_process_info_query() - process heartbeat process list
 *   return:  none
 *   conn(in): connection info
 *   verbose_yn(in): 
 */
static void
process_ha_process_info_query (CSS_CONN_ENTRY * conn, int verbose_yn)
{
  char *reply_buffer = NULL;
  int size = 0;
#if !defined(WINDOWS)
  unsigned short rid;
#endif /* !WINDOWS */

#if !defined(WINDOWS)
  rid = send_request_no_args (conn,
			      (verbose_yn) ?
			      GET_HA_PROCESS_LIST_VERBOSE :
			      GET_HA_PROCESS_LIST);
  return_string (conn, rid, &reply_buffer, &size);
#endif

  if (size)
    {
      printf ("\n%s\n", reply_buffer);
    }

  if (reply_buffer != NULL)
    {
      free_and_init (reply_buffer);
    }
}

/*
 * process_dereg_ha_process() - deregister heartbeat process
 *   return:  none
 *   conn(in): connection info
 *   pid_string(in):
 */
static void
process_dereg_ha_process (CSS_CONN_ENTRY * conn, char *pid_string)
{
  char *reply_buffer = NULL;
  int size = 0;
#if !defined(WINDOWS)
  unsigned short rid;
#endif /* !WINDOWS */

#if !defined(WINDOWS)
  pid_t pid;

  pid = htonl (atoi (pid_string));

  rid = send_request_one_arg (conn, DEREGISTER_HA_PROCESS, (char *) &pid,
			      sizeof (pid));
  return_string (conn, rid, &reply_buffer, &size);
#endif

  if (size)
    {
      printf ("\n%s\n", reply_buffer);
    }

  if (reply_buffer != NULL)
    {
      free_and_init (reply_buffer);
    }
}

/*
 * process_reconfig_heartbeat() - reconfigure heartbeat node
 *   return:  none
 *   conn(in): connection info
 */
static void
process_reconfig_heartbeat (CSS_CONN_ENTRY * conn)
{
  char *reply_buffer = NULL;
  int size = 0;
#if !defined(WINDOWS)
  unsigned short rid;
#endif /* !WINDOWS */

#if !defined(WINDOWS)
  rid = send_request_no_args (conn, RECONFIG_HEARTBEAT);
  return_string (conn, rid, &reply_buffer, &size);
#endif

  if (size)
    {
      printf ("\n%s\n", reply_buffer);
    }

  if (reply_buffer != NULL)
    {
      free_and_init (reply_buffer);
    }
}

/*
 * process_deactivate_heartbeat() - deactivate heartbeat 
 *   return:  none
 *   conn(in): connection info
 */
static void
process_deactivate_heartbeat (CSS_CONN_ENTRY * conn)
{
  char *reply_buffer = NULL;
  int size = 0;
#if !defined(WINDOWS)
  unsigned short rid;
#endif /* !WINDOWS */

#if !defined(WINDOWS)
  rid = send_request_no_args (conn, DEACTIVATE_HEARTBEAT);
  return_string (conn, rid, &reply_buffer, &size);
#endif

  if (size)
    {
      printf ("\n%s\n", reply_buffer);
    }

  if (reply_buffer != NULL)
    {
      free_and_init (reply_buffer);
    }
}

/*
 * process_activate_heartbeat() - activate heartbeat 
 *   return:  none
 *   conn(in): connection info
 */
static void
process_activate_heartbeat (CSS_CONN_ENTRY * conn)
{
  char *reply_buffer = NULL;
  int size = 0;
#if !defined(WINDOWS)
  unsigned short rid;
#endif /* !WINDOWS */

#if !defined(WINDOWS)
  rid = send_request_no_args (conn, ACTIVATE_HEARTBEAT);
  return_string (conn, rid, &reply_buffer, &size);
#endif

  if (size)
    {
      printf ("\n%s\n", reply_buffer);
    }

  if (reply_buffer != NULL)
    {
      free_and_init (reply_buffer);
    }
}


/*
 * process_batch_command() - process user command in batch mode
 *   return: none
 *   conn(in): connection info
 */
static void
process_batch_command (CSS_CONN_ENTRY * conn)
{
  int pid;

  if ((commdb_Arg_server_name) && (!commdb_Arg_halt_shutdown))
    {
      pid = process_server_info_pid (conn, (char *) commdb_Arg_server_name,
				     COMM_SERVER);
      process_slave_kill (conn, (char *) commdb_Arg_server_name,
			  commdb_Arg_shutdown_time, pid);
    }
  if ((commdb_Arg_repl_server_name) && (!commdb_Arg_halt_shutdown))
    {
      char repl_name[DB_MAX_IDENTIFIER_LENGTH];

      snprintf (repl_name, DB_MAX_IDENTIFIER_LENGTH - 1, "+%s",
		commdb_Arg_repl_server_name);

      pid = process_server_info_pid (conn,
				     (char *) commdb_Arg_repl_server_name,
				     COMM_REPL_SERVER);
      process_repl_kill (conn, repl_name, pid);
    }
  if ((commdb_Arg_repl_agent_name) && (!commdb_Arg_halt_shutdown))
    {
      char repl_name[DB_MAX_IDENTIFIER_LENGTH];

      snprintf (repl_name, sizeof (repl_name), "&%s",
		commdb_Arg_repl_agent_name);
      pid = process_server_info_pid (conn,
				     (char *) commdb_Arg_repl_agent_name,
				     COMM_REPL_AGENT);
      process_repl_kill (conn, repl_name, pid);
    }
  if (commdb_Arg_kill_all)
    process_master_shutdown (conn, commdb_Arg_shutdown_time);
  if (commdb_Arg_halt_shutdown)
    process_master_stop_shutdown (conn);
  if (commdb_Arg_print_info)
    process_status_query (conn, COMM_SERVER, NULL);
  if (commdb_Arg_print_repl_info)
    process_status_query (conn, COMM_REPL_SERVER, NULL);
  if (commdb_Arg_print_all_info)
    process_status_query (conn, COMM_ALL, NULL);
  if (commdb_Arg_ha_mode_server_info)
    process_ha_server_mode (conn, (char *) commdb_Arg_ha_mode_server_name);
  if (commdb_Arg_print_ha_node_info)
    process_ha_node_info_query (conn, commdb_Arg_verbose_output);
  if (commdb_Arg_print_ha_process_info)
    process_ha_process_info_query (conn, commdb_Arg_verbose_output);
  if (commdb_Arg_dereg_ha_process)
    process_dereg_ha_process (conn, (char *) commdb_Arg_dereg_ha_process_pid);
  if (commdb_Arg_reconfig_heartbeat)
    process_reconfig_heartbeat (conn);
  if (commdb_Arg_deactivate_heartbeat)
    process_deactivate_heartbeat (conn);
  if (commdb_Arg_activate_heartbeat)
    process_activate_heartbeat (conn);
}

/*
 * main() - commdb main function
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
main (int argc, char **argv)
{
  int status = EXIT_SUCCESS;
  int port_id;
  unsigned short rid;
  char hostname[NAME_MAX];
  CSS_CONN_ENTRY *conn;

  static struct option commdb_options[] = {
    {COMMDB_SERVER_LIST_L, 0, 0, COMMDB_SERVER_LIST_S},
    {COMMDB_REPL_LIST_L, 0, 0, COMMDB_REPL_LIST_S},
    {COMMDB_ALL_LIST_L, 0, 0, COMMDB_ALL_LIST_S},
    {COMMDB_SHUTDOWN_SERVER_L, 1, 0, COMMDB_SHUTDOWN_SERVER_S},
    {COMMDB_SHUTDOWN_REPL_SERVER_L, 1, 0, COMMDB_SHUTDOWN_REPL_SERVER_S},
    {COMMDB_SHUTDOWN_REPL_AGENT_L, 1, 0, COMMDB_SHUTDOWN_REPL_AGENT_S},
    {COMMDB_SHUTDOWN_ALL_L, 0, 0, COMMDB_SHUTDOWN_ALL_S},
    {COMMDB_SERVER_MODE_L, 1, 0, COMMDB_SERVER_MODE_S},
    {COMMDB_HA_NODE_LIST_L, 0, 0, COMMDB_HA_NODE_LIST_S},
    {COMMDB_HA_PROCESS_LIST_L, 0, 0, COMMDB_HA_PROCESS_LIST_S},
    {COMMDB_DEREG_HA_PROCESS_L, 1, 0, COMMDB_DEREG_HA_PROCESS_S},
    {COMMDB_RECONFIG_HEARTBEAT_L, 0, 0, COMMDB_RECONFIG_HEARTBEAT_S},
    {COMMDB_DEACTIVATE_HEARTBEAT_L, 0, 0, COMMDB_DEACTIVATE_HEARTBEAT_S},
    {COMMDB_ACTIVATE_HEARTBEAT_L, 0, 0, COMMDB_ACTIVATE_HEARTBEAT_S},
    {COMMDB_VERBOSE_OUTPUT_L, 0, 0, COMMDB_VERBOSE_OUTPUT_S},
    {0, 0, 0, 0}
  };

#if defined(WINDOWS)
  if (css_windows_startup () < 0)
    {
      return EXIT_FAILURE;
    }
#endif /* WINDOWS */

#if !defined(WINDOWS)
  if (os_set_signal_handler (SIGPIPE, SIG_IGN) == SIG_ERR)
    {
      return EXIT_FAILURE;
    }
#endif /* ! WINDOWS */

  /* initialize message catalog for argument parsing and usage() */
  if (utility_initialize () != NO_ERROR)
    {
      return EXIT_FAILURE;
    }

  if (argc == 1)
    {
      goto usage;
    }

  while (1)
    {
      int option_index = 0;
      int option_key;
      char optstring[64];

      utility_make_getopt_optstring (commdb_options, optstring);
      option_key = getopt_long (argc, argv, optstring,
				commdb_options, &option_index);
      if (option_key == -1)
	{
	  break;
	}

      switch (option_key)
	{
	case 'P':
	  commdb_Arg_print_info = true;
	  break;
	case 'R':
	  commdb_Arg_print_repl_info = true;
	  break;
	case 'O':
	  commdb_Arg_print_all_info = true;
	  break;
	case 'A':
	  commdb_Arg_kill_all = true;
	  break;
	case 'S':
	  if (commdb_Arg_server_name != NULL)
	    {
	      free (commdb_Arg_server_name);
	    }
	  commdb_Arg_server_name = strdup (optarg);
	  break;
	case 'K':
	  if (commdb_Arg_repl_server_name != NULL)
	    {
	      free (commdb_Arg_repl_server_name);
	    }
	  commdb_Arg_repl_server_name = strdup (optarg);
	  break;
	case 'k':
	  if (commdb_Arg_repl_agent_name != NULL)
	    {
	      free (commdb_Arg_repl_agent_name);
	    }
	  commdb_Arg_repl_agent_name = strdup (optarg);
	  break;
	case 'c':
	  if (commdb_Arg_ha_mode_server_name != NULL)
	    {
	      free (commdb_Arg_ha_mode_server_name);
	    }
	  commdb_Arg_ha_mode_server_name = strdup (optarg);
	  commdb_Arg_ha_mode_server_info = true;
	  break;
	case 'N':
	  commdb_Arg_print_ha_node_info = true;
	  break;
	case 'L':
	  commdb_Arg_print_ha_process_info = true;
	  break;
	case 'D':
	  if (commdb_Arg_dereg_ha_process_pid != NULL)
	    {
	      free (commdb_Arg_dereg_ha_process_pid);
	    }
	  commdb_Arg_dereg_ha_process_pid = strdup (optarg);
	  commdb_Arg_dereg_ha_process = true;
	  break;
	case 'F':
	  commdb_Arg_reconfig_heartbeat = true;
	  break;
	case 'U':
	  commdb_Arg_deactivate_heartbeat = true;
	  break;
	case 'T':
	  commdb_Arg_activate_heartbeat = true;
	  break;
	case 'V':
	  commdb_Arg_verbose_output = true;
	  break;
	default:
	  goto usage;
	}
    }

  er_init (NULL, ER_NEVER_EXIT);

  if (GETHOSTNAME (hostname, MAXHOSTNAMELEN) != 0)
    {
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_COMMDB,
				       COMMDB_STRING9));
      status = EXIT_FAILURE;
      goto error;
    }

  if (master_util_config_startup ((argc > 1) ? argv[1] : NULL,
				  &port_id) == false)
    {
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_COMMDB,
				       COMMDB_STRING10));
      status = EXIT_FAILURE;
      goto error;
    }

  conn = css_connect_to_master_for_info (hostname, port_id, &rid);
  if (conn == NULL)
    {
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_COMMDB,
				       COMMDB_STRING11), hostname);
      status = EXIT_FAILURE;
      goto error;
    }

  /* command mode */
  process_batch_command (conn);

error:
#if defined(WINDOWS)
  css_windows_shutdown ();
#endif /* WINDOWS */
  msgcat_final ();
  goto end;

usage:
  printf (msgcat_message
	  (MSGCAT_CATALOG_UTILS, MSGCAT_UTIL_SET_COMMDB, COMMDB_STRING7));
  msgcat_final ();
  status = EXIT_FAILURE;

end:
  if (commdb_Arg_server_name != NULL)
    {
      free_and_init (commdb_Arg_server_name);
    }
  if (commdb_Arg_repl_server_name != NULL)
    {
      free_and_init (commdb_Arg_repl_server_name);
    }
  if (commdb_Arg_repl_agent_name != NULL)
    {
      free_and_init (commdb_Arg_repl_agent_name);
    }
  if (commdb_Arg_dereg_ha_process_pid != NULL)
    {
      free_and_init (commdb_Arg_dereg_ha_process_pid);
    }
  return status;
}
