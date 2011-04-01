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
 * connection_sr.c - Client/Server connection list management
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>

#if defined(WINDOWS)
#include <winsock2.h>
#include <windows.h>
#else /* WINDOWS */
#include <sys/time.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#endif /* WINDOWS */

#if defined(_AIX)
#include <sys/select.h>
#endif /* _AIX */

#if defined(SOLARIS)
#include <sys/filio.h>
#include <netdb.h>
#endif /* SOLARIS */

#if defined(SOLARIS) || defined(LINUX)
#include <unistd.h>
#endif /* SOLARIS || LINUX */

#include "porting.h"
#include "error_manager.h"
#include "connection_globals.h"
#include "memory_alloc.h"
#include "environment_variable.h"
#include "system_parameter.h"
#include "thread_impl.h"
#include "critical_section.h"
#include "log_manager.h"
#include "object_representation.h"
#include "connection_error.h"
#include "log_impl.h"
#if defined(WINDOWS)
#include "wintcp.h"
#else /* WINDOWS */
#include "tcp.h"
#endif /* WINDOWS */
#include "connection_sr.h"

#include "network.h"

#ifdef PACKET_TRACE
#define TRACE(string, arg)					\
	do {							\
		er_log_debug(ARG_FILE_LINE, string, arg);	\
	}							\
	while(0);
#else /* PACKET_TRACE */
#define TRACE(string, arg)
#endif /* PACKET_TRACE */

/* data wait queue */
typedef struct css_wait_queue_entry
{
  char **buffer;
  int *size;
  int *rc;
  struct thread_entry *thrd_entry;	/* thread waiting for data */
  struct css_wait_queue_entry *next;
  unsigned int key;
} CSS_WAIT_QUEUE_ENTRY;

typedef struct queue_search_arg
{
  CSS_QUEUE_ENTRY *entry_ptr;
  int key;
  int remove_entry;
} CSS_QUEUE_SEARCH_ARG;

typedef struct wait_queue_search_arg
{
  CSS_WAIT_QUEUE_ENTRY *entry_ptr;
  unsigned int key;
  int remove_entry;
} CSS_WAIT_QUEUE_SEARCH_ARG;

static const int CSS_MAX_CLIENT_ID = 32766;

static int css_Client_id = 0;
static MUTEX_T css_Client_id_lock = MUTEX_INITIALIZER;
static CSS_CONN_ENTRY *css_Free_conn_anchor = NULL;
static int css_Num_free_conn = 0;
static CSS_CRITICAL_SECTION css_Free_conn_csect;

CSS_CONN_ENTRY *css_Conn_array = NULL;
CSS_CONN_ENTRY *css_Active_conn_anchor = NULL;
static int css_Num_active_conn = 0;
CSS_CRITICAL_SECTION css_Active_conn_csect;

/* This will handle new connections */
int (*css_Connect_handler) (CSS_CONN_ENTRY *) = NULL;

/* This will handle new requests per connection */
CSS_THREAD_FN css_Request_handler = NULL;

/* This will handle closed connection errors */
CSS_THREAD_FN css_Connection_error_handler = NULL;


/* 
 * buffer size equals to (1 << alloc_id)
 */
#define  CSS_BUFFER_MIN_ALLOC     8
#define  CSS_BUFFER_MAX_ALLOC     15
#define  CSS_BUFFER_MIN_SIZE      ( 1 << CSS_BUFFER_MIN_ALLOC )
#define  CSS_BUFFER_MAX_SIZE      ( 1 << CSS_BUFFER_MAX_ALLOC )
#define  CSS_BUFFER_ALLOC_COUNT   ( CSS_BUFFER_MAX_ALLOC - CSS_BUFFER_MIN_ALLOC + 1 )

static CSS_BUFFER *css_Buffer_free_list[CSS_BUFFER_ALLOC_COUNT] = { NULL };


/*make the s2s connection pooling here*/
#ifdef PACKET_TRACE
#define TRACE(string, arg)					\
	do {							\
	er_log_debug(ARG_FILE_LINE, string, arg);	\
	}							\
	while(0);
#else /* PACKET_TRACE */
#define TRACE(string, arg)
#endif /* PACKET_TRACE */

typedef struct css_conn_table CSS_CONN_TABLE;
struct css_conn_table
{
  unsigned short num_using;
  MUTEX_T mutex;
  COND_T cond;
  CSS_CONN_ENTRY *free_anchor;
  CSS_CONN_ENTRY *using_anchor;
};

typedef struct host_table_entry HOST_TABLE_ENTRY;
struct host_table_entry
{
  int node_id;
  CSS_CONN_TABLE *conn_table;
};

typedef struct host_table HOST_TABLE;
struct host_table
{
  MUTEX_T mutex;
  unsigned short conn_host_num;
  HOST_TABLE_ENTRY host_entry[DB_MAX_NODE_IN_CLUSTER - 1];
};

/*static HOST_TABLE_ENTRY hentry[DB_MAX_NODE_IN_CLUSTER] = { {0, NULL} };*/
static HOST_TABLE h_table;

static CSS_CONN_TABLE *get_or_create (int node_id);

/*
 * S2S Connection pool
 */
static CSS_CONN_ENTRY *redirect_conns = NULL;

static CSS_CRITICAL_SECTION css_Data_csect;
static void (*css_Task_data_handler) (const NET_HEADER *, CSS_BUFFER *,
				      THREAD_ENTRY **) = NULL;

static char css_Net_db_server_name[128] = "";


static int css_get_next_client_id (void);
static CSS_CONN_ENTRY *css_common_connect (CSS_CONN_ENTRY * conn,
					   unsigned short *rid,
					   const char *host_name,
					   int connect_type,
					   const char *server_name,
					   int server_name_length, int port,
					   bool sendmagic);

static int css_abort_request (CSS_CONN_ENTRY * conn, unsigned short rid);
static void css_dealloc_conn (CSS_CONN_ENTRY * conn);

static unsigned int css_make_eid (unsigned short entry_id,
				  unsigned short rid);

static CSS_QUEUE_ENTRY *css_make_queue_entry (CSS_CONN_ENTRY * conn,
					      unsigned int key, char *buffer,
					      int size, int rc,
					      int transid, int db_error);
static void css_free_queue_entry (CSS_CONN_ENTRY * conn,
				  CSS_QUEUE_ENTRY * entry);
static int css_add_queue_entry (CSS_CONN_ENTRY * conn,
				CSS_LIST * list, unsigned short request_id,
				char *buffer, int buffer_size,
				int rc, int transid, int db_error);
static CSS_QUEUE_ENTRY *css_find_queue_entry (CSS_LIST * list,
					      unsigned int key);
static CSS_QUEUE_ENTRY *css_find_and_remove_queue_entry (CSS_LIST * list,
							 unsigned int key);
static CSS_WAIT_QUEUE_ENTRY *css_make_wait_queue_entry
  (CSS_CONN_ENTRY * conn, unsigned int key, char **buffer, int *size,
   int *rc);
static void css_free_wait_queue_entry (CSS_CONN_ENTRY * conn,
				       CSS_WAIT_QUEUE_ENTRY * entry);
static CSS_WAIT_QUEUE_ENTRY *css_add_wait_queue_entry
  (CSS_CONN_ENTRY * conn, CSS_LIST * list,
   unsigned short request_id, char **buffer, int *buffer_size, int *rc);
static CSS_WAIT_QUEUE_ENTRY *css_find_and_remove_wait_queue_entry
  (CSS_LIST * list, unsigned int key);

static void css_process_close_packet (CSS_CONN_ENTRY * conn);
static void css_process_abort_packet (CSS_CONN_ENTRY * conn,
				      unsigned short request_id);
static bool css_is_request_aborted (CSS_CONN_ENTRY * conn,
				    unsigned short request_id);
static int css_return_queued_data_timeout (CSS_CONN_ENTRY * conn,
					   unsigned short rid, char **buffer,
					   int *bufsize,
					   int *rc, int waitsec);

static void css_queue_data_packet (CSS_CONN_ENTRY * conn,
				   unsigned short request_id,
				   const NET_HEADER * header,
				   THREAD_ENTRY ** wait_thrd);
static void css_queue_error_packet (CSS_CONN_ENTRY * conn,
				    unsigned short request_id,
				    const NET_HEADER * header);
static void css_queue_command_packet (CSS_CONN_ENTRY * conn,
				      unsigned short request_id,
				      const NET_HEADER * header, int size);
#if defined (ENABLE_UNUSED_FUNCTION)
static char *css_return_oob_buffer (int size);
#endif
static bool css_is_valid_request_id (CSS_CONN_ENTRY * conn,
				     unsigned short request_id);
static void css_remove_unexpected_packets (CSS_CONN_ENTRY * conn,
					   unsigned short request_id);

static void css_queue_packet (CSS_CONN_ENTRY * conn, int type,
			      unsigned short request_id,
			      const NET_HEADER * header, int size);
static int css_remove_and_free_queue_entry (void *data, void *arg);
static int css_remove_and_free_wait_queue_entry (void *data, void *arg);

static int css_send_magic (CSS_CONN_ENTRY * conn);

static void css_clean_free_list ();
static int css_net_transmit_package (CSS_CONN_ENTRY * conn,
				     const NET_HEADER * header);
/*
 * get_next_client_id() -
 *   return: client id
 */
static int
css_get_next_client_id (void)
{
  int next_client_id, rv;

  MUTEX_LOCK (rv, css_Client_id_lock);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_MUTEX_LOCK, 0);
      return ER_FAILED;
    }

  css_Client_id++;
  if (css_Client_id == CSS_MAX_CLIENT_ID)
    {
      css_Client_id = 1;
    }
  next_client_id = css_Client_id;

  rv = MUTEX_UNLOCK (css_Client_id_lock);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_MUTEX_UNLOCK, 0);
      return ER_FAILED;
    }

  return next_client_id;
}

/*
 * css_initialize_conn() - initialize connection entry
 *   return: void
 *   conn(in):
 *   fd(in):
 */
int
css_initialize_conn (CSS_CONN_ENTRY * conn, SOCKET fd)
{
  int err;

  conn->fd = fd;
  conn->request_id = 0;
  conn->status = CONN_OPEN;
  conn->transaction_id = -1;
  err = css_get_next_client_id ();
  if (err < 0)
    {
      return ER_CSS_CONN_INIT;
    }
  conn->client_id = err;
  conn->db_error = 0;
  conn->in_transaction = false;
  conn->reset_on_commit = false;
  conn->stop_talk = false;
  conn->stop_phase = THREAD_WORKER_STOP_PHASE_0;
  conn->version_string = NULL;
  conn->free_queue_list = NULL;
  conn->free_queue_count = 0;
  conn->free_wait_queue_list = NULL;
  conn->free_wait_queue_count = 0;
  conn->free_net_header_list = NULL;
  conn->free_net_header_count = 0;
  conn->remote_status = 0;
  conn->next_remote_server = NULL;
  conn->node_id = 0;
  conn->trans_conn = NULL;

  err = css_initialize_list (&conn->request_queue, 0);
  if (err != NO_ERROR)
    {
      return ER_CSS_CONN_INIT;
    }
  err = css_initialize_list (&conn->data_queue, 0);
  if (err != NO_ERROR)
    {
      return ER_CSS_CONN_INIT;
    }
  err = css_initialize_list (&conn->data_wait_queue, 0);
  if (err != NO_ERROR)
    {
      return ER_CSS_CONN_INIT;
    }
  err = css_initialize_list (&conn->abort_queue, 0);
  if (err != NO_ERROR)
    {
      return ER_CSS_CONN_INIT;
    }
  err = css_initialize_list (&conn->buffer_queue, 0);
  if (err != NO_ERROR)
    {
      return ER_CSS_CONN_INIT;
    }
  err = css_initialize_list (&conn->error_queue, 0);
  if (err != NO_ERROR)
    {
      return ER_CSS_CONN_INIT;
    }
  return NO_ERROR;
}

/*
 * css_shutdown_conn() - close connection entry
 *   return: void
 *   conn(in):
 */
void
css_shutdown_conn (CSS_CONN_ENTRY * conn)
{
  csect_enter_critical_section (NULL, &conn->csect, INF_WAIT);

  if (!IS_INVALID_SOCKET (conn->fd))
    {
      /* if this is the PC, it also shuts down Winsock */
      css_shutdown_socket (conn->fd);
      conn->fd = INVALID_SOCKET;

      /* set host id to 0 means remove it from connection pool */
      conn->node_id = 0;
      conn->next_remote_server = NULL;
      conn->remote_status = 0;
    }

  if (conn->status == CONN_OPEN || conn->status == CONN_CLOSING)
    {
      conn->status = CONN_CLOSED;
      conn->stop_talk = false;
      conn->stop_phase = THREAD_WORKER_STOP_PHASE_0;

      if (conn->version_string)
	{
	  free_and_init (conn->version_string);
	}

      css_remove_all_unexpected_packets (conn);

      css_finalize_list (&conn->request_queue);
      css_finalize_list (&conn->data_queue);
      css_finalize_list (&conn->data_wait_queue);
      css_finalize_list (&conn->abort_queue);
      css_finalize_list (&conn->buffer_queue);
      css_finalize_list (&conn->error_queue);
    }

  if (conn->free_queue_count > 0)
    {
      CSS_QUEUE_ENTRY *p;

      while (conn->free_queue_list != NULL)
	{
	  p = conn->free_queue_list;
	  conn->free_queue_list = p->next;
	  conn->free_queue_count--;
	  free_and_init (p);
	}
    }

  if (conn->free_wait_queue_count > 0)
    {
      CSS_WAIT_QUEUE_ENTRY *p;

      while (conn->free_wait_queue_list != NULL)
	{
	  p = conn->free_wait_queue_list;
	  conn->free_wait_queue_list = p->next;
	  conn->free_wait_queue_count--;
	  free_and_init (p);
	}
    }

  if (conn->free_net_header_count > 0)
    {
      char *p;

      while (conn->free_net_header_list != NULL)
	{
	  p = conn->free_net_header_list;
	  conn->free_net_header_list = (char *) (*(UINTPTR *) p);
	  conn->free_net_header_count--;
	  free_and_init (p);
	}
    }

  csect_exit_critical_section (&conn->csect);
}

/*
 * css_init_conn_list() - initialize connection list
 *   return: NO_ERROR if success, or error code
 */
int
css_init_conn_list (void)
{
  int i, err;
  CSS_CONN_ENTRY *conn;

  if (css_Conn_array != NULL)
    {
      return NO_ERROR;
    }

  /*
   * allocate PRM_CSS_MAX_CLIENTS conn entries
   *         + 1(conn with master)
   */
  css_Conn_array = (CSS_CONN_ENTRY *)
    malloc (sizeof (CSS_CONN_ENTRY) * (PRM_CSS_MAX_CLIENTS));
  if (css_Conn_array == NULL)
    {
      return ER_CSS_CONN_INIT;
    }

  /* initialize all CSS_CONN_ENTRY */
  for (i = 0; i < PRM_CSS_MAX_CLIENTS; i++)
    {
      conn = &css_Conn_array[i];
      conn->idx = i;
      err = css_initialize_conn (conn, -1);
      if (err != NO_ERROR)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_CONN_INIT, 0);
	  return ER_CSS_CONN_INIT;
	}
      err = csect_initialize_critical_section (&conn->csect);
      if (err != NO_ERROR)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_CONN_INIT, 0);
	  return ER_CSS_CONN_INIT;
	}

      if (i < PRM_CSS_MAX_CLIENTS - 1)
	{
	  conn->next = &css_Conn_array[i + 1];
	}
      else
	{
	  conn->next = NULL;
	}
    }

  /* initialize active conn list, used for stopping all threads */
  css_Active_conn_anchor = NULL;
  css_Free_conn_anchor = &css_Conn_array[0];
  css_Num_free_conn = PRM_CSS_MAX_CLIENTS;

  err = csect_initialize_critical_section (&css_Active_conn_csect);
  if (err != NO_ERROR)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_CONN_INIT,
			   0);
      return ER_CSS_CONN_INIT;
    }

  err = csect_initialize_critical_section (&css_Free_conn_csect);
  if (err != NO_ERROR)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_CONN_INIT,
			   0);
      return ER_CSS_CONN_INIT;
    }

  err = csect_initialize_critical_section (&css_Data_csect);
  if (err != NO_ERROR)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_CONN_INIT,
			   0);
      return ER_CSS_CONN_INIT;
    }

  err = MUTEX_INIT (css_Client_id_lock);
  if (err != NO_ERROR)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_CONN_INIT,
			   0);
      return ER_CSS_CONN_INIT;
    }

  return NO_ERROR;
}

/*
 * css_final_conn_list() - free connection list
 *   return: void
 */
void
css_final_conn_list (void)
{
  CSS_CONN_ENTRY *conn, *next;
  int i;

  if (css_Active_conn_anchor != NULL)
    {
      for (conn = css_Active_conn_anchor; conn != NULL; conn = next)
	{
	  next = conn->next;
	  css_shutdown_conn (conn);
	  css_dealloc_conn (conn);

	  css_Num_active_conn--;
	  assert (css_Num_active_conn >= 0);
	}

      css_Active_conn_anchor = NULL;
    }

  assert (css_Num_active_conn == 0 && css_Active_conn_anchor == NULL);

  css_clean_free_list ();
  csect_finalize_critical_section (&css_Data_csect);

  csect_finalize_critical_section (&css_Active_conn_csect);
  csect_finalize_critical_section (&css_Free_conn_csect);

  for (i = 0; i < PRM_CSS_MAX_CLIENTS; i++)
    {
      conn = &css_Conn_array[i];
      csect_finalize_critical_section (&conn->csect);
    }

  free_and_init (css_Conn_array);
  /*free the connection pooling */
  s2s_free_conn_pooling ();
}

/*
 * css_make_conn() - make new connection entry, but not insert into active
 *                   conn list
 *   return: new connection entry
 *   fd(in): socket discriptor
 */
CSS_CONN_ENTRY *
css_make_conn (SOCKET fd)
{
  CSS_CONN_ENTRY *conn = NULL;

  csect_enter_critical_section (NULL, &css_Free_conn_csect, INF_WAIT);

  if (css_Free_conn_anchor != NULL)
    {
      conn = css_Free_conn_anchor;
      css_Free_conn_anchor = css_Free_conn_anchor->next;
      conn->next = NULL;

      css_Num_free_conn--;
      assert (css_Num_free_conn >= 0);
    }

  csect_exit_critical_section (&css_Free_conn_csect);

  if (conn != NULL)
    {
      if (css_initialize_conn (conn, fd) != NO_ERROR)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_CONN_INIT, 0);
	  return NULL;
	}
    }

  return conn;
}

/*
 * css_insert_into_active_conn_list() - insert/remove into/from active conn
 *                                      list. this operation must be called
 *                                      after/before css_free_conn etc.
 *   return: void
 *   conn(in): connection entry will be inserted
 */
void
css_insert_into_active_conn_list (CSS_CONN_ENTRY * conn)
{
  csect_enter_critical_section (NULL, &css_Active_conn_csect, INF_WAIT);

  conn->next = css_Active_conn_anchor;
  css_Active_conn_anchor = conn;

  css_Num_active_conn++;

  assert (css_Num_active_conn > 0
	  && css_Num_active_conn <= PRM_CSS_MAX_CLIENTS);

  csect_exit_critical_section (&css_Active_conn_csect);
}

/*
 * css_dealloc_conn_csect() - free critical section of connection entry
 *   return: void
 *   conn(in): connection entry
 */
void
css_dealloc_conn_csect (CSS_CONN_ENTRY * conn)
{
  csect_finalize_critical_section (&conn->csect);
}

/*
 * css_dealloc_conn() - free connection entry
 *   return: void
 *   conn(in): connection entry will be free
 */
static void
css_dealloc_conn (CSS_CONN_ENTRY * conn)
{
  csect_enter_critical_section (NULL, &css_Free_conn_csect, INF_WAIT);

  conn->next = css_Free_conn_anchor;
  css_Free_conn_anchor = conn;

  css_Num_free_conn++;
  assert (css_Num_free_conn > 0 && css_Num_free_conn <= PRM_CSS_MAX_CLIENTS);

  csect_exit_critical_section (&css_Free_conn_csect);
}

/*
 * css_free_conn() - destroy all connection related structures, and free conn
 *                   entry, delete from css_Active_conn_anchor list
 *   return: void
 *   conn(in): connection entry will be free
 */
void
css_free_conn (CSS_CONN_ENTRY * conn)
{
  CSS_CONN_ENTRY *p, *prev = NULL, *next;

  csect_enter_critical_section (NULL, &css_Active_conn_csect, INF_WAIT);

  /* find and remove from active conn list */
  for (p = css_Active_conn_anchor; p != NULL; p = next)
    {
      next = p->next;

      if (p == conn)
	{
	  if (prev == NULL)
	    {
	      css_Active_conn_anchor = next;
	    }
	  else
	    {
	      prev->next = next;
	    }

	  css_Num_active_conn--;
	  assert (css_Num_active_conn >= 0
		  && css_Num_active_conn < PRM_CSS_MAX_CLIENTS);

	  break;
	}

      prev = p;
    }

  assert (css_Active_conn_anchor == NULL
	  || (css_Active_conn_anchor != NULL && p != NULL));

  css_shutdown_conn (conn);
  css_dealloc_conn (conn);

  csect_exit_critical_section (&css_Active_conn_csect);
}

/*
 * css_print_conn_entry_info() - print connection entry information to stderr
 *   return: void
 *   conn(in): connection entry
 */
void
css_print_conn_entry_info (CSS_CONN_ENTRY * conn)
{
  fprintf (stderr,
	   "CONN_ENTRY: %p, next(%p), idx(%d),fd(%d),request_id(%d),transaction_id(%d),client_id(%d)\n",
	   conn, conn->next, conn->idx, conn->fd, conn->request_id,
	   conn->transaction_id, conn->client_id);
}

/*
 * css_print_conn_list() - print active connection list to stderr
 *   return: void
 */
void
css_print_conn_list (void)
{
  CSS_CONN_ENTRY *conn, *next;
  int i;

  if (css_Active_conn_anchor != NULL)
    {
      csect_enter_critical_section_as_reader (NULL, &css_Active_conn_csect,
					      INF_WAIT);

      fprintf (stderr, "active conn list (%d)\n", css_Num_active_conn);

      for (conn = css_Active_conn_anchor, i = 0; conn != NULL;
	   conn = next, i++)
	{
	  next = conn->next;
	  css_print_conn_entry_info (conn);
	}

      assert (i == css_Num_active_conn);

      csect_exit_critical_section (&css_Active_conn_csect);
    }
}

/*
 * css_print_free_conn_list() - print free connection list to stderr
 *   return: void
 */
void
css_print_free_conn_list (void)
{
  CSS_CONN_ENTRY *conn, *next;
  int i;

  if (css_Free_conn_anchor != NULL)
    {
      csect_enter_critical_section_as_reader (NULL, &css_Free_conn_csect,
					      INF_WAIT);

      fprintf (stderr, "free conn list (%d)\n", css_Num_free_conn);

      for (conn = css_Free_conn_anchor, i = 0; conn != NULL; conn = next, i++)
	{
	  next = conn->next;
	  css_print_conn_entry_info (conn);
	}

      assert (i == css_Num_free_conn);

      csect_exit_critical_section (&css_Free_conn_csect);
    }
}

/*
 * css_register_handler_routines() - enroll handler routines
 *   return: void
 *   connect_handler(in): connection handler function pointer
 *   conn(in): connection entry
 *   request_handler(in): request handler function pointer
 *   connection_error_handler(in): error handler function pointer
 *
 * Note: This is the routine that will enroll various handler routines
 *       that the client/server interface software may use. Any of these
 *       routines may be given a NULL value in which case a default routine
 *       will be used, or nothing will be done.
 *
 *       The connect handler is called when a new connection is made.
 *
 *       The request handler is called to handle a new request. This must
 *       return non zero, otherwise, the server will halt.
 *
 *       The abort handler is called by the server when an abort command
 *       is sent from the client.
 *
 *       The alloc function is called instead of malloc when new buffers
 *       are to be created.
 *
 *       The free function is called when a buffer is to be released.
 *
 *       The error handler function is called when the client/server system
 *       detects an error it considers to be fatal.
 */
void
css_register_handler_routines (int (*connect_handler) (CSS_CONN_ENTRY * conn),
			       CSS_THREAD_FN request_handler,
			       CSS_THREAD_FN connection_error_handler)
{
  css_Connect_handler = connect_handler;
  css_Request_handler = request_handler;

  if (connection_error_handler)
    {
      css_Connection_error_handler = connection_error_handler;
    }
}

/*
 * css_common_connect() - actually try to make a connection to a server.
 *   return: connection entry if success, or NULL
 *   conn(in): connection entry will be connected
 *   rid(out): request id
 *   host_name(in): host name of server
 *   connect_type(in):
 *   server_name(in):
 *   server_name_length(in):
 *   port(in):
 *   sendmagic(in): true for connect to master, false for connect to server
 */
static CSS_CONN_ENTRY *
css_common_connect (CSS_CONN_ENTRY * conn, unsigned short *rid,
		    const char *host_name, int connect_type,
		    const char *server_name, int server_name_length, int port,
		    bool sendmagic)
{
  SOCKET fd;

  fd = css_tcp_client_open ((char *) host_name, port);

  if (!IS_INVALID_SOCKET (fd))
    {
      conn->fd = fd;

      /* when send to master, we must send magic first,
       * otherwise needn't send the magic packet
       */
      if (sendmagic)
	{
	  if (css_send_magic (conn) != NO_ERRORS)
	    {
	      return NULL;
	    }
	}

      if (css_send_request (conn, connect_type, rid, server_name,
			    server_name_length) == NO_ERRORS)
	{
	  return (conn);
	}
    }

  return (NULL);
}

/*
 * css_connect_to_master_server() - Connect to the master from the server.
 *   return: connection entry if success, or NULL
 *   master_port_id(in):
 *   server_name(in): name + version
 *   name_length(in):
 */
CSS_CONN_ENTRY *
css_connect_to_master_server (int master_port_id, const char *server_name,
			      int name_length)
{
  char hname[MAXHOSTNAMELEN];
  CSS_CONN_ENTRY *conn;
  unsigned short rid;
  int response, response_buff;
  int server_port_id;
  int connection_protocol;
#if !defined(WINDOWS)
  char *pname;
  int datagram_fd, socket_fd;
#endif

  css_Service_id = master_port_id;
  if (GETHOSTNAME (hname, MAXHOSTNAMELEN) == 0)
    {
      conn = css_make_conn (0);
      if (conn == NULL)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_ERROR_DURING_SERVER_CONNECT, 1,
			       server_name);
	  return (NULL);
	}

      /* select the connection protocol, for PC's this will always be new */
      if (css_Server_use_new_connection_protocol)
	{
	  connection_protocol = SERVER_REQUEST_NEW;
	}
      else
	{
	  connection_protocol = SERVER_REQUEST;
	}

      if (css_common_connect (conn, &rid, hname, connection_protocol,
			      server_name, name_length,
			      master_port_id, true) == NULL)
	{
	  css_free_conn (conn);
	  return (NULL);
	}
      else
	{
	  if (css_readn (conn->fd, (char *) &response_buff,
			 sizeof (int), -1) == sizeof (int))
	    {
	      response = ntohl (response_buff);
	      TRACE
		("css_connect_to_master_server received %d as response from master\n",
		 response);

	      switch (response)
		{
		case SERVER_ALREADY_EXISTS:
		  css_free_conn (conn);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ERR_CSS_SERVER_ALREADY_EXISTS, 1, server_name);
		  return (NULL);

		case SERVER_REQUEST_ACCEPTED_NEW:
		  /*
		   * Master requests a new-style connect, must go get
		   * our port id and set up our connection socket.
		   * For drivers, we don't need a connection socket and we
		   * don't want to allocate a bunch of them.  Let a flag variable
		   * control whether or not we actually create one of these.
		   */
		  if (css_Server_inhibit_connection_socket)
		    {
		      server_port_id = -1;
		    }
		  else
		    {
		      server_port_id = css_open_server_connection_socket ();
		    }

		  response = htonl (server_port_id);
		  css_net_send (conn, (char *) &response, sizeof (int), -1);
		  /* this connection remains our only contact with the master */
		  return conn;

		case SERVER_REQUEST_ACCEPTED:
#if defined(WINDOWS)
		  /* PC's can't handle this style of connection at all */
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ERR_CSS_ERROR_DURING_SERVER_CONNECT, 1,
			  server_name);
		  css_free_conn (conn);
		  return (NULL);
#else /* WINDOWS */
		  /* send the "pathname" for the datagram */
		  /* be sure to open the datagram first.  */
		  pname = tempnam (NULL, "cubrid");
		  if (pname)
		    {
		      if (css_tcp_setup_server_datagram (pname, &socket_fd)
			  && (css_send_data (conn, rid, pname,
					     strlen (pname) + 1) == NO_ERRORS)
			  && (css_tcp_listen_server_datagram (socket_fd,
							      &datagram_fd)))
			{
			  (void) unlink (pname);
			  /* don't use free_and_init on pname since
			     it came from tempnam() */
			  free (pname);
			  css_free_conn (conn);
			  return (css_make_conn (datagram_fd));
			}
		      else
			{
			  /* don't use free_and_init on pname since
			     it came from tempnam() */
			  free (pname);
			  er_set_with_oserror (ER_ERROR_SEVERITY,
					       ARG_FILE_LINE,
					       ERR_CSS_ERROR_DURING_SERVER_CONNECT,
					       1, server_name);
			  css_free_conn (conn);
			  return (NULL);
			}
		    }
		  else
		    {
		      /* Could not create the temporary file */
		      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
					   ERR_CSS_ERROR_DURING_SERVER_CONNECT,
					   1, server_name);
		      css_free_conn (conn);
		      return (NULL);
		    }
#endif /* WINDOWS */
		}
	    }
	}
      css_free_conn (conn);
    }
  return (NULL);
}

/*
 * css_find_conn_by_tran_index() - find connection entry having given
 *                                 transaction id
 *   return: connection entry if find, or NULL
 *   tran_index(in): transaction id
 */
CSS_CONN_ENTRY *
css_find_conn_by_tran_index (int tran_index)
{
  CSS_CONN_ENTRY *conn = NULL, *next;

  if (css_Active_conn_anchor != NULL)
    {
      csect_enter_critical_section_as_reader (NULL, &css_Active_conn_csect,
					      INF_WAIT);

      for (conn = css_Active_conn_anchor; conn != NULL; conn = next)
	{
	  next = conn->next;
	  if (conn->transaction_id == tran_index)
	    {
	      break;
	    }
	}

      csect_exit_critical_section (&css_Active_conn_csect);
    }

  return conn;
}

/*
 * css_find_conn_from_fd() - find a connection having given socket fd.
 *   return: connection entry if find, or NULL
 *   fd(in): socket fd
 */
CSS_CONN_ENTRY *
css_find_conn_from_fd (SOCKET fd)
{
  CSS_CONN_ENTRY *conn = NULL, *next;

  if (css_Active_conn_anchor != NULL)
    {
      csect_enter_critical_section_as_reader (NULL, &css_Active_conn_csect,
					      INF_WAIT);

      for (conn = css_Active_conn_anchor; conn != NULL; conn = next)
	{
	  next = conn->next;
	  if (conn->fd == fd)
	    {
	      break;
	    }
	}

      csect_exit_critical_section (&css_Active_conn_csect);
    }
  return conn;
}

/*
 * css_shutdown_conn_by_tran_index() - shutdown connection having given
 *                                     transaction id
 *   return: void
 *   tran_index(in): transaction id
 */
void
css_shutdown_conn_by_tran_index (int tran_index)
{
  CSS_CONN_ENTRY *conn = NULL;

  if (css_Active_conn_anchor != NULL)
    {
      csect_enter_critical_section (NULL, &css_Active_conn_csect, INF_WAIT);

      for (conn = css_Active_conn_anchor; conn != NULL; conn = conn->next)
	{
	  if (conn->transaction_id == tran_index)
	    {
	      if (conn->status == CONN_OPEN)
		{
		  conn->status = CONN_CLOSING;
		}
	      break;
	    }
	}

      csect_exit_critical_section (&css_Active_conn_csect);
    }
}

/*
 * css_get_request_id() - return the next valid request id
 *   return: request id
 *   conn(in): connection entry
 */
unsigned short
css_get_request_id (CSS_CONN_ENTRY * conn)
{
  unsigned short old_rid;
  unsigned short request_id;
  csect_enter_critical_section (NULL, &conn->csect, INF_WAIT);

  old_rid = conn->request_id++;
  if (conn->request_id == 0)
    {
      conn->request_id++;
    }

  while (conn->request_id != old_rid)
    {
      if (css_is_valid_request_id (conn, conn->request_id))
	{
	  request_id = conn->request_id;
	  csect_exit_critical_section (&conn->csect);
	  return (request_id);
	}
      else
	{
	  conn->request_id++;
	  if (conn->request_id == 0)
	    {
	      conn->request_id++;
	    }
	}
    }
  csect_exit_critical_section (&conn->csect);

  /* Should never reach this point */
  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ERR_CSS_REQUEST_ID_FAILURE, 0);
  return (0);
}

/*
 * css_abort_request() - helper routine to actually send the abort request.
 *   return:  0 if success, or error code
 *   conn(in): connection entry
 *   rid(in): request id
 */
static int
css_abort_request (CSS_CONN_ENTRY * conn, unsigned short rid)
{
  NET_HEADER header = DEFAULT_HEADER_DATA;

  header.type = htonl (ABORT_TYPE);
  header.request_id = htonl (rid);
  header.transaction_id = htonl (conn->transaction_id);
  header.db_error = htonl (conn->db_error);

  /* timeout in mili-second in css_net_send() */
  return (css_net_send (conn, (char *) &header, sizeof (NET_HEADER),
			PRM_TCP_CONNECTION_TIMEOUT * 1000));
}

/*
 * css_send_abort_request() - abort an outstanding request.
 *   return:  0 if success, or error code
 *   conn(in): connection entry
 *   request_id(in): request id
 *
 * Note: Once this is issued, any queued data buffers for this command will be
 *       released.
 */
int
css_send_abort_request (CSS_CONN_ENTRY * conn, unsigned short request_id)
{
  int rc;

  if (!conn || conn->status != CONN_OPEN)
    {
      return (CONNECTION_CLOSED);
    }

  csect_enter_critical_section (NULL, &conn->csect, INF_WAIT);

  css_remove_unexpected_packets (conn, request_id);
  rc = css_abort_request (conn, request_id);

  csect_exit_critical_section (&conn->csect);
  return rc;
}

/*
 * css_read_header() - helper routine that will read a header from the socket.
 *   return: 0 if success, or error code
 *   conn(in): connection entry
 *   local_header(in):
 *
 * Note: It is a blocking read.
 */
int
css_read_header (CSS_CONN_ENTRY * conn, const NET_HEADER * local_header)
{
  int buffer_size;
  int rc = 0;

  buffer_size = sizeof (NET_HEADER);

  if (conn->stop_talk == true)
    {
      return (CONNECTION_CLOSED);
    }

  do
    {
      rc = css_net_read_header (conn->fd, (char *) local_header,
				&buffer_size);
    }
  while (rc == INTERRUPTED_READ);

  if (rc == NO_ERRORS && ntohl (local_header->type) == CLOSE_TYPE)
    {
      return (CONNECTION_CLOSED);
    }
  if (!((rc == NO_ERRORS) || (rc == RECORD_TRUNCATED)))
    {
      return (CONNECTION_CLOSED);
    }

  ((NET_HEADER *) local_header)->transaction_id =
    htonl (log_get_local_by_global_tran_id
	   (ntohl (local_header->transaction_id)));
  conn->transaction_id = ntohl (local_header->transaction_id);
  conn->db_error = (int) ntohl (local_header->db_error);

  return (rc);
}

/*
 * css_receive_request() - receive request from client
 *   return: 0 if success, or error code
 *   conn(in): connection entry
 *   rid(out): request id
 *   request(out): request
 *   buffer_size(out): request data size
 */
int
css_receive_request (CSS_CONN_ENTRY * conn, unsigned short *rid,
		     int *request, int *buffer_size)
{
  return css_return_queued_request (conn, rid, request, buffer_size);
}

/*
 * css_read_and_queue() - Attempt to read any data packet from the connection.
 *   return: 0 if success, or error code
 *   conn(in): connection entry
 *   type(out): request type
 */
int
css_read_and_queue (CSS_CONN_ENTRY * conn, int *type)
{
  int rc;
  NET_HEADER header = DEFAULT_HEADER_DATA;

  *type = 0;
  if (!conn || conn->status != CONN_OPEN)
    {
      return (ERROR_ON_READ);
    }

  rc = css_read_header (conn, &header);

  if (conn->stop_talk == true)
    {
      return (CONNECTION_CLOSED);
    }

  if (rc != NO_ERRORS)
    {
      er_log_debug (ARG_FILE_LINE,
		    "Read header error. local transaction id: %d . error id: %d\n",
		    conn->transaction_id, rc);
      return rc;
    }

  /* 
   * handling for packet routine control
   */
  if (COMMAND_TYPE == ntohl (header.type)
      && NET_PROXY_SET_ROUTER == ntohs (header.function_code))
    {
      unsigned short rid;
      int pak_node_id = ntohl (header.node_id);

      /*
       * Step 1: if no change then no action, return directly.
       */
      if (conn->trans_conn && conn->trans_conn->node_id == pak_node_id)
	{
	  return NO_ERRORS;
	}

      /*
       * Step 2: close old connection if exists
       */
      if (NULL != conn->trans_conn)
	{
	  CSS_CONN_ENTRY *transConn = conn->trans_conn;
	  conn->trans_conn = NULL;
	  transConn->trans_conn = NULL;
	  css_close_connection_to_remote_server (transConn);
	}

      /*
       * Step 3 : establish new connection
       */
      if (pak_node_id != 0)
	{
	  conn->trans_conn =
	    css_open_connection_to_remote_server (ntohl
						  (header.transaction_id),
						  pak_node_id, &rid);
	  if (conn->trans_conn == NULL)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "cannot open connection to remote server. remote node_id : %d. \n",
			    pak_node_id);
	      return CONNECTION_CLOSED;
	    }
	  conn->trans_conn->trans_conn = conn;
	}

      return NO_ERRORS;
    }

  /* transmit the packages */
  if (conn->trans_conn != NULL)
    {
      LOG_TDES *tdes;
      int tran_index = (int) ntohl (header.transaction_id);

      if (tran_index != NULL_TRANID)
	{
	  if ((tdes = LOG_FIND_TDES (tran_index)) != NULL)
	    {
	      header.transaction_id = htonl (tdes->global_tran_id);
	    }
	  else
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_UNKNOWN_TRANINDEX, 1, tran_index);
	      return -1;
	    }
	}

      rc = css_net_transmit_package (conn, &header);
      if (rc != NO_ERRORS)
	{
	  er_log_debug (ARG_FILE_LINE,
			"Transmit packages may failed: error id: %d, tran_index  : %d, request_id : %d \n",
			rc, tran_index, conn->trans_conn->request_id);
	}
      return rc;
    }

  *type = ntohl (header.type);
  css_queue_packet (conn, (int) ntohl (header.type),
		    (unsigned short) ntohl (header.request_id),
		    &header, sizeof (NET_HEADER));
  return (rc);
}

/*
 * css_receive_data() - receive a data for an associated request.
 *   return: 0 if success, or error code
 *   conn(in): connection entry
 *   req_id(in): request id
 *   buffer(out): buffer for data
 *   buffer_size(out): buffer size
 *
 * Note: this is a blocking read.
 */
int
css_receive_data (CSS_CONN_ENTRY * conn, unsigned short req_id,
		  char **buffer, int *buffer_size)
{
  int *r, rc;

  /* at here, do not use stack variable; must alloc it */
  r = (int *) malloc (sizeof (int));
  if (r == NULL)
    {
      return NO_DATA_AVAILABLE;
    }

  css_return_queued_data (conn, req_id, buffer, buffer_size, r);
  rc = *r;

  free_and_init (r);
  return rc;
}

/*
 * css_return_eid_from_conn() - get enquiry id from connection entry
 *   return: enquiry id
 *   conn(in): connection entry
 *   rid(in): request id
 */
unsigned int
css_return_eid_from_conn (CSS_CONN_ENTRY * conn, unsigned short rid)
{
  return css_make_eid ((unsigned short) conn->idx, rid);
}

/*
 * css_make_eid() - make enquiry id
 *   return: enquiry id
 *   entry_id(in): connection entry id
 *   rid(in): request id
 */
static unsigned int
css_make_eid (unsigned short entry_id, unsigned short rid)
{
  int top;

  top = entry_id;
  return ((top << 16) | rid);
}

/* CSS_CONN_ENTRY's queues related functions */

/*
 * css_make_queue_entry() - make queue entey
 *   return: queue entry
 *   conn(in): connection entry
 *   key(in):
 *   buffer(in):
 *   size(in):
 *   rc(in):
 *   transid(in):
 *   db_error(in):
 */
static CSS_QUEUE_ENTRY *
css_make_queue_entry (CSS_CONN_ENTRY * conn, unsigned int key,
		      char *buffer, int size, int rc,
		      int transid, int db_error)
{
  CSS_QUEUE_ENTRY *p;

  if (conn->free_queue_list != NULL)
    {
      p = (CSS_QUEUE_ENTRY *) conn->free_queue_list;
      conn->free_queue_list = p->next;
      conn->free_queue_count--;
    }
  else
    {
      p = (CSS_QUEUE_ENTRY *) malloc (sizeof (CSS_QUEUE_ENTRY));
    }

  if (p != NULL)
    {
      p->key = key;
      p->buffer = buffer;
      p->size = size;
      p->rc = rc;
      p->transaction_id = transid;
      p->db_error = db_error;
    }

  return p;
}

/*
 * css_free_queue_entry() - free queue entry
 *   return: void
 *   conn(in): connection entry
 *   entry(in): queue entry
 */
static void
css_free_queue_entry (CSS_CONN_ENTRY * conn, CSS_QUEUE_ENTRY * entry)
{
  if (entry != NULL)
    {
      free_and_init (entry->buffer);

      entry->next = conn->free_queue_list;
      conn->free_queue_list = entry;
      conn->free_queue_count++;
    }
}

/*
 * css_add_queue_entry() - add queue entry
 *   return: NO_ERRORS if success, or css error code
 *   conn(in): connection entry
 *   list(in): queue list
 *   request_id(in): request id
 *   buffer(in):
 *   buffer_size(in):
 *   rc(in):
 *   transid(in):
 *   db_error(in):
 */
static int
css_add_queue_entry (CSS_CONN_ENTRY * conn, CSS_LIST * list,
		     unsigned short request_id, char *buffer,
		     int buffer_size, int rc, int transid, int db_error)
{
  CSS_QUEUE_ENTRY *p;
  int r = NO_ERRORS;

  p = css_make_queue_entry (conn, request_id, buffer, buffer_size, rc,
			    transid, db_error);
  if (p == NULL)
    {
      r = CANT_ALLOC_BUFFER;
    }
  else
    {
      css_add_list (list, p);
    }

  return r;
}

/*
 * css_find_queue_entry_by_key() - find queue entry using key
 *   return: status of traverse
 *   data(in): queue entry
 *   user(in): search argument
 */
static int
css_find_queue_entry_by_key (void *data, void *user)
{
  CSS_QUEUE_SEARCH_ARG *arg = (CSS_QUEUE_SEARCH_ARG *) user;
  CSS_QUEUE_ENTRY *p = (CSS_QUEUE_ENTRY *) data;

  if (p->key == arg->key)
    {
      arg->entry_ptr = p;
      if (arg->remove_entry)
	{
	  return TRAV_STOP_DELETE;
	}
      else
	{
	  return TRAV_STOP;
	}
    }

  return TRAV_CONT;
}

/*
 * css_find_queue_entry() - find queue entry
 *   return: queue entry
 *   list(in): queue list
 *   key(in): key
 */
static CSS_QUEUE_ENTRY *
css_find_queue_entry (CSS_LIST * list, unsigned int key)
{
  CSS_QUEUE_SEARCH_ARG arg;

  arg.entry_ptr = NULL;
  arg.key = key;
  arg.remove_entry = 0;

  css_traverse_list (list, css_find_queue_entry_by_key, &arg);

  return arg.entry_ptr;
}

/*
 * css_find_and_remove_queue_entry() - find queue entry and remove it
 *   return: queue entry
 *   list(in): queue list
 *   key(in): key
 */
static CSS_QUEUE_ENTRY *
css_find_and_remove_queue_entry (CSS_LIST * list, unsigned int key)
{
  CSS_QUEUE_SEARCH_ARG arg;

  arg.entry_ptr = NULL;
  arg.key = key;
  arg.remove_entry = 1;

  css_traverse_list (list, css_find_queue_entry_by_key, &arg);

  return arg.entry_ptr;
}

/*
 * css_make_wait_queue_entry() - make wait queue entry
 *   return: wait queue entry
 *   conn(in): connection entry
 *   key(in):
 *   buffer(out):
 *   size(out):
 *   rc(out):
 */
static CSS_WAIT_QUEUE_ENTRY *
css_make_wait_queue_entry (CSS_CONN_ENTRY * conn, unsigned int key,
			   char **buffer, int *size, int *rc)
{
  CSS_WAIT_QUEUE_ENTRY *p;

  if (conn->free_wait_queue_list != NULL)
    {
      p = (CSS_WAIT_QUEUE_ENTRY *) conn->free_wait_queue_list;
      conn->free_wait_queue_list = p->next;
      conn->free_wait_queue_count--;
    }
  else
    {
      p = (CSS_WAIT_QUEUE_ENTRY *) malloc (sizeof (CSS_WAIT_QUEUE_ENTRY));
    }

  if (p != NULL)
    {
      p->key = key;
      p->buffer = buffer;
      p->size = size;
      p->rc = rc;
      p->thrd_entry = thread_get_thread_entry_info ();
    }

  return p;
}

/*
 * css_free_wait_queue_entry() - free wait queue entry
 *   return: void
 *   conn(in): connection entry
 *   entry(in): wait queue entry
 */
static void
css_free_wait_queue_entry (CSS_CONN_ENTRY * conn,
			   CSS_WAIT_QUEUE_ENTRY * entry)
{
  if (entry != NULL)
    {
      if (entry->thrd_entry)
	{
	  thread_wakeup (entry->thrd_entry, THREAD_CSS_QUEUE_RESUMED);
	}

      entry->next = conn->free_wait_queue_list;
      conn->free_wait_queue_list = entry;
      conn->free_wait_queue_count++;
    }
}

/*
 * css_add_wait_queue_entry() - add wait queue entry
 *   return: wait queue entry
 *   conn(in): connection entry
 *   list(in): wait queue list
 *   request_id(in): request id
 *   buffer(out):
 *   buffer_size(out):
 *   rc(out):
 */
static CSS_WAIT_QUEUE_ENTRY *
css_add_wait_queue_entry (CSS_CONN_ENTRY * conn, CSS_LIST * list,
			  unsigned short request_id, char **buffer,
			  int *buffer_size, int *rc)
{
  CSS_WAIT_QUEUE_ENTRY *p;

  p = css_make_wait_queue_entry (conn, request_id, buffer, buffer_size, rc);
  if (p != NULL)
    {
      css_add_list (list, p);
    }

  return p;
}

/*
 * find_wait_queue_entry_by_key() - find wait queue entry using key
 *   return: status of traverse
 *   data(in): wait queue entry
 *   user(in): search argument
 */
static int
find_wait_queue_entry_by_key (void *data, void *user)
{
  CSS_WAIT_QUEUE_SEARCH_ARG *arg = (CSS_WAIT_QUEUE_SEARCH_ARG *) user;
  CSS_WAIT_QUEUE_ENTRY *p = (CSS_WAIT_QUEUE_ENTRY *) data;

  if (p->key == arg->key)
    {
      arg->entry_ptr = p;
      if (arg->remove_entry)
	{
	  return TRAV_STOP_DELETE;
	}
      else
	{
	  return TRAV_STOP;
	}
    }

  return TRAV_CONT;
}

/*
 * css_find_and_remove_wait_queue_entry() - find wait queue entry and remove it
 *   return: wait queue entry
 *   list(in): wait queue list
 *   key(in):
 */
static CSS_WAIT_QUEUE_ENTRY *
css_find_and_remove_wait_queue_entry (CSS_LIST * list, unsigned int key)
{
  CSS_WAIT_QUEUE_SEARCH_ARG arg;

  arg.entry_ptr = NULL;
  arg.key = key;
  arg.remove_entry = 1;

  css_traverse_list (list, find_wait_queue_entry_by_key, &arg);

  return arg.entry_ptr;
}


/*
 * css_queue_packet() - queue packet
 *   return: void
 *   conn(in): connection entry
 *   type(in): packet type
 *   request_id(in): request id
 *   header(in): network header
 *   size(in): packet size
 */
static void
css_queue_packet (CSS_CONN_ENTRY * conn, int type,
		  unsigned short request_id, const NET_HEADER * header,
		  int size)
{
  THREAD_ENTRY *wait_thrd = NULL, *p, *next;

  csect_enter_critical_section (NULL, &conn->csect, INF_WAIT);

  conn->transaction_id = ntohl (header->transaction_id);
  conn->db_error = (int) ntohl (header->db_error);

  switch (type)
    {
    case CLOSE_TYPE:
      css_process_close_packet (conn);
      break;
    case ABORT_TYPE:
      css_process_abort_packet (conn, request_id);
      break;
    case DATA_TYPE:
      css_queue_data_packet (conn, request_id, header, &wait_thrd);
      break;
    case ERROR_TYPE:
      css_queue_error_packet (conn, request_id, header);
      break;
    case COMMAND_TYPE:
      css_queue_command_packet (conn, request_id, header, size);
      break;
    default:
      CSS_TRACE2 ("Asked to queue an unknown packet id = %d.\n", type);
    }

  p = wait_thrd;
  while (p != NULL)
    {
      thread_lock_entry (p);
      p = p->next_wait_thrd;
    }
  csect_exit_critical_section (&conn->csect);

  p = wait_thrd;
  while (p != NULL)
    {
      next = p->next_wait_thrd;
      p->resume_status = THREAD_CSS_QUEUE_RESUMED;
      COND_SIGNAL (p->wakeup_cond);
      p->next_wait_thrd = NULL;
      thread_unlock_entry (p);
      p = next;
    }
}

/*
 * css_process_close_packet() - prccess close packet
 *   return: void
 *   conn(in): conenction entry
 */
static void
css_process_close_packet (CSS_CONN_ENTRY * conn)
{
  if (!IS_INVALID_SOCKET (conn->fd))
    {
      css_shutdown_socket (conn->fd);
      conn->fd = INVALID_SOCKET;
    }

  conn->status = CONN_CLOSED;
}

/*
 * css_process_abort_packet() - process abort packet
 *   return: void
 *   conn(in): connection entry
 *   request_id(in): request id
 */
static void
css_process_abort_packet (CSS_CONN_ENTRY * conn, unsigned short request_id)
{
  CSS_QUEUE_ENTRY *request, *data;

  request = css_find_and_remove_queue_entry (&conn->request_queue,
					     request_id);
  if (request)
    {
      css_free_queue_entry (conn, request);
    }

  data = css_find_and_remove_queue_entry (&conn->data_queue, request_id);
  if (data)
    {
      css_free_queue_entry (conn, data);
    }

  if (css_find_queue_entry (&conn->abort_queue, request_id) == NULL)
    {
      css_add_queue_entry (conn, &conn->abort_queue, request_id, NULL, 0,
			   NO_ERRORS, conn->transaction_id, conn->db_error);
    }
}

/*
 * css_queue_data_packet() - queue data packet
 *   return: void
 *   conn(in): connection entry
 *   request_id(in): request id
 *   header(in): network header
 *   wake_thrd(out): thread that wake up
 */
static void
css_queue_data_packet (CSS_CONN_ENTRY * conn, unsigned short request_id,
		       const NET_HEADER * header, THREAD_ENTRY ** wake_thrd)
{
  THREAD_ENTRY *thrd = NULL, *last = NULL;
  CSS_QUEUE_ENTRY *buffer_entry;
  CSS_WAIT_QUEUE_ENTRY *data_wait = NULL;
  char *buffer = NULL;
  int rc;
  int size;			/* size to be read */

  /* setup wake_thrd. hmm.. consider recursion */
  if (*wake_thrd != NULL)
    {
      last = *wake_thrd;
      while (last->next_wait_thrd != NULL)
	{
	  last = last->next_wait_thrd;
	}
    }

  size = ntohl (header->buffer_size);
  /* check if user have given a buffer */
  buffer_entry = css_find_and_remove_queue_entry (&conn->buffer_queue,
						  request_id);
  if (buffer_entry != NULL)
    {
      /* compare data and buffer size. if different? something wrong!!! */
      if (size > buffer_entry->size)
	{
	  size = buffer_entry->size;
	}
      buffer = buffer_entry->buffer;
      buffer_entry->buffer = NULL;

      css_free_queue_entry (conn, buffer_entry);
    }
  else if (size == 0)
    {
      buffer = NULL;
    }
  else
    {
      buffer = (char *) malloc (size);
    }

  /*
   * check if there exists thread waiting for data.
   * Add to wake_thrd list.
   */
  data_wait = css_find_and_remove_wait_queue_entry (&conn->data_wait_queue,
						    request_id);

  if (data_wait != NULL)
    {
      thrd = data_wait->thrd_entry;
      thrd->next_wait_thrd = NULL;
      if (last == NULL)
	{
	  *wake_thrd = thrd;
	}
      else
	{
	  last->next_wait_thrd = thrd;
	}
      last = thrd;
    }

  /* receive data into buffer and queue data if there's no waiting thread */
  if (buffer != NULL)
    {
      do
	{
	  /* timeout in mili-second in css_net_recv() */
	  rc = css_net_recv (conn->fd, buffer, &size,
			     PRM_TCP_CONNECTION_TIMEOUT * 1000);
	}
      while (rc == INTERRUPTED_READ);

      if (rc == NO_ERRORS || rc == RECORD_TRUNCATED)
	{
	  if (!css_is_request_aborted (conn, request_id))
	    {
	      if (data_wait == NULL)
		{
		  /* if waiter not exists, add to data queue */
		  css_add_queue_entry (conn, &conn->data_queue, request_id,
				       buffer, size, rc, conn->transaction_id,
				       conn->db_error);
		  return;
		}
	      else
		{
		  *data_wait->buffer = buffer;
		  *data_wait->size = size;
		  *data_wait->rc = rc;
		  data_wait->thrd_entry = NULL;
		  css_free_wait_queue_entry (conn, data_wait);
		  return;
		}
	    }
	}
      /* if error occurred */
      free_and_init (buffer);
    }
  else
    {
      rc = CANT_ALLOC_BUFFER;
      css_read_remaining_bytes (conn->fd, sizeof (int) + size);
      if (!css_is_request_aborted (conn, request_id))
	{
	  if (data_wait == NULL)
	    {
	      css_add_queue_entry (conn, &conn->data_queue, request_id, NULL,
				   0, rc, conn->transaction_id,
				   conn->db_error);
	      return;
	    }
	}
    }

  /* if error was occurred, setup error status */
  if (data_wait != NULL)
    {
      *data_wait->buffer = NULL;
      *data_wait->size = 0;
      *data_wait->rc = rc;
    }
}

/*
 * css_queue_error_packet() - queue error packet
 *   return: void
 *   conn(in): connection entry
 *   request_id(in): request id
 *   header(in): network header
 */
static void
css_queue_error_packet (CSS_CONN_ENTRY * conn, unsigned short request_id,
			const NET_HEADER * header)
{
  char *buffer;
  int rc;
  int size;

  size = ntohl (header->buffer_size);
  buffer = (char *) malloc (size);

  if (buffer != NULL)
    {
      do
	{
	  /* timeout in mili-second in css_net_recv() */
	  rc = css_net_recv (conn->fd, buffer, &size,
			     PRM_TCP_CONNECTION_TIMEOUT * 1000);
	}
      while (rc == INTERRUPTED_READ);

      if (rc == NO_ERRORS || rc == RECORD_TRUNCATED)
	{
	  if (!css_is_request_aborted (conn, request_id))
	    {
	      css_add_queue_entry (conn, &conn->error_queue, request_id,
				   buffer, size, rc, conn->transaction_id,
				   conn->db_error);
	      return;
	    }
	}
      free_and_init (buffer);
    }
  else
    {
      rc = CANT_ALLOC_BUFFER;
      css_read_remaining_bytes (conn->fd, sizeof (int) + size);
      if (!css_is_request_aborted (conn, request_id))
	{
	  css_add_queue_entry (conn, &conn->error_queue, request_id, NULL, 0,
			       rc, conn->transaction_id, conn->db_error);
	}
    }
}

/*
 * css_queue_command_packet() - queue command packet
 *   return: void
 *   conn(in): connection entry
 *   request_id(in): request id
 *   header(in): network header
 *   size(in): packet size
 */
static void
css_queue_command_packet (CSS_CONN_ENTRY * conn, unsigned short request_id,
			  const NET_HEADER * header, int size)
{
  NET_HEADER *p;
  NET_HEADER data_header = { 0, 0, 0, 0, 0, 0, 0, 0, 0 };

  if (css_is_request_aborted (conn, request_id))
    {
      return;
    }

  if (conn->free_net_header_list != NULL)
    {
      p = (NET_HEADER *) conn->free_net_header_list;
      conn->free_net_header_list = (char *) (*(UINTPTR *) p);
      conn->free_net_header_count--;
    }
  else
    {
      p = (NET_HEADER *) malloc (sizeof (NET_HEADER));
    }

  if (p != NULL)
    {
      memcpy ((char *) p, (char *) header, sizeof (NET_HEADER));
      css_add_queue_entry (conn, &conn->request_queue, request_id, (char *) p,
			   size, NO_ERRORS, conn->transaction_id,
			   conn->db_error);
      if (ntohl (header->buffer_size) > 0)
	{
	  css_read_header (conn, &data_header);
	  css_queue_packet (conn, (int) ntohl (data_header.type),
			    (unsigned short) ntohl (data_header.request_id),
			    &data_header, sizeof (NET_HEADER));
	}
    }
}

/*
 * css_request_aborted() - check request is aborted
 *   return: true if aborted, or false
 *   conn(in): connection entry
 *   request_id(in): request id
 */
static bool
css_is_request_aborted (CSS_CONN_ENTRY * conn, unsigned short request_id)
{
  CSS_QUEUE_ENTRY *p;

  p = css_find_queue_entry (&conn->abort_queue, request_id);
  if (p != NULL)
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * css_return_queued_request() - get request from queue
 *   return: 0 if success, or error code
 *   conn(in): connection entry
 *   rid(out): request id
 *   request(out): request
 *   buffer_size(out): request buffer size
 */
int
css_return_queued_request (CSS_CONN_ENTRY * conn, unsigned short *rid,
			   int *request, int *buffer_size)
{
  CSS_QUEUE_ENTRY *p;
  NET_HEADER *buffer;
  int rc;

  csect_enter_critical_section (NULL, &conn->csect, INF_WAIT);

  if (conn->status == CONN_OPEN)
    {
      p =
	(CSS_QUEUE_ENTRY *) css_remove_list_from_head (&conn->request_queue);
      if (p != NULL)
	{
	  *rid = p->key;
	  buffer = (NET_HEADER *) p->buffer;
	  p->buffer = NULL;
	  *request = ntohs (buffer->function_code);
	  *buffer_size = ntohl (buffer->buffer_size);
	  conn->transaction_id = p->transaction_id;
	  conn->db_error = p->db_error;
	  *(UINTPTR *) buffer = (UINTPTR) conn->free_net_header_list;
	  conn->free_net_header_list = (char *) buffer;
	  conn->free_net_header_count++;
	  css_free_queue_entry (conn, p);
	  rc = NO_ERRORS;
	}
      else
	{
	  rc = NO_DATA_AVAILABLE;
	}
    }
  else
    {
      rc = CONN_CLOSED;
    }

  csect_exit_critical_section (&conn->csect);
  return rc;
}

/*
 * css_return_queued_data_timeout() - get request data from queue until timeout
 *   return: 0 if success, or error code
 *   conn(in): connection entry
 *   rid(out): request id
 *   buffer(out): data buffer
 *   bufsize(out): buffer size
 *   rc(out):
 *   waitsec: timeout second
 */
static int
css_return_queued_data_timeout (CSS_CONN_ENTRY * conn, unsigned short rid,
				char **buffer, int *bufsize,
				int *rc, int waitsec)
{
  CSS_QUEUE_ENTRY *data_entry, *buffer_entry;
  CSS_WAIT_QUEUE_ENTRY *data_wait;

  /* enter the critical section of this connection */
  csect_enter_critical_section (NULL, &conn->csect, INF_WAIT);

  *buffer = NULL;
  *bufsize = -1;

  /* if conn is closed or to be closed, return CONECTION_CLOSED */
  if (conn->status == CONN_OPEN)
    {
      /* look up the data queue first to see if the required data is arrived
         and queued already */
      data_entry = css_find_and_remove_queue_entry (&conn->data_queue, rid);

      if (data_entry)
	{
	  /* look up the buffer queue to see if the user provided the receive
	     data buffer */
	  buffer_entry = css_find_and_remove_queue_entry (&conn->buffer_queue,
							  rid);

	  if (buffer_entry)
	    {
	      /* copy the received data to the user provided buffer area */
	      *buffer = buffer_entry->buffer;
	      *bufsize = MIN (data_entry->size, buffer_entry->size);
	      if (*buffer != data_entry->buffer
		  || *bufsize != data_entry->size)
		{
		  memcpy (*buffer, data_entry->buffer, *bufsize);
		}

	      /* destroy the buffer queue entry */
	      buffer_entry->buffer = NULL;
	      css_free_queue_entry (conn, buffer_entry);
	    }
	  else
	    {
	      /* set the buffer to point to the data queue entry */
	      *buffer = data_entry->buffer;
	      *bufsize = data_entry->size;
	      data_entry->buffer = NULL;
	    }

	  /* set return code, transaction id, and error code */
	  *rc = data_entry->rc;
	  conn->transaction_id = data_entry->transaction_id;
	  conn->db_error = data_entry->db_error;

	  css_free_queue_entry (conn, data_entry);
	  csect_exit_critical_section (&conn->csect);

	  return NO_ERRORS;
	}
      else
	{
	  THREAD_ENTRY *thrd;

	  /* no data queue entry means that the data is not arrived yet;
	     wait until the data arrives */
	  *rc = NO_DATA_AVAILABLE;

	  /* lock thread entry before enqueue an entry to data wait queue
	     in order to prevent being woken up by 'css_queue_packet()'
	     before this thread suspends */
	  thrd = thread_get_thread_entry_info ();
	  thread_lock_entry (thrd);
	  /* make a data wait queue entry */
	  data_wait = css_add_wait_queue_entry (conn, &conn->data_wait_queue,
						rid, buffer, bufsize, rc);
	  if (data_wait)
	    {
	      /* exit the critical section before to be suspended */
	      csect_exit_critical_section (&conn->csect);

	      /* fall to the thread sleep until the socket listener
	         'css_server_thread()' receives and enqueues the data */
	      if (waitsec < 0)
		{
		  thread_suspend_wakeup_and_unlock_entry (thrd,
							  THREAD_CSS_QUEUE_SUSPENDED);

		  if (thrd->resume_status != THREAD_CSS_QUEUE_RESUMED)
		    {
		      assert (thrd->resume_status ==
			      THREAD_RESUME_DUE_TO_INTERRUPT);

		      *buffer = NULL;
		      *bufsize = -1;
		      return NO_DATA_AVAILABLE;
		    }
		  else
		    {
		      assert (thrd->resume_status ==
			      THREAD_CSS_QUEUE_RESUMED);
		    }
		}
	      else
		{
		  int r;
#if defined(WINDOWS)
		  int abstime;

		  abstime = waitsec * 1000;
#else /* WINDOWS */
		  struct timespec abstime;

		  abstime.tv_sec = time (NULL) + waitsec;
		  abstime.tv_nsec = 0;
#endif /* WINDOWS */

		  r = thread_suspend_timeout_wakeup_and_unlock_entry (thrd,
								      &abstime,
								      THREAD_CSS_QUEUE_SUSPENDED);

		  if (r == ETIMEDOUT
		      || thrd->resume_status != THREAD_CSS_QUEUE_RESUMED)
		    {
		      assert (r == ETIMEDOUT
			      || (thrd->resume_status ==
				  THREAD_RESUME_DUE_TO_INTERRUPT));

		      *buffer = NULL;
		      *bufsize = -1;
		      return NO_DATA_AVAILABLE;
		    }
		  else
		    {
		      assert (thrd->resume_status ==
			      THREAD_CSS_QUEUE_RESUMED);
		    }
		}

	      if (*buffer == NULL || *bufsize < 0)
		{
		  return CONNECTION_CLOSED;
		}

	      if (*rc == CONNECTION_CLOSED)
		{
		  csect_enter_critical_section (NULL, &conn->csect, INF_WAIT);

		  /* check the deadlock related problem */
		  data_wait = css_find_and_remove_wait_queue_entry
		    (&conn->data_wait_queue, rid);
		  /* data_wait might be always not NULL
		     except the actual connection close */
		  if (data_wait)
		    {
		      data_wait->thrd_entry = NULL;
		      css_free_wait_queue_entry (conn, data_wait);
		    }

		  csect_exit_critical_section (&conn->csect);
		}

	      return NO_ERRORS;
	    }
	  else
	    {
	      /* oops! error! unlock thread entry */
	      thread_unlock_entry (thrd);
	      /* allocation error */
	      *rc = CANT_ALLOC_BUFFER;
	    }
	}
    }
  else
    {
      /* conn->status == CONN_CLOSED || CONN_CLOSING;
         the connection was closed */
      *rc = CONNECTION_CLOSED;
    }

  /* exit the critical section */
  csect_exit_critical_section (&conn->csect);
  return *rc;
}

/*
 * css_return_queued_data() - get data from queue
 *   return: 0 if success, or error code
 *   conn(in): connection entry
 *   rid(out): request id
 *   buffer(out): data buffer
 *   bufsize(out): buffer size
 *   rc(out):
 */
int
css_return_queued_data (CSS_CONN_ENTRY * conn, unsigned short rid,
			char **buffer, int *bufsize, int *rc)
{
  return css_return_queued_data_timeout (conn, rid, buffer, bufsize, rc, -1);
}

/*
 * css_return_queued_error() - get error from queue
 *   return: 0 if success, or error code
 *   conn(in): connection entry
 *   request_id(out): request id
 *   buffer(out): data buffer
 *   buffer_size(out): buffer size
 *   rc(out):
 */
int
css_return_queued_error (CSS_CONN_ENTRY * conn, unsigned short request_id,
			 char **buffer, int *buffer_size, int *rc)
{
  CSS_QUEUE_ENTRY *p;
  int r = 0;

  csect_enter_critical_section (NULL, &conn->csect, INF_WAIT);
  p = css_find_and_remove_queue_entry (&conn->error_queue, request_id);
  if (p != NULL)
    {
      *buffer = p->buffer;
      *buffer_size = p->size;
      *rc = p->db_error;
      p->buffer = NULL;
      css_free_queue_entry (conn, p);
      r = 1;
    }

  csect_exit_critical_section (&conn->csect);
  return r;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * css_return_oob_buffer() - alloc oob buffer
 *   return: allocated buffer
 *   size(in): buffer size
 */
static char *
css_return_oob_buffer (int size)
{
  if (size == 0)
    {
      return NULL;
    }
  else
    {
      return ((char *) malloc (size));
    }
}
#endif

/*
 * css_is_valid_request_id() - check request id id valid
 *   return: true if valid, or false
 *   conn(in): connection entry
 *   request_id(in): request id
 */
static bool
css_is_valid_request_id (CSS_CONN_ENTRY * conn, unsigned short request_id)
{
  if (css_find_queue_entry (&conn->data_queue, request_id) != NULL)
    {
      return false;
    }

  if (css_find_queue_entry (&conn->request_queue, request_id) != NULL)
    {
      return false;
    }

  if (css_find_queue_entry (&conn->abort_queue, request_id) != NULL)
    {
      return false;
    }

  if (css_find_queue_entry (&conn->error_queue, request_id) != NULL)
    {
      return false;
    }

  return true;
}

/*
 * css_remove_unexpected_packets() - remove unexpected packet
 *   return: void
 *   conn(in): connection entry
 *   request_id(in): request id
 */
void
css_remove_unexpected_packets (CSS_CONN_ENTRY * conn,
			       unsigned short request_id)
{
  css_free_queue_entry (conn,
			css_find_and_remove_queue_entry (&conn->request_queue,
							 request_id));
  css_free_queue_entry (conn,
			css_find_and_remove_queue_entry (&conn->data_queue,
							 request_id));
  css_free_queue_entry (conn,
			css_find_and_remove_queue_entry (&conn->error_queue,
							 request_id));
}

/*
 * css_queue_user_data_buffer() - queue user data
 *   return: NO_ERRORS if success, or css error code
 *   conn(in): connection entry
 *   request_id(in): request id
 *   size(in): buffer size
 *   buffer(in): buffer
 */
int
css_queue_user_data_buffer (CSS_CONN_ENTRY * conn, unsigned short request_id,
			    int size, char *buffer)
{
  int rc = NO_ERRORS;

  csect_enter_critical_section (NULL, &conn->csect, INF_WAIT);

  if (buffer && (!css_is_request_aborted (conn, request_id)))
    {
      rc = css_add_queue_entry (conn, &conn->buffer_queue, request_id, buffer,
				size, NO_ERRORS, conn->transaction_id,
				conn->db_error);
    }

  csect_exit_critical_section (&conn->csect);
  return rc;
}

/*
 * css_remove_and_free_queue_entry() - free queue entry
 *   return: status if traverse
 *   data(in): connection entry
 *   arg(in): queue entry
 */
static int
css_remove_and_free_queue_entry (void *data, void *arg)
{
  css_free_queue_entry ((CSS_CONN_ENTRY *) arg, (CSS_QUEUE_ENTRY *) data);
  return TRAV_CONT_DELETE;
}

/*
 * css_remove_and_free_wait_queue_entry() - free wait queue entry
 *   return: status if traverse
 *   data(in): connection entry
 *   arg(in): wait queue entry
 */
static int
css_remove_and_free_wait_queue_entry (void *data, void *arg)
{
  css_free_wait_queue_entry ((CSS_CONN_ENTRY *) arg,
			     (CSS_WAIT_QUEUE_ENTRY *) data);
  return TRAV_CONT_DELETE;
}

/*
 * css_remove_all_unexpected_packets() - remove all unexpected packets
 *   return: void
 *   conn(in): connection entry
 */
void
css_remove_all_unexpected_packets (CSS_CONN_ENTRY * conn)
{
  csect_enter_critical_section (NULL, &conn->csect, INF_WAIT);

  css_traverse_list (&conn->request_queue, css_remove_and_free_queue_entry,
		     conn);

  css_traverse_list (&conn->data_queue, css_remove_and_free_queue_entry,
		     conn);

  css_traverse_list (&conn->data_wait_queue,
		     css_remove_and_free_wait_queue_entry, conn);

  css_traverse_list (&conn->abort_queue, css_remove_and_free_queue_entry,
		     conn);

  css_traverse_list (&conn->error_queue, css_remove_and_free_queue_entry,
		     conn);

  csect_exit_critical_section (&conn->csect);
}

/*
 * css_send_magic () - send magic
 *                    
 *   return: void
 *   conn(in/out):
 */
int
css_send_magic (CSS_CONN_ENTRY * conn)
{
  NET_HEADER header;

  memset ((char *) &header, 0, sizeof (NET_HEADER));
  memcpy ((char *) &header, css_Net_magic, sizeof (css_Net_magic));

  return (css_net_send
	  (conn, (const char *) &header,
	   sizeof (NET_HEADER), PRM_TCP_CONNECTION_TIMEOUT * 1000));
}


/*
 * css_alloc_buffer()  : alloc a CSS_BUFFER object 
 *  return a CSS_BUFFER object
 *  size(in):   size of buffer
 */
CSS_BUFFER *
css_alloc_buffer (size_t size)
{
  unsigned alloc_id;
  size_t alloc_size;
  CSS_BUFFER *buf = NULL, **free_list;

  if (size == 0)
    {
      return NULL;
    }

  /* if size great than the max-size alloc then direct alloc it */
  if ((unsigned) size > CSS_BUFFER_MAX_SIZE)
    {
      alloc_id = 0;
      alloc_size = DB_ALIGN (size, MAX_ALIGNMENT);
      goto direct_alloc;
    };

  /* alloc the alloc id for specified memory size */
  for (alloc_id = CSS_BUFFER_MIN_ALLOC, alloc_size = CSS_BUFFER_MIN_SIZE;
       alloc_id <= CSS_BUFFER_MAX_ALLOC; alloc_id++, (alloc_size <<= 1))
    {
      if (size <= alloc_size)
	{
	  break;
	}
    }

  /* if free_list is empty then direct alloc */
  free_list = css_Buffer_free_list + alloc_id - CSS_BUFFER_MIN_ALLOC;

  csect_enter_critical_section (NULL, &css_Data_csect, INF_WAIT);

  if (*free_list)
    {
      buf = *free_list;
      *free_list = (*free_list)->next;
      buf->data_size = 0;
    }

  csect_exit_critical_section (&css_Data_csect);

direct_alloc:

  if (buf == NULL)
    {
      alloc_size += (sizeof (CSS_BUFFER) + MAX_ALIGNMENT);
      buf = (CSS_BUFFER *) malloc (alloc_size);

      if (buf)
	{
	  buf->buff = PTR_ALIGN ((char *) (buf + 1), MAX_ALIGNMENT);
	  buf->alloc_id = alloc_id;
	  buf->data_size = 0;
	}
    }

  return buf;
};

/*
 *  css_free_buffer()   : free a CSS_BUFFER object
 */
void
css_free_buffer (CSS_BUFFER * buff)
{
  CSS_BUFFER **free_list;

  if (buff == NULL)
    {
      return;
    }

  if (buff->alloc_id < CSS_BUFFER_MIN_ALLOC ||
      buff->alloc_id > CSS_BUFFER_MAX_ALLOC)
    {
      free (buff);
      return;
    }

  free_list = css_Buffer_free_list + buff->alloc_id - CSS_BUFFER_MIN_ALLOC;

  csect_enter_critical_section (NULL, &css_Data_csect, INF_WAIT);

  buff->next = *free_list;
  *free_list = buff;

  csect_exit_critical_section (&css_Data_csect);

};


/*
 * css_clean_free_list()   free all the buffer in free list
 */
static void
css_clean_free_list ()
{
  unsigned alloc_id;
  CSS_BUFFER *list[CSS_BUFFER_ALLOC_COUNT];

  csect_enter_critical_section (NULL, &css_Data_csect, INF_WAIT);

  memcpy (list, css_Buffer_free_list, sizeof (css_Buffer_free_list));

  memset (css_Buffer_free_list, 0, sizeof (css_Buffer_free_list));

  csect_exit_critical_section (&css_Data_csect);

  for (alloc_id = CSS_BUFFER_MIN_ALLOC;
       alloc_id <= CSS_BUFFER_MAX_ALLOC; alloc_id++)
    {
      CSS_BUFFER **free_list, *p;

      free_list = list + alloc_id - CSS_BUFFER_MIN_ALLOC;

      while (*free_list)
	{
	  p = *free_list;
	  *free_list = p->next;
	  free (p);
	}
    }
};


/*
 * css_register_task_data_handler()
 */
void
css_register_task_data_handler (void (*handler)
				(const NET_HEADER *, CSS_BUFFER *,
				 THREAD_ENTRY **))
{
  css_Task_data_handler = handler;
};


 /*
  * css_connect_to_remote_server() - connect to remote server
  *   return: CSS_CONN_ENTRY*
  *   node_id (int): the remote node id
  *   req_id(out): request id
  * Note: it is only uesd in server mode and added for remote server communication
  *
  */
CSS_CONN_ENTRY *
css_connect_to_remote_server (int node_id, unsigned short *req_id,
			      bool create_new)
{
  CSS_CONN_ENTRY *conn = NULL;
  NET_HEADER header = DEFAULT_HEADER_DATA;
  const char *server_name;
  char server_addr[64];
  int rc, port_id = PRM_TCP_PORT_ID;
  int size, reason, server_name_length;
  bool sendmagic = true;

  /* get database name */
  server_name = css_Net_db_server_name;
  server_name_length = strlen (server_name) + 1;
  sprintf (server_addr, "%u.%u.%u.%u",
	   ((unsigned) node_id) >> 24,
	   (((unsigned) node_id) >> 16) & 0xFF,
	   (((unsigned) node_id) >> 8) & 0xFF, ((unsigned) node_id) & 0xFF);

  if (*server_addr == 0)
    {
      goto error_exit;
    };

  if (create_new)
    {
      conn = (CSS_CONN_ENTRY *) malloc (sizeof (CSS_CONN_ENTRY));
      if (NULL == conn)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (CSS_CONN_ENTRY));
	  goto error_exit;
	}

      if (css_initialize_conn (conn, -1) != NO_ERROR)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_CONN_INIT, 0);
	  goto error_exit;
	}
      rc = csect_initialize_critical_section (&conn->csect);
      if (rc != NO_ERROR)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_CONN_INIT, 0);
	  goto error_exit;
	}
    }
  else
    {
      conn = css_make_conn (-1);
      if (NULL == conn)
	{
	  goto error_exit;
	}
    }

conn_again:
  if (css_common_connect
      (conn, req_id, server_addr, DATA_REQUEST, server_name,
       server_name_length, port_id, sendmagic) == NULL)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_CANNOT_CONNECT_TO_MASTER, 0);
      goto error_exit;
    }

  rc = css_read_header (conn, &header);
  if (rc != NO_ERRORS)
    {
      goto error_exit;
    }

  size = ntohl (header.buffer_size);
  if (NO_ERRORS != css_net_recv (conn->fd, (char *) &reason, &size,
				 PRM_TCP_CONNECTION_TIMEOUT * 1000))
    {
      goto error_exit;
    }

  reason = ntohl (reason);

  if (reason == SERVER_CONNECTED_NEW)
    {
      rc = css_read_header (conn, &header);

      if (rc != NO_ERRORS)
	{
	  goto error_exit;
	}

      size = sizeof (int);
      css_net_recv (conn->fd, (char *) &port_id, &size,
		    PRM_TCP_CONNECTION_TIMEOUT * 1000);
      port_id = ntohl (port_id);

      sendmagic = false;
      server_name_length = 0;
      server_name = NULL;

      css_shutdown_conn (conn);
      css_initialize_conn (conn, -1);
      goto conn_again;
    }

  if (reason != SERVER_CONNECTED)
    {
      goto error_exit;
    }

  conn->node_id = node_id;
  conn->next_remote_server = NULL;

  return conn;

error_exit:;

  if (conn)
    {
      css_free_conn (conn);
    }

  return NULL;
}

/* 
 * css_open_connection_to_remote_server()
 */
CSS_CONN_ENTRY *
css_open_connection_to_remote_server (int tran_index, int node_id,
				      unsigned short *rid)
{
  CSS_CONN_ENTRY *p, *conn;
  unsigned short rid_tmp;

  if (rid == NULL)
    {
      rid = &rid_tmp;
    }

  p = redirect_conns;
  conn = NULL;

  /* first find the conn in cache */
  csect_enter_critical_section (NULL, &css_Data_csect, INF_WAIT);

  for (; p; p = p->next_remote_server)
    {
      if (p->node_id == node_id && p->remote_status == 0)
	{
	  conn = p;

	  if (rid)
	    {
	      *rid = css_get_request_id (conn);
	    }

	  conn->remote_status = 1;
	  break;
	}
    };

  if (conn == NULL)
    {
      /* create a new connection */
      conn = css_connect_to_remote_server (node_id, rid, false);
      conn->remote_status = 1;
      conn->next_remote_server = redirect_conns;
      redirect_conns = conn;

      if (css_Connect_handler)
	{
	  (*css_Connect_handler) (conn);
	}
    }

  csect_exit_critical_section (&css_Data_csect);

  if (conn != NULL)
    {
      conn->transaction_id = tran_index;

      if (tran_index != NULL_TRAN_INDEX)
	{
	  LOG_2PC_PARTICIPANT *logt;
	  logt = log_2pc_add_participant (tran_index, node_id);
	  if (logt == NULL)
	    {
	      return NULL;
	    }
	}
    }

  return conn;
};

/*
 * css_close_connection_to_remote_server() : 
 */
void
css_close_connection_to_remote_server (CSS_CONN_ENTRY * conn)
{
  /* at this time, there is no other thread to access conn */
  if (conn)
    {
      conn->remote_status = 0;
    }
};

/*
 * css_net_set_database_name () - set the global database name
 *   return: 
 *   db_name(in): database name
 */
void
css_net_set_database_name (const char *db_name)
{
  strncpy (css_Net_db_server_name, db_name,
	   sizeof (css_Net_db_server_name) - 1);
}

/*
 * css_net_get_database_name () - get the global database name
 *   return: database name
  */
const char *
css_net_get_database_name ()
{
  return css_Net_db_server_name;
}

/*
 * css_net_transmit_package():-
 * return : error code , please refers to css_error_code
 */
static int
css_net_transmit_package (CSS_CONN_ENTRY * rcv_conn,
			  const NET_HEADER * header)
{
  int rc;
  int size;
  int type;

  size = ntohl (header->buffer_size);
  type = ntohl (header->type);

  /*send header to remote server */
  rc =
    css_net_send (rcv_conn->trans_conn, (char *) header, sizeof (NET_HEADER),
		  PRM_TCP_CONNECTION_TIMEOUT * 1000);

  if (rc == NO_ERRORS && size > 0
      && (DATA_TYPE == type || ERROR_TYPE == type))
    {
      char buffer_tmp[8192];
      char *buffer = buffer_tmp;

      if (size > sizeof (buffer_tmp))
	{
	  buffer = malloc (size);
	}

      if (NULL == buffer)
	{
	  rc = CANT_ALLOC_BUFFER;
	  return rc;
	}
      do
	{
	  /* timeout in mili-second in css_net_recv() */
	  rc = css_net_recv (rcv_conn->fd, buffer, &size,
			     PRM_TCP_CONNECTION_TIMEOUT * 1000);
	}
      while (rc == INTERRUPTED_READ);

      rc = css_net_send (rcv_conn->trans_conn, buffer, size,
			 PRM_TCP_CONNECTION_TIMEOUT * 1000);

      if (buffer != buffer_tmp)
	{
	  free (buffer);
	}
    }

  return rc;
}

/*for server to server communication*/
/*
 *   s2s_begin_request() - begin request in s2s
 *   return: 
 *   node_id(in): the remote node id
 *   tran_index(in): transaction id
 *   rid(out): the request id
 *   timeout(in): the waiting time to create new connection
 *
 */
CSS_CONN_ENTRY *
s2s_begin_request (int node_id, int tran_index, unsigned short *rid,
		   int wait_millisec)
{
  CSS_CONN_TABLE *conn_table;
  int rc;
  CSS_CONN_ENTRY *conn = NULL;

#if defined(WINDOWS)
  int to = 0;
#else /* WINDOWS */
  struct timespec to = {
    0, 0
  };
#endif /* WINDOWS */

  conn_table = get_or_create (node_id);
  if (conn_table == NULL)
    {
      return NULL;
    }

  MUTEX_LOCK (rc, conn_table->mutex);
  if (rc != NO_ERROR)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_MUTEX_LOCK, 0);
      return NULL;
    }

#if defined(WINDOWS)
  to = wait_millisec * 1000;
#else /* WINDOWS */
  to.tv_sec = time (NULL) + wait_millisec;
  to.tv_nsec = 0;
#endif /* WINDOWS */

  rc = NO_ERROR;
  /*if is the max num of s2s connection */
  while (conn_table->num_using == PRM_MAX_CONN_POOL_SIZE && rc == NO_ERROR)
    {
      rc = COND_TIMEDWAIT (conn_table->cond, conn_table->mutex, to);
      if (rc != NO_ERROR && rc != ETIMEDOUT)
	{
#if !defined(WINDOWS)
	  MUTEX_UNLOCK (conn_table->mutex);
#endif
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_COND_WAIT, 0);
	  return NULL;
	}

#if defined(WINDOWS)
      MUTEX_LOCK (rc, conn_table->mutex);
      if (rc != NO_ERROR)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_MUTEX_LOCK, 0);
	  return NULL;
	}
#endif /* WINDOWS */

    }

  if (rc == ETIMEDOUT)
    {
      MUTEX_UNLOCK (conn_table->mutex);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_SERVER_DATA_RECEIVE,
	      0);
      return NULL;
    }

  if (conn_table->free_anchor != NULL)
    {
      conn = conn_table->free_anchor;
      conn_table->free_anchor = conn->next;
      conn->next = conn_table->using_anchor;
      conn_table->using_anchor = conn;

      conn_table->num_using++;
      conn->transaction_id = tran_index;

      MUTEX_UNLOCK (conn_table->mutex);

      *rid = css_get_request_id (conn);
      goto conn_available;
    }

  conn = css_connect_to_remote_server (node_id, rid, true);

  if (conn == NULL)
    {
      MUTEX_UNLOCK (conn_table->mutex);
      er_log_debug (ARG_FILE_LINE,
		    "cannot open connection to remote server. remote node_id : %d. \n",
		    node_id);
      return NULL;
    }

  conn->conn_table = conn_table;
  conn->next = conn_table->using_anchor;
  conn_table->using_anchor = conn;
  conn_table->num_using++;

  MUTEX_UNLOCK (conn_table->mutex);

conn_available:
  if (tran_index != NULL_TRAN_INDEX)
    {
      LOG_2PC_PARTICIPANT *logt;
      logt = log_2pc_add_participant (tran_index, node_id);
      if (logt == NULL)
	{
	  return NULL;
	}
    }
  conn->transaction_id = tran_index;

  return conn;
}

/*
 *   s2s_end_request - end the request in s2s
 *   conn(CSS_CONN_ENTRY *): the conn entry to close
 *
 */
int
s2s_end_request (CSS_CONN_ENTRY * conn)
{
  CSS_CONN_TABLE *conn_table;
  CSS_CONN_ENTRY *iterator, *preconn;
  int rc = NO_ERROR;

  if (conn == NULL)
    {
      return NO_ERROR;
    }

  conn_table = conn->conn_table;

  MUTEX_LOCK (rc, conn_table->mutex);
  if (rc != NO_ERROR)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_MUTEX_LOCK, 0);
      return rc;
    }

  preconn = NULL;

  for (iterator = conn_table->using_anchor; iterator != NULL;
       preconn = iterator, iterator = iterator->next)
    {
      if (iterator == conn)
	{
	  if (preconn == NULL)
	    {
	      conn_table->using_anchor = iterator->next;
	    }
	  else
	    {
	      preconn->next = iterator->next;
	    }

	  iterator->next = conn_table->free_anchor;
	  conn_table->free_anchor = iterator;
	  conn_table->num_using--;
	  COND_SIGNAL (conn_table->cond);
	  break;
	}
    }

  MUTEX_UNLOCK (conn_table->mutex);
  return rc;
}

/*
 *   get_conn_or_create - get a conn_table according to node_id
 *   node_id(int): the node_id of remote server
 *
 */
static CSS_CONN_TABLE *
get_or_create (int node_id)
{
  int i, rc = NO_ERROR;
  CSS_CONN_TABLE *conn_table = NULL;

  MUTEX_LOCK (rc, h_table.mutex);
  if (rc != NO_ERROR)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_MUTEX_LOCK, 0);
      return NULL;
    }

  for (i = 0; i < h_table.conn_host_num; i++)
    {
      if (h_table.host_entry[i].node_id == node_id)
	{
	  conn_table = h_table.host_entry[i].conn_table;
	  break;
	}
    }

  if (conn_table == NULL)
    {
      /*the h_table is the max number of the connection entry, so we can't create new conn_table */
      if (h_table.conn_host_num >= DB_MAX_NODE_IN_CLUSTER - 1)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_MAX_CLUSTER_NODE_NUM_EXCEEDED, 1,
		  DB_MAX_NODE_IN_CLUSTER);
	  goto exit;
	}

      /*create a new conn_table */
      conn_table = (CSS_CONN_TABLE *) malloc (sizeof (CSS_CONN_TABLE));
      if (conn_table == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (CSS_CONN_TABLE));
	  goto exit;
	}

      rc = MUTEX_INIT (conn_table->mutex);
      if (rc != NO_ERROR)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_MUTEX_INIT, 0);
	  goto exit;
	}

      rc = COND_INIT (conn_table->cond);
      if (rc != NO_ERROR)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_COND_INIT, 0);
	  goto exit;
	}

      conn_table->num_using = 0;
      conn_table->free_anchor = NULL;
      conn_table->using_anchor = NULL;

      /*append it to the host_table */
      h_table.host_entry[h_table.conn_host_num].node_id = node_id;
      h_table.host_entry[h_table.conn_host_num].conn_table = conn_table;
      h_table.conn_host_num++;
    }

exit:
  MUTEX_UNLOCK (h_table.mutex);
  return conn_table;
}

/*
 * css_receive_response () - return a data buffer for an associated request
 *   return:
 *   conn(in): the conn entry to remote server
 *   req_id(in): the request id
 *   buffer(out): the buffer geting result from remote server
 *   buffer_size(out): size of buffer
 *
 * Note: this is a blocking read.
 */
int
css_receive_response (CSS_CONN_ENTRY * conn, unsigned short req_id,
		      char **buffer, int *buffer_size)
{
  NET_HEADER header = DEFAULT_HEADER_DATA;
  int header_size;
  int rc;
  int rid;
  int type;
  CSS_QUEUE_ENTRY *buffer_entry;

  if (conn == NULL)
    {
      return CONNECTION_CLOSED;
    }

begin:
  header_size = sizeof (NET_HEADER);
  do
    {
      rc = css_net_recv (conn->fd, (char *) &header, &header_size,
			 PRM_TCP_CONNECTION_TIMEOUT * 1000);
    }
  while (rc == INTERRUPTED_READ);

  if (rc == NO_ERRORS)
    {
      rid = ntohl (header.request_id);
      conn->transaction_id = ntohl (header.transaction_id);	/*if it necessary */
      conn->db_error = (int) ntohl (header.db_error);
      type = ntohl (header.type);
      *buffer_size = ntohl (header.buffer_size);

      if (*buffer_size != 0)
	{
	  if (DATA_TYPE == type)
	    {
	      buffer_entry =
		css_find_and_remove_queue_entry (&conn->buffer_queue, req_id);
	      if (buffer_entry == NULL)
		{
		  rc = CANT_ALLOC_BUFFER;
		  goto error;
		}

	      *buffer = buffer_entry->buffer;
	      buffer_entry->buffer = NULL;
	      free_and_init (buffer_entry);
	      if (*buffer == NULL)
		{
		  rc = CANT_ALLOC_BUFFER;
		  goto error;
		}

	      do
		{
		  rc =
		    css_net_recv (conn->fd, *buffer, buffer_size,
				  PRM_TCP_CONNECTION_TIMEOUT * 1000);
		}
	      while (rc == INTERRUPTED_READ);
	    }


	  else if (type == ERROR_TYPE)
	    {
	      *buffer = (char *) malloc (*buffer_size);


	      if (*buffer != NULL)

		{

		  do

		    {

		      /* timeout in mili-second in css_net_recv() */
		      rc =
			css_net_recv (conn->fd, *buffer, buffer_size,
				      PRM_TCP_CONNECTION_TIMEOUT * 1000);

		    }

		  while (rc == INTERRUPTED_READ);


		  if (rc == NO_ERRORS || rc == RECORD_TRUNCATED)

		    {
		      er_set_area_error ((void *) *buffer);
		      free_and_init (*buffer);

		      goto begin;
		    }

		  else

		    {

		      free_and_init (*buffer);

		    }

		}

	      else

		{

		  rc = CANT_ALLOC_BUFFER;

		  css_read_remaining_bytes (conn->fd,
					    sizeof (int) + *buffer_size);


		  css_add_queue_entry (conn, &conn->error_queue, req_id,
				       NULL, 0,
				       rc, conn->transaction_id,
				       conn->db_error);
		  css_find_and_remove_queue_entry (&conn->buffer_queue,
						   req_id);

		}
	    }
	  else
	    {
	      if (type == ABORT_TYPE)
		{
		  return SERVER_ABORTED;
		}

	      TRACE
		("unexpected data packet in css_css_receive_response. TYPE = %d\n",
		 TYPE);
	      goto error;
	    }
	}
      else
	{
	  TRACE
	    ("getting data buffer of length 0 in css_css_receive_response\n",
	     0);
	  buffer_entry =
	    css_find_and_remove_queue_entry (&conn->buffer_queue, req_id);
	  if (buffer_entry != NULL)
	    {
	      *buffer = buffer_entry->buffer;
	    }
	  else
	    {
	      rc = CANT_ALLOC_BUFFER;
	      goto error;
	    }
	}

    }
error:
  if (rc == NO_ERRORS)
    {
      return rc;
    }
  css_read_remaining_bytes (conn->fd, sizeof (int) + *buffer_size);
  TRACE ("error in css_receive_response. error = %d\n", rc);
  return rc;
}

/*
 *   s2s_free_conn_pooling - free the conn pooling
 *
 */
void
s2s_free_conn_pooling (void)
{
  HOST_TABLE_ENTRY host_entry;
  int i;
  CSS_CONN_TABLE *conn_table;
  CSS_CONN_ENTRY *conn, *iterator;

  for (i = 0; i < h_table.conn_host_num; i++)
    {
      host_entry = h_table.host_entry[i];
      conn_table = host_entry.conn_table;

      iterator = conn_table->free_anchor;
      while (iterator)
	{
	  conn = iterator;
	  iterator = iterator->next;
	  free_and_init (conn);
	}

      iterator = conn_table->using_anchor;
      while (iterator)
	{
	  conn = iterator;
	  iterator = iterator->next;
	  free_and_init (conn);
	}

      free_and_init (host_entry.conn_table);
    }
}
