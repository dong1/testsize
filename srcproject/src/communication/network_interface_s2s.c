/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
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
 * network_interface_s2s.c - server to server communication
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include "cluster_config.h"
#include "connection_sr.h"
#include "query_list.h"
#include "list_file.h"
#include "network.h"
#include "query_executor.h"
#include "connection_support.h"
#include "network_s2s.h"
#include "network_interface_s2s.h"

/* remote_end_query() : -
 * thread_p(in):
 * remote_id(in):
 * query_id(in):
 * status(out):
 */
int
remote_end_query (THREAD_ENTRY * thread_p, int remote_id,
		  QUERY_ID query_id, int *status)
{
  unsigned short rid;
  OR_ALIGNED_BUF (OR_PTR_SIZE) a_request_buffer;
  char *request_buffer;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply_buffer;
  char *reply_buffer;
  CSS_CONN_ENTRY *conn = NULL;
  int error;

  request_buffer = OR_ALIGNED_BUF_START (a_request_buffer);
  reply_buffer = OR_ALIGNED_BUF_START (a_reply_buffer);

  or_pack_ptr (request_buffer, query_id);

  conn =
    s2s_begin_request (remote_id, LOG_FIND_THREAD_TRAN_INDEX (thread_p), &rid,
		       PRM_TCP_CONNECTION_TIMEOUT);

  if (conn == NULL)
    {
      error = ER_NET_CANT_CONNECT_SERVER;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, remote_id);
      return error;
    };

  error = network_s2s_request_1_data (conn,
				      NET_SERVER_QM_QUERY_END,
				      &rid,
				      request_buffer,
				      OR_ALIGNED_BUF_SIZE (a_request_buffer),
				      reply_buffer,
				      OR_ALIGNED_BUF_SIZE (a_reply_buffer));

  if (error == NO_ERROR)
    {
      or_unpack_int (reply_buffer, status);
    }

  s2s_end_request (conn);
  return error;
}

/* remote_prepare_and_execute_query () - 
 *
 *
 *   return: Query result file identifier, or NULL
 * 
 *   thread_p (in): thread entry
 *   remote_node_id (in): remote server you want to connect
 *   query_p (in): include xasl_data, xasl_size, qstmt, creator_oid
 *   dbvals_p (in): list of positional values
 *   dbvals_count (in): number of positional values
 *   flag_p (in):
 *   client_cache_time_p (in): 
 *...list_id_p (out): the list id of execution
 *
 * Note: This is added for sending NET_SERVER_QM_QUERY_PREPARE_AND_EXECUTE request to 
 *       remote server
 *
 */
int
remote_prepare_and_execute_query (THREAD_ENTRY * thread_p,
				  int remote_node_id,
				  char *xasl_buffer,
				  int xasl_size,
				  const DB_VALUE * dbvals_p,
				  int dbval_count,
				  QUERY_FLAG flag, QFILE_LIST_ID ** list_id)
{
  int senddata_size, request_type, size = 0;
  char *ptr, *senddata, *reply, *request;
  DB_VALUE *dbval;
  OR_ALIGNED_BUF (OR_INT_SIZE * 3) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE * 3
		  + OR_PTR_ALIGNED_SIZE + OR_CACHE_TIME_SIZE) a_reply;
  CSS_CONN_ENTRY *conn = NULL;
  unsigned short rid;
  int i, error = ER_FAILED;
  char *replydata1, *replydata2;
  int replydata_size1, replydata_size2;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  senddata = NULL;
  senddata_size = 0;

  for (i = 0, dbval = (DB_VALUE *) dbvals_p; i < dbval_count; i++, dbval++)
    {
      senddata_size += OR_VALUE_ALIGNED_SIZE (dbval);
    }

  if (senddata_size)
    {
      senddata = (char *) db_private_alloc (thread_p, senddata_size);
      if (senddata == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, senddata_size);
	  return error;
	}
    }

  ptr = or_pack_int (request, dbval_count);
  ptr = or_pack_int (ptr, senddata_size);
  ptr = or_pack_int (ptr, flag);

  ptr = senddata;

  for (i = 0, dbval = (DB_VALUE *) dbvals_p; i < dbval_count; i++, dbval++)
    {
      ptr = or_pack_db_value (ptr, dbval);
    }

  if (IS_SYNC_EXEC_MODE (flag))
    {
      request_type = NET_SERVER_QM_QUERY_PREPARE_AND_EXECUTE;
    }
  else
    {
      request_type = NET_SERVER_QM_QUERY_PREPARE_AND_EXECUTE_ASYNC;
    }

  conn =
    s2s_begin_request (remote_node_id, LOG_FIND_THREAD_TRAN_INDEX (thread_p),
		       &rid, PRM_TCP_CONNECTION_TIMEOUT);

  if (conn == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_CANT_CONNECT_SERVER, 1,
	      remote_node_id);
      goto end;
    };

  /* request command will invoke remote sqmgr_prepare_and_execution function */
  /* send request data and command to remote server */

  error = network_s2s_request_with_callback (conn,
					     request_type,
					     &rid,
					     request,
					     OR_ALIGNED_BUF_SIZE (a_request),
					     reply,
					     OR_ALIGNED_BUF_SIZE (a_reply),
					     xasl_buffer, xasl_size, senddata,
					     senddata_size, &replydata1,
					     &replydata_size1, &replydata2,
					     &replydata_size2);

  if (error == NO_ERROR)
    {
      if (replydata1 && replydata_size1)
	{
	  /* unpack list file id of query result from the reply data */
	  ptr = or_unpack_unbound_listid (replydata1, (void **) list_id);
	  /* QFILE_LIST_ID shipped with last page */

	  if (ptr == NULL)
	    {
	      goto end;
	    }
	  if (replydata_size2)
	    {
	      (*list_id)->last_pgptr = replydata2;
	      (*list_id)->remote_total_size = replydata_size2;
	      (*list_id)->remote_offset = 0;
	    }
	  else
	    {
	      (*list_id)->last_pgptr = NULL;
	      (*list_id)->remote_total_size = 0;
	      (*list_id)->remote_offset = 0;
	    }
	  free_and_init (replydata1);
	}
    }
end:

  s2s_end_request (conn);

  if (senddata)
    {
      db_private_free (thread_p, senddata);
    }

  return error;
}

/* remote_get_list_file_page() : - get list file page from remote server.
 * return : error code.
 * thread_p(in): current thread entry ptr;
 * reote_id(in): remote node id;
 * volid(in):
 * pageid(in):
 * buffer(out): the page content from remote server.
 * buffer_size(out):
 */
int
remote_get_list_file_page (THREAD_ENTRY * thread_p,
			   int remote_id,
			   QUERY_ID query_id,
			   VOLID volid,
			   PAGEID pageid, char **buffer, int *buffer_size)
{
  CSS_CONN_ENTRY *conn = NULL;
  unsigned short rid;
  int error;
  char *ptr;
  OR_ALIGNED_BUF (OR_PTR_SIZE + OR_INT_SIZE + OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_ptr (request, query_id);
  ptr = or_pack_int (ptr, (int) volid);
  ptr = or_pack_int (ptr, (int) pageid);

  conn =
    s2s_begin_request (remote_id, LOG_FIND_THREAD_TRAN_INDEX (thread_p), &rid,
		       PRM_TCP_CONNECTION_TIMEOUT);
  if (conn == NULL)
    {
      error = ER_NET_CANT_CONNECT_SERVER;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, remote_id);
      goto end;
    }

  error = network_s2s_request_2_data (conn,
				      NET_SERVER_LS_GET_LIST_FILE_PAGE,
				      &rid,
				      request,
				      OR_ALIGNED_BUF_SIZE (a_request), reply,
				      OR_ALIGNED_BUF_SIZE (a_reply), NULL, 0,
				      buffer, buffer_size);

  if (error == NO_ERROR)
    {
      ptr = or_unpack_int (&reply[OR_INT_SIZE], &error);
    }

end:
  s2s_end_request (conn);
  return error;
}

/*
 * remote_locator_force() - send copy area to specified host\
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 * copy_area(in): copy area where objects are placed
 * node_id(in): to be sent host id
 */
int
remote_locator_force (THREAD_ENTRY * thread_p, LC_COPYAREA * copy_area,
		      int node_id)
{
  int success = ER_FAILED;
  OR_ALIGNED_BUF (OR_INT_SIZE * 5 + OR_OID_SIZE) a_request;
  char *request;
  char *request_ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE * 3) a_reply;
  char *reply;
  char *desc_ptr = NULL;
  int desc_size;
  char *content_ptr;
  int content_size;
  int num_objs = 0;
  int req_error = ER_FAILED;
  LC_COPYAREA_MANYOBJS *mobjs;
  CSS_CONN_ENTRY *conn = NULL;
  unsigned short rid;

  mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (copy_area);

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  num_objs = locator_send_copy_area (copy_area, &content_ptr, &content_size,
				     &desc_ptr, &desc_size);

  request_ptr = or_pack_int (request, num_objs);
  request_ptr = or_pack_int (request_ptr, mobjs->start_multi_update);
  request_ptr = or_pack_int (request_ptr, mobjs->end_multi_update);
  request_ptr = or_pack_oid (request_ptr, &mobjs->class_oid);
  request_ptr = or_pack_int (request_ptr, desc_size);
  request_ptr = or_pack_int (request_ptr, content_size);

  conn =
    s2s_begin_request (node_id, LOG_FIND_THREAD_TRAN_INDEX (thread_p), &rid,
		       PRM_TCP_CONNECTION_TIMEOUT);

  req_error = network_s2s_request_3_data (conn, NET_SERVER_LC_FORCE, &rid,
					  request,
					  OR_ALIGNED_BUF_SIZE (a_request),
					  desc_ptr, desc_size, content_ptr,
					  content_size, reply,
					  OR_ALIGNED_BUF_SIZE (a_reply),
					  desc_ptr, desc_size, NULL, 0);


  if (!req_error)
    {
      (void) or_unpack_int (reply, &success);
      if (success == NO_ERROR)
	{
	  locator_unpack_copy_area_descriptor (num_objs, copy_area, desc_ptr);
	}
    }
  if (desc_ptr)
    {
      free_and_init (desc_ptr);
    }

  s2s_end_request (conn);
  return success;
}

/*
 * remote_get_num_objects - get remote objects number
 *
 * return: status
 *
 *   thread_p(in):
 *   node_id(in): remote host id
 *   hfid(in): object hfid
 *   approximation(in): estimated or exact statistics info
 *   nobjs(in/out): object count
 *   npages(in/out): page count
 *
 */
int
remote_get_num_objects (THREAD_ENTRY * thread_p, int remote_id,
			const HFID * hfid, int *npages, int *nobjs,
			int *avg_length)
{
  int error, status, num_objs, num_pages;
  OR_ALIGNED_BUF (OR_HFID_SIZE + OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE * 3) a_reply;
  char *reply;
  char *ptr;
  int approximation = 0;
  CSS_CONN_ENTRY *conn = NULL;
  unsigned short rid;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_hfid (request, hfid);
  ptr = or_pack_int (ptr, approximation);

  conn =
    s2s_begin_request (remote_id, LOG_FIND_THREAD_TRAN_INDEX (thread_p), &rid,
		       PRM_TCP_CONNECTION_TIMEOUT);

  if (conn == NULL)
    {
      error = ER_NET_CANT_CONNECT_SERVER;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, remote_id);
      return error;
    };

  error = network_s2s_request_1_data (conn,
				      NET_SERVER_HEAP_GET_CLASS_NOBJS_AND_NPAGES,
				      &rid,
				      request,
				      OR_ALIGNED_BUF_SIZE (a_request),
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));

  if (error)
    {
      status = ER_FAILED;
    }
  else
    {
      ptr = or_unpack_int (reply, &status);
      ptr = or_unpack_int (ptr, &num_objs);
      ptr = or_unpack_int (ptr, &num_pages);
      *nobjs = (int) num_objs;
      *npages = (int) num_pages;
    }

  s2s_end_request (conn);
  return status;
}

/*
 * serial_get_real_oid () - get real serial OID by serial name
 *   return: error code
 *   thread_p(in)    : current thread entry
 *   name_val(in)    : name of global serial
 *   real_oid(out)   : real serial object OID. remote OID
 *   node_id(in)     : node id of the real serial
 *
 *   Note:
 *    it's used for the proxy serial, get the real serial OID
 *    by S2S communication
 */
int
serial_get_real_oid (THREAD_ENTRY * thread_p, DB_VALUE * name_val,
		     OID * real_oid, int node_id)
{
  unsigned short rid;
  int rc = ER_FAILED, req_error;
  int request_size, reply_size;
  CSS_CONN_ENTRY *conn;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_OID_SIZE) a_reply;
  char *request_buffer, *reply_buffer, *ptr;

  reply_size = OR_ALIGNED_BUF_SIZE (a_reply);
  reply_buffer = OR_ALIGNED_BUF_START (a_reply);
  request_size = OR_VALUE_ALIGNED_SIZE (name_val);
  request_buffer = (char *) malloc (request_size);

  if (request_buffer == NULL)
    {
      goto end;
    }

  (void) or_pack_value (request_buffer, name_val);

  conn = s2s_begin_request (node_id, LOG_FIND_THREAD_TRAN_INDEX
			    (thread_p), &rid, -1);
  if (conn == NULL)
    {
      goto end;
    }

  req_error = network_s2s_request_1_data (conn,
					  NET_SERVER_SERIAL_GET_REAL_OID,
					  &rid,
					  request_buffer, request_size,
					  reply_buffer, reply_size);
  if (req_error != NO_ERROR)
    {
      goto end;
    }

  ptr = or_unpack_int (reply_buffer, &rc);
  if (rc == NO_ERROR)
    {
      (void) or_unpack_oid (ptr, real_oid);
    }
  else
    {
      OID_SET_NULL (real_oid);
    }

end:
  free_and_init (request_buffer);
  s2s_end_request (conn);
  return rc;
}

/*
 * serial_get_cache_range () - get the proxy serial cache range from the real
 *                             serial
 *   return: error code
 *   thread_p(in)   : current thread entry
 *   real_oid(in)   : OID of real serial. remote OID
 *   start_val(out) : the start value of cache
 *   end_val(out)   : the last value of cache
 *   node_id(in)    : the node id of real serial
 *
 *   Note:
 *    it's used for the proxy serial, get the cache range by S2S communication
 */
int
serial_get_cache_range (THREAD_ENTRY * thread_p, OID * real_oid,
			DB_VALUE * start_val, DB_VALUE * end_val, int node_id)
{
  int req_error;
  OR_ALIGNED_BUF (OR_OID_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply;
  char *area = NULL, *ptr;
  int val_size;
  int rc;

  unsigned short rid;
  CSS_CONN_ENTRY *conn;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_pack_oid (request, real_oid);

  conn = s2s_begin_request (node_id, LOG_FIND_THREAD_TRAN_INDEX
			    (thread_p), &rid, -1);
  if (conn == NULL)
    {
      goto end;
    }

  req_error =
    network_s2s_request_2_data (conn, NET_SERVER_SERIAL_GET_CACHE_RANGE, &rid,
				request, OR_ALIGNED_BUF_SIZE (a_request),
				reply, OR_ALIGNED_BUF_SIZE (a_reply), NULL, 0,
				&area, &val_size);
  if (!req_error)
    {
      reply = or_unpack_int (reply, &val_size);
      (void) or_unpack_int (reply, &req_error);
    }

  if (req_error == NO_ERROR)
    {
      ptr = or_unpack_value (area, start_val);
      (void) or_unpack_value (ptr, end_val);
      rc = NO_ERROR;
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, req_error, 0);
      rc = ER_FAILED;
    }

  if (area != NULL)
    {
      free_and_init (area);
    }

end:
  s2s_end_request (conn);
  return rc;
}

/*
 * log_2pc_send_prepare_to_particp () - Send prepare command to specified participant.
 * return : state of participant.
 *
 *  particp (in) : 
 */
TRAN_STATE
log_2pc_send_prepare_to_particp (LOG_2PC_PARTICIPANT * particp)
{
  TRAN_STATE state = TRAN_UNACTIVE_UNKNOWN;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  int reply_size;
  CSS_CONN_ENTRY *conn = NULL;
  unsigned short rid;
  int rc = -1;

  assert (particp != NULL);

  reply = OR_ALIGNED_BUF_START (a_reply);
  reply_size = OR_ALIGNED_BUF_SIZE (a_reply);

  conn =
    s2s_begin_request (particp->host_id, thread_get_current_tran_index (),
		       &rid, 0);
  if (conn == NULL)
    {
      goto end;
    };

  rc = network_s2s_request_1_data (conn, NET_SERVER_TM_SERVER_2PC_PREPARE,
				   &rid, NULL, 0, reply, reply_size);
  if (rc != NO_ERROR)
    {
      goto end;
    }

  /* TODO: maybe we need check if the reply is correct. */

  or_unpack_int (reply, (int *) &state);

end:
  s2s_end_request (conn);
  return (TRAN_STATE) state;
}

/*
 * log_2pc_send_commit_to_particp () - Send commit to specified participant.
 * return : state of participant.
 *
 *  particp (in) : 
 */
TRAN_STATE
log_2pc_send_commit_to_particp (LOG_2PC_PARTICIPANT * particp)
{
  TRAN_STATE state = TRAN_UNACTIVE_UNKNOWN;
  int tran_state_int, reset_on_commit;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply, *ptr;
  int reply_size;
  CSS_CONN_ENTRY *conn = NULL;
  unsigned short rid;
  int rc = -1;

  assert (particp != NULL);

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);
  reply_size = OR_ALIGNED_BUF_SIZE (a_reply);

  (void) or_pack_int (request, (int) false);	/* global transaction does support remain_lock */

  conn =
    s2s_begin_request (particp->host_id, thread_get_current_tran_index (),
		       &rid, 0);
  if (conn == NULL)
    {
      goto end;
    };

  rc = network_s2s_request_1_data (conn, NET_SERVER_TM_SERVER_COMMIT,
				   &rid, request,
				   OR_ALIGNED_BUF_SIZE (a_request),
				   reply, reply_size);
  if (rc != NO_ERROR)
    {
      goto end;
    }

  /* TODO: maybe we need check if the reply is correct. */

  ptr = or_unpack_int (reply, &tran_state_int);
  state = (TRAN_STATE) tran_state_int;
  ptr = or_unpack_int (ptr, &reset_on_commit);

end:
  s2s_end_request (conn);
  return (TRAN_STATE) state;
}

/*
 * log_2pc_send_abort_to_particp () - Send abort to specified participant.
 * return : state of participant.
 *
 *  particp (in) : 
 */
TRAN_STATE
log_2pc_send_abort_to_particp (LOG_2PC_PARTICIPANT * particp)
{
  TRAN_STATE state = TRAN_UNACTIVE_UNKNOWN;
  int tran_state_int, reset_on_commit;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply, *ptr;
  int reply_size;
  CSS_CONN_ENTRY *conn = NULL;
  unsigned short rid;
  int rc = -1;

  assert (particp != NULL);

  reply = OR_ALIGNED_BUF_START (a_reply);
  reply_size = OR_ALIGNED_BUF_SIZE (a_reply);

  conn =
    s2s_begin_request (particp->host_id, thread_get_current_tran_index (),
		       &rid, 0);
  if (conn == NULL)
    {
      goto end;
    };

  rc = network_s2s_request_1_data (conn, NET_SERVER_TM_SERVER_ABORT,
				   &rid, NULL, 0, reply, reply_size);
  if (rc != NO_ERROR)
    {
      goto end;
    }

  /* TODO: maybe we need check if the reply is correct. */

  ptr = or_unpack_int (reply, &tran_state_int);
  state = (TRAN_STATE) tran_state_int;
  ptr = or_unpack_int (ptr, &reset_on_commit);

end:
  s2s_end_request (conn);
  return state;
}
