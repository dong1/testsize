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
 * network_s2s.c - server to server communication
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

#define COMPARE_AND_FREE_BUFFER(queued, reply) \
  do { \
  if (((reply) != NULL) && ((reply) != (queued))) { \
  free_and_init ((reply)); \
    } \
    (reply) = NULL; \
    } while (0)

#define COMPARE_SIZE_AND_BUFFER(replysize, size, replybuf, buf)       \
  compare_size_and_buffer((replysize), (size), (replybuf), (buf),     \
  __FILE__, __LINE__)

static int
compare_size_and_buffer (int *replysize, int size, char **replybuf, char *buf,
			 const char *file, const int line);

static int set_server_error (int error);
/*
 * network_s2s_request_with_callback () - 
 *
 * return: error status
 *
 *   conn(in): the conn of server side
 *   request(in): server request id
 *   request_id(in): server request id
 *   requestbuf(in): argument buffer (small)
 *   requestsize(in): byte size of requestbuf
 *   replybuf(in): reply argument buffer (small)
 *   replysize(in): size of reply argument buffer
 *   databuf1(in): first data buffer to send (large)
 *   datasize1(in): size of first data buffer
 *   databuf2(in): second data buffer to send (large)
 *   datasize2(in): size of second data buffer
 *   replydata_ptr1(in): first receive data buffer (large)
 *   replydatasize_ptr1(in): size of first expected reply data
 *   replydata_ptr2(in): second receive data buffer (large)
 *   replydatasize_ptr2(in): size of second expected reply data
 *
 * Note: This is one of the functions that is called to perform a server
 *    request. It send execution request to remote server.
 */
int
network_s2s_request_with_callback (CSS_CONN_ENTRY * conn,
				   int request,
				   unsigned short *request_id,
				   char *requestbuf, int requestsize,
				   char *replybuf, int replysize,
				   char *databuf1, int datasize1,
				   char *databuf2, int datasize2,
				   char **replydata_ptr1,
				   int *replydatasize_ptr1,
				   char **replydata_ptr2,
				   int *replydatasize_ptr2)
{
  unsigned int rc;
  QUERY_SERVER_REQUEST server_request;
  int server_request_num;
  int reply_datasize1, reply_datasize2;
  char *reply = NULL, *replydata, *ptr;
  int size;

  (*replydata_ptr1) = (*replydata_ptr2) = NULL;
  (*replydatasize_ptr1) = (*replydatasize_ptr2) = 0;


  rc = css_send_req_with_3_buffers (conn,
				    request,
				    request_id,
				    requestbuf, requestsize,
				    databuf1, datasize1,
				    databuf2, datasize2, replybuf, replysize);
  if (rc != NO_ERRORS)
    {
      goto Err;
    }

  rc = css_receive_response (conn, *request_id, &reply, &size);
  if (rc != NO_ERRORS || reply == NULL)
    {
      COMPARE_AND_FREE_BUFFER (replybuf, reply);
      goto Err;
    }

  ptr = or_unpack_int (reply, &server_request_num);
  server_request = (QUERY_SERVER_REQUEST) server_request_num;

  switch (server_request)
    {
    case QUERY_END:
      /* here we assume that the first integer in the reply is the length
         of the following data block */
      ptr = or_unpack_int (ptr, &reply_datasize1);
      ptr = or_unpack_int (ptr, &reply_datasize2);
      COMPARE_AND_FREE_BUFFER (replybuf, reply);

      if (reply_datasize1)
	{
	  replydata = (char *) malloc (reply_datasize1);
	  if (replydata == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, reply_datasize1);
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }

	  css_queue_user_data_buffer (conn, *request_id, reply_datasize1,
				      replydata);

	  rc = css_receive_response (conn, *request_id, &reply, &size);
	  if (rc != NO_ERRORS)
	    {
	      COMPARE_AND_FREE_BUFFER (replydata, reply);
	      free_and_init (replydata);
	      goto Err;
	    }

	  *replydata_ptr1 = reply;
	  *replydatasize_ptr1 = size;
	  reply = NULL;
	}

      if (reply_datasize2)
	{
	  replydata = (char *) malloc (DB_PAGESIZE);
	  if (replydata == NULL)
	    {
	      free_and_init (*replydata_ptr1);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_PAGESIZE);
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }

	  css_queue_user_data_buffer (conn, *request_id, reply_datasize2,
				      replydata);

	  rc = css_receive_response (conn, *request_id, &reply, &size);
	  if (rc != NO_ERRORS)
	    {
	      COMPARE_AND_FREE_BUFFER (replydata, reply);
	      free_and_init (replydata);
	      goto Err;
	    }

	  *replydata_ptr2 = reply;
	  *replydatasize_ptr2 = size;
	  reply = NULL;
	}
      break;

    default:

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_SERVER_DATA_RECEIVE,
	      0);
      return ER_NET_SERVER_DATA_RECEIVE;
    }

  return NO_ERROR;

Err:
  if (*replydata_ptr1)
    {
      free_and_init (*replydata_ptr1);
    }
  if (*replydata_ptr2)
    {
      free_and_init (*replydata_ptr2);
    }
  return set_server_error (rc);
}

/*
 * network_s2s_request_2_data -
 *
 * return: error status
 *
 *   conn(in): the conn of server side
 *   request(in): server request id
 *   request_id(in): server request id
 *   requestbuf(in): argument buffer (small)
 *   requestsize(in):  byte size of requestbuf
 *   replybuf(in): reply argument buffer (small)
 *   replysize(in): size of reply argument buffer
 *   databuf(in): data buffer to send (large)
 *   datasize(in): size of data buffer
 *   replydata_ptr1(in): receive data buffer (large)
 *   replydatasize_ptr1(in): size of expected reply data
 *
 */
int
network_s2s_request_2_data (CSS_CONN_ENTRY * conn,
			    int request,
			    unsigned short *request_id,
			    char *requestbuf, int requestsize,
			    char *replybuf, int replysize,
			    char *databuf, int datasize,
			    char **replydata_ptr1, int *replydatasize_ptr1)
{
  unsigned int rc;
  int size;
  int reply_datasize;
  char *reply = NULL, *replydata = NULL;

  (*replydata_ptr1) = NULL;
  (*replydatasize_ptr1) = 0;

  rc = css_send_req_with_2_buffers (conn,
				    request,
				    request_id,
				    requestbuf, requestsize,
				    databuf, datasize, replybuf, replysize);
  if (rc != NO_ERRORS)
    {
      goto Err;
    }

  rc = css_receive_response (conn, *request_id, &reply, &size);
  if (rc != NO_ERRORS || reply == NULL)
    {
      COMPARE_AND_FREE_BUFFER (replybuf, reply);
      goto Err;
    }

  /* here we assume that the first integer in the reply is the length
     of the following data block */
  or_unpack_int (reply, &reply_datasize);

  if (reply_datasize > 0)
    {
      replydata = (char *) malloc (reply_datasize);
      if (replydata == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, reply_datasize);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      css_queue_user_data_buffer (conn, *request_id, reply_datasize,
				  replydata);

      rc = css_receive_response (conn, *request_id, &reply, &size);
      if (rc != NO_ERRORS || reply == NULL)
	{
	  COMPARE_AND_FREE_BUFFER (replydata, reply);
	  free_and_init (replydata);
	  goto Err;
	}

      *replydata_ptr1 = reply;
      *replydatasize_ptr1 = size;
    }
  return NO_ERROR;

Err:
  return set_server_error (rc);
}

/*
 * network_s2s_request_1_data -
 *
 * return: error status
 *
 *   conn(in): the conn of server side
 *   request(in): server request id
 *   request_id(in): server request id
 *   requestbuf(in): argument buffer (small)
 *   requestsize(in):  byte size of requestbuf
 *   replybuf(in): reply argument buffer (small)
 *   replysize(in): size of reply argument buffer
 *
 */
int
network_s2s_request_1_data (CSS_CONN_ENTRY * conn,
			    int request,
			    unsigned short *request_id,
			    char *requestbuf, int requestsize,
			    char *replybuf, int replysize)
{
  unsigned int rc;
  int size;
  char *reply = NULL;

  rc = css_send_request_with_data_buffer (conn,
					  request,
					  request_id,
					  requestbuf, requestsize,
					  replybuf, replysize);
  if (rc != NO_ERRORS)
    {
      goto Err;
    }

  rc = css_receive_response (conn, *request_id, &reply, &size);
  if (rc != NO_ERRORS || reply == NULL)
    {
      COMPARE_AND_FREE_BUFFER (replybuf, reply);
      goto Err;
    }

  return NO_ERROR;

Err:
  return set_server_error (rc);
}

/*
 * network_s2s_request_3_data -
 *
 * return: error status (0 = success, non-zero = error)
 *
 *   request(in): server request id
 *   argbuf(in): argument buffer (small)
 *   argsize(in): byte size of argbuf
 *   databuf1(in): first data buffer to send
 *   datasize1(in): size of first data buffer
 *   databuf2(in): second data buffer to send
 *   datasize2(in): size of second data buffer
 *   reply0(in): first reply argument buffer (small)
 *   replysize0(in): size of first reply argument buffer
 *   reply1(in): second reply argument buffer
 *   replysize1(in): size of second reply argument buffer
 *   reply2(in): third reply argument buffer
 *   replysize2(in): size of third reply argument buffer
 *
 * Note: This is one of two functions that is called to perform a server
 *    request.
 */
int
network_s2s_request_3_data (CSS_CONN_ENTRY * conn,
			    int request,
			    unsigned short *request_id,
			    char *argbuf, int argsize,
			    char *databuf1, int datasize1,
			    char *databuf2, int datasize2,
			    char *reply0, int replysize0,
			    char *reply1, int replysize1,
			    char *reply2, int replysize2)
{
  int rc;
  int size;
  int p1_size, p2_size, error;
  char *reply = NULL, *ptr = NULL;
  error = NO_ERROR;

  rc = css_send_req_with_3_buffers (conn,
				    request,
				    request_id,
				    argbuf, argsize,
				    databuf1, datasize1,
				    databuf2, datasize2, reply0, replysize0);
  if (rc != NO_ERRORS)
    {
      goto Err;
    }

  rc = css_receive_response (conn, *request_id, &reply, &size);
  if (rc != NO_ERRORS || reply == NULL)
    {
      COMPARE_AND_FREE_BUFFER (reply0, reply);
      goto Err;
    }
  else
    {
      /* Ignore this error status here, since the caller must check it */
      ptr = or_unpack_int (reply0, &error);
      ptr = or_unpack_int (ptr, &p1_size);
      (void) or_unpack_int (ptr, &p2_size);

      if (p1_size == 0)
	{
	  COMPARE_AND_FREE_BUFFER (reply0, reply);
	  goto Err;
	}

      css_queue_user_data_buffer (conn, *request_id, p1_size, reply1);
      rc = css_receive_response (conn, *request_id, &reply, &size);
      if (rc != NO_ERRORS)
	{
	  COMPARE_AND_FREE_BUFFER (reply1, reply);
	  goto Err;
	}
      else
	{
	  error = COMPARE_SIZE_AND_BUFFER (&replysize1, size, &reply1, reply);
	}

      if (p2_size > 0)
	{
	  css_queue_user_data_buffer (conn, *request_id, p2_size, reply2);
	  rc = css_receive_response (conn, *request_id, &reply, &size);
	  if (rc != NO_ERRORS)
	    {
	      COMPARE_AND_FREE_BUFFER (reply2, reply);
	      goto Err;
	    }
	  else
	    {
	      error =
		COMPARE_SIZE_AND_BUFFER (&replysize2, size, &reply2, reply);
	    }
	}
    }

  return NO_ERROR;

Err:
  return set_server_error (rc);
}

/*
 * compare_size_and_buffer - this function is the same as compare_size_and_buffer
 *                           in network_cl.c
 *
 * return:
 *
 *   replysize(in):
 *   size(in):
 *   replybuf(in):
 *   buf(in):
 *   file(in):
 *   line(in):
 *
 * Note:
 *    Compares sizes and buffers that have been queued with the actual
 *    received values after a data read.  Called by macro of the same name.
 */
static int
compare_size_and_buffer (int *replysize, int size, char **replybuf, char *buf,
			 const char *file, const int line)
{
  int err = NO_ERROR;

  if (size != *replysize)
    {
      err = ER_NET_DATASIZE_MISMATCH;
      er_set (ER_ERROR_SEVERITY, file, line, err, 2, *replysize, size);
      *replysize = size;
    }

  if (buf != *replybuf)
    {
      err = ER_NET_UNUSED_BUFFER;
      er_set (ER_ERROR_SEVERITY, file, line, err, 0);
      /* free it ? */
      *replybuf = buf;
    }

  return err;
}

/*
 * set_server_error - Call it only the s2s connection occur error
 *
 * return:
 *   error(in):
 *
 * Note:
 */

static int
set_server_error (int error)
{
  int server_error;

  switch (error)
    {
    case CANT_ALLOC_BUFFER:
      server_error = ER_NET_CANT_ALLOC_BUFFER;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, server_error, 0);
      break;
    case RECORD_TRUNCATED:
      server_error = ER_NET_DATA_TRUNCATED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, server_error, 0);
      break;
    case REQUEST_REFUSED:
      server_error = er_errid ();
      break;
    case SERVER_ABORTED:
      server_error = er_errid ();
      /* those errors are generated by the net_server_request()
       * so that do not fall to server crash handling */
      switch (server_error)
	{
	case ER_DB_NO_MODIFICATIONS:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, server_error, 0);
	  return (server_error);
	case ER_AU_DBA_ONLY:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, server_error, 1, "");
	  return (server_error);
	}
      /* no break; fall through */
    default:
      server_error = ER_NET_SERVER_CRASHED;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, server_error, 0);
      break;
    }

  er_log_debug (ARG_FILE_LINE, "set_server_error(%d) server_error %d\n",
		error, server_error);

  db_Connect_status = DB_CONNECTION_STATUS_NOT_CONNECTED;

  return (server_error);
}
