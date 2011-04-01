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

#ifndef _NETWORK_INTERFACE_S2S_H_
#define _NETWORK_INTERFACE_S2S_H_

#include "config.h"
#include "query_list.h"

extern int remote_end_query (THREAD_ENTRY * thread_p,
			     int remote_id, QUERY_ID query_id, int *status);

extern int
remote_locator_force (THREAD_ENTRY * thread_p,
		      LC_COPYAREA * copy_area, int node_id);

extern int
remote_get_list_file_page (THREAD_ENTRY * thread_p,
			   int remote_node_id,
			   QUERY_ID query_id,
			   VOLID volid,
			   PAGEID pageid, char **buffer, int *buffer_size);

extern int
remote_prepare_and_execute_query (THREAD_ENTRY * thread_p,
				  int remote_node_id,
				  char *xasl_buffer,
				  int xasl_size,
				  const DB_VALUE * dbvals_p,
				  int dbval_count,
				  QUERY_FLAG flag,
				  QFILE_LIST_ID ** list_id_p);

extern int
remote_get_num_objects (THREAD_ENTRY * thread_p, int remote_id,
			const HFID * hfid, int *npages, int *nobjs,
			int *avg_length);

extern int
serial_get_real_oid (THREAD_ENTRY * thread_p, DB_VALUE * name_val,
		     OID * real_oid, int node_id);
extern int
serial_get_cache_range (THREAD_ENTRY * thread_p, OID * real_oid,
			DB_VALUE * start_val, DB_VALUE * end_val,
			int node_id);

extern TRAN_STATE log_2pc_send_prepare_to_particp (LOG_2PC_PARTICIPANT *
						   particp);

extern TRAN_STATE log_2pc_send_commit_to_particp (LOG_2PC_PARTICIPANT *
						  particp);

extern TRAN_STATE log_2pc_send_abort_to_particp (LOG_2PC_PARTICIPANT *
						 particp);

#endif
