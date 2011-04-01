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

#ifndef _NETWORK_S2S_H_
#define _NETWORK_S2S_H_

#include "config.h"

extern int
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
				   int *replydatasize_ptr2);

extern int
network_s2s_request_2_data (CSS_CONN_ENTRY * conn,
			    int request,
			    unsigned short *request_id,
			    char *requestbuf, int requestsize,
			    char *replybuf, int replysize,
			    char *databuf, int datasize,
			    char **replydata_ptr1, int *replydatasize_ptr1);

extern int
network_s2s_request_1_data (CSS_CONN_ENTRY * conn,
			    int request,
			    unsigned short *request_id,
			    char *requestbuf, int requestsize,
			    char *replybuf, int replysize);

extern int
network_s2s_request_3_data (CSS_CONN_ENTRY * conn,
			    int request,
			    unsigned short *request_id,
			    char *argbuf, int argsize,
			    char *databuf1, int datasize1,
			    char *databuf2, int datasize2,
			    char *reply0, int replysize0,
			    char *reply1, int replysize1,
			    char *reply2, int replysize2);
#endif
