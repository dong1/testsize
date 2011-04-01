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
 * remote_query.h - remote query execution
 */

#ident "$Id$"

#ifndef _REMOTE_QUERY_H_
#define _REMOTE_QUERY_H_

#include "config.h"

extern int
remote_xqmgr_execute_delete_query (THREAD_ENTRY * thread_p,
				   QUERY_ID query_id,
				   XASL_NODE * local_xasl,
				   const DB_VALUE * dbvals_p,
				   int dbval_count,
				   QFILE_LIST_ID ** remote_list_id_p);
extern int
remote_xqmgr_execute_update_query (THREAD_ENTRY * thread_p,
				   QUERY_ID query_id,
				   XASL_NODE * local_xasl,
				   const DB_VALUE * dbvals_p,
				   int dbval_count,
				   QFILE_LIST_ID ** remote_list_id_p);
#endif
