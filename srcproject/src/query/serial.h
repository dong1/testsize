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
 * serial.h: interface for serial functions
 */

#ifndef _SERIAL_H_
#define _SERIAL_H_

#ident "$Id$"

#include "dbtype.h"
#include "thread_impl.h"

#define SR_ATT_NAME                     "name"
#define SR_ATT_OWNER                    "owner"
#define SR_ATT_CURRENT_VAL              "current_val"
#define SR_ATT_INCREMENT_VAL            "increment_val"
#define SR_ATT_MAX_VAL                  "max_val"
#define SR_ATT_MIN_VAL                  "min_val"
#define SR_ATT_CYCLIC                   "cyclic"
#define SR_ATT_STARTED                  "started"
#define SR_ATT_CLASS_NAME               "class_name"
#define SR_ATT_ATT_NAME                 "att_name"
#define SR_ATT_CACHED_NUM               "cached_num"
#define SR_ATT_IS_GLOBAL                "is_global"
#define SR_ATT_NODE_NAME                "node_name"

extern int xserial_get_current_value (THREAD_ENTRY * thread_p,
				      const DB_VALUE * oid_str_val,
				      DB_VALUE * result_num);
extern int xserial_get_next_value (THREAD_ENTRY * thread_p,
				   const DB_VALUE * oid_str_val,
				   DB_VALUE * result_num);
extern void serial_finalize_cache_pool (void);
extern int serial_initialize_cache_pool (THREAD_ENTRY * thread_p);
extern void xserial_decache (OID * oidp);
#if defined(SERVER_MODE)
extern int xserial_get_real_oid (THREAD_ENTRY * thread_p, DB_VALUE * name_val,
				 OID * real_oid);
extern int xserial_get_cache_range (THREAD_ENTRY * thread_p, OID * serial_oid,
				    DB_VALUE * start_val, DB_VALUE * end_val);
#endif
#endif /* _SERIAL_H_ */
