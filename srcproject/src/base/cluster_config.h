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
 * cluster_config.h - config for cluster
 */

#ifndef _CLUSTER_CONFIG_H_
#define _CLUSTER_CONFIG_H_

#ident "$Id$"

#include "dbdef.h"

extern int ccf_init (void);
extern void ccf_final (void);

extern MOP ccf_find_node (const char *node_name);

extern unsigned int ccf_get_node_ip (MOP node);

extern int ccf_get_node_port (MOP node);

extern int ccf_register_node (const char *node_name, const char *db_host,
			      int cubrid_port);

extern int ccf_unregister_node (const char *node_name);

extern void ccf_set_need_read_flag (void);

extern int ccf_get_node_count (void);

extern DB_OBJLIST *ccf_get_node_list (void);

extern MOP ccf_get_connected_node (void);

extern const char *ccf_get_connected_node_name (void);


#endif
