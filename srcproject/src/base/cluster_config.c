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
 * cluster_config.c : config for cluster
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>

#if defined(WINDOWS)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif

#include "cluster_config.h"
#include "dbi.h"
#include "dbdef.h"
#include "error_manager.h"
#include "object_accessor.h"
#include "transform.h"
#include "schema_manager.h"
#include "authenticate.h"
#include "misc_string.h"
#include "porting.h"

#define MAX_HOST_NAME_LEN  64
#define MAX_NODE_NAME_LEN  64
#define HOST_IP_CACHE_SIZE DB_MAX_NODE_IN_CLUSTER

#define NODE_NAME_ATTR    "node_name"
#define DB_HOST_ATTR      "db_host"
#define CUBRID_PORT_ATTR  "cubrid_port"

#define CCF_SMART_LOAD_INFO(_error_return_) \
  do{if(Ccf_need_load_cluster_info && ccf_read_configuration_info() < 0) \
      {return (_error_return_);}\
    }while(0)

static DB_OBJECT *Ccf_cluster_node_class;
static bool Ccf_need_load_cluster_info;

static DB_OBJLIST *Ccf_cluster_node_list;
static int Ccf_cluster_node_count;
static MOP Ccf_connected_node;
static char Ccf_connected_node_name[MAX_NODE_NAME_LEN + 1];

typedef struct ccf_ip_cache_entry CCF_IP_CACHE_ENTRY;
struct ccf_ip_cache_entry
{
  char *host_name;
  unsigned int host_ip;
  unsigned int used_count;
};

typedef struct ccf_cache_mgr CCF_CACHE_MGR;
struct ccf_cache_mgr
{
  char host_name_buffer[HOST_IP_CACHE_SIZE][MAX_HOST_NAME_LEN + 1];
  int sorted_cache_num;
  int cache_count;
  CCF_IP_CACHE_ENTRY ip_cache_entry[HOST_IP_CACHE_SIZE];
};

static CCF_CACHE_MGR *Ccf_cache_mgr;

static bool ccf_is_new_ip (unsigned int host_ip);
static unsigned int ccf_get_connected_host_ip ();
static int ccf_is_global_table_on_node (const char *node, bool * result);
static int ccf_read_configuration_info ();

static int cache_cmp (const void *arg1, const void *arg2);
static unsigned int ccf_find_ip_in_cache (const char *host_name);
static void ccf_add_ip_to_cache (const char *host_name, unsigned int ip);
static int ccf_get_host_main_ip (const char *host_name);
/*
 * cache_cmp() - compare function. Used for sort and search the cache items
 * return : return value is the same as strcasecmp
 */
static int
cache_cmp (const void *arg1, const void *arg2)
{
  return strcasecmp (((CCF_IP_CACHE_ENTRY *) arg1)->host_name,
		     ((CCF_IP_CACHE_ENTRY *) arg2)->host_name);
}

/*
 * ccf_find_ip_in_cache() - find ip address in the cache
 * host_name(in) : host name
 * return : If found return the IP, else return INADDR_NONE
 */
static unsigned int
ccf_find_ip_in_cache (const char *host_name)
{
  CCF_IP_CACHE_ENTRY *entry_ptr = NULL;
  int i;

  if (Ccf_cache_mgr->sorted_cache_num > 0)
    {
      CCF_IP_CACHE_ENTRY tmp_entry;
      tmp_entry.host_name = (char *) host_name;
      entry_ptr =
	(CCF_IP_CACHE_ENTRY *) bsearch (&tmp_entry,
					Ccf_cache_mgr->ip_cache_entry,
					Ccf_cache_mgr->sorted_cache_num,
					sizeof (CCF_IP_CACHE_ENTRY),
					cache_cmp);
    }

  if (entry_ptr == NULL)
    {
      for (i = Ccf_cache_mgr->sorted_cache_num;
	   i < Ccf_cache_mgr->cache_count; i++)
	{
	  if (strcasecmp
	      (host_name, Ccf_cache_mgr->ip_cache_entry[i].host_name) == 0)
	    {
	      entry_ptr = Ccf_cache_mgr->ip_cache_entry + i;
	    }
	}
    }

  if (entry_ptr == NULL)
    {
      return INADDR_NONE;
    }

  entry_ptr->used_count++;
  return entry_ptr->host_ip;
}

/*
 * ccf_add_ip_to_cache() - add the host name to IP mapping to cache.
 *                         Don't add invalid IP to cache.
 * host_name(in) : host name
 * ip(in): IP address
 * return : NULL
 */
static void
ccf_add_ip_to_cache (const char *host_name, unsigned int ip)
{
  CCF_IP_CACHE_ENTRY *entry_ptr;

  if (!is_valid_ip (ip))
    {
      return;
    }

  if (Ccf_cache_mgr->cache_count == HOST_IP_CACHE_SIZE)
    {
      /* The cache is full, replace the least used cache item */
      int i = HOST_IP_CACHE_SIZE - 1;
      unsigned int min_value;

      min_value = Ccf_cache_mgr->ip_cache_entry[i].used_count;
      entry_ptr = Ccf_cache_mgr->ip_cache_entry + i;
      for (; i != -1; --i)
	{
	  if (min_value > Ccf_cache_mgr->ip_cache_entry[i].used_count)
	    {
	      min_value = Ccf_cache_mgr->ip_cache_entry[i].used_count;
	      entry_ptr = Ccf_cache_mgr->ip_cache_entry + i;
	      /* The cache need sort */
	      if (Ccf_cache_mgr->sorted_cache_num > i)
		{
		  Ccf_cache_mgr->sorted_cache_num = i;
		}
	    }
	}
    }
  else
    {
      entry_ptr = Ccf_cache_mgr->ip_cache_entry + Ccf_cache_mgr->cache_count;
      Ccf_cache_mgr->cache_count++;
    }

  entry_ptr->host_ip = ip;
  strncpy (entry_ptr->host_name, host_name, MAX_HOST_NAME_LEN);
  entry_ptr->host_name[MAX_HOST_NAME_LEN] = '\0';
  entry_ptr->used_count = 1;
}

/*
 * ccf_get_host_main_ip() - get the IP address of given host. This function
 *            will try to get IP from cache firstly, it not found IP in cache,
 *            get the IP by other way then add the ip into the cache.
 * return : the IP address.
 */
static int
ccf_get_host_main_ip (const char *host_name)
{
  unsigned int ip = ccf_find_ip_in_cache (host_name);
  if (ip == INADDR_NONE)
    {
      ip = get_host_main_ip (host_name);
      ccf_add_ip_to_cache (host_name, ip);
    }
  return ip;
}

/*
 * ccf_get_connected_host_ip() - get the IP address of the connected host
 * return : the IP address.
 */
static unsigned int
ccf_get_connected_host_ip ()
{
#if defined(CS_MODE)
  const char *host_name = db_get_host_connected ();
  return ccf_get_host_main_ip (host_name);

#else
  return get_my_host_id ();
#endif
}

/*
 * ccf_is_new_ip() - get whether the given IP already exists in cluster
 * return : true if does not exist in cluster, false else
 */
static bool
ccf_is_new_ip (unsigned int host_ip)
{
  DB_OBJLIST *node_ptr = Ccf_cluster_node_list;
  while (node_ptr)
    {
      if (ccf_get_node_ip (node_ptr->op) == host_ip)
	{
	  return false;
	}
      node_ptr = node_ptr->next;
    }
  return true;
}

/*
 * ccf_is_global_table_on_node() - get whether the given node contains global table
 * node_name(in) : node name
 * result(out) : if the given node contains global table the value will be set to
 *     true, else the value will be set to false
 * return : error code
 */
static int
ccf_is_global_table_on_node (const char *node_name, bool * result)
{
  DB_OBJECT *db_class;
  DB_OBJLIST *db_class_instances, *cur_class;
  DB_VALUE node_name_val;
  char *class_node_name;
  int err_code = NO_ERROR;
  int au_save;

  AU_DISABLE (au_save);
  *result = false;
  db_class = sm_find_class (CT_CLASS_NAME);
  if (db_class == NULL)
    {
      err_code = er_errid ();
      goto end;
    }

  db_class_instances = db_get_all_objects (db_class);
  for (cur_class = db_class_instances; cur_class; cur_class = cur_class->next)
    {
      db_make_null (&node_name_val);
      err_code = obj_get (cur_class->op, "node_name", &node_name_val);
      if (err_code != NO_ERROR)
	{
	  goto end;
	}
      class_node_name = db_get_string (&node_name_val);
      if (class_node_name != NULL
	  && strcasecmp (class_node_name, node_name) == 0)
	{
	  *result = true;
	  pr_clear_value (&node_name_val);
	  break;
	}
      pr_clear_value (&node_name_val);
    }

end:
  AU_ENABLE (au_save);
  return err_code;
}

/*
 * ccf_read_configuration_info () - read cluster configuration info from the
 *    system catalog db_cluster_node
 * return: error code
 */
static int
ccf_read_configuration_info ()
{
  DB_OBJLIST *mops;
  DB_VALUE node_name_val;
  char *node_name;
  int node_count, err_code;
  unsigned int connected_node_ip = ccf_get_connected_host_ip ();

  *Ccf_connected_node_name = '\0';
  if (!is_valid_ip (htonl (connected_node_ip)))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_CCF_CANT_GET_CONNECTED_NODE_INFO, 0);
      return ER_CCF_CANT_GET_CONNECTED_NODE_INFO;
    }

  if (Ccf_cluster_node_class == NULL)
    {
      Ccf_cluster_node_class = sm_find_class (CT_CLUSTER_NODE_NAME);
    }

  if (Ccf_cluster_node_class == NULL)
    {
      return er_errid ();
    }
  if (Ccf_cluster_node_list != NULL)
    {
      db_objlist_free (Ccf_cluster_node_list);
    }

  Ccf_connected_node = NULL;
  Ccf_cluster_node_list = db_get_all_objects (Ccf_cluster_node_class);
  for (mops = Ccf_cluster_node_list, node_count = 0; mops; mops = mops->next)
    {
      node_count++;
      if (Ccf_connected_node == NULL
	  && connected_node_ip == ccf_get_node_ip (mops->op))
	{
	  db_make_null (&node_name_val);
	  err_code = obj_get (mops->op, NODE_NAME_ATTR, &node_name_val);
	  if (err_code != NO_ERROR)
	    {
	      return err_code;
	    }
	  node_name = db_get_string (&node_name_val);
	  strncpy (Ccf_connected_node_name, node_name,
		   sizeof (Ccf_connected_node_name));
	  Ccf_connected_node_name[sizeof (Ccf_connected_node_name) - 1] =
	    '\0';
	  Ccf_connected_node = mops->op;
	  pr_clear_value (&node_name_val);
	}
    }
  if (node_count > 0 && Ccf_connected_node == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_CCF_CANT_GET_CONNECTED_NODE_INFO, 0);
      return ER_CCF_CANT_GET_CONNECTED_NODE_INFO;
    }
  Ccf_cluster_node_count = node_count;
  Ccf_need_load_cluster_info = false;

  if (Ccf_cache_mgr->sorted_cache_num != Ccf_cache_mgr->cache_count)
    {
      qsort (Ccf_cache_mgr->ip_cache_entry, Ccf_cache_mgr->cache_count,
	     sizeof (CCF_IP_CACHE_ENTRY), cache_cmp);
      Ccf_cache_mgr->sorted_cache_num = Ccf_cache_mgr->cache_count;
    }

  return NO_ERROR;
}

/*
 * ccf_init() - this is called when the database is being restarted
 * return : error code
 */
int
ccf_init (void)
{
  int i, error_code = NO_ERROR;
  size_t size = sizeof (CCF_CACHE_MGR);
  Ccf_need_load_cluster_info = true;
  Ccf_cluster_node_class = NULL;
  Ccf_cluster_node_list = NULL;
  Ccf_connected_node = NULL;
  *Ccf_connected_node_name = '\0';
  Ccf_cluster_node_count = 0;
  Ccf_cache_mgr = (CCF_CACHE_MGR *) malloc (size);
  if (Ccf_cache_mgr == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, size);
      return error_code;
    }

  for (i = 0; i < HOST_IP_CACHE_SIZE; i++)
    {
      Ccf_cache_mgr->ip_cache_entry[i].host_name =
	Ccf_cache_mgr->host_name_buffer[i];
    }
  Ccf_cache_mgr->cache_count = Ccf_cache_mgr->sorted_cache_num = 0;

  return error_code;
}

/*
 * ccf_final() - called during the bo_shutdown sequence
 * return : none
 */

void
ccf_final (void)
{
  Ccf_need_load_cluster_info = false;
  Ccf_cluster_node_class = NULL;
  if (Ccf_cluster_node_list != NULL)
    {
      db_objlist_free (Ccf_cluster_node_list);
    }
  Ccf_cluster_node_list = NULL;
  Ccf_connected_node = NULL;
  *Ccf_connected_node_name = '\0';
  Ccf_cluster_node_count = 0;
  free_and_init (Ccf_cache_mgr);
}

/*
 * ccf_find_node() - get the cluster node MOP by node name
 * return : cluster node object if it's found, else return NULL
 * node_name(in) : cluster node name
 */
MOP
ccf_find_node (const char *node_name)
{
  MOP node = NULL;
  DB_OBJLIST *mops;
  DB_VALUE node_name_val;
  char *node_name_ptr;

  CCF_SMART_LOAD_INFO (NULL);

  for (mops = Ccf_cluster_node_list; mops; mops = mops->next)
    {
      if (obj_get (mops->op, NODE_NAME_ATTR, &node_name_val) != NO_ERROR)
	{
	  return NULL;
	}
      node_name_ptr = db_get_string (&node_name_val);
      if (strcasecmp (node_name_ptr, node_name) == 0)
	{
	  return mops->op;
	}
    }

  pr_clear_value (&node_name_val);
  return node;
}

/*
 * ccf_get_node_ip() - get the cluster node IP address from the given
 *    cluster node object
 * return : the IP address of cluster node
 * node(in) : cluster node
 */
unsigned int
ccf_get_node_ip (MOP node)
{
  unsigned int node_ip = INADDR_NONE;
  DB_VALUE db_host_val;
  char *db_host;

  db_make_null (&db_host_val);

  if (obj_get (node, DB_HOST_ATTR, &db_host_val) != NO_ERROR)
    {
      return node_ip;
    }

  db_host = db_get_string (&db_host_val);
  node_ip = ccf_get_host_main_ip (db_host);

  pr_clear_value (&db_host_val);
  return node_ip;
}

/*
 * ccf_get_node_port() - get the cluster node port
 * return : port value if get it, else return -1
 * node_name(in)  : cluster node name
 */
int
ccf_get_node_port (MOP node)
{
  int cubrid_port = -1;
  DB_VALUE cubrid_port_val;

  db_make_null (&cubrid_port_val);

  if (obj_get (node, CUBRID_PORT_ATTR, &cubrid_port_val) != NO_ERROR)
    {
      return cubrid_port;
    }

  cubrid_port = db_get_int (&cubrid_port_val);
  pr_clear_value (&cubrid_port_val);
  return cubrid_port;
}

/*
 * ccf_register_node() - register a new node into cluster
 * return : error code
 * node_name(in) : the node name of new cluster node
 * db_host(in) : the host name or host address of new cluster node
 * cubrid_port(in) : the port of new cluster node
 */
int
ccf_register_node (const char *node_name, const char *db_host,
		   int cubrid_port)
{
  int err_code = NO_ERROR;
  int au_save, db_host_len;
  unsigned int host_ip;
  DB_OBJECT *node;
  DB_VALUE node_name_val, db_host_val, cubrid_port_val;
  char lower_case_name[MAX_NODE_NAME_LEN + 1];

  CCF_SMART_LOAD_INFO (er_errid ());

  if (!au_is_dba_group_member (Au_user))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_AU_DBA_ONLY, 1,
	      "register_node");
      return ER_AU_DBA_ONLY;
    }

  if (ccf_find_node (node_name) != NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_CCF_DUPLICATE_NODE, 0);
      return ER_CCF_DUPLICATE_NODE;
    }

  if (Ccf_cluster_node_count == DB_MAX_NODE_IN_CLUSTER)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_MAX_CLUSTER_NODE_NUM_EXCEEDED, 1, DB_MAX_NODE_IN_CLUSTER);
      return ER_MAX_CLUSTER_NODE_NUM_EXCEEDED;
    }

  lower_case_name[MAX_NODE_NAME_LEN] = '\0';
  strncpy (lower_case_name, node_name, MAX_NODE_NAME_LEN + 1);
  /* check the node name length here */
  if (lower_case_name[MAX_NODE_NAME_LEN] != '\0')
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_CCF_NODE_NAME_TOO_LONG,
	      1, MAX_NODE_NAME_LEN);
      return ER_CCF_NODE_NAME_TOO_LONG;
    }
  ustr_lower (lower_case_name);

  /* check host name length here */
  db_host_len = strlen (db_host);
  if (db_host_len > MAX_HOST_NAME_LEN)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_CCF_HOST_NAME_TOO_LONG,
	      1, MAX_HOST_NAME_LEN);
      return MAX_HOST_NAME_LEN;
    }

  host_ip = ccf_get_host_main_ip (db_host);
  if (!is_valid_ip (htonl (host_ip)))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_CCF_INVALID_HOST_ADDRESS,
	      0);
      return ER_CCF_INVALID_HOST_ADDRESS;
    }

  if (!ccf_is_new_ip (host_ip))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_CCF_DUPLICATE_ADDRESS,
	      0);
      return ER_CCF_DUPLICATE_ADDRESS;
    }

  if (Ccf_cluster_node_count == 0 && host_ip != ccf_get_connected_host_ip ())
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_CCF_CANT_REGISTER_OTHER_DB_FIRSTLY, 0);
      return ER_CCF_CANT_REGISTER_OTHER_DB_FIRSTLY;
    }

  if (cubrid_port <= 0 || cubrid_port > 65535)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_CCF_INVALID_PORT_VALUE,
	      0);
      return ER_CCF_INVALID_PORT_VALUE;
    }

  db_make_null (&node_name_val);
  db_make_null (&db_host_val);
  db_make_null (&cubrid_port_val);

  AU_DISABLE (au_save);

  if ((node = obj_create (Ccf_cluster_node_class)) == NULL)
    {
      err_code = er_errid ();
      goto end;
    }

  db_make_string (&node_name_val, lower_case_name);
  db_make_string (&db_host_val, db_host);
  db_make_int (&cubrid_port_val, cubrid_port);

  err_code = obj_set (node, NODE_NAME_ATTR, &node_name_val);
  if (err_code != NO_ERROR)
    {
      goto end;
    }

  err_code = obj_set (node, DB_HOST_ATTR, &db_host_val);
  if (err_code != NO_ERROR)
    {
      goto end;
    }

  err_code = obj_set (node, CUBRID_PORT_ATTR, &cubrid_port_val);
  if (err_code != NO_ERROR)
    {
      goto end;
    }
  err_code = ccf_read_configuration_info ();
  if (err_code != NO_ERROR)
    {
      goto end;
    }

end:
  AU_ENABLE (au_save);
  pr_clear_value (&node_name_val);
  pr_clear_value (&db_host_val);
  pr_clear_value (&cubrid_port_val);
  return err_code;
}

/*
 * ccf_unregister_node() - unregister an existed node from cluster
 * return : error code
 * node_name(in) : the node name of the existed cluster node
 */
int
ccf_unregister_node (const char *node_name)
{
  int err_code = NO_ERROR;
  int au_save;
  DB_OBJECT *node;
  char lower_node_name[MAX_NODE_NAME_LEN + 1];
  bool is_node_contains_cluster_table;

  CCF_SMART_LOAD_INFO (er_errid ());
  if (!au_is_dba_group_member (Au_user))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_AU_DBA_ONLY, 1,
	      "unregister_node");
      return ER_AU_DBA_ONLY;
    }

  lower_node_name[MAX_NODE_NAME_LEN] = '\0';
  strncpy (lower_node_name, node_name, MAX_NODE_NAME_LEN + 1);
  /* check the length here */
  if (lower_node_name[MAX_NODE_NAME_LEN] != '\0')
    {
      /* node name too long, this node cannot in system catalog */
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_CCF_NOT_FIND_NODE, 0);
      return ER_CCF_NOT_FIND_NODE;
    }
  ustr_lower (lower_node_name);
  node = ccf_find_node (lower_node_name);
  if (node == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_CCF_NOT_FIND_NODE, 0);
      return ER_CCF_NOT_FIND_NODE;
    }

  err_code =
    ccf_is_global_table_on_node (node_name, &is_node_contains_cluster_table);
  if (err_code != NO_ERROR)
    {
      return err_code;
    }
  if (is_node_contains_cluster_table)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_CCF_NODE_NOT_EMPTY, 0);
      return ER_CCF_NODE_NOT_EMPTY;
    }
  au_save = au_disable ();
  if (node != Ccf_connected_node)
    {
      err_code = db_drop (node);
      if (err_code != NO_ERROR)
	{
	  goto end;
	}
    }
  else
    {
      /* Drop all cluster node information */
      DB_OBJLIST *cur = Ccf_cluster_node_list;
      while (cur)
	{
	  err_code = db_drop (cur->op);
	  if (err_code != NO_ERROR)
	    {
	      goto end;
	    }
	  cur = cur->next;
	}
    }

  err_code = ccf_read_configuration_info ();
  if (err_code != NO_ERROR)
    {
      goto end;
    }

end:
  au_enable (au_save);
  return err_code;
}

/*
 * ccf_set_need_read_flag () - set need read cluster info flag to true.
 *     should be called when transaction commit or abort
 * return : NULL
 */
void
ccf_set_need_read_flag (void)
{
  Ccf_need_load_cluster_info = true;
}

/*
 * ccf_get_node_count() - get how many nodes in the cluster
 * return : node count
 */
int
ccf_get_node_count (void)
{
  CCF_SMART_LOAD_INFO (0);
  return Ccf_cluster_node_count;
}

/*
 * ccf_get_node_list() - get cluster node list which contains all
 *     cluster node object
 * return : cluster node object list
 */
DB_OBJLIST *
ccf_get_node_list (void)
{
  CCF_SMART_LOAD_INFO (NULL);
  return Ccf_cluster_node_list;
}

/*
 * ccf_get_connected_node() - get connected cluster node object
 * return : the node object of the connected node,
 *    if connected DB is in cluster, else return NULL.
 */
MOP
ccf_get_connected_node (void)
{
  CCF_SMART_LOAD_INFO (NULL);
  return Ccf_connected_node;
}

/*
 * ccf_get_connected_node_name() - get the node name of the connected cluster node
 * return : the node name
 */
const char *
ccf_get_connected_node_name (void)
{
  CCF_SMART_LOAD_INFO (NULL);
  return Ccf_connected_node_name;
}
