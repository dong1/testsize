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
 * cas_handle.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(WINDOWS)
#include <winsock2.h>
#include <windows.h>
#else /* WINDOWS */
#include <unistd.h>
#endif /* WINDOWS */

#if !defined(CAS_FOR_ORACLE) && !defined(CAS_FOR_MYSQL)
#include "cas_db_inc.h"
#endif /* !CAS_FOR_ORACLE && !CAS_FOR_MYSQL */
#include "cas_execute.h"

#include "cas.h"
#include "cas_common.h"
#include "cas_handle.h"
#include "cas_log.h"

#define SRV_HANDLE_ALLOC_SIZE		256

static void srv_handle_content_free (T_SRV_HANDLE * srv_handle);
static void col_update_info_free (T_QUERY_RESULT * q_result);
static void srv_handle_rm_tmp_file (int h_id, T_SRV_HANDLE * srv_handle);

static T_SRV_HANDLE **srv_handle_table = NULL;
static int max_srv_handle = 0;
static int max_handle_id = 0;

static T_SRV_HANDLE **active_handle_table = NULL;
static int active_handle_table_size = 0;
static int active_handle_count = 0;

int
hm_new_srv_handle (T_SRV_HANDLE ** new_handle, unsigned int seq_num)
{
  int i;
  int new_max_srv_handle;
  int new_handle_id = 0;
  T_SRV_HANDLE **new_srv_handle_table = NULL;
  T_SRV_HANDLE *srv_handle;

  for (i = 0; i < max_srv_handle; i++)
    {
      if (srv_handle_table[i] == NULL)
	{
	  *new_handle = srv_handle_table[i];
	  new_handle_id = i + 1;
	  break;
	}
    }

  if (new_handle_id == 0)
    {
      new_max_srv_handle = max_srv_handle + SRV_HANDLE_ALLOC_SIZE;
      new_srv_handle_table = (T_SRV_HANDLE **)
	REALLOC (srv_handle_table,
		 sizeof (T_SRV_HANDLE *) * new_max_srv_handle);
      if (new_srv_handle_table == NULL)
	{
	  return ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
	}

      new_handle_id = max_srv_handle + 1;
      memset (new_srv_handle_table + max_srv_handle,
	      0, sizeof (T_SRV_HANDLE *) * SRV_HANDLE_ALLOC_SIZE);
      max_srv_handle = new_max_srv_handle;
      srv_handle_table = new_srv_handle_table;
    }

  srv_handle = (T_SRV_HANDLE *) MALLOC (sizeof (T_SRV_HANDLE));
  if (srv_handle == NULL)
    {
      return ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
    }
  memset (srv_handle, 0, sizeof (T_SRV_HANDLE));
  srv_handle->id = new_handle_id;
  srv_handle->query_seq_num = seq_num;
  srv_handle->use_plan_cache = false;
  srv_handle->use_query_cache = false;
  srv_handle->is_fetch_completed = false;

  *new_handle = srv_handle;
  srv_handle_table[new_handle_id - 1] = srv_handle;
  if (new_handle_id > max_handle_id)
    max_handle_id = new_handle_id;
  return new_handle_id;
}

T_SRV_HANDLE *
hm_find_srv_handle (int h_id)
{
  if (h_id <= 0 || h_id > max_srv_handle)
    return NULL;

  return (srv_handle_table[h_id - 1]);
}

void
hm_srv_handle_free (int h_id)
{
  T_SRV_HANDLE *srv_handle;

  if (h_id <= 0 || h_id > max_srv_handle)
    return;

  srv_handle = srv_handle_table[h_id - 1];
  if (srv_handle == NULL)
    return;

  srv_handle_content_free (srv_handle);
  srv_handle_rm_tmp_file (h_id, srv_handle);

#if !defined(CAS_FOR_ORACLE) && !defined(CAS_FOR_MYSQL)
  FREE_MEM (srv_handle->classes);
  FREE_MEM (srv_handle->classes_chn);
#endif /* !CAS_FOR_ORACLE && !CAS_FOR_MYSQL */

  FREE_MEM (srv_handle);
  srv_handle_table[h_id - 1] = NULL;
}

void
hm_srv_handle_free_all ()
{
  T_SRV_HANDLE *srv_handle;
  int i;

  for (i = 0; i < max_handle_id; i++)
    {
      srv_handle = srv_handle_table[i];
      if (srv_handle == NULL)
	continue;
      srv_handle_content_free (srv_handle);
      srv_handle_rm_tmp_file (i + 1, srv_handle);
      FREE_MEM (srv_handle);
      srv_handle_table[i] = NULL;
    }
  max_handle_id = 0;
}

#if defined (ENABLE_UNUSED_FUNCTION)
void
hm_srv_handle_set_pooled ()
{
  T_SRV_HANDLE *srv_handle;
  int i;

  for (i = 0; i < max_handle_id; i++)
    {
      srv_handle = srv_handle_table[i];
      if (srv_handle == NULL)
	continue;
      srv_handle->is_pooled = 1;
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */

void
hm_qresult_clear (T_QUERY_RESULT * q_result)
{
  memset (q_result, 0, sizeof (T_QUERY_RESULT));
}

void
hm_qresult_end (T_SRV_HANDLE * srv_handle, char free_flag)
{
  T_QUERY_RESULT *q_result;
  int i;
  q_result = srv_handle->q_result;
#if defined(CAS_FOR_ORACLE) || defined(CAS_FOR_MYSQL)
  if (free_flag == TRUE)
    {
      ux_free_result (q_result);
      FREE_MEM (q_result);
      srv_handle->q_result = NULL;
    }
#else /* CAS_FOR_ORACLE || CAS_FOR_MYSQL */
  if (q_result)
    {
      for (i = 0; i < srv_handle->num_q_result; i++)
	{
	  if (q_result[i].copied != TRUE && q_result[i].result)
	    {
	      ux_free_result (q_result[i].result);
	    }
	  q_result[i].result = NULL;

	  if (q_result[i].column_info)
	    {
	      db_query_format_free ((DB_QUERY_TYPE *) q_result[i].
				    column_info);
	    }

	  q_result[i].column_info = NULL;
	  if (free_flag == TRUE)
	    {
	      col_update_info_free (&(q_result[i]));
	      FREE_MEM (q_result[i].null_type_column);
	    }
	}

      if (free_flag == TRUE)
	{
	  FREE_MEM (q_result);
	}
    }

  if (free_flag == TRUE)
    {
      srv_handle->q_result = NULL;
      srv_handle->num_q_result = 0;
    }

  srv_handle->cur_result = NULL;
  srv_handle->cur_result_index = 0;
#endif /* CAS_FOR_ORACLE || CAS_FOR_MYSQL */
}

void
hm_session_free (T_SRV_HANDLE * srv_handle)
{
  if (srv_handle->session)
    {
#if defined(CAS_FOR_ORACLE)
      cas_oracle_stmt_close (srv_handle->session);
#elif defined(CAS_FOR_MYSQL)
      cas_mysql_stmt_close (srv_handle->session);
#else /* CAS_FOR_ORACLE */
      db_close_session ((DB_SESSION *) (srv_handle->session));
#endif /* CAS_FOR_ORACLE */
    }
  srv_handle->session = NULL;
}

void
hm_col_update_info_clear (T_COL_UPDATE_INFO * col_update_info)
{
  memset (col_update_info, 0, sizeof (T_COL_UPDATE_INFO));
}

static void
srv_handle_content_free (T_SRV_HANDLE * srv_handle)
{
#if defined(CAS_FOR_ORACLE) || defined(CAS_FOR_MYSQL)
  FREE_MEM (srv_handle->sql_stmt);
  ux_prepare_call_info_free (srv_handle->prepare_call_info);
  hm_qresult_end (srv_handle, TRUE);
  hm_session_free (srv_handle);
#else /* CAS_FOR_ORACLE || CAS_FOR_MYSQL */
  FREE_MEM (srv_handle->sql_stmt);
  ux_prepare_call_info_free (srv_handle->prepare_call_info);
  if (srv_handle->schema_type < 0
      || srv_handle->schema_type == CCI_SCH_CLASS
      || srv_handle->schema_type == CCI_SCH_VCLASS
      || srv_handle->schema_type == CCI_SCH_ATTRIBUTE
      || srv_handle->schema_type == CCI_SCH_CLASS_ATTRIBUTE
      || srv_handle->schema_type == CCI_SCH_QUERY_SPEC
      || srv_handle->schema_type == CCI_SCH_DIRECT_SUPER_CLASS
      || srv_handle->schema_type == CCI_SCH_PRIMARY_KEY)
    {
      hm_qresult_end (srv_handle, TRUE);
      hm_session_free (srv_handle);
    }
  else if (srv_handle->schema_type == CCI_SCH_CLASS_PRIVILEGE
	   || srv_handle->schema_type == CCI_SCH_ATTR_PRIVILEGE
	   || srv_handle->schema_type == CCI_SCH_SUPERCLASS
	   || srv_handle->schema_type == CCI_SCH_SUBCLASS)
    {
      FREE_MEM (srv_handle->session);
      srv_handle->cur_result = NULL;
    }
  else if (srv_handle->schema_type == CCI_SCH_TRIGGER)
    {
      if (srv_handle->session)
	db_objlist_free ((DB_OBJLIST *) (srv_handle->session));
      srv_handle->cur_result = NULL;
    }
#endif /* CAS_FOR_ORACLE || CAS_FOR_MYSQL */
}

static void
col_update_info_free (T_QUERY_RESULT * q_result)
{
  int i;

#if !defined(CAS_FOR_ORACLE) && !defined(CAS_FOR_MYSQL)
  if (q_result->col_update_info)
    {
      for (i = 0; i < q_result->num_column; i++)
	{
	  FREE_MEM (q_result->col_update_info[i].attr_name);
	  FREE_MEM (q_result->col_update_info[i].class_name);
	}
      FREE_MEM (q_result->col_update_info);
    }
  q_result->col_updatable = FALSE;
  q_result->num_column = 0;
#endif /* !CAS_FOR_ORACLE && !CAS_FOR_MYSQL */
}

static void
srv_handle_rm_tmp_file (int h_id, T_SRV_HANDLE * srv_handle)
{
#if !defined(CAS_FOR_ORACLE) && !defined(CAS_FOR_MYSQL)
  if (srv_handle->query_info_flag == TRUE)
    {
      char *p;
      p = cas_log_query_plan_file (h_id);
      if (p != NULL)
	{
	  unlink (p);
	}
      p = cas_log_query_histo_file (h_id);
      if (p != NULL)
	{
	  unlink (p);
	}
    }
#endif /* !CAS_FOR_ORACLE && !CAS_FOR_MYSQL */
}

int
hm_srv_handle_append_active (T_SRV_HANDLE * srv_handle)
{
  if (srv_handle == NULL)
    {
#if !defined(CAS_FOR_ORACLE) && !defined(CAS_FOR_MYSQL)
      return 0;
#else
      return CCI_ER_NO_ERROR;
#endif
    }

  if (active_handle_table == NULL
      || active_handle_count >=
      (active_handle_table_size / sizeof (T_SRV_HANDLE *)))
    {
      if (active_handle_table_size == 0)
	{
	  active_handle_table_size =
	    sizeof (T_SRV_HANDLE *) * SRV_HANDLE_ALLOC_SIZE;
	}
      else
	{
	  active_handle_table_size *= 2;
	}

      active_handle_table = (T_SRV_HANDLE **) REALLOC (active_handle_table,
						       active_handle_table_size);

      if (active_handle_table == NULL)
	{
	  active_handle_count = 0;
	  active_handle_table_size = 0;
	  return ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
	}
    }

  active_handle_table[active_handle_count] = srv_handle;
  active_handle_count++;
  srv_handle->is_fetch_completed = false;

#if !defined(CAS_FOR_ORACLE) && !defined(CAS_FOR_MYSQL)
  return 0;
#else /* !CAS_FOR_ORACLE && !CAS_FOR_MYSQL */
  return CCI_ER_NO_ERROR;
#endif /* !CAS_FOR_ORACLE && !CAS_FOR_MYSQL */
}

void
hm_srv_handle_set_fetch_completed (T_SRV_HANDLE * srv_handle)
{
  srv_handle->is_fetch_completed = true;
}

bool
hm_srv_handle_is_all_active_fetch_completed (void)
{
  int i;
  T_SRV_HANDLE *srv_handle;

  for (i = 0; i < active_handle_count; i++)
    {
      srv_handle = active_handle_table[i];
      if (srv_handle->is_fetch_completed != true)
	{
	  return false;
	}
    }

  return true;
}

void
hm_srv_handle_reset_active (void)
{
  active_handle_count = 0;
}
