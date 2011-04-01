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
 * remote_query.c - remote execution
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
#include "query_manager.h"
#include "network_interface_s2s.h"

#define QEXEC_REMOTE_OUTPTR_LIST(xasl, ptrptr) \
  do { \
    *(ptrptr) = (xasl)->outptr_list->valptrp->value.value.dbvalptr;\
    (xasl)->outptr_list->valptrp->value.value.dbvalptr = (xasl)->val_list->valp->val;\
  } while (0)

#define QEXEC_LOCAL_OUTPTR_LIST(xasl, ptr)\
  do{\
    (xasl)->outptr_list->valptrp->value.value.dbvalptr = (ptr);\
  } while (0)

#define REXEC_SET_ZERO_ON_SPEC(tmp_spec, node_id) \
  do { \
    while ((tmp_spec))\
    {\
      if ((tmp_spec)->s.cls_node.node_id == (node_id))\
      {\
        (tmp_spec)->s.cls_node.node_id = DB_CLUSTER_NODE_LOCAL;\
      }\
      (tmp_spec) = (tmp_spec)->next;\
    }\
  }while(0)

#define REXEC_SET_BACK(local_xasl, tmp_spec, no_classes)\
  do {\
    while ((no_classes))\
    {\
      (tmp_spec)->s.cls_node.node_id = (local_xasl)->proc.update.node_ids[(no_classes) - 1];\
      (no_classes)--;\
      (tmp_spec) = (tmp_spec)->next;\
    }\
  }while(0)
static int
remote_generate_remote_xasl_by_host (THREAD_ENTRY * thread_p,
				     XASL_NODE ** target, XASL_NODE * src,
				     int node_id);
static int remote_generate_count_star_xasl (THREAD_ENTRY * thread_p,
					    XASL_NODE ** target,
					    XASL_NODE * src,
					    ACCESS_SPEC_TYPE * spec);
static SCAN_CODE remote_scan_next_scan (THREAD_ENTRY * thread_p,
					XASL_NODE * xasl,
					ACCESS_SPEC_TYPE * spec,
					int direction);
static SCAN_CODE remote_count_star_scan (THREAD_ENTRY * thread_p,
					 XASL_NODE * xasl,
					 ACCESS_SPEC_TYPE * spec,
					 int direction,
					 bool count_star_with_iscan_opt);
static int remote_execute_delete_query_by_host (THREAD_ENTRY * thread_p,
						XASL_NODE * local_xasl,
						const DB_VALUE * dbvals_p,
						int dbval_count,
						QUERY_FLAG flag,
						int remote_host,
						QFILE_LIST_ID ** list_id_p);
static int remote_execute_update_query_by_host (THREAD_ENTRY * thread_p,
						XASL_NODE * local_xasl,
						const DB_VALUE * dbvals_p,
						int dbval_count,
						QUERY_FLAG flag,
						int remote_host,
						QFILE_LIST_ID ** list_id_p);
static void remote_reorganize_local_delete_xasl (THREAD_ENTRY * thread_p,
						 XASL_NODE * local_xasl);
static void remote_reorganize_local_update_xasl (THREAD_ENTRY * thread_p,
						 XASL_NODE * local_xasl);

/*
 * scan_start_scan_proxy () - proxy for scan_start_scan
 *   return : the result of the scan operation.
 *   xasl (in): the XASL node carrying the scan spec 
 *   spec (in): the scan spec
 *
 * Note: If the spec is not remote, do the scan operation as
 *       before.
 *       Otherwise send request to the remote host and get
 *       the remote result.
 */
int
scan_start_scan_proxy (THREAD_ENTRY * thread_p,
		       XASL_NODE * xasl, ACCESS_SPEC_TYPE * spec)
{
  int error = NO_ERROR;

  if (spec == NULL)
    {
      return ER_FAILED;
    }

  if ((spec->type != TARGET_CLASS && spec->type != TARGET_CLASS_ATTR)
      || spec->s.cls_node.node_id == 0)
    {
      return scan_start_scan (thread_p, &spec->s_id);
    }

  return error;
}

/*
 * scan_next_scan_block_proxy () - proxy for scan_next_scan_block
 *   return : the result of the scan operation.
 *   xasl (in): the XASL node carrying the scan spec 
 *   spec (in): the scan spec
 *
 * Note: If the spec is not remote, do the scan operation as
 *       before.
 *       Otherwise send request to the remote host and get
 *       the remote result.
 */
SCAN_CODE
scan_next_scan_block_proxy (THREAD_ENTRY * thread_p, ACCESS_SPEC_TYPE * spec)
{
  SCAN_CODE retval = S_SUCCESS;

  if (spec == NULL)
    {
      return ER_FAILED;
    }

  if ((spec->type != TARGET_CLASS && spec->type != TARGET_CLASS_ATTR)
      || spec->s.cls_node.node_id == 0)
    {
      return scan_next_scan_block (thread_p, &spec->s_id);
    }

  if (XASL_IS_FLAGED (spec, XASL_SPEC_VISITED))
    {
      retval = S_END;
    }

  return retval;
}

/*
 * scan_end_scan_proxy () - proxy for qexec_end_scan
 *   xasl (in): the XASL node carrying the scan spec 
 *   spec (in): the scan spec
 *
 * Note: If the spec is not remote, do the scan operation as
 *       before.
 *       Otherwise send request to the remote host.
 */
void
scan_end_scan_proxy (THREAD_ENTRY * thread_p,
		     XASL_NODE * xasl, ACCESS_SPEC_TYPE * spec)
{
  if (spec == NULL)
    {
      return;
    }

  if ((spec->type != TARGET_CLASS && spec->type != TARGET_CLASS_ATTR)
      || spec->s.cls_node.node_id == 0)
    {
      qexec_end_scan (thread_p, spec);
      return;
    }
}

/*
 * qexec_close_scan_proxy () - proxy for qexec_close_scan
 *   xasl (in): the XASL node carrying the scan spec 
 *   spec (in): the scan spec
 *
 * Note: If the spec is not remote, do the scan operation as
 *       before.
 *       Otherwise send request to the remote host.
 */
void
qexec_close_scan_proxy (THREAD_ENTRY * thread_p,
			XASL_NODE * xasl, ACCESS_SPEC_TYPE * spec)
{
  if (spec == NULL)
    {
      return;
    }

  if ((spec->type != TARGET_CLASS && spec->type != TARGET_CLASS_ATTR)
      || spec->s.cls_node.node_id == 0)
    {
      qexec_close_scan (thread_p, spec);
      return;
    }

}

/*
 * scan_reset_scan_proxy () - proxy for scan_reset_scan_block
 *   return : the result of the scan operation.
 *   xasl (in): the XASL node carrying the scan spec 
 *   spec (in): the scan spec
 *
 * Note: If the spec is note remote, do the scan operation as
 *       before.
 *       Otherwise send request to the remote host and get
 *       the remote result.
 */
SCAN_CODE
scan_reset_scan_block_proxy (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{

  SCAN_CODE retval = S_SUCCESS;
  QFILE_LIST_ID *llsid_listid = NULL, *xasl_node_listid = NULL;
  ACCESS_SPEC_TYPE *spec = xasl->curr_spec;

  if (spec == NULL)
    {
      return ER_FAILED;
    }

  if ((spec->type != TARGET_CLASS && spec->type != TARGET_CLASS_ATTR)
      || spec->s.cls_node.node_id == 0)
    {
      return scan_reset_scan_block (thread_p, &spec->s_id);
    }

  return retval;
}

/*
 * scan_next_scan_proxy () - proxy for scan_next_scan
 *   return : the result scan code of the scan operation.
 *   xasl (in): the XASL node carrying the scan spec 
 *   spec (in): the scan spec
 *   direction (in): S_FORWARD of S_BACKWARD
 *   remote_scan_all (in): if scanning multiple records
 *
 * Note: If the spec is not remote, do the scan operation as
 *       before.
 *       Otherwise send request to the remote host and get
 *       the remote result.
 */
SCAN_CODE
scan_next_scan_proxy (THREAD_ENTRY * thread_p,
		      XASL_NODE * xasl,
		      ACCESS_SPEC_TYPE * spec, int direction,
		      bool count_star_with_iscan_opt)
{
  SCAN_CODE retval = S_ERROR;

  if (spec == NULL)
    {
      return retval;
    }

  if ((spec->type != TARGET_CLASS && spec->type != TARGET_CLASS_ATTR)
      || spec->s.cls_node.node_id == DB_CLUSTER_NODE_LOCAL)
    {
      retval = S_FORWARD == direction
	? scan_next_scan (thread_p, &spec->s_id)
	: scan_prev_scan (thread_p, &spec->s_id);
    }
  else
    {
      if (xasl->type == BUILDVALUE_PROC
	  && xasl->proc.buildvalue.agg_list
	  && xasl->proc.buildvalue.agg_list->function == PT_COUNT_STAR)
	{
	  retval =
	    remote_count_star_scan (thread_p, xasl, spec, direction,
				    count_star_with_iscan_opt);
	}
      else
	{
	  retval = remote_scan_next_scan (thread_p, xasl, spec, direction);
	}
    }

  return retval;
}

/* remote_scan_next_scan() : -
 * thread_p(in):
 * xasl(in/out):
 * spec(in):
 * direction(in):
 */
static SCAN_CODE
remote_scan_next_scan (THREAD_ENTRY * thread_p,
		       XASL_NODE * xasl,
		       ACCESS_SPEC_TYPE * spec, int direction)
{
  int size = 0, status, error;
  SCAN_CODE retval = S_ERROR;
  char *spec_stream = NULL;
  QUERY_EXEC_PARM *query_parm = QPARM_PTR (thread_p);
  XASL_NODE *remote_xasl = NULL;
  QFILE_LIST_ID *list_id = NULL;
  DB_VALUE *tmp_ptr = NULL;
  QUERY_FLAG remote_flag = QPARM_PTR (thread_p)->flag;
  ACCESS_SPEC_TYPE *tmp_spec = NULL;

  /* stage 1: remote scan and open list can. */
  if (spec->s_id.scan_id == NULL)
    {
      if (XASL_IS_FLAGED (spec, XASL_SPEC_VISITED))
	{
	  return S_END;
	}

      error =
	remote_generate_remote_xasl_by_host (thread_p, &remote_xasl, xasl,
					     spec->s.cls_node.node_id);
      if (NO_ERROR != error)
	{
	  goto end;
	}

      if (xasl->type == BUILDVALUE_PROC && xasl->val_list->valp == NULL)
	{
	  remote_xasl->val_list->valp =
	    db_private_alloc (thread_p, sizeof (QPROC_DB_VALUE_LIST));
	  remote_xasl->val_list->valp->val =
	    remote_xasl->outptr_list->valptrp->value.value.dbvalptr;
	  remote_xasl->val_list->valp->next = NULL;
	  remote_xasl->val_list->val_cnt = 1;
	}
      QEXEC_REMOTE_OUTPTR_LIST (remote_xasl, &tmp_ptr);

      error =
	xts_map_xasl_to_stream (thread_p, remote_xasl, &spec_stream, &size);
      if (NO_ERROR != error)
	{
	  goto end;
	}

      QEXEC_LOCAL_OUTPTR_LIST (remote_xasl, tmp_ptr);

      if (IS_ASYNC_EXEC_MODE (remote_flag))
	{
	  remote_flag &= ~ASYNC_EXEC;
	}

      error = remote_prepare_and_execute_query (thread_p,
						spec->s.cls_node.
						node_id, spec_stream,
						size,
						query_parm->
						xasl_state->vd.
						dbval_ptr,
						query_parm->
						xasl_state->vd.
						dbval_cnt, remote_flag,
						&list_id);
      if (error != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_REMOTE_SERVER_SCAN_FAIL, 1, spec->s.cls_node.node_id);
	  qmgr_set_query_error (thread_p, query_parm->xasl_state->query_id);

	  goto end;
	}

      list_id->remote_node_id = spec->s.cls_node.node_id;

      spec->s_id.scan_id =
	db_private_alloc (thread_p, sizeof (QFILE_LIST_SCAN_ID));
      if (spec->s_id.scan_id == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (QFILE_LIST_SCAN_ID));
	  goto end;
	}

      if (qfile_open_list_scan (list_id, spec->s_id.scan_id) != NO_ERROR)
	{
	  goto end;
	}

      XASL_SET_FLAG (spec, XASL_SPEC_VISITED);
    }

  /* stage 2 : do next scan */
  retval = qfile_to_val_list (thread_p, spec->s_id.scan_id, xasl->val_list);

  /* stage 3 : close scan and send NET_SERVER_QM_QUERY_END to service node */
  if (retval == S_SUCCESS)
    {
      return retval;
    }
  else if (retval == S_END)
    {
      remote_end_query (thread_p,
			spec->s_id.scan_id->list_id.remote_node_id,
			spec->s_id.scan_id->list_id.query_id, &status);
      if (NO_ERROR != status)
	{
	  retval = S_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_REMOTE_SERVER_END_FAIL,
		  1, spec->s.cls_node.node_id);
	}
    }

end:

  if (spec->s_id.scan_id)
    {
      qfile_close_scan (thread_p, spec->s_id.scan_id);
      db_private_free_and_init (thread_p, spec->s_id.scan_id);
    }

  if (remote_xasl)
    {
      db_private_free_and_init (thread_p, remote_xasl->list_id);

      while (remote_xasl->spec_list)
	{
	  tmp_spec = remote_xasl->spec_list->next;
	  db_private_free_and_init (thread_p, remote_xasl->spec_list);
	  remote_xasl->spec_list = tmp_spec;
	}
    }

  db_private_free_and_init (thread_p, remote_xasl);
  db_private_free_and_init (thread_p, spec_stream);

  if (list_id)
    {
      qfile_free_list_id (list_id);
      list_id = NULL;
    }

  return retval;
}

/* remote_count_star_scan() : -
 * thread_p(in):
 * xasl(in/out):
 * spec(in):
 * direction(in):
 */
static SCAN_CODE
remote_count_star_scan (THREAD_ENTRY * thread_p,
			XASL_NODE * xasl,
			ACCESS_SPEC_TYPE * spec, int direction,
			bool count_star_with_iscan_opt)
{
  int size = 0, status, error;
  SCAN_CODE retval = S_ERROR;
  char *spec_stream = NULL;
  QUERY_EXEC_PARM *query_parm = QPARM_PTR (thread_p);
  XASL_NODE *remote_xasl = NULL;
  QFILE_LIST_ID *list_id = NULL;
  DB_VALUE *tmp_ptr = NULL;
  QUERY_FLAG remote_flag = QPARM_PTR (thread_p)->flag;

  /* stage 1: remote scan and open list can. */
  if (!XASL_IS_FLAGED (spec, XASL_SPEC_VISITED))
    {
      error =
	remote_generate_count_star_xasl (thread_p, &remote_xasl, xasl, spec);
      if (NO_ERROR != error)
	{
	  goto end;
	}

      error =
	xts_map_xasl_to_stream (thread_p, remote_xasl, &spec_stream, &size);
      if (NO_ERROR != error)
	{
	  goto end;
	}

      if (IS_ASYNC_EXEC_MODE (remote_flag))
	{
	  remote_flag &= ~ASYNC_EXEC;
	}

      error = remote_prepare_and_execute_query (thread_p,
						spec->s.cls_node.
						node_id, spec_stream,
						size,
						query_parm->
						xasl_state->vd.
						dbval_ptr,
						query_parm->
						xasl_state->vd.
						dbval_cnt, remote_flag,
						&list_id);
      if (error != NO_ERROR)
	{
	  goto end;
	}

      list_id->remote_node_id = spec->s.cls_node.node_id;

      spec->s_id.scan_id =
	db_private_alloc (thread_p, sizeof (QFILE_LIST_SCAN_ID));
      if (spec->s_id.scan_id == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (QFILE_LIST_SCAN_ID));
	  goto end;
	}

      if (qfile_open_list_scan (list_id, spec->s_id.scan_id) != NO_ERROR)
	{
	  goto end;
	}

      XASL_SET_FLAG (spec, XASL_SPEC_VISITED);

      retval =
	qfile_to_val_list (thread_p, spec->s_id.scan_id, xasl->val_list);
      if (retval == S_ERROR)
	{
	  goto end;
	}
      spec->s_id.scan_id->position = S_AFTER;
      spec->s_id.scan_id->curr_vpid.pageid = NULL_PAGEID;

    }

  if (db_get_int (xasl->val_list->valp->val) > 0)
    {
      if (count_star_with_iscan_opt)
	{
	  (&xasl->curr_spec->s_id)->s.isid.oid_list.oid_cnt =
	    db_get_int (xasl->val_list->valp->val);
	  db_make_int (xasl->val_list->valp->val, 0);
	}
      else
	{
	  db_make_int (xasl->val_list->valp->val,
		       db_get_int (xasl->val_list->valp->val) - 1);
	}

      return S_SUCCESS;
    }
  else
    {
      retval = S_END;
      remote_end_query (thread_p,
			spec->s_id.scan_id->list_id.remote_node_id,
			spec->s_id.scan_id->list_id.query_id, &status);
      if (NO_ERROR != status)
	{
	  retval = S_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_REMOTE_SERVER_END_FAIL,
		  1, spec->s.cls_node.node_id);
	}
    }

end:


  if (spec->s_id.scan_id)
    {
      if (spec->s_id.scan_id->curr_pgptr)
	{
	  free_and_init (spec->s_id.scan_id->curr_pgptr);
	}
      qfile_close_scan (thread_p, spec->s_id.scan_id);
      db_private_free_and_init (thread_p, spec->s_id.scan_id);
    }

  if (remote_xasl)
    {
      db_private_free_and_init (thread_p, remote_xasl->list_id);
      db_private_free_and_init (thread_p, remote_xasl->spec_list);
    }

  db_private_free_and_init (thread_p, remote_xasl);
  db_private_free_and_init (thread_p, spec_stream);

  if (list_id)
    {
      qfile_free_list_id (list_id);
      list_id = NULL;
    }

  return retval;
}


/*
 * remote_generate_remote_xasl () - from src xasl generate new xasl 
 *                           for remote table
 * return: sub_xasl
 * thread_p(in): current thread
 * target(out): remote xasl
 * src(in): local xasl
 * spec(in): current spec
 */

static int
remote_generate_remote_xasl_by_host (THREAD_ENTRY * thread_p,
				     XASL_NODE ** target, XASL_NODE * src,
				     int node_id)
{
  int rc = ER_FAILED;
  ACCESS_SPEC_TYPE *src_spec = NULL, *tar_spec = NULL, *tmp_spec = NULL;

  if (src == NULL)
    {
      return rc;
    }

  *target = (XASL_NODE *) db_private_alloc (thread_p, sizeof (XASL_NODE));
  if ((*target) == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (XASL_NODE));
      return rc;
    }

  memcpy (*target, src, sizeof (XASL_NODE));

  (*target)->n_oid_list = 0;

  if (src->type == BUILDVALUE_PROC
      && src->proc.buildvalue.agg_list->function == PT_COUNT_STAR)
    {
      /* nothing */
      /* temp code */
    }
  else
    {
      (*target)->outptr_list = (*target)->remote_outptr_list;
      (*target)->remote_outptr_list = NULL;
    }

  (*target)->spec_list = NULL;
  (*target)->curr_spec = NULL;
  (*target)->scan_ptr = NULL;
  (*target)->aptr_list = NULL;
  (*target)->next = NULL;
  (*target)->after_iscan_list = NULL;
  (*target)->ordbynum_flag = 0;
  (*target)->ordbynum_pred = NULL;
  (*target)->ordbynum_val = NULL;
  (*target)->orderby_list = NULL;
  (*target)->instnum_pred = NULL;
  (*target)->instnum_val = NULL;

  (*target)->list_id =
    (QFILE_LIST_ID *) db_private_alloc (thread_p, sizeof (QFILE_LIST_ID));
  if ((*target)->list_id == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (XASL_NODE));
      return rc;
    }

  QFILE_CLEAR_LIST_ID ((*target)->list_id);

  src_spec = src->spec_list;

  while (src_spec)
    {
      if (src_spec->s.cls_node.node_id == node_id)
	{
	  tar_spec =
	    (ACCESS_SPEC_TYPE *) db_private_alloc (thread_p,
						   sizeof (ACCESS_SPEC_TYPE));
	  if (tar_spec == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (ACCESS_SPEC_TYPE));
	      return rc;
	    }

	  memcpy (tar_spec, src_spec, sizeof (ACCESS_SPEC_TYPE));
	  tar_spec->s.cls_node.node_id = DB_CLUSTER_NODE_LOCAL;
	  tar_spec->next = NULL;

	  if (!(*target)->spec_list)
	    {
	      (*target)->spec_list = tar_spec;
	    }
	  else
	    {
	      tmp_spec = (*target)->spec_list;
	      while (tmp_spec->next)
		{
		  tmp_spec = tmp_spec->next;
		}
	      tmp_spec->next = tar_spec;
	    }

	  XASL_SET_FLAG (src_spec, XASL_SPEC_VISITED);
	}
      src_spec = src_spec->next;
    }

  if ((*target)->type == BUILDVALUE_PROC)
    {
      BUILDLIST_PROC_NODE *bl_proc = &((*target)->proc.buildlist);

      (*target)->type = BUILDLIST_PROC;

      memset (bl_proc, 0, sizeof (BUILDLIST_PROC_NODE));

    }

  rc = NO_ERROR;

  return rc;
}

/*
 * remote_generate_remote_xasl () - from src xasl generate new xasl 
 *                           for remote table
 * return: sub_xasl
 * thread_p(in): current thread
 * target(out): remote xasl
 * src(in): local xasl
 * spec(in): current spec
 */

static int
remote_generate_count_star_xasl (THREAD_ENTRY * thread_p, XASL_NODE ** target,
				 XASL_NODE * src, ACCESS_SPEC_TYPE * spec)
{
  int rc = ER_FAILED;

  if (src == NULL)
    {
      return rc;
    }

  *target = (XASL_NODE *) db_private_alloc (thread_p, sizeof (XASL_NODE));
  if ((*target) == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (XASL_NODE));
      return rc;
    }

  memcpy (*target, src, sizeof (XASL_NODE));

  (*target)->n_oid_list = 0;
  (*target)->outptr_list = (*target)->outptr_list;
  (*target)->remote_outptr_list = NULL;
  (*target)->spec_list = NULL;
  (*target)->curr_spec = NULL;
  (*target)->scan_ptr = NULL;
  (*target)->aptr_list = NULL;
  (*target)->next = NULL;
  (*target)->after_iscan_list = NULL;
  (*target)->ordbynum_flag = 0;
  (*target)->ordbynum_pred = NULL;
  (*target)->ordbynum_val = NULL;
  (*target)->orderby_list = NULL;
  (*target)->instnum_pred = NULL;
  (*target)->instnum_val = NULL;

  (*target)->list_id =
    (QFILE_LIST_ID *) db_private_alloc (thread_p, sizeof (QFILE_LIST_ID));
  if ((*target)->list_id == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (XASL_NODE));
      return rc;
    }

  QFILE_CLEAR_LIST_ID ((*target)->list_id);

  (*target)->spec_list =
    (ACCESS_SPEC_TYPE *) db_private_alloc (thread_p,
					   sizeof (ACCESS_SPEC_TYPE));
  if ((*target)->spec_list == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (ACCESS_SPEC_TYPE));
      return rc;
    }

  memcpy ((*target)->spec_list, spec, sizeof (ACCESS_SPEC_TYPE));
  (*target)->spec_list->s.cls_node.node_id = DB_CLUSTER_NODE_LOCAL;
  (*target)->spec_list->next = NULL;

  rc = NO_ERROR;

  return rc;
}



/* remote_execute_delete_query_by_host() - reorganize sub-XASL for specified host to execute
* return: rc
*
* local_xasl(in): original XASL
* db_vals_p(in):
* dbval_count(in):
* flag (in):
* remote_host (in): specified host
*/
static int
remote_execute_delete_query_by_host (THREAD_ENTRY * thread_p,
				     XASL_NODE * local_xasl,
				     const DB_VALUE * dbvals_p,
				     int dbval_count,
				     QUERY_FLAG flag,
				     int remote_host,
				     QFILE_LIST_ID ** list_id_p)
{
  int rc = ER_FAILED;
  int no_classes = 0;
  int i, j, size, status = ER_FAILED;
  XASL_NODE *remote_xasl = NULL;
  ACCESS_SPEC_TYPE *remote_spec_list = NULL, *local_spec_list = NULL;
  ACCESS_SPEC_TYPE *remote_spec = NULL, *tmp_spec = NULL;
  OID *remote_oid = NULL;
  HFID *remote_hfid = NULL;
  int remote_spec_cnt = 0;
  int *remote_node_ids = NULL;
  char *remote_stream = NULL;
  int node_id;

  remote_xasl = (XASL_NODE *) db_private_alloc (thread_p, sizeof (XASL_NODE));
  if (remote_xasl == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (XASL_NODE));
      goto end;
    }

  memcpy (remote_xasl, local_xasl, sizeof (XASL_NODE));

  remote_xasl->proc.delete_.no_classes = 0;

  /* save original XASL spec list */
  local_spec_list = local_xasl->aptr_list->spec_list;

  no_classes = local_xasl->proc.delete_.no_classes;

  remote_oid = (OID *) db_private_alloc (thread_p, no_classes * sizeof (OID));
  if (remote_oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (OID));
      goto end;
    }

  remote_hfid =
    (HFID *) db_private_alloc (thread_p, no_classes * sizeof (HFID));
  if (remote_hfid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (HFID));
      goto end;
    }

  remote_node_ids =
    (int *) db_private_alloc (thread_p, no_classes * sizeof (int));
  if (remote_node_ids == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (int));
      goto end;
    }

  memset (remote_node_ids, 0, no_classes * sizeof (int));

  remote_xasl->proc.delete_.class_oid = remote_oid;
  remote_xasl->proc.delete_.class_hfid = remote_hfid;

  /* step1: find out spec and oid/hfid to reorgnize sub-XASL for specified host */
  for (i = 0; i < no_classes; i++)
    {
      node_id = local_xasl->proc.delete_.node_ids[i];

      if (node_id != remote_host)
	{
	  continue;
	}

      /* for remote spec */
      tmp_spec = local_spec_list;
      j = no_classes - i - 1;

      while (j--)
	{
	  tmp_spec = tmp_spec->next;
	}

      remote_spec =
	(ACCESS_SPEC_TYPE *) db_private_alloc (thread_p,
					       sizeof (ACCESS_SPEC_TYPE));
      if (remote_spec == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (ACCESS_SPEC_TYPE));
	  goto end;
	}
      memcpy (remote_spec, tmp_spec, sizeof (ACCESS_SPEC_TYPE));

      tmp_spec = remote_spec_list;
      if (remote_spec_list)
	{
	  j = remote_spec_cnt - 1;
	  while (j--)
	    {
	      tmp_spec = tmp_spec->next;
	    }
	  tmp_spec->next = remote_spec;
	}

      remote_spec->s.cls_node.node_id = 0;

      /* for remote delete proc */
      remote_oid[remote_spec_cnt] = local_xasl->proc.delete_.class_oid[i];
      remote_hfid[remote_spec_cnt] = local_xasl->proc.delete_.class_hfid[i];

      remote_spec_cnt++;
      remote_xasl->proc.delete_.no_classes++;

      if (remote_spec_cnt == 1)
	{
	  remote_spec_list = remote_spec;
	}
    }

  i = remote_spec_cnt - 1;
  tmp_spec = remote_spec_list;

  while (i--)
    {
      tmp_spec = tmp_spec->next;
    }
  tmp_spec->next = NULL;

  remote_xasl->proc.delete_.no_classes = remote_spec_cnt;
  remote_xasl->proc.delete_.no_logging = local_xasl->proc.delete_.no_logging;
  remote_xasl->proc.delete_.node_ids = remote_node_ids;
  remote_xasl->proc.delete_.release_lock =
    local_xasl->proc.delete_.release_lock;
  remote_xasl->proc.delete_.waitsecs = local_xasl->proc.delete_.waitsecs;

  remote_xasl->aptr_list->spec_list = remote_spec_list;

  /* step 2: send to remote host */
  if (NO_ERROR !=
      xts_map_xasl_to_stream (thread_p, remote_xasl, &remote_stream, &size))
    {
      goto end;
    }

  if (NO_ERROR != remote_prepare_and_execute_query (thread_p,
						    remote_host,
						    remote_stream,
						    size,
						    dbvals_p,
						    dbval_count,
						    flag, list_id_p))
    {
      goto end;
    }

  /* step3: end query for remote host */
  remote_end_query (thread_p, remote_host, (*list_id_p)->query_id, &status);

  if (NO_ERROR != status)
    {
      rc = ER_FAILED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_REMOTE_SERVER_END_FAIL, 1,
	      node_id);
      goto end;
    }

  local_xasl->aptr_list->spec_list = local_spec_list;

  rc = NO_ERROR;

end:

  db_private_free_and_init (thread_p, remote_stream);
  db_private_free_and_init (thread_p, remote_xasl);

  while (remote_spec_list)
    {
      remote_spec = remote_spec_list->next;
      db_private_free_and_init (thread_p, remote_spec_list);
      remote_spec_list = remote_spec;
    }

  db_private_free_and_init (thread_p, remote_oid);
  db_private_free_and_init (thread_p, remote_hfid);
  db_private_free_and_init (thread_p, remote_node_ids);

  return rc;
}

/* remote_execute_update_query_by_host() - reorganize sub-XASL for specified host to execute
* return: rc
*
* local_xasl(in): original XASL
* db_vals_p(in):
* dbval_count(in):
* flag (in):
* remote_host (in): specified host
*/
static int
remote_execute_update_query_by_host (THREAD_ENTRY * thread_p,
				     XASL_NODE * local_xasl,
				     const DB_VALUE * dbvals_p,
				     int dbval_count,
				     QUERY_FLAG flag,
				     int remote_host,
				     QFILE_LIST_ID ** list_id_p)
{
  int rc = ER_FAILED;
  int no_classes = 0;
  int i, j, size, status;
  XASL_NODE *remote_xasl = NULL;
  ACCESS_SPEC_TYPE *remote_spec_list = NULL, *local_spec_list = NULL;
  ACCESS_SPEC_TYPE *remote_spec = NULL, *tmp_spec = NULL;
  XASL_PARTS_INFO **remote_parts = NULL;
  OID *remote_oid = NULL;
  HFID *remote_hfid = NULL;
  int remote_spec_cnt = 0;
  int *remote_node_ids = NULL;
  char *remote_stream = NULL;
  int node_id;

  remote_xasl = (XASL_NODE *) db_private_alloc (thread_p, sizeof (XASL_NODE));
  if (remote_xasl == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (XASL_NODE));
      goto end;
    }

  memcpy (remote_xasl, local_xasl, sizeof (XASL_NODE));

  remote_xasl->proc.update.no_classes = 0;

  /* save original XASL spec list */
  local_spec_list = local_xasl->aptr_list->spec_list;

  no_classes = local_xasl->proc.update.no_classes;

  remote_oid = (OID *) db_private_alloc (thread_p, no_classes * sizeof (OID));
  remote_hfid =
    (HFID *) db_private_alloc (thread_p, no_classes * sizeof (HFID));
  remote_node_ids =
    (int *) db_private_alloc (thread_p, no_classes * sizeof (int));
  remote_parts =
    (XASL_PARTS_INFO **) db_private_alloc (thread_p,
					   (no_classes -
					    1) * sizeof (XASL_PARTS_INFO *));

  memset (remote_node_ids, 0, no_classes * sizeof (int));

  remote_xasl->proc.update.class_oid = remote_oid;
  remote_xasl->proc.update.class_hfid = remote_hfid;

  /* step1: find out spec and oid/hfid to reorgnize sub-XASL for specified host */
  for (i = 0; i < no_classes; i++)
    {
      node_id = local_xasl->proc.update.node_ids[i];

      if (node_id != remote_host)
	{
	  continue;
	}

      /* for remote spec */
      tmp_spec = local_spec_list;
      j = no_classes - i - 1;

      while (j--)
	{
	  tmp_spec = tmp_spec->next;
	}

      remote_spec =
	(ACCESS_SPEC_TYPE *) db_private_alloc (thread_p,
					       sizeof (ACCESS_SPEC_TYPE));
      if (remote_spec == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (ACCESS_SPEC_TYPE));
	  goto end;
	}
      memcpy (remote_spec, tmp_spec, sizeof (ACCESS_SPEC_TYPE));

      tmp_spec = remote_spec_list;
      if (remote_spec_list)
	{
	  j = remote_spec_cnt - 1;
	  while (j--)
	    {
	      tmp_spec = tmp_spec->next;
	    }
	  tmp_spec->next = remote_spec;
	}

      remote_spec->s.cls_node.node_id = DB_CLUSTER_NODE_LOCAL;

      /* for remote delete proc */
      remote_oid[remote_spec_cnt] = local_xasl->proc.update.class_oid[i];
      remote_hfid[remote_spec_cnt] = local_xasl->proc.update.class_hfid[i];

      if ((*local_xasl->proc.update.partition) != NULL)
	{
	  remote_parts[remote_spec_cnt] =
	    (*local_xasl->proc.update.partition)->parts[i];
	}

      remote_spec_cnt++;
      remote_xasl->proc.update.no_classes++;

      if (remote_spec_cnt == 1)
	{
	  remote_spec_list = remote_spec;
	}
    }

  i = remote_spec_cnt - 1;
  tmp_spec = remote_spec_list;

  while (i--)
    {
      tmp_spec = tmp_spec->next;
    }
  tmp_spec->next = NULL;

  if ((*local_xasl->proc.update.partition) != NULL)
    {
      (*local_xasl->proc.update.partition)->no_parts = remote_spec_cnt;
    }

  remote_xasl->proc.update.no_classes = remote_spec_cnt;
  remote_xasl->proc.update.no_logging = local_xasl->proc.update.no_logging;
  remote_xasl->proc.update.node_ids = remote_node_ids;
  remote_xasl->proc.update.release_lock =
    local_xasl->proc.update.release_lock;
  remote_xasl->proc.update.waitsecs = local_xasl->proc.update.waitsecs;

  remote_xasl->aptr_list->spec_list = remote_spec_list;

  /* step 2: send to remote host */
  if (NO_ERROR !=
      xts_map_xasl_to_stream (thread_p, remote_xasl, &remote_stream, &size))
    {
      goto end;
    }

  if (NO_ERROR != remote_prepare_and_execute_query (thread_p,
						    remote_host,
						    remote_stream,
						    size,
						    dbvals_p,
						    dbval_count,
						    flag, list_id_p))
    {
      goto end;
    }

  if (!(*list_id_p))
    {
      goto end;
    }
  /* step3: end query for remote host */
  remote_end_query (thread_p, remote_host, (*list_id_p)->query_id, &status);

  if (NO_ERROR != status)
    {
      rc = ER_FAILED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_REMOTE_SERVER_END_FAIL, 1,
	      node_id);
      goto end;
    }

  local_xasl->aptr_list->spec_list = local_spec_list;

  rc = NO_ERROR;

end:

  db_private_free_and_init (thread_p, remote_stream);
  db_private_free_and_init (thread_p, remote_xasl);

  while (remote_spec_list)
    {
      remote_spec = remote_spec_list->next;
      db_private_free_and_init (thread_p, remote_spec_list);
      remote_spec_list = remote_spec;
    }

  db_private_free_and_init (thread_p, remote_oid);
  db_private_free_and_init (thread_p, remote_hfid);
  db_private_free_and_init (thread_p, remote_node_ids);
  db_private_free_and_init (thread_p, remote_parts);

  return rc;
}

/*
* remote_reorganize_local_delete_xasl() - reorgnize local XASL for local execution
* 
* thread_p(in):
* local_xasl(in):
*/
static void
remote_reorganize_local_delete_xasl (THREAD_ENTRY * thread_p,
				     XASL_NODE * local_xasl)
{
  int i, j, node_id;
  int no_classes;
  ACCESS_SPEC_TYPE *tmp_spec = NULL;

  no_classes = local_xasl->proc.delete_.no_classes;

  for (i = 0; i < no_classes; i++)
    {
      node_id = local_xasl->proc.delete_.node_ids[i];

      if (node_id == 0)
	{
	  continue;
	}

      /* for delete proc */
      j = i;
      while (j != no_classes)
	{
	  local_xasl->proc.delete_.class_oid[j] =
	    local_xasl->proc.delete_.class_oid[j + 1];
	  local_xasl->proc.delete_.class_hfid[j] =
	    local_xasl->proc.delete_.class_hfid[j + 1];
	  local_xasl->proc.delete_.node_ids[j] =
	    local_xasl->proc.delete_.node_ids[j + 1];
	  j++;
	}

      local_xasl->proc.delete_.no_classes--;

      /* for delete spec */
      j = no_classes - i - 2;
      tmp_spec = local_xasl->aptr_list->spec_list;
      while (j > 0)
	{
	  tmp_spec = tmp_spec->next;
	  j--;
	}

      if (tmp_spec == local_xasl->aptr_list->spec_list)
	{
	  local_xasl->aptr_list->spec_list =
	    local_xasl->aptr_list->spec_list->next;
	}
      else
	{
	  if (tmp_spec->next)
	    {
	      tmp_spec->next = tmp_spec->next->next;
	    }
	}

      no_classes = local_xasl->proc.delete_.no_classes;
      i--;
    }

}

/*
* remote_reorganize_local_xasl() - reorgnize local XASL for local execution
* 
* thread_p(in):
* local_xasl(in):
*/
static void
remote_reorganize_local_update_xasl (THREAD_ENTRY * thread_p,
				     XASL_NODE * local_xasl)
{
  int i, j, node_id;
  int no_classes;
  ACCESS_SPEC_TYPE *tmp_spec = NULL;
  struct xasl_partition_info *tmp_partition = NULL;

  no_classes = local_xasl->proc.update.no_classes;
  i = j = 0;

  for (i = 0, j = 0; i < no_classes; i++, j++)
    {
      node_id = local_xasl->proc.update.node_ids[i];

      if (node_id == DB_CLUSTER_NODE_LOCAL)
	{
	  continue;
	}

      while (local_xasl->proc.update.node_ids[j] != DB_CLUSTER_NODE_LOCAL
	     && j < no_classes)
	{
	  j++;
	  local_xasl->proc.update.no_classes--;
	  if (*local_xasl->proc.update.partition)
	    {
	      (*local_xasl->proc.update.partition)->no_parts--;
	    }
	}

      if (j == no_classes)
	{
	  break;
	}

      local_xasl->proc.update.class_oid[i] =
	local_xasl->proc.update.class_oid[j];
      local_xasl->proc.update.class_hfid[i] =
	local_xasl->proc.update.class_hfid[j];
      local_xasl->proc.update.node_ids[i] =
	local_xasl->proc.update.node_ids[j];

      if ((*local_xasl->proc.update.partition) != NULL)
	{
	  (*local_xasl->proc.update.partition)->parts[i - 1] =
	    (*local_xasl->proc.update.partition)->parts[j - 1];
	}
    }

  tmp_spec = local_xasl->aptr_list->spec_list;
  while (tmp_spec && tmp_spec->s.cls_node.node_id != DB_CLUSTER_NODE_LOCAL)
    {
      tmp_spec = tmp_spec->next;
    }
  if (tmp_spec)
    {
      local_xasl->aptr_list->spec_list = tmp_spec;
    }

  tmp_spec = local_xasl->aptr_list->spec_list;
  while (tmp_spec->next
	 && tmp_spec->next->s.cls_node.node_id != DB_CLUSTER_NODE_LOCAL)
    {
      tmp_spec->next = tmp_spec->next->next;
      if (tmp_spec->next->s.cls_node.node_id == DB_CLUSTER_NODE_LOCAL)
	{
	  tmp_spec = tmp_spec->next;
	}
    }
}

/*
* remote_xqmgr_execute_delete_query() - split original XASL to each remote host sub XASL for execution
* return:
* 
* thread_p(in):
* query_id(in):
* local_xasl(in): original XASL
* dbvals_p(in):
* dbval_count(in):
* flag(in):
* remote_list_id_p(out): remote execution result file
*/
int
remote_xqmgr_execute_delete_query (THREAD_ENTRY * thread_p,
				   QUERY_ID query_id,
				   XASL_NODE * local_xasl,
				   const DB_VALUE * dbvals_p,
				   int dbval_count,
				   QFILE_LIST_ID ** remote_list_id_p)
{
  int rc = ER_FAILED;
  int i;
  ACCESS_SPEC_TYPE *tmp_spec = NULL;
  int node_id, no_classes;
  QFILE_LIST_ID *list_id_p = NULL;
  QUERY_FLAG flag = QPARM_PTR (thread_p)->flag;

  no_classes = local_xasl->proc.delete_.no_classes;

  /* for each host send sub-XASL to execute */
  for (i = 0; i < no_classes; i++)
    {
      node_id = local_xasl->proc.delete_.node_ids[i];

      if (node_id == 0)
	{
	  continue;
	}

      tmp_spec = local_xasl->aptr_list->spec_list;
      while (tmp_spec)
	{
	  if (tmp_spec->s.cls_node.node_id == node_id)
	    {
	      if (NO_ERROR != remote_execute_delete_query_by_host (thread_p,
								   local_xasl,
								   dbvals_p,
								   dbval_count,
								   flag,
								   node_id,
								   &list_id_p))
		{
		  goto end;
		}

	      if ((*remote_list_id_p) == NULL)
		{
		  (*remote_list_id_p) =
		    qfile_open_list (thread_p, &list_id_p->type_list, NULL,
				     query_id, QFILE_FLAG_ALL);

		  if ((*remote_list_id_p) == NULL)
		    {
		      goto end;
		    }
		}
	      (*remote_list_id_p)->tuple_cnt += list_id_p->tuple_cnt;

	      /* free QFILE_LIST_ID */
	      if (list_id_p)
		{
		  qfile_free_list_id (list_id_p);
		  list_id_p = NULL;
		}

	      break;
	    }
	  tmp_spec = tmp_spec->next;
	}

      tmp_spec = local_xasl->aptr_list->spec_list;

      while (tmp_spec)
	{
	  if (tmp_spec->s.cls_node.node_id == node_id)
	    {
	      tmp_spec->s.cls_node.node_id = 0;
	    }
	  tmp_spec = tmp_spec->next;
	}

    }

  /* reorganize XASL for local execution */
  remote_reorganize_local_delete_xasl (thread_p, local_xasl);

  rc = NO_ERROR;

end:
  /* free QFILE_LIST_ID */
  if (list_id_p)
    {
      qfile_free_list_id (list_id_p);
      list_id_p = NULL;
    }
  return rc;
}

/*
* remote_xqmgr_execute_update_query() - split original XASL to each remote host sub XASL for execution
* return:
* 
* thread_p(in):
* query_id(in):
* local_xasl(in): original XASL
* dbvals_p(in):
* dbval_count(in):
* flag(in):
* remote_list_id_p(out): remote execution result file
*/
int
remote_xqmgr_execute_update_query (THREAD_ENTRY * thread_p,
				   QUERY_ID query_id,
				   XASL_NODE * local_xasl,
				   const DB_VALUE * dbvals_p,
				   int dbval_count,
				   QFILE_LIST_ID ** remote_list_id_p)
{
  int rc = ER_FAILED;
  int i;
  ACCESS_SPEC_TYPE *tmp_spec = NULL;
  int node_id, no_classes;
  QFILE_LIST_ID *list_id_p = NULL;
  QUERY_FLAG flag = QPARM_PTR (thread_p)->flag;

  no_classes = local_xasl->proc.update.no_classes;

  /* for each host send sub-XASL to execute */
  for (i = 0; i < no_classes; i++)
    {
      node_id = local_xasl->proc.update.node_ids[i];

      if (node_id == DB_CLUSTER_NODE_LOCAL)
	{
	  continue;
	}

      tmp_spec = local_xasl->aptr_list->spec_list;
      while (tmp_spec)
	{
	  if (tmp_spec->s.cls_node.node_id == node_id)
	    {
	      if (NO_ERROR != remote_execute_update_query_by_host (thread_p,
								   local_xasl,
								   dbvals_p,
								   dbval_count,
								   flag,
								   node_id,
								   &list_id_p))
		{
		  goto end;
		}

	      if ((*remote_list_id_p) == NULL)
		{
		  (*remote_list_id_p) =
		    qfile_open_list (thread_p, &list_id_p->type_list, NULL,
				     query_id, QFILE_FLAG_ALL);

		  if ((*remote_list_id_p) == NULL)
		    {
		      goto end;
		    }
		}
	      (*remote_list_id_p)->tuple_cnt += list_id_p->tuple_cnt;

	      /* free QFILE_LIST_ID */
	      if (list_id_p)
		{
		  qfile_free_list_id (list_id_p);
		  list_id_p = NULL;
		}

	      break;
	    }
	  tmp_spec = tmp_spec->next;
	}

      tmp_spec = local_xasl->aptr_list->spec_list;

      REXEC_SET_ZERO_ON_SPEC (tmp_spec, node_id);
    }

  tmp_spec = local_xasl->aptr_list->spec_list;
  no_classes = local_xasl->proc.update.no_classes;

  REXEC_SET_BACK (local_xasl, tmp_spec, no_classes);

  /* reorganize XASL for local execution */
  remote_reorganize_local_update_xasl (thread_p, local_xasl);

  rc = NO_ERROR;

end:
  /* free QFILE_LIST_ID */
  if (list_id_p)
    {
      qfile_free_list_id (list_id_p);
      list_id_p = NULL;
    }
  return rc;
}

/*
 * heap_get_num_objects_proxy() - Count the number of local/remote objects
 *   return: number of records or -1 in case of an error
 *   thread_p(in): 
 *   node_id(in): specified node id
 *   hfid(in): Object heap file identifier
 *   npages(in):
 *   nobjs(in):
 *   avg_length(in):
 *
 */
int
heap_get_num_objects_proxy (THREAD_ENTRY * thread_p, int node_id,
			    const HFID * hfid_p, int *npages, int *nobjs,
			    int *avg_length)
{
  if (node_id == 0)
    {
      if (heap_get_num_objects (thread_p, hfid_p, npages, nobjs, avg_length) <
	  0)
	{
	  return ER_FAILED;
	}
    }
  else
    {
      if (remote_get_num_objects
	  (thread_p, node_id, hfid_p, npages, nobjs, avg_length) < 0)
	{
	  return ER_FAILED;
	}
    }

  return *nobjs;
}
