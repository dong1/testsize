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
 * transaction_cl.c -
 */

#ident "$Id$"

#include "config.h"

#if !defined(WINDOWS)
#include <unistd.h>
#else /* !WINDOWS */
#include <process.h>
#endif /* !WINDOWS */
#include <stdio.h>
#if !defined(WINDOWS)
#include <sys/param.h>
#endif
#if defined(SOLARIS)
/* for MAXHOSTNAMELEN */
#include <netdb.h>
#endif

#include "dbi.h"
#include "misc_string.h"
#include "transaction_cl.h"
#include "memory_alloc.h"
#include "locator_cl.h"
#include "work_space.h"
#include "server_interface.h"
#include "log_comm.h"
#include "recovery_cl.h"
#include "db_query.h"
#include "boot_cl.h"
#include "virtual_object.h"
#include "schema_manager.h"
#include "trigger_manager.h"
#include "system_parameter.h"
#include "dbdef.h"
#include "db.h"			/* for db_Connect_status */
#include "porting.h"
#include "network_interface_cl.h"
#include "cluster_config.h"

#if defined(WINDOWS)
#include "wintcp.h"
#endif /* WINDOWS */

int tm_Tran_index = NULL_TRAN_INDEX;
TRAN_ISOLATION tm_Tran_isolation = TRAN_DEFAULT_ISOLATION;
bool tm_Tran_async_ws = false;
int tm_Tran_waitsecs = TRAN_LOCK_INFINITE_WAIT;
int tm_Tran_ID = -1;

/* this is a local list of user-defined savepoints.  It may be updated upon
 * the following calls:
 *    tran_savepoint()		-> tm_add_savepoint()
 *    tran_commit()		-> tran_free_savepoint_list()
 *    tran_abort()		-> tran_free_savepoint_list()
 *    tran_abort_upto_savepoint() -> tm_free_list_upto_savepoint()
 */
static DB_NAMELIST *user_savepoint_list = NULL;

static int tran_add_savepoint (const char *savept_name);
static void tran_free_list_upto_savepoint (const char *savept_name);

/*
 * tran_cache_tran_settings - Cache transaction settings
 *
 * return:
 *
 *   tran_index(in): Transaction index assigned to client
 *   lock_timeout(in): Transaction lock wait assigned to client transaction
 *   tran_isolation(in): Transaction isolation assigned to client transactions
 *
 * Note: Transaction settings are cached for future retieval.
 *       If tm_Tran_index is NULL then we can safely assume that the
 *       database connect flag can be turned off. i.e., db_Connect_status=0
 */
void
tran_cache_tran_settings (int tran_index, float lock_timeout,
			  TRAN_ISOLATION tran_isolation)
{
  tm_Tran_index = tran_index;
  tm_Tran_waitsecs = (int) (lock_timeout * 1000);	/* lock timeout in miliseconds */
  tm_Tran_isolation = tran_isolation;

  /* This is a dirty, but quick, method by which we can flag that
   * the database connection has been terminated. This flag is used by
   * the C API calls to determine if a database connection exists.
   */
  if (tm_Tran_index == NULL_TRAN_INDEX)
    {
      db_Connect_status = DB_CONNECTION_STATUS_NOT_CONNECTED;
    }
}

/*
 * tran_get_tran_settings - Get transaction settings
 *
 * return: nothing
 *
 *   lock_wait(in/out): Transaction lock wait assigned to client transaction
 *   tran_isolation(in/out): Transaction isolation assigned to client
 *                     transactions
 *   async_ws(in/out): async_workspace assigned to client transactions
 *
 * Note: Retrieve transaction settings.
 */
void
tran_get_tran_settings (float *lock_wait, TRAN_ISOLATION * tran_isolation,
			bool * async_ws)
{
  *lock_wait = (float) TM_TRAN_WAITSECS () / 1000;
  /* lock timeout in miliseconds */ ;
  *tran_isolation = TM_TRAN_ISOLATION ();
  *async_ws = TM_TRAN_ASYNC_WS ();

}

/*
 * tran_reset_wait_times - Reset future waiting times for client transactions
 *
 * return: The old waitsecs.
 *
 *   waitsecs(in): Wait for at least this number of seconds to acquire a lock
 *               before the transaction is timed out.
 *               A negative value (e.g., -1) means wait forever until a lock
 *               is granted or transaction is selected as a victim of a
 *               deadlock.
 *               A value of zero means do not wait at all, timeout immediately
 *
 * NOTE: Reset the default waiting time for the client transactions.
 */
float
tran_reset_wait_times (float waitsecs)
{
  if (waitsecs > 0)
    {
      /* set lock timeout in miliseconds */
      tm_Tran_waitsecs = (int) (waitsecs * 1000);
    }
  else
    {
      /* special value like -1 (LK_INFINITE_WAIT) */
      tm_Tran_waitsecs = (int) waitsecs;
    }

  return (float) log_reset_waitsecs (tm_Tran_waitsecs);
}

/*
 * tran_reset_isolation - Reset isolation level of client session (transaction
 *                     index)
 *
 * return:  NO_ERROR if all OK, ER_ status otherwise
 *
 *   isolation(in): New Isolation level. One of the following:
 *                         TRAN_REP_CLASS_REP_INSTANCE
 *                         TRAN_REP_CLASS_COMMIT_INSTANCE
 *                         TRAN_REP_CLASS_UNCOMMIT_INSTANCE
 *                         TRAN_COMMIT_CLASS_COMMIT_INSTANCE
 *                         TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE
 *   async_ws(in): New async_workspace
 *
 * NOTE: Reset the default isolation level for the current transaction
 *              index (client). It is recommended that the isolation level of
 *              a client session get reseted at the beginning of a transaction
 *              (i.e., just after client restart, abort or commit). If this is
 *              not done some of the current acquired locks of the transaction
 *              may be released according to the new isolation level.
 */
int
tran_reset_isolation (TRAN_ISOLATION isolation, bool async_ws)
{
  int error_code = NO_ERROR;

  if (isolation < TRAN_MINVALUE_ISOLATION
      || isolation > TRAN_MAXVALUE_ISOLATION)
    {
      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_INVALID_ISOLATION_LEVEL, 2, TRAN_MINVALUE_ISOLATION,
	      TRAN_MAXVALUE_ISOLATION);
      return ER_LOG_INVALID_ISOLATION_LEVEL;
    }

  if (tm_Tran_isolation != isolation)
    {
      error_code = log_reset_isolation (isolation, true);
      if (error_code == NO_ERROR)
	{
	  tm_Tran_isolation = isolation;
	}
    }

  if (error_code == NO_ERROR)
    {
      tm_Tran_async_ws = async_ws;
    }

  return error_code;
}

/*
 * tran_commit_client_loose_ends - Execute client user loose ends of current
 *                              committed transaction (i.e., do client
 *                              postpone)
 *
 * return: state of commit operation
 *
 * NOTE: Execute client postpone operations of current transaction
 *              which has been already declared as committed.
 */
TRAN_STATE
tran_commit_client_loose_ends (void)
{
  LOG_COPY *log_area;		/* Area of loose end actions                */
  struct manylogs *manylogs;	/* All loose end action sin area            */
  struct onelog *onelog;	/* One loose end action                     */
  LOG_LSA next_lsa;		/* Next loose end actions start at next_lsa */
  int i;

  /* Get the first set of loose end actions */
  log_area = log_client_get_first_postpone (&next_lsa);
  while (log_area != NULL)
    {
      manylogs =
	(struct manylogs *) ((char *) log_area->mem + log_area->length -
			     sizeof (*manylogs));
      onelog =
	(struct onelog *) ((char *) log_area->mem + log_area->length -
			   sizeof (*manylogs));
      /* Execute all postpone/loose_end actions given in the area */
      for (i = 0; i < manylogs->num_logs; i++)
	{
	  (void) (*RVCL_fun[onelog->rcvindex].redofun)
	    (onelog->length, (char *) log_area->mem + onelog->offset);
	  onelog--;
	}
      /* Free the area and if there are more loose end actions... bring them */
      log_free_client_copy_area (log_area);
      if (next_lsa.pageid != NULL_PAGEID)
	{
	  log_area = log_client_get_next_postpone (&next_lsa);
	}
      else
	{
	  log_area = NULL;
	}
    }

  /* Indicate the recovery manager that all loose actions have been done */
  return log_has_finished_client_postpone ();
}

/*
 * tran_abort_client_loose_ends - UNDO CLIENT LOOSE ENDS OF CURRENT ABORTED
 *                             TRANSACTION
 *
 * return: state of abort operation
 *
 *   isknown_state(in): Do we know the state of the abort ?
 *
 * NOTE: Execute client undo operations of current transaction which
 *              has been already declared as aborted.
 */
TRAN_STATE
tran_abort_client_loose_ends (bool isknown_state)
{
  LOG_COPY *log_area;		/* Area of loose end actions                */
  struct manylogs *manylogs;	/* All loose end action sin area            */
  struct onelog *onelog;	/* One loose end action                     */
  LOG_LSA next_lsa;		/* Next loose end actions start at next_lsa */
  int i;

  /* Get the first set of loose end actions */
  if (isknown_state == true)
    {
      log_area = log_client_get_first_undo (&next_lsa);
    }
  else
    {
      log_area = log_client_unknown_state_abort_get_first_undo (&next_lsa);
      /* Are there any loose ends ? */
      if (log_area == NULL)
	{
	  return TRAN_UNACTIVE_ABORTED;
	}
    }
  while (log_area != NULL)
    {
      manylogs =
	(struct manylogs *) ((char *) log_area->mem + log_area->length -
			     sizeof (*manylogs));
      onelog =
	(struct onelog *) ((char *) log_area->mem + log_area->length -
			   sizeof (*manylogs));
      /* Execute all undo loose_end actions given in the area */
      for (i = 0; i < manylogs->num_logs; i++)
	{
	  (void) (*RVCL_fun[onelog->rcvindex].undofun)
	    (onelog->length, (char *) log_area->mem + onelog->offset);
	  onelog--;
	}
      /* Free the area and if there are more loose end actions... bring them */
      log_free_client_copy_area (log_area);
      if (next_lsa.pageid == NULL_PAGEID)
	{
	  break;
	}
      log_area = log_client_get_next_undo (&next_lsa);
    }

  /* Indicate the recovery manager that all loose actions have been done */
  return log_has_finished_client_undo ();
}

/* only loaddb changes this setting */
bool tm_Use_OID_preflush = true;

/*
 * tran_commit - COMMIT THE CURRENT TRANSACTION
 *
 * return:
 *
 *   retain_lock(in): false = release locks (default)
 *                    true  = retain locks
 *
 * NOTE: commit the current transaction. All objects that have been
 *              updated by the transaction and are still dirty in the
 *              workspace are flushed to the page buffer pool (server). Then,
 *              the commit statement is forwarded to the transaction manager
 *              in the server. The transaction manager in the server will do a
 *              few things and the notify the recovery manager of the commit.
 *              The recovery manager commits the transaction and may notify of
 *              some loose end actions that need to be executed in the client
 *              as part of the commit (after commit actions). As a result of
 *              the commit all changes made by the transaction are made
 *              permanent and all acquired locks are released. Any locks
 *              cached in the workspace are cleared.
 */
int
tran_commit (bool retain_lock)
{
  TRAN_STATE state;
  int error_code = NO_ERROR;

  /* check deferred trigger activities, these may prevent the transaction
     from being committed. */
  error_code = tr_check_commit_triggers (TR_TIME_BEFORE);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* tell the schema manager to flush any transaction caches */
  sm_transaction_boundary ();

  if (ws_need_flush ())
    {
      if (tm_Use_OID_preflush)
	{
	  (void) locator_assign_all_permanent_oids ();
	}

      /* Flush all dirty objects */
      /* Flush virtual objects first so that locator_all_flush doesn't see any */
      error_code = locator_all_flush ();
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  /* Clear all the queries */
  db_clear_client_query_result (true);

  /* if the commit fails or not, we should clear the clients savepoint list */
  tran_free_savepoint_list ();

  /* set need read flag of cluster config. */
  ccf_set_need_read_flag ();

  /* Forward the commit the transaction manager in the server */
  state = tran_server_commit (retain_lock);

  switch (state)
    {
    case TRAN_UNACTIVE_COMMITTED:
    case TRAN_UNACTIVE_COMMITTED_INFORMING_PARTICIPANTS:
      /* Successful commit */
      error_code = NO_ERROR;
      break;

    case TRAN_UNACTIVE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS:
      /*
       * The recovery manager has declared the transaction as committed but
       * there are loose postpone actions that need to be executed in the client
       */
      state = tran_commit_client_loose_ends ();
      error_code = NO_ERROR;
      break;

    case TRAN_UNACTIVE_ABORTED:
    case TRAN_UNACTIVE_ABORTED_INFORMING_PARTICIPANTS:
    case TRAN_UNACTIVE_UNILATERALLY_ABORTED:
      /* The commit failed */
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_commit: Unable to commit. Transaction was aborted\n");
#endif /* CUBRID_DEBUG */
      break;

    case TRAN_UNACTIVE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS:
      /*
       * The commit failed and there are loose end undo action to execute in
       * the client
       */
      error_code = er_errid ();
      state = tran_abort_client_loose_ends (true);
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_commit: Unable to commit. Transaction was aborted\n");
#endif /* CUBRID_DEBUG */
      break;

    case TRAN_UNACTIVE_UNKNOWN:
      if (!BOOT_IS_CLIENT_RESTARTED ())
	{
	  error_code = er_errid ();
	  break;
	}
      /* Fall Thru */
    case TRAN_RECOVERY:
    case TRAN_ACTIVE:
    case TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE:
    case TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE:
    case TRAN_UNACTIVE_XTOPOPE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_2PC_PREPARE:
    case TRAN_UNACTIVE_2PC_COLLECTING_PARTICIPANT_VOTES:
    case TRAN_UNACTIVE_2PC_ABORT_DECISION:
    case TRAN_UNACTIVE_2PC_COMMIT_DECISION:
    default:
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_commit: Unknown commit state = %s at client\n",
		    log_state_string (state));
#endif /* CUBRID_DEBUG */
      break;
    }

  /* clear workspace information and any open query cursors */
  if (error_code == NO_ERROR || BOOT_IS_CLIENT_RESTARTED ())
    {
      ws_clear_all_hints (retain_lock);
      er_stack_clearall ();
    }

  /* allow triggers AFTER the commit */
  if (error_code == NO_ERROR)
    {
      error_code = tr_check_commit_triggers (TR_TIME_AFTER);
    }

  return error_code;
}

/*
 * tran_abort - ABORT THE CURRENT TRANSACTION
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 * NOTE:Abort the current transaction. All objects updated by the
 *              current transaction that are still dirty are removed from the
 *              workspace and then the abort is forwarded to the transaction
 *              manager in the server. In the server all updates made to the
 *              database and the page buffer pool are rolled back, and the
 *              transaction is declared as aborted. The server may notify the
 *              the transaction manager in the client of any loose end undoes
 *              that need to be executed in the client as part of the abort.
 *              As a result of the abort all changes made by the transaction
 *              are rolled back and acquired locks are released. Any locks
 *              cached in the workspace are cleared.
 */
int
tran_abort (void)
{
  TRAN_STATE state;
  int error_cod = NO_ERROR;

  /*
   * inform the trigger manager of the event, triggers can't prevent a
   * rollback, might not want to do this if we're being unilaterally
   * aborted ?
   */
  tr_check_rollback_triggers (TR_TIME_BEFORE);

  /* tell the schema manager to flush any transaction caches */
  sm_transaction_boundary ();

#if defined(SA_MODE)
  ws_clear ();
#else /* SA_MODE */
  /* Remove any dirty objects and remove any hints */
  ws_abort_mops (false);
  ws_filter_dirty ();
#endif /* SA_MODE */

  /* free the local list of savepoint names */
  tran_free_savepoint_list ();

  /* Clear any query cursor */
  db_clear_client_query_result (true);

  /* set need read flag of cluster config. */
  ccf_set_need_read_flag ();

  /* Forward the abort the transaction manager in the server */
  state = tran_server_abort ();

  switch (state)
    {
      /* Successful abort */
    case TRAN_UNACTIVE_ABORTED:
    case TRAN_UNACTIVE_ABORTED_INFORMING_PARTICIPANTS:
      break;

    case TRAN_UNACTIVE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS:
      /* Successful abort with client loose end undoes */
      state = tran_abort_client_loose_ends (true);
      break;

    case TRAN_UNACTIVE_UNKNOWN:
      if (!BOOT_IS_CLIENT_RESTARTED ())
	{
	  error_cod = er_errid ();
	  break;
	}
      /* Fall Thru */
    case TRAN_RECOVERY:
    case TRAN_ACTIVE:
    case TRAN_UNACTIVE_COMMITTED:
    case TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE:
    case TRAN_UNACTIVE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_UNILATERALLY_ABORTED:
    case TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE:
    case TRAN_UNACTIVE_XTOPOPE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_2PC_PREPARE:
    case TRAN_UNACTIVE_2PC_COLLECTING_PARTICIPANT_VOTES:
    case TRAN_UNACTIVE_2PC_ABORT_DECISION:
    case TRAN_UNACTIVE_2PC_COMMIT_DECISION:
    case TRAN_UNACTIVE_COMMITTED_INFORMING_PARTICIPANTS:
    default:
      error_cod = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE, "tm_abort: Unknown abort state = %s\n",
		    log_state_string (state));
#endif /* CUBRID_DEBUG */
      break;
    }

  er_stack_clearall ();

  /* can these do anything useful ? */
  tr_check_rollback_triggers (TR_TIME_AFTER);

  return error_cod;
}

/*
 * tran_unilaterally_abort - Unilaterally abort the current transaction
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 * NOTE:The current transaction is unilaterally aborted by a client
 *              module of the system.
 *              Execute tran_abort & set an error message
 */
int
tran_unilaterally_abort (void)
{
  int error_code = NO_ERROR;
  char user_name[L_cuserid + 1];
  char host[MAXHOSTNAMELEN];
  int pid;

  /* Get the user name, host, and process identifier */
  if (getuserid (user_name, L_cuserid) == NULL)
    {
      strcpy (user_name, "(unknown)");
    }
  if (GETHOSTNAME (host, MAXHOSTNAMELEN) != 0)
    {
      /* unknown error */
      strcpy (host, "(unknown)");
    }
  pid = getpid ();

  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	  ER_LK_UNILATERALLY_ABORTED, 4, tm_Tran_index, user_name, host, pid);

  error_code = tran_abort ();

  /* does it make sense to have these ? */
  tr_check_abort_triggers ();

  return error_code;
}

/*
 * tran_abort_only_client - Abort the current transaction only at the client
 *                       level. (the server aborted the transaction)
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   is_server_down(in):Was the transaction aborted because of the server crash?
 *
 * NOTE: The current transaction is aborted only at the client level,
 *              since the transaction has already been aborted at the server.
 *              All dirty objects in the workspace are removed and cached
 *              locks are cleared.
 *       This function is called when the transaction component (e.g.,
 *              transaction object locator) finds that the transaction was
 *              unilaterally aborted.
 */
int
tran_abort_only_client (bool is_server_down)
{
  if (!BOOT_IS_CLIENT_RESTARTED ())
    {
      if (is_server_down)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED, 0);
	  return ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED;
	}

      return NO_ERROR;
    }

  /* Remove any dirty objects and close all open query cursors */
  ws_abort_mops (true);
  ws_filter_dirty ();
  db_clear_client_query_result (false);

  if (is_server_down == false)
    {
      /* Do we need to execute any loose ends ? */
      (void) tran_abort_client_loose_ends (false);
      tr_check_abort_triggers ();
      return NO_ERROR;
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED, 0);
      return ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED;
    }

  return NO_ERROR;
}

/*
 * tran_has_updated - HAS TRANSACTION UPDATED THE DATABASE ?
 *
 * return:
 *
 * NOTE:Find if the transaction has dirtied the database.
 */
bool
tran_has_updated (void)
{
  return (ws_has_updated () || tran_server_has_updated ());
}

/*
 * tran_is_active_and_has_updated - Find if transaction is active and 
 *				    has updated the database ?
 *
 * return:
 *
 * NOTE:Find if the transaction is active and has updated/dirtied the
 *              database.
 */
bool
tran_is_active_and_has_updated (void)
{
  return (ws_has_updated () || tran_server_is_active_and_has_updated ());
}

/*
 * tran_set_global_tran_info - Set global transaction information
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   gtrid(in): global transaction identifier
 *   info(in): pointer to the user information to be set
 *   size(in): size of the user information to be set
 *
 * Note:Set the user information related with the global transaction.
 *              The global transaction identified by the 'gtrid' should exist
 *              and should be the value returned by 'db_2pc_start_transaction'
 *              You can use this function to set the longer format of global
 *              transaction identifier such as XID of XA interface.
 */
int
tran_set_global_tran_info (int gtrid, void *info, int size)
{
  if (tran_server_set_global_tran_info (gtrid, info, size) == NO_ERROR)
    {
      return NO_ERROR;
    }
  else
    {
      return er_errid ();
    }
}

/*
 * tran_get_global_tran_info - Get global transaction information
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   gtrid(in): global transaction identifier
 *   buffer(in):pointer to the buffer into which the user information is stored
 *   size(in):  size of the buffer
 *
 * NOTE: Get the user information of the global transaction identified
 *              by the 'gtrid'.
 *              You can use this function to get the longer format of global
 *              transaction identifier such as XID of XA interface. This
 *              function is designed to use if you want to get XID after
 *              calling 'db_2pc_prepared_transactions' to support xa_recover()
 */
int
tran_get_global_tran_info (int gtrid, void *buffer, int size)
{
  int error_code = NO_ERROR;

  if (tran_server_get_global_tran_info (gtrid, buffer, size) != NO_ERROR)
    {
      error_code = er_errid ();
    }

  return error_code;
}

/*
 * tran_2pc_start -  Start transaction as a part of global transaction
 *
 * return: return global transaction identifier
 *
 * NOTE: Make current transaction as a part of a global transaction by
 *              assigning a global transaction identifier(gtrid).
 *              It is recommended to call this function just after the end of
 *              a transaction(commit or abort) before executing other works.
 *              This function is one way of getting gtrid of the transaction.
 *              The other way is to use 'db_2pc_prepare_to_commit_transaction'
 *              The function 'db_2pc_prepare_transaction' should be used if
 *              this function is called.
 */
int
tran_2pc_start (void)
{
  return tran_server_2pc_start ();
}

/*
 * tran_2pc_prepare - Prepare transaction to commit
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 * NOTE: Prepare the current transaction for commitment in 2PC. The
 *              transaction should be made as a part of a global transaction
 *              before by 'db_2pc_start_transaction', a pair one of this
 *              function.
 *              The system promises not to unilaterally abort the transaction.
 *              After this function call, the only API functions that should
 *              be executed are 'db_commit_transaction' &
 *              'db_abort_transaction'.
 */
int
tran_2pc_prepare (void)
{
  TRAN_STATE state;
  int error_code = NO_ERROR;

  /* flush all dirty objects */
  error_code = locator_all_flush ();
  if (error_code != NO_ERROR)
    {
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_2pc_prepare: Unable to prepare. Flush failed\n");
#endif /* CUBRID_DEBUG */
      goto end;
    }

  /* forward the prepare to the transaction manager in the server */
  state = tran_server_2pc_prepare ();
  switch (state)
    {
    case TRAN_ACTIVE:
      /* The preparation to commit failed probably due to inproper state;
         Transaction is still active */
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_2pc_prepare: Unable to prepare. Transaction is still active\n");
#endif /* CUBRID_DEBUG */
      break;

    case TRAN_UNACTIVE_2PC_PREPARE:
      /* Successful to prepare (or repeated preparation). */
      error_code = NO_ERROR;
      break;

    case TRAN_UNACTIVE_ABORTED:
    case TRAN_UNACTIVE_UNILATERALLY_ABORTED:
      /* The preparation to commit failed; Transaction has been aborted */
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_2pc_prepare: Unable to prepare. Transaction was aborted\n");
#endif /* CUBRID_DEBUG */
      break;

    case TRAN_UNACTIVE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS:
      /* The preparation failed and there are loose end undo action to execute
         in the client */
      error_code = er_errid ();
      state = tran_abort_client_loose_ends (true);
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_2pc_prepare: Unable to prepare. Transaction was aborted\n");
#endif /* CUBRID_DEBUG */
      break;

    case TRAN_UNACTIVE_COMMITTED:
      /* The transaction was committed. There is not a need for 2PC prepare.
         This could happend for read only transactions. */
      error_code = NO_ERROR;
      break;

    case TRAN_UNACTIVE_UNKNOWN:
      if (!BOOT_IS_CLIENT_RESTARTED ())
	{
	  error_code = er_errid ();
	  break;
	}
      /* fall thru */

    case TRAN_RECOVERY:
    case TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE:
    case TRAN_UNACTIVE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE:
    case TRAN_UNACTIVE_XTOPOPE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_2PC_COLLECTING_PARTICIPANT_VOTES:
    case TRAN_UNACTIVE_2PC_ABORT_DECISION:
    case TRAN_UNACTIVE_2PC_COMMIT_DECISION:
    case TRAN_UNACTIVE_COMMITTED_INFORMING_PARTICIPANTS:
    case TRAN_UNACTIVE_ABORTED_INFORMING_PARTICIPANTS:
    default:
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_2pc_prepare: Unexpected prepare. state = %s\n",
		    log_state_string (state));
#endif /* CUBRID_DEBUG */
      break;
    }				/* switch (state) */

  /* clear workspace information and any open query cursors */
  if (error_code == NO_ERROR || BOOT_IS_CLIENT_RESTARTED ())
    {
      db_clear_client_query_result (true);
      ws_clear_all_hints (false);
      er_stack_clearall ();
    }

end:
  return error_code;
}

/*
 * tran_2pc_recovery_prepared - Obtain list of prepared transactions
 *
 * return: the number of ids copied into 'gtrids[]'
 *
 *   gtrids(in): array into which global transaction identifiers are copied
 *   size(in): size of 'gtrids[]' array
 *
 * NOTE: For restart recovery of global transactions, this function
 *              returns gtrids of transactions in prepared state, which was
 *              a part of a global transaction.
 *              If the return value is less than the 'size', there's no more
 *              transactions to recover.
 */
int
tran_2pc_recovery_prepared (int gtrids[], int size)
{
  return tran_server_2pc_recovery_prepared (gtrids, size);
}

/*
 * tran_2pc_attach_global_tran - Attach to a loose end 2pc transaction
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   gtrid(in):  Global transaction identifier
 *
 * NOTE: Attaches the user to the transaction that was the local
 *              part of the specified global transaction. The current
 *              transaction is aborted before the attachement takes place. The
 *              current transaction must not be in the middle of a 2PC.
 *              It is recommended to attach a client to a 2PC loose end
 *              transaction just after the client restart or after a commit
 *              or abort.
 */
int
tran_2pc_attach_global_tran (int gtrid)
{
  int new_tran_index;

  new_tran_index = tran_server_2pc_attach_global_tran (gtrid);
  if (new_tran_index == NULL_TRAN_INDEX)
    {
      return er_errid ();
    }

  tm_Tran_index = new_tran_index;

  return NO_ERROR;
}

/*
 * tran_2pc_prepare_global_tran - Prepare to commit the current transaction
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   gtrid(in): Identifier of the global transaction
 *
 * NOTE: This function prepares the transaction identified by "gtrid"
 *              for commitment. All objects that have been updated by the
 *              transaction and are still dirty in the workspace are flushed
 *              to the page buffer pool (server). Then, prepare statement is
 *              forwarded to the transaction manager to guarantee the
 *              the commitment.
 */
int
tran_2pc_prepare_global_tran (int gtrid)
{
  TRAN_STATE state;
  int error_code = NO_ERROR;

  /* Flush all dirty objects */
  error_code = locator_all_flush ();
  if (error_code != NO_ERROR)
    {
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_2pc_prepare: Unable to prepare to commit. \nFlush failed\n");
#endif /* CUBRID_DEBUG */
      return error_code;
    }

  /* Forward the prepare to commit to the transaction manager in the server */
  state = tran_server_2pc_prepare_global_tran (gtrid);
  switch (state)
    {
    case TRAN_ACTIVE:
      /* The preperation to commit failed probabely due to the given global
       * transaction identifier; Transaction is still active
       */
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_2pc_prepare: Unable to prepare to commit.\n %s",
		    "Transaction is still active\n");
#endif /* CUBRID_DEBUG */
      break;

    case TRAN_UNACTIVE_2PC_PREPARE:
      /* Successful preperation to commit */
      error_code = NO_ERROR;
      break;

    case TRAN_UNACTIVE_ABORTED:
    case TRAN_UNACTIVE_UNILATERALLY_ABORTED:
      /* The preperation to commit failed; Transaction has been aborted */
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_2pc_prepare: Unable to prepare to commit.\n %s",
		    "Transaction was aborted\n");
#endif /* CUBRID_DEBUG */
      break;

    case TRAN_UNACTIVE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS:
      /*
       * The preperation to commit failed and there are loose end undo action
       * to execute in the client
       */
      error_code = er_errid ();
      state = tran_abort_client_loose_ends (true);
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_2pc_prepare: Unable to prepare to commit.\n %s",
		    "Transaction was aborted\n");
#endif /* CUBRID_DEBUG */
      break;

    case TRAN_UNACTIVE_COMMITTED:
      /*
       * The transaction was committed. There is not a need for 2PC prepare.
       * This could happend for read only transactions
       */
      error_code = NO_ERROR;
      break;

    case TRAN_UNACTIVE_UNKNOWN:
      if (!BOOT_IS_CLIENT_RESTARTED ())
	{
	  error_code = er_errid ();
	  break;
	}
      /* Fall Thru */

    case TRAN_RECOVERY:
    case TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE:
    case TRAN_UNACTIVE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE:
    case TRAN_UNACTIVE_XTOPOPE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS:
    case TRAN_UNACTIVE_2PC_COLLECTING_PARTICIPANT_VOTES:
    case TRAN_UNACTIVE_2PC_ABORT_DECISION:
    case TRAN_UNACTIVE_2PC_COMMIT_DECISION:
    case TRAN_UNACTIVE_COMMITTED_INFORMING_PARTICIPANTS:
    case TRAN_UNACTIVE_ABORTED_INFORMING_PARTICIPANTS:
    default:
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_2pc_prepare: Unexpected prepare to commit state = %s\n",
		    log_state_string (state));
#endif /* CUBRID_DEBUG */
      break;
    }

  /* clear workspace information and any open query cursors */
  if (error_code == NO_ERROR || BOOT_IS_CLIENT_RESTARTED ())
    {
      db_clear_client_query_result (true);
      ws_clear_all_hints (false);
      er_stack_clearall ();
    }

  return error_code;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * tran_start_topop - Start a macro nested top operation
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 */
int
tran_start_topop (void)
{
  LOG_LSA topop_lsa;
  int error_code = NO_ERROR;

  /* Flush all dirty objects */
  if (ws_need_flush ())
    {
      error_code = locator_all_flush ();
      if (error_code != NO_ERROR)
	{
#if defined(CUBRID_DEBUG)
	  er_log_debug (ARG_FILE_LINE,
			"tran_start_topop: Unable to start a top operation. \n %s",
			" Flush failed.\nerrmsg = %s\n", er_msg ());
#endif /* CUBRID_DEBUG */
	  goto end;
	}
    }

  if (tran_server_start_topop (&topop_lsa) != NO_ERROR)
    {
      error_code = ER_FAILED;
    }

end:
  return error_code;
}

static int
tran_end_topop_commit (void)
{
  int error_code = NO_ERROR;
  TRAN_STATE state;
  LOG_LSA topop_lsa;

  /* Flush all dirty objects */
  if (ws_need_flush ())
    {
      error_code = locator_all_flush ();
      if (error_code != NO_ERROR)
	{
#if defined(CUBRID_DEBUG)
	  er_log_debug (ARG_FILE_LINE,
			"tm_end_topop: Unable to finish a nested top oper. Flush failed.\nerrmsg = %s\n",
			er_msg ());
#endif /* CUBRID_DEBUG */
	  goto end;
	}
    }

  state = tran_server_end_topop (LOG_RESULT_TOPOP_COMMIT, &topop_lsa);
  if (state == TRAN_UNACTIVE_XTOPOPE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS)
    {
      /*
       * The recovery manager has declared the top system operation as
       * committed but there are loose postpone actions that need
       * to be executed in the client
       */
      state = tran_commit_client_loose_ends ();
    }
  else if (state == TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS)
    {
      /*
       * The commit of the nested top action failed and there are loose
       * end undo actions to execute in the client
       */
      state = tran_abort_client_loose_ends (true);
    }

  if (state != TRAN_UNACTIVE_COMMITTED)
    {
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_end_topop: oper failed with state = %s at client.\n",
		    log_state_string (state));
#endif /* CUBRID_DEBUG */
    }

end:
  return error_code;
}

static int
tran_end_topop_abort (void)
{
  int error_code = NO_ERROR;
  TRAN_STATE state;
  LOG_LSA topop_lsa;

  /* Remove any dirty objects */
  ws_abort_mops (false);
  ws_filter_dirty ();
#if defined(SA_MODE)
  ws_clear ();
#endif /* SA_MODE */
  state = tran_server_end_topop (LOG_RESULT_TOPOP_ABORT, &topop_lsa);
  if (state == TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS)
    {
      /* Successful abort with client loose end undoes */
      state = tran_abort_client_loose_ends (true);
    }
  if (state != TRAN_UNACTIVE_ABORTED)
    {
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "tm_end_topop: oper failed with state = %s at client.\n",
		    log_state_string (state));
#endif /* CUBRID_DEBUG */
    }

  return error_code;
}

/*
 * tran_end_topop - END A MACRO NESTED TOP OPERATION
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   result(in): Result of the nested top action
 *
 * NOTE: Finish the latest nested top macro operation by either
 *              committing, aborting, or attaching to outter parent. Note that
 *              a top operation is not associated with the current
 *              transaction, thus, it can be committed and/or aborted
 *              independently of the transaction.
 */
int
tran_end_topop (LOG_RESULT_TOPOP result)
{
  int error_code = NO_ERROR;
  LOG_LSA topop_lsa;
  TRAN_STATE state;

  switch (result)
    {
    case LOG_RESULT_TOPOP_COMMIT:
      error_code = tran_end_topop_commit ();
      break;

    case LOG_RESULT_TOPOP_ABORT:
      error_code = tran_end_topop_abort ();
      break;

    case LOG_RESULT_TOPOP_ATTACH_TO_OUTER:
    default:
      state = tran_server_end_topop (result, &topop_lsa);
      if (state != TRAN_ACTIVE)
	{
	  error_code = er_errid ();
#if defined(CUBRID_DEBUG)
	  er_log_debug (ARG_FILE_LINE,
			"tm_end_topop: oper failed with state = %s at client.\n",
			log_state_string (state));
#endif /* CUBRID_DEBUG */
	}
      break;
    }

  return error_code;

}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * tran_add_savepoint -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   savept_name(in):
 *
 * NOTE:insert a savepoint name into the front of the list.  This way, the list
 * is sorted in reverse chronological order
 */
static int
tran_add_savepoint (const char *savept_name)
{
  DB_NAMELIST *sp;

  sp = (DB_NAMELIST *) db_ws_alloc (sizeof (DB_NAMELIST));
  if (sp == NULL)
    {
      return er_errid ();
    }

  sp->name = ws_copy_string (savept_name);
  if (sp->name == NULL)
    {
      db_ws_free (sp);
      return er_errid ();
    }
  sp->next = user_savepoint_list;
  user_savepoint_list = sp;

  return NO_ERROR;
}

/*
 * tran_free_savepoint_list -
 *
 * return:
 *
 * NOTE:free the entire user savepoint list.  Called during abort, commit, or
 * restart
 */
void
tran_free_savepoint_list (void)
{
  nlist_free (user_savepoint_list);
  user_savepoint_list = NULL;
}

/*
 * tran_free_list_upto_savepoint -
 *
 * return:
 *
 *   savept_name(in):
 *
 * NOTE:frees the latest savepoints from the list up to, but not including, the
 * given savepoint.  Called during rollback to savepoint command.
 */
static void
tran_free_list_upto_savepoint (const char *savept_name)
{
  DB_NAMELIST *sp, *temp;
  bool found = false;

  /* first, check to see if it's in the list */
  for (sp = user_savepoint_list; sp && !found; sp = sp->next)
    {
      if (intl_mbs_casecmp (sp->name, savept_name) == 0)
	{
	  found = true;
	}
    }

  /* not 'found' is not necessarily an error.  We may be rolling back to a
   * system-defined savepoint rather than a user-defined savepoint.  In that
   * case, the name would not appear on the user savepoint list and the list
   * should be preserved.  We should be able to guarantee that any rollback
   * to a system-defined savepoint will affect only the latest atomic command
   * and not overlap any user-defined savepoint.  That is, system invoked
   * partial rollbacks should never rollback farther than the last
   * user-defined savepoint.
   */
  if (found == true)
    {
      for (sp = user_savepoint_list; sp;)
	{
	  if (intl_mbs_casecmp (sp->name, savept_name) == 0)
	    {
	      break;
	    }

	  temp = sp;
	  sp = sp->next;
	  db_ws_free ((char *) temp->name);
	  db_ws_free (temp);
	}
      user_savepoint_list = sp;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * tran_get_savepoints - Get list of current savepoint names.  The latest
 *                    savepoint is listed first. (Reverse chronological
 *                    order)
 *
 * return: NO_ERROR
 *
 *   savepoint_list(in): savepoint list pointer
 *
 * NOTE: A list of user-defined savepoint names is maintained locally.
 *              The list has a lifespan of the current transaction and is
 *              freed upon commit or abort.  It is partially freed upon a
 *              partial rollback (to savepoint).  The savepoint list returned
 *              in this function must be later freed by db_namelist_free() or
 *              nlist_free().
 */
int
tran_get_savepoints (DB_NAMELIST ** savepoint_list)
{
  *savepoint_list = nlist_copy (user_savepoint_list);
  return NO_ERROR;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * tran_savepoint - Declare a user savepoint
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   savept_name(in): Name of the savepoint
 *   user(in): true if user-defined, not internally by system
 *
 * NOTE: A savepoint is established for the current transaction, so
 *              that future transaction actions can be rolled back to this
 *              established savepoint. We call this operation a partial abort
 *              (rollback). That is, all database actions affected by the
 *              transaction after the savepoint are "undone", and all effects
 *              of the transaction preceding the savepoint remain. The
 *              transaction can then continue executing other database
 *              statement. It is permissible to abort to the same savepoint
 *              repeatedly within the same transaction.
 *              If the same savepoint name is used in multiple savepoint
 *              declarations within the same transaction, then only the latest
 *              savepoint with that name is available for aborts and the
 *              others are forgotten.
 *              There are no limits on the number of savepoints that a
 *              transaction can have.
 */
int
tran_savepoint (const char *savept_name, bool user)
{
  LOG_LSA savept_lsa;
  int error_code = NO_ERROR;

  /* Flush all dirty objects */
  if (ws_need_flush ())
    {
      error_code = locator_all_flush ();
      if (error_code != NO_ERROR)
	{
#if defined(CUBRID_DEBUG)
	  er_log_debug (ARG_FILE_LINE,
			"tran_savepoint: Unable to start a top operation\n Flush failed.\nerrmsg = %s",
			er_msg ());
#endif /* CUBRID_DEBUG */
	  return error_code;
	}
    }

  if (tran_server_savepoint (savept_name, &savept_lsa) != NO_ERROR)
    {
      error_code = er_errid ();
      return error_code;
    }

  /* add savepoint to local list */
  if (user)
    {
      error_code = tran_add_savepoint (savept_name);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  return error_code;
}

/*
 * tran_abort_upto_savepoint - Abort operations of a transaction upto a savepoint
 *
 * return: state of partial aborted operation (i.e., notify if
 *              there are client actions that need to be undone).
 *
 *   savepoint_name(in): Name of the savepoint
 *
 * NOTE: All the effects of the current transaction after the
 *              given savepoint are undone, and all effects of the transaction
 *              preceding the given savepoint remain. After the partial abort
 *              the transaction can continue its normal execution as if
 *              the statements that were undone were never executed.
 *              All objects updated by the current transaction that are still
 *              dirty are removed from the workspace and then the partial
 *              abort is forwarded to the transaction manager in the server.
 *              In the server all updates made to the database and the page
 *              buffer pool after the given savepoint are rolled back. The
 *              server may notify the transaction manager in the client of
 *              any client loose_end undoes that need to be executed at the
 *              client as part of the partial abort.
 *              The locks in the workspace will need to be cleared since we do
 *              not know in the client what objects were rolled back. This is
 *              needed since the client does not request the objects from the
 *              server if the desired lock has been already acquired (cached
 *              in the workspace). Therefore, from the point of view of the
 *              workspace, the transaction will need to validate the objects
 *              that need to be accessed in the future.
 */
int
tran_abort_upto_savepoint (const char *savepoint_name)
{
  /* delete client's local copy of savepoint names back to here */
  tran_free_list_upto_savepoint (savepoint_name);

  return tran_internal_abort_upto_savepoint (savepoint_name, false);
}

/*
 * tran_internal_abort_upto_savepoint -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   savepoint_name(in):
 *   client_decache_all_but_norealclasses(in):
 *
 * NOTE:
 */
int
tran_internal_abort_upto_savepoint (const char *savepoint_name,
				    bool client_decache_all_but_norealclasses)
{
  int error_code = NO_ERROR;
  LOG_LSA savept_lsa;
  TRAN_STATE state;

  /* tell the schema manager to flush any transaction caches */
  sm_transaction_boundary ();

  /*
   * We need to start all over since we do not know what set of objects are
   * going to be rolled back.. Thuis, we need to remove any kind of hints
   * cached in the workspace.
   */

  if (client_decache_all_but_norealclasses == true)
    {
      ws_decache_allxlockmops_but_norealclasses ();
      ws_filter_dirty ();
    }
  else
    {
#if defined(SA_MODE)
      ws_clear ();
#else /* SA_MODE */
      /* Remove any dirty objects and remove any hints */
      ws_abort_mops (false);
      ws_filter_dirty ();
#endif /* SA_MODE */
    }

  state = tran_server_partial_abort (savepoint_name, &savept_lsa);
  if (state == TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS)
    {
      /* Successful abort with client loose end undoes */
      state = tran_abort_client_loose_ends (true);
    }
  if (state != TRAN_UNACTIVE_ABORTED)
    {
      error_code = er_errid ();
#if defined(CUBRID_DEBUG)
      if (error_code != ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED &&
	  error_code != ER_NET_SERVER_CRASHED)
	{
	  er_log_debug (ARG_FILE_LINE,
			"tm_abort_upto_savepoint: oper failed with state = %s %s",
			log_state_string (state), " at client.\n");
	}
#endif /* CUBRID_DEBUG */
    }

  return error_code;
}
