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
 * log_comm.c - log and recovery manager (at client & server)
 */

#ident "$Id$"

#include "config.h"

#include <assert.h>
#if !defined(WINDOWS)
#include <sys/time.h>
#endif /* !WINDOWS */

#include "log_comm.h"
#include "memory_alloc.h"
#include "storage_common.h"
#include "error_manager.h"
#include "porting.h"
#include "environment_variable.h"
#include "system_parameter.h"
#include "misc_string.h"
#include "intl_support.h"
#include "log_impl.h"

struct tran_state_name
{
  TRAN_STATE state;
  const char *name;
};
typedef struct tran_state_name TRAN_STATE_NAME;

static TRAN_STATE_NAME log_Tran_state_names[] = {
  {TRAN_RECOVERY,
   "TRAN_RECOVERY"},
  {TRAN_ACTIVE,
   "TRAN_ACTIVE"},
  {TRAN_UNACTIVE_COMMITTED,
   "TRAN_UNACTIVE_COMMITTED"},
  {TRAN_UNACTIVE_WILL_COMMIT,
   "TRAN_UNACTIVE_WILL_COMMIT"},
  {TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE,
   "TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE"},
  {TRAN_UNACTIVE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS,
   "TRAN_UNACTIVE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS"},
  {TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE,
   "TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE"},
  {TRAN_UNACTIVE_XTOPOPE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS,
   "TRAN_UNACTIVE_XTOPOPE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS"},
  {TRAN_UNACTIVE_ABORTED,
   "TRAN_UNACTIVE_ABORTED"},
  {TRAN_UNACTIVE_UNILATERALLY_ABORTED,
   "TRAN_UNACTIVE_UNILATERALLY_ABORTED"},
  {TRAN_UNACTIVE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS,
   "TRAN_UNACTIVE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS"},
  {TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS,
   "TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS"},
  {TRAN_UNACTIVE_2PC_PREPARE,
   "TRAN_UNACTIVE_2PC_PREPARE"},
  {TRAN_UNACTIVE_2PC_COLLECTING_PARTICIPANT_VOTES,
   "TRAN_UNACTIVE_2PC_COLLECTING_PARTICIPANT_VOTES"},
  {TRAN_UNACTIVE_2PC_ABORT_DECISION,
   "TRAN_UNACTIVE_2PC_ABORT_DECISION"},
  {TRAN_UNACTIVE_2PC_COMMIT_DECISION,
   "TRAN_UNACTIVE_2PC_COMMIT_DECISION"},
  {TRAN_UNACTIVE_COMMITTED_INFORMING_PARTICIPANTS,
   "TRAN_UNACTIVE_COMMITTED_INFORMING_PARTICIPANTS"},
  {TRAN_UNACTIVE_ABORTED_INFORMING_PARTICIPANTS,
   "TRAN_UNACTIVE_ABORTED_INFORMING_PARTICIPANTS"},
  {TRAN_UNACTIVE_UNKNOWN,
   "TRAN_STATE_UNKNOWN"}
};

struct isolation_name
{
  TRAN_ISOLATION isolation;
  const char *name;
};
typedef struct isolation_name TRAN_ISOLATION_NAME;

static TRAN_ISOLATION_NAME log_Isolation_names[] = {
  {TRAN_SERIALIZABLE,
   "SERIALIZABLE"},
  {TRAN_REP_CLASS_REP_INSTANCE,
   "REPEATABLE CLASSES AND REPEATABLE INSTANCES"},
  {TRAN_REP_CLASS_COMMIT_INSTANCE,
   "REPEATABLE CLASSES AND READ COMMITTED INSTANCES (STABILITY)"},
  {TRAN_REP_CLASS_UNCOMMIT_INSTANCE,
   "REPEATABLE CLASSES AND READ UNCOMMITTED INSTANCES"},
  {TRAN_COMMIT_CLASS_COMMIT_INSTANCE,
   "READ COMMITTED CLASSES AND READ COMMITTED INSTANCES"},
  {TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE,
   "READ COMMITTED CLASSES AND READ UNCOMMITTED INSTANCES"},
  {TRAN_UNKNOWN_ISOLATION,
   "TRAN_UNKNOWN_ISOLATION"}
};

const int LOG_MIN_NBUFFERS = 3;

/*
 * log_state_string - Translate state into string representation
 *
 * return:
 *
 *   state(in): Transaction state
 *
 * NOTE: Translate state into a string representation.
 */
const char *
log_state_string (TRAN_STATE state)
{
  int num = sizeof (log_Tran_state_names) / sizeof (TRAN_STATE_NAME);
  int i;

  for (i = 0; i < num; i++)
    {
      if (log_Tran_state_names[i].state == state)
	{
	  return log_Tran_state_names[i].name;
	}
    }

  return "TRAN_STATE_UNKNOWN";

}

/*
 * log_isolation_string - Translate isolation level into string representation
 *
 * return:
 *
 *   isolation(in): Isolation level. One of the following:
 *                         TRAN_REP_CLASS_REP_INSTANCE
 *                         TRAN_REP_CLASS_COMMIT_INSTANCE
 *                         TRAN_REP_CLASS_UNCOMMIT_INSTANCE
 *                         TRAN_COMMIT_CLASS_COMMIT_INSTANCE
 *                         TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE
 *
 * NOTE:Translate degree of consistency into a string representation.
 */
const char *
log_isolation_string (TRAN_ISOLATION isolation)
{
  int num = sizeof (log_Isolation_names) / sizeof (TRAN_ISOLATION_NAME);
  int i;

  for (i = 0; i < num; i++)
    {
      if (log_Isolation_names[i].isolation == isolation)
	{
	  return log_Isolation_names[i].name;
	}
    }

  return "TRAN_UNKNOWN_ISOLATION";
}

/*
 * log_alloc_client_copy_area - ALLOCATE A COPY AREA FOR COPYING LOG RECORDS OF
 *                             CLIENTS
 *
 * return: LOG_COPY *
 *
 *   min_length(in): Minimal size of needed area
 *
 * NOTE: Allocate a copy area for transmitting client log records to
 *              transaction manager in the client.
 */
LOG_COPY *
log_alloc_client_copy_area (int min_length)
{
  LOG_COPY *copy_area;
  unsigned int total_length;
  unsigned int network_pagesize;

  if (min_length <= 0)
    {
      total_length = db_network_page_size ();
    }
  else
    {
      total_length = min_length + sizeof (*copy_area);

      /*
       * Make the total_length to be multiple of NETWORK_PAGESIZE since the
       * copyareas are used to log records to/from server and we would like to
       * maximize the communication line.
       */
      network_pagesize = db_network_page_size ();
      total_length = DB_ALIGN (total_length, network_pagesize);
    }

  /* Allocate the stuff in pagesize */
  copy_area = (LOG_COPY *) malloc (total_length);
  if (copy_area == NULL)
    {
      return NULL;
    }
  copy_area->mem = (char *) copy_area + sizeof (*copy_area);
  copy_area->length = total_length - sizeof (*copy_area);

  return copy_area;
}

/*
 * log_free_client_copy_area - FREE LOG COPY AREA
 *
 * return: nothing..
 *
 *   copy_area(in): Area to free
 *
 * NOTE: Free a copy area which was used for transmitting client log
 *              records to transaction manager in the client.
 */
void
log_free_client_copy_area (LOG_COPY * copy_area)
{
  assert (copy_area != NULL);

  free_and_init (copy_area);
}

/*
 * log_pack_descriptors - PACK DESCRIPTORS FOR A LOG COPY AREA
 *
 * return: updated pack area pointer
 *
 *   num_records(in): Number of records
 *   log_area(in): Copy area where records are placed
 *   descriptors(in): Packed descriptor array
 *
 * NOTE: Pack the descriptors to be sent over the network from the
 *              copy area.  The caller is responsible for determining that
 *              descriptors is large enough to hold the packed data.
 */
char *
log_pack_descriptors (int num_records, LOG_COPY * log_area, char *descriptors)
{
  struct manylogs *manylogs;
  struct onelog *onelog;
  int i;
  char *ptr;

  assert (log_area != NULL);
  assert (descriptors != NULL);

  manylogs = LOG_MANYLOGS_PTR_IN_LOGAREA (log_area);
  ptr = descriptors;
  for (i = 0, onelog = &manylogs->onelog; i < num_records; ++i, --onelog)
    {
      ptr = or_pack_int (ptr, onelog->rcvindex);
      ptr = or_pack_int (ptr, onelog->length);
      ptr = or_pack_int (ptr, onelog->offset);
    }

  return ptr;
}

/*
 * log_unpack_descriptors - Unpack descriptors for a log copy area
 *
 * return: updated pack area pointer
 *
 *   num_records(in): Number of records
 *   log_area(in): Copy area where records are placed
 *   descriptors(in): Packed descriptor array
 *
 * NOTE:Unpack the descriptors sent over the network and place them
 *              in the copy area.  The caller is responsible for determining
 *              that copy_area is large enough to hold the unpacked data.
 */
char *
log_unpack_descriptors (int num_records, LOG_COPY * log_area,
			char *descriptors)
{
  struct manylogs *manylogs;
  struct onelog *onelog;
  int i;
  char *ptr;

  assert (log_area != NULL);
  assert (descriptors != NULL);

  manylogs = LOG_MANYLOGS_PTR_IN_LOGAREA (log_area);
  manylogs->num_logs = num_records;
  ptr = descriptors;
  for (i = 0, onelog = &manylogs->onelog; i < num_records; ++i, --onelog)
    {
      ptr = or_unpack_int (ptr, &onelog->rcvindex);
      ptr = or_unpack_int (ptr, &onelog->length);
      ptr = or_unpack_int (ptr, &onelog->offset);
    }

  return ptr;
}

/*
 * log_copy_area_send - Find the active areas to be sent over the net
 *
 * return: number of records in the copy area
 *
 *   log_area(in): Log area
 *   contents_ptr(in/out): Pointer to content of records
 *   contents_length(in/out): Length of content area
 *   descriptors_ptr(in/out): Pointer to descriptor pointer array
 *   descriptors_length(in/out): Length of descriptor pointer array
 *
 * NOTE:Find the active areas (content and descriptor) to be sent over
 *              the network.
 *              The content is sent as is, but the descriptors are packed.
 *              The caller needs to free *descriptors_ptr.
 */
int
log_copy_area_send (LOG_COPY * log_area, char **contents_ptr,
		    int *contents_length, char **descriptors_ptr,
		    int *descriptors_length)
{
  struct manylogs *manylogs;
  struct onelog *onelog;
  int i;
  int offset = -1;

  assert (log_area != NULL);

  *contents_ptr = log_area->mem;
  *contents_length = 0;

  manylogs = LOG_MANYLOGS_PTR_IN_LOGAREA (log_area);
  *descriptors_length = DB_ALIGN (manylogs->num_logs * LOG_ONELOG_PACKED_SIZE,
				  MAX_ALIGNMENT);
  *descriptors_ptr = (char *) malloc (*descriptors_length);
  if (*descriptors_ptr == NULL)
    {
      *descriptors_length = 0;
      return 0;
    }

  /* Find the length of the content area and pack the descriptors */

  if (manylogs->num_logs > 0)
    {
      onelog = &manylogs->onelog;
      onelog++;
      for (i = 0; i < manylogs->num_logs; i++)
	{
	  onelog--;
	  if (onelog->offset > offset)
	    {
	      /* To the right */
	      *contents_length = onelog->length;
	      offset = onelog->offset;
	    }
	}

      if (offset != -1)
	{
	  *contents_length = DB_ALIGN (*contents_length, DOUBLE_ALIGNMENT);
	  *contents_length += offset;
	}
    }
  (void) log_pack_descriptors (manylogs->num_logs, log_area,
			       *descriptors_ptr);

  return manylogs->num_logs;

}

/*
 * log_copy_area_malloc_recv - ALLOCATE A COPY AREA FOR RECEIVING A LOG COPY
 *                            AREA FROM THE NET.
 *
 * return: copy_area or NULL(in case of error)
 *
 *   num_records(in): Number of records
 *   packed_descriptors(in/out): Pointer to packed descriptor array
 *   packed_descriptors_length(in): Length of packed descriptor array
 *   contents_ptr(in/out): Pointer to content of records
 *   contents_length(in): Length of content area
 *
 * NOTE:Prepare a copy area for receiving a log copy area of records
 *              send by either the client or server.
 */
LOG_COPY *
log_copy_area_malloc_recv (int num_records, char **packed_descriptors,
			   int packed_descriptors_length,
			   char **contents_ptr, int contents_length)
{
  LOG_COPY *log_area;
  int length;
  int descriptors_length;

  descriptors_length = (sizeof (struct manylogs) +
			sizeof (struct onelog) * (num_records - 1));
  length = contents_length + descriptors_length;
  log_area = log_alloc_client_copy_area (length);
  if (log_area == NULL)
    {
      *contents_ptr = NULL;
      return NULL;
    }

  *contents_ptr = log_area->mem;
  *packed_descriptors = (char *) malloc (packed_descriptors_length);
  if (*packed_descriptors == NULL)
    {
      log_free_client_copy_area (log_area);
      log_area = NULL;
      *contents_ptr = NULL;
    }

  return log_area;

}

/*
 * log_dump_log_info - Dump log information
 *
 * return: nothing
 *
 *   logname_info(in): Name of the log information file
 *   also_stdout(in):
 *   fmt(in): Format for the variable list of arguments (like printf)
 *   va_alist: Variable number of arguments
 *
 * NOTE:Dump some log information
 */
int
log_dump_log_info (const char *logname_info, bool also_stdout,
		   const char *fmt, ...)
{
  FILE *fp;			/* Pointer to file                   */
  va_list ap;			/* Point to each unnamed arg in turn */
  time_t log_time;
  struct tm log_tm;
  struct tm *log_tm_p = &log_tm;
  struct timeval tv;
  char time_array[128];
  char time_array_of_log_info[255];

  va_start (ap, fmt);

  if (logname_info == NULL)
    {
      return ER_FAILED;
    }

  fp = fopen (logname_info, "a");
  if (fp == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_LOG_MOUNT_FAIL, 1,
	      logname_info);
      va_end (ap);
      return ER_LOG_MOUNT_FAIL;
    }

  log_time = time (NULL);
#if defined (SERVER_MODE) && !defined (WINDOWS)
  log_tm_p = localtime_r (&log_time, &log_tm);
#else /* SERVER_MODE && !WINDOWS */
  log_tm_p = localtime (&log_time);
#endif /* SERVER_MODE && !WINDOWS */
  if (log_tm_p == NULL)
    {
      strcpy (time_array_of_log_info, "Time: 00/00/00 00:00:00.000 - ");
    }
  else
    {
      gettimeofday (&tv, NULL);
      strftime (time_array, 128, "%m/%d/%y %H:%M:%S", log_tm_p);
      snprintf (time_array_of_log_info, 255, "Time: %s.%03ld - ",
		time_array, tv.tv_usec / 1000);
    }

  if (strlen (time_array_of_log_info) != TIME_SIZE_OF_DUMP_LOG_INFO)
    {
      strcpy (time_array_of_log_info, "Time: 00/00/00 00:00:00.000 - ");
    }

  fprintf (fp, "%s", time_array_of_log_info);

  (void) vfprintf (fp, fmt, ap);
  fflush (fp);
  fclose (fp);

#if !defined(NDEBUG)
  if (also_stdout && PRM_LOG_TRACE_DEBUG)
    {
      va_start (ap, fmt);
      (void) vfprintf (stdout, fmt, ap);
    }
#endif

  va_end (ap);

  return NO_ERROR;
}
