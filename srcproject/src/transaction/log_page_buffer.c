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
 * log_page_buffer.c -
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#if defined(WINDOWS)
#include <io.h>
#else /* !WINDOWS */
#include <unistd.h>
#endif /* !WINDOWS */
#include <stdarg.h>

#if defined(SOLARIS)
#include <netdb.h>
#endif /* SOLARIS */

#if !defined(WINDOWS)
#include <sys/param.h>
#endif /* WINDOWS */

#include <assert.h>

#include "porting.h"
#include "connection_defs.h"
#include "thread_impl.h"
#include "log_impl.h"
#include "log_manager.h"
#include "log_comm.h"
#include "log_writer.h"
#include "lock_manager.h"
#include "boot_sr.h"
#if !defined(SERVER_MODE)
#include "boot_cl.h"
#include "transaction_cl.h"
#endif /* !SERVER_MODE */
#include "page_buffer.h"
#include "file_io.h"
#include "disk_manager.h"
#include "error_manager.h"
#include "xserver_interface.h"
#include "perf_monitor.h"
#include "storage_common.h"
#include "system_parameter.h"
#include "memory_alloc.h"
#include "memory_hash.h"
#include "release_string.h"
#include "message_catalog.h"
#include "environment_variable.h"
#include "util_func.h"
#include "errno.h"
#if defined(WINDOWS)
#include "wintcp.h"
#include "connection_error.h"
#else /* WINDOWS */
#include "tcp.h"
#endif /* WINDOWS */
#include "db.h"			/* for db_Connect_status */

#if !defined(SERVER_MODE)
#undef  MUTEX_INIT
#define MUTEX_INIT(a)
#undef  MUTEX_DESTROY
#define MUTEX_DESTROY(a)
#undef  MUTEX_LOCK
#define MUTEX_LOCK(a, b)
#undef  MUTEX_UNLOCK
#define MUTEX_UNLOCK(a)
#undef  COND_INIT
#define COND_INIT(a)
#undef  COND_BROADCAST
#define COND_BROADCAST(a)
#undef  COND_DESTROY
#define COND_DESTROY(a)
#endif /* !SERVER_MODE */

#define LOGPB_FIND_BUFPTR(bufid) log_Pb.pool[(bufid)]
#define LOGPB_FIND_NBUFFER_FROM_CONT_BUFFERS(setbufs, nbuf) \
  ((struct log_buffer *) ((SIZEOF_LOG_BUFFER * (nbuf)) + (char *)(setbufs)))

/* PAGES OF ACTIVE LOG PORTION */
#define LOGPB_HEADER_PAGE_ID             (-9)	/* The first log page in the infinite
						   log sequence. It is always kept
						   on the active portion of the log.
						   Log records are not stored on this
						   page. This page is backed up in
						   all archive logs
						 */
#define LOGPB_NEXT_ARCHIVE_PAGE_ID    (log_Gl.hdr.nxarv_pageid)
#define LOGPB_FIRST_ACTIVE_PAGE_ID    (log_Gl.hdr.fpageid)
#define LOGPB_LAST_ACTIVE_PAGE_ID     (log_Gl.hdr.nxarv_pageid +                 \
                                    log_Gl.hdr.npages - 1)
#define LOGPB_ACTIVE_NPAGES          (log_Gl.hdr.npages)


/*
 * TRANSLATING LOGICAL LOG PAGES (I.E., PAGES IN THE INFINITE LOG) TO PHYSICAL
 * PAGES IN THE CURRENT LOG FILE
 */
#define LOGPB_PHYSICAL_HEADER_PAGE_ID    0

#define LOGPB_IS_FIRST_PHYSICAL_PAGE(pageid) (logpb_to_physical_pageid(pageid) == 1)

/* ARCHIVE LOG PAGES */
#define LOGPB_IS_ARCHIVE_PAGE(pageid) \
  ((pageid) != LOGPB_HEADER_PAGE_ID && (pageid) < LOGPB_NEXT_ARCHIVE_PAGE_ID)
#define LOGPB_AT_NEXT_ARCHIVE_PAGE_ID(pageid)  \
  (logpb_to_physical_pageid(pageid) == log_Gl.hdr.nxarv_phy_pageid)

/* LOG BUFFER STRUCTURE */

/* WARNING:
 * Don't use sizeof(struct log_buffer) or of any structure that contains it
 * Use macro SIZEOF_LOG_BUFFER instead.
 * It is also bad idea to create a variable for this on the stack.
 */
typedef struct log_buffer LOG_BUFFER;
struct log_buffer
{
  PAGEID pageid;		/* Logical page of the log. (Page identifier of
				 * the infinite log)
				 */
  PAGEID phy_pageid;		/* Physical pageid for the active log portion */
  int fcnt;			/* Fix count */
  int ipool;			/* Buffer pool index. Used to optimize the Clock
				 * algorithm and to find the address of buffer given
				 * the page address
				 */
  bool dirty;			/* Is page dirty */
  bool recently_freed;		/* Reference value 0/1 used by the clock
				 * algorithm
				 */
  bool flush_running;		/* Is page beging flushed ? */

  int dummy_for_align;		/* Dummy field for 8byte alignment of log page */
  LOG_PAGE logpage;		/* The actual buffered log page */
};

struct log_bufarea
{				/* A buffer area */
  struct log_buffer *bufarea;
  struct log_bufarea *next;
};

/* callback function for scan pages to flush */
typedef void (*log_buffer_apply_func) (struct log_buffer * bufptr);

/* Global structure to trantable, log buffer pool, etc   */
typedef struct log_pb_global_data LOG_PB_GLOBAL_DATA;
struct log_pb_global_data
{
  MHT_TABLE *ht;		/* Hash table from logical pageid
				   to log buffer
				 */
  struct log_bufarea *poolarea;	/* Contiguous area of buffers */
  struct log_buffer **pool;	/* Log buffer pool           */
  int num_buffers;		/* Number of log buffers     */
  int clock_hand;		/* Clock hand                */
#if defined(SERVER_MODE)
  MUTEX_T lpb_mutex;
#if defined(CUBRID_DEBUG)
  volatile log_mutex_info mutex_info;
#endif				/* CUBRID_DEBUG */
#endif				/* SERVER_MODE */
};

#define SIZEOF_LOG_BUFFER \
  (offsetof(struct log_buffer, logpage) + LOG_PAGESIZE)

#define LOG_CAST_LOGPAGEPTR_TO_BFPTR(bufptr, log_pgptr)                       \
  ((bufptr) = ((struct log_buffer *)                                          \
              ((char *)log_pgptr - offsetof(struct log_buffer, logpage))))

static const int LOG_BKUP_HASH_NUM_PAGEIDS = 1000;
/* MIN AND MAX BUFFERS */
#define LOG_MAX_NUM_CONTIGUOUS_BUFFERS \
  ((unsigned int)(INT_MAX / (5 * SIZEOF_LOG_BUFFER)))

#define LOG_MAX_LOGINFO_LINE (PATH_MAX * 4)

/* skip prompting for archive log location */
int log_default_input_for_archive_log_location = -1;

LOG_PB_GLOBAL_DATA log_Pb = {
  NULL, NULL, NULL, 0, 0
#if defined(SERVER_MODE)
    , MUTEX_INITIALIZER
#if defined(CUBRID_DEBUG)
    ,
  {
   false, NULL_THREAD_T}
#endif /* CUBRID_DEBUG */
#endif /* SERVER_MODE */
};

LOG_LOGGING_STAT log_Stat;

/*
 * Functions
 */

static int logpb_expand_pool (int num_new_buffers);
static struct log_buffer *logpb_replace (THREAD_ENTRY * thread_p,
					 bool * retry);
static LOG_PAGE *logpb_fix_page (THREAD_ENTRY * thread_p, PAGEID pageid,
				 int fetch_mode);
static bool logpb_is_dirty (LOG_PAGE * log_pgptr);
#if !defined(NDEBUG)
static bool logpb_is_any_dirty (void);
#endif /* !NDEBUG */
#if defined(CUBRID_DEBUG)
static bool logpb_is_any_fix (void);
#endif /* CUBRID_DEBUG */
static void logpb_dump_information (FILE * out_fp);
static void logpb_dump_to_flush_page (FILE * out_fp);
static void logpb_dump_pages (FILE * out_fp);
static void logpb_initialize_backup_info (void);
static LOG_PAGE **logpb_writev_append_pages (THREAD_ENTRY * thread_p,
					     LOG_PAGE ** to_flush,
					     DKNPAGES npages);
static int logpb_get_guess_archive_num (PAGEID pageid);
static void logpb_set_unavailable_archive (int arv_num);
static bool logpb_is_archive_available (int arv_num);
static void logpb_archive_active_log (THREAD_ENTRY * thread_p,
				      bool force_archive);
static void logpb_remove_archive_to_page_id (THREAD_ENTRY * thread_p,
					     const char *info_reason,
					     PAGEID safe_pageid);
static int logpb_get_remove_archive_num (THREAD_ENTRY * thread_p,
					 PAGEID safe_pageid, int archive_num);
static void
logpb_remove_archive_logs_internal (THREAD_ENTRY * thread_p,
				    int first, int last,
				    const char *info_reason,
				    bool check_backup,
				    int lowest_arv_num_for_backup,
				    int highest_arv_num_for_backup);
static void logpb_append_archives_removed_to_log_info (int first, int last,
						       const char
						       *info_reason);
static void logpb_append_archives_delete_pend_to_log_info (int first,
							   int last);
static int logpb_verify_length (const char *db_fullname, const char *log_path,
				const char *log_prefix);
static int logpb_backup_for_volume (THREAD_ENTRY * thread_p, VOLID volid,
				    LOG_LSA * chkpt_lsa,
				    FILEIO_BACKUP_SESSION * session,
				    bool only_updated);
static int logpb_update_backup_volume_info (const char *bkupinfo_file_name);
static int logpb_start_where_path (const char *to_db_fullname,
				   const char *toext_path,
				   const char **toext_name,
				   char **ext_path,
				   char **alloc_extpath,
				   const char *fileof_vols_and_wherepaths,
				   FILE ** where_paths_fp);
static int
logpb_next_where_path (const char *to_db_fullname, const char *toext_path,
		       const char *ext_name, char *ext_path,
		       const char *fileof_vols_and_wherepaths,
		       FILE * where_paths_fp, int num_perm_vols,
		       VOLID volid, char *from_volname, char *to_volname);
static int logpb_copy_volume (THREAD_ENTRY * thread_p, VOLID from_volid,
			      const char *tonew_volname,
			      INT64 * db_creation, LOG_LSA * vol_chkpt_lsa);
static bool logpb_check_if_exists (const char *fname, char *first_vol);
#if defined(SERVER_MODE)
static int
logpb_backup_needed_archive_logs (THREAD_ENTRY * thread_p,
				  FILEIO_BACKUP_SESSION * session,
				  int first_arv_num, int last_arv_num);
#endif /* SERVER_MODE */
static bool logpb_remote_ask_user_before_delete_volumes (THREAD_ENTRY *
							 thread_p,
							 const char *volpath);
static int logpb_must_archive_last_log_page (THREAD_ENTRY * thread_p);
static int logpb_initialize_flush_info (void);
static void logpb_finalize_flush_info (void);
static void logpb_finalize_writer_info (void);
static void logpb_dump_log_header (FILE * outfp);
static void logpb_dump_parameter (FILE * outfp);
static void logpb_dump_runtime (FILE * outfp);
static void logpb_reset_clock_hand (int buffer_index);
static void logpb_move_next_clock_hand (void);
static void logpb_unfix_page (struct log_buffer *bufptr);
static void logpb_initialize_log_buffer (LOG_BUFFER * log_buffer_p);

static int logpb_check_stop_at_time (FILEIO_BACKUP_SESSION * session,
				     time_t stop_at, time_t backup_time);
static void logpb_write_toflush_pages_to_archive (THREAD_ENTRY * thread_p);

#if 0
static FILEIO_BACKUP_LEVEL log_find_most_recent_backup_level (void);
#endif



/*
 * FUNCTIONS RELATED TO LOG BUFFERING
 *
 */

/*
 * logpb_reset_clock_hand - reset clock hand of log page buffer
 *
 * return: nothing
 *
 * NOTE:
 *
 */
static void
logpb_reset_clock_hand (int buffer_index)
{
  if (buffer_index >= log_Pb.num_buffers || buffer_index < 0)
    {
      log_Pb.clock_hand = 0;
    }
  else
    {
      log_Pb.clock_hand = buffer_index;
    }
}

/*
 * logpb_move_next_clock_hand - move next clock hand of log page buffer
 *
 * return: nothing
 *
 * NOTE:
 *
 */
static void
logpb_move_next_clock_hand (void)
{
  log_Pb.clock_hand++;
  if (log_Pb.clock_hand >= log_Pb.num_buffers)
    {
      log_Pb.clock_hand = 0;
    }
}

/*
 * logpb_unfix_page - unfix of log page buffer
 *
 * return: nothing
 *
 *   bufptr(in/oiut):
 *
 * NOTE:
 *
 */
static void
logpb_unfix_page (struct log_buffer *bufptr)
{
  bufptr->fcnt--;
  if (bufptr->fcnt < 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_FREEING_TOO_MUCH, 0);
      bufptr->fcnt = 0;
    }

  bufptr->recently_freed = true;
}

/*
 * logpb_initialize_log_buffer -
 *
 * return: nothing
 *
 *   log_buffer_p(in/oiut):
 *
 * NOTE:
 *
 */
static void
logpb_initialize_log_buffer (LOG_BUFFER * log_buffer_p)
{
  log_buffer_p->pageid = NULL_PAGEID;
  log_buffer_p->phy_pageid = NULL_PAGEID;
  log_buffer_p->fcnt = 0;
  log_buffer_p->recently_freed = false;
  log_buffer_p->dirty = false;
  log_buffer_p->flush_running = false;
  /*
   * Scramble the content of buffers. This is done for debugging
   * reasons to make sure that a user of a buffer does not assume
   * that buffers are initialized to zero. For safty reasons, the
   * buffers are initialized to zero, instead of scrambled, when
   * not in debugging mode.
   */
  MEM_REGION_INIT (&log_buffer_p->logpage, LOG_PAGESIZE);
  log_buffer_p->logpage.hdr.logical_pageid = NULL_PAGEID;
  log_buffer_p->logpage.hdr.offset = NULL_OFFSET;
}

/*
 * TODO : STL
 * logpb_expand_pool - Expand log buffer pool
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   num_new_buffers(in): Number of new buffers (expansion) or -1.
 *
 * NOTE:Expand the log buffer pool with the given number of buffers.
 *              If a zero or a negative value is given, the function expands
 *              the buffer pool with a default porcentage of the currently
 *              size.
 */
static int
logpb_expand_pool (int num_new_buffers)
{
  struct log_buffer *log_bufptr;	/* Pointer to array of buffers */
  struct log_bufarea *area;	/* Contiguous area for buffers */
  int bufid, i;			/* Buffer index                */
  int total_buffers;		/* Total num buffers           */
  int size;			/* Size of area to allocate    */
  float expand_rate;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
  struct log_buffer **buffer_pool;
  int error_code = NO_ERROR;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  assert (LOG_CS_OWN (NULL));

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

  if (num_new_buffers <= 0)
    {
      /*
       * Calculate a default expansion for the buffer pool.
       */
      if (log_Pb.num_buffers > 0)
	{
	  if (log_Pb.num_buffers > 100)
	    {
	      expand_rate = 0.10f;
	    }
	  else
	    {
	      expand_rate = 0.20f;
	    }
	  num_new_buffers = (int) (((float) log_Pb.num_buffers * expand_rate)
				   + 0.9);
#if defined(CUBRID_DEBUG)
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_BUFFER_POOL_TOO_SMALL, 2, log_Pb.num_buffers,
		  num_new_buffers);
#endif /* CUBRID_DEBUG */
	}
      else
	{
	  num_new_buffers = PRM_LOG_NBUFFERS;
	  if (num_new_buffers < LOG_MIN_NBUFFERS)
	    {
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_NBUFFERS_TOO_SMALL, 2, num_new_buffers,
		      LOG_MIN_NBUFFERS);
	      num_new_buffers = LOG_MIN_NBUFFERS;
	    }
	}
    }

  while ((unsigned int) num_new_buffers > LOG_MAX_NUM_CONTIGUOUS_BUFFERS)
    {
      /* Note that we control overflow of size in this way */
      LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
      error_code = logpb_expand_pool (LOG_MAX_NUM_CONTIGUOUS_BUFFERS);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
      LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);
      num_new_buffers -= LOG_MAX_NUM_CONTIGUOUS_BUFFERS;
    }

  if (num_new_buffers > 0)
    {
      total_buffers = log_Pb.num_buffers + num_new_buffers;

      /*
       * Allocate an area for the buffers, set the address of each buffer
       * and keep the address of the buffer area for deallocation purposes at a
       * later time.
       */
      size = ((num_new_buffers * SIZEOF_LOG_BUFFER)
	      + sizeof (struct log_bufarea));

      area = (struct log_bufarea *) malloc (size);
      if (area == NULL)
	{
	  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, size);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      /* allocate a pointer array to point to each buffer */
      buffer_pool = (struct log_buffer **) realloc (log_Pb.pool,
						    total_buffers *
						    sizeof (*log_Pb.pool));
      if (buffer_pool == NULL)
	{
	  free_and_init (area);

	  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, total_buffers * sizeof (*log_Pb.pool));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      memset (area, 1, size);

      area->bufarea = ((struct log_buffer *) ((char *) area +
					      sizeof (struct log_bufarea)));
      area->next = log_Pb.poolarea;

      /* Initialize every new buffer */
      for (i = 0, bufid = log_Pb.num_buffers; i < num_new_buffers;
	   bufid++, i++)
	{
	  log_bufptr = LOGPB_FIND_NBUFFER_FROM_CONT_BUFFERS (area->bufarea,
							     i);
	  buffer_pool[bufid] = log_bufptr;
	  logpb_initialize_log_buffer (log_bufptr);
	  log_bufptr->ipool = bufid;
	}

      log_Pb.pool = buffer_pool;
      logpb_reset_clock_hand (log_Pb.num_buffers);
      log_Pb.poolarea = area;
      log_Pb.num_buffers = total_buffers;
    }

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

  log_Stat.log_buffer_expand_count++;

  return NO_ERROR;
}

/*
 * logpb_initialize_pool - Initialize the log buffer pool
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 * NOTE:Initialize the log buffer pool. All resident pages are
 *              invalidated.
 */
int
logpb_initialize_pool (THREAD_ENTRY * thread_p)
{
  int error_code = NO_ERROR;
  LOG_GROUP_COMMIT_INFO *group_commit_info = &log_Gl.group_commit_info;
  LOGWR_INFO *writer_info = &log_Gl.writer_info;

  assert (LOG_CS_OWN (thread_p));

  if (log_Pb.pool != NULL)
    {
      logpb_finalize_pool ();
    }

  assert (log_Pb.pool == NULL && log_Pb.poolarea == NULL
	  && log_Pb.ht == NULL);

  log_Pb.num_buffers = 0;
  log_Pb.pool = NULL;
  log_Pb.poolarea = NULL;

  MUTEX_INIT (log_Pb.lpb_mutex);

  /*
   * Create an area to keep the number of desired buffers
   */
  error_code = logpb_expand_pool (-1);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  log_Pb.ht = mht_create ("Log buffer pool hash table",
			  log_Pb.num_buffers * 8, mht_numhash, mht_numcmpeq);
  if (log_Pb.ht == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error;
    }

  error_code = logpb_initialize_flush_info ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  MUTEX_INIT (log_Gl.chkpt_lsa_lock);

  COND_INIT (group_commit_info->gc_cond);
  MUTEX_INIT (group_commit_info->gc_mutex);

  MUTEX_INIT (writer_info->wr_list_mutex);

  COND_INIT (writer_info->flush_start_cond);
  MUTEX_INIT (writer_info->flush_start_mutex);

  COND_INIT (writer_info->flush_end_cond);
  MUTEX_INIT (writer_info->flush_end_mutex);

  return error_code;

error:

  logpb_finalize_pool ();
  logpb_fatal_error (thread_p, false, ARG_FILE_LINE, "log_pbpool_init");

  return error_code;
}

/*
 * logpb_finalize_pool - TERMINATES THE LOG BUFFER POOL
 *
 * return: nothing
 *
 * NOTE:Terminate the log buffer pool. All log resident pages are
 *              invalidated.
 */
void
logpb_finalize_pool (void)
{
  struct log_bufarea *area;	/* Buffer area to free */
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  assert (LOG_CS_OWN (NULL));

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);
  if (log_Pb.pool != NULL)
    {
      if (log_Gl.append.log_pgptr != NULL)
	{
	  logpb_free_without_mutex (log_Gl.append.log_pgptr);
	  log_Gl.append.log_pgptr = NULL;
	  log_Gl.append.delayed_free_log_pgptr = NULL;
	}
      LSA_SET_NULL (&log_Gl.append.nxio_lsa);
      LSA_SET_NULL (&log_Gl.append.prev_lsa);

#if defined(CUBRID_DEBUG)
      if (logpb_is_any_dirty () == true || logpb_is_any_fix () == true)
	{
	  er_log_debug (ARG_FILE_LINE, "log_pbpool_final: Log Buffer"
			" pool contains dirty or fixed pages at the end.\n");
	  logpb_dump (stdout);
	}
#endif /* CUBRID_DEBUG */

      /*
       * Remove hash table
       */
      if (log_Pb.ht != NULL)
	{
	  mht_destroy (log_Pb.ht);
	  log_Pb.ht = NULL;
	}

      free_and_init (log_Pb.pool);
      log_Pb.num_buffers = 0;
    }

  /*
   * Remove all the buffer pool areas
   */
  while ((area = log_Pb.poolarea) != NULL)
    {
      log_Pb.poolarea = area->next;
      free_and_init (area);
    }

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

  MUTEX_DESTROY (log_Pb.lpb_mutex);

  logpb_finalize_flush_info ();

  MUTEX_DESTROY (log_Gl.chkpt_lsa_lock);

  MUTEX_DESTROY (log_Gl.group_commit_info.gc_mutex);
  COND_DESTROY (log_Gl.group_commit_info.gc_cond);

  logpb_finalize_writer_info ();
}

/*
 * logpb_is_initialize_pool - Find out if buffer pool has been initialized
 *
 * return:
 *
 * NOTE:Find out if the buffer pool has been initialized.
 */
bool
logpb_is_initialize_pool (void)
{
  assert (LOG_CS_OWN (NULL));
  if (log_Pb.pool != NULL)
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * logpb_invalidate_pool - Invalidate all buffers in buffer pool
 *
 * return: Pointer to the page or NULL
 *
 * NOTE:Invalidate all unfixed buffers in the buffer pool.
 *              This is needed when we reset the log header information.
 */
void
logpb_invalidate_pool (THREAD_ENTRY * thread_p)
{
  register struct log_buffer *log_bufptr;	/* A log buffer */
  int i;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  assert (LOG_CS_OWN (thread_p));

  if (log_Pb.pool == NULL)
    {
      return;
    }

  /*
   * Flush any append dirty buffers at this moment.
   * Then, invalidate any buffer that it is not fixed and dirty
   */
  logpb_force (thread_p);

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

  for (i = 0; i < log_Pb.num_buffers; i++)
    {
      log_bufptr = LOGPB_FIND_BUFPTR (i);
      if (log_bufptr->fcnt <= 0 && log_bufptr->dirty == false)
	{
	  (void) mht_rem (log_Pb.ht, &log_bufptr->pageid, NULL, NULL);
	  logpb_initialize_log_buffer (log_bufptr);
	  logpb_reset_clock_hand (log_bufptr->ipool);
	}
    }

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
}

/*
 * logpb_replace - Find a page to replace
 *
 * return: struct log_buffer *
 *                       retry : true if need to retry, false if not
 *
 *   retry(in/out): true if need to retry, false if not
 *
 */
static struct log_buffer *
logpb_replace (THREAD_ENTRY * thread_p, bool * retry)
{
  register struct log_buffer *log_bufptr = NULL;	/* A log buffer */
  int bufid;
  int ixpool = -1;
  int num_unfixed = 1;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
  int error_code = NO_ERROR;

  assert (retry != NULL);

  *retry = false;

  num_unfixed = log_Pb.num_buffers;

  while (ixpool == -1 && num_unfixed > 0)
    {
      /* Recalculate the num_unfixed to avoid infinite loops in case of error */
      num_unfixed = 0;

      for (bufid = 0; bufid < log_Pb.num_buffers; bufid++)
	{
	  log_bufptr = LOGPB_FIND_BUFPTR (log_Pb.clock_hand);
	  logpb_move_next_clock_hand ();
	  /*
	   * Can we replace the current buffer. That is, is it unfixed ?
	   */
	  if ((log_bufptr->fcnt <= 0) && (!log_bufptr->flush_running))
	    {
	      num_unfixed++;
	      /*
	       * Has this buffer recently been freed ?
	       */
	      if (log_bufptr->recently_freed)
		{
		  /* Set it to false for new cycle */
		  log_bufptr->recently_freed = false;
		}
	      else
		{
		  /*
		   * Replace current buffer.
		   * Force the log if the current page is dirty.
		   */
		  ixpool = log_bufptr->ipool;
		  if (log_bufptr->dirty == true)
		    {

		      LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

		      assert (LOG_CS_OWN (thread_p));
		      log_Stat.log_buffer_flush_count_by_replacement++;
		      logpb_flush_all_append_pages (thread_p,
						    LOG_FLUSH_DIRECT);

		      LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

		      *retry = true;
		      return NULL;
		    }
		  break;
		}
	    }
	}
    }

  assert (log_bufptr != NULL);

  /*
   * The page is not resident or we are requesting a buffer for working
   * purposes.
   */

  if (ixpool == -1)
    {
      /*
       * All log buffers are fixed. There are many concurrent transactions being
       * aborted at the same time or it is likely that there is a bug in the
       * system (e.g., buffer are not being freed).
       */
      LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

      error_code = logpb_expand_pool (-1);

      if (error_code != NO_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_ALL_BUFFERS_FIXED, 0);
	  error_code = ER_LOG_ALL_BUFFERS_FIXED;
	  *retry = false;
	}
      else
	{
	  *retry = true;
	}
      log_bufptr = NULL;

      LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);
    }

  return log_bufptr;
}

/*
 * logpb_create - Create a log page on a log buffer
 *
 * return: Pointer to the page or NULL
 *
 *   pageid(in): Page identifier
 *
 * NOTE:Creates the log page identified by pageid on a log buffer and
 *              return such buffer.
 *              Just intializes log buffer hdr,
 *              To read a page from disk is not needed.
 */
LOG_PAGE *
logpb_create (THREAD_ENTRY * thread_p, PAGEID pageid)
{
  return logpb_fix_page (thread_p, pageid, NEW_PAGE);
}

/*
 * logpb_fix_page - Fetch a log page
 *
 * return: Pointer to the page or NULL
 *
 *   pageid(in): Page identifier
 *   fetch_mode(in): Is this a new log page ?. That is, can we avoid the I/O
 *
 * NOTE:Fetch the log page identified by pageid into a log buffer and
 *              return such buffer. The page is guaranteed to stay cached in
 *              the buffer until the page is released by the caller.
 *              If the page is not resident in the log buffer pool, the oldest
 *              log page that is not fixed is replaced. The rationale for this
 *              is that if we need to go back to rollback other transactions,
 *              it is likely that we need the earliest log pages and not the
 *              oldest one. A very old page may be in the log buffer pool as a
 *              consequence of a previous rollback. This does not provide
 *              problems to the restart recovery since we hardly go back to
 *              the same page.
 */
static LOG_PAGE *
logpb_fix_page (THREAD_ENTRY * thread_p, PAGEID pageid, int fetch_mode)
{
  struct log_buffer *log_bufptr;	/* A log buffer */
  PAGEID phy_pageid = NULL_PAGEID;	/* The corresponding
					 * physical page
					 */
  bool retry;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  assert (pageid != NULL_PAGEID);
  assert ((fetch_mode == NEW_PAGE) || (fetch_mode == OLD_PAGE));
  assert (LOG_CS_OWN (thread_p));

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

  log_bufptr = (struct log_buffer *) mht_get (log_Pb.ht, &pageid);
  if (log_bufptr == NULL)
    {
      /*
       * Page is not resident. Find a replacement buffer for it
       */

      while (1)
	{
	  log_bufptr = logpb_replace (thread_p, &retry);
	  if (log_bufptr == NULL)
	    {
	      if (retry)
		{
		  continue;
		}
	      else
		{
		  goto error;
		}
	    }
	  else
	    {
	      break;
	    }
	}

      assert (log_bufptr != NULL);

      /*
       * Remove the current page from the hash table and get the desired
       * page from disk when the page is an old page
       */
      if (log_bufptr->pageid != NULL_PAGEID)
	{
	  (void) mht_rem (log_Pb.ht, &log_bufptr->pageid, NULL, NULL);
	}

      /* Fix the page and mark its pageid invalid */
      log_bufptr->pageid = NULL_PAGEID;
      log_bufptr->fcnt++;

      phy_pageid = logpb_to_physical_pageid (pageid);

      if (fetch_mode == NEW_PAGE)
	{
	  log_bufptr->logpage.hdr.logical_pageid = pageid;
	  log_bufptr->logpage.hdr.offset = NULL_OFFSET;
	}
      else
	{
	  if (logpb_read_page_from_file (thread_p, pageid,
					 &log_bufptr->logpage) == NULL)
	    {
	      goto error;
	    }
	}

      /* Recall the page in the buffer pool, and hash the identifer */
      log_bufptr->pageid = pageid;
      log_bufptr->phy_pageid = phy_pageid;

      if (mht_put (log_Pb.ht, &log_bufptr->pageid, log_bufptr) == NULL)
	{
	  logpb_initialize_log_buffer (log_bufptr);
	  log_bufptr = NULL;
	}
    }
  else
    {
      log_bufptr->fcnt++;
    }

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

  assert (log_bufptr != NULL);

  /* for debugging in release mode  */
  if (log_bufptr == NULL)
    {
      return NULL;
    }

  assert (((UINTPTR) (log_bufptr->logpage.area) % 8) == 0);
  return &(log_bufptr->logpage);

error:

  if (log_bufptr != NULL)
    {
      logpb_initialize_log_buffer (log_bufptr);
      logpb_reset_clock_hand (log_bufptr->ipool);
    }
  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

  return NULL;
}

/*
 * logpb_set_dirty - Mark the current page dirty
 *
 * return: nothing
 *
 *   log_pgptr(in): Log page pointer
 *   free_page(in): Free the page too ? Valid values:  FREE, DONT_FREE
 *
 * NOTE:Mark the current log page as dirty and optionally free the
 *              page.
 */
void
logpb_set_dirty (LOG_PAGE * log_pgptr, int free_page)
{
  struct log_buffer *bufptr;	/* Log buffer associated with given page */
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  /* Get the address of the buffer from the page. */
  LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, log_pgptr);

#if defined(CUBRID_DEBUG)
  if (bufptr->pageid != LOGPB_HEADER_PAGE_ID
      && (bufptr->pageid < LOGPB_NEXT_ARCHIVE_PAGE_ID
	  || bufptr->pageid > LOGPB_LAST_ACTIVE_PAGE_ID))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_LOG_FLUSHING_UNUPDATABLE,
	      1, bufptr->pageid);
    }
#endif /* CUBRID_DEBUG */

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

  bufptr->dirty = true;
  if (free_page == FREE)
    {
      logpb_unfix_page (bufptr);
    }

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
}

/*
 * logpb_is_dirty - Find if current log page pointer is dirty
 *
 * return:
 *
 *   log_pgptr(in): Log page pointer
 *
 * NOTE:Find if the current log page is dirty.
 */
static bool
logpb_is_dirty (LOG_PAGE * log_pgptr)
{
  struct log_buffer *bufptr;	/* Log buffer associated with given page */
  bool is_dirty;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  /* Get the address of the buffer from the page. */
  LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, log_pgptr);

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

  is_dirty = (bool) bufptr->dirty;

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

  return is_dirty;
}

#if !defined(NDEBUG)
/*
 * logpb_is_any_dirty - FIND IF ANY LOG BUFFER IS DIRTY
 *
 * return:
 *
 * NOTE:Find if any log buffer is dirty.
 */
static bool
logpb_is_any_dirty (void)
{
  register struct log_buffer *bufptr;	/* A log buffer */
  int i;
  bool ret;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);
  ret = false;
  for (i = 0; i < log_Pb.num_buffers; i++)
    {
      bufptr = LOGPB_FIND_BUFPTR (i);
      if (bufptr->dirty == true)
	{
	  ret = true;
	  break;
	}
    }

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
  return ret;
}
#endif /* !NDEBUG || CUBRID_DEBUG */

#if defined(CUBRID_DEBUG)
/*
 * logpb_is_any_fix - Find if any log buffer is fixed
 *
 * return:
 *
 * NOTE:Find if any buffer is fixed
 */
static bool
logpb_is_any_fix (void)
{
  register struct log_buffer *bufptr;	/* A log buffer */
  int i;
  bool ret;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);
  ret = false;
  for (i = 0; i < log_Pb.num_buffers; i++)
    {
      bufptr = LOGPB_FIND_BUFPTR (i);
      if (bufptr->pageid != NULL_PAGEID && bufptr->fcnt != 0)
	{
	  ret = true;
	  break;
	}
    }
  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

  return ret;
}
#endif /* CUBRID_DEBUG */

/*
 * logpb_flush_page - Flush a page of the active portion of the log to disk
 *
 * return: nothing
 *
 *   log_pgptr(in): Log page pointer
 *   free_page(in): Free the page too ? Valid values:  FREE, DONT_FREE
 *
 * NOTE:The log page (of the active portion of the log) associated
 *              with pageptr is written out to disk and is optionally freed.
 */
int
logpb_flush_page (THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr,
		  int free_page)
{
  struct log_buffer *bufptr;	/* Log buffer associated with given page */
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  /* Get the address of the buffer from the page. */
  LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, log_pgptr);

  assert (LOG_CS_OWN (thread_p));

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

#if defined(CUBRID_DEBUG)
  if (bufptr->pageid != LOGPB_HEADER_PAGE_ID
      && (bufptr->pageid < LOGPB_NEXT_ARCHIVE_PAGE_ID
	  || bufptr->pageid > LOGPB_LAST_ACTIVE_PAGE_ID))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_LOG_FLUSHING_UNUPDATABLE,
	      1, bufptr->pageid);
      return ER_LOG_FLUSHING_UNUPDATABLE;
    }
  if (bufptr->phy_pageid == NULL_PAGEID
      || bufptr->phy_pageid != logpb_to_physical_pageid (bufptr->pageid))
    {
      /* Bad physical log page for such logical page */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_PAGE_CORRUPTED,
	      1, bufptr->pageid);
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "logpb_flush_page");
      return ER_LOG_PAGE_CORRUPTED;
    }
#endif /* CUBRID_DEBUG */

  if (bufptr->dirty == true)
    {
      /*
       * The buffer is dirty, flush it
       */

      /* Record number of writes in statistics */
      mnt_log_iowrites (thread_p);

      /*
       * Even when the log has been open with the o_sync option, force a sync
       * since some Operationg system (HP) seems that does not have the effect
       * of forcing the page to disk without doing fync
       */

      if (logpb_write_page_to_disk (thread_p, log_pgptr, bufptr->pageid) ==
	  NULL)
	{
	  goto error;
	}
      else
	{
	  bufptr->dirty = false;
	}
    }

  if (free_page == FREE)
    {
      logpb_unfix_page (bufptr);
    }

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

  return NO_ERROR;

error:
  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

  return ER_FAILED;
}

/*
 * logpb_free_page - Free a log page
 *
 * return: nothing
 *
 *   log_pgptr(in):Log page pointer
 *
 * NOTE:Free the log buffer where the page associated with log_pgptr
 *              resides. The page is subject to replacement, if not fixed by
 *              other thread of execution.
 */
void
logpb_free_page (LOG_PAGE * log_pgptr)
{
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

  logpb_free_without_mutex (log_pgptr);

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
}

/*
 * logpb_free_without_mutex - Free a log page without mutex
 *
 * return: nothing
 *
 *   log_pgptr(in): Log page pointer
 *
 * NOTE:see logpb_free_page
 */
void
logpb_free_without_mutex (LOG_PAGE * log_pgptr)
{
  struct log_buffer *bufptr;	/* Log buffer associated with given page */

  /* Get the address of the buffer from the page. */
  LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, log_pgptr);

  if (bufptr->pageid == NULL_PAGEID)
    {
      /*
       * Freeing a buffer used as working area. This buffer can be used for
       * replacement immediately.
       */
      logpb_reset_clock_hand (bufptr->ipool);
    }

  logpb_unfix_page (bufptr);
}

/*
 * logpb_get_page_id - Logical pageid of log buffer/page
 *
 * return: pageid
 *
 *   log_pgptr(in): Log page pointer
 *
 * NOTE:The page identifier of the given log page/buffer.
 *              The page is always fix when this funtion is called.
 *              In replacement, the page cannot be replaced because fix > 0.
 *              So, it isn't needed to lock mutex.
 */
PAGEID
logpb_get_page_id (LOG_PAGE * log_pgptr)
{
  struct log_buffer *bufptr;	/* Log buffer associated with given page */

  LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, log_pgptr);

  assert (bufptr->fcnt > 0);
  return bufptr->pageid;
}

/*
 * logpb_dump - DUMP THE LOG PAGE BUFFER POOL
 *
 * return: nothing
 *
 * NOTE:Dump the log page buffer pool. This function is used for
 *              debugging purposes.
 */
void
logpb_dump (FILE * out_fp)
{
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

  if (log_Pb.pool == NULL)
    {
      return;
    }

  logpb_dump_information (out_fp);

  if (log_Gl.flush_info.num_toflush > 0)
    {
      logpb_dump_to_flush_page (out_fp);
    }

  (void) fprintf (out_fp, "\n\n");
  (void) fprintf (out_fp, "Buf Log_Pageid Phy_pageid Drt Rct Fcnt Bufaddr"
		  "   Pagearea    HDR:Pageid offset\n");

  logpb_dump_pages (out_fp);

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
}

/*
 * logpb_dump_information -
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_dump_information (FILE * out_fp)
{
  (void) fprintf (out_fp,
		  "\n\n ** DUMP OF LOG BUFFER POOL INFORMATION **\n\n");

  (void) fprintf (out_fp, "\nHash table dump\n");
  (void) mht_dump (out_fp, log_Pb.ht, false, logpb_print_hash_entry, NULL);
  (void) fprintf (out_fp, "\n\n");

  (void) fprintf (out_fp,
		  " Next IO_LSA = %d|%d, Current append LSA = %d|%d,\n"
		  " Prev append LSA = %d|%d\n\n",
		  log_Gl.append.nxio_lsa.pageid,
		  log_Gl.append.nxio_lsa.offset, log_Gl.hdr.append_lsa.pageid,
		  log_Gl.hdr.append_lsa.offset, log_Gl.append.prev_lsa.pageid,
		  log_Gl.append.prev_lsa.offset);

  (void) fprintf (out_fp,
		  " Append to_flush array: max = %d, num_active = %d\n"
		  " Delayed free page = %d, Current append page = %d\n",
		  log_Gl.flush_info.max_toflush,
		  log_Gl.flush_info.num_toflush,
		  ((log_Gl.append.delayed_free_log_pgptr ==
		    NULL) ? NULL_PAGEID : logpb_get_page_id (log_Gl.append.
							     delayed_free_log_pgptr)),
		  ((log_Gl.append.log_pgptr ==
		    NULL) ? NULL_PAGEID : logpb_get_page_id (log_Gl.append.
							     log_pgptr)));
}

/*
 * logpb_dump_to_flush_page -
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_dump_to_flush_page (FILE * out_fp)
{
  int i;
  struct log_buffer *log_bufptr;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  (void) fprintf (out_fp, " Candidate append pages to flush are:\n");

  for (i = 0; i < flush_info->num_toflush; i++)
    {
      LOG_CAST_LOGPAGEPTR_TO_BFPTR (log_bufptr, flush_info->toflush[i]);
      if (i != 0)
	{
	  if ((i % 10) == 0)
	    {
	      fprintf (out_fp, ",\n");
	    }
	  else
	    {
	      fprintf (out_fp, ",");
	    }
	}
      fprintf (out_fp, " %4d", log_bufptr->pageid);
    }

  fprintf (out_fp, "\n");
}

/*
 * logpb_dump_pages -
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_dump_pages (FILE * out_fp)
{
  int i;
  struct log_buffer *log_bufptr;

  for (i = 0; i < log_Pb.num_buffers; i++)
    {
      log_bufptr = LOGPB_FIND_BUFPTR (i);
      if (log_bufptr->pageid == NULL_PAGEID && log_bufptr->fcnt <= 0)
	{
	  /* ***
	   *** (void)fprintf(stdout, "%3d ..\n", i);
	   */
	  continue;
	}
      else
	{
	  fprintf (out_fp, "%3d %10d %10d %3d %3d %4d  %p %p-%p"
		   " %4s %5d %5d\n",
		   i, log_bufptr->pageid, log_bufptr->phy_pageid,
		   log_bufptr->dirty, log_bufptr->recently_freed,
		   log_bufptr->fcnt, (void *) log_bufptr,
		   (void *) (&log_bufptr->logpage),
		   (void *) (&log_bufptr->logpage.area[LOGAREA_SIZE - 1]), "",
		   log_bufptr->logpage.hdr.logical_pageid,
		   log_bufptr->logpage.hdr.offset);
	}
    }
  (void) fprintf (out_fp, "\n");
}


/*
 * logpb_print_hash_entry - Print a hash entry
 *
 * return: always return true.
 *
 *   outfp(in): FILE stream where to dump the entry.
 *   key(in): The pageid
 *   ent(in): The Buffer pointer
 *   ignore(in): Nothing..
 *
 * NOTE:A page hash table entry is dumped.
 */
int
logpb_print_hash_entry (FILE * outfp, const void *key, void *ent,
			void *ignore)
{
  const PAGEID *pageid = (PAGEID *) key;
  struct log_buffer *log_bufptr = (struct log_buffer *) ent;

  fprintf (outfp, "Pageid = %5d, Address = %p\n",
	   *pageid, (void *) log_bufptr);

  return (true);
}

/*
 * logpb_initialize_backup_info - initialized backup information
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_initialize_backup_info ()
{
  int i;

  for (i = 0; i < FILEIO_BACKUP_UNDEFINED_LEVEL; i++)
    {
      log_Gl.hdr.bkinfo[i].bkup_attime = 0;
      log_Gl.hdr.bkinfo[i].ndirty_pages_post_bkup = 0;
      log_Gl.hdr.bkinfo[i].io_baseln_time = 0;
      log_Gl.hdr.bkinfo[i].io_numpages = 0;
      log_Gl.hdr.bkinfo[i].io_bkuptime = 0;
    }
}

/*
 * logpb_initialize_header - Initialize log header structure
 *
 * return: nothing
 *
 *   loghdr(in/out): Log header structure
 *   prefix_logname(in): Name of the log volumes. It is usually set the same as
 *                      database name. For example, if the value is equal to
 *                      "db", the names of the log volumes created are as
 *                      follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *   npages(in): Size of active log in pages
 *   db_creation(in): Database creation time.
 *
 * NOTE:Initialize a log header structure.
 */
int
logpb_initialize_header (THREAD_ENTRY * thread_p, struct log_header *loghdr,
			 const char *prefix_logname, DKNPAGES npages,
			 INT64 * db_creation)
{
  assert (LOG_CS_OWN (thread_p));
  assert (loghdr != NULL);

  strncpy (loghdr->magic, CUBRID_MAGIC_LOG_ACTIVE, CUBRID_MAGIC_MAX_LENGTH);

  if (db_creation != NULL)
    {
      loghdr->db_creation = *db_creation;
    }
  else
    {
      loghdr->db_creation = -1;
    }

  if (strlen (rel_release_string ()) >= REL_MAX_RELEASE_LENGTH)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_COMPILATION_RELEASE, 2,
	      rel_release_string (), REL_MAX_RELEASE_LENGTH);
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_init_logheader");
      return ER_LOG_COMPILATION_RELEASE;
    }

  strncpy (loghdr->db_release, rel_release_string (), REL_MAX_RELEASE_LENGTH);
  loghdr->db_compatibility = rel_disk_compatible ();
  loghdr->db_iopagesize = IO_PAGESIZE;
  loghdr->db_logpagesize = LOG_PAGESIZE;
  loghdr->is_shutdown = true;
  loghdr->next_trid = LOG_SYSTEM_TRANID + 1;
  loghdr->avg_ntrans = LOG_ESTIMATE_NACTIVE_TRANS;
  loghdr->avg_nlocks = LOG_ESTIMATE_NOBJ_LOCKS;
  loghdr->npages = npages - 1;	/* Hdr pg is stolen */
  loghdr->fpageid = 0;
  loghdr->append_lsa.pageid = loghdr->fpageid;
  loghdr->append_lsa.offset = 0;
  LSA_COPY (&loghdr->chkpt_lsa, &loghdr->append_lsa);
  loghdr->nxarv_pageid = loghdr->fpageid;
  loghdr->nxarv_phy_pageid = 1;
  loghdr->nxarv_num = 0;
  loghdr->last_arv_num_for_syscrashes = -1;
  loghdr->last_deleted_arv_num = -1;
  loghdr->has_logging_been_skipped = false;
  LSA_SET_NULL (&loghdr->bkup_level0_lsa);
  LSA_SET_NULL (&loghdr->bkup_level1_lsa);
  LSA_SET_NULL (&loghdr->bkup_level2_lsa);
  if (prefix_logname != NULL)
    {
      strcpy (loghdr->prefix_name, prefix_logname);
    }
  else
    {
      loghdr->prefix_name[0] = '\0';
    }
  loghdr->lowest_arv_num_for_backup = -1;
  loghdr->highest_arv_num_for_backup = -1;
  loghdr->perm_status = LOG_PSTAT_CLEAR;

  logpb_initialize_backup_info ();

  loghdr->ha_server_state = HA_SERVER_STATE_IDLE;
  loghdr->ha_file_status = -1;
  LSA_SET_NULL (&loghdr->eof_lsa);

  return NO_ERROR;
}

/*
 * logpb_create_header_page - Create log header page
 *
 * return: Pointer to the page or NULL
 *
 * NOTE:Create the log header page.
 */
LOG_PAGE *
logpb_create_header_page (THREAD_ENTRY * thread_p)
{
  assert (LOG_CS_OWN (thread_p));
  return logpb_create (thread_p, LOGPB_HEADER_PAGE_ID);
}

/*
 * logpb_fetch_header - Fetch log header
 *
 * return: nothing
 *
 *   hdr(in/out): Pointer where log header is stored
 *
 * NOTE:Read the log header into the area pointed by hdr.
 */
void
logpb_fetch_header (THREAD_ENTRY * thread_p, struct log_header *hdr)
{
  assert (hdr != NULL);
  assert (LOG_CS_OWN (thread_p));
  assert (log_Gl.loghdr_pgptr != NULL);

  logpb_fetch_header_with_buffer (thread_p, hdr, log_Gl.loghdr_pgptr);
}

/*
 * logpb_fetch_header_with_buffer - Fetch log header using given buffer
 *
 * return: nothing
 *
 *   hdr(in/out): Pointer where log header is stored
 *   log_pgptr(in/out): log page buffer ptr
 *
 * NOTE:Read the log header into the area pointed by hdr
 */
void
logpb_fetch_header_with_buffer (THREAD_ENTRY * thread_p,
				struct log_header *hdr, LOG_PAGE * log_pgptr)
{
  struct log_header *log_hdr;	/* The log header  */

  assert (hdr != NULL);
  assert (LOG_CS_OWN (thread_p));
  assert (log_pgptr != NULL);

  if ((logpb_fetch_page (thread_p, LOGPB_HEADER_PAGE_ID, log_pgptr)) == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_fetch_hdr_with_buf");
      /* This statement should not be reached */
      (void) logpb_initialize_header (thread_p, hdr, NULL, 0, NULL);
      return;
    }

  log_hdr = (struct log_header *) (log_pgptr->area);
  *hdr = *log_hdr;

  assert (log_pgptr->hdr.logical_pageid == LOGPB_HEADER_PAGE_ID);
  assert (log_pgptr->hdr.offset == NULL_OFFSET);
}

/*
 * logpb_flush_header - Flush log header
 *
 * return: nothing
 *
 * NOTE:Flush out the log header from the global variable log_Gl.hdr
 *              to disk. Note append pages are not flushed.
 */
void
logpb_flush_header (THREAD_ENTRY * thread_p)
{
  struct log_header *log_hdr;

#if defined(CUBRID_DEBUG)
  struct timeval start_time = {
    0, 0
  };
  struct timeval end_time = {
    0, 0
  };

  css_gettimeofday (&start_time, NULL);
#endif /* CUBRID_DEBUG */

  assert (LOG_CS_OWN (thread_p));
  assert (log_Gl.loghdr_pgptr != NULL);

  log_hdr = (struct log_header *) (log_Gl.loghdr_pgptr->area);
  *log_hdr = log_Gl.hdr;

  log_Gl.loghdr_pgptr->hdr.logical_pageid = LOGPB_HEADER_PAGE_ID;
  log_Gl.loghdr_pgptr->hdr.offset = NULL_OFFSET;

  logpb_write_page_to_disk (thread_p, log_Gl.loghdr_pgptr,
			    LOGPB_HEADER_PAGE_ID);

  log_Stat.flush_hdr_call_count++;
#if defined(CUBRID_DEBUG)
  gettimeofday (&end_time, NULL);
  log_Stat.last_flush_hdr_sec_by_LFT = LOG_GET_ELAPSED_TIME (end_time,
							     start_time);
  log_Stat.total_flush_hdr_sec_by_LFT += log_Stat.last_flush_hdr_sec_by_LFT;
#endif /* CUBRID_DEBUG */

#if defined(CUBRID_DEBUG)
  er_log_debug (ARG_FILE_LINE,
		"log_flush_hdr: "
		"call count(%ld) avg flush (%f) \n",
		log_Stat.flush_hdr_call_count,
		(double) log_Stat.total_flush_hdr_sec_by_LFT /
		log_Stat.flush_hdr_call_count);
#endif /* CUBRID_DEBUG */
}

/*
 * logpb_fetch_page - Fetch a exist_log page using local buffer
 *
 * return: Pointer to the page or NULL
 *
 *   pageid(in): Page identifier
 *   log_pgptr(in): Page buffer to copy
 *
 * NOTE:Fetch the log page identified by pageid into a log buffer and
 *              return such buffer.
 *              If there is the page in hash table, copy it to buffer
 *              and return it.
 *              If not, read log page from log.
 */
LOG_PAGE *
logpb_fetch_page (THREAD_ENTRY * thread_p, PAGEID pageid,
		  LOG_PAGE * log_pgptr)
{
  register struct log_buffer *log_bufptr = NULL;
  LOG_PAGE *ret_pgptr = NULL;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  assert (log_pgptr != NULL);
  assert (pageid != NULL_PAGEID);

  LOG_CS_ENTER_READ_MODE (thread_p);

/*   assert(log_lpb_mutex_not_own()); */
  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

  log_bufptr = (struct log_buffer *) mht_get (log_Pb.ht, &pageid);
  if (log_bufptr != NULL)
    {
      memcpy (log_pgptr, &log_bufptr->logpage, LOG_PAGESIZE);
      ret_pgptr = log_pgptr;
    }
  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);

  if (log_bufptr == NULL)
    {
      ret_pgptr = logpb_read_page_from_file (thread_p, pageid, log_pgptr);
    }

  LOG_CS_EXIT ();
  return ret_pgptr;
}

/*
 * logpb_read_page_from_file - Fetch a exist_log page from log files
 *
 * return: Pointer to the page or NULL
 *
 *   pageid(in): Page identifier
 *   log_pgptr(in): Page buffer to read
 *
 * NOTE:read the log page identified by pageid into a buffer
 *              from from archive or active log.
 */
LOG_PAGE *
logpb_read_page_from_file (THREAD_ENTRY * thread_p, PAGEID pageid,
			   LOG_PAGE * log_pgptr)
{
  assert (log_pgptr != NULL);
  assert (pageid != NULL_PAGEID);
  assert (LOG_CS_OWN (thread_p));

  if (logpb_is_page_in_archive (pageid)
      && (LOG_ISRESTARTED () == false
	  || (pageid + LOGPB_ACTIVE_NPAGES) <= log_Gl.hdr.append_lsa.pageid))
    {
      if (logpb_fetch_from_archive (thread_p, pageid, log_pgptr, NULL, NULL)
	  == NULL)
	{
	  return NULL;
	}
    }
  else
    {
      PAGEID phy_pageid;
      /*
       * Page is contained in the active log.
       * Find the corresponding physical page and read the page form disk.
       */
      phy_pageid = logpb_to_physical_pageid (pageid);

      mnt_log_ioreads (thread_p);

      if (fileio_read (thread_p, log_Gl.append.vdes, log_pgptr, phy_pageid,
		       LOG_PAGESIZE) == NULL)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_READ, 3, pageid, phy_pageid, log_Name_active);
	  return NULL;
	}
      else
	{
	  if (log_pgptr->hdr.logical_pageid != pageid)
	    {
	      /* Clean the buffer... since it may be corrupted */
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_PAGE_CORRUPTED, 1, pageid);
	      return NULL;
	    }
	}
    }

  /* keep old function's usage */
  return log_pgptr;
}

int
logpb_read_page_from_active_log (THREAD_ENTRY * thread_p, PAGEID pageid,
				 int num_pages, LOG_PAGE * log_pgptr)
{
  PAGEID phy_start_pageid;

  assert (log_pgptr != NULL);
  assert (pageid != NULL_PAGEID);
  assert (num_pages > 0);

  /*
   * Page is contained in the active log.
   * Find the corresponding physical page and read the page from disk.
   */
  phy_start_pageid = logpb_to_physical_pageid (pageid);
  num_pages = MIN (num_pages, LOGPB_ACTIVE_NPAGES - phy_start_pageid + 1);

  mnt_log_ioreads (thread_p);
  if (fileio_read_pages (thread_p, log_Gl.append.vdes, (char *) log_pgptr,
			 phy_start_pageid, num_pages, LOG_PAGESIZE) == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_READ, 3, pageid,
	      phy_start_pageid, log_Name_active);
      return -1;
    }
  else
    {
      if (log_pgptr->hdr.logical_pageid != pageid)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_PAGE_CORRUPTED,
		  1, pageid);
	  return -1;
	}
    }

  return num_pages;
}

/*
 * logpb_write_page_to_disk - writes and syncs a log page to disk
 *
 * return: nothing
 *
 *   log_pgptr(in): Log page pointer
 *   logical_pageid(in): logical page id
 *
 * NOTE:writes and syncs a log page to disk
 */
LOG_PAGE *
logpb_write_page_to_disk (THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr,
			  PAGEID logical_pageid)
{
  int nbytes;
  PAGEID phy_pageid;

  assert (log_pgptr != NULL);

  phy_pageid = logpb_to_physical_pageid (logical_pageid);

  /* log_Gl.append.vdes is only changed
   * while starting or finishing or recovering server.
   * So, log cs is not needed.
   */
#if 1				/* yaw */
  if (fileio_write (thread_p, log_Gl.append.vdes, log_pgptr, phy_pageid,
		    LOG_PAGESIZE) == NULL)
#else
  if (fileio_write (thread_p, log_Gl.append.vdes, log_pgptr, phy_pageid,
		    LOG_PAGESIZE) == NULL
      || fileio_synchronize (thread_p, log_Gl.append.vdes,
			     log_Name_active) == NULL_VOLDES)
#endif
    {
      if (er_errid () == ER_IO_WRITE_OUT_OF_SPACE)
	{
	  nbytes = ((log_Gl.hdr.npages + 1 - logical_pageid) *
		    log_Gl.hdr.db_logpagesize);
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_WRITE_OUT_OF_SPACE,
		  4, logical_pageid, phy_pageid, log_Name_active, nbytes);
	}
      else
	{
	  er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_LOG_WRITE, 3,
			       logical_pageid, phy_pageid, log_Name_active);
	}

      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "logpb_write_page_to_disk");
      return NULL;
    }

  return log_pgptr;
}

/*
 * logpb_find_header_parameters - Find some database creation parameters
 *
 * return: iopagesize or -1
 *
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in): Name of the log volumes. It is usually set as database
 *                      name. For example, if the value is equal to "db", the
 *                      names of the log volumes created are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *   db_iopagesize(in): Set as a side effect to iopagesize
 *   db_creation(in): Set as a side effect to time of database creation
 *   db_compatibility(in): Set as a side effect to database disk compatibility
 *
 * NOTE:Find some database creation parameters such as pagesize,
 *              creation time, and disk compatability.
 */
PGLENGTH
logpb_find_header_parameters (THREAD_ENTRY * thread_p,
			      const char *db_fullname, const char *logpath,
			      const char *prefix_logname,
			      PGLENGTH * io_page_size,
			      PGLENGTH * log_page_size,
			      INT64 * creation_time, float *db_compatibility)
{
  struct log_header hdr;	/* Log header */
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_log_pgbuf;
  LOG_PAGE *log_pgptr = NULL;
  int error_code = NO_ERROR;

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

  assert (LOG_CS_OWN (thread_p));

  /* Is the system restarted ? */
  if (log_Gl.trantable.area != NULL && log_Gl.append.log_pgptr != NULL)
    {
      *io_page_size = log_Gl.hdr.db_iopagesize;
      *log_page_size = log_Gl.hdr.db_logpagesize;
      *creation_time = log_Gl.hdr.db_creation;
      *db_compatibility = log_Gl.hdr.db_compatibility;

      if (IO_PAGESIZE != *io_page_size || LOG_PAGESIZE != *log_page_size)
	{
	  if (db_set_page_size (*io_page_size, *log_page_size) != NO_ERROR)
	    {
	      goto error;
	    }

	  logpb_finalize_pool ();
	  error_code =
	    logtb_define_trantable_log_latch (thread_p,
					      log_Gl.trantable.
					      num_total_indices);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	  error_code = logpb_initialize_pool (thread_p);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	  if (logpb_fetch_start_append_page (thread_p) == NULL)
	    {
	      goto error;
	    }
	}
      return *io_page_size;
    }

  /* System is not restarted. Read the header from disk */

  error_code = logpb_initialize_log_names (thread_p, db_fullname, logpath,
					   prefix_logname);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  /*
   * Avoid setting errors at this moment related to existance of files.
   */

  if (fileio_is_volume_exist (log_Name_active) == false)
    {
      error_code = ER_FAILED;
      goto error;
    }

  log_Gl.append.vdes = fileio_mount (thread_p, db_fullname, log_Name_active,
				     LOG_DBLOG_ACTIVE_VOLID, true, false);
  if (log_Gl.append.vdes == NULL_VOLDES)
    {
      error_code = ER_FAILED;
      goto error;
    }

  error_code = logpb_initialize_pool (thread_p);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

  logpb_fetch_header_with_buffer (thread_p, &hdr, log_pgptr);
  logpb_finalize_pool ();

  fileio_dismount (thread_p, log_Gl.append.vdes);
  log_Gl.append.vdes = NULL_VOLDES;

  *io_page_size = hdr.db_iopagesize;
  *log_page_size = hdr.db_logpagesize;
  *creation_time = hdr.db_creation;
  *db_compatibility = hdr.db_compatibility;

  /*
   * Make sure that the log is a log file and that it is compatible with the
   * running database and system
   */

  if (strcmp (hdr.prefix_name, prefix_logname) != 0)
    {
      /*
       * This does not look like the log or the log was renamed. Incompatible
       * prefix name with the prefix stored on disk
       */
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_INCOMPATIBLE_PREFIX_NAME, 2,
	      log_Name_active, hdr.prefix_name);
      /* Continue anyhow.. */
    }

  /* only check for incompatibility here, this will be done again in
   * log_xinit which will run the compatibility functions if there are any.
   */
  /* We added disk compatibility rules to R2.2. Before that release,
   * rel_is_disk_compatible function returned only REL_FULLY_COMPATIBLE or
   * REL_NOT_COMPATIBLE. However, it might return REL_BACKWARD_COMPATIBLE now.
   */
  if (rel_is_disk_compatible (*db_compatibility, NULL) !=
      REL_FULLY_COMPATIBLE)
    {
      log_Gl.hdr.db_compatibility = *db_compatibility;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_INCOMPATIBLE_DATABASE,
	      2, rel_name (), rel_release_string ());
      error_code = ER_LOG_INCOMPATIBLE_DATABASE;
      goto error;
    }

  if (IO_PAGESIZE != *io_page_size || LOG_PAGESIZE != *log_page_size)
    {
      if (db_set_page_size (*io_page_size, *log_page_size) != NO_ERROR)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
      else
	{
	  error_code =
	    logtb_define_trantable_log_latch (thread_p,
					      log_Gl.trantable.
					      num_total_indices);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}
    }

  return *io_page_size;

error:
  *io_page_size = -1;
  *log_page_size = -1;
  *creation_time = 0;
  *db_compatibility = -1.0;

  return *io_page_size;
}

/*
 *
 *       	       FUNCTIONS RELATED TO APPEND PAGES
 *
 */

/*
 * logpb_fetch_start_append_page - FETCH THE START APPEND PAGE
 *
 * return: Pointer to the page or NULL
 *
 * NOTE:Fetch the start append page.
 */
LOG_PAGE *
logpb_fetch_start_append_page (THREAD_ENTRY * thread_p)
{
  int flag = OLD_PAGE;
  bool need_flush;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  assert (LOG_CS_OWN (thread_p));

  /* detect empty log (page and offset of zero) */
  if ((log_Gl.hdr.append_lsa.pageid == 0)
      && (log_Gl.hdr.append_lsa.offset == 0))
    {
      flag = NEW_PAGE;
    }

  if (log_Gl.append.log_pgptr != NULL)
    {
      /*
       * Somehow we already have an append page, flush all current append page
       * and start form scratch
       */
      logpb_invalid_all_append_pages (thread_p);
    }

  /*
   * Fetch the start append page and set delayed free to NULL.
   */

  log_Gl.append.delayed_free_log_pgptr = NULL;
  log_Gl.append.log_pgptr =
    logpb_fix_page (thread_p, log_Gl.hdr.append_lsa.pageid, flag);
  if (log_Gl.append.log_pgptr == NULL)
    {
      return NULL;
    }

  LSA_COPY (&log_Gl.append.nxio_lsa, &log_Gl.hdr.append_lsa);

  /*
   * Save this log append page as an active page to be flushed at a later
   * time if the page is modified (dirty).
   * We must save the log append pages in the order that they are defined
   * and need to be flushed.
   */

  need_flush = false;

  LOG_MUTEX_LOCK (rv, flush_info->flush_mutex);
  flush_info->toflush[flush_info->num_toflush] = log_Gl.append.log_pgptr;
  flush_info->num_toflush++;

  if (flush_info->num_toflush >= flush_info->max_toflush)
    {
      /*
       * Force the dirty pages including the current one at this moment
       */
      need_flush = true;
    }
  LOG_MUTEX_UNLOCK (flush_info->flush_mutex);

  if (need_flush)
    {
      logpb_flush_all_append_pages (thread_p, LOG_FLUSH_FORCE);
    }

  return log_Gl.append.log_pgptr;
}

/*
 * logpb_fetch_start_append_page_new - FETCH THE NEW START APPEND PAGE
 *
 * return: Pointer to the page or NULL
 */
LOG_PAGE *
logpb_fetch_start_append_page_new (THREAD_ENTRY * thread_p)
{
  assert (LOG_CS_OWN (thread_p));

  log_Gl.append.delayed_free_log_pgptr = NULL;
  log_Gl.append.log_pgptr = logpb_fix_page (thread_p,
					    log_Gl.hdr.append_lsa.pageid,
					    NEW_PAGE);
  if (log_Gl.append.log_pgptr == NULL)
    {
      return NULL;
    }

  LSA_COPY (&log_Gl.append.nxio_lsa, &log_Gl.hdr.append_lsa);

  return log_Gl.append.log_pgptr;
}

/*
 * logpb_next_append_page - Fetch next append page
 *
 * return: nothing
 *
 *   current_setdirty(in): Set the current append page dirty ?
 *
 * NOTE:Fetch the next append page.
 *              If the current append page contains the beginning of the log
 *              record being appended (i.e., log record did fit on current
 *              append page), the freeing of this page is delayed until the
 *              record is completely appended/logged. This is needed since
 *              every log record has a forward pointer to next log record
 *              (i.e., next append address). In addition, we must avoid
 *              flushing this page to disk (e.g., page replacement),
 *              otherwise, during crash recovery we could try to read a log
 *              record that has never been finished and the end of the log may
 *              mot be detected. That is, the log would be corrupted.
 *
 *              If the current append page does not contain the beginning of
 *              the log record, the page can be freed and flushed at any time.
 *
 *              If the next page to archive is located at the physical
 *              location of the desired append page, a set of log pages is
 *              archived, so we can continue the append operations.
 */
void
logpb_next_append_page (THREAD_ENTRY * thread_p,
			LOG_SETDIRTY current_setdirty)
{
  bool need_flush;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
#if defined(CUBRID_DEBUG)
  long commit_count = 0;
  static struct timeval start_append_time = {
    0, 0
  };
  struct timeval end_append_time = {
    0, 0
  };
  static long prev_commit_count_in_append = 0;
  double elapsed = 0;

  gettimeofday (&end_append_time, NULL);
#endif /* CUBRID_DEBUG */
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  assert (LOG_CS_OWN (thread_p));

  if (current_setdirty == LOG_SET_DIRTY)
    {
      logpb_set_dirty (log_Gl.append.log_pgptr, DONT_FREE);
    }

  /*
   * If a log append page is already delayed. We can free the current page
   * since it is not the first one. (i.e., end of log can be detected since
   * previous page has not been released).
   */

  if (log_Gl.append.delayed_free_log_pgptr != NULL)
    {
      /*
       * The free of an append page has already been fdelayed. Thus, the current
       * page cannot be the first page of the append record that expands several
       * log pages.
       */
      logpb_free_page (log_Gl.append.log_pgptr);
    }
  else
    {
      /*
       * We have not delayed freeing an append page. Therefore, the current
       * log append page is a candidate for a page holding a new append log
       * record.
       */

      if (logpb_is_dirty (log_Gl.append.log_pgptr) == true)
	{
	  log_Gl.append.delayed_free_log_pgptr = log_Gl.append.log_pgptr;
	  /* assert(current_setdirty == LOG_SET_DIRTY); */
	}
      else
	{
	  logpb_free_page (log_Gl.append.log_pgptr);
	}

    }

  log_Gl.append.log_pgptr = NULL;

  log_Gl.hdr.append_lsa.pageid++;
  log_Gl.hdr.append_lsa.offset = 0;

  /*
   * Is the next logical page to archive, currently located at the physical
   * location of the next logical append page ? (Remember the log is a RING).
   * If so, we need to archive the log from the next logical page to archive
   * up to the closest page that does not hold the current append log record.
   */

  if (LOGPB_AT_NEXT_ARCHIVE_PAGE_ID (log_Gl.hdr.append_lsa.pageid))
    {
      /* The log must be archived */
      logpb_archive_active_log (thread_p, false);
    }

  /*
   * Has the log been cycled ?
   */
  if (LOGPB_IS_FIRST_PHYSICAL_PAGE (log_Gl.hdr.append_lsa.pageid))
    {
      log_Gl.hdr.fpageid += LOGPB_ACTIVE_NPAGES;

      /* Flush the header to save updates by archiving. */
      logpb_flush_header (thread_p);
    }

  /*
   * Fetch the next page as a newly defined append page. Append pages are
   * always new pages
   */

  log_Gl.append.log_pgptr =
    logpb_create (thread_p, log_Gl.hdr.append_lsa.pageid);
  if (log_Gl.append.log_pgptr == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_next_append_page");
      /* This statement should not be reached */
      return;
    }

#if defined(CUBRID_DEBUG)
  {
    log_Stat.last_append_pageid = log_Gl.hdr.append_lsa.pageid;

    log_Stat.last_delayed_pageid = NULL_PAGEID;
    if (log_Gl.append.delayed_free_log_pgptr != NULL)
      {
	struct log_buffer *delayed_bufptr;
	LOG_CAST_LOGPAGEPTR_TO_BFPTR (delayed_bufptr,
				      log_Gl.append.delayed_free_log_pgptr);
	log_Stat.last_delayed_pageid =
	  log_Gl.append.delayed_free_log_pgptr->hdr.logical_pageid;
	log_Stat.total_delayed_page_count++;
      }
  }
#endif /* CUBRID_DEBUG */

  /*
   * Save this log append page as an active page to be flushed at a later
   * time if the page is modified (dirty).
   * We must save the log append pages in the order that they are defined
   * and need to be flushed.
   */

  LOG_MUTEX_LOCK (rv, flush_info->flush_mutex);

  flush_info->toflush[flush_info->num_toflush] = log_Gl.append.log_pgptr;
  flush_info->num_toflush++;

  need_flush = false;
  if (flush_info->num_toflush >= flush_info->max_toflush)
    {
      need_flush = true;
    }

  LOG_MUTEX_UNLOCK (flush_info->flush_mutex);

  if (need_flush)
    {
      logpb_flush_all_append_pages (thread_p, LOG_FLUSH_DIRECT);
    }

#if defined(CUBRID_DEBUG)
  if (start_append_time.tv_sec != 0 && start_append_time.tv_usec != 0)
    {
      elapsed = LOG_GET_ELAPSED_TIME (end_append_time, start_append_time);
    }

  log_Stat.use_append_page_sec = elapsed;
  gettimeofday (&start_append_time, NULL);

  commit_count = log_Stat.commit_count - prev_commit_count_in_append;

  prev_commit_count_in_append = log_Stat.commit_count;

  log_Stat.last_commit_count_while_using_a_page = commit_count;
  log_Stat.total_commit_count_while_using_a_page +=
    log_Stat.last_commit_count_while_using_a_page;
#endif /* CUBRID_DEBUG */

  log_Stat.total_append_page_count++;

#if defined(CUBRID_DEBUG)
  er_log_debug (ARG_FILE_LINE,
		"log_next_append_page: "
		"new page id(%d) delayed page id(%d) "
		"total_append_page_count(%ld) total_delayed_page_count(%ld) "
		" num_toflush(%d) use_append_page_sec(%f) need_flush(%d) "
		"commit_count(%ld) total_commit_count(%ld)\n",
		(int) log_Stat.last_append_pageid,
		(int) log_Stat.last_delayed_pageid,
		log_Stat.total_append_page_count,
		log_Stat.total_delayed_page_count,
		flush_info->num_toflush,
		log_Stat.use_append_page_sec,
		need_flush,
		log_Stat.last_commit_count_while_using_a_page,
		log_Stat.total_commit_count_while_using_a_page);
#endif /* CUBRID_DEBUG */
}

/*
 * log_writev_append_pages - Write a set of sequential pages
 *
 * return: to_flush or NULL
 *
 *   to_flush(in): Array to address of content of pages to flush
 *   npages(in): Number of pages to flush
 *
 * NOTE:Flush to disk a set of log contiguous pages.
 */
static LOG_PAGE **
logpb_writev_append_pages (THREAD_ENTRY * thread_p, LOG_PAGE ** to_flush,
			   DKNPAGES npages)
{
  struct log_buffer *bufptr;
  PAGEID phy_pageid;

  /* In this point, flush buffer cannot be replaced by trans.
   * So, bufptr's pageid and phy_pageid are not changed.
   */

  if (npages > 0)
    {
      /* Record number of writes in statistics */
      mnt_log_iowrites (thread_p);

      LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, to_flush[0]);
      phy_pageid = bufptr->phy_pageid;

      if (fileio_writev (thread_p, log_Gl.append.vdes, (void **) to_flush,
			 phy_pageid, npages, LOG_PAGESIZE) == NULL)
	{
	  if (er_errid () == ER_IO_WRITE_OUT_OF_SPACE)
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_WRITE_OUT_OF_SPACE, 4,
		      bufptr->pageid, phy_pageid, log_Name_active,
		      ((log_Gl.hdr.npages + 1 - bufptr->pageid) *
		       log_Gl.hdr.db_logpagesize));
	    }
	  else
	    {
	      er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_LOG_WRITE, 3,
				   bufptr->pageid, phy_pageid,
				   log_Name_active);
	    }
	  to_flush = NULL;
	}
    }

  return to_flush;
}

/*
 * logpb_write_toflush_pages_to_archive - Background archiving
 *
 * NOTE : write flushed pages to temporary archiving volume
 * (which will be renamed to real archiving volume) at this time.
 * but don't write last page because it will be modified & flushed again.
 * in error case, dismount temp archiving volume and give up background
 * archiving.
 */
static void
logpb_write_toflush_pages_to_archive (THREAD_ENTRY * thread_p)
{
  int i, pageid, phy_pageid;
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT];
  LOG_PAGE *log_pgptr = NULL;
  struct log_buffer *bufptr;

  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;
  BACKGROUND_ARCHIVING_INFO *bg_arv_info = &log_Gl.bg_archive_info;
  LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, flush_info->toflush[0]);

  assert (PRM_LOG_BACKGROUND_ARCHIVING);
  if (log_Gl.bg_archive_info.vdes == NULL_VOLDES
      || flush_info->num_toflush <= 1)
    {
      return;
    }


  assert (bg_arv_info->current_page_id >= bufptr->pageid);

  /*
   * defense for above unknown assert case
   * in this case, fetch unwritten log pages and write them to archiving
   * voume manually
   */
  if (bg_arv_info->current_page_id < bufptr->pageid)
    {
      log_pgptr = (LOG_PAGE *) PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

      for (pageid = bg_arv_info->current_page_id;
	   pageid <= bufptr->pageid; pageid++)
	{
	  if (logpb_fetch_page (thread_p, pageid, log_pgptr) == NULL)
	    {
	      fileio_dismount (thread_p, bg_arv_info->vdes);
	      bg_arv_info->vdes = NULL_VOLDES;
	      break;
	    }

	  /* calculate real physical page index in archiving volume */
	  phy_pageid = pageid - bg_arv_info->start_page_id + 1;

	  if (fileio_write (thread_p, bg_arv_info->vdes, log_pgptr,
			    phy_pageid, LOG_PAGESIZE) == NULL)
	    {
	      fileio_dismount (thread_p, bg_arv_info->vdes);
	      bg_arv_info->vdes = NULL_VOLDES;
	      break;
	    }
	}

      bg_arv_info->current_page_id = bufptr->pageid;
    }

  /*
   * main dual writing logic
   * write flushed pages (except last page which will be modified & flushed again)
   * to temp archiving volume
   */
  if (bg_arv_info->vdes != NULL_VOLDES)
    {
      for (i = 0; i < flush_info->num_toflush - 1; i++)
	{
	  LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, flush_info->toflush[i]);

	  /* calculate real physical page index in archiving volume */
	  phy_pageid = bufptr->pageid - bg_arv_info->start_page_id + 1;

	  if (fileio_write (thread_p, bg_arv_info->vdes,
			    flush_info->toflush[i], phy_pageid,
			    LOG_PAGESIZE) == NULL)
	    {
	      fileio_dismount (thread_p, bg_arv_info->vdes);
	      bg_arv_info->vdes = NULL_VOLDES;
	      break;
	    }
	}

      /*
       * 0 to i-1 pages of toflush buffer are written at temp
       * archiving volume.
       * And now, set current_page_id as position of toflush[i].
       * (which will be written at next time)
       */
      LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, flush_info->toflush[i]);
      bg_arv_info->current_page_id = bufptr->pageid;
    }
}

/*
 * logpb_flush_all_append_pages_helper - Flush log append pages
 *
 * return: 1 : log flushed, 0 : do not need log flush, < 0 : error code
 *
 *   from_commit(in):
 *
 */
int
logpb_flush_all_append_pages_helper (THREAD_ENTRY * thread_p)
{
  LOG_RECTYPE save_type = LOG_SMALLER_LOGREC_TYPE;	/* Save type of last
							 * record
							 */
  struct log_rec *eof;		/* End of log record */
  struct log_buffer *bufptr;	/* The current buffer log append page
				 * scanned
				 */
  struct log_buffer *prv_bufptr;	/* The previous buffer log append page
					 * scanned
					 */
  int last_idxflush;		/* The smallest dirty append log page to
				 * flush. This is the last one to flush.
				 */
  int idxflush;			/* An index into the first log page buffer
				 * to flush
				 */
  bool need_sync;		/* How we flush anything ? */

  int i;
  bool need_flush = true;
  int error_code = NO_ERROR;
  int flush_page_count = 0;
#if defined(CUBRID_DEBUG)
  struct timeval start_time = {
    0, 0
  };
  struct timeval end_time = {
    0, 0
  };
  int dirty_page_count = 0;
  int curr_flush_count = 0;
  long commit_count = 0;
  static long prev_commit_count_in_flush = 0;
#endif /* CUBRID_DEBUG */
  bool hold_flush_mutex = false, hold_lpb_mutex = false;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;
  LOGWR_INFO *writer_info = &log_Gl.writer_info;

#if defined(SERVER_MODE)
  int rv;
  LOGWR_ENTRY *entry;
#endif /* SERVER_MODE */

  assert (LOG_CS_OWN (thread_p));

#if defined(CUBRID_DEBUG)
  er_log_debug (ARG_FILE_LINE, "logpb_flush_all_append_pages: start\n");

  gettimeofday (&start_time, NULL);
#endif /* CUBRID_DEBUG */

  LOG_MUTEX_LOCK (rv, flush_info->flush_mutex);
  hold_flush_mutex = true;

  if (flush_info->num_toflush < 1)
    {
      need_flush = false;
    }
  if (flush_info->num_toflush == 1)
    {
      /*
       * Don't need to do anything if the page is not dirty.
       *
       * This block is used to avoid updating the last page with an
       * end of file log when it is not needed at all.
       */

      LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, flush_info->toflush[0]);
      assert (bufptr->fcnt > 0);

      if (!logpb_is_dirty (flush_info->toflush[0]))
	{
	  need_flush = false;
	}
    }

  LOG_MUTEX_UNLOCK (flush_info->flush_mutex);
  hold_flush_mutex = false;

  if (!need_flush)
    {
      return 0;
    }

#if !defined(NDEBUG)
  {
    const char *env_value;
    int verbose_dump = -1;


    if (verbose_dump == -1)
      {
	/*
	 * Do we want to dump the buffer pool for future verification
	 */
	if ((env_value = envvar_get ("LOG_FLUSH_VERBOSE_DUMP")) != NULL)
	  {
	    verbose_dump = atoi (env_value) != 0 ? 1 : 0;
	  }
	else
	  {
	    verbose_dump = 0;
	  }
      }

    if (verbose_dump != 0)
      {
	fprintf (stdout, "\n DUMP BEFORE FLUSHING APPEND PAGES\n");
	logpb_dump (stdout);
      }
  }
#endif

#if defined(CUBRID_DEBUG)
  if (log_Gl.append.nxio_lsa.pageid !=
      logpb_get_page_id (flush_info->toflush[0]))
    {
      er_log_debug (ARG_FILE_LINE,
		    "logpb_flush_all_append_pages: SYSTEM ERROR\n "
		    " NXIO_PAGE %d does not seem the same as next free"
		    " append page %d to flush\n",
		    log_Gl.append.nxio_lsa.pageid,
		    logpb_get_page_id (flush_info->toflush[0]));
      goto error;
    }
#endif /* CUBRID_DEBUG */

  /*
   * Add an end of log marker to detect the end of the log.
   * The marker should be added at the end of the log if there is not any
   * delayed free page. That is, if we are not in the middle of appending
   * a new log record. Otherwise, we need to change the label of the last
   * append record as log end record. Flush and then check it back.
   */

  if (log_Gl.append.delayed_free_log_pgptr != NULL)
    {
      /*
       * Flush all log append records on such page except the current log
       * record which has not been finished. Save the log record type of
       * this record, overwrite an eof record on such position, and flush
       * the page. Then, restore the record back on the page and change
       * the current append log sequence address
       */

#if defined(CUBRID_DEBUG)
      if (logpb_get_page_id (log_Gl.append.delayed_free_log_pgptr)
	  != log_Gl.append.prev_lsa.pageid)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_WRONG_FORCE_DELAYED, 2,
		  log_Gl.append.prev_lsa.pageid,
		  logpb_get_page_id (log_Gl.append.delayed_free_log_pgptr));
	  goto error;
	}
#endif /* CUBRID_DEBUG */

      eof = (struct log_rec *) LOG_PREV_APPEND_PTR ();
      save_type = eof->type;

      /* Overwrite it with an end of log marker */
      LSA_SET_NULL (&eof->forw_lsa);
      eof->type = LOG_END_OF_LOG;
      LSA_COPY (&log_Gl.hdr.eof_lsa, &log_Gl.append.prev_lsa);

      logpb_set_dirty (log_Gl.append.delayed_free_log_pgptr, DONT_FREE);
    }
  else
    {
      /*
       * Add an end of log marker to detect the end of the log. Don't advance the
       * log address, the log end of file is overwritten at a later point.
       */
      LOG_APPEND_ADVANCE_WHEN_DOESNOT_FIT (thread_p, sizeof (*eof));
      eof = (struct log_rec *) LOG_APPEND_PTR ();

      eof->trid = LOG_READ_NEXT_TRANID;
      LSA_SET_NULL (&eof->prev_tranlsa);
      LSA_COPY (&eof->back_lsa, &log_Gl.append.prev_lsa);
      LSA_SET_NULL (&eof->forw_lsa);
      eof->type = LOG_END_OF_LOG;
      LSA_COPY (&log_Gl.hdr.eof_lsa, &log_Gl.hdr.append_lsa);

      logpb_set_dirty (log_Gl.append.log_pgptr, DONT_FREE);
    }

  /*
   * Now flush all contiguous log append dirty pages. The first log append
   * dirty page is flushed at the end, so we can synchronize it with the
   * rest.
   */

#if defined(SERVER_MODE)
  /* It changes the status of waiting log writer threads and wakes them up */
  if (PRM_HA_MODE != HA_MODE_OFF && !writer_info->skip_flush)
    {
      assert (hold_flush_mutex == false);
      LOG_CS_DEMOTE (thread_p);

      LOG_MUTEX_LOCK (rv, writer_info->flush_start_mutex);

      LOG_MUTEX_LOCK (rv, writer_info->wr_list_mutex);
      entry = writer_info->writer_list;
      while (entry)
	{
	  if (entry->status == LOGWR_STATUS_WAIT)
	    {
	      entry->status = LOGWR_STATUS_FETCH;
	      thread_wakeup_with_tran_index (entry->thread_p->tran_index,
					     THREAD_LOGWR_RESUMED);
	    }
	  entry = entry->next;
	}
      LOG_MUTEX_UNLOCK (writer_info->wr_list_mutex);
      LOG_MUTEX_UNLOCK (writer_info->flush_start_mutex);
    }
#endif /* SERVER_MODE */

  idxflush = -1;
  last_idxflush = -1;
  prv_bufptr = NULL;
  need_sync = false;

  LOG_MUTEX_LOCK (rv, flush_info->flush_mutex);
  hold_flush_mutex = true;

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);
  hold_lpb_mutex = true;

#if defined(CUBRID_DEBUG)
  log_scan_flush_info (log_dump_pageid);
  er_log_debug (ARG_FILE_LINE, "\n");
#endif /* CUBRID_DEBUG */

  for (i = 0; i < flush_info->num_toflush; i++)
    {
      LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, flush_info->toflush[i]);

      /*
       * Make sure that we have found the smallest dirty append page to flush
       * which should be flushed at the end.
       */
      assert (!bufptr->flush_running);
      if (last_idxflush == -1)
	{
	  if (bufptr->dirty == true)
	    {
	      /* We have found the smallest dirty page */
	      last_idxflush = i;
	      prv_bufptr = bufptr;
	    }
	  continue;
	}

      if (idxflush != -1 && prv_bufptr != NULL)
	{
	  /*
	   * This append log page should be dirty and contiguous to previous
	   * append page. If it is not, we need to flush the accumulated pages
	   * up to this point, and then start accumulating pages again.
	   */
	  if ((bufptr->dirty == false)
	      || (bufptr->pageid != (prv_bufptr->pageid + 1))
	      || (bufptr->phy_pageid != (prv_bufptr->phy_pageid + 1)))
	    {
	      /*
	       * This page is not contiguous or it is not dirty.
	       *
	       * Flush the accumulated contiguous pages
	       */
	      LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
	      hold_lpb_mutex = false;

	      if (logpb_writev_append_pages (thread_p,
					     &(flush_info->toflush[idxflush]),
					     i - idxflush) == NULL)
		{
		  error_code = ER_FAILED;
		  goto error;
		}
	      else
		{
		  need_sync = true;
#if defined(CUBRID_DEBUG)
		  {
		    int j;

		    for (j = 0; j != i - idxflush; ++j)
		      {
			er_log_debug (ARG_FILE_LINE,
				      "logpb_flush_all_append_pages: flush1 "
				      "pageid(%d)\n",
				      (flush_info->toflush[idxflush])->
				      hdr.logical_pageid + j);
		      }
		  }
#endif /* CUBRID_DEBUG */

		  /*
		   * Start over the accumulation of pages
		   */

		  flush_page_count += i - idxflush;
		  idxflush = -1;
		}
	      LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);
	      hold_lpb_mutex = true;
	    }
	}

      if ((idxflush == -1) && (bufptr->dirty == true))
	{

	  /*
	   * This page should be included in the flush
	   */
	  idxflush = i;
	}

      /* prv_bufptr was not bufptr's previous buffer.
       * prv_bufptr was the first buffer to flush, so only 2 continous pages always were flushed together.
       */
      prv_bufptr = bufptr;

    }
  /*
   * If there are any accumulated pages, flush them at this point
   */

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
  hold_lpb_mutex = false;

  if (idxflush != -1)
    {
      int pageToFlush = flush_info->num_toflush - idxflush;

      /* last countious pages */
      if (logpb_writev_append_pages (thread_p,
				     &(flush_info->toflush[idxflush]),
				     pageToFlush) == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
      else
	{
#if defined(CUBRID_DEBUG)
	  {
	    int j;

	    for (j = 0; j != pageToFlush; ++j)
	      {
		er_log_debug (ARG_FILE_LINE,
			      "logpb_flush_all_append_pages: flush2 pageid(%d)\n",
			      (flush_info->toflush[idxflush])->hdr.
			      logical_pageid + j);
	      }
	  }
#endif /* CUBRID_DEBUG */
	  need_sync = true;
	  flush_page_count += pageToFlush;
	}
    }

  /*
   * Make sure that all of the above log writes are synchronized with any
   * future log writes. That is, the pages should be stored on physical disk.
   */

  if (need_sync == true)
    {
      log_Stat.total_sync_count++;
      if (PRM_SUPPRESS_FSYNC == 0
	  || (log_Stat.total_sync_count % PRM_SUPPRESS_FSYNC == 0))
	{
	  if (fileio_synchronize (thread_p,
				  log_Gl.append.vdes,
				  log_Name_active) == NULL_VOLDES)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }
	}
    }
  assert (last_idxflush != -1);
  if (last_idxflush != -1)
    {
      /*
       * Now flush and sync the first log append dirty page
       */
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "logpb_flush_all_append_pages: flush3 pageid(%d)\n",
		    (flush_info->toflush[last_idxflush])->hdr.logical_pageid);
#endif /* CUBRID_DEBUG */

      ++flush_page_count;
#if defined(CUBRID_DEBUG)
      log_Stat.total_sync_count++;
#endif /* CUBRID_DEBUG */

      if (logpb_writev_append_pages (thread_p,
				     &(flush_info->toflush[last_idxflush]),
				     1) == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
    }

  /* dual writing (Background archiving) */
  if (PRM_LOG_BACKGROUND_ARCHIVING)
    {
      logpb_write_toflush_pages_to_archive (thread_p);
    }

  /*
   * Now indicate that buffers of the append log pages are not dirty
   * any more.
   */

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);
  hold_lpb_mutex = true;

  for (i = 0; i < flush_info->num_toflush; i++)
    {
      LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, flush_info->toflush[i]);
#if defined(CUBRID_DEBUG)
      if (bufptr->dirty)
	{
	  ++dirty_page_count;
	}
      flush_info->toflush[i] = NULL;
#endif /* CUBRID_DEBUG */
      bufptr->dirty = false;
    }
#if defined(CUBRID_DEBUG)
  assert (flush_page_count == dirty_page_count);
#endif /* CUBRID_DEBUG */

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
  hold_lpb_mutex = false;

#if defined(CUBRID_DEBUG)
  assert (!logpb_is_any_dirty ());
#endif /* CUBRID_DEBUG */

#if !defined(NDEBUG)
  if (PRM_LOG_TRACE_DEBUG && logpb_is_any_dirty () == true)
    {
      er_log_debug (ARG_FILE_LINE, "logpb_flush_all_append_pages: Log Buffer"
		    " contains dirty pages\n");
      logpb_dump (stdout);
    }
#endif

  if (flush_info->num_toflush == flush_info->max_toflush)
    {
      log_Stat.log_buffer_full_count++;
    }
#if defined(CUBRID_DEBUG)
  curr_flush_count = flush_info->num_toflush;
#endif /* CUBRID_DEBUG */


  /*
   * Change the log sequence address to indicate the next append address to
   * flush and synchronize
   */

  if (log_Gl.append.delayed_free_log_pgptr != NULL)
    {
      /*
       * Restore the type of the log append record
       */
      eof->type = save_type;
      logpb_set_dirty (log_Gl.append.delayed_free_log_pgptr, DONT_FREE);

      flush_info->toflush[0] = log_Gl.append.delayed_free_log_pgptr;
      flush_info->num_toflush = 1;
      LSA_COPY (&log_Gl.append.nxio_lsa, &log_Gl.append.prev_lsa);
    }
  else
    {
      flush_info->num_toflush = 0;
      LSA_COPY (&log_Gl.append.nxio_lsa, &log_Gl.hdr.append_lsa);
    }

  if (log_Gl.append.log_pgptr != NULL)
    {
      /* Add the append page */
      flush_info->toflush[flush_info->num_toflush] = log_Gl.append.log_pgptr;
      flush_info->num_toflush++;
    }

  log_Stat.flushall_append_pages_call_count++;
  log_Stat.last_flush_count_by_trans = flush_page_count;
  log_Stat.total_flush_count_by_trans += flush_page_count;

#if defined(CUBRID_DEBUG)
  gettimeofday (&end_time, NULL);

  log_Stat.last_flush_sec_by_trans =
    LOG_GET_ELAPSED_TIME (end_time, start_time);

  log_Stat.total_flush_sec_by_trans += log_Stat.last_flush_sec_by_trans;

  commit_count = log_Stat.commit_count - prev_commit_count_in_flush;
  prev_commit_count_in_flush = log_Stat.commit_count;

  log_Stat.last_commit_count_in_flush_pages = commit_count;
  log_Stat.total_commit_count_in_flush_pages +=
    log_Stat.last_commit_count_in_flush_pages;

  er_log_debug (ARG_FILE_LINE,
		"logpb_flush_all_append_pages_helper: "
		"flush page(%ld / %d / %ld)"
		"avg flush count(%f), avg flush sec(%f)"
		"commit count(%ld) avg commit count(%f)\n",
		log_Stat.last_flush_count_by_trans,
		dirty_page_count,
		curr_flush_count,
		(double) log_Stat.total_flush_count_by_trans /
		log_Stat.flushall_append_pages_call_count,
		log_Stat.total_flush_sec_by_trans /
		log_Stat.flushall_append_pages_call_count,
		commit_count,
		log_Stat.total_commit_count_in_flush_pages /
		log_Stat.flushall_append_pages_call_count);
#endif /* CUBRID_DEBUG */

  LOG_MUTEX_UNLOCK (flush_info->flush_mutex);
  hold_flush_mutex = false;

#if defined(SERVER_MODE)
  if (PRM_HA_MODE != HA_MODE_OFF && !writer_info->skip_flush)
    {
      /* It waits until all log writer threads are done */
      MUTEX_LOCK (rv, writer_info->flush_end_mutex);

      MUTEX_LOCK (rv, writer_info->wr_list_mutex);
      entry = writer_info->writer_list;
      while (entry != NULL)
	{
	  if (entry->status == LOGWR_STATUS_FETCH)
	    break;
	  entry = entry->next;
	}
      MUTEX_UNLOCK (writer_info->wr_list_mutex);

      if (entry != NULL)
	{
	  rv = COND_WAIT (writer_info->flush_end_cond,
			  writer_info->flush_end_mutex);
	}
#if !defined(WINDOWS)
      MUTEX_UNLOCK (writer_info->flush_end_mutex);
#else
      if (entry == NULL)
	{
	  MUTEX_UNLOCK (writer_info->flush_end_mutex);
	}
#endif
      assert (hold_flush_mutex == false);
      LOG_CS_PROMOTE (thread_p);
    }
#endif /* SERVER_MODE */

  return 1;

error:

  if (hold_lpb_mutex)
    {
      LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
    }
  if (hold_flush_mutex)
    {
      LOG_MUTEX_UNLOCK (flush_info->flush_mutex);
    }

  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
		     "logpb_flush_all_append_pages");

  return error_code;
}

/*
 * logpb_flush_all_append_pages - FLUSH LOG APPEND PAGES
 *
 * return: nothing
 *
 *   flush_type(in): type of flushing
 *
 * NOTE:There are 4 cases to commit.
 *              ASYNC | GROUP COMMIT
 *                X           X         : normal commit, wakeup LFT and wait
 *                X           O         : group commit, wait
 *                O           X         : async commit, wakeup LFT and return
 *                O           O         : async & group commit, just return
 *              Some thread have to flush log pages by itself, because they
 *              cannot release log cs.
 *              There are 3 types of flushing.
 *              LOG_FLUSH_NORMAL : normal threads's flush request.
 *              LOG_FLUSH_FORCE : have to wait LFT's flushall_append_pages.
 *              LOG_FLUSH_DIRECT : have to flushall_append_pages by itself.
 */
void
logpb_flush_all_append_pages (THREAD_ENTRY * thread_p,
			      LOG_FLUSH_TYPE flush_type)
{
#if !defined(SERVER_MODE)
  (void) logpb_flush_all_append_pages_helper (thread_p);
#else /* SERVER_MODE */
  int rv;
  int ret;
  struct timeval start_time = {
    0, 0
  };
  struct timeval end_time = {
    0, 0
  };
  double elapsed;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;
  LOG_GROUP_COMMIT_INFO *group_commit_info = &log_Gl.group_commit_info;

direct_flush:

  assert (LOG_CS_OWN (thread_p));

  /* While archiving,
   * log_Gl.append.log_pgptr == NULL
   * So, log_cs must not be released.
   */
  if (flush_info->flush_type == LOG_FLUSH_DIRECT
      || flush_type == LOG_FLUSH_DIRECT || !LOG_ISRESTARTED ())
    {
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "logpb_flush_all_append_pages: "
		    "[%d]flush direct\n", (int) THREAD_ID ());
#endif /* CUBRID_DEBUG */
      flush_type = flush_info->flush_type;
      flush_info->flush_type = LOG_FLUSH_DIRECT;
      (void) logpb_flush_all_append_pages_helper (thread_p);
      flush_info->flush_type = flush_type;
      log_Stat.direct_flush_count++;

      log_Stat.total_sync_count++;
      if (PRM_SUPPRESS_FSYNC == 0
	  || (log_Stat.total_sync_count % PRM_SUPPRESS_FSYNC == 0))
	{
	  (void) fileio_synchronize (thread_p, log_Gl.append.vdes,
				     log_Name_active);
	}

      return;
    }

  assert (LOG_ISRESTARTED ());

  if (PRM_LOG_ASYNC_COMMIT && LOG_IS_GROUP_COMMIT_ACTIVE ()
      && (flush_type != LOG_FLUSH_FORCE))
    {
      log_Stat.async_commit_request_count++;
      return;
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "logpb_flush_all_append_pages: "
		    "[%d]just return\n", (int) THREAD_ID ());
#endif /* CUBRID_DEBUG */
    }

  LOG_CS_EXIT ();

  MUTEX_LOCK (rv, thread_Log_flush_thread.lock);

  if (thread_Log_flush_thread.is_valid == false)
    {
      flush_type = LOG_FLUSH_DIRECT;
      MUTEX_UNLOCK (thread_Log_flush_thread.lock);
      LOG_CS_ENTER (thread_p);
      goto direct_flush;
    }

  if (!LOG_IS_GROUP_COMMIT_ACTIVE () || (flush_type == LOG_FLUSH_FORCE))
    {
      thread_Log_flush_thread.is_log_flush_force = true;
      COND_SIGNAL (thread_Log_flush_thread.cond);

#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "logpb_flush_all_append_pages: "
		    "[%d]wakeup flush thread\n", (int) THREAD_ID ());
#endif /* CUBRID_DEBUG */
    }

  MUTEX_LOCK (rv, group_commit_info->gc_mutex);
  MUTEX_UNLOCK (thread_Log_flush_thread.lock);

  if (!PRM_LOG_ASYNC_COMMIT || (flush_type == LOG_FLUSH_FORCE))
    {
      ++(group_commit_info->waiters);
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "logpb_flush_all_append_pages: "
		    "[%d]cond wait, flush_type(%d) waiters(%d)\n",
		    (int) THREAD_ID (),
		    flush_type, group_commit_info->waiters);
#endif /* CUBRID_DEBUG */
      if (LOG_IS_GROUP_COMMIT_ACTIVE ())
	{
	  gettimeofday (&start_time, NULL);
	}

      ret = COND_WAIT (group_commit_info->gc_cond,
		       group_commit_info->gc_mutex);
      assert (ret != TIMEDWAIT_TIMEOUT);

      if (LOG_IS_GROUP_COMMIT_ACTIVE () && (flush_type != LOG_FLUSH_FORCE))
	{
	  gettimeofday (&end_time, NULL);
	  elapsed = LOG_GET_ELAPSED_TIME (end_time, start_time);
	  log_Stat.gc_total_wait_time += elapsed;
	  log_Stat.gc_commit_request_count++;
	}

#if defined(CUBRID_DEBUG)
      {
	long temp = 1;

	if (log_Stat.gc_commit_request_count != 0)
	  temp = log_Stat.gc_commit_request_count;

	er_log_debug (ARG_FILE_LINE,
		      "logpb_flush_all_append_pages: "
		      "avg gc wait time(%f)\n",
		      log_Stat.gc_total_wait_time / temp);
      }

#endif /* CUBRID_DEBUG */
    }

  MUTEX_UNLOCK (group_commit_info->gc_mutex);
  LOG_CS_ENTER (thread_p);
#endif /* SERVER_MODE */
}

/*
 * logpb_invalid_all_append_pages - Invalidate all append pages
 *
 * return: nothing
 *
 * NOTE:Invalidate and free all append pages. Before invalidating the
 *              pages if their are dirty, they are flushed.
 */
void
logpb_invalid_all_append_pages (THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  assert (LOG_CS_OWN (thread_p));

  if (log_Gl.append.log_pgptr != NULL)
    {
      /*
       * Somehow we already have an append page, flush all current append page
       * and start form scratch
       */
      logpb_flush_all_append_pages (thread_p, LOG_FLUSH_DIRECT);
      logpb_free_page (log_Gl.append.log_pgptr);
      log_Gl.append.log_pgptr = NULL;
    }
  LOG_MUTEX_LOCK (rv, flush_info->flush_mutex);
  flush_info->num_toflush = 0;
  flush_info->toflush[flush_info->num_toflush] = NULL;
  LOG_MUTEX_UNLOCK (flush_info->flush_mutex);
}

/*
 * logpb_flush_log_for_wal - Flush log if needed
 *
 * return: nothing
 *
 *   lsa_ptr(in): Force all log records up to this lsa
 *
 * NOTE:Flush the log up to given log sequence address according to
 *              the WAL rule.
 *              The page buffer manager must call this function whenever a
 *              page is about to be flushed due to a page replacement.
 */
void
logpb_flush_log_for_wal (THREAD_ENTRY * thread_p, const LOG_LSA * lsa_ptr)
{
  if (LSA_LE (&log_Gl.append.nxio_lsa, lsa_ptr))
    {

      /* Need to set a latch since an end of log marker may be appended */

      LOG_CS_ENTER (thread_p);

      logpb_flush_all_append_pages (thread_p, LOG_FLUSH_FORCE);

#if defined(CUBRID_DEBUG)
      if (LSA_LE (&log_Gl.append.nxio_lsa, lsa_ptr)
	  && !LSA_EQ (&log_Gl.rcv_phase_lsa, lsa_ptr))
	{
	  er_log_debug (ARG_FILE_LINE,
			"log_wal: SYSTEM ERROR.. DUMP LOG BUFFER\n");
	  logpb_dump (stdout);
	}
#endif /* CUBRID_DEBUG */

      LOG_CS_EXIT ();
    }
}

/*
 * logpb_force - FORCE THE LOG
 *
 * return: nothing
 *
 * NOTE:Force the log.
 */
void
logpb_force (THREAD_ENTRY * thread_p)
{
  /* Need to set a latch since an end of log marker may be appended */
  assert (LOG_CS_OWN (thread_p));

  if (log_Pb.pool != NULL)
    {
      logpb_flush_all_append_pages (thread_p, LOG_FLUSH_FORCE);
    }
}

/*
 *
 *       	   FUNCTIONS RELATED TO DATA APPEND
 *
 */

/*
 * logpb_start_append - Start appending a new log record
 *
 * return: nothing
 *
 *   rectype(in): Type of the log record
 *               (e.g., LOG_BEFORE_AFTER_DATA, LOG_COMMIT)
 *   tdes(in/out): State structure of transaction of the log record
 *
 * NOTE:Start appending a new log record of the given type.
 */
void
logpb_start_append (THREAD_ENTRY * thread_p, LOG_RECTYPE rectype,
		    LOG_TDES * tdes)
{
  struct log_rec *log_rec;	/* Log record */
  PAGEID initial_pageid;

  assert (LOG_CS_OWN (thread_p));

  /* Record number of append log record in statistics */
  mnt_log_appendrecs (thread_p);

  /* Does the new log record fit in this page ? */
  LOG_APPEND_ADVANCE_WHEN_DOESNOT_FIT (thread_p, sizeof (*log_rec));

  initial_pageid = log_Gl.hdr.append_lsa.pageid;
  assert (log_Gl.append.log_pgptr != NULL);
  log_rec = (struct log_rec *) LOG_APPEND_PTR ();

  log_rec->trid = tdes->trid;
  log_rec->type = rectype;

  /*
   * Link the record with the previous transaction record for quick undos.
   * Link the record backward for backward traversal of the log.
   */
  LSA_COPY (&log_rec->prev_tranlsa, &tdes->tail_lsa);
  LSA_COPY (&log_rec->back_lsa, &log_Gl.append.prev_lsa);
  LSA_SET_NULL (&log_rec->forw_lsa);

  /*
   * Remember the address of new append record
   */

  LSA_COPY (&tdes->tail_lsa, &log_Gl.hdr.append_lsa);
  LSA_COPY (&tdes->undo_nxlsa, &log_Gl.hdr.append_lsa);

  /*
   * Is this the first log record of transaction ?
   */

  if (LSA_ISNULL (&tdes->head_lsa))
    {
      LSA_COPY (&tdes->head_lsa, &tdes->tail_lsa);
    }

  /*
   * If the header of the append page does not have the offset set to the
   * first log record, this is the first log record in the page, set to it.
   */

  if (log_Gl.append.log_pgptr->hdr.offset == NULL_OFFSET)
    {
      log_Gl.append.log_pgptr->hdr.offset = log_Gl.hdr.append_lsa.offset;
    }

  LSA_COPY (&log_Gl.append.prev_lsa, &log_Gl.hdr.append_lsa);

  /*
   * Set the page dirty, increase and align the append offset
   */
  LOG_APPEND_SETDIRTY_ADD_ALIGN (thread_p, sizeof (*log_rec));

  log_rec = (struct log_rec *) LOG_PREV_APPEND_PTR ();
  if (log_rec->type == LOG_DUMMY_FILLPAGE_FORARCHIVE)
    {
      /*
       * Get to start of next page if not already advanced ... Note
       * this record type is only safe during backups.
       */
      if (initial_pageid == log_Gl.hdr.append_lsa.pageid)
	{
	  LOG_APPEND_ADVANCE_WHEN_DOESNOT_FIT (thread_p, LOG_PAGESIZE);
	}
    }
}

/*
 * logpb_append_data - Append data
 *
 * return: nothing
 *
 *   length(in): Length of data to append
 *   data(in):  Data to append
 *
 * NOTE:Append data as part of current log record.
 */
void
logpb_append_data (THREAD_ENTRY * thread_p, int length, const char *data)
{
  int copy_length;		/* Amount of contiguos data that can be copied        */
  char *ptr;			/* Pointer for copy data into log append buffer       */
  char *last_ptr;		/* Pointer to last portion available to copy into log
				 * append buffer
				 */

  assert (LOG_CS_OWN (thread_p));

  if (length != 0 && data != NULL)
    {
      /*
       * Align if needed,
       * don't set it dirty since this function has not updated
       */
      LOG_APPEND_ALIGN (thread_p, LOG_DONT_SET_DIRTY);

      ptr = LOG_APPEND_PTR ();
      last_ptr = LOG_LAST_APPEND_PTR ();

      /* Does data fit completely in current page ? */
      if ((ptr + length) >= last_ptr)
	{
	  while (length > 0)
	    {
	      if (ptr >= last_ptr)
		{
		  /*
		   * Get next page and set the current one dirty
		   */
		  logpb_next_append_page (thread_p, LOG_SET_DIRTY);
		  ptr = LOG_APPEND_PTR ();
		  last_ptr = LOG_LAST_APPEND_PTR ();
		}
	      /* Find the amount of contiguous data that can be copied */
	      if (ptr + length >= last_ptr)
		{
		  copy_length = CAST_BUFLEN (last_ptr - ptr);
		}
	      else
		{
		  copy_length = length;
		}
	      memcpy (ptr, data, copy_length);
	      ptr += copy_length;
	      data += copy_length;
	      length -= copy_length;
	      log_Gl.hdr.append_lsa.offset += copy_length;
	    }
	}
      else
	{
	  memcpy (ptr, data, length);
	  log_Gl.hdr.append_lsa.offset += length;
	}
      /*
       * Align the data for future appends.
       * Indicate that modifications were done
       */
      LOG_APPEND_ALIGN (thread_p, LOG_SET_DIRTY);
    }
}

/*
 * logpb_append_crumbs - Append crumbs of data
 *
 * return: nothing
 *
 *   num_crumbs(in): Number of crumbs
 *   crumbs(in): The crumbs (length + data)
 *
 * NOTE: Append crumbs of data by gluing them. After this the log
 *              manager will lose track of what was glued.
 */
void
logpb_append_crumbs (THREAD_ENTRY * thread_p, int num_crumbs,
		     const LOG_CRUMB * crumbs)
{
  const char *data;		/* Data to copy                                 */
  char *ptr;			/* Pointer for copy data into log append buffer */
  char *last_ptr;		/* Pointer to last portion available to copy into
				 * log append buffer
				 */
  int copy_length;		/* Amount of contiguos data that can be copied  */
  int length;
  int i;

  assert (LOG_CS_OWN (thread_p));

  if (num_crumbs != 0)
    {
      /*
       * Align if needed,
       * don't set it dirty since this function has not updated
       */
      LOG_APPEND_ALIGN (thread_p, LOG_DONT_SET_DIRTY);

      ptr = LOG_APPEND_PTR ();
      last_ptr = LOG_LAST_APPEND_PTR ();

      for (i = 0; i < num_crumbs; i++)
	{
	  length = crumbs[i].length;
	  data = (char *) crumbs[i].data;

	  /* Does data fit completely in current page ? */
	  if ((ptr + length) >= last_ptr)
	    while (length > 0)
	      {
		if (ptr >= last_ptr)
		  {
		    /*
		     * Get next page and set the current one dirty
		     */
		    logpb_next_append_page (thread_p, LOG_SET_DIRTY);
		    ptr = LOG_APPEND_PTR ();
		    last_ptr = LOG_LAST_APPEND_PTR ();
		  }
		/* Find the amount of contiguous data that can be copied */
		if ((ptr + length) >= last_ptr)
		  {
		    copy_length = CAST_BUFLEN (last_ptr - ptr);
		  }
		else
		  {
		    copy_length = length;
		  }
		memcpy (ptr, data, copy_length);
		ptr += copy_length;
		data += copy_length;
		length -= copy_length;
		log_Gl.hdr.append_lsa.offset += copy_length;
	      }
	  else
	    {
	      memcpy (ptr, data, length);
	      ptr += length;
	      log_Gl.hdr.append_lsa.offset += length;
	    }
	}
      /*
       * Align the data for future appends.
       * Indicate that modifications were done
       */
      LOG_APPEND_ALIGN (thread_p, LOG_SET_DIRTY);
    }
}

/*
 * logpb_end_append - Finish appending a log record
 *
 * return: nothing
 *
 *   flush(in): Is it a requirement to flush the log ?
 *   from_commit(in): called from normal commit ?
 *   force_flush(in):
 *
 * NOTE:  Finish appending a log record. If the log record was appended
 *              in several log buffers, these buffers are flushed and freed.
 *              Only one append buffer will remain pin (fetched) in memory.
 *              If the log record was appended in only one buffer, the buffer
 *              is not flushed unless the caller requested flushing (e.g.,
 *              for a log_commit record).
 */
void
logpb_end_append (THREAD_ENTRY * thread_p)
{
  struct log_rec *log_rec;	/* The log record */
  PAGEID initial_pageid;	/* remember starting log page */

  assert (LOG_CS_OWN (thread_p));

  initial_pageid = log_Gl.hdr.append_lsa.pageid;
  LOG_APPEND_ALIGN (thread_p, LOG_DONT_SET_DIRTY);
  LOG_APPEND_ADVANCE_WHEN_DOESNOT_FIT (thread_p, sizeof (*log_rec));

  /*
   * Find the log_rec portion of the append record, it may not be in the
   * current append buffer since it can be stored in several buffers. Then,
   * make the log_rec point to next future append record, unless it is
   * the special record type used for archives created during backups
   * that cannot have a forward lsa and must waste the remaining space
   * on the current page.
   */

  log_rec = (struct log_rec *) LOG_PREV_APPEND_PTR ();
  LSA_COPY (&log_rec->forw_lsa, &log_Gl.hdr.append_lsa);

  if (log_Gl.append.delayed_free_log_pgptr != NULL)
    {
      assert (logpb_is_dirty (log_Gl.append.delayed_free_log_pgptr));

      logpb_set_dirty (log_Gl.append.delayed_free_log_pgptr, FREE);
      log_Gl.append.delayed_free_log_pgptr = NULL;
    }
  else
    {
      logpb_set_dirty (log_Gl.append.log_pgptr, DONT_FREE);
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * logpb_remove_append - Remove the appending of current log record
 *
 * return: nothing
 *
 *   tdes(in/out): State structure of transaction of the log record
 *
 * NOTE: Remove the current log record. This function is called when an
 *              error is found during the appending of a log record.
 */
void
logpb_remove_append (LOG_TDES * tdes)
{
  struct log_rec *log_rec;	/* The log record */
  int i;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  assert (0);

  /*
   * Find the log_rec portion of the append record, it may not be in the
   * current append buffer since we may have gone quite far appending the
   * the record before the failure
   */

  if (log_Gl.append.delayed_free_log_pgptr != NULL)
    {
      /* Go back to such buffer */
      LSA_COPY (&log_Gl.hdr.append_lsa, &log_Gl.append.prev_lsa);

      logpb_free_page (log_Gl.append.log_pgptr);
      log_Gl.append.log_pgptr = log_Gl.append.delayed_free_log_pgptr;
      log_Gl.append.delayed_free_log_pgptr = NULL;
      /*
       * Remove all the pages to flush ahead of current append
       */
      for (i = flush_info->num_toflush - 1; i > 0; i--)
	{
	  if (flush_info->toflush[i] == log_Gl.append.log_pgptr)
	    break;
	  flush_info->num_toflush--;
	}
    }
  else
    {
      log_Gl.hdr.append_lsa.offset = log_Gl.append.prev_lsa.offset;
    }

  /*
   * The following is correct since we are in the middle of a critical
   * section, therefore, the record that we are removing must belong to
   * current transaction
   */
  log_rec = (struct log_rec *) LOG_APPEND_PTR ();

  LSA_COPY (&tdes->tail_lsa, &log_rec->prev_tranlsa);
  LSA_COPY (&tdes->undo_nxlsa, &log_rec->prev_tranlsa);

  if (LSA_ISNULL (&tdes->tail_lsa))
    {
      LSA_SET_NULL (&tdes->head_lsa);
    }

  LSA_COPY (&log_Gl.append.prev_lsa, &log_rec->back_lsa);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 *
 *       	   FUNCTIONS RELATED TO LOG INFORMATION FILE
 *
 */

/*
 * logpb_create_log_info - Create a log information file
 *
 * return: nothing
 *
 *   logname_info(in): Name of the log information file
 *   db_fullname(in): Name of the database or NULL (defualt to current one)
 *
 * NOTE: Creates a log information file. This file is used as a help
 *              for the DBA of what things has been archived and what archive
 *              logs are not needed during normal restart recovery (i.e.,
 *              other than media crash).
 */
void
logpb_create_log_info (const char *logname_info, const char *db_fullname)
{
  FILE *fp;			/* Pointer to file */
  const char *catmsg;
  const char *db_name = db_fullname;
  int error_code = NO_ERROR;

  /* Create the information file */
  fp = fopen (logname_info, "w");
  if (fp != NULL)
    {
      fclose (fp);
      catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
			       MSGCAT_SET_LOG, MSGCAT_LOG_LOGINFO_COMMENT);
      if (db_name == NULL)
	{
	  db_name = log_Db_fullname;
	}
      if (catmsg == NULL)
	{
	  catmsg = "COMMENT: %s for database %s\n";
	}
      error_code = log_dump_log_info (logname_info, false, catmsg,
				      CUBRID_MAGIC_LOG_INFO, db_name);
      if (error_code != NO_ERROR)
	{
	  return;
	}

      (void) logpb_add_volume (db_fullname, LOG_DBLOG_INFO_VOLID,
			       logname_info, DISK_UNKNOWN_PURPOSE);
    }
}


/*
 * logpb_get_guess_archive_num - Guess archive number
 *
 * return: arvnum or -1
 *
 *   pageid(in): Desired page
 *
 * NOTE: Guess the archive number where the desired page is archived by
 *              searching the log information file.
 */
static int
logpb_get_guess_archive_num (PAGEID pageid)
{
  FILE *fp;
  char line[LOG_MAX_LOGINFO_LINE];
  int arv_num = -1;
  int last_arvnum = -1;
  int next_arvnum;
  bool isfound = false;
  PAGEID from_pageid;
  PAGEID to_pageid;

  /*
   * Guess by looking into the log information file. This is just a guess
   */
  fp = fopen (log_Name_info, "r");
  if (fp != NULL)
    {
      while (fgets (line, LOG_MAX_LOGINFO_LINE, fp) != NULL)
	{
	  if (strstr (line + TIME_SIZE_OF_DUMP_LOG_INFO,
		      msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOG,
				      MSGCAT_LOG_LOGINFO_KEYWORD_ARCHIVE))
	      == line + TIME_SIZE_OF_DUMP_LOG_INFO)
	    {
	      /* A candidate for a guess */
	      if (sscanf (line + TIME_SIZE_OF_DUMP_LOG_INFO,
			  "%*s %d %*s %d %d",
			  &next_arvnum, &from_pageid, &to_pageid) == 3)
		{

		  last_arvnum = next_arvnum;

		  if (pageid < from_pageid)
		    {
		      /*
		       * keep looking.
		       * There is likely a hole in the archive process due to media
		       * crashes off or the log information contains some missing
		       * entries.
		       */
		      continue;
		    }

		  arv_num = next_arvnum;

		  if (pageid >= from_pageid && pageid <= to_pageid)
		    {
		      /* Found the page in this archive */
		      isfound = true;
		      break;
		    }
		}
	    }
	}
      fclose (fp);
    }

  if (arv_num == -1)
    {
      /*
       * If I have a log active, use it to find out a better archive number
       * for initial search
       */
      if (log_Gl.append.vdes != NULL_VOLDES)
	{
	  arv_num = pageid / LOGPB_ACTIVE_NPAGES;
	}
      else
	{
	  /*
	   * We do not have a clue what it is available. Don't have log active
	   * and likely we did not have backups.
	   * Must trace for available archive volumes
	   */
	  arv_num = 0;
	}
    }
  else if (isfound == false && last_arvnum == arv_num
	   && log_Gl.append.vdes != NULL_VOLDES)
    {
      /*
       * The log archive was chopped somehow.
       */
      arv_num = log_Gl.hdr.nxarv_num - 1;
    }

  /* Insure that we never pick one larger than the next one to be created */
  if (arv_num >= log_Gl.hdr.nxarv_num)
    {
      arv_num = log_Gl.hdr.nxarv_num - 1;
    }

  return arv_num;
}

/*
 * logpb_find_volume_info_exist - Find if volume information exists ?
 *
 * return:
 *
 * NOTE: Find if volume information exist.
 */
bool
logpb_find_volume_info_exist (void)
{
  return fileio_is_volume_exist (log_Name_volinfo);
}

/*
 * logpb_create_volume_info - Create the volume information and add first volume
 *
 * return: NO_ERROR or error code
 *
 *   db_fullname(in): Name of the database or NULL (defualt to current one)
 *
 * NOTE: Create the volume information and add the first volume.
 */
int
logpb_create_volume_info (const char *db_fullname)
{
  char vol_fullname[PATH_MAX];
  char *volinfo_fullname;
  FILE *volinfo_fp = NULL;

  if (db_fullname != NULL)
    {
      fileio_make_volume_info_name (vol_fullname, db_fullname);
      volinfo_fullname = vol_fullname;
    }
  else
    {
      volinfo_fullname = log_Name_volinfo;
    }

  volinfo_fp = fopen (volinfo_fullname, "w");
  if (volinfo_fp == NULL)
    {
      /* Unable to create the database volume information */
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_BO_CANNOT_CREATE_VOL, 2,
			   volinfo_fullname, db_fullname);
      return ER_BO_CANNOT_CREATE_VOL;
    }
  /*
   * Write information about:
   * the active log and the first volume of the database
   * in the volume information file
   */
  fprintf (volinfo_fp, "%4d %s\n", LOG_DBVOLINFO_VOLID, volinfo_fullname);

  fflush (volinfo_fp);
  fclose (volinfo_fp);

  return NO_ERROR;
}

/*
 * logpb_recreate_volume_info - Recreate the database volume information
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 * NOTE: Recreate the database volume information from the internal
 *              information that is stored in each volume.
 */
int
logpb_recreate_volume_info (THREAD_ENTRY * thread_p)
{
  VOLID next_volid = LOG_DBFIRST_VOLID;	/* Next volume identifier */
  char next_vol_fullname[PATH_MAX];	/* Next volume name       */
  int error_code = NO_ERROR;

  error_code = logpb_create_volume_info (NULL);
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  if (logpb_add_volume (NULL, LOG_DBLOG_INFO_VOLID, log_Name_info,
			DISK_UNKNOWN_PURPOSE) != LOG_DBLOG_INFO_VOLID)
    {
      error_code = ER_FAILED;
      goto error;
    }
  if (logpb_add_volume (NULL, LOG_DBLOG_BKUPINFO_VOLID,
			log_Name_bkupinfo,
			DISK_UNKNOWN_PURPOSE) != LOG_DBLOG_BKUPINFO_VOLID)
    {
      error_code = ER_FAILED;
      goto error;
    }
  if (logpb_add_volume (NULL, LOG_DBLOG_ACTIVE_VOLID, log_Name_active,
			DISK_UNKNOWN_PURPOSE) != LOG_DBLOG_ACTIVE_VOLID)
    {
      error_code = ER_FAILED;
      goto error;
    }

  /* First the primary volume, then the rest of the volumes */
  next_volid = LOG_DBFIRST_VOLID;
  strcpy (next_vol_fullname, log_Db_fullname);

  do
    {
      if (logpb_add_volume (NULL, next_volid, next_vol_fullname,
			    DISK_PERMVOL_GENERIC_PURPOSE) != next_volid)
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      if (disk_get_link (thread_p, next_volid, next_vol_fullname) == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      next_volid++;
    }
  while (next_vol_fullname[0] != '\0');

  return error_code;

  /* ****** */
error:
  (void) remove (log_Name_volinfo);
  return error_code;
}

/*
 * logpb_add_volume - Add a new volume entry to the volume information
 *
 * return: new_volid or NULL_VOLID
 *
 *   db_fullname(in):
 *   new_volid(in): New volume identifier
 *   new_volfullname(in): New volume name
 *   new_volpurpose(in): Purpose of new volume
 *
 * NOTE: Add a new entry to the volume information
 */
VOLID
logpb_add_volume (const char *db_fullname, VOLID new_volid,
		  const char *new_volfullname, DISK_VOLPURPOSE new_volpurpose)
{
  if (new_volpurpose != DISK_TEMPVOL_TEMP_PURPOSE)
    {
      char vol_fullname[PATH_MAX];
      char *volinfo_fullname;
      FILE *volinfo_fp = NULL;

      if (db_fullname != NULL)
	{
	  fileio_make_volume_info_name (vol_fullname, db_fullname);
	  volinfo_fullname = vol_fullname;
	}
      else
	{
	  volinfo_fullname = log_Name_volinfo;
	}

      volinfo_fp = fopen (volinfo_fullname, "a");
      if (volinfo_fp != NULL)
	{
	  /* Write information about this volume in the volume information file */
	  fprintf (volinfo_fp, "%4d %s\n", new_volid, new_volfullname);
	  fflush (volinfo_fp);
	  fclose (volinfo_fp);

	  return new_volid;
	}
      else
	{
	  return NULL_VOLID;
	}
    }

  return new_volid;
}

/*
 * logpb_scan_volume_info - Scan the volume information entries
 *
 * return: number of entries or -1 in case of error.
 *
 *   db_fullname(in):
 *   ignore_volid(in): Don't call function with this volume
 *   start_volid(in): Scan should start at this point.
 *   fun(in): Function to be called on each entry
 *   args(in): Additional arguments to be passed to function
 *
 * NOTE: Scan the volume information entries calling the given function
 *              on each entry.
 */
int
logpb_scan_volume_info (THREAD_ENTRY * thread_p, const char *db_fullname,
			VOLID ignore_volid, VOLID start_volid,
			int (*fun) (THREAD_ENTRY * thread_p, VOLID xvolid,
				    const char *vlabel, void *args),
			void *args)
{
  char xxvolinfo_fullname[PATH_MAX];
  char *volinfo_fullname;
  FILE *volinfo_fp = NULL;	/* Pointer to new volinfo */
  char vol_fullname[PATH_MAX];	/* Next volume name       */
  VOLID volid = LOG_DBFIRST_VOLID - 1;	/* Next volume identifier */
  int read_int_volid;
  VOLID num_vols = 0;
  bool start_scan = false;
  char format_string[64];

  if (db_fullname != NULL)
    {
      fileio_make_volume_info_name (xxvolinfo_fullname, db_fullname);
      volinfo_fullname = xxvolinfo_fullname;
    }
  else
    {
      volinfo_fullname = log_Name_volinfo;
    }

  volinfo_fp = fopen (volinfo_fullname, "r");
  if (volinfo_fp == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_CANNOT_FINE_VOLINFO, 1,
	      volinfo_fullname);
      return -1;
    }

  sprintf (format_string, "%%d %%%ds", PATH_MAX - 1);
  while (true)
    {
      if (fscanf (volinfo_fp, format_string, &read_int_volid,
		  vol_fullname) != 2)
	{
	  break;
	}

      if ((volid + 1) != NULL_VOLID
	  && (volid + 1) != (VOLID) read_int_volid && num_vols != 0)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_BO_UNSORTED_VOLINFO,
		  4, volinfo_fullname, num_vols, read_int_volid,
		  vol_fullname);
	  num_vols = -1;
	  break;
	}
      volid = (VOLID) read_int_volid;

      if (volid == NULL_VOLID)
	{
	  continue;
	}

      if (start_scan == false)
	{
	  if (start_volid == read_int_volid)
	    {
	      start_scan = true;
	    }
	  else
	    {
	      continue;
	    }
	}

      if (volid != ignore_volid)
	{
	  num_vols++;
	  if (((*fun) (thread_p, volid, vol_fullname, args)) != NO_ERROR)
	    {
	      break;
	    }
	}
    }

  fclose (volinfo_fp);

  return num_vols;
}

/*
 *
 *       	   FUNCTIONS RELATED TO LOG ARCHIVES
 *
 */

/*
 * logpb_to_physical_pageid - Find physical page identifier of given logic page
 *
 * return: phy page identifier
 *
 *   logical_pageid(in): logical_pageid: Logical log page
 *
 * NOTE: Returns the physical page identifier associated with given
 *              logical page.
 */
PAGEID
logpb_to_physical_pageid (PAGEID logical_pageid)
{
  int phy_pageid;

  if (logical_pageid == LOGPB_HEADER_PAGE_ID)
    {
      phy_pageid = LOGPB_PHYSICAL_HEADER_PAGE_ID;
    }
  else
    {
      phy_pageid = logical_pageid - LOGPB_FIRST_ACTIVE_PAGE_ID;
      if (phy_pageid >= LOGPB_ACTIVE_NPAGES)
	{
	  phy_pageid %= LOGPB_ACTIVE_NPAGES;
	}
      else if (phy_pageid < 0)
	{
	  phy_pageid =
	    LOGPB_ACTIVE_NPAGES - ((-phy_pageid) % LOGPB_ACTIVE_NPAGES);
	}

      phy_pageid++;
      if (phy_pageid > LOGPB_ACTIVE_NPAGES)
	{
	  phy_pageid %= LOGPB_ACTIVE_NPAGES;
	}
    }

  return phy_pageid;
}

/*
 * logpb_is_page_in_archive - Is the given page an archive page ?
 *
 * return:
 *
 *   pageid(in): Log page identifier
 *
 * NOTE:Find if given page is an archive page identifier.
 */
bool
logpb_is_page_in_archive (PAGEID pageid)
{
  return LOGPB_IS_ARCHIVE_PAGE (pageid);
}

/*
 * logpb_is_smallest_lsa_in_archive - IS THE SMALLEST ACTIVE OF THE LOG ARCHIVE ?
 *
 * return:
 *
 * NOTE: Returns true if the smallest active LSA is located in an
 *              archive log.
 */
bool
logpb_is_smallest_lsa_in_archive (THREAD_ENTRY * thread_p)
{
  LOG_LSA lsa;			/* smallest lsa */

  logtb_find_smallest_lsa (thread_p, &lsa);
  return (!LSA_ISNULL (&lsa) && logpb_is_page_in_archive (lsa.pageid));
}

/*
 * logpb_get_archive_number - Archive location of given page
 *
 * return: archive number
 *
 *   pageid(in): The desired logical page
 *
 * NOTE: Find in what archive the page is located or in what archive
 *              the page should have been located.
 */
int
logpb_get_archive_number (THREAD_ENTRY * thread_p, PAGEID pageid)
{
  int arv_num = 0;

  (void) logpb_fetch_from_archive (thread_p, pageid, NULL, &arv_num, NULL);
  if (arv_num < 0)
    {
      arv_num = 0;
    }

  return arv_num;
}

/*
 * logpb_set_unavailable_archive - Cache that given archive is unavailable
 *
 * return: nothing
 *
 *   arv_num(in): Log archive number
 *
 * NOTE: Record that give archive is unavialble.
 */
static void
logpb_set_unavailable_archive (int arv_num)
{
  int *ptr;
  int size;

  assert (LOG_CS_OWN (NULL));

  if (log_Gl.archive.unav_archives == NULL)
    {
      size = sizeof (*log_Gl.archive.unav_archives) * 10;
      ptr = (int *) malloc (size);
      if (ptr == NULL)
	{
	  return;
	}
      log_Gl.archive.max_unav = 10;
      log_Gl.archive.next_unav = 0;
      log_Gl.archive.unav_archives = ptr;
    }
  else
    {
      if ((log_Gl.archive.next_unav + 1) >= log_Gl.archive.max_unav)
	{
	  size = (sizeof (*log_Gl.archive.unav_archives) *
		  (log_Gl.archive.max_unav + 10));
	  ptr = (int *) realloc (log_Gl.archive.unav_archives, size);
	  if (ptr == NULL)
	    {
	      return;
	    }
	  log_Gl.archive.max_unav += 10;
	  log_Gl.archive.unav_archives = ptr;
	}
    }

  log_Gl.archive.unav_archives[log_Gl.archive.next_unav++] = arv_num;
}

/*
 * logpb_decache_archive_info - Decache any archive log memory information
 *
 * return: nothing
 *
 * NOTE: Decache any archive log memory information.
 */
void
logpb_decache_archive_info (THREAD_ENTRY * thread_p)
{
  assert (LOG_CS_OWN (NULL));

  if (log_Gl.archive.vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, log_Gl.archive.vdes);
      log_Gl.archive.vdes = NULL_VOLDES;
    }
  if (log_Gl.archive.unav_archives != NULL)
    {
      free_and_init (log_Gl.archive.unav_archives);
      log_Gl.archive.max_unav = 0;
      log_Gl.archive.next_unav = 0;
    }
}

/*
 * log_isarchive_available - Is given archive available ?
 *
 * return: true/false
 *        true: means that the archive may be available.
 *       false: it is known that archive is not available.
 *
 *   arv_num(in): Log archive number
 *
 * NOTE:Find if the current archive is available.
 */
static bool
logpb_is_archive_available (int arv_num)
{
  int i;

  assert (LOG_CS_OWN (NULL));

  if (arv_num >= log_Gl.hdr.nxarv_num || arv_num < 0)
    {
      return false;
    }

  if (log_Gl.archive.unav_archives != NULL)
    {
      for (i = 0; i < log_Gl.archive.next_unav; i++)
	{
	  if (log_Gl.archive.unav_archives[i] == arv_num)
	    {
	      return false;
	    }
	}
    }

  return true;
}

/*
 * log_fetch_from_archive - Fetch a log page from the log archives
 *
 * return: log_pgptr or NULL (in case of error)
 *
 *   pageid(in): The desired logical page
 *   log_pgptr(in): Place to return the log page
 *   arv_num(in): Set to archive number where page was found or where page
 *                 should have been found.
 *
 * NOTE: Fetch a log page from archive logs.
 */
LOG_PAGE *
logpb_fetch_from_archive (THREAD_ENTRY * thread_p, PAGEID pageid,
			  LOG_PAGE * log_pgptr, int *ret_arv_num,
			  struct log_arv_header * ret_arv_hdr)
{
  char hdr_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_hdr_pgbuf;
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_log_pgbuf;
  struct log_arv_header *arv_hdr;
  LOG_PAGE *hdr_pgptr;
  PAGEID phy_pageid = NULL_PAGEID;
  char arv_name[PATH_MAX];
  const char *tmp_arv_name;
  int arv_num, vdes;
  int direction = 0, retry;
  bool has_guess_arvnum = false, first_time = true;
  int error_code = NO_ERROR;
  char format_string[64];

  aligned_hdr_pgbuf = PTR_ALIGN (hdr_pgbuf, MAX_ALIGNMENT);
  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

  assert (LOG_CS_OWN (thread_p));

#if !defined(NDEBUG)
  if (PRM_LOG_TRACE_DEBUG)
    fprintf (stdout, "\n **log_fetch_from_archive has been called on"
	     " pageid = %d ** \n", pageid);
#endif

  hdr_pgptr = (LOG_PAGE *) aligned_hdr_pgbuf;
  if (log_pgptr == NULL)
    {
      log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;
    }
  if (ret_arv_num == NULL)
    {
      ret_arv_num = &arv_num;
    }

  if (log_Gl.archive.vdes == NULL_VOLDES)
    {
      if (log_Gl.hdr.nxarv_num <= 0)
	{
	  /* We do not have any archives */
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_NOTIN_ARCHIVE, 1, pageid);
	  return NULL;
	}

      /*
       * Guess the archive where that page is stored
       */

      has_guess_arvnum = true;
      *ret_arv_num = logpb_get_guess_archive_num (pageid);
      fileio_make_log_archive_name (arv_name, log_Archive_path, log_Prefix,
				    *ret_arv_num);

      error_code = ER_FAILED;
      if (logpb_is_archive_available (*ret_arv_num) == true
	  && fileio_is_volume_exist (arv_name) == true)
	{
	  vdes = fileio_mount (thread_p, log_Db_fullname, arv_name,
			       LOG_DBLOG_ARCHIVE_VOLID, false, false);
	  if (vdes != NULL_VOLDES)
	    {
	      if (fileio_read (thread_p, vdes, hdr_pgptr, 0, LOG_PAGESIZE)
		  == NULL)
		{
		  fileio_dismount (thread_p, vdes);
		  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_READ,
			  3, 0, 0, arv_name);
		  return NULL;
		}
	      error_code = NO_ERROR;
	      arv_hdr = (struct log_arv_header *) hdr_pgptr->area;
	      if (log_Gl.append.vdes != NULL_VOLDES)
		{
		  if (difftime64 ((time_t) arv_hdr->db_creation,
				  (time_t) log_Gl.hdr.db_creation) != 0)
		    {
		      /*
		       * This volume does not belong to the database. For now, assume
		       * that it is not only. Later, we will give this error to user
		       */
		      vdes = NULL_VOLDES;
		      arv_hdr = NULL;
		    }
		}
	    }
	}

      if (error_code != NO_ERROR)
	{
	  /*
	   * The volume is not online. Ask for it later (below). But first try to
	   * make the best guess for the archive number.
	   */
	  vdes = NULL_VOLDES;
	  arv_hdr = NULL;
	}
    }
  else
    {
      vdes = log_Gl.archive.vdes;
      arv_hdr = &log_Gl.archive.hdr;
      *ret_arv_num = arv_hdr->arv_num;
    }

  sprintf (format_string, "%%%ds", PATH_MAX - 1);

  log_Gl.archive.vdes = NULL_VOLDES;
  while (true)
    {
      /* Is the page in current archive log ? */
      if (arv_hdr != NULL
	  && pageid >= arv_hdr->fpageid
	  && pageid <= arv_hdr->fpageid + arv_hdr->npages - 1)
	{
	  /* Find location of logical page in the archive log */
	  phy_pageid = pageid - arv_hdr->fpageid + 1;

	  /* Record number of reads in statistics */
	  mnt_log_ioreads (thread_p);

	  if (fileio_read (thread_p, vdes, log_pgptr, phy_pageid,
			   LOG_PAGESIZE) == NULL)
	    {
	      /* Error reading archive page */
	      tmp_arv_name = fileio_get_volume_label_by_fd (vdes);
	      fileio_dismount (thread_p, vdes);
	      log_Gl.archive.vdes = NULL_VOLDES;
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_READ, 3,
		      pageid, phy_pageid, tmp_arv_name);
	      return NULL;
	    }
	  /* Cast the archive information. May be used again */
	  if (arv_hdr != &log_Gl.archive.hdr)
	    {
	      log_Gl.archive.hdr = *arv_hdr;
	    }
	  log_Gl.archive.vdes = vdes;
	  break;
	}
      else
	{
	  /* If any archive dismount it */
	  if (vdes != NULL_VOLDES)
	    {
	      fileio_dismount (thread_p, vdes);
	      vdes = NULL_VOLDES;
	    }

	  if (has_guess_arvnum == false)
	    {
	      has_guess_arvnum = true;
	      retry = logpb_get_guess_archive_num (pageid);
	      if (retry != *ret_arv_num)
		{
		  *ret_arv_num = retry;
		}
	    }
	  else
	    {
	      if (direction == 0)
		{
		  /*
		   * Define the direction by looking for desired page
		   */
		  if (arv_hdr != NULL)
		    {
		      if (pageid < arv_hdr->fpageid)
			{
			  /* Try older archives */
			  direction = -1;
			}
		      else
			{
			  /* Try newer archives */
			  direction = 1;
			}
		    }
		  else
		    {
		      if (first_time != true)
			{
			  if (log_Gl.append.vdes == NULL_VOLDES)
			    {
			      direction = 1;
			    }
			  else
			    {
			      /*
			       * Start looking from the last archive.
			       * Optimized for UNDO.. This is not so bad since this branch
			       * will be reached only when the guess archive is not
			       * available.
			       */
			      *ret_arv_num = log_Gl.hdr.nxarv_num;
			      direction = -1;
			    }
			}
		    }
		}

	      if (arv_hdr != NULL)
		{
		  if (direction == -1)
		    {
		      /*
		       * Try an older archive.
		       * The page that I am looking MUST be smaller than the first
		       * page in current archive
		       */
		      if (pageid < arv_hdr->fpageid)
			{
			  *ret_arv_num -= 1;
			}
		      else
			{
			  *ret_arv_num = -1;
			}
		    }
		  else
		    {
		      /* Try a newer archive.
		       * The page that I am looking MUST be larger than the last page in
		       * current archive
		       */
		      if (pageid > arv_hdr->fpageid + arv_hdr->npages - 1)
			{
			  *ret_arv_num += 1;
			}
		      else
			{
			  *ret_arv_num = log_Gl.hdr.nxarv_num;
			}
		    }
		}
	      else
		{
		  /*
		   * The archive number is not increased the first time in the loop,
		   * so we can ask for it when it is not available.
		   */
		  if (first_time != true)
		    {
		      /*
		       * If we do not have the log active, we don't really know how to
		       * continue, we could be looping forever.
		       */
		      if (log_Gl.append.vdes == NULL_VOLDES)
			{
			  *ret_arv_num = -1;
			}
		      else
			{
			  *ret_arv_num = *ret_arv_num + direction;
			}
		    }
		}

	      first_time = false;
	      if (*ret_arv_num < 0 || *ret_arv_num == log_Gl.hdr.nxarv_num)
		{
		  /* Unable to find page in archive */
		  if (log_Gl.append.vdes != NULL_VOLDES)
		    {
		      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_LOG_NOTIN_ARCHIVE, 1, pageid);
		    }
		  else
		    {
		      /*
		       * This is likely an incomplete recovery (restore).
		       * We do not have the active log and we are looking for a log page
		       */
		      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			      ER_LOG_NOTIN_ARCHIVE, 1, pageid);
		    }

		  return NULL;
		}
	    }

	  if (logpb_is_archive_available (*ret_arv_num) == false)
	    {
	      arv_hdr = NULL;
	      continue;
	    }

	  fileio_make_log_archive_name (arv_name, log_Archive_path,
					log_Prefix, *ret_arv_num);
	  retry = 3;
	  while (retry != 0 && retry != 1
		 && (vdes = fileio_mount (thread_p, log_Db_fullname, arv_name,
					  LOG_DBLOG_ARCHIVE_VOLID,
					  false, false)) == NULL_VOLDES)
	    {
	      char line_buf[PATH_MAX * 2];

	      /*
	       * The archive is not online.
	       */
	      fprintf (stdout, "%s\n", er_msg ());
	    retry_prompt:
	      if (log_default_input_for_archive_log_location >= 0)
		{
		  retry = log_default_input_for_archive_log_location;
		  if (retry == 1)
		    {
		      fprintf (stdout,
			       "Continue without present archive. (Partial recovery).\n");
		    }
		}
	      else
		{
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
						   MSGCAT_SET_LOG,
						   MSGCAT_LOG_STARTS));
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
						   MSGCAT_SET_LOG,
						   MSGCAT_LOG_LOGARCHIVE_NEEDED),
			   arv_name);
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
						   MSGCAT_SET_LOG,
						   MSGCAT_LOG_STARTS));

		  if (fgets (line_buf, PATH_MAX, stdin) == NULL)
		    {
		      retry = 0;	/* EOF */
		    }
		  else if (sscanf (line_buf, "%d", &retry) != 1)
		    {
		      retry = -1;	/* invalid input */
		    }
		}

	      switch (retry)
		{
		case 0:	/* quit */
		  logpb_set_unavailable_archive (*ret_arv_num);
		  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_LOG_NOTIN_ARCHIVE, 1, pageid);
		  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				     "log_fetch_from_archive");
		  return NULL;

		case 1:	/* Not available */
		  logpb_set_unavailable_archive (*ret_arv_num);
		  break;

		case 3:	/* Relocate */
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
						   MSGCAT_SET_LOG,
						   MSGCAT_LOG_NEWLOCATION));
		  if (fgets (line_buf, PATH_MAX, stdin) == 0
		      || (sscanf (line_buf, format_string, arv_name) != 1))
		    {
		      fileio_make_log_archive_name (arv_name,
						    log_Archive_path,
						    log_Prefix, *ret_arv_num);
		    }
		  break;

		case 2:	/* Retry */
		  break;

		default:	/* Something strange.  Get user to try again. */
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
						   MSGCAT_SET_LOG,
						   MSGCAT_LOG_INPUT_RANGE_ERROR),
			   0, 3);
		  goto retry_prompt;
		}
	    }

	  if (vdes != NULL_VOLDES)
	    {
	      /* Read header page and make sure the page is here */

	      /* Record number of reads in statistics */
	      mnt_log_ioreads (thread_p);

	      if (fileio_read (thread_p, vdes, hdr_pgptr, 0, LOG_PAGESIZE)
		  == NULL)
		{
		  fileio_dismount (thread_p, vdes);
		  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_READ,
			  3, 0, 0, arv_name);
		  return NULL;
		}
	      arv_hdr = (struct log_arv_header *) hdr_pgptr->area;
	      if (log_Gl.append.vdes != NULL_VOLDES)
		{
		  if (difftime64 ((time_t) arv_hdr->db_creation,
				  (time_t) log_Gl.hdr.db_creation) != 0)
		    {
		      /*
		       * This volume does not belong to the database. For now, assume
		       * that it is not only. Later, we will give this error to user
		       */
		      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_LOG_DOESNT_CORRESPOND_TO_DATABASE, 1,
			      arv_name);
		      arv_hdr = NULL;
		    }
		}
	    }
	  else
	    {
	      arv_hdr = NULL;
	    }
	}
    }

#if defined(CUBRID_DEBUG)
  if (log_pgptr->hdr.logical_pageid != pageid)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_PAGE_CORRUPTED,
	      1, pageid);
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_fetch_from_archive");
      return NULL;
    }
#endif /* CUBRID_DEBUG */

  assert (log_pgptr != NULL && *ret_arv_num != -1 && arv_hdr != NULL);
  if (ret_arv_hdr != NULL)
    {
      *ret_arv_hdr = *arv_hdr;
    }
  return log_pgptr;
}


/*
 * logpb_archive_active_log - Archive the active portion of the log
 *
 * return: nothing
 *
 *   force_archive(in):
 *
 * NOTE: The active portion of the log is archived from the next log
 *              archive page to the previous log page of the current append
 *              log record, to the next log archive.
 */
static void
logpb_archive_active_log (THREAD_ENTRY * thread_p, bool force_archive)
{
  char arv_name[PATH_MAX] = { '\0' };	/* Archive name        */
  LOG_PAGE *malloc_arv_hdr_pgptr = NULL;	/* Archive header page
						 * PTR
						 */
  struct log_arv_header *arvhdr;	/* Archive header      */
  BACKGROUND_ARCHIVING_INFO *bg_arv_info;
  char log_pgbuf[IO_MAX_PAGE_SIZE * LOGPB_IO_NPAGES + MAX_ALIGNMENT];
  char *aligned_log_pgbuf;
  LOG_PAGE *log_pgptr = NULL;
  PAGEID pageid, last_pageid, ar_phy_pageid;
  LOG_LSA smallest_lsa, newsmallest_lsa;
  int vdes = NULL_VOLDES;
  const char *catmsg;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
  int error_code = NO_ERROR;
  LOG_FLUSH_TYPE old_flush_type;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;
  int num_pages = 0;

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

  assert (LOG_CS_OWN (thread_p));

  old_flush_type = flush_info->flush_type;
  flush_info->flush_type = LOG_FLUSH_DIRECT;

#if !defined(SERVER_MODE)
  logpb_remove_archive_logs_exceed_limit (thread_p);
#endif

  if (log_Gl.hdr.nxarv_pageid >= log_Gl.hdr.append_lsa.pageid)
    {
      er_log_debug (ARG_FILE_LINE, "log_archive_active_log: WARNING "
		    "Trying to archive ONLY the append page"
		    " which is incomplete\n");
      flush_info->flush_type = old_flush_type;
      return;
    }

  bg_arv_info = &log_Gl.bg_archive_info;
  if (log_Gl.archive.vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, log_Gl.archive.vdes);
      log_Gl.archive.vdes = NULL_VOLDES;
    }

  malloc_arv_hdr_pgptr = (LOG_PAGE *) malloc (LOG_PAGESIZE);
  if (malloc_arv_hdr_pgptr == NULL)
    {
      goto error;
    }
  memset (malloc_arv_hdr_pgptr, 0, LOG_PAGESIZE);

  /* Must force the log here to avoid nasty side effects */
  logpb_flush_all_append_pages (thread_p, LOG_FLUSH_DIRECT);

  malloc_arv_hdr_pgptr->hdr.logical_pageid = LOGPB_HEADER_PAGE_ID;
  malloc_arv_hdr_pgptr->hdr.offset = NULL_OFFSET;

  /* Construct the archive log header */
  arvhdr = (struct log_arv_header *) malloc_arv_hdr_pgptr->area;
  strncpy (arvhdr->magic, CUBRID_MAGIC_LOG_ARCHIVE, CUBRID_MAGIC_MAX_LENGTH);
  arvhdr->db_creation = log_Gl.hdr.db_creation;
  arvhdr->next_trid = log_Gl.hdr.next_trid;
  arvhdr->arv_num = log_Gl.hdr.nxarv_num;

  if (PRM_LOG_MEDIA_FAILURE_SUPPORT || force_archive)
    {
      /*
       * All pages must be archived... even the ones with unactive log records
       * This is the desired parameter to support multimedia crashes.
       *
       *
       * Note that the npages field does not include the previous lsa page
       *
       */
      arvhdr->fpageid = log_Gl.hdr.nxarv_pageid;
      last_pageid = log_Gl.append.prev_lsa.pageid;

      if (force_archive)
	{
	  /*
	   * When forcing an archive for backup purposes, it is imperative that
	   * every single log record make it into the archive including the
	   * current page.  This often means archiving an incomplete page.
	   * To archive the last page in a way that recovery analysis will
	   * realize it is incomplete, requires a dummy record with no forward
	   * lsa pointer.  It also requires the the next record after that
	   * be appended to a new page (which will happen automatically).
	   */
	  if (logpb_must_archive_last_log_page (thread_p) != NO_ERROR)
	    {
	      goto error;
	    }
	}
      else if (log_Gl.hdr.append_lsa.pageid == log_Gl.append.prev_lsa.pageid)
	{
	  /* Avoid archiving an incomplete page (may lead to a an
	   * incomplete restore) */
	  last_pageid--;
	}
      if (last_pageid < arvhdr->fpageid)
	{
	  last_pageid = arvhdr->fpageid;
	}
      arvhdr->npages = last_pageid - arvhdr->fpageid + 1;
    }
  else
    {
      /*
       * Only pages between active log records are archived.
       * This feature is only used to support very long active transactions
       * that use lot of log pages.
       * This feature does not support multimedia crashes
       */

      logtb_find_smallest_and_largest_active_pages (thread_p,
						    &arvhdr->fpageid,
						    &last_pageid);

      if (arvhdr->fpageid < log_Gl.hdr.nxarv_pageid)
	{
	  arvhdr->fpageid = log_Gl.hdr.nxarv_pageid;
	}
      else if (arvhdr->fpageid > log_Gl.hdr.nxarv_pageid)
	{
	  /*
	   * We may not need to archive at this moment if we can flush all dirty
	   * data pages whose LSA is smaller that the smallest LSA of
	   * arvhdr->fpageid
	   */

	  smallest_lsa.pageid = arvhdr->fpageid;
	  smallest_lsa.offset = 0;

#if !defined(SERVER_MODE)
	  pgbuf_flush_check_point (thread_p, &smallest_lsa, &newsmallest_lsa);
	  if (fileio_synchronize_all (thread_p, false) != NO_ERROR)
	    {
	      LSA_SET_NULL (&newsmallest_lsa);
	    }
#else /* !SERVER_MODE */
	  /* if archiving occurs during restart recovery
	   * log_Gl.flushed_lsa_lower_bound becomes NULL_LSA,
	   * So, we need to call pgbuf_flush_check_point()
	   */
	  if (!BO_IS_SERVER_RESTARTED ())
	    {
	      pgbuf_flush_check_point (thread_p, &smallest_lsa,
				       &newsmallest_lsa);
	      if (fileio_synchronize_all (thread_p, false) != NO_ERROR)
		{
		  LSA_SET_NULL (&newsmallest_lsa);
		}
	      if (!LSA_ISNULL (&newsmallest_lsa))
		{
		  log_Gl.flushed_lsa_lower_bound.pageid =
		    newsmallest_lsa.pageid;
		  log_Gl.flushed_lsa_lower_bound.offset = NULL_OFFSET;
		}
	      else
		{
		  LSA_COPY (&log_Gl.flushed_lsa_lower_bound, &smallest_lsa);
		}
	    }
	  else
	    {
	      newsmallest_lsa.pageid = log_Gl.flushed_lsa_lower_bound.pageid;
	      newsmallest_lsa.offset = NULL_OFFSET;
	    }
#endif /* !SERVER_MODE */

	  if (!LSA_ISNULL (&newsmallest_lsa)
	      && LSA_LT (&newsmallest_lsa, &smallest_lsa))
	    {
	      if (newsmallest_lsa.pageid < log_Gl.hdr.nxarv_pageid)
		{
		  arvhdr->fpageid = log_Gl.hdr.nxarv_pageid;
		}
	      else
		{
		  arvhdr->fpageid = newsmallest_lsa.pageid;
		}
	    }

	  if (arvhdr->fpageid > log_Gl.hdr.nxarv_pageid)
	    {
	      /*
	       * Do not archive at this moment since there are some pages that can be
	       * rewritten between the next archive page and the smallest page with
	       * active log records
	       */
	      log_Gl.hdr.nxarv_pageid = arvhdr->fpageid;
	      log_Gl.hdr.nxarv_phy_pageid =
		logpb_to_physical_pageid (log_Gl.hdr.nxarv_pageid);


	      if (!LSA_ISNULL (&newsmallest_lsa)
		  && LSA_LT (&newsmallest_lsa, &smallest_lsa))
		{
		  MUTEX_LOCK (rv, log_Gl.chkpt_lsa_lock);
		  if (LSA_LT (&log_Gl.hdr.chkpt_lsa, &newsmallest_lsa))
		    {
		      LSA_COPY (&log_Gl.hdr.chkpt_lsa, &newsmallest_lsa);
		    }
		  MUTEX_UNLOCK (log_Gl.chkpt_lsa_lock);
		}
	      else
		{
		  LOG_LSA lsa;
		  lsa.pageid = arvhdr->fpageid;
		  lsa.offset = NULL_OFFSET;

		  MUTEX_LOCK (rv, log_Gl.chkpt_lsa_lock);
		  if (LSA_ISNULL (&log_Gl.hdr.chkpt_lsa)
		      || LSA_LT (&log_Gl.hdr.chkpt_lsa, &lsa))
		    {
		      LSA_COPY (&log_Gl.hdr.chkpt_lsa, &lsa);
		    }
		  MUTEX_UNLOCK (log_Gl.chkpt_lsa_lock);
		}

	      logpb_flush_header (thread_p);
	      free_and_init (malloc_arv_hdr_pgptr);
	      flush_info->flush_type = old_flush_type;

	      return;
	    }
	}

      if (last_pageid < log_Gl.hdr.nxarv_pageid)
	{
	  last_pageid = arvhdr->fpageid;
	}
      else if (last_pageid > log_Gl.append.prev_lsa.pageid - 1)
	{
	  last_pageid = log_Gl.append.prev_lsa.pageid;
	  if (log_Gl.hdr.append_lsa.pageid == log_Gl.append.prev_lsa.pageid)
	    {
	      /* Avoid archiving an incomplete page */
	      last_pageid--;
	    }
	}

      if (last_pageid < arvhdr->fpageid)
	{
	  last_pageid = arvhdr->fpageid;
	}

      arvhdr->npages = last_pageid - arvhdr->fpageid + 1;
    }

  /*
   * Now create the archive and start copying pages
   */

  mnt_log_archives (thread_p);

  fileio_make_log_archive_name (arv_name, log_Archive_path,
				log_Prefix, log_Gl.hdr.nxarv_num);

  if (PRM_LOG_BACKGROUND_ARCHIVING && bg_arv_info->vdes != NULL_VOLDES)
    {
      vdes = bg_arv_info->vdes;
    }
  else
    {
      vdes = fileio_format (thread_p, log_Db_fullname, arv_name,
			    LOG_DBLOG_ARCHIVE_VOLID, arvhdr->npages + 1,
			    false, false, false, LOG_PAGESIZE);
      if (vdes == NULL_VOLDES)
	{
	  /* Unable to create archive log to archive */
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_CREATE_LOGARCHIVE_FAIL, 3,
		  arv_name, arvhdr->fpageid,
		  arvhdr->fpageid + arvhdr->npages - 1);
	  goto error;
	}
    }

  /* Added for Goorm BMT:
   * Somehow, the previous code writes active pages to archive
   * even when media failure is not necessary. To fix this, we just
   * skip the writing portion of the code so that the code change
   * is minimized.
   */
#if defined(GOORM_BMT)
  if (PRM_LOG_MEDIA_FAILURE_SUPPORT)
    {
#endif

      /* Record number of writes in statistics */
      mnt_log_iowrites (thread_p);

      er_log_debug (ARG_FILE_LINE,
		    "logpb_archive_active_log, arvhdr->fpageid = %d\n",
		    arvhdr->fpageid);

      if (fileio_write (thread_p, vdes, malloc_arv_hdr_pgptr, 0, LOG_PAGESIZE)
	  == NULL)
	{
	  /* Error archiving header page into archive */
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_WRITE, 3,
		  0, 0, arv_name);
	  goto error;
	}

      if (PRM_LOG_BACKGROUND_ARCHIVING
	  && bg_arv_info->vdes != NULL_VOLDES
	  && arvhdr->fpageid == bg_arv_info->start_page_id)
	{
	  pageid = bg_arv_info->current_page_id;
	  ar_phy_pageid = (bg_arv_info->current_page_id
			   - bg_arv_info->start_page_id + 1);
	}
      else
	{
	  assert (!PRM_LOG_BACKGROUND_ARCHIVING
		  || bg_arv_info->vdes == NULL_VOLDES);

	  pageid = arvhdr->fpageid;
	  ar_phy_pageid = 1;
	}

      log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

      /* Now start dumping the current active pages to archive */
      for (; pageid < last_pageid;
	   pageid += num_pages, ar_phy_pageid += num_pages)
	{
	  num_pages = MIN (LOGPB_IO_NPAGES, last_pageid - pageid);
	  num_pages = logpb_read_page_from_active_log (thread_p, pageid,
						       num_pages, log_pgptr);
	  if (num_pages <= 0)
	    {
	      goto error;
	    }

	  /* Record number of writes in statistics */
	  mnt_log_iowrites (thread_p);
	  if (fileio_write_pages (thread_p, vdes, (char *) log_pgptr,
				  ar_phy_pageid, num_pages,
				  LOG_PAGESIZE) == NULL)
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_WRITE, 3,
		      pageid, ar_phy_pageid, arv_name);
	      goto error;
	    }
	}
      /* last page */
      if (logpb_fetch_page (thread_p, last_pageid, log_pgptr) == NULL)
	{
	  goto error;
	}
      if (fileio_write (thread_p, vdes, (char *) log_pgptr, ar_phy_pageid,
			LOG_PAGESIZE) == NULL)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_WRITE, 3,
		  pageid, ar_phy_pageid, arv_name);
	  goto error;
	}


      if (PRM_LOG_BACKGROUND_ARCHIVING && bg_arv_info->vdes != NULL_VOLDES)
	{
	  fileio_dismount (thread_p, vdes);
	  vdes = NULL_VOLDES;
	  bg_arv_info->vdes = NULL_VOLDES;

	  /* rename _lgar_t to _lgar[number] name */
	  if (fileio_rename (NULL_VOLID, log_Name_bg_archive, arv_name) ==
	      NULL)
	    {
	      goto error;
	    }

	  vdes = fileio_mount (thread_p, log_Db_fullname, arv_name,
			       LOG_DBLOG_ARCHIVE_VOLID, 0, false);
	  if (vdes == NULL_VOLDES)
	    {
	      goto error;
	    }
	}
      else
	{
	  /*
	   * Make sure that the whole log archive is in physical storage at this
	   * moment
	   */
	  if (fileio_synchronize (thread_p, vdes, arv_name) == NULL_VOLDES)
	    {
	      goto error;
	    }
	}

#if defined(GOORM_BMT)
    }
#endif

  /* The last archive needed for system crashes */
  if (log_Gl.hdr.last_arv_num_for_syscrashes == -1)
    {
      log_Gl.hdr.last_arv_num_for_syscrashes = log_Gl.hdr.nxarv_num;
    }

  log_Gl.hdr.nxarv_num++;
  log_Gl.hdr.nxarv_pageid = last_pageid + 1;
  log_Gl.hdr.nxarv_phy_pageid =
    logpb_to_physical_pageid (log_Gl.hdr.nxarv_pageid);

  /* Flush the log header to reflect the archive */
  logpb_flush_header (thread_p);

#if 0
  if (PRM_SUPPRESS_FSYNC != 0)
    {
      fileio_synchronize (thread_p, log_Gl.append.vdes);
    }
#endif

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_LOG_ARCHIVE_CREATED, 3,
	  arv_name, arvhdr->fpageid, last_pageid);

  /* Cast the archive information. May be used again */

  log_Gl.archive.hdr = *arvhdr;	/* Copy of structure */
  log_Gl.archive.vdes = vdes;

  catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
			   MSGCAT_SET_LOG, MSGCAT_LOG_LOGINFO_ARCHIVE);
  if (catmsg == NULL)
    {
      catmsg = "ARCHIVE: %d %s %d %d\n";
    }
  error_code = log_dump_log_info (log_Name_info, true, catmsg,
				  log_Gl.hdr.nxarv_num - 1, arv_name,
				  arvhdr->fpageid, last_pageid);
  if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
    {
      goto error;
    }

#if defined(SERVER_MODE)
  if (PRM_HA_MODE != HA_MODE_OFF)
    {
      PAGEID min_fpageid = logwr_get_min_copied_fpageid ();
      if (min_fpageid != NULL_PAGEID)
	{
	  int unneeded_arvnum;
	  if (min_fpageid >= arvhdr->fpageid)
	    {
	      unneeded_arvnum = arvhdr->arv_num - 1;
	    }
	  else
	    {
	      struct log_arv_header min_arvhdr;
	      if (logpb_fetch_from_archive (thread_p, min_fpageid,
					    NULL, NULL, &min_arvhdr) == NULL)
		{
		  goto error;
		}
	      unneeded_arvnum = min_arvhdr.arv_num - 1;
	    }
	  if (unneeded_arvnum >= 0)
	    {
	      char unneeded_logarv_name[PATH_MAX];
	      fileio_make_log_archive_name (unneeded_logarv_name,
					    log_Archive_path, log_Prefix,
					    unneeded_arvnum);

	      catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOG,
				       MSGCAT_LOG_LOGINFO_COMMENT_UNUSED_ARCHIVE_NAME);
	      if (catmsg == NULL)
		{
		  catmsg = "29 COMMENT: Log archive %s,"
		    " which contains log pages before %d,"
		    " is not needed any longer by any HA utilities.\n";
		}
	      error_code = log_dump_log_info (log_Name_info, true,
					      catmsg, unneeded_logarv_name,
					      min_fpageid);
	      if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
		{
		  goto error;
		}
	    }
	}
    }
#endif /* SERVER_MODE */


  if (PRM_LOG_BACKGROUND_ARCHIVING)
    {
      bg_arv_info->vdes = fileio_format (thread_p, log_Db_fullname,
					 log_Name_bg_archive,
					 LOG_DBLOG_BG_ARCHIVE_VOLID,
					 log_Gl.hdr.npages, false,
					 false, false, LOG_PAGESIZE);
      if (bg_arv_info->vdes != NULL_VOLDES)
	{
	  bg_arv_info->start_page_id = log_Gl.hdr.nxarv_pageid;
	  bg_arv_info->current_page_id = log_Gl.hdr.nxarv_pageid;
	}
      else
	{
	  bg_arv_info->start_page_id = NULL_PAGEID;
	  bg_arv_info->current_page_id = NULL_PAGEID;

	  er_log_debug (ARG_FILE_LINE,
			"Unable to create temporary archive log %s\n",
			log_Name_bg_archive);
	}
    }

  er_log_debug (ARG_FILE_LINE,
		"logpb_archive_active_log end, arvhdr->fpageid = %d, "
		"arvhdr->npages = %d\n", arvhdr->fpageid, arvhdr->npages);

  free_and_init (malloc_arv_hdr_pgptr);
  flush_info->flush_type = old_flush_type;

  return;

  /* ********* */
error:

  flush_info->flush_type = old_flush_type;
  if (malloc_arv_hdr_pgptr != NULL)
    {
      free_and_init (malloc_arv_hdr_pgptr);
    }

  if (vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, vdes);
      fileio_unformat (thread_p, arv_name);
    }

  if (PRM_LOG_BACKGROUND_ARCHIVING)
    {
      if (bg_arv_info->vdes != NULL_VOLDES && bg_arv_info->vdes != vdes)
	{
	  fileio_dismount (thread_p, bg_arv_info->vdes);
	}
      fileio_unformat (thread_p, log_Name_bg_archive);
      bg_arv_info->vdes = NULL_VOLDES;
    }

  logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_archive_active_log");
}

void
logpb_remove_archive_logs_exceed_limit (THREAD_ENTRY * thread_p)
{
  int first_arv_num_to_delete = -1;
  int last_arv_num_to_delete = -1;
  int lowest_arv_num_for_backup = -1;
  int highest_arv_num_for_backup = -1;
  char *catmsg;

  if (PRM_LOG_MAX_ARCHIVES == INT_MAX)
    {
      return;
    }

  LOG_CS_ENTER (thread_p);

  if ((log_Gl.hdr.nxarv_num - (log_Gl.hdr.last_deleted_arv_num + 1))
      > PRM_LOG_MAX_ARCHIVES)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_MAX_ARCHIVES_HAS_BEEN_EXCEEDED, 1, PRM_LOG_MAX_ARCHIVES);

      /* Remove the log archives at this point */
      first_arv_num_to_delete = log_Gl.hdr.last_deleted_arv_num + 1;
      last_arv_num_to_delete = log_Gl.hdr.nxarv_num - PRM_LOG_MAX_ARCHIVES;
      if (log_Gl.hdr.last_arv_num_for_syscrashes != -1)
	{
	  last_arv_num_to_delete = MIN (last_arv_num_to_delete,
					log_Gl.hdr.
					last_arv_num_for_syscrashes);
	}
      last_arv_num_to_delete--;
      if (last_arv_num_to_delete >= first_arv_num_to_delete)
	{
	  log_Gl.hdr.last_deleted_arv_num = last_arv_num_to_delete;
	  logpb_flush_header (thread_p);	/* to get rid of archives */
	}
      lowest_arv_num_for_backup = log_Gl.hdr.lowest_arv_num_for_backup;
      highest_arv_num_for_backup = log_Gl.hdr.highest_arv_num_for_backup;
    }

  LOG_CS_EXIT ();

  if (last_arv_num_to_delete >= 0
      && last_arv_num_to_delete >= first_arv_num_to_delete)
    {
      catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
			       MSGCAT_SET_LOG,
			       MSGCAT_LOG_MAX_ARCHIVES_HAS_BEEN_EXCEEDED);
      if (catmsg == NULL)
	{
	  catmsg = "Number of active log archives has been exceeded"
	    " the max desired number.";
	}
      logpb_remove_archive_logs_internal (thread_p,
					  first_arv_num_to_delete,
					  last_arv_num_to_delete, catmsg,
					  true, lowest_arv_num_for_backup,
					  highest_arv_num_for_backup);
    }

}

/*
 * logpb_remove_archive_logs - Remove all unactive log archives
 *
 * return: nothing
 *
 *   info_reason(in):
 *
 * NOTE: Archive that are not needed for system crashes are removed.
 *       That these archives may be needed for media crash recovery.
 *       Therefore, it is important that the user copy these archives
 *       to tape. Check the log information file.
 */
void
logpb_remove_archive_logs (THREAD_ENTRY * thread_p, const char *info_reason)
{
  logpb_remove_archive_to_page_id (thread_p, info_reason, NULL_PAGEID);
}

/*
 * logpb_remove_archive_to_page_id -
 *
 * return:
 *
 *   info_reason(in):
 *   safe_pageid(in):
 *
 * NOTE:
 */
static void
logpb_remove_archive_to_page_id (THREAD_ENTRY * thread_p,
				 const char *info_reason, PAGEID safe_pageid)
{
#if !defined(SERVER_MODE)
  LOG_LSA flush_upto_lsa;	/* Flush data pages up to LSA */
#endif /* !SERVER_MODE */
  LOG_LSA newflush_upto_lsa;	/* Next to be flush           */
  int first_deleted_arv_num;
  int last_deleted_arv_num;

  assert (LOG_CS_OWN (thread_p));

  /* Close any log archives that are opened */
  if (log_Gl.archive.vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, log_Gl.archive.vdes);
      log_Gl.archive.vdes = NULL_VOLDES;
    }

#if defined(SERVER_MODE)
  LSA_COPY (&newflush_upto_lsa, &log_Gl.flushed_lsa_lower_bound);
#else /* SERVER_MODE */

  flush_upto_lsa.pageid = LOGPB_NEXT_ARCHIVE_PAGE_ID;
  flush_upto_lsa.offset = NULL_OFFSET;

  pgbuf_flush_check_point (thread_p, &flush_upto_lsa, &newflush_upto_lsa);

  if ((!LSA_ISNULL (&newflush_upto_lsa)
       && LSA_LT (&newflush_upto_lsa, &flush_upto_lsa))
      || fileio_synchronize_all (thread_p, false) != NO_ERROR)
    {
      /* Cannot remove the archives at this moment */
      return;
    }

  if (log_Gl.run_nxchkpt_atpageid != NULL_PAGEID)
    {
      if (LSA_LT (&log_Gl.hdr.chkpt_lsa, &flush_upto_lsa))
	{
	  /*
	   * Reset the checkpoint record to the first possible active page and
	   * flush the log header before the archives are removed
	   */
	  LSA_COPY (&log_Gl.hdr.chkpt_lsa, &flush_upto_lsa);
	  logpb_flush_header (thread_p);
	}
    }
#endif /* SERVER_MODE */

  last_deleted_arv_num = log_Gl.hdr.last_arv_num_for_syscrashes;
  if (last_deleted_arv_num == -1)
    {
      last_deleted_arv_num = log_Gl.hdr.nxarv_num;
    }

  last_deleted_arv_num--;

  if (log_Gl.hdr.last_deleted_arv_num + 1 > last_deleted_arv_num)
    {
      /* Nothing to remove */
      return;
    }

  first_deleted_arv_num = log_Gl.hdr.last_deleted_arv_num + 1;

  if (safe_pageid != NULL_PAGEID)
    {
      last_deleted_arv_num = MIN (last_deleted_arv_num,
				  logpb_get_remove_archive_num (thread_p,
								safe_pageid,
								last_deleted_arv_num));
    }

  if (last_deleted_arv_num >= 0)
    {
      logpb_remove_archive_logs_internal (thread_p, first_deleted_arv_num,
					  last_deleted_arv_num, info_reason,
					  true,
					  log_Gl.hdr.
					  lowest_arv_num_for_backup,
					  log_Gl.hdr.
					  highest_arv_num_for_backup);

      log_Gl.hdr.last_deleted_arv_num = last_deleted_arv_num;
      logpb_flush_header (thread_p);	/* to get rid of archives */
    }
}

/*
 * logpb_get_remove_archive_num -
 *
 * return:
 *
 *   safe_pageid(in):
 *   archive_num(in):
 *
 * NOTE:
 */
static int
logpb_get_remove_archive_num (THREAD_ENTRY * thread_p, PAGEID safe_pageid,
			      int archive_num)
{
  struct log_arv_header *arvhdr;
  char arv_name[PATH_MAX];
  char arv_hdr_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT],
    *aligned_arv_hdr_pgbuf;
  LOG_PAGE *arv_hdr_pgptr;
  int vdes;

  assert (LOG_CS_OWN (NULL));

  aligned_arv_hdr_pgbuf = PTR_ALIGN (arv_hdr_pgbuf, MAX_ALIGNMENT);
  arv_hdr_pgptr = (LOG_PAGE *) aligned_arv_hdr_pgbuf;

  while (archive_num >= 0)
    {
      fileio_make_log_archive_name (arv_name, log_Archive_path,
				    log_Prefix, archive_num);
      /* open the archive file */
      if (logpb_is_archive_available (archive_num) == true
	  && fileio_is_volume_exist (arv_name) == true
	  && ((vdes = fileio_mount (thread_p, log_Db_fullname, arv_name,
				    LOG_DBLOG_ARCHIVE_VOLID, false, false))
	      != NULL_VOLDES))
	{
	  if (fileio_read (thread_p, vdes, arv_hdr_pgptr, 0, LOG_PAGESIZE)
	      == NULL)
	    {
	      fileio_dismount (thread_p, vdes);
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_READ, 3,
		      0, 0, arv_name);
	      return -1;
	    }
	  fileio_dismount (thread_p, vdes);

	  arvhdr = (struct log_arv_header *) arv_hdr_pgptr->area;
	  if (safe_pageid > arvhdr->fpageid + arvhdr->npages)
	    {
	      break;
	    }
	}

      vdes = 0;
      archive_num--;
    }

  return archive_num;
}

/*
 * log_remove_archive_logs_internal - Remove all unactive log archives
 *
 * return: nothing
 *
 *   first(in): first archive to be deleted
 *   last(in): last archive to be deleted
 *   info_reason(in): message describing the reason the archives are being deleted
 *   check_backup(in): when true, avoid deleting archives needed for a restore
 *
 * NOTE: This routine does the actual deletion and notifies the user
 *   via the lginf file.  This routine does not do any logical verification
 *   to insure that the archives are no longer needed, so the caller must be
 *   explicitly careful and insure that there are no circumstances under
 *   which those archives could possibly be needed before they are deleted.
 *   Common reasons why they might be needed include a media crash or a
 *   restore from a fuzzy backup.
 */
static void
logpb_remove_archive_logs_internal (THREAD_ENTRY * thread_p, int first,
				    int last, const char *info_reason,
				    bool check_backup,
				    int lowest_arv_num_for_backup,
				    int highest_arv_num_for_backup)
{
  char logarv_name[PATH_MAX];
  int i;
  bool deleted_one = false;	/* true if at least one was deleted */
  int last_bkup_arv_inrange;

  last_bkup_arv_inrange = MIN (highest_arv_num_for_backup, last);

  for (i = first; i <= last; i++)
    {
      if (check_backup
	  && lowest_arv_num_for_backup <= i
	  && i <= highest_arv_num_for_backup)
	{

	  /* mark the previous range (if any) deleted */
	  if (deleted_one)
	    {
	      logpb_append_archives_removed_to_log_info (first, i,
							 info_reason);
	    }

	  /* mark the captured range */
	  logpb_append_archives_delete_pend_to_log_info (i,
							 last_bkup_arv_inrange);

	  /* Continue with more deletions after the deleted range */
	  i = last_bkup_arv_inrange;
	  first = last_bkup_arv_inrange;
	  deleted_one = false;
	  continue;
	}
      fileio_make_log_archive_name (logarv_name, log_Archive_path, log_Prefix,
				    i);
      fileio_unformat (thread_p, logarv_name);
      deleted_one = true;
    }

  if (deleted_one)
    {
      logpb_append_archives_removed_to_log_info (first, last, info_reason);
    }
}

/*
 * logpb_append_archives_removed_to_log_info - Record deletion of one or more archives
 *
 * return: nothing
 *
 *   first(in): first archive to be deleted
 *   last(in): last archive to be deleted
 *   info_reason(in): message describing the reason the archives are being deleted
 *
 * NOTE: This routine makes an entry into the loginfo file that the
 *   given log archives have been removed.
 */
static void
logpb_append_archives_removed_to_log_info (int first, int last,
					   const char *info_reason)
{
  const char *catmsg;
  char logarv_name[PATH_MAX];
  char logarv_name_first[PATH_MAX];
  int error_code;

  if (info_reason != NULL)
    {
      catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
			       MSGCAT_SET_LOG,
			       MSGCAT_LOG_LOGINFO_REMOVE_REASON);
      if (catmsg == NULL)
	{
	  catmsg = "REMOVE: %d %s to \n%d %s.\nREASON: %s\n";
	}

      fileio_make_log_archive_name (logarv_name, log_Archive_path, log_Prefix,
				    last);
      if (first == last)
	{
	  error_code = log_dump_log_info (log_Name_info, true, catmsg,
					  first, logarv_name,
					  last, logarv_name, info_reason);
	}
      else
	{
	  fileio_make_log_archive_name (logarv_name_first, log_Archive_path,
					log_Prefix, first);
	  error_code = log_dump_log_info (log_Name_info, true, catmsg,
					  first, logarv_name_first,
					  last, logarv_name, info_reason);
	}
      if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
	{
	  return;
	}
    }
}

/*
 * logpb_append_archives_delete_pend_to_log_info -  Record pending delete
 *                                                  of one or more archive
 *
 * return: nothing
 *
 *   first(in): number of the first archive captured
 *   last(in): number of the last archive in the range
 *
 * NOTE: This routine makes an entry into the loginfo file that the
 *   given log archives have been "captured" by a backup and therefore cannot
 *   be deleted at this time (DELETE PENDING).
 */
static void
logpb_append_archives_delete_pend_to_log_info (int first, int last)
{
  const char *catmsg;
  char logarv_name[PATH_MAX];	/* Archive name */
  char logarv_name_first[PATH_MAX];	/* Archive name */
  int error_code;

  catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
			   MSGCAT_SET_LOG,
			   MSGCAT_LOG_LOGINFO_ARCHIVES_NEEDED_FOR_RESTORE);
  if (catmsg == NULL)
    {
      catmsg = "DELETE POSTPONED: Archives %d %s to %d %s \n"
	"are no longer needed unless a restore from current backup occurs.\n";
    }

  fileio_make_log_archive_name (logarv_name, log_Archive_path,
				log_Prefix, last);

  if (first == last)
    {
      error_code = log_dump_log_info (log_Name_info, true, catmsg, first,
				      logarv_name, last, logarv_name);
    }
  else
    {
      fileio_make_log_archive_name (logarv_name_first, log_Archive_path,
				    log_Prefix, first);
      error_code = log_dump_log_info (log_Name_info, true, catmsg, first,
				      logarv_name_first, last, logarv_name);
    }
  if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
    {
      return;
    }
}

/*
 *
 *       	   FUNCTIONS RELATED TO MISCELANEOUS I/O
 *
 */

/*
 * logpb_copy_from_log: Copy a portion of the log
 *
 * arguments:
 *         area: Area where the portion of the log is copied.
 *               (Set as a side effect)
 *  area_length: the length to copy
 *   log_lsa: log address of the log data to copy
 *               (May be set as a side effect)
 *    log_pgptr: the buffer containing the log page
 *               (May be set as a side effect)
 *
 * returns/side-effects: nothing
 *    area is set as a side effect.
 *    log_lsa, and log_pgptr are set as a side effect.
 *
 * description: Copy "length" bytes of the log starting at log_lsa->pageid,
 *              log_offset onto the given area.
 * NOTE:        The location of the log is updated to point to the end of the
 *              data.
 */
void
logpb_copy_from_log (THREAD_ENTRY * thread_p, char *area, int length,
		     LOG_LSA * log_lsa, LOG_PAGE * log_page_p)
{
  int copy_length;		/* Length to copy into area */
  int area_offset;		/* The area offset          */

  /*
   * If the data is contained in only one buffer, copy the data in one
   * statement, otherwise, copy it in parts
   */

  if (log_lsa->offset + length < (int) LOGAREA_SIZE)
    {
      /* The log data is contiguos */
      memcpy (area, (char *) log_page_p->area + log_lsa->offset, length);
      log_lsa->offset += length;
    }
  else
    {
      /* The log data is not contiguos */
      area_offset = 0;
      while (length > 0)
	{
	  LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, 0, log_lsa, log_page_p);
	  if (log_lsa->offset + length < (int) LOGAREA_SIZE)
	    {
	      copy_length = length;
	    }
	  else
	    {
	      copy_length = LOGAREA_SIZE - log_lsa->offset;
	    }
	  memcpy (area + area_offset,
		  (char *) log_page_p->area + log_lsa->offset, copy_length);
	  length -= copy_length;
	  area_offset += copy_length;
	  log_lsa->offset += copy_length;
	}
    }
}

/*
 * logpb_verify_length - Verify db and log lengths
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   db_fullname(in): Full name of the database
 *   log_path(in): Directory where the log volumes reside
 *   log_prefix(in): Name of the log volumes. It is usually set as database
 *                   name. For example, if the value is equal to "db", the
 *                   names of the log volumes created are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *
 * NOTE: Make sure that any database name will not be overpassed.
 */
static int
logpb_verify_length (const char *db_fullname, const char *log_path,
		     const char *log_prefix)
{
  int volmax_suffix;
  int length;
  const char *dbname;
  long int filename_max;
  long int pathname_max;

  volmax_suffix = fileio_get_volume_max_suffix ();

  dbname = fileio_get_base_file_name (db_fullname);
  if (fileio_get_max_name (db_fullname, &filename_max, &pathname_max) < 0)
    {
      return ER_FAILED;
    }

  if (pathname_max > DB_MAX_PATH_LENGTH)
    {
      pathname_max = DB_MAX_PATH_LENGTH;
    }

  /*
   * Make sure that names of volumes (information and log volumes), that is,
   * OS files will not exceed the maximum allowed value.
   */

  if ((int) (strlen (dbname) + 1 + volmax_suffix) > filename_max)
    {
      /* The name of the volume is too long */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_NOFULL_DATABASE_NAME_IS_TOO_LONG, 2,
	      dbname, filename_max - volmax_suffix - 1);
      return ER_LOG_NOFULL_DATABASE_NAME_IS_TOO_LONG;
    }

  if ((int) (strlen (log_prefix) + 1) > filename_max
      || (int) (strlen (log_prefix) + 1) > MAXLOGNAME)
    {
      /* Bad prefix log name */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_PREFIX_NAME_IS_TOO_LONG, 2,
	      log_prefix, ((MAXLOGNAME > filename_max)
			   ? filename_max - 1 : MAXLOGNAME - 1));
      return ER_LOG_PREFIX_NAME_IS_TOO_LONG;
    }

  /*
   * Make sure that the length for the volume is OK
   */

  if ((int) (strlen (db_fullname) + 1 + volmax_suffix) > pathname_max)
    {
      /* The full name of the database is too long */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_BO_FULL_DATABASE_NAME_IS_TOO_LONG, 3, NULL, db_fullname,
	      strlen (db_fullname) + 1, pathname_max);
      return ER_BO_FULL_DATABASE_NAME_IS_TOO_LONG;
    }

  /*
   * First create a new log for the new database. This log is not a copy of
   * of the old log; it is a newly created one.
   */

  if (log_path != NULL)
    {
      length = strlen (log_path) + strlen (log_prefix) + 2;
    }
  else
    {
      length = strlen (log_prefix) + 1;
    }

  if (length + volmax_suffix > pathname_max)
    {
      /*
       * Database name is too long.
       * Path + prefix < pathname_max - 2
       */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_NAME_IS_TOO_LONG,
	      3, log_path, log_prefix, pathname_max - 2);
      return ER_LOG_NAME_IS_TOO_LONG;
    }

  return NO_ERROR;
}

/*
 * logpb_initialize_log_names - Initialize the names of log volumes and files
 *
 * return: nothing
 *
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in): Name of the log volumes. It is usually set as database
 *                      name. For example, if the value is equal to "db", the
 *                      names of the log volumes created are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *
 * NOTE:Initialize name of log volumes and files
 */
int
logpb_initialize_log_names (THREAD_ENTRY * thread_p, const char *db_fullname,
			    const char *logpath, const char *prefix_logname)
{
  int error_code = NO_ERROR;

  error_code = logpb_verify_length (db_fullname, logpath, prefix_logname);
  if (error_code != NO_ERROR)
    {
      /* Names are too long */
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "logpb_initialize_log_names");

      return error_code;
    }

  /* Save the log Path */

  if (logpath != NULL)
    {
      strcpy (log_Path, logpath);
    }
  else
    {
      strcpy (log_Path, ".");
    }

  strcpy (log_Archive_path, log_Path);

  /* Save the log Prefix */
  strcpy (log_Prefix, prefix_logname);

  /*
   * Build Name of log active
   */
  fileio_make_log_active_name (log_Name_active, log_Path, log_Prefix);
  fileio_make_log_info_name (log_Name_info, log_Path, log_Prefix);
  fileio_make_backup_volume_info_name (log_Name_bkupinfo, log_Path,
				       log_Prefix);
  fileio_make_volume_info_name (log_Name_volinfo, db_fullname);
  fileio_make_log_archive_temp_name (log_Name_bg_archive, log_Archive_path,
				     log_Prefix);
  log_Db_fullname = db_fullname;

  return error_code;
}

/*
 * logpb_exist_log - Find if given log exists
 *
 * return:
 *
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in): Name of the log volumes. It is usually set as database
 *                   name. For example, if the value is equal to "db", the
 *                   names of the log volumes created are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *
 * NOTE:Find if the log associated with the aboive database exists.
 */
bool
logpb_exist_log (THREAD_ENTRY * thread_p, const char *db_fullname,
		 const char *logpath, const char *prefix_logname)
{
  /* Is the system restarted ? */
  if (!logpb_is_initialize_pool ())
    {
      if (logpb_initialize_log_names (thread_p, db_fullname, logpath,
				      prefix_logname) != NO_ERROR)
	{
	  return false;
	}
    }

  return fileio_is_volume_exist (log_Name_active);
}

#if defined(SERVER_MODE)
/*
 * logpb_do_checkpoint -
 *
 * return:
 *
 * NOTE:
 */
void
logpb_do_checkpoint (void)
{
  thread_wakeup_checkpoint_thread ();
}
#endif /* SERVER_MODE */

/*
 * logpb_checkpoint - Execute a fuzzy checkpoint
 *
 * return: pageid where a redo will start
 *
 */
PAGEID
logpb_checkpoint (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;		/* System transaction descriptor */
  LOG_TDES *act_tdes;		/* Transaction descriptor of an
				 * active transaction
				 */
  struct log_chkpt *chkpt, tmp_chkpt;	/* Checkpoint log records       */
  struct log_chkpt_trans *chkpt_trans;	/* Checkpoint tdes              */
  struct log_chkpt_trans *chkpt_one;	/* Checkpoint tdes for one tran */
  struct log_chkpt_topops_commit_posp *chkpt_topops;	/* Checkpoint top system
							 * operations that are in
							 * commit postpone mode
							 */
  struct log_chkpt_topops_commit_posp *chkpt_topone;	/* One top system ope   */
  LOG_LSA flush_upto_lsa;	/* Flush data pages up to LSA   */
  LOG_LSA chkpt_lsa;		/* copy of log_Gl.hdr.chkpt_lsa */
  LOG_LSA newchkpt_lsa;		/* New address of the checkpoint
				 * record
				 */
  unsigned int nobj_locks;	/* Avg number of locks          */
  char logarv_name[PATH_MAX];	/* Archive name       */
  char logarv_name_first[PATH_MAX];	/* Archive name */
  int ntrans;			/* Number of trans              */
  int ntops;			/* Number of total active top
				 * actions
				 */
  int length_all_chkpt_trans;
  int length_all_tops = 0;
  int i, j;
  const char *catmsg;
  int num_perm_vols;
  VOLID volid;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
  int error_code = NO_ERROR;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  LOG_CS_ENTER (thread_p);
  flush_info->flush_type = LOG_FLUSH_DIRECT;

#if defined(SERVER_MODE)
  if (BO_IS_SERVER_RESTARTED () && log_Gl.run_nxchkpt_atpageid == NULL_PAGEID)
    {
      flush_info->flush_type = LOG_FLUSH_NORMAL;
      LOG_CS_EXIT ();
      return NULL_PAGEID;
    }

  /*
   * Critical section is entered several times to allow other transaction to
   * use the log manger
   */
#endif /* SERVER_MODE */

  mnt_log_checkpoints (thread_p);

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_LOG_CHECKPOINT_STARTED,
	  1, log_Gl.hdr.chkpt_lsa.pageid);
  er_log_debug (ARG_FILE_LINE, "start checkpoint\n");

  /*
   * Indicate that the checkpoint process is running. Don't run another one,
   * until we are done with the present one.
   */

  log_Gl.run_nxchkpt_atpageid = NULL_PAGEID;

  tdes = LOG_FIND_TDES (LOG_SYSTEM_TRAN_INDEX);
  if (tdes == NULL)
    {
      flush_info->flush_type = LOG_FLUSH_NORMAL;
      LOG_CS_EXIT ();
      return NULL_PAGEID;
    }

  /*
   * FLUSH all append LOG PAGES and flush all DIRTY DATA PAGES whose LSA
   * are SMALLER OR EQUAL than flush_upto_lsa value and find the next redo
   * point.
   */

  MUTEX_LOCK (rv, log_Gl.chkpt_lsa_lock);
  LSA_COPY (&chkpt_lsa, &log_Gl.hdr.chkpt_lsa);
  MUTEX_UNLOCK (log_Gl.chkpt_lsa_lock);

  if (!logpb_is_page_in_archive (chkpt_lsa.pageid))
    {
      LSA_COPY (&flush_upto_lsa, &chkpt_lsa);
    }
  else
    {
      flush_upto_lsa.pageid = LOGPB_NEXT_ARCHIVE_PAGE_ID;

      if (flush_upto_lsa.pageid > log_Gl.hdr.append_lsa.pageid)
	{
	  /* Likely we archive an incomplete page */
	  flush_upto_lsa.pageid = log_Gl.hdr.append_lsa.pageid;
	}

      if (flush_upto_lsa.pageid == log_Gl.hdr.append_lsa.pageid)
	{
	  flush_upto_lsa.offset = log_Gl.hdr.append_lsa.offset - 1;
	}
      else
	{
	  flush_upto_lsa.offset = LOGAREA_SIZE;
	}
    }

  er_log_debug (ARG_FILE_LINE, "logpb_checkpoint: call logpb_force()\n");
  logpb_force (thread_p);

  /* MARK THE CHECKPOINT PROCESS */
  logpb_start_append (thread_p, LOG_START_CHKPT, tdes);
  logpb_end_append (thread_p);

  /*
   * Modify log header to record present checkpoint. The header is flushed
   * later
   */

  LSA_COPY (&newchkpt_lsa, &tdes->tail_lsa);

  flush_info->flush_type = LOG_FLUSH_NORMAL;
  LOG_CS_EXIT ();

  er_log_debug (ARG_FILE_LINE,
		"logpb_checkpoint: call pbbuf_flush_check_point()\n");
  pgbuf_flush_victim_candidate (thread_p, PGBUF_VICTIM_FLUSH_MAX_RATIO);
  pgbuf_flush_check_point (thread_p, &flush_upto_lsa, &tmp_chkpt.redo_lsa);

  er_log_debug (ARG_FILE_LINE,
		"logpb_checkpoint: call fileio_synchronize_all()\n");
  if (fileio_synchronize_all (thread_p, false) != NO_ERROR)
    {
      LOG_CS_ENTER (thread_p);
      goto error_cannot_chkpt;
    }

  LOG_CS_ENTER (thread_p);
  flush_info->flush_type = LOG_FLUSH_DIRECT;

  if (LSA_ISNULL (&tmp_chkpt.redo_lsa))
    {
      LSA_COPY (&tmp_chkpt.redo_lsa, &newchkpt_lsa);
    }

#if defined(SERVER_MODE)
  /* Save lower bound of flushed lsa */
  if (!LSA_ISNULL (&tmp_chkpt.redo_lsa))
    {
      if (LSA_ISNULL (&log_Gl.flushed_lsa_lower_bound)
	  || LSA_GT (&tmp_chkpt.redo_lsa, &log_Gl.flushed_lsa_lower_bound))
	LSA_COPY (&log_Gl.flushed_lsa_lower_bound, &tmp_chkpt.redo_lsa);
    }
#endif /* SERVER_MODE */

  TR_TABLE_CS_ENTER (thread_p);

  /* allocate memory space for the transaction descriptors */
  tmp_chkpt.ntrans = log_Gl.trantable.num_assigned_indices;
  length_all_chkpt_trans = sizeof (*chkpt_trans) * tmp_chkpt.ntrans;

  chkpt_trans = (struct log_chkpt_trans *) malloc (length_all_chkpt_trans);
  if (chkpt_trans == NULL)
    {
      TR_TABLE_CS_EXIT ();
      flush_info->flush_type = LOG_FLUSH_NORMAL;
      goto error_cannot_chkpt;
    }

  /* CHECKPOINT THE TRANSACTION TABLE */

  for (i = 0, ntrans = 0, ntops = 0;
       i < log_Gl.trantable.num_total_indices; i++)
    {
      /*
       * Don't checkpoint current system transaction. That is, the one of
       * checkpoint process
       */
      if (i == LOG_SYSTEM_TRAN_INDEX)
	{
	  continue;
	}
      act_tdes = LOG_FIND_TDES (i);
      if (act_tdes != NULL && act_tdes->trid != NULL_TRANID
	  && !LSA_ISNULL (&act_tdes->tail_lsa))
	{
	  chkpt_one = &chkpt_trans[ntrans];
	  chkpt_one->isloose_end = act_tdes->isloose_end;
	  chkpt_one->trid = act_tdes->trid;
	  chkpt_one->state = act_tdes->state;
	  LSA_COPY (&chkpt_one->head_lsa, &act_tdes->head_lsa);
	  LSA_COPY (&chkpt_one->tail_lsa, &act_tdes->tail_lsa);
	  if (chkpt_one->state == TRAN_UNACTIVE_ABORTED)
	    {
	      /*
	       * Transaction is in the middle of an abort, since rollback does
	       * is not run in a critical section. Set the undo point to be the
	       * same as its tail. The recovery process will read the last
	       * record which is likely a compensating one, and find where to
	       * continue a rollback operation.
	       */
	      LSA_COPY (&chkpt_one->undo_nxlsa, &act_tdes->tail_lsa);
	    }
	  else
	    {
	      LSA_COPY (&chkpt_one->undo_nxlsa, &act_tdes->undo_nxlsa);
	    }

	  LSA_COPY (&chkpt_one->posp_nxlsa, &act_tdes->posp_nxlsa);
	  LSA_COPY (&chkpt_one->savept_lsa, &act_tdes->savept_lsa);
	  LSA_COPY (&chkpt_one->tail_topresult_lsa,
		    &act_tdes->tail_topresult_lsa);
	  LSA_COPY (&chkpt_one->client_undo_lsa, &act_tdes->client_undo_lsa);
	  LSA_COPY (&chkpt_one->client_posp_lsa, &act_tdes->client_posp_lsa);
	  *(chkpt_one->user_name) = *(act_tdes->client.db_user);
	  ntrans++;
	  if (act_tdes->topops.last >= 0
	      && ((act_tdes->state ==
		   TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE)
		  || (act_tdes->state ==
		      TRAN_UNACTIVE_XTOPOPE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS)
		  || (act_tdes->state ==
		      TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS)))
	    {
	      ntops += act_tdes->topops.last + 1;
	    }
	}
    }

  /*
   * Reset the structure to the correct number of transactions and
   * recalculate the length
   */
  tmp_chkpt.ntrans = ntrans;
  length_all_chkpt_trans = sizeof (*chkpt_trans) * tmp_chkpt.ntrans;

  /*
   * Scan again if there were any top system operations in the process of
   * being committed.
   * NOTE that we checkpoint top system operations only when there are in the
   * process of commit. Not knownledge of top system operations that are not
   * in the process of commit is required since if there is a crash, the system
   * operation is aborted as part of the transaction.
   */

  if (ntops > 0)
    {
      tmp_chkpt.ntops = ntops;
      length_all_tops = sizeof (*chkpt_topops) * ntops;
      chkpt_topops =
	(struct log_chkpt_topops_commit_posp *) malloc (length_all_tops);
      if (chkpt_topops == NULL)
	{
	  free_and_init (chkpt_trans);
	  TR_TABLE_CS_EXIT ();
	  flush_info->flush_type = LOG_FLUSH_NORMAL;
	  goto error_cannot_chkpt;
	}

      /* CHECKPOINTING THE TOP ACTIONS */
      for (i = 0, ntrans = 0, ntops = 0;
	   i < log_Gl.trantable.num_total_indices; i++)
	{
	  /*
	   * Don't checkpoint current system transaction. That is, the one of
	   * checkpoint process
	   */
	  if (i == LOG_SYSTEM_TRAN_INDEX)
	    {
	      continue;
	    }
	  act_tdes = LOG_FIND_TDES (i);
	  if (act_tdes != NULL && act_tdes->trid != NULL_TRANID)
	    {
	      for (j = 0; j < act_tdes->topops.last + 1; j++)
		{
		  switch (act_tdes->state)
		    {
		    case TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE:
		    case TRAN_UNACTIVE_XTOPOPE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS:
		    case TRAN_UNACTIVE_TOPOPE_ABORTED_WITH_CLIENT_USER_LOOSE_ENDS:
		      chkpt_topone = &chkpt_topops[ntops];
		      chkpt_topone->trid = act_tdes->trid;
		      LSA_COPY (&chkpt_topone->lastparent_lsa,
				&act_tdes->topops.stack[j].lastparent_lsa);
		      LSA_COPY (&chkpt_topone->posp_lsa,
				&act_tdes->topops.stack[j].posp_lsa);
		      LSA_COPY (&chkpt_topone->client_posp_lsa,
				&act_tdes->topops.stack[j].client_posp_lsa);
		      LSA_COPY (&chkpt_topone->client_undo_lsa,
				&act_tdes->topops.stack[j].client_undo_lsa);
		      ntops++;
		      break;
		    default:
		      continue;
		    }
		}
	    }
	}
    }
  else
    {
      tmp_chkpt.ntops = 0;
      chkpt_topops = NULL;
    }

  TR_TABLE_CS_EXIT ();

  /* NOW DO THE APPEND */
  logpb_start_append (thread_p, LOG_END_CHKPT, tdes);

  /* ADD the data header */
  LOG_APPEND_ADVANCE_WHEN_DOESNOT_FIT (thread_p, sizeof (*chkpt));

  chkpt = (struct log_chkpt *) LOG_APPEND_PTR ();
  *chkpt = tmp_chkpt;

  LOG_APPEND_SETDIRTY_ADD_ALIGN (thread_p, sizeof (*chkpt));

  /* INSERT data */
  logpb_append_data (thread_p, length_all_chkpt_trans,
		     (const char *) chkpt_trans);
  free_and_init (chkpt_trans);

  /* Any topops to log ? */
  if (chkpt_topops != NULL)
    {
      logpb_append_data (thread_p, length_all_tops,
			 (const char *) chkpt_topops);
      free_and_init (chkpt_topops);
    }

  /*
   * END append
   * Flush the page since we are going to flush the log header which
   * reflects the new location of the last checkpoint log record
   */

  logpb_end_append (thread_p);
  logpb_flush_all_append_pages (thread_p, LOG_FLUSH_DIRECT);
  er_log_debug (ARG_FILE_LINE,
		"logpb_checkpoint: call logpb_flush_all_append_pages()\n");
  assert (LOG_CS_OWN (thread_p));

  /*
   * Flush the log data header and update all checkpoints in volumes to
   * point to new checkpoint
   */

  /* Average the number of active transactions and locks */
  nobj_locks = lock_get_number_object_locks ();
  log_Gl.hdr.avg_ntrans = (log_Gl.hdr.avg_ntrans + ntrans) >> 1;
  log_Gl.hdr.avg_nlocks = (log_Gl.hdr.avg_nlocks + nobj_locks) >> 1;


  /* Flush the header */
  MUTEX_LOCK (rv, log_Gl.chkpt_lsa_lock);
  if (LSA_LT (&log_Gl.hdr.chkpt_lsa, &newchkpt_lsa))
    {
      LSA_COPY (&log_Gl.hdr.chkpt_lsa, &newchkpt_lsa);
    }
  LSA_COPY (&chkpt_lsa, &log_Gl.hdr.chkpt_lsa);
  MUTEX_UNLOCK (log_Gl.chkpt_lsa_lock);

  er_log_debug (ARG_FILE_LINE,
		"logpb_checkpoint: call logpb_flush_header()\n");
  logpb_flush_header (thread_p);

  /*
   * Exit from the log critical section since we are going to call another
   * module which may be blocked on a lock.
   */

  flush_info->flush_type = LOG_FLUSH_NORMAL;
  LOG_CS_EXIT ();

  /*
   * Record the checkpoint address on every volume header
   */

  num_perm_vols = xboot_find_number_permanent_volumes (thread_p);

  for (volid = LOG_DBFIRST_VOLID; volid < num_perm_vols; volid++)
    {
      (void) disk_set_checkpoint (thread_p, volid, &chkpt_lsa);
    }

  /*
   * Get the critical section again, so we can check if any archive can be
   * declare as un-needed
   */

  LOG_CS_ENTER (thread_p);

  /*
   * If the log archives are not needed for any normal undos and redos,
   * indicate so. However, the log archives may be needed during media
   * crash recovery.
   */
#if defined(SERVER_MODE)
  if (log_Gl.hdr.last_arv_num_for_syscrashes != -1
      && !logpb_is_smallest_lsa_in_archive (thread_p)
      /* following routine may not be required */
      && !logpb_is_page_in_archive (log_Gl.flushed_lsa_lower_bound.pageid)
      && !logpb_is_page_in_archive (tmp_chkpt.redo_lsa.pageid))
#else /* SERVER_MODE */
  if (log_Gl.hdr.last_arv_num_for_syscrashes != -1
      && !logpb_is_smallest_lsa_in_archive (thread_p)
      && !logpb_is_page_in_archive (tmp_chkpt.redo_lsa.pageid))
#endif /* SERVER_MODE */
    {
      /* Close any log archives that are opened */
      if (log_Gl.archive.vdes != NULL_VOLDES)
	{
	  fileio_dismount (thread_p, log_Gl.archive.vdes);
	  log_Gl.archive.vdes = NULL_VOLDES;
	}

      /* This is OK since we have already flushed the log header page */

      if (PRM_LOG_MEDIA_FAILURE_SUPPORT == false)
	{
	  /* Remove the log archives at this point */
	  log_Gl.hdr.last_arv_num_for_syscrashes = -1;
	  catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
				   MSGCAT_SET_LOG,
				   MSGCAT_LOG_MEDIACRASH_NOT_IMPORTANT);
	  if (catmsg)
	    {
	      logpb_remove_archive_logs (thread_p, catmsg);
	    }
	}
      else
	{
	  if (log_Gl.hdr.last_arv_num_for_syscrashes ==
	      log_Gl.hdr.nxarv_num - 1)
	    {
	      fileio_make_log_archive_name (logarv_name, log_Archive_path,
					    log_Prefix,
					    log_Gl.hdr.nxarv_num - 1);
	      catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOG,
				       MSGCAT_LOG_LOGINFO_COMMENT_ARCHIVE_NONEEDED);
	      if (catmsg == NULL)
		{
		  catmsg = "COMMENT: Log archive %s is not needed any longer"
		    " unless a database media crash occurs.\n";
		}
	      error_code = log_dump_log_info (log_Name_info, true,
					      catmsg, logarv_name);
	    }
	  else
	    {
	      fileio_make_log_archive_name (logarv_name_first,
					    log_Archive_path, log_Prefix,
					    log_Gl.hdr.
					    last_arv_num_for_syscrashes);
	      fileio_make_log_archive_name (logarv_name, log_Archive_path,
					    log_Prefix,
					    log_Gl.hdr.nxarv_num - 1);

	      catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOG,
				       MSGCAT_LOG_LOGINFO_COMMENT_MANY_ARCHIVES_NONEEDED);
	      if (catmsg == NULL)
		{
		  catmsg = "COMMENT: Log archives from %s to %s are not"
		    " needed any longer unless a database media crash occurs.\n";
		}
	      error_code =
		log_dump_log_info (log_Name_info, true, catmsg,
				   logarv_name_first, logarv_name);
	    }
	  if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
	    {
	      goto error_cannot_chkpt;
	    }
	  log_Gl.hdr.last_arv_num_for_syscrashes = -1;
	}
      logpb_flush_header (thread_p);	/* Yes, one more time, to get rid of archives */
    }

  /*
   * Checkpoint process is not running any longer. Indicate when do we expect
   * it to run.
   */

  log_Gl.run_nxchkpt_atpageid = (log_Gl.hdr.append_lsa.pageid +
				 log_Gl.chkpt_every_npages);
  /*
   * Clear all tail and heads information of current system transaction
   */
  logtb_clear_tdes (thread_p, tdes);

  LOG_CS_EXIT ();

#if 0
  /* have to sync log vol, data vol */
  fileio_synchronize_all (thread_p, true /* include_log */ );
#endif

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_LOG_CHECKPOINT_FINISHED,
	  1, log_Gl.hdr.chkpt_lsa.pageid);
  er_log_debug (ARG_FILE_LINE, "end checkpoint\n");
  return tmp_chkpt.redo_lsa.pageid;

  /* ******** */
error_cannot_chkpt:
  assert (LOG_CS_OWN (thread_p));
  log_Gl.run_nxchkpt_atpageid = (log_Gl.hdr.append_lsa.pageid +
				 log_Gl.chkpt_every_npages);
  LOG_CS_EXIT ();
  return NULL_PAGEID;
}

/*
 * logpb_dump_checkpoint_trans - Dump checkpoint transactions
 *
 * return: nothing
 *
 *   length(in): Length to dump in bytes
 *   data(in): The data being logged
 *
 * NOTE: Dump a checkpoint transactions structure.
 */
void
logpb_dump_checkpoint_trans (FILE * out_fp, int length, void *data)
{
  int ntrans, i;
  struct log_chkpt_trans *chkpt_trans, *chkpt_one;	/* Checkpoint tdes */

  chkpt_trans = (struct log_chkpt_trans *) data;
  ntrans = length / sizeof (*chkpt_trans);

  /* Start dumping each checkpoint transaction descriptor */

  for (i = 0; i < ntrans; i++)
    {
      chkpt_one = &chkpt_trans[i];
      fprintf (out_fp,
	       "     Trid = %d, State = %s, isloose_end = %d,\n"
	       "        Head_lsa = %d|%d, Tail_lsa = %d|%d, UndoNxtLSA = %d|%d,\n"
	       "        Postpone_lsa = %d|%d, Save_lsa = %d|%d,"
	       " Tail_topresult_lsa = %d|%d,\n"
	       "        Client_User: (name = %s, undo_lsa = %d|%d,"
	       " posp_nxlsa = %d|%d)\n",
	       chkpt_one->trid, log_state_string (chkpt_one->state),
	       chkpt_one->isloose_end,
	       chkpt_one->head_lsa.pageid, chkpt_one->head_lsa.offset,
	       chkpt_one->tail_lsa.pageid, chkpt_one->tail_lsa.offset,
	       chkpt_one->undo_nxlsa.pageid, chkpt_one->undo_nxlsa.offset,
	       chkpt_one->posp_nxlsa.pageid, chkpt_one->posp_nxlsa.offset,
	       chkpt_one->savept_lsa.pageid, chkpt_one->savept_lsa.offset,
	       chkpt_one->tail_topresult_lsa.pageid,
	       chkpt_one->tail_topresult_lsa.offset,
	       chkpt_one->user_name,
	       chkpt_one->client_undo_lsa.pageid,
	       chkpt_one->client_undo_lsa.offset,
	       chkpt_one->client_posp_lsa.pageid,
	       chkpt_one->client_posp_lsa.offset);
    }
  (void) fprintf (out_fp, "\n");
}

/*
 * logpb_dump_checkpoint_topops - DUMP CHECKPOINT OF TOP SYSTEM OPERATIONS
 *
 * return: nothing
 *
 *   length(in): Length to dump in bytes
 *   data(in): The data being logged
 *
 * NOTE: Dump the checkpoint top system operation structure.
 */
void
logpb_dump_checkpoint_topops (FILE * out_fp, int length, void *data)
{
  int ntops, i;
  struct log_chkpt_topops_commit_posp *chkpt_topops;	/* Checkpoint top system
							 * operations that are in
							 * commit postpone mode
							 */
  struct log_chkpt_topops_commit_posp *chkpt_topone;	/* One top system ope   */

  chkpt_topops = (struct log_chkpt_topops_commit_posp *) data;
  ntops = length / sizeof (*chkpt_topops);

  /* Start dumping each checkpoint top system operation */

  for (i = 0; i < ntops; i++)
    {
      chkpt_topone = &chkpt_topops[i];
      fprintf (out_fp,
	       "     Trid = %d, Lastparent_lsa = %d|%d, Postpone_lsa = %d|%d,"
	       "     Client_Postpone_lsa = %d|%d, Client_undo_lsa = %d|%d\n",
	       chkpt_topone->trid, chkpt_topone->lastparent_lsa.pageid,
	       chkpt_topone->lastparent_lsa.offset,
	       chkpt_topone->posp_lsa.pageid, chkpt_topone->posp_lsa.offset,
	       chkpt_topone->client_posp_lsa.pageid,
	       chkpt_topone->client_posp_lsa.offset,
	       chkpt_topone->client_undo_lsa.pageid,
	       chkpt_topone->client_undo_lsa.offset);
    }
  (void) fprintf (out_fp, "\n");
}

/*
 * logpb_backup_for_volume - Execute a full backup for the given volume
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   volid(in): The identifier of the volume to backup
 *   chkpt_lsa(in): Checkpoint of the backup process
 *   session(in/out): The session array which is set as a side effect.
 *   only_updated(in):
 *
 * NOTE: The volume with the given volume identifier is backed up on
 *              the given fullname_backup. The backup that is taken is a fuzzy
 *              snapshot of the volume since updates to the volume may be in
 *              progress. Thus, the backup may contain some uncommitted data.
 *              Even worse, the backup is performed using the disk directly.
 *              That is, we copy the pages of the disk without going through
 *              the page buffer pool to avoid disrupting the locality of the
 *              page buffers (e.g., a query executed by another transaction)
 *              For temp volumes, incremental backups are not allowed (because
 *              of logging issues) so always save the system pages of temp
 *              volumes.
 */
static int
logpb_backup_for_volume (THREAD_ENTRY * thread_p, VOLID volid,
			 LOG_LSA * chkpt_lsa, FILEIO_BACKUP_SESSION * session,
			 bool only_updated)
{
  DISK_VOLPURPOSE volpurpose;
  PAGEID vol_sys_lastpage;
  int error_code = NO_ERROR;

  /*
   * Determine the purpose of the volume.  For most volumes we need to
   * backup every page, but for temporary volumes we only need the system
   * pages.
   */
  if (xdisk_get_purpose_and_sys_lastpage (thread_p, volid, &volpurpose,
					  &vol_sys_lastpage) == NULL_VOLID)
    {
      error_code = ER_FAILED;
      return error_code;
    }
  else if (volpurpose == DISK_PERMVOL_TEMP_PURPOSE
	   || volpurpose == DISK_TEMPVOL_TEMP_PURPOSE)
    {
      /*
       * Do not allow incremental backups of temp volumes; but since we
       * only backup the system pages, they be only a handfull of pages.
       */
      only_updated = false;
    }
  else
    {
      vol_sys_lastpage = -1;	/* must backup entire volume */
    }

  /*
   * Reset the checkpoint of the volume to backup and flush all its dirty
   * pages, so that the the backup reflects the actual state of the volume
   * as much as possible
   */
  error_code = disk_set_checkpoint (thread_p, volid, chkpt_lsa);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  LOG_CS_ENTER (thread_p);
  logpb_force (thread_p);
  LOG_CS_EXIT ();

  error_code = pgbuf_flush_all_unfixed (thread_p, volid);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /*
   * Create the backup file/volume and copy the content onto it.
   *
   * Note that the copy is done from disk to disk. The page buffer pool is
   * not used. Thus, some more recent version of some copied pages may be
   * present in the buffer pool. We copy the database without using the page
   * buffer pool to avoid disrupting the locality of pages in the page
   * buffer pool and avoid the overhead of calling the page buffer manager.
   */

  error_code =
    fileio_backup_volume (thread_p, session, fileio_get_volume_label (volid),
			  volid, vol_sys_lastpage, only_updated);

  return error_code;
}

/*
 * logpb_backup - Execute a level backup for the given database volume
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   num_perm_vols(in): Number of permanent volumes
 *   allbackup_path(in): Location where information volumes are
 *                                 backed up. If NULL is given, the following
 *                                 defaults are assumed to back up each
 *                                 information volume:
 *                                 - If file "fileof_vols_and_backup_paths" is
 *                                   given, the path to backup each volume is
 *                                   found in this file.
 *                                 - All information volumes are backed up on
 *                                   the same location where the log files are
 *                                   located.
 *   backup_level(in): backup levels allowed: 0 - Full (default),
 *                                 1 - Incremental1, 2 - Incremental
 *   delete_unneeded_logarchives(in): Whetear to remove log archives that are
 *                                 not needed any longer to recovery from
 *                                 crashes when the backup just created is
 *                                 used.
 *   backup_verbose_file(in): verbose mode file path
 *   num_threads(in): number of threads
 *   zip_method(in): compression method
 *   zip_level(in): compression level
 *   skip_activelog(in):
 *   safe_pageid(in):
 *
 */
int
logpb_backup (THREAD_ENTRY * thread_p, int num_perm_vols,
	      const char *allbackup_path, FILEIO_BACKUP_LEVEL backup_level,
	      bool delete_unneeded_logarchives,
	      const char *backup_verbose_file, int num_threads,
	      FILEIO_ZIP_METHOD zip_method, FILEIO_ZIP_LEVEL zip_level,
	      int skip_activelog, PAGEID safe_pageid)
{
  FILEIO_BACKUP_SESSION session;
  const char *from_vlabel;	/* Name of volume to backup (FROM) */
  char vol_backup[PATH_MAX];	/* Name of the backup volume (TO) */
#if 0
  /* Not used */
  char real_pathbuf[PATH_MAX];	/* Real path                      */
#endif
  VOLID volid;			/* Current volume to backup       */
  LOG_LSA bkup_start_lsa;	/* Start point of backup          */
  LOG_LSA chkpt_lsa;		/* Checkpoint address where the
				 * backup process starts
				 */
#if defined(SERVER_MODE)
  PAGEID saved_run_nxchkpt_atpageid;
#endif /* SERVER_MODE */
  FILE *backup_volinfo_fp = NULL;	/* Pointer to backup
					 * information/directory file
					 */
  const char *catmsg;
  const char *bk_vol;		/* ptr to old bkup volume name */
  int unit_num;
  FILEIO_BACKUP_RECORD_INFO all_bkup_info[FILEIO_BACKUP_UNDEFINED_LEVEL];
  int first_arv_needed = -1;	/* for self contained, consistent */
  int last_arv_needed = -1;	/* backups, some arv are needed   */
  int unneeded_arv_low = -1, unneeded_arv_high = -1;
  bool beenwarned;
  bool isincremental = false;	/* Assume full backups */
  char logarv_name[PATH_MAX];
  char logarv_name_first[PATH_MAX];
  bool bkup_in_progress = false;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
  int error_code = NO_ERROR;
  FILEIO_BACKUP_HEADER *io_bkup_hdr_p;

  time_t backup_start_time, backup_end_time;
  char old_bkpath[PATH_MAX];
  const char *str_tmp;
  time_t tmp_time;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  memset (&session, 0, sizeof (FILEIO_BACKUP_SESSION));

  /*
   * Determine the absolute path that corresponds with the desired location.
   */
  if (allbackup_path == NULL)
    {
      allbackup_path = log_Path;
    }
#if 0
  /* Removed because it causes problems with deliberate symlinks,
   * i.e. solaris tape device names.
   */
  else if (realpath (allbackup_path, real_pathbuf) != NULL)
    {
      allbackup_path = real_pathbuf;
    }
#endif

  /*
   * Determine the first log archive that will be needed to insure
   * consistency if we are forced to restore the database with nothing
   * but this backup.
   * first_arv_needed may need to be based on what archive chkpt_lsa is in.
   */
  LOG_CS_ENTER (thread_p);
#if defined(SERVER_MODE)
  if (log_Gl.backup_in_progress == true)
    {
      LOG_CS_EXIT ();
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_BKUP_DUPLICATE_REQUESTS, 0);
      return ER_LOG_BKUP_DUPLICATE_REQUESTS;
    }

  log_Gl.backup_in_progress = true;
  LOG_CS_EXIT ();

loop:
  LOG_CS_ENTER (thread_p);
  /* check if checkpoint is in progress */
  if (log_Gl.run_nxchkpt_atpageid == NULL_PAGEID)
    {
      LOG_CS_EXIT ();
      /* wait until checkpoint process is finished */
      thread_sleep (1, 0);
      goto loop;
    }

  /* block checkpoint process */
  saved_run_nxchkpt_atpageid = log_Gl.run_nxchkpt_atpageid;
  log_Gl.run_nxchkpt_atpageid = NULL_PAGEID;
#endif /* SERVER_MODE */

  if (log_Gl.hdr.last_arv_num_for_syscrashes > -1)
    {
      first_arv_needed = log_Gl.hdr.last_arv_num_for_syscrashes;
    }
  else
    {
      first_arv_needed = log_Gl.hdr.nxarv_num;
    }

  /* Get the current checkpoint address */
  MUTEX_LOCK (rv, log_Gl.chkpt_lsa_lock);
  LSA_COPY (&chkpt_lsa, &log_Gl.hdr.chkpt_lsa);
  MUTEX_UNLOCK (log_Gl.chkpt_lsa_lock);

  LOG_CS_EXIT ();

  switch (backup_level)
    {

    case FILEIO_BACKUP_BIG_INCREMENT_LEVEL:
      /* Start at the lower level backup */
      if (LSA_ISNULL (&log_Gl.hdr.bkup_level0_lsa))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_BACKUP_LEVEL_NOGAPS, 2, backup_level,
		  backup_level - 1);
	  fileio_finalize_backup_info (FILEIO_FIRST_BACKUP_VOL_INFO);
	  error_code = ER_LOG_BACKUP_LEVEL_NOGAPS;
	  goto failure;
	}
      else
	{
	  LSA_COPY (&bkup_start_lsa, &log_Gl.hdr.bkup_level0_lsa);
	}
      isincremental = true;
      break;

    case FILEIO_BACKUP_SMALL_INCREMENT_LEVEL:
      /* Start at the lower level backup */
      if (LSA_ISNULL (&log_Gl.hdr.bkup_level1_lsa))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_BACKUP_LEVEL_NOGAPS, 2, backup_level,
		  backup_level - 1);
	  fileio_finalize_backup_info (FILEIO_FIRST_BACKUP_VOL_INFO);
	  error_code = ER_LOG_BACKUP_LEVEL_NOGAPS;
	  goto failure;
	}
      else
	{
	  LSA_COPY (&bkup_start_lsa, &log_Gl.hdr.bkup_level1_lsa);
	}
      isincremental = true;
      break;

    default:
    case FILEIO_BACKUP_FULL_LEVEL:
      /* Always start at -1|-1 */
      LSA_SET_NULL (&bkup_start_lsa);
      backup_level = FILEIO_BACKUP_FULL_LEVEL;
      break;
    }

  /* Initialization gives us some useful information about the
   * backup location.
   */
  session.type = FILEIO_BACKUP_WRITE;	/* access backup device for write */
  if (fileio_initialize_backup (log_Db_fullname, allbackup_path,
				&session, backup_level, backup_verbose_file,
				num_threads) == NULL)
    {
      error_code = ER_FAILED;
      goto error;
    }

  /*
   * Check for existing backup volumes in this location, and warn
   * the user that they will be destroyed.
   */
  if (fileio_is_volume_exist (log_Name_bkupinfo))
    {
      /* check for writable regular file */
      if (fileio_is_volume_exist_and_file (log_Name_bkupinfo) != true)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_LOG_CREATE_DBBACKUP_DIRINFO, 1,
			       log_Name_bkupinfo);
	  error_code = ER_LOG_CREATE_DBBACKUP_DIRINFO;
	  goto error;
	}
      /* check permission */
      if (access (log_Name_bkupinfo, W_OK) != 0)
	{
	  error_code = ER_LOG_CANNOT_ACCESS_BACKUP;
	}
      backup_volinfo_fp = fopen (log_Name_bkupinfo, "r");
      if (error_code != NO_ERROR || backup_volinfo_fp == NULL)
	{
	  er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_LOG_CANNOT_ACCESS_BACKUP, 1,
			       log_Name_bkupinfo);
	  goto error;
	}
      if (fileio_read_backup_info_entries (backup_volinfo_fp,
					   FILEIO_FIRST_BACKUP_VOL_INFO) !=
	  NO_ERROR)
	{
	  er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_LOG_CANNOT_ACCESS_BACKUP, 1,
			       log_Name_bkupinfo);
	  fclose (backup_volinfo_fp);
	  error_code = ER_LOG_CANNOT_ACCESS_BACKUP;
	  goto error;
	}
      fclose (backup_volinfo_fp);
    }

  if (session.bkup.dtype != FILEIO_BACKUP_VOL_DEVICE)
    {
      /*
       * Delete previous backup volumes from this level.  The first
       * loop tries to determine if indeed there will be anything to
       * unformat/delete, while the second loop does the actual deletions.
       * Deletion is done only if the backup is taking place
       * in the same location as the previous backup or
       * is the same path-name as the the previous backup.
       */
      beenwarned = false;
      unit_num = FILEIO_INITIAL_BACKUP_UNITS;
      bk_vol = fileio_get_backup_info_volume_name (backup_level, unit_num++,
						   FILEIO_FIRST_BACKUP_VOL_INFO);
      while (bk_vol && !beenwarned)
	{
	  if (fileio_is_volume_exist_and_file (bk_vol))
	    {
	      fileio_get_directory_path (old_bkpath, bk_vol);
	      if (strcmp (old_bkpath, allbackup_path) == 0
		  || strcmp (bk_vol, allbackup_path) == 0)
		{
		  if (!logpb_remote_ask_user_before_delete_volumes
		      (thread_p, allbackup_path))
		    {
		      io_bkup_hdr_p = session.bkup.bkuphdr;
		      tmp_time = (time_t) io_bkup_hdr_p->start_time;
		      sprintf (old_bkpath,
			       msgcat_message (MSGCAT_CATALOG_CUBRID,
					       MSGCAT_SET_LOG,
					       MSGCAT_LOG_BACKUP_HALTED_BY_USER),
			       io_bkup_hdr_p->level,
			       fileio_get_base_file_name (io_bkup_hdr_p->
							  db_fullname),
			       ctime (&tmp_time), io_bkup_hdr_p->unit_num);
		      fileio_request_user_response (thread_p,
						    FILEIO_PROMPT_DISPLAY_ONLY,
						    old_bkpath, NULL, NULL,
						    -1, -1, NULL, -1);
		      error_code = ER_FAILED;
		      goto error;
		    }
		  beenwarned = true;
		  break;
		}
	    }			/* if (fileio_is_volume_exist_and_file(bk_vol)) */
	  bk_vol =
	    fileio_get_backup_info_volume_name (backup_level, unit_num++,
						FILEIO_FIRST_BACKUP_VOL_INFO);
	}			/* while */

      /* try to delete the superceded backup volumes */
      if (beenwarned)
	{
	  fileio_remove_all_backup (thread_p, backup_level);
	}
    }

  /* Throw away the old names and make room for new names */
  fileio_clear_backup_info_level (backup_level, false,
				  FILEIO_FIRST_BACKUP_VOL_INFO);

  /*
   * Aside: all backup level related log hdr stuff should be in its own
   * array, including log_Gl (but I am hesitant to change it to avoid subtly
   * changing the disk rep). So for now, only have lower levels think array.
   * Arrays would allow future expansion to more than 3 levels.
   */
  LSA_COPY (&all_bkup_info[0].lsa, &log_Gl.hdr.bkup_level0_lsa);
  all_bkup_info[0].at_time = log_Gl.hdr.bkinfo[0].bkup_attime;
  LSA_COPY (&all_bkup_info[1].lsa, &log_Gl.hdr.bkup_level1_lsa);
  all_bkup_info[1].at_time = log_Gl.hdr.bkinfo[1].bkup_attime;
  LSA_COPY (&all_bkup_info[2].lsa, &log_Gl.hdr.bkup_level2_lsa);
  all_bkup_info[2].at_time = log_Gl.hdr.bkinfo[2].bkup_attime;

  if (session.verbose_fp)
    {
      if (backup_level != 0)
	{
	  fprintf (session.verbose_fp, "\n\n\n");
	}

      switch (backup_level)
	{
	case 0:
	  str_tmp = "Full";
	  break;
	case 1:
	  str_tmp = "Incremental Level 1";
	  break;
	default:
	  str_tmp = "Incremental Level 2";
	  break;
	}
      fprintf (session.verbose_fp, "[ Database(%s) %s Backup start ]\n\n",
	       boot_db_name (), str_tmp);

      fprintf (session.verbose_fp, "- num-threads: %d\n\n",
	       session.read_thread_info.num_threads);

      if (zip_method == FILEIO_ZIP_NONE_METHOD)
	{
	  fprintf (session.verbose_fp, "- compression method: %s\n\n",
		   fileio_get_zip_method_string (zip_method));
	}
      else
	{
	  fprintf (session.verbose_fp,
		   "- compression method: %d (%s), compression level: %d (%s)\n\n",
		   zip_method, fileio_get_zip_method_string (zip_method),
		   zip_level, fileio_get_zip_level_string (zip_level));
	}

      if (skip_activelog)
	{
	  fprintf (session.verbose_fp, "- not include active log.\n\n");
	}

      backup_start_time = time (NULL);
      fprintf (session.verbose_fp, "- backup start time: %s\n",
	       ctime (&backup_start_time));

      fprintf (session.verbose_fp, "- number of permanent volumes: %d\n\n",
	       num_perm_vols);

      fprintf (session.verbose_fp, "- backup progress status\n");
      fprintf (session.verbose_fp,
	       "-----------------------------------------------------------------------------\n");
      fprintf (session.verbose_fp,
	       " volume name                  | # of pages | backup progress status    | done \n");
      fprintf (session.verbose_fp,
	       "-----------------------------------------------------------------------------\n");
    }


  /* Begin backing up in earnest */
  session.bkup.bkuphdr->skip_activelog = skip_activelog;

  if (fileio_start_backup (thread_p, log_Db_fullname, &log_Gl.hdr.db_creation,
			   backup_level, &bkup_start_lsa,
			   &chkpt_lsa, all_bkup_info, &session,
			   zip_method, zip_level) == NULL)
    {
      error_code = ER_FAILED;
      goto error;
    }

  /* Backup every volume */
  for (volid = LOG_DBVOLINFO_VOLID; volid < num_perm_vols; volid++)
    {

      if (volid == NULL_VOLID)
	{
	  continue;
	}

      switch (volid)
	{
	case LOG_DBVOLINFO_VOLID:
	  fileio_make_volume_info_name (vol_backup, log_Db_fullname);
	  from_vlabel = vol_backup;
	  break;
	case LOG_DBLOG_INFO_VOLID:
	case LOG_DBLOG_BKUPINFO_VOLID:
	  /*
	   * These information volumes are backed-up at the very end.
	   */
	  continue;
	case LOG_DBLOG_ACTIVE_VOLID:
	  /*
	   * Archiving log active must be done after all data volumes
	   * have been backed up (in order to insure we get all of the
	   * log records needed to restore consistency).
	   */
	  continue;
	default:
	  from_vlabel = fileio_get_volume_label (volid);
	  break;
	}

      if (volid >= LOG_DBFIRST_VOLID)
	{
	  error_code =
	    logpb_backup_for_volume (thread_p, volid, &chkpt_lsa, &session,
				     isincremental);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}
      else
	{
	  error_code =
	    fileio_backup_volume (thread_p, &session, from_vlabel, volid, -1,
				  false);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}
      /* in case an error occurs, we must destroy our partial backup */
      bkup_in_progress = true;
    }

#if defined(SERVER_MODE)
  /*
   * Only when in client/server, we may need to force an archive
   * of the current log active if there were any active transactions
   * before or during the backup.
   * This is to insure we have enough log records to restore consistency
   * to the database in the event a restore with no other log archives
   * is needed.
   */
  LOG_CS_ENTER (thread_p);
  if (LSA_LT (&chkpt_lsa, &log_Gl.hdr.append_lsa)
      || log_Gl.hdr.append_lsa.pageid > LOGPB_NEXT_ARCHIVE_PAGE_ID)
    {
      logpb_archive_active_log (thread_p, true);
    }

  last_arv_needed = log_Gl.hdr.nxarv_num - 1;
  LOG_CS_EXIT ();

  if (last_arv_needed >= first_arv_needed)
    {
      error_code = logpb_backup_needed_archive_logs (thread_p, &session,
						     first_arv_needed,
						     last_arv_needed);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }
#else /* SERVER_MODE */
  /*
   * In stand-alone, there can be no other activity modifying the log
   * so we do not have to keep any log records to be consistent.
   */
  first_arv_needed = -1;
  last_arv_needed = -1;
#endif /* SERVER_MODE */

  LOG_CS_ENTER (thread_p);

#if defined(SERVER_MODE)
  /* backup the archive logs created during backup existing archive logs */
  if (last_arv_needed < log_Gl.hdr.nxarv_num - 1)
    {
      error_code = logpb_backup_needed_archive_logs (thread_p, &session,
						     last_arv_needed + 1,
						     log_Gl.hdr.nxarv_num -
						     1);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      last_arv_needed = log_Gl.hdr.nxarv_num - 1;
    }
#endif

  flush_info->flush_type = LOG_FLUSH_DIRECT;

  /*
   * Remember the previous backup arv range, in order to delete or tag
   * archives from the previous backup which are no longer needed.
   */
  unneeded_arv_low = log_Gl.hdr.lowest_arv_num_for_backup;
  unneeded_arv_high = log_Gl.hdr.highest_arv_num_for_backup;

  /*
   * Remember the range of archives needed for complete restore of
   * this backup.
   */

  log_Gl.hdr.lowest_arv_num_for_backup = first_arv_needed;
  log_Gl.hdr.highest_arv_num_for_backup = last_arv_needed;

  if (fileio_is_volume_exist (log_Name_info) == true)
    {
      error_code = fileio_backup_volume (thread_p, &session, log_Name_info,
					 LOG_DBLOG_INFO_VOLID, -1, false);
      if (error_code != NO_ERROR)
	{
	  flush_info->flush_type = LOG_FLUSH_NORMAL;
	  LOG_CS_EXIT ();
	  goto error;
	}
    }

  /*
   * We must store the final bkvinf file at the very end of the backup
   * to have the best chance of having all of the information in it.
   * Note: that there is a window that the last bkvinf entry still not being
   * there if a new backup volume is needed while writing this volume.
   * However, in this case, then restore will ask the user for the
   * subsequent backup unit num.
   */
  error_code = logpb_update_backup_volume_info (log_Name_bkupinfo);
  if (error_code != NO_ERROR)
    {
      flush_info->flush_type = LOG_FLUSH_NORMAL;
      LOG_CS_EXIT ();
      goto error;
    }

  /* Clear log header information regarding previous backups */
  logpb_initialize_backup_info ();

  /* Save additional info and metrics from this backup */
  log_Gl.hdr.bkinfo[backup_level].bkup_attime =
    session.bkup.bkuphdr->start_time;

  switch (backup_level)
    {
    case FILEIO_BACKUP_FULL_LEVEL:
    default:
      LSA_COPY (&log_Gl.hdr.bkup_level0_lsa, &chkpt_lsa);
      LSA_SET_NULL (&log_Gl.hdr.bkup_level1_lsa);
      LSA_SET_NULL (&log_Gl.hdr.bkup_level2_lsa);
      break;

    case FILEIO_BACKUP_BIG_INCREMENT_LEVEL:
      LSA_COPY (&log_Gl.hdr.bkup_level1_lsa, &chkpt_lsa);
      LSA_SET_NULL (&log_Gl.hdr.bkup_level2_lsa);
      break;

    case FILEIO_BACKUP_SMALL_INCREMENT_LEVEL:
      LSA_COPY (&log_Gl.hdr.bkup_level2_lsa, &chkpt_lsa);
      break;
    }

  /* Now indicate how many volumes were backed up */
  logpb_flush_header (thread_p);

  if (skip_activelog == 0)
    {
      error_code = fileio_backup_volume (thread_p, &session, log_Name_active,
					 LOG_DBLOG_ACTIVE_VOLID, -1, false);
      if (error_code != NO_ERROR)
	{
	  flush_info->flush_type = LOG_FLUSH_NORMAL;
	  LOG_CS_EXIT ();
	  goto error;
	}
    }

  if (fileio_finish_backup (thread_p, &session) == NULL)
    {
      flush_info->flush_type = LOG_FLUSH_NORMAL;
      LOG_CS_EXIT ();
      error_code = ER_FAILED;
      goto error;
    }

  if (delete_unneeded_logarchives != false)
    {
      catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
			       MSGCAT_SET_LOG,
			       MSGCAT_LOG_DATABASE_BACKUP_WAS_TAKEN);
      if (catmsg)
	{
	  logpb_remove_archive_to_page_id (thread_p, catmsg, safe_pageid);
	}
    }

  /*
   * May need to delete the archives which were "captured" by the
   * previous backup, if they are not within the current deletion range,
   * (The normal deletion range doesn't handle gaps.)
   */
  if (unneeded_arv_high > -1
      && unneeded_arv_low <= log_Gl.hdr.last_deleted_arv_num)
    {

      /*
       * Only necessary to "release" the archives up to the regular
       * deletion range.
       */
      unneeded_arv_high = MIN (unneeded_arv_high,
			       log_Gl.hdr.last_deleted_arv_num);

      if (PRM_LOG_MEDIA_FAILURE_SUPPORT == false)
	{
	  /* Remove the log archives at this point */
	  catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
				   MSGCAT_SET_LOG,
				   MSGCAT_LOG_LOGINFO_PENDING_ARCHIVES_RELEASED);
	  if (catmsg)
	    {
	      logpb_remove_archive_logs_internal (thread_p, unneeded_arv_low,
						  unneeded_arv_high, catmsg,
						  false,
						  log_Gl.hdr.
						  lowest_arv_num_for_backup,
						  log_Gl.hdr.
						  highest_arv_num_for_backup);
	    }
	}
      else
	{
	  /* Mark the range of archive logs as not needed by restore anymore */
	  if (unneeded_arv_high == unneeded_arv_low)
	    {
	      fileio_make_log_archive_name (logarv_name, log_Archive_path,
					    log_Prefix, unneeded_arv_low);
	      catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOG,
				       MSGCAT_LOG_LOGINFO_NOTPENDING_ARCHIVE_COMMENT);
	      if (catmsg == NULL)
		{
		  catmsg = "COMMENT: Log archive %s is not DELETE POSTPONED"
		    " any longer due to more recent\n"
		    "backup and therefore is not needed unless a database media"
		    " crash occurs.\n";
		}
	      error_code = log_dump_log_info (log_Name_info, true,
					      catmsg, logarv_name);
	    }
	  else
	    {
	      fileio_make_log_archive_name (logarv_name_first,
					    log_Archive_path, log_Prefix,
					    log_Gl.hdr.
					    last_arv_num_for_syscrashes);
	      fileio_make_log_archive_name (logarv_name, log_Archive_path,
					    log_Prefix,
					    log_Gl.hdr.nxarv_num - 1);

	      catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOG,
				       MSGCAT_LOG_LOGINFO_MULT_NOTPENDING_ARCHIVES_COMMENT);
	      if (catmsg == NULL)
		{
		  catmsg = "COMMENT: Log archives from %s to %s"
		    " are not DELETE POSTPONED any longer due to\n"
		    "more recent backup and therefore are not needed unless"
		    " a database media\ncrash occurs.\n";
		}
	      error_code = log_dump_log_info (log_Name_info, false, catmsg,
					      logarv_name_first, logarv_name);
	    }
	  if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
	    {
	      flush_info->flush_type = LOG_FLUSH_NORMAL;
	      LOG_CS_EXIT ();
	      goto error;
	    }
	}
    }

  flush_info->flush_type = LOG_FLUSH_NORMAL;
  LOG_CS_EXIT ();

  error_code = logpb_update_backup_volume_info (log_Name_bkupinfo);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  if (session.verbose_fp)
    {
      fprintf (session.verbose_fp,
	       "-----------------------------------------------------------------------------\n\n");
      backup_end_time = time (NULL);
      fprintf (session.verbose_fp, "# backup end time: %s\n",
	       ctime (&backup_end_time));
      switch (backup_level)
	{
	case 0:
	  str_tmp = "Full";
	  break;
	case 1:
	  str_tmp = "Incremental Level 1";
	  break;
	default:
	  str_tmp = "Incremental Level 2";
	  break;
	}
      fprintf (session.verbose_fp, "[ Database(%s) %s Backup end ]\n",
	       boot_db_name (), str_tmp);
    }

  fileio_abort_backup (thread_p, &session, false);

#if defined(SERVER_MODE)
  LOG_CS_ENTER (thread_p);
  log_Gl.run_nxchkpt_atpageid = saved_run_nxchkpt_atpageid;
  log_Gl.backup_in_progress = false;
  LOG_CS_EXIT ();
#endif /* SERVER_MODE */

  return NO_ERROR;

  /* ********* */
error:

  /*
   * Destroy the backup that has been created.
   */
  fileio_abort_backup (thread_p, &session, bkup_in_progress);

failure:
  if (session.verbose_fp)
    {
      fclose (session.verbose_fp);
      session.verbose_fp = NULL;
    }

#if defined(SERVER_MODE)
  LOG_CS_ENTER (thread_p);
  log_Gl.run_nxchkpt_atpageid = saved_run_nxchkpt_atpageid;
  log_Gl.backup_in_progress = false;
  LOG_CS_EXIT ();
#endif /* SERVER_MODE */

  return error_code;
}

/*
 * log_update_backup_volinfo - UPDATE DISK VERSION OF BKUP VOLINFO FILE
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   bkupinfo_file_name(in): file name to write info to
 *
 * NOTE: Save the in-memory cache of backup volume names to a file.
 */
static int
logpb_update_backup_volume_info (const char *bkupinfo_file_name)
{
  FILE *backup_volinfo_fp;
  int error_code = NO_ERROR;

  backup_volinfo_fp = fopen (bkupinfo_file_name, "w");
  if (backup_volinfo_fp == NULL)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_LOG_CREATE_DBBACKUP_DIRINFO, 1,
			   bkupinfo_file_name);
      error_code = ER_LOG_CREATE_DBBACKUP_DIRINFO;
      return error_code;
    }

  /*
   * If an error occurs while writing, should allow the user to try again.
   */
  error_code = fileio_write_backup_info_entries (backup_volinfo_fp,
						 FILEIO_FIRST_BACKUP_VOL_INFO);
  if (error_code != NO_ERROR)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_LOG_CREATE_DBBACKUP_DIRINFO, 1,
			   bkupinfo_file_name);
      fclose (backup_volinfo_fp);
      return error_code;
    }
  fflush (backup_volinfo_fp);
  fclose (backup_volinfo_fp);

  return error_code;
}

/*
 * logpb_check_stop_at_time - Check if the stopat time is valid
 *
 * return: NO_ERROR if valid, ER_FAILED otherwise
 *
 *   session(in):
 *   stop_at(in):
 *   backup_time(in):
 */
static int
logpb_check_stop_at_time (FILEIO_BACKUP_SESSION * session,
			  time_t stop_at, time_t backup_time)
{
  char ctime_buf1[64], ctime_buf2[64];

  if (stop_at < backup_time)
    {
      strcpy (ctime_buf1, ctime (&stop_at));
      strcpy (ctime_buf2, ctime (&backup_time));

      ctime_buf1[strlen (ctime_buf1) - 1] = 0;	/* strip '\n' */
      ctime_buf2[strlen (ctime_buf2) - 1] = 0;

      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOG,
				       MSGCAT_LOG_UPTODATE_ERROR),
	       ctime_buf1, ctime_buf2);

      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * logpb_restore - Restore volume from its backup
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in):
 *   newall_path(in):
 *   ask_forpath(in):
 *   r_args(in):
 *
 * NOTE:Restore a database from its backup files. This function is run
 *              without recovery. The logs must be applied to the restored
 *              volume to finish the full restore process. This is not done
 *              here since the database may have other volumes to restore.
 *              We must lock the active log during the restore to keep other
 *              utilities from trying to use the db files being restored.
 *              This routine leaves the database locked, as there will
 *              probably be a restart following the restore.
 *
 *     This function must be run offline.
 *
 *     Note this function is incomplete.. How to change location of files
 *     during restore... The main problem here is the VOLINFO which has to
 *     be recreated without updating the header of the volumes.
 */
int
logpb_restore (THREAD_ENTRY * thread_p, const char *db_fullname,
	       const char *logpath, const char *prefix_logname,
	       BO_RESTART_ARG * r_args)
{
  FILEIO_BACKUP_SESSION session_storage;
  FILEIO_BACKUP_SESSION *session = NULL;
  const char *nopath_name;	/* Name without path          */
  const char *volnameptr;
  char to_volname[PATH_MAX];	/* Name of a volume (TO)      */
  char verbose_to_volname[PATH_MAX];	/* Printable name of a volume (TO) */
  char prev_volname[PATH_MAX];	/* Name of a prev volume (TO) */
  char from_volbackup[PATH_MAX];	/* Name of the backup volume
					 * (FROM)
					 */
  VOLID to_volid;
  int retry;
  struct stat stbuf;
  FILE *backup_volinfo_fp = NULL;	/* Pointer to backup
					 * information/directory file
					 */
  int another_vol;
  INT64 db_creation;
  INT64 bkup_match_time = 0;
  PGLENGTH db_iopagesize;
  PGLENGTH log_page_size;
  float db_compatibility;
  PGLENGTH bkdb_iopagesize;
  float bkdb_compatibility;
  FILEIO_RESTORE_PAGE_CACHE pages_cache;
  FILEIO_BACKUP_LEVEL try_level;
  bool first_time = true;
  bool remember_pages;
  bool error_expected = false;
  bool restore_in_progress = false;	/* true if any vols restored */
  int lgat_vdes = NULL_VOLDES;
  time_t restore_start_time, restore_end_time;
  int loop_cnt = 0;
  char lgat_tmpname[PATH_MAX];	/* active log temp name */
  struct stat stat_buf;
  int error_code = NO_ERROR, success = NO_ERROR;
  bool printtoc;
  const char *verbose_file;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;
  char format_string[64];
  INT64 backup_time;
  REL_COMPATIBILITY compat;

  try_level = (FILEIO_BACKUP_LEVEL) r_args->level;
  memset (&session_storage, 0, sizeof (FILEIO_BACKUP_SESSION));
  memset (verbose_to_volname, 0, PATH_MAX);
  memset (lgat_tmpname, 0, PATH_MAX);

  LOG_CS_ENTER (thread_p);
  flush_info->flush_type = LOG_FLUSH_DIRECT;
  pages_cache.ht = NULL;
  pages_cache.heap_id = HL_NULL_HEAPID;

  if (logpb_find_header_parameters (thread_p, db_fullname, logpath,
				    prefix_logname, &db_iopagesize,
				    &log_page_size, &db_creation,
				    &db_compatibility) == -1)
    {
      db_iopagesize = IO_PAGESIZE;
      log_page_size = LOG_PAGESIZE;
      db_creation = 0;
      db_compatibility = rel_disk_compatible ();
    }

  /*
   * Must lock the database if possible. Would be nice to have a way
   * to lock the db somehow even when the lgat file does not exist.
   */
  if (fileio_is_volume_exist (log_Name_active))
    {
      lgat_vdes = fileio_mount (thread_p, db_fullname, log_Name_active,
				LOG_DBLOG_ACTIVE_VOLID, true, false);
      if (lgat_vdes == NULL_VOLDES)
	{
	  error_code = ER_FAILED;
	  flush_info->flush_type = LOG_FLUSH_NORMAL;
	  LOG_CS_EXIT ();
	  goto error;
	}
    }

  /*
   * Create a memory hash table to remember the id's of pages
   * that have been written.  We only need to write the page (once)
   * from the most recent backup.
   */

  /* For the hash table size, it would be nice to have a real estimate
   * of the number of pages.  The trouble is, we are restoring, and thus
   * haven't the foggiest notion of the eventual number of pages in all
   * restored volumes.
   */
  pages_cache.ht = mht_create ("Restored Pages hash table",
			       LOG_BKUP_HASH_NUM_PAGEIDS, pgbuf_hash_vpid,
			       pgbuf_compare_vpid);
  if (pages_cache.ht == NULL)
    {
      error_code = ER_FAILED;
      flush_info->flush_type = LOG_FLUSH_NORMAL;
      LOG_CS_EXIT ();
      goto error;
    }
  pages_cache.heap_id = db_create_fixed_heap (sizeof (VPID),
					      LOG_BKUP_HASH_NUM_PAGEIDS);
  if (pages_cache.heap_id == HL_NULL_HEAPID)
    {
      error_code = ER_FAILED;
      flush_info->flush_type = LOG_FLUSH_NORMAL;
      LOG_CS_EXIT ();
      goto error;
    }

  sprintf (format_string, "%%%ds", PATH_MAX - 1);

  while (success == NO_ERROR && try_level >= FILEIO_BACKUP_FULL_LEVEL
	 && try_level < FILEIO_BACKUP_UNDEFINED_LEVEL)
    {
      /*
       * Open the backup information/directory file. This backup file contains
       * information related to the volumes that were backed up.
       */
      nopath_name = fileio_get_base_file_name (db_fullname);
      fileio_make_backup_volume_info_name (from_volbackup, logpath,
					   nopath_name);

      if (!first_time)
	{
	  /* Prepare to reread bkvinf file restored by higher level */
	  fileio_clear_backup_info_level (FILEIO_BACKUP_FULL_LEVEL, false,
					  FILEIO_FIRST_BACKUP_VOL_INFO);

	  /* Have to match timestamps of lower levels when restoring */
	  bkup_match_time =
	    session->bkup.bkuphdr->previnfo[try_level].at_time;

	  /* Clean up previous restore of higher level */
	  if (fileio_finish_restore (thread_p, session) == NO_ERROR)
	    {
	      success = NO_ERROR;
	    }
	  else
	    {
	      success = ER_FAILED;
	    }
	}


      /*
       * Mount the backup directory information, if it exists.
       */
      while ((stat (from_volbackup, &stbuf) == -1) ||
	     (backup_volinfo_fp = fopen (from_volbackup, "r")) == NULL)
	{

	  /*
	   * When user specifies an explicit location, the backup vinf
	   * file is optional.
	   */
	  if (r_args->backuppath)
	    {
	      break;
	    }

	  /*
	   * Backup volume information is not online
	   */
	  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
					   MSGCAT_SET_LOG,
					   MSGCAT_LOG_STARTS));
	  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
					   MSGCAT_SET_LOG,
					   MSGCAT_LOG_BACKUPINFO_NEEDED),
		   from_volbackup);
	  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
					   MSGCAT_SET_LOG,
					   MSGCAT_LOG_STARTS));

	  if (scanf ("%d", &retry) != 1)
	    {
	      retry = 0;
	    }

	  switch (retry)
	    {
	    case 0:		/* quit */
	      /* Cannot access backup file.. Restore from backup is cancelled */
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_CANNOT_ACCESS_BACKUP, 1, from_volbackup);
	      error_expected = true;
	      error_code = ER_LOG_CANNOT_ACCESS_BACKUP;
	      flush_info->flush_type = LOG_FLUSH_NORMAL;
	      LOG_CS_EXIT ();
	      goto error;

	    case 2:
	      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
					       MSGCAT_SET_LOG,
					       MSGCAT_LOG_NEWLOCATION));
	      if (scanf (format_string, from_volbackup) != 1)
		{
		  /* Cannot access backup file.. Restore from backup is cancelled */
		  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_LOG_CANNOT_ACCESS_BACKUP, 1, from_volbackup);
		  error_code = ER_LOG_CANNOT_ACCESS_BACKUP;
		  flush_info->flush_type = LOG_FLUSH_NORMAL;
		  LOG_CS_EXIT ();
		  goto error;
		}
	      break;

	    case 1:
	    default:
	      break;
	    }
	}			/* while */

      /*
       * If we get to here, we can read the bkvinf file, OR one does not
       * exist and it is not required.
       */
      if (backup_volinfo_fp != NULL)
	{
	  if (fileio_read_backup_info_entries (backup_volinfo_fp,
					       FILEIO_FIRST_BACKUP_VOL_INFO)
	      == NO_ERROR)
	    {
	      volnameptr =
		fileio_get_backup_info_volume_name (try_level,
						    FILEIO_INITIAL_BACKUP_UNITS,
						    FILEIO_FIRST_BACKUP_VOL_INFO);
	      if (volnameptr != NULL)
		{
		  strcpy (from_volbackup, volnameptr);
		}
	      else
		{
		  fileio_make_backup_name (from_volbackup, nopath_name,
					   logpath, try_level,
					   FILEIO_INITIAL_BACKUP_UNITS);
		}
	    }
	  else
	    {
	      fclose (backup_volinfo_fp);
	      backup_volinfo_fp = NULL;
	      error_code = ER_FAILED;
	      flush_info->flush_type = LOG_FLUSH_NORMAL;
	      LOG_CS_EXIT ();
	      goto error;
	    }

	  fclose (backup_volinfo_fp);
	  backup_volinfo_fp = NULL;
	}

      /* User wishes to override the bkvinf file entry for some locations. */
      if (r_args->backuppath)
	{
	  strncpy (from_volbackup, r_args->backuppath, PATH_MAX);
	}

      /* User only wants information about the backup */
      if (r_args->printtoc)
	{
	  error_code = fileio_list_restore (thread_p, db_fullname,
					    from_volbackup, &bkdb_iopagesize,
					    &bkdb_compatibility, try_level,
					    r_args->newvolpath);

	  flush_info->flush_type = LOG_FLUSH_NORMAL;
	  mht_destroy (pages_cache.ht);
	  db_destroy_fixed_heap (pages_cache.heap_id);
	  LOG_CS_EXIT ();
	  return error_code;
	}

      printtoc = (r_args->printtoc) ? false : true;
      verbose_file = (r_args->verbose_file) ? r_args->verbose_file : NULL;
      if (fileio_start_restore (thread_p, db_fullname, from_volbackup,
				db_creation, &bkdb_iopagesize,
				&bkdb_compatibility, &session_storage,
				try_level, printtoc, bkup_match_time,
				verbose_file, r_args->newvolpath) == NULL)
	{
	  /* Cannot access backup file.. Restore from backup is cancelled */
	  if (er_errid () == ER_GENERIC_ERROR)
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_CANNOT_ACCESS_BACKUP, 1, from_volbackup);
	    }
	  error_code = ER_LOG_CANNOT_ACCESS_BACKUP;
	  flush_info->flush_type = LOG_FLUSH_NORMAL;
	  LOG_CS_EXIT ();
	  goto error;
	}

      memset (session_storage.bkup.log_path, 0, PATH_MAX);
      strncpy (session_storage.bkup.log_path, logpath, PATH_MAX);

      session = &session_storage;

      if (first_time)
	{
	  if (r_args->restore_upto_bktime)
	    {
	      r_args->stopat = (time_t) session->bkup.bkuphdr->end_time;
	    }
	  else if (r_args->stopat > 0)
	    {
	      if (session->bkup.bkuphdr->end_time > 0)
		{
		  backup_time = session->bkup.bkuphdr->end_time;
		}
	      else
		{
		  backup_time = session->bkup.bkuphdr->start_time;
		}

	      if (logpb_check_stop_at_time (session, r_args->stopat,
					    (time_t) backup_time) != NO_ERROR)
		{
		  error_code = fileio_finish_restore (thread_p, session);

		  flush_info->flush_type = LOG_FLUSH_NORMAL;
		  mht_destroy (pages_cache.ht);
		  db_destroy_fixed_heap (pages_cache.heap_id);
		  LOG_CS_EXIT ();
		  return error_code;
		}
	    }
	}

      if (first_time && db_iopagesize != bkdb_iopagesize)
	{
	  /*
	   * Pagesize is incorrect. We need to undefine anything that has been
	   * created with old pagesize and start again.
	   * If we do not have a log, we should reset the pagesize and start the
	   * restore process.
	   */
	  if (log_Gl.append.vdes == NULL_VOLDES)
	    {
	      /*
	       * Reset the page size
	       */
	      if (db_set_page_size (bkdb_iopagesize, log_page_size) !=
		  NO_ERROR)
		{
		  error_code = ER_FAILED;
		  flush_info->flush_type = LOG_FLUSH_NORMAL;
		  LOG_CS_EXIT ();
		  goto error;
		}

	      error_code = logtb_define_trantable_log_latch (thread_p, -1);
	      if (error_code != NO_ERROR)
		{
		  flush_info->flush_type = LOG_FLUSH_NORMAL;
		  LOG_CS_EXIT ();
		  goto error;
		}
	    }
	}

      /* Can this be moved to restore_continue? */
      /* Removed strict condition for checking disk compatibility.
       * Check it according to the predefined rules.
       */
      compat = rel_is_disk_compatible (bkdb_compatibility, NULL);
      if (compat != REL_FULLY_COMPATIBLE && compat != REL_BACKWARD_COMPATIBLE)
	{
	  /* Database is incompatible with current release */
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_BKUP_INCOMPATIBLE, 2,
		  rel_name (), rel_release_string ());
	  error_code = ER_LOG_BKUP_INCOMPATIBLE;
	  flush_info->flush_type = LOG_FLUSH_NORMAL;
	  LOG_CS_EXIT ();
	  goto error;
	}

      if (session->verbose_fp)
	{
	  if (first_time)
	    {
	      fprintf (session->verbose_fp,
		       "\n[ Database(%s) Restore (level = %d) start ]\n\n",
		       boot_db_name (), r_args->level);

	      restore_start_time = time (NULL);
	      fprintf (session->verbose_fp, "- restore start time: %s\n",
		       ctime (&restore_start_time));

	      fprintf (session->verbose_fp, "- restore steps: %d \n",
		       r_args->level + 1);
	    }

	  fprintf (session->verbose_fp,
		   " step %1d) restore using (level = %d) backup data\n",
		   ++loop_cnt, try_level);
	  fprintf (session->verbose_fp, "\n");

	  fprintf (session->verbose_fp,
		   "- restore progress status (using level = %d backup data)\n",
		   try_level);
	  fprintf (session->verbose_fp,
		   " -----------------------------------------------------------------------------\n");
	  fprintf (session->verbose_fp,
		   " volume name                  | # of pages |  restore progress status  | done \n");
	  fprintf (session->verbose_fp,
		   " -----------------------------------------------------------------------------\n");
	}

      /* add new bkvinf entry into cache data */
      fileio_add_volume_to_backup_info (session->bkup.vlabel,
					try_level,
					session->bkup.bkuphdr->unit_num,
					FILEIO_SECOND_BACKUP_VOL_INFO);

      if (first_time)
	{
	  LSA_COPY (&session->bkup.last_chkpt_lsa,
		    &session->bkup.bkuphdr->chkpt_lsa);
	}

      while (success == NO_ERROR)
	{
	  another_vol =
	    fileio_get_next_restore_file (thread_p, session, to_volname,
					  &to_volid);
	  if (another_vol == 1)
	    {
	      if (session->verbose_fp)
		{
		  strcpy (verbose_to_volname, to_volname);
		}

	      if (to_volid == LOG_DBLOG_ACTIVE_VOLID)
		{
		  /* rename _lgat to _lgat_tmp name */
		  fileio_make_log_active_temp_name (lgat_tmpname,
						    (FILEIO_BACKUP_LEVEL)
						    r_args->level,
						    to_volname);
		  strcpy (to_volname, lgat_tmpname);
		}

	      restore_in_progress = true;
	      remember_pages = true;
	      /*
	       * Another volume/file to restore
	       */
	      switch (to_volid)
		{
		case LOG_DBLOG_BKUPINFO_VOLID:
		case LOG_DBLOG_ACTIVE_VOLID:
		case LOG_DBLOG_INFO_VOLID:
		case LOG_DBVOLINFO_VOLID:
		case LOG_DBLOG_ARCHIVE_VOLID:


		  /* We can only take the most recent information, and we
		   * do not want to overwrite it with out of data information
		   * from earlier backups.  This is because we are
		   * applying the restoration in reverse time order.
		   */
		  if (!first_time)
		    {
		      /* Need to skip over this volume in the backup */
		      success =
			fileio_skip_restore_volume (thread_p, session);
		      if (success != NO_ERROR)
			{
			  success = ER_FAILED;
			  error_code = ER_FAILED;
			  flush_info->flush_type = LOG_FLUSH_NORMAL;
			  LOG_CS_EXIT ();
			  goto error;
			}
		      else
			{
			  success = NO_ERROR;
			  continue;
			}
		    }
		  break;

		default:
		  break;
		}

	      /* we need to tell fileio_restore_volume to avoid tracking the
	       * pages for some volids.
	       */
	      if (to_volid < LOG_DBFIRST_VOLID)
		{
		  remember_pages = false;
		}

	      success = fileio_restore_volume (thread_p, session,
					       to_volname, verbose_to_volname,
					       prev_volname, &pages_cache,
					       remember_pages);
	    }
	  else if (another_vol == 0)
	    {
	      break;
	    }
	  else
	    {
	      success = ER_FAILED;
	      break;
	    }
	}

      /* if the device type is FILEIO_BACKUP_VOL_DEVICE, the end time of backup
       * is loaded during the last fileio_get_next_restore_file() call,
       * so we must check if the stopat time is valid.
       */
      if (first_time && session->bkup.dtype == FILEIO_BACKUP_VOL_DEVICE)
	{
	  if (r_args->restore_upto_bktime)
	    {
	      r_args->stopat = (time_t) session->bkup.bkuphdr->end_time;
	    }
	  else if (r_args->stopat > 0
		   && logpb_check_stop_at_time (session, r_args->stopat,
						(time_t) session->bkup.
						bkuphdr->end_time) !=
		   NO_ERROR)
	    {
	      flush_info->flush_type = LOG_FLUSH_NORMAL;
	      LOG_CS_EXIT ();
	      error_code = ER_FAILED;
	      goto error;
	    }
	}

      first_time = false;

      if (session->verbose_fp)
	{
	  fprintf (session->verbose_fp,
		   " -----------------------------------------------------------------------------\n\n");
	}

      try_level = (FILEIO_BACKUP_LEVEL) (try_level - 1);
    }

  /* make bkvinf file */
  nopath_name = fileio_get_base_file_name (db_fullname);
  fileio_make_backup_volume_info_name (from_volbackup, logpath, nopath_name);
  backup_volinfo_fp = fopen (from_volbackup, "w");
  if (backup_volinfo_fp != NULL)
    {
      fileio_write_backup_info_entries (backup_volinfo_fp,
					FILEIO_SECOND_BACKUP_VOL_INFO);
      fclose (backup_volinfo_fp);
    }

  /* rename logactive tmp to logactive */
  if (stat (log_Name_active, &stat_buf) != 0
      && stat (lgat_tmpname, &stat_buf) == 0)
    {
      rename (lgat_tmpname, log_Name_active);
    }

  unlink (lgat_tmpname);

  if (session != NULL)
    {
      if (session->verbose_fp)
	{
	  restore_end_time = time (NULL);
	  fprintf (session->verbose_fp, "- restore end time: %s\n",
		   ctime (&restore_end_time));
	  fprintf (session->verbose_fp,
		   "[ Database(%s) Restore (level = %d) end ]\n",
		   boot_db_name (), r_args->level);
	}

      if (fileio_finish_restore (thread_p, session) == NO_ERROR)
	{
	  error_code = NO_ERROR;
	}
      else
	{
	  error_code = ER_FAILED;
	}
    }

  flush_info->flush_type = LOG_FLUSH_NORMAL;
  LOG_CS_EXIT ();

  mht_destroy (pages_cache.ht);
  db_destroy_fixed_heap (pages_cache.heap_id);

  fileio_finalize_backup_info (FILEIO_SECOND_BACKUP_VOL_INFO);

  if (success != NO_ERROR)
    {
      return success;
    }
  else
    {
      return error_code;
    }

  /* **** */
error:
  if (restore_in_progress)
    {
      /*
       * We have probably already restored something to their database
       * and therefore they need to be sure and try another restore until
       * they succeed.
       */
      fprintf (stdout,
	       msgcat_message (MSGCAT_CATALOG_CUBRID,
			       MSGCAT_SET_LOG,
			       MSGCAT_LOG_READ_ERROR_DURING_RESTORE),
	       session->bkup.name,
	       ((session->bkup.bkuphdr == NULL)
		? FILEIO_INITIAL_BACKUP_UNITS
		: session->bkup.bkuphdr->unit_num), to_volname,
	       session->dbfile.volid);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_MAYNEED_MEDIA_RECOVERY,
	      1, log_Name_bkupinfo);
      error_code = ER_LOG_MAYNEED_MEDIA_RECOVERY;
    }

  if (backup_volinfo_fp != NULL)
    {
      fclose (backup_volinfo_fp);
    }

  if (session != NULL)
    {
      fileio_abort_restore (thread_p, session);
    }

  if (pages_cache.ht != NULL)
    {
      mht_destroy (pages_cache.ht);
    }

  if (pages_cache.heap_id != HL_NULL_HEAPID)
    {
      db_destroy_fixed_heap (pages_cache.heap_id);
    }

  if (lgat_vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, lgat_vdes);
    }

  if (!error_expected)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "logpb_restore");
    }

  if (lgat_tmpname[0] != '\0')
    {
      unlink (lgat_tmpname);
    }

  return error_code;
}

/*
 * logpb_start_where_path - Start where paths for copy/rename volumes
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   to_db_fullname(in): Full name of the new database
 *   toext_path(in): A path if all volumes are placed in one
 *                               place. If NULL is given,
 *                                 - If file "fileof_vols_and_wherepaths" is
 *                                   given, the path is found in this file.
 *                                 - Each volume is copied to same place where
 *                                   the volume reside.
 *                               This parameter should be NULL, if the above
 *                               file is given.
 *   toext_name(in/out): Name to be used for volume extensions when
 *                               individual volume entries are not given in
 *                               file "fileof_vols_and_wherepaths".
 *   ext_path(in/out): Location for entries
 *                               Set as a side effect to either:
 *                                 - toext_path when all vols are placed in
 *                                   the same location.
 *                                 - alloc_extpath when volumes are copied or
 *                                   renamed at the same location as the
 *                                   original volumes.
 *                                 - NULL when file of "fileof_vols_and_
 *                                   wherepaths" is given.
 *   alloc_extpath(in/out): Set as a side effect to newly malloced area
 *                               when the volumes are copied/renamed in place.
 *                               If an area is allocated, it should be freed
 *                               using free_and_init.
 *   fileof_vols_and_wherepaths(in): A file is given when the user decides to
 *                               control the copy/rename of the volume by
 *                               individual bases. That is, user decides to
 *                               spread the volumes over several locations and
 *                               or to name the volumes.
 *                               Each volume entry consists of:
 *                                 volid from_fullvolname to_fullvolname
 *   where_paths_fp(in/out): Set as a side effect to file descriptor of
 *                               the given file. The caller must close the
 *                               file, when a pointer is returned.
 *
 * NOTE: Prepare variables to start finding paths for copying or
 *              renaming volumes.
 */
static int
logpb_start_where_path (const char *to_db_fullname, const char *toext_path,
			const char **toext_name, char **ext_path,
			char **alloc_extpath,
			const char *fileof_vols_and_wherepaths,
			FILE ** where_paths_fp)
{
  /*
   * Get the name of extensions to be used when "fileof_vols_and_wherepaths"
   * is not given.
   */

  *toext_name = fileio_get_base_file_name (to_db_fullname);
  *alloc_extpath = NULL;
  *ext_path = NULL;
  *where_paths_fp = NULL;

  /*
   * Where are the volumes going to be placed ?
   *   - All or them at same place: toext_path or the same as database
   *   - The user indicated where to create each individual copy
   */

  if (fileof_vols_and_wherepaths != NULL)
    {
      /*
       * The user seems to want to spread the volumes over several locations.
       * User must indicate where each individual database volume is going to
       * be placed.
       */
      *where_paths_fp = fopen (fileof_vols_and_wherepaths, "r");
      if (*where_paths_fp == NULL)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_LOG_USER_FILE_UNKNOWN, 1,
			       fileof_vols_and_wherepaths);
	  return ER_LOG_USER_FILE_UNKNOWN;
	}
    }
  /*
   * Either all copies are created at the same place, or each individual
   * volumes is copied at the original volume location
   */
  if (toext_path == NULL)
    {
      /*
       * Each volume is copied to the same place where the original volume
       * resides
       */
      *alloc_extpath = (char *) malloc (PATH_MAX);
      if (*alloc_extpath == NULL)
	{
	  return ER_FAILED;
	}
      *ext_path = *alloc_extpath;
    }
  else
    {
      /* All the volumes are created in one location */
      *ext_path = (char *) toext_path;
    }

  return NO_ERROR;
}

/*
 * logpb_next_where_path - Find next path for copy/rename next volume
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   to_db_fullname(in): Full name of the new database
 *   toext_path(in): A path if all volumes are placed in one
 *                               place. If NULL is given,
 *                                 - If file "fileof_vols_and_wherepaths" is
 *                                   given, the path is found in this file.
 *                                 - Each volume is copied to same place where
 *                                   the volume reside.
 *                               This parameter should be NULL, if the above
 *                               file is given.
 *   ext_name(in): Name to be used for volume extensions when
 *                               individual volume entries are not given in
 *                               file "fileof_vols_and_wherepaths".
 *   ext_path(in): Location for entries
 *   fileof_vols_and_wherepaths(in): A file is given when the user decides to
 *                               control the copy/rename of the volume by
 *                               individual bases. That is, user decides to
 *                               spread the volumes over several locations and
 *                               or to name the volumes.
 *                               Each volume entry consists of:
 *                                 volid from_fullvolname to_fullvolname
 *   where_paths_fp(in): Pointer to above file if any or NULL
 *   num_perm_vols(in): Number of permanent volumes in the database.
 *   volid(in): Next volume that must be processes.
 *   from_volname(in/out): Current name of volume.
 *   to_volname(in): New name to be used for the volume.
 *
 * NOTE: Get the name of next volume to be processed.
 */
static int
logpb_next_where_path (const char *to_db_fullname, const char *toext_path,
		       const char *ext_name, char *ext_path,
		       const char *fileof_vols_and_wherepaths,
		       FILE * where_paths_fp, int num_perm_vols,
		       VOLID volid, char *from_volname, char *to_volname)
{
  const char *current_vlabel;
  int from_volid;
#if !defined(WINDOWS)
  char link_path[PATH_MAX];
  struct stat stat_buf;
#endif
  int error_code = NO_ERROR;
  char format_string[64];

  current_vlabel = fileio_get_volume_label (volid);
  sprintf (format_string, "%%d %%%ds %%%ds", PATH_MAX - 1, PATH_MAX - 1);

  /*
   * If a file for paths was given, get the name of the "to" volume from it
   */
  if (where_paths_fp != NULL)
    {
      if (fscanf (where_paths_fp, format_string,
		  &from_volid, from_volname, to_volname) != 3)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_USER_FILE_WITHOUT_ENOUGH_ENTRIES, 2,
		  fileof_vols_and_wherepaths, num_perm_vols);
	  return ER_LOG_USER_FILE_WITHOUT_ENOUGH_ENTRIES;
	}
      /*
       * Primary volume must be identical to database name
       */
      if (volid == LOG_DBFIRST_VOLID
	  && strcmp (to_volname, to_db_fullname) != 0)
	{

#if defined(WINDOWS)
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_USER_FILE_INCORRECT_PRIMARY_VOLNAME, 8,
		  fileof_vols_and_wherepaths, volid + 1, from_volid,
		  from_volname, to_volname, (int) volid, current_vlabel,
		  to_db_fullname);
	  return ER_ERROR_SEVERITY;
#else /* WINDOWS */
	  error_code = fileio_symlink (to_volname, to_db_fullname, true);
	  if (error_code != NO_ERROR)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_USER_FILE_INCORRECT_PRIMARY_VOLNAME, 8,
		      fileof_vols_and_wherepaths, volid + 1, from_volid,
		      from_volname, to_volname, (int) volid, current_vlabel,
		      to_db_fullname);
	      return error_code;
	    }

	  strcpy (to_volname, to_db_fullname);
#endif /* WINDOWS */
	}
      else
	{
	  if (volid != from_volid
	      || util_compare_filepath (current_vlabel, from_volname) != 0)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_USER_FILE_UNORDERED_ENTRIES, 7,
		      fileof_vols_and_wherepaths, volid + 1, from_volid,
		      from_volname, to_volname, (int) volid, current_vlabel);
	      return ER_LOG_USER_FILE_UNORDERED_ENTRIES;
	    }

#if !defined(WINDOWS)
	  if (stat (to_volname, &stat_buf) != -1)
	    {
	      if (S_ISCHR (stat_buf.st_mode))
		{
		  fileio_get_directory_path (ext_path, to_db_fullname);
		  fileio_make_volume_ext_name (link_path, ext_path, ext_name,
					       volid);
		  error_code = fileio_symlink (to_volname, link_path, true);
		  if (error_code != NO_ERROR)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_BO_CANNOT_CREATE_LINK, 2, to_volname,
			      link_path);
		      return error_code;
		    }

		  strcpy (to_volname, link_path);
		}
	    }
#endif /* !WINDOWS */
	}
    }
  else
    {
      /*
       * The decision is done consulting the arguments of the function
       */

      /*
       * Primary volume must be identical to database name
       */
      if (volid == LOG_DBFIRST_VOLID)
	{
	  strcpy (to_volname, to_db_fullname);
	}
      else
	{
	  if (toext_path == NULL)
	    {
	      /*
	       * The volume is copied to the same place where the original volume
	       * resides
	       */
	      if (fileio_get_directory_path (ext_path, current_vlabel) ==
		  NULL)
		{
		  ext_path[0] = '\0';
		}
	    }
	  fileio_make_volume_ext_name (to_volname, ext_path, ext_name, volid);
	}
      strcpy (from_volname, current_vlabel);
    }

  return NO_ERROR;
}

/*
 * logpb_copy_volume - Copy a volume
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   from_volid(in): The identifier of the volume to be copied
 *   to_volname(in): Name of the new volume
 *   db_creation(in): Creation timestamp for the volume
 *   to_volchkpt_lsa(in): Checkpoint location to be used in the new volume
 *
 * NOTE: Copy the volume identified by "from_volid" to "tonew_volname".
 */
static int
logpb_copy_volume (THREAD_ENTRY * thread_p, VOLID from_volid,
		   const char *to_volname, INT64 * db_creation,
		   LOG_LSA * to_volchkpt_lsa)
{
  int from_vdes, to_vdes;	/* Descriptor for "from" and "to" volumes */
  DKNPAGES npages;		/* Size of the volume                     */
  int error_code = NO_ERROR;

  /* Find the current pages of the volume and its descriptor */

  npages = xdisk_get_total_numpages (thread_p, from_volid);
  from_vdes = fileio_get_volume_descriptor (from_volid);

  /* Flush all dirty pages */
  logpb_force (thread_p);

  error_code = pgbuf_flush_all_unfixed (thread_p, from_volid);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (fileio_synchronize (thread_p, from_vdes,
			  fileio_get_volume_label (from_vdes)) != from_vdes)
    {
      return ER_FAILED;
    }

  /* Copy the database volume and initialize recovery information on it */

  to_vdes = fileio_copy_volume (thread_p, from_vdes, npages, to_volname,
				LOG_DBCOPY_VOLID, true);
  if (to_vdes == NULL_VOLDES)
    {
      return ER_FAILED;
    }

  /*
   * Change the name of the volume, its database creation time and its
   * checkpoint lsa
   */

  (void) disk_set_creation (thread_p, LOG_DBCOPY_VOLID, to_volname,
			    db_creation, to_volchkpt_lsa, false,
			    DISK_DONT_FLUSH);

  logpb_force (thread_p);
  (void) pgbuf_flush_all_unfixed_and_set_lsa_as_null (thread_p,
						      LOG_DBCOPY_VOLID);

  /*
   * To set the LSA of temp volume to special temp LSA (-2,-2).
   * Especially, in case of copydb.
   */
  (void) logpb_check_and_reset_temp_lsa (thread_p, LOG_DBCOPY_VOLID);

  (void) pgbuf_invalidate_all (thread_p, LOG_DBCOPY_VOLID);
  fileio_dismount (thread_p, to_vdes);

  return NO_ERROR;
}

/*
 * logpb_copy_database - Copy a database
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   num_perm_vols(in): Number of permanent volume for the database
 *   to_db_fullname(in): Full name of the new database
 *   to_logpath(in): Directory where the log volumes reside
 *   to_prefix_logname(in): Name of the log volumes. It is usually set
 *                      the same as database name. For example, if the value
 *                      is equal to "db", the names of the log volumes created
 *                      are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *   toext_path(in): A path is included if all volumes are placed in one
 *                      place/directory. If NULL is given,
 *                      - If file "fileof_vols_and_wherepaths" is given, the
 *                        path is found in this file.
 *                      - Each volume is copied to same place where the volume
 *                        resides.
 *                      NOTE: This parameter should be NULL, if the above file
 *                            is given.
 *   fileof_vols_and_copypaths(in): A file is given when the user decides to
 *                               control the copy/rename of the volume by
 *                               individual bases. That is, user decides to
 *                               spread the volumes over several locations and
 *                               or to label the volumes with specific names.
 *                               Each volume entry consists of:
 *                                 volid from_fullvolname to_fullvolname
 *
 * NOTE: A new log volume is created for the new database, and each
 *              data volume of the database is copied to the new database.
 *              Recovery information from the old database is not included
 *              onto the copy (i.e., new database). Backups of the old
 *              are not included either. Thus, a backup of the new database is
 *              recommended after the copy is done.
 *
 * NOTE:        This function must be run offline. That is, should not be run
 *              when there are multiusers in the system.
 */
int
logpb_copy_database (THREAD_ENTRY * thread_p, VOLID num_perm_vols,
		     const char *to_db_fullname, const char *to_logpath,
		     const char *to_prefix_logname, const char *toext_path,
		     const char *fileof_vols_and_copypaths)
{
  struct log_rec *eof;		/* End of log record       */
  char from_volname[PATH_MAX];	/* Name of new volume      */
  FILE *fromfile_paths_fp = NULL;	/* Pointer to open file for
					 * location of copy files
					 */
  int fromfile_volid;		/* Volume identifier as an
				 * integer
				 */
  int to_vdes;			/* A volume descriptor     */
  char to_volname[PATH_MAX];	/* Name of "to" volume     */
  LOG_PAGE *to_malloc_log_pgptr = NULL;	/* Log header page "to" log */
  struct log_header *to_hdr;	/* Log header for "to" log */
  FILE *to_volinfo_fp = NULL;	/* Pointer to new volinfo
				 * file
				 */
  char *alloc_extpath = NULL;	/* Copy path for specific
				 * volume
				 */
  const char *ext_name;
  char *ext_path;
  VOLID volid;
  INT64 db_creation;
  int phy_pageid;
  bool stop_eof = false;
  const char *catmsg;
  int error_code;
  char format_string[64];

  db_creation = time (NULL);

  /*
   * Make sure that the length name of the volumes are OK
   */

  error_code = logpb_verify_length (to_db_fullname, to_logpath,
				    to_prefix_logname);
  if (error_code != NO_ERROR)
    {
      /* Names are too long */
      return error_code;
    }

  /*
   * Create the DATABASE VOLUME INFORMATION file
   */
  error_code = logpb_create_volume_info (to_db_fullname);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  /*
   * Create a LOG INFORMATION FILE
   */

  fileio_make_log_info_name (to_volname, to_logpath, to_prefix_logname);
  logpb_create_log_info (to_volname, to_db_fullname);

  catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
			   MSGCAT_SET_LOG, MSGCAT_LOG_LOGINFO_ACTIVE);
  if (catmsg == NULL)
    {
      catmsg = "ACTIVE: %s %d pages\n";
    }
  error_code = log_dump_log_info (to_volname, false, catmsg,
				  to_volname, log_Gl.hdr.npages + 1);
  if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
    {
      goto error;
    }

  fileio_make_backup_volume_info_name (to_volname, to_logpath,
				       to_prefix_logname);
  if (logpb_add_volume (to_db_fullname, LOG_DBLOG_BKUPINFO_VOLID, to_volname,
			DISK_UNKNOWN_PURPOSE) != LOG_DBLOG_BKUPINFO_VOLID)
    {
      error_code = ER_FAILED;
      goto error;
    }


  /*
   * FIRST CREATE A NEW LOG FOR THE NEW DATABASE. This log is not a copy of
   * of the old log; it is a newly created one.
   * Compose the LOG name for the ACTIVE portion of the log.
   * Make sure that nobody else is using this database
   */

  to_malloc_log_pgptr = (LOG_PAGE *) malloc (LOG_PAGESIZE);
  if (to_malloc_log_pgptr == NULL)
    {
      error_code = ER_FAILED;
      goto error;
    }

  fileio_make_log_active_name (to_volname, to_logpath, to_prefix_logname);
  if (logpb_add_volume (to_db_fullname, LOG_DBLOG_ACTIVE_VOLID,
			to_volname,
			DISK_UNKNOWN_PURPOSE) != LOG_DBLOG_ACTIVE_VOLID)
    {
      error_code = ER_FAILED;
      goto error;
    }
  to_vdes =
    fileio_format (thread_p, to_db_fullname, to_volname, LOG_DBCOPY_VOLID,
		   log_Gl.hdr.npages + 1, false, true, false, LOG_PAGESIZE);
  if (to_vdes == NULL_VOLDES)
    {
      error_code = ER_FAILED;
      goto error;
    }

  /*
   * Write an end of log mark at first append page. This is used to detect the
   * end of new log
   */

  phy_pageid = LOGPB_PHYSICAL_HEADER_PAGE_ID + 1;
  to_malloc_log_pgptr->hdr.logical_pageid = 0;
  to_malloc_log_pgptr->hdr.offset = NULL_OFFSET;

  eof = (struct log_rec *) to_malloc_log_pgptr->area;
  eof->trid = LOG_SYSTEM_TRANID + 1;
  LSA_SET_NULL (&eof->prev_tranlsa);
  LSA_SET_NULL (&eof->back_lsa);
  LSA_SET_NULL (&eof->forw_lsa);
  eof->type = LOG_END_OF_LOG;

  log_Gl.hdr.eof_lsa.pageid = to_malloc_log_pgptr->hdr.logical_pageid;
  log_Gl.hdr.eof_lsa.offset = 0;

  /* Record number of writes in statistics */
  mnt_log_iowrites (thread_p);

  if (fileio_write (thread_p, to_vdes, to_malloc_log_pgptr, phy_pageid,
		    LOG_PAGESIZE) == NULL)
    {
      error_code = ER_FAILED;
      fileio_dismount (thread_p, to_vdes);
      goto error;
    }

  /*
   * Initialize the active log header from the old log.
   */

  /*
   * Now, modify the log header. Similar than log_create
   */
  to_malloc_log_pgptr->hdr.logical_pageid = LOGPB_HEADER_PAGE_ID;
  to_malloc_log_pgptr->hdr.offset = NULL_OFFSET;

  to_hdr = (struct log_header *) to_malloc_log_pgptr->area;
  error_code =
    logpb_initialize_header (thread_p, to_hdr, to_prefix_logname,
			     log_Gl.hdr.npages + 1, &db_creation);
  if (error_code != NO_ERROR)
    {
      fileio_dismount (thread_p, to_vdes);
      goto error;
    }

  /* Record number of writes in statistics */
  mnt_log_iowrites (thread_p);

  /* Now write the log header */
  phy_pageid =
    logpb_to_physical_pageid (to_malloc_log_pgptr->hdr.logical_pageid);
  if (fileio_write (thread_p, to_vdes, to_malloc_log_pgptr, phy_pageid,
		    LOG_PAGESIZE) == NULL)
    {
      error_code = ER_FAILED;
      fileio_dismount (thread_p, to_vdes);
      goto error;
    }

  /* Dismount the copy of the log */
  fileio_dismount (thread_p, to_vdes);

  /*
   * Start the COPYING all INFORMATION VOLUMES
   */

  /*
   * Prepare the where path for the volumes according to the input
   */

  error_code = logpb_start_where_path (to_db_fullname, toext_path, &ext_name,
				       &ext_path, &alloc_extpath,
				       fileof_vols_and_copypaths,
				       &fromfile_paths_fp);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  for (volid = LOG_DBFIRST_VOLID; volid < num_perm_vols; volid++)
    {
      error_code =
	logpb_next_where_path (to_db_fullname, toext_path, ext_name, ext_path,
			       fileof_vols_and_copypaths, fromfile_paths_fp,
			       num_perm_vols, volid, from_volname,
			       to_volname);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
      error_code =
	logpb_copy_volume (thread_p, volid, to_volname, &to_hdr->db_creation,
			   &to_hdr->chkpt_lsa);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      /* Write information about this volume in the volume information file */
      if (logpb_add_volume (to_db_fullname, volid, to_volname,
			    DISK_PERMVOL_GENERIC_PURPOSE) != volid)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
    }

  /*
   * We need to change the name of the volumes in our internal tables.
   * That is, first volume points to second volume
   *          second volume points to third volume
   *          and so on..
   *          last volume points to nobody
   */

  fileio_make_volume_info_name (to_volname, to_db_fullname);
  sprintf (format_string, "%%d %%%ds", PATH_MAX - 1);

  to_volinfo_fp = fopen (to_volname, "r");
  if (to_volinfo_fp != NULL)
    {
      volid = NULL_VOLID;
      while (true)
	{
	  if (fscanf (to_volinfo_fp, format_string, &fromfile_volid,
		      to_volname) != 2)
	    {
	      stop_eof = true;
	      fromfile_volid = NULL_VOLID;
	      to_volname[0] = '\0';
	    }
	  else
	    {
	      if ((VOLID) fromfile_volid < LOG_DBFIRST_VOLID)
		{
		  continue;
		}
	    }

	  /*
	   * The previous volume must point to new volume
	   */

	  if (volid != NULL_VOLID)
	    {
	      error_code = disk_set_link (thread_p, LOG_DBCOPY_VOLID,
					  to_volname, false, DISK_FLUSH);
	      if (error_code != NO_ERROR)
		{
		  fileio_dismount (thread_p, to_vdes);
		  goto error;
		}
	      logpb_force (thread_p);
	      error_code =
		pgbuf_flush_all_unfixed_and_set_lsa_as_null (thread_p,
							     LOG_DBCOPY_VOLID);
	      if (error_code != NO_ERROR)
		{
		  fileio_dismount (thread_p, to_vdes);
		  goto error;
		}
	      if (fileio_synchronize (thread_p, to_vdes, to_volname) !=
		  to_vdes)
		{
		  fileio_dismount (thread_p, to_vdes);
		  error_code = ER_FAILED;
		  goto error;
		}
	      (void) pgbuf_invalidate_all (thread_p, LOG_DBCOPY_VOLID);
	      fileio_dismount (thread_p, to_vdes);
	    }

	  if (stop_eof == true)
	    {
	      break;
	    }

	  /*
	   * Now, mount the current volume
	   */
	  volid = (VOLID) fromfile_volid;

	  to_vdes =
	    fileio_mount (thread_p, log_Db_fullname, to_volname,
			  LOG_DBCOPY_VOLID, false, false);
	  if (to_vdes == NULL_VOLDES)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }
	}
      fclose (to_volinfo_fp);
    }

  if (fromfile_paths_fp != NULL)
    {
      fclose (fromfile_paths_fp);
    }

  if (alloc_extpath != NULL)
    {
      free_and_init (alloc_extpath);
    }

  free_and_init (to_malloc_log_pgptr);

  return NO_ERROR;

  /* ******** */
error:

  if (to_malloc_log_pgptr)
    {
      free_and_init (to_malloc_log_pgptr);
    }

  if (fromfile_paths_fp != NULL)
    {
      fclose (fromfile_paths_fp);
    }

  if (alloc_extpath != NULL)
    {
      free_and_init (alloc_extpath);
    }

  /* Destroy the log */

  fileio_make_log_active_name (to_volname, to_logpath, to_prefix_logname);
  fileio_unformat (thread_p, to_volname);
  fileio_make_log_info_name (to_volname, to_logpath, to_prefix_logname);
  fileio_unformat (thread_p, to_volname);

  /*
   * Rewind the volume information to destroy any created volumes if any
   */

  sprintf (format_string, "%%*d %%%ds", PATH_MAX - 1);
  if (to_volinfo_fp != NULL)
    {
      fclose (to_volinfo_fp);
      fileio_make_volume_info_name (to_volname, to_db_fullname);
      if ((to_volinfo_fp = fopen (to_volname, "r")) != NULL)
	{
	  while (true)
	    {
	      if (fscanf (to_volinfo_fp, format_string, to_volname) != 1)
		{
		  break;
		}
	      fileio_unformat (thread_p, to_volname);
	    }
	  fclose (to_volinfo_fp);
	  /* Destroy the volinfo file */
	  fileio_make_volume_info_name (to_volname, to_db_fullname);
	  fileio_unformat (thread_p, to_volname);
	}
    }

  return error_code;
}

/*
 * logpb_rename_all_volumes_files - Rename all volumes/files of the database
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   num_perm_vols(in):
 *   to_db_fullname(in): New full name of the database
 *   to_logpath(in): Directory where the log volumes reside
 *   to_prefix_logname(in): New prefix name for log volumes. It is usually set
 *                      as database name. For example, if the value is equal to
 *                      "db", the names of the log volumes created are as
 *                      follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *   toext_path(in):
 *   fileof_vols_and_renamepaths(in):
 *   extern_rename(in): Rename the volumes/files at OS too.
 *   force_delete(in): Force delete backup volumes and information file
 *
 * NOTE:All volumes/files of the database are renamed according to the
 *              new specifications. This function performs a soft rename, it
 *              will no copy files.
 *
 *              This function must be run offline. That is, it should not be
 *              run when there are multiusers in the system.
 */
int
logpb_rename_all_volumes_files (THREAD_ENTRY * thread_p, VOLID num_perm_vols,
				const char *to_db_fullname,
				const char *to_logpath,
				const char *to_prefix_logname,
				const char *toext_path,
				const char *fileof_vols_and_renamepaths,
				int extern_rename, bool force_delete)
{
  char from_volname[PATH_MAX];	/* Name of new volume      */
  char to_volname[PATH_MAX];	/* Name of "to" volume     */
  char *alloc_extpath = NULL;	/* Copy path for specific
				 * volume
				 */
  FILE *to_volinfo_fp = NULL;	/* Pointer to new volinfo
				 * file
				 */
  const char *ext_name;
  char *ext_path;
  VOLID volid;
  FILE *fromfile_paths_fp = NULL;	/* Pointer to open file for
					 * location of rename files
					 */
  int i;
  const char *catmsg;

  struct stat ext_path_stat;
  struct stat vol_stat;
  char real_pathbuf[PATH_MAX];
  int error_code = NO_ERROR;

  if (toext_path != NULL
      && realpath ((char *) toext_path, real_pathbuf) != NULL)
    {
      toext_path = real_pathbuf;
    }

  /*
   * Make sure that the length name of the volumes are OK
   */

  error_code = logpb_verify_length (to_db_fullname, to_logpath,
				    to_prefix_logname);
  if (error_code != NO_ERROR)
    {
      /* Names are too long */
      return error_code;
    }

  /* toext_path validation check. */
  if (toext_path != NULL)
    {
      if (stat (toext_path, &ext_path_stat))
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_TM_GET_STAT_FAIL, 1, toext_path);
	  error_code = ER_TM_GET_STAT_FAIL;
	  goto error;
	}
      if ((access (toext_path, W_OK) < 0)
	  || !(S_ISDIR (ext_path_stat.st_mode)))
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_TM_IS_NOT_WRITEABLE, 1, toext_path);
	  error_code = ER_TM_IS_NOT_WRITEABLE;
	  goto error;
	}

      error_code =
	logpb_start_where_path (to_db_fullname, toext_path, &ext_name,
				&ext_path, &alloc_extpath,
				fileof_vols_and_renamepaths,
				&fromfile_paths_fp);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      for (volid = LOG_DBFIRST_VOLID; volid < num_perm_vols; volid++)
	{
	  error_code = logpb_next_where_path (to_db_fullname, toext_path,
					      ext_name, ext_path,
					      fileof_vols_and_renamepaths,
					      fromfile_paths_fp,
					      num_perm_vols, volid,
					      from_volname, to_volname);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	  if (stat (from_volname, &vol_stat))
	    {
	      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_TM_GET_STAT_FAIL, 1, from_volname);
	      error_code = ER_TM_GET_STAT_FAIL;
	      goto error;
	    }
	  if ((volid > 0) && (ext_path_stat.st_dev != vol_stat.st_dev))
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_TM_CROSS_DEVICE_LINK, 2, from_volname, toext_path);
	      error_code = ER_TM_CROSS_DEVICE_LINK;
	      goto error;
	    }
	}
    }

  if (log_Gl.archive.vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, log_Gl.archive.vdes);
      log_Gl.archive.vdes = NULL_VOLDES;
    }

  if (PRM_LOG_BACKGROUND_ARCHIVING)
    {
      /* Destroy temporary log archive */
      fileio_unformat (thread_p, log_Name_bg_archive);
      log_Gl.bg_archive_info.vdes = NULL_VOLDES;
    }

  if (force_delete)
    {
      /*
       * REMOVE ONLINE BACKUPS OF PRESENT DATABASE
       * Obtain the name of the backups from the backup volume information file.
       */

      to_volinfo_fp = fopen (log_Name_bkupinfo, "r");
      if (to_volinfo_fp != NULL)
	{

	  if (fileio_read_backup_info_entries (to_volinfo_fp,
					       FILEIO_FIRST_BACKUP_VOL_INFO)
	      == NO_ERROR)
	    {
	      /* Remove any backups that have been created up to now */
	      fileio_remove_all_backup (thread_p, -1);
	      fileio_finalize_backup_info (FILEIO_FIRST_BACKUP_VOL_INFO);
	    }

	  fclose (to_volinfo_fp);

	  /* Destroy the backup volume information */
	  fileio_unformat (thread_p, log_Name_bkupinfo);
	}
    }

  /*
   * REMOVE ANY LOG ARCHIVES from present database
   */

  for (i = log_Gl.hdr.last_deleted_arv_num + 1; i < log_Gl.hdr.nxarv_num; i++)
    {
      fileio_make_log_archive_name (from_volname, log_Archive_path,
				    log_Prefix, i);
      /*
       * Just to avoid the warning, the volume is check first
       */
      if (fileio_is_volume_exist (from_volname) == true)
	{
	  fileio_unformat (thread_p, from_volname);
	}
    }

  /*
   * RENAME LOG ACTIVE
   */

  /*
   * Modify the name in the log header. Similar from log_create
   */

  log_Gl.hdr.nxarv_num = 0;
  log_Gl.hdr.last_arv_num_for_syscrashes = -1;
  log_Gl.hdr.last_deleted_arv_num = -1;
  LSA_SET_NULL (&log_Gl.hdr.bkup_level0_lsa);
  LSA_SET_NULL (&log_Gl.hdr.bkup_level1_lsa);
  LSA_SET_NULL (&log_Gl.hdr.bkup_level2_lsa);
  strcpy (log_Gl.hdr.prefix_name, to_prefix_logname);

  logpb_force (thread_p);
  logpb_flush_header (thread_p);

  if (extern_rename == true)
    {
      logpb_finalize_pool ();
      fileio_dismount (thread_p, log_Gl.append.vdes);

      fileio_make_log_active_name (to_volname, to_logpath, to_prefix_logname);
      if (fileio_rename (LOG_DBLOG_ACTIVE_VOLID, log_Name_active, to_volname)
	  != NULL)
	{
	  log_Gl.append.vdes =
	    fileio_mount (thread_p, to_db_fullname, to_volname,
			  LOG_DBLOG_ACTIVE_VOLID, true, false);
	}
      else
	{
	  log_Gl.append.vdes =
	    fileio_mount (thread_p, log_Db_fullname, log_Name_active,
			  LOG_DBLOG_ACTIVE_VOLID, true, false);
	  error_code = ER_FAILED;
	  goto error;
	}

      /* Get the append page */
      error_code = logpb_initialize_pool (thread_p);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
      if (logpb_fetch_start_append_page (thread_p) == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
    }

  /*
   * Create the DATABASE VOLUME INFORMATION file
   */

  /*
   * Destroy the old VOLUME INFORMATION AND LOG INFORMATION. Then, create
   * them back.
   */
  fileio_unformat (thread_p, log_Name_volinfo);
  fileio_unformat (thread_p, log_Name_info);

  error_code = logpb_create_volume_info (to_db_fullname);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  fileio_make_log_info_name (to_volname, to_logpath, to_prefix_logname);
  logpb_create_log_info (to_volname, to_db_fullname);

  catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
			   MSGCAT_SET_LOG,
			   MSGCAT_LOG_LOGINFO_COMMENT_FROM_RENAMED);
  if (catmsg == NULL)
    {
      catmsg = "COMMENT: from renamed database = %s\n";
    }
  error_code = log_dump_log_info (to_volname, false, catmsg, log_Db_fullname);
  if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
    {
      goto error;
    }

  catmsg = msgcat_message (MSGCAT_CATALOG_CUBRID,
			   MSGCAT_SET_LOG, MSGCAT_LOG_LOGINFO_ACTIVE);
  if (catmsg == NULL)
    {
      catmsg = "ACTIVE: %s %d pages\n";
    }
  error_code = log_dump_log_info (to_volname, false, catmsg,
				  to_volname, log_Gl.hdr.npages + 1);
  if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
    {
      goto error;
    }

  /*
   * Add the backup information and the log active to the volume
   * information
   */
  fileio_make_backup_volume_info_name (to_volname, to_logpath,
				       to_prefix_logname);
  if (logpb_add_volume (to_db_fullname, LOG_DBLOG_BKUPINFO_VOLID,
			to_volname,
			DISK_UNKNOWN_PURPOSE) != LOG_DBLOG_BKUPINFO_VOLID)
    {
      error_code = ER_FAILED;
      goto error;
    }

  fileio_make_log_active_name (to_volname, to_logpath, to_prefix_logname);
  if (logpb_add_volume (to_db_fullname, LOG_DBLOG_ACTIVE_VOLID,
			to_volname, DISK_UNKNOWN_PURPOSE)
      != LOG_DBLOG_ACTIVE_VOLID)
    {
      error_code = ER_FAILED;
      goto error;
    }

  /*
   * Start the RENAMING all DATA VOLUMES
   */

  /*
   * Prepare the where path for the volumes according to the input
   */

  error_code = logpb_start_where_path (to_db_fullname, toext_path, &ext_name,
				       &ext_path, &alloc_extpath,
				       fileof_vols_and_renamepaths,
				       &fromfile_paths_fp);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  for (volid = LOG_DBFIRST_VOLID; volid < num_perm_vols; volid++)
    {
      /* Change the name of the volume */
      error_code =
	logpb_next_where_path (to_db_fullname, toext_path, ext_name, ext_path,
			       fileof_vols_and_renamepaths, fromfile_paths_fp,
			       num_perm_vols, volid, from_volname,
			       to_volname);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      error_code = disk_set_creation (thread_p, volid, to_volname,
				      &log_Gl.hdr.db_creation,
				      &log_Gl.hdr.chkpt_lsa, true,
				      DISK_DONT_FLUSH);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      /*
       * We need to change the name of the volumes in our internal tables.
       * That is, first volume points to second volume
       *          second volume points to third volume
       *          and so on..
       *          last volume points to nobody
       */

      if (volid != LOG_DBFIRST_VOLID)
	{
	  error_code = disk_set_link (thread_p, volid - 1, to_volname, false,
				      DISK_FLUSH);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}

      /*
       * Now flush every single page of this volume, dismount the volume, rename
       * the volume, and mount the volume
       */
      logpb_force (thread_p);
      error_code = pgbuf_flush_all (thread_p, volid);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
      if (fileio_synchronize (thread_p,
			      fileio_get_volume_descriptor (volid),
			      fileio_get_volume_label (volid)) == NULL_VOLDES)
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      (void) pgbuf_invalidate_all (thread_p, volid);

      if (extern_rename == true)
	{
	  fileio_dismount (thread_p, fileio_get_volume_descriptor (volid));
	  if (fileio_rename (volid, from_volname, to_volname) != NULL)
	    {
	      (void) fileio_mount (thread_p, to_db_fullname, to_volname,
				   volid, false, false);
	    }
	  else
	    {
	      (void) fileio_mount (thread_p, log_Db_fullname, from_volname,
				   volid, false, false);
	      error_code = ER_FAILED;
	      goto error;
	    }
	}

      /* Write information about this volume in the volume information file */
      if (logpb_add_volume (to_db_fullname, volid, to_volname,
			    DISK_PERMVOL_GENERIC_PURPOSE) != volid)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
    }

  if (fromfile_paths_fp != NULL)
    {
      fclose (fromfile_paths_fp);
    }

  if (alloc_extpath != NULL)
    {
      free_and_init (alloc_extpath);
    }

  /* Indicate the new names */
  error_code =
    logpb_initialize_log_names (thread_p, to_db_fullname, to_logpath,
				to_prefix_logname);
  return error_code;

  /* ******* */
error:
  /* May need to rename some volumes back */

  if (to_volinfo_fp != NULL)
    {
      fclose (to_volinfo_fp);
    }

  if (fromfile_paths_fp != NULL)
    {
      fclose (fromfile_paths_fp);
    }

  if (alloc_extpath != NULL)
    {
      free_and_init (alloc_extpath);
    }

  /* May need to rename back whatever was renamed */

  return error_code;
}

/*
 * logpb_delete - Delete all log files and database backups
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   num_perm_vols(in):
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in): Name of the log volumes. It is usually set as database
 *                      name. For example, if the value is equal to "db", the
 *                      names of the log volumes created are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *   force_delete(in):
 *
 * NOTE:All log volumes (active, archives) and database backups that
 *              are accessible (i.e., located on disk) are removed from the
 *              system. This is a very dangerous operation since the database
 *              cannot be recovered after this operation is done. It is
 *              recommended to backup the database and put the backup on tape
 *              or outside the log and backup directories before this
 *              operation is done.
 *
 *              This function must be run offline. That is, it should not be
 *              run when there are multiusers in the system.
 */
int
logpb_delete (THREAD_ENTRY * thread_p, VOLID num_perm_vols,
	      const char *db_fullname, const char *logpath,
	      const char *prefix_logname, bool force_delete)
{
  const char *vlabel;		/* Name of volume     */
  char vol_fullname[PATH_MAX];	/* Name of volume     */
  struct log_header disk_hdr;	/* Log header area    */
  struct log_header *loghdr;	/* Log header pointer */
  VOLID volid;
  FILE *db_volinfo_fp = NULL;
  int read_int_volid;
  int i;
  int error_code = NO_ERROR;
  char format_string[64];

  /*
   * FIRST: Destroy data volumes of the database.
   * That is, the log, and information files are not removed at this point.
   */

  /* If the system is not restarted, read the header directly from disk */
  if (num_perm_vols < 0 || log_Gl.trantable.area == NULL
      || log_Pb.pool == NULL)
    {
      /*
       * The system is not restarted. Read the log header from disk and remove
       * the data volumes by reading the database volume information
       */

      er_clear ();
      error_code =
	logpb_initialize_log_names (thread_p, db_fullname, logpath,
				    prefix_logname);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}

      if (fileio_is_volume_exist (log_Name_active) == false
	  || (log_Gl.append.vdes =
	      fileio_mount (thread_p, db_fullname, log_Name_active,
			    LOG_DBLOG_ACTIVE_VOLID, true,
			    false)) == NULL_VOLDES)
	{
	  /* Unable to mount the active log */
	  if (er_errid () == ER_IO_MOUNT_LOCKED)
	    {
	      return ER_IO_MOUNT_LOCKED;
	    }
	  else
	    {
	      loghdr = NULL;
	    }
	}
      else
	{
	  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT],
	    *aligned_log_pgbuf;
	  LOG_PAGE *log_pgptr;

	  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);
	  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

	  /* Initialize the buffer pool, so we can read the header */
	  if (log_Pb.pool == NULL)
	    {
	      error_code = logpb_initialize_pool (thread_p);
	      if (error_code != NO_ERROR)
		{
		  return error_code;
		}
	    }
	  logpb_fetch_header_with_buffer (thread_p, &disk_hdr, log_pgptr);
	  logpb_finalize_pool ();
	  fileio_dismount (thread_p, log_Gl.append.vdes);
	  log_Gl.append.vdes = NULL_VOLDES;
	  loghdr = &disk_hdr;
	  /*
	   * Make sure that the log is a log file and that it is compatible
	   * with the running database and system
	   */
	  if (loghdr->db_compatibility != rel_disk_compatible ())
	    {
	      loghdr = NULL;
	    }
	  else if (loghdr->db_iopagesize != IO_PAGESIZE
		   || loghdr->db_logpagesize != LOG_PAGESIZE)
	    {
	      /* Pagesize is incorrect,...reset it and call again... */
	      if (db_set_page_size (loghdr->db_iopagesize,
				    loghdr->db_logpagesize) != NO_ERROR)
		{
		  loghdr = NULL;
		}
	      else
		{
		  error_code =
		    logtb_define_trantable_log_latch (thread_p, -1);
		  if (error_code != NO_ERROR)
		    {
		      return error_code;
		    }
		  error_code =
		    logpb_delete (thread_p, num_perm_vols, db_fullname,
				  logpath, prefix_logname, force_delete);
		  return error_code;
		}
	    }
	}

      /*
       * DESTROY DATA VOLUMES using the database volume information since
       * the database system is not restarted.
       *
       * NOTE: only data volumes are removed, logs, and information files
       *       are not removed at this point.
       */

      fileio_make_volume_info_name (vol_fullname, db_fullname);
      sprintf (format_string, "%%d %%%ds", PATH_MAX - 1);

      db_volinfo_fp = fopen (vol_fullname, "r");
      if (db_volinfo_fp != NULL)
	{
	  while (true)
	    {
	      if (fscanf (db_volinfo_fp, format_string, &read_int_volid,
			  vol_fullname) != 2)
		{
		  break;
		}

	      volid = (VOLID) read_int_volid;
	      /*
	       * Remove data volumes at this point
	       */
	      switch (volid)
		{
		case LOG_DBVOLINFO_VOLID:
		case LOG_DBLOG_INFO_VOLID:
		case LOG_DBLOG_BKUPINFO_VOLID:
		case LOG_DBLOG_ACTIVE_VOLID:
		  continue;
		default:
		  fileio_unformat (thread_p, vol_fullname);
		}
	    }

	  fclose (db_volinfo_fp);
	}
      else
	{
	  /* Destory at least the database main volume */
	  fileio_unformat (thread_p, db_fullname);
	}
    }
  else
    {
      loghdr = &log_Gl.hdr;
      /*
       * DESTROY DATA VOLUMES
       */
      for (volid = LOG_DBFIRST_VOLID; volid < num_perm_vols; volid++)
	{
	  vlabel = fileio_get_volume_label (volid);
	  if (vlabel != NULL)
	    {
	      (void) pgbuf_invalidate_all (thread_p, volid);
	      /*
	       * We need to copy the name since it is undefined once the volume is
	       * dismounted
	       */
	      strcpy (vol_fullname, vlabel);
	      fileio_dismount (thread_p,
			       fileio_get_volume_descriptor (volid));
	      fileio_unformat (thread_p, vol_fullname);
	    }
	}
    }

  /* Destroy the database volume information */
  fileio_make_volume_info_name (vol_fullname, db_fullname);
  fileio_unformat (thread_p, vol_fullname);

  if (force_delete)
    {
      /*
       * SECOND: Destroy backups of data volumes, and the backup log information
       *         The backups are located by reading the backup info log.
       */

      db_volinfo_fp = fopen (log_Name_bkupinfo, "r");
      if (db_volinfo_fp != NULL)
	{

	  if (fileio_read_backup_info_entries (db_volinfo_fp,
					       FILEIO_FIRST_BACKUP_VOL_INFO)
	      == NO_ERROR)
	    {
	      /* Remove any backups that have been created up to now */
	      fileio_remove_all_backup (thread_p, -1);
	      fileio_finalize_backup_info (FILEIO_FIRST_BACKUP_VOL_INFO);
	    }

	  fclose (db_volinfo_fp);

	  /* Now, destroy the backup volume information */
	  fileio_unformat (thread_p, log_Name_bkupinfo);
	}
    }

  /*
   * THIRD: Destroy log active, online log archives, and log information
   */

  /* If there is any archive current mounted, dismount the archive */
  if (log_Gl.trantable.area != NULL && log_Gl.append.log_pgptr != NULL
      && log_Gl.archive.vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, log_Gl.archive.vdes);
      log_Gl.archive.vdes = NULL_VOLDES;
    }

  /* Destroy online log archives */
  if (loghdr != NULL)
    {
      for (i = loghdr->last_deleted_arv_num + 1; i < loghdr->nxarv_num; i++)
	{
	  fileio_make_log_archive_name (vol_fullname, log_Archive_path,
					log_Prefix, i);
	  fileio_unformat (thread_p, vol_fullname);
	}
    }

  if (PRM_LOG_BACKGROUND_ARCHIVING)
    {
      /* Destroy temporary log archive */
      fileio_unformat (thread_p, log_Name_bg_archive);
      log_Gl.bg_archive_info.vdes = NULL_VOLDES;
    }

  /* Now undefine all pages */
  if (log_Gl.trantable.area != NULL && log_Gl.append.log_pgptr != NULL)
    {
      logpb_finalize_pool ();
      (void) pgbuf_invalidate_all (thread_p, NULL_VOLID);
      logtb_undefine_trantable (thread_p);
      if (log_Gl.append.vdes != NULL_VOLDES)
	{
	  fileio_dismount (thread_p, log_Gl.append.vdes);
	  log_Gl.append.vdes = NULL_VOLDES;
	}
      log_Gl.archive.vdes = NULL_VOLDES;
    }

  fileio_unformat (thread_p, log_Name_active);
  fileio_unformat (thread_p, log_Name_info);

  return NO_ERROR;
}

/*
 * logpb_check_if_exists -
 *
 * return:
 *
 *   fname(in):
 *   first_vol(in):
 *
 * NOTE:
 */
static bool
logpb_check_if_exists (const char *fname, char *first_vol)
{
  struct stat stat_buf;

  if (stat (fname, &stat_buf) != 0)
    {
      return false;		/* not exist */
    }
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_VOLUME_EXISTS, 1, fname);
  if (first_vol[0] == 0)
    {
      strcpy (first_vol, fname);
    }
  return true;
}

/*
 * logpb_check_exist_any_volumes - check existence of DB files
 *
 * return: NO_ERROR or error code
 *
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in): Name of the log volumes.
 *   first_vol(in):
 *
 * NOTE: All log volumes (active, archives) and database backups that
 *              are accessible (i.e., located on disk) are checked
 */
int
logpb_check_exist_any_volumes (THREAD_ENTRY * thread_p,
			       const char *db_fullname, const char *logpath,
			       const char *prefix_logname, char *first_vol,
			       bool * is_exist)
{
  int exist_cnt;
  int error_code = NO_ERROR;

  exist_cnt = 0;
  first_vol[0] = 0;

  *is_exist = false;

  error_code =
    logpb_initialize_log_names (thread_p, db_fullname, logpath,
				prefix_logname);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  exist_cnt += logpb_check_if_exists (db_fullname, first_vol) ? 1 : 0;
  exist_cnt += logpb_check_if_exists (log_Name_active, first_vol) ? 1 : 0;
  exist_cnt += logpb_check_if_exists (log_Name_info, first_vol) ? 1 : 0;
  exist_cnt += logpb_check_if_exists (log_Name_volinfo, first_vol) ? 1 : 0;

  if (exist_cnt > 0)
    {
      *is_exist = true;
    }
  else
    {
      *is_exist = false;
    }

  return error_code;
}

/*
 *
 *       		       LOG FATAL ERRORS
 *
 */

/*
 * logpb_fatal_error - Log error
 *
 * return: nothing
 *
 *   logexit(in):
 *   file_name(in):
 *   lineno(in):
 *   fmt(in):
 *   va_alist(in): Variable number of arguments (just like fprintf)
 *
 * NOTE: An error was found during logging. A short error message is
 *              produced on the stderr describing the error. Currently, the
 *              database is exited.
 */
void
logpb_fatal_error (THREAD_ENTRY * thread_p, bool logexit,
		   const char *file_name, const int lineno,
		   const char *fmt, ...)
{
  const char *msglog;

  /*
   * Flush any unfixed, dirty pages before the system exits. This is done
   * to make sure that all committed actions are reflected on disk.
   * Unfortunately, we may be placing some uncommitted action od disk. This
   * will be fixed by our recovery process. Note if the user runs the pathdb,
   * utility after this, the uncommitted actions will be considered as
   * committed.
   */

  if (logexit != false && log_Gl.append.log_pgptr != NULL)
    {
      /* Flush up to the smaller of the previous LSA record or the
       * previous flushed append page.
       */
      LOG_LSA tmp_lsa1, tmp_lsa2;
      static int infatal = false;

      if (infatal == false)
	{
	  infatal = true;

	  if (log_Gl.append.prev_lsa.pageid < log_Gl.append.nxio_lsa.pageid)
	    {
	      LSA_COPY (&tmp_lsa1, &log_Gl.append.prev_lsa);
	    }
	  else
	    {
	      /* TODO : valid code ?? */
	      /*
	         if ((tmp_lsa1.pageid = log_Gl.append.nxio_lsa.pageid - 1) < 0)
	         tmp_lsa1.pageid = 0;
	       */
	      tmp_lsa1.pageid = 0;
	    }

	  /*
	   * Flush as much as you can without forcing the current unfinish log
	   * record.
	   */
	  pgbuf_flush_check_point (thread_p, &tmp_lsa1, &tmp_lsa2);
	  infatal = false;
	}
    }

  (void) fileio_synchronize_all (thread_p, false);

  (void) fflush (stderr);
  (void) fflush (stdout);

#if defined(CUBRID_DEBUG)
  {
    va_list ap;			/* Point to each unnamed arg in turn */

    (void) fprintf (stderr,
		    "\n--->>>\n*** LOG FATAL ERROR *** file %s - line %d\n",
		    file_name, lineno);
    /* Print out remainder of message */
    va_start (ap, fmt);
    (void) vfprintf (stderr, fmt, ap);
    (void) fprintf (stderr, "\n");
    va_end (ap);
  }
#else /* CUBRID_DEBUG */
  (void) fprintf (stderr, "\n--->>>\n*** FATAL ERROR *** \n");
#endif /* CUBRID_DEBUG */

  (void) fprintf (stderr, "%s\n", er_msg ());

  /*
   * If error message log is different from terminal or /dev/null..indicate
   * that additional information can be found in the error log file
   */
  if ((msglog = er_msglog_filename ()) != NULL
      && strcmp (msglog, "/dev/null") != 0)
    {
      (void) fprintf (stderr,
		      "Please consult error_log file = %s for additional"
		      " information\n", msglog);
    }

  (void) fflush (stderr);
  (void) fflush (stdout);

  if (logexit != false)
    {
      (void) fprintf (stderr, "... ABORT/EXIT IMMEDIATELY ...<<<---\n");

#if defined(SERVER_MODE)
      boot_donot_shutdown_server_at_exit ();
      boot_server_status (BOOT_SERVER_DOWN);
#else /* SERVER_MODE */
      /*
       * The following crap is added to the standalone version to avoid the
       * client to continue accessing the database system in presence of
       * call on exit functions of the applications.
       */
      boot_donot_shutdown_client_at_exit ();
      tran_cache_tran_settings (NULL_TRAN_INDEX, -1, TRAN_DEFAULT_ISOLATION);
      db_Connect_status = DB_CONNECTION_STATUS_NOT_CONNECTED;
#endif /* SERVER_MODE */

#if defined(NDEBUG)
      exit (EXIT_FAILURE);
#else /* NDEBUG */
      /* debugging purpose */
      abort ();
#endif /* NDEBUG */
    }
}

#if defined(SERVER_MODE)
/*
 * logpb_backup_needed_archive_logs - Backup active log archives
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   session(in): The session array which is set as a side effect.
 *   first_arv_num(in): last arv num to archive (inclusive)
 *   last_arv_num(in): last arv num to archive (inclusive)
 *
 * NOTE: Determine which active log records will be required to fully restore
 *   this backup in the event recovery is needed.  This probably includes
 *   the active log archives as well as at least some portion of the
 *   log active.  Force a log archive of the active log, to insure that we
 *   have the necessary log records to restore if this backup is "fuzzy".
 */
static int
logpb_backup_needed_archive_logs (THREAD_ENTRY * thread_p,
				  FILEIO_BACKUP_SESSION * session,
				  int first_arv_num, int last_arv_num)
{
  int i;
  char logarv_name[PATH_MAX];	/* Archive name               */
  int error_code = NO_ERROR;

  for (i = first_arv_num; i >= 0 && i <= last_arv_num; i++)
    {
      /* Backup this archive volume */
      fileio_make_log_archive_name (logarv_name, log_Archive_path, log_Prefix,
				    i);

      error_code = fileio_backup_volume (thread_p, session, logarv_name,
					 LOG_DBLOG_ARCHIVE_VOLID, -1, false);
      if (error_code != NO_ERROR)
	{
	  break;
	}
    }

  return error_code;
}
#endif /* SERVER_MODE */

/*
 * logpb_remote_ask_user_before_delete_volumes - Remote prompt before arv deletion
 *
 * return: true if ok to proceed, false if user declines
 *
 *   volpath(in): the pathname to location where deletion will occur
 *
 * NOTE:Ask the user if it is ok to proceed with a destructive operation, namely
 *   deleting one or more prior backups.
 */
static bool
logpb_remote_ask_user_before_delete_volumes (THREAD_ENTRY * thread_p,
					     const char *volpath)
{
  char *ptr1 = NULL, *ptr2 = NULL, *ptr3 = NULL;
  char *fullmsg = NULL;
  char user_response[FILEIO_MAX_USER_RESPONSE_SIZE];
  bool r;

  if (asprintf (&ptr1, msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOG,
				       MSGCAT_LOG_STARTS)) < 0
      || asprintf (&ptr2, msgcat_message (MSGCAT_CATALOG_CUBRID,
					  MSGCAT_SET_LOG,
					  MSGCAT_LOG_DELETE_BKVOLS),
		   volpath) < 0
      || asprintf (&ptr3,
		   msgcat_message (MSGCAT_CATALOG_CUBRID, MSGCAT_SET_LOG,
				   MSGCAT_LOG_ENTER_Y2_CONFIRM)) < 0
      || asprintf (&fullmsg, "%s%s%s%s", ptr1, ptr2, ptr3, ptr1) < 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      r = false;
      goto end;
    }

  if (fileio_request_user_response (thread_p, FILEIO_PROMPT_BOOLEAN_TYPE,
				    fullmsg, user_response,
				    NULL, -1, -1, NULL, -1) != NO_ERROR)
    {
      r = false;
      goto end;
    }

  /* process the return */
  r = (user_response[0] == '1');

end:
  if (ptr1 != NULL)
    {
      free (ptr1);
    }
  if (ptr2 != NULL)
    {
      free (ptr2);
    }
  if (ptr3 != NULL)
    {
      free (ptr3);
    }
  if (fullmsg != NULL)
    {
      free (fullmsg);
    }

  return r;
}

/*
 * logpb_must_archive_last_log_page - Log special record to arv current page
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 * NOTE: When archiving the last log page, as is necessary during backups, must
 *   first append a new record on the last page that has no forward pointer.
 *   This insures that recovery will have to search explicitly for the next
 *   record forward when it is needed, otherwise we might find a "false" end
 *   and do an incomplete recovery by mistake.
 */
static int
logpb_must_archive_last_log_page (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;
  int tran_index;

  assert (LOG_CS_OWN (thread_p));

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes == NULL)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_UNKNOWN_TRANINDEX, 1, tran_index);
      return ER_LOG_UNKNOWN_TRANINDEX;
    }

  log_append_dummy_record (thread_p, tdes, LOG_DUMMY_FILLPAGE_FORARCHIVE);
  logpb_flush_all_append_pages (thread_p, LOG_FLUSH_DIRECT);

  return NO_ERROR;
}

/*
 * logpb_check_and_reset_temp_lsa -
 *
 * return:
 *
 *   volid(in):
 *
 * NOTE:
 */
int
logpb_check_and_reset_temp_lsa (THREAD_ENTRY * thread_p, VOLID volid)
{
  VPID vpid;
  PAGE_PTR pgptr;

  vpid.volid = volid;
  vpid.pageid = 0;
  pgptr = pgbuf_fix (thread_p, &vpid, OLD_PAGE, PGBUF_LATCH_WRITE,
		     PGBUF_UNCONDITIONAL_LATCH);
  if (pgptr == NULL)
    {
      return ER_FAILED;
    }

  if (xdisk_get_purpose (thread_p, volid) == DISK_PERMVOL_TEMP_PURPOSE)
    {
      pgbuf_reset_temp_lsa (pgptr);
      pgbuf_set_dirty (thread_p, pgptr, FREE);
    }
  else
    {
      pgbuf_unfix (thread_p, pgptr);
    }

  return NO_ERROR;
}

/*
 * logpb_initialize_flush_info - initialize flush information
 *
 * return: nothing
 *
 * NOTE:
 */
static int
logpb_initialize_flush_info (void)
{
  int error = NO_ERROR;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  if (flush_info->toflush != NULL)
    {
      logpb_finalize_flush_info ();
    }
  assert (flush_info->toflush == NULL);

  flush_info->max_toflush = log_Pb.num_buffers - 1;
  flush_info->num_toflush = 0;
  flush_info->toflush = (LOG_PAGE **) calloc (log_Pb.num_buffers,
					      sizeof (flush_info->toflush));
  if (flush_info->toflush == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      log_Pb.num_buffers * sizeof (flush_info->toflush));
      error = ER_OUT_OF_VIRTUAL_MEMORY;
    }

  MUTEX_INIT (flush_info->flush_mutex);

  return error;
}

/*
 * logpb_finalize_flush_info - Destroy flush information
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_finalize_flush_info (void)
{
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  LOG_MUTEX_LOCK (rv, flush_info->flush_mutex);

  if (flush_info->toflush != NULL)
    {
      free_and_init (flush_info->toflush);
    }

  flush_info->max_toflush = 0;
  flush_info->num_toflush = 0;

  LOG_MUTEX_UNLOCK (flush_info->flush_mutex);
  MUTEX_DESTROY (flush_info->flush_mutex);
}

/*
 * logpb_finalize_writer_info - Destroy writer information
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_finalize_writer_info (void)
{
#if defined (SERVER_MODE)
  int rv;
#endif
  LOGWR_ENTRY *entry, *next_entry;
  LOGWR_INFO *writer_info = &log_Gl.writer_info;

  LOG_MUTEX_LOCK (rv, writer_info->wr_list_mutex);
  entry = writer_info->writer_list;
  while (entry)
    {
      next_entry = entry->next;
      free (entry);
      entry = next_entry;
    }
  writer_info->writer_list = NULL;
  LOG_MUTEX_UNLOCK (writer_info->wr_list_mutex);

  MUTEX_DESTROY (writer_info->wr_list_mutex);

  MUTEX_DESTROY (writer_info->flush_start_mutex);
  COND_DESTROY (writer_info->flush_start_cond);

  MUTEX_DESTROY (writer_info->flush_end_mutex);
  COND_DESTROY (writer_info->flush_end_cond);
}

/*
 * logpb_initialize_logging_statistics - Initialize logging statistics
 *
 * return: nothing
 *
 * NOTE:
 */
void
logpb_initialize_logging_statistics (void)
{
  memset (&log_Stat, 0, sizeof (LOG_LOGGING_STAT));
}

/*
 * logpb_flush_pages_background - Flush log pages background
 *
 * return: flushed page count
 *
 * NOTE:Flushes pages except first, current and delayed log pages.
 *           Flushing pages in this function and appending log
 *           in logpb_next_append_page are executed concurrently,
 *           because there is no log cs acquired while I/O in this function.
 *           There are 2 threads which can access the buffer which is flushing.
 *           1. log_pbreplace (logpb_next_append_page calls it)
 *           Because flush_running = true, can't replace the buffer.
 *           2. logpb_flush_all_append_pages
 *           Because of flush_run_mutex, can't flush the same buffer.
 *           Now, only one thread can exist flush routine, fileio_writev
 *           Log_writev_append_pages() can be called by 2 threads concurrently.
 *           There is no problem because it calls pwrite() instead of write().
 *
 *           There are two type flushing.
 *           - flushing by LFT(log_flush_pages())
 *           - flushing by normal threads(logpb_flush_all_append_pages)
 *           They must not be executed concurrently because
 *           2 threads can flushes the same page. It's why run mutex exists.
 *           If 2 flushing threads have to be executed concurrently
 *           for increasing performance, mutex for each log buffer is needed.
 */
void
logpb_flush_pages_background (THREAD_ENTRY * thread_p)
{
  int loop;
  struct log_buffer *bufptr;
  bool find_first = false;
  int flush_count = 0;
  int flush_expected;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  assert (LOG_CS_OWN (thread_p));

  assert (flush_info->toflush != NULL);

  LOG_MUTEX_LOCK (rv, flush_info->flush_mutex);
  if (flush_info->num_toflush <= 2)
    {
      LOG_MUTEX_UNLOCK (flush_info->flush_mutex);
      return;
    }

  /* num_toflush can be changed because flush lock is released in loop */

  flush_expected = PRM_LOG_BG_FLUSH_NUM_PAGES;
  if (flush_expected == 0)
    {
      flush_expected = flush_info->max_toflush;
    }

  assert (flush_info->num_toflush > 0);

  LOG_MUTEX_LOCK (rv, log_Pb.lpb_mutex);

  find_first = false;
  for (loop = 0; loop != flush_info->num_toflush; ++loop)
    {
      LOG_CAST_LOGPAGEPTR_TO_BFPTR (bufptr, flush_info->toflush[loop]);

      if (!bufptr->dirty || bufptr->fcnt > 0)
	{
	  continue;
	}

      if (!find_first)
	{
	  find_first = true;
	  continue;
	}

      if (flush_count >= flush_expected)
	{
	  break;
	}

      assert (bufptr->dirty && bufptr->fcnt == 0);
      assert (!bufptr->flush_running);

      /* loop = log_Gl.flush_info.num_toflush - 1 is possible.
       * Last log page is not fixed between freeing current log page and
       * allocation of new page
       * And LFT can flushes it because delayed page always exists.
       */

      bufptr->flush_running = true;
      if (logpb_writev_append_pages (thread_p, &flush_info->toflush[loop], 1)
	  == NULL)
	{
	  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
	  LOG_MUTEX_UNLOCK (flush_info->flush_mutex);
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_flush_pages_bg");
	  return;
	}

      bufptr->flush_running = false;
      bufptr->dirty = false;

      ++flush_count;

      assert (flush_info->toflush[loop] != NULL);
    }

  /* dual writing (Background archiving) */
  if (PRM_LOG_BACKGROUND_ARCHIVING)
    {
      logpb_write_toflush_pages_to_archive (thread_p);
    }

  LOG_MUTEX_UNLOCK (log_Pb.lpb_mutex);
  assert (flush_count < flush_info->num_toflush - 1);

  /* while I/O, to append log can be executed concurrently. */
  LOG_MUTEX_UNLOCK (flush_info->flush_mutex);

  if (flush_count > 0)
    {
      log_Stat.total_sync_count++;
      log_Stat.flush_pages_call_count++;
      log_Stat.last_flush_count_by_LFT = flush_count;
      log_Stat.total_flush_count_by_LFT += flush_count;

      if (PRM_SUPPRESS_FSYNC == 0
	  || (log_Stat.total_sync_count % PRM_SUPPRESS_FSYNC == 0))
	{
	  if (fileio_synchronize (thread_p,
				  log_Gl.append.vdes,
				  log_Name_active) == NULL_VOLDES)
	    {
	      return;
	    }
	}
    }
  else
    {
      log_Stat.flush_pages_call_miss_count++;
    }

  assert (flush_count <= flush_expected);
  return;
}

/*
 * logpb_background_archiving -
 *
 * return:
 *
 * NOTE: this function is called by log_initialize_internal only
 *       (in server startup time)
 */
int
logpb_background_archiving (THREAD_ENTRY * thread_p)
{
  char log_pgbuf[IO_MAX_PAGE_SIZE * LOGPB_IO_NPAGES + MAX_ALIGNMENT];
  char *aligned_log_pgbuf;
  LOG_PAGE *log_pgptr;
  int page_id, phy_pageid, last_page_id;
  int num_pages = 0;
  int vdes;
  int error_code = NO_ERROR;
  BACKGROUND_ARCHIVING_INFO *bg_arv_info;

  assert (PRM_LOG_BACKGROUND_ARCHIVING);

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);
  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

  bg_arv_info = &log_Gl.bg_archive_info;
  vdes = bg_arv_info->vdes;
  if (vdes == NULL_VOLDES)
    {
      return NO_ERROR;
    }

  last_page_id = log_Gl.hdr.chkpt_lsa.pageid - 1;
  page_id = bg_arv_info->current_page_id;
  phy_pageid = page_id - bg_arv_info->start_page_id + 1;

  /* Now start dumping the current active pages to archive */
  for (; page_id <= last_page_id;
       page_id += num_pages, phy_pageid += num_pages)
    {
      num_pages = MIN (LOGPB_IO_NPAGES, last_page_id - page_id + 1);

      num_pages = logpb_read_page_from_active_log (thread_p, page_id,
						   num_pages, log_pgptr);
      if (num_pages <= 0)
	{
	  error_code = er_errid ();
	  goto error;
	}

      /* Record number of writes in statistics */
      mnt_log_iowrites (thread_p);
      if (fileio_write_pages (thread_p, vdes, (char *) log_pgptr, phy_pageid,
			      num_pages, LOG_PAGESIZE) == NULL)
	{
	  error_code = ER_LOG_WRITE;
	  goto error;
	}

      bg_arv_info->current_page_id = page_id + num_pages;
    }

error:
  if (error_code == ER_LOG_WRITE || error_code == ER_LOG_READ)
    {
      fileio_dismount (thread_p, bg_arv_info->vdes);
      bg_arv_info->vdes = NULL_VOLDES;
      bg_arv_info->start_page_id = NULL_PAGEID;
      bg_arv_info->current_page_id = NULL_PAGEID;

      er_log_debug (ARG_FILE_LINE,
		    "background archiving error, hdr->start_page_id = %d, "
		    "hdr->current_page_id = %d, error:%d\n",
		    bg_arv_info->start_page_id,
		    bg_arv_info->current_page_id, error_code);
    }

  er_log_debug (ARG_FILE_LINE,
		"logpb_background_archiving end, hdr->start_page_id = %d, "
		"hdr->current_page_id = %d\n",
		bg_arv_info->start_page_id, bg_arv_info->current_page_id);

  return error_code;
}

/*
 * logpb_dump_log_header - dump log header
 *
 * return: Nothing
 *
 *   outfp(in):  file descriptor
 *
 * NOTE:
 */
static void
logpb_dump_log_header (FILE * outfp)
{
  fprintf (outfp, "Log Header:\n");

  fprintf (outfp, "\tfirst log page id : %d\n", log_Gl.hdr.fpageid);

  fprintf (outfp, "\tcurrent log append lsa : (%d, %d)\n",
	   log_Gl.hdr.append_lsa.pageid, log_Gl.hdr.append_lsa.offset);

  fprintf (outfp, "\tlast log append lsa : (%d, %d)\n",
	   log_Gl.append.prev_lsa.pageid, log_Gl.append.prev_lsa.offset);

  fprintf (outfp,
	   "\tlowest lsa which hasn't been written to disk : (%d, %d)\n",
	   log_Gl.append.nxio_lsa.pageid, log_Gl.append.nxio_lsa.offset);

  fprintf (outfp, "\tcheckpoint lsa : (%d, %d)\n",
	   log_Gl.hdr.chkpt_lsa.pageid, log_Gl.hdr.chkpt_lsa.offset);

  fprintf (outfp, "\tnext archive page id : %d\n", log_Gl.hdr.nxarv_pageid);

  fprintf (outfp, "\tnext archive physical page id : %d\n",
	   log_Gl.hdr.nxarv_phy_pageid);

  fprintf (outfp, "\tnext archive number : %d\n", log_Gl.hdr.nxarv_num);

  fprintf (outfp, "\tlast archive number needed for system crashes : %d\n",
	   log_Gl.hdr.last_arv_num_for_syscrashes);

  fprintf (outfp, "\tlast archive number deleted : %d\n",
	   log_Gl.hdr.last_deleted_arv_num);

  fprintf (outfp, "\tbackup level 0 lsa : (%d, %d)\n",
	   log_Gl.hdr.bkup_level0_lsa.pageid,
	   log_Gl.hdr.bkup_level0_lsa.offset);

  fprintf (outfp, "\tbackup level 1 lsa : (%d, %d)\n",
	   log_Gl.hdr.bkup_level1_lsa.pageid,
	   log_Gl.hdr.bkup_level1_lsa.offset);

  fprintf (outfp, "\tbackup level 2 lsa : (%d, %d)\n",
	   log_Gl.hdr.bkup_level2_lsa.pageid,
	   log_Gl.hdr.bkup_level2_lsa.offset);

  fprintf (outfp, "\tlowest log archive number for backup : %d\n",
	   log_Gl.hdr.lowest_arv_num_for_backup);

  fprintf (outfp, "\thighest log archive number for backup : %d\n",
	   log_Gl.hdr.highest_arv_num_for_backup);
}

/*
 * logpb_dump_parameter - dump logging parameter
 *
 * return: Nothing
 *
 *   outfp(in): file descriptor
 *
 * NOTE:
 */
static void
logpb_dump_parameter (FILE * outfp)
{
  fprintf (outfp, "Log Parameters:\n");

  fprintf (outfp, "\treplication_meta_flush_interval (sec) : %d\n",
	   PRM_LOG_HEADER_FLUSH_INTERVAL);

  fprintf (outfp, "\tgroup_commit_interval_msec : %d\n",
	   PRM_LOG_GROUP_COMMIT_INTERVAL_MSECS);

  fprintf (outfp, "\tasync_commit : %s\n",
	   PRM_LOG_ASYNC_COMMIT ? "on" : "off");

  fprintf (outfp, "\tbg_flush_interval_msecs : %d\n",
	   PRM_LOG_BG_FLUSH_INTERVAL_MSECS);

  fprintf (outfp, "\tbg_flush_num_pages : %d\n", PRM_LOG_BG_FLUSH_NUM_PAGES);
}

/*
 * logpb_dump_runtime - dump runtime logging information
 *
 * return: Nothing
 *
 *   outfp(in): file descriptor
 *
 * NOTE:
 */
static void
logpb_dump_runtime (FILE * outfp)
{
  long temp = 1;

  fprintf (outfp, "Log Statistics:\n");

  fprintf (outfp, "\ttotal flush count = %ld\n",
	   log_Stat.flushall_append_pages_call_count);

  fprintf (outfp, "\tgroup commit flush count= %ld\n",
	   log_Stat.gc_flush_count);

  fprintf (outfp, "\tdirect flush count= %ld\n", log_Stat.direct_flush_count);

  fprintf (outfp, "\tgroup commit request count = %ld\n",
	   log_Stat.gc_commit_request_count);

  fprintf (outfp, "\tasync commit request count = %ld\n",
	   log_Stat.async_commit_request_count);

  if (log_Stat.flushall_append_pages_call_count != 0)
    {
      temp = (log_Stat.flushall_append_pages_call_count
	      - log_Stat.direct_flush_count);
    }

  fprintf (outfp, "\tgroup commit grouping rate = %f\n",
	   (double) log_Stat.gc_commit_request_count / temp);

  fprintf (outfp, "\tasync commit grouping rate = %f\n",
	   (double) log_Stat.async_commit_request_count / temp);

  temp = 1;
  if (log_Stat.gc_commit_request_count != 0)
    {
      temp = log_Stat.gc_commit_request_count;
    }

  fprintf (outfp, "\tavg group commit wait time = %f\n",
	   log_Stat.gc_total_wait_time / temp);

  fprintf (outfp, "\tbackground flush count = %ld\n",
	   log_Stat.flush_pages_call_count);

  fprintf (outfp, "\tbackground flush miss count = %ld\n",
	   log_Stat.flush_pages_call_miss_count);

  fprintf (outfp, "\tbackground flush pages count = %ld\n",
	   log_Stat.total_flush_count_by_LFT);

  temp = 1;
  if (log_Stat.flush_pages_call_count != 0)
    {
      temp = log_Stat.flush_pages_call_count;
    }

  fprintf (outfp, "\tavg background flush pages count = %f\n",
	   (double) log_Stat.total_flush_count_by_LFT / temp);

  fprintf (outfp, "\tavg background flush hit ratio = %f\n",
	   (double) log_Stat.flush_pages_call_count /
	   (temp + log_Stat.flush_pages_call_miss_count));

  fprintf (outfp, "\ttotal commit count = %ld\n", log_Stat.commit_count);

  fprintf (outfp, "\ttotal allocated log pages count = %ld\n",
	   log_Stat.total_append_page_count);

  fprintf (outfp, "\tlog buffer full count = %ld\n",
	   log_Stat.log_buffer_full_count);

  fprintf (outfp, "\tlog buffer flush count by replacement = %ld\n",
	   log_Stat.log_buffer_flush_count_by_replacement);

  fprintf (outfp, "\tlog buffer expand count = %ld\n",
	   log_Stat.log_buffer_expand_count);
}

/*
 * xlogpb_dump_stat - dump logging information
 *
 * return: Nothing
 *
 *   outfp(in): file descriptor
 *
 * NOTE:
 */
void
xlogpb_dump_stat (FILE * outfp)
{
  logpb_dump_parameter (outfp);
  logpb_dump_log_header (outfp);
  logpb_dump_runtime (outfp);
}
