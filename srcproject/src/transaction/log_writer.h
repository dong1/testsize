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
 * log_writer.h - DECLARATIONS FOR LOG WRITER (AT CLIENT & SERVER)
 */

#ifndef _LOG_WRITER_HEADER_
#define _LOG_WRITER_HEADER_

#ident "$Id$"

#include <stdio.h>
#include "log_impl.h"

typedef struct logwr_context LOGWR_CONTEXT;
struct logwr_context
{
  int rc;
  int last_error;
  bool shutdown;
};

#if defined(CS_MODE)
typedef enum logwr_action LOGWR_ACTION;
enum logwr_action
{
  LOGWR_ACTION_NONE = 0x00,
  LOGWR_ACTION_DELAYED_WRITE = 0x01,
  LOGWR_ACTION_ASYNC_WRITE = 0x02,
  LOGWR_ACTION_HDR_WRITE = 0x04,
  LOGWR_ACTION_ARCHIVING = 0x08
};

typedef struct logwr_global LOGWR_GLOBAL;
struct logwr_global
{
  struct log_header hdr;
  LOG_PAGE *loghdr_pgptr;

  char db_name[PATH_MAX];
  char log_path[PATH_MAX];
  char loginf_path[PATH_MAX];
  char active_name[PATH_MAX];
  int append_vdes;

  char *logpg_area;
  int logpg_area_size;
  int logpg_fill_size;

  LOG_PAGE **toflush;
  int max_toflush;
  int num_toflush;

  LOGWR_MODE mode;
  LOGWR_ACTION action;

  PAGEID last_chkpt_pageid;
  PAGEID last_recv_pageid;
  PAGEID last_arv_fpageid;
  PAGEID last_arv_lpageid;
  int last_arv_num;
  int last_deleted_arv_num;

  bool force_flush;
  struct timeval last_flush_time;
  /* background log archiving info */
  BACKGROUND_ARCHIVING_INFO bg_archive_info;
  char bg_archive_name[PATH_MAX];
};

#define LOGWR_AT_NEXT_ARCHIVE_PAGE_ID(pageid) \
        (logwr_to_physical_pageid(pageid) == logwr_Gl.hdr.nxarv_phy_pageid)

#define LOGWR_IS_ARCHIVE_PAGE(pageid) \
        ((pageid) != LOGPB_HEADER_PAGE_ID && (pageid) < logwr_Gl.hdr.nxarv_pageid)

#define LOGWR_AT_SERVER_ARCHIVING() \
	(LOGWR_AT_NEXT_ARCHIVE_PAGE_ID(logwr_Gl.hdr.append_lsa.pageid) \
	 && (logwr_Gl.hdr.eof_lsa.pageid < logwr_Gl.hdr.append_lsa.pageid))

extern LOGWR_GLOBAL logwr_Gl;
extern void logwr_flush_header_page (void);
extern int logwr_write_log_pages (void);
extern int logwr_set_hdr_and_flush_info (void);
#endif /* CS_MODE */

extern int logwr_copy_log_file (const char *db_name, const char *log_path,
				int mode);
extern PAGEID logwr_to_physical_pageid (PAGEID logical_pageid);

#if defined(SERVER_MODE)
int xlogwr_get_log_pages (THREAD_ENTRY * thread_p, PAGEID first_pageid,
			  int mode);
extern int logwr_get_min_copied_fpageid (void);
#endif /* SERVER_MODE */
#endif /* _LOG_WRITER_HEADER_ */
