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
 * perf_monitor.c - Monitor execution statistics at Client
 * 					Monitor execution statistics
 *                  Monitor execution statistics at Server
 * 					diag server module
 */

#include <stdio.h>
#include <time.h>
#if !defined (WINDOWS)
#include <sys/time.h>
#include <sys/resource.h>
#endif /* WINDOWS */
#include "perf_monitor.h"
#include "network_interface_cl.h"
#include "error_manager.h"

#if !defined(SERVER_MODE)
#include "memory_alloc.h"
#include "server_interface.h"
#endif /* !SERVER_MODE */

#if defined(SERVER_MODE)
#include <string.h>
#include <errno.h>
#include <sys/types.h>

#if !defined(WINDOWS)
#include <sys/shm.h>
#include <sys/ipc.h>
#endif /* WINDOWS */

#include <sys/stat.h>
#include "connection_defs.h"
#include "environment_variable.h"
#include "connection_error.h"
#include "databases_file.h"
#endif /* SERVER_MODE */

#include "thread_impl.h"
#include "log_impl.h"

#if !defined(CS_MODE)
#include <string.h>
#include <assert.h>

#include "error_manager.h"
#include "log_manager.h"
#include "system_parameter.h"
#include "xserver_interface.h"

#if defined (SERVER_MODE)
#include "thread_impl.h"
#include "connection_error.h"
#endif /* SERVER_MODE */

#if !defined(SERVER_MODE)
#undef MUTEX_INIT
#define MUTEX_INIT(a)
#undef MUTEX_DESTROY
#define MUTEX_DESTROY(a)
#undef MUTEX_LOCK
#define MUTEX_LOCK(a, b)
#undef MUTEX_UNLOCK
#define MUTEX_UNLOCK(a)
#endif /* SERVER_MODE */
#endif /* !CS_MODE */

static void mnt_server_reset_stats_internal (MNT_SERVER_EXEC_STATS * stats);
static void mnt_server_calc_stats (MNT_SERVER_EXEC_STATS * stats);
static void mnt_server_calc_global_stats (MNT_SERVER_EXEC_GLOBAL_STATS *
					  stats);
static void mnt_server_check_stats_threshold (int tran_index,
					      MNT_SERVER_EXEC_STATS * stats);


#if defined(CS_MODE) || defined(SA_MODE)
/* Client execution statistics */
static MNT_EXEC_STATS mnt_Stats;
static bool mnt_Iscollecting_stats = false;
static void mnt_client_reset_stats (void);

#if defined(CS_MODE)
/* Server execution statistics */
static MNT_SERVER_EXEC_STATS mnt_Server_stats;
#endif /* CS_MODE */

/*
 * mnt_start_stats - Start collecting client execution statistics
 *   return: NO_ERROR or ER_FAILED
 */
int
mnt_start_stats (bool for_all_trans)
{
  int err = NO_ERROR;

  if (mnt_Iscollecting_stats != true)
    {
      err = mnt_server_start_stats (for_all_trans);
      if (err != ER_FAILED)
	{
	  mnt_Iscollecting_stats = true;
	  mnt_client_reset_stats ();
	}
    }
  return err;
}

/*
 * mnt_stop_stats - Stop collecting client execution statistics
 *   return: NO_ERROR or ER_FAILED
 */
int
mnt_stop_stats (void)
{
  int err = NO_ERROR;

  if (mnt_Iscollecting_stats != false)
    {
      err = mnt_server_stop_stats ();
      mnt_client_reset_stats ();
      mnt_Iscollecting_stats = false;
      mnt_Stats.server_stats = NULL;
    }
  return err;
}

/*
 * mnt_reset_stats - Reset client and server statistics
 *   return: none
 */
void
mnt_reset_stats (void)
{
  if (mnt_Iscollecting_stats != false)
    {
      mnt_server_reset_stats ();
      mnt_client_reset_stats ();
    }
}

/*
 * mnt_reset_global_stats - Reset server global statistics
 *   return: none
 */
void
mnt_reset_global_stats (void)
{
  if (mnt_Iscollecting_stats != false)
    {
      mnt_server_reset_global_stats ();
    }
}

/*
 * mnt_client_reset_stats - Reset the client statistics
 *   return: none
 */
static void
mnt_client_reset_stats (void)
{
  mnt_get_current_times (&mnt_Stats.cpu_start_usr_time,
			 &mnt_Stats.cpu_start_sys_time,
			 &mnt_Stats.elapsed_start_time);
  mnt_Stats.server_stats = NULL;
}

/*
 * mnt_get_stats - Get the recorded client statistics
 *   return: client statistics
 */
MNT_EXEC_STATS *
mnt_get_stats ()
{
  if (mnt_Iscollecting_stats != true)
    {
      return NULL;
    }
  else
    {
      /* Refresh statistics from server */
#if defined (SA_MODE)
      mnt_Stats.server_stats = mnt_server_get_stats (NULL);
      mnt_server_calc_stats (mnt_Stats.server_stats);
#else /* SA_MODE */
      mnt_server_copy_stats (&mnt_Server_stats);
      mnt_Stats.server_stats = &mnt_Server_stats;
#endif /* SA_MODE */

      return &mnt_Stats;
    }
}

/*
 * mnt_get_global_stats - Get the recorded client statistics
 *   return: client statistics
 */
MNT_SERVER_EXEC_GLOBAL_STATS *
mnt_get_global_stats (void)
{
  if (mnt_Iscollecting_stats != true)
    {
      return NULL;
    }
  else
    {
      /* Refresh statistics from server */
      mnt_server_copy_global_stats (&mnt_Stats.server_global_stats);

      return &mnt_Stats.server_global_stats;
    }
}

/*
 * mnt_print_stats - Print the current client statistics
 *   return:
 *   stream(in): if NULL is given, stdout is used
 */
void
mnt_print_stats (FILE * stream)
{
  MNT_EXEC_STATS *stats;
  time_t cpu_total_usr_time;
  time_t cpu_total_sys_time;
  time_t elapsed_total_time;

  if (stream == NULL)
    {
      stream = stdout;
    }

  stats = mnt_get_stats ();
  if (stats != NULL)
    {
      mnt_get_current_times (&cpu_total_usr_time, &cpu_total_sys_time,
			     &elapsed_total_time);

      fprintf (stream, "\n *** CLIENT EXECUTION STATISTICS ***\n");

      fprintf (stream, "System CPU (sec)              = %10d\n",
	       (int) (cpu_total_sys_time - mnt_Stats.cpu_start_sys_time));
      fprintf (stream, "User CPU (sec)                = %10d\n",
	       (int) (cpu_total_usr_time - mnt_Stats.cpu_start_usr_time));
      fprintf (stream, "Elapsed (sec)                 = %10d\n",
	       (int) (elapsed_total_time - mnt_Stats.elapsed_start_time));

      mnt_server_dump_stats (stats->server_stats, stream);
    }
}

/*
 * mnt_print_global_stats - Print the global statistics
 *   return:
 *   stream(in): if NULL is given, stdout is used
 */
void
mnt_print_global_stats (FILE * stream)
{
  MNT_SERVER_EXEC_GLOBAL_STATS *stats;

  if (stream == NULL)
    stream = stdout;

  stats = mnt_get_global_stats ();
  if (stats != NULL)
    {
      mnt_server_dump_global_stats (stats, stream);
    }
}
#endif /* CS_MODE || SA_MODE */

#if defined (DIAG_DEVEL)
#if defined(SERVER_MODE)
#if defined(WINDOWS)
#define SERVER_SHM_CREATE(SHM_KEY, SIZE, HANDLE_PTR)    \
        server_shm_create(SHM_KEY, SIZE, HANDLE_PTR)
#define SERVER_SHM_OPEN(SHM_KEY, HANDLE_PTR)            \
        server_shm_open(SHM_KEY, HANDLE_PTR)
#define SERVER_SHM_DETACH(PTR, HMAP)	\
        do {				\
          if (HMAP != NULL) {		\
            UnmapViewOfFile(PTR);	\
            CloseHandle(HMAP);		\
          }				\
        } while (0)
#else /* WINDOWS */
#define SERVER_SHM_CREATE(SHM_KEY, SIZE, HANDLE_PTR)    \
        server_shm_create(SHM_KEY, SIZE)
#define SERVER_SHM_OPEN(SHM_KEY, HANDLE_PTR)            \
        server_shm_open(SHM_KEY)
#define SERVER_SHM_DETACH(PTR, HMAP)    shmdt(PTR)
#endif /* WINDOWS */

#define SERVER_SHM_DESTROY(SHM_KEY)     \
        server_shm_destroy(SHM_KEY)

#define CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT(ERR_BUF) \
    do { \
        if (thread_is_manager_initialized() == false) {\
            if (ERR_BUF) strcpy(ERR_BUF, "thread mgr is not initialized");\
            return -1;\
        }\
    } while(0)

#define CHECK_SHM() \
    do { \
        if (g_ShmServer == NULL) return -1; \
    } while(0)

#define CUBRID_KEY_GEN_ID 0x08
#define DIAG_SERVER_MAGIC_NUMBER 07115

/* Global variables */
bool diag_executediag;
int diag_long_query_time;

static int ShmPort;
static T_SHM_DIAG_INFO_SERVER *g_ShmServer = NULL;

#if defined(WINDOWS)
static HANDLE shm_map_object;
#endif /* WINDOWS */

/* Diag value modification function */
static int diag_val_set_query_open_page (int value,
					 T_DIAG_VALUE_SETTYPE settype,
					 char *err_buf);
static int diag_val_set_query_opened_page (int value,
					   T_DIAG_VALUE_SETTYPE settype,
					   char *err_buf);
static int diag_val_set_buffer_page_read (int value,
					  T_DIAG_VALUE_SETTYPE settype,
					  char *err_buf);
static int diag_val_set_buffer_page_write (int value,
					   T_DIAG_VALUE_SETTYPE settype,
					   char *err_buf);
static int diag_val_set_conn_aborted_clients (int value,
					      T_DIAG_VALUE_SETTYPE settype,
					      char *err_buf);
static int diag_val_set_conn_cli_request (int value,
					  T_DIAG_VALUE_SETTYPE settype,
					  char *err_buf);
static int diag_val_set_query_slow_query (int value,
					  T_DIAG_VALUE_SETTYPE settype,
					  char *err_buf);
static int diag_val_set_lock_deadlock (int value,
				       T_DIAG_VALUE_SETTYPE settype,
				       char *err_buf);
static int diag_val_set_lock_request (int value,
				      T_DIAG_VALUE_SETTYPE settype,
				      char *err_buf);
static int diag_val_set_query_full_scan (int value,
					 T_DIAG_VALUE_SETTYPE settype,
					 char *err_buf);
static int diag_val_set_conn_conn_req (int value,
				       T_DIAG_VALUE_SETTYPE settype,
				       char *err_buf);
static int diag_val_set_conn_conn_reject (int value,
					  T_DIAG_VALUE_SETTYPE settype,
					  char *err_buf);

static int server_shm_destroy (int shm_key);
static bool diag_sm_isopened (void);
static bool init_server_diag_value (T_SHM_DIAG_INFO_SERVER * shm_server);
static bool init_diag_sm (const char *server_name, int num_thread,
			  char *err_buf);
static bool rm_diag_sm (void);

static char *trim_line (char *str);
static int create_shm_key_file (int port, char *vol_dir,
				const char *servername);
static int read_diag_system_config (DIAG_SYS_CONFIG * config, char *err_buf);
static int get_volumedir (char *vol_dir, const char *dbname);
static int get_server_shmid (char *dir, const char *dbname);


#if defined(WINDOWS)
static void shm_key_to_name (int shm_key, char *name_str);
static void *server_shm_create (int shm_key, int size, HANDLE * hOut);
static void *server_shm_open (int shm_key, HANDLE * hOut);
#else /* WINDOWS */
static void *server_shm_create (int shm_key, int size);
static void *server_shm_open (int shm_key);
#endif /* WINDOWS */

T_DIAG_OBJECT_TABLE diag_obj_list[] = {
  {"open_page", DIAG_OBJ_TYPE_QUERY_OPEN_PAGE, diag_val_set_query_open_page}
  , {"opened_page", DIAG_OBJ_TYPE_QUERY_OPENED_PAGE,
     diag_val_set_query_opened_page}
  , {"slow_query", DIAG_OBJ_TYPE_QUERY_SLOW_QUERY,
     diag_val_set_query_slow_query}
  , {"full_scan", DIAG_OBJ_TYPE_QUERY_FULL_SCAN, diag_val_set_query_full_scan}
  , {"cli_request", DIAG_OBJ_TYPE_CONN_CLI_REQUEST,
     diag_val_set_conn_cli_request}
  , {"aborted_client", DIAG_OBJ_TYPE_CONN_ABORTED_CLIENTS,
     diag_val_set_conn_aborted_clients}
  , {"conn_req", DIAG_OBJ_TYPE_CONN_CONN_REQ, diag_val_set_conn_conn_req}
  , {"conn_reject", DIAG_OBJ_TYPE_CONN_CONN_REJECT,
     diag_val_set_conn_conn_reject}
  , {"buffer_page_read", DIAG_OBJ_TYPE_BUFFER_PAGE_READ,
     diag_val_set_buffer_page_read}
  , {"buffer_page_write", DIAG_OBJ_TYPE_BUFFER_PAGE_WRITE,
     diag_val_set_buffer_page_write}
  , {"lock_deadlock", DIAG_OBJ_TYPE_LOCK_DEADLOCK, diag_val_set_lock_deadlock}
  , {"lock_request", DIAG_OBJ_TYPE_LOCK_REQUEST, diag_val_set_lock_request}
};

/* function definition */
/*
 * trim_line()
 *    return: char *
 *    str(in):
 */
static char *
trim_line (char *str)
{
  char *p;
  char *s;

  if (str == NULL)
    return (str);

  for (s = str;
       *s != '\0' && (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r');
       s++)
    ;
  if (*s == '\0')
    {
      *str = '\0';
      return (str);
    }

  /* *s must be a non-white char */
  for (p = s; *p != '\0'; p++)
    ;
  for (p--; *p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'; p--)
    ;
  *++p = '\0';

  if (s != str)
    {
      memcpy (str, s, strlen (s) + 1);
    }

  return (str);
}

/*
 * create_shm_key_file()
 *    return: int
 *    port(in):
 *    vol_dir(in):
 *    servername(in):
 */
static int
create_shm_key_file (int port, char *vol_dir, const char *servername)
{
  FILE *keyfile;
  char keyfilepath[PATH_MAX];

  if (!vol_dir || !servername)
    {
      return -1;
    }

  sprintf (keyfilepath, "%s/%s_shm.key", vol_dir, servername);
  keyfile = fopen (keyfilepath, "w+");
  if (keyfile)
    {
      fprintf (keyfile, "%x", port);
      fclose (keyfile);
      return 1;
    }

  return -1;
}

/*
 * read_diag_system_config()
 *    return: int
 *    config(in):
 *    err_buf(in):
 */
static int
read_diag_system_config (DIAG_SYS_CONFIG * config, char *err_buf)
{
  FILE *conf_file;
  char cbuf[1024], file_path[PATH_MAX];
  char *cubrid_home;
  char ent_name[128], ent_val[128];

  if (config == NULL)
    {
      return -1;
    }

  /* Initialize config data */
  config->Executediag = 0;
  config->server_long_query_time = 0;

  cubrid_home = envvar_root ();

  if (cubrid_home == NULL)
    {
      if (err_buf)
	{
	  strcpy (err_buf, "Environment variable CUBRID is not set.");
	}
      return -1;
    }

  envvar_confdir_file (file_path, PATH_MAX, "cm.conf");

  conf_file = fopen (file_path, "r");

  if (conf_file == NULL)
    {
      if (err_buf)
	{
	  sprintf (err_buf, "File(%s) open error.", file_path);
	}
      return -1;
    }

  while (fgets (cbuf, sizeof (cbuf), conf_file))
    {
      char format[1024];

      trim_line (cbuf);
      if (cbuf[0] == '\0' || cbuf[0] == '#')
	{
	  continue;
	}

      snprintf (format, sizeof (format), "%%%ds %%%ds",
		(int) sizeof (ent_name), (int) sizeof (ent_val));
      if (sscanf (cbuf, format, ent_name, ent_val) < 2)
	{
	  continue;
	}

      if (strcasecmp (ent_name, "Execute_diag") == 0)
	{
	  if (strcasecmp (ent_val, "ON") == 0)
	    {
	      config->Executediag = 1;
	    }
	  else
	    {
	      config->Executediag = 0;
	    }
	}
      else if (strcasecmp (ent_name, "server_long_query_time") == 0)
	{
	  config->server_long_query_time = atoi (ent_val);
	}
    }

  fclose (conf_file);
  return 1;
}

/*
 * get_volumedir()
 *    return: int
 *    vol_dir(in):
 *    dbname(in):
 *    err_buf(in):
 */
static int
get_volumedir (char *vol_dir, const char *dbname)
{
  FILE *databases_txt;
#if !defined (DO_NOT_USE_CUBRIDENV)
  const char *envpath;
#endif
  char db_txt[PATH_MAX];
  char cbuf[PATH_MAX * 2];
  char volname[MAX_SERVER_NAMELENGTH];

  if (vol_dir == NULL || dbname == NULL)
    {
      return -1;
    }

#if !defined (DO_NOT_USE_CUBRIDENV)
  envpath = envvar_get ("DATABASES");
  if (envpath == NULL || strlen (envpath) == 0)
    {
      return -1;
    }

  sprintf (db_txt, "%s/%s", envpath, DATABASES_FILENAME);
#else
  envvar_vardir_file (db_txt, PATH_MAX, DATABASES_FILENAME);
#endif
  databases_txt = fopen (db_txt, "r");
  if (databases_txt == NULL)
    {
      return -1;
    }

  while (fgets (cbuf, sizeof (cbuf), databases_txt))
    {
      char format[1024];
      snprintf (format, sizeof (format), "%%%ds %%%ds %%*s %%*s",
		(int) sizeof (volname), PATH_MAX);

      if (sscanf (cbuf, format, volname, vol_dir) < 2)
	continue;

      if (strcmp (volname, dbname) == 0)
	{
	  fclose (databases_txt);
	  return 1;
	}
    }

  fclose (databases_txt);
  return -1;
}

/*
 * get_server_shmid()
 *    return: int
 *    dir(in):
 *    dbname(in):
 */
static int
get_server_shmid (char *dir, const char *dbname)
{
  int shm_key = 0;
  char vol_full_path[PATH_MAX];
  char *p;

  sprintf (vol_full_path, "%s/%s", dir, dbname);
  for (p = vol_full_path; *p; p++)
    {
      shm_key = 31 * shm_key + (*p);
    }
  shm_key &= 0x00ffffff;

  return shm_key;
}

#if defined(WINDOWS)

/*
 * shm_key_to_name()
 *    return: none
 *    shm_key(in):
 *    name_str(in):
 */
static void
shm_key_to_name (int shm_key, char *name_str)
{
  sprintf (name_str, "cubrid_shm_%d", shm_key);
}

/*
 * server_shm_create()
 *    return: void*
 *    shm_key(in):
 *    size(in):
 *    hOut(in):
 */
static void *
server_shm_create (int shm_key, int size, HANDLE * hOut)
{
  LPVOID lpvMem = NULL;
  HANDLE hMapObject = NULL;
  char shm_name[64];

  *hOut = NULL;

  shm_key_to_name (shm_key, shm_name);

  hMapObject = CreateFileMapping (INVALID_HANDLE_VALUE,
				  NULL, PAGE_READWRITE, 0, size, shm_name);
  if (hMapObject == NULL)
    {
      return NULL;
    }

  if (GetLastError () == ERROR_ALREADY_EXISTS)
    {
      CloseHandle (hMapObject);
      return NULL;
    }

  lpvMem = MapViewOfFile (hMapObject, FILE_MAP_WRITE, 0, 0, 0);
  if (lpvMem == NULL)
    {
      CloseHandle (hMapObject);
      return NULL;
    }

  *hOut = hMapObject;
  return lpvMem;
}

/*
 * server_shm_open()
 *    return: void *
 *    shm_key(in):
 *    hOut(in):
 */
static void *
server_shm_open (int shm_key, HANDLE * hOut)
{
  LPVOID lpvMem = NULL;		/* address of shared memory */
  HANDLE hMapObject = NULL;
  char shm_name[64];

  *hOut = NULL;

  shm_key_to_name (shm_key, shm_name);

  hMapObject = OpenFileMapping (FILE_MAP_WRITE,	/* read/write access */
				FALSE,	/* inherit flag */
				shm_name);	/* name of map object */
  if (hMapObject == NULL)
    {
      return NULL;
    }

  /* Get a pointer to the file-mapped shared memory. */
  lpvMem = MapViewOfFile (hMapObject,	/* object to map view of    */
			  FILE_MAP_WRITE,	/* read/write access        */
			  0,	/* high offset:   map from  */
			  0,	/* low offset:    beginning */
			  0);	/* default: map entire file */
  if (lpvMem == NULL)
    {
      CloseHandle (hMapObject);
      return NULL;
    }

  *hOut = hMapObject;
  return lpvMem;
}

#else /* WINDOWS */

/*
 * server_shm_create()
 *    return: void *
 *    shm_key(in):
 *    size(in):
 */
static void *
server_shm_create (int shm_key, int size)
{
  int mid;
  void *p;

  if (size <= 0 || shm_key <= 0)
    {
      return NULL;
    }

  mid = shmget (shm_key, size, IPC_CREAT | IPC_EXCL | SH_MODE);

  if (mid == -1)
    {
      return NULL;
    }
  p = shmat (mid, (char *) 0, 0);

  if (p == (void *) -1)
    {
      return NULL;
    }

  return p;
}

/*
 * server_shm_open()
 *    return: void *
 *    shm_key(in):
 */
static void *
server_shm_open (int shm_key)
{
  int mid;
  void *p;

  if (shm_key < 0)
    {
      return NULL;
    }
  mid = shmget (shm_key, 0, SHM_RDONLY);

  if (mid == -1)
    return NULL;

  p = shmat (mid, (char *) 0, SHM_RDONLY);

  if (p == (void *) -1)
    {
      return NULL;
    }
  return p;
}
#endif /* WINDOWS */

/*
 * server_shm_destroy() -
 *    return: int
 *    shm_key(in):
 */
static int
server_shm_destroy (int shm_key)
{
#if !defined(WINDOWS)
  int mid;

  mid = shmget (shm_key, 0, SH_MODE);

  if (mid == -1)
    {
      return -1;
    }

  if (shmctl (mid, IPC_RMID, 0) == -1)
    {
      return -1;
    }
#endif /* WINDOWS */
  return 0;
}

/*
 * diag_sm_isopened() -
 *    return : bool
 */
static bool
diag_sm_isopened (void)
{
  return (g_ShmServer == NULL) ? false : true;
}

/*
 * init_server_diag_value() -
 *    return : bool
 *    shm_server(in):
 */
static bool
init_server_diag_value (T_SHM_DIAG_INFO_SERVER * shm_server)
{
  int i, thread_num;

  if (!shm_server)
    return false;

  thread_num = shm_server->num_thread;
  for (i = 0; i < thread_num; i++)
    {
      shm_server->thread[i].query_open_page = 0;
      shm_server->thread[i].query_opened_page = 0;
      shm_server->thread[i].query_slow_query = 0;
      shm_server->thread[i].query_full_scan = 0;
      shm_server->thread[i].conn_cli_request = 0;
      shm_server->thread[i].conn_aborted_clients = 0;
      shm_server->thread[i].conn_conn_req = 0;
      shm_server->thread[i].conn_conn_reject = 0;
      shm_server->thread[i].buffer_page_write = 0;
      shm_server->thread[i].buffer_page_read = 0;
      shm_server->thread[i].lock_deadlock = 0;
      shm_server->thread[i].lock_request = 0;
    }

  return true;
}

/*
 * init_diag_sm()
 *    return: bool
 *    server_name(in):
 *    num_thread(in):
 *    err_buf(in):
 */
static bool
init_diag_sm (const char *server_name, int num_thread, char *err_buf)
{
  DIAG_SYS_CONFIG config_diag;
  char vol_dir[PATH_MAX];
  int i;

  if (server_name == NULL)
    {
      goto init_error;
    }
  if (read_diag_system_config (&config_diag, err_buf) != 1)
    {
      goto init_error;
    }
  if (!config_diag.Executediag)
    {
      goto init_error;
    }
  if (get_volumedir (vol_dir, server_name) == -1)
    {
      goto init_error;
    }

  ShmPort = get_server_shmid (vol_dir, server_name);

  if (ShmPort == -1)
    {
      goto init_error;
    }

  g_ShmServer = (T_SHM_DIAG_INFO_SERVER *)
    SERVER_SHM_CREATE (ShmPort,
		       sizeof (T_SHM_DIAG_INFO_SERVER), &shm_map_object);

  for (i = 0; (i < 5 && !g_ShmServer); i++)
    {
      if (errno == EEXIST)
	{
	  T_SHM_DIAG_INFO_SERVER *shm = (T_SHM_DIAG_INFO_SERVER *)
	    SERVER_SHM_OPEN (ShmPort,
			     &shm_map_object);
	  if (shm != NULL)
	    {
	      if ((shm->magic_key == DIAG_SERVER_MAGIC_NUMBER)
		  && (shm->servername)
		  && strcmp (shm->servername, server_name) == 0)
		{
		  SERVER_SHM_DETACH ((void *) shm, shm_map_object);
		  SERVER_SHM_DESTROY (ShmPort);
		  g_ShmServer = (T_SHM_DIAG_INFO_SERVER *)
		    SERVER_SHM_CREATE (ShmPort,
				       sizeof (T_SHM_DIAG_INFO_SERVER),
				       &shm_map_object);
		  break;
		}
	      else
		SERVER_SHM_DETACH ((void *) shm, shm_map_object);
	    }

	  ShmPort++;
	  g_ShmServer = (T_SHM_DIAG_INFO_SERVER *)
	    SERVER_SHM_CREATE (ShmPort,
			       sizeof (T_SHM_DIAG_INFO_SERVER),
			       &shm_map_object);
	}
      else
	{
	  break;
	}
    }

  if (g_ShmServer == NULL)
    {
      if (err_buf)
	{
	  strcpy (err_buf, strerror (errno));
	}
      goto init_error;
    }

  diag_long_query_time = config_diag.server_long_query_time;
  diag_executediag = (config_diag.Executediag == 0) ? false : true;

  if (diag_long_query_time < 1)
    {
      diag_long_query_time = DB_INT32_MAX;
    }

  strcpy (g_ShmServer->servername, server_name);
  g_ShmServer->num_thread = num_thread;
  g_ShmServer->magic_key = DIAG_SERVER_MAGIC_NUMBER;

  init_server_diag_value (g_ShmServer);

  if (create_shm_key_file (ShmPort, vol_dir, server_name) == -1)
    {
      if (err_buf)
	{
	  strcpy (err_buf, strerror (errno));
	}
      SERVER_SHM_DETACH ((void *) g_ShmServer, shm_map_object);
      SERVER_SHM_DESTROY (ShmPort);
      goto init_error;
    }

  return true;

init_error:
  g_ShmServer = NULL;
  diag_executediag = false;
  diag_long_query_time = DB_INT32_MAX;
  return false;
}

/*
 * rm_diag_sm()
 *    return: bool
 *
 */
static bool
rm_diag_sm (void)
{
  if (diag_sm_isopened () == true)
    {
      SERVER_SHM_DESTROY (ShmPort);
      return true;
    }

  return false;
}

/*
 * diag_val_set_query_open_page()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 *
 */
static int
diag_val_set_query_open_page (int value,
			      T_DIAG_VALUE_SETTYPE settype, char *err_buf)
{
  int thread_index;

  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  if (settype == DIAG_VAL_SETTYPE_INC)
    {
      g_ShmServer->thread[thread_index].query_open_page += value;
    }
  else if (settype == DIAG_VAL_SETTYPE_SET)
    {
      g_ShmServer->thread[thread_index].query_open_page = value;
    }
  else if (settype == DIAG_VAL_SETTYPE_DEC)
    {
      g_ShmServer->thread[thread_index].query_open_page -= value;
    }

  return 0;
}

/*
 * diag_val_set_query_opened_page()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 *
 */
static int
diag_val_set_query_opened_page (int value,
				T_DIAG_VALUE_SETTYPE settype, char *err_buf)
{
  int thread_index;

  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].query_opened_page += value;

  return 0;
}

/*
 * diag_val_set_buffer_page_read()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 *
 */
static int
diag_val_set_buffer_page_read (int value,
			       T_DIAG_VALUE_SETTYPE settype, char *err_buf)
{
  int thread_index;

  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].buffer_page_read += value;

  return 0;
}

/*
 * diag_val_set_buffer_page_write()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 *
 */
static int
diag_val_set_buffer_page_write (int value,
				T_DIAG_VALUE_SETTYPE settype, char *err_buf)
{
  int thread_index;

  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].buffer_page_write += value;

  return 0;
}


/*
 * diag_val_set_conn_aborted_clients()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 *
 */
static int
diag_val_set_conn_aborted_clients (int value, T_DIAG_VALUE_SETTYPE settype,
				   char *err_buf)
{
  int thread_index;
  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].conn_aborted_clients += value;

  return 0;
}

/*
 * diag_val_set_conn_cli_request()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 *
 */
static int
diag_val_set_conn_cli_request (int value, T_DIAG_VALUE_SETTYPE settype,
			       char *err_buf)
{
  int thread_index;
  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].conn_cli_request += value;

  return 0;
}

/*
 * diag_val_set_query_slow_query()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 *
 */
static int
diag_val_set_query_slow_query (int value, T_DIAG_VALUE_SETTYPE settype,
			       char *err_buf)
{
  int thread_index;
  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].query_slow_query += value;

  return 0;
}

/*
 * diag_val_set_lock_deadlock()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 *
 */
static int
diag_val_set_lock_deadlock (int value, T_DIAG_VALUE_SETTYPE settype,
			    char *err_buf)
{
  int thread_index;
  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].lock_deadlock += value;

  return 0;
}

/*
 * diag_val_set_lock_request()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 */
static int
diag_val_set_lock_request (int value, T_DIAG_VALUE_SETTYPE settype,
			   char *err_buf)
{
  int thread_index;
  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].lock_request += value;

  return 0;
}

/*
 * diag_val_set_query_full_scan()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 */
static int
diag_val_set_query_full_scan (int value, T_DIAG_VALUE_SETTYPE settype,
			      char *err_buf)
{
  int thread_index;
  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].query_full_scan += value;

  return 0;
}

/*
 * diag_val_set_conn_conn_req()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 */
static int
diag_val_set_conn_conn_req (int value, T_DIAG_VALUE_SETTYPE settype,
			    char *err_buf)
{
  int thread_index;
  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].conn_conn_req += value;

  return 0;
}

/*
 * diag_val_set_conn_conn_reject()
 *    return: int
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 */
static int
diag_val_set_conn_conn_reject (int value, T_DIAG_VALUE_SETTYPE settype,
			       char *err_buf)
{
  int thread_index;
  CHECK_SHM ();
  CHECK_DIAG_OBJ_FUNC_THREAD_MGR_INIT (err_buf);
  thread_index = thread_get_current_entry_index ();

  g_ShmServer->thread[thread_index].conn_conn_reject += value;

  return 0;
}

/* Interface function */
/*
 * init_diag_mgr()
 *    return: bool
 *    server_name(in):
 *    num_thread(in):
 *    err_buf(in):
 */
bool
init_diag_mgr (const char *server_name, int num_thread, char *err_buf)
{
  if (init_diag_sm (server_name, num_thread, err_buf) == false)
    return false;

  return true;
}

/*
 * close_diag_mgr()
 *    return: none
 */
void
close_diag_mgr (void)
{
  rm_diag_sm ();
}

/*
 * set_diag_value() -
 *    return: bool
 *    type(in):
 *    value(in):
 *    settype(in):
 *    err_buf(in):
 */
bool
set_diag_value (T_DIAG_OBJ_TYPE type, int value, T_DIAG_VALUE_SETTYPE settype,
		char *err_buf)
{
  T_DO_FUNC task_func;

  if (diag_executediag == false)
    return false;

  task_func = diag_obj_list[type].func;

  if (task_func (value, settype, err_buf) < 0)
    {
      return false;
    }
  else
    {
      return true;
    }
}
#endif /* SERVER_MODE */
#endif /* DIAG_DEVEL */

static const char *mnt_Stats_name[MNT_SIZE_OF_SERVER_EXEC_STATS] = {
  "Num_file_creates",
  "Num_file_removes",
  "Num_file_ioreads",
  "Num_file_iowrites",
  "Num_file_iosynches",
  "Num_data_page_fetches",
  "Num_data_page_dirties",
  "Num_data_page_ioreads",
  "Num_data_page_iowrites",
  "Num_data_page_victims",
  "Num_data_page_iowrites_for_replacement",
  "Num_log_page_ioreads",
  "Num_log_page_iowrites",
  "Num_log_append_records",
  "Num_log_archives",
  "Num_log_checkpoints",
  "Num_log_wals",
  "Num_page_locks_acquired",
  "Num_object_locks_acquired",
  "Num_page_locks_converted",
  "Num_object_locks_converted",
  "Num_page_locks_re-requested",
  "Num_object_locks_re-requested",
  "Num_page_locks_waits",
  "Num_object_locks_waits",
  "Num_tran_commits",
  "Num_tran_rollbacks",
  "Num_tran_savepoints",
  "Num_tran_start_topops",
  "Num_tran_end_topops",
  "Num_tran_interrupts",
  "Num_btree_inserts",
  "Num_btree_deletes",
  "Num_btree_updates",
  "Num_query_selects",
  "Num_query_inserts",
  "Num_query_deletes",
  "Num_query_updates",
  "Num_query_sscans",
  "Num_query_iscans",
  "Num_query_lscans",
  "Num_query_setscans",
  "Num_query_methscans",
  "Num_query_nljoins",
  "Num_query_mjoins",
  "Num_query_objfetches",
  "Num_network_requests",
  "Num_adaptive_flush_pages",
  "Num_adaptive_flush_log_pages",
  "Num_adaptive_flush_max_pages",
  "Data_page_buffer_hit_ratio"
};

#if defined(SERVER_MODE) || defined(SA_MODE)
int mnt_Num_tran_exec_stats = 0;

/* Server execution statistics on each transactions */
struct mnt_server_table
{
  int num_tran_indices;
  MNT_SERVER_EXEC_STATS **stats;
  MNT_SERVER_EXEC_GLOBAL_STATS global_stats;
#if defined (SERVER_MODE)
  MUTEX_T lock;
#endif				/* SERVER_MODE */
};
static struct mnt_server_table mnt_Server_table = {
  /* num_tran_indices */
  0,
  /* stats */
  NULL,
  /* global_stats */
  {
   /* file io */
   0, 0, 0, 0, 0,
   /* page buffer manager */
   0, 0, 0, 0, 0, 0,
   /* log manager */
   0, 0, 0, 0, 0, 0,
   /* lock manager */
   0, 0, 0, 0, 0, 0, 0, 0,
   /* transactions */
   0, 0, 0, 0, 0, 0,
   /* btree manager */
   0, 0, 0,
   /* query manager */
   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
   /* network communication */
   0,
   /* flush control statistics */
   0, 0, 0,
   /* other statistics */
   0}
#if defined (SERVER_MODE)
  , MUTEX_INITIALIZER
#endif /* SERVER_MODE */
};


/*
 * mnt_server_init - Initialize monitoring resources in the server
 *   return: NO_ERROR or ER_FAILED
 *   num_tran_indices(in): maximum number of know transaction indices
 */
int
mnt_server_init (int num_tran_indices)
{
  MNT_SERVER_EXEC_STATS **stats;
  size_t size;
  int i;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  size = num_tran_indices * sizeof (*stats);

  MUTEX_LOCK (rv, mnt_Server_table.lock);
  if (mnt_Server_table.stats != NULL)
    {
      MUTEX_UNLOCK (mnt_Server_table.lock);
      return ER_FAILED;
    }

  mnt_Server_table.stats = malloc (size);
  if (mnt_Server_table.stats == NULL)
    {
      MUTEX_UNLOCK (mnt_Server_table.lock);
      return ER_FAILED;
    }

  for (i = 0; i < num_tran_indices; i++)
    {
      mnt_Server_table.stats[i] = NULL;
    }
  mnt_Server_table.num_tran_indices = num_tran_indices;
  mnt_Num_tran_exec_stats = 0;
  MUTEX_UNLOCK (mnt_Server_table.lock);

  return NO_ERROR;
}

/*
 * mnt_server_final - Terminate monitoring resources in the server
 *   return: none
 */
void
mnt_server_final (void)
{
  void *table;
  int i;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  MUTEX_LOCK (rv, mnt_Server_table.lock);
  if (mnt_Server_table.stats != NULL)
    {
      table = mnt_Server_table.stats;
      for (i = 0; i < mnt_Server_table.num_tran_indices; i++)
	{
	  if (mnt_Server_table.stats[i] != NULL)
	    free_and_init (mnt_Server_table.stats[i]);
	}
      mnt_Server_table.stats = NULL;
      mnt_Server_table.num_tran_indices = 0;
      mnt_Num_tran_exec_stats = 0;
      free_and_init (table);
    }
  MUTEX_UNLOCK (mnt_Server_table.lock);
}


/*
 * xmnt_server_start_stats - Start collecting server execution statistics
 *                           for the current transaction index
 *   return: NO_ERROR or ER_FAILED
 */
int
xmnt_server_start_stats (THREAD_ENTRY * thread_p, bool for_all_trans)
{
  int i, tran_index;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  assert (tran_index >= 0);

  MUTEX_LOCK (rv, mnt_Server_table.lock);

  if (for_all_trans == false)
    {
      /* Get an statistic block for current transaction if one is not available */
      if (mnt_Server_table.num_tran_indices > tran_index)
	{
	  if (mnt_Server_table.stats[tran_index] == NULL)
	    {
	      mnt_Server_table.stats[tran_index] =
		malloc (sizeof (MNT_SERVER_EXEC_STATS));
	      if (mnt_Server_table.stats[tran_index] == NULL)
		{
		  MUTEX_UNLOCK (mnt_Server_table.lock);
		  return ER_FAILED;
		}
	      MUTEX_INIT (mnt_Server_table.stats[tran_index]->lock);
	    }
	}

      if (mnt_Server_table.stats[0] == NULL)
	{
	  mnt_Server_table.stats[0] = malloc (sizeof (MNT_SERVER_EXEC_STATS));
	  if (mnt_Server_table.stats[0] == NULL)
	    {
	      MUTEX_UNLOCK (mnt_Server_table.lock);
	      return ER_FAILED;
	    }
	  MUTEX_INIT (mnt_Server_table.stats[0]->lock);
	  mnt_server_reset_stats_internal (mnt_Server_table.stats[0]);
	}

      /* Restart the statistics */
      mnt_server_reset_stats_internal (mnt_Server_table.stats[tran_index]);
    }
  else
    {
      for (i = 0; i < mnt_Server_table.num_tran_indices; i++)
	{
	  if (mnt_Server_table.stats[i] != NULL)
	    {
	      continue;
	    }

	  mnt_Server_table.stats[i] =
	    (MNT_SERVER_EXEC_STATS *) malloc (sizeof (MNT_SERVER_EXEC_STATS));
	  if (mnt_Server_table.stats[i] == NULL)
	    {
	      MUTEX_UNLOCK (mnt_Server_table.lock);
	      return ER_FAILED;
	    }

	  MUTEX_INIT (mnt_Server_table.stats[i]->lock);
	  mnt_server_reset_stats_internal (mnt_Server_table.stats[i]);
	}
    }

  mnt_Num_tran_exec_stats++;

  MUTEX_UNLOCK (mnt_Server_table.lock);

  return NO_ERROR;
}

/*
 * xmnt_server_stop_stats - Stop collecting server execution statistics
 *                          for the current transaction index
 *   return: none
 */
void
xmnt_server_stop_stats (THREAD_ENTRY * thread_p)
{
  int tran_index;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  assert (tran_index >= 0);

  MUTEX_LOCK (rv, mnt_Server_table.lock);
  if (mnt_Server_table.num_tran_indices > tran_index
      && mnt_Server_table.stats[tran_index] != NULL)
    {
      MUTEX_DESTROY (mnt_Server_table.stats[tran_index]->lock);
      free_and_init (mnt_Server_table.stats[tran_index]);
      mnt_Num_tran_exec_stats--;
    }
  MUTEX_UNLOCK (mnt_Server_table.lock);
}

/*
 * mnt_server_reflect_local_stats - Reflect the recorded server statistics
 *				    to the global server statistics
 */
void
mnt_server_reflect_local_stats (THREAD_ENTRY * thread_p)
{
  int tran_index;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
  MNT_SERVER_EXEC_STATS *p = NULL;
  MNT_SERVER_EXEC_GLOBAL_STATS *global_stats;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  assert (tran_index >= 0);

  MUTEX_LOCK (rv, mnt_Server_table.lock);
  if (mnt_Server_table.num_tran_indices > tran_index)
    {
      p = mnt_Server_table.stats[tran_index];
    }

  if (p == NULL)
    {
      MUTEX_UNLOCK (mnt_Server_table.lock);
      return;
    }

  MUTEX_LOCK (rv, p->lock);

  global_stats = &mnt_Server_table.global_stats;
  global_stats->file_num_creates += p->file_num_creates;
  global_stats->file_num_removes += p->file_num_removes;
  global_stats->file_num_ioreads += p->file_num_ioreads;
  global_stats->file_num_iowrites += p->file_num_iowrites;
  global_stats->file_num_iosynches += p->file_num_iosynches;

  global_stats->pb_num_fetches += p->pb_num_fetches;
  global_stats->pb_num_dirties += p->pb_num_dirties;
  global_stats->pb_num_ioreads += p->pb_num_ioreads;
  global_stats->pb_num_iowrites += p->pb_num_iowrites;
  global_stats->pb_num_victims += p->pb_num_victims;
  global_stats->pb_num_replacements += p->pb_num_replacements;

  global_stats->fc_num_pages += p->fc_num_pages;
  global_stats->fc_num_log_pages += p->fc_num_log_pages;
  global_stats->fc_tokens += p->fc_tokens;

  global_stats->log_num_ioreads += p->log_num_ioreads;
  global_stats->log_num_iowrites += p->log_num_iowrites;
  global_stats->log_num_appendrecs += p->log_num_appendrecs;
  global_stats->log_num_archives += p->log_num_archives;
  global_stats->log_num_checkpoints += p->log_num_checkpoints;
  global_stats->log_num_wals += p->log_num_wals;

  global_stats->lk_num_acquired_on_pages += p->lk_num_acquired_on_pages;
  global_stats->lk_num_acquired_on_objects += p->lk_num_acquired_on_objects;
  global_stats->lk_num_converted_on_pages += p->lk_num_converted_on_pages;
  global_stats->lk_num_converted_on_objects += p->lk_num_converted_on_objects;
  global_stats->lk_num_re_requested_on_pages +=
    p->lk_num_re_requested_on_pages;
  global_stats->lk_num_re_requested_on_objects +=
    p->lk_num_re_requested_on_objects;
  global_stats->lk_num_waited_on_pages += p->lk_num_waited_on_pages;
  global_stats->lk_num_waited_on_objects += p->lk_num_waited_on_objects;

  global_stats->tran_num_commits += p->tran_num_commits;
  global_stats->tran_num_rollbacks += p->tran_num_rollbacks;
  global_stats->tran_num_savepoints += p->tran_num_savepoints;
  global_stats->tran_num_start_topops += p->tran_num_start_topops;
  global_stats->tran_num_end_topops += p->tran_num_end_topops;
  global_stats->tran_num_interrupts += p->tran_num_interrupts;

  global_stats->bt_num_inserts += p->bt_num_inserts;
  global_stats->bt_num_deletes += p->bt_num_deletes;
  global_stats->bt_num_updates += p->bt_num_updates;

  global_stats->qm_num_selects += p->qm_num_selects;
  global_stats->qm_num_inserts += p->qm_num_inserts;
  global_stats->qm_num_deletes += p->qm_num_deletes;
  global_stats->qm_num_updates += p->qm_num_updates;
  global_stats->qm_num_sscans += p->qm_num_sscans;
  global_stats->qm_num_iscans += p->qm_num_iscans;
  global_stats->qm_num_lscans += p->qm_num_lscans;
  global_stats->qm_num_setscans += p->qm_num_setscans;
  global_stats->qm_num_methscans += p->qm_num_methscans;
  global_stats->qm_num_nljoins += p->qm_num_nljoins;
  global_stats->qm_num_mjoins += p->qm_num_mjoins;
  global_stats->qm_num_objfetches += p->qm_num_objfetches;

  global_stats->net_num_requests += p->net_num_requests;

  mnt_server_calc_stats (p);
  mnt_server_check_stats_threshold (LOG_FIND_THREAD_TRAN_INDEX (thread_p), p);
  mnt_server_reset_stats_internal (p);

  MUTEX_UNLOCK (p->lock);

  mnt_server_calc_global_stats (global_stats);

  MUTEX_UNLOCK (mnt_Server_table.lock);
}

/*
 * mnt_server_get_stats - Get the recorded server statistics for the current
 *                        transaction index
 */
MNT_SERVER_EXEC_STATS *
mnt_server_get_stats (THREAD_ENTRY * thread_p)
{
  int tran_index;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */
  MNT_SERVER_EXEC_STATS *p;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  assert (tran_index >= 0);

  MUTEX_LOCK (rv, mnt_Server_table.lock);
  if (mnt_Server_table.num_tran_indices > tran_index)
    {
      p = mnt_Server_table.stats[tran_index];
    }
  else
    {
      p = NULL;
    }
  MUTEX_UNLOCK (mnt_Server_table.lock);

  return p;
}

/*
 * mnt_server_check_stats_threshold -
 */
static void
mnt_server_check_stats_threshold (int tran_index,
				  MNT_SERVER_EXEC_STATS * stats)
{
  unsigned int i, size;
  unsigned int *stats_ptr;
  int *prm_ptr;

  if (PRM_MNT_STATS_THRESHOLD)
    {
      size = (unsigned int) PRM_MNT_STATS_THRESHOLD[0];
      size = MIN (size, MNT_SIZE_OF_SERVER_EXEC_STATS - 1);
      stats_ptr = (unsigned int *) stats;
      prm_ptr = (int *) &PRM_MNT_STATS_THRESHOLD[1];

      for (i = 0; i < size; i++)
	{
	  if (*prm_ptr > 0 && (unsigned int) *prm_ptr < *stats_ptr)
	    {
	      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
		      ER_MNT_STATS_THRESHOLD, 2, tran_index,
		      mnt_Stats_name[i]);
	    }
	  stats_ptr++, prm_ptr++;
	}
    }
}

/*
 * xmnt_server_copy_stats - Copy recorded server statistics for the current
 *                          transaction index
 *   return: none
 *   to_stats(out): buffer to copy
 */
void
xmnt_server_copy_stats (THREAD_ENTRY * thread_p,
			MNT_SERVER_EXEC_STATS * to_stats)
{
  MNT_SERVER_EXEC_STATS *from_stats;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  from_stats = mnt_server_get_stats (thread_p);
  if (from_stats == NULL)
    {
      mnt_server_reset_stats_internal (to_stats);
    }
  else
    {
      MUTEX_LOCK (rv, from_stats->lock);
      mnt_server_calc_stats (from_stats);
      *to_stats = *from_stats;	/* Structure copy */
      MUTEX_UNLOCK (from_stats->lock);
    }
}

/*
 * xmnt_server_copy_global_stats - Copy recorded system wide statistics
 *   return: none
 *   to_stats(out): buffer to copy
 */
void
xmnt_server_copy_global_stats (THREAD_ENTRY * thread_p,
			       MNT_SERVER_EXEC_GLOBAL_STATS * to_stats)
{
  MNT_SERVER_EXEC_STATS *p;
  MNT_SERVER_EXEC_GLOBAL_STATS *global_stats;
  int i;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  MUTEX_LOCK (rv, mnt_Server_table.lock);

  global_stats = &mnt_Server_table.global_stats;
  for (i = 0; i < mnt_Server_table.num_tran_indices; i++)
    {
      p = mnt_Server_table.stats[i];
      if (p == NULL)
	{
	  continue;
	}

      MUTEX_LOCK (rv, p->lock);

      global_stats->file_num_creates += p->file_num_creates;
      global_stats->file_num_removes += p->file_num_removes;
      global_stats->file_num_ioreads += p->file_num_ioreads;
      global_stats->file_num_iowrites += p->file_num_iowrites;
      global_stats->file_num_iosynches += p->file_num_iosynches;

      global_stats->pb_num_fetches += p->pb_num_fetches;
      global_stats->pb_num_dirties += p->pb_num_dirties;
      global_stats->pb_num_ioreads += p->pb_num_ioreads;
      global_stats->pb_num_iowrites += p->pb_num_iowrites;
      global_stats->pb_num_victims += p->pb_num_victims;
      global_stats->pb_num_replacements += p->pb_num_replacements;

      global_stats->fc_num_pages += p->fc_num_pages;
      global_stats->fc_num_log_pages += p->fc_num_log_pages;
      global_stats->fc_tokens += p->fc_tokens;

      global_stats->log_num_ioreads += p->log_num_ioreads;
      global_stats->log_num_iowrites += p->log_num_iowrites;
      global_stats->log_num_appendrecs += p->log_num_appendrecs;
      global_stats->log_num_archives += p->log_num_archives;
      global_stats->log_num_checkpoints += p->log_num_checkpoints;
      global_stats->log_num_wals += p->log_num_wals;

      global_stats->lk_num_acquired_on_pages += p->lk_num_acquired_on_pages;
      global_stats->lk_num_acquired_on_objects +=
	p->lk_num_acquired_on_objects;
      global_stats->lk_num_converted_on_pages += p->lk_num_converted_on_pages;
      global_stats->lk_num_converted_on_objects +=
	p->lk_num_converted_on_objects;
      global_stats->lk_num_re_requested_on_pages +=
	p->lk_num_re_requested_on_pages;
      global_stats->lk_num_re_requested_on_objects +=
	p->lk_num_re_requested_on_objects;
      global_stats->lk_num_waited_on_pages += p->lk_num_waited_on_pages;
      global_stats->lk_num_waited_on_objects += p->lk_num_waited_on_objects;

      global_stats->tran_num_commits += p->tran_num_commits;
      global_stats->tran_num_rollbacks += p->tran_num_rollbacks;
      global_stats->tran_num_savepoints += p->tran_num_savepoints;
      global_stats->tran_num_start_topops += p->tran_num_start_topops;
      global_stats->tran_num_end_topops += p->tran_num_end_topops;
      global_stats->tran_num_interrupts += p->tran_num_interrupts;

      global_stats->bt_num_inserts += p->bt_num_inserts;
      global_stats->bt_num_deletes += p->bt_num_deletes;
      global_stats->bt_num_updates += p->bt_num_updates;

      global_stats->qm_num_selects += p->qm_num_selects;
      global_stats->qm_num_inserts += p->qm_num_inserts;
      global_stats->qm_num_deletes += p->qm_num_deletes;
      global_stats->qm_num_updates += p->qm_num_updates;
      global_stats->qm_num_sscans += p->qm_num_sscans;
      global_stats->qm_num_iscans += p->qm_num_iscans;
      global_stats->qm_num_lscans += p->qm_num_lscans;
      global_stats->qm_num_setscans += p->qm_num_setscans;
      global_stats->qm_num_methscans += p->qm_num_methscans;
      global_stats->qm_num_nljoins += p->qm_num_nljoins;
      global_stats->qm_num_mjoins += p->qm_num_mjoins;
      global_stats->qm_num_objfetches += p->qm_num_objfetches;

      global_stats->net_num_requests += p->net_num_requests;

      mnt_server_calc_stats (p);
      mnt_server_check_stats_threshold (i, p);
      mnt_server_reset_stats_internal (p);

      MUTEX_UNLOCK (p->lock);
    }
  mnt_server_calc_global_stats (global_stats);

  *to_stats = *global_stats;	/* Structure copy */

  MUTEX_UNLOCK (mnt_Server_table.lock);
}

/*
 * xmnt_server_reset_stats - Reset the server statistics for current
 *                           transaction index
 *   return: none
 */
void
xmnt_server_reset_stats (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      MUTEX_LOCK (rv, stats->lock);
      mnt_server_check_stats_threshold (LOG_FIND_THREAD_TRAN_INDEX (thread_p),
					stats);
      mnt_server_reset_stats_internal (stats);
      MUTEX_UNLOCK (stats->lock);
    }
}

/*
 * mnt_server_reset_stats_internal - Reset server statistics of given block
 *   return: none
 *   stats(in/out): server statistics block to reset
 */
static void
mnt_server_reset_stats_internal (MNT_SERVER_EXEC_STATS * stats)
{
  stats->file_num_creates = 0;
  stats->file_num_removes = 0;
  stats->file_num_ioreads = 0;
  stats->file_num_iowrites = 0;
  stats->file_num_iosynches = 0;

  stats->pb_num_fetches = 0;
  stats->pb_num_dirties = 0;
  stats->pb_num_ioreads = 0;
  stats->pb_num_iowrites = 0;
  stats->pb_num_victims = 0;
  stats->pb_num_replacements = 0;

  stats->fc_num_pages = 0;
  stats->fc_num_log_pages = 0;
  stats->fc_tokens = 0;

  stats->log_num_ioreads = 0;
  stats->log_num_iowrites = 0;
  stats->log_num_appendrecs = 0;
  stats->log_num_archives = 0;
  stats->log_num_checkpoints = 0;
  stats->log_num_wals = 0;

  stats->lk_num_acquired_on_pages = 0;
  stats->lk_num_acquired_on_objects = 0;
  stats->lk_num_converted_on_pages = 0;
  stats->lk_num_converted_on_objects = 0;
  stats->lk_num_re_requested_on_pages = 0;
  stats->lk_num_re_requested_on_objects = 0;
  stats->lk_num_waited_on_pages = 0;
  stats->lk_num_waited_on_objects = 0;

  stats->tran_num_commits = 0;
  stats->tran_num_rollbacks = 0;
  stats->tran_num_savepoints = 0;
  stats->tran_num_start_topops = 0;
  stats->tran_num_end_topops = 0;
  stats->tran_num_interrupts = 0;

  stats->bt_num_inserts = 0;
  stats->bt_num_deletes = 0;
  stats->bt_num_updates = 0;

  stats->qm_num_selects = 0;
  stats->qm_num_inserts = 0;
  stats->qm_num_deletes = 0;
  stats->qm_num_updates = 0;
  stats->qm_num_sscans = 0;
  stats->qm_num_iscans = 0;
  stats->qm_num_lscans = 0;
  stats->qm_num_setscans = 0;
  stats->qm_num_methscans = 0;
  stats->qm_num_nljoins = 0;
  stats->qm_num_mjoins = 0;
  stats->qm_num_objfetches = 0;

  stats->net_num_requests = 0;

  stats->pb_hit_ratio = 0;
}

/*
 * mnt_server_calc_stats - Do post processing of server statistics
 *   return: none
 *   stats(in/out): server statistics block to be processed
 */
static void
mnt_server_calc_stats (MNT_SERVER_EXEC_STATS * stats)
{
  stats->pb_hit_ratio =
    stats->pb_num_fetches == 0 ? 0 :
    (stats->pb_num_fetches - stats->pb_num_ioreads) * 100 * 100
    / stats->pb_num_fetches;
}

/*
 * mnt_server_calc_global_stats - Do post processing of server statistics
 *   return: none
 *   stats(in/out): server statistics block to be processed
 */
static void
mnt_server_calc_global_stats (MNT_SERVER_EXEC_GLOBAL_STATS * stats)
{
  stats->pb_hit_ratio =
    stats->pb_num_fetches == 0 ? 0 :
    (stats->pb_num_fetches - stats->pb_num_ioreads) * 100 * 100
    / stats->pb_num_fetches;
}

/*
 * xmnt_server_reset_global_stats - Reset the server global statistics
 *   return: none
 */
void
xmnt_server_reset_global_stats (void)
{
  MNT_SERVER_EXEC_GLOBAL_STATS *global_stats;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  global_stats = &mnt_Server_table.global_stats;

  MUTEX_LOCK (rv, mnt_Server_table.lock);

  global_stats->file_num_creates = 0;
  global_stats->file_num_removes = 0;
  global_stats->file_num_ioreads = 0;
  global_stats->file_num_iowrites = 0;
  global_stats->file_num_iosynches = 0;

  global_stats->pb_num_fetches = 0;
  global_stats->pb_num_dirties = 0;
  global_stats->pb_num_ioreads = 0;
  global_stats->pb_num_iowrites = 0;
  global_stats->pb_num_victims = 0;
  global_stats->pb_num_replacements = 0;

  global_stats->fc_num_pages = 0;
  global_stats->fc_num_log_pages = 0;
  global_stats->fc_tokens = 0;

  global_stats->log_num_ioreads = 0;
  global_stats->log_num_iowrites = 0;
  global_stats->log_num_appendrecs = 0;
  global_stats->log_num_archives = 0;
  global_stats->log_num_checkpoints = 0;
  global_stats->log_num_wals = 0;

  global_stats->lk_num_acquired_on_pages = 0;
  global_stats->lk_num_acquired_on_objects = 0;
  global_stats->lk_num_converted_on_pages = 0;
  global_stats->lk_num_converted_on_objects = 0;
  global_stats->lk_num_re_requested_on_pages = 0;
  global_stats->lk_num_re_requested_on_objects = 0;
  global_stats->lk_num_waited_on_pages = 0;
  global_stats->lk_num_waited_on_objects = 0;

  global_stats->tran_num_commits = 0;
  global_stats->tran_num_rollbacks = 0;
  global_stats->tran_num_savepoints = 0;
  global_stats->tran_num_start_topops = 0;
  global_stats->tran_num_end_topops = 0;
  global_stats->tran_num_interrupts = 0;

  global_stats->bt_num_inserts = 0;
  global_stats->bt_num_deletes = 0;
  global_stats->bt_num_updates = 0;

  global_stats->qm_num_selects = 0;
  global_stats->qm_num_inserts = 0;
  global_stats->qm_num_deletes = 0;
  global_stats->qm_num_updates = 0;
  global_stats->qm_num_sscans = 0;
  global_stats->qm_num_iscans = 0;
  global_stats->qm_num_lscans = 0;
  global_stats->qm_num_setscans = 0;
  global_stats->qm_num_methscans = 0;
  global_stats->qm_num_nljoins = 0;
  global_stats->qm_num_mjoins = 0;
  global_stats->qm_num_objfetches = 0;

  global_stats->net_num_requests = 0;

  global_stats->pb_hit_ratio = 0;

  MUTEX_UNLOCK (mnt_Server_table.lock);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * enclosing_method - Print server statistics for current transaction index
 *   return: none
 *   stream(in): if NULL is given, stdout is used
 */
void
mnt_server_print_stats (THREAD_ENTRY * thread_p, FILE * stream)
{
  MNT_SERVER_EXEC_STATS *stats;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  stats = mnt_server_get_stats (thread_p);
  if (stats == NULL)
    {
      return;
    }
  MUTEX_LOCK (rv, stats->lock);
  mnt_server_dump_stats (stats, stream);
  MUTEX_UNLOCK (stats->lock);
}
#endif

/*
 * mnt_x_file_creates - Increase file_num_creates counter of the current
 *                      transaction index
 *   return: none
 */
void
mnt_x_file_creates (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->file_num_creates++;
    }
}

/*
 * mnt_x_file_removes - Increase file_num_remvoes counter of the current
 *                      transaction index
 *   return: none
 */
void
mnt_x_file_removes (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->file_num_removes++;
    }
}

/*
 * mnt_x_file_ioreads - Increase file_num_ioreads counter of the current
 *                    transaction index
 *   return: none
 */
void
mnt_x_file_ioreads (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->file_num_ioreads++;
    }
}

/*
 * mnt_x_file_iowrites - Increase file_num_iowrites counter of the current
 *                    transaction index
 *   return: none
 */
void
mnt_x_file_iowrites (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->file_num_iowrites++;
    }
}

/*
 * mnt_x_file_iosynches - Increase file_num_iosynches counter of the current
 *                    transaction index
 *   return: none
 */
void
mnt_x_file_iosynches (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->file_num_iosynches++;
    }
}

/*
 * mnt_x_pb_fetches - Increase pb_num_fetches counter of the current
 *                    transaction index
 *   return: none
 */
void
mnt_x_pb_fetches (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->pb_num_fetches++;
    }
}

/*
 * mnt_x_pb_dirties - Increase pb_num_dirties counter of the current
 *                    transaction index
 *   return: none
 */
void
mnt_x_pb_dirties (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->pb_num_dirties++;
    }
}

/*
 * mnt_x_pb_ioreads - Increase pb_num_ioreads counter of the current
 *                    transaction index
 *   return: none
 */
void
mnt_x_pb_ioreads (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->pb_num_ioreads++;
    }
}

/*
 * mnt_x_pb_iowrites - Increase pb_num_iowrites counter of the current
 *                     transaction index
 *   return: none
 */
void
mnt_x_pb_iowrites (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->pb_num_iowrites++;
    }
}

/*
 * mnt_x_pb_victims - Increase pb_num_victims counter of the current
 *                     transaction index
 *   return: none
 */
void
mnt_x_pb_victims (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->pb_num_victims++;
    }
}

/*
 * mnt_x_pb_replacements - Increase page replacement counter of the current
 *                     transaction index
 *   return: none
 */
void
mnt_x_pb_replacements (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->pb_num_replacements++;
    }
}

/*
 * mnt_x_fc_stats -
 *   return: none
 */
void
mnt_x_fc_stats (THREAD_ENTRY * thread_p, unsigned int num_pages,
		unsigned int num_log_pages, unsigned int tokens)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      stats->fc_num_pages += num_pages;
      stats->fc_num_log_pages += num_log_pages;
      stats->fc_tokens += tokens;
    }
}

/*
 * mnt_x_log_ioreads - Increase pb_num_ioreads counter of the current
 *                     transaction index
 *   return: none
 */
void
mnt_x_log_ioreads (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->log_num_ioreads++;
    }
}

/*
 * mnt_x_log_iowrites - Increase log_num_iowrites counter of the current
 *                      transaction index
 *   return: none
 */
void
mnt_x_log_iowrites (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->log_num_iowrites++;
    }
}

/*
 * mnt_x_log_appendrecs - Increase log_num_appendrecs counter of the current
 *                        transaction index
 *   return: none
 */
void
mnt_x_log_appendrecs (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->log_num_appendrecs++;
    }
}

/*
 * mnt_x_log_archives - Increase log_num_archives counter of the current
 *                      transaction index
 *   return: none
 */
void
mnt_x_log_archives (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->log_num_archives++;
    }
}

/*
 * mnt_x_log_checkpoints - Increase log_num_checkpoints counter of the current
 *                      transaction index
 *   return: none
 */
void
mnt_x_log_checkpoints (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->log_num_checkpoints++;
    }
}

/*
 * mnt_x_log_wals - Increase log flush for wal counter of the current
 *                      transaction index
 *   return: none
 */
void
mnt_x_log_wals (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->log_num_wals++;
    }
}

/*
 * mnt_x_lk_acquired_on_pages - Increase lk_num_acquired_on_pages counter
 *                              of the current transaction index
 *   return: none
 */
void
mnt_x_lk_acquired_on_pages (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->lk_num_acquired_on_pages++;
    }
}

/*
 * mnt_x_lk_acquired_on_objects - Increase lk_num_acquired_on_objects counter
 *                                of the current transaction index
 *   return: none
 */
void
mnt_x_lk_acquired_on_objects (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->lk_num_acquired_on_objects++;
    }
}

/*
 * mnt_x_lk_converted_on_pages - Increase lk_num_converted_on_pages counter
 *                               of the current transaction index
 *   return: none
 */
void
mnt_x_lk_converted_on_pages (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->lk_num_converted_on_pages++;
    }
}

/*
 * mnt_x_lk_converted_on_objects - Increase lk_num_converted_on_objects
 *                                 counter of the current transaction index
 *   return: none
 */
void
mnt_x_lk_converted_on_objects (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->lk_num_converted_on_objects++;
    }
}

/*
 * mnt_x_lk_re_requested_on_pages - Increase lk_num_re_requested_on_pages
 *                                  counter of the current transaction index
 *   return: none
 */
void
mnt_x_lk_re_requested_on_pages (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->lk_num_re_requested_on_pages++;
    }
}

/*
 * mnt_x_lk_re_requested_on_objects - Increase lk_num_re_requested_on_objects
 *                                    counter of the current transaction index
 *   return: none
 */
void
mnt_x_lk_re_requested_on_objects (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->lk_num_re_requested_on_objects++;
    }
}

/*
 * mnt_x_lk_waited_on_pages - Increase lk_num_waited_on_pages counter of the
 *                            current transaction index
 *   return: none
 */
void
mnt_x_lk_waited_on_pages (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->lk_num_waited_on_pages++;
    }
}

/*
 * mnt_x_lk_waited_on_objects - Increase lk_num_waited_on_objects counter of
 *                              the current transaction index
 *   return: none
 */
void
mnt_x_lk_waited_on_objects (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->lk_num_waited_on_objects++;
    }
}

/*
 * mnt_x_tran_commits - Increase tran_num_commits counter of the current
 *                      transaction index
 *   return: none
 */
void
mnt_x_tran_commits (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->tran_num_commits++;
    }
}

/*
 * mnt_x_tran_rollbacks - Increase tran_num_rollbacks counter of the current
 *                        transaction index
 *   return: none
 */
void
mnt_x_tran_rollbacks (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->tran_num_rollbacks++;
    }
}

/*
 * mnt_x_tran_savepoints - Increase tran_num_savepoints counter of the current
 *                         transaction index
 *   return: none
 */
void
mnt_x_tran_savepoints (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->tran_num_savepoints++;
    }
}

/*
 * mnt_x_tran_start_topops - Increase tran_num_start_topops counter of the
 *                           current transaction index
 *   return: none
 */
void
mnt_x_tran_start_topops (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->tran_num_start_topops++;
    }
}

/*
 * mnt_x_tran_end_topops - Increase tran_num_end_topops counter of the current
 *                         transaction index
 *   return: none
 */
void
mnt_x_tran_end_topops (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->tran_num_end_topops++;
    }
}

/*
 * mnt_x_tran_interrupts - Increase tran_num_interrupts counter of the current
 *                         transaction index
 *   return: none
 */
void
mnt_x_tran_interrupts (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->tran_num_interrupts++;
    }
}

/*
 * mnt_x_bt_inserts - Increase bt_num_inserts counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_bt_inserts (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->bt_num_inserts++;
    }
}

/*
 * mnt_x_bt_deletes - Increase bt_num_deletes counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_bt_deletes (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->bt_num_deletes++;
    }
}

/*
 * mnt_x_bt_updates - Increase bt_num_updates counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_bt_updates (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->bt_num_updates++;
    }
}

/*
 * mnt_x_qm_selects - Increase qm_num_selects counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_qm_selects (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_selects++;
    }
}

/*
 * mnt_x_qm_inserts - Increase qm_num_inserts counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_qm_inserts (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_inserts++;
    }
}

/*
 * mnt_x_qm_deletes - Increase qm_num_deletes counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_qm_deletes (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_deletes++;
    }
}

/*
 * mnt_x_qm_updates - Increase qm_num_updates counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_qm_updates (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_updates++;
    }
}

/*
 * mnt_x_qm_sscans - Increase qm_num_sscans counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_qm_sscans (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_sscans++;
    }
}

/*
 * mnt_x_qm_iscans - Increase qm_num_iscans counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_qm_iscans (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_iscans++;
    }
}

/*
 * mnt_x_qm_lscans - Increase qm_num_lscans counter of the current
                     transaction index
 *   return: none
 */
void
mnt_x_qm_lscans (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_lscans++;
    }
}

/*
 * mnt_x_qm_setscans - Increase qm_num_setscans counter of the current
                       transaction index
 *   return: none
 */
void
mnt_x_qm_setscans (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_setscans++;
    }
}

/*
 * mnt_x_qm_methscans - Increase qm_num_methscans counter of the current
                        transaction index
 *   return: none
 */
void
mnt_x_qm_methscans (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_methscans++;
    }
}

/*
 * mnt_x_qm_nljoins - Increase qm_num_nljoins counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_qm_nljoins (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_nljoins++;
    }
}

/*
 * mnt_x_qm_mjoins - Increase qm_num_mjoins counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_qm_mjoins (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_mjoins++;
    }
}

/*
 * mnt_x_qm_objfetches - Increase qm_num_objfetches counter of the current
                      transaction index
 *   return: none
 */
void
mnt_x_qm_objfetches (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->qm_num_objfetches++;
    }
}

/*
 * mnt_x_net_requests - Increase net_num_requests counter of the current
 *                      transaction index
 *   return: none
 */
void
mnt_x_net_requests (THREAD_ENTRY * thread_p)
{
  MNT_SERVER_EXEC_STATS *stats;

  stats = mnt_server_get_stats (thread_p);
  if (stats != NULL)
    {
      /*
       * We will suffer lost update for these counters.
       * You may use atomic operation instead.
       */
      stats->net_num_requests++;
    }
}
#endif /* SERVER_MODE || SA_MODE */

/*
 * mnt_server_dump_stats - Print the given server statistics
 *   return: none
 *   stats(in) server statistics to print
 *   stream(in): if NULL is given, stdout is used
 */
void
mnt_server_dump_stats (const MNT_SERVER_EXEC_STATS * stats, FILE * stream)
{
  unsigned int i, *stats_ptr;

  if (stream == NULL)
    {
      stream = stdout;
    }

  fprintf (stream, "\n *** SERVER EXECUTION STATISTICS *** \n");

  stats_ptr = (unsigned int *) stats;
  for (i = 0; i < MNT_SIZE_OF_SERVER_EXEC_STATS - 1; i++)
    {
      if (mnt_Stats_name[i])
	{
	  fprintf (stream, "%-29s = %10u\n", mnt_Stats_name[i], *stats_ptr++);
	}
    }

  fprintf (stream, "\n *** OTHER STATISTICS *** \n");

  fprintf (stream, "Data_page_buffer_hit_ratio    = %10.2f\n",
	   (float) stats->pb_hit_ratio / 100);
}

/*
 * mnt_server_dump_global_stats - Print the given server global statistics
 *   return: none
 *   stats(in) server statistics to print
 *   stream(in): if NULL is given, stdout is used
 */
void
mnt_server_dump_global_stats (const MNT_SERVER_EXEC_GLOBAL_STATS * stats,
			      FILE * stream)
{
  unsigned int i;
  UINT64 *stats_ptr;

  if (stream == NULL)
    {
      stream = stdout;
    }

  fprintf (stream, "\n *** SERVER EXECUTION GLOBAL STATISTICS *** \n");

  stats_ptr = (UINT64 *) stats;
  for (i = 0; i < MNT_SIZE_OF_SERVER_EXEC_GLOBAL_STATS - 1; i++)
    {
      if (mnt_Stats_name[i])
	{
	  fprintf (stream, "%-29s = %10llu\n", mnt_Stats_name[i],
		   (unsigned long long) *stats_ptr++);
	}
    }

  fprintf (stream, "\n *** OTHER STATISTICS *** \n");

  fprintf (stream, "Data_page_buffer_hit_ratio    = %10.2f\n",
	   (float) stats->pb_hit_ratio / 100);
}

/*
 * mnt_get_current_times - Get current CPU and elapsed times
 *   return:
 *   cpu_user_time(out):
 *   cpu_sys_time(out):
 *   elapsed_time(out):
 *
 * Note:
 */
void
mnt_get_current_times (time_t * cpu_user_time, time_t * cpu_sys_time,
		       time_t * elapsed_time)
{
#if defined (WINDOWS)
  *cpu_user_time = 0;
  *cpu_sys_time = 0;
  *elapsed_time = 0;

  *elapsed_time = time (NULL);
#else /* WINDOWS */
  struct rusage rusage;

  *cpu_user_time = 0;
  *cpu_sys_time = 0;
  *elapsed_time = 0;

  *elapsed_time = time (NULL);

  if (getrusage (RUSAGE_SELF, &rusage) == 0)
    {
      *cpu_user_time = rusage.ru_utime.tv_sec;
      *cpu_sys_time = rusage.ru_stime.tv_sec;
    }
#endif /* WINDOWS */
}
