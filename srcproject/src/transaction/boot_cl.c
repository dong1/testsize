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
 * boot_cl.c - Boot management in the client
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#if !defined(WINDOWS)
#include <sys/time.h>
#endif /* WINDOWS */

#if !defined(WINDOWS)
#include <stdio.h>
#include <unistd.h>
#endif /* !WINDOWS */

#if defined(SOLARIS)
#include <netdb.h>		/* for MAXHOSTNAMELEN */
#endif /* SOLARIS */

#include <assert.h>

#if !defined(HPUX)
#include "util_func.h"
#endif /* !HPUX */
#include "boot_cl.h"
#include "memory_alloc.h"
#include "area_alloc.h"
#include "storage_common.h"
#include "oid.h"
#include "error_manager.h"
#include "work_space.h"
#include "schema_manager.h"
#include "authenticate.h"
#include "trigger_manager.h"
#include "db.h"
#if !defined(WINDOWS)
#include "dynamic_load.h"
#endif /* !WINDOWS */
#include "transaction_cl.h"
#include "log_comm.h"
#include "server_interface.h"
#include "release_string.h"
#include "system_parameter.h"
#include "locator_cl.h"
#include "databases_file.h"
#include "db_query.h"
#include "language_support.h"
#include "message_catalog.h"
#include "parser.h"
#include "perf_monitor.h"
#include "set_object.h"
#include "cnv.h"
#include "environment_variable.h"
#include "locator.h"
#include "transform.h"
#include "trigger_manager.h"
#include "jsp_cl.h"
#include "client_support.h"
#include "cluster_config.h"
#include "query_executor.h"

#if defined(CS_MODE)
#include "network.h"
#include "connection_cl.h"
#endif /* CS_MODE */
#include "network_interface_cl.h"

#if defined(WINDOWS)
#include "wintcp.h"
#endif /* WINDOWS */

#include "porting.h"

/* TODO : Move .h */
#if defined(SA_MODE)
extern bool catcls_Enable;
extern int catcls_compile_catalog_classes (THREAD_ENTRY * thread_p);
#endif /* SA_MODE */

#define BOOT_FORMAT_MAX_LENGTH 500

typedef int (*DEF_FUNCTION) ();
typedef int (*DEF_CLASS_FUNCTION) (MOP);

typedef struct catcls_function CATCLS_FUNCTION;
struct catcls_function
{
  const char *name;
  const DEF_FUNCTION function;
};

typedef struct column COLUMN;
struct column
{
  const char *name;
  const char *type;
};


static BOOT_SERVER_CREDENTIAL boot_Server_credential = {
  /* db_full_name */ NULL, /* host_name */ NULL, /* process_id */ -1,
  /* root_class_oid */ {NULL_PAGEID, NULL_SLOTID, NULL_VOLID},
  /* root_class_hfid */ {{NULL_FILEID, NULL_VOLID}, NULL_PAGEID},
  /* data page_size */ -1, /* log page_size */ -1,	/* disk_compatibility */
  0.0,
  /* ha_server_state */ -1
};

static const char *boot_Client_no_user_string = "(nouser)";
static const char *boot_Client_id_unknown_string = "(unknown)";

static char boot_Client_id_buffer[L_cuserid + 1];
static char boot_Db_path_buf[PATH_MAX];
static char boot_Log_path_buf[PATH_MAX];
static char boot_Db_host_buf[MAXHOSTNAMELEN + 1];

/* Volume assigned for new files/objects (e.g., heap files) */
VOLID boot_User_volid = 0;
#if defined(CS_MODE)
/* Server host connected */
char boot_Host_connected[MAXHOSTNAMELEN] = "";
#endif /* CS_MODE */
char boot_Host_name[MAXHOSTNAMELEN] = "";

static char boot_Volume_label[PATH_MAX] = " ";
static bool boot_Is_client_all_final = true;
static bool boot_Set_client_at_exit = false;
static int boot_Process_id = -1;

static int boot_client (int tran_index, int lock_wait,
			TRAN_ISOLATION tran_isolation);
static void boot_shutdown_client_at_exit (void);
#if defined(CS_MODE)
static int boot_client_initialize_css (DB_INFO * db, bool check_capabilities,
				       bool discriminative);
#endif /* CS_MODE */
static int boot_define_class (MOP class_mop);
static int boot_define_attribute (MOP class_mop);
static int boot_define_domain (MOP class_mop);
static int boot_define_method (MOP class_mop);
static int boot_define_meth_sig (MOP class_mop);
static int boot_define_meth_argument (MOP class_mop);
static int boot_define_meth_file (MOP class_mop);
static int boot_define_query_spec (MOP class_mop);
static int boot_define_index (MOP class_mop);
static int boot_define_index_key (MOP class_mop);
static int boot_define_class_authorization (MOP class_mop);
static int boot_define_partition (MOP class_mop);
static int boot_add_data_type (MOP class_mop);
static int boot_define_data_type (MOP class_mop);
static int boot_define_stored_procedure (MOP class_mop);
static int boot_define_stored_procedure_arguments (MOP class_mop);
static int boot_define_serial (MOP class_mop);
static int boot_define_ha_apply_info (MOP class_mop);
static int boot_define_cluster_node (MOP class_mop);
static int boot_define_view_class (void);
static int boot_define_view_super_class (void);
static int boot_define_view_vclass (void);
static int boot_define_view_attribute (void);
static int boot_define_view_attribute_set_domain (void);
static int boot_define_view_method (void);
static int boot_define_view_method_argument (void);
static int boot_define_view_method_argument_set_domain (void);
static int boot_define_view_method_file (void);
static int boot_define_view_index (void);
static int boot_define_view_index_key (void);
static int boot_define_view_authorization (void);
static int boot_define_view_trigger (void);
static int boot_define_view_partition (void);
static int boot_define_view_stored_procedure (void);
static int boot_define_view_stored_procedure_arguments (void);
static int catcls_class_install (void);
static int catcls_vclass_install (void);

/*
 * boot_client () -
 *
 * return :
 *
 *   tran_index(in) : transaction index
 *   lock_wait(in) :
 *   tran_isolation(in):
 *
 * Note: macros that find if the cubrid client is restarted
 */
static int
boot_client (int tran_index, int lock_wait, TRAN_ISOLATION tran_isolation)
{
  tran_cache_tran_settings (tran_index, (float) lock_wait, tran_isolation);

  if (boot_Set_client_at_exit)
    {
      return NO_ERROR;
    }

  boot_Set_client_at_exit = true;
  boot_Process_id = getpid ();
  atexit (boot_shutdown_client_at_exit);

  return NO_ERROR;
}

/*
 * boot_initialize_client () -
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   program_name(in) : Name of the program that started the system
 *   print_version(in) : Flag which indicates if the version of CUBRID is
 *                      printed at the end of the initialization process.
 *   db_name(in)      : Database Name
 *   db_path(in)      : Directory where the database is created. It allows you
 *                      to specify the exact pathname of a directory in which
  *                     to create the new database. If NULL is passed, the
 *                      current directory is used.
 *   log_path(in)     : Directory where the log and backups of the database are
 *                      created. We recommend placing log and backup in a
 *                      different directory and disk device from the directory
 *                      and disk device of the data volumes. If NULL is passed,
 *                      the value of the system parameter is used.
 *   server_host(in)  : Server host where the database will reside. The host is
 *                      needed in a client/server environment to identify the
 *                      server which will maintain (e.g., restart) the database
 *                      If NULL is given, the current host is used.
 *   db_overwrite(in) : Wheater to overwrite the database if it already exist.
 *   db_comments(in)  : Database creation comments such as name of the user who
 *                      created the database, the date of the creation,the name
 *                      of the intended application, or nothing at all. NULL
 *                      can be passed if no comments are desired.
 *   npages(in)       : Total number of pages to allocate for the database.
 *   file_addmore_vols(in): More volumes are created during the initialization
 *                      process.
 *   db_desired_pagesize(in): Desired pagesize for the new database.
 *                      The given size must be power of 2 and greater or
 *                      equal than 512.
 *   log_npages(in)   : Number of log pages. If log_npages <=0, default value
 *                      of system parameter is used.
 *
 * Note:
 *              The first step of any CUBRID application is to initialize a
 *              database. A database is composed of data volumes (or Unix file
 *              system files), database backup files, and log files. A data
 *              volume contains information on attributes, classes, indexes,
 *              and objects created in the database. A database backup is a
 *              fuzzy snapshot of the entire database. The backup is fuzzy
 *              since it can be taken online when other transactions are
 *              updating the database. The logs contain records that reflect
 *              changes to the database. The log and backup files are used by
 *              the system to recover committed and uncommitted transactions
 *              in the event of system and media crashes. Logs are also used
 *              to support user-initiated rollbacks. This function also
 *              initializes the database with built-in CUBRID classes.
 *
 *              The rest of this function is identical to the restart. The
 *              transaction for the current client session is automatically
 *              started.
 */
int
boot_initialize_client (BOOT_CLIENT_CREDENTIAL * client_credential,
			BOOT_DB_PATH_INFO * db_path_info,
			bool db_overwrite, const char *file_addmore_vols,
			DKNPAGES npages, PGLENGTH db_desired_pagesize,
			DKNPAGES log_npages,
			PGLENGTH db_desired_log_page_size)
{
  OID rootclass_oid;		/* Oid of root class */
  HFID rootclass_hfid;		/* Heap for classes */
  int tran_index;		/* Assigned transaction index */
  TRAN_ISOLATION tran_isolation;	/* Desired client Isolation level */
  int tran_lock_waitsecs;	/* Default lock waiting */
  unsigned int length;
  int error_code = NO_ERROR;
  DB_INFO *db;
  const char *hosts[2];
#if defined (CS_MODE)
  char format[BOOT_FORMAT_MAX_LENGTH];
#endif

  assert (client_credential != NULL);
  assert (db_path_info != NULL);

  /* If the client is restarted, shutdown the client */
  if (BOOT_IS_CLIENT_RESTARTED ())
    {
      (void) boot_shutdown_client (true);
    }

  if (!boot_Is_client_all_final)
    {
      boot_client_all_finalize (true);
    }

#if defined(WINDOWS)
  /* set up the WINDOWS stream emulations */
  pc_init ();
#endif /* WINDOWS */

  /*
   * initialize language parameters, if we can't access the CUBRID
   * environment variable, should return an appropriate error code even
   * if we can't actually print anything
   */
  (void) lang_init ();

  /* database name must be specified */
  if (client_credential->db_name == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_UNKNOWN_DATABASE, 1,
	      "(null)");
      return ER_BO_UNKNOWN_DATABASE;
    }

  /* open the system message catalog, before prm_ ?  */
  if (msgcat_init () != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_BO_CANNOT_ACCESS_MESSAGE_CATALOG, 0);
      return ER_BO_CANNOT_ACCESS_MESSAGE_CATALOG;
    }

  /* initialize system parameters */
  if (sysprm_load_and_init (client_credential->db_name, NULL) != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_BO_CANNOT_ACCESS_MESSAGE_CATALOG, 0);
      return ER_BO_CANNOT_ACCESS_MESSAGE_CATALOG;
    }

  /* initialize the "areas" memory manager */
  area_init (false);
  locator_initialize_areas ();

  (void) db_set_page_size (db_desired_pagesize, db_desired_log_page_size);

  /* If db_path and/or log_path are NULL find the defaults */

  if (db_path_info->db_path == NULL)
    {
      db_path_info->db_path = getcwd (boot_Db_path_buf, PATH_MAX);
      if (db_path_info->db_path == NULL)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_BO_CWD_FAIL, 0);
	  return ER_BO_CWD_FAIL;
	}
    }
  if (db_path_info->log_path == NULL)
    {
      /* Assign the data volume directory */
      strcpy (boot_Log_path_buf, db_path_info->db_path);
      db_path_info->log_path = boot_Log_path_buf;
    }

  /* Make sure that the full path for the database is not too long */
  length = strlen (client_credential->db_name)
    + strlen (db_path_info->db_path) + 2;
  if (length > (unsigned) PATH_MAX)
    {
      /* db_path + db_name is too long */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_BO_FULL_DATABASE_NAME_IS_TOO_LONG, 3, db_path_info->db_path,
	      client_credential->db_name, length, PATH_MAX);

      return ER_BO_FULL_DATABASE_NAME_IS_TOO_LONG;
    }

  /* If a host was not given, assume the current host */
  if (db_path_info->db_host == NULL)
    {
#if 0				/* use Unix-domain socket for localhost */
      if (GETHOSTNAME (db_host_buf, MAXHOSTNAMELEN) != 0)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_BO_UNABLE_TO_FIND_HOSTNAME, 0);
	  return ER_BO_UNABLE_TO_FIND_HOSTNAME;
	}
      db_host_buf[MAXHOSTNAMELEN] = '\0';
#else
      strcpy (boot_Db_host_buf, "localhost");
#endif
      db_path_info->db_host = boot_Db_host_buf;
    }

  /* make new DB_INFO */
  hosts[0] = db_path_info->db_host;
  hosts[1] = NULL;
  db = cfg_new_db (client_credential->db_name, db_path_info->db_path,
		   db_path_info->log_path, hosts);
  if (db == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_UNKNOWN_DATABASE, 1,
	      client_credential->db_name);
      return ER_BO_UNKNOWN_DATABASE;
    }

  /* Get the absolute path name */
  COMPOSE_FULL_NAME (boot_Volume_label, sizeof (boot_Volume_label),
		     db_path_info->db_path, client_credential->db_name);

  er_clear ();

  /* Get the user name */
  if (client_credential->db_user == NULL)
    {
      client_credential->db_user = au_user_name_dup ();
      if (client_credential->db_user == NULL)
	{
	  client_credential->db_user = (char *) boot_Client_no_user_string;
	}
    }
  /* Get the login name, host, and process identifier */
  if (client_credential->login_name == NULL)
    {
      if (getuserid (boot_Client_id_buffer, L_cuserid) != (char *) NULL)
	{
	  client_credential->login_name = boot_Client_id_buffer;
	}
      else
	{
	  client_credential->login_name =
	    (char *) boot_Client_id_unknown_string;
	}
    }

  if (client_credential->host_name == NULL)
    {
      if (boot_Host_name[0] == '\0')
	{
	  if (GETHOSTNAME (boot_Host_name, MAXHOSTNAMELEN) != 0)
	    {
	      strcpy (boot_Host_name, boot_Client_id_unknown_string);
	    }
	}
      client_credential->host_name = boot_Host_name;
    }

  /*
   * Initialize the dynamic loader. Don't care about failures. If dynamic
   * loader fails, methods will fail when they are invoked
   */
#if !defined(WINDOWS)
#if !defined (SOLARIS) && !defined(LINUX)
  (void) dl_initiate_module (client_credential->program_name);
#else /* !SOLARIS && !LINUX */
  (void) dl_initiate_module ();
#endif /* !SOLARIS && !LINUX */
#endif /* !WINDOWS */

#if defined(CS_MODE)
  /* Initialize the communication subsystem */
  error_code = boot_client_initialize_css (db, false, false);
  if (error_code != NO_ERROR)
    {
      cfg_free_directory (db);
      if (client_credential->db_user != boot_Client_no_user_string)
	{
	  free_and_init (client_credential->db_user);
	}
      return error_code;
    }
#endif /* CS_MODE */
  boot_User_volid = 0;
  tran_isolation = (TRAN_ISOLATION) PRM_LOG_ISOLATION_LEVEL;
  tran_lock_waitsecs = PRM_LK_TIMEOUT_SECS;

  /* this must be done before the init_server because recovery steps
   * may need domains.
   */
  tp_init ();

  /* Initialize the disk and the server part */
  tran_index = boot_initialize_server (client_credential, db_path_info,
				       db_overwrite, file_addmore_vols,
				       npages, db_desired_pagesize,
				       log_npages, db_desired_log_page_size,
				       &rootclass_oid, &rootclass_hfid,
				       tran_lock_waitsecs, tran_isolation);
  /* free the thing get from au_user_name_dup() */
  if (client_credential->db_user != boot_Client_no_user_string)
    {
      free_and_init (client_credential->db_user);
    }
  if (tran_index == NULL_TRAN_INDEX)
    {
      error_code = er_errid ();
      if (error_code == NO_ERROR)
	{
	  error_code = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
	}
      cfg_free_directory (db);
      return error_code;
    }

  oid_set_root (&rootclass_oid);
  OID_INIT_TEMPID ();

  error_code = ws_init ();

  if (error_code == NO_ERROR)
    {
      /* Create system classes such as the root and authorization classes */

      sm_create_root (&rootclass_oid, &rootclass_hfid);
      au_init ();

      /* Create authorization classes and enable authorization */
      error_code = au_install ();
      if (error_code == NO_ERROR)
	{
	  error_code = au_start ();
	}
      if (error_code == NO_ERROR)
	{
	  tr_init ();
	  error_code = tr_install ();
	  if (error_code == NO_ERROR)
	    {
	      error_code = catcls_class_install ();
	      if (error_code == NO_ERROR)
		{
		  error_code = catcls_vclass_install ();
		}
	      if (error_code == NO_ERROR)
		{
		  /*
		   * mark all classes created during the initialization as "system"
		   * classes,
		   */
		  sm_mark_system_classes ();
		  error_code = tran_commit (false);
		}
	    }
	}
    }

  if (error_code != NO_ERROR)
    {
      (void) boot_shutdown_client (false);
    }
  else
    {
      boot_client (tran_index, tran_lock_waitsecs, tran_isolation);
#if defined (CS_MODE)
      /* print version string */
      strncpy (format, msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_GENERAL,
				       MSGCAT_GENERAL_DATABASE_INIT),
	       BOOT_FORMAT_MAX_LENGTH);
      (void) fprintf (stdout, format, rel_name ());
#endif /* CS_MODE */
    }

  cfg_free_directory (db);
  return error_code;
}

/*
 * boot_restart_client () - restart client
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   program_name(in) : Name of the program that started the system
 *   print_version(in): Flag which indicates if the version of CUBRID is
 *                     printed at the end of the restart process.
 *   db_name(in) : Database Name
 *
 * Note:
 *              An application must restart the database system with the
 *              desired database (the database must have already been created)
 *              before the application start invoking the CUBRID functional
 *              interface. This function restarts the CUBRID client. It also
 *              initializes all client modules for the execution of the client
 *              interface. A transaction for the current client session is
 *              automatically started.
 *
 *              It is very important that the application check for success
 *              of this function before calling any other CUBRID function.
 */

int
boot_restart_client (BOOT_CLIENT_CREDENTIAL * client_credential)
{
  int tran_index;
  TRAN_ISOLATION tran_isolation;
  int tran_lock_waitsecs;
  TRAN_STATE transtate;
  int error_code;
  DB_INFO *db = NULL;
#if !defined(WINDOWS)
  bool dl_initialized = false;
#endif /* !WINDOWS */
  char *ptr;
#if defined(CS_MODE)
  const char *hosts[2];
  size_t size;
#endif /* CS_MODE */

  assert (client_credential != NULL);

  /* If the client is restarted, shutdown the client */
  if (BOOT_IS_CLIENT_RESTARTED ())
    {
      (void) boot_shutdown_client (true);
    }

  if (!boot_Is_client_all_final)
    {
      boot_client_all_finalize (true);
    }

#if defined(WINDOWS)
  /* set up the WINDOWS stream emulations */
  pc_init ();
#endif /* WINDOWS */

  /* initialize language parameters */
  (void) lang_init ();

  /* database name must be specified */
  if (client_credential->db_name == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_UNKNOWN_DATABASE, 1,
	      "(null)");
      return ER_BO_UNKNOWN_DATABASE;
    }

  /* open the system message catalog, before prm_ ?  */
  if (msgcat_init () != NO_ERROR)
    {
      error_code = ER_BO_CANNOT_ACCESS_MESSAGE_CATALOG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
      goto error;
    }

  /* initialize system parameters */
  if (sysprm_load_and_init (client_credential->db_name, NULL) != NO_ERROR)
    {
      error_code = ER_BO_CANNOT_ACCESS_MESSAGE_CATALOG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
      goto error;
    }

  /* initialize the "areas" memory manager, requires prm_ */
  area_init (false);
  locator_initialize_areas ();

  ptr = (char *) strstr (client_credential->db_name, "@");
  if (ptr == NULL)
    {
      /* Find the location of the database and the log from the database.txt */
      db = cfg_find_db (client_credential->db_name);
#if defined(CS_MODE)
      if (db == NULL)
	{
	  /* if not found, use secondary host lists */
	  db = cfg_new_db (client_credential->db_name, NULL, NULL, NULL);
	}

      if (db->num_hosts > 1
	  && (BOOT_ADMIN_CLIENT_TYPE (client_credential->client_type)
	      || BOOT_LOG_REPLICATOR_TYPE (client_credential->client_type)
	      || BOOT_CSQL_CLIENT_TYPE (client_credential->client_type)))
	{
	  error_code = ER_NET_NO_EXPLICIT_SERVER_HOST;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
	  goto error;
	}
#endif /* CS_MODE */
    }
  else
    {
      /* db_name@host_name */
#if defined(CS_MODE)
      size = strlen (client_credential->db_name) + 1;
      hosts[0] = ptr + 1;
      hosts[1] = NULL;
      *ptr = '\0';		/* screen 'db@host' */
      db = cfg_new_db (client_credential->db_name, NULL, NULL, hosts);
      *ptr = (char) '@';
#else /* CS_MODE */
      error_code = ER_NOT_IN_STANDALONE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code,
	      1, client_credential->db_name);
      goto error;
#endif /* !CS_MODE */
    }

  if (db == NULL)
    {
      error_code = ER_BO_UNKNOWN_DATABASE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
	      client_credential->db_name);
      goto error;
    }

  er_clear ();

  /* Get the user name */
  if (client_credential->db_user == NULL)
    {
      client_credential->db_user = au_user_name_dup ();
      if (client_credential->db_user == NULL)
	{
	  client_credential->db_user = (char *) boot_Client_no_user_string;
	}
      else if (client_credential->db_user[0] == '\0')
	{
	  free_and_init (client_credential->db_user);
	  client_credential->db_user = (char *) AU_PUBLIC_USER_NAME;
	}
    }
  /* Get the login name, host, and process identifier */
  if (client_credential->login_name == NULL)
    {
      if (getuserid (boot_Client_id_buffer, L_cuserid) != (char *) NULL)
	{
	  client_credential->login_name = boot_Client_id_buffer;
	}
      else
	{
	  client_credential->login_name =
	    (char *) boot_Client_id_unknown_string;
	}
    }
  if (client_credential->host_name == NULL)
    {
      if (boot_Host_name[0] == '\0')
	{
	  if (GETHOSTNAME (boot_Host_name, MAXHOSTNAMELEN) != 0)
	    {
	      strcpy (boot_Host_name, boot_Client_id_unknown_string);
	    }
	  boot_Host_name[MAXHOSTNAMELEN - 1] = '\0';	/* bullet proof */
	}
      client_credential->host_name = boot_Host_name;
    }
  client_credential->process_id = getpid ();

  /*
   * Initialize the dynamic loader. Don't care about failures. If dynamic
   * loader fails, methods will fail when they are invoked
   */
#if !defined(WINDOWS)
#if !defined (SOLARIS) && !defined(LINUX)
  (void) dl_initiate_module (program_name);
#else /* !SOLARIS && !LINUX */
  (void) dl_initiate_module ();
#endif /* !SOLARIS && !LINUX */
  dl_initialized = true;
#endif /* !WINDOWS */

  /* read only mode? */
  if (PRM_READ_ONLY_MODE
      || BOOT_READ_ONLY_CLIENT_TYPE (client_credential->client_type))
    {
      db_disable_modification ();
    }

#if defined(CS_MODE)
  /* Initialize the communication subsystem */
  if (BOOT_NORMAL_CLIENT_TYPE (client_credential->client_type))
    {
      error_code = boot_client_initialize_css (db, true, false);

      if (error_code == ER_NET_SERVER_HAND_SHAKE)
	{
	  er_log_debug (ARG_FILE_LINE, "boot_restart_client: "
			"boot_client_initialize_css () ER_NET_SERVER_HAND_SHAKE\n");
	  css_send_close_request (css_Client_anchor->conn);

	  error_code = boot_client_initialize_css (db, false, false);
	}
    }
  else if (client_credential->client_type == BOOT_CLIENT_SLAVE_ONLY_BROKER)
    {
      error_code = boot_client_initialize_css (db, true, false);
    }
  else
    {
      error_code = boot_client_initialize_css (db, false, false);
    }
  if (error_code != NO_ERROR)
    {
      er_log_debug (ARG_FILE_LINE, "boot_restart_client: "
		    "boot_client_initialize_css () error %d\n", error_code);
      goto error;
    }
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_BO_CONNECTED_TO, 5,
	  client_credential->program_name, client_credential->process_id,
	  client_credential->db_name, boot_Host_connected, PRM_TCP_PORT_ID);

  /* tune some client parameters with the value from the server */
  sysprm_tune_client_parameters ();
#else /* CS_MODE */
#if defined(WINDOWS)
  css_windows_startup ();
#endif /* WINDOWS */
#endif /* !CS_MODE */

  /* Free the information about the database */
  cfg_free_directory (db);
  db = NULL;

  /* this must be done before the register_client because recovery steps
   * may need domains.
   */
  tp_init ();
  error_code = ws_init ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  /*
   * At this moment, we should use the default isolation level and wait
   * timeout, since the client fetches objects during the restart process.
   * This values are reset at a later point, once the client has been fully
   * restarted.
   */

  tran_isolation = TRAN_DEFAULT_ISOLATION;
  tran_lock_waitsecs = TRAN_LOCK_INFINITE_WAIT;

  er_log_debug (ARG_FILE_LINE, "boot_restart_client: "
		"register client { type %d db %s user %s password %s "
		"program %s login %s host %s pid %d }\n",
		client_credential->client_type,
		client_credential->db_name,
		client_credential->db_user,
		client_credential->db_password,
		client_credential->program_name,
		client_credential->login_name,
		client_credential->host_name, client_credential->process_id);
  tran_index = boot_register_client (client_credential,
				     tran_lock_waitsecs, tran_isolation,
				     &transtate, &boot_Server_credential);

  /* free the thing get from au_user_name_dup() */
  if (client_credential->db_user != boot_Client_no_user_string
      && client_credential->db_user != AU_PUBLIC_USER_NAME)
    {
      free_and_init (client_credential->db_user);
    }
  if (tran_index == NULL_TRAN_INDEX)
    {
      error_code = er_errid ();
      goto error;
    }

#if defined(CS_MODE)
  /* Reset the pagesize according to server.. */
  if (db_set_page_size (boot_Server_credential.page_size,
			boot_Server_credential.log_page_size) != NO_ERROR)
    {
      error_code = er_errid ();
      goto error;
    }

  /* Reset the disk_level according to server.. */
  if (rel_disk_compatible () != boot_Server_credential.disk_compatibility)
    {
      rel_set_disk_compatible (boot_Server_credential.disk_compatibility);
    }
#endif /* CS_MODE */

  /* Initialize client modules for execution */
  boot_client (tran_index, tran_lock_waitsecs, tran_isolation);

  oid_set_root (&boot_Server_credential.root_class_oid);
  OID_INIT_TEMPID ();

  sm_init (&boot_Server_credential.root_class_oid,
	   &boot_Server_credential.root_class_hfid);
  au_init ();			/* initialize authorization globals */

  /* start authorization and make sure the logged in user has access */
  error_code = au_start ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  error_code = ccf_init ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  lang_server_charset_init ();

  tr_init ();			/* initialize trigger manager */

  jsp_init ();

  /* If the client has any loose ends from the recovery manager, do them */

  if (transtate != TRAN_ACTIVE)
    {
      if (transtate == TRAN_UNACTIVE_COMMITTED_WITH_CLIENT_USER_LOOSE_ENDS)
	{
	  transtate = tran_commit_client_loose_ends ();
	  /* We expect loose_ends are gone and ready to move on */
	  if (transtate == TRAN_UNACTIVE_COMMITTED)
	    {
	      transtate = TRAN_ACTIVE;
	    }
	}
      else
	{
	  transtate = tran_abort_client_loose_ends (true);
	  /* We expect loose_ends are gone and ready to move on */
	  if (transtate == TRAN_UNACTIVE_ABORTED)
	    {
	      transtate = TRAN_ACTIVE;
	    }
	}
      if (transtate != TRAN_ACTIVE)
	{
	  error_code = er_errid ();
	  goto error;
	}
    }
  /* Does not care if was committed/aborted .. */
  (void) tran_commit (false);

  /*
   * If there is a need to change the isolation level and the lock wait,
   * do it at this moment
   */
  tran_isolation = (TRAN_ISOLATION) PRM_LOG_ISOLATION_LEVEL;
  tran_lock_waitsecs = PRM_LK_TIMEOUT_SECS;
  if (tran_isolation != TRAN_DEFAULT_ISOLATION)
    {
      error_code = tran_reset_isolation (tran_isolation, TM_TRAN_ASYNC_WS ());
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }
  if (tran_lock_waitsecs != TRAN_LOCK_INFINITE_WAIT)
    {
      (void) tran_reset_wait_times ((float) tran_lock_waitsecs);
    }

  return error_code;

error:

  /* free the thing get from au_user_name_dup() */
  if (client_credential->db_user != NULL)
    {
      if (client_credential->db_user != boot_Client_no_user_string
	  && client_credential->db_user != AU_PUBLIC_USER_NAME)
	{
	  free_and_init (client_credential->db_user);
	}
    }

  /* Protect against falsely returning NO_ERROR to caller */
  if (error_code == NO_ERROR)
    {
      error_code = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
    }

  if (db != NULL)
    {
      cfg_free_directory (db);
    }

  if (BOOT_IS_CLIENT_RESTARTED ())
    {
      er_log_debug (ARG_FILE_LINE, "boot_shutdown_client: "
		    "unregister client { tran %d }\n", tm_Tran_index);
      boot_shutdown_client (false);
    }
  else
    {
#if !defined(WINDOWS)
      if (dl_initialized == true)
	{
	  (void) dl_destroy_module ();
	  dl_initialized = false;
	}
#endif /* !WINDOWS */
      /*msgcat_final (); */
      lang_final ();
      sysprm_final ();
      area_final ();
#if defined(WINDOWS)
      pc_final ();
#endif /* WINDOWS */
    }

  return error_code;
}

/*
 * boot_shutdown_client () - shutdown client
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   is_er_final(in) :
 *
 * Note:
 *              This function should be called before the CUBRID
 *              application is finished. This function will notify the
 *              recovery manager that the application has finished and will
 *              terminate all client modules (e.g., allocation of memory is
 *              deallocated).If there are active transactions, they are either
 *              committed or aborted according to the commit_on_shutdown
 *              system parameter.
 */

int
boot_shutdown_client (bool is_er_final)
{
  if (BOOT_IS_CLIENT_RESTARTED ())
    {
      /*
       * wait for other server request to finish.
       * if db_shutdown() is called by signal handler or atexit handler,
       * the server request may be running.
       */
      tran_wait_server_active_trans ();

      /*
       * Either Abort or commit the current transaction depending upon the value
       * of the commit_on_shutdown system parameter.
       */
      if (tran_is_active_and_has_updated ())
	{
	  if (PRM_COMMIT_ON_SHUTDOWN != false)
	    {
	      (void) tran_commit (false);
	    }
	  else
	    {
	      (void) tran_abort ();
	    }
	}

      /*
       * Make sure that we are still up. For example, if the server died, we do
       * not need to call the following stuff any longer.
       */

      if (BOOT_IS_CLIENT_RESTARTED ())
	{
	  (void) boot_unregister_client (tm_Tran_index);
#if defined(CS_MODE)
	  (void) net_client_final ();
#else /* CS_MODE */
#if defined(WINDOWS)
	  css_windows_shutdown ();
#endif /* WINDOWS */
#endif /* !CS_MODE */
	}

      boot_client_all_finalize (is_er_final);
      jsp_close_connection ();
    }

  return NO_ERROR;
}

/*
 * boot_shutdown_client_at_exit () - make sure that the client is shutdown at exit
 *
 * return : nothing
 *
 * Note:
 *       This function is called when the invoked program terminates
 *       normally. This function make sure that the client is shutdown
 *       in a nice way.
 */
static void
boot_shutdown_client_at_exit (void)
{
  if (BOOT_IS_CLIENT_RESTARTED () && boot_Process_id == getpid ())
    {
      /* Avoid infinite looping if someone calls exit during shutdown */
      boot_Process_id++;
      (void) boot_shutdown_client (true);
    }
}

/*
 * boot_donot_shutdown_client_at_exit: do not shutdown client at exist.
 *
 * return : nothing
 *
 * This function must be called when the system needs to exit
 *  without shutting down the client (e.g., in case of fatal
 *  failure).
 */
void
boot_donot_shutdown_client_at_exit (void)
{
  if (BOOT_IS_CLIENT_RESTARTED () && boot_Process_id == getpid ())
    {
      boot_Process_id++;
    }
}

/*
 * boot_server_die_or_reject: shutdown client when the server is dead
 *
 * return : nothing
 *
 * Note: The server has been terminated for circumstances beyond the client
 *       control. All active client transactions have been unilaterally
 *       aborted as a consequence of the termination of server.
 */
void
boot_server_die_or_changed (void)
{
  /*
   * If the clinet is restarted, abort the active transaction in the client and
   * terminate the client modules
   */
  if (BOOT_IS_CLIENT_RESTARTED ())
    {
      (void) tran_abort_only_client (true);
      boot_client (NULL_TRAN_INDEX, -1, TRAN_DEFAULT_ISOLATION);
      boot_Is_client_all_final = false;
#if defined(CS_MODE)
      css_terminate (true);
#endif /* !CS_MODE */
      er_log_debug (ARG_FILE_LINE,
		    "boot_server_die_or_changed() terminated\n");
    }
}

/*
 * boot_client_all_finalize () - terminate every single client
 *
 * return : nothing
 *
 *   is_er_final(in): Terminate the error module..
 *
 *
 * Note: Terminate every single module of the client. This function is called
 *       during the shutdown of the client.
 */
void
boot_client_all_finalize (bool is_er_final)
{
  if (BOOT_IS_CLIENT_RESTARTED () || boot_Is_client_all_final == false)
    {
      if (boot_Server_credential.db_full_name)
	{
	  db_private_free_and_init (NULL,
				    boot_Server_credential.db_full_name);
	}
      if (boot_Server_credential.host_name)
	{
	  db_private_free_and_init (NULL, boot_Server_credential.host_name);
	}

      /* release resource for xasl */
      xts_final (NULL);
      tran_free_savepoint_list ();
      sm_flush_static_methods ();
      set_final ();
      parser_final ();
      tr_final ();
      ccf_final ();
      au_final ();
      sm_final ();
      ws_final ();
      tp_final ();

#if !defined(WINDOWS)
      (void) dl_destroy_module ();
#endif /* !WINDOWS */

      locator_free_areas ();
      sysprm_final ();
      area_final ();

      msgcat_final ();
      if (is_er_final)
	{
	  er_final ();
	}
      lang_final ();

      /* adj_arrays & lex buffers in the cnv formatting library. */
      cnv_cleanup ();

#if defined(WINDOWS)
      pc_final ();
#endif /* WINDOWS */

      /* Clean up stuff allocated by the utilities library too.
       * Not really necessary but avoids warnings from memory tracking
       * tools that customers might be using.
       */
      co_final ();

      memset (&boot_Server_credential, 0, sizeof (boot_Server_credential));

      boot_client (NULL_TRAN_INDEX, -1, TRAN_DEFAULT_ISOLATION);
      boot_Is_client_all_final = true;
    }

}

#if defined(CS_MODE)
/*
 * boot_client_initialize_css () - Attempts to connect to hosts in list send to it
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   db(in) : host information
 *
 * Note: This function will try an initialize the communications with the hosts
 *       in hostlist until success or the end of list is reached.
 */
static int
boot_client_initialize_css (DB_INFO * db, bool check_capabilities,
			    bool discriminative)
{
  int error = ER_NET_NO_SERVER_HOST;
  int hn, tn, n;
  char *hostlist[10], strbuf[(MAXHOSTNAMELEN + 1) * 10];
  bool cap_error = false;

  assert (db != NULL);
  assert (db->num_hosts > 0);

  if (db->hosts == NULL)
    {
      db->hosts = cfg_get_hosts (NULL, &db->num_hosts, false);
      if (db->hosts == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  return ER_GENERIC_ERROR;
	}
    }

  hn = 0;
  /* try the connected host first */
  if (boot_Host_connected[0] != '\0')
    {
      hostlist[hn++] = boot_Host_connected;
    }
  for (n = 0; hn < 10 && n < db->num_hosts; n++)
    {
      hostlist[hn++] = db->hosts[n];
    }

  /*
   * tn: number of hosts trying to connect
   * cn: current host trying to connect
   * If hosts array is "host1, host2, and host3", try to connect to host
   * in sequence of "host1", "host1, host2", "host1, host2, host3"
   * if 'discriminative' is set
   */
  for (tn = 0; tn < hn; tn++)
    {
      for (n = discriminative ? 0 : tn; n <= tn; n++)
	{
	  er_log_debug (ARG_FILE_LINE, "trying to connect '%s@%s'\n",
			db->name, hostlist[n]);
	  error = net_client_init (db->name, hostlist[n]);
	  if (error == NO_ERROR)
	    {
	      /* save the hostname for the use of calling functions */
	      strncpy (boot_Host_connected, hostlist[n], MAXHOSTNAMELEN);
	      er_log_debug (ARG_FILE_LINE, "ping server with handshake\n");
	      /* ping to validate availability and to check compatibility */
	      er_clear ();
	      error =
		net_client_ping_server_with_handshake (check_capabilities);
	    }

	  /* connect error to the db at the host */
	  switch (error)
	    {
	    case NO_ERROR:
	      return NO_ERROR;

	    case ER_NET_DIFFERENT_RELEASE:
	    case ER_NET_NO_SERVER_HOST:
	    case ER_NET_CANT_CONNECT_SERVER:
	    case ER_NET_NO_MASTER:
	    case ERR_CSS_TCP_CANNOT_CONNECT_TO_MASTER:
	    case ERR_CSS_ERROR_FROM_SERVER:
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_BO_CONNECT_FAILED, 2, db->name, hostlist[n]);
	      /* try to connect to next host */
	      er_log_debug (ARG_FILE_LINE,
			    "error %d. try to connect to next host\n", error);
	      continue;

	    case ER_NET_SERVER_HAND_SHAKE:
	      cap_error = true;
	      break;

	    case ER_CSS_CLIENTS_EXCEEDED:
	      /* return error */
	      er_log_debug (ARG_FILE_LINE, "error %d. exceeded max clients\n",
			    error);
	      return error;

	    default:
	      /* ?? */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_CONNECT_FAILED,
		      2, db->name, hostlist[n]);
	    }

	}			/* for (cn) */
    }				/* for (tn) */

  /* failed to connect all hosts; write an error message */
  strbuf[0] = '\0';
  for (n = 0; n < hn - 1 && n < 9; n++)
    {
      strncat (strbuf, hostlist[n], MAXHOSTNAMELEN);
      strcat (strbuf, ":");

    }
  strncat (strbuf, hostlist[n], MAXHOSTNAMELEN);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_CONNECT_FAILED, 2,
	  db->name, strbuf);

  if (check_capabilities == true && cap_error == true)
    {
      /*
       * There'a a live host which has cause handshake error,
       * so adjust the return value
       */
      error = ER_NET_SERVER_HAND_SHAKE;
    }

  return (error);
}
#endif /* CS_MODE */

/*
 * boot_define_class :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_class (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "class_name", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "class_of", "object", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "class_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "class_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_system_class", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "owner", AU_USER_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "inst_attr_count", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "class_attr_count", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "shared_attr_count", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "inst_meth_count", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "class_meth_count", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_CLASS_NAME);

  error_code = smt_add_attribute (def, "sub_classes", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "super_classes", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_ATTRIBUTE_NAME);

  error_code = smt_add_attribute (def, "inst_attrs", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "class_attrs", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "shared_attrs", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_METHOD_NAME);

  error_code = smt_add_attribute (def, "inst_meths", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "class_meths", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_METHFILE_NAME);

  error_code = smt_add_attribute (def, "meth_files", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_QUERYSPEC_NAME);

  error_code = smt_add_attribute (def, "query_specs", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_INDEX_NAME);

  error_code = smt_add_attribute (def, "indexes", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_global", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "node_name", "varchar(64)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_attribute :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_attribute (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *index_col_names[3] = { "class_of", "attr_name", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "attr_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "attr_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "from_class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code =
    smt_add_attribute (def, "from_attr_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "def_order", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "data_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "default_value", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_DOMAIN_NAME);

  error_code = smt_add_attribute (def, "domains", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_nullable", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_domain :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 *
 * Note:
 *
 */
static int
boot_define_domain (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "object_of", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "object_of", "object", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "data_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "prec", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "scale", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "code_set", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_DOMAIN_NAME);

  error_code = smt_add_attribute (def, "set_domains", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_method :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_method (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *names[3] = { "class_of", "meth_name", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "meth_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "meth_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "from_class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code =
    smt_add_attribute (def, "from_meth_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_METHSIG_NAME);

  error_code = smt_add_attribute (def, "signatures", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_meth_sig :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_meth_sig (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *names[2] = { "meth_of", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "meth_of", CT_METHOD_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "func_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "arg_count", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_METHARG_NAME);

  error_code = smt_add_attribute (def, "return_value", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "arguments", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_meth_argument :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_meth_argument (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "meth_sig_of", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "meth_sig_of", CT_METHSIG_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "data_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "index_of", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_DOMAIN_NAME);

  error_code = smt_add_attribute (def, "domains", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_meth_file :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_meth_file (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "class_of", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "from_class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "path_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_query_spec :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_query_spec (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "class_of", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "spec", "varchar(4096)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_index :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_index (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "class_of", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "index_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_unique", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "key_count", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_INDEXKEY_NAME);

  error_code = smt_add_attribute (def, "key_attrs", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_reverse", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_primary_key", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_foreign_key", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_global", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "node_name", "varchar(64)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_meth_argument :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_index_key (MOP class_mop)
{
  SM_TEMPLATE *def;
  DB_VALUE prefix_default;
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "index_of", NULL };

  def = smt_edit_class_mop (class_mop);
  if (def == NULL)
    {
      return er_errid ();
    }

  error_code = smt_add_attribute (def, "index_of", CT_INDEX_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "key_attr_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "key_order", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "asc_desc", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "key_prefix_length", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  DB_MAKE_INTEGER (&prefix_default, -1);

  error_code = smt_set_attribute_default (def, "key_prefix_length", 0,
					  &prefix_default);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_class_authorization :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_class_authorization (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "grantee", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "grantor", AU_USER_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "grantee", AU_USER_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "auth_type", "varchar(7)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_grantable", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_partition :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_partition (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;
  const char *index_col_names[] = { "class_of", "pname", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "class_of", CT_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "pname", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "ptype", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "pexpr", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "pvalues", "sequence of", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_add_data_type :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 *
 * Note:
 *
 */
static int
boot_add_data_type (MOP class_mop)
{
  DB_OBJECT *obj;
  DB_VALUE val;
  int i;

  /* BLOB and CLOB are added in advance to avoid migration R3.0 to R3.1.
     This fix should be removed at R3.1. */

  const char *names[DB_TYPE_LAST + 2] = {
    "INTEGER", "FLOAT", "DOUBLE", "STRING", "OBJECT",
    "SET", "MULTISET", "SEQUENCE", "ELO", "TIME",
    "TIMESTAMP", "DATE", "MONETARY", NULL /* VARIABLE */ , NULL /* SUB */ ,
    NULL /* POINTER */ , NULL /* ERROR */ , "SHORT", NULL /* VOBJ */ ,
    NULL /* OID */ ,
    NULL /* VALUE */ , "NUMERIC", "BIT", "VARBIT", "CHAR",
    "NCHAR", "VARNCHAR", NULL /* RESULTSET */ , NULL /* MIDXKEY */ ,
    NULL /* TABLE */ ,
    "BIGINT", "DATETIME",
    "BLOB", "CLOB"
  };

  for (i = 0; i < DB_TYPE_LAST + 2; i++)
    {

      if (names[i] != NULL)
	{
	  obj = db_create_internal (class_mop);
	  if (obj == NULL)
	    {
	      return er_errid ();
	    }

	  DB_MAKE_INTEGER (&val, i + 1);
	  db_put_internal (obj, "type_id", &val);

	  DB_MAKE_VARCHAR (&val, 9, (char *) names[i], strlen (names[i]));
	  db_put_internal (obj, "type_name", &val);
	}
    }

  return NO_ERROR;
}

/*
 * boot_define_data_type :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_data_type (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "type_id", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "type_name", "varchar(9)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = boot_add_data_type (class_mop);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_stored_procedure :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_stored_procedure (MOP class_mop)
{
  SM_TEMPLATE *def;
  char args_string[64];
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "sp_name", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "sp_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "sp_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "return_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "arg_count", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (args_string, "sequence of %s", CT_STORED_PROC_ARGS_NAME);
  error_code = smt_add_attribute (def, "args", args_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "lang", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "target", "varchar(4096)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "owner", AU_USER_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_UNIQUE, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_stored_procedure_arguments :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_stored_procedure_arguments (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "sp_name", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "sp_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "index_of", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "arg_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "data_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "mode", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_serial :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_serial (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  unsigned char num[DB_NUMERIC_BUF_SIZE];	/* Copy of a DB_C_NUMERIC */
  DB_VALUE default_value;
  int error_code = NO_ERROR;
  const char *index_col_names[] = { "name", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "name", "string", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "owner", AU_USER_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "numeric(%d,0)", DB_MAX_NUMERIC_PRECISION);
  numeric_coerce_int_to_num (1, num);
  DB_MAKE_NUMERIC (&default_value, num, DB_MAX_NUMERIC_PRECISION, 0);

  error_code = smt_add_attribute (def, "current_val", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  error_code = smt_set_attribute_default (def, "current_val", 0,
					  &default_value);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "increment_val", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  error_code = smt_set_attribute_default (def, "increment_val", 0,
					  &default_value);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "max_val", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "min_val", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  DB_MAKE_INTEGER (&default_value, 0);

  error_code = smt_add_attribute (def, "cyclic", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  error_code = smt_set_attribute_default (def, "cyclic", 0, &default_value);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "started", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  error_code = smt_set_attribute_default (def, "started", 0, &default_value);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "class_name", "string", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "att_name", "string", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_class_method (def, "change_serial_owner",
				     "au_change_serial_owner_method");
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "cached_num", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  error_code = smt_set_attribute_default (def, "cached_num", 0,
					  &default_value);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_global", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "node_name", "varchar(64)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }


  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_PRIMARY_KEY, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = db_constrain_non_null (class_mop, "current_val", 0, 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = db_constrain_non_null (class_mop, "increment_val", 0, 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = db_constrain_non_null (class_mop, "max_val", 0, 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = db_constrain_non_null (class_mop, "min_val", 0, 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_ha_apply_info :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_ha_apply_info (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;
  const char *index_col_names[] = { "db_name", "copied_log_path", NULL };

  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "db_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "db_creation_time", "datetime", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "copied_log_path", "varchar(4096)",
				  NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "page_id", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "offset", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "log_record_time", "datetime", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "last_access_time", "datetime", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "status", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "insert_counter", "bigint", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "update_counter", "bigint", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "delete_counter", "bigint", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "schema_counter", "bigint", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "commit_counter", "bigint", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "fail_counter", "bigint", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "required_page_id", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "start_time", "datetime", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add constraints */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_UNIQUE, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = db_constrain_non_null (class_mop, "db_name", 0, 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = db_constrain_non_null (class_mop, "copied_log_path", 0, 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = db_constrain_non_null (class_mop, "page_id", 0, 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = db_constrain_non_null (class_mop, "offset", 0, 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
* boot_define_cluster_node :
*
* returns : NO_ERROR if all OK, ER_ status otherwise
*
*/
static int
boot_define_cluster_node (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;
  const char *index_col_names[] = { "node_name", NULL };
  def = smt_edit_class_mop (class_mop);

  error_code = smt_add_attribute (def, "node_name", "varchar(64)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "db_host", "varchar(64)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "cubrid_port", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = sm_update_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add constraints */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_UNIQUE, NULL,
				  index_col_names, 0);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = db_constrain_non_null (class_mop, "node_name", 0, 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = db_constrain_non_null (class_mop, "db_host", 0, 1);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * catcls_class_install :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
catcls_class_install (void)
{
  CATCLS_FUNCTION clist[] = {
    {CT_CLASS_NAME, (DEF_FUNCTION) boot_define_class},
    {CT_ATTRIBUTE_NAME, (DEF_FUNCTION) boot_define_attribute},
    {CT_DOMAIN_NAME, (DEF_FUNCTION) boot_define_domain},
    {CT_METHOD_NAME, (DEF_FUNCTION) boot_define_method},
    {CT_METHSIG_NAME, (DEF_FUNCTION) boot_define_meth_sig},
    {CT_METHARG_NAME, (DEF_FUNCTION) boot_define_meth_argument},
    {CT_METHFILE_NAME, (DEF_FUNCTION) boot_define_meth_file},
    {CT_QUERYSPEC_NAME, (DEF_FUNCTION) boot_define_query_spec},
    {CT_INDEX_NAME, (DEF_FUNCTION) boot_define_index},
    {CT_INDEXKEY_NAME, (DEF_FUNCTION) boot_define_index_key},
    {CT_DATATYPE_NAME, (DEF_FUNCTION) boot_define_data_type},
    {CT_CLASSAUTH_NAME, (DEF_FUNCTION) boot_define_class_authorization},
    {CT_PARTITION_NAME, (DEF_FUNCTION) boot_define_partition},
    {CT_STORED_PROC_NAME, (DEF_FUNCTION) boot_define_stored_procedure},
    {CT_STORED_PROC_ARGS_NAME,
     (DEF_FUNCTION) boot_define_stored_procedure_arguments},
    {CT_SERIAL_NAME, (DEF_FUNCTION) boot_define_serial},
    {CT_HA_APPLY_INFO_NAME, (DEF_FUNCTION) boot_define_ha_apply_info},
    {CT_CLUSTER_NODE_NAME, (DEF_FUNCTION) boot_define_cluster_node}
  };

  MOP class_mop[sizeof (clist) / sizeof (clist[0])];
  int i, save;
  int error_code = NO_ERROR;
  int num_classes = sizeof (clist) / sizeof (clist[0]);

  AU_DISABLE (save);

  for (i = 0; i < num_classes; i++)
    {
      class_mop[i] = db_create_class (clist[i].name);
      if (class_mop[i] == NULL)
	{
	  error_code = er_errid ();
	  goto end;
	}
    }

  for (i = 0; i < num_classes; i++)
    {
      error_code = ((DEF_CLASS_FUNCTION) (clist[i].function)) (class_mop[i]);
      if (error_code != NO_ERROR)
	{
	  error_code = er_errid ();
	  goto end;
	}
    }

end:
  AU_ENABLE (save);

  return error_code;
}

/*
 * boot_define_view_class :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 */
static int
boot_define_view_class (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"class_name", "varchar(255)"},
    {"owner_name", "varchar(255)"},
    {"class_type", "varchar(6)"},
    {"is_system_class", "varchar(3)"},
    {"partitioned", "varchar(3)"},
    {"is_reuse_oid_class", "varchar(3)"},
    {"is_global_class", "varchar(3)"},
    {"node_name", "varchar(64)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_CLASS_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT c.class_name, CAST(c.owner.name AS VARCHAR(255)),"
	   " CASE c.class_type WHEN 0 THEN 'CLASS'"
	   " WHEN 1 THEN 'VCLASS' WHEN 3 THEN 'PCLASS'"
	   " ELSE 'UNKNOW' END,"
	   " CASE WHEN MOD(c.is_system_class, 2) = 1 THEN 'YES' ELSE 'NO' END,"
	   " CASE WHEN c.sub_classes IS NULL THEN 'NO' ELSE NVL((SELECT 'YES'"
	   " FROM %s p WHERE p.class_of = c and p.pname IS NULL), 'NO') END,"
	   " CASE WHEN MOD(c.is_system_class / 8, 2) = 1 THEN 'YES' ELSE 'NO' END,"
	   " CASE WHEN MOD(c.is_global, 2) = 1 THEN 'YES' ELSE 'NO' END,"
	   " CASE WHEN MOD(c.is_global, 2) = 1 THEN NVL(c.node_name, '@LOCAL') ELSE NULL END"
	   " FROM %s c"
	   " WHERE CURRENT_USER = 'DBA' OR"
	   " {c.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {c} SUBSETEQ ("
	   "  SELECT SUM(SET{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT')",
	   CT_PARTITION_NAME,
	   CT_CLASS_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_super_class :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_super_class (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"class_name", "varchar(255)"},
    {"super_class_name", "varchar(255)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_SUPER_CLASS_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT c.class_name, s.class_name"
	   " FROM %s c, TABLE(c.super_classes) AS t(s)"
	   " WHERE CURRENT_USER = 'DBA' OR"
	   " {c.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {c} SUBSETEQ ("
	   "  SELECT SUM(SET{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT')",
	   CT_CLASS_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_vclass :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_vclass (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"vclass_name", "varchar(255)"},
    {"vclass_def", "varchar(4096)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_VCLASS_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT q.class_of.class_name, q.spec"
	   " FROM %s q"
	   " WHERE CURRENT_USER = 'DBA' OR"
	   " {q.class_of.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {q.class_of} SUBSETEQ (SELECT SUM(SET{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER ) AND"
	   "  au.auth_type = 'SELECT')",
	   CT_QUERYSPEC_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_attribute :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_attribute (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"attr_name", "varchar(255)"},
    {"class_name", "varchar(255)"},
    {"attr_type", "varchar(8)"},
    {"def_order", "integer"},
    {"from_class_name", "varchar(255)"},
    {"from_attr_name", "varchar(255)"},
    {"data_type", "varchar(9)"},
    {"prec", "integer"},
    {"scale", "integer"},
    {"code_set", "integer"},
    {"domain_class_name", "varchar(255)"},
    {"default_value", "varchar(255)"},
    {"is_nullable", "varchar(3)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_ATTRIBUTE_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT a.attr_name, c.class_name,"
	   " CASE WHEN a.attr_type = 0 THEN 'INSTANCE'"
	   " WHEN a.attr_type = 1 THEN 'CLASS' ELSE 'SHARED' END,"
	   " a.def_order, a.from_class_of.class_name,"
	   " a.from_attr_name, t.type_name, d.prec, d.scale,"
	   " d.code_set, d.class_of.class_name, a.default_value,"
	   " CASE WHEN a.is_nullable = 1 THEN 'YES' ELSE 'NO' END"
	   " FROM %s c, %s a, %s d, %s t"
	   " WHERE a.class_of = c AND d.object_of = a AND d.data_type = t.type_id AND"
	   " (CURRENT_USER = 'DBA' OR"
	   " {c.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {c} SUBSETEQ (SELECT SUM(SET{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT'))",
	   CT_CLASS_NAME,
	   CT_ATTRIBUTE_NAME,
	   CT_DOMAIN_NAME,
	   CT_DATATYPE_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_attribute_set_domain :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_attribute_set_domain (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"attr_name", "varchar(255)"},
    {"class_name", "varchar(255)"},
    {"attr_type", "varchar(8)"},
    {"data_type", "varchar(9)"},
    {"prec", "integer"},
    {"scale", "integer"},
    {"code_set", "integer"},
    {"domain_class_name", "varchar(255)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_ATTR_SD_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT a.attr_name, c.class_name,"
	   " CASE WHEN a.attr_type = 0 THEN 'INSTANCE'"
	   " WHEN a.attr_type = 1 THEN 'CLASS' ELSE 'SHARED' END,"
	   " et.type_name, e.prec, e.scale, e.code_set, e.class_of.class_name"
	   " FROM %s c, %s a, %s d, TABLE(d.set_domains) AS t(e), %s et"
	   " WHERE a.class_of = c AND d.object_of = a AND e.data_type = et.type_id AND"
	   " (CURRENT_USER = 'DBA' OR"
	   " {c.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {c} SUBSETEQ (SELECT SUM(SET{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER)  AND"
	   "  au.auth_type = 'SELECT'))",
	   CT_CLASS_NAME,
	   CT_ATTRIBUTE_NAME,
	   CT_DOMAIN_NAME,
	   CT_DATATYPE_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_method :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_method (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"meth_name", "varchar(255)"},
    {"class_name", "varchar(255)"},
    {"meth_type", "varchar(8)"},
    {"from_class_name", "varchar(255)"},
    {"from_meth_name", "varchar(255)"},
    {"func_name", "varchar(255)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_METHOD_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT m.meth_name, m.class_of.class_name,"
	   " CASE WHEN m.meth_type = 0 THEN 'INSTANCE' ELSE 'CLASS' END,"
	   " m.from_class_of.class_name, m.from_meth_name, s.func_name"
	   " FROM %s m, %s s"
	   " WHERE s.meth_of = m AND"
	   " (CURRENT_USER = 'DBA' OR"
	   " {m.class_of.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {m.class_of} SUBSETEQ ("
	   "  SELECT SUM(set{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT'))",
	   CT_METHOD_NAME,
	   CT_METHSIG_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_method_argument :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_method_argument (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"meth_name", "varchar(255)"},
    {"class_name", "varchar(255)"},
    {"meth_type", "varchar(8)"},
    {"index_of", "integer"},
    {"data_type", "varchar(9)"},
    {"prec", "integer"},
    {"scale", "integer"},
    {"code_set", "integer"},
    {"domain_class_name", "varchar(255)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_METHARG_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT s.meth_of.meth_name, s.meth_of.class_of.class_name,"
	   " CASE WHEN s.meth_of.meth_type = 0 THEN 'INSTANCE' ELSE 'CLASS' END,"
	   " a.index_of, t.type_name, d.prec, d.scale, d.code_set,"
	   " d.class_of.class_name"
	   " FROM %s s, %s a, %s d, %s t"
	   " WHERE a.meth_sig_of = s AND d.object_of = a AND d.data_type = t.type_id AND"
	   " (CURRENT_USER = 'DBA' OR"
	   " {s.meth_of.class_of.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {s.meth_of.class_of} SUBSETEQ ("
	   "  SELECT sum(set{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT'))",
	   CT_METHSIG_NAME,
	   CT_METHARG_NAME,
	   CT_DOMAIN_NAME,
	   CT_DATATYPE_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_method_argument_set_domain :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 * Note:
 *
 */
static int
boot_define_view_method_argument_set_domain (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"meth_name", "varchar(255)"},
    {"class_name", "varchar(255)"},
    {"meth_type", "varchar(8)"},
    {"index_of", "integer"},
    {"data_type", "varchar(9)"},
    {"prec", "integer"},
    {"scale", "integer"},
    {"code_set", "integer"},
    {"domain_class_name", "varchar(255)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_METHARG_SD_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT s.meth_of.meth_name, s.meth_of.class_of.class_name,"
	   " CASE WHEN s.meth_of.meth_type = 0 THEN 'INSTANCE' ELSE 'CLASS' END,"
	   " a.index_of, et.type_name, e.prec, e.scale, e.code_set,"
	   " e.class_of.class_name"
	   " FROM %s s, %s a, %s d, TABLE(d.set_domains) AS t(e), %s et"
	   " WHERE a.meth_sig_of = s AND d.object_of = a AND e.data_type = et.type_id AND"
	   " (CURRENT_USER = 'DBA' OR"
	   " {s.meth_of.class_of.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {s.meth_of.class_of} SUBSETEQ ("
	   "  SELECT sum(set{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT'))",
	   CT_METHSIG_NAME,
	   CT_METHARG_NAME,
	   CT_DOMAIN_NAME,
	   CT_DATATYPE_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_method_file :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_method_file (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"class_name", "varchar(255)"},
    {"path_name", "varchar(255)"},
    {"from_class_name", "varchar(255)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_METHFILE_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT f.class_of.class_name, f.path_name, f.from_class_of.class_name"
	   " FROM %s f"
	   " WHERE CURRENT_USER = 'DBA' OR"
	   " {f.class_of.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {f.class_of} SUBSETEQ ("
	   "  SELECT SUM(SET{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT')",
	   CT_METHFILE_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_index :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_index (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"index_name", "varchar(255)"},
    {"is_unique", "varchar(3)"},
    {"is_reverse", "varchar(3)"},
    {"class_name", "varchar(255)"},
    {"key_count", "integer"},
    {"is_primary_key", "varchar(3)"},
    {"is_foreign_key", "varchar(3)"},
    {"is_global_index", "varchar(3)"},
    {"node_name", "varchar(64)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_INDEX_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT i.index_name, CASE WHEN i.is_unique = 0 THEN 'NO' ELSE 'YES' END,"
	   " CASE WHEN i.is_reverse = 0 THEN 'NO' ELSE 'YES' END,"
	   " i.class_of.class_name, i.key_count,"
	   " CASE WHEN i.is_primary_key = 0 THEN 'NO' ELSE 'YES' END,"
	   " CASE WHEN i.is_foreign_key = 0 THEN 'NO' ELSE 'YES' END,"
	   " CASE WHEN i.is_global = 0 THEN 'NO' ELSE 'YES' END,"
	   " CASE WHEN i.is_global = 0 THEN NULL ELSE NVL(i.node_name, '@LOCAL') END"
	   " FROM %s i"
	   " WHERE CURRENT_USER = 'DBA' OR"
	   " {i.class_of.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {i.class_of} SUBSETEQ ("
	   "  SELECT SUM(SET{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT')",
	   CT_INDEX_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_index_key :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_index_key (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"index_name", "varchar(255)"},
    {"class_name", "varchar(255)"},
    {"key_attr_name", "varchar(255)"},
    {"key_order", "integer"},
    {"asc_desc", "varchar(4)"},
    {"key_prefix_length", "integer"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_INDEXKEY_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT k.index_of.index_name, k.index_of.class_of.class_name,"
	   " k.key_attr_name, k.key_order,"
	   " CASE k.asc_desc WHEN 0 THEN 'ASC'"
	   " WHEN 1 THEN 'DESC'"
	   " ELSE 'UNKN' END"
	   ", k.key_prefix_length"
	   " FROM %s k"
	   " WHERE CURRENT_USER = 'DBA' OR"
	   " {k.index_of.class_of.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {k.index_of.class_of} SUBSETEQ ("
	   "  SELECT SUM(SET{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT')",
	   CT_INDEXKEY_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_authorization :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_authorization (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"grantor_name", "varchar(255)"},
    {"grantee_name", "varchar(255)"},
    {"class_name", "varchar(255)"},
    {"auth_type", "varchar(7)"},
    {"is_grantable", "varchar(3)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_AUTH_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT CAST(a.grantor.name AS VARCHAR(255)),"
	   " CAST(a.grantee.name AS VARCHAR(255)), a.class_of.class_name, a.auth_type,"
	   " CASE WHEN a.is_grantable = 0 THEN 'NO' ELSE 'YES' END"
	   " FROM %s a"
	   " WHERE CURRENT_USER = 'DBA' OR"
	   " {a.class_of.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {a.class_of} SUBSETEQ ("
	   "  SELECT SUM(SET{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT')",
	   CT_CLASSAUTH_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_trigger :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_trigger (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"trigger_name", "varchar(255)"},
    {"target_class_name", "varchar(255)"},
    {"target_attr_name", "varchar(255)"},
    {"target_attr_type", "varchar(8)"},
    {"action_type", "integer"},
    {"action_time", "integer"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_TRIGGER_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT CAST(t.name AS VARCHAR(255)), c.class_name,"
	   " CAST(t.target_attribute AS VARCHAR(255)),"
	   " CASE t.target_class_attribute WHEN 0 THEN 'INSTANCE' ELSE 'CLASS' END,"
	   " t.action_type, t.action_time"
	   " FROM %s t LEFT OUTER JOIN %s c ON t.target_class = c.class_of"
	   " WHERE CURRENT_USER = 'DBA' OR"
	   " {t.owner.name} SUBSETEQ (SELECT SET{CURRENT_USER} +"
	   " COALESCE(SUM(SET{t.g.name}), SET{})"
	   " FROM %s u, TABLE(groups) AS t(g)"
	   " WHERE u.name = CURRENT_USER ) OR"
	   " {c} SUBSETEQ (SELECT SUM(SET{au.class_of})"
	   " FROM %s au"
	   " WHERE {au.grantee.name} SUBSETEQ"
	   " (SELECT SET{CURRENT_USER} +"
	   " COALESCE(SUM(SET{t.g.name}), SET{})"
	   " FROM %s u, TABLE(groups) AS t(g)"
	   " WHERE u.name = CURRENT_USER) AND"
	   " au.auth_type = 'SELECT')",
	   TR_CLASS_NAME,
	   CT_CLASS_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_partition :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_partition (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"class_name", "varchar(255)"},
    {"partition_name", "varchar(255)"},
    {"partition_class_name", "varchar(255)"},
    {"partition_type", "varchar(32)"},
    {"partition_expr", "varchar(255)"},
    {"partition_values", "sequence of"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_PARTITION_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT p.class_of.class_name AS class_name, p.pname AS partition_name,"
	   " p.class_of.class_name + '__p__' + p.pname AS partition_class_name,"
	   " CASE WHEN p.ptype = 0 THEN 'HASH'"
	   " WHEN p.ptype = 1 THEN 'RANGE' ELSE 'LIST' END AS partition_type,"
	   " TRIM(SUBSTRING(pi.pexpr FROM 8 FOR (POSITION(' FROM ' IN pi.pexpr)-8)))"
	   " AS partition_expression, p.pvalues AS partition_values"
	   " FROM %s p, (select * from %s sp where sp.class_of = "
	   " p.class_of AND sp.pname is null) pi"
	   " WHERE p.pname is not null AND"
	   " (CURRENT_USER = 'DBA' OR"
	   " {p.class_of.owner.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) OR"
	   " {p.class_of} SUBSETEQ (SELECT SUM(SET{au.class_of})"
	   "  FROM %s au"
	   "  WHERE {au.grantee.name} SUBSETEQ ("
	   "  SELECT SET{CURRENT_USER} + COALESCE(SUM(SET{t.g.name}), SET{})"
	   "  FROM %s u, TABLE(groups) AS t(g)"
	   "  WHERE u.name = CURRENT_USER) AND"
	   "  au.auth_type = 'SELECT'))",
	   CT_PARTITION_NAME,
	   CT_PARTITION_NAME,
	   AU_USER_CLASS_NAME, CT_CLASSAUTH_NAME, AU_USER_CLASS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_stored_procedure :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_stored_procedure (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"sp_name", "varchar(255)"},
    {"sp_type", "varchar(16)"},
    {"return_type", "varchar(16)"},
    {"arg_count", "integer"},
    {"lang", "varchar(16)"},
    {"target", "varchar(4096)"},
    {"owner", "varchar(256)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_STORED_PROC_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT sp.sp_name,"
	   " CASE sp.sp_type"
	   "   WHEN 1 THEN 'PROCEDURE'"
	   "   ELSE 'FUNCTION'"
	   " END,"
	   " CASE"
	   "   WHEN sp.return_type = 0 THEN 'void'"
	   "   WHEN sp.return_type = 28 THEN 'CURSOR'"
	   "   ELSE (SELECT dt.type_name FROM %s dt WHERE sp.return_type = dt.type_id)"
	   " END,"
	   " sp.arg_count,"
	   " CASE sp.lang"
	   "   WHEN 1 THEN 'JAVA'"
	   "   ELSE '' END,"
	   " sp.target, sp.owner.name"
	   " FROM %s sp", CT_DATATYPE_NAME, CT_STORED_PROC_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_view_stored_procedure_arguments :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
boot_define_view_stored_procedure_arguments (void)
{
  MOP class_mop;
  COLUMN columns[] = {
    {"sp_name", "varchar(255)"},
    {"index_of", "integer"},
    {"arg_name", "varchar(256)"},
    {"data_type", "varchar(16)"},
    {"mode", "varchar(6)"}
  };
  int num_cols = sizeof (columns) / sizeof (columns[0]);
  int i;
  char stmt[2048];
  int error_code = NO_ERROR;

  class_mop = db_create_vclass (CTV_STORED_PROC_ARGS_NAME);
  if (class_mop == NULL)
    {
      error_code = er_errid ();
      return error_code;
    }

  for (i = 0; i < num_cols; i++)
    {
      error_code = db_add_attribute (class_mop, columns[i].name,
				     columns[i].type, NULL);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  sprintf (stmt,
	   "SELECT sp.sp_name, sp.index_of, sp.arg_name,"
	   " CASE sp.data_type"
	   "   WHEN 28 THEN 'CURSOR'"
	   "   ELSE (SELECT dt.type_name FROM %s dt WHERE sp.data_type = dt.type_id)"
	   " END,"
	   " CASE"
	   "   WHEN sp.mode = 1 THEN 'IN'"
	   "   WHEN sp.mode = 2 THEN 'OUT'"
	   "   ELSE 'INOUT'"
	   " END"
	   " FROM %s sp"
	   " ORDER BY sp.sp_name, sp.index_of",
	   CT_DATATYPE_NAME, CT_STORED_PROC_ARGS_NAME);

  error_code = db_add_query_spec (class_mop, stmt);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = au_grant (Au_public_user, class_mop, AU_SELECT, false);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * catcls_vclass_install :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
catcls_vclass_install (void)
{
  CATCLS_FUNCTION clist[] = {
    {"CTV_CLASS_NAME", boot_define_view_class},
    {"CTV_SUPER_CLASS_NAME", boot_define_view_super_class},
    {"CTV_VCLASS_NAME", boot_define_view_vclass},
    {"CTV_ATTRIBUTE_NAME", boot_define_view_attribute},
    {"CTV_ATTR_SD_NAME", boot_define_view_attribute_set_domain},
    {"CTV_METHOD_NAME", boot_define_view_method},
    {"CTV_METHARG_NAME", boot_define_view_method_argument},
    {"CTV_METHARG_SD_NAME", boot_define_view_method_argument_set_domain},
    {"CTV_METHFILE_NAME", boot_define_view_method_file},
    {"CTV_INDEX_NAME", boot_define_view_index},
    {"CTV_INDEXKEY_NAME", boot_define_view_index_key},
    {"CTV_AUTH_NAME", boot_define_view_authorization},
    {"CTV_TRIGGER_NAME", boot_define_view_trigger},
    {"CTV_PARTITION_NAME", boot_define_view_partition},
    {"CTV_STORED_PROC_NAME", boot_define_view_stored_procedure},
    {"CTV_STORED_PROC_ARGS_NAME",
     boot_define_view_stored_procedure_arguments}
  };

  int save;
  size_t i;
  size_t num_vclasses = sizeof (clist) / sizeof (clist[0]);
  int error_code = NO_ERROR;

  AU_DISABLE (save);

  for (i = 0; i < num_vclasses; i++)
    {
      error_code = (clist[i].function) ();
      if (error_code != NO_ERROR)
	{
	  goto end;
	}
    }

end:
  AU_ENABLE (save);

  return error_code;
}

#if defined (SA_MODE)
#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * boot_build_catalog_classes :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   dbname(in) :
 */
int
boot_build_catalog_classes (const char *dbname)
{
  int error_code = NO_ERROR;

  /* check if an old version database */
  if (locator_find_class (CT_CLASS_NAME) != NULL)
    {

      fprintf (stdout,
	       "Database %s already has system catalog class/vclass\n",
	       dbname);
      return 1;
    }
  else
    {
      bool cc_save;

      /* save and catcls_Enable */
      cc_save = catcls_Enable;
      catcls_Enable = false;

      error_code = catcls_class_install ();
      if (error_code == NO_ERROR)
	{
	  error_code = catcls_vclass_install ();
	}
      if (error_code == NO_ERROR)
	{
	  /* add method to db_authorization */
	  au_add_method_check_authorization ();

	  /* mark catalog class/view as a system class */
	  sm_mark_system_class_for_catalog ();

	  if (!tf_Metaclass_class.n_variable)
	    {
	      tf_compile_meta_classes ();
	    }
	  if (catcls_Enable != true)
	    {
	      error_code = catcls_compile_catalog_classes (NULL);
	      if (error_code == NO_ERROR)
		{
		  error_code = sm_force_write_all_classes ();
		  if (error_code == NO_ERROR)
		    {
		      error_code = au_force_write_new_auth ();
		    }
		}
	    }
	}
      /* restore catcls_Enable */
      catcls_Enable = cc_save;
    }

  return error_code;
}

/*
 * boot_destroy_catalog_classes :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   dbname(in) :
 *
 * Note: destroy catalog by reverse order of building
 *
 */
int
boot_destroy_catalog_classes (void)
{
  int error_code = NO_ERROR;
  bool cc_save, save;

  int i;
  MOP classmop;
  const char *classes[] = {
    CT_CLASS_NAME, CT_ATTRIBUTE_NAME, CT_DOMAIN_NAME,
    CT_METHOD_NAME, CT_METHSIG_NAME, CT_METHARG_NAME,
    CT_METHFILE_NAME, CT_QUERYSPEC_NAME, CT_INDEX_NAME,
    CT_INDEXKEY_NAME, CT_CLASSAUTH_NAME, CT_DATATYPE_NAME,
    CT_PARTITION_NAME, CT_STORED_PROC_NAME, CT_STORED_PROC_ARGS_NAME,
    CTV_CLASS_NAME, CTV_SUPER_CLASS_NAME, CTV_VCLASS_NAME,
    CTV_ATTRIBUTE_NAME, CTV_ATTR_SD_NAME, CTV_METHOD_NAME,
    CTV_METHARG_NAME, CTV_METHARG_SD_NAME, CTV_METHFILE_NAME,
    CTV_INDEX_NAME, CTV_INDEXKEY_NAME, CTV_AUTH_NAME,
    CTV_TRIGGER_NAME, CTV_PARTITION_NAME, CTV_STORED_PROC_NAME,
    CTV_STORED_PROC_ARGS_NAME, NULL
  };

  /* check if catalog exists */
  if (locator_find_class (CT_CLASS_NAME) == NULL)
    {
      /* catalog does not exists */
      return NO_ERROR;
    }

  /* save and off catcls_Enable */
  cc_save = catcls_Enable;
  catcls_Enable = false;

  AU_DISABLE (save);

  /* drop method of db_authorization */
  error_code = db_drop_class_method (locator_find_class ("db_authorization"),
				     "check_authorization");
  /* error checking */
  if (error_code != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* drop catalog class/vclass */
  for (i = 0; classes[i] != NULL; i++)
    {
      classmop = locator_find_class (classes[i]);
      if (!classmop)
	{
	  continue;		/* not found */
	}
      /* for vclass, revoke before drop */
      if (db_is_vclass (classmop))
	{
	  error_code = db_revoke (Au_public_user, classmop, AU_SELECT);
	  if (error_code != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}

      /* drop class/view */
      error_code = db_drop_class (classmop);
      if (error_code == ER_OBJ_INVALID_ARGUMENTS)
	{
	  continue;
	}

      /* error checking */
      if (error_code != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

exit_on_error:

  AU_ENABLE (save);

  /* restore catcls_Enable */
  catcls_Enable = cc_save;

  return error_code;
}

/*
 * boot_rebuild_catalog_classes :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   dbname(in) :
 */
int
boot_rebuild_catalog_classes (const char *dbname)
{
  int error_code = NO_ERROR;

  error_code = boot_destroy_catalog_classes ();

  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return boot_build_catalog_classes (dbname);
}
#endif /* ENABLE_UNUSED_FUNCTION */
#endif /* SA_MODE */

#if defined(CS_MODE)
char *
boot_get_host_connected (void)
{
  return boot_Host_connected;
}

int
boot_get_ha_server_state (void)
{
  return boot_Server_credential.ha_server_state;
}
#endif /* CS_MODE */
