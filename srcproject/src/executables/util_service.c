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
 * util_service.c - a front end of service utilities
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#if !defined(WINDOWS)
#include <sys/wait.h>
#endif
#if defined(WINDOWS)
#include <io.h>
#endif
#include "porting.h"
#include "utility.h"
#include "error_code.h"
#include "system_parameter.h"
#include "connection_cl.h"
#if defined(WINDOWS)
#include "wintcp.h"
#endif
#include "environment_variable.h"

typedef enum
{
  SERVICE,
  SERVER,
  BROKER,
  MANAGER,
  REPL_SERVER,
  REPL_AGENT,
  HEARTBEAT,
  UTIL_HELP,
  UTIL_VERSION,
  ADMIN,
} UTIL_SERVICE_INDEX_E;

typedef enum
{
  START,
  STOP,
  RESTART,
  STATUS,
  DEACTIVATE,
  ACTIVATE,
  DEREGISTER,
  LIST,
  RELOAD,
  ON,
  OFF,
} UTIL_SERVICE_COMMAND_E;

typedef enum
{
  SERVICE_START_SERVER,
  SERVICE_START_BROKER,
  SERVICE_START_MANAGER,
  SERVICE_START_REPL_SERVER,
  SERVICE_START_REPL_AGENT,
  SERVER_START_LIST,
  REPL_SERVER_START_LIST,
  REPL_AGENT_START_LIST,
  SERVICE_START_HEARTBEAT
} UTIL_SERVICE_PROPERTY_E;

typedef struct
{
  int option_index;
  const char *option_name;
  int option_mask;
} UTIL_SERVICE_OPTION_MAP_T;

typedef struct
{
  int property_index;
  const char *property_value;
} UTIL_SERVICE_PROPERTY_T;

#define UTIL_TYPE_SERVICE       "service"
#define UTIL_TYPE_SERVER        "server"
#define UTIL_TYPE_BROKER        "broker"
#define UTIL_TYPE_MANAGER       "manager"
#define UTIL_TYPE_REPL_SERVER   "repl_server"
#define UTIL_TYPE_REPL_AGENT    "repl_agent"
#define UTIL_TYPE_HEARTBEAT     "heartbeat"

static UTIL_SERVICE_OPTION_MAP_T us_Service_map[] = {
  {SERVICE, UTIL_TYPE_SERVICE, MASK_SERVICE},
  {SERVER, UTIL_TYPE_SERVER, MASK_SERVER},
  {BROKER, UTIL_TYPE_BROKER, MASK_BROKER},
  {MANAGER, UTIL_TYPE_MANAGER, MASK_MANAGER},
  {REPL_SERVER, UTIL_TYPE_REPL_SERVER, MASK_REPL},
  {REPL_AGENT, UTIL_TYPE_REPL_AGENT, MASK_REPL},
  {HEARTBEAT, UTIL_TYPE_HEARTBEAT, MASK_HEARTBEAT},
  {UTIL_HELP, "--help", MASK_ALL},
  {UTIL_VERSION, "--version", MASK_ALL},
  {ADMIN, UTIL_OPTION_CREATEDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_RENAMEDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_COPYDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_DELETEDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_BACKUPDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_RESTOREDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_ADDVOLDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_SPACEDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_LOCKDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_KILLTRAN, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_OPTIMIZEDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_INSTALLDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_DIAGDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_PATCHDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_CHECKDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_ALTERDBHOST, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_PLANDUMP, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_ESTIMATE_DATA, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_ESTIMATE_INDEX, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_LOADDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_UNLOADDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_COMPACTDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_PARAMDUMP, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_STATDUMP, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_CHANGEMODE, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_COPYLOGDB, MASK_ADMIN},
  {ADMIN, UTIL_OPTION_APPLYLOGDB, MASK_ADMIN},
  {-1, "", MASK_ADMIN}
};

#define COMMAND_TYPE_START      "start"
#define COMMAND_TYPE_STOP       "stop"
#define COMMAND_TYPE_RESTART    "restart"
#define COMMAND_TYPE_STATUS     "status"
#define COMMAND_TYPE_DEACTIVATE "deact"
#define COMMAND_TYPE_ACTIVATE   "act"
#define COMMAND_TYPE_DEREG      "deregister"
#define COMMAND_TYPE_LIST       "list"
#define COMMAND_TYPE_RELOAD     "reload"
#define COMMAND_TYPE_ON         "on"
#define COMMAND_TYPE_OFF        "off"

static UTIL_SERVICE_OPTION_MAP_T us_Command_map[] = {
  {START, COMMAND_TYPE_START, MASK_ALL},
  {STOP, COMMAND_TYPE_STOP, MASK_ALL},
  {RESTART, COMMAND_TYPE_RESTART, MASK_SERVICE | MASK_SERVER | MASK_BROKER},
  {STATUS, COMMAND_TYPE_STATUS, MASK_ALL},
  {ACTIVATE, COMMAND_TYPE_ACTIVATE, MASK_HEARTBEAT},
  {DEACTIVATE, COMMAND_TYPE_DEACTIVATE, MASK_HEARTBEAT},
  {DEREGISTER, COMMAND_TYPE_DEREG, MASK_HEARTBEAT},
  {LIST, COMMAND_TYPE_LIST, MASK_HEARTBEAT},
  {RELOAD, COMMAND_TYPE_RELOAD, MASK_HEARTBEAT},
  {ON, COMMAND_TYPE_ON, MASK_BROKER},
  {OFF, COMMAND_TYPE_OFF, MASK_BROKER},
  {-1, "", MASK_ALL}
};

static UTIL_SERVICE_PROPERTY_T us_Property_map[] = {
  {SERVICE_START_SERVER, NULL},
  {SERVICE_START_BROKER, NULL},
  {SERVICE_START_MANAGER, NULL},
  {SERVICE_START_REPL_SERVER, NULL},
  {SERVICE_START_REPL_AGENT, NULL},
  {SERVER_START_LIST, NULL},
  {REPL_SERVER_START_LIST, NULL},
  {REPL_AGENT_START_LIST, NULL},
  {SERVICE_START_HEARTBEAT, NULL},
  {-1, NULL}
};

static const char **Argv;

static void util_service_usage (int util_type);
static void util_service_version (const char *argv0);
static int load_properties (void);
static void finalize_properties (void);
static const char *get_property (int property_type);
static int parse_arg (UTIL_SERVICE_OPTION_MAP_T * option, const char *arg);
static int process_service (int command_type, bool process_window_service);
static int process_server (int command_type, char *name, bool show_usage);
static int process_broker (int command_type, int argc, const char **argv);
static int process_manager (int command_type);
static int process_repl_server (int command_type, char *name, char *repl_port,
				int argc, const char **argv);
static int process_repl_agent (int command_type, char *name, char *password);
static int process_heartbeat (int command_type, char *name);
static int proc_execute (const char *file, const char *args[],
			 bool wait_child, bool close_output, int *pid);
static int process_master (int command_type);
static void print_message (FILE * output, int message_id, ...);
static void print_result (const char *util_name, int status,
			  int command_type);
static bool is_terminated_process (const int pid);
static char *make_exec_abspath (char *buf, int buf_len, char *cmd);
static const char *command_string (int command_type);

static char *
make_exec_abspath (char *buf, int buf_len, char *cmd)
{
  buf[0] = '\0';

  (void) envvar_bindir_file (buf, buf_len, cmd);

  return buf;
}

static const char *
command_string (int command_type)
{
  const char *command;

  switch (command_type)
    {
    case START:
      command = PRINT_CMD_START;
      break;
    case STATUS:
      command = PRINT_CMD_STATUS;
      break;
    case DEACTIVATE:
      command = PRINT_CMD_DEACTIVATE;
      break;
    case ACTIVATE:
      command = PRINT_CMD_ACTIVATE;
      break;
    case DEREGISTER:
      command = PRINT_CMD_DEREG;
      break;
    case LIST:
      command = PRINT_CMD_LIST;
      break;
    case RELOAD:
      command = PRINT_CMD_RELOAD;
      break;
    case STOP:
    default:
      command = PRINT_CMD_STOP;
      break;
    }

  return command;
}

static void
print_result (const char *util_name, int status, int command_type)
{
  const char *result;

  if (status != NO_ERROR)
    {
      result = PRINT_RESULT_FAIL;
    }
  else
    {
      result = PRINT_RESULT_SUCCESS;
    }

  print_message (stdout, MSGCAT_UTIL_GENERIC_RESULT, util_name,
		 command_string (command_type), result);
}

/*
 * print_message() -
 *
 * return:
 *
 */
static void
print_message (FILE * output, int message_id, ...)
{
  va_list arg_list;
  const char *format;

  format = utility_get_generic_message (message_id);
  va_start (arg_list, message_id);
  vfprintf (output, format, arg_list);
  va_end (arg_list);
}

/*
 * process_admin() - process admin utility
 *
 * return:
 *
 */
static int
process_admin (int argc, char **argv)
{
  char **copy_argv;
  int status;

  copy_argv = (char **) malloc (sizeof (char *) * (argc + 1));
  if (copy_argv == NULL)
    {
      return ER_GENERIC_ERROR;
    }

  memcpy (copy_argv, argv, sizeof (char *) * argc);
  copy_argv[0] = argv[0];
  copy_argv[argc] = 0;
  status = proc_execute (UTIL_ADMIN_NAME, (const char **) copy_argv, true,
			 false, NULL);
  free (copy_argv);

  return status;
}

/*
 * main() - a service utility's entry point
 *
 * return:
 *
 * NOTE:
 */
int
main (int argc, char *argv[])
{
  int util_type, command_type;
  int status;
#if defined (DO_NOT_USE_CUBRIDENV)
  char *envval;
  char path[PATH_MAX];

  envval = getenv (envvar_prefix ());
  if (envval != NULL)
    {
      fprintf (stderr, "CAUTION : "
	       "The environment variable $%s is set to %s.\n"
	       "          But, built-in prefix (%s) will be used.\n\n",
	       envvar_prefix (), envval, envvar_root ());
    }

  envval = envvar_get ("DATABASES");
  if (envval != NULL)
    {
      fprintf (stderr, "CAUTION : "
	       "The environment variable $%s_%s is set to %s.\n"
	       "          But, built-in prefix (%s) will be used.\n\n",
	       envvar_prefix (), "DATABASES", envval,
	       envvar_vardir_file (path, PATH_MAX, ""));
    }
#endif

  Argv = (const char **) argv;
  if (argc == 2)
    {
      if (parse_arg (us_Service_map, (char *) argv[1]) == UTIL_VERSION)
	{
	  util_service_version (argv[0]);
	  return EXIT_SUCCESS;
	}
    }

  /* validate the number of arguments to avoid klocwork's error message */
  if (argc < 2 || argc > 1024)
    {
      util_type = -1;
      goto usage;
    }

  util_type = parse_arg (us_Service_map, (char *) argv[1]);
  if (util_type == ER_GENERIC_ERROR)
    {
      util_type = parse_arg (us_Service_map, (char *) argv[2]);
      if (util_type == ER_GENERIC_ERROR)
	{
	  print_message (stderr, MSGCAT_UTIL_GENERIC_SERVICE_INVALID_NAME,
			 argv[1]);
	  goto error;
	}
    }

  if (util_type == ADMIN)
    {
      return process_admin (argc, argv);
    }

  if (util_type == UTIL_HELP)
    {
      util_type = -1;
      goto usage;
    }

  if (argc < 3)
    {
      goto usage;
    }

  command_type = parse_arg (us_Command_map, (char *) argv[2]);
  if (command_type == ER_GENERIC_ERROR)
    {
      command_type = parse_arg (us_Command_map, (char *) argv[1]);
      if (command_type == ER_GENERIC_ERROR)
	{
	  print_message (stderr, MSGCAT_UTIL_GENERIC_SERVICE_INVALID_CMD,
			 argv[2]);
	  goto error;
	}
    }
  else
    {
      int util_mask = us_Service_map[util_type].option_mask;
      int command_mask = us_Command_map[command_type].option_mask;
      if ((util_mask & command_mask) == 0)
	{
	  print_message (stderr, MSGCAT_UTIL_GENERIC_SERVICE_INVALID_CMD,
			 argv[2]);
	  goto error;
	}
    }

  if (load_properties () != NO_ERROR)
    {
      print_message (stderr, MSGCAT_UTIL_GENERIC_SERVICE_PROPERTY_FAIL);
      return EXIT_FAILURE;
    }

#if defined(WINDOWS)
  if (css_windows_startup () < 0)
    {
      printf ("Unable to initialize Winsock.\n");
      goto error;
    }
#endif /* WINDOWS */

  switch (util_type)
    {
    case SERVICE:
      {
	bool process_window_service = false;

#if defined(WINDOWS)
	process_window_service = true;

	if (argc > 3
	    && strcmp ((char *) argv[3], "--for-windows-service") == 0)
	  {
	    process_window_service = false;
	  }
#endif
	status = process_service (command_type, process_window_service);
      }
      break;
    case SERVER:
      status =
	process_server (command_type, argc > 3 ? (char *) argv[3] : NULL,
			true);
      break;
    case BROKER:
      status = process_broker (command_type, argc - 3,
			       (const char **) &argv[3]);
      break;
    case MANAGER:
      status = process_manager (command_type);
      break;
    case REPL_SERVER:
#if defined (WINDOWS)
      print_message (stderr, MSGCAT_UTIL_GENRIC_REPL_NOT_SUPPORTED);
      return EXIT_FAILURE;
#else /* WINDOWS */
      status = process_repl_server (command_type, (char *) argv[3],
				    (char *) argv[4], argc,
				    (const char **) argv);
#endif /* !WINDOWS */
      break;
    case REPL_AGENT:
#if defined (WINDOWS)
      print_message (stderr, MSGCAT_UTIL_GENRIC_REPL_NOT_SUPPORTED);
      return EXIT_FAILURE;
#else /* WINDOWS */
      status = process_repl_agent (command_type, (char *) argv[3],
				   (char *) argv[4]);
#endif /* !WINDOWs */
      break;
    case HEARTBEAT:
#if defined(WINDOWS)
      /* TODO : define message catalog for heartbeat */
      return EXIT_FAILURE;
#else
      status = process_heartbeat (command_type, (argc > 3) ? argv[3] : NULL);
#endif /* !WINDOWs */
      break;
    default:
      goto usage;
    }
  return status;

usage:
  util_service_usage (util_type);
error:
  finalize_properties ();
#if defined(WINDOWS)
  css_windows_shutdown ();
#endif /* WINDOWS */
  return EXIT_FAILURE;
}

/*
 * util_service_usage - display an usage of this utility
 *
 * return:
 *
 * NOTE:
 */
static void
util_service_usage (int util_type)
{
  char *exec_name;

  if (util_type < 0)
    {
      util_type = 0;
    }
  else
    {
      util_type++;
    }

  exec_name = basename ((char *) Argv[0]);
  print_message (stdout, MSGCAT_UTIL_GENERIC_CUBRID_USAGE + util_type,
		 VERSION, exec_name, exec_name, exec_name);
}

/*
 * util_service_version - display a version of this utility
 *
 * return:
 *
 * NOTE:
 */
static void
util_service_version (const char *argv0)
{
  const char *exec_name;

  exec_name = basename ((char *) argv0);
  print_message (stdout, MSGCAT_UTIL_GENERIC_VERSION, exec_name, VERSION);
}


#if defined(WINDOWS)
static int
proc_execute (const char *file, const char *args[], bool wait_child,
	      bool close_output, int *out_pid)
{
  STARTUPINFO si;
  PROCESS_INFORMATION pi;
  int i, cmd_arg_len;
  char cmd_arg[1024];
  char executable_path[PATH_MAX];
  int ret_code = NO_ERROR;
  bool inherited_handle = TRUE;

  if (out_pid)
    {
      *out_pid = 0;
    }

  (void) envvar_bindir_file (executable_path, PATH_MAX, file);

  for (i = 0, cmd_arg_len = 0; args[i]; i++)
    {
      cmd_arg_len += sprintf (cmd_arg + cmd_arg_len, "\"%s\" ", args[i]);
    }

  GetStartupInfo (&si);
  if (close_output)
    {
      si.dwFlags = si.dwFlags | STARTF_USESTDHANDLES;
      si.hStdOutput = NULL;
      si.hStdError = NULL;
      inherited_handle = FALSE;
    }

  if (!CreateProcess (executable_path, cmd_arg, NULL, NULL, inherited_handle,
		      0, NULL, NULL, &si, &pi))
    {
      return ER_FAILED;
    }

  if (wait_child)
    {
      DWORD status = 0;
      WaitForSingleObject (pi.hProcess, INFINITE);
      GetExitCodeProcess (pi.hProcess, &status);
      ret_code = status;
    }
  else
    {
      if (out_pid)
	{
	  *out_pid = pi.dwProcessId;
	}
    }

  CloseHandle (pi.hProcess);
  CloseHandle (pi.hThread);
  return ret_code;
}

#else
static int
proc_execute (const char *file, const char *args[], bool wait_child,
	      bool close_output, int *out_pid)
{
  pid_t pid;
  char executable_path[PATH_MAX];

  if (out_pid)
    {
      *out_pid = 0;
    }

  (void) envvar_bindir_file (executable_path, PATH_MAX, file);

  /* do not process SIGCHLD, a child process will be defunct */
  if (wait_child)
    {
      signal (SIGCHLD, SIG_DFL);
    }
  else
    {
      signal (SIGCHLD, SIG_IGN);
    }

  pid = fork ();
  if (pid == -1)
    {
      perror ("fork");
      return ER_GENERIC_ERROR;
    }
  else if (pid == 0)
    {
      /* a child process handle SIGCHLD to SIG_DFL */
      signal (SIGCHLD, SIG_DFL);
      if (close_output)
	{
	  fclose (stdout);
	  fclose (stderr);
	}
      if (execv (executable_path, (char *const *) args) == -1)
	{
	  perror ("execv");
	  return ER_GENERIC_ERROR;
	}
    }
  else
    {
      int status = 0;

      /* sleep (0); */
      if (wait_child)
	{
	  if (waitpid (pid, &status, 0) == -1)
	    {
	      perror ("waitpid");
	      return ER_GENERIC_ERROR;
	    }
	}
      else
	{
	  /*sleep (3); */
	  if (out_pid)
	    {
	      *out_pid = pid;
	    }
	  return NO_ERROR;
	}

      if (WIFEXITED (status))
	{
	  return WEXITSTATUS (status);
	}
    }
  return ER_GENERIC_ERROR;
}
#endif

/*
 * process_master -
 *
 * return:
 *
 *      command_type(in):
 */
static int
process_master (int command_type)
{
  int status = NO_ERROR;
  int master_port = prm_get_master_port_id ();

  switch (command_type)
    {
    case START:
      {
	print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		       PRINT_MASTER_NAME, PRINT_CMD_START);
	if (!css_does_master_exist (master_port))
	  {
	    const char *args[] = { UTIL_MASTER_NAME, NULL };
	    status = proc_execute (UTIL_MASTER_NAME, args, false, false,
				   NULL);
	    /* The master process needs a few seconds to bind port */
	    sleep (2);
	    status = css_does_master_exist (master_port) ?
	      NO_ERROR : ER_GENERIC_ERROR;
	    print_result (PRINT_MASTER_NAME, status, command_type);
	  }
	else
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			   PRINT_MASTER_NAME);
	  }
      }
      break;
    case STOP:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_MASTER_NAME, PRINT_CMD_STOP);
      if (css_does_master_exist (master_port))
	{
	  const char *args[] = { UTIL_COMMDB_NAME, COMMDB_ALL_STOP, NULL };
	  status = proc_execute (UTIL_COMMDB_NAME, args, true, false, NULL);
	  print_result (PRINT_MASTER_NAME, status, command_type);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      break;
    }
  return status;
}

#if defined(WINDOWS)
/*
 * is_windwos_service_running -
 *
 * return:
 *
 *      sleep_time(in):
 *
 * NOTE:
 */
static bool
is_windows_service_running (unsigned int sleep_time)
{
  FILE *input;
  char buf[32], cmd[PATH_MAX];

  sleep (sleep_time);

  make_exec_abspath (cmd, PATH_MAX,
		     (char *) UTIL_WIN_SERVICE_CONTROLLER_NAME " " "-status");

  input = popen (cmd, "r");
  if (input == NULL)
    {
      return false;
    }

  memset (buf, '\0', sizeof (buf));

  if ((fgets (buf, 32, input) == NULL)
      || strncmp (buf, "SERVICE_RUNNING", 15) != 0)
    {
      pclose (input);
      return false;
    }

  pclose (input);

  return true;
}
#endif

/*
 * process_service -
 *
 * return:
 *
 *      command_type(in):
 *      process_window_service(in):
 *
 * NOTE:
 */
static int
process_service (int command_type, bool process_window_service)
{
  int status;

  switch (command_type)
    {
    case START:
      if (process_window_service)
	{
#if defined(WINDOWS)
	  if (!is_windows_service_running (0))
	    {
	      const char *args[] =
		{ UTIL_WIN_SERVICE_CONTROLLER_NAME, "-start", NULL };

	      proc_execute (UTIL_WIN_SERVICE_CONTROLLER_NAME, args, true,
			    false, NULL);
	      status =
		is_windows_service_running (0) ? NO_ERROR : ER_GENERIC_ERROR;
	      print_result (PRINT_SERVICE_NAME, status, command_type);
	    }
	  else
	    {
	      print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			     PRINT_SERVICE_NAME);
	      return NO_ERROR;
	    }
#endif
	}
      else
	{
	  status = process_master (command_type);
	  if (strcmp (get_property (SERVICE_START_SERVER), PROPERTY_ON) == 0)
	    {
	      status = process_server (command_type, NULL, false);
	    }
	  if (strcmp (get_property (SERVICE_START_BROKER), PROPERTY_ON) == 0)
	    {
	      status = process_broker (command_type, 0, NULL);
	    }
	  if (strcmp (get_property (SERVICE_START_MANAGER), PROPERTY_ON) == 0)
	    {
	      status = process_manager (command_type);
	    }
	  if (strcmp (get_property (SERVICE_START_REPL_SERVER), PROPERTY_ON)
	      == 0)
	    {
	      status = process_repl_server (command_type, NULL, 0, 0, 0);
	    }
	  if (strcmp (get_property (SERVICE_START_REPL_AGENT), PROPERTY_ON) ==
	      0)
	    {
	      status = process_repl_agent (command_type, NULL, NULL);
	    }
	}
      break;
    case STOP:
      if (process_window_service)
	{
#if defined(WINDOWS)
	  if (is_windows_service_running (0))
	    {
	      const char *args[] =
		{ UTIL_WIN_SERVICE_CONTROLLER_NAME, "-stop", NULL };

	      proc_execute (UTIL_WIN_SERVICE_CONTROLLER_NAME, args, true,
			    false, NULL);
	      status =
		is_windows_service_running (0) ? ER_GENERIC_ERROR : NO_ERROR;
	      print_result (PRINT_SERVICE_NAME, status, command_type);
	    }
	  else
	    {
	      print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			     PRINT_SERVICE_NAME);
	      return NO_ERROR;
	    }
#endif
	}
      else
	{
	  if (strcmp (get_property (SERVICE_START_SERVER), PROPERTY_ON) == 0)
	    {
	      status = process_server (command_type, NULL, false);
	    }
	  if (strcmp (get_property (SERVICE_START_BROKER), PROPERTY_ON) == 0)
	    {
	      status = process_broker (command_type, 0, NULL);
	    }
	  if (strcmp (get_property (SERVICE_START_MANAGER), PROPERTY_ON) == 0)
	    {
	      status = process_manager (command_type);
	    }
	  if (strcmp (get_property (SERVICE_START_REPL_SERVER), PROPERTY_ON)
	      == 0)
	    {
	      status = process_repl_server (command_type, NULL, 0, 0, 0);
	    }
	  if (strcmp (get_property (SERVICE_START_REPL_AGENT), PROPERTY_ON) ==
	      0)
	    {
	      status = process_repl_agent (command_type, NULL, NULL);
	    }
	  status = process_master (command_type);
	}
      break;
    case RESTART:
      status = process_service (STOP, process_window_service);
      status = process_service (START, process_window_service);
      break;
    case STATUS:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_MASTER_NAME, PRINT_CMD_STATUS);
      if (css_does_master_exist (prm_get_master_port_id ()))
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}

      {
	const char *args[] = { "-b" };
	status = process_server (command_type, NULL, false);
	status = process_broker (command_type, 1, args);
	status = process_manager (command_type);
	status = process_repl_server (command_type, NULL, 0, 0, 0);
      }
      break;
    default:
      return ER_GENERIC_ERROR;
    }
  return status == ER_GENERIC_ERROR ? EXIT_FAILURE : status;
}

/*
 * check_server -
 *
 * return:
 *
 *      type(in):
 *      server_name(in):
 */
static bool
check_server (const char *type, const char *server_name)
{
  FILE *input;
  char buf[4096], *token, *save_ptr, *delim = (char *) " ";
  char cmd[PATH_MAX];

  make_exec_abspath (cmd, PATH_MAX,
		     (char *) UTIL_COMMDB_NAME " " COMMDB_ALL_STATUS);
  input = popen (cmd, "r");
  if (input == NULL)
    {
      return false;
    }

  while (fgets (buf, 4096, input) != NULL)
    {
      token = strtok_r (buf, delim, &save_ptr);
      if (token == NULL)
	{
	  continue;
	}

      if (strcmp (type, CHECK_SERVER) == 0)
	{
	  if (strcmp (token, CHECK_SERVER) != 0
	      && strcmp (token, CHECK_HA_SERVER) != 0)
	    {
	      continue;
	    }
	}
      else
	{
	  if (strcmp (token, type) != 0)
	    {
	      continue;
	    }
	}

      token = strtok_r (NULL, delim, &save_ptr);
      if (token != NULL && strcmp (token, server_name) == 0)
	{
	  pclose (input);
	  return true;
	}
    }
  pclose (input);
  return false;
}

/*
 * is_server_running -
 *
 * return:
 *
 *      type(in):
 *      server_name(in):
 *      pid(in):
 */
static bool
is_server_running (const char *type, const char *server_name, int pid)
{
  if (!css_does_master_exist (prm_get_master_port_id ()))
    {
      return false;
    }

  if (pid <= 0)
    {
      return check_server (type, server_name);
    }

  while (true)
    {
      if (!is_terminated_process (pid))
	{
	  if (check_server (type, server_name))
	    {
	      return true;
	    }
	  sleep (1);

	  /* A child process is defunct because the SIGCHLD signal ignores. */
	  /*
	     if (waitpid (pid, &status, WNOHANG) == -1)
	     {
	     perror ("waitpid");
	     }
	   */
	}
      else
	{
	  return false;
	}
    }
}

/*
 * process_server -
 *
 * return:
 *
 *      command_type(in):
 *      name(in):
 *      show_usage(in):
 *
 * NOTE:
 */
static int
process_server (int command_type, char *name, bool show_usage)
{
  char buf[4096];
  char *list, *token, *save;
  const char *delim = " ,";
  int status = NO_ERROR;
  int master_port = prm_get_master_port_id ();

  /* A string is copyed because strtok_r() modify an original string. */
  if (name == NULL)
    {
      strncpy (buf, us_Property_map[SERVER_START_LIST].property_value,
	       sizeof (buf) - 1);
    }
  else
    {
      strncpy (buf, name, sizeof (buf) - 1);
    }

  if (command_type != STATUS && strlen (buf) == 0)
    {
      if (show_usage)
	{
	  util_service_usage (SERVER);
	}
      return ER_GENERIC_ERROR;
    }

  switch (command_type)
    {
    case START:
      if (!css_does_master_exist (master_port))
	{
	  status = process_master (command_type);
	}
      for (list = buf;; list = NULL)
	{
	  token = strtok_r (list, delim, &save);
	  if (token == NULL)
	    {
	      break;
	    }
	  print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_3S,
			 PRINT_SERVER_NAME, PRINT_CMD_START, token);
	  if (is_server_running (CHECK_SERVER, token, 0))
	    {
	      print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_2S,
			     PRINT_SERVER_NAME, token);
	      continue;
	    }
	  else
	    {
	      int pid;
	      const char *args[] = { UTIL_CUBRID_NAME, token, NULL };
	      status = proc_execute (UTIL_CUBRID_NAME, args, false, false,
				     &pid);
	      status = is_server_running (CHECK_SERVER, token, pid) ?
		NO_ERROR : ER_GENERIC_ERROR;
	      print_result (PRINT_SERVER_NAME, status, command_type);
	    }
	}
      break;
    case STOP:
      for (list = buf;; list = NULL)
	{
	  token = strtok_r (list, delim, &save);
	  if (token == NULL)
	    {
	      break;
	    }
	  print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_3S,
			 PRINT_SERVER_NAME, PRINT_CMD_STOP, token);
	  if (is_server_running (CHECK_SERVER, token, 0))
	    {
	      const char *args[] =
		{ UTIL_COMMDB_NAME, COMMDB_SERVER_STOP, token, NULL };
	      status = proc_execute (UTIL_COMMDB_NAME, args, true, false,
				     NULL);
	      print_result (PRINT_SERVER_NAME, status, command_type);
	    }
	  else
	    {
	      print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_2S,
			     PRINT_SERVER_NAME, token);
	    }
	}
      break;
    case RESTART:
      status = process_server (STOP, name, show_usage);
      status = process_server (START, name, show_usage);
      break;
    case STATUS:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_SERVER_NAME, PRINT_CMD_STATUS);
      if (css_does_master_exist (master_port))
	{
	  const char *args[] =
	    { UTIL_COMMDB_NAME, COMMDB_SERVER_STATUS, NULL };
	  status = proc_execute (UTIL_COMMDB_NAME, args, true, false, NULL);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      break;
    default:
      return ER_GENERIC_ERROR;
    }
  return status;
}

/*
 * is_broker_running -
 *
 * return:
 *
 */
static bool
is_broker_running (void)
{
  const char *args[] = { UTIL_MONITOR_NAME, 0 };
  int status = proc_execute (UTIL_MONITOR_NAME, args, true, true, NULL);
  return status == NO_ERROR ? true : false;
}

/*
 * process_broker -
 *
 * return:
 *
 *      command_type(in):
 *      name(in):
 *
 */
static int
process_broker (int command_type, int argc, const char **argv)
{
  int status = NO_ERROR;

  switch (command_type)
    {
    case START:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_BROKER_NAME, PRINT_CMD_START);
      if (is_broker_running () == true)
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			 PRINT_BROKER_NAME);
	  return NO_ERROR;
	}
      else
	{
	  const char *args[] = { UTIL_BROKER_NAME, COMMAND_TYPE_START, NULL };
	  status = proc_execute (UTIL_BROKER_NAME, args, true, false, NULL);
	  print_result (PRINT_BROKER_NAME, status, command_type);
	}
      break;
    case STOP:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_BROKER_NAME, PRINT_CMD_STOP);
      if (is_broker_running () != true)
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_BROKER_NAME);
	  return NO_ERROR;
	}
      else
	{
	  const char *args[] = { UTIL_BROKER_NAME, COMMAND_TYPE_STOP, NULL };
	  status = proc_execute (UTIL_BROKER_NAME, args, true, false, NULL);
	  print_result (PRINT_BROKER_NAME, status, command_type);
	}
      break;
    case RESTART:
      process_broker (STOP, 0, NULL);
      process_broker (START, 0, NULL);
      break;
    case STATUS:
      {
	int i;
	const char **args;

	print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		       PRINT_BROKER_NAME, PRINT_CMD_STATUS);
	if (is_broker_running ())
	  {
	    args = (const char **) malloc (sizeof (char *) * (argc + 2));
	    if (args == NULL)
	      {
		return ER_GENERIC_ERROR;
	      }

	    args[0] = PRINT_BROKER_NAME " " PRINT_CMD_STATUS;
	    for (i = 0; i < argc; i++)
	      {
		args[i + 1] = argv[i];
	      }
	    args[argc + 1] = NULL;
	    status =
	      proc_execute (UTIL_MONITOR_NAME, args, true, false, NULL);

	    free (args);
	  }
	else
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			   PRINT_BROKER_NAME);
	  }
      }
      break;
    case ON:
      {
	const char *args[] =
	  { UTIL_BROKER_NAME, COMMAND_TYPE_ON, argv[0], NULL };
	if (argc <= 0)
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	    return ER_GENERIC_ERROR;
	  }
	status = proc_execute (UTIL_BROKER_NAME, args, true, false, NULL);
      }
      break;
    case OFF:
      {
	const char *args[] =
	  { UTIL_BROKER_NAME, COMMAND_TYPE_OFF, argv[0], NULL };
	if (argc <= 0)
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	    return ER_GENERIC_ERROR;
	  }
	status = proc_execute (UTIL_BROKER_NAME, args, true, false, NULL);
      }
      break;
    default:
      return ER_GENERIC_ERROR;
    }
  return status;
}

/*
 * is_manager_running -
 *
 * return:
 *
 */
static bool
is_manager_running (unsigned int sleep_time)
{
  FILE *input;
  char buf[16], cmd[PATH_MAX];

  sleep (sleep_time);

  /* check cub_auto */
  make_exec_abspath (cmd, PATH_MAX, (char *) UTIL_CUB_AUTO_NAME " " "getpid");
  input = popen (cmd, "r");
  if (input == NULL)
    {
      return false;
    }

  memset (buf, '\0', sizeof (buf));
  if ((fgets (buf, 16, input) == NULL) || atoi (buf) <= 0)
    {
      pclose (input);
      return false;
    }

  pclose (input);

  /* chech cub_js */
  make_exec_abspath (cmd, PATH_MAX, (char *) UTIL_CUB_JS_NAME " " "getpid");
  input = popen (cmd, "r");
  if (input == NULL)
    {
      return false;
    }

  memset (buf, '\0', sizeof (buf));
  if ((fgets (buf, 16, input) == NULL) || atoi (buf) <= 0)
    {
      pclose (input);
      return false;
    }

  pclose (input);

  return true;
}

/*
 * process_manager -
 *
 * return:
 *
 *      command_type(in):
 *      cms_port(in):
 *
 */
static int
process_manager (int command_type)
{
  int cub_auto, cub_js, status;
  char cub_auto_path[PATH_MAX];
  char cub_js_path[PATH_MAX];
  struct stat stbuf;

  print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		 PRINT_MANAGER_NAME, command_string (command_type));

  (void) envvar_bindir_file (cub_auto_path, PATH_MAX, UTIL_CUB_AUTO_NAME);
  (void) envvar_bindir_file (cub_js_path, PATH_MAX, UTIL_CUB_JS_NAME);
  if (stat (cub_auto_path, &stbuf) == -1 || stat (cub_js_path, &stbuf) == -1)
    {
      printf ("cubrid manager server is not installed.\n");
      return NO_ERROR;
    }

  status = NO_ERROR;
  switch (command_type)
    {
    case START:
      if (!is_manager_running (0))
	{
	  {
	    const char *args[] =
	      { UTIL_CUB_AUTO_NAME, COMMAND_TYPE_START, NULL };
	    cub_auto = proc_execute (UTIL_CUB_AUTO_NAME, args, false, false,
				     NULL);
	  }
	  {
	    const char *args[] =
	      { UTIL_CUB_JS_NAME, COMMAND_TYPE_START, NULL };
	    cub_js =
	      proc_execute (UTIL_CUB_JS_NAME, args, false, false, NULL);
	  }
	  status = is_manager_running (3) ? NO_ERROR : ER_GENERIC_ERROR;
	  print_result (PRINT_MANAGER_NAME, status, command_type);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			 PRINT_MANAGER_NAME);
	  return NO_ERROR;
	}
      break;
    case STOP:
      if (is_manager_running (0))
	{
	  {
	    const char *args[] =
	      { UTIL_CUB_AUTO_NAME, COMMAND_TYPE_STOP, NULL };
	    cub_auto = proc_execute (UTIL_CUB_AUTO_NAME, args, true, false,
				     NULL);
	  }
	  {
	    const char *args[] =
	      { UTIL_CUB_JS_NAME, COMMAND_TYPE_STOP, NULL };
	    cub_js = proc_execute (UTIL_CUB_JS_NAME, args, true, false, NULL);
	  }
	  status = is_manager_running (2) ? ER_GENERIC_ERROR : NO_ERROR;
	  print_result (PRINT_MANAGER_NAME, status, command_type);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MANAGER_NAME);
	  return NO_ERROR;
	}
      break;
    case STATUS:
      {
	if (is_manager_running (0))
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			   PRINT_MANAGER_NAME);
	  }
	else
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			   PRINT_MANAGER_NAME);
	  }
      }
      break;
    default:
      return ER_GENERIC_ERROR;
    }
  return status;
}

/*
 * process_repl_server -
 *
 * return:
 *
 *      command_type(in):
 *      name(in):
 *      repl_port(in):
 *
 */
static int
process_repl_server (int command_type, char *name, char *repl_port,
		     int argc, const char **argv)
{
  int status = NO_ERROR;
  int count;

  switch (command_type)
    {
    case START:
      count = argc - 5;
      if (count > 4 || count % 2 != 0)
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	  return ER_GENERIC_ERROR;
	}
      if (name == NULL || repl_port == NULL)
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	  return ER_GENERIC_ERROR;
	}
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_3S,
		     PRINT_REPL_SERVER_NAME, PRINT_CMD_START, name);
      if (is_server_running (CHECK_REPL_SERVER, name, 0))
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			 PRINT_REPL_SERVER_NAME);
	  return NO_ERROR;
	}
      else
	{
	  const char *args[] = { UTIL_REPL_SERVER_NAME, "-d", name,
	    "-p", repl_port, NULL, NULL, NULL, NULL, NULL
	  };
	  if (count >= 2)
	    {
	      args[5] = argv[5];
	      args[6] = argv[6];
	    }
	  if (count == 4)
	    {
	      args[7] = argv[7];
	      args[8] = argv[8];
	    }
	  status = proc_execute (UTIL_REPL_SERVER_NAME, args, true, false,
				 NULL);
	  print_result (PRINT_REPL_SERVER_NAME, status, command_type);
	}
      break;
    case STOP:
      if (name == NULL)
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	  return ER_GENERIC_ERROR;
	}
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_3S,
		     PRINT_REPL_SERVER_NAME, PRINT_CMD_STOP, name);
      if (is_server_running (CHECK_REPL_SERVER, name, 0))
	{
	  const char *args[] = { UTIL_COMMDB_NAME, "-K", name, NULL };
	  status = proc_execute (UTIL_COMMDB_NAME, args, true, false, NULL);
	  print_result (PRINT_REPL_SERVER_NAME, status, command_type);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_REPL_SERVER_NAME);
	  return NO_ERROR;
	}
      break;
    case STATUS:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_REPL_NAME, PRINT_CMD_STATUS);
      if (css_does_master_exist (prm_get_master_port_id ()))
	{
	  const char *args[] = { UTIL_COMMDB_NAME, COMMDB_REPL_STATUS, NULL };
	  status = proc_execute (UTIL_COMMDB_NAME, args, true, false, NULL);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      break;
    default:
      return ER_GENERIC_ERROR;
    }
  return status;
}

/*
 * process_repl_agent -
 *
 * return:
 *
 *      command_type(in):
 *      name(in):
 *      password(in):
 *
 */
static int
process_repl_agent (int command_type, char *name, char *password)
{
  int status = NO_ERROR;

  switch (command_type)
    {
    case START:
      {
	if (name == NULL)
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	    return ER_GENERIC_ERROR;
	  }
	print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_3S,
		       PRINT_REPL_AGENT_NAME, PRINT_CMD_START, name);
	if (is_server_running (CHECK_REPL_AGENT, name, 0))
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			   PRINT_REPL_AGENT_NAME);
	    return NO_ERROR;
	  }
	else
	  {
	    const char *args[] = { UTIL_REPL_AGENT_NAME, "-d", name,
	      password != NULL ? "-p" : NULL, password, NULL
	    };
	    status = proc_execute (UTIL_REPL_AGENT_NAME, args, true, false,
				   NULL);
	    print_result (PRINT_REPL_AGENT_NAME, status, command_type);
	  }
      }
      break;
    case STOP:
      {
	if (name == NULL)
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	    return ER_GENERIC_ERROR;
	  }
	print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_3S,
		       PRINT_REPL_AGENT_NAME, PRINT_CMD_STOP, name);
	if (is_server_running (CHECK_REPL_AGENT, name, 0))
	  {
	    const char *args[] = { UTIL_COMMDB_NAME, "-k", name, NULL };
	    status = proc_execute (UTIL_COMMDB_NAME, args, true, false, NULL);
	    print_result (PRINT_REPL_AGENT_NAME, status, command_type);
	  }
	else
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			   CHECK_REPL_AGENT);
	    return NO_ERROR;
	  }
      }
      break;
    case STATUS:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_REPL_NAME, PRINT_CMD_STATUS);
      if (css_does_master_exist (prm_get_master_port_id ()))
	{
	  const char *args[] = { UTIL_COMMDB_NAME, COMMDB_REPL_STATUS, NULL };
	  status = proc_execute (UTIL_COMMDB_NAME, args, true, false, NULL);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      break;
    default:
      return ER_GENERIC_ERROR;
    }
  return status;
}

/*
 * process_heartbeat -
 *
 * return:
 *
 *      command_type(in):
 *      name(in):
 *      password(in):
 *
 */
static int
process_heartbeat (int command_type, char *name)
{
  int status = NO_ERROR;
  int master_port = prm_get_master_port_id ();

  switch (command_type)
    {
    case DEACTIVATE:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_HEARTBEAT_NAME, PRINT_CMD_DEACTIVATE);
      if (css_does_master_exist (master_port))
	{
	  const char *args[] =
	    { UTIL_COMMDB_NAME, COMMDB_HA_DEACTIVATE, NULL };
	  status = proc_execute (UTIL_COMMDB_NAME, args, true, false, NULL);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      break;
    case ACTIVATE:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_HEARTBEAT_NAME, PRINT_CMD_ACTIVATE);
      if (css_does_master_exist (master_port))
	{
	  const char *args[] = { UTIL_COMMDB_NAME, COMMDB_HA_ACTIVATE, NULL };
	  status = proc_execute (UTIL_COMMDB_NAME, args, true, false, NULL);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      break;
    case DEREGISTER:
      if (name == NULL)
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	  return ER_GENERIC_ERROR;
	}

      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_3S,
		     PRINT_HEARTBEAT_NAME, PRINT_CMD_DEREG, name);
      if (css_does_master_exist (master_port))
	{
	  const char *args[] =
	    { UTIL_COMMDB_NAME, COMMDB_HA_DEREG, name, NULL };
	  status = proc_execute (UTIL_COMMDB_NAME, args, true, false, NULL);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      break;
    case LIST:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_HEARTBEAT_NAME, PRINT_CMD_LIST);
      if (css_does_master_exist (master_port))
	{
	  const char *node_list_args[] =
	    { UTIL_COMMDB_NAME, COMMDB_HA_NODE_LIST, NULL };
	  const char *proc_list_args[] =
	    { UTIL_COMMDB_NAME, COMMDB_HA_PROC_LIST, NULL };

	  status =
	    proc_execute (UTIL_COMMDB_NAME, node_list_args, true, false,
			  NULL);
	  if (status != NO_ERROR)
	    {
	      return status;
	    }

	  status =
	    proc_execute (UTIL_COMMDB_NAME, proc_list_args, true, false,
			  NULL);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      break;
    case RELOAD:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_HEARTBEAT_NAME, PRINT_CMD_RELOAD);
      if (css_does_master_exist (master_port))
	{
	  const char *args[] = { UTIL_COMMDB_NAME, COMMDB_HA_RELOAD, NULL };
	  status = proc_execute (UTIL_COMMDB_NAME, args, true, false, NULL);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      break;
    default:
      return ER_GENERIC_ERROR;
    }
  return status;

}

/*
 * parse_arg -
 *
 * return:
 *
 *      option(in):
 *      arg(in):
 *
 */
static int
parse_arg (UTIL_SERVICE_OPTION_MAP_T * option, const char *arg)
{
  int i;

  if (arg == NULL || arg[0] == 0)
    {
      return ER_GENERIC_ERROR;
    }
  for (i = 0; option[i].option_index != -1; i++)
    {
      if (strcasecmp (option[i].option_name, arg) == 0)
	{
	  return option[i].option_index;
	}
    }
  return ER_GENERIC_ERROR;
}

/*
 * load_properties -
 *
 * return:
 *
 * NOTE:
 */
static int
load_properties (void)
{
  char service_list[4096];
  char server_list[4096];

  bool server_flag = false;
  bool broker_flag = false;
  bool manager_flag = false;
  bool repl_server_flag = false;
  bool repl_agent_flag = false;
  char *start_server_list = NULL;

  if (sysprm_load_and_init (NULL, NULL) != NO_ERROR)
    {
      return ER_GENERIC_ERROR;
    }

  strcpy (service_list, "service::service");
  if (sysprm_obtain_parameters (service_list, 4096) == NO_ERROR)
    {
      char *save_ptr1, *save_ptr2, *value, *util;
      char *delim = (char *) "\t=\"";
      value = strtok_r (service_list, delim, &save_ptr1);
      value = strtok_r (NULL, delim, &save_ptr1);
      if (value != NULL)
	{
	  for (util = value;; util = NULL)
	    {
	      util = strtok_r (util, " \t,", &save_ptr2);
	      if (util == NULL)
		{
		  break;
		}

	      if (strcmp (util, UTIL_TYPE_SERVER) == 0)
		{
		  server_flag = true;
		}
	      else if (strcmp (util, UTIL_TYPE_BROKER) == 0)
		{
		  broker_flag = true;
		}
	      else if (strcmp (util, UTIL_TYPE_MANAGER) == 0)
		{
		  manager_flag = true;
		}
	      else
		{
		  return ER_GENERIC_ERROR;
		}
	    }
	}
    }

  strcpy (server_list, "service::server");
  if (sysprm_obtain_parameters (server_list, 4096) == NO_ERROR)
    {
      char *save_ptr, *value;
      char *delim = (char *) "\t=\"";
      value = strtok_r (server_list, delim, &save_ptr);
      value = strtok_r (NULL, delim, &save_ptr);
      if (value != NULL)
	{
	  start_server_list = strdup (value);
	}
    }

  us_Property_map[SERVICE_START_SERVER].property_value =
    strdup (server_flag ? PROPERTY_ON : PROPERTY_OFF);
  us_Property_map[SERVICE_START_BROKER].property_value =
    strdup (broker_flag ? PROPERTY_ON : PROPERTY_OFF);
  us_Property_map[SERVICE_START_MANAGER].property_value =
    strdup (manager_flag ? PROPERTY_ON : PROPERTY_OFF);
  us_Property_map[SERVICE_START_REPL_SERVER].property_value =
    strdup (repl_server_flag ? PROPERTY_ON : PROPERTY_OFF);
  us_Property_map[SERVICE_START_REPL_AGENT].property_value =
    strdup (repl_agent_flag ? PROPERTY_ON : PROPERTY_OFF);
  us_Property_map[SERVER_START_LIST].property_value =
    start_server_list ? start_server_list : strdup ("");
  us_Property_map[REPL_SERVER_START_LIST].property_value = strdup ("");
  us_Property_map[REPL_AGENT_START_LIST].property_value = strdup ("");
  return NO_ERROR;
}

/*
 * finalize_properties - free alloced memory by strdup ()
 *
 * return:
 *
 */
static void
finalize_properties (void)
{
  int i;

  for (i = 0; us_Property_map[i].property_index != -1; i++)
    {
      if (us_Property_map[i].property_value != NULL)
	{
	  free ((void *) us_Property_map[i].property_value);
	}
    }
}

/*
 * get_property -
 *
 * return:
 *
 *      property_type(in):
 *
 * NOTE:
 */
static const char *
get_property (int property_type)
{
  return us_Property_map[property_type].property_value;
}

/*
 * is_terminated_process() - test if the process is terminated
 *   return: true if the process is terminated, otherwise false
 *   pid(in): process id
 */
static bool
is_terminated_process (const int pid)
{
#if defined(WINDOWS)
  HANDLE h_process;

  h_process = OpenProcess (PROCESS_QUERY_INFORMATION, FALSE, pid);
  if (h_process == NULL)
    {
      return true;
    }
  else
    {
      CloseHandle (h_process);
      return false;
    }
#else /* WINDOWS */
  if (kill (pid, 0) == -1)
    {
      return true;
    }
  else
    {
      return false;
    }
#endif /* WINDOWS */
}
