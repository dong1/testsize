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
 * db_vdb.c - Stubs for SQL interface functions.
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "db.h"
#include "dbi.h"
#include "db_query.h"
#include "error_manager.h"
#include "chartype.h"
#include "system_parameter.h"
#include "environment_variable.h"
#include "memory_alloc.h"
#include "parser.h"
#include "parser_message.h"
#include "object_domain.h"
#include "schema_manager.h"
#include "view_transform.h"
#include "execute_statement.h"
#include "xasl_generation.h"	/* TODO: remove */
#include "locator_cl.h"
#include "server_interface.h"
#include "query_manager.h"
#include "api_compat.h"
#include "network_interface_cl.h"

#define BUF_SIZE 1024

enum
{
  StatementInitialStage = 0,
  StatementCompiledStage,
  StatementPreparedStage,
  StatementExecutedStage,
};


static int get_dimension_of (PT_NODE ** array);
static DB_SESSION *db_open_local (void);
static DB_SESSION *initialize_session (DB_SESSION * session);
static int db_execute_and_keep_statement_local (DB_SESSION * session,
						int stmt_ndx,
						DB_QUERY_RESULT ** result);
static DB_OBJLIST *db_get_all_chosen_classes (int (*p) (MOBJ o));
static int is_vclass_object (MOBJ class_);
static char *get_reasonable_predicate (DB_ATTRIBUTE * att);
static void update_execution_values (PARSER_CONTEXT * parser, int result,
				     CUBRID_STMT_TYPE statement_type);
static void copy_execution_values (EXECUTION_STATE_VALUES * source,
				   EXECUTION_STATE_VALUES * destination);
static int values_list_to_values_array (DB_SESSION * session,
					PT_NODE * values_list,
					DB_VALUE_ARRAY * values_array);
static int do_process_prepare_statement (DB_SESSION * session,
					 PT_NODE * statement);
static int do_process_prepare_statement_internal (DB_SESSION * session,
						  const char *const name,
						  const char *const
						  statement_literal);
static int do_process_execute_prepare (DB_SESSION * session,
				       PT_NODE * statement,
				       DB_EXECUTED_STATEMENT_TYPE *
				       const statement_type,
				       DB_QUERY_RESULT ** result,
				       const bool recompile_before_execution,
				       const bool fail_on_xasl_error);
static int do_process_deallocate_prepare (DB_SESSION * session,
					  PT_NODE * statement);
static DB_PREPARE_INFO *add_prepared_statement_by_name (DB_SESSION * session,
							const char *name);
static DB_PREPARE_INFO *get_prepared_statement_by_name (DB_SESSION * session,
							const char *name);
static bool delete_prepared_statement_if_exists (DB_SESSION * session,
						 const char *name);
static void delete_all_prepared_statements (DB_SESSION * session);
static bool is_disallowed_as_prepared_statement (PT_NODE_TYPE node_type);

/*
 * get_dimemsion_of() - returns the number of elements of a null-terminated
 *   pointer array
 * returns  : number of elements of array
 * array (in): a null-terminated array of pointers
 */
static int
get_dimension_of (PT_NODE ** array)
{
  int rank = 0;

  if (!array)
    {
      return rank;
    }

  while (*array++)
    {
      rank++;
    }

  return rank;
}

/*
 * db_statement_count() - This function returns the number of statements
 *    in a session.
 * return : number of statements in the session
 * session(in): compiled session
 */
int
db_statement_count (DB_SESSION * session)
{
  int retval;

  if (session == NULL)
    {
      return 0;
    }

  retval = get_dimension_of (session->statements);

  return (retval);
}

/*
 * db_open_local() - Starts a new SQL empty compile session
 * returns : new DB_SESSION
 */
static DB_SESSION *
db_open_local (void)
{
  DB_SESSION *session = NULL;

  session = (DB_SESSION *) malloc (sizeof (DB_SESSION));
  if (session == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (DB_SESSION));
      return NULL;
    }

  session->parser = parser_create_parser ();
  if (session->parser == NULL)
    {
      free_and_init (session);
      return NULL;
    }

  session->stage = NULL;
  session->dimension = 0;
  session->stmt_ndx = 0;
  session->type_list = NULL;
  session->line_offset = 0;
  session->include_oid = DB_NO_OIDS;
  session->statements = NULL;

  session->prepared_statements.name = NULL;
  session->prepared_statements.prepared_session = NULL;
  session->prepared_statements.statement = NULL;
  session->prepared_statements.next = NULL;
  session->is_subsession_for_prepared = false;
  session->executed_statements = NULL;

  return session;
}

/*
 * initialize_session() -
 * returns  : DB_SESSION *, NULL if fails
 * session(in/out):
 */
static DB_SESSION *
initialize_session (DB_SESSION * session)
{
  int i;

  assert (session != NULL && session->statements != NULL);

  session->dimension = get_dimension_of (session->statements);

  session->executed_statements =
    malloc (sizeof (*session->executed_statements) * session->dimension);
  if (session->executed_statements == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (*session->executed_statements) * session->dimension);
      db_close_session (session);
      return NULL;
    }
  else
    {
      for (i = 0; i < session->dimension; ++i)
	{
	  session->executed_statements[i].query_type_list = NULL;
	  session->executed_statements[i].statement_type = -1;
	}
    }

  return session;
}

/*
 * db_open_buffer_local() - Please refer to the db_open_buffer() function
 * returns  : new DB_SESSION
 * buffer(in): contains query text to be compiled
 */
DB_SESSION *
db_open_buffer_local (const char *buffer)
{
  DB_SESSION *session;

  CHECK_1ARG_NULL (buffer);

  session = db_open_local ();

  if (session)
    {
      session->statements = parser_parse_string (session->parser, buffer);
      if (session->statements)
	{
	  return initialize_session (session);
	}
    }

  return session;
}

/*
 * db_open_buffer() - Starts a new SQL compile session on a nul terminated
 *    string
 * return:new DB_SESSION
 * buffer(in) : contains query text to be compiled
 */
DB_SESSION *
db_open_buffer (const char *buffer)
{
  DB_SESSION *session;

  CHECK_1ARG_NULL (buffer);

  session = db_open_buffer_local (buffer);

  return session;
}


/*
 * db_open_file() - Starts a new SQL compile session on a query file
 * returns  : new DB_SESSION
 * file(in): contains query text to be compiled
 */
DB_SESSION *
db_open_file (FILE * file)
{
  DB_SESSION *session;

  session = db_open_local ();

  if (session)
    {
      session->statements = parser_parse_file (session->parser, file);
      if (session->statements)
	{
	  return initialize_session (session);
	}
    }

  return session;
}

/*
 * db_make_session_for_one_statement_execution() -
 * return:
 * file(in) :
 */
DB_SESSION *
db_make_session_for_one_statement_execution (FILE * file)
{
  DB_SESSION *session;

  session = db_open_local ();

  if (session)
    {
      pt_init_one_statement_parser (session->parser, file);
      parse_one_statement (0);
    }

  return session;
}

/*
 * db_parse_one_statement() -
 * return:
 * session(in) :
 */
int
db_parse_one_statement (DB_SESSION * session)
{
  if (session->dimension > 0)
    {
      /* check if this statement is skipped */
      if (session->type_list)
	{
	  db_free_query_format (session->type_list[0]);
	}
      if (session->statements[0])
	{
	  parser_free_tree (session->parser, session->statements[0]);
	  session->statements[0] = NULL;
	}

      session->dimension = 0;
      session->stmt_ndx = 0;

      session->parser->stack_top = 0;
      if (session->stage)
	{
	  session->stage[0] = StatementInitialStage;
	}
    }

  if (parse_one_statement (1) == 0 &&
      session->parser->error_msgs == NULL &&
      session->parser->stack_top > 0 && session->parser->node_stack != NULL)
    {
      session->parser->statements =
	(PT_NODE **) parser_alloc (session->parser, 2 * sizeof (PT_NODE *));
      if (session->parser->statements == NULL)
	{
	  return -1;
	}

      session->parser->statements[0] = session->parser->node_stack[0];
      session->parser->statements[1] = NULL;

      session->statements = session->parser->statements;
      session->dimension = get_dimension_of (session->statements);

      return session->dimension;
    }
  else
    {
      session->parser->statements = NULL;
      return -1;
    }
}

/*
 * db_get_parser_line_col() -
 * return:
 * session(in) :
 * line(out) :
 * col(out) :
 */
int
db_get_parser_line_col (DB_SESSION * session, int *line, int *col)
{
  if (line)
    {
      *line = session->parser->line;
    }
  if (col)
    {
      *col = session->parser->column;
    }
  return 0;
}

/*
 * db_open_file_name() - This functions allocates and initializes a session and
 *    parses the named file. Similar to db_open_file() except that it takes a
 *    name rather than a file handle.
 * return : new session
 * name(in): file name
 */
DB_SESSION *
db_open_file_name (const char *name)
{
  FILE *fp;
  DB_SESSION *session;

  session = db_open_local ();

  if (session)
    {
      fp = fopen (name, "r");
      if (fp != NULL)
	{
	  session->statements = parser_parse_file (session->parser, fp);
	  fclose (fp);
	}
      if (session->statements)
	{
	  return initialize_session (session);
	}
    }

  return session;
}


/*
 * db_compile_statement_local() -
 * return:
 * session(in) :
 */
int
db_compile_statement_local (DB_SESSION * session)
{
  PARSER_CONTEXT *parser;
  int stmt_ndx;
  PT_NODE *statement = NULL;
  PT_NODE *statement_result = NULL;
  DB_QUERY_TYPE *qtype;
  int cmd_type;
  int err;

  DB_VALUE *hv;
  int i;
  SERVER_INFO server_info;
  static long seed = 0;

  /* obvious error checking - invalid parameter */
  if (!session || !session->parser)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return er_errid ();
    }
  /* no statement was given in the session */
  if (session->dimension == 0 || !session->statements)
    {
      /* if the parser already has something wrong - syntax error */
      if (pt_has_error (session->parser))
	{
	  pt_report_to_ersys (session->parser, PT_SYNTAX);
	  return er_errid ();
	}

      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_EMPTY_STATEMENT, 0);
      return er_errid ();
    }
  /* check the end of statements */
  if (session->stmt_ndx == session->dimension)
    {
      /* return 0 if all statement were compiled */
      return 0;
    }

  /* allocate memory for session->type_list and session->stage
     if not allocated */
  if (session->type_list == NULL)
    {
      size_t size = session->dimension * sizeof (DB_QUERY_TYPE *)
	+ session->dimension * sizeof (char);
      void *p = malloc (size);
      if (p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, size);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      (void) memset (p, 0, size);
      session->type_list = (DB_QUERY_TYPE **) p;
      session->stage =
	(char *) p + session->dimension * sizeof (DB_QUERY_TYPE *);
    }

  /*
   * Compilation Stage
   */

  /* the statements in this session have been parsed without error
     and it is time to compile the next statement */
  parser = session->parser;
  stmt_ndx = session->stmt_ndx++;
  statement = session->statements[stmt_ndx];
  statement->use_plan_cache = 0;
  statement->use_query_cache = 0;

  /* check if the statement is already processed */
  if (session->stage[stmt_ndx] >= StatementPreparedStage)
    {
      return stmt_ndx;
    }

  /* forget about any previous parsing errors, if any */
  pt_reset_error (parser);

  /* get type list describing the output columns titles of the given query */
  cmd_type = pt_node_to_cmd_type (statement);
  qtype = NULL;
  if (cmd_type == CUBRID_STMT_SELECT)
    {
      qtype = pt_get_titles (parser, statement);
      /* to prevent a memory leak, register the query type list to session */
      session->type_list[stmt_ndx] = qtype;
    }

  /* prefetch and lock classes to avoid deadlock */
  (void) pt_class_pre_fetch (parser, statement);
  if (pt_has_error (parser))
    {
      pt_report_to_ersys_with_statement (parser, PT_SYNTAX, statement);
      return er_errid ();
    }

  /* get sys_date, sys_time, sys_timestamp, sys_datetime values from the server */
  server_info.info_bits = 0;	/* init */
  if (statement->si_datetime)
    {
      server_info.info_bits |= SI_SYS_DATETIME;
      server_info.value[0] = &parser->sys_datetime;
    }
  if (statement->si_tran_id)
    {
      server_info.info_bits |= SI_LOCAL_TRANSACTION_ID;
      server_info.value[1] = &parser->local_transaction_id;
    }
  /* request to the server */
  if (server_info.info_bits)
    {
      (void) qp_get_server_info (&server_info);
    }

  if (seed == 0)
    {
      srand48 (seed = (long) time (NULL));
    }

  /* do semantic check for the statement */
  statement_result = pt_compile (parser, statement);

  if (!statement_result || pt_has_error (parser))
    {
      pt_report_to_ersys_with_statement (parser, PT_SEMANTIC, statement);
      return er_errid ();
    }
  statement = statement_result;

  /* get type list describing the output columns titles of the given query */
  if (cmd_type == CUBRID_STMT_SELECT)
    {
      /* for a select-type query of the form:
         SELECT * FROM class c
         we store into type_list the nice column headers:
         c.attr1    attr2    c.attr3
         before they get fully resolved by mq_translate(). */
      if (!qtype)
	{
	  qtype = pt_get_titles (parser, statement);
	  /* to prevent a memory leak,
	     register the query type list to session */
	  session->type_list[stmt_ndx] = qtype;
	}
      if (qtype)
	{
	  /* NOTE, this is here on purpose. If something is busting
	     because it tries to continue corresponding this type
	     information and the list file columns after having jacked
	     with the list file, by for example adding a hidden OID
	     column, fix the something else.
	     This needs to give the results as user views the
	     query, ie related to the original text. It may guess
	     wrong about attribute/column updatability.
	     Thats what they asked for. */
	  qtype = pt_fillin_type_size (parser, statement, qtype, DB_NO_OIDS,
				       false);
	}
    }

  /* reset auto parameterized variables */
  for (i = 0, hv = parser->host_variables + parser->host_var_count;
       i < parser->auto_param_count; i++, hv++)
    db_value_clear (hv);
  parser->auto_param_count = 0;
  /* translate views or virtual classes into base classes */
  statement_result = mq_translate (parser, statement);
  if (!statement_result || pt_has_error (parser))
    {
      pt_report_to_ersys_with_statement (parser, PT_SEMANTIC, statement);
      return er_errid ();
    }
  statement = statement_result;

  /* prefetch and lock translated real classes to avoid deadlock */
  (void) pt_class_pre_fetch (parser, statement);
  if (pt_has_error (parser))
    {
      pt_report_to_ersys_with_statement (parser, PT_SYNTAX, statement);
      return er_errid ();
    }

  /* validate include_oid setting in the session */
  if (session->include_oid)
    {
      if (mq_updatable (parser, statement))
	{
	  if (session->include_oid == DB_ROW_OIDS)
	    {
	      (void) pt_add_row_oid (parser, statement);
	    }
	}
      else
	{
	  /* disallow OID column for non-updatable query */
	  session->include_oid = DB_NO_OIDS;
	}
    }

  /* so now, the statement is compiled */
  session->statements[stmt_ndx] = statement;
  session->stage[stmt_ndx] = StatementCompiledStage;


  /*
   * Preparation Stage
   */

  statement->xasl_id = NULL;	/* bullet proofing */

  /* New interface of do_prepare_statement()/do_execute_statment() is used
     only when the XASL cache is enabled. If it is disabled, old interface
     of do_statement() will be used instead. do_statement() makes a XASL
     everytime rather than using XASL cache. Also, it can be executed in
     the server without touching the XASL cache by calling
     query_prepare_and_execute(). */
  if (PRM_XASL_MAX_PLAN_CACHE_ENTRIES > 0 && statement->cannot_prepare == 0)
    {

      /* now, prepare the statement by calling do_prepare_statement() */
      err = do_prepare_statement (parser, statement);
#if 0
      if (err == ER_QPROC_INVALID_XASLNODE)
	{
	  /* There is a kind of problem in the XASL cache.
	     It is possible when the cache entry was deleted by the other.
	     In this case, retry to prepare once more (generate and stored
	     the XASL again). */
	  statement->xasl_id = NULL;
	  er_clear ();
	  /* execute the statement by calling do_statement() */
	  err = do_prepare_statement (parser, statement);
	}
#endif
      if (err < 0)
	{
	  if (pt_has_error (parser))
	    {
	      pt_report_to_ersys_with_statement (parser, PT_SEMANTIC,
						 statement);
	      return er_errid ();
	    }
	  return err;
	}
    }

  /* so now, the statement is prepared */
  session->stage[stmt_ndx] = StatementPreparedStage;

  return stmt_ndx + 1;
}

/*
 * db_compile_statement() - This function compiles the next statement in the
 *    session. The first compilation reports any syntax errors that occurred
 *    in the entire session.
 * return: an integer that is the relative statement ID of the next statement
 *    within the session, with the first statement being statement number 1.
 *    If there are no more statements in the session (end of statements), 0 is
 *    returned. If an error occurs, the return code is negative.
 * session(in) : session handle
 */
int
db_compile_statement (DB_SESSION * session)
{
  int statement_id;

  er_clear ();

  CHECK_CONNECT_MINUSONE ();

  statement_id = db_compile_statement_local (session);

  return statement_id;
}

/*
 * db_set_client_cache_time() -
 * return:
 * session(in) :
 * stmt_ndx(in) :
 * cache_time(in) :
 */
int
db_set_client_cache_time (DB_SESSION * session, int stmt_ndx,
			  CACHE_TIME * cache_time)
{
  PT_NODE *statement;
  int result = NO_ERROR;

  if (!session
      || !session->parser
      || session->dimension == 0
      || !session->statements
      || stmt_ndx < 1
      || stmt_ndx > session->dimension
      || !(statement = session->statements[stmt_ndx - 1]))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      result = er_errid ();
    }
  else
    {
      if (cache_time)
	{
	  statement->cache_time = *cache_time;
	  statement->clt_cache_check = 1;
	}
    }

  return result;
}

/*
 * db_get_jdbccachehint() -
 * return:
 * session(in) :
 * stmt_ndx(in) :
 * life_time(out) :
 */
bool
db_get_jdbccachehint (DB_SESSION * session, int stmt_ndx, int *life_time)
{
  PT_NODE *statement;

  /* obvious error checking - invalid parameter */
  if (!session
      || !session->parser
      || session->dimension == 0
      || !session->statements
      || stmt_ndx < 1
      || stmt_ndx > session->dimension
      || !(statement = session->statements[stmt_ndx - 1]))
    {
      return false;
    }

  if (statement->info.query.q.select.hint & PT_HINT_JDBC_CACHE)
    {
      if (life_time != NULL &&
	  statement->info.query.q.select.jdbc_life_time->info.name.original !=
	  NULL)
	{
	  *life_time =
	    atoi (statement->info.query.q.select.jdbc_life_time->info.name.
		  original);
	}
      return true;
    }

  return false;
}

/*
 * db_get_useplancache() -
 * return:
 * session(in) :
 * stmt_ndx(in) :
 * life_time(out) :
 */
bool
db_get_cacheinfo (DB_SESSION * session, int stmt_ndx,
		  bool * use_plan_cache, bool * use_query_cache)
{
  PT_NODE *statement;

  /* obvious error checking - invalid parameter */
  if (!session
      || !session->parser
      || session->dimension == 0
      || !session->statements
      || stmt_ndx < 1
      || stmt_ndx > session->dimension
      || !(statement = session->statements[stmt_ndx - 1]))
    {
      return false;
    }

  if (use_plan_cache)
    {
      if (statement->use_plan_cache)
	{
	  *use_plan_cache = true;
	}
      else
	{
	  *use_plan_cache = false;
	}
    }

  if (use_query_cache)
    {
      if (statement->use_query_cache)
	{
	  *use_query_cache = true;
	}
      else
	{
	  *use_query_cache = false;
	}
    }

  return true;
}

/*
 * db_get_errors() - This function returns a list of errors that occurred during
 *    compilation. NULL is returned if no errors occurred.
 * returns : compilation error list
 * session(in): session handle
 *
 * note : A call to the db_get_next_error() function can be used to examine
 *    each error. You do not free this list of errors.
 */
DB_SESSION_ERROR *
db_get_errors (DB_SESSION * session)
{
  DB_SESSION_ERROR *result;

  if (!session || !session->parser)
    {
      result = NULL;
    }
  else
    {
      result = pt_get_errors (session->parser);
    }

  return result;
}

/*
 * db_get_next_error() - This function returns the line and column number of
 *    the next error that was passed in the compilation error list.
 * return : next error in compilation error list
 * errors (in) : DB_SESSION_ERROR iterator
 * line(out): source line number of error
 * col(out): source column number of error
 *
 * note : Do not free this list of errors.
 */
DB_SESSION_ERROR *db_get_next_error
  (DB_SESSION_ERROR * errors, int *line, int *col)
{
  DB_SESSION_ERROR *result;
  int stmt_no;
  const char *e_msg = NULL;

  if (!errors)
    {
      return NULL;
    }

  result = pt_get_next_error (errors, &stmt_no, line, col, &e_msg);
  if (e_msg)
    {
      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_ERROR, 1, e_msg);
    }

  return result;
}

/*
 * db_get_warnings: This function returns a list of warnings that occurred
 *    during the compilation. NULL is returned if no warnings are found.
 *    A non-NULL return value indicates that one or more warnings occurred
 *    during compilation.
 * returns: DB_SESSION_WARNING iterator if there were any compilation warnings,
 *          NULL, otherwise.
 * session(in): session handle
 *
 * note : Do not free this list of warnings.
 */
DB_SESSION_WARNING *
db_get_warnings (DB_SESSION * session)
{
  DB_SESSION_WARNING *result;

  if (!session || !session->parser)
    {
      result = NULL;
    }
  else
    {
      result = pt_get_warnings (session->parser);
    }

  return result;
}

/*
 * db_get_next_warning: This function returns the line and column number of the
 *    next warning that was passed in the compilation warning list.
 * returns: DB_SESSION_WARNING iterator if there are more compilation warnings
 *          NULL, otherwise.
 * warnings(in) : DB_SESSION_WARNING iterator
 * line(out): source line number of warning
 * col(out): source column number of warning
 *
 * note : Do not free this list of warnings.
 */
DB_SESSION_WARNING *db_get_next_warning
  (DB_SESSION_WARNING * warnings, int *line, int *col)
{
  DB_SESSION_WARNING *result;
  int stmt_no;
  const char *e_msg = NULL;

  if (!warnings)
    {
      return NULL;
    }

  result = pt_get_next_error (warnings, &stmt_no, line, col, &e_msg);
  if (e_msg)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_PT_ERROR, 1, e_msg);
    }

  return result;
}

/*
 * db_get_line_col_of_1st_error() - get the source line & column of first error
 * returns: 1 if there were any query compilation errors, 0, otherwise.
 * session(in) : contains the SQL query that has just been compiled
 * linecol(out): the source line & column of first error if any
 *
 * note : DO NOT USE THIS FUNCTION.  USE db_get_errors & db_get_next_error
 *	  instead.  This function is provided for the sole purpose of
 *	  facilitating conversion of old code.
 */
int
db_get_line_col_of_1st_error (DB_SESSION * session, DB_QUERY_ERROR * linecol)
{
  if (!session || !session->parser || !pt_has_error (session->parser))
    {
      if (linecol)
	{
	  linecol->err_lineno = linecol->err_posno = 0;
	}
      return 0;
    }
  else
    {
      PT_NODE *errors;
      int stmt_no;
      const char *msg;

      errors = pt_get_errors (session->parser);
      if (linecol)
	pt_get_next_error (errors, &stmt_no, &linecol->err_lineno,
			   &linecol->err_posno, &msg);
      return 1;
    }
}

/*
 * db_number_of_input_markers() -
 * return : number of host variable input markers in statement
 * session(in): compilation session
 * stmt(in): statement number of compiled statement
 */
int
db_number_of_input_markers (DB_SESSION * session, int stmt)
{
  PARSER_CONTEXT *parser;
  PT_NODE *statement;
  int result = 0;

  if (!session
      || !(parser = session->parser)
      || !session->statements
      || stmt < 1
      || stmt > session->dimension
      || !(statement = session->statements[stmt - 1]))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      result = er_errid ();
    }
  else
    {
      result = parser->host_var_count;
    }

  return result;
}

/*
 * db_number_of_output_markers() -
 * return : number of host variable output markers in statement
 * session(in): compilation session
 * stmt(in): statement number of compiled statement
 */
int
db_number_of_output_markers (DB_SESSION * session, int stmt)
{
  PARSER_CONTEXT *parser;
  PT_NODE *statement;
  int result = 0;

  if (!session
      || !(parser = session->parser)
      || !session->statements
      || stmt < 1
      || stmt > session->dimension
      || !(statement = session->statements[stmt - 1]))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      result = er_errid ();
    }
  else
    {
      (void) parser_walk_tree (parser, statement, pt_count_output_markers,
			       &result, NULL, NULL);
    }

  return result;
}

/*
 * db_get_input_markers() -
 * return : host variable input markers list in statement
 * session(in): compilation session
 * stmt(in): statement number of compiled statement
 */
DB_MARKER *
db_get_input_markers (DB_SESSION * session, int stmt)
{
  PARSER_CONTEXT *parser;
  PT_NODE *statement;
  DB_MARKER *result = NULL;
  PT_HOST_VARS *hv;

  if (!session
      || !(parser = session->parser)
      || !session->statements
      || stmt < 1
      || stmt > session->dimension
      || !(statement = session->statements[stmt - 1])
      || pt_has_error (parser))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      result = NULL;
    }
  else
    {
      hv = pt_host_info (parser, statement);
      result = pt_get_input_host_vars (hv);
      pt_free_host_info (hv);
    }

  return result;
}

/*
 * db_get_output_markers() -
 * return : host variable output markers list in statement
 * session(in): compilation session
 * stmt(in): statement number of compiled statement
 */
DB_MARKER *
db_get_output_markers (DB_SESSION * session, int stmt)
{
  PARSER_CONTEXT *parser;
  PT_NODE *statement;
  DB_MARKER *result = NULL;
  PT_HOST_VARS *hv;

  if (!session
      || !(parser = session->parser)
      || !session->statements
      || stmt < 1
      || stmt > session->dimension
      || !(statement = session->statements[stmt - 1])
      || pt_has_error (parser))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      result = NULL;
    }
  else
    {
      hv = pt_host_info (parser, statement);
      result = pt_get_output_host_vars (hv);
      pt_free_host_info (hv);
    }

  return result;
}

/*
 * db_marker_next: This function returns the next marker in the list
 * return : the next host variable (input/output) marker in the list or NULL
 * marker(in): DB_MARKER
 */
DB_MARKER *
db_marker_next (DB_MARKER * marker)
{
  DB_MARKER *result = NULL;

  if (marker)
    {
      result = pt_node_next (marker);
    }

  return result;
}

/*
 * db_marker_index() - This function return the index of an host variable
 *    (input/output) marker
 * return : index of an marker
 * marker(in): DB_MARKER
 */
int
db_marker_index (DB_MARKER * marker)
{
  int result = -1;

  if (marker)
    {
      result = pt_host_var_index (marker);
    }

  return result;
}

/*
 * db_marker_domain() - This function returns the domain of an host variable
 *    (input/output) marker
 * return : domain of marker
 * marker(in): DB_MARKER
 */
DB_DOMAIN *
db_marker_domain (DB_MARKER * marker)
{
  DB_DOMAIN *result = NULL;

  if (marker)
    {
      result = pt_node_to_db_domain (NULL, marker, NULL);
    }
  /* it is safet to call pt_node_to_db_domain() without parser */

  return result;
}

/*
 * db_is_input_marker() - Returns true iff it is the host variable input marker
 * return : boolean
 * marker(in): DB_MARKER
 */
bool
db_is_input_marker (DB_MARKER * marker)
{
  bool result = false;

  if (marker)
    {
      result = pt_is_input_hostvar (marker);
    }

  return result;
}

/*
 * db_is_output_marker() - Returns true iff it is the host variable
 *    output marker
 * return : boolean
 * marker(in): DB_MARKER
 */
bool
db_is_output_marker (DB_MARKER * marker)
{
  bool result = false;

  if (marker)
    {
      result = pt_is_output_hostvar (marker);
    }

  return result;
}


/*
 * db_get_query_type_list() - This function returns a type list that describes
 *    the columns of a SELECT statement. This includes the column title, data
 *    type, and size. The statement ID must have been returned by a previously
 *    successful call to the db_compile_statement() function. The query type
 *    list is freed by using the db_query_format_free() function.
 * return : query type.
 * session(in): session handle
 * stmt(in): statement id
 */
DB_QUERY_TYPE *
db_get_query_type_list (DB_SESSION * session, int stmt_ndx)
{
  PT_NODE *statement;
  DB_QUERY_TYPE *qtype;
  int cmd_type;

  /* obvious error checking - invalid parameter */
  if (!session || !session->parser)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return NULL;
    }
  /* no statement was given in the session */
  if (session->dimension == 0 || !session->statements)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_EMPTY_STATEMENT, 0);
      return NULL;
    }
  /* invalid parameter */
  statement = session->statements[--stmt_ndx];
  if (stmt_ndx < 0 || stmt_ndx >= session->dimension || !statement)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_OBJ_INVALID_ARGUMENTS, 0);
      return NULL;
    }
  /* check if the statement is compiled and prepared */
  if (session->stage[stmt_ndx] < StatementPreparedStage)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return NULL;
    }

  /* make DB_QUERY_TYPE structure to return */

  if (statement != NULL && statement->node_type == PT_EXECUTE_PREPARE)
    {
      /* We lie about the type of an executed prepared statement so that
       * clients are given the type of the statement that is actually executed.
       */
      DB_EXECUTED_STATEMENT_TYPE *statement_type;

      assert (session->executed_statements != NULL);

      statement_type = &session->executed_statements[stmt_ndx];
      if (statement_type->query_type_list == NULL)
	{
	  /* The PT_EXECUTE_PREPARE statement has not yet been executed but we
	     can find the information we need if the corresponding
	     PT_PREPARE_STATEMENT was executed. */
	  DB_PREPARE_INFO *prepare_info;
	  int err = 0;

	  prepare_info =
	    get_prepared_statement_by_name (session,
					    statement->info.prepare.name->
					    info.name.original);
	  if (prepare_info == NULL)
	    {
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_IT_INVALID_SESSION, 0);
	      return NULL;
	    }
	  /* This is very unfortunate but we need to recompile the statement
	   * because the schema might have changed in the meantime.
	   */
	  err = do_process_prepare_statement_internal (session,
						       prepare_info->name,
						       prepare_info->
						       statement);
	  if (err < 0)
	    {
	      return NULL;
	    }
	  prepare_info = get_prepared_statement_by_name (session,
							 statement->info.
							 prepare.name->info.
							 name.original);
	  if (prepare_info == NULL)
	    {
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_IT_INVALID_SESSION, 0);
	      return NULL;
	    }
	  return db_get_query_type_list (prepare_info->prepared_session, 1);
	}
      else
	{
	  /* The PT_EXECUTE_PREPARE statement has already been executed and
	     the information we need has been saved. */
	  return db_cp_query_type (statement_type->query_type_list, true);
	}
    }

  cmd_type = pt_node_to_cmd_type (statement);
  if (cmd_type == CUBRID_STMT_SELECT)
    {
      PT_NODE *select_list = pt_get_select_list (session->parser, statement);
      if (pt_length_of_select_list (select_list, EXCLUDE_HIDDEN_COLUMNS) > 0)
	{
	  /* duplicate one from stored list */
	  qtype = db_cp_query_type (session->type_list[stmt_ndx], true);
	}
      else
	{
	  qtype = NULL;
	}
    }
  else
    {
      /* make new one containing single value */
      qtype = db_alloc_query_format (1);
      if (qtype)
	{
	  switch (cmd_type)
	    {
	    case CUBRID_STMT_CALL:
	      qtype->db_type = pt_node_to_db_type (statement);
	      break;
	    case CUBRID_STMT_INSERT:
	      /* the type of result of INSERT is object */
	      qtype->db_type = DB_TYPE_OBJECT;
	      break;
	    case CUBRID_STMT_GET_ISO_LVL:
	    case CUBRID_STMT_GET_TIMEOUT:
	    case CUBRID_STMT_GET_OPT_LVL:
	    case CUBRID_STMT_GET_TRIGGER:
	      /* the type of result of some command is integer */
	      qtype->db_type = DB_TYPE_INTEGER;
	      break;
	    }
	}
    }

  return qtype;
}

/*
 * db_get_query_type_ptr() - This function returns query_type of query result
 * return : result->query_type
 * result(in): query result
 */
DB_QUERY_TYPE *
db_get_query_type_ptr (DB_QUERY_RESULT * result)
{
  return (result->query_type);
}

/*
 * db_get_start_line() - This function returns source line position of
 *    a query statement
 * return : stmt's source line position
 * session(in): contains the SQL query that has been compiled
 * stmt(in): int returned by a successful compilation
 */
int
db_get_start_line (DB_SESSION * session, int stmt)
{
  int retval;
  PARSER_CONTEXT *parser;
  PT_NODE *statement;

  if (!session
      || !(parser = session->parser)
      || !session->statements
      || stmt < 1
      || stmt > session->dimension
      || !(statement = session->statements[stmt - 1]))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      retval = er_errid ();
    }
  else
    {
      retval = pt_statement_line_number (statement);
    }

  return (retval);
}

/*
 * db_get_statement_type() - This function returns query statement node type
 * return : stmt's node type
 * session(in): contains the SQL query that has been compiled
 * stmt(in): statement id returned by a successful compilation
 */
int
db_get_statement_type (DB_SESSION * session, int stmt)
{
  int retval;
  PARSER_CONTEXT *parser;
  PT_NODE *statement;

  if (!session
      || !(parser = session->parser)
      || !session->statements
      || stmt < 1
      || stmt > session->dimension
      || !(statement = session->statements[stmt - 1]))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      retval = er_errid ();
    }
  else
    {
      if (statement != NULL && statement->node_type == PT_EXECUTE_PREPARE)
	{
	  /* We lie about the type of an executed prepared statement so that
	   * clients are given the type of the statement that is actually
	   * executed
	   */
	  DB_EXECUTED_STATEMENT_TYPE *statement_type;

	  assert (session->executed_statements != NULL);

	  statement_type = &session->executed_statements[stmt - 1];
	  if (statement_type->query_type_list == NULL)
	    {
	      /* The PT_EXECUTE_PREPARE statement has not yet been executed but
	         we can find the information we need if the corresponding
	         PT_PREPARE_STATEMENT was executed. */
	      DB_PREPARE_INFO *const prepare_info =
		get_prepared_statement_by_name (session,
						statement->info.prepare.name->
						info.name.original);
	      if (prepare_info == NULL)
		{
		  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			  ER_IT_INVALID_SESSION, 0);
		  return er_errid ();
		}
	      else
		{
		  /* The PT_EXECUTE_PREPARE statement has already been executed
		     and the information we need has been saved. We do not need
		     to recompile because schema changes cannot affect the
		     statement type. */
		  retval =
		    db_get_statement_type (prepare_info->prepared_session, 1);
		}
	    }
	  else
	    {
	      retval = statement_type->statement_type;
	    }
	}
      else
	{
	  retval = pt_node_to_cmd_type (statement);
	}
    }

  return retval;
}

/*
 * db_include_oid() - This function set the session->parser->oid_included flag
 * return : void
 * session(in): the current session context
 * include_oid(in): non-zero means include oid,
 *	            zero means don't include it.
 */
void
db_include_oid (DB_SESSION * session, int include_oid)
{
  if (!session)
    {
      return;
    }

  session->include_oid = include_oid;
}

/*
 * db_push_values() - This function set session->parser->host_variables
 *   & host_var_count
 * return : void
 * session(in): contains the SQL query that has been compiled
 * count(in): number of elements in in_values table
 * in_values(in): a table of host_variable initialized DB_VALUEs
 */
void
db_push_values (DB_SESSION * session, int count, DB_VALUE * in_values)
{
  PARSER_CONTEXT *parser;

  if (session)
    {
      parser = session->parser;
      if (parser)
	{
	  pt_set_host_variables (parser, count, in_values);
	}
    }
}

/*
 * db_get_hostvars() -
 * return:
 * session(in) :
 */
DB_VALUE *
db_get_hostvars (DB_SESSION * session)
{
  return session->parser->host_variables;
}

/*
 * db_get_lock_classes() -
 * return:
 * session(in) :
 */
char **
db_get_lock_classes (DB_SESSION * session)
{
  if (session == NULL || session->parser == NULL)
    {
      return NULL;
    }

  return (char **) (session->parser->lcks_classes);
}

/*
 * db_set_sync_flag() -
 * return:
 * session(in) :
 * exec_mode(in) :
 */
void
db_set_sync_flag (DB_SESSION * session, QUERY_EXEC_MODE exec_mode)
{
  session->parser->exec_mode = exec_mode;
}

/*
 * db_get_session_mode() -
 * return:
 * session(in) :
 */
int
db_get_session_mode (DB_SESSION * session)
{
  if (!session || !session->parser)
    {
      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION,
	      0);
      return ER_IT_INVALID_SESSION;
    }
  else
    {
      return (session->parser->exec_mode == SYNC_EXEC);
    }
}

/*
 * db_set_session_mode_sync() -
 * return:
 * session(in) :
 */
int
db_set_session_mode_sync (DB_SESSION * session)
{
  if (!session || !session->parser)
    {
      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION,
	      0);
      return ER_IT_INVALID_SESSION;
    }

  db_set_sync_flag (session, SYNC_EXEC);

  return NO_ERROR;
}

/*
 * db_set_session_mode_async() -
 * return:
 * session(in) :
 */
int
db_set_session_mode_async (DB_SESSION * session)
{
  if (!session || !session->parser)
    {
      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION,
	      0);
      return ER_IT_INVALID_SESSION;
    }

  db_set_sync_flag (session, ASYNC_EXEC);

  return NO_ERROR;
}

/*
 * db_execute_and_keep_statement_local() - This function executes the SQL
 *    statement identified by the stmt argument and returns the result.
 *    The statement ID must have already been returned by a successful call
 *    to the db_open_file() function or the db_open_buffer() function that
 *    came from a call to the db_compile_statement()function. The compiled
 *    statement is preserved, and may be executed again within the same
 *    transaction.
 * return : error status, if execution failed
 *          number of affected objects, if a success & stmt is a SELECT,
 *          UPDATE, DELETE, or INSERT
 * session(in) : contains the SQL query that has been compiled
 * stmt(in) : int returned by a successful compilation
 * result(out): query results descriptor
 */
static int
db_execute_and_keep_statement_local (DB_SESSION * session, int stmt_ndx,
				     DB_QUERY_RESULT ** result)
{
  PARSER_CONTEXT *parser;
  PT_NODE *statement;
  DB_QUERY_RESULT *qres;
  DB_VALUE *val;
  int err = NO_ERROR;
  SERVER_INFO server_info;
  SEMANTIC_CHK_INFO sc_info = { NULL, NULL, 0, 0, 0, false, false, false };

  if (result != NULL)
    {
      *result = NULL;
    }

  /* obvious error checking - invalid parameter */
  if (!session || !session->parser)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return er_errid ();
    }
  /* no statement was given in the session */
  if (session->dimension == 0 || !session->statements)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_EMPTY_STATEMENT, 0);
      return er_errid ();
    }
  /* invalid parameter */
  stmt_ndx--;
  if (stmt_ndx < 0 || stmt_ndx >= session->dimension
      || !session->statements[stmt_ndx])
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_OBJ_INVALID_ARGUMENTS, 0);
      return er_errid ();
    }

  /* valid host variable was not set before */
  if (session->parser->host_var_count > 0
      && session->parser->set_host_var == 0)
    {
      if (pt_has_error (session->parser))
	{
	  pt_report_to_ersys (session->parser, PT_SEMANTIC);
	  /* forget about any previous compilation errors, if any */
	  pt_reset_error (session->parser);
	}
      else
	{
	  /* parsed statement has some host variable parameters
	     (input marker '?'), but no host variable (DB_VALUE array) was set
	     by db_push_values() API */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_UCI_TOO_FEW_HOST_VARS,
		  0);
	}
      return er_errid ();
    }

  /* if the parser already has something wrong - semantic error */
  if (session->stage[stmt_ndx] < StatementExecutedStage
      && pt_has_error (session->parser))
    {
      pt_report_to_ersys (session->parser, PT_SEMANTIC);
      return er_errid ();
    }

  /*
   * Execution Stage
   */
  er_clear ();

  /* now, we have a statement to execute */
  parser = session->parser;
  statement = session->statements[stmt_ndx];

  /* if the statement was not compiled and prepared, do it */
  if (session->stage[stmt_ndx] < StatementPreparedStage)
    {
      session->stmt_ndx = stmt_ndx;
      if (db_compile_statement_local (session) < 0)
	{
	  return er_errid ();
	}
    }

  /* forget about any previous compilation errors, if any */
  pt_reset_error (parser);

  /* get sys_date, sys_time, sys_timestamp, sys_datetime values from the server */
  server_info.info_bits = 0;	/* init */
  if (statement->si_datetime && DB_IS_NULL (&parser->sys_datetime))
    {
      /* if it was reset in the previous execution step, fills it now */
      server_info.info_bits |= SI_SYS_DATETIME;
      server_info.value[0] = &parser->sys_datetime;
    }
  if (statement->si_tran_id && DB_IS_NULL (&parser->local_transaction_id))
    {
      /* if it was reset in the previous execution step, fills it now */
      server_info.info_bits |= SI_LOCAL_TRANSACTION_ID;
      server_info.value[1] = &parser->local_transaction_id;
    }
  /* request to the server */
  if (server_info.info_bits)
    {
      (void) qp_get_server_info (&server_info);
    }

  if (statement->node_type == PT_PREPARE_STATEMENT)
    {
      assert (!session->is_subsession_for_prepared);

      err = do_process_prepare_statement (session, statement);
      update_execution_values (parser, -1, CUBRID_MAX_STMT_TYPE);
      assert (result == NULL || *result == NULL);
      return err;
    }
  else if (statement->node_type == PT_EXECUTE_PREPARE)
    {
      /* If the XASL cache is disabled we have no quick way of knowing that the
       * compiled statement has become outdated so we need to compile it every
       * time. If the XASL cache is enabled we recompile the statement each time
       * its XASL has been invalidated.
       */
      bool recompile_before_execution = !(PRM_XASL_MAX_PLAN_CACHE_ENTRIES > 0
					  && statement->cannot_prepare == 0);
      bool fail_on_xasl_error = recompile_before_execution;

      assert (!session->is_subsession_for_prepared);
      assert (session->executed_statements != NULL);

      err = do_process_execute_prepare (session, statement,
					&session->
					executed_statements[stmt_ndx], result,
					recompile_before_execution,
					fail_on_xasl_error);
      return err;
    }
  else if (statement->node_type == PT_DEALLOCATE_PREPARE)
    {
      assert (!session->is_subsession_for_prepared);

      err = do_process_deallocate_prepare (session, statement);
      update_execution_values (parser, -1, CUBRID_MAX_STMT_TYPE);
      assert (result == NULL || *result == NULL);
      return err;
    }

  /* New interface of do_prepare_statement()/do_execute_statment() is used
     only when the XASL cache is enabled. If it is disabled, old interface
     of do_statement() will be used instead. do_statement() makes a XASL
     everytime rather than using XASL cache. Also, it can be executed in
     the server without touching the XASL cache by calling
     query_prepare_and_execute(). */
  do_Trigger_involved = false;
  if (PRM_XASL_MAX_PLAN_CACHE_ENTRIES > 0 && statement->cannot_prepare == 0)
    {
      /* now, execute the statement by calling do_execute_statement() */
      err = do_execute_statement (parser, statement);
      if (err == ER_QPROC_INVALID_XASLNODE &&
	  session->stage[stmt_ndx] == StatementPreparedStage)
	{
	  /* Hmm, there is a kind of problem in the XASL cache.
	     It is possible when the cache entry was deleted before 'execute'
	     and after 'prepare' by the other, e.g. qmgr_drop_all_query_plans().
	     In this case, retry to prepare once more (generate and stored
	     the XASL again). */
	  if (statement->xasl_id)
	    {
	      (void) qmgr_drop_query_plan (NULL, NULL, statement->xasl_id,
					   false);
	      free_and_init (statement->xasl_id);
	    }
	  /* forget all errors */
	  er_clear ();
	  pt_reset_error (parser);

	  /* retry the statement by calling do_prepare/execute_statement() */
	  err = do_prepare_statement (parser, statement);
	  if (!(err < 0))
	    {
	      err = do_execute_statement (parser, statement);
	    }
	}
      else if (err == ER_QPROC_INVALID_XASLNODE
	       && session->is_subsession_for_prepared)
	{
	  return ER_QPROC_INVALID_XASLNODE;
	}
    }
  else
    {
      /* bind and resolve host variables */
      assert (parser->host_var_count >= 0 && parser->auto_param_count >= 0);
      if (parser->host_var_count > 0)
	{
	  assert (parser->set_host_var == 1);
	}
      if (parser->host_var_count > 0 || parser->auto_param_count > 0)
	{
	  /* In this case, pt_bind_values_to_hostvars() will change
	     PT_HOST_VAR node. Must duplicate the statement and execute with
	     the new one and free the copied one before returning */
	  statement = parser_copy_tree_list (parser, statement);

	  sc_info.top_node = statement;
	  sc_info.donot_fold = false;

	  if (!(statement = pt_bind_values_to_hostvars (parser, statement))
	      || !(statement = pt_resolve_names (parser, statement, &sc_info))
	      || !(statement = pt_semantic_type (parser, statement,
						 &sc_info)))
	    {
	      /* something wrong */
	      if (er_errid () == NO_ERROR)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_DO_UNKNOWN_HOSTVAR_TYPE, 0);
		}
	      if (pt_has_error (parser))
		{
		  pt_report_to_ersys_with_statement (parser, PT_SYNTAX,
						     statement);
		}
	      if (statement != session->statements[stmt_ndx])
		{
		  parser_free_tree (parser, statement);
		}
	      return er_errid ();
	    }
	}

      err = do_statement (parser, statement);
    }

  do_Trigger_involved = false;
  if (err < 0)
    {
      /* Do not override original error id with */
      if (er_errid () == NO_ERROR
	  && pt_has_error (parser) && err != ER_QPROC_INVALID_XASLNODE)
	{
	  pt_report_to_ersys_with_statement (parser, PT_EXECUTION, statement);
	  err = er_errid ();
	}
      /* free the allocated list_id area before leaving */
      if (pt_node_to_cmd_type (statement) == CUBRID_STMT_SELECT ||
	  pt_node_to_cmd_type (statement) == CUBRID_STMT_DO)
	{
	  pt_free_query_etc_area (statement);
	}
    }
  /* so now, the statement is executed */
  session->stage[stmt_ndx] = StatementExecutedStage;

  /* execution succeeded, maybe. process result of the query */
  if (result && !(err < 0))
    {
      qres = NULL;

      if (statement->clt_cache_reusable)
	{
	  qres = pt_make_cache_hit_result_descriptor ();
	  if (!qres)
	    {
	      err = er_errid ();
	    }
	}
      else
	{

	  switch (pt_node_to_cmd_type (statement))
	    {
	    case CUBRID_STMT_SELECT:
	      /* Check whether pt_new_query_result_descriptor() fails.
	         Similar tests are required for CUBRID_STMT_INSERT and
	         CUBRID_STMT_CALL cases. */
	      qres = pt_new_query_result_descriptor (parser, statement);
	      if (qres)
		{
		  /* get number of rows as result */
		  err = db_query_tuple_count (qres);
		  qres->query_type =
		    db_cp_query_type (session->type_list[stmt_ndx], false);
		  qres->res.s.stmt_id = stmt_ndx;
		}
	      else
		{
		  err = er_errid ();
		}
	      break;

	    case CUBRID_STMT_DO:
	      pt_free_query_etc_area (statement);
	      break;

	    case CUBRID_STMT_GET_ISO_LVL:
	    case CUBRID_STMT_GET_TIMEOUT:
	    case CUBRID_STMT_GET_OPT_LVL:
	    case CUBRID_STMT_GET_TRIGGER:
	    case CUBRID_STMT_EVALUATE:
	    case CUBRID_STMT_CALL:
	    case CUBRID_STMT_INSERT:
	    case CUBRID_STMT_GET_STATS:
	      /* csql (in csql.c) may throw away any non-null *result,
	         but we create a DB_QUERY_RESULT structure anyway for other
	         callers of db_execute that use the *result like esql_cli.c  */
	      val = (DB_VALUE *) pt_node_etc (statement);
	      if (val)
		{
		  /* got a result, so use it */
		  qres = db_get_db_value_query_result (val);
		  if (qres)
		    {
		      /* get number of rows as result */
		      err = db_query_tuple_count (qres);
		    }
		  else
		    {
		      err = er_errid ();
		    }

		  /* db_get_db_value_query_result copied val, so free val */
		  db_value_free (val);
		  pt_null_etc (statement);
		}
	      else
		{
		  /* avoid changing err. it should have been
		     meaningfully set. if err = 0, uci_static will set
		     SQLCA to SQL_NOTFOUND! */
		}
	      break;

	    default:
	      break;
	    }			/* switch (pt_node_to_cmd_type()) */

	}			/* else */

      *result = qres;
    }				/* if (result) */

  /* Do not override original error id with  */
  /* last error checking */
  if (er_errid () == NO_ERROR
      && pt_has_error (parser) && err != ER_QPROC_INVALID_XASLNODE)
    {
      pt_report_to_ersys_with_statement (parser, PT_EXECUTION, statement);
      err = er_errid ();
    }

  /* reset the parser values */
  if (statement->si_datetime)
    {
      db_make_null (&parser->sys_datetime);
    }
  if (statement->si_tran_id)
    {
      db_make_null (&parser->local_transaction_id);
    }

  update_execution_values (parser, err, pt_node_to_cmd_type (statement));

  /* free if the statement was duplicated for host variable binding */
  if (statement != session->statements[stmt_ndx])
    {
      parser_free_tree (parser, statement);
    }

  return err;
}

static void
update_execution_values (PARSER_CONTEXT * parser, int result,
			 CUBRID_STMT_TYPE statement_type)
{
  if (result < 0)
    {
      parser->execution_values.row_count = -1;
    }
  else if (statement_type == CUBRID_STMT_UPDATE
	   || statement_type == CUBRID_STMT_INSERT
	   || statement_type == CUBRID_STMT_DELETE)
    {
      parser->execution_values.row_count = result;
    }
  else
    {
      parser->execution_values.row_count = -1;
    }
}

static void
copy_execution_values (EXECUTION_STATE_VALUES * source,
		       EXECUTION_STATE_VALUES * destination)
{
  assert (destination != NULL && source != NULL);
  destination->row_count = source->row_count;
}

static int
values_list_to_values_array (DB_SESSION * session, PT_NODE * values_list,
			     DB_VALUE_ARRAY * values_array)
{
  DB_VALUE_ARRAY values;
  PT_NODE *current_value = values_list;
  int i = 0;
  int err = NO_ERROR;

  values.size = 0;
  values.vals = NULL;

  if (session == NULL || session->parser == NULL || values_list == NULL
      || values_array == NULL || values_array->size != 0
      || values_array->vals != NULL)
    {
      assert (false);
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      err = er_errid ();
      goto error_exit;
    }

  while (current_value != NULL)
    {
      values.size++;
      current_value = current_value->next;
    }

  values.vals = malloc (values.size * sizeof (DB_VALUE));
  if (values.vals == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      values.size * sizeof (DB_VALUE));
      err = er_errid ();
      goto error_exit;
    }

  for (i = 0; i < values.size; ++i)
    {
      db_make_null (&values.vals[i]);
    }
  for (current_value = values_list, i = 0;
       current_value != NULL; current_value = current_value->next, ++i)
    {
      int more_type_info_needed = 0;
      DB_VALUE *db_val = pt_value_to_db (session->parser, current_value);
      if (db_val == NULL)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  err = er_errid ();
	  goto error_exit;
	}
      db_value_clone (db_val, &values.vals[i]);
    }

  values_array->size = values.size;
  values_array->vals = values.vals;
  values.size = 0;
  values.vals = NULL;

  return err;

error_exit:

  if (values.vals != NULL)
    {
      db_value_clear_array (&values);
      free_and_init (values.vals);
      values.size = 0;
    }
  return err;
}

static int
do_process_prepare_statement (DB_SESSION * session, PT_NODE * statement)
{
  const char *const name = statement->info.prepare.name->info.name.original;
  const char *const statement_literal =
    (char *) statement->info.prepare.statement->info.value.data_value.str->
    bytes;

  assert (statement->node_type == PT_PREPARE_STATEMENT);

  return do_process_prepare_statement_internal (session, name,
						statement_literal);
}

static int
do_process_prepare_statement_internal (DB_SESSION * session,
				       const char *const name,
				       const char *const statement_literal)
{
  DB_PREPARE_INFO *prepare_info = NULL;
  DB_SESSION *prepared_session = NULL;
  int prepared_statement_ndx = 0;

  /* existing statements with the same name are lost; MySQL does it this way */
  delete_prepared_statement_if_exists (session, name);
  assert (get_prepared_statement_by_name (session, name) == NULL);

  prepare_info = add_prepared_statement_by_name (session, name);
  if (prepare_info == NULL)
    {
      return er_errid ();
    }
  prepare_info->statement = statement_literal;

  prepared_session = db_open_buffer_local (prepare_info->statement);
  if (prepared_session == NULL)
    {
      delete_prepared_statement_if_exists (session, name);
      return er_errid ();
    }
  prepared_session->is_subsession_for_prepared = true;

  /* we need to copy all the relevant settings */
  prepared_session->include_oid = session->include_oid;

  prepare_info->prepared_session = prepared_session;

  if ((prepared_statement_ndx =
       db_compile_statement_local (prepare_info->prepared_session)) < 0)
    {
      delete_prepared_statement_if_exists (session, name);
      return er_errid ();
    }
  if (NO_ERROR != db_check_single_query (prepare_info->prepared_session,
					 prepared_statement_ndx))
    {
      delete_prepared_statement_if_exists (session, name);
      return er_errid ();
    }
  assert (prepared_statement_ndx == 1);
  assert (prepared_session->dimension == 1);
  assert (prepared_session->statements[0] != NULL);
  if (is_disallowed_as_prepared_statement
      (prepared_session->statements[0]->node_type))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_IT_IS_DISALLOWED_AS_PREPARED, 0);
      delete_prepared_statement_if_exists (session, name);
      return er_errid ();
    }
  return NO_ERROR;
}

static int
do_process_execute_prepare (DB_SESSION * session, PT_NODE * statement,
			    DB_EXECUTED_STATEMENT_TYPE * const statement_type,
			    DB_QUERY_RESULT ** result,
			    const bool recompile_before_execution,
			    const bool fail_on_xasl_error)
{
  const char *const name = statement->info.prepare.name->info.name.original;
  PT_NODE *const using_list = statement->info.prepare.using_list;
  DB_PREPARE_INFO *const prepare_info =
    get_prepared_statement_by_name (session, name);
  int err = 0;

  assert (statement->node_type == PT_EXECUTE_PREPARE);

  if (prepare_info == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_PREPARED_NAME_NOT_FOUND,
	      1, name);
      return er_errid ();
    }
  if (recompile_before_execution)
    {
      err =
	do_process_prepare_statement_internal (session, prepare_info->name,
					       prepare_info->statement);
      if (err < 0)
	{
	  return err;
	}
      return do_process_execute_prepare (session, statement, statement_type,
					 result, false, fail_on_xasl_error);
    }
  if (using_list == NULL
      && prepare_info->prepared_session->parser->host_var_count != 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_INCORRECT_HOSTVAR_COUNT,
	      2, 0, prepare_info->prepared_session->parser->host_var_count);
      return er_errid ();
    }
  if (using_list != NULL)
    {
      DB_VALUE_ARRAY values_array;
      int i = 0;

      values_array.size = 0;
      values_array.vals = NULL;

      if (NO_ERROR !=
	  values_list_to_values_array (prepare_info->prepared_session,
				       using_list, &values_array))
	{
	  return er_errid ();
	}
      assert (values_array.size != 0 && values_array.vals != NULL);

      if (prepare_info->prepared_session->parser->host_var_count !=
	  values_array.size)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_IT_INCORRECT_HOSTVAR_COUNT, 2, values_array.size,
		  prepare_info->prepared_session->parser->host_var_count);
	  db_value_clear_array (&values_array);
	  free_and_init (values_array.vals);
	  values_array.size = 0;
	  return er_errid ();
	}

      db_push_values (prepare_info->prepared_session, values_array.size,
		      values_array.vals);

      db_value_clear_array (&values_array);
      free_and_init (values_array.vals);
      values_array.size = 0;
    }

  err = db_execute_and_keep_statement_local (prepare_info->prepared_session,
					     1, result);
  copy_execution_values (&prepare_info->prepared_session->parser->
			 execution_values,
			 &session->parser->execution_values);
  if (err >= 0)
    {
      /* Successful execution; we need to save the query type in order to answer
       * db_get_query_type_list and db_get_statement_type after the prepared
       * statement has been deleted */
      if (statement_type->query_type_list != NULL)
	{
	  /* The PT_EXECUTE_PREPARE statement has been executed before (this
	   * execution scenario is pretty weird and might lead to unexpected
	   * results). We clear the old type of the query as the new type might
	   * be different (for example due to schema changes).
	   */
	  db_free_query_format (statement_type->query_type_list);
	  statement_type->query_type_list = NULL;
	  statement_type->statement_type = -1;
	}
      else
	{
	  assert (statement_type->statement_type < 0);
	}
      statement_type->statement_type =
	db_get_statement_type (prepare_info->prepared_session, 1);
      if (statement_type->statement_type < 0)
	{
	  return er_errid ();
	}
      statement_type->query_type_list =
	db_get_query_type_list (prepare_info->prepared_session, 1);
      if (statement_type->query_type_list == NULL)
	{
	  return er_errid ();
	}
    }
  if (err == ER_QPROC_INVALID_XASLNODE)
    {
      if (fail_on_xasl_error)
	{
	  return err;
	}
      else
	{
	  /* We need to recompile as the missing XASL might indicate a schema
	     change. */
	  return do_process_execute_prepare (session, statement,
					     statement_type, result, true,
					     true);
	}
    }
  else
    {
      return err;
    }
}

static int
do_process_deallocate_prepare (DB_SESSION * session, PT_NODE * statement)
{
  const char *const name = statement->info.prepare.name->info.name.original;
  const bool was_deleted =
    delete_prepared_statement_if_exists (session, name);

  assert (statement->node_type == PT_DEALLOCATE_PREPARE);
  assert (get_prepared_statement_by_name (session, name) == NULL);
  if (!was_deleted)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_PREPARED_NAME_NOT_FOUND,
	      1, name);
      return er_errid ();
    }
  return NO_ERROR;
}

static DB_PREPARE_INFO *
add_prepared_statement_by_name (DB_SESSION * session, const char *name)
{
  DB_PREPARE_INFO *prepare_info = malloc (sizeof (*prepare_info));
  if (prepare_info == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (*prepare_info));
      return NULL;
    }
  prepare_info->next = session->prepared_statements.next;
  session->prepared_statements.next = prepare_info;
  prepare_info->name = name;
  prepare_info->statement = NULL;
  prepare_info->prepared_session = NULL;
  return prepare_info;
}

static DB_PREPARE_INFO *
get_prepared_statement_by_name (DB_SESSION * session, const char *name)
{
  DB_PREPARE_INFO *current = session->prepared_statements.next;
  while (current != NULL)
    {
      if (strcasecmp (current->name, name) == 0)
	{
	  return current;
	}
      current = current->next;
    }
  return NULL;
}

static bool
delete_prepared_statement_if_exists (DB_SESSION * session, const char *name)
{
  DB_PREPARE_INFO *current = &session->prepared_statements;
  while (current->next != NULL)
    {
      if (strcasecmp (current->next->name, name) == 0)
	{
	  DB_PREPARE_INFO *to_delete = current->next;
	  current->next = current->next->next;
	  db_close_session_local (to_delete->prepared_session);
	  free_and_init (to_delete);
	  return true;
	}
      current = current->next;
    }
  return false;
}

static void
delete_all_prepared_statements (DB_SESSION * session)
{
  DB_PREPARE_INFO *current = session->prepared_statements.next;
  session->prepared_statements.next = NULL;
  while (current != NULL)
    {
      DB_PREPARE_INFO *to_delete = current;
      current = current->next;
      db_close_session_local (to_delete->prepared_session);
      free_and_init (to_delete);
    }
}

static bool
is_disallowed_as_prepared_statement (PT_NODE_TYPE node_type)
{
  if (node_type == PT_PREPARE_STATEMENT
      || node_type == PT_EXECUTE_PREPARE
      || node_type == PT_DEALLOCATE_PREPARE)
    {
      return true;
    }
  return false;
}


/*
 * db_execute_and_keep_statement() - Please refer to the
 *         db_execute_and_keep_statement_local() function
 * return : error status, if execution failed
 *          number of affected objects, if a success & stmt is a SELECT,
 *          UPDATE, DELETE, or INSERT
 * session(in) : contains the SQL query that has been compiled
 * stmt(in) : int returned by a successful compilation
 * result(out): query results descriptor
 */
int
db_execute_and_keep_statement (DB_SESSION * session, int stmt_ndx,
			       DB_QUERY_RESULT ** result)
{
  int err;

  CHECK_CONNECT_MINUSONE ();

  err = db_execute_and_keep_statement_local (session, stmt_ndx, result);

  return err;
}

/*
 * db_execute_statement_local() - This function executes the SQL statement
 *    identified by the stmt argument and returns the result. The
 *    statement ID must have already been returned by a previously successful
 *    call to the db_compile_statement() function.
 * returns  : error status, if execution failed
 *            number of affected objects, if a success & stmt is a
 *            SELECT, UPDATE, DELETE, or INSERT
 * session(in) : contains the SQL query that has been compiled
 * stmt(in) : int returned by a successful compilation
 * result(out): query results descriptor
 *
 * note : You must free the results of calling this function by using the
 *    db_query_end() function. The resources for the identified compiled
 *    statement (not its result) are freed. Consequently, the statement may
 *    not be executed again.
 */
int
db_execute_statement_local (DB_SESSION * session, int stmt_ndx,
			    DB_QUERY_RESULT ** result)
{
  int err;
  PT_NODE *statement;

  if (session == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  if (session->statements && stmt_ndx > 0 && stmt_ndx <= session->dimension
      && session->statements[stmt_ndx - 1])
    {
      session->statements[stmt_ndx - 1]->do_not_keep = 1;
    }

  err = db_execute_and_keep_statement_local (session, stmt_ndx, result);

  statement = session->statements[stmt_ndx - 1];
  if (statement != NULL)
    {
      /* free XASL_ID allocated by query_prepare()
         before freeing the statement */
      if (statement->xasl_id)
	{
	  free_and_init (statement->xasl_id);
	}
      parser_free_tree (session->parser, statement);
      session->statements[stmt_ndx - 1] = NULL;
    }

  return err;
}

/*
 * db_execute_statement() - Please refer to the
 *    db_execute_statement_local() function
 * returns  : error status, if execution failed
 *            number of affected objects, if a success & stmt is a
 *            SELECT, UPDATE, DELETE, or INSERT
 * session(in) : contains the SQL query that has been compiled
 * stmt(in) : int returned by a successful compilation
 * result(out): query results descriptor
 */
int
db_execute_statement (DB_SESSION * session, int stmt_ndx,
		      DB_QUERY_RESULT ** result)
{
  int err;

  CHECK_CONNECT_MINUSONE ();

  err = db_execute_statement_local (session, stmt_ndx, result);
  return err;
}

/*
 * db_drop_statement() - This function frees the resources allocated to a
 *    compiled statement
 * return : void
 * session(in) : session handle
 * stmt(in) : statement id returned by a successful compilation
 */
void
db_drop_statement (DB_SESSION * session, int stmt)
{
  PT_NODE *statement;

  statement = session->statements[stmt - 1];
  if (statement != NULL)
    {
      /* free XASL_ID allocated by query_prepare()
         before freeing the statement */
      if (statement->xasl_id)
	{
	  if (statement->do_not_keep == 0)
	    {
	      (void) qmgr_drop_query_plan (NULL, NULL, statement->xasl_id,
					   false);
	    }
	  free_and_init (statement->xasl_id);
	}
      parser_free_tree (session->parser, statement);
      session->statements[stmt - 1] = NULL;
      session->stage[stmt - 1] = StatementInitialStage;
    }
}

/*
 * db_drop_all_statements() - This function frees the resources allocated
 *    to a session's compiled statements
 * rerutn : void
 * session(in) : session handle contains the SQL queries that have been
 *   compiled
 */
void
db_drop_all_statements (DB_SESSION * session)
{
  PT_NODE *statement;
  int stmt;

  for (stmt = 0; stmt < session->dimension; stmt++)
    {
      statement = session->statements[stmt];
      if (statement != NULL)
	{
	  /* free XASL_ID allocated by query_prepare()
	     before freeing the statement */
	  if (statement->xasl_id)
	    {
	      if (statement->do_not_keep == 0)
		{
		  (void) qmgr_drop_query_plan (NULL, NULL, statement->xasl_id,
					       false);
		}
	      free_and_init (statement->xasl_id);
	    }
	  parser_free_tree (session->parser, statement);
	  session->statements[stmt] = NULL;
	  session->stage[stmt] = StatementInitialStage;
	}
    }
  session->dimension = session->stmt_ndx = 0;
}

/*
 * db_close_session_local() - This function frees all resources of this session
 *    except query results
 * return : void
 * session(in) : session handle
 */
void
db_close_session_local (DB_SESSION * session)
{
  PARSER_CONTEXT *parser;
  int i;

  if (!session)
    {
      return;
    }

  delete_all_prepared_statements (session);

  parser = session->parser;
  for (i = 0; i < session->dimension; i++)
    {
      PT_NODE *statement;
      DB_EXECUTED_STATEMENT_TYPE *statement_type;

      if (session->type_list && session->type_list[i])
	{
	  db_free_query_format (session->type_list[i]);
	}
      if (session->statements)
	{
	  statement = session->statements[i];
	  if (statement != NULL)
	    {
	      /* free XASL_ID allocated by query_prepare()
	         before freeing the statement */
	      if (statement->xasl_id)
		{
		  if (statement->do_not_keep == 0)
		    {
		      (void) qmgr_drop_query_plan (NULL, NULL,
						   statement->xasl_id, false);
		    }
		  free_and_init (statement->xasl_id);
		}
	      parser_free_tree (parser, statement);
	      session->statements[i] = NULL;
	    }
	}
      if (session->executed_statements)
	{
	  statement_type = &session->executed_statements[i];
	  if (statement_type->query_type_list != NULL)
	    {
	      db_free_query_format (statement_type->query_type_list);
	      statement_type->query_type_list = NULL;
	    }
	}
    }
  session->dimension = session->stmt_ndx = 0;
  if (session->type_list)
    {
      free_and_init (session->type_list);	/* see db_compile_statement_local() */
    }
  if (session->executed_statements)
    {
      free_and_init (session->executed_statements);
    }

  if (parser->host_variables)
    {
      DB_VALUE *hv;

      for (i = 0, hv = parser->host_variables;
	   i < parser->host_var_count + parser->auto_param_count; i++, hv++)
	{
	  db_value_clear (hv);
	}
    }
  parser->host_var_count = parser->auto_param_count = 0;

  pt_free_orphans (session->parser);
  parser_free_parser (session->parser);

  free_and_init (session);
}

/*
 * db_close_session() - Please refer to the db_close_session_local() function
 * return: void
 * session(in) : session handle
 */
void
db_close_session (DB_SESSION * session)
{
  db_close_session_local (session);
}


/*
 * db_get_all_chosen_classes() - This function returns list of all classes
 *    that pass a predicate
 * return : list of class objects that pass a given predicate, if all OK,
 *          NULL otherwise.
 * p(in) : a predicate function
 *
 * note    : the caller is responsible for freeing the list with a call to
 *	       db_objlist_free.
 *
 */
static DB_OBJLIST *
db_get_all_chosen_classes (int (*p) (MOBJ o))
{
  LIST_MOPS *lmops;
  DB_OBJLIST *objects, *last, *new_;
  int i;

  objects = NULL;
  lmops = NULL;
  if (au_check_user () == NO_ERROR)
    {
      /* make sure we have a user */
      last = NULL;
      lmops = locator_get_all_class_mops (DB_FETCH_CLREAD_INSTREAD, p);
      /* probably should make sure
       * we push here because the list could be long */
      if (lmops != NULL)
	{
	  for (i = 0; i < lmops->num; i++)
	    {
	      /* is it necessary to have this check ? */
	      if (!WS_MARKED_DELETED (lmops->mops[i]) &&
		  lmops->mops[i] != sm_Root_class_mop)
		{
		  /* should have a ext_ append function */
		  new_ = ml_ext_alloc_link ();
		  if (new_ == NULL)
		    {
		      goto memory_error;
		    }
		  new_->op = lmops->mops[i];
		  new_->next = NULL;
		  if (last != NULL)
		    {
		      last->next = new_;
		    }
		  else
		    {
		      objects = new_;
		    }
		  last = new_;
		}
	    }
	  locator_free_list_mops (lmops);
	}
    }
  return (objects);

memory_error:
  if (lmops != NULL)
    {
      locator_free_list_mops (lmops);
    }
  if (objects)
    {
      ml_ext_free (objects);
    }
  return NULL;
}

/*
 * is_vclass_object() -
 * return:
 * class(in) :
 */
static int
is_vclass_object (MOBJ class_)
{
  return sm_get_class_type ((SM_CLASS *) class_) == SM_VCLASS_CT;
}

/*
 * db_get_all_vclasses_on_ldb() - This function returns list of all ldb
 *    virtual classes
 * return : list of all ldb virtual class objects if all OK,
 *	    NULL otherwise.
 */
DB_OBJLIST *
db_get_all_vclasses_on_ldb (void)
{
  return NULL;
}

/*
 * db_get_all_vclasses() - This function returns list of all virtual classes
 * returns  : list of all virtual class objects if all OK,
 *	      NULL otherwise.
 */
DB_OBJLIST *
db_get_all_vclasses (void)
{
  DB_OBJLIST *retval;

  retval = db_get_all_chosen_classes (is_vclass_object);

  return (retval);
}


/*
 * db_validate_query_spec() - This function checks that a query_spec is
 *    compatible with a given {vclass} object
 * return  : an ER status code if an error was found, NO_ERROR otherwise.
 * vclass(in) : an {vclass} object
 * query_spec(in) : a query specification string
 */
int
db_validate_query_spec (DB_OBJECT * vclass, const char *query_spec)
{
  PARSER_CONTEXT *parser = NULL;
  PT_NODE **spec = NULL;
  int rc = NO_ERROR;
  const char *const vclass_name = db_get_class_name (vclass);

  if (vclass_name == NULL)
    {
      rc = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, rc, 0);
      return rc;
    }

  if (!db_is_vclass (vclass))
    {
      rc = ER_SM_NOT_A_VIRTUAL_CLASS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, rc, 1, vclass_name);
      return rc;
    }

  parser = parser_create_parser ();
  if (parser == NULL)
    {
      rc = ER_GENERIC_ERROR;
      return rc;
    }

  spec = parser_parse_string (parser, query_spec);
  if (spec != NULL && !pt_has_error (parser))
    {
      rc = pt_validate_query_spec (parser, *spec, vclass);
    }
  else
    {
      pt_report_to_ersys (parser, PT_SYNTAX);
      rc = er_errid ();
    }

  parser_free_parser (parser);

  return rc;
}

/*
 * get_reasonable_predicate() - This function determines if we can compose
 *   any reasonable predicate against this attribute and return that predicate
 * returns: a reasonable predicate against att if one exists, NULL otherwise
 * att(in) : an instance attribute
 */
static char *
get_reasonable_predicate (DB_ATTRIBUTE * att)
{
  static char predicate[300];
  const char *att_name, *cond;

  if (!att
      || db_attribute_is_shared (att)
      || !(att_name = db_attribute_name (att)))
    {
      return NULL;
    }

  switch (db_attribute_type (att))
    {
    case DB_TYPE_INTEGER:
    case DB_TYPE_FLOAT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_SHORT:
    case DB_TYPE_BIGINT:
    case DB_TYPE_MONETARY:
      cond = " = 1 ";
      break;

    case DB_TYPE_STRING:
      cond = " = 'x' ";
      break;

    case DB_TYPE_OBJECT:
      cond = " is null ";
      break;

    case DB_TYPE_SET:
    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
      cond = " = {} ";
      break;

    case DB_TYPE_TIME:
      cond = " = '09:30' ";
      break;

    case DB_TYPE_TIMESTAMP:
      cond = " = '10/15/1986 5:45 am' ";
      break;

    case DB_TYPE_DATETIME:
      cond = " = '10/15/1986 5:45:15.135 am' ";
      break;

    case DB_TYPE_DATE:
      cond = " = '10/15/1986' ";
      break;

    default:
      return NULL;
    }

  snprintf (predicate, sizeof (predicate) - 1, "%s%s", att_name, cond);
  return predicate;
}

/*
 * db_validate() - This function checks if a {class|vclass} definition
 *    is reasonable
 * returns  : an ER status code if an error was found, NO_ERROR otherwise.
 * vc(in) : a {class|vclass} object
 */
int
db_validate (DB_OBJECT * vc)
{
  int retval = NO_ERROR;
  DB_QUERY_SPEC *specs;
  const char *s, *separator = " where ";
  char buffer[BUF_SIZE], *pred, *bufp, *newbuf;
  DB_QUERY_RESULT *result = NULL;
  DB_ATTRIBUTE *attributes;
  int len, limit = BUF_SIZE;

  CHECK_CONNECT_ERROR ();

  if (!vc)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      retval = er_errid ();
    }
  else
    {
      if (!db_is_any_class (vc))
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_NOT_A_CLASS, 0);
	  retval = er_errid ();
	}
      else
	{

	  for (specs = db_get_query_specs (vc);
	       specs; specs = db_query_spec_next (specs))
	    {
	      s = db_query_spec_string (specs);
	      if (s)
		{
		  retval = db_validate_query_spec (vc, s);
		  if (retval < 0)
		    {
		      break;
		    }
		}
	    }
	}
    }

  if (retval >= 0)
    {
      strcpy (buffer, "select count(*) from ");
      strcat (buffer, db_get_class_name (vc));
      attributes = db_get_attributes (vc);
      len = strlen (buffer);
      bufp = buffer;

      while (attributes)
	{
	  pred = get_reasonable_predicate (attributes);
	  if (pred)
	    {
	      /* make sure we have enough room in the buffer */
	      len += (strlen (separator) + strlen (pred));
	      if (len >= limit)
		{
		  /* increase buffer by BUF_SIZE */
		  limit += BUF_SIZE;
		  newbuf = (char *) malloc (limit * sizeof (char));
		  if (newbuf == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1,
			      limit * sizeof (char));
		      break;	/* ran out of memory */
		    }

		  /* copy old buffer into new buffer and switch */
		  strcpy (newbuf, bufp);
		  if (bufp != buffer)
		    {
		      free_and_init (bufp);
		    }
		  bufp = newbuf;
		}
	      /* append another predicate */
	      strcat (bufp, separator);
	      strcat (bufp, pred);
	      separator = " and ";
	    }
	  attributes = db_attribute_next (attributes);
	}

      retval = db_query_execute (bufp, &result, NULL);
      if (result)
	{
	  db_query_end (result);
	}
      if (bufp != buffer)
	{
	  free_and_init (bufp);
	}
    }

  return retval;
}

/*
 * db_free_query() - If an implicit query was executed, free the query on the
 *   server.
 * returns  : void
 * session(in) : session handle
 */
void
db_free_query (DB_SESSION * session)
{
  pt_end_query (session->parser);

}

/*
 * db_check_single_query() - This function checks to see if there is only
 *    one statement given, and that it is a valid query statement.
 * return : error code
 * session(in) : session handle
 * stmt_no(in) : statement number
 */
int
db_check_single_query (DB_SESSION * session, int stmt_no)
{
  if (session->dimension > 1)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_MULTIPLE_STATEMENT, 0);
      return er_errid ();
    }
  return NO_ERROR;
}


/*
 * db_get_parser() - This function returns session's parser
 * returns: session->parser
 * session (in): session handle
 *
 * note : This is a debugging function.
 */
PARSER_CONTEXT *
db_get_parser (DB_SESSION * session)
{
  return session->parser;
}

/*
 * db_get_statement() - This function returns session's statement for id
 * arguments: session (IN): compilation session
 * returns: PT_NODE
 *
 * note : This is a debugging function only.
 *
 */
DB_NODE *
db_get_statement (DB_SESSION * session, int id)
{
  return session->statements[id];
}

/*
 * db_get_parameters() - This function returns a list of the parameters in the
 *    specified statement. There is no implied ordering in the returned list
 *    Parameter names appear once in the returned list, even if they appear
 *    more than once in the statement.
 * return: DB_PARAMETER iterator if there were any parameters in the statement
 * session(in): session handle
 * statement(in): statement number
 */
DB_PARAMETER *
db_get_parameters (DB_SESSION * session, int statement_id)
{
  DB_PARAMETER *result = NULL;
  DB_NODE *statement;

  if (!session
      || !session->parser
      || !session->statements
      || statement_id < 1
      || statement_id > session->dimension
      || !(statement = session->statements[statement_id - 1])
      || pt_has_error (session->parser))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      result = NULL;
    }
  else
    {
      result = pt_get_parameters (session->parser, statement);
    }

  return result;
}

/*
 * db_parameter_next() - This function returns the next parameter in a
 *    parameter list or NULL if at the end of the parameter list. The
 *    value given for param must not be NULL Returns the next parameter
 *    in a parameter list or NULL if at the end of the list.
 * return : next parameter in a parameter list
 * param(in) : a parameter
 */
DB_PARAMETER *
db_parameter_next (DB_PARAMETER * param)
{
  DB_PARAMETER *result = NULL;

  if (param)
    {
      result = pt_node_next (param);
    }

  return result;
}

/*
 * db_parameter_name() - This function returns the name for the given
 *    parameter. param must not be a NULL value.
 * return : parameter name
 * param(in) : a parameter
 */
const char *
db_parameter_name (DB_PARAMETER * param)
{
  const char *result = NULL;

  if (param)
    {
      result = pt_string_part (param);
    }

  return result;
}

/*
 * db_bind_parameter_name() -
 * return: error code
 * name(in) : parameter name
 * value(in) : value to be associated
 *
 * note : This function is analogous to other database vendors' use of the
 *        term bind in that it is an association with a variable location.
 */
int
db_bind_parameter_name (const char *name, DB_VALUE * value)
{
  return pt_associate_label_with_value_check_reference (name, value);
}

/*
 * db_query_produce_updatable_result() -
 * return:
 * session(in) :
 * stmt_ndx(in) :
 */
int
db_query_produce_updatable_result (DB_SESSION * session, int stmt_ndx)
{
  PT_NODE *statement;

  /* obvious error checking - invalid parameter */
  if (!session || !session->parser)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return er_errid ();
    }
  /* no statement was given in the session */
  if (session->dimension == 0 || !session->statements)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_EMPTY_STATEMENT, 0);
      return er_errid ();
    }
  /* invalid parameter */
  statement = session->statements[--stmt_ndx];
  if (stmt_ndx < 0 || stmt_ndx >= session->dimension || !statement)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_OBJ_INVALID_ARGUMENTS, 0);
      return er_errid ();
    }
  /* check if the statement is compiled and prepared */
  if (session->stage[stmt_ndx] < StatementPreparedStage)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return er_errid ();
    }

  if (statement->node_type == PT_SELECT || statement->node_type == PT_UNION)
    {
      return statement->info.query.oids_included;
    }
  else
    {
      return false;
    }
}

/*
 * db_is_query_async_executable() -
 * return:
 * session(in) :
 * stmt_ndx(in) :
 */
bool
db_is_query_async_executable (DB_SESSION * session, int stmt_ndx)
{
  PT_NODE *statement;

  /* obvious error checking - invalid parameter */
  if (!session || !session->parser)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return false;
    }
  /* no statement was given in the session */
  if (session->dimension == 0 || !session->statements)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_EMPTY_STATEMENT, 0);
      return false;
    }
  /* invalid parameter */
  statement = session->statements[--stmt_ndx];
  if (stmt_ndx < 0 || stmt_ndx >= session->dimension || !statement)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_OBJ_INVALID_ARGUMENTS, 0);
      return false;
    }
  /* check if the statement is compiled and prepared */
  if (session->stage[stmt_ndx] < StatementPreparedStage)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return false;
    }

  if (pt_node_to_cmd_type (statement) != CUBRID_STMT_SELECT)
    {
      return false;
    }

  return !pt_statement_have_methods (session->parser, statement);
}
