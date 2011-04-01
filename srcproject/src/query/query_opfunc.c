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
 * query_opfunc.c - The manipulation of data stored in the XASL nodes
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <float.h>
#include <math.h>

#include "system_parameter.h"

#include "error_manager.h"
#include "memory_alloc.h"
#include "object_representation.h"
#include "external_sort.h"
#include "extendible_hash.h"

#include "fetch.h"
#include "list_file.h"
#include "xasl_support.h"
#include "object_primitive.h"
#include "object_domain.h"
#include "set_object.h"
#include "page_buffer.h"

#include "query_executor.h"
#include "databases_file.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define NOT_NULL_VALUE(a, b)	((a) ? (a) : (b))
#define INITIAL_OID_STACK_SIZE  1

#define	SYS_CONNECT_BY_PATH_MEM_STEP	256

static int qdata_dummy (THREAD_ENTRY * thread_p, DB_VALUE * result_p,
			int num_args, DB_VALUE ** args);

static int qdata_add_short (short s, DB_VALUE * dbval_p, DB_VALUE * result_p);
static int qdata_add_int (int i1, int i2, DB_VALUE * result_p);
static int qdata_add_bigint (DB_BIGINT i1, DB_BIGINT i2, DB_VALUE * result_p);
static int qdata_add_float (float f1, float f2, DB_VALUE * result_p);
static int qdata_add_double (double d1, double d2, DB_VALUE * result_p);
static double qdata_coerce_numeric_to_double (DB_VALUE * numeric_val_p);
static void qdata_coerce_dbval_to_numeric (DB_VALUE * dbval_p,
					   DB_VALUE * result_p);
static int qdata_add_numeric (DB_VALUE * numeric_val_p, DB_VALUE * dbval_p,
			      DB_VALUE * result_p);
static int qdata_add_numeric_to_monetary (DB_VALUE * numeric_val_p,
					  DB_VALUE * monetary_val_p,
					  DB_VALUE * result_p);
static int qdata_add_monetary (double d1, double d2, DB_CURRENCY type,
			       DB_VALUE * result_p);
static int qdata_add_bigint_to_time (DB_VALUE * time_val_p,
				     DB_BIGINT add_time, DB_VALUE * result_p);
static int qdata_add_short_to_utime_asymmetry (DB_VALUE * utime_val_p,
					       short s, unsigned int *utime,
					       DB_VALUE * result_p,
					       TP_DOMAIN * domain_p);
static int qdata_add_int_to_utime_asymmetry (DB_VALUE * utime_val_p, int i,
					     unsigned int *utime,
					     DB_VALUE * result_p,
					     TP_DOMAIN * domain_p);
static int qdata_add_short_to_utime (DB_VALUE * utime_val_p, short s,
				     DB_VALUE * result_p,
				     TP_DOMAIN * domain_p);
static int qdata_add_int_to_utime (DB_VALUE * utime_val_p, int i,
				   DB_VALUE * result_p, TP_DOMAIN * domain_p);
static int qdata_add_bigint_to_utime (DB_VALUE * utime_val_p, DB_BIGINT bi,
				      DB_VALUE * result_p,
				      TP_DOMAIN * domain_p);
static int qdata_add_int_to_datetime_asymmetry (DB_VALUE * datetime_val_p,
						int i, DB_DATETIME * datetime,
						DB_VALUE * result_p,
						TP_DOMAIN * domain_p);
static int qdata_add_short_to_datetime (DB_VALUE * datetime_val_p, short s,
					DB_VALUE * result_p,
					TP_DOMAIN * domain_p);
static int qdata_add_int_to_datetime (DB_VALUE * datetime_val_p, int i,
				      DB_VALUE * result_p,
				      TP_DOMAIN * domain_p);
static int qdata_add_bigint_to_datetime (DB_VALUE * datetime_val_p,
					 DB_BIGINT bi, DB_VALUE * result_p,
					 TP_DOMAIN * domain_p);
static int qdata_add_short_to_date (DB_VALUE * date_val_p, short s,
				    DB_VALUE * result_p,
				    TP_DOMAIN * domain_p);
static int qdata_add_int_to_date (DB_VALUE * date_val_p, int i,
				  DB_VALUE * result_p, TP_DOMAIN * domain_p);
static int qdata_add_bigint_to_date (DB_VALUE * date_val_p, DB_BIGINT i,
				     DB_VALUE * result_p,
				     TP_DOMAIN * domain_p);

static int qdata_add_short_to_dbval (DB_VALUE * short_val_p,
				     DB_VALUE * dbval_p,
				     DB_VALUE * result_p,
				     TP_DOMAIN * domain_p);
static int qdata_add_int_to_dbval (DB_VALUE * int_val_p,
				   DB_VALUE * dbval_p,
				   DB_VALUE * result_p, TP_DOMAIN * domain_p);
static int qdata_add_bigint_to_dbval (DB_VALUE * bigint_val_p,
				      DB_VALUE * dbval_p, DB_VALUE * result_p,
				      TP_DOMAIN * domain_p);
static int qdata_add_float_to_dbval (DB_VALUE * float_val_p,
				     DB_VALUE * dbval_p, DB_VALUE * result_p);
static int qdata_add_double_to_dbval (DB_VALUE * double_val_p,
				      DB_VALUE * dbval_p,
				      DB_VALUE * result_p);
static int qdata_add_numeric_to_dbval (DB_VALUE * numeric_val_p,
				       DB_VALUE * dbval_p,
				       DB_VALUE * result_p);
static int qdata_add_monetary_to_dbval (DB_VALUE * monetary_val_p,
					DB_VALUE * dbval_p,
					DB_VALUE * result_p);
static int qdata_add_chars_to_dbval (DB_VALUE * dbval1_p,
				     DB_VALUE * dbval2_p,
				     DB_VALUE * result_p);
static int qdata_add_sequence_to_dbval (DB_VALUE * seq_val_p,
					DB_VALUE * dbval_p,
					DB_VALUE * result_p,
					TP_DOMAIN * domain_p);
static int qdata_add_time_to_dbval (DB_VALUE * time_val_p,
				    DB_VALUE * dbval_p, DB_VALUE * result_p);
static int qdata_add_utime_to_dbval (DB_VALUE * utime_val_p,
				     DB_VALUE * dbval_p,
				     DB_VALUE * result_p,
				     TP_DOMAIN * domain_p);
static int qdata_add_datetime_to_dbval (DB_VALUE * datetime_val_p,
					DB_VALUE * dbval_p,
					DB_VALUE * result_p,
					TP_DOMAIN * domain_p);
static int qdata_add_date_to_dbval (DB_VALUE * date_val_p, DB_VALUE * dbval_p,
				    DB_VALUE * result_p,
				    TP_DOMAIN * domain_p);
static int qdata_coerce_result_to_domain (DB_VALUE * result_p,
					  TP_DOMAIN * domain_p);
static int qdata_cast_to_domain (DB_VALUE * dbval_p, DB_VALUE * result_p,
				 TP_DOMAIN * domain_p);

static int qdata_subtract_short (short s1, short s2, DB_VALUE * result_p);
static int qdata_subtract_int (int i1, int i2, DB_VALUE * result_p);
static int qdata_subtract_bigint (DB_BIGINT i1, DB_BIGINT i2,
				  DB_VALUE * result_p);
static int qdata_subtract_float (float f1, float f2, DB_VALUE * result_p);
static int qdata_subtract_double (double d1, double d2, DB_VALUE * result_p);
static int qdata_subtract_monetary (double d1, double d2,
				    DB_CURRENCY currency,
				    DB_VALUE * result_p);
static int qdata_subtract_time (DB_TIME u1, DB_TIME u2, DB_VALUE * result_p);
static int qdata_subtract_utime (DB_UTIME u1, DB_UTIME u2,
				 DB_VALUE * result_p);
static int qdata_subtract_utime_to_short_asymmetry (DB_VALUE *
						    utime_val_p, short s,
						    unsigned int *utime,
						    DB_VALUE * result_p,
						    TP_DOMAIN * domain_p);
static int qdata_subtract_utime_to_int_asymmetry (DB_VALUE * utime_val_p,
						  int i,
						  unsigned int *utime,
						  DB_VALUE * result_p,
						  TP_DOMAIN * domain_p);
static int qdata_subtract_datetime_to_int (DB_DATETIME * dt1, DB_BIGINT i2,
					   DB_VALUE * result_p);
static int qdata_subtract_datetime (DB_DATETIME * dt1, DB_DATETIME * dt2,
				    DB_VALUE * result_p);
static int qdata_subtract_datetime_to_int_asymmetry (DB_VALUE *
						     datetime_val_p,
						     DB_BIGINT i,
						     DB_DATETIME * datetime,
						     DB_VALUE * result_p,
						     TP_DOMAIN * domain_p);
static int qdata_subtract_short_to_dbval (DB_VALUE * short_val_p,
					  DB_VALUE * dbval_p,
					  DB_VALUE * result_p);
static int qdata_subtract_int_to_dbval (DB_VALUE * int_val_p,
					DB_VALUE * dbval_p,
					DB_VALUE * result_p);
static int qdata_subtract_bigint_to_dbval (DB_VALUE * bigint_val_p,
					   DB_VALUE * dbval_p,
					   DB_VALUE * result_p);
static int qdata_subtract_float_to_dbval (DB_VALUE * float_val_p,
					  DB_VALUE * dbval_p,
					  DB_VALUE * result_p);
static int qdata_subtract_double_to_dbval (DB_VALUE * double_val_p,
					   DB_VALUE * dbval_p,
					   DB_VALUE * result_p);
static int qdata_subtract_numeric_to_dbval (DB_VALUE * numeric_val_p,
					    DB_VALUE * dbval_p,
					    DB_VALUE * result_p);
static int qdata_subtract_monetary_to_dbval (DB_VALUE * monetary_val_p,
					     DB_VALUE * dbval_p,
					     DB_VALUE * result_p);
static int qdata_subtract_sequence_to_dbval (DB_VALUE * seq_val_p,
					     DB_VALUE * dbval_p,
					     DB_VALUE * result_p,
					     TP_DOMAIN * domain_p);
static int qdata_subtract_time_to_dbval (DB_VALUE * time_val_p,
					 DB_VALUE * dbval_p,
					 DB_VALUE * result_p);
static int qdata_subtract_utime_to_dbval (DB_VALUE * utime_val_p,
					  DB_VALUE * dbval_p,
					  DB_VALUE * result_p,
					  TP_DOMAIN * domain_p);
static int qdata_subtract_datetime_to_dbval (DB_VALUE * datetime_val_p,
					     DB_VALUE * dbval_p,
					     DB_VALUE * result_p,
					     TP_DOMAIN * domain_p);
static int qdata_subtract_date_to_dbval (DB_VALUE * date_val_p,
					 DB_VALUE * dbval_p,
					 DB_VALUE * result_p,
					 TP_DOMAIN * domain_p);

static int qdata_multiply_short (DB_VALUE * short_val_p, short s2,
				 DB_VALUE * result_p);
static int qdata_multiply_int (DB_VALUE * int_val_p, int i2,
			       DB_VALUE * result_p);
static int qdata_multiply_bigint (DB_VALUE * bigint_val_p, DB_BIGINT bi2,
				  DB_VALUE * result_p);
static int qdata_multiply_float (DB_VALUE * float_val_p, float f2,
				 DB_VALUE * result_p);
static int qdata_multiply_double (double d1, double d2, DB_VALUE * result_p);
static int qdata_multiply_numeric (DB_VALUE * numeric_val_p,
				   DB_VALUE * dbval, DB_VALUE * result_p);
static int qdata_multiply_monetary (DB_VALUE * monetary_val_p, double d,
				    DB_VALUE * result_p);

static int qdata_multiply_short_to_dbval (DB_VALUE * short_val_p,
					  DB_VALUE * dbval_p,
					  DB_VALUE * result_p);
static int qdata_multiply_int_to_dbval (DB_VALUE * int_val_p,
					DB_VALUE * dbval_p,
					DB_VALUE * result_p);
static int qdata_multiply_bigint_to_dbval (DB_VALUE * bigint_val_p,
					   DB_VALUE * dbval_p,
					   DB_VALUE * result_p);
static int qdata_multiply_float_to_dbval (DB_VALUE * float_val_p,
					  DB_VALUE * dbval_p,
					  DB_VALUE * result_p);
static int qdata_multiply_double_to_dbval (DB_VALUE * double_val_p,
					   DB_VALUE * dbval_p,
					   DB_VALUE * result_p);
static int qdata_multiply_numeric_to_dbval (DB_VALUE * numeric_val_p,
					    DB_VALUE * dbval_p,
					    DB_VALUE * result_p);
static int qdata_multiply_monetary_to_dbval (DB_VALUE * monetary_val_p,
					     DB_VALUE * dbval_p,
					     DB_VALUE * result_p);
static int qdata_multiply_sequence_to_dbval (DB_VALUE * seq_val_p,
					     DB_VALUE * dbval_p,
					     DB_VALUE * result_p,
					     TP_DOMAIN * domain_p);

static bool qdata_is_divided_zero (DB_VALUE * dbval_p);
static int qdata_divide_short (short s1, short s2, DB_VALUE * result_p);
static int qdata_divide_int (int i1, int i2, DB_VALUE * result_p);
static int qdata_divide_bigint (DB_BIGINT bi1, DB_BIGINT bi2,
				DB_VALUE * result_p);
static int qdata_divide_float (float f1, float f2, DB_VALUE * result_p);
static int qdata_divide_double (double d1, double d2,
				DB_VALUE * result_p, bool is_check_overflow);
static int qdata_divide_monetary (double d1, double d2,
				  DB_CURRENCY currency,
				  DB_VALUE * result_p,
				  bool is_check_overflow);

static int qdata_divide_short_to_dbval (DB_VALUE * short_val_p,
					DB_VALUE * dbval_p,
					DB_VALUE * result_p);
static int qdata_divide_int_to_dbval (DB_VALUE * int_val_p,
				      DB_VALUE * dbval_p,
				      DB_VALUE * result_p);
static int qdata_divide_bigint_to_dbval (DB_VALUE * bigint_val_p,
					 DB_VALUE * dbval_p,
					 DB_VALUE * result_p);
static int qdata_divide_float_to_dbval (DB_VALUE * float_val_p,
					DB_VALUE * dbval_p,
					DB_VALUE * result_p);
static int qdata_divide_double_to_dbval (DB_VALUE * double_val_p,
					 DB_VALUE * dbval_p,
					 DB_VALUE * result_p);
static int qdata_divide_numeric_to_dbval (DB_VALUE * numeric_val_p,
					  DB_VALUE * dbval_p,
					  DB_VALUE * result_p);
static int qdata_divide_monetary_to_dbval (DB_VALUE * monetary_val_p,
					   DB_VALUE * dbval_p,
					   DB_VALUE * result_p);

static int qdata_process_distinct (THREAD_ENTRY * thread_p,
				   AGGREGATE_TYPE * agg_p, QUERY_ID query_id);

static int qdata_get_tuple_value_size_from_dbval (DB_VALUE * i2);
static DB_VALUE
  * qdata_get_dbval_from_constant_regu_variable (THREAD_ENTRY * thread_p,
						 REGU_VARIABLE * regu_var,
						 VAL_DESCR * val_desc_p);
static int qdata_convert_dbvals_to_set (THREAD_ENTRY * thread_p,
					DB_TYPE stype,
					REGU_VARIABLE * func,
					VAL_DESCR * val_desc_p,
					OID * obj_oid_p, QFILE_TUPLE tuple);
static int qdata_evaluate_generic_function (THREAD_ENTRY * thread_p,
					    FUNCTION_TYPE * function_p,
					    VAL_DESCR * val_desc_p,
					    OID * obj_oid_p,
					    QFILE_TUPLE tuple);
static int qdata_get_class_of_function (THREAD_ENTRY * thread_p,
					FUNCTION_TYPE * function_p,
					VAL_DESCR * val_desc_p,
					OID * obj_oid_p, QFILE_TUPLE tuple);

static int qdata_convert_table_to_set (THREAD_ENTRY * thread_p,
				       DB_TYPE stype,
				       REGU_VARIABLE * func,
				       VAL_DESCR * val_desc_p);

static int (*generic_func_ptrs[]) (THREAD_ENTRY * thread_p, DB_VALUE *,
				   int, DB_VALUE **) =
{
qdata_dummy};

/*
 * qdata_dummy () -
 *   return:
 *   res(in)    :
 *   num_args(in)       :
 *   args(in)   :
 *
 * Note: dummy generic function.
 */
static int
qdata_dummy (THREAD_ENTRY * thread_p, DB_VALUE * result_p, int num_args,
	     DB_VALUE ** args)
{
  DB_MAKE_NULL (result_p);
  return ER_FAILED;
}

/*
 * qdata_set_value_list_to_null () -
 *   return:
 *   val_list(in)       : Value List
 *
 * Note: Set all db_values on the value list to null.
 */
void
qdata_set_value_list_to_null (VAL_LIST * val_list_p)
{
  QPROC_DB_VALUE_LIST db_val_list;

  if (val_list_p == NULL)
    {
      return;
    }

  db_val_list = val_list_p->valp;
  while (db_val_list)
    {
      pr_clear_value (db_val_list->val);
      db_val_list = db_val_list->next;
    }
}

/*
 * COPY ROUTINES
 */

/*
 * qdata_copy_db_value () -
 *   return: int (true on success, false on failure)
 *   dbval1(in) : Destination db_value node
 *   dbval2(in) : Source db_value node
 *
 * Note: Copy source value to destination value.
 */
int
qdata_copy_db_value (DB_VALUE * dest_p, DB_VALUE * src_p)
{
  PR_TYPE *pr_type_p;
  DB_TYPE src_type;

  /* check if there is nothing to do, so we don't clobber
   * a db_value if we happen to try to copy it to itself
   */
  if (dest_p == src_p)
    {
      return true;
    }

  /* clear any value from a previous iteration */
  pr_clear_value (dest_p);

  src_type = DB_VALUE_DOMAIN_TYPE (src_p);
  pr_type_p = PR_TYPE_FROM_ID (src_type);
  if (pr_type_p == NULL)
    {
      return false;
    }

  (*(pr_type_p->setval)) (dest_p, src_p, true);

  return true;
}

/*
 * qdata_copy_db_value_to_tuple_value () -
 *   return: int (true on success, false on failure)
 *   dbval(in)  : Source dbval node
 *   tvalp(in)  :  Tuple value
 *   tval_size(out)      : Set to the tuple value size
 *
 * Note: Copy an db_value to an tuple value.
 * THIS ROUTINE ASSUMES THAT THE VALUE WILL FIT IN THE TPL!!!!
 */
int
qdata_copy_db_value_to_tuple_value (DB_VALUE * dbval_p, char *tuple_val_p,
				    int *tuple_val_size)
{
  char *val_p;
  int val_size, align;
  OR_BUF buf;
  PR_TYPE *pr_type;
  DB_TYPE dbval_type;

  if (DB_IS_NULL (dbval_p))
    {
      QFILE_PUT_TUPLE_VALUE_FLAG (tuple_val_p, V_UNBOUND);
      QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_val_p, 0);
      *tuple_val_size = QFILE_TUPLE_VALUE_HEADER_SIZE;
    }
  else
    {
      QFILE_PUT_TUPLE_VALUE_FLAG (tuple_val_p, V_BOUND);
      val_p = (char *) tuple_val_p + QFILE_TUPLE_VALUE_HEADER_SIZE;

      dbval_type = DB_VALUE_DOMAIN_TYPE (dbval_p);
      pr_type = PR_TYPE_FROM_ID (dbval_type);

      val_size = pr_writeval_disk_size (dbval_p);

      OR_BUF_INIT (buf, val_p, val_size);

      if (pr_type == NULL
	  || (*(pr_type->writeval)) (&buf, dbval_p) != NO_ERROR)
	{
	  return ER_FAILED;
	}

      /* I don't know if the following is still true. */
      /* since each tuple data value field is already aligned with
       * MAX_ALIGNMENT, val_size by itself can be used to find the maximum
       * alignment for the following field which is next val_header
       */

      align = DB_ALIGN (val_size, MAX_ALIGNMENT);	/* to align for the next field */
      *tuple_val_size = QFILE_TUPLE_VALUE_HEADER_SIZE + align;
      QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_val_p, align);

#if defined(CUBRID_DEBUG)
      /*
       * If there's any gap at the end of the record, fill it with zeroes.
       * This will keep purify from getting confused when we start sending
       * tuples via writev.
       */
      memset (tuple_val_p + QFILE_TUPLE_VALUE_HEADER_SIZE + val_size, 0,
	      align - val_size);
#endif
    }

  return NO_ERROR;
}

/*
 * qdata_copy_valptr_list_to_tuple () -
 *   return: NO_ERROR, or ER_code
 *   valptr_list(in)    : Value pointer list
 *   vd(in)     : Value descriptor
 *   tplrec(in) : Tuple descriptor
 *
 * Note: Copy valptr_list values to tuple descriptor.  Regu variables
 * that are hidden columns are not copied to the list file tuple
 */
int
qdata_copy_valptr_list_to_tuple (THREAD_ENTRY * thread_p,
				 VALPTR_LIST * valptr_list_p,
				 VAL_DESCR * val_desc_p,
				 QFILE_TUPLE_RECORD * tuple_record_p)
{
  REGU_VARIABLE_LIST reg_var_p;
  DB_VALUE *dbval_p;
  char *tuple_p;
  int k, tval_size, tlen, tpl_size;
  int n_size, toffset;

  tpl_size = 0;
  tlen = QFILE_TUPLE_LENGTH_SIZE;
  toffset = 0;			/* tuple offset position */

  /* skip the length of the tuple, we'll fill it in after we know what it is */
  tuple_p = (char *) (tuple_record_p->tpl) + tlen;
  toffset += tlen;

  /* copy each value into the tuple */
  reg_var_p = valptr_list_p->valptrp;
  for (k = 0; k < valptr_list_p->valptr_cnt; k++, reg_var_p = reg_var_p->next)
    {
      if (!reg_var_p->value.hidden_column)
	{
	  dbval_p =
	    qdata_get_dbval_from_constant_regu_variable (thread_p,
							 &reg_var_p->value,
							 val_desc_p);
	  if (dbval_p == NULL)
	    {
	      return ER_FAILED;
	    }

	  n_size = qdata_get_tuple_value_size_from_dbval (dbval_p);
	  if ((tuple_record_p->size - toffset) < n_size)
	    {
	      /* no space left in tuple to put next item, increase the tuple size
	       * by the max of n_size and DB_PAGE_SIZE since we can't compute the
	       * actual tuple size without re-evaluating the expressions.  This
	       * guarantees that we can at least get the next value into the tuple.
	       */
	      tpl_size = MAX (tuple_record_p->size, QFILE_TUPLE_LENGTH_SIZE);
	      tpl_size += MAX (n_size, DB_PAGESIZE);
	      if (tuple_record_p->size == 0)
		{
		  tuple_record_p->tpl =
		    (char *) db_private_alloc (thread_p, tpl_size);
		  if (tuple_record_p->tpl == NULL)
		    {
		      return ER_FAILED;
		    }
		}
	      else
		{
		  tuple_record_p->tpl =
		    (char *) db_private_realloc (thread_p,
						 tuple_record_p->tpl,
						 tpl_size);
		  if (tuple_record_p->tpl == NULL)
		    {
		      return ER_FAILED;
		    }
		}

	      tuple_record_p->size = tpl_size;
	      tuple_p = (char *) (tuple_record_p->tpl) + toffset;
	    }

	  if (qdata_copy_db_value_to_tuple_value (dbval_p, tuple_p,
						  &tval_size) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }

	  tlen += tval_size;
	  tuple_p += tval_size;
	  toffset += tval_size;
	}
    }

  /* now that we know the tuple size, set it. */
  QFILE_PUT_TUPLE_LENGTH (tuple_record_p->tpl, tlen);

  return NO_ERROR;
}

/*
 * qdata_generate_tuple_desc_for_valptr_list () -
 *   return: QPROC_TPLDESCR_SUCCESS on success or
 *           QP_TPLDESCR_RETRY_xxx,
 *           QPROC_TPLDESCR_FAILURE
 *   valptr_list(in)    : Value pointer list
 *   vd(in)     : Value descriptor
 *   tdp(in)    : Tuple descriptor
 *
 * Note: Generate tuple descriptor for given valptr_list values.
 * Regu variables that are hidden columns are not copied
 * to the list file tuple
 */
QPROC_TPLDESCR_STATUS
qdata_generate_tuple_desc_for_valptr_list (THREAD_ENTRY * thread_p,
					   VALPTR_LIST * valptr_list_p,
					   VAL_DESCR * val_desc_p,
					   QFILE_TUPLE_DESCRIPTOR *
					   tuple_desc_p)
{
  REGU_VARIABLE_LIST reg_var_p;
  int i;
  QPROC_TPLDESCR_STATUS status = QPROC_TPLDESCR_SUCCESS;

  tuple_desc_p->tpl_size = QFILE_TUPLE_LENGTH_SIZE;	/* set tuple size as header size */
  tuple_desc_p->f_cnt = 0;

  /* copy each value pointer into the each tdp field */
  reg_var_p = valptr_list_p->valptrp;
  for (i = 0; i < valptr_list_p->valptr_cnt; i++)
    {
      if (!reg_var_p->value.hidden_column)
	{
	  tuple_desc_p->f_valp[tuple_desc_p->f_cnt] =
	    qdata_get_dbval_from_constant_regu_variable (thread_p,
							 &reg_var_p->value,
							 val_desc_p);

	  if (tuple_desc_p->f_valp[tuple_desc_p->f_cnt] == NULL)
	    {
	      status = QPROC_TPLDESCR_FAILURE;
	      goto exit_with_status;
	    }

	  /* SET data-type cannot use tuple descriptor */
	  if (pr_is_set_type
	      (DB_VALUE_DOMAIN_TYPE
	       (tuple_desc_p->f_valp[tuple_desc_p->f_cnt])))
	    {
	      status = QPROC_TPLDESCR_RETRY_SET_TYPE;
	      goto exit_with_status;
	    }

	  /* add aligned field size to tuple size */
	  tuple_desc_p->tpl_size +=
	    qdata_get_tuple_value_size_from_dbval (tuple_desc_p->
						   f_valp[tuple_desc_p->
							  f_cnt]);
	  tuple_desc_p->f_cnt += 1;	/* increase field number */
	}

      reg_var_p = reg_var_p->next;
    }

  /* BIG RECORD cannot use tuple descriptor */
  if (tuple_desc_p->tpl_size >= QFILE_MAX_TUPLE_SIZE_IN_PAGE)
    {
      status = QPROC_TPLDESCR_RETRY_BIG_REC;
    }

exit_with_status:

  return status;
}

/*
 * qdata_set_valptr_list_unbound () -
 *   return: NO_ERROR, or ER_code
 *   valptr_list(in)    : Value pointer list
 *   vd(in)     : Value descriptor
 *
 * Note: Set valptr_list values UNBOUND.
 */
int
qdata_set_valptr_list_unbound (THREAD_ENTRY * thread_p,
			       VALPTR_LIST * valptr_list_p,
			       VAL_DESCR * val_desc_p)
{
  REGU_VARIABLE_LIST reg_var_p;
  DB_VALUE *dbval_p;
  int i;

  reg_var_p = valptr_list_p->valptrp;
  for (i = 0; i < valptr_list_p->valptr_cnt; i++)
    {
      dbval_p =
	qdata_get_dbval_from_constant_regu_variable (thread_p,
						     &reg_var_p->value,
						     val_desc_p);

      if (dbval_p != NULL)
	{
	  if (db_value_domain_init (dbval_p, DB_VALUE_DOMAIN_TYPE (dbval_p),
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE)
	      != NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	}

      reg_var_p = reg_var_p->next;
    }

  return NO_ERROR;
}

/*
 * ARITHMETIC EXPRESSION EVALUATION ROUTINES
 */

static int
qdata_add_short (short s, DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  short result, tmp;

  tmp = DB_GET_SHORT (dbval_p);
  result = s + tmp;

  if (OR_CHECK_ADD_OVERFLOW (s, tmp, result))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  DB_MAKE_SHORT (result_p, result);
  return NO_ERROR;
}

static int
qdata_add_int (int i1, int i2, DB_VALUE * result_p)
{
  int result;

  result = i1 + i2;

  if (OR_CHECK_ADD_OVERFLOW (i1, i2, result))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  DB_MAKE_INT (result_p, result);
  return NO_ERROR;
}

static int
qdata_add_bigint (DB_BIGINT bi1, DB_BIGINT bi2, DB_VALUE * result_p)
{
  DB_BIGINT result;

  result = bi1 + bi2;

  if (OR_CHECK_ADD_OVERFLOW (bi1, bi2, result))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  DB_MAKE_BIGINT (result_p, result);
  return NO_ERROR;
}

static int
qdata_add_float (float f1, float f2, DB_VALUE * result_p)
{
  float result;

  result = f1 + f2;

  if (OR_CHECK_FLOAT_OVERFLOW (result))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  DB_MAKE_FLOAT (result_p, result);
  return NO_ERROR;
}

static int
qdata_add_double (double d1, double d2, DB_VALUE * result_p)
{
  double result;

  result = d1 + d2;

  if (OR_CHECK_DOUBLE_OVERFLOW (result))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  DB_MAKE_DOUBLE (result_p, result);
  return NO_ERROR;
}

static double
qdata_coerce_numeric_to_double (DB_VALUE * numeric_val_p)
{
  DB_VALUE dbval_tmp;
  DB_DATA_STATUS data_stat;

  db_value_domain_init (&dbval_tmp, DB_TYPE_DOUBLE,
			DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
  (void) numeric_db_value_coerce_from_num (numeric_val_p, &dbval_tmp,
					   &data_stat);

  return DB_GET_DOUBLE (&dbval_tmp);
}

static void
qdata_coerce_dbval_to_numeric (DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  DB_DATA_STATUS data_stat;

  db_value_domain_init (result_p, DB_TYPE_NUMERIC,
			DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
  (void) numeric_db_value_coerce_to_num (dbval_p, result_p, &data_stat);
}

static int
qdata_add_numeric (DB_VALUE * numeric_val_p, DB_VALUE * dbval_p,
		   DB_VALUE * result_p)
{
  DB_VALUE dbval_tmp;

  qdata_coerce_dbval_to_numeric (dbval_p, &dbval_tmp);

  if (numeric_db_value_add (&dbval_tmp, numeric_val_p, result_p) != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  return NO_ERROR;
}

static int
qdata_add_numeric_to_monetary (DB_VALUE * numeric_val_p,
			       DB_VALUE * monetary_val_p, DB_VALUE * result_p)
{
  double d1, d2, dtmp;

  d1 = qdata_coerce_numeric_to_double (numeric_val_p);
  d2 = (DB_GET_MONETARY (monetary_val_p))->amount;

  dtmp = d1 + d2;

  DB_MAKE_MONETARY_TYPE_AMOUNT (result_p,
				(DB_GET_MONETARY (monetary_val_p))->type,
				dtmp);

  return NO_ERROR;
}

static int
qdata_add_monetary (double d1, double d2, DB_CURRENCY type,
		    DB_VALUE * result_p)
{
  double result;

  result = d1 + d2;

  if (OR_CHECK_DOUBLE_OVERFLOW (result))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  DB_MAKE_MONETARY_TYPE_AMOUNT (result_p, type, result);
  return NO_ERROR;
}

static int
qdata_add_int_to_time (DB_VALUE * time_val_p, unsigned int add_time,
		       DB_VALUE * result_p)
{
  unsigned int result, utime;
  DB_TIME *time;
  int hour, minute, second;

  time = DB_GET_TIME (time_val_p);
  utime = (unsigned int) *time % SECONDS_OF_ONE_DAY;

  result = (utime + add_time) % SECONDS_OF_ONE_DAY;

  db_time_decode (&result, &hour, &minute, &second);

  if (PRM_COMPAT_MODE != COMPAT_MYSQL)
    {
      DB_MAKE_TIME (result_p, hour, minute, second);
    }
  else
    {
      DB_TYPE type = DB_VALUE_DOMAIN_TYPE (result_p);

      switch (type)
	{
	case DB_TYPE_INTEGER:
	  DB_MAKE_INT (result_p, (hour * 100 + minute) * 100 + second);
	  break;

	case DB_TYPE_SHORT:
	  DB_MAKE_SHORT (result_p, (hour * 100 + minute) * 100 + second);
	  break;

	default:
	  DB_MAKE_TIME (result_p, hour, minute, second);
	  break;
	}
    }

  return NO_ERROR;
}

static int
qdata_add_bigint_to_time (DB_VALUE * time_val_p, DB_BIGINT add_time,
			  DB_VALUE * result_p)
{
  DB_TIME utime, result;
  int hour, minute, second;

  utime = *(DB_GET_TIME (time_val_p)) % SECONDS_OF_ONE_DAY;
  if (add_time < 0)
    {
      return qdata_subtract_time (utime,
				  (DB_TIME) ((-add_time) %
					     SECONDS_OF_ONE_DAY), result_p);
    }

  result = (utime + add_time) % SECONDS_OF_ONE_DAY;
  db_time_decode (&result, &hour, &minute, &second);

  if (PRM_COMPAT_MODE != COMPAT_MYSQL)
    {
      DB_MAKE_TIME (result_p, hour, minute, second);
    }
  else
    {
      DB_TYPE type = DB_VALUE_DOMAIN_TYPE (result_p);

      switch (type)
	{
	case DB_TYPE_BIGINT:
	  DB_MAKE_BIGINT (result_p, (hour * 100 + minute) * 100 + second);
	  break;

	case DB_TYPE_INTEGER:
	  DB_MAKE_INTEGER (result_p, (hour * 100 + minute) * 100 + second);
	  break;

	default:
	  DB_MAKE_TIME (result_p, hour, minute, second);
	  break;
	}
    }

  return NO_ERROR;
}

static int
qdata_add_short_to_utime_asymmetry (DB_VALUE * utime_val_p, short s,
				    unsigned int *utime, DB_VALUE * result_p,
				    TP_DOMAIN * domain_p)
{
  DB_VALUE tmp;

  if (s == DB_INT16_MIN)	/* check for asymmetry */
    {
      if (*utime == 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_HAPPENED, 0);
	  return ER_QPROC_OVERFLOW_HAPPENED;
	}

      (*utime)--;
      s++;
    }

  DB_MAKE_SHORT (&tmp, -(s));
  return (qdata_subtract_dbval (utime_val_p, &tmp, result_p, domain_p));
}

static int
qdata_add_int_to_utime_asymmetry (DB_VALUE * utime_val_p, int i,
				  unsigned int *utime, DB_VALUE * result_p,
				  TP_DOMAIN * domain_p)
{
  DB_VALUE tmp;

  if (i == DB_INT32_MIN)	/* check for asymmetry */
    {
      if (*utime == 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_HAPPENED, 0);
	  return ER_QPROC_OVERFLOW_HAPPENED;
	}

      (*utime)--;
      i++;
    }

  DB_MAKE_INT (&tmp, -i);
  return (qdata_subtract_dbval (utime_val_p, &tmp, result_p, domain_p));
}

static int
qdata_add_bigint_to_utime_asymmetry (DB_VALUE * utime_val_p, DB_BIGINT bi,
				     unsigned int *utime, DB_VALUE * result_p,
				     TP_DOMAIN * domain_p)
{
  DB_VALUE tmp;

  if (bi == DB_BIGINT_MIN)	/* check for asymmetry */
    {
      if (*utime == 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_HAPPENED, 0);
	  return ER_QPROC_OVERFLOW_HAPPENED;
	}

      (*utime)--;
      bi++;
    }

  DB_MAKE_BIGINT (&tmp, -bi);
  return (qdata_subtract_dbval (utime_val_p, &tmp, result_p, domain_p));
}

static int
qdata_add_short_to_utime (DB_VALUE * utime_val_p, short s,
			  DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_UTIME *utime;
  int utmp, u1, u2;
  DB_DATE date;
  DB_TIME time;
  DB_TYPE type;
  DB_BIGINT bigint = 0;
  int d, m, y, h, mi, sec;

  utime = DB_GET_UTIME (utime_val_p);

  if (s < 0)
    {
      return qdata_add_short_to_utime_asymmetry (utime_val_p, s, utime,
						 result_p, domain_p);
    }

  u1 = (int) s;
  u2 = (int) *utime;
  utmp = u1 + u2;

  if (OR_CHECK_ADD_OVERFLOW (u1, u2, utmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  if (PRM_COMPAT_MODE != COMPAT_MYSQL)
    {
      DB_MAKE_UTIME (result_p, utmp);
    }
  else
    {
      type = DB_VALUE_DOMAIN_TYPE (result_p);

      switch (type)
	{
	case DB_TYPE_BIGINT:
	  db_timestamp_decode (&utmp, &date, &time);
	  db_date_decode (&date, &m, &d, &y);
	  db_time_decode (&time, &h, &mi, &sec);
	  bigint = (y * 100 + m) * 100 + d;
	  bigint = ((bigint * 100 + h) * 100 + mi) * 100 + sec;
	  DB_MAKE_BIGINT (result_p, bigint);
	  break;

	default:
	  DB_MAKE_UTIME (result_p, utmp);
	  break;
	}
    }

  return NO_ERROR;
}

static int
qdata_add_int_to_utime (DB_VALUE * utime_val_p, int i, DB_VALUE * result_p,
			TP_DOMAIN * domain_p)
{
  DB_UTIME *utime;
  int utmp, u1, u2;
  DB_DATE date;
  DB_TIME time;
  DB_TYPE type;
  DB_BIGINT bigint;
  int d, m, y, h, mi, s;

  utime = DB_GET_UTIME (utime_val_p);

  if (i < 0)
    {
      return qdata_add_int_to_utime_asymmetry (utime_val_p, i, utime,
					       result_p, domain_p);
    }

  u1 = (int) i;
  u2 = (int) *utime;
  utmp = u1 + u2;

  if (OR_CHECK_ADD_OVERFLOW (u1, u2, utmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  if (PRM_COMPAT_MODE != COMPAT_MYSQL)
    {
      DB_MAKE_UTIME (result_p, utmp);
    }
  else
    {
      type = DB_VALUE_DOMAIN_TYPE (result_p);

      switch (type)
	{
	case DB_TYPE_BIGINT:
	  db_timestamp_decode (&utmp, &date, &time);
	  db_date_decode (&date, &m, &d, &y);
	  db_time_decode (&time, &h, &mi, &s);
	  bigint = (y * 100 + m) * 100 + d;
	  bigint = ((bigint * 100 + h) * 100 + mi) * 100 + s;
	  DB_MAKE_BIGINT (result_p, bigint);
	  break;

	default:
	  DB_MAKE_UTIME (result_p, utmp);
	  break;
	}
    }

  return NO_ERROR;
}

static int
qdata_add_bigint_to_utime (DB_VALUE * utime_val_p, DB_BIGINT bi,
			   DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_UTIME *utime;
  DB_BIGINT utmp, u1, u2;
  DB_DATE date;
  DB_TIME time;
  DB_TYPE type;
  DB_BIGINT bigint;
  int d, m, y, h, mi, s;

  utime = DB_GET_UTIME (utime_val_p);

  if (bi < 0)
    {
      return qdata_add_bigint_to_utime_asymmetry (utime_val_p, bi, utime,
						  result_p, domain_p);
    }

  u1 = bi;
  u2 = *utime;
  utmp = u1 + u2;

  if (OR_CHECK_UNS_ADD_OVERFLOW (u1, u2, utmp) || INT_MAX < utmp)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }
  if (PRM_COMPAT_MODE != COMPAT_MYSQL)
    {
      DB_MAKE_UTIME (result_p, (unsigned int) utmp);	/* truncate to 4bytes time_t */
    }
  else
    {
      type = DB_VALUE_DOMAIN_TYPE (result_p);

      switch (type)
	{
	case DB_TYPE_BIGINT:
	  {
	    DB_TIMESTAMP timestamp = (DB_TIMESTAMP) utmp;
	    db_timestamp_decode (&timestamp, &date, &time);
	    db_date_decode (&date, &m, &d, &y);
	    db_time_decode (&time, &h, &mi, &s);
	    bigint = (y * 100 + m) * 100 + d;
	    bigint = ((bigint * 100 + h) * 100 + mi) * 100 + s;
	    DB_MAKE_BIGINT (result_p, bigint);
	  }
	  break;

	default:
	  DB_MAKE_UTIME (result_p, (unsigned int) utmp);
	  break;
	}
    }

  return NO_ERROR;
}

static int
qdata_add_short_to_datetime (DB_VALUE * datetime_val_p, short s,
			     DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_DATETIME *datetime;
  DB_DATETIME tmp;
  int error = NO_ERROR;

  datetime = DB_GET_DATETIME (datetime_val_p);

  error = db_add_int_to_datetime (datetime, s, &tmp);
  if (error == NO_ERROR)
    {
      DB_MAKE_DATETIME (result_p, &tmp);
    }
  return error;
}

static int
qdata_add_int_to_datetime (DB_VALUE * datetime_val_p, int i,
			   DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_DATETIME *datetime;
  DB_DATETIME tmp;
  int error = NO_ERROR;

  datetime = DB_GET_DATETIME (datetime_val_p);

  error = db_add_int_to_datetime (datetime, i, &tmp);
  if (error == NO_ERROR)
    {
      DB_MAKE_DATETIME (result_p, &tmp);
    }
  return error;
}

static int
qdata_add_bigint_to_datetime (DB_VALUE * datetime_val_p, DB_BIGINT bi,
			      DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_DATETIME *datetime;
  DB_DATETIME tmp;
  int error = NO_ERROR;

  datetime = DB_GET_DATETIME (datetime_val_p);

  error = db_add_int_to_datetime (datetime, bi, &tmp);
  if (error == NO_ERROR)
    {
      DB_MAKE_DATETIME (result_p, &tmp);
    }
  return error;
}

static int
qdata_add_short_to_date (DB_VALUE * date_val_p, short s, DB_VALUE * result_p,
			 TP_DOMAIN * domain_p)
{
  DB_DATE *date;
  unsigned int utmp, u1, u2;
  int day, month, year;

  date = DB_GET_DATE (date_val_p);
  if (s < 0)
    {
      return qdata_add_short_to_utime_asymmetry (date_val_p, s, date,
						 result_p, domain_p);
    }

  u1 = (unsigned int) s;
  u2 = (unsigned int) *date;
  utmp = u1 + u2;

  if (OR_CHECK_UNS_ADD_OVERFLOW (u1, u2, utmp) || utmp > DB_DATE_MAX)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  db_date_decode (&utmp, &month, &day, &year);

  if (PRM_COMPAT_MODE != COMPAT_MYSQL)
    {
      DB_MAKE_DATE (result_p, month, day, year);
    }
  else
    {
      DB_TYPE type = DB_VALUE_DOMAIN_TYPE (result_p);

      switch (type)
	{
	case DB_TYPE_SHORT:
	  DB_MAKE_SHORT (result_p, (year * 100 + month) * 100 + day);
	  break;

	default:
	  DB_MAKE_DATE (result_p, month, day, year);
	  break;
	}
    }

  return NO_ERROR;
}

static int
qdata_add_int_to_date (DB_VALUE * date_val_p, int i, DB_VALUE * result_p,
		       TP_DOMAIN * domain_p)
{
  DB_DATE *date;
  unsigned int utmp, u1, u2;
  int day, month, year;

  date = DB_GET_DATE (date_val_p);

  if (i < 0)
    {
      return qdata_add_int_to_utime_asymmetry (date_val_p, i, date, result_p,
					       domain_p);
    }

  u1 = (unsigned int) i;
  u2 = (unsigned int) *date;
  utmp = u1 + u2;

  if (OR_CHECK_UNS_ADD_OVERFLOW (u1, u2, utmp) || utmp > DB_DATE_MAX)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  db_date_decode (&utmp, &month, &day, &year);
  if (PRM_COMPAT_MODE != COMPAT_MYSQL)
    {
      DB_MAKE_DATE (result_p, month, day, year);
    }
  else
    {
      DB_TYPE type = DB_VALUE_DOMAIN_TYPE (result_p);

      switch (type)
	{
	case DB_TYPE_INTEGER:
	  DB_MAKE_INT (result_p, (year * 100 + month) * 100 + day);
	  break;

	default:
	  DB_MAKE_DATE (result_p, month, day, year);
	  break;
	}
    }

  return NO_ERROR;
}

static int
qdata_add_bigint_to_date (DB_VALUE * date_val_p, DB_BIGINT bi,
			  DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_DATE *date;
  DB_BIGINT utmp, u1, u2;
  DB_DATE tmp_date;
  int day, month, year;

  date = DB_GET_DATE (date_val_p);

  if (bi < 0)
    {
      return qdata_add_bigint_to_utime_asymmetry (date_val_p, bi, date,
						  result_p, domain_p);
    }

  u1 = bi;
  u2 = *date;
  utmp = u1 + u2;

  if (OR_CHECK_UNS_ADD_OVERFLOW (u1, u2, utmp) || utmp > DB_DATE_MAX)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  tmp_date = (DB_DATE) utmp;
  db_date_decode (&tmp_date, &month, &day, &year);
  if (PRM_COMPAT_MODE == COMPAT_MYSQL)
    {
      DB_MAKE_DATE (result_p, month, day, year);
    }
  else
    {
      DB_TYPE type = DB_VALUE_DOMAIN_TYPE (result_p);

      switch (type)
	{
	case DB_TYPE_BIGINT:
	  DB_MAKE_BIGINT (result_p, (year * 100 + month) * 100 + day);
	  break;

	default:
	  DB_MAKE_DATE (result_p, month, day, year);
	  break;
	}
    }

  return NO_ERROR;
}

static int
qdata_add_short_to_dbval (DB_VALUE * short_val_p, DB_VALUE * dbval_p,
			  DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  short s;
  DB_TYPE type;

  s = DB_GET_SHORT (short_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_add_short (s, dbval_p, result_p);

    case DB_TYPE_INTEGER:
      return qdata_add_int (s, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_add_bigint (s, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_FLOAT:
      return qdata_add_float (s, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_add_double (s, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_add_numeric (dbval_p, short_val_p, result_p);

    case DB_TYPE_MONETARY:
      return qdata_add_monetary (s, (DB_GET_MONETARY (dbval_p))->amount,
				 (DB_GET_MONETARY (dbval_p))->type, result_p);

    case DB_TYPE_TIME:
      return qdata_add_bigint_to_time (dbval_p, (DB_BIGINT) s, result_p);

    case DB_TYPE_UTIME:
      return qdata_add_short_to_utime (dbval_p, s, result_p, domain_p);

    case DB_TYPE_DATETIME:
      return qdata_add_short_to_datetime (dbval_p, s, result_p, domain_p);

    case DB_TYPE_DATE:
      return qdata_add_short_to_date (dbval_p, s, result_p, domain_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_int_to_dbval (DB_VALUE * int_val_p, DB_VALUE * dbval_p,
			DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  int i;
  DB_TYPE type;

  i = DB_GET_INT (int_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_add_int (i, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_add_int (i, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_add_bigint (i, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_FLOAT:
      return qdata_add_float ((float) i, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_add_double (i, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_add_numeric (dbval_p, int_val_p, result_p);
      break;

    case DB_TYPE_MONETARY:
      return qdata_add_monetary (i, (DB_GET_MONETARY (dbval_p))->amount,
				 (DB_GET_MONETARY (dbval_p))->type, result_p);

    case DB_TYPE_TIME:
      return qdata_add_bigint_to_time (dbval_p, (DB_BIGINT) i, result_p);

    case DB_TYPE_UTIME:
      return qdata_add_int_to_utime (dbval_p, i, result_p, domain_p);

    case DB_TYPE_DATETIME:
      return qdata_add_int_to_datetime (dbval_p, i, result_p, domain_p);

    case DB_TYPE_DATE:
      return qdata_add_int_to_date (dbval_p, i, result_p, domain_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_bigint_to_dbval (DB_VALUE * bigint_val_p, DB_VALUE * dbval_p,
			   DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_BIGINT bi;
  DB_TYPE type;

  bi = DB_GET_BIGINT (bigint_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_add_bigint (bi, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_add_bigint (bi, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_add_bigint (bi, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_FLOAT:
      return qdata_add_float ((float) bi, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_add_double ((double) bi, DB_GET_DOUBLE (dbval_p),
			       result_p);

    case DB_TYPE_NUMERIC:
      return qdata_add_numeric (dbval_p, bigint_val_p, result_p);
      break;

    case DB_TYPE_MONETARY:
      return qdata_add_monetary ((double) bi,
				 (DB_GET_MONETARY (dbval_p))->amount,
				 (DB_GET_MONETARY (dbval_p))->type, result_p);

    case DB_TYPE_TIME:
      return qdata_add_bigint_to_time (dbval_p, bi, result_p);

    case DB_TYPE_UTIME:
      return qdata_add_bigint_to_utime (dbval_p, bi, result_p, domain_p);

    case DB_TYPE_DATE:
      return qdata_add_bigint_to_date (dbval_p, bi, result_p, domain_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_float_to_dbval (DB_VALUE * float_val_p, DB_VALUE * dbval_p,
			  DB_VALUE * result_p)
{
  float f1;
  DB_TYPE type;

  f1 = DB_GET_FLOAT (float_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_add_float (f1, (float) DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_add_float (f1, (float) DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_add_double (f1, (double) DB_GET_BIGINT (dbval_p),
			       result_p);

    case DB_TYPE_FLOAT:
      return qdata_add_float (f1, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_add_double (f1, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_add_double (f1, qdata_coerce_numeric_to_double (dbval_p),
			       result_p);

    case DB_TYPE_MONETARY:
      return qdata_add_monetary (f1, (DB_GET_MONETARY (dbval_p))->amount,
				 (DB_GET_MONETARY (dbval_p))->type, result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_double_to_dbval (DB_VALUE * double_val_p, DB_VALUE * dbval_p,
			   DB_VALUE * result_p)
{
  double d1;
  DB_TYPE type;

  d1 = DB_GET_DOUBLE (double_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_add_double (d1, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_add_double (d1, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_add_double (d1, (double) DB_GET_BIGINT (dbval_p),
			       result_p);

    case DB_TYPE_FLOAT:
      return qdata_add_double (d1, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_add_double (d1, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_add_double (d1, qdata_coerce_numeric_to_double (dbval_p),
			       result_p);

    case DB_TYPE_MONETARY:
      return qdata_add_monetary (d1, (DB_GET_MONETARY (dbval_p))->amount,
				 (DB_GET_MONETARY (dbval_p))->type, result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_numeric_to_dbval (DB_VALUE * numeric_val_p, DB_VALUE * dbval_p,
			    DB_VALUE * result_p)
{
  DB_TYPE type;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
      return qdata_add_numeric (numeric_val_p, dbval_p, result_p);

    case DB_TYPE_NUMERIC:
      if (numeric_db_value_add (numeric_val_p, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_HAPPENED, 0);
	  return ER_QPROC_OVERFLOW_HAPPENED;
	}
      break;

    case DB_TYPE_FLOAT:
      return qdata_add_double (qdata_coerce_numeric_to_double (numeric_val_p),
			       DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_add_double (qdata_coerce_numeric_to_double (numeric_val_p),
			       DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_MONETARY:
      return qdata_add_numeric_to_monetary (numeric_val_p, dbval_p, result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_monetary_to_dbval (DB_VALUE * monetary_val_p, DB_VALUE * dbval_p,
			     DB_VALUE * result_p)
{
  DB_TYPE type;
  double d1;
  DB_CURRENCY currency;

  d1 = (DB_GET_MONETARY (monetary_val_p))->amount;
  currency = (DB_GET_MONETARY (monetary_val_p))->type;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_add_monetary (d1, DB_GET_SHORT (dbval_p), currency,
				 result_p);

    case DB_TYPE_INTEGER:
      return qdata_add_monetary (d1, DB_GET_INT (dbval_p), currency,
				 result_p);

    case DB_TYPE_BIGINT:
      return qdata_add_monetary (d1, (double) DB_GET_BIGINT (dbval_p),
				 currency, result_p);

    case DB_TYPE_FLOAT:
      return qdata_add_monetary (d1, DB_GET_FLOAT (dbval_p), currency,
				 result_p);

    case DB_TYPE_DOUBLE:
      return qdata_add_monetary (d1, DB_GET_DOUBLE (dbval_p), currency,
				 result_p);

    case DB_TYPE_NUMERIC:
      return qdata_add_numeric_to_monetary (dbval_p, monetary_val_p,
					    result_p);

    case DB_TYPE_MONETARY:
      /* Note: we probably should return an error if the two monetaries
       * have different monetary types.
       */
      return qdata_add_monetary (d1, (DB_GET_MONETARY (dbval_p))->amount,
				 currency, result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_chars_to_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
			  DB_VALUE * result_p)
{
  DB_DATA_STATUS data_stat;

  if ((db_string_concatenate (dbval1_p, dbval2_p, result_p,
			      &data_stat) != NO_ERROR)
      || (data_stat != DATA_STATUS_OK))
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
qdata_add_sequence_to_dbval (DB_VALUE * seq_val_p, DB_VALUE * dbval_p,
			     DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_SET *set_tmp;
  DB_SEQ *seq_tmp, *seq_tmp1;
  DB_VALUE dbval_tmp;
  int i, card, card1;

  if (domain_p == NULL)
    {
      return ER_FAILED;
    }

  DB_MAKE_NULL (&dbval_tmp);

  if (domain_p->type->id == DB_TYPE_SEQUENCE)
    {
      if (tp_value_coerce (seq_val_p, result_p, domain_p) !=
	  DOMAIN_COMPATIBLE)
	{
	  return ER_FAILED;
	}

      seq_tmp = DB_GET_SEQUENCE (dbval_p);
      card = db_seq_size (seq_tmp);
      seq_tmp1 = DB_GET_SEQUENCE (result_p);
      card1 = db_seq_size (seq_tmp1);

      for (i = 0; i < card; i++)
	{
	  if (db_seq_get (seq_tmp, i, &dbval_tmp) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }

	  if (db_seq_put (seq_tmp1, card1 + i, &dbval_tmp) != NO_ERROR)
	    {
	      pr_clear_value (&dbval_tmp);
	      return ER_FAILED;
	    }

	  pr_clear_value (&dbval_tmp);
	}
    }
  else
    {
      /* set or multiset */
      if (set_union (DB_GET_SET (seq_val_p), DB_GET_SET (dbval_p),
		     &set_tmp, domain_p) < 0)
	{
	  return ER_FAILED;
	}

      pr_clear_value (result_p);
      set_make_collection (result_p, set_tmp);
    }

  return NO_ERROR;
}

static int
qdata_add_time_to_dbval (DB_VALUE * time_val_p, DB_VALUE * dbval_p,
			 DB_VALUE * result_p)
{
  DB_TYPE type;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_add_bigint_to_time (time_val_p,
				       (DB_BIGINT) DB_GET_SHORT (dbval_p),
				       result_p);

    case DB_TYPE_INTEGER:
      return qdata_add_bigint_to_time (time_val_p,
				       (DB_BIGINT) DB_GET_INT (dbval_p),
				       result_p);

    case DB_TYPE_BIGINT:
      return qdata_add_bigint_to_time (time_val_p, DB_GET_BIGINT (dbval_p),
				       result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_utime_to_dbval (DB_VALUE * utime_val_p, DB_VALUE * dbval_p,
			  DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_add_short_to_utime (utime_val_p, DB_GET_SHORT (dbval_p),
				       result_p, domain_p);

    case DB_TYPE_INTEGER:
      return qdata_add_int_to_utime (utime_val_p, DB_GET_INT (dbval_p),
				     result_p, domain_p);

    case DB_TYPE_BIGINT:
      return qdata_add_bigint_to_utime (utime_val_p, DB_GET_BIGINT (dbval_p),
					result_p, domain_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_datetime_to_dbval (DB_VALUE * datetime_val_p, DB_VALUE * dbval_p,
			     DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_add_short_to_datetime (datetime_val_p,
					  DB_GET_SHORT (dbval_p), result_p,
					  domain_p);

    case DB_TYPE_INTEGER:
      return qdata_add_int_to_datetime (datetime_val_p, DB_GET_INT (dbval_p),
					result_p, domain_p);

    case DB_TYPE_BIGINT:
      return qdata_add_bigint_to_datetime (datetime_val_p,
					   DB_GET_BIGINT (dbval_p), result_p,
					   domain_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_date_to_dbval (DB_VALUE * date_val_p, DB_VALUE * dbval_p,
			 DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_add_short_to_date (date_val_p, DB_GET_SHORT (dbval_p),
				      result_p, domain_p);

    case DB_TYPE_INTEGER:
      return qdata_add_int_to_date (date_val_p, DB_GET_INT (dbval_p),
				    result_p, domain_p);

    case DB_TYPE_BIGINT:
      return qdata_add_bigint_to_date (date_val_p, DB_GET_BIGINT (dbval_p),
				       result_p, domain_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_coerce_result_to_domain (DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  if (domain_p != NULL)
    {
      if (tp_value_coerce (result_p, result_p, domain_p) != DOMAIN_COMPATIBLE)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
		  pr_type_name (PRIM_TYPE (result_p)),
		  pr_type_name (domain_p->type->id));
	  return ER_TP_CANT_COERCE;
	}
    }

  return NO_ERROR;
}

static int
qdata_cast_to_domain (DB_VALUE * dbval_p, DB_VALUE * result_p,
		      TP_DOMAIN * domain_p)
{
  if (domain_p != NULL)
    {
      if (tp_value_cast (dbval_p, result_p, domain_p, false)
	  != DOMAIN_COMPATIBLE)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
		  pr_type_name (PRIM_TYPE (dbval_p)),
		  pr_type_name (domain_p->type->id));
	  return ER_TP_CANT_COERCE;
	}
    }

  return NO_ERROR;
}

/*
 * qdata_add_dbval () -
 *   return: NO_ERROR, or ER_code
 *   dbval1(in) : First db_value node
 *   dbval2(in) : Second db_value node
 *   res(out)   : Resultant db_value node
 *   domain(in) :
 *
 * Note: Add two db_values.
 * Overflow checks are only done when both operand maximums have
 * overlapping precision/scale.  That is,
 *     short + integer -> overflow is checked
 *     float + double  -> overflow is not checked.  Maximum float
 *                        value does not overlap maximum double
 *                        precision/scale.
 *                        MAX_FLT + MAX_DBL = MAX_DBL
 */
int
qdata_add_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		 DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type1;
  int error = NO_ERROR;

  if ((domain_p != NULL && domain_p->type->id == DB_TYPE_NULL)
      || DB_IS_NULL (dbval1_p) || DB_IS_NULL (dbval2_p))
    {
      return NO_ERROR;
    }

  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);

  switch (type1)
    {
    case DB_TYPE_SHORT:
      error = qdata_add_short_to_dbval (dbval1_p, dbval2_p, result_p,
					domain_p);
      break;

    case DB_TYPE_INTEGER:
      error = qdata_add_int_to_dbval (dbval1_p, dbval2_p, result_p, domain_p);
      break;

    case DB_TYPE_BIGINT:
      error = qdata_add_bigint_to_dbval (dbval1_p, dbval2_p, result_p,
					 domain_p);
      break;

    case DB_TYPE_FLOAT:
      error = qdata_add_float_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_DOUBLE:
      error = qdata_add_double_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_NUMERIC:
      error = qdata_add_numeric_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_MONETARY:
      error = qdata_add_monetary_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
    case DB_TYPE_BIT:
    case DB_TYPE_VARBIT:
      error = qdata_add_chars_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
    case DB_TYPE_SET:
      error = qdata_add_sequence_to_dbval (dbval1_p, dbval2_p, result_p,
					   domain_p);
      break;

    case DB_TYPE_TIME:
      error = qdata_add_time_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_UTIME:
      error = qdata_add_utime_to_dbval (dbval1_p, dbval2_p, result_p,
					domain_p);
      break;

    case DB_TYPE_DATETIME:
      error = qdata_add_datetime_to_dbval (dbval1_p, dbval2_p, result_p,
					   domain_p);
      break;

    case DB_TYPE_DATE:
      error = qdata_add_date_to_dbval (dbval1_p, dbval2_p, result_p,
				       domain_p);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      return ER_QPROC_INVALID_DATATYPE;
    }

  if (error != NO_ERROR)
    {
      return error;
    }

  return qdata_coerce_result_to_domain (result_p, domain_p);
}

/*
 * qdata_increment_dbval () -
 *   return: NO_ERROR, or ER_code
 *   dbval1(in) : db_value node
 *   res(in)    :
 *   incval(in) :
 *
 * Note: Increment the db_value.
 * If overflow happens, reset the db_value as 0.
 */
int
qdata_increment_dbval (DB_VALUE * dbval_p, DB_VALUE * result_p, int inc_val)
{
  DB_TYPE type1;
  short stmp, s1;
  int itmp, i1;
  DB_BIGINT bitmp, bi1;

  type1 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type1)
    {
    case DB_TYPE_SHORT:
      s1 = DB_GET_SHORT (dbval_p);
      stmp = s1 + inc_val;
      if ((inc_val > 0 && OR_CHECK_ADD_OVERFLOW (s1, inc_val, stmp))
	  || (inc_val < 0 && OR_CHECK_SUB_UNDERFLOW (s1, -inc_val, stmp)))
	{
	  stmp = 0;
	}

      DB_MAKE_SHORT (result_p, stmp);
      break;

    case DB_TYPE_INTEGER:
      i1 = DB_GET_INT (dbval_p);
      itmp = i1 + inc_val;
      if ((inc_val > 0 && OR_CHECK_ADD_OVERFLOW (i1, inc_val, itmp))
	  || (inc_val < 0 && OR_CHECK_SUB_UNDERFLOW (i1, -inc_val, itmp)))
	{
	  itmp = 0;
	}

      DB_MAKE_INT (result_p, itmp);
      break;

    case DB_TYPE_BIGINT:
      bi1 = DB_GET_BIGINT (dbval_p);
      bitmp = bi1 + inc_val;
      if ((inc_val > 0 && OR_CHECK_ADD_OVERFLOW (bi1, inc_val, bitmp))
	  || (inc_val < 0 && OR_CHECK_SUB_UNDERFLOW (bi1, -inc_val, bitmp)))
	{
	  bitmp = 0;
	}

      DB_MAKE_BIGINT (result_p, bitmp);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
qdata_subtract_short (short s1, short s2, DB_VALUE * result_p)
{
  short stmp;

  stmp = s1 - s2;

  if (OR_CHECK_SUB_UNDERFLOW (s1, s2, stmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_SUBTRACTION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_SHORT (result_p, stmp);
  return NO_ERROR;
}

static int
qdata_subtract_int (int i1, int i2, DB_VALUE * result_p)
{
  int itmp;

  itmp = i1 - i2;

  if (OR_CHECK_SUB_UNDERFLOW (i1, i2, itmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_SUBTRACTION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_INT (result_p, itmp);
  return NO_ERROR;
}

static int
qdata_subtract_bigint (DB_BIGINT bi1, DB_BIGINT bi2, DB_VALUE * result_p)
{
  DB_BIGINT bitmp;

  bitmp = bi1 - bi2;

  if (OR_CHECK_SUB_UNDERFLOW (bi1, bi2, bitmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_SUBTRACTION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_BIGINT (result_p, bitmp);
  return NO_ERROR;
}

static int
qdata_subtract_float (float f1, float f2, DB_VALUE * result_p)
{
  float ftmp;

  ftmp = f1 - f2;

  if (OR_CHECK_FLOAT_OVERFLOW (ftmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_SUBTRACTION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_FLOAT (result_p, ftmp);
  return NO_ERROR;
}

static int
qdata_subtract_double (double d1, double d2, DB_VALUE * result_p)
{
  double dtmp;

  dtmp = d1 - d2;

  if (OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_SUBTRACTION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_DOUBLE (result_p, dtmp);
  return NO_ERROR;
}

static int
qdata_subtract_monetary (double d1, double d2, DB_CURRENCY currency,
			 DB_VALUE * result_p)
{
  double dtmp;

  dtmp = d1 - d2;

  if (OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_SUBTRACTION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_MONETARY_TYPE_AMOUNT (result_p, currency, dtmp);
  return NO_ERROR;
}

static int
qdata_subtract_time (DB_TIME u1, DB_TIME u2, DB_VALUE * result_p)
{
  DB_TIME utmp;
  int hour, minute, second;

  if (u1 < u2)
    {
      u1 += SECONDS_OF_ONE_DAY;
    }

  utmp = u1 - u2;
  db_time_decode (&utmp, &hour, &minute, &second);
  DB_MAKE_TIME (result_p, hour, minute, second);

  return NO_ERROR;
}

static int
qdata_subtract_utime (DB_UTIME u1, DB_UTIME u2, DB_VALUE * result_p)
{
  DB_UTIME utmp;

  utmp = u1 - u2;
  if (OR_CHECK_UNS_SUB_UNDERFLOW (u1, u2, utmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_TIME_UNDERFLOW, 0);
      return ER_FAILED;
    }

  DB_MAKE_UTIME (result_p, utmp);
  return NO_ERROR;
}

static int
qdata_subtract_utime_to_short_asymmetry (DB_VALUE * utime_val_p, short s,
					 unsigned int *utime,
					 DB_VALUE * result_p,
					 TP_DOMAIN * domain_p)
{
  DB_VALUE tmp;

  if (s == DB_INT16_MIN)	/* check for asymmetry. */
    {
      if (*utime == DB_UINT32_MAX)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_TIME_UNDERFLOW,
		  0);
	  return ER_FAILED;
	}

      (*utime)++;
      s++;
    }

  DB_MAKE_SHORT (&tmp, -(s));
  return (qdata_add_dbval (utime_val_p, &tmp, result_p, domain_p));
}

static int
qdata_subtract_utime_to_int_asymmetry (DB_VALUE * utime_val_p, int i,
				       unsigned int *utime,
				       DB_VALUE * result_p,
				       TP_DOMAIN * domain_p)
{
  DB_VALUE tmp;

  if (i == DB_INT32_MIN)	/* check for asymmetry. */
    {
      if (*utime == DB_UINT32_MAX)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_TIME_UNDERFLOW,
		  0);
	  return ER_FAILED;
	}

      (*utime)++;
      i++;
    }

  DB_MAKE_INT (&tmp, -(i));
  return (qdata_add_dbval (utime_val_p, &tmp, result_p, domain_p));
}

static int
qdata_subtract_utime_to_bigint_asymmetry (DB_VALUE * utime_val_p,
					  DB_BIGINT bi, unsigned int *utime,
					  DB_VALUE * result_p,
					  TP_DOMAIN * domain_p)
{
  DB_VALUE tmp;

  if (bi == DB_BIGINT_MIN)	/* check for asymmetry. */
    {
      if (*utime == DB_UINT32_MAX)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_TIME_UNDERFLOW,
		  0);
	  return ER_FAILED;
	}

      (*utime)++;
      bi++;
    }

  DB_MAKE_BIGINT (&tmp, -(bi));
  return (qdata_add_dbval (utime_val_p, &tmp, result_p, domain_p));
}

static int
qdata_subtract_datetime_to_int (DB_DATETIME * dt1, DB_BIGINT i2,
				DB_VALUE * result_p)
{
  DB_DATETIME datetime_tmp;
  int error;

  error = db_subtract_int_from_datetime (dt1, i2, &datetime_tmp);
  if (error != NO_ERROR)
    {
      return error;
    }

  DB_MAKE_DATETIME (result_p, &datetime_tmp);
  return NO_ERROR;
}

static int
qdata_subtract_datetime (DB_DATETIME * dt1, DB_DATETIME * dt2,
			 DB_VALUE * result_p)
{
  DB_BIGINT u1, u2, tmp;

  u1 = ((DB_BIGINT) dt1->date) * MILLISECONDS_OF_ONE_DAY + dt1->time;
  u2 = ((DB_BIGINT) dt2->date) * MILLISECONDS_OF_ONE_DAY + dt2->time;

  tmp = u1 - u2;
  if (OR_CHECK_SUB_UNDERFLOW (u1, u2, tmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_TIME_UNDERFLOW, 0);
      return ER_FAILED;
    }

  DB_MAKE_BIGINT (result_p, tmp);
  return NO_ERROR;
}

static int
qdata_subtract_datetime_to_int_asymmetry (DB_VALUE * datetime_val_p,
					  DB_BIGINT i,
					  DB_DATETIME * datetime,
					  DB_VALUE * result_p,
					  TP_DOMAIN * domain_p)
{
  DB_VALUE tmp;

  if (i == DB_BIGINT_MIN)	/* check for asymmetry. */
    {
      if (datetime->time == 0)
	{
	  datetime->date--;
	  datetime->time = MILLISECONDS_OF_ONE_DAY;
	}

      datetime->time--;
      i++;
    }

  DB_MAKE_BIGINT (&tmp, -(i));
  return (qdata_add_dbval (datetime_val_p, &tmp, result_p, domain_p));
}

static int
qdata_subtract_short_to_dbval (DB_VALUE * short_val_p, DB_VALUE * dbval_p,
			       DB_VALUE * result_p)
{
  short s;
  DB_TYPE type2;
  DB_VALUE dbval_tmp;
  DB_TIME *timeval, timetmp;
  DB_DATE *date;
  unsigned int u1, u2, utmp;
  DB_UTIME *utime;
  DB_DATETIME *datetime;
  DB_DATETIME datetime_tmp;
  int hour, minute, second;

  s = DB_GET_SHORT (short_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_subtract_short (s, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_subtract_int (s, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_subtract_bigint (s, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_FLOAT:
      return qdata_subtract_float (s, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_subtract_double (s, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      qdata_coerce_dbval_to_numeric (short_val_p, &dbval_tmp);

      if (numeric_db_value_sub (&dbval_tmp, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_SUBTRACTION, 0);
	  return ER_QPROC_OVERFLOW_SUBTRACTION;
	}
      break;

    case DB_TYPE_MONETARY:
      return qdata_subtract_monetary (s, (DB_GET_MONETARY (dbval_p))->amount,
				      (DB_GET_MONETARY (dbval_p))->type,
				      result_p);

    case DB_TYPE_TIME:
      if (s < 0)
	{
	  timetmp = s + SECONDS_OF_ONE_DAY;
	}
      else
	{
	  timetmp = s;
	}
      timeval = DB_GET_TIME (dbval_p);
      return qdata_subtract_time (timetmp,
				  (DB_TIME) (*timeval % SECONDS_OF_ONE_DAY),
				  result_p);

    case DB_TYPE_UTIME:
      utime = DB_GET_UTIME (dbval_p);
      return qdata_subtract_utime ((DB_UTIME) s, *utime, result_p);

    case DB_TYPE_DATETIME:
      datetime = DB_GET_DATETIME (dbval_p);

      datetime_tmp.date = s / MILLISECONDS_OF_ONE_DAY;
      datetime_tmp.time = s % MILLISECONDS_OF_ONE_DAY;

      return qdata_subtract_datetime (&datetime_tmp, datetime, result_p);

    case DB_TYPE_DATE:
      date = DB_GET_DATE (dbval_p);

      u1 = (unsigned int) s;
      u2 = (unsigned int) *date;
      utmp = u1 - u2;

      if (s < 0 || OR_CHECK_UNS_SUB_UNDERFLOW (u1, u2, utmp))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_DATE_UNDERFLOW,
		  0);
	  return ER_FAILED;
	}

      db_time_decode (&utmp, &hour, &minute, &second);
      DB_MAKE_TIME (result_p, hour, minute, second);
      break;

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_int_to_dbval (DB_VALUE * int_val_p, DB_VALUE * dbval_p,
			     DB_VALUE * result_p)
{
  int i;
  DB_TYPE type;
  DB_VALUE dbval_tmp;
  DB_TIME *timeval;
  DB_DATE *date;
  DB_DATETIME *datetime, datetime_tmp;
  unsigned int u1, u2, utmp;
  DB_UTIME *utime;
  int day, month, year;

  i = DB_GET_INT (int_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_subtract_int (i, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_subtract_int (i, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_subtract_bigint (i, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_FLOAT:
      return qdata_subtract_float ((float) i,
				   DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_subtract_double (i, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      qdata_coerce_dbval_to_numeric (int_val_p, &dbval_tmp);

      if (numeric_db_value_sub (&dbval_tmp, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_SUBTRACTION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_MONETARY:
      return qdata_subtract_monetary (i, (DB_GET_MONETARY (dbval_p))->amount,
				      (DB_GET_MONETARY (dbval_p))->type,
				      result_p);

    case DB_TYPE_TIME:
      if (i < 0)
	{
	  i = (i % SECONDS_OF_ONE_DAY) + SECONDS_OF_ONE_DAY;
	}
      else
	{
	  i %= SECONDS_OF_ONE_DAY;
	}
      timeval = DB_GET_TIME (dbval_p);
      return qdata_subtract_time ((DB_TIME) i,
				  (DB_TIME) (*timeval % SECONDS_OF_ONE_DAY),
				  result_p);

    case DB_TYPE_UTIME:
      utime = DB_GET_UTIME (dbval_p);
      return qdata_subtract_utime ((DB_UTIME) i, *utime, result_p);

    case DB_TYPE_DATETIME:
      datetime = DB_GET_DATETIME (dbval_p);

      datetime_tmp.date = i / MILLISECONDS_OF_ONE_DAY;
      datetime_tmp.time = i % MILLISECONDS_OF_ONE_DAY;

      return qdata_subtract_datetime (&datetime_tmp, datetime, result_p);

    case DB_TYPE_DATE:
      date = DB_GET_DATE (dbval_p);

      u1 = (unsigned int) i;
      u2 = (unsigned int) *date;
      utmp = u1 - u2;

      if (i < 0 || OR_CHECK_UNS_SUB_UNDERFLOW (u1, u2, utmp))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_DATE_UNDERFLOW,
		  0);
	  return ER_FAILED;
	}

      db_date_decode (&utmp, &month, &day, &year);
      DB_MAKE_DATE (result_p, month, day, year);
      break;

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_bigint_to_dbval (DB_VALUE * bigint_val_p, DB_VALUE * dbval_p,
				DB_VALUE * result_p)
{
  DB_BIGINT bi;
  DB_TYPE type;
  DB_VALUE dbval_tmp;
  DB_TIME *timeval;
  DB_DATE *date;
  unsigned int u1, u2, utmp;
  DB_UTIME *utime;
  int day, month, year;

  bi = DB_GET_BIGINT (bigint_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_subtract_bigint (bi, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_subtract_bigint (bi, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_subtract_bigint (bi, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_FLOAT:
      return qdata_subtract_float ((float) bi,
				   DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_subtract_double ((double) bi, DB_GET_DOUBLE (dbval_p),
				    result_p);

    case DB_TYPE_NUMERIC:
      qdata_coerce_dbval_to_numeric (bigint_val_p, &dbval_tmp);

      if (numeric_db_value_sub (&dbval_tmp, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_SUBTRACTION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_MONETARY:
      return qdata_subtract_monetary ((double) bi,
				      (DB_GET_MONETARY (dbval_p))->amount,
				      (DB_GET_MONETARY (dbval_p))->type,
				      result_p);

    case DB_TYPE_TIME:
      if (bi < 0)
	{
	  bi = (bi % SECONDS_OF_ONE_DAY) + SECONDS_OF_ONE_DAY;
	}
      else
	{
	  bi %= SECONDS_OF_ONE_DAY;
	}
      timeval = DB_GET_TIME (dbval_p);
      return qdata_subtract_time ((DB_TIME) bi,
				  (DB_TIME) (*timeval % SECONDS_OF_ONE_DAY),
				  result_p);

    case DB_TYPE_UTIME:
      utime = DB_GET_UTIME (dbval_p);
      return qdata_subtract_utime ((DB_UTIME) bi, *utime, result_p);

    case DB_TYPE_DATE:
      date = DB_GET_DATE (dbval_p);

      u1 = (unsigned int) bi;
      u2 = (unsigned int) *date;
      utmp = u1 - u2;

      if (bi < 0 || OR_CHECK_UNS_SUB_UNDERFLOW (u1, u2, utmp))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_DATE_UNDERFLOW,
		  0);
	  return ER_FAILED;
	}

      db_date_decode (&utmp, &month, &day, &year);
      DB_MAKE_DATE (result_p, month, day, year);
      break;

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_float_to_dbval (DB_VALUE * float_val_p, DB_VALUE * dbval_p,
			       DB_VALUE * result_p)
{
  float f;
  DB_TYPE type;

  f = DB_GET_FLOAT (float_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_subtract_float (f, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_subtract_float (f, (float) DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_subtract_float (f, (float) DB_GET_BIGINT (dbval_p),
				   result_p);

    case DB_TYPE_FLOAT:
      return qdata_subtract_float (f, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_subtract_double (f, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_subtract_double (f,
				    qdata_coerce_numeric_to_double (dbval_p),
				    result_p);

    case DB_TYPE_MONETARY:
      return qdata_subtract_monetary (f, (DB_GET_MONETARY (dbval_p))->amount,
				      (DB_GET_MONETARY (dbval_p))->type,
				      result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_double_to_dbval (DB_VALUE * double_val_p, DB_VALUE * dbval_p,
				DB_VALUE * result_p)
{
  double d;
  DB_TYPE type;

  d = DB_GET_DOUBLE (double_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_subtract_double (d, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_subtract_double (d, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_subtract_double (d, (double) DB_GET_BIGINT (dbval_p),
				    result_p);

    case DB_TYPE_FLOAT:
      return qdata_subtract_double (d, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_subtract_double (d, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_subtract_double (d,
				    qdata_coerce_numeric_to_double (dbval_p),
				    result_p);

    case DB_TYPE_MONETARY:
      return qdata_subtract_monetary (d, (DB_GET_MONETARY (dbval_p))->amount,
				      (DB_GET_MONETARY (dbval_p))->type,
				      result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_numeric_to_dbval (DB_VALUE * numeric_val_p,
				 DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  DB_TYPE type;
  DB_VALUE dbval_tmp;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
      qdata_coerce_dbval_to_numeric (dbval_p, &dbval_tmp);

      if (numeric_db_value_sub (numeric_val_p, &dbval_tmp, result_p) !=
	  NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_SUBTRACTION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_NUMERIC:
      if (numeric_db_value_sub (numeric_val_p, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_SUBTRACTION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_FLOAT:
      return
	qdata_subtract_double (qdata_coerce_numeric_to_double (numeric_val_p),
			       DB_GET_FLOAT (dbval_p), result_p);
      break;

    case DB_TYPE_DOUBLE:
      return
	qdata_subtract_double (qdata_coerce_numeric_to_double (numeric_val_p),
			       DB_GET_DOUBLE (dbval_p), result_p);
      break;

    case DB_TYPE_MONETARY:
      return
	qdata_subtract_monetary (qdata_coerce_numeric_to_double
				 (numeric_val_p),
				 (DB_GET_MONETARY (dbval_p))->amount,
				 (DB_GET_MONETARY (dbval_p))->type, result_p);
      break;

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_monetary_to_dbval (DB_VALUE * monetary_val_p,
				  DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  double d;
  DB_CURRENCY currency;
  DB_TYPE type;

  d = (DB_GET_MONETARY (monetary_val_p))->amount;
  currency = (DB_GET_MONETARY (monetary_val_p))->type;
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return qdata_subtract_monetary (d, DB_GET_SHORT (dbval_p), currency,
				      result_p);

    case DB_TYPE_INTEGER:
      return qdata_subtract_monetary (d, DB_GET_INT (dbval_p), currency,
				      result_p);

    case DB_TYPE_BIGINT:
      return qdata_subtract_monetary (d, (double) DB_GET_BIGINT (dbval_p),
				      currency, result_p);

    case DB_TYPE_FLOAT:
      return qdata_subtract_monetary (d, DB_GET_FLOAT (dbval_p), currency,
				      result_p);

    case DB_TYPE_DOUBLE:
      return qdata_subtract_monetary (d, DB_GET_DOUBLE (dbval_p), currency,
				      result_p);

    case DB_TYPE_NUMERIC:
      return qdata_subtract_monetary (d,
				      qdata_coerce_numeric_to_double
				      (dbval_p), currency, result_p);

    case DB_TYPE_MONETARY:
      /* Note: we probably should return an error if the two monetaries
       * have different monetary types. */
      return qdata_subtract_monetary (d, (DB_GET_MONETARY (dbval_p))->amount,
				      currency, result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_sequence_to_dbval (DB_VALUE * seq_val_p, DB_VALUE * dbval_p,
				  DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_SET *set_tmp;

  if (domain_p == NULL)
    {
      return ER_FAILED;
    }

  if (set_difference (DB_GET_SET (seq_val_p), DB_GET_SET (dbval_p),
		      &set_tmp, domain_p) < 0)
    {
      return ER_FAILED;
    }

  set_make_collection (result_p, set_tmp);
  return NO_ERROR;
}

static int
qdata_subtract_time_to_dbval (DB_VALUE * time_val_p, DB_VALUE * dbval_p,
			      DB_VALUE * result_p)
{
  DB_TYPE type;
  DB_TIME *timeval, *timeval1;
  int subval;

  timeval = DB_GET_TIME (time_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      subval = (int) DB_GET_SHORT (dbval_p);
      if (subval < 0)
	{
	  return qdata_add_bigint_to_time (time_val_p, (DB_BIGINT) (-subval),
					   result_p);
	}
      return qdata_subtract_time ((DB_TIME) (*timeval % SECONDS_OF_ONE_DAY),
				  (DB_TIME) subval, result_p);

    case DB_TYPE_INTEGER:
      subval = (int) (DB_GET_INT (dbval_p) % SECONDS_OF_ONE_DAY);
      if (subval < 0)
	{
	  return qdata_add_bigint_to_time (time_val_p, (DB_BIGINT) (-subval),
					   result_p);
	}
      return qdata_subtract_time ((DB_TIME) (*timeval % SECONDS_OF_ONE_DAY),
				  (DB_TIME) subval, result_p);

    case DB_TYPE_BIGINT:
      subval = (int) (DB_GET_BIGINT (dbval_p) % SECONDS_OF_ONE_DAY);
      if (subval < 0)
	{
	  return qdata_add_bigint_to_time (time_val_p, (DB_BIGINT) (-subval),
					   result_p);
	}
      return qdata_subtract_time ((DB_TIME) (*timeval % SECONDS_OF_ONE_DAY),
				  (DB_TIME) subval, result_p);

    case DB_TYPE_TIME:
      timeval1 = DB_GET_TIME (dbval_p);
      DB_MAKE_INT (result_p, ((int) *timeval - (int) *timeval1));
      break;

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_utime_to_dbval (DB_VALUE * utime_val_p, DB_VALUE * dbval_p,
			       DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type;
  DB_UTIME *utime, *utime1;
  DB_DATETIME *datetime;
  DB_DATETIME tmp_datetime;
  unsigned int u1;
  short s2;
  int i2;
  DB_BIGINT bi2;

  utime = DB_GET_UTIME (utime_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      u1 = (unsigned int) *utime;
      s2 = DB_GET_SHORT (dbval_p);
      if (s2 < 0)
	{
	  /* We're really adding.  */
	  return qdata_subtract_utime_to_short_asymmetry (utime_val_p, s2,
							  utime, result_p,
							  domain_p);
	}

      return qdata_subtract_utime (*utime, (DB_UTIME) s2, result_p);

    case DB_TYPE_INTEGER:
      u1 = (unsigned int) *utime;
      i2 = DB_GET_INT (dbval_p);
      if (i2 < 0)
	{
	  /* We're really adding.  */
	  return qdata_subtract_utime_to_int_asymmetry (utime_val_p, i2,
							utime, result_p,
							domain_p);
	}

      return qdata_subtract_utime (*utime, (DB_UTIME) i2, result_p);

    case DB_TYPE_BIGINT:
      u1 = (unsigned int) *utime;
      bi2 = DB_GET_BIGINT (dbval_p);
      if (bi2 < 0)
	{
	  /* We're really adding. */
	  return qdata_subtract_utime_to_bigint_asymmetry (utime_val_p, bi2,
							   utime, result_p,
							   domain_p);
	}

      return qdata_subtract_utime (*utime, (DB_UTIME) bi2, result_p);

    case DB_TYPE_UTIME:
      utime1 = DB_GET_UTIME (dbval_p);
      DB_MAKE_INT (result_p, ((int) *utime - (int) *utime1));
      break;

    case DB_TYPE_DATETIME:
      datetime = DB_GET_DATETIME (dbval_p);
      db_timestamp_to_datetime (utime, &tmp_datetime);

      return qdata_subtract_datetime (&tmp_datetime, datetime, result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_datetime_to_dbval (DB_VALUE * datetime_val_p,
				  DB_VALUE * dbval_p, DB_VALUE * result_p,
				  TP_DOMAIN * domain_p)
{
  DB_TYPE type;
  DB_DATETIME *datetime1_p;

  datetime1_p = DB_GET_DATETIME (datetime_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      {
	short s2;
	s2 = DB_GET_SHORT (dbval_p);
	if (s2 < 0)
	  {
	    /* We're really adding.  */
	    return qdata_subtract_datetime_to_int_asymmetry (datetime_val_p,
							     s2, datetime1_p,
							     result_p,
							     domain_p);
	  }

	return qdata_subtract_datetime_to_int (datetime1_p, s2, result_p);
      }

    case DB_TYPE_INTEGER:
      {
	int i2;
	i2 = DB_GET_INT (dbval_p);
	if (i2 < 0)
	  {
	    /* We're really adding.  */
	    return qdata_subtract_datetime_to_int_asymmetry (datetime_val_p,
							     i2, datetime1_p,
							     result_p,
							     domain_p);
	  }

	return qdata_subtract_datetime_to_int (datetime1_p, i2, result_p);
      }

    case DB_TYPE_BIGINT:
      {
	DB_BIGINT bi2;

	bi2 = DB_GET_BIGINT (dbval_p);
	if (bi2 < 0)
	  {
	    /* We're really adding.  */
	    return qdata_subtract_datetime_to_int_asymmetry (datetime_val_p,
							     bi2, datetime1_p,
							     result_p,
							     domain_p);
	  }

	return qdata_subtract_datetime_to_int (datetime1_p, bi2, result_p);
      }

    case DB_TYPE_UTIME:
      {
	DB_BIGINT u1, u2;
	DB_DATETIME datetime2;

	db_timestamp_to_datetime (DB_GET_UTIME (dbval_p), &datetime2);

	u1 = ((DB_BIGINT) datetime1_p->date) * MILLISECONDS_OF_ONE_DAY
	  + datetime1_p->time;
	u2 = ((DB_BIGINT) datetime2.date) * MILLISECONDS_OF_ONE_DAY
	  + datetime2.time;

	return db_make_bigint (result_p, u1 - u2);
      }

    case DB_TYPE_DATETIME:
      {
	DB_BIGINT u1, u2;
	DB_DATETIME *datetime2_p;

	datetime2_p = DB_GET_DATETIME (dbval_p);

	u1 = ((DB_BIGINT) datetime1_p->date) * MILLISECONDS_OF_ONE_DAY
	  + datetime1_p->time;
	u2 = ((DB_BIGINT) datetime2_p->date) * MILLISECONDS_OF_ONE_DAY
	  + datetime2_p->time;

	return db_make_bigint (result_p, u1 - u2);
      }

    case DB_TYPE_DATE:
      {
	DB_BIGINT u1, u2;

	u1 = ((DB_BIGINT) datetime1_p->date) * MILLISECONDS_OF_ONE_DAY
	  + datetime1_p->time;
	u2 = ((DB_BIGINT) * DB_GET_DATE (dbval_p)) * MILLISECONDS_OF_ONE_DAY;

	return db_make_bigint (result_p, u1 - u2);
      }

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_date_to_dbval (DB_VALUE * date_val_p, DB_VALUE * dbval_p,
			      DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type;
  DB_DATE *date, *date1;
  unsigned int u1, u2, utmp;
  short s2;
  int i2;
  DB_BIGINT bi1, bi2, bitmp;
  int day, month, year;

  date = DB_GET_DATE (date_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      u1 = (unsigned int) *date;
      s2 = DB_GET_SHORT (dbval_p);

      if (s2 < 0)
	{
	  /* We're really adding.  */
	  return qdata_subtract_utime_to_short_asymmetry (date_val_p, s2,
							  date, result_p,
							  domain_p);
	}

      u2 = (unsigned int) s2;
      utmp = u1 - u2;
      if (OR_CHECK_UNS_SUB_UNDERFLOW (u1, u2, utmp) || utmp < DB_DATE_MIN)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_DATE_UNDERFLOW,
		  0);
	  return ER_QPROC_DATE_UNDERFLOW;
	}

      db_date_decode (&utmp, &month, &day, &year);
      DB_MAKE_DATE (result_p, month, day, year);
      break;

    case DB_TYPE_BIGINT:
      bi1 = (DB_BIGINT) * date;
      bi2 = DB_GET_BIGINT (dbval_p);

      if (bi2 < 0)
	{
	  /* We're really adding.  */
	  return qdata_subtract_utime_to_bigint_asymmetry (date_val_p, bi2,
							   date, result_p,
							   domain_p);
	}

      bitmp = bi1 - bi2;
      if (OR_CHECK_SUB_UNDERFLOW (bi1, bi2, bitmp)
	  || OR_CHECK_UINT_OVERFLOW (bitmp) || bitmp < DB_DATE_MIN)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_DATE_UNDERFLOW,
		  0);
	  return ER_FAILED;
	}

      utmp = (unsigned int) bitmp;
      db_date_decode (&utmp, &month, &day, &year);
      DB_MAKE_DATE (result_p, month, day, year);
      break;

    case DB_TYPE_INTEGER:
      u1 = (unsigned int) *date;
      i2 = DB_GET_INT (dbval_p);

      if (i2 < 0)
	{
	  /* We're really adding.  */
	  return qdata_subtract_utime_to_int_asymmetry (date_val_p, i2, date,
							result_p, domain_p);
	}

      u2 = (unsigned int) i2;
      utmp = u1 - u2;
      if (OR_CHECK_UNS_SUB_UNDERFLOW (u1, u2, utmp) || utmp < DB_DATE_MIN)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_DATE_UNDERFLOW,
		  0);
	  return ER_QPROC_DATE_UNDERFLOW;
	}

      db_date_decode (&utmp, &month, &day, &year);
      DB_MAKE_DATE (result_p, month, day, year);
      break;

    case DB_TYPE_DATE:
      date1 = DB_GET_DATE (dbval_p);
      DB_MAKE_INT (result_p, (int) *date - (int) *date1);
      break;

    default:
      break;
    }

  return NO_ERROR;
}

/*
 * qdata_subtract_dbval () -
 *   return: NO_ERROR, or ER_code
 *   dbval1(in) : First db_value node
 *   dbval2(in) : Second db_value node
 *   res(out    : Resultant db_value node
 *   domain(in) :
 *
 * Note: Subtract dbval2 value from dbval1 value.
 * Overflow checks are only done when both operand maximums have
 * overlapping precision/scale.  That is,
 *     short - integer -> overflow is checked
 *     float - double  -> overflow is not checked.  Maximum float
 *                        value does not overlap maximum double
 *                        precision/scale.
 *                        MAX_FLT - MAX_DBL = -MAX_DBL
 */
int
qdata_subtract_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		      DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type1;
  int error = NO_ERROR;

  if ((domain_p != NULL && domain_p->type->id == DB_TYPE_NULL)
      || DB_IS_NULL (dbval1_p) || DB_IS_NULL (dbval2_p))
    {
      return NO_ERROR;
    }

  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);

  switch (type1)
    {
    case DB_TYPE_SHORT:
      error = qdata_subtract_short_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_BIGINT:
      error = qdata_subtract_bigint_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_INTEGER:
      error = qdata_subtract_int_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_FLOAT:
      error = qdata_subtract_float_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_DOUBLE:
      error = qdata_subtract_double_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_NUMERIC:
      error = qdata_subtract_numeric_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_MONETARY:
      error = qdata_subtract_monetary_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
    case DB_TYPE_SET:
      error = qdata_subtract_sequence_to_dbval (dbval1_p, dbval2_p, result_p,
						domain_p);
      break;

    case DB_TYPE_TIME:
      error = qdata_subtract_time_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_UTIME:
      error = qdata_subtract_utime_to_dbval (dbval1_p, dbval2_p, result_p,
					     domain_p);
      break;

    case DB_TYPE_DATETIME:
      error = qdata_subtract_datetime_to_dbval (dbval1_p, dbval2_p, result_p,
						domain_p);
      break;

    case DB_TYPE_DATE:
      error = qdata_subtract_date_to_dbval (dbval1_p, dbval2_p, result_p,
					    domain_p);
      break;

    case DB_TYPE_STRING:
    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      return ER_QPROC_INVALID_DATATYPE;
    }

  if (error != NO_ERROR)
    {
      return error;
    }

  return qdata_coerce_result_to_domain (result_p, domain_p);
}

static int
qdata_multiply_short (DB_VALUE * short_val_p, short s2, DB_VALUE * result_p)
{
  short s1, stmp;

  s1 = DB_GET_SHORT (short_val_p);
  stmp = s1 * s2;

  if (OR_CHECK_MULT_OVERFLOW (s1, s2, stmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  DB_MAKE_SHORT (result_p, stmp);

  return NO_ERROR;
}

static int
qdata_multiply_int (DB_VALUE * int_val_p, int i2, DB_VALUE * result_p)
{
  int i1, itmp;

  i1 = DB_GET_INT (int_val_p);
  itmp = i1 * i2;

  if (OR_CHECK_MULT_OVERFLOW (i1, i2, itmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  DB_MAKE_INT (result_p, itmp);
  return NO_ERROR;
}

static int
qdata_multiply_bigint (DB_VALUE * bigint_val_p, DB_BIGINT bi2,
		       DB_VALUE * result_p)
{
  DB_BIGINT bi1, bitmp;

  bi1 = DB_GET_BIGINT (bigint_val_p);
  bitmp = bi1 * bi2;

  if (OR_CHECK_MULT_OVERFLOW (bi1, bi2, bitmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  DB_MAKE_BIGINT (result_p, bitmp);
  return NO_ERROR;
}

static int
qdata_multiply_float (DB_VALUE * float_val_p, float f2, DB_VALUE * result_p)
{
  float f1, ftmp;

  f1 = DB_GET_FLOAT (float_val_p);
  ftmp = f1 * f2;

  if (OR_CHECK_FLOAT_OVERFLOW (ftmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  DB_MAKE_FLOAT (result_p, ftmp);
  return NO_ERROR;
}

static int
qdata_multiply_double (double d1, double d2, DB_VALUE * result_p)
{
  double dtmp;

  dtmp = d1 * d2;

  if (OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  DB_MAKE_DOUBLE (result_p, dtmp);
  return NO_ERROR;
}

static int
qdata_multiply_numeric (DB_VALUE * numeric_val_p, DB_VALUE * dbval,
			DB_VALUE * result_p)
{
  DB_VALUE dbval_tmp;

  qdata_coerce_dbval_to_numeric (dbval, &dbval_tmp);

  if (numeric_db_value_mul (numeric_val_p, &dbval_tmp, result_p) != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
qdata_multiply_monetary (DB_VALUE * monetary_val_p, double d,
			 DB_VALUE * result_p)
{
  double dtmp;

  dtmp = (DB_GET_MONETARY (monetary_val_p))->amount * d;

  if (OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  DB_MAKE_MONETARY_TYPE_AMOUNT (result_p,
				(DB_GET_MONETARY (monetary_val_p))->type,
				dtmp);

  return NO_ERROR;
}

static int
qdata_multiply_short_to_dbval (DB_VALUE * short_val_p, DB_VALUE * dbval_p,
			       DB_VALUE * result_p)
{
  short s;
  DB_TYPE type2;

  s = DB_GET_SHORT (short_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_multiply_short (dbval_p, s, result_p);

    case DB_TYPE_BIGINT:
      return qdata_multiply_bigint (dbval_p, s, result_p);

    case DB_TYPE_INTEGER:
      return qdata_multiply_int (dbval_p, s, result_p);

    case DB_TYPE_FLOAT:
      return qdata_multiply_float (dbval_p, s, result_p);

    case DB_TYPE_DOUBLE:
      return qdata_multiply_double (DB_GET_DOUBLE (dbval_p), s, result_p);

    case DB_TYPE_NUMERIC:
      return qdata_multiply_numeric (dbval_p, short_val_p, result_p);

    case DB_TYPE_MONETARY:
      return qdata_multiply_monetary (dbval_p, s, result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_int_to_dbval (DB_VALUE * int_val_p, DB_VALUE * dbval_p,
			     DB_VALUE * result_p)
{
  DB_TYPE type2;

  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_multiply_int (int_val_p, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_multiply_int (int_val_p, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_multiply_bigint (dbval_p, DB_GET_INT (int_val_p),
				    result_p);

    case DB_TYPE_FLOAT:
      return qdata_multiply_float (dbval_p, (float) DB_GET_INT (int_val_p),
				   result_p);

    case DB_TYPE_DOUBLE:
      return qdata_multiply_double (DB_GET_DOUBLE (dbval_p),
				    DB_GET_INT (int_val_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_multiply_numeric (dbval_p, int_val_p, result_p);

    case DB_TYPE_MONETARY:
      return qdata_multiply_monetary (dbval_p, DB_GET_INT (int_val_p),
				      result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_bigint_to_dbval (DB_VALUE * bigint_val_p, DB_VALUE * dbval_p,
				DB_VALUE * result_p)
{
  DB_TYPE type2;

  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_multiply_bigint (bigint_val_p, DB_GET_SHORT (dbval_p),
				    result_p);

    case DB_TYPE_INTEGER:
      return qdata_multiply_bigint (bigint_val_p, DB_GET_INT (dbval_p),
				    result_p);

    case DB_TYPE_BIGINT:
      return qdata_multiply_bigint (bigint_val_p, DB_GET_BIGINT (dbval_p),
				    result_p);

    case DB_TYPE_FLOAT:
      return qdata_multiply_float (dbval_p,
				   (float) DB_GET_BIGINT (bigint_val_p),
				   result_p);

    case DB_TYPE_DOUBLE:
      return qdata_multiply_double (DB_GET_DOUBLE (dbval_p),
				    (double) DB_GET_BIGINT (bigint_val_p),
				    result_p);

    case DB_TYPE_NUMERIC:
      return qdata_multiply_numeric (dbval_p, bigint_val_p, result_p);

    case DB_TYPE_MONETARY:
      return qdata_multiply_monetary (dbval_p,
				      (double) DB_GET_BIGINT (bigint_val_p),
				      result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_float_to_dbval (DB_VALUE * float_val_p, DB_VALUE * dbval_p,
			       DB_VALUE * result_p)
{
  DB_TYPE type2;

  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_multiply_float (float_val_p, DB_GET_SHORT (dbval_p),
				   result_p);

    case DB_TYPE_INTEGER:
      return qdata_multiply_float (float_val_p, (float) DB_GET_INT (dbval_p),
				   result_p);

    case DB_TYPE_BIGINT:
      return qdata_multiply_float (float_val_p,
				   (float) DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_FLOAT:
      return qdata_multiply_float (float_val_p, DB_GET_FLOAT (dbval_p),
				   result_p);

    case DB_TYPE_DOUBLE:
      return qdata_multiply_double (DB_GET_FLOAT (float_val_p),
				    DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_multiply_double (DB_GET_FLOAT (float_val_p),
				    qdata_coerce_numeric_to_double (dbval_p),
				    result_p);

    case DB_TYPE_MONETARY:
      return qdata_multiply_monetary (dbval_p, DB_GET_FLOAT (float_val_p),
				      result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_double_to_dbval (DB_VALUE * double_val_p, DB_VALUE * dbval_p,
				DB_VALUE * result_p)
{
  double d;
  DB_TYPE type2;

  d = DB_GET_DOUBLE (double_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)

    {
    case DB_TYPE_SHORT:
      return qdata_multiply_double (d, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_multiply_double (d, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_multiply_double (d, (double) DB_GET_BIGINT (dbval_p),
				    result_p);

    case DB_TYPE_FLOAT:
      return qdata_multiply_double (d, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_multiply_double (d, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_multiply_double (d,
				    qdata_coerce_numeric_to_double (dbval_p),
				    result_p);

    case DB_TYPE_MONETARY:
      return qdata_multiply_monetary (dbval_p, d, result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_numeric_to_dbval (DB_VALUE * numeric_val_p,
				 DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  DB_TYPE type2;

  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
      return qdata_multiply_numeric (numeric_val_p, dbval_p, result_p);

    case DB_TYPE_NUMERIC:
      if (numeric_db_value_mul (numeric_val_p, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_FLOAT:
      return
	qdata_multiply_double (qdata_coerce_numeric_to_double (numeric_val_p),
			       DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return
	qdata_multiply_double (qdata_coerce_numeric_to_double (numeric_val_p),
			       DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_MONETARY:
      return qdata_multiply_monetary (dbval_p,
				      qdata_coerce_numeric_to_double
				      (numeric_val_p), result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_monetary_to_dbval (DB_VALUE * monetary_val_p,
				  DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  DB_TYPE type2;

  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_multiply_monetary (monetary_val_p, DB_GET_SHORT (dbval_p),
				      result_p);

    case DB_TYPE_INTEGER:
      return qdata_multiply_monetary (monetary_val_p, DB_GET_INT (dbval_p),
				      result_p);

    case DB_TYPE_BIGINT:
      return qdata_multiply_monetary (monetary_val_p,
				      (double) DB_GET_BIGINT (dbval_p),
				      result_p);

    case DB_TYPE_FLOAT:
      return qdata_multiply_monetary (monetary_val_p, DB_GET_FLOAT (dbval_p),
				      result_p);

    case DB_TYPE_DOUBLE:
      return qdata_multiply_monetary (monetary_val_p, DB_GET_DOUBLE (dbval_p),
				      result_p);

    case DB_TYPE_NUMERIC:
      return qdata_multiply_monetary (monetary_val_p,
				      qdata_coerce_numeric_to_double
				      (dbval_p), result_p);

    case DB_TYPE_MONETARY:
      /* Note: we probably should return an error if the two monetaries
       * have different montetary types.
       */
      return qdata_multiply_monetary (monetary_val_p,
				      (DB_GET_MONETARY (dbval_p))->amount,
				      result_p);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_sequence_to_dbval (DB_VALUE * seq_val_p, DB_VALUE * dbval_p,
				  DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_SET *set_tmp = NULL;

  if (set_intersection (DB_GET_SET (seq_val_p),
			DB_GET_SET (dbval_p), &set_tmp, domain_p) < 0)
    {
      return ER_FAILED;
    }

  set_make_collection (result_p, set_tmp);
  return NO_ERROR;
}

/*
 * qdata_multiply_dbval () -
 *   return: NO_ERROR, or ER_code
 *   dbval1(in) : First db_value node
 *   dbval2(in) : Second db_value node
 *   res(out)   : Resultant db_value node
 *   domain(in) :
 *
 * Note: Multiply two db_values.
 */
int
qdata_multiply_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		      DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type1;
  int error = NO_ERROR;

  if (domain_p->type->id == DB_TYPE_NULL
      || DB_IS_NULL (dbval1_p) || DB_IS_NULL (dbval2_p))
    {
      return NO_ERROR;
    }

  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);

  switch (type1)
    {
    case DB_TYPE_SHORT:
      error = qdata_multiply_short_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_INTEGER:
      error = qdata_multiply_int_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_BIGINT:
      error = qdata_multiply_bigint_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_FLOAT:
      error = qdata_multiply_float_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_DOUBLE:
      error = qdata_multiply_double_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_NUMERIC:
      error = qdata_multiply_numeric_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_MONETARY:
      error = qdata_multiply_monetary_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_SET:
    case DB_TYPE_SEQUENCE:
    case DB_TYPE_MULTISET:
      error = qdata_multiply_sequence_to_dbval (dbval1_p, dbval2_p, result_p,
						domain_p);
      break;

    case DB_TYPE_TIME:
    case DB_TYPE_UTIME:
    case DB_TYPE_DATE:
    case DB_TYPE_DATETIME:
    case DB_TYPE_STRING:
    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      return ER_FAILED;
    }

  if (error != NO_ERROR)
    {
      return error;
    }

  return qdata_coerce_result_to_domain (result_p, domain_p);
}

static bool
qdata_is_divided_zero (DB_VALUE * dbval_p)
{
  DB_TYPE type;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_SHORT:
      return DB_GET_SHORT (dbval_p) == 0;

    case DB_TYPE_INTEGER:
      return DB_GET_INT (dbval_p) == 0;

    case DB_TYPE_BIGINT:
      return DB_GET_BIGINT (dbval_p) == 0;

    case DB_TYPE_FLOAT:
      return fabs ((double) DB_GET_FLOAT (dbval_p)) <= DBL_EPSILON;

    case DB_TYPE_DOUBLE:
      return fabs (DB_GET_DOUBLE (dbval_p)) <= DBL_EPSILON;

    case DB_TYPE_MONETARY:
      return DB_GET_MONETARY (dbval_p)->amount <= DBL_EPSILON;

    case DB_TYPE_NUMERIC:
      return numeric_db_value_is_zero (dbval_p);

    default:
      break;
    }

  return false;
}

static int
qdata_divide_short (short s1, short s2, DB_VALUE * result_p)
{
  short stmp;

  stmp = s1 / s2;
  DB_MAKE_SHORT (result_p, stmp);

  return NO_ERROR;
}

static int
qdata_divide_int (int i1, int i2, DB_VALUE * result_p)
{
  int itmp;

  itmp = i1 / i2;
  DB_MAKE_INT (result_p, itmp);

  return NO_ERROR;
}

static int
qdata_divide_bigint (DB_BIGINT bi1, DB_BIGINT bi2, DB_VALUE * result_p)
{
  DB_BIGINT bitmp;

  bitmp = bi1 / bi2;
  DB_MAKE_BIGINT (result_p, bitmp);

  return NO_ERROR;
}

static int
qdata_divide_float (float f1, float f2, DB_VALUE * result_p)
{
  float ftmp;

  ftmp = f1 / f2;

  if (OR_CHECK_FLOAT_OVERFLOW (ftmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_DIVISION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_FLOAT (result_p, ftmp);
  return NO_ERROR;
}

static int
qdata_divide_double (double d1, double d2, DB_VALUE * result_p,
		     bool is_check_overflow)
{
  double dtmp;

  dtmp = d1 / d2;

  if (is_check_overflow && OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_DIVISION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_DOUBLE (result_p, dtmp);
  return NO_ERROR;
}

static int
qdata_divide_monetary (double d1, double d2, DB_CURRENCY currency,
		       DB_VALUE * result_p, bool is_check_overflow)
{
  double dtmp;

  dtmp = d1 / d2;

  if (is_check_overflow && OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_DIVISION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_MONETARY_TYPE_AMOUNT (result_p, currency, dtmp);
  return NO_ERROR;
}

static int
qdata_divide_short_to_dbval (DB_VALUE * short_val_p, DB_VALUE * dbval_p,
			     DB_VALUE * result_p)
{
  short s;
  DB_TYPE type2;
  DB_VALUE dbval_tmp;

  s = DB_GET_SHORT (short_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_divide_short (s, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_divide_int (s, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_divide_bigint (s, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_FLOAT:
      return qdata_divide_float (s, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_divide_double (s, DB_GET_DOUBLE (dbval_p), result_p, true);

    case DB_TYPE_NUMERIC:
      qdata_coerce_dbval_to_numeric (short_val_p, &dbval_tmp);
      if (numeric_db_value_div (&dbval_tmp, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_DIVISION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_MONETARY:
      return qdata_divide_monetary (s, (DB_GET_MONETARY (dbval_p))->amount,
				    (DB_GET_MONETARY (dbval_p))->type,
				    result_p, true);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_divide_int_to_dbval (DB_VALUE * int_val_p, DB_VALUE * dbval_p,
			   DB_VALUE * result_p)
{
  int i;
  DB_TYPE type2;
  DB_VALUE dbval_tmp;

  i = DB_GET_INT (int_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_divide_int (i, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_divide_int (i, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_divide_bigint (i, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_FLOAT:
      return qdata_divide_float ((float) i, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_divide_double (i, DB_GET_DOUBLE (dbval_p), result_p, true);

    case DB_TYPE_NUMERIC:
      qdata_coerce_dbval_to_numeric (int_val_p, &dbval_tmp);
      if (numeric_db_value_div (&dbval_tmp, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_DIVISION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_MONETARY:
      return qdata_divide_monetary (i, (DB_GET_MONETARY (dbval_p))->amount,
				    (DB_GET_MONETARY (dbval_p))->type,
				    result_p, true);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_divide_bigint_to_dbval (DB_VALUE * bigint_val_p, DB_VALUE * dbval_p,
			      DB_VALUE * result_p)
{
  DB_BIGINT bi;
  DB_TYPE type2;
  DB_VALUE dbval_tmp;

  bi = DB_GET_BIGINT (bigint_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_divide_bigint (bi, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_divide_bigint (bi, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_divide_bigint (bi, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_FLOAT:
      return qdata_divide_float ((float) bi, DB_GET_FLOAT (dbval_p),
				 result_p);

    case DB_TYPE_DOUBLE:
      return qdata_divide_double ((double) bi, DB_GET_DOUBLE (dbval_p),
				  result_p, true);

    case DB_TYPE_NUMERIC:
      qdata_coerce_dbval_to_numeric (bigint_val_p, &dbval_tmp);
      if (numeric_db_value_div (&dbval_tmp, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_DIVISION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_MONETARY:
      return qdata_divide_monetary ((double) bi,
				    (DB_GET_MONETARY (dbval_p))->amount,
				    (DB_GET_MONETARY (dbval_p))->type,
				    result_p, true);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_divide_float_to_dbval (DB_VALUE * float_val_p, DB_VALUE * dbval_p,
			     DB_VALUE * result_p)
{
  float f;
  DB_TYPE type2;

  f = DB_GET_FLOAT (float_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_divide_float (f, DB_GET_SHORT (dbval_p), result_p);

    case DB_TYPE_INTEGER:
      return qdata_divide_float (f, (float) DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_divide_float (f, (float) DB_GET_BIGINT (dbval_p),
				 result_p);

    case DB_TYPE_FLOAT:
      return qdata_divide_float (f, DB_GET_FLOAT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_divide_double (f, DB_GET_DOUBLE (dbval_p), result_p, true);

    case DB_TYPE_NUMERIC:
      return qdata_divide_double (f, qdata_coerce_numeric_to_double (dbval_p),
				  result_p, false);

    case DB_TYPE_MONETARY:
      return qdata_divide_monetary (f, (DB_GET_MONETARY (dbval_p))->amount,
				    (DB_GET_MONETARY (dbval_p))->type,
				    result_p, true);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_divide_double_to_dbval (DB_VALUE * double_val_p, DB_VALUE * dbval_p,
			      DB_VALUE * result_p)
{
  double d;
  DB_TYPE type2;

  d = DB_GET_DOUBLE (double_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_divide_double (d, DB_GET_SHORT (dbval_p), result_p, false);

    case DB_TYPE_INTEGER:
      return qdata_divide_double (d, DB_GET_INT (dbval_p), result_p, false);

    case DB_TYPE_BIGINT:
      return qdata_divide_double (d, (double) DB_GET_BIGINT (dbval_p),
				  result_p, false);

    case DB_TYPE_FLOAT:
      return qdata_divide_double (d, DB_GET_FLOAT (dbval_p), result_p, true);

    case DB_TYPE_DOUBLE:
      return qdata_divide_double (d, DB_GET_DOUBLE (dbval_p), result_p, true);

    case DB_TYPE_NUMERIC:
      return qdata_divide_double (d, qdata_coerce_numeric_to_double (dbval_p),
				  result_p, false);

    case DB_TYPE_MONETARY:
      return qdata_divide_monetary (d, (DB_GET_MONETARY (dbval_p))->amount,
				    (DB_GET_MONETARY (dbval_p))->type,
				    result_p, true);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_divide_numeric_to_dbval (DB_VALUE * numeric_val_p, DB_VALUE * dbval_p,
			       DB_VALUE * result_p)
{
  DB_TYPE type2;
  DB_VALUE dbval_tmp;

  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
      qdata_coerce_dbval_to_numeric (dbval_p, &dbval_tmp);
      if (numeric_db_value_div (numeric_val_p, &dbval_tmp, result_p) !=
	  NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_DIVISION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_NUMERIC:
      if (numeric_db_value_div (numeric_val_p, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_DIVISION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_FLOAT:
      return
	qdata_divide_double (qdata_coerce_numeric_to_double (numeric_val_p),
			     DB_GET_FLOAT (dbval_p), result_p, false);

    case DB_TYPE_DOUBLE:
      return
	qdata_divide_double (qdata_coerce_numeric_to_double (numeric_val_p),
			     DB_GET_DOUBLE (dbval_p), result_p, true);

    case DB_TYPE_MONETARY:
      return
	qdata_divide_monetary (qdata_coerce_numeric_to_double (numeric_val_p),
			       (DB_GET_MONETARY (dbval_p))->amount,
			       (DB_GET_MONETARY (dbval_p))->type, result_p,
			       true);

    default:
      break;
    }

  return NO_ERROR;
}

static int
qdata_divide_monetary_to_dbval (DB_VALUE * monetary_val_p,
				DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  double d;
  DB_CURRENCY currency;
  DB_TYPE type2;

  d = (DB_GET_MONETARY (monetary_val_p))->amount;
  currency = (DB_GET_MONETARY (monetary_val_p))->type;
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_SHORT:
      return qdata_divide_monetary (d, DB_GET_SHORT (dbval_p), currency,
				    result_p, false);

    case DB_TYPE_INTEGER:
      return qdata_divide_monetary (d, DB_GET_INT (dbval_p), currency,
				    result_p, false);

    case DB_TYPE_BIGINT:
      return qdata_divide_monetary (d, (double) DB_GET_BIGINT (dbval_p),
				    currency, result_p, false);

    case DB_TYPE_FLOAT:
      return qdata_divide_monetary (d, DB_GET_FLOAT (dbval_p), currency,
				    result_p, true);

    case DB_TYPE_DOUBLE:
      return qdata_divide_monetary (d, DB_GET_DOUBLE (dbval_p), currency,
				    result_p, true);

    case DB_TYPE_NUMERIC:
      return qdata_divide_monetary (d,
				    qdata_coerce_numeric_to_double (dbval_p),
				    currency, result_p, true);

    case DB_TYPE_MONETARY:
      /* Note: we probably should return an error if the two monetaries
       * have different montetary types.
       */
      return qdata_divide_monetary (d, (DB_GET_MONETARY (dbval_p))->amount,
				    currency, result_p, true);

    default:
      break;
    }

  return NO_ERROR;
}

/*
 * qdata_divide_dbval () -
 *   return: NO_ERROR, or ER_code
 *   dbval1(in) : First db_value node
 *   dbval2(in) : Second db_value node
 *   res(out)   : Resultant db_value node
 *   domain(in) :
 *
 * Note: Divide dbval1 by dbval2
 * Overflow checks are only done when the right operand may be
 * smaller than one.  That is,
 *     short / integer -> overflow is not checked.  Result will
 *                        always be smaller than the numerand.
 *     float / short   -> overflow is not checked.  Minimum float
 *                        representation (e-38) overflows to zero
 *                        which we want.
 *     Because of zero divide checks, most of the others will not
 *     overflow but is still being checked in case we are on a
 *     platform where DBL_EPSILON approaches the value of FLT_MIN.
 */
int
qdata_divide_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		    DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type1;
  int error = NO_ERROR;

  if (domain_p->type->id == DB_TYPE_NULL
      || DB_IS_NULL (dbval1_p) || DB_IS_NULL (dbval2_p))
    {
      return NO_ERROR;
    }

  if (qdata_is_divided_zero (dbval2_p))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_ZERO_DIVIDE, 0);
      return ER_FAILED;
    }

  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);

  switch (type1)
    {
    case DB_TYPE_SHORT:
      error = qdata_divide_short_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_INTEGER:
      error = qdata_divide_int_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_BIGINT:
      error = qdata_divide_bigint_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_FLOAT:
      error = qdata_divide_float_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_DOUBLE:
      error = qdata_divide_double_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_NUMERIC:
      error = qdata_divide_numeric_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_MONETARY:
      error = qdata_divide_monetary_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_SET:
    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
    case DB_TYPE_TIME:
    case DB_TYPE_UTIME:
    case DB_TYPE_DATETIME:
    case DB_TYPE_DATE:
    case DB_TYPE_STRING:
    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      return ER_FAILED;
    }

  if (error != NO_ERROR)
    {
      return error;
    }

  return qdata_coerce_result_to_domain (result_p, domain_p);
}

/*
 * qdata_unary_minus_dbval () -
 *   return: NO_ERROR, or ER_code
 *   res(out)   : Resultant db_value node
 *   dbval1(in) : First db_value node
 *
 * Note: Take unary minus of db_value.
 */
int
qdata_unary_minus_dbval (DB_VALUE * result_p, DB_VALUE * dbval_p)
{
  DB_TYPE res_type;
  short stmp;
  int itmp;
  DB_BIGINT bitmp;
  double dtmp;

  res_type = DB_VALUE_DOMAIN_TYPE (dbval_p);
  if (res_type == DB_TYPE_NULL || DB_IS_NULL (dbval_p))
    {
      return NO_ERROR;
    }

  switch (res_type)
    {
    case DB_TYPE_INTEGER:
      itmp = DB_GET_INT (dbval_p);
      if (itmp == INT_MIN)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_UMINUS,
		  0);
	  return ER_QPROC_OVERFLOW_UMINUS;
	}
      DB_MAKE_INT (result_p, (-1) * itmp);
      break;

    case DB_TYPE_BIGINT:
      bitmp = DB_GET_BIGINT (dbval_p);
      if (bitmp == DB_BIGINT_MIN)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_UMINUS,
		  0);
	  return ER_QPROC_OVERFLOW_UMINUS;
	}
      DB_MAKE_BIGINT (result_p, (-1) * bitmp);
      break;

    case DB_TYPE_FLOAT:
      DB_MAKE_FLOAT (result_p, (-1) * DB_GET_FLOAT (dbval_p));
      break;

    case DB_TYPE_DOUBLE:
      DB_MAKE_DOUBLE (result_p, (-1) * DB_GET_DOUBLE (dbval_p));
      break;

    case DB_TYPE_NUMERIC:
      DB_MAKE_NUMERIC (result_p,
		       DB_GET_NUMERIC (dbval_p),
		       DB_VALUE_PRECISION (dbval_p),
		       DB_VALUE_SCALE (dbval_p));
      if (numeric_db_value_negate (result_p) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_MONETARY:
      dtmp = (-1) * (DB_GET_MONETARY (dbval_p))->amount;
      DB_MAKE_MONETARY_TYPE_AMOUNT (result_p,
				    (DB_GET_MONETARY (dbval_p))->type, dtmp);
      break;

    case DB_TYPE_SHORT:
      stmp = DB_GET_SHORT (dbval_p);
      if (stmp == SHRT_MIN)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_UMINUS,
		  0);
	  return ER_QPROC_OVERFLOW_UMINUS;
	}
      DB_MAKE_SHORT (result_p, (-1) * stmp);
      break;

    default:
      break;
    }

  return NO_ERROR;
}

/*
 * qdata_extract_dbval () -
 *   return: NO_ERROR, or ER_code
 *   extr_operand(in)   : Specifies datetime field to be extracted
 *   dbval(in)  : Extract source db_value node
 *   res(out)   : Resultant db_value node
 *   domain(in) :
 *
 * Note: Extract a datetime field from db_value.
 */
int
qdata_extract_dbval (const MISC_OPERAND extr_operand,
		     DB_VALUE * dbval_p, DB_VALUE * result_p,
		     TP_DOMAIN * domain_p)
{
  DB_TYPE dbval_type;
  DB_DATE date;
  DB_TIME time;
  DB_UTIME *utime;
  DB_DATETIME *datetime;
  int extvar[NUM_MISC_OPERANDS];

  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval_p);
  if (domain_p->type->id == DB_TYPE_NULL || DB_IS_NULL (dbval_p))
    {
      return NO_ERROR;
    }

  switch (dbval_type)
    {
    case DB_TYPE_TIME:
      time = *DB_GET_TIME (dbval_p);
      db_time_decode (&time, &extvar[HOUR], &extvar[MINUTE], &extvar[SECOND]);
      break;

    case DB_TYPE_DATE:
      date = *DB_GET_DATE (dbval_p);
      db_date_decode (&date, &extvar[MONTH], &extvar[DAY], &extvar[YEAR]);
      break;

    case DB_TYPE_UTIME:
      utime = DB_GET_UTIME (dbval_p);
      db_timestamp_decode (utime, &date, &time);

      if (extr_operand == YEAR || extr_operand == MONTH
	  || extr_operand == DAY)
	{
	  db_date_decode (&date, &extvar[MONTH], &extvar[DAY], &extvar[YEAR]);
	}
      else
	{
	  db_time_decode (&time, &extvar[HOUR], &extvar[MINUTE],
			  &extvar[SECOND]);
	}
      break;

    case DB_TYPE_DATETIME:
      datetime = DB_GET_DATETIME (dbval_p);
      db_datetime_decode (datetime, &extvar[MONTH], &extvar[DAY],
			  &extvar[YEAR], &extvar[HOUR], &extvar[MINUTE],
			  &extvar[SECOND], &extvar[MILLISECOND]);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      return ER_FAILED;
    }

  DB_MAKE_INT (result_p, extvar[extr_operand]);
  return NO_ERROR;
}

/*
 * qdata_strcat_dbval () -
 *   return:
 *   dbval1(in) :
 *   dbval2(in) :
 *   res(in)    :
 *   domain(in) :
 */
int
qdata_strcat_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		    DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type1, type2;
  int error = NO_ERROR;

  if (domain_p != NULL && domain_p->type->id == DB_TYPE_NULL)
    {
      return NO_ERROR;
    }

  type1 = dbval1_p ? DB_VALUE_DOMAIN_TYPE (dbval1_p) : DB_TYPE_NULL;
  type2 = dbval2_p ? DB_VALUE_DOMAIN_TYPE (dbval2_p) : DB_TYPE_NULL;

  if (DB_IS_NULL (dbval1_p) || DB_IS_NULL (dbval2_p))
    {
      /* ORACLE7 ServerSQL Language Reference Manual 3-4;
       * Although ORACLE treats zero-length character strings as
       * nulls, concatenating a zero-length character string with another
       * operand always results in the other operand, rather than a null.
       * However, this may not continue to be true in future versions of
       * ORACLE. To concatenate an expression that might be null, use the
       * NVL function to explicitly convert the expression to a
       * zero-length string.
       */
      if (!PRM_ORACLE_STYLE_EMPTY_STRING)
	{
	  return NO_ERROR;
	}

      if ((DB_IS_NULL (dbval1_p) && QSTR_IS_ANY_CHAR_OR_BIT (type2))
	  || (DB_IS_NULL (dbval2_p) && QSTR_IS_ANY_CHAR_OR_BIT (type1)))
	{
	  ;			/* go ahead */
	}
      else
	{
	  return NO_ERROR;
	}
    }

  switch (type1)
    {
    case DB_TYPE_SHORT:
      error = qdata_add_short_to_dbval (dbval1_p, dbval2_p, result_p,
					domain_p);
      break;

    case DB_TYPE_INTEGER:
      error = qdata_add_int_to_dbval (dbval1_p, dbval2_p, result_p, domain_p);
      break;

    case DB_TYPE_BIGINT:
      error = qdata_add_bigint_to_dbval (dbval1_p, dbval2_p, result_p,
					 domain_p);
      break;

    case DB_TYPE_FLOAT:
      error = qdata_add_float_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_DOUBLE:
      error = qdata_add_double_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_NUMERIC:
      error = qdata_add_numeric_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_MONETARY:
      error = qdata_add_monetary_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_NULL:
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
    case DB_TYPE_BIT:
    case DB_TYPE_VARBIT:
      if (dbval1_p != NULL && dbval2_p != NULL)
	{
	  error = qdata_add_chars_to_dbval (dbval1_p, dbval2_p, result_p);
	}
      break;

    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
    case DB_TYPE_SET:
      error = qdata_add_sequence_to_dbval (dbval1_p, dbval2_p, result_p,
					   domain_p);
      break;

    case DB_TYPE_TIME:
      error = qdata_add_time_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_UTIME:
      error = qdata_add_utime_to_dbval (dbval1_p, dbval2_p, result_p,
					domain_p);
      break;

    case DB_TYPE_DATETIME:
      error = qdata_add_datetime_to_dbval (dbval1_p, dbval2_p, result_p,
					   domain_p);
      break;

    case DB_TYPE_DATE:
      error = qdata_add_date_to_dbval (dbval1_p, dbval2_p, result_p,
				       domain_p);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      error = ER_FAILED;
      break;
    }

  if (error != NO_ERROR)
    {
      return error;
    }

  return qdata_coerce_result_to_domain (result_p, domain_p);
}

/*
 * Aggregate Expression Evaluation Routines
 */

static int
qdata_process_distinct (THREAD_ENTRY * thread_p, AGGREGATE_TYPE * agg_p,
			QUERY_ID query_id)
{
  QFILE_TUPLE_VALUE_TYPE_LIST type_list;
  QFILE_LIST_ID *list_id_p;

  /* since max(distinct a) == max(a), handle these without distinct
     processing */
  if (agg_p->function == PT_MAX || agg_p->function == PT_MIN)
    {
      agg_p->option = Q_ALL;
      return NO_ERROR;
    }

  type_list.type_cnt = 1;
  type_list.domp =
    (TP_DOMAIN **) db_private_alloc (thread_p, sizeof (TP_DOMAIN *));

  if (type_list.domp == NULL)
    {
      return ER_FAILED;
    }

  type_list.domp[0] = agg_p->operand.domain;
  list_id_p = qfile_open_list (thread_p, &type_list, NULL, query_id,
			       QFILE_FLAG_DISTINCT);

  if (list_id_p == NULL)
    {
      db_private_free_and_init (thread_p, type_list.domp);
      return ER_FAILED;
    }

  db_private_free_and_init (thread_p, type_list.domp);

  if (qfile_copy_list_id (agg_p->list_id, list_id_p, true) != NO_ERROR)
    {
      qfile_free_list_id (list_id_p);
      return ER_FAILED;
    }

  qfile_free_list_id (list_id_p);

  return NO_ERROR;
}

/*
 * qdata_initialize_aggregate_list () -
 *   return: NO_ERROR, or ER_code
 *   agg_list(in)       : Aggregate expression node list
 *   query_id(in)       : Associated query id
 *
 * Note: Initialize the aggregate expression list.
 */
int
qdata_initialize_aggregate_list (THREAD_ENTRY * thread_p,
				 AGGREGATE_TYPE * agg_list_p,
				 QUERY_ID query_id)
{
  AGGREGATE_TYPE *agg_p;

  for (agg_p = agg_list_p; agg_p != NULL; agg_p = agg_p->next)
    {

      /* the value of groupby_num() remains unchanged;
         it will be changed while evaluating groupby_num predicates
         against each group at 'xs_eval_grbynum_pred()' */
      if (agg_p->function == PT_GROUPBY_NUM)
	{
	  /* nothing to do with groupby_num() */
	  continue;
	}

      agg_p->curr_cnt = 0;
      if (db_value_domain_init (agg_p->value,
				DB_VALUE_DOMAIN_TYPE (agg_p->value),
				DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE)
	  != NO_ERROR)
	{
	  return ER_FAILED;
	}

      /* This set is made, because if class is empty, aggregate
       * results should return NULL, except count(*) and count
       */
      if (agg_p->function == PT_COUNT_STAR || agg_p->function == PT_COUNT)
	{
	  DB_MAKE_INT (agg_p->value, 0);
	}

      /* create temporary list file to handle distincts */
      if (agg_p->option == Q_DISTINCT)
	{
	  if (qdata_process_distinct (thread_p, agg_p, query_id) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	}
    }

  return NO_ERROR;
}

/*
 * qdata_evaluate_aggregate_list () -
 *   return: NO_ERROR, or ER_code
 *   agg_list(in): Aggregate expression node list
 *   vd(in)      : Value descriptor
 *
 * Note: Evaluate given aggregate expression list.
 */
int
qdata_evaluate_aggregate_list (THREAD_ENTRY * thread_p,
			       AGGREGATE_TYPE * agg_list_p,
			       VAL_DESCR * val_desc_p)
{
  AGGREGATE_TYPE *agg_p;
  DB_VALUE dbval, sqr_val;
  DB_VALUE *opr_dbval_p = NULL;
  PR_TYPE *pr_type_p;
  char *disk_repr_p = NULL;
  OR_BUF buf;
  int dbval_size;
  int copy_opr;
  TP_DOMAIN *tmp_domain_p = NULL;
  DB_TYPE dbval_type;

  PRIM_INIT_NULL (&dbval);
  PRIM_INIT_NULL (&sqr_val);

  for (agg_p = agg_list_p; agg_p != NULL; agg_p = agg_p->next)
    {
      if (agg_p->flag_agg_optimize)
	{
	  continue;
	}

      if (agg_p->function == PT_COUNT_STAR)
	{			/* increment and continue */
	  agg_p->curr_cnt++;
	  continue;
	}

      /*
       * the value of groupby_num() remains unchanged;
       * it will be changed while evaluating groupby_num predicates
       * against each group at 'xs_eval_grbynum_pred()'
       */
      if (agg_p->function == PT_GROUPBY_NUM)
	{
	  /* nothing to do with groupby_num() */
	  continue;
	}

      /*
       * fetch operand value. aggregate regulator variable should only
       * contain constants
       */
      if (fetch_copy_dbval (thread_p, &agg_p->operand, val_desc_p, NULL, NULL,
			    NULL, &dbval) != NO_ERROR)
	{
	  return ER_FAILED;
	}

      if (DB_IS_NULL (&dbval))	/* eliminate null values */
	{
	  continue;
	}

      /*
       * handle distincts by inserting each operand into a list file,
       * which will be distinct-ified and counted/summed/averaged
       * in qdata_finalize_aggregate_list ()
       */
      if (agg_p->option == Q_DISTINCT)
	{
	  dbval_type = DB_VALUE_DOMAIN_TYPE (&dbval);
	  pr_type_p = PR_TYPE_FROM_ID (dbval_type);

	  if (pr_type_p == NULL)
	    {
	      pr_clear_value (&dbval);
	      return ER_FAILED;
	    }

	  dbval_size = pr_writeval_disk_size (&dbval);
	  if ((dbval_size != 0)
	      && (disk_repr_p = (char *) db_private_alloc (thread_p,
							   dbval_size)))
	    {
	      OR_BUF_INIT (buf, disk_repr_p, dbval_size);
	      if ((*(pr_type_p->writeval)) (&buf, &dbval) != NO_ERROR)
		{
		  db_private_free_and_init (thread_p, disk_repr_p);
		  pr_clear_value (&dbval);
		  return ER_FAILED;
		}
	    }
	  else
	    {
	      pr_clear_value (&dbval);
	      return ER_FAILED;
	    }

	  if (qfile_add_item_to_list (thread_p, disk_repr_p,
				      dbval_size, agg_p->list_id) != NO_ERROR)
	    {
	      db_private_free_and_init (thread_p, disk_repr_p);
	      pr_clear_value (&dbval);
	      return ER_FAILED;
	    }

	  db_private_free_and_init (thread_p, disk_repr_p);
	  pr_clear_value (&dbval);
	  continue;
	}

      copy_opr = false;
      switch (agg_p->function)
	{
	case PT_MIN:
	  opr_dbval_p = &dbval;
	  if (agg_p->curr_cnt < 1
	      || (*(agg_p->domain->type->cmpval)) (agg_p->value, &dbval,
						   NULL, 0, 1, 1, NULL) > 0)
	    {
	      copy_opr = true;
	    }
	  break;

	case PT_MAX:
	  opr_dbval_p = &dbval;
	  if (agg_p->curr_cnt < 1
	      || (*(agg_p->domain->type->cmpval)) (agg_p->value, &dbval,
						   NULL, 0, 1, 1, NULL) < 0)
	    {
	      copy_opr = true;
	    }
	  break;

	case PT_AGG_BIT_AND:
	case PT_AGG_BIT_OR:
	case PT_AGG_BIT_XOR:
	  {
	    int error;
	    DB_VALUE tmp_val;
	    DB_MAKE_BIGINT (&tmp_val, (DB_BIGINT) 0);
	    copy_opr = false;

	    if (agg_p->curr_cnt < 1)
	      {
		/* init result value */
		DB_MAKE_NULL (agg_p->value);

		if (!DB_IS_NULL (&dbval))
		  {
		    if (qdata_bit_or_dbval (&tmp_val, &dbval, agg_p->value,
					    agg_p->domain) != NO_ERROR)
		      {
			return ER_FAILED;
		      }
		  }
	      }
	    else
	      {
		/* update result value */
		if (!DB_IS_NULL (&dbval))
		  {
		    if (DB_IS_NULL (agg_p->value))
		      {
			if (qdata_bit_or_dbval
			    (&tmp_val, &dbval, agg_p->value,
			     agg_p->domain) != NO_ERROR)
			  {
			    return ER_FAILED;
			  }
		      }
		    else
		      {
			if (agg_p->function == PT_AGG_BIT_AND)
			  {
			    error =
			      qdata_bit_and_dbval (agg_p->value, &dbval,
						   agg_p->value,
						   agg_p->domain);
			  }
			else if (agg_p->function == PT_AGG_BIT_OR)
			  {
			    error =
			      qdata_bit_or_dbval (agg_p->value, &dbval,
						  agg_p->value,
						  agg_p->domain);
			  }
			else
			  {
			    error =
			      qdata_bit_xor_dbval (agg_p->value, &dbval,
						   agg_p->value,
						   agg_p->domain);
			  }
			if (error != NO_ERROR)
			  {
			    return ER_FAILED;
			  }
		      }
		  }
	      }
	  }
	  break;

	case PT_AVG:
	case PT_SUM:
	  if (agg_p->curr_cnt < 1)
	    {
	      opr_dbval_p = &dbval;
	      copy_opr = true;

	      /* this type setting is necessary, it ensures that for the case
	       * average handling, which is treated like sum until final iteration,
	       * starts with the initial data type
	       */
	      if (db_value_domain_init (agg_p->value,
					DB_VALUE_DOMAIN_TYPE (opr_dbval_p),
					DB_DEFAULT_PRECISION,
					DB_DEFAULT_SCALE) != NO_ERROR)
		{
		  pr_clear_value (&dbval);
		  return ER_FAILED;
		}
	    }
	  else
	    {
	      TP_DOMAIN *result_domain;

	      result_domain = ((agg_p->domain->type->id == DB_TYPE_NUMERIC) ?
			       NULL : agg_p->domain);
	      if (qdata_add_dbval (agg_p->value, &dbval, agg_p->value,
				   result_domain) != NO_ERROR)
		{
		  pr_clear_value (&dbval);
		  return ER_FAILED;
		}
	      copy_opr = false;
	    }
	  break;

	case PT_STDDEV:
	case PT_VARIANCE:
	  copy_opr = false;
	  tmp_domain_p = NULL;
	  if (agg_p->domain->type->id == DB_TYPE_NUMERIC)
	    {
	      tmp_domain_p = tp_Domains[DB_TYPE_DOUBLE];
	      if (tp_value_coerce (&dbval, &dbval, tmp_domain_p)
		  != DOMAIN_COMPATIBLE)
		{
		  pr_clear_value (&dbval);
		  return ER_FAILED;
		}
	    }

	  if (agg_p->curr_cnt < 1)
	    {
	      opr_dbval_p = &dbval;
	      /* agg_ptr->value contains SUM(X) */
	      if (db_value_domain_init (agg_p->value,
					DB_VALUE_DOMAIN_TYPE (opr_dbval_p),
					DB_DEFAULT_PRECISION,
					DB_DEFAULT_SCALE) != NO_ERROR)
		{
		  pr_clear_value (&dbval);
		  return ER_FAILED;
		}

	      /* agg_ptr->value contains SUM(X^2) */
	      if (db_value_domain_init (agg_p->value2,
					DB_VALUE_DOMAIN_TYPE (opr_dbval_p),
					DB_DEFAULT_PRECISION,
					DB_DEFAULT_SCALE) != NO_ERROR)
		{
		  pr_clear_value (&dbval);
		  return ER_FAILED;
		}

	      /* calculate X^2 */
	      if (qdata_multiply_dbval (&dbval, &dbval, &sqr_val,
					NOT_NULL_VALUE (tmp_domain_p,
							agg_p->domain)) !=
		  NO_ERROR)
		{
		  pr_clear_value (&dbval);
		  return ER_FAILED;
		}

	      pr_clear_value (agg_p->value);
	      pr_clear_value (agg_p->value2);
	      dbval_type = DB_VALUE_DOMAIN_TYPE (agg_p->value);
	      pr_type_p = PR_TYPE_FROM_ID (dbval_type);
	      if (pr_type_p == NULL)
		{
		  pr_clear_value (&dbval);
		  return ER_FAILED;
		}

	      (*(pr_type_p->setval)) (agg_p->value, &dbval, true);
	      (*(pr_type_p->setval)) (agg_p->value2, &sqr_val, true);
	    }
	  else
	    {
	      if (qdata_multiply_dbval (&dbval, &dbval, &sqr_val,
					NOT_NULL_VALUE (tmp_domain_p,
							agg_p->domain)) !=
		  NO_ERROR)
		{
		  pr_clear_value (&dbval);
		  return ER_FAILED;
		}

	      if (qdata_add_dbval (agg_p->value, &dbval, agg_p->value,
				   NOT_NULL_VALUE (tmp_domain_p,
						   agg_p->domain)) !=
		  NO_ERROR)
		{
		  pr_clear_value (&dbval);
		  pr_clear_value (&sqr_val);
		  return ER_FAILED;
		}

	      if (qdata_add_dbval (agg_p->value2, &sqr_val, agg_p->value2,
				   NOT_NULL_VALUE (tmp_domain_p,
						   agg_p->domain)) !=
		  NO_ERROR)
		{
		  pr_clear_value (&dbval);
		  pr_clear_value (&sqr_val);
		  return ER_FAILED;
		}

	      pr_clear_value (&sqr_val);
	    }
	  break;

	case PT_COUNT:
	  if (agg_p->curr_cnt < 1)
	    {
	      DB_MAKE_INT (agg_p->value, 1);
	    }
	  else
	    {
	      DB_MAKE_INT (agg_p->value, DB_GET_INT (agg_p->value) + 1);
	    }
	  break;

	default:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE,
		  0);
	  pr_clear_value (&dbval);
	  return ER_FAILED;
	}

      if (copy_opr)
	{
	  /* copy resultant operand value to aggregate node */
	  pr_clear_value (agg_p->value);
	  dbval_type = DB_VALUE_DOMAIN_TYPE (agg_p->value);
	  pr_type_p = PR_TYPE_FROM_ID (dbval_type);
	  if (pr_type_p == NULL)
	    {
	      pr_clear_value (&dbval);
	      return ER_FAILED;
	    }

	  (*(pr_type_p->setval)) (agg_p->value, opr_dbval_p, true);
	}

      agg_p->curr_cnt++;
      pr_clear_value (&dbval);
    }

  return NO_ERROR;
}

/*
 * qdata_evaluate_aggregate_optimize () -
 *   return:
 *   agg_ptr(in)        :
 *   hfid(in)   :
 */
int
qdata_evaluate_aggregate_optimize (THREAD_ENTRY * thread_p,
				   AGGREGATE_TYPE * agg_p, HFID * hfid_p,
				   int node_id)
{
  int oid_count = 0, null_count = 0, key_count = 0;
  int flag_btree_stat_needed = true;

  if (!agg_p->flag_agg_optimize)
    {
      return ER_FAILED;
    }

  if (hfid_p->vfid.fileid < 0)
    {
      return ER_FAILED;
    }

  if ((agg_p->function == PT_MIN) || (agg_p->function == PT_MAX))
    {
      flag_btree_stat_needed = false;
    }

  if (agg_p->function == PT_COUNT_STAR)
    {
      if (BTID_IS_NULL (&agg_p->btid))
	{
	  if (heap_get_num_objects_proxy
	      (thread_p, node_id, hfid_p, &null_count, &oid_count,
	       &key_count) < 0)
	    {
	      return ER_FAILED;
	    }
	  flag_btree_stat_needed = false;
	}
    }

  if (flag_btree_stat_needed)
    {
      if (BTID_IS_NULL (&agg_p->btid))
	{
	  return ER_FAILED;
	}

      if (btree_get_unique_statistics (thread_p, &agg_p->btid, &oid_count,
				       &null_count, &key_count) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  switch (agg_p->function)
    {
    case PT_COUNT:
      if (agg_p->option == Q_ALL)
	{
	  DB_MAKE_INT (agg_p->value, oid_count - null_count);
	}
      else
	{
	  DB_MAKE_INT (agg_p->value, key_count);
	}
      break;

    case PT_COUNT_STAR:
      agg_p->curr_cnt = oid_count;
      break;

    case PT_MIN:
      if (btree_find_min_or_max_key (thread_p, &agg_p->btid,
				     agg_p->value, true) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      break;

    case PT_MAX:
      if (btree_find_min_or_max_key (thread_p, &agg_p->btid,
				     agg_p->value, false) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      break;

    default:
      break;
    }

  return NO_ERROR;
}

/*
 * qdata_finalize_aggregate_list () -
 *   return: NO_ERROR, or ER_code
 *   agg_list(in)       : Aggregate expression node list
 *
 * Note: Make the final evaluation on the aggregate expression list.
 */
int
qdata_finalize_aggregate_list (THREAD_ENTRY * thread_p,
			       AGGREGATE_TYPE * agg_list_p)
{
  AGGREGATE_TYPE *agg_p;
  DB_VALUE sqr_val;
  DB_VALUE dbval;
  DB_VALUE xavgval, xavg_1val, x2avgval;
  DB_VALUE xavg2val, varval, stdevval;
  DB_VALUE dval;
  double dtmp;
  QFILE_LIST_ID *list_id_p;
  QFILE_LIST_SCAN_ID scan_id;
  SCAN_CODE scan_code;
  QFILE_TUPLE_RECORD tuple_record = {
    NULL, 0
  };
  char *tuple_p;
  PR_TYPE *pr_type_p;
  OR_BUF buf;

  DB_MAKE_NULL (&sqr_val);
  DB_MAKE_NULL (&dbval);
  DB_MAKE_NULL (&xavgval);
  DB_MAKE_NULL (&xavg_1val);
  DB_MAKE_NULL (&x2avgval);
  DB_MAKE_NULL (&xavg2val);
  DB_MAKE_NULL (&varval);
  DB_MAKE_NULL (&stdevval);
  DB_MAKE_NULL (&dval);

  for (agg_p = agg_list_p; agg_p != NULL; agg_p = agg_p->next)
    {
      TP_DOMAIN *tmp_domain_ptr = NULL;

      if (agg_p->domain->type->id == DB_TYPE_NUMERIC
	  && (agg_p->function == PT_VARIANCE || agg_p->function == PT_STDDEV))
	{
	  tmp_domain_ptr = tp_Domains[DB_TYPE_DOUBLE];
	}

      /* set count-star aggregate values */
      if (agg_p->function == PT_COUNT_STAR)
	{
	  DB_MAKE_INT (agg_p->value, agg_p->curr_cnt);
	}

      /* the value of groupby_num() remains unchanged;
       * it will be changed while evaluating groupby_num predicates
       * against each group at 'xs_eval_grbynum_pred()'
       */
      if (agg_p->function == PT_GROUPBY_NUM)
	{
	  /* nothing to do with groupby_num() */
	  continue;
	}

      /* process list file for sum/avg/count distinct */
      if (agg_p->option == Q_DISTINCT
	  && agg_p->function != PT_MAX && agg_p->function != PT_MIN)
	{
	  if (agg_p->flag_agg_optimize == false)
	    {
	      list_id_p = agg_p->list_id =
		qfile_sort_list (thread_p, agg_p->list_id, NULL, Q_DISTINCT);

	      if (!list_id_p)
		{
		  return ER_FAILED;
		}

	      if (agg_p->function == PT_COUNT)
		{
		  DB_MAKE_INT (agg_p->value, list_id_p->tuple_cnt);
		}
	      else
		{
		  pr_type_p = list_id_p->type_list.domp[0]->type;

		  /* scan list file, accumulating total for sum/avg */
		  if (qfile_open_list_scan (list_id_p, &scan_id) != NO_ERROR)
		    {
		      qfile_close_list (thread_p, list_id_p);
		      qfile_destroy_list (thread_p, list_id_p);
		      return ER_FAILED;
		    }

		  while (true)
		    {
		      scan_code =
			qfile_scan_list_next (thread_p, &scan_id,
					      &tuple_record, PEEK);
		      if (scan_code != S_SUCCESS)
			{
			  break;
			}

		      tuple_p = ((char *) tuple_record.tpl
				 + QFILE_TUPLE_LENGTH_SIZE);
		      if (QFILE_GET_TUPLE_VALUE_FLAG (tuple_p) == V_UNBOUND)
			{
			  continue;
			}

		      or_init (&buf,
			       (char *) tuple_p +
			       QFILE_TUPLE_VALUE_HEADER_SIZE,
			       QFILE_GET_TUPLE_VALUE_LENGTH (tuple_p));

		      if ((*(pr_type_p->readval)) (&buf, &dbval,
						   list_id_p->type_list.
						   domp[0], -1, true, NULL,
						   0) != NO_ERROR)
			{
			  qfile_close_scan (thread_p, &scan_id);
			  qfile_close_list (thread_p, list_id_p);
			  qfile_destroy_list (thread_p, list_id_p);
			  return ER_FAILED;
			}

		      if (agg_p->domain->type->id == DB_TYPE_NUMERIC
			  && (agg_p->function == PT_VARIANCE
			      || agg_p->function == PT_STDDEV))
			{
			  if (tp_value_coerce (&dbval, &dbval, tmp_domain_ptr)
			      != DOMAIN_COMPATIBLE)
			    {
			      pr_clear_value (&dbval);
			      qfile_close_scan (thread_p, &scan_id);
			      qfile_close_list (thread_p, list_id_p);
			      qfile_destroy_list (thread_p, list_id_p);
			      return ER_FAILED;
			    }
			}

		      if (DB_IS_NULL (agg_p->value))
			{
			  /* first iteration: can't add to a null agg_ptr->value */
			  PR_TYPE *tmp_pr_type;
			  DB_TYPE dbval_type = DB_VALUE_DOMAIN_TYPE (&dbval);

			  tmp_pr_type = PR_TYPE_FROM_ID (dbval_type);
			  if (tmp_pr_type == NULL)
			    {
			      qfile_close_scan (thread_p, &scan_id);
			      qfile_close_list (thread_p, list_id_p);
			      qfile_destroy_list (thread_p, list_id_p);
			      return ER_FAILED;
			    }

			  if (agg_p->function == PT_STDDEV
			      || agg_p->function == PT_VARIANCE)
			    {
			      if (qdata_multiply_dbval
				  (&dbval, &dbval, &sqr_val,
				   NOT_NULL_VALUE (tmp_domain_ptr,
						   agg_p->domain)) !=
				  NO_ERROR)
				{
				  pr_clear_value (&dbval);
				  qfile_close_scan (thread_p, &scan_id);
				  qfile_close_list (thread_p, list_id_p);
				  qfile_destroy_list (thread_p, list_id_p);
				  return ER_FAILED;
				}

			      (*(tmp_pr_type->setval)) (agg_p->value2,
							&sqr_val, true);
			    }
			  (*(tmp_pr_type->setval)) (agg_p->value, &dbval,
						    true);
			}
		      else
			{
			  if (agg_p->function == PT_STDDEV
			      || agg_p->function == PT_VARIANCE)
			    {
			      if (qdata_multiply_dbval
				  (&dbval, &dbval, &sqr_val,
				   NOT_NULL_VALUE (tmp_domain_ptr,
						   agg_p->domain)) !=
				  NO_ERROR)
				{
				  pr_clear_value (&dbval);
				  qfile_close_scan (thread_p, &scan_id);
				  qfile_close_list (thread_p, list_id_p);
				  qfile_destroy_list (thread_p, list_id_p);
				  return ER_FAILED;
				}

			      if (qdata_add_dbval (agg_p->value2, &sqr_val,
						   agg_p->value2,
						   NOT_NULL_VALUE
						   (tmp_domain_ptr,
						    agg_p->domain)) !=
				  NO_ERROR)
				{
				  pr_clear_value (&dbval);
				  pr_clear_value (&sqr_val);
				  qfile_close_scan (thread_p, &scan_id);
				  qfile_close_list (thread_p, list_id_p);
				  qfile_destroy_list (thread_p, list_id_p);
				  return ER_FAILED;
				}
			    }

			  if (qdata_add_dbval (agg_p->value, &dbval,
					       agg_p->value,
					       NOT_NULL_VALUE (tmp_domain_ptr,
							       agg_p->
							       domain)) !=
			      NO_ERROR)
			    {
			      qfile_close_scan (thread_p, &scan_id);
			      qfile_close_list (thread_p, list_id_p);
			      qfile_destroy_list (thread_p, list_id_p);
			      return ER_FAILED;
			    }
			}
		    }

		  qfile_close_scan (thread_p, &scan_id);
		  agg_p->curr_cnt = list_id_p->tuple_cnt;
		}
	    }

	  /* close and destroy temporary list files */
	  qfile_close_list (thread_p, agg_p->list_id);
	  qfile_destroy_list (thread_p, agg_p->list_id);
	}

      /* compute averages */
      if (agg_p->curr_cnt > 0
	  && (agg_p->function == PT_AVG
	      || agg_p->function == PT_STDDEV
	      || agg_p->function == PT_VARIANCE))
	{
	  TP_DOMAIN *double_domain_ptr = tp_Domains[DB_TYPE_DOUBLE];

	  /* compute AVG(X) = SUM(X)/COUNT(X) */
	  DB_MAKE_INT (&dbval, agg_p->curr_cnt);
	  if (qdata_divide_dbval (agg_p->value, &dbval, &xavgval,
				  NOT_NULL_VALUE (tmp_domain_ptr,
						  agg_p->domain)) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }

	  if (agg_p->function == PT_AVG)
	    {
	      if (tp_value_coerce (&xavgval, agg_p->value, agg_p->domain)
		  != DOMAIN_COMPATIBLE)
		{
		  return ER_FAILED;
		}
	      continue;
	    }

	  if (agg_p->domain->type->id == DB_TYPE_INTEGER
	      || agg_p->domain->type->id == DB_TYPE_SHORT
	      || agg_p->domain->type->id == DB_TYPE_BIGINT)
	    {
	      DB_MAKE_DOUBLE (&dbval, agg_p->curr_cnt);
	      if (qdata_divide_dbval (agg_p->value, &dbval, &xavgval,
				      double_domain_ptr) != NO_ERROR)
		{
		  return ER_FAILED;
		}
	    }

	  /* compute SUM(X^2) / (n-1) */
	  if (agg_p->curr_cnt > 1)
	    {
	      DB_MAKE_INT (&dbval, agg_p->curr_cnt - 1);
	    }
	  else
	    {
	      DB_MAKE_INT (&dbval, 1);
	    }

	  if (qdata_divide_dbval (agg_p->value2, &dbval, &x2avgval,
				  double_domain_ptr) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }

	  /* compute {SUM(X) / (n-1)} */
	  if (qdata_divide_dbval (agg_p->value, &dbval, &xavg_1val,
				  double_domain_ptr) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }

	  /* compute AVG(X) * {SUM(X) / (n-1)} */
	  if (qdata_multiply_dbval (&xavgval, &xavg_1val, &xavg2val,
				    double_domain_ptr) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }

	  /* compute VAR(X) = SUM(X^2)/(n-1) - AVG(X) * {SUM(X) / (n-1)} */
	  if (qdata_subtract_dbval (&x2avgval, &xavg2val, &varval,
				    NOT_NULL_VALUE (tmp_domain_ptr,
						    agg_p->domain)) !=
	      NO_ERROR)
	    {
	      return ER_FAILED;
	    }

	  if (agg_p->function == PT_VARIANCE || agg_p->function == PT_STDDEV)
	    {
	      TP_DOMAIN_STATUS status;

	      if (tmp_domain_ptr)
		{
		  DB_DATA_STATUS data_stat;
		  int error;

		  db_value_domain_init (agg_p->value, DB_TYPE_NUMERIC,
					agg_p->domain->precision,
					agg_p->domain->scale);
		  error = numeric_db_value_coerce_to_num (&varval,
							  agg_p->value,
							  &data_stat);
		  if (error != NO_ERROR)
		    {
		      if (error == ER_NUM_OVERFLOW)
			{
			  status = DOMAIN_OVERFLOW;
			}
		      else
			{
			  status = DOMAIN_INCOMPATIBLE;
			}
		    }
		  else
		    {
		      status = DOMAIN_COMPATIBLE;
		    }
		}
	      else
		{
		  status = tp_value_coerce (&varval, agg_p->value,
					    agg_p->domain);
		}

	      if (status == DOMAIN_OVERFLOW)
		{
		  char buf[64];

		  (void) tp_domain_name (agg_p->domain, buf, sizeof (buf));
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_IT_DATA_OVERFLOW, 1, buf);

		  return ER_FAILED;
		}
	      else if (status != DOMAIN_COMPATIBLE)
		{
		  return ER_FAILED;
		}
	    }

	  if (agg_p->function == PT_STDDEV)
	    {
	      TP_DOMAIN *tmp_domain_ptr;

	      db_value_domain_init (&dval, DB_TYPE_DOUBLE,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	      /* Construct TP_DOMAIN whose type is DB_TYPE_DOUBLE     */
	      tmp_domain_ptr = tp_Domains[DB_TYPE_DOUBLE];
	      if (tp_value_coerce (&varval, &dval, tmp_domain_ptr)
		  != DOMAIN_COMPATIBLE)
		{
		  return ER_FAILED;
		}

	      dtmp = DB_GET_DOUBLE (&dval);
	      dtmp = sqrt (dtmp);
	      DB_MAKE_DOUBLE (&dval, dtmp);
	      if (tp_value_coerce (&dval, agg_p->value, agg_p->domain)
		  != DOMAIN_COMPATIBLE)
		{
		  return ER_FAILED;
		}
	    }
	}
    }

  return NO_ERROR;
}

/*
 * MISCELLANEOUS
 */

/*
 * qdata_get_tuple_value_size_from_dbval () - Return the tuple value size
 *	for the db_value
 *   return:
 *   dbval(in)  : db_value node
 */
static int
qdata_get_tuple_value_size_from_dbval (DB_VALUE * dbval_p)
{
  int val_size, align;
  int tuple_value_size = 0;
  PR_TYPE *type_p;
  DB_TYPE dbval_type;

  if (DB_IS_NULL (dbval_p))
    {
      tuple_value_size = QFILE_TUPLE_VALUE_HEADER_SIZE;
    }
  else
    {
      dbval_type = DB_VALUE_DOMAIN_TYPE (dbval_p);
      type_p = PR_TYPE_FROM_ID (dbval_type);
      if (type_p)
	{
	  if (type_p->lengthval == NULL)
	    {
	      val_size = type_p->disksize;
	    }
	  else
	    {
	      val_size = (*(type_p->lengthval)) (dbval_p, 1);
	    }

	  align = DB_ALIGN (val_size, MAX_ALIGNMENT);	/* to align for the next field */
	  tuple_value_size = QFILE_TUPLE_VALUE_HEADER_SIZE + align;
	}
    }

  return tuple_value_size;
}

/*
 * qdata_get_single_tuple_from_list_id () -
 *   return: NO_ERROR or error code
 *   list_id(in)        : List file identifier
 *   single_tuple(in)   : VAL_LIST
 */
int
qdata_get_single_tuple_from_list_id (THREAD_ENTRY * thread_p,
				     QFILE_LIST_ID * list_id_p,
				     VAL_LIST * single_tuple_p)
{
  QFILE_TUPLE_RECORD tuple_record = {
    NULL, 0
  };
  QFILE_LIST_SCAN_ID scan_id;
  OR_BUF buf;
  PR_TYPE *pr_type_p;
  QFILE_TUPLE_VALUE_FLAG flag;
  int length;
  TP_DOMAIN *domain_p;
  char *ptr;
  int tuple_count, value_count, i;
  QPROC_DB_VALUE_LIST value_list;
  int error_code;

  tuple_count = list_id_p->tuple_cnt;
  value_count = list_id_p->type_list.type_cnt;

  if (tuple_count > 1 || value_count != single_tuple_p->val_cnt)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_INVALID_QRY_SINGLE_TUPLE, 0);
      return ER_QPROC_INVALID_QRY_SINGLE_TUPLE;
    }

  if (tuple_count == 1)
    {
      error_code = qfile_open_list_scan (list_id_p, &scan_id);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}

      if (qfile_scan_list_next (thread_p, &scan_id, &tuple_record, PEEK) !=
	  S_SUCCESS)
	{
	  qfile_close_scan (thread_p, &scan_id);
	  return ER_FAILED;
	}

      for (i = 0, value_list = single_tuple_p->valp;
	   i < value_count; i++, value_list = value_list->next)
	{
	  domain_p = list_id_p->type_list.domp[i];
	  if (domain_p == NULL || domain_p->type == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_QPROC_INVALID_QRY_SINGLE_TUPLE, 0);
	      return ER_QPROC_INVALID_QRY_SINGLE_TUPLE;
	    }

	  if (db_value_domain_init (value_list->val, domain_p->type->id,
				    domain_p->precision,
				    domain_p->scale) != NO_ERROR)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_QPROC_INVALID_QRY_SINGLE_TUPLE, 0);
	      return ER_QPROC_INVALID_QRY_SINGLE_TUPLE;
	    }

	  pr_type_p = domain_p->type;
	  if (pr_type_p == NULL)
	    {
	      return ER_FAILED;
	    }

	  flag = (QFILE_TUPLE_VALUE_FLAG)
	    qfile_locate_tuple_value (tuple_record.tpl, i, &ptr, &length);
	  OR_BUF_INIT (buf, ptr, length);
	  if (flag == V_BOUND)
	    {
	      if ((*(pr_type_p->readval)) (&buf, value_list->val, domain_p,
					   -1, true, NULL, 0) != NO_ERROR)
		{
		  qfile_close_scan (thread_p, &scan_id);
		  return ER_FAILED;
		}
	    }
	  else
	    {
	      /* If value is NULL, properly initialize the result */
	      db_value_domain_init (value_list->val, pr_type_p->id,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	    }
	}

      qfile_close_scan (thread_p, &scan_id);
    }

  return NO_ERROR;
}

/*
 * qdata_get_valptr_type_list () -
 *   return: NO_ERROR, or ER_code
 *   valptr_list(in)    : Value pointer list
 *   type_list(out)     : Set to the result type list
 *
 * Note: Find the result type list of value pointer list and set to
 * type list.  Regu variables that are hidden columns are not
 * entered as part of the type list because they are not entered
 * in the list file.
 */
int
qdata_get_valptr_type_list (VALPTR_LIST * valptr_list_p,
			    QFILE_TUPLE_VALUE_TYPE_LIST * type_list_p)
{
  REGU_VARIABLE_LIST reg_var_p;
  int i, count;

  if (type_list_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1);
      return ER_FAILED;
    }

  reg_var_p = valptr_list_p->valptrp;
  count = 0;

  for (i = 0; i < valptr_list_p->valptr_cnt; i++)
    {
      if (!reg_var_p->value.hidden_column)
	{
	  count++;
	}

      reg_var_p = reg_var_p->next;
    }

  type_list_p->type_cnt = count;
  type_list_p->domp = NULL;

  if (type_list_p->type_cnt != 0)
    {
      type_list_p->domp = (TP_DOMAIN **)
	db_private_alloc (NULL, sizeof (TP_DOMAIN *) * type_list_p->type_cnt);
      if (type_list_p->domp == NULL)
	{
	  return ER_FAILED;
	}
    }

  reg_var_p = valptr_list_p->valptrp;
  for (i = 0; i < type_list_p->type_cnt;)
    {
      if (!reg_var_p->value.hidden_column)
	{
	  type_list_p->domp[i++] = reg_var_p->value.domain;
	}

      reg_var_p = reg_var_p->next;
    }

  return NO_ERROR;
}

/*
 * qdata_get_dbval_from_constant_regu_variable () -
 *   return: DB_VALUE *, or NULL
 *   regu_var(in): Regulator Variable
 *   vd(in)      : Value descriptor
 *
 * Note: Find the db_value represented by regu_var node and
 *       return a pointer to it.
 *
 * Note: Regulator variable should point to only constant values.
 */
static DB_VALUE *
qdata_get_dbval_from_constant_regu_variable (THREAD_ENTRY *
					     thread_p,
					     REGU_VARIABLE *
					     regu_var_p,
					     VAL_DESCR * val_desc_p)
{
  DB_VALUE *peek_value_p;
  int result;

  result =
    fetch_peek_dbval (thread_p, regu_var_p, val_desc_p, NULL, NULL, NULL,
		      &peek_value_p);
  if (result != NO_ERROR)
    {
      return NULL;
    }

  return peek_value_p;
}

/*
 * qdata_convert_dbvals_to_set () -
 *   return: NO_ERROR, or ER_code
 *   stype(in)  : set type
 *   func(in)   : regu variable (guaranteed TYPE_FUNC)
 *   vd(in)     : Value descriptor
 *   obj_oid(in): object identifier
 *   tpl(in)    : list file tuple
 *
 * Note: Convert a list of vars into a sequence and return a pointer to it.
 */
static int
qdata_convert_dbvals_to_set (THREAD_ENTRY * thread_p, DB_TYPE stype,
			     REGU_VARIABLE * regu_func_p,
			     VAL_DESCR * val_desc_p, OID * obj_oid_p,
			     QFILE_TUPLE tuple)
{
  DB_VALUE dbval, *result_p;
  DB_COLLECTION *collection_p = NULL;
  SETOBJ *setobj_p;
  int n, size;
  REGU_VARIABLE_LIST regu_var_p, operand;
  int error;
  TP_DOMAIN *domain_p;

  result_p = regu_func_p->value.funcp->value;
  operand = regu_func_p->value.funcp->operand;
  domain_p = regu_func_p->domain;
  PRIM_INIT_NULL (&dbval);

  if (stype == DB_TYPE_SET)
    {
      collection_p = db_set_create_basic (NULL, NULL);
    }
  else if (stype == DB_TYPE_MULTISET)
    {
      collection_p = db_set_create_multi (NULL, NULL);
    }
  else if (stype == DB_TYPE_SEQUENCE || stype == DB_TYPE_VOBJ)
    {
      size = 0;
      for (regu_var_p = operand; regu_var_p; regu_var_p = regu_var_p->next)
	{
	  size++;
	}

      collection_p = db_seq_create (NULL, NULL, size);
    }
  else
    {
      return ER_FAILED;
    }

  error = set_get_setobj (collection_p, &setobj_p, 1);
  if (error != NO_ERROR || !setobj_p)
    {
      set_free (collection_p);
      return ER_FAILED;
    }

  /*
   * DON'T set the "set"'s domain if it's really a vobj; they don't
   * play by quite the same rules.  The domain coming in here is some
   * flavor of vobj domain,  which is definitely *not* what the
   * components of the sequence will be.  Putting the domain in here
   * evidently causes the vobj's to get packed up in list files in some
   * way that readers can't cope with.
   */
  if (stype != DB_TYPE_VOBJ)
    {
      setobj_put_domain (setobj_p, domain_p);
    }

  n = 0;
  while (operand)
    {
      if (fetch_copy_dbval (thread_p, &operand->value, val_desc_p, NULL,
			    obj_oid_p, tuple, &dbval) != NO_ERROR)
	{
	  return ER_FAILED;
	}

      if ((stype == DB_TYPE_VOBJ) && (n == 2))
	{

	  if (DB_IS_NULL (&dbval))
	    {
	      set_free (collection_p);
	      return NO_ERROR;
	    }
	}

      /* using setobj_put_value transfers "ownership" of the
       * db_value memory to the set. This avoids a redundant clone/free.
       */
      error = setobj_put_value (setobj_p, n, &dbval);

      /*
       * if we attempt to add a duplicate value to a set,
       * clear the value, but do not set an error
       */
      if (error == SET_DUPLICATE_VALUE)
	{
	  pr_clear_value (&dbval);
	  error = NO_ERROR;
	}

      if (error != NO_ERROR)
	{
	  pr_clear_value (&dbval);
	  set_free (collection_p);
	  return ER_FAILED;
	}

      operand = operand->next;
      ++n;
    }

  set_make_collection (result_p, collection_p);
  if (stype == DB_TYPE_VOBJ)
    {
      db_value_alter_type (result_p, DB_TYPE_VOBJ);
    }

  return NO_ERROR;
}

/*
 * qdata_evaluate_generic_function () - Evaluates a generic function.
 *   return: NO_ERROR, or ER_code
 *   funcp(in)  :
 *   vd(in)     :
 *   obj_oid(in)        :
 *   tpl(in)    :
 */
static int
qdata_evaluate_generic_function (THREAD_ENTRY * thread_p,
				 FUNCTION_TYPE * function_p,
				 VAL_DESCR * val_desc_p, OID * obj_oid_p,
				 QFILE_TUPLE tuple)
{
#if defined(ENABLE_UNUSED_FUNCTION)
  DB_VALUE *args[NUM_F_GENERIC_ARGS];
  DB_VALUE *result_p = function_p->value;
  DB_VALUE *offset_dbval_p;
  int offset;
  REGU_VARIABLE_LIST operand = function_p->operand;
  int i, num_args;
  int (*function) (THREAD_ENTRY * thread_p, DB_VALUE *, int, DB_VALUE **);

  /* by convention the first argument for the function is the function
   * jump table offset and is not a real argument to the function.
   */
  if (!operand
      || fetch_peek_dbval (thread_p, &operand->value, val_desc_p, NULL,
			   obj_oid_p, tuple, &offset_dbval_p) != NO_ERROR
      || db_value_type (offset_dbval_p) != DB_TYPE_INTEGER)
    {
      goto error;
    }

  offset = DB_GET_INTEGER (offset_dbval_p);
  if (offset >=
      (SSIZEOF (generic_func_ptrs) / SSIZEOF (generic_func_ptrs[0])))
    {
      goto error;
    }

  function = generic_func_ptrs[offset];
  /* initialize the argument array */
  for (i = 0; i < NUM_F_GENERIC_ARGS; i++)
    {
      args[i] = NULL;
    }

  /* skip the first argument, it is only the offset into the jump table */
  operand = operand->next;
  num_args = 0;

  while (operand)
    {
      num_args++;
      if (num_args > NUM_F_GENERIC_ARGS)
	{
	  goto error;
	}

      if (fetch_peek_dbval (thread_p, &operand->value, val_desc_p, NULL,
			    obj_oid_p, tuple,
			    &args[num_args - 1]) != NO_ERROR)
	{
	  goto error;
	}

      operand = operand->next;
    }

  if ((*function) (thread_p, result_p, num_args, args) != NO_ERROR)
    {
      goto error;
    }

  return NO_ERROR;

error:
#else /* ENABLE_UNUSED_FUNCTION */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_GENERIC_FUNCTION_FAILURE,
	  0);
  return ER_FAILED;
#endif /* ENABLE_UNUSED_FUNCTION */
}

/*
 * qdata_get_class_of_function () -
 *   return: NO_ERROR, or ER_code
 *   funcp(in)  :
 *   vd(in)     :
 *   obj_oid(in)        :
 *   tpl(in)    :
 *
 * Note: This routine returns the class of its argument.
 */
static int
qdata_get_class_of_function (THREAD_ENTRY * thread_p,
			     FUNCTION_TYPE * function_p,
			     VAL_DESCR * val_desc_p, OID * obj_oid_p,
			     QFILE_TUPLE tuple)
{
  OID class_oid;
  OID *instance_oid_p;
  DB_VALUE *val_p, element;
  DB_TYPE type;

  if (fetch_peek_dbval
      (thread_p, &function_p->operand->value, val_desc_p, NULL, obj_oid_p,
       tuple, &val_p) != NO_ERROR || DB_IS_NULL (val_p))
    {
      return ER_FAILED;
    }

  type = db_value_domain_type (val_p);
  if (type == DB_TYPE_VOBJ)
    {
      /* grab the real oid */
      if (db_seq_get (DB_GET_SEQUENCE (val_p), 2, &element) != NO_ERROR)
	{
	  return ER_FAILED;
	}

      val_p = &element;
      type = db_value_domain_type (val_p);
    }

  if (type != DB_TYPE_OID)
    {
      return ER_FAILED;
    }

  instance_oid_p = DB_PULL_OID (val_p);
  if (heap_get_class_oid (thread_p, instance_oid_p, &class_oid) == NULL)
    {
      return ER_FAILED;
    }

  DB_MAKE_OID (function_p->value, &class_oid);
  return NO_ERROR;
}

/*
 * qdata_evaluate_function () -
 *   return: NO_ERROR, or ER_code
 *   func(in)   :
 *   vd(in)     :
 *   obj_oid(in)        :
 *   tpl(in)    :
 *
 * Note: Evaluate given function.
 */
int
qdata_evaluate_function (THREAD_ENTRY * thread_p,
			 REGU_VARIABLE * function_p, VAL_DESCR * val_desc_p,
			 OID * obj_oid_p, QFILE_TUPLE tuple)
{
  FUNCTION_TYPE *funcp;

  funcp = function_p->value.funcp;
  /* clear any value from a previous iteration */
  pr_clear_value (funcp->value);

  switch (funcp->ftype)
    {
    case F_SET:
      return qdata_convert_dbvals_to_set (thread_p, DB_TYPE_SET, function_p,
					  val_desc_p, obj_oid_p, tuple);

    case F_MULTISET:
      return qdata_convert_dbvals_to_set (thread_p, DB_TYPE_MULTISET,
					  function_p, val_desc_p, obj_oid_p,
					  tuple);

    case F_SEQUENCE:
      return qdata_convert_dbvals_to_set (thread_p, DB_TYPE_SEQUENCE,
					  function_p, val_desc_p, obj_oid_p,
					  tuple);

    case F_VID:
      return qdata_convert_dbvals_to_set (thread_p, DB_TYPE_VOBJ, function_p,
					  val_desc_p, obj_oid_p, tuple);

    case F_TABLE_SET:
      return qdata_convert_table_to_set (thread_p, DB_TYPE_SET, function_p,
					 val_desc_p);

    case F_TABLE_MULTISET:
      return qdata_convert_table_to_set (thread_p, DB_TYPE_MULTISET,
					 function_p, val_desc_p);

    case F_TABLE_SEQUENCE:
      return qdata_convert_table_to_set (thread_p, DB_TYPE_SEQUENCE,
					 function_p, val_desc_p);

    case F_GENERIC:
      return qdata_evaluate_generic_function (thread_p, funcp, val_desc_p,
					      obj_oid_p, tuple);

    case F_CLASS_OF:
      return qdata_get_class_of_function (thread_p, funcp, val_desc_p,
					  obj_oid_p, tuple);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      return ER_FAILED;
    }
}

/*
 * qdata_convert_table_to_set () -
 *   return: NO_ERROR, or ER_code
 *   stype(in)  : set type
 *   func(in)   : regu variable (guaranteed TYPE_FUNC)
 *   vd(in)     : Value descriptor
 *
 * Note: Convert a list file into a set/sequence and return a pointer to it.
 */
static int
qdata_convert_table_to_set (THREAD_ENTRY * thread_p, DB_TYPE stype,
			    REGU_VARIABLE * function_p,
			    VAL_DESCR * val_desc_p)
{
  QFILE_LIST_SCAN_ID scan_id;
  QFILE_TUPLE_RECORD tuple_record = {
    NULL, 0
  };
  SCAN_CODE scan_code;
  QFILE_LIST_ID *list_id_p;
  int i, seq_pos;
  int val_size;
  OR_BUF buf;
  DB_VALUE dbval, *result_p;
  DB_COLLECTION *collection_p = NULL;
  SETOBJ *setobj_p;
  DB_TYPE type;
  PR_TYPE *pr_type_p;
  int error;
  REGU_VARIABLE_LIST operand;
  TP_DOMAIN *domain_p;
  char *ptr;

  result_p = function_p->value.funcp->value;
  operand = function_p->value.funcp->operand;

  /* execute linked query */
  EXECUTE_REGU_VARIABLE_XASL (thread_p, &(operand->value), val_desc_p);

  if (CHECK_REGU_VARIABLE_XASL_STATUS (&(operand->value)) != XASL_SUCCESS)
    {
      return ER_FAILED;
    }

  domain_p = function_p->domain;
  list_id_p = operand->value.value.srlist_id->list_id;
  DB_MAKE_NULL (&dbval);

  if (stype == DB_TYPE_SET)
    {
      collection_p = db_set_create_basic (NULL, NULL);
    }
  else if (stype == DB_TYPE_MULTISET)
    {
      collection_p = db_set_create_multi (NULL, NULL);
    }
  else if (stype == DB_TYPE_SEQUENCE || stype == DB_TYPE_VOBJ)
    {
      collection_p = db_seq_create (NULL, NULL,
				    (list_id_p->tuple_cnt *
				     list_id_p->type_list.type_cnt));
    }
  else
    {
      return ER_FAILED;
    }

  error = set_get_setobj (collection_p, &setobj_p, 1);
  if (error != NO_ERROR || !setobj_p)
    {
      set_free (collection_p);
      return ER_FAILED;
    }

  /*
   * Don't need to worry about the vobj case here; this function can't
   * be called in a context where it's expected to produce a vobj.  See
   * xd_dbvals_to_set for the contrasting case.
   */
  setobj_put_domain (setobj_p, domain_p);
  if (qfile_open_list_scan (list_id_p, &scan_id) != NO_ERROR)
    {
      return ER_FAILED;
    }

  seq_pos = 0;
  while (true)
    {
      scan_code =
	qfile_scan_list_next (thread_p, &scan_id, &tuple_record, PEEK);
      if (scan_code != S_SUCCESS)
	{
	  break;
	}

      for (i = 0; i < list_id_p->type_list.type_cnt; i++)
	{
	  /* grab column i and add it to the col */
	  type = list_id_p->type_list.domp[i]->type->id;
	  pr_type_p = PR_TYPE_FROM_ID (type);
	  if (pr_type_p == NULL)
	    {
	      qfile_close_scan (thread_p, &scan_id);
	      return ER_FAILED;
	    }

	  if (qfile_locate_tuple_value (tuple_record.tpl, i, &ptr, &val_size)
	      == V_BOUND)
	    {
	      or_init (&buf, ptr, val_size);

	      if ((*(pr_type_p->readval)) (&buf, &dbval,
					   list_id_p->type_list.domp[i], -1,
					   true, NULL, 0) != NO_ERROR)
		{
		  qfile_close_scan (thread_p, &scan_id);
		  return ER_FAILED;
		}
	    }

	  /*
	   * using setobj_put_value transfers "ownership" of the
	   * db_value memory to the set. This avoids a redundant clone/free.
	   */
	  error = setobj_put_value (setobj_p, seq_pos++, &dbval);

	  /*
	   * if we attempt to add a duplicate value to a set,
	   * clear the value, but do not set an error
	   */
	  if (error == SET_DUPLICATE_VALUE)
	    {
	      pr_clear_value (&dbval);
	      error = NO_ERROR;
	    }

	  if (error != NO_ERROR)
	    {
	      set_free (collection_p);
	      pr_clear_value (&dbval);
	      qfile_close_scan (thread_p, &scan_id);
	      return ER_FAILED;
	    }
	}
    }

  qfile_close_scan (thread_p, &scan_id);
  set_make_collection (result_p, collection_p);

  return NO_ERROR;
}

/*
 * qdata_evaluate_connect_by_root () - CONNECT_BY_ROOT operator evaluation func
 *    return:
 *  xasl_p(in):
 *  regu_p(in):
 *  result_val_p(in/out):
 *  vd(in):
 */
bool
qdata_evaluate_connect_by_root (THREAD_ENTRY * thread_p,
				void *xasl_p,
				REGU_VARIABLE * regu_p,
				DB_VALUE * result_val_p, VAL_DESCR * vd)
{
  QFILE_TUPLE tpl;
  QFILE_LIST_ID *list_id_p;
  QFILE_LIST_SCAN_ID s_id;
  QFILE_TUPLE_RECORD tuple_rec = { (QFILE_TUPLE) NULL, 0 };
  QFILE_TUPLE_POSITION p_pos, *bitval;
  QPROC_DB_VALUE_LIST valp;
  DB_VALUE p_pos_dbval;
  XASL_NODE *xasl, *xptr;
  int length, i;

  /* sanity checks */
  if (regu_p->type != TYPE_CONSTANT)
    {
      return false;
    }

  xasl = (XASL_NODE *) xasl_p;
  if (!xasl)
    {
      return false;
    }

  if (!XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
    {
      return false;
    }

  xptr = xasl->connect_by_ptr;
  if (!xptr)
    {
      return false;
    }

  tpl = xptr->proc.connect_by.curr_tuple;

  /* walk the parents up to root */

  list_id_p = xptr->list_id;

  if (qfile_open_list_scan (list_id_p, &s_id) != NO_ERROR)
    {
      return false;
    }

  /* we start with tpl itself */
  tuple_rec.tpl = tpl;

  do
    {
      /* get the parent node */
      if (qexec_get_tuple_column_value (tuple_rec.tpl,
					xptr->outptr_list->valptr_cnt -
					PCOL_PARENTPOS_TUPLE_OFFSET,
					&p_pos_dbval,
					&tp_Bit_domain) != NO_ERROR)
	{
	  qfile_close_scan (thread_p, &s_id);
	  return false;
	}

      bitval = (QFILE_TUPLE_POSITION *) DB_GET_BIT (&p_pos_dbval, &length);

      if (bitval)
	{
	  p_pos.status = s_id.status;
	  p_pos.position = S_ON;
	  p_pos.vpid = bitval->vpid;
	  p_pos.offset = bitval->offset;
	  p_pos.tpl = NULL;
	  p_pos.tplno = bitval->tplno;

	  if (qfile_jump_scan_tuple_position (thread_p, &s_id, &p_pos,
					      &tuple_rec, PEEK) != S_SUCCESS)
	    {
	      qfile_close_scan (thread_p, &s_id);
	      return false;
	    }
	}
    }
  while (bitval);		/* the parent tuple pos is null for the root node */

  qfile_close_scan (thread_p, &s_id);

  /* here tuple_rec.tpl is the root tuple; get the required column */

  for (i = 0, valp = xptr->val_list->valp; valp; i++, valp = valp->next)
    {
      if (valp->val == regu_p->value.dbvalptr)
	{
	  break;
	}
    }

  if (i >= xptr->val_list->val_cnt)
    {
      return false;
    }

  if (qexec_get_tuple_column_value (tuple_rec.tpl, i, result_val_p,
				    regu_p->domain) != NO_ERROR)
    {
      return false;
    }

  return true;
}

/*
 * qdata_evaluate_qprior () - PRIOR in SELECT list evaluation func
 *    return:
 *  xasl_p(in):
 *  regu_p(in):
 *  result_val_p(in/out):
 *  vd(in):
 */
bool
qdata_evaluate_qprior (THREAD_ENTRY * thread_p,
		       void *xasl_p,
		       REGU_VARIABLE * regu_p,
		       DB_VALUE * result_val_p, VAL_DESCR * vd)
{
  QFILE_TUPLE tpl;
  QFILE_LIST_ID *list_id_p;
  QFILE_LIST_SCAN_ID s_id;
  QFILE_TUPLE_RECORD tuple_rec = { (QFILE_TUPLE) NULL, 0 };
  QFILE_TUPLE_POSITION p_pos, *bitval;
  DB_VALUE p_pos_dbval;
  XASL_NODE *xasl, *xptr;
  int length;

  xasl = (XASL_NODE *) xasl_p;

  /* sanity checks */
  if (!xasl)
    {
      return false;
    }

  if (!XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
    {
      return false;
    }

  xptr = xasl->connect_by_ptr;
  if (!xptr)
    {
      return false;
    }

  tpl = xptr->proc.connect_by.curr_tuple;

  list_id_p = xptr->list_id;

  if (qfile_open_list_scan (list_id_p, &s_id) != NO_ERROR)
    {
      return false;
    }

  tuple_rec.tpl = tpl;

  /* get the parent node */
  if (qexec_get_tuple_column_value (tuple_rec.tpl,
				    xptr->outptr_list->valptr_cnt -
				    PCOL_PARENTPOS_TUPLE_OFFSET,
				    &p_pos_dbval, &tp_Bit_domain) != NO_ERROR)
    {
      qfile_close_scan (thread_p, &s_id);
      return false;
    }

  bitval = (QFILE_TUPLE_POSITION *) DB_GET_BIT (&p_pos_dbval, &length);

  if (bitval)
    {
      p_pos.status = s_id.status;
      p_pos.position = S_ON;
      p_pos.vpid = bitval->vpid;
      p_pos.offset = bitval->offset;
      p_pos.tpl = NULL;
      p_pos.tplno = bitval->tplno;

      if (qfile_jump_scan_tuple_position (thread_p, &s_id, &p_pos,
					  &tuple_rec, PEEK) != S_SUCCESS)
	{
	  qfile_close_scan (thread_p, &s_id);
	  return false;
	}
    }
  else
    {
      /* the parent tuple pos is null for the root node */
      tuple_rec.tpl = NULL;
    }

  qfile_close_scan (thread_p, &s_id);

  if (tuple_rec.tpl != NULL)
    {
      /* fetch val list from the parent tuple */
      if (fetch_val_list (thread_p,
			  xptr->proc.connect_by.prior_regu_list_pred,
			  vd, NULL, NULL, tuple_rec.tpl, PEEK) != NO_ERROR)
	{
	  return false;
	}
      if (fetch_val_list (thread_p,
			  xptr->proc.connect_by.prior_regu_list_rest,
			  vd, NULL, NULL, tuple_rec.tpl, PEEK) != NO_ERROR)
	{
	  return false;
	}

      /* replace values in T_QPRIOR argument with values from parent tuple */
      qexec_replace_prior_regu_vars_prior_expr (thread_p, regu_p, xptr, xptr);

      /* evaluate the modified regu_p */
      if (fetch_copy_dbval (thread_p, regu_p, vd, NULL, NULL,
			    tuple_rec.tpl, result_val_p) != NO_ERROR)
	{
	  return false;
	}
    }
  else
    {
      DB_MAKE_NULL (result_val_p);
    }

  return true;
}

/*
 * qdata_evaluate_sys_connect_by_path () - SYS_CONNECT_BY_PATH function
 *	evaluation func
 *    return:
 *  select_xasl(in):
 *  regu_p1(in): column
 *  regu_p2(in): character
 *  result_val_p(in/out):
 */
bool
qdata_evaluate_sys_connect_by_path (THREAD_ENTRY * thread_p,
				    void *xasl_p,
				    REGU_VARIABLE * regu_p,
				    DB_VALUE * value_char,
				    DB_VALUE * result_p, VAL_DESCR * vd)
{
  QFILE_TUPLE tpl;
  QFILE_LIST_ID *list_id_p;
  QFILE_LIST_SCAN_ID s_id;
  QFILE_TUPLE_RECORD tuple_rec = { (QFILE_TUPLE) NULL, 0 };
  QFILE_TUPLE_POSITION p_pos, *bitval;
  QPROC_DB_VALUE_LIST valp;
  DB_VALUE p_pos_dbval, cast_value, arg_dbval;
  XASL_NODE *xasl, *xptr;
  int length, i;
  char *result_path = NULL, *path_tmp = NULL;
  int len_result_path, len_tmp = 0, len;
  char *sep = NULL;
  DB_VALUE *arg_dbval_p;
  DB_VALUE **save_values = NULL;
  bool use_extended = false;	/* flag for using extended form, accepting an
				 * expression as the first argument of
				 * SYS_CONNECT_BY_PATH() */

  /* sanity checks */
  xasl = (XASL_NODE *) xasl_p;
  if (!xasl)
    {
      return false;
    }

  if (!XASL_IS_FLAGED (xasl, XASL_HAS_CONNECT_BY))
    {
      return false;
    }

  xptr = xasl->connect_by_ptr;
  if (!xptr)
    {
      return false;
    }

  tpl = xptr->proc.connect_by.curr_tuple;

  /* column */
  if (regu_p->type != TYPE_CONSTANT)
    {
      /* NOTE: if the column is non-string, a cast will be made (see T_CAST).
       *  This is specific to sys_connect_by_path because the result is always
       *  varchar (by comparison to connect_by_root which has the result of the
       *  root specifiec column). The cast is propagated from the parser tree
       *  into the regu variabile, which has the TYPE_INARITH type with arithptr
       *  with type T_CAST and right argument the real column, which will be
       *  further used for column retrieving in the xasl->val_list->valp.
       */

      if (regu_p->type == TYPE_INARITH)
	{
	  if (regu_p->value.arithptr
	      && regu_p->value.arithptr->opcode == T_CAST)
	    {
	      /* correct column */
	      regu_p = regu_p->value.arithptr->rightptr;
	    }
	}
    }

  /* set the flag for using extended form, but keep the single-column argument
   * code too for being faster for its particular case
   */
  if (regu_p->type != TYPE_CONSTANT)
    {
      use_extended = true;
    }
  else
    {
      arg_dbval_p = &arg_dbval;
      DB_MAKE_NULL (arg_dbval_p);
    }

  /* character */
  i = strlen (DB_GET_STRING_SAFE (value_char));
  sep = (char *) db_private_alloc (thread_p, sizeof (char) * (i + 1));
  if (sep == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, i + 1);
      goto error;
    }
  sep[0] = 0;
  if (i > 0)
    {
      strcpy (sep, DB_GET_STRING_SAFE (value_char));
    }

  /* walk the parents up to root */

  list_id_p = xptr->list_id;

  if (qfile_open_list_scan (list_id_p, &s_id) != NO_ERROR)
    {
      goto error2;
    }

  if (!use_extended)
    {
      /* column index */
      for (i = 0, valp = xptr->val_list->valp; valp; i++, valp = valp->next)
	{
	  if (valp->val == regu_p->value.dbvalptr)
	    {
	      break;
	    }
	}

      if (i >= xptr->val_list->val_cnt)
	{
	  goto error;
	}
    }
  else
    {
      /* save val_list */
      if (xptr->val_list->val_cnt > 0)
	{
	  save_values =
	    (DB_VALUE **) db_private_alloc (thread_p, sizeof (DB_VALUE *) *
					    xptr->val_list->val_cnt);
	  if (save_values == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      sizeof (DB_VALUE *) * xptr->val_list->val_cnt);
	      goto error;
	    }

	  memset (save_values, 0,
		  sizeof (DB_VALUE *) * xptr->val_list->val_cnt);
	  for (i = 0, valp = xptr->val_list->valp;
	       valp && i < xptr->val_list->val_cnt; i++, valp = valp->next)
	    {
	      save_values[i] = db_value_copy (valp->val);
	    }
	}
    }

  /* we start with tpl itself */
  tuple_rec.tpl = tpl;

  len_result_path = SYS_CONNECT_BY_PATH_MEM_STEP;
  result_path =
    (char *) db_private_alloc (thread_p, sizeof (char) * len_result_path);
  if (result_path == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, len_result_path);
      goto error;
    }

  strcpy (result_path, "");

  do
    {
      if (!use_extended)
	{
	  /* get the required column */
	  if (qexec_get_tuple_column_value (tuple_rec.tpl, i, arg_dbval_p,
					    regu_p->domain) != NO_ERROR)
	    {
	      goto error;
	    }
	}
      else
	{
	  /* fetch value list */
	  if (fetch_val_list (thread_p, xptr->proc.connect_by.regu_list_pred,
			      vd, NULL, NULL, tuple_rec.tpl,
			      PEEK) != NO_ERROR)
	    {
	      goto error;
	    }
	  if (fetch_val_list (thread_p, xptr->proc.connect_by.regu_list_rest,
			      vd, NULL, NULL, tuple_rec.tpl,
			      PEEK) != NO_ERROR)
	    {
	      goto error;
	    }

	  /* evaluate argument expression */
	  if (fetch_peek_dbval (thread_p, regu_p, vd, NULL, NULL,
				tuple_rec.tpl, &arg_dbval_p) != NO_ERROR)
	    {
	      goto error;
	    }
	}

      if (DB_IS_NULL (arg_dbval_p))
	{
	  DB_MAKE_NULL (&cast_value);
	}
      else
	{
	  /* cast result to string; this call also allocates the container */
	  if (qdata_cast_to_domain (arg_dbval_p, &cast_value,
				    &tp_String_domain) != NO_ERROR)
	    {
	      goto error;
	    }
	}

      len = (strlen (sep) +
	     (DB_IS_NULL (&cast_value) ? 0 : DB_GET_STRING_SIZE (&cast_value))
	     + strlen (result_path) + 1);
      if (len > len_tmp)
	{
	  len_tmp = len;
	  path_tmp =
	    (char *) db_private_alloc (thread_p, sizeof (char) * len_tmp);
	  if (path_tmp == NULL)
	    {
	      db_value_clear (&cast_value);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, len_tmp);
	      goto error;
	    }
	}

      strcpy (path_tmp, sep);
      strcat (path_tmp, DB_GET_STRING_SAFE (&cast_value));

      strcat (path_tmp, result_path);

      /* free the container for cast_value */
      if (db_value_clear (&cast_value) != NO_ERROR)
	{
	  goto error;
	}

      while (strlen (path_tmp) + 1 > len_result_path)
	{
	  len_result_path += SYS_CONNECT_BY_PATH_MEM_STEP;
	  db_private_free_and_init (thread_p, result_path);
	  result_path =
	    (char *) db_private_alloc (thread_p,
				       sizeof (char) * len_result_path);
	  if (result_path == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, len_result_path);
	      goto error;
	    }
	}

      strcpy (result_path, path_tmp);

      /* get the parent node */
      if (qexec_get_tuple_column_value (tuple_rec.tpl,
					xptr->outptr_list->valptr_cnt -
					PCOL_PARENTPOS_TUPLE_OFFSET,
					&p_pos_dbval,
					&tp_Bit_domain) != NO_ERROR)
	{
	  goto error;
	}

      bitval = (QFILE_TUPLE_POSITION *) DB_GET_BIT (&p_pos_dbval, &length);

      if (bitval)
	{
	  p_pos.status = s_id.status;
	  p_pos.position = S_ON;
	  p_pos.vpid = bitval->vpid;
	  p_pos.offset = bitval->offset;
	  p_pos.tpl = NULL;
	  p_pos.tplno = bitval->tplno;

	  if (qfile_jump_scan_tuple_position (thread_p, &s_id, &p_pos,
					      &tuple_rec, PEEK) != S_SUCCESS)
	    {
	      goto error;
	    }
	}
    }
  while (bitval);		/* the parent tuple pos is null for the root node */

  qfile_close_scan (thread_p, &s_id);

  DB_MAKE_STRING (result_p, result_path);

  if (use_extended)
    {
      /* restore val_list */
      if (xptr->val_list->val_cnt > 0)
	{
	  for (i = 0, valp = xptr->val_list->valp;
	       valp && i < xptr->val_list->val_cnt; i++, valp = valp->next)
	    {
	      if (db_value_clear (valp->val) != NO_ERROR)
		{
		  goto error2;
		}
	      if (db_value_clone (save_values[i], valp->val) != NO_ERROR)
		{
		  goto error2;
		}
	    }

	  for (i = 0; i < xptr->val_list->val_cnt; i++)
	    {
	      if (save_values[i])
		{
		  if (db_value_free (save_values[i]) != NO_ERROR)
		    {
		      goto error2;
		    }
		  save_values[i] = NULL;
		}
	    }
	  db_private_free_and_init (thread_p, save_values);
	}
    }

  if (path_tmp)
    {
      db_private_free_and_init (thread_p, path_tmp);
    }

  if (sep)
    {
      db_private_free_and_init (thread_p, sep);
    }

  return true;

error:
  qfile_close_scan (thread_p, &s_id);

  if (save_values)
    {
      for (i = 0; i < xptr->val_list->val_cnt; i++)
	{
	  if (save_values[i])
	    {
	      db_value_free (save_values[i]);
	    }
	  db_private_free_and_init (thread_p, save_values);
	}
    }

error2:
  if (result_path)
    {
      db_private_free_and_init (thread_p, result_path);
    }

  if (path_tmp)
    {
      db_private_free_and_init (thread_p, path_tmp);
    }

  if (sep)
    {
      db_private_free_and_init (thread_p, sep);
    }

  return false;
}

/*
 * qdata_bit_not_dbval () - bitwise not
 *   return: NO_ERROR, or ER_code
 *   dbval_p(in) : db_value node
 *   result_p(out) : resultant db_value node
 *   domain_p(in) :
 *
 */
int
qdata_bit_not_dbval (DB_VALUE * dbval_p, DB_VALUE * result_p,
		     TP_DOMAIN * domain_p)
{
  DB_TYPE type;

  if ((domain_p != NULL && domain_p->type->id == DB_TYPE_NULL)
      || DB_IS_NULL (dbval_p))
    {
      return NO_ERROR;
    }

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_NULL:
      db_make_null (result_p);
      break;

    case DB_TYPE_INTEGER:
      if (DB_GET_INTEGER (dbval_p) == DB_INT32_MIN)
	{
	  goto overflow;
	}
      else
	{
	  db_make_bigint (result_p, ~((INT64) DB_GET_INTEGER (dbval_p)));
	}
      break;

    case DB_TYPE_BIGINT:
      if (DB_GET_BIGINT (dbval_p) == DB_BIGINT_MIN)
	{
	  goto overflow;
	}
      else
	{
	  db_make_bigint (result_p, ~DB_GET_BIGINT (dbval_p));
	}
      break;

    case DB_TYPE_SHORT:
      if (DB_GET_SHORT (dbval_p) == DB_INT16_MIN)
	{
	  goto overflow;
	}
      else
	{
	  db_make_bigint (result_p, ~((INT64) DB_GET_SHORT (dbval_p)));
	}
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      return ER_QPROC_INVALID_DATATYPE;
    }

  return NO_ERROR;

overflow:
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_BITOP, 0);
  return ER_QPROC_OVERFLOW_BITOP;
}

/*
 * qdata_bit_and_dbval () - bitwise and
 *   return: NO_ERROR, or ER_code
 *   dbval1_p(in) : first db_value node
 *   dbval2_p(in) : second db_value node
 *   result_p(out) : resultant db_value node
 *   domain_p(in) :
 *
 */
int
qdata_bit_and_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		     DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type[2];
  DB_BIGINT bi[2];
  DB_VALUE *dbval[2];
  int i;

  if ((domain_p != NULL && domain_p->type->id == DB_TYPE_NULL)
      || DB_IS_NULL (dbval1_p) || DB_IS_NULL (dbval2_p))
    {
      return NO_ERROR;
    }

  type[0] = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type[1] = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  dbval[0] = dbval1_p;
  dbval[1] = dbval2_p;

  for (i = 0; i < 2; i++)
    {
      switch (type[i])
	{
	case DB_TYPE_NULL:
	  db_make_null (result_p);
	  break;

	case DB_TYPE_INTEGER:
	  if (DB_GET_INTEGER (dbval[i]) == DB_INT32_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = (DB_BIGINT) DB_GET_INTEGER (dbval[i]);
	    }
	  break;

	case DB_TYPE_BIGINT:
	  if (DB_GET_BIGINT (dbval[i]) == DB_BIGINT_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = DB_GET_BIGINT (dbval[i]);
	    }
	  break;

	case DB_TYPE_SHORT:
	  if (DB_GET_SHORT (dbval[i]) == DB_INT16_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = (DB_BIGINT) DB_GET_SHORT (dbval[i]);
	    }
	  break;

	default:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE,
		  0);
	  return ER_QPROC_INVALID_DATATYPE;
	}
    }

  if (type[0] != DB_TYPE_NULL && type[1] != DB_TYPE_NULL)
    db_make_bigint (result_p, bi[0] & bi[1]);

  return NO_ERROR;

overflow:
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_BITOP, 0);
  return ER_QPROC_OVERFLOW_BITOP;
}

/*
 * qdata_bit_or_dbval () - bitwise or
 *   return: NO_ERROR, or ER_code
 *   dbval1_p(in) : first db_value node
 *   dbval2_p(in) : second db_value node
 *   result_p(out) : resultant db_value node
 *   domain_p(in) :
 *
 */
int
qdata_bit_or_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		    DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type[2];
  DB_BIGINT bi[2];
  DB_VALUE *dbval[2];
  int i;

  if ((domain_p != NULL && domain_p->type->id == DB_TYPE_NULL)
      || DB_IS_NULL (dbval1_p) || DB_IS_NULL (dbval2_p))
    {
      return NO_ERROR;
    }

  type[0] = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type[1] = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  dbval[0] = dbval1_p;
  dbval[1] = dbval2_p;

  for (i = 0; i < 2; i++)
    {
      switch (type[i])
	{
	case DB_TYPE_NULL:
	  db_make_null (result_p);
	  break;

	case DB_TYPE_INTEGER:
	  if (DB_GET_INTEGER (dbval[i]) == DB_INT32_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = (DB_BIGINT) DB_GET_INTEGER (dbval[i]);
	    }
	  break;

	case DB_TYPE_BIGINT:
	  if (DB_GET_BIGINT (dbval[i]) == DB_BIGINT_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = DB_GET_BIGINT (dbval[i]);
	    }
	  break;

	case DB_TYPE_SHORT:
	  if (DB_GET_SHORT (dbval[i]) == DB_INT16_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = (DB_BIGINT) DB_GET_SHORT (dbval[i]);
	    }
	  break;

	default:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE,
		  0);
	  return ER_QPROC_INVALID_DATATYPE;
	}
    }

  if (type[0] != DB_TYPE_NULL && type[1] != DB_TYPE_NULL)
    db_make_bigint (result_p, bi[0] | bi[1]);

  return NO_ERROR;

overflow:
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_BITOP, 0);
  return ER_QPROC_OVERFLOW_BITOP;
}

/*
 * qdata_bit_xor_dbval () - bitwise xor
 *   return: NO_ERROR, or ER_code
 *   dbval1_p(in) : first db_value node
 *   dbval2_p(in) : second db_value node
 *   result_p(out) : resultant db_value node
 *   domain_p(in) :
 *
 */
int
qdata_bit_xor_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		     DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE type[2];
  DB_BIGINT bi[2];
  DB_VALUE *dbval[2];
  int i;

  if ((domain_p != NULL && domain_p->type->id == DB_TYPE_NULL)
      || DB_IS_NULL (dbval1_p) || DB_IS_NULL (dbval2_p))
    {
      return NO_ERROR;
    }

  type[0] = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type[1] = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  dbval[0] = dbval1_p;
  dbval[1] = dbval2_p;

  for (i = 0; i < 2; i++)
    {
      switch (type[i])
	{
	case DB_TYPE_NULL:
	  db_make_null (result_p);
	  break;

	case DB_TYPE_INTEGER:
	  if (DB_GET_INTEGER (dbval[i]) == DB_INT32_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = (DB_BIGINT) DB_GET_INTEGER (dbval[i]);
	    }
	  break;

	case DB_TYPE_BIGINT:
	  if (DB_GET_BIGINT (dbval[i]) == DB_BIGINT_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = DB_GET_BIGINT (dbval[i]);
	    }
	  break;

	case DB_TYPE_SHORT:
	  if (DB_GET_SHORT (dbval[i]) == DB_INT16_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = (DB_BIGINT) DB_GET_SHORT (dbval[i]);
	    }
	  break;

	default:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE,
		  0);
	  return ER_QPROC_INVALID_DATATYPE;
	}
    }

  if (type[0] != DB_TYPE_NULL && type[1] != DB_TYPE_NULL)
    db_make_bigint (result_p, bi[0] ^ bi[1]);

  return NO_ERROR;

overflow:
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_BITOP, 0);
  return ER_QPROC_OVERFLOW_BITOP;
}

/*
 * qdata_bit_shift_dbval () - bitshift
 *   return: NO_ERROR, or ER_code
 *   dbval1_p(in) : first db_value node
 *   dbval2_p(in) : second db_value node
 *   result_p(out) : resultant db_value node
 *   domain_p(in) :
 *
 */
int
qdata_bit_shift_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		       OPERATOR_TYPE op, DB_VALUE * result_p,
		       TP_DOMAIN * domain_p)
{
  DB_TYPE type[2];
  DB_BIGINT bi[2];
  DB_VALUE *dbval[2];
  int i;

  if ((domain_p != NULL && domain_p->type->id == DB_TYPE_NULL)
      || DB_IS_NULL (dbval1_p) || DB_IS_NULL (dbval2_p))
    {
      return NO_ERROR;
    }

  type[0] = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type[1] = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  dbval[0] = dbval1_p;
  dbval[1] = dbval2_p;

  for (i = 0; i < 2; i++)
    {
      switch (type[i])
	{
	case DB_TYPE_NULL:
	  db_make_null (result_p);
	  break;

	case DB_TYPE_INTEGER:
	  if (DB_GET_INTEGER (dbval[i]) == DB_INT32_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = (DB_BIGINT) DB_GET_INTEGER (dbval[i]);
	    }
	  break;

	case DB_TYPE_BIGINT:
	  if (DB_GET_BIGINT (dbval[i]) == DB_BIGINT_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = DB_GET_BIGINT (dbval[i]);
	    }
	  break;

	case DB_TYPE_SHORT:
	  if (DB_GET_SHORT (dbval[i]) == DB_INT16_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = (DB_BIGINT) DB_GET_SHORT (dbval[i]);
	    }
	  break;

	default:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE,
		  0);
	  return ER_QPROC_INVALID_DATATYPE;
	}
    }

  if (type[0] != DB_TYPE_NULL && type[1] != DB_TYPE_NULL)
    {
      if (bi[1] < (sizeof (DB_BIGINT) * 8) && bi[1] >= 0)
	{
	  if (op == T_BITSHIFT_LEFT)
	    {
	      db_make_bigint (result_p, ((UINT64) bi[0]) << ((UINT64) bi[1]));
	    }
	  else
	    {
	      db_make_bigint (result_p, ((UINT64) bi[0]) >> ((UINT64) bi[1]));
	    }
	}
      else
	{
	  db_make_bigint (result_p, 0);
	}
    }

  return NO_ERROR;

overflow:
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_DIVISION, 0);
  return ER_QPROC_OVERFLOW_DIVISION;
}

/*
 * qdata_divmod_dbval () - DIV/MOD operator
 *   return: NO_ERROR, or ER_code
 *   dbval1_p(in) : first db_value node
 *   dbval2_p(in) : second db_value node
 *   result_p(out) : resultant db_value node
 *   domain_p(in) :
 *
 */
int
qdata_divmod_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		    OPERATOR_TYPE op, DB_VALUE * result_p,
		    TP_DOMAIN * domain_p)
{
  DB_TYPE type[2];
  DB_BIGINT bi[2];
  DB_VALUE *dbval[2];
  int i;

  if ((domain_p != NULL && domain_p->type->id == DB_TYPE_NULL)
      || DB_IS_NULL (dbval1_p) || DB_IS_NULL (dbval2_p))
    {
      return NO_ERROR;
    }

  type[0] = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type[1] = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  dbval[0] = dbval1_p;
  dbval[1] = dbval2_p;

  for (i = 0; i < 2; i++)
    {
      switch (type[i])
	{
	case DB_TYPE_NULL:
	  db_make_null (result_p);
	  break;

	case DB_TYPE_INTEGER:
	  if (DB_GET_INTEGER (dbval[i]) == DB_INT32_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = (DB_BIGINT) DB_GET_INTEGER (dbval[i]);
	    }
	  break;

	case DB_TYPE_BIGINT:
	  if (DB_GET_BIGINT (dbval[i]) == DB_BIGINT_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = DB_GET_BIGINT (dbval[i]);
	    }
	  break;

	case DB_TYPE_SHORT:
	  if (DB_GET_SHORT (dbval[i]) == DB_INT16_MIN)
	    {
	      goto overflow;
	    }
	  else
	    {
	      bi[i] = (DB_BIGINT) DB_GET_SHORT (dbval[i]);
	    }
	  break;

	default:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE,
		  0);
	  return ER_QPROC_INVALID_DATATYPE;
	}
    }

  if (type[0] != DB_TYPE_NULL && type[1] != DB_TYPE_NULL)
    {
      if (bi[1] != 0)
	{
	  if (op == T_INTDIV)
	    {
	      if (type[0] == DB_TYPE_INTEGER)
		{
		  db_make_int (result_p, (INT32) (bi[0] / bi[1]));
		}
	      else if (type[0] == DB_TYPE_BIGINT)
		{
		  db_make_bigint (result_p, bi[0] / bi[1]);
		}
	      else
		{
		  db_make_short (result_p, (INT16) (bi[0] / bi[1]));
		}
	    }
	  else
	    {
	      if (type[0] == DB_TYPE_INTEGER)
		{
		  db_make_int (result_p, (INT32) (bi[0] % bi[1]));
		}
	      else if (type[0] == DB_TYPE_BIGINT)
		{
		  db_make_bigint (result_p, bi[0] % bi[1]);
		}
	      else
		{
		  db_make_short (result_p, (INT16) (bi[0] % bi[1]));
		}
	    }
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_ZERO_DIVIDE, 0);
	  return ER_QPROC_ZERO_DIVIDE;
	}
    }

  return NO_ERROR;

overflow:
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_DIVISION, 0);
  return ER_QPROC_OVERFLOW_DIVISION;
}

/*
 * qdata_list_dbs () - lists all databases names
 *   return: NO_ERROR, or ER_code
 *   result_p(out) : resultant db_value node
 */
int
qdata_list_dbs (THREAD_ENTRY * thread_p, DB_VALUE * result_p)
{
  DB_INFO *db_info_p;

  if (cfg_read_directory (&db_info_p, false) != NO_ERROR)
    {
      if (er_errid () == NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CFG_NO_FILE, 1,
		  DATABASES_FILENAME);
	}
      goto error;
    }

  if (db_info_p)
    {
      DB_INFO *list_p;
      char *name_list;
      size_t name_list_size = 0;
      bool is_first;

      for (list_p = db_info_p; list_p != NULL; list_p = list_p->next)
	{
	  if (list_p->name)
	    {
	      name_list_size += strlen (list_p->name) + 1;
	    }
	}

      if (name_list_size != 0)
	{
	  name_list = (char *) db_private_alloc (thread_p, name_list_size);
	  if (name_list == NULL)
	    {
	      cfg_free_directory (db_info_p);
	      goto error;
	    }
	  strcpy (name_list, "");

	  for (list_p = db_info_p, is_first = true; list_p != NULL;
	       list_p = list_p->next)
	    {
	      if (list_p->name)
		{
		  if (!is_first)
		    {
		      strcat (name_list, " ");
		    }
		  else
		    {
		      is_first = false;
		    }
		  strcat (name_list, list_p->name);
		}
	    }

	  cfg_free_directory (db_info_p);

	  if (db_make_string (result_p, name_list) != NO_ERROR)
	    {
	      goto error;
	    }
	}
      else
	{
	  cfg_free_directory (db_info_p);
	  db_make_null (result_p);
	}
    }
  else
    {
      db_make_null (result_p);
    }

  return NO_ERROR;

error:
  return er_errid ();
}
