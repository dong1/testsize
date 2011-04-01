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
 * db_macro.c - API functions related to db_make and DB_GET
 *
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <string.h>
#include "system_parameter.h"
#include "error_manager.h"
#include "language_support.h"
#include "db.h"
#include "object_print.h"
#include "intl_support.h"
#include "string_opfunc.h"
#include "object_domain.h"
#include "set_object.h"
#include "cnv.h"
#if !defined(SERVER_MODE)
#include "object_accessor.h"
#endif

enum
{
  C_TO_VALUE_NOERROR = 0,
  C_TO_VALUE_UNSUPPORTED_CONVERSION = -1,
  C_TO_VALUE_CONVERSION_ERROR = -2,
  C_TO_VALUE_TRUNCATION_ERROR = -3
};

typedef enum
{
  SMALL_STRING,
  MEDIUM_STRING,
  LARGE_STRING
} STRING_STYLE;

#if defined(SERVER_MODE)
int db_Connect_status = DB_CONNECTION_STATUS_CONNECTED;
#else
int db_Connect_status = DB_CONNECTION_STATUS_NOT_CONNECTED;
int db_Client_type = DB_CLIENT_TYPE_DEFAULT;
#endif
int db_Enable_replications = 0;
int db_Disable_modifications = 0;

static int transfer_string (char *dst, int *xflen, int *outlen,
			    const int dstlen, const char *src,
			    const int srclen, const DB_TYPE_C type,
			    const INTL_CODESET codeset);
static int transfer_bit_string (char *buf, int *xflen,
				int *outlen, const int buflen,
				const DB_VALUE * src, const DB_TYPE_C c_type);
static int coerce_char_to_dbvalue (DB_VALUE * value, char *buf,
				   const int buflen);
static int coerce_numeric_to_dbvalue (DB_VALUE * value, char *buf,
				      const DB_TYPE_C c_type);
static int coerce_binary_to_dbvalue (DB_VALUE * value, char *buf,
				     const int buflen);
static int coerce_date_to_dbvalue (DB_VALUE * value, char *buf);
static int coerce_time_to_dbvalue (DB_VALUE * value, char *buf);
static int coerce_timestamp_to_dbvalue (DB_VALUE * value, char *buf);
static int coerce_datetime_to_dbvalue (DB_VALUE * value, char *buf);

/*
 *  db_value_put_null()
 *  return : Error indicator
 *  value(out) : value container to set NULL.
 */
int
db_value_put_null (DB_VALUE * value)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.is_null = 1;
  value->need_clear = false;

  return NO_ERROR;
}

#define IS_INVALID_PRECISION(p,m) \
  (((p) != DB_DEFAULT_PRECISION) && (((p) <= 0) || ((p) > (m))))
/*
 *  db_value_domain_init() - initialize value container with given type
 *                           and precision/scale.
 *  return : Error indicator.
 *  value(in/out) : DB_VALUE container to initialize.
 *  type(in)      : Type.
 *  precision(in) : Precision.
 *  scale(in)     : Scale.
 *
 */

int
db_value_domain_init (DB_VALUE * value, const DB_TYPE type,
		      const int precision, const int scale)
{
  int error = NO_ERROR;

  CHECK_1ARG_ERROR (value);

  /* It's important to initialize the codeset of the data portion since
     it is considered domain information.  It doesn't matter what we set
     it to, since it will be reset correctly when data is stored in the
     DB_VALUE by one of the db_make* function.
   */
  value->data.ch.info.codeset = 0;
  value->domain.general_info.type = type;
  value->domain.numeric_info.precision = precision;
  value->domain.numeric_info.scale = scale;
  value->need_clear = false;
  value->domain.general_info.is_null = 1;

  switch (type)
    {
    case DB_TYPE_NUMERIC:
      if (precision == DB_DEFAULT_PRECISION)
	{
	  value->domain.numeric_info.precision = DB_DEFAULT_NUMERIC_PRECISION;
	}
      else
	{
	  value->domain.numeric_info.precision = precision;
	}
      if (scale == DB_DEFAULT_SCALE)
	{
	  value->domain.numeric_info.scale = DB_DEFAULT_NUMERIC_SCALE;
	}
      else
	{
	  value->domain.numeric_info.scale = scale;
	}
      if (IS_INVALID_PRECISION (precision, DB_MAX_NUMERIC_PRECISION))
	{
	  error = ER_INVALID_PRECISION;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_INVALID_PRECISION, 3, precision, 0,
		  DB_MAX_NUMERIC_PRECISION);
	  value->domain.numeric_info.precision = DB_DEFAULT_NUMERIC_PRECISION;
	  value->domain.numeric_info.scale = DB_DEFAULT_NUMERIC_SCALE;
	}
      break;

    case DB_TYPE_BIT:
      if (precision == DB_DEFAULT_PRECISION)
	{
	  value->domain.char_info.length = TP_FLOATING_PRECISION_VALUE;
	}
      else
	{
	  value->domain.char_info.length = precision;
	}
      if (IS_INVALID_PRECISION (precision, DB_MAX_BIT_PRECISION))
	{
	  error = ER_INVALID_PRECISION;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_INVALID_PRECISION, 3, precision, 0,
		  DB_MAX_BIT_PRECISION);
	  value->domain.char_info.length = TP_FLOATING_PRECISION_VALUE;
	}
      break;

    case DB_TYPE_VARBIT:
      if (precision == DB_DEFAULT_PRECISION)
	{
	  value->domain.char_info.length = DB_MAX_VARBIT_PRECISION;
	}
      else
	{
	  value->domain.char_info.length = precision;
	}
      if (IS_INVALID_PRECISION (precision, DB_MAX_VARBIT_PRECISION))
	{
	  error = ER_INVALID_PRECISION;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_INVALID_PRECISION, 3, precision, 0,
		  DB_MAX_VARBIT_PRECISION);
	  value->domain.char_info.length = DB_MAX_VARBIT_PRECISION;
	}
      break;

    case DB_TYPE_CHAR:
      if (precision == DB_DEFAULT_PRECISION)
	{
	  value->domain.char_info.length = TP_FLOATING_PRECISION_VALUE;
	}
      else
	{
	  value->domain.char_info.length = precision;
	}
      if (IS_INVALID_PRECISION (precision, DB_MAX_CHAR_PRECISION))
	{
	  error = ER_INVALID_PRECISION;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_INVALID_PRECISION, 3, precision, 0,
		  DB_MAX_CHAR_PRECISION);
	  value->domain.char_info.length = TP_FLOATING_PRECISION_VALUE;
	}
      break;

    case DB_TYPE_NCHAR:
      if (precision == DB_DEFAULT_PRECISION)
	{
	  value->domain.char_info.length = TP_FLOATING_PRECISION_VALUE;
	}
      else
	{
	  value->domain.char_info.length = precision;
	}
      if (IS_INVALID_PRECISION (precision, DB_MAX_NCHAR_PRECISION))
	{
	  error = ER_INVALID_PRECISION;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_INVALID_PRECISION, 3, precision, 0,
		  DB_MAX_NCHAR_PRECISION);
	  value->domain.char_info.length = TP_FLOATING_PRECISION_VALUE;
	}
      break;

    case DB_TYPE_VARCHAR:
      if (precision == DB_DEFAULT_PRECISION)
	{
	  value->domain.char_info.length = DB_MAX_VARCHAR_PRECISION;
	}
      else
	{
	  value->domain.char_info.length = precision;
	}
      if (IS_INVALID_PRECISION (precision, DB_MAX_VARCHAR_PRECISION))
	{
	  error = ER_INVALID_PRECISION;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_INVALID_PRECISION, 3, precision, 0,
		  DB_MAX_VARCHAR_PRECISION);
	  value->domain.char_info.length = DB_MAX_VARCHAR_PRECISION;
	}
      break;

    case DB_TYPE_VARNCHAR:
      if (precision == DB_DEFAULT_PRECISION)
	{
	  value->domain.char_info.length = DB_MAX_VARNCHAR_PRECISION;
	}
      else
	{
	  value->domain.char_info.length = precision;
	}
      if (IS_INVALID_PRECISION (precision, DB_MAX_VARNCHAR_PRECISION))
	{
	  error = ER_INVALID_PRECISION;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_INVALID_PRECISION, 3, precision, 0,
		  DB_MAX_VARNCHAR_PRECISION);
	  value->domain.char_info.length = DB_MAX_VARNCHAR_PRECISION;
	}
      break;

    case DB_TYPE_NULL:
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
    case DB_TYPE_FLOAT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_OBJECT:
    case DB_TYPE_SET:
    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
    case DB_TYPE_MIDXKEY:
    case DB_TYPE_ELO:
    case DB_TYPE_TIME:
    case DB_TYPE_TIMESTAMP:
    case DB_TYPE_DATETIME:
    case DB_TYPE_DATE:
    case DB_TYPE_MONETARY:
    case DB_TYPE_VARIABLE:
    case DB_TYPE_SUB:
    case DB_TYPE_POINTER:
    case DB_TYPE_ERROR:
    case DB_TYPE_SHORT:
    case DB_TYPE_VOBJ:
    case DB_TYPE_OID:
      break;

    default:
      error = ER_UCI_INVALID_DATA_TYPE;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_UCI_INVALID_DATA_TYPE,
	      0);
      break;
    }

  return error;
}

/*
 * db_value_domain_min() - Initialize value(db_value_init)
 *                         and set to the minimum value of the domain.
 * return : Error indicator.
 * value(in/out) : Pointer to a DB_VALUE.
 * type(in)      : type.
 * precision(in) : precision.
 * scale(in)     : scale.
 */
int
db_value_domain_min (DB_VALUE * value, const DB_TYPE type,
		     const int precision, const int scale)
{
  int error;

  error = db_value_domain_init (value, type, precision, scale);
  if (error != NO_ERROR)
    {
      return error;
    }

  switch (type)
    {
    case DB_TYPE_NULL:
      break;
    case DB_TYPE_INTEGER:
      value->data.i = DB_INT32_MIN;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_BIGINT:
      value->data.bigint = DB_BIGINT_MIN;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_FLOAT:
      value->data.f = FLT_MIN;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_DOUBLE:
      value->data.d = DBL_MIN;
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_OBJECT: not in server-side code */
    case DB_TYPE_SET:
    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
      value->data.set = NULL;
      value->domain.general_info.is_null = 1;	/* NULL SET value */
      break;
    case DB_TYPE_ELO:
      value->data.elo = NULL;
      value->domain.general_info.is_null = 1;	/* NULL ELO value */
      break;
    case DB_TYPE_TIME:
      value->data.time = DB_TIME_MIN;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_TIMESTAMP:
      value->data.utime = 0 /*DB_UTIME_MIN */ ;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_DATETIME:
      value->data.datetime.date = DB_DATE_MIN;
      value->data.datetime.time = DB_TIME_MIN;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_DATE:
      value->data.date = DB_DATE_MIN;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_MONETARY:
      value->data.money.amount = DBL_MIN;
      value->data.money.type = DB_CURRENCY_DEFAULT;
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_VARIABLE: internal use only */
      /* case DB_TYPE_SUB: internal use only */
      /* case DB_TYPE_POINTER: method arguments only */
      /* case DB_TYPE_ERROR: method arguments only */
    case DB_TYPE_SHORT:
      value->data.sh = DB_INT16_MIN;
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_VOBJ: internal use only */
    case DB_TYPE_OID:
      value->data.oid.pageid = NULL_PAGEID;
      value->data.oid.slotid = NULL_PAGEID;
      value->data.oid.volid = NULL_PAGEID;
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_DB_VALUE: special for esql */
    case DB_TYPE_NUMERIC:
      {
	char str[DB_MAX_NUMERIC_PRECISION + 2];

	memset (str, 0, DB_MAX_NUMERIC_PRECISION + 2);
	str[0] = '-';
	memset (str + 1, '9', value->domain.numeric_info.precision);
	numeric_coerce_dec_str_to_num (str, value->data.num.d.buf);
	value->domain.general_info.is_null = 0;
      }
      break;
    case DB_TYPE_BIT:
    case DB_TYPE_VARBIT:
      value->data.ch.info.style = MEDIUM_STRING;
      value->data.ch.info.codeset = INTL_CODESET_RAW_BITS;
      value->data.ch.medium.size = 1;
      value->data.ch.medium.buf = (char *) "\0";	/* zero; 0 */
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_STRING: internally DB_TYPE_VARCHAR */
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
      value->data.ch.info.style = MEDIUM_STRING;
      value->data.ch.info.codeset = INTL_CODESET_ISO88591;
      value->data.ch.medium.size = 1;
      value->data.ch.medium.buf = (char *) "\40";	/* space; 32 */
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      value->data.ch.info.style = MEDIUM_STRING;
      value->data.ch.info.codeset = lang_charset ();
      value->data.ch.medium.size = 1;
      value->data.ch.medium.buf = (char *) "\40";	/* space; 32 */
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_TABLE: internal use only */
    default:
      error = ER_UCI_INVALID_DATA_TYPE;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_UCI_INVALID_DATA_TYPE, 0);
      break;
    }

  return error;
}

/*
 * db_value_domain_max() - Initialize value(db_value_init)
 *                         and set to the maximum value of the domain.
 * return : Error indicator.
 * value(in/out) : Pointer to a DB_VALUE.
 * type(in)      : type.
 * precision(in) : precision.
 * scale(in)     : scale.
 */
int
db_value_domain_max (DB_VALUE * value, const DB_TYPE type,
		     const int precision, const int scale)
{
  int error;

  error = db_value_domain_init (value, type, precision, scale);
  if (error != NO_ERROR)
    {
      return error;
    }

  switch (type)
    {
    case DB_TYPE_NULL:
      break;
    case DB_TYPE_INTEGER:
      value->data.i = DB_INT32_MAX;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_BIGINT:
      value->data.bigint = DB_BIGINT_MAX;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_FLOAT:
      value->data.f = FLT_MAX;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_DOUBLE:
      value->data.d = DBL_MAX;
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_OBJECT: not in server-side code */
    case DB_TYPE_SET:
    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
      value->data.set = NULL;
      value->domain.general_info.is_null = 1;	/* NULL SET value */
      break;
    case DB_TYPE_ELO:
      value->data.elo = NULL;
      value->domain.general_info.is_null = 1;	/* NULL ELO value */
      break;
    case DB_TYPE_TIME:
      value->data.time = DB_TIME_MAX;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_TIMESTAMP:
      value->data.utime = DB_UTIME_MAX;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_DATETIME:
      value->data.datetime.date = DB_DATE_MAX;
      value->data.datetime.time = DB_TIME_MAX;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_DATE:
      value->data.date = DB_DATE_MAX;
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_MONETARY:
      value->data.money.amount = DBL_MAX;
      value->data.money.type = DB_CURRENCY_DEFAULT;
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_VARIABLE: internal use only */
      /* case DB_TYPE_SUB: internal use only */
      /* case DB_TYPE_POINTER: method arguments only */
      /* case DB_TYPE_ERROR: method arguments only */
    case DB_TYPE_SHORT:
      value->data.sh = DB_INT16_MAX;
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_VOBJ: internal use only */
    case DB_TYPE_OID:
      value->data.oid.pageid = DB_INT32_MAX;
      value->data.oid.slotid = DB_INT16_MAX;
      value->data.oid.volid = DB_INT16_MAX;
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_DB_VALUE: special for esql */
    case DB_TYPE_NUMERIC:
      {
	char str[DB_MAX_NUMERIC_PRECISION + 1];

	memset (str, 0, DB_MAX_NUMERIC_PRECISION + 1);
	memset (str, '9', value->domain.numeric_info.precision);
	numeric_coerce_dec_str_to_num (str, value->data.num.d.buf);
	value->domain.general_info.is_null = 0;
      }
      break;
    case DB_TYPE_BIT:
    case DB_TYPE_VARBIT:
      value->data.ch.info.style = MEDIUM_STRING;
      value->data.ch.info.codeset = INTL_CODESET_RAW_BITS;
      value->data.ch.medium.size = 1;
      value->data.ch.medium.buf = (char *) "\377";	/* 255 */
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_STRING: internally DB_TYPE_VARCHAR */
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
      value->data.ch.info.style = MEDIUM_STRING;
      value->data.ch.info.codeset = INTL_CODESET_ISO88591;
      value->data.ch.medium.size = 1;
      value->data.ch.medium.buf = (char *) "\377";	/* 255 */
      value->domain.general_info.is_null = 0;
      break;
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      value->data.ch.info.style = MEDIUM_STRING;
      value->data.ch.info.codeset = lang_charset ();
      value->data.ch.medium.size = 1;
      value->data.ch.medium.buf = (char *) "\377";	/* 255 */
      value->domain.general_info.is_null = 0;
      break;
      /* case DB_TYPE_TABLE: internal use only */
    default:
      error = ER_UCI_INVALID_DATA_TYPE;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_UCI_INVALID_DATA_TYPE, 0);
      break;
    }

  return error;
}

/*
 * db_string_truncate() - truncate string in DB_TYPE_STRING value container
 * return         : Error indicator.
 * value(in/out)  : Pointer to a DB_VALUE
 * precision(in)  : value's precision after truncate.
 */
int
db_string_truncate (DB_VALUE * value, const int precision)
{
  int error = NO_ERROR;
  DB_VALUE src_value;
  char *string, *val_str;
  int length;

  switch (DB_VALUE_TYPE (value))
    {
    case DB_TYPE_STRING:
      if (DB_GET_STRING_LENGTH (value) > precision)
	{
	  string = (char *) malloc (precision);
	  if (string == NULL)
	    {
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, precision);
	    }
	  else
	    {
	      val_str = DB_GET_STRING (value);
	      if (val_str != NULL)
		{
		  strncpy (string, val_str, precision);
		  db_make_varchar (&src_value, precision, string, precision);

		  pr_clear_value (value);
		  (*(tp_String.setval)) (value, &src_value, true);

		  pr_clear_value (&src_value);
		}

	      free (string);
	    }
	}
      break;

    case DB_TYPE_CHAR:
      val_str = DB_GET_CHAR (value, &length);
      if (length > precision)
	{
	  string = (char *) malloc (precision);
	  if (string == NULL)
	    {
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, precision);
	    }
	  else
	    {
	      if (val_str != NULL)
		{
		  strncpy (string, val_str, precision);
		  db_make_char (&src_value, precision, string, precision);

		  pr_clear_value (value);
		  (*(tp_Char.setval)) (value, &src_value, true);

		  pr_clear_value (&src_value);
		}

	      free (string);
	    }
	}
      break;

    case DB_TYPE_VARNCHAR:
      val_str = DB_GET_NCHAR (value, &length);
      if (length > precision)
	{
	  string = (char *) malloc (precision);
	  if (string == NULL)
	    {
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, precision);
	    }
	  else
	    {
	      if (val_str != NULL)
		{
		  strncpy (string, val_str, precision);
		  db_make_varnchar (&src_value, precision, string, precision);

		  pr_clear_value (value);
		  (*(tp_VarNChar.setval)) (value, &src_value, true);

		  pr_clear_value (&src_value);
		}

	      free (string);
	    }
	}
      break;

    case DB_TYPE_NCHAR:
      val_str = DB_GET_NCHAR (value, &length);
      if (length > precision)
	{
	  string = (char *) malloc (precision);
	  if (string == NULL)
	    {
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, precision);
	    }
	  else
	    {
	      if (val_str != NULL)
		{
		  strncpy (string, val_str, precision);
		  db_make_nchar (&src_value, precision, string, precision);

		  pr_clear_value (value);
		  (*(tp_NChar.setval)) (value, &src_value, true);

		  pr_clear_value (&src_value);
		}

	      free (string);
	    }
	}
      break;

    case DB_TYPE_BIT:
      val_str = DB_GET_BIT (value, &length);
      length = (length + 7) >> 3;
      if (length > precision)
	{
	  string = (char *) malloc (precision);
	  if (string == NULL)
	    {
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, precision);
	    }
	  else
	    {
	      if (val_str != NULL)
		{
		  memcpy (string, val_str, precision);
		  db_make_bit (&src_value, precision << 3, string,
			       precision << 3);

		  pr_clear_value (value);
		  (*(tp_Bit.setval)) (value, &src_value, true);

		  pr_clear_value (&src_value);
		}

	      free (string);
	    }
	}
      break;

    case DB_TYPE_VARBIT:
      val_str = DB_GET_BIT (value, &length);
      length = (length >> 3) + ((length & 7) ? 1 : 0);
      if (length > precision)
	{
	  string = (char *) malloc (precision);
	  if (string == NULL)
	    {
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, precision);
	    }
	  else
	    {
	      if (val_str != NULL)
		{
		  memcpy (string, val_str, precision);
		  db_make_varbit (&src_value, precision << 3, string,
				  precision << 3);

		  pr_clear_value (value);
		  (*(tp_VarBit.setval)) (value, &src_value, true);

		  pr_clear_value (&src_value);
		}

	      free (string);
	    }
	}
      break;

    case DB_TYPE_NULL:
      break;
    default:
      {
	error = ER_UCI_INVALID_DATA_TYPE;
	er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		ER_UCI_INVALID_DATA_TYPE, 0);
      }
      break;
    }

  return error;
}

/*
 * db_value_domain_type() - get the type of value's domain.
 * return     : DB_TYPE of value's domain
 * value(in)  : Pointer to a DB_VALUE
 */
DB_TYPE
db_value_domain_type (const DB_VALUE * value)
{
  CHECK_1ARG_UNKNOWN (value);
  return (DB_TYPE) value->domain.general_info.type;
}

/*
 * db_value_type()
 * return     : DB_TYPE of value's domain or DB_TYPE_NULL
 * value(in)  : Pointer to a DB_VALUE
 */
DB_TYPE
db_value_type (const DB_VALUE * value)
{
  CHECK_1ARG_UNKNOWN (value);

  if (value->domain.general_info.is_null)
    {
      return DB_TYPE_NULL;
    }
  else
    {
      return (DB_TYPE) value->domain.general_info.type;
    }
}

/*
 * db_value_precision() - get the precision of value.
 * return     : precision of given value.
 * value(in)  : Pointer to a DB_VALUE.
 */
int
db_value_precision (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  switch (value->domain.general_info.type)
    {
    case DB_TYPE_NUMERIC:
      return value->domain.numeric_info.precision;
    case DB_TYPE_BIT:
    case DB_TYPE_VARBIT:
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      return value->domain.char_info.length;
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
    case DB_TYPE_FLOAT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_OBJECT:
    case DB_TYPE_SET:
    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
    case DB_TYPE_ELO:
    case DB_TYPE_TIME:
    case DB_TYPE_UTIME:
    case DB_TYPE_DATE:
    case DB_TYPE_MONETARY:
    case DB_TYPE_VARIABLE:
    case DB_TYPE_SUB:
    case DB_TYPE_POINTER:
    case DB_TYPE_ERROR:
    case DB_TYPE_SHORT:
    case DB_TYPE_VOBJ:
    case DB_TYPE_OID:
    default:
      return 0;
    }
}

/*
 * db_value_scale() - get the scale of value.
 * return     : scale of given value.
 * value(in)  : Pointer to a DB_VALUE.
 */
int
db_value_scale (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  if (value->domain.general_info.type == DB_TYPE_NUMERIC)
    {
      return value->domain.numeric_info.scale;
    }
  else
    {
      return 0;
    }
}

/*
 * db_value_type_is_collection() -
 * return :
 * value(in) :
 */
bool
db_value_type_is_collection (const DB_VALUE * value)
{
  bool is_collection;
  DB_TYPE type;

  CHECK_1ARG_FALSE (value);

  type = db_value_type (value);
  is_collection = (type == DB_TYPE_SET || type == DB_TYPE_MULTISET
		   || type == DB_TYPE_SEQUENCE || type == DB_TYPE_VOBJ);
  return is_collection;
}

/*
 * db_value_is_null() -
 * return :
 * value(in) :
 */
bool
db_value_is_null (const DB_VALUE * value)
{
  CHECK_1ARG_TRUE (value);

  return (value->domain.general_info.is_null != 0);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_value_eh_key() -
 * return :
 * value(in) :
 */
void *
db_value_eh_key (DB_VALUE * value)
{
  DB_OBJECT *obj;

  CHECK_1ARG_NULL (value);

  switch (value->domain.general_info.type)
    {
    case DB_TYPE_STRING:
      return DB_GET_STRING (value);
    case DB_TYPE_OBJECT:
      obj = DB_GET_OBJECT (value);
      if (obj == NULL)
	{
	  return NULL;
	}
      else
	{
	  return WS_OID (obj);
	}
    default:
      return &value->data;
    }
}

/*
 * db_value_put_db_data()
 * return     : Error indicator.
 * value(in/out) : Pointer to a DB_VALUE to set data.
 * data(in)      : Pointer to a DB_DATA.
 */
int
db_value_put_db_data (DB_VALUE * value, const DB_DATA * data)
{
  CHECK_2ARGS_ERROR (value, data);

  value->data = *data;		/* structure copy */
  return NO_ERROR;
}
#endif

/*
 * db_value_get_db_data()
 * return      : DB_DATA of value container.
 * value(in)   : Pointer to a DB_VALUE.
 */
DB_DATA *
db_value_get_db_data (DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  return &value->data;
}

/*
 * db_value_alter_type() - change the type of given value container.
 * return         : Error indicator.
 * value(in/out)  : Pointer to a DB_VALUE.
 * type(in)       : new type.
 */
int
db_value_alter_type (DB_VALUE * value, const DB_TYPE type)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = type;
  return NO_ERROR;
}

/*
 *  db_value_put() -
 *
 *  return: an error indicator
 *     ER_DB_UNSUPPORTED_CONVERSION -
 *          The C type to DB type conversion is not supported.
 *
 *     ER_OBJ_VALUE_CONVERSION_ERROR -
 *         An error occurred while performing the requested conversion.
 *
 *     ER_OBJ_INVALID_ARGUMENTS - The value pointer is NULL.
 *
 *  value(out)      : Pointer to a DB_VALUE.  The value container will need
 *                    to be initialized prior to entry as explained below.
 *  c_type(in)      : The type of the C destination buffer (and, therefore, an
 *                    indication of the type of coercion desired)
 *  input(in)       : Pointer to a C buffer
 *  input_length(in): The length of the buffer.  The buffer length is measured
 *                    in bit for C types DB_C_BIT and DB_C_VARBIT and is
 *                    measured in bytes for all other types.
 *
 */
int
db_value_put (DB_VALUE * value, const DB_TYPE_C c_type, void *input,
	      const int input_length)
{
  int error_code = NO_ERROR;
  int status = C_TO_VALUE_NOERROR;

  if ((value == NULL) || (input == NULL))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }
  else if (input_length == -1)
    {
      db_make_null (value);
      return NO_ERROR;
    }

  switch (c_type)
    {
    case DB_TYPE_C_CHAR:
    case DB_TYPE_C_VARCHAR:
    case DB_TYPE_C_NCHAR:
    case DB_TYPE_C_VARNCHAR:
      status = coerce_char_to_dbvalue (value, (char *) input, input_length);
      break;
    case DB_TYPE_C_INT:
    case DB_TYPE_C_SHORT:
    case DB_TYPE_C_LONG:
    case DB_TYPE_C_FLOAT:
    case DB_TYPE_C_DOUBLE:
    case DB_TYPE_C_MONETARY:
      status = coerce_numeric_to_dbvalue (value, (char *) input, c_type);
      break;
    case DB_TYPE_C_BIT:
    case DB_TYPE_C_VARBIT:
      status = coerce_binary_to_dbvalue (value, (char *) input, input_length);
      break;
    case DB_TYPE_C_DATE:
      status = coerce_date_to_dbvalue (value, (char *) input);
      break;
    case DB_TYPE_C_TIME:
      status = coerce_time_to_dbvalue (value, (char *) input);
      break;
    case DB_TYPE_C_TIMESTAMP:
      status = coerce_timestamp_to_dbvalue (value, (char *) input);
      break;
    case DB_TYPE_C_DATETIME:
      status = coerce_datetime_to_dbvalue (value, (char *) input);
      break;
    case DB_TYPE_C_OBJECT:
      if (DB_VALUE_DOMAIN_TYPE (value) == DB_TYPE_OBJECT)
	{
	  db_make_object (value, *(DB_C_OBJECT **) input);
	}
      else
	{
	  status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
	}
      break;
    case DB_TYPE_C_SET:
      switch (DB_VALUE_DOMAIN_TYPE (value))
	{
	case DB_TYPE_SET:
	  db_make_set (value, *(DB_C_SET **) input);
	  break;
	case DB_TYPE_MULTISET:
	  db_make_multiset (value, *(DB_C_SET **) input);
	  break;
	case DB_TYPE_SEQUENCE:
	  db_make_sequence (value, *(DB_C_SET **) input);
	  break;
	default:
	  status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
	  break;
	}
      break;
    default:
      status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
      break;
    }

  if (status == C_TO_VALUE_UNSUPPORTED_CONVERSION)
    {
      error_code = ER_DB_UNSUPPORTED_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_DB_UNSUPPORTED_CONVERSION, 1, "db_value_put");
    }
  else if (status == C_TO_VALUE_CONVERSION_ERROR)
    {
      error_code = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  return error_code;
}

/*
 * db_make_null() -
 * return :
 * value(out) :
 */
int
db_make_null (DB_VALUE * value)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_NULL;
  value->domain.general_info.is_null = 1;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_int() -
 * return :
 * value(out) :
 * num(in):
 */
int
db_make_int (DB_VALUE * value, const int num)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_INTEGER;
  value->data.i = num;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_short() -
 * return :
 * value(out) :
 * num(in) :
 */
int
db_make_short (DB_VALUE * value, const short num)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_SHORT;
  value->data.sh = num;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_bigint() -
 * return :
 * value(out) :
 * num(in) :
 */
int
db_make_bigint (DB_VALUE * value, const DB_BIGINT num)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_BIGINT;
  value->data.bigint = num;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_float() -
 * return :
 * value(out) :
 * num(in):
 */
int
db_make_float (DB_VALUE * value, const float num)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_FLOAT;
  value->data.f = num;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_double() -
 * return :
 * value(out) :
 * num(in):
 */
int
db_make_double (DB_VALUE * value, const double num)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_DOUBLE;
  value->data.d = num;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_numeric() -
 * return :
 * value(out) :
 * num(in):
 * precision(in):
 * scale(in):
 */
int
db_make_numeric (DB_VALUE * value, const DB_C_NUMERIC num,
		 const int precision, const int scale)
{
  int error = NO_ERROR;

  CHECK_1ARG_ERROR (value);

  error = db_value_domain_init (value, DB_TYPE_NUMERIC, precision, scale);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (num)
    {
      value->domain.general_info.is_null = 0;
      memcpy (value->data.num.d.buf, num, DB_NUMERIC_BUF_SIZE);
    }
  else
    {
      value->domain.general_info.is_null = 1;
    }
  return error;
}

/*
 * db_make_db_char() -
 * return :
 * value(out) :
 * codeset(in):
 * str(in):
 * size(in):
 */
int
db_make_db_char (DB_VALUE * value, const INTL_CODESET codeset,
		 const char *str, const int size)
{
  int error = NO_ERROR;
  bool is_char_type;

  CHECK_1ARG_ERROR (value);

  is_char_type = (value->domain.general_info.type == DB_TYPE_VARCHAR
		  || value->domain.general_info.type == DB_TYPE_CHAR
		  || value->domain.general_info.type == DB_TYPE_NCHAR
		  || value->domain.general_info.type == DB_TYPE_VARNCHAR
		  || value->domain.general_info.type == DB_TYPE_BIT
		  || value->domain.general_info.type == DB_TYPE_VARBIT);

  if (is_char_type)
    {
#if 0
      if (size <= DB_SMALL_CHAR_BUF_SIZE)
	{
	  value->data.ch.info.style = SMALL_STRING;
	  value->data.ch.sm.codeset = codeset;
	  value->data.ch.sm.size = size;
	  memcpy (value->data.ch.sm.buf, str, size);
	}
      else
#endif
      if (size <= DB_MAX_STRING_LENGTH)
	{
	  value->data.ch.info.style = MEDIUM_STRING;
	  value->data.ch.info.codeset = codeset;
	  /*
	   * If size is set to the default, and the type is any
	   * kind of character string, assume the string is NULL
	   * terminated.
	   */
	  if (size == DB_DEFAULT_STRING_LENGTH
	      && QSTR_IS_ANY_CHAR (value->domain.general_info.type))
	    {
	      value->data.ch.medium.size = str ? strlen (str) : 0;
	    }
	  else if (size < 0)
	    {
	      error = ER_QSTR_BAD_LENGTH;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_QSTR_BAD_LENGTH, 1, size);
	    }
	  else
	    {
	      /* We need to ensure that we don't exceed the max size
	       * for the char value specified in the domain.
	       */
	      if (value->domain.char_info.length ==
		  TP_FLOATING_PRECISION_VALUE)
		{
		  value->data.ch.medium.size = size;
		}
	      else
		{
		  value->data.ch.medium.size = MIN (size,
						    value->domain.char_info.
						    length);
		}
	    }
	  value->data.ch.medium.buf = (char *) str;
	}
      else
	{
	  /* case LARGE_STRING: Currently Not Implemented */
	}

      if (str)
	{
	  value->domain.general_info.is_null = 0;
	}
      else
	{
	  value->domain.general_info.is_null = 1;
	}

      if (PRM_ORACLE_STYLE_EMPTY_STRING)
	{
	  if (size == 0)
	    {
	      value->domain.general_info.is_null = 1;
	    }
	}

      value->need_clear = false;
    }
  else
    {
      error = ER_QPROC_INVALID_DATATYPE;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE,
	      0);
    }
  return error;
}

/*
 * db_make_bit() -
 * return :
 * value(out) :
 * bit_length(in):
 * bit_str(in):
 * bit_str_bit_size(in):
 */
int
db_make_bit (DB_VALUE * value, const int bit_length,
	     const DB_C_BIT bit_str, const int bit_str_bit_size)
{
  int error;

  CHECK_1ARG_ERROR (value);

  error = db_value_domain_init (value, DB_TYPE_BIT, bit_length, 0);
  if (error != NO_ERROR)
    {
      return error;
    }

  error =
    db_make_db_char (value, INTL_CODESET_RAW_BITS, bit_str, bit_str_bit_size);
  return error;
}

/*
 * db_make_varbit() -
 * return :
 * value(out) :
 * max_bit_length(in):
 * bit_str(in):
 * bit_str_bit_size(in):
 */
int
db_make_varbit (DB_VALUE * value, const int max_bit_length,
		const DB_C_BIT bit_str, const int bit_str_bit_size)
{
  int error;

  CHECK_1ARG_ERROR (value);

  error = db_value_domain_init (value, DB_TYPE_VARBIT, max_bit_length, 0);
  if (error != NO_ERROR)
    {
      return error;
    }

  error =
    db_make_db_char (value, INTL_CODESET_RAW_BITS, bit_str, bit_str_bit_size);

  return error;
}

/*
 * db_make_string() -
 * return :
 * value(out) :
 * str(in):
 */
int
db_make_string (DB_VALUE * value, const char *str)
{
  int error;
  int size;

  CHECK_1ARG_ERROR (value);

  error = db_value_domain_init (value, DB_TYPE_VARCHAR,
				TP_FLOATING_PRECISION_VALUE, 0);
  if (error == NO_ERROR)
    {
      if (str)
	{
	  size = strlen (str);
	}
      else
	{
	  size = 0;
	}
      error = db_make_db_char (value, INTL_CODESET_ISO88591, str, size);
    }
  return error;
}

/*
 * db_make_char() -
 * return :
 * value(out) :
 * char_length(in):
 * str(in):
 * char_str_byte_size(in):
 */
int
db_make_char (DB_VALUE * value, const int char_length,
	      const DB_C_CHAR str, const int char_str_byte_size)
{
  int error;

  CHECK_1ARG_ERROR (value);

  error = db_value_domain_init (value, DB_TYPE_CHAR, char_length, 0);
  if (error == NO_ERROR)
    {
      error = db_make_db_char (value, INTL_CODESET_ISO88591, str,
			       char_str_byte_size);
    }

  return error;
}

/*
 * db_make_varchar() -
 * return :
 * value(out) :
 * max_char_length(in):
 * str(in):
 * char_str_byte_size(in):
 */
int
db_make_varchar (DB_VALUE * value, const int max_char_length,
		 const DB_C_CHAR str, const int char_str_byte_size)
{
  int error;

  CHECK_1ARG_ERROR (value);

  error = db_value_domain_init (value, DB_TYPE_VARCHAR, max_char_length, 0);
  if (error == NO_ERROR)
    {
      error = db_make_db_char (value, INTL_CODESET_ISO88591, str,
			       char_str_byte_size);
    }

  return error;
}

/*
 * db_make_nchar() -
 * return :
 * value(out) :
 * nchar_length(in):
 * str(in):
 * nchar_str_byte_size(in):
 */
int
db_make_nchar (DB_VALUE * value, const int nchar_length,
	       const DB_C_NCHAR str, const int nchar_str_byte_size)
{
  int error;

  CHECK_1ARG_ERROR (value);

  error = db_value_domain_init (value, DB_TYPE_NCHAR, nchar_length, 0);
  if (error == NO_ERROR)
    {
      error =
	db_make_db_char (value, lang_charset (), str, nchar_str_byte_size);
    }

  return error;
}

/*
 * db_make_varnchar() -
 * return :
 * value(out) :
 * max_nchar_length(in):
 * str(in):
 * nchar_str_byte_size(in):
 */
int
db_make_varnchar (DB_VALUE * value, const int max_nchar_length,
		  const DB_C_NCHAR str, const int nchar_str_byte_size)
{
  int error;

  CHECK_1ARG_ERROR (value);

  error = db_value_domain_init (value, DB_TYPE_VARNCHAR, max_nchar_length, 0);
  if (error == NO_ERROR)
    {
      error
	= db_make_db_char (value, lang_charset (), str, nchar_str_byte_size);
    }

  return error;
}

/*
 * db_make_object() -
 * return :
 * value(out) :
 * obj(in):
 */
int
db_make_object (DB_VALUE * value, DB_OBJECT * obj)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_OBJECT;
  value->data.op = obj;
  if (obj)
    {
      value->domain.general_info.is_null = 0;
    }
  else
    {
      value->domain.general_info.is_null = 1;
    }

  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_set() -
 * return :
 * value(out) :
 * set(in):
 */
int
db_make_set (DB_VALUE * value, DB_SET * set)
{
  int error = NO_ERROR;

  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_SET;
  value->data.set = set;
  if (set)
    {
      if ((set->set && setobj_type (set->set) == DB_TYPE_SET)
	  || set->disk_set)
	{
	  value->domain.general_info.is_null = 0;
	}
      else
	{
	  error = ER_QPROC_INVALID_DATATYPE;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_INVALID_DATATYPE, 0);
	}
    }
  else
    {
      value->domain.general_info.is_null = 1;
    }

  value->need_clear = false;

  return error;
}

/*
 * db_make_multiset() -
 * return :
 * value(out) :
 * set(in):
 */
int
db_make_multiset (DB_VALUE * value, DB_SET * set)
{
  int error = NO_ERROR;

  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_MULTISET;
  value->data.set = set;
  if (set)
    {
      if ((set->set && setobj_type (set->set) == DB_TYPE_MULTISET)
	  || set->disk_set)
	{
	  value->domain.general_info.is_null = 0;
	}
      else
	{
	  error = ER_QPROC_INVALID_DATATYPE;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_INVALID_DATATYPE, 0);
	}
    }
  else
    {
      value->domain.general_info.is_null = 1;
    }

  value->need_clear = false;

  return error;
}

/*
 * db_make_sequence() -
 * return :
 * value(out) :
 * set(in):
 */
int
db_make_sequence (DB_VALUE * value, DB_SET * set)
{
  int error = NO_ERROR;

  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_SEQUENCE;
  value->data.set = set;
  if (set)
    {
      if ((set->set && setobj_type (set->set) == DB_TYPE_SEQUENCE)
	  || set->disk_set)
	{
	  value->domain.general_info.is_null = 0;
	}
      else
	{
	  error = ER_QPROC_INVALID_DATATYPE;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_INVALID_DATATYPE, 0);
	}
    }
  else
    {
      value->domain.general_info.is_null = 1;
    }

  value->need_clear = false;

  return error;
}

/*
 * db_make_collection() -
 * return :
 * value(out) :
 * col(in):
 */
int
db_make_collection (DB_VALUE * value, DB_COLLECTION * col)
{
  int error = NO_ERROR;

  CHECK_1ARG_ERROR (value);

  /* Rather than being DB_TYPE_COLLECTION, the value type is taken from the
     base type of the collection. */
  if (col == NULL)
    {
      value->domain.general_info.type = DB_TYPE_SEQUENCE;	/* undefined */
      value->data.set = NULL;
      value->domain.general_info.is_null = 1;
    }
  else
    {
      value->domain.general_info.type = db_col_type (col);
      value->data.set = col;
      /* note, we have been testing set->set for non-NULL here in order to set
         the is_null flag, this isn't appropriate, the set pointer can be NULL
         if the set has been swapped out.The existance of a set handle alone
         determines the nullness of the value.  Actually, the act of calling
         db_col_type above will have resulted in a re-fetch of the referenced
         set if it had been swapped out. */
      value->domain.general_info.is_null = 0;
    }
  value->need_clear = false;

  return error;
}

/*
 * db_make_midxkey() -
 * return :
 * value(out) :
 * midxkey(in):
 */
int
db_make_midxkey (DB_VALUE * value, DB_MIDXKEY * midxkey)
{
  int error = NO_ERROR;

  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_MIDXKEY;

  if (midxkey == NULL)
    {
      value->domain.general_info.is_null = 1;
      value->data.midxkey.ncolumns = -1;
      value->data.midxkey.domain = NULL;
      value->data.midxkey.size = 0;
      value->data.midxkey.buf = NULL;
    }
  else
    {
      value->domain.general_info.is_null = 0;
      value->data.midxkey = *midxkey;
    }

  value->need_clear = false;

  return error;
}

/*
 * db_make_pointer() -
 * return :
 * value(out) :
 * ptr(in):
 */
int
db_make_pointer (DB_VALUE * value, void *ptr)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_POINTER;
  value->data.p = ptr;
  if (ptr)
    {
      value->domain.general_info.is_null = 0;
    }
  else
    {
      value->domain.general_info.is_null = 1;
    }
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_time() -
 * return :
 * value(out) :
 * hour(in):
 * min(in):
 * sec(in):
 */
int
db_make_time (DB_VALUE * value, const int hour, const int min, const int sec)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_TIME;
  db_time_encode (&value->data.time, hour, min, sec);
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_value_put_encoded_time() -
 * return :
 * value(out):
 * time(in):
 */
int
db_value_put_encoded_time (DB_VALUE * value, const DB_TIME * time)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_TIME;
  if (time)
    {
      value->data.time = *time;
    }
  else
    {
      db_time_encode (&value->data.time, 0, 0, 0);
    }
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_date() -
 * return :
 * value(out):
 * mon(in):
 * day(in):
 * year(in):
 */
int
db_make_date (DB_VALUE * value, const int mon, const int day, const int year)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_DATE;
  db_date_encode (&value->data.date, mon, day, year);
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_value_put_encoded_date() -
 * return :
 * value(out):
 * date(in):
 */
int
db_value_put_encoded_date (DB_VALUE * value, const DB_DATE * date)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_DATE;
  if (date)
    {
      value->data.date = *date;
    }
  else
    {
      db_date_encode (&value->data.date, 0, 0, 0);
    }
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_monetary() -
 * return :
 * value(out):
 * type(in):
 * amount(in):
 */
int
db_make_monetary (DB_VALUE * value,
		  const DB_CURRENCY type, const double amount)
{
  int error;

  CHECK_1ARG_ERROR (value);

  /* check for valid currency type
     don't put default case in the switch!!! */
  error = ER_INVALID_CURRENCY_TYPE;
  switch (type)
    {
    case DB_CURRENCY_DOLLAR:
    case DB_CURRENCY_WON:
    case DB_CURRENCY_YUAN:
      error = NO_ERROR;		/* it's a type we expect */
      break;
    default:
      break;
    }

  if (error != NO_ERROR)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, type);
      return error;
    }

  value->domain.general_info.type = DB_TYPE_MONETARY;
  value->data.money.type = type;
  value->data.money.amount = amount;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return error;
}

/*
 * db_value_put_monetary_currency() -
 * return :
 * value(out):
 * type(in):
 */
int
db_value_put_monetary_currency (DB_VALUE * value, DB_CURRENCY const type)
{
  int error;

  CHECK_1ARG_ERROR (value);

  /* check for valid currency type
     don't put default case in the switch!!! */
  error = ER_INVALID_CURRENCY_TYPE;
  switch (type)
    {
    case DB_CURRENCY_DOLLAR:
    case DB_CURRENCY_WON:
    case DB_CURRENCY_YUAN:
      error = NO_ERROR;		/* it's a type we expect */
      break;
    default:
      break;
    }

  if (error != NO_ERROR)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, type);
    }
  else
    {
      value->domain.general_info.type = DB_TYPE_MONETARY;
      value->data.money.type = type;
    }
  value->need_clear = false;

  return error;
}

/*
 * db_value_put_monetary_amount_as_double() -
 * return :
 * value(out):
 * amount(in):
 */
int
db_value_put_monetary_amount_as_double (DB_VALUE * value, const double amount)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_MONETARY;
  value->data.money.amount = amount;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_timestamp() -
 * return :
 * value(out):
 * timeval(in):
 */
int
db_make_timestamp (DB_VALUE * value, const DB_TIMESTAMP timeval)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_TIMESTAMP;
  value->data.utime = timeval;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_datetime() -
 * return :
 * value(out):
 * date(in):
 */
int
db_make_datetime (DB_VALUE * value, const DB_DATETIME * datetime)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_DATETIME;
  if (datetime)
    {
      value->data.datetime = *datetime;
    }
  else
    {
      db_datetime_encode (&value->data.datetime, 0, 0, 0, 0, 0, 0, 0);
    }
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_error() -
 * return :
 * value(out):
 * errcode(in):
 */
int
db_make_error (DB_VALUE * value, const int errcode)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_ERROR;
  value->data.error = errcode;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_method_error() -
 * return :
 * value(out):
 * errcode(in):
 * errmsg(in);
 */
int
db_make_method_error (DB_VALUE * value, const int errcode, const char *errmsg)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_ERROR;
  value->data.error = errcode;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

#if !defined(SERVER_MODE)
  if (obj_Method_error_msg)
    {
      free (obj_Method_error_msg);	/* free old last error */
    }
  obj_Method_error_msg = NULL;
  if (errmsg)
    {
      obj_Method_error_msg = strdup (errmsg);
    }
#endif

  return NO_ERROR;
}

/*
 * db_get_method_error_msg() -
 * return :
 */
char *
db_get_method_error_msg (void)
{
#if !defined(SERVER_MODE)
  return obj_Method_error_msg;
#else
  return NULL;
#endif
}

/*
 * db_make_elo() -
 * return :
 * value(out):
 * elo(in):
 */
int
db_make_elo (DB_VALUE * value, DB_ELO * elo)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_ELO;
  value->data.elo = elo;
  if (elo)
    {
      value->domain.general_info.is_null = 0;
    }
  else
    {
      value->domain.general_info.is_null = 1;
    }
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_oid() -
 * return :
 * value(out):
 * oid(in):
 */
int
db_make_oid (DB_VALUE * value, const OID * oid)
{
  CHECK_2ARGS_ERROR (value, oid);

  value->domain.general_info.type = DB_TYPE_OID;
  value->data.oid.pageid = oid->pageid;
  value->data.oid.slotid = oid->slotid;
  value->data.oid.volid = oid->volid;
  value->domain.general_info.is_null = OID_ISNULL (oid);
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * db_make_resultset() -
 * return :
 * value(out):
 * handle(in):
 */
int
db_make_resultset (DB_VALUE * value, const DB_RESULTSET handle)
{
  CHECK_1ARG_ERROR (value);

  value->domain.general_info.type = DB_TYPE_RESULTSET;
  value->data.rset = handle;
  value->domain.general_info.is_null = 0;
  value->need_clear = false;

  return NO_ERROR;
}

/*
 *  OBTAIN DATA VALUES OF DB_VALUE
 */
/*
 * db_get_int() -
 * return :
 * value(in):
 */
int
db_get_int (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  return value->data.i;
}

/*
 * db_get_short() -
 * return :
 * value(in):
 */
short
db_get_short (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  return value->data.sh;
}

/*
 * db_get_bigint() -
 * return :
 * value(in):
 */
DB_BIGINT
db_get_bigint (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  return value->data.bigint;
}

/*
 * db_get_string() -
 * return :
 * value(in):
 */
char *
db_get_string (const DB_VALUE * value)
{
  char *str = NULL;
  CHECK_1ARG_NULL (value);

  if (value->domain.general_info.is_null
      || value->domain.general_info.type == DB_TYPE_ERROR)
    {
      return NULL;
    }

  switch (value->data.ch.info.style)
    {
    case SMALL_STRING:
      str = (char *) value->data.ch.sm.buf;
      break;
    case MEDIUM_STRING:
      str = value->data.ch.medium.buf;
      break;
    case LARGE_STRING:
      /* Currently not implemented */
      str = NULL;
      break;
    }

  return str;
}

/*
 * db_get_char() -
 * return :
 * value(in):
 * length(out):
 */
char *
db_get_char (const DB_VALUE * value, int *length)
{
  char *str = NULL;

  CHECK_1ARG_NULL (value);
  CHECK_1ARG_NULL (length);

  if (value->domain.general_info.is_null
      || value->domain.general_info.type == DB_TYPE_ERROR)
    {
      return NULL;
    }

  switch (value->data.ch.info.style)
    {
    case SMALL_STRING:
      {
	str = (char *) value->data.ch.sm.buf;
	intl_char_count ((unsigned char *) str,
			 value->data.ch.sm.size,
			 (INTL_CODESET) value->data.ch.info.codeset, length);
      }
      break;
    case MEDIUM_STRING:
      {
	str = value->data.ch.medium.buf;
	intl_char_count ((unsigned char *) str,
			 value->data.ch.medium.size,
			 (INTL_CODESET) value->data.ch.info.codeset, length);
      }
      break;
    case LARGE_STRING:
      {
	/* Currently not implemented */
	str = NULL;
	*length = 0;
      }
      break;
    }

  return str;
}

/*
 * db_get_nchar() -
 * return :
 * value(in):
 * length(out):
 */
char *
db_get_nchar (const DB_VALUE * value, int *length)
{
  return db_get_char (value, length);
}

/*
 * db_get_bit() -
 * return :
 * value(in):
 * length(out):
 */
char *
db_get_bit (const DB_VALUE * value, int *length)
{
  char *str = NULL;

  CHECK_1ARG_NULL (value);
  CHECK_1ARG_NULL (length);

  if (value->domain.general_info.is_null)
    {
      return NULL;
    }

  switch (value->data.ch.info.style)
    {
    case SMALL_STRING:
      {
	*length = value->data.ch.sm.size;
	str = (char *) value->data.ch.sm.buf;
      }
      break;
    case MEDIUM_STRING:
      {
	*length = value->data.ch.medium.size;
	str = value->data.ch.medium.buf;
      }
      break;
    case LARGE_STRING:
      {
	/* Currently not implemented */
	*length = 0;
	str = NULL;
      }
      break;
    }

  return str;
}

/*
 * db_get_string_size() -
 * return :
 * value(in):
 */
int
db_get_string_size (const DB_VALUE * value)
{
  int size = 0;

  CHECK_1ARG_ZERO (value);

  switch (value->data.ch.info.style)
    {
    case SMALL_STRING:
      size = value->data.ch.sm.size;
      break;
    case MEDIUM_STRING:
      size = value->data.ch.medium.size;
      break;
    case LARGE_STRING:
      /* Currently not implemented */
      size = 0;
      break;
    }

  /* Convert the number of bits to the number of bytes */
  if (value->domain.general_info.type == DB_TYPE_BIT
      || value->domain.general_info.type == DB_TYPE_VARBIT)
    {
      size = (size + 7) / 8;
    }

  return size;
}

/*
 * db_get_string_codeset() -
 * return :
 * value(in):
 */
INTL_CODESET
db_get_string_codeset (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO_WITH_TYPE (value, INTL_CODESET);

  return (INTL_CODESET) value->data.ch.info.codeset;
}

/*
 * db_get_numeric() -
 * return :
 * value(in):
 */
DB_C_NUMERIC
db_get_numeric (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  if (value->domain.general_info.is_null
      || value->domain.general_info.type == DB_TYPE_ERROR)
    {
      return NULL;
    }
  else
    {
      return (DB_C_NUMERIC) value->data.num.d.buf;
    }
}

/*
 * db_get_float() -
 * return :
 * value(in):
 */
float
db_get_float (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  return value->data.f;
}

/*
 * db_get_double() -
 * return :
 * value(in):
 */
double
db_get_double (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  return value->data.d;
}

/*
 * db_get_object() -
 * return :
 * value(in):
 */
DB_OBJECT *
db_get_object (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  if (value->domain.general_info.is_null
      || value->domain.general_info.type == DB_TYPE_ERROR)
    {
      return NULL;
    }
  else
    {
      return value->data.op;
    }
}

/*
 * db_get_set() -
 * return :
 * value(in):
 */
DB_SET *
db_get_set (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  if (value->domain.general_info.is_null
      || value->domain.general_info.type == DB_TYPE_ERROR)
    {
      return NULL;
    }
  else
    {
      return value->data.set;
    }
}

/*
 * db_get_midxkey() -
 * return :
 * value(in):
 */
DB_MIDXKEY *
db_get_midxkey (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  if (value->domain.general_info.is_null
      || value->domain.general_info.type == DB_TYPE_ERROR)
    {
      return NULL;
    }
  else
    {
      return (DB_MIDXKEY *) (&(value->data.midxkey));
    }
}

/*
 * db_get_pointer() -
 * return :
 * value(in):
 */
void *
db_get_pointer (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  if (value->domain.general_info.is_null
      || value->domain.general_info.type == DB_TYPE_ERROR)
    {
      return NULL;
    }
  else
    {
      return value->data.p;
    }
}

/*
 * db_get_time() -
 * return :
 * value(in):
 */
DB_TIME *
db_get_time (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  return ((DB_TIME *) (&value->data.time));
}

/*
 * db_get_timestamp() -
 * return :
 * value(in):
 */
DB_TIMESTAMP *
db_get_timestamp (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  return ((DB_TIMESTAMP *) (&value->data.utime));
}

/*
 * db_get_datetime() -
 * return :
 * value(in):
 */
DB_DATETIME *
db_get_datetime (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  return ((DB_DATETIME *) (&value->data.datetime));
}

/*
 * db_get_date() -
 * return :
 * value(in):
 */
DB_DATE *
db_get_date (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  return ((DB_DATE *) (&value->data.date));
}

/*
 * db_get_monetary() -
 * return :
 * value(in):
 */
DB_MONETARY *
db_get_monetary (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  return ((DB_MONETARY *) (&value->data.money));
}

/*
 * db_value_get_monetary_currency() -
 * return :
 * value(in):
 */
DB_CURRENCY
db_value_get_monetary_currency (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO_WITH_TYPE (value, DB_CURRENCY);

  return value->data.money.type;
}

/*
 * db_value_get_monetary_amount_as_double() -
 * return :
 * value(in):
 */
double
db_value_get_monetary_amount_as_double (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  return value->data.money.amount;
}

/*
 * db_get_resultset() -
 * return :
 * value(in):
 */
DB_RESULTSET
db_get_resultset (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  return value->data.rset;
}

/*
 * db_value_create() - construct an empty value container
 * return : a newly allocated value container
 */
DB_VALUE *
db_value_create (void)
{
  DB_VALUE *retval;

  CHECK_CONNECT_NULL ();

  retval = pr_make_ext_value ();

  return (retval);
}

/*
 * db_value_copy()- A new value is created and a copy is made of the contents
 *                  of the supplied container.  If the supplied value contains
 *                  external allocates such as strings or sets,
 *                  the external strings or sets are copied as well.
 * return    : A newly created value container.
 * value(in) : The value to copy.
 */
DB_VALUE *
db_value_copy (DB_VALUE * value)
{
  DB_VALUE *new_ = NULL;

  CHECK_CONNECT_NULL ();

  if (value != NULL)
    {
      new_ = pr_make_ext_value ();
      pr_clone_value (value, new_);
    }

  return (new_);
}

/*
 * db_value_clone() - Copies the contents of one value to another without
 *                    allocating a new container.
 *                    The destination container is NOT initialized prior
 *                    to the clone so it must be cleared before calling this
 *                    function.
 * return : Error indicator
 * src(in)     : DB_VALUE pointer of source value container.
 * dest(out)   : DB_VALUE pointer of destination value container.
 *
 */
int
db_value_clone (DB_VALUE * src, DB_VALUE * dest)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();

  if (src != NULL && dest != NULL)
    {
      error = pr_clone_value (src, dest);
    }

  return error;
}

/*
 * db_value_clear() - the value container is initialized to an empty state
 *        the internal type tag will be set to DB_TYPE_NULL.  Any external
 *        allocations such as strings or sets will be freed.
 *        The container itself is not freed and may be reused.
 * return: Error indicator
 * value(out) : the value to clear
 *
 */
int
db_value_clear (DB_VALUE * value)
{
  int error = NO_ERROR;

  /* don't check connection here, we always allow things to be freed */
  if (value != NULL)
    {
      error = pr_clear_value (value);
    }

  return error;
}

/*
 * db_value_free() - the value container is cleared and freed.  Any external
 *        allocations within the container such as strings or sets will also
 *        be freed.
 *
 * return     : Error indicator.
 * value(out) : The value to free.
 */
int
db_value_free (DB_VALUE * value)
{
  int error = NO_ERROR;

  /* don't check connection here, we always allow things to be freed */
  if (value != NULL)
    {
      error = pr_free_ext_value (value);
    }

  return error;
}

/*
 * db_value_clear_array() - all the value containers in the values array are
 *        initialized to an empty state; their internal type tag will be set
 *        to DB_TYPE_NULL. Any external allocations such as strings or sets
 *        will be freed.
 *        The array itself is not freed and may be reused.
 * return: Error indicator
 * value_array(out) : the value array to clear
 */
int
db_value_clear_array (DB_VALUE_ARRAY * value_array)
{
  int error = NO_ERROR;
  int i = 0;

  assert (value_array != NULL && value_array->size >= 0);

  for (i = 0; i < value_array->size; ++i)
    {
      int tmp_error = NO_ERROR;

      assert (value_array->vals != NULL);

      /* don't check connection here, we always allow things to be freed */
      tmp_error = pr_clear_value (&value_array->vals[i]);
      if (tmp_error != NO_ERROR && error == NO_ERROR)
	{
	  error = tmp_error;
	}
    }

  return error;
}

/*
 * db_value_print() - describe the contents of a value container
 * return   : none
 * value(in): value container to print.
 */
void
db_value_print (const DB_VALUE * value)
{
  CHECK_CONNECT_VOID ();

  if (value != NULL)
    {
      help_fprint_value (stdout, value);
    }

}

/*
 * db_value_fprint() - describe the contents of a value to the specified file
 * return    : none
 * fp(in)    : file pointer.
 * value(in) : value container to print.
 */
void
db_value_fprint (FILE * fp, const DB_VALUE * value)
{
  CHECK_CONNECT_VOID ();

  if (fp != NULL && value != NULL)
    {
      help_fprint_value (fp, value);
    }

}

/*
 * db_type_to_db_domain() - see the note below.
 *
 * return : DB_DOMAIN of a primitive DB_TYPE, returns NULL otherwise
 * type(in) : a primitive DB_TYPE
 *
 * note:
 *   This function is used only in special cases where we need to get the
 *   DB_DOMAIN of a primitive DB_TYPE that has no domain parameters, or of a
 *   parameterized DB_TYPE with the default domain parameters.
 *
 *   For example, it can be used to get the DB_DOMAIN of primitive
 *   DB_TYPES like DB_TYPE_INTEGER, DB_TYPE_FLOAT, etc., or of
 *   DB_TYPE_NUMERIC(DB_DEFAULT_NUMERIC_PRECISION, DB_DEFAULT_NUMERIC_SCALE).
 *
 *   This function CANNOT be used to get the DB_DOMAIN of
 *   set/multiset/sequence and object DB_TYPEs.
 */
DB_DOMAIN *
db_type_to_db_domain (const DB_TYPE type)
{
  DB_DOMAIN *result = NULL;

  switch (type)
    {
    case DB_TYPE_INTEGER:
    case DB_TYPE_FLOAT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_TIME:
    case DB_TYPE_UTIME:
    case DB_TYPE_DATETIME:
    case DB_TYPE_DATE:
    case DB_TYPE_MONETARY:
    case DB_TYPE_SHORT:
    case DB_TYPE_BIGINT:
    case DB_TYPE_NUMERIC:
    case DB_TYPE_CHAR:
    case DB_TYPE_NCHAR:
    case DB_TYPE_BIT:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_VARNCHAR:
    case DB_TYPE_VARBIT:
    case DB_TYPE_SET:
    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
    case DB_TYPE_NULL:
      result = tp_domain_resolve_default (type);
      break;
    case DB_TYPE_ELO:
    case DB_TYPE_SUB:
    case DB_TYPE_POINTER:
    case DB_TYPE_ERROR:
    case DB_TYPE_VOBJ:
    case DB_TYPE_OID:
    case DB_TYPE_OBJECT:
    case DB_TYPE_DB_VALUE:
    case DB_TYPE_VARIABLE:
    case DB_TYPE_RESULTSET:
    case DB_TYPE_MIDXKEY:
    case DB_TYPE_TABLE:
      result = NULL;
      break;
      /* NO DEFAULT CASE!!!!! ALL TYPES MUST GET HANDLED HERE! */
    }

  return result;
}

/*
 * db_value_coerce()-coerces a DB_VALUE to another compatible DB_VALUE domain.
 * return       : error indicator.
 * src(in)      : a pointer to the original DB_VALUE.
 * dest(in/out) : a pointer to a place to put the coerced DB_VALUE.
 * desired_domain(in) : the desired domain of the coerced result.
 */
int
db_value_coerce (const DB_VALUE * src, DB_VALUE * dest,
		 const DB_DOMAIN * desired_domain)
{
  TP_DOMAIN_STATUS status;
  int err = NO_ERROR;

  status = tp_value_cast (src, dest, desired_domain, false);
  switch (status)
    {
    case DOMAIN_INCOMPATIBLE:
      {
	err = ER_TP_INCOMPATIBLE_DOMAINS;
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		ER_TP_INCOMPATIBLE_DOMAINS, 2,
		pr_type_name ((DB_TYPE) src->domain.
			      general_info.type),
		pr_type_name (desired_domain->type->id));
      }
      break;
    case DOMAIN_OVERFLOW:
      {
	err = ER_IT_DATA_OVERFLOW;
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		ER_IT_DATA_OVERFLOW, 1,
		pr_type_name (desired_domain->type->id));
      }
      break;
    case DOMAIN_ERROR:
      err = er_errid ();
      break;
    default:
      break;
    }

  return (err);
}

/*
 * db_value_equal()- compare two values to see if they are equal.
 * return : non-zero if equal, zero if not equal
 * value1(in) : value container to compare.
 * value2(in) : value container to compare.
 *
 * note : this is a boolean test, the sign or magnitude of the non-zero return
 *        value has no meaning.
 */
int
db_value_equal (const DB_VALUE * value1, const DB_VALUE * value2)
{
  int retval;

  CHECK_CONNECT_ZERO ();

  /* this handles NULL arguments */
  retval = (tp_value_equal (value1, value2, 1));

  return (retval);
}

/*
 * db_set_compare() - This compares the two collection values.
 *    It will determine if the value1 is a subset or superset of value2.
 *    If either value is not a collection type, it will return DB_NE
 *    If both values are set types, a set comparison will be done.
 *    Otherwise a multiset comparison will be done.
 * return : DB_EQ        - values are equal
 *          DB_SUBSET    - value1 is subset of value2
 *          DB_SUPERSET  - value1 is superset of value2
 *          DB_NE        - values are not both collections.
 *          DB_UNK       - collections contain NULLs preventing
 *                         a more certain answer.
 *
 * value1(in): A db_value of some collection type.
 * value2(in): A db_value of some collection type.
 *
 */
int
db_set_compare (const DB_VALUE * value1, const DB_VALUE * value2)
{
  int retval;

  /* this handles NULL arguments */
  retval = (tp_set_compare (value1, value2, 1, 0));

  return (retval);
}

/*
 * db_value_compare() - Compares the two values for ordinal position
 *    It will attempt to coerce the two values to the most general
 *    of the two types passed in. Then a connonical comparison is done.
 *
 * return : DB_EQ  - values are equal
 *          DB_GT  - value1 is cannonicaly before value2
 *          DB_LT  - value1 is cannonicaly after value2
 *          DB_UNK - value is or contains NULLs preventing
 *                   a more certain answer
 */
int
db_value_compare (const DB_VALUE * value1, const DB_VALUE * value2)
{
  /* this handles NULL arguments */
  return tp_value_compare (value1, value2, 1, 0);
}

/*
 * db_get_error() -
 * return :
 * value(in):
 */
int
db_get_error (const DB_VALUE * value)
{
  CHECK_1ARG_ZERO (value);

  return value->data.error;
}

/*
 * db_get_elo() -
 * return :
 * value(in):
 */
DB_ELO *
db_get_elo (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  if (value->domain.general_info.is_null)
    {
      return NULL;
    }
  else
    {
      return value->data.elo;
    }
}

/*
 * db_get_oid() -
 * return :
 * value(in):
 */
OID *
db_get_oid (const DB_VALUE * value)
{
  CHECK_1ARG_NULL (value);

  return (OID *) & value->data.oid;
}

/*
 * db_get_currency_default() - This returns the value of the default currency
 *    identifier based on the current locale.  This was formerly defined with
 *    the variable DB_CURRENCY_DEFAULT but for the PC, we need to
 *    have this available through a function so it can be exported through
 *    the DLL.
 * return  : currency identifier.
 */
DB_CURRENCY
db_get_currency_default ()
{
  return lang_currency ();
}

/*
 * transfer_string() -
 * return     : an error indicator.
 *
 * dst(out)   : pointer to destination buffer area
 * xflen(out) : pointer to int field that will receive the number of bytes
 *              transferred.
 * outlen(out): pointer to int field that will receive an indication of number
 *              of bytes transferred.(see the note below)
 * dstlen(in) : size of destination buffer area (in bytes, *including* null
 *              terminator in the case of strings)
 * src(in)    : pointer to source buffer area
 * srclen(in) : size of source buffer area (in bytes, *NOT* including null
 *              terminator in the case of strings)
 * c_type(in) : the type of the destination buffer (i.e., whether it is varying
 *              or fixed)
 * codeset(in): International codeset for the character string.  This is needed
 *              to properly pad the strings.
 *
 */
static int
transfer_string (char *dst, int *xflen, int *outlen,
		 const int dstlen, const char *src, const int srclen,
		 const DB_TYPE_C type, const INTL_CODESET codeset)
{
  int code;
  unsigned char *ptr;
  int length, size;

  code = NO_ERROR;

  if (dstlen > srclen)
    {
      /*
       * No truncation; copy the data and blank pad if necessary.
       */
      memcpy (dst, src, srclen);
      if ((type == DB_TYPE_C_CHAR) || (type == DB_TYPE_C_NCHAR))
	{
	  ptr = qstr_pad_string ((unsigned char *) &dst[srclen],
				 (int) ((dstlen - srclen -
					 1) / qstr_pad_size (codeset)),
				 codeset);
	  *xflen = CAST_STRLEN ((char *) ptr - (char *) dst);
	}
      else
	{
	  *xflen = srclen;
	}
      dst[*xflen] = '\0';

      if (outlen)
	{
	  *outlen = 0;
	}
    }
  else
    {
      /*
       * Truncation is necessary; put as many bytes as possible into
       * the receiving buffer and null-terminate it (i.e., it receives
       * at most dstlen-1 bytes).  If there is not outlen indicator by
       * which we can indicate truncation, this is an error.
       *
       */
      intl_char_count ((unsigned char *) src, dstlen - 1, codeset, &length);
      intl_char_size ((unsigned char *) src, length, codeset, &size);
      memcpy (dst, src, size);
      dst[size] = '\0';
      *xflen = size;
      if (outlen)
	{
	  *outlen = srclen;
	}
      else
	{
	  code = ER_UCI_NULL_IND_NEEDED;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_UCI_NULL_IND_NEEDED,
		  0);
	}
    }

  return code;
}

/*
 * transfer_bit_string() -
 *    Transfers at most buflen bytes to the region pointed at by buf.
 *    If c_type is DB_TYPE_C_BIT, strings shorter than dstlen will be
 *    blank-padded out to buflen-1 bytes.  All strings will be
 *    null-terminated.  If truncation is necessary (i.e., if buflen is
 *    less than or equal to length of src), *outlen is set to length of
 *    src; if truncation is is not necessary, *outlen is set to 0.
 *
 * return     : an error indicator
 *
 * buf(out)   : pointer to destination buffer area
 * xflen(out) : Number of bits transfered from the src string to the buf.
 * outlen(out): pointer to a int field.  *outlen will equal 0 if no
 *              truncation occurred and will equal the size of the destination
 *              buffer in bytes needed to avoid truncation, otherwise.
 * buflen(in) : size of destination buffer area in bytes
 * src(in)    : pointer to source buffer area.
 * c_type(in) : the type of the destination buffer (i.e., whether it is varying
 *              or fixed).
 */
static int
transfer_bit_string (char *buf, int *xflen, int *outlen,
		     const int buflen, const DB_VALUE * src,
		     const DB_TYPE_C c_type)
{
  DB_VALUE tmp_value;
  DB_DATA_STATUS data_status;
  DB_TYPE db_type;
  int error_code;
  char *tmp_val_str;

  if (c_type == DB_TYPE_C_BIT)
    {
      db_type = DB_TYPE_BIT;
    }
  else
    {
      db_type = DB_TYPE_VARBIT;
    }

  db_value_domain_init (&tmp_value, db_type, buflen, DB_DEFAULT_SCALE);
  error_code = db_bit_string_coerce (src, &tmp_value, &data_status);
  if (error_code == NO_ERROR)
    {
      *xflen = DB_GET_STRING_LENGTH (&tmp_value);
      if (data_status == DATA_STATUS_TRUNCATED)
	{
	  if (outlen != NULL)
	    {
	      *outlen = DB_GET_STRING_LENGTH (src);
	    }
	}

      tmp_val_str = DB_GET_STRING (&tmp_value);
      if (tmp_val_str != NULL)
	{
	  memcpy (buf, tmp_val_str, DB_GET_STRING_SIZE (&tmp_value));
	}

      error_code = db_value_clear (&tmp_value);
    }

  return error_code;
}

/*
 * db_value_get() -
 *
 * return      : Error indicator.
 * value(in)   : Pointer to a DB_VALUE
 * c_type(in)  : The type of the C destination buffer (and, therefore, an
 *               indication of the type of coercion desired)
 * buf(out)    : Pointer to a C buffer
 * buflen(in)  : The length of that buffer in bytes.  The buffer should include
 *               room for a terminating NULL character which is appended to
 *               character strings.
 * xflen(out)  : Pointer to a int field that will contain the number of
 *               elemental-units transfered into the buffer.  This value does
 *               not include terminating NULL characters which are appended
 *               to character strings.  An elemental-unit will be a byte
 *               for all types except bit strings in are represented in bits.
 * outlen(out) : Pointer to a int field that will contain the length
 *               of the source if truncation occurred and 0 otherwise.  This
 *               value will be in terms of bytes for all types.  <outlen>
 *               can be used to reallocate buffer space if the buffer
 *               was too small to contain the value.
 *
 *
 */
int
db_value_get (DB_VALUE * value, const DB_TYPE_C c_type,
	      void *buf, const int buflen, int *xflen, int *outlen)
{
  int error_code = NO_ERROR;

  if (DB_IS_NULL (value))
    {
      if (outlen)
	{
	  *outlen = -1;
	  return NO_ERROR;
	}
      else
	{
	  error_code = ER_UCI_NULL_IND_NEEDED;
	  goto error0;
	}
    }

  if ((buf == NULL) || (xflen == NULL))
    {
      goto invalid_args;
    }

  /*
   * *outlen will be non-zero only when converting to a character
   * output and truncation is necessary.  All other cases should set
   * *outlen to 0 unless a NULL is encountered (which case we've
   * already dealt with).
   */
  if (outlen)
    {
      *outlen = 0;
    }

  /*
   * The numeric conversions below probably ought to be checking for
   * overflow and complaining when it happens.  For example, trying to
   * get a double out into a DB_C_SHORT is likely to overflow; the
   * user probably wants to know about it.
   */
  switch (DB_VALUE_TYPE (value))
    {
    case DB_TYPE_INTEGER:
      {
	int i = DB_GET_INT (value);

	switch (c_type)
	  {
	  case DB_TYPE_C_INT:
	    *(DB_C_INT *) buf = (DB_C_INT) i;
	    *xflen = sizeof (DB_C_INT);
	    break;
	  case DB_TYPE_C_SHORT:
	    *(DB_C_SHORT *) buf = (DB_C_SHORT) i;
	    *xflen = sizeof (DB_C_SHORT);
	    break;
	  case DB_TYPE_C_LONG:
	    *(DB_C_LONG *) buf = (DB_C_LONG) i;
	    *xflen = sizeof (DB_C_LONG);
	    break;
	  case DB_TYPE_C_FLOAT:
	    *(DB_C_FLOAT *) buf = (DB_C_FLOAT) i;
	    *xflen = sizeof (DB_C_FLOAT);
	    break;
	  case DB_TYPE_C_DOUBLE:
	    *(DB_C_DOUBLE *) buf = (DB_C_DOUBLE) i;
	    *xflen = sizeof (DB_C_DOUBLE);
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%d", i);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, INTL_CODESET_ISO88591);
	    }
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%d", i);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, lang_charset ());
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_INT */
      break;

    case DB_TYPE_SMALLINT:
      {
	short s = DB_GET_SMALLINT (value);

	switch (c_type)
	  {
	  case DB_TYPE_C_INT:
	    {
	      *(DB_C_INT *) buf = (DB_C_INT) s;
	      *xflen = sizeof (DB_C_INT);
	    }
	    break;
	  case DB_TYPE_C_SHORT:
	    {
	      *(DB_C_SHORT *) buf = (DB_C_SHORT) s;
	      *xflen = sizeof (DB_C_SHORT);
	    }
	    break;
	  case DB_TYPE_C_LONG:
	    {
	      *(DB_C_LONG *) buf = (DB_C_LONG) s;
	      *xflen = sizeof (DB_C_LONG);
	    }
	    break;
	  case DB_TYPE_C_FLOAT:
	    {
	      *(DB_C_FLOAT *) buf = (DB_C_FLOAT) s;
	      *xflen = sizeof (DB_C_FLOAT);
	    }
	    break;
	  case DB_TYPE_C_DOUBLE:
	    {
	      *(DB_C_DOUBLE *) buf = (DB_C_DOUBLE) s;
	      *xflen = sizeof (DB_C_DOUBLE);
	    }
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%d", s);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, INTL_CODESET_ISO88591);
	    }
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%d", s);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, lang_charset ());
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_SMALLINT */
      break;

    case DB_TYPE_BIGINT:
      {
	DB_BIGINT bigint = DB_GET_BIGINT (value);

	switch (c_type)
	  {
	  case DB_TYPE_C_INT:
	    *(DB_C_INT *) buf = (DB_C_INT) bigint;
	    *xflen = sizeof (DB_C_INT);
	    break;
	  case DB_TYPE_C_SHORT:
	    *(DB_C_SHORT *) buf = (DB_C_SHORT) bigint;
	    *xflen = sizeof (DB_C_SHORT);
	    break;
	  case DB_TYPE_C_BIGINT:
	    *(DB_C_BIGINT *) buf = (DB_C_BIGINT) bigint;
	    *xflen = sizeof (DB_C_BIGINT);
	    break;
	  case DB_TYPE_C_LONG:
	    *(DB_C_LONG *) buf = (DB_C_LONG) bigint;
	    *xflen = sizeof (DB_C_LONG);
	    break;
	  case DB_TYPE_C_FLOAT:
	    *(DB_C_FLOAT *) buf = (DB_C_FLOAT) bigint;
	    *xflen = sizeof (DB_C_FLOAT);
	    break;
	  case DB_TYPE_C_DOUBLE:
	    *(DB_C_DOUBLE *) buf = (DB_C_DOUBLE) bigint;
	    *xflen = sizeof (DB_C_DOUBLE);
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%lld", (long long) bigint);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, INTL_CODESET_ISO88591);
	    }
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%lld", (long long) bigint);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, lang_charset ());
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_BIGINT */
      break;

    case DB_TYPE_FLOAT:
      {
	float f = DB_GET_FLOAT (value);

	switch (c_type)
	  {
	  case DB_TYPE_C_INT:
	    {
	      *(DB_C_INT *) buf = (DB_C_INT) f;
	      *xflen = sizeof (DB_C_INT);
	    }
	    break;
	  case DB_TYPE_C_SHORT:
	    {
	      *(DB_C_SHORT *) buf = (DB_C_SHORT) f;
	      *xflen = sizeof (DB_C_SHORT);
	    }
	    break;
	  case DB_TYPE_C_LONG:
	    {
	      *(DB_C_LONG *) buf = (DB_C_LONG) f;
	      *xflen = sizeof (DB_C_LONG);
	    }
	    break;
	  case DB_TYPE_C_FLOAT:
	    {
	      *(DB_C_FLOAT *) buf = (DB_C_FLOAT) f;
	      *xflen = sizeof (DB_C_FLOAT);
	    }
	    break;
	  case DB_TYPE_C_DOUBLE:
	    {
	      *(DB_C_DOUBLE *) buf = (DB_C_DOUBLE) f;
	      *xflen = sizeof (DB_C_DOUBLE);
	    }
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%f", (DB_C_DOUBLE) f);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, INTL_CODESET_ISO88591);
	    }
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%f", (DB_C_DOUBLE) f);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, lang_charset ());
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_FLOAT */
      break;

    case DB_TYPE_DOUBLE:
      {
	double d = DB_GET_DOUBLE (value);

	switch (c_type)
	  {
	  case DB_TYPE_C_INT:
	    {
	      *(DB_C_INT *) buf = (DB_C_INT) d;
	      *xflen = sizeof (DB_C_INT);
	    }
	    break;
	  case DB_TYPE_C_SHORT:
	    {
	      *(DB_C_SHORT *) buf = (DB_C_SHORT) d;
	      *xflen = sizeof (DB_C_SHORT);
	    }
	    break;
	  case DB_TYPE_C_LONG:
	    {
	      *(DB_C_LONG *) buf = (DB_C_LONG) d;
	      *xflen = sizeof (DB_C_LONG);
	    }
	    break;
	  case DB_TYPE_C_FLOAT:
	    {
	      *(DB_C_FLOAT *) buf = (DB_C_FLOAT) d;
	      *xflen = sizeof (DB_C_FLOAT);
	    }
	    break;
	  case DB_TYPE_C_DOUBLE:
	    {
	      *(DB_C_DOUBLE *) buf = (DB_C_DOUBLE) d;
	      *xflen = sizeof (DB_C_DOUBLE);
	    }
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%f", (DB_C_DOUBLE) d);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, INTL_CODESET_ISO88591);
	    }
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%f", (DB_C_DOUBLE) d);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, lang_charset ());
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_DOUBLE */
      break;

    case DB_TYPE_MONETARY:
      {
	DB_MONETARY *money = DB_GET_MONETARY (value);
	double d = money->amount;

	switch (c_type)
	  {
	  case DB_TYPE_C_INT:
	    {
	      *(DB_C_INT *) buf = (DB_C_INT) d;
	      *xflen = sizeof (DB_C_INT);
	    }
	    break;
	  case DB_TYPE_C_SHORT:
	    {
	      *(DB_C_SHORT *) buf = (DB_C_SHORT) d;
	      *xflen = sizeof (DB_C_SHORT);
	    }
	    break;
	  case DB_TYPE_C_LONG:
	    {
	      *(DB_C_LONG *) buf = (DB_C_LONG) d;
	      *xflen = sizeof (DB_C_LONG);
	    }
	    break;
	  case DB_TYPE_C_FLOAT:
	    {
	      *(DB_C_FLOAT *) buf = (DB_C_FLOAT) d;
	      *xflen = sizeof (DB_C_FLOAT);
	    }
	    break;
	  case DB_TYPE_C_DOUBLE:
	    {
	      *(DB_C_DOUBLE *) buf = (DB_C_DOUBLE) d;
	      *xflen = sizeof (DB_C_DOUBLE);
	    }
	    break;
	  case DB_TYPE_C_MONETARY:
	    {
	      /*
	       * WARNING: this works only so long as DB_C_MONETARY
	       * is typedef'ed as a DB_MONETARY.  If that changes,
	       * so must this.
	       */
	      *(DB_C_MONETARY *) buf = *money;
	      *xflen = sizeof (DB_C_MONETARY);
	    }
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%4.2f", (DB_C_DOUBLE) d);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, INTL_CODESET_ISO88591);
	    }
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      char tmp[NUM_BUF_SIZE];
	      sprintf (tmp, "%4.2f", (DB_C_DOUBLE) d);
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, lang_charset ());
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_MONETARY */
      break;

    case DB_TYPE_VARCHAR:
    case DB_TYPE_CHAR:
    case DB_TYPE_VARNCHAR:
    case DB_TYPE_NCHAR:
      {
	const char *s = DB_GET_STRING (value);
	int n = DB_GET_STRING_SIZE (value);

	if (s == NULL)
	  {
	    goto invalid_args;
	  }

	switch (c_type)
	  {
	  case DB_TYPE_C_INT:
	    {
	      char tmp[NUM_BUF_SIZE];
	      if (n >= NUM_BUF_SIZE)
		{
		  goto invalid_args;
		}

	      memcpy (tmp, s, n);
	      tmp[n] = '\0';
	      *(DB_C_INT *) buf = (DB_C_INT) atol (tmp);
	      *xflen = sizeof (DB_C_INT);
	    }
	    break;
	  case DB_TYPE_C_SHORT:
	    {
	      char tmp[NUM_BUF_SIZE];
	      if (n >= NUM_BUF_SIZE)
		{
		  goto invalid_args;
		}

	      memcpy (tmp, s, n);
	      tmp[n] = '\0';
	      *(DB_C_SHORT *) buf = (DB_C_SHORT) atol (tmp);
	      *xflen = sizeof (DB_C_SHORT);
	    }
	    break;
	  case DB_TYPE_C_LONG:
	    {
	      char tmp[NUM_BUF_SIZE];
	      if (n >= NUM_BUF_SIZE)
		{
		  goto invalid_args;
		}

	      memcpy (tmp, s, n);
	      tmp[n] = '\0';
	      *(DB_C_LONG *) buf = (DB_C_LONG) atol (tmp);
	      *xflen = sizeof (DB_C_LONG);
	    }
	    break;
	  case DB_TYPE_C_FLOAT:
	    {
	      char tmp[NUM_BUF_SIZE];
	      if (n >= NUM_BUF_SIZE)
		{
		  goto invalid_args;
		}

	      memcpy (tmp, s, n);
	      tmp[n] = '\0';
	      *(DB_C_FLOAT *) buf = (DB_C_FLOAT) atof (tmp);
	      *xflen = sizeof (DB_C_FLOAT);
	    }
	    break;
	  case DB_TYPE_C_DOUBLE:
	    {
	      char tmp[NUM_BUF_SIZE];
	      if (n >= NUM_BUF_SIZE)
		{
		  goto invalid_args;
		}

	      memcpy (tmp, s, n);
	      tmp[n] = '\0';
	      *(DB_C_DOUBLE *) buf = (DB_C_DOUBLE) atof (tmp);
	      *xflen = sizeof (DB_C_DOUBLE);
	    }
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    error_code = transfer_string ((char *) buf, xflen, outlen, buflen,
					  s, n, c_type,
					  INTL_CODESET_ISO88591);
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    error_code = transfer_string ((char *) buf, xflen, outlen, buflen,
					  s, n, c_type,
					  INTL_CODESET_ISO88591);
	    break;

	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_VARCHAR, DB_TYPE_CHAR */
      break;

    case DB_TYPE_OBJECT:
      {
	switch (c_type)
	  {
	  case DB_TYPE_C_OBJECT:
	    {
	      *(DB_OBJECT **) buf = (DB_OBJECT *) DB_GET_OBJECT (value);
	      *xflen = sizeof (DB_OBJECT *);
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_OBJECT */
      break;

    case DB_TYPE_SET:
    case DB_TYPE_MULTISET:
    case DB_TYPE_SEQUENCE:
      {
	switch (c_type)
	  {
	  case DB_TYPE_C_SET:
	    {
	      *(DB_SET **) buf = (DB_SET *) DB_GET_SET (value);
	      *xflen = sizeof (DB_SET *);
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_SET, DB_TYPE_MULTI_SET, DB_TYPE_SEQUENCE */
      break;

    case DB_TYPE_TIME:
      {
	switch (c_type)
	  {
	  case DB_TYPE_C_TIME:
	    {
	      *(DB_TIME *) buf = *(DB_GET_TIME (value));
	      *xflen = sizeof (DB_TIME);
	    }
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    {
	      int n;
	      char tmp[TIME_BUF_SIZE];
	      n = db_time_to_string (tmp, sizeof (tmp), DB_GET_TIME (value));
	      if (n < 0)
		{
		  goto invalid_args;
		}
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, INTL_CODESET_ISO88591);
	    }
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      int n;
	      char tmp[TIME_BUF_SIZE];
	      n = db_time_to_string (tmp, sizeof (tmp), DB_GET_TIME (value));
	      if (n < 0)
		{
		  goto invalid_args;
		}
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, lang_charset ());
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_TIME */
      break;

    case DB_TYPE_TIMESTAMP:
      {
	switch (c_type)
	  {
	  case DB_TYPE_C_TIMESTAMP:
	    {
	      *(DB_TIMESTAMP *) buf = *(DB_GET_TIMESTAMP (value));
	      *xflen = sizeof (DB_TIMESTAMP);
	    }
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    {
	      int n;
	      char tmp[TIMESTAMP_BUF_SIZE];
	      n = db_timestamp_to_string (tmp,
					  sizeof (tmp),
					  DB_GET_TIMESTAMP (value));
	      if (n < 0)
		{
		  goto invalid_args;
		}
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, INTL_CODESET_ISO88591);

	    }
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      int n;
	      char tmp[TIMESTAMP_BUF_SIZE];
	      n = db_timestamp_to_string (tmp,
					  sizeof (tmp),
					  DB_GET_TIMESTAMP (value));
	      if (n < 0)
		{
		  goto invalid_args;
		}
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, lang_charset ());
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_TIMESTAMP */
      break;

    case DB_TYPE_DATETIME:
      {
	switch (c_type)
	  {
	  case DB_TYPE_C_DATETIME:
	    {
	      *(DB_DATETIME *) buf = *(DB_GET_DATETIME (value));
	      *xflen = sizeof (DB_DATETIME);
	    }
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    {
	      int n;
	      char tmp[DATETIME_BUF_SIZE];
	      n = db_datetime_to_string (tmp,
					 sizeof (tmp),
					 DB_GET_DATETIME (value));
	      if (n < 0)
		{
		  goto invalid_args;
		}
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, INTL_CODESET_ISO88591);

	    }
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      int n;
	      char tmp[DATETIME_BUF_SIZE];
	      n = db_datetime_to_string (tmp,
					 sizeof (tmp),
					 DB_GET_DATETIME (value));
	      if (n < 0)
		{
		  goto invalid_args;
		}
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, lang_charset ());
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_DATETIME */

    case DB_TYPE_DATE:
      {
	switch (c_type)
	  {
	  case DB_TYPE_C_DATE:
	    {
	      *(DB_DATE *) buf = *(DB_GET_DATE (value));
	      *xflen = sizeof (DB_DATE);
	    }
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	    {
	      int n;
	      char tmp[DATE_BUF_SIZE];
	      n = db_date_to_string (tmp, sizeof (tmp), DB_GET_DATE (value));
	      if (n < 0)
		{
		  goto invalid_args;
		}
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, INTL_CODESET_ISO88591);
	    }
	    break;
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      int n;
	      char tmp[DATE_BUF_SIZE];
	      n = db_date_to_string (tmp, sizeof (tmp), DB_GET_DATE (value));
	      if (n < 0)
		{
		  goto invalid_args;
		}
	      error_code =
		transfer_string ((char *) buf, xflen, outlen, buflen, tmp,
				 strlen (tmp), c_type, lang_charset ());
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_DATE */
      break;

    case DB_TYPE_NUMERIC:
      {
	switch (c_type)
	  {
	  case DB_TYPE_C_INT:
	  case DB_TYPE_C_SHORT:
	  case DB_TYPE_C_LONG:
	  case DB_TYPE_C_BIGINT:
	    {
	      DB_VALUE v;
	      DB_DATA_STATUS status;

	      db_value_domain_init (&v,
				    DB_TYPE_BIGINT,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	      (void) numeric_db_value_coerce_from_num (value, &v, &status);
	      if (status != NO_ERROR)
		{
		  goto invalid_args;
		}
	      error_code = db_value_get (&v, c_type, buf, buflen,
					 xflen, outlen);
	      pr_clear_value (&v);
	    }
	    break;
	  case DB_TYPE_C_FLOAT:
	  case DB_TYPE_C_DOUBLE:
	    {
	      DB_VALUE v;
	      DB_DATA_STATUS status;

	      db_value_domain_init (&v,
				    DB_TYPE_DOUBLE,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	      (void) numeric_db_value_coerce_from_num (value, &v, &status);
	      if (status != NO_ERROR)
		{
		  goto invalid_args;
		}
	      error_code = db_value_get (&v, c_type, buf, buflen, xflen,
					 outlen);
	      pr_clear_value (&v);
	    }
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_VARCHAR:
	  case DB_TYPE_C_NCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      DB_VALUE v;
	      DB_DATA_STATUS status;

	      db_value_domain_init (&v,
				    DB_TYPE_VARCHAR,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	      (void) numeric_db_value_coerce_from_num (value, &v, &status);
	      if (status != NO_ERROR)
		{
		  goto invalid_args;
		}
	      error_code = db_value_get (&v, c_type, buf, buflen, xflen,
					 outlen);
	      pr_clear_value (&v);
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_NUMERIC */
      break;

    case DB_TYPE_BIT:
    case DB_TYPE_VARBIT:
      {
	switch (c_type)
	  {
	  case DB_TYPE_C_BIT:
	  case DB_TYPE_C_VARBIT:
	    error_code =
	      transfer_bit_string ((char *) buf, xflen, outlen, buflen, value,
				   c_type);
	    break;
	  case DB_TYPE_C_CHAR:
	  case DB_TYPE_C_NCHAR:
	    {
	      int truncated;
	      qstr_bit_to_hex_coerce ((char *) buf,
				      buflen,
				      DB_GET_STRING (value),
				      DB_GET_STRING_LENGTH (value),
				      true, xflen, &truncated);
	      if (truncated > 0)
		{
		  if (outlen)
		    {
		      *outlen = truncated;
		    }
		  else
		    {
		      error_code = ER_UCI_NULL_IND_NEEDED;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_UCI_NULL_IND_NEEDED, 0);
		    }
		}
	    }
	    break;
	  case DB_TYPE_C_VARCHAR:
	  case DB_TYPE_C_VARNCHAR:
	    {
	      int truncated;
	      qstr_bit_to_hex_coerce ((char *) buf,
				      buflen,
				      DB_GET_STRING (value),
				      DB_GET_STRING_LENGTH (value),
				      false, xflen, &truncated);
	      if (truncated > 0)
		{
		  if (outlen)
		    {
		      *outlen = truncated;
		    }
		  else
		    {
		      error_code = ER_UCI_NULL_IND_NEEDED;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_UCI_NULL_IND_NEEDED, 0);
		    }
		}
	    }
	    break;
	  default:
	    goto unsupported_conversion;
	  }
      }				/* DB_TYPE_BIT, DB_TYPE_VARBIT */
      break;

    case DB_TYPE_ELO:
    case DB_TYPE_VARIABLE:
    case DB_TYPE_SUB:
    case DB_TYPE_POINTER:
    case DB_TYPE_ERROR:
    case DB_TYPE_VOBJ:
    case DB_TYPE_OID:		/* Probably won't ever happen */
      goto unsupported_conversion;

    case DB_TYPE_FIRST:
    case DB_TYPE_DB_VALUE:
    case DB_TYPE_RESULTSET:
    case DB_TYPE_MIDXKEY:
    case DB_TYPE_TABLE:	/* Should be impossible. */
      goto invalid_args;
    }

  if (error_code != NO_ERROR)
    {
      goto error0;
    }

  return NO_ERROR;

invalid_args:
  error_code = ER_OBJ_INVALID_ARGUMENTS;
  goto error0;

error0:
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
  return error_code;

unsupported_conversion:
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	  ER_DB_UNSUPPORTED_CONVERSION, 1, "db_value_get");
  return ER_DB_UNSUPPORTED_CONVERSION;
}

/*
 * coerce_char_to_dbvalue() - Coerce the C character string into the
 *                       desired type and place in a DB_VALUE container.
 * return :
 *     C_TO_VALUE_NOERROR                - No errors occurred
 *     C_TO_VALUE_UNSUPPORTED_CONVERSION - The conversion to the db_value
 *                                         type is not supported
 * value(in/out): DB_VALUE container for result.  This also contains the DB
 *                type to convert to.
 * buf(in)      : Pointer to character buffer
 * buflen(in)   : Length of character buffer
 *
 */
static int
coerce_char_to_dbvalue (DB_VALUE * value, char *buf, const int buflen)
{
  int status = C_TO_VALUE_NOERROR;
  DB_TYPE db_type = DB_VALUE_DOMAIN_TYPE (value);

  switch (db_type)
    {
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
      {
	int precision = DB_VALUE_PRECISION (value);

	if (precision == TP_FLOATING_PRECISION_VALUE && buflen != 0)
	  {
	    precision = buflen;
	  }

	if ((precision == TP_FLOATING_PRECISION_VALUE)
	    || (db_type == DB_TYPE_VARCHAR && precision >= buflen)
	    || (db_type == DB_TYPE_CHAR && precision == buflen))
	  {
	    qstr_make_typed_string (db_type, value, precision, buf, buflen);
	  }
	else
	  {
	    DB_VALUE tmp_value;
	    DB_DATA_STATUS data_status;
	    int error;

	    qstr_make_typed_string (db_type, &tmp_value, precision, buf,
				    buflen);

	    error = db_char_string_coerce (&tmp_value, value, &data_status);
	    if (error != NO_ERROR)
	      {
		status = C_TO_VALUE_CONVERSION_ERROR;
	      }
	    else if (data_status == DATA_STATUS_TRUNCATED)
	      {
		status = C_TO_VALUE_TRUNCATION_ERROR;
	      }

	    (void) db_value_clear (&tmp_value);
	  }
      }
      break;
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      {
	int char_count;
	int precision = DB_VALUE_PRECISION (value);

	intl_char_count ((unsigned char *) buf,
			 buflen, lang_charset (), &char_count);

	if (precision == TP_FLOATING_PRECISION_VALUE)
	  {
	    precision = char_count;
	  }

	if ((db_type == DB_TYPE_VARNCHAR && precision >= char_count)
	    || (db_type == DB_TYPE_NCHAR && precision == char_count))
	  {

	    qstr_make_typed_string (db_type, value, precision, buf, buflen);
	  }
	else
	  {
	    DB_VALUE tmp_value;
	    DB_DATA_STATUS data_status;

	    qstr_make_typed_string (db_type, &tmp_value, precision, buf,
				    buflen);

	    (void) db_char_string_coerce (&tmp_value, value, &data_status);
	    if (data_status == DATA_STATUS_TRUNCATED)
	      {
		status = C_TO_VALUE_TRUNCATION_ERROR;
	      }

	    (void) db_value_clear (&tmp_value);
	  }
      }
      break;
    case DB_TYPE_BIT:
    case DB_TYPE_VARBIT:
      {
	DB_VALUE tmp_value;
	DB_DATA_STATUS data_status;

	db_value_domain_init (&tmp_value, db_type,
			      DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	if (db_string_value (buf, "%H", &tmp_value) == NULL)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
	else
	  {
	    /*
	     *  If the precision is not specified, fix it to
	     *  the input precision otherwise db_bit_string_coerce()
	     *  will fail.
	     */
	    if (DB_VALUE_PRECISION (value) == TP_FLOATING_PRECISION_VALUE)
	      {
		db_value_domain_init (value,
				      db_type,
				      DB_GET_STRING_LENGTH (&tmp_value),
				      DB_DEFAULT_SCALE);
	      }

	    (void) db_bit_string_coerce (&tmp_value, value, &data_status);
	    if (data_status == DATA_STATUS_TRUNCATED)
	      {
		status = C_TO_VALUE_TRUNCATION_ERROR;
	      }
	  }

	(void) db_value_clear (&tmp_value);
      }
      break;
    case DB_TYPE_DOUBLE:
    case DB_TYPE_FLOAT:
    case DB_TYPE_INTEGER:
    case DB_TYPE_SHORT:
    case DB_TYPE_BIGINT:
    case DB_TYPE_DATE:
    case DB_TYPE_TIME:
    case DB_TYPE_TIMESTAMP:
    case DB_TYPE_DATETIME:
    case DB_TYPE_MONETARY:
      if (db_string_value (buf, "", value) == NULL)
	{
	  status = C_TO_VALUE_CONVERSION_ERROR;
	}
      break;
    case DB_TYPE_NUMERIC:
      {
	DB_VALUE tmp_value;
	unsigned char new_num[DB_NUMERIC_BUF_SIZE];
	int desired_precision = DB_VALUE_PRECISION (value);
	int desired_scale = DB_VALUE_SCALE (value);

	/* string_to_num will coerce the string to a numeric, but will
	 * set the precision and scale based on the value passed.
	 * Then we call num_to_num to coerce to the desired precision
	 * and scale.
	 */

	if (numeric_coerce_string_to_num (buf, &tmp_value) != NO_ERROR)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
	else if (numeric_coerce_num_to_num (db_get_numeric (&tmp_value),
					    DB_VALUE_PRECISION (&tmp_value),
					    DB_VALUE_SCALE (&tmp_value),
					    desired_precision, desired_scale,
					    new_num) != NO_ERROR)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
	else
	  {
	    /* Yes, I know that the precision and scale are already
	     * set, but this is neater than just assigning the value.
	     */
	    db_make_numeric (value, new_num, desired_precision,
			     desired_scale);
	  }

	db_value_clear (&tmp_value);
      }
      break;
    default:
      status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
      break;
    }

  return status;
}

/*
 * coerce_numeric_to_dbvalue() - Coerce the C character number string
 *              into the desired type and place in a DB_VALUE container.
 *
 * return :
 *     C_TO_VALUE_NOERROR                - no errors occurred
 *     C_TO_VALUE_UNSUPPORTED_CONVERSION - The conversion to the db_value
 *                                         type is not supported.
 * value(out) : DB_VALUE container for result.  This also contains the DB
 *              type to convert to.
 * buf(in)    : Pointer to character buffer.
 * c_type(in) : type of c string to coerce.
 */

static int
coerce_numeric_to_dbvalue (DB_VALUE * value, char *buf,
			   const DB_TYPE_C c_type)
{
  int status = C_TO_VALUE_NOERROR;
  DB_TYPE db_type = DB_VALUE_DOMAIN_TYPE (value);

  switch (c_type)
    {
    case DB_TYPE_C_INT:
      {
	DB_C_INT num = *(DB_C_INT *) buf;

	switch (db_type)
	  {
	  case DB_TYPE_NUMERIC:
	    {
	      DB_DATA_STATUS data_status;
	      DB_VALUE value2;

	      db_make_int (&value2, (int) num);
	      (void) numeric_db_value_coerce_to_num (&value2, value,
						     &data_status);
	    }
	    break;
	  case DB_TYPE_INTEGER:
	    db_make_int (value, (int) num);
	    break;
	  case DB_TYPE_BIGINT:
	    db_make_bigint (value, (DB_C_BIGINT) num);
	    break;
	  case DB_TYPE_FLOAT:
	    db_make_float (value, (DB_C_FLOAT) num);
	    break;
	  case DB_TYPE_DOUBLE:
	    db_make_double (value, (DB_C_DOUBLE) num);
	    break;
	  case DB_TYPE_SHORT:
	    db_make_short (value, (DB_C_SHORT) num);
	    break;
	  case DB_TYPE_MONETARY:
	    db_make_monetary (value, DB_CURRENCY_DEFAULT, (DB_C_DOUBLE) num);
	    break;
	  default:
	    status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
	    break;
	  }
      }
      break;

    case DB_TYPE_C_SHORT:
      {
	DB_C_SHORT num = *(DB_C_SHORT *) buf;

	switch (db_type)
	  {
	  case DB_TYPE_NUMERIC:
	    {
	      DB_DATA_STATUS data_status;
	      DB_VALUE value2;

	      db_make_short (&value2, (DB_C_SHORT) num);
	      (void) numeric_db_value_coerce_to_num (&value2, value,
						     &data_status);
	    }
	    break;
	  case DB_TYPE_INTEGER:
	    db_make_int (value, (int) num);
	    break;
	  case DB_TYPE_BIGINT:
	    db_make_bigint (value, (DB_C_BIGINT) num);
	    break;
	  case DB_TYPE_FLOAT:
	    db_make_float (value, (DB_C_FLOAT) num);
	    break;
	  case DB_TYPE_DOUBLE:
	    db_make_double (value, (DB_C_DOUBLE) num);
	    break;
	  case DB_TYPE_SHORT:
	    db_make_short (value, (DB_C_SHORT) num);
	    break;
	  case DB_TYPE_MONETARY:
	    db_make_monetary (value, DB_CURRENCY_DEFAULT, (DB_C_DOUBLE) num);
	    break;
	  default:
	    status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
	    break;
	  }
      }
      break;

    case DB_TYPE_C_LONG:
      {
	DB_C_LONG num = *(DB_C_LONG *) buf;

	switch (db_type)
	  {
	  case DB_TYPE_NUMERIC:
	    {
	      DB_DATA_STATUS data_status;
	      DB_VALUE value2;

	      db_make_int (&value2, (int) num);
	      (void) numeric_db_value_coerce_to_num (&value2, value,
						     &data_status);
	    }
	    break;
	  case DB_TYPE_INTEGER:
	    db_make_int (value, (int) num);
	    break;
	  case DB_TYPE_BIGINT:
	    db_make_bigint (value, (DB_C_BIGINT) num);
	    break;
	  case DB_TYPE_FLOAT:
	    db_make_float (value, (DB_C_FLOAT) num);
	    break;
	  case DB_TYPE_DOUBLE:
	    db_make_double (value, (DB_C_DOUBLE) num);
	    break;
	  case DB_TYPE_SHORT:
	    db_make_short (value, (DB_C_SHORT) num);
	    break;
	  case DB_TYPE_MONETARY:
	    db_make_monetary (value, DB_CURRENCY_DEFAULT, (DB_C_DOUBLE) num);
	    break;
	  default:
	    status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
	    break;
	  }
      }
      break;

    case DB_TYPE_C_FLOAT:
      {
	DB_C_FLOAT num = *(DB_C_FLOAT *) buf;

	switch (db_type)
	  {
	  case DB_TYPE_NUMERIC:
	    {
	      DB_DATA_STATUS data_status;
	      DB_VALUE value2;

	      db_make_double (&value2, (DB_C_DOUBLE) num);
	      (void) numeric_db_value_coerce_to_num (&value2, value,
						     &data_status);
	    }
	    break;
	  case DB_TYPE_INTEGER:
	    db_make_int (value, (int) num);
	    break;
	  case DB_TYPE_BIGINT:
	    db_make_bigint (value, (DB_C_BIGINT) num);
	    break;
	  case DB_TYPE_FLOAT:
	    db_make_float (value, (DB_C_FLOAT) num);
	    break;
	  case DB_TYPE_DOUBLE:
	    db_make_double (value, (DB_C_DOUBLE) num);
	    break;
	  case DB_TYPE_SHORT:
	    db_make_short (value, (DB_C_SHORT) num);
	    break;
	  case DB_TYPE_MONETARY:
	    db_make_monetary (value, DB_CURRENCY_DEFAULT, (DB_C_DOUBLE) num);
	    break;
	  default:
	    status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
	    break;
	  }
      }
      break;

    case DB_TYPE_C_DOUBLE:
      {
	DB_C_DOUBLE num = *(DB_C_DOUBLE *) buf;

	switch (db_type)
	  {
	  case DB_TYPE_NUMERIC:
	    {
	      DB_DATA_STATUS data_status;
	      DB_VALUE value2;

	      db_make_double (&value2, (DB_C_DOUBLE) num);
	      (void) numeric_db_value_coerce_to_num (&value2, value,
						     &data_status);
	    }
	    break;
	  case DB_TYPE_INTEGER:
	    db_make_int (value, (int) num);
	    break;
	  case DB_TYPE_BIGINT:
	    db_make_bigint (value, (DB_C_BIGINT) num);
	    break;
	  case DB_TYPE_FLOAT:
	    db_make_float (value, (DB_C_FLOAT) num);
	    break;
	  case DB_TYPE_DOUBLE:
	    db_make_double (value, (DB_C_DOUBLE) num);
	    break;
	  case DB_TYPE_SHORT:
	    db_make_short (value, (DB_C_SHORT) num);
	    break;
	  case DB_TYPE_MONETARY:
	    db_make_monetary (value, DB_CURRENCY_DEFAULT, (DB_C_DOUBLE) num);
	    break;
	  default:
	    status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
	    break;
	  }
      }
      break;

    case DB_TYPE_C_MONETARY:
      {
	DB_C_MONETARY *money = (DB_C_MONETARY *) buf;
	double num = money->amount;

	switch (db_type)
	  {
	  case DB_TYPE_NUMERIC:
	    /*
	     *  We need a better way to convert a numerical C type
	     *  into a NUMERIC.  This will have to suffice for now.
	     */
	    {
	      DB_DATA_STATUS data_status;
	      DB_VALUE value2;

	      db_make_double (&value2, (DB_C_DOUBLE) num);
	      (void) numeric_db_value_coerce_to_num (&value2, value,
						     &data_status);
	    }
	    break;
	  case DB_TYPE_INTEGER:
	    db_make_int (value, (int) num);
	    break;
	  case DB_TYPE_BIGINT:
	    db_make_bigint (value, (DB_C_BIGINT) num);
	    break;
	  case DB_TYPE_FLOAT:
	    db_make_float (value, (DB_C_FLOAT) num);
	    break;
	  case DB_TYPE_DOUBLE:
	    db_make_double (value, (DB_C_DOUBLE) num);
	    break;
	  case DB_TYPE_SHORT:
	    db_make_short (value, (DB_C_SHORT) num);
	    break;
	  case DB_TYPE_MONETARY:
	    db_make_monetary (value, money->type, money->amount);
	    break;
	  default:
	    status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
	    break;
	  }
      }
      break;

    default:
      status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
      break;
    }

  return status;
}

/*
 * coerce_binary_to_dbvalue() - Coerce a C bit type into the desired type
 *                                 and place in a DB_VALUE container.
 * return  :
 *     C_TO_VALUE_NOERROR                - No errors occurred
 *     C_TO_VALUE_UNSUPPORTED_CONVERSION - The conversion to the db_value
 *                                         type is not supported
 *     C_TO_VALUE_CONVERSION_ERROR       - An error occurred during conversion
 *     C_TO_VALUE_TRUNCATION_ERROR       - The input data was truncated
 *                                         during coercion
 * value(in/out) : DB_VALUE container for result.  This also contains the DB
 *                 type to convert to.
 * buf(in)       : Pointer to data buffer
 * buflen(in)    : Length of data (in bits)
 */
static int
coerce_binary_to_dbvalue (DB_VALUE * value, char *buf, const int buflen)
{
  int status = C_TO_VALUE_NOERROR;
  DB_TYPE db_type = DB_VALUE_DOMAIN_TYPE (value);

  switch (db_type)
    {
    case DB_TYPE_BIT:
    case DB_TYPE_VARBIT:
      {
	int precision = DB_VALUE_PRECISION (value);

	if (precision == TP_FLOATING_PRECISION_VALUE && buflen != 0)
	  {
	    precision = buflen;
	  }

	if ((precision == TP_FLOATING_PRECISION_VALUE)
	    || (db_type == DB_TYPE_VARBIT && precision >= buflen)
	    || (db_type == DB_TYPE_BIT && precision == buflen))
	  {
	    qstr_make_typed_string (db_type, value, precision, buf, buflen);
	  }
	else
	  {
	    DB_VALUE tmp_value;
	    DB_DATA_STATUS data_status;
	    int error;

	    qstr_make_typed_string (db_type, &tmp_value, precision, buf,
				    buflen);

	    error = db_bit_string_coerce (&tmp_value, value, &data_status);
	    if (error != NO_ERROR)
	      {
		status = C_TO_VALUE_CONVERSION_ERROR;
	      }
	    else if (data_status == DATA_STATUS_TRUNCATED)
	      {
		status = C_TO_VALUE_TRUNCATION_ERROR;
	      }

	    (void) db_value_clear (&tmp_value);
	  }
      }
      break;
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
      {
	int error_code;
	DB_VALUE tmp_value;
	DB_DATA_STATUS data_status;

	db_make_varchar (&tmp_value,
			 DB_DEFAULT_PRECISION, buf, QSTR_NUM_BYTES (buflen));

	/*
	 *  If the precision is not specified, fix it to
	 *  the input precision otherwise db_char_string_coerce()
	 *  will fail.
	 */
	if (DB_VALUE_PRECISION (value) == TP_FLOATING_PRECISION_VALUE)
	  {
	    db_value_domain_init (value, db_type, QSTR_NUM_BYTES (buflen), 0);
	  }

	error_code = db_char_string_coerce (&tmp_value, value, &data_status);
	if (error_code != NO_ERROR)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
	else if (data_status == DATA_STATUS_TRUNCATED)
	  {
	    status = C_TO_VALUE_TRUNCATION_ERROR;
	  }

	error_code = db_value_clear (&tmp_value);
	if (error_code != NO_ERROR)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
      }
      break;
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      {
	int error_code;
	DB_VALUE tmp_value;
	DB_DATA_STATUS data_status;

	db_make_varnchar (&tmp_value,
			  DB_DEFAULT_PRECISION, buf, QSTR_NUM_BYTES (buflen));

	/*
	 *  If the precision is not specified, fix it to
	 *  the input precision otherwise db_char_string_coerce()
	 *  will fail.
	 */
	if (DB_VALUE_PRECISION (value) == TP_FLOATING_PRECISION_VALUE)
	  {
	    db_value_domain_init (value, db_type, QSTR_NUM_BYTES (buflen), 0);
	  }

	error_code = db_char_string_coerce (&tmp_value, value, &data_status);
	if (error_code != NO_ERROR)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
	else if (data_status == DATA_STATUS_TRUNCATED)
	  {
	    status = C_TO_VALUE_TRUNCATION_ERROR;
	  }

	error_code = db_value_clear (&tmp_value);
	if (error_code != NO_ERROR)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
      }
      break;
    case DB_TYPE_INTEGER:
      db_make_int (value, *(int *) buf);
      break;
    case DB_TYPE_BIGINT:
      db_make_bigint (value, *(DB_C_BIGINT *) buf);
      break;
    case DB_TYPE_FLOAT:
      db_make_float (value, *(DB_C_FLOAT *) buf);
      break;
    case DB_TYPE_DOUBLE:
      db_make_double (value, *(DB_C_DOUBLE *) buf);
      break;
    case DB_TYPE_SHORT:
      db_make_short (value, *(DB_C_SHORT *) buf);
      break;
    case DB_TYPE_DATE:
      db_value_put_encoded_date (value, (DB_DATE *) buf);
      break;
    case DB_TYPE_TIME:
      db_value_put_encoded_time (value, (DB_TIME *) buf);
      break;
    case DB_TYPE_TIMESTAMP:
      db_make_timestamp (value, *(DB_TIMESTAMP *) buf);
      break;
    case DB_TYPE_DATETIME:
      db_make_datetime (value, (DB_DATETIME *) buf);
      break;
    default:
      status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
      break;
    }

  return status;
}

/*
 * coerce_date_to_dbvalue() - Coerce a C date type into the desired type
 *                               and place in a DB_VALUE container.
 * return  :
 *     C_TO_VALUE_NOERROR                - No errors occurred
 *     C_TO_VALUE_UNSUPPORTED_CONVERSION - The conversion to the db_value
 *                                         type is not supported
 *     C_TO_VALUE_CONVERSION_ERROR       - An error occurred during conversion
 *
 * value(in/out): DB_VALUE container for result. This also contains the DB
 *                type to convert to.
 * buf(in)      : Pointer to data buffer.
 *
 */
static int
coerce_date_to_dbvalue (DB_VALUE * value, char *buf)
{
  int status = C_TO_VALUE_NOERROR;
  DB_TYPE db_type = DB_VALUE_DOMAIN_TYPE (value);
  DB_C_DATE *date = (DB_C_DATE *) buf;

  switch (db_type)
    {
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      {
	DB_DATE db_date;
	char tmp[DATE_BUF_SIZE];

	db_date_encode (&db_date, date->month, date->day, date->year);
	if (db_date_to_string (tmp, DATE_BUF_SIZE, &db_date) == 0)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
	else
	  {
	    DB_VALUE tmp_value;
	    DB_DATA_STATUS data_status;
	    int length = strlen (tmp);

	    if (length == 0)
	      {
		length = 1;
	      }

	    qstr_make_typed_string (db_type, &tmp_value, length, tmp, length);

	    /*
	     *  If the precision is not specified, fix it to
	     *  the input precision otherwise db_char_string_coerce()
	     *  will fail.
	     */
	    if (DB_VALUE_PRECISION (value) == TP_FLOATING_PRECISION_VALUE)
	      {
		db_value_domain_init (value, db_type, length, 0);
	      }

	    (void) db_char_string_coerce (&tmp_value, value, &data_status);
	    if (data_status == DATA_STATUS_TRUNCATED)
	      {
		status = C_TO_VALUE_TRUNCATION_ERROR;
	      }

	    (void) db_value_clear (&tmp_value);
	  }
      }
      break;
    case DB_TYPE_DATE:
      db_make_date (value, date->month, date->day, date->year);
      break;
    default:
      status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
      break;
    }

  return status;
}

/*
 * coerce_time_to_dbvalue() - Coerce a C time type into the desired type
 *                               and place in a DB_VALUE container.
 * return :
 *     C_TO_VALUE_NOERROR                - No errors occurred.
 *     C_TO_VALUE_UNSUPPORTED_CONVERSION - If the conversion to the db_value
 *                                         type is not supported.
 *     C_TO_VALUE_CONVERSION_ERROR       - An error occurred during conversion.
 * value(in/out) : DB_VALUE container for result.  This also contains the DB
 *                 type to convert to.
 * buf(in)       : Pointer to data buffer.
 *
 */

static int
coerce_time_to_dbvalue (DB_VALUE * value, char *buf)
{
  int status = C_TO_VALUE_NOERROR;
  DB_TYPE db_type = DB_VALUE_DOMAIN_TYPE (value);
  DB_C_TIME *c_time = (DB_C_TIME *) buf;

  switch (db_type)
    {
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      {
	DB_TIME db_time;
	char tmp[TIME_BUF_SIZE];

	db_time_encode (&db_time, c_time->hour, c_time->minute,
			c_time->second);
	if (db_time_string (&db_time, "", tmp, TIME_BUF_SIZE) != 0)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
	else
	  {
	    DB_VALUE tmp_value;
	    DB_DATA_STATUS data_status;
	    int length = strlen (tmp);

	    if (length == 0)
	      {
		length = 1;
	      }

	    qstr_make_typed_string (db_type, &tmp_value, length, tmp, length);

	    /*
	     *  If the precision is not specified, fix it to
	     *  the input precision otherwise db_char_string_coerce()
	     *  will fail.
	     */
	    if (DB_VALUE_PRECISION (value) == TP_FLOATING_PRECISION_VALUE)
	      {
		db_value_domain_init (value, db_type, length, 0);
	      }

	    (void) db_char_string_coerce (&tmp_value, value, &data_status);
	    if (data_status == DATA_STATUS_TRUNCATED)
	      {
		status = C_TO_VALUE_TRUNCATION_ERROR;
	      }

	    (void) db_value_clear (&tmp_value);
	  }
      }
      break;
    case DB_TYPE_TIME:
      db_make_time (value, c_time->hour, c_time->minute, c_time->second);
      break;
    default:
      status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
      break;
    }

  return status;
}

/*
 * coerce_timestamp_to_dbvalue() - Coerce a C timestamp type into the
 *                        desired type and place in a DB_VALUE container.
 * return :
 *     C_TO_VALUE_NOERROR                - No errors occurred.
 *     C_TO_VALUE_UNSUPPORTED_CONVERSION - If the conversion to the db_value
 *                                         type is not supported.
 *     C_TO_VALUE_CONVERSION_ERROR       - An error occurred during conversion.
 *
 * value(in/out) : DB_VALUE container for result.  This also contains the DB
 *                 type to convert to.
 * buf(in)       : Pointer to data buffer.
 *
 */
static int
coerce_timestamp_to_dbvalue (DB_VALUE * value, char *buf)
{
  int status = C_TO_VALUE_NOERROR;
  DB_TYPE db_type = DB_VALUE_DOMAIN_TYPE (value);
  DB_C_TIMESTAMP *timestamp = (DB_C_TIMESTAMP *) buf;

  switch (db_type)
    {
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      {
	char tmp[TIME_BUF_SIZE];

	if (db_timestamp_string (timestamp, "", tmp, TIMESTAMP_BUF_SIZE) != 0)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
	else
	  {
	    DB_VALUE tmp_value;
	    DB_DATA_STATUS data_status;
	    int length = strlen (tmp);

	    if (length == 0)
	      {
		length = 1;
	      }

	    qstr_make_typed_string (db_type, &tmp_value, length, tmp, length);

	    /*
	     *  If the precision is not specified, fix it to
	     *  the input precision otherwise db_char_string_coerce()
	     *  will fail.
	     */
	    if (DB_VALUE_PRECISION (value) == TP_FLOATING_PRECISION_VALUE)
	      {
		db_value_domain_init (value, db_type, length, 0);
	      }

	    (void) db_char_string_coerce (&tmp_value, value, &data_status);
	    if (data_status == DATA_STATUS_TRUNCATED)
	      {
		status = C_TO_VALUE_TRUNCATION_ERROR;
	      }

	    (void) db_value_clear (&tmp_value);
	  }
      }
      break;
    case DB_TYPE_DATE:
      {
	DB_DATE edate;
	DB_TIME etime;

	db_timestamp_decode (timestamp, &edate, &etime);
	db_value_put_encoded_date (value, &edate);
      }
      break;
    case DB_TYPE_TIME:
      {
	DB_DATE edate;
	DB_TIME etime;

	db_timestamp_decode (timestamp, &edate, &etime);
	db_value_put_encoded_time (value, &etime);
      }
      break;
    case DB_TYPE_TIMESTAMP:
      db_make_timestamp (value, *timestamp);
      break;
    case DB_TYPE_DATETIME:
      {
	DB_DATETIME tmp_datetime;

	db_timestamp_to_datetime (timestamp, &tmp_datetime);
	db_make_datetime (value, &tmp_datetime);
      }
      break;
    default:
      status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
      break;
    }

  return status;
}

/*
 * coerce_datetime_to_dbvalue() - Coerce a C datetime type into the
 *                        desired type and place in a DB_VALUE container.
 * return :
 *     C_TO_VALUE_NOERROR                - No errors occured.
 *     C_TO_VALUE_UNSUPPORTED_CONVERSION - If the conversion to the db_value
 *                                         type is not supported.
 *     C_TO_VALUE_CONVERSION_ERROR       - An error occured during conversion.
 *
 * value(in/out) : DB_VALUE container for result.  This also contains the DB
 *                 type to convert to.
 * buf(in)       : Pointer to data buffer.
 *
 */
static int
coerce_datetime_to_dbvalue (DB_VALUE * value, char *buf)
{
  int status = C_TO_VALUE_NOERROR;
  DB_TYPE db_type = DB_VALUE_DOMAIN_TYPE (value);
  DB_DATETIME *datetime = (DB_DATETIME *) buf;

  switch (db_type)
    {
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      {
	char tmp[TIME_BUF_SIZE];

	if (db_datetime_string (datetime, "", tmp, DATETIME_BUF_SIZE) != 0)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
	else
	  {
	    DB_VALUE tmp_value;
	    DB_DATA_STATUS data_status;
	    int length = strlen (tmp);

	    if (length == 0)
	      {
		length = 1;
	      }

	    qstr_make_typed_string (db_type, &tmp_value, length, tmp, length);

	    /*
	     *  If the precision is not specified, fix it to
	     *  the input precision otherwise db_char_string_coerce()
	     *  will fail.
	     */
	    if (DB_VALUE_PRECISION (value) == TP_FLOATING_PRECISION_VALUE)
	      {
		db_value_domain_init (value, db_type, length, 0);
	      }

	    (void) db_char_string_coerce (&tmp_value, value, &data_status);
	    if (data_status == DATA_STATUS_TRUNCATED)
	      {
		status = C_TO_VALUE_TRUNCATION_ERROR;
	      }

	    (void) db_value_clear (&tmp_value);
	  }
      }
      break;
    case DB_TYPE_DATE:
      {
	DB_DATE tmp_date;

	tmp_date = datetime->date;
	db_value_put_encoded_date (value, &tmp_date);
      }
      break;
    case DB_TYPE_TIME:
      {
	DB_TIME tmp_time;

	tmp_time = datetime->time / 1000;
	db_value_put_encoded_time (value, &tmp_time);
      }
      break;
    case DB_TYPE_TIMESTAMP:
      {
	DB_TIMESTAMP tmp_timestamp;
	DB_DATE tmp_date;
	DB_TIME tmp_time;

	tmp_date = datetime->date;
	tmp_time = datetime->time / 1000;
	db_value_put_encoded_date (value, &tmp_date);
	db_value_put_encoded_time (value, &tmp_time);
	if (db_timestamp_encode (&tmp_timestamp, &tmp_date, &tmp_time)
	    != NO_ERROR)
	  {
	    status = C_TO_VALUE_CONVERSION_ERROR;
	  }
	else
	  {
	    db_make_timestamp (value, tmp_timestamp);
	  }
	break;
      }
    case DB_TYPE_DATETIME:
      {
	db_make_datetime (value, datetime);
	break;
      }
    default:
      status = C_TO_VALUE_UNSUPPORTED_CONVERSION;
      break;
    }

  return status;
}

/*
 * DOMAIN ACCESSORS
 */

/*
 * db_domain_next() - This can be used to iterate through a list of domain
 *           descriptors returned by functions such as db_attribute_domain.
 * return : The next domain descriptor(or NULL if at end of list).
 * domain(in): domain descriptor.
 */
DB_DOMAIN *
db_domain_next (const DB_DOMAIN * domain)
{
  DB_DOMAIN *next = NULL;

  if (domain != NULL)
    {
      next = domain->next;
    }

  return (next);
}

/*
 * db_domain_type() - See the note below.
 * return    : type identifier constant.
 * domain(in): domain descriptor.
 *
 * note:
 *    Returns the basic type identifier for a domain descriptor.
 *    This will be a numeric value as defined by the DB_TYPE_ enumeration.
 *    IFF this value is DB_TYPE_OBJECT then the domain will have additional
 *    information in the "class" field that can be accessed with
 *    db_domain_class.  This will tell you the specific class that was
 *    defined for this domain.  If the class field is NULL, the domain is
 *    a generic object and can be a reference to any object.  IFF the domain
 *    type is DB_TYPE_SET, DB_TYPE_MULTI_SET, or DB_TYPE_SEQUENCE, there
 *    will be additional domain information in the "set" field of the domain
 *    descriptor that can be accessed with db_domain_set.  This will be
 *    an additional list of domain descriptors that define the domains of the
 *    elements of the set.
 */
DB_TYPE
db_domain_type (const DB_DOMAIN * domain)
{
  DB_TYPE type = DB_TYPE_NULL;

  if (domain != NULL)
    {
      type = domain->type->id;
    }

  return (type);
}

/*
 * db_domain_class() - see the note below.
 *
 * return     : a class pointer
 * domain(in) : domain descriptor
 * note:
 *    This can be used to get the specific domain class for a domain whose
 *    basic type is DB_TYPE_OBJECT.  This value may be NULL indicating that
 *    the domain is the general object domain and can reference any type of
 *    object.
 *    This should check to see if the domain class was dropped.  This won't
 *    happen in the ususal case because the domain list is filtered by
 *    db_attribute_domain and related functions which always serve as the
 *    sources for this list.  Filtering again here would slow it down
 *    even more.  If it is detected as deleted and we downgrade to
 *    "object", this could leave the containing domain list will multiple
 *    object domains.
 */
DB_OBJECT *
db_domain_class (const DB_DOMAIN * domain)
{
  DB_OBJECT *class_mop = NULL;

  if ((domain != NULL) && (domain->type == tp_Type_object))
    {
      class_mop = domain->class_mop;
    }

  return (class_mop);
}

/*
 * db_domain_set() - see the note below.
 * return : domain descriptor or NULL.
 * domain(in): domain descriptor.
 *
 * note:
 *    This can be used to get set domain information for a domain
 *    whose basic type is DB_TYPE_SET, DB_TYPE_MULTI_SET, or DB_TYPE_SEQUENCE
 *    This field will always be NULL for any other kind of basic type.
 *    This field may be NULL even for set types if there was no additional
 *    domain information specified when the attribute was defined.
 *    The returned domain list can be examined just like other domain
 *    descriptors using the db_domain functions.  In theory, domains
 *    could be fully hierarchical by containing nested sets.  Currently,
 *    this is not allowed by the schema manager but it may be allowed
 *    in the future.
 */
DB_DOMAIN *
db_domain_set (const DB_DOMAIN * domain)
{
  DB_DOMAIN *setdomain = NULL;

  if ((domain != NULL) && pr_is_set_type (domain->type->id))
    {
      setdomain = domain->setdomain;
    }

  return (setdomain);
}

/*
 * db_domain_precision() - Get the precision of the given domain.
 * return    : precision of domain.
 * domain(in): domain descriptor.
 *
 */
int
db_domain_precision (const DB_DOMAIN * domain)
{
  int precision = 0;

  if (domain != NULL)
    {
      precision = domain->precision;
    }

  return (precision);
}

/*
 * db_domain_scale() - Get the scale of the given domain.
 * return    : scale of domain.
 * domain(in): domain descriptor.
 *
 */
int
db_domain_scale (const DB_DOMAIN * domain)
{
  int scale = 0;

  if (domain != NULL)
    {
      scale = domain->scale;
    }

  return (scale);
}

/*
 * db_domain_codeset() - Get the codeset of the given domain.
 * return    : codeset of domain.
 * domain(in): domain descriptor.
 */
int
db_domain_codeset (const DB_DOMAIN * domain)
{
  int codeset = 0;

  if (domain != NULL)
    {
      codeset = domain->codeset;
    }

  return (codeset);
}