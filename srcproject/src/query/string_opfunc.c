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
 * string_opfunc.c - Routines that manipulate arbitrary strings
 */

#ident "$Id$"

/* This includes bit strings, character strings, and national character strings
 */

#include "config.h"

#include <assert.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <sys/timeb.h>

#include "chartype.h"
#include "system_parameter.h"
#include "intl_support.h"
#include "error_code.h"
#include "db.h"
#include "memory_alloc.h"
#include "language_support.h"
#include "query_evaluator.h"
#if defined(SERVER_MODE)
#include "thread_impl.h"
#endif

#include "misc_string.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define BYTE_SIZE               (8)
#define QSTR_VALUE_PRECISION(value)                                       \
            ((DB_VALUE_PRECISION(value) == TP_FLOATING_PRECISION_VALUE)  \
                     ?      DB_GET_STRING_LENGTH(value)       :          \
                            DB_VALUE_PRECISION(value))

#define QSTR_MAX_PRECISION(str_type)                                         \
            (QSTR_IS_CHAR(str_type)          ?	DB_MAX_VARCHAR_PRECISION :  \
	     QSTR_IS_NATIONAL_CHAR(str_type) ?	DB_MAX_VARNCHAR_PRECISION : \
	                                        DB_MAX_VARBIT_PRECISION)

#define IS_KOREAN(ch) \
	(( ch >= 0xb0 && ch <= 0xc8 ) || ( ch >= 0xa1 && ch <= 0xfe ))

#define ABS(i) ((i) >= 0 ? (i) : -(i))

#define STACK_SIZE        100

#define LEAP(y)	  (((y) % 400 == 0) || ((y) % 100 != 0 && (y) % 4 == 0))

#define DBL_MAX_DIGITS    ((int)ceil(DBL_MAX_EXP * log10((double) FLT_RADIX)))

/*
 *  This enumeration type is used to categorize the different
 *  string types into function like groups.
 *
 *      DB_STRING and DB_CHAR    become QSTR_CHAR
 *      DB_NCHAR and DB_VARNCHAR become QSTR_NATIONAL_CHAR
 *      DB_BIT and DB_VARBIT     become QSTR_BIT
 *      All others               become QSTR_UNKNOWN, although this
 *                                      categorizations doesn't apply to
 *                                      any other domain type.
 */
typedef enum
{
  QSTR_UNKNOWN,
  QSTR_CHAR,
  QSTR_NATIONAL_CHAR,
  QSTR_BIT
} QSTR_CATEGORY;

/*
 * Timestamp format
 */
typedef enum
{
  DT_END = -2,			/*format string end */
  DT_INVALID = -1,		/* invalid format */
  DT_NORMAL,
  DT_YYYY,
  DT_YY,
  DT_MM,
  DT_MONTH,
  DT_MON,
  DT_DD,
  DT_CC,
  DT_Q,
  DT_DAY,
  DT_DY,			/*  So far, DATE format */
  DT_AM,
  DT_A_M,
  DT_PM,
  DT_P_M,
  DT_HH,
  DT_HH12,
  DT_HH24,
  DT_MI,
  DT_SS,
  DT_MS,			/*  So far, TIME format */
  DT_TEXT,
  DT_PUNCTUATION,
  DT_D
} TIMESTAMP_FORMAT;

/*
 * Number format
 */
typedef enum
{
  N_END = -2,			/*format string end */
  N_INVALID = -1,		/* invalid format */
  N_FORMAT,
  N_SPACE,
  N_TEXT
} NUMBER_FORMAT;

#define WHITE_CHARS             " \t\n"

#define QSTR_DATE_LENGTH 10
#define QSTR_TIME_LENGTH 11
#define QSTR_TIME_STAMPLENGTH 22
#define QSTR_DATETIME_LENGTH 26
#define QSTR_EXTRA_LENGTH_RATIO 3
#define MAX_TOKEN_SIZE 16000

/* TODO: extract the following messages */
static unsigned char moneysymbols[2][2] = {
  {0xa3, 0xdc}			/* '��' *//* Korean money symbols */
};

/* Routines for string positioning */
static unsigned char *qstr_next_char (unsigned char *s,
				      INTL_CODESET codeset, int *curr_length);
static unsigned char *qstr_prev_char (unsigned char *s,
				      INTL_CODESET codeset, int *prev_length);
static int qstr_trim (MISC_OPERAND tr_operand,
		      const unsigned char *trim,
		      int trim_length,
		      int trim_size,
		      const unsigned char *src_ptr,
		      DB_TYPE src_type,
		      int src_length,
		      int src_size,
		      INTL_CODESET codeset,
		      unsigned char **res,
		      DB_TYPE * res_type, int *res_length, int *res_size);
static void trim_leading (const unsigned char *trim_charset_ptr,
			  int trim_charset_size,
			  const unsigned char *src_ptr, DB_TYPE src_type,
			  int src_length, int src_size,
			  INTL_CODESET codeset,
			  unsigned char **lead_trimmed_ptr,
			  int *lead_trimmed_length, int *lead_trimmed_size);
static void trim_trailing (const unsigned char *trim_charset_ptr,
			   int trim_charset_size,
			   const unsigned char *src_ptr,
			   DB_TYPE src_type,
			   int src_length,
			   int src_size,
			   INTL_CODESET codeset,
			   int *trail_trimmed_length,
			   int *trail_trimmed_size);
static int qstr_pad (MISC_OPERAND pad_operand,
		     int pad_length,
		     const unsigned char *pad_charset_ptr,
		     int pad_charset_length,
		     int pad_charset_size,
		     const unsigned char *src_ptr,
		     DB_TYPE src_type,
		     int src_length,
		     int src_size,
		     INTL_CODESET codeset,
		     unsigned char **result,
		     DB_TYPE * result_type,
		     int *result_length, int *result_size);
static unsigned char *qstr_strstr (const unsigned char *src,
				   const unsigned char *pattern);
static int qstr_eval_like (const unsigned char *tar,
			   const unsigned char *expr, unsigned char escape);
static int kor_cmp (unsigned char *src, unsigned char *dest, int size);
static int qstr_replace (unsigned char *src_ptr,
			 DB_TYPE src_type,
			 int src_len,
			 int src_size,
			 INTL_CODESET codeset,
			 unsigned char *srch_str_ptr,
			 int srch_str_size,
			 unsigned char *repl_str_ptr,
			 int repl_str_size,
			 unsigned char **result_ptr,
			 DB_TYPE * result_type,
			 int *result_len, int *result_size);
static int qstr_translate (unsigned char *src_ptr,
			   DB_TYPE src_type,
			   int src_size,
			   INTL_CODESET codeset,
			   unsigned char *from_str_ptr,
			   int from_str_size,
			   unsigned char *to_str_ptr,
			   int to_str_size,
			   unsigned char **result_ptr,
			   DB_TYPE * result_type,
			   int *result_len, int *result_size);
static QSTR_CATEGORY qstr_get_category (const DB_VALUE * s);
#if defined (ENABLE_UNUSED_FUNCTION)
static bool is_string (const DB_VALUE * s);
#endif /* ENABLE_UNUSED_FUNCTION */
static bool is_char_string (const DB_VALUE * s);
static bool is_integer (const DB_VALUE * i);
static bool is_number (const DB_VALUE * n);
static int qstr_concatenate (const unsigned char *s1,
			     int s1_length,
			     int s1_precision,
			     DB_TYPE s1_type,
			     const unsigned char *s2,
			     int s2_length,
			     int s2_precision,
			     DB_TYPE s2_type,
			     INTL_CODESET codeset,
			     unsigned char **result,
			     int *result_length,
			     int *result_size,
			     DB_TYPE * result_type,
			     DB_DATA_STATUS * data_status);
static int qstr_bit_concatenate (const unsigned char *s1,
				 int s1_length,
				 int s1_precision,
				 DB_TYPE s1_type,
				 const unsigned char *s2,
				 int s2_length,
				 int s2_precision,
				 DB_TYPE s2_type,
				 unsigned char **result,
				 int *result_length,
				 int *result_size,
				 DB_TYPE * result_type,
				 DB_DATA_STATUS * data_status);
static void qstr_pad_char (INTL_CODESET codeset,
			   unsigned char *pad_char, int *pad_size);
static bool varchar_truncated (const unsigned char *s,
			       DB_TYPE s_type,
			       int s_length,
			       int used_chars, INTL_CODESET codeset);
static bool varbit_truncated (const unsigned char *s,
			      int s_length, int used_bits);
static void bit_ncat (unsigned char *r, int offset, const unsigned char *s,
		      int n);
static int bstring_fls (const char *s, int n);
static int qstr_bit_coerce (const unsigned char *src,
			    int src_length,
			    int src_precision,
			    DB_TYPE src_type,
			    unsigned char **dest,
			    int *dest_length,
			    int dest_precision,
			    DB_TYPE dest_type, DB_DATA_STATUS * data_status);
static int qstr_coerce (const unsigned char *src, int src_length,
			int src_precision, DB_TYPE src_type,
			INTL_CODESET codeset,
			unsigned char **dest, int *dest_length,
			int *dest_size, int dest_precision,
			DB_TYPE dest_type, DB_DATA_STATUS * data_status);
static int qstr_position (const char *sub_string, int sub_length,
			  const char *src_string, int src_length,
			  INTL_CODESET codeset, bool is_forward_search,
			  int *position);
static int qstr_bit_position (const unsigned char *sub_string,
			      int sub_length,
			      const unsigned char *src_string,
			      int src_length, int *position);
static int shift_left (unsigned char *bit_string, int bit_string_size);
static int qstr_substring (const unsigned char *src,
			   int src_length,
			   int start,
			   int length,
			   INTL_CODESET codeset,
			   unsigned char **r, int *r_length, int *r_size);
static int qstr_bit_substring (const unsigned char *src,
			       int src_length,
			       int start,
			       int length, unsigned char **r, int *r_length);
static void left_nshift (const unsigned char *bit_string, int bit_string_size,
			 int shift_amount, unsigned char *r, int r_size);
static int qstr_ffs (int v);
static int hextoi (char hex_char);
static int adjust_precision (char *data, int precision, int scale);
static int date_to_char (const DB_VALUE * src_value,
			 const DB_VALUE * format_str,
			 const DB_VALUE * date_lang, DB_VALUE * result_str);
static int number_to_char (const DB_VALUE * src_value,
			   const DB_VALUE * format_str,
			   const DB_VALUE * date_lang, DB_VALUE * result_str);
static int make_number_to_char (char *num_string, char *format_str,
				int *length, char **result_str);
static int make_scientific_notation (char *src_string, int cipher);
static int roundoff (char *src_string, int flag, int *cipher, char *format);
static int scientific_to_decimal_string (char *src_string,
					 char **scientific_str);
static int to_number_next_state (int previous_state, int input_char);
static int make_number (char *src, char *last_src, char *token, int
			*token_length, DB_VALUE * r, int precision,
			int scale);
static int get_number_token (char *fsp, int *length, char *last_position,
			     char **next_fsp);
static int get_next_format (unsigned char *sp, DB_TYPE str_type,
			    int *format_length, unsigned char **next_pos);
static int qstr_length (unsigned char *sp);
static int get_cur_year (void);
static int get_cur_month (void);
static int is_valid_date (int month, int day, int year, int day_of_the_week);
/* utility functions */
static int add_and_normalize_date_time (int *years,
					int *months,
					int *days,
					int *hours,
					int *minutes,
					int *seconds,
					int *milliseconds,
					DB_BIGINT y,
					DB_BIGINT m,
					DB_BIGINT d,
					DB_BIGINT h,
					DB_BIGINT mi,
					DB_BIGINT s, DB_BIGINT ms);
static int sub_and_normalize_date_time (int *years,
					int *months,
					int *days,
					int *hours,
					int *minutes,
					int *seconds,
					int *milliseconds,
					DB_BIGINT y,
					DB_BIGINT m,
					DB_BIGINT d,
					DB_BIGINT h,
					DB_BIGINT mi,
					DB_BIGINT s, DB_BIGINT ms);
static void set_time_argument (struct tm *dest, int year, int month, int day,
			       int hour, int min, int sec);
static long calc_unix_timestamp (struct tm *time_argument);
static int parse_for_next_int (char **ch, char *output);
static int db_str_to_millisec (const char *str);
static void copy_and_shift_values (int shift, int n, DB_BIGINT * first, ...);
static DB_BIGINT get_single_unit_value (char *expr, DB_BIGINT int_val);
static int db_date_add_sub_interval_expr (DB_VALUE * result,
					  const DB_VALUE * date,
					  const DB_VALUE * expr,
					  const int unit, bool is_add);
static int db_date_add_sub_interval_days (DB_VALUE * result,
					  const DB_VALUE * date,
					  const DB_VALUE * db_days,
					  bool is_add);

/* reads cnt alphabetic characters until a non-alpha char reached,
 * returns nr of characters traversed
 */
static int parse_characters (char *s, char *res, int cnt, int res_size);
/* reads cnt digits until non-digit char reached,
 * returns nr of characters traversed
 */
static int parse_digits (char *s, int *nr, int cnt);
static int parse_time_string (const char *timestr, int *sign, int *h,
			      int *m, int *s, int *ms);
#define TRIM_FORMAT_STRING(sz, n) {if (strlen(sz) > n) sz[n] = 0;}
#define WHITESPACE(c) ((c) == ' ' || (c) == '\t' || (c) == '\r' || (c) == '\n')
#define ALPHABETICAL(c) (((c) >= 'A' && (c) <= 'Z') || \
			 ((c) >= 'a' && (c) <= 'z'))
#define DIGIT(c) ((c) >= '0' && (c) <= '9')
/* concatenate a char to s */
#define STRCHCAT(s, c) \
  {\
    char __cch__[2];\
    __cch__[0] = c;__cch__[1] = 0; strcat(s, __cch__);\
  }

#define SKIP_SPACES(ch) 	do {\
	while (char_isspace(*(ch))) (ch)++; \
}while(0)


/*
 * qstr_next_char () -
 *   return: Pointer to the previous character in the string.
 *   s(in)      : string
 *   codeset(in)        : enumeration of the codeset of s
 *   current_char_size(out)      : length of the character at s
 * Note: Returns a pointer to the next character in the string.
 * curr_char_length is set to the byte length of the current character.
 */
static unsigned char *
qstr_next_char (unsigned char *s, INTL_CODESET codeset,
		int *current_char_size)
{
  switch (codeset)
    {
    case INTL_CODESET_ISO88591:
      *current_char_size = 1;
      return ++s;

    case INTL_CODESET_KSC5601_EUC:
      return intl_nextchar_euc (s, current_char_size);

    default:
      *current_char_size = 0;
      return s;
    }
}

/*
 * qstr_prev_char () -
 *
 * arguments:
 *                  s : (IN) string
 *            codeset : (IN) enumeration of the codeset of a
 *   prev_char_length : (OUT) length of the previous character
 *
 * returns: void
 *
 * Errors: None
 *
 * description:
 *      Replaces all upper case ASCII and ROMAN characters with their lower
 *      case codes.
 *
 */
static unsigned char *
qstr_prev_char (unsigned char *s, INTL_CODESET codeset, int *prev_char_size)
{
  switch (codeset)
    {
    case INTL_CODESET_ISO88591:
      *prev_char_size = 1;
      return --s;

    case INTL_CODESET_KSC5601_EUC:
      return intl_prevchar_euc (s, prev_char_size);

    default:
      *prev_char_size = 0;
      return s;
    }
}



/*
 *  Public Functions for Strings - Bit and Character
 */

/*
 * db_string_compare () -
 *
 * Arguments:
 *                string1: Left side of compare.
 *                string2: Right side of compare
 *                 result: Integer result of comparison.
 *            data_status: Status of errors.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE   :
 *        <string1> or <string2> are not character strings.
 *
 *    ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *        <string1> and <string2> have differing character code sets.
 *
 */

int
db_string_compare (const DB_VALUE * string1, const DB_VALUE * string2,
		   DB_VALUE * result)
{
  QSTR_CATEGORY string1_category, string2_category;
  int cmp_result = 0;
  DB_TYPE str1_type, str2_type;

  /* Assert that DB_VALUE structures have been allocated. */
  assert (string1 != (DB_VALUE *) NULL);
  assert (string2 != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);

  /* Categorize the two input parameters and check for errors.
     Verify that the parameters are both character strings.
     Verify that the input strings belong to compatible categories. */
  string1_category = qstr_get_category (string1);
  string2_category = qstr_get_category (string2);

  str1_type = DB_VALUE_DOMAIN_TYPE (string1);
  str2_type = DB_VALUE_DOMAIN_TYPE (string2);

  if (!QSTR_IS_ANY_CHAR_OR_BIT (str1_type)
      || !QSTR_IS_ANY_CHAR_OR_BIT (str2_type))
    {
      return ER_QSTR_INVALID_DATA_TYPE;
    }
  if (string1_category != string2_category)
    {
      return ER_QSTR_INCOMPATIBLE_CODE_SETS;
    }

  /* A string which is NULL (not the same as a NULL string) is
     ordered less than a string which is not NULL.  Two strings
     which are NULL are ordered equivalently.
     If both strings are not NULL, then the strings themselves
     are compared. */
  if (DB_IS_NULL (string1) && !DB_IS_NULL (string2))
    {
      cmp_result = -1;
    }
  else if (!DB_IS_NULL (string1) && DB_IS_NULL (string2))
    {
      cmp_result = 1;
    }
  else if (DB_IS_NULL (string1) && DB_IS_NULL (string2))
    {
      cmp_result = 0;
    }
  else
    {
      switch (string1_category)
	{
	case QSTR_CHAR:
	  cmp_result =
	    qstr_compare ((unsigned char *) DB_PULL_STRING (string1),
			  (int) DB_GET_STRING_SIZE (string1),
			  (unsigned char *) DB_PULL_STRING (string2),
			  (int) DB_GET_STRING_SIZE (string2));
	  break;
	case QSTR_NATIONAL_CHAR:
	  cmp_result =
	    varnchar_compare ((unsigned char *) DB_PULL_STRING (string1),
			      (int) DB_GET_STRING_SIZE (string1),
			      (unsigned char *) DB_PULL_STRING (string2),
			      (int) DB_GET_STRING_SIZE (string2),
			      (INTL_CODESET) DB_GET_STRING_CODESET (string1));
	  break;
	case QSTR_BIT:
	  cmp_result =
	    varbit_compare ((unsigned char *) DB_PULL_STRING (string1),
			    (int) DB_GET_STRING_SIZE (string1),
			    (unsigned char *) DB_PULL_STRING (string2),
			    (int) DB_GET_STRING_SIZE (string2));
	  break;
	default:		/* QSTR_UNKNOWN */
	  break;
	}
    }

  if (cmp_result < 0)
    {
      cmp_result = -1;
    }
  else if (cmp_result > 0)
    {
      cmp_result = 1;
    }
  DB_MAKE_INTEGER (result, cmp_result);

  return NO_ERROR;
}

/*
 * db_string_unique_prefix () -
 *
 * Arguments:
 *                string1: (IN) Left side of compare.
 *                string2: (IN) Right side of compare.
 *                 result: (OUT) string such that >= string1, and < string2.
 *
 * Returns: int
 *
 * Errors:
 *    (TBD)
 *
 * Note:
 *    The purpose of this routine is to find a prefix that is greater
 *    than or equal to the first string but strictly less than the second
 *    string.
 *
 *    This routine assumes:
 *       a) The second string is strictly greater than the first
 *           (according to the ANSI SQL string comparison rules).
 *       b) The two strings are both of the same 'type', although one may be
 *           'fixed' and the other may be 'varying'.
 *       c) No padding is done.
 *
 * Assert:
 *
 *    1. string1 != (DB_VALUE *)NULL
 *    2. string2 != (DB_VALUE *)NULL
 *    3. result  != (DB_VALUE *)NULL
 *
 */
#if 1
int
db_string_unique_prefix (const DB_VALUE * db_string1,
			 const DB_VALUE * db_string2, DB_VALUE * db_result,
			 int is_reverse)
{
  DB_TYPE result_type = (DB_TYPE) 0;
  int error_status = NO_ERROR;
  int precision;
  DB_VALUE tmp_result;
  int c;

  /* Assertions */
  assert (db_string1 != (DB_VALUE *) NULL);
  assert (db_string2 != (DB_VALUE *) NULL);
  assert (db_result != (DB_VALUE *) NULL);

  error_status = db_string_compare (db_string1, db_string2, &tmp_result);
  if ((error_status != NO_ERROR) ||
      ((c = DB_GET_INTEGER (&tmp_result)) &&
       ((!is_reverse && c > 0) || (is_reverse && c < 0))))
    {
      DB_MAKE_NULL (db_result);
#if defined(CUBRID_DEBUG)
      if (error_status == ER_QSTR_INVALID_DATA_TYPE)
	{
	  printf ("db_string_unique_prefix(): non-string type: %s and %s\n",
		  pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string1)),
		  pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string2)));
	}
      if (error_status == ER_QSTR_INCOMPATIBLE_CODE_SETS)
	{
	  printf
	    ("db_string_unique_prefix(): incompatible types: %s and %s\n",
	     pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string1)),
	     pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string2)));
	}
      if (DB_GET_INTEGER (&tmp_result) > 0)
	{
	  printf
	    ("db_string_unique_prefix(): string1 %s, greater than string2 %s\n",
	     DB_GET_STRING (db_string1), DB_GET_STRING (db_string2));
	}
#endif
      return ER_GENERIC_ERROR;
    }

  precision = DB_VALUE_PRECISION (db_string1);
  /* Determine the result type */
  result_type = DB_VALUE_DOMAIN_TYPE (db_string1);
  if (QSTR_IS_CHAR (result_type))
    {
      result_type = DB_TYPE_VARCHAR;
    }
  else if (QSTR_IS_NATIONAL_CHAR (result_type))
    {
      result_type = DB_TYPE_VARNCHAR;
    }
  else if (QSTR_IS_BIT (result_type))
    {
      result_type = DB_TYPE_VARBIT;
    }
  else
    {
      DB_MAKE_NULL (db_result);
#if defined(CUBRID_DEBUG)
      printf ("db_string_unique_prefix(): non-string type: %s and %s\n",
	      pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string1)),
	      pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string2)));
#endif
      return ER_GENERIC_ERROR;
    }

  /* A string which is NULL (not the same as a NULL string) is
     ordered less than a string which is not NULL.  Since string2 is
     assumed to be strictly > string1, string2 can never be NULL. */
  if (DB_IS_NULL (db_string1))
    {
      db_value_domain_init (db_result, result_type, precision, 0);
    }

  /* Find the first byte where the 2 strings differ.  Set the result
     accordingly. */
  else
    {
      int size1, size2, result_size, pad_size = 0, n;
      unsigned char *string1, *string2, *result, *key, pad[2], *t, *t2;
      INTL_CODESET codeset;
      int num_bits = -1;

      string1 = (unsigned char *) DB_GET_STRING (db_string1);
      size1 = (int) DB_GET_STRING_SIZE (db_string1);
      string2 = (unsigned char *) DB_GET_STRING (db_string2);
      size2 = (int) DB_GET_STRING_SIZE (db_string2);
      codeset = (INTL_CODESET) DB_GET_STRING_CODESET (db_string1);

      if (string1 == NULL || string2 == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_INVALID_PARAMETER, 0);
	  return ER_QPROC_INVALID_PARAMETER;
	}

      qstr_pad_char (codeset, pad, &pad_size);

      /* We need to implicitly trim both strings since we don't want padding
         for the result (its of varying type) and since padding can mask the
         logical end of both of the strings.  We need to be careful how the
         trimming is done.  Char and varchar can do the normal trim, nchar
         and varnchar need to worry about codeset and pad chars, and bit
         and varbit don't want to trim at all. */
      if (result_type == DB_TYPE_VARCHAR)
	{
	  for (t = string1 + (size1 - 1); t >= string1 && *t == pad[0];
	       t--, size1--)
	    {
	      ;
	    }
	  for (t = string2 + (size2 - 1); t >= string2 && *t == pad[0];
	       t--, size2--)
	    {
	      ;
	    }
	}
      else if (result_type == DB_TYPE_VARNCHAR)
	{
	  /* This is going to look a lot like trim_trailing.  We don't call
	     trim_trailing because he works on length of characters and we
	     need to work on length of bytes.  We could calculate the length
	     in characters, but that requires a full scan of the strings
	     which is not necessary. */
	  for (t = qstr_prev_char (string1 + size1, codeset, &n);
	       t >= string1 && n == pad_size
	       && memcmp (t, pad, pad_size) == 0;
	       t = qstr_prev_char (t, codeset, &n), size1 -= n)
	    {
	      ;
	    }
	  for (t = qstr_prev_char (string2 + size2, codeset, &n);
	       t >= string2 && n == pad_size
	       && memcmp (t, pad, pad_size) == 0;
	       t = qstr_prev_char (t, codeset, &n), size2 -= n)
	    {
	      ;
	    }
	}

      /* now find the first byte where the strings differ */
      for (result_size = 0, t = string1, t2 = string2;
	   result_size < size1 && result_size < size2 && *t++ == *t2++;
	   result_size++)
	{
	  ;
	}

      if (!is_reverse)
	{			/* normal index */
	  if (result_size == size1 || result_size == size2 - 1)
	    {
	      key = string1;
	      result_size = size1;
	      /* if we have bits, we need all the string1 bits.  Remember
	         that you may not use all of the bits in the last byte. */
	      if (result_type == DB_TYPE_VARBIT)
		{
		  num_bits = db_string1->data.ch.medium.size;
		}
	    }
	  else
	    {
	      result_size += 1;
	      key = string2;

	      /* if we have bits, we will take all the bits for the
	         differentiating byte.  This is fine since in this branch
	         we are guaranteed not to be at the end of either string. */
	      if (result_type == DB_TYPE_VARBIT)
		num_bits = result_size * BYTE_SIZE;
	    }
	}
      else
	{			/* reverse index */
	  if (result_size == size1)
	    {
	      /* actually, this could not happen */
	      /* only when string1 == string2 */
	      key = string1;
	      result_size = size1;
	      /* if we have bits, we need all the string1 bits.  Remember
	         that you may not use all of the bits in the last byte. */
	      if (result_type == DB_TYPE_VARBIT)
		{
		  num_bits = db_string1->data.ch.medium.size;
		}
	    }
	  else
	    {
	      result_size += 1;
	      key = string1;

	      /* if we have bits, we will take all the bits for the
	         differentiating byte.  This is fine since in this branch
	         we are guaranteed not to be at the end of either string. */
	      if (result_type == DB_TYPE_VARBIT)
		{
		  num_bits = result_size * BYTE_SIZE;
		}
	    }
	}

      result = db_private_alloc (NULL, result_size + 1);
      if (result)
	{
	  if (result_size)
	    {
	      (void) memcpy (result, key, result_size);
	    }
	  result[result_size] = 0;
	  db_value_domain_init (db_result, result_type, precision, 0);
	  error_status = db_make_db_char (db_result, codeset,
					  (const char *) result,
					  (result_type ==
					   DB_TYPE_VARBIT ? num_bits :
					   result_size));
	  db_result->need_clear = true;
	}
      else
	{
	  /* will already be set by memory mgr */
	  error_status = er_errid ();
	}
    }

  return (error_status);
}
#else
int
db_string_unique_prefix (const DB_VALUE * db_string1,
			 const DB_VALUE * db_string2, DB_VALUE * db_result)
{
  DB_TYPE result_type = 0;
  int error_status = NO_ERROR;
  int precision, num_bits = -1;
  DB_TYPE string_type;

  /* Assertions */
  assert (db_string1 != (DB_VALUE *) NULL);
  assert (db_string2 != (DB_VALUE *) NULL);
  assert (db_result != (DB_VALUE *) NULL);

  precision = DB_VALUE_PRECISION (db_string1);
  string_type = DB_VALUE_DOMAIN_TYPE (db_string1);

  /* Determine the result type */
  if (QSTR_IS_CHAR (string_type))
    {
      result_type = DB_TYPE_VARCHAR;
    }
  else if (QSTR_IS_NATIONAL_CHAR (string_type))
    {
      result_type = DB_TYPE_VARNCHAR;
    }
  else if (QSTR_IS_BIT (string_type))
    {
      result_type = DB_TYPE_VARBIT;
    }
  else
    {
      result_type = DB_TYPE_NULL;
      DB_MAKE_NULL (db_result);
#if defined(CUBRID_DEBUG)
      printf ("db_string_unique_prefix called with non-string type: %s\n",
	      pr_type_name (string_type));
#endif
      return ER_GENERIC_ERROR;
    }

  /*
   * A string which is NULL (not the same as a NULL string) is
   * ordered less than a string which is not NULL.  Since string2 is
   * assumed to be strictly > string1, string2 can never be NULL.
   */
  if (DB_IS_NULL (db_string1))
    db_value_domain_init (db_result, result_type, precision, 0);

  /*
   *  Find the first byte where the 2 strings differ.  Set the result
   *  accordingly.
   */
  else
    {
      int string1_size = DB_GET_STRING_SIZE (db_string1);
      int string2_size = DB_GET_STRING_SIZE (db_string2);
      const unsigned char *string1 =
	(const unsigned char *) DB_GET_STRING (db_string1);
      const unsigned char *string2 =
	(const unsigned char *) DB_GET_STRING (db_string2);
      unsigned char *result;
      const unsigned char *key;
      int result_size;
      INTL_CODESET codeset =
	(INTL_CODESET) DB_GET_STRING_CODESET ((DB_VALUE *) db_string1);

      /* We need to implicitly trim both strings since we don't want padding
       * for the result (its of varying type) and since padding can mask the
       * logical end of both of the strings.  We need to be careful how the
       * trimming is done.  Char and varchar can do the normal trim, nchar
       * and varnchar need to worry about codeset and pad chars, and bit
       * and varbit don't want to trim at all.
       */
      if (result_type == DB_TYPE_VARCHAR)
	{
	  for (;
	       string1_size && string1[string1_size - 1] == ' ';
	       string1_size--)
	    {
	      ;			/* do nothing */
	    }
	  for (;
	       string2_size && string2[string2_size - 1] == ' ';
	       string2_size--)
	    {
	      ;			/* do nothing */
	    }
	}
      else if (result_type == DB_TYPE_VARNCHAR)
	{
	  /* This is going to look a lot like trim_trailing.  We don't call
	   * trim_trailing because he works on length of characters and we
	   * need to work on length of bytes.  We could calculate the length
	   * in characters, but that requires a full scan of the strings
	   * which is not necessary.
	   */
	  int i, pad_size, trim_length, cmp_flag, prev_size;
	  unsigned char *prev_ptr, *current_ptr, pad[2];

	  qstr_pad_char (codeset, pad, &pad_size);

	  trim_length = string1_size;
	  current_ptr = (unsigned char *) (string1 + string1_size);
	  for (i = 0, cmp_flag = 0;
	       (i < string1_size) && (cmp_flag == 0); i++)
	    {
	      prev_ptr = qstr_prev_char (current_ptr, codeset, &prev_size);
	      if (pad_size == prev_size)
		{
		  cmp_flag =
		    memcmp ((char *) prev_ptr, (char *) pad, pad_size);

		  if (cmp_flag == 0)
		    {
		      trim_length -= pad_size;
		    }
		}
	      else
		{
		  cmp_flag = 1;
		}

	      current_ptr = prev_ptr;
	    }
	  string1_size = trim_length;

	  trim_length = string2_size;
	  current_ptr = (unsigned char *) (string2 + string2_size);
	  for (i = 0, cmp_flag = 0;
	       (i < string2_size) && (cmp_flag == 0); i++)
	    {
	      prev_ptr = qstr_prev_char (current_ptr, codeset, &prev_size);
	      if (pad_size == prev_size)
		{
		  cmp_flag =
		    memcmp ((char *) prev_ptr, (char *) pad, pad_size);

		  if (cmp_flag == 0)
		    {
		      trim_length -= pad_size;
		    }
		}
	      else
		{
		  cmp_flag = 1;
		}

	      current_ptr = prev_ptr;
	    }
	  string2_size = trim_length;

	}

      /* now find the first byte where the strings differ */
      for (result_size = 0;
	   result_size < string1_size && result_size < string2_size &&
	   string1[result_size] == string2[result_size]; result_size++)
	{
	  ;			/* do nothing */
	}

      /* Check for string2 < string1.  This check can only be done
       * when we haven't exhausted one of the strings.  If string2
       * is exhausted it is an error.
       */
      if ((result_size != string1_size) &&
	  ((result_size == string2_size) ||
	   (string2[result_size] < string1[result_size])))
	{
#if defined(CUBRID_DEBUG)
	  printf ("db_string_unique_prefix called with ");
	  printf ("string1: %s, greater than string2: %s\n",
		  string1, string2);
#endif
	  error_status = ER_GENERIC_ERROR;
	}
      else
	{

	  if (result_size == string1_size || result_size == string2_size - 1)
	    {
	      key = string1;
	      result_size = string1_size;
	      /* if we have bits, we need all the string1 bits.  Remember
	       * that you may not use all of the bits in the last byte.
	       */
	      if (result_type == DB_TYPE_VARBIT)
		{
		  num_bits = db_string1->data.ch.medium.size;
		}
	    }
	  else
	    {
	      result_size += 1;
	      key = string2;
	      /* if we have bits, we will take all the bits for the
	       * differentiating byte.  This is fine since in this branch
	       * we are guaranteed not to be at the end of either string.
	       */
	      if (result_type == DB_TYPE_VARBIT)
		{
		  num_bits = result_size * BYTE_SIZE;
		}
	    }

	  result = db_private_alloc (NULL, result_size + 1);
	  if (result)
	    {
	      if (result_size)
		{
		  memcpy (result, key, result_size);
		}
	      result[result_size] = 0;
	      db_value_domain_init (db_result, result_type, precision, 0);
	      if (result_type == DB_TYPE_VARBIT)
		{
		  error_status = db_make_db_char (db_result, codeset,
						  (const char *) result,
						  num_bits);
		}
	      else
		{
		  error_status = db_make_db_char (db_result, codeset,
						  (const char *) result,
						  result_size);
		}

	      db_result->need_clear = true;
	    }
	  else
	    {
	      /* will already be set by memory mgr */
	      error_status = er_errid ();
	    }
	}
    }

  return (error_status);
}
#endif

/*
 * db_string_concatenate () -
 *
 * Arguments:
 *          string1: Left string to concatenate.
 *          string2: Right string to concatenate.
 *           result: Result of concatenation of both strings.
 *      data_status: DB_DATA_STATUS which indicates if truncation occurred.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE     :
 *          <string1> or <string2> not string types.
 *
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *          <string1> or <string2> have different character code sets
 *          or are not all bit strings.
 *
 */

int
db_string_concatenate (const DB_VALUE * string1,
		       const DB_VALUE * string2,
		       DB_VALUE * result, DB_DATA_STATUS * data_status)
{
  QSTR_CATEGORY string1_code_set, string2_code_set;
  int error_status = NO_ERROR;
  DB_TYPE string_type1, string_type2;

  /*
   *  Initialize status value
   */
  *data_status = DATA_STATUS_OK;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string1 != (DB_VALUE *) NULL);
  assert (string2 != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);

  /*
   *  Categorize the parameters into respective code sets.
   *  Verify that the parameters are both character strings.
   *  Verify that the input strings belong to compatible code sets.
   */
  string1_code_set = qstr_get_category (string1);
  string2_code_set = qstr_get_category (string2);

  string_type1 = DB_VALUE_DOMAIN_TYPE (string1);
  string_type2 = DB_VALUE_DOMAIN_TYPE (string2);

  if (!QSTR_IS_ANY_CHAR_OR_BIT (string_type1)
      || !QSTR_IS_ANY_CHAR_OR_BIT (string_type2))
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
      if (PRM_ORACLE_STYLE_EMPTY_STRING)
	{
	  if (DB_IS_NULL (string1) && QSTR_IS_ANY_CHAR_OR_BIT (string_type2))
	    {
	      db_value_clone ((DB_VALUE *) string2, result);
	    }
	  else if (DB_IS_NULL (string2)
		   && QSTR_IS_ANY_CHAR_OR_BIT (string_type1))
	    {
	      db_value_clone ((DB_VALUE *) string1, result);
	    }
	  else
	    {
	      error_status = ER_QSTR_INVALID_DATA_TYPE;
	    }
	}
      else
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	}
    }
  else if ((string1_code_set != string2_code_set))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
    }
  else if (DB_IS_NULL (string1) || DB_IS_NULL (string2))
    {
      bool check_empty_string;

      /* ORACLE7 ServerSQL Language Reference Manual 3-4;
       * Although ORACLE treats zero-length character strings as
       * nulls, concatenating a zero-length character string with another
       * operand always results in the other operand, rather than a null.
       * However, this may not continue to be true in future versions of
       * ORACLE. To concatenate an expression that might be null, use the
       * NVL function to explicitly convert the expression to a
       * zero-length string.
       */
      check_empty_string = PRM_ORACLE_STYLE_EMPTY_STRING ? true : false;

      if (check_empty_string && DB_IS_NULL (string1)
	  && QSTR_IS_ANY_CHAR_OR_BIT (string_type2))
	{
	  db_value_clone ((DB_VALUE *) string2, result);
	}
      else if (check_empty_string && DB_IS_NULL (string2)
	       && QSTR_IS_ANY_CHAR_OR_BIT (string_type1))
	{
	  db_value_clone ((DB_VALUE *) string1, result);
	}
      else
	{
	  if (QSTR_IS_CHAR (string_type1))
	    {
	      if (string_type1 == DB_TYPE_VARCHAR
		  || string_type2 == DB_TYPE_VARCHAR)
		{
		  db_value_domain_init (result, DB_TYPE_VARCHAR, 0, 0);
		}
	      else
		{
		  db_value_domain_init (result, DB_TYPE_CHAR, 0, 0);
		}
	    }
	  else if (QSTR_IS_NATIONAL_CHAR (string_type1))
	    {
	      if (string_type1 == DB_TYPE_VARNCHAR
		  || string_type2 == DB_TYPE_VARNCHAR)
		{
		  db_value_domain_init (result, DB_TYPE_VARNCHAR, 0, 0);
		}
	      else
		{
		  db_value_domain_init (result, DB_TYPE_NCHAR, 0, 0);
		}
	    }
	  else
	    {
	      if (string_type1 == DB_TYPE_VARBIT
		  || string_type2 == DB_TYPE_VARBIT)
		{
		  db_value_domain_init (result, DB_TYPE_VARBIT, 0, 0);
		}
	      else
		{
		  db_value_domain_init (result, DB_TYPE_BIT, 0, 0);
		}
	    }
	}
    }
  else
    {
      unsigned char *r;
      int r_length, r_size;
      DB_TYPE r_type;

      if (string1_code_set == QSTR_BIT)
	{
	  int result_domain_length;

	  error_status =
	    qstr_bit_concatenate ((unsigned char *) DB_PULL_STRING (string1),
				  (int) DB_GET_STRING_LENGTH (string1),
				  (int) QSTR_VALUE_PRECISION (string1),
				  DB_VALUE_DOMAIN_TYPE (string1),
				  (unsigned char *) DB_PULL_STRING (string2),
				  (int) DB_GET_STRING_LENGTH (string2),
				  (int) QSTR_VALUE_PRECISION (string2),
				  DB_VALUE_DOMAIN_TYPE (string2),
				  &r, &r_length,
				  &r_size, &r_type, data_status);

	  if (error_status == NO_ERROR)
	    {
	      if ((DB_VALUE_PRECISION (string1) ==
		   TP_FLOATING_PRECISION_VALUE) ||
		  (DB_VALUE_PRECISION (string2) ==
		   TP_FLOATING_PRECISION_VALUE))
		{
		  result_domain_length = TP_FLOATING_PRECISION_VALUE;
		}
	      else
		{
		  result_domain_length = MIN (DB_MAX_BIT_LENGTH,
					      DB_VALUE_PRECISION (string1) +
					      DB_VALUE_PRECISION (string2));
		}

	      qstr_make_typed_string (r_type,
				      result,
				      result_domain_length,
				      (char *) r, r_length);
	      result->need_clear = true;
	    }
	}
      else
	{
	  int result_domain_length;

	  error_status =
	    qstr_concatenate ((unsigned char *) DB_PULL_STRING (string1),
			      (int) DB_GET_STRING_LENGTH (string1),
			      (int) QSTR_VALUE_PRECISION (string1),
			      DB_VALUE_DOMAIN_TYPE (string1),
			      (unsigned char *) DB_PULL_STRING (string2),
			      (int) DB_GET_STRING_LENGTH (string2),
			      (int) QSTR_VALUE_PRECISION (string2),
			      DB_VALUE_DOMAIN_TYPE (string2),
			      (INTL_CODESET) DB_GET_STRING_CODESET (string1),
			      &r, &r_length, &r_size, &r_type, data_status);

	  if (error_status == NO_ERROR && r != NULL)
	    {
	      if ((DB_VALUE_PRECISION (string1) ==
		   TP_FLOATING_PRECISION_VALUE) ||
		  (DB_VALUE_PRECISION (string2) ==
		   TP_FLOATING_PRECISION_VALUE))
		{
		  result_domain_length = TP_FLOATING_PRECISION_VALUE;
		}
	      else
		{
		  result_domain_length = MIN (QSTR_MAX_PRECISION (r_type),
					      DB_VALUE_PRECISION (string1) +
					      DB_VALUE_PRECISION (string2));
		}

	      qstr_make_typed_string (r_type,
				      result,
				      result_domain_length,
				      (char *) r, r_size);
	      r[r_size] = 0;
	      result->need_clear = true;
	    }
	}
    }

  return error_status;
}

/*
 * db_string_chr () - take character of db_value
 *   return: NO_ERROR, or ER_code
 *   res(OUT)   : resultant db_value node
 *   dbval1(IN) : first db_value node
 */

int
db_string_chr (DB_VALUE * res, DB_VALUE * dbval1)
{
  DB_TYPE res_type;
  int itmp;
  DB_BIGINT bi;
  double dtmp;
  char buf[2];
  DB_VALUE v;
  int ret = NO_ERROR;

  res_type = DB_VALUE_DOMAIN_TYPE (dbval1);

  if (res_type == DB_TYPE_NULL || DB_IS_NULL (dbval1))
    {
      return ret;
    }

  switch (res_type)
    {
    case DB_TYPE_SHORT:
      itmp = DB_GET_SHORT (dbval1);
      if (itmp < 0)
	{
	  break;
	}
      buf[0] = (char) (itmp % 256);
      buf[1] = '\0';
      DB_MAKE_CHAR (&v, 1, buf, 1);
      pr_clone_value (&v, res);
      break;
    case DB_TYPE_INTEGER:
      itmp = DB_GET_INTEGER (dbval1);
      if (itmp < 0)
	{
	  break;
	}
      buf[0] = (char) (itmp % 256);
      buf[1] = '\0';
      DB_MAKE_CHAR (&v, 1, buf, 1);
      pr_clone_value (&v, res);
      break;
    case DB_TYPE_BIGINT:
      bi = DB_GET_BIGINT (dbval1);
      if (bi < 0)
	{
	  break;
	}
      buf[0] = (char) (bi % 256);
      buf[1] = '\0';
      DB_MAKE_CHAR (&v, 1, buf, 1);
      pr_clone_value (&v, res);
      break;
    case DB_TYPE_FLOAT:
      dtmp = DB_GET_FLOAT (dbval1);
      if (dtmp < 0)
	{
	  break;
	}
      buf[0] = (char) fmod (floor (dtmp), 256);
      buf[1] = '\0';
      DB_MAKE_CHAR (&v, 1, buf, 1);
      pr_clone_value (&v, res);
      break;
    case DB_TYPE_DOUBLE:
      dtmp = DB_GET_DOUBLE (dbval1);
      if (dtmp < 0)
	{
	  break;
	}
      buf[0] = (char) fmod (floor (dtmp), 256);
      buf[1] = '\0';
      DB_MAKE_CHAR (&v, 1, buf, 1);
      pr_clone_value (&v, res);
      break;
    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double (db_locate_numeric (dbval1),
				    DB_VALUE_SCALE (dbval1), &dtmp);
      if (dtmp < 0)
	{
	  break;
	}
      buf[0] = (char) fmod (floor (dtmp), 256);
      buf[1] = '\0';
      DB_MAKE_CHAR (&v, 1, buf, 1);
      pr_clone_value (&v, res);
      break;
    case DB_TYPE_MONETARY:
      dtmp = (DB_GET_MONETARY (dbval1))->amount;
      if (dtmp < 0)
	{
	  break;
	}
      buf[0] = (char) fmod (floor (dtmp), 256);
      buf[1] = '\0';
      DB_MAKE_CHAR (&v, 1, buf, 1);
      pr_clone_value (&v, res);
      break;
    default:
      break;
    }				/* switch */

  return ret;
}

/*
 * db_string_instr () -
 *
 * Arguments:
 *      sub_string: String fragment to search for within <src_string>.
 *      src_string: String to be searched.
 *          result: Character or bit position of the first <sub_string>
 *                  occurance.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE     :
 *         <sub_string> or <src_string> are not a character strings.
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *         <sub_string> and <src_string> have different character
 *         code sets, or are not both bit strings.
 *
 */

int
db_string_instr (const DB_VALUE * src_string,
		 const DB_VALUE * sub_string,
		 const DB_VALUE * start_pos, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_TYPE str1_type, str2_type;
  DB_TYPE arg3_type;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_string != (DB_VALUE *) NULL);
  assert (sub_string != (DB_VALUE *) NULL);
  assert (start_pos != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);

  /*
   *  Categorize the parameters into respective code sets.
   *  Verify that the parameters are both character strings.
   *  Verify that the input strings belong to compatible code sets.
   */
  str1_type = DB_VALUE_DOMAIN_TYPE (src_string);
  str2_type = DB_VALUE_DOMAIN_TYPE (sub_string);
  arg3_type = DB_VALUE_DOMAIN_TYPE (start_pos);

  if (DB_IS_NULL (src_string) || DB_IS_NULL (sub_string) ||
      DB_IS_NULL (start_pos))
    {
      DB_MAKE_NULL (result);
    }
  else
    {
      if (!(str1_type == DB_TYPE_STRING || str1_type == DB_TYPE_CHAR
	    || str1_type == DB_TYPE_VARCHAR || str1_type == DB_TYPE_NCHAR
	    || str1_type == DB_TYPE_VARNCHAR)
	  || !(str2_type == DB_TYPE_STRING || str2_type == DB_TYPE_CHAR
	       || str2_type == DB_TYPE_VARCHAR || str2_type == DB_TYPE_NCHAR
	       || str2_type == DB_TYPE_VARNCHAR)
	  || !(arg3_type == DB_TYPE_INTEGER || arg3_type == DB_TYPE_SHORT
	       || arg3_type == DB_TYPE_BIGINT))
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	}
      else if (qstr_get_category (src_string) !=
	       qstr_get_category (sub_string))
	{
	  error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
	}
      else
	{
	  int position = 0;
	  int src_str_len = DB_GET_STRING_LENGTH (src_string);
	  int sub_str_len = DB_GET_STRING_LENGTH (sub_string);
	  int offset = DB_GET_INT (start_pos);
	  INTL_CODESET codeset =
	    (INTL_CODESET) DB_GET_STRING_CODESET (src_string);
	  char *search_from;

	  if (offset > 0)
	    {
	      offset--;
	      if (offset + sub_str_len > src_str_len)
		{		/* out of bound */
		  position = 0;
		}
	      else
		{
		  search_from = DB_PULL_STRING (src_string) + offset;

		  intl_char_count ((unsigned char *) search_from,
				   strlen (search_from), codeset,
				   &src_str_len);

		  /* forward search */
		  error_status =
		    qstr_position (DB_PULL_STRING (sub_string),
				   sub_str_len, search_from, src_str_len,
				   codeset, true, &position);
		  position += (position != 0) ? offset : 0;
		}
	    }
	  else if (offset < 0)
	    {
	      if (src_str_len + offset + 1 < sub_str_len)
		{
		  position = 0;
		}
	      else
		{
		  search_from = DB_PULL_STRING (src_string);
		  search_from += src_str_len + offset - (sub_str_len - 1);

		  /* backward search */
		  error_status =
		    qstr_position (DB_PULL_STRING (sub_string),
				   sub_str_len, search_from,
				   src_str_len + offset + 1, codeset,
				   false, &position);
		  if (position != 0)
		    {
		      position = src_str_len - (-offset - 1)
			- (position - 1) - (sub_str_len - 1);
		    }
		}
	    }
	  else
	    {
	      /* offset == 0 */
	      position = 0;
	    }

	  if (error_status == NO_ERROR)
	    {
	      DB_MAKE_INTEGER (result, position);
	    }
	}
    }

  return error_status;
}


/*
 * db_string_position () -
 *
 * Arguments:
 *      sub_string: String fragment to search for within <src_string>.
 *      src_string: String to be searched.
 *          result: Character or bit position of the first <sub_string>
 *                  occurance.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE     :
 *         <sub_string> or <src_string> are not a character strings.
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *         <sub_string> and <src_string> have different character
 *         code sets, or are not both bit strings.
 *
 */

int
db_string_position (const DB_VALUE * sub_string,
		    const DB_VALUE * src_string, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_TYPE str1_type, str2_type;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (sub_string != (DB_VALUE *) NULL);
  assert (src_string != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);


  /*
   *  Categorize the parameters into respective code sets.
   *  Verify that the parameters are both character strings.
   *  Verify that the input strings belong to compatible code sets.
   */
  str1_type = DB_VALUE_DOMAIN_TYPE (sub_string);
  str2_type = DB_VALUE_DOMAIN_TYPE (src_string);

  if (DB_IS_NULL (sub_string) || DB_IS_NULL (src_string))
    {
      DB_MAKE_NULL (result);
    }
  else if (!QSTR_IS_ANY_CHAR_OR_BIT (str1_type)
	   || !QSTR_IS_ANY_CHAR_OR_BIT (str2_type))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
    }
  else if (qstr_get_category (sub_string) != qstr_get_category (src_string))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
    }
  else
    {
      int position;
      DB_TYPE src_type = DB_VALUE_DOMAIN_TYPE (src_string);

      if (QSTR_IS_CHAR (src_type) || QSTR_IS_NATIONAL_CHAR (src_type))
	{
	  error_status =
	    qstr_position (DB_PULL_STRING (sub_string),
			   DB_GET_STRING_LENGTH (sub_string),
			   DB_PULL_STRING (src_string),
			   DB_GET_STRING_LENGTH (src_string),
			   (INTL_CODESET) DB_GET_STRING_CODESET (src_string),
			   true, &position);
	}
      else
	{
	  error_status =
	    qstr_bit_position ((unsigned char *) DB_PULL_STRING (sub_string),
			       DB_GET_STRING_LENGTH (sub_string),
			       (unsigned char *) DB_PULL_STRING (src_string),
			       DB_GET_STRING_LENGTH (src_string), &position);
	}

      if (error_status == NO_ERROR)
	{
	  DB_MAKE_INTEGER (result, position);
	}
    }

  return error_status;
}

/*
 * db_string_substring
 *
 * Arguments:
 *             src_string: String from which extraction will occur.
 *              start_pos: Character position to begin extraction from.
 *      extraction_length: Number of characters to extract (Optional).
 *             sub_string: Extracted subtring is returned here.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE     :
 *         <src_string> is not a string type,
 *         <start_pos> or <extraction_length>  is not an integer type
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *         <src_string> have different character
 *         code sets or are not both bit strings.
 *
 */

int
db_string_substring (const MISC_OPERAND substr_operand,
		     const DB_VALUE * src_string,
		     const DB_VALUE * start_position,
		     const DB_VALUE * extraction_length,
		     DB_VALUE * sub_string)
{
  int error_status = NO_ERROR;
  int extraction_length_is_null = false;
  DB_TYPE result_type;
  DB_TYPE src_type;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_string != (DB_VALUE *) NULL);
  assert (start_position != (DB_VALUE *) NULL);
  assert (sub_string != (DB_VALUE *) NULL);

  src_type = DB_VALUE_DOMAIN_TYPE (src_string);

  if ((extraction_length == NULL) || DB_IS_NULL (extraction_length))
    {
      extraction_length_is_null = true;
    }

  if (QSTR_IS_CHAR (src_type))
    {
      result_type = DB_TYPE_VARCHAR;
    }
  else if (QSTR_IS_NATIONAL_CHAR (src_type))
    {
      result_type = DB_TYPE_VARNCHAR;
    }
  else
    {
      result_type = DB_TYPE_VARBIT;
    }

  if (DB_IS_NULL (src_string) || DB_IS_NULL (start_position))
    {
      DB_MAKE_NULL (sub_string);
    }
  else
    {
      if (!QSTR_IS_ANY_CHAR_OR_BIT (src_type)
	  || !is_integer (start_position)
	  || (!extraction_length_is_null && !is_integer (extraction_length)))
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	}
      else
	{
	  unsigned char *sub;
	  int sub_length;

	  int extract_nchars = -1;

	  if (!extraction_length_is_null)
	    {
	      extract_nchars = (int) DB_GET_INTEGER (extraction_length);
	    }

	  /* Initialize the memory manager of the substring */
	  if (QSTR_IS_CHAR (src_type) || QSTR_IS_NATIONAL_CHAR (src_type))
	    {
	      int sub_size = 0;

	      unsigned char *string =
		(unsigned char *) DB_PULL_STRING (src_string);
	      int start_offset = DB_GET_INTEGER (start_position);
	      int string_len = DB_GET_STRING_LENGTH (src_string);

	      if (extraction_length_is_null)
		{
		  extract_nchars = string_len;
		}

	      if (substr_operand == SUBSTR)
		{
		  if (extract_nchars < 0 || string_len < ABS (start_offset))
		    {
		      return error_status;
		    }

		  if (start_offset < 0)
		    {
		      string += string_len + start_offset;
		      string_len = -start_offset;
		    }
		}

	      error_status =
		qstr_substring (string, string_len, start_offset,
				extract_nchars,
				DB_GET_STRING_CODESET (src_string), &sub,
				&sub_length, &sub_size);
	      if (error_status == NO_ERROR && sub != NULL)
		{
		  qstr_make_typed_string (result_type, sub_string,
					  DB_VALUE_PRECISION (src_string),
					  (char *) sub, sub_size);
		  sub[sub_size] = 0;
		  sub_string->need_clear = true;
		}
	    }
	  else
	    {
	      error_status =
		qstr_bit_substring ((unsigned char *)
				    DB_PULL_STRING (src_string),
				    (int) DB_GET_STRING_LENGTH (src_string),
				    (int) DB_GET_INTEGER (start_position),
				    extract_nchars, &sub, &sub_length);
	      if (error_status == NO_ERROR)
		{
		  qstr_make_typed_string (result_type,
					  sub_string,
					  DB_VALUE_PRECISION (src_string),
					  (char *) sub, sub_length);
		  sub_string->need_clear = true;
		}
	    }
	}
    }

  return error_status;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_string_byte_length
 *
 * Arguments:
 *          string: (IN)  Input string of which the byte count is desired.
 *      byte_count: (OUT) The number of bytes in string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE:
 *          <string> is not a string type
 *
 * Note:
 *   This function returns the number of bytes in <string>.
 *
 *   If the NULL flag is set for <string>, then the NULL flag
 *   for the <byte_count> is set.
 *
 * Assert:
 *   1. string     != (DB_VALUE *) NULL
 *   2. byte_count != (DB_VALUE *) NULL
 *
 */

int
db_string_byte_length (const DB_VALUE * string, DB_VALUE * byte_count)
{
  int error_status = NO_ERROR;
  DB_TYPE str_type;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string != (DB_VALUE *) NULL);
  assert (byte_count != (DB_VALUE *) NULL);

  /*
   *  Verify that the input string is a valid character
   *  string.  Bit strings are not allowed.
   *
   *  If the input string is a NULL, then set
   *  the output null flag.
   *
   *  Otherwise, calculte the byte size.
   */

  str_type = DB_VALUE_DOMAIN_TYPE (string);
  if (!QSTR_IS_ANY_CHAR_OR_BIT (str_type))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
    }
  else if (DB_IS_NULL (string))
    {
      db_value_domain_init (byte_count, DB_TYPE_INTEGER, 0, 0);
    }
  else
    {
      DB_MAKE_INTEGER (byte_count, DB_GET_STRING_SIZE (string));
    }

  return error_status;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * db_string_bit_length () -
 *
 * Arguments:
 *          string: Inpute string of which the bit length is desired.
 *       bit_count: Bit count of string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE:
 *          <string> is not a string type
 *
 * Note:
 *   This function returns the number of bits in <string>.
 *
 *   If the NULL flag is set for <string>, then the NULL flag
 *   for the <bit_count> is set.
 *
 * Assert:
 *   1. string    != (DB_VALUE *) NULL
 *   2. bit_count != (DB_VALUE *) NULL
 *
 */

int
db_string_bit_length (const DB_VALUE * string, DB_VALUE * bit_count)
{
  int error_status = NO_ERROR;
  DB_TYPE str_type;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string != (DB_VALUE *) NULL);
  assert (bit_count != (DB_VALUE *) NULL);

  /*
   *  Verify that the input string is a valid character string.
   *  Bit strings are not allowed.
   *
   *  If the input string is a NULL, then set the output null flag.
   *
   *  If the input parameter is valid, then extract the byte length
   *  of the string.
   */

  str_type = DB_VALUE_DOMAIN_TYPE (string);
  if (!QSTR_IS_ANY_CHAR_OR_BIT (str_type))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
    }
  else if (DB_IS_NULL (string))
    {
      db_value_domain_init (bit_count, DB_TYPE_INTEGER, 0, 0);
    }
  else
    {
      if (qstr_get_category (string) == QSTR_BIT)
	{
	  DB_MAKE_INTEGER (bit_count, DB_GET_STRING_LENGTH (string));
	}
      else
	{
	  DB_MAKE_INTEGER (bit_count,
			   (DB_GET_STRING_SIZE (string) * BYTE_SIZE));
	}
    }

  return error_status;
}

/*
 * db_string_char_length () -
 *
 * Arguments:
 *          string: String for which the number of characters is desired.
 *      char_count: Number of characters in string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE:
 *          <string> is not a character string
 *
 * Note:
 *   This function returns the number of characters in <string>.
 *
 *   If the NULL flag is set for <string>, then the NULL flag
 *   for the <char_count> is set.
 *
 * Assert:
 *   1. string     != (DB_VALUE *) NULL
 *   2. char_count != (DB_VALUE *) NULL
 *
 */

int
db_string_char_length (const DB_VALUE * string, DB_VALUE * char_count)
{
  int error_status = NO_ERROR;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string != (DB_VALUE *) NULL);
  assert (char_count != (DB_VALUE *) NULL);


  /*
   *  Verify that the input string is a valid character
   *  string.  Bit strings are not allowed.
   *
   *  If the input string is a NULL, then set the output null flag.
   *
   *  If the input parameter is valid, then extract the character
   *  length of the string.
   */
  if (!is_char_string (string))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
    }
  else if (DB_IS_NULL (string))
    {
      db_value_domain_init (char_count, DB_TYPE_INTEGER, 0, 0);
    }
  else
    {
      DB_MAKE_INTEGER (char_count, DB_GET_STRING_LENGTH (string));
    }

  return error_status;
}

/*
 * db_string_lower () -
 *
 * Arguments:
 *            string: Input string that will be converted to lower case.
 *      lower_string: Output converted string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE     :
 *         <string> is not a character string.
 *
 * Note:
 *   This function returns a string with all uppercase ASCII
 *   and LATIN alphabetic characters converted to lowercase.
 *
 *   If the NULL flag is set for <string>, then the NULL flag
 *   for the <lower_string> is set.
 *
 *   The <lower_string> value structure will be cloned from <string>.
 *   <lower_string> should be cleared with db_value_clone() if it has
 *   already been initialized or DB_MAKE_NULL if it has not been
 *   previously used by the system.
 *
 * Assert:
 *
 *   1. string       != (DB_VALUE *) NULL
 *   2. lower_string != (DB_VALUE *) NULL
 *
 */

int
db_string_lower (const DB_VALUE * string, DB_VALUE * lower_string)
{
  int error_status = NO_ERROR;
  DB_TYPE str_type;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string != (DB_VALUE *) NULL);
  assert (lower_string != (DB_VALUE *) NULL);

  /*
   *  Categorize the two input parameters and check for errors.
   *    Verify that the parameters are both character strings.
   */

  str_type = DB_VALUE_DOMAIN_TYPE (string);
  if (DB_IS_NULL (string))
    {
      DB_MAKE_NULL (lower_string);
    }
  else if (!QSTR_IS_ANY_CHAR (str_type))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
    }
  /*
   *  If the input parameters have been properly validated, then
   *  we are ready to operate.
   */
  else
    {
      db_value_clone ((DB_VALUE *) string, lower_string);

      intl_lower_string ((unsigned char *) DB_PULL_STRING (lower_string),
			 DB_GET_STRING_LENGTH (lower_string),
			 (INTL_CODESET) DB_GET_STRING_CODESET (lower_string));
    }

  return error_status;
}

/*
 * db_string_upper () -
 *
 * Arguments:
 *            string: Input string that will be converted to upper case.
 *      lower_string: Output converted string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE     :
 *         <string> is not a character string.
 *
 * Note:
 *
 *   This function returns a string with all lowercase ASCII
 *   and LATIN alphabetic characters converted to uppercase.
 *
 *   If the NULL flag is set for <string>, then the NULL flag
 *   for the <upper_string> is set.
 *
 *   The <upper_string> value structure will be cloned from <string>.
 *   <upper_string> should be cleared with db_value_clone() if it has
 *   already been initialized or DB_MAKE_NULL if it has not been
 *   previously used by the system.
 *
 * Assert:
 *
 *   1. string       != (DB_VALUE *) NULL
 *   2. upper_string != (DB_VALUE *) NULL
 *
 */

int
db_string_upper (const DB_VALUE * string, DB_VALUE * upper_string)
{
  int error_status = NO_ERROR;
  DB_TYPE str_type;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string != (DB_VALUE *) NULL);
  assert (upper_string != (DB_VALUE *) NULL);

  /*
   *  Categorize the two input parameters and check for errors.
   *    Verify that the parameters are both character strings.
   */

  str_type = DB_VALUE_DOMAIN_TYPE (string);
  if (DB_IS_NULL (string))
    {
      DB_MAKE_NULL (upper_string);
    }
  else if (!QSTR_IS_ANY_CHAR (str_type))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
    }
  /*
   *  If the input parameters have been properly validated, then
   *  we are ready to operate.
   */
  else
    {
      db_value_clone ((DB_VALUE *) string, upper_string);

      intl_upper_string ((unsigned char *) DB_PULL_STRING (upper_string),
			 DB_GET_STRING_LENGTH (upper_string),
			 (INTL_CODESET) DB_GET_STRING_CODESET (upper_string));
    }

  return error_status;
}

/*
 * db_string_trim () -
 *
 * Arguments:
 *        trim_operand: Specifies whether the character to be trimmed is
 *                      removed from the beginning, ending or both ends
 *                      of the string.
 *        trim_charset: (Optional) The characters to be removed.
 *          src_string: String to remove trim character from.
 *      trimmed_string: Resultant trimmed string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE     : <trim_char> or <src_string> are
 *                                     not character strings.
 *      ER_QSTR_INVALID_TRIM_OPERAND  : <trim_char> has char length > 1.
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS: <trim_char>, <src_string> and
 *                                     <trimmed_string> have different
 *                                     character code sets.
 *
 */

int
db_string_trim (const MISC_OPERAND tr_operand,
		const DB_VALUE * trim_charset,
		const DB_VALUE * src_string, DB_VALUE * trimmed_string)
{
  int error_status = NO_ERROR;
  int trim_charset_is_null = false;

  unsigned char *result;
  int result_length, result_size = 0, result_domain_length;
  DB_TYPE result_type = DB_TYPE_NULL;

  unsigned char *trim_charset_ptr = NULL;
  int trim_charset_length = 0;
  int trim_charset_size = 0;
  DB_TYPE src_type, trim_type;

  /*
   * Assert DB_VALUE structures have been allocated
   */

  assert (src_string != (DB_VALUE *) NULL);
  assert (trimmed_string != (DB_VALUE *) NULL);
  assert (trim_charset != (DB_VALUE *) NULL);

  /* if source is NULL, return NULL */
  if (DB_IS_NULL (src_string))
    {
      if (QSTR_IS_CHAR (DB_VALUE_DOMAIN_TYPE (src_string)))
	{
	  db_value_domain_init (trimmed_string, DB_TYPE_VARCHAR, 0, 0);
	}
      else
	{
	  db_value_domain_init (trimmed_string, DB_TYPE_VARNCHAR, 0, 0);
	}
      return error_status;
    }

  if (trim_charset == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_PARAMETER,
	      0);
      return ER_QPROC_INVALID_PARAMETER;
    }

  trim_type = DB_VALUE_DOMAIN_TYPE (trim_charset);
  if (trim_type == DB_TYPE_NULL)
    {
      trim_charset_is_null = true;
    }
  else if (DB_IS_NULL (trim_charset))
    {
      if (QSTR_IS_CHAR (DB_VALUE_DOMAIN_TYPE (src_string)))
	{
	  db_value_domain_init (trimmed_string, DB_TYPE_VARCHAR, 0, 0);
	}
      else
	{
	  db_value_domain_init (trimmed_string, DB_TYPE_VARNCHAR, 0, 0);
	}
      return error_status;
    }

  /*
   * Verify input parameters are all char strings and are compatible
   */

  src_type = DB_VALUE_DOMAIN_TYPE (src_string);
  if (!QSTR_IS_ANY_CHAR (src_type)
      || (!trim_charset_is_null && !QSTR_IS_ANY_CHAR (trim_type)))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QSTR_INVALID_DATA_TYPE, 0);
      return error_status;
    }

  if (!trim_charset_is_null
      && (qstr_get_category (src_string) != qstr_get_category (trim_charset)))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QSTR_INCOMPATIBLE_CODE_SETS, 0);
      return error_status;
    }

  /*
   * begin of main codes
   */
  if (!trim_charset_is_null)
    {
      trim_charset_ptr = (unsigned char *) DB_PULL_STRING (trim_charset);
      trim_charset_length = DB_GET_STRING_LENGTH (trim_charset);
      trim_charset_size = DB_GET_STRING_SIZE (trim_charset);
    }

  error_status = qstr_trim (tr_operand,
			    trim_charset_ptr,
			    trim_charset_length,
			    trim_charset_size,
			    (unsigned char *) DB_PULL_STRING (src_string),
			    DB_VALUE_DOMAIN_TYPE (src_string),
			    DB_GET_STRING_LENGTH (src_string),
			    DB_GET_STRING_SIZE (src_string),
			    (INTL_CODESET) DB_GET_STRING_CODESET (src_string),
			    &result,
			    &result_type, &result_length, &result_size);

  if (error_status == NO_ERROR && result != NULL)
    {
      result_domain_length = MIN (QSTR_MAX_PRECISION (result_type),
				  DB_VALUE_PRECISION (src_string));
      qstr_make_typed_string (result_type,
			      trimmed_string,
			      result_domain_length,
			      (char *) result, result_size);
      result[result_size] = 0;
      trimmed_string->need_clear = true;
    }

  return error_status;
}

/* qstr_trim () -
*/
static int
qstr_trim (MISC_OPERAND trim_operand,
	   const unsigned char *trim_charset,
	   int trim_charset_length,
	   int trim_charset_size,
	   const unsigned char *src_ptr,
	   DB_TYPE src_type,
	   int src_length,
	   int src_size,
	   INTL_CODESET codeset,
	   unsigned char **result,
	   DB_TYPE * result_type, int *result_length, int *result_size)
{
  unsigned char pad_char[2], *lead_trimmed_ptr, *trail_trimmed_ptr;
  int lead_trimmed_length, trail_trimmed_length;
  int lead_trimmed_size, trail_trimmed_size, pad_char_size = 0;
  int error_status = NO_ERROR;

  /* default case */
  qstr_pad_char (codeset, pad_char, &pad_char_size);
  if (trim_charset_length == 0)
    {
      trim_charset = pad_char;
      trim_charset_length = 1;
      trim_charset_size = pad_char_size;
    }

  /* trim from front */
  lead_trimmed_ptr = (unsigned char *) src_ptr;
  lead_trimmed_length = src_length;
  lead_trimmed_size = src_size;

  if (trim_operand == LEADING || trim_operand == BOTH)
    {
      trim_leading (trim_charset, trim_charset_size,
		    src_ptr, src_type, src_length, src_size,
		    codeset,
		    &lead_trimmed_ptr,
		    &lead_trimmed_length, &lead_trimmed_size);
    }

  trail_trimmed_ptr = lead_trimmed_ptr;
  trail_trimmed_length = lead_trimmed_length;
  trail_trimmed_size = lead_trimmed_size;

  if (trim_operand == TRAILING || trim_operand == BOTH)
    {
      trim_trailing (trim_charset, trim_charset_size,
		     lead_trimmed_ptr,
		     src_type,
		     lead_trimmed_length,
		     lead_trimmed_size,
		     codeset, &trail_trimmed_length, &trail_trimmed_size);
    }

  /* setup result */
  *result = (unsigned char *)
    db_private_alloc (NULL, (size_t) trail_trimmed_size + 1);
  if (*result == NULL)
    {
      error_status = er_errid ();
      return error_status;
    }

  (void) memcpy ((char *) (*result), (char *) trail_trimmed_ptr,
		 trail_trimmed_size);

  if (QSTR_IS_NATIONAL_CHAR (src_type))
    {
      *result_type = DB_TYPE_VARNCHAR;
    }
  else
    {
      *result_type = DB_TYPE_VARCHAR;
    }
  *result_length = trail_trimmed_length;
  *result_size = trail_trimmed_size;

  return error_status;
}

/*
 * trim_leading () -
 *
 * Arguments:
 *       trim_charset_ptr: (in)  Single character trim string.
 *      trim_charset_size: (in)  Size of trim string.
 *         src_string_ptr: (in)  Source string to be trimmed.
 *      src_string_length: (in)  Length of source string.
 *                codeset: (in)  International codeset of source string.
 *       lead_trimmed_ptr: (out) Pointer to start of trimmed string.
 *    lead_trimmed_length: (out) Length of trimmed string.
 *
 * Returns: nothing
 *
 * Errors:
 *
 * Note:
 *     Remove trim character from the front of the source string.  No
 *     characters are actually removed.  Instead, the function returns
 *     a pointer to the beginning of the source string after the trim
 *     characters and the resultant length of the string.
 *
 */
static void
trim_leading (const unsigned char *trim_charset_ptr,
	      int trim_charset_size,
	      const unsigned char *src_ptr,
	      DB_TYPE src_type,
	      int src_length,
	      int src_size,
	      INTL_CODESET codeset,
	      unsigned char **lead_trimmed_ptr,
	      int *lead_trimmed_length, int *lead_trimmed_size)
{
  int cur_src_char_size, cur_trim_char_size;
  unsigned char *cur_src_char_ptr, *cur_trim_char_ptr;

  int cmp_flag = 0;
  int matched = 0;

  *lead_trimmed_ptr = (unsigned char *) src_ptr;
  *lead_trimmed_length = src_length;
  *lead_trimmed_size = src_size;

  /* iterate for source string */
  for (cur_src_char_ptr = (unsigned char *) src_ptr;
       cur_src_char_ptr < src_ptr + src_size;)
    {
      intl_char_size (cur_src_char_ptr, 1, codeset, &cur_src_char_size);
      if (!PRM_SINGLE_BYTE_COMPARE
	  && !QSTR_IS_NATIONAL_CHAR (src_type)
	  && IS_KOREAN (*cur_src_char_ptr))
	{
	  cur_src_char_size++;
	}

      matched = 0;

      /* iterate for trim charset */
      for (cur_trim_char_ptr = (unsigned char *) trim_charset_ptr;
	   cur_trim_char_ptr < trim_charset_ptr + trim_charset_size;)
	{
	  intl_char_size (cur_trim_char_ptr, 1, codeset, &cur_trim_char_size);

	  if (!PRM_SINGLE_BYTE_COMPARE
	      && !QSTR_IS_NATIONAL_CHAR (src_type)
	      && IS_KOREAN (*cur_trim_char_ptr))
	    {
	      cur_trim_char_size++;
	    }

	  if (cur_src_char_size != cur_trim_char_size)
	    {
	      cur_trim_char_ptr += cur_trim_char_size;
	      continue;
	    }

	  cmp_flag = memcmp ((char *) cur_src_char_ptr,
			     (char *) cur_trim_char_ptr, cur_trim_char_size);

	  if (cmp_flag == 0)
	    {
	      *lead_trimmed_length -= 1;
	      if (!PRM_SINGLE_BYTE_COMPARE
		  && !QSTR_IS_NATIONAL_CHAR (src_type)
		  && IS_KOREAN (*cur_src_char_ptr))
		{
		  *lead_trimmed_length -= 1;
		}
	      *lead_trimmed_size -= cur_trim_char_size;
	      *lead_trimmed_ptr += cur_src_char_size;
	      matched = 1;
	      break;
	    }

	  cur_trim_char_ptr += cur_trim_char_size;
	}

      if (!matched)
	{
	  break;
	}
      cur_src_char_ptr += cur_src_char_size;
    }
}

/*
 * trim_trailing () -
 *
 * Arguments:
 *       trim_charset_ptr: (in)  Single character trim string.
 *      trim_charset_size: (in)  Size of trim string.
 *                src_ptr: (in)  Source string to be trimmed.
 *             src_length: (in)  Length of source string.
 *                codeset: (in)  International codeset of source string.
 *   trail_trimmed_length: (out) Length of trimmed string.
 *
 * Returns: nothing
 *
 * Errors:
 *
 * Note:
 *     Remove trim character from the end of the source string.  No
 *     characters are actually removed.  Instead, the function returns
 *     a pointer to the beginning of the source string after the trim
 *     characters and the resultant length of the string.
 *
 */
static void
trim_trailing (const unsigned char *trim_charset_ptr,
	       int trim_charset_size,
	       const unsigned char *src_ptr,
	       DB_TYPE src_type,
	       int src_length,
	       int src_size,
	       INTL_CODESET codeset,
	       int *trail_trimmed_length, int *trail_trimmed_size)
{
  int cur_trim_char_size;
  int prev_src_char_size;
  unsigned char *cur_src_char_ptr, *cur_trim_char_ptr;
  unsigned char *prev_src_char_ptr;

  int cmp_flag = 0;
  int matched = 0;

  *trail_trimmed_length = src_length;
  *trail_trimmed_size = src_size;

  /* iterate for source string */
  for (cur_src_char_ptr = (unsigned char *) src_ptr + src_size;
       cur_src_char_ptr > src_ptr;)
    {
      /* get previous letter */
      prev_src_char_ptr =
	qstr_prev_char (cur_src_char_ptr, codeset, &prev_src_char_size);
      if (!PRM_SINGLE_BYTE_COMPARE
	  && !QSTR_IS_NATIONAL_CHAR (src_type)
	  && IS_KOREAN (*prev_src_char_ptr))
	{
	  prev_src_char_ptr -= 1;
	  prev_src_char_size += 1;
	}

      /* iterate for trim charset */
      matched = 0;
      for (cur_trim_char_ptr = (unsigned char *) trim_charset_ptr;
	   cur_trim_char_ptr < trim_charset_ptr + trim_charset_size;)
	{
	  intl_char_size (cur_trim_char_ptr, 1, codeset, &cur_trim_char_size);
	  if (!PRM_SINGLE_BYTE_COMPARE
	      && !QSTR_IS_NATIONAL_CHAR (src_type)
	      && IS_KOREAN (*cur_trim_char_ptr))
	    {
	      cur_trim_char_size++;
	    }

	  if (cur_trim_char_size != prev_src_char_size)
	    {
	      cur_trim_char_ptr += cur_trim_char_size;
	      continue;
	    }

	  cmp_flag = memcmp ((char *) prev_src_char_ptr,
			     (char *) cur_trim_char_ptr, cur_trim_char_size);
	  if (cmp_flag == 0)
	    {
	      *trail_trimmed_length -= 1;
	      if (!PRM_SINGLE_BYTE_COMPARE
		  && !QSTR_IS_NATIONAL_CHAR (src_type)
		  && IS_KOREAN (*cur_trim_char_ptr))
		{
		  *trail_trimmed_length -= 1;
		}
	      *trail_trimmed_size -= cur_trim_char_size;
	      matched = 1;
	      break;
	    }

	  cur_trim_char_ptr += cur_trim_char_size;
	}

      if (!matched)
	{
	  break;
	}

      cur_src_char_ptr -= prev_src_char_size;
    }
}

/*
 * db_string_pad () -
 *
 * Arguments:
 *      pad_operand: (in)  Left or Right padding?
 *       src_string: (in)  Source string to be padded.
 *       pad_length: (in)  Length of padded string
 *      pad_charset: (in)  Padding char set
 *    padded_string: (out) Padded string
 *
 * Returns: nothing
 */
int
db_string_pad (const MISC_OPERAND pad_operand, const DB_VALUE * src_string,
	       const DB_VALUE * pad_length, const DB_VALUE * pad_charset,
	       DB_VALUE * padded_string)
{
  int error_status = NO_ERROR;
  int total_length;

  unsigned char *result;
  int result_length = 0, result_size = 0;
  DB_TYPE result_type;

  unsigned char *pad_charset_ptr = NULL;
  int pad_charset_length = 0;
  int pad_charset_size = 0;
  DB_TYPE src_type;

  assert (src_string != (DB_VALUE *) NULL);
  assert (padded_string != (DB_VALUE *) NULL);

  /* if source is NULL, return NULL */
  if (DB_IS_NULL (src_string) || DB_IS_NULL (pad_charset))
    {
      if (QSTR_IS_CHAR (DB_VALUE_DOMAIN_TYPE (src_string)))
	{
	  db_value_domain_init (padded_string, DB_TYPE_VARCHAR, 0, 0);
	}
      else
	{
	  db_value_domain_init (padded_string, DB_TYPE_VARNCHAR, 0, 0);
	}
      return error_status;
    }

  if (DB_IS_NULL (pad_length) ||
      (total_length = DB_GET_INTEGER (pad_length)) <= 0)
    {
      /*error_status = ER_QPROC_INVALID_PARAMETER; */
      if (QSTR_IS_CHAR (DB_VALUE_DOMAIN_TYPE (src_string)))
	{
	  db_value_domain_init (padded_string, DB_TYPE_VARCHAR, 0, 0);
	}
      else
	{
	  db_value_domain_init (padded_string, DB_TYPE_VARNCHAR, 0, 0);
	}
      return error_status;
    }

  src_type = DB_VALUE_DOMAIN_TYPE (src_string);
  if (!QSTR_IS_ANY_CHAR (src_type) || !is_char_string (pad_charset))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QSTR_INVALID_DATA_TYPE, 0);
      return error_status;
    }

  if (qstr_get_category (src_string) != qstr_get_category (pad_charset))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QSTR_INCOMPATIBLE_CODE_SETS, 0);
      return error_status;
    }

  error_status = qstr_pad (pad_operand, total_length,
			   (unsigned char *) DB_PULL_STRING (pad_charset),
			   DB_GET_STRING_LENGTH (pad_charset),
			   DB_GET_STRING_SIZE (pad_charset),
			   (unsigned char *) DB_PULL_STRING (src_string),
			   DB_VALUE_DOMAIN_TYPE (src_string),
			   DB_GET_STRING_LENGTH (src_string),
			   DB_GET_STRING_SIZE (src_string),
			   (INTL_CODESET) DB_GET_STRING_CODESET (src_string),
			   &result,
			   &result_type, &result_length, &result_size);

  if (error_status == NO_ERROR && result != NULL)
    {
      qstr_make_typed_string (result_type,
			      padded_string,
			      result_length, (char *) result, result_size);

      result[result_size] = 0;
      padded_string->need_clear = true;
    }

  return error_status;
}

/*
 * qstr_pad () -
 */
static int
qstr_pad (MISC_OPERAND pad_operand,
	  int pad_length,
	  const unsigned char *pad_charset_ptr,
	  int pad_charset_length,
	  int pad_charset_size,
	  const unsigned char *src_ptr,
	  DB_TYPE src_type,
	  int src_length,
	  int src_size,
	  INTL_CODESET codeset,
	  unsigned char **result,
	  DB_TYPE * result_type, int *result_length, int *result_size)
{
  unsigned char def_pad_char[2];
  unsigned char *cur_src_char_ptr;
  unsigned char *cur_pad_char_ptr;
  int cur_char_size;
  int def_pad_char_size = 0;	/* default padding char */
  int truncate_size, pad_size, alloc_size, cnt;
  int length_to_be_padded;	/* length that will be really padded */
  int remain_length_to_be_padded;	/* remained length that will be padded */
  int char_broken = 0;
  int error_status = NO_ERROR;

  qstr_pad_char (codeset, def_pad_char, &def_pad_char_size);
  if (pad_charset_length == 0)
    {
      pad_charset_ptr = def_pad_char;
      pad_charset_length = 1;
      pad_charset_size = def_pad_char_size;
    }

  if (QSTR_IS_NATIONAL_CHAR (src_type))
    {
      alloc_size = 2 * pad_length;
      *result_type = DB_TYPE_VARNCHAR;
    }
  else
    {
      alloc_size = pad_length;
      *result_type = DB_TYPE_VARCHAR;
    }

  *result =
    (unsigned char *) db_private_alloc (NULL, (size_t) alloc_size + 1);
  if (*result == NULL)
    {
      error_status = er_errid ();
      return error_status;
    }

  /*
   * now start padding
   */

  /* if source length is greater than pad_length */
  if (src_length >= pad_length)
    {
      truncate_size = 0;	/* SIZE to be cut */
      pad_size = 0;		/* padded SIZE */
      cnt = 0;			/* pading LENGTH count */
      for (cur_src_char_ptr = (unsigned char *) src_ptr; cnt < pad_length;)
	{
	  intl_char_size (cur_src_char_ptr, 1, codeset, &cur_char_size);
	  cnt++;
	  if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*cur_src_char_ptr))
	    {
	      cnt++;
	      cur_char_size++;
	    }

	  if (truncate_size + cur_char_size > pad_length)
	    {
	      cnt--;
	      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*cur_src_char_ptr))
		{
		  cnt--;
		}
	      break;
	    }
	  else
	    {
	      truncate_size += cur_char_size;
	    }
	  pad_size += cur_char_size;
	  cur_src_char_ptr += cur_char_size;
	}
      /* if 2-byte char was the last and deleted */
      pad_size += (pad_length - cnt);

      if (pad_operand == LEADING)
	{

	  (void) memset ((char *) (*result), ' ', pad_size - truncate_size);
	  (void) memcpy ((char *) (*result) + (pad_size - truncate_size),
			 (char *) src_ptr, truncate_size);
	}
      else
	{
	  (void) memcpy ((char *) (*result), (char *) src_ptr, truncate_size);
	  (void) memset ((char *) (*result) + truncate_size,
			 ' ', pad_size - truncate_size);
	}
      *result_length = pad_length;
      *result_size = truncate_size + (pad_length - cnt);
      return error_status;
    }

  /*
   * Get real length to be paded
   * if source length is greater than pad_length
   */

  length_to_be_padded = pad_length - src_length;

  /* pad heading first */

  cnt = 0;			/* how many times copy pad_char_set */
  pad_size = 0;			/* SIZE of padded char */
  remain_length_to_be_padded = 0;

  for (; cnt < (length_to_be_padded / pad_charset_length); cnt++)
    {
      (void) memcpy ((char *) (*result) + pad_charset_size * cnt,
		     (char *) pad_charset_ptr, pad_charset_size);
    }
  pad_size = pad_charset_size * cnt;
  remain_length_to_be_padded = (pad_length - src_length) % pad_charset_length;

  if (remain_length_to_be_padded != 0)
    {
      cur_pad_char_ptr = (unsigned char *) pad_charset_ptr;

      while (remain_length_to_be_padded > 0)
	{
	  intl_char_size (cur_pad_char_ptr, 1, codeset, &cur_char_size);
	  if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*cur_pad_char_ptr))
	    {
	      remain_length_to_be_padded--;
	      cur_char_size++;
	    }
	  remain_length_to_be_padded--;

	  (void) memcpy ((char *) (*result) + pad_size,
			 (char *) cur_pad_char_ptr, cur_char_size);
	  cur_pad_char_ptr += cur_char_size;
	  pad_size += cur_char_size;
	}

      /* if 2-byte char is broken */
      if (remain_length_to_be_padded < 0)
	{
	  char_broken = 1;
	  pad_size -= 1;
	  (void) memset ((char *) (*result) + pad_size - 1, ' ', 1);
	}
    }

  memcpy ((char *) (*result) + pad_size, src_ptr, src_size);

  if (pad_operand == LEADING)
    {
      if (char_broken)
	{
	  memmove ((char *) (*result) + 1, (char *) (*result), pad_size - 1);
	  (void) memset ((char *) (*result), ' ', 1);
	}
    }
  else if (pad_operand == TRAILING)
    {				/* switch source and padded string */
      memmove ((char *) (*result) + src_size, (char *) (*result), pad_size);
      memcpy ((char *) (*result), src_ptr, src_size);
    }

  pad_size += src_size;

  *result_length = pad_length;
  *result_size = pad_size;

  return error_status;
}

/*
 * db_string_like () -
 *
 * Arguments:
 *             src_string:  (IN) Source string.
 *                pattern:  (IN) Pattern string which can contain % and _
 *                               characters.
 *               esc_char:  (IN) Optional escape character.
 *                 result: (OUT) Integer result.
 *
 * Returns: int
 *
 * Errors:
 *      ER_QSTR_INVALID_DATA_TYPE   :
 *          <src_string>, <pattern>, or <esc_char> (if it's not NULL)
 *          is not a character string.
 *
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *          <src_string>, <pattern>, and <esc_char> (if it's not NULL)
 *          have different character code sets.
 *
 *      ER_QSTR_INVALID_ESCAPE_SEQUENCE:
 *          An illegal pattern is specified.
 *
 *      ER_QSTR_INVALID_ESCAPE_CHARACTER:
 *          If <esc_char> is not NULL and the length of E is > 1.
 *
 */

int
db_string_like (const DB_VALUE * src_string,
		const DB_VALUE * pattern,
		const DB_VALUE * esc_char, int *result)
{
  QSTR_CATEGORY src_category, pattern_category;
  int error_status = NO_ERROR;
  DB_TYPE src_type, pattern_type;
  unsigned char *src_char_string_p;
  unsigned char *pattern_char_string_p;
  unsigned char *esc_char_p;
  unsigned char *src_char_buffer = NULL, *pattern_char_buffer = NULL;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_string != (DB_VALUE *) NULL);
  assert (pattern != (DB_VALUE *) NULL);

  src_category = qstr_get_category (src_string);
  pattern_category = qstr_get_category (pattern);

  src_type = DB_VALUE_DOMAIN_TYPE (src_string);
  pattern_type = DB_VALUE_DOMAIN_TYPE (pattern);

  if (!QSTR_IS_ANY_CHAR (src_type) || !QSTR_IS_ANY_CHAR (pattern_type))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QSTR_INVALID_DATA_TYPE, 0);
      *result = V_ERROR;
      return error_status;
    }

  if (src_category != pattern_category)
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QSTR_INCOMPATIBLE_CODE_SETS, 0);
      *result = V_ERROR;
      return error_status;
    }

  if (DB_IS_NULL (src_string))
    {
      *result = V_UNKNOWN;
      return error_status;
    }

  if (DB_IS_NULL (pattern))
    {
      *result = V_FALSE;
      return error_status;
    }

  if (src_type == DB_TYPE_CHAR || src_type == DB_TYPE_NCHAR)
    {
      int src_len = 0;

      src_char_string_p = (unsigned char *) DB_PULL_CHAR (src_string,
							  &src_len);
      src_char_buffer = malloc (src_len + 1);
      if (src_char_buffer == NULL)
	{
	  *result = V_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, src_len + 1);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      memcpy (src_char_buffer, src_char_string_p, src_len);
      *(src_char_buffer + src_len) = '\0';

      src_char_string_p = src_char_buffer;
    }
  else
    {
      /* varchar, varnchar has a terminating NULL */
      src_char_string_p = (unsigned char *) DB_PULL_STRING (src_string);
    }

  if (pattern_type == DB_TYPE_CHAR || pattern_type == DB_TYPE_NCHAR)
    {
      int pattern_len = 0;

      pattern_char_string_p = (unsigned char *) DB_PULL_CHAR (pattern,
							      &pattern_len);
      pattern_char_buffer = malloc (pattern_len + 1);
      if (pattern_char_buffer == NULL)
	{
	  *result = V_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, pattern_len + 1);
	  if (src_char_buffer)
	    free_and_init (src_char_buffer);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      memcpy (pattern_char_buffer, pattern_char_string_p, pattern_len);
      *(pattern_char_buffer + pattern_len) = '\0';

      pattern_char_string_p = pattern_char_buffer;
    }
  else
    {
      /* varchar, varnchar has a terminating NULL */
      pattern_char_string_p = (unsigned char *) DB_PULL_STRING (pattern);
    }

  if (esc_char)
    {
      esc_char_p = (unsigned char *) DB_PULL_STRING (esc_char);
      assert (esc_char_p != NULL);
    }

  *result = qstr_eval_like (src_char_string_p, pattern_char_string_p,
			    (esc_char ? *esc_char_p : 1));

  if (*result == V_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QSTR_INVALID_ESCAPE_SEQUENCE, 0);
    }

  if (src_char_buffer)
    {
      free_and_init (src_char_buffer);
    }
  if (pattern_char_buffer)
    {
      free_and_init (pattern_char_buffer);
    }

  return ((*result == V_ERROR) ? ER_QSTR_INVALID_ESCAPE_SEQUENCE
	  : error_status);
}

/*
 * qstr_strstr () -
 */
static unsigned char *
qstr_strstr (const unsigned char *src, const unsigned char *pattern)
{
  unsigned char *matched;
  unsigned char *p;
  bool do_more;

  p = (unsigned char *) src;
  do
    {
      do_more = false;
      matched = (unsigned char *) strstr ((char *) p, (const char *) pattern);
      if (matched)
	{
	  while (*p && p < matched)
	    {
	      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*p))
		{
		  p++;
		}
	      p++;
	    }
	  if (p == matched)
	    {
	      return matched;
	    }
	  else
	    {
	      /* false drop */
	      do_more = true;
	    }
	}
    }
  while (do_more);

  return matched;
}

/*
 * qstr_eval_like () -
 */
static int
qstr_eval_like (const unsigned char *tar,
		const unsigned char *expr, unsigned char escape)
{
  const int IN_CHECK = 0;
  const int IN_PERCENT = 1;
  const int IN_PERCENT_UNDERLINE = 2;

  int status = IN_CHECK;
  const unsigned char *tarstack[STACK_SIZE], *exprstack[STACK_SIZE];
  int stackp = -1;
  int inescape = 0;

  unsigned char *tar_ptr;
  unsigned char *expr_ptr;
  unsigned char *temp_ptr;
  int substrlen = 0;
  unsigned char escapestr[2];

  if (*expr == '%')
    {
      expr_ptr = (unsigned char *) strchr ((const char *) (expr + 1), '%');
      if (expr_ptr == NULL)
	{
	  if (*(expr + 1) == 0)
	    {
	      return V_TRUE;	/* case : '%' */
	    }
	  /* case : %xxx */
	  temp_ptr = (unsigned char *) strchr ((const char *) expr, '_');
	  if (temp_ptr == NULL)
	    {
	      if (escape == 1)
		{
		  temp_ptr = NULL;
		}
	      else
		{
		  escapestr[0] = escape;
		  escapestr[1] = 0;
		  temp_ptr = (unsigned char *) qstr_strstr (expr, escapestr);
		}
	      if (!temp_ptr)
		{
		  expr_ptr = (unsigned char *) expr + 1;
		  substrlen = strlen ((const char *) expr_ptr);
		  tar_ptr = (unsigned char *) qstr_strstr (tar, expr_ptr);
		  if (tar_ptr)
		    {
		      tar_ptr = tar_ptr + substrlen;
		      while (*tar_ptr == ' ')
			{
			  tar_ptr++;
			}
		      if (*tar_ptr == 0)
			{
			  return V_TRUE;
			}
		    }
		  else
		    {
		      return V_FALSE;
		    }
		}
	    }
	}
      else
	{
	  if (strlen ((const char *) expr_ptr) == 1)
	    {
	      /* case : %xxx% */
	      expr_ptr = (unsigned char *) expr + 1;
	      temp_ptr = (unsigned char *) strchr ((const char *) expr, '_');
	      if (!temp_ptr)
		{
		  if (escape == 1)
		    {
		      temp_ptr = NULL;
		    }
		  else
		    {
		      escapestr[0] = escape;
		      escapestr[1] = 0;
		      temp_ptr =
			(unsigned char *) qstr_strstr (expr, escapestr);
		    }
		  if (!temp_ptr)
		    {
		      substrlen = strlen ((const char *) expr_ptr) - 1;
		      *(expr_ptr + substrlen) = 0;
		      tar_ptr = (unsigned char *) qstr_strstr (tar, expr_ptr);
		      *(expr_ptr + substrlen) = '%';
		      if (tar_ptr)
			{
			  return V_TRUE;
			}
		      else
			{
			  return V_FALSE;
			}
		    }
		}
	    }
	}
    }

  if (escape == 0)
    {
      escape = 2;
    }
  while (1)
    {
      if (status == IN_CHECK)
	{
	  if (*expr == escape)
	    {
	      expr++;
	      inescape = 1;
	      if (*expr != '%' && *expr != '_')
		{
		  return V_ERROR;
		}
	      continue;
	    }

	  if (inescape)
	    {
	      if (*tar == *expr)
		{
		  tar++;
		  expr++;
		}
	      else
		{
		  if (stackp >= 0 && stackp < STACK_SIZE)
		    {
		      tar = tarstack[stackp];
		      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*tar))
			{
			  tar += 2;
			}
		      else
			{
			  tar++;
			}
		      expr = exprstack[stackp--];
		    }
		  else
		    {
		      return V_FALSE;
		    }
		}
	      inescape = 0;
	      continue;
	    }

	  /* goto check */
	  if (*expr == 0)
	    {
	      while (*tar == ' ')
		{
		  tar++;
		}

	      if (*tar == 0)
		{
		  return V_TRUE;
		}
	      else
		{
		  if (stackp >= 0 && stackp < STACK_SIZE)
		    {
		      tar = tarstack[stackp];
		      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*tar))
			{
			  tar += 2;
			}
		      else
			{
			  tar++;
			}
		      expr = exprstack[stackp--];
		    }
		  else
		    {
		      return V_FALSE;
		    }
		}
	    }
	  else if (*expr == '%')
	    {
	      status = IN_PERCENT;
	      while (*(expr + 1) == '%')
		{
		  expr++;
		}
	    }
	  else if (*tar && ((*expr == '_')
			    || ((PRM_SINGLE_BYTE_COMPARE
				 || !IS_KOREAN (*tar))
				&& *tar == *expr)
			    || (!PRM_SINGLE_BYTE_COMPARE
				&& IS_KOREAN (*tar)
				&& *tar == *expr
				&& *(tar + 1) == *(expr + 1))))
	    {
	      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*tar))
		{
		  tar += 2;
		}
	      else
		{
		  tar++;
		}
	      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*expr))
		{
		  expr += 2;
		}
	      else
		{
		  expr++;
		}
	    }
	  else if (stackp >= 0 && stackp < STACK_SIZE)
	    {
	      tar = tarstack[stackp];
	      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*tar))
		{
		  tar += 2;
		}
	      else
		{
		  tar++;
		}

	      expr = exprstack[stackp--];
	    }
	  else if (stackp > STACK_SIZE)
	    {
	      return V_ERROR;
	    }
	  else
	    {
	      return V_FALSE;
	    }
	}
      else if (status == IN_PERCENT)
	{
	  if (*(expr + 1) == '_')
	    {
	      if (stackp >= STACK_SIZE - 1)
		{
		  return V_ERROR;
		}
	      tarstack[++stackp] = tar;
	      exprstack[stackp] = expr;
	      expr++;

	      if (stackp > STACK_SIZE)
		{
		  return V_ERROR;
		}
	      inescape = 0;
	      status = IN_PERCENT_UNDERLINE;
	      continue;
	    }

	  if (*(expr + 1) == escape)
	    {
	      expr++;
	      inescape = 1;
	      if (*(expr + 1) != '%' && *(expr + 1) != '_')
		{
		  return V_ERROR;
		}
	    }

	  if (*(expr + 1) == 0)
	    {
	      return V_TRUE;
	    }

	  while (*tar && *tar != *(expr + 1))
	    {
	      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*tar))
		{
		  tar += 2;
		}
	      else
		{
		  tar++;
		}
	    }

	  if (*tar == *(expr + 1))
	    {
	      if (stackp >= STACK_SIZE - 1)
		{
		  return V_ERROR;
		}
	      tarstack[++stackp] = tar;
	      if (inescape)
		{
		  expr--;
		}
	      exprstack[stackp] = expr;
	      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*expr))
		{
		  expr += 2;
		}
	      else
		{
		  expr++;
		}

	      if (stackp > STACK_SIZE)
		{
		  return V_ERROR;
		}
	      inescape = 0;
	      status = IN_CHECK;
	    }
	}
      if (status == IN_PERCENT_UNDERLINE)
	{
	  if (*expr == escape)
	    {
	      expr++;
	      inescape = 1;
	      if (*expr != '%' && *expr != '_')
		{
		  return V_ERROR;
		}
	      continue;
	    }

	  if (inescape)
	    {
	      if (*tar == *expr)
		{
		  tar++;
		  expr++;
		}
	      else
		{
		  if (stackp >= 0 && stackp < STACK_SIZE)
		    {
		      tar = tarstack[stackp];
		      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*tar))
			{
			  tar += 2;
			}
		      else
			{
			  tar++;
			}
		      expr = exprstack[stackp--];
		    }
		  else
		    {
		      return V_FALSE;
		    }
		}
	      inescape = 0;
	      continue;
	    }

	  /* goto check */
	  if (*expr == 0)
	    {
	      while (*tar == ' ')
		{
		  tar++;
		}

	      if (*tar == 0)
		{
		  return V_TRUE;
		}
	      else
		{
		  if (stackp >= 0 && stackp < STACK_SIZE)
		    {
		      tar = tarstack[stackp];
		      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*tar))
			{
			  tar += 2;
			}
		      else
			{
			  tar++;
			}
		      expr = exprstack[stackp--];
		    }
		  else
		    {
		      return V_FALSE;
		    }
		}
	    }
	  else if (*expr == '%')
	    {
	      status = IN_PERCENT;
	      while (*(expr + 1) == '%')
		{
		  expr++;
		}
	    }
	  else if ((*expr == '_')
		   || ((PRM_SINGLE_BYTE_COMPARE
			|| !IS_KOREAN (*tar))
		       && *tar == *expr)
		   || (!PRM_SINGLE_BYTE_COMPARE
		       && IS_KOREAN (*tar)
		       && *tar == *expr && *(tar + 1) == *(expr + 1)))
	    {
	      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*tar))
		{
		  tar += 2;
		}
	      else
		{
		  tar++;
		}
	      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*expr))
		{
		  expr += 2;
		}
	      else
		{
		  expr++;
		}
	    }
	  else if (stackp >= 0 && stackp < STACK_SIZE)
	    {
	      tar = tarstack[stackp];
	      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*tar))
		{
		  tar += 2;
		}
	      else
		{
		  tar++;
		}

	      expr = exprstack[stackp--];
	    }
	  else if (stackp > STACK_SIZE)
	    {
	      return V_ERROR;
	    }
	  else
	    {
	      return V_FALSE;
	    }
	}

      if (*tar == 0)
	{
	  if (*expr)
	    {
	      while (*expr == '%')
		{
		  expr++;
		}
	    }

	  if (*expr == 0)
	    {
	      return V_TRUE;
	    }
	  else
	    {
	      return V_FALSE;
	    }
	}
    }
}

/*
 * db_string_replace () -
 */
int
db_string_replace (const DB_VALUE * src_string, const DB_VALUE * srch_string,
		   const DB_VALUE * repl_string, DB_VALUE * replaced_string)
{
  int error_status = NO_ERROR;
  unsigned char *result_ptr;
  int result_length = 0, result_size = 0;
  DB_TYPE result_type = DB_TYPE_NULL;

  assert (src_string != (DB_VALUE *) NULL);
  assert (replaced_string != (DB_VALUE *) NULL);

  if (DB_IS_NULL (src_string) || DB_IS_NULL (srch_string)
      || DB_IS_NULL (repl_string))
    {
      if (QSTR_IS_CHAR (DB_VALUE_DOMAIN_TYPE (src_string)))
	{
	  db_value_domain_init (replaced_string, DB_TYPE_VARCHAR, 0, 0);
	}
      else
	{
	  db_value_domain_init (replaced_string, DB_TYPE_VARNCHAR, 0, 0);
	}
      return error_status;
    }

  if (!is_char_string (srch_string) || !is_char_string (repl_string)
      || !is_char_string (src_string))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QSTR_INVALID_DATA_TYPE, 0);
      return error_status;
    }

  if ((qstr_get_category (src_string) != qstr_get_category (srch_string))
      || (qstr_get_category (src_string) != qstr_get_category (repl_string))
      || (qstr_get_category (srch_string) != qstr_get_category (repl_string)))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QSTR_INCOMPATIBLE_CODE_SETS, 0);
      return error_status;
    }

  error_status = qstr_replace ((unsigned char *) DB_PULL_STRING (src_string),
			       DB_VALUE_DOMAIN_TYPE (src_string),
			       DB_GET_STRING_LENGTH (src_string),
			       DB_GET_STRING_SIZE (src_string),
			       (INTL_CODESET)
			       DB_GET_STRING_CODESET (src_string),
			       (unsigned char *) DB_PULL_STRING (srch_string),
			       DB_GET_STRING_SIZE (srch_string),
			       (unsigned char *) DB_PULL_STRING (repl_string),
			       DB_GET_STRING_SIZE (repl_string),
			       &result_ptr, &result_type,
			       &result_length, &result_size);

  if (error_status == NO_ERROR && result_ptr != NULL)
    {
      if (result_length == 0)
	{
	  qstr_make_typed_string (result_type,
				  replaced_string,
				  (DB_GET_STRING_LENGTH (src_string) == 0) ?
				  1 : DB_GET_STRING_LENGTH (src_string),
				  (char *) result_ptr, result_size);
	}
      else
	{
	  qstr_make_typed_string (result_type,
				  replaced_string,
				  result_length,
				  (char *) result_ptr, result_size);
	}
      result_ptr[result_size] = 0;
      replaced_string->need_clear = true;
    }

  return error_status;
}

/*
 * kor_cmp () -
 */
static int
kor_cmp (unsigned char *src, unsigned char *dest, int size)
{
  int r;
  while (size > 0)
    {
      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*src) && IS_KOREAN (*dest))
	{
	  r = memcmp (src, dest, 2);
	  if (r == 0)
	    {
	      src += 2;
	      dest += 2;
	      size -= 2;
	    }
	  else
	    {
	      return r;
	    }
	}
      else if ((PRM_SINGLE_BYTE_COMPARE || !IS_KOREAN (*src))
	       && *src == *dest)
	{
	  src++;
	  dest++;
	  size--;
	}
      else
	{
	  return (*src - *dest);
	}
    }
  return 0;
}

/* qstr_replace () -
 */
static int
qstr_replace (unsigned char *src_ptr,
	      DB_TYPE src_type,
	      int src_len,
	      int src_size,
	      INTL_CODESET codeset,
	      unsigned char *srch_str_ptr,
	      int srch_str_size,
	      unsigned char *repl_str_ptr,
	      int repl_str_size,
	      unsigned char **result_ptr,
	      DB_TYPE * result_type, int *result_len, int *result_size)
{
  int error_status = NO_ERROR;
  int repl_cnt = 0, i, offset;
  unsigned char *head, *tail, *target;

  /*
   * if search string is NULL or is longer than source string
   * copy source string as a result
   */
  if (srch_str_ptr == NULL || src_size < srch_str_size)
    {
      *result_ptr =
	(unsigned char *) db_private_alloc (NULL, (size_t) src_size + 1);
      if (*result_ptr == NULL)
	{
	  error_status = er_errid ();
	  return error_status;
	}

      (void) memcpy ((char *) (*result_ptr), (char *) src_ptr, src_size);
      *result_type = QSTR_IS_NATIONAL_CHAR (src_type) ?
	DB_TYPE_VARNCHAR : DB_TYPE_VARCHAR;
      *result_len = src_len;
      *result_size = src_size;
      return error_status;
    }

  if (repl_str_ptr == NULL)
    {
      repl_str_ptr = (unsigned char *) "";
    }

  for (repl_cnt = 0, i = 0; src_size > 0 && srch_str_size > 0
       && i <= (src_size - srch_str_size);)
    {
      if (kor_cmp (src_ptr + i, srch_str_ptr, srch_str_size) == 0)
	{
	  i += srch_str_size;
	  repl_cnt++;
	  continue;
	}
      else
	{
	  intl_char_size (src_ptr + i, 1, codeset, &offset);

	  if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*(src_ptr + i)))
	    {
	      offset++;
	    }
	  i += offset;
	}
    }

  *result_size = src_size - (srch_str_size * repl_cnt)
    + (repl_str_size * repl_cnt);

  *result_ptr = (unsigned char *)
    db_private_alloc (NULL, (size_t) * result_size + 1);
  if (*result_ptr == NULL)
    {
      error_status = er_errid ();
      return error_status;
    }

  head = tail = src_ptr;
  target = *result_ptr;
  for (; src_size > 0 && srch_str_size > 0
       && head <= src_ptr + (src_size - srch_str_size);)
    {
      if (kor_cmp (head, srch_str_ptr, srch_str_size) == 0)
	{
	  /* first, copy non matched original string */
	  if ((head - tail) > 0)
	    {
	      (void) memcpy ((char *) target, tail, head - tail);
	      target += head - tail;
	    }

	  /* second, copy replacing string */
	  (void) memcpy ((char *) target,
			 (char *) repl_str_ptr, repl_str_size);
	  i += srch_str_size;

	  head += srch_str_size;
	  tail = head;
	  target += repl_str_size;
	  continue;
	}
      else
	{
	  intl_char_size (head, 1, codeset, &offset);

	  if (!PRM_SINGLE_BYTE_COMPARE
	      && !QSTR_IS_NATIONAL_CHAR (src_type) && IS_KOREAN (*head))
	    {
	      offset++;
	    }
	  head += offset;
	}
    }
  if ((src_ptr + src_size - tail) > 0)
    {
      (void) memcpy ((char *) target, tail, src_ptr + src_size - tail);
    }

  *result_type = QSTR_IS_NATIONAL_CHAR (src_type) ?
    DB_TYPE_VARNCHAR : DB_TYPE_VARCHAR;

  *result_len = 0;
  for (i = 0, head = *result_ptr; i < *result_size;)
    {
      intl_char_size (head, 1, codeset, &offset);
      *result_len += 1;
      head += offset;
      i += offset;
    }

  return error_status;
}

/*
 * db_string_translate () -
 */
int
db_string_translate (const DB_VALUE * src_string,
		     const DB_VALUE * from_string, const DB_VALUE * to_string,
		     DB_VALUE * transed_string)
{
  int error_status = NO_ERROR;
  int from_string_is_null = false;
  int to_string_is_null = false;

  unsigned char *result_ptr = NULL;
  int result_length = 0, result_size = 0;
  DB_TYPE result_type = DB_TYPE_NULL;

  assert (src_string != (DB_VALUE *) NULL);
  assert (transed_string != (DB_VALUE *) NULL);

  if (DB_IS_NULL (src_string) || DB_IS_NULL (from_string)
      || DB_IS_NULL (to_string))
    {
      if (QSTR_IS_CHAR (DB_VALUE_DOMAIN_TYPE (src_string)))
	{
	  db_value_domain_init (transed_string, DB_TYPE_VARCHAR, 0, 0);
	}
      else
	{
	  db_value_domain_init (transed_string, DB_TYPE_VARNCHAR, 0, 0);
	}
      return error_status;
    }

  if (!is_char_string (from_string) || !is_char_string (to_string)
      || !is_char_string (src_string))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if ((qstr_get_category (src_string) != qstr_get_category (from_string))
      || (qstr_get_category (src_string) != qstr_get_category (to_string))
      || (qstr_get_category (from_string) != qstr_get_category (to_string)))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  error_status =
    qstr_translate ((unsigned char *) DB_PULL_STRING (src_string),
		    DB_VALUE_DOMAIN_TYPE (src_string),
		    DB_GET_STRING_SIZE (src_string),
		    (INTL_CODESET) DB_GET_STRING_CODESET (src_string),
		    (unsigned char *) DB_PULL_STRING (from_string),
		    DB_GET_STRING_SIZE (from_string),
		    (unsigned char *) DB_PULL_STRING (to_string),
		    DB_GET_STRING_SIZE (to_string),
		    &result_ptr, &result_type, &result_length, &result_size);

  if (error_status == NO_ERROR && result_ptr != NULL)
    {
      if (result_length == 0)
	{
	  qstr_make_typed_string (result_type,
				  transed_string,
				  (DB_GET_STRING_LENGTH (src_string) == 0) ?
				  1 : DB_GET_STRING_LENGTH (src_string),
				  (char *) result_ptr, result_size);
	}
      else
	{
	  qstr_make_typed_string (result_type,
				  transed_string,
				  result_length,
				  (char *) result_ptr, result_size);
	}
      result_ptr[result_size] = 0;
      transed_string->need_clear = true;
    }

  return error_status;
}

/*
 * qstr_translate () -
 */
static int
qstr_translate (unsigned char *src_ptr,
		DB_TYPE src_type,
		int src_size,
		INTL_CODESET codeset,
		unsigned char *from_str_ptr,
		int from_str_size,
		unsigned char *to_str_ptr,
		int to_str_size,
		unsigned char **result_ptr,
		DB_TYPE * result_type, int *result_len, int *result_size)
{
  int error_status = NO_ERROR;
  int j, offset, offset1, offset2;
  int from_char_loc, to_char_cnt, to_char_loc;
  unsigned char *srcp, *fromp, *target = NULL;
  int matched = 0, phase = 0;

  if ((from_str_ptr == NULL && to_str_ptr != NULL))
    {
      error_status = ER_QPROC_INVALID_PARAMETER;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (to_str_ptr == NULL)
    {
      to_str_ptr = (unsigned char *) "";
    }

  /* check from, to string */
  to_char_cnt = 0;
  for (j = 0; j < to_str_size;)
    {
      intl_char_size (to_str_ptr + j, 1, codeset, &offset2);

      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*(to_str_ptr + j)))
	{
	  offset2++;
	}
      j += offset2;
      to_char_cnt++;
    }

  /* calculate total length */
  *result_size = 0;
  phase = 0;

loop:
  srcp = src_ptr;
  for (srcp = src_ptr; srcp < src_ptr + src_size;)
    {
      intl_char_size (srcp, 1, codeset, &offset);
      if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*srcp))
	{
	  offset++;
	}

      matched = 0;
      from_char_loc = 0;
      for (fromp = from_str_ptr;
	   fromp != NULL && fromp < from_str_ptr + from_str_size;
	   from_char_loc++)
	{
	  intl_char_size (fromp, 1, codeset, &offset1);

	  if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (*(fromp)))
	    {
	      offset1++;
	    }

	  /* if source and from char are matched, translate */
	  if ((offset == offset1) && (memcmp (srcp, fromp, offset) == 0))
	    {
	      matched = 1;
	      to_char_loc = 0;
	      for (j = 0; j < to_str_size;)
		{
		  intl_char_size (to_str_ptr + j, 1, codeset, &offset2);

		  if (!PRM_SINGLE_BYTE_COMPARE
		      && IS_KOREAN (*(to_str_ptr + j)))
		    {
		      offset2++;
		    }
		  if (to_char_loc == from_char_loc)
		    {		/* if matched char exist, replace */
		      if (phase == 0)
			{
			  *result_size += offset2;
			}
		      else
			{
			  memcpy (target, to_str_ptr + j, offset2);
			  target += offset2;
			}
		      break;
		    }
		  j += offset2;
		  to_char_loc++;
		}
	      break;
	    }
	  fromp += offset1;
	}
      if (!matched)
	{			/* preserve source char */
	  if (phase == 0)
	    {
	      *result_size += offset;
	    }
	  else
	    {
	      memcpy (target, srcp, offset);
	      target += offset;
	    }
	}
      srcp += offset;
    }

  if (phase == 1)
    {
      return error_status;
    }

  /* evaluate result string length */
  *result_type = QSTR_IS_NATIONAL_CHAR (src_type) ?
    DB_TYPE_VARNCHAR : DB_TYPE_VARCHAR;
  *result_ptr = (unsigned char *)
    db_private_alloc (NULL, (size_t) * result_size + 1);
  if (*result_ptr == NULL)
    {
      error_status = er_errid ();
      return error_status;
    }
  if (phase == 0)
    {
      phase = 1;
      target = *result_ptr;
      *result_len = *result_size;
      goto loop;
    }

  return error_status;
}

/*
 * db_bit_string_coerce () -
 *
 * Arguments:
 *        src_string:  (In) Source string
 *       dest_string: (Out) Coerced string
 *       data_status: (Out) Data status
 *
 * Returns: int
 *
 * Errors:
 *   ER_QSTR_INVALID_DATA_TYPE
 *      <src_string> is not a bit string
 *   ER_QSTR_INCOMPATIBLE_CODE_SETS
 *      <dest_domain> is not a compatible domain type
 *
 * Note:
 *
 *   This function coerces a bit string from one domain to another.
 *   A new DB_VALUE is created making use of the memory manager and
 *   domain information stored in <dest_value>, and coercing the
 *   data portion of <src_string>.
 *
 *   If any loss of data due to truncation occurs, <data_status>
 *   is set to DATA_STATUS_TRUNCATED.
 *
 *   The destination container should have the memory manager, precision
 *   and domain type initialized.
 *
 * Assert:
 *
 *   1. src_string  != (DB_VALUE *) NULL
 *   2. dest_value  != (DB_VALUE *) NULL
 *   3. data_status != (DB_DATA_STATUS *) NULL
 *
 */

int
db_bit_string_coerce (const DB_VALUE * src_string,
		      DB_VALUE * dest_string, DB_DATA_STATUS * data_status)
{
  DB_TYPE src_type, dest_type;

  int error_status = NO_ERROR;

  /* Assert that DB_VALUE structures have been allocated. */
  assert (src_string != (DB_VALUE *) NULL);
  assert (dest_string != (DB_VALUE *) NULL);
  assert (data_status != (DB_DATA_STATUS *) NULL);

  /* Initialize status value */
  *data_status = DATA_STATUS_OK;

  /* Categorize the two input parameters and check for errors.
     Verify that the parameters are both character strings. */
  src_type = DB_VALUE_DOMAIN_TYPE (src_string);
  dest_type = DB_VALUE_DOMAIN_TYPE (dest_string);

  if (!QSTR_IS_BIT (src_type) || !QSTR_IS_BIT (dest_type))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
    }
  else if (qstr_get_category (src_string) != qstr_get_category (dest_string))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
    }
  else if (DB_IS_NULL (src_string))
    {
      db_value_domain_init (dest_string, DB_VALUE_DOMAIN_TYPE (dest_string),
			    0, 0);
    }
  else
    {
      unsigned char *dest;
      int dest_prec;
      int dest_length;

      if (DB_VALUE_PRECISION (dest_string) == TP_FLOATING_PRECISION_VALUE)
	{
	  dest_prec = DB_GET_STRING_LENGTH (src_string);
	}
      else
	{
	  dest_prec = DB_VALUE_PRECISION (dest_string);
	}

      error_status =
	qstr_bit_coerce ((unsigned char *) DB_PULL_STRING (src_string),
			 DB_GET_STRING_LENGTH (src_string),
			 QSTR_VALUE_PRECISION (src_string),
			 src_type, &dest, &dest_length, dest_prec,
			 dest_type, data_status);

      if (error_status == NO_ERROR)
	{
	  qstr_make_typed_string (dest_type,
				  dest_string,
				  DB_VALUE_PRECISION (dest_string),
				  (char *) dest, dest_length);
	  dest_string->need_clear = true;
	}
    }

  return error_status;
}

/*
 * db_char_string_coerce () -
 *
 * Arguments:
 *        src_string:  (In) Source string
 *       dest_string: (Out) Coerced string
 *       data_status: (Out) Data status
 *
 * Returns: int
 *
 * Errors:
 *   ER_QSTR_INVALID_DATA_TYPE
 *      <src_string> and <dest_string> are not both char strings
 *   ER_QSTR_INCOMPATIBLE_CODE_SETS
 *      <dest_domain> is not a compatible domain type
 *
 * Note:
 *
 *   This function coerces a char string from one domain to
 *   another.  A new DB_VALUE is created making use of the
 *   memory manager and domain information stored in
 *   <dest_value>, and coercing the data portion of
 *   <src_string>.
 *
 *   If any loss of data due to truncation occurs, <data_status>
 *   is set to DATA_STATUS_TRUNCATED.
 *
 * Assert:
 *
 *   1. src_string  != (DB_VALUE *) NULL
 *   2. dest_value  != (DB_VALUE *) NULL
 *   3. data_status != (DB_DATA_STATUS *) NULL
 *
 */

int
db_char_string_coerce (const DB_VALUE * src_string,
		       DB_VALUE * dest_string, DB_DATA_STATUS * data_status)
{
  int error_status = NO_ERROR;

  /* Assert that DB_VALUE structures have been allocated. */
  assert (src_string != (DB_VALUE *) NULL);
  assert (dest_string != (DB_VALUE *) NULL);
  assert (data_status != (DB_DATA_STATUS *) NULL);

  /* Initialize status value */
  *data_status = DATA_STATUS_OK;

  /*
   *  Categorize the two input parameters and check for errors.
   *    Verify that the parameters are both character strings.
   *    Verify that the source and destination strings are of
   *    the same character code set.
   *    Verify that the source string is not NULL.
   *    Otherwise, coerce.
   */
  if (!is_char_string (src_string) || !is_char_string (dest_string))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
    }

  else if (qstr_get_category (src_string) != qstr_get_category (dest_string))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
    }

  else if (DB_IS_NULL (src_string))
    {
      db_value_domain_init (dest_string, DB_VALUE_DOMAIN_TYPE (dest_string),
			    0, 0);
    }
  else
    {
      unsigned char *dest;
      int dest_prec;
      int dest_length;
      int dest_size;

      /* Initialize the memory manager of the destination */
      if (DB_VALUE_PRECISION (dest_string) == TP_FLOATING_PRECISION_VALUE)
	{
	  dest_prec = DB_GET_STRING_LENGTH (src_string);
	}
      else
	{
	  dest_prec = DB_VALUE_PRECISION (dest_string);
	}

      error_status =
	qstr_coerce ((unsigned char *) DB_PULL_STRING (src_string),
		     DB_GET_STRING_LENGTH (src_string),
		     QSTR_VALUE_PRECISION (src_string),
		     DB_VALUE_DOMAIN_TYPE (src_string),
		     (INTL_CODESET) DB_GET_STRING_CODESET (src_string),
		     &dest, &dest_length, &dest_size, dest_prec,
		     DB_VALUE_DOMAIN_TYPE (dest_string), data_status);

      if (error_status == NO_ERROR && dest != NULL)
	{
	  qstr_make_typed_string (DB_VALUE_DOMAIN_TYPE (dest_string),
				  dest_string,
				  DB_VALUE_PRECISION (dest_string),
				  (char *) dest, dest_size);
	  dest[dest_size] = 0;
	  dest_string->need_clear = true;
	}
    }

  return error_status;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * db_string_convert () -
 *
 * Arguments:
 *        src_string:  (In) Source string
 *       dest_string: (Out) Converted string
 *       data_status: (Out) Data status
 *
 * Returns: int
 *
 * Errors:
 *   ER_QSTR_INVALID_DATA_TYPE
 *      <src_string> and <dest_string> are not both national char strings
 *   ER_QSTR_INCOMPATIBLE_CODE_SETS
 *      Conversion not supported between code sets of <src_string>
 *      and <dest_string>
 *
 * Note:
 *   This function converts a national character string from one
 *   set encoding to another.
 *
 *   A new DB_VALUE is created making use of the code set and
 *   memory manager stored in <dest_string>, and converting
 *   the characters in the data portion of <src_string>.
 *
 *   If the source string is fixed-length, the destination will be
 *   fixed-length with pad characters.  If the source string is
 *   variable-length, the result will also be variable length.
 *
 * Assert:
 *
 *   1. src_string  != (DB_VALUE *) NULL
 *   2. dest_value  != (DB_VALUE *) NULL
 *   3. data_status != (DB_DATA_STATUS *) NULL
 *
 */

int
db_string_convert (const DB_VALUE * src_string, DB_VALUE * dest_string)
{
  DB_TYPE src_type, dest_type;
  int error_status = NO_ERROR;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_string != (DB_VALUE *) NULL);
  assert (dest_string != (DB_VALUE *) NULL);

  /*
   *  Categorize the two input parameters and check for errors.
   *    Verify that the parameters are both character strings.
   */
  src_type = DB_VALUE_DOMAIN_TYPE (src_string);
  dest_type = DB_VALUE_DOMAIN_TYPE (dest_string);

  if (!QSTR_IS_NATIONAL_CHAR (src_type) || !QSTR_IS_NATIONAL_CHAR (dest_type))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
    }

  else if (DB_IS_NULL (src_string))
    {
      db_value_domain_init (dest_string, DB_VALUE_DOMAIN_TYPE (src_string), 0,
			    0);
    }
  else
    {
      unsigned char *src, *dest;
      int src_length = 0, src_precision;
      INTL_CODESET src_codeset, dest_codeset;
      int convert_status;
      int num_unconverted, cnv_size;


      src = (unsigned char *) DB_GET_NCHAR (src_string, &src_length);
      src_precision = QSTR_VALUE_PRECISION (src_string);

      src_codeset = (INTL_CODESET) DB_GET_STRING_CODESET (src_string);
      dest_codeset = (INTL_CODESET) DB_GET_STRING_CODESET (dest_string);

      /*  Fixed-length strings */

      if (QSTR_IS_FIXED_LENGTH (src_type))
	{
	  /*  Allocate enough room for a fully padded string */
	  dest = (unsigned char *)
	    db_private_alloc (NULL, (size_t) (2 * src_precision) + 1);
	  if (dest == NULL)
	    {
	      goto mem_error;
	    }

	  /*  Convert the string codeset */
	  convert_status = intl_convert_charset (src,
						 src_length,
						 src_codeset,
						 dest,
						 dest_codeset,
						 &num_unconverted);

	  /*  Pad the result */
	  if (convert_status == NO_ERROR)
	    {
	      intl_char_size (dest,
			      (src_length - num_unconverted),
			      dest_codeset, &cnv_size);
	      qstr_pad_string ((unsigned char *) &dest[cnv_size],
			       (src_precision - src_length + num_unconverted),
			       dest_codeset);
	      dest[src_precision] = 0;
	      DB_MAKE_NCHAR (dest_string,
			     src_precision, (char *) dest, src_precision);
	      dest_string->need_clear = true;
	    }
	  else
	    {
	      db_private_free_and_init (NULL, dest);
	    }
	}

      /*  Variable-length strings */
      else
	{
	  /*  Allocate enough room for the string */
	  dest = (unsigned char *)
	    db_private_alloc (NULL, (size_t) (2 * src_length) + 1);
	  if (dest == NULL)
	    {
	      goto mem_error;
	    }

	  /* Convert the string codeset */
	  convert_status = intl_convert_charset (src,
						 src_length,
						 src_codeset,
						 dest,
						 dest_codeset,
						 &num_unconverted);

	  if (convert_status == NO_ERROR)
	    {
	      dest[src_length - num_unconverted] = 0;
	      DB_MAKE_VARNCHAR (dest_string,
				src_precision,
				(char *) dest,
				(src_length - num_unconverted));
	      dest_string->need_clear = true;
	    }
	  else
	    {
	      db_private_free_and_init (NULL, dest);
	    }
	}

      /*
       *  If intl_convert_charset() returned an error, map
       *  to an ER_QSTR_INCOMPATIBLE_CODE_SETS error.
       */
      if (convert_status != NO_ERROR)
	{
	  error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
	}
    }

  return error_status;

  /*
   *  Error handling
   */
mem_error:
  error_status = er_errid ();
  return error_status;
}
#endif

/*
 * qstr_pad_size () -
 *
 * Arguments:
 *      codeset:  (IN) International codeset.
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     Returns the byte size of the pad character for the given codeset.
 *
 */

int
qstr_pad_size (INTL_CODESET codeset)
{
  int size;

  switch (codeset)
    {
    case INTL_CODESET_ISO88591:
    case INTL_CODESET_KSC5601_EUC:
    default:
      size = 1;
      break;
    }

  return size;
}

/*
 * qstr_pad_string () -
 *
 * Arguments:
 *            s: (IN OUT) Pointer to input string.
 *       length: (IN)     Size of input string.
 *      codeset: (IN)     International codeset of input string.
 *
 * Returns: unsigned char
 *
 * Errors:
 *
 * Note:
 *     This is a convinience function which will copy pad characters into
 *     the input string.  It is assumed that the pad character will consist
 *     of one or two bytes (this is currently true).
 *
 *     The address immediately after the padded string is returned.  Thus,
 *     If a NULL terminated string was desired, then a call could be made:
 *
 *         ptr = qstr_pad_string();
 *         *ptr = '\0';
 *
 */

unsigned char *
qstr_pad_string (unsigned char *s, int length, INTL_CODESET codeset)
{
  unsigned char pad[2];
  int i, j, pad_size = 0;


  qstr_pad_char (codeset, pad, &pad_size);

  if (pad_size == 1)
    {
      (void) memset ((char *) s, (int) pad[0], length);
      s = s + length;
    }
  else
    {
      for (i = 0; i < length; i++)
	{
	  for (j = 0; j < pad_size; j++)
	    {
	      *(s++) = pad[j];
	    }
	}
    }

  return s;
}

/*
 * qstr_bin_to_hex () -
 *
 * arguments:
 *        dest: Pointer to destination hex buffer area
 *   dest_size: Size of destination buffer area in bytes
 *         src: Pointer to source binary buffer area
 *    src_size: Size of source buffer area in bytes
 *
 * returns/side-effects: int
 *    The number of converted source bytes is returned.  This value will
 *    equal src_size if (dest_size >= 2*src_size) and less otherwise.
 *
 * description:
 *    Convert the binary data in the source buffer to ASCII hex characters
 *    in the destination buffer.  The destination buffer should be at
 *    least 2 * src_size.  If not, as much of the source string is processed
 *    as possible.  The number of ASCII Hex characters in dest will
 *    equal two times the returned value.
 *
 */

int
qstr_bin_to_hex (char *dest, int dest_size, const char *src, int src_size)
{
  int i, copy_size;

  if (dest_size >= (2 * src_size))
    {
      copy_size = src_size;
    }
  else
    {
      copy_size = dest_size / 2;
    }

  for (i = 0; i < copy_size; i++)
    {
      sprintf (&(dest[2 * i]), "%02x", (unsigned char) (src[i]));
    }

  return copy_size;
}

/*
 * qstr_hex_to_bin () -
 *
 * arguments:
 *        dest: Pointer to destination hex buffer area
 *   dest_size: Size of destination buffer area in bytes
 *         src: Pointer to source binary buffer area
 *    src_size: Size of source buffer area in bytes
 *
 * returns/side-effects: int
 *    The number of converted hex characters is returned.
 *
 * description:
 *    Convert the string of hex characters to decimal values.  For each two
 *    characters, one unsigned character value is produced.  If the number
 *    of characters is odd, then the second nibble of the last byte will
 *    be 0 padded.  If the destination buffer is not large enough to hold
 *    the converted data, as much data is converted as possible.
 *
 */

int
qstr_hex_to_bin (char *dest, int dest_size, char *src, int src_size)
{
  int i, copy_size, src_index, required_size;

  required_size = (src_size + 1) / 2;

  if (dest_size >= required_size)
    {
      copy_size = required_size;
    }
  else
    {
      copy_size = dest_size;
    }

  src_index = 0;
  for (i = 0; i < copy_size; i++)
    {
      int hex_digit;

      hex_digit = hextoi (src[src_index++]);
      if (hex_digit < 0)
	{
	  return -1;
	}
      else
	{
	  dest[i] = hex_digit << 4;
	  if (src_index < src_size)
	    {
	      hex_digit = hextoi (src[src_index++]);
	      if (hex_digit < 0)
		{
		  return -1;
		}
	      else
		{
		  dest[i] += hex_digit;
		}
	    }
	}
    }

  return src_index;
}

/*
 * qstr_bit_to_bin () -
 *
 * arguments:
 *        dest: Pointer to destination buffer area
 *   dest_size: Size of destination buffer area in bytes
 *         src: Pointer to source binary buffer area
 *    src_size: Size of source buffer area in bytes
 *
 * returns/side-effects: int
 *    The number of converted binary characters is returned.
 *
 * description:
 *    Convert the string of '0's and '1's to decimal values.  For each 8
 *    characters, one unsigned character value is produced.  If the number
 *    of characters is not a multiple of 8, the result will assume trailing
 *    0 padding.  If the destination buffer is not large enough to hold
 *    the converted data, as much data is converted as possible.
 *
 */

int
qstr_bit_to_bin (char *dest, int dest_size, char *src, int src_size)
{
  int dest_byte, copy_size, src_index, required_size;

  required_size = (src_size + 7) / 8;

  if (dest_size >= required_size)
    {
      copy_size = required_size;
    }
  else
    {
      copy_size = dest_size;
    }

  src_index = 0;
  for (dest_byte = 0; dest_byte < copy_size; dest_byte++)
    {
      int bit_count;

      dest[dest_byte] = 0;
      for (bit_count = 0; bit_count < 8; bit_count++)
	{
	  dest[dest_byte] = dest[dest_byte] << 1;
	  if (src_index < src_size)
	    {
	      if (src[src_index] == '1')
		{
		  dest[dest_byte]++;
		}
	      else if (src[src_index] != '0')
		{
		  return -1;	/* Illegal digit */
		}
	      src_index++;
	    }
	}
    }

  return src_index;
}

/*
 * qstr_bit_to_hex_coerce () -
 *
 * arguments:
 *      buffer: Pointer to destination buffer area
 * buffer_size: Size of destination buffer area (in bytes, *including* null
 *              terminator)
 *         src: Pointer to source buffer area
 *  src_length: Length of source buffer area in bits
 *    pad_flag: TRUE if the buffer should be padded and FALSE otherwise
 *   copy_size: Number of bytes transfered from the src string to the dst
 *              buffer
 *  truncation: pointer to a int field.  *outlen will equal 0 if no
 *              truncation occurred and will equal the size of the dst buffer
 *              in bytes needed to avoid truncation (not including the
 *              terminating NULL), otherwise.
 *
 * returns/side-effects: void
 *
 * description:
 *    Transfers at most buffer_size bytes to the region pointed at by dst.
 *    If  pad_flag is TRUE, strings shorter than buffer_size will be
 *    blank-padded out to buffer_size-1 bytes.  All strings will be
 *    null-terminated.  If truncation is necessary (i.e., if buffer_size is
 *    less than or equal to src_length), *truncation is set to src_length;
 *    if truncation is is not necessary, *truncation is set to 0.
 *
 */

void
qstr_bit_to_hex_coerce (char *buffer,
			int buffer_size,
			const char *src,
			int src_length,
			int pad_flag, int *copy_size, int *truncation)
{
  int src_size = QSTR_NUM_BYTES (src_length);


  if (buffer_size > (2 * src_size))
    {
      /*
       * No truncation; copy the data and blank pad if necessary.
       */
      qstr_bin_to_hex (buffer, buffer_size, src, src_size);
/*
	for (i=0; i<src_size; i++)
	    sprintf(&(buffer[2*i]), "%02x", (unsigned char)(src[i]));
*/
      if (pad_flag == true)
	{
	  memset (&(buffer[2 * src_size]), '0',
		  (buffer_size - (2 * src_size)));
	  *copy_size = buffer_size - 1;
	}
      else
	{
	  *copy_size = 2 * src_size;
	}
      buffer[*copy_size] = '\0';
      *truncation = 0;
    }
  else
    {
      /*
       * Truncation is necessary; put as many bytes as possible into
       * the receiving buffer and null-terminate it (i.e., it receives
       * at most dstsize-1 bytes).  If there is not outlen indicator by
       * which we can indicate truncation, this is an error.
       *
       */
      if (buffer_size % 2)
	{
	  src_size = buffer_size / 2;
	}
      else
	{
	  src_size = (buffer_size - 1) / 2;
	}

      qstr_bin_to_hex (buffer, buffer_size, src, src_size);
/*
	for (i=0; i<src_size; i++)
	    sprintf(&(buffer[2*i]), "%02x", (unsigned char)(src[i]));
*/
      *copy_size = 2 * src_size;
      buffer[*copy_size] = '\0';

      *truncation = src_size;
    }
}

/*
 * db_get_string_length
 *
 * Arguments:
 *        value: Value  container
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     Returns the character length of the string in the container.
 *
 */

int
db_get_string_length (const DB_VALUE * value)
{
  DB_C_CHAR str;
  int size;
  INTL_CODESET codeset;
  int length = 0;

#if 0
  /* Currently, only the medium model is used */

  switch (value->data.ch.info.style)
    {
    case SMALL_STRING:
      str = value->data.ch.small.buf;
      length = size = value->data.ch.small.size;
      codeset = value->data.ch.small.codeset;
      break;

    case MEDIUM_STRING:
      str = value->data.ch.medium.buf;
      length = size = value->data.ch.medium.size;
      codeset = value->data.ch.medium.codeset;
      break;

    case LARGE_STRING:
      str = NULL;
      size = 0;
      break;

    default:
      break;
    }
#endif

  str = value->data.ch.medium.buf;
  length = size = value->data.ch.medium.size;
  codeset = (INTL_CODESET) value->data.ch.medium.codeset;

  if (value->domain.general_info.type != DB_TYPE_BIT &&
      value->domain.general_info.type != DB_TYPE_VARBIT)
    {
      intl_char_count ((unsigned char *) str, size, codeset, &length);
    }

  return length;
}

/*
 * qstr_make_typed_string () -
 *
 * Arguments:
 *       domain: Domain type for the result.
 *        value: Value container for the result.
 *    precision: Length of the string precision.
 *          src: Pointer to string.
 *       s_unit: Size of the string.
 *
 * Returns: void
 *
 * Errors:
 *
 * Note:
 *     Make a value container from the string of the given domain.
 *     This is a convinience function which allows for all string
 *     types given the proper domain type.
 *
 */

void
qstr_make_typed_string (DB_TYPE domain,
			DB_VALUE * value,
			int precision, const DB_C_CHAR src, const int s_unit)
{
  switch (domain)
    {
    case DB_TYPE_CHAR:
      DB_MAKE_CHAR (value, precision, src, s_unit);
      break;

    case DB_TYPE_VARCHAR:
      DB_MAKE_VARCHAR (value, precision, src, s_unit);
      break;

    case DB_TYPE_NCHAR:
      DB_MAKE_NCHAR (value, precision, src, s_unit);
      break;

    case DB_TYPE_VARNCHAR:
      DB_MAKE_VARNCHAR (value, precision, src, s_unit);
      break;

    case DB_TYPE_BIT:
      DB_MAKE_BIT (value, precision, src, s_unit);
      break;

    case DB_TYPE_VARBIT:
      DB_MAKE_VARBIT (value, precision, src, s_unit);
      break;

    default:
      DB_MAKE_NULL (value);
      break;
    }
}

/*
 *  Private Functions
 */

/*
 * qstr_get_category
 *
 * Arguments:
 *      s: DB_VALUE representation of a string.
 *
 * Returns: QSTR_CATEGORY
 *
 * Errors:
 *
 * Note:
 *   Returns the character code set of the string "s."  The character code
 *   set of strings is:
 *
 *       QSTR_CHAR, QSTR_NATIONAL_CHAR, QSTR_BIT
 *
 *   as defined in type QSTR_CATEGORY.  A value of QSTR_UNKNOWN is defined
 *   if the string does not fit into one of these categories.  This should
 *   never happen if is_string() returns TRUE.
 *
 */

static QSTR_CATEGORY
qstr_get_category (const DB_VALUE * s)
{
  QSTR_CATEGORY code_set;

  switch (DB_VALUE_DOMAIN_TYPE (s))
    {

    case DB_TYPE_VARCHAR:
    case DB_TYPE_CHAR:
      code_set = QSTR_CHAR;
      break;

    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      code_set = QSTR_NATIONAL_CHAR;
      break;

    case DB_TYPE_BIT:
    case DB_TYPE_VARBIT:
      code_set = QSTR_BIT;
      break;

    default:
      code_set = QSTR_UNKNOWN;
      break;
    }

  return code_set;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * is_string () -
 *
 * Arguments:
 *      s: (IN) DB_VALUE variable.
 *
 * Returns: bool
 *
 * Errors:
 *
 * Note:
 *   Verifies that the value is a string.  Returns TRUE if the
 *   domain type is one of:
 *
 *       DB_TYPE_STRING
 *       DB_TYPE_CHAR
 *       DB_TYPE_VARCHAR
 *       DB_TYPE_NCHAR
 *       DB_TYPE_VARNCHAR
 *       DB_TYPE_BIT
 *       DB_TYPE_VARBIT
 *
 *   Returns FALSE otherwise.
 *
 *   This function supports the older type DB_TYPE_STRING which
 *   has been replaced by DB_TYPE_VARCHAR.
 *
 */

static bool
is_string (const DB_VALUE * s)
{
  DB_TYPE domain_type = DB_VALUE_DOMAIN_TYPE (s);

  return QSTR_IS_ANY_CHAR_OR_BIT (domain_type);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * is_char_string () -
 *
 * Arguments:
 *      s: DB_VALUE variable.
 *
 * Returns: bool
 *
 * Errors:
 *
 * Note:
 *   Verifies that the value is a character string.  Returns TRUE if the
 *   value is of domain type is one of:
 *
 *       DB_TYPE_STRING
 *       DB_TYPE_VARCHAR
 *       DB_TYPE_CHAR
 *       DB_TYPE_NCHAR
 *       DB_TYPE_VARNCHAR
 *
 *   Returns FALSE otherwise.
 *
 *   This function supports the older type DB_TYPE_STRING which
 *   has been replaced by DB_TYPE_VARCHAR.
 *
 */

static bool
is_char_string (const DB_VALUE * s)
{
  DB_TYPE domain_type = DB_VALUE_DOMAIN_TYPE (s);

  return ((domain_type == DB_TYPE_STRING)
	  || (QSTR_IS_ANY_CHAR (domain_type)));
}

/*
 * is_integer () -
 *
 * Arguments:
 *      i: (IN) DB_VALUE variable.
 *
 * Returns: bool
 *
 * Errors:
 *
 * Note:
 *   Verifies that the value is an integer.  Returns TRUE if the
 *   value is of domain type is one of:
 *
 *       DB_TYPE_INTEGER
 *
 *   Returns FALSE otherwise.
 *
 */

static bool
is_integer (const DB_VALUE * i)
{
  return (DB_VALUE_DOMAIN_TYPE (i) == DB_TYPE_INTEGER);
}

/*
 * is_number () -
 *
 * Arguments:
 *      n: (IN) DB_VALUE variable.
 *
 * Returns: bool
 *
 * Errors:
 *
 * Note:
 *   Verifies that the value is an number.  Returns TRUE if the
 *   value is of domain type is one of:
 *
 *       DB_TYPE_NUMERIC
 *       DB_TYPE_INTEGER
 *       DB_TYPE_SMALLINT
 *       DB_TYPE_DOUBLE
 *       DB_TYPE_FLOAT
 *
 *   Returns FALSE otherwise.
 *
 */

static bool
is_number (const DB_VALUE * n)
{
  DB_TYPE domain_type = DB_VALUE_DOMAIN_TYPE (n);

  return ((domain_type == DB_TYPE_NUMERIC) ||
	  (domain_type == DB_TYPE_INTEGER) ||
	  (domain_type == DB_TYPE_SMALLINT) ||
	  (domain_type == DB_TYPE_BIGINT) ||
	  (domain_type == DB_TYPE_DOUBLE) || (domain_type == DB_TYPE_FLOAT));
}


/*
 * qstr_compare () - compare two character strings of DB_TYPE_STRING(tp_String)
 *
 * Arguments:
 *      string1: 1st character string
 *        size1: size of 1st string
 *      string2: 2nd character string
 *        size2: size of 2nd string
 *
 * Returns:
 *   Greater than 0 if string1 > string2
 *   Equal to 0     if string1 = string2
 *   Less than 0    if string1 < string2
 *
 * Errors:
 *
 * Note:
 *   This function is similar to strcmp(3) or bcmp(3). It is designed to
 *   follow SQL_TEXT character set collation. Padding character(space ' ') is
 *   the smallest character in the set. (e.g.) "ab z" < "ab\t1"
 *
 */

int
qstr_compare (const unsigned char *string1, int size1,
	      const unsigned char *string2, int size2)
{
  int n, i, cmp;
  unsigned char c1, c2;

#define PAD ' '			/* str_pad_char(INTL_CODESET_ISO88591, pad, &pad_size) */
#define SPACE PAD		/* smallest character in the collation sequence */
#define ZERO '\0'		/* space is treated as zero */

  n = size1 < size2 ? size1 : size2;
  for (i = 0, cmp = 0; i < n && cmp == 0; i++)
    {
      c1 = *string1++;
      if (c1 == SPACE)
	{
	  c1 = ZERO;
	}
      c2 = *string2++;
      if (c2 == SPACE)
	{
	  c2 = ZERO;
	}
      cmp = c1 - c2;
    }
  if (cmp != 0)
    {
      return cmp;
    }
  if (size1 == size2)
    {
      return cmp;
    }

  c1 = c2 = ZERO;
  if (size1 < size2)
    {
      n = size2 - size1;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c2 = *string2++;
	  if (c2 == PAD)
	    {
	      c2 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  else
    {
      n = size1 - size2;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c1 = *string1++;
	  if (c1 == PAD)
	    {
	      c1 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  return cmp;

#undef PAD
#undef SPACE
#undef ZERO
}				/* qstr_compare() */

/*
 * char_compare () - compare two character strings of DB_TYPE_CHAR(tp_Char)
 *
 * Arguments:
 *      string1: 1st character string
 *        size1: size of 1st string
 *      string2: 2nd character string
 *        size2: size of 2nd string
 *
 * Returns:
 *   Greater than 0 if string1 > string2
 *   Equal to 0     if string1 = string2
 *   Less than 0    if string1 < string2
 *
 * Errors:
 *
 * Note:
 *   This function is identical to qstr_compare().
 *
 */

int
char_compare (const unsigned char *string1, int size1,
	      const unsigned char *string2, int size2)
{
  int n, i, cmp;
  unsigned char c1, c2;

#define PAD ' '			/* str_pad_char(INTL_CODESET_ISO88591, pad, &pad_size) */
#define SPACE PAD		/* smallest character in the collation sequence */
#define ZERO '\0'		/* space is treated as zero */

  n = size1 < size2 ? size1 : size2;
  for (i = 0, cmp = 0; i < n && cmp == 0; i++)
    {
      c1 = *string1++;
      if (c1 == SPACE)
	{
	  c1 = ZERO;
	}
      c2 = *string2++;
      if (c2 == SPACE)
	{
	  c2 = ZERO;
	}
      cmp = c1 - c2;
    }
  if (cmp != 0)
    {
      return cmp;
    }
  if (size1 == size2)
    {
      return cmp;
    }

  c1 = c2 = ZERO;
  if (size1 < size2)
    {
      n = size2 - size1;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c2 = *string2++;
	  if (c2 == PAD)
	    {
	      c2 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  else
    {
      n = size1 - size2;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c1 = *string1++;
	  if (c1 == PAD)
	    {
	      c1 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  return cmp;

#undef PAD
#undef SPACE
#undef ZERO
}				/* char_compare() */

/*
 * varnchar_compare () - compare two national character strings of
 *                    DB_TYPE_VARNCHAR(tp_VarNChar)
 *
 * Arguments:
 *      string1: 1st national character string
 *        size1: size of 1st string
 *      string2: 2nd national character string
 *        size2: size of 2nd string
 *      codeset: codeset of strings
 *
 * Returns:
 *   Greater than 0 if string1 > string2
 *   Equal to 0     if string1 = string2
 *   Less than 0    if string1 < string2
 *
 * Errors:
 *
 * Note:
 *   This function is identical to qstr_compare() except that it awares
 *   of the codeset.
 *
 */

int
varnchar_compare (const unsigned char *string1, int size1,
		  const unsigned char *string2, int size2,
		  INTL_CODESET codeset)
{
  int n, i, cmp, pad_size = 0;
  unsigned char c1, c2, pad[2];

  qstr_pad_char (codeset, pad, &pad_size);
#define PAD pad[i % pad_size]
#define SPACE PAD		/* smallest character in the collation sequence */
#define ZERO '\0'		/* space is treated as zero */
  n = size1 < size2 ? size1 : size2;
  for (i = 0, cmp = 0; i < n && cmp == 0; i++)
    {
      c1 = *string1++;
      if (c1 == SPACE)
	{
	  c1 = ZERO;
	}
      c2 = *string2++;
      if (c2 == SPACE)
	{
	  c2 = ZERO;
	}
      cmp = c1 - c2;
    }
  if (cmp != 0)
    {
      return cmp;
    }
  if (size1 == size2)
    {
      return cmp;
    }

  c1 = c2 = ZERO;
  if (size1 < size2)
    {
      n = size2 - size1;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c2 = *string2++;
	  if (c2 == PAD)
	    {
	      c2 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  else
    {
      n = size1 - size2;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c1 = *string1++;
	  if (c1 == PAD)
	    {
	      c1 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  return cmp;
#undef SPACE
#undef ZERO
#undef PAD
}				/* varnchar_compare() */

/*
 * nchar_compare () - compare two national character strings of
 *                 DB_TYPE_NCHAR(tp_NChar)
 *
 * Arguments:
 *      string1: 1st national character string
 *        size1: size of 1st string
 *      string2: 2nd national character string
 *        size2: size of 2nd string
 *      codeset: codeset of strings
 *
 * Returns:
 *   Greater than 0 if string1 > string2
 *   Equal to 0     if string1 = string2
 *   Less than 0    if string1 < string2
 *
 * Errors:
 *
 * Note:
 *   This function is identical to qstr_compare() except that it awares
 *   of the codeset.
 *
 */

int
nchar_compare (const unsigned char *string1, int size1,
	       const unsigned char *string2, int size2, INTL_CODESET codeset)
{
  int n, i, cmp, pad_size = 0;
  unsigned char c1, c2, pad[2];

  qstr_pad_char (codeset, pad, &pad_size);
#define PAD pad[i % pad_size]
#define SPACE PAD		/* smallest character in the collation sequence */
#define ZERO '\0'		/* space is treated as zero */
  n = size1 < size2 ? size1 : size2;
  for (i = 0, cmp = 0; i < n && cmp == 0; i++)
    {
      c1 = *string1++;
      if (c1 == SPACE)
	{
	  c1 = ZERO;
	}
      c2 = *string2++;
      if (c2 == SPACE)
	{
	  c2 = ZERO;
	}
      cmp = c1 - c2;
    }
  if (cmp != 0)
    {
      return cmp;
    }
  if (size1 == size2)
    {
      return cmp;
    }

  c1 = c2 = ZERO;
  if (size1 < size2)
    {
      n = size2 - size1;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c2 = *string2++;
	  if (c2 == PAD)
	    {
	      c2 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  else
    {
      n = size1 - size2;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c1 = *string1++;
	  if (c1 == PAD)
	    {
	      c1 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  return cmp;
#undef SPACE
#undef ZERO
#undef PAD
}				/* nchar_compare() */

/*
 * bit_compare () - compare two bit strings of DB_TYPE_BIT(tp_Bit)
 *
 * Arguments:
 *      string1: 1st bit string
 *        size1: size of 1st string
 *      string2: 2nd bit string
 *        size2: size of 2nd string
 *      codeset: codeset of strings
 *
 * Returns:
 *   Greater than 0 if string1 > string2
 *   Equal to 0     if string1 = string2
 *   Less than 0    if string1 < string2
 *
 * Errors:
 *
 * Note:
 *   This function is identical to qstr_compare().
 *
 */

int
bit_compare (const unsigned char *string1, int size1,
	     const unsigned char *string2, int size2)
{
  int n, i, cmp;

#define PAD '\0'		/* str_pad_char(INTL_CODESET_RAW_BITS, pad, &pad_size) */
  n = size1 < size2 ? size1 : size2;
  for (i = 0, cmp = 0; i < n && cmp == 0; i++)
    {
      cmp = (*string1++ - *string2++);
    }
  if (cmp != 0)
    {
      return cmp;
    }
  cmp = size1 - size2;
  return cmp;
#undef PAD
}				/* bit_compare() */

/*
 * varbit_compare () - compare two bit strings of DB_TYPE_VARBIT(tp_VarBit)
 *
 * Arguments:
 *      string1: 1st bit string
 *        size1: size of 1st string
 *      string2: 2nd bit string
 *        size2: size of 2nd string
 *      codeset: codeset of strings
 *
 * Returns:
 *   Greater than 0 if string1 > string2
 *   Equal to 0     if string1 = string2
 *   Less than 0    if string1 < string2
 *
 * Errors:
 *
 * Note:
 *   This function is identical to qstr_compare().
 *
 */

int
varbit_compare (const unsigned char *string1, int size1,
		const unsigned char *string2, int size2)
{
  int n, i, cmp;

#define PAD '\0'		/* str_pad_char(INTL_CODESET_RAW_BITS, pad, &pad_size) */
  n = size1 < size2 ? size1 : size2;
  for (i = 0, cmp = 0; i < n && cmp == 0; i++)
    {
      cmp = (*string1++ - *string2++);
    }
  if (cmp != 0)
    {
      return cmp;
    }
  cmp = size1 - size2;
  return cmp;
#undef PAD
}				/* varbit_compare() */

/*
 * qstr_concatenate () -
 *
 * Arguments:
 *             s1: (IN)  First string pointer.
 *      s1_length: (IN)  Character length of <s1>.
 *   s1_precision: (IN)  Max character length of <s1>.
 *        s1_type: (IN)  Domain type of <s1>.
 *             s2: (IN)  Second string pointer.
 *      s2_length: (IN)  Character length of <s2>.
 *   s2_precision: (IN)  Max character length of <s2>.
 *        s2_type: (IN)  Domain type of <s2>.
 *        codeset: (IN)  international codeset.
 *         result: (OUT) Concatenated string pointer.
 *  result_length: (OUT) Character length of <result>.
 *    result_size: (OUT) Byte size of <result>.
 *    result_type: (OUT) Domain type of <result>
 *
 * Returns:
 *
 * Errors:
 *
 */

static int
qstr_concatenate (const unsigned char *s1,
		  int s1_length,
		  int s1_precision,
		  DB_TYPE s1_type,
		  const unsigned char *s2,
		  int s2_length,
		  int s2_precision,
		  DB_TYPE s2_type,
		  INTL_CODESET codeset,
		  unsigned char **result,
		  int *result_length,
		  int *result_size,
		  DB_TYPE * result_type, DB_DATA_STATUS * data_status)
{
  int copy_length, copy_size;
  int pad1_length, pad2_length;
  int length_left, cat_length, cat_size;
  int s1_logical_length, s2_logical_length;
  unsigned char *cat_ptr;
  int error_status = NO_ERROR;

  *data_status = DATA_STATUS_OK;

  /*
   *  Categorize the source string into fixed and variable
   *  length.  Variable length strings are simple.  Fixed
   *  length strings have to be handled special since the
   *  strings may not have all of their pad character allocated
   *  yet.  We have to account for this and act as if all of the
   *  characters are present.  They all will be by the time
   *  we are through.
   */
  if (QSTR_IS_FIXED_LENGTH (s1_type))
    {
      s1_logical_length = s1_precision;
    }
  else
    {
      s1_logical_length = s1_length;
    }


  if (QSTR_IS_FIXED_LENGTH (s2_type))
    {
      s2_logical_length = s2_precision;
    }
  else
    {
      s2_logical_length = s2_length;
    }

  /*
   *  If both source strings are fixed-length, the concatenated
   *  result will be fixed-length.
   */
  if (QSTR_IS_FIXED_LENGTH (s1_type) && QSTR_IS_FIXED_LENGTH (s2_type))
    {
      /*
       *  The result will be a chararacter string of length =
       *  string1_precision + string2_precision.  If the result
       *  length is greater than the maximum allowed for a fixed
       *  length string, the TRUNCATED exception is raised and
       *  the string is  shortened appropriately.
       */
      *result_length = s1_logical_length + s2_logical_length;
      if (*result_length > QSTR_MAX_PRECISION (s1_type))
	{
	  *result_length = QSTR_MAX_PRECISION (s1_type);
	  *data_status = DATA_STATUS_TRUNCATED;
	}

      if (QSTR_IS_NATIONAL_CHAR (s1_type))
	{
	  *result_size = *result_length * 2;
	  *result_type = DB_TYPE_NCHAR;
	}
      else
	{
	  *result_size = *result_length;
	  *result_type = DB_TYPE_CHAR;
	}

      /* Allocate storage for the result string */
      *result = (unsigned char *) db_private_alloc (NULL,
						    (size_t) * result_size +
						    1);
      if (*result == NULL)
	{
	  goto mem_error;
	}

      /*
       *  Determine how much of string1 needs to be copied.
       *  Remember that this may or may not include needed padding.
       *  Then determine how much padding must be added to each
       *  source string.
       */
      copy_length = MIN (s1_length, *result_length);
      intl_char_size ((unsigned char *) s1, copy_length, codeset, &copy_size);

      pad1_length = MIN (s1_logical_length, *result_length) - copy_length;
      length_left = *result_length - copy_length - pad1_length;

      /*
       *  Determine how much of string2 can be concatenated after
       *  string1.  Remember that string2 is concatentated after
       *  the full length of string1 including any necessary pad
       *  characters.
       */
      cat_length = MIN (s2_length, length_left);
      intl_char_size ((unsigned char *) s2, cat_length, codeset, &cat_size);

      pad2_length = length_left - cat_length;

      /*
       *  Copy the source strings into the result string
       */
      memcpy ((char *) *result, (char *) s1, copy_size);
      cat_ptr = qstr_pad_string ((unsigned char *) &((*result)[copy_size]),
				 pad1_length, codeset);

      memcpy ((char *) cat_ptr, (char *) s2, cat_size);
      (void) qstr_pad_string ((unsigned char *) &cat_ptr[cat_size],
			      pad2_length, codeset);
    }
  /*
   *  If either source string is variable-length, the concatenated
   *  result will be variable-length.
   */
  else
    {
      /*
       *  The result length will be the sum of the lengths of
       *  the two source strings.  If this is greater than the
       *  maximum length of a variable length string, then the
       *  result length is adjusted appropriately.  This does
       *  not necessarily indicate a truncation condition.
       */
      *result_length = MIN ((s1_logical_length + s2_logical_length),
			    QSTR_MAX_PRECISION (s1_type));

      if ((s1_type == DB_TYPE_NCHAR) || (s1_type == DB_TYPE_VARNCHAR))
	{
	  *result_size = *result_length * 2;
	  *result_type = DB_TYPE_VARNCHAR;
	}
      else
	{
	  *result_size = *result_length;
	  *result_type = DB_TYPE_VARCHAR;
	}

      /*  Allocate the result string */
      *result = (unsigned char *) db_private_alloc (NULL,
						    (size_t) * result_size +
						    1);
      if (*result == NULL)
	{
	  goto mem_error;
	}


      /*
       *  Calculate the number of characters from string1 that can
       *  be copied to the result.  If we cannot copy the entire
       *  string and if the portion of the string which was not
       *  copied contained anything but pad characters, then raise
       *  a truncation exception.
       */
      copy_length = s1_length;
      if (copy_length > *result_length)
	{
	  copy_length = *result_length;

	  if (varchar_truncated ((unsigned char *) s1,
				 s1_type, s1_length, copy_length, codeset))
	    {
	      *data_status = DATA_STATUS_TRUNCATED;
	    }
	}
      intl_char_size ((unsigned char *) s1, copy_length, codeset, &copy_size);

      pad1_length = MIN (s1_logical_length, *result_length) - copy_length;
      length_left = *result_length - copy_length - pad1_length;

      /*
       *  Processess string2 as we did for string1.
       */
      cat_length = s2_length;
      if (cat_length > (*result_length - copy_length))
	{
	  cat_length = *result_length - copy_length;

	  if (varchar_truncated ((unsigned char *) s2,
				 s2_type, s2_length, cat_length, codeset))
	    {
	      *data_status = DATA_STATUS_TRUNCATED;
	    }
	}
      intl_char_size ((unsigned char *) s2, cat_length, codeset, &cat_size);

      pad2_length = length_left - cat_length;

      /*
       *  Actually perform the copy operations.
       */
      memcpy ((char *) *result, (char *) s1, copy_size);
      cat_ptr = qstr_pad_string ((unsigned char *) &((*result)[copy_size]),
				 pad1_length, codeset);

      memcpy ((char *) cat_ptr, (char *) s2, cat_size);
      (void) qstr_pad_string ((unsigned char *) &cat_ptr[cat_size],
			      pad2_length, codeset);
    }

  intl_char_size (*result, *result_length, codeset, result_size);

  return error_status;

  /*
   * Error handler
   */
mem_error:
  error_status = er_errid ();
  return error_status;
}

/*
 * qstr_bit_concatenate () -
 *
 * Arguments:
 *           s1: (IN)  First string pointer.
 *    s1_length: (IN)  Character length of <s1>.
 * s1_precision: (IN)  Max character length of <s1>.
 *      s1_type: (IN)  Domain type of <s1>.
 *           s2: (IN)  Second string pointer.
 *    s2_length: (IN)  Character length of <s2>.
 * s2_precision: (IN)  Max character length of <s2>.
 *      s2_type: (IN)  Domain type of <s2>.
 *       result: (OUT) Concatenated string pointer.
 * result_length: (OUT) Character length of <result>.
 *  result_size: (OUT) Byte size of <result>.
 *  result_type: (OUT) Domain type of <result>
 *
 * Returns:
 *
 * Errors:
 *
 */

static int
qstr_bit_concatenate (const unsigned char *s1,
		      int s1_length,
		      int s1_precision,
		      DB_TYPE s1_type,
		      const unsigned char *s2,
		      int s2_length,
		      int s2_precision,
		      DB_TYPE s2_type,
		      unsigned char **result,
		      int *result_length,
		      int *result_size,
		      DB_TYPE * result_type, DB_DATA_STATUS * data_status)
{
  int s1_size, s2_size;
  int copy_length, cat_length;
  int s1_logical_length, s2_logical_length;
  int error_status = NO_ERROR;

  *data_status = DATA_STATUS_OK;

  /*
   *  Calculate the byte size of the strings.
   *  Calculate the bit length and byte size needed to concatenate
   *  the two strings without truncation.
   */
  s1_size = QSTR_NUM_BYTES (s1_length);
  s2_size = QSTR_NUM_BYTES (s2_length);


  /*
   *  Categorize the source string into fixed and variable
   *  length.  Variable length strings are simple.  Fixed
   *  length strings have to be handled special since the
   *  strings may not have all of their pad character allocated
   *  yet.  We have to account for this and act as if all of the
   *  characters are present.  They all will be by the time
   *  we are through.
   */
  if ((s1_type == DB_TYPE_CHAR) || (s1_type == DB_TYPE_NCHAR))
    {
      s1_logical_length = s1_precision;
    }
  else
    {
      s1_logical_length = s1_length;
    }


  if ((s2_type == DB_TYPE_CHAR) || (s2_type == DB_TYPE_NCHAR))
    {
      s2_logical_length = s2_precision;
    }
  else
    {
      s2_logical_length = s2_length;
    }


  if ((s1_type == DB_TYPE_BIT) && (s2_type == DB_TYPE_BIT))
    {
      /*
       *  The result will be a bit string of length =
       *  string1_precision + string2_precision.  If the result
       *  length is greater than the maximum allowed for a fixed
       *  length string, the TRUNCATED exception is raised and
       *  the string is shortened appropriately.
       */
      *result_type = DB_TYPE_BIT;
      *result_length = s1_logical_length + s2_logical_length;

      if (*result_length > DB_MAX_BIT_LENGTH)
	{
	  *result_length = DB_MAX_BIT_LENGTH;
	  *data_status = DATA_STATUS_TRUNCATED;
	}
      *result_size = QSTR_NUM_BYTES (*result_length);


      /*  Allocate the result string */
      *result = (unsigned char *) db_private_alloc (NULL,
						    (size_t) * result_size +
						    1);
      if (*result == NULL)
	{
	  goto mem_error;
	}

      /*
       *  The source strings may not be fully padded, so
       *  we pre-pad the result string.
       */
      (void) memset ((char *) *result, (int) 0, (int) *result_size);

      /*
       *  Determine how much of string1 needs to be copied.
       *  Remember that this may or may not include needed padding
       */
      copy_length = s1_length;
      if (copy_length > *result_length)
	{
	  copy_length = *result_length;
	}

      /*
       *  Determine how much of string2 can be concatenated after
       *  string1.  Remember that string2 is concatentated after
       *  the full length of string1 including any necessary pad
       *  characters.
       */
      cat_length = s2_length;
      if (cat_length > (*result_length - s1_logical_length))
	{
	  cat_length = *result_length - s1_logical_length;
	}


      /*
       *  Copy the source strings into the result string.
       *  We are being a bit sloppy here by performing a byte
       *  copy as opposed to a bit copy.  But this should be OK
       *  since the bit strings should be bit padded with 0' s */
      bit_ncat (*result, 0, (unsigned char *) s1, copy_length);
      bit_ncat (*result, s1_logical_length, (unsigned char *) s2, cat_length);
    }

  else				/* Assume DB_TYPE_VARBIT */
    {
      /*
       *  The result length will be the sum of the lengths of
       *  the two source strings.  If this is greater than the
       *  maximum length of a variable length string, then the
       *  result length is adjusted appropriately.  This does
       *  not necessarily indicate a truncation condition.
       */
      *result_type = DB_TYPE_VARBIT;
      *result_length = s1_logical_length + s2_logical_length;
      if (*result_length > DB_MAX_BIT_LENGTH)
	*result_length = DB_MAX_BIT_LENGTH;

      *result_size = QSTR_NUM_BYTES (*result_length);

      /* Allocate storage for the result string */
      *result = (unsigned char *) db_private_alloc (NULL,
						    (size_t) * result_size +
						    1);
      if (*result == NULL)
	{
	  goto mem_error;
	}

      /*
       *  The source strings may not be fully padded, so
       *  we pre-pad the result string.
       */
      (void) memset ((char *) *result, (int) 0, (int) *result_size);

      /*
       *  Calculate the number of bits from string1 that can
       *  be copied to the result.  If we cannot copy the entire
       *  string and if the portion of the string which was not
       *  copied contained anything but 0's, then raise a
       *  truncation exception.
       */
      copy_length = s1_length;
      if (copy_length > *result_length)
	{
	  copy_length = *result_length;
	  if (varbit_truncated (s1, s1_length, copy_length))
	    {
	      *data_status = DATA_STATUS_TRUNCATED;
	    }
	}

      /*  Processess string2 as we did for string1. */
      cat_length = s2_length;
      if (cat_length > (*result_length - copy_length))
	{
	  cat_length = *result_length - copy_length;
	  if (varbit_truncated (s2, s2_length, cat_length))
	    {
	      *data_status = DATA_STATUS_TRUNCATED;
	    }
	}


      /*
       *  Actually perform the copy operations and
       *  place the result string in a container.
       */
      bit_ncat (*result, 0, (unsigned char *) s1, copy_length);
      bit_ncat (*result, copy_length, s2, cat_length);
    }

  return error_status;

  /*
   *  Error handling
   */
mem_error:
  error_status = er_errid ();
  return error_status;
}

/*
 * qstr_pad_char () -
 *
 * Arguments:
 *      codeset:  (IN) International codeset.
 *     pad_char: (OUT) Pointer to array which will be filled with
 *                     the pad character.
 *     pad_size: (OUT) Size of pad character.
 *
 * Returns: void
 *
 * Errors:
 *
 * Note:
 *     There is a pad character associated with every character code
 *     set.  This function will retrieve the pad character for a given
 *     code set.  The pad character is written into an array that must
 *     allocated by the caller.
 */

static void
qstr_pad_char (INTL_CODESET codeset, unsigned char *pad_char, int *pad_size)
{
  switch (codeset)
    {
    case INTL_CODESET_RAW_BITS:
    case INTL_CODESET_RAW_BYTES:
      pad_char[0] = '\0';
      *pad_size = 1;
      break;

    case INTL_CODESET_KSC5601_EUC:
      pad_char[0] = pad_char[1] = '\241';
      *pad_size = 2;
      break;

    case INTL_CODESET_ASCII:
    case INTL_CODESET_ISO88591:
      pad_char[0] = ' ';
      *pad_size = 1;
      break;

    default:
      break;
    }
}

/*
 * varchar_truncated () -
 *
 * Arguments:
 *            s:  (IN) Pointer to input string.
 *     s_length:  (IN) Length of input string.
 *   used_chars:  (IN) Number of characters which were used by caller.
 *                     0 <= <used_chars> <= <s_length>
 *      codeset:  (IN) international codeset of input string.
 *
 * Returns: bool
 *
 * Errors:
 *
 * Note:
 *     This is a convinience function which is used by the concatenation
 *     function to determine if a variable length string has been
 *     truncated.  When concatenating variable length strings, the string
 *     is not considered truncated if only pad characters were omitted.
 *
 *     This function accepts a string <s>, its length <s_length>, and
 *     a count of characters <used_chars>.  If the remaining characters
 *     are all pad characters, then the function returns true value.
 *     A False value is returned otherwise.
 *
 */

static bool
varchar_truncated (const unsigned char *s,
		   DB_TYPE s_type,
		   int s_length, int used_chars, INTL_CODESET codeset)
{
  unsigned char pad[2];
  int pad_size = 0, trim_length, trim_size;
  int s_size;

  bool truncated = false;

  qstr_pad_char (codeset, pad, &pad_size);
  intl_char_size ((unsigned char *) s, s_length, codeset, &s_size);

  trim_trailing (pad, pad_size,
		 s, s_type, s_length, s_size, codeset,
		 &trim_length, &trim_size);

  if (trim_length > used_chars)
    {
      truncated = true;
    }

  return truncated;
}

/*
 * varbit_truncated () -
 *
 * Arguments:
 *            s:  (IN) Pointer to input string.
 *     s_length:  (IN) Length of input string.
 *    used_bits:  (IN) Number of characters which were used by caller.
 *                     0 <= <used_chars> <= <s_length>
 *
 * Returns:
 *
 * Errors:
 *
 * Note:
 *     This is a convinience function which is used by the concatenation
 *     function to determine if a variable length string has been
 *     truncated.  When concatenating variable length strings, the bit
 *     string is not considered truncated if only 0's were omitted.
 *
 *     This function accepts a string <s>, its length <s_length>, and
 *     a count of characters <used_chars>.  If the remaining characters
 *     are all 0's, then the function returns true value.  A False value
 *     is returned otherwise.
 *
 */

static bool
varbit_truncated (const unsigned char *s, int s_length, int used_bits)
{
  int last_set_bit;
  bool truncated = false;


  last_set_bit = bstring_fls ((char *) s, QSTR_NUM_BYTES (s_length));

  if (last_set_bit > used_bits)
    {
      truncated = true;
    }

  return truncated;
}

/*
 * bit_ncat () -
 *
 * Arguments:
 *            r: Pointer to bit string 1
 *       offset: Number of bits in string1
 *            s: Pointer to bit string 2
 *            n: Number of bits in string 2
 *
 * Returns: void
 *
 * Errors:
 *
 * Note:
 *   Shift the bits of <s> onto the end of <r>.  This is a helper
 *   function to str_bit_concatenate.  This function shifts
 *   (concatenates) exactly the number of bits specified into the result
 *   buffer which must be preallocated to the correct size.
 *
 */

static void
bit_ncat (unsigned char *r, int offset, const unsigned char *s, int n)
{
  int i, copy_size, cat_size, total_size;
  unsigned int remainder, shift_amount;
  unsigned short tmp_shifted;
  unsigned char mask;

  copy_size = QSTR_NUM_BYTES (offset);
  cat_size = QSTR_NUM_BYTES (n);
  total_size = QSTR_NUM_BYTES (offset + n);

  remainder = offset % BYTE_SIZE;

  if (remainder == 0)
    {
      memcpy ((char *) &r[copy_size], (char *) s, cat_size);
    }
  else
    {
      int start_byte = copy_size - 1;

      shift_amount = BYTE_SIZE - remainder;
      mask = 0xff << shift_amount;

      /*
       *  tmp_shifted is loaded with a byte from the source
       *  string and shifted into poition.  The upper byte is
       *  used for the current destination location, while the
       *  lower byte is used by the next destination location.
       */
      for (i = start_byte; i < total_size; i++)
	{
	  tmp_shifted = (unsigned short) (s[i - start_byte]);
	  tmp_shifted = tmp_shifted << shift_amount;
	  r[i] = (r[i] & mask) | (tmp_shifted >> BYTE_SIZE);

	  if (i < (total_size - 1))
	    {
	      r[i + 1] =
		(unsigned char) (tmp_shifted & (unsigned short) 0xff);
	    }
	}
    }

  /*  Mask out the unused bits */
  mask = 0xff << (BYTE_SIZE - ((offset + n) % BYTE_SIZE));
  if (mask != 0)
    {
      r[total_size - 1] &= mask;
    }
}

/*
 * bstring_fls () -
 *
 * Arguments:
 *            s: Pointer to source bit string
 *            n: Number of bits in string1
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *   Find the last set bit in the bit string.  The bits are numbered left
 *   to right starting at 1.  A value of 0 indicates that no set bits were
 *   found in the string.
 *
 */

static int
bstring_fls (const char *s, int n)
{
  int byte_num, bit_num, inter_bit_num;


  /*
   *  We are looking for the first non-zero byte (starting at the end).
   */
  byte_num = n - 1;
  while ((byte_num >= 0) && ((int) (s[byte_num]) == 0))
    {
      byte_num--;
    }

  /*
   *  If byte_num is < 0, then the string is all 0's.
   *  Othersize, byte_num is the index for the first byte which has
   *  some bits set (from the end).
   */
  if (byte_num < 0)
    {
      bit_num = 0;
    }
  else
    {
      inter_bit_num = (int) qstr_ffs ((int) (s[byte_num]));
      bit_num = (byte_num * BYTE_SIZE) + (BYTE_SIZE - inter_bit_num + 1);
    }

  return bit_num;
}

/*
 * qstr_bit_coerce () -
 *
 * Arguments:
 *        src_string:  (In) Source string
 *       dest_string: (Out) Coerced string
 *
 * Returns: DB_DATA_STATUS
 *
 * Errors:
 *
 * Note:
 *   This is a helper function which performs the actual coercion for
 *   bit strings.  It is called from db_bit_string_coerce().
 *
 *   If any loss of data due to truncation occurs DATA_STATUS_TRUNCATED
 *   is returned.
 *
 */

static int
qstr_bit_coerce (const unsigned char *src,
		 int src_length,
		 int src_precision,
		 DB_TYPE src_type,
		 unsigned char **dest,
		 int *dest_length,
		 int dest_precision,
		 DB_TYPE dest_type, DB_DATA_STATUS * data_status)
{
  int src_padded_length, copy_size, dest_size, copy_length;
  int error_status = NO_ERROR;

  *data_status = DATA_STATUS_OK;

  /*
   *  <src_padded_length> is the length of the fully padded
   *  source string.
   */
  if (QSTR_IS_FIXED_LENGTH (src_type))
    {
      src_padded_length = src_precision;
    }
  else
    {
      src_padded_length = src_length;
    }

  /*
   *  If there is not enough precision in the destination string,
   *  then some bits will be omited from the source string.
   */
  if (src_padded_length > dest_precision)
    {
      src_padded_length = dest_precision;
      *data_status = DATA_STATUS_TRUNCATED;
    }

  copy_length = MIN (src_length, src_padded_length);
  copy_size = QSTR_NUM_BYTES (copy_length);

  /*
   *  For fixed-length destination strings...
   *    Allocate the destination precision size, copy the source
   *    string and pad the rest.
   *
   *  For variable-length destination strings...
   *    Allocate enough for a fully padded source string, copy
   *    the source string and pad the rest.
   */
  if (QSTR_IS_FIXED_LENGTH (dest_type))
    {
      *dest_length = dest_precision;
    }
  else
    {
      *dest_length = MIN (src_padded_length, dest_precision);
    }

  dest_size = QSTR_NUM_BYTES (*dest_length);

  *dest = (unsigned char *) db_private_alloc (NULL, dest_size + 1);
  if (*dest == NULL)
    {
      error_status = er_errid ();
    }
  else
    {
      bit_ncat (*dest, 0, src, copy_length);
      (void) memset ((char *) &((*dest)[copy_size]),
		     (int) 0, (dest_size - copy_size));
    }

  return error_status;
}

/*
 * qstr_coerce () -
 *
 * Arguments:
 *        src_string:  (In) Source string
 *       dest_string: (Out) Coerced string
 *
 * Returns: DB_DATA_STATUS
 *
 * Errors:
 *
 * Note:
 *   This is a helper function which performs the actual coercion for
 *   character strings.  It is called from db_char_string_coerce().
 *
 *   If any loss of data due to truncation occurs DATA_STATUS_TRUNCATED
 *   is returned.
 *
 */

static int
qstr_coerce (const unsigned char *src,
	     int src_length,
	     int src_precision,
	     DB_TYPE src_type,
	     INTL_CODESET codeset,
	     unsigned char **dest,
	     int *dest_length,
	     int *dest_size,
	     int dest_precision,
	     DB_TYPE dest_type, DB_DATA_STATUS * data_status)
{
  int src_padded_length, copy_length, copy_size;
  int alloc_size;
  char *end_of_string;
  int error_status = NO_ERROR;

  *data_status = DATA_STATUS_OK;
  *dest_size = 0;

  /*
   *  <src_padded_length> is the length of the fully padded
   *  source string.
   */
  if (QSTR_IS_FIXED_LENGTH (src_type))
    {
      src_padded_length = src_precision;
    }
  else
    {
      src_padded_length = src_length;
    }

  /*
   *  Some characters will be truncated if there is not enough
   *  precision in the destination string.  If any of the
   *  truncated characters are non-pad characters, a truncation
   *  exception is raised.
   */
  if (src_padded_length > dest_precision)
    {
      src_padded_length = dest_precision;
      if ((src_length > src_padded_length) &&
	  (varchar_truncated (src, src_type, src_length,
			      src_padded_length, codeset)))
	{
	  *data_status = DATA_STATUS_TRUNCATED;
	}
    }

  copy_length = MIN (src_length, src_padded_length);
  intl_char_size ((unsigned char *) src, copy_length, codeset, &copy_size);


  /*
   *  For fixed-length destination strings...
   *    Allocate the destination precision size, copy the source
   *    string and pad the rest.
   *
   *  For variable-length destination strings...
   *    Allocate enough for a fully padded source string, copy
   *    the source string and pad the rest.
   */
  if (QSTR_IS_FIXED_LENGTH (dest_type))
    {
      *dest_length = dest_precision;
    }
  else
    {
      *dest_length = src_padded_length;
    }

  /*
   *  National character strings can consume on or two bytes per
   *  character, so we allocate twice the character length to
   *  be safe.
   */
  if (QSTR_IS_NATIONAL_CHAR (dest_type))
    {
      alloc_size = 2 * (*dest_length);
    }
  else
    {
      alloc_size = *dest_length;
    }

  if (!alloc_size)
    {
      alloc_size = 1;
    }

  *dest = (unsigned char *) db_private_alloc (NULL, alloc_size + 1);
  if (*dest == NULL)
    {
      error_status = er_errid ();
    }
  else
    {
      (void) memcpy ((char *) *dest, (char *) src, (int) copy_size);
      end_of_string = (char *) qstr_pad_string ((unsigned char *)
						&((*dest)[copy_size]),
						(*dest_length - copy_length),
						codeset);
      *dest_size = CAST_STRLEN (end_of_string - (char *) (*dest));
    }

  return error_status;
}

/*
 * qstr_position () -
 *
 * Arguments:
 *        sub_string: String fragment to search for within <src_string>.
 *        sub_length: Number of characters in sub_string.
 *        src_string: String to be searched.
 *        src_length: Number of characters in src_string.
 * is_forward_search: forward search or backward search.
 *           codeset: Codeset of strings.
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     This function accepts a source string <src_sring> and a string
 *     string fragment <sub_string> and returns the character position
 *     corresponding to the first occurance of <sub_string> within
 *     <src_string>.
 *
 *     This function works with National character strings.
 *
 */

static int
qstr_position (const char *sub_string,
	       int sub_length,
	       const char *src_string,
	       int src_length,
	       INTL_CODESET codeset, bool is_forward_search, int *position)
{
  int error_status = NO_ERROR;
  *position = 0;

  if (sub_length == 0)
    {
      *position = 1;
    }
  else if (sub_length > src_length)
    {
      *position = 0;
    }
  else
    {
      int i, num_searches, current_position, result;
      unsigned char *ptr;
      int sub_size, char_size;

      /*
       *  Since the entire sub-string must be matched, a reduced
       *  number of compares <num_searches> are needed.  A binary
       *  comparison of <sub_size> bytes will be used.
       */
      num_searches = src_length - sub_length + 1;
      intl_char_size ((unsigned char *) sub_string,
		      sub_length, codeset, &sub_size);

      /*
       *  Starting at the first position of the string, match the
       *  sub-string to the source string.  If a match is not found,
       *  then increment into the source string by one character and
       *  try again.  This is repeated until a match is found, or
       *  there are no more comparisons to be made.
       */
      ptr = (unsigned char *) src_string;
      current_position = 0;
      result = 1;

      for (i = 0; ((i < num_searches) && (result != 0)); i++)
	{
	  result = memcmp (sub_string, (char *) ptr, sub_size);
	  if (is_forward_search)
	    {
	      ptr = qstr_next_char ((unsigned char *) ptr,
				    codeset, &char_size);
	    }
	  else
	    {			/* backward search */
	      ptr = qstr_prev_char ((unsigned char *) ptr,
				    codeset, &char_size);
	    }
	  current_position++;
	}

      /*
       *  Return the position of the match, if found.
       */
      if (result == 0)
	{
	  *position = current_position;
	}
    }

  return error_status;
}

/*
 * qstr_bit_position () -
 *
 * Arguments:
 *      sub_string: String fragment to search for within <src_string>.
 *      sub_length: Number of characters in sub_string.
 *      src_string: String to be searched.
 *      src_length: Number of characters in src_string.
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     This function accepts a source string <src_sring> and a string
 *     string fragment <sub_string> and returns the bit position
 *     corresponding to the first occurance of <sub_string> within
 *     <src_string>.
 *
 */

static int
qstr_bit_position (const unsigned char *sub_string,
		   int sub_length,
		   const unsigned char *src_string,
		   int src_length, int *position)
{
  int error_status = NO_ERROR;

  *position = 0;

  if (sub_length == 0)
    {
      *position = 1;
    }

  else if (sub_length > src_length)
    {
      *position = 0;
    }

  else
    {
      int i, num_searches, result;
      int sub_size, sub_remainder, shift_amount;
      unsigned char *ptr, *tmp_string, tmp_byte, mask;

      num_searches = src_length - sub_length + 1;
      sub_size = QSTR_NUM_BYTES (sub_length);

      sub_remainder = sub_length % BYTE_SIZE;
      shift_amount = BYTE_SIZE - sub_remainder;
      mask = 0xff << shift_amount;

      /*
       *  We will be manipulating the source string prior to
       *  comparison.  So that we do not corrupt the source string,
       *  we'll allocate a storage area so that we can make a copy
       *  of the string.  This copy need only be he length of the
       *  sub-string since that is the limit of the comparison.
       */
      tmp_string = (unsigned char *) db_private_alloc (NULL,
						       (size_t) sub_size + 1);
      if (tmp_string == NULL)
	{
	  error_status = er_errid ();
	}
      else
	{
	  ptr = (unsigned char *) src_string;

	  /*
	   *  Make a copy of the source string.
	   *  Initialize the bit index.
	   */
	  (void) memcpy ((char *) tmp_string, (char *) ptr, sub_size);
	  i = 0;
	  result = 1;

	  while ((i < num_searches) && (result != 0))
	    {
	      /* Pad the irrelevant bits of the source string with 0's */
	      tmp_byte = tmp_string[sub_size - 1];
	      tmp_string[sub_size - 1] &= mask;

	      /* Compare the source string with the sub-string */
	      result = memcmp (sub_string, tmp_string, sub_size);

	      /* Restore the padded byte to its original value */
	      tmp_string[sub_size - 1] = tmp_byte;

	      /* Shift the copied source string left one bit */
	      (void) shift_left (tmp_string, sub_size);

	      i++;

	      /*
	       *  Every time we hit a byte boundary,
	       *  Move on to the next byte of the source string.
	       */
	      if ((i % BYTE_SIZE) == 0)
		{
		  ptr++;
		  memcpy (tmp_string, ptr, sub_size);
		}
	    }


	  db_private_free_and_init (NULL, tmp_string);

	  /*
	   *  If a match was found, then return the position
	   *  of the match.
	   */
	  if (result == 0)
	    {
	      *position = i;
	    }
	}
    }

  return error_status;
}

/*
 * shift_left () -
 *
 * Arguments:
 *             bit_string: Byte array representing a bit string.
 *        bit_string_size: Number of bytes in the array.
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     Shift the bit string left one bit.  The left most bit is shifted out
 *     and returned.  A 0 is inserted into the rightmost bit position.
 *     The entire array is shifted regardless of the number of significant
 *     bits in the array.
 *
 */

static int
shift_left (unsigned char *bit_string, int bit_string_size)
{
  int i, highest_bit;


  highest_bit = ((bit_string[0] & 0x80) != 0);
  bit_string[0] = bit_string[0] << 1;

  for (i = 1; i < bit_string_size; i++)
    {
      if (bit_string[i] & 0x80)
	{
	  bit_string[i - 1] |= 0x01;
	}

      bit_string[i] = bit_string[i] << 1;
    }

  return highest_bit;
}

/*
 * qstr_substring () -
 *
 * Arguments:
 *             src_string: Source string.
 *         start_position: Starting character position of sub-string.
 *      extraction_length: Length of sub-string.
 *             sub_string: Returned sub-string.
 *
 * Returns: void
 *
 * Errors:
 *
 * Note:
 *     Extract the sub-string from the source string.  The sub-string is
 *     specified by a starting position and length.
 *
 *     This functions works on character and national character strings.
 *
 */

static int
qstr_substring (const unsigned char *src,
		int src_length,
		int start,
		int length,
		INTL_CODESET codeset,
		unsigned char **r, int *r_length, int *r_size)
{
  int error_status = NO_ERROR;
  const unsigned char *sub;
  int src_size, leading_bytes;
  *r_size = 0;

  /* Get the size of the source string. */
  intl_char_size ((unsigned char *) src, src_length, codeset, &src_size);

  /*
   * Perform some error chaecking.
   * If the starting position is < 1, then set it to 1.
   * If the starting position is after the end of the source string,
   * then set the sub-string length to 0.
   * If the sub-string length will extend beyond the end of the source string,
   * then shorten the sub-string length to fit.
   */
  if (start < 1)
    {
      start = 1;
    }

  if (start > src_length)
    {
      start = 1;
      length = 0;
    }

  if ((length < 0) || ((start + length - 1) > src_length))
    {
      length = src_length - start + 1;
    }

  *r_length = length;

  /*
   *  Get a pointer to the start of the sub-string and the
   *  size of the sub-string.
   *
   *  Compute the starting byte of the sub-string.
   *  Compute the length of the sub-string in bytes.
   */
  intl_char_size ((unsigned char *) src, (start - 1), codeset,
		  &leading_bytes);
  sub = &(src[leading_bytes]);
  intl_char_size ((unsigned char *) sub, *r_length, codeset, r_size);

  *r = (unsigned char *) db_private_alloc (NULL, (size_t) ((*r_size) + 1));
  if (*r == NULL)
    {
      error_status = er_errid ();
    }
  else
    {
      (void) memcpy (*r, sub, *r_size);
    }

  return error_status;
}

/*
 * qstr_bit_substring () -
 *
 * Arguments:
 *             src_string: Source string.
 *         start_position: Starting character position of sub-string.
 *      extraction_length: Length of sub-string.
 *             sub_string: Returned sub-string.
 *
 * Returns: void
 *
 * Errors:
 *
 * Note:
 *     Extract the sub-string from the source string.  The sub-string is
 *     specified by a starting position and length.
 *
 *     This functions works on bit strings.
 *
 */

static int
qstr_bit_substring (const unsigned char *src,
		    int src_length,
		    int start, int length, unsigned char **r, int *r_length)
{
  int src_size, sub_size, rem;
  unsigned char trailing_mask;
  int error_status = NO_ERROR;

  src_size = QSTR_NUM_BYTES (src_length);

  /*
   *  Perform some error checking.
   *  If the starting position is < 1, then set it to 1.
   *  If the starting position is after the end of the source
   *    string, then set the sub-string length to 0.
   *  If the sub-string length will extend beyond the end of the
   *    source string, then shorten the sub-string length to fit.
   */
  if (start < 1)
    {
      start = 1;
    }

  if (start > src_length)
    {
      start = 1;
      length = 0;
    }

  if ((length < 0) || ((start + length - 1) > src_length))
    {
      length = src_length - start + 1;
    }

  sub_size = QSTR_NUM_BYTES (length);
  *r_length = length;

  rem = length % BYTE_SIZE;
  if (rem == 0)
    {
      trailing_mask = 0xff;
    }
  else
    {
      trailing_mask = 0xff << (BYTE_SIZE - rem);
    }

  /*
   *  Allocate storage for the sub-string.
   *  Copy the sub-string.
   */
  *r = (unsigned char *) db_private_alloc (NULL, (size_t) sub_size + 1);
  if (*r == NULL)
    {
      error_status = er_errid ();
    }
  else
    {
      left_nshift (src, src_size, (start - 1), *r, sub_size);
      (*r)[sub_size - 1] &= trailing_mask;
    }

  return error_status;
}

/*
 * left_nshift () -
 *
 * Arguments:
 *             bit_string: Byte array containing the bit string.
 *        bit_string_size: Size of the bit array in bytes.
 *           shift_amount: Number of bit positions to shift by.
 *                             range: 0 <= shift_amount
 *                      r: Pointer to result buffer where the shifted bit
 *                         array will be stored.
 *                 r_size: Size of the result array in bytes.
 *
 * Returns: void
 *
 * Errors:
 *
 * Note:
 *     Shift the bit string left <shift_amount> bits.  The left most bits
 *     are shifted out.  0's are inserted into the rightmost bit positions.
 *     The entire array is shifted regardless of the number of significant
 *     bits in the array.
 *
 */

static void
left_nshift (const unsigned char *bit_string,
	     int bit_string_size,
	     int shift_amount, unsigned char *r, int r_size)
{
  int i, shift_bytes, shift_bits, adj_bit_string_size;
  const unsigned char *ptr;

  shift_bytes = shift_amount / BYTE_SIZE;
  shift_bits = shift_amount % BYTE_SIZE;
  ptr = &(bit_string[shift_bytes]);

  adj_bit_string_size = bit_string_size - shift_bytes;

  for (i = 0; i < r_size; i++)
    {
      if (i < (adj_bit_string_size - 1))
	{
	  r[i] = ((ptr[i] << shift_bits) |
		  (ptr[i + 1] >> (BYTE_SIZE - shift_bits)));
	}
      else if (i == (adj_bit_string_size - 1))
	{
	  r[i] = (ptr[i] << shift_bits);
	}
      else
	{
	  r[i] = 0;
	}
    }
}

/*
 *  The version below handles multibyte character sets by promoting all
 *  characters to two bytes each.  Unfortunately, the current implementation
 *  of the regular expression package has some limitations with characters
 *  that are not char sized.  The above version works with char sized
 *  sets only and therefore will not work with national character sets.
 */

/*
 * qstr_ffs () -
 *   Returns: int
 *   v: (IN) Source string.
 *
 *
 * Errors:
 *
 * Note:
 *     Finds the first bit set in the passed argument and returns
 *     the index of that bit.  Bits are numbered starting at 1
 *     from the right.  A return value of 0 indicates that the value
 *     passed is zero.
 *
 */

static int
qstr_ffs (int v)
{
  int nbits;

  int i = 0;
  int position = 0;
  unsigned int uv = (unsigned int) v;

  nbits = sizeof (int) * 8;

  if (uv != 0)
    {
      while ((i < nbits) && (position == 0))
	{
	  if (uv & 0x01)
	    {
	      position = i + 1;
	    }

	  i++;
	  uv >>= 1;
	}
    }

  return position;
}

/*
 * hextoi () -
 *
 * Arguments:
 *             hex_char: (IN) Character containing ASCII hex character
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     Returns the decimal value associated with the ASCII hex character.
 *     Will return a -1 if hex_char is not a hexadecimal ASCII character.
 *
 */

static int
hextoi (char hex_char)
{
  if ((hex_char >= '0') && (hex_char <= '9'))
    {
      return (hex_char - '0');
    }
  else if ((hex_char >= 'A') && (hex_char <= 'F'))
    {
      return (hex_char - 'A' + 10);
    }
  else if ((hex_char >= 'a') && (hex_char <= 'f'))
    {
      return (hex_char - 'a' + 10);
    }
  else
    {
      return (-1);
    }
}

/*
 * set_time_argument() - construct struct tm
 *   return:
 *   dest(out):
 *   year(in):
 *   month(in):
 *   day(in):
 *   hour(in):
 *   min(in):
 *   sec(in):
 */
static void
set_time_argument (struct tm *dest, int year, int month, int day,
		   int hour, int min, int sec)
{
  if (year >= 1900)
    {
      dest->tm_year = year - 1900;
    }
  else
    {
      dest->tm_year = -1;
    }
  dest->tm_mon = month - 1;
  dest->tm_mday = day;
  dest->tm_hour = hour;
  dest->tm_min = min;
  dest->tm_sec = sec;
  dest->tm_isdst = -1;
}

/*
 * calc_unix_timestamp() - calculates UNIX timestamp
 *   return:
 *   time_argument(in):
 */
static long
calc_unix_timestamp (struct tm *time_argument)
{
  time_t result;

  if (time_argument != NULL)
    {
      /* validation for tm fields in order to cover for mktime conversion's
         like 40th of Sept equals 10th of Oct */
      if (time_argument->tm_year < 0 || time_argument->tm_year > 9999
	  || time_argument->tm_mon < 0 || time_argument->tm_mon > 11
	  || time_argument->tm_mday < 1 || time_argument->tm_mday > 31
	  || time_argument->tm_hour < 0 || time_argument->tm_hour > 23
	  || time_argument->tm_min < 0 || time_argument->tm_min > 59
	  || time_argument->tm_sec < 0 || time_argument->tm_sec > 59)
	{
	  return -1L;
	}
      result = mktime (time_argument);
    }
  else
    {
      result = time (NULL);
    }

  if (result < (time_t) 0)
    {
      return -1L;
    }
  return (long) result;
}

/*
 * parse_for_next_int () -
 *
 * Arguments:
 *         ch: char position from which we start parsing
 *	   output: integer read
 *
 * Returns: -1 if error, 0 if success
 *
 * Note:
 *  parses a string for integers while skipping non-alpha delimitators
 */
static int
parse_for_next_int (char **ch, char *output)
{
  int i;
  /* we need in fact only 6 (upper bound for the integers we want) */
  char buf[16];

  i = 0;
  memset (buf, 0, sizeof (buf));

  /* trailing zeroes - accept only 2 (for year 00 which is short for 2000) */
  while (**ch == '0')
    {
      if (i < 2)
	{
	  buf[i++] = **ch;
	}
      (*ch)++;
    }

  while (i < 6 && char_isdigit (**ch) && **ch != 0)
    {
      buf[i++] = **ch;
      (*ch)++;
    }
  if (i > 6)
    {
      return -1;
    }
  strcpy (output, buf);

  /* skip all delimitators */
  while (**ch != 0 && !char_isalpha (**ch) && !char_isdigit (**ch))
    {
      (*ch)++;
    }
  return 0;
}

/*
 * db_unix_timestamp () -
 *
 * Arguments:
 *         src_date: datetime from which we calculate timestamp
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 * Returns a Unix timestamp (seconds since '1970-01-01 00:00:00' UTC)
 */
int
db_unix_timestamp (const DB_VALUE * src_date, DB_VALUE * result_timestamp)
{
  DB_TYPE type;
  int error_status = NO_ERROR;
  int val;
  char *str_input = NULL;
  double d_time;
  char str_time[32];
  char str_year[32], str_month[32], str_day[32];
  char str_hour[32], str_min[32], str_sec[32];
  struct tm time_argument;
  char *cp = NULL;
  int nb_arg = 0;
  char *ch;
  int i, n, only_digits = 1;
  /* long_year 1 -> year on 4 chars, long_year 0 -> year on 2 chars */
  int long_year = 0;
  int y, m, d;
  int days[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  time_t ts;
  int month = 0, day = 0, year = 0;
  int second = 0, minute = 0, hour = 0, ms = 0;

  memset (str_year, '\0', sizeof (str_year));
  memset (str_month, '\0', sizeof (str_month));
  memset (str_day, '\0', sizeof (str_day));
  memset (str_hour, '\0', sizeof (str_hour));
  memset (str_min, '\0', sizeof (str_min));
  memset (str_sec, '\0', sizeof (str_sec));

  if (DB_IS_NULL (src_date))
    {
      DB_MAKE_NULL (result_timestamp);
      return error_status;
    }

  type = DB_VALUE_DOMAIN_TYPE (src_date);
  switch (type)
    {
      /* a number in the format YYMMDD or YYYYMMDD */
    case DB_TYPE_BIGINT:
      d_time = (double) DB_GET_BIGINT (src_date);
      sprintf (str_time, "%.0f", d_time);
      cp = str_time;
      break;

    case DB_TYPE_INTEGER:
      d_time = (double) DB_GET_INT (src_date);
      sprintf (str_time, "%.0f", d_time);
      cp = str_time;
      break;

      /* a DATE or DATETIME string */
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
      cp = DB_GET_STRING (src_date);
      if (cp == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return ER_FAILED;
	}
      break;

      /* a TIMESTAMP format */
    case DB_TYPE_TIMESTAMP:
      /* The supported timestamp range is '1970-01-01 00:00:01'
       * UTC to '2038-01-19 03:14:07' UTC */
      ts = *DB_GET_TIMESTAMP (src_date);
      /* supplementary conversion from long to int will be needed on
       * 64 bit platforms.  */
      val = (int) ts;
      DB_MAKE_INT (result_timestamp, val);
      return NO_ERROR;

      /* a DATETIME format */
    case DB_TYPE_DATETIME:
      /* The supported datetime range is '1970-01-01 00:00:01'
       * UTC to '2038-01-19 03:14:07' UTC */

      error_status = db_datetime_decode (DB_GET_DATETIME (src_date),
					 &month, &day, &year, &hour,
					 &minute, &second, &ms);
      if (error_status != NO_ERROR)
	{
	  return error_status;
	}
      set_time_argument (&time_argument, year, month, day, hour,
			 minute, second);
      val = (int) calc_unix_timestamp (&time_argument);
      if (val < 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return ER_FAILED;
	}
      DB_MAKE_INT (result_timestamp, val);
      return NO_ERROR;

      /* a DATE format */
    case DB_TYPE_DATE:
      /* The supported datetime range is '1970-01-01 00:00:01'
       * UTC to '2038-01-19 03:14:07' UTC */

      db_date_decode (DB_GET_DATE (src_date), &month, &day, &year);

      set_time_argument (&time_argument, year, month, day, hour,
			 minute, second);
      val = (int) calc_unix_timestamp (&time_argument);
      if (val < 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return ER_FAILED;
	}
      DB_MAKE_INT (result_timestamp, val);
      return NO_ERROR;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      return ER_FAILED;
    }

  assert (cp != NULL);

  /* allocate at least 32 bytes because we will fill with some zeroes */
  str_input = (char *) db_private_alloc (NULL, MAX (strlen (cp) + 1, 32));
  if (str_input == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 0);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* analyze the string */
  /* trim for beginning spaces only (like mysql) */
  ch = cp;
  while (*ch != 0 && *ch == ' ')
    {
      ch++;
    }
  strcpy (str_input, ch);

  /* mysql yields error if non-digit is the first character, after trim */
  if (!char_isdigit (str_input[0]))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      goto final;
    }

  /*
   * now test if we have only digits, if so we are in case 1 with inputs
   * like YYMMDD, etc (which does not have delimitators)
   */
  n = strlen (str_input);
  for (i = 0; i < n; i++)
    {
      if (!char_isdigit (str_input[i]))
	{
	  only_digits = 0;
	  break;
	}
    }
  if (only_digits == 1)
    {
      /*
       * if we have only digits we identified the following formats
       *  based on many mysql tests:
       *    len <  5 -> 0
       *    len =  5 -> YYMMD
       *    len =  6 -> YYMMDD
       *    len =  7 -> YYMMDDH
       *    len =  8 -> YYYYMMDD
       *    len =  9 -> YYMMDDHHM
       *    len = 10 -> YYMMDDHHMM
       *    len = 11 -> YYMMDDHHMMS
       *    len = 12 -> YYMMDDHHMMSS
       *    len = 13 -> YYMMDDHHMMSSX -> X is not used
       *    len = 14 -> YYYYMMDDHHMMSS
       *    len > 14 -> same as len = 14, all after the 14th is skipped
       */
      if (n < 5)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
		  0);
	  goto final;
	}
      if (n == 8 || n >= 14)
	{
	  long_year = 1;
	}
      if (n == 13)
	{
	  /* cut last X, it's useless */
	  str_input[n - 1] = 0;
	  n--;
	}

      /* when odd, the last unit ('X') is incomplete (has just one digit)
       * so we replace it with '0X' so that further we can add zeroes.
       * (ex: YYMMD->YYMM0D, YYMMDDHHMMS->YYMMDDHHMM0S)
       */
      if (n & 1)
	{
	  str_input[n] = str_input[n - 1];
	  str_input[n - 1] = '0';
	  str_input[n + 1] = 0;
	  n++;
	}
      /*
       * fill with zeroes:
       *  when YYYY we fill until we are in the YYYYMMDDHHMMSS format
       *  when YY we fill until we are in the YYMMDDHHMMSS format
       */
      for (; n < (long_year ? 14 : 12); n++)
	{
	  str_input[n] = '0';
	}
      str_input[n] = 0;
      n = strlen (str_input);
      ch = str_input;

      /* years */
      strncpy (str_year, ch, long_year ? 4 : 2);
      ch += (long_year ? 4 : 2);

      /* months */
      strncpy (str_month, ch, 2);
      ch += 2;

      /* days */
      strncpy (str_day, ch, 2);
      ch += 2;

      /* hours */
      strncpy (str_hour, ch, 2);
      ch += 2;

      /* minutes */
      strncpy (str_min, ch, 2);
      ch += 2;

      /* seconds */
      strncpy (str_sec, ch, 2);
      ch += 2;
    }
  else if (only_digits == 0)
    {
      /* the case where the input can contain some delimitators
       * on mysql all non-alphabetic characters are permitted
       * the big picture of this case is the following:
       *  - [digits][non-digits and non-letters] x6
       *  - if the digits are more than 2 (4 for year), we return 0
       *  - after filling all 6 fields, we ignore the rest (which may
       *      contain 'a'..'z'!)
       *
       */
      ch = str_input;

      /* years */
      if (parse_for_next_int (&ch, str_year) < 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
		  0);
	  goto final;
	}

      /* months */
      if (parse_for_next_int (&ch, str_month) < 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
		  0);
	  goto final;
	}

      /* days */
      if (parse_for_next_int (&ch, str_day) < 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
		  0);
	  goto final;
	}

      /* hours */
      if (parse_for_next_int (&ch, str_hour) < 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
		  0);
	  goto final;
	}

      /* minutes */
      if (parse_for_next_int (&ch, str_min) < 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
		  0);
	  goto final;
	}

      /* seconds */
      if (parse_for_next_int (&ch, str_sec) < 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
		  0);
	  goto final;
	}
    }

  /* validations */
  /* year */
  y = atoi (str_year);
  if (strlen (str_year) == 2)
    {
      if (y >= 70)
	{
	  y += 1900;
	}
      else
	{
	  y += 2000;
	}
    }
  if (y < 1970 || y > 2038)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      goto final;
    }

  /* month */
  m = atoi (str_month);
  if (m <= 0 || m > 12)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      goto final;
    }

  /* day */
  d = atoi (str_day);
  days[2] = LEAP (y) ? 29 : 28;
  if (d <= 0 || d > days[m])
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      goto final;
    }

  /* the rest of validations are made in calc_unix_timestamp */

  /* now we have all needed data, compute final answer */
  set_time_argument (&time_argument, y, m, d,
		     (str_hour ? atoi (str_hour) : 0),
		     (str_min ? atoi (str_min) : 0),
		     (str_sec ? atoi (str_sec) : 0));
  val = (int) calc_unix_timestamp (&time_argument);
  if (val < 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }
  DB_MAKE_INT (result_timestamp, val);

final:
  if (str_input)
    {
      db_private_free (NULL, str_input);
    }
  return er_errid ();
}

/*
 * db_time_format ()
 *
 * Arguments:
 *         time_value: time from which we get the informations
 *         format: format specifiers string
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     This is used like the DATE_FORMAT() function, but the format
 *  string may contain format specifiers only for hours, minutes, seconds, and
 *  milliseconds. Other specifiers produce a NULL value or 0.
 */
int
db_time_format (const DB_VALUE * time_value, const DB_VALUE * format,
		DB_VALUE * result)
{
  DB_TIME db_time, *t_p;
  DB_TIMESTAMP db_timestamp, *ts_p;
  DB_DATETIME db_datetime, *dt_p;
  DB_DATE db_date;
  DB_TYPE res_type;
  char *date_s = NULL, *res, *res2, *format_s;
  int error_status = NO_ERROR, len;
  int h, mi, s, ms, year, month, day;
  char format_specifiers[256][64];
  int is_date, is_datetime, is_timestamp, is_time;
  char och = -1, ch;

  is_date = is_datetime = is_timestamp = is_time = 0;
  h = mi = s = ms = 0;
  memset (format_specifiers, 0, sizeof (format_specifiers));

  if (time_value == NULL || format == NULL
      || DB_IS_NULL (time_value) || DB_IS_NULL (format))
    {
      DB_MAKE_NULL (result);
      goto error;
    }

  res_type = DB_VALUE_DOMAIN_TYPE (time_value);

  /* 1. Get date values */
  switch (res_type)
    {
    case DB_TYPE_TIMESTAMP:
      ts_p = DB_GET_TIMESTAMP (time_value);
      db_timestamp_decode (ts_p, &db_date, &db_time);
      db_time_decode (&db_time, &h, &mi, &s);
      break;

    case DB_TYPE_DATETIME:
      dt_p = DB_GET_DATETIME (time_value);
      db_datetime_decode (dt_p, &month, &day, &year, &h, &mi, &s, &ms);
      break;

    case DB_TYPE_TIME:
      t_p = DB_GET_TIME (time_value);
      db_time_decode (t_p, &h, &mi, &s);
      break;

    case DB_TYPE_STRING:
    case DB_TYPE_CHAR:
      date_s = DB_GET_STRING (time_value);

      is_date = db_string_to_date (date_s, &db_date);
      is_datetime = db_string_to_datetime (date_s, &db_datetime);
      is_timestamp = db_string_to_timestamp (date_s, &db_timestamp);
      is_time = db_string_to_time (date_s, &db_time);

      /* if we have a date return error */
      if (is_date != ER_DATE_CONVERSION)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}
      /* we have a datetime? */
      else if (is_datetime != ER_DATE_CONVERSION)
	{
	  db_datetime_decode (&db_datetime, &month, &day, &year,
			      &h, &mi, &s, &ms);
	}
      /* ... or maybe we have a timestamp? */
      else if (is_timestamp != ER_DATE_CONVERSION)
	{
	  db_timestamp_decode (&db_timestamp, &db_date, &db_time);

	  db_time_decode (&db_time, &h, &mi, &s);
	}
      /* ... a time? */
      /* if it's a time we do not need to decode */
      else
	{
	  sscanf (date_s, "%d:%d:%d.%d", &h, &mi, &s, &ms);
	}

      break;

    default:
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }

  /* 2. Compute the value for each format specifier */
  if (mi < 0)
    {
      mi = -mi;
    }
  if (s < 0)
    {
      s = -s;
    }
  if (ms < 0)
    {
      ms = -ms;
    }

  /* %f       Milliseconds (000..999) */
  sprintf (format_specifiers['f'], "%03d", ms);

  /* %H       Hour (00..23) */
  if (h < 0)
    {
      sprintf (format_specifiers['H'], "-%02d", -h);
    }
  else
    {
      sprintf (format_specifiers['H'], "%02d", h);
    }
  if (h < 0)
    {
      h = -h;
    }

  /* %h       Hour (01..12) */
  sprintf (format_specifiers['h'], "%02d", (h % 12 == 0) ? 12 : (h % 12));

  /* %I       Hour (01..12) */
  sprintf (format_specifiers['I'], "%02d", (h % 12 == 0) ? 12 : (h % 12));

  /* %i       Minutes, numeric (00..59) */
  sprintf (format_specifiers['i'], "%02d", mi);

  /* %k       Hour (0..23) */
  sprintf (format_specifiers['k'], "%d", h);

  /* %l       Hour (1..12) */
  sprintf (format_specifiers['l'], "%d", (h % 12 == 0) ? 12 : (h % 12));

  /* %p       AM or PM */
  strcpy (format_specifiers['p'], (h > 11) ? "PM" : "AM");

  /* %r       Time, 12-hour (hh:mm:ss followed by AM or PM) */
  sprintf (format_specifiers['r'], "%02d:%02d:%02d %s",
	   (h % 12 == 0) ? 12 : (h % 12), mi, s, (h > 11) ? "PM" : "AM");

  /* %S       Seconds (00..59) */
  sprintf (format_specifiers['S'], "%02d", s);

  /* %s       Seconds (00..59) */
  sprintf (format_specifiers['s'], "%02d", s);

  /* %T       Time, 24-hour (hh:mm:ss) */
  sprintf (format_specifiers['T'], "%02d:%02d:%02d", h, mi, s);

  /* 3. Generate the output according to the format and the values */
  format_s = DB_PULL_STRING (format);
  len = 1024;

  res = (char *) db_private_alloc (NULL, len);
  if (!res)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error;
    }
  memset (res, 0, len);

  ch = *format_s;
  while (ch != 0)
    {
      format_s++;
      och = ch;
      ch = *format_s;

      if (och == '%' /* && (res[strlen(res) - 1] != '%') */ )
	{
	  if (ch == '%')
	    {
	      STRCHCAT (res, '%');

	      /* jump a character */
	      format_s++;
	      och = ch;
	      ch = *format_s;

	      continue;
	    }
	  /* parse the character */
	  if (strlen (format_specifiers[(unsigned char) ch]) == 0)
	    {
	      /* append the character itself */
	      STRCHCAT (res, ch);
	    }
	  else
	    {
	      strcat (res, format_specifiers[(unsigned char) ch]);
	    }

	  /* jump a character */
	  format_s++;
	  och = ch;
	  ch = *format_s;
	}
      else
	{
	  STRCHCAT (res, och);
	}

      /* chance of overflow ? */
      /* assume we can't add at a time mode than 16 chars */
      if (strlen (res) + 16 > len)
	{
	  /* realloc - copy temporary in res2 */
	  res2 = (char *) db_private_alloc (NULL, len);
	  if (!res2)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  memset (res2, 0, len);
	  strcpy (res2, res);
	  db_private_free (NULL, res);

	  len += 1024;
	  res = (char *) db_private_alloc (NULL, len);
	  if (!res)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  memset (res, 0, len);
	  strcpy (res, res2);
	  db_private_free (NULL, res2);
	}
    }
  /* finished string */

  /* 4. */
  DB_MAKE_STRING (result, res);

error:
  /* do not free res as it was moved to result and will be freed later */
  return error_status;
}

/*
 * db_timestamp() -
 *
 * Arguments:
 *         src_datetime1: date or datetime expression
 *         src_time2: time expression
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 * This function is used in the function TIMESTAMP().
 * It returns the date or datetime expression expr as a datetime value.
 * With both arguments, it adds the time expression src_time2 to the date or
 * datetime expression src_datetime1 and returns the result as a datetime value.
 */
int
db_timestamp (const DB_VALUE * src_datetime1, const DB_VALUE * src_time2,
	      DB_VALUE * result_datetime)
{
  int error_status = NO_ERROR;
  int year, month, day, hour, minute, second, millisecond;
  int y = 0, m = 0, d = 0, h = 0, mi = 0, s = 0, ms = 0;
  DB_BIGINT amount = 0;
  double amount_d = 0;
  DB_TYPE type;
  DB_DATE db_date;
  DB_TIME db_time;
  DB_DATETIME datetime, db_datetime, calculated_datetime;
  /* if sign is 1 then we perform a subtraction */
  int sign = 0;

  if (result_datetime == (DB_VALUE *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_PARAMETER,
	      0);
      return ER_QPROC_INVALID_PARAMETER;
    }

  db_value_domain_init (result_datetime, DB_TYPE_DATETIME, 0, 0);

  /* Return NULL if NULL is explicitly given as the second argument.
   * If no second argument is given, we consider it as 0.
   */
  if (DB_IS_NULL (src_datetime1) || (src_time2 && DB_IS_NULL (src_time2)))
    {
      DB_MAKE_NULL (result_datetime);
      return NO_ERROR;
    }

  year = month = day = hour = minute = second = millisecond = 0;

  type = DB_VALUE_DOMAIN_TYPE (src_datetime1);
  switch (type)
    {
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
      error_status =
	db_string_to_datetime (DB_GET_STRING (src_datetime1), &db_datetime);
      if (error_status != NO_ERROR)
	{
	  return error_status;
	}
      db_datetime_decode (&db_datetime, &month, &day, &year, &hour,
			  &minute, &second, &millisecond);
      break;

    case DB_TYPE_DATETIME:
      db_datetime_decode (DB_GET_DATETIME (src_datetime1),
			  &month, &day, &year, &hour,
			  &minute, &second, &millisecond);
      break;

    case DB_TYPE_DATE:
      db_date_decode (DB_GET_DATE (src_datetime1), &month, &day, &year);
      break;

    case DB_TYPE_TIMESTAMP:
      db_timestamp_decode (DB_GET_TIMESTAMP (src_datetime1), &db_date,
			   &db_time);
      db_date_decode (&db_date, &month, &day, &year);
      db_time_decode (&db_time, &hour, &minute, &second);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QSTR_INVALID_DATA_TYPE, 0);
      return ER_QSTR_INVALID_DATA_TYPE;
    }

  /* If no second argument is given, just encode the first argument. */
  if (src_time2 == NULL)
    {
      goto encode_result;
    }

  type = DB_VALUE_DOMAIN_TYPE (src_time2);
  switch (type)
    {
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
      parse_time_string ((const char *) DB_GET_STRING (src_time2), &sign,
			 &h, &mi, &s, &ms);
      break;

    case DB_TYPE_TIME:
      db_time_decode (DB_GET_TIME (src_time2), &h, &mi, &s);
      break;

    case DB_TYPE_SMALLINT:
      amount = (DB_BIGINT) DB_GET_SMALLINT (src_time2);
      break;

    case DB_TYPE_INTEGER:
      amount = (DB_BIGINT) DB_GET_INTEGER (src_time2);
      break;

    case DB_TYPE_BIGINT:
      amount = DB_GET_BIGINT (src_time2);
      break;

    case DB_TYPE_FLOAT:
      amount_d = DB_GET_FLOAT (src_time2);
      break;

    case DB_TYPE_DOUBLE:
      amount_d = DB_GET_DOUBLE (src_time2);
      break;

    case DB_TYPE_MONETARY:
      amount_d = db_value_get_monetary_amount_as_double (src_time2);
      break;

    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double ((DB_C_NUMERIC)
				    db_locate_numeric (src_time2),
				    DB_VALUE_SCALE (src_time2), &amount_d);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QSTR_INVALID_DATA_TYPE, 0);
      return ER_QSTR_INVALID_DATA_TYPE;
    }

  if (type == DB_TYPE_DOUBLE || type == DB_TYPE_FLOAT ||
      type == DB_TYPE_MONETARY || type == DB_TYPE_NUMERIC)
    {
      amount = (DB_BIGINT) amount_d;
      ms = ((long) (amount_d * 1000.0)) % 1000;
    }

  if (type != DB_TYPE_VARCHAR && type != DB_TYPE_CHAR && type != DB_TYPE_TIME)
    {
      if (amount < 0)
	{
	  amount = -amount;
	  ms = -ms;
	  sign = 1;
	}
      s = (int) ((DB_BIGINT) amount % 100);
      amount /= 100;
      mi = (int) ((DB_BIGINT) amount % 100);
      amount /= 100;
      h = (int) amount;
    }

  /* validation of minute and second */
  if ((mi < 0 || mi > 59) || (s < 0 || s > 59))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_INVALID_PARAMETER, 0);
      return ER_QPROC_INVALID_PARAMETER;
    }

  /* Convert time to milliseconds. */
  amount =
    ((DB_BIGINT) h * 60 * 60 * 1000) + ((DB_BIGINT) mi * 60 * 1000) +
    ((DB_BIGINT) s * 1000) + (DB_BIGINT) ms;

encode_result:

  db_datetime_encode (&datetime,
		      month, day, year, hour, minute, second, millisecond);
  if (amount > 0)
    {
      if (sign == 0)
	{
	  error_status =
	    db_add_int_to_datetime (&datetime, amount, &calculated_datetime);
	}
      else
	{
	  error_status =
	    db_subtract_int_from_datetime (&datetime, amount,
					   &calculated_datetime);
	}
      if (error_status != NO_ERROR)
	{
	  return error_status;
	}

      db_make_datetime (result_datetime, &calculated_datetime);
    }
  else
    {
      db_make_datetime (result_datetime, &datetime);
    }

  return error_status;
}

/*
 * db_add_months () -
 */
int
db_add_months (const DB_VALUE * src_date,
	       const DB_VALUE * nmonth, DB_VALUE * result_date)
{
  int error_status = NO_ERROR;

  int n;
  int month, day, year;
  int old_month, old_year;

  assert (src_date != (DB_VALUE *) NULL);
  assert (nmonth != (DB_VALUE *) NULL);
  assert (result_date != (DB_VALUE *) NULL);

  db_value_domain_init (result_date, DB_TYPE_DATE, 0, 0);

  if (DB_IS_NULL (src_date) || DB_IS_NULL (nmonth))
    {
      DB_MAKE_NULL (result_date);
      return error_status;
    }

  n = DB_GET_INTEGER (nmonth);
  db_date_decode ((DB_DATE *) & src_date->data.date, &month, &day, &year);

  old_month = month;
  old_year = year;

  if ((month + n) >= 0)		/* Calculate month,year         */
    {
      year = year + (month + n) / 12;
      month = (month + n) % 12;
      year = (month == 0) ? year - 1 : year;
      month = (month == 0) ? 12 : month;
    }
  else
    {
      year = year + (month + n - 12) / 12;
      month = 12 + (month + n) % 12;
    }
  /* Check last day of month      */
  if (day == get_last_day (old_month, old_year)
      || day > get_last_day (month, year))
    {
      day = get_last_day (month, year);
    }

  if (0 < year && year < 10000)
    {
      DB_MAKE_DATE (result_date, month, day, year);
    }
  else
    {
      error_status = ER_DATE_EXCEED_LIMIT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }
  return error_status;
}

/*
 * db_last_day () -
 */
int
db_last_day (const DB_VALUE * src_date, DB_VALUE * result_day)
{
  int error_status = NO_ERROR;


  int month, day, year;
  int lastday;


  assert (src_date != (DB_VALUE *) NULL);
  assert (result_day != (DB_VALUE *) NULL);

  if (DB_IS_NULL (src_date))
    {
      DB_MAKE_NULL (result_day);
      return error_status;
    }

  db_date_decode ((DB_DATE *) & src_date->data.date, &month, &day, &year);

  lastday = get_last_day (month, year);

  DB_MAKE_DATE (result_day, month, lastday, year);

  return error_status;
}

/*
 * db_months_between () -
 */
int
db_months_between (const DB_VALUE * start_mon,
		   const DB_VALUE * end_mon, DB_VALUE * result_mon)
{
  int error_status = NO_ERROR;

  double result_double;
  int start_month, start_day, start_year;
  int end_month, end_day, end_year;
  DB_DATE *start_date, *end_date;


  assert (start_mon != (DB_VALUE *) NULL);
  assert (end_mon != (DB_VALUE *) NULL);
  assert (result_mon != (DB_VALUE *) NULL);

  /* now return null */

  if (DB_IS_NULL (start_mon) || DB_IS_NULL (end_mon))
    {
      DB_MAKE_NULL (result_mon);
      return error_status;
    }

  db_date_decode ((DB_DATE *) & start_mon->data.date, &start_month,
		  &start_day, &start_year);
  db_date_decode ((DB_DATE *) & end_mon->data.date, &end_month, &end_day,
		  &end_year);

  if (start_day == end_day
      || (start_day == get_last_day (start_month, start_year)
	  && end_day == get_last_day (end_month, end_year)))
    {
      result_double = (double) (start_year * 12 + start_month -
				end_year * 12 - end_month);
    }
  else
    {
      start_date = DB_GET_DATE (start_mon);
      end_date = DB_GET_DATE (end_mon);

      result_double = (double) ((start_year - end_year) * 12.0) +
	(double) (start_month - end_month) +
	(double) ((start_day - end_day) / 31.0);
    }

  DB_MAKE_DOUBLE (result_mon, result_double);

  return error_status;
}

/*
 * db_sys_date () -
 */
int
db_sys_date (DB_VALUE * result_date)
{
  int error_status = NO_ERROR;

  time_t tloc;
  struct tm *c_time_struct;

  assert (result_date != (DB_VALUE *) NULL);

  /* now return null */
  db_value_domain_init (result_date, DB_TYPE_DATE, 0, 0);

  /* Need checking error                  */

  if (time (&tloc) == -1)
    {
      error_status = ER_SYSTEM_DATE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  c_time_struct = localtime (&tloc);
  if (c_time_struct == NULL)
    {
      error_status = ER_SYSTEM_DATE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  DB_MAKE_DATE (result_date, c_time_struct->tm_mon + 1,
		c_time_struct->tm_mday, c_time_struct->tm_year + 1900);

  return error_status;
}

/*
 * db_sys_time () -
 */
int
db_sys_time (DB_VALUE * result_time)
{
  int error_status = NO_ERROR;

  time_t tloc;
  struct tm *c_time_struct;

  assert (result_time != (DB_VALUE *) NULL);

  /* now return null */
  db_value_domain_init (result_time, DB_TYPE_TIME, 0, 0);

  /* Need checking error                  */

  if (time (&tloc) == -1)
    {
      error_status = ER_SYSTEM_DATE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  c_time_struct = localtime (&tloc);
  if (c_time_struct == NULL || c_time_struct == (struct tm *) -1)
    {
      error_status = ER_SYSTEM_DATE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }
  DB_MAKE_TIME (result_time, c_time_struct->tm_hour, c_time_struct->tm_min,
		c_time_struct->tm_sec);

  return error_status;
}

/*
 * db_sys_timestamp () -
 */
int
db_sys_timestamp (DB_VALUE * result_timestamp)
{
  int error_status = NO_ERROR;

  time_t tloc;

  assert (result_timestamp != (DB_VALUE *) NULL);

  /* now return null */
  db_value_domain_init (result_timestamp, DB_TYPE_TIMESTAMP, 0, 0);

  if (time (&tloc) == -1 || OR_CHECK_INT_OVERFLOW (tloc))
    {
      error_status = ER_SYSTEM_DATE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }
  DB_MAKE_TIMESTAMP (result_timestamp, (DB_TIMESTAMP) tloc);

  return error_status;
}

/*
 * db_sys_datetime () -
 */
int
db_sys_datetime (DB_VALUE * result_datetime)
{
  int error_status = NO_ERROR;
  DB_DATETIME datetime;

  struct timeb tloc;
  struct tm *c_time_struct;

  assert (result_datetime != (DB_VALUE *) NULL);

  /* now return null */
  db_value_domain_init (result_datetime, DB_TYPE_DATETIME, 0, 0);

  if (ftime (&tloc) != 0)
    {
      error_status = ER_SYSTEM_DATE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  c_time_struct = localtime (&tloc.time);
  if (c_time_struct == NULL || c_time_struct == (struct tm *) -1)
    {
      error_status = ER_SYSTEM_DATE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  db_datetime_encode (&datetime, c_time_struct->tm_mon + 1,
		      c_time_struct->tm_mday, c_time_struct->tm_year + 1900,
		      c_time_struct->tm_hour, c_time_struct->tm_min,
		      c_time_struct->tm_sec, tloc.millitm);
  DB_MAKE_DATETIME (result_datetime, &datetime);

  return error_status;
}

/*
 * get_last_day () -
 */
int
get_last_day (int month, int year)
{
  int lastday = 0;

  if (year >= 1700)
    {
      switch (month)
	{
	case 1:
	case 3:
	case 5:
	case 7:
	case 8:
	case 10:
	case 12:
	  lastday = 31;
	  break;
	case 4:
	case 6:
	case 9:
	case 11:
	  lastday = 30;
	  break;
	case 2:
	  if (year % 4 == 0)
	    {
	      if (year % 100 == 0)
		{
		  if (year % 400 == 0)
		    {
		      lastday = 29;
		    }
		  else
		    {
		      lastday = 28;
		    }
		}
	      else
		{
		  lastday = 29;
		}
	    }
	  else
	    {
	      lastday = 28;
	    }
	  break;
	default:
	  break;		/*  Need Error Checking          */
	}
    }
  else
    {
      switch (month)
	{
	case 1:
	case 3:
	case 5:
	case 7:
	case 8:
	case 10:
	case 12:
	  lastday = 31;
	  break;
	case 4:
	case 6:
	case 9:
	case 11:
	  lastday = 30;
	  break;
	case 2:
	  if (year % 4 == 0)
	    {
	      lastday = 29;
	    }
	  else
	    {
	      lastday = 28;
	    }
	  break;
	default:
	  break;		/*  Need Error Checking          */
	}
    }
  return lastday;
}

/*
 * db_to_char () -
 */
int
db_to_char (const DB_VALUE * src_value,
	    const DB_VALUE * format_str,
	    const DB_VALUE * lang_str, DB_VALUE * result_str)
{
  int error_status = NO_ERROR;
  DB_TYPE type;

  if (DB_VALUE_DOMAIN_TYPE (src_value) == DB_TYPE_NULL
      || is_number (src_value))
    {
      return number_to_char (src_value, format_str, lang_str, result_str);
    }
  else if ((type = DB_VALUE_DOMAIN_TYPE (src_value)) == DB_TYPE_DATE
	   || type == DB_TYPE_TIME || type == DB_TYPE_TIMESTAMP
	   || type == DB_TYPE_DATETIME)
    {
      return date_to_char (src_value, format_str, lang_str, result_str);
    }
  else
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }
}

/* TODO: extract the following messages */
const char *Month_name[][12] = {
  {"January", "February", "March", "April",
   "May", "June", "July", "August", "September", "October",
   "November", "December"},	/* US */
  {"1\xbf\xf9",			/* 1�� */
   "2\xbf\xf9",			/* 2�� */
   "3\xbf\xf9",			/* 3�� */
   "4\xbf\xf9",			/* 4�� */
   "5\xbf\xf9",			/* 5�� */
   "6\xbf\xf9",			/* 6�� */
   "7\xbf\xf9",			/* 7�� */
   "8\xbf\xf9",			/* 8�� */
   "9\xbf\xf9",			/* 9�� */
   "10\xbf\xf9",		/* 10�� */
   "11\xbf\xf9",		/* 11�� */
   "12\xbf\xf9" /* 12�� */ }	/* KR */
};

const char *Day_name[][7] = {
  {"Sunday", "Monday", "Tuesday", "Wednesday",
   "Thursday", "Friday", "Saturday"},	/* US */
  {"\xc0\xcf\xbf\xe4\xc0\xcf",	/* �Ͽ��� */
   "\xbf\xf9\xbf\xe4\xc0\xcf",	/* ����� */
   "\xc8\xad\xbf\xe4\xc0\xcf",	/* ȭ���� */
   "\xbc\xf6\xbf\xe4\xc0\xcf",	/* ����� */
   "\xb8\xf1\xbf\xe4\xc0\xcf",	/* ����� */
   "\xb1\xdd\xbf\xe4\xc0\xcf",	/* �ݿ��� */
   "\xc5\xe4\xbf\xe4\xc0\xcf" /* ����� */ }	/* KR */
};

/* TODO: koreean short names */
const char *Short_Month_name[][12] = {
  {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"},	/* US */
  {"1\xbf\xf9",			/* 1�� */
   "2\xbf\xf9",			/* 2�� */
   "3\xbf\xf9",			/* 3�� */
   "4\xbf\xf9",			/* 4�� */
   "5\xbf\xf9",			/* 5�� */
   "6\xbf\xf9",			/* 6�� */
   "7\xbf\xf9",			/* 7�� */
   "8\xbf\xf9",			/* 8�� */
   "9\xbf\xf9",			/* 9�� */
   "10\xbf\xf9",		/* 10�� */
   "11\xbf\xf9",		/* 11�� */
   "12\xbf\xf9" /* 12�� */ }	/* KR */
};

const char *Short_Day_name[][7] = {
  {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"},	/* US */
  {"\xc0\xcf",			/* �Ͽ��� */
   "\xbf\xf9",			/* ����� */
   "\xc8\xad",			/* ȭ���� */
   "\xbc\xf6",			/* ����� */
   "\xb8\xf1",			/* ����� */
   "\xb1\xdd",			/* �ݿ��� */
   "\xc5\xe4" /* ����� */ }	/* KR */
};

#define AM_NAME_KR "\xbf\xc0\xc0\xfc"	/* ���� */
#define PM_NAME_KR "\xbf\xc0\xc8\xc4"	/* ���� */

const char *Am_Pm_name[][12] = {
  {"am", "pm", "Am", "Pm", "AM", "PM",
   "a.m.", "p.m.", "A.m.", "P.m.", "A.M.", "P.M."},	/* US */
  {AM_NAME_KR, PM_NAME_KR, AM_NAME_KR, PM_NAME_KR, AM_NAME_KR, PM_NAME_KR,
   AM_NAME_KR, PM_NAME_KR, AM_NAME_KR, PM_NAME_KR, AM_NAME_KR, PM_NAME_KR}
  /* KR */
};

enum
{ am_NAME, pm_NAME, Am_NAME, Pm_NAME, AM_NAME, PM_NAME,
  a_m_NAME, p_m_NAME, A_m_NAME, P_m_NAME, A_M_NAME, P_M_NAME
};

/*
 * db_to_date () -
 */
int
db_to_date (const DB_VALUE * src_str,
	    const DB_VALUE * format_str,
	    const DB_VALUE * date_lang, DB_VALUE * result_date)
{
  int error_status = NO_ERROR;
  unsigned char *cur_format_str_ptr, *next_format_str_ptr;
  unsigned char *cs;		/*current source string pointer */
  unsigned char *last_src, *last_format;

  int cur_format;

  int cur_format_size;

  int month = 0, day = 0, year = 0, day_of_the_week = 0, week = -1;
  int monthcount = 0, daycount = 0, yearcount = 0, day_of_the_weekcount = 0;

  int i;
  bool no_user_format;
  int date_lang_id;
  const char **p;

  assert (src_str != (DB_VALUE *) NULL);
  assert (result_date != (DB_VALUE *) NULL);
  assert (date_lang != (DB_VALUE *) NULL);

  if (DB_IS_NULL (src_str))
    {
      DB_MAKE_NULL (result_date);
      return error_status;
    }

  if (false == is_char_string (src_str))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (DB_GET_STRING_SIZE (src_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (DB_GET_STRING_SIZE (src_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_SRC_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  cs = (unsigned char *) DB_PULL_STRING (src_str);
  last_src = &cs[DB_GET_STRING_SIZE (src_str)];

  no_user_format = false;
  if (format_str == NULL || date_lang == NULL
      || (DB_GET_INT (date_lang) & 0x1))
    {
      no_user_format = true;
    }

  if (no_user_format)
    {
      DB_DATE date_tmp;

      if (NO_ERROR != db_string_to_date ((char *) cs, &date_tmp))
	{
	  error_status = ER_DATE_CONVERSION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      DB_MAKE_ENCODED_DATE (result_date, &date_tmp);
      return error_status;
    }
  else
    {
      if (DB_IS_NULL (format_str) || DB_IS_NULL (date_lang))
	{
	  DB_MAKE_NULL (result_date);
	  return error_status;
	}

      date_lang_id = ((DB_GET_INT (date_lang) & 2) ?
		      INTL_LANG_ENGLISH : INTL_LANG_KOREAN);

      if (false == is_char_string (format_str))
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
	{
	  error_status = ER_QSTR_FORMAT_TOO_LONG;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) == 0)
	{
	  error_status = ER_QSTR_EMPTY_STRING;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      cur_format_str_ptr = (unsigned char *) DB_PULL_STRING (format_str);
      last_format = &cur_format_str_ptr[DB_GET_STRING_SIZE (format_str)];

      /* Skip space, tab, CR     */
      while (cs < last_src && strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      /* Skip space, tab, CR     */
      while (cur_format_str_ptr < last_format &&
	     strchr (WHITE_CHARS, *cur_format_str_ptr))
	{
	  cur_format_str_ptr++;
	}

      while (cs < last_src)
	{
	  cur_format = get_next_format (cur_format_str_ptr, DB_TYPE_DATE,
					&cur_format_size,
					&next_format_str_ptr);
	  switch (cur_format)
	    {
	    case DT_YYYY:
	      if (yearcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  yearcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 4 && char_isdigit (*cs); cs++, i++)
		{
		  year = year * 10 + (*cs - '0');
		}
	      break;

	    case DT_YY:
	      if (yearcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  yearcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  year = year * 10 + (*cs - '0');
		}

	      i = get_cur_year ();
	      if (i == -1)
		{
		  error_status = ER_SYSTEM_DATE;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      year += (i / 100) * 100;
	      break;

	    case DT_MM:
	      if (monthcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  monthcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  month = month * 10 + (*cs - '0');
		}

	      if (month < 1 || month > 12)
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_MONTH:
	      if (monthcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  monthcount++;
		}

	      p = Month_name[date_lang_id];

	      for (i = 0; i < 12; i++)
		{
		  if (strncasecmp ((const char *) p[i], (const char *) cs,
				   strlen (p[i])) == 0)
		    {
		      month = i + 1;
		      cs += strlen (p[i]);
		      break;
		    }
		}

	      if (month == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_MON:
	      if (monthcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  monthcount++;
		}

	      month = 0;
	      if (date_lang_id == INTL_LANG_KOREAN)
		{
		  if (!char_isdigit (*cs))
		    {
		      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }

		  for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		    {
		      month = month * 10 + (*cs - '0');
		    }

		  if (month < 1 || month > 12)
		    {
		      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }
		  break;
		}
	      else
		{
		  p = Month_name[date_lang_id];
		  for (i = 0; i < 12; i++)
		    {
		      if (strncasecmp (p[i], (const char *) cs, 3) == 0)
			{
			  month = i + 1;
			  cs += 3;
			  break;
			}
		    }
		}

	      if (month == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_DD:
	      if (daycount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  daycount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  day = day * 10 + (*cs - '0');
		}

	      if (day < 0 || day > 31)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_TEXT:
	      if (strncasecmp ((const char *) (cur_format_str_ptr + 1),
			       (const char *) cs, cur_format_size - 2) != 0)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      cs += cur_format_size - 2;
	      break;

	    case DT_PUNCTUATION:
	      if (strncasecmp ((const char *) cur_format_str_ptr,
			       (const char *) cs, cur_format_size) != 0)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      cs += cur_format_size;
	      break;

	    case DT_CC:
	    case DT_Q:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      /* Does it need error message? */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      return error_status;

	    case DT_DAY:
	      if (day_of_the_weekcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  day_of_the_weekcount++;
		}

	      p = Day_name[date_lang_id];

	      for (i = 0; i < 7; i++)
		{
		  if (strncasecmp ((const char *) p[i], (const char *) cs,
				   strlen (p[i])) == 0)
		    {
		      day_of_the_week = i + 1;
		      cs += strlen (p[i]);
		      break;
		    }
		}

	      if (day_of_the_week == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_DY:
	      if (day_of_the_weekcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  day_of_the_weekcount++;
		}

	      p = Day_name[date_lang_id];

	      for (i = 0; i < 7; i++)
		{
		  int tmp_len;
		  tmp_len = (date_lang_id == INTL_LANG_KOREAN) ? 2 : 3;
		  if (strncasecmp (p[i], (const char *) cs, tmp_len) == 0)
		    {
		      day_of_the_week = i + 1;
		      cs += tmp_len;
		      break;
		    }
		}

	      if (day_of_the_week == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_D:
	      if (day_of_the_weekcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  day_of_the_weekcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      if (char_isdigit (*cs))
		{
		  day_of_the_week = *cs - '0';
		  cs += 1;
		}

	      if (day_of_the_week < 1 || day_of_the_week > 7)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_INVALID:
	    case DT_NORMAL:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      return error_status;
	    }

	  /* Skip space, tab, CR     */
	  while (cs < last_src && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }

	  cur_format_str_ptr = next_format_str_ptr;

	  /* Skip space, tab, CR     */
	  while (cur_format_str_ptr < last_format &&
		 strchr (WHITE_CHARS, *cur_format_str_ptr))
	    {
	      cur_format_str_ptr++;
	    }

	  if (last_format == next_format_str_ptr)
	    {
	      while (cs < last_src && strchr (WHITE_CHARS, *cs))
		{
		  cs++;
		}

	      if (cs != last_src)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;
	    }
	}
    }

  /* Both format and src should end at same time     */
  if (cs != last_src || cur_format_str_ptr != last_format)
    {
      error_status = ER_QSTR_INVALID_FORMAT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  year = (yearcount == 0) ? get_cur_year () : year;
  month = (monthcount == 0) ? get_cur_month () : month;
  day = (daycount == 0) ? 1 : day;
  week = (day_of_the_weekcount == 0) ? -1 : day_of_the_week - 1;

  if (is_valid_date (month, day, year, week) == true)
    {
      DB_MAKE_DATE (result_date, month, day, year);
    }
  else
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  return error_status;
}

/*
 * db_to_time () -
 */
int
db_to_time (const DB_VALUE * src_str,
	    const DB_VALUE * format_str,
	    const DB_VALUE * date_lang, DB_VALUE * result_time)
{
  int error_status = NO_ERROR;

  unsigned char *cur_format_str_ptr, *next_format_str_ptr;
  unsigned char *cs;		/*current source string pointer */
  unsigned char *last_format, *last_src;

  int cur_format;

  int cur_format_size;

  int second = 0, minute = 0, hour = 0;
  int time_count = 0;
  int mil_time_count = 0;
  int am = false;
  int pm = false;

  int i;
  bool no_user_format;
  int date_lang_id;

  assert (src_str != (DB_VALUE *) NULL);
  assert (result_time != (DB_VALUE *) NULL);
  assert (date_lang != (DB_VALUE *) NULL);

  if (DB_IS_NULL (src_str))
    {
      DB_MAKE_NULL (result_time);
      return error_status;
    }

  /* now return null */
  if (false == is_char_string (src_str))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (DB_GET_STRING_SIZE (src_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (DB_GET_STRING_SIZE (src_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_SRC_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  cs = (unsigned char *) DB_PULL_STRING (src_str);
  last_src = &cs[DB_GET_STRING_SIZE (src_str)];

  no_user_format = false;
  if (format_str == NULL || (DB_GET_INT (date_lang) & 0x1))
    {
      no_user_format = true;
    }

  if (no_user_format)
    {
      DB_TIME time_tmp;

      if (NO_ERROR != db_string_to_time ((const char *) cs, &time_tmp))
	{
	  error_status = ER_TIME_CONVERSION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      DB_MAKE_ENCODED_TIME (result_time, &time_tmp);
      return error_status;
    }
  else
    {
      date_lang_id = ((DB_GET_INT (date_lang) & 2) ?
		      INTL_LANG_ENGLISH : INTL_LANG_KOREAN);

      if (DB_IS_NULL (format_str))
	{
	  DB_MAKE_NULL (result_time);
	  return error_status;
	}

      if (false == is_char_string (format_str))
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
	{
	  error_status = ER_QSTR_FORMAT_TOO_LONG;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) == 0)
	{
	  error_status = ER_QSTR_EMPTY_STRING;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      cur_format_str_ptr = (unsigned char *) DB_PULL_STRING (format_str);
      last_format = &cur_format_str_ptr[DB_GET_STRING_SIZE (format_str)];

      /* Skip space, tab, CR     */
      while (cs < last_src && strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      /* Skip space, tab, CR     */
      while (cur_format_str_ptr < last_format &&
	     strchr (WHITE_CHARS, *cur_format_str_ptr))
	{
	  cur_format_str_ptr++;
	}

      while (cs < last_src)
	{
	  cur_format = get_next_format (cur_format_str_ptr, DB_TYPE_TIME,
					&cur_format_size,
					&next_format_str_ptr);
	  switch (cur_format)
	    {
	    case DT_AM:
	    case DT_A_M:
	    case DT_PM:
	    case DT_P_M:
	      if (mil_time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  mil_time_count++;
		}

	      if (date_lang_id == INTL_LANG_KOREAN)
		{
		  if (strlen ((const char *) cs) >= strlen (AM_NAME_KR) &&
		      strncasecmp (AM_NAME_KR, (const char *) cs,
				   strlen (AM_NAME_KR)) == 0)
		    {
		      am = true;
		      cs += strlen (AM_NAME_KR);
		    }
		  else if (strlen ((const char *) cs) >=
			   strlen (PM_NAME_KR)
			   && strncasecmp (PM_NAME_KR, (const char *) cs,
					   strlen (PM_NAME_KR)) == 0)
		    {
		      pm = true;
		      cs += strlen (PM_NAME_KR);
		    }
		  else
		    {
		      error_status = ER_QSTR_INVALID_FORMAT;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }
		}
	      else
		{
		  if (strlen ((const char *) cs) >= strlen ("am") &&
		      strncasecmp ("am", (const char *) cs,
				   strlen ("am")) == 0)
		    {
		      am = true;
		      cs += strlen ("am");
		    }
		  else if (strlen ((const char *) cs) >= strlen ("a.m.") &&
			   strncasecmp ("a.m.", (const char *) cs,
					strlen ("a.m.")) == 0)
		    {
		      am = true;
		      cs += strlen ("a.m.");
		    }
		  else if (strlen ((const char *) cs) >= strlen ("pm") &&
			   strncasecmp ("pm", (const char *) cs,
					strlen ("pm")) == 0)
		    {
		      pm = true;
		      cs += strlen ("pm");
		    }
		  else if (strlen ((const char *) cs) >= strlen ("p.m.") &&
			   strncasecmp ("p.m.", (const char *) cs,
					strlen ("p.m.")) == 0)
		    {
		      pm = true;
		      cs += strlen ("p.m.");
		    }
		  else
		    {
		      error_status = ER_QSTR_INVALID_FORMAT;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }
		}
	      break;

	    case DT_HH:
	    case DT_HH12:
	      if (time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  time_count++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  hour = hour * 10 + (*cs - '0');
		}

	      if (hour < 1 || hour > 12)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_HH24:
	      if (time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  time_count++;
		}

	      if (mil_time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  mil_time_count++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  hour = hour * 10 + (*cs - '0');
		}

	      if (hour < 0 || hour > 23)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_MI:
	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  minute = minute * 10 + (*cs - '0');
		}

	      if (minute < 0 || minute > 59)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_SS:
	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  second = second * 10 + (*cs - '0');
		}

	      if (second < 0 || second > 59)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_TEXT:
	      if (strncasecmp ((const char *) cur_format_str_ptr + 1,
			       (const char *) cs, cur_format_size - 2) != 0)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      cs += cur_format_size - 2;
	      break;

	    case DT_PUNCTUATION:
	      if (strncasecmp ((const char *) cur_format_str_ptr,
			       (const char *) cs, cur_format_size) != 0)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      cs += cur_format_size;
	      break;

	    case DT_INVALID:
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      return error_status;
	    }

	  /* Skip space, tab, CR     */
	  while (cs < last_src && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }

	  cur_format_str_ptr = next_format_str_ptr;

	  /* Skip space, tab, CR     */
	  while (cur_format_str_ptr < last_format &&
		 strchr (WHITE_CHARS, *cur_format_str_ptr))
	    {
	      cur_format_str_ptr++;
	    }

	  if (last_format == next_format_str_ptr)
	    {
	      while (cs < last_src && strchr (WHITE_CHARS, *cs))
		{
		  cs++;
		}

	      if (cs != last_src)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;
	    }
	}
    }

  /* Both format and src should end at same time     */
  if (cs != last_src || cur_format_str_ptr != last_format)
    {
      error_status = ER_TIME_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (am == true && pm == false && hour <= 12)
    {				/* If A.M.    */
      hour = (hour == 12) ? 0 : hour;
    }
  else if (am == false && pm == true && hour <= 12)
    {				/* If P.M.    */
      hour = (hour == 12) ? hour : hour + 12;
    }
  else if (am == false && pm == false)
    {				/* If military time    */
      ;
    }
  else
    {
      error_status = ER_TIME_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  DB_MAKE_TIME (result_time, hour, minute, second);

  return error_status;
}

/*
 * db_to_timestamp () -
 */
int
db_to_timestamp (const DB_VALUE * src_str,
		 const DB_VALUE * format_str,
		 const DB_VALUE * date_lang, DB_VALUE * result_timestamp)
{
  int error_status = NO_ERROR;

  DB_DATE tmp_date;
  DB_TIME tmp_time;
  DB_TIMESTAMP tmp_timestamp;

  unsigned char *cur_format_str_ptr, *next_format_str_ptr;
  unsigned char *cs;		/*current source string pointer */
  unsigned char *last_format, *last_src;

  int cur_format_size;
  int cur_format;

  int month = 0, day = 0, year = 0, day_of_the_week = 0, week = -1;
  int monthcount = 0, daycount = 0, yearcount = 0, day_of_the_weekcount = 0;

  int second = 0, minute = 0, hour = 0;
  int time_count = 0;
  int mil_time_count = 0;
  int am = false;
  int pm = false;

  int i;
  bool no_user_format;
  int date_lang_id;
  const char **p;

  assert (src_str != (DB_VALUE *) NULL);
  assert (result_timestamp != (DB_VALUE *) NULL);
  assert (date_lang != (DB_VALUE *) NULL);

  if (DB_IS_NULL (src_str))
    {
      DB_MAKE_NULL (result_timestamp);
      return error_status;
    }

  if (false == is_char_string (src_str))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (DB_GET_STRING_SIZE (src_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (DB_GET_STRING_SIZE (src_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_SRC_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  cs = (unsigned char *) DB_PULL_STRING (src_str);
  last_src = &cs[DB_GET_STRING_SIZE (src_str)];

  no_user_format = false;
  if (format_str == NULL || (DB_GET_INT (date_lang) & 0x1))
    {
      no_user_format = true;
    }

  if (no_user_format)
    {
      DB_TIMESTAMP timestamp_tmp;

      if (NO_ERROR !=
	  db_string_to_timestamp ((const char *) cs, &timestamp_tmp))
	{
	  error_status = ER_TIMESTAMP_CONVERSION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      DB_MAKE_TIMESTAMP (result_timestamp, timestamp_tmp);
      return error_status;
    }
  else
    {
      date_lang_id = ((DB_GET_INT (date_lang) & 2) ?
		      INTL_LANG_ENGLISH : INTL_LANG_KOREAN);

      if (DB_IS_NULL (format_str))
	{
	  DB_MAKE_NULL (result_timestamp);
	  return error_status;
	}

      if (false == is_char_string (format_str))
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
	{
	  error_status = ER_QSTR_FORMAT_TOO_LONG;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) == 0)
	{
	  error_status = ER_QSTR_EMPTY_STRING;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      cur_format_str_ptr = (unsigned char *) DB_PULL_STRING (format_str);
      last_format = &cur_format_str_ptr[DB_GET_STRING_SIZE (format_str)];

      /* Skip space, tab, CR     */
      while (cs < last_src && strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      /* Skip space, tab, CR     */
      while (cur_format_str_ptr < last_format &&
	     strchr (WHITE_CHARS, *cur_format_str_ptr))
	{
	  cur_format_str_ptr++;
	}

      while (cs < last_src)
	{
	  cur_format = get_next_format (cur_format_str_ptr, DB_TYPE_TIMESTAMP,
					&cur_format_size,
					&next_format_str_ptr);
	  switch (cur_format)
	    {
	    case DT_YYYY:
	      if (yearcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  yearcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 4 && char_isdigit (*cs); cs++, i++)
		{
		  year = year * 10 + (*cs - '0');
		}
	      break;

	    case DT_YY:
	      if (yearcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  yearcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  year = year * 10 + (*cs - '0');
		}

	      i = get_cur_year ();
	      if (i == -1)
		{
		  error_status = ER_SYSTEM_DATE;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      year += (i / 100) * 100;
	      break;

	    case DT_MM:
	      if (monthcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  monthcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  month = month * 10 + (*cs - '0');
		}

	      if (month < 1 || month > 12)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_MONTH:
	      if (monthcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  monthcount++;
		}

	      p = Month_name[date_lang_id];
	      for (i = 0; i < 12; i++)
		{
		  if (strncasecmp
		      ((const char *) p[i], (const char *) cs,
		       strlen (p[i])) == 0)
		    {
		      month = i + 1;
		      cs += strlen (p[i]);
		      break;
		    }
		}

	      if (month == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_MON:
	      if (monthcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  monthcount++;
		}

	      month = 0;
	      if (date_lang_id == INTL_LANG_KOREAN)
		{
		  if (!char_isdigit (*cs))
		    {
		      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }

		  for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		    {
		      month = month * 10 + (*cs - '0');
		    }

		  if (month < 1 || month > 12)
		    {
		      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }
		  break;
		}
	      else
		{
		  p = Month_name[date_lang_id];
		  for (i = 0; i < 12; i++)
		    {
		      if (strncasecmp (p[i], (const char *) cs, 3) == 0)
			{
			  month = i + 1;
			  cs += 3;
			  break;
			}
		    }
		}

	      if (month == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_DD:
	      if (daycount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  daycount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  day = day * 10 + (*cs - '0');
		}

	      if (day < 0 || day > 31)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_AM:
	    case DT_A_M:
	    case DT_PM:
	    case DT_P_M:
	      if (mil_time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  mil_time_count++;
		}

	      if (date_lang_id == INTL_LANG_KOREAN)
		{
		  if (strlen ((const char *) cs) >= strlen (AM_NAME_KR) &&
		      strncasecmp (AM_NAME_KR, (const char *) cs,
				   strlen (AM_NAME_KR)) == 0)
		    {
		      am = true;
		      cs += strlen (AM_NAME_KR);
		    }
		  else if (strlen ((const char *) cs) >=
			   strlen (PM_NAME_KR)
			   && strncasecmp (PM_NAME_KR, (const char *) cs,
					   strlen (PM_NAME_KR)) == 0)
		    {
		      pm = true;
		      cs += strlen (PM_NAME_KR);
		    }
		  else
		    {
		      error_status = ER_QSTR_INVALID_FORMAT;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }
		}
	      else
		{
		  if (strlen ((const char *) cs) >= strlen ("am") &&
		      strncasecmp ("am", (const char *) cs,
				   strlen ("am")) == 0)
		    {
		      am = true;
		      cs += strlen ("am");
		    }
		  else if (strlen ((const char *) cs) >= strlen ("a.m.") &&
			   strncasecmp ("a.m.", (const char *) cs,
					strlen ("a.m.")) == 0)
		    {
		      am = true;
		      cs += strlen ("a.m.");
		    }
		  else if (strlen ((const char *) cs) >= strlen ("pm") &&
			   strncasecmp ("pm", (const char *) cs,
					strlen ("pm")) == 0)
		    {
		      pm = true;
		      cs += strlen ("pm");
		    }
		  else if (strlen ((const char *) cs) >= strlen ("p.m.") &&
			   strncasecmp ("p.m.", (const char *) cs,
					strlen ("p.m.")) == 0)
		    {
		      pm = true;
		      cs += strlen ("p.m.");
		    }
		  else
		    {
		      error_status = ER_QSTR_INVALID_FORMAT;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }
		}
	      break;

	    case DT_HH:
	    case DT_HH12:
	      if (time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  time_count++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  hour = hour * 10 + (*cs - '0');
		}

	      if (hour < 1 || hour > 12)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_HH24:
	      if (time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  time_count++;
		}

	      if (mil_time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  mil_time_count++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  hour = hour * 10 + (*cs - '0');
		}

	      if (hour < 0 || hour > 23)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_MI:
	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  minute = minute * 10 + (*cs - '0');
		}

	      if (minute < 0 || minute > 59)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_SS:
	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  second = second * 10 + (*cs - '0');
		}

	      if (second < 0 || second > 59)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_TEXT:
	      if (strncasecmp ((const char *) (void *) cur_format_str_ptr + 1,
			       (const char *) cs, cur_format_size - 2) != 0)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      cs += cur_format_size - 2;
	      break;

	    case DT_PUNCTUATION:
	      if (strncasecmp ((const char *) (void *) cur_format_str_ptr,
			       (const char *) cs, cur_format_size) != 0)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      cs += cur_format_size;
	      break;

	    case DT_CC:
	    case DT_Q:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      return error_status;

	    case DT_DAY:
	      if (day_of_the_weekcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  day_of_the_weekcount++;
		}

	      p = Day_name[date_lang_id];

	      for (i = 0; i < 7; i++)
		{
		  if (strncasecmp ((const char *) p[i], (const char *) cs,
				   strlen (p[i])) == 0)
		    {
		      day_of_the_week = i + 1;
		      cs += strlen (p[i]);
		      break;
		    }
		}

	      if (day_of_the_week == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_DY:
	      if (day_of_the_weekcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  day_of_the_weekcount++;
		}

	      p = Day_name[date_lang_id];

	      for (i = 0; i < 7; i++)
		{
		  int tmp_len;
		  tmp_len = (date_lang_id == INTL_LANG_KOREAN) ? 2 : 3;
		  if (strncasecmp (p[i], (const char *) cs, tmp_len) == 0)
		    {
		      day_of_the_week = i + 1;
		      cs += tmp_len;
		      break;
		    }
		}

	      if (day_of_the_week == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_D:
	      if (day_of_the_weekcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  day_of_the_weekcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      if (char_isdigit (*cs))
		{
		  day_of_the_week = *cs - '0';
		  cs += 1;
		}

	      if (day_of_the_week < 1 || day_of_the_week > 7)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_NORMAL:
	    case DT_INVALID:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      return error_status;
	    }

	  /* Skip space, tab, CR     */
	  while (cs < last_src && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }

	  cur_format_str_ptr = next_format_str_ptr;

	  /* Skip space, tab, CR     */
	  while (cur_format_str_ptr < last_format &&
		 strchr (WHITE_CHARS, *cur_format_str_ptr))
	    {
	      cur_format_str_ptr++;
	    }

	  if (last_format == next_format_str_ptr)
	    {
	      while (cs < last_src && strchr (WHITE_CHARS, *cs))
		{
		  cs++;
		}

	      if (cs != last_src)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;
	    }
	}
    }

  /* Both format and src should end at same time     */
  if (cs != last_src || cur_format_str_ptr != last_format)
    {
      error_status = ER_QSTR_INVALID_FORMAT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  /**************            Check DATE        ****************/
  year = (yearcount == 0) ? get_cur_year () : year;
  month = (monthcount == 0) ? get_cur_month () : month;
  day = (daycount == 0) ? 1 : day;
  week = (day_of_the_weekcount == 0) ? -1 : day_of_the_week - 1;

  if (is_valid_date (month, day, year, week) == true)
    {
      db_date_encode (&tmp_date, month, day, year);
    }
  else
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  /**************            Check TIME        ****************/
  if (am == true && pm == false && hour <= 12)
    {				/* If A.M.    */
      hour = (hour == 12) ? 0 : hour;
    }
  else if (am == false && pm == true && hour <= 12)
    {				/* If P.M.    */
      hour = (hour == 12) ? hour : hour + 12;
    }
  else if (am == false && pm == false)
    {				/* If military time    */
      ;
    }
  else
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  db_time_encode (&tmp_time, hour, minute, second);

  /*************         Make TIMESTAMP        *****************/
  if (NO_ERROR != db_timestamp_encode (&tmp_timestamp, &tmp_date, &tmp_time))
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  DB_MAKE_TIMESTAMP (result_timestamp, tmp_timestamp);
  return error_status;
}

/*
 * db_to_datetime () -
 */
int
db_to_datetime (const DB_VALUE * src_str, const DB_VALUE * format_str,
		const DB_VALUE * date_lang, DB_VALUE * result_datetime)
{
  int error_status = NO_ERROR;

  DB_DATETIME tmp_datetime;

  unsigned char *cur_format_str_ptr, *next_format_str_ptr;
  unsigned char *cs;		/*current source string pointer */
  unsigned char *last_format, *last_src;

  int cur_format_size;
  int cur_format;

  int month = 0, day = 0, year = 0, day_of_the_week = 0, week = -1;
  int monthcount = 0, daycount = 0, yearcount = 0, day_of_the_weekcount = 0;

  double fraction;
  int millisecond = 0, second = 0, minute = 0, hour = 0;
  int time_count = 0;
  int mil_time_count = 0;
  int am = false;
  int pm = false;

  int i;
  bool no_user_format;
  int date_lang_id;
  const char **p;

  assert (src_str != (DB_VALUE *) NULL);
  assert (result_datetime != (DB_VALUE *) NULL);

  if (DB_IS_NULL (src_str))
    {
      DB_MAKE_NULL (result_datetime);
      return error_status;
    }

  if (false == is_char_string (src_str))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (DB_GET_STRING_SIZE (src_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (DB_GET_STRING_SIZE (src_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_SRC_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  cs = (unsigned char *) DB_PULL_STRING (src_str);
  last_src = &cs[DB_GET_STRING_SIZE (src_str)];

  no_user_format = false;
  if (format_str == NULL || (DB_GET_INT (date_lang) & 0x1))
    {
      no_user_format = true;
    }

  if (no_user_format)
    {
      DB_DATETIME datetime_tmp;

      if (db_string_to_datetime ((const char *) cs,
				 &datetime_tmp) != NO_ERROR)
	{
	  error_status = ER_TIMESTAMP_CONVERSION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      DB_MAKE_DATETIME (result_datetime, &datetime_tmp);
      return error_status;
    }
  else
    {
      date_lang_id = ((DB_GET_INT (date_lang) & 2) ?
		      INTL_LANG_ENGLISH : INTL_LANG_KOREAN);

      if (DB_IS_NULL (format_str))
	{
	  DB_MAKE_NULL (result_datetime);
	  return error_status;
	}

      if (false == is_char_string (format_str))
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
	{
	  error_status = ER_QSTR_FORMAT_TOO_LONG;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) == 0)
	{
	  error_status = ER_QSTR_EMPTY_STRING;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      cur_format_str_ptr = (unsigned char *) DB_PULL_STRING (format_str);
      last_format = &cur_format_str_ptr[DB_GET_STRING_SIZE (format_str)];

      /* Skip space, tab, CR     */
      while (cs < last_src && strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      /* Skip space, tab, CR     */
      while (cur_format_str_ptr < last_format &&
	     strchr (WHITE_CHARS, *cur_format_str_ptr))
	{
	  cur_format_str_ptr++;
	}

      while (cs < last_src)
	{
	  cur_format = get_next_format (cur_format_str_ptr, DB_TYPE_DATETIME,
					&cur_format_size,
					&next_format_str_ptr);
	  switch (cur_format)
	    {
	    case DT_YYYY:
	      if (yearcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  yearcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 4 && char_isdigit (*cs); cs++, i++)
		{
		  year = year * 10 + (*cs - '0');
		}
	      break;

	    case DT_YY:
	      if (yearcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  yearcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  year = year * 10 + (*cs - '0');
		}

	      i = get_cur_year ();
	      if (i == -1)
		{
		  error_status = ER_SYSTEM_DATE;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      year += (i / 100) * 100;
	      break;

	    case DT_MM:
	      if (monthcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  monthcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  month = month * 10 + (*cs - '0');
		}

	      if (month < 1 || month > 12)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_MONTH:
	      if (monthcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  monthcount++;
		}

	      p = Month_name[date_lang_id];
	      for (i = 0; i < 12; i++)
		{
		  if (strncasecmp ((const char *) p[i], (const char *) cs,
				   strlen (p[i])) == 0)
		    {
		      month = i + 1;
		      cs += strlen (p[i]);
		      break;
		    }
		}

	      if (month == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_MON:
	      if (monthcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  monthcount++;
		}

	      month = 0;
	      if (date_lang_id == INTL_LANG_KOREAN)
		{
		  if (!char_isdigit (*cs))
		    {
		      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }

		  for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		    {
		      month = month * 10 + (*cs - '0');
		    }

		  if (month < 1 || month > 12)
		    {
		      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }
		  break;
		}
	      else
		{
		  p = Month_name[date_lang_id];
		  for (i = 0; i < 12; i++)
		    {
		      if (strncasecmp (p[i], (const char *) cs, 3) == 0)
			{
			  month = i + 1;
			  cs += 3;
			  break;
			}
		    }
		}

	      if (month == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_DD:
	      if (daycount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  daycount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  day = day * 10 + (*cs - '0');
		}

	      if (day < 0 || day > 31)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_AM:
	    case DT_A_M:
	    case DT_PM:
	    case DT_P_M:
	      if (mil_time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  mil_time_count++;
		}

	      if (date_lang_id == INTL_LANG_KOREAN)
		{
		  if (strlen ((const char *) cs) >= strlen (AM_NAME_KR) &&
		      strncasecmp (AM_NAME_KR, (const char *) cs,
				   strlen (AM_NAME_KR)) == 0)
		    {
		      am = true;
		      cs += strlen (AM_NAME_KR);
		    }
		  else if (strlen ((const char *) cs) >= strlen (PM_NAME_KR)
			   && strncasecmp (PM_NAME_KR, (const char *) cs,
					   strlen (PM_NAME_KR)) == 0)
		    {
		      pm = true;
		      cs += strlen (PM_NAME_KR);
		    }
		  else
		    {
		      error_status = ER_QSTR_INVALID_FORMAT;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }
		}
	      else
		{
		  if (strlen ((const char *) cs) >= strlen ("am") &&
		      strncasecmp ("am", (const char *) cs,
				   strlen ("am")) == 0)
		    {
		      am = true;
		      cs += strlen ("am");
		    }
		  else if (strlen ((const char *) cs) >= strlen ("a.m.") &&
			   strncasecmp ("a.m.", (const char *) cs,
					strlen ("a.m.")) == 0)
		    {
		      am = true;
		      cs += strlen ("a.m.");
		    }
		  else if (strlen ((const char *) cs) >= strlen ("pm") &&
			   strncasecmp ("pm", (const char *) cs,
					strlen ("pm")) == 0)
		    {
		      pm = true;
		      cs += strlen ("pm");
		    }
		  else if (strlen ((const char *) cs) >= strlen ("p.m.") &&
			   strncasecmp ("p.m.", (const char *) cs,
					strlen ("p.m.")) == 0)
		    {
		      pm = true;
		      cs += strlen ("p.m.");
		    }
		  else
		    {
		      error_status = ER_QSTR_INVALID_FORMAT;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error_status, 0);
		      return error_status;
		    }
		}
	      break;

	    case DT_HH:
	    case DT_HH12:
	      if (time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  time_count++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  hour = hour * 10 + (*cs - '0');
		}

	      if (hour < 1 || hour > 12)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_HH24:
	      if (time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  time_count++;
		}

	      if (mil_time_count != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  mil_time_count++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  hour = hour * 10 + (*cs - '0');
		}

	      if (hour < 0 || hour > 23)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_MI:
	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  minute = minute * 10 + (*cs - '0');
		}

	      if (minute < 0 || minute > 59)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_SS:
	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0; i < 2 && char_isdigit (*cs); cs++, i++)
		{
		  second = second * 10 + (*cs - '0');
		}

	      if (second < 0 || second > 59)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_MS:
	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      for (i = 0, fraction = 100; char_isdigit (*cs); cs++, i++)
		{
		  millisecond += (int) ((*cs - '0') * fraction + 0.5);
		  fraction /= 10;
		}

	      if (millisecond < 0 || millisecond > 999)
		{
		  error_status = ER_TIME_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_TEXT:
	      if (strncasecmp ((const char *) (void *) cur_format_str_ptr + 1,
			       (const char *) cs, cur_format_size - 2) != 0)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      cs += cur_format_size - 2;
	      break;

	    case DT_PUNCTUATION:
	      if (strncasecmp ((const char *) (void *) cur_format_str_ptr,
			       (const char *) cs, cur_format_size) != 0)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      cs += cur_format_size;
	      break;

	    case DT_CC:
	    case DT_Q:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      return error_status;

	    case DT_DAY:
	      if (day_of_the_weekcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  day_of_the_weekcount++;
		}

	      p = Day_name[date_lang_id];

	      for (i = 0; i < 7; i++)
		{
		  if (strncasecmp ((const char *) p[i], (const char *) cs,
				   strlen (p[i])) == 0)
		    {
		      day_of_the_week = i + 1;
		      cs += strlen (p[i]);
		      break;
		    }
		}

	      if (day_of_the_week == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_DY:
	      if (day_of_the_weekcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  day_of_the_weekcount++;
		}

	      p = Day_name[date_lang_id];

	      for (i = 0; i < 7; i++)
		{
		  int tmp_len;
		  tmp_len = (date_lang_id == INTL_LANG_KOREAN) ? 2 : 3;
		  if (strncasecmp (p[i], (const char *) cs, tmp_len) == 0)
		    {
		      day_of_the_week = i + 1;
		      cs += tmp_len;
		      break;
		    }
		}

	      if (day_of_the_week == 0)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_D:
	      if (day_of_the_weekcount != 0)
		{
		  error_status = ER_QSTR_FORMAT_DUPLICATION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      else
		{
		  day_of_the_weekcount++;
		}

	      if (!char_isdigit (*cs))
		{
		  error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}

	      if (char_isdigit (*cs))
		{
		  day_of_the_week = *cs - '0';
		  cs += 1;
		}

	      if (day_of_the_week < 1 || day_of_the_week > 7)
		{
		  error_status = ER_DATE_CONVERSION;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;

	    case DT_NORMAL:
	    case DT_INVALID:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      return error_status;
	    }

	  while (cs < last_src && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }

	  cur_format_str_ptr = next_format_str_ptr;

	  /* Skip space, tab, CR     */
	  while (cur_format_str_ptr < last_format &&
		 strchr (WHITE_CHARS, *cur_format_str_ptr))
	    {
	      cur_format_str_ptr++;
	    }

	  if (last_format == next_format_str_ptr)
	    {
	      while (cs < last_src && strchr (WHITE_CHARS, *cs))
		{
		  cs++;
		}

	      if (cs != last_src)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  return error_status;
		}
	      break;
	    }
	}
    }

  /* Both format and src should end at same time     */
  if (cs != last_src || cur_format_str_ptr != last_format)
    {
      error_status = ER_QSTR_INVALID_FORMAT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  /**************            Check DATE        ****************/
  year = (yearcount == 0) ? get_cur_year () : year;
  month = (monthcount == 0) ? get_cur_month () : month;
  day = (daycount == 0) ? 1 : day;
  week = (day_of_the_weekcount == 0) ? -1 : day_of_the_week - 1;

  if (!is_valid_date (month, day, year, week) == true)
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  /**************            Check TIME        ****************/
  if (am == true && pm == false && hour <= 12)
    {				/* If A.M.    */
      hour = (hour == 12) ? 0 : hour;
    }
  else if (am == false && pm == true && hour <= 12)
    {				/* If P.M.    */
      hour = (hour == 12) ? hour : hour + 12;
    }
  else if (am == false && pm == false)
    {				/* If military time    */
      ;
    }
  else
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  /*************         Make DATETIME        *****************/
  db_datetime_encode (&tmp_datetime, month, day, year, hour, minute,
		      second, millisecond);

  if (DB_MAKE_DATETIME (result_datetime, &tmp_datetime) != NO_ERROR)
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  return error_status;
}


/*
 * adjust_precision () - Change representation of 'data' as of
 *    'precision' and 'scale'.
 *                       When data has invalid format, just return
 * return : DOMAIN_INCOMPATIBLE, DOMAIN_OVERFLOW, NO_ERROR
 */
static int
adjust_precision (char *data, int precision, int scale)
{
  char tmp_data[DB_MAX_NUMERIC_PRECISION * 2 + 1];
  int scale_counter = 0;
  int i = 0;
  int before_dec_point = 0;
  int after_dec_point = 0;
  int space_started = false;

  if (data == NULL || precision < 0 || precision > DB_MAX_NUMERIC_PRECISION
      || scale < 0 || scale > DB_MAX_NUMERIC_PRECISION)
    {
      return DOMAIN_INCOMPATIBLE;
    }

  if (*data == '-')
    {
      tmp_data[0] = '-';
      i++;
    }
  else if (*data == '+')
    {
      i++;
    }

  for (; i < DB_MAX_NUMERIC_PRECISION && *(data + i) != '\0'
       && *(data + i) != '.'; i++)
    {
      if (char_isdigit (*(data + i)))
	{
	  tmp_data[i] = *(data + i);
	  before_dec_point++;
	}
      else if (char_isspace (*(data + i)))
	{
	  space_started = true;
	  break;
	}
      else
	{
	  return DOMAIN_INCOMPATIBLE;
	}
    }

  if (space_started == true)
    {
      int j = i;
      while (char_isspace (*(data + j)))
	{
	  j++;
	}

      if (*(data + j) != '\0')
	{
	  return DOMAIN_INCOMPATIBLE;
	}
    }

  if (*(data + i) == '.')
    {
      tmp_data[i] = '.';
      i++;
      while (*(data + i) != '\0' && scale_counter < scale)
	{
	  if (char_isdigit (*(data + i)))
	    {
	      tmp_data[i] = *(data + i);
	      after_dec_point++;
	    }
	  else if (char_isspace (*(data + i)))
	    {
	      space_started = true;
	      break;
	    }
	  else
	    {
	      return DOMAIN_INCOMPATIBLE;
	    }
	  scale_counter++;
	  i++;
	}

      if (space_started == true)
	{
	  int j = i;
	  while (char_isspace (*(data + j)))
	    {
	      j++;
	    }

	  if (*(data + j) != '\0')
	    {
	      return DOMAIN_INCOMPATIBLE;
	    }
	}

      while (scale_counter < scale)
	{
	  tmp_data[i] = '0';
	  scale_counter++;
	  i++;
	}

    }
  else if (*(data + i) == '\0')
    {
      tmp_data[i] = '.';
      i++;
      while (scale_counter < scale)
	{
	  tmp_data[i] = '0';
	  scale_counter++;
	  i++;
	}

    }
  else
    {
      return DOMAIN_COMPATIBLE;
    }

  if (before_dec_point + after_dec_point > DB_MAX_NUMERIC_PRECISION
      || after_dec_point > DB_DEFAULT_NUMERIC_PRECISION
      || before_dec_point > precision - scale)
    {
      return DOMAIN_OVERFLOW;
    }

  tmp_data[i] = '\0';
  strcpy (data, tmp_data);
  return NO_ERROR;
}

/*
 * db_to_number () -
 */
int
db_to_number (const DB_VALUE * src_str,
	      const DB_VALUE * format_str, DB_VALUE * result_num)
{
  /* default precision and scale is (38, 0) */
  /* it is more profitable that the definition of this value is located in
     some header file */
  const char *dflt_format_str = "99999999999999999999999999999999999999";

  int error_status = NO_ERROR;

  char *cs;			/* current source string pointer        */
  char *last_cs;
  char *format_str_ptr;
  char *last_format;
  char *next_fsp;		/* next format string pointer   */
  int token_length;
  int count_format = 0;
  int cur_format;

  int precision = 0;		/* retain precision of format_str */
  int scale = 0;
  int loopvar, met_decptr = 0;
  int use_default_precision = 0;

  char *first_cs_for_error, *first_format_str_for_error;

  assert (src_str != (DB_VALUE *) NULL);
  assert (result_num != (DB_VALUE *) NULL);

  if (DB_IS_NULL (src_str))
    {
      DB_MAKE_NULL (result_num);
      return error_status;
    }

  if (false == is_char_string (src_str))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (DB_GET_STRING_SIZE (src_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (DB_GET_STRING_SIZE (src_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_SRC_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  cs = DB_PULL_STRING (src_str);
  last_cs = &cs[DB_GET_STRING_SIZE (src_str)];

  /* If there is no format */
  if (format_str == NULL)
    {
      format_str_ptr = (char *) dflt_format_str;
      last_format = format_str_ptr + strlen (dflt_format_str);
    }
  else				/* format_str != NULL */
    {
      if (DB_IS_NULL (format_str))
	{
	  DB_MAKE_NULL (result_num);
	  return error_status;
	}

      /*      Format string type checking     */
      if (is_char_string (format_str))
	{
	  if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
	    {
	      error_status = ER_QSTR_FORMAT_TOO_LONG;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      return error_status;
	    }

	  format_str_ptr = DB_PULL_STRING (format_str);
	  last_format = &format_str_ptr[DB_GET_STRING_SIZE (format_str)];
	}
      else
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) == 0)
	{
	  error_status = ER_QSTR_EMPTY_STRING;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}
    }

  /* Skip space, tab, CR  */
  while (cs < last_cs && strchr (WHITE_CHARS, *cs))
    {
      cs++;
    }
  while (format_str_ptr < last_format
	 && strchr (WHITE_CHARS, *format_str_ptr))
    {
      format_str_ptr++;
    }
  first_cs_for_error = cs;
  first_format_str_for_error = format_str_ptr;

  /* get precision and scale of format_str */
  for (loopvar = 0; format_str_ptr + loopvar < last_format; loopvar++)
    {
      switch (*(format_str_ptr + loopvar))
	{
	case '9':
	case '0':
	  precision++;
	  if (met_decptr > 0)
	    {
	      scale++;
	    }
	  break;
	case '.':
	  met_decptr++;
	  break;

	case 'c':
	case 'C':
	case 's':
	case 'S':
	  if (precision == 0)
	    {
	      break;
	    }
	case ',':
	  break;

	default:
	  precision = 0;
	  scale = 0;
	  use_default_precision = 1;
	}

      if (precision + scale > DB_MAX_NUMERIC_PRECISION)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NUM_OVERFLOW, 0);
	  return ER_NUM_OVERFLOW;
	}

      if (use_default_precision == 1)
	{
	  /* scientific notation */
	  precision = DB_MAX_NUMERIC_PRECISION;
	  scale = DB_DEFAULT_NUMERIC_PRECISION;
	  break;
	}
    }

  /* Skip space, tab, CR  */
  while (cs < last_cs)
    {
      cur_format = get_number_token (format_str_ptr, &token_length,
				     last_format, &next_fsp);
      switch (cur_format)
	{
	case N_FORMAT:
	  if (count_format != 0)
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      return error_status;
	    }

	  error_status = make_number (cs, last_cs, format_str_ptr,
				      &token_length, result_num, precision,
				      scale);
	  if (error_status == NO_ERROR)
	    {
	      count_format++;
	      cs += token_length;
	    }
	  else if (error_status == ER_NUM_OVERFLOW)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      return error_status;
	    }
	  else
	    {
	      goto format_mismatch;
	    }

	  break;

	case N_SPACE:
	  if (!strchr (WHITE_CHARS, *cs))
	    {
	      goto format_mismatch;
	    }

	  while (cs < last_cs && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }
	  break;

	case N_TEXT:
	  if (strncasecmp ((format_str_ptr + 1), cs, token_length - 2) != 0)
	    {
	      goto format_mismatch;
	    }
	  cs += token_length - 2;
	  break;

	case N_INVALID:
	  error_status = ER_QSTR_INVALID_FORMAT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;

	case N_END:
	  /* Skip space, tab, CR  */
	  while (cs < last_cs && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }

	  if (cs != last_cs)
	    {
	      goto format_mismatch;
	    }
	  break;
	}

      while (cs < last_cs && strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      format_str_ptr = next_fsp;

      /* Skip space, tab, CR  */
      while (format_str_ptr < last_format &&
	     strchr (WHITE_CHARS, *format_str_ptr))
	{
	  format_str_ptr++;
	}
    }

  /* Both format and src should end at same time  */
  if (cs != last_cs || format_str_ptr != last_format)
    {
      goto format_mismatch;
    }

  result_num->domain.numeric_info.precision = precision;
  result_num->domain.numeric_info.scale = scale;

  return error_status;

format_mismatch:
  while (strchr (WHITE_CHARS, *(last_cs - 1)))
    {
      last_cs--;
    }
  *last_cs = '\0';

  error_status = ER_QSTR_TONUM_FORMAT_MISMATCH;
  if (first_format_str_for_error == dflt_format_str)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      first_cs_for_error, "default");
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      first_cs_for_error, first_format_str_for_error);
    }

  return error_status;
}

/*
 * date_to_char () -
 */
static int
date_to_char (const DB_VALUE * src_value,
	      const DB_VALUE * format_str,
	      const DB_VALUE * date_lang, DB_VALUE * result_str)
{
  int error_status = NO_ERROR;
  DB_TYPE src_type;
  unsigned char *cur_format_str_ptr, *next_format_str_ptr;
  unsigned char *last_format_str_ptr;

  int cur_format_size;
  int cur_format;

  char *result_buf = NULL;
  int result_len = 0;

  int month = 0, day = 0, year = 0;
  int second = 0, minute = 0, hour = 0, millisecond = 0;

  int i;
  int j;

  unsigned int tmp_int;
  DB_DATE tmp_date;
  DB_TIME tmp_time;

  bool no_user_format;
  int date_lang_id;

  assert (src_value != (DB_VALUE *) NULL);
  assert (result_str != (DB_VALUE *) NULL);
  assert (date_lang != (DB_VALUE *) NULL);

  if (DB_IS_NULL (src_value))
    {
      DB_MAKE_NULL (result_str);
      return error_status;
    }

  src_type = DB_VALUE_DOMAIN_TYPE (src_value);

  if (src_type != DB_TYPE_DATE && src_type != DB_TYPE_TIME
      && src_type != DB_TYPE_TIMESTAMP && src_type != DB_TYPE_DATETIME)
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (date_lang == NULL)
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  no_user_format = false;
  if (format_str == NULL || (DB_GET_INT (date_lang) & 0x1))
    {
      no_user_format = true;
    }

  if (no_user_format)
    {
      int retval = 0;
      switch (src_type)
	{
	case DB_TYPE_DATE:
	  result_buf = (char *) db_private_alloc (NULL, QSTR_DATE_LENGTH + 1);
	  if (result_buf == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      return error_status;
	    }
	  result_len = QSTR_DATE_LENGTH;
	  retval = db_date_to_string (result_buf,
				      QSTR_DATE_LENGTH + 1,
				      DB_GET_DATE (src_value));
	  break;

	case DB_TYPE_TIME:
	  result_buf = (char *) db_private_alloc (NULL, QSTR_TIME_LENGTH + 1);
	  if (result_buf == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      return error_status;
	    }
	  result_len = QSTR_TIME_LENGTH;
	  retval = db_time_to_string (result_buf,
				      QSTR_TIME_LENGTH + 1,
				      DB_GET_TIME (src_value));
	  break;

	case DB_TYPE_TIMESTAMP:
	  result_buf =
	    (char *) db_private_alloc (NULL, QSTR_TIME_STAMPLENGTH + 1);
	  if (result_buf == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      return error_status;
	    }
	  result_len = QSTR_TIME_STAMPLENGTH;
	  retval = db_timestamp_to_string (result_buf,
					   QSTR_TIME_STAMPLENGTH + 1,
					   DB_GET_TIMESTAMP (src_value));
	  break;

	case DB_TYPE_DATETIME:
	  result_buf = (char *) db_private_alloc (NULL,
						  QSTR_DATETIME_LENGTH + 1);
	  if (result_buf == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      return error_status;
	    }
	  result_len = QSTR_DATETIME_LENGTH;
	  retval = db_datetime_to_string (result_buf,
					  QSTR_DATETIME_LENGTH + 1,
					  DB_GET_DATETIME (src_value));
	  break;

	default:
	  break;
	}

      if (retval == 0)
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  db_private_free_and_init (NULL, result_buf);
	  return error_status;
	}

      DB_MAKE_VARCHAR (result_str, result_len, result_buf, result_len);
    }
  else
    {
      date_lang_id = ((DB_GET_INT (date_lang) & 2) ?
		      INTL_LANG_ENGLISH : INTL_LANG_KOREAN);

      if (DB_IS_NULL (format_str))
	{
	  DB_MAKE_NULL (result_str);
	  return error_status;
	}

      result_len = (DB_GET_STRING_LENGTH (format_str)
		    * QSTR_EXTRA_LENGTH_RATIO);
      if (result_len > MAX_TOKEN_SIZE)
	{
	  error_status = ER_QSTR_FORMAT_TOO_LONG;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      cur_format_str_ptr = (unsigned char *) DB_PULL_STRING (format_str);
      last_format_str_ptr =
	&cur_format_str_ptr[DB_GET_STRING_LENGTH (format_str)];
      if (cur_format_str_ptr == last_format_str_ptr)
	{
	  error_status = ER_QSTR_EMPTY_STRING;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      switch (src_type)
	{
	case DB_TYPE_DATE:
	  db_date_decode (DB_GET_DATE (src_value), &month, &day, &year);
	  break;
	case DB_TYPE_TIME:
	  db_time_decode (DB_GET_TIME (src_value), &hour, &minute, &second);
	  break;
	case DB_TYPE_TIMESTAMP:
	  db_timestamp_decode (DB_GET_TIMESTAMP (src_value), &tmp_date,
			       &tmp_time);
	  db_date_decode (&tmp_date, &month, &day, &year);
	  db_time_decode (&tmp_time, &hour, &minute, &second);
	  break;
	case DB_TYPE_DATETIME:
	  db_datetime_decode (DB_GET_DATETIME (src_value), &month, &day,
			      &year, &hour, &minute, &second, &millisecond);
	  break;
	default:
	  break;
	}

      result_buf = (char *) db_private_alloc (NULL, result_len + 1);
      if (result_buf == NULL)
	{
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  return error_status;
	}

      i = 0;
      cur_format = DT_NORMAL;

      while (i < result_len)
	{
	  cur_format = get_next_format (cur_format_str_ptr, src_type,
					&cur_format_size,
					&next_format_str_ptr);
	  switch (cur_format)
	    {
	    case DT_CC:
	      tmp_int = (year / 100) + 1;
	      sprintf (&result_buf[i], "%02d\n", tmp_int);
	      i += 2;
	      cur_format_str_ptr += 2;
	      break;

	    case DT_YYYY:
	      sprintf (&result_buf[i], "%04d\n", year);
	      i += 4;
	      break;

	    case DT_YY:
	      tmp_int = year - (year / 100) * 100;
	      sprintf (&result_buf[i], "%02d\n", tmp_int);
	      i += 2;
	      break;

	    case DT_MM:
	      sprintf (&result_buf[i], "%02d\n", month);
	      i += 2;
	      break;

	    case DT_MONTH:
	      if (date_lang_id == INTL_LANG_KOREAN)
		{
		  sprintf (&result_buf[i], "%-4s",
			   Month_name[date_lang_id][month - 1]);
		  i += 4;
		}
	      else
		{

		  sprintf (&result_buf[i], "%-9s",
			   Month_name[date_lang_id][month - 1]);
		  if (*cur_format_str_ptr == 'm')
		    {
		      for (j = 0; j < 9; j++)
			{
			  result_buf[i + j] =
			    char_tolower (result_buf[i + j]);
			}
		    }
		  else if (*(cur_format_str_ptr + 1) == 'O')
		    {
		      for (j = 0; j < 9; j++)
			{
			  result_buf[i + j] =
			    char_toupper (result_buf[i + j]);
			}
		    }
		  i += 9;
		}
	      break;

	    case DT_MON:
	      if (date_lang_id == INTL_LANG_KOREAN)
		{
		  sprintf (&result_buf[i], "%d", month);
		  i += (month < 10) ? 1 : 2;
		}
	      else
		{
		  for (j = 0; j < 3; j++, i++)
		    {
		      result_buf[i] = Month_name[date_lang_id][month - 1][j];
		    }
		  i -= 3;
		  if (*cur_format_str_ptr == 'm')
		    {
		      for (j = 0; j < 3; j++)
			{
			  result_buf[i + j] =
			    char_tolower (result_buf[i + j]);
			}
		    }
		  else if (*(cur_format_str_ptr + 1) == 'O')
		    {
		      for (j = 0; j < 3; j++)
			{
			  result_buf[i + j] =
			    char_toupper (result_buf[i + j]);
			}
		    }
		  i += 3;
		}
	      break;

	    case DT_Q:
	      result_buf[i] = '1' + ((month - 1) / 3);
	      i++;
	      break;

	    case DT_DD:
	      sprintf (&result_buf[i], "%02d\n", day);
	      i += 2;
	      break;

	    case DT_DAY:
	      tmp_int = get_day (month, day, year);
	      if (date_lang_id == INTL_LANG_KOREAN)
		{
		  sprintf (&result_buf[i], "%-6s",
			   Day_name[date_lang_id][tmp_int]);
		  i += 6;
		}
	      else
		{
		  sprintf (&result_buf[i], "%-9s",
			   Day_name[date_lang_id][tmp_int]);
		  if (*cur_format_str_ptr == 'd')
		    {
		      for (j = 0; j < 9; j++)
			{
			  result_buf[i + j] =
			    char_tolower (result_buf[i + j]);
			}
		    }
		  else if (*(cur_format_str_ptr + 1) == 'A')
		    {
		      for (j = 0; j < 9; j++)
			{
			  result_buf[i + j] =
			    char_toupper (result_buf[i + j]);
			}
		    }
		  i += 9;
		}
	      break;

	    case DT_DY:
	      tmp_int = get_day (month, day, year);
	      if (date_lang_id == INTL_LANG_KOREAN)
		{
		  for (j = 0; j < 2; j++, i++)
		    {
		      result_buf[i] = Day_name[date_lang_id][tmp_int][j];
		    }
		}
	      else
		{
		  for (j = 0; j < 3; j++, i++)
		    {
		      result_buf[i] = Day_name[date_lang_id][tmp_int][j];
		    }
		  i -= 3;
		  if (*cur_format_str_ptr == 'd')
		    {
		      for (j = 0; j < 3; j++)
			{
			  result_buf[i + j] =
			    char_tolower (result_buf[i + j]);
			}
		    }
		  else if (*(cur_format_str_ptr + 1) == 'Y')
		    {
		      for (j = 0; j < 3; j++)
			{
			  result_buf[i + j] =
			    char_toupper (result_buf[i + j]);
			}
		    }
		  i += 3;
		}
	      break;

	    case DT_D:
	      tmp_int = get_day (month, day, year);
	      result_buf[i] = '0' + tmp_int + 1;	/* sun=1 */
	      i += 1;
	      break;

	    case DT_AM:
	    case DT_PM:
	      if (0 <= hour && hour <= 11)
		{
		  if (*cur_format_str_ptr == 'a'
		      || *cur_format_str_ptr == 'p')
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][am_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][am_NAME]);
		    }
		  else if (*(cur_format_str_ptr + 1) == 'm')
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][Am_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][Am_NAME]);
		    }
		  else
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][AM_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][AM_NAME]);
		    }
		}
	      else if (12 <= hour && hour <= 23)
		{
		  if (*cur_format_str_ptr == 'p'
		      || *cur_format_str_ptr == 'a')
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][pm_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][pm_NAME]);
		    }
		  else if (*(cur_format_str_ptr + 1) == 'm')
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][Pm_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][Pm_NAME]);
		    }
		  else
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][PM_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][PM_NAME]);
		    }
		}
	      else
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  db_private_free_and_init (NULL, result_buf);
		  return error_status;
		}
	      break;

	    case DT_A_M:
	    case DT_P_M:
	      if (0 <= hour && hour <= 11)
		{
		  if (*cur_format_str_ptr == 'a'
		      || *cur_format_str_ptr == 'p')
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][a_m_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][a_m_NAME]);
		    }
		  else if (*(cur_format_str_ptr + 2) == 'm')
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][A_m_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][A_m_NAME]);
		    }
		  else
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][A_M_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][A_M_NAME]);
		    }
		}
	      else if (12 <= hour && hour <= 23)
		{
		  if (*cur_format_str_ptr == 'p'
		      || *cur_format_str_ptr == 'a')
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][p_m_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][p_m_NAME]);
		    }
		  else if (*(cur_format_str_ptr + 2) == 'm')
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][P_m_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][P_m_NAME]);
		    }
		  else
		    {
		      strcpy (&result_buf[i],
			      Am_Pm_name[date_lang_id][P_M_NAME]);
		      i += strlen (Am_Pm_name[date_lang_id][P_M_NAME]);
		    }
		}
	      else
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  db_private_free_and_init (NULL, result_buf);
		  return error_status;
		}
	      break;

	    case DT_HH:
	    case DT_HH12:
	      tmp_int = hour % 12;
	      if (tmp_int == 0)
		{
		  tmp_int = 12;
		}
	      sprintf (&result_buf[i], "%02d\n", tmp_int);
	      i += 2;
	      break;

	    case DT_HH24:
	      sprintf (&result_buf[i], "%02d\n", hour);
	      i += 2;
	      break;

	    case DT_MI:
	      sprintf (&result_buf[i], "%02d\n", minute);
	      i += 2;
	      break;

	    case DT_SS:
	      sprintf (&result_buf[i], "%02d\n", second);
	      i += 2;
	      break;

	    case DT_MS:
	      sprintf (&result_buf[i], "%03d\n", millisecond);
	      i += 3;
	      break;

	    case DT_INVALID:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      db_private_free_and_init (NULL, result_buf);
	      return error_status;

	    case DT_NORMAL:
	      memcpy (&result_buf[i], cur_format_str_ptr, cur_format_size);
	      i += cur_format_size;
	      break;

	    case DT_TEXT:
	      memcpy (&result_buf[i], cur_format_str_ptr + 1,
		      cur_format_size - 2);
	      i += cur_format_size - 2;
	      break;

	    case DT_PUNCTUATION:
	      memcpy (&result_buf[i], cur_format_str_ptr, cur_format_size);
	      i += cur_format_size;
	      break;

	    default:
	      break;
	    }

	  cur_format_str_ptr = next_format_str_ptr;
	  if (next_format_str_ptr == last_format_str_ptr)
	    {
	      break;
	    }
	}

      DB_MAKE_VARCHAR (result_str, result_len, result_buf, i);
    }

  result_str->need_clear = true;
  return error_status;
}

/*
 * number_to_char () -
 */
static int
number_to_char (const DB_VALUE * src_value,
		const DB_VALUE * format_str,
		const DB_VALUE * date_lang, DB_VALUE * result_str)
{
  int error_status = NO_ERROR;
  char tmp_str[64];
  char *tmp_buf;

  char *cs;			/* current source string pointer     */
  char *format_str_ptr, *last_format;
  char *next_fsp;		/* next format string pointer    */
  int token_length = 0;
  int cur_format;
  char *res_string, *res_ptr;
  int i, j;
  int date_lang_id;

  assert (src_value != (DB_VALUE *) NULL);
  assert (result_str != (DB_VALUE *) NULL);
  assert (date_lang != (DB_VALUE *) NULL);

  if (date_lang == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  /* now return null */
  if (DB_IS_NULL (src_value))
    {
      DB_MAKE_NULL (result_str);
      return error_status;
    }

  switch (DB_VALUE_TYPE (src_value))
    {
    case DB_TYPE_NUMERIC:
      tmp_buf = numeric_db_value_print ((DB_VALUE *) src_value);
      cs = (char *) db_private_alloc (NULL, strlen (tmp_buf) + 1);
      if (cs == NULL)
	{
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  return error_status;
	}
      strcpy (cs, tmp_buf);
      break;

    case DB_TYPE_INTEGER:
      sprintf (tmp_str, "%d", DB_GET_INTEGER (src_value));
      cs = (char *) db_private_alloc (NULL, strlen (tmp_str) + 1);
      if (cs == NULL)
	{
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  return error_status;
	}
      strcpy (cs, tmp_str);
      break;

    case DB_TYPE_BIGINT:
      sprintf (tmp_str, "%lld", (long long) DB_GET_BIGINT (src_value));
      cs = (char *) db_private_alloc (NULL, strlen (tmp_str) + 1);
      if (cs == NULL)
	{
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  return error_status;
	}
      strcpy (cs, tmp_str);
      break;

    case DB_TYPE_SMALLINT:
      sprintf (tmp_str, "%d", DB_GET_SMALLINT (src_value));
      cs = (char *) db_private_alloc (NULL, strlen (tmp_str) + 1);
      if (cs == NULL)
	{
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  return error_status;
	}
      strcpy (cs, tmp_str);
      break;

    case DB_TYPE_FLOAT:
      sprintf (tmp_str, "%.6e", DB_GET_FLOAT (src_value));
      if (scientific_to_decimal_string (tmp_str, &cs) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_DOUBLE:
      sprintf (tmp_str, "%.15e", DB_GET_DOUBLE (src_value));
      if (scientific_to_decimal_string (tmp_str, &cs) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      break;

    default:
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  /*        Remove    'trailing zero' source string    */
  for (i = 0; i < strlen (cs); i++)
    {
      if (cs[i] == '.')
	{
	  i = strlen (cs);
	  i--;
	  while (cs[i] == '0')
	    {
	      i--;
	    }
	  if (cs[i] == '.')
	    {
	      cs[i] = '\0';
	    }
	  else
	    {
	      i++;
	      cs[i] = '\0';
	    }
	  break;
	}
    }

  if (format_str == NULL || (DB_GET_INT (date_lang) & 0x1))
    {
      /*    Caution: VARCHAR's Size        */
      DB_MAKE_VARCHAR (result_str, (ssize_t) strlen (cs), cs, strlen (cs));
      result_str->need_clear = true;
      return error_status;
    }
  else
    {
      date_lang_id = ((DB_GET_INT (date_lang) & 2) ?
		      INTL_LANG_ENGLISH : INTL_LANG_KOREAN);

      if (DB_IS_NULL (format_str))
	{
	  db_private_free_and_init (NULL, cs);
	  DB_MAKE_NULL (result_str);
	  return error_status;
	}

      /*    Format string type checking     */
      if (is_char_string (format_str))
	{
	  if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      db_private_free_and_init (NULL, cs);
	      return error_status;
	    }

	  format_str_ptr = DB_PULL_STRING (format_str);
	  last_format = &format_str_ptr[DB_GET_STRING_SIZE (format_str)];
	}
      else
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  db_private_free_and_init (NULL, cs);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) == 0)
	{
	  error_status = ER_QSTR_EMPTY_STRING;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  db_private_free_and_init (NULL, cs);
	  return error_status;
	}

      /*    Memory allocation for result                            */
      /*    size is bigger two times than strlen(format_str_ptr)    */
      /*        because of format 'C'(currency)                        */
      /*        'C' can be  expanded accoding to CODE_SET            */
      /*        +1 implies minus -                                     */
      res_string =
	(char *) db_private_alloc (NULL, strlen (format_str_ptr) * 2 + 1);
      if (res_string == NULL)
	{
	  db_private_free_and_init (NULL, cs);
	  return er_errid ();
	}

      res_ptr = res_string;

      /* Skip space, tab, CR     */
      while (strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      while (format_str_ptr != last_format)
	{
	  cur_format = get_number_token (format_str_ptr, &token_length,
					 last_format, &next_fsp);
	  switch (cur_format)
	    {
	    case N_FORMAT:
	      if (make_number_to_char (cs, format_str_ptr, &token_length,
				       &res_ptr) != NO_ERROR)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  db_private_free_and_init (NULL, cs);
		  db_private_free_and_init (NULL, res_string);
		  return error_status;
		}
	      /*    Remove space character between sign,curerency and number */
	      i = 0;
	      j = 0;
	      while (i < token_length)
		{
		  /*    chekc won(korean currency), yen(japanese currency)    */
		  /*
		     if(res_ptr[i]==0xa3 || res_ptr[i]==0xa1)
		     i+=2;
		   */
		  /*    patch for 'to_char(.01230,'sc99999.99999')' */
		  if (strlen (&res_ptr[i]) >= 2 &&
		      (!memcmp (&res_ptr[i], moneysymbols[0], 2) ||
		       !memcmp (&res_ptr[i], moneysymbols[1], 2)))
		    {
		      i += 2;
		    }
		  else if (res_ptr[i] == '$' || res_ptr[i] == '+' ||
			   res_ptr[i] == '-')
		    {
		      i += 1;
		    }
		  else if (res_ptr[i] == ' ')
		    {
		      while (res_ptr[i + j] == ' ')
			{
			  j++;
			}
		      while (i > 0)
			{
			  i--;
			  res_ptr[i + j] = res_ptr[i];
			  res_ptr[i] = ' ';
			}
		      break;
		    }
		  else
		    {
		      break;
		    }
		}
	      res_ptr += token_length;
	      break;
	    case N_SPACE:
	      strncpy (res_ptr, format_str_ptr, token_length);
	      res_ptr += token_length;
	      break;
	    case N_TEXT:
	      strncpy (res_ptr, (format_str_ptr + 1), token_length - 2);
	      res_ptr += token_length - 2;
	      break;
	    case N_INVALID:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      db_private_free_and_init (NULL, cs);
	      db_private_free_and_init (NULL, res_string);
	      return error_status;
	    case N_END:
	      *res_ptr = '\0';
	      break;
	    }

	  format_str_ptr = next_fsp;
	}

      *res_ptr = '\0';
    }

  /* Both format and src should end at same time     */
  if (format_str_ptr != last_format)
    {
      error_status = ER_QSTR_INVALID_FORMAT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      db_private_free_and_init (NULL, cs);
      db_private_free_and_init (NULL, res_string);

      return error_status;
    }

  DB_MAKE_VARCHAR (result_str, (ssize_t) strlen (res_string), res_string,
		   strlen (res_string));
  result_str->need_clear = true;
  db_private_free_and_init (NULL, cs);

  return error_status;
}

/*
 * make_number_to_char () -
 */
static int
make_number_to_char (char *num_string,
		     char *format_str, int *length, char **result_str)
{
  int flag_sign = 1;
  int leadingzero = false;
  char *res_str = *result_str;
  char *num, *format, *res;
  char *init_format = format_str;

  char format_end_char = init_format[*length];
  init_format[*length] = '\0';

  /* code for patch..     emm..   */
  if (strlen (format_str) == 5 && !strncasecmp (format_str, "seeee", 5))
    {
      return ER_FAILED;
    }
  else if (strlen (format_str) == 5 && !strncasecmp (format_str, "ceeee", 5))
    {
      return ER_FAILED;
    }
  else if (strlen (format_str) == 6 && !strncasecmp (format_str, "sceeee", 6))
    {
      return ER_FAILED;
    }

  /*              Check minus                     */
  if (*num_string == '-')
    {
      *res_str = '-';
      num_string++;
      res_str++;
      flag_sign = -1;
    }

  /*              Check sign                      */
  if (char_tolower (*format_str) == 's')
    {
      if (flag_sign == 1)
	{
	  *res_str = '+';
	  res_str++;
	}
      format_str++;
    }

  if (*format_str == '\0')
    {
      init_format[*length] = format_end_char;
      /* patch for format: '9999 s'   */
      *res_str = '\0';

      *length = strlen (*result_str);
      return NO_ERROR;
    }

  /*              Check currency          */
  if (char_tolower (*format_str) == 'c')
    {
      switch (lang_currency ())
	{
	case DB_CURRENCY_DOLLAR:
	  *res_str = '$';
	  break;
	case DB_CURRENCY_WON:
	  *res_str = moneysymbols[1][0];
	  res_str++;
	  *res_str = moneysymbols[1][1];
	  break;
	case DB_CURRENCY_YUAN:
	  *res_str = 'Y';
	  break;
	default:
	  return ER_FAILED;
	}

      res_str++;
      format_str++;
    }

  if (*format_str == '\0')
    {
      init_format[*length] = format_end_char;
      /* patch for format: '9999 s'   */
      *res_str = '\0';
      *length = strlen (*result_str);

      return NO_ERROR;
    }

  /* So far, format:'s','c' are settled   */
  if (*length > 4 && !strncasecmp (&init_format[*length - 4], "eeee", 4))
    {
      int cipher = 0;

      num = num_string;
      format = format_str;

      if (*num == '0')
	{
	  num++;
	  if (*num == '\0')
	    {
	      while (*format == '0' || *format == '9' || *format == ',')
		{
		  format++;
		}

	      if (*format == '.')
		{
		  *res_str = '0';
		  res_str++;

		  format++;

		  *res_str = '.';
		  res_str++;

		  while (1)
		    {
		      if (*format == '0' || *format == '9')
			{
			  *res_str = '0';
			  res_str++;
			  format++;
			}
		      else if (char_tolower (*format) == 'e')
			{
			  *res_str = '\0';
			  init_format[*length] = format_end_char;
			  make_scientific_notation (*result_str, cipher);
			  *length = strlen (*result_str);

			  return NO_ERROR;
			}
		      else
			{
			  return ER_FAILED;
			}
		    }
		}
	      else if (*format == 'e')
		{
		  *res_str = '0';
		  res_str++;
		  *res_str = '\0';
		  init_format[*length] = format_end_char;
		  make_scientific_notation (*result_str, cipher);
		  *length = strlen (*result_str);

		  return NO_ERROR;
		}
	      else
		{
		  return ER_FAILED;
		}
	    }
	  else if (*num == '.')
	    {
	      num++;
	      while (1)
		{
		  if (*num == '0')
		    {
		      cipher--;
		      num++;
		    }
		  else if (char_isdigit (*num))
		    {
		      cipher--;
		      break;
		    }
		  else if (char_tolower (*num) == 'e')
		    {
		      break;
		    }
		  else if (*num == '\0')
		    {
		      return ER_FAILED;
		    }
		  else
		    {
		      return ER_FAILED;
		    }
		}
	    }
	  else
	    {
	      return ER_FAILED;
	    }
	}
      else
	{
	  while (1)
	    {
	      if (char_isdigit (*num))
		{
		  cipher++;
		  num++;
		}
	      else if (*num == '.' || *num == '\0')
		{
		  cipher--;
		  break;
		}
	      else
		{
		  return ER_FAILED;
		}
	    }
	}

      while (*format == '0' || *format == '9' || *format == ',')
	{
	  format++;
	}

      if (*format != '.' && char_tolower (*format) != 'e')
	{
	  return ER_FAILED;
	}

      num = num_string;
      res = res_str;

      while (1)
	{
	  if ('0' < *num && *num <= '9')
	    {
	      *res = *num;
	      res++;
	      num++;
	      break;
	    }
	  else
	    {
	      num++;
	    }
	}

      if (char_tolower (*format) == 'e')
	{
	  *res = '\0';
	  if (*num == '.')
	    {
	      num++;
	      if (char_isdigit (*num) && *num - '0' > 4)
		{
		  roundoff (*result_str, 1, &cipher, (char *) NULL);
		}
	    }
	  else if (char_isdigit (*num))
	    {
	      if (char_isdigit (*num) && *num - '0' > 4)
		{
		  roundoff (*result_str, 1, &cipher, (char *) NULL);
		}
	    }
	  else if (*num == '\0')
	    {
	      /* do nothing */
	    }
	  else
	    {
	      return ER_FAILED;
	    }

	  /*      emm     */
	  init_format[*length] = format_end_char;
	  make_scientific_notation (*result_str, cipher);
	  *length = strlen (*result_str);

	  return NO_ERROR;
	}
      else
	{
	  *res = *format;
	  res++;
	  format++;
	}

      while (1)
	{
	  if (*format == '0' || *format == '9')
	    {
	      if (*num == '.')
		{
		  num++;
		  *res = *num;
		}
	      else if (*num == '\0')
		{
		  while (*format == '0' || *format == '9')
		    {
		      *res = '0';
		      format++;
		      res++;
		    }

		  if (char_tolower (*format) != 'e')
		    {
		      return ER_FAILED;
		    }

		  *res = '\0';
		  init_format[*length] = format_end_char;
		  make_scientific_notation (*result_str, cipher);
		  *length = strlen (*result_str);

		  return NO_ERROR;
		}
	      else
		{
		  *res = *num;
		}

	      format++;
	      res++;
	      num++;
	    }
	  else if (char_tolower (*format) == 'e')
	    {
	      if (strlen (format) > 4)
		{
		  return ER_FAILED;
		}

	      if (*num == '\0')
		{
		  *res = '\0';
		  init_format[*length] = format_end_char;
		  make_scientific_notation (*result_str, cipher);
		  *length = strlen (*result_str);

		  return NO_ERROR;
		}
	      else
		{
		  *res = '\0';
		  /*      patch                   */
		  if (*num == '.' && *(num + 1) - '0' > 4)
		    {
		      roundoff (*result_str, 1, &cipher, (char *) NULL);
		    }
		  if (*num - '0' > 4)
		    {
		      roundoff (*result_str, 1, &cipher, (char *) NULL);
		    }
		  /*      emm     */
		  init_format[*length] = format_end_char;
		  make_scientific_notation (*result_str, cipher);
		  *length = strlen (*result_str);

		  return NO_ERROR;
		}
	    }
	  else
	    {
	      return ER_FAILED;
	    }
	}
    }
  /* So far, format:scientific notation are settled       */

  /*              Check leading zero              */
  if (*format_str == '0')
    {
      leadingzero = true;
    }

  num = num_string;
  format = format_str;

  /*      Scan unitl '.' or '\0' of both num or format    */
  while (char_isdigit (*num))
    {
      num++;
    }

  while (*format == '0' || *format == '9' || *format == ',')
    {
      format++;
    }

  if (*format != '.' && *format != '\0')
    {
      return ER_FAILED;
    }

  /* '.' or '\0' is copied into middle or last position of res_string */
  *(res_str + (format - format_str)) = *format;
  res = res_str + (format - format_str);

  /*      num: .xxx       format: .xxx    */
  if (format == format_str && num == num_string)
    {
      ;
    }
  /*      num: .xxx       format: xxx.xxx */
  else if (format != format_str && num == num_string)
    {
      if (leadingzero == true)
	{
	  while (format != format_str)
	    {
	      format--;

	      if (*format == '9' || *format == '0')
		{
		  *(res_str + (format - format_str)) = '0';
		}
	      else if (*format == ',')
		{
		  *(res_str + (format - format_str)) = ',';
		}
	      else
		{
		  return ER_FAILED;
		}
	    }
	}
      else
	{
	  while (format != format_str)
	    {
	      format--;
	      *(res_str + (format - format_str)) = ' ';
	    }
	}
    }
  /*      num: xxx.xxx    format: .xxx    */
  else if (format == format_str && num != num_string)
    {
      while (num != num_string)
	{
	  num--;
	  if (*num != '0')
	    {
	      /*      Make num be different from num_string   */
	      num = num_string + 1;
	      break;
	    }
	}
    }
  /*      num: xxx.xxx    format: xxx.xxx */
  else
    {
      format--;
      num--;
      /*      if      size of format string is 1              */
      if (format == format_str)
	{
	  *res_str = *num;
	}
      else
	{
	  while (format != format_str)
	    {
	      if (*format == ',')
		{
		  *(res_str + (format - format_str)) = *format;
		}
	      else if ((*format == '9' || *format == '0')
		       && num != num_string)
		{
		  *(res_str + (format - format_str)) = *num;
		  num--;
		}
	      else
		{
		  *(res_str + (format - format_str)) = *num;
		  if (leadingzero == true)
		    {
		      while (format != format_str)
			{
			  format--;
			  if (*format == '9' || *format == '0')
			    {
			      *(res_str + (format - format_str)) = '0';
			    }
			  else if (*format == ',')
			    {
			      *(res_str + (format - format_str)) = ',';
			    }
			  else
			    {
			      return ER_FAILED;
			    }
			}
		    }
		  else
		    {
		      while (format != format_str)
			{
			  format--;
			  *(res_str + (format - format_str)) = ' ';
			}
		    }
		  break;
		}
	      format--;
	      if (format == format_str && num == num_string)
		{
		  *(res_str + (format - format_str)) = *num;
		}
	    }
	}
    }

  if (num != num_string)
    {
      int i;

      i = strlen (init_format) - 1;
      while (init_format != &init_format[i])
	{
	  if (init_format[i] == '.')
	    {
	      break;
	    }
	  else if (init_format[i] != '0' && init_format[i] != '9' &&
		   init_format[i] != 's' && init_format[i] != 'c' &&
		   init_format[i] != ',')
	    {
	      return ER_FAILED;
	    }
	  else
	    {
	      i--;
	    }
	}

      i = 0;
      while (i < *length)
	{
	  (*result_str)[i] = '#';
	  i++;
	}

      (*result_str)[*length] = '\0';
      init_format[*length] = format_end_char;

      return NO_ERROR;
    }
  /* So far, Left side of decimal point is settled        */

  while (char_isdigit (*num))
    {
      num++;
    }

  while (*format == '0' || *format == '9' || *format == ',')
    {
      format++;
    }

  if (*format != '.' && *format != '\0')
    {
      return ER_FAILED;
    }

  if (*format == '.' && *num == '.')
    {
      res++;
      format++;
      num++;

      while (*format != '\0')
	{
	  if ((*format == '9' || *format == '0') && *num != '\0')
	    {
	      *res = *num;
	      num++;
	      res++;
	    }
	  else
	    {
	      while (*format != '\0')
		{
		  if (*format == '9' || *format == '0')
		    {
		      *res = '0';
		    }
		  else
		    {
		      return ER_FAILED;
		    }

		  format++;
		  res++;
		}

	      *res = '\0';
	      break;
	    }

	  format++;
	}

      *res = '\0';
      if (*num != '\0')
	{
	  /* rounding     */
	  if (*num - '0' > 4)
	    {
	      if (roundoff (*result_str, 0, (int *) NULL, format_str)
		  != NO_ERROR)
		{
		  return ER_FAILED;
		}
	    }
	}
    }
  else if (*format == '.' && *num == '\0')
    {
      res++;
      format++;

      while (*format != '\0')
	{
	  if (*format == '9' || *format == '0')
	    {
	      *res = '0';
	    }
	  else
	    {
	      return ER_FAILED;
	    }

	  format++;
	  res++;
	}

      *res = '\0';
    }
  else if (*format == '\0' && *num == '.')
    {
      if (*(num + 1) - '0' > 4)
	{
	  if (roundoff (*result_str, 0, (int *) NULL, format_str) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	}
      /*      rounding        */
    }
  else if (*format == '\0' && *num == '\0')
    {
      /* Nothing      */
    }
  else
    {
      return ER_FAILED;
    }

  init_format[*length] = format_end_char;
  *length = strlen (*result_str);

  return NO_ERROR;
}

/*
 * make_scientific_notation () -
 */
static int
make_scientific_notation (char *src_string, int cipher)
{
  int leng = strlen (src_string);

  src_string[leng] = 'E';
  leng++;

  if (cipher >= 0)
    {
      src_string[leng] = '+';
    }
  else
    {
      src_string[leng] = '-';
      cipher *= (-1);
    }

  leng++;

  if (cipher > 99)
    {
      sprintf (&src_string[leng], "%d", cipher);
    }
  else
    {
      sprintf (&src_string[leng], "%02d", cipher);
    }

  return NO_ERROR;
}

/*
 * roundoff () -
 */
static int
roundoff (char *src_string, int flag, int *cipher, char *format)
{
  int loop_state = true;
  int is_overflow = false;
  char *res = &src_string[strlen (src_string)];
  char *for_ptr = NULL;
  int i;

  if (flag == 0)
    {
      for_ptr = &format[strlen (format)];
    }

  if (*src_string == '\0')
    {
      return ER_FAILED;
    }
  if (flag == 0 && *format == '\0')
    {
      return ER_FAILED;
    }

  res--;

  if (flag == 0)
    {
      for_ptr--;
    }

  while (loop_state)
    {
      if ('0' <= *res && *res <= '9')
	{
	  switch (*res - '0' + 1)
	    {
	    case 1:
	    case 2:
	    case 3:
	    case 4:
	    case 5:
	    case 6:
	    case 7:
	    case 8:
	    case 9:
	      *res = *res + 1;
	      loop_state = false;
	      break;

	    case 10:
	      *res = '0';
	      if (res == src_string)
		{
		  loop_state = false;
		  is_overflow = true;
		}
	      else
		{
		  res--;
		  if (flag == 0)
		    {
		      for_ptr--;
		    }
		}
	      break;
	    }
	}
      else if (*res == '.' || *res == ',')
	{
	  if (res == src_string)
	    {
	      loop_state = false;
	      is_overflow = true;
	    }
	  else
	    {
	      res--;
	      if (flag == 0)
		{
		  for_ptr--;
		}
	    }
	}
      else if (*res == ' ')
	{
	  if (flag == 0 && *for_ptr == ',')
	    {
	      *res = ',';
	      res--;
	      for_ptr--;
	    }

	  *res = '1';
	  loop_state = false;
	}
      else
	{			/* in case of sign, currency     */
	  loop_state = false;
	  is_overflow = true;
	}
    }

  if (is_overflow)
    {
      if (flag == 0)
	{			/* if decimal format    */
	  i = 0;

	  while (i < strlen (src_string))
	    {
	      src_string[i] = '#';
	      i++;
	    }

	  src_string[i] = '\0';
	}
      else
	{			/*      if scientific format    */
	  i = 0;

	  res = src_string;
	  while (!('0' <= *res && *res <= '9'))
	    {
	      res++;
	    }

	  while (i < strlen (res))
	    {
	      if (i == 0)
		{
		  res[i] = '1';
		}
	      else if (i == 1)
		{
		  res[i] = '.';
		}
	      else
		{
		  res[i] = '0';
		}
	      i++;
	    }

	  (*cipher)++;
	  res[i] = '\0';
	}
    }

  return NO_ERROR;
}

/*
 * scientific_to_decimal_string () -
 */
static int
scientific_to_decimal_string (char *src_string, char **scientific_str)
{
#define PLUS 1
#define MINUS 0
  int src_len = strlen (src_string);
  int sign = PLUS, exponent_sign = PLUS, cipher = 0;
  char *ptr = src_string;
  char *result_str;
  int i;
  int tmp_digit;

  while (char_isspace (*ptr))
    {
      ptr++;
    }

  if (*ptr == '+')
    {
      sign = PLUS;
      ptr++;
    }
  else if (*ptr == '-')
    {
      sign = MINUS;
      ptr++;
    }

  tmp_digit = 0;
  while (char_isdigit (*ptr))
    {
      tmp_digit = tmp_digit * 10 + (*ptr - '0');
      ptr++;
    }
  if (tmp_digit >= 10)
    {
      return ER_FAILED;
    }
  if (*ptr != '.')
    {
      return ER_FAILED;
    }
  ptr++;
  while (char_isdigit (*ptr))
    {
      ptr++;
    }
  if (*ptr == 'e' || *ptr == 'E')
    {
      ptr++;
    }
  else
    {
      return ER_FAILED;
    }

  if (*ptr == '+')
    {
      exponent_sign = PLUS;
    }
  else if (*ptr == '-')
    {
      exponent_sign = MINUS;
    }
  else
    {
      return ER_FAILED;
    }

  ptr++;
  for (; char_isdigit (*ptr); ptr++)
    {
      cipher = cipher * 10 + (*ptr - '0');
    }
  /* So far, one pass     */
  /* Fron now, two pass   */
  while (char_isspace (*ptr))
    {
      ptr++;
    }
  if (*ptr != '\0')
    {
      return ER_FAILED;
    }
  ptr = src_string;
  while (char_isspace (*ptr))
    {
      ptr++;
    }
  *scientific_str = (char *) db_private_alloc (NULL, src_len + cipher);
  if (*scientific_str == NULL)
    {
      return ER_FAILED;
    }
  /* patch for MemoryTrash   */
  for (i = 0; i < src_len + cipher; i++)
    {
      (*scientific_str)[i] = '\0';
    }

  result_str = *scientific_str;
  if (sign == MINUS)
    {
      *result_str = '-';
      result_str++;
      ptr++;
    }
  if (exponent_sign == PLUS)
    {
      i = 0;
      while (char_isdigit (*ptr))
	{
	  *result_str = *ptr;
	  (result_str)++;
	  ptr++;
	}
      *(result_str + cipher) = '.';
      ptr++;
      while (i < cipher || char_isdigit (*ptr))
	{
	  if (*result_str == '.')
	    {
	      (result_str)++;
	      continue;
	    }
	  else if (char_isdigit (*ptr))
	    {
	      *result_str = *ptr;
	      ptr++;
	    }
	  else
	    {
	      *result_str = '0';
	    }
	  (result_str)++;
	  i++;
	}
    }
  else
    {
      *result_str = '0';
      result_str++;
      *result_str = '.';
      result_str++;
      i = 0;
      while (i < cipher - 1)
	{
	  *result_str = '0';
	  result_str++;
	  i++;
	}
      while (char_isdigit (*ptr) || *ptr == '.')
	{
	  if (*ptr == '.')
	    {
	      ptr++;
	    }
	  *result_str = *ptr;
	  (result_str)++;
	  ptr++;
	}
    }
  *result_str = '\0';
  return NO_ERROR;
}

/*
 * to_number_next_state () -
 */
static int
to_number_next_state (int previous_state, int input_char)
{
  int state_table[7][7] = { {4, 5, 2, 3, -1, 6, -1},
  {4, 5, -1, 3, -1, 6, -1},
  {4, 5, -1, -1, -1, 6, -1},
  {4, 4, -1, -1, 4, 6, 7},
  {5, 5, -1, -1, 5, 6, 7},
  {6, 6, -1, -1, 6, -1, 7},
  {0, 0, 0, 0, 0, 0, 0}
  };
  int state;
  if (previous_state == -1)
    return -1;
  switch (char_tolower (input_char))
    {
    case '0':
      state = state_table[previous_state - 1][0];
      break;
    case '9':
      state = state_table[previous_state - 1][1];
      break;
    case 's':
      state = state_table[previous_state - 1][2];
      break;
    case 'c':
      state = state_table[previous_state - 1][3];
      break;
    case ',':
      state = state_table[previous_state - 1][4];
      break;
    case '.':
      state = state_table[previous_state - 1][5];
      break;
    default:
      state = state_table[previous_state - 1][6];
      break;
    }
  return state;
}

/*
 * to_number_next_state () -
 * Note: assume precision and scale are correct
 */
static int
make_number (char *src, char *last_src, char *token, int
	     *token_length, DB_VALUE * r, int precision, int scale)
{
  int error_status = NO_ERROR;
  int state = 1;
  int i, j, k;
  char result_str[DB_MAX_NUMERIC_PRECISION + 2];
  char *res_ptr;

  result_str[0] = '\0';
  result_str[DB_MAX_NUMERIC_PRECISION] = '\0';
  result_str[DB_MAX_NUMERIC_PRECISION + 1] = '\0';
  *token_length = 0;

  while (state != 7 && src < last_src)
    {
      switch (to_number_next_state (state, *token))
	{
	case 1:		/* Not reachable state  */
	  break;
	case 2:
	  if (*src == '-')
	    {
	      strncat (result_str, src, 1);
	      src++;
	      (*token_length)++;
	      token++;
	      state = 2;
	    }
	  else if (*src == '+')
	    {
	      src++;
	      (*token_length)++;
	      token++;
	      state = 2;
	    }
	  else
	    {
	      return ER_QSTR_MISMATCHING_ARGUMENTS;
	    }
	  break;
	case 3:
	  switch (lang_id ())
	    {
	    case INTL_LANG_ENGLISH:
	      if (*src == '$')
		{
		  src++;
		  (*token_length)++;
		  token++;
		}
	      break;
	    case INTL_LANG_KOREAN:
	      if (!memcmp (moneysymbols[1], src, 2))
		{
		  src += 2;
		  (*token_length) += 2;
		  token += 1;
		}
	      break;
	    }
	  state = 3;
	  break;
	case 4:
	case 5:
	  if (*src == '-')
	    {
	      strncat (result_str, src, 1);
	      src++;
	      (*token_length)++;
	    }
	  j = 0;
	  k = 0;
	  while (token[j] == '0' || token[j] == '9' || token[j] == ',')
	    {
	      j++;
	    }
	  while ((&src[k] < last_src) &&
		 (char_isdigit (src[k]) || src[k] == ','))
	    {
	      k++;
	    }
	  i = j;

	  if (k > DB_MAX_NUMERIC_PRECISION)
	    {
	      return ER_NUM_OVERFLOW;
	    }
	  if (k > 0)
	    {
	      k--;
	    }
	  j--;
	  while (k > 0 && j > 0)
	    {
	      if (token[j] == ',' && src[k] != ',')
		{
		  return ER_QSTR_MISMATCHING_ARGUMENTS;
		}
	      k--;
	      j--;
	    }

	  if (k != 0)
	    {			/* format = '99' && src = '4444' */
	      return ER_QSTR_MISMATCHING_ARGUMENTS;
	    }
	  /* patch select to_number('30','9,9') from dual;                */
	  if ((src[k] == ',' && token[j] != ',') ||
	      (token[j] == ',' && src[k] != ','))
	    {
	      return ER_QSTR_MISMATCHING_ARGUMENTS;
	    }
	  if (j > 0)
	    {
	      j = 0;
	    }
	  while (src < last_src && (char_isdigit (*src) || *src == ','))
	    {
	      if (*src != ',')
		{
		  strncat (result_str, src, 1);
		}
	      (*token_length)++;
	      src++;
	    }
	  token = token + i;
	  state = 4;
	  break;
	case 6:
	  token++;
	  if (*src == '.')
	    {
	      strncat (result_str, src, 1);
	      src++;
	      (*token_length)++;
	      while (src < last_src && char_isdigit (*src))
		{
		  if (*token == '0' || *token == '9')
		    {
		      strncat (result_str, src, 1);
		      token++;
		      src++;
		      (*token_length)++;
		    }
		  else
		    {
		      return ER_QSTR_MISMATCHING_ARGUMENTS;
		    }
		}
	    }
	  while (*token == '0' || *token == '9')
	    {
	      token++;
	    }
	  state = 6;
	  break;
	case 7:
	  state = 7;
	  break;
	case -1:
	  return ER_QSTR_MISMATCHING_ARGUMENTS;
	}			/* switch       */
    }				/* while        */

  /* For Scientific notation      */
  if (strlen (token) >= 4 && strncasecmp (token, "eeee", 4) == 0 &&
      char_tolower (*src) == 'e' && (*(src + 1) == '+' || *(src + 1) == '-'))
    {
      strncat (result_str, src, 2);
      src += 2;
      (*token_length) += 2;

      while (src < last_src && char_isdigit (*src))
	{
	  strncat (result_str, src, 1);
	  src += 1;
	  (*token_length) += 1;
	}

      if (scientific_to_decimal_string (result_str, &res_ptr) != NO_ERROR)
	{
	  return ER_QSTR_MISMATCHING_ARGUMENTS;
	  /* This line needs to be modified to reflect appropriate error */
	}

      /*
       * modify result_str to contain correct string value with respect to
       * the given precision and scale.
       */
      strncpy (result_str, res_ptr, sizeof (result_str) - 1);
      db_private_free_and_init (NULL, res_ptr);

      error_status = adjust_precision (result_str, precision, scale);
      if (error_status == DOMAIN_OVERFLOW)
	{
	  return ER_NUM_OVERFLOW;
	}
      if (error_status != NO_ERROR ||
	  numeric_coerce_string_to_num (result_str, r) != NO_ERROR)
	{
	  /*       patch for to_number('-1.23e+03','9.99eeee')    */
	  return ER_QSTR_MISMATCHING_ARGUMENTS;
	}
      /* old comment
         DB_MAKE_NUMERIC(r,num,precision,scale);
       */
    }
  else
    {
      /*
       * modify result_str to contain correct string value with respect to
       * the given precision and scale.
       */
      error_status = adjust_precision (result_str, precision, scale);
      if (error_status == DOMAIN_OVERFLOW)
	{
	  return ER_NUM_OVERFLOW;
	}
      if (error_status != NO_ERROR ||
	  numeric_coerce_string_to_num (result_str, r) != NO_ERROR)
	{
	  return ER_QSTR_MISMATCHING_ARGUMENTS;
	}
    }

  return error_status;
}

/*
 * get_number_token () -
 */
static int
get_number_token (char *fsp, int *length,
		  char *last_position, char **next_fsp)
{
  *length = 0;
  if (fsp == last_position)
    {
      return N_END;
    }
  switch (char_tolower (fsp[*length]))
    {
    case 'c':
    case 's':
      if (fsp[*length + 1] == ',')
	{
	  return N_INVALID;
	}

      if ((char_tolower (fsp[*length + 1]) == 'c' ||
	   char_tolower (fsp[*length + 1]) == 's') && fsp[*length + 2] == ',')
	{
	  return N_INVALID;
	}

    case '9':
    case '0':
    case '.':
      while (fsp[*length] == '9' || fsp[*length] == '0' ||
	     char_tolower (fsp[*length]) == 's' ||
	     char_tolower (fsp[*length]) == 'c' ||
	     fsp[*length] == '.' || fsp[*length] == ',')
	{
	  *length += 1;
	}

      *next_fsp = &fsp[*length];
      if (strlen (*next_fsp) >= 4 && !strncasecmp (*next_fsp, "eeee", 4))
	{
	  *length += 4;
	  *next_fsp = &fsp[*length];
	}
      return N_FORMAT;

    case ' ':
    case '\t':
    case '\n':
      while (last_position != &fsp[*length]
	     && (fsp[*length] == ' ' || fsp[*length] == '\t'
		 || fsp[*length] == '\n'))
	{
	  *length += 1;
	}
      *next_fsp = &fsp[*length];
      return N_SPACE;

    case '"':
      *length += 1;
      while (fsp[*length] != '"')
	{
	  if (&fsp[*length] == last_position)
	    {
	      return N_INVALID;
	    }
	  *length += 1;
	}
      *length += 1;
      *next_fsp = &fsp[*length];
      return N_TEXT;

    default:
      return N_INVALID;
    }
}

/*
 * get_number_format () -
 */
static int
get_next_format (unsigned char *sp,
		 DB_TYPE str_type,
		 int *format_length, unsigned char **next_pos)
{
  /* sp : start position          */
  *format_length = 0;

  switch (char_tolower (*sp))
    {
    case 'y':
      if ((str_type == DB_TYPE_DATE || str_type == DB_TYPE_TIMESTAMP
	   || str_type == DB_TYPE_DATETIME)
	  && qstr_length (sp) >= 4
	  && strncasecmp ((const char *) (void *) sp, "yyyy", 4) == 0)
	{
	  *format_length += 4;
	  *next_pos = sp + *format_length;
	  return DT_YYYY;
	}
      else if ((str_type == DB_TYPE_DATE || str_type == DB_TYPE_TIMESTAMP
		|| str_type == DB_TYPE_DATETIME)
	       && qstr_length (sp) >= 2
	       && strncasecmp ((const char *) (void *) sp, "yy", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_YY;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'd':
      if ((str_type == DB_TYPE_DATE || str_type == DB_TYPE_TIMESTAMP
	   || str_type == DB_TYPE_DATETIME)
	  && qstr_length (sp) >= 2
	  && strncasecmp ((const char *) (void *) sp, "dd", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_DD;
	}
      else if ((str_type == DB_TYPE_DATE || str_type == DB_TYPE_TIMESTAMP
		|| str_type == DB_TYPE_DATETIME)
	       && qstr_length (sp) >= 2
	       && strncasecmp ((const char *) (void *) sp, "dy", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_DY;
	}
      else if ((str_type == DB_TYPE_DATE || str_type == DB_TYPE_TIMESTAMP
		|| str_type == DB_TYPE_DATETIME)
	       && qstr_length (sp) >= 3
	       && strncasecmp ((const char *) (void *) sp, "day", 3) == 0)
	{
	  *format_length += 3;
	  *next_pos = sp + *format_length;
	  return DT_DAY;
	}
      else
	{
	  *format_length += 1;
	  *next_pos = sp + *format_length;
	  return DT_D;
	}

    case 'c':
      if ((str_type == DB_TYPE_DATE || str_type == DB_TYPE_TIMESTAMP
	   || str_type == DB_TYPE_DATETIME)
	  && qstr_length (sp) >= 2
	  && strncasecmp ((const char *) (void *) sp, "cc", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_CC;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'q':
      if ((str_type == DB_TYPE_DATE || str_type == DB_TYPE_TIMESTAMP
	   || str_type == DB_TYPE_DATETIME)
	  && qstr_length (sp) >= 1
	  && strncasecmp ((const char *) (void *) sp, "q", 1) == 0)
	{
	  *format_length += 1;
	  *next_pos = sp + *format_length;
	  return DT_Q;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'm':
      if ((str_type == DB_TYPE_DATE || str_type == DB_TYPE_TIMESTAMP
	   || str_type == DB_TYPE_DATETIME)
	  && qstr_length (sp) >= 2
	  && strncasecmp ((const char *) (void *) sp, "mm", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_MM;
	}
      else if ((str_type == DB_TYPE_DATE || str_type == DB_TYPE_TIMESTAMP
		|| str_type == DB_TYPE_DATETIME)
	       && qstr_length (sp) >= 5
	       && strncasecmp ((const char *) (void *) sp, "month", 5) == 0)
	{
	  *format_length += 5;
	  *next_pos = sp + *format_length;
	  return DT_MONTH;
	}
      else if ((str_type == DB_TYPE_DATE || str_type == DB_TYPE_TIMESTAMP
		|| str_type == DB_TYPE_DATETIME)
	       && qstr_length (sp) >= 3
	       && strncasecmp ((const char *) (void *) sp, "mon", 3) == 0)
	{
	  *format_length += 3;
	  *next_pos = sp + *format_length;
	  return DT_MON;
	}
      else if ((str_type == DB_TYPE_TIME || str_type == DB_TYPE_TIMESTAMP
		|| str_type == DB_TYPE_DATETIME)
	       && qstr_length (sp) >= 2
	       && strncasecmp ((const char *) (void *) sp, "mi", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_MI;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'a':
      if ((str_type == DB_TYPE_TIME || str_type == DB_TYPE_TIMESTAMP
	   || str_type == DB_TYPE_DATETIME)
	  && qstr_length (sp) >= 2
	  && strncasecmp ((const char *) (void *) sp, "am", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_AM;
	}
      else if ((str_type == DB_TYPE_TIME || str_type == DB_TYPE_TIMESTAMP
		|| str_type == DB_TYPE_DATETIME)
	       && qstr_length (sp) >= 4
	       && strncasecmp ((const char *) (void *) sp, "a.m.", 4) == 0)
	{
	  *format_length += 4;
	  *next_pos = sp + *format_length;
	  return DT_A_M;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'p':
      if ((str_type == DB_TYPE_TIME || str_type == DB_TYPE_TIMESTAMP
	   || str_type == DB_TYPE_DATETIME)
	  && qstr_length (sp) >= 2
	  && strncasecmp ((const char *) (void *) sp, "pm", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_PM;
	}
      else if ((str_type == DB_TYPE_TIME || str_type == DB_TYPE_TIMESTAMP
		|| str_type == DB_TYPE_DATETIME)
	       && qstr_length (sp) >= 4
	       && strncasecmp ((const char *) (void *) sp, "p.m.", 4) == 0)
	{
	  *format_length += 4;
	  *next_pos = sp + *format_length;
	  return DT_P_M;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'h':
      if ((str_type == DB_TYPE_TIME || str_type == DB_TYPE_TIMESTAMP
	   || str_type == DB_TYPE_DATETIME)
	  && qstr_length (sp) >= 4
	  && strncasecmp ((const char *) (void *) sp, "hh24", 4) == 0)
	{
	  *format_length += 4;
	  *next_pos = sp + *format_length;
	  return DT_HH24;
	}
      else if ((str_type == DB_TYPE_TIME || str_type == DB_TYPE_TIMESTAMP
		|| str_type == DB_TYPE_DATETIME)
	       && qstr_length (sp) >= 4
	       && strncasecmp ((const char *) (void *) sp, "hh12", 4) == 0)
	{
	  *format_length += 4;
	  *next_pos = sp + *format_length;
	  return DT_HH12;
	}
      else if ((str_type == DB_TYPE_TIME || str_type == DB_TYPE_TIMESTAMP
		|| str_type == DB_TYPE_DATETIME)
	       && qstr_length (sp) >= 2
	       && strncasecmp ((const char *) (void *) sp, "hh", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_HH;
	}
      else
	{
	  return DT_INVALID;
	}

    case 's':
      if ((str_type == DB_TYPE_TIME || str_type == DB_TYPE_TIMESTAMP
	   || str_type == DB_TYPE_DATETIME)
	  && qstr_length (sp) >= 2
	  && strncasecmp ((const char *) (void *) sp, "ss", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_SS;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'f':
      if (str_type == DB_TYPE_DATETIME
	  && qstr_length (sp) >= 2
	  && strncasecmp ((const char *) (void *) sp, "ff", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_MS;
	}
      else
	{
	  return DT_INVALID;
	}

    case '"':
      *format_length += 1;
      while (sp[*format_length] != '"')
	{
	  if (!PRM_SINGLE_BYTE_COMPARE && IS_KOREAN (sp[*format_length]))
	    {
	      *format_length += 2;
	    }
	  else
	    {
	      if (sp[*format_length] == '\0')
		{
		  return DT_INVALID;
		}
	      *format_length += 1;
	    }
	}
      *format_length += 1;
      *next_pos = &sp[*format_length];
      return DT_TEXT;

    case '-':
    case '/':
    case ',':
    case '.':
    case ';':
    case ':':
    case ' ':
    case '\t':
    case '\n':
      *format_length += 1;
      *next_pos = sp + *format_length;
      return DT_PUNCTUATION;

    default:
      return DT_INVALID;
    }
}

/*
 * qstr_length () -
 */
static int
qstr_length (unsigned char *sp)
{
  int len = 0;

  while (*sp != '\0')
    {
      len++;
      sp = sp + 1;
    }

  return len;
}

/*
 * get_cur_year () -
 */
static int
get_cur_year (void)
{
  time_t tloc;
  struct tm *tm;

  if (time (&tloc) == -1)
    {
      return -1;
    }

  tm = localtime (&tloc);
  if (tm == NULL)
    {
      return -1;
    }

  return tm->tm_year + 1900;
}

/*
 * get_cur_month () -
 */
static int
get_cur_month (void)
{
  time_t tloc;
  struct tm *tm;

  if (time (&tloc) == -1)
    {
      return -1;
    }

  tm = localtime (&tloc);
  if (tm == NULL)
    {
      return -1;
    }

  return tm->tm_mon + 1;
}

/*
 * get_day () -
 */
int
get_day (int month, int day, int year)
{
  return day_of_week (julian_encode (month, day, year));
}

/*
 * is_valid_date () -
 */
static int
is_valid_date (int month, int day, int year, int day_of_the_week)
{
  int julian_date;
  int test_month, test_day, test_year, test_day_of_the_week;

  /*
   * Now encode it and then decode it again and see if we get the same
   * day; if not, it was a bogus specification, like 2/29 on a non-leap
   * year.
   */
  julian_date = julian_encode (month, day, year);
  julian_decode (julian_date, &test_month, &test_day, &test_year,
		 &test_day_of_the_week);

  if (month == test_month && day == test_day && year == test_year)
    {
      if (day_of_the_week == test_day_of_the_week || day_of_the_week == -1)
	{
	  return true;
	}
      else
	{
	  return false;
	}
    }
  else
    {
      return false;
    }
}

/*
 * db_format () -
 */
int
db_format (const DB_VALUE * value, const DB_VALUE * decimals,
	   DB_VALUE * result)
{
  DB_TYPE arg1_type, arg2_type;
  int error = NO_ERROR;
  int ndec = 0, i, j;
  const char *integer_format_max =
    "99,999,999,999,999,999,999,999,999,999,999,999,999";
  char format[128];
  DB_VALUE format_val, date_lang_val, trim_charset, formatted_val,
    numeric_val, trimmed_val;
  const DB_VALUE *num_dbval_p = NULL;

  arg1_type = DB_VALUE_DOMAIN_TYPE (value);
  arg2_type = DB_VALUE_DOMAIN_TYPE (decimals);

  if (arg1_type == DB_TYPE_NULL || DB_IS_NULL (value)
      || arg2_type == DB_TYPE_NULL || DB_IS_NULL (decimals))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  DB_MAKE_NULL (&formatted_val);
  DB_MAKE_NULL (&trimmed_val);

  if (arg2_type == DB_TYPE_INTEGER)
    {
      ndec = DB_GET_INT (decimals);
    }
  else if (arg2_type == DB_TYPE_SHORT)
    {
      ndec = DB_GET_SHORT (decimals);
    }
  else if (arg2_type == DB_TYPE_BIGINT)
    {
      DB_BIGINT bi = DB_GET_BIGINT (decimals);
      if (bi > INT_MAX || bi < 0)
	{
	  goto invalid_argument_error;
	}
      ndec = (int) bi;
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      return ER_FAILED;
    }

  if (ndec < 0)
    {
      goto invalid_argument_error;
    }
  /* 30 is the decimal limit for formating floating points with this function,
     in mysql */
  if (ndec > 30)
    {
      ndec = 30;
    }

  switch (arg1_type)
    {
    case DB_TYPE_VARCHAR:
    case DB_TYPE_VARNCHAR:
    case DB_TYPE_CHAR:
    case DB_TYPE_NCHAR:
      {
	char *c;
	int len, dot = 0;
	/* Trim first because the input string can be given like below:
	 *  - ' 1.1 ', '1.1 ', ' 1.1'
	 */
	db_make_null (&trim_charset);
	error = db_string_trim (BOTH, &trim_charset, value, &trimmed_val);
	if (error != NO_ERROR)
	  {
	    return error;
	  }

	c = DB_GET_CHAR (&trimmed_val, &len);
	if (c == NULL)
	  {
	    goto invalid_argument_error;
	  }

	for (i = 0; i < len; i++)
	  {
	    if (c[i] == '.')
	      {
		dot++;
		continue;
	      }
	    if (!char_isdigit (c[i]))
	      {
		goto invalid_argument_error;
	      }
	  }
	if (dot > 1)
	  {
	    goto invalid_argument_error;
	  }

	error = numeric_coerce_string_to_num (c, &numeric_val);
	if (error != NO_ERROR)
	  {
	    pr_clear_value (&trimmed_val);
	    return error;
	  }

	num_dbval_p = &numeric_val;
	pr_clear_value (&trimmed_val);
      }
      break;

    case DB_TYPE_MONETARY:
      {
	double d = db_value_get_monetary_amount_as_double (value);
	db_make_double (&numeric_val, d);
	num_dbval_p = &numeric_val;
      }
      break;

    case DB_TYPE_SHORT:
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
    case DB_TYPE_FLOAT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_NUMERIC:
      num_dbval_p = value;
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_DATATYPE, 0);
      return ER_FAILED;
    }

  /* Make format string. */
  i = snprintf (format, sizeof (format) - 1, "%s", integer_format_max);
  if (ndec > 0)
    {
      format[i++] = '.';
      for (j = 0; j < ndec; j++)
	{
	  format[i++] = '9';
	}
      format[i] = '\0';
    }

  db_make_string (&format_val, format);

  /* Make a dummy DATE_LANG value. We only accept English. */
  db_make_int (&date_lang_val, 2);

  error = number_to_char (num_dbval_p, &format_val, &date_lang_val,
			  &formatted_val);
  if (error == NO_ERROR)
    {
      /* number_to_char function returns a string with leading empty characters.
       * So, we need to remove them.
       */
      db_make_null (&trim_charset);
      error = db_string_trim (LEADING, &trim_charset, &formatted_val, result);

      pr_clear_value (&formatted_val);
    }

  return error;

invalid_argument_error:
  if (!DB_IS_NULL (&trimmed_val))
    {
      pr_clear_value (&trimmed_val);
    }
  if (!DB_IS_NULL (&formatted_val))
    {
      pr_clear_value (&formatted_val);
    }

  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
  return ER_FAILED;
}

/*
 * db_string_reverse () - reverse the source DB_VALUE string
 *
 *   return:
 *   src_str(in): source DB_VALUE string
 *   result_str(in/out): result DB_VALUE string
 */
int
db_string_reverse (const DB_VALUE * src_str, DB_VALUE * result_str)
{
  int error_status = NO_ERROR;
  DB_TYPE str_type;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_str != (DB_VALUE *) NULL);
  assert (result_str != (DB_VALUE *) NULL);

  /*
   *  Categorize the two input parameters and check for errors.
   *    Verify that the parameters are both character strings.
   */

  str_type = DB_VALUE_DOMAIN_TYPE (src_str);
  if (DB_IS_NULL (src_str))
    {
      DB_MAKE_NULL (result_str);
    }
  else if (!QSTR_IS_ANY_CHAR (str_type))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
    }
  /*
   *  If the input parameters have been properly validated, then
   *  we are ready to operate.
   */
  else
    {
      error_status = db_value_clone ((DB_VALUE *) src_str, result_str);
      if (error_status == NO_ERROR)
	{
	  intl_reverse_string ((unsigned char *) DB_PULL_STRING (src_str),
			       (unsigned char *) DB_PULL_STRING (result_str),
			       DB_GET_STRING_LENGTH (src_str),
			       DB_GET_STRING_SIZE (src_str),
			       (INTL_CODESET)
			       DB_GET_STRING_CODESET (src_str));
	}
    }

  return error_status;
}

/*
 * add_and_normalize_date_time ()
 *
 * Arguments: date & time values to modify,
 *	      date & time amounts to add
 *
 * Returns: NO_ERROR/ER_FAILED
 *
 * Errors:
 *
 * Note:
 *    transforms all values in a correct interval (h: 0..23, m: 0..59, etc)
 */
int
add_and_normalize_date_time (int *year, int *month,
			     int *day, int *hour,
			     int *minute, int *second,
			     int *millisecond, DB_BIGINT y, DB_BIGINT m,
			     DB_BIGINT d, DB_BIGINT h, DB_BIGINT mi,
			     DB_BIGINT s, DB_BIGINT ms)
{
  DB_BIGINT days[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  DB_BIGINT i;
  DB_BIGINT _y, _m, _d, _h, _mi, _s, _ms;
  DB_BIGINT old_day = *day;

  _y = *year;
  _m = *month;
  _d = *day;
  _h = *hour;
  _mi = *minute;
  _s = *second;
  _ms = *millisecond;

  _y += y;
  _m += m;
  _d += d;
  _h += h;
  _mi += mi;
  _s += s;
  _ms += ms;

  /* just years and/or months case */
  if (d == 0 && h == 0 && mi == 0 && s == 0 && ms == 0 && (m > 0 || y > 0))
    {
      if (_m % 12 == 0)
	{
	  _y += (_m - 12) / 12;
	  _m = 12;
	}
      else
	{
	  _y += _m / 12;
	  _m %= 12;
	}

      days[2] = LEAP (_y) ? 29 : 28;

      if (old_day > days[_m])
	{
	  _d = days[_m];
	}

      goto set_and_return;
    }

  /* time */
  _s += _ms / 1000;
  _ms %= 1000;

  _mi += _s / 60;
  _s %= 60;

  _h += _mi / 60;
  _mi %= 60;

  _d += _h / 24;
  _h %= 24;

  /* date */
  if (_m > 12)
    {
      _y += _m / 12;
      _m %= 12;

      if (_m == 0)
	{
	  _m = 1;
	}
    }

  days[2] = LEAP (_y) ? 29 : 28;

  if (_d > days[_m])
    {
      /* rewind to 1st january */
      for (i = 1; i < _m; i++)
	{
	  _d += days[i];
	}
      _m = 1;

      /* days for years */
      while (_d >= 366)
	{
	  days[2] = LEAP (_y) ? 29 : 28;
	  _d -= (days[2] == 29) ? 366 : 365;
	  _y++;
	  if (_y > 9999)
	    {
	      goto set_and_return;
	    }
	}

      /* days within a year */
      days[2] = LEAP (_y) ? 29 : 28;
      for (_m = 1;; _m++)
	{
	  if (_d <= days[_m])
	    {
	      break;
	    }
	  _d -= days[_m];
	}
    }

  if (_m == 0)
    {
      _m = 1;
    }
  if (_d == 0)
    {
      _d = 1;
    }

set_and_return:

  if (_y >= 10000 || _y <= 0)
    {
      return ER_FAILED;
    }

  *year = (int) _y;
  *month = (int) _m;
  *day = (int) _d;
  *hour = (int) _h;
  *minute = (int) _mi;
  *second = (int) _s;
  *millisecond = (int) _ms;

  return NO_ERROR;
}

/*
 * sub_and_normalize_date_time ()
 *
 * Arguments: date & time values to modify,
 *	      date & time amounts to subtract
 *
 * Returns: NO_ERROR/ER_FAILED
 *
 * Errors:
 *
 * Note:
 *    transforms all values in a correct interval (h: 0..23, m: 0..59, etc)
 */
int
sub_and_normalize_date_time (int *year, int *month,
			     int *day, int *hour,
			     int *minute, int *second,
			     int *millisecond, DB_BIGINT y, DB_BIGINT m,
			     DB_BIGINT d, DB_BIGINT h, DB_BIGINT mi,
			     DB_BIGINT s, DB_BIGINT ms)
{
  DB_BIGINT days[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  DB_BIGINT i;
  DB_BIGINT old_day = *day;
  DB_BIGINT _y, _m, _d, _h, _mi, _s, _ms;

  _y = *year;
  _m = *month;
  _d = *day;
  _h = *hour;
  _mi = *minute;
  _s = *second;
  _ms = *millisecond;

  _y -= y;
  _m -= m;
  _d -= d;
  _h -= h;
  _mi -= mi;
  _s -= s;
  _ms -= ms;

  /* time */
  _s += _ms / 1000;
  _ms %= 1000;
  if (_ms < 0)
    {
      _ms += 1000;
      _s--;
    }

  _mi += _s / 60;
  _s %= 60;
  if (_s < 0)
    {
      _s += 60;
      _mi--;
    }

  _h += _mi / 60;
  _mi %= 60;
  if (_mi < 0)
    {
      _mi += 60;
      _h--;
    }

  _d += _h / 24;
  _h %= 24;
  if (_h < 0)
    {
      _h += 24;
      _d--;
    }

  if (_d == 0)
    {
      _m--;

      if (_m == 0)
	{
	  _y--;
	  days[2] = LEAP (_y) ? 29 : 28;
	  _m = 12;
	}
      _d = days[_m];
    }

  if (_m == 0)
    {
      _y--;
      days[2] = LEAP (_y) ? 29 : 28;
      _m = 12;
    }

  /* date */
  if (_m < 0)
    {
      _y += (_m / 12);
      if (_m % 12 == 0)
	{
	  _m = 1;
	}
      else
	{
	  _m %= 12;
	  if (_m < 0)
	    {
	      _m += 12;
	      _y--;
	    }
	}
    }

  /* just years and/or months case */
  if (d == 0 && h == 0 && mi == 0 && s == 0 && ms == 0 && (m > 0 || y > 0))
    {
      if (_m <= 0)
	{
	  _y += (_m / 12);
	  if (_m % 12 == 0)
	    {
	      _m = 1;
	    }
	  else
	    {
	      _m %= 12;
	      if (_m <= 0)
		{
		  _m += 12;
		  _y--;
		}
	    }
	}

      days[2] = LEAP (_y) ? 29 : 28;

      if (old_day > days[_m])
	{
	  _d = days[_m];
	}

      goto set_and_return;
    }

  days[2] = LEAP (_y) ? 29 : 28;

  if (_d > days[_m] || _d < 0)
    {
      /* rewind to 1st january */
      for (i = 1; i < _m; i++)
	{
	  _d += days[i];
	}
      _m = 1;

      /* days for years */
      while (_d < 0)
	{
	  _y--;
	  if (_y < 0)
	    {
	      goto set_and_return;
	    }
	  days[2] = LEAP (_y) ? 29 : 28;
	  _d += (days[2] == 29) ? 366 : 365;
	}

      /* days within a year */
      days[2] = LEAP (_y) ? 29 : 28;
      for (_m = 1;; _m++)
	{
	  if (_d <= days[_m])
	    {
	      break;
	    }
	  _d -= days[_m];
	}
    }

  if (_m == 0)
    {
      _m = 1;
    }
  if (_d == 0)
    {
      _d = 1;
    }

set_and_return:

  if (_y >= 10000 || _y <= 0)
    {
      return ER_FAILED;
    }

  *year = (int) _y;
  *month = (int) _m;
  *day = (int) _d;
  *hour = (int) _h;
  *minute = (int) _mi;
  *second = (int) _s;
  *millisecond = (int) _ms;

  return NO_ERROR;
}

/*
 * db_date_add_sub_interval_days ()
 *
 * Arguments:
 *         date: starting date
 *         db_days: number of days to add
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *    Returns date + an interval of db_days days.
 */
static int
db_date_add_sub_interval_days (DB_VALUE * result, const DB_VALUE * date,
			       const DB_VALUE * db_days, bool is_add)
{
  int error_status = NO_ERROR;
  int days;
  DB_DATETIME db_datetime, *dt_p;
  DB_TIME db_time;
  DB_DATE db_date, *d_p;
  DB_TIMESTAMP db_timestamp, *ts_p;
  int is_dt = -1, is_d = -1, is_t = -1, is_timest = -1;
  DB_TYPE res_type;
  char *date_s = NULL, res_s[64];
  int y, m, d, h, mi, s, ms;
  int ret;
  char *res_final;

  res_type = DB_VALUE_DOMAIN_TYPE (date);
  if (res_type == DB_TYPE_NULL || DB_IS_NULL (date))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  if (DB_VALUE_DOMAIN_TYPE (db_days) == DB_TYPE_NULL || DB_IS_NULL (db_days))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  /* simple case, where just a number of days is added to date */

  days = DB_GET_INT (db_days);

  switch (res_type)
    {
    case DB_TYPE_STRING:
    case DB_TYPE_CHAR:
      date_s = DB_GET_STRING (date);

      /* try to figure out which format has the string */
      is_dt = db_string_to_datetime (date_s, &db_datetime);
      is_d = db_string_to_date (date_s, &db_date);
      is_t = db_string_to_time (date_s, &db_time);
      is_timest = db_string_to_timestamp (date_s, &db_timestamp);

      if (is_dt == ER_DATE_CONVERSION && is_d == ER_DATE_CONVERSION
	  && is_t == ER_DATE_CONVERSION && is_timest == ER_DATE_CONVERSION)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  goto error;
	}

      /* add date stuff to a time -> error */
      /* in fact, disable time operations, not available on mysql */
      if (is_t == 0)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  goto error;
	}

      dt_p = &db_datetime;
      d_p = &db_date;
      ts_p = &db_timestamp;

      /* except just TIME business, convert all to DATETIME */
      break;

    case DB_TYPE_DATE:
      is_d = 1;
      d_p = DB_GET_DATE (date);
      break;

    case DB_TYPE_DATETIME:
      is_dt = 1;
      dt_p = DB_GET_DATETIME (date);
      break;

    case DB_TYPE_TIMESTAMP:
      is_timest = 1;
      ts_p = DB_GET_TIMESTAMP (date);
      break;

    case DB_TYPE_TIME:
      /* should not reach here */
      assert (0);
      break;

    default:
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      goto error;
    }

  if (is_d >= 0)
    {
      y = m = d = h = mi = s = ms = 0;
      db_date_decode (d_p, &m, &d, &y);

      if (is_add)
	{
	  if (days > 0)
	    {
	      ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, days, 0, 0, 0, 0);
	    }
	  else
	    {
	      ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, -days, 0, 0, 0, 0);
	    }
	}
      else
	{
	  if (days > 0)
	    {
	      ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, days, 0, 0, 0, 0);
	    }
	  else
	    {
	      ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, -days, 0, 0, 0, 0);
	    }
	}

      /* year should always be greater than 1 and less than 9999(for mysql) */
      if (ret != NO_ERROR)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  goto error;
	}

      db_date_encode (&db_date, m, d, y);

      if (res_type == DB_TYPE_STRING || res_type == DB_TYPE_CHAR)
	{
	  db_date_to_string (res_s, 64, &db_date);

	  res_final = db_private_alloc (NULL, strlen (res_s) + 1);
	  if (!res_final)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  strcpy (res_final, res_s);
	  DB_MAKE_STRING (result, res_final);
	}
      else
	{
	  DB_MAKE_DATE (result, m, d, y);
	}
    }
  else if (is_dt >= 0)
    {
      y = m = d = h = mi = s = ms = 0;
      db_datetime_decode (dt_p, &m, &d, &y, &h, &mi, &s, &ms);

      if (is_add)
	{
	  if (days > 0)
	    {
	      ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, days, 0, 0, 0, 0);
	    }
	  else
	    {
	      ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, -days, 0, 0, 0, 0);
	    }
	}
      else
	{
	  if (days > 0)
	    {
	      ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, days, 0, 0, 0, 0);
	    }
	  else
	    {
	      ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, -days, 0, 0, 0, 0);
	    }
	}
      /* year should always be greater than 1 and less than 9999(for mysql) */
      if (ret != NO_ERROR)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  goto error;
	}

      db_datetime.date = db_datetime.time = 0;
      db_datetime_encode (&db_datetime, m, d, y, h, mi, s, ms);

      if (res_type == DB_TYPE_STRING || res_type == DB_TYPE_CHAR)
	{
	  db_datetime_to_string (res_s, 64, &db_datetime);

	  res_final = db_private_alloc (NULL, strlen (res_s) + 1);
	  if (!res_final)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  strcpy (res_final, res_s);
	  DB_MAKE_STRING (result, res_final);
	}
      else
	{
	  /* datetime, date + time units, timestamp => return datetime */
	  DB_MAKE_DATETIME (result, &db_datetime);
	}
    }
  else if (is_timest >= 0)
    {
      y = m = d = h = mi = s = ms = 0;
      db_timestamp_decode (ts_p, &db_date, &db_time);
      db_date_decode (&db_date, &m, &d, &y);
      db_time_decode (&db_time, &h, &mi, &s);

      if (is_add)
	{
	  if (days > 0)
	    {
	      ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, days, 0, 0, 0, 0);
	    }
	  else
	    {
	      ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, -days, 0, 0, 0, 0);
	    }
	}
      else
	{
	  if (days > 0)
	    {
	      ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, days, 0, 0, 0, 0);
	    }
	  else
	    {
	      ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, -days, 0, 0, 0, 0);
	    }
	}

      /* year should always be greater than 1 and less than 9999(for mysql) */
      if (ret != NO_ERROR)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  goto error;
	}

      db_datetime.date = db_datetime.time = 0;
      db_datetime_encode (&db_datetime, m, d, y, h, mi, s, ms);

      if (res_type == DB_TYPE_STRING || res_type == DB_TYPE_CHAR)
	{
	  db_datetime_to_string (res_s, 64, &db_datetime);

	  res_final = db_private_alloc (NULL, strlen (res_s) + 1);
	  if (!res_final)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  strcpy (res_final, res_s);
	  DB_MAKE_STRING (result, res_final);
	}
      else
	{
	  /* datetime, date + time units, timestamp => return datetime */
	  DB_MAKE_DATETIME (result, &db_datetime);
	}
    }

error:
  return error_status;
}

int
db_date_add_interval_days (DB_VALUE * result, const DB_VALUE * date,
			   const DB_VALUE * db_days)
{
  return db_date_add_sub_interval_days (result, date, db_days, true);
}

int
db_date_sub_interval_days (DB_VALUE * result, const DB_VALUE * date,
			   const DB_VALUE * db_days)
{
  return db_date_add_sub_interval_days (result, date, db_days, false);
}

/*
 * db_str_to_millisec () -
 *
 * Arguments:
 *         str: millisecond format
 *
 * Returns: int
 *
 * Errors:
 */
static int
db_str_to_millisec (const char *str)
{
  int digit_num, value, ret;

  if (str == NULL || str[0] == '\0')
    {
      return 0;
    }

  digit_num = strlen (str);
  if (digit_num >= 1 && str[0] == '-')
    {
      digit_num--;
      ret = sscanf (str, "%4d", &value);
    }
  else
    {
      ret = sscanf (str, "%3d", &value);
    }

  if (ret != 1)
    {
      return 0;
    }

  switch (digit_num)
    {
    case 1:
      value *= 100;
      break;

    case 2:
      value *= 10;
      break;

    default:
      break;
    }

  return value;
}

/*
 * copy_and_shift_values () -
 *
 * Arguments:
 *         shift: the offset the values are shifted
 *         n: normal number of arguments
 *	   first...: arguments
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *    shifts all arguments by the given value
 */
static void
copy_and_shift_values (int shift, int n, DB_BIGINT * first, ...)
{
  va_list marker;
  DB_BIGINT *curr = first;
  DB_BIGINT *v[16];		/* will contain max 5 elements */
  int i, count = 0, cnt_src = 0;

  /*
   * numeric arguments from interval expression have a delimiter read also
   * as argument so out of N arguments there are actually (N + 1)/2 numeric
   * values (ex: 1:2:3:4 or 1:2 or 1:2:3)
   */
  shift = (shift + 1) / 2;

  if (shift == n)
    {
      return;
    }

  va_start (marker, first);	/* init variable arguments */
  while (cnt_src < n)
    {
      cnt_src++;
      v[count++] = curr;
      curr = va_arg (marker, DB_BIGINT *);
    }
  va_end (marker);

  cnt_src = shift - 1;
  /* move backwards to not overwrite values */
  for (i = count - 1; i >= 0; i--)
    {
      if (cnt_src >= 0)
	{
	  /* replace */
	  *v[i] = *v[cnt_src--];
	}
      else
	{
	  /* reset */
	  *v[i] = 0;
	}
    }
}

/*
 * get_single_unit_value () -
 *   return:
 *   expr (in): input as string
 *   int_val (in) : input as integer
 */
static DB_BIGINT
get_single_unit_value (char *expr, DB_BIGINT int_val)
{
  DB_BIGINT v;

  if (expr == NULL)
    {
      v = int_val;
    }
  else
    {
      sscanf (expr, "%lld", (long long *) &v);
    }

  return v;
}

/*
 * db_date_add_sub_interval_expr () -
 *
 * Arguments:
 *         date: starting date
 *         expr: string with the amounts to add
 *	   unit: unit(s) of the amounts
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *    Returns date + the amounts from expr
 */
static int
db_date_add_sub_interval_expr (DB_VALUE * result, const DB_VALUE * date,
			       const DB_VALUE * expr, const int unit,
			       bool is_add)
{
  int sign = 0;
  int type = 0;			/* 1 -> time, 2 -> date, 3 -> both */
  DB_TYPE res_type, expr_type;
  char *date_s = NULL, *expr_s, res_s[64], millisec_s[64];
  int error_status = NO_ERROR;
  DB_BIGINT millisec, seconds, minutes, hours;
  DB_BIGINT days, weeks, months, quarters, years;
  DB_DATETIME db_datetime, *dt_p;
  DB_TIME db_time;
  DB_DATE db_date, *d_p;
  DB_TIMESTAMP db_timestamp, *ts_p;
  int narg, is_dt = -1, is_d = -1, is_t = -1, is_timest = -1;
  char delim;
  DB_VALUE trimed_expr, charset;
  DB_BIGINT unit_int_val;
  double dbl;
  int y, m, d, h, mi, s, ms;
  int ret;
  char *res_final;

  res_type = DB_VALUE_DOMAIN_TYPE (date);
  if (res_type == DB_TYPE_NULL || DB_IS_NULL (date))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  expr_type = DB_VALUE_DOMAIN_TYPE (expr);
  if (expr_type == DB_TYPE_NULL || DB_IS_NULL (expr))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  DB_MAKE_NULL (&trimed_expr);
  unit_int_val = 0;
  expr_s = NULL;

  /* 1. Prepare the input: convert expr to char */

  /*
   * expr is converted to char because it may contain a more complicated form
   * for the multiple unit formats, for example:
   * 'DAYS HOURS:MINUTES:SECONDS.MILLISECONDS'
   * For the simple unit tags, expr is integer
   */

  expr_type = DB_VALUE_DOMAIN_TYPE (expr);
  switch (expr_type)
    {
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      DB_MAKE_NULL (&charset);
      error_status = db_string_trim (BOTH, &charset, expr, &trimed_expr);
      if (error_status != NO_ERROR)
	{
	  goto error;
	}

      expr_s = DB_GET_STRING (&trimed_expr);
      if (expr_s == NULL)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}
      break;

    case DB_TYPE_SHORT:
      unit_int_val = DB_GET_SHORT (expr);
      break;

    case DB_TYPE_INTEGER:
      unit_int_val = DB_GET_INTEGER (expr);
      break;

    case DB_TYPE_BIGINT:
      unit_int_val = DB_GET_BIGINT (expr);
      break;

    case DB_TYPE_FLOAT:
      unit_int_val = (DB_BIGINT) round (DB_GET_FLOAT (expr));
      break;

    case DB_TYPE_DOUBLE:
      unit_int_val = (DB_BIGINT) round (DB_GET_DOUBLE (expr));
      break;

    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double ((DB_C_NUMERIC) db_locate_numeric (expr),
				    DB_VALUE_SCALE (expr), &dbl);
      unit_int_val = (DB_BIGINT) round (dbl);
      break;

    default:
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }

  /* 2. the big switch: according to unit, we parse expr and get amounts of
     ms/s/m/h/d/m/y/w/q to add or subtract */

  millisec_s[0] = '\0';
  millisec = seconds = minutes = hours = 0;
  days = weeks = months = quarters = years = 0;

  switch (unit)
    {
    case PT_MILLISECOND:
      millisec = get_single_unit_value (expr_s, unit_int_val);
      sign = (millisec >= 0);
      type |= 1;
      break;

    case PT_SECOND:
      seconds = get_single_unit_value (expr_s, unit_int_val);
      sign = (seconds >= 0);
      type |= 1;
      break;

    case PT_MINUTE:
      minutes = get_single_unit_value (expr_s, unit_int_val);
      sign = (minutes >= 0);
      type |= 1;
      break;

    case PT_HOUR:
      hours = get_single_unit_value (expr_s, unit_int_val);
      sign = (hours >= 0);
      type |= 1;
      break;

    case PT_DAY:
      days = get_single_unit_value (expr_s, unit_int_val);
      sign = (days >= 0);
      type |= 2;
      break;

    case PT_WEEK:
      weeks = get_single_unit_value (expr_s, unit_int_val);
      sign = (weeks >= 0);
      type |= 2;
      break;

    case PT_MONTH:
      months = get_single_unit_value (expr_s, unit_int_val);
      sign = (months >= 0);
      type |= 2;
      break;

    case PT_QUARTER:
      quarters = get_single_unit_value (expr_s, unit_int_val);
      sign = (quarters >= 0);
      type |= 2;
      break;

    case PT_YEAR:
      years = get_single_unit_value (expr_s, unit_int_val);
      sign = (years >= 0);
      type |= 2;
      break;

    case PT_SECOND_MILLISECOND:
      narg = sscanf (expr_s, "%lld%c%s", (long long *) &seconds, &delim,
		     millisec_s);
      millisec = db_str_to_millisec (millisec_s);
      copy_and_shift_values (narg, 2, &seconds, &millisec);
      sign = (seconds >= 0);
      type |= 1;
      break;

    case PT_MINUTE_MILLISECOND:
      narg = sscanf (expr_s, "%lld%c%lld%c%s", (long long *) &minutes, &delim,
		     (long long *) &seconds, &delim, millisec_s);
      millisec = db_str_to_millisec (millisec_s);
      copy_and_shift_values (narg, 3, &minutes, &seconds, &millisec);
      sign = (minutes >= 0);
      type |= 1;
      break;

    case PT_MINUTE_SECOND:
      narg = sscanf (expr_s, "%lld%c%lld", (long long *) &minutes, &delim,
		     (long long *) &seconds);
      copy_and_shift_values (narg, 2, &minutes, &seconds);
      sign = (minutes >= 0);
      type |= 1;
      break;

    case PT_HOUR_MILLISECOND:
      narg = sscanf (expr_s, "%lld%c%lld%c%lld%c%s", (long long *) &hours,
		     &delim, (long long *) &minutes, &delim,
		     (long long *) &seconds, &delim, millisec_s);
      millisec = db_str_to_millisec (millisec_s);
      copy_and_shift_values (narg, 4, &hours, &minutes, &seconds, &millisec);
      sign = (hours >= 0);
      type |= 1;
      break;

    case PT_HOUR_SECOND:
      narg = sscanf (expr_s, "%lld%c%lld%c%lld", (long long *) &hours, &delim,
		     (long long *) &minutes, &delim, (long long *) &seconds);
      copy_and_shift_values (narg, 3, &hours, &minutes, &seconds);
      sign = (hours >= 0);
      type |= 1;
      break;

    case PT_HOUR_MINUTE:
      narg = sscanf (expr_s, "%lld%c%lld", (long long *) &hours, &delim,
		     (long long *) &minutes);
      copy_and_shift_values (narg, 2, &hours, &minutes);
      sign = (hours >= 0);
      type |= 1;
      break;

    case PT_DAY_MILLISECOND:
      narg = sscanf (expr_s, "%lld%c%lld%c%lld%c%lld%c%s",
		     (long long *) &days, &delim, (long long *) &hours,
		     &delim, (long long *) &minutes, &delim,
		     (long long *) &seconds, &delim, millisec_s);
      millisec = db_str_to_millisec (millisec_s);
      copy_and_shift_values (narg, 5, &days, &hours, &minutes, &seconds,
			     &millisec);
      sign = (days >= 0);
      type |= 1;
      type |= 2;
      break;

    case PT_DAY_SECOND:
      narg = sscanf (expr_s, "%lld%c%lld%c%lld%c%lld", (long long *) &days,
		     &delim, (long long *) &hours, &delim,
		     (long long *) &minutes, &delim, (long long *) &seconds);
      copy_and_shift_values (narg, 4, &days, &hours, &minutes, &seconds);
      sign = (days >= 0);
      type |= 1;
      type |= 2;
      break;

    case PT_DAY_MINUTE:
      narg = sscanf (expr_s, "%lld%c%lld%c%lld", (long long *) &days, &delim,
		     (long long *) &hours, &delim, (long long *) &minutes);
      copy_and_shift_values (narg, 3, &days, &hours, &minutes);
      sign = (days >= 0);
      type |= 1;
      type |= 2;
      break;

    case PT_DAY_HOUR:
      narg = sscanf (expr_s, "%lld%c%lld", (long long *) &days, &delim,
		     (long long *) &hours);
      copy_and_shift_values (narg, 2, &days, &hours);
      sign = (days >= 0);
      type |= 1;
      type |= 2;
      break;

    case PT_YEAR_MONTH:
      narg = sscanf (expr_s, "%lld%c%lld", (long long *) &years, &delim,
		     (long long *) &months);
      copy_and_shift_values (narg, 2, &years, &months);
      sign = (years >= 0);
      type |= 2;
      break;

    default:
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }

  /* we have the sign of the amounts, turn them in absolute value */
  years = ABS (years);
  months = ABS (months);
  days = ABS (days);
  weeks = ABS (weeks);
  quarters = ABS (quarters);
  hours = ABS (hours);
  minutes = ABS (minutes);
  seconds = ABS (seconds);
  millisec = ABS (millisec);

  /* convert weeks and quarters to our units */
  if (weeks != 0)
    {
      days += weeks * 7;
      weeks = 0;
    }

  if (quarters != 0)
    {
      months += 3 * quarters;
      quarters = 0;
    }

  /* 3. Convert string with date to DateTime or Time */

  switch (res_type)
    {
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_NCHAR:
    case DB_TYPE_VARNCHAR:
      date_s = DB_GET_STRING (date);

      /* try to figure out which format has the string */
      is_dt = db_string_to_datetime (date_s, &db_datetime);
      is_d = db_string_to_date (date_s, &db_date);
      is_t = db_string_to_time (date_s, &db_time);
      is_timest = db_string_to_timestamp (date_s, &db_timestamp);

      if (is_dt == ER_DATE_CONVERSION && is_d == ER_DATE_CONVERSION
	  && is_t == ER_DATE_CONVERSION && is_timest == ER_DATE_CONVERSION)
	{
	  error_status = ER_DATE_CONVERSION;
	  goto error;
	}

      /* add date stuff to a time -> error */
      /* in fact, disable time operations, not available on mysql */
      if (is_t == 0)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}

      dt_p = &db_datetime;
      d_p = &db_date;
      ts_p = &db_timestamp;

      /* except just TIME business, convert all to DATETIME */
      break;

    case DB_TYPE_DATE:
      is_d = 1;
      d_p = DB_GET_DATE (date);
      break;

    case DB_TYPE_DATETIME:
      is_dt = 1;
      dt_p = DB_GET_DATETIME (date);
      break;

    case DB_TYPE_TIMESTAMP:
      is_timest = 1;
      ts_p = DB_GET_TIMESTAMP (date);
      break;

    case DB_TYPE_TIME:
      /* should not reach here */
      assert (0);
      break;

    default:
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }

  /* treat as date only if adding date units, else treat as datetime */
  if (is_d >= 0)
    {
      y = m = d = h = mi = s = ms = 0;
      db_date_decode (d_p, &m, &d, &y);

      if (sign ^ is_add)
	{
	  ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
					     years, months, days, hours,
					     minutes, seconds, millisec);
	}
      else
	{
	  ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
					     years, months, days, hours,
					     minutes, seconds, millisec);
	}

      /* year should always be greater than 1 and less than 9999(for mysql) */
      if (ret != NO_ERROR)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}

      if (type == 2)
	{
	  db_date_encode (&db_date, m, d, y);

	  if (res_type == DB_TYPE_STRING || res_type == DB_TYPE_CHAR)
	    {
	      db_date_to_string (res_s, 64, &db_date);

	      res_final = db_private_alloc (NULL, strlen (res_s) + 1);
	      if (res_final == NULL)
		{
		  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
		  goto error;
		}
	      strcpy (res_final, res_s);
	      DB_MAKE_STRING (result, res_final);
	    }
	  else
	    {
	      DB_MAKE_DATE (result, m, d, y);
	    }
	}
      else if (type & 1)
	{
	  db_datetime.date = db_datetime.time = 0;
	  db_datetime_encode (&db_datetime, m, d, y, h, mi, s, ms);

	  if (res_type == DB_TYPE_STRING || res_type == DB_TYPE_CHAR)
	    {
	      db_datetime_to_string (res_s, 64, &db_datetime);

	      res_final = db_private_alloc (NULL, strlen (res_s) + 1);
	      if (res_final == NULL)
		{
		  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
		  goto error;
		}
	      strcpy (res_final, res_s);
	      DB_MAKE_STRING (result, res_final);
	    }
	  else
	    {
	      DB_MAKE_DATETIME (result, &db_datetime);
	    }
	}
    }
  else if (is_dt >= 0)
    {
      y = m = d = h = mi = s = ms = 0;
      db_datetime_decode (dt_p, &m, &d, &y, &h, &mi, &s, &ms);

      if (sign ^ is_add)
	{
	  ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
					     years, months, days, hours,
					     minutes, seconds, millisec);
	}
      else
	{
	  ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
					     years, months, days, hours,
					     minutes, seconds, millisec);
	}

      /* year should always be greater than 1 and less than 9999(for mysql) */
      if (ret != NO_ERROR)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}

      db_datetime.date = db_datetime.time = 0;
      db_datetime_encode (&db_datetime, m, d, y, h, mi, s, ms);

      if (res_type == DB_TYPE_STRING || res_type == DB_TYPE_CHAR)
	{
	  db_datetime_to_string (res_s, 64, &db_datetime);

	  res_final = db_private_alloc (NULL, strlen (res_s) + 1);
	  if (res_final == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  strcpy (res_final, res_s);
	  DB_MAKE_STRING (result, res_final);
	}
      else
	{
	  /* datetime, date + time units, timestamp => return datetime */
	  DB_MAKE_DATETIME (result, &db_datetime);
	}
    }
  else if (is_timest >= 0)
    {
      y = m = d = h = mi = s = ms = 0;
      db_timestamp_decode (ts_p, &db_date, &db_time);
      db_date_decode (&db_date, &m, &d, &y);
      db_time_decode (&db_time, &h, &mi, &s);

      if (sign ^ is_add)
	{
	  ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
					     years, months, days, hours,
					     minutes, seconds, millisec);
	}
      else
	{
	  ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
					     years, months, days, hours,
					     minutes, seconds, millisec);
	}

      /* year should always be greater than 1 and less than 9999(for mysql) */
      if (ret != NO_ERROR)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}

      db_datetime.date = db_datetime.time = 0;
      db_datetime_encode (&db_datetime, m, d, y, h, mi, s, ms);

      if (res_type == DB_TYPE_STRING || res_type == DB_TYPE_CHAR)
	{
	  db_datetime_to_string (res_s, 64, &db_datetime);

	  res_final = db_private_alloc (NULL, strlen (res_s) + 1);
	  if (res_final == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  strcpy (res_final, res_s);
	  DB_MAKE_STRING (result, res_final);
	}
      else
	{
	  /* datetime, date + time units, timestamp => return datetime */
	  DB_MAKE_DATETIME (result, &db_datetime);
	}
    }

error:
  db_value_clear (&trimed_expr);
  return error_status;
}

/*
 * db_date_add_interval_expr ()
 *
 * Arguments:
 *         result(out):
 *         date(in): source date
 *         expr(in): to be added interval
 *         unit(in): unit of interval expr
 *
 * Returns: int
 *
 * Note:
 */
int
db_date_add_interval_expr (DB_VALUE * result, const DB_VALUE * date,
			   const DB_VALUE * expr, const int unit)
{
  return db_date_add_sub_interval_expr (result, date, expr, unit, true);
}

/*
 * db_date_sub_interval_expr ()
 *
 * Arguments:
 *         result(out):
 *         date(in): source date
 *         expr(in): to be substracted interval
 *         unit(in): unit of interval expr
 *
 * Returns: int
 *
 * Note:
 */
int
db_date_sub_interval_expr (DB_VALUE * result, const DB_VALUE * date,
			   const DB_VALUE * expr, const int unit)
{
  return db_date_add_sub_interval_expr (result, date, expr, unit, false);
}

/*
 * db_date_format ()
 *
 * Arguments:
 *         date_value: source date
 *         expr: string with format specifiers
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *    formats the date according to a specified format
 */
int
db_date_format (const DB_VALUE * date_value, const DB_VALUE * format,
		DB_VALUE * result)
{
  DB_DATETIME db_datetime, *dt_p;
  DB_DATE db_date, *d_p;
  DB_TIME db_time;
  DB_TIMESTAMP *ts_p;
  DB_TYPE res_type;
  char *date_s = NULL, *res, *res2, *format_s;
  int error_status = NO_ERROR, len;
  int y, m, d, h, mi, s, ms;
  int days[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  char format_specifiers[256][64];
  int i, j;
  int dow, dow2;
  int lang_Loc_id = lang_id ();
  int tu, tv, tx, weeks, ld_fw, days_counter;
  char och = -1, ch;

  y = m = d = h = mi = s = ms = 0;
  memset (format_specifiers, 0, sizeof (format_specifiers));

  if (date_value == NULL || format == NULL
      || DB_IS_NULL (date_value) || DB_IS_NULL (format))
    {
      DB_MAKE_NULL (result);
      goto error;
    }

  if (!is_char_string (format))
    {
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  res_type = DB_VALUE_DOMAIN_TYPE (date_value);

  /* 1. Get date values */
  switch (res_type)
    {
    case DB_TYPE_DATETIME:
      dt_p = DB_GET_DATETIME (date_value);
      db_datetime_decode (dt_p, &m, &d, &y, &h, &mi, &s, &ms);
      break;

    case DB_TYPE_DATE:
      d_p = DB_GET_DATE (date_value);
      db_date_decode (d_p, &m, &d, &y);
      break;

    case DB_TYPE_TIMESTAMP:
      ts_p = DB_GET_TIMESTAMP (date_value);
      db_timestamp_decode (ts_p, &db_date, &db_time);
      db_time_decode (&db_time, &h, &mi, &s);
      db_date_decode (&db_date, &m, &d, &y);
      break;

    case DB_TYPE_STRING:
    case DB_TYPE_CHAR:
      date_s = DB_GET_STRING (date_value);

      if (db_string_to_datetime (date_s, &db_datetime) == ER_DATE_CONVERSION)
	{
	  error_status = ER_QSTR_INVALID_DATA_TYPE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}

      db_datetime_decode (&db_datetime, &m, &d, &y, &h, &mi, &s, &ms);
      break;

    default:
      error_status = ER_QSTR_INVALID_DATA_TYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }

  /* 2. Compute the value for each format specifier */
  days[2] += LEAP (y);
  dow = db_get_day_of_week (y, m, d);

  /* %a       Abbreviated weekday name (Sun..Sat) */
  strcpy (format_specifiers['a'], Short_Day_name[lang_Loc_id][dow]);

  /* %b       Abbreviated m name (Jan..Dec) */
  strcpy (format_specifiers['b'], Short_Month_name[lang_Loc_id][m - 1]);

  /* %c       Month, numeric (0..12) - actually (1..12) */
  sprintf (format_specifiers['c'], "%d", m);

  /* %D       Day of the m with English suffix (0th, 1st, 2nd, 3rd,...) */
  sprintf (format_specifiers['D'], "%d", d);
  /* 11-19 are special */
  if (d % 10 == 1 && d / 10 != 1)
    {
      strcat (format_specifiers['D'], "st");
    }
  else if (d % 10 == 2 && d / 10 != 1)
    {
      strcat (format_specifiers['D'], "nd");
    }
  else if (d % 10 == 3 && d / 10 != 1)
    {
      strcat (format_specifiers['D'], "rd");
    }
  else
    {
      strcat (format_specifiers['D'], "th");
    }

  /* %d       Day of the m, numeric (00..31) */
  sprintf (format_specifiers['d'], "%02d", d);

  /* %e       Day of the m, numeric (0..31) - actually (1..31) */
  sprintf (format_specifiers['e'], "%d", d);

  /* %f       Milliseconds (000..999) */
  sprintf (format_specifiers['f'], "%03d", ms);

  /* %H       Hour (00..23) */
  sprintf (format_specifiers['H'], "%02d", h);

  /* %h       Hour (01..12) */
  sprintf (format_specifiers['h'], "%02d", (h % 12 == 0) ? 12 : (h % 12));

  /* %I       Hour (01..12) */
  sprintf (format_specifiers['I'], "%02d", (h % 12 == 0) ? 12 : (h % 12));

  /* %i       Minutes, numeric (00..59) */
  sprintf (format_specifiers['i'], "%02d", mi);

  /* %j       Day of y (001..366) */
  for (j = d, i = 1; i < m; i++)
    {
      j += days[i];
    }
  sprintf (format_specifiers['j'], "%03d", j);

  /* %k       Hour (0..23) */
  sprintf (format_specifiers['k'], "%d", h);

  /* %l       Hour (1..12) */
  sprintf (format_specifiers['l'], "%d", (h % 12 == 0) ? 12 : (h % 12));

  /* %M       Month name (January..December) */
  strcpy (format_specifiers['M'], Month_name[lang_Loc_id][m - 1]);

  /* %m       Month, numeric (00..12) */
  sprintf (format_specifiers['m'], "%02d", m);

  /* %p       AM or PM */
  strcpy (format_specifiers['p'], (h > 11) ? "PM" : "AM");

  /* %r       Time, 12-hour (hh:mm:ss followed by AM or PM) */
  sprintf (format_specifiers['r'], "%02d:%02d:%02d %s",
	   (h % 12 == 0) ? 12 : (h % 12), mi, s, (h > 11) ? "PM" : "AM");

  /* %S       Seconds (00..59) */
  sprintf (format_specifiers['S'], "%02d", s);

  /* %s       Seconds (00..59) */
  sprintf (format_specifiers['s'], "%02d", s);

  /* %T       Time, 24-hour (hh:mm:ss) */
  sprintf (format_specifiers['T'], "%02d:%02d:%02d", h, mi, s);

  /* %U       Week (00..53), where Sunday is the first d of the week */
  /* %V       Week (01..53), where Sunday is the first d of the week;
     used with %X  */
  /* %X       Year for the week where Sunday is the first day of the week,
     numeric, four digits; used with %V */

  dow2 = db_get_day_of_week (y, 1, 1);

  ld_fw = 7 - dow2;

  for (days_counter = d, i = 1; i < m; i++)
    {
      days_counter += days[i];
    }

  if (days_counter <= ld_fw)
    {
      weeks = dow2 == 0 ? 1 : 0;
    }
  else
    {
      days_counter -= (dow2 == 0) ? 0 : ld_fw;
      weeks = days_counter / 7 + (days_counter % 7 ? 1 : 0);
    }

  tu = tv = weeks;
  tx = y;
  if (tv == 0)
    {
      dow2 = db_get_day_of_week (y - 1, 1, 1);
      days_counter = 365 + LEAP (y - 1) - (dow2 == 0 ? 0 : 7 - dow2);
      tv = days_counter / 7 + (days_counter % 7 ? 1 : 0);
      tx = y - 1;
    }

  sprintf (format_specifiers['U'], "%02d", tu);
  sprintf (format_specifiers['V'], "%02d", tv);
  sprintf (format_specifiers['X'], "%04d", tx);

  /* %u       Week (00..53), where Monday is the first d of the week */
  /* %v       Week (01..53), where Monday is the first d of the week;
     used with %x  */
  /* %x       Year for the week, where Monday is the first day of the week,
     numeric, four digits; used with %v */

  dow2 = db_get_day_of_week (y, 1, 1);
  weeks = dow2 >= 1 && dow2 <= 4 ? 1 : 0;

  ld_fw = dow2 == 0 ? 1 : 7 - dow2 + 1;

  for (days_counter = d, i = 1; i < m; i++)
    {
      days_counter += days[i];
    }

  if (days_counter > ld_fw)
    {
      days_counter -= ld_fw;
      weeks += days_counter / 7 + (days_counter % 7 ? 1 : 0);
    }

  tu = weeks;
  tv = weeks;
  tx = y;
  if (tv == 0)
    {
      dow2 = db_get_day_of_week (y - 1, 1, 1);
      weeks = dow2 >= 1 && dow2 <= 4 ? 1 : 0;
      ld_fw = dow2 == 0 ? 1 : 7 - dow2 + 1;
      days_counter = 365 + LEAP (y - 1) - ld_fw;
      tv = weeks + days_counter / 7 + (days_counter % 7 ? 1 : 0);
      tx = y - 1;
    }
  else if (tv == 53)
    {
      dow2 = db_get_day_of_week (y + 1, 1, 1);
      if (dow2 >= 1 && dow2 <= 4)
	{
	  tv = 1;
	  tx = y + 1;
	}
    }

  sprintf (format_specifiers['u'], "%02d", tu);
  sprintf (format_specifiers['v'], "%02d", tv);
  sprintf (format_specifiers['x'], "%04d", tx);

  /* %W       Weekday name (Sunday..Saturday) */
  strcpy (format_specifiers['W'], Day_name[lang_Loc_id][dow]);

  /* %w       Day of the week (0=Sunday..6=Saturday) */
  sprintf (format_specifiers['w'], "%d", dow);

  /* %Y       Year, numeric, four digits */
  sprintf (format_specifiers['Y'], "%04d", y);

  /* %y       Year, numeric (two digits) */
  sprintf (format_specifiers['y'], "%02d", y % 100);

  /* 3. Generate the output according to the format and the values */
  format_s = DB_PULL_STRING (format);
  len = 1024;

  res = (char *) db_private_alloc (NULL, len);
  if (res == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error;
    }
  memset (res, 0, len);

  ch = *format_s;
  while (ch != 0)
    {
      format_s++;
      och = ch;
      ch = *format_s;

      if (och == '%' /* && (res[strlen(res) - 1] != '%') */ )
	{
	  if (ch == '%')
	    {
	      STRCHCAT (res, '%');

	      /* jump a character */
	      format_s++;
	      och = ch;
	      ch = *format_s;

	      continue;
	    }
	  /* parse the character */
	  if (strlen (format_specifiers[(unsigned char) ch]) == 0)
	    {
	      /* append the character itself */
	      STRCHCAT (res, ch);
	    }
	  else
	    {
	      strcat (res, format_specifiers[(unsigned char) ch]);
	    }

	  /* jump a character */
	  format_s++;
	  och = ch;
	  ch = *format_s;
	}
      else
	{
	  STRCHCAT (res, och);
	}

      /* chance of overflow ? */
      /* assume we can't add at a time mode than 16 chars */
      if (strlen (res) + 16 > len)
	{
	  /* realloc - copy temporary in res2 */
	  res2 = (char *) db_private_alloc (NULL, len);
	  if (res2 == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  memset (res2, 0, len);
	  strcpy (res2, res);
	  db_private_free (NULL, res);

	  len += 1024;
	  res = (char *) db_private_alloc (NULL, len);
	  if (res == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  memset (res, 0, len);
	  strcpy (res, res2);
	  db_private_free (NULL, res2);
	}
    }
  /* finished string */

  /* 4. */
  DB_MAKE_STRING (result, res);

error:
  /* do not free res as it was moved to result and will be freed later */

  return error_status;
}

/*
 * parse_characters ()
 *
 * Arguments:
 *         s: source string to parse
 *         res: output string with the parsed word
 *	   cnt: length at which we trim the string (-1 if none)
 *
 * Returns: int - actual number of characters read
 *
 * Errors:
 *
 * Note:
 *    reads cnt alphabetic characters until a non-alpha char reached
 */
int
parse_characters (char *s, char *res, int cnt, int res_size)
{
  int count = 0;
  char *ch;
  int len;
  int lang_Loc_id = lang_id ();

  ch = s;

  while (WHITESPACE (*ch))
    ch++, count++;

  memset (res, 0, sizeof (char) * res_size);
  /* in Korean accept any character */
  while (*ch != 0
	 && ((*ch >= 'A' && *ch <= 'Z') || (*ch >= 'a' && *ch <= 'z')
	     || lang_Loc_id == INTL_LANG_KOREAN))
    {
      if (WHITESPACE (*ch))
	{
	  break;
	}
      STRCHCAT (res, *ch);
      ch++;
      count++;

      /* trim at cnt characters */
      len = strlen (res);
      if (len == cnt || len == res_size - 1)
	{
	  break;
	}
    }

  return count;
}

/*
 * parse_digits ()
 *
 * Arguments:
 *         s: source string to parse
 *         nr: output number
 *	   cnt: length at which we trim the number (-1 if none)
 *
 * Returns: int - actual number of characters read
 *
 * Errors:
 *
 * Note:
 *    reads cnt digits until non-digit char reached
 */
int
parse_digits (char *s, int *nr, int cnt)
{
  int count = 0, len;
  char *ch;
  /* res[64] is safe because res has a max length of cnt, which is max 4 */
  char res[64];
  const int res_count = sizeof (res) / sizeof (char);

  ch = s;
  *nr = 0;

  memset (res, 0, sizeof (res));

  while (WHITESPACE (*ch))
    {
      ch++;
      count++;
    }

  /* do not support negative numbers because... they are not supported :) */
  while (*ch != 0 && (*ch >= '0' && *ch <= '9'))
    {
      STRCHCAT (res, *ch);

      ch++;
      count++;

      /* trim at cnt characters */
      len = strlen (res);
      if (len == cnt || len == res_count - 1)
	{
	  break;
	}
    }

  *nr = atol (res);

  return count;
}

/*
 * db_str_to_date ()
 *
 * Arguments:
 *         str: string from which we get the data
 *         format: format specifiers to match the str
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *    inverse function for date_format - compose a date/time from some format
 *    specifiers and some informations.
 */
int
db_str_to_date (const DB_VALUE * str, const DB_VALUE * format,
		DB_VALUE * result)
{
  char *sstr = NULL, *format_s = NULL, *format2_s = NULL;
  int i, j, k, error_status = NO_ERROR;
  int type, len1, len2, h24 = 0, _v, _x;
  DB_TYPE res_type;
  int days[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  int y, m, d, h, mi, s, ms, am /* 0 = AM, 1 = PM */ ;
  int u, U, v, V, dow, doy, w;

  if (str == (DB_VALUE *) NULL || format == (DB_VALUE *) NULL)
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }

  if (DB_IS_NULL (str) || DB_IS_NULL (format))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  y = m = d = V = v = U = u = -1;
  h = mi = s = ms = 0;
  dow = doy = am = -1;
  _v = _x = 0;
  sstr = DB_PULL_STRING (str);
  format2_s = DB_PULL_STRING (format);

  format_s = (char *) db_private_alloc (NULL, strlen (format2_s) + 1);
  if (!format_s)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }
  memset (format_s, 0, sizeof (char) * (strlen (format2_s) + 1));

  len2 = strlen (format2_s);

  /* delete all whitespace from format */
  for (i = 0; i < len2; i++)
    {
      if (!WHITESPACE (format2_s[i]))
	{
	  STRCHCAT (format_s, format2_s[i]);
	}
      /* '%' without format specifier */
      else if (WHITESPACE (format2_s[i]) && i > 0 && format2_s[i - 1] == '%')
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}
    }

  type = db_check_time_date_format (format_s);
  if (type == 1)
    {
      res_type = DB_TYPE_TIME;
    }
  else if (type == 2)
    {
      res_type = DB_TYPE_DATE;
    }
  else if (type == 3)
    {
      res_type = DB_TYPE_DATETIME;
    }
  else
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }

  /*
   * 1. Get information according to format specifiers
   *    iterate simultaneously through each string and sscanf when
   *    it is a format specifier.
   *    If a format specifier has more than one occurence, get the last value.
   */
  do
    {
      int lang_Loc_id = lang_id ();
      char sz[64];
      const int sz_count = sizeof (sz) / sizeof (char);

      len1 = strlen (sstr);
      len2 = strlen (format_s);

      i = j = k = 0;

      while (i < len1 && j < len2)
	{
	  while (WHITESPACE (sstr[i]))
	    {
	      i++;
	    }

	  while (WHITESPACE (format_s[j]))
	    {
	      j++;
	    }

	  if (j > 0 && format_s[j - 1] == '%')
	    {
	      /* do not accept a double % */
	      if (j > 1 && format_s[j - 2] == '%')
		{
		  error_status = ER_OBJ_INVALID_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  goto error;
		}

	      /* we have a format specifier */
	      switch (format_s[j])
		{
		case 'a':
		  /* %a Abbreviated weekday name (Sun..Sat) */
		  i += parse_characters (sstr + i, sz, -1, sz_count);
		  ustr_lower (sz);

		  /* capitalize first letter for matching to Short_Day_name */
		  if (lang_Loc_id == INTL_LANG_ENGLISH)
		    {
		      sz[0] -= 32;
		    }

		  for (dow = 0; dow < 7; dow++)
		    {
		      if (!strcmp (sz, Short_Day_name[lang_Loc_id][dow]))
			{
			  break;
			}
		    }

		  if (dow == 7)	/* not found - error */
		    {
		      goto conversion_error;
		    }

		  break;

		case 'b':
		  /* %b Abbreviated month name (Jan..Dec) */
		  i += parse_characters (sstr + i, sz, -1, sz_count);
		  ustr_lower (sz);
		  /* capitalize first letter for matching to Short_Month_name */
		  if (lang_Loc_id == INTL_LANG_ENGLISH)
		    {
		      sz[0] -= 32;
		    }

		  for (m = 0; m < 12; m++)
		    {
		      if (!strcmp (sz, Short_Month_name[lang_Loc_id][m]))
			{
			  break;
			}
		    }

		  if (m == 12)	/* not found - error */
		    {
		      goto conversion_error;
		    }
		  m++;
		  break;

		case 'c':
		  /* %c Month, numeric (0..12) */
		  k = parse_digits (sstr + i, &m, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'D':
		  /* %D Day of the month with English suffix (0th, 1st, 2nd,
		     3rd, ...) */
		  k = parse_digits (sstr + i, &d, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  /* need 2 necessary characters or whitespace (!) after */
		  i += 2;
		  break;

		case 'd':
		  /* %d Day of the month, numeric (00..31) */
		  k = parse_digits (sstr + i, &d, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'e':
		  /* %e Day of the month, numeric (0..31) */
		  k = parse_digits (sstr + i, &d, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'f':
		  /* %f Milliseconds (000..999) */
		  k = parse_digits (sstr + i, &ms, 3);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'H':
		  /* %H Hour (00..23) */
		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  h24 = 1;
		  break;

		case 'h':
		  /* %h Hour (01..12) */
		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'I':
		  /* %I Hour (01..12) */
		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'i':
		  /* %i Minutes, numeric (00..59) */
		  k = parse_digits (sstr + i, &mi, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'j':
		  /* %j Day of year (001..366) */
		  k = parse_digits (sstr + i, &doy, 3);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'k':
		  /* %k Hour (0..23) */
		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  h24 = 1;
		  break;

		case 'l':
		  /* %l Hour (1..12) */
		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'M':
		  /* %M Month name (January..December) */
		  i += parse_characters (sstr + i, sz, -1, sz_count);
		  ustr_lower (sz);
		  /* capitalize first letter for matching to Month_name */
		  if (lang_Loc_id == INTL_LANG_ENGLISH)
		    {
		      sz[0] -= 32;
		    }

		  for (m = 0; m < 12; m++)
		    {
		      if (!strcmp (sz, Month_name[lang_Loc_id][m]))
			{
			  break;
			}
		    }

		  if (m == 12)	/* not found - error */
		    {
		      goto conversion_error;
		    }
		  m++;
		  break;

		case 'm':
		  /* %m Month, numeric (00..12) */
		  k = parse_digits (sstr + i, &m, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'p':
		  /* %p AM or PM */
		  i += parse_characters (sstr + i, sz, 2, sz_count);
		  ustr_lower (sz);
		  if (!strcmp (sz, "am"))
		    {
		      am = 0;
		    }
		  else if (!strcmp (sz, "pm"))
		    {
		      am = 1;
		    }
		  else
		    {
		      goto conversion_error;
		    }

		  break;

		case 'r':
		  /* %r Time, 12-hour (hh:mm:ss followed by AM or PM) */

		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;

		  while (WHITESPACE (sstr[i]))
		    {
		      i++;
		    }

		  if (sstr[i] != ':')
		    {
		      goto conversion_error;
		    }
		  i++;

		  k = parse_digits (sstr + i, &mi, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;

		  while (WHITESPACE (sstr[i]))
		    {
		      i++;
		    }

		  if (sstr[i] != ':')
		    {
		      goto conversion_error;
		    }
		  i++;

		  k = parse_digits (sstr + i, &s, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;

		  i += parse_characters (sstr + i, sz, 2, sz_count);
		  ustr_lower (sz);
		  if (!strcmp (sz, "am"))
		    {
		      am = 0;
		    }
		  else if (!strcmp (sz, "pm"))
		    {
		      am = 1;
		    }
		  else
		    {
		      goto conversion_error;
		    }

		  break;

		case 'S':
		  /* %S Seconds (00..59) */
		  k = parse_digits (sstr + i, &s, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 's':
		  /* %s Seconds (00..59) */
		  k = parse_digits (sstr + i, &s, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'T':
		  /* %T Time, 24-hour (hh:mm:ss) */

		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;

		  while (WHITESPACE (sstr[i]))
		    {
		      i++;
		    }

		  if (sstr[i] != ':')
		    {
		      goto conversion_error;
		    }
		  i++;

		  k = parse_digits (sstr + i, &mi, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }

		  i += k;
		  while (WHITESPACE (sstr[i]))
		    {
		      i++;
		    }

		  if (sstr[i] != ':')
		    {
		      goto conversion_error;
		    }
		  i++;

		  k = parse_digits (sstr + i, &s, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  h24 = 1;

		  break;

		case 'U':
		  /* %U Week (00..53), where Sunday is the first day
		     of the week */
		  k = parse_digits (sstr + i, &U, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'u':
		  /* %u Week (00..53), where Monday is the first day
		     of the week */
		  k = parse_digits (sstr + i, &u, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'V':
		  /* %V Week (01..53), where Sunday is the first day
		     of the week; used with %X  */
		  k = parse_digits (sstr + i, &V, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  _v = 1;
		  break;

		case 'v':
		  /* %v Week (01..53), where Monday is the first day
		     of the week; used with %x  */
		  k = parse_digits (sstr + i, &v, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  _v = 2;
		  break;

		case 'W':
		  /* %W Weekday name (Sunday..Saturday) */
		  i += parse_characters (sstr + i, sz, -1, sz_count);
		  ustr_lower (sz);
		  /* capitalize first letter for matching to Day_name */
		  if (lang_Loc_id == INTL_LANG_ENGLISH)
		    {
		      sz[0] -= 32;
		    }

		  for (dow = 0; dow < 7; dow++)
		    {
		      if (!strcmp (sz, Day_name[lang_Loc_id][dow]))
			{
			  break;
			}
		    }

		  if (dow == 7)	/* not found - error */
		    {
		      goto conversion_error;
		    }
		  break;

		case 'w':
		  /* %w Day of the week (0=Sunday..6=Saturday) */
		  k = parse_digits (sstr + i, &dow, 1);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'X':
		  /* %X Year for the week where Sunday is the first day
		     of the week, numeric, four digits; used with %V  */
		  k = parse_digits (sstr + i, &y, 4);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  _x = 1;
		  break;

		case 'x':
		  /* %x Year for the week, where Monday is the first day
		     of the week, numeric, four digits; used with %v  */
		  k = parse_digits (sstr + i, &y, 4);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  _x = 2;
		  break;

		case 'Y':
		  /* %Y Year, numeric, four digits */
		  k = parse_digits (sstr + i, &y, 4);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;
		  break;

		case 'y':
		  /* %y Year, numeric (two digits) */
		  k = parse_digits (sstr + i, &y, 2);
		  if (k <= 0)
		    {
		      goto conversion_error;
		    }
		  i += k;

		  /* TODO: 70 convention always available? */
		  if (y < 70)
		    {
		      y = 2000 + y;
		    }
		  else
		    {
		      y = 1900 + y;
		    }

		  break;

		default:
		  goto conversion_error;
		  break;
		}
	    }
	  else if (sstr[i] != format_s[j] && format_s[j] != '%')
	    {
	      error_status = ER_OBJ_INVALID_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto error;
	    }
	  else if (format_s[j] != '%')
	    {
	      i++;
	    }

	  /* when is a format specifier do not advance in sstr
	     because we need the entire value */
	  j++;
	}
    }
  while (0);

  /* 2. Validations */
  if (am != -1 && h24 == 1)	/* 24h time format and am/pm */
    {
      goto conversion_error;
    }

  if (_x != _v && _x != -1)	/* accept %v only if %x and %V only if %X */
    {
      goto conversion_error;
    }

  if (am == 1 && h != -1)
    {
      h += 12;
    }

  days[2] += LEAP (y);

  /*
   * validations are done here because they are done just on the last memorized
   * values (ie: if you supply a month 99 then a month 12 the 99 isn't validated
   * because it's overwritten by 12 which is correct).
   */

  /*
   * check only upper bounds, lower bounds will be checked later and
   * will return error
   */
  if (res_type == DB_TYPE_DATE || res_type == DB_TYPE_DATETIME)
    {
      /* year is validated becuase it's vital for m & d */
      if (y <= 0 || y > 9999)
	{
	  goto conversion_error;
	}

      if (m > 12)
	{
	  goto conversion_error;
	}

      /* because we do not support invalid dates ... */
      if (m != -1 && d > days[m])
	{
	  goto conversion_error;
	}

      if (u > 53)
	{
	  goto conversion_error;
	}

      if (v > 53)
	{
	  goto conversion_error;
	}

      if (v == 0 || u > 53)
	{
	  goto conversion_error;
	}

      if (V == 0 || u > 53)
	{
	  goto conversion_error;
	}

      if (doy == 0 || doy > 365 + LEAP (y))
	{
	  goto conversion_error;
	}

      if (dow > 6)
	{
	  goto conversion_error;
	}
    }

  if (res_type == DB_TYPE_TIME || res_type == DB_TYPE_DATETIME)
    {
      if ((am != -1 && h > 12) || (am == -1 && h > 23))
	{
	  goto conversion_error;
	}

      if (mi > 59)
	{
	  goto conversion_error;
	}

      if (s > 59)
	{
	  goto conversion_error;
	}
      /* milli does not need checking, it has all values from 0 to 999 */
    }


  /* 3. Try to compute a date according to the information from the format
     specifiers */

  if (res_type == DB_TYPE_TIME)
    {
      /* --- no job to do --- */
      goto write_results;
    }

  /* the year is fixed, compute the day and month from dow, doy, etc */
  /*
   * the day and month can be supplied specifically which suppres all other
   * informations or can be computed from dow and week or from doy
   */

  /* 3.1 - we have a valid day and month */
  if (m >= 1 && m <= 12 && d >= 1 && d <= days[m])
    {
      /* --- no job to do --- */
      goto write_results;
    }

  w = MAX (v, MAX (V, MAX (u, U)));
  /* 3.2 - we have the day of week and a week */
  if (dow != -1 && w != -1)
    {
      int dow2 = db_get_day_of_week (y, 1, 1);
      int ld_fw, save_dow, dowdiff;

      if (U == w || V == w)
	{
	  ld_fw = 7 - dow2;

	  if (w == 0)
	    {
	      dowdiff = dow - dow2;
	      d = dow2 == 0 ? 32 - (7 - dow) : dowdiff < 0 ?
		32 + dowdiff : 1 + dowdiff;
	      m = dow2 == 0 || dowdiff < 0 ? 12 : 1;
	      y = dow2 == 0 || dowdiff < 0 ? y - 1 : y;
	    }
	  else
	    {
	      d = dow2 == 0 ? 1 : ld_fw + 1;
	      m = 1;
	      if (db_add_weeks_and_days_to_date (&d, &m, &y, w - 1, dow) ==
		  ER_FAILED)
		{
		  goto error;
		}
	    }
	}
      else if (u == w || v == w)
	{
	  ld_fw = dow2 == 0 ? 1 : 7 - dow2 + 1;
	  if (w == 0 || w == 1)
	    {
	      save_dow = dow;
	      dow = dow == 0 ? 7 : dow;
	      dow2 = dow2 == 0 ? 7 : dow2;
	      dowdiff = dow - dow2;

	      if (dow2 >= 1 && dow2 <= 4)	/* start with week 1 */
		{
		  d = w == 0 ? 32 + dowdiff - 7 :
		    dowdiff < 0 ? 32 + dowdiff : 1 + dowdiff;
		  m = w == 0 || dowdiff < 0 ? 12 : 1;
		  y = w == 0 || dowdiff < 0 ? y - 1 : y;
		}
	      else
		{
		  d = dowdiff < 0 ? (w == 0 ? 32 + dowdiff : ld_fw + dow) :
		    (w == 0 ? 1 + dowdiff : 1 + dowdiff + 7);
		  m = dowdiff < 0 && w == 0 ? 12 : 1;
		  y = dowdiff < 0 && w == 0 ? y - 1 : y;
		}
	      dow = save_dow;
	    }
	  else
	    {
	      d = ld_fw + 1;
	      m = 1;

	      if (db_add_weeks_and_days_to_date (&d, &m, &y,
						 dow2 >= 1
						 && dow2 <= 4 ? w - 2 : w - 1,
						 dow == 0 ? 6 : dow - 1) ==
		  ER_FAILED)
		{
		  goto error;
		}
	    }
	}
      else
	{
	  goto conversion_error;	/* should not happen */
	}
    }
  /* 3.3 - we have the day of year */
  else if (doy != -1)
    {
      for (m = 1; doy > days[m] && m <= 12; m++)
	{
	  doy -= days[m];
	}

      d = doy;
    }

write_results:
  /* last validations before writing results - we need only complete data info */

  if (res_type == DB_TYPE_DATE || res_type == DB_TYPE_DATETIME)
    {
      if (y <= 0 || m <= 0 || d <= 0)
	{
	  goto conversion_error;
	}
    }

  if (res_type == DB_TYPE_TIME || res_type == DB_TYPE_DATETIME)
    {
      if (h < 0 || mi < 0 || s < 0)
	{
	  goto conversion_error;
	}
    }

  if (res_type == DB_TYPE_DATE)
    {
      DB_MAKE_DATE (result, m, d, y);
    }
  else if (res_type == DB_TYPE_TIME)
    {
      DB_MAKE_TIME (result, h, mi, s);
    }
  else if (res_type == DB_TYPE_DATETIME)
    {
      DB_DATETIME db_datetime;

      db_datetime_encode (&db_datetime, m, d, y, h, mi, s, 0);

      DB_MAKE_DATETIME (result, &db_datetime);
    }

error:
  if (format_s)
    {
      db_private_free (NULL, format_s);
    }

  return error_status;

conversion_error:
  if (format_s)
    {
      db_private_free (NULL, format_s);
    }

  error_status = ER_DATE_CONVERSION;
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
  DB_MAKE_NULL (result);

  return error_status;
}

/*
 * db_date_dbval () - extract the date from input parameter.
 *   return: NO_ERROR, or ER_code
 *   result(out) : resultant db_value
 *   date_value(in) : date or datetime expression
 */
int
db_date_dbval (const DB_VALUE * date_value, DB_VALUE * result)
{
  DB_TYPE type;
  DB_DATETIME db_datetime, *dt_p;
  DB_TIMESTAMP *ts_p;
  DB_DATE db_date, *d_p;
  char *date_s = NULL, temp[64], *res_s;
  int y, m, d;
  int error_status = NO_ERROR;

  if (date_value == NULL || result == NULL)
    {
      return ER_FAILED;
    }

  y = m = d = 0;

  type = DB_VALUE_DOMAIN_TYPE (date_value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (date_value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  switch (type)
    {
    case DB_TYPE_DATETIME:
      dt_p = DB_GET_DATETIME (date_value);
      db_datetime_decode (dt_p, &m, &d, &y, NULL, NULL, NULL, NULL);
      break;

    case DB_TYPE_DATE:
      d_p = DB_GET_DATE (date_value);
      db_date_decode (d_p, &m, &d, &y);
      break;

    case DB_TYPE_TIMESTAMP:
      ts_p = DB_GET_TIMESTAMP (date_value);
      db_timestamp_decode (ts_p, &db_date, NULL);
      db_date_decode (&db_date, &m, &d, &y);
      break;

    case DB_TYPE_STRING:
    case DB_TYPE_CHAR:
      date_s = DB_GET_STRING (date_value);
      if (date_s == NULL)
	{
	  return ER_FAILED;
	}

      if (db_string_to_datetime (date_s, &db_datetime) != NO_ERROR)
	{
	  return ER_DATE_CONVERSION;
	}

      db_datetime_decode (&db_datetime, &m, &d, &y, NULL, NULL, NULL, NULL);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_PARAMETER,
	      0);
      return ER_QPROC_INVALID_PARAMETER;
    }

  sprintf (temp, "%02d/%02d/%04d", m, d, y);

  res_s = db_private_alloc (NULL, strlen (temp) + 1);
  if (res_s == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  strcpy (res_s, temp);
  DB_MAKE_STRING (result, res_s);

  return error_status;
}

/*
 *  count_leap_years_up_to - count the leap years up to year
 *  return: the counted value
 *  year(in) : the last year to evaluate
 */
int
count_leap_years_up_to (int year)
{
  return year / 4 - year / 100 + year / 400;
}

/*
 *  count_nonleap_years_up_to - count the non leap years up to year
 *  return: the counted value
 *  year(in) : the last year to evaluate
 */
int
count_nonleap_years_up_to (int year)
{
  return year - count_leap_years_up_to (year);
}

/*
 * db_date_diff () - expr1 – expr2 expressed as a value in days from
 *		     one date to the other.
 *   return: int
 *   result(out) : resultant db_value
 *   date_value1(in)   : first date
 *   date_value2(in)   : second date
 */
int
db_date_diff (const DB_VALUE * date_value1, const DB_VALUE * date_value2,
	      DB_VALUE * result)
{
  DB_TYPE type1, type2;
  DB_DATETIME db_datetime1, db_datetime2, *dt1_p, *dt2_p;
  DB_TIMESTAMP *ts1_p, *ts2_p;
  DB_DATE db_date1, db_date2, *d1_p, *d2_p;
  int y1 = 0, m1 = 0, d1 = 0;
  int y2 = 0, m2 = 0, d2 = 0;
  int cly1, cly2, cnly1, cnly2, cdpm1, cdpm2, cdpy1, cdpy2, diff, i, cd1, cd2;
  int m_days[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  char *date_s1 = NULL, *date_s2 = NULL;
  int error_status = NO_ERROR;

  if (date_value1 == NULL || date_value2 == NULL || result == NULL)
    {
      error_status = ER_FAILED;
      goto error;
    }

  type1 = DB_VALUE_DOMAIN_TYPE (date_value1);
  if (type1 == DB_TYPE_NULL || DB_IS_NULL (date_value1))
    {
      DB_MAKE_NULL (result);
      goto error;
    }

  type2 = DB_VALUE_DOMAIN_TYPE (date_value2);
  if (type2 == DB_TYPE_NULL || DB_IS_NULL (date_value2))
    {
      DB_MAKE_NULL (result);
      goto error;
    }

  switch (type1)
    {
    case DB_TYPE_STRING:
    case DB_TYPE_CHAR:
      date_s1 = DB_GET_STRING (date_value1);
      if (date_s1 == NULL)
	{
	  error_status = ER_FAILED;
	  goto error;
	}

      if (db_string_to_datetime (date_s1, &db_datetime1) != NO_ERROR)
	{
	  error_status = ER_DATE_CONVERSION;
	  DB_MAKE_NULL (result);
	  goto error;
	}

      db_datetime_decode (&db_datetime1, &m1, &d1, &y1, NULL, NULL, NULL,
			  NULL);
      break;

    case DB_TYPE_DATETIME:
      dt1_p = DB_GET_DATETIME (date_value1);
      db_datetime_decode (dt1_p, &m1, &d1, &y1, NULL, NULL, NULL, NULL);
      break;

    case DB_TYPE_DATE:
      d1_p = DB_GET_DATE (date_value1);
      db_date_decode (d1_p, &m1, &d1, &y1);
      break;

    case DB_TYPE_TIMESTAMP:
      ts1_p = DB_GET_TIMESTAMP (date_value1);
      db_timestamp_decode (ts1_p, &db_date1, NULL);
      db_date_decode (&db_date1, &m1, &d1, &y1);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_INVALID_PARAMETER, 0);
      error_status = ER_FAILED;
      goto error;
    }

  switch (type2)
    {
    case DB_TYPE_STRING:
    case DB_TYPE_CHAR:
      date_s2 = DB_GET_STRING (date_value2);
      if (date_s2 == NULL)
	{
	  error_status = ER_FAILED;
	  goto error;
	}

      if (db_string_to_datetime (date_s2, &db_datetime2) != NO_ERROR)
	{
	  error_status = ER_DATE_CONVERSION;
	  DB_MAKE_NULL (result);
	  goto error;
	}

      db_datetime_decode (&db_datetime2, &m2, &d2, &y2, NULL, NULL, NULL,
			  NULL);
      break;

    case DB_TYPE_DATETIME:
      dt2_p = DB_GET_DATETIME (date_value2);
      db_datetime_decode (dt2_p, &m2, &d2, &y2, NULL, NULL, NULL, NULL);
      break;

    case DB_TYPE_DATE:
      d2_p = DB_GET_DATE (date_value2);
      db_date_decode (d2_p, &m2, &d2, &y2);
      break;

    case DB_TYPE_TIMESTAMP:
      ts2_p = DB_GET_TIMESTAMP (date_value2);
      db_timestamp_decode (ts2_p, &db_date2, NULL);
      db_date_decode (&db_date2, &m2, &d2, &y2);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_INVALID_PARAMETER, 0);
      error_status = ER_FAILED;
      goto error;
    }

  cly1 = count_leap_years_up_to (y1 - 1);
  cnly1 = count_nonleap_years_up_to (y1 - 1);
  cdpy1 = cly1 * 366 + cnly1 * 365;
  m_days[2] = LEAP (y1) ? 29 : 28;
  cdpm1 = 0;
  for (i = 1; i < m1; i++)
    {
      cdpm1 += m_days[i];
    }

  cly2 = count_leap_years_up_to (y2 - 1);
  cnly2 = count_nonleap_years_up_to (y2 - 1);
  cdpy2 = cly2 * 366 + cnly2 * 365;
  m_days[2] = LEAP (y2) ? 29 : 28;
  cdpm2 = 0;
  for (i = 1; i < m2; i++)
    {
      cdpm2 += m_days[i];
    }

  cd1 = cdpy1 + cdpm1 + d1;
  cd2 = cdpy2 + cdpm2 + d2;
  diff = cd1 - cd2;

  DB_MAKE_INTEGER (result, diff);

error:
  return error_status;
}

/*
 *  parse_time_string - parse a string given by the second argument of
 *                      timestamp function
 *  return: NO_ERROR
 *
 *  timestr(in) : input string
 *  sign(out)   : 0 if positive, -1 if negative
 *  h(out)      : hours
 *  m(out)      : minutes
 *  s(out)      : seconds
 *  ms(out)     : milliseconds
 */
static int
parse_time_string (const char *timestr, int *sign, int *h,
		   int *m, int *s, int *ms)
{
  int args[4], num_args = 0;
  const char *ch;
  char *dot = NULL;

  assert (sign != NULL && h != NULL && m != NULL && s != NULL && ms != NULL);
  *sign = *h = *m = *s = *ms = 0;

  if (timestr == NULL || strlen (timestr) == 0)
    {
      return NO_ERROR;
    }

  ch = timestr;
  SKIP_SPACES (ch);

  if (*ch == '-')
    {
      *sign = 1;
      ch++;
    }

  /* Find dot('.') to separate milli-seconds part from whole string. */
  dot = strchr (ch, '.');
  if (dot)
    {
      char ms_string[4];
      strncpy (ms_string, dot + 1, 3);
      ms_string[3] = '\0';

      switch (strlen (ms_string))
	{
	case 0:
	  *ms = 0;
	  break;

	case 1:
	  ms_string[1] = '0';
	case 2:
	  ms_string[2] = '0';
	default:
	  *ms = atoi (ms_string);
	}
    }

  /* First ':' character means '0:'. */
  SKIP_SPACES (ch);
  if (*ch == ':')
    {
      args[num_args++] = 0;
      ch++;
    }

  while (char_isdigit (*ch))
    {
      const char *start = ch;
      do
	{
	  ch++;
	}
      while (char_isdigit (*ch));

      args[num_args++] = atoi (start);

      /* Digits should be separated by ':' character.
       * If we meet other characters, stop parsing.
       */
      if (*ch != ':')
	{
	  break;
	}
      ch++;
    }

  switch (num_args)
    {
    case 1:
      /* Consider single value as H...HMMSS. */
      *s = args[0] % 100;
      args[0] /= 100;
      *m = args[0] % 100;
      *h = args[0] / 100;
      break;

    case 2:
      *h = args[0];
      *m = args[1];
      break;

    case 3:
      *h = args[0];
      *m = args[1];
      *s = args[2];
      break;

    case 0:
    default:
      /* do nothing */
      ;
    }
  return NO_ERROR;
}
