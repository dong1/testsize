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
 * cnvlex.h - Lexical scanning interface for string conversion API.
 */

#ifndef _CNVLEX_H_
#define _CNVLEX_H_

#ident "$Id$"

#include "language_support.h"

/*
 * Lexical scan modes. These correspond directly to start conditions defined
 * in the scanner definition file. Be sure to update db_fmt_lex_start() to
 * maintain the correspondence.
 */

typedef enum
{
  FL_LOCAL_NUMBER = 0,
  FL_ENGLISH_NUMBER,
  FL_KOREA_NUMBER,
  FL_CHINA_NUMBER,
  FL_LOCAL_TIME,
  FL_ENGLISH_TIME,
  FL_KOREA_TIME,
  FL_CHINA_TIME,
  FL_INTEGER_FORMAT,
  FL_TIME_FORMAT,
  FL_BIT_STRING_FORMAT,
  FL_BIT_STRING,
  FL_VALIDATE_DATE_FORMAT,
  FL_VALIDATE_FLOAT_FORMAT,
  FL_VALIDATE_INTEGER_FORMAT,
  FL_VALIDATE_MONETARY_FORMAT,
  FL_VALIDATE_TIME_FORMAT,
  FL_VALIDATE_TIMESTAMP_FORMAT,
  FL_VALIDATE_BIT_STRING_FORMAT
} FMT_LEX_MODE;


typedef enum
{
  FT_NONE = 0,
  FT_AM_PM,
  FT_BLANKS,
  FT_BINARY_DIGITS,
  FT_BIT_STRING_FORMAT,
  FT_CURRENCY,
  FT_DATE,
  FT_DATE_FORMAT,
  FT_DATE_SEPARATOR,
  FT_LOCAL_DATE_SEPARATOR,
  FT_DECIMAL,
  FT_FLOAT_FORMAT,
  FT_HOUR,
  FT_HEX_DIGITS,
  FT_INTEGER_FORMAT,
  FT_MINUS,
  FT_MINUTE,
  FT_MONETARY_FORMAT,
  FT_MONTH,
  FT_MONTHDAY,
  FT_MONTH_LONG,
  FT_NUMBER,
  FT_PATTERN,
  FT_PLUS,
  FT_SECOND,
  FT_SPACES,
  FT_STARS,
  FT_THOUSANDS,
  FT_TIME,
  FT_TIME_DIGITS,
  FT_TIME_DIGITS_ANY,
  FT_TIME_DIGITS_0,
  FT_TIME_DIGITS_BLANK,
  FT_TIME_FORMAT,
  FT_TIME_SEPARATOR,
  FT_LOCAL_TIME_SEPARATOR,
  FT_UNKNOWN,
  FT_TIMESTAMP,
  FT_TIMESTAMP_FORMAT,
  FT_WEEKDAY,
  FT_WEEKDAY_LONG,
  FT_YEAR,
  FT_ZEROES,
  FT_ZONE,
  FT_MILLISECOND
} FMT_TOKEN_TYPE;

typedef struct fmt_token FMT_TOKEN;
struct fmt_token
{
  FMT_TOKEN_TYPE type;
  const char *text;
  int length;
  const char *raw_text;
  int value;
};

extern void cnv_fmt_analyze (const char *instring, FMT_LEX_MODE mode);
extern FMT_TOKEN_TYPE cnv_fmt_lex (FMT_TOKEN * token);
extern void cnv_fmt_unlex (void);
extern const char *cnv_fmt_next_token (void);
extern FMT_LEX_MODE cnv_fmt_number_mode (INTL_LANG zone);
extern FMT_LEX_MODE cnv_fmt_time_mode (INTL_LANG zone);
extern void cnv_fmt_exit (void);
#endif /* _CNVLEX_H_ */
