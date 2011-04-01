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
 * language_support.h : Multi-language and character set support
 *
 */

#ifndef _LANGUAGE_SUPPORT_H_
#define _LANGUAGE_SUPPORT_H_

#ident "$Id$"

#include <stddef.h>

#include "intl_support.h"

/*
 * currently recognized language names.
 */
#define LANG_NAME_C		"C"
#define LANG_NAME_ENGLISH	"en_US"
#define LANG_NAME_KOREAN	"ko_KR"
#define LANG_NAME_CHINESE       "zh_CN"
#define LANG_NAME_DEFAULT 	LANG_NAME_ENGLISH
#define LANG_CHARSET_ISO8859	"iso8859"
#define LANG_CHARSET_EUCKR      "euckr"
#define LANG_CHARSET_GB2312     "gb2312"
#define LANG_CHARSET_UTF8       "utf8"

/*
 * message for fundamental error that occur before any messages catalogs
 * can be accessed or opened.
 */
#define LANG_ERR_NO_CUBRID "The `%s' environment variable is not set.\n"

#define LANG_MAX_LANGNAME       256
#define LANG_MAX_BYTES_PER_CHAR	2

#define LANG_VARIABLE_CHARSET(x) ((x) != INTL_CODESET_ASCII     && \
				  (x) != INTL_CODESET_RAW_BITS  && \
				  (x) != INTL_CODESET_RAW_BYTES && \
				  (x) != INTL_CODESET_ISO88591)


typedef struct db_charset DB_CHARSET;
struct db_charset
{
  const char *charset_name;
  const char *charset_desc;
  const char *space_char;
  INTL_CODESET charset_id;
  int default_collation;
  int space_size;
};

#ifdef __cplusplus
extern "C"
{
#endif

  extern bool lang_init (void);
  extern void lang_final (void);
  extern const char *lang_name (void);
  extern INTL_LANG lang_id (void);
  extern INTL_CODESET lang_charset (void);
  extern int lang_loc_bytes_per_char (void);
  extern DB_CURRENCY lang_currency (void);
  extern const char *lang_currency_symbol (DB_CURRENCY curr);
#if defined(ENABLE_UNUSED_FUNCTION)
  extern int lang_char_mem_size (const char *p);
  extern int lang_char_screen_size (const char *p);
  extern int lang_wchar_mem_size (const wchar_t * p);
  extern int lang_wchar_screen_size (const wchar_t * p);
#endif
  extern bool lang_check_identifier (const char *name, int length);

#if defined (CS_MODE) || defined (SA_MODE)
  extern void lang_server_charset_init (void);
  extern int lang_set_national_charset (const char *charset_name);
  extern INTL_CODESET lang_server_charset_id (void);
#if defined(ENABLE_UNUSED_FUNCTION)
  extern DB_CHARSET lang_server_db_charset (void);
  extern void lang_server_space_char (char *space, int *size);
  extern void lang_server_charset_name (char *name);
  extern void lang_server_charset_desc (char *desc);
#endif				/* ENABLE_UNUSED_FUNCTION */
  extern int lang_charset_space_char (INTL_CODESET codeset, char *space_char,
				      int *space_size);
#endif				/* CS_MODE || SA_MODE */

#ifdef __cplusplus
}
#endif

#endif				/* _LANGUAGE_SUPPORT_H_ */
