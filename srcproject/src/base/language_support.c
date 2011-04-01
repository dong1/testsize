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
 * language_support.c : Multi-language and character set support
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>

#include "chartype.h"
#include "misc_string.h"
#include "language_support.h"
#include "authenticate.h"
#include "environment_variable.h"
#include "db.h"
/* this must be the last header file included! */
#include "dbval.h"

static char lang_Loc_name[LANG_MAX_LANGNAME];
static INTL_LANG lang_Loc_id = INTL_LANG_NONE;
static INTL_CODESET lang_Loc_charset = INTL_CODESET_NONE;
static int lang_Loc_bytes_per_char = 1;
static DB_CURRENCY lang_Loc_currency = DB_CURRENCY_NULL;

static bool lang_Initialized = false;

/*
 * lang_init - Initializes any global state required by the multi-language
 *             module
 *   return: true if success
 */
bool
lang_init (void)
{
  const char *env, *charset;

  if (lang_Initialized)
    {
      return lang_Initialized;
    }

  /*
   * Determine the locale by examining environment variables.
   * First we check our internal variable CUBRID_LANG to allow CUBRID
   * to operate in a different locale than is set with LANG.  This is
   * necessary when lang must be set to something other than one of the
   * recognized settings used to select message catalogs in
   * $CUBRID/admin/msg.
   */
  env = envvar_get ("LANG");
  if (env != NULL)
    {
      strncpy (lang_Loc_name, env, sizeof (lang_Loc_name));
    }
  else
    {
      /* allow environment to override the locale settings */
      env = getenv ("LANG");
      if (env != NULL)
	{
	  strncpy (lang_Loc_name, env, sizeof (lang_Loc_name));
	}
      else
	{
	  /* if unset, use default */
	  strcpy (lang_Loc_name, LANG_NAME_DEFAULT);
	}
    }

  /* Set up some internal constants based on the locale name */

  /* compare until null character for "C" */
  if (strncasecmp (lang_Loc_name, LANG_NAME_C, sizeof (LANG_NAME_C)) == 0)
    {
      strcpy (lang_Loc_name, LANG_NAME_DEFAULT);
    }
  else if (strncasecmp (lang_Loc_name, LANG_NAME_ENGLISH,
			sizeof (LANG_NAME_ENGLISH) - 1) == 0)
    {
      lang_Loc_id = INTL_LANG_ENGLISH;
      lang_Loc_currency = DB_CURRENCY_DOLLAR;
      lang_Loc_bytes_per_char = 1;
      charset = lang_Loc_name + sizeof (LANG_NAME_ENGLISH) - 1;
      if (*charset == '.')
	{
	  charset++;
	}
      if (*charset == '\0')
	{
	  /* "en_US" */
	  lang_Loc_charset = INTL_CODESET_ISO88591;
	}
      else if (strcasecmp (charset, LANG_CHARSET_ISO8859) == 0)
	{
	  /* "en_US.iso8859" */
	  lang_Loc_charset = INTL_CODESET_ISO88591;
	}
      else if (strcasecmp (charset, LANG_CHARSET_UTF8) == 0)
	{
	  /* "en_US.utf8" */
	  lang_Loc_charset = INTL_CODESET_UTF8;
	}
      else
	{
	  lang_Loc_id = INTL_LANG_NONE;
	}
    }
  else if (strncasecmp (lang_Loc_name, LANG_NAME_KOREAN,
			sizeof (LANG_NAME_KOREAN) - 1) == 0)
    {
      lang_Loc_id = INTL_LANG_KOREAN;
      lang_Loc_currency = DB_CURRENCY_WON;
      charset = lang_Loc_name + sizeof (LANG_NAME_KOREAN) - 1;
      if (*charset == '.')
	{
	  charset++;
	}
      if (*charset == '\0')
	{
	  /* "ko_KR" */
	  lang_Loc_charset = INTL_CODESET_KSC5601_EUC;
	  lang_Loc_bytes_per_char = 2;
	}
      else if (strcasecmp (charset, LANG_CHARSET_EUCKR) == 0)
	{
	  /* "ko_KR.euckr" */
	  lang_Loc_charset = INTL_CODESET_KSC5601_EUC;
	  lang_Loc_bytes_per_char = 2;
	}
      else if (strcasecmp (charset, LANG_CHARSET_UTF8) == 0)
	{
	  /* "ko_KR.utf8" */
	  lang_Loc_charset = INTL_CODESET_UTF8;
	  lang_Loc_bytes_per_char = 3;
	}
      else
	{
	  lang_Loc_id = INTL_LANG_NONE;
	}
    }
  else if (strncasecmp (lang_Loc_name, LANG_NAME_CHINESE,
			sizeof (LANG_NAME_CHINESE) - 1) == 0)
    {
      lang_Loc_id = INTL_LANG_CHINESE;
      lang_Loc_currency = DB_CURRENCY_YUAN;
      charset = lang_Loc_name + sizeof (LANG_NAME_CHINESE) - 1;
      if (*charset == '.')
	{
	  charset++;
	}
      if (*charset == '\0')
	{
	  /* "zh_CN" */
	  lang_Loc_charset = INTL_CODESET_GB2312;
	  lang_Loc_bytes_per_char = 2;
	}
      else if (strcasecmp (charset, LANG_CHARSET_GB2312) == 0)
	{
	  /* "zh_CN.gb2312" */
	  lang_Loc_charset = INTL_CODESET_GB2312;
	  lang_Loc_bytes_per_char = 2;
	}
      else if (strcasecmp (charset, LANG_CHARSET_UTF8) == 0)
	{
	  /* "zh_CN.utf8" */
	  lang_Loc_charset = INTL_CODESET_UTF8;
	  lang_Loc_bytes_per_char = 3;
	}
      else
	{
	  lang_Loc_id = INTL_LANG_NONE;
	}
    }
  else
    {
      lang_Loc_id = INTL_LANG_NONE;
    }

  /*
   * Adjust locale string standard form
   * because the string is compared case insensitively.
   * 'lang_Loc_name' should be the directory name of message catalog set.
   */
  switch (lang_Loc_id)
    {
    case INTL_LANG_ENGLISH:
      strcpy (lang_Loc_name, LANG_NAME_ENGLISH);
      break;
    case INTL_LANG_KOREAN:
      if (lang_Loc_charset == INTL_CODESET_KSC5601_EUC)
	{
	  strcpy (lang_Loc_name, LANG_NAME_KOREAN "." LANG_CHARSET_EUCKR);
	}
      else if (lang_Loc_charset == INTL_CODESET_UTF8)
	{
	  strcpy (lang_Loc_name, LANG_NAME_KOREAN "." LANG_CHARSET_UTF8);
	}
      break;
    case INTL_LANG_CHINESE:
      if (lang_Loc_charset == INTL_CODESET_GB2312)
	{
	  strcpy (lang_Loc_name, LANG_NAME_CHINESE "." LANG_CHARSET_GB2312);
	}
      else if (lang_Loc_charset == INTL_CODESET_UTF8)
	{
	  strcpy (lang_Loc_name, LANG_NAME_CHINESE "." LANG_CHARSET_UTF8);
	}
      break;
    case INTL_LANG_NONE:
    default:
      printf ("Unsupported language setting CUBRID_LANG=%s."
	      " Set to default %s\n", lang_Loc_name, LANG_NAME_DEFAULT);
      /* if unrecognized, use default */
      strcpy (lang_Loc_name, LANG_NAME_DEFAULT);
      lang_Loc_id = INTL_LANG_ENGLISH;
      lang_Loc_charset = INTL_CODESET_ISO88591;
      lang_Loc_bytes_per_char = 1;
      lang_Loc_currency = DB_CURRENCY_DOLLAR;
      break;
    }

  lang_Initialized = true;
  return (lang_Initialized);
}

/*
 * lang_name - returns language name per env settings.
 *   return: language name string
 */
const char *
lang_name (void)
{
  if (!lang_Initialized)
    {
      lang_init ();
    }
  return lang_Loc_name;
}

/*
 * lang_id - Returns language id per env settings
 *   return: language identifier
 */
INTL_LANG
lang_id (void)
{
  if (!lang_Initialized)
    {
      lang_init ();
    }
  return lang_Loc_id;
}

/*
 * lang_currency - Returns language currency per env settings
 *   return: language currency identifier
 */
DB_CURRENCY
lang_currency ()
{
  if (!lang_Initialized)
    {
      lang_init ();
    }
  return lang_Loc_currency;
}

/*
 * lang_charset - Returns language charset per env settings
 *   return: language charset
 */
INTL_CODESET
lang_charset (void)
{
  if (!lang_Initialized)
    {
      lang_init ();
    }
  return lang_Loc_charset;
}

/*
 * lang_loc_bytes_per_char - Returns language charset maximum bytes per char
 *   return: charset maximum bytes per char per
 */
int
lang_loc_bytes_per_char (void)
{
  if (!lang_Initialized)
    {
      lang_init ();
    }
  return lang_Loc_bytes_per_char;
}

/*
 * lang_final - Releases any resources held by this module
 *   return: none
 */
void
lang_final (void)
{
  lang_Initialized = false;
}

/*
 * lang_currency_symbol - Computes an appropriate printed representation for
 *                        a currency identifier
 *   return: currency string
 *   curr(in): currency constant
 */
const char *
lang_currency_symbol (DB_CURRENCY curr)
{
  const char *symbol;

  switch (curr)
    {
    case DB_CURRENCY_DOLLAR:
      symbol = "$";
      break;
    case DB_CURRENCY_WON:
      symbol = "\\";
      break;
    case DB_CURRENCY_YUAN:
      symbol = "Y";
      break;
    default:
      /* If we can't identify it, print nothing */
      symbol = "";
      break;
    }
  return (symbol);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_char_mem_size - Returns the character memory size for the given
 *                      pointer to a character
 *   return: memory size for the first character
 *   p(in)
 */
int
lang_char_mem_size (const char *p)
{
  if (LANG_VARIABLE_CHARSET (lang_charset ()))
    {
      if (0x80 & (p[0]))
	{
	  return 2;
	}
    }
  return 1;
}

/*
 * lang_char_screen_size - Returns the screen size for the given pointer
 *                         to a character
 *   return: screen size for the first character
 *   p(in)
 */
int
lang_char_screen_size (const char *p)
{
  if (LANG_VARIABLE_CHARSET (lang_charset ()))
    {
      return (0x80 & (p[0]) ? 2 : 1);
    }
  return 1;
}

/*
 * lang_wchar_mem_size - Returns the memory size for the given pointer
 *                       to a wide character
 *   return: memory size for the first character
 *   p(in)
 */
int
lang_wchar_mem_size (const wchar_t * p)
{
  if (LANG_VARIABLE_CHARSET (lang_charset ()))
    {
      if (0x8000 & (p[0]))
	{
	  return 2;
	}
    }
  return 1;
}

/*
 * lang_wchar_screen_size - Returns the screen size for the given pointer
 *                          to a wide character
 *   return: screen size for the first character
 *   p(in)
 */
int
lang_wchar_screen_size (const wchar_t * p)
{
  if (LANG_VARIABLE_CHARSET (lang_charset ()))
    {
      return (0x8000 & (p[0]) ? 2 : 1);
    }
  return 1;
}
#endif

/*
 * lang_check_identifier - Tests an identifier for possibility
 *   return: true if the name is suitable for identifier,
 *           false otherwise.
 *   name(in): identifier name
 *   length(in): identifier name length
 */
bool
lang_check_identifier (const char *name, int length)
{
  bool ok = false;
  int i;

  if (name == NULL)
    {
      return false;
    }

  if (char_isalpha (name[0]))
    {
      ok = true;
      for (i = 0; i < length && ok; i++)
	{
	  if (!char_isalnum (name[i]) && name[i] != '_')
	    {
	      ok = false;
	    }
	}
    }

  return (ok);
}


#if defined (CS_MODE) || defined (SA_MODE)
static DB_CHARSET lang_Server_charset;

static const DB_CHARSET lang_Db_charsets[] = {
  {"ascii", "US English charset - ASCII encoding", " ", INTL_CODESET_ASCII, 0,
   1},
  {"raw-bits", "Uninterpreted bits - Raw encoding", "", INTL_CODESET_RAW_BITS,
   0, 1},
  {"raw-bytes", "Uninterpreted bytes - Raw encoding", "",
   INTL_CODESET_RAW_BYTES, 0, 1},
  {"iso8859-1", "Latin 1 charset - ISO 8859 encoding", " ",
   INTL_CODESET_ISO88591, 0, 1},
  {"utf-8", "UTF-8 encoding", " ", INTL_CODESET_UTF8, 0, 1},
  {"euc-kr", "KSC 5601 1990 charset - EUC encoding", "\241\241",
   INTL_CODESET_KSC5601_EUC, 0, 2},
  {"euc-cn", "GB2312 charset - EUC encoding", "\241\241", INTL_CODESET_GB2312,
   0, 2},
  {"", "", "", INTL_CODESET_NONE, 0, 0}
};

static int lang_Server_charset_Initialized = 0;

/*
 * lang_server_charset_init - Initializes the global value of the server's
 *                            charset
 *   return: none
 *
 * Note: This is the charset that is bound to the database at the time
 *       of creation.
 */
void
lang_server_charset_init (void)
{
  DB_VALUE value;
  INTL_CODESET server_codeset;
  int i;

  /* default */
  server_codeset = lang_charset ();

  /* Determine the Server's charset */

  /* Currently can't read the db_root table while on the server.
   * Temporarily just get the server's codeset from the locale if
   * on the server.
   *
   * The following is safe since this is a client only function. If this
   * needs to move to the server, the db_get must be pre-processed out
   * or something ...
   */
  if (Au_root)
    {
      /* Can't find the server's codeset.  This should only happen if using
       * a database prior to NCHAR implementation, or prior to completely
       * logging in.  If so, set the server codeset to be ASCII until
       * the the db_get can work correctly.  All string handling prior
       * to that time will be done without conversion.
       */
      if (db_get (Au_root, "charset", &value) == NO_ERROR)
	{
	  server_codeset = (INTL_CODESET) db_get_int (&value);
	}
    }

  /* Find the charset in the Db_Charsets array */
  for (i = 0; lang_Db_charsets[i].charset_id != INTL_CODESET_NONE; i++)
    {
      if (lang_Db_charsets[i].charset_id == server_codeset)
	{
	  lang_Server_charset = lang_Db_charsets[i];
	  lang_Server_charset_Initialized = 1;
	  return;
	}
    }

  /* Server's codeset not found in the list; Initialize to ASCII */
  lang_Server_charset = lang_Db_charsets[0];
  return;
}

/*
 * lang_set_national_charset - Changes the charset definition for NCHAR
 *   return: error code
 *   charset_name(in): desired charset name
 *
 * Note: This should be called only by the DBA.
 */
int
lang_set_national_charset (const char *charset_name)
{
  INTL_CODESET server_codeset;
  DB_VALUE value;
  int i;

  server_codeset = lang_charset ();

  if (charset_name != NULL)
    {
      /* Find the charset in the lang_Db_charsets array */
      for (i = 0; lang_Db_charsets[i].charset_id != INTL_CODESET_NONE; i++)
	{
	  if (strcmp (lang_Db_charsets[i].charset_name, charset_name) == 0)
	    {
	      server_codeset = lang_Db_charsets[i].charset_id;
	      break;
	    }
	}
    }

  db_make_int (&value, (int) server_codeset);
  return db_put_internal (Au_root, "charset", &value);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_server_db_charset - Initializes if necessary, then return server's
 *                          charset
 *   return: DB_CHARSET structure associated with the server
 */
DB_CHARSET
lang_server_db_charset (void)
{
  if (!lang_Server_charset_Initialized)
    {
      lang_server_charset_init ();
    }
  return lang_Server_charset;
}
#endif

/*
 * lang_server_charset_id - Initializes if necessary, then return server's
 *                          charset_id
 *   return: INTL_CODESET of the server's charset
 */
INTL_CODESET
lang_server_charset_id (void)
{
  if (!lang_Server_charset_Initialized)
    {
      lang_server_charset_init ();
    }
  return lang_Server_charset.charset_id;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_server_space_char - Initializes if necessary, then return server's
 *                          space character
 *   return: none
 *   space(out): string containing the space character for the server's charset
 *   size(out): number of bytes in the space char
 */
void
lang_server_space_char (char *space, int *size)
{
  if (!lang_Server_charset_Initialized)
    {
      lang_server_charset_init ();
    }
  (void) strcpy (space, lang_Server_charset.space_char);
  *size = lang_Server_charset.space_size;
}

/*
 * lang_server_charset_name - Initializes if necessary, then return server's
 *                            charset name
 *   return: none
 *   name(out): the name of the server's charset
 */
void
lang_server_charset_name (char *name)
{
  if (!lang_Server_charset_Initialized)
    {
      lang_server_charset_init ();
    }
  (void) strcpy (name, lang_Server_charset.charset_name);
}

/*
 * lang_server_charset_desc - Initializes if necessary, then return server's
 *                            charset desc
 *   return: none
 *   desc(out): the description of the server's charset
 */
void
lang_server_charset_desc (char *desc)
{
  if (!lang_Server_charset_Initialized)
    {
      lang_server_charset_init ();
    }
  (void) strcpy (desc, lang_Server_charset.charset_desc);
}
#endif

/*
 * lang_charset_space_char - Returns the space character and its size in bytes
 *                           for a codeset
 *   return: NO_ERROR or error code
 *   codeset(in): INTL_CODESET of the desired charset
 *   space_char(out): character string holding the space character
 *   space_size(out): the number of bytes in the space character
 *
 * Note: This routine assumes that the calling routine has allocated
 *       enough space for space_char, which will use 3 bytes at a maximum.
 */
int
lang_charset_space_char (INTL_CODESET codeset, char *space_char,
			 int *space_size)
{
  int i;

  /* Find the charset in the Db_Charsets array */
  for (i = 0; lang_Db_charsets[i].charset_id != INTL_CODESET_NONE; i++)
    {
      if (lang_Db_charsets[i].charset_id == codeset)
	{
	  *space_size = lang_Db_charsets[i].space_size;
	  (void) memcpy (space_char, lang_Db_charsets[i].space_char,
			 (*space_size));
	  return NO_ERROR;
	}
    }

  *space_size = 0;
  space_char[0] = '\0';
  return ER_FAILED;
}
#endif /* CS_MODE || SA_MODE */
