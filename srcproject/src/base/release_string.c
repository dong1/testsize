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
 * release_string.c - release related information (at client and server)
 *
 * Note: This file contains some very simple functions related to version and
 *       releases of CUBRID. Among these functions are copyright information
 *       of CUBRID products, name of CUBRID engine, and version.
 */

#ident "$Id$"

#include <stdlib.h>
#include <string.h>

#include "config.h"
#include "release_string.h"
#include "message_catalog.h"
#include "chartype.h"
#include "language_support.h"
#include "environment_variable.h"
#include "log_comm.h"
#include "log_manager.h"


#define CSQL_NAME_MAX_LENGTH 100
#define MAXPATCHLEN  256

/*
 * COMPATIBILITY_RULE - Structure that encapsulates compatibility rules.
 *                      For two revision levels, both a compatibility type
 *                      and optional fixup function list is defined.
 */
typedef struct compatibility_rule
{
  float base_level;
  float apply_level;
  REL_COMPATIBILITY compatibility;
  REL_FIXUP_FUNCTION *fix_function;
} COMPATIBILITY_RULE;

/*
 * Copyright Information
 */
static const char *copyright_header = "\
Copyright (C) 2010 Search Solution Corporation. All rights reserved by Search Solution.\n\
";

static const char *copyright_body = "\
Copyright Information\n\
";

/*
 * CURRENT VERSIONS
 */
#define makestring1(x) #x
#define makestring(x) makestring1(x)

static const char *release_string = makestring (RELEASE_STRING);
static const char *major_release_string = makestring (MAJOR_RELEASE_STRING);
static const char *major_version = makestring (MAJOR_VERSION);
static const char *minor_version = makestring (MINOR_VERSION);
static const char *patch_number = makestring (PATCH_NUMBER);
static const char *build_number = makestring (BUILD_NUMBER);
static const char *package_string = PACKAGE_STRING;
static int bit_platform = __WORDSIZE;
static int major_release_version = MAJOR_VERSION;
static int minor_release_version = MINOR_VERSION;
static int patch_release_number = PATCH_NUMBER;
/*
 * build_serial_number is not an int value because that BUILD_SERIAL_NUMBER
 * may equal to 0008, in C language, it's an octal with wrong format.
 */
static const char *build_serial_number = makestring (BUILD_SERIAL_NUMBER);


static REL_COMPATIBILITY
rel_is_compatible_internal (const char *base_rel_str,
			    const char *apply_rel_str,
			    COMPATIBILITY_RULE rules[]);

/*
 * Disk (database image) Version Compatibility
 */
static float disk_compatibility_level = 10.0f;

/*
 * rel_name - Name of the product from the message catalog
 *   return: static character string
 */
const char *
rel_name (void)
{
  return package_string;
}

/*
 * rel_release_string - Release number of the product
 *   return: static char string
 */
const char *
rel_release_string (void)
{
  return release_string;
}

/*
 * rel_major_version - Major release version of the product
 *   return: int
 */
int
rel_major_version (void)
{
  return major_release_version;
}

/*
* rel_minor_version - Minor release version of the product
*   return: int
*/
int
rel_minor_version (void)
{
  return minor_release_version;
}

/*
* rel_patch_number - Patch release number of the product
*   return: int
*/
int
rel_patch_number (void)
{
  return patch_release_number;
}

/*
* rel_build_serial_number - Build serial number portion of the release string
*   return: int
*/
int
rel_build_serial_number (void)
{
  return atoi (build_serial_number);
}

/*
 * rel_major_release_string - Major release portion of the release string
 *   return: static char string
 */
const char *
rel_major_release_string (void)
{
  return major_release_string;
}

/*
 * rel_build_number - Build bumber portion of the release string
 *   return: static char string
 */
const char *
rel_build_number (void)
{
  return build_number;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * rel_copyright_header - Copyright header from the message catalog
 *   return: static char string
 */
const char *
rel_copyright_header (void)
{
  const char *name;

  lang_init ();
  name = msgcat_message (MSGCAT_CATALOG_CUBRID, MSGCAT_SET_GENERAL,
			 MSGCAT_GENERAL_COPYRIGHT_HEADER);
  return (name) ? name : copyright_header;
}

/*
 * rel_copyright_body - Copyright body fromt he message catalog
 *   return: static char string
 */
const char *
rel_copyright_body (void)
{
  const char *name;

  lang_init ();
  name = msgcat_message (MSGCAT_CATALOG_CUBRID, MSGCAT_SET_GENERAL,
			 MSGCAT_GENERAL_COPYRIGHT_BODY);
  return (name) ? name : copyright_body;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * rel_disk_compatible - Disk compatibility level
 *   return:
 */
float
rel_disk_compatible (void)
{
  return disk_compatibility_level;
}


/*
 * rel_set_disk_compatible - Change disk compatibility level
 *   return: none
 *   level(in):
 */
void
rel_set_disk_compatible (float level)
{
  disk_compatibility_level = level;
}

/*
 * rel_platform - built platform
 *   return: none
 *   level(in):
 */
int
rel_bit_platform (void)
{
  return bit_platform;
}

/*
 * compatibility_rules - Static table of compatibility rules.
 *         Each time a change is made to the disk_compatibility_level
 *         a rule needs to be added to this table.
 *         If pair of numbers is absent from this table, the two are considered
 *         to be incompatible.
 * {base_level (of database), apply_level (of system), compatibility, fix_func}
 */
static COMPATIBILITY_RULE disk_compatibility_rules[] = {
  /*{1.1f, 1.0f, REL_FORWARD_COMPATIBLE, NULL},*/
  /*{1.0f, 1.1f, REL_BACKWARD_COMPATIBLE, NULL},*/

  /* a zero indicates the end of the table */
  {0.0f, 0.0f, REL_NOT_COMPATIBLE, NULL}
};

/*
 * rel_is_disk_compatible - Test compatibility of disk (database image)
 *                          Check a disk compatibility number stored in
 *                          a database with the disk compatibility number
 *                          for the system being run
 *   return: One of the three compatibility constants (REL_FULLY_COMPATIBLE,
 *           REL_FORWARD_COMPATIBLE, and REL_BACKWARD_COMPATIBLE) or
 *           REL_NOT_COMPATIBLE if they are not compatible
 *   db_level(in):
 *   REL_FIXUP_FUNCTION(in): function pointer table
 *
 * Note: The rules for compatibility are stored in the compatibility_rules
 *       table.  Whenever the disk_compatibility_level variable is changed
 *       an entry had better be made in the table.
 */
REL_COMPATIBILITY
rel_is_disk_compatible (float db_level, REL_FIXUP_FUNCTION ** fixups)
{
  COMPATIBILITY_RULE *rule;
  REL_COMPATIBILITY compat;
  REL_FIXUP_FUNCTION *func;

  func = NULL;

  if (disk_compatibility_level == db_level)
    {
      compat = REL_FULLY_COMPATIBLE;
    }
  else
    {
      compat = REL_NOT_COMPATIBLE;
      for (rule = &disk_compatibility_rules[0];
	   rule->base_level != 0 && compat == REL_NOT_COMPATIBLE; rule++)
	{

	  if (rule->base_level == db_level
	      && rule->apply_level == disk_compatibility_level)
	    {
	      compat = rule->compatibility;
	      func = rule->fix_function;
	    }
	}
    }

  if (fixups != NULL)
    *fixups = func;

  return compat;
}


/* Compare release strings.
 *
 * Returns:  < 0, if rel_a is earlier than rel_b
 *          == 0, if rel_a is the same as rel_b
 *           > 0, if rel_a is later than rel_b
 */
/*
 * rel_compare - Compare release strings, A and B
 *   return:  < 0, if rel_a is earlier than rel_b
 *           == 0, if rel_a is the same as rel_b
 *            > 0, if rel_a is later than rel_b
 *   rel_a(in): release string A
 *   rel_b(in): release string B
 *
 * Note:
 */
int
rel_compare (const char *rel_a, const char *rel_b)
{
  int a, b, retval = 0;
  char *a_temp, *b_temp;

  /*
   * If we get a NULL for one of the values (and we shouldn't), guess that
   * the versions are the same.
   */
  if (!rel_a || !rel_b)
    {
      retval = 0;
    }
  else
    {
      /*
       * Compare strings
       */
      a_temp = (char *) rel_a;
      b_temp = (char *) rel_b;
      /*
       * The following loop terminates if we determine that one string
       * is greater than the other, or we reach the end of one of the
       * strings.
       */
      while (!retval && *a_temp && *b_temp)
	{
	  a = strtol (a_temp, &a_temp, 10);
	  b = strtol (b_temp, &b_temp, 10);
	  if (a < b)
	    retval = -1;
	  else if (a > b)
	    retval = 1;
	  /*
	   * This skips over the '.'.
	   * This means that "?..?" will parse out to "?.?".
	   */
	  while (*a_temp && *a_temp == '.')
	    a_temp++;
	  while (*b_temp && *b_temp == '.')
	    b_temp++;
	  if (*a_temp && *b_temp
	      && char_isalpha (*a_temp) && char_isalpha (*b_temp))
	    {
	      if (*a_temp != *b_temp)
		retval = -1;
	      a_temp++;
	      b_temp++;
	    }
	}

      if (!retval)
	{
	  /*
	   * Both strings are the same up to this point.  If the rest is zeros,
	   * they're still equal.
	   */
	  while (*a_temp)
	    {
	      if (*a_temp != '.' && *a_temp != '0')
		{
		  retval = 1;
		  break;
		}
	      a_temp++;
	    }
	  while (*b_temp)
	    {
	      if (*b_temp != '.' && *b_temp != '0')
		{
		  retval = -1;
		  break;
		}
	      b_temp++;
	    }
	}
    }
  return retval;
}

/*
 * log compatibility matrix
 * {base_level (of server), apply_level (of client), compatibility, fix_func}
 * minor and patch number of the release string is to be the value of level
 * e.g. release 8.2.0 -> level 2.0
 * if the major numbers are different, no network compatibility!
 */
static COMPATIBILITY_RULE log_compatibility_rules[] = {
  /*{1.1f, 1.0f, REL_FORWARD_COMPATIBLE, NULL},*/
  /*{1.0f, 1.1f, REL_BACKWARD_COMPATIBLE, NULL},*/

  /* zero indicates the end of the table */
  {0.0f, 0.0f, REL_NOT_COMPATIBLE, NULL}
};

/*
 * rel_is_log_compatible - Test compatiblility of log file format
 *   return: true if compatible
 *   writer_rel_str(in): release string of the log writer (log file)
 *   reader_rel_str(in): release string of the log reader (system being run)
 */
bool
rel_is_log_compatible (const char *writer_rel_str, const char *reader_rel_str)
{
  return rel_is_compatible_internal (writer_rel_str, reader_rel_str,
				     log_compatibility_rules);
}

/*
 * network compatibility matrix
 * {base_level (of server), apply_level (of client), compatibility, fix_func}
 * minor and patch number of the release string is to be the value of level
 * e.g. release 8.2.0 -> level 2.0
 * if the major numbers are different, no network compatibility!
 */
static COMPATIBILITY_RULE net_compatibility_rules[] = {
  /*{1.1f, 1.0f, REL_FORWARD_COMPATIBLE, NULL},*/
  /*{1.0f, 1.1f, REL_BACKWARD_COMPATIBLE, NULL},*/

  /* zero indicates the end of the table */
  {0.0f, 0.0f, REL_NOT_COMPATIBLE, NULL}
};

/*
 * rel_is_net_compatible - Compare the release strings from
 *                          the server and client to determine compatibility.
 * return: REL_COMPATIBILITY
 *  REL_NOT_COMPATIBLE if the client and the server are not compatible
 *  REL_FULLY_COMPATIBLE if the client and the server are compatible
 *  REL_FORWARD_COMPATIBLE if the client is forward compatible with the server
 *                         if the server is backward compatible with the client
 *                         the client is older than the server
 *  REL_BACKWARD_COMPATIBLE if the client is backward compatible with the server
 *                          if the server is forward compatible with the client
 *                          the client is newer than the server
 *
 *   client_rel_str(in): client's release string
 *   server_rel_str(in): server's release string
 */
REL_COMPATIBILITY
rel_is_net_compatible (const char *client_rel_str, const char *server_rel_str)
{
  return rel_is_compatible_internal (server_rel_str, client_rel_str,
				     net_compatibility_rules);
}

/*
 * rel_is_compatible_internal - Compare the release to determine compatibility.
 *   return: REL_COMPATIBILITY
 *
 *   base_rel_str(in): base release string (of database)
 *   apply_rel_str(in): applier's release string (of system)
 *   rules(in): rules to determine forward/backward compatibility
 */
static REL_COMPATIBILITY
rel_is_compatible_internal (const char *base_rel_str,
			    const char *apply_rel_str,
			    COMPATIBILITY_RULE rules[])
{
  COMPATIBILITY_RULE *rule;
  REL_COMPATIBILITY compat;
  float apply_level, base_level;
  char *str_a, *str_b;

  if (apply_rel_str == NULL || base_rel_str == NULL)
    {
      return REL_NOT_COMPATIBLE;
    }

  /* release string should be in the form of <major>.<minor>[.<patch>] */

  /* check major number */
  apply_level = (float) strtol (apply_rel_str, &str_a, 10);
  base_level = (float) strtol (base_rel_str, &str_b, 10);
  if (apply_level == 0.0 || base_level == 0.0 || apply_level != base_level)
    {
      return REL_NOT_COMPATIBLE;
    }
/* skip '.' */
  while (*str_a && *str_a == '.')
    {
      str_a++;
    }
  while (*str_b && *str_b == '.')
    {
      str_b++;
    }
  /* check minor.patch number */
  apply_level = (float) strtod (str_a, NULL);
  base_level = (float) strtod (str_b, NULL);
  if (apply_level == base_level)
    {
      return REL_FULLY_COMPATIBLE;
    }

  compat = REL_NOT_COMPATIBLE;
  for (rule = &rules[0];
       rule->base_level != 0 && compat == REL_NOT_COMPATIBLE; rule++)
    {
      if (rule->base_level == base_level && rule->apply_level == apply_level)
	{
	  compat = rule->compatibility;
	}
    }

  return compat;
}
