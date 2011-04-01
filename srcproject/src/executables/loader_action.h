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
 *      loader_action.h: Definitions for loader action routines
 */

#ifndef _LOADER_ACTION_H_
#define _LOADER_ACTION_H_

#ident "$Id$"

typedef enum
{
  SYS_ELO_INTERNAL,
  SYS_ELO_EXTERNAL,
  SYS_USER,
  SYS_CLASS
} ACT_SYSOBJ_TYPE;

enum
{
  LDR_OLD_SYS_ELO_INTERNAL,
  LDR_OLD_SYS_ELO_EXTERNAL,
  LDR_OLD_SYS_USER,
  LDR_OLD_SYS_CLASS,
  LDR_OLD_NULL,
  LDR_OLD_INT,
  LDR_OLD_STR,
  LDR_OLD_NSTR,
  LDR_OLD_REAL,
  LDR_OLD_OID,
  LDR_OLD_CLASS_OID,
  LDR_OLD_DATE,
  LDR_OLD_DATE2,
  LDR_OLD_TIME,
  LDR_OLD_TIME4,
  LDR_OLD_TIME42,
  LDR_OLD_TIME3,
  LDR_OLD_TIME31,
  LDR_OLD_TIME2,
  LDR_OLD_TIME1,
  LDR_OLD_TIMESTAMP,
  LDR_OLD_DATETIME,
  LDR_OLD_COLLECTION,
  LDR_OLD_MONETARY,
  LDR_OLD_BSTR,
  LDR_OLD_XSTR
};

extern void act_init (void);
extern void act_finish (int error);
extern void act_newline (void);

extern void display_error_line (int adjust);

extern void act_start_id (char *classname);
extern void act_set_id (int id);

extern void act_set_class (char *classname);
extern void act_class_attributes (void);
extern void act_shared_attributes (void);
extern void act_default_attributes (void);
extern void act_add_attribute (char *attname);
extern void act_add_instance (int id);
extern void act_set_ref_class (char *name);
extern void act_set_ref_class_id (int id);

extern void act_start_set (void);
extern void act_end_set (void);

extern void act_reference (int id);
extern void act_reference_class (void);
extern void act_int (char *token);
extern void act_real (char *token);
extern void act_monetary (char *token);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void act_datetime (char *token);
#endif
extern void act_date (char *token);
extern void act_time (char *token, int type);
extern void act_string (char *token, int size, DB_TYPE dtype);
extern void act_null (void);

extern void act_set_constructor (char *name);
extern void act_add_argument (char *name);

extern void act_nstring (char *);
extern void act_bstring (char *, int);


extern void act_system (const char *text, ACT_SYSOBJ_TYPE type);

#endif /* _LOADER_ACTION_H_ */
