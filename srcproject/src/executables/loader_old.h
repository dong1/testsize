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
 *      loader_old.h: loader definitions
 */

#ifndef _LOADER_OLD_H_
#define _LOADER_OLD_H_

#ident "$Id$"

#include "object_representation.h"
#include "work_space.h"
#include "class_object.h"

/* attribute type identifiers for ldr_restrict_attribute() */
#define LDR_ATTRIBUTE_ANY      0
#define LDR_ATTRIBUTE_INSTANCE 1
#define LDR_ATTRIBUTE_SHARED   2
#define LDR_ATTRIBUTE_CLASS    3
#define LDR_ATTRIBUTE_DEFAULT  4

/* module control */

extern int ldr_init (bool verbose);
extern int ldr_start (int periodic_commit);
extern int ldr_finish (int error);
extern int ldr_final (void);
extern int ldr_update_statistics (void);

extern void ldr_invalid_class (void);
extern void ldr_invalid_object (void);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void ldr_invalid_file (void);
#endif
extern void ldr_internal_error (void);

/* class definition */

extern int ldr_start_class (MOP class, const char *classname);
extern int ldr_restrict_attributes (int type);
extern int ldr_add_attribute (const char *attname);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int ldr_add_class_attribute (const char *attname);
#endif
extern int ldr_set_constructor (const char *name);
extern int ldr_add_argument (const char *name);
extern int ldr_add_instance (int id);
extern int ldr_assign_class_id (MOP class, int id);
extern MOP ldr_get_class_from_id (int id);

/* value addition */

extern int ldr_start_set (void);
extern int ldr_end_set (void);
extern int ldr_add_value (DB_TYPE token_type, DB_VALUE ** retval);
extern int ldr_add_reference (MOP class, int id);
extern int ldr_add_reference_to_class (MOP class);
extern int ldr_add_elo (const char *filename, int external);

/* state information */

extern void ldr_stats (int *errors, int *objects, int *defaults);
extern const char *ldr_att_name (void);
extern const char *ldr_prev_att_name (void);
extern SM_DOMAIN *ldr_att_domain (void);
extern const char *ldr_class_name (void);
extern const char *ldr_constructor_name (void);
extern const char *ldr_att_domain_name (void);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int ldr_expected_values (void);
#endif

/* Callback functions  */

typedef void (*LDR_POST_COMMIT_HANDLER) (int);
typedef void (*LDR_POST_INTERRUPT_HANDLER) (int);

extern void ldr_register_post_commit_handler (LDR_POST_COMMIT_HANDLER
					      handler);
extern void ldr_register_post_interrupt_handler (LDR_POST_INTERRUPT_HANDLER
						 handler,
						 jmp_buf * ldr_jmp_buf);
extern void ldr_interrupt_has_occurred (int type);

#define LDR_NO_INTERRUPT 0
#define LDR_STOP_AND_ABORT_INTERRUPT 1
#define LDR_STOP_AND_COMMIT_INTERRUPT 2

extern void print_log_msg (int verbose, const char *fmt, ...);

#endif /* _LOADER_OLD_H_ */
