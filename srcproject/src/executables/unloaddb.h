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
 *      unloaddb.h: simplified object descriptions
 */

#ifndef _UNLOADDB_H_
#define _UNLOADDB_H_

#ident "$Id$"

#include "locator_cl.h"

extern char *database_name;
extern const char *output_dirname;
extern char *input_filename;
extern FILE *output_file;
extern struct text_output *obj_out;
extern int page_size;
extern int cached_pages;
extern int est_size;
extern char *hash_filename;
extern int debug_flag;
extern bool verbose_flag;
extern bool include_references;
extern char *output_prefix;
extern bool do_schema;
extern bool do_objects;
extern bool delimited_id_flag;
extern bool ignore_err_flag;
extern bool required_class_only;
extern LIST_MOPS *class_table;
extern DB_OBJECT **req_class_table;
extern int is_req_class (DB_OBJECT * class_);
extern int get_requested_classes (const char *input_filename,
				  DB_OBJECT * class_list[]);

extern int lo_count;

#define LEFT_DEL 	"["
#define RIGHT_DEL	"]"
#define NO_DEL		""

#define PRINT_IDENTIFIER(s) need_quotes((s)) ? LEFT_DEL : NO_DEL, \
                            (s),                                  \
			    need_quotes((s)) ? RIGHT_DEL : NO_DEL

extern int extractschema (const char *exec_name, int do_auth);
extern int extractobjects (const char *exec_name);
extern bool need_quotes (const char *identifier);

#endif /* _UNLOADDB_H_ */
