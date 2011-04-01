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
 * semantic_check.h - contains semantic check prototypes
 */

#ifndef _SEMANTIC_CHECK_H_
#define _SEMANTIC_CHECK_H_

#ident "$Id$"


extern int pt_class_assignable (PARSER_CONTEXT * parser,
				const PT_NODE * d_class,
				const PT_NODE * s_class);

extern PT_NODE *pt_type_cast_vclass_query_spec_column (PARSER_CONTEXT *
						       parser, PT_NODE * attr,
						       PT_NODE * col);

extern PT_NODE *pt_check_union_compatibility (PARSER_CONTEXT * parser,
					      PT_NODE * node);

extern PT_NODE *pt_semantic_check (PARSER_CONTEXT * parser,
				   PT_NODE * statement);

extern PT_NODE *pt_invert (PARSER_CONTEXT * parser,
			   PT_NODE * name_expr, PT_NODE * result);

extern PT_NODE *pt_find_attr_def (const PT_NODE * attr_def_list,
				  const PT_NODE * name);
extern PT_NODE *pt_find_cnstr_def (const PT_NODE * cnstr_def_list,
				   const PT_NODE * name);

extern PT_NODE *pt_insert_entity (PARSER_CONTEXT * parser, PT_NODE * node,
				  PT_NODE * prev_entity,
				  PT_NODE * correlation_spec);
extern PT_NODE *pt_find_class_of_index (PARSER_CONTEXT * parser,
					const char *const index_name,
					const DB_CONSTRAINT_TYPE index_type);
extern int pt_has_text_domain (PARSER_CONTEXT * parser,
			       DB_ATTRIBUTE * attribute);
extern PT_NODE *pt_find_order_value_in_list (PARSER_CONTEXT * parser,
					     const PT_NODE * sort_value,
					     const PT_NODE * order_list);

#endif /* _SEMANTIC_CHECK_H_ */
