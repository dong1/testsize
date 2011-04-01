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
 * query_rewrite.c - Query rewrite optimization
 */

#ident "$Id$"

#include "parser.h"
#include "parser_message.h"
#include "parse_tree.h"
#include "optimizer.h"
#include "xasl_generation.h"
#include "virtual_object.h"
#include "system_parameter.h"
#include "semantic_check.h"
#include "execute_schema.h"
#include "view_transform.h"
#include "parser.h"

#define DB_MAX_LITERAL_PRECISION 255

typedef struct spec_id_info SPEC_ID_INFO;
struct spec_id_info
{
  UINTPTR id;
  bool appears;
};

typedef struct to_dot_info TO_DOT_INFO;
struct to_dot_info
{
  PT_NODE *old_spec;
  PT_NODE *new_spec;
};

typedef struct pt_name_spec_info PT_NAME_SPEC_INFO;
struct pt_name_spec_info
{
  PT_NODE *c_name;		/* attr name which will be reduced to constant */
  int c_name_num;
  int query_serial_num;		/* query, serial number */
  PT_NODE *s_point_list;	/* list of other specs name.
				 * these are joined with spec of c_name */
};

/* result of CompDBValueWithOpType() function */
typedef enum COMP_DBVALUE_WITH_OPTYPE_RESULT
{
  CompResultLess = -2,		/* less than */
  CompResultLessAdj = -1,	/* less than and adjacent to */
  CompResultEqual = 0,		/* equal */
  CompResultGreaterAdj = 1,	/* greater than and adjacent to */
  CompResultGreater = 2,	/* greater than */
  CompResultError = 3		/* error */
} COMP_DBVALUE_WITH_OPTYPE_RESULT;

typedef struct qo_reset_location_info RESET_LOCATION_INFO;
struct qo_reset_location_info
{
  PT_NODE *start_spec;
  short start;
  short end;
  bool found_outerjoin;
};

/*
 * qo_find_best_path_type () -
 *   return: PT_NODE *
 *   spec(in): path entity to test
 *
 * Note: prunes non spec's
 */
static PT_MISC_TYPE
qo_find_best_path_type (PT_NODE * spec)
{
  PT_MISC_TYPE best_path_type = PT_PATH_OUTER;
  PT_MISC_TYPE path_type;

  /* if any is an inner, the result is inner.
   * if all are outer, the result is outer
   */

  while (spec)
    {
      path_type = spec->info.spec.meta_class;
      if (path_type == PT_PATH_INNER)
	return PT_PATH_INNER;
      if (path_type != PT_PATH_OUTER)
	best_path_type = PT_PATH_OUTER_WEASEL;

      path_type = qo_find_best_path_type (spec->info.spec.path_entities);
      if (path_type == PT_PATH_INNER)
	return PT_PATH_INNER;
      if (path_type != PT_PATH_OUTER)
	best_path_type = PT_PATH_OUTER_WEASEL;

      spec = spec->next;
    }

  return best_path_type;
}

/*
 * qo_get_name_by_spec_id () - looks for a name with a matching id
 *   return: PT_NODE *
 *   parser(in): parser environment
 *   node(in): (name) node to compare id's with
 *   arg(in): info of spec and result
 *   continue_walk(in):
 */
static PT_NODE *
qo_get_name_by_spec_id (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
			int *continue_walk)
{
  SPEC_ID_INFO *info = (SPEC_ID_INFO *) arg;

  if (node->node_type == PT_NAME && node->info.name.spec_id == info->id)
    {
      *continue_walk = PT_STOP_WALK;
      info->appears = true;
    }

  return node;
}

/*
 * qo_check_nullable_expr () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
PT_NODE *
qo_check_nullable_expr (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
			int *continue_walk)
{
  int *nullable_cntp = (int *) arg;

  if (node->node_type == PT_EXPR)
    {
      /* check for nullable term: expr(..., NULL, ...) can be non-NULL
       */
      switch (node->info.expr.op)
	{
	case PT_IS_NULL:
	case PT_CASE:
	case PT_COALESCE:
	case PT_NVL:
	case PT_NVL2:
	case PT_DECODE:
	case PT_IF:
	case PT_IFNULL:
	case PT_ISNULL:
	  /* NEED FUTURE OPTIMIZATION */
	  (*nullable_cntp)++;
	  break;
	default:
	  break;
	}
    }

  return node;
}

/*
 * qo_replace_spec_name_with_null () - replace spec names with PT_TYPE_NULL pt_values
 *   return: PT_NODE *
 *   parser(in): parser environment
 *   node(in): (name) node to compare id's with
 *   arg(in): spec
 *   continue_walk(in):
 */
static PT_NODE *
qo_replace_spec_name_with_null (PARSER_CONTEXT * parser, PT_NODE * node,
				void *arg, int *continue_walk)
{
  PT_NODE *spec = (PT_NODE *) arg;
  PT_NODE *name;

  if (node->node_type == PT_NAME
      && node->info.name.spec_id == spec->info.spec.id)
    {
      node->node_type = PT_VALUE;
      node->type_enum = PT_TYPE_NULL;
    }

  if (node->node_type == PT_DOT_
      && (name = node->info.dot.arg2)
      && name->info.name.spec_id == spec->info.spec.id)
    {
      parser_free_tree (parser, name);
      parser_free_tree (parser, node->info.expr.arg1);
      node->node_type = PT_VALUE;
      node->type_enum = PT_TYPE_NULL;
      /* By changing this node, we need to null the value container
       * so that we protect parts of the code that ignore type_enum
       * set to PT_TYPE_NULL.  This is particularly problematic on
       * PCs since they have different alignment requirements.
       */
      node->info.value.data_value.set = NULL;
    }

  return node;
}

/*
 * qo_check_condition_yields_null () -
 *   return:
 *   parser(in): parser environment
 *   path_spec(in): to test attributes as NULL
 *   query_where(in): clause to evaluate
 */
static bool
qo_check_condition_yields_null (PARSER_CONTEXT * parser, PT_NODE * path_spec,
				PT_NODE * query_where)
{
  PT_NODE *where;
  bool result = false;
  SEMANTIC_CHK_INFO sc_info = { NULL, NULL, 0, 0, 0, false, false, false };

  if (query_where == NULL)
    {
      return result;
    }

  where = parser_copy_tree_list (parser, query_where);
  where =
    parser_walk_tree (parser, where, qo_replace_spec_name_with_null,
		      path_spec, NULL, NULL);

  sc_info.top_node = where;
  sc_info.donot_fold = false;
  where = pt_semantic_type (parser, where, &sc_info);
  result = pt_false_search_condition (parser, where);
  parser_free_tree (parser, where);

  /*
   * Ignore any error returned from semantic type check.
   * Just wanted to evaluate where clause with nulled spec names.
   */
  if (parser->error_msgs)
    {
      parser_free_tree (parser, parser->error_msgs);
      parser->error_msgs = NULL;
    }

  return result;
}

/*
 * qo_analyze_path_join_pre () -
 *   return: PT_NODE *
 *   parser(in): parser environment
 *   spec(in): path entity to test
 *   arg(in): where clause to test
 *   continue_walk(in):
 *
 * Note : prunes non spec's
 */
static PT_NODE *
qo_analyze_path_join_pre (PARSER_CONTEXT * parser, PT_NODE * spec, void *arg,
			  int *continue_walk)
{
  *continue_walk = PT_CONTINUE_WALK;

  if (spec->node_type != PT_SPEC)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return spec;
}

/*
 * qo_analyze_path_join () -
 *   return: PT_NODE *
 *   parser(in): parser environment
 *   path_spec(in): path entity to test
 *   arg(in): where clause to test
 *   continue_walk(in):
 *
 * Note: tests all non-selector path spec's for the type of join
 * 	that can be done.
 * 	if a null path can be guaranteed to produce no row
 *	tags spec as PT_INNER_PATH
 *
 *	if a null path can have no effect on
 *	(does not appear in) the where clause
 *	tags spec as PT_PATH_OUTER
 *
 *	if a null path COULD affect the where clause (appears),
 *	but cannot be guaranteed to have no effect,
 *	tags the spec as PT_PATH_OUTER_WEASEL. This means
 *	no merge, since I can't prove that this is equivalent
 *	to PT_PATH_INNER. This is treated the same as
 *	PT_PATH_OUTER, with apologies for the silly name.
 *
 */
static PT_NODE *
qo_analyze_path_join (PARSER_CONTEXT * parser, PT_NODE * path_spec, void *arg,
		      int *continue_walk)
{
  PT_NODE *where = (PT_NODE *) arg;
  PT_MISC_TYPE path_type;
  SPEC_ID_INFO info;

  *continue_walk = PT_CONTINUE_WALK;

  if (path_spec->node_type == PT_SPEC
      && path_spec->info.spec.path_conjuncts
      && path_spec->info.spec.meta_class != PT_PATH_INNER)
    {
      /* to get here, this must be a 'normal' outer path entity
       * We may be able to optimize this to an inner path
       *
       * if any sub path is an PT_PATH_INNER, so is this one.
       * otherwise, if any sub-path is NOT an PT_PATH_OUTER,
       * the best we can be is a WEASEL :).
       * Since we are a post function, sub-paths are already set.
       */
      path_type = qo_find_best_path_type (path_spec->info.spec.path_entities);

      path_spec->info.spec.meta_class = path_type;

      if (path_type != PT_PATH_INNER)
	{
	  info.id = path_spec->info.spec.id;
	  info.appears = false;
	  parser_walk_tree (parser, where, qo_get_name_by_spec_id, &info,
			    NULL, NULL);

	  if (info.appears)
	    {
	      if (qo_check_condition_yields_null (parser, path_spec, where))
		{
		  path_spec->info.spec.meta_class = PT_PATH_INNER;
		}
	      else
		{
		  path_spec->info.spec.meta_class = PT_PATH_OUTER_WEASEL;
		}
	    }
	  else
	    {
	      /* best path type already assigned above */
	    }
	}
    }

  return path_spec;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * qo_convert_path_to_name () -
 *   return: PT_NODE *
 *   parser(in): parser environment
 *   node(in): node to test for path conversion
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_convert_path_to_name (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
			 int *continue_walk)
{
  PT_NODE *spec = (PT_NODE *) arg;
  PT_NODE *name;

  if (node->node_type == PT_DOT_
      && (name = node->info.dot.arg2)
      && name->node_type == PT_NAME
      && name->info.name.spec_id == spec->info.spec.id)
    {
      node->info.dot.arg2 = NULL;
      name->next = node->next;
      node->next = NULL;
      parser_free_tree (parser, node);
      node = name;
      if (spec->info.spec.range_var)
	{
	  name->info.name.resolved =
	    spec->info.spec.range_var->info.name.original;
	}
    }
  return node;
}

/*
 * qo_rewrite_as_join () - Given a statement, a path root, a path spec ptr,
 *			rewrite the statement into a join with the path spec
 *   return:
 *   parser(in):
 *   root(in):
 *   statement(in):
 *   path_spec_ptr(in):
 */
static void
qo_rewrite_as_join (PARSER_CONTEXT * parser, PT_NODE * root,
		    PT_NODE * statement, PT_NODE ** path_spec_ptr)
{
  PT_NODE *path_spec;
  PT_NODE *conjunct;

  path_spec = *path_spec_ptr;

  conjunct = path_spec->info.spec.path_conjuncts;
  path_spec->info.spec.path_conjuncts = NULL;
  *path_spec_ptr = path_spec->next;
  path_spec->next = root->next;
  root->next = path_spec;
  statement->info.query.q.select.where = parser_append_node
    (conjunct, statement->info.query.q.select.where);
  statement = parser_walk_tree (parser, statement,
				qo_convert_path_to_name, path_spec, NULL,
				NULL);
}

/*
 * qo_rewrite_as_derived () - Given a statement, a path root, a path spec ptr,
 *			   rewrite the spec to be a table derived from a join
 *			   of the path_spec table and the root table
 *   return:
 *   parser(in):
 *   root(in):
 *   root_where(in):
 *   statement(in):
 *   path_spec_ptr(in):
 */
static void
qo_rewrite_as_derived (PARSER_CONTEXT * parser, PT_NODE * root,
		       PT_NODE * root_where,
		       PT_NODE * statement, PT_NODE ** path_spec_ptr)
{
  PT_NODE *path_spec;
  PT_NODE *conjunct;
  PT_NODE *new_spec;
  PT_NODE *new_root;
  PT_NODE *query;
  PT_NODE *temp;

  path_spec = *path_spec_ptr;
  new_spec = parser_copy_tree (parser, path_spec);
  if (new_spec == NULL)
    {
      PT_INTERNAL_ERROR (parser, "copy tree");
      return;
    }

  conjunct = new_spec->info.spec.path_conjuncts;
  new_spec->info.spec.path_conjuncts = NULL;

  if (root->info.spec.derived_table)
    {
      /* if the root spec is a derived table query, construct a derived
       * table query for this path spec by building on top of that.
       * This will be the case for outer path expressions 2 or more deep.
       */
      query = parser_copy_tree (parser, root->info.spec.derived_table);
      if (query == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "copy tree");
	  return;
	}

      new_root = query->info.query.q.select.from;
      parser_free_tree (parser, query->info.query.q.select.list);
    }
  else
    {
      /* if the root spec is a class spec, construct a
       * derived table query for this path spec from scratch.
       */
      new_root = parser_copy_tree (parser, root);
      query = parser_new_node (parser, PT_SELECT);

      if (query == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  return;
	}

      query->info.query.q.select.from = new_root;
      query->info.query.correlation_level = 0;
    }
  new_root = parser_append_node (new_spec, new_root);
  query->info.query.all_distinct = PT_DISTINCT;
  query->info.query.q.select.where =
    parser_append_node (root_where, query->info.query.q.select.where);
  query->info.query.q.select.where =
    parser_append_node (conjunct, query->info.query.q.select.where);
  temp = query->info.query.q.select.list = parser_copy_tree_list
    (parser, path_spec->info.spec.referenced_attrs);
  while (temp)
    {
      /* force all the names to be fully qualified */
      temp->info.name.resolved =
	new_spec->info.spec.range_var->info.name.original;
      temp = temp->next;
    }
  query->info.query.is_subquery = PT_IS_SUBQUERY;
  mq_regenerate_if_ambiguous (parser, new_spec, query, new_root);
  mq_set_references (parser, query, new_spec);
  mq_set_references (parser, query, new_root);

  /* Here we set up positional correspondance to the derived
   * queries select list, but we must preserve the spec identity
   * of the path_spec, so we copy the original referenced attrs,
   * not the copied/reset list.
   */
  temp = path_spec->info.spec.as_attr_list = parser_copy_tree_list
    (parser, path_spec->info.spec.referenced_attrs);
  while (temp)
    {
      temp->info.name.resolved = NULL;
      temp = temp->next;
    }

  parser_free_tree (parser, path_spec->info.spec.entity_name);
  path_spec->info.spec.entity_name = NULL;
  parser_free_tree (parser, path_spec->info.spec.flat_entity_list);
  path_spec->info.spec.flat_entity_list = NULL;

  path_spec->info.spec.derived_table = query;
  path_spec->info.spec.derived_table_type = PT_IS_SUBQUERY;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * qo_convert_attref_to_dotexpr_pre () -
 *   return:
 *   parser(in):
 *   spec(in):
 *   arg(in):
 *   continue_walk(in):
 *
 * Note: prunes PT_SPEC
 */
static PT_NODE *
qo_convert_attref_to_dotexpr_pre (PARSER_CONTEXT * parser, PT_NODE * spec,
				  void *arg, int *continue_walk)
{
  TO_DOT_INFO *info = (TO_DOT_INFO *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (spec->node_type == PT_SPEC
      && spec->info.spec.id == info->old_spec->info.spec.id)
    {
      *continue_walk = PT_LIST_WALK;
    }
  return spec;
}

/*
 * qo_convert_attref_to_dotexpr () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 *
 * Note: looks for any attribute reference x.i in
 *     	select x.i, ... from c x, ... where x.i ... and x {=|IN} expr
 *   	and rewrites those into path expressions t.x.i in
 *     	select t.x.i, ... from table({expr}) as t(x), ... where t.x.i ...
 */
static PT_NODE *
qo_convert_attref_to_dotexpr (PARSER_CONTEXT * parser, PT_NODE * node,
			      void *arg, int *continue_walk)
{
  TO_DOT_INFO *info = (TO_DOT_INFO *) arg;
  PT_NODE *arg1, *arg2, *attr, *rvar;
  PT_NODE *new_spec = info->new_spec;

  if (node->node_type == PT_NAME
      && node->info.name.spec_id == info->old_spec->info.spec.id)
    {
      attr = new_spec->info.spec.as_attr_list;
      rvar = new_spec->info.spec.range_var;

      switch (node->info.name.meta_class)
	{
	case PT_CLASS:
	  /* must be a data_type entity, so don't change its
	   * original name because later xasl domain handling code
	   * may use that name to look up the class.
	   */
	  break;
	case PT_OID_ATTR:
	  /* resolve the name to the new_spec */
	  node->info.name.spec_id = new_spec->info.spec.id;
	  node->info.name.original = attr->info.name.original;
	  node->info.name.resolved = rvar->info.name.original;
	  /* an OID_ATTR becomes a NORMAL attribute reference */
	  if (node->info.name.meta_class == PT_OID_ATTR)
	    node->info.name.meta_class = PT_NORMAL;
	  break;
	case PT_NORMAL:
	  /* we must transform this NAME node into a DOT node in place to
	   * preserve its address. (Otherwise, we have to find all the
	   * places that point to it and change them all.)
	   */
	  arg2 = parser_copy_tree (parser, node);
	  if (arg2)
	    arg2->next = NULL;
	  arg1 = pt_name (parser, attr->info.name.original);
	  if (arg1)
	    {
	      arg1->info.name.resolved = rvar->info.name.original;
	      arg1->info.name.spec_id = new_spec->info.spec.id;
	      arg1->info.name.meta_class = PT_NORMAL;
	      arg1->type_enum = attr->type_enum;
	      arg1->data_type = parser_copy_tree (parser, attr->data_type);
	    }
	  node->node_type = PT_DOT_;
	  node->info.dot.arg1 = arg1;
	  node->info.dot.arg2 = arg2;
	  node->info.dot.selector = NULL;
	  break;
	default:
	  break;
	}
    }
  else if (node->node_type == PT_SPEC
	   && node->info.spec.id == info->old_spec->info.spec.id)
    {
      *continue_walk = PT_LIST_WALK;
    }
  return node;
}

/*
 * qo_get_next_oid_pred () -
 *   return:
 *   pred(in): cursor into a subquery's where clause
 *
 * Note:
 *   It requires pred is a cursor into a subquery's where clause that has been
 *   transformed into conjunctive normal form and
 *   effects that returns a pointer to subquery's next CNF-term that can be
 *   rewritten into an oid attribute equality test, if one exists.
 *   returns a NULL pointer otherwise.
 */
static PT_NODE *
qo_get_next_oid_pred (PT_NODE * pred)
{
  while (pred && pred->node_type == PT_EXPR && pred->or_next == NULL)
    {
      if (pred->info.expr.op == PT_EQ || pred->info.expr.op == PT_IS_IN)
	{
	  if (pred->info.expr.arg1
	      && pred->info.expr.arg1->node_type == PT_NAME
	      && pred->info.expr.arg1->info.name.meta_class == PT_OID_ATTR)
	    {
	      return pred;
	    }
	  if (pred->info.expr.arg2
	      && pred->info.expr.arg2->node_type == PT_NAME
	      && pred->info.expr.arg2->info.name.meta_class == PT_OID_ATTR)
	    {
	      return pred;
	    }
	}
      pred = pred->next;
    }

  return pred;
}

/*
 * qo_is_oid_const () -
 *   return: Returns true iff the argument looks like a constant for
 *	     the purposes f the oid equality rewrite optimization
 *   node(in):
 */
static int
qo_is_oid_const (PT_NODE * node)
{
  if (!node)
    return 0;

  switch (node->node_type)
    {
    case PT_VALUE:
    case PT_HOST_VAR:
      return 1;

    case PT_NAME:
      /*
       * This *could* look to see if the name is correlated to the same
       * level as the caller, but that's going to require more context
       * to come in...
       */
      return node->info.name.meta_class == PT_PARAMETER;

    case PT_FUNCTION:
      if (node->info.function.function_type != F_SET
	  && node->info.function.function_type != F_MULTISET
	  && node->info.function.function_type != F_SEQUENCE)
	{
	  return 0;
	}
      else
	{
	  /*
	   * The is the case for an expression like
	   *
	   *  {:a, :b, :c}
	   *
	   * Here the the expression '{:a, :b, :c}' comes in as a
	   * sequence function call, with PT_NAMEs 'a', 'b', and 'c' as
	   * its arglist.
	   */
	  PT_NODE *p;

	  for (p = node->info.function.arg_list; p; p = p->next)
	    {
	      if (!qo_is_oid_const (p))
		return 0;
	    }
	  return 1;
	}

    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      return node->info.query.correlation_level != 1;

    default:
      return 0;
    }
}

/*
 * qo_construct_new_set () -
 *   return:
 *   parser(in): parser context
 *   node(in): an OID_ATTR equality/IN predicate
 *
 * Note:
 *   It requires that node is an OID_ATTR predicate (x {=|IN} expr) from
 *        select ... from c x, ... where ... and x {=|IN} expr
 *   and modifies parser heap
 *   and effects that creates, initializes, returns a new set constructor
 *   subtree that can be used for the derived table field of a new PT_SPEC
 *    node representing 'table({expr}) as t(x)' in the rewritten
 *        select ... from table({expr}) as t(x), ... where ...
 */
static PT_NODE *
qo_construct_new_set (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *arg = NULL, *set = NULL;
  /*jabaek: modify SQLM */
  PT_NODE *targ = NULL;

  /* make sure we have reasonable arguments */
  if (!node || node->node_type != PT_EXPR)
    return set;

  /* if control reaches here, then qo_get_next_oid_pred must have succeeded
   * in finding a CNF-term: 'x {=|IN} expr' from a query
   *   select ... from c x, ... where ... and x {=|IN} expr
   * Now, copy 'expr' into a derived table: 'table({expr})' which the
   * caller will put into the transformed query
   *   select ... from table({expr}) as t(x), ... where ...
   */
  switch (node->info.expr.op)
    {
    case PT_EQ:
      if (node->info.expr.arg1
	  && node->info.expr.arg1->node_type == PT_NAME
	  && node->info.expr.arg1->info.name.meta_class == PT_OID_ATTR
	  && qo_is_oid_const (node->info.expr.arg2))
	{
	  arg = parser_copy_tree (parser, node->info.expr.arg2);
	  targ = node->info.expr.arg1;
	}
      else if (node->info.expr.arg2
	       && node->info.expr.arg2->node_type == PT_NAME
	       && node->info.expr.arg2->info.name.meta_class == PT_OID_ATTR
	       && qo_is_oid_const (node->info.expr.arg1))
	{
	  arg = parser_copy_tree (parser, node->info.expr.arg1);
	  targ = node->info.expr.arg2;
	}
      break;
    case PT_IS_IN:
      if (PT_IS_OID_NAME (node->info.expr.arg1)
	  && PT_IS_FUNCTION (node->info.expr.arg2)
	  && PT_IS_CONST_INPUT_HOSTVAR (node->info.expr.arg2->info.function.
					arg_list))
	{
	  arg = parser_copy_tree (parser,
				  node->info.expr.arg2->info.function.
				  arg_list);
	  targ = node->info.expr.arg1;
	}
      else
	if (PT_IS_OID_NAME (node->info.expr.arg2)
	    && PT_IS_FUNCTION (node->info.expr.arg1)
	    && PT_IS_CONST_INPUT_HOSTVAR (node->info.expr.arg1->info.function.
					  arg_list))
	{
	  arg = parser_copy_tree (parser,
				  node->info.expr.arg1->info.function.
				  arg_list);
	  targ = node->info.expr.arg2;
	}
      break;
    default:
      break;
    }

  /* create mset constructor subtree */
  if (arg && (set = parser_new_node (parser, PT_FUNCTION)) != NULL)
    {
      parser_init_node (set);
      set->info.function.function_type = F_SEQUENCE;
      set->info.function.arg_list = arg;
      set->type_enum = PT_TYPE_SEQUENCE;

      set->data_type = parser_copy_tree_list (parser, arg->data_type);
    }

  return set;
}

/*
 * qo_make_new_derived_tblspec () -
 *   return:
 *   parser(in): parser context
 *   node(in): a PT_SPEC node
 *   pred(in): node's OID_ATTR predicate
 *   seqno(in/out): sequence number for generating unique derived table names
 *
 * Note:
 *   It requires that node is the PT_SPEC node (c x) and
 *   pred is the OID_ATTR predicate (x {=|IN} expr) from
 *        select ... from c x, ... where ... and x {=|IN} expr
 *   and modifies parser heap, node
 *   and effects that creates, initializes, returns a new derived table
 *   type PT_SPEC node representing 'table({expr}) as t(x)' in the rewritten
 *        select ... from table({expr}) as t(x), ... where ...
 */
static PT_NODE *
qo_make_new_derived_tblspec (PARSER_CONTEXT * parser, PT_NODE * node,
			     PT_NODE * pred, int *seqno)
{
  PT_NODE *spec = NULL, *dtbl, *eq, *rvar;
  UINTPTR spec_id;
  const char *dtblnam, *dattnam;

  dtbl = qo_construct_new_set (parser, pred);
  if (dtbl)
    spec = parser_new_node (parser, PT_SPEC);
  if (spec)
    {
      parser_init_node (spec);
      spec_id = (UINTPTR) spec;
      spec->info.spec.id = spec_id;
      spec->info.spec.only_all = PT_ONLY;
      spec->info.spec.derived_table_type = PT_IS_SET_EXPR;
      spec->info.spec.derived_table = dtbl;
      dtblnam = mq_generate_name (parser, "dt", seqno);
      dattnam = mq_generate_name (parser, "da", seqno);
      spec->info.spec.range_var = pt_name (parser, dtblnam);
      spec->info.spec.range_var->info.name.spec_id = spec_id;
      spec->info.spec.as_attr_list = pt_name (parser, dattnam);
      spec->info.spec.as_attr_list->info.name.spec_id = spec_id;
      spec->info.spec.as_attr_list->info.name.meta_class = PT_NORMAL;
      spec->info.spec.as_attr_list->type_enum = PT_TYPE_OBJECT;
      spec->info.spec.as_attr_list->data_type =
	parser_copy_tree (parser, dtbl->data_type);
      if (node && node->node_type == PT_SPEC
	  && (rvar = node->info.spec.range_var) != NULL)
	{
	  /* new derived table spec needs path entities */
	  spec->info.spec.path_entities = node;

	  /* we also need to graft a path conjunct to node */
	  node->info.spec.path_conjuncts = eq =
	    parser_new_node (parser, PT_EXPR);
	  if (eq)
	    {
	      parser_init_node (eq);
	      eq->type_enum = PT_TYPE_LOGICAL;
	      eq->info.expr.op = PT_EQ;
	      eq->info.expr.arg1 = pt_name (parser, dattnam);
	      eq->info.expr.arg1->info.name.spec_id = spec_id;
	      eq->info.expr.arg1->info.name.resolved = dtblnam;
	      eq->info.expr.arg1->info.name.meta_class = PT_NORMAL;
	      eq->info.expr.arg1->type_enum = PT_TYPE_OBJECT;
	      eq->info.expr.arg1->data_type =
		parser_copy_tree (parser, dtbl->data_type);
	      eq->info.expr.arg2 = pt_name (parser, "");
	      eq->info.expr.arg2->info.name.spec_id = node->info.spec.id;
	      eq->info.expr.arg2->info.name.resolved =
		rvar->info.name.original;
	      eq->info.expr.arg2->info.name.meta_class = PT_OID_ATTR;
	      eq->info.expr.arg2->type_enum = PT_TYPE_OBJECT;
	      eq->info.expr.arg2->data_type =
		parser_copy_tree (parser, dtbl->data_type);
	    }
	}
    }
  return spec;
}

/*
 * qo_rewrite_oid_equality () -
 *   return:
 *   parser(in): parser context
 *   node(in): a subquery
 *   pred(in): subquery's OID_ATTR equality/IN predicate
 *   seqno(in/out): seq number for generating unique derived table/attr names
 *
 * Note:
 *   It requires that node is a subquery of the form
 *       select ... from c x, ... where ... and x {=|IN} expr
 *       pred is x {=|IN} expr
 *   and modifies node
 *   and effects that rewrites node into the form
 *       select ... from table({expr}) as t(x), ... where ...
 */
static PT_NODE *
qo_rewrite_oid_equality (PARSER_CONTEXT * parser, PT_NODE * node,
			 PT_NODE * pred, int *seqno)
{
  PT_NODE *prev, *next, *from, *new_spec, *prev_spec = NULL;
  UINTPTR spec_id = 0;
  int found;

  /* make sure we have reasonable arguments */
  if (pred->node_type != PT_EXPR || pred->type_enum != PT_TYPE_LOGICAL
      || (pred->info.expr.op != PT_EQ && pred->info.expr.op != PT_IS_IN))
    {
      return node;
    }
  else if (pred->info.expr.arg1
	   && pred->info.expr.arg1->node_type == PT_NAME
	   && pred->info.expr.arg1->info.name.meta_class == PT_OID_ATTR
	   && qo_is_oid_const (pred->info.expr.arg2))
    {
      spec_id = pred->info.expr.arg1->info.name.spec_id;
    }
  else if (pred->info.expr.arg2
	   && pred->info.expr.arg2->node_type == PT_NAME
	   && pred->info.expr.arg2->info.name.meta_class == PT_OID_ATTR
	   && qo_is_oid_const (pred->info.expr.arg1))
    {
      spec_id = pred->info.expr.arg2->info.name.spec_id;
    }
  else
    {
      return node;		/* bail out without rewriting node */
    }

  /* make sure spec_id resolves to a regular spec in node */
  from = node->info.query.q.select.from;
  if (from && from->node_type == PT_SPEC && from->info.spec.id == spec_id)
    {
      found = 1;
    }
  else
    {
      found = 0;
      prev_spec = from;
      while (from && from->node_type == PT_SPEC)
	{
	  if (from->info.spec.id == spec_id)
	    {
	      found = 1;
	      break;
	    }
	  prev_spec = from;
	  from = from->next;
	}
    }

  if (!found)
    {
      return node;		/* bail out without rewriting node */
    }

  /* There is no advantage to rewriting class OID predicates like
   *   select ... from class c x, ... where x = expr
   * so screen those cases out now.
   */
  if (from->info.spec.meta_class == PT_META_CLASS)
    return node;		/* bail out without rewriting node */

  /* put node's PT_SPEC into a new derived table type PT_SPEC */
  new_spec = qo_make_new_derived_tblspec (parser, from, pred, seqno);
  if (!new_spec)
    return node;		/* bail out without rewriting node */

  /* excise pred from node's where clause */
  if (pred == node->info.query.q.select.where)
    {
      node->info.query.q.select.where = pred->next;
    }
  else
    {
      prev = next = node->info.query.q.select.where;
      while (next)
	{
	  if (next == pred)
	    {
	      prev->next = next->next;
	      break;
	    }
	  prev = next;
	  next = next->next;
	}
    }

  /* replace old PT_SPEC with new_spec in node's from list */
  new_spec->next = from->next;
  from->next = NULL;
  if (from == node->info.query.q.select.from)
    {
      node->info.query.q.select.from = new_spec;
    }
  else if (prev_spec != NULL)
    {
      prev_spec->next = new_spec;
    }

  /* transform attribute references x.i in
   *   select x.i, ... from c x, ... where x.i ... and x {=|IN} expr
   * into path expressions t.x.i in
   *   select t.x.i, ... from table({expr}) as t(x), ... where t.x.i ...
   */
  {
    TO_DOT_INFO dinfo;
    dinfo.old_spec = from;
    dinfo.new_spec = new_spec;
    parser_walk_tree (parser, node, qo_convert_attref_to_dotexpr_pre, &dinfo,
		      qo_convert_attref_to_dotexpr, &dinfo);
  }

  node = mq_reset_ids_in_statement (parser, node);
  return node;
}

/*
 * qo_collect_name_spec () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_collect_name_spec (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		      int *continue_walk)
{
  PT_NAME_SPEC_INFO *info = (PT_NAME_SPEC_INFO *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  switch (node->node_type)
    {
    case PT_DOT_:
      node = pt_get_end_path_node (node);
      if (node->node_type != PT_NAME)
	{
	  break;		/* impossible case, give up */
	}

      /* FALL THROUGH */

    case PT_NAME:
      if (info->c_name->info.name.location > 0
	  && info->c_name->info.name.location < node->info.name.location)
	{
	  /* next outer join location */
	}
      else
	{
	  if (node->info.name.spec_id == info->c_name->info.name.spec_id)
	    {
	      /* check for name spec is same */
	      if (pt_name_equal (parser, node, info->c_name))
		{
		  info->c_name_num++;	/* found reduced attr */
		}
	    }
	  else
	    {
	      PT_NODE *point, *s_name;

	      /* check for spec in other spec */
	      for (point = info->s_point_list; point; point = point->next)
		{
		  s_name = point;
		  CAST_POINTER_TO_NODE (s_name);
		  if (s_name->info.name.spec_id == node->info.name.spec_id)
		    break;
		}

	      /* not found */
	      if (!point)
		{
		  info->s_point_list =
		    parser_append_node (pt_point (parser, node),
					info->s_point_list);
		}
	    }
	}

      *continue_walk = PT_LIST_WALK;
      break;

    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      /* simply give up when we find query in predicate
       */
      info->query_serial_num++;
      break;

    case PT_EXPR:
      if (node->info.expr.op == PT_NEXT_VALUE
	  || node->info.expr.op == PT_CURRENT_VALUE)
	{
	  /* simply give up when we find serial
	   */
	  info->query_serial_num++;
	}
      break;
    default:
      break;
    }				/* switch (node->node_type) */

  if (info->query_serial_num > 0)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return node;
}

/*
 * qo_collect_name_spec_post () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_collect_name_spec_post (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
			   int *continue_walk)
{
  PT_NAME_SPEC_INFO *info = (PT_NAME_SPEC_INFO *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (info->query_serial_num > 0)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return node;
}

/*
 * qo_is_cast_attr () -
 *   return:
 *   expr(in):
 */
static int
qo_is_cast_attr (PT_NODE * expr)
{
  PT_NODE *arg1;

  /* check for CAST-expr  */
  if (!expr || expr->node_type != PT_EXPR || expr->info.expr.op != PT_CAST ||
      !(arg1 = expr->info.expr.arg1))
    {
      return 0;
    }

  return pt_is_attr (arg1);
}

/*
 * qo_is_reduceable_const () -
 *   return:
 *   expr(in):
 */
static int
qo_is_reduceable_const (PT_NODE * expr)
{
  while (expr && expr->node_type == PT_EXPR && expr->info.expr.op == PT_CAST)
    {
      expr = expr->info.expr.arg1;
    }

  return PT_IS_CONST_INPUT_HOSTVAR (expr);
}

/*
 * qo_reduce_equality_terms () -
 *   return:
 *   parser(in):
 *   node(in):
 *   wherep(in):
 *
 *  Obs: modified to support PRIOR operator as follows:
 *    -> PRIOR field = exp1 AND PRIOR field = exp2 =>
 *	 PRIOR field = exp1 AND exp1 = exp2
 *    -> PRIOR ? -> replace with ?
 */
static void
qo_reduce_equality_terms (PARSER_CONTEXT * parser, PT_NODE * node,
			  PT_NODE ** wherep)
{
  PT_NODE *from;
  PT_NODE **orgp;
  PT_NODE *accumulator, *expr, *arg1, *arg2, *temp, *next;
  PT_NODE *join_term, *join_term_list, *s_name1, *s_name2;
  PT_NAME_SPEC_INFO info1, info2;
  int spec1_cnt, spec2_cnt;
  bool found_equality_term, found_join_term;
  PT_NODE *spec, *derived_table, *attr, *col;
  int i, num_check, idx;
  PT_NODE *save_where_next;
  bool copy_arg2;
  PT_NODE *dt1, *dt2;

  /* init */
  orgp = wherep;
  accumulator = NULL;
  join_term_list = NULL;

  while ((expr = *wherep))
    {
      col = NULL;		/* init - reserve for constant column of derived-table */

      /* check for 1st phase; keep out OR conjunct; 1st init
       */
      found_equality_term = (expr->or_next == NULL) ? true : false;

      if (found_equality_term != true)
	{
	  wherep = &(*wherep)->next;
	  continue;		/* give up */
	}

      /* check for 2nd phase; '=', 'range ( =)'
       */
      found_equality_term = false;	/* 2nd init */

      if (expr->info.expr.op == PT_EQ
	  && expr->info.expr.arg1 && expr->info.expr.arg2)
	{			/* 'opd = opd' */
	  found_equality_term = true;	/* pass 2nd phase */
	  num_check = 2;
	}
      else if (expr->info.expr.op == PT_RANGE)
	{			/* 'opd range (opd =)' */
	  PT_NODE *between_and;

	  between_and = expr->info.expr.arg2;
	  if (between_and->or_next == NULL	/* has only one range */
	      && between_and->info.expr.op == PT_BETWEEN_EQ_NA)
	    {
	      found_equality_term = true;	/* pass 2nd phase */
	      num_check = 1;
	    }
	}

      if (found_equality_term != true)
	{
	  wherep = &(*wherep)->next;
	  continue;		/* give up */
	}

      /* check for 3rd phase; 'attr = const', 'attr range (const =)'
       */
      found_equality_term = false;	/* 3rd init */

      for (i = 0; i < num_check; i++)
	{
	  arg1 = (i == 0) ? expr->info.expr.arg1 : expr->info.expr.arg2;
	  arg2 = (i == 0) ? expr->info.expr.arg2 : expr->info.expr.arg1;

	  if (expr->info.expr.op == PT_RANGE)
	    {
	      arg2 = arg2->info.expr.arg1;
	    }

	  /* if arg1 is expression with PRIOR, move arg1 to the arg1 of PRIOR */

	  if (arg1->node_type == PT_EXPR && arg1->info.expr.op == PT_PRIOR
	      && pt_is_attr (arg1->info.expr.arg1))
	    {
	      arg1 = arg1->info.expr.arg1;
	    }

	  if (pt_is_attr (arg1))
	    {
	      if (qo_is_reduceable_const (arg2))
		{
		  found_equality_term = true;
		  break;	/* immediately break */
		}
	      else if (pt_is_attr (arg2))
		{
		  ;		/* nop */
		}
	      else if (qo_is_cast_attr (arg2))
		{
		  arg2 = arg2->info.expr.arg1;
		}
	      else
		{
		  continue;	/* not found. step to next */
		}

	      if (node->node_type == PT_SELECT)
		{
		  from = node->info.query.q.select.from;
		}
	      else
		{
		  from = NULL;	/* not found. step to next */
		}

	      for (spec = from; spec; spec = spec->next)
		{
		  if (spec->info.spec.id == arg2->info.name.spec_id)
		    break;	/* found match */
		}

	      /* if arg2 is derived alias col, get its corresponding
	       * constant column from derived-table
	       */
	      if (spec
		  && spec->info.spec.derived_table_type == PT_IS_SUBQUERY
		  && (derived_table = spec->info.spec.derived_table)
		  && derived_table->node_type == PT_SELECT)
		{
		  /* traverse as_attr_list */
		  for (attr = spec->info.spec.as_attr_list, idx = 0;
		       attr; attr = attr->next, idx++)
		    {
		      if (pt_name_equal (parser, attr, arg2))
			break;	/* found match */
		    }		/* for (attr = ...) */

		  /* get corresponding column */
		  col = pt_get_select_list (parser, derived_table);
		  for (; col && idx; col = col->next, idx--)
		    {
		      ;		/* step to next */
		    }

		  if (attr && col && qo_is_reduceable_const (col))
		    {
		      /* add additional equailty-term; is reduced */
		      *wherep =
			parser_append_node (parser_copy_tree (parser, expr),
					    *wherep);

		      /* reset arg1, arg2 */
		      arg1 = arg2;
		      arg2 = col;

		      found_equality_term = true;
		      break;	/* immediately break */
		    }
		}		/* if arg2 is derived alias-column */
	    }			/* if (pt_is_attr(arg1)) */
	}			/* for (i = 0; ...) */

      if (found_equality_term != true)
	{
	  wherep = &(*wherep)->next;
	  continue;		/* give up */
	}

      /*
       * now, finally pass all check
       */

      save_where_next = (*wherep)->next;

      if (pt_is_attr (arg2))
	{
	  temp = arg1;
	  arg1 = arg2;
	  arg2 = temp;
	}

      /* at here, arg1 is reduced attr */

      *wherep = expr->next;
      if (col)
	{
	  ;			/* corresponding constant column of derived-table */
	}
      else
	{
	  expr->next = accumulator;
	  accumulator = expr;
	}

      /* Restart where at beginning of WHERE clause because
         we may find new terms after substitution, and must
         substitute entire where clause because incoming
         order is arbitrary. */
      wherep = orgp;

      temp = pt_get_end_path_node (arg1);

      info1.c_name = temp;
      info2.c_name = temp;

      /* save reduced join terms */
      for (temp = *wherep; temp; temp = temp->next)
	{
	  if (temp == expr)
	    {
	      /* this is the working equality_term, skip and go ahead */
	      continue;
	    }

	  if (temp->node_type != PT_EXPR ||
	      !pt_is_symmetric_op (temp->info.expr.op))
	    {
	      /* skip and go ahead */
	      continue;
	    }

	  next = temp->next;	/* save and cut-off link */
	  temp->next = NULL;

	  /* check for already added join term */
	  for (join_term = join_term_list; join_term;
	       join_term = join_term->next)
	    {
	      if (join_term->etc == (void *) temp)
		break;		/* found */
	    }

	  /* check for not added join terms */
	  if (join_term == NULL)
	    {

	      found_join_term = false;	/* init */

	      /* check for attr of other specs */
	      if (temp->or_next == NULL)
		{
		  info1.c_name_num = 0;
		  info1.query_serial_num = 0;
		  info1.s_point_list = NULL;
		  (void) parser_walk_tree (parser, temp->info.expr.arg1,
					   qo_collect_name_spec, &info1,
					   qo_collect_name_spec_post, &info1);

		  info2.c_name_num = 0;
		  info2.query_serial_num = 0;
		  info2.s_point_list = NULL;
		  if (info1.query_serial_num == 0)
		    {
		      (void) parser_walk_tree (parser, temp->info.expr.arg2,
					       qo_collect_name_spec, &info2,
					       qo_collect_name_spec_post,
					       &info2);
		    }

		  if (info1.query_serial_num == 0
		      && info2.query_serial_num == 0)
		    {
		      /* check for join term related to reduced attr
		       * lhs and rhs has name of other spec
		       *   CASE 1: X.c_name          = Y.attr
		       *   CASE 2: X.c_name + Y.attr = ?
		       *   CASE 3:            Y.attr =          X.c_name
		       *   CASE 4:                 ? = Y.attr + X.c_name
		       */

		      spec1_cnt = pt_length_of_list (info1.s_point_list);
		      spec2_cnt = pt_length_of_list (info2.s_point_list);

		      if (info1.c_name_num)
			{
			  if (spec1_cnt == 0)
			    {	/* CASE 1 */
			      if (spec2_cnt == 1)
				{
				  found_join_term = true;
				}
			    }
			  else if (spec1_cnt == 1)
			    {	/* CASE 2 */
			      if (spec2_cnt == 0)
				{
				  found_join_term = true;
				}
			      else if (spec2_cnt == 1)
				{
				  s_name1 = info1.s_point_list;
				  s_name2 = info2.s_point_list;
				  CAST_POINTER_TO_NODE (s_name1);
				  CAST_POINTER_TO_NODE (s_name2);
				  if (s_name1->info.name.spec_id ==
				      s_name2->info.name.spec_id)
				    {
				      /* X.c_name + Y.attr = Y.attr */
				      found_join_term = true;
				    }
				  else
				    {
				      /* X.c_name + Y.attr = Z.attr */
				      ;	/* nop */
				    }
				}
			    }
			}
		      else if (info2.c_name_num)
			{
			  if (spec2_cnt == 0)
			    {	/* CASE 3 */
			      if (spec1_cnt == 1)
				{
				  found_join_term = true;
				}
			    }
			  else if (spec2_cnt == 1)
			    {	/* CASE 4 */
			      if (spec1_cnt == 0)
				{
				  found_join_term = true;
				}
			      else if (spec1_cnt == 1)
				{
				  s_name1 = info1.s_point_list;
				  s_name2 = info2.s_point_list;
				  CAST_POINTER_TO_NODE (s_name1);
				  CAST_POINTER_TO_NODE (s_name2);
				  if (s_name1->info.name.spec_id ==
				      s_name2->info.name.spec_id)
				    {
				      /* Y.attr = Y.attr + X.c_name */
				      found_join_term = true;
				    }
				  else
				    {
				      /* Z.attr = Y.attr + X.c_name */
				      ;	/* nop */
				    }
				}
			    }
			}
		    }

		  /* free name list */
		  if (info1.s_point_list)
		    {
		      parser_free_tree (parser, info1.s_point_list);
		    }
		  if (info2.s_point_list)
		    {
		      parser_free_tree (parser, info2.s_point_list);
		    }
		}		/* if (temp->or_next == NULL) */

	      if (found_join_term)
		{
		  join_term = parser_copy_tree (parser, temp);
		  join_term->etc = (void *) temp;	/* mark as added */
		  join_term_list =
		    parser_append_node (join_term, join_term_list);
		}

	    }			/* if (join_term == NULL) */

	  temp->next = next;	/* restore link */
	}			/* for (term = *wherep; term; term = term->next) */

      copy_arg2 = false;	/* init */

      if (PT_IS_PARAMETERIZED_TYPE (arg1->type_enum))
	{
	  DB_VALUE *dbval, dbval_res;
	  TP_DOMAIN *dom;

	  /* don't replace node's data type precision, scale
	   */
	  if (PT_IS_CONST_NOT_HOSTVAR (arg2))
	    {
	      dom = pt_node_to_db_domain (parser, arg1, NULL);
	      dom = tp_domain_cache (dom);
	      if (dom->precision <= DB_MAX_LITERAL_PRECISION)
		{
		  if ((dbval = pt_value_to_db (parser, arg2)) == NULL)
		    {
		      *wherep = save_where_next;
		      continue;	/* give up */
		    }
		  DB_MAKE_NULL (&dbval_res);
		  if (tp_value_cast (dbval, &dbval_res, dom,
				     false) != DOMAIN_COMPATIBLE)
		    {
		      PT_ERRORmf2 (parser, arg2,
				   MSGCAT_SET_PARSER_SEMANTIC,
				   MSGCAT_SEMANTIC_CANT_COERCE_TO,
				   pt_short_print (parser, arg2),
				   pt_show_type_enum (arg1->type_enum));
		      *wherep = save_where_next;
		      continue;	/* give up */
		    }
		  temp = pt_dbval_to_value (parser, &dbval_res);
		  pr_clear_value (&dbval_res);
		}
	      else
		{		/* too big literal string */
		  temp =
		    pt_wrap_with_cast_op (parser,
					  parser_copy_tree_list (parser,
								 arg2),
					  arg1->type_enum,
					  ((arg1->data_type) ? arg1->
					   data_type->info.data_type.
					   precision :
					   TP_FLOATING_PRECISION_VALUE),
					  ((arg1->data_type) ? arg1->
					   data_type->info.data_type.
					   dec_precision :
					   TP_FLOATING_PRECISION_VALUE));
		  if (temp == NULL)
		    {
		      PT_ERRORm (parser, arg2, MSGCAT_SET_PARSER_SEMANTIC,
				 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
		      *wherep = save_where_next;
		      continue;	/* give up */
		    }
		}
	    }
	  else
	    {			/* is CAST expr */
	      if ((dt1 = arg1->data_type) && (dt2 = arg2->data_type)
		  && dt1->type_enum == dt2->type_enum
		  && (dt1->info.data_type.precision ==
		      dt2->info.data_type.precision)
		  && (dt1->info.data_type.dec_precision ==
		      dt2->info.data_type.dec_precision))
		{
		  /* exactly the same type */
		  if ((temp = parser_copy_tree_list (parser, arg2)) == NULL)
		    {
		      PT_ERRORm (parser, arg2, MSGCAT_SET_PARSER_SEMANTIC,
				 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
		      *wherep = save_where_next;
		      continue;	/* give up */
		    }
		}
	      else
		{		/* create nested CAST node */
		  temp =
		    pt_wrap_with_cast_op (parser,
					  parser_copy_tree_list (parser,
								 arg2),
					  arg1->type_enum,
					  ((arg1->data_type) ? arg1->
					   data_type->info.data_type.
					   precision :
					   TP_FLOATING_PRECISION_VALUE),
					  ((arg1->data_type) ? arg1->
					   data_type->info.data_type.
					   dec_precision :
					   TP_FLOATING_PRECISION_VALUE));
		  if (temp == NULL)
		    {
		      PT_ERRORm (parser, arg2, MSGCAT_SET_PARSER_SEMANTIC,
				 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
		      *wherep = save_where_next;
		      continue;	/* give up */
		    }
		}
	    }

	  arg2 = temp;

	  copy_arg2 = true;	/* mark as copy */
	}

      /* replace 'arg1' in '*wherep' with 'arg2' with location checking */
      temp = pt_get_end_path_node (arg1);

      if (node->node_type == PT_SELECT)
	{			/* query with WHERE condition */
	  node->info.query.q.select.list =
	    pt_lambda_with_arg (parser, node->info.query.q.select.list,
				arg1, arg2,
				(temp->info.name.location > 0 ? true
				 : false),
				1 /* type: check normal func data_type */ ,
				true /* dont_replace */ );
	}
      *wherep =
	pt_lambda_with_arg (parser, *wherep, arg1, arg2,
			    (temp->info.name.location > 0 ? true
			     : false),
			    1 /* type: check normal func data_type */ ,
			    false /* dont_replace: DEFAULT */ );

      /* Leave "wherep" pointing at the begining of the
         rest of the predicate. We still gurantee loop
         termination because we have removed a term.
         future iterations which do not fall into this
         case will advance to the next term. */

      /* free copied constant column */
      if (copy_arg2)
	{
	  parser_free_tree (parser, arg2);
	}
    }

  *orgp = parser_append_node (accumulator, *orgp);

  if (join_term_list)
    {
      /* mark as transitive join terms and append to the WHERE clause */
      for (join_term = join_term_list; join_term; join_term = join_term->next)
	{
	  PT_EXPR_INFO_SET_FLAG (join_term, PT_EXPR_INFO_TRANSITIVE);
	  join_term->etc = (void *) NULL;	/* clear */
	}

      *orgp = parser_append_node (join_term_list, *orgp);
    }

}

/*
 * qo_reduce_order_by_for () - move orderby_num() to groupby_num()
 *   return: NO_ERROR if successful, otherwise returns error number
 *   parser(in): parser global context info for reentrancy
 *   node(in): query node has ORDER BY
 *
 * Note:
 *   It modifies parser's heap of PT_NODEs(parser->error_msgs)
 *   and effects that remove order by for clause
 */
static int
qo_reduce_order_by_for (PARSER_CONTEXT * parser, PT_NODE * node)
{
  int error = NO_ERROR;
  PT_NODE *ord_num, *grp_num;

  if (node->node_type != PT_SELECT)
    {
      return error;
    }

  ord_num = NULL;
  grp_num = NULL;

  /* move orderby_num() to groupby_num() */
  if (node->info.query.orderby_for)
    {
      /* generate orderby_num(), groupby_num() */
      if (!(ord_num = parser_new_node (parser, PT_EXPR))
	  || !(grp_num = parser_new_node (parser, PT_FUNCTION)))
	{
	  if (ord_num)
	    {
	      parser_free_tree (parser, ord_num);
	    }
	  PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		     MSGCAT_SEMANTIC_OUT_OF_MEMORY);
	  goto exit_on_error;
	}

      ord_num->type_enum = PT_TYPE_INTEGER;
      ord_num->info.expr.op = PT_ORDERBY_NUM;
      PT_EXPR_INFO_SET_FLAG (ord_num, PT_EXPR_INFO_ORDERBYNUM_C);

      grp_num->type_enum = PT_TYPE_INTEGER;
      grp_num->info.function.function_type = PT_GROUPBY_NUM;
      grp_num->info.function.arg_list = NULL;
      grp_num->info.function.all_or_distinct = PT_ALL;

      /* replace orderby_num() to groupby_num() */
      node->info.query.orderby_for =
	pt_lambda_with_arg (parser, node->info.query.orderby_for,
			    ord_num, grp_num, false /* loc_check: DEFAULT */ ,
			    0 /* type: DEFAULT */ ,
			    false /* dont_replace: DEFAULT */ );

      node->info.query.q.select.having =
	parser_append_node (node->info.query.orderby_for,
			    node->info.query.q.select.having);

      node->info.query.orderby_for = NULL;

      parser_free_tree (parser, ord_num);
      parser_free_tree (parser, grp_num);
    }

exit_on_end:

  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      /* missing compiler error list */
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }
  goto exit_on_end;
}

/*
 * reduce_order_by () -
 *   return: NO_ERROR, if successful, otherwise returns error number
 *   parser(in): parser global context info for reentrancy
 *   node(in): query node has ORDER BY
 *
 * Note:
 *   It modifies parser's heap of PT_NODEs(parser->error_msgs)
 *   and effects that reduce the constant orders
 */
static int
qo_reduce_order_by (PARSER_CONTEXT * parser, PT_NODE * node)
{
  int error = NO_ERROR;
  PT_NODE *order, *order_next, *order_prev, *col, *col2, *col2_next;
  PT_NODE *r, *new_r;
  int i, j;
  int const_order_count, order_move_count;
  bool need_merge_check;

  if (node->node_type != PT_SELECT)
    {
      return error;
    }

  /* init */
  const_order_count = order_move_count = 0;
  need_merge_check = false;

  /* check for merge order by to group by( without DISTINCT and HAVING clause)
   */

  if (node->info.query.all_distinct == PT_DISTINCT)
    {
      ;				/* give up */
    }
  else
    {
      if (node->info.query.q.select.group_by
	  && node->info.query.q.select.having == NULL
	  && node->info.query.order_by)
	{
	  bool ordbynum_flag;

	  ordbynum_flag = false;	/* init */

	  /* check for orderby_num() in the select list */
	  (void) parser_walk_tree (parser, node->info.query.q.select.list,
				   pt_check_orderbynum_pre, NULL,
				   pt_check_orderbynum_post, &ordbynum_flag);

	  if (ordbynum_flag)
	    {			/* found orderby_num() in the select list */
	      ;			/* give up */
	    }
	  else
	    {
	      need_merge_check = true;	/* mark to checking */
	    }
	}
    }

  /* the first phase, do check the current order by */
  if (need_merge_check)
    {
      if (pt_sort_spec_cover (node->info.query.q.select.group_by,
			      node->info.query.order_by))
	{
	  if (qo_reduce_order_by_for (parser, node) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  if (node->info.query.orderby_for == NULL)
	    {
	      /* clear unnecessary node info */
	      parser_free_tree (parser, node->info.query.order_by);
	      node->info.query.order_by = NULL;
	    }

	  need_merge_check = false;	/* clear */
	}
    }

  order_prev = NULL;
  for (order = node->info.query.order_by; order; order = order_next)
    {
      order_next = order->next;

      r = order->info.sort_spec.expr;

      /*
         safe guard: check for integer value.
       */
      if (r->node_type != PT_VALUE)
	{
	  goto exit_on_error;
	}

      col = node->info.query.q.select.list;
      for (i = 1; i < r->info.value.data_value.i; i++)
	{
	  if (col == NULL)
	    {			/* impossible case */
	      break;
	    }
	  col = col->next;
	}

      /*
         safe guard: invalid parse tree
       */
      if (col == NULL)
	{
	  goto exit_on_error;
	}

      col = pt_get_end_path_node (col);
      if (col->node_type == PT_NAME)
	{
	  if (PT_NAME_INFO_IS_FLAGED (col, PT_NAME_INFO_CONSTANT))
	    {
	      /* remove constant order node
	       */
	      if (order_prev == NULL)
		{		/* the first */
		  node->info.query.order_by = order->next;	/* re-link */
		}
	      else
		{
		  order_prev->next = order->next;	/* re-link */
		}
	      order->next = NULL;	/* cut-off */
	      parser_free_tree (parser, order);

	      const_order_count++;	/* increase const entry remove count */

	      continue;		/* go ahead */
	    }

	  /* for non-constant order, change order position to
	   * the same left-most col's position
	   */
	  col2 = node->info.query.q.select.list;
	  for (j = 1; j < i; j++)
	    {
	      col2_next = col2->next;	/* save next link */

	      col2 = pt_get_end_path_node (col2);

	      /* change to the same left-most col */
	      if (pt_name_equal (parser, col2, col))
		{
		  new_r = parser_new_node (parser, PT_VALUE);
		  if (new_r == NULL)
		    {
		      error = MSGCAT_SEMANTIC_OUT_OF_MEMORY;
		      PT_ERRORm (parser, col, MSGCAT_SET_PARSER_SEMANTIC,
				 error);
		      goto exit_on_error;
		    }

		  new_r->type_enum = PT_TYPE_INTEGER;
		  new_r->info.value.data_value.i = j;
		  pt_value_to_db (parser, new_r);
		  parser_free_tree (parser, r);
		  order->info.sort_spec.expr = new_r;
		  order->info.sort_spec.pos_descr.pos_no = j;

		  order_move_count++;	/* increase entry move count */

		  break;	/* exit for-loop */
		}

	      col2 = col2_next;	/* restore next link */
	    }
	}

      order_prev = order;	/* go ahead */
    }

  if (order_move_count > 0)
    {
      PT_NODE *match;

      /* now check for duplicate entries.
       *  - If they match on ascending/descending, remove the second.
       *  - If they do not, generate an error.
       */
      for (order = node->info.query.order_by; order; order = order->next)
	{
	  while ((match =
		  pt_find_order_value_in_list (parser,
					       order->info.sort_spec.expr,
					       order->next)))
	    {
	      if (order->info.sort_spec.asc_or_desc !=
		  match->info.sort_spec.asc_or_desc)
		{
		  error = MSGCAT_SEMANTIC_SORT_DIR_CONFLICT;
		  PT_ERRORmf (parser, match, MSGCAT_SET_PARSER_SEMANTIC,
			      error, pt_short_print (parser, match));
		  goto exit_on_error;
		}
	      else
		{
		  order->next = pt_remove_from_list (parser,
						     match, order->next);
		}
	    }			/* while */
	}			/* for (order = ...) */
    }

  if (const_order_count > 0)
    {				/* is reduced */
      /* the second phase, do check with reduced order by */
      if (need_merge_check)
	{
	  if (pt_sort_spec_cover (node->info.query.q.select.group_by,
				  node->info.query.order_by))
	    {
	      if (qo_reduce_order_by_for (parser, node) != NO_ERROR)
		{
		  goto exit_on_error;
		}

	      if (node->info.query.orderby_for == NULL)
		{
		  /* clear unnecessary node info */
		  parser_free_tree (parser, node->info.query.order_by);
		  node->info.query.order_by = NULL;
		}

	      need_merge_check = false;	/* clear */
	    }
	}
      else
	{
	  if (node->info.query.order_by == NULL)
	    {
	      /* move orderby_num() to inst_num() */
	      if (node->info.query.orderby_for)
		{
		  PT_NODE *ord_num, *ins_num;

		  ord_num = NULL;
		  ins_num = NULL;

		  /* generate orderby_num(), inst_num() */
		  if (!(ord_num = parser_new_node (parser, PT_EXPR))
		      || !(ins_num = parser_new_node (parser, PT_EXPR)))
		    {
		      if (ord_num)
			{
			  parser_free_tree (parser, ord_num);
			}
		      PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
				 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
		      goto exit_on_error;
		    }

		  ord_num->type_enum = PT_TYPE_INTEGER;
		  ord_num->info.expr.op = PT_ORDERBY_NUM;
		  PT_EXPR_INFO_SET_FLAG (ord_num, PT_EXPR_INFO_ORDERBYNUM_C);

		  ins_num->type_enum = PT_TYPE_INTEGER;
		  ins_num->info.expr.op = PT_INST_NUM;
		  PT_EXPR_INFO_SET_FLAG (ins_num, PT_EXPR_INFO_INSTNUM_C);

		  /* replace orderby_num() to inst_num() */
		  node->info.query.orderby_for =
		    pt_lambda_with_arg (parser, node->info.query.orderby_for,
					ord_num, ins_num,
					false /* loc_check: DEFAULT */ ,
					0 /* type: DEFAULT */ ,
					false /* dont_replace: DEFAULT */ );

		  node->info.query.q.select.where =
		    parser_append_node (node->info.query.orderby_for,
					node->info.query.q.select.where);

		  node->info.query.orderby_for = NULL;

		  parser_free_tree (parser, ord_num);
		  parser_free_tree (parser, ins_num);
		}
	    }
	}
    }

exit_on_end:

  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      /* missing compiler error list */
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }
  goto exit_on_end;
}

/*
 * qo_converse_sarg_terms () -
 *   return:
 *   parser(in):
 *   where(in): CNF list of WHERE clause
 *
 * Note:
 *      Convert terms of the form 'constant op attr' to 'attr op constant'
 *      by traversing expression tree with prefix order (left child,
 *      right child, and then parent). Convert 'attr op attr' so, LHS has more
 *      common attribute.
 *
 * 	examples:
 *  	0. where 5 = a                     -->  where a = 5
 *  	1. where -5 = -a                   -->  where a = 5
 *  	2. where -5 = -(-a)                -->  where a = -5
 *  	3. where 5 = -a                    -->  where a = -5
 *  	4. where 5 = -(-a)                 -->  where a = 5
 *  	5. where 5 > x.a and/or x.a = y.b  -->  where x.a < 5 and/or x.a = y.b
 *  	6. where b = a or c = a            -->  where a = b or a = c
 *  	7. where b = -a or c = a           -->  where a = -b or a = c
 *  	8. where b = a or c = a            -->  where a = b or a = c
 *  	9. where a = b or b = c or d = b   -->  where b = a or b = c or b = d
 *
 * Obs: modified to support PRIOR
 * 	examples:
 *  	0. connect by 5 = prior a          -->  connect by prior a = 5
 *  	1. connect by -5 = prior (-a)      -->  connect by prior a = 5
 *	...
 *	prior(-attr) between opd1 and opd2 -->
 *      prior(-attr) >= opd1 AND prior(-attr) <= opd2 -->
 *	prior (attr) <= -opd1 AND prior(attr) >= -opd2 -->
 *	prior (attr) between -opd2 and -opd1
 */
static void
qo_converse_sarg_terms (PARSER_CONTEXT * parser, PT_NODE * where)
{
  PT_NODE *cnf_node, *dnf_node, *arg1, *arg2, *arg1_arg1, *arg2_arg1;
  PT_NODE *arg1_prior_father, *arg2_prior_father;
  PT_OP_TYPE op_type;
  PT_NODE *attr, *attr_list;
  int arg1_cnt, arg2_cnt;


  /* traverse CNF list */
  for (cnf_node = where; cnf_node; cnf_node = cnf_node->next)
    {
      attr_list = NULL;		/* init */

      /* STEP 1: traverse DNF list to generate attr_list */
      for (dnf_node = cnf_node; dnf_node; dnf_node = dnf_node->or_next)
	{

	  if (dnf_node->node_type != PT_EXPR)
	    {
	      continue;
	    }

	  op_type = dnf_node->info.expr.op;
	  /* not CNF/DNF form; give up */
	  if (op_type == PT_AND || op_type == PT_OR)
	    {
	      if (attr_list)
		{
		  parser_free_tree (parser, attr_list);
		  attr_list = NULL;
		}

	      break;		/* immediately, exit loop */
	    }

	  arg1_prior_father = arg2_prior_father = NULL;

	  arg1 = dnf_node->info.expr.arg1;
	  /*  go in PRIOR argument but memorize it for further node manag */
	  if (pt_is_expr_node (arg1) && arg1->info.expr.op == PT_PRIOR)
	    {
	      arg1_prior_father = arg1;
	      arg1 = arg1->info.expr.arg1;
	    }

	  arg1_arg1 = ((pt_is_expr_node (arg1)
			&& arg1->info.expr.op == PT_UNARY_MINUS)
		       ? arg1->info.expr.arg1 : NULL);
	  while (pt_is_expr_node (arg1)
		 && arg1->info.expr.op == PT_UNARY_MINUS)
	    {
	      arg1 = arg1->info.expr.arg1;
	    }

	  if (op_type == PT_BETWEEN && arg1_arg1 && pt_is_attr (arg1))
	    {
	      /* term in the form of '-attr between opd1 and opd2'
	         convert to '-attr >= opd1 and -attr <= opd2' */

	      /* check for one range spec */
	      if (cnf_node == dnf_node && dnf_node->or_next == NULL)
		{
		  arg2 = dnf_node->info.expr.arg2;
		  /* term of '-attr >= opd1' */
		  dnf_node->info.expr.arg2 = arg2->info.expr.arg1;
		  op_type = dnf_node->info.expr.op = PT_GE;
		  /* term of '-attr <= opd2' */
		  arg2->info.expr.arg1 =
		    parser_copy_tree (parser, dnf_node->info.expr.arg1);
		  arg2->info.expr.op = PT_LE;
		  /* term of 'and' */
		  arg2->next = dnf_node->next;
		  dnf_node->next = arg2;
		}
	    }

	  arg2 = dnf_node->info.expr.arg2;

	  /*  go in PRIOR argument but memorize it for further node manag */
	  if (pt_is_expr_node (arg2) && arg2->info.expr.op == PT_PRIOR)
	    {
	      arg2_prior_father = arg2;
	      arg2 = arg2->info.expr.arg1;
	    }

	  while (pt_is_expr_node (arg2)
		 && arg2->info.expr.op == PT_UNARY_MINUS)
	    {
	      arg2 = arg2->info.expr.arg1;
	    }

	  /* add sargable attribute to attr_list */
	  if (arg1 && arg2 && pt_converse_op (op_type) != 0)
	    {
	      if (pt_is_attr (arg1))
		{
		  for (attr = attr_list; attr; attr = attr->next)
		    {
		      if (pt_name_equal (parser, attr, arg1))
			{
			  attr->line_number++;	/* increase attribute count */
			  break;
			}
		    }

		  /* not found; add new attribute */
		  if (attr == NULL)
		    {
		      attr = pt_point (parser, arg1);
		      if (attr != NULL)
			{
			  attr->line_number = 1;	/* set attribute count */

			  attr_list = parser_append_node (attr_list, attr);
			}
		    }
		}

	      if (pt_is_attr (arg2))
		{
		  for (attr = attr_list; attr; attr = attr->next)
		    {
		      if (pt_name_equal (parser, attr, arg2))
			{
			  attr->line_number++;	/* increase attribute count */
			  break;
			}
		    }

		  /* not found; add new attribute */
		  if (attr == NULL)
		    {
		      attr = pt_point (parser, arg2);

		      if (attr != NULL)
			{
			  attr->line_number = 1;	/* set attribute count */

			  attr_list = parser_append_node (attr_list, attr);
			}
		    }
		}
	    }
	}

      /* STEP 2: re-traverse DNF list to converse sargable terms */
      for (dnf_node = cnf_node; dnf_node; dnf_node = dnf_node->or_next)
	{
	  if (dnf_node->node_type != PT_EXPR)
	    continue;

	  arg1_prior_father = arg2_prior_father = NULL;

	  /* filter out unary minus nodes */
	  while ((arg1 = dnf_node->info.expr.arg1)
		 && (arg2 = dnf_node->info.expr.arg2))
	    {
	      /*  go in PRIOR argument but memorize it for further node manag */
	      if (pt_is_expr_node (arg1) && arg1->info.expr.op == PT_PRIOR)
		{
		  arg1_prior_father = arg1;
		  arg1 = arg1->info.expr.arg1;
		}

	      if (pt_is_expr_node (arg2) && arg2->info.expr.op == PT_PRIOR)
		{
		  arg2_prior_father = arg2;
		  arg2 = arg2->info.expr.arg1;
		}

	      op_type = pt_converse_op (dnf_node->info.expr.op);
	      arg1_arg1 = ((pt_is_expr_node (arg1)
			    && arg1->info.expr.op == PT_UNARY_MINUS)
			   ? arg1->info.expr.arg1 : NULL);
	      arg2_arg1 = ((pt_is_expr_node (arg2)
			    && arg2->info.expr.op == PT_UNARY_MINUS)
			   ? arg2->info.expr.arg1 : NULL);

	      if (arg1_arg1 && arg2_arg1)
		{
		  /* Delete both minus from prior also. */
		  if (arg1_prior_father)
		    {
		      arg1_prior_father->info.expr.arg1 =
			arg1_prior_father->info.expr.arg1->info.expr.arg1;
		    }
		  if (arg2_prior_father)
		    {
		      arg2_prior_father->info.expr.arg1 =
			arg2_prior_father->info.expr.arg1->info.expr.arg1;
		    }

		  /* term in the form of '-something op -something' */
		  dnf_node->info.expr.arg1 = arg1->info.expr.arg1;
		  arg1->info.expr.arg1 = NULL;
		  parser_free_tree (parser, arg1);
		  dnf_node->info.expr.arg2 = arg2->info.expr.arg1;
		  arg2->info.expr.arg1 = NULL;
		  parser_free_tree (parser, arg2);

		  /* both minus operators are gone but they were written over
		     the prior operator so we must add them again. */
		  if (arg1_prior_father)
		    {
		      dnf_node->info.expr.arg1 = arg1_prior_father;
		    }
		  if (arg2_prior_father)
		    {
		      dnf_node->info.expr.arg2 = arg2_prior_father;
		    }
		}
	      else if (op_type != 0 && arg1_arg1
		       && (pt_is_attr (arg1_arg1)
			   || (pt_is_expr_node (arg1_arg1)
			       && arg1_arg1->info.expr.op == PT_UNARY_MINUS))
		       && pt_is_const (arg2))
		{
		  /* arg1 was with prior, make the modifications in prior
		     and move the prior to dnf_node->info.expr.arg2 */

		  /* prior (-attr) op const => prior attr op -const */
		  if (arg1_prior_father)
		    {
		      /* cut - from prior -attr */
		      arg1_prior_father->info.expr.arg1 =
			arg1->info.expr.arg1;

		      dnf_node->info.expr.arg1 = arg1_prior_father;
		      arg1->info.expr.arg1 = arg2;
		      dnf_node->info.expr.arg2 = arg1;
		    }
		  else
		    {
		      /* term in the form of '-attr op const' or
		         '-(-something) op const' */
		      dnf_node->info.expr.arg1 = arg1->info.expr.arg1;
		      arg1->info.expr.arg1 = arg2;
		      dnf_node->info.expr.arg2 = arg1;
		    }
		}
	      else if (op_type != 0 && arg2_arg1
		       && (pt_is_attr (arg2->info.expr.arg1)
			   || (pt_is_expr_node (arg2_arg1)
			       && arg2_arg1->info.expr.op == PT_UNARY_MINUS))
		       && pt_is_const (arg1))
		{
		  /* arg2 was with prior, make the modifications in prior
		     and move the prior to dnf_node->info.expr.arg1 */

		  /* const op prior (-attr) => -const op prior attr */
		  if (arg2_prior_father)
		    {
		      /* cut - from prior -attr */
		      arg2_prior_father->info.expr.arg1 =
			arg2_prior_father->info.expr.arg1->info.expr.arg1;

		      dnf_node->info.expr.arg2 = arg2_prior_father;
		      arg2->info.expr.arg1 = arg1;
		      dnf_node->info.expr.arg1 = arg2;
		    }
		  else
		    {
		      /* term in the form of 'const op -attr' or
		         'const op -(-something)' */
		      dnf_node->info.expr.arg2 = arg2->info.expr.arg1;
		      arg2->info.expr.arg1 = arg1;
		      dnf_node->info.expr.arg1 = arg2;
		    }
		}
	      else
		{
		  break;
		}

	      /* swap term's operator */
	      dnf_node->info.expr.op = op_type;
	    }

	  op_type = dnf_node->info.expr.op;
	  arg1 = dnf_node->info.expr.arg1;
	  arg2 = dnf_node->info.expr.arg2;

	  arg1_prior_father = arg2_prior_father = NULL;
	  /* if arg1 or arg2 is PT_PRIOR, go in its argument */
	  if (pt_is_expr_node (arg1) && arg1->info.expr.op == PT_PRIOR)
	    {
	      /* keep its parent so when swapping the two elements,
	         swap with its father */
	      arg1_prior_father = arg1;
	      arg1 = arg1->info.expr.arg1;
	    }
	  if (pt_is_expr_node (arg2) && arg2->info.expr.op == PT_PRIOR)
	    {
	      arg2_prior_father = arg2;
	      arg2 = arg2->info.expr.arg1;
	    }

	  if (op_type == PT_AND)
	    {
	      /* not CNF form; what do I have to do? */

	      /* traverse left child */
	      qo_converse_sarg_terms (parser, arg1);
	      /* traverse right child */
	      qo_converse_sarg_terms (parser, arg2);

	    }
	  else if (op_type == PT_OR)
	    {
	      /* not DNF form; what do I have to do? */

	      /* traverse left child */
	      qo_converse_sarg_terms (parser, arg1);
	      /* traverse right child */
	      qo_converse_sarg_terms (parser, arg2);

	    }
	  /* sargable term, where 'op_type' is one of
	   * '=', '<' '<=', '>', or '>='
	   */
	  else if (arg1 && arg2
		   && (op_type = pt_converse_op (op_type)) != 0
		   && pt_is_attr (arg2))
	    {

	      if (pt_is_attr (arg1))
		{
		  /* term in the form of 'attr op attr' */

		  arg1_cnt = arg2_cnt = 0;	/* init */
		  for (attr = attr_list; attr; attr = attr->next)
		    {
		      if (pt_name_equal (parser, attr, arg1))
			{
			  arg1_cnt = attr->line_number;
			}
		      else if (pt_name_equal (parser, attr, arg2))
			{
			  arg2_cnt = attr->line_number;
			}

		      if (arg1_cnt && arg2_cnt)
			{
			  break;	/* already found both arg1, arg2 */
			}
		    }

		  if (!arg1_cnt || !arg2_cnt)
		    {
		      /* something wrong; skip and go ahead */
		      continue;
		    }

		  /* swap */
		  if (arg1_cnt < arg2_cnt)
		    {
		      /* check if arg1 and/or arg2 have PRIOR above them.
		         If so, swap the arg with the prior also */
		      if (arg1_prior_father)
			{
			  arg1 = arg1_prior_father;
			}
		      if (arg2_prior_father)
			{
			  arg2 = arg2_prior_father;
			}

		      dnf_node->info.expr.arg1 = arg2;
		      dnf_node->info.expr.arg2 = arg1;
		      dnf_node->info.expr.op = op_type;

		      /* change back arg1 and arg2 */
		      if (arg1_prior_father)
			{
			  arg1 = arg1_prior_father->info.expr.arg1;
			}
		      if (arg2_prior_father)
			{
			  arg2 = arg2_prior_father->info.expr.arg1;
			}
		    }
		}
	      else
		{
		  /* term in the form of 'non-attr op attr' */

		  /* swap */

		  /* check if arg1 and/or arg2 have PRIOR above them.
		     If so, swap the arg with the prior also */
		  if (arg1_prior_father)
		    {
		      arg1 = arg1_prior_father;
		    }
		  if (arg2_prior_father)
		    {
		      arg2 = arg2_prior_father;
		    }

		  dnf_node->info.expr.arg1 = arg2;
		  dnf_node->info.expr.arg2 = arg1;
		  dnf_node->info.expr.op = op_type;

		  /* change back arg1 and arg2 */
		  if (arg1_prior_father)
		    {
		      arg1 = arg1_prior_father->info.expr.arg1;
		    }
		  if (arg2_prior_father)
		    {
		      arg2 = arg2_prior_father->info.expr.arg1;
		    }
		}
	    }
	}

      if (attr_list)
	{
	  parser_free_tree (parser, attr_list);
	  attr_list = NULL;
	}
    }
}

/*
 * qo_fold_is_and_not_null () - Make IS NOT NULL node that is always true as 1
 *				 and make IS NULL node that is always false as 0
 *   return:
 *   parser(in):
 *   wherep(in): pointer to WHERE list
 */
static void
qo_fold_is_and_not_null (PARSER_CONTEXT * parser, PT_NODE ** wherep)
{
  PT_NODE *node, *sibling, *prev, *fold;
  DB_VALUE value;
  bool found;
  PT_NODE *node_prior, *sibling_prior;

  /* traverse CNF list and keep track of the pointer to previous node */
  prev = NULL;
  while ((node = (prev ? prev->next : *wherep)))
    {
      if (node->node_type != PT_EXPR
	  || (node->info.expr.op != PT_IS_NULL
	      && node->info.expr.op != PT_IS_NOT_NULL)
	  || node->or_next != NULL)
	{
	  /* niether expression node, IS NULL/IS NOT NULL node nor one predicate term */
	  prev = prev ? prev->next : node;
	  continue;
	}

      node_prior = NULL;
      if (PT_IS_PRIOR_NODE (node))
	{
	  node_prior = node->info.expr.arg1->info.expr.arg1;
	}
      else
	{
	  node_prior = node->info.expr.arg1;
	}
      if (!pt_is_attr (node_prior))
	{
	  /* LHS is not an attribute */
	  prev = prev ? prev->next : node;
	  continue;
	}

      /* search if there's a term that make this IS NULL/IS NOT NULL node
         meaningless; that is, a term that has the same attribute */
      found = false;
      for (sibling = *wherep; sibling; sibling = sibling->next)
	{
	  if (sibling == node
	      || sibling->node_type != PT_EXPR || sibling->or_next != NULL)
	    {
	      continue;
	    }

	  if (sibling->info.expr.location != node->info.expr.location)
	    {
	      continue;
	    }

	  sibling_prior = NULL;
	  if (PT_IS_PRIOR_NODE (sibling))
	    {
	      sibling_prior = sibling->info.expr.arg1->info.expr.arg1;
	    }
	  else
	    {
	      sibling_prior = sibling->info.expr.arg1;
	    }

	  /* just one node from node and sibling contains the PRIOR ->
	     do nothing, they are not comparable */
	  if ((PT_IS_PRIOR_NODE (node) && !PT_IS_PRIOR_NODE (sibling))
	      || (!PT_IS_PRIOR_NODE (node) && PT_IS_PRIOR_NODE (sibling)))
	    {
	      continue;
	    }

	  if (pt_check_path_eq (parser, node_prior,
				sibling_prior) == 0
	      || pt_check_path_eq (parser, node_prior,
				   sibling->info.expr.arg2) == 0)
	    {
	      found = true;
	      break;
	    }
	}

      if (found)
	{
	  int truefalse;

	  if (sibling->info.expr.op == PT_IS_NULL
	      || sibling->info.expr.op == PT_IS_NOT_NULL)
	    {
	      /* a IS NULL(IS NOT NULL) AND a IS NULL(IS NOT NULL) case */
	      truefalse = (node->info.expr.op == sibling->info.expr.op);
	    }
	  else
	    {
	      /* a IS NULL(IS NOT NULL) AND a < 10 case */
	      truefalse = (node->info.expr.op == PT_IS_NOT_NULL);
	    }
	  DB_MAKE_INTEGER (&value, truefalse);
	  fold = pt_dbval_to_value (parser, &value);
	  if (fold == NULL)
	    {
	      return;
	    }

	  fold->type_enum = node->type_enum;
	  fold->info.value.location = node->info.expr.location;
	  pr_clear_value (&value);
	  /* replace IS NULL/IS NOT NULL node with newly created VALUE node */
	  if (prev)
	    {
	      prev->next = fold;
	    }
	  else
	    {
	      *wherep = fold;
	    }
	  fold->next = node->next;
	  node->next = NULL;
	  /* node->or_next == NULL */
	  parser_free_tree (parser, node);
	  node = fold->next;
	}

      prev = prev ? prev->next : node;
    }
}

/*
 * qo_search_comp_pair_term () -
 *   return:
 *   parser(in):
 *   start(in):
 */
static PT_NODE *
qo_search_comp_pair_term (PARSER_CONTEXT * parser, PT_NODE * start)
{
  PT_NODE *node, *arg2;
  PT_OP_TYPE op_type1, op_type2;
  int find_const, find_attr;
  int is_prior = 0;
  PT_NODE *arg_prior, *arg_prior_start;

  arg_prior = arg_prior_start = NULL;

  switch (start->info.expr.op)
    {
    case PT_GE:
    case PT_GT:
      op_type1 = PT_LE;
      op_type2 = PT_LT;
      break;
    case PT_LE:
    case PT_LT:
      op_type1 = PT_GE;
      op_type2 = PT_GT;
      break;
    default:
      return NULL;
    }
  /* skip out unary minus expr */
  arg2 = start->info.expr.arg2;
  while (pt_is_expr_node (arg2) && arg2->info.expr.op == PT_UNARY_MINUS)
    {
      arg2 = arg2->info.expr.arg1;
    }
  find_const = pt_is_const_expr_node (arg2);
  find_attr = pt_is_attr (start->info.expr.arg2);

  arg_prior_start = start->info.expr.arg1;	/* original value */
  if (arg_prior_start->info.expr.op == PT_PRIOR)
    {
      arg_prior_start = arg_prior_start->info.expr.arg1;
    }

  /* search CNF list */
  for (node = start; node; node = node->next)
    {
      if (node->node_type != PT_EXPR || node->or_next != NULL)
	{
	  /* neither expression node nor one predicate term */
	  continue;
	}

      if (node->info.expr.location != start->info.expr.location)
	{
	  continue;
	}

      is_prior = PT_IS_PRIOR_NODE (node) ? 1 : 0;
      if (is_prior)
	{
	  arg_prior = node->info.expr.arg1->info.expr.arg1;
	}
      else
	{
	  arg_prior = node->info.expr.arg1;
	}

      if (node->info.expr.op == op_type1 || node->info.expr.op == op_type2)
	{
	  if (find_const
	      && pt_is_attr (arg_prior)
	      && (pt_check_path_eq (parser, arg_prior_start, arg_prior) == 0))
	    {
	      /* skip out unary minus expr */
	      arg2 = node->info.expr.arg2;
	      while (pt_is_expr_node (arg2)
		     && arg2->info.expr.op == PT_UNARY_MINUS)
		{
		  arg2 = arg2->info.expr.arg1;
		}
	      if (pt_is_const_expr_node (arg2))
		{
		  /* found 'attr op const' term */
		  break;
		}
	    }
	  if (find_attr
	      && pt_is_attr (arg_prior)
	      && pt_is_attr (node->info.expr.arg2)
	      && (pt_check_path_eq (parser, arg_prior_start,
				    node->info.expr.arg1) == 0)
	      && (pt_check_class_eq (parser, start->info.expr.arg2,
				     node->info.expr.arg2) == 0))
	    {
	      /* found 'attr op attr' term */
	      break;
	    }
	}
    }

  return node;
}

/*
 * qo_reduce_comp_pair_terms () - Convert a pair of comparison terms to one
 *			       BETWEEN term
 *   return:
 *   parser(in):
 *   wherep(in): pointer to WHERE
 *
 * Note:
 * 	examples:
 *  	1) where a<=20 and a=>10        -->  where a between 10 and(ge_le) 20
 *  	2) where a<20 and a>10          -->  where a between 10 gt_lt 20
 *  	3) where a<B.b and a>=B.c       -->  where a between B.c ge_lt B.b
 */
static void
qo_reduce_comp_pair_terms (PARSER_CONTEXT * parser, PT_NODE ** wherep)
{
  PT_NODE *node, *pair, *lower, *upper, *prev, *next, *arg2;
  int location;
  DB_VALUE *lower_val, *upper_val;
  DB_VALUE_COMPARE_RESULT cmp;

  /* traverse CNF list */
  for (node = *wherep; node; node = node->next)
    {
      if (node->node_type != PT_EXPR
	  || (!pt_is_attr (node->info.expr.arg1)
	      && (!PT_IS_PRIOR_NODE (node)
		  || !pt_is_attr (node->info.expr.arg1->info.expr.arg1)))
	  || node->or_next != NULL)
	{
	  /* neither expression node, LHS is attribute, nor one predicate
	     term */
	  continue;
	}

      switch (node->info.expr.op)
	{
	case PT_GT:
	case PT_GE:
	  lower = node;
	  upper = pair = qo_search_comp_pair_term (parser, node);
	  break;
	case PT_LT:
	case PT_LE:
	  lower = pair = qo_search_comp_pair_term (parser, node);
	  upper = node;
	  break;
	default:
	  /* not comparison term; continue to next node */
	  continue;
	}
      if (!pair)
	{
	  /* there's no pair comparison term having the same attribute */
	  continue;
	}

      if ((PT_IS_PRIOR_NODE (lower) && !PT_IS_PRIOR_NODE (upper))
	  || (!PT_IS_PRIOR_NODE (lower) && PT_IS_PRIOR_NODE (upper)))
	{
	  /* one of the bounds does not contain prior */
	  continue;
	}

      /* the node will be converted to BETWEEN node and the pair node will be
         converted to the right operand(arg2) of BETWEEN node denoting the
         range of BETWEEN such as BETWEEN_GE_LE, BETWEEN_GE_LT, BETWEEN_GT_LE,
         and BETWEEN_GT_LT */

      /* make the pair node to the right operand of BETWEEN node */
      if (pt_comp_to_between_op (lower->info.expr.op, upper->info.expr.op,
				 PT_REDUCE_COMP_PAIR_TERMS,
				 &pair->info.expr.op) != 0)
	{
	  /* cannot be occurred but something wrong */
	  continue;
	}
      parser_free_tree (parser, pair->info.expr.arg1);
      pair->info.expr.arg1 = lower->info.expr.arg2;
      pair->info.expr.arg2 = upper->info.expr.arg2;
      /* should set pair->info.expr.arg1 before pair->info.expr.arg2 */
      /* make the node to BETWEEN node */
      node->info.expr.op = PT_BETWEEN;
      /* revert BETWEEN_GE_LE to BETWEEN_AND */
      if (pair->info.expr.op == PT_BETWEEN_GE_LE)
	{
	  pair->info.expr.op = PT_BETWEEN_AND;
	}
      node->info.expr.arg2 = pair;

      /* adjust linked list */
      for (prev = node; prev->next != pair; prev = prev->next)
	;
      prev->next = pair->next;
      pair->next = NULL;

      /* check if the between range is valid */
      arg2 = node->info.expr.arg2;

      lower = arg2->info.expr.arg1;
      upper = arg2->info.expr.arg2;
      if (pt_is_const_not_hostvar (lower) && pt_is_const_not_hostvar (upper))
	{
	  lower_val = pt_value_to_db (parser, lower);
	  upper_val = pt_value_to_db (parser, upper);
	  cmp = (DB_VALUE_COMPARE_RESULT) db_value_compare (lower_val,
							    upper_val);
	  if (cmp == DB_GT
	      || (cmp == DB_EQ
		  && (arg2->info.expr.op == PT_BETWEEN_GE_LT
		      || arg2->info.expr.op == PT_BETWEEN_GT_LE
		      || arg2->info.expr.op == PT_BETWEEN_GT_LT)))
	    {
	      /* lower bound is greater than upper bound */

	      location = node->info.expr.location;	/* save location */

	      if (location == 0)
		{
		  /* empty conjuctive make whole condition always false */
		  /* NOTICE: that is valid only when we handle one predicate
		     terms in this function */
		  parser_free_tree (parser, *wherep);

		  /* make a single false node */
		  node = parser_new_node (parser, PT_VALUE);
		  if (node == NULL)
		    {
		      PT_INTERNAL_ERROR (parser, "allocate new node");
		      return;
		    }

		  node->type_enum = PT_TYPE_LOGICAL;
		  node->info.value.data_value.i = 0;
		  node->info.value.location = location;
		  (void) pt_value_to_db (parser, node);
		  *wherep = node;
		}
	      else
		{
		  /* empty conjunctive is outer join ON condition.
		     remove all nodes which have same location number */
		  prev = NULL;
		  node = *wherep;
		  while (node)
		    {
		      if ((node->node_type == PT_EXPR
			   && node->info.expr.location == location)
			  || (node->node_type == PT_VALUE
			      && node->info.value.location == location))
			{
			  next = node->next;
			  node->next = NULL;
			  parser_free_tree (parser, node);
			  if (prev)
			    {
			      prev->next = next;
			    }
			  else
			    {
			      *wherep = next;
			    }
			  node = next;
			}
		      else
			{
			  prev = node;
			  node = node->next;
			}
		    }

		  /* make a single false node and append it to WHERE list */
		  node = parser_new_node (parser, PT_VALUE);
		  if (node == NULL)
		    {
		      PT_INTERNAL_ERROR (parser, "allocate new node");
		      return;
		    }

		  node->type_enum = PT_TYPE_LOGICAL;
		  node->info.value.data_value.i = 0;
		  node->info.value.location = location;
		  (void) pt_value_to_db (parser, node);
		  node->next = *wherep;
		  *wherep = node;
		}

	      return;
	    }
	}
    }
}

/*
 * qo_compress_wildcards_in_like_pattern () -
 *   return:
 *   parser(in):
 *   pattern(in):
 */
static PARSER_VARCHAR *
qo_compress_wildcards_in_like_pattern (PARSER_CONTEXT * parser,
				       PARSER_VARCHAR * pattern)
{
  PARSER_VARCHAR *new_pattern;
  char *p, *q;

  if (!pattern || !strstr ((const char *) pattern->bytes, "%%"))
    return pattern;

  new_pattern = pt_append_varchar (parser, NULL, pattern);

  for (p = (char *) pattern->bytes, q = (char *) new_pattern->bytes;
       *p; p++, q++)
    {
      *q = *p;
      if (*p == '%')
	{
	  while (*(p + 1) && *(p + 1) == '%')
	    p++;
	}
    }
  *q = '\0';

  new_pattern->length = strlen ((const char *) new_pattern->bytes);

  return new_pattern;
}

/*
 * qo_rewrite_like_terms () - Convert a leftmost LIKE term to a BETWEEN
 *			   (GE_LT) term o increase chance to use index
 *   return:
 *   parser(in):
 *   wherep(in): CNF list of WHERE clause
 *
 * Note:
 * 	examples:
 *   	where s like 'abc%'   --->  where s between 'abc' ge_lt 'abd'
 */
static void
qo_rewrite_like_terms (PARSER_CONTEXT * parser, PT_NODE ** wherep)
{
  PT_NODE *cnf_node, *dnf_node, *arg2, *between_and, *lower, *upper;
  bool found_unbound;
  PARSER_VARCHAR *str, *new_str;
  int i, j;
  PT_NODE *arg1_prior;

  /* traverse CNF list */
  for (cnf_node = *wherep; cnf_node; cnf_node = cnf_node->next)
    {
      /* init */
      found_unbound = false;

      /* traverse DNF list */
      for (dnf_node = cnf_node; dnf_node; dnf_node = dnf_node->or_next)
	{
	  if (dnf_node->node_type != PT_EXPR
	      || dnf_node->info.expr.op != PT_LIKE)
	    {
	      /* neither expression node nor LIKE pred */
	      continue;
	    }

	  arg1_prior = NULL;
	  if (PT_IS_PRIOR_NODE (dnf_node))
	    {
	      arg1_prior = dnf_node->info.expr.arg1->info.expr.arg1;
	    }
	  else
	    {
	      arg1_prior = dnf_node->info.expr.arg1;
	    }
	  if (!pt_is_attr (arg1_prior))
	    {
	      /* LHS is not an attribute */
	      continue;
	    }

	  arg2 = dnf_node->info.expr.arg2;
	  if (arg2->node_type == PT_VALUE
	      && PT_IS_CHAR_STRING_TYPE (arg2->type_enum)
	      && arg2->info.value.string_type == ' ')
	    {
	      /* it is LIKE char_literal predicate */

	      arg2->info.value.data_value.str =
		qo_compress_wildcards_in_like_pattern (parser,
						       arg2->info.value.
						       data_value.str);
	      arg2->info.value.text =
		(char *) arg2->info.value.data_value.str->bytes;
	      arg2->info.value.db_value_is_initialized = false;

	      str = arg2->info.value.data_value.str;

	      if (str && strchr ((const char *) str->bytes, '_') == NULL)
		{
		  if (strchr ((const char *) str->bytes, '%') == NULL)
		    {
		      /* it is LIKE 'abc' predicate */


		      if (str->bytes[str->length - 1] == ' ')
			{
			  /* if right-most charater in pattern is blank,
			   * can not rewrite this term; skip and go ahead */

			  /*
			   * NEED MORE CONSIDERATION
			   */
			}
		      else
			{
			  /* rewrite this term as equal predicate,
			   * to permit to cheat the optimizer. */
			  dnf_node->info.expr.op = PT_EQ;
			}

		      continue;
		    }
		  for (i = 0; i < str->length; i++)
		    {
		      if (str->bytes[i] == '%')
			{
			  /* found the first mark */
			  break;
			}
		    }		/* for */
		  for (j = i + 1; j < str->length; j++)
		    {
		      if (str->bytes[j] != '%')
			{
			  /* found another character */
			  break;
			}
		    }		/* for */
		  if (str->length == 1 && str->bytes[0] == '%')
		    {
		      /* it is LIKE '%' predicate */
		      found_unbound = true;

		      /* exit inner dnf_node traverse loop */
		      break;
		    }
		  else if (i > 0 && i < str->length && j == str->length)
		    {
		      /* it is leftmost LIKE 'abc%' predicate */

		      /* lower value */
		      lower = parser_new_node (parser, PT_VALUE);
		      if (lower == NULL)
			{
			  PT_INTERNAL_ERROR (parser, "allocate new node");
			  return;
			}

		      lower->type_enum = arg2->type_enum;
		      new_str = pt_append_varchar (parser, NULL, str);
		      new_str->length = i;
		      new_str->bytes[new_str->length] = '\0';
		      lower->info.value.data_value.str = new_str;
		      lower->info.value.text =
			(char *) lower->info.value.data_value.str->bytes;
		      (void) pt_value_to_db (parser, lower);

		      /* upper value */
		      upper = parser_new_node (parser, PT_VALUE);
		      if (upper == NULL)
			{
			  PT_INTERNAL_ERROR (parser, "allocate new node");
			  return;
			}

		      upper->type_enum = arg2->type_enum;
		      new_str = pt_append_varchar (parser, NULL, str);
		      new_str->length = i;
		      new_str->bytes[new_str->length] = '\0';
		      new_str->bytes[new_str->length - 1]++;
		      upper->info.value.data_value.str = new_str;
		      upper->info.value.text =
			(char *) upper->info.value.data_value.str->bytes;
		      (void) pt_value_to_db (parser, upper);

		      /* BETWEEN_GE_LT node */
		      between_and = parser_new_node (parser, PT_EXPR);
		      if (between_and == NULL)
			{
			  PT_INTERNAL_ERROR (parser, "allocate new node");
			  return;
			}

		      between_and->type_enum = PT_TYPE_LOGICAL;
		      between_and->info.expr.op = PT_BETWEEN_GE_LT;
		      between_and->info.expr.arg1 = lower;
		      between_and->info.expr.arg2 = upper;
		      between_and->info.expr.location =
			dnf_node->info.expr.location;

		      /* BETWEEN node */
		      dnf_node->info.expr.op = PT_BETWEEN;
		      dnf_node->info.expr.arg2 = between_and;

		      parser_free_tree (parser, arg2);
		    }
		}
	    }
	}

      if (found_unbound == true)
	{
	  /* change unbound LIKE '%' node to IS NOT NULL node */
	  parser_free_tree (parser, cnf_node->info.expr.arg2);
	  cnf_node->info.expr.arg2 = NULL;
	  cnf_node->info.expr.op = PT_IS_NOT_NULL;
	}
    }
}

/*
 * qo_set_value_to_range_list () -
 *   return:
 *   parser(in):
 *   node(in):
 */
static PT_NODE *
qo_set_value_to_range_list (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *set_val, *list, *last, *range;

  list = last = NULL;
  if (node->node_type == PT_VALUE)
    {
      set_val = node->info.value.data_value.set;
    }
  else if (node->node_type == PT_FUNCTION)
    {
      set_val = node->info.function.arg_list;
    }
  else if (node->node_type == PT_NAME
	   && !PT_IS_COLLECTION_TYPE (node->type_enum))
    {
      set_val = node;
    }
  else
    {
      set_val = NULL;
    }

  while (set_val)
    {
      range = parser_new_node (parser, PT_EXPR);
      if (!range)
	goto error;
      range->type_enum = PT_TYPE_LOGICAL;
      range->info.expr.op = PT_BETWEEN_EQ_NA;
      range->info.expr.arg1 = parser_copy_tree (parser, set_val);
      range->info.expr.arg2 = NULL;
      range->info.expr.location = set_val->info.expr.location;
#if defined(CUBRID_DEBUG)
      range->next = NULL;
      range->or_next = NULL;
#endif /* CUBRID_DEBUG */
      if (last)
	{
	  last->or_next = range;
	}
      else
	{
	  list = range;
	}
      last = range;
      set_val = set_val->next;
    }

  return list;

error:
  if (list)
    parser_free_tree (parser, list);
  return NULL;
}


/*
 * qo_convert_to_range_helper () -
 *   return:
 *   parser(in):
 *   node(in):
 */
static void
qo_convert_to_range_helper (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *between_and, *sibling, *last, *prev, *in_arg2;
  PT_OP_TYPE op_type;
  PT_NODE *node_prior, *sibling_prior;

  assert (PT_IS_EXPR_NODE (node));
  node_prior = sibling_prior = NULL;
  if (PT_IS_PRIOR_NODE (node))
    {
      node_prior = node->info.expr.arg1->info.expr.arg1;
    }
  else
    {
      node_prior = node->info.expr.arg1;
    }

  /* convert the given node to RANGE node */

  /* construct BETWEEN_AND node as arg2(RHS) of RANGE node */
  op_type = node->info.expr.op;
  switch (op_type)
    {
    case PT_EQ:
      between_and = parser_new_node (parser, PT_EXPR);
      if (!between_and)
	{
	  return;		/* error; stop converting */
	}
      between_and->type_enum = PT_TYPE_LOGICAL;
      between_and->info.expr.op = PT_BETWEEN_EQ_NA;
      between_and->info.expr.arg1 = node->info.expr.arg2;
      between_and->info.expr.arg2 = NULL;
      between_and->info.expr.location = node->info.expr.location;
#if defined(CUBRID_DEBUG)
      between_and->next = NULL;
      between_and->or_next = NULL;
#endif /* CUBRID_DEBUG */
      break;
    case PT_GT:
    case PT_GE:
    case PT_LT:
    case PT_LE:
      between_and = parser_new_node (parser, PT_EXPR);
      if (!between_and)
	{
	  return;		/* error; stop converting */
	}
      between_and->type_enum = PT_TYPE_LOGICAL;
      between_and->info.expr.op = (op_type == PT_GT ? PT_BETWEEN_GT_INF :
				   (op_type == PT_GE ? PT_BETWEEN_GE_INF :
				    (op_type == PT_LT ? PT_BETWEEN_INF_LT :
				     PT_BETWEEN_INF_LE)));
      between_and->info.expr.arg1 = node->info.expr.arg2;
      between_and->info.expr.arg2 = NULL;
      between_and->info.expr.location = node->info.expr.location;
#if defined(CUBRID_DEBUG)
      between_and->next = NULL;
      between_and->or_next = NULL;
#endif
      break;
    case PT_BETWEEN:
      between_and = node->info.expr.arg2;
      /* replace PT_BETWEEN_AND with PT_BETWEEN_GE_LE */
      if (between_and->info.expr.op == PT_BETWEEN_AND)
	{
	  between_and->info.expr.op = PT_BETWEEN_GE_LE;
	}
      break;
    case PT_IS_IN:
      in_arg2 = node->info.expr.arg2;
      if (PT_IS_COLLECTION_TYPE (node->type_enum)
	  || PT_IS_QUERY_NODE_TYPE (in_arg2->node_type)
	  || !PT_IS_COLLECTION_TYPE (in_arg2->type_enum))
	/* subquery cannot be converted to RANGE */
	return;
      between_and = qo_set_value_to_range_list (parser, in_arg2);
      if (!between_and)
	{
	  return;		/* error; stop converting */
	}
      /* free the converted set value node, which is the operand of IN */
      parser_free_tree (parser, in_arg2);
      break;
    case PT_RANGE:
      /* already converted. do nothing */
      return;
    default:
      /* unsupported operator; only PT_EQ, PT_GT, PT_GE, PT_LT, PT_LE, and
         PT_BETWEEN can be converted to RANGE */
      return;			/* error; stop converting */
    }
#if 0
  between_and->next = between_and->or_next = NULL;
#endif
  /* change the node to RANGE */
  node->info.expr.op = PT_RANGE;
  node->info.expr.arg2 = last = between_and;
  while (last->or_next)
    {
      last = last->or_next;
    }


  /* link all nodes in the list whose LHS is the same attribute with the
     RANGE node */

  /* search DNF list from the next to the node and keep track of the pointer
     to previous node */
  prev = node;
  while ((sibling = prev->or_next))
    {
      if (sibling->node_type != PT_EXPR)
	{
	  /* sibling is not an expression node */
	  prev = prev->or_next;
	  continue;
	}

      sibling_prior = NULL;
      if (PT_IS_PRIOR_NODE (sibling))
	{
	  sibling_prior = sibling->info.expr.arg1->info.expr.arg1;
	  if (!PT_IS_PRIOR_NODE (node))
	    {
	      /* sibling has prior, node hasn't */
	      prev = prev->or_next;
	      continue;
	    }
	}
      else
	{
	  sibling_prior = sibling->info.expr.arg1;
	  if (PT_IS_PRIOR_NODE (node))
	    {
	      /* sibling hasn't prior, node has */
	      prev = prev->or_next;
	      continue;
	    }
	}
      /* if node had prior check that sibling also contains prior and
         vice-versa */

      if (!pt_is_attr (sibling_prior) && !pt_is_instnum (sibling_prior))
	{
	  /* LHS is not an attribute */
	  prev = prev->or_next;
	  continue;
	}

      if ((node_prior->node_type != sibling_prior->node_type)
	  || (pt_is_attr (node_prior)
	      && pt_is_attr (sibling_prior)
	      && pt_check_path_eq (parser, node_prior, sibling_prior)))
	{
	  /* pt_check_path_eq() return non-zero if two are different */
	  prev = prev->or_next;
	  continue;
	}

      /* found a node of the same attribute */

      /* construct BETWEEN_AND node as the tail of RANGE node's range list */
      op_type = sibling->info.expr.op;
      switch (op_type)
	{
	case PT_EQ:
	  between_and = parser_new_node (parser, PT_EXPR);
	  if (!between_and)
	    {
	      return;		/* error; stop converting */
	    }
	  between_and->type_enum = PT_TYPE_LOGICAL;
	  between_and->info.expr.op = PT_BETWEEN_EQ_NA;
	  between_and->info.expr.arg1 = sibling->info.expr.arg2;
	  between_and->info.expr.arg2 = NULL;
	  between_and->info.expr.location = sibling->info.expr.location;
#if defined(CUBRID_DEBUG)
	  between_and->next = NULL;
	  between_and->or_next = NULL;
#endif /* CUBRID_DEBUG */
	  break;
	case PT_GT:
	case PT_GE:
	case PT_LT:
	case PT_LE:
	  between_and = parser_new_node (parser, PT_EXPR);
	  if (!between_and)
	    {
	      return;		/* error; stop converting */
	    }
	  between_and->type_enum = PT_TYPE_LOGICAL;
	  between_and->info.expr.op = (op_type == PT_GT ? PT_BETWEEN_GT_INF :
				       (op_type == PT_GE ? PT_BETWEEN_GE_INF :
					(op_type ==
					 PT_LT ? PT_BETWEEN_INF_LT :
					 PT_BETWEEN_INF_LE)));
	  between_and->info.expr.arg1 = sibling->info.expr.arg2;
	  between_and->info.expr.arg2 = NULL;
	  between_and->info.expr.location = sibling->info.expr.location;
#if defined(CUBRID_DEBUG)
	  between_and->next = NULL;
	  between_and->or_next = NULL;
#endif
	  break;
	case PT_BETWEEN:
	  between_and = sibling->info.expr.arg2;
	  /* replace PT_BETWEEN_AND with PT_BETWEEN_GE_LE */
	  if (between_and->info.expr.op == PT_BETWEEN_AND)
	    {
	      between_and->info.expr.op = PT_BETWEEN_GE_LE;
	    }
	  break;
	case PT_IS_IN:
	  in_arg2 = sibling->info.expr.arg2;
	  if (PT_IS_COLLECTION_TYPE (sibling->type_enum)
	      || PT_IS_QUERY_NODE_TYPE (in_arg2->node_type)
	      || !PT_IS_COLLECTION_TYPE (in_arg2->type_enum))
	    {
	      /* subquery cannot be converted to RANGE */
	      prev = prev->or_next;
	      continue;
	    }
	  between_and = qo_set_value_to_range_list (parser, in_arg2);
	  if (!between_and)
	    {
	      prev = prev->or_next;
	      continue;
	    }
	  /* free the converted set value node, which is the operand of IN */
	  parser_free_tree (parser, in_arg2);
	  break;
	default:
	  /* unsupported operator; continue to next node */
	  prev = prev->or_next;
	  continue;
	}			/* switch (op_type) */
#if 0
      between_and->next = between_and->or_next = NULL;
#endif
      /* append to the range list */
      last->or_next = between_and;
      last = between_and;
      while (last->or_next)
	{
	  last = last->or_next;
	}

      /* delete the node and its arg1(LHS), and adjust linked list */
      prev->or_next = sibling->or_next;
      sibling->next = sibling->or_next = NULL;
      sibling->info.expr.arg2 = NULL;	/* parser_free_tree() will handle 'arg1' */
      parser_free_tree (parser, sibling);
    }
}

/*
 * qo_compare_dbvalue_with_optype () - compare two DB_VALUEs specified
 *					by range operator
 *   return:
 *   val1(in):
 *   op1(in):
 *   val2(in):
 *   op2(in):
 */
static COMP_DBVALUE_WITH_OPTYPE_RESULT
qo_compare_dbvalue_with_optype (DB_VALUE * val1, PT_OP_TYPE op1,
				DB_VALUE * val2, PT_OP_TYPE op2)
{
  DB_VALUE_COMPARE_RESULT rc;

  switch (op1)
    {
    case PT_EQ:
    case PT_GE:
    case PT_GT:
    case PT_LT:
    case PT_LE:
    case PT_GT_INF:
    case PT_LT_INF:
      break;
    default:
      return CompResultError;
    }
  switch (op2)
    {
    case PT_EQ:
    case PT_GE:
    case PT_GT:
    case PT_LT:
    case PT_LE:
    case PT_GT_INF:
    case PT_LT_INF:
      break;
    default:
      return CompResultError;
    }

  if (op1 == PT_GT_INF)		/* val1 is -INF */
    return (op1 == op2) ? CompResultEqual : CompResultLess;
  if (op1 == PT_LT_INF)		/* val1 is +INF */
    return (op1 == op2) ? CompResultEqual : CompResultGreater;
  if (op2 == PT_GT_INF)		/* val2 is -INF */
    return (op2 == op1) ? CompResultEqual : CompResultGreater;
  if (op2 == PT_LT_INF)		/* va2 is +INF */
    return (op2 == op1) ? CompResultEqual : CompResultLess;

  rc = (DB_VALUE_COMPARE_RESULT) tp_value_compare (val1, val2, 1, 1);
  if (rc == DB_EQ)
    {
      /* (val1, op1) == (val2, op2) */
      if (op1 == op2)
	return CompResultEqual;
      if (op1 == PT_EQ || op1 == PT_GE || op1 == PT_LE)
	{
	  if (op2 == PT_EQ || op2 == PT_GE || op2 == PT_LE)
	    return CompResultEqual;
	  return (op2 == PT_GT) ? CompResultLessAdj : CompResultGreaterAdj;
	}
      if (op1 == PT_GT)
	{
	  if (op2 == PT_EQ || op2 == PT_GE || op2 == PT_LE)
	    return CompResultGreaterAdj;
	  return (op2 == PT_LT) ? CompResultGreater : CompResultEqual;
	}
      if (op1 == PT_LT)
	{
	  if (op2 == PT_EQ || op2 == PT_GE || op2 == PT_LE)
	    return CompResultLessAdj;
	  return (op2 == PT_GT) ? CompResultLess : CompResultEqual;
	}
    }
  else if (rc == DB_LT)
    {
      /* (val1, op1) < (val2, op2) */
      return CompResultLess;
    }
  else if (rc == DB_GT)
    {
      /* (val1, op1) > (val2, op2) */
      return CompResultGreater;
    }

  /* tp_value_compare() returned error? */
  return CompResultError;
}


/*
 * qo_merge_range_helper () -
 *   return:
 *   parser(in):
 *   node(in):
 */
static void
qo_merge_range_helper (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *range, *sibling, *prev;
  PT_OP_TYPE r_op, r_lop, r_uop, s_op, s_lop, s_uop;
  DB_VALUE *r_lv, *r_uv, *s_lv, *s_uv;
  bool r_lv_copied = false, r_uv_copied = false;
  COMP_DBVALUE_WITH_OPTYPE_RESULT cmp1, cmp2, cmp3, cmp4;
  bool need_to_determine_upper_bound;

  if (node->info.expr.arg2->or_next == NULL)
    /* one range spec; nothing to merge */
    return;

  r_lv = r_uv = s_lv = s_uv = NULL;
  prev = NULL;
  /* for each range spec of the RANGE node */
  for (range = node->info.expr.arg2; range; range = range->or_next)
    {

      if (range->info.expr.arg2)
	{
	  if (!pt_is_const_not_hostvar (range->info.expr.arg1) ||
	      !pt_is_const_not_hostvar (range->info.expr.arg2))
	    /* not constant; cannot be merged */
	    continue;
	}
      else
	{
	  if (!pt_is_const_not_hostvar (range->info.expr.arg1))
	    /* not constant; cannot be merged */
	    continue;
	}

      r_op = range->info.expr.op;
      if (pt_between_to_comp_op (r_op, &r_lop, &r_uop) != 0)
	/* something wrong; continue to next range spec */
	continue;

      /* search DNF list from the next to the node and keep track of the
         pointer to previous node */
      prev = range;
      while ((sibling = prev->or_next))
	{

	  if (sibling->info.expr.arg2)
	    {
	      if (!pt_is_const_not_hostvar (sibling->info.expr.arg1) ||
		  !pt_is_const_not_hostvar (sibling->info.expr.arg2))
		{
		  /* not constant; cannot be merged */
		  prev = prev->or_next;
		  continue;
		}
	    }
	  else
	    {
	      if (!pt_is_const_not_hostvar (sibling->info.expr.arg1))
		{
		  /* not constant; cannot be merged */
		  prev = prev->or_next;
		  continue;
		}
	    }

	  s_op = sibling->info.expr.op;
	  if (pt_between_to_comp_op (s_op, &s_lop, &s_uop) != 0)
	    {
	      /* something wrong; continue to next range spec */
	      prev = prev->or_next;
	      continue;
	    }

	  if (r_lop == PT_GT_INF)
	    {
	      /* PT_BETWEEN_INF_LE or PT_BETWEEN_INF_LT */
	      if (r_lv_copied && r_lv)
		{
		  pr_free_value (r_lv);
		  r_lv_copied = false;
		}
	      if (r_uv_copied && r_uv)
		{
		  pr_free_value (r_uv);
		  r_uv_copied = false;
		}
	      r_lv = NULL;
	      r_uv = pt_value_to_db (parser, range->info.expr.arg1);
	    }
	  else if (r_uop == PT_LT_INF)
	    {
	      /* PT_BETWEEN_GE_INF or PT_BETWEEN_GT_INF */
	      if (r_lv_copied && r_lv)
		{
		  pr_free_value (r_lv);
		  r_lv_copied = false;
		}
	      if (r_uv_copied && r_uv)
		{
		  pr_free_value (r_uv);
		  r_uv_copied = false;
		}
	      r_lv = pt_value_to_db (parser, range->info.expr.arg1);
	      r_uv = NULL;
	    }
	  else if (r_lop == PT_EQ)
	    {
	      /* PT_BETWEEN_EQ_NA */
	      if (r_lv_copied && r_lv)
		{
		  pr_free_value (r_lv);
		  r_lv_copied = false;
		}
	      if (r_uv_copied && r_uv)
		{
		  pr_free_value (r_uv);
		  r_uv_copied = false;
		}
	      r_lv = pt_value_to_db (parser, range->info.expr.arg1);
	      r_uv = r_lv;
	    }
	  else
	    {
	      /* PT_BETWEEN_GE_LE, PT_BETWEEN_GE_LT, PT_BETWEEN_GT_LE, or
	         PT_BETWEEN_GT_LT */
	      if (r_lv_copied && r_lv)
		{
		  pr_free_value (r_lv);
		  r_lv_copied = false;
		}
	      if (r_uv_copied && r_uv)
		{
		  pr_free_value (r_uv);
		  r_uv_copied = false;
		}
	      r_lv = pt_value_to_db (parser, range->info.expr.arg1);
	      r_uv = pt_value_to_db (parser, range->info.expr.arg2);
	    }

	  if (s_lop == PT_GT_INF)
	    {
	      /* PT_BETWEEN_INF_LE or PT_BETWEEN_INF_LT */
	      s_lv = NULL;
	      s_uv = pt_value_to_db (parser, sibling->info.expr.arg1);
	    }
	  else if (s_uop == PT_LT_INF)
	    {
	      /* PT_BETWEEN_GE_INF or PT_BETWEEN_GT_INF */
	      s_lv = pt_value_to_db (parser, sibling->info.expr.arg1);
	      s_uv = NULL;
	    }
	  else if (s_lop == PT_EQ)
	    {
	      /* PT_BETWEEN_EQ_NA */
	      s_lv = pt_value_to_db (parser, sibling->info.expr.arg1);
	      s_uv = s_lv;
	    }
	  else
	    {
	      /* PT_BETWEEN_GE_LE, PT_BETWEEN_GE_LT, PT_BETWEEN_GT_LE, or
	         PT_BETWEEN_GT_LT */
	      s_lv = pt_value_to_db (parser, sibling->info.expr.arg1);
	      s_uv = pt_value_to_db (parser, sibling->info.expr.arg2);
	    }

	  PT_EXPR_INFO_CLEAR_FLAG (node, PT_EXPR_INFO_EMPTY_RANGE);
	  /* check if the two range specs are mergable */
	  cmp1 = qo_compare_dbvalue_with_optype (r_lv, r_lop, s_lv, s_lop);
	  cmp2 = qo_compare_dbvalue_with_optype (r_lv, r_lop, s_uv, s_uop);
	  cmp3 = qo_compare_dbvalue_with_optype (r_uv, r_uop, s_lv, s_lop);
	  cmp4 = qo_compare_dbvalue_with_optype (r_uv, r_uop, s_uv, s_uop);
	  if (cmp1 == CompResultError || cmp2 == CompResultError ||
	      cmp3 == CompResultError || cmp4 == CompResultError)
	    {
	      /* somthine wrong; continue to next range spec */
	      prev = prev->or_next;
	      continue;
	    }
	  if ((cmp1 == CompResultLess || cmp1 == CompResultGreater)
	      && cmp1 == cmp2 && cmp1 == cmp3 && cmp1 == cmp4)
	    {
	      /* they are disjoint; continue to next range spec */
	      prev = prev->or_next;
	      continue;
	    }

	  /* merge the two range specs */
	  /* swap arg1 and arg2 if op type is INF_LT or INF_LE to make easy
	     the following merge algorithm */
	  if (r_op == PT_BETWEEN_INF_LT || r_op == PT_BETWEEN_INF_LE)
	    {
	      range->info.expr.arg2 = range->info.expr.arg1;
	      range->info.expr.arg1 = NULL;
	    }
	  if (s_op == PT_BETWEEN_INF_LT || s_op == PT_BETWEEN_INF_LE)
	    {
	      sibling->info.expr.arg2 = sibling->info.expr.arg1;
	      sibling->info.expr.arg1 = NULL;
	    }
	  /* determine the lower bound of the merged range spec */
	  need_to_determine_upper_bound = true;
	  if (cmp1 == CompResultGreaterAdj || cmp1 == CompResultGreater)
	    {
	      parser_free_tree (parser, range->info.expr.arg1);
	      if (s_op == PT_BETWEEN_EQ_NA)
		{
		  range->info.expr.arg1 = parser_copy_tree (parser,
							    sibling->info.
							    expr.arg1);
		}
	      else
		{
		  range->info.expr.arg1 = sibling->info.expr.arg1;
		}
	      r_lop = s_lop;
	      if (r_lv_copied && r_lv)
		{
		  pr_free_value (r_lv);
		  r_lv_copied = false;
		}
	      if (s_lv)
		{
		  r_lv = pr_copy_value (s_lv);
		  r_lv_copied = true;
		}
	      else
		{
		  r_lv = s_lv;
		}

	      sibling->info.expr.arg1 = NULL;
	      if (r_op == PT_BETWEEN_EQ_NA)
		{		/* PT_BETWEEN_EQ_NA */
		  parser_free_tree (parser, range->info.expr.arg2);
		  if (s_op == PT_BETWEEN_EQ_NA)
		    {
		      range->info.expr.arg2 = parser_copy_tree (parser,
								sibling->info.
								expr.arg1);
		    }
		  else
		    {
		      range->info.expr.arg2 = sibling->info.expr.arg2;
		    }
		  sibling->info.expr.arg2 = NULL;
		  r_uop = PT_LE;
		  need_to_determine_upper_bound = false;
		}

	      if (r_lop == PT_EQ)
		{		/* PT_BETWEEN_EQ_NA */
		  r_lop = PT_GE;
		}
	    }
	  /* determine the upper bound of the merged range spec */
	  if (cmp4 == CompResultLess || cmp4 == CompResultLessAdj)
	    {
	      if (need_to_determine_upper_bound == true)
		{
		  parser_free_tree (parser, range->info.expr.arg2);
		  if (s_op == PT_BETWEEN_EQ_NA)
		    {
		      range->info.expr.arg2 = parser_copy_tree (parser,
								sibling->info.
								expr.arg1);
		    }
		  else
		    {
		      range->info.expr.arg2 = sibling->info.expr.arg2;
		    }
		  sibling->info.expr.arg2 = NULL;
		}
	      r_uop = s_uop;
	      if (r_uv_copied && r_uv)
		{
		  pr_free_value (r_uv);
		  r_uv_copied = false;
		}
	      if (s_uv)
		{
		  r_uv = pr_copy_value (s_uv);
		  r_uv_copied = true;
		}
	      else
		{
		  r_uv = s_uv;
		}

	      if (r_uop == PT_EQ)
		{		/* PT_BETWEEN_EQ_NA */
		  r_uop = PT_LE;
		}
	    }

	  /* determine the new range type */
	  if (pt_comp_to_between_op (r_lop, r_uop, PT_RANGE_MERGE, &r_op) !=
	      0)
	    {
	      /* the merge result is unbound range spec, INF_INF; this means
	         that this RANGE node is always true and meaningless */
	      r_op = (PT_OP_TYPE) 0;
	    }
	  /* check if the range is invalid, that is, lower bound is greater
	     than upper bound */
	  cmp1 = qo_compare_dbvalue_with_optype (r_lv, r_lop, r_uv, r_uop);
	  if (cmp1 == CompResultGreaterAdj || cmp1 == CompResultGreater)
	    {
	      r_op = (PT_OP_TYPE) 0;
	    }
	  else if (cmp1 == CompResultEqual)
	    {
	      if (r_op == PT_BETWEEN_GE_LE)
		{		/* convert to PT_EQ */
		  r_lop = r_uop = PT_EQ;

		  r_op = PT_BETWEEN_EQ_NA;
		  parser_free_tree (parser, range->info.expr.arg2);
		  range->info.expr.arg2 = NULL;
		}
	    }

	  range->info.expr.op = r_op;
	  /* recover arg1 and arg2 for the type of INF_LT and INF_LE */
	  if (r_op == PT_BETWEEN_INF_LT || r_op == PT_BETWEEN_INF_LE)
	    {
	      range->info.expr.arg1 = range->info.expr.arg2;
	      range->info.expr.arg2 = NULL;
	    }
	  /* no need to recover the sibling because it is to be deleted */

	  /* delete the sibling node and adjust linked list */
	  prev->or_next = sibling->or_next;
	  sibling->next = sibling->or_next = NULL;
	  parser_free_tree (parser, sibling);

	  if (r_op == 0)
	    {
	      /* unbound range spec; mark this fact at the RANGE node */
	      PT_EXPR_INFO_SET_FLAG (node, PT_EXPR_INFO_EMPTY_RANGE);
	      return;
	    }

	  /* with merged range,
	     search DNF list from the next to the node and keep track of the
	     pointer to previous node */
	  prev = range;
	}
    }

  if (r_lv_copied && r_lv)
    pr_free_value (r_lv);
  if (r_uv_copied && r_uv)
    pr_free_value (r_uv);

  for (range = node->info.expr.arg2; range; range = range->or_next)
    {
      if (range->info.expr.op == PT_BETWEEN_EQ_NA
	  && range->info.expr.arg2 != NULL)
	{
	  parser_free_tree (parser, range->info.expr.arg2);
	  range->info.expr.arg2 = NULL;
	}
    }
}

/*
 * qo_convert_to_range () - Convert comparison term to RANGE term
 *   return:
 *   parser(in):
 *   wherep(in): pointer to WHERE list
 *
 * Note:
 * 	examples:
 *  	1. WHERE a<=20 AND a=>10   -->  WHERE a RANGE(10 GE_LE 20)
 *  	2. WHERE a<10              -->  WHERE a RANGE(10 INF_LT)
 *  	3. WHERE a>=20             -->  WHERE a RANGE(20 GE_INF)
 *  	4. WHERE a<10 OR a>=20     -->  WHERE a RANGE(10 INF_LT, 20 GE_INF)
 */
static void
qo_convert_to_range (PARSER_CONTEXT * parser, PT_NODE ** wherep)
{
  PT_NODE *cnf_node, *dnf_node, *cnf_prev, *dnf_prev;
  PT_NODE *arg1_prior;

  /* traverse CNF list and keep track of the pointer to previous node */
  cnf_prev = NULL;
  while ((cnf_node = (cnf_prev ? cnf_prev->next : *wherep)))
    {

      /* traverse DNF list and keep track of the pointer to previous node */
      dnf_prev = NULL;
      while ((dnf_node = (dnf_prev ? dnf_prev->or_next : cnf_node)))
	{
	  if (dnf_node->node_type != PT_EXPR)
	    {
	      /* dnf_node is not an expression node */
	      dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
	      continue;
	    }

	  arg1_prior = NULL;
	  if (PT_IS_PRIOR_NODE (dnf_node))
	    {
	      arg1_prior = dnf_node->info.expr.arg1->info.expr.arg1;
	    }
	  else
	    {
	      arg1_prior = dnf_node->info.expr.arg1;
	    }

	  if (!pt_is_attr (arg1_prior) && !pt_is_instnum (arg1_prior))
	    {
	      /* LHS is not an attribute */
	      dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
	      continue;
	    }

	  if (dnf_node == cnf_node && dnf_node->or_next == NULL
	      && dnf_node->info.expr.op == PT_EQ
	      && !pt_is_instnum (arg1_prior))
	    {
	      /* do not convert one predicate '=' term to RANGE */
	      dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
	      continue;
	    }

	  switch (dnf_node->info.expr.op)
	    {
	    case PT_EQ:
	    case PT_GT:
	    case PT_GE:
	    case PT_LT:
	    case PT_LE:
	    case PT_BETWEEN:
	    case PT_IS_IN:
	    case PT_RANGE:
	      /* convert all comparison nodes in the DNF list which have
	         the same attribute as its LHS into one RANGE node
	         containing multi-range spec */
	      qo_convert_to_range_helper (parser, dnf_node);

	      if (dnf_node->info.expr.op == PT_RANGE)
		{
		  /* merge range specs in the RANGE node */
		  (void) qo_merge_range_helper (parser, dnf_node);

		  if (PT_EXPR_INFO_IS_FLAGED (dnf_node,
					      PT_EXPR_INFO_EMPTY_RANGE))
		    {
		      /* change unbound range spec to IS NOT NULL node */
		      parser_free_tree (parser, dnf_node->info.expr.arg2);
		      dnf_node->info.expr.arg2 = NULL;
		      dnf_node->info.expr.op = PT_IS_NOT_NULL;
		    }
		}
	      break;
	    default:
	      break;
	    }
	  dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
	}
      cnf_prev = cnf_prev ? cnf_prev->next : cnf_node;
    }
}

/*
 * qo_apply_range_intersection_helper () -
 *   return:
 *   parser(in):
 *   node1(in):
 *   node2(in):
 */
static void
qo_apply_range_intersection_helper (PARSER_CONTEXT * parser,
				    PT_NODE * node1, PT_NODE * node2)
{
  PT_NODE *range, *sibling, *prev, *new_range, *temp1, *temp2;
  PT_OP_TYPE r_op, r_lop, r_uop, s_op, s_lop, s_uop, new_op, new_lop, new_uop;
  DB_VALUE *r_lv, *r_uv, *s_lv, *s_uv, *new_lv, *new_uv;
  COMP_DBVALUE_WITH_OPTYPE_RESULT cmp1, cmp2, cmp3, cmp4, new_cmp;
  bool dont_remove_sibling = false;

  /* for each range spec of the node1 */
  prev = NULL;
  while ((range = (prev ? prev->or_next : node1->info.expr.arg2)))
    {

      if (range->info.expr.arg2)
	{
	  if (!pt_is_const_not_hostvar (range->info.expr.arg1)
	      || !pt_is_const_not_hostvar (range->info.expr.arg2))
	    {
	      /* not constant; cannot be merged */
	      prev = prev ? prev->or_next : range;
	      dont_remove_sibling = true;
	      continue;
	    }
	}
      else
	{
	  if (!pt_is_const_not_hostvar (range->info.expr.arg1))
	    {
	      /* not constant; cannot be merged */
	      prev = prev ? prev->or_next : range;
	      dont_remove_sibling = true;
	      continue;
	    }
	}

      r_op = range->info.expr.op;
      if (pt_between_to_comp_op (r_op, &r_lop, &r_uop) != 0)
	{
	  /* something wrong; continue to next range spec */
	  prev = prev ? prev->or_next : range;
	  dont_remove_sibling = true;
	  continue;
	}

      if (r_lop == PT_GT_INF)
	{
	  /* PT_BETWEEN_INF_LE or PT_BETWEEN_INF_LT */
	  r_lv = NULL;
	  r_uv = pt_value_to_db (parser, range->info.expr.arg1);
	}
      else if (r_uop == PT_LT_INF)
	{
	  /* PT_BETWEEN_GE_INF or PT_BETWEEN_GT_INF */
	  r_lv = pt_value_to_db (parser, range->info.expr.arg1);
	  r_uv = NULL;
	}
      else if (r_lop == PT_EQ)
	{
	  /* PT_BETWEEN_EQ_NA */
	  r_lv = pt_value_to_db (parser, range->info.expr.arg1);
	  r_uv = r_lv;
	}
      else
	{
	  /* PT_BETWEEN_GE_LE, PT_BETWEEN_GE_LT, PT_BETWEEN_GT_LE, or
	     PT_BETWEEN_GT_LT */
	  r_lv = pt_value_to_db (parser, range->info.expr.arg1);
	  r_uv = pt_value_to_db (parser, range->info.expr.arg2);
	}

      if (DB_IS_NULL (r_lv) && DB_IS_NULL (r_uv))
	{
	  /* if both are null, this expr is false. */
	  prev = prev ? prev->or_next : range;
	  dont_remove_sibling = true;
	  continue;
	}

      new_range = NULL;

      /* for each range spec of the node2 */
      for (sibling = node2->info.expr.arg2; sibling;
	   sibling = sibling->or_next)
	{

	  if (sibling->info.expr.arg2)
	    {
	      if (!pt_is_const_not_hostvar (sibling->info.expr.arg1)
		  || !pt_is_const_not_hostvar (sibling->info.expr.arg2))
		/* not constant; cannot be merged */
		continue;
	    }
	  else
	    {
	      if (!pt_is_const_not_hostvar (sibling->info.expr.arg1))
		/* not constant; cannot be merged */
		continue;
	    }

	  s_op = sibling->info.expr.op;
	  if (pt_between_to_comp_op (s_op, &s_lop, &s_uop) != 0)
	    /* something wrong; continue to next range spec */
	    continue;

	  if (s_lop == PT_GT_INF)
	    {
	      /* PT_BETWEEN_INF_LE or PT_BETWEEN_INF_LT */
	      s_lv = NULL;
	      s_uv = pt_value_to_db (parser, sibling->info.expr.arg1);
	    }
	  else if (s_uop == PT_LT_INF)
	    {
	      /* PT_BETWEEN_GE_INF or PT_BETWEEN_GT_INF */
	      s_lv = pt_value_to_db (parser, sibling->info.expr.arg1);
	      s_uv = NULL;
	    }
	  else if (s_lop == PT_EQ)
	    {
	      /* PT_BETWEEN_EQ_NA */
	      s_lv = pt_value_to_db (parser, sibling->info.expr.arg1);
	      s_uv = s_lv;
	    }
	  else
	    {
	      /* PT_BETWEEN_GE_LE, PT_BETWEEN_GE_LT, PT_BETWEEN_GT_LE, or
	         PT_BETWEEN_GT_LT */
	      s_lv = pt_value_to_db (parser, sibling->info.expr.arg1);
	      s_uv = pt_value_to_db (parser, sibling->info.expr.arg2);
	    }

	  if (DB_IS_NULL (s_lv) && DB_IS_NULL (s_uv))
	    {
	      /* if both are null, this expr is false. */
	      PT_EXPR_INFO_SET_FLAG (sibling, PT_EXPR_INFO_EMPTY_RANGE);
	      dont_remove_sibling = true;
	      continue;
	    }

	  PT_EXPR_INFO_CLEAR_FLAG (sibling, PT_EXPR_INFO_EMPTY_RANGE);
	  /* check if the two range specs are mergable */
	  cmp1 = qo_compare_dbvalue_with_optype (r_lv, r_lop, s_lv, s_lop);
	  cmp2 = qo_compare_dbvalue_with_optype (r_lv, r_lop, s_uv, s_uop);
	  cmp3 = qo_compare_dbvalue_with_optype (r_uv, r_uop, s_lv, s_lop);
	  cmp4 = qo_compare_dbvalue_with_optype (r_uv, r_uop, s_uv, s_uop);
	  if (cmp1 == CompResultError || cmp2 == CompResultError ||
	      cmp3 == CompResultError || cmp4 == CompResultError)
	    /* somthine wrong; continue to next range spec */
	    continue;
	  if (!new_range)
	    new_range = range;
	  if (!((cmp1 == CompResultLess || cmp1 == CompResultGreater) &&
		cmp1 == cmp2 && cmp1 == cmp3 && cmp1 == cmp4))
	    {
	      /* they are not disjoint; apply intersection to the two range
	         specs */

	      /* allocate new range spec node */
	      temp1 = range->or_next;
	      range->or_next = NULL;
	      temp2 = parser_copy_tree (parser, range);
	      new_op = r_op;
	      if (r_op == PT_BETWEEN_EQ_NA)
		{
		  parser_free_tree (parser, temp2->info.expr.arg2);
		  temp2->info.expr.arg2 = parser_copy_tree (parser,
							    temp2->info.expr.
							    arg1);
		}
	      new_lop = r_lop;
	      new_uop = r_uop;
	      temp2->or_next = (new_range == range) ? NULL : new_range;
	      new_range = temp2;
	      range->or_next = temp1;
	      /* swap arg1 and arg2 if op type is INF_LT or INF_LE to make
	         easy the following merge algorithm */
	      if (new_op == PT_BETWEEN_INF_LT || new_op == PT_BETWEEN_INF_LE)
		{
		  new_range->info.expr.arg2 = new_range->info.expr.arg1;
		  new_range->info.expr.arg1 = NULL;
		}
	      if (s_op == PT_BETWEEN_INF_LT || s_op == PT_BETWEEN_INF_LE)
		{
		  sibling->info.expr.arg2 = sibling->info.expr.arg1;
		  sibling->info.expr.arg1 = NULL;
		}
	      /* determine the lower bound of the merged range spec */
	      if (cmp1 == CompResultLess || cmp1 == CompResultLessAdj)
		{
		  parser_free_tree (parser, new_range->info.expr.arg1);
		  new_range->info.expr.arg1 =
		    parser_copy_tree (parser, sibling->info.expr.arg1);
		  new_lop = s_lop;
		  if (cmp3 == CompResultEqual && cmp4 == CompResultEqual)
		    {
		      new_uop = PT_EQ;
		    }
		}
	      /* determine the upper bound of the merged range spec */
	      if (cmp4 == CompResultGreaterAdj || cmp4 == CompResultGreater)
		{
		  parser_free_tree (parser, new_range->info.expr.arg2);
		  new_range->info.expr.arg2 =
		    parser_copy_tree (parser, sibling->info.expr.arg2);
		  new_uop = s_uop;
		}
	      /* determine the new range type */
	      if (pt_comp_to_between_op (new_lop, new_uop,
					 PT_RANGE_INTERSECTION, &new_op) != 0)
		{
		  /* they are not disjoint; remove empty range */
		  if (new_range->or_next == NULL)
		    {
		      parser_free_tree (parser, new_range);
		      new_range = range;
		    }
		  else
		    {
		      temp1 = new_range->or_next;
		      new_range->or_next = NULL;
		      parser_free_tree (parser, new_range);
		      new_range = temp1;
		    }
		}
	      else
		{		/* merged range is empty */
		  new_range->info.expr.op = new_op;
		  /* check if the new range is valid */
		  if (new_range->info.expr.arg1 && new_range->info.expr.arg2)
		    {
		      if (pt_between_to_comp_op (new_op, &new_lop, &new_uop)
			  != 0)
			{
			  /* must be be impossible; skip and go ahead */
			}
		      else
			{
			  new_lv = pt_value_to_db (parser,
						   new_range->info.expr.arg1);
			  new_uv = pt_value_to_db (parser,
						   new_range->info.expr.arg2);
			  new_cmp =
			    qo_compare_dbvalue_with_optype (new_lv, new_lop,
							    new_uv, new_uop);
			  if (new_cmp == CompResultGreater
			      || new_cmp == CompResultGreaterAdj)
			    {
			      /* they are not disjoint; remove empty range */
			      if (new_range->or_next == NULL)
				{
				  parser_free_tree (parser, new_range);
				  new_range = range;
				}
			      else
				{
				  temp1 = new_range->or_next;
				  new_range->or_next = NULL;
				  parser_free_tree (parser, new_range);
				  new_range = temp1;
				}
			    }
			}
		    }
		}		/* merged range is empty */

	      /* recover arg1 and arg2 for the type of INF_LT, INF_LE */
	      if (new_op == PT_BETWEEN_INF_LT || new_op == PT_BETWEEN_INF_LE)
		{
		  if (new_range->info.expr.arg1 == NULL
		      && new_range->info.expr.arg2 != NULL)
		    {
		      new_range->info.expr.arg1 = new_range->info.expr.arg2;
		      new_range->info.expr.arg2 = NULL;
		    }
		}
	      if (s_op == PT_BETWEEN_INF_LT || s_op == PT_BETWEEN_INF_LE)
		{
		  if (sibling->info.expr.arg1 == NULL
		      && sibling->info.expr.arg2 != NULL)
		    {
		      sibling->info.expr.arg1 = sibling->info.expr.arg2;
		      sibling->info.expr.arg2 = NULL;
		    }
		}
	    }

	  /* mark this sibling node to be deleted */
	  PT_EXPR_INFO_SET_FLAG (sibling, PT_EXPR_INFO_EMPTY_RANGE);
	}

      if (new_range == NULL)
	{
	  /* there was no application */
	  prev = prev ? prev->or_next : range;
	  continue;
	}

      /* replace the range node with the new_range node */
      if (new_range != range)
	{
	  if (prev)
	    {
	      prev->or_next = new_range;
	    }
	  else
	    {
	      node1->info.expr.arg2 = new_range;
	    }
	  for (prev = new_range; prev->or_next; prev = prev->or_next)
	    ;
	  prev->or_next = range->or_next;
	}
      else
	{
	  /* the result is empty range */
	  if (prev)
	    {
	      prev->or_next = range->or_next;
	    }
	  else
	    {
	      node1->info.expr.arg2 = range->or_next;
	    }
	}
      /* range->next == NULL */
      range->or_next = NULL;
      parser_free_tree (parser, range);
    }


  if (dont_remove_sibling != true)
    {
      /* remove nodes marked as to be deleted while applying intersction */
      prev = NULL;
      while ((sibling = (prev ? prev->or_next : node2->info.expr.arg2)))
	{
	  if (PT_EXPR_INFO_IS_FLAGED (sibling, PT_EXPR_INFO_EMPTY_RANGE))
	    {
	      if (prev)
		{
		  prev->or_next = sibling->or_next;
		}
	      else
		{
		  node2->info.expr.arg2 = sibling->or_next;
		}
	      /* sibling->next == NULL */
	      sibling->or_next = NULL;
	      parser_free_tree (parser, sibling);
	    }
	  else
	    {
	      prev = prev ? prev->or_next : sibling;
	    }
	}
    }

  for (range = node1->info.expr.arg2; range; range = range->or_next)
    {
      if (range->info.expr.op == PT_BETWEEN_EQ_NA
	  && range->info.expr.arg2 != NULL)
	{
	  parser_free_tree (parser, range->info.expr.arg2);
	  range->info.expr.arg2 = NULL;
	}
    }
  for (range = node2->info.expr.arg2; range; range = range->or_next)
    {
      if (range->info.expr.op == PT_BETWEEN_EQ_NA
	  && range->info.expr.arg2 != NULL)
	{
	  parser_free_tree (parser, range->info.expr.arg2);
	  range->info.expr.arg2 = NULL;
	}
    }
}

/*
 * qo_apply_range_intersection () - Apply range intersection
 *   return:
 *   parser(in):
 *   wherep(in): pointer to WHERE list
 */
static void
qo_apply_range_intersection (PARSER_CONTEXT * parser, PT_NODE ** wherep)
{
  PT_NODE *node, *sibling, *node_prev, *sibling_prev;
  int location;
  PT_NODE *arg1_prior, *sibling_prior;

  /* traverse CNF list and keep track of the pointer to previous node */
  node_prev = NULL;
  while ((node = (node_prev ? node_prev->next : *wherep)))
    {
      if (node->node_type != PT_EXPR
	  || node->info.expr.op != PT_RANGE || node->or_next != NULL)
	{
	  /* NOTE: Due to implementation complexity, handle one predicate
	     term only. */
	  /* neither expression node, RANGE node, nor one predicate term */
	  node_prev = node_prev ? node_prev->next : *wherep;
	  continue;
	}

      arg1_prior = NULL;
      if (PT_IS_PRIOR_NODE (node))
	{
	  arg1_prior = node->info.expr.arg1->info.expr.arg1;
	}
      else
	{
	  arg1_prior = node->info.expr.arg1;
	}

      if (!pt_is_attr (arg1_prior) && !pt_is_instnum (arg1_prior))
	{
	  /* LHS is not an attribute */
	  node_prev = node_prev ? node_prev->next : *wherep;
	  continue;
	}

      if (node->next == NULL)
	{			/* one range spec; nothing to intersect */
	  PT_NODE *range;
	  PT_OP_TYPE r_lop, r_uop;
	  DB_VALUE *r_lv, *r_uv;
	  COMP_DBVALUE_WITH_OPTYPE_RESULT cmp;

	  range = node->info.expr.arg2;
	  if (range->info.expr.arg2
	      && pt_is_const_not_hostvar (range->info.expr.arg1)
	      && pt_is_const_not_hostvar (range->info.expr.arg2))
	    {
	      /* both constant; check range spec */
	      if (!pt_between_to_comp_op (range->info.expr.op, &r_lop,
					  &r_uop))
		{
		  r_lv = pt_value_to_db (parser, range->info.expr.arg1);
		  r_uv = pt_value_to_db (parser, range->info.expr.arg2);
		  /* check if the range spec is valid */
		  cmp = qo_compare_dbvalue_with_optype (r_lv, r_lop,
							r_uv, r_uop);
		  if (cmp == CompResultError)
		    {
		      ;		/* somthine wrong; do nothing */
		    }
		  else if (cmp == CompResultGreaterAdj ||
			   cmp == CompResultGreater)
		    {
		      /* the range is invalid, that is, lower bound is greater
		       * than upper bound */
		      node->info.expr.arg2 = NULL;
		      parser_free_tree (parser, range);
		    }
		}
	    }
	}

      /* search CNF list from the next to the node and keep track of the
         pointer to previous node */
      sibling_prev = node;

      while ((sibling = sibling_prev->next))
	{
	  if (sibling->node_type != PT_EXPR
	      || sibling->info.expr.op != PT_RANGE
	      || sibling->or_next != NULL)
	    {
	      /* neither an expression node, RANGE node, nor one predicate term */
	      sibling_prev = sibling_prev->next;
	      continue;
	    }

	  sibling_prior = NULL;
	  if (PT_IS_PRIOR_NODE (sibling))
	    {
	      sibling_prior = sibling->info.expr.arg1->info.expr.arg1;
	      if (!PT_IS_PRIOR_NODE (node))
		{
		  /* sibling has prior, node hasn't */
		  sibling_prev = sibling_prev->next;
		  continue;
		}
	    }
	  else
	    {
	      sibling_prior = sibling->info.expr.arg1;
	      if (PT_IS_PRIOR_NODE (node))
		{
		  /* sibling hasn't prior, node has */
		  sibling_prev = sibling_prev->next;
		  continue;
		}
	    }
	  /* if node had prior check that sibling also contains prior and
	     vice-versa */

	  if (!pt_is_attr (sibling_prior) && !pt_is_instnum (sibling_prior))
	    {
	      /* LHS is not an attribute */
	      sibling_prev = sibling_prev->next;
	      continue;
	    }

	  if (sibling->info.expr.location != node->info.expr.location)
	    {
	      sibling_prev = sibling_prev->next;
	      continue;
	    }

	  if ((arg1_prior->node_type != sibling_prior->node_type)
	      || (pt_is_attr (arg1_prior)
		  && pt_is_attr (sibling_prior)
		  && pt_check_path_eq (parser, arg1_prior, sibling_prior)))
	    {
	      /* pt_check_path_eq() return non-zero if two are different */
	      sibling_prev = sibling_prev->next;
	      continue;
	    }

	  /* found a node of the same attribute */

	  /* combine each range specs of two RANGE nodes */
	  qo_apply_range_intersection_helper (parser, node, sibling);

	  /* remove the sibling node if its range is empty */
	  if (sibling->info.expr.arg2 == NULL)
	    {
	      sibling_prev->next = sibling->next;
	      sibling->next = NULL;
	      /* sibling->or_next == NULL */
	      parser_free_tree (parser, sibling);
	    }
	  else
	    {
	      sibling_prev = sibling_prev->next;
	    }

	  if (node->info.expr.arg2 == NULL)
	    break;
	}

      /* remove the node if its range is empty */
      if (node->info.expr.arg2 == NULL)
	{
	  if (node_prev)
	    node_prev->next = node->next;
	  else
	    *wherep = node->next;
	  node->next = NULL;
	  location = node->info.expr.location;	/* save location */

	  /* node->or_next == NULL */
	  parser_free_tree (parser, node);

	  if (location == 0)
	    {
	      /* empty conjuctive make whole condition always false */
	      /* NOTICE: that is valid only when we handle one predicate terms
	         in this function */
	      parser_free_tree (parser, *wherep);

	      /* make a single false node */
	      node = parser_new_node (parser, PT_VALUE);
	      if (node == NULL)
		{
		  PT_INTERNAL_ERROR (parser, "allocate new node");
		  return;
		}

	      node->type_enum = PT_TYPE_LOGICAL;
	      node->info.value.data_value.i = 0;
	      node->info.value.location = location;
	      (void) pt_value_to_db (parser, node);
	      *wherep = node;

	      return;
	    }
	  else
	    {
	      PT_NODE *prev, *next;

	      /* empty conjunctive is outer join ON condition.
	         remove all nodes which have same location number */
	      prev = NULL;
	      node = *wherep;
	      while (node)
		{
		  if ((node->node_type == PT_EXPR
		       && node->info.expr.location == location)
		      || (node->node_type == PT_VALUE
			  && node->info.value.location == location))
		    {
		      next = node->next;
		      node->next = NULL;
		      parser_free_tree (parser, node);
		      if (prev)
			prev->next = next;
		      else
			*wherep = next;
		      node = next;
		    }
		  else
		    {
		      prev = node;
		      node = node->next;
		    }
		}

	      /* make a single false node and append it to WHERE list */
	      node = parser_new_node (parser, PT_VALUE);
	      if (node == NULL)
		{
		  PT_INTERNAL_ERROR (parser, "allocate new node");
		  return;
		}

	      node->type_enum = PT_TYPE_LOGICAL;
	      node->info.value.data_value.i = 0;
	      node->info.value.location = location;
	      (void) pt_value_to_db (parser, node);
	      node->next = *wherep;
	      *wherep = node;

	      /* re-traverse CNF list */
	      node_prev = node;
	    }
	}
      else
	{
	  node_prev = (node_prev) ? node_prev->next : *wherep;
	}
    }
}

/*
 * qo_rewrite_outerjoin () - Rewrite outer join to inner join
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): SELECT node
 *   arg(in):
 *   continue_walk(in):
 *
 * Note: do parser_walk_tree() pre function
 */
static PT_NODE *
qo_rewrite_outerjoin (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		      int *continue_walk)
{
  PT_NODE *spec, *prev_spec, *expr, *ns;
  SPEC_ID_INFO info;
  int nullable_cnt;		/* nullable terms count */

  if (node->node_type != PT_SELECT)
    return node;

  /* traverse spec list */
  prev_spec = NULL;
  for (spec = node->info.query.q.select.from;
       spec; prev_spec = spec, spec = spec->next)
    {
      if (spec->info.spec.join_type == PT_JOIN_LEFT_OUTER
	  || spec->info.spec.join_type == PT_JOIN_RIGHT_OUTER)
	{
	  if (spec->info.spec.join_type == PT_JOIN_LEFT_OUTER)
	    {
	      info.id = spec->info.spec.id;
	    }
	  else if (prev_spec != NULL)
	    {
	      info.id = prev_spec->info.spec.id;
	    }

	  info.appears = false;
	  nullable_cnt = 0;

	  /* search where list */
	  for (expr = node->info.query.q.select.where;
	       expr; expr = expr->next)
	    {

	      /* skip out non-null RANGE sarg term only used for index scan;
	       * 'attr RANGE ( Min ge_inf )'
	       */
	      if (PT_EXPR_INFO_IS_FLAGED (expr, PT_EXPR_INFO_FULL_RANGE))
		{
		  continue;
		}

	      if (expr->node_type == PT_EXPR
		  && expr->info.expr.location == 0
		  && expr->info.expr.op != PT_IS_NULL
		  && expr->or_next == NULL)
		{
		  (void) parser_walk_leaves (parser, expr,
					     qo_get_name_by_spec_id, &info,
					     qo_check_nullable_expr,
					     &nullable_cnt);
		  /* have found a term which makes outer join to inner */
		  if (info.appears && nullable_cnt == 0)
		    {
		      spec->info.spec.join_type = PT_JOIN_INNER;
		      /* rewrite the following connected right outer join
		       * to inner join */
		      for (ns = spec->next;	/* traverse next spec */
			   ns && ns->info.spec.join_type != PT_JOIN_NONE;
			   ns = ns->next)
			{
			  if (ns->info.spec.join_type == PT_JOIN_RIGHT_OUTER)
			    ns->info.spec.join_type = PT_JOIN_INNER;
			}
		      break;
		    }
		}
	    }
	}

      if (spec->info.spec.derived_table
	  && spec->info.spec.derived_table_type == PT_IS_SUBQUERY)
	{
	  /* apply qo_rewrite_outerjoin() to derived table's subquery */
	  (void) parser_walk_tree (parser, spec->info.spec.derived_table,
				   qo_rewrite_outerjoin, NULL, NULL, NULL);
	}
    }

  *continue_walk = PT_LIST_WALK;

  return node;
}

/*
 * qo_reset_location () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_reset_location (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		   int *continue_walk)
{
  RESET_LOCATION_INFO *infop = (RESET_LOCATION_INFO *) arg;

  if (node->node_type == PT_EXPR
      && node->info.expr.location >= infop->start
      && node->info.expr.location <= infop->end)
    {
      node->info.expr.location = 0;
    }

  if (node->node_type == PT_NAME
      && node->info.name.location >= infop->start
      && node->info.name.location <= infop->end)
    {
      node->info.name.location = 0;
    }

  if (node->node_type == PT_VALUE
      && node->info.value.location >= infop->start
      && node->info.value.location <= infop->end)
    {
      node->info.value.location = 0;
    }

  return node;
}

/*
 * qo_rewrite_innerjoin () - Rewrite explicit(ordered) inner join
 *			  to implicit(unordered) inner join
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): SELECT node
 *   arg(in):
 *   continue_walk(in):
 *
 * Note: If join order hint is set, skip and go ahead.
 *   do parser_walk_tree() pre function
 */
static PT_NODE *
qo_rewrite_innerjoin (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		      int *continue_walk)
{
  PT_NODE *spec, *spec2;
  RESET_LOCATION_INFO info;	/* spec location reset info */

  if (node->node_type != PT_SELECT)
    return node;

  if (node->info.query.q.select.hint & PT_HINT_ORDERED)
    {
      /* join hint: force join left-to-right.
       * skip and go ahead.
       */
      return node;
    }

  info.start = 0;
  info.end = 0;
  info.found_outerjoin = false;

  /* traverse spec list to find disconnected spec list */
  for (info.start_spec = spec = node->info.query.q.select.from;
       spec; spec = spec->next)
    {

      switch (spec->info.spec.join_type)
	{
	case PT_JOIN_LEFT_OUTER:
	case PT_JOIN_RIGHT_OUTER:
	  /* case PT_JOIN_FULL_OUTER: */
	  info.found_outerjoin = true;
	  break;
	default:
	  break;
	}

      if (spec->info.spec.join_type == PT_JOIN_NONE
	  && info.found_outerjoin == false && info.start < info.end)
	{
	  /* rewrite explicit inner join to implicit inner join */
	  for (spec2 = info.start_spec; spec2 != spec; spec2 = spec2->next)
	    {
	      if (spec2->info.spec.join_type == PT_JOIN_INNER)
		spec2->info.spec.join_type = PT_JOIN_NONE;
	    }

	  /* reset location of spec list */
	  (void) parser_walk_tree (parser, node->info.query.q.select.where,
				   qo_reset_location, &info, NULL, NULL);

	  /* reset start spec, found_outerjoin */
	  info.start = spec->info.spec.location;
	  info.start_spec = spec;
	  info.found_outerjoin = false;
	}

      info.end = spec->info.spec.location;

      if (spec->info.spec.derived_table
	  && spec->info.spec.derived_table_type == PT_IS_SUBQUERY)
	{
	  /* apply qo_rewrite_innerjoin() to derived table's subquery */
	  (void) parser_walk_tree (parser, spec->info.spec.derived_table,
				   qo_rewrite_innerjoin, NULL, NULL, NULL);
	}
    }

  if (info.found_outerjoin == false && info.start < info.end)
    {
      /* rewrite explicit inner join to implicit inner join */
      for (spec2 = info.start_spec; spec2; spec2 = spec2->next)
	{
	  if (spec2->info.spec.join_type == PT_JOIN_INNER)
	    {
	      spec2->info.spec.join_type = PT_JOIN_NONE;
	    }
	}

      /* reset location of spec list */
      (void) parser_walk_tree (parser, node->info.query.q.select.where,
			       qo_reset_location, &info, NULL, NULL);
    }

  *continue_walk = PT_LIST_WALK;

  return node;
}

/*
 * qo_rewrite_query_as_derived () -
 *   return: rewritten select statement with derived table subquery
 *   parser(in):
 *   query(in):
 *
 * Note: returned result depends on global schema state.
 */
static PT_NODE *
qo_rewrite_query_as_derived (PARSER_CONTEXT * parser, PT_NODE * query)
{
  PT_NODE *new_query, *derived;
  PT_NODE *range, *spec, *temp, *node;
  PT_NODE **head;
  int i = 0;

  /* set line number to range name */
  range = pt_name (parser, "d3201");

  /* construct new spec
   * We are now copying the query and updating the spec_id references
   */
  spec = parser_new_node (parser, PT_SPEC);
  if (spec == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  derived = parser_copy_tree (parser, query);
  derived = mq_reset_ids_in_statement (parser, derived);

  /* increase correlation level of the query */
  if (query->info.query.correlation_level)
    {
      derived = mq_bump_correlation_level (parser, derived, 1,
					   derived->info.query.
					   correlation_level);
    }

  spec->info.spec.derived_table = derived;
  spec->info.spec.derived_table_type = PT_IS_SUBQUERY;
  spec->info.spec.range_var = range;
  spec->info.spec.id = (UINTPTR) spec;
  range->info.name.spec_id = (UINTPTR) spec;

  new_query = parser_new_node (parser, PT_SELECT);
  if (new_query == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  if (query->info.query.correlation_level)
    {
      new_query->info.query.correlation_level =
	query->info.query.correlation_level;
    }

  new_query->info.query.q.select.from = spec;


  temp = pt_get_select_list (parser, spec->info.spec.derived_table);
  head = &new_query->info.query.q.select.list;

  while (temp)
    {
      /* generate as_attr_list */
      node = pt_name (parser, mq_generate_name (parser, "a", &i));
      /* set line, column number */
      node->line_number = temp->line_number;
      node->column_number = temp->column_number;

      node->info.name.meta_class = PT_NORMAL;
      node->info.name.resolved = range->info.name.original;
      node->info.name.spec_id = spec->info.spec.id;
      node->type_enum = temp->type_enum;
      node->data_type = parser_copy_tree (parser, temp->data_type);
      spec->info.spec.as_attr_list =
	parser_append_node (node, spec->info.spec.as_attr_list);
      /* keep out hidden columns from derived select list */
      if (query->info.query.order_by && temp->is_hidden_column)
	{
	  temp->is_hidden_column = 0;
	}
      else
	{
	  if (temp->node_type == PT_NAME
	      && temp->info.name.meta_class == PT_SHARED)
	    {
	      /* This should not get lambda replaced during translation.
	       * Copy this node as-is rather than rewriting.
	       */
	      *head = parser_copy_tree (parser, temp);
	    }
	  else
	    {
	      *head = parser_copy_tree (parser, node);
	    }
	  head = &((*head)->next);
	}

      temp = temp->next;
    }

  /* move query id # */
  new_query->info.query.id = query->info.query.id;
  query->info.query.id = 0;

  return new_query;
}

/*
 * qo_rewrite_hidden_col_as_derived () - Rewrite subquery with ORDER BY
 *				      hidden column as derived one
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): QUERY node
 *   parent_node(in):
 *
 * Note: Keep out hidden column from derived select list
 */
static PT_NODE *
qo_rewrite_hidden_col_as_derived (PARSER_CONTEXT * parser, PT_NODE * node,
				  PT_NODE * parent_node)
{
  PT_NODE *t_node, *next, *derived;

  switch (node->node_type)
    {
    case PT_SELECT:
      if (node->info.query.order_by)
	{
	  bool remove_order_by = true;	/* guessing */

	  /* check parent context */
	  if (parent_node)
	    {
	      switch (parent_node->node_type)
		{
		case PT_FUNCTION:
		  switch (parent_node->info.function.function_type)
		    {
		    case F_TABLE_SEQUENCE:
		      remove_order_by = false;
		      break;
		    default:
		      break;
		    }
		  break;
		default:
		  break;
		}
	    }
	  else
	    {
	      remove_order_by = false;
	    }

	  /* check node context */
	  if (remove_order_by == true)
	    {
	      if (node->info.query.orderby_for)
		{
		  remove_order_by = false;
		}
	    }

	  if (remove_order_by == true)
	    {
	      for (t_node = node->info.query.q.select.list;
		   t_node; t_node = t_node->next)
		{
		  if (t_node->node_type == PT_EXPR
		      && t_node->info.expr.op == PT_ORDERBY_NUM)
		    {
		      remove_order_by = false;
		      break;
		    }
		}
	    }

	  /* remove unnecessary ORDER BY clause */
	  if (remove_order_by == true)
	    {
	      parser_free_tree (parser, node->info.query.order_by);
	      node->info.query.order_by = NULL;

	      for (t_node = node->info.query.q.select.list;
		   t_node && t_node->next; t_node = next)
		{
		  next = t_node->next;
		  if (next->is_hidden_column)
		    {
		      parser_free_tree (parser, next);
		      t_node->next = NULL;
		      break;
		    }
		}
	    }
	  else
	    {
	      for (t_node = node->info.query.q.select.list;
		   t_node; t_node = t_node->next)
		{
		  if (t_node->is_hidden_column)
		    {
		      /* make derived query */
		      derived = qo_rewrite_query_as_derived (parser, node);
		      if (derived == NULL)
			{
			  break;
			}

		      PT_NODE_MOVE_NUMBER_OUTERLINK (derived, node);
		      derived->info.query.q.select.flavor =
			node->info.query.q.select.flavor;
		      derived->info.query.is_subquery =
			node->info.query.is_subquery;

		      /* free old composite query */
		      parser_free_tree (parser, node);
		      node = derived;
		      break;
		    }
		}
	    }			/* else */
	}
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      node->info.query.q.union_.arg1 =
	qo_rewrite_hidden_col_as_derived (parser,
					  node->info.query.q.union_.arg1,
					  NULL);
      node->info.query.q.union_.arg2 =
	qo_rewrite_hidden_col_as_derived (parser,
					  node->info.query.q.union_.arg2,
					  NULL);
      break;
    default:
      return node;
    }

  return node;
}

/*
 * qo_rewrite_subqueries () - Rewrite uncorrelated subquery to join query
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): SELECT node
 *   arg(in):
 *   continue_walk(in):
 *
 * Note: do parser_walk_tree() pre function
 */
static PT_NODE *
qo_rewrite_subqueries (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		       int *continue_walk)
{
  PT_NODE *cnf_node, *arg1, *arg2, *select_list;
  PT_OP_TYPE op_type;
  PT_NODE *new_spec, *new_attr, *new_func;
  int *idx = (int *) arg;
  bool do_rewrite;
  PT_NODE *save_next, *arg1_next, *new_attr_next, *tmp;
  PT_OP_TYPE saved_op_type;

  if (node->node_type != PT_SELECT)
    {
      return node;
    }

  /* traverse CNF list */
  for (cnf_node = node->info.query.q.select.where; cnf_node;
       cnf_node = cnf_node->next)
    {

      if (cnf_node->or_next != NULL)
	{
	  continue;
	}

      if (cnf_node->node_type != PT_EXPR)
	{
	  continue;
	}

      op_type = cnf_node->info.expr.op;
      arg1 = cnf_node->info.expr.arg1;
      arg2 = cnf_node->info.expr.arg2;

      if (arg1 && arg2
	  && (op_type == PT_EQ
	      || op_type == PT_IS_IN
	      || op_type == PT_EQ_SOME
	      || op_type == PT_GT_SOME
	      || op_type == PT_GE_SOME
	      || op_type == PT_LT_SOME || op_type == PT_LE_SOME))
	{
	  /* go ahead */
	}
      else
	{
	  continue;
	}

      /* should be 'attr op uncorr-subquery',
         'set_func = uncorr-subquery',
         'set_func in uncorr-subquery',
         'uncorr-subquery = set_func',
         and select list of the subquery should be single column */

      do_rewrite = false;

      if ((select_list = pt_get_select_list (parser, arg1)) != NULL
	  && pt_length_of_select_list (select_list,
				       EXCLUDE_HIDDEN_COLUMNS) == 1
	  && arg1->info.query.correlation_level == 0)
	{
	  if ((arg2->node_type == PT_VALUE
	       || arg2->node_type == PT_FUNCTION)
	      && pt_is_set_type (arg2) && op_type == PT_EQ)
	    {
	      /* 'subquery = set_func' */
	      do_rewrite = true;

	      /* swap arg1, arg2 */
	      arg1 = cnf_node->info.expr.arg2;	/* set_func */
	      arg2 = cnf_node->info.expr.arg1;	/* subquery */

	      if ((select_list->node_type == PT_VALUE
		   || select_list->node_type == PT_FUNCTION)
		  && pt_is_set_type (select_list))
		{
		  if (arg1->node_type == PT_VALUE)
		    {
		      arg1 = arg1->info.value.data_value.set;
		    }
		  else
		    {		/* PT_FUNCTION */
		      arg1 = arg1->info.function.arg_list;
		    }
		  /* convert one column to select list */
		  pt_select_list_to_one_col (parser, arg2, false);
		}
	      else
		{		/* unknown error. skip and go ahead */
		  continue;
		}
	    }
	}
      else if ((select_list = pt_get_select_list (parser, arg2)) != NULL
	       && pt_length_of_select_list (select_list,
					    EXCLUDE_HIDDEN_COLUMNS) == 1
	       && arg2->info.query.correlation_level == 0)
	{
	  if (pt_is_attr (arg1))
	    {
	      /* 'attr op subquery' */
	      do_rewrite = true;
	    }
	  else if ((arg1->node_type == PT_VALUE
		    || arg1->node_type == PT_FUNCTION)
		   && pt_is_set_type (arg1)
		   && (op_type == PT_EQ || op_type == PT_IS_IN))
	    {
	      /* 'set_func = subquery' or 'set_func in subquery' */
	      do_rewrite = true;

	      if ((select_list->node_type == PT_VALUE
		   || select_list->node_type == PT_FUNCTION)
		  && pt_is_set_type (select_list))
		{
		  if (arg1->node_type == PT_VALUE)
		    {
		      arg1 = arg1->info.value.data_value.set;
		    }
		  else
		    {		/* PT_FUNCTION */
		      arg1 = arg1->info.function.arg_list;
		    }
		  /* convert one column to select list */
		  pt_select_list_to_one_col (parser, arg2, false);
		}
	      else
		{		/* unknown error. skip and go ahead */
		  continue;
		}
	    }
	}

      if (do_rewrite)
	{

	  /* rewrite subquery to join with derived table */
	  switch (op_type)
	    {
	    case PT_EQ:	/* arg1 = set_func_elements */
	    case PT_IS_IN:	/* arg1 = set_func_elements, attr */
	    case PT_EQ_SOME:	/* arg1 = attr */
	      /* make new derived spec and append it to FROM */
	      node = mq_make_derived_spec (parser, node, arg2,
					   idx, &new_spec, &new_attr);

	      /* convert to 'attr op attr' */
	      cnf_node->info.expr.arg1 = arg1;
	      arg1 = arg1->next;
	      cnf_node->info.expr.arg1->next = NULL;

	      cnf_node->info.expr.arg2 = new_attr;

	      saved_op_type = cnf_node->info.expr.op;

	      if (PT_IS_SET_TYPE (new_attr))
		{
		  ;		/* leave op as it is */
		}
	      else
		{
		  cnf_node->info.expr.op = PT_EQ;
		}

	      if (new_attr != NULL)
		{
		  new_attr = new_attr->next;
		  cnf_node->info.expr.arg2->next = NULL;
		}

	      /* save, cut-off link */
	      save_next = cnf_node->next;
	      cnf_node->next = NULL;

	      /* create the following 'attr op attr' */
	      for (tmp = NULL;
		   arg1 && new_attr;
		   arg1 = arg1_next, new_attr = new_attr_next)
		{
		  tmp = parser_new_node (parser, PT_EXPR);
		  if (tmp == NULL)
		    {
		      PT_INTERNAL_ERROR (parser, "allocate new node");
		      return NULL;
		    }

		  /* save, cut-off link */
		  arg1_next = arg1->next;
		  arg1->next = NULL;
		  new_attr_next = new_attr->next;
		  new_attr->next = NULL;

		  tmp->info.expr.arg1 = arg1;
		  tmp->info.expr.arg2 = new_attr;
		  if (PT_IS_SET_TYPE (new_attr))
		    {
		      tmp->info.expr.op = saved_op_type;
		    }
		  else
		    {
		      tmp->info.expr.op = PT_EQ;
		    }
		  cnf_node = parser_append_node (tmp, cnf_node);
		}

	      if (tmp)
		{		/* move to the last cnf */
		  cnf_node = tmp;
		}
	      cnf_node->next = save_next;	/* restore link */

	      /* apply qo_rewrite_subqueries() to derived table's subquery */
	      (void) parser_walk_tree (parser,
				       new_spec->info.spec.derived_table,
				       qo_rewrite_subqueries, idx, NULL,
				       NULL);
	      break;

	    case PT_GT_SOME:	/* arg1 = attr */
	    case PT_GE_SOME:	/* arg1 = attr */
	    case PT_LT_SOME:	/* arg1 = attr */
	    case PT_LE_SOME:	/* arg1 = attr */
	      if (arg2->node_type == PT_UNION
		  || arg2->node_type == PT_INTERSECTION
		  || arg2->node_type == PT_DIFFERENCE
		  || pt_has_aggregate (parser, arg2))
		{
		  /* if it is composite query, rewrite to simple query */
		  arg2 = qo_rewrite_query_as_derived (parser, arg2);
		  if (arg2 == NULL)
		    {
		      return NULL;
		    }

		  /* set as uncorrelated subquery */
		  arg2->info.query.q.select.flavor = PT_USER_SELECT;
		  arg2->info.query.is_subquery = PT_IS_SUBQUERY;
		  arg2->info.query.correlation_level = 0;

		  /* free old composite query */
		  parser_free_tree (parser, cnf_node->info.expr.arg2);
		  cnf_node->info.expr.arg2 = arg2;
		  select_list = pt_get_select_list (parser, arg2);
		}

	      if (select_list == NULL)
		{
		  return NULL;
		}

	      /* convert select list of subquery to MIN()/MAX() */
	      new_func = parser_new_node (parser, PT_FUNCTION);
	      if (new_func == NULL)
		{
		  PT_INTERNAL_ERROR (parser, "allocate new node");
		  return NULL;
		}

	      new_func->info.function.function_type =
		((op_type == PT_GT_SOME || op_type == PT_GE_SOME) ?
		 PT_MIN : PT_MAX);
	      new_func->info.function.all_or_distinct = PT_ALL;
	      new_func->info.function.arg_list = select_list;
	      new_func->type_enum = select_list->type_enum;
	      new_func->data_type =
		parser_copy_tree (parser, select_list->data_type);
	      arg2->info.query.q.select.list = new_func;
	      /* mark as agg select */
	      PT_SELECT_INFO_SET_FLAG (arg2, PT_SELECT_INFO_HAS_AGG);
	      /* make new derived spec and append it to FROM */
	      node = mq_make_derived_spec (parser, node, arg2,
					   idx, &new_spec, &new_attr);
	      /* convert to 'attr > new_attr' */
	      cnf_node->info.expr.arg2 = new_attr;
	      cnf_node->info.expr.op = (op_type == PT_GT_SOME) ? PT_GT :
		(op_type == PT_GE_SOME) ? PT_GE :
		(op_type == PT_LT_SOME) ? PT_LT : PT_LE;
	      /* apply qo_rewrite_subqueries() to derived table's subquery */
	      (void) parser_walk_tree (parser,
				       new_spec->info.spec.derived_table,
				       qo_rewrite_subqueries, idx, NULL,
				       NULL);
	      break;

	    default:
	      break;
	    }
	}
    }				/* for (cnf_node = ...) */

  *continue_walk = PT_LIST_WALK;

  return node;
}

/*
 * qo_is_partition_attr () -
 *   return:
 *   node(in):
 */
static int
qo_is_partition_attr (PT_NODE * node)
{
  if (node == NULL)
    {
      return 0;
    }

  node = pt_get_end_path_node (node);

  if (node->node_type == PT_NAME
      && node->info.name.meta_class == PT_NORMAL && node->info.name.spec_id)
    {
      if (node->info.name.partition_of)
	{
	  return 1;
	}
    }

  return 0;
}

/*
 * qo_do_auto_parameterize () - Convert value to host variable (input marker)
 *   return:
 *   parser(in):
 *   where(in): pointer to WHERE list
 *
 * Note:
 * 	examples:
 *      WHERE a=10 AND b<20   -->  WHERE a=? AND b<? w/ input host var 10, 20
 *
 */
static void
qo_do_auto_parameterize (PARSER_CONTEXT * parser, PT_NODE * where)
{
  PT_NODE *cnf_node, *dnf_node, *between_and;
  PT_NODE *node_prior;

  /* traverse CNF list */
  for (cnf_node = where; cnf_node; cnf_node = cnf_node->next)
    {

      /* traverse DNF list  */
      for (dnf_node = cnf_node; dnf_node; dnf_node = dnf_node->or_next)
	{
	  if (dnf_node->node_type != PT_EXPR)
	    {
	      /* dnf_node is not an expression node */
	      continue;
	    }

	  node_prior = NULL;
	  if (PT_IS_PRIOR_NODE (dnf_node))
	    {
	      node_prior = dnf_node->info.expr.arg1->info.expr.arg1;
	    }
	  else
	    {
	      node_prior = dnf_node->info.expr.arg1;
	    }

	  if (!pt_is_attr (node_prior)
	      && !pt_is_instnum (node_prior)
	      && !pt_is_orderbynum (node_prior))
	    {
	      /* neither LHS is an attribute, inst_num, nor orderby_num */
	      continue;
	    }
	  /* if it is partition prunning key */
	  if (!where->partition_pruned && qo_is_partition_attr (node_prior))
	    {
	      continue;
	    }
	  if (PT_EXPR_INFO_IS_FLAGED (dnf_node, PT_EXPR_INFO_FULL_RANGE))
	    {
	      continue;
	    }

	  switch (dnf_node->info.expr.op)
	    {
	    case PT_EQ:
	    case PT_GT:
	    case PT_GE:
	    case PT_LT:
	    case PT_LE:
	    case PT_LIKE:
	      if (pt_is_const_not_hostvar (dnf_node->info.expr.arg2)
		  && !PT_IS_NULL_NODE (dnf_node->info.expr.arg2))
		{
		  dnf_node->info.expr.arg2 =
		    pt_rewrite_to_auto_param (parser,
					      dnf_node->info.expr.arg2);
		}
	      break;
	    case PT_BETWEEN:
	      between_and = dnf_node->info.expr.arg2;
	      if (pt_is_const_not_hostvar (between_and->info.expr.arg1)
		  && !PT_IS_NULL_NODE (between_and->info.expr.arg1))
		{
		  between_and->info.expr.arg1 =
		    pt_rewrite_to_auto_param (parser,
					      between_and->info.expr.arg1);
		}
	      if (pt_is_const_not_hostvar (between_and->info.expr.arg2)
		  && !PT_IS_NULL_NODE (between_and->info.expr.arg2))
		{
		  between_and->info.expr.arg2 =
		    pt_rewrite_to_auto_param (parser,
					      between_and->info.expr.arg2);
		}
	      break;
	    case PT_IS_IN:
	      /* not yet implemented */
	      break;
	    case PT_RANGE:
	      between_and = dnf_node->info.expr.arg2;
	      if (between_and->or_next == NULL)
		{
		  if (pt_is_const_not_hostvar (between_and->info.expr.arg1)
		      && !PT_IS_NULL_NODE (between_and->info.expr.arg1))
		    {
		      between_and->info.expr.arg1 =
			pt_rewrite_to_auto_param (parser,
						  between_and->info.expr.
						  arg1);
		    }
		  if (pt_is_const_not_hostvar (between_and->info.expr.arg2)
		      && !PT_IS_NULL_NODE (between_and->info.expr.arg2))
		    {
		      between_and->info.expr.arg2 =
			pt_rewrite_to_auto_param (parser,
						  between_and->info.expr.
						  arg2);
		    }
		}
	      break;
	    default:
	      /* Is any other expression type possible to be auto-parameterized? */
	      break;
	    }
	}
    }

}

/*
 * qo_can_generate_single_table_connect_by () - checks a SELECT ... CONNECT BY
 *                                              query for single-table
 *                                              optimizations
 *   return: whether single-table optimization can be performed
 *   parser(in): parser environment
 *   node(in): SELECT ... CONNECT BY query
 * Note: The single-table optimizations (potentially using indexes for table
 *       access in START WITH and CONNECT BY predicates) can be performed if
 *       the query does not involve joins or partitioned tables.
 */
static bool
qo_can_generate_single_table_connect_by (PARSER_CONTEXT * parser,
					 PT_NODE * node)
{
  int level = 0;
  PT_NODE *name = NULL;
  PT_NODE *spec = NULL;
  DB_OBJECT *mobj = NULL;
  SM_CLASS *class_ = NULL;

  assert (node->node_type == PT_SELECT &&
	  node->info.query.q.select.connect_by != NULL);

  spec = node->info.query.q.select.from;

  if (node->info.query.q.select.where || spec->next)
    {
      /* joins */
      return false;
    }

  qo_get_optimization_param (&level, QO_PARAM_LEVEL);
  if (!OPTIMIZATION_ENABLED (level))
    {
      return false;
    }

  assert (spec->next == NULL);
  if (spec->node_type != PT_SPEC)
    {
      assert (false);
      return false;
    }

  if (spec->info.spec.only_all != PT_ONLY)
    {
      /* class hierarchy */
      return false;
    }

  name = spec->info.spec.entity_name;
  if (name == NULL)
    {
      return false;
    }
  assert (name->node_type == PT_NAME);
  if (name == NULL || name->node_type != PT_NAME)
    {
      assert (false);
      return false;
    }

  mobj = name->info.name.db_object;
  if (au_fetch_class (mobj, &class_, AU_FETCH_READ, AU_SELECT) != NO_ERROR)
    {
      return false;
    }
  if (class_ == NULL || class_->partition_of != NULL)
    {
      /* partitioned tables */
      return false;
    }
  return true;
}

/*
 * qo_optimize_queries () - checks all subqueries for rewrite optimizations
 *   return: PT_NODE *
 *   parser(in): parser environment
 *   node(in): possible query
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_optimize_queries (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		     int *continue_walk)
{
  int level, seqno = 0;
  PT_NODE *next, *pred, **wherep, **havingp, *dummy;
  PT_NODE *t_node, *spec;
  PT_NODE **startwithp, **connectbyp, **aftercbfilterp;
  bool call_auto_parameterize = false;

  dummy = NULL;
  wherep = havingp = startwithp = connectbyp = aftercbfilterp = &dummy;

  switch (node->node_type)
    {
    case PT_SELECT:
      if (node->info.query.q.select.connect_by != NULL
	  && !PT_IS_VALUE_NODE (node->info.query.q.select.where))
	{
	  PT_NODE *join_part = NULL;
	  PT_NODE *after_connectby_filter_part = NULL;

	  /* We need to separate the join predicates before we perform
	   * rewriting and optimizations so that they don't get mixed up with
	   * the filtering predicates (to be applied after connect by).
	   */
	  pt_split_join_preds (parser, node->info.query.q.select.where,
			       &join_part, &after_connectby_filter_part);

	  node->info.query.q.select.where = join_part;
	  assert (node->info.query.q.select.after_cb_filter == NULL);
	  node->info.query.q.select.after_cb_filter =
	    after_connectby_filter_part;

	  /* if we have no joins prepare for using heap scans/index scans for
	   * start with list and connect by processing
	   */
	  if (qo_can_generate_single_table_connect_by (parser, node))
	    {
	      node->info.query.q.select.where =
		node->info.query.q.select.start_with;
	      node->info.query.q.select.start_with = NULL;
	      node->info.query.q.select.single_table_opt = 1;
	    }
	}

      /* Put all join conditions together with WHERE clause for rewrite
         optimization. But we can distinguish a join condition from
         each other and from WHERE clause by location information
         that were marked at 'pt_bind_names()'. We'll recover the parse
         tree of join conditions using the location information in shortly. */
      t_node = node->info.query.q.select.where;
      while (t_node && t_node->next)
	{
	  t_node = t_node->next;
	}
      for (spec = node->info.query.q.select.from; spec; spec = spec->next)
	{
	  if (spec->node_type == PT_SPEC && spec->info.spec.on_cond)
	    {
	      if (!t_node)
		{
		  t_node = node->info.query.q.select.where
		    = spec->info.spec.on_cond;
		}
	      else
		{
		  t_node->next = spec->info.spec.on_cond;
		}
	      spec->info.spec.on_cond = NULL;
	      while (t_node->next)
		{
		  t_node = t_node->next;
		}
	    }
	}
      wherep = &node->info.query.q.select.where;
      havingp = &node->info.query.q.select.having;
      if (node->info.query.q.select.start_with)
	{
	  startwithp = &node->info.query.q.select.start_with;
	}
      if (node->info.query.q.select.connect_by)
	{
	  connectbyp = &node->info.query.q.select.connect_by;
	}
      if (node->info.query.q.select.after_cb_filter)
	{
	  aftercbfilterp = &node->info.query.q.select.after_cb_filter;
	}

      break;

    case PT_UPDATE:
      wherep = &node->info.update.search_cond;
      break;

    case PT_DELETE:
      wherep = &node->info.delete_.search_cond;
      break;

    case PT_INSERT:
      {
	PT_NODE *const subquery_ptr = pt_get_subquery_of_insert_select (node);

	if (subquery_ptr == NULL || subquery_ptr->node_type != PT_SELECT)
	  {
	    return node;
	  }
	wherep = &subquery_ptr->info.query.q.select.where;
      }
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      node->info.query.q.union_.arg1 =
	qo_rewrite_hidden_col_as_derived (parser,
					  node->info.query.q.union_.arg1,
					  NULL);
      node->info.query.q.union_.arg2 =
	qo_rewrite_hidden_col_as_derived (parser,
					  node->info.query.q.union_.arg2,
					  NULL);
      /* no WHERE clause */
      return node;

    case PT_EXPR:
      switch (node->info.expr.op)
	{
	case PT_EQ:
	case PT_NE:
	case PT_NULLSAFE_EQ:
	  node->info.expr.arg1 =
	    qo_rewrite_hidden_col_as_derived (parser, node->info.expr.arg1,
					      node);
	  /* fall through */

	  /* keep out hidden column subquery from UPDATE assignment */
	case PT_ASSIGN:
	  /* quantified comparisons */
	case PT_GE_SOME:
	case PT_GT_SOME:
	case PT_LT_SOME:
	case PT_LE_SOME:
	case PT_GE_ALL:
	case PT_GT_ALL:
	case PT_LT_ALL:
	case PT_LE_ALL:
	  /* quantified equality comparisons */
	case PT_EQ_SOME:
	case PT_NE_SOME:
	case PT_EQ_ALL:
	case PT_NE_ALL:
	case PT_IS_IN:
	case PT_IS_NOT_IN:
	  node->info.expr.arg2 =
	    qo_rewrite_hidden_col_as_derived (parser, node->info.expr.arg2,
					      node);
	  break;
	default:
	  break;
	}
      /* no WHERE clause */
      return node;

    case PT_FUNCTION:
      switch (node->info.function.function_type)
	{
	case F_TABLE_SET:
	case F_TABLE_MULTISET:
	case F_TABLE_SEQUENCE:
	  node->info.function.arg_list =
	    qo_rewrite_hidden_col_as_derived (parser,
					      node->info.function.arg_list,
					      node);
	  break;
	default:
	  break;
	}
      /* no WHERE clause */
      return node;

    default:
      /* no WHERE clause */
      return node;
    }

  if (node->node_type == PT_SELECT)
    {
      /* analyze paths for possible optimizations */
      node->info.query.q.select.from =
	parser_walk_tree (parser, node->info.query.q.select.from,
			  qo_analyze_path_join_pre, NULL,
			  qo_analyze_path_join,
			  node->info.query.q.select.where);
    }

  qo_get_optimization_param (&level, QO_PARAM_LEVEL);
  if (OPTIMIZATION_ENABLED (level))
    {

      if (node->node_type == PT_SELECT)
	{
	  int continue_walk;
	  int idx = 0;

	  /* rewrite uncorrelated subquery to join query */
	  qo_rewrite_subqueries (parser, node, &idx, &continue_walk);
	}

      /* rewrite optimization on WHERE, HAVING clause
       */

      if (!*wherep && !*havingp && !*aftercbfilterp && !*startwithp
	  && !*connectbyp)
	{
	  if (node->node_type != PT_SELECT)
	    {
	      return node;
	    }
	  else
	    {
	      /* check for group by, order by */
	      if (node->info.query.q.select.group_by == NULL
		  && node->info.query.order_by == NULL)
		{
		  return node;
		}		/* else - go ahead */
	    }
	}

      /* convert to CNF and tag taggable terms */
      if (*wherep)
	{
	  *wherep = pt_cnf (parser, *wherep);
	}
      if (*havingp)
	{
	  *havingp = pt_cnf (parser, *havingp);
	}
      if (*startwithp)
	{
	  *startwithp = pt_cnf (parser, *startwithp);
	}
      if (*connectbyp)
	{
	  *connectbyp = pt_cnf (parser, *connectbyp);
	}
      if (*aftercbfilterp)
	{
	  *aftercbfilterp = pt_cnf (parser, *aftercbfilterp);
	}

      /* in HAVING clause with GROUP BY,
       * move non-aggregate terms to WHERE clause
       */
      if (node->node_type == PT_SELECT
	  && node->info.query.q.select.group_by && *havingp)
	{
	  PT_NODE *prev, *cnf, *next;
	  PT_AGG_INFO info;
	  int has_pseudocolumn;

	  prev = NULL;		/* init */
	  for (cnf = *havingp; cnf; cnf = next)
	    {
	      next = cnf->next;	/* save and cut-off link */
	      cnf->next = NULL;

	      /* init agg info */
	      info.from = node->info.query.q.select.from;	/* set as SELECT */
	      info.agg_found = false;
	      info.depth = 0;
	      (void) parser_walk_tree (parser, cnf,
				       pt_is_aggregate_node, &info,
				       pt_is_aggregate_node_post, &info);

	      /* Note: Do not move the cnf node if it contains a pseudo-column!
	       */
	      if (!info.agg_found)
		{
		  has_pseudocolumn = 0;
		  (void) parser_walk_tree (parser, cnf,
					   pt_is_pseudocolumn_node,
					   &has_pseudocolumn, NULL, NULL);
		  if (has_pseudocolumn)
		    {
		      info.agg_found = true;
		    }
		}

	      /* Not found aggregate function in cnf node.
	       * So, move it from HAVING clause to WHERE clause.
	       */
	      if (!info.agg_found)
		{
		  /* delete cnf node from HAVING clause */
		  if (!prev)
		    {		/* very the first node */
		      *havingp = next;
		    }
		  else
		    {
		      prev->next = next;
		    }

		  /* add cnf node to WHERE clause
		   */
		  *wherep = parser_append_node (*wherep, cnf);
		}
	      else
		{		/* do noting and go ahead */
		  cnf->next = next;	/* restore link */

		  /* save previous */
		  prev = cnf;
		}
	    }
	}

      /* reduce equality terms */
      if (*wherep)
	{
	  qo_reduce_equality_terms (parser, node, wherep);
	}
      if (*havingp)
	{
	  qo_reduce_equality_terms (parser, node, havingp);
	}

      /*  we don't reduce equality terms for startwith and connectby. This optimization
       *  for every A after a statement like A = 5, replaced the column with the
       *  scalar 5. If the column is in an ORDER BY clause, the sorting may not
       *  occur on column A because it's always 5. This behavior is incorrect
       *  when running a hierarchical query because there may be a A = 5 in the
       *  START WITH part or CONNECT BY part but the ORDER BY on A should sort
       *  all elements from all levels, column A being different.
       */
      if (*aftercbfilterp)
	{
	  qo_reduce_equality_terms (parser, node, aftercbfilterp);
	}

      /* convert terms of the form 'const op attr' to 'attr op const' */
      if (*wherep)
	{
	  qo_converse_sarg_terms (parser, *wherep);
	}
      if (*havingp)
	{
	  qo_converse_sarg_terms (parser, *havingp);
	}
      if (*startwithp)
	{
	  qo_converse_sarg_terms (parser, *startwithp);
	}
      if (*connectbyp)
	{
	  qo_converse_sarg_terms (parser, *connectbyp);
	}
      if (*aftercbfilterp)
	{
	  qo_converse_sarg_terms (parser, *aftercbfilterp);
	}

      /* reduce a pair of comparison terms into one BETWEEN term */
      if (*wherep)
	{
	  qo_reduce_comp_pair_terms (parser, wherep);
	}
      if (*havingp)
	{
	  qo_reduce_comp_pair_terms (parser, havingp);
	}
      if (*startwithp)
	{
	  qo_reduce_comp_pair_terms (parser, startwithp);
	}
      if (*connectbyp)
	{
	  qo_reduce_comp_pair_terms (parser, connectbyp);
	}
      if (*aftercbfilterp)
	{
	  qo_reduce_comp_pair_terms (parser, aftercbfilterp);
	}

      /* convert a leftmost LIKE term to a BETWEEN (GE_LT) term */
      if (*wherep)
	{
	  qo_rewrite_like_terms (parser, wherep);
	}
      if (*havingp)
	{
	  qo_rewrite_like_terms (parser, havingp);
	}
      if (*startwithp)
	{
	  qo_rewrite_like_terms (parser, startwithp);
	}
      if (*connectbyp)
	{
	  qo_rewrite_like_terms (parser, connectbyp);
	}
      if (*aftercbfilterp)
	{
	  qo_rewrite_like_terms (parser, aftercbfilterp);
	}

      /* convert comparison terms to RANGE */
      if (*wherep)
	{
	  qo_convert_to_range (parser, wherep);
	}
      if (*havingp)
	{
	  qo_convert_to_range (parser, havingp);
	}
      if (*startwithp)
	{
	  qo_convert_to_range (parser, startwithp);
	}
      if (*connectbyp)
	{
	  qo_convert_to_range (parser, connectbyp);
	}
      if (*aftercbfilterp)
	{
	  qo_convert_to_range (parser, aftercbfilterp);
	}

      /* narrow search range by applying range intersection */
      if (*wherep)
	{
	  qo_apply_range_intersection (parser, wherep);
	}
      if (*havingp)
	{
	  qo_apply_range_intersection (parser, havingp);
	}
      if (*startwithp)
	{
	  qo_apply_range_intersection (parser, startwithp);
	}
      if (*connectbyp)
	{
	  qo_apply_range_intersection (parser, connectbyp);
	}
      if (*aftercbfilterp)
	{
	  qo_apply_range_intersection (parser, aftercbfilterp);
	}

      /* remove meaningless IS NULL/IS NOT NULL terms */
      if (*wherep)
	{
	  qo_fold_is_and_not_null (parser, wherep);
	}
      if (*havingp)
	{
	  qo_fold_is_and_not_null (parser, havingp);
	}
      if (*startwithp)
	{
	  qo_fold_is_and_not_null (parser, startwithp);
	}
      if (*connectbyp)
	{
	  qo_fold_is_and_not_null (parser, connectbyp);
	}
      if (*aftercbfilterp)
	{
	  qo_fold_is_and_not_null (parser, aftercbfilterp);
	}

      if (node->node_type == PT_SELECT)
	{
	  int continue_walk;

	  /* rewrite outer join to inner join */
	  qo_rewrite_outerjoin (parser, node, NULL, &continue_walk);

	  /* rewrite explicit inner join to implicit inner join */
	  qo_rewrite_innerjoin (parser, node, NULL, &continue_walk);

	  pred = qo_get_next_oid_pred (*wherep);
	  if (pred)
	    {
	      while (pred)
		{
		  next = pred->next;
		  node = qo_rewrite_oid_equality (parser, node, pred, &seqno);
		  pred = qo_get_next_oid_pred (next);
		}		/* while (pred) */
	      /* re-analyze paths for possible optimizations */
	      node->info.query.q.select.from =
		parser_walk_tree (parser, node->info.query.q.select.from,
				  qo_analyze_path_join_pre, NULL,
				  qo_analyze_path_join,
				  node->info.query.q.select.where);
	    }			/* if (pred) */

	  if (qo_reduce_order_by (parser, node) != NO_ERROR)
	    {
	      return node;	/* give up */
	    }
	}

      if (!node->partition_pruned
	  && (node->node_type == PT_SELECT
	      || node->node_type == PT_DELETE
	      || node->node_type == PT_UPDATE))
	{
	  if (node->node_type == PT_SELECT
	      && node->info.query.q.select.from->partition_pruned)
	    {
	      node->partition_pruned = 1;	/* for DELETE/UPDATE */
	      node->info.query.q.select.where->partition_pruned = 1;
	    }
	  else
	    {
	      do_apply_partition_pruning (parser, node);
	    }
	}

      /* auto-parameterization is safe when it is done as the last step
         of rewrite optimization */
      if (!PRM_HOSTVAR_LATE_BINDING
	  && PRM_XASL_MAX_PLAN_CACHE_ENTRIES > 0 && node->cannot_prepare == 0)
	{
	  call_auto_parameterize = true;
	}
    }

  /* auto-parameterize
     convert value in expression to host variable (input marker) */
  if (*wherep && (call_auto_parameterize ||
		  (*wherep)->force_auto_parameterize))
    {
      qo_do_auto_parameterize (parser, *wherep);
    }
  if (*havingp && (call_auto_parameterize ||
		   (*havingp)->force_auto_parameterize))
    {
      qo_do_auto_parameterize (parser, *havingp);
    }
  if (*startwithp && (call_auto_parameterize ||
		      (*startwithp)->force_auto_parameterize))
    {
      qo_do_auto_parameterize (parser, *startwithp);
    }
  if (*connectbyp && (call_auto_parameterize ||
		      (*connectbyp)->force_auto_parameterize))
    {
      qo_do_auto_parameterize (parser, *connectbyp);
    }
  if (*aftercbfilterp && (call_auto_parameterize ||
			  (*aftercbfilterp)->force_auto_parameterize))
    {
      qo_do_auto_parameterize (parser, *aftercbfilterp);
    }
  if (node->node_type == PT_SELECT && node->info.query.orderby_for &&
      (call_auto_parameterize ||
       node->info.query.orderby_for->force_auto_parameterize))
    {
      qo_do_auto_parameterize (parser, node->info.query.orderby_for);
    }

  if (node->node_type == PT_SELECT
      && node->info.query.is_subquery == PT_IS_SUBQUERY)
    {
      if (node->info.query.single_tuple == 1)
	{
	  node = qo_rewrite_hidden_col_as_derived (parser, node, NULL);
	}
    }

  return node;
}

/*
 * qo_optimize_queries_post () -
 *   return:
 *   parser(in):
 *   tree(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_optimize_queries_post (PARSER_CONTEXT * parser, PT_NODE * tree, void *arg,
			  int *continue_walk)
{
  PT_NODE *node, *prev, *next, *spec;
  short location;

  switch (tree->node_type)
    {
    case PT_SELECT:
      prev = NULL;
      for (node = tree->info.query.q.select.where; node; node = next)
	{
	  next = node->next;
	  node->next = NULL;

	  if (node->node_type == PT_EXPR)
	    location = node->info.expr.location;
	  else if (node->node_type == PT_VALUE)
	    location = node->info.value.location;
	  else
	    location = -1;

	  if (location > 0)
	    {
	      for (spec = tree->info.query.q.select.from;
		   spec && spec->info.spec.location != location;
		   spec = spec->next)
		/* nop */ ;
	      if (spec)
		{
		  if (spec->info.spec.join_type == PT_JOIN_LEFT_OUTER
		      || spec->info.spec.join_type == PT_JOIN_RIGHT_OUTER
		      || spec->info.spec.join_type == PT_JOIN_INNER)
		    {
		      node->next = spec->info.spec.on_cond;
		      spec->info.spec.on_cond = node;

		      if (prev)
			{
			  prev->next = next;
			}
		      else
			{
			  tree->info.query.q.select.where = next;
			}
		    }
		  else
		    {		/* already converted to inner join */
		      /* clear on cond location */
		      if (node->node_type == PT_EXPR)
			{
			  node->info.expr.location = 0;
			}
		      else if (node->node_type == PT_VALUE)
			{
			  node->info.value.location = 0;
			}

		      /* Here - at the last stage of query optimize,
		       * remove copy-pushed term */
		      if (node->node_type == PT_EXPR
			  && PT_EXPR_INFO_IS_FLAGED (node,
						     PT_EXPR_INFO_COPYPUSH))
			{
			  parser_free_tree (parser, node);

			  if (prev)
			    {
			      prev->next = next;
			    }
			  else
			    {
			      tree->info.query.q.select.where = next;
			    }
			}
		      else
			{
			  prev = node;
			  node->next = next;
			}
		    }
		}
	      else
		{
		  /* might be impossible
		   * might be outer join error
		   */
		  PT_ERRORf (parser, node, "check outer join syntax at '%s'",
			     pt_short_print (parser, node));

		  prev = node;
		  node->next = next;
		}
	    }
	  else
	    {
	      /* Here - at the last stage of query optimize,
	       * remove copy-pushed term */
	      if (node->node_type == PT_EXPR
		  && PT_EXPR_INFO_IS_FLAGED (node, PT_EXPR_INFO_COPYPUSH))
		{
		  parser_free_tree (parser, node);

		  if (prev)
		    {
		      prev->next = next;
		    }
		  else
		    {
		      tree->info.query.q.select.where = next;
		    }
		}
	      else
		{
		  prev = node;
		  node->next = next;
		}
	    }
	}
      break;
    default:
      break;
    }

  return tree;
}

/*
 * mq_optimize () - optimize statements by a variety of rewrites
 *   return: void
 *   parser(in): parser environment
 *   statement(in): select tree to optimize
 *
 * Note: rewrite only if optimization is enabled
 */
PT_NODE *
mq_optimize (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  return parser_walk_tree (parser, statement,
			   qo_optimize_queries, NULL,
			   qo_optimize_queries_post, NULL);
}
