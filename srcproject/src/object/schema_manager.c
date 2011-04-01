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
 * schema_manager.c - "Schema" (in the SQL standard sense) implementation
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#ifdef HPUX
#include <a.out.h>
#endif /* HPUX */


#include "dbtype.h"
#include "authenticate.h"
#include "string_opfunc.h"
#include "schema_manager.h"
#include "porting.h"
#include "chartype.h"
#if !defined(WINDOWS)
#include "dynamic_load.h"
#endif /* !WINDOWS */
#include "error_manager.h"
#include "work_space.h"
#include "object_primitive.h"
#include "class_object.h"
#include "message_catalog.h"
#include "memory_alloc.h"
#include "environment_variable.h"

#include "language_support.h"
#include "object_representation.h"
#include "object_domain.h"
#include "set_object.h"
#include "virtual_object.h"
#include "transform_cl.h"
#include "locator_cl.h"
#include "statistics.h"
#include "network_interface_cl.h"
#include "parser.h"
#include "trigger_manager.h"
#include "storage_common.h"
#include "transform.h"
#include "system_parameter.h"
#include "object_template.h"
#include "execute_schema.h"
#include "transaction_cl.h"
#include "release_string.h"
#include "execute_statement.h"
#include "cluster_config.h"

#include "db.h"
#include "object_accessor.h"
#include "boot_cl.h"

#define UNIQUE_SAVEPOINT_NAME "aDDuNIQUEcONSTRAINT"
#define UNIQUE_SAVEPOINT_NAME2 "dELETEcLASSmOP"
#define UNIQUE_SAVEPOINT_SM_TRUNCATE "SmtRUnCATE"

/*
 * SCHEMA_DEFINITION
 *
 * description:
 *    Maintains information about an SQL schema.
 */

/*
   NOTE: This is simple-minded implementation for now since we don't yet
         support CREATE SCHEMA, SET SCHEMA, and associated statements.
 */

typedef struct schema_def
{

  /* This is the default qualifier for class/vclass names */
  char name[DB_MAX_SCHEMA_LENGTH + 4];

  /* The only user who can delete this schema. */
  /* But, note that entry level doesn't support DROP SCHEMA anyway */
  MOP owner;

  /* The next three items are currently not used at all.
     They are simply a reminder of future TODOs.
     Although entry level SQL leaves out many schema management functions,
     entry level SQL does include specification of tables, views, and grants
     as part of CREATE SCHEMA statements. */

  void *tables;			/* unused dummy                             */
  void *views;			/* unused dummy                             */
  void *grants;			/* unused dummy                             */

} SCHEMA_DEF;

/*
 * Current_schema
 *
 * description:
 *    This is the current schema.  The schema name in this structure is the
 *    default qualifier for any class/vclass names which are not
 *    explicitly qualified.
 *    This structure should only be changed with sc_set_current_schema which
 *    currently is called only from AU_SET_USER
 */

static SCHEMA_DEF Current_Schema = { {'\0'}, NULL, NULL, NULL, NULL };





#define WC_PERIOD L'.'

/*
 *    Internal structure for maintaining the global list of static
 *    method linkage information.  This list is built with user supplied
 *    information by calling sm_add_static_method().
 */
typedef struct static_method STATIC_METHOD;

struct static_method
{
  struct static_method *next;

  char *name;
  void (*function) ();

};

/*
 *    Temporary structure used to hold linking state during dynamic linking.
 */
typedef struct method_link METHOD_LINK;

struct method_link
{
  struct method_link *next;

  SM_METHOD *method;
  int namelist_index;

};

/* various states of a domain comparison. */
typedef enum
{

  DC_INCOMPATIBLE,
  DC_EQUAL,
  DC_LESS_SPECIFIC,
  DC_MORE_SPECIFIC
} DOMAIN_COMP;

/*
 *    Structure used internally during the flattening of a class
 *    hierarchy.  This information could be folded in with the
 *    class and method structure definitions but its only used once
 *    and makes it less confusing for the flattener.
 *    For each attribute and method in a class hierarchy, a candidate
 *    structure will be built during flattening.
 */
typedef struct sm_candidate SM_CANDIDATE;

struct sm_candidate
{
  struct sm_candidate *next;

  const char *name;
  const char *alias;
  MOP origin;
  MOP source;
  SM_COMPONENT *obj;		/* actual component structure */
  SM_NAME_SPACE name_space;
  int order;

  unsigned int is_alias:1;	/* expanded alias candidates */
  unsigned int is_requested:1;	/* requested in a resolution specifier */
};

/*
 * Structure used internally during truncating a class.
 * When truncating, indexes are dropped and recreated. The information needed
 * to recreate the constraints (indexes) are saved in this structure.
 */
typedef struct sm_constraint_info SM_CONSTRAINT_INFO;

struct sm_constraint_info
{
  struct sm_constraint_info *next;
  char *name;
  char **att_names;
  int *asc_desc;
  int *prefix_length;
  char *ref_cls_name;
  char **ref_attrs;
  char *fk_cache_attr;
  SM_FOREIGN_KEY_ACTION fk_delete_action;
  SM_FOREIGN_KEY_ACTION fk_update_action;
  DB_CONSTRAINT_TYPE constraint_type;
};

/*
 * Static_method_table
 *
 * description:
 *    Global list of static method link information.
 */

static STATIC_METHOD *Static_method_table = NULL;

/*
 * Platform specific default extension for method files.
 * These are added automatically if the extension is not found in the schema
 */
#if defined(WINDOWS)
static const char *method_file_extension = ".dll";
#elif defined (HPUX)
static const char *method_file_extension = ".sl";
#elif defined (SOLARIS) || defined(LINUX)
static const char *method_file_extension = ".so";
#elif defined(sun) || defined(AIX)
static const char *method_file_extension = ".o";
#else /* WINDOWS */
#error "Unknown machine type."
#endif /* WINDOWS */

#if !defined(WINDOWS)
#include <nlist.h>
#endif /* !WINDOWS */




/*
 *    This is used only in the internationalized version.
 *    If this flag is non-zero, it will cause sm_check_name to not perform
 *    any validation of the names supplied to the API.
 *    This is used when the API functions are called by the interpreter
 *    which has already performed the validation and character set
 *    conversion.  Since this is done by the interpreter, we don't need to
 *    duplicate it here.
*/
int sm_Inhibit_identifier_check = 0;

const char TEXT_CONSTRAINT_PREFIX[] = "#text_";

/*
 *    This is the root of the currently active attribute/method descriptors.
 *    These are kept on a list so we can quickly invalidate them during
 *    significant but unusual events like schema changes.
 *    This list is generally short.  If it can be long, consider a
 *    doubly linked list for faster removal.
*/

/* When execute do_schema_sync function, this will be set to true */
bool sm_Do_schema_sync = false;

SM_DESCRIPTOR *sm_Descriptors = NULL;

/* ROOT_CLASS GLOBALS */
/* Global root class structure */
ROOT_CLASS sm_Root_class;

/* Global MOP for the root class object.  Used by the locator */
MOP sm_Root_class_mop = NULL;

/* Name of the root class */
const char *sm_Root_class_name = ROOTCLASS_NAME;

/* Heap file identifier for the root class */
HFID *sm_Root_class_hfid = &sm_Root_class.header.heap;

/* Flag to do update statistics */
bool sm_Disable_updating_statistics = false;

static unsigned int schema_version_number = 0;

static int domain_search (MOP dclass_mop, MOP class_mop);
static int annotate_method_files (MOP classmop, SM_CLASS * class_);
static int alter_trigger_cache (SM_CLASS * class_, const char *attribute,
				int class_attribute,
				DB_OBJECT * trigger, int drop_it);
static int alter_trigger_hierarchy (DB_OBJECT * classop,
				    const char *attribute,
				    int class_attribute,
				    DB_OBJECT * target_class,
				    DB_OBJECT * trigger, int drop_it);
static int find_attribute_op (MOP op, const char *name,
			      SM_CLASS ** classp, SM_ATTRIBUTE ** attp);
#if defined (ENABLE_UNUSED_FUNCTION)
static int lock_query_subclasses (DB_OBJLIST ** subclasses, MOP op,
				  DB_OBJLIST * exceptions, int update);
static void sm_gc_domain (TP_DOMAIN * domain, void (*gcmarker) (MOP));
static void sm_gc_attribute (SM_ATTRIBUTE * att, void (*gcmarker) (MOP));
static void sm_gc_method (SM_METHOD * meth, void (*gcmarker) (MOP));
#endif

static int fetch_descriptor_class (MOP op, SM_DESCRIPTOR * desc,
				   int for_update, SM_CLASS ** class_);



static STATIC_METHOD *sm_find_static_method (const char *name);
static int sm_count_tokens (const char *string, int *maxcharp);
static int sm_split_loader_commands (const char *string,
				     const char ***command_ptr);
static void sm_free_loader_commands (char **commands);
static void sm_free_method_links (METHOD_LINK * links);
static int sm_link_static_method (SM_METHOD * method,
				  METHOD_LINK ** link_ptr);
static int sm_link_static_methods (SM_CLASS * class_,
				   METHOD_LINK ** links_ptr);
static int sm_expand_method_files (SM_METHOD_FILE * files);
#if !defined(WINDOWS)
static int sm_build_function_nlist (METHOD_LINK * links,
				    struct nlist **nlist_ptr);
static void sm_free_function_nlist (struct nlist *namelist);
#endif /* !WINDOWS */
#if defined (sun) || defined(SOLARIS) || defined(LINUX)
#if defined(SOLARIS) || defined(LINUX)
static int sm_link_dynamic_methods (METHOD_LINK * links, const char **files);
#else /* SOLARIS || LINUX */
static int sm_link_dynamic_methods (METHOD_LINK * links, const char **files,
				    const char **commands);
#endif /* SOLARIS || LINUX */
#elif defined (_AIX)
static int sm_link_dynamic_methods (METHOD_LINK * links, const char **files,
				    const char **commands);
#elif defined(HPUX)
static int sm_link_dynamic_methods (METHOD_LINK * links, const char **files,
				    const char **commands);
#elif defined(WINDOWS)
static HINSTANCE load_dll (const char *name);
static int sm_link_dynamic_methods (METHOD_LINK * links, const char **files,
				    const char **commands);
#endif /*  sun || SOLARIS || LINUX */
static int sm_file_extension (const char *path, const char *ext);
static int sm_dynamic_link_class (SM_CLASS * class_, METHOD_LINK * links);
static int sm_link_methods (SM_CLASS * class_);


static int check_resolution_target (SM_TEMPLATE * template_,
				    SM_RESOLUTION * res, int *valid_ptr);
static const char *template_classname (SM_TEMPLATE * template_);
static const char *candidate_source_name (SM_TEMPLATE * template_,
					  SM_CANDIDATE * candidate);
static int find_superclass (DB_OBJECT * classop, SM_TEMPLATE * temp,
			    DB_OBJECT * super);
static DOMAIN_COMP compare_domains (TP_DOMAIN * d1, TP_DOMAIN * d2);
static SM_METHOD_ARGUMENT *find_argument (SM_METHOD_SIGNATURE * sig,
					  int argnum);
static DOMAIN_COMP compare_argument_domains (SM_METHOD * m1, SM_METHOD * m2);
static DOMAIN_COMP compare_component_domains (SM_COMPONENT * c1,
					      SM_COMPONENT * c2);
static SM_CANDIDATE *make_candidate_from_component (SM_COMPONENT * comp,
						    MOP source);
static void free_candidates (SM_CANDIDATE * candidates);
static SM_CANDIDATE *prune_candidate (SM_CANDIDATE ** clist_pointer);
static void add_candidate (SM_CANDIDATE ** candlist, SM_COMPONENT * comp,
			   int order, MOP source,
			   SM_RESOLUTION * resolutions);
static SM_COMPONENT *make_component_from_candidate (MOP classop,
						    SM_CANDIDATE * cand);
static SM_CANDIDATE *get_candidates (SM_TEMPLATE * def, SM_TEMPLATE * flat,
				     SM_NAME_SPACE name_space);
static int check_attribute_method_overlap (SM_TEMPLATE * template_,
					   SM_CANDIDATE * candidates);
static int check_alias_conflict (SM_TEMPLATE * template_,
				 SM_CANDIDATE * candidates);
static int check_alias_domains (SM_TEMPLATE * template_,
				SM_CANDIDATE * candidates,
				SM_CANDIDATE ** most_specific);
static void auto_resolve_conflict (SM_CANDIDATE * candidate,
				   SM_RESOLUTION ** resolutions,
				   SM_NAME_SPACE resspace);
static int resolve_candidates (SM_TEMPLATE * template_,
			       SM_CANDIDATE * candidates,
			       int auto_resolve,
			       SM_CANDIDATE ** winner_return);
static void insert_attribute (SM_ATTRIBUTE ** attlist, SM_ATTRIBUTE * att);
static void insert_method (SM_METHOD ** methlist, SM_METHOD * method);
static int flatten_components (SM_TEMPLATE * def, SM_TEMPLATE * flat,
			       SM_NAME_SPACE name_space, int auto_res);
static int flatten_method_files (SM_TEMPLATE * def, SM_TEMPLATE * flat);
static int flatten_query_spec_lists (SM_TEMPLATE * def, SM_TEMPLATE * flat);
static void filter_component_resolutions (SM_TEMPLATE * template_,
					  const char *name,
					  SM_NAME_SPACE resspace);
static void remove_shadowed_resolutions (SM_TEMPLATE * original,
					 SM_TEMPLATE * flat);
static void filter_reslist (SM_RESOLUTION ** reslist, MOP deleted_class);
static int check_invalid_resolutions (SM_TEMPLATE * template_,
				      SM_RESOLUTION ** resolutions,
				      SM_RESOLUTION * original_list);
static int filter_resolutions (SM_TEMPLATE * def, SM_TEMPLATE * flat,
			       MOP deleted_class);
static SM_ATTRIBUTE *find_matching_att (SM_ATTRIBUTE * list,
					SM_ATTRIBUTE * att, int idmatch);
static void retain_former_ids (SM_TEMPLATE * flat);
static int flatten_trigger_cache (SM_TEMPLATE * def, SM_TEMPLATE * flat);
static int flatten_properties (SM_TEMPLATE * def, SM_TEMPLATE * flat);
static int flatten_template (SM_TEMPLATE * def, MOP deleted_class,
			     SM_TEMPLATE ** flatp, int auto_res);
static void assign_attribute_id (SM_CLASS * class_, SM_ATTRIBUTE * att,
				 int class_attribute);
static void assign_method_id (SM_CLASS * class_, SM_METHOD * method,
			      bool class_method);
static SM_ATTRIBUTE *order_atts_by_alignment (SM_ATTRIBUTE * atts);
static int build_storage_order (SM_CLASS * class_, SM_TEMPLATE * flat);
static void fixup_component_classes (MOP classop, SM_TEMPLATE * flat);
static void fixup_self_domain (TP_DOMAIN * domain, MOP self);
static void fixup_method_self_domains (SM_METHOD * meth, MOP self);
static void fixup_attribute_self_domain (SM_ATTRIBUTE * att, MOP self);
static void fixup_self_reference_domains (MOP classop, SM_TEMPLATE * flat);
static TP_DOMAIN *construct_index_key_domain (int n_atts,
					      SM_ATTRIBUTE ** atts,
					      const int *asc_desc,
					      const int *prefix_lengths);
static int collect_hier_class_info (MOP classop, DB_OBJLIST * subclasses,
				    const char *constraint_name,
				    int reverse,
				    int *n_classes,
				    int n_attrs, OID * oids,
				    int *attr_ids, HFID * hfids);
static int allocate_index (MOP classop, SM_CLASS * class_,
			   DB_OBJLIST * subclasses,
			   SM_ATTRIBUTE ** attrs, const int *asc_desc,
			   const int *attrs_prefix_length,
			   int unique, int reverse,
			   const char *constraint_name, BTID * index,
			   OID * fk_refcls_oid, BTID * fk_refcls_pk_btid,
			   int cache_attr_id, const char *fk_name);
static int deallocate_index (SM_CLASS_CONSTRAINT * cons, BTID * index,
			     const char *node_name);
static int rem_class_from_index (OID * oid, BTID * index, HFID * heap);
static int build_fk_obj_cache (MOP classop, SM_CLASS * class_,
			       SM_ATTRIBUTE ** key_attrs,
			       const int *asc_desc, OID * pk_cls_oid,
			       BTID * pk_btid, int cache_attr_id,
			       char *fk_name);
static int update_foreign_key_ref (MOP ref_clsop,
				   SM_FOREIGN_KEY_INFO * fk_info);
static int allocate_unique_constraint (MOP classop, SM_CLASS * class_,
				       SM_CLASS_CONSTRAINT * con,
				       DB_OBJLIST * subclasses);
static int allocate_foreign_key (MOP classop, SM_CLASS * class_,
				 SM_CLASS_CONSTRAINT * con,
				 bool * recache_cls_cons);
static int allocate_disk_structure_helper (MOP classop, SM_CLASS * class_,
					   SM_CLASS_CONSTRAINT * con,
					   DB_OBJLIST * subclasses,
					   bool * recache_cls_cons);
static int allocate_disk_structures (MOP classop, SM_CLASS * class_,
				     DB_OBJLIST * subclasses);
static int allocate_disk_structures_by_node (MOP classop, SM_CLASS * class_,
					     DB_OBJLIST * subclasses);
static int drop_foreign_key_ref (MOP classop,
				 SM_CLASS_CONSTRAINT * flat_cons,
				 SM_CLASS_CONSTRAINT * cons);
static bool is_index_owner (MOP classop, SM_CLASS_CONSTRAINT * con);
static int inherit_constraint (MOP classop, SM_CLASS_CONSTRAINT * con);
static int transfer_disk_structures (MOP classop, SM_CLASS * class_,
				     SM_TEMPLATE * flat);
static void save_previous_value (SM_ATTRIBUTE * old, SM_ATTRIBUTE * new_);
static void check_inherited_attributes (MOP classmop, SM_CLASS * class_,
					SM_TEMPLATE * flat);
static void invalidate_unused_triggers (MOP class_mop, SM_CLASS * class_,
					SM_TEMPLATE * flat);
static int install_new_representation (MOP classop, SM_CLASS * class_,
				       SM_TEMPLATE * flat);
static int lock_supers (SM_TEMPLATE * def, DB_OBJLIST * current,
			DB_OBJLIST ** oldlist, DB_OBJLIST ** newlist);
static int update_supers (MOP classop, DB_OBJLIST * oldsupers,
			  DB_OBJLIST * newsupers);
static int lock_supers_drop (DB_OBJLIST * supers);
static int update_supers_drop (MOP classop, DB_OBJLIST * supers);
static int lock_subclasses_internal (SM_TEMPLATE * def, MOP op,
				     DB_OBJLIST * newsupers,
				     DB_OBJLIST ** newsubs);
static int lock_subclasses (SM_TEMPLATE * def, DB_OBJLIST * newsupers,
			    DB_OBJLIST * cursubs, DB_OBJLIST ** newsubs);
static int check_catalog_space (MOP classmop, SM_CLASS * class_);
static int flatten_subclasses (DB_OBJLIST * subclasses, MOP deleted_class);
static void abort_subclasses (DB_OBJLIST * subclasses);
static int update_subclasses (DB_OBJLIST * subclasses);
static int lockhint_subclasses (SM_TEMPLATE * temp, SM_CLASS * class_);
static int update_class (SM_TEMPLATE * template_, MOP * classmop,
			 int auto_res);
static void remove_class_triggers (MOP classop, SM_CLASS * class_);
static int sm_exist_index (MOP classop, const char *idxname, BTID * btid);
static char *sm_default_constraint_name (const char *class_name,
					 DB_CONSTRAINT_TYPE type,
					 const char **att_names,
					 const int *asc_desc);


static int sm_drop_index (MOP classop, const char *constraint_name);

static const char *sm_locate_method_file (SM_CLASS * class_,
					  const char *function);

static void sm_method_final ();

static DB_OBJLIST *sm_get_all_objects (DB_OBJECT * op);
static TP_DOMAIN *sm_get_set_domain (MOP classop, int att_id);

static int sm_check_index_exist (MOP classop,
				 char **out_shared_cons_name,
				 DB_CONSTRAINT_TYPE constraint_type,
				 const char *constraint_name,
				 const char **att_names, const int *asc_desc);

static void sm_reset_descriptors (MOP class_);

static bool sm_is_possible_to_recreate_constraint (MOP class_mop,
						   const SM_CLASS *
						   const class_,
						   const SM_CLASS_CONSTRAINT *
						   const constraint);

static int save_constraint_info (SM_CONSTRAINT_INFO ** save_info,
				 const SM_CLASS_CONSTRAINT * const c);
static void free_constraint_info (SM_CONSTRAINT_INFO ** save_info);
static int sm_truncate_using_delete (MOP class_mop);
static void sm_trans_to_proxy (OID * real_oid, SM_CLASS * class_);
static void sm_trans_to_global (SM_CLASS * class_);
#if 0
static int sm_truncate_using_destroy_heap (MOP class_mop);
#endif

#if defined(CUBRID_DEBUG)
static void sm_print (MOP classmop);
#endif

#if defined(ENABLE_UNUSED_FUNCTION)
static DB_OBJLIST *sm_query_lock (MOP classop, DB_OBJLIST * exceptions,
				  int only, int update);
static DB_OBJLIST *sm_get_all_classes (int external_list);
static DB_OBJLIST *sm_get_base_classes (int external_list);
static const char *sm_get_class_name_internal (MOP op, bool return_null);
static const char *sm_get_class_name (MOP op);
static const char *sm_get_class_name_not_null (MOP op);
static int sm_update_trigger_cache (DB_OBJECT * class_,
				    const char *attribute,
				    int class_attribute, void *cache);
static const char *sc_current_schema_name (void);
static int sm_object_disk_size (MOP op);
static int sm_has_constraint (MOBJ classobj, SM_ATTRIBUTE_FLAG constraint);
static int sm_get_att_domain (MOP op, const char *name, TP_DOMAIN ** domain);
static const char *sm_type_name (DB_TYPE id);
#endif

/*
 * sc_set_current_schema()
 *      return: NO_ERROR if successful
 *              ER_FAILED if any problem extracting name from authorization
 *
 *  user(in) : MOP for authorization (user)
 *
 * Note :
 *    This function is temporary kludge to allow initial implementation
 *    of schema names.  It is to be called from just one place: AU_SET_USER.
 *    Entry level SQL specifies that a schema name is equal to the
 *    <authorization user name>, so this function simply extracts the user
 *    name from the input argument, makes it lower case, and uses that name
 *    as the schema name.
 *
 *
 */

int
sc_set_current_schema (MOP user)
{
  int error = ER_FAILED;
  char *wsp_user_name;

  Current_Schema.name[0] = '\0';
  Current_Schema.owner = user;
  wsp_user_name = au_get_user_name (user);

  if (wsp_user_name == NULL)
    {
      return error;
    }

  /* As near as I can tell, this is the most generalized  */
  /* case conversion function on our system.  If it's not */
  /* the most general, change this code accordingly.      */
  if (intl_mbs_lower (wsp_user_name, Current_Schema.name) == 0)
    {
      /* Last time I looked, intl_mbs_lower always returns 0.      */
      /* However, it does malloc without checking result, so  */
      /* perhaps someday it might return an error.            */
      error = NO_ERROR;
    }
  ws_free_string (wsp_user_name);

  /* If there's any error, it's not obvious what can be done about it here. */
  /* Probably some code needs to be fixed in the caller: AU_SET_USER        */
  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sc_current_schema_name() - Returns current schema name which is
 *                            the default qualifier for otherwise
 *                            unqualified class/vclass names
 *      return: pointer to current schema name
 *
 */

static const char *
sc_current_schema_name (void)
{
  return (const char *) &(Current_Schema.name);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sm_add_static_method() - Adds an element to the static link table.
 *    The name argument and the name of the function pointed to
 *    are usually the same but this is not mandatory.
 *   return: none
 *   name(in): method function name
 *   function(in): method function pointer
 */
void
sm_add_static_method (const char *name, void (*function) ())
{
  STATIC_METHOD *m, *found, *new_;

  if (name == NULL)
    {
      return;
    }

  found = NULL;
  for (m = Static_method_table; m != NULL && found == NULL; m = m->next)
    {
      if (strcmp (m->name, name) == 0)
	{
	  found = m;
	}
    }
  /* if found, assume we just want to change the function */
  if (found != NULL)
    {
      found->function = function;
    }
  else
    {
      new_ = (STATIC_METHOD *) malloc (sizeof (STATIC_METHOD));
      if (new_ == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (STATIC_METHOD));
	  return;
	}

      new_->next = Static_method_table;
      Static_method_table = new_;
      new_->function = function;

      new_->name = (char *) malloc (strlen (name) + 1);
      if (new_->name == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, strlen (name) + 1);
	  free (new_);
	  return;
	}

      strcpy ((char *) new_->name, name);
    }
}

/*
 * sm_delete_static_method() - Removes static link information for
 * 				the named method function
 *   return: none
 *   name(in): method function name
 */
void
sm_delete_static_method (const char *name)
{
  STATIC_METHOD *m, *prev, *found;

  found = NULL;
  prev = NULL;

  for (m = Static_method_table; m != NULL && found == NULL; m = m->next)
    {
      if (strcmp (m->name, name) == 0)
	{
	  found = m;
	}
      else
	{
	  prev = m;
	}
    }

  if (found == NULL)
    {
      return;
    }

  if (prev == NULL)
    {
      Static_method_table = found->next;
    }
  else
    {
      prev->next = found->next;
    }

  free_and_init (found->name);
  free_and_init (found);
}

/*
 * sm_flush_static_methods() - Clear the static method table
 */

void
sm_flush_static_methods ()
{
  STATIC_METHOD *m, *next;

  for (m = Static_method_table, next = NULL; m != NULL; m = next)
    {
      next = m->next;
      free_and_init (m->name);
      free_and_init (m);
    }

  Static_method_table = NULL;
}

/*
 * sm_find_static_method() - Searches the global static method list for
 *                            the named function
 *  return: static method structure
 *  name(in): method function name
 */

static STATIC_METHOD *
sm_find_static_method (const char *name)
{
  STATIC_METHOD *m, *found;

  found = NULL;

  m = Static_method_table;

  while (m != NULL && found == NULL)
    {
      if (strcmp (m->name, name) == 0)
	{
	  found = m;
	}
      m = m->next;
    }

  return found;
}

/*
 * sm_count_tokens() - Work function for split_loader_commands.
 *    A token is defined as any string of characters separated by
 *    whitespace.  Calculate the number of tokens in the string and the
 *    maximum length of all tokens.
 *
 *   return: number of tokens in the command string
 *   string(in): loader command string
 *   maxcharp(out): returned size of maximum token length
 */

static int
sm_count_tokens (const char *string, int *maxcharp)
{
  int tokens, chars, maxchars, i;

  tokens = 0;
  maxchars = 0;

  if (string == NULL)
    {
      return (tokens);
    }

  for (i = 0; i < (int) strlen (string); i++)
    {
      if (char_isspace (string[i]))
	{
	  continue;
	}
      tokens++;

      for (chars = 0;
	   i < (int) strlen (string) && !char_isspace (string[i]);
	   i++, chars++)
	;
      if (chars > maxchars)
	{
	  maxchars = chars;
	}
    }

  if (maxcharp != NULL)
    {
      *maxcharp = maxchars;
    }

  return tokens;
}

/*
 * sm_split_loader_commands() - Takes a string containing loader commands
 *    separated by whitespace and creates an argv style array with
 *    NULL termination. This is required for the dynamic loader.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   string(in): loader command string
 *   command_ptr(out): argv style array with loader commands
 */

static int
sm_split_loader_commands (const char *string, const char ***command_p)
{
  int error = NO_ERROR;
  int tokens, maxchars, i, j;
  char *buf, *ptr;
  const char *new_;
  char **commands;

  commands = NULL;
  tokens = sm_count_tokens (string, &maxchars);
  if (!tokens)
    {
      goto end;
    }

  buf = (char *) db_ws_alloc (sizeof (char) * (maxchars + 1));
  if (buf == NULL)
    {
      error = er_errid ();
      goto end;
    }

  commands = (char **) db_ws_alloc (sizeof (char *) * (tokens + 1));
  if (commands == NULL)
    {
      error = er_errid ();
      db_ws_free (buf);
      goto end;
    }

  ptr = (char *) string;
  for (i = 0; i < tokens && error == NO_ERROR; i++)
    {
      for (; *ptr != '\0' && char_isspace (*ptr); ptr++)
	;

      for (j = 0; *ptr != '\0' && !char_isspace (*ptr); ptr++, j++)
	{
	  buf[j] = *ptr;
	}
      buf[j] = '\0';

      new_ = ws_copy_string (buf);
      if (new_ != NULL)
	{
	  commands[i] = (char *) new_;
	}
      else
	{
	  error = er_errid ();
	  db_ws_free (commands);
	  db_ws_free (buf);

	  return error;
	}
    }

  commands[i] = NULL;
  db_ws_free (buf);

end:
  if (error == NO_ERROR)
    {
      *command_p = (const char **) commands;
    }

  return error;
}

/*
 * sm_free_loader_commands() - Frees an array of loader commands created with
 * 			    split_loader_commands()
 *   return: none
 *   commands(in): argv style loader command array
 */

static void
sm_free_loader_commands (char **commands)
{
  int i;

  if (commands != NULL)
    {
      for (i = 0; commands[i] != NULL; i++)
	{
	  db_ws_free ((char *) commands[i]);
	}
      db_ws_free (commands);
    }
}

/* STATIC LINKING */
/*
 * sm_free_method_links() - Free a list of temporary method link structures
 *    after dynamic linking has finished
 *   return: none
 *   links(in): list of method link structures
 */

static void
sm_free_method_links (METHOD_LINK * links)
{
  METHOD_LINK *link, *next = NULL;

  for (link = links; link != NULL; link = next)
    {
      next = link->next;
      db_ws_free (link);
    }
}

/*
 * sm_link_static_method() - Attempt to link a single method using the
 *    static method table. If a static link could not be made, construct
 *    and return a method link structure that will be used later during
 *    dynamic linking.
 *    If the method could be statically linked, set up the function
 *    pointer in the method structure and return NULL.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   method(in/out): schema method structure
 *   link_ptr(out): schema method structure
 */

static int
sm_link_static_method (SM_METHOD * method, METHOD_LINK ** link_ptr)
{
  int error = NO_ERROR;
  STATIC_METHOD *m;
  METHOD_LINK *link;

  link = NULL;

  if (method->signatures == NULL)
    {
      goto end;
    }

  m = sm_find_static_method (method->signatures->function_name);
  if (m != NULL)
    {
      /* should check for reasonable type */
      method->signatures->function = (METHOD_FUNCTION) m->function;
      /* put it in the cache as well */
      method->function = (METHOD_FUNCTION) m->function;
    }
  else
    {
      /* couldn't statically link, build dynamic link state */
      link = (METHOD_LINK *) db_ws_alloc (sizeof (METHOD_LINK));
      if (link == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  link->next = NULL;
	  link->method = method;
	  link->namelist_index = -1;
	}
    }
end:
  if (error == NO_ERROR)
    {
      *link_ptr = link;
    }

  return error;
}

/*
 * sm_link_static_methods() - Attempts to statically link all of the methods
 *    in a class. A METHOD_LINK structure is created for every method that
 *    could not be statically linked and returned in a list.  This list
 *    is used later to dynamically link the remaining methods
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class(in): class with methods to be linked
 *   links_ptr(out): list of method link structures
 */

static int
sm_link_static_methods (SM_CLASS * class_, METHOD_LINK ** links_ptr)
{
  int error = NO_ERROR;
  METHOD_LINK *links, *link;
  SM_METHOD *method;

  links = NULL;

  for (method = class_->methods; method != NULL && error == NO_ERROR;
       method = (SM_METHOD *) method->header.next)
    {
      error = sm_link_static_method (method, &link);
      if (error == NO_ERROR)
	{
	  if (link != NULL)
	    {
	      link->next = links;
	      links = link;
	    }
	}
    }
  for (method = class_->class_methods; method != NULL && error == NO_ERROR;
       method = (SM_METHOD *) method->header.next)
    {
      error = sm_link_static_method (method, &link);
      if (error == NO_ERROR)
	{
	  if (link != NULL)
	    {
	      link->next = links;
	      links = link;
	    }
	}
    }

  if (error == NO_ERROR)
    {
      *links_ptr = links;
    }

  return error;
}

/* DYNAMIC LINKING */
/*
 * sm_expand_method_files() - This is called prior to dynamic linking to go
 *    through all the method files for a class and expand any environment
 *    variables that may be included in the file pathnames.
 *    This expansion is delayed until link time so that changing the values of
 *    the env variables allow site specific customization of behavior.
 *    When finished, the expanded_name field in the file structures will
 *    be non-NULL if expansions were performed or will be NULL if
 *    no expansion was necessary.  If no error code is returned,
 *    assume all expansions were performed.  If the expansion_name field
 *    is already set, free it and recalculate the expansion.
 *
 *    Changed to automatically supply method file extensions if they have
 *    not been specified in the schema.  This is useful when databases
 *    are used in a heterogeneous environment, eliminating the need to
 *    have multiple versions of the schema for each platform.  This will
 *    handle the most common cases, for really radical platforms, a more
 *    general mechanism may be necessary
 *   return: NO_ERROR on success, non-zero for ERROR
 *   files(in/out): list of method files
 */
static int
sm_expand_method_files (SM_METHOD_FILE * files)
{
  char filebuf[PATH_MAX];
  int error = NO_ERROR;
  SM_METHOD_FILE *f;

  for (f = files; f != NULL && error == NO_ERROR; f = f->next)
    {
      if (f->expanded_name != NULL)
	{
	  ws_free_string (f->expanded_name);
	  f->expanded_name = NULL;
	}
      if (envvar_expand (f->name, filebuf, PATH_MAX) == NO_ERROR)
	{
	  /* check for automatic extensions, this is determined by checking to see
	   * if there are no '.' characters in the name, could be more complicated.
	   * Use intl_mbs_chr just in case we need to be dealing with wide strings.
	   */
	  if (intl_mbs_chr (filebuf, WC_PERIOD) == NULL)
	    {
	      strcat (filebuf, method_file_extension);
	    }

	  /* If the name we've been manipulating is different then the original name,
	   * copy it and use it later.
	   */
	  if (strcmp (filebuf, f->name) != 0)
	    {
	      f->expanded_name = ws_copy_string (filebuf);
	      if (f->expanded_name == NULL)
		{
		  error = er_errid ();	/* out of memory */
		}
	    }
	}
      else
	{
	  /* could stop at the first one but just go through them all */
	  error = ER_SM_INVALID_METHOD_ENV;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, filebuf);
	}
    }

  return error;
}

/*
 * sm_build_function_nlist() - Builds an nlist function name array from a list of
 *    method link structures.  The links are filtered so that only unique
 *    names will be in the nlist array.  The links structures are
 *    modified as a side effect so that their namelist_index is set to the
 *    index in the nlist array where the information for that function
 *    will be found.
 *    The nlist array must be freed with free_function_nlist
 *   return: NO_ERROR on success, non-zero for ERROR
 *   links(in): list of method links
 *   nlist_ptr(out): nlist array
 */

#if !defined(WINDOWS)

static int
sm_build_function_nlist (METHOD_LINK * links, struct nlist **nlist_p)
{
  int error = NO_ERROR;
  struct nlist *namelist;
  METHOD_LINK *ml;
  const char **fnames;
  const char *new_;
  int i, nlinks, index;
  char fname[SM_MAX_IDENTIFIER_LENGTH + 2];

  namelist = NULL;
  if (links == NULL)
    {
      goto end;
    }

  /* allocation & initialize an array for building the unique name list */
  nlinks = WS_LIST_LENGTH (links);
  fnames = (const char **) db_ws_alloc (sizeof (char *) * nlinks);
  if (fnames == NULL)
    {
      error = er_errid ();
    }
  else
    {
      for (i = 0; i < nlinks; i++)
	{
	  fnames[i] = NULL;
	}

      /* populate the unique name array */
      index = 0;
      for (ml = links; ml != NULL && error == NO_ERROR; ml = ml->next)
	{
	  ml->namelist_index = -1;
	  if (ml->method->signatures->function_name != NULL)
	    {
	      /* mangle the name as appropriate, sun wants prepended '_', ibm doesn't */
#if defined(sun) && !defined(SOLARIS)
	      sprintf (fname, "_%s", ml->method->signatures->function_name);
#else /* sun && !SOLARIS */
	      sprintf (fname, "%s", ml->method->signatures->function_name);
#endif /* sun && !SOLARIS */
	      /* see if it is already in the nlist array */
	      for (i = 0; i < index && ml->namelist_index == -1; i++)
		{
		  if (strcmp (fname, fnames[i]) == 0)
		    {
		      ml->namelist_index = i;
		    }
		}
	      /* add it if not already there */
	      if (ml->namelist_index == -1)
		{
		  ml->namelist_index = index;
		  new_ = ws_copy_string ((const char *) fname);
		  if (new_ != NULL)
		    {
		      fnames[index++] = new_;
		    }
		  else
		    {
		      error = er_errid ();
		    }
		}
	    }
	}

      if (error == NO_ERROR)
	{
	  /* build an actual nlist structure from the unique name array */
	  namelist =
	    (struct nlist *) db_ws_alloc (sizeof (struct nlist) *
					  (index + 1));
	  if (namelist == NULL)
	    {
	      error = er_errid ();
	    }
	  else
	    {
	      for (i = 0; i < index; i++)
		{
		  namelist[i].n_name = (char *) fnames[i];
		}
	      namelist[index].n_name = NULL;

	    }
	}
      /* don't need this anymore */
      db_ws_free (fnames);
    }
end:
  if (error == NO_ERROR)
    {
      *nlist_p = namelist;
    }

  return error;
}

/*
 * sm_free_function_nlist() - Frees an nlist array that was allocated with
 * 			   build_function_nlist()
 *   return: none
 *   namelist(in): nlist array
 */

static void
sm_free_function_nlist (struct nlist *namelist)
{
  int i;

  if (namelist != NULL)
    {
      for (i = 0; namelist[i].n_name != NULL; i++)
	{
	  db_ws_free (namelist[i].n_name);
	}
      db_ws_free (namelist);
    }
}
#endif /* !WINDOWS */

/*
 * sm_link_dynamic_methods() - Call the dynamic linker to resolve any function
 *    references that could not be statically linked.  The static linking phase
 *    produces a list of METHOD_LINK structures for the methods that could
 *    not be linked.  We use this list here to build the control structures
 *    for the dynamic loader.
 *    The files array has the names of the method files specified in the
 *    schema.  The commands array has the loader commands.
 *    This can be used to link methods for several classes
 *   return: NO_ERROR on success, non-zero for ERROR
 *   links(in/out): list of method link structures
 *   files(in): array of method files (NULL terminated)
 *   commands(in): array of loader commands (NULL terminated)
 */

#if defined (sun) || defined(SOLARIS) || defined(LINUX)
#if defined(SOLARIS) || defined(LINUX)
static int
sm_link_dynamic_methods (METHOD_LINK * links, const char **files)
#else /* SOLARIS || LINUX */
static int
sm_link_dynamic_methods (METHOD_LINK * links,
			 const char **files, const char **commands)
#endif				/* SOLARIS || LINUX */
{
  int error = NO_ERROR;
  METHOD_LINK *ml;
  struct nlist *namelist, *nl;
  const char *msg;
  int status;

  error = sm_build_function_nlist (links, &namelist);
  if (error == NO_ERROR && namelist != NULL)
    {
      /* invoke the linker */
#if defined(SOLARIS) || defined(LINUX)
      status = dl_load_object_module (files, &msg);
#else /* SOLARIS || LINUX */
      status = dl_load_object_module (files, &msg, commands);
#endif /* SOLARIS || LINUX */
      if (status)
	{
	  error = er_errid ();
	}
      else
	{
	  /* resolve functions */
	  status = dl_resolve_object_symbol (namelist);
	  if (status == -1)
	    {
	      error = er_errid ();
	    }
	  else
	    {
	      /* what does this accomplish ? */
	      if (status)
		{
		  error = ER_SM_UNRESOLVED_METHODS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, status);
		}

	      /* now link the methods, signal and return an error when one is
	         encountered but go ahead and try to link whatever is there */
	      for (ml = links; ml != NULL; ml = ml->next)
		{
		  nl = &namelist[ml->namelist_index];
		  if (nl->n_type == (N_TEXT | N_EXT))
		    {
		      ml->method->signatures->function =
			(METHOD_FUNCTION) nl->n_value;
		      ml->method->function = (METHOD_FUNCTION) nl->n_value;
		    }
		  else
		    {
		      error = ER_SM_UNRESOLVED_METHOD;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			      nl->n_name);
		    }
		}
	    }
	}
      sm_free_function_nlist (namelist);
    }

  return error;
}

#elif defined (_AIX)
static int
sm_link_dynamic_methods (METHOD_LINK * links,
			 const char **files, const char **commands)
{
  int error = NO_ERROR;
  METHOD_LINK *ml;
  struct nlist *nl, *namelist;
  const char *msg;
  int status;

  error = sm_build_function_nlist (links, &namelist);
  if (error == NO_ERROR && namelist != NULL)
    {
      /* invoke the linker and resolve functions */
      status = dl_load_and_resolve (files, &msg, commands, namelist);

      if (status == -1)
	{
	  error = er_errid ();
	}
      else
	{
	  if (status)
	    {
	      error = ER_SM_UNRESOLVED_METHODS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_SM_UNRESOLVED_METHODS, 1, status);
	    }

	  /* go ahead and try to resolve the ones that exist even if there were
	     errors on some others */
	  for (ml = links; ml != NULL; ml = ml->next)
	    {
	      nl = &namelist[ml->namelist_index];
	      ml->method->signatures->function =
		(METHOD_FUNCTION) nl->n_value;
	      ml->method->function = (METHOD_FUNCTION) nl->n_value;
	      if (nl->n_value)
		{
		  ml->method->signatures->function =
		    (METHOD_FUNCTION) nl->n_value;
		  ml->method->function = (METHOD_FUNCTION) nl->n_value;
		}
	      else
		{
		  error = ER_SM_UNRESOLVED_METHOD;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			  nl->n_name);
		}
	    }
	}
      db_ws_free (namelist);
    }

  return error;
}

#elif defined(HPUX)
static int
sm_link_dynamic_methods (METHOD_LINK * links,
			 const char **files, const char **commands)
{
  int error = NO_ERROR;
  METHOD_LINK *ml;
  struct nlist *namelist, *nl;
  const char *msg;
  int status;

  error = sm_build_function_nlist (links, &namelist);
  if (error == NO_ERROR && namelist != NULL)
    {

      /* invoke the linker */
      status = dl_load_object_module (files, &msg, commands);
      if (status)
	{
	  error = er_errid ();
	}
      else
	{
	  /* resolve functions */
	  status = dl_resolve_object_symbol (namelist);
	  if (status == -1)
	    {
	      error = er_errid ();
	    }
	  else
	    {
	      /* what does this accomplish ? */
	      if (status)
		{
		  error = ER_SM_UNRESOLVED_METHODS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, status);
		}

	      /* now link the methods, signal and return an error when one is
	         encountered but go ahead and try to link whatever is there */
	      for (ml = links; ml != NULL; ml = ml->next)
		{
		  nl = &namelist[ml->namelist_index];
		  if (nl->n_type == (ST_ENTRY))
		    {
		      ml->method->signatures->function =
			(METHOD_FUNCTION) nl->n_value;
		      ml->method->function = (METHOD_FUNCTION) nl->n_value;
		    }
		  else
		    {
		      error = ER_SM_UNRESOLVED_METHOD;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			      nl->n_name);
		    }
		}
	    }
	}
      sm_free_function_nlist (namelist);
    }

  return error;
}
#elif defined(WINDOWS)

/* DYNAMIC LINK LIBRARY MAINTENANCE */
/* Structure that maintains a global list of DLL's that have been opened */
typedef struct pc_dll
{
  struct pc_dll *next;
  HINSTANCE handle;
  char *name;
} PC_DLL;

/* Global list of opened DLL's */
static PC_DLL *pc_dll_list = NULL;

/*
 * load_dll() - This returns a Windows library handle for the named DLL.
 *    It will first look on our list of opened libraries, if one is not found,
 *    it asks windows to load it and adds it to the list.
 *    Only called by the PC version of link_dynamic_methods
 *   return: library handle
 *   name(in): library name
 */

static HINSTANCE
load_dll (const char *name)
{
  PC_DLL *dll;
  HINSTANCE handle;

  handle = NULL;

  /* first see if we've already loaded this */
  for (dll = pc_dll_list; dll != NULL && strcmp (name, dll->name) != 0;
       dll = dll->next);

  if (dll != NULL)
    {
      handle = dll->handle;
    }
  else
    {
      /* never been loaded, ask windows to go find it */

      handle = LoadLibrary (name);
      if (handle != NULL)
	{
	  /* successfully loaded, add to the list */

	  dll = (PC_DLL *) malloc (sizeof (PC_DLL) + strlen (name) + 2);
	  if (dll == NULL)
	    {
	      /* memory problems */
	      FreeLibrary (handle);
	      handle = NULL;
	    }
	  else
	    {
	      dll->next = pc_dll_list;
	      pc_dll_list = dll;
	      dll->handle = handle;
	      dll->name = (char *) dll + sizeof (PC_DLL);
	      strcpy (dll->name, name);
	    }
	}
    }

  return handle;
}

/*
 * sm_method_final() - Called by sm_final() to clean up state required by
 *    dynamic linking. This goes through the global DLL list and closes all
 *    the DLL's we used during this session
 */

void
sm_method_final ()
{
  PC_DLL *dll, *next;

  for (dll = pc_dll_list, next = NULL; dll != NULL; dll = next)
    {
      next = dll->next;
      FreeLibrary (dll->handle);
      free_and_init (dll);
    }

  pc_dll_list = NULL;
}

/*
 * link_dynamic_methods()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   links(in):
 *   files(in):
 *   commands(in):
 */
static int
sm_link_dynamic_methods (METHOD_LINK * links,
			 const char **files, const char **commands)
{
  char filebuf[PATH_MAX];
  char fname[SM_MAX_IDENTIFIER_LENGTH + 2];
  int error = NO_ERROR;
  METHOD_LINK *ml;
  const char *file;
  HINSTANCE libhandle;
  FARPROC func;
  int i, j;

  if (links != NULL)
    {
      /* Load the DLL associated with each file in the files array and try
         to locate each method in them.  If there are errors loading a
         DLL, could continue assuming that Windows has had a chance to
         popup a message window.
       */
      for (i = 0; files[i] != NULL && error == NO_ERROR; i++)
	{
	  file = files[i];
	  /* Should have a "method name too long" error but I don't want to
	     introduce one right now.  If we have problems with a particular
	     DLL file, just ignore it and attempt to get the methods from
	     the other files.
	   */
	  if (strlen (file) + 3 < PATH_MAX)
	    {
	      /* massage the file extension so that it has .dll */
	      strcpy (filebuf, file);

	      for (j = strlen (file) - 1; j > 0 && filebuf[j] != '.'; j--)
		;

	      if (j > 0)
		{
		  strcpy (&filebuf[j], ".dll");
		}
	      else
		{
		  /* its a file without an extension, hmm, assume that it needs .dll
		     appended to the end */
		  strcat (filebuf, ".dll");
		}

	      /* Ask Windows to open the DLL, example for GetProcAddress uses
	         SetErrorMode to turn off the "file not found" boxes,
	         we want these though.
	       */
	      libhandle = load_dll (filebuf);
	      if (libhandle != NULL)
		{
		  /* look for each unresolved method in this file */
		  for (ml = links; ml != NULL; ml = ml->next)
		    {
		      /* Formerly only did the GetProcAddress if the signature's function
		       * pointer was NULL, this prevents us from getting new addresses
		       * if the DLL changes.  Hopefully this isn't very expensive.
		       * if (ml->method->signatures->function == NULL) {
		       */
		      /* its possible that the name they've given for the function
		         name matches exactly the name in the export list of
		         the DLL, in that case, always try the given name first,
		         if that fails, assume that they've left off the initial
		         underscore necessary for DLL function references and
		         add one automatically. */
		      strcpy (fname, ml->method->signatures->function_name);
		      func = GetProcAddress (libhandle, fname);
		      if (func == NULL)
			{
			  /* before giving up, try prefixing an underscore */
			  strcpy (fname, "_");
			  strcat (fname,
				  ml->method->signatures->function_name);
			  func = GetProcAddress (libhandle, fname);
			}
		      if (func != NULL)
			{
			  /* found one */
			  ml->method->signatures->function =
			    (METHOD_FUNCTION) func;
			  ml->method->function = (METHOD_FUNCTION) func;
			}
		    }
		}
	      /* else, could abort now but lets look in the other files to see
	         if our methods all get resolved */
	    }
	}

      /* now all the files have been processed, check to see if we couldn't resolve
         any methods */

      for (ml = links; ml != NULL && error == NO_ERROR; ml = ml->next)
	{
	  if (ml->method->function == NULL)
	    {
	      error = ER_SM_UNRESOLVED_METHOD;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		      ml->method->header.name);
	    }
	}

    }

  return (error);
}

#else /*  sun || SOLARIS || LINUX */
#error "Unknown machine type for link_dynamic_methods"
#endif /*  sun || SOLARIS || LINUX */

/*
 * sm_file_extension() - Hack to check file extensions, used by dynamic_link_class
 *    to sort .a files apart from the method object files
 *   return: non-zero if the path has the given file extension
 *   path(in):
 *   ext(in):
 */

static int
sm_file_extension (const char *path, const char *ext)
{
  DB_C_INT plen, elen;

  plen = strlen (path);
  elen = strlen (ext);

  return (plen > elen) && (strcmp (&(path[plen - elen]), ext) == 0);
}

/*
 * sm_dynamic_link_class() - Perform dynamic linking for a class.
 *    Tries to resolve the methods in the METHOD_LINK list which could not be
 *    resolved through static linking.
 *    Work function for sm_link_methods & sm_link_method.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class_(in/out): class requiring linking
 *   links(in): unresolved method links
 */

static int
sm_dynamic_link_class (SM_CLASS * class_, METHOD_LINK * links)
{
  int error = NO_ERROR;
  SM_METHOD_FILE *files, *file;
  char **names, **sorted_names, **commands;
  int i, nfiles, psn;

  if (links == NULL)
    {
      return error;
    }

  files = class_->method_files;
  nfiles = ws_list_length ((DB_LIST *) files);

  names = (char **) db_ws_alloc (sizeof (char *) * (nfiles + 1));
  if (names == NULL)
    {
      error = er_errid ();
      return error;
    }

  sorted_names = (char **) db_ws_alloc (sizeof (char *) * (nfiles + 1));
  if (sorted_names == NULL)
    {
      error = er_errid ();
      db_ws_free (names);
      return error;
    }

  error = sm_expand_method_files (files);
  if (error != NO_ERROR)
    {
      db_ws_free (sorted_names);
      db_ws_free (names);
      return (error);

    }
  for (file = files, i = 0; file != NULL; file = file->next, i++)
    {
      if (file->expanded_name != NULL)
	{
	  names[i] = (char *) file->expanded_name;
	}
      else
	{
	  names[i] = (char *) file->name;
	}
    }
  names[nfiles] = NULL;

  /* Hack, if we have any Unix library (.a) files in the file list,
   * put them at the end.  Useful if libraries are used for method
   * file support, particularly, when inherited.  Try to keep the files
   * int the same order otherwise.
   */
  psn = 0;
  for (i = 0; i < nfiles; i++)
    {
      if (!sm_file_extension (names[i], ".a"))
	{
	  sorted_names[psn++] = names[i];
	}
    }
  for (i = 0; i < nfiles; i++)
    {
      if (sm_file_extension (names[i], ".a"))
	{
	  sorted_names[psn++] = names[i];
	}
    }
  sorted_names[nfiles] = NULL;
  error = sm_split_loader_commands (class_->loader_commands,
				    (const char ***) &commands);
  if (error == NO_ERROR)
    {
#if defined(SOLARIS) || defined(LINUX)
      error = sm_link_dynamic_methods (links, (const char **) sorted_names);
#else /* SOLARIS || LINUX */
      error = sm_link_dynamic_methods (links, sorted_names, commands);
#endif /* SOLARIS || LINUX */
      if (commands != NULL)
	{
	  sm_free_loader_commands (commands);
	}

      /* ONLY set this after we have tried to dynamically link the class */
      if (error == NO_ERROR)
	{
	  class_->methods_loaded = 1;
	}
    }

  db_ws_free (sorted_names);
  db_ws_free (names);

  return error;
}

/*  FUNCTIONS */
/*
 * sm_link_methods() - Links the method functions for a class.
 *    First tries to use static linking and then uses dynamic linking
 *    for the methods that could not be statically linked
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class(in): class with methods to link
 */

static int
sm_link_methods (SM_CLASS * class_)
{
  int error = NO_ERROR;
  METHOD_LINK *links;

  if (class_->methods_loaded)
    {
      return NO_ERROR;
    }

  /* first link through the static table */
  error = sm_link_static_methods (class_, &links);
  if (error == NO_ERROR)
    {
      /* if there are unresolved references, use the dynamic loader */
      if (links != NULL)
	{
	  error = sm_dynamic_link_class (class_, links);
	  sm_free_method_links (links);
	}
    }

  return error;
}

/*
 * sm_link_method() - Link a single method.
 *    This will first try to statically link the method, while we're at it,
 *    statically link all methods.
 *    If the link fails, try dynamic linking.  Note that this is different
 *    than calling sm_link_methods (to link all methods) because it
 *    will only invoke the dynamic loader if the desired method could not
 *    be statically linked.  sm_link_static_methods will dynamic link
 *    if ANY methods in the class could not be statically linked.
 *    Note that this may return an error yet still have linked the
 *    requested function
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class(in): class with method
 *   method(in): method to link
 */

int
sm_link_method (SM_CLASS * class_, SM_METHOD * method)
{
  int error = NO_ERROR;
  METHOD_LINK *links;

  if (class_->methods_loaded)
    {
      return NO_ERROR;
    }

  /* first link through the static table */
  error = sm_link_static_methods (class_, &links);
  if (error == NO_ERROR)
    {
      if (links != NULL)
	{
	  /* only dynamic link if the desired method was not resolved */
	  if (method->function == NULL)
	    {
	      error = sm_dynamic_link_class (class_, links);
	    }
	  sm_free_method_links (links);
	}
    }

  return error;
}

/*
 * sm_force_method_link() - Called to force a method reload for a class.
 *    Note that the class is passed in as an object here
 *   return: NO_ERROR on success, non-zero for ERROR
 *   obj(in): class object
 */

int
sm_force_method_link (MOP obj)
{
  int error = NO_ERROR;
  SM_CLASS *class_;

  if (obj == NULL)
    {
      return NO_ERROR;
    }

  error = au_fetch_class (obj, &class_, AU_FETCH_READ, AU_SELECT);
  if (error == NO_ERROR)
    {
      class_->methods_loaded = 0;
      error = sm_link_methods (class_);
    }

  return error;
}

/*
 * sm_prelink_methods() - Used to link the methods for a set of classes
 *    at one time. Since dynamic linking can be expensive, this avoids repeated
 *    links for each class
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classes(in): list of class objects
 */

int
sm_prelink_methods (DB_OBJLIST * classes)
{
  int error = NO_ERROR;
  DB_OBJLIST *cl;
  SM_METHOD_FILE *f;
  SM_CLASS *class_;
  char **names;
  DB_NAMELIST *filenames, *name;
  int nfiles, i;
  METHOD_LINK *total_links, *links;

  filenames = NULL;
  total_links = NULL;

  /* build link structures for all classes */
  for (cl = classes; cl != NULL && error == NO_ERROR; cl = cl->next)
    {
      /* ignore authorization errors here, what happens if the transaction
         is aborted ??? */
      if (au_fetch_class (cl->op, &class_, AU_FETCH_READ, AU_EXECUTE) !=
	  NO_ERROR)
	{
	  continue;
	}
      /* Ignore this if the class has already been fully linked */

      if (class_->methods_loaded)
	{
	  continue;
	}

      /* first link through the static table */
      error = sm_link_static_methods (class_, &links);
      if (error != NO_ERROR)
	{
	  continue;
	}
      /* if there are unresolved references, use the dynamic loader */
      if (links == NULL)
	{
	  continue;
	}

      error = sm_expand_method_files (class_->method_files);
      if (error != NO_ERROR)
	{
	  continue;
	}

      /* NEED TO BE DETECTING MEMORY ALLOCATION FAILURES IN THE nlist
         LIBRARY FUNCTIONS ! */

      /* add the files for this class */
      for (f = class_->method_files; f != NULL && !error; f = f->next)
	{
	  if (f->expanded_name != NULL)
	    {
	      error = nlist_append (&filenames, f->expanded_name, NULL, NULL);
	    }
	  else
	    {
	      error = nlist_append (&filenames, f->name, NULL, NULL);
	    }
	}

      if (!error)
	{
	  /* put the links on the combined list */
	  WS_LIST_APPEND (&total_links, links);
	}

      /* will need to have a composite list of loader commands !! */
    }

  /* proceed only if we have references that haven't already been statically linked */
  if (error == NO_ERROR && total_links != NULL)
    {
      /* build a name array for dl_load_object_module */
      nfiles = ws_list_length ((DB_LIST *) filenames);
      names = (char **) db_ws_alloc (sizeof (char *) * (nfiles + 1));
      if (names == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  for (i = 0, name = filenames; name != NULL; name = name->next, i++)
	    {
	      names[i] = (char *) name->name;
	    }
	  names[nfiles] = NULL;

	  /* need to have commands here ! */
#if defined(SOLARIS) || defined(LINUX)
	  error =
	    sm_link_dynamic_methods (total_links, (const char **) names);
#else /* SOLARIS || LINUX */
	  error = sm_link_dynamic_methods (total_links, names, NULL);
#endif /* SOLARIS || LINUX */
	  db_ws_free (names);
	}
    }

  /* mark the classes as loaded, don't do this if there were errors */
  if (error == NO_ERROR)
    {
      for (cl = classes; cl != NULL; cl = cl->next)
	{
	  if (au_fetch_class (cl->op, &class_, AU_FETCH_READ, AU_EXECUTE) ==
	      NO_ERROR)
	    {
	      class_->methods_loaded = 1;
	    }
	}
    }

  nlist_free (filenames);
  sm_free_method_links (total_links);

  return error;
}

/*
 * sm_locate_method_file() - Search a class' list of method files and
 *    find which one contains a particular implementation function.
 *    Uses the Sun OS "nlist" facility.  This may not be portable
 *   return: method file name
 *   class(in): class to search
 *   function(in): method function name
 */

const char *
sm_locate_method_file (SM_CLASS * class_, const char *function)
{
  /*
     DO NOT use nlist() because of installation problems.
     - elf library linking error on some Linux platform
   */
  return NULL;
#if 0
#if defined(WINDOWS)
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PC_UNIMPLEMENTED, 1,
	  "sm_locate_method_file");
  return NULL;
#else /* WINDOWS */
  struct nlist nl[2];
  SM_METHFILE *files;
  const char *found;
  int status;
  char fname[SM_MAX_IDENTIFIER_LENGTH + 2];
  const char *filename;

  found = NULL;

  /* machine dependent name mangling */
#if defined(_AIX)
  sprintf (fname, "%s", function);
#else /* _AIX */
  sprintf (fname, "_%s", function);
#endif /* _AIX */

  nl[0].n_name = fname;
  nl[1].n_name = NULL;

  /* if the class hasn't been dynamically linked, expand the method files */
  if (class->methods_loaded ||
      sm_expand_method_files (class->method_files) == NO_ERROR)
    {

      for (files = class->method_files; files != NULL && found == NULL;
	   files = files->next)
	{
	  if (files->expanded_name != NULL)
	    {
	      filename = files->expanded_name;
	    }
	  else
	    {
	      filename = files->name;
	    }

	  status = nlist (filename, &nl[0]);
	  if (nl[0].n_type != 0)
	    {
	      found = filename;
	    }
	}
    }

  return (found);
#endif /* WINDOWS */
#endif /* 0 */
}

/*
 * sm_get_method_source_file() - This is an experimental function for
 *    the initial browser.  It isn't guaranteed to work in all cases.
 *    It will attempt to locate the .c file that contains the source for
 *    a method implementation.
 *    There isn't any way that this can be determined for certain, what it
 *    does now is find the .o file that contains the implementation function
 *    and assume that a .c file exists in the same directory that contains
 *    the source.  This will be true in almost all of the current cases
 *    but cannot be relied upon.  In the final version, there will need
 *    to be some form of checking/checkout procedure so that the method
 *    source can be stored within the database
 *   return: C string
 *   class(in): class or instance
 *   method(in): method name
 */

char *
sm_get_method_source_file (MOP obj, const char *name)
{
#if defined(WINDOWS)
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PC_UNIMPLEMENTED, 1,
	  "sm_get_method_source_file");
  return NULL;
#else /* WINDOWS */
  SM_CLASS *class_;
  SM_METHOD *method;
  const char *ofile;
  char *cfile;
  const char *const_cfile;
  int len;

  cfile = NULL;
  if (au_fetch_class (obj, &class_, AU_FETCH_READ, AU_SELECT) != NO_ERROR)
    {
      return NULL;
    }
  method = classobj_find_method (class_, name, 0);

  if (method != NULL && method->signatures != NULL)
    {
      ofile =
	sm_locate_method_file (class_, method->signatures->function_name);
      if (ofile == NULL)
	{
	  return cfile;
	}

      len = strlen (ofile);
      if (len <= 2)
	{
	  return cfile;
	}
      if (ofile[len - 1] == 'o' && ofile[len - 2] == '.')
	{
	  /* noise to prevent const conversion warnings */
	  const_cfile = ws_copy_string (ofile);
	  cfile = (char *) const_cfile;
	  cfile[len - 1] = 'c';
	}
    }

  return cfile;
#endif /* WINDOWS */
}


/*
 * sm_set_inhibit_identifier_check()
 *   return:
 *   inhibit(in):
 */

int
sm_set_inhibit_identifier_check (int inhibit)
{
  int current;

  current = sm_Inhibit_identifier_check;
  sm_Inhibit_identifier_check = inhibit;

  return current;
}

/*
 * sm_init() - Called during database restart.
 *    Setup the global variables that contain the root class OID and HFID.
 *    Also initialize the descriptor list
 *   return: none
 *   rootclass_oid(in): OID of root class
 *   rootclass_hfid(in): heap file of root class
 */

void
sm_init (OID * rootclass_oid, HFID * rootclass_hfid)
{

  sm_Root_class_mop = ws_mop (rootclass_oid, NULL);
  oid_Root_class_oid = ws_oid (sm_Root_class_mop);

  sm_Root_class.header.heap.vfid.volid = rootclass_hfid->vfid.volid;
  sm_Root_class.header.heap.vfid.fileid = rootclass_hfid->vfid.fileid;
  sm_Root_class.header.heap.hpgid = rootclass_hfid->hpgid;

  sm_Root_class_hfid = &sm_Root_class.header.heap;

  sm_Do_schema_sync = false;

  sm_Descriptors = NULL;
}

/*
 * sm_create_root() - Called when the database is first created.
 *    Sets up the root class globals, used later when the root class
 *    is flushed to disk
 *   return: none
 *   rootclass_oid(in): OID of root class
 *   rootclass_hfid(in): heap file of root class
 */

void
sm_create_root (OID * rootclass_oid, HFID * rootclass_hfid)
{
  sm_Root_class.header.obj_header.chn = 0;
  sm_Root_class.header.type = Meta_root;
  sm_Root_class.header.name = (char *) sm_Root_class_name;

  sm_Root_class.header.heap.vfid.volid = rootclass_hfid->vfid.volid;
  sm_Root_class.header.heap.vfid.fileid = rootclass_hfid->vfid.fileid;
  sm_Root_class.header.heap.hpgid = rootclass_hfid->hpgid;
  sm_Root_class_hfid = &sm_Root_class.header.heap;

  /* Sets up sm_Root_class_mop and Rootclass_oid */
  locator_add_root (rootclass_oid, (MOBJ) & sm_Root_class);
}


/*
 * sm_final() - Called during the shutdown sequence
 */

void
sm_final ()
{
  SM_DESCRIPTOR *d, *next;
  SM_CLASS *class_;
  DB_OBJLIST *cl;

#if defined(WINDOWS)
  /* unload any DLL's we may have opened for methods */
  sm_method_final ();
#endif /* WINDOWS */

  /* If there are any remaining descriptors it represents a memory leak
     in the application. Should be displaying warning messages here !
   */

  for (d = sm_Descriptors, next = NULL; d != NULL; d = next)
    {
      next = d->next;
      sm_free_descriptor (d);
    }

  /* go through the resident class list and free anything attached
     to the class that wasn't allocated in the workspace, this is
     only the virtual_query_cache at this time */
  for (cl = ws_Resident_classes; cl != NULL; cl = cl->next)
    {
      class_ = (SM_CLASS *) cl->op->object;
      if (class_ != NULL && class_->virtual_query_cache != NULL)
	{
	  mq_free_virtual_query_cache (class_->virtual_query_cache);
	  class_->virtual_query_cache = NULL;
	}
    }
}

/*
 * sm_transaction_boundary() - This is called by tm_commit() and tm_abort()
 *    to inform the schema manager that a transaction boundary has been crossed.
 *    If the commit-flag is non-zero it indicates that we've committed
 *    the transaction.
 *    We used to call sm_bump_schema_version directly from the tm_ functions.
 *    Now that we have more than one thing to do however, start
 *    encapsulating them in a module specific transaction boundary handler
 *    so we don't have to keep modifying transaction_cl.c
 */

void
sm_transaction_boundary (void)
{
  /* reset any outstanding descriptor caches */
  sm_reset_descriptors (NULL);

  /* Could be resetting the transaction caches in each class too
     but the workspace is controlling that */
}

/* UTILITY FUNCTIONS */
/*
 * sm_check_name() - This is made void for ANSI compatibility.
 *      It previously insured that identifiers which were accepted could be
 *      parsed in the language interface.
 *
 *  	ANSI allows any character in an identifier. It also allows reserved
 *  	words. In order to parse identifiers with non-alpha characters
 *  	or that are reserved words, an escape syntax is defined. See the lexer
 *      tokens DELIMITED_ID_NAME, BRACKET_ID_NAME and BACKTICK_ID_NAME for
 *      details on the escaping rules.
 *   return: non-zero if name is ok
 *   name(in): name to check
 */

int
sm_check_name (const char *name)
{
  if (name == NULL || name[0] == '\0')
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_SM_INVALID_NAME, 1,
	      name);
      return 0;
    }
  else
    {
      return 1;
    }
}

/*
 * sm_downcase_name() - This is a kludge to make sure that class names are
 *    always converted to lower case in the API.
 *    This conversion is already done by the parser so we must be consistent.
 *    This is necessarily largely because the eh_ module on the server does not
 *    offer a mode for case insensitive string comparison.
 *    Is there a system function that does this? I couldn't find one
 *   return: none
 *   name(in): class name
 *   buf(out): output buffer
 *   maxlen(in): maximum buffer length
 */

void
sm_downcase_name (const char *name, char *buf, int maxlen)
{
  intl_mbs_nlower (buf, name, maxlen);
}

/*
 * sm_resolution_space() -  This is used to convert a full component
 *    name_space to one of the more constrained resolution namespaces.
 *   return: resolution name_space
 *   name_space(in): component name_space
 */

SM_NAME_SPACE
sm_resolution_space (SM_NAME_SPACE name_space)
{
  SM_NAME_SPACE res_space = ID_INSTANCE;

  if (name_space == ID_CLASS_ATTRIBUTE || name_space == ID_CLASS_METHOD)
    {
      res_space = ID_CLASS;
    }

  return res_space;
}

/* CLASS LOCATION FUNCTIONS */
/*
 * sm_get_class() - This is just a convenience function used to
 *    return a class MOP for any possible MOP.
 *    If this is a class mop return it, if this is  an object MOP,
 *    fetch the class and return its mop.
 *   return: class mop
 *   obj(in): object or class mop
 */

MOP
sm_get_class (MOP obj)
{
  MOP op = NULL;

  if (obj != NULL)
    {
      if (locator_is_class (obj, DB_FETCH_READ))
	{
	  op = obj;
	}
      else
	{
	  if (obj->class_mop == NULL)
	    {
	      /* force class load through object load */
	      (void) au_fetch_class (obj, NULL, AU_FETCH_READ, AU_SELECT);
	    }
	  op = obj->class_mop;
	}
    }

  return op;
}

/*
 * sm_fetch_all_classes() - Fetch all classes for a given purpose.
 *    Builds a list of all classes in the system.  Be careful to filter
 *    out the root class since it isn't really a class to the callers.
 *    The external_list flag is set when the object list is to be returned
 *    above the database interface layer (db_ functions) and must therefore
 *    be allocated in storage that will serve as roots to the garbage
 *    collector.
 *   return: object list of class MOPs
 *   external_list(in): non-zero if external list links are to be used
 *   purpose(in): Fetch purpose
 */
DB_OBJLIST *
sm_fetch_all_classes (int external_list, DB_FETCH_MODE purpose)
{
  LIST_MOPS *lmops;
  DB_OBJLIST *objects, *last, *new_;
  int i;

  objects = NULL;
  lmops = NULL;

  if (au_check_user () == NO_ERROR)
    {				/* make sure we have a user */
      last = NULL;
      lmops = locator_get_all_mops (sm_Root_class_mop, purpose);
      /* probably should make sure we push here because the list could be long */
      if (lmops != NULL)
	{
	  for (i = 0; i < lmops->num; i++)
	    {
	      /* is it necessary to have this check ? */
	      if (!WS_MARKED_DELETED (lmops->mops[i])
		  && lmops->mops[i] != sm_Root_class_mop)
		{
		  if (!external_list)
		    {
		      if (ml_append (&objects, lmops->mops[i], NULL))
			{
			  goto memory_error;
			}
		    }
		  else
		    {
		      /* should have a ext_ append function */
		      new_ = ml_ext_alloc_link ();
		      if (new_ == NULL)
			{
			  goto memory_error;
			}
		      new_->op = lmops->mops[i];
		      new_->next = NULL;
		      if (last != NULL)
			{
			  last->next = new_;
			}
		      else
			{
			  objects = new_;
			}
		      last = new_;
		    }
		}
	    }
	  locator_free_list_mops (lmops);
	}
    }

  return objects;

memory_error:
  if (lmops != NULL)
    {
      locator_free_list_mops (lmops);
    }

  if (external_list)
    {
      ml_ext_free (objects);
    }
  else
    {
      ml_free (objects);
    }

  return NULL;
}

/*
 * sm_fetch_base_classes() - Fetch base classes for the given mode.
 *   Returns a list of classes that have no super classes.
 *   return: list of class MOPs
 *   external_list(in): non-zero to create external MOP list
 *   purpose(in): Fetch purpose
 */

DB_OBJLIST *
sm_fetch_all_base_classes (int external_list, DB_FETCH_MODE purpose)
{
  LIST_MOPS *lmops;
  DB_OBJLIST *objects, *last, *new_;
  int i;
  int error;
  SM_CLASS *class_;

  objects = NULL;
  lmops = NULL;
  if (au_check_user () == NO_ERROR)
    {				/* make sure we have a user */
      last = NULL;
      lmops = locator_get_all_mops (sm_Root_class_mop, purpose);
      /* probably should make sure we push here because the list could be long */
      if (lmops != NULL)
	{
	  for (i = 0; i < lmops->num; i++)
	    {
	      /* is it necessary to have this check ? */
	      if (!WS_MARKED_DELETED (lmops->mops[i])
		  && lmops->mops[i] != sm_Root_class_mop)
		{
		  error = au_fetch_class_force (lmops->mops[i], &class_,
						AU_FETCH_READ);
		  if (error != NO_ERROR)
		    {
		      /* problems accessing the class list, abort */
		      locator_free_list_mops (lmops);
		      ml_ext_free (objects);
		      return NULL;
		    }
		  /* only put classes without supers on the list */
		  else if (class_->inheritance == NULL)
		    {
		      if (!external_list)
			{
			  if (ml_append (&objects, lmops->mops[i], NULL))
			    {
			      goto memory_error;
			    }
			}
		      else
			{
			  /* should have a ext_ append function */
			  new_ = ml_ext_alloc_link ();
			  if (new_ == NULL)
			    {
			      goto memory_error;
			    }
			  new_->op = lmops->mops[i];
			  new_->next = NULL;
			  if (last != NULL)
			    {
			      last->next = new_;
			    }
			  else
			    {
			      objects = new_;
			    }
			  last = new_;
			}
		    }
		}
	    }
	  locator_free_list_mops (lmops);
	}
    }

  return objects;

memory_error:
  if (lmops != NULL)
    {
      locator_free_list_mops (lmops);
    }

  if (external_list)
    {
      ml_ext_free (objects);
    }
  else
    {
      ml_free (objects);
    }

  return NULL;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_get_all_classes() -  Builds a list of all classes in the system.
 *    Be careful to filter out the root class since it isn't really a class
 *    to the callers. The external_list flag is set when the object list is
 *    to be returned above the database interface layer (db_ functions) and
 *    must therefore be allocated in storage that will serve as roots to
 *    the garbage collector.
 *    Authorization checking is not performed at this level so there may be
 *    MOPs in the list that you can't actually access.
 *   return: object list of class MOPs
 *   external_list(in): non-zero if external list links are to be used
 */

static DB_OBJLIST *
sm_get_all_classes (int external_list)
{
  /* Lock all the classes in shared mode */
  return sm_fetch_all_classes (external_list, DB_FETCH_QUERY_READ);
}				/* sm_get_all_classes */

/*
 * sm_get_base_classes() - Returns a list of classes that have no super classes
 *   return: list of class MOPs
 *   external_list(in): non-zero to create external MOP list
*/
static DB_OBJLIST *
sm_get_base_classes (int external_list)
{
  /* Lock all the classes in shared mode */
  return sm_fetch_all_base_classes (external_list, DB_FETCH_QUERY_READ);
}
#endif

/* OBJECT LOCATION */
/*
 * sm_get_all_objects() - Returns a list of all the instances that have
 *    been created for a class.
 *    This was used early on before query was available, it should not
 *    be heavily used now.  Be careful, this can potentially bring
 *    in lots of objects and overflow the workspace.
 *    This is used in the implementation of a db_ function so it must
 *    allocate an external mop list !
 *   return: list of objects
 *   op(in): class or instance object
 *   purpose(in): Fetch purpose
 */

DB_OBJLIST *
sm_fetch_all_objects (DB_OBJECT * op, DB_FETCH_MODE purpose)
{
  LIST_MOPS *lmops;
  SM_CLASS *class_;
  DB_OBJLIST *objects, *new_;
  MOP classmop;
  SM_CLASS_TYPE ct;
  int i;

  objects = NULL;
  classmop = NULL;
  lmops = NULL;

  if (op != NULL)
    {
      if (locator_is_class (op, purpose))
	{
	  classmop = op;
	}
      else
	{
	  if (op->class_mop == NULL)
	    {
	      /* force load */
	      (void) au_fetch_class (op, &class_, AU_FETCH_READ, AU_SELECT);
	    }
	  classmop = op->class_mop;
	}
      if (classmop != NULL)
	{
	  class_ = (SM_CLASS *) classmop->object;
	  if (!class_)
	    {
	      (void) au_fetch_class (classmop, &class_, AU_FETCH_READ,
				     AU_SELECT);
	    }
	  if (!class_)
	    {
	      return NULL;
	    }

	  ct = sm_get_class_type (class_);
	  if (ct == SM_CLASS_CT)
	    {
	      lmops = locator_get_all_mops (classmop, purpose);
	      if (lmops != NULL)
		{
		  for (i = 0; i < lmops->num; i++)
		    {
		      /* is it necessary to have this check ? */
		      if (!WS_MARKED_DELETED (lmops->mops[i]))
			{
			  new_ = ml_ext_alloc_link ();
			  if (new_ == NULL)
			    {
			      goto memory_error;
			    }

			  new_->op = lmops->mops[i];
			  new_->next = objects;
			  objects = new_;
			}
		    }
		  locator_free_list_mops (lmops);
		}
	    }
	  else
	    {
	      objects = vid_getall_mops (classmop, class_, purpose);
	    }
	}
    }

  return objects;

memory_error:
  if (lmops != NULL)
    {
      locator_free_list_mops (lmops);
    }

  ml_ext_free (objects);

  return NULL;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_get_all_objects() - Returns a list of all the instances that
 *    have been created for a class.
 *    This was used early on before query was available, it should not
 *    be heavily used now.  Be careful, this can potentially bring
 *    in lots of objects and overflow the workspace.
 *    This is used in the implementation of a db_ function so it must
 *    allocate an external mop list !
 *   return: list of objects
 *   op(in): class or instance object
 */

static DB_OBJLIST *
sm_get_all_objects (DB_OBJECT * op)
{
  return sm_fetch_all_objects (op, DB_FETCH_QUERY_READ);
}
#endif

/* MISC SCHEMA OPERATIONS */
/*
 * sm_rename_class() - This is used to change the name of a class if possible.
 *    It is not part of the smt_ template layer because its a fairly
 *    fundamental change that must be checked early.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in/out): class mop
 *   new_name(in):
 */

int
sm_rename_class (MOP op, const char *new_name)
{
  int error;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  const char *current, *newname;
  char realname[SM_MAX_IDENTIFIER_LENGTH];
  int is_partition = 0, subren = 0;
/*  TR_STATE *trstate; */

  /* make sure this gets into the server table with no capitalization */
  sm_downcase_name (new_name, realname, SM_MAX_IDENTIFIER_LENGTH);

#if defined (ENABLE_UNUSED_FUNCTION)
  if (sm_has_text_domain (db_get_attributes (op), 1))
    {
      /* prevent to rename class */
      ERROR1 (error, ER_REGU_NOT_IMPLEMENTED, rel_major_release_string ());
      return error;
    }
#endif /* ENABLE_UNUSED_FUNCTION */
  error = sm_get_remote_info_if_need (op);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_is_partitioned_classobj (&is_partition, op, NULL, NULL);
  if (is_partition == 1)
    {
      if ((error = tran_savepoint (UNIQUE_PARTITION_SAVEPOINT_RENAME,
				   false)) != NO_ERROR)
	{
	  return error;
	}

      if ((error = do_rename_partition (op, realname)) != NO_ERROR)
	{
	  if (error != ER_LK_UNILATERALLY_ABORTED)
	    {
	      (void)
		tran_abort_upto_savepoint (UNIQUE_PARTITION_SAVEPOINT_RENAME);
	    }
	  return error;
	}
      subren = 1;
    }

  if (!sm_check_name (realname))
    {
      error = er_errid ();
    }
  else if ((error = au_fetch_class (op, &class_, AU_FETCH_UPDATE, AU_ALTER))
	   == NO_ERROR)
    {
      /*  We need to go ahead and copy the string since prepare_rename uses
       *  the address of the string in the hash table.
       */
      current = class_->header.name;
      newname = ws_copy_string (realname);
      if (newname == NULL)
	{
	  return er_errid ();
	}

      if (locator_prepare_rename_class (op, current, newname) == NULL)
	{
	  ws_free_string (newname);
	  error = er_errid ();
	}
      else
	{
	  class_->header.name = newname;
	  error = sm_flush_objects (op);

	  if (error == NO_ERROR)
	    {
	      /* rename related auto_increment serial obj name */
	      for (att = class_->attributes; att != NULL;
		   att = (SM_ATTRIBUTE *) att->header.next)
		{
		  if (att->auto_increment != NULL)
		    {
		      DB_VALUE name_val;
		      char *class_name;

		      if (db_get (att->auto_increment, "class_name",
				  &name_val) != NO_ERROR)
			{
			  break;
			}

		      class_name = DB_GET_STRING (&name_val);
		      if (class_name != NULL
			  && (strcmp (current, class_name) == 0))
			{
			  int save;
			  AU_DISABLE (save);
			  error =
			    do_update_auto_increment_serial_on_rename
			    (att->auto_increment, newname, att->header.name);
			  AU_ENABLE (save);
			}
		      db_value_clear (&name_val);

		      if (error != NO_ERROR)
			{
			  break;
			}
		    }
		}
	    }
	  ws_free_string (current);
	}
    }

  if (subren && error != NO_ERROR && error != ER_LK_UNILATERALLY_ABORTED)
    {
      (void) tran_abort_upto_savepoint (UNIQUE_PARTITION_SAVEPOINT_RENAME);
    }

  return error;
}

/*
 * sm_mark_system_classes() - Hack used to set the "system class" flag for
 *    all currently resident classes.
 *    This is only to make it more convenient to tell the
 *    difference between CUBRID and user defined classes.  This is intended
 *    to be called after the appropriate CUBRID class initialization function.
 *    Note that authorization is disabled here because these are normally
 *    called on the authorization classes.
 */

void
sm_mark_system_classes (void)
{
  LIST_MOPS *lmops;
  SM_CLASS *class_;
  int i;

  if (au_check_user () == NO_ERROR)
    {
      lmops = locator_get_all_mops (sm_Root_class_mop, DB_FETCH_QUERY_WRITE);
      if (lmops != NULL)
	{
	  for (i = 0; i < lmops->num; i++)
	    {
	      if (!WS_MARKED_DELETED (lmops->mops[i]) && lmops->mops[i]
		  != sm_Root_class_mop)
		{
		  if (au_fetch_class_force (lmops->mops[i], &class_,
					    AU_FETCH_UPDATE) == NO_ERROR)
		    {
		      class_->flags |= SM_CLASSFLAG_SYSTEM;
		    }
		}
	    }
	  locator_free_list_mops (lmops);
	}
    }
}

/*
 * sm_mark_system_class() - This turns on or off the system class flag.
 *   This flag is tested by the sm_is_system_class function.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop (in): class pointer
 *   on_or_off(in): state of the flag
 */

int
sm_mark_system_class (MOP classop, int on_or_off)
{
  SM_CLASS *class_;
  int error = NO_ERROR;

  if (classop != NULL)
    {
      if ((error = au_fetch_class_force (classop, &class_, AU_FETCH_UPDATE))
	  == NO_ERROR)
	{
	  if (on_or_off)
	    {
	      class_->flags |= SM_CLASSFLAG_SYSTEM;
	    }
	  else
	    {
	      class_->flags &= ~SM_CLASSFLAG_SYSTEM;
	    }
	}
    }

  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
#ifdef SA_MODE
void
sm_mark_system_class_for_catalog (void)
{
  MOP classmop;
  SM_CLASS *class_;
  int i;

  const char *classes[] = {
    CT_CLASS_NAME, CT_ATTRIBUTE_NAME, CT_DOMAIN_NAME,
    CT_METHOD_NAME, CT_METHSIG_NAME, CT_METHARG_NAME,
    CT_METHFILE_NAME, CT_QUERYSPEC_NAME, CT_INDEX_NAME,
    CT_INDEXKEY_NAME, CT_CLASSAUTH_NAME, CT_DATATYPE_NAME,
    CT_STORED_PROC_NAME, CT_STORED_PROC_ARGS_NAME, CT_PARTITION_NAME,
    CTV_CLASS_NAME, CTV_SUPER_CLASS_NAME, CTV_VCLASS_NAME,
    CTV_ATTRIBUTE_NAME, CTV_ATTR_SD_NAME, CTV_METHOD_NAME,
    CTV_METHARG_NAME, CTV_METHARG_SD_NAME, CTV_METHFILE_NAME,
    CTV_INDEX_NAME, CTV_INDEXKEY_NAME, CTV_AUTH_NAME,
    CTV_TRIGGER_NAME, CTV_STORED_PROC_NAME, CTV_STORED_PROC_ARGS_NAME,
    CTV_PARTITION_NAME, NULL
  };

  for (i = 0; classes[i] != NULL; i++)
    {
      classmop = locator_find_class (classes[i]);
      if (au_fetch_class_force (classmop, &class_, AU_FETCH_UPDATE) ==
	  NO_ERROR)
	{
	  class_->flags |= SM_CLASSFLAG_SYSTEM;
	}
    }
}
#endif /* SA_MODE */
#endif

/*
 * sm_set_class_flag() - This turns on or off the given flag.
 *    The flag may be tested by the sm_get_class_flag function.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop (in): class pointer
 *   flag  (in): flag to set or clear
 *   on_or_off(in): 1 to set 0 to clear
 */

int
sm_set_class_flag (MOP classop, SM_CLASS_FLAG flag, int on_or_off)
{
  SM_CLASS *class_;
  int error = NO_ERROR;

  if (classop != NULL)
    {
      if ((error = au_fetch_class_force (classop, &class_, AU_FETCH_UPDATE))
	  == NO_ERROR)
	{
	  if (on_or_off)
	    {
	      class_->flags |= flag;
	    }
	  else
	    {
	      class_->flags &= ~flag;
	    }
	}
    }

  return error;
}

/*
 * sm_is_system_class() - Tests the system class flag of a class object.
 *   return: non-zero if class is a system defined class
 *   op(in): class object
 */

int
sm_is_system_class (MOP op)
{
  return sm_get_class_flag (op, SM_CLASSFLAG_SYSTEM);
}

/*
 * sm_is_reuse_oid_class() - Tests the reuse OID class flag of a class object.
 *   return: non-zero if class is an OID reusable class
 *   op(in): class object
 */

bool
sm_is_reuse_oid_class (MOP op)
{
  return sm_get_class_flag (op, SM_CLASSFLAG_REUSE_OID) ? true : false;
}

/*
 * sm_get_class_flag() - Tests the class flag of a class object.
 *   return: non-zero if flag set
 *   op(in): class object
 *   flag(in): flag to test
 */

int
sm_get_class_flag (MOP op, SM_CLASS_FLAG flag)
{
  SM_CLASS *class_;
  int result = 0;

  if (op != NULL && locator_is_class (op, DB_FETCH_READ))
    {
      if (au_fetch_class_force (op, &class_, AU_FETCH_READ) == NO_ERROR)
	{
	  result = class_->flags & flag;
	}
    }

  return result;
}



/*
 * sm_force_write_all_classes()
 *   return: NO_ERROR on success, non-zero for ERROR
 */

int
sm_force_write_all_classes (void)
{
  LIST_MOPS *lmops;
  int i;

  /* get all class objects */
  lmops = locator_get_all_mops (sm_Root_class_mop, DB_FETCH_QUERY_WRITE);
  if (lmops != NULL)
    {
      for (i = 0; i < lmops->num; i++)
	{
	  ws_dirty (lmops->mops[i]);
	}

      /* insert all class objects into the catalog classes */
      if (locator_flush_all_instances (sm_Root_class_mop, DONT_DECACHE) !=
	  NO_ERROR)
	{
	  return er_errid ();
	}

      for (i = 0; i < lmops->num; i++)
	{
	  ws_dirty (lmops->mops[i]);
	}

      /* update class hierarchy values for some class objects.
       * the hierarchy makes class/class mutual references
       * so some class objects were inserted with no hierarchy values.
       */
      if (locator_flush_all_instances (sm_Root_class_mop, DONT_DECACHE) !=
	  NO_ERROR)
	{
	  return er_errid ();
	}

      locator_free_list_mops (lmops);
    }

  return NO_ERROR;
}

/*
 * sm_destroy_representations() - This is called by the compaction utility
 *    after it has swept through the instances of a class and converted them
 *    all to the latest representation.
 *    Once this is done, the schema manager no longer needs to maintain
 *    the history of old representations. In order for this to become
 *    persistent, the transaction must be committed.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): class object
 */

int
sm_destroy_representations (MOP op)
{
  int error = NO_ERROR;
  SM_CLASS *class_;

  error = au_fetch_class_force (op, &class_, AU_FETCH_UPDATE);
  if (error == NO_ERROR)
    {
      ws_list_free ((DB_LIST *) class_->representations,
		    (LFREEER) classobj_free_representation);
      class_->representations = NULL;
    }

  return error;
}

/* DOMAIN MAINTENANCE FUNCTIONS */

/*
 * sm_filter_domain() - This removes any invalid domain references from a
 *    domain list.  See description of filter_domain_list for more details.
 *    If the domain list was changed, we could get a write lock on the owning
 *    class to ensure that the change is made persistent.
 *    Making the change persistent doesn't really improve much since we
 *    always have to do a filter pass when the class is fetched.
 *   return: non-zero if changes were made
 *   domain(in): domain list for attribute or method arg
 */

int
sm_filter_domain (TP_DOMAIN * domain)
{
  int changes = 0;

  if (domain != NULL)
    {
      changes = tp_domain_filter_list (domain);
      /* if changes, could get write lock on owning_class here */
    }

  return changes;
}

/*
 * domain_search() - This recursively searches through the class hierarchy
 *    to see if the "class_mop" is equal to or a subclass of "dclass_mop"
 *    in which case it is within the domain of dlcass_mop.
 *    This is essentially the same as sm_is_superclass except that it
 *    doesn't check for authorization.
 *   return: non-zero if the class was valid
 *   dclass_mop(in): domain class
 *   class_mop(in): class in question
 */

static int
domain_search (MOP dclass_mop, MOP class_mop)
{
  DB_OBJLIST *cl;
  SM_CLASS *class_;
  int ok = 0;

  if (dclass_mop == class_mop)
    {
      ok = 1;
    }
  else
    {
      /* ignore authorization for the purposes of domain checking */
      if (au_fetch_class_force (class_mop, &class_, AU_FETCH_READ) ==
	  NO_ERROR)
	{
	  for (cl = class_->inheritance; cl != NULL && !ok; cl = cl->next)
	    {
	      ok = domain_search (dclass_mop, cl->op);
	    }
	}
    }

  return ok;
}

/*
 * sm_check_object_domain() - This checks to see if an instance is valid for
 *    a given domain. It checks to see if the instance's class is equal to or
 *    a subclass of the class in the domain.  Also handles the various NULL
 *    conditions.
 *   return: non-zero if object is within the domain
 *   domain(in): domain to examine
 *   object(in): instance
 */

int
sm_check_object_domain (TP_DOMAIN * domain, MOP object)
{
  int ok;

  ok = 0;
  if (domain->type == tp_Type_object)
    {
      /* check for physical and logical NULLness of the MOP, treat it
         as if it were SQL NULL which is allowed in all domains */
      if (WS_MOP_IS_NULL (object))
	{
	  ok = 1;
	}
      /* check for the wildcard object domain */
      else if (domain->class_mop == NULL)
	{
	  ok = 1;
	}
      else
	{
	  /* fetch the class if it hasn't been cached, should this be a write
	     lock ?  don't need to pin, only forcing the class fetch
	   */
	  if (object->class_mop == NULL)
	    {
	      au_fetch_instance (object, NULL, AU_FETCH_READ, AU_SELECT);
	    }

	  /* if its still NULL, assume an authorization error and go on */
	  if (object->class_mop != NULL)
	    {
	      ok = domain_search (domain->class_mop, object->class_mop);
	    }
	}
    }

  return ok;
}

/*
 * sm_coerce_object_domain() - This checks to see if an instance is valid for
 *    a given domain.
 *    It checks to see if the instance's class is equal to or a subclass
 *    of the class in the domain.  Also handles the various NULL
 *    conditions.
 *    If dest_object is not NULL and the object is a view on a real object,
 *    the real object will be returned.
 *   return: non-zero if object is within the domain
 *   domain(in): domain to examine
 *   object(in): instance
 *   dest_object(out): ptr to instance to coerce object to
 */

int
sm_coerce_object_domain (TP_DOMAIN * domain, MOP object, MOP * dest_object)
{
  int ok;
  SM_CLASS *class_;

  ok = 0;
  if (!dest_object)
    {
      return 0;
    }

  if (domain->type == tp_Type_object)
    {
      /* check for physical and logical NULLness of the MOP, treat it
         as if it were SQL NULL which is allowed in all domains */
      if (WS_MOP_IS_NULL (object))
	{
	  ok = 1;
	}
      /* check for the wildcard object domain */
      else if (domain->class_mop == NULL)
	{
	  ok = 1;
	}
      else
	{
	  /* fetch the class if it hasn't been cached, should this be a write lock ?
	     don't need to pin, only forcing the class fetch
	   */
	  if (object->class_mop == NULL)
	    {
	      au_fetch_instance (object, NULL, AU_FETCH_READ, AU_SELECT);
	    }

	  /* if its still NULL, assume an authorization error and go on */
	  if (object->class_mop != NULL)
	    {
	      if (domain->class_mop == object->class_mop)
		{
		  ok = 1;
		}
	      else
		{
		  if (au_fetch_class_force (object->class_mop, &class_,
					    AU_FETCH_READ) == NO_ERROR)
		    {
		      /* Coerce a view to a real class. */
		      if (class_->class_type == SM_VCLASS_CT)
			{
			  object = vid_get_referenced_mop (object);
			  if (object
			      && (au_fetch_class_force (object->class_mop,
							&class_,
							AU_FETCH_READ) ==
				  NO_ERROR)
			      && (class_->class_type == SM_CLASS_CT))
			    {
			      ok = domain_search (domain->class_mop,
						  object->class_mop);
			    }
			}
		      else
			{
			  ok = domain_search (domain->class_mop,
					      object->class_mop);
			}
		    }
		}
	    }
	}
    }

  if (ok)
    {
      *dest_object = object;
    }

  return ok;
}

/*
 * sm_check_class_domain() - see if a class is within the domain.
 *    It is similar to sm_check_object_domain except that we get
 *    a pointer directly to the class and we don't allow NULL conditions.
 *   return: non-zero if the class is within the domain
 *   domain(in): domain to examine
 *   class(in): class to look for
 */

int
sm_check_class_domain (TP_DOMAIN * domain, MOP class_)
{
  int ok = 0;

  if (domain->type == tp_Type_object && class_ != NULL)
    {
      /* check for domain class deletions and other delayed updates
         SINCE THIS IS CALLED FOR EVERY ATTRIBUTE UPDATE, WE MUST EITHER
         CACHE THIS INFORMATION OR PERFORM IT ONCE WHEN THE CLASS
         IS FETCHED */
      (void) sm_filter_domain (domain);

      /* wildcard case */
      if (domain->class_mop == NULL)
	{
	  ok = 1;
	}
      else
	{
	  /* recursively check domains for class & super classes
	     for now assume only one possible base class */
	  ok = domain_search (domain->class_mop, class_);
	}
    }

  return ok;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_get_set_domain() - used only by the set support to get the domain list for
 *    the attribute that owns a set.  Need to be careful that the cached
 *    domain pointer is cleared if the class is ever swapped out.
 *   return: domain list
 *   classop(in): class mop
 *   att_id(in): attribute id
 */

static TP_DOMAIN *
sm_get_set_domain (MOP classop, int att_id)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  TP_DOMAIN *domain;

  domain = NULL;
  if (au_fetch_class_force (classop, &class_, AU_FETCH_READ) == NO_ERROR)
    {
      att = NULL;

      /* search the attribute spaces, ids won't overlap */
      for (att = class_->attributes; att != NULL && att->id != att_id;
	   att = (SM_ATTRIBUTE *) att->header.next)
	;

      if (att == NULL)
	{
	  for (att = class_->shared; att != NULL && att->id != att_id;
	       att = (SM_ATTRIBUTE *) att->header.next)
	    ;

	  if (att == NULL)
	    {
	      for (att = class_->class_attributes;
		   att != NULL && att->id != att_id;
		   att = (SM_ATTRIBUTE *) att->header.next)
		;
	    }
	}

      if (att != NULL)
	{
	  domain = att->domain;
	}
    }

  return domain;
}
#endif

/*
 * annotate_method_files() - This is a kludge to work around the fact that
 *    we don't store origin or source classes with method files.
 *    These have inheritance semantics like the other class components.
 *    They can't be deleted if they were not locally defined etc.
 *    The source class needs to be stored in the disk representation but
 *    since we can't change that until 2.0, we have to fake it and compute
 *    the source class after the class has been brought in from disk.
 *    Warning, since the transformer doesn't have the class MOP when it
 *    is building the class structures, it can't call this with a valid
 *    MOP for the actual class.  In this case, we let the class pointer
 *    remain NULL and assume that that "means" this is a local attribute.
 *    If we ever support the db_methfile_source() function, this will
 *    need to be fixed.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classmop(in):
 *   class(in): class being edited
 */

static int
annotate_method_files (MOP classmop, SM_CLASS * class_)
{
  DB_OBJLIST *cl;
  SM_CLASS *super;
  SM_METHOD_FILE *f;

  if (class_->method_files != NULL)
    {
      /* might want to have the class loop outside and make multiple passes over
         the method files ?  Probably doesn't matter much */

      for (f = class_->method_files; f != NULL; f = f->next)
	{
	  if (f->class_mop == NULL)
	    {
	      for (cl = class_->inheritance;
		   cl != NULL && f->class_mop == NULL; cl = cl->next)
		{
		  if (au_fetch_class_force (cl->op, &super, AU_FETCH_READ) !=
		      NO_ERROR)
		    {
		      return (er_errid ());
		    }
		  else
		    {
		      if (NLIST_FIND (super->method_files, f->name) != NULL)
			{
			  f->class_mop = cl->op;
			}
		    }
		}

	      /* if its still NULL, assume its defined locally */
	      if (f->class_mop == NULL)
		{
		  f->class_mop = classmop;
		}
	    }
	}
    }

  return NO_ERROR;
}

/*
 * sm_clean_class() - used mainly before constructing a class template but
 *    it could be used in other places as well.  It will walk through the
 *    class structure and prune out any references to deleted objects
 *    in domain lists, etc. and do any other housekeeping tasks that it is
 *    convenient to delay until a major operation is performed.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classmop(in):
 *   class(in/out): class structure
 */

int
sm_clean_class (MOP classmop, SM_CLASS * class_)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;

  /* we only need to do this once because once we have read locks,
     the referenced classes can't be deleted */

  for (att = class_->attributes; att != NULL;
       att = (SM_ATTRIBUTE *) att->header.next)
    {
      sm_filter_domain (att->domain);
    }
  for (att = class_->shared; att != NULL;
       att = (SM_ATTRIBUTE *) att->header.next)
    {
      sm_filter_domain (att->domain);
    }
  for (att = class_->class_attributes; att != NULL;
       att = (SM_ATTRIBUTE *) att->header.next)
    {
      sm_filter_domain (att->domain);
    }

  if (!class_->post_load_cleanup)
    {
      /* initialize things that weren't done by the transformer */

      error = annotate_method_files (classmop, class_);

      class_->post_load_cleanup = 1;
    }

  return error;
}

/* CLASS STATISTICS FUNCTIONS */
/*
 * sm_get_class_with_statistics() - Fetches and returns the statistics information for a
 *    class from the system catalog on the server.
 *    Must make sure callers keep the class MOP visible to the garbage
 *    collector so the stat structures don't get reclaimed.
 *    Currently used only by the query optimizer.
 *   return: class object which contains statistics structure
 *   classop(in): class object
 */

SM_CLASS *
sm_get_class_with_statistics (MOP classop)
{
  SM_CLASS *class_ = NULL;

  /* only try to get statistics if we know the class has been flushed
     if it has a temporary oid, it isn't flushed and there are no statistics */

  if (classop != NULL
      && locator_is_class (classop, DB_FETCH_QUERY_READ)
      && !OID_ISTEMP (WS_OID (classop)))
    {
      if (au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT) ==
	  NO_ERROR)
	{
	  int node_id = DB_CLUSTER_NODE_LOCAL;
	  OID *class_oid = WS_OID (classop);

	  if (sm_get_class_type (class_) == SM_PCLASS_CT)
	    {
	      node_id = db_find_node_ip (sm_node_name ((MOBJ) class_));
	      class_oid = &class_->real_oid;
	    }

	  if (class_->stats == NULL)
	    {
	      /* it's first time to get the statistics of this class */
	      if (!OID_ISTEMP (WS_OID (classop)))
		{
		  /* make sure the class is flushed before asking for statistics,
		     this handles the case where an index has been added to the class
		     but the catalog & statistics do not reflect this fact until
		     the class is flushed.  We might want to flush instances
		     as well but that shouldn't affect the statistics ? */
		  if (locator_flush_class (classop) != NO_ERROR)
		    {
		      return NULL;
		    }
		  class_->stats =
		    stats_get_statistics (class_oid, 0, node_id);
		}
	    }
	  else
	    {
	      CLASS_STATS *stats;

	      /* to get the statistics to be updated, it send timestamp
	         as uninitialized value */
	      stats = stats_get_statistics (class_oid,
					    class_->stats->time_stamp,
					    node_id);
	      /* if newly updated statistics are fetched, replace the old one */
	      if (stats != NULL)
		{
		  stats_free_statistics (class_->stats);
		  class_->stats = stats;
		}
	    }
	}
    }

  return class_;
}

/*
 * sm_get_statistics_force()
 *   return: class statistics
 *   classop(in):
 */
CLASS_STATS *
sm_get_statistics_force (MOP classop)
{
  SM_CLASS *class_;
  CLASS_STATS *stats = NULL;

  if (classop != NULL
      && locator_is_class (classop, DB_FETCH_QUERY_READ)
      && !OID_ISTEMP (WS_OID (classop)))
    {
      if (au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT)
	  == NO_ERROR)
	{
	  int node_id = DB_CLUSTER_NODE_LOCAL;
	  OID *class_oid = WS_OID (classop);

	  if (sm_get_class_type (class_) == SM_PCLASS_CT)
	    {
	      node_id = db_find_node_ip (sm_node_name ((MOBJ) class_));
	      class_oid = &class_->real_oid;
	    }

	  if (class_->stats)
	    {
	      stats_free_statistics (class_->stats);
	    }
	  stats = class_->stats =
	    stats_get_statistics (class_oid, 0, node_id);
	}
    }

  return stats;
}

/*
 * sm_update_statistics() - Update class statistics on the server for a
 *    particular class. When finished, fetch the new statistics and
 *    cache them with the class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 */

int
sm_update_statistics (MOP classop)
{
  int error = NO_ERROR;
  SM_CLASS *class_;

  /*
   * TODO: class statistic should be processed specially.
   * */

  if (sm_Disable_updating_statistics == true)
    {
      return NO_ERROR;
    }

  /* only try to get statistics if we know the class has been flushed
     if it has a temporary oid, it isn't flushed and there are no statistics */

  if (classop != NULL && !OID_ISTEMP (WS_OID (classop))
      && locator_is_class (classop, DB_FETCH_QUERY_READ))
    {

      /* make sure the workspace is flushed before calculating stats */
      if (locator_flush_all_instances (classop, false) != NO_ERROR)
	{
	  return er_errid ();
	}

      /* get sm_class for check whether is proxy class */
      error = au_fetch_class_force (classop, &class_, AU_FETCH_READ);
      if (error == NO_ERROR)
	{
	  int node_id = DB_CLUSTER_NODE_LOCAL;
	  OID *class_oid = WS_OID (classop);

	  if (sm_get_class_type (class_) == SM_PCLASS_CT)
	    {
	      node_id = db_find_node_ip (sm_node_name ((MOBJ) class_));
	      class_oid = &class_->real_oid;
	    }

	  error = stats_update_class_statistics (class_oid, node_id);
	  if (error == NO_ERROR)
	    {
	      if (class_->stats != NULL)
		{
		  stats_free_statistics (class_->stats);
		}
	      class_->stats = NULL;

	      /* make sure the class is flushed before acquiring stats,
	         see comments above in sm_get_class_with_statistics */
	      if (locator_flush_class (classop) != NO_ERROR)
		{
		  return (er_errid ());
		}

	      /* get the new ones, should do this at the same time as the
	         update operation to avoid two server calls */
	      class_->stats = stats_get_statistics (class_oid, 0, node_id);
	    }
	}
    }

  return error;
}

/*
 * sm_update_all_statistics() - Update the statistics for all classes
 * 			        in the database.
 *   return: NO_ERROR on success, non-zero for ERROR
 */

int
sm_update_all_statistics ()
{
  int error = NO_ERROR;
  DB_OBJLIST *cl;
  SM_CLASS *class_;

  /* make sure the workspace is flushed before calculating stats */
  if (locator_all_flush () != NO_ERROR)
    {
      return er_errid ();
    }

  if ((error = stats_update_statistics ()) == NO_ERROR)
    {
      /* Need to reset the statistics cache for all resident classes */
      for (cl = ws_Resident_classes; cl != NULL; cl = cl->next)
	{
	  if (!WS_ISMARK_DELETED (cl->op))
	    {
	      /* uncache statistics only if object is cached - MOP trickery */
	      if (cl->op->object != NULL)
		{
		  int node_id = DB_CLUSTER_NODE_LOCAL;
		  OID *class_oid = WS_OID (cl->op);

		  class_ = (SM_CLASS *) cl->op->object;
		  if (class_->stats != NULL)
		    {
		      stats_free_statistics (class_->stats);
		      class_->stats = NULL;
		    }
		  /* make sure the class is flushed but quit if an error happens */
		  if (locator_flush_class (cl->op) != NO_ERROR)
		    {
		      return (er_errid ());
		    }

		  if (sm_get_class_type (class_) == SM_PCLASS_CT)
		    {
		      node_id =
			db_find_node_ip (sm_node_name ((MOBJ) class_));
		      class_oid = &class_->real_oid;
		    }

		  class_->stats =
		    stats_get_statistics (class_oid, 0, node_id);
		}
	    }
	}
    }

  return error;
}

/*
 * sm_update_all_catalog_statistics()
 *   return: NO_ERROR on success, non-zero for ERROR
 */

int
sm_update_all_catalog_statistics (void)
{
  int error = NO_ERROR;
  int i;

  const char *classes[] = {
    CT_CLASS_NAME, CT_ATTRIBUTE_NAME, CT_DOMAIN_NAME,
    CT_METHOD_NAME, CT_METHSIG_NAME, CT_METHARG_NAME,
    CT_METHFILE_NAME, CT_QUERYSPEC_NAME, CT_INDEX_NAME,
    CT_INDEXKEY_NAME, CT_CLASSAUTH_NAME, CT_DATATYPE_NAME, NULL
  };

  for (i = 0; classes[i] != NULL && error == NO_ERROR; i++)
    {
      error = sm_update_catalog_statistics (classes[i]);
    }

  return error;
}

/*
 * sm_update_catalog_statistics()
 *   return: NO_ERROR on success, non-zero for ERROR
 */

int
sm_update_catalog_statistics (const char *class_name)
{
  int error = NO_ERROR;
  DB_OBJECT *obj;

  obj = db_find_class (class_name);
  if (obj != NULL)
    {
      error = sm_update_statistics (obj);
    }
  else
    {
      error = er_errid ();
    }

  return error;
}

/* TRIGGER FUNCTIONS */
/*
 * sm_get_trigger_cache() - used to access a trigger cache within a class object.
 *    It is called by the trigger manager and object manager.
 *    The "attribute" argument may be NULL in which case the class
 *    level trigger cache is returned.  If the "attribute" argument
 *    is set, an attribute level trigger cache is returned.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   attribute(in): attribute name
 *   class_attribute(in): flag indicating class attribute name
 *   cache(out): cache pointer (returned)
 */

int
sm_get_trigger_cache (DB_OBJECT * classop,
		      const char *attribute, int class_attribute,
		      void **cache)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;
  SM_CLASS *class_;

  *cache = NULL;
  error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT);
  if (error == NO_ERROR)
    {
      if (attribute == NULL)
	{
	  *cache = class_->triggers;
	}
      else
	{
	  att = classobj_find_attribute (class_, attribute, class_attribute);
	  if (att != NULL)
	    {
	      *cache = att->triggers;
	    }
	}
    }
  return (error);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_update_trigger_cache() - This adds or modifies the trigger cache pointer
 *    in the schema.  The class is also marked as dirty so
 *    the updated cache can be stored with the class definition.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   attribute(in): attribute name
 *   class_attribute(in): flag indicating class attribute name
 *   cache(in/out): cache to update
 */

static int
sm_update_trigger_cache (DB_OBJECT * classop,
			 const char *attribute, int class_attribute,
			 void *cache)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  error = au_fetch_class (classop, &class_, AU_FETCH_UPDATE, AU_ALTER);

  if (error == NO_ERROR)
    {
      if (attribute == NULL)
	{
	  class_->triggers = cache;
	}
      else
	{
	  att = classobj_find_attribute (class_, attribute, class_attribute);
	  if (att != NULL)
	    {
	      att->triggers = cache;
	    }
	}

      /* turn off the cache validation bits so we have to recalculate them
         next time */
      class_->triggers_validated = 0;
    }
  return (error);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sm_active_triggers() - Quick check to see if the class has active triggers.
 *    Returns <0 if errors were encountered.
 *   return: non-zero if the class has active triggers
 *   class(in/out): class structure
 *   event_type(in) : event type of trigger to check.
 */
int
sm_active_triggers (SM_CLASS * class_, DB_TRIGGER_EVENT event_type)
{
  SM_ATTRIBUTE *att;
  int status;
  bool has_event_type_triggers = false;

  /* If trigger firing has been disabled we do not want to search for
   * active triggers.
   */
  if (tr_get_execution_state () != true)
    {
      return (0);
    }

  if (event_type == TR_EVENT_ALL && (class_->triggers_validated))
    {
      return (class_->has_active_triggers);
    }

  class_->has_active_triggers = 0;

  status = tr_active_schema_cache (class_->triggers, event_type,
				   &has_event_type_triggers);
  if (status < 0)
    {
      return status;
    }
  else if (status)
    {
      class_->has_active_triggers = 1;
    }

  /* no class level event type triggers, look for attribute level triggers */
  for (att = class_->ordered_attributes;
       att != NULL && !has_event_type_triggers; att = att->order_link)
    {
      status = tr_active_schema_cache (att->triggers, event_type,
				       &has_event_type_triggers);
      if (status < 0)
	{
	  return status;
	}
      else if (status)
	{
	  class_->has_active_triggers = 1;
	}
    }

  if (!has_event_type_triggers)
    {
      for (att = class_->class_attributes; att != NULL;
	   att = (SM_ATTRIBUTE *) att->header.next)
	{
	  status = tr_active_schema_cache (att->triggers, event_type,
					   &has_event_type_triggers);
	  if (status < 0)
	    {
	      return status;
	    }
	  else if (status)
	    {
	      class_->has_active_triggers = 1;
	    }
	}
    }

  /* don't repeat this process again */
  class_->triggers_validated = 1;

  return ((has_event_type_triggers) ? 1 : 0);
}

/*
 * sm_class_has_triggers() - This function can be used to determine if
 *    there are any triggers defined for a particular class.
 *    This could be used to optimize execution paths for the case where
 *    we know there will be no trigger processing.
 *    It is important that the trigger support not slow down operations
 *    on classes that do not have triggers.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   status_ptr(in): return status (non-zero if triggers for the class)
 *   event_type(in): event type of trigger to find.
 */

int
sm_class_has_triggers (DB_OBJECT * classop, int *status_ptr,
		       DB_TRIGGER_EVENT event_type)
{
  int error;
  SM_CLASS *class_;
  int status;

  if (classop == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENT;
    }

  if ((error = au_fetch_class (classop, &class_, AU_FETCH_READ,
			       AU_SELECT)) == NO_ERROR)
    {
      status = sm_active_triggers (class_, event_type);
      if (status < 0)
	{
	  error = er_errid ();
	}
      else
	{
	  *status_ptr = status;
	}
    }

  return error;
}

/*
 * sm_invalidate_trigger_cache() - This is called by the trigger manager
 *    when a trigger associated with this class has undergone a status change.
 *    When this happens, we need to recalculate the state of the
 *    has_active_triggers flag that is cached in the class structure.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 */

int
sm_invalidate_trigger_cache (DB_OBJECT * classop)
{
  int error;
  SM_CLASS *class_;

  if (!(error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT)))
    {
      class_->triggers_validated = 0;
    }

  return error;
}

/*
 * alter_trigger_cache() - This function encapsulates the mechanics of updating
 *    the trigger caches on a class.  It calls out to tr_ functions to perform
 *    the actual modification of the caches.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class(in/out): class structure
 *   attribute(in): attribute name
 *   class_attribute(in): non-zero if class attribute name
 *   trigger(in/out): trigger object to drop/add
 *   drop_it(in): non-zero if we're dropping the trigger object
 */

static int
alter_trigger_cache (SM_CLASS * class_,
		     const char *attribute, int class_attribute,
		     DB_OBJECT * trigger, int drop_it)
{
  int error = NO_ERROR;
  TR_SCHEMA_CACHE **location = NULL;
  TR_CACHE_TYPE ctype;
  SM_ATTRIBUTE *att;

  /* find the slot containing the appropriate schema cache */
  if (attribute == NULL)
    {
      location = &class_->triggers;
    }
  else
    {
      att = classobj_find_attribute (class_, attribute, class_attribute);
      if (att != NULL)
	{
	  location = &att->triggers;
	}
    }

  if (location != NULL)
    {
      if (drop_it)
	{
	  if (*location != NULL)
	    {
	      error = tr_drop_cache_trigger (*location, trigger);
	    }
	}
      else
	{
	  /* we're adding it, create a cache if one doesn't exist */
	  if (*location == NULL)
	    {
	      ctype =
		(attribute == NULL) ? TR_CACHE_CLASS : TR_CACHE_ATTRIBUTE;
	      *location = tr_make_schema_cache (ctype, NULL);
	    }
	  if (*location == NULL)
	    {
	      error = er_errid ();	/* couldn't allocate one */
	    }
	  else
	    {
	      error = tr_add_cache_trigger (*location, trigger);
	    }
	}
    }

  /* Turn off the cache validation bits so we have to recalculate them
     next time.  This is VERY important. */
  class_->triggers_validated = 0;

  return error;
}

/*
 * alter_trigger_hierarchy() - This function walks a subclass hierarchy
 *    performing an alteration to the trigger caches.
 *    This can be called in two ways.  If the trigger attribute is NULL,
 *    it will walk the hierarchy obtaining the appropriate write locks
 *    on the subclasses but will not make any changes.
 *    This is used to make sure that we can in fact lock all the affected
 *    subclasses before we try to perform the operation.
 *    When the trigger argument is non-NULL, we walk the hierarchy
 *    in the same way but this time we actually modify the trigger caches.
 *    The recursion stops when we encounter a class that has a local
 *    "shadow" attribute of the given name.  For class triggers,
 *    no shadowing is possible so we go all the way to the bottom.
 *    I'm not sure if this makes sense, we may need to go all the way
 *    for attribute triggers too.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class MOP
 *   attribute(in): target attribute (optional)
 *   class_attribute(in): non-zero if class attribute
 *   target_class(in):
 *   trigger(in/out): trigger object (NULL if just locking the hierarchy)
 *   drop_it(in): non-zero if we're going to drop the trigger
 */

static int
alter_trigger_hierarchy (DB_OBJECT * classop,
			 const char *attribute,
			 int class_attribute,
			 DB_OBJECT * target_class,
			 DB_OBJECT * trigger, int drop_it)
{
  int error = NO_ERROR;
  AU_FETCHMODE mode;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  DB_OBJLIST *u;
  int dive;

  /* fetch the class */
  mode = (trigger == NULL) ? AU_FETCH_WRITE : AU_FETCH_UPDATE;
  error = au_fetch_class_force (classop, &class_, mode);
  if (error != NO_ERROR)
    {
      if (WS_ISMARK_DELETED (classop))
	{
	  error = NO_ERROR;	/* in this case, just ignore the error */
	}
    }
  else
    {
      dive = 1;
      if (attribute != NULL)
	{
	  /* dive only if we don't have a shadow of this attribute */
	  if (classop != target_class)
	    {
	      att = classobj_find_attribute (class_, attribute,
					     class_attribute);
	      if (att == NULL || att->class_mop != target_class)
		{
		  dive = 0;
		}
	    }
	}

      if (dive)
	{
	  /* dive to the bottom */
	  for (u = class_->users; u != NULL && !error; u = u->next)
	    {
	      error = alter_trigger_hierarchy (u->op, attribute,
					       class_attribute, target_class,
					       trigger, drop_it);
	    }
	}

      /* if everything went ok, alter the cache */
      if (!error && trigger != NULL)
	{
	  error = alter_trigger_cache (class_, attribute, class_attribute,
				       trigger, drop_it);
	}
    }
  return (error);
}

/*
 * sm_add_trigger() - This is called by the trigger manager to associate
 *    a trigger object with a class.
 *    The trigger is added to the trigger list for this class and all of
 *    its subclasses.
 *    ALTER authorization is required for the topmost class, the subclasses
 *    are fetched without authorization because the trigger must be added
 *    to them to maintain the "is-a" relationship.
 *    The class and the affected subclasses are marked dirty so the new
 *    trigger will be stored.
 *    This function must create a trigger cache and add the trigger
 *    object by calling back to the trigger manager functions
 *    tr_make_schema_cache, and tr_add_schema_cache at the appropriate times.
 *    This lets the schema manager perform the class hierarchy walk,
 *    while the trigger manager still has control over how the caches
 *    are created and updated.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class on which to add a trigger
 *   attribute(in): attribute name (optional)
 *   class_attribute(in): non-zero if its a class attribute name
 *   trigger (in/out): trigger object to add
 */

int
sm_add_trigger (DB_OBJECT * classop,
		const char *attribute, int class_attribute,
		DB_OBJECT * trigger)
{
  int error = NO_ERROR;
  SM_CLASS *class_;

  /* first fetch with authorization on the outer class */
  if (!(error = au_fetch_class (classop, &class_, AU_FETCH_UPDATE, AU_ALTER)))
    {
      /* Make sure all the affected subclasses are accessible. */
      if (!(error = alter_trigger_hierarchy (classop, attribute,
					     class_attribute, classop, NULL,
					     0)))
	{
	  error = alter_trigger_hierarchy (classop, attribute,
					   class_attribute, classop, trigger,
					   0);
	}
    }

  return error;
}

/*
 * sm_drop_trigger() - called by the trigger manager when a trigger is dropped.
 *    It will walk the class hierarchy and remove the trigger from
 *    the caches of this class and any subclasses that inherit the trigger.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   attribute(in): attribute name
 *   class_attribute(in): non-zero if class attribute
 *   trigger(in/out): trigger object to drop
 */

int
sm_drop_trigger (DB_OBJECT * classop,
		 const char *attribute, int class_attribute,
		 DB_OBJECT * trigger)
{
  int error = NO_ERROR;
  SM_CLASS *class_;

  /* first fetch with authorization on the outer class */
  error = au_fetch_class (classop, &class_, AU_FETCH_UPDATE, AU_ALTER);

  /* if the error is "deleted object", just ignore the request since
     the trigger will be marked invalid and the class can't possibly be
     pointing to it */
  if (error == ER_HEAP_UNKNOWN_OBJECT)
    {
      error = NO_ERROR;
    }
  else if (!error)
    {
      /* Make sure all the affected subclasses are accessible. */
      if (!(error = alter_trigger_hierarchy (classop, attribute,
					     class_attribute, classop, NULL,
					     1)))
	{
	  error = alter_trigger_hierarchy (classop, attribute,
					   class_attribute, classop, trigger,
					   1);
	}
    }

  return error;
}

/* MISC INFORMATION FUNCTIONS */
/*
 * sm_class_name() - Returns the name of a class associated with an object.
 *    If the object is a class, its own class name is returned.
 *    If the object is an instance, the name of the instance's class
 *    is returned.
 *    Authorization is ignored for this one case.
 *   return: class name
 *   op(in): class or instance object
 */

const char *
sm_class_name (MOP op)
{
  SM_CLASS *class_;
  const char *name = NULL;

  if (op != NULL)
    {
      if (au_fetch_class_force (op, &class_, AU_FETCH_READ) == NO_ERROR)
	{
	  name = class_->header.name;
	}
    }

  return name;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_get_class_name_internal()
 * sm_get_class_name()
 * sm_get_class_name_not_null() - Returns the name of a class associated with
 *    an object. If the object is a class, its own class name is returned.
 *    If the object is an instance, the name of the instance's class
 *    is returned.
 *    Authorization is ignored for this one case.
 *    This function is lighter than sm_class_name(), and returns not null.
 *   return: class name
 *   op(in): class or instance object
 *   return_null(in):
 */

static const char *
sm_get_class_name_internal (MOP op, bool return_null)
{
  SM_CLASS *class_ = NULL;
  const char *name = NULL;
  int save;

  if (op != NULL)
    {
      AU_DISABLE (save);
      if (au_fetch_class (op, &class_, AU_FETCH_READ, AU_SELECT) == NO_ERROR)
	{
	  if (class_)
	    {
	      name = class_->header.name;
	    }
	}
      AU_ENABLE (save);
    }

  return (name ? name : (return_null ? NULL : ""));
}

static const char *
sm_get_class_name (MOP op)
{
  return sm_get_class_name_internal (op, true);
}

static const char *
sm_get_class_name_not_null (MOP op)
{
  return sm_get_class_name_internal (op, false);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sm_is_subclass() - Checks to see if one class is a subclass of another.
 *   return: non-zero if classmop is subclass of supermop
 *   classmop(in): possible sub class
 *   supermop(in): possible super class
 */

int
sm_is_subclass (MOP classmop, MOP supermop)
{
  DB_OBJLIST *s;
  SM_CLASS *class_;
  int found;

  found = 0;
  if (au_fetch_class (classmop, &class_, AU_FETCH_READ, AU_SELECT) ==
      NO_ERROR)
    {
      for (s = class_->inheritance; !found && s != NULL; s = s->next)
	{
	  if (s->op == supermop)
	    {
	      found = 1;
	    }
	}

      if (!found)
	{
	  for (s = class_->inheritance; !found && s != NULL; s = s->next)
	    {
	      found = sm_is_subclass (s->op, supermop);
	    }
	}
    }

  return found;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_object_size() - Walk through the instance or class and tally up
 *    the number of bytes used for storing the various object components.
 *    Information function only.  Not guaranteed acurate but should
 *    always be maintained as close as possible.
 *   return: memory byte size of object
 *   op(in): class or instance object
 */

int
sm_object_size (MOP op)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  MOBJ obj;
  int size, pin;

  size = 0;
  if (locator_is_class (op, DB_FETCH_READ))
    {
      if (au_fetch_class (op, &class_, AU_FETCH_READ, AU_SELECT) == NO_ERROR)
	{
	  size = classobj_class_size (class_);
	}
    }
  else
    {
      if (au_fetch_class (op, &class_, AU_FETCH_READ, AU_SELECT) == NO_ERROR)
	{
	  if (au_fetch_instance (op, &obj, AU_FETCH_READ, AU_SELECT) ==
	      NO_ERROR)
	    {
	      /* wouldn't have to pin here since we don't allocate storage
	         but can't hurt to be safe */
	      pin = ws_pin (op, 1);
	      size = class_->object_size;
	      for (att = class_->attributes; att != NULL;
		   att = (SM_ATTRIBUTE *) att->header.next)
		{
		  if (att->type->variable_p)
		    {
		      size +=
			pr_total_mem_size (att->type, obj + att->offset);
		    }
		}
	      (void) ws_pin (op, pin);
	    }
	}
    }

  return size;
}
#endif /* ENABLE_UNUSED_FUNCTION */
/*
 * sm_object_size_quick() - Calculate the memory size of an instance.
 *    Called only by the workspace statistics functions.
 *    Like sm_object_size but doesn't do any fetches.
 *   return: byte size of instance
 *   class(in): class structure
 *   obj(in): pointer to instance memory
 */

int
sm_object_size_quick (SM_CLASS * class_, MOBJ obj)
{
  SM_ATTRIBUTE *att;
  int size = 0;

  if (class_ != NULL && obj != NULL)
    {
      size = class_->object_size;
      for (att = class_->attributes; att != (void *) 0;
	   att = (SM_ATTRIBUTE *) att->header.next)
	{
	  if (att->type->variable_p)
	    {
	      size += pr_total_mem_size (att->type, obj + att->offset);
	    }
	}
    }

  return size;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_object_disk_size() - Calculates the disk size of an object.
 *    General information function that should be pretty accurate but
 *    not guaranteed to be absolutely accurate.
 *   return: byte size of disk representation of object
 *   op(in): class or instance object
 */

static int
sm_object_disk_size (MOP op)
{
  SM_CLASS *class_;
  MOBJ obj;
  int size, pin;

  size = 0;
  if (au_fetch_class (op->class_mop, &class_, AU_FETCH_READ, AU_SELECT) ==
      NO_ERROR)
    {
      obj = NULL;
      if (locator_is_class (op, DB_FETCH_READ))
	{
	  au_fetch_class (op, (SM_CLASS **) & obj, AU_FETCH_READ, AU_SELECT);
	  if (obj != NULL)
	    {
	      size = tf_object_size ((MOBJ) class_, obj);
	    }
	}
      else
	{
	  au_fetch_instance (op, &obj, AU_FETCH_READ, AU_SELECT);
	  if (obj != NULL)
	    {
	      /* probably woudn't have to pin here since we don't allocate */
	      pin = ws_pin (op, 1);
	      size = tf_object_size ((MOBJ) class_, obj);
	      (void) ws_pin (op, pin);
	    }
	}
    }

  return size;
}
#endif /* ENABLE_UNUSED_FUNCTION */

#if defined(CUBRID_DEBUG)
/*
 * sm_dump() - Debug function to dump internal information about class objects.
 *   return: none
 *   classmop(in): class object
 */

static void
sm_print (MOP classmop)
{
  SM_CLASS *class_;

  if (au_fetch_class (classmop, &class_, AU_FETCH_READ, AU_SELECT) ==
      NO_ERROR)
    {
      classobj_print (class_);
    }
}
#endif

/* LOCATOR SUPPORT FUNCTIONS */
/*
 * sm_classobj_name() - Given a pointer to a class object in memory,
 *    return the name. Used by the transaction locator.
 *   return: class name
 *   classobj(in): class structure
 */

const char *
sm_classobj_name (MOBJ classobj)
{
  SM_CLASS_HEADER *class_;
  const char *name = NULL;

  if (classobj != NULL)
    {
      class_ = (SM_CLASS_HEADER *) classobj;
      name = class_->name;
    }

  return name;
}

/*
 * sm_heap() - Support function for the transaction locator.
 *    This returns a pointer to the heap file identifier in a class.
 *    This will work for either classes or the root class.
 *   return: HFID of class
 *   clobj(in): pointer to class structure in memory
 */

HFID *
sm_heap (MOBJ clobj)
{
  SM_CLASS_HEADER *header;
  HFID *heap;

  header = (SM_CLASS_HEADER *) clobj;

  heap = &header->heap;

  return heap;
}

/*
 * sm_get_heap() - Return the HFID of a class given a MOP.
 *    Like sm_heap but takes a MOP.
 *   return: hfid of class
 *   classmop(in): class object
 */

HFID *
sm_get_heap (MOP classmop)
{
  SM_CLASS *class_ = NULL;
  HFID *heap;

  heap = NULL;
  if (locator_is_class (classmop, DB_FETCH_READ))
    {
      if (au_fetch_class (classmop, &class_, AU_FETCH_READ, AU_SELECT) ==
	  NO_ERROR)
	{
	  heap = &class_->header.heap;
	}
    }

  return heap;
}

/*
 * sm_has_indexes() - This is used to determine if there are any indexes
 *    associated with a particular class.
 *    Currently, this is used only by the locator so
 *    that when deleted instances are flushed, we can set the appropriate
 *    flags so that the indexes on the server will be updated.  For updated
 *    objects, the "has indexes" flag is returned by tf_mem_to_disk().
 *    Since we don't transform deleted objects however, we need a different
 *    mechanism for determining whether indexes exist.  Probably we should
 *    be using this function for all cases and remove the flag from the
 *    tf_ interface.
 *    This will return an error code if the class could not be fetched for
 *    some reason.  Authorization is NOT checked here.
 *    All of the constraint information is also contained on the class
 *    property list as well as the class constraint cache.  The class
 *    constraint cache is probably easier and faster to search than
 *    scanning over each attribute.  Something that we might want to change
 *    later.
 *   return: Non-zero if there are indexes defined
 *   classmop(in): class pointer
 */

bool
sm_has_indexes (MOBJ classobj)
{
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *con;
  bool has_indexes = false;

  class_ = (SM_CLASS *) classobj;
  for (con = class_->constraints; con != NULL; con = con->next)
    {
      if (SM_IS_CONSTRAINT_INDEX_FAMILY (con->type))
	{
	  has_indexes = true;
	  break;
	}
    }

  return has_indexes;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_has_constraint() - This is used to determine if a constraint is
 *    associated with a particular class.
 *   return: Non-zero if there are constraints defined
 *   classobj(in): class pointer
 *   constraint(in): the constraint to look for
 */

static int
sm_has_constraint (MOBJ classobj, SM_ATTRIBUTE_FLAG constraint)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  int has_constraint = 0;

  class_ = (SM_CLASS *) classobj;
  for (att = class_->attributes; att != NULL;
       att = (SM_ATTRIBUTE *) att->header.next)
    {
      if (att->flags & constraint)
	{
	  has_constraint = 1;
	  break;
	}
    }

  return has_constraint;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sm_class_constraints() - Return a pointer to the class constraint cache.
 *    A NULL pointer is returned is an error occurs.
 *   return: class constraint
 *   classop(in): class pointer
 */

SM_CLASS_CONSTRAINT *
sm_class_constraints (MOP classop)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *constraints = NULL;

  error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT);
  if (error == NO_ERROR)
    {
      constraints = class_->constraints;
    }

  return constraints;
}

/* INTERPRETER SUPPORT FUNCTIONS */
/*
 * sm_find_class() - Given a class name, return the class object.
 *    All this really does is call locator_find_class but it makes sure the
 *    search is case insensitive.
 *   return: class object
 *   name(in): class name
 */

MOP
sm_find_class (const char *name)
{
  char realname[SM_MAX_IDENTIFIER_LENGTH];

  sm_downcase_name (name, realname, SM_MAX_IDENTIFIER_LENGTH);

  return (locator_find_class (realname));
}

/*
 * find_attribute_op() - Given the MOP of an object and an attribute name,
 *    return a pointer to the class structure and a pointer to the
 *    attribute structure with the given name.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): class or instance MOP
 *   name(in): attribute name
 *   classp(out): return pointer to class
 *   attp(out): return pointer to attribute
 */

static int
find_attribute_op (MOP op, const char *name,
		   SM_CLASS ** classp, SM_ATTRIBUTE ** attp)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  if (!sm_check_name (name))
    {
      error = er_errid ();
    }
  else
    {
      if ((error = au_fetch_class (op, &class_, AU_FETCH_READ,
				   AU_SELECT)) == NO_ERROR)
	{
	  att = classobj_find_attribute (class_, name, 0);
	  if (att == NULL)
	    {
	      ERROR1 (error, ER_SM_ATTRIBUTE_NOT_FOUND, name);
	    }
	  else
	    {
	      *classp = class_;
	      *attp = att;
	    }
	}
    }

  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_get_att_domain() - Get the domain descriptor for an attribute.
 *    This should be replaced with sm_get_att_info.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): class object
 *   name(in): attribute name
 *   domain(out): returned pointer to domain
 */

static int
sm_get_att_domain (MOP op, const char *name, TP_DOMAIN ** domain)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;
  SM_CLASS *class_;

  if ((error = find_attribute_op (op, name, &class_, &att)) == NO_ERROR)
    {
      sm_filter_domain (att->domain);
      *domain = att->domain;
    }

  return error;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sm_get_att_name() - Get the name of an attribute with its id.
 *   return: attribute name
 *   classop(in): class object
 *   id(in): attribute ID
 */

const char *
sm_get_att_name (MOP classop, int id)
{
  const char *name = NULL;
  int error;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  if ((error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT))
      == NO_ERROR)
    {
      if ((att = classobj_find_attribute_id (class_, id, 0)) != NULL)
	{
	  name = att->header.name;
	}
    }

  return name;
}				/* sm_get_att_name() */

/*
 * sm_att_id() - Returns the internal id number assigned to the attribute.
 *   return: attribute id number
 *   classop(in): class object
 *   name(in): attribute
 */

int
sm_att_id (MOP classop, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;
  int id;

  id = -1;
  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      id = att->id;
    }

  return id;
}

/*
 * sm_att_type_id() - Return the type constant for the basic
 * 		      type of an attribute.
 *   return: type identifier
 *   classop(in): class object
 *   name(in): attribute name
 */

DB_TYPE
sm_att_type_id (MOP classop, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;
  DB_TYPE type;

  type = DB_TYPE_NULL;
  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      type = att->type->id;
    }

  return type;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_type_name() - Accesses the primitive type name for a type identifier.
 *    Used by the interpreter for error messages during semantic checking.
 *   return: internal primitive type name
 *   id(in): type identifier
 */

static const char *
sm_type_name (DB_TYPE id)
{
  PR_TYPE *type;

  type = PR_TYPE_FROM_ID (id);
  if (type != NULL)
    {
      return type->name;
    }

  return NULL;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sm_att_class() - Returns the domain class of an attribute if its basic type
 *    is DB_TYPE_OBJECT.
 *   return: domain class of attribute
 *   classop(in): class object
 *   name(in): attribute name
 */

MOP
sm_att_class (MOP classop, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;
  MOP attclass;

  attclass = NULL;
  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      sm_filter_domain (att->domain);
      if (att->domain != NULL && att->domain->type == tp_Type_object)
	{
	  attclass = att->domain->class_mop;
	}
    }

  return attclass;
}

/*
 * sm_att_info() - Used by the interpreter and query compiler to gather
 *    misc information about an attribute.  Don't set errors
 *    if the attribute was not found, the compiler may use this to
 *    probe classes for information and will handle errors on its own.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   name(in): attribute name
 *   idp(out): returned attribute identifier
 *   domainp(out): returned domain structure
 *   sharedp(out): returned flag set if shared attribute
 *   class_attr(in): flag to indicate if you want att info for class attributes
 */

int
sm_att_info (MOP classop, const char *name, int *idp,
	     TP_DOMAIN ** domainp, int *sharedp, int class_attr)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  att = NULL;
  *sharedp = 0;

  if ((error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT))
      == NO_ERROR)
    {
      if ((att = classobj_find_attribute (class_, name, class_attr)) == NULL)
	{
	  /* return error but don't call er_set */
	  error = ER_SM_ATTRIBUTE_NOT_FOUND;
	}

      if (error == NO_ERROR)
	{
	  if (att->header.name_space == ID_SHARED_ATTRIBUTE)
	    {
	      *sharedp = 1;
	    }
	  sm_filter_domain (att->domain);
	  *idp = att->id;
	  *domainp = att->domain;
	}
    }

  return error;
}

/*
 * sm_find_index()
 *   return: Pointer to B-tree ID variable.
 *   classop(in): class object
 *   att_names(in):
 *   num_atts(in):
 *   unique_index_only(in):
 *   btid(out):
 */

BTID *
sm_find_index (MOP classop, char **att_names, int num_atts,
	       bool unique_index_only, BTID * btid)
{
  int error = NO_ERROR;
  int i;
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *con = NULL;
  SM_ATTRIBUTE *att1, *att2;
  BTID *index = NULL;

  index = NULL;
  error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT);
  if (error == NO_ERROR)
    {
      /* never use an unique index upon a class hierarchy */
      if (unique_index_only && (class_->inheritance || class_->users))
	{
	  return NULL;
	}

      for (con = class_->constraints; con != NULL; con = con->next)
	{
	  if (!SM_IS_CONSTRAINT_INDEX_FAMILY (con->type))
	    {
	      continue;
	    }

	  if (unique_index_only
	      && !SM_IS_CONSTRAINT_UNIQUE_FAMILY (con->type))
	    {
	      continue;
	    }

	  if (num_atts > 0)
	    {
	      for (i = 0; i < num_atts; i++)
		{
		  att1 = con->attributes[i];
		  if (att1 == NULL)
		    {
		      break;
		    }

		  att2 = classobj_find_attribute (class_, att_names[i], 0);
		  if (att2 == NULL || att1->id != att2->id)
		    {
		      break;
		    }
		}

	      if ((i == num_atts) && con->attributes[i] == NULL)
		{
		  /* found it */
		  break;
		}
	    }
	  else
	    {
	      break;
	    }
	}
    }

  if (con)
    {
      BTID_COPY (btid, &con->index);
      index = btid;
    }

  return (index);
}

/*
 * sm_att_constrained() - Returns whether the attribute is auto_increment.
 *   classop(in): class object
 *   name(in): attribute
 */

bool
sm_att_auto_increment (MOP classop, const char *name)
{
  SM_CLASS *class_ = NULL;
  SM_ATTRIBUTE *att = NULL;
  bool rc = false;

  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      rc = att->flags & SM_ATTFLAG_AUTO_INCREMENT ? true : false;
    }

  return rc;
}

/*
 * sm_att_default_value() - Gets the default value of a column.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   name(in): attribute
 *   value(out): the default value of the specified attribute
 */

int
sm_att_default_value (MOP classop, const char *name, DB_VALUE * value)
{
  SM_CLASS *class_ = NULL;
  SM_ATTRIBUTE *att = NULL;
  int error = NO_ERROR;

  assert (value != NULL);

  error = db_value_clear (value);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = find_attribute_op (classop, name, &class_, &att);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = db_value_clone (&att->value, value);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }
  return error;

error_exit:
  return error;
}


/*
 * sm_att_constrained() - Returns whether the attribute is constained.
 *   return: whether the attribute is constrained.
 *   classop(in): class object
 *   name(in): attribute
 *   cons(in): constraint
 */

int
sm_att_constrained (MOP classop, const char *name, SM_ATTRIBUTE_FLAG cons)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;
  int rc;

  rc = 0;
  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      if (SM_IS_ATTFLAG_INDEX_FAMILY (cons))
	{
	  rc = classobj_get_cached_constraint (att->constraints,
					       SM_MAP_INDEX_ATTFLAG_TO_CONSTRAINT
					       (cons), NULL);
	}
      else
	{
	  rc = att->flags & cons;
	}
    }

  return rc;
}

/*
 * sm_is_att_fk_cache()
 *   return:
 *   classop(in): class object
 *   name(in):
 */

int
sm_is_att_fk_cache (MOP classop, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;

  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      return att->is_fk_cache_attr;
    }

  return false;
}

/*
 * sm_att_fk_constrained() - Returns whether the attribute is foreign key
 *			     constrained.
 *   return: whether the attribute is foreign key constrained.
 *   classop(in): class object
 *   name(in): attribute
 */
int
sm_att_fk_constrained (MOP classop, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;

  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      return db_attribute_is_foreign_key (att);
    }

  return false;
}

/*
 * sm_att_unique_constrained() - Returns whether the attribute is UNIQUE constained.
 *   return: whether the attribute is UNIQUE constrained.
 *   classop(in): class object
 *   name(in): attribute
 */

int
sm_att_unique_constrained (MOP classop, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;
  int rc;

  rc = 0;
  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      rc = classobj_has_unique_constraint (att->constraints);
    }

  return rc;
}

/*
 * sm_class_check_uniques() - Returns NO_ERROR if there are no unique constraints
 *    or if none of the unique constraints are violated.
 *    Used by the interpreter to check batched unique constraints.
 *   return: whether class failed unique constraint tests.
 *   classop(in): class object
 */

int
sm_class_check_uniques (MOP classop)
{
  SM_CLASS *class_;
  int error = NO_ERROR;
  OR_ALIGNED_BUF (200) a_buffer;	/* should handle most of the cases */
  char *buffer;
  int buf_size, buf_len = 0, buf_malloced = 0, uniques = 0;
  char *bufp, *buf_start;
  SM_CLASS_CONSTRAINT *con;

  buffer = OR_ALIGNED_BUF_START (a_buffer);
  bufp = buffer;
  buf_start = buffer;
  buf_size = 200;		/* could use OR_ALIGNED_BUF_SIZE */

  if ((error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT))
      == NO_ERROR)
    {
      for (con = class_->constraints; con != NULL; con = con->next)
	{
	  if (SM_IS_CONSTRAINT_UNIQUE_FAMILY (con->type))
	    {
	      uniques = 1;

	      /* check if we have space for one more btid */
	      if (buf_len + OR_BTID_ALIGNED_SIZE > buf_size)
		{
		  buf_size = buf_size * 2;
		  if (buf_malloced)
		    {
		      if ((buf_start = (char *)
			   realloc (buf_start, buf_size)) == NULL)
			{
			  buf_malloced = 0;
			  goto error_class_check_uniques;
			}
		    }
		  else
		    {
		      if ((buf_start = malloc (buf_size)) == NULL)
			{
			  goto error_class_check_uniques;
			}
		      memcpy (buf_start, buffer, buf_len);
		    }
		  buf_malloced = 1;
		  bufp = buf_start + buf_len;
		}

	      bufp = or_pack_btid (bufp, &(con->index));
	      buf_len += OR_BTID_ALIGNED_SIZE;
	    }
	}

      if (uniques)
	{
	  error = btree_class_test_unique (buf_start, buf_len);
	}
    }

  if (buf_malloced)
    {
      free_and_init (buf_start);
    }

  return error;

error_class_check_uniques:
  if (buf_malloced)
    {
      free_and_init (buf_start);
    }

  return er_errid ();
}

/* QUERY PROCESSOR SUPPORT FUNCTIONS */
/*
 * sm_get_class_repid() - Used by the query compiler to tag compiled
 *    queries/views with the representation ids of the involved classes.
 *    This allows it to check for class modifications at a later date and
 *    invalidate the query/view.
 *   return: current representation id if class. Returns -1 if an error ocurred
 *   classop(in): class object
 */

int
sm_get_class_repid (MOP classop)
{
  SM_CLASS *class_;
  int id = -1;

  if (classop != NULL && locator_is_class (classop, DB_FETCH_READ))
    {
      if (au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT) ==
	  NO_ERROR)
	{
	  id = class_->repid;
	}
    }

  return id;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lock_query_subclasses()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   subclasses(in):
 *   op(in): root class of query
 *   exceptions(in):  list of exception classes
 *   update(in): set if classes are to be locked for update
 */

static int
lock_query_subclasses (DB_OBJLIST ** subclasses, MOP op,
		       DB_OBJLIST * exceptions, int update)
{
  int error = NO_ERROR;
  DB_OBJLIST *l, *found, *new_, *u;
  SM_CLASS *class_;

  if (!ml_find (exceptions, op))
    {
      /* must be more effecient here */
      if (update)
	{
	  error = au_fetch_class (op, &class_, AU_FETCH_READ, AU_UPDATE);
	}
      else
	{
	  error = au_fetch_class (op, &class_, AU_FETCH_READ, AU_SELECT);
	}

      if (error == NO_ERROR)
	{
	  /* upgrade the lock, MUST change this to be part of the au call */
	  if (update)
	    {
	      class_ = (SM_CLASS *) locator_fetch_class (op,
							 DB_FETCH_QUERY_WRITE);
	    }
	  else
	    {
	      class_ = (SM_CLASS *) locator_fetch_class (op,
							 DB_FETCH_QUERY_READ);
	    }

	  if (class_ == NULL)
	    {
	      error = er_errid ();
	    }
	  else
	    {
	      /* dive to the bottom */
	      for (u = class_->users; u != NULL && error == NO_ERROR;
		   u = u->next)
		{
		  error =
		    lock_query_subclasses (subclasses, u->op, exceptions,
					   update);
		}

	      /* push the class on the list */
	      for (l = *subclasses, found = NULL;
		   l != NULL && found == NULL; l = l->next)
		{
		  if (l->op == op)
		    {
		      found = l;
		    }
		}
	      if (found == NULL)
		{
		  new_ = (DB_OBJLIST *) db_ws_alloc (sizeof (DB_OBJLIST));
		  if (new_ == NULL)
		    {
		      return er_errid ();
		    }
		  new_->op = op;
		  new_->next = *subclasses;
		  *subclasses = new_;
		}
	    }
	}
    }
  return (error);
}

/*
 * sm_query_lock() - Lock a class hierarchy in preparation for a query.
 *   return: object list
 *   classop(in): root class of query
 *   exceptions(in): list of exception classes
 *   only(in): set if only top level class is locked
 *   update(in): set if classes are to be locked for update
 */

static DB_OBJLIST *
sm_query_lock (MOP classop, DB_OBJLIST * exceptions, int only, int update)
{
  int error;
  DB_OBJLIST *classes, *u;
  SM_CLASS *class_;

  classes = NULL;
  if (classop != NULL)
    {
      if (update)
	{
	  error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_UPDATE);
	}
      else
	{
	  error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT);
	}

      if (error == NO_ERROR)
	{
	  /* upgrade the lock, MUST change this to be part of the au call */
	  if (update)
	    {
	      class_ = (SM_CLASS *) locator_fetch_class (classop,
							 DB_FETCH_QUERY_WRITE);
	    }
	  else
	    {
	      class_ = (SM_CLASS *) locator_fetch_class (classop,
							 DB_FETCH_QUERY_READ);
	    }
	  if (class_ == NULL)
	    {
	      ml_free (classes);
	      return (NULL);
	    }
	  if (!ml_find (exceptions, classop))
	    {
	      if (ml_add (&classes, classop, NULL))
		{
		  ml_free (classes);
		  return NULL;
		}
	    }

	  if (!only)
	    {
	      for (u = class_->users; u != NULL && error == NO_ERROR;
		   u = u->next)
		{
		  error = lock_query_subclasses (&classes, u->op, exceptions,
						 update);
		}
	    }
	}
    }
  else if (!only)
    {
      /* KLUDGE, if the classop is NULL, assume that the domain is "object" and that
         all classes are available - shouldn't have to do this !!! */
      classes = sm_get_all_classes (0);
    }

  return (classes);
}
#endif

/*
 * sm_flush_objects() - Flush all the instances of a particular class
 *    to the server. Used by the query processor to ensure that all
 *    dirty objects of a class are flushed before attempting to
 *    execute the query.
 *    It is important that the class be flushed as well.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   obj(in): class or instance
 */

int
sm_flush_objects (MOP obj)
{
  return sm_flush_and_decache_objects (obj, false);
}

/*
 * sm_flush_and_decache_objects() - Flush all the instances of a particular
 *    class to the server. Optionally decache the instances of the class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   obj(in): class or instance
 *   decache(in): whether to decache the instances of the class.
 */

int
sm_flush_and_decache_objects (MOP obj, int decache)
{
  int error = NO_ERROR;
  MOBJ mem;
  SM_CLASS *class_;

  if (obj != NULL)
    {
      if (locator_is_class (obj, DB_FETCH_READ))
	{
	  /* always make sure the class is flushed as well */
	  if (locator_flush_class (obj) != NO_ERROR)
	    {
	      return (er_errid ());
	    }

	  class_ = (SM_CLASS *) locator_fetch_class (obj, DB_FETCH_READ);
	  if (class_ == NULL)
	    {
	      ERROR0 (error, ER_WS_NO_CLASS_FOR_INSTANCE);
	    }
	  else
	    {
	      switch (class_->class_type)
		{
		case SM_CLASS_CT:
		  if (class_->flags & SM_CLASSFLAG_SYSTEM)
		    {
		      /* if system class, flush all dirty class */
		      if (locator_flush_all_instances (sm_Root_class_mop,
						       DONT_DECACHE) !=
			  NO_ERROR)
			{
			  error = er_errid ();
			  break;
			}
		    }

		  if (locator_flush_all_instances (obj, decache) != NO_ERROR)
		    {
		      error = er_errid ();
		    }
		  break;

		case SM_VCLASS_CT:
		  if (vid_flush_all_instances (obj, decache) != NO_ERROR)
		    {
		      error = er_errid ();
		    }
		  break;

		case SM_ADT_CT:
		case SM_PCLASS_CT:
		  /* what to do here?? */
		  break;
		}
	    }
	}
      else
	{
	  if (obj->class_mop != NULL)
	    {
	      if (locator_flush_class (obj->class_mop) != NO_ERROR)
		return (er_errid ());

	      if ((class_ =
		   (SM_CLASS *) locator_fetch_class (obj,
						     DB_FETCH_READ)) == NULL)
		ERROR0 (error, ER_WS_NO_CLASS_FOR_INSTANCE);
	      else
		{
		  switch (class_->class_type)
		    {
		    case SM_CLASS_CT:
		      if (locator_flush_all_instances (obj->class_mop,
						       decache) != NO_ERROR)
			error = er_errid ();
		      break;

		    case SM_VCLASS_CT:
		      if (vid_flush_all_instances (obj, decache) != NO_ERROR)
			error = er_errid ();
		      break;

		    case SM_ADT_CT:
		    case SM_PCLASS_CT:
		      /* what to do here?? */
		      break;
		    }
		}
	    }
	  else
	    {
	      if ((error =
		   au_fetch_instance (obj, &mem, AU_FETCH_READ,
				      AU_SELECT)) == NO_ERROR)
		{
		  /* don't need to pin here, we only wanted to check authorization */
		  if (obj->class_mop != NULL)
		    {
		      if (locator_flush_class (obj->class_mop) != NO_ERROR)
			return (er_errid ());

		      if ((class_ =
			   (SM_CLASS *) locator_fetch_class (obj,
							     DB_FETCH_READ))
			  == NULL)
			ERROR0 (error, ER_WS_NO_CLASS_FOR_INSTANCE);
		      else
			{
			  switch (class_->class_type)
			    {
			    case SM_CLASS_CT:
			      if (locator_flush_all_instances (obj->class_mop,
							       decache) !=
				  NO_ERROR)
				error = er_errid ();
			      break;

			    case SM_VCLASS_CT:
			      if (vid_flush_all_instances (obj, decache) !=
				  NO_ERROR)
				error = er_errid ();
			      break;

			    case SM_ADT_CT:
			    case SM_PCLASS_CT:
			      /* what to do here?? */
			      break;
			    }
			}
		    }
		  else
		    ERROR0 (error, ER_WS_NO_CLASS_FOR_INSTANCE);
		}
	    }
	}
    }

  return error;
}

/*
 * sm_flush_for_multi_update() - Flush all the dirty instances of a particular
 *    class to the server.
 *    It is invoked only in case that client updates multiple instances.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class_mop (in): class MOP
 */

int
sm_flush_for_multi_update (MOP class_mop)
{
  int success = NO_ERROR;

  if (WS_ISVID (class_mop))
    {
      /* This case must not occur. */
      /* goto error; */

      /* The second argument, decache, is false. */
      if (vid_flush_all_instances (class_mop, false) != NO_ERROR)
	{
	  goto error;
	}

      success = sm_class_check_uniques (class_mop);
      return success;
    }

  if (locator_flush_for_multi_update (class_mop) != NO_ERROR)
    {
      goto error;
    }

  return success;

error:
  return er_errid ();

}

/* WORKSPACE/GARBAGE COLLECTION SUPPORT FUNCTIONS */
/*
 * sm_issystem() - This is called by the workspace manager to see
 *    if a class is a system class.  This avoids having the ws files know about
 *    the class structure and flags.
 *   return: non-zero if class is system class
 */

int
sm_issystem (SM_CLASS * class_)
{
  return (class_->flags & SM_CLASSFLAG_SYSTEM);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_gc_domain()
 * sm_gc_attribute()
 * sm_gc_method() - GC sweep functions for structures internal to the
 * 		    class structure.
 *   return: none
 *   domain, att, meth(in): structure to examine
 *   gcmarker(in): callback function to mark referenced objects
 */

static void
sm_gc_domain (TP_DOMAIN * domain, void (*gcmarker) (MOP))
{
  TP_DOMAIN *d;

  for (d = domain; d != NULL; d = d->next)
    {
      if (d->class_mop != NULL)
	{
	  (*gcmarker) (d->class_mop);
	}
      else if (d->setdomain != NULL)
	{
	  sm_gc_domain (d->setdomain, gcmarker);
	}
    }
}

static void
sm_gc_attribute (SM_ATTRIBUTE * att, void (*gcmarker) (MOP))
{
  if (att->class_mop != NULL)
    {
      (*gcmarker) (att->class_mop);
    }

  sm_gc_domain (att->domain, gcmarker);

  pr_gc_value (&att->value, gcmarker);
  pr_gc_value (&att->original_value, gcmarker);

  if (att->properties != NULL)
    {
      pr_gc_setref (att->properties, gcmarker);
    }

  if (att->triggers != NULL)
    {
      tr_gc_schema_cache (att->triggers, gcmarker);
    }
}

static void
sm_gc_method (SM_METHOD * meth, void (*gcmarker) (MOP))
{
  SM_METHOD_SIGNATURE *sig;
  SM_METHOD_ARGUMENT *arg;

  if (meth->class_mop != NULL)
    {
      (*gcmarker) (meth->class_mop);
    }

  for (sig = meth->signatures; sig != NULL; sig = sig->next)
    {
      for (arg = sig->value; arg != NULL; arg = arg->next)
	{
	  sm_gc_domain (arg->domain, gcmarker);
	}
      for (arg = sig->args; arg != NULL; arg = arg->next)
	{
	  sm_gc_domain (arg->domain, gcmarker);
	}
    }

  if (meth->properties != NULL)
    {
      pr_gc_setref (meth->properties, gcmarker);
    }
}

/*
 * sm_gc_class() - Called by the workspace manager to perform a
 *    GC sweep on a class object.
 *   return: none
 *   mop(in): class object
 *   gcmarker(in): callback function for marking referenced objects
 */

void
sm_gc_class (MOP mop, void (*gcmarker) (MOP))
{
  SM_CLASS *class_;
  DB_OBJLIST *ml;
  SM_ATTRIBUTE *att;
  SM_METHOD *meth;
  SM_RESOLUTION *res;

  /* this is what should happen, this could be very dangerous for GC !! ?? */
  /* class = (SM_CLASS *) locator_fetch_class(mop, DB_FETCH_READ); */

  class_ = (SM_CLASS *) mop->object;
  if (class_ != NULL)
    {
      /* mops in the subclass list */
      for (ml = class_->users; ml != NULL; ml = ml->next)
	{
	  (*gcmarker) (ml->op);
	}

      /* super classes */
      for (ml = class_->inheritance; ml != NULL; ml = ml->next)
	{
	  (*gcmarker) (ml->op);
	}

      /* attributes */
      for (att = class_->attributes; att != NULL;
	   att = (SM_ATTRIBUTE *) att->header.next)
	{
	  sm_gc_attribute (att, gcmarker);
	}
      for (att = class_->shared; att != NULL;
	   att = (SM_ATTRIBUTE *) att->header.next)
	{
	  sm_gc_attribute (att, gcmarker);
	}
      for (att = class_->class_attributes; att != NULL;
	   att = (SM_ATTRIBUTE *) att->header.next)
	{
	  sm_gc_attribute (att, gcmarker);
	}

      /* methods */
      for (meth = class_->methods; meth != NULL;
	   meth = (SM_METHOD *) meth->header.next)
	{
	  sm_gc_method (meth, gcmarker);
	}
      for (meth = class_->class_methods; meth != NULL;
	   meth = (SM_METHOD *) meth->header.next)
	{
	  sm_gc_method (meth, gcmarker);
	}

      /* resolutions */
      for (res = class_->resolutions; res != NULL; res = res->next)
	{
	  if (res->class_mop != NULL)
	    {
	      (*gcmarker) (res->class_mop);
	    }
	}

      /* owner */
      if (class_->owner != NULL)
	{
	  (*gcmarker) (class_->owner);
	}

      /* properties */
      if (class_->properties != NULL)
	{
	  pr_gc_setref (class_->properties, gcmarker);
	}

      /* trigger cache */
      if (class_->triggers != NULL)
	{
	  tr_gc_schema_cache (class_->triggers, gcmarker);
	}
    }
}

/*
 * sm_gc_object() - Called by the workspace manager to do a GC sweep on
 *    an instance. Might want to have the pr_gc level functions here too.
 *   return: none
 *   mop(in): instance object
 *   gcmarker(in): callback function to mark referenced objects
 */

void
sm_gc_object (MOP mop, void (*gcmarker) (MOP))
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  char *mem;

  /* this has to be accurate at this point - think about this very
     carefully */
  if (mop->class_mop != NULL)
    {
      class_ = (SM_CLASS *) mop->class_mop->object;
      if (class_ != NULL)
	{
	  for (att = class_->attributes; att != NULL;
	       att = (SM_ATTRIBUTE *) att->header.next)
	    {
	      mem = ((char *) mop->object) + att->offset;
	      pr_gc_type (att->type, mem, gcmarker);
	    }
	}
    }
}
#endif

/*
 * sm_schema_version()
 *   return: unsigned int indicating any change in schema as none
 */

unsigned int
sm_schema_version ()
{
  return schema_version_number;
}

/*
 * sm_bump_schema_version()
 *   return: indicates global schema version has changed none
 */

void
sm_bump_schema_version ()
{
  schema_version_number++;
}

/*
 * sm_virtual_queries() - Frees a session for a class.
 *   return: SM_CLASS pointer, with valid virtual query cache a class db_object
 *   class_object(in):
 */

struct parser_context *
sm_virtual_queries (DB_OBJECT * class_object)
{
  SM_CLASS *cl;
  unsigned int current_schema_id;
  PARSER_CONTEXT *cache = NULL, *tmp = NULL;

  if (au_fetch_class_force (class_object, &cl, AU_FETCH_READ) == NO_ERROR)
    {
      (void) ws_pin (class_object, 1);

      current_schema_id = sm_schema_version ();

      if (cl->virtual_cache_schema_id != current_schema_id
	  && cl->virtual_query_cache != NULL)
	{
	  mq_free_virtual_query_cache (cl->virtual_query_cache);
	  cl->virtual_query_cache = NULL;
	}

      if (cl->class_type != SM_CLASS_CT && cl->virtual_query_cache == NULL)
	{
	  /* Okay, this is a bit of a kludge:  If there happens to be a
	   * cyclic view definition, then the virtual_query_cache will be
	   * allocated during the call to mq_virtual_queries. So, we'll
	   * assign it to a temp pointer and check it again.  We need to
	   * keep the old one and free the new one because the parser
	   * assigned originally contains the error message.
	   */
	  tmp = mq_virtual_queries (class_object);
	  if (cl->virtual_query_cache)
	    {
	      mq_free_virtual_query_cache (tmp);
	    }
	  else
	    {
	      cl->virtual_query_cache = tmp;
	    }
	  cl->virtual_cache_schema_id = current_schema_id;
	}

      cache = cl->virtual_query_cache;
    }

  return cache;
}


/*
 * sm_get_attribute_descriptor() - Find the named attribute structure
 *    in the class and return it. Lock the class with the appropriate intent.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): class or instance
 *   name(in): attribute name
 *   class_attribute(in): non-zero for locating class attributes
 *   for_update(in): non-zero if we're intending to update the attribute
 *   desc_ptr(out): returned attribute descriptor
 */

int
sm_get_attribute_descriptor (DB_OBJECT * op, const char *name,
			     int class_attribute,
			     int for_update, SM_DESCRIPTOR ** desc_ptr)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  SM_DESCRIPTOR *desc;
  MOP classmop;
  DB_FETCH_MODE class_purpose;

  att = NULL;

  if (class_attribute)
    {
      /* looking for class attribute */
      if (for_update)
	{
	  error = au_fetch_class (op, &class_, AU_FETCH_UPDATE, AU_ALTER);
	}
      else
	{
	  error = au_fetch_class (op, &class_, AU_FETCH_READ, AU_SELECT);
	}

      if (error == NO_ERROR)
	{
	  att = classobj_find_attribute (class_, name, 1);
	  if (att == NULL)
	    {
	      ERROR1 (error, ER_OBJ_INVALID_ATTRIBUTE, name);
	    }
	}
    }
  else
    {
      /* looking for an instance attribute */
      if ((error = au_fetch_class (op, &class_, AU_FETCH_READ,
				   AU_SELECT)) == NO_ERROR)
	{
	  att = classobj_find_attribute (class_, name, 0);
	  if (att == NULL)
	    {
	      ERROR1 (error, ER_OBJ_INVALID_ATTRIBUTE, name);
	    }
	  else if (att->header.name_space == ID_SHARED_ATTRIBUTE)
	    {
	      /* sigh, we didn't know that this was going to be a shared attribute
	         when we checked class authorization above, we must now upgrade
	         the lock and check for alter access.

	         Since this is logically in the name_space of the instance,
	         should we use simple AU_UPDATE authorization rather than AU_ALTER
	         even though we're technically modifying the class ?
	       */
	      if (for_update)
		{
		  error = au_fetch_class (op, &class_, AU_FETCH_UPDATE,
					  AU_ALTER);
		}
	    }
	}
    }

  if (!error && att != NULL)
    {
      /* class must have been fetched at this point */
      class_purpose = ((for_update)
		       ? DB_FETCH_CLREAD_INSTWRITE
		       : DB_FETCH_CLREAD_INSTREAD);

      classmop = (locator_is_class (op, class_purpose)) ? op : op->class_mop;

      desc = classobj_make_descriptor (classmop, class_, (SM_COMPONENT *) att,
				       for_update);
      if (desc == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  desc->next = sm_Descriptors;
	  sm_Descriptors = desc;
	  *desc_ptr = desc;
	}
    }

  return error;
}

/*
 * sm_get_method_desc() - This returns a method descriptor for the named method.
 *    The descriptor can then be used for faster access the method
 *    avoiding the name search.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op (in): class or instance
 *   name(in): method name
 *   class_method(in): class method name
 *   desc_ptr(out): returned method descirptor
 */

int
sm_get_method_descriptor (DB_OBJECT * op, const char *name,
			  int class_method, SM_DESCRIPTOR ** desc_ptr)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_METHOD *method = NULL;
  SM_DESCRIPTOR *desc;
  MOP classmop;

  if (!(error = au_fetch_class (op, &class_, AU_FETCH_READ, AU_EXECUTE)))
    {
      method = classobj_find_method (class_, name, class_method);
      if (method == NULL)
	{
	  ERROR1 (error, ER_OBJ_INVALID_METHOD, name);
	}

      /* could do the link here too ? */
    }

  if (!error && method != NULL)
    {
      /* class must have been fetched at this point */
      classmop = (locator_is_class (op, DB_FETCH_READ)) ? op : op->class_mop;

      desc = classobj_make_descriptor (classmop, class_,
				       (SM_COMPONENT *) method, 0);
      if (desc == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  desc->next = sm_Descriptors;
	  sm_Descriptors = desc;
	  *desc_ptr = desc;
	}
    }

  return error;
}

/*
 * sm_free_descriptor() - Free an attribute or method descriptor.
 *    Remember to remove it from the global descriptor list.
 *   return: none
 *   desc(in): descriptor to free
 */

void
sm_free_descriptor (SM_DESCRIPTOR * desc)
{
  SM_DESCRIPTOR *d, *prev;

  for (d = sm_Descriptors, prev = NULL; d != desc; d = d->next)
    {
      prev = d;
    }

  /* if d == NULL, the descriptor wasn't on the global list and
     is probably a suspect pointer, ignore it */
  if (d != NULL)
    {
      if (prev == NULL)
	{
	  sm_Descriptors = d->next;
	}
      else
	{
	  prev->next = d->next;
	}

      classobj_free_descriptor (d);
    }
}

/*
 * sm_invalidate_descriptors() - This is called whenever a class is edited.
 *    Or when a transaction commits.
 *    We need to mark any descriptors that reference this class as
 *    being invalid since the attribute/method structure pointers contained
 *    in the descriptor are no longer valid.
 *   return: none
 *   class(in): class being modified
 */

void
sm_reset_descriptors (MOP class_)
{
  SM_DESCRIPTOR *d;
  SM_DESCRIPTOR_LIST *dl;

  if (class_ == NULL)
    {
      /* transaction boundary, unconditionally clear all outstanding
         descriptors */
      for (d = sm_Descriptors; d != NULL; d = d->next)
	{
	  classobj_free_desclist (d->map);
	  d->map = NULL;
	}
    }
  else
    {
      /* Schema change, clear any descriptors that reference the class.
         Note, the schema manager will call this for EVERY class in the
         hierarcy.
       */
      for (d = sm_Descriptors; d != NULL; d = d->next)
	{
	  for (dl = d->map; dl != NULL && dl->classobj != class_;
	       dl = dl->next)
	    ;

	  if (dl != NULL)
	    {
	      /* found one, free the whole list */
	      classobj_free_desclist (d->map);
	      d->map = NULL;
	    }
	}
    }
}

/*
 * fetch_descriptor_class() - Work function for sm_get_descriptor_component.
 *    If the descriptor has been cleared or if we need to fetch the
 *    class and check authorization for some reason, this function obtains
 *    the appropriate locks and checks the necessary authorization.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): object
 *   desc(in): descriptor
 *   for_update(in): non-zero if we're intending to update the attribute
 *   class(out): returned class pointer
 */

static int
fetch_descriptor_class (MOP op, SM_DESCRIPTOR * desc, int for_update,
			SM_CLASS ** class_)
{
  int error = NO_ERROR;

  if (for_update)
    {
      if (desc->name_space == ID_CLASS_ATTRIBUTE
	  || desc->name_space == ID_SHARED_ATTRIBUTE)
	{
	  error = au_fetch_class (op, class_, AU_FETCH_UPDATE, AU_ALTER);
	}
      else
	{
	  error = au_fetch_class (op, class_, AU_FETCH_READ, AU_UPDATE);
	}
    }
  else
    {
      if (desc->name_space == ID_METHOD
	  || desc->name_space == ID_CLASS_METHOD)
	{
	  error = au_fetch_class (op, class_, AU_FETCH_READ, AU_EXECUTE);
	}
      else
	{
	  error = au_fetch_class (op, class_, AU_FETCH_READ, AU_SELECT);
	}
    }

  return error;
}

/*
 * sm_get_descriptor_component() - This locates an attribute structure
 *    associated with the class of the supplied object and identified
 *    by the descriptor.
 *    If the attribute has already been cached in the descriptor it is
 *    returned, otherwise, we search the class for the matching component
 *    and add it to the descriptor cache.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): object
 *   desc(in): descriptor
 *   for_update(in): non-zero if we're intending to update the attribute
 *   class_ptr(out):
 *   comp_ptr(out):
 */

int
sm_get_descriptor_component (MOP op, SM_DESCRIPTOR * desc,
			     int for_update,
			     SM_CLASS ** class_ptr, SM_COMPONENT ** comp_ptr)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_COMPONENT *comp;
  SM_DESCRIPTOR_LIST *d, *prev, *new_;
  MOP classmop;
  int class_component;

  /* handle common case quickly, allow either an instance MOP or
     class MOP to be used here */
  if (desc->map != NULL
      && (desc->map->classobj == op || desc->map->classobj == op->class_mop)
      && (!for_update || desc->map->write_access))
    {
      *comp_ptr = desc->map->comp;
      *class_ptr = desc->map->class_;
    }
  else
    {
      /* this is set when a fetch is performed, try to avoid if possible */
      class_ = NULL;

      /* get the class MOP for this thing, avoid fetching if possible */
      if (op->class_mop == NULL)
	{
	  if (fetch_descriptor_class (op, desc, for_update, &class_))
	    {
	      return er_errid ();
	    }
	}
      classmop = (IS_CLASS_MOP (op)) ? op : op->class_mop;

      /* search the descriptor map for this class */
      for (d = desc->map, prev = NULL; d != NULL && d->classobj != classmop;
	   d = d->next)
	{
	  prev = d;
	}

      if (d != NULL)
	{
	  /* found an existing one, move it to the head of the list */
	  if (prev != NULL)
	    {
	      prev->next = d->next;
	      d->next = desc->map;
	      desc->map = d;
	    }
	  /* check update authorization if we haven't done it yet */
	  if (for_update && !d->write_access)
	    {
	      if (class_ == NULL)
		{
		  if (fetch_descriptor_class (op, desc, for_update, &class_))
		    {
		      return er_errid ();
		    }
		}
	      d->write_access = 1;
	    }
	  *comp_ptr = d->comp;
	  *class_ptr = d->class_;
	}
      else
	{
	  /* not on the list, fetch it if we haven't already done so */
	  if (class_ == NULL)
	    {
	      if (fetch_descriptor_class (op, desc, for_update, &class_))
		{
		  return er_errid ();
		}
	    }

	  class_component = (desc->name_space == ID_CLASS_ATTRIBUTE
			     || desc->name_space == ID_CLASS_METHOD);
	  comp = classobj_find_component (class_, desc->name,
					  class_component);
	  if (comp == NULL)
	    {
	      if (desc->name_space == ID_METHOD
		  || desc->name_space == ID_CLASS_METHOD)
		{
		  ERROR1 (error, ER_OBJ_INVALID_METHOD, desc->name);
		}
	      else
		{
		  ERROR1 (error, ER_OBJ_INVALID_ATTRIBUTE, desc->name);
		}
	    }
	  else
	    {
	      /* make a new descriptor and add it to the head of the list */
	      new_ = classobj_make_desclist (classmop, class_, comp,
					     for_update);
	      if (new_ == NULL)
		{
		  error = er_errid ();
		}
	      else
		{
		  new_->next = desc->map;
		  desc->map = new_;
		  *comp_ptr = comp;
		  *class_ptr = class_;
		}
	    }
	}
    }

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)	/* to disable TEXT */
/*
 * sm_has_text_domain() - Check if it is a TEXT typed attribute
 *   return: 1 if it has TEXT or 0
 *   attribute(in): attributes to check a domain
 *   check_all(in): scope to check a domain, 1 if all check, or 0
 */
int
sm_has_text_domain (DB_ATTRIBUTE * attributes, int check_all)
{
  DB_ATTRIBUTE *attr;
  DB_OBJLIST *supers;
  DB_OBJECT *domain;

  attr = attributes;
  while (attr)
    {
      if (db_attribute_type (attr) == DB_TYPE_OBJECT)
	{
	  domain = db_domain_class (db_attribute_domain (attr));
	  if (domain)
	    {
	      supers = db_get_superclasses (domain);
	      if (supers && supers->op
		  && (intl_mbs_casecmp (db_get_class_name (supers->op),
					"db_text") == 0))
		{
		  return true;
		}
	    }
	}
      if (!check_all)
	{
	  break;
	}
      attr = db_attribute_next (attr);
    }

  return false;
}
#endif /* ENABLE_UNUSED_FUNCTION */


/* NAME SEARCHERS */
/*
 * template_classname() - Shorthand function for calls to er_set.
 *    Get the class name for the class associated with a template.
 *   return: class name
 *   template(in): schema template
 */

static const char *
template_classname (SM_TEMPLATE * template_)
{
  const char *name;

  name = template_->name;
  if (name == NULL && template_->op != NULL)
    {
      name = sm_class_name (template_->op);
    }

  return name;
}

/*
 * candidate_source_name() - Shorthand function to determine the class name
 *    that is the source of the given candidate.
 *   return: class name
 *   template(in): template for class being edited
 *   candidate(in): candidate of interest
 */

static const char *
candidate_source_name (SM_TEMPLATE * template_, SM_CANDIDATE * candidate)
{
  const char *name = NULL;

  if (candidate->source != NULL)
    {
      name = sm_class_name (candidate->source);
    }
  else
    {
      if (template_->name != NULL)
	{
	  name = template_->name;
	}
      else if (template_->op != NULL)
	{
	  name = sm_class_name (template_->op);
	}
    }

  return name;
}

/* DOMAIN COMPARISON */

/*
 * find_superclass() - searches up the class hierarchy looking for a superclass.
 *    If a superclass has a template pending, we use the superclass list
 *    of the template rather than the real superclass list so that we can
 *    recognize domain compatibility in a schema operation that has not
 *    yet been fully applied.
 *   return: non-zero if the superclass was found
 *   classop(in): class object (if passed, temp should be NULL)
 *   temp(in): template (if passed, classop should be NULL)
 *   super(in): super class we're looking for
 */

static int
find_superclass (DB_OBJECT * classop, SM_TEMPLATE * temp, DB_OBJECT * super)
{
  DB_OBJLIST *super_list, *el;
  SM_CLASS *class_;
  int status = 0;

  super_list = NULL;

  if (classop != NULL)
    {
      /* fetch the class and check for a pending template */
      if (au_fetch_class_force (classop, &class_, AU_FETCH_READ) != NO_ERROR)
	{
	  return 0;
	}
      if (class_->new_ != NULL)
	{
	  /* its got a template, use the pending inheritance list */
	  super_list = class_->new_->inheritance;
	}
      else
	{
	  /* no template, use the real inheritance list */
	  super_list = class_->inheritance;
	}
    }
  else if (temp != NULL)
    {
      /* use the inheritance list of the supplied template */
      super_list = temp->inheritance;
    }

  /* search immediate superclasses first */
  for (el = super_list; el != NULL && !status; el = el->next)
    {
      if (el->op == super)
	{
	  status = 1;
	}
    }
  if (!status)
    {
      /* Look all the way up the hierarchy, could be doing this in the
         previous loop but lets try to make the detection of immediate
         superclasses fast as it is likely to be the most common.
         Recurse so we recognize pending templates on the way up.
       */
      for (el = super_list; el != NULL && !status; el = el->next)
	{
	  status = find_superclass (el->op, NULL, super);
	}
    }

  return status;
}

/*
 * compare_domains() - Compare two domains and calculate the appropriate
 *    comparison code.
 *    The result indicates the state of d1 relative to d2, that is, if the
 *    result is DC_MORE_SPECIFIC it indicates that d1 is more specific
 *    than d2.
 *    If a domain comes in whose type is tp_Type_null, the domain is actually
 *    a tp_Type_object domain for a class that has not yet been created.
 *    In this case, the "class" field of the TP_DOMAIN structure will point
 *    to the template for the new class.
 *   return: domain comparison code
 *   d1(in): domain structure
 *   d2(in): domain structure
 */

static DOMAIN_COMP
compare_domains (TP_DOMAIN * d1, TP_DOMAIN * d2)
{
  DOMAIN_COMP status = DC_INCOMPATIBLE;

  if (d1->type == tp_Type_null || d2->type == tp_Type_null)
    {
      /* domain comparison involving classes that haven't been created yet */
      if (d1->type == d2->type)
	{
	  if (d1->class_mop == d2->class_mop)
	    {
	      status = DC_EQUAL;
	    }
	  /* else, you can't create two different classes in the same template
	     so this can never happen */
	}
      else if (d1->type == tp_Type_null)
	{
	  if (d2->type != tp_Type_object)
	    {
	      status = DC_INCOMPATIBLE;
	    }
	  else if (d2->class_mop == NULL)
	    {
	      status = DC_MORE_SPECIFIC;
	    }
	  else
	    {
	      /* If d2->class is accessible by scanning upwards from
	         the inheritance list of the template, then d1 is in the
	         process of becoming a subtype of d2 and is therefore more
	         specific.
	       */
	      if (find_superclass (NULL, (SM_TEMPLATE *) (d1->class_mop),
				   d2->class_mop))
		{
		  status = DC_MORE_SPECIFIC;
		}
	    }
	}
      else
	{
	  /* same as previous clause except the polarity is reversed */
	  if (d1->type != tp_Type_object)
	    {
	      status = DC_INCOMPATIBLE;
	    }
	  else if (d1->class_mop == NULL)
	    {
	      status = DC_LESS_SPECIFIC;
	    }
	  else
	    {
	      if (find_superclass (NULL, (SM_TEMPLATE *) (d2->class_mop),
				   d1->class_mop))
		{
		  status = DC_LESS_SPECIFIC;
		}
	    }
	}
    }
  else if (d1->type == d2->type)
    {
      if (d1->type == tp_Type_object)
	{
	  if (d1->class_mop == d2->class_mop)
	    {
	      status = DC_EQUAL;
	    }
	  else if (d1->class_mop == NULL)
	    {
	      status = DC_LESS_SPECIFIC;
	    }
	  else if (d2->class_mop == NULL)
	    {
	      status = DC_MORE_SPECIFIC;
	    }
	  else if (find_superclass (d1->class_mop, NULL, d2->class_mop))
	    {
	      status = DC_MORE_SPECIFIC;
	    }
	  else if (find_superclass (d2->class_mop, NULL, d1->class_mop))
	    {
	      status = DC_LESS_SPECIFIC;
	    }
	  else
	    {
	      status = DC_INCOMPATIBLE;
	    }
	}
      else if (pr_is_set_type (d1->type->id))
	{
	  /* set element domains must be compatible */
	  status = DC_EQUAL;
	}
      else
	{
	  status = DC_EQUAL;
	}
    }

  return status;
}

/*
 * find_argument() - Helper function for compare_argument_domains.
 *    Locate an argument by number.  These need to be stored in
 *    an array for easier lookup.
 *   return: argument structure
 *   sig(in): method signature
 *   argnum(in): argument index
 */

static SM_METHOD_ARGUMENT *
find_argument (SM_METHOD_SIGNATURE * sig, int argnum)
{
  SM_METHOD_ARGUMENT *arg;

  for (arg = sig->args; arg != NULL && arg->index != argnum; arg = arg->next)
    ;

  return arg;
}

/*
 * compare_argument_domains() - This compares the argument lists of two methods
 *    to see if they are compatible.
 *    Currently this is defined so that the arguments must match
 *    exactly.  Eventually this could support the notion of "contravariance"
 *    in the signature.
 *   return: domain comparison code
 *   m1(in): method 1
 *   m2(in): method 2
 */

static DOMAIN_COMP
compare_argument_domains (SM_METHOD * m1, SM_METHOD * m2)
{
  DOMAIN_COMP status, arg_status;
  SM_METHOD_SIGNATURE *sig1, *sig2;
  SM_METHOD_ARGUMENT *arg1, *arg2;
  int i;

  status = DC_EQUAL;

  sig1 = m1->signatures;
  sig2 = m2->signatures;

  /* If both signatures are NULL, assume its ok, this is largely
     for backward compatibility.   */
  if (sig1 == NULL || sig2 == NULL)
    {
      if (sig1 != sig2)
	{
	  status = DC_INCOMPATIBLE;
	}
    }
  else if (sig1->num_args == sig2->num_args)
    {
      /* Since the arguments aren't set stored in an array, lookup
         is harder than it should be.  Recall that arg indexes start with 1 */
      for (i = 1; i <= sig1->num_args && status == DC_EQUAL; i++)
	{
	  arg1 = find_argument (sig1, i);
	  arg2 = find_argument (sig2, i);

	  /* if either arg is missing, could assume its a "void" and allow it */
	  if (arg1 == NULL || arg2 == NULL)
	    {
	      if (arg1 != arg2)
		{
		  status = DC_INCOMPATIBLE;
		}
	    }
	  else
	    {
	      arg_status = compare_domains (arg1->domain, arg2->domain);
	      if (arg_status != DC_EQUAL)
		{
		  status = DC_INCOMPATIBLE;
		}
	    }
	}
    }

  return status;
}

/*
 * compare_component_domains() - Compare the domains of two components and
 *    return an appropriate comparison code.
 *    The result of this function indicates the state of c1 relative to
 *    c2, that is, if the result is ER_DOMAIN_LESS_SPECIFIC it means that
 *    c1 is less specific than c2.
 *   return: domain comparison code
 *   c1(in): component
 *   c2(in): component
 */

static DOMAIN_COMP
compare_component_domains (SM_COMPONENT * c1, SM_COMPONENT * c2)
{
  DOMAIN_COMP arg_status, status = DC_INCOMPATIBLE;
  SM_ATTRIBUTE *a1, *a2;
  SM_METHOD *m1, *m2;
  TP_DOMAIN *d1, *d2;

  if (c1->name_space == ID_METHOD || c1->name_space == ID_CLASS_METHOD)
    {
      if (c2->name_space == c1->name_space)
	{
	  /* compare return argument domains, should do full argument
	     signatures as well !
	     be careful here because methods don't always have domains specified */
	  m1 = (SM_METHOD *) c1;
	  m2 = (SM_METHOD *) c2;
	  d1 = NULL;
	  d2 = NULL;
	  if (m1->signatures != NULL && m1->signatures->value != NULL)
	    {
	      d1 = m1->signatures->value->domain;
	    }
	  if (m2->signatures != NULL && m2->signatures->value != NULL)
	    {
	      d2 = m2->signatures->value->domain;
	    }

	  if (d1 != NULL && d2 != NULL)
	    {
	      status = compare_domains (d1, d2);
	    }
	  else if (d1 == NULL && d2 == NULL)
	    {
	      /* neither specified, assume the same */
	      status = DC_EQUAL;
	    }
	  else
	    {
	      /* for now, if either method has no domain, assume its ok.  this
	         happens a lot with the multimedia classes and will happen when
	         using db_add_method before the argument domains are
	         fully specified */
	      status = DC_EQUAL;
	    }

	  if (status != DC_INCOMPATIBLE)
	    {
	      arg_status = compare_argument_domains (m1, m2);
	      if (arg_status != DC_EQUAL)
		{
		  status = DC_INCOMPATIBLE;
		}
	    }
	}
    }
  else
    {
      /* allow combination of instance/shared but not instance/class */
      if (c1->name_space == c2->name_space ||
	  (c1->name_space != ID_CLASS_ATTRIBUTE
	   && c2->name_space != ID_CLASS_ATTRIBUTE))
	{
	  /* regular, shared, or class attribute, these must have domains */
	  a1 = (SM_ATTRIBUTE *) c1;
	  a2 = (SM_ATTRIBUTE *) c2;
	  status = compare_domains (a1->domain, a2->domain);
	}
    }

  return status;
}

/* CANDIDATE STRUCTURE MAINTENANCE */

/*
 * make_candidate_from_component() - Construct a candidate structure from
 * 				     a class component.
 *   return: candidate structure
 *   comp(in): component (attribute or method)
 *   source(in): MOP of source class (immediate super class)
 */

static SM_CANDIDATE *
make_candidate_from_component (SM_COMPONENT * comp, MOP source)
{
  SM_CANDIDATE *candidate;

  candidate = (SM_CANDIDATE *) db_ws_alloc (sizeof (SM_CANDIDATE));
  if (candidate != NULL)
    {
      candidate->next = NULL;
      candidate->name = comp->name;
      candidate->alias = NULL;
      candidate->name_space = comp->name_space;
      candidate->source = source;
      candidate->obj = comp;
      candidate->is_alias = 0;
      candidate->is_requested = 0;
      candidate->order = 0;

      if (comp->name_space == ID_METHOD
	  || comp->name_space == ID_CLASS_METHOD)
	{
	  candidate->origin = ((SM_METHOD *) comp)->class_mop;
	}
      else
	{
	  candidate->origin = ((SM_ATTRIBUTE *) comp)->class_mop;
	}
    }

  return (candidate);
}

/*
 * free_candidates() - Free a list of candidates structures
 * 		       when done with schema flattening.
 *   return: none
 *   candidates(in): candidates list
 */

static void
free_candidates (SM_CANDIDATE * candidates)
{
  SM_CANDIDATE *c, *next;

  for (c = candidates, next = NULL; c != NULL; c = next)
    {
      next = c->next;
      db_ws_free (c);
    }
}

/*
 * prune_candidate() - This will remove the first candidate in the list AND
 *    all other candidates in the list that have the same name.  The list of
 *    candidates with the same name as the first candidate as returned.
 *    The source list is destructively modified to remove the pruned
 *    candidates.
 *   return: pruned candidates
 *   clist_pointer (in): source candidates list
 */

static SM_CANDIDATE *
prune_candidate (SM_CANDIDATE ** clist_pointer)
{
  SM_CANDIDATE *candidates, *head;

  candidates = NULL;
  head = *clist_pointer;
  if (head != NULL)
    {
      candidates =
	(SM_CANDIDATE *) nlist_filter ((DB_NAMELIST **) clist_pointer,
				       head->name,
				       (NLSEARCHER) SM_COMPARE_NAMES);
    }

  return candidates;
}

/*
 * add_candidate() - This adds a candidate structure for the component to
 *    the candidates list.
 *    If the component has an alias resolution in the resolution list,
 *    the candidate is marked as being aliased and an additional candidate
 *    is added to the list with the alias name.
 *   return: none
 *   candlist(in/out): pointer to candidate list head
 *   comp(in): component to build a candidate for
 *   order(in): the definition order of this candidate
 *   source(in): the source class of the candidate
 *   resolutions(in): resolution list in effect
 */

static void
add_candidate (SM_CANDIDATE ** candlist, SM_COMPONENT * comp, int order,
	       MOP source, SM_RESOLUTION * resolutions)
{
  SM_CANDIDATE *new_;
  SM_RESOLUTION *res;

  new_ = make_candidate_from_component (comp, source);
  if (new_ == NULL)
    {
      return;
    }

  new_->order = order;
  new_->next = *candlist;
  *candlist = new_;

  /* check the resolution list to see if there are any aliases for this
     component */
  res = classobj_find_resolution (resolutions, source, comp->name, ID_NULL);
  if (res != NULL)
    {
      if (res->alias == NULL)
	{
	  /* mark the component as being specifically requested */
	  new_->is_requested = 1;
	}
      else
	{
	  /* mark the candidate as having an alias */
	  new_->alias = res->alias;
	  /* make an entry in the candidates list for the alias */
	  new_ = make_candidate_from_component (comp, source);
	  if (new_ == NULL)
	    {
	      return;
	    }
	  new_->name = res->alias;
	  new_->is_alias = 1;
	  new_->order = order;
	  new_->next = *candlist;
	  *candlist = new_;
	}
    }
}

/*
 * make_component_from_candidate() - Called after candidate flattening
 *    to construct an actual class component for a flattened candidate.
 *   return: class component
 *   classop(in): class being defined
 *   cand(in): candidate structure
 */

static SM_COMPONENT *
make_component_from_candidate (MOP classop, SM_CANDIDATE * cand)
{
  SM_COMPONENT *new_;
  SM_ATTRIBUTE *att = NULL;
  SM_METHOD *method = NULL;
  SM_NAME_SPACE space;

  new_ = NULL;

  space = cand->obj->name_space;
  if (space == ID_METHOD || space == ID_CLASS_METHOD)
    {
      method = classobj_copy_method ((SM_METHOD *) cand->obj, NULL);
      if (method == NULL)
	{
	  return NULL;
	}
      new_ = (SM_COMPONENT *) method;
      method->order = cand->order;

      /* if this is an inherited component, clear out certain fields that
         don't get inherited automatically */
      if (cand->source != NULL && cand->source != classop)
	{
	  method->id = -1;
	}
    }
  else
    {
      att = classobj_copy_attribute ((SM_ATTRIBUTE *) cand->obj, NULL);
      if (att == NULL)
	{
	  return NULL;
	}
      new_ = (SM_COMPONENT *) att;
      att->order = cand->order;

      /* !! ALWAYS CLEAR THIS, ITS A RUN TIME ONLY FLAG AND CAN'T
         MAKE IT TO DISK */
      att->flags &= ~SM_ATTFLAG_NEW;

      /* if this is an inherited component, clear out certain fields that
         don't get inherited automatically.
         We now allow the UNIQUE constraint to be inherited but not INDEX */

      if (cand->source != NULL && cand->source != classop)
	{
	  att->id = -1;		/* must reassign this */
	}
    }

  /* if this is an alias candidate, change the name */
  if (cand->is_alias)
    {
      ws_free_string (new_->name);
      new_->name = ws_copy_string (cand->name);
      if (new_->name == NULL)
	{
	  if (method)
	    {
	      classobj_free_method (method);
	    }

	  if (att)
	    {
	      classobj_free_attribute (att);
	    }
	  new_ = NULL;
	}
    }

  return new_;
}

/* CANDIDATE GATHERING */
/*
 * get_candidates() - This builds a candidates list for either the instance
 *    or class name_space.  The candidates list is the raw flattened list of all
 *    the attribute and method definitions in the name_space.
 *    Each candidate is tagged with an order counter so that the definition
 *    order can be preserved in the resulting class.  Although attributes
 *    and methods are included on the same candidates list, they are ordered
 *    separately.
 *   return: candidates list
 *   def(in): original template
 *   flag(in): flattened template (in progress)
 *   namespace(in): ID_CLASS or ID_INSTANCE
 */

static SM_CANDIDATE *
get_candidates (SM_TEMPLATE * def, SM_TEMPLATE * flat,
		SM_NAME_SPACE name_space)
{
  SM_COMPONENT *complist, *comp;
  SM_RESOLUTION *reslist;
  SM_ATTRIBUTE *att;
  SM_CANDIDATE *candlist;
  DB_OBJLIST *super;
  SM_CLASS *sclass;
  int att_order, meth_order;

  candlist = NULL;
  /* get appropriate resolution list from the flattened template */
  if (name_space == ID_CLASS)
    {
      reslist = flat->class_resolutions;
    }
  else
    {
      reslist = flat->resolutions;
    }

  /* initialize the component order counters */
  att_order = 0;
  meth_order = 0;

  /* go left to right through the supers adding the components in order */
  for (super = def->inheritance; super != NULL; super = super->next)
    {
      if (au_fetch_class_force (super->op, &sclass, AU_FETCH_READ) !=
	  NO_ERROR)
	{
	  continue;
	}

      if (name_space == ID_CLASS)
	{
	  /* add the class attributes */
	  complist = (SM_COMPONENT *) ((sclass->new_ == NULL) ?
				       sclass->class_attributes :
				       sclass->new_->class_attributes);
	  for (comp = complist; comp != NULL; comp = comp->next, att_order++)
	    {
	      add_candidate (&candlist, comp, att_order, super->op, reslist);
	    }

	  /* add the class methods */
	  complist = (SM_COMPONENT *) ((sclass->new_ == NULL) ?
				       sclass->class_methods :
				       sclass->new_->class_methods);
	  for (comp = complist; comp != NULL; comp = comp->next, meth_order++)
	    {
	      add_candidate (&candlist, comp, meth_order, super->op, reslist);
	    }
	}
      else
	{
	  /* add the instance and shared attributes, the template is ordered */
	  if (sclass->new_ != NULL)
	    {
	      for (att = sclass->new_->attributes; att != NULL;
		   att = (SM_ATTRIBUTE *) att->header.next, att_order++)
		{
		  add_candidate (&candlist, (SM_COMPONENT *) att, att_order,
				 super->op, reslist);
		}
	    }
	  else
	    {
	      /* get these from the ordered list ! */
	      for (att = sclass->ordered_attributes; att != NULL;
		   att = att->order_link, att_order++)
		{
		  add_candidate (&candlist, (SM_COMPONENT *) att, att_order,
				 super->op, reslist);
		}
	    }
	  /* add the instance methods */
	  complist = (SM_COMPONENT *) ((sclass->new_ == NULL) ?
				       sclass->methods :
				       sclass->new_->methods);
	  for (comp = complist; comp != NULL; comp = comp->next, meth_order++)
	    {
	      add_candidate (&candlist, comp, meth_order, super->op, reslist);
	    }
	}
    }

  /* get local definition component list */
  if (name_space == ID_CLASS)
    {
      /* add local class attributes */
      complist = (SM_COMPONENT *) def->class_attributes;
      for (comp = complist; comp != NULL; comp = comp->next, att_order++)
	{
	  add_candidate (&candlist, comp, att_order, def->op, NULL);
	}

      /* add local class methods */
      complist = (SM_COMPONENT *) def->class_methods;
      for (comp = complist; comp != NULL; comp = comp->next, meth_order++)
	{
	  add_candidate (&candlist, comp, meth_order, def->op, NULL);
	}
    }
  else
    {
      /* add local attributes */
      complist = (SM_COMPONENT *) def->attributes;
      for (comp = complist; comp != NULL; comp = comp->next, att_order++)
	{
	  add_candidate (&candlist, comp, att_order, def->op, NULL);
	}

      /* add local methods */
      complist = (SM_COMPONENT *) def->methods;
      for (comp = complist; comp != NULL; comp = comp->next, meth_order++)
	{
	  add_candidate (&candlist, comp, meth_order, def->op, NULL);
	}
    }

  return candlist;
}

/*
 * CANDIDATE LIST RULES
 * These functions map over a pruned candidates list checking for various
 * rules of inheritance.  Some rules are checked in the more complex
 * function resolve_candidates(), so that better error messages can
 * be produced.
 */

/*
 * check_attribute_method_overlap() - This checks the candidates in the list
 *    to see if there are any attributes and methods with the same name.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): template of class being edited (used for class name only)
 *   candidates(in): candidate list
 */

static int
check_attribute_method_overlap (SM_TEMPLATE * template_,
				SM_CANDIDATE * candidates)
{
  int error = NO_ERROR;
  SM_CANDIDATE *att_cand, *meth_cand, *c;

  att_cand = meth_cand = NULL;

  for (c = candidates; c != NULL && error == NO_ERROR; c = c->next)
    {
      if (c->name_space == ID_METHOD || c->name_space == ID_CLASS_METHOD)
	{
	  meth_cand = c;
	  if (att_cand != NULL)
	    {
	      ERROR3 (error, ER_SM_INCOMPATIBLE_COMPONENTS, c->name,
		      candidate_source_name (template_, att_cand),
		      candidate_source_name (template_, c));
	    }
	}
      else
	{
	  att_cand = c;
	  if (meth_cand != NULL)
	    {
	      ERROR3 (error, ER_SM_INCOMPATIBLE_COMPONENTS, c->name,
		      candidate_source_name (template_, c),
		      candidate_source_name (template_, meth_cand));
	    }
	}
    }

  return error;
}

/*
 * check_alias_conflict() - This checks for candidates that were produced
 *    by aliasing an inherited component. If an alias is defined, there can be
 *    only one component with that name.  Two inherited components cannot
 *    have the same alias and an alias cannot conflict with a "real"
 *    component.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): class template (used for name only)
 *   candidates(in): candidates list
 */

static int
check_alias_conflict (SM_TEMPLATE * template_, SM_CANDIDATE * candidates)
{
  int error = NO_ERROR;
  SM_CANDIDATE *c, *normal, *alias;

  normal = alias = NULL;

  for (c = candidates; c != NULL && error == NO_ERROR; c = c->next)
    {
      /* ignore candidates that have been aliased to something else */
      if (c->alias == NULL)
	{
	  /* is this candidate using an alias name ? */
	  if (c->is_alias == 0)
	    {
	      /* could be smarter and recognize most specific domains
	       * and shadowing in case we get one of the error conditions below                */
	      normal = c;
	    }
	  else
	    {
	      if (alias != NULL)
		{
		  /* Alias name `%1s' is used more than once. */
		  ERROR1 (error, ER_SM_MULTIPLE_ALIAS, alias->name);
		}
	      else
		{
		  alias = c;
		}
	    }
	}
    }

  if (error == NO_ERROR && alias != NULL && normal != NULL)
    {

      if (normal->source == NULL || normal->source == template_->op)
	{
	  /* Can't use "alias" as an alias for inherited component "name", there
	     is already a locally defined component with that name */
	  ERROR2 (error, ER_SM_ALIAS_COMPONENT_EXISTS,
		  alias->name, alias->obj->name);
	}
      else
	{
	  /* Can't use `%1$s' as an alias for `%2$s' of `%3$s'.
	     A component with that name is already inherited from `%4s'.
	   */
	  ERROR4 (error, ER_SM_ALIAS_COMPONENT_INHERITED,
		  alias->name, alias->obj->name,
		  candidate_source_name (template_, alias),
		  candidate_source_name (template_, normal));
	}
    }

  return error;
}

/*
 * check_alias_domains() - This checks the domains of all candidates in the
 *    list that have been given aliases.
 *    Candidates with aliases will be ignored during resolution.
 *    The rule is however that if a candidate is aliased,
 *    there must be an appropriate substitute candidate from another class.
 *    This function first checks to make sure that all of the aliased
 *    candidates have compatible domains.
 *    While this check is being done, the alias with the most specific domain
 *    is found.  The domain of the alias substitute must be at least
 *    as specific as the domains of all the candidates that were aliased.
 *    This last test is performed at the end of resolve_candidates().
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): class template (used for name only)
 *   candidates(in): candidates list
 *   most_specific(out): return pointer to most specific aliased candidate
 */

static int
check_alias_domains (SM_TEMPLATE * template_,
		     SM_CANDIDATE * candidates, SM_CANDIDATE ** most_specific)
{
  int error = NO_ERROR;
  SM_CANDIDATE *c, *most;
  DOMAIN_COMP dstate;

  most = NULL;
  for (c = candidates; c != NULL && error == NO_ERROR; c = c->next)
    {
      if (c->alias != NULL)
	{			/* only look at candidates that are aliased */
	  if (most == NULL)
	    {
	      most = c;
	    }
	  else if (c->origin != most->origin)
	    {
	      dstate = compare_component_domains (c->obj, most->obj);
	      switch (dstate)
		{
		case DC_INCOMPATIBLE:
		  ERROR4 (error, ER_SM_INCOMPATIBLE_DOMAINS, c->name,
			  candidate_source_name (template_, most),
			  candidate_source_name (template_, c),
			  template_classname (template_));
		  break;

		case DC_MORE_SPECIFIC:
		  most = c;
		  break;

		case DC_EQUAL:
		case DC_LESS_SPECIFIC:
		  /* ignore it */
		  break;
		}
	    }
	}
    }

  *most_specific = most;
  return error;
}

/* CANDIDATE RESOLUTION */
/*
 * auto_resolve_conflict() - Add (or modify an existing) resolution for
 *    the candidate to a resolution list.
 *   return: none
 *   candiate(in): candidate needing resolution
 *   resolutions(in/out): pointer to resolution list
 *   resspace(in): resolution space (class or instance)
 */

static void
auto_resolve_conflict (SM_CANDIDATE * candidate, SM_RESOLUTION ** resolutions,
		       SM_NAME_SPACE resspace)
{
  SM_RESOLUTION *res, *found;

  found = NULL;
  for (res = *resolutions; res != NULL && found == NULL; res = res->next)
    {
      if (res->name_space == resspace
	  && (SM_COMPARE_NAMES (res->name, candidate->name) == 0))
	{
	  if (res->alias == NULL)
	    {
	      found = res;
	    }
	}
    }
  if (found != NULL)
    {
      /* adjust the existing resolution to point at the new class */
      found->class_mop = candidate->source;
    }
  else
    {
      /* generate a new resolution */
      res = classobj_make_resolution (candidate->source, candidate->name,
				      NULL, resspace);
      if (res)
	{
	  res->next = *resolutions;
	}
      *resolutions = res;
    }
}

/*
 * resolve_candidates() - This is the main function for checking component
 *    combination rules. Given a list of candidates, all of the rules for
 *    compatibility are checked and a winner is determined if there is
 *    more than one possible candidate.
 *    If any of the rules fail, an error code is returned.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): schema template
 *   candidates(in): candidates list
 *   auto_resolve(in): non-zero to enable auto resolution of conflicts
 *   winner_return(out): returned pointer to winning candidates
 */

static int
resolve_candidates (SM_TEMPLATE * template_,
		    SM_CANDIDATE * candidates,
		    int auto_resolve, SM_CANDIDATE ** winner_return)
{
  int error = NO_ERROR;
  SM_CANDIDATE *winner, *c, *requested, *conflict, *local, *alias;
  SM_NAME_SPACE resspace;
  DOMAIN_COMP dstate;

  winner = NULL;
  alias = NULL;
  requested = NULL;
  local = NULL;
  conflict = NULL;

  /* first check some simple rules */
  if ((error = check_attribute_method_overlap (template_, candidates)))
    {
      return error;
    }

  if ((error = check_alias_conflict (template_, candidates)))
    {
      return error;
    }

  if ((error = check_alias_domains (template_, candidates, &alias)))
    {
      return error;
    }

  /* look for a local & requested component */
  for (c = candidates; c != NULL; c = c->next)
    {
      if (c->source == NULL || c->source == template_->op)
	{
	  /* if local is not NULL here, its technically an error */
	  local = c;
	}
      if (c->is_requested)
	{
	  /* if local is not NULL here, its technically an error */
	  requested = c;
	}
    }

  /* establish an initial winner if possible */
  if (local == NULL)
    {
      winner = requested;
    }
  else
    {
      winner = local;
      /* this means that we found a resolution for an inherited attribute
         but we also have a local definition, in this case the resolution
         has no effect and is deleted */
      /* remove_invalid_resolution(template, requested); */
      requested = NULL;
    }

  /* loop through the non-aliases candidates looking for a winner
     should detect aliases that are ignored because of a local definition
     and remove them from the resolution list ! - try to handle
     this during template building
   */

  for (c = candidates; c != NULL && error == NO_ERROR; c = c->next)
    {
      if (c->alias == NULL)
	{
	  if (winner == NULL)
	    {
	      winner = c;
	    }
	  else if (c != winner && c->origin != winner->origin)
	    {
	      dstate = compare_component_domains (c->obj, winner->obj);
	      switch (dstate)
		{
		case DC_INCOMPATIBLE:
		  if (local == NULL)
		    /* incompatibility between two inherited things */
		    ERROR4 (error, ER_SM_INCOMPATIBLE_DOMAINS, winner->name,
			    candidate_source_name (template_, winner),
			    candidate_source_name (template_, c),
			    template_classname (template_));
		  else
		    {
		      /* incompatiblity between inherited thing and a locally
		         defined thing */
		      ERROR3 (error, ER_SM_INCOMPATIBLE_SHADOW, winner->name,
			      candidate_source_name (template_, c),
			      template_classname (template_));
		    }
		  break;
		case DC_MORE_SPECIFIC:
		  if (local != NULL)
		    {
		      /* trying to shadow an inherited attribute with a more specific
		         domain */
		      ERROR3 (error, ER_SM_INCOMPATIBLE_SHADOW, winner->name,
			      candidate_source_name (template_, c),
			      template_classname (template_));
		    }
		  else
		    {
		      /* must override requested resolution or issue error */
		      if (winner != requested || auto_resolve)
			{
			  winner = c;
			  /* reset conflict when upgrading the domain of the winner */
			  conflict = NULL;
			}
		      else
			{
			  /* can't override resolution on <attname> of <classname> with
			     required attribute from <classname2> */
			  ERROR4 (error, ER_SM_RESOLUTION_OVERRIDE,
				  winner->name,
				  candidate_source_name (template_, winner),
				  candidate_source_name (template_, c),
				  template_classname (template_));
			}
		    }
		  break;
		case DC_EQUAL:
		  /* remember the conflict for later, it may be ignored if there
		     is another candidate with a more specific domain */
		  if (local == NULL && winner != requested)
		    conflict = c;
		  break;
		case DC_LESS_SPECIFIC:
		  /* ignore it */
		  break;
		}
	    }
	}
    }

  /* check for compatibility with any aliased components */
  if (error == NO_ERROR && alias != NULL)
    {
      if (winner == NULL)
	{
	  ERROR3 (error, ER_SM_MISSING_ALIAS_SUBSTITUTE, alias->name,
		  candidate_source_name (template_, alias),
		  template_classname (template_));
	}
      else
	{
	  dstate = compare_component_domains (winner->obj, alias->obj);
	  if (dstate == DC_INCOMPATIBLE)
	    {
	      /* we need to differentiate between a local reference
	       * conflicting with an alias so that we can give a
	       * better error message.
	       */
	      if (local == winner)
		{
		  ERROR3 (error, ER_SM_INCOMPATIBLE_ALIAS_LOCAL_SUB,
			  winner->name, candidate_source_name (template_,
							       alias),
			  template_classname (template_));
		}
	      else
		{
		  ERROR4 (error, ER_SM_INCOMPATIBLE_ALIAS_SUBSTITUTE,
			  winner->name, candidate_source_name (template_,
							       winner),
			  candidate_source_name (template_, alias),
			  template_classname (template_));
		}
	    }
	  else if (dstate == DC_LESS_SPECIFIC)
	    {
	      ERROR4 (error, ER_SM_LESS_SPECIFIC_ALIAS_SUBSTITUTE,
		      winner->name, candidate_source_name (template_, alias),
		      candidate_source_name (template_, winner),
		      template_classname (template_));
	    }
	}
    }

  /* check for conflicts between two classes of the most specific domains */
  if (error == NO_ERROR && conflict != NULL)
    {
      if (auto_resolve)
	{
	  resspace = sm_resolution_space (winner->name_space);
	  auto_resolve_conflict (winner, &template_->resolutions, resspace);
	}
      else
	{
	  ERROR3 (error, ER_SM_ATTRIBUTE_NAME_CONFLICT, winner->name,
		  candidate_source_name (template_, winner),
		  candidate_source_name (template_, conflict));
	}
    }

  if (error == NO_ERROR)
    {
      *winner_return = winner;
    }
  else
    {
      *winner_return = NULL;
    }

  return error;
}

/* COMPONENT FLATTENING */
/*
 * insert_attribute()
 * insert_method() - This inserts an attribute into a list positioned according
 *    to the "order" field.
 *    This is intended to be used for the ordering of the flattened attribute
 *    list.  As such, we don't use the order_link field here we just use
 *    the regular next field.
 *    Unfortunately we need a separate method version of this since the
 *    order field isn't part of the common header.
 *   return: none
 *   attlist(in/out): pointer to attribte list
 *   att(in): attribute to insert
 */

static void
insert_attribute (SM_ATTRIBUTE ** attlist, SM_ATTRIBUTE * att)
{
  SM_ATTRIBUTE *a, *prev;

  prev = NULL;
  for (a = *attlist; a != NULL && a->order < att->order;
       a = (SM_ATTRIBUTE *) a->header.next)
    {
      prev = a;
    }

  att->header.next = (SM_COMPONENT *) a;
  if (prev == NULL)
    {
      *attlist = att;
    }
  else
    {
      prev->header.next = (SM_COMPONENT *) att;
    }
}

static void
insert_method (SM_METHOD ** methlist, SM_METHOD * method)
{
  SM_METHOD *m, *prev;

  prev = NULL;
  for (m = *methlist; m != NULL && m->order < method->order;
       m = (SM_METHOD *) m->header.next)
    {
      prev = m;
    }

  method->header.next = (SM_COMPONENT *) m;
  if (prev == NULL)
    {
      *methlist = method;
    }
  else
    {
      prev->header.next = (SM_COMPONENT *) method;
    }
}

/*
 * flatten_components() - This is used to flatten the components of a template.
 *    The components are first converted into a list of candidates.
 *    The candidates list is then checked for the rules of compatibility
 *    and conflicts are resolved.  The winning candidate for each name
 *    is then converted back to a component and added to the template
 *    on the appropriate list.
 *    NOTE: Formerly we assumed that the candidates would be pruned and
 *    resolved in order.  Although the "order" field in each candidate
 *    will be set correctly we can't assume that the resulting list we
 *    produce is also ordered.  This is important mainly because this
 *    template will be stored on the class and used in the flattening
 *    of any subclasses.  get_candidates assumes that the template
 *    lists of the super classes are ordered.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   flat(out): flattened template
 *   namespace(in): component name_space
 *   auto_res(in): non-zero to enable auto resolution of conflicts
 */

static int
flatten_components (SM_TEMPLATE * def, SM_TEMPLATE * flat,
		    SM_NAME_SPACE name_space, int auto_res)
{
  int error = NO_ERROR;
  SM_CANDIDATE *candlist, *candidates, *winner = NULL;
  SM_COMPONENT *comp;

  /* get all of the possible candidates for this name_space (class or instance) */
  candlist = get_candidates (def, flat, name_space);

  /* prune the like named candidates of the list one at a time, check
     for consistency and resolve any conflicts */

  while (error == NO_ERROR
	 && ((candidates = prune_candidate (&candlist)) != NULL))
    {
      error = resolve_candidates (flat, candidates, auto_res, &winner);

      if (error == NO_ERROR)
	{
	  if (winner != NULL)
	    {
	      /* convert the candidate back to a component */
	      comp = make_component_from_candidate (def->op, winner);
	      if (comp == NULL)
		{
		  error = er_errid ();
		  free_candidates (candidates);
		  break;
		}

	      /* add it to the appropriate list */
	      switch (comp->name_space)
		{
		case ID_ATTRIBUTE:
		case ID_SHARED_ATTRIBUTE:
		  insert_attribute (&flat->attributes, (SM_ATTRIBUTE *) comp);
		  break;
		case ID_CLASS_ATTRIBUTE:
		  insert_attribute (&flat->class_attributes,
				    (SM_ATTRIBUTE *) comp);
		  break;
		case ID_METHOD:
		  insert_method (&flat->methods, (SM_METHOD *) comp);
		  break;
		case ID_CLASS_METHOD:
		  insert_method (&flat->class_methods, (SM_METHOD *) comp);
		  break;
		default:
		  break;
		}
	    }
	}
      free_candidates (candidates);
    }

  /* If an error occurs, the remaining candidates in candlist should be freed
   */

  if (candlist)
    {
      free_candidates (candlist);
    }

  return error;
}

/*
 * flatten_method_files() - Flatten the method file lists from the template
 *    into the flattened template.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   flat(out): flattened template
 */

static int
flatten_method_files (SM_TEMPLATE * def, SM_TEMPLATE * flat)
{
  DB_OBJLIST *super;
  SM_CLASS *class_;
  SM_METHOD_FILE *mfile, *new_mfile;

  /* start by copying the local files to the template */
  if (classobj_copy_methfiles (def->method_files, NULL, &flat->method_files))
    {
      goto memory_error;
    }

  /* collect files from the super classes if we don't already have them */

  for (super = flat->inheritance; super != NULL; super = super->next)
    {
      /* better not be any fetch errors at this point */
      if (au_fetch_class_force (super->op, &class_, AU_FETCH_READ))
	{
	  goto memory_error;	/* may be a deadlock abort !, don't overwrite the error */
	}

      /* if the class is being edited, be sure and get its pending file list */
      if (class_->new_ != NULL)
	{
	  mfile = class_->new_->method_files;
	}
      else
	{
	  mfile = class_->method_files;
	}

      for (; mfile != NULL; mfile = mfile->next)
	{
	  if (!NLIST_FIND (flat->method_files, mfile->name))
	    {
	      new_mfile = classobj_make_method_file (mfile->name);
	      if (new_mfile == NULL)
		{
		  goto memory_error;
		}
	      new_mfile->class_mop = mfile->class_mop;
	      WS_LIST_APPEND (&flat->method_files, new_mfile);
	    }
	}
    }

  return NO_ERROR;

memory_error:
  return er_errid ();
}

/*
 * flatten_query_spec_lists() - Flatten the query_spec lists.
 *    Note that query_spec lists aren't flattened, we just use the one
 *    currently in the template.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   flat(out): flattened template
 */

static int
flatten_query_spec_lists (SM_TEMPLATE * def, SM_TEMPLATE * flat)
{
  /* start by copying the local definitions to the template */
  if (def->query_spec == NULL)
    {
      flat->query_spec = NULL;
    }
  else
    {
      flat->query_spec = classobj_copy_query_spec_list (def->query_spec);
      if (flat->query_spec == NULL)
	{
	  return er_errid ();
	}
    }

  /* no need to flatten the query_spec lists */
  return NO_ERROR;
}

/*
 * filter_resolutions() - Work function for check_shadowed_resolutions.
 *    This will search the resolution list for entries that use a particular
 *    name and remove them.  This is used to remove resolution entries
 *    that are invalid because there is a local definition for a class
 *    component (attribute or method) that must use that name.
 *   return: none
 *   template(in/out): class definition template
 *   name(in): component name
 *   resspace(in): component name_space
 */

static void
filter_component_resolutions (SM_TEMPLATE * template_,
			      const char *name, SM_NAME_SPACE resspace)
{
  SM_RESOLUTION **reslist, *res, *prev, *next;
  const char *rname;

  reslist = (resspace == ID_INSTANCE) ?
    &(template_->resolutions) : &(template_->class_resolutions);

  prev = next = NULL;
  for (res = *reslist; res != NULL; res = next)
    {
      next = res->next;
      if (res->name_space != resspace)
	{
	  prev = res;
	}
      else
	{
	  rname = (res->alias == NULL) ? res->name : res->alias;
	  if (SM_COMPARE_NAMES (rname, name) != 0)
	    {
	      prev = res;
	    }
	  else
	    {
	      if (prev == NULL)
		{
		  *reslist = next;
		}
	      else
		{
		  prev->next = next;
		}
	      res->next = NULL;
	      classobj_free_resolution (res);
	    }
	}
    }
}

/*
 * remove_shadowed_resolutions() - This will make sure that there are
 *    no resolutions in the flattened template that conflict with the names
 *    of any locally defined components.
 *    Since the local components will always take precidence over the
 *    inherited components, resolutions for these inherited components would
 *    make no sense.
 *    Note that since the flattened template hasn't been populated with
 *    components yet, we get the local component list from the original
 *    template but we modify the resolution list on the flattened
 *    template.
 *   return: none
 *   original(in):
 *   flat(in/out):
 */

static void
remove_shadowed_resolutions (SM_TEMPLATE * original, SM_TEMPLATE * flat)
{
  SM_COMPONENT *comp;

  for (comp = (SM_COMPONENT *) original->attributes; comp != NULL;
       comp = comp->next)
    {
      filter_component_resolutions (flat, comp->name, ID_INSTANCE);
    }

  for (comp = (SM_COMPONENT *) original->methods; comp != NULL;
       comp = comp->next)
    {
      filter_component_resolutions (flat, comp->name, ID_INSTANCE);
    }

  for (comp = (SM_COMPONENT *) original->class_attributes; comp != NULL;
       comp = comp->next)
    {
      filter_component_resolutions (flat, comp->name, ID_CLASS);
    }

  for (comp = (SM_COMPONENT *) original->class_methods; comp != NULL;
       comp = comp->next)
    {
      filter_component_resolutions (flat, comp->name, ID_CLASS);
    }
}

/*
 * filter_reslist() - This removes any resolutions in the list that
 *    reference the deleted class.
 *   return: none
 *   reslist(in/out): resolution list filter
 *   deleted_class(in): class to remove
 */

static void
filter_reslist (SM_RESOLUTION ** reslist, MOP deleted_class)
{
  SM_RESOLUTION *res, *next, *prev;

  /* filter out any resolutions for the deleted class */
  if (deleted_class != NULL)
    {
      for (res = *reslist, prev = NULL, next = NULL; res != NULL; res = next)
	{
	  next = res->next;
	  if (res->class_mop != deleted_class)
	    {
	      prev = res;
	    }
	  else
	    {
	      if (prev == NULL)
		*reslist = next;
	      else
		prev->next = next;
	      classobj_free_resolution (res);
	    }
	}
    }
}

/*
 * check_resolution_target() - This checks to see if a particular resolution
 *    makes sense for a template.  This means that the class specified in the
 *    resolution must be on the inheritance list of the template and that the
 *    component name in the resolution must be a valid component of the
 *    class.
 *    This may be more easily done if we keep track of the resolutions
 *    that were actually used during flattening and then prune the ones
 *    that weren't used.  Think about doing this when we rewrite the
 *    flattening algorithm.
 *    Determination of which list to look on to match the resolution is
 *    kind of brute force, when the flattening structures are redesigned,
 *    Try to maintain them in such a way that this sort of operation is
 *    easier.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): class template
 *   res(in): resolution to check
 *   valid_ptr(out): set if resolution is valid (returned)
 */

static int
check_resolution_target (SM_TEMPLATE * template_, SM_RESOLUTION * res,
			 int *valid_ptr)
{
  int error = NO_ERROR;
  SM_CLASS *super;
  int valid;

  valid = 0;
  if (ml_find (template_->inheritance, res->class_mop))
    {
      /* the class exists, must check to see if the attribute still
         exists in the class. Note that since we may be in a subclass of
         the edited class, we have to look for templates on the superclass. */
      error = au_fetch_class_force (res->class_mop, &super, AU_FETCH_READ);
      if (error == NO_ERROR)
	{
	  if (super->new_ != NULL)
	    {
	      /* its got a template */
	      if (res->name_space == ID_INSTANCE)
		{
		  if (SM_FIND_NAME_IN_COMPONENT_LIST (super->new_->attributes,
						      res->name) != NULL
		      || SM_FIND_NAME_IN_COMPONENT_LIST (super->new_->methods,
							 res->name) != NULL)
		    {
		      valid = 1;
		    }
		}
	      else
		{
		  if (SM_FIND_NAME_IN_COMPONENT_LIST
		      (super->new_->class_attributes, res->name) != NULL
		      || SM_FIND_NAME_IN_COMPONENT_LIST (super->
							 new_->class_methods,
							 res->name) != NULL)
		    {
		      valid = 1;
		    }
		}
	    }
	  else
	    {
	      /* no template, look directly at the class */
	      if (res->name_space == ID_INSTANCE)
		{
		  if (classobj_find_component (super, res->name, 0) != NULL)
		    {
		      valid = 1;
		    }
		}
	      else
		{
		  if (classobj_find_component (super, res->name, 1))
		    {
		      valid = 1;
		    }
		}
	    }
	}
    }

  *valid_ptr = valid;
  return error;
}

/*
 * check_invalid_resolutions() - This checks a new resolution list for
 *    resolutions that don't make any sense. If an invalid resolution appears
 *    in the current definition of a class, it has atrophied as a side affect
 *    of some operation and will be removed silently.
 *    If an invalid resolution does not appear in the current definition,
 *    it was placed in the template by the user in an invalid state and
 *    an error will be generated.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): class template
 *   resolutions(in): the resolution list to examine
 *   original_list(in): the resolution list in the current class definition
 */

static int
check_invalid_resolutions (SM_TEMPLATE * template_,
			   SM_RESOLUTION ** resolutions,
			   SM_RESOLUTION * original_list)
{
  int error = NO_ERROR;
  SM_RESOLUTION *res, *prev, *next, *original;
  int valid;

  for (res = *resolutions, prev = NULL, next = NULL;
       res != NULL && error == NO_ERROR; res = next)
    {
      next = res->next;
      error = check_resolution_target (template_, res, &valid);
      if (error == NO_ERROR)
	{
	  if (valid)
	    {
	      prev = res;
	    }
	  else
	    {
	      /* looks bogus try to find it in the original list */
	      original =
		classobj_find_resolution (original_list, res->class_mop,
					  res->name, res->name_space);
	      if (original != NULL)
		{
		  /* see if the aliases are the same */
		  if (res->alias != original->alias)
		    {
		      if (res->alias != NULL && original->alias != NULL)
			{
			  if (SM_COMPARE_NAMES (res->alias, original->alias)
			      != 0)
			    {
			      original = NULL;	/* aliases different */
			    }
			}
		      else
			{
			  original = NULL;	/* aliases different */
			}
		    }
		}
	      if (original != NULL)
		{
		  /* an old resolution that is no longer valid, remove it */
		  if (prev == NULL)
		    {
		      *resolutions = next;
		    }
		  else
		    {
		      prev->next = next;
		    }
		  classobj_free_resolution (res);
		}
	      else
		{
		  /* a new resolution that is not valid, signal an error */
		  ERROR3 (error, ER_SM_INVALID_RESOLUTION,
			  template_classname (template_),
			  res->name, sm_class_name (res->class_mop));
		}
	    }
	}
    }

  return error;
}

/*
 * flatten_resolutions() - Flatten the resolutions for a template.
 *    This doesn't really flatten, it just cleans up the resolution
 *    lists.
 *    If a class was deleted, remove any references to the deleted class.
 *    Remove resolutions for inherited components that are now shadowed
 *    by local components.
 *    Remove resolutions for non-existent super classes.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   flat(in/out): flattened template
 *   deleted_class(in): deleted class object (optional, can be NULL)
 */

static int
filter_resolutions (SM_TEMPLATE * def, SM_TEMPLATE * flat, MOP deleted_class)
{
  int error = NO_ERROR;
  SM_RESOLUTION *original;

  /* no flattening, just get the locally defined resolutions */
  if (classobj_copy_reslist (def->resolutions, ID_INSTANCE,
			     &flat->resolutions))
    {
      return er_errid ();
    }

  if (classobj_copy_reslist (def->class_resolutions, ID_CLASS,
			     &flat->class_resolutions))
    {
      return er_errid ();
    }

  /* filter resolutions that are shadowed by local definitions,
     might consider these error conditions ? */
  remove_shadowed_resolutions (def, flat);

  /* remove all references to the deleted class if any */
  filter_reslist (&flat->resolutions, deleted_class);
  filter_reslist (&flat->class_resolutions, deleted_class);

  /* look for newly added bogus resolutions */
  original = (def->current == NULL) ? NULL : def->current->resolutions;
  error = check_invalid_resolutions (flat, &flat->resolutions, original);
  if (error == NO_ERROR)
    {
      error = check_invalid_resolutions (flat, &flat->class_resolutions,
					 original);
    }

  return error;
}

/*
 * find_matching_att() - This is a work function for retain_former_ids and
 *    others. It performs a very common attribute lookup operation.
 *    An attribute is said to match if the name, source class, and type
 *    are the same.
 *    If idmatch is selected the match is based on the id numbers only.
 *   return: matching attribute
 *   list(in): attribute list to search
 *   att(in): attribute to look for
 *   idmatch(in): flag to cause search based on id rather than name
 */

static SM_ATTRIBUTE *
find_matching_att (SM_ATTRIBUTE * list, SM_ATTRIBUTE * att, int idmatch)
{
  SM_ATTRIBUTE *a, *found;

  found = NULL;
  for (a = list; a != NULL && found == NULL;
       a = (SM_ATTRIBUTE *) a->header.next)
    {
      if (idmatch)
	{
	  if (a->header.name_space == att->header.name_space
	      && a->id == att->id)
	    {
	      found = a;
	    }
	}
      else
	{
	  if (a->header.name_space == att->header.name_space
	      && SM_COMPARE_NAMES (a->header.name, att->header.name) == 0
	      && a->class_mop == att->class_mop && a->type == att->type)
	    {
	      found = a;
	    }
	}
    }

  return found;
}

/*
 * retain_former_ids() - This is a bit of a kludge because we lost the ids of
 *    the inherited attributes when the template was created.
 *    This is a problem for inherited attributes that have been renamed
 *    in the super class. Since they won't match based on name and the
 *    attribute id is -1, build_storage_order will think the inherited
 *    attribute was dropped and replaced with one of a different name.
 *    Immediately after flattening, we call this to fix the attribute
 *    id assignments for things that are the same.
 *    I think this would be a good place to copy the values of shared
 *    and class attributes as well. We will have the same problem of
 *    name matching.
 *    When shadowing an inherited attribute, we used to think that we should
 *    retain the former attribute ID so that we don't lose access to data
 *    previously stored for that attribute. We now think that this is not
 *    the correct behavior. A shadowed attribute is a "new" attribute and
 *    it should shadow the inherited attribute along with its previously
 *    stored values.
 *   return: none
 *   flat(in): template
 */

static void
retain_former_ids (SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *new_att, *found, *super_new, *super_old;
  SM_CLASS *sclass;

  /* Does this class have a previous representation ? */
  if (flat->current != NULL)
    {
      /* Check each new inherited attribute.  These attribute will not have
         an assigned id and their class MOPs will not match */
      for (new_att = flat->attributes; new_att != NULL;
	   new_att = (SM_ATTRIBUTE *) new_att->header.next)
	{
	  /* is this a new attribute ? */
	  if (new_att->id == -1)
	    {
	      /* is it inherited ? */
	      if (new_att->class_mop != NULL
		  && new_att->class_mop != flat->op)
		{
		  /* look for a matching attribute in the existing representation */
		  found = find_matching_att (flat->current->attributes,
					     new_att, 0);
		  if (found != NULL)
		    {
		      /* re-use this attribute */
		      new_att->id = found->id;
		    }
		  else
		    {
		      /* couldn't find it, it may have been renamed in the super
		         class though */
		      if (au_fetch_class_force (new_att->class_mop, &sclass,
						AU_FETCH_READ) == NO_ERROR)
			{
			  /* search the super class' pending attribute list for
			     this name */
			  if (sclass->new_ != NULL)
			    {
			      super_new =
				find_matching_att (sclass->new_->attributes,
						   new_att, 0);
			      if (super_new != NULL)
				{
				  /*
				   * search the supers original attribute list
				   * based on the id of the new one
				   */
				  super_old =
				    find_matching_att (sclass->attributes,
						       super_new, 1);
				  if (super_old != NULL)
				    {
				      if (SM_COMPARE_NAMES
					  (super_old->header.name,
					   new_att->header.name) != 0)
					{
					  /* search our old list with the old name */
					  found =
					    find_matching_att (flat->
							       current->
							       attributes,
							       super_old, 0);
					  if (found != NULL)
					    {
					      /* found the renamed attribute, reuse id */
					      new_att->id = found->id;
					    }
					}
				    }
				}
			    }
			}
		    }
		}

/* As mentioned in the description above, we no longer think that
   it is a good idea to retain the old attribute ID when shadowing
   an inherited attribute.  Since we had thought differently before
   and might think differently again I would rather keep this part
   of the code in here as a reminder. */
#if 0
	      else
		{
		  /* Its a new local attribute.  If we're shadowing a previously
		     inherited attribute, reuse the old id so we don't lose the
		     previous value.  This is new (12/7/94), does it cause
		     unexpected problems ? */
		  /* look for one in the existing representation */
		  found =
		    classobj_find_attribute_list (flat->current->attributes,
						  new->header.name, -1);
		  /* was it inherited ? */
		  if (found != NULL && found->class != new->class)
		    {
		      /* reuse the attribute id, don't have to worry about type
		         compatibility because that must have been checked during
		         flattening. */
		      new->id = found->id;
		    }
		  /* else couldn't find it, do we need to deal with the case where
		     the inherited attribute from the super class has been renamed
		     as is done above ? */
		}
#endif /* 0 */
	    }
	}
    }
}


/*
 * flatten_trigger_cache() - This re-flattens the trigger cache for triggers
 *    directly on this class (not associated with an attribute).
 *    The attribute caches are maintained directly on the attributes.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   flat(out): flattened template
 */

static int
flatten_trigger_cache (SM_TEMPLATE * def, SM_TEMPLATE * flat)
{
  int error = NO_ERROR;
  TR_SCHEMA_CACHE *flat_triggers = NULL, *super_triggers = NULL;
  DB_OBJLIST *super;
  SM_CLASS *class_;

  /* trigger list in def has been filtered to contain only those
     triggers defined directly on the class, combine these with
     those on the current super classes */

  if (def->triggers != NULL)
    {
      flat_triggers = tr_copy_schema_cache (def->triggers, NULL);
    }
  else
    {
      flat_triggers = tr_make_schema_cache (TR_CACHE_CLASS, NULL);
    }

  if (flat_triggers == NULL)
    {
      error = er_errid ();
    }

  for (super = flat->inheritance;
       ((super != NULL) && (error == NO_ERROR)); super = super->next)
    {
      /* better not be any fetch errors at this point */
      if (!(error = au_fetch_class_force (super->op, &class_, AU_FETCH_READ)))
	{
	  /* if the class is being edited, be sure and get its updated trigger cache */
	  if (class_->new_ != NULL)
	    {
	      super_triggers = class_->new_->triggers;
	    }
	  else
	    {
	      super_triggers = class_->triggers;
	    }

	  if (super_triggers != NULL)
	    {
	      error = tr_merge_schema_cache (flat_triggers, super_triggers);
	    }
	}
    }

  if (error)
    {
      if (flat_triggers != NULL)
	{
	  tr_free_schema_cache (flat_triggers);
	}
    }
  else
    {
      if (tr_empty_schema_cache (flat_triggers))
	{
	  tr_free_schema_cache (flat_triggers);
	}
      else
	{
	  flat->triggers = flat_triggers;
	}
    }

  return error;
}

/*
 * flatten_properties() - This combines the interesting properties from the
 *    superclasses into the template property list. This is used mainly for
 *    UNIQUE constraint properties which must be inherited uniformly by
 *    the subclasses.
 *    NOTE: Things will get a lot more complicated here when we start having
 *    to deal with constraints over multiple attributes.
 *    Note that for NEW classes or constraints, the BTID will not have been
 *    allocated at this time, it is allocated in
 *    allocate_disk_structures() call after flattening has finished.
 *    This means that unique constraint info that we inherit may have a NULL
 *    BTID (fields are all -1).  That's ok for now, it will look as if it
 *    was one of our own local unique constraints.  When we get around
 *    to calling allocate_disk_structures() we must always check to see
 *    if the associated attributes were inherited and if so, go back
 *    to the super class to get its real BTID.  It is assumred that the
 *    super class will have the real BTID by this time because the call
 *    to allocate_disk_structures() has been moved to preceed the call
 *    to update_subclasses().
 *    It would be nice if we could allocate the indexes DURING flattening
 *    rather than deferring it until the end.  This would make the whole
 *    think cleaner and less prone to error.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): original class template
 *   flat(out): flattened template being built
 */

static int
flatten_properties (SM_TEMPLATE * def, SM_TEMPLATE * flat)
{
  DB_OBJLIST *super;
  SM_CLASS *class_;
  DB_SET *props;
  SM_CLASS_CONSTRAINT *constraints, *c;
  SM_ATTRIBUTE *atts, *att;
  int error = NO_ERROR;

  constraints = NULL;

  /* start by copying over any locally defined properties */
  if (def->properties != NULL)
    {
      if (classobj_copy_props (def->properties, NULL, &flat->properties) !=
	  NO_ERROR)
	{
	  goto structure_error;	/* should be a memory error */
	}
    }

  /* map over each super class */
  for (super = flat->inheritance; super != NULL; super = super->next)
    {
      /* better not be any fetch errors at this point */
      if (au_fetch_class_force (super->op, &class_, AU_FETCH_READ))
	{
	  goto structure_error;
	}

      /* If the class is being edited, be sure and get its updated property &
       * attribute list. This is going to get really annoying if we have to deal
       * with non-instance attributes.
       */
      if (class_->new_ != NULL)
	{
	  props = class_->new_->properties;
	  atts = class_->new_->attributes;
	}
      else
	{
	  props = class_->properties;
	  atts = class_->attributes;
	}

      /* For right now, the only thing we're interested in is unique
       * constraint information.  As other inheritable things make
       * their way onto the property list, this function will become
       * more complicated.  Since its so much easier to walk over the
       * SM_CLASS_CONSTRAINT list than the property list, built a
       * transient constraint list.
       */
      if (classobj_make_class_constraints (props, atts, &constraints))
	{
	  goto structure_error;
	}

      for (c = constraints; c != NULL; c = c->next)
	{
	  /* ignore non-unique for now */
	  if (SM_IS_CONSTRAINT_UNIQUE_FAMILY (c->type) ||
	      c->type == SM_CONSTRAINT_FOREIGN_KEY)
	    {
	      SM_ATTRIBUTE **attrs;
	      int found_match;
	      int i;

	      attrs = c->attributes;
	      if (attrs[0] != NULL)
		{
		  /* Loop over each attribute in the constraint */
		  found_match = 1;
		  for (i = 0; ((attrs[i] != NULL) && found_match); i++)
		    {
		      /*
		       * Try to find a corresponding attribute in the flattened template
		       */
		      for (att = flat->attributes;
			   att != NULL;
			   att = (SM_ATTRIBUTE *) att->header.next)
			{
			  if (SM_COMPARE_NAMES
			      (attrs[i]->header.name, att->header.name) == 0)
			    {
			      break;
			    }
			}

		      /*
		       * If we found an attribute with a matching name but from a
		       * different source class, it still isn't a match since it was
		       * inherited from somewhere else.
		       */
		      if ((att == NULL)
			  || (att->class_mop != attrs[i]->class_mop))
			{
			  found_match = 0;
			}
		    }

		  if (found_match)
		    {
		      DB_VALUE cnstr_val;
		      int cnstr_exists = 0;

		      /* Does the constraint exist in the subclass ? */
		      DB_MAKE_NULL (&cnstr_val);
		      cnstr_exists =
			classobj_find_prop_constraint (flat->properties,
						       classobj_map_constraint_to_property
						       (c->type), c->name,
						       &cnstr_val);
		      if (cnstr_exists)
			{
			  DB_SEQ *local_property;
			  DB_VALUE btid_val;
			  BTID btid;

			  /* Get the BTID from the local constraint */
			  DB_MAKE_NULL (&btid_val);
			  local_property = DB_GET_SEQ (&cnstr_val);
			  if (set_get_element (local_property, 0, &btid_val))
			    {
			      pr_clear_value (&cnstr_val);
			      goto structure_error;
			    }
			  if (classobj_btid_from_property_value
			      (&btid_val, &btid, NULL))
			    {
			      pr_clear_value (&btid_val);
			      pr_clear_value (&cnstr_val);
			      goto structure_error;
			    }
			  pr_clear_value (&btid_val);

			  /* Raise an error if the B-trees are not equal */
			  if (!BTID_IS_EQUAL (&btid, &c->index))
			    {
			      ERROR1 (error, ER_SM_CONSTRAINT_EXISTS,
				      c->name);
			    }
			}
		      else
			{
			  if (classobj_put_index (&(flat->properties),
						  c->type, c->name, attrs,
						  c->asc_desc, &c->index,
						  c->fk_info,
						  NULL) != NO_ERROR)
			    {
			      pr_clear_value (&cnstr_val);
			      goto structure_error;
			    }
			}

		      pr_clear_value (&cnstr_val);
		    }
		}
	    }
	}

      /* make sure we free the transient constraint list */
      classobj_free_class_constraints (constraints);
    }

  return error;

structure_error:

  classobj_free_class_constraints (constraints);

  /* should have a more appropriate error for this */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SM_INVALID_PROPERTY, 0);

  return er_errid ();
}

/*
 * flatten_template() - Flatten a template, checking for all of the various
 *    schema rules.  Returns a flattened template that forms the basis
 *    for a new class representation if all went well.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   deleted_class(in): MOP of deleted class (optional, can be NULL)
 *   flatp(out): returned pointer to flattened template
 *   auto_res(in): non-zero to enable auto resolution of conflicts
 */

static int
flatten_template (SM_TEMPLATE * def, MOP deleted_class,
		  SM_TEMPLATE ** flatp, int auto_res)
{
  int error = NO_ERROR;
  SM_TEMPLATE *flat;

  /* start with an empty template */
  flat = classobj_make_template (def->name, def->op, NULL);
  if (flat == NULL)
    {
      goto memory_error;
    }

  /* is this necessary ? */
  flat->class_type = def->class_type;
  flat->is_global = def->is_global;

  /* remember this, CAN'T PASS THIS AS AN ARGUMENT to classobj_make_template */
  flat->current = def->current;
  flat->partition_of = def->partition_of;

  /* copy the super class list filtering out the deleted class if any */
  if (deleted_class != NULL)
    ml_remove (&def->inheritance, deleted_class);
  if (def->inheritance != NULL)
    {
      flat->inheritance = ml_copy (def->inheritance);
      if (flat->inheritance == NULL)
	{
	  goto memory_error;
	}
    }

  /* merge the method file lists */
  if (flatten_method_files (def, flat))
    {
      goto memory_error;
    }

  /* merge query_spec lists */
  if (flatten_query_spec_lists (def, flat))
    {
      goto memory_error;
    }

  /* merge trigger caches */
  if (flatten_trigger_cache (def, flat))
    {
      goto memory_error;
    }

  /* copy the loader commands, we should be flattening these as well ? */
  if (def->loader_commands != NULL)
    {
      flat->loader_commands = ws_copy_string (def->loader_commands);
      if (flat->loader_commands == NULL)
	{
	  goto memory_error;
	}
    }

  /* filter out any useless resolutions */
  error = filter_resolutions (def, flat, deleted_class);
  if (error == NO_ERROR)
    {
      /* flatten each component list */
      error = flatten_components (def, flat, ID_INSTANCE, auto_res);
      if (error == NO_ERROR)
	{
	  error = flatten_components (def, flat, ID_CLASS, auto_res);
	}
    }

  /* Flatten the properties (primarily for constraints).
   * Do this after the components have been flattened so we can see use this
   * information for selecting constraint properties.
   */
  if (flatten_properties (def, flat))
    {
      goto memory_error;
    }

  COPY_OID (&flat->real_oid, &def->real_oid);
  HFID_COPY (&flat->real_hfid, &def->real_hfid);
  flat->node_name = ws_copy_string (def->node_name);

  /* if errors, throw away the template and abort */
  if (error != NO_ERROR)
    {
      classobj_free_template (flat);
      flat = NULL;
    }
  else
    {
      /* make sure these get kept */
      retain_former_ids (flat);
    }

  *flatp = flat;
  return error;

memory_error:
  if (flat != NULL)
    {
      classobj_free_template (flat);
    }

  return er_errid ();
}

/* PREPARATION FOR NEW REPRESENTATIONS */
/*
 * assign_attribute_id() - Generate an id for a shared or class attribute.
 *    Instance attribute id's are assigned during order_attributes.
 *    Must use an existing attribute id if one exists.
 *    Note that the attribute id counter for all attributes is the same
 *    so the ids can be assumed unique across all attribute
 *    namespaces.
 *   return: none
 *   class(in/out): class structure
 *   att(in/out): attribute needing id
 *   class_attribute(in): non-zero for class name_space
 */

static void
assign_attribute_id (SM_CLASS * class_, SM_ATTRIBUTE * attribute,
		     int class_attribute)
{
  SM_ATTRIBUTE *attr;

  /* if it already has one, just leave it alone */
  if (attribute->id == -1)
    {
      if (class_attribute)
	{
	  attr = class_->class_attributes;
	}
      else
	{
	  attr = class_->shared;
	}

      for (; attr != NULL && attribute->id == -1;
	   attr = (SM_ATTRIBUTE *) attr->header.next)
	{
	  if ((SM_COMPARE_NAMES (attr->header.name, attribute->header.name) ==
	       0) && (attr->class_mop == attribute->class_mop)
	      && (attr->type == attribute->type))
	    {
	      /* reuse old id */
	      attribute->id = attr->id;
	    }
	}

      if (attribute->id)
	{
	  /* couldn't find an existing one, generate a new one */
	  attribute->id = class_->att_ids++;
	}
    }
}

/*
 * assign_method_id() - Generate an id for an instance or class method.
 *    Use the existing id if already present in the class.
 *   return: none
 *   class(in/out): class structure
 *   method(in/out): method needing an id
 *   class_method(in): non-zero for class name_space
 */

static void
assign_method_id (SM_CLASS * class_, SM_METHOD * method, bool class_method)
{
  SM_METHOD *m;

  if (method->id == -1)
    {
      if (class_method)
	{
	  m = class_->class_methods;
	}
      else
	{
	  m = class_->methods;
	}

      for (; m != NULL && method->id == -1; m = (SM_METHOD *) m->header.next)
	{
	  /* need to check return domains here and reassign id ? */
	  if ((SM_COMPARE_NAMES (m->header.name, method->header.name) == 0)
	      && m->class_mop == method->class_mop)
	    {
	      method->id = m->id;
	    }
	}

      if (method->id == -1)
	{
	  method->id = class_->method_ids++;
	}
    }
}

/*
 * order_atts_by_alignment() - Order the attributes by descending order of
 *    alignment needs.  Within the same alignment group, order the attributes
 *    by ascending order of disk size (this is mainly for the char types).
 *    In this way, if the object is too large to fit on one page, we can try to
 *    keep the smaller char types on the same page as the OID and thereby
 *    we might be able to read the attributes we need without reading
 *    the overflow page.
 *    This algorithm is simplistic but these lists are not long.
 *   return: ordered attributes.
 *   atts(in/out): attributes to be ordered
 */
static SM_ATTRIBUTE *
order_atts_by_alignment (SM_ATTRIBUTE * atts)
{
  SM_ATTRIBUTE *newatts, *found, *attr;

  newatts = NULL;

  while (atts != NULL)
    {
      for (found = atts, attr = atts; attr != NULL;
	   attr = (SM_ATTRIBUTE *) attr->header.next)
	{

	  /* the new attr becomes the found attr if it has larger alignment
	   * requirements or if it has the same alignment needs but has
	   * smaller disk size.
	   */
	  if ((attr->type->alignment > found->type->alignment)
	      || ((attr->type->alignment == found->type->alignment)
		  && (tp_domain_disk_size (attr->domain) <
		      tp_domain_disk_size (found->domain))))
	    {
	      found = attr;
	    }
	}

      /* move the one we found to the new list */
      WS_LIST_REMOVE (&atts, found);
      found->header.next = NULL;

      WS_LIST_APPEND (&newatts, found);
    }

  return newatts;
}

/*
 * build_storage_order() - Here we take a flattened template and reorder
 *    the attributes to be close to the ordering the class had before editing.
 *    In the process we assign attribute ids. If the current and new attribute
 *    lists turn out to be the same, we can avoid the generation of a
 *    new representation since the disk structure of the objects will
 *    be the same.  If the two attribute lists differ, non-zero is
 *    returned indicating that a new representation must be generated.
 *    At the start, the template has a combined list of instance
 *    and shared attributes in the attributes list.  When this completes,
 *    the attributes list will be NULL and the attributes will have
 *    been split into two lists, ordered_attributes and shared_attributes.
 *    Formerly this function tried to retain ids of attributes that
 *    hadn't changed.  This is now done in retain_former_ids above.
 *    When we get here, this is the state of attribute ids in the
 *    flattened template:
 *      id != -1  : this is a local attribute (possibly renamed) that needs
 *                  to keep its former attribute id.
 *      id == -1  : this is an inherited attribute or new local attribute
 *                  if its new, assign a new id, if it's inherited, look
 *                  in the old attribute list for one that matches and reuse
 *                  the old id.  Searching the old list for matching
 *                  components could be done by make_component_from_candidate?
 *   return: non-zero if new representation is needed
 *   class(in): class being modified
 *   flat(out): new flattened template
 */

static int
build_storage_order (SM_CLASS * class_, SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *fixed, *variable, *current, *new_att, *found, *next, *newatts;
  int newrep;

  fixed = variable = NULL;
  newrep = 0;

  newatts = (SM_ATTRIBUTE *)
    classobj_filter_components ((SM_COMPONENT **) (&flat->attributes),
				ID_ATTRIBUTE);

  for (current = class_->attributes; current != NULL;
       current = (SM_ATTRIBUTE *) current->header.next)
    {
      found = NULL;
      for (new_att = newatts; new_att != NULL && found == NULL;
	   new_att = (SM_ATTRIBUTE *) new_att->header.next)
	{

	  /* if the ids are the same, use it without looking at the name,
	     this is how rename works */
	  if (new_att->id != -1)
	    {
	      if (new_att->id == current->id)
		{
		  found = new_att;
		}
	    }

	  /* this shouldn't be necessary now that we assume ids have been
	     assigned where there was one before */

	  else if ((SM_COMPARE_NAMES (current->header.name,
				      new_att->header.name) == 0)
		   && (current->class_mop == new_att->class_mop)
		   && (current->type == new_att->type))
	    {
	      found = new_att;
	    }
	}

      if (found == NULL)
	{
	  newrep = 1;		/* attribute was deleted */
	}
      else
	{
	  /* there was a match, either in name or id */
	  if (found->id == -1)
	    {
	      /* name match, reuse the old id */
	      found->id = current->id;
	    }

	  (void) WS_LIST_REMOVE (&newatts, found);
	  found->header.next = NULL;
	  if (found->type->variable_p)
	    {
	      WS_LIST_APPEND (&variable, found);
	    }
	  else
	    {
	      WS_LIST_APPEND (&fixed, found);
	    }
	}
    }

  /* check for new attributes */
  if (newatts != NULL)
    {
      newrep = 1;
      for (new_att = newatts, next = NULL; new_att != NULL; new_att = next)
	{
	  next = (SM_ATTRIBUTE *) new_att->header.next;
	  new_att->header.next = NULL;
	  new_att->id = class_->att_ids++;

	  if (new_att->type->variable_p)
	    {
	      WS_LIST_APPEND (&variable, new_att);
	    }
	  else
	    {
	      WS_LIST_APPEND (&fixed, new_att);
	    }
	}
    }

  /* order the fixed attributes in descending order by alignment needs */
  if (fixed != NULL)
    {
      fixed = order_atts_by_alignment (fixed);
    }

  /* join the two lists */
  if (fixed == NULL)
    {
      newatts = variable;
    }
  else
    {
      newatts = fixed;
      for (new_att = fixed; new_att != NULL && new_att->header.next != NULL;
	   new_att = (SM_ATTRIBUTE *) new_att->header.next)
	;
      new_att->header.next = (SM_COMPONENT *) variable;
    }

  if (flat->partition_parent_atts != NULL)
    {
      /* if partition subclass is created, the class must have the same
         attributes order and id with its parent class
       */
      SM_ATTRIBUTE *supatt, *reorder = NULL, *a, *prev;

      for (supatt = flat->partition_parent_atts; supatt != NULL;
	   supatt = (SM_ATTRIBUTE *) supatt->header.next)
	{
	  prev = found = NULL;
	  for (a = newatts; a != NULL; a = (SM_ATTRIBUTE *) a->header.next)
	    {
	      if (SM_COMPARE_NAMES (a->header.name, supatt->header.name) == 0)
		{
		  found = a;
		  found->id = supatt->id;

		  if (prev == NULL)
		    {
		      newatts = (SM_ATTRIBUTE *) newatts->header.next;
		    }
		  else
		    {
		      prev->header.next = found->header.next;
		    }
		  found->header.next = NULL;
		  WS_LIST_APPEND (&reorder, found);

		  break;
		}
	      prev = a;
	    }
	}

      WS_LIST_APPEND (&reorder, newatts);
      newatts = reorder;
    }

  /* now change the template to reflect the divided instance and shared
     attribute lists */
  flat->instance_attributes = newatts;
  flat->shared_attributes = flat->attributes;
  flat->attributes = NULL;

  return newrep;
}

/*
 * fixup_component_classes() - Work function for install_new_representation.
 *    Now that we're certain that the template can be applied
 *    and we have a MOP for the class being edited, go through and stamp
 *    the attributes and methods of the class with the classmop.  This
 *    makes it easier later for the browsing functions to get the origin
 *    class of attributes.  This is only a problem when the class is
 *    defined for the first time.
 *   return: none
 *   classop(in): class object
 *   flat(out): flattened template
 */

static void
fixup_component_classes (MOP classop, SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *a;
  SM_METHOD *m;
  SM_METHOD_FILE *f;

  for (a = flat->attributes; a != NULL; a = (SM_ATTRIBUTE *) a->header.next)
    {
      if (a->class_mop == NULL)
	{
	  a->class_mop = classop;
	}
    }

  for (a = flat->class_attributes; a != NULL;
       a = (SM_ATTRIBUTE *) a->header.next)
    {
      if (a->class_mop == NULL)
	{
	  a->class_mop = classop;
	}
    }

  for (m = flat->methods; m != NULL; m = (SM_METHOD *) m->header.next)
    {
      if (m->class_mop == NULL)
	{
	  m->class_mop = classop;
	}
    }

  for (m = flat->class_methods; m != NULL; m = (SM_METHOD *) m->header.next)
    {
      if (m->class_mop == NULL)
	{
	  m->class_mop = classop;
	}
    }

  for (f = flat->method_files; f != NULL; f = f->next)
    {
      if (f->class_mop == NULL)
	{
	  f->class_mop = classop;
	}
    }
}


/*
 * fixup_self_domain()
 * fixup_method_self_domains()
 * fixup_attribute_self_domain()
 * fixup_self_reference_domains() - Domains that were build for new classes
 *    that need to reference the class being build were constructed in a
 *    special way since the MOP of the class was not available at the time the
 *    domain structure was created.  Once semantic checking has been performed
 *    and the class is created, we not must go through and modify the
 *    temporary domain structures to look like real self-referencing
 *    domains.
 *    We now have a number of last minute fixup functions.  Try to bundle
 *    these into a single function sometime to avoid repeated passes
 *    over the class structures.  Not really that performance critical but
 *    nicer if this isn't spread out all over.
 *   return: none
 *   classop(in): class object
 *   flag(in/out): flattened template
 */

static void
fixup_self_domain (TP_DOMAIN * domain, MOP self)
{
  TP_DOMAIN *d;

  for (d = domain; d != NULL; d = d->next)
    {
      /* PR_TYPE is changeable only for transient domain. */
      assert (d->type != tp_Type_null || !d->is_cached);
      if (d->type == tp_Type_null && !d->is_cached)
	{
	  d->type = tp_Type_object;
	  d->class_mop = self;
	}
      fixup_self_domain (d->setdomain, self);
    }
}

static void
fixup_method_self_domains (SM_METHOD * meth, MOP self)
{
  SM_METHOD_SIGNATURE *sig;
  SM_METHOD_ARGUMENT *arg;

  for (sig = meth->signatures; sig != NULL; sig = sig->next)
    {
      for (arg = sig->value; arg != NULL; arg = arg->next)
	{
	  fixup_self_domain (arg->domain, self);
	  arg->domain = tp_domain_cache (arg->domain);
	}
      for (arg = sig->args; arg != NULL; arg = arg->next)
	{
	  fixup_self_domain (arg->domain, self);
	  arg->domain = tp_domain_cache (arg->domain);
	}
    }
}

static void
fixup_attribute_self_domain (SM_ATTRIBUTE * att, MOP self)
{
  /*
     Remember that attributes have a type pointer cache as well as a full
     domain.  BOTH of these need to be updated.  This is unfortunate, I
     think its time to remove the type pointer and rely on the domain
     structure only. */

  fixup_self_domain (att->domain, self);
  att->domain = tp_domain_cache (att->domain);

  /* get the type cache as well */
  if (att->type == tp_Type_null)
    {
      att->type = tp_Type_object;
    }
}

static void
fixup_self_reference_domains (MOP classop, SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *a;
  SM_METHOD *m;

  /* should only bother with this if the class is new, can we somehow
     determine this here ? */

  for (a = flat->attributes; a != NULL; a = (SM_ATTRIBUTE *) a->header.next)
    {
      fixup_attribute_self_domain (a, classop);
    }

  for (a = flat->class_attributes; a != NULL;
       a = (SM_ATTRIBUTE *) a->header.next)
    {
      fixup_attribute_self_domain (a, classop);
    }

  for (a = flat->shared_attributes; a != NULL;
       a = (SM_ATTRIBUTE *) a->header.next)
    {
      fixup_attribute_self_domain (a, classop);
    }

  for (m = flat->methods; m != NULL; m = (SM_METHOD *) m->header.next)
    {
      fixup_method_self_domains (m, classop);
    }

  for (m = flat->class_methods; m != NULL; m = (SM_METHOD *) m->header.next)
    {
      fixup_method_self_domains (m, classop);
    }
}

/* DISK STRUCTURE ALLOCATION */
/*
 * construct_index_key_domain()
 *   return:
 *   n_atts(in):
 *   atts(in):
 *   asc_desc(in):
 */

static TP_DOMAIN *
construct_index_key_domain (int n_atts, SM_ATTRIBUTE ** atts,
			    const int *asc_desc, const int *prefix_lengths)
{
  int i;
  TP_DOMAIN *head = NULL;
  TP_DOMAIN *current = NULL;
  TP_DOMAIN *set_domain = NULL;
  TP_DOMAIN *new_domain = NULL;
  TP_DOMAIN *cached_domain = NULL;

  if (n_atts == 1)
    {
      if ((asc_desc && asc_desc[0] == 1) ||
	  (prefix_lengths && (*prefix_lengths != -1) &&
	   QSTR_IS_ANY_CHAR_OR_BIT (atts[0]->domain->type->id)))
	{
	  new_domain = tp_domain_copy (atts[0]->domain, false);
	  if (new_domain == NULL)
	    {
	      goto mem_error;
	    }

	  new_domain->is_desc = asc_desc && asc_desc[0] == 1;

	  if (prefix_lengths && (*prefix_lengths != -1) &&
	      QSTR_IS_ANY_CHAR_OR_BIT (atts[0]->domain->type->id))
	    {
	      int scale = (atts[0]->domain->type->id == DB_TYPE_BIT) ? 8 : 1;
	      new_domain->precision = MIN (new_domain->precision,
					   *prefix_lengths * scale);
	    }

	  cached_domain = tp_domain_cache (new_domain);
	}
      else
	{
	  cached_domain = atts[0]->domain;
	}
    }
  else if (n_atts > 1)
    {
      for (i = 0; i < n_atts; i++)
	{
	  new_domain = tp_domain_new (DB_TYPE_NULL);
	  if (new_domain == NULL)
	    {
	      goto mem_error;
	    }

	  new_domain->type = atts[i]->domain->type;
	  new_domain->precision = atts[i]->domain->precision;
	  new_domain->scale = atts[i]->domain->scale;
	  new_domain->codeset = atts[i]->domain->codeset;
	  new_domain->is_parameterized = atts[i]->domain->is_parameterized;
	  if (asc_desc && asc_desc[i] == 1)
	    {			/* is descending order */
	      new_domain->is_desc = true;
	    }
	  else
	    {
	      new_domain->is_desc = false;
	    }

	  if (head == NULL)
	    {
	      head = new_domain;
	      current = new_domain;
	    }
	  else
	    {
	      current->next = new_domain;
	      current = new_domain;
	    }
	}

      set_domain = tp_domain_construct (DB_TYPE_MIDXKEY, NULL, 0, 0, head);
      if (set_domain == NULL)
	{
	  goto mem_error;
	}

      cached_domain = tp_domain_cache (set_domain);
    }

  return cached_domain;

mem_error:

  if (head != NULL)
    {
      TP_DOMAIN *td, *next;
      for (td = head, next = NULL; td != NULL; td = next)
	{
	  next = td->next;
	  tp_domain_free (td);
	}
    }
  return NULL;
}

/*
 * collect_hier_class_info() - calling this function in which case *n_classes
 *   			       will equal to 1 upon entry.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): Class MOP of the base class.
 *   subclasses(in): List of subclasses.
 *   constraint_name(in): Name of UNIQUE constraint to search for.
 *   reverse(in):
 *   n_classes(out): Number of subclasses which inherit the constraint.
 *   n_attrs(in): Number of attributes in constraint.
 *   oids(out): Array of class OID's which inherit the constraint.
 *   attr_ids(out): Array of attribute ID's for each class
 *   hfids(out): Array of heaps for classes whicTraverse the subclasses list
 *               looking for matching constraints.
 */

static int
collect_hier_class_info (MOP classop, DB_OBJLIST * subclasses,
			 const char *constraint_name, int reverse,
			 int *n_classes, int n_attrs,
			 OID * oids, int *attr_ids, HFID * hfids)
{
  DB_OBJLIST *sub;
  SM_CLASS *class_;
  int error = NO_ERROR;

  for (sub = subclasses; ((sub != NULL) && (error == NO_ERROR));
       sub = sub->next)
    {
      error = au_fetch_class_force (sub->op, &class_, AU_FETCH_READ);
      if (error == NO_ERROR)
	{
	  SM_TEMPLATE *flat;
	  SM_CLASS_CONSTRAINT *constraints, *found;
	  int *attr_ptr;

	  /* Get flattened template */
	  flat = class_->new_;

	  /* Make transient constraint cache from property list.  At this point
	     in the process, the property list should be current and include
	     inherited constraints */
	  error =
	    classobj_make_class_constraints (flat->properties,
					     flat->attributes, &constraints);
	  if (error == NO_ERROR)
	    {
	      /* Does this class contain the constraint that we're looking for?
	         Note that we're only interested in UNIQUE constraints at this
	         time. */
	      if (reverse)
		{
		  found = classobj_find_class_constraint (constraints,
							  SM_CONSTRAINT_REVERSE_UNIQUE,
							  constraint_name);
		}
	      else
		{
		  found = classobj_find_class_constraint (constraints,
							  SM_CONSTRAINT_UNIQUE,
							  constraint_name);
		  if (!found)
		    {
		      found = classobj_find_class_constraint (constraints,
							      SM_CONSTRAINT_PRIMARY_KEY,
							      constraint_name);
		    }
		}

	      /* If we found a constraint with a matching name, we also need to
	         make sure that the constraint originated in the class that we're
	         interested in.  If so, then save the class OID, attribute ID's
	         and HFID.  We attempt to maintain unique constraint names, but
	         it is possible for different constraint to have the same name.
	         This might happen if a subclass shadows and attribute which
	         invalidates the constraint and then adds a constraint of the same
	         name.  This might also be possible if a class inherits from
	         multiple parent which each have constraints of the same name. */
	      if (found && (found->attributes[0]->class_mop == classop))
		{
		  int i;

		  /* Make sure that we have a permanent OID for the class.  This
		     function only processes the subclasses.  We're assuming that
		     the base class has already been processed. */
		  if (OID_ISTEMP (ws_oid (sub->op)))
		    {
		      locator_assign_permanent_oid (sub->op);
		    }

		  COPY_OID (&oids[*n_classes], WS_OID (sub->op));

		  attr_ptr = &attr_ids[(*n_classes) * n_attrs];
		  for (i = 0; i < n_attrs; i++)
		    {
		      attr_ptr[i] = found->attributes[i]->id;
		    }

		  HFID_COPY (&hfids[*n_classes], &class_->header.heap);
		  (*n_classes)++;
		}

	      classobj_free_class_constraints (constraints);
	    }
	}
    }

  return error;
}

/*
   This done as a post processing pass of sm_update_class to make sure
   that all attributes that were declared to have indexes or unique btids
   tables have the necessary disk structures allocated.

   Logically this should be done before the class is created so if any
   errors occur, we can abort the operation.  Unfortunately, doing this
   accurately requires attribute id's being assigned so it would have
   to go in install_new_representation.  After beta, restructure the
   sequence of operations in sm_update_class and install_new_representation
   (and probably the flattener as well) so we have all the information necessary
   to generate the disk structures before the call to
   install_new_representation and before the class is created.

   allocate_index is also called directly by sm_add_index which for now
   will be the only official way to add an index.
*/

/*
 * allocate_index() - Allocates an index on disk for an attribute of a class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   subclasses(in): List of subclasses
 *   attrs(in): attribute getting the index
 *   asc_desc(in): asc/desc info list
 *   unique(in): True if were allocating a UNIQUE index.  False otherwise.
 *   reverse(in):
 *   constraint_name(in): Name of constraint.
 *   index(out): The BTID of the returned index.
 *   fk_refcls_oid(in):
 *   fk_refcls_pk_btid(in):
 *   cache_attr_id(in):
 *   fk_name(in):
 */

static int
allocate_index (MOP classop, SM_CLASS * class_, DB_OBJLIST * subclasses,
		SM_ATTRIBUTE ** attrs, const int *asc_desc,
		const int *attrs_prefix_length,
		int unique,
		int reverse, const char *constraint_name, BTID * index,
		OID * fk_refcls_oid, BTID * fk_refcls_pk_btid,
		int cache_attr_id, const char *fk_name)
{
  int error = NO_ERROR;
  DB_TYPE type;
  int i, n_attrs;
  int *attr_ids = NULL;
  size_t attr_ids_size;
  OID *oids = NULL;
  HFID *hfids = NULL;
  int node_id = DB_CLUSTER_NODE_LOCAL;

  /* Count the attributes */
  for (i = 0, n_attrs = 0; attrs[i] != NULL; i++, n_attrs++)
    {
      type = attrs[i]->type->id;
      if (!tp_valid_indextype (type))
	{
	  error = ER_SM_INVALID_INDEX_TYPE;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1,
		  pr_type_name (type));
	}
      else if (attrs_prefix_length && attrs_prefix_length[i] >= 0 &&
	       !TP_IS_CHAR_TYPE (type) && !TP_IS_BIT_TYPE (type))
	{
	  error = ER_SM_INVALID_INDEX_WITH_PREFIX_TYPE;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1,
		  pr_type_name (type));
	}
    }

  if (error == NO_ERROR)
    {
      TP_DOMAIN *domain = NULL;

      domain = construct_index_key_domain (n_attrs, attrs, asc_desc,
					   attrs_prefix_length);
      if (domain == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  int max_classes, n_classes, has_instances;
	  DB_OBJLIST *sub;

	  /* need to have macros for this !! */
	  index->vfid.volid = boot_User_volid;

	  /* Count maximum possible subclasses */
	  max_classes = 1;	/* Start with 1 for the current class */
	  for (sub = subclasses; sub != NULL; sub = sub->next)
	    {
	      max_classes++;
	    }

	  /* Allocate arrays to hold subclass information */
	  attr_ids_size = max_classes * n_attrs * sizeof (int);
	  attr_ids = (int *) malloc (attr_ids_size);
	  if (attr_ids == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, attr_ids_size);
	      goto mem_error;
	    }

	  oids = (OID *) malloc (max_classes * sizeof (OID));
	  if (oids == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      max_classes * sizeof (OID));
	      goto mem_error;
	    }

	  hfids = (HFID *) malloc (max_classes * sizeof (HFID));
	  if (hfids == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      max_classes * sizeof (HFID));
	      goto mem_error;
	    }

	  /* Enter the base class information into the arrays */
	  n_classes = 0;
	  if (class_->class_type == SM_PCLASS_CT)
	    {
	      COPY_OID (&oids[n_classes], &class_->real_oid);
	      HFID_COPY (&hfids[n_classes], &class_->real_hfid);
	    }
	  else
	    {
	      COPY_OID (&oids[n_classes], WS_OID (classop));
	      HFID_COPY (&hfids[n_classes], &class_->header.heap);

	    }
	  for (i = 0; i < n_attrs; i++)
	    {
	      attr_ids[i] = attrs[i]->id;
	    }
	  n_classes++;

	  /* If we're creating a UNIQUE B-tree, we need to collect information
	     from subclasses which inherit the UNIQUE constraint */
	  if (unique)
	    {
	      error =
		collect_hier_class_info (classop, subclasses, constraint_name,
					 reverse, &n_classes, n_attrs, oids,
					 attr_ids, hfids);
	      if (error != NO_ERROR)
		{
		  goto gen_error;
		}
	    }

	  /* If the class is global, get which node class created on */
	  if (class_->flags & SM_CLASSFLAG_GLOBAL)
	    {
	      node_id = (int) db_find_node_ip (class_->node_name);
	    }

	  /* Are there any populated classes for this index ? */
	  has_instances = 0;
	  for (i = 0; i < n_classes; i++)
	    {
	      if (!HFID_IS_NULL (&hfids[i])
		  && heap_has_instance (&hfids[i], &oids[i], node_id))
		{
		  /* in case of error and instances exist */
		  has_instances = 1;
		  break;
		}
	    }

	  /* If there are no instances, then call btree_add_index() to create an
	     empty index, otherwise call btree_load_index () to load all of the
	     instances (including applicable subclasses) into a new B-tree */
	  if (!has_instances)
	    {
	      error = btree_add_index (index, domain, WS_OID (classop),
				       attrs[0]->id, unique, reverse,
				       node_id);
	    }
	  /* If there are instances, load all of them (including applicable
	     subclasses) into the new B-tree */
	  else
	    {
	      int last_key_desc = 0;

	      if (reverse || (asc_desc && asc_desc[n_attrs - 1] == 1))
		{
		  last_key_desc = true;
		}
	      else
		{
		  last_key_desc = false;
		}

	      error =
		btree_load_index (index, domain, oids, n_classes, n_attrs,
				  attr_ids, (int *) attrs_prefix_length,
				  hfids, unique, reverse,
				  last_key_desc, fk_refcls_oid,
				  fk_refcls_pk_btid, cache_attr_id, fk_name,
				  node_id);
	    }

	  free_and_init (attr_ids);
	  free_and_init (oids);
	  free_and_init (hfids);
	}
    }

  return error;

mem_error:
  error = er_errid ();

gen_error:
  if (attr_ids != NULL)
    {
      free_and_init (attr_ids);
    }
  if (oids != NULL)
    {
      free_and_init (oids);
    }
  if (hfids != NULL)
    {
      free_and_init (hfids);
    }

  return error;
}

/*
 * deallocate_index() - Deallocate an index that was previously created for
 * 			an attribute.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   cons(in):
 *   index(in/out): index disk identifier
 *   node_name(in): the index on whcih node
 */

static int
deallocate_index (SM_CLASS_CONSTRAINT * cons, BTID * index,
		  const char *node_name)
{
  int error = NO_ERROR;
  SM_CLASS_CONSTRAINT *con;
  int ref_count = 0;
  int node_id = DB_CLUSTER_NODE_LOCAL;

  if (index == NULL || BTID_IS_NULL (index))
    {
      /* Maybe this is a global partition parent. */
      return error;
    }

  for (con = cons; con != NULL; con = con->next)
    {
      if (BTID_IS_EQUAL (index, &con->index))
	{
	  ref_count++;
	}
    }

  if (ref_count != 1)
    {
      return error;
    }

  if (node_name != NULL)
    {
      node_id = (int) db_find_node_ip (node_name);
    }

  if (btree_delete_index (index, node_id) != NO_ERROR)
    {
      error = er_errid ();
    }

  return error;
}

/*
 * remove_class_from_index() - Remove the class from the B-tree.
 *    This is used when it's necessary to delete instances from a particular
 *    class out of a B-tree while leaving other class instances intact.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   oid(in): Class OID
 *   index(in/out): B-tree index
 *   heap(in/out): Class heap
 */

static int
rem_class_from_index (OID * oid, BTID * index, HFID * heap)
{
  int error = NO_ERROR;

  /* If there is no heap, then there cannot be instances to remove. */
  if (HFID_IS_NULL (heap))
    {
      return error;
    }

  if (!locator_remove_class_from_index (oid, index, heap))
    {
      error = er_errid ();
    }

  return error;
}

/*
 * build_fk_obj_cache()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   key_attrs(in): attribute getting the index
 *   asc_desc(in): asc/desc info list
 *   pk_cls_oid(in):
 *   pk_btid(in):
 *   cache_attr_id(in):
 *   fk_name(in):
 */

static int
build_fk_obj_cache (MOP classop, SM_CLASS * class_, SM_ATTRIBUTE ** key_attrs,
		    const int *asc_desc, OID * pk_cls_oid, BTID * pk_btid,
		    int cache_attr_id, char *fk_name)
{
  int error = NO_ERROR;
  int i, n_attrs;
  int *attr_ids = NULL;
  TP_DOMAIN *domain = NULL;
  OID *cls_oid;
  HFID *hfid;

  cls_oid = ws_oid (classop);
  hfid = &class_->header.heap;

  if (!HFID_IS_NULL (hfid)
      && heap_has_instance (hfid, cls_oid, DB_CLUSTER_NODE_LOCAL))
    {
      for (i = 0, n_attrs = 0; key_attrs[i] != NULL; i++, n_attrs++);

      domain =
	construct_index_key_domain (n_attrs, key_attrs, asc_desc, NULL);
      if (domain == NULL)
	{
	  return er_errid ();
	}

      attr_ids = (int *) malloc (n_attrs * sizeof (int));
      if (attr_ids == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, n_attrs * sizeof (int));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      for (i = 0; i < n_attrs; i++)
	{
	  attr_ids[i] = key_attrs[i]->id;
	}

      error = locator_build_fk_obj_cache (cls_oid, hfid, domain, n_attrs,
					  attr_ids, pk_cls_oid, pk_btid,
					  cache_attr_id, fk_name);

      free_and_init (attr_ids);
    }

  return error;
}

/*
 * update_foreign_key_ref() - Update PK referenced by FK
 *   return: NO_ERROR on success, non-zero for ERROR
 *   ref_clsop(in): referenced class by FK
 *   fk_info(in): foreign key info
 */
static int
update_foreign_key_ref (MOP ref_clsop, SM_FOREIGN_KEY_INFO * fk_info)
{
  SM_TEMPLATE *template_;
  int save, error;

  AU_DISABLE (save);

  template_ = dbt_edit_class (ref_clsop);
  if (template_ == NULL)
    {
      AU_ENABLE (save);
      return er_errid ();
    }

  error = classobj_put_foreign_key_ref (&(template_->properties), fk_info);
  if (error != NO_ERROR)
    {
      dbt_abort_class (template_);
      AU_ENABLE (save);
      return error;
    }

  ref_clsop = dbt_finish_class (template_);
  if (ref_clsop == NULL)
    {
      dbt_abort_class (template_);
      AU_ENABLE (save);
      return er_errid ();
    }

  AU_ENABLE (save);
  return NO_ERROR;
}

/*
 * allocate_unique_constraint() - Allocate index for unique constraints
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   con(in):constraint info
 *   subclasses(in): sub class list
 *   asc_desc(in): asc/desc info list
 */
static int
allocate_unique_constraint (MOP classop, SM_CLASS * class_,
			    SM_CLASS_CONSTRAINT * con,
			    DB_OBJLIST * subclasses)
{
  int unique, reverse;
  SM_CLASS *super_class;
  SM_CLASS_CONSTRAINT *super_con, *shared_con;
  const int *asc_desc;
  bool is_global, is_global_partition_subclass = false;
  int is_partition;

  is_global = class_->flags & SM_CLASSFLAG_GLOBAL ? true : false;
  if (do_is_partitioned_classobj (&is_partition, classop, NULL, NULL) !=
      NO_ERROR)
    {
      return er_errid ();
    }

  if (is_global)
    {
      is_global_partition_subclass =
	(is_partition == PARTITION_CLASS || class_->node_name != NULL);
    }

  if (con->attributes[0]->class_mop == classop
      || is_global_partition_subclass)
    {
      /* its local,or it is a global partition subclass,
       * allocate our very own index */
      unique = BTREE_CONSTRAINT_UNIQUE;
      if (con->type == SM_CONSTRAINT_PRIMARY_KEY)
	{
	  unique |= BTREE_CONSTRAINT_PRIMARY_KEY;
	}

      if (con->shared_cons_name)
	{
	  shared_con = classobj_find_constraint_by_name (class_->constraints,
							 con->
							 shared_cons_name);
	  con->index = shared_con->index;
	}
      else
	{
	  if (con->type == SM_CONSTRAINT_UNIQUE
	      || con->type == SM_CONSTRAINT_REVERSE_UNIQUE)
	    {
	      asc_desc = con->asc_desc;
	    }
	  else
	    {
	      asc_desc = NULL;
	    }

	  reverse = SM_IS_CONSTRAINT_REVERSE_INDEX_FAMILY (con->type);

	  if (allocate_index (classop, class_, subclasses, con->attributes,
			      asc_desc, con->attrs_prefix_length,
			      unique, reverse, con->name,
			      &con->index, NULL, NULL, -1, NULL))
	    {
	      return er_errid ();
	    }
	}
    }
  else
    {
      /* its inherited, go get the btid from the super class */
      if (au_fetch_class_force (con->attributes[0]->class_mop,
				&super_class, AU_FETCH_READ))
	{
	  return er_errid ();
	}

      super_con = classobj_find_class_constraint (super_class->constraints,
						  con->type, con->name);
      if (super_con != NULL)
	{
	  con->index = super_con->index;
	}
      else
	{
	  /* not supposed to happen, need a better error */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_SM_INVALID_PROPERTY, 0);
	  return ER_SM_INVALID_PROPERTY;
	}
    }

  return NO_ERROR;
}

/*
 * allocate_foreign_key() - Allocate index for foreign key
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   con(in):constraint info
 *   recache_cls_cons(out):
 */
static int
allocate_foreign_key (MOP classop, SM_CLASS * class_,
		      SM_CLASS_CONSTRAINT * con, bool * recache_cls_cons)
{
  SM_CLASS_CONSTRAINT *pk, *shared_con;
  MOP ref_clsop;
  SM_ATTRIBUTE *cache_attr;

  if (OID_ISNULL (&con->fk_info->ref_class_oid))
    {
      con->fk_info->ref_class_oid = *(ws_oid (classop));

      pk = classobj_find_cons_primary_key (class_->constraints);
      if (pk == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_FK_REF_CLASS_HAS_NOT_PK, 1, class_->header.name);
	  return ER_FK_REF_CLASS_HAS_NOT_PK;
	}
      con->fk_info->ref_class_pk_btid = pk->index;
    }

  if (con->fk_info->cache_attr && con->fk_info->cache_attr_id < 0)
    {
      cache_attr = classobj_find_attribute (class_, con->fk_info->cache_attr,
					    0);
      if (cache_attr == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_FK_CANT_ASSIGN_CACHE_ATTR, 1, con->fk_info->cache_attr);
	  return ER_FK_CANT_ASSIGN_CACHE_ATTR;
	}

      con->fk_info->cache_attr_id = cache_attr->id;
      cache_attr->is_fk_cache_attr = true;
    }

  if (con->shared_cons_name)
    {
      shared_con = classobj_find_constraint_by_name (class_->constraints,
						     con->shared_cons_name);
      con->index = shared_con->index;

      if (con->fk_info->cache_attr_id >= 0
	  && build_fk_obj_cache (classop, class_, con->attributes,
				 con->asc_desc,
				 &(con->fk_info->ref_class_oid),
				 &(con->fk_info->ref_class_pk_btid),
				 con->fk_info->cache_attr_id,
				 (char *) con->fk_info->name) != NO_ERROR)
	{
	  return er_errid ();
	}
    }
  else
    {
      if (allocate_index (classop, class_, NULL, con->attributes, NULL,
			  con->attrs_prefix_length, false,
			  false, con->name, &con->index,
			  &(con->fk_info->ref_class_oid),
			  &(con->fk_info->ref_class_pk_btid),
			  con->fk_info->cache_attr_id, con->fk_info->name))
	{
	  return er_errid ();
	}
    }

  con->fk_info->self_oid = *(ws_oid (classop));
  con->fk_info->self_btid = con->index;

  ref_clsop = ws_mop (&(con->fk_info->ref_class_oid), NULL);

  if (classop == ref_clsop)
    {
      if (classobj_put_foreign_key_ref (&(class_->properties),
					con->fk_info) != NO_ERROR)
	{
	  return er_errid ();
	}
      *recache_cls_cons = true;
    }
  else if (!classobj_is_exist_foreign_key_ref (ref_clsop, con->fk_info))
    {
      if (update_foreign_key_ref (ref_clsop, con->fk_info) != NO_ERROR)
	{
	  return er_errid ();
	}
    }

  return NO_ERROR;
}

/*
 * allocate_disk_structure_helper() - Helper for index allocation
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   con(in):constraint info
 *   subclasses(in): sub class list
 *   recache_cls_cons(out):
 */
static int
allocate_disk_structure_helper (MOP classop, SM_CLASS * class_,
				SM_CLASS_CONSTRAINT * con,
				DB_OBJLIST * subclasses,
				bool * recache_cls_cons)
{
  int error = NO_ERROR;
  int reverse;
  bool is_global, is_global_partition_parent = false;
  int is_partition;

  if (!SM_IS_CONSTRAINT_INDEX_FAMILY (con->type))
    {
      return NO_ERROR;
    }

  is_global = class_->flags & SM_CLASSFLAG_GLOBAL ? true : false;
  error = do_is_partitioned_classobj (&is_partition, classop, NULL, NULL);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (is_global)
    {
      is_global_partition_parent =
	(is_partition == PARTITIONED_CLASS || class_->node_name == NULL);
    }

  if (BTID_IS_NULL (&con->index) && !is_global_partition_parent)
    {
      if (SM_IS_CONSTRAINT_UNIQUE_FAMILY (con->type))
	{
	  error =
	    allocate_unique_constraint (classop, class_, con, subclasses);
	}
      else if (con->type == SM_CONSTRAINT_INDEX ||
	       con->type == SM_CONSTRAINT_REVERSE_INDEX)
	{
	  reverse = (con->type == SM_CONSTRAINT_INDEX) ? false : true;
	  error = allocate_index (classop, class_, NULL, con->attributes,
				  con->asc_desc,
				  con->attrs_prefix_length,
				  false, reverse, con->name,
				  &con->index, NULL, NULL, -1, NULL);
	}
      else if (con->type == SM_CONSTRAINT_FOREIGN_KEY)
	{
	  error = allocate_foreign_key (classop, class_, con,
					recache_cls_cons);
	}

      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  /* Whether we allocated a BTID or not, always write the contraint info
   * back out to the property list.  This is where the promotion of
   * attribute name references to ids references happens.
   */
  if (classobj_put_index_id (&(class_->properties), con->type,
			     con->name, con->attributes,
			     con->asc_desc,
			     con->attrs_prefix_length,
			     &(con->index), con->fk_info, NULL) != NO_ERROR)
    {
      return er_errid ();
    }

  return NO_ERROR;
}

/*
 * allocate_disk_structures() - Allocate the necessary disk structures for
 *    a new or modified class. For constraints, be careful to recognize
 *    a place holder for a BTID that hasn't been allocated yet but whose
 *    definition was actually inherited from a super class. When we find these,
 *    go to the super class and use the BTID that will have by now been
 *    allocated in there rather than allocating a new one of our own.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   subclasses(in):
 */
static int
allocate_disk_structures (MOP classop, SM_CLASS * class_,
			  DB_OBJLIST * subclasses)
{
  bool is_global;
  int err = NO_ERROR;
  MOBJ object;

  if (classop == NULL || WS_IS_REMOTE (classop)
      || WS_ISMARK_DELETED (classop))
    {
      return ER_FAILED;
    }

  is_global = sm_is_global_class (classop);

  if (is_global)
    {
      MOP my_node, cur_node, real_node;
      DB_OBJLIST *node_list;
      const char *rnode_name;
      bool is_tmp_oid;

      is_tmp_oid = OID_ISTEMP (WS_OID (classop));
      node_list = ccf_get_node_list ();
      my_node = ccf_get_connected_node ();
      if (node_list == NULL || my_node == NULL)
	{
	  err = ER_FAILED;
	  goto end;
	}
      rnode_name = sm_node_name ((MOBJ) class_);

      real_node =
	(rnode_name ==
	 NULL ? NULL : ccf_find_node (sm_node_name ((MOBJ) class_)));

      /* step 1: process real node */
      if (real_node != NULL)
	{
	  if (ws_mop_compare (real_node, my_node))
	    {
	      if (!sm_set_remote (classop, ccf_get_node_ip (real_node)))
		{
		  err = ER_FAILED;
		  goto end;
		}
	      if (!is_tmp_oid
		  && !ws_set_rmt_oid (classop, sm_real_oid ((MOBJ) class_),
				      DB_CLUSTER_NODE_NULL))
		{
		  err = ER_FAILED;
		  goto end;
		}
	      sm_trans_to_global (class_);
	    }
	  err =
	    allocate_disk_structures_by_node (classop, class_, subclasses);

	  /* reset classop and global attributes. */
	  sm_trans_to_proxy (WS_OID (classop), class_);
	}

      /* step 2: process other nodes except connectted node */
      for (; node_list != NULL && err == NO_ERROR;
	   node_list = node_list->next)
	{
	  cur_node = node_list->op;
	  if (ws_mop_compare (my_node, cur_node)
	      && (real_node == NULL || ws_mop_compare (real_node, cur_node)))
	    {
	      if (!sm_set_remote (classop, ccf_get_node_ip (cur_node)))
		{
		  err = ER_FAILED;
		}
	      else
		{
		  err =
		    allocate_disk_structures_by_node (classop, class_,
						      subclasses);
		}
	    }
	}

      /* step 3: process connectted node */
      sm_set_remote (classop, DB_CLUSTER_NODE_LOCAL);
      if (real_node != NULL && ws_mop_compare (my_node, real_node) == 0)
	{
	  /* Connected node is the real node and 
	   * the real node has been processed in step 1. 
	   */
	  sm_trans_to_global (class_);
	}
      else if (err == NO_ERROR)
	{
	  err =
	    allocate_disk_structures_by_node (classop, class_, subclasses);
	}
      else
	{
	  /* nothing. */
	}
    }
  else
    {
      err = allocate_disk_structures_by_node (classop, class_, subclasses);
    }

end:
  if (WS_IS_REMOTE (classop))
    {
      ws_set_remote (classop, DB_CLUSTER_NODE_LOCAL);
    }

  if (WS_ISDIRTY (classop)
      && ws_find (classop, &object) != WS_FIND_MOP_DELETED && object != NULL)
    {
      ws_clean (classop);
    }

  class_->allow_new_rep = 1;
  class_->allow_new_chn = 1;

  return err;
}

/*
 * allocate_disk_structures_by_node() - Allocate the necessary disk structures for
 *    a new or modified class. For constraints, be careful to recognize
 *    a place holder for a BTID that hasn't been allocated yet but whose
 *    definition was actually inherited from a super class. When we find these,
 *    go to the super class and use the BTID that will have by now been
 *    allocated in there rather than allocating a new one of our own.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   subclasses(in):
 */
static int
allocate_disk_structures_by_node (MOP classop, SM_CLASS * class_,
				  DB_OBJLIST * subclasses)
{
  SM_CLASS_CONSTRAINT *con;
  bool recache_cls_cons = false;
  bool is_global;

  if (classop == NULL)
    {
      return ER_FAILED;
    }

  is_global = sm_is_global_class (classop);

  if (classobj_cache_class_constraints (class_))
    {
      goto structure_error;
    }

  if (OID_ISTEMP (ws_oid (classop)))
    {
      locator_assign_permanent_oid (classop);
    }

  for (con = class_->constraints; con != NULL; con = con->next)
    {
      if (con->shared_cons_name == NULL && con->attributes[0] != NULL)
	{
	  if (allocate_disk_structure_helper (classop, class_, con,
					      subclasses,
					      &recache_cls_cons) != NO_ERROR)
	    {
	      goto structure_error;
	    }
	}
    }

  for (con = class_->constraints; con != NULL; con = con->next)
    {
      if (con->shared_cons_name != NULL && con->attributes[0] != NULL)
	{
	  if (allocate_disk_structure_helper (classop, class_, con,
					      subclasses,
					      &recache_cls_cons) != NO_ERROR)
	    {
	      goto structure_error;
	    }
	}
    }

  if (class_->allow_new_rep)
    {
      if (classobj_snapshot_representation (class_))
	{
	  goto structure_error;
	}

      /* To guanrantee all global table on each node have same repid,
       * snapshot_representation will be called only once. */
      if (is_global)
	{
	  class_->allow_new_rep = 0;
	}
    }

  /* recache class constraint for foreign key */
  if (recache_cls_cons && classobj_cache_class_constraints (class_))
    {
      goto structure_error;
    }

  /* when we're done, make sure that each attribute's cache is also updated */
  if (!classobj_cache_constraints (class_))
    {
      goto structure_error;
    }

  /* When do schema sync, we don't need update class on current node */
  if (!sm_Do_schema_sync && locator_update_class (classop) == NULL)
    {
      goto structure_error;
    }

  if (is_global && sm_node_name ((MOBJ) class_) != NULL)
    {
      if (locator_create_heap_if_needed (classop, true) == NULL)
	{
	  goto structure_error;
	}
    }

  if (is_global && !sm_get_class_flag (classop, SM_CLASSFLAG_REUSE_OID))
    {
      if (sm_set_class_flag (classop, SM_CLASSFLAG_REUSE_OID, 1) != NO_ERROR)
	{
	  goto structure_error;
	}
    }

  /* cannot use locator_flush_class() here. */
  if (locator_flush_class_helper (classop) != NO_ERROR)
    {
      goto structure_error;
    }

  if (!is_global && sm_update_statistics (classop))
    {
      goto structure_error;
    }

  return NO_ERROR;

structure_error:
  /* the workspace has already been damaged by this point, the caller will
   * have to recognize the error and abort the transaction.
   */
  return er_errid ();
}

/*
 * drop_foreign_key_ref()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in):
 *   flat_cons(in):
 *   cons(in):
 */

static int
drop_foreign_key_ref (MOP classop,
		      SM_CLASS_CONSTRAINT * flat_cons,
		      SM_CLASS_CONSTRAINT * cons)
{
  int err = NO_ERROR;
  MOP ref_clsop;
  SM_TEMPLATE *refcls_template;
  int save;
  SM_CLASS_CONSTRAINT *con;
  SM_FOREIGN_KEY_INFO *fk;

  AU_DISABLE (save);

  ref_clsop = ws_mop (&cons->fk_info->ref_class_oid, NULL);

  if (classop == ref_clsop)
    {
      for (con = flat_cons; con != NULL; con = con->next)
	{
	  if (con->type == SM_CONSTRAINT_PRIMARY_KEY)
	    {
	      for (fk = con->fk_info; fk != NULL; fk = fk->next)
		{
		  if (BTID_IS_EQUAL (&fk->self_btid, &cons->index))
		    {
		      fk->is_dropped = true;
		      break;
		    }
		}
	      break;
	    }
	}
    }
  else
    {
      if ((refcls_template = dbt_edit_class (ref_clsop)) == NULL)
	{
	  AU_ENABLE (save);
	  return er_errid ();
	}

      if ((err =
	   classobj_drop_foreign_key_ref (&(refcls_template->properties),
					  &cons->index)) != NO_ERROR)
	{
	  goto error;
	}

      if ((ref_clsop = dbt_finish_class (refcls_template)) == NULL)
	{
	  err = er_errid ();
	  goto error;
	}
    }

  AU_ENABLE (save);
  return NO_ERROR;

error:
  dbt_abort_class (refcls_template);
  AU_ENABLE (save);

  return err;
}

/*
 * is_index_owner() - check if class is index owner
 *   return: true if index owner or false
 *   classop(in):
 *   con(in):
 */

static bool
is_index_owner (MOP classop, SM_CLASS_CONSTRAINT * con)
{
  BTID btid;
  MOP origin_classop;

  origin_classop = con->attributes[0]->class_mop;

  if (origin_classop == classop)
    {
      return true;
    }

  if (sm_exist_index (origin_classop, con->name, &btid) == NO_ERROR)
    {
      if (BTID_IS_EQUAL (&btid, &con->index))
	{
	  return false;
	}
    }

  return true;
}

/*
 * inherit_constraint() - inherit constraint from super class
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in):
 *   con(in):
 */

static int
inherit_constraint (MOP classop, SM_CLASS_CONSTRAINT * con)
{
  SM_ATTRIBUTE *att;
  SM_CLASS *super_class;
  SM_CLASS_CONSTRAINT *super_con;
  int error = NO_ERROR;

  att = con->attributes[0];
  if (att != NULL && att->class_mop != classop)
    {
      /* its inherited, go get the btid from the super class */

      error = au_fetch_class_force (att->class_mop, &super_class,
				    AU_FETCH_READ);
      if (error == NO_ERROR)
	{
	  super_con =
	    classobj_find_class_constraint (super_class->constraints,
					    con->type, con->name);
	  if (super_con == NULL)
	    {
	      /* not supposed to happen, need a better error */
	      error = ER_SM_INVALID_PROPERTY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	    }
	  else
	    {
	      /* copy the index */
	      con->index = super_con->index;
	    }
	}
    }

  return error;
}

/*
 * transfer_disk_structures() - Work function for install_new_representation.
 *    Here we look for any attributes that are being dropped from the
 *    class and remove their associated disk structures (if any).
 *    This also moves the index ids from the existing attribute structures
 *    into the new ones.  It must do this because copying the index
 *    field is not part of the usual copying done by the cl_ functions.
 *    This is because indexes are not inherited and we
 *    must be very careful that they stay only with the class on which
 *    they were defined.
 *    This can also be called for sm_delete_class with a template of
 *    NULL in which case we just free all disk structures we find.
 *    We DO NOT allocate new index structures here, see
 *    allocate_disk_structures to see how that is done.
 *    This is where BTID's for unique & indexes get inherited.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   flat(out): new flattened template
 */
/*
 * TODO: Think about moving the functionality of allocate_disk_structures
 *       in here, it should be possible to do that and would simplify things.
 */

static int
transfer_disk_structures (MOP classop, SM_CLASS * class_, SM_TEMPLATE * flat)
{
  int error = NO_ERROR;
  SM_CLASS_CONSTRAINT *flat_constraints, *con, *new_con, *prev, *next;
  bool is_global;
  bool is_global_subclass;

  /* Get the cached constraint info for the flattened template.
   * Sigh, convert the template property list to a transient constraint
   * cache so we have a prayer of dealing with it.
   */
  if (flat == NULL)
    {
      flat_constraints = NULL;
    }
  else
    {
      error = classobj_make_class_constraints (flat->properties,
					       flat->instance_attributes,
					       &flat_constraints);
    }

  /* loop over each old constraint */
  for (con = class_->constraints; ((con != NULL) && (error == NO_ERROR));
       con = con->next)
    {
      if (!SM_IS_CONSTRAINT_INDEX_FAMILY (con->type))
	{
	  continue;
	}

      new_con = classobj_find_class_constraint (flat_constraints, con->type,
						con->name);
      if (new_con == NULL)
	{
	  /* Constraint does not exist in the template */
	  if (con->attributes[0] != NULL)
	    {
	      if (con->type == SM_CONSTRAINT_FOREIGN_KEY)
		{
		  error = drop_foreign_key_ref (classop, flat_constraints,
						con);
		}

	      /* destroy the old index but only if we're the owner of it ! */
	      if (is_index_owner (classop, con))
		{
		  deallocate_index (class_->constraints, &con->index,
				    class_->node_name);
		}
	      /* If we're not the owner of it, then only remove this class
	         from the B-tree (the B-tree will still exist) 
	         This case will occurs when super-class's unique index
	         inherited to the sub-class, so sub-class is not the owner.
	         THIS CASE WILL NOT OCCURS IN CLUSTER.
	       */
	      else
		{
		  rem_class_from_index (WS_OID (classop), &con->index,
					&class_->header.heap);
		}

	      BTID_SET_NULL (&con->index);
	    }
	}
      else if (!BTID_IS_EQUAL (&con->index, &new_con->index))
	{
	  if (BTID_IS_NULL (&(new_con->index)))
	    {
	      /* Template index isn't set, transfer the old one
	       * Can this happen, it should have been transfered by now.
	       */
	      new_con->index = con->index;
	    }
	  else
	    {
	      /* The index in the new template is not the same, I'm not entirely
	       * sure what this means or how we can get here.
	       * Possibly if we drop the unique but add it again with the same
	       * name but over different attributes.
	       */
	      if (con->attributes[0] != NULL && is_index_owner (classop, con))
		{
		  if (con->type == SM_CONSTRAINT_FOREIGN_KEY)
		    {
		      error = drop_foreign_key_ref (classop, flat_constraints,
						    con);
		    }

		  deallocate_index (class_->constraints, &con->index,
				    class_->node_name);
		  BTID_SET_NULL (&con->index);
		}
	    }
	}
    }

  /* Filter out any constraints that don't have associated attributes,
   * this is normally only the case for old constraints whose attributes
   * have been deleted.
   */
  for (con = flat_constraints, prev = NULL, next = NULL;
       con != NULL; con = next)
    {
      next = con->next;
      if (con->attributes[0] != NULL)
	{
	  prev = con;
	}
      else
	{
	  if (prev == NULL)
	    {
	      flat_constraints = con->next;
	    }
	  else
	    {
	      prev->next = con->next;
	    }

	  con->next = NULL;
	  if (!BTID_IS_NULL (&con->index))
	    {
	      if (con->type == SM_CONSTRAINT_FOREIGN_KEY)
		{
		  error = drop_foreign_key_ref (classop, flat_constraints,
						con);
		}

	      deallocate_index (class_->constraints, &con->index,
				class_->node_name);
	      BTID_SET_NULL (&con->index);
	    }
	  classobj_free_class_constraints (con);
	}
    }

  /* Loop over each new constraint, if we find any without indexes,
   * this must be inherited, go get the real index from the super class.
   * If this is local constraint without an allocated index,
   * we could allocate one here rather than maintaining
   * separate logic in allocate_disk_structures!  Think about this.
   *
   * UNIQUE constraints are inheritable but INDEX'es are not.
   */

  is_global = class_->flags & SM_CLASSFLAG_GLOBAL ? true : false;
  is_global_subclass = is_global && class_->partition_of != NULL
    && class_->node_name != NULL;

  for (con = flat_constraints;
       ((con != NULL) && (error == NO_ERROR)); con = con->next)
    {
      if ((SM_IS_CONSTRAINT_UNIQUE_FAMILY (con->type) ||
	   con->type == SM_CONSTRAINT_FOREIGN_KEY) && !is_global_subclass)
	{
	  if (BTID_IS_NULL (&(con->index)))
	    {
	      error = inherit_constraint (classop, con);
	    }
	}
    }

  /* rebuild the unique property list entry based on the modified
   * constraint list
   */
  if (flat != NULL)
    {
      classobj_drop_prop (flat->properties, SM_PROPERTY_UNIQUE);
      classobj_drop_prop (flat->properties, SM_PROPERTY_INDEX);
      classobj_drop_prop (flat->properties, SM_PROPERTY_REVERSE_UNIQUE);
      classobj_drop_prop (flat->properties, SM_PROPERTY_REVERSE_INDEX);
      classobj_drop_prop (flat->properties, SM_PROPERTY_PRIMARY_KEY);
      classobj_drop_prop (flat->properties, SM_PROPERTY_FOREIGN_KEY);

      for (con = flat_constraints;
	   ((con != NULL) && (error == NO_ERROR)); con = con->next)
	{
	  if (SM_IS_CONSTRAINT_UNIQUE_FAMILY (con->type) ||
	      con->type == SM_CONSTRAINT_FOREIGN_KEY)
	    {
	      if (classobj_put_index_id (&(flat->properties), con->type,
					 con->name, con->attributes,
					 con->asc_desc,
					 con->attrs_prefix_length,
					 &(con->index),
					 con->fk_info, con->shared_cons_name)
		  != NO_ERROR)
		{
		  error = ER_SM_INVALID_PROPERTY;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
		}
	    }
	  else if (con->type == SM_CONSTRAINT_INDEX ||
		   con->type == SM_CONSTRAINT_REVERSE_INDEX)
	    {
	      if (classobj_put_index_id (&(flat->properties), con->type,
					 con->name, con->attributes,
					 con->asc_desc,
					 con->attrs_prefix_length,
					 &(con->index), NULL, NULL)
		  != NO_ERROR)
		{
		  error = ER_SM_INVALID_PROPERTY;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
		}
	    }
	}
    }

  /* This was used only for convenience here, be sure to free it.
   * Eventually, we'll just maintain these directly on the template.
   */
  classobj_free_class_constraints (flat_constraints);
  return error;
}

/*
 * save_previous_value() - Transfer the value in the old attribute definition
 *    to the new attribute definition.
 *    Work function for check_inherited_attributes.
 *   return: none
 *   old(in): old attribute definition
 *   new(out): new attribute definition
 */

static void
save_previous_value (SM_ATTRIBUTE * old, SM_ATTRIBUTE * new_)
{
  pr_clear_value (&new_->value);
  pr_clone_value (&old->value, &new_->value);

  pr_clear_value (&new_->original_value);

  /* Transfer the current value to the copied definition.
   * Note that older code copied old->value into new->original_value, I don't
   * think thats, right, I changed it to copy the old->original_value
   */
  pr_clone_value (&old->original_value, &new_->original_value);
}

/*
 * check_inherited_attributes() - We maintain a separate copy of the values
 *    for inherited class attributes and shared attributes for each class.
 *    That is, the value of a class/shared attribute is not inherited, only the
 *    definition.  When we have finished re-flattening a class, we must
 *    remember to use the value of the class/shared attribute that is
 *    currently stored in the class NOT the value that came from the
 *    inherited definitions.
 *   return: none
 *   classmop(in): class object
 *   class(in): class structure
 *   flat(in): flattened template
 */

static void
check_inherited_attributes (MOP classmop, SM_CLASS * class_,
			    SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *old, *att, *new_attr;

  if (flat != NULL)
    {
      for (old = class_->shared; old != NULL;
	   old = (SM_ATTRIBUTE *) old->header.next)
	{
	  new_attr = NULL;
	  for (att = flat->attributes; att != NULL && new_attr == NULL;
	       att = (SM_ATTRIBUTE *) att->header.next)
	    {
	      if (att->header.name_space == ID_SHARED_ATTRIBUTE
		  && SM_COMPARE_NAMES (att->header.name,
				       old->header.name) == 0
		  && att->class_mop != classmop
		  && att->class_mop == old->class_mop)
		{
		  /* inherited attribute */
		  new_attr = att;
		}
	    }
	  if (new_attr != NULL)
	    {
	      save_previous_value (old, new_attr);
	    }
	}

      for (old = class_->class_attributes; old != NULL;
	   old = (SM_ATTRIBUTE *) old->header.next)
	{
	  new_attr = NULL;
	  for (att = flat->class_attributes; att != NULL && new_attr == NULL;
	       att = (SM_ATTRIBUTE *) att->header.next)
	    {
	      if (SM_COMPARE_NAMES (att->header.name, old->header.name) == 0
		  && att->class_mop != classmop
		  && att->class_mop == old->class_mop)
		{
		  /* inherited attribute */
		  new_attr = att;
		}
	    }

	  if (new_attr != NULL)
	    {
	      save_previous_value (old, new_attr);
	    }
	}
    }
}

/*
 * invalidate_unused_triggers() - This will invalidate any triggers that are
 *    associated with  attributes that have been deleted.  This is performed
 *    by the function tr_delete_schema_cache which frees the schema cache
 *    but also marks the triggers contained in the cache as invalid.
 *    Note that since a trigger can be referenced by caches throughout
 *    the hierarchy, we only invalidate the trigger if the attribute
 *    being removed was defined directly on this class and not inherited.
 *    We can't invalidate triggers on inherited attributes because the
 *    attribute may still exist in the super class.  tr_delete_schema_cache
 *    must be passed in the MOP of the current class, it will only
 *    invalidate triggers whose target class is the same as this
 *    class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class_mop(in): class object
 *   class(in): class structure
 *   flat(in): flattened template
 */

static void
invalidate_unused_triggers (MOP class_mop,
			    SM_CLASS * class_, SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *old, *new_;

  /* instance level attributes */
  for (old = class_->ordered_attributes; old != NULL; old = old->order_link)
    {
      new_ = NULL;
      if (flat != NULL)
	{
	  for (new_ = flat->instance_attributes;
	       new_ != NULL && new_->id != old->id;
	       new_ = (SM_ATTRIBUTE *) new_->header.next)
	    ;
	}

      if (new_ == NULL)
	{
	  if (old->triggers != NULL)
	    {
	      tr_delete_schema_cache (old->triggers, class_mop);
	      old->triggers = NULL;
	    }
	}
    }

  /* class attributes */
  for (old = class_->class_attributes; old != NULL;
       old = (SM_ATTRIBUTE *) old->header.next)
    {
      new_ = NULL;
      if (flat != NULL)
	{
	  for (new_ = flat->class_attributes;
	       new_ != NULL && new_->id != old->id;
	       new_ = (SM_ATTRIBUTE *) new_->header.next)
	    ;
	}

      if (new_ == NULL)
	{
	  if (old->triggers != NULL)
	    {
	      tr_delete_schema_cache (old->triggers, class_mop);
	      old->triggers = NULL;
	    }
	}
    }
}

/*
 * install_new_representation() - Final installation of a class template.
 *    It is necessary to guarantee that this is an atomic operation and
 *    the workspace will not change while this executes.
 *    Garbage collection should be disabled while this happens
 *    although we keep MOP cached in structures everywhere so it won't
 *    make a difference.
 *    This is essentially the "commit" operation of a schema modification,
 *    be VERY sure you know what you're doing if you change this
 *    code.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   flat(out): flattened template
 */

static int
install_new_representation (MOP classop, SM_CLASS * class_,
			    SM_TEMPLATE * flat)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *a;
  SM_METHOD *m;
  int needrep, newrep;

  assert (classop != NULL);

  if (classop == NULL)
    {
      return ER_FAILED;
    }

  class_->class_type = flat->class_type;

  /* cluster info */
  COPY_OID (&class_->real_oid, &flat->real_oid);
  HFID_COPY (&class_->real_hfid, &flat->real_hfid);
  class_->node_name = ws_copy_string (flat->node_name);
  if (class_->node_name == NULL && flat->node_name)
    {
      return ER_FAILED;
    }

  /* now that we're ready, make sure attribute/methods are stamped with the
     proper class mop */
  fixup_component_classes (classop, flat);

  /* go through and replace kludged "self referencing" domain with a proper
     domain containing the new class MOP */
  fixup_self_reference_domains (classop, flat);

  /* check for inherited class and shared attributes and make sure we maintain
     the current value */
  check_inherited_attributes (classop, class_, flat);

  /* assign attribute ids and check for structural representation changes */
  needrep = build_storage_order (class_, flat);

  /* assign identifiers for the shared and class attributes */
  for (a = flat->shared_attributes; a != NULL;
       a = (SM_ATTRIBUTE *) a->header.next)
    {
      assign_attribute_id (class_, a, 0);
    }
  for (a = flat->class_attributes; a != NULL;
       a = (SM_ATTRIBUTE *) a->header.next)
    {
      assign_attribute_id (class_, a, 1);
    }

  /* methods don't currently have ids stored persistently but go ahead and
     assign them anyway in the hopes that someday they'll be stored */
  for (m = flat->methods; m != NULL; m = (SM_METHOD *) m->header.next)
    {
      assign_method_id (class_, m, 0);
    }
  for (m = flat->class_methods; m != NULL; m = (SM_METHOD *) m->header.next)
    {
      assign_method_id (class_, m, 1);
    }

  /* if the representation changed but there have been no objects
     created with the previous representation, don't create a new one,
     otherwise, flush all resident instances */
  newrep = 0;
  if (needrep)
    {

      /* NEW: 12/5/92, check for error on each of the locator functions, an error
         can happen if we run out of space during flushing. */

      if (!classop->no_objects)
	{
	  switch (class_->class_type)
	    {
	    case SM_CLASS_CT:
	      if (locator_flush_all_instances (classop, true) != NO_ERROR)
		{
		  return (er_errid ());
		}
	      break;

	    case SM_VCLASS_CT:
	      if (vid_flush_all_instances (classop, true) != NO_ERROR)
		{
		  return (er_errid ());
		}
	      break;
	    case SM_PCLASS_CT:
	      /* TODO: what should do for proxy class? */
	      break;

	    default:
	      break;
	    }

	  /* note that the previous operation will flush the current class representation
	     along with the instances and clear the dirty bit, this is unnecessary if
	     the class was only marked dirty in preparation for the new representation.
	     Because the dirty bit is clear however, we must turn it back on after
	     the new representation is installed so it will be properly flushed,
	     the next time a transaction commits or locator_flush_all_instances is called
	   */
	  if (locator_update_class (classop) == NULL)
	    {
	      return (er_errid ());
	    }

	  /* !!! I've seen some cases where objects are left cached while this
	     flag is on which is illegal.  Not sure how this happens but leave
	     this trap so we can track it down.  Shouldn't be necessary */
	  if (ws_class_has_cached_objects (classop))
	    {
	      ERROR0 (error, ER_SM_CORRUPTED);
	      return error;
	    }

	  newrep = 1;

	  /* Set the no_objects flag so we know that if no object dependencies are
	     introduced on this representation, we don't have to generate another
	     one the next time the class is updated. */
	  /* this used to be outside, think about why */
	  WS_SET_NO_OBJECTS (classop);

	}
      else
	{
	  newrep = 1;
	}
    }

  error = transfer_disk_structures (classop, class_, flat);

  /* Delete the trigger caches associated with attributes that
     are no longer part of the class.  This will also mark the
     triggers as invalid since their associated attribute has gone
     away. */
  invalidate_unused_triggers (classop, class_, flat);

  /* clear any attribute or method descriptor caches that reference this
     class. */
  sm_reset_descriptors (classop);

  /* install the template, the dirty bit must be on at this point */
  if ((error = classobj_install_template (class_, flat, newrep)) != NO_ERROR)
    {
      return error;
    }

  /* make absolutely sure this gets marked dirty after the installation,
     this is usually redundant but the class could get flushed
     during memory panics so we always must make sure it gets flushed again */
  if (locator_update_class (classop) == NULL)
    {
      return er_errid ();
    }

  /* If the representation was incremented, invalidate any existing
     statistics cache.  The next time statistics are requested, we'll
     go to the server and get them based on the new catalog information.
     This probably isn't necessary in all cases but let's be safe and
     waste it unconditionally.
   */
  if (newrep && class_->stats != NULL)
    {
      stats_free_statistics (class_->stats);
      class_->stats = NULL;
    }

  /* formerly had classop->no_objects = 1 here, why ? */

  /* now that we don't always load methods immediately after editing,
     must make sure that the methods_loaded flag is cleared so they
     will be loaded the next time a message is sent */
  class_->methods_loaded = 0;

  return error;
}

/* CLASS DEPENDENCY LOCKING */

/*
 * lock_supers() - Get write locks on any super classes that will need to
 *    have their subclass list updated because of changes in the inheritance
 *    of the class being edited.
 *    As a side effect, this constructs the "oldsupers" and "newsupers" list
 *    by comparing the new super class list with the old definition.
 *    These lists will be used later by update_supers.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   current(in): list of current super classes
 *   oldlist(out): returned list of supers being dropped
 *   newlist(out): returned list of supers being added
 */

static int
lock_supers (SM_TEMPLATE * def, DB_OBJLIST * current,
	     DB_OBJLIST ** oldlist, DB_OBJLIST ** newlist)
{
  int error = NO_ERROR;
  DB_OBJLIST *super;
  SM_CLASS *class_;

  /* first check for removals */
  for (super = current;
       ((super != NULL) && (error == NO_ERROR)); super = super->next)
    {
      if (!ml_find (def->inheritance, super->op))
	{
	  if ((error = au_fetch_class (super->op, &class_, AU_FETCH_WRITE,
				       AU_SELECT)) == NO_ERROR)
	    {
	      error = ml_append (oldlist, super->op, NULL);
	    }
	}
    }

  /* now check for new supers */
  for (super = def->inheritance;
       ((super != NULL) && (error == NO_ERROR)); super = super->next)
    {
      if (!ml_find (current, super->op))
	{
	  if ((error = au_fetch_class (super->op, &class_, AU_FETCH_WRITE,
				       AU_SELECT)) == NO_ERROR)
	    {
	      error = ml_append (newlist, super->op, NULL);
	    }
	}
    }
  return (error);
}

/*
 * update_supers() - This updates the subclass list on all super classes that
 *    were affected by a class edit.  It uses the lists built by the
 *    lock_supers function.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class being edited
 *   oldsupers(in): supers no longer connected
 *   newsupers(out): supers being added
 */

static int
update_supers (MOP classop, DB_OBJLIST * oldsupers, DB_OBJLIST * newsupers)
{
  int error = NO_ERROR;
  DB_OBJLIST *super;
  SM_CLASS *class_;

  /* removals */
  for (super = oldsupers;
       ((super != NULL) && (error == NO_ERROR)); super = super->next)
    {
      if ((error = au_fetch_class_force (super->op, &class_, AU_FETCH_UPDATE))
	  == NO_ERROR)
	{
	  ml_remove (&class_->users, classop);
	}
    }

  /* additions */
  for (super = newsupers;
       ((super != NULL) && (error == NO_ERROR)); super = super->next)
    {
      if ((error = au_fetch_class_force (super->op, &class_, AU_FETCH_UPDATE))
	  == NO_ERROR)
	{
	  error = ml_append (&class_->users, classop, NULL);
	}
    }

  return error;
}

/*
 * lock_supers_drop() - Lock the super classes in preparation for a drop
 *    operation. All supers in the list will have to be locked.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   supers(in): list of super classes
 */

static int
lock_supers_drop (DB_OBJLIST * supers)
{
  int error = NO_ERROR;
  DB_OBJLIST *super;
  SM_CLASS *class_;

  for (super = supers;
       ((super != NULL) && (error == NO_ERROR)); super = super->next)
    {
      error = au_fetch_class (super->op, &class_, AU_FETCH_WRITE, AU_SELECT);
    }

  return error;
}

/*
 * update_supers_drop() - This updates the subclass list on super classes
 *    after a class has been deleted.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classmop(in): class object being dropped
 *   supers(in): super class list to update
 */

static int
update_supers_drop (MOP classop, DB_OBJLIST * supers)
{
  int error = NO_ERROR;
  DB_OBJLIST *super;
  SM_CLASS *class_;

  for (super = supers;
       ((super != NULL) && (error == NO_ERROR)); super = super->next)
    {
      if ((error = au_fetch_class_force (super->op, &class_, AU_FETCH_UPDATE))
	  == NO_ERROR)
	{
	  ml_remove (&class_->users, classop);
	}
    }

  return error;
}

/*
 * lock_subclasses_internal()
 * lock_subclasses() - Recursively get write locks on all subclasses that
 *    inherit directly or indirectly from the class being edited. Returns zero
 *    if all classes were successfully locked.
 *    NOTE: The order of the list produced here is very important.
 *    We must make sure that the classes are updated BEFORE any other
 *    classes that use them.
 *    As a side effect, a flattened list of all effected subclasses
 *    is build for later use by update_users.
 *    We're also checking for cycles in the class hierarchy here.
 *    If any of the encountered subclasses are in the immediate super class
 *    list of the class being edited, we must abort.  This is the reason
 *    we pass in the immediate super class list.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   op(in): MOP of class being edited
 *   newsupers(in): new super class list
 *   newsubs(out): retured list of flattened subclasses
 */
static int
lock_subclasses_internal (SM_TEMPLATE * def, MOP op,
			  DB_OBJLIST * newsupers, DB_OBJLIST ** newsubs)
{
  int error = NO_ERROR;
  DB_OBJLIST *l, *found, *new_, *u;
  SM_CLASS *class_;

  if (ml_find (newsupers, op))
    {
      ERROR2 (error, ER_SM_CYCLE_DETECTED, sm_class_name (op), def->name);
    }
  else
    {
      error = au_fetch_class_force (op, &class_, AU_FETCH_WRITE);
      if (error != NO_ERROR)
	{
	  if (WS_ISMARK_DELETED (op))
	    /* in this case, just ignore the error */
	    error = NO_ERROR;
	}
      else
	{
	  /* dive to the bottom */
	  for (u = class_->users;
	       ((u != NULL) && (error == NO_ERROR)); u = u->next)
	    {
	      error = lock_subclasses_internal (def, u->op, newsupers,
						newsubs);
	    }

	  if (error == NO_ERROR)
	    {
	      /* push the class on the list */
	      for (l = *newsubs, found = NULL; l != NULL && found == NULL;
		   l = l->next)
		{
		  if (l->op == op)
		    {
		      found = l;
		    }
		}

	      if (found == NULL)
		{
		  new_ = (DB_OBJLIST *) db_ws_alloc (sizeof (DB_OBJLIST));
		  if (new_ == NULL)
		    {
		      return er_errid ();
		    }
		  new_->op = op;
		  new_->next = *newsubs;
		  *newsubs = new_;
		}
	    }
	}
    }

  return error;
}

static int
lock_subclasses (SM_TEMPLATE * def, DB_OBJLIST * newsupers,
		 DB_OBJLIST * cursubs, DB_OBJLIST ** newsubs)
{
  int error = NO_ERROR;
  DB_OBJLIST *sub;

  /* TODO: we need consider the global tables on all nodes. 
   * we should have a uniform strategy when lock a global object,
   * and this should be resolved by global lock module.*/
  for (sub = cursubs; ((sub != NULL) && (error == NO_ERROR)); sub = sub->next)
    {
      error = lock_subclasses_internal (def, sub->op, newsupers, newsubs);
    }

  return error;
}

/*
 * check_catalog_space() - Checks to see if the catalog manager is able to
 *    handle another representation for this class.  There is a fixed limit
 *    on the number of representations that can be stored in the catalog
 *    for each class.  If this limit is reached, the schema operation
 *    cannot be performed until the database is compacted.
 *    Note that this needs only be called when a schema operation
 *    will actually result in the generation of a new catalog entry.
 *    Since this won't be a problem very often, its also ok just to check
 *    it up front even if the operation may not result in the generation
 *    of a new representation.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classmop(in): class pointer
 *   class(in): class structure
 */

static int
check_catalog_space (MOP classmop, SM_CLASS * class_)
{
  int error = NO_ERROR;
  int status, can_accept;

  /* if the OID is temporary, then we haven't flushed the class yet
     and it isn't necessary to check since there will be no
     existing entries in the catalog */

  /* TODO: if this is a global table,
   * we need check catalog space on all nodes in cluster. */

  if (!OID_ISTEMP (WS_OID (classmop)))
    {

      /* if the oid is permanent, we still may not have flushed the class
         because the OID could have been assigned during the transformation
         of another object that referenced this class.
         In this case, the catalog manager will return ER_CT_UNKNOWN_CLASSID
         because it will have no entries for this class oid.
       */

      status =
	catalog_is_acceptable_new_representation (WS_OID (classmop),
						  &class_->header.heap,
						  &can_accept);
      if (status != NO_ERROR)
	{
	  error = er_errid ();
	  /* ignore if if the class hasn't been flushed yet */
	  if (error == ER_CT_UNKNOWN_CLASSID)
	    {
	      /* if dirty bit isn't on in this MOP, its probably an internal error */
	      error = NO_ERROR;
	    }
	}
      else if (!can_accept)
	{
	  ERROR1 (error, ER_SM_CATALOG_SPACE, class_->header.name);
	}
    }

  return error;
}

/*
 * flatten_subclasses() - Construct a flattened template for every subclass
 *    affected by a class edit (or deletion).  If flattening fails for any
 *    of the subclasses, the entire class edit must be aborted.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   subclasses(in): list of subclasses needing flattening
 *   deleted_class(in): MOP of delete_class (if any, can be NULL)
 */

static int
flatten_subclasses (DB_OBJLIST * subclasses, MOP deleted_class)
{
  int error = NO_ERROR;
  DB_OBJLIST *sub;
  SM_CLASS *class_;
  SM_TEMPLATE *utemplate, *flat;

  for (sub = subclasses;
       ((sub != NULL) && (error == NO_ERROR)); sub = sub->next)
    {
      if ((error = au_fetch_class_force (sub->op, &class_,
					 AU_FETCH_UPDATE)) == NO_ERROR)
	{
	  /* make sure the catalog manager can handle another modification */
	  if ((error = check_catalog_space (sub->op, class_)) == NO_ERROR)
	    {
	      /* make sure the run-time stuff is cached before editing, this
	         is particularly important for the method file source class kludge */
	      if ((error = sm_clean_class (sub->op, class_)) == NO_ERROR)
		{
		  /* create a template */
		  if ((utemplate =
		       classobj_make_template (class_->header.name, sub->op,
					       class_)) == NULL)
		    {
		      error = er_errid ();
		    }
		  else
		    {
		      /* reflatten it without any local changes (will inherit changes) */
		      error = flatten_template (utemplate, deleted_class,
						&flat, 1);
		      if (error == NO_ERROR)
			{
			  class_->new_ = flat;
			}

		      /* free the definition template */
		      classobj_free_template (utemplate);
		    }
		}
	    }
	}
    }

  return error;
}

/*
 * abort_subclasses() - If subclass flattening failed for some reason, must go
 *    through the list and free the temporary templates for those subclasses
 *    that were sucessfully flattened.
 *   return: none
 *   subclasses(in): subclass list
 */

static void
abort_subclasses (DB_OBJLIST * subclasses)
{
  DB_OBJLIST *sub;
  SM_CLASS *class_;

  /* don't stop the loop if we get fetch errors, we're just trying
   * to clean up the templates that are attached to the classes here.
   */
  for (sub = subclasses; sub != NULL; sub = sub->next)
    {
      if (au_fetch_class_force (sub->op, &class_, AU_FETCH_WRITE) == NO_ERROR)
	{
	  if (class_->new_ != NULL)
	    {
	      classobj_free_template (class_->new_);
	      class_->new_ = NULL;
	    }
	}
    }
}

static bool
sm_constraint_belongs_to_class (const SM_CLASS_CONSTRAINT * const con,
				MOP const mop)
{
  if (con->attributes[0] == NULL)
    {
      assert (false);
      return true;
    }
  if (con->attributes[0]->class_mop == mop)
    {
      return true;
    }
  return false;
}

/*
 * update_subclasses() - At this point, all subclasses have been successfully
 *    flattened and it is ok to install new representations for each.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   subclasses(in): list of subclasses
 */

static int
update_subclasses (DB_OBJLIST * subclasses)
{
  int error = NO_ERROR;
  DB_OBJLIST *sub;
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *con;
  bool found_inherited_index;
  bool is_global;
  bool is_global_subclass;

  for (sub = subclasses; sub != NULL && error == NO_ERROR; sub = sub->next)
    {
      if (au_fetch_class_force (sub->op, &class_, AU_FETCH_UPDATE) ==
	  NO_ERROR)
	{
	  if (class_->new_ == NULL)
	    {
	      ERROR0 (error, ER_SM_CORRUPTED);
	    }
	  else
	    {
	      is_global = sm_is_global_class (sub->op);
	      is_global_subclass = is_global && class_->partition_of != NULL
		&& class_->node_name != NULL;

	      error = install_new_representation (sub->op, class_,
						  class_->new_);
	      if (error == NO_ERROR)
		{
		  /*
		   * currently, install_new_representation, allocate_disk_structures
		   * both increment repr_id.
		   *   NEED MORE CONSIDERATION
		   *   someday later, consider the following:
		   *   modify install_new_representation and
		   *   remove allocated_disk_structures
		   */
		  found_inherited_index = false;
		  for (con = class_->constraints; con != NULL;
		       con = con->next)
		    {
		      if (SM_IS_CONSTRAINT_UNIQUE_FAMILY (con->type) ||
			  con->type == SM_CONSTRAINT_FOREIGN_KEY)
			{
			  if (!sm_constraint_belongs_to_class (con, sub->op))
			    {
			      /* there is inherited B+tree index */
			      found_inherited_index = true;
			      break;
			    }
			}
		    }

		  if (found_inherited_index == true || is_global_subclass)
		    {
		      error = allocate_disk_structures (sub->op, class_,
							NULL);
		      if (error != NO_ERROR)
			{
			  return error;
			}
		    }

		  classobj_free_template (class_->new_);
		  class_->new_ = NULL;
		}
	    }
	}
    }

  return error;
}

/*
 * lockhint_subclasses() - This is called early during the processing of
 *    sm_update_class. It will use the new subclass lattice locking function
 *    to try to get all the locks we need before we proceed.  This will
 *    be better for deadlock avoidance.
 *    This is done as a "hint" only, if we don't lock everything,
 *    we'll hit them again later and suspend.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   temp(in):
 *   class(in): class structure
 */

static int
lockhint_subclasses (SM_TEMPLATE * temp, SM_CLASS * class_)
{
  int error = NO_ERROR;
  const char *names[1];
  LOCK locks[1];
  int subs[1];
  bool is_global = false;

  if (class_ != NULL)
    {
      names[0] = class_->header.name;
      locks[0] = locator_fetch_mode_to_lock (DB_FETCH_WRITE, LC_CLASS);
      subs[0] = 1;
      is_global = (class_->node_name != NULL);
    }
  else if (temp != NULL)
    {
      names[0] = temp->name;
      locks[0] = locator_fetch_mode_to_lock (DB_FETCH_WRITE, LC_CLASS);
      subs[0] = 1;
      is_global = (temp->node_name != NULL);
    }

  if (locator_lockhint_classes
      (1, names, locks, subs, 1, DB_CLUSTER_NODE_LOCAL) == LC_CLASSNAME_ERROR)
    {
      error = er_errid ();
    }

#if defined(CS_MODE)
  if (is_global)
    {
      MOP my_node, cur_node;
      DB_OBJLIST *node_list;

      my_node = ccf_get_connected_node ();
      node_list = ccf_get_node_list ();
      if (my_node == NULL || node_list == NULL)
	{
	  return ER_FAILED;
	}
      cur_node = NULL;
      while (node_list != NULL && error == NO_ERROR)
	{
	  cur_node = node_list->op;
	  if (ws_mop_compare (cur_node, my_node))
	    {
	      if (locator_lockhint_classes
		  (1, names, locks, subs, 1,
		   ccf_get_node_ip (cur_node)) == LC_CLASSNAME_ERROR)
		{
		  error = er_errid ();
		}
	    }
	  node_list = node_list->next;
	}
    }
#endif
  return error;
}

/*
 * update_class() - Apply a schema template for a new or existing class.
 *    If there is an error in the local class or any affected subclass
 *    because of a change in the template, none of the changes in the
 *    template will be applied.
 *    Even if there were  no errors detected during the building of the
 *    template, there still may be some outstanding errors detected
 *    during actual flattening that will cause application of the template
 *    to fail.
 *    Locking affected superclasses and subclasses has also been deferred
 *    till now so if locks cannot be obtained, the template cannot be
 *    applied.
 *    If the returned error status is zero, the template application
 *    was successful and the template was freed and can no longer be used.
 *    If the returned error status indicates a problem locking an affected
 *    object, you either abort the template or wait and try again later.
 *    If there is another error in the template, you can either abort
 *    the template or alter the template and try again.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): schema template
 *   classmop(in): MOP of existing class (NULL if new class)
 *   auto_res(in): non-zero to enable auto-resolution of conflicts
 *   verify_oid(in): whether to verify object id
 */

/* NOTE: There were some problems when the transaction was unilaterally
 * aborted in the middle of a schema change operation.  What happens is
 * that during flattening, each class structure is given a pointer to
 * the flattened template.  If the transaction is aborted, all of the
 * dirty objects in the workspace would be flushed.  classobj_free_class()
 * would always free an associated template if one was present.  When
 * that happened, we would get back from some function, find and error,
 * but not realize that our template had been freed out from under us.
 *
 * The simplest solution to this problem is to prevent classobj_free_class()
 * from freeing templates.  This is ok in the normal case but in the
 * event of a unilateral abort, we may end up with some memory leaks as
 * the templates that had been attached to the classes will be lost.
 *
 * Fixing this to avoid this leak will be complicated and the likelihood
 * of this problem is very remote.
 */

static int
update_class (SM_TEMPLATE * template_, MOP * classmop, int auto_res)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  DB_OBJLIST *cursupers, *oldsupers, *newsupers, *cursubs, *newsubs;
  SM_TEMPLATE *flat;

  sm_bump_schema_version ();
  class_ = NULL;
  cursupers = NULL;
  oldsupers = NULL;
  newsupers = NULL;
  cursubs = NULL;
  newsubs = NULL;

  /*
   *  Set a savepoint in the event that we are adding a unique constraint
   *  to a class with instances and the constraint is violated.  In this
   *  situation, we do not want to abort the entire transaction.
   */
  error = tran_savepoint (UNIQUE_SAVEPOINT_NAME, false);

  if ((error == NO_ERROR) && (template_->op != NULL))
    {
      /* existing class, fetch it */
      error = au_fetch_class (template_->op, &class_, AU_FETCH_UPDATE,
			      AU_ALTER);

      /* make sure the catalog manager can deal with another representation */
      if (error == NO_ERROR)
	{
	  error = check_catalog_space (template_->op, class_);
	}
    }

  if (error != NO_ERROR)
    {
      goto end;
    }

  /* pre-lock subclass lattice to the extent possible */
  error = lockhint_subclasses (template_, class_);
  if (error != NO_ERROR)
    {
      goto end;
    }

  /* get write locks on all super classes */
  if (class_ != NULL)
    {
      cursupers = class_->inheritance;
    }

  error = lock_supers (template_, cursupers, &oldsupers, &newsupers);
  if (error != NO_ERROR)
    {
      goto end;
    }

  /* flatten template, store the pending template in the "new" field
     of the class in case we need it to make domain comparisons */
  if (class_ != NULL)
    {
      class_->new_ = template_;
    }

  error = flatten_template (template_, NULL, &flat, auto_res);
  if (error != NO_ERROR)
    {
      /* If we aborted the operation (error == ER_LK_UNILATERALLY_ABORTED)
         then the class may no longer be in the workspace.  So make sure
         that the class exists before using it.  */
      if (class_ != NULL && error != ER_LK_UNILATERALLY_ABORTED)
	{
	  class_->new_ = NULL;
	}

      goto end;
    }

  /* get write locks on all subclasses */
  if (class_ != NULL)
    {
      cursubs = class_->users;
    }

  error = lock_subclasses (template_, newsupers, cursubs, &newsubs);
  if (error != NO_ERROR)
    {
      classobj_free_template (flat);
      /* don't touch this class if we aborted ! */
      if (class_ != NULL && error != ER_LK_UNILATERALLY_ABORTED)
	{
	  class_->new_ = NULL;
	}

      goto end;
    }

  /* put the flattened definition in the class for use during subclass
     flattening */
  if (class_ != NULL)
    {
      class_->new_ = flat;
    }

  /* flatten all subclasses */
  error = flatten_subclasses (newsubs, NULL);
  if (error != NO_ERROR)
    {
      abort_subclasses (newsubs);
      classobj_free_template (flat);
      /* don't touch this class if we aborted ! */
      if (class_ != NULL && error != ER_LK_UNILATERALLY_ABORTED)
	{
	  class_->new_ = NULL;
	}

      goto end;
    }

  /* now we can assume that every class we need to touch has a write
     lock - proceed with the installation of the changes */

  /* are we creating a new class ? */
  if (class_ == NULL)
    {
      class_ = classobj_make_class (template_->name);
      if (class_ == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  class_->owner = Au_user;	/* remember the owner id */
	  if (template_->is_global)
	    {
	      class_->flags |= SM_CLASSFLAG_GLOBAL;
	    }

	  /* NOTE: Garbage collection can occur in the following function
	     as a result of the allocation of the class MOP.  We must
	     ensure that there are no object handles in the SM_CLASS structure
	     at this point that don't have roots elsewhere.  Currently, this
	     is the case since we are simply caching a newly created empty
	     class structure which will later be populated with
	     install_new_representation.  The template that holds
	     the new class contents IS already a GC root.
	   */
	  template_->op = locator_add_class ((MOBJ) class_,
					     (char *) class_->header.name);
	  if (template_->op == NULL)
	    {
	      /* return locator error code */
	      error = er_errid ();
	      abort_subclasses (newsubs);
	      classobj_free_template (flat);
	      classobj_free_class (class_);
	    }
	}
    }

  if (error != NO_ERROR || class_ == NULL)
    {
      goto end;
    }

  /* the next sequence of operations is extremely critical,
     if any errors are detected, we'll have to abort the current
     transaction or the database will be left in an inconsistent
     state */

  if (error == NO_ERROR)
    {
      flat->partition_parent_atts = template_->partition_parent_atts;
      error = install_new_representation (template_->op, class_, flat);
      if (error == NO_ERROR)
	{
	  /* This used to be done toward the end but since the
	   * unique btid has to be inherited, the disk structures
	   * have to be created before we update the subclasses.
	   */
	  error = allocate_disk_structures (template_->op, class_, newsubs);
	  if (error == NO_ERROR)
	    {
	      error = update_supers (template_->op, oldsupers, newsupers);
	      if (error == NO_ERROR)
		{
		  error = update_subclasses (newsubs);
		  if (error == NO_ERROR)
		    {
		      if (classmop != NULL)
			{
			  *classmop = template_->op;
			}
		      /* we're done */
		      class_->new_ = NULL;

		      classobj_free_template (flat);
		      classobj_free_template (template_);
		    }
		}
	    }
	}
    }

  if (error != NO_ERROR)
    {
      classobj_free_template (flat);
      abort_subclasses (newsubs);
      if (error == ER_BTREE_UNIQUE_FAILED || error == ER_FK_INVALID)
	{
	  (void) tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_NAME);
	}
      else
	{
	  (void) tran_unilaterally_abort ();
	}
    }

end:
  ml_free (oldsupers);
  ml_free (newsupers);
  ml_free (newsubs);

  return error;
}

/*
 * sm_finish_class() - this is called to finish a dbt template,
 *                  don't perform auto resolutions
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): schema template
 *   classmop(in): MOP of existing class (NULL if new class)
 */

int
sm_finish_class (SM_TEMPLATE * template_, MOP * classmop)
{
  return update_class (template_, classmop, 0);
}

/*
 * sm_update_class() - this is what the interpreter calls,
 *                     don't perform auto resolutions
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): schema template
 *   classmop(in): MOP of existing class (NULL if new class)
 */

int
sm_update_class (SM_TEMPLATE * template_, MOP * classmop)
{
  return update_class (template_, classmop, 0);
}

/*
 * sm_update_class_auto() - this is called by the db_ layer,
 *                          perform auto resolution
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): schema template
 *   classmop(in): MOP of existing class (NULL if new class)
 */

int
sm_update_class_auto (SM_TEMPLATE * template_, MOP * classmop)
{
  return update_class (template_, classmop, 1);
}

/*
 * remove_class_triggers() - Work function for sm_delete_class_mop.
 *    Inform the trigger manager that the class is going away so
 *    it can update the triggers defined for this class.
 *    Need a better strategy for handling errors here.
 *   return: none
 *   classop(in):
 *   class(in): class structure
 */

static void
remove_class_triggers (MOP classop, SM_CLASS * class_)
{
  SM_ATTRIBUTE *att;

  for (att = class_->ordered_attributes; att != NULL; att = att->order_link)
    {
      (void) tr_delete_schema_cache (att->triggers, classop);
      att->triggers = NULL;
    }

  for (att = class_->class_attributes; att != NULL;
       att = (SM_ATTRIBUTE *) att->header.next)
    {
      (void) tr_delete_schema_cache (att->triggers, classop);
      att->triggers = NULL;
    }

  (void) tr_delete_schema_cache (class_->triggers, classop);
  class_->triggers = NULL;
}

/*
 * sm_delete_class() - This will delete a class from the schema and
 *    delete all instances of the class from the database.  All classes that
 *    inherit from this class will be updated so that inherited components
 *    are removed.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): class object
 */

int
sm_delete_class_mop (MOP op)
{
  int error = NO_ERROR;
  DB_OBJLIST *oldsupers, *oldsubs;
  SM_CLASS *class_;
  SM_TEMPLATE *template_;
  SM_ATTRIBUTE *att;
  int is_partition = 0, subdel = 0;
  SM_CLASS_CONSTRAINT *pk;
  bool is_global;

  error = do_is_partitioned_classobj (&is_partition, op, NULL, NULL);
  if (error != NO_ERROR)
    {
      return error;
    }

  is_global = sm_is_global_class (op);
  if (is_partition == 1)
    {
      error = tran_savepoint (UNIQUE_PARTITION_SAVEPOINT_DROP, false);
      if (error != NO_ERROR)
	{
	  return error;
	}

      if ((error = do_drop_partition (op, 1)) != NO_ERROR)
	{
	  if (error != ER_LK_UNILATERALLY_ABORTED)
	    {
	      (void)
		tran_abort_upto_savepoint (UNIQUE_PARTITION_SAVEPOINT_DROP);
	    }
	  return error;
	}
      subdel = 1;
    }

  /* if the delete fails, we'll need to rollback to savepoint */
  error = tran_savepoint (UNIQUE_SAVEPOINT_NAME2, false);
  if (error < 0)
    {
      goto fail_end;
    }

  oldsubs = NULL;
  oldsupers = NULL;
  sm_bump_schema_version ();

  if (op != NULL)
    {
      /* op should be a class */
      if (!locator_is_class (op, DB_FETCH_WRITE))
	{
	  ERROR0 (error, ER_OBJ_NOT_A_CLASS);
	}
      /* Authorization + pre-lock subclass lattice to the extent possible */
      else if ((error = au_fetch_class (op, &class_, AU_FETCH_WRITE,
					AU_ALTER)) == NO_ERROR
	       && (error = lockhint_subclasses (NULL, class_)) == NO_ERROR)
	{
	  char *fk_name = NULL;

	  pk = classobj_find_cons_primary_key (class_->constraints);
	  if (pk && pk->fk_info
	      && classobj_is_pk_referred (op, pk->fk_info, false, &fk_name))
	    {
	      ERROR2 (error, ER_FK_CANT_DROP_PK_REFERRED, pk->name, fk_name);
	      goto fail_end;
	    }

	  /* remove auto_increment serial object if exist */
	  if (db_Enable_replications <= 0)
	    {
	      for (att = class_->ordered_attributes;
		   att; att = att->order_link)
		{
		  if (att->auto_increment != NULL)
		    {
		      DB_VALUE name_val;
		      char *class_name;

		      error = db_get (att->auto_increment, "class_name",
				      &name_val);
		      if (error == NO_ERROR)
			{
			  class_name = DB_GET_STRING (&name_val);
			  if (class_name != NULL
			      && (strcmp (class_->header.name, class_name) ==
				  0))
			    {
			      int save;
			      AU_DISABLE (save);
			      error = obj_delete (att->auto_increment);
			      AU_ENABLE (save);
			    }
			  db_value_clear (&name_val);
			}

		      if (error != NO_ERROR)
			{
			  goto fail_end;
			}
		    }
		}
	    }

	  /* we don't really need this but some of the support routines use it */
	  template_ = classobj_make_template (NULL, op, class_);
	  if (template_ == NULL)
	    {
	      error = er_errid ();
	      goto fail_end;
	    }

	  if (class_->inheritance != NULL)
	    {
	      oldsupers = ml_copy (class_->inheritance);
	      if (oldsupers == NULL)
		{
		  error = er_errid ();
		  goto fail_end;
		}
	    }

	  if ((error = lock_supers_drop (oldsupers)) != NO_ERROR)
	    {
	      classobj_free_template (template_);
	    }
	  else
	    {
	      /* get write locks on all subclasses */
	      if ((error = lock_subclasses (template_, NULL, class_->users,
					    &oldsubs)) != NO_ERROR)
		{
		  classobj_free_template (template_);
		}
	      else
		{
		  /* now we can assume that every class we need to touch has a write
		     lock - attempt to flatten subclasses to reflect the deletion */
		  error = flatten_subclasses (oldsubs, op);
		  if (error != NO_ERROR)
		    {
		      abort_subclasses (oldsubs);
		    }
		  else
		    {
		      /* flush all instances of this class */
		      switch (class_->class_type)
			{
			case SM_CLASS_CT:
			  if (locator_flush_all_instances (op, true) !=
			      NO_ERROR)
			    {
			      error = er_errid ();
			    }
			  break;

			case SM_VCLASS_CT:
			  if (vid_flush_all_instances (op, true) != NO_ERROR)
			    {
			      error = er_errid ();
			    }
			  break;

			case SM_PCLASS_CT:
			default:
			  break;
			}

		      if (error != NO_ERROR)
			{
			  /* we had problems flushing, this may be due to an out of
			     space condition, probably the transaction should
			     be aborted as well */
			  abort_subclasses (oldsubs);
			}
		      else
			{
			  /* this section is critical, if any errors happen here,
			     the workspace will be in an inconsistent state and the
			     transaction will have to be aborted */

			  /* now update the supers and users */
			  if ((error =
			       update_supers_drop (op,
						   oldsupers)) == NO_ERROR
			      && (error =
				  update_subclasses (oldsubs)) == NO_ERROR)
			    {

			      /* OLD CODE, here we removed the class from the resident
			       * class list, this causes bad problems for GC since the
			       * class will be GC'd before instances have been decached.
			       * This operation has been moved below with
			       * ws_remove_resident_class().  Not sure if this is position
			       * dependent.  If it doesn't cause any problems remove this
			       * comment.
			       */
			      /* ml_remove(&ws_Resident_classes, op); */

			      /* free any indexes, unique btids, or other associated
			       * disk structures
			       */
			      transfer_disk_structures (op, class_, NULL);

			      /* notify all associated triggers that the class is gone */
			      remove_class_triggers (op, class_);

			      /* This to be maintained as long as the class
			       * is cached in the workspace, dirty or not.  When the
			       * deleted class is flushed, the name is removed.
			       * Assuming this doesn't cause problems, remove this comment
			       */
			      /* ws_drop_classname((MOBJ) class); */

			      /* if this is a global table and its is dirty, 
			       * we need flush the dirty to server first,
			       * otherwise may cause errors.*/
			      if (is_global && WS_ISDIRTY (op))
				{
				  error = locator_flush_class (op);
				}

			      /* inform the locator - this will mark the class MOP as
			       * deleted so all operations that require the current class
			       * object must be done before calling this function */

			      if (error == NO_ERROR
				  && locator_remove_class (op) == NO_ERROR)
				{

				  /* mark all instance MOPs as deleted, should the locator
				   * be doing this ? */

				  ws_mark_instances_deleted (op);

				  /* make sure this is removed from the resident class list,
				   * this will also make the class mop subject to garbage
				   * collection.
				   * This function will expect that all of the instances of
				   * the class have been decached by this point ! */

				  ws_remove_resident_class (op);

				  classobj_free_template (template_);
				}
			      else
				{
				  /* an error occurred - we need to abort */
				  error = er_errid ();
				  if (error !=
				      ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED
				      && error != ER_LK_UNILATERALLY_ABORTED)
				    {
				      /* Not already aborted, so abort to savepoint */
				      tran_abort_upto_savepoint
					(UNIQUE_SAVEPOINT_NAME2);
				    }
				}
			    }
			}
		    }
		}
	    }
	}
    }

  ml_free (oldsupers);
  ml_free (oldsubs);

fail_end:

  if (subdel && error != NO_ERROR && error != ER_LK_UNILATERALLY_ABORTED)
    {
      (void) tran_abort_upto_savepoint (UNIQUE_PARTITION_SAVEPOINT_DROP);
    }

  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_delete_class() - Delete a class by name.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   name(in): class name
 */

int
sm_delete_class (const char *name)
{
  int error = NO_ERROR;
  MOP classop;

  classop = sm_find_class (name);
  if (classop == NULL)
    {
      error = er_errid ();
    }
  else
    {
      error = sm_delete_class_mop (classop);
    }

  return error;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/* INDEX FUNCTIONS */
/*
 * These are in here bacause they share some of the internal
 * allocation/deallocation for indexes.
 * They also play games with the representation id so the
 * catalog gets updated correctly to include the new index.
*/
/*
 * sm_exist_index() - Checks to see if an index exist
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   idxname(in): index name
 */
static int
sm_exist_index (MOP classop, const char *idxname, BTID * btid)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *cons;

  if ((error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT))
      == NO_ERROR)
    {
      if ((cons = classobj_find_class_index (class_, idxname)))
	{
	  if (btid)
	    {
	      BTID_COPY (btid, &cons->index);
	    }

	  return NO_ERROR;
	}
    }

  return ER_FAILED;
}

/*
 * sm_add_index() - Adds an index to an attribute.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   db_constraint_type(in): constraint type
 *   constraint_name(in): Name of constraint.
 *   attname(in): attribute name
 *   asc_desc(in): asc/desc info list
 *   attrs_prefix_length(in): prefix length
 */

int
sm_add_index (MOP classop, DB_CONSTRAINT_TYPE db_constraint_type,
	      const char *constraint_name, const char **attnames,
	      const int *asc_desc, const int *attrs_prefix_length)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  BTID index;
  int i, n_attrs, is_partition = 0, savepoint_index = 0;
  MOP *sub_partitions = NULL;
  SM_ATTRIBUTE **attrs = NULL;
  size_t attrs_size;
  const char *class_name;
  int use_prefix_length;
  SM_CONSTRAINT_TYPE constraint_type;
  int reverse_index;
  bool is_g_partition_parent;

  assert (db_constraint_type == DB_CONSTRAINT_INDEX
	  || db_constraint_type == DB_CONSTRAINT_REVERSE_INDEX);

  error = sm_check_index_exist (classop, NULL, db_constraint_type,
				constraint_name, attnames, asc_desc);
  if (error != NO_ERROR)
    {
      return error;
    }

  constraint_type =
    SM_MAP_DB_INDEX_CONSTRAINT_TO_SM_CONSTRAINT (db_constraint_type);
  reverse_index = SM_IS_CONSTRAINT_REVERSE_INDEX_FAMILY (constraint_type);

  error = do_is_partitioned_classobj (&is_partition, classop, NULL,
				      &sub_partitions);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (is_partition == 1)
    {
      if (attrs_prefix_length)
	{
	  /* Count the number of attributes */
	  n_attrs = 0;
	  for (i = 0; attnames[i] != NULL; i++)
	    {
	      n_attrs++;
	    }

	  use_prefix_length = false;
	  for (i = 0; i < n_attrs; i++)
	    {
	      if (attrs_prefix_length[i] != -1)
		{
		  use_prefix_length = true;
		  break;
		}
	    }

	  if (use_prefix_length)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_SM_INDEX_PREFIX_LENGTH_ON_PARTITIONED_CLASS, 0);
	      error = ER_SM_INDEX_PREFIX_LENGTH_ON_PARTITIONED_CLASS;
	      goto fail_end;
	    }
	}
      error = tran_savepoint (UNIQUE_PARTITION_SAVEPOINT_INDEX, false);
      if (error != NO_ERROR)
	{
	  goto fail_end;
	}

      savepoint_index = 1;
      for (i = 0; error == NO_ERROR && sub_partitions[i]; i++)
	{
	  if (sm_exist_index (sub_partitions[i], constraint_name, NULL) ==
	      NO_ERROR)
	    {
	      class_name = sm_class_name (sub_partitions[i]);
	      if (class_name)
		{
		  error = ER_SM_INDEX_EXISTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2,
			  class_name, constraint_name);
		}
	      else
		{
		  error = er_errid ();
		}
	      break;
	    }

	  error = sm_add_index (sub_partitions[i], db_constraint_type,
				constraint_name, attnames, asc_desc, NULL);
	}

      if (error != NO_ERROR)
	{
	  goto fail_end;
	}
    }

  if (sub_partitions)
    {
      free_and_init (sub_partitions);
    }

  error = au_fetch_class (classop, &class_, AU_FETCH_UPDATE, AU_INDEX);
  if (error == NO_ERROR)
    {

      /* should be had checked before if this index already exist */

      /* make sure the catalog can handle another representation */
      error = check_catalog_space (classop, class_);
      if (error)
	{
	  goto general_error;
	}

      /* Count the number of attributes */
      n_attrs = 0;
      for (i = 0; attnames[i] != NULL; i++)
	{
	  n_attrs++;
	}

      /* Allocate memory for the attribute array */
      attrs_size = sizeof (SM_ATTRIBUTE *) * (n_attrs + 1);
      attrs = (SM_ATTRIBUTE **) malloc (attrs_size);
      if (attrs == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, attrs_size);
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto general_error;
	}

      /* Retrieve all of the attributes */
      for (i = 0; i < n_attrs; i++)
	{
	  attrs[i] = classobj_find_attribute (class_, attnames[i], 0);
	  if (attrs[i] == NULL || attrs[i]->header.name_space != ID_ATTRIBUTE)
	    {
	      ERROR1 (error, ER_SM_ATTRIBUTE_NOT_FOUND, attnames[i]);
	      goto general_error;
	    }
#if defined (ENABLE_UNUSED_FUNCTION)	/* to disable TEXT */
	  if (sm_has_text_domain (attrs[i], 0))
	    {
	      if (strstr (constraint_name, TEXT_CONSTRAINT_PREFIX))
		{
		  /* prevent to create index on TEXT attribute */
		  ERROR1 (error, ER_REGU_NOT_IMPLEMENTED,
			  rel_major_release_string ());
		  goto general_error;
		}
	    }
#endif /* ENABLE_UNUSED_FUNCTION */
	}
      attrs[n_attrs] = NULL;

      /* Make sure both the class and the instances are flushed before
         creating the index.  NOTE THAT THIS WILL REMOVE THE DIRTY
         BIT FROM THE CLASS OBJECT BEFORE THE INDEX HAS ACTUALLY BEEN
         ATTACHED !  WE NEED TO MAKE SURE THE CLASS IS MARKED DIRTY
         AGAIN AFTER THE INDEX LOAD.
       */

      if (locator_flush_class (classop) != NO_ERROR ||
	  locator_flush_all_instances (classop, true) != NO_ERROR)
	{
	  free_and_init (attrs);
	  return er_errid ();
	}

      is_g_partition_parent = (class_->flags & SM_CLASSFLAG_GLOBAL)
	&& (is_partition == 1);

      BTID_SET_NULL (&index);
      if (!is_g_partition_parent)
	{
	  /* allocate the index - this will result in a btree load if there
	     are existing instances */
	  error = allocate_index (classop, class_, NULL, attrs, asc_desc,
				  attrs_prefix_length, false,
				  reverse_index, constraint_name, &index,
				  NULL, NULL, -1, NULL);
	}
      else
	{
	  /* if is global partition class and is parent, don't create index */
	}


      if (error == NO_ERROR)
	{
	  /* must bump the representation in order to get the index into
	     the catalog unfortunate but not worth changing now, this is
	     alot simpler than install_new_representation because there
	     are no structural changes made to the instances
	     - this must be an atomic operation.
	     If this fails, the transaction must be aborted.
	   */
	  if (classobj_snapshot_representation (class_))
	    {
	      goto severe_error;
	    }

	  /* modify the class to point at the new index */
	  if (classobj_put_index_id (&(class_->properties),
				     constraint_type,
				     constraint_name, attrs, asc_desc,
				     attrs_prefix_length,
				     &index, NULL, NULL) != NO_ERROR)
	    {
	      error = er_errid ();
	      goto general_error;
	    }

	  error = classobj_cache_class_constraints (class_);
	  if (error != NO_ERROR)
	    {
	      goto general_error;
	    }

	  if (!classobj_cache_constraints (class_))
	    {
	      error = er_errid ();
	      goto general_error;
	    }

	  /* now that the index is physically attached to the class, we must
	     mark it as dirty and flush it again to make sure the catalog
	     is updated correctly.  This is necessary because the allocation
	     and loading of the instance are done at the same time.  We need
	     to be able to allocate the index and flush the class BEFORE
	     the loading to avoid this extra step. */

	  /* If either of these operations fail, the transaction should
	     be aborted */
	  if (locator_update_class (classop) == NULL)
	    {
	      goto severe_error;
	    }

	  if (locator_flush_class (classop) != NO_ERROR)
	    {
	      goto severe_error;
	    }

	  /* since we almost always want to use the index after
	     it has been created, cause the statistics for this
	     class to be updated so that the optimizer is able
	     to make use of the new index.  Recall that the optimizer
	     looks at the statistics structures, not the schema structures.
	   */
	  if (sm_update_statistics (classop))
	    {
	      goto severe_error;
	    }
	}

      free_and_init (attrs);
    }

fail_end:
  if (savepoint_index && error != NO_ERROR
      && error != ER_LK_UNILATERALLY_ABORTED)
    {
      (void) tran_abort_upto_savepoint (UNIQUE_PARTITION_SAVEPOINT_INDEX);
    }
  if (sub_partitions)
    {
      free_and_init (sub_partitions);
    }

  return error;

general_error:
  if (attrs != NULL)
    {
      free_and_init (attrs);
    }

  return error;

severe_error:
  /* Something happened at a bad time, the database is in an inconsistent
     state.  Must abort the transaction.
     Save the error that caused the problem.
     We should try to disable error overwriting when we
     abort so the caller can find out what happened.
   */
  if (attrs != NULL)
    {
      free_and_init (attrs);
    }

  error = er_errid ();
  (void) tran_unilaterally_abort ();

  return error;
}

/*
 * sm_drop_index() - Removes an index for an attribute.
 *    Take care to remove the class property list entry for this
 *    index if one has been created.  !! This works now because
 *    sm_drop_index is the only way that we can remove indexes.  If
 *    index add/drop can ever be done during template processing, we'll
 *    have to make that code more aware of this.  I suspect that this
 *    will all get cleaned up during the migration to multi-column
 *    indexes.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   constraint_name(in): constraint name
 */

static int
sm_drop_index (MOP classop, const char *constraint_name)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *found;
  SM_CONSTRAINT_TYPE ctype;
  int i, is_partition = 0, savepoint_index = 0;
  bool is_g_partition_parent;
  MOP *sub_partitions = NULL;

  error = do_is_partitioned_classobj (&is_partition, classop, NULL,
				      &sub_partitions);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (is_partition == 1)
    {
      error = tran_savepoint (UNIQUE_PARTITION_SAVEPOINT_INDEX, false);
      if (error != NO_ERROR)
	{
	  goto fail_end;
	}

      savepoint_index = 1;
      for (i = 0; sub_partitions[i]; i++)
	{
	  if (sm_exist_index (sub_partitions[i], constraint_name, NULL) !=
	      NO_ERROR)
	    {
	      continue;
	    }
	  error = sm_drop_index (sub_partitions[i], constraint_name);
	  if (error != NO_ERROR)
	    {
	      goto fail_end;
	    }
	}
    }

  if (sub_partitions)
    {
      free_and_init (sub_partitions);
    }

  error = au_fetch_class (classop, &class_, AU_FETCH_UPDATE, AU_INDEX);
  if (error == NO_ERROR)
    {
      /* Verify that this constraint does exist */
      ctype = SM_CONSTRAINT_INDEX;
      found = classobj_find_class_constraint (class_->constraints, ctype,
					      constraint_name);

      if (found == NULL)
	{
	  ctype = SM_CONSTRAINT_REVERSE_INDEX;
	  found = classobj_find_class_constraint (class_->constraints, ctype,
						  constraint_name);
	}

      if (found == NULL)
	{
	  ERROR1 (error, ER_SM_NO_INDEX, constraint_name);
	}
      else
	{
	  /* make sure the catalog can handle another representation */
	  error = check_catalog_space (classop, class_);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }

	  /* must bump the representation in order to get the catalog updated,
	     unfortunate but not worth changing now, this is alot simpler
	     than install_new_representation because there are no structural
	     changes made to the instances - this must be an atomic operation */
	  if (classobj_snapshot_representation (class_))
	    {
	      goto severe_error;
	    }

	  is_g_partition_parent = (class_->flags & SM_CLASSFLAG_GLOBAL)
	    && (is_partition == 1);

	  /*
	   *  Remove the index from the class.  We do this is an awkward
	   *  way.  First we remove it from the class constraint cache and
	   *  then we back propagate the changes to the class property list.
	   *  We do this backwards because it's easier, go figure.
	   */
	  /* A parent class of global partition contains no index,
	   * So, don't need deallocate index on this kind of class
	   */
	  if (!is_g_partition_parent && deallocate_index
	      (class_->constraints, &found->index, class_->node_name))
	    {
	      goto severe_error;
	    }

	  BTID_SET_NULL (&found->index);
	  classobj_remove_class_constraint_node (&class_->constraints, found);
	  classobj_free_class_constraints (found);

	  error = classobj_populate_class_properties (&class_->properties,
						      class_->constraints,
						      ctype);

	  if (classobj_cache_class_constraints (class_) != NO_ERROR)
	    {
	      goto severe_error;
	    }

	  if (!classobj_cache_constraints (class_))
	    {
	      goto severe_error;
	    }

	  /* Make sure the class is now marked dirty and flushed so that
	     the catalog is updated.  Also update statistics so that
	     the optimizer will know that the index no longer exists.
	   */
	  if (locator_update_class (classop) == NULL)
	    {
	      goto severe_error;
	    }

	  if (locator_flush_class (classop) != NO_ERROR)
	    {
	      goto severe_error;
	    }

	  if (sm_update_statistics (classop))
	    {
	      goto severe_error;
	    }
	}
    }

fail_end:
  if (savepoint_index && error != NO_ERROR
      && error != ER_LK_UNILATERALLY_ABORTED)
    {
      (void) tran_abort_upto_savepoint (UNIQUE_PARTITION_SAVEPOINT_INDEX);
    }
  if (sub_partitions)
    {
      free_and_init (sub_partitions);
    }

  return error;

severe_error:
  /* Something happened at a bad time, the database is in an inconsistent
     state.  Must abort the transaction.
     Save the error that caused the problem.
     We should try to disable error overwriting when we
     abort so the caller can find out what happened.
   */
  error = er_errid ();
  (void) tran_unilaterally_abort ();

  return error;
}

/*
 * sm_get_index() - Checks to see if an attribute has an index and if so,
 *    returns the BTID of the index.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   attname(in): attribute name
 *   index(out): returned pointer to index
 */

int
sm_get_index (MOP classop, const char *attname, BTID * index)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  /* what happens if we formerly indexed the attribute, revoked index
     authorization and now want to remove it ? */

  if ((error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT))
      == NO_ERROR)
    {
      att = classobj_find_attribute (class_, attname, 0);
      if (att == NULL || att->header.name_space != ID_ATTRIBUTE)
	{
	  ERROR1 (error, ER_SM_ATTRIBUTE_NOT_FOUND, attname);
	}
      else
	{
	  SM_CONSTRAINT *con;
	  int found = 0;

	  /*  First look for the index in the attribute constraint cache */
	  for (con = att->constraints; ((con != NULL) && !found);
	       con = con->next)
	    {
	      if (con->type == SM_CONSTRAINT_INDEX ||
		  con->type == SM_CONSTRAINT_REVERSE_INDEX)
		{
		  *index = con->index;
		  found = 1;
		}
	    }
	}
    }

  return error;
}

/*
 * sm_default_constraint_name() - Constructs a constraint name based upon
 *    the class and attribute names and names' asc/desc info.
 *    Returns the constraint name or NULL is an error occurred.  The string
 *    should be deallocated with free_and_init() when no longer needed.
 *    The class name is normally obtained from the Class Object.  This is
 *    not always possible, though (for instance at class creation time,
 *    there is no class object and <classop> will be NULL).  Under this
 *    condition, the class name will be taken from the Class Template
 *    <ctmpl>.
 *    The format of the default name is;
 *        X_<class>_att1_att2_... or
 *        X_<class>_att1_d_att2_...  --> _d implies that att1 order is 'desc'
 *    where X indicates the constraint type;
 *          i=INDEX,            u=UNIQUE,       pk=PRIMARY KEY,
 *          fk=FOREIGN KEY,     n=NOT NULL,     ru=REVERSE UNIQUE,
 *          ri=REVERSE INDEX
 *          <class> is the class name
 *          attn is the attribute name
 *    (ex)  If we are generating a default name for
 *              create index on foo (a, b);
 *          It would look like i_foo_a_b
 *    (ex)  If we are generating a default name for
 *              create index on foo (a desc, b);
 *          It would look like i_foo_a_d_b --> use '_d' for 'desc'
 *    (ex)  If we are generating a default name for
 *              create reverse index on foo (a desc, b);
 *          It would look like ri_foo_a_b --> not use '_d' for reverse type
 *   return: constraint name
 *   class_name(in): class name
 *   type(in): Constraint Type
 *   att_names(in): Attribute Names
 *   asc_desc(in): asc/desc info list
 */

static char *
sm_default_constraint_name (const char *class_name,
			    DB_CONSTRAINT_TYPE type,
			    const char **att_names, const int *asc_desc)
{
  const char **ptr;
  char *name = NULL;
  int name_length = 0;
  bool do_desc;
  int error = NO_ERROR;

  /*
   *  Construct the constraint name
   */
  if ((class_name == NULL) || (att_names == NULL))
    {
      ERROR0 (error, ER_SM_INVALID_DEF_CONSTRAINT_NAME_PARAMS);
    }
  else
    {
      const char *prefix;
      int i;

      /* Constraint Type */
      prefix = (type == DB_CONSTRAINT_INDEX) ? "i_" :
	(type == DB_CONSTRAINT_UNIQUE) ? "u_" :
	(type == DB_CONSTRAINT_PRIMARY_KEY) ? "pk_" :
	(type == DB_CONSTRAINT_FOREIGN_KEY) ? "fk_" :
	(type == DB_CONSTRAINT_NOT_NULL) ? "n_" :
	(type == DB_CONSTRAINT_REVERSE_UNIQUE) ? "ru_" :
	(type == DB_CONSTRAINT_REVERSE_INDEX) ? "ri_" :
	/*          UNKNOWN TYPE            */ "x_";

      /*
       *  Count the number of characters that we'll need for the name
       */
      name_length = sizeof (prefix);
      name_length += strlen (class_name);	/* class name */

      i = 0;
      for (ptr = att_names; *ptr != NULL; ptr++, i++)
	{
	  do_desc = false;	/* init */
	  if (asc_desc)
	    {
	      if (!DB_IS_CONSTRAINT_REVERSE_INDEX_FAMILY (type))
		{
		  /* attr is marked as 'desc' in the non-reverse index */
		  if (asc_desc[i] == 1)
		    {
		      do_desc = true;
		    }
		}
	    }

	  name_length += (1 + strlen (*ptr));	/* seprator and attr name */
	  if (do_desc)
	    {
	      name_length += 2;	/* '_d' for 'desc' */
	    }
	}			/* for (ptr = ...) */

      /*
       *  Allocate space for the name and construct it
       */
      name = (char *) malloc (name_length + 1);	/* Remember terminating NULL */
      if (name != NULL)
	{
	  /* Constraint Type */
	  strcpy (name, prefix);

	  /* Class name */
	  strcat (name, class_name);

	  /* separated list of attribute names */
	  i = 0;
	  for (ptr = att_names; *ptr != NULL; ptr++, i++)
	    {
	      do_desc = false;	/* init */
	      if (asc_desc)
		{
		  if (!DB_IS_CONSTRAINT_REVERSE_INDEX_FAMILY (type))
		    {
		      /* attr is marked as 'desc' in the non-reverse index */
		      if (asc_desc[i] == 1)
			{
			  do_desc = true;
			}
		    }
		}

	      strcat (name, "_");

	      intl_mbs_lower (*ptr, &name[strlen (name)]);

	      /* attr is marked as 'desc' */
	      if (do_desc)
		{
		  strcat (name, "_d");
		}
	    }			/* for (ptr = ...) */

	  /* now, strcat already appended terminating NULL character */
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, name_length + 1);
	}
    }

  return name;
}

/*
 * sm_produce_constraint_name() - Generate a normalized constraint name.
 *    If a constraint name is given <given_name> != NULL, then this name is
 *    downcased and returned. In this case, the constraint type and attribute
 *    names are not needed.
 *    If a given name is not provided <given_name> == NULL, then a
 *    normalized name is generated using the constraint type and attribute
 *    names.
 *    In either case, the returned name is generated its own memory area
 *    and should be deallocated with by calling sm_free_constraint_name()
 *    when it is no longer needed.
 *    This function differs from sm_produce_constraint_name_mop() in that
 *    the class name is supplied as a parameters and therefore, does not
 *    need to be derived.
 *   return: constraint name
 *   class_name(in): Class Name
 *   constraint_type(in): Constraint Type
 *   att_names(in): Attribute Names
 *   asc_desc(in): asc/desc info list
 *   given_name(in): Optional constraint name.
 */

char *
sm_produce_constraint_name (const char *class_name,
			    DB_CONSTRAINT_TYPE constraint_type,
			    const char **att_names,
			    const int *asc_desc, const char *given_name)
{
  char *name = NULL;
  size_t name_size;

  if (given_name == NULL)
    {
      name = sm_default_constraint_name (class_name, constraint_type,
					 att_names, asc_desc);
    }
  else
    {
      name_size = (strlen (given_name) + 1) * sizeof (char);
      name = (char *) malloc (name_size);
      if (name != NULL)
	{
	  intl_mbs_lower (given_name, name);
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, name_size);
	}
    }

  return name;
}


/*
 * sm_produce_constraint_name_mop() - This function serves the same
 *    functionality as sm_produce_constraint_name() except that it accepts
 *    a class MOP instead of a class name.
 *   return: constraint name
 *   classop(in): Class Object
 *   constraint_type(in): Constraint Type
 *   att_names(in): Attribute Names
 *   given_name(in): Optional constraint name.
 */

char *
sm_produce_constraint_name_mop (MOP classop,
				DB_CONSTRAINT_TYPE constraint_type,
				const char **att_names,
				const int *asc_desc, const char *given_name)
{

  return sm_produce_constraint_name (sm_class_name (classop), constraint_type,
				     att_names, asc_desc, given_name);
}

/*
 * sm_produce_constraint_name_tmpl() - This function serves the same
 *    functionality as sm_produce_constraint_name() except that it accepts
 *    a class template instead of a class name.
 *   return: constraint name
 *   tmpl(in): Class Template
 *   constraint_type(in): Constraint Type
 *   att_names(in): Attribute Names
 *   given_name(in): Optional constraint name.
 */
char *
sm_produce_constraint_name_tmpl (SM_TEMPLATE * tmpl,
				 DB_CONSTRAINT_TYPE constraint_type,
				 const char **att_names,
				 const int *asc_desc, const char *given_name)
{

  return sm_produce_constraint_name (template_classname (tmpl),
				     constraint_type, att_names, asc_desc,
				     given_name);
}

/*
 * sm_free_constraint_name() - Deallocate the system generated constraint name.
 *   return: none
 *   constraint_name(in): Constraint name
 */

void
sm_free_constraint_name (char *constraint_name)
{
  if (constraint_name != NULL)
    {
      free_and_init (constraint_name);
    }
}

/*
 * sm_check_index_exist() - Check index is duplicated.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class (or instance) pointer
 *   out_shared_cons_name(out):
 *   constraint_type: constraint type
 *   constraint_name(in): Constraint name.
 *   att_names(in): array of attribute names
 *   asc_desc(in): asc/desc info list
 */
static int
sm_check_index_exist (MOP classop,
		      char **out_shared_cons_name,
		      DB_CONSTRAINT_TYPE constraint_type,
		      const char *constraint_name,
		      const char **att_names, const int *asc_desc)
{
  int error = NO_ERROR;
  SM_CLASS *class_;

  if (!DB_IS_CONSTRAINT_INDEX_FAMILY (constraint_type))
    {
      return NO_ERROR;
    }

  error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_INDEX);
  if (error != NO_ERROR)
    {
      return error;
    }

  return classobj_check_index_exist (class_->constraints,
				     out_shared_cons_name,
				     class_->header.name, constraint_type,
				     constraint_name, att_names, asc_desc);
}

/*
 * sm_add_constraint() - Add a constraint to the class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class (or instance) pointer
 *   constraint_type(in): Type of constraint to add (UNIQUE, NOT NULL or INDEX)
 *   constraint_name(in): What to call the new constraint
 *   att_names(in): Names of attributes to be constrained
 *   asc_desc(in): asc/desc info list
 *   class_attributes(in): Flag.  A true value indicates that the names refer to
 *     		class attributes. A false value indicates that the names
 *     		refer to instance attributes.
 */
int
sm_add_constraint (MOP classop, DB_CONSTRAINT_TYPE constraint_type,
		   const char *constraint_name,
		   const char **att_names, const int *asc_desc,
		   const int *attrs_prefix_length, int class_attributes)
{
  int error = NO_ERROR;
  char *shared_cons_name = NULL;
  SM_TEMPLATE *def;

  if (att_names == NULL)
    {
      ERROR0 (error, ER_OBJ_INVALID_ARGUMENTS);
      return error;
    }

  switch (constraint_type)
    {
    case DB_CONSTRAINT_INDEX:
    case DB_CONSTRAINT_REVERSE_INDEX:
      error = sm_add_index (classop, constraint_type, constraint_name,
			    att_names, asc_desc, attrs_prefix_length);
      break;

    case DB_CONSTRAINT_UNIQUE:
    case DB_CONSTRAINT_REVERSE_UNIQUE:
    case DB_CONSTRAINT_PRIMARY_KEY:
      def = smt_edit_class_mop (classop);
      if (def == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = smt_add_constraint (def, constraint_type, constraint_name,
				      att_names, asc_desc, class_attributes,
				      NULL);
	  if (error == NO_ERROR)
	    {
	      error = sm_update_class (def, NULL);
	    }

	  if (error != NO_ERROR)
	    {
	      smt_quit (def);
	    }
	}
      break;

    case DB_CONSTRAINT_NOT_NULL:
      def = smt_edit_class_mop (classop);
      if (def == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = smt_add_constraint (def, constraint_type, constraint_name,
				      att_names, asc_desc, class_attributes,
				      NULL);
	  if (error == NO_ERROR)
	    {
	      error = do_check_fk_constraints (def, NULL);
	    }
	  if (error == NO_ERROR)
	    {
	      error = sm_update_class (def, NULL);
	    }

	  if (error != NO_ERROR)
	    {
	      smt_quit (def);
	    }
	}
      break;

    default:
      break;
    }

  return error;
}

/*
 * sm_drop_constraint() - Drops a constraint from a class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class (or instance) pointer
 *   constraint_type(in): Type of constraint to drop (UNIQUE, PK, NOT NULL or
 *                        INDEX). Foreign keys are not dropped by this
 *                        function. See dbt_drop_constraint instead.
 *   constraint_name(in): The name of the constraint to drop
 *   att_names(in): Names of attributes the constraint is defined on
 *   class_attributes(in): Whether the names in att_names refer to class
 *                         attributes or instance attributes.
 *   mysql_index_name(in): If true and constraint_type is DB_CONSTRAINT_INDEX,
 *                         the function behaves like MySQL and drops the
 *                         constraint with the name given by constraint_name
 *                         even if it has a different type than
 *                         DB_CONSTRAINT_INDEX
 */
int
sm_drop_constraint (MOP classop,
		    DB_CONSTRAINT_TYPE constraint_type,
		    const char *constraint_name,
		    const char **att_names, bool class_attributes,
		    bool mysql_index_name)
{
  int error = NO_ERROR;
  SM_TEMPLATE *def = NULL;

  if (mysql_index_name && constraint_type == DB_CONSTRAINT_INDEX)
    {
      SM_CLASS *smcls = NULL;

      /* MySQL does not differentiate between index types. Therefore MySQL's
         DROP INDEX idx ON tbl;
         will drop idx even if it is a UNIQUE index.
         On MySQL primary keys don't have names - and therefore should not be
         considered here - while foreign keys need to be dropped in two steps:
         first the constraint and then the associated index. We don't provide
         compatibility for foreign keys because CUBRID's behavior makes much
         more sense and changing it is difficult.
       */
      if (au_fetch_class (classop, &smcls, AU_FETCH_READ, AU_SELECT) ==
	  NO_ERROR)
	{
	  const SM_CLASS_CONSTRAINT *const constraint =
	    classobj_find_class_index (smcls, constraint_name);

	  if (constraint != NULL &&
	      (constraint->type == SM_CONSTRAINT_INDEX ||
	       constraint->type == SM_CONSTRAINT_REVERSE_INDEX ||
	       constraint->type == SM_CONSTRAINT_UNIQUE ||
	       constraint->type == SM_CONSTRAINT_REVERSE_UNIQUE))
	    {
	      constraint_type = db_constraint_type (constraint);
	    }
	}
    }

  switch (constraint_type)
    {
    case DB_CONSTRAINT_INDEX:
    case DB_CONSTRAINT_REVERSE_INDEX:
      error = sm_drop_index (classop, constraint_name);
      break;

    case DB_CONSTRAINT_UNIQUE:
    case DB_CONSTRAINT_REVERSE_UNIQUE:
    case DB_CONSTRAINT_PRIMARY_KEY:
      def = smt_edit_class_mop (classop);
      if (def == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = smt_drop_constraint (def, att_names, constraint_name,
				       class_attributes,
				       SM_MAP_CONSTRAINT_TO_ATTFLAG
				       (constraint_type));

	  if (error == NO_ERROR)
	    {
	      error = sm_update_class (def, NULL);
	    }

	  if (error != NO_ERROR)
	    {
	      smt_quit (def);
	    }
	}
      break;

    case DB_CONSTRAINT_NOT_NULL:
      def = smt_edit_class_mop (classop);
      if (def == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = smt_drop_constraint (def, att_names, constraint_name,
				       class_attributes, SM_ATTFLAG_NON_NULL);
	  if (error == NO_ERROR)
	    {
	      error = sm_update_class (def, NULL);
	    }

	  if (error != NO_ERROR)
	    {
	      smt_quit (def);
	    }
	}
      break;

    default:
      break;
    }

  return error;
}

/*
 * sm_is_possible_to_recreate_constraint() -
 *   return: Whether it is safe/efficient to drop a constraint on a class and
 *           then recreate it during truncation
 *   class_mop(in): class (or instance) pointer
 *   class_(in): class to be truncated
 *   constraint(in): the constraint to be considered
 * NOTE: If an index can refer to multiple classes (in an inheritance
 *       hierarchy), we choose not to recreate it. If it is a non-unique index
 *       (not involved in inheritance issues) or if the class or constraint
 *       are not involved in inheritance, we are sure that the index will
 *       only contain OIDs of the class being truncated. It is safe to drop
 *       and recreate the index in this scenario.
 */
static bool
sm_is_possible_to_recreate_constraint (MOP class_mop,
				       const SM_CLASS * const class_,
				       const SM_CLASS_CONSTRAINT *
				       const constraint)
{
  if (class_->inheritance == NULL && class_->users == NULL)
    {
      return true;
    }

  if (constraint->type == SM_CONSTRAINT_NOT_NULL ||
      constraint->type == SM_CONSTRAINT_INDEX ||
      constraint->type == SM_CONSTRAINT_REVERSE_INDEX)
    {
      return true;
    }

  if (class_->users != NULL)
    {
      return false;
    }

  assert (class_->inheritance != NULL && class_->users == NULL);
  if (sm_constraint_belongs_to_class (constraint, class_mop))
    {
      return true;
    }

  return false;
}

/*
 * free_constraint_info() - Frees a SM_CONSTRAINT_INFO list
 *   save_info(in/out): The list to be freed
 * NOTE: the pointer to the list is set to NULL after the list is freed.
 */
static void
free_constraint_info (SM_CONSTRAINT_INFO ** save_info)
{
  SM_CONSTRAINT_INFO *info = NULL;

  if (save_info == NULL || *save_info == NULL)
    {
      return;
    }

  info = *save_info;
  while (info != NULL)
    {
      SM_CONSTRAINT_INFO *next = info->next;
      char **crt_name_p = NULL;

      for (crt_name_p = info->att_names; *crt_name_p != NULL; ++crt_name_p)
	{
	  free_and_init (*crt_name_p);
	}
      free_and_init (info->att_names);

      if (info->ref_attrs != NULL)
	{
	  for (crt_name_p = info->ref_attrs; *crt_name_p != NULL;
	       ++crt_name_p)
	    {
	      free_and_init (*crt_name_p);
	    }
	  free_and_init (info->ref_attrs);
	}

      free_and_init (info->name);
      free_and_init (info->asc_desc);
      free_and_init (info->prefix_length);
      free_and_init (info->ref_cls_name);
      free_and_init (info->fk_cache_attr);

      free_and_init (info);
      info = next;
    }

  *save_info = NULL;
  return;
}

/*
 * save_constraint_info() - Saves the information necessary to recreate a
 *                          constraint
 *   return: NO_ERROR on success, non-zero for ERROR
 *   save_info(in/out): The information saved
 *   c(in): The constraint to be saved
 */
static int
save_constraint_info (SM_CONSTRAINT_INFO ** save_info,
		      const SM_CLASS_CONSTRAINT * const c)
{
  int error_code = NO_ERROR;
  SM_CONSTRAINT_INFO *new_constraint = NULL;
  int num_atts = 0;
  int i = 0;
  SM_ATTRIBUTE **crt_att_p = NULL;

  new_constraint =
    (SM_CONSTRAINT_INFO *) calloc (1, sizeof (SM_CONSTRAINT_INFO));
  if (new_constraint == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
	      sizeof (SM_CONSTRAINT_INFO));
      goto error_exit;
    }

  new_constraint->constraint_type = db_constraint_type (c);
  new_constraint->name = strdup (c->name);
  if (new_constraint->name == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
	      strlen (c->name) + 1);
      goto error_exit;
    }

  assert (c->attributes != NULL);
  for (crt_att_p = c->attributes, num_atts = 0; *crt_att_p != NULL;
       ++crt_att_p)
    {
      ++num_atts;
    }
  assert (num_atts > 0);

  new_constraint->att_names =
    (char **) calloc (num_atts + 1, sizeof (char *));
  if (new_constraint->att_names == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
	      (num_atts + 1) * sizeof (char *));
      goto error_exit;
    }

  for (crt_att_p = c->attributes, i = 0; *crt_att_p != NULL; ++crt_att_p, ++i)
    {
      const char *const attr_name = (*crt_att_p)->header.name;

      new_constraint->att_names[i] = strdup (attr_name);
      if (new_constraint->att_names[i] == NULL)
	{
	  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
		  strlen (attr_name) + 1);
	  goto error_exit;
	}
    }

  if (c->asc_desc != NULL)
    {
      int i = 0;

      new_constraint->asc_desc = (int *) calloc (num_atts, sizeof (int));
      if (new_constraint->asc_desc == NULL)
	{
	  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
		  num_atts * sizeof (int));
	  goto error_exit;
	}
      for (i = 0; i < num_atts; ++i)
	{
	  new_constraint->asc_desc[i] = c->asc_desc[i];
	}
    }

  if (c->attrs_prefix_length != NULL)
    {
      int i = 0;

      new_constraint->prefix_length = (int *) calloc (num_atts, sizeof (int));
      if (new_constraint->prefix_length == NULL)
	{
	  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
		  num_atts * sizeof (int));
	  goto error_exit;
	}
      for (i = 0; i < num_atts; ++i)
	{
	  new_constraint->prefix_length[i] = c->attrs_prefix_length[i];
	}
    }

  if (c->type == SM_CONSTRAINT_FOREIGN_KEY)
    {
      MOP ref_clsop = NULL;
      SM_CLASS *ref_cls = NULL;
      SM_CLASS_CONSTRAINT *pk_cons = NULL;

      assert (c->fk_info != NULL);
      assert (c->fk_info->next == NULL);

      ref_clsop = ws_mop (&(c->fk_info->ref_class_oid), NULL);
      if (ref_clsop == NULL)
	{
	  error_code = er_errid ();
	  goto error_exit;
	}
      error_code = au_fetch_class_force (ref_clsop, &ref_cls, AU_FETCH_READ);
      if (error_code != NO_ERROR)
	{
	  goto error_exit;
	}
      assert (ref_cls->constraints != NULL);

      new_constraint->ref_cls_name = strdup (ref_cls->header.name);
      if (new_constraint->ref_cls_name == NULL)
	{
	  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
		  strlen (ref_cls->header.name) + 1);
	  goto error_exit;
	}

      pk_cons = classobj_find_cons_primary_key (ref_cls->constraints);
      if (pk_cons == NULL)
	{
	  assert (false);
	  error_code = ER_FK_REF_CLASS_HAS_NOT_PK;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code,
		  1, ref_cls->header.name);
	  goto error_exit;
	}

      new_constraint->ref_attrs =
	(char **) calloc (num_atts + 1, sizeof (char *));
      if (new_constraint->att_names == NULL)
	{
	  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
		  (num_atts + 1) * sizeof (char *));
	  goto error_exit;
	}

      for (crt_att_p = pk_cons->attributes, i = 0; *crt_att_p != NULL;
	   ++crt_att_p, ++i)
	{
	  const char *const attr_name = (*crt_att_p)->header.name;

	  assert (i < num_atts);

	  new_constraint->ref_attrs[i] = strdup (attr_name);
	  if (new_constraint->ref_attrs[i] == NULL)
	    {
	      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
		      strlen (attr_name) + 1);
	      goto error_exit;
	    }
	}

      if (c->fk_info->cache_attr != NULL)
	{
	  new_constraint->fk_cache_attr = strdup (c->fk_info->cache_attr);
	  if (new_constraint->fk_cache_attr == NULL)
	    {
	      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
		      strlen (c->fk_info->cache_attr) + 1);
	      goto error_exit;
	    }
	}

      new_constraint->fk_delete_action = c->fk_info->delete_action;
      new_constraint->fk_update_action = c->fk_info->update_action;

      ref_cls = NULL;
      ref_clsop = NULL;
    }

  assert (new_constraint->next == NULL);
  while ((*save_info) != NULL)
    {
      save_info = &((*save_info)->next);
    }
  *save_info = new_constraint;

  return error_code;

error_exit:
  if (new_constraint != NULL)
    {
      free_constraint_info (&new_constraint);
    }
  return error_code;
}

/*
 * sm_truncate_using_delete() -
 *   return: error code
 *   class_mop(in): class (or instance) pointer
 */
static int
sm_truncate_using_delete (MOP class_mop)
{
  DB_SESSION *session = NULL;
  char delete_query[DB_MAX_IDENTIFIER_LENGTH + 64] = { 0 };
  int stmt_id = 0;
  int error = NO_ERROR;
  const char *class_name;
  bool save_tr_state;

  class_name = db_get_class_name (class_mop);
  if (class_name == NULL)
    {
      return ER_FAILED;
    }

  /* We will run a DELETE statement with triggers disabled. */
  save_tr_state = tr_set_execution_state (false);
  (void) snprintf (delete_query, sizeof (delete_query),
		   "DELETE /*+ RECOMPILE */ FROM %s;", class_name);

  session = db_open_buffer (delete_query);
  if (session == NULL)
    {
      error = er_errid ();
      goto end;
    }

  if (db_get_errors (session) || db_statement_count (session) != 1)
    {
      error = er_errid ();
      goto end;
    }

  stmt_id = db_compile_statement (session);
  if (stmt_id != 1)
    {
      error = er_errid ();
      goto end;
    }

  error = db_execute_statement (session, stmt_id, NULL);
  if (error < 0)
    {
      goto end;
    }

  error = NO_ERROR;

end:
  if (session != NULL)
    {
      db_free_query (session);
      db_close_session (session);
    }

  (void) tr_set_execution_state (save_tr_state);

  return error;
}

#if 0
/*
 * sm_truncate_using_destroy_heap() -
 *   return: error code
 *   class_mop(in): class (or instance) pointer
 */
static int
sm_truncate_using_destroy_heap (MOP class_mop)
{
  HFID *insts_hfid = NULL;
  SM_CLASS *class_ = NULL;
  int error = NO_ERROR;
  bool reuse_oid = false;
  OID *oid = NULL;

  oid = ws_oid (class_mop);
  assert (!OID_ISTEMP (oid));

  reuse_oid = sm_is_reuse_oid_class (class_mop);

  error = au_fetch_class (class_mop, &class_, AU_FETCH_WRITE, DB_AUTH_ALTER);
  if (error != NO_ERROR || class_ == NULL)
    {
      return er_errid ();
    }

  insts_hfid = sm_heap ((MOBJ) class_);
  assert (!HFID_IS_NULL (insts_hfid));

  /* Destroy the heap */
  error = heap_destroy_newly_created (insts_hfid, DB_CLUSTER_NODE_LOCAL);
  if (error != NO_ERROR)
    {
      return error;
    }

  HFID_SET_NULL (insts_hfid);
  ws_dirty (class_mop);

  error = locator_flush_class (class_mop);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* Create a new heap */
  error = heap_create (insts_hfid, oid, reuse_oid);
  if (error != NO_ERROR)
    {
      return error;
    }

  ws_dirty (class_mop);
  error = locator_flush_class (class_mop);

  return error;
}
#endif

/*
 * sm_truncate_class () - truncates a class
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class_mop(in):
 */
int
sm_truncate_class (MOP class_mop)
{
  SM_CLASS *class_ = NULL;
  SM_CLASS_CONSTRAINT *c = NULL;
  int error = NO_ERROR;
  SM_CONSTRAINT_INFO *unique_save_info = NULL;
  SM_CONSTRAINT_INFO *fk_save_info = NULL;
  SM_CONSTRAINT_INFO *index_save_info = NULL;
  SM_CONSTRAINT_INFO *saved = NULL;
  DB_CTMPL *ctmpl = NULL;
  SM_ATTRIBUTE *att = NULL;
  bool keep_pk = false;
  int au_save = 0;

  assert (class_mop != NULL);

  error = tran_savepoint (UNIQUE_SAVEPOINT_SM_TRUNCATE, false);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* 
   * DO NOT WRITE REPLICATION LOG DURING TRUNCATION.
   *
   * Actually we do not want to write replication logs for deleting records 
   * on sm_truncate_using_delete function. Because 'truncate' schema will 
   * be transferred to the slave DB server after success of truncation.
   */
  db_set_suppress_repl_on_transaction (true);


  /* We need to flush everything so that the server logs the inserts that
   * happened before the truncate. We need this in order to make sure that
   * a rollback takes us into a consistent state. If we can prove that
   * simply discarding the objects would work correctly we would be able to
   * remove this call. However, it's better to be safe than sorry.
   */
  error = sm_flush_and_decache_objects (class_mop, true);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = au_fetch_class (class_mop, &class_, AU_FETCH_WRITE, DB_AUTH_ALTER);
  if (error != NO_ERROR || class_ == NULL)
    {
      error = er_errid ();
      goto error_exit;
    }

  c = classobj_find_cons_primary_key (class_->constraints);
  if (c != NULL
      && classobj_is_pk_referred (class_mop, c->fk_info, false, NULL))
    {
      /* We need to perform a normal delete operation because we need to
       * execute the FOREIGN KEY actions on the referring classes. We also
       * need to keep the PRIMARY KEY constraint in order to correctly perform
       * the FOREIGN KEY actions.
       */
      keep_pk = true;
    }

  /* collect index information */
  for (c = class_->constraints; c; c = c->next)
    {
      if (!SM_IS_CONSTRAINT_INDEX_FAMILY (c->type))
	{
	  assert (c->type == SM_CONSTRAINT_NOT_NULL);
	  continue;
	}

      if (keep_pk == true && c->type == SM_CONSTRAINT_PRIMARY_KEY)
	{
	  /* Do not save PK referred by FK. the PK can't be dropped. */
	  continue;
	}

      if (sm_is_possible_to_recreate_constraint (class_mop, class_, c))
	{
	  /* All the OIDs in the index should belong to the current class, so
	   * it is safe to drop and create the constraint again. We save the
	   * information required to recreate the constraint.
	   */

	  if (SM_IS_CONSTRAINT_UNIQUE_FAMILY (c->type))
	    {
	      if (save_constraint_info (&unique_save_info, c) != NO_ERROR)
		{
		  goto error_exit;
		}
	    }
	  else if (c->type == SM_CONSTRAINT_FOREIGN_KEY)
	    {
	      if (save_constraint_info (&fk_save_info, c) != NO_ERROR)
		{
		  goto error_exit;
		}
	    }
	  else
	    {
	      if (save_constraint_info (&index_save_info, c) != NO_ERROR)
		{
		  goto error_exit;
		}
	    }
	}
    }

  /* Drop constraints.
   * It's also faster to do this if we truncate by deleting. */

  /* FK must be dropped earlier than PK, because of self referencing case */
  if (fk_save_info != NULL)
    {
      ctmpl = dbt_edit_class (class_mop);
      if (ctmpl == NULL)
	{
	  error = er_errid ();
	  goto error_exit;
	}

      for (saved = fk_save_info; saved != NULL; saved = saved->next)
	{
	  error = dbt_drop_constraint (ctmpl, saved->constraint_type,
				       saved->name,
				       (const char **) saved->att_names, 0);
	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (ctmpl);
	      goto error_exit;
	    }
	}

      if (dbt_finish_class (ctmpl) == NULL)
	{
	  dbt_abort_class (ctmpl);
	  error = er_errid ();
	  goto error_exit;
	}
    }

  for (saved = unique_save_info; saved != NULL; saved = saved->next)
    {
      error = sm_drop_constraint (class_mop, saved->constraint_type,
				  saved->name,
				  (const char **) saved->att_names, 0, false);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  for (saved = index_save_info; saved != NULL; saved = saved->next)
    {
      error = sm_drop_index (class_mop, saved->name);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

#if 0
  if (keep_pk == true && db_Enable_replications <= 0)
    {
      error = sm_truncate_using_delete (class_mop);
    }
  else
    {
      error = sm_truncate_using_destroy_heap (class_mop);
    }
  if (error != NO_ERROR)
    {
      goto error_exit;
    }
#else
  error = sm_truncate_using_delete (class_mop);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }
#endif

  /* Normal index must be created earlier than unique constraint or FK,
   * because of shared btree case. */
  for (saved = index_save_info; saved != NULL; saved = saved->next)
    {
      error = sm_add_index (class_mop, saved->constraint_type,
			    saved->name, (const char **) saved->att_names,
			    saved->asc_desc, saved->prefix_length);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  /* PK must be created earlier than FK, because of self referencing case */
  for (saved = unique_save_info; saved != NULL; saved = saved->next)
    {
      error = sm_add_constraint (class_mop, saved->constraint_type,
				 saved->name,
				 (const char **) saved->att_names,
				 saved->asc_desc, saved->prefix_length, 0);

      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  /* To drop all xasl cache related class, we need to touch class. */
  ctmpl = dbt_edit_class (class_mop);
  if (ctmpl == NULL)
    {
      error = er_errid ();
      goto error_exit;
    }

  for (saved = fk_save_info; saved != NULL; saved = saved->next)
    {
      error = dbt_add_foreign_key (ctmpl, saved->name,
				   (const char **) saved->att_names,
				   saved->ref_cls_name,
				   (const char **) saved->ref_attrs,
				   saved->fk_delete_action,
				   saved->fk_update_action,
				   saved->fk_cache_attr);

      if (error != NO_ERROR)
	{
	  dbt_abort_class (ctmpl);
	  goto error_exit;
	}
    }

  if (dbt_finish_class (ctmpl) == NULL)
    {
      dbt_abort_class (ctmpl);
      error = er_errid ();
      goto error_exit;
    }

  /* reset auto_increment starting value */
  for (att = db_get_attributes (class_mop); att != NULL;
       att = db_attribute_next (att))
    {
      if (att->auto_increment != NULL)
	{
	  AU_DISABLE (au_save);
	  error = do_reset_auto_increment_serial (att->auto_increment);
	  AU_ENABLE (au_save);

	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	}
    }

  if (unique_save_info != NULL)
    {
      free_constraint_info (&unique_save_info);
    }

  if (fk_save_info != NULL)
    {
      free_constraint_info (&fk_save_info);
    }

  if (index_save_info != NULL)
    {
      free_constraint_info (&index_save_info);
    }

  /* Do not suppress writing replication log any more. */
  db_set_suppress_repl_on_transaction (false);

  return NO_ERROR;

error_exit:

  if (error != ER_LK_UNILATERALLY_ABORTED)
    {
      tran_abort_upto_savepoint (UNIQUE_SAVEPOINT_SM_TRUNCATE);
    }

  if (unique_save_info != NULL)
    {
      free_constraint_info (&unique_save_info);
    }

  if (fk_save_info != NULL)
    {
      free_constraint_info (&fk_save_info);
    }

  if (index_save_info != NULL)
    {
      free_constraint_info (&index_save_info);
    }

  /* Do not suppress writing replication log any more. */
  db_set_suppress_repl_on_transaction (false);

  return error;
}

/*
 * sm_is_global_class () - Tests the global class flag of a class object.
 *   return: non-zero if class is a global class
 *   op(in): class object
 */
bool
sm_is_global_class (MOP op)
{
  return sm_get_class_flag (op, SM_CLASSFLAG_GLOBAL) ? true : false;
}

/*
 * sm_real_hfid () - Return the pointer of real_hfid in a class 
 * return:
 *
 *  classobj(in):
 */
HFID *
sm_real_hfid (MOBJ classobj)
{
  HFID *heap = NULL;
  SM_CLASS *class_;
  if (classobj)
    {
      class_ = (SM_CLASS *) classobj;
      heap = &class_->real_hfid;
    }

  return heap;
}

/*
 * sm_get_real_hfid () - Return the real_hfid of a class given a MOP 
 * return:
 *
 *  op(in):
 */
HFID *
sm_get_real_hfid (MOP op)
{
  SM_CLASS *class_ = NULL;
  HFID *heap;

  heap = NULL;
  if (locator_is_class (op, DB_FETCH_READ))
    {
      if (au_fetch_class (op, &class_, AU_FETCH_READ, AU_SELECT) == NO_ERROR)
	{
	  heap = &class_->real_hfid;
	}
    }
  return heap;
}

/*
 * sm_real_oid () - Return the pointer of real_oid in a class 
 * return:
 *
 *  classobj(in):
 */
OID *
sm_real_oid (MOBJ classobj)
{
  OID *oid = NULL;
  SM_CLASS *class_;
  if (classobj)
    {
      class_ = (SM_CLASS *) classobj;
      oid = &class_->real_oid;
    }

  return oid;
}

/*
 * sm_get_real_oid () - Return the real_oid of a class given a MOP
 * return:
 *
 *  op(in):
 */
OID *
sm_get_real_oid (MOP op)
{
  SM_CLASS *class_ = NULL;
  OID *real_oid = NULL;

  if (locator_is_class (op, DB_FETCH_READ))
    {
      if (au_fetch_class (op, &class_, AU_FETCH_READ, AU_SELECT) == NO_ERROR)
	{
	  real_oid = &class_->real_oid;
	}
    }
  return real_oid;

}

/*
 * sm_node_name () - Return the pointer of node_name in a class 
 * return:
 *
 *  classobj(in):
 */
const char *
sm_node_name (MOBJ classobj)
{
  const char *node_name;
  SM_CLASS *class_;
  if (classobj)
    {
      class_ = (SM_CLASS *) classobj;
      node_name = class_->node_name;
    }

  return node_name;

}

/*
 * sm_get_node_name () - Return the node_name of a class given a MOP
 * op (in):
 */
const char *
sm_get_node_name (MOP op)
{
  SM_CLASS *class_ = NULL;
  const char *node_name;

  if (locator_is_class (op, DB_FETCH_READ))
    {
      if (au_fetch_class (op, &class_, AU_FETCH_READ, AU_SELECT) == NO_ERROR)
	{
	  node_name = class_->node_name;
	}
    }
  return node_name;
}

/*
 * sm_trans_to_proxy () - translate a sm_class to be proxy
 * return: 
 *
 *  real_oid(in):
 *  class_(int): 
 */
void
sm_trans_to_proxy (OID * real_oid, SM_CLASS * class_)
{
  HFID *heap;

  assert (real_oid && class_);
  heap = sm_heap ((MOBJ) class_);

  class_->class_type = SM_PCLASS_CT;
  COPY_OID (sm_real_oid ((MOBJ) class_), real_oid);
  HFID_COPY (sm_real_hfid ((MOBJ) class_), heap);

  if (class_->new_ != NULL)
    {
      COPY_OID (&class_->new_->real_oid, real_oid);
      HFID_COPY (&class_->new_->real_hfid, heap);
    }

  HFID_SET_NULL (heap);
}

/*
 * sm_trans_to_global() - translate a sm_class to global
 * return:
 *
 *  class_(in):
 */
static void
sm_trans_to_global (SM_CLASS * class_)
{
  HFID *real_heap;
  assert (class_);

  real_heap = sm_real_hfid ((MOBJ) class_);

  class_->class_type = SM_CLASS_CT;
  HFID_COPY (sm_heap ((MOBJ) class_), real_heap);
}


/*
 * sm_sync_global_schema() - sync an global schema to remote node
 * return: error code
 *
 *  classop(in): global class MOP
 *  remote_node(in): remote node ID
 */
int
sm_sync_global_schema (MOP classop, unsigned int remote_node)
{
  SM_CLASS *class_;
  int rmt_nodeid_bak;
  OID rmt_oid_bak;
  HFID hfid_bak;
  OID class_temp_oid;
  SM_CLASS_TYPE class_type_bak;
  LOCK lock = locator_fetch_mode_to_lock (DB_FETCH_WRITE, LC_CLASS);
  int sub = 1;
  int error_code = NO_ERROR;

  if (ws_find (classop, (MOBJ *) & class_) == WS_FIND_MOP_DELETED)
    {
      return NO_ERROR;
    }

  if (locator_lockhint_classes
      (1, &class_->header.name, &lock, &sub, 1,
       remote_node) == LC_CLASSNAME_ERROR)
    {
      return er_errid ();
    }

  OID_ASSIGN_TEMPOID (&class_temp_oid);
  if (OID_ISNULL (&class_temp_oid))
    {
      if (locator_all_flush () != NO_ERROR)
	{
	  return er_errid ();
	}

      OID_INIT_TEMPID ();
      OID_ASSIGN_TEMPOID (&class_temp_oid);
    }

  if (locator_reserve_class_names
      (1, &class_->header.name, &class_temp_oid,
       remote_node) != LC_CLASSNAME_RESERVED)
    {
      error_code = ER_LC_CLASSNAME_EXIST;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_CLASSNAME_EXIST, 1,
	      class_->header.name);
      return error_code;
    }

  /* backup the values */
  class_type_bak = class_->class_type;
  HFID_COPY (&hfid_bak, &class_->header.heap);
  rmt_nodeid_bak = WS_NODEID (classop);
  COPY_OID (&rmt_oid_bak, WS_OID (classop));

  /* set the new values */
  ws_set_remote (classop, remote_node);
  class_->class_type = SM_PCLASS_CT;
  HFID_SET_NULL (&class_->header.heap);
  class_->header.heap.vfid.volid = boot_User_volid;
  COPY_OID (WS_OID (classop), &class_temp_oid);

  error_code = allocate_disk_structures_by_node (classop, class_, NULL);
  if (error_code != NO_ERROR)
    {
      goto restore_and_exit;
    }

restore_and_exit:
  /* restore the values */
  class_->class_type = class_type_bak;
  ws_set_remote (classop, rmt_nodeid_bak);
  COPY_OID (WS_OID (classop), &rmt_oid_bak);
  HFID_COPY (&class_->header.heap, &hfid_bak);

  return error_code;
}


/*
 * sm_set_remote - Set MOPs to remote. 
 * return : return true if success, otherwise return false.
 *
 *  op(in):
 *  node(in):
 * Note: Including : users, inheritance, partition_of.
 */
bool
sm_set_remote (MOP op, int node)
{
  MOBJ object;
  bool success;
  int save_node;
  CHECK_1ARG_FALSE (op);

  save_node = WS_NODEID (op);

  success = ws_set_remote (op, node);
  if (!success)
    {
      goto end;
    }

  if (ws_find (op, &object) == WS_FIND_MOP_DELETED)
    {
      object = op->object;
    }

  if (object == NULL)
    {
      success = false;
      goto end;
    }

  if (object != NULL && locator_is_class (op, DB_FETCH_READ)
      && ((SM_CLASS *) object)->flags & SM_CLASSFLAG_GLOBAL)
    {
      DB_OBJLIST *obj_list;
      SM_CLASS *class_ = (SM_CLASS *) object;

      obj_list = class_->users;
      while (obj_list != NULL && obj_list->op != NULL && success)
	{
	  success = ws_set_remote (obj_list->op, node);
	  obj_list = obj_list->next;
	}

      obj_list = class_->inheritance;
      while (obj_list != NULL && obj_list->op != NULL && success)
	{
	  success = ws_set_remote (obj_list->op, node);
	  obj_list = obj_list->next;
	}

      if (success && class_->partition_of != NULL)
	{
	  success = ws_set_remote (class_->partition_of, node);
	}
    }

  if (!success)
    {
      /* This operation could guarantee success. */
      sm_set_remote (op, save_node);
    }

end:
  return success;
}


/*
 * sm_get_remote_info_if_need(): if the class is a global partition class, 
 * we need get sm_class->partiton_of from all remote nodes.
 *
 * return : error if failed
 *
 *  classop (in)
 */
int
sm_get_remote_info_if_need (MOP classop)
{
  int is_partition;
  SM_CLASS *class_, *subclass_;
  int error = NO_ERROR;
  MOP ct_instance = NULL;
  DB_OBJLIST *users;
  DB_OBJECT *catalog_class;
  DB_VALUE name_val;
  char *class_name;

  if (classop == NULL || !locator_is_class (classop, DB_FETCH_READ))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  if (!sm_is_global_class (classop))
    {
      /* this is not a global class, we needn't get remote information */
      goto end;
    }

  error = do_is_partitioned_classobj (&is_partition, classop, NULL, NULL);
  if (error != NO_ERROR)
    {
      goto end;
    }
  if (is_partition != 1)
    {
      /* this is not a partition parent table we needn't get remote information. */
      goto end;
    }

  /* get super class remote info. */
  error = au_fetch_class (classop, &class_, AU_FETCH_READ, AU_SELECT);
  if (error != NO_ERROR)
    {
      goto end;
    }

  catalog_class = sm_find_class (CT_CLASS_NAME);
  if (catalog_class == NULL)
    {
      error = er_errid ();
      goto end;
    }

  class_name = sm_classobj_name ((MOBJ) class_);
  db_make_string (&name_val, class_name);

  ct_instance =
    obj_find_oids_in_catalog_global (catalog_class, CLASS_ATT_NAME,
				     &name_val);
  pr_clear_value (&name_val);
  if (ct_instance == NULL)
    {
      error = ER_FAILED;
      goto end;
    }

  error = obj_find_partition_global (classop, class_, ct_instance);
  if (error != NO_ERROR)
    {
      goto end;
    }

  /* get supclass remote info. */
  for (users = class_->users; users != NULL && error == NO_ERROR;
       users = users->next)
    {
      error =
	au_fetch_class (users->op, &subclass_, AU_FETCH_READ, AU_SELECT);
      if (error != NO_ERROR)
	{
	  break;
	}
      error = obj_find_partition_global (users->op, subclass_, ct_instance);
    }

end:
  return error;
}
