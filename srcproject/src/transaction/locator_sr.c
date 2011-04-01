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
 * locator_sr.c : Transaction object locator (at server)
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "locator_sr.h"
#include "locator.h"
#include "memory_alloc.h"
#include "storage_common.h"
#include "oid.h"
#include "memory_hash.h"
#include "error_manager.h"
#include "xserver_interface.h"
#include "list_file.h"
#include "query_manager.h"
#include "slotted_page.h"
#include "extendible_hash.h"
#include "btree.h"
#include "btree_load.h"
#include "heap_file.h"
#include "object_representation.h"
#include "object_representation_sr.h"
#include "log_manager.h"
#include "lock_manager.h"
#include "system_catalog.h"
#include "replication.h"
#include "critical_section.h"
#if defined(SERVER_MODE)
#include "connection_error.h"
#include "remote_query.h"
#include "thread_impl.h"
#endif /* SERVER_MODE */
#include "object_print.h"
#include "object_primitive.h"
#include "object_domain.h"
#include "system_parameter.h"
#include "log_impl.h"
#include "transaction_sr.h"
#include "boot_sr.h"

#include "db.h"

#if defined(DMALLOC)
#include "dmalloc.h"
#endif /* DMALLOC */

/* TODO : remove */
extern bool catcls_Enable;

static const int LOCATOR_GUESS_NUM_NESTED_REFERENCES = 100;
#define LOCATOR_GUESS_HT_SIZE    LOCATOR_GUESS_NUM_NESTED_REFERENCES * 2

#define MAX_CLASSNAME_CACHE_ENTRIES     1024
#define CLASSNAME_CACHE_SIZE            1024

extern int catcls_insert_catalog_classes (THREAD_ENTRY * thread_p,
					  RECDES * record);
extern int catcls_delete_catalog_classes (THREAD_ENTRY * thread_p,
					  const char *name, OID * class_oid);
extern int catcls_update_catalog_classes (THREAD_ENTRY * thread_p,
					  const char *name, RECDES * record);
extern int catcls_remove_entry (OID * class_oid);

typedef struct locator_tmp_classname_action LOCATOR_TMP_CLASSNAME_ACTION;
struct locator_tmp_classname_action
{
  LC_FIND_CLASSNAME action;	/* The transient operation, delete or reserve
				 * name
				 */
  OID oid;			/* The class identifier of classname             */
  LOG_LSA savep_lsa;		/* A top action LSA address (likely a savepoint)
				 * for return NULL is for current
				 */
  LOCATOR_TMP_CLASSNAME_ACTION *prev;	/* To previous top action */
};

typedef struct locator_tmp_classname_entry LOCATOR_TMP_CLASSNAME_ENTRY;
struct locator_tmp_classname_entry
{
  char *name;			/* Name of the class                               */
  int tran_index;		/* Transaction of entry                            */
  LOCATOR_TMP_CLASSNAME_ACTION current;	/* The most current action */
};

typedef struct locator_tmp_desired_classname_entries
  LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES;
struct locator_tmp_desired_classname_entries
{
  int tran_index;
  LOG_LSA *savep_lsa;
};

typedef struct locator_return_nxobj LOCATOR_RETURN_NXOBJ;
struct locator_return_nxobj
{				/* Location of next object to return in communication
				   (fetch) area */
  LC_COPYAREA *comm_area;	/* Communication area where objects are
				 * returned and described
				 */
  LC_COPYAREA_MANYOBJS *mobjs;	/* Location in the communication area
				 * where all objects to be returned are
				 * described.
				 */
  LC_COPYAREA_ONEOBJ *obj;	/* Location in the communication area
				 * where the next object to return is
				 * described.
				 */
  HEAP_SCANCACHE *ptr_scancache;	/* Scan cache used for fetching
					 * purposes
					 */
  HEAP_SCANCACHE area_scancache;	/* Scan cache used for fetching
					 * purposes
					 */
  RECDES recdes;		/* Location in the communication area
				 * where the content of the next object
				 * to return is placed.
				 */
  int area_offset;		/* Relative offset to recdes->data in the
				 * communication area
				 */
};

bool locator_Dont_check_foreign_key = false;

static EHID locator_Classnames_table;
static EHID *locator_Eht_classnames = &locator_Classnames_table;
static MHT_TABLE *locator_Mht_classnames = NULL;

static const HFID NULL_HFID = { {-1, -1}, -1 };

static void locator_permoid_class_name (THREAD_ENTRY * thread_p,
					const char *classname,
					OID * class_oid);
static int locator_force_drop_class_name_entry (const void *name, void *ent,
						void *rm);
static int locator_drop_class_name_entry (const void *name, void *ent,
					  void *rm);
static int locator_savepoint_class_name_entry (const void *ignore_name,
					       void *ent, void *sp);
static int locator_decache_class_name_entries (void);
static int locator_decache_class_name_entry (const void *name, void *ent,
					     void *dc);
static int locator_print_class_name (FILE * outfp, const void *key,
				     void *ent, void *ignore);
static int locator_check_class_on_heap (THREAD_ENTRY * thread_p,
					void *classname, void *classoid,
					void *xvalid);
static SCAN_CODE locator_return_object (THREAD_ENTRY * thread_p,
					LOCATOR_RETURN_NXOBJ * assign,
					OID * oid, int chn);
static int locator_find_lockset_missing_class_oids (THREAD_ENTRY * thread_p,
						    LC_LOCKSET * lockset);
static LC_LOCKSET *locator_all_reference_lockset (THREAD_ENTRY * thread_p,
						  OID * oid, int prune_level,
						  LOCK inst_lock,
						  LOCK class_lock,
						  int quit_on_errors);
static bool locator_notify_decache (const OID * oid, void *notify_area);
static int locator_guess_sub_classes (THREAD_ENTRY * thread_p,
				      LC_LOCKHINT ** lockhint_subclasses);
static int locator_insert_force (THREAD_ENTRY * thread_p, HFID * hfid,
				 OID * oid, RECDES * recdes, int has_index,
				 int op_type, HEAP_SCANCACHE * scan_cache,
				 int *force_count);
static int locator_update_force (THREAD_ENTRY * thread_p, HFID * hfid,
				 OID * oid, RECDES * ikdrecdes,
				 RECDES * recdes, int has_index,
				 ATTR_ID * att_id, int n_att_id, int op_type,
				 HEAP_SCANCACHE * scan_cache,
				 int *force_count, bool not_check_fk,
				 REPL_INFO_TYPE repl_info);
static int locator_delete_force (THREAD_ENTRY * thread_p, HFID * hfid,
				 OID * oid, int has_index, int op_type,
				 HEAP_SCANCACHE * scan_cache,
				 int *force_count);
static int locator_force_for_multi_update (THREAD_ENTRY * thread_p,
					   LC_COPYAREA * force_area);

static void locator_increase_catalog_count (THREAD_ENTRY * thread_p,
					    OID * cls_oid);
static void locator_decrease_catalog_count (THREAD_ENTRY * thread_p,
					    OID * cls_oid);

static int
locator_set_foreign_key_object_cache (THREAD_ENTRY * thread_p,
				      OID * class_oid, OID * inst_oid,
				      OID * pk_oid, RECDES * old_recdes,
				      RECDES * new_recdes, int cache_attr_id,
				      LC_COPYAREA ** cparea);
static int locator_check_foreign_key (THREAD_ENTRY * thread_p, HFID * hfid,
				      OID * class_oid, OID * inst_oid,
				      RECDES * recdes, RECDES * new_recdes,
				      bool * is_cached,
				      LC_COPYAREA ** copyarea);
static int locator_check_primary_key_delete (THREAD_ENTRY * thread_p,
					     OR_INDEX * index,
					     DB_VALUE * key);
static int locator_repair_object_cache (THREAD_ENTRY * thread_p,
					OR_INDEX * index, DB_VALUE * key,
					OID * pk_oid);
static int locator_check_primary_key_update (THREAD_ENTRY * thread_p,
					     OR_INDEX * index,
					     DB_VALUE * key);
static TP_DOMAIN *locator_make_midxkey_domain (OR_INDEX * index);
static DISK_ISVALID
locator_check_unique_btree_entries (THREAD_ENTRY * thread_p, BTID * btid,
				    RECDES * classrec, ATTR_ID * attr_ids,
				    const char *btname, bool repair);
static bool locator_was_index_already_applied (HEAP_CACHE_ATTRINFO *
					       index_attrinfo, BTID * btid,
					       int pos);
static LC_FIND_CLASSNAME xlocator_reserve_class_name (THREAD_ENTRY * thread_p,
						      const char *classname,
						      const OID * class_oid);



/*
 * locator_initialize () - Initialize the locator on the server
 *
 * return: EHID *(classname_table on success or NULL on failure)
 *
 *   classname_table(in): Classname_to_OID permanent hash file
 *
 * Note: Initialize the server transaction object locator. Currently,
 *              only the classname memory hash table for transient entries is
 *              initialized.
 */
EHID *
locator_initialize (THREAD_ENTRY * thread_p, EHID * classname_table)
{
  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return NULL;
    }

  VFID_COPY (&locator_Eht_classnames->vfid, &classname_table->vfid);
  locator_Eht_classnames->pageid = classname_table->pageid;
  if (locator_Mht_classnames != NULL)
    {
      mht_clear (locator_Mht_classnames);
    }
  else
    {
      locator_Mht_classnames = mht_create ("Memory hash Classname to OID",
					   CLASSNAME_CACHE_SIZE,
					   mht_1strhash, mht_strcmpeq);
    }

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  if (locator_Mht_classnames == NULL)
    {
      return NULL;
    }
  else
    {
      return classname_table;
    }
}

/*
 * locator_finalize () - Terminates the locator on the server
 *
 * return: nothing
 *
 * Note:Terminate the object locator on the server. Currently, only
 *              the classname memory hash table is removed.
 */
void
locator_finalize (THREAD_ENTRY * thread_p)
{
  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We will leak resources.
       */
      return;
    }

  if (locator_Mht_classnames == NULL)
    {
      return;
    }

  if (csect_enter (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) != NO_ERROR)
    {
      return;
    }
  (void) mht_map (locator_Mht_classnames,
		  locator_force_drop_class_name_entry, NULL);
  mht_destroy (locator_Mht_classnames);
  locator_Mht_classnames = NULL;
  csect_exit (CSECT_CT_OID_TABLE);

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
}

/*
 * xlocator_reserve_class_names () - Reserve several classnames
 *
 * return: LC_FIND_CLASSNAME
 *                       (either of LC_CLASSNAME_RESERVED,
 *                                  LC_CLASSNAME_EXIST,
 *                                  LC_CLASSNAME_ERROR)
 *
 *   num_classes(in): Number of classes
 *   classnames(in): Names of the classes
 *   class_oids(in): Object identifiers of the classes
 */
LC_FIND_CLASSNAME
xlocator_reserve_class_names (THREAD_ENTRY * thread_p, const int num_classes,
			      const char **classnames, const OID * class_oids)
{
  int i = 0;
  LC_FIND_CLASSNAME result = LC_CLASSNAME_RESERVED;

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return LC_CLASSNAME_ERROR;
    }

  for (i = 0; i < num_classes; ++i)
    {
      result = xlocator_reserve_class_name (thread_p, classnames[i],
					    &class_oids[i]);
      if (result != LC_CLASSNAME_RESERVED)
	{
	  /* We could potentially revert the reservation but the transient
	     entries should be properly cleaned up by the rollback so we don't
	     really need to do this here. */
	  break;
	}
    }

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  return result;
}

/*
 * xlocator_reserve_class_name () - Reserve a classname
 *
 * return: LC_FIND_CLASSNAME
 *                       (either of LC_CLASSNAME_RESERVED,
 *                                  LC_CLASSNAME_EXIST,
 *                                  LC_CLASSNAME_ERROR)
 *
 *   classname(in): Name of class
 *   class_oid(in): Object identifier of the class
 *
 * Note: Reserve the name of a class.
 *              If there is an entry on the transient/memory classname table,
 *              we can proceed if the entry belongs to the current
 *              transaction, otherwise, we must wait until the transaction
 *              holding the entry terminates since the fate of the classname
 *              entry cannot be predicted. If the transient entry belongs to
 *              the current transaction, we can reserve the name only if the
 *              entry indicates that a class with such a name has been
 *              deleted or reserved. If there is not a transient entry with
 *              such a name the permanent classname to OID table is consulted
 *              and depending on the existence of an entry, the classname is
 *              reserved or an error is returned. The classname_to_OID entry
 *              that is created is a transient entry in main memory, the entry
 *              is added onto the permanent hash when the class is stored in
 *              the page buffer pool/database.
 */
static LC_FIND_CLASSNAME
xlocator_reserve_class_name (THREAD_ENTRY * thread_p, const char *classname,
			     const OID * class_oid)
{
  EH_SEARCH search;
  OID oid;
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  LOCATOR_TMP_CLASSNAME_ACTION *old_action;
  LC_FIND_CLASSNAME reserve = LC_CLASSNAME_RESERVED;
  OID tmp_classoid;

start:
  reserve = LC_CLASSNAME_RESERVED;

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return LC_CLASSNAME_ERROR;
    }

  /* Is there any transient entries on the classname hash table ? */
  entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						   classname);
  if (entry != NULL && entry->current.action != LC_CLASSNAME_EXIST)
    {
      /*
       * We can only proceed if the entry belongs to the current transaction,
       * otherwise, we must lock the class associated with the classname and
       * retry the operation once the lock is granted.
       */
      if (entry->tran_index == LOG_FIND_THREAD_TRAN_INDEX (thread_p))
	{
	  /*
	   * The name can be reserved only if it has been deleted or
	   * previously reserved. We allow double reservations in order for
	   * multiple table renaming to properly reserve all the names
	   * involved.
	   */
	  if (entry->current.action == LC_CLASSNAME_DELETED
	      || entry->current.action == LC_CLASSNAME_DELETED_RENAME
	      || entry->current.action == LC_CLASSNAME_RESERVED)
	    {
	      /*
	       * The entry can be changed.
	       * Do we need to save the old action...just in case we do a
	       * partial rollback ?
	       */
	      if (!LSA_ISNULL (&entry->current.savep_lsa))
		{
		  /*
		   * There is a possibility of returning to this top LSA
		   * (savepoint). Save the action.. just in case
		   */
		  old_action =
		    (LOCATOR_TMP_CLASSNAME_ACTION *)
		    malloc (sizeof (*old_action));
		  if (old_action == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1,
			      sizeof (*old_action));
		      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		      return LC_CLASSNAME_ERROR;
		    }
		  *old_action = entry->current;
		  LSA_SET_NULL (&entry->current.savep_lsa);
		  entry->current.prev = old_action;
		}
	      entry->current.action = LC_CLASSNAME_RESERVED;
	      COPY_OID (&entry->current.oid, class_oid);
	    }
	  else
	    {
	      reserve = LC_CLASSNAME_EXIST;
	    }
	}
      else
	{
	  COPY_OID (&tmp_classoid, &entry->current.oid);

	  /*
	   * The fate of this entry is known when the transaction holding
	   * this entry either commits or aborts. Get the lock and try again.
	   */

	  /*
	   * Exit from critical section since we are going to be suspended and
	   * then retry again.
	   */
	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

	  if (lock_object (thread_p, &tmp_classoid, oid_Root_class_oid,
			   X_LOCK, LK_UNCOND_LOCK) != LK_GRANTED)
	    {
	      /*
	       * Unable to acquired lock
	       */
	      return LC_CLASSNAME_ERROR;
	    }
	  else
	    {
	      /*
	       * Try again
	       * Remove the lock.. since the above was a dirty read
	       */
	      lock_unlock_object (thread_p, &tmp_classoid, oid_Root_class_oid,
				  X_LOCK, true);
	      goto start;
	    }
	}
    }
  else if (entry != NULL)
    {
      /* There is a class with such a name on the classname cache. */
      reserve = LC_CLASSNAME_EXIST;
    }
  else
    {
      /*
       * Is there a class with such a name on the permanent classname hash
       * table ?
       *
       * It is too dangerous to call the extendible hash table while holding
       * the critical section. We do not know what the extendible hash will
       * do. It may be blocked. Instead, we do the following:
       *
       *    Exit critical section
       *    execute ehash_search
       *    if not found,
       *       enter critical section
       *       double check to make sure that there is not a new entry on
       *              this name. if there is one, return immediately with
       *              classname exist, since I was not the one that add
       *              the entry.
       *       reserver name
       *       exit critical section
       */

      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
      search = ehash_search (thread_p, locator_Eht_classnames,
			     (char *) classname, &oid);
      if (search == EH_ERROR_OCCURRED)
	{
	  /*
	   * Some kind of failure. We must notify the error to the caller.
	   */
	  return LC_CLASSNAME_ERROR;
	}
      if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT)
	  != NO_ERROR)
	{
	  return LC_CLASSNAME_ERROR;
	}

      /* Double check */
      entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						       classname);
      if (entry != NULL)
	{
	  reserve = LC_CLASSNAME_EXIST;
	}
      else
	{
	  if (search == EH_KEY_NOTFOUND)
	    {
	      entry =
		(LOCATOR_TMP_CLASSNAME_ENTRY *) malloc (sizeof (*entry));

	      if (entry == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (*entry));
		  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		  return LC_CLASSNAME_ERROR;
		}

	      entry->name = strdup ((char *) classname);
	      if (entry->name == NULL)
		{
		  free_and_init (entry);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1, strlen (classname));
		  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		  return LC_CLASSNAME_ERROR;
		}

	      entry->tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
	      entry->current.action = LC_CLASSNAME_RESERVED;
	      COPY_OID (&entry->current.oid, class_oid);
	      LSA_SET_NULL (&entry->current.savep_lsa);
	      entry->current.prev = NULL;
	      log_increase_num_transient_classnames (entry->tran_index);
	      (void) mht_put (locator_Mht_classnames, entry->name, entry);
	    }
	  else
	    {
	      /* We can cache this class but don't cache it. */
	      reserve = LC_CLASSNAME_EXIST;
	    }
	}
    }

  /*
   * Note that the index has not been made permanently into the database.
   *      That is, it has not been inserted onto extendible hash.
   */

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  /*
   * Get the lock on the class if we were able to reserve the name
   */
  if (reserve == LC_CLASSNAME_RESERVED && entry != NULL)
    {
      if (lock_object (thread_p, class_oid, oid_Root_class_oid,
		       X_LOCK, LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  /*
	   * Something wrong. Remove the entry from hash table.
	   */
	  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE,
			   INF_WAIT) != NO_ERROR)
	    {
	      return LC_CLASSNAME_ERROR;
	    }
	  if (entry->current.prev == NULL)
	    {
	      log_decrease_num_transient_classnames (entry->tran_index);
	      (void) mht_rem (locator_Mht_classnames, entry->name, NULL,
			      NULL);
	      free_and_init (entry->name);
	      free_and_init (entry);
	    }
	  else
	    {
	      old_action = entry->current.prev;
	      entry->current = *old_action;
	      free_and_init (old_action);
	    }
	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
	  reserve = LC_CLASSNAME_ERROR;
	}
    }

  return reserve;
}

/*
 * xlocator_delete_class_name () - Remove a classname
 *
 * return: LC_FIND_CLASSNAME (either of LC_CLASSNAME_DELETED,
 *                                      LC_CLASSNAME_ERROR)
 *
 *   classname(in): Name of the class to delete
 *
 * Note: Indicate that a classname has been deleted. A transient
 *              classname to OID entry is created in memory to indicate the
 *              deletion. The permanent classname to OID entry is deleted from
 *              permanent classname to OID hash table when the class is
 *              removed from the database.
 *              If there is an entry on the transient/memory classname table,
 *              we can proceed if the entry belongs to the current
 *              transaction, otherwise, we must wait until the transaction
 *              holding the entry terminates since the fate of the classname
 *              entry cannot be predicted. If the transient entry belongs to
 *              the current transaction, we can delete the name only if the
 *              entry indicates that a class with such a name has been
 *              reserved. If there is not a transient entry with such a name
 *              the permanent classname to OID table is consulted and
 *              depending on the existence of an entry, the deleted class is
 *              locked and a transient entry is created informing of the
 *              deletion.
 */
LC_FIND_CLASSNAME
xlocator_delete_class_name (THREAD_ENTRY * thread_p, const char *classname)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  LOCATOR_TMP_CLASSNAME_ACTION *old_action;
  LC_FIND_CLASSNAME classname_delete = LC_CLASSNAME_DELETED;
  EH_SEARCH search;
  OID tmp_classoid;

start:
  classname_delete = LC_CLASSNAME_DELETED;

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return LC_CLASSNAME_ERROR;
    }

  entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						   classname);
  if (entry != NULL && entry->current.action != LC_CLASSNAME_EXIST)
    {
      /*
       * We can only proceed if the entry belongs to the current transaction,
       * otherwise, we must lock the class associated with the classname and
       * retry the operation once the lock is granted.
       */
      if (entry->tran_index == LOG_FIND_THREAD_TRAN_INDEX (thread_p))
	{
	  /*
	   * The name can be deleted only if it has been reserved by current
	   * transaction
	   */
	  if (entry->current.action == LC_CLASSNAME_DELETED
	      || entry->current.action == LC_CLASSNAME_DELETED_RENAME)
	    {
	      classname_delete = LC_CLASSNAME_ERROR;
	      goto error;
	    }

	  /*
	   * The entry can be changed.
	   * Do we need to save the old action...just in case we do a partial
	   * rollback ?
	   */
	  if (!LSA_ISNULL (&entry->current.savep_lsa))
	    {
	      /*
	       * There is a possibility of returning to this top LSA (savepoint).
	       * Save the action.. just in case
	       */
	      old_action =
		(LOCATOR_TMP_CLASSNAME_ACTION *)
		malloc (sizeof (*old_action));
	      if (old_action == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (*old_action));
		  classname_delete = LC_CLASSNAME_ERROR;
		  goto error;
		}
	      *old_action = entry->current;
	      LSA_SET_NULL (&entry->current.savep_lsa);
	      entry->current.prev = old_action;
	    }
	  entry->current.action = LC_CLASSNAME_DELETED;
	}
      else
	{
	  /*
	   * Do not know the fate of this entry until the transaction holding
	   * this entry either commits or aborts. Get the lock and try again.
	   */
	  COPY_OID (&tmp_classoid, &entry->current.oid);

	  /*
	   * Exit from critical section since we are going to be suspended and
	   * then retry again.
	   */

	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

	  if (lock_object
	      (thread_p, &tmp_classoid, oid_Root_class_oid, X_LOCK,
	       LK_UNCOND_LOCK) != LK_GRANTED)
	    {
	      /*
	       * Unable to acquired lock
	       */
	      return LC_CLASSNAME_ERROR;
	    }
	  else
	    {
	      /*
	       * Try again
	       * Remove the lock.. since the above was a dirty read
	       */
	      lock_unlock_object (thread_p, &tmp_classoid, oid_Root_class_oid,
				  X_LOCK, true);
	      goto start;
	    }
	}
    }
  else if (entry != NULL)
    {
      /* There is a class with such a name on the classname cache.
       * We should convert it to transient one.
       */
      entry->tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
      entry->current.action = LC_CLASSNAME_DELETED;
      log_increase_num_transient_classnames (entry->tran_index);
    }
  else
    {
      OID class_oid;
      /*
       * Is there a class with such a name on the permanent classname hash
       * table ?
       */

      /*
       * Now check the permanent classname hash table.
       *
       * It is too dangerous to call the extendible hash table while holding
       * the critical section. We do not know what the extendible hash will
       * do. It may be blocked. Instead, we do the following:
       *
       *    Exit critical section
       *    execute ehash_search
       *    if not found,
       *       enter critical section
       *       double check to make sure that there is not a new entry on
       *              this name. if there is one, return immediately with
       *              classname exist, since I was not the one that add
       *              the entry.
       *       reserver name
       *       exit critical section
       */

      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

      search = ehash_search (thread_p, locator_Eht_classnames,
			     (void *) classname, &class_oid);

      if (search == EH_ERROR_OCCURRED)
	{
	  /*
	   * Some kind of failure. We must notify the error to the caller.
	   */
	  return LC_CLASSNAME_ERROR;
	}
      if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT)
	  != NO_ERROR)
	{
	  return LC_CLASSNAME_ERROR;
	}

      /* Double check */
      entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						       classname);
      if (entry != NULL)
	{
	  if (entry->current.action != LC_CLASSNAME_EXIST)
	    {
	      /* Transient classname by other transaction exists. */
	      classname_delete = LC_CLASSNAME_ERROR;
	      goto error;
	    }

	  entry->tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
	  entry->current.action = LC_CLASSNAME_DELETED;
	  COPY_OID (&entry->current.oid, &class_oid);
	  log_increase_num_transient_classnames (entry->tran_index);
	}
      else
	{
	  entry =
	    (LOCATOR_TMP_CLASSNAME_ENTRY *)
	    malloc (sizeof (LOCATOR_TMP_CLASSNAME_ENTRY));
	  if (entry == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      sizeof (LOCATOR_TMP_CLASSNAME_ENTRY));
	      classname_delete = LC_CLASSNAME_ERROR;
	      goto error;
	    }
	  entry->name = strdup ((char *) classname);
	  if (entry->name == NULL)
	    {
	      free_and_init (entry);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, strlen (classname));
	      classname_delete = LC_CLASSNAME_ERROR;
	      goto error;
	    }
	  entry->tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
	  entry->current.action = LC_CLASSNAME_DELETED;
	  COPY_OID (&entry->current.oid, &class_oid);
	  LSA_SET_NULL (&entry->current.savep_lsa);
	  entry->current.prev = NULL;
	  log_increase_num_transient_classnames (entry->tran_index);
	  (void) mht_put (locator_Mht_classnames, entry->name, entry);
	}
    }

error:
  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  /*
   * We do not need to lock the entry->oid since it has already been locked
   * in exclusive mode when the class was deleted or renamed. Avoid duplicate
   * calls.
   */

  /* Note that the index has not been dropped permanently from the database */
  return classname_delete;
}

/*
 * xlocator_rename_class_name () - Rename a classname
 *
 * return: LC_FIND_CLASSNAME
 *                       (either of LC_CLASSNAME_RESERVED_RENAME,
 *                                  LC_CLASSNAME_EXIST,
 *                                  LC_CLASSNAME_ERROR)
 *
 *   oldname(in): Oldname of class
 *   newname(in): Newname of class
 *   class_oid(in): Object identifier of the class
 *
 * Note: Rename a class in transient form.
 */
LC_FIND_CLASSNAME
xlocator_rename_class_name (THREAD_ENTRY * thread_p, const char *oldname,
			    const char *newname, const OID * class_oid)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  LC_FIND_CLASSNAME renamed;

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return LC_CLASSNAME_ERROR;
    }

  renamed = xlocator_reserve_class_name (thread_p, newname, class_oid);
  entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						   newname);
  if (renamed == LC_CLASSNAME_RESERVED && entry != NULL)
    {
      entry->current.action = LC_CLASSNAME_RESERVED_RENAME;
      renamed = xlocator_delete_class_name (thread_p, oldname);
      entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						       oldname);
      if (renamed == LC_CLASSNAME_DELETED && entry != NULL)
	{
	  entry->current.action = LC_CLASSNAME_DELETED_RENAME;
	  renamed = LC_CLASSNAME_RESERVED_RENAME;
	}
      else
	{
	  entry = ((LOCATOR_TMP_CLASSNAME_ENTRY *)
		   mht_get (locator_Mht_classnames, newname));

	  if (entry == NULL
	      || csect_enter (thread_p, CSECT_CT_OID_TABLE,
			      INF_WAIT) != NO_ERROR)
	    {
	      renamed = LC_CLASSNAME_ERROR;
	      goto error;
	    }
	  (void) locator_drop_class_name_entry (newname, entry, NULL);
	  csect_exit (CSECT_CT_OID_TABLE);
	}
    }

error:

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  return renamed;
}

/*
 * xlocator_find_class_oid () - Find oid of a classname
 *
 * return: LC_FIND_CLASSNAME
 *                       (either of LC_CLASSNAME_EXIST,
 *                                  LC_CLASSNAME_DELETED,
 *                                  LC_CLASSNAME_ERROR)
 *
 *   classname(in): Name of class to find
 *   class_oid(in): Set as a side effect
 *   lock(in): Lock to acquire for the class
 *
 * Note: Find the class identifier of the given class name and lock the
 *              class with the given mode.
 *              If there is an entry on the transient/memory classname table,
 *              we can proceed if the entry belongs to the current
 *              transaction, otherwise, we must wait until the transaction
 *              holding the entry terminates since the fate of the classname
 *              entry cannot be predicted.
 */
LC_FIND_CLASSNAME
xlocator_find_class_oid (THREAD_ENTRY * thread_p, const char *classname,
			 OID * class_oid, LOCK lock)
{
  EH_SEARCH search;
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  LOCK tmp_lock;
  LC_FIND_CLASSNAME find = LC_CLASSNAME_EXIST;

start:
  find = LC_CLASSNAME_EXIST;

  if (csect_enter_as_reader (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE,
			     INF_WAIT) != NO_ERROR)
    {
      return LC_CLASSNAME_ERROR;
    }

  entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						   classname);

  if (entry != NULL)
    {
      /*
       * We can only proceed if the entry belongs to the current transaction,
       * otherwise, we must lock the class associated with the classname and
       * retry the operation once the lock is granted.
       */
      COPY_OID (class_oid, &entry->current.oid);
      if (entry->tran_index == LOG_FIND_THREAD_TRAN_INDEX (thread_p))
	{
	  if (entry->current.action == LC_CLASSNAME_DELETED
	      || entry->current.action == LC_CLASSNAME_DELETED_RENAME)
	    {
	      OID_SET_NULL (class_oid);
	      find = LC_CLASSNAME_DELETED;
	    }
	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
	}
      else if (entry->current.action == LC_CLASSNAME_EXIST)
	{
	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
	}
      else
	{
	  /*
	   * Do not know the fate of this entry until the transaction is
	   * committed or aborted. Get the lock and try again.
	   */
	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

	  if (lock != NULL_LOCK)
	    {
	      tmp_lock = lock;
	    }
	  else
	    {
	      tmp_lock = IS_LOCK;
	    }
	  if (lock_object (thread_p, class_oid, oid_Root_class_oid, tmp_lock,
			   LK_UNCOND_LOCK) != LK_GRANTED)
	    {
	      /*
	       * Unable to acquired lock
	       */
	      OID_SET_NULL (class_oid);
	      find = LC_CLASSNAME_ERROR;
	    }
	  else
	    {
	      /*
	       * Try again
	       * Remove the lock.. since the above was a dirty read
	       */
	      lock_unlock_object (thread_p, class_oid, oid_Root_class_oid,
				  tmp_lock, true);
	      goto start;
	    }
	}
    }
  else
    {
      /*
       * Is there a class with such a name on the permanent classname hash
       * table ?
       */
      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
      search = ehash_search (thread_p, locator_Eht_classnames,
			     (void *) classname, class_oid);
      if (search != EH_KEY_FOUND)
	{
	  if (search == EH_KEY_NOTFOUND)
	    {
	      find = LC_CLASSNAME_DELETED;
	    }
	  else
	    {
	      find = LC_CLASSNAME_ERROR;
	    }
	}
      else
	{
	  if (csect_enter
	      (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE,
	       INF_WAIT) != NO_ERROR)
	    {
	      return LC_CLASSNAME_ERROR;
	    }
	  /* Double check */
	  entry = ((LOCATOR_TMP_CLASSNAME_ENTRY *)
		   mht_get (locator_Mht_classnames, classname));
	  if (entry == NULL)
	    {
	      if ((int) mht_count (locator_Mht_classnames) <
		  MAX_CLASSNAME_CACHE_ENTRIES
		  || locator_decache_class_name_entries () == NO_ERROR)
		{
		  entry =
		    (LOCATOR_TMP_CLASSNAME_ENTRY *) malloc (sizeof (*entry));
		  if (entry == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (*entry));
		      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		      return LC_CLASSNAME_ERROR;

		    }

		  entry->name = strdup ((char *) classname);
		  if (entry->name == NULL)
		    {
		      free_and_init (entry);
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1,
			      strlen (classname));
		      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		      return LC_CLASSNAME_ERROR;
		    }

		  entry->tran_index = NULL_TRAN_INDEX;
		  entry->current.action = LC_CLASSNAME_EXIST;
		  COPY_OID (&entry->current.oid, class_oid);
		  LSA_SET_NULL (&entry->current.savep_lsa);
		  entry->current.prev = NULL;
		  (void) mht_put (locator_Mht_classnames, entry->name, entry);
		}
	    }
	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
	}
    }

  if (lock != NULL_LOCK && find == LC_CLASSNAME_EXIST)
    {
      /* Now acquired the desired lock */
      if (lock_object (thread_p, class_oid, oid_Root_class_oid,
		       lock, LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  /*
	   * Unable to acquired lock
	   */
	  OID_SET_NULL (class_oid);
	  find = LC_CLASSNAME_ERROR;
	}
      else
	{
	  lock_unlock_object (thread_p, class_oid, oid_Root_class_oid, lock,
			      false);
	}
    }

  return find;
}

/*
 * locator_permoid_class_name () - Change reserve name with permanent oid
 *
 * return:
 *
 *   classname(in): Name of class
 *   class_oid(in):  New OID
 *
 * Note: Update the transient entry for the given classname with the
 *              given class identifier. The transient entry must belong to the
 *              current transaction.
 */
static void
locator_permoid_class_name (THREAD_ENTRY * thread_p, const char *classname,
			    OID * class_oid)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  LOCATOR_TMP_CLASSNAME_ACTION *old_action;

  /* Is there any transient entries on the classname hash table ? */
  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      return;
    }
  entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						   classname);

  if (entry != NULL && entry->current.action != LC_CLASSNAME_EXIST
      && entry->tran_index == LOG_FIND_THREAD_TRAN_INDEX (thread_p))
    {
      /*
       * Remove the old lock entry. The new entry has already been acquired by
       * the caller
       */
      lock_unlock_object (thread_p, &entry->current.oid, oid_Root_class_oid,
			  X_LOCK, true);

      /*
       * Do we need to save the old action...just in case we do a partial
       * rollback ?
       */
      if (!LSA_ISNULL (&entry->current.savep_lsa))
	{
	  /*
	   * There is a possibility of returning to this top LSA (savepoint).
	   * Save the action.. just in case
	   */
	  old_action =
	    (LOCATOR_TMP_CLASSNAME_ACTION *) malloc (sizeof (*old_action));
	  if (old_action == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (*old_action));
	      goto error;
	    }
	  *old_action = entry->current;
	  LSA_SET_NULL (&entry->current.savep_lsa);
	  entry->current.prev = old_action;
	}
      COPY_OID (&entry->current.oid, class_oid);
    }

error:
  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
  return;
}

/*
 * locator_drop_transient_class_name_entries () - Drop transient classname entries
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   tran_index(in): Transaction index
 *   savep_lsa(in): up to given LSA
 *
 * Note: Remove all the classname transient entries of the given
 *              transaction up to the given savepoint.
 *              This is done when the transaction terminates and
 *              the permanent hash table has been updated with the correct
 *              entries.
 */
int
locator_drop_transient_class_name_entries (THREAD_ENTRY * thread_p,
					   int tran_index,
					   LOG_LSA * savep_lsa)
{
  LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES rm;
  int error_code = NO_ERROR;

  if (tran_index != NULL_TRAN_INDEX)
    {
      if (log_get_num_transient_classnames (tran_index) <= 0)
	{
	  return error_code;
	}
    }

  rm.tran_index = tran_index;
  rm.savep_lsa = savep_lsa;

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return ER_FAILED;
    }

/* TODO: SystemCatalog: 1st Phase: 2002/06/20: */
  if (csect_enter (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) != NO_ERROR)
    {
      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
      return ER_FAILED;
    }

  error_code = mht_map (locator_Mht_classnames,
			locator_drop_class_name_entry, &rm);
  if (error_code != NO_ERROR)
    {
      error_code = ER_FAILED;
    }

/* TODO: SystemCatalog: 1st Phase: 2002/06/20: */
  csect_exit (CSECT_CT_OID_TABLE);

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  return error_code;
}

/*
 * locator_drop_class_name_entry () - Remove one transient entry
 *
 * return: NO_ERROR or error code
 *
 *   name(in): The classname (key)
 *   ent(in): The entry (data)
 *   rm(in): Structure that indicates what to remove or NULL.
 *
 * Note: Remove transient entry if it belongs to current transaction.
 */
static int
locator_drop_class_name_entry (const void *name, void *ent, void *rm)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) ent;
  LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES *drop;
  LOCATOR_TMP_CLASSNAME_ACTION *old_action;
  char *classname;
  OID class_oid;

  drop = (LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES *) rm;

  COPY_OID (&class_oid, &entry->current.oid);

  classname = entry->name;
  if ((entry->current.action != LC_CLASSNAME_EXIST)
      && (drop == NULL || drop->tran_index == NULL_TRAN_INDEX
	  || drop->tran_index == entry->tran_index))
    {
      if (drop == NULL || drop->savep_lsa == NULL
	  || LSA_ISNULL (drop->savep_lsa))
	{
	  while (entry->current.prev != NULL)
	    {
	      old_action = entry->current.prev;
	      entry->current = *old_action;
	      free_and_init (old_action);
	    }
	  log_decrease_num_transient_classnames (entry->tran_index);
	  (void) mht_rem (locator_Mht_classnames, name, NULL, NULL);

	  (void) catcls_remove_entry (&class_oid);

	  free_and_init (ent);
	  free_and_init (classname);
	}
      else
	{
	  while (LSA_ISNULL (&entry->current.savep_lsa)
		 || LSA_LE (drop->savep_lsa, &entry->current.savep_lsa))
	    {
	      if (entry->current.prev != NULL)
		{
		  old_action = entry->current.prev;
		  entry->current = *old_action;
		  free_and_init (old_action);
		}
	      else
		{
		  log_decrease_num_transient_classnames (entry->tran_index);
		  (void) mht_rem (locator_Mht_classnames, name, NULL, NULL);

		  (void) catcls_remove_entry (&class_oid);

		  free_and_init (ent);
		  free_and_init (classname);
		  break;
		}
	    }
	}
    }

  return NO_ERROR;
}

/*
 * locator_force_drop_class_name_entry () -
 *
 * return:
 *
 *   name(in):
 *   ent(in):
 *   rm(in):
 */
static int
locator_force_drop_class_name_entry (const void *name, void *ent, void *rm)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) ent;
  LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES *drop;
  LOCATOR_TMP_CLASSNAME_ACTION *old_action;
  char *classname;
  OID class_oid;

  drop = (LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES *) rm;

  COPY_OID (&class_oid, &entry->current.oid);

  classname = entry->name;

  while (entry->current.prev != NULL)
    {
      old_action = entry->current.prev;
      entry->current = *old_action;
      free_and_init (old_action);
    }
  (void) mht_rem (locator_Mht_classnames, name, NULL, NULL);

  (void) catcls_remove_entry (&class_oid);

  free_and_init (ent);
  free_and_init (classname);

  return NO_ERROR;
}

/*
 * locator_savepoint_transient_class_name_entries () - Reset savepoint of classname
 *                                               entries
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   tran_index(in): Transaction iendentifier
 *   savep_lsa(in): LSA of a possible rollback point
 *
 * Note: Reset the classname transient entries of the current
 *              transaction with the given LSA address. This is done when a
 *              new savepoint is taken or top system operations is started
 */
int
locator_savepoint_transient_class_name_entries (THREAD_ENTRY * thread_p,
						int tran_index,
						LOG_LSA * savep_lsa)
{
  LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES savep;
  int error_code = NO_ERROR;

  if (tran_index != NULL_TRAN_INDEX)
    {
      if (log_get_num_transient_classnames (tran_index) <= 0)
	{
	  return error_code;
	}
    }

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return ER_FAILED;
    }

  savep.tran_index = tran_index;
  savep.savep_lsa = savep_lsa;

  error_code = mht_map (locator_Mht_classnames,
			locator_savepoint_class_name_entry, &savep);

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  return error_code;
}

/*
 * locator_savepoint_class_name_entry () - Savepoint one transient entry
 *
 * return: NO_ERROR
 *
 *   ignore_name(in):  The classname (key)
 *   ent(in): The entry (data)
 *   sp(in): Structure that indicates what to savepoint
 *
 * Note: Savepoint a transient entry if it belongs to current
 *              transaction and it does not have a savepoint as the last
 *              modified point.
 */
static int
locator_savepoint_class_name_entry (const void *ignore_name, void *ent,
				    void *sp)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) ent;
  LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES *savep;

  savep = (LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES *) sp;

  if (savep->tran_index == entry->tran_index)
    {
      if (LSA_ISNULL (&entry->current.savep_lsa))
	{
	  LSA_COPY (&entry->current.savep_lsa, savep->savep_lsa);
	}
    }

  return NO_ERROR;
}

/*
 * locator_decache_class_name_entries () -
 *
 * return:
 */
static int
locator_decache_class_name_entries (void)
{
  int decache_count = 0;

  /* You are already in the critical section CSECT_LOCATOR_SR_CLASSNAME_TABLE.
   * So you don't need to enter CSECT_LOCATOR_SR_CLASSNAME_TABLE.
   */

  (void) mht_map (locator_Mht_classnames, locator_decache_class_name_entry,
		  &decache_count);

  /* You are in the critical section CSECT_LOCATOR_SR_CLASSNAME_TABLE yet.
   * So you should not exit CSECT_LOCATOR_SR_CLASSNAME_TABLE.
   */

  return NO_ERROR;
}

/*
 * locator_decache_class_name_entry  () -
 *
 * return: NO_ERROR or error code
 *
 *   name(in):
 *   ent(in):
 *   dc(in):
 */
static int
locator_decache_class_name_entry (const void *name, void *ent, void *dc)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) ent;
  int *decache_count;
  LOCATOR_TMP_CLASSNAME_ACTION *old_action;
  char *classname;

  decache_count = (int *) dc;
  classname = entry->name;

  if (entry->current.action == LC_CLASSNAME_EXIST)
    {
      while (entry->current.prev != NULL)
	{
	  old_action = entry->current.prev;
	  entry->current = *old_action;
	  free_and_init (old_action);
	}
      (void) mht_rem (locator_Mht_classnames, name, NULL, NULL);
      free_and_init (ent);
      free_and_init (classname);

      *decache_count += 1;
      if (*decache_count >= MAX_CLASSNAME_CACHE_ENTRIES * 0.1)
	{
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

/*
 * locator_print_class_name () - Print an entry of classname memory hash table
 *
 * return: always return true
 *
 *   outfp(in): FILE stream where to dump the entry
 *   key(in): Classname
 *   ent(in): The entry associated with classname
 *   ignore(in):
 *
 * Note:Print an entry of the classname memory hash table.
 */
static int
locator_print_class_name (FILE * outfp, const void *key, void *ent,
			  void *ignore)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) ent;
  LOCATOR_TMP_CLASSNAME_ACTION *action;
  const char *str_action;

  fprintf (outfp, "Classname = %s, TRAN_INDEX = %d,\n",
	   (char *) key, entry->tran_index);
  action = &entry->current;
  while (action != NULL)
    {
      switch (action->action)
	{
	case LC_CLASSNAME_RESERVED:
	  str_action = "CLASSNAME_RESERVE";
	  break;
	case LC_CLASSNAME_RESERVED_RENAME:
	  str_action = "LC_CLASSNAME_RESERVED_RENAME";
	  break;
	case LC_CLASSNAME_DELETED:
	  str_action = "LC_CLASSNAME_DELETED";
	  break;
	case LC_CLASSNAME_DELETED_RENAME:
	  str_action = "LC_CLASSNAME_DELETED_RENAME";
	  break;
	case LC_CLASSNAME_EXIST:
	  str_action = "LC_CLASSNAME_EXIST";
	  break;
	default:
	  str_action = "UNKNOWN";
	  break;
	}
      fprintf (outfp,
	       "     action = %s, OID = %d|%d|%d, Save_Lsa = %d|%d\n",
	       str_action, action->oid.volid, action->oid.pageid,
	       action->oid.slotid, action->savep_lsa.pageid,
	       action->savep_lsa.offset);
      action = action->prev;
    }

  return (true);
}

/*
 * locator_dump_class_names () - Dump all classname entries
 *
 * return:
 *
 *    out_fp(in): output file
 *
 * Note:Dump all names of classes and their corresponding OIDs.
 *              This function is used for debugging purposes.
 */
void
locator_dump_class_names (THREAD_ENTRY * thread_p, FILE * out_fp)
{
  if (csect_enter_as_reader (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE,
			     INF_WAIT) != NO_ERROR)
    {
      return;
    }
  (void) mht_dump (out_fp, locator_Mht_classnames, false,
		   locator_print_class_name, NULL);
  /* TODO : output file */
  ehash_dump (thread_p, locator_Eht_classnames);

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
}

/*
 * locator_check_class_on_heap () - Check the classname on the heap object
 *
 * return: NO_ERROR continue checking, error code stop checking, bad error
 *
 *   classname(in): The expected class name
 *   classoid(in): The class object identifier
 *   xvalid(in): Could be set as a side effect to either: DISK_INVALID,
 *                 DISK_ERROR when an inconsistency is found. Otherwise, it is
 *                 left in touch. The caller should initialize it to DISK_VALID
 *
 * Note: Check if the classname of the class associated with classoid
 *              is the same as the given class name.
 *              If class does not exist, or its name is different from the
 *              given one, xvalid is set to DISK_INVALID. In the case of other
 *              kind of error, xvalid is set to DISK_ERROR.
 *              If xvalid is set to DISK_ERROR, we return false to stop
 *              the map hash, otheriwse, we return true to continue.
 */
static int
locator_check_class_on_heap (THREAD_ENTRY * thread_p, void *classname,
			     void *classoid, void *xvalid)
{
  DISK_ISVALID *isvalid = (DISK_ISVALID *) xvalid;
  const char *class_name;
  char *heap_class_name;
  OID *class_oid;

  class_name = (char *) classname;
  class_oid = (OID *) classoid;

  heap_class_name =
    heap_get_class_name_alloc_if_diff (thread_p, class_oid,
				       (char *) classname);
  if (heap_class_name == NULL)
    {
      if (er_errid () == ER_HEAP_UNKNOWN_OBJECT)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LC_INCONSISTENT_CLASSNAME_TYPE4, 4,
		  class_name, class_oid->volid, class_oid->pageid,
		  class_oid->slotid);
	  *isvalid = DISK_INVALID;
	}
      else
	{
	  *isvalid = DISK_ERROR;
	}

      goto error;
    }
  /*
   * Compare the classname pointers. If the same pointers classes are the
   * same since the class was no malloc
   */
  if (heap_class_name != classname)
    {
      /*
       * Different names
       */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LC_INCONSISTENT_CLASSNAME_TYPE1, 5,
	      class_oid->volid, class_oid->pageid, class_oid->slotid,
	      class_name, heap_class_name);
      *isvalid = DISK_INVALID;
      free_and_init (heap_class_name);
    }

error:
  if (*isvalid == DISK_ERROR)
    {
      return ER_FAILED;
    }
  else
    {
      return NO_ERROR;
    }
}

/*
 * locator_check_class_names () - Check classname consistency
 *
 * return: DISK_ISVALID
 *
 * Note: Check the consistency of the classname_to_oid and the heap of
 *              classes and vice versa.
 */
DISK_ISVALID
locator_check_class_names (THREAD_ENTRY * thread_p)
{
  DISK_ISVALID isvalid;
  RECDES peek;			/* Record descriptor for peeking object */
  HFID *root_hfid;
  OID class_oid;
  char *class_name = NULL;
  OID class_oid2;
  HEAP_SCANCACHE scan_cache;
  EH_SEARCH search;

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return DISK_ERROR;
    }

  /*
   * CHECK 1: Each class that is found by scanning the heap of classes, must
   *          be part of the permanent classname_to_oid table.
   */

  /* Find the heap for the classes */

  /*
   * Find every single class
   */

  root_hfid = boot_find_root_heap ();
  if (root_hfid == NULL)
    {
      goto error;
    }
  if (heap_scancache_start (thread_p, &scan_cache, root_hfid,
			    oid_Root_class_oid, true, false,
			    LOCKHINT_NONE) != NO_ERROR)
    {
      goto error;
    }

  class_oid.volid = root_hfid->vfid.volid;
  class_oid.pageid = NULL_PAGEID;
  class_oid.slotid = NULL_SLOTID;

  isvalid = DISK_VALID;
  while (heap_next (thread_p, root_hfid, oid_Root_class_oid, &class_oid,
		    &peek, &scan_cache, PEEK) == S_SUCCESS)
    {
      class_name = or_class_name (&peek);
      /*
       * Make sure that this class exists in classname_to_OID table and that
       * the OIDS matches
       */
      search = ehash_search (thread_p, locator_Eht_classnames,
			     (void *) class_name, &class_oid2);
      if (search != EH_KEY_FOUND)
	{
	  if (search == EH_ERROR_OCCURRED)
	    {
	      isvalid = DISK_ERROR;
	      break;
	    }
	  else
	    {
	      isvalid = DISK_INVALID;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LC_INCONSISTENT_CLASSNAME_TYPE3, 4,
		      class_name, class_oid.volid, class_oid.pageid,
		      class_oid.slotid);
	    }
	}
      else
	{
	  /* Are OIDs the same ? */
	  if (!OID_EQ (&class_oid, &class_oid2))
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LC_INCONSISTENT_CLASSNAME_TYPE2, 7,
		      class_name, class_oid2.volid, class_oid2.pageid,
		      class_oid2.slotid, class_oid.volid, class_oid.pageid,
		      class_oid.slotid);
	      isvalid = DISK_INVALID;
	    }
	}
    }

  /* End the scan cursor */
  if (heap_scancache_end (thread_p, &scan_cache) != NO_ERROR)
    {
      isvalid = DISK_ERROR;
    }

  /*
   * CHECK 2: Same that check1 but from classname_to_OID to existance of class
   */

  if (ehash_map (thread_p, locator_Eht_classnames,
		 locator_check_class_on_heap, &isvalid) != NO_ERROR)
    {
      isvalid = DISK_ERROR;
    }

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  return isvalid;

error:
  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  return DISK_ERROR;
}

/*
 * Functions related to fetching and flushing
 */

/*
 * xlocator_assign_oid () - Assign a permanent oid
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap where the object will be stored
 *   perm_oid(in/out): Object identifier.. (set as a side effect)
 *   expected_length(in): Expected length of the object
 *   class_oid(in): The class of the instance
 *   classname(in): Optional... classname for classes
 *
 * Note: A permanent oid is assigned, the object associated with that
 *              OID is locked through the new OID. If the object is a class
 *              the transient classname to OID entry is updated to reflect the
 *              newly assigned OID.
 */
int
xlocator_assign_oid (THREAD_ENTRY * thread_p, const HFID * hfid,
		     OID * perm_oid, int expected_length, OID * class_oid,
		     const char *classname)
{
  if (heap_assign_address_with_class_oid (thread_p, hfid, perm_oid,
					  expected_length,
					  class_oid) != NO_ERROR)
    {
      return ER_FAILED;
    }

  if (classname != NULL)
    {
      locator_permoid_class_name (thread_p, classname, perm_oid);
    }

  /* Release the lock which was set in heap_assign_address_with_class_oid
     according to isolation level */
  lock_unlock_object (thread_p, perm_oid, class_oid, X_LOCK, false);

  return NO_ERROR;
}

/*
 * locator_find_lockset_missing_class_oids () - Find missing classoids
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   lockset(in): Request for finding mising classes
 *
 * Note: Find missing classoids in requested area.
 *              The function does not quit when an error is found if the value
 *              of lockset->quit_on_errors is false. In this case the
 *              object with the error is set to a NULL_OID. For example, when
 *              a class of an object does not exist.
 * Note: There must be enough space in the lockset area to define all
 *              missing classes.
 */
static int
locator_find_lockset_missing_class_oids (THREAD_ENTRY * thread_p,
					 LC_LOCKSET * lockset)
{
  LC_LOCKSET_REQOBJ *reqobjs;	/* Description of one instance to fetch */
  LC_LOCKSET_CLASSOF *reqclasses;	/* Description of one class of a
					 * requested object */
  OID class_oid;		/* Uses to hold the class_oid when
				 * it is unknown */
  int i, j;
  int error_code = NO_ERROR;

  /* Locate array of objects and array of classes */

  reqobjs = lockset->objects;
  reqclasses = lockset->classes;

#if defined(CUBRID_DEBUG)
  i = (sizeof (*lockset)
       + (lockset->num_reqobjs * (sizeof (*reqclasses) + sizeof (*reqobjs))));

  if (lockset->length < i
      || lockset->classes
      != ((LC_LOCKSET_CLASSOF *) (lockset->mem + sizeof (*lockset)))
      || lockset->objects
      < ((LC_LOCKSET_REQOBJ *) (lockset->classes + lockset->num_reqobjs)))
    {
      er_log_debug (ARG_FILE_LINE,
		    "locator_find_lockset_missing_class_oids: "
		    " *** SYSTEM ERROR. Requesting area is incorrect,\n"
		    " either area is too small %d (expect at least %d),\n"
		    " pointer to classes %p (expected %p), or\n"
		    " pointer to objects %p (expected >= %p) are incorrect\n",
		    lockset->length, i, lockset->classes,
		    ((LC_LOCKSET_CLASSOF *) (lockset->mem +
					     sizeof (*lockset))),
		    lockset->objects,
		    ((LC_LOCKSET_REQOBJ *) (lockset->classes +
					    lockset->num_reqobjs)));
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);

      error_code = ER_GENERIC_ERROR;
      goto error;
    }
#endif /* CUBRID_DEBUG */


  /*
   * All class identifiers of requested objects must be known. Find the ones
   * that the caller is unaware
   */

  for (i = 0; i < lockset->num_reqobjs; i++)
    {
      if (reqobjs[i].class_index != -1 || OID_ISNULL (&reqobjs[i].oid))
	{
	  continue;
	}
      /*
       * Caller does not know the class identifier of the requested object.
       * Get the class identifier from disk
       */
      if (heap_get_class_oid (thread_p, &reqobjs[i].oid, &class_oid) == NULL)
	{
	  /*
	   * Unable to find the class of the object. Remove the object from
	   * the list of requested objects.
	   */
	  OID_SET_NULL (&reqobjs[i].oid);
	  if (lockset->quit_on_errors != false)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }
	  continue;
	}

      /*
       * Insert this class in the list of classes of requested objects.
       * Make sure that the class is not already present in the list.
       */

      for (j = 0; j < lockset->num_classes_of_reqobjs; j++)
	{
	  if (OID_EQ (&class_oid, &reqclasses[j].oid))
	    {
	      /* OID is already in the list */
	      reqobjs[i].class_index = j;
	      break;
	    }
	}
      if (j >= lockset->num_classes_of_reqobjs)
	{
	  /* OID is not in the list */
	  COPY_OID (&reqclasses[lockset->num_classes_of_reqobjs].oid,
		    &class_oid);
	  reqclasses[lockset->num_classes_of_reqobjs].chn =
	    CHN_UNKNOWN_ATCLIENT;
	  reqobjs[i].class_index = lockset->num_classes_of_reqobjs;
	  lockset->num_classes_of_reqobjs++;
	}
    }

error:
  return error_code;
}

static SCAN_CODE
locator_return_object_assign (THREAD_ENTRY * thread_p,
			      LOCATOR_RETURN_NXOBJ * assign, OID * oid,
			      int chn, int guess_chn, SCAN_CODE scan,
			      int tran_index)
{
  OID class_oid;

  switch (scan)
    {
    case S_SUCCESS:
      /*
       * The cached object was obsolete.
       */
      or_class_oid (&assign->recdes, &class_oid);
      if (OID_IS_ROOTOID (&class_oid))
	{
	  if (tran_index == NULL_TRAN_INDEX)
	    {
	      tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
	    }
	  (void) heap_chnguess_put (thread_p, oid, tran_index,
				    or_chn (&assign->recdes));
	}
      assign->mobjs->num_objs++;

      COPY_OID (&assign->obj->oid, oid);
      assign->obj->has_index = false;
      assign->obj->hfid = NULL_HFID;
      assign->obj->length = assign->recdes.length;
      assign->obj->offset = assign->area_offset;
      assign->obj->operation = LC_FETCH;
      assign->obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (assign->obj);

      assign->recdes.length = DB_ALIGN (assign->recdes.length, MAX_ALIGNMENT);
      assign->area_offset += assign->recdes.length;
      assign->recdes.data += assign->recdes.length;
      assign->recdes.area_size -= (assign->recdes.length +
				   sizeof (*assign->obj));
      break;

    case S_SUCCESS_CHN_UPTODATE:
      /*
       * the cached object was on the right state
       */
      scan = S_SUCCESS;

      if (guess_chn == CHN_UNKNOWN_ATCLIENT)
	{
	  assign->mobjs->num_objs++;

	  /* Indicate to the caller that the object does not exist any
	   * longer */
	  COPY_OID (&assign->obj->oid, oid);
	  assign->obj->has_index = false;
	  assign->obj->hfid = NULL_HFID;
	  assign->obj->length = -chn;
	  assign->obj->offset = -1;
	  assign->obj->operation = LC_FETCH_VERIFY_CHN;
	  assign->obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (assign->obj);
	  assign->recdes.area_size -= sizeof (*assign->obj);
	}
      break;

    case S_DOESNT_EXIST:
      /*
       * The object does not exist
       */
      assign->mobjs->num_objs++;

      /* Indicate to the caller that the object does not exist any longer */
      COPY_OID (&assign->obj->oid, oid);
      assign->obj->has_index = false;
      assign->obj->hfid = NULL_HFID;
      assign->obj->length = -1;
      assign->obj->offset = -1;
      assign->obj->operation = LC_FETCH_DELETED;
      assign->obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (assign->obj);
      assign->recdes.area_size -= sizeof (*assign->obj);

      break;

    default:
      break;

    }

  return scan;
}

/*
 * locator_return_object () - Place an object in communication area
 *
 * return: SCAN_CODE
 *              (Either of S_SUCCESS,
 *                         S_DOESNT_FIT,
 *                         S_DOESNT_EXIST,
 *                         S_ERROR)
 *
 *   assign(in/out): Description for returing the desired object
 *                  (Set as a side effect to next free area)
 *   oid(in): Identifier of the desired object
 *   chn(in): Cache coherence number of desired object in client
 *                  workspace.
 *
 * Note: The desired object is placed in the assigned return area when
 *              the state of the object(chn) in the client workspace is
 *              different from the one on disk. If the object does not fit in
 *              assigned return area, the length of the object is returned as
 *              a negative value in the area recdes length.
 */
static SCAN_CODE
locator_return_object (THREAD_ENTRY * thread_p,
		       LOCATOR_RETURN_NXOBJ * assign, OID * oid, int chn)
{
  SCAN_CODE scan;		/* Scan return value for next operation */
  int guess_chn = chn;
  int tran_index = NULL_TRAN_INDEX;

  /*
   * The next object is placed in the assigned recdes area iff the cached
   * object is obsolete and the object fits in the recdes
   */

  if (chn == CHN_UNKNOWN_ATCLIENT)
    {
      tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
      chn = heap_chnguess_get (thread_p, oid, tran_index);
    }

  scan =
    heap_get (thread_p, oid, &assign->recdes, assign->ptr_scancache, COPY,
	      chn);

  scan = locator_return_object_assign (thread_p, assign, oid, chn, guess_chn,
				       scan, tran_index);

  return scan;
}

/*
 * xlocator_fetch () - Lock and fetch an object, and prefetch some other objects
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   thrd(in):
 *   oid(in): Object identifier of requested object
 *   chn(in): Cache coherence number of object
 *   lock(in): Lock to acquire before the object is fetched
 *   class_oid(in): Class identifier of the object
 *   class_chn(in): Cache coherence number of the class of the object
 *   prefetching(in): true when pretching of neighbors is desired
 *   fetch_area(in/out): Pointer to area where the objects are placed
                         (set to point to fetching area)
 *
 * Note: This function locks and fetches the object associated with the
 *              given oid. The object is only placed in the fetch area when
 *              the state of the object (chn) in the workspace (client) is
 *              different from the one on disk. The class along with several
 *              other neighbors of the object may also be included in the
 *              fetch area. It is up to the caller if the additional objects
 *              are cached. Fetch_area is set to NULL when there is an error
 *              or when there is not a need to send any object back since the
 *              cache coherent numbers were the same as those on disk. The
 *              caller must check the return value of the function to find out
 *              if there was any error.
 *
 *       The returned fetch area should be freed by the caller.
 */
int
xlocator_fetch (THREAD_ENTRY * thread_p, OID * oid, int chn, LOCK lock,
		OID * class_oid, int class_chn, int prefetching,
		LC_COPYAREA ** fetch_area)
{
  OID tmp_oid;			/* Uses to hold the class_oid when
				 * it is not know by the caller */
  LC_COPYAREA_DESC prefetch_des;
  LOCATOR_RETURN_NXOBJ nxobj;	/* Description to return next obj   */
  int copyarea_length;
  SCAN_CODE scan = S_ERROR;
  RECDES recdes;
  LC_COPYAREA_ONEOBJ *first;
  int error_code = NO_ERROR;

  if (class_oid == NULL)
    {
      /* The class_oid is not known by the caller. */
      class_oid = &tmp_oid;
      OID_SET_NULL (class_oid);
    }

  /* Obtain the desired lock */
  if (lock != NULL_LOCK)
    {
      if (OID_ISNULL (class_oid))
	{
	  /*
	   * Caller does not know the class of the object. Get the class
	   * identifer from disk
	   */
	  if (heap_get_class_oid (thread_p, oid, class_oid) == NULL)
	    {
	      /* Unable to find the class of the object.. return */
	      *fetch_area = NULL;
	      return ER_FAILED;
	    }

	  /*
	   * Since the client (caller) did not know the class identifier of the
	   * instance, make sure that a bad lock is not assigned to an instance.
	   * An instance cannot have an intention lock
	   */

	  if (!OID_IS_ROOTOID (class_oid))
	    {
	      /*
	       * AN INSTANCE
	       */
	      switch (lock)
		{
		case IS_LOCK:
		  if (logtb_find_isolation
		      (LOG_FIND_THREAD_TRAN_INDEX (thread_p)) ==
		      TRAN_SERIALIZABLE)
		    {
		      lock = S_LOCK;
		    }
		  else
		    {
		      lock = NS_LOCK;
		    }
		  break;

		case IX_LOCK:
		case SIX_LOCK:
		  lock = X_LOCK;
		  break;

		default:
		  break;
		}
	    }
	}

      if (lock_object (thread_p, oid, class_oid, lock, LK_UNCOND_LOCK) !=
	  LK_GRANTED)
	{
	  /*
	   * Unable to acquired lock
	   */
	  *fetch_area = NULL;
	  return ER_FAILED;
	}
    }

  /*
   * Fetch the object and its class
   */

  error_code = NO_ERROR;

  /* Assume that the needed object can fit in one page */
  copyarea_length = DB_PAGESIZE;

  while (true)
    {
      nxobj.comm_area = *fetch_area =
	locator_allocate_copy_area_by_length (copyarea_length, NO_CLEAR_MEM);
      if (nxobj.comm_area == NULL)
	{
	  nxobj.mobjs = NULL;
	  error_code = ER_FAILED;
	  goto error;
	}

      nxobj.ptr_scancache = NULL;
      nxobj.mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (nxobj.comm_area);
      nxobj.obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (nxobj.mobjs);
      LC_RECDES_IN_COPYAREA (nxobj.comm_area, &nxobj.recdes);
      nxobj.area_offset = 0;
      nxobj.mobjs->num_objs = 0;

      /* Get the interested object first */

      scan = locator_return_object (thread_p, &nxobj, oid, chn);
      if (scan == S_SUCCESS)
	{
	  break;
	}
      /* Get the real length of current fetch/copy area */

      copyarea_length = nxobj.comm_area->length;
      locator_free_copy_area (nxobj.comm_area);

      /*
       * If the object does not fit even when the copy area seems to be
       * large enough, increase the copy area by at least one page size.
       */

      if (scan != S_DOESNT_FIT)
	{
	  nxobj.comm_area = *fetch_area = NULL;
	  error_code = ER_FAILED;
	  goto error;
	}
      if ((-nxobj.recdes.length) > copyarea_length)
	{
	  copyarea_length = ((-nxobj.recdes.length) + sizeof (*nxobj.mobjs));
	}
      else
	{
	  copyarea_length += DB_PAGESIZE;
	}
    }

  if (class_oid != NULL && OID_ISNULL (class_oid))
    {
      /*
       * The caller does not know the identifier of the class. Get it
       */

      first = LC_START_ONEOBJ_PTR_IN_COPYAREA (nxobj.mobjs);
      LC_RECDES_TO_GET_ONEOBJ (nxobj.comm_area, first, &recdes);
      or_class_oid (&recdes, class_oid);
      class_chn = CHN_UNKNOWN_ATCLIENT;
    }

  /*
   * Then, get the interested class, if given class coherency number is not
   * current.
   */

  if (class_oid != NULL)
    {
      scan = locator_return_object (thread_p, &nxobj, class_oid, class_chn);
      if (scan == S_SUCCESS && nxobj.mobjs->num_objs == 2)
	{
	  LC_COPYAREA_ONEOBJ *first, *second;
	  LC_COPYAREA_ONEOBJ save;
	  /*
	   * It is better if the class is cached first, so swap the
	   * description. The object was stored first because it has
	   * priority of retrieval, however, if both the object and its
	   * class fits, the class should go first for performance reasons
	   */

	  /* Save the object information, then move the class information */
	  first = LC_START_ONEOBJ_PTR_IN_COPYAREA (nxobj.mobjs);
	  second = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (first);

	  /* Swap the values */
	  save = *first;
	  *first = *second;
	  *second = save;
	}
    }

  prefetch_des.mobjs = nxobj.mobjs;
  prefetch_des.obj = &nxobj.obj;

  prefetch_des.offset = &nxobj.area_offset;
  prefetch_des.recdes = &nxobj.recdes;

  /*
   * Find any decache notifications and prefetch any neighbors of the
   * instance
   */

  lock_notify_isolation_incons (thread_p, locator_notify_decache,
				&prefetch_des);
  if (prefetching && nxobj.mobjs->num_objs > 0)
    {
      error_code = heap_prefetch (thread_p, oid, class_oid, &prefetch_des);
    }

  if (nxobj.mobjs->num_objs == 0)
    {
      /*
       * Don't need to send anything. The cache coherency numbers were
       * identical. Deallocate the area and return without failure
       */
      locator_free_copy_area (nxobj.comm_area);
      *fetch_area = NULL;
    }

error:
  if (lock != NULL_LOCK)
    {
      lock_unlock_object (thread_p, oid, class_oid, lock, false);
    }

  return error_code;
}

/*
 * xlocator_get_class () - Lock and fetch the class of an instance, and prefetch
 *                    given instance and some other instances of class
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   class_oid(in/out): Class identifier of the object. (It is set as a side
 *                 effect when its initial value is a null OID)
 *   class_chn(in): Cache coherence number of the class of the object
 *   oid(in): Object identifier of the instance of the desired class
 *   lock(in): Lock to acquire before the class is acquired/fetched
 *                 Note that the lock is for the class.
 *   prefetching(in): true when pretching of neighbors is desired
 *   fetch_area(in/out): Pointer to area where the objects are placed (set to
 *                 point to fetching area)
 *
 * Note: This function locks and fetches the class of the given
 *              instance. The class is only placed in a communication copy
 *              area when the state of the class (class_chn) in the workspace
 *              (client) is different from the one on disk. Other neighbors of
 *              the class are included in the copy_area. It is up to the
 *              caller if the additional classes are cached.  Fetch_area is
 *              set to NULL when there is an error or when there is not a need
 *              to send any object back since the cache coherent numbers were
 *              the same as those on disk. The caller must check the return
 *              value of the function to find out if there was any error.
 *
 * Note: The returned fetch area should be freed by the caller.
 */
int
xlocator_get_class (THREAD_ENTRY * thread_p, OID * class_oid, int class_chn,
		    const OID * oid, LOCK lock, int prefetching,
		    LC_COPYAREA ** fetch_area)
{
  int error_code;

  if (OID_ISNULL (class_oid))
    {
      /*
       * Caller does not know the class of the object. Get the class identifer
       * from disk
       */
      if (heap_get_class_oid (thread_p, oid, class_oid) == NULL)
	{
	  /*
	   * Unable to find out the class identifer...
	   */
	  *fetch_area = NULL;
	  return ER_FAILED;
	}
    }

  /* Now acquired the desired lock */
  if (lock != NULL_LOCK)
    {
      /* Now acquired the desired lock */
      if (lock_object
	  (thread_p, class_oid, oid_Root_class_oid, lock,
	   LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  /*
	   * Unable to acquired lock
	   */
	  *fetch_area = NULL;
	  return ER_FAILED;
	}
    }

  /*
   * Now fetch the class, the instance and optinally prefetch some
   * neighbors of the instance
   */

  error_code = xlocator_fetch (NULL, class_oid, class_chn, NULL_LOCK,
			       oid_Root_class_oid, -1, prefetching,
			       fetch_area);

  if (lock != NULL_LOCK)
    {
      lock_unlock_object (thread_p, class_oid, oid_Root_class_oid, lock,
			  false);
    }

  return error_code;
}

/*
 * xlocator_fetch_all () - Fetch all instances of a class
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap file where the instances of the class are placed
 *   lock(in): Lock to acquired (Set as a side effect to NULL_LOCKID)
 *   class_oid(in): Class identifier of the instances to fetch
 *   nobjects(out): Total number of objects to fetch.
 *   nfetched(out): Current number of object fetched.
 *   last_oid(out): Object identifier of last fetched object
 *   fetch_area(in/out): Pointer to area where the objects are placed
 *
 */
int
xlocator_fetch_all (THREAD_ENTRY * thread_p, const HFID * hfid, LOCK * lock,
		    OID * class_oid, int *nobjects, int *nfetched,
		    OID * last_oid, LC_COPYAREA ** fetch_area)
{
  LC_COPYAREA_DESC prefetch_des;	/* Descriptor for decache of
					 * objects related to transaction
					 * isolation level */
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in
				 * area */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area  */
  RECDES recdes;		/* Record descriptor for
				 * insertion */
  int offset;			/* Place to store next object in
				 * area */
  int round_length;		/* Length of object rounded to
				 * integer alignment */
  int copyarea_length;
  OID oid;
  HEAP_SCANCACHE scan_cache;
  SCAN_CODE scan;
  int error_code = NO_ERROR;


  if (OID_ISNULL (last_oid))
    {
      /* FIRST TIME. */

      /* Obtain the desired lock for the class scan */
      if (*lock != NULL_LOCK
	  && lock_object (thread_p, class_oid, oid_Root_class_oid, *lock,
			  LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  /*
	   * Unable to acquired lock
	   */
	  *fetch_area = NULL;
	  *lock = NULL_LOCK;
	  *nobjects = -1;
	  *nfetched = -1;

	  error_code = ER_FAILED;
	  goto error;
	}

      /* Get statistics */
      last_oid->volid = hfid->vfid.volid;
      last_oid->pageid = NULL_PAGEID;
      last_oid->slotid = NULL_SLOTID;
      /* Estimate the number of objects to be fetched */
      *nobjects = heap_estimate_num_objects (thread_p, hfid);
      *nfetched = 0;
      if (*nobjects == -1)
	{
	  if (*lock != NULL_LOCK)
	    {
	      lock_unlock_object (thread_p, class_oid, oid_Root_class_oid,
				  *lock, false);
	    }
	  *fetch_area = NULL;
	  error_code = ER_FAILED;
	  goto error;
	}
    }

  /* Set OID to last fetched object */
  COPY_OID (&oid, last_oid);

  /* Start a scan cursor for getting several classes */
  error_code = heap_scancache_start (thread_p, &scan_cache, hfid, class_oid,
				     true, false, LOCKHINT_NONE);
  if (error_code != NO_ERROR)
    {
      if (*lock != NULL_LOCK)
	{
	  lock_unlock_object (thread_p, class_oid, oid_Root_class_oid, *lock,
			      false);
	}
      *fetch_area = NULL;

      goto error;
    }

  /* Assume that the next object can fit in one page */
  copyarea_length = DB_PAGESIZE;

  while (true)
    {
      *fetch_area = locator_allocate_copy_area_by_length (copyarea_length,
							  CLEAR_MEM);
      if (*fetch_area == NULL)
	{
	  (void) heap_scancache_end (thread_p, &scan_cache);
	  if (*lock != NULL_LOCK)
	    {
	      lock_unlock_object (thread_p, class_oid, oid_Root_class_oid,
				  *lock, false);
	    }

	  error_code = ER_FAILED;
	  goto error;
	}

      mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (*fetch_area);
      LC_RECDES_IN_COPYAREA (*fetch_area, &recdes);
      obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);
      mobjs->num_objs = 0;
      offset = 0;

      while ((scan = heap_next (thread_p, hfid, class_oid, &oid, &recdes,
				&scan_cache, COPY)) == S_SUCCESS)
	{
	  mobjs->num_objs++;
	  COPY_OID (&obj->oid, &oid);
	  obj->has_index = false;
	  obj->hfid = NULL_HFID;
	  obj->length = recdes.length;
	  obj->offset = offset;
	  obj->operation = LC_FETCH;
	  obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);
	  round_length = DB_ALIGN (recdes.length, MAX_ALIGNMENT);
	  offset += round_length;
	  recdes.data += round_length;
	  recdes.area_size -= round_length + sizeof (*obj);
	}

      if (scan != S_DOESNT_FIT || mobjs->num_objs > 0)
	{
	  break;
	}
      /*
       * The first object does not fit into given copy area
       * Get a larger area
       */

      /* Get the real length of current fetch/copy area */
      copyarea_length = (*fetch_area)->length;
      locator_free_copy_area (*fetch_area);

      /*
       * If the object does not fit even when the copy area seems to be
       * large enough, increase the copy area by at least one page size.
       */

      if ((-recdes.length) > copyarea_length)
	{
	  copyarea_length = (-recdes.length) + sizeof (*mobjs);
	}
      else
	{
	  copyarea_length += DB_PAGESIZE;
	}
    }

  if (scan == S_END)
    {
      /*
       * This is the end of the loop. Indicate the caller that no more calls
       * are needed by setting nobjects and nfetched to the same value.
       */
      error_code = heap_scancache_end (thread_p, &scan_cache);
      if (error_code != NO_ERROR)
	{
	  locator_free_copy_area (*fetch_area);
	  *fetch_area = NULL;
	  if (*lock != NULL_LOCK)
	    {
	      lock_unlock_object (thread_p, class_oid, oid_Root_class_oid,
				  *lock, false);
	    }
	  *nobjects = *nfetched = -1;

	  goto error;
	}

      *nfetched += mobjs->num_objs;
      *nobjects = *nfetched;
      OID_SET_NULL (last_oid);
      if (*lock != NULL_LOCK)
	{
	  lock_unlock_object (thread_p, class_oid, oid_Root_class_oid, *lock,
			      false);
	}
    }
  else if (scan == S_ERROR)
    {
      /* There was an error.. */
      (void) heap_scancache_end (thread_p, &scan_cache);

      locator_free_copy_area (*fetch_area);
      *fetch_area = NULL;
      if (*lock != NULL_LOCK)
	{
	  lock_unlock_object (thread_p, class_oid, oid_Root_class_oid, *lock,
			      false);
	}
      *nobjects = *nfetched = -1;

      error_code = ER_FAILED;
      goto error;
    }
  else if (mobjs->num_objs != 0)
    {
      heap_scancache_end_when_scan_will_resume (thread_p, &scan_cache);
      /* Set the last_oid.. and the number of fetched objects */
      obj = LC_PRIOR_ONEOBJ_PTR_IN_COPYAREA (obj);
      COPY_OID (last_oid, &obj->oid);
      *nfetched += mobjs->num_objs;
      /*
       * If the guess on the number of objects to fetch was low, reset the
       * value, so that the caller continue to call us until the end of the
       * scan
       */
      if (*nobjects <= *nfetched)
	{
	  *nobjects = *nfetched + 10;
	}
    }
  else
    {
      error_code = heap_scancache_end (thread_p, &scan_cache);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

  if (*fetch_area != NULL)
    {
      prefetch_des.mobjs = mobjs;
      prefetch_des.obj = &obj;
      prefetch_des.offset = &offset;
      prefetch_des.recdes = &recdes;
      lock_notify_isolation_incons (thread_p, locator_notify_decache,
				    &prefetch_des);
    }

error:
  return error_code;
}

/*
 * xlocator_fetch_lockset () - Lock and fetch many objects
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   lockset(in/out): Request for finding mising classes and the lock requested
 *                  objects (Set as a side effect)
 *   fetch_area(in/out): Pointer to area where the objects are placed
 *
 */
int
xlocator_fetch_lockset (THREAD_ENTRY * thread_p, LC_LOCKSET * lockset,
			LC_COPYAREA ** fetch_area)
{
  LC_COPYAREA_DESC prefetch_des;	/* Descriptor for decache of objects
					 * related to transaction isolation
					 * level */
  LOCATOR_RETURN_NXOBJ nxobj;	/* Description to return next obj   */
  struct lc_lockset_reqobj *reqobjs;	/* Description of requested objects */
  struct lc_lockset_classof *reqclasses;	/* Description of classes of requested
						 * objects. */
  int copyarea_length;
  SCAN_CODE scan = S_SUCCESS;
  int i, j;
  int error_code = NO_ERROR;


  *fetch_area = NULL;

  reqobjs = lockset->objects;
  reqclasses = lockset->classes;

  if (lockset->num_reqobjs_processed == -1)
    {
      /*
       * FIRST CALL.
       * Initialize num of object processed.
       * Make sure that all classes are known and lock the classes and objects
       */
      lockset->num_reqobjs_processed = 0;
      lockset->num_classes_of_reqobjs_processed = 0;

      error_code =
	locator_find_lockset_missing_class_oids (thread_p, lockset);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      /* Obtain the locks */

      if (lockset->reqobj_inst_lock != NULL_LOCK
	  && lock_objects_lock_set (thread_p, lockset) != LK_GRANTED)
	{
	  if (lockset->quit_on_errors != false)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }
	}
    }

  /* Start a scan cursor for getting several classes */
  error_code =
    heap_scancache_start (thread_p, &nxobj.area_scancache, NULL, NULL, true,
			  false, LOCKHINT_NONE);
  if (error_code != NO_ERROR)
    {
      lock_unlock_objects_lock_set (thread_p, lockset);
      goto error;
    }
  nxobj.ptr_scancache = &nxobj.area_scancache;

  /*
   * Assume that there are not any objects larger than one page. If there are
   * the number of pages is fixed later.
   */

  copyarea_length = DB_PAGESIZE;

  nxobj.mobjs = NULL;
  nxobj.comm_area = NULL;

  while (scan == S_SUCCESS
	 && ((lockset->num_classes_of_reqobjs_processed
	      < lockset->num_classes_of_reqobjs)
	     || (lockset->num_reqobjs_processed < lockset->num_reqobjs)))
    {
      nxobj.comm_area = locator_allocate_copy_area_by_length (copyarea_length,
							      CLEAR_MEM);

      if (nxobj.comm_area == NULL)
	{
	  (void) heap_scancache_end (thread_p, &nxobj.area_scancache);
	  lock_unlock_objects_lock_set (thread_p, lockset);
	  error_code = ER_FAILED;
	  goto error;
	}

      nxobj.mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (nxobj.comm_area);
      nxobj.obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (nxobj.mobjs);
      LC_RECDES_IN_COPYAREA (nxobj.comm_area, &nxobj.recdes);
      nxobj.area_offset = 0;
      nxobj.mobjs->num_objs = 0;

      /*
       * CLASSES
       * Place the classes on the communication area, don't place those classes
       * with correct chns.
       */

      for (i = lockset->num_classes_of_reqobjs_processed;
	   scan == S_SUCCESS && i < lockset->num_classes_of_reqobjs; i++)
	{
	  if (OID_ISNULL (&reqclasses[i].oid))
	    {
	      lockset->num_classes_of_reqobjs_processed++;
	      continue;
	    }
	  if (OID_ISTEMP (&reqclasses[i].oid))
	    {
	      lockset->num_classes_of_reqobjs_processed++;
	      continue;
	    }
	  scan = locator_return_object (thread_p, &nxobj, &reqclasses[i].oid,
					reqclasses[i].chn);
	  if (scan == S_SUCCESS)
	    {
	      lockset->num_classes_of_reqobjs_processed++;
	    }
	  else if (scan == S_DOESNT_FIT && nxobj.mobjs->num_objs == 0)
	    {
	      /*
	       * The first object does not fit into given copy area
	       * Get a larger area
	       */

	      /* Get the real length of current fetch/copy area */

	      copyarea_length = nxobj.comm_area->length;

	      /*
	       * If the object does not fit even when the copy area seems
	       * to be large enough, increase the copy area by at least one
	       * page size.
	       */

	      if ((-nxobj.recdes.length) > copyarea_length)
		{
		  copyarea_length = ((-nxobj.recdes.length) +
				     sizeof (*nxobj.mobjs));
		}
	      else
		{
		  copyarea_length += DB_PAGESIZE;
		}

	      locator_free_copy_area (nxobj.comm_area);
	      scan = S_SUCCESS;
	      break;		/* finish the for */
	    }
	  else if (scan != S_DOESNT_FIT
		   && (scan == S_DOESNT_EXIST
		       || lockset->quit_on_errors == false))
	    {
	      OID_SET_NULL (&reqclasses[i].oid);
	      lockset->num_classes_of_reqobjs_processed += 1;
	      scan = S_SUCCESS;
	    }
	  else
	    {
	      break;		/* Quit on errors */
	    }
	}

      if (i >= lockset->num_classes_of_reqobjs)
	{
	  /*
	   * DONE WITH CLASSES... NOW START WITH INSTANCES
	   * Place the instances in the fetching area, don't place those
	   * instances with correct chns or when they have been placed through
	   * the class array
	   */

	  for (i = lockset->num_reqobjs_processed;
	       scan == S_SUCCESS && i < lockset->num_reqobjs; i++)
	    {
	      if (OID_ISNULL (&reqobjs[i].oid)
		  || OID_ISTEMP (&reqobjs[i].oid)
		  || reqobjs[i].class_index == -1
		  || OID_ISNULL (&reqclasses[reqobjs[i].class_index].oid))
		{
		  lockset->num_reqobjs_processed += 1;
		  continue;
		}

	      if (OID_IS_ROOTOID (&reqclasses[reqobjs[i].class_index].oid))
		{
		  /*
		   * The requested object is a class. If this object is a class
		   * of other requested objects, the object has already been
		   * processed in the previous class iteration
		   */
		  for (j = 0; j < lockset->num_classes_of_reqobjs; j++)
		    {
		      if (OID_EQ (&reqobjs[i].oid, &reqclasses[j].oid))
			{
			  /* It has already been processed */
			  lockset->num_reqobjs_processed += 1;
			  break;
			}
		    }
		  if (j < lockset->num_classes_of_reqobjs)
		    {
		      continue;
		    }
		}

	      /* Now return the object */
	      scan = locator_return_object (thread_p, &nxobj, &reqobjs[i].oid,
					    reqobjs[i].chn);
	      if (scan == S_SUCCESS)
		{
		  lockset->num_reqobjs_processed++;
		  continue;
		}

	      if (scan == S_DOESNT_FIT && nxobj.mobjs->num_objs == 0)
		{
		  /*
		   * The first object does not fit into given copy area
		   * Get a larger area
		   */

		  /* Get the real length of current fetch/copy area */

		  copyarea_length = nxobj.comm_area->length;

		  /*
		   * If the object does not fit even when the copy area
		   * seems to be large enough, increase the copy area by at
		   * least one page size.
		   */

		  if ((-nxobj.recdes.length) > copyarea_length)
		    {
		      copyarea_length = ((-nxobj.recdes.length)
					 + sizeof (*nxobj.mobjs));
		    }
		  else
		    {
		      copyarea_length += DB_PAGESIZE;
		    }

		  locator_free_copy_area (nxobj.comm_area);
		  scan = S_SUCCESS;
		  break;	/* finish the for */
		}
	      else if (scan != S_DOESNT_FIT
		       && (scan == S_DOESNT_EXIST
			   || lockset->quit_on_errors == false))
		{
		  OID_SET_NULL (&reqobjs[i].oid);
		  lockset->num_reqobjs_processed += 1;
		  scan = S_SUCCESS;
		}
	    }
	}
    }

  /* End the scan cursor */
  error_code = heap_scancache_end (thread_p, &nxobj.area_scancache);
  if (error_code != NO_ERROR)
    {
      /* There was an error.. */
      if (nxobj.mobjs != NULL)
	{
	  locator_free_copy_area (nxobj.comm_area);
	}
      lock_unlock_objects_lock_set (thread_p, lockset);
      goto error;
    }

  if (scan == S_ERROR)
    {
      /* There was an error.. */
      if (nxobj.mobjs != NULL)
	{
	  locator_free_copy_area (nxobj.comm_area);
	}
      lock_unlock_objects_lock_set (thread_p, lockset);
      error_code = ER_FAILED;
      goto error;
    }
  else if (nxobj.mobjs != NULL && nxobj.mobjs->num_objs == 0)
    {
      locator_free_copy_area (nxobj.comm_area);
    }
  else
    {
      *fetch_area = nxobj.comm_area;
    }

  if (*fetch_area != NULL)
    {
      prefetch_des.mobjs = nxobj.mobjs;
      prefetch_des.obj = &nxobj.obj;
      prefetch_des.offset = &nxobj.area_offset;
      prefetch_des.recdes = &nxobj.recdes;
      lock_notify_isolation_incons (thread_p, locator_notify_decache,
				    &prefetch_des);
    }

  if ((lockset->num_classes_of_reqobjs_processed
       >= lockset->num_classes_of_reqobjs)
      && lockset->num_reqobjs_processed >= lockset->num_reqobjs)
    {
      lock_unlock_objects_lock_set (thread_p, lockset);
    }

error:
  return error_code;
}

/*
 * locator_all_reference_lockset () - Find all objects referenced by given object
 *
 * return: LC_LOCKSET * or NULL (in case of error)
 *
 *   oid(in): The desired object.
 *   prune_level(in): Get references upto this level. If the value is <= 0
 *                     means upto an infonite level (i.e., all references).
 *   inst_lock(in): Indicate this lock in the request area for objects that
 *                     are instances.
 *   class_lock(in): Indicate this lock in the request area for objects that
 *                     are classes.
 *   quit_on_errors(in): Quit when an error is found such as cannot lock all
 *                 nested objects.
 *
 * Note: This function find all direct and indirect references from the
 *              given object upto the given prune level in the nested graph.
 *              The given object is also included as a reference. Thus, the
 *              function can be seen as listing a graph of referenced/nested
 *              objects.
 *
 *        For performance reasons, the search for duplicate oids now uses an
 *        mht (hash table).  This means that we have to copy the oids into a
 *        non-relocatable place (see lc_ht_permoids below) until the entire
 *        graph is known.
 */
static LC_LOCKSET *
locator_all_reference_lockset (THREAD_ENTRY * thread_p,
			       OID * oid, int prune_level,
			       LOCK inst_lock,
			       LOCK class_lock, int quit_on_errors)
{
  OID class_oid;		/* The class_oid of an inst */
  int max_refs, ref_num;	/* Max and reference number in
				 * request area */
  int max_stack;		/* Total size of stack       */
  int stack_actual_size;	/* Actual size of stack      */
  int level;			/* The current listing level */
  int oid_list_size = 0;	/* Oid list size             */
  OID *oid_list = NULL;		/* List of ref for one object */
  LC_LOCKSET *lockset = NULL;	/* Building request for obj. */
  struct lc_lockset_reqobj *reqobjs;	/* Description of one inst   */
  struct lc_lockset_reqobj *to_reqobjs;	/* Description of one inst   */
  struct lc_lockset_classof *reqclasses;	/* Description of one class  */
  int *stack = NULL;		/* The stack for the search  */
  HEAP_SCANCACHE scan_cache;	/* Scan cache used for fetching
				 * purposes */
  SCAN_CODE scan;		/* Scan return value for an object */
  RECDES peek_recdes;
  void *new_ptr;
  int i, tmp_ref_num, number;
  MHT_TABLE *lc_ht_permoids = NULL;	/* Hash table of already found oids */
  HL_HEAPID heap_id = HL_NULL_HEAPID;	/* Id of Heap allocator */

  struct ht_obj_info
  {
    OID oid;
    int ref_num;
  };				/* info stored into hash table */
  struct ht_obj_info *ht_obj;

  /* Make sure that the object exists ? */
  if (heap_does_exist (thread_p, oid, NULL) != true)
    {
      if (er_errid () != ER_INTERRUPTED)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT, 3,
		  oid->volid, oid->pageid, oid->slotid);
	}

      goto error;
    }

  /* Let's assume a number of references for allocation start purposes */
  max_refs = max_stack = LOCATOR_GUESS_NUM_NESTED_REFERENCES;

  lockset = locator_allocate_lockset (max_refs, inst_lock, class_lock,
				      quit_on_errors);
  if (lockset == NULL)
    {
      goto error;
    }

  stack = (int *) malloc (sizeof (*stack) * max_stack);
  if (stack == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (*stack) * max_stack);
      goto error;
    }

  /* Use a hash table to speed up lockset verification when looking for
   * cycles in the graph */
  lc_ht_permoids = mht_create ("Memory hash lc_allrefs",
			       LOCATOR_GUESS_HT_SIZE, oid_hash,
			       oid_compare_equals);
  if (lc_ht_permoids == NULL)
    {
      goto error;
    }

  /* Use a chunky memory manager, for fewer mallocs of small stuff */
  heap_id = db_create_fixed_heap (sizeof (struct ht_obj_info),
				  LOCATOR_GUESS_HT_SIZE);
  if (heap_id == HL_NULL_HEAPID)
    {
      goto error;
    }

  /* Initialize the stack */
  stack_actual_size = 0;
  level = 0;

  reqobjs = lockset->objects;
  reqclasses = lockset->classes;

  /*
   * Add first object to the stack and request structure.
   * Indicate that the object is only on the stack. That is, the object has
   * not been visited.
   * The cache coherence number is used to hold the level of the object in
   * the nested graph/tree
   */

  COPY_OID (&reqobjs->oid, oid);
  reqobjs->class_index = -1;
  reqobjs->chn = level;
  stack[stack_actual_size++] = lockset->num_reqobjs++;	/* Push */
  reqobjs++;

  /*
   * Start a kind of depth-first search algorithm to find out all references
   * until the prune level is reached
   */

  /* Start a scan cursor for getting several classes */
  if (heap_scancache_start (thread_p, &scan_cache, NULL, NULL, true,
			    false, LOCKHINT_NONE) != NO_ERROR)
    {
      goto error;
    }

  while (stack_actual_size > 0)
    {
      ref_num = stack[--stack_actual_size];	/* Pop */
      /* Get the object to find out its direct references */
      scan = heap_get (thread_p, &lockset->objects[ref_num].oid, &peek_recdes,
		       &scan_cache, PEEK, NULL_CHN);
      if (scan != S_SUCCESS)
	{
	  if (scan != S_DOESNT_EXIST
	      && (quit_on_errors == true || er_errid () == ER_INTERRUPTED))
	    {
	      (void) heap_scancache_end (thread_p, &scan_cache);
	      goto error;
	    }

	  /* Remove the object from the list of requested objects */
	  if (ref_num == lockset->num_reqobjs - 1)
	    {
	      /* Last element remove it */
	      lockset->num_reqobjs--;
	      reqobjs--;
	    }
	  else
	    {
	      /* Marked it as invalid */
	      OID_SET_NULL (&lockset->objects[ref_num].oid);
	    }
	  er_clear ();
	  continue;
	}

      /*
       * has the object been visited ?
       */

      or_class_oid (&peek_recdes, &class_oid);

      if (lockset->objects[ref_num].class_index == -1)
	{
	  /*
	   * Object has never been visited. First time in the stack.
	   * Find its class and marked as listed in the lockset structure
	   */

	  /* Is this class already stored ? */

	  for (i = 0; i < lockset->num_classes_of_reqobjs; i++)
	    {
	      if (OID_EQ (&class_oid, &lockset->classes[i].oid))
		{
		  break;
		}
	    }
	  if (i < lockset->num_classes_of_reqobjs)
	    {
	      /* Class is already in the lockset class list array */
	      lockset->objects[ref_num].class_index = i;
	    }
	  else
	    {
	      /*
	       * Class is not in the lockset class list array.
	       * Make sure that this is a valid class
	       */
	      if (!heap_does_exist (thread_p, &class_oid, oid_Root_class_oid))
		{
		  /* Remove the object from the list of requested objects */
		  if (ref_num == lockset->num_reqobjs - 1)
		    {
		      /* Last element remove it */
		      lockset->num_reqobjs--;
		      reqobjs--;
		    }
		  else
		    {
		      /* Marked it as invalid */
		      OID_SET_NULL (&lockset->objects[ref_num].oid);
		    }
		  continue;
		}
	      COPY_OID (&reqclasses->oid, &class_oid);
	      reqclasses->chn = -1;	/* Note that this is a level */
	      lockset->objects[ref_num].class_index =
		lockset->num_classes_of_reqobjs;
	      lockset->num_classes_of_reqobjs++;
	      reqclasses++;
	    }
	}

      /* Level for the directly referenced objects */

      level = lockset->objects[ref_num].chn + 1;
      if (prune_level >= 0 && level > prune_level)
	{
	  continue;
	}

      /*
       * Find all direct references from the given object
       */
      if (OID_IS_ROOTOID (&class_oid))
	{
	  continue;
	}
      number = heap_get_referenced_by (thread_p,
				       &lockset->objects[ref_num].oid,
				       &peek_recdes, &oid_list_size,
				       &oid_list);
      if (number <= 0)
	{
	  continue;
	}

      /*
       * Add the above references to the stack if these objects have not
       * been alredy visited or if their current level is smaller than their
       * visited level
       */

      if (oid_list == NULL || number <= 0)
	{
	  continue;
	}
      for (i = 0; i < number; i++)
	{
	  if (OID_ISNULL (&oid_list[i]))
	    {
	      continue;
	    }

	  ht_obj =
	    (struct ht_obj_info *) mht_get (lc_ht_permoids, &oid_list[i]);
	  if (ht_obj != NULL)
	    {
	      tmp_ref_num = ht_obj->ref_num;
	      if (lockset->objects[tmp_ref_num].chn > level)
		{
		  /*
		   * Re-visit the object again since some of its
		   * references may have been pruned
		   */
		  lockset->objects[tmp_ref_num].chn = level;
		  /* push */
		  stack[stack_actual_size++] = tmp_ref_num;
		}
	    }
	  else
	    {
	      tmp_ref_num = lockset->num_reqobjs;
	      /*
	       * Push the object onto the stack.
	       * Make sure that we have area in the stack and the
	       * request area
	       */
	      if (stack_actual_size >= max_stack)
		{
		  /* Expand the stack */
		  if (number > LOCATOR_GUESS_NUM_NESTED_REFERENCES)
		    {
		      max_stack += number;
		    }
		  else
		    {
		      max_stack += LOCATOR_GUESS_NUM_NESTED_REFERENCES;
		    }
		  new_ptr = realloc (stack, sizeof (*stack) * max_stack);
		  if (new_ptr == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1,
			      sizeof (*stack) * max_stack);
		      if (quit_on_errors == false)
			{
			  break;
			}
		      (void) heap_scancache_end (thread_p, &scan_cache);
		      goto error;
		    }
		  stack = (int *) new_ptr;
		}

	      if ((lockset->num_reqobjs + 1) > max_refs)
		{
		  if (number > LOCATOR_GUESS_NUM_NESTED_REFERENCES)
		    {
		      max_refs += number;
		    }
		  else
		    {
		      max_refs += LOCATOR_GUESS_NUM_NESTED_REFERENCES;
		    }
		  new_ptr = locator_reallocate_lockset (lockset, max_refs);
		  if (new_ptr == NULL)
		    {
		      if (quit_on_errors == false)
			{
			  break;
			}
		      (void) heap_scancache_end (thread_p, &scan_cache);
		      goto error;
		    }
		  lockset = (LC_LOCKSET *) new_ptr;
		  /* Find the new locations since the structure was
		   * reallocated */
		  reqobjs = lockset->objects + lockset->num_reqobjs;
		  reqclasses =
		    lockset->classes + lockset->num_classes_of_reqobjs;
		}

	      /* Put object in the hash table */
	      ht_obj = (struct ht_obj_info *) db_fixed_alloc (heap_id,
							      sizeof
							      (*ht_obj));
	      if (ht_obj == NULL)
		{
		  if (quit_on_errors == false)
		    {
		      break;
		    }
		  (void) heap_scancache_end (thread_p, &scan_cache);
		  goto error;
		}
	      COPY_OID (&ht_obj->oid, &oid_list[i]);
	      ht_obj->ref_num = tmp_ref_num;

	      if (mht_put (lc_ht_permoids, &ht_obj->oid, ht_obj) != ht_obj)
		{
		  if (quit_on_errors == false)
		    {
		      break;
		    }
		  (void) heap_scancache_end (thread_p, &scan_cache);
		  goto error;
		}

	      /*
	       * Push the object
	       * Indicate that the object is only on the stack. That is,
	       * the object has not been visited.
	       * The cache coherence number is used to hold the level of
	       * the object in the nested graph/tree
	       */
	      COPY_OID (&reqobjs->oid, &oid_list[i]);
	      reqobjs->class_index = -1;
	      reqobjs->chn = level;
	      /* Push */
	      stack[stack_actual_size++] = lockset->num_reqobjs++;
	      reqobjs++;
	    }
	}
    }

  /* Cleanup */
  if (oid_list != NULL)
    {
      free_and_init (oid_list);
    }
  free_and_init (stack);
  (void) heap_scancache_end (thread_p, &scan_cache);
  db_destroy_fixed_heap (heap_id);
  mht_destroy (lc_ht_permoids);

  /*
   * Set the cache coherence numbers as unknown (these are the ones of the
   * client workspace) and compact the array of requested objects. Note that
   * before we have used the chn as the level, so it needs to be reset.
   */

  number = 0;
  to_reqobjs = reqobjs = lockset->objects;
  for (i = 0; i < lockset->num_reqobjs; i++)
    {
      if (!OID_ISNULL (&reqobjs->oid))
	{
	  /* Move it to */
	  if (to_reqobjs != reqobjs)
	    {
	      memcpy (to_reqobjs, reqobjs, sizeof (*reqobjs));
	    }
	  to_reqobjs->chn = NULL_CHN;
	  to_reqobjs++;
	}
      else
	{
	  number++;
	}
      reqobjs++;
    }
  lockset->num_reqobjs -= number;

  for (i = 0; i < lockset->num_classes_of_reqobjs; i++)
    {
      lockset->classes[i].chn = CHN_UNKNOWN_ATCLIENT;
    }

  return lockset;

error:
  if (oid_list != NULL)
    {
      free_and_init (oid_list);
    }
  if (lc_ht_permoids != NULL)
    {
      mht_destroy (lc_ht_permoids);
      lc_ht_permoids = NULL;
    }
  if (stack != NULL)
    {
      free_and_init (stack);
    }
  if (lockset != NULL)
    {
      locator_free_lockset (lockset);
      lockset = NULL;
    }
  if (heap_id != HL_NULL_HEAPID)
    {
      db_destroy_fixed_heap (heap_id);
    }

  return NULL;
}

/*
 * xlocator_fetch_all_reference_lockset () - Lock and fetch the requested objects and its
 *                                direct and indirect references
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   oid(in): The desired object in the root of nested references
 *   chn(in): Cache coherence number of desired object
 *   class_oid(in): Class identifier of the desired object
 *   class_chn(in): Cache coherence number of the class of the desired object
 *   lock(in): Lock to acquire on the desired object and its references
 *   quit_on_errors(in): Wheater to continue in case an error, such as an
 *                 object can be locked
 *   prune_level(in): Get references upto this level. If the value is <= 0
 *                 means upto an infonite level (i.e., all references).
 *   lockset(in/out): Request for finding the all references. This is set to
 *                 NULL when the references are unknown.
 *   fetch_area(in/out): Pointer to area where the objects are placed (Set as a
 *                 side effect)
 *
 */
int
xlocator_fetch_all_reference_lockset (THREAD_ENTRY * thread_p, OID * oid,
				      int chn, OID * class_oid,
				      int class_chn, LOCK lock,
				      int quit_on_errors, int prune_level,
				      LC_LOCKSET ** lockset,
				      LC_COPYAREA ** fetch_area)
{
  int i;
  LOCK instance_lock;

  /* Find all the references */
  if (lock <= S_LOCK)
    {
      instance_lock = IS_LOCK;
    }
  else
    {
      instance_lock = IX_LOCK;
    }
  *lockset = locator_all_reference_lockset (thread_p, oid, prune_level, lock,
					    instance_lock, quit_on_errors);
  if (*lockset == NULL)
    {
      return ER_FAILED;
    }

  /*
   * Set the known cache coherence numbers of the desired object and its
   * class
   */

  if (chn != NULL_CHN)
    {
      for (i = 0; i < (*lockset)->num_reqobjs; i++)
	{
	  if (OID_EQ (oid, &(*lockset)->objects[i].oid))
	    {
	      (*lockset)->objects[i].chn = chn;
	      break;
	    }
	}
    }

  if (class_oid != NULL && class_chn != NULL_CHN)
    {
      for (i = 0; i < (*lockset)->num_classes_of_reqobjs; i++)
	{
	  if (OID_EQ (class_oid, &(*lockset)->classes[i].oid))
	    {
	      (*lockset)->classes[i].chn = class_chn;
	      break;
	    }
	}
    }

  /* Get the first batch of classes and objects */
  return xlocator_fetch_lockset (thread_p, *lockset, fetch_area);
}

/*
 * xlocator_does_exist () - Does object exist? if it does prefetch it
 *
 * return: Either of (LC_EXIST, LC_DOESNOT_EXIST, LC_ERROR)
 *
 *   oid(in): Object identifier of desired object
 *   chn(in): Cache coherence number of object
 *   lock(in): Lock to acquire for the object
 *   class_oid(in): Class identifier of the object
 *   class_chn(in): Cache coherence number of the class of the object
 *   need_fetching(in):
 *   prefetching(in): true if prefetching of some of the object neighbors is
 *                 desired.
 *   fetch_area(in/out):  Pointer to area where the objects are placed
                   (set to point to fetching area)
 *
 * Note:This function checks if the desired object exist. An error is
 *              not set if the object does not exist. If the object exists and
 *              prefetching is desired, prefetching is done for the object and
 *              some of its neighbors.
 */
int
xlocator_does_exist (THREAD_ENTRY * thread_p, OID * oid, int chn, LOCK lock,
		     OID * class_oid, int class_chn, int need_fetching,
		     int prefetching, LC_COPYAREA ** fetch_area)
{
  OID tmp_oid;

  if (need_fetching)
    {
      *fetch_area = NULL;
    }

  if (class_oid == NULL)
    {
      class_oid = &tmp_oid;
      OID_SET_NULL (class_oid);
    }

  if (OID_ISNULL (class_oid))
    {
      /*
       * Caller does not know the class of the object. Get the class identifer
       * from disk
       */
      class_chn = CHN_UNKNOWN_ATCLIENT;
      if (heap_get_class_oid (thread_p, oid, class_oid) == NULL)
	{
	  /* Unable to find the class of the object.. return */
	  return LC_DOESNOT_EXIST;
	}

      /*
       * Since the client (caller) did not know the class identifier of the
       * instance, make sure that a bad lock is not assigned to an instance.
       * An instance cannot have an intention lock
       */

      if (!OID_IS_ROOTOID (class_oid))
	{
	  /*
	   * AN INSTANCE
	   */
	  switch (lock)
	    {
	    case IS_LOCK:

	      if (logtb_find_current_isolation (thread_p) ==
		  TRAN_SERIALIZABLE)
		{
		  lock = S_LOCK;
		}
	      else
		{
		  lock = NS_LOCK;
		}
	      break;

	    case IX_LOCK:
	    case SIX_LOCK:
	      lock = X_LOCK;
	      break;

	    default:
	      break;
	    }
	}
    }

  /* Obtain the desired lock */
  if (lock != NULL_LOCK
      && lock_object (thread_p, oid, class_oid, lock,
		      LK_UNCOND_LOCK) != LK_GRANTED)
    {
      /*
       * Unable to acquired lock
       */
      return LC_ERROR;
    }

  if (heap_does_exist (thread_p, oid, class_oid) != false)
    {
      if (need_fetching)
	{
	  /* The object exist. Prefetch the object if that operation is
	   * desirable */
	  (void) xlocator_fetch (NULL, oid, chn, NULL_LOCK, class_oid,
				 class_chn, prefetching, fetch_area);
	}
      if (lock != NULL_LOCK)
	{
	  lock_unlock_object (thread_p, oid, class_oid, lock, false);
	}
      return LC_EXIST;
    }
  else
    {
      if (lock != NULL_LOCK)
	{
	  lock_unlock_object (thread_p, oid, class_oid, lock, false);
	}
      return LC_DOESNOT_EXIST;
    }
}

/*
 * locator_start_force_scan_cache () -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   scan_cache(in/out):
 *   hfid(in):
 *   class_oid(in):
 *   op_type(in):
 */
int
locator_start_force_scan_cache (THREAD_ENTRY * thread_p,
				HEAP_SCANCACHE * scan_cache,
				const HFID * hfid, const OID * class_oid,
				int op_type)
{
  return heap_scancache_start_modify (thread_p, scan_cache, hfid, class_oid,
				      op_type);
}

/*
 * locator_end_force_scan_cache () -
 *
 * return:
 *
 *   scan_cache(in):
 */
void
locator_end_force_scan_cache (THREAD_ENTRY * thread_p,
			      HEAP_SCANCACHE * scan_cache)
{
  heap_scancache_end_modify (thread_p, scan_cache);
}

/*
 * locator_set_foreign_key_object_cache () -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   class_oid(in):
 *   inst_oid(in):
 *   pk_oid(in):
 *   old_recdes(in):
 *   new_recdes(in):
 *   cache_attr_id(in):
 *   cparea(in):
 */
static int
locator_set_foreign_key_object_cache (THREAD_ENTRY * thread_p,
				      OID * class_oid, OID * inst_oid,
				      OID * pk_oid, RECDES * old_recdes,
				      RECDES * new_recdes,
				      int cache_attr_id,
				      LC_COPYAREA ** cparea)
{
  HEAP_CACHE_ATTRINFO attr_info, *attr_info_p = NULL;
  DB_VALUE val;
  LC_COPYAREA *copyarea;
  int copyarea_length;
  SCAN_CODE scan;
  int error_code = NO_ERROR;

  error_code = heap_attrinfo_start (thread_p, class_oid, -1, NULL,
				    &attr_info);
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  attr_info_p = &attr_info;

  db_make_oid (&val, pk_oid);
  error_code = heap_attrinfo_clear_dbvalues (attr_info_p);
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = heap_attrinfo_set (inst_oid, cache_attr_id, &val, attr_info_p);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  scan = S_DOESNT_FIT;

  /* Assume that the object can fit in one page */
  copyarea_length = DB_PAGESIZE;

  while (scan == S_DOESNT_FIT)
    {
      copyarea = locator_allocate_copy_area_by_length (copyarea_length,
						       CLEAR_MEM);
      if (copyarea == NULL)
	{
	  break;
	}

      new_recdes->data = copyarea->mem;
      new_recdes->area_size = copyarea->length;

      scan = heap_attrinfo_transform_to_disk (thread_p, attr_info_p,
					      old_recdes, new_recdes);
      if (scan == S_SUCCESS)
	{
	  break;
	}

      /* Get the real length used in the copy area */
      copyarea_length = copyarea->length;
      locator_free_copy_area (copyarea);
      copyarea = NULL;

      /* Is more space needed ? */
      if (scan == S_DOESNT_FIT)
	{
	  /*
	   * The object does not fit into copy area, increase the area to
	   * estimated size included in length of record descriptor.
	   */
	  if (copyarea_length < (-new_recdes->length))
	    {
	      copyarea_length = -new_recdes->length;
	    }
	  else
	    {
	      /*
	       * This is done only for security purposes, since the
	       * transformation may not be given us the correct length,
	       * somehow.
	       */
	      copyarea_length += DB_PAGESIZE;
	    }
	}
    }

  *cparea = copyarea;

error:
  if (attr_info_p)
    {
      heap_attrinfo_end (thread_p, attr_info_p);
    }

  return error_code;
}

/*
 * locator_check_foreign_key () -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in):
 *   class_oid(in):
 *   inst_oid(in):
 *   recdes(in):
 *   new_recdes(in):
 *   is_cached(in):
 *   copyarea(in):
 */
static int
locator_check_foreign_key (THREAD_ENTRY * thread_p, HFID * hfid,
			   OID * class_oid, OID * inst_oid, RECDES * recdes,
			   RECDES * new_recdes, bool * is_cached,
			   LC_COPYAREA ** copyarea)
{
  int num_found, i;
  HEAP_CACHE_ATTRINFO index_attrinfo;
  HEAP_IDX_ELEMENTS_INFO idx_info;
  BTID btid;
  DB_VALUE *key_dbvalue;
  DB_VALUE dbvalue;
  char buf[DBVAL_BUFSIZE + MAX_ALIGNMENT], *aligned_buf;
  OR_INDEX *index;
  OID unique_oid;
  bool is_null;
  int error_code = NO_ERROR;

  aligned_buf = PTR_ALIGN (buf, MAX_ALIGNMENT);

  num_found = heap_attrinfo_start_with_index (thread_p, class_oid, NULL,
					      &index_attrinfo, &idx_info);
  if (num_found <= 0)
    {
      return error_code;
    }

  if (idx_info.has_single_col)
    {
      error_code = heap_attrinfo_read_dbvalues (thread_p, inst_oid, recdes,
						&index_attrinfo);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

  for (i = 0; i < idx_info.num_btids; i++)
    {
      index = &(index_attrinfo.last_classrepr->indexes[i]);
      if (index->type != BTREE_FOREIGN_KEY)
	{
	  continue;
	}

      /* must be updated when key_prefix_length will be added for FK and PK */
      key_dbvalue = heap_attrvalue_get_key (thread_p, i, &index_attrinfo,
					    recdes, &btid, &dbvalue,
					    aligned_buf);
      if (key_dbvalue == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      if (index->n_atts > 1)
	{
	  is_null = btree_multicol_key_is_null (key_dbvalue);
	}
      else
	{
	  is_null = DB_IS_NULL (key_dbvalue);
	}

      if (!is_null)
	{
	  if (xbtree_find_unique (thread_p, &index->fk->ref_class_pk_btid,
				  true, key_dbvalue,
				  &index->fk->ref_class_oid, &unique_oid,
				  true) != BTREE_KEY_FOUND)
	    {
	      if (key_dbvalue == &dbvalue)
		{
		  pr_clear_value (&dbvalue);
		}

	      if (LOG_CHECK_LOG_APPLIER (thread_p))
		{
		  continue;
		}

	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FK_INVALID, 1,
		      index->fk->fkname);

	      error_code = ER_FK_INVALID;
	      goto error;
	    }

	  if (index->fk->is_cache_obj && index->fk->cache_attr_id >= 0)
	    {
	      error_code = locator_set_foreign_key_object_cache (thread_p,
								 class_oid,
								 inst_oid,
								 &unique_oid,
								 recdes,
								 new_recdes,
								 index->fk->
								 cache_attr_id,
								 copyarea);
	      if (error_code != NO_ERROR)
		{
		  if (key_dbvalue == &dbvalue)
		    {
		      pr_clear_value (&dbvalue);
		    }

		  goto error;
		}

	      *is_cached = true;
	    }
	}

      if (key_dbvalue == &dbvalue)
	{
	  pr_clear_value (&dbvalue);
	}
    }

error:
  heap_attrinfo_end (thread_p, &index_attrinfo);
  return error_code;
}

/*
 * locator_check_primary_key_delete () -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   fkref(in):
 *   key(in):
 */
static int
locator_check_primary_key_delete (THREAD_ENTRY * thread_p,
				  OR_INDEX * index, DB_VALUE * key)
{
  OR_FOREIGN_KEY *fkref;
  int oid_cnt, force_count, i;
  RECDES recdes;
  HEAP_SCANCACHE scan_cache;
  HFID hfid;
  OID *oid_buf = NULL;
  int oid_buf_size = ISCAN_OID_BUFFER_SIZE;
  BTREE_SCAN bt_scan;
  INDX_SCAN_ID isid;
  DB_VALUE copied_key;
  bool is_upd_scan_init;
  int error_code = NO_ERROR;
  HEAP_CACHE_ATTRINFO attr_info;
  DB_VALUE null_value;
  ATTR_ID *attr_ids = NULL;
  int num_attrs = 0;
  int k;
  int *keys_prefix_length = NULL;

  db_make_null (&copied_key);
  db_make_null (&null_value);
  heap_attrinfo_start (thread_p, NULL, 0, NULL, &attr_info);

  for (fkref = index->fk; fkref != NULL; fkref = fkref->next)
    {
      if (fkref->del_action == SM_FOREIGN_KEY_RESTRICT
	  || fkref->del_action == SM_FOREIGN_KEY_NO_ACTION)
	{
	  if (!LOG_CHECK_LOG_APPLIER (thread_p)
	      && btree_find_foreign_key (thread_p, &fkref->self_btid, key,
					 &fkref->self_oid) > 0)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FK_RESTRICT, 1,
		      fkref->fkname);
	      error_code = ER_FK_RESTRICT;
	      goto error3;
	    }
	}
      else if (fkref->del_action == SM_FOREIGN_KEY_CASCADE
	       || fkref->del_action == SM_FOREIGN_KEY_SET_NULL)
	{
	  error_code = heap_get_indexinfo_of_btid (thread_p, &fkref->self_oid,
						   &fkref->self_btid, NULL,
						   &num_attrs, &attr_ids,
						   &keys_prefix_length, NULL);
	  if (error_code != NO_ERROR)
	    {
	      goto error3;
	    }
	  assert (num_attrs == index->n_atts);
	  /* We might check for foreign key and schema consistency problems here
	     but we rely on the schema manager to prevent inconsistency;
	     see do_check_fk_constraints() for details */

	  error_code = heap_scancache_quick_start (&scan_cache);
	  if (error_code != NO_ERROR)
	    {
	      goto error3;
	    }
	  if (heap_get (thread_p, &fkref->self_oid, &recdes, &scan_cache,
			PEEK, NULL_CHN) == S_SUCCESS)
	    {
	      orc_class_hfid_from_record (&recdes, &hfid);
	    }
	  error_code = heap_scancache_end (thread_p, &scan_cache);
	  if (error_code != NO_ERROR)
	    {
	      goto error3;
	    }

	  if (oid_buf == NULL)
	    {
	      oid_buf = (OID *) db_private_alloc (thread_p, oid_buf_size);
	      if (oid_buf == NULL)
		{
		  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
		  goto error3;
		}
	    }

	  error_code = heap_scancache_start (thread_p, &isid.scan_cache,
					     &hfid, &fkref->self_oid, true,
					     true, LOCKHINT_NONE);
	  if (error_code != NO_ERROR)
	    {
	      db_private_free_and_init (thread_p, oid_buf);
	      goto error3;
	    }

	  isid.oid_list.oid_cnt = 0;
	  isid.oid_list.oidp = oid_buf;
	  isid.copy_buf = NULL;
	  isid.copy_buf_len = 0;

	  is_upd_scan_init = false;
	  pr_clone_value (key, &copied_key);

	  do
	    {
	      BTREE_INIT_SCAN (&bt_scan);
	      oid_cnt = btree_range_search (thread_p, &fkref->self_btid,
					    false, LOCKHINT_NONE, &bt_scan,
					    key, &copied_key, GE_LE,
					    1, &fkref->self_oid,
					    isid.oid_list.oidp, oid_buf_size,
					    NULL, &isid, true, false);

	      if (oid_cnt < 1)
		{
		  break;
		}

	      if (is_upd_scan_init == false)
		{
		  int op_type = -1;
		  if (fkref->del_action == SM_FOREIGN_KEY_CASCADE)
		    {
		      op_type = SINGLE_ROW_DELETE;
		    }
		  else if (fkref->del_action == SM_FOREIGN_KEY_SET_NULL)
		    {
		      op_type = SINGLE_ROW_UPDATE;
		    }
		  else
		    {
		      assert (false);
		    }
		  error_code = heap_scancache_start_modify (thread_p,
							    &scan_cache,
							    &hfid,
							    &fkref->self_oid,
							    op_type);
		  if (error_code != NO_ERROR)
		    {
		      goto error2;
		    }
		  is_upd_scan_init = true;
		  if (fkref->del_action == SM_FOREIGN_KEY_SET_NULL)
		    {
		      error_code = heap_attrinfo_start (thread_p,
							&fkref->self_oid, -1,
							NULL, &attr_info);
		      if (error_code != NO_ERROR)
			{
			  goto error1;
			}
		    }
		}

	      for (i = 0; i < oid_cnt; i++)
		{
		  OID *const oid_ptr = &(oid_buf[i]);
		  if (lock_object (thread_p, oid_ptr,
				   &fkref->self_oid, X_LOCK,
				   LK_UNCOND_LOCK) != LK_GRANTED)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_FK_NOT_GRANTED_LOCK, 1, fkref->fkname);
		      error_code = ER_FK_NOT_GRANTED_LOCK;
		      goto error1;
		    }

		  if (fkref->del_action == SM_FOREIGN_KEY_CASCADE)
		    {
		      error_code = locator_delete_force (thread_p, &hfid,
							 oid_ptr, true,
							 SINGLE_ROW_DELETE,
							 &scan_cache,
							 &force_count);
		      if (error_code != NO_ERROR)
			{
			  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				  ER_FK_CANT_DELETE_INSTANCE, 1,
				  fkref->fkname);
			  goto error1;
			}
		    }
		  else if (fkref->del_action == SM_FOREIGN_KEY_SET_NULL)
		    {
		      if ((error_code =
			   heap_attrinfo_clear_dbvalues (&attr_info))
			  != NO_ERROR)
			{
			  goto error1;
			}
		      for (k = 0; k < num_attrs; ++k)
			{
			  error_code =
			    heap_attrinfo_set (oid_ptr, attr_ids[k],
					       &null_value, &attr_info);
			  if (error_code != NO_ERROR)
			    {
			      goto error1;
			    }
			}
		      error_code = locator_attribute_info_force (thread_p,
								 &hfid,
								 oid_ptr,
								 0,
								 &attr_info,
								 attr_ids,
								 index->
								 n_atts,
								 LC_FLUSH_UPDATE,
								 SINGLE_ROW_UPDATE,
								 &scan_cache,
								 &force_count,
								 false,
								 REPL_INFO_TYPE_STMT_NORMAL);
		      if (error_code != NO_ERROR)
			{
			  goto error1;
			}
		    }
		  else
		    {
		      assert (false);
		    }
		}
	    }
	  while (!BTREE_END_OF_SCAN (&bt_scan));

	  if (is_upd_scan_init)
	    {
	      heap_scancache_end_modify (thread_p, &scan_cache);
	      if (fkref->del_action == SM_FOREIGN_KEY_SET_NULL)
		{
		  heap_attrinfo_end (thread_p, &attr_info);
		}
	    }

	  btree_scan_clear_key (&bt_scan);
	  pr_clear_value (&copied_key);
	  error_code = heap_scancache_end (thread_p, &isid.scan_cache);
	  if (error_code != NO_ERROR)
	    {
	      if (oid_buf)
		{
		  db_private_free_and_init (thread_p, oid_buf);
		}

	      return NO_ERROR;
	    }
	}
      else
	{
	  assert (false);
	}
    }

  if (oid_buf)
    {
      db_private_free_and_init (thread_p, oid_buf);
    }
  if (attr_ids)
    {
      db_private_free_and_init (thread_p, attr_ids);
    }
  if (keys_prefix_length)
    {
      db_private_free_and_init (thread_p, keys_prefix_length);
    }

  return error_code;

error1:
  heap_scancache_end_modify (thread_p, &scan_cache);

error2:
  btree_scan_clear_key (&bt_scan);
  pr_clear_value (&copied_key);
  (void) heap_scancache_end (thread_p, &isid.scan_cache);

error3:
  if (attr_ids)
    {
      db_private_free_and_init (thread_p, attr_ids);
    }
  if (keys_prefix_length)
    {
      db_private_free_and_init (thread_p, keys_prefix_length);
    }
  heap_attrinfo_end (thread_p, &attr_info);

  if (oid_buf)
    {
      db_private_free_and_init (thread_p, oid_buf);
    }

  return error_code;
}

/*
 * locator_repair_object_cache () -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   index(in):
 *   key(in):
 *   pk_oid(in):
 */
static int
locator_repair_object_cache (THREAD_ENTRY * thread_p, OR_INDEX * index,
			     DB_VALUE * key, OID * pk_oid)
{
  OR_FOREIGN_KEY *fkref;
  RECDES recdes;
  HEAP_SCANCACHE scan_cache, upd_scancache;
  HFID hfid;
  OID *oid_buf = NULL;
  int oid_buf_size = ISCAN_OID_BUFFER_SIZE;
  int oid_cnt, i, force_count;
  HEAP_CACHE_ATTRINFO attr_info;
  DB_VALUE val;
  BTREE_SCAN bt_scan;
  INDX_SCAN_ID isid;
  bool is_upd_scan_init;
  DB_VALUE copied_key;
  int error_code = NO_ERROR;

  db_make_null (&copied_key);

  for (fkref = index->fk; fkref != NULL; fkref = fkref->next)
    {
      if (fkref->cache_attr_id < 0)
	{
	  continue;
	}

      error_code = heap_scancache_quick_start (&scan_cache);
      if (error_code != NO_ERROR)
	{
	  goto error3;
	}
      if (heap_get (thread_p, &fkref->self_oid, &recdes, &scan_cache,
		    PEEK, NULL_CHN) == S_SUCCESS)
	{
	  orc_class_hfid_from_record (&recdes, &hfid);
	}
      error_code = heap_scancache_end (thread_p, &scan_cache);
      if (error_code != NO_ERROR)
	{
	  goto error3;
	}

      if (oid_buf == NULL)
	{
	  oid_buf = (OID *) db_private_alloc (thread_p, oid_buf_size);
	  if (oid_buf == NULL)
	    {
	      error_code = ER_FAILED;
	      goto error3;
	    }
	}

      error_code = heap_scancache_start (thread_p, &isid.scan_cache, &hfid,
					 &fkref->self_oid, true, true,
					 LOCKHINT_NONE);
      if (error_code != NO_ERROR)
	{
	  db_private_free_and_init (thread_p, oid_buf);
	  goto error3;
	}

      BTREE_INIT_SCAN (&bt_scan);
      isid.oid_list.oid_cnt = 0;
      isid.oid_list.oidp = oid_buf;
      isid.copy_buf = NULL;
      isid.copy_buf_len = 0;

      is_upd_scan_init = false;
      pr_clone_value (key, &copied_key);

      do
	{
	  oid_cnt = btree_range_search (thread_p, &fkref->self_btid, false,
					LOCKHINT_NONE, &bt_scan, key,
					&copied_key, GE_LE,
					1,
					&fkref->self_oid, isid.oid_list.oidp,
					oid_buf_size, NULL, &isid, true,
					false);

	  if (oid_cnt < 1)
	    {
	      break;
	    }

	  if (is_upd_scan_init == false)
	    {
	      error_code = heap_scancache_start_modify (thread_p,
							&upd_scancache, &hfid,
							&fkref->self_oid,
							SINGLE_ROW_UPDATE);
	      if (error_code != NO_ERROR)
		{
		  goto error2;
		}

	      error_code = heap_attrinfo_start (thread_p, &fkref->self_oid,
						-1, NULL, &attr_info);
	      if (error_code != NO_ERROR)
		{
		  heap_scancache_end_modify (thread_p, &upd_scancache);
		  goto error2;
		}

	      is_upd_scan_init = true;
	    }

	  for (i = 0; i < oid_cnt; i++)
	    {
	      db_make_oid (&val, pk_oid);
	      error_code = heap_attrinfo_clear_dbvalues (&attr_info);
	      if (error_code != NO_ERROR)
		{
		  goto error1;
		}

	      error_code = heap_attrinfo_set (&(oid_buf[i]),
					      fkref->cache_attr_id, &val,
					      &attr_info);
	      if (error_code != NO_ERROR)
		{
		  goto error1;
		}

	      if (lock_object (thread_p, &(oid_buf[i]), &fkref->self_oid,
			       X_LOCK, LK_UNCOND_LOCK) != LK_GRANTED)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_FK_NOT_GRANTED_LOCK, 1, fkref->fkname);
		  error_code = ER_FK_NOT_GRANTED_LOCK;
		  goto error1;
		}

	      error_code = locator_attribute_info_force (thread_p, &hfid,
							 &(oid_buf[i]),
							 0,
							 &attr_info,
							 &fkref->
							 cache_attr_id, 1,
							 LC_FLUSH_UPDATE,
							 SINGLE_ROW_UPDATE,
							 &upd_scancache,
							 &force_count, true,
							 REPL_INFO_TYPE_STMT_NORMAL);
	      if (error_code != NO_ERROR)
		{
		  goto error1;
		}
	    }
	}
      while (!BTREE_END_OF_SCAN (&bt_scan));

      if (is_upd_scan_init)
	{
	  heap_attrinfo_end (thread_p, &attr_info);
	  heap_scancache_end_modify (thread_p, &upd_scancache);
	}

      btree_scan_clear_key (&bt_scan);
      pr_clear_value (&copied_key);
      error_code = heap_scancache_end (thread_p, &isid.scan_cache);
      if (error_code != NO_ERROR)
	{
	  if (oid_buf)
	    {
	      db_private_free_and_init (thread_p, oid_buf);
	    }

	  goto error3;
	}
    }

  if (oid_buf)
    {
      db_private_free_and_init (thread_p, oid_buf);
    }

  return error_code;

error1:
  heap_attrinfo_end (thread_p, &attr_info);
  heap_scancache_end_modify (thread_p, &upd_scancache);

error2:
  btree_scan_clear_key (&bt_scan);
  pr_clear_value (&copied_key);
  (void) heap_scancache_end (thread_p, &isid.scan_cache);

  if (oid_buf)
    {
      db_private_free_and_init (thread_p, oid_buf);
    }

error3:
  return error_code;
}

/*
 * locator_check_primary_key_update () -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   index(in):
 *   key(in):
 */
static int
locator_check_primary_key_update (THREAD_ENTRY * thread_p,
				  OR_INDEX * index, DB_VALUE * key)
{
  OR_FOREIGN_KEY *fkref;
  int oid_cnt, force_count, i;
  RECDES recdes;
  HEAP_SCANCACHE scan_cache;
  HFID hfid;
  OID *oid_buf = NULL;
  int oid_buf_size = (int) (DB_PAGESIZE * PRM_BT_OID_NBUFFERS);
  BTREE_SCAN bt_scan;
  INDX_SCAN_ID isid;
  DB_VALUE copied_key;
  bool is_upd_scan_init;
  int error_code = NO_ERROR;
  HEAP_CACHE_ATTRINFO attr_info;
  DB_VALUE null_value;
  ATTR_ID *attr_ids = NULL;
  int num_attrs = 0;
  int k;
  int *keys_prefix_length = NULL;

  db_make_null (&copied_key);
  db_make_null (&null_value);
  heap_attrinfo_start (thread_p, NULL, 0, NULL, &attr_info);

  for (fkref = index->fk; fkref != NULL; fkref = fkref->next)
    {
      if (fkref->upd_action == SM_FOREIGN_KEY_RESTRICT
	  || fkref->upd_action == SM_FOREIGN_KEY_NO_ACTION)
	{
	  if (!LOG_CHECK_LOG_APPLIER (thread_p)
	      && btree_find_foreign_key (thread_p, &fkref->self_btid, key,
					 &fkref->self_oid) > 0)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_FK_RESTRICT, 1,
		      fkref->fkname);
	      return ER_FK_RESTRICT;
	    }
	}
      else if (fkref->upd_action == SM_FOREIGN_KEY_CASCADE
	       || fkref->upd_action == SM_FOREIGN_KEY_SET_NULL)
	{
	  error_code = heap_get_indexinfo_of_btid (thread_p, &fkref->self_oid,
						   &fkref->self_btid, NULL,
						   &num_attrs, &attr_ids,
						   &keys_prefix_length, NULL);
	  if (error_code != NO_ERROR)
	    {
	      goto error3;
	    }
	  assert (num_attrs == index->n_atts);
	  /* We might check for foreign key and schema consistency problems here
	     but we rely on the schema manager to prevent inconsistency;
	     see do_check_fk_constraints() for details */

	  error_code = heap_scancache_quick_start (&scan_cache);
	  if (error_code != NO_ERROR)
	    {
	      goto error3;
	    }
	  if (heap_get (thread_p, &fkref->self_oid, &recdes, &scan_cache,
			PEEK, NULL_CHN) == S_SUCCESS)
	    {
	      orc_class_hfid_from_record (&recdes, &hfid);
	    }
	  error_code = heap_scancache_end (thread_p, &scan_cache);
	  if (error_code != NO_ERROR)
	    {
	      goto error3;
	    }

	  if (oid_buf == NULL)
	    {
	      oid_buf = (OID *) db_private_alloc (thread_p, oid_buf_size);
	      if (oid_buf == NULL)
		{
		  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
		  goto error3;
		}
	    }

	  error_code = heap_scancache_start (thread_p, &isid.scan_cache,
					     &hfid, &fkref->self_oid, true,
					     true, LOCKHINT_NONE);
	  if (error_code != NO_ERROR)
	    {
	      db_private_free_and_init (thread_p, oid_buf);
	      goto error3;
	    }

	  isid.oid_list.oid_cnt = 0;
	  isid.oid_list.oidp = oid_buf;
	  isid.copy_buf = NULL;
	  isid.copy_buf_len = 0;

	  is_upd_scan_init = false;
	  pr_clone_value (key, &copied_key);

	  do
	    {
	      BTREE_INIT_SCAN (&bt_scan);
	      oid_cnt = btree_range_search (thread_p, &fkref->self_btid,
					    false, LOCKHINT_NONE, &bt_scan,
					    key, &copied_key, GE_LE,
					    1, &fkref->self_oid,
					    isid.oid_list.oidp, oid_buf_size,
					    NULL, &isid, true, false);

	      if (oid_cnt < 1)
		{
		  break;
		}

	      if (is_upd_scan_init == false)
		{
		  error_code = heap_scancache_start_modify (thread_p,
							    &scan_cache,
							    &hfid,
							    &fkref->self_oid,
							    SINGLE_ROW_UPDATE);
		  if (error_code != NO_ERROR)
		    {
		      goto error2;
		    }
		  is_upd_scan_init = true;
		  error_code =
		    heap_attrinfo_start (thread_p, &fkref->self_oid, -1, NULL,
					 &attr_info);
		  if (error_code != NO_ERROR)
		    {
		      goto error1;
		    }
		}

	      for (i = 0; i < oid_cnt; i++)
		{
		  OID *const oid_ptr = &(oid_buf[i]);
		  if (lock_object (thread_p, oid_ptr,
				   &fkref->self_oid, X_LOCK,
				   LK_UNCOND_LOCK) != LK_GRANTED)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_FK_NOT_GRANTED_LOCK, 1, fkref->fkname);
		      error_code = ER_FK_NOT_GRANTED_LOCK;
		      goto error1;
		    }

		  if ((error_code = heap_attrinfo_clear_dbvalues (&attr_info))
		      != NO_ERROR)
		    {
		      goto error1;
		    }

		  if (fkref->upd_action == SM_FOREIGN_KEY_CASCADE)
		    {
		      /* This is not yet implemented and this code should not
		         be reached. */
		      assert (false);
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_FK_RESTRICT, 1, fkref->fkname);
		      error_code = ER_FK_RESTRICT;
		      goto error1;
		    }
		  else if (fkref->upd_action == SM_FOREIGN_KEY_SET_NULL)
		    {
		      for (k = 0; k < num_attrs; ++k)
			{
			  error_code =
			    heap_attrinfo_set (oid_ptr, attr_ids[k],
					       &null_value, &attr_info);
			  if (error_code != NO_ERROR)
			    {
			      goto error1;
			    }
			}
		    }
		  else
		    {
		      assert (false);
		    }
		  error_code = locator_attribute_info_force (thread_p,
							     &hfid,
							     oid_ptr,
							     0,
							     &attr_info,
							     attr_ids,
							     index->n_atts,
							     LC_FLUSH_UPDATE,
							     SINGLE_ROW_UPDATE,
							     &scan_cache,
							     &force_count,
							     false,
							     REPL_INFO_TYPE_STMT_NORMAL);
		  if (error_code != NO_ERROR)
		    {
		      goto error1;
		    }
		}
	    }
	  while (!BTREE_END_OF_SCAN (&bt_scan));

	  if (is_upd_scan_init)
	    {
	      heap_scancache_end_modify (thread_p, &scan_cache);
	      heap_attrinfo_end (thread_p, &attr_info);
	    }

	  btree_scan_clear_key (&bt_scan);
	  pr_clear_value (&copied_key);
	  error_code = heap_scancache_end (thread_p, &isid.scan_cache);
	  if (error_code != NO_ERROR)
	    {
	      if (oid_buf)
		{
		  db_private_free_and_init (thread_p, oid_buf);
		}

	      return NO_ERROR;
	    }
	}
      else
	{
	  assert (false);
	}
    }

  if (oid_buf)
    {
      db_private_free_and_init (thread_p, oid_buf);
    }
  if (attr_ids)
    {
      db_private_free_and_init (thread_p, attr_ids);
    }
  if (keys_prefix_length)
    {
      db_private_free_and_init (thread_p, keys_prefix_length);
    }

  return error_code;

error1:
  heap_scancache_end_modify (thread_p, &scan_cache);

error2:
  btree_scan_clear_key (&bt_scan);
  pr_clear_value (&copied_key);
  (void) heap_scancache_end (thread_p, &isid.scan_cache);

error3:
  if (attr_ids)
    {
      db_private_free_and_init (thread_p, attr_ids);
    }

  if (keys_prefix_length)
    {
      db_private_free_and_init (thread_p, keys_prefix_length);
    }

  heap_attrinfo_end (thread_p, &attr_info);

  if (oid_buf)
    {
      db_private_free_and_init (thread_p, oid_buf);
    }

  return error_code;
}

/*
 * locator_insert_force () - Insert the given object on is heap
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap where the object is going to be inserted
 *   oid(in/out): The new object identifier
 *   recdes(in): The object in disk format
 *   has_index(in): false if we now for sure that there is not any index on the
 *              instances of the class.
 *   op_type(in):
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *              between heap changes.
 *   force_count(in):
 *
 * Note: The given object is inserted on his heap and all appropiate
 *              index entries are inserted.
 */
static int
locator_insert_force (THREAD_ENTRY * thread_p, HFID * hfid, OID * oid,
		      RECDES * recdes, int has_index, int op_type,
		      HEAP_SCANCACHE * scan_cache, int *force_count)
{
  OID class_oid;		/* Class identifier   */
  char *classname;		/* Classname to update */
  RECDES new_recdes;
  bool is_cached = false;
  LC_COPYAREA *cache_attr_copyarea = NULL;
  int error_code = NO_ERROR;
  OID null_oid = {
    NULL_PAGEID, NULL_SLOTID, NULL_VOLID
  };

  *force_count = 0;

  or_class_oid (recdes, &class_oid);

  if (has_index && !locator_Dont_check_foreign_key)
    {
      error_code = locator_check_foreign_key (thread_p, hfid, &class_oid,
					      oid, recdes, &new_recdes,
					      &is_cached,
					      &cache_attr_copyarea);
      if (error_code != NO_ERROR)
	{
	  goto error2;
	}
    }

  if (is_cached)
    {
      recdes = &new_recdes;
    }
  /*
   * This is a new object. The object must be locked in exclusive mode,
   * once its OID is assigned. We just do it for the classes, the new
   * instances are not locked since another client cannot get to them,
   * in any way. How can a client know their OIDs
   */

  /* insert object and lock it */

  if (heap_insert (thread_p, hfid, oid, recdes, scan_cache) == NULL)
    {
      /*
       * Problems inserting the object...Maybe, the transaction should be
       * aborted by the caller...Quit..
       */
      error_code = ER_FAILED;
      goto error2;
    }

  if (OID_IS_ROOTOID (&class_oid))
    {
      /*
       * A CLASS: Add the classname to class_OID entry and add the class
       *          to the catalog. Update both the permanent and transient
       *          classname tables
       *          remove XASL cache entries which is relevant with that class
       */

      classname = or_class_name (recdes);

      if (ehash_insert (thread_p, locator_Eht_classnames,
			(void *) classname, oid) == NULL)
	{
	  /*
	   * error inserting the hash entry information.
	   *
	   * Maybe the transaction should be aborted by the caller.
	   */
	  error_code = ER_FAILED;
	  goto error1;
	}

      /* Indicate new oid to transient table */
      locator_permoid_class_name (thread_p, classname, oid);

      if (!OID_IS_ROOTOID (oid) && catalog_insert (thread_p, recdes, oid) < 0)
	{
	  /*
	   * There is an error inserting the hash entry or catalog
	   * information. Maybe, the transaction should be aborted by
	   * the caller...Quit
	   */
	  error_code = ER_FAILED;
	  goto error1;
	}

      if (catcls_Enable == true
	  && catcls_insert_catalog_classes (thread_p, recdes) != NO_ERROR)
	{
	  error_code = ER_FAILED;
	  goto error1;
	}

      /* remove XASL cache entries which are relevant with this class */
      if (!OID_IS_ROOTOID (oid)
	  && PRM_XASL_MAX_PLAN_CACHE_ENTRIES > 0
	  && qexec_remove_xasl_cache_ent_by_class (thread_p, oid) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"locator_insert_force: xs_remove_xasl_cache_ent_by_class "
			"failed for class { %d %d %d }\n",
			oid->pageid, oid->slotid, oid->volid);
	}
    }
  else
    {
      /*
       * AN INSTANCE: Apply the necessary index insertions
       */
      if (has_index
	  && locator_add_or_remove_index (thread_p, recdes, oid, &class_oid,
					  true, op_type, scan_cache, true,
					  true, hfid) != NO_ERROR)
	{
	  error_code = ER_FAILED;
	  goto error1;
	}

      /* increase the counter of the catalog */
      locator_increase_catalog_count (thread_p, &class_oid);

      /* remove query result cache entries which are relevant with this class */
      if (!QFILE_IS_LIST_CACHE_DISABLED)
	{
	  if (qexec_clear_list_cache_by_class (thread_p, &class_oid) !=
	      NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "locator_insert_force: qexec_clear_list_cache_by_class "
			    "failed for class { %d %d %d }\n",
			    class_oid.pageid, class_oid.slotid,
			    class_oid.volid);
	    }
	  qmgr_add_modified_class (thread_p, &class_oid);
	}
    }

  /* Unlock the object according to isolation level */
  /* locked by heap_insert */
  /* manual duration */
  lock_unlock_object (thread_p, &null_oid, &class_oid, X_LOCK, false);

  *force_count = 1;

  if (cache_attr_copyarea != NULL)
    {
      locator_free_copy_area (cache_attr_copyarea);
    }

  return error_code;

error1:
  lock_unlock_object (thread_p, oid, &class_oid, X_LOCK, false);

  if (cache_attr_copyarea != NULL)
    {
      locator_free_copy_area (cache_attr_copyarea);
    }

error2:
  return error_code;
}

/*
 * locator_update_force () - Update the given object
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap where the object is going to be inserted
 *   oid(in): The object identifier
 *   oldrecdes(in):
 *   recdes(in):  The object in disk format
 *   has_index(in): false if we now for sure that there is not any index
 *                   on the instances of the class.
 *   att_id(in): Updated attr id array
 *   n_att_id(in): Updated attr id array length
 *   op_type(in):
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *                   between heap changes.
 *   force_count(in):
 *   not_check_fk(in):
 *
 * Note: The given object is updated on his heap and all appropiate
 *              index entries are updated.
 */
static int
locator_update_force (THREAD_ENTRY * thread_p, HFID * hfid, OID * oid,
		      RECDES * oldrecdes, RECDES * recdes, int has_index,
		      ATTR_ID * att_id, int n_att_id, int op_type,
		      HEAP_SCANCACHE * scan_cache, int *force_count,
		      bool not_check_fk, REPL_INFO_TYPE repl_info)
{
  OID class_oid;		/* Class identifier   */
  char *old_classname = NULL;	/* Classname that may have been
				 * renamed */
  char *classname = NULL;	/* Classname to update */
  bool isold_object;		/* Make sure that this is an old
				 * object */
  RECDES copy_recdes;
  SCAN_CODE scan;

  RECDES new_recdes;
  bool is_cached = false;
  LC_COPYAREA *cache_attr_copyarea = NULL;
  int error_code = NO_ERROR;

  /*
   * While scanning objects, the given scancache does not fix the last
   * accessed page. So, the object must be copied to the record descriptor.
   */
  copy_recdes.data = NULL;

  *force_count = 0;

  /*
   * Is the object a class ?
   */

  or_class_oid (recdes, &class_oid);

  if (has_index && !not_check_fk && !locator_Dont_check_foreign_key)
    {
      error_code = locator_check_foreign_key (thread_p, hfid, &class_oid,
					      oid, recdes, &new_recdes,
					      &is_cached,
					      &cache_attr_copyarea);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

  if (is_cached)
    {
      recdes = &new_recdes;
    }

  if (OID_IS_ROOTOID (&class_oid))
    {
      /*
       * A CLASS: classes do not have any indices...however, the classname
       * to oid table may need to be updated
       */
      classname = or_class_name (recdes);
      old_classname = heap_get_class_name_alloc_if_diff (thread_p, oid,
							 classname);
      /*
       * Compare the classname pointers. If the same pointers classes are the
       * same since the class was no malloc
       */
      if (old_classname != NULL && old_classname != classname)
	{
	  /*
	   * Different names, the class was renamed.
	   */
	  if (ehash_insert (thread_p, locator_Eht_classnames,
			    (void *) classname, oid) == NULL
	      || ehash_delete (thread_p, locator_Eht_classnames,
			       (void *) old_classname) == NULL)
	    {
	      /*
	       * Problems inserting/deleting the new name to the classname to
	       * OID table.
	       * Maybe, the transaction should be aborted by the caller.
	       * Quit..
	       */
	      free_and_init (old_classname);

	      error_code = ER_FAILED;
	      goto error;
	    }
	}

      if ((catcls_Enable == true) && (old_classname != NULL))
	{
	  error_code = catcls_update_catalog_classes (thread_p,
						      old_classname, recdes);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}

      if (heap_update (thread_p, hfid, oid, recdes, &isold_object, scan_cache)
	  == NULL)
	{
	  /*
	   * Problems updating the object...Maybe, the transaction should be
	   * aborted by the caller...Quit..
	   */
	  error_code = ER_FAILED;
	  goto error;
	}

      if (isold_object)
	{
	  /* Update the catalog as long as it is not the root class */
	  if (!OID_IS_ROOTOID (oid))
	    {
	      error_code = catalog_update (thread_p, recdes, oid);
	      if (error_code < 0)
		{
		  /*
		   * An error occurred during the update of the catalog
		   */
		  goto error;
		}
	    }
	  if (old_classname != NULL && old_classname != classname)
	    {
	      free_and_init (old_classname);
	    }
	}
      else
	{
	  /*
	   * NEW CLASS
	   * The class was flushed for first time. Add the classname to
	   * class_OID entry and add the class to the catalog. We don't need
	   * to update the transient table since the class has already a
	   * permananet OID...
	   */
	  classname = or_class_name (recdes);
	  if (ehash_insert (thread_p, locator_Eht_classnames,
			    (void *) classname, oid) == NULL
	      || (!OID_IS_ROOTOID (oid)
		  && catalog_insert (thread_p, recdes, oid) < 0))
	    {
	      /*
	       * There is an error inserting the hash entry or catalog
	       * information. The transaction must be aborted by the caller
	       */
	      error_code = ER_FAILED;
	      goto error;
	    }
	  if (catcls_Enable == true)
	    {
	      error_code = catcls_insert_catalog_classes (thread_p, recdes);
	      if (error_code != NO_ERROR)
		{
		  goto error;
		}
	    }
	}

      /* remove XASL cache entries which is relevant with that class */
      if (!OID_IS_ROOTOID (oid)
	  && PRM_XASL_MAX_PLAN_CACHE_ENTRIES > 0
	  && qexec_remove_xasl_cache_ent_by_class (thread_p, oid) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"locator_update_force: xs_remove_xasl_cache_ent_by_class "
			"failed for class { %d %d %d }\n",
			oid->pageid, oid->slotid, oid->volid);
	}
    }
  else
    {
      /* AN INSTANCE: Update indices if any */

      if (has_index)
	{
	  if (oldrecdes == NULL)
	    {
	      scan = heap_get (thread_p, oid, &copy_recdes, scan_cache, COPY,
			       NULL_CHN);
	      oldrecdes = &copy_recdes;
	    }
	  else
	    {
	      scan = S_SUCCESS;
	    }

	  if (scan == S_SUCCESS)
	    {
	      /* Update the indices */
	      error_code = locator_update_index (thread_p, recdes, oldrecdes,
						 att_id, n_att_id, oid,
						 &class_oid, op_type,
						 scan_cache, true, true,
						 repl_info);
	      if (error_code != NO_ERROR)
		{
		  /*
		   * There is an error updating the index... Quit... The
		   * transaction must be aborted by the caller
		   */
		  goto error;
		}
	    }
	  else
	    {
	      /*
	       * We could not get the object.
	       * The object may be a new instance, that is only the address
	       * (no content) is known by the heap manager.
	       */
	      int err_id = er_errid ();

	      if (err_id == ER_HEAP_NODATA_NEWADDRESS)
		{
		  er_clear ();	/* clear the error code */
		  if (op_type == SINGLE_ROW_MODIFY)
		    {		/* to enable uniqueness checking */
		      op_type = SINGLE_ROW_INSERT;
		    }

		  error_code = locator_add_or_remove_index (thread_p, recdes,
							    oid, &class_oid,
							    true, op_type,
							    scan_cache, true,
							    true, hfid);
		  if (error_code != NO_ERROR)
		    {
		      goto error;
		    }
		}
	      else
		{
		  if (err_id == ER_HEAP_UNKNOWN_OBJECT)
		    {
		      er_log_debug (ARG_FILE_LINE, "locator_update_force: "
				    "unknown oid ( %d|%d|%d )\n",
				    oid->pageid, oid->slotid, oid->volid);
		    }

		  error_code = ER_FAILED;
		  goto error;
		}
	    }
	}

      if (heap_update (thread_p, hfid, oid, recdes, &isold_object,
		       scan_cache) == NULL)
	{
	  /*
	   * Problems updating the object...Maybe, the transaction should be
	   * aborted by the caller...Quit..
	   */
	  error_code = ER_FAILED;
	  goto error;
	}

      /*
       * for replication,
       * We have to set UPDATE LSA number to the log info.
       * The target log info was already created when the locator_update_index
       */
      if (db_Enable_replications > 0
	  && repl_class_is_replicated (&class_oid)
	  && !LOG_CHECK_LOG_APPLIER (thread_p))
	{
	  repl_add_update_lsa (thread_p, oid);
	}

      if (isold_object == false)
	{
	  locator_increase_catalog_count (thread_p, &class_oid);
	}

      /* remove query result cache entries which are relevant with this class */
      if (!QFILE_IS_LIST_CACHE_DISABLED)
	{
	  if (qexec_clear_list_cache_by_class (thread_p, &class_oid) !=
	      NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "locator_update_force: qexec_clear_list_cache_by_class failed for"
			    " class { %d %d %d }\n",
			    class_oid.pageid, class_oid.slotid,
			    class_oid.volid);
	    }
	  qmgr_add_modified_class (thread_p, &class_oid);
	}
    }

  *force_count = 1;

error:

  if (cache_attr_copyarea != NULL)
    {
      locator_free_copy_area (cache_attr_copyarea);
    }

  return error_code;
}

/*
 * locator_delete_force () - Delete the given object
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap where the object is going to be inserted
 *   oid(in): The object identifier
 *   has_index(in): false if we now for sure that there is not any index
 *                   on the instances of the class.
 *   op_type(in):
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *                   between heap changes.
 *   force_count(in):
 *
 * Note: The given object is deleted on his heap and all appropiate
 *              index entries are deleted.
 */
static int
locator_delete_force (THREAD_ENTRY * thread_p, HFID * hfid, OID * oid,
		      int has_index, int op_type,
		      HEAP_SCANCACHE * scan_cache, int *force_count)
{
  bool isold_object;		/* Make sure that this is an old object
				 * during the deletion */
  OID class_oid = {
    NULL_PAGEID, NULL_SLOTID, NULL_VOLID
  };
  /* Class identifier */
  char *classname;		/* Classname to update */
  RECDES copy_recdes;
  int error_code = NO_ERROR;

  /* Update note :
   *   While scanning objects, the given scancache does not fix the last
   *   accessed page. So, the object must be copied to the record descriptor.
   * Changes :
   *   (1) variable name : peek_recdes => copy_recdes
   *   (2) function call : heap_get(..., PEEK, ...) => heap_get(..., COPY, ...)
   *   (3) SCAN_CODE scan, char *new_area are added
   */

  copy_recdes.data = NULL;

  *force_count = 0;

  /*
   * Is the object a class ?
   */
  isold_object = true;

  if (heap_get (thread_p, oid, &copy_recdes, scan_cache, COPY, NULL_CHN) !=
      S_SUCCESS)
    {
      int err_id = er_errid ();

      if (err_id == ER_HEAP_NODATA_NEWADDRESS)
	{
	  isold_object = false;
	  er_clear ();		/* clear ER_HEAP_NODATA_NEWADDRESS */
	}
      else if (err_id == ER_HEAP_UNKNOWN_OBJECT)
	{
	  isold_object = false;
	  er_clear ();

	  error_code = NO_ERROR;
	  goto error;
	}
      else
	{
	  /*
	   * Problems reading the object...Maybe, the transaction should be
	   * aborted by the caller...Quit..
	   */
	  error_code = ER_FAILED;
	  goto error;
	}
    }

  /*
   * Find the class OID
   */
  if (isold_object == true)
    {
      or_class_oid (&copy_recdes, &class_oid);
    }
  else
    {
      OID_SET_NULL (&class_oid);
    }

  if (isold_object == true && OID_IS_ROOTOID (&class_oid))
    {
      /*
       * A CLASS: Remove classname to classOID entry
       *          remove class from catalog and
       *          remove any indices on that class
       *          remove XASL cache entries which is relevant with that class
       */

      /* Delete the classname entry */
      classname = or_class_name (&copy_recdes);
      if ((ehash_delete (thread_p, locator_Eht_classnames, (void *) classname)
	   == NULL))
	{
	  er_log_debug (ARG_FILE_LINE,
			"locator_delete_force: ehash_delete failed for tran %d\n",
			LOG_FIND_THREAD_TRAN_INDEX (thread_p));
	  error_code = ER_FAILED;
	  goto error;
	}

      /* Note: by now, the client has probably already requested this class
       * be deleted. We try again here
       * just to be sure it has been marked properly.  Note that we would
       * normally want to check the return code, but we must not check the
       * return code for this one function in its current form, because we
       * cannot distinguish between a class that has already been
       * marked deleted and a real error.
       */
      (void) xlocator_delete_class_name (thread_p, classname);
      /* remove from the catalog... when is not the root */
      if (!OID_IS_ROOTOID (oid))
	{
	  error_code = catalog_delete (thread_p, oid);
	  if (error_code != NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "locator_delete_force: ct_delete_catalog failed "
			    "for tran %d\n",
			    LOG_FIND_THREAD_TRAN_INDEX (thread_p));
	      goto error;
	    }
	}
      if (catcls_Enable)
	{
	  error_code = catcls_delete_catalog_classes (thread_p, classname,
						      oid);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}

      /* remove XASL cache entries which is relevant with that class */
      if (!OID_IS_ROOTOID (oid)
	  && PRM_XASL_MAX_PLAN_CACHE_ENTRIES > 0
	  && qexec_remove_xasl_cache_ent_by_class (thread_p, oid) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"locator_delete_force: xs_remove_xasl_cache_ent_by_class"
			" failed for class { %d %d %d }\n",
			oid->pageid, oid->slotid, oid->volid);
	}
    }
  else
    {
      /*
       * Likely an INSTANCE: Apply the necessary index deletions
       *
       * If this is a server delete on an instance, the object must be locked
       * in exclusive  mode since it is likely that we have just added an
       * SIX lock on the class at this moment.
       *
       * Note that we cannot have server deletes on classes.
       */
      if (isold_object == true && has_index)
	{
	  error_code = locator_add_or_remove_index (thread_p, &copy_recdes,
						    oid, &class_oid, false,
						    op_type, scan_cache, true,
						    true, hfid);
	  if (error_code != NO_ERROR)
	    {
	      /*
	       * There is an error deleting the index... Quit... The
	       * transaction must be aborted by the caller
	       */
	      goto error;
	    }
	}

      /* remove query result cache entries which are relevant with this class */
      if (!QFILE_IS_LIST_CACHE_DISABLED)
	{
	  if (qexec_clear_list_cache_by_class (thread_p, &class_oid) !=
	      NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "locator_delete_force: qexec_clear_list_cache_by_class"
			    " failed for class { %d %d %d }\n",
			    class_oid.pageid, class_oid.slotid,
			    class_oid.volid);
	    }
	  qmgr_add_modified_class (thread_p, &class_oid);
	}
    }

  if (heap_delete (thread_p, hfid, oid, scan_cache) == NULL)
    {
      /*
       * Problems deleting the object...Maybe, the transaction should be
       * aborted by the caller...Quit..
       */
      er_log_debug (ARG_FILE_LINE,
		    "locator_delete_force: hf_delete failed for tran %d\n",
		    LOG_FIND_THREAD_TRAN_INDEX (thread_p));
      error_code = ER_FAILED;
      goto error;
    }
  *force_count = 1;

  if (isold_object == true && !OID_IS_ROOTOID (&class_oid))
    {
      /* decrease the counter of the catalog */
      locator_decrease_catalog_count (thread_p, &class_oid);
    }

error:

  return error_code;
}

/*
 * locator_force_for_multi_update () -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   force_area(in):  Copy area where objects are placed
 *
 * Note: This function update given objects that are sent by clients.
 *              The objects are updated by multiple row update performed
 *              on client and sent to the server through flush request.
 */
static int
locator_force_for_multi_update (THREAD_ENTRY * thread_p,
				LC_COPYAREA * force_area)
{
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in area */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area */
  RECDES recdes;		/* Record descriptor for object */
  HEAP_SCANCACHE scan_cache;
  int scan_cache_inited = 0;
  LOG_TDES *tdes;
  int i, s, t;
  int malloc_size;
  BTREE_UNIQUE_STATS *temp_info;
  char *ptr;
  int force_count;
  int tran_index;
  int error_code = NO_ERROR;
  REPL_INFO_TYPE repl_info;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes == NULL)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_UNKNOWN_TRANINDEX, 1, tran_index);
      error_code = ER_LOG_UNKNOWN_TRANINDEX;
      goto error;
    }

  mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (force_area);

  if (mobjs->start_multi_update && tdes->unique_stat_info == NULL)
    {
      tdes->num_unique_btrees = 0;
      tdes->max_unique_btrees = UNIQUE_STAT_INFO_INCREMENT;

      malloc_size = sizeof (BTREE_UNIQUE_STATS) * UNIQUE_STAT_INFO_INCREMENT;
      tdes->unique_stat_info =
	(BTREE_UNIQUE_STATS *) db_private_alloc (thread_p, malloc_size);
      if (tdes->unique_stat_info == NULL)
	{
	  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto error;
	}
    }

  if (mobjs->num_objs > 0)
    {
      obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);
      obj = LC_PRIOR_ONEOBJ_PTR_IN_COPYAREA (obj);
      LC_RECDES_IN_COPYAREA (force_area, &recdes);

      for (i = 0; i < mobjs->num_objs; i++)
	{
	  obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);
	  LC_RECDES_TO_GET_ONEOBJ (force_area, obj, &recdes);

	  if (i == 0)
	    {
	      /* Initialize a modify scancache */
	      error_code = locator_start_force_scan_cache (thread_p,
							   &scan_cache,
							   &obj->hfid,
							   &mobjs->class_oid,
							   MULTI_ROW_UPDATE);
	      if (error_code != NO_ERROR)
		{
		  goto error;
		}
	      scan_cache_inited = 1;
	    }

	  if (mobjs->start_multi_update && i == 0)
	    {
	      repl_info = REPL_INFO_TYPE_STMT_START;
	    }
	  else if (mobjs->end_multi_update && (i + 1) == mobjs->num_objs)
	    {
	      repl_info = REPL_INFO_TYPE_STMT_END;
	    }
	  else
	    {
	      repl_info = REPL_INFO_TYPE_STMT_NORMAL;
	    }
	  error_code = locator_update_force (thread_p, &obj->hfid, &obj->oid,
					     NULL, &recdes, obj->has_index,
					     NULL, 0, MULTI_ROW_UPDATE,
					     &scan_cache, &force_count,
					     false, repl_info);
	  if (error_code != NO_ERROR)
	    {
	      /*
	       * Problems updating the object...Maybe, the transaction should be
	       * aborted by the caller...Quit..
	       */
	      goto error;
	    }
	}			/* end-for */

      for (s = 0; s < scan_cache.num_btids; s++)
	{
	  temp_info = &(scan_cache.index_stat_info[s]);
	  if (temp_info->num_nulls == 0 && temp_info->num_keys == 0
	      && temp_info->num_oids == 0)
	    {
	      continue;
	    }
	  /* non-unique index would be filtered out at above statement. */

	  for (t = 0; t < tdes->num_unique_btrees; t++)
	    {
	      if (BTID_IS_EQUAL (&temp_info->btid,
				 &tdes->unique_stat_info[t].btid))
		{
		  break;
		}
	    }
	  if (t < tdes->num_unique_btrees)
	    {
	      /* The same unique index has been found */
	      tdes->unique_stat_info[t].num_nulls += temp_info->num_nulls;
	      tdes->unique_stat_info[t].num_keys += temp_info->num_keys;
	      tdes->unique_stat_info[t].num_oids += temp_info->num_oids;
	    }
	  else
	    {
	      /* The same unique index has not been found */
	      if (tdes->num_unique_btrees == tdes->max_unique_btrees)
		{
		  /* we need more space for storing unique index stat. info.  */
		  tdes->max_unique_btrees += UNIQUE_STAT_INFO_INCREMENT;
		  malloc_size = (sizeof (BTREE_UNIQUE_STATS) *
				 tdes->max_unique_btrees);
		  ptr = (char *) db_private_realloc (thread_p,
						     tdes->unique_stat_info,
						     malloc_size);
		  if (ptr == NULL)
		    {
		      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
		      goto error;
		    }
		  tdes->unique_stat_info = (BTREE_UNIQUE_STATS *) ptr;
		}
	      t = tdes->num_unique_btrees;
	      BTID_COPY (&tdes->unique_stat_info[t].btid, &temp_info->btid);
	      tdes->unique_stat_info[t].num_nulls = temp_info->num_nulls;
	      tdes->unique_stat_info[t].num_keys = temp_info->num_keys;
	      tdes->unique_stat_info[t].num_oids = temp_info->num_oids;
	      tdes->num_unique_btrees++;	/* increment */
	    }
	}

      locator_end_force_scan_cache (thread_p, &scan_cache);
      scan_cache_inited = 0;
    }

  if (mobjs->end_multi_update)
    {
      for (s = 0; s < tdes->num_unique_btrees; s++)
	{
	  if (tdes->unique_stat_info[s].num_nulls == 0
	      && tdes->unique_stat_info[s].num_keys == 0
	      && tdes->unique_stat_info[s].num_oids == 0)
	    {
	      continue;		/* no modification : non-unique index */
	    }
	  if ((tdes->unique_stat_info[s].num_nulls
	       + tdes->unique_stat_info[s].num_keys)
	      != tdes->unique_stat_info[s].num_oids)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_BTREE_UNIQUE_FAILED, 0);
	      error_code = ER_BTREE_UNIQUE_FAILED;
	      goto error;
	    }

	  /* (num_nulls + num_keys) == num_oids */
	  error_code = btree_reflect_unique_statistics (thread_p,
							&tdes->
							unique_stat_info[s]);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}
      if (tdes->unique_stat_info != NULL)
	{
	  db_private_free_and_init (thread_p, tdes->unique_stat_info);
	}
    }

  return error_code;

error:
  if (scan_cache_inited)
    {
      locator_end_force_scan_cache (thread_p, &scan_cache);
    }

  if (tdes != NULL && tdes->unique_stat_info != NULL)
    {
      db_private_free_and_init (thread_p, tdes->unique_stat_info);
    }

  return error_code;
}

/*
 * xlocator_force () - Updates objects placed on page
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   force_area(in): Copy area where objects are placed
 *
 * Note: This function applies all the desired operations on each of
 *              object placed in the force_area.
 */
int
xlocator_force (THREAD_ENTRY * thread_p, LC_COPYAREA * force_area)
{
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in area */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area        */
  RECDES recdes;		/* Record descriptor for object      */
  int i;
  HEAP_SCANCACHE *force_scancache = NULL;
  HEAP_SCANCACHE scan_cache;
  int force_count;
  LOG_LSA lsa;
  int error_code = NO_ERROR;

  /* need to start a topop to ensure the atomic operation. */
  error_code = xtran_server_start_topop (thread_p, &lsa);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (force_area);

  if (!OID_ISNULL (&mobjs->class_oid))
    {
      error_code = locator_force_for_multi_update (thread_p, force_area);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
      else
	{
	  goto done;
	}
    }

  obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);
  obj = LC_PRIOR_ONEOBJ_PTR_IN_COPYAREA (obj);
  LC_RECDES_IN_COPYAREA (force_area, &recdes);

  for (i = 0; i < mobjs->num_objs; i++)
    {
      obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);
      LC_RECDES_TO_GET_ONEOBJ (force_area, obj, &recdes);

      if (i == 0)
	{
	  /*
	   * Initialize a modify scancache
	   */
	  error_code = locator_start_force_scan_cache (thread_p, &scan_cache,
						       &obj->hfid, NULL,
						       SINGLE_ROW_UPDATE);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	  force_scancache = &scan_cache;
	}

      switch (obj->operation)
	{
	case LC_FLUSH_INSERT:
	  error_code = locator_insert_force (thread_p, &obj->hfid, &obj->oid,
					     &recdes, obj->has_index,
					     SINGLE_ROW_INSERT,
					     force_scancache, &force_count);
	  if (error_code != NO_ERROR)
	    {
	      /*
	       * Problems inserting the object...Maybe, the transaction should
	       * be aborted by the caller...Quit..
	       */
	      goto error;
	    }
	  /* monitor */
	  mnt_qm_inserts (thread_p);
	  break;

	case LC_FLUSH_UPDATE:
	  error_code = locator_update_force (thread_p, &obj->hfid, &obj->oid,
					     NULL, &recdes, obj->has_index,
					     NULL, 0, SINGLE_ROW_UPDATE,
					     force_scancache, &force_count,
					     false,
					     REPL_INFO_TYPE_STMT_NORMAL);
	  if (error_code != NO_ERROR)
	    {
	      /*
	       * Problems updating the object...Maybe, the transaction should be
	       * aborted by the caller...Quit..
	       */
	      goto error;
	    }
	  /* monitor */
	  mnt_qm_updates (thread_p);
	  break;

	case LC_FLUSH_DELETE:
	  error_code = locator_delete_force (thread_p, &obj->hfid, &obj->oid,
					     obj->has_index,
					     SINGLE_ROW_DELETE,
					     force_scancache, &force_count);
	  if (error_code != NO_ERROR)
	    {
	      /*
	       * Problems reading the object...Maybe, the transaction should be
	       * aborted by the caller...Quit..
	       */
	      goto error;
	    }
	  /* monitor */
	  mnt_qm_deletes (thread_p);
	  break;

	default:
	  /*
	   * Problems forcing the object. Don't known what flush/force operation
	   * to execute on the object... This is a system error...
	   * Maybe, the transaction should be aborted by the caller...Quit..
	   */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LC_BADFORCE_OPERATION, 4, obj->operation,
		  obj->oid.volid, obj->oid.pageid, obj->oid.slotid);
	  error_code = ER_LC_BADFORCE_OPERATION;
	  goto error;
	}			/* end-switch */
    }				/* end-for */

done:

  if (force_scancache != NULL)
    {
      locator_end_force_scan_cache (thread_p, force_scancache);
    }

  (void) xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER,
				 &lsa);

  return error_code;

error:

  if (force_scancache != NULL)
    {
      locator_end_force_scan_cache (thread_p, force_scancache);
    }

  (void) xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);

  return error_code;
}

/*
 * locator_allocate_copy_area_by_attr_info () - Transforms attribute
 *              information into a disk representation and allocates a
 *              LC_COPYAREA big enough to fit the representation
 *
 * return: the allocated LC_COPYAREA if all OK, NULL otherwise
 *
 *   attr_info(in/out): Attribute information
 *                      (Set as a side effect to fill the rest of values)
 *   old_recdes(in): The old representation of the object or NULL if this is a
 *                   new object (to be inserted).
 *   new_recdes(in): The resulting new representation of the object.
 *   copyarea_length_hint(in): An estimated size for the LC_COPYAREA or -1 if
 *                             an estimated size is not known.
 *
 * Note: The allocated should be freed by using locator_free_copy_area ()
 */
LC_COPYAREA *
locator_allocate_copy_area_by_attr_info (THREAD_ENTRY * thread_p,
					 HEAP_CACHE_ATTRINFO * attr_info,
					 RECDES * old_recdes,
					 RECDES * new_recdes,
					 const int copyarea_length_hint)
{
  LC_COPYAREA *copyarea = NULL;
  int copyarea_length =
    copyarea_length_hint <= 0 ? DB_PAGESIZE : copyarea_length_hint;
  SCAN_CODE scan = S_DOESNT_FIT;

  while (scan == S_DOESNT_FIT)
    {
      copyarea = locator_allocate_copy_area_by_length (copyarea_length,
						       CLEAR_MEM);
      if (copyarea == NULL)
	{
	  break;
	}

      new_recdes->data = copyarea->mem;
      new_recdes->area_size = copyarea->length;

      scan = heap_attrinfo_transform_to_disk (thread_p, attr_info, old_recdes,
					      new_recdes);
      if (scan != S_SUCCESS)
	{
	  /* Get the real length used in the copy area */
	  copyarea_length = copyarea->length;
	  locator_free_copy_area (copyarea);
	  copyarea = NULL;
	  new_recdes->data = NULL;
	  new_recdes->area_size = 0;

	  /* Is more space needed ? */
	  if (scan == S_DOESNT_FIT)
	    {
	      /*
	       * The object does not fit into copy area, increase the area
	       * to estimated size included in length of record descriptor.
	       */
	      if (copyarea_length < (-new_recdes->length))
		{
		  copyarea_length = -new_recdes->length;
		}
	      else
		{
		  /*
		   * This is done for security purposes only, since the
		   * transformation may not have given us the correct length,
		   * somehow.
		   */
		  copyarea_length += DB_PAGESIZE;
		}
	    }
	}
    }
  return copyarea;
}

/*
 * locator_attribute_info_force () - Force an object represented by attribute
 *                                   information structure
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Where of the object
 *   oid(in/out): The object identifier
 *                   (Set as a side effect when operation is insert)
 *   attr_info(in/out): Attribute information
 *                   (Set as a side effect to fill the rest of values)
 *   att_id(in): Updated attr id array
 *   n_att_id(in/out): Updated attr id array length
 *                   (Set as a side effect to fill the rest of values)
 *   operation(in): Type of operation (either LC_FLUSH_INSERT,
 *                   LC_FLUSH_UPDATE, or LC_FLUSH_DELETE)
 *   op_type(in):
 *   scan_cache(in):
 *   force_count(in):
 *   not_check_fk(in):
 *
 * Note: Force an object represented by an attribute information structure.
 *       For insert the oid is set as a side effect.
 *       For delete, the attr_info does not need to be given.
 */
int
locator_attribute_info_force (THREAD_ENTRY * thread_p, HFID * hfid,
			      OID * oid, int partition_node_id,
			      HEAP_CACHE_ATTRINFO * attr_info,
			      ATTR_ID * att_id, int n_att_id,
			      LC_COPYAREA_OPERATION operation, int op_type,
			      HEAP_SCANCACHE * scan_cache, int *force_count,
			      bool not_check_fk, REPL_INFO_TYPE repl_info)
{
  LC_COPYAREA *copyarea = NULL;
  RECDES new_recdes;
  SCAN_CODE scan;		/* Scan return value for next operation */
  RECDES copy_recdes;
  RECDES *old_recdes = NULL;
  int error_code = NO_ERROR;

  /*
   * While scanning objects, the given scancache does not fix the last
   * accessed page. So, the object must be copied to the record descriptor.
   */
  copy_recdes.data = NULL;

  switch (operation)
    {
    case LC_FLUSH_UPDATE:
      scan = heap_get (thread_p, oid, &copy_recdes, scan_cache, COPY,
		       NULL_CHN);
      if (scan == S_SUCCESS)
	{
	  old_recdes = &copy_recdes;
	}
      else if (scan == S_ERROR || scan == S_DOESNT_FIT)
	{
	  /* Whenever an error including an interrupt was broken out,
	   * quit the update.
	   */
	  return ER_FAILED;
	}
      else if (scan == S_DOESNT_EXIST)
	{
	  int err_id = er_errid ();

	  if (err_id == ER_HEAP_NODATA_NEWADDRESS)
	    {
	      /* it is an immature record. go ahead to update */
	      er_clear ();
	    }
	  else
	    {
	      return ((err_id == NO_ERROR) ? ER_FAILED : err_id);
	    }
	}
      else
	{
	  assert (false);
	  return ER_FAILED;
	}

      /* Fall through */

    case LC_FLUSH_INSERT:
      copyarea = locator_allocate_copy_area_by_attr_info (thread_p, attr_info,
							  old_recdes,
							  &new_recdes, -1);
      if (copyarea == NULL)
	{
	  error_code = ER_FAILED;
	  break;
	}

      if (partition_node_id == 0)
	{

	  /* Assume that it has indices */
	  if (operation == LC_FLUSH_INSERT)
	    {
	      error_code =
		locator_insert_force (thread_p, hfid, oid, &new_recdes, true,
				      op_type, scan_cache, force_count);
	    }
	  else
	    {
	      assert (operation == LC_FLUSH_UPDATE);
	      error_code =
		locator_update_force (thread_p, hfid, oid, old_recdes,
				      &new_recdes, true, att_id, n_att_id,
				      op_type, scan_cache, force_count,
				      not_check_fk, repl_info);
	    }
	}
      else
	{
#if defined (SERVER_MODE)
	  LC_COPYAREA_MANYOBJS *mobjs;	/* Structure which describes mflush objects */
	  LC_COPYAREA_ONEOBJ *obj;	/* Describe one object for remote host */
	  mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (copyarea);
	  obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);

	  mobjs->num_objs++;

	  mobjs->class_oid.pageid = NULL_PAGEID;
	  mobjs->class_oid.slotid = NULL_SLOTID;
	  mobjs->class_oid.volid = NULL_VOLID;

	  obj->operation = operation;
	  obj->has_index = true;
	  HFID_COPY (&obj->hfid, hfid);
	  COPY_OID (&obj->oid, oid);
	  obj->length = new_recdes.length;
	  obj->offset = CAST_BUFLEN (new_recdes.data - copyarea->mem);

	  error_code =
	    remote_locator_force (thread_p, copyarea, partition_node_id);

	  if (error_code == NO_ERROR)
	    {
	      (*force_count) = 1;
	    }
#endif
	}

      if (copyarea != NULL)
	{
	  locator_free_copy_area (copyarea);
	  copyarea = NULL;
	  new_recdes.data = NULL;
	  new_recdes.area_size = 0;
	}
      break;

    case LC_FLUSH_DELETE:
      error_code = locator_delete_force (thread_p, hfid, oid, true,
					 op_type, scan_cache, force_count);
      break;

    default:
      /*
       * Problems forcing the object. Don't known what flush/force operation
       * to execute on the object... This is a system error...
       * Maybe, the transaction should be aborted by the caller...Quit..
       */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_BADFORCE_OPERATION, 4,
	      operation, oid->volid, oid->pageid, oid->slotid);
      error_code = ER_LC_BADFORCE_OPERATION;
      break;
    }

  return error_code;
}

/*
 * locator_other_insert_delete () -
 *
 * return:  NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Where of the object
 *   oid(in): The object identifier
 *   newhfid(in): Where of the new object
 *   newoid_para(in): The new object identifier
 *   attr_info(in/out): Attribute information
 *                   (Set as a side effect to fill the rest of values)
 *   scan_cache(in):
 *   force_count(in):
 *   prev_oid(in):
 *   new_reprid(in):
 *
 * Note:Partition change - old read/old delete/new insert
 */
int
locator_other_insert_delete (THREAD_ENTRY * thread_p, HFID * hfid,
			     OID * oid, HFID * newhfid, OID * newoid_para,
			     HEAP_CACHE_ATTRINFO * attr_info,
			     HEAP_SCANCACHE * scan_cache, int *force_count,
			     OID * prev_oid, REPR_ID * new_reprid)
{
  LC_COPYAREA *copyarea;
  int copyarea_length;
  RECDES new_recdes;
  SCAN_CODE scan;		/* Scan return value for next operation */
  int old_cache;
  RECDES copy_recdes;
  RECDES *old_recdes = NULL;
  OID oldoid, newoid;
  REPR_ID old_reprid;
  int error_code = NO_ERROR;

  copy_recdes.data = NULL;
  if (heap_get (thread_p, oid, &copy_recdes, scan_cache, COPY, NULL_CHN) !=
      S_SUCCESS)
    {
      return ER_FAILED;
    }
  else
    {
      old_recdes = &copy_recdes;
    }

  scan = S_DOESNT_FIT;
  copyarea_length = DB_PAGESIZE;

  old_cache = attr_info->last_cacheindex;
  error_code = heap_attrinfo_set_uninitialized_global (thread_p,
						       &attr_info->inst_oid,
						       old_recdes, attr_info);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  OID_SET_NULL (&attr_info->inst_oid);
  attr_info->inst_chn = NULL_CHN;
  attr_info->last_cacheindex = -1;
  COPY_OID (&newoid, newoid_para);
  COPY_OID (&oldoid, &attr_info->class_oid);
  COPY_OID (&attr_info->class_oid, &newoid);
  old_reprid = attr_info->last_classrepr->id;

  if (!OID_EQ (prev_oid, &newoid))
    {
      *new_reprid = heap_get_class_repr_id (thread_p, &newoid);
      if (*new_reprid <= 0)
	{
	  error_code = ER_FAILED;
	  goto end;
	}
      COPY_OID (prev_oid, &newoid);
    }

  attr_info->last_classrepr->id = *new_reprid;

  while (scan == S_DOESNT_FIT)
    {
      copyarea = locator_allocate_copy_area_by_length (copyarea_length,
						       CLEAR_MEM);
      if (copyarea == NULL)
	{
	  break;
	}

      new_recdes.data = copyarea->mem;
      new_recdes.area_size = copyarea->length;
      scan = heap_attrinfo_transform_to_disk (thread_p, attr_info, NULL,
					      &new_recdes);
      if (scan != S_SUCCESS)
	{
	  copyarea_length = copyarea->length;
	  locator_free_copy_area (copyarea);
	  copyarea = NULL;

	  if (scan == S_DOESNT_FIT)
	    {
	      if (copyarea_length < (-new_recdes.length))
		{
		  copyarea_length = -new_recdes.length;
		}
	      else
		{
		  copyarea_length += DB_PAGESIZE;
		}
	    }
	}
    }

  if (scan != S_SUCCESS)
    {
      error_code = ER_FAILED;
      goto end;
    }

  error_code = locator_delete_force (thread_p, hfid, oid, true,
				     SINGLE_ROW_DELETE, scan_cache,
				     force_count);
  if (error_code == NO_ERROR)
    {
      error_code = locator_insert_force (thread_p, newhfid, &newoid,
					 &new_recdes, true, SINGLE_ROW_INSERT,
					 scan_cache, force_count);
    }
  if (copyarea != NULL)
    {
      locator_free_copy_area (copyarea);
    }

end:

  attr_info->last_classrepr->id = old_reprid;
  COPY_OID (&attr_info->class_oid, &oldoid);
  attr_info->last_cacheindex = old_cache;

  return error_code;
}

/*
 * locator_was_index_already_applied () - Check B-Tree was already added
 *                                        or removed entries
 *
 * return: true if index was already applied
 *
 *   index_attrinfo(in): information of indices
 *   btid(in): btid of index
 *   pos(in): index position on indices
 *
 * Note: B-Tree can be shared by constraints (PK, or FK). The shared B-Tree can
 * be added or removed entries one more times during one INSERT or DELETE
 * statement is executing. Therefore, we need to check B-Tree was already added
 * or removed entries.
 */
static bool
locator_was_index_already_applied (HEAP_CACHE_ATTRINFO * index_attrinfo,
				   BTID * btid, int pos)
{
  OR_INDEX *index;
  int i;

  for (i = 0; i < pos; i++)
    {
      index = &(index_attrinfo->last_classrepr->indexes[i]);
      if (BTID_IS_EQUAL (btid, &index->btid))
	{
	  return true;
	}
    }

  return false;
}

/*
 * locator_add_or_remove_index () - Add or remove index entries
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   recdes(in): The object
 *   inst_oid(in): The object identifier
 *   class_oid(in): The class object identifier
 *   is_insert(in): whether to add or remove the object from the indexes
 *   op_type(in):
 *   scan_cache(in):
 *   datayn(in): true if the target object is "data",
 *                false if the target object is "schema"
 *   need_replication(in): true if replication is needed
 *   hfid(in):
 *
 * Note:Either insert indices (in_insert) or delete indices.
 */
int
locator_add_or_remove_index (THREAD_ENTRY * thread_p, RECDES * recdes,
			     OID * inst_oid, OID * class_oid, int is_insert,
			     int op_type, HEAP_SCANCACHE * scan_cache,
			     bool datayn, bool need_replication, HFID * hfid)
{
  int num_found;
  int i, num_btids;
  HEAP_CACHE_ATTRINFO index_attrinfo;
  BTID btid;
  DB_VALUE *key_dbvalue, *key_ins_del = NULL;
  DB_VALUE dbvalue;
  int unique;
  BTREE_UNIQUE_STATS *unique_stat_info;
  HEAP_IDX_ELEMENTS_INFO idx_info;
  char buf[DBVAL_BUFSIZE + MAX_ALIGNMENT], *aligned_buf;
  OR_INDEX *index;
  int error_code = NO_ERROR;

  aligned_buf = PTR_ALIGN (buf, MAX_ALIGNMENT);

  /*
   *  Populate the index_attrinfo structure.
   *  Return the number of indexed attributes found.
   */
  num_found = heap_attrinfo_start_with_index (thread_p, class_oid, NULL,
					      &index_attrinfo, &idx_info);
  num_btids = idx_info.num_btids;

  if (num_found == 0)
    {
      return NO_ERROR;
    }
  else if (num_found < 0)
    {
      return ER_FAILED;
    }

  /*
   *  At this point, there are indices and the index attrinfo has
   *  been initialized
   *
   *  Read the values of the indexed attributes
   */
  if (idx_info.has_single_col)
    {
      error_code = heap_attrinfo_read_dbvalues (thread_p, inst_oid, recdes,
						&index_attrinfo);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

  for (i = 0; i < num_btids; i++)
    {
      /*
       *  Generate a B-tree key contained in a DB_VALUE and return a
       *  pointer to it.
       */
      key_dbvalue = heap_attrvalue_get_key (thread_p, i, &index_attrinfo,
					    recdes, &btid, &dbvalue,
					    aligned_buf);
      if (key_dbvalue == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      index = &(index_attrinfo.last_classrepr->indexes[i]);
      if (!locator_Dont_check_foreign_key && index->type == BTREE_PRIMARY_KEY
	  && index->fk)
	{
	  /* actually key_prefix_length is -1 for BTREE_PRIMARY_KEY;
	   * must be changed when key_prefix_length will be added to FK and PK
	   */
	  if (is_insert)
	    {
	      error_code = locator_repair_object_cache (thread_p, index,
							key_dbvalue,
							inst_oid);
	      if (error_code != NO_ERROR)
		{
		  goto error;
		}
	    }
	  else
	    {
	      error_code = locator_check_primary_key_delete (thread_p,
							     index,
							     key_dbvalue);
	      if (error_code != NO_ERROR)
		{
		  goto error;
		}
	    }
	}

      if (i < 1 || !locator_was_index_already_applied (&index_attrinfo,
						       &index->btid, i))
	{
	  if (scan_cache == NULL)
	    {
	      unique_stat_info = NULL;
	    }
	  else
	    {
	      if (op_type == MULTI_ROW_UPDATE || op_type == MULTI_ROW_INSERT
		  || op_type == MULTI_ROW_DELETE)
		{
		  unique_stat_info = &(scan_cache->index_stat_info[i]);
		}
	      else
		{
		  unique_stat_info = NULL;
		}
	    }

	  if (is_insert)
	    {
	      key_ins_del = btree_insert (thread_p, &btid, key_dbvalue,
					  class_oid, inst_oid, op_type,
					  unique_stat_info, &unique);
	    }
	  else
	    {
	      key_ins_del = btree_delete (thread_p, &btid, key_dbvalue,
					  class_oid, inst_oid, &unique,
					  op_type, unique_stat_info);
	    }
	}

      /*
       * for replication,
       * Following step would be executed only when the target index is a
       * primary key.
       * The right place to insert a replication log info is here
       * to avoid another "fetching key values"
       * Generates the replication log info. for data insert/delete
       * for the update cases, refer to locator_update_force()
       */
      if (db_Enable_replications > 0 && need_replication
	  && index->type == BTREE_PRIMARY_KEY
	  && repl_class_is_replicated (class_oid)
	  && key_ins_del != NULL && !LOG_CHECK_LOG_APPLIER (thread_p))
	{
	  error_code = repl_log_insert (thread_p, class_oid, inst_oid,
					datayn ? LOG_REPLICATION_DATA :
					LOG_REPLICATION_SCHEMA,
					is_insert ? RVREPL_DATA_INSERT :
					RVREPL_DATA_DELETE, key_dbvalue,
					REPL_INFO_TYPE_STMT_NORMAL);
	}

      if (key_dbvalue == &dbvalue)
	{
	  pr_clear_value (&dbvalue);
	}

      if (key_ins_del == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
    }

error:

  heap_attrinfo_end (thread_p, &index_attrinfo);

  return error_code;
}

/*
 * locator_make_midxkey_domain () -
 *
 * return:
 *
 *   index(in):
 */
static TP_DOMAIN *
locator_make_midxkey_domain (OR_INDEX * index)
{
  TP_DOMAIN *set_domain;
  TP_DOMAIN *domain = NULL;

  OR_ATTRIBUTE **atts;
  int num_atts, i;

  if (index == NULL || index->n_atts < 2)
    {
      return NULL;
    }

  num_atts = index->n_atts;
  atts = index->atts;

  set_domain = NULL;
  for (i = 0; i < num_atts; i++)
    {
      if (i == 0)
	{
	  set_domain = tp_domain_copy (atts[i]->domain, 0);
	  if (set_domain == NULL)
	    {
	      return NULL;
	    }
	  domain = set_domain;
	}
      else
	{
	  domain->next = tp_domain_copy (atts[i]->domain, 0);
	  if (domain->next == NULL)
	    {
	      goto error;
	    }
	  domain = domain->next;
	}
    }

  domain = tp_domain_construct (DB_TYPE_MIDXKEY, (DB_OBJECT *) 0, 0, 0,
				set_domain);

  if (domain)
    {
      return tp_domain_cache (domain);
    }
  else
    {
      goto error;
    }

error:

  if (set_domain)
    {
      TP_DOMAIN *td, *next;

      /* tp_domain_free(set_domain); */
      for (td = set_domain, next = NULL; td != NULL; td = next)
	{
	  next = td->next;
	  tp_domain_free (td);
	}
    }

  return NULL;
}

/*
 * locator_update_index () - Update index entries
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   new_recdes(in): The new recdes object
 *   old_recdes(in): The old recdes object
 *   att_id(in): Updated attr id array
 *   n_att_id(in): Updated attr id array length
 *   inst_oid(in): The object identifier
 *   class_oid(in): The class object identifier
 *   op_type(in):
 *   scan_cache(in):
 *   data_update(in):
 *   need_replication(in): true if replication is needed.
 *
 * Note: Updatet the index entries of the given object.
 */
int
locator_update_index (THREAD_ENTRY * thread_p, RECDES * new_recdes,
		      RECDES * old_recdes, ATTR_ID * att_id, int n_att_id,
		      OID * inst_oid, OID * class_oid, int op_type,
		      HEAP_SCANCACHE * scan_cache, bool data_update,
		      bool need_replication, REPL_INFO_TYPE repl_info)
{
  HEAP_CACHE_ATTRINFO space_attrinfo[2];
  HEAP_CACHE_ATTRINFO *new_attrinfo = NULL;
  HEAP_CACHE_ATTRINFO *old_attrinfo = NULL;
  int new_num_found, old_num_found;
  BTID new_btid, old_btid;
  BTID *tmp_btid;
  int pk_btid_index = -1;
  DB_VALUE *new_key = NULL, *old_key = NULL;
  DB_VALUE *repl_old_key = NULL;
  DB_VALUE new_dbvalue, old_dbvalue;
  bool new_isnull, old_isnull;
  PR_TYPE *pr_type;
  OR_INDEX *index = NULL;
  int i, j, k, num_btids, old_num_btids, unique;
  bool found_btid = true;
  BTREE_UNIQUE_STATS *unique_stat_info;
  HEAP_IDX_ELEMENTS_INFO new_idx_info;
  HEAP_IDX_ELEMENTS_INFO old_idx_info;
  char newbuf[DBVAL_BUFSIZE + MAX_ALIGNMENT], *aligned_newbuf;
  char oldbuf[DBVAL_BUFSIZE + MAX_ALIGNMENT], *aligned_oldbuf;
  DB_TYPE dbval_type;
  int error_code;

  aligned_newbuf = PTR_ALIGN (newbuf, MAX_ALIGNMENT);
  aligned_oldbuf = PTR_ALIGN (oldbuf, MAX_ALIGNMENT);

  new_num_found = heap_attrinfo_start_with_index (thread_p, class_oid, NULL,
						  &space_attrinfo[0],
						  &new_idx_info);
  num_btids = new_idx_info.num_btids;
  if (new_num_found < 0)
    {
      return ER_FAILED;
    }
  new_attrinfo = &space_attrinfo[0];

  old_num_found = heap_attrinfo_start_with_index (thread_p, class_oid, NULL,
						  &space_attrinfo[1],
						  &old_idx_info);
  old_num_btids = old_idx_info.num_btids;
  if (old_num_found < 0)
    {
      error_code = ER_FAILED;
      goto error;
    }
  old_attrinfo = &space_attrinfo[1];

  if (new_num_found != old_num_found)
    {
      if (new_num_found > 0)
	{
	  heap_attrinfo_end (thread_p, &space_attrinfo[0]);
	}
      if (old_num_found > 0)
	{
	  heap_attrinfo_end (thread_p, &space_attrinfo[1]);
	}
      return ER_FAILED;
    }

  if (new_num_found == 0)
    {
      return NO_ERROR;
    }

  /*
   * There are indices and the index attrinfo has been initialized
   * Indices must be updated when the indexed attributes have changed in value
   * Get the new and old values of key and update the index when
   * the keys are different
   */

  new_attrinfo = &space_attrinfo[0];
  old_attrinfo = &space_attrinfo[1];

  error_code = heap_attrinfo_read_dbvalues (thread_p, inst_oid, new_recdes,
					    new_attrinfo);
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = heap_attrinfo_read_dbvalues (thread_p, inst_oid, old_recdes,
					    old_attrinfo);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  /*
   *  Ensure that we have the same nunber of indexes and
   *  get the number of B-tree IDs.
   */
  if (old_attrinfo->last_classrepr->n_indexes !=
      new_attrinfo->last_classrepr->n_indexes)
    {
      error_code = ER_FAILED;
      goto error;
    }

  for (i = 0; i < num_btids; i++)
    {
      if (pk_btid_index == -1 && db_Enable_replications > 0
	  && need_replication && !LOG_CHECK_LOG_APPLIER (thread_p))
	{
	  tmp_btid = heap_indexinfo_get_btid (i, new_attrinfo);
	  if (tmp_btid != NULL
	      && repl_class_is_replicated (class_oid)
	      && BTREE_IS_PRIMARY_KEY (xbtree_get_unique (thread_p,
							  tmp_btid)))
	    {
	      pk_btid_index = i;
	    }
	}
      index = &(new_attrinfo->last_classrepr->indexes[i]);

      /* check for specified update attributes */
      if (att_id)
	{
	  found_btid = false;	/* geuss as not found */

	  for (j = 0; j < n_att_id && !found_btid; j++)
	    {
	      for (k = 0; k < index->n_atts && !found_btid; k++)
		{
		  if (att_id[j] == (ATTR_ID) (index->atts[k]->id))
		    {		/* the index key_type has updated attr */
		      found_btid = true;
		    }
		}
	    }

	  if (!found_btid)
	    {
	      continue;		/* skip and go ahead */
	    }
	}

      new_key = heap_attrvalue_get_key (thread_p, i, new_attrinfo, new_recdes,
					&new_btid, &new_dbvalue,
					aligned_newbuf);
      old_key = heap_attrvalue_get_key (thread_p, i, old_attrinfo, old_recdes,
					&old_btid, &old_dbvalue,
					aligned_oldbuf);

      if ((new_key == NULL) || (old_key == NULL))
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      dbval_type = DB_VALUE_DOMAIN_TYPE (old_key);
      if (DB_VALUE_DOMAIN_TYPE (new_key) != dbval_type)
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      if (scan_cache == NULL)
	{
	  unique_stat_info = NULL;
	}
      else
	{
	  if (op_type == MULTI_ROW_UPDATE || op_type == MULTI_ROW_INSERT
	      || op_type == MULTI_ROW_DELETE)
	    {
	      unique_stat_info = &(scan_cache->index_stat_info[i]);
	    }
	  else
	    {
	      unique_stat_info = NULL;
	    }
	}

      new_isnull = db_value_is_null (new_key);
      old_isnull = db_value_is_null (old_key);
      pr_type = PR_TYPE_FROM_ID (dbval_type);
      if (pr_type == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      if (pr_type->id == DB_TYPE_MIDXKEY)
	{
	  new_key->data.midxkey.domain =
	    old_key->data.midxkey.domain =
	    locator_make_midxkey_domain (&(old_attrinfo->last_classrepr->
					   indexes[i]));
	}

      if ((new_isnull && !old_isnull)
	  || (old_isnull && !new_isnull)
	  || (!(new_isnull && old_isnull)
	      && ((*(pr_type->cmpval))
		  (old_key, new_key, NULL, 0, 0, 1, NULL) != DB_EQ)))
	{
	  if (!locator_Dont_check_foreign_key
	      && index->type == BTREE_PRIMARY_KEY && index->fk)
	    {
	      error_code = locator_check_primary_key_update (thread_p,
							     index, old_key);
	      if (error_code != NO_ERROR)
		{
		  goto error;
		}

	      error_code = locator_repair_object_cache (thread_p, index,
							new_key, inst_oid);
	      if (error_code != NO_ERROR)
		{
		  goto error;
		}
	    }

	  if (i < 1 || !locator_was_index_already_applied (new_attrinfo,
							   &index->btid, i))
	    {
	      error_code = btree_update (thread_p, &old_btid, old_key,
					 new_key,
					 class_oid, inst_oid,
					 op_type, unique_stat_info, &unique);

	      if (error_code != NO_ERROR)
		{
		  goto error;
		}
	    }
	}

      if (pk_btid_index == i && repl_old_key == NULL)
	{
	  repl_old_key = db_value_create ();
	  pr_clone_value (old_key, repl_old_key);
	}

      if (new_key == &new_dbvalue)
	{
	  pr_clear_value (&new_dbvalue);
	}
      if (old_key == &old_dbvalue)
	{
	  pr_clear_value (&old_dbvalue);
	}
    }

  if (pk_btid_index != -1)
    {
      if (repl_old_key == NULL)
	{
	  repl_old_key = heap_attrvalue_get_key (thread_p, pk_btid_index,
						 old_attrinfo, old_recdes,
						 &old_btid, &old_dbvalue,
						 aligned_oldbuf);
	  if (repl_old_key == NULL)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }

	  old_isnull = db_value_is_null (repl_old_key);
	  pr_type = pr_type_from_id (DB_VALUE_DOMAIN_TYPE (repl_old_key));
	  if (pr_type == NULL)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }

	  if (pr_type->id == DB_TYPE_MIDXKEY)
	    {
	      repl_old_key->data.midxkey.domain =
		locator_make_midxkey_domain (&(old_attrinfo->last_classrepr->
					       indexes[pk_btid_index]));
	    }

	  error_code = repl_log_insert (thread_p, class_oid, inst_oid,
					LOG_REPLICATION_DATA,
					RVREPL_DATA_UPDATE, repl_old_key,
					repl_info);
	  if (repl_old_key == &old_dbvalue)
	    {
	      pr_clear_value (&old_dbvalue);
	    }
	}
      else
	{
	  error_code = repl_log_insert (thread_p, class_oid, inst_oid,
					LOG_REPLICATION_DATA,
					RVREPL_DATA_UPDATE, repl_old_key,
					repl_info);
	  db_value_free (repl_old_key);
	}
    }

  heap_attrinfo_end (thread_p, new_attrinfo);
  heap_attrinfo_end (thread_p, old_attrinfo);

  return error_code;

error:

  if (new_key == &new_dbvalue)
    {
      pr_clear_value (&new_dbvalue);
    }
  if (old_key == &old_dbvalue)
    {
      pr_clear_value (&old_dbvalue);
    }

  /* Deallocate any index_list .. if any */

  if (new_attrinfo != NULL)
    {
      heap_attrinfo_end (thread_p, new_attrinfo);
    }

  if (old_attrinfo != NULL)
    {
      heap_attrinfo_end (thread_p, old_attrinfo);
    }

  return error_code;
}

/*
 * xlocator_remove_class_from_index () - Removes class instances from the B-tree
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   class_oid(in): The class object identifier
 *   btid(in): B-tree ID
 *   hfid(in): Heap ID
 *
 * Note: This function searches for instances belonging to the passes
 *      class OID and removes the ones found.  This function is used to
 *      remove a class from a spanning B-tree such as a UNIQUE.
 */
int
xlocator_remove_class_from_index (THREAD_ENTRY * thread_p, OID * class_oid,
				  BTID * btid, HFID * hfid)
{
  HEAP_CACHE_ATTRINFO index_attrinfo;
  HEAP_SCANCACHE scan_cache;
  OID inst_oid;
  int key_index, i, num_btids, num_found, dummy, key_found;
  RECDES copy_rec;
  BTID inst_btid;
  DB_VALUE dbvalue;
  DB_VALUE *dbvalue_ptr = NULL;
  DB_VALUE *key_del = NULL;
  SCAN_CODE scan;
  char *new_area;
  BTREE_UNIQUE_STATS unique_info;
  HEAP_IDX_ELEMENTS_INFO idx_info;
  char buf[DBVAL_BUFSIZE + MAX_ALIGNMENT], *aligned_buf;
  int error_code = NO_ERROR;

  aligned_buf = PTR_ALIGN (buf, MAX_ALIGNMENT);

  /* allocate memory space for copying an instance image. */
  copy_rec.area_size = DB_PAGESIZE;
  copy_rec.data = (char *) malloc (copy_rec.area_size);
  if (copy_rec.data == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      copy_rec.area_size);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize 'unique_info' structure. */
  BTID_COPY (&(unique_info.btid), btid);
  unique_info.num_nulls = 0;
  unique_info.num_keys = 0;
  unique_info.num_oids = 0;

  /* Start a scan cursor */
  error_code = heap_scancache_start (thread_p, &scan_cache, hfid,
				     class_oid, false, false, LOCKHINT_NONE);
  if (error_code != NO_ERROR)
    {
      free_and_init (copy_rec.data);
      return error_code;
    }

  /*
   *  Populate the index_attrinfo structure.
   *  Return the number of indexed attributes found.
   */
  num_found = heap_attrinfo_start_with_index (thread_p, class_oid, NULL,
					      &index_attrinfo, &idx_info);
  num_btids = idx_info.num_btids;
  if (num_found < 1)
    {
      (void) heap_scancache_end (thread_p, &scan_cache);
      free_and_init (copy_rec.data);
      return ER_FAILED;
    }

  /* Loop over each instance of the class found in the heap */
  inst_oid.volid = hfid->vfid.volid;
  inst_oid.pageid = NULL_PAGEID;
  inst_oid.slotid = NULL_SLOTID;
  key_found = false;
  key_index = 0;

  while (true)
    {
      scan = heap_next (thread_p, hfid, class_oid, &inst_oid, &copy_rec,
			&scan_cache, COPY);
      if (scan != S_SUCCESS)
	{
	  if (scan != S_DOESNT_FIT)
	    {
	      break;
	    }
	  new_area = (char *) realloc (copy_rec.data, -(copy_rec.length));
	  if (new_area == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      copy_rec.data, -(copy_rec.length));
	      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  copy_rec.area_size = -copy_rec.length;
	  copy_rec.data = new_area;
	  continue;
	}

      error_code = heap_attrinfo_read_dbvalues (thread_p, &inst_oid,
						&copy_rec, &index_attrinfo);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      /* Find the correct key by matching the BTID */
      if (key_found == false)
	{
	  for (i = 0; i < num_btids; i++)
	    {
	      dbvalue_ptr = heap_attrvalue_get_key (thread_p, i,
						    &index_attrinfo,
						    &copy_rec, &inst_btid,
						    &dbvalue, aligned_buf);
	      if (dbvalue_ptr == NULL)
		{
		  continue;
		}

	      if (BTID_IS_EQUAL (btid, &inst_btid))
		{
		  key_found = true;
		  key_index = i;
		  break;
		}
	    }
	}
      /*
       * We already know the correct BTID index (key_index) so just use it
       * to retrieve the key
       */
      else
	{
	  dbvalue_ptr = heap_attrvalue_get_key (thread_p, key_index,
						&index_attrinfo, &copy_rec,
						&inst_btid, &dbvalue,
						aligned_buf);
	}

      /* Delete the instance from the B-tree */
      if (!key_found)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
      key_del = btree_delete (thread_p, btid, dbvalue_ptr,
			      class_oid,
			      &inst_oid, &dummy, MULTI_ROW_DELETE,
			      &unique_info);
    }

  if (unique_info.num_nulls != 0 || unique_info.num_keys != 0
      || unique_info.num_oids != 0)
    {
      /* reflect local statistical information 'unique_info'
       * into global statistical information kept in root page.
       */
      error_code = btree_reflect_unique_statistics (thread_p, &unique_info);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

  error_code = heap_scancache_end (thread_p, &scan_cache);
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  heap_attrinfo_end (thread_p, &index_attrinfo);

  if (copy_rec.data != NULL)
    {
      free_and_init (copy_rec.data);
    }

  if (dbvalue_ptr == &dbvalue)
    {
      pr_clear_value (dbvalue_ptr);
    }

  return error_code;

error:

  (void) heap_scancache_end (thread_p, &scan_cache);
  heap_attrinfo_end (thread_p, &index_attrinfo);
  if (copy_rec.data != NULL)
    {
      free_and_init (copy_rec.data);
    }
  if (dbvalue_ptr == &dbvalue)
    {
      pr_clear_value (dbvalue_ptr);
    }

  return error_code;
}

/*
 * locator_notify_decache  () - Notify of a decache
 *
 * return:
 *
 *   oid(in): Oid to decache
 *   notify_area(in): Information used for notification purposes
 *
 * Note: Add an entry in the fetch area with respect to decaching an
 *              object at the workspace.
 */
static bool
locator_notify_decache (const OID * oid, void *notify_area)
{
  LC_COPYAREA_DESC *notify;
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area */
  int i;

  notify = (LC_COPYAREA_DESC *) notify_area;
  if (notify->recdes->area_size <= SSIZEOF (**notify->obj))
    {
      return false;
    }

  /*
   * Make sure that the object is not already part of the notification area
   */
  obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (notify->mobjs);
  obj = LC_PRIOR_ONEOBJ_PTR_IN_COPYAREA (obj);
  for (i = 0; i < notify->mobjs->num_objs; i++)
    {
      obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);
      if (OID_EQ (&obj->oid, oid))
	{
	  /* The object is already in the notification/fetch area */
	  obj->operation = LC_FETCH_DECACHE_LOCK;
	  return true;
	}
    }
  /*
   * The object was not part of the notification/fetch area
   */
  notify->mobjs->num_objs++;
  COPY_OID (&((*notify->obj)->oid), oid);
  (*notify->obj)->has_index = false;
  (*notify->obj)->hfid = NULL_HFID;
  (*notify->obj)->length = -1;
  (*notify->obj)->operation = LC_FETCH_DECACHE_LOCK;

  (*notify->obj)->offset = -1;
  *notify->obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (*notify->obj);
  notify->recdes->area_size -= sizeof (**notify->obj);

  return true;
}

/*
 * xlocator_notify_isolation_incons () - Synchronize possible inconsistencies related
 *                                  to non two phase locking
 *
 * return:
 *
 *   synch_area(in): Pointer to area where the name of the objects are placed.
 *
 * Note: Notify all inconsistencies related to the transaction
 *              isolation level.
 */
bool
xlocator_notify_isolation_incons (THREAD_ENTRY * thread_p,
				  LC_COPYAREA ** synch_area)
{
  LC_COPYAREA_DESC prefetch_des;	/* Descriptor for decache of
					 * objects related to transaction
					 * isolation level */
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in
				 * area */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area    */
  RECDES recdes;		/* Record descriptor for
				 * insertion */
  int offset;			/* Place to store next object in area */
  bool more_synch = false;

  *synch_area = locator_allocate_copy_area_by_length (DB_PAGESIZE, CLEAR_MEM);
  if (*synch_area == NULL)
    {
      return false;
    }

  mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (*synch_area);
  LC_RECDES_IN_COPYAREA (*synch_area, &recdes);
  obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);
  mobjs->num_objs = 0;
  offset = 0;

  prefetch_des.mobjs = mobjs;
  prefetch_des.obj = &obj;
  prefetch_des.offset = &offset;
  prefetch_des.recdes = &recdes;
  lock_notify_isolation_incons (thread_p, locator_notify_decache,
				&prefetch_des);
  if (mobjs->num_objs == 0)
    {
      /*
       * Don't need to notify of any client workspace lock decaches
       * (i.e., possible object inconsistencies).
       */
      locator_free_copy_area (*synch_area);
      *synch_area = NULL;
    }
  else if (recdes.area_size >= SSIZEOF (*obj))
    {
      more_synch = true;
    }
  else
    {
      lock_unlock_by_isolation_level (thread_p);
    }

  return more_synch;
}

static DISK_ISVALID
locator_repair_btree_by_insert (THREAD_ENTRY * thread_p, OID * class_oid,
				BTID * btid, DB_VALUE * key, OID * inst_oid)
{
  DISK_ISVALID isvalid = DISK_INVALID;
  LOG_LSA lsa;
#if defined(SERVER_MODE)
  int tran_index;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
#endif /* SERVER_MODE */

  if (lock_object (thread_p, inst_oid, class_oid, X_LOCK,
		   LK_UNCOND_LOCK) != LK_GRANTED)
    {
      return DISK_INVALID;
    }

  if (xtran_server_start_topop (thread_p, &lsa) == NO_ERROR)
    {
      if (btree_insert (thread_p, btid, key,
			class_oid, inst_oid, SINGLE_ROW_INSERT, NULL,
			NULL) != NULL)
	{
	  isvalid = DISK_VALID;
	  xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_COMMIT, &lsa);
	}
      else
	{
	  xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
	}
    }

#if defined(SERVER_MODE)
  lock_remove_all_inst_locks (thread_p, tran_index, class_oid, X_LOCK);
#endif /* SERVER_MODE */

  return isvalid;
}

static DISK_ISVALID
locator_repair_btree_by_delete (THREAD_ENTRY * thread_p, OID * class_oid,
				BTID * btid, OID * inst_oid)
{
  DB_VALUE key;
  bool clear_key = false;
  int unique;
  LOG_LSA lsa;
  DISK_ISVALID isvalid = DISK_INVALID;
#if defined(SERVER_MODE)
  int tran_index;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
#endif /* SERVER_MODE */

  if (btree_find_key (thread_p, btid, inst_oid, &key,
		      &clear_key) != DISK_VALID)
    {
      return DISK_INVALID;
    }

  if (lock_object (thread_p, inst_oid, class_oid, X_LOCK, LK_UNCOND_LOCK)
      == LK_GRANTED)
    {
      if (xtran_server_start_topop (thread_p, &lsa) == NO_ERROR)
	{
	  if (btree_delete (thread_p, btid, &key,
			    class_oid, inst_oid, &unique, SINGLE_ROW_DELETE,
			    NULL) != NULL)
	    {
	      isvalid = DISK_VALID;
	      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_COMMIT,
				      &lsa);
	    }
	  else
	    {
	      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
	    }
	}

#if defined(SERVER_MODE)
      lock_remove_all_inst_locks (thread_p, tran_index, class_oid, X_LOCK);
#endif /* SERVER_MODE */
    }

  if (clear_key)
    {
      pr_clear_value (&key);
    }

  return isvalid;
}

/*
 * locator_check_btree_entries () - Check consistency of btree entries and heap
 *
 * return: valid
 *
 *   btid(in): Btree identifer
 *   hfid(in): Heap identfier of the instances of class that are indexed
 *   class_oid(in): The class identifer
 *   n_attr_ids(in):  Number of attribute ids (size of the array).
 *   attr_ids(in): Attribute ID array.
 *   btname(in) :
 *   repair(in):
 *
 * Note: Check the consistency of the btree entries against the
 *              instances stored on heap and vive versa.
 */
DISK_ISVALID
locator_check_btree_entries (THREAD_ENTRY * thread_p, BTID * btid,
			     HFID * hfid, OID * class_oid, int n_attr_ids,
			     ATTR_ID * attr_ids,
			     int *atts_prefix_length,
			     const char *btname, bool repair)
{
  DISK_ISVALID isvalid = DISK_VALID;
  DISK_ISVALID isallvalid = DISK_VALID;
  OID inst_oid;
  RECDES peek;			/* Record descriptor for peeking object */
  SCAN_CODE scan;
  HEAP_SCANCACHE scan_cache;
  BTREE_CHECKSCAN bt_checkscan;
  BTREE_SCAN bt_scan;
  HEAP_CACHE_ATTRINFO attr_info;
  int num_btree_oids = 0;
  int num_heap_oids = 0;
  int oid_cnt;
  OID *oid_area = NULL;
  INDX_SCAN_ID isid;
  int i;
  DB_VALUE dbvalue;
  DB_VALUE *key = NULL;
  char buf[DBVAL_BUFSIZE + MAX_ALIGNMENT], *aligned_buf;
  char *class_name_p = NULL;

#if defined(SERVER_MODE)
  int tran_index;
#endif /* SERVER_MODE */

  aligned_buf = PTR_ALIGN (buf, MAX_ALIGNMENT);

#if defined(SERVER_MODE)
  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
#endif /* SERVER_MODE */

  /* Start a scan cursor and a class attribute information */
  if (heap_scancache_start (thread_p, &scan_cache, hfid, class_oid, true,
			    false, LOCKHINT_NONE) != NO_ERROR)
    {
      return DISK_ERROR;
    }

  if (heap_attrinfo_start (thread_p, class_oid, n_attr_ids, attr_ids,
			   &attr_info) != NO_ERROR)
    {
      (void) heap_scancache_end (thread_p, &scan_cache);
      return DISK_ERROR;
    }

  /*
   * Step 1) From Heap to B+tree
   */

  /* start a check-scan on index */
  if (btree_keyoid_checkscan_start (btid, &bt_checkscan) != NO_ERROR)
    {
      (void) heap_scancache_end (thread_p, &scan_cache);
      return DISK_ERROR;
    }

  inst_oid.volid = hfid->vfid.volid;
  inst_oid.pageid = NULL_PAGEID;
  inst_oid.slotid = NULL_SLOTID;

  while ((scan = heap_next (thread_p, hfid, class_oid, &inst_oid, &peek,
			    &scan_cache, PEEK)) == S_SUCCESS)
    {
      num_heap_oids++;

      /* Make sure that the index entry exist */
      if ((n_attr_ids == 1
	   && heap_attrinfo_read_dbvalues (thread_p, &inst_oid, &peek,
					   &attr_info) != NO_ERROR)
	  || (key = heap_attrinfo_generate_key (thread_p, n_attr_ids,
						attr_ids, atts_prefix_length,
						&attr_info,
						&peek, &dbvalue,
						aligned_buf)) == NULL)
	{
	  if (isallvalid != DISK_INVALID)
	    {
	      isallvalid = DISK_ERROR;
	    }
	}
      else
	{
	  if (db_value_is_null (key) || btree_multicol_key_is_null (key))
	    {
	      /* Do not check the btree since unbound values are not recorded */
	      num_heap_oids--;
	    }
	  else
	    {
	      isvalid = btree_keyoid_checkscan_check (thread_p, &bt_checkscan,
						      class_oid, key,
						      &inst_oid);
	      if (isvalid == DISK_INVALID)
		{
		  if (repair)
		    {
		      isvalid =
			locator_repair_btree_by_insert (thread_p, class_oid,
							btid, key, &inst_oid);
		    }

		  if (isvalid == DISK_INVALID)
		    {
		      char *key_dmp;

		      key_dmp = pr_valstring (key);

		      if (!OID_ISNULL (class_oid))
			{
			  class_name_p = heap_get_class_name (thread_p,
							      class_oid);
			}

		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE1, 12,
			      (btname) ? btname : "*UNKNOWN-INDEX*",
			      (class_name_p) ? class_name_p :
			      "*UNKNOWN-CLASS*",
			      class_oid->volid, class_oid->pageid,
			      class_oid->slotid,
			      (key_dmp) ? key_dmp : "_NULL_KEY",
			      inst_oid.volid, inst_oid.pageid,
			      inst_oid.slotid, btid->vfid.volid,
			      btid->vfid.fileid, btid->root_pageid);

		      if (key_dmp)
			{
			  free_and_init (key_dmp);
			}

		      if (class_name_p)
			{
			  free_and_init (class_name_p);
			}

		      if (isallvalid != DISK_INVALID)
			{
			  isallvalid = isvalid;
			}
		    }
		}
	    }
	}

      if (key == &dbvalue)
	{
	  pr_clear_value (key);
	}
    }

  if (scan != S_END && isallvalid != DISK_INVALID)
    {
      isallvalid = DISK_ERROR;
    }

  /* close the index check-scan */
  btree_keyoid_checkscan_end (&bt_checkscan);

  /* Finish scan cursor and class attribute cache information */
  heap_attrinfo_end (thread_p, &attr_info);

  /*
   * Step 2) From B+tree to Heap
   */

  BTREE_INIT_SCAN (&bt_scan);

  isid.oid_list.oid_cnt = 0;
  isid.oid_list.oidp = (OID *) malloc (ISCAN_OID_BUFFER_SIZE);
  if (isid.oid_list.oidp == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, ISCAN_OID_BUFFER_SIZE);
      return DISK_ERROR;
    }

  /* alloc index key copy_buf */
  isid.copy_buf = (char *) db_private_alloc (thread_p, DBVAL_BUFSIZE);
  if (isid.copy_buf == NULL)
    {
      free_and_init (isid.oid_list.oidp);
      return DISK_ERROR;
    }
  isid.copy_buf_len = DBVAL_BUFSIZE;

  if (heap_scancache_start (thread_p, &isid.scan_cache, hfid, class_oid, true,
			    true, LOCKHINT_NONE) != NO_ERROR)
    {
      free_and_init (isid.oid_list.oidp);
      /* free index key copy_buf */
      if (isid.copy_buf)
	{
	  db_private_free_and_init (thread_p, isid.copy_buf);
	}

      return DISK_ERROR;
    }

  do
    {
      /* search index */
      oid_cnt = btree_range_search (thread_p, btid, true, LOCKHINT_NONE,
				    &bt_scan, NULL, NULL, INF_INF,
				    1, class_oid,
				    isid.oid_list.oidp,
				    ISCAN_OID_BUFFER_SIZE,
				    NULL, &isid, true, false);
      if (oid_cnt == -1)
	{
	  break;
	}

      oid_area = isid.oid_list.oidp;
      num_btree_oids += oid_cnt;
      for (i = 0; i < oid_cnt; i++)
	{
	  if (heap_does_exist (thread_p, &oid_area[i], class_oid) == false)
	    {
	      isvalid = DISK_INVALID;

	      if (repair)
		{
		  isvalid =
		    locator_repair_btree_by_delete (thread_p, class_oid,
						    btid, &oid_area[i]);
		}

	      if (isvalid == DISK_VALID)
		{
		  num_btree_oids--;
		}
	      else
		{
		  if (!OID_ISNULL (class_oid))
		    {
		      class_name_p = heap_get_class_name (thread_p,
							  class_oid);
		    }

		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE2, 11,
			  (btname) ? btname : "*UNKNOWN-INDEX*",
			  (class_name_p) ? class_name_p : "*UNKNOWN-CLASS*",
			  class_oid->volid, class_oid->pageid,
			  class_oid->slotid, oid_area[i].volid,
			  oid_area[i].pageid, oid_area[i].slotid,
			  btid->vfid.volid, btid->vfid.fileid,
			  btid->root_pageid);

		  if (class_name_p)
		    {
		      free_and_init (class_name_p);
		    }

		  isallvalid = DISK_INVALID;
		}
	    }
	}

    }
  while (!BTREE_END_OF_SCAN (&bt_scan));

  if (heap_scancache_end (thread_p, &isid.scan_cache) != NO_ERROR)
    {
      isallvalid = DISK_INVALID;
    }

  free_and_init (isid.oid_list.oidp);
  /* free index key copy_buf */
  if (isid.copy_buf)
    {
      db_private_free_and_init (thread_p, isid.copy_buf);
    }

  if (num_heap_oids != num_btree_oids)
    {
      if (!OID_ISNULL (class_oid))
	{
	  class_name_p = heap_get_class_name (thread_p, class_oid);
	}

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE3, 10,
	      (btname) ? btname : "*UNKNOWN-INDEX*",
	      (class_name_p) ? class_name_p : "*UNKNOWN-CLASS*",
	      class_oid->volid, class_oid->pageid, class_oid->slotid,
	      num_heap_oids, num_btree_oids,
	      btid->vfid.volid, btid->vfid.fileid, btid->root_pageid);

      if (class_name_p)
	{
	  free_and_init (class_name_p);
	}
      isallvalid = DISK_INVALID;
    }

  if (heap_scancache_end (thread_p, &scan_cache) != NO_ERROR)
    {
      isallvalid = DISK_INVALID;
    }

  return isallvalid;
}

/*
 * locator_check_unique_btree_entries () - Check consistency of unique btree entries
 *                                    and heaps
 *
 * return: valid
 *
 *   btid(in): Btree identifer
 *   classrec(in):
 *   attr_ids(in): Array of indexed attributes for the btid
 *   repair(in):
 *
 * Note: Check the consistency of the unique btree entries against the
 *              instances stored on heap and vice versa.  Unique btrees are
 *              special because they span hierarchies and can have multiple
 *              heaps associated with them.
 */
static DISK_ISVALID
locator_check_unique_btree_entries (THREAD_ENTRY * thread_p, BTID * btid,
				    RECDES * classrec, ATTR_ID * attr_ids,
				    const char *btname, bool repair)
{
  DISK_ISVALID isvalid = DISK_VALID, isallvalid = DISK_VALID;
  OID inst_oid;
  RECDES peek;
  SCAN_CODE scan;
  HEAP_SCANCACHE *scan_cache = NULL;
  BTREE_CHECKSCAN bt_checkscan;
  BTREE_SCAN bt_scan;
  HEAP_CACHE_ATTRINFO attr_info;
  DB_VALUE *key = NULL;
  DB_VALUE dbvalue;
  int num_btree_oids = 0, num_heap_oids = 0, num_nulls = 0;
  int oid_cnt, btree_oid_cnt, btree_null_cnt, btree_key_cnt;
  OID *oid_area = NULL;
  int num_classes, scancache_inited = 0, attrinfo_inited = 0;
  int i, j, index_id;
  HFID *hfids = NULL, *hfid = NULL;
  OID *class_oids = NULL, *class_oid = NULL;
  INDX_SCAN_ID isid;
  char buf[DBVAL_BUFSIZE + MAX_ALIGNMENT], *aligned_buf;
  char *class_name_p = NULL;
#if defined(SERVER_MODE)
  int tran_index;
#endif /* SERVER_MODE */

  aligned_buf = PTR_ALIGN (buf, MAX_ALIGNMENT);

#if defined(SERVER_MODE)
  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
#endif /* SERVER_MODE */
  isid.oid_list.oidp = NULL;
  isid.copy_buf = NULL;
  isid.copy_buf_len = 0;

  /* get all the heap files associated with this unique btree */
  if ((or_get_unique_hierarchy (thread_p, classrec, attr_ids[0], btid,
				&class_oids, &hfids,
				&num_classes) != NO_ERROR)
      || class_oids == NULL || hfids == NULL || num_classes < 1)
    {
      if (class_oids != NULL)
	{
	  free_and_init (class_oids);
	}

      if (hfids != NULL)
	{
	  free_and_init (hfids);
	}

      return DISK_ERROR;
    }

  /*
   * Step 1) Check if all instances of all the heaps are in the unique btree.
   */

  scan_cache =
    (HEAP_SCANCACHE *) malloc (num_classes * sizeof (HEAP_SCANCACHE));
  if (scan_cache == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      num_classes * sizeof (HEAP_SCANCACHE));
      goto error;
    }

  for (j = 0; j < num_classes; j++)
    {
      hfid = &hfids[j];
      class_oid = &class_oids[j];

      /* Start a scan cursor and a class attribute information */
      if (heap_scancache_start (thread_p, &scan_cache[j], hfid, class_oid,
				true, false, LOCKHINT_NONE) != NO_ERROR)
	{
	  goto error;
	}
      scancache_inited++;

      index_id = heap_attrinfo_start_with_btid (thread_p, class_oid, btid,
						&attr_info);
      if (index_id < 0)
	{
	  goto error;
	}

      attrinfo_inited = 1;

      /* start a check-scan on index */
      if (btree_keyoid_checkscan_start (btid, &bt_checkscan) != NO_ERROR)
	{
	  goto error;
	}

      inst_oid.volid = hfid->vfid.volid;
      inst_oid.pageid = NULL_PAGEID;
      inst_oid.slotid = NULL_SLOTID;

      while ((scan = heap_next (thread_p, hfid, class_oid, &inst_oid,
				&peek, &scan_cache[j], PEEK)) == S_SUCCESS)
	{
	  num_heap_oids++;

	  /* Make sure that the index entry exists */
	  if ((heap_attrinfo_read_dbvalues (thread_p, &inst_oid, &peek,
					    &attr_info) != NO_ERROR)
	      || ((key = heap_attrvalue_get_key (thread_p, index_id,
						 &attr_info, &peek, btid,
						 &dbvalue,
						 aligned_buf)) == NULL))
	    {
	      if (isallvalid != DISK_INVALID)
		{
		  isallvalid = DISK_ERROR;
		}
	    }
	  else
	    {
	      if (db_value_is_null (key) || btree_multicol_key_is_null (key))
		{
		  num_nulls++;
		}
	      else
		{
		  isvalid = btree_keyoid_checkscan_check (thread_p,
							  &bt_checkscan,
							  class_oid,
							  key, &inst_oid);
		  if (isvalid == DISK_INVALID)
		    {
		      if (repair)
			{
			  isvalid =
			    locator_repair_btree_by_insert (thread_p,
							    class_oid, btid,
							    key, &inst_oid);
			}

		      if (isvalid == DISK_INVALID)
			{
			  char *key_dmp;

			  key_dmp = pr_valstring (key);
			  if (!OID_ISNULL (class_oid))
			    {
			      class_name_p =
				heap_get_class_name (thread_p, class_oid);
			    }

			  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				  ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE1,
				  12,
				  (btname) ? btname : "*UNKNOWN-INDEX*",
				  (class_name_p) ? class_name_p :
				  "*UNKNOWN-CLASS*", class_oid->volid,
				  class_oid->pageid, class_oid->slotid,
				  (key_dmp) ? key_dmp : "_NULL_KEY",
				  inst_oid.volid, inst_oid.pageid,
				  inst_oid.slotid, btid->vfid.volid,
				  btid->vfid.fileid, btid->root_pageid);

			  if (key_dmp)
			    {
			      free_and_init (key_dmp);
			    }

			  if (class_name_p)
			    {
			      free_and_init (class_name_p);
			    }

			  if (isallvalid != DISK_INVALID)
			    {
			      isallvalid = isvalid;
			    }
			}
		    }
		}
	    }

	  if (key == &dbvalue)
	    {
	      pr_clear_value (key);
	    }
	}

      if (scan != S_END && isallvalid != DISK_INVALID)
	{
	  isallvalid = DISK_ERROR;
	}

      /* close the index check-scan */
      btree_keyoid_checkscan_end (&bt_checkscan);

      /* Finish scan cursor and class attribute cache information */
      heap_attrinfo_end (thread_p, &attr_info);
      attrinfo_inited = 0;
    }

  /*
   * Step 2) Check that all the btree entries are members of one of the heaps.
   */

  BTREE_INIT_SCAN (&bt_scan);

  isid.oid_list.oid_cnt = 0;
  isid.oid_list.oidp = (OID *) malloc (ISCAN_OID_BUFFER_SIZE);
  if (isid.oid_list.oidp == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, ISCAN_OID_BUFFER_SIZE);
      goto error;
    }
  /* alloc index key copy_buf */
  isid.copy_buf = (char *) db_private_alloc (thread_p, DBVAL_BUFSIZE);
  if (isid.copy_buf == NULL)
    {
      goto error;
    }
  isid.copy_buf_len = DBVAL_BUFSIZE;

  if (heap_scancache_start (thread_p, &isid.scan_cache, hfid, class_oid, true,
			    true, LOCKHINT_NONE) != NO_ERROR)
    {
      goto error;
    }

  do
    {
      /* search index */
      oid_cnt = btree_range_search (thread_p, btid, true, LOCKHINT_NONE,
				    &bt_scan, NULL, NULL, INF_INF,
				    0, (OID *) NULL, isid.oid_list.oidp,
				    ISCAN_OID_BUFFER_SIZE,
				    NULL, &isid, true, false);
      /* TODO: unique with prefix length */

      if (oid_cnt == -1)
	{
	  break;
	}

      oid_area = isid.oid_list.oidp;

      num_btree_oids += oid_cnt;
      for (i = 0; i < oid_cnt; i++)
	{
	  if (heap_does_exist (thread_p, &oid_area[i], NULL) == false)
	    {
	      isvalid = DISK_INVALID;
	      if (repair)
		{
		  isvalid =
		    locator_repair_btree_by_delete (thread_p, class_oid,
						    btid, &oid_area[i]);
		}

	      if (isvalid == DISK_VALID)
		{
		  num_btree_oids--;
		}
	      else
		{
		  if (!OID_ISNULL (class_oid))
		    {
		      class_name_p = heap_get_class_name (thread_p,
							  class_oid);
		    }

		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE2, 11,
			  (btname) ? btname : "*UNKNOWN-INDEX*",
			  (class_name_p) ? class_name_p :
			  "*UNKNOWN-CLASS*", class_oid->volid,
			  class_oid->pageid, class_oid->slotid,
			  oid_area[i].volid, oid_area[i].pageid,
			  oid_area[i].slotid, btid->vfid.volid,
			  btid->vfid.fileid, btid->root_pageid);

		  if (class_name_p)
		    {
		      free_and_init (class_name_p);
		    }

		  isallvalid = DISK_INVALID;
		}
	    }
	  else
	    {
	      OID cl_oid;
	      int found = 0;
	      /*
	       * check to make sure that the OID is one of the OIDs from our
	       * list of classes.
	       */
	      if (heap_get_class_oid (thread_p, &oid_area[i], &cl_oid) ==
		  NULL)
		{
		  (void) heap_scancache_end (thread_p, &isid.scan_cache);
		  goto error;
		}

	      for (j = 0, found = 0;
		   found == 0 && class_oids != NULL && j < num_classes; j++)
		{
		  if (OID_EQ (&cl_oid, &(class_oids[j])))
		    {
		      found = 1;
		    }
		}

	      if (!found)
		{
		  if (!OID_ISNULL (class_oid))
		    {
		      class_name_p = heap_get_class_name (thread_p,
							  class_oid);
		    }

		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE8, 11,
			  (btname) ? btname : "*UNKNOWN-INDEX*",
			  (class_name_p) ? class_name_p :
			  "*UNKNOWN-CLASS*",
			  class_oid->volid, class_oid->pageid,
			  class_oid->slotid,
			  oid_area[i].volid,
			  oid_area[i].pageid,
			  oid_area[i].slotid, btid->vfid.volid,
			  btid->vfid.fileid, btid->root_pageid);

		  if (class_name_p)
		    {
		      free_and_init (class_name_p);
		    }
		  isallvalid = DISK_INVALID;
		}
	    }
	}
    }
  while (!BTREE_END_OF_SCAN (&bt_scan));

  free_and_init (isid.oid_list.oidp);
  /* free index key copy_buf */
  if (isid.copy_buf)
    {
      db_private_free_and_init (thread_p, isid.copy_buf);
    }

  if (heap_scancache_end (thread_p, &isid.scan_cache) != NO_ERROR)
    {
      goto error;
    }

  /* check to see that the btree root statistics are correct. */
  if (btree_get_unique_statistics (thread_p, btid, &btree_oid_cnt,
				   &btree_null_cnt,
				   &btree_key_cnt) != NO_ERROR)
    {
      goto error;
    }

  /* Do the numbers add up? */
  if (num_heap_oids != num_btree_oids + num_nulls)
    {
      if (!OID_ISNULL (class_oid))
	{
	  class_name_p = heap_get_class_name (thread_p, class_oid);
	}

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE4, 11,
	      (btname) ? btname : "*UNKNOWN-INDEX*",
	      (class_name_p) ? class_name_p :
	      "*UNKNOWN-CLASS*",
	      class_oid->volid, class_oid->pageid,
	      class_oid->slotid,
	      num_heap_oids, num_btree_oids, num_nulls,
	      btid->vfid.volid, btid->vfid.fileid, btid->root_pageid);

      if (class_name_p)
	{
	  free_and_init (class_name_p);
	}

      isallvalid = DISK_INVALID;
    }

  if (num_heap_oids != btree_oid_cnt)
    {
      if (!OID_ISNULL (class_oid))
	{
	  class_name_p = heap_get_class_name (thread_p, class_oid);
	}

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE5, 10,
	      (btname) ? btname : "*UNKNOWN-INDEX*",
	      (class_name_p) ? class_name_p :
	      "*UNKNOWN-CLASS*",
	      class_oid->volid, class_oid->pageid,
	      class_oid->slotid,
	      num_heap_oids, btree_oid_cnt,
	      btid->vfid.volid, btid->vfid.fileid, btid->root_pageid);

      if (class_name_p)
	{
	  free_and_init (class_name_p);
	}

      isallvalid = DISK_INVALID;
    }

  if (num_nulls != btree_null_cnt)
    {
      if (!OID_ISNULL (class_oid))
	{
	  class_name_p = heap_get_class_name (thread_p, class_oid);
	}

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE7, 10,
	      (btname) ? btname : "*UNKNOWN-INDEX*",
	      (class_name_p) ? class_name_p :
	      "*UNKNOWN-CLASS*",
	      class_oid->volid, class_oid->pageid,
	      class_oid->slotid,
	      num_nulls, btree_null_cnt,
	      btid->vfid.volid, btid->vfid.fileid, btid->root_pageid);

      if (class_name_p)
	{
	  free_and_init (class_name_p);
	}
      isallvalid = DISK_INVALID;
    }

  /* finally check if the btree thinks that it is unique */
  if (btree_oid_cnt != btree_null_cnt + btree_key_cnt)
    {
      if (!OID_ISNULL (class_oid))
	{
	  class_name_p = heap_get_class_name (thread_p, class_oid);
	}

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE6, 11,
	      (btname) ? btname : "*UNKNOWN-INDEX*",
	      (class_name_p) ? class_name_p :
	      "*UNKNOWN-CLASS*",
	      class_oid->volid, class_oid->pageid,
	      class_oid->slotid,
	      btree_oid_cnt, btree_null_cnt, btree_key_cnt,
	      btid->vfid.volid, btid->vfid.fileid, btid->root_pageid);

      if (class_name_p)
	{
	  free_and_init (class_name_p);
	}

      isallvalid = DISK_INVALID;
    }

  for (j = 0; j < scancache_inited; j++)
    {
      if (heap_scancache_end (thread_p, &scan_cache[j]) != NO_ERROR)
	{
	  goto error;
	}
    }

  if (scan_cache)
    {
      free_and_init (scan_cache);
    }

  free_and_init (class_oids);
  free_and_init (hfids);

  return isallvalid;

error:

  if (isid.oid_list.oidp)
    {
      free_and_init (isid.oid_list.oidp);
    }
  /* free index key copy_buf */
  if (isid.copy_buf)
    {
      db_private_free_and_init (thread_p, isid.copy_buf);
    }
  if (class_oids)
    {
      free_and_init (class_oids);
    }
  if (hfids)
    {
      free_and_init (hfids);
    }
  if (attrinfo_inited)
    {
      heap_attrinfo_end (thread_p, &attr_info);
    }
  for (j = 0; j < scancache_inited; j++)
    {
      (void) heap_scancache_end (thread_p, &scan_cache[j]);
    }
  if (scan_cache)
    {
      free_and_init (scan_cache);
    }

  return DISK_ERROR;
}

/*
 * locator_check_all_entries_of_all_btrees () - Check consistency of all entries of all
 *                                       btrees
 *
 * return: valid
 *
 *   repair(in):
 *
 * Note: Check the consistency of all entries of all btrees against the
 *              the corresponding heaps.
 */
DISK_ISVALID
locator_check_all_entries_of_all_btrees (THREAD_ENTRY * thread_p, bool repair)
{
  RECDES peek;			/* Record descriptor for peeking object */
  HFID *root_hfid;
  HFID class_hfid;
  OID class_oid;
  HEAP_SCANCACHE scan_cache;
  SCAN_CODE scan = S_SUCCESS;
  DISK_ISVALID isvalid = DISK_VALID;
  DISK_ISVALID isallvalid = DISK_VALID;
  HEAP_CACHE_ATTRINFO index_attrinfo;
  int i;
  int num_class_btids;
  HEAP_IDX_ELEMENTS_INFO idx_info;

  /*
   * Find all the classes.
   * If the class has an index, check the logical consistency of the index
   */

  /* Find the heap for the root classes */

  root_hfid = boot_find_root_heap ();
  if (heap_scancache_start (thread_p, &scan_cache, root_hfid,
			    oid_Root_class_oid, true, false,
			    LOCKHINT_NONE) != NO_ERROR)
    {
      return DISK_ERROR;
    }

  class_oid.volid = root_hfid->vfid.volid;
  class_oid.pageid = NULL_PAGEID;
  class_oid.slotid = NULL_SLOTID;

  while (isallvalid != DISK_ERROR
	 && (scan = heap_next (thread_p, root_hfid, oid_Root_class_oid,
			       &class_oid, &peek, &scan_cache,
			       PEEK)) == S_SUCCESS)
    {
      /* Any indices ? */

      orc_class_hfid_from_record (&peek, &class_hfid);
      if (!HFID_IS_NULL (&class_hfid))
	{
	  if (heap_attrinfo_start_with_index (thread_p, &class_oid, &peek,
					      &index_attrinfo, &idx_info) < 0)
	    {
	      isallvalid = DISK_ERROR;
	      break;
	    }
	  num_class_btids = idx_info.num_btids;

	  if (num_class_btids > 0)
	    {
	      BTID *btid;
	      ATTR_ID *attrids = NULL;
	      int n_attrs;
	      char *btname = NULL;
	      int *attrs_prefix_length = NULL;

	      /*
	       * Check the indices in this class
	       */
	      if (heap_attrinfo_clear_dbvalues (&index_attrinfo) != NO_ERROR)
		{
		  isallvalid = DISK_ERROR;
		  break;
		}

	      for (i = 0;
		   i < num_class_btids && isallvalid != DISK_ERROR; i++)
		{
		  btid = heap_indexinfo_get_btid (i, &index_attrinfo);
		  if (btid == NULL)
		    {
		      isallvalid = DISK_ERROR;
		      break;
		    }
		  n_attrs = heap_indexinfo_get_num_attrs (i, &index_attrinfo);
		  if (n_attrs <= 0)
		    {
		      isallvalid = DISK_ERROR;
		      break;
		    }

		  attrids = (ATTR_ID *) malloc (n_attrs * sizeof (ATTR_ID));
		  if (attrids == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1,
			      n_attrs * sizeof (ATTR_ID));
		      isallvalid = DISK_ERROR;
		      break;
		    }

		  if (heap_indexinfo_get_attrids (i, &index_attrinfo,
						  attrids) != NO_ERROR)
		    {
		      free_and_init (attrids);
		      isallvalid = DISK_ERROR;
		      break;
		    }

		  attrs_prefix_length =
		    (int *) malloc (n_attrs * sizeof (int));
		  if (attrs_prefix_length == NULL)
		    {
		      free_and_init (attrids);
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1,
			      n_attrs * sizeof (int));
		      isallvalid = DISK_ERROR;
		      break;
		    }

		  if (heap_indexinfo_get_attrs_prefix_length
		      (i, &index_attrinfo, attrs_prefix_length, n_attrs)
		      != NO_ERROR)
		    {
		      free_and_init (attrids);
		      free_and_init (attrs_prefix_length);
		      isallvalid = DISK_ERROR;
		      break;
		    }

		  if (heap_get_indexinfo_of_btid (thread_p, &class_oid, btid,
						  NULL, NULL, NULL, NULL,
						  &btname) != NO_ERROR)
		    {
		      free_and_init (attrids);
		      free_and_init (attrs_prefix_length);
		      isallvalid = DISK_ERROR;
		      break;
		    }

		  if (btree_is_unique (thread_p, btid))
		    {
		      isvalid = locator_check_unique_btree_entries (thread_p,
								    btid,
								    &peek,
								    attrids,
								    btname,
								    repair);
		      if (isvalid != DISK_VALID)
			{
			  isallvalid = isvalid;
			}
		    }
		  else
		    {
		      isvalid = locator_check_btree_entries (thread_p, btid,
							     &class_hfid,
							     &class_oid,
							     n_attrs,
							     attrids,
							     attrs_prefix_length,
							     btname, repair);
		      if (isvalid != DISK_VALID)
			{
			  isallvalid = isvalid;
			}
		    }
		  free_and_init (attrids);
		  if (attrs_prefix_length)
		    {
		      free_and_init (attrs_prefix_length);
		    }
		  if (btname)
		    {
		      free_and_init (btname);
		    }
		}
	      heap_attrinfo_end (thread_p, &index_attrinfo);
	    }
	}
    }

  if (scan != S_END)
    {
      isallvalid = DISK_ERROR;
    }

  /* End the scan cursor */
  if (heap_scancache_end (thread_p, &scan_cache) != NO_ERROR)
    {
      isallvalid = DISK_ERROR;
    }

  return isallvalid;
}

/*
 * locator_guess_sub_classes () - Guess the subclasses of the given hinted classes
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   lockhint_subclasses(in): lockhint structure which describes classes
 *             The content is updated and the structure itself may be
 *             reallocated
 *
 * Note:This function guess the subclasses identifiers of requested
 *              subclasses for the classes referenced by the lockhint
 *              structure. The lockhint structure is updated to contain the
 *              needed subclasses.
 *
 *              The subclasses are only guessed for lock hint purposes and
 *              they should not be used for any other purposes (it is OK, to
 *              send the objects to the client). That is, the found subclasses
 *              reflects the classes on the server(database volumes) as they
 *              are when the function is invoked; the function does not wait
 *              even when the classes/subclasses may be in the process of been
 *              updated by any transaction.
 *
 *              In general the function is used to approximately find out all
 *              needed subclasses, so that they can be locked along with a
 *              requested set of classes all at once...and not in pieces since
 *              the later can produce deadlocks.
 */
static int
locator_guess_sub_classes (THREAD_ENTRY * thread_p,
			   LC_LOCKHINT ** lockhint_subclasses)
{
  int ref_num;			/* Max and reference number in request
				 * area
				 */
  int max_stack;		/* Total size of stack                    */
  int stack_actual_size;	/* Actual size of stack                   */
  int *stack;			/* The stack for the search               */
  int max_oid_list;		/* Max number of immediate subclasses     */
  OID *oid_list = NULL;		/* List of ref for one object             */
  HEAP_SCANCACHE scan_cache;	/* Scan cache used for fetching purposes  */
  SCAN_CODE scan;		/* Scan return value for an object        */
  void *new_ptr;
  RECDES peek_recdes;
  LC_LOCKHINT *lockhint;
  int num_original_classes;
  LOCK lock;
  int i, j, k;
  int error_code = NO_ERROR;

  /*
   * Start a scan cursor for fetching the desired classes.
   */

  error_code = heap_scancache_start (thread_p, &scan_cache, NULL, NULL,
				     true, false, LOCKHINT_NONE);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  lockhint = *lockhint_subclasses;

  /*
   * Let's assume a number of subclasses for allocation purposes of the stack.
   * We will assume at least one subclass per class and a minimum of 10
   * subclasses for all requested classes.
   */

  max_stack = lockhint->max_classes * 2;
  if (max_stack < 10)
    {
      max_stack = 10;
    }
  max_oid_list = max_stack;

  stack = (int *) malloc (sizeof (*stack) * max_stack);
  if (stack == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (*stack) * max_stack);
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error;
    }
  oid_list = (OID *) malloc (sizeof (*oid_list) * max_oid_list);
  if (oid_list == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (*oid_list) * max_oid_list);
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error;
    }

  /*
   * Obtain the subclasses requested.
   */

  num_original_classes = lockhint->num_classes;
  for (i = 0; i < num_original_classes; i++)
    {
      if (OID_ISNULL (&lockhint->classes[i].oid)
	  || OID_ISTEMP (&lockhint->classes[i].oid)
	  || lockhint->classes[i].need_subclasses <= 0)
	{
	  /*
	   * It has already been visited or we don't care about its subclasses
	   */
	  continue;
	}

      /*
       * Make sure that this is a valid class
       */

      if (!heap_does_exist (thread_p, &lockhint->classes[i].oid, NULL))
	{
	  if (er_errid () == ER_INTERRUPTED)
	    {
	      error_code = ER_INTERRUPTED;
	      goto error;
	    }

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HEAP_UNKNOWN_OBJECT, 3,
		  lockhint->classes[i].oid.volid,
		  lockhint->classes[i].oid.pageid,
		  lockhint->classes[i].oid.slotid);
	  /*
	   * The class did not exist, continue even in the event of errors.
	   * Eliminate this class from the list of requested classes.
	   */
	  OID_SET_NULL (&lockhint->classes[i].oid);
	  continue;
	}

      /*
       * Add the class to the stack and indicate that it has not been visited.
       */

      /* Initialize the stack and push the element */
      stack_actual_size = 0;
      stack[stack_actual_size++] = i;

      /*
       * Star a kind of depth-first search algorithm to find out subclasses
       */

      while (stack_actual_size > 0)
	{
	  /* Pop */
	  ref_num = stack[--stack_actual_size];

	  /*
	   * Get the class to find out its immediate subclasses
	   */

	  scan = heap_get (thread_p, &lockhint->classes[ref_num].oid,
			   &peek_recdes, &scan_cache, PEEK, NULL_CHN);
	  if (scan != S_SUCCESS)
	    {
	      if (scan != S_DOESNT_EXIST
		  && (lockhint->quit_on_errors == true
		      || er_errid () == ER_INTERRUPTED))
		{
		  error_code = ER_FAILED;
		  goto error;
		}

	      /*
	       * Continue after an error. Remove the class from the list of
	       * requested classes
	       */

	      if (ref_num == lockhint->num_classes - 1)
		{
		  /* Last element remove it */
		  lockhint->num_classes--;
		}
	      else
		{
		  /* Marked it as invalid */
		  OID_SET_NULL (&lockhint->classes[ref_num].oid);
		}
	      er_clear ();
	      continue;
	    }

	  /*
	   * has the class been visited ?
	   */

	  if (lockhint->classes[i].need_subclasses <= 0)
	    {
	      /*
	       * This class has already been visited;
	       */
	      continue;
	    }


	  /*
	   * Object has never been visited. First time in the stack.
	   * Mark this class as visited.
	   */

	  lockhint->classes[ref_num].need_subclasses =
	    -lockhint->classes[ref_num].need_subclasses;

	  /*
	   * Find all immediate subclasses for this class
	   */

	  OID_SET_NULL (&oid_list[0]);

	  error_code = orc_subclasses_from_record (&peek_recdes,
						   &max_oid_list, &oid_list);
	  if (error_code != NO_ERROR)
	    {
	      if (lockhint->quit_on_errors == true)
		{
		  goto error;
		}

	      /* Continue even in the case of an error */
	      error_code = NO_ERROR;
	      continue;
	    }

	  /*
	   * Add the above references to the stack if these classes have not
	   * been already been visited or if their current level is smaller
	   * than their visited level
	   */

	  for (k = 0; k < max_oid_list && !OID_ISNULL (&oid_list[k]); k++)
	    {
	      /*
	       * Has this class already been listed ?
	       */
	      for (j = 0; j < lockhint->num_classes; j++)
		{
		  if (OID_EQ (&oid_list[k], &lockhint->classes[j].oid))
		    {
		      break;	/* It is already listed */
		    }
		}

	      if (j == lockhint->num_classes)
		{
		  /*
		   * This is the first time we have seen this class. Push the
		   * class onto the stack.
		   * Make sure that we have area in the stack and the lockhint
		   * area
		   */

		  if (stack_actual_size >= max_stack)
		    {
		      /* Expand the stack by two */
		      max_stack = max_stack * 2;
		      new_ptr = realloc (stack, sizeof (*stack) * max_stack);
		      if (new_ptr == NULL)
			{
			  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				  ER_OUT_OF_VIRTUAL_MEMORY, 1,
				  sizeof (*stack) * max_stack);
			  if (lockhint->quit_on_errors == false)
			    {
			      /* Finish but without an error */
			      break;
			    }
			  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
			  goto error;
			}
		      stack = (int *) new_ptr;
		    }
		  if (lockhint->num_classes >= lockhint->max_classes)
		    {
		      /* Expand the lockhint area by two */
		      new_ptr = locator_reallocate_lockhint (lockhint,
							     (lockhint->
							      max_classes *
							      2));
		      if (new_ptr == NULL)
			{
			  if (lockhint->quit_on_errors == false)
			    {
			      /* Finish but without an error */
			      break;
			    }
			  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
			  goto error;
			}
		      lockhint = *lockhint_subclasses =
			(LC_LOCKHINT *) new_ptr;
		    }

		  /*
		   * Push the class on the stack.
		   */

		  /* Push */
		  stack[stack_actual_size++] = lockhint->num_classes;

		  COPY_OID (&lockhint->
			    classes[lockhint->num_classes].oid, &oid_list[k]);
		  lockhint->classes[lockhint->num_classes].chn =
		    CHN_UNKNOWN_ATCLIENT;
		  lockhint->classes[lockhint->num_classes].lock =
		    lockhint->classes[ref_num].lock;
		  lockhint->classes[lockhint->num_classes].
		    need_subclasses = 1;
		  lockhint->num_classes++;

		}
	      else
		{
		  /*
		   * This is a class that has already been listed and it may
		   * have already been visited.
		   */
		  assert (lockhint->classes[j].lock >= NULL_LOCK
			  && lockhint->classes[ref_num].lock >= NULL_LOCK);

		  if (lockhint->classes[j].need_subclasses >= 0)
		    {
		      /*
		       * The class is only listed at this point. It will be
		       * visited later. The lock may need to be changed, as well
		       * as its subclass flag
		       */

		      /* May be lock change */
		      lockhint->classes[j].lock =
			lock_Conv[lockhint->classes[j].lock]
			[lockhint->classes[ref_num].lock];
		      assert (lockhint->classes[j].lock != NA_LOCK);

		      /* Make sure that subclasses are obtained */
		      lockhint->classes[j].need_subclasses = 1;
		    }
		  else
		    {
		      /*
		       * This class has already been visited. We may need to
		       * revisit if a lock conversion is needed as a result of
		       * several super classes
		       */
		      lock = lock_Conv[lockhint->classes[j].lock]
			[lockhint->classes[ref_num].lock];
		      assert (lock != NA_LOCK);

		      if (lockhint->classes[j].lock != lock)
			{
			  /*
			   * Re-visit
			   */
			  lockhint->classes[j].lock = lock;
			  lockhint->classes[j].need_subclasses = 1;
			  /* Push */
			  stack[stack_actual_size++] = j;
			}
		    }
		}
	    }
	}
    }

  free_and_init (stack);
  free_and_init (oid_list);
  error_code = heap_scancache_end (thread_p, &scan_cache);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /*
   * Scan the lockhint area to make the prune levels positive
   */

  for (i = 0; i < lockhint->num_classes; i++)
    {
      lockhint->classes[i].need_subclasses =
	-lockhint->classes[i].need_subclasses;
    }

  return error_code;

error:

  if (stack)
    {
      free_and_init (stack);
    }
  if (oid_list)
    {
      free_and_init (oid_list);
    }
  (void) heap_scancache_end (thread_p, &scan_cache);

  return error_code;
}

/*
 * xlocator_find_lockhint_class_oids () - Find the oids associated with the given
 *                                  classes
 *
 * return: LC_FIND_CLASSNAME
 *                        (either of LC_CLASSNAME_EXIST,
 *                                   LC_CLASSNAME_DELETED,
 *                                   LC_CLASSNAME_ERROR)
 *
 *   num_classes(in): Number of needed classes
 *   many_classnames(in): Name of the classes
 *   many_locks(in): The desired lock for each class
 *   many_need_subclasses(in): Wheater or not the subclasses are needed.
 *   guessed_class_oids(in):
 *   guessed_class_chns(in):
 *   quit_on_errors(in): Wheater to continue finding the classes in case of
 *                          an error, such as a class does not exist or a lock
 *                          one a may not be granted.
 *   hlock(in): hlock structure which is set to describe the
 *                          classes
 *   fetch_area(in):
 *
 * Note: This function find the class identifiers of the given class
 *              names and requested subclasses of the above classes, and lock
 *              the classes with given locks. The function does not quit when
 *              an error is found and the value of quit_on_errors is false.
 *              In this case the class (an may be its subclasses) with the
 *              error is not locked/fetched.
 *              The function tries to lock all the classes at once, however if
 *              this fails and the function is allowed to continue when errors
 *              are detected, the classes are locked individually.
 *
 *              The subclasses are only guessed for locking purposed and they
 *              should not be used for any other purposes. For example, the
 *              subclasses should not given to the upper levels of the system.
 *
 *              In general the function is used to find out all needed classes
 *              and lock them togheter.
 */
LC_FIND_CLASSNAME
xlocator_find_lockhint_class_oids (THREAD_ENTRY * thread_p, int num_classes,
				   const char **many_classnames,
				   LOCK * many_locks,
				   int *many_need_subclasses,
				   OID * guessed_class_oids,
				   int *guessed_class_chns,
				   int quit_on_errors,
				   LC_LOCKHINT ** hlock,
				   LC_COPYAREA ** fetch_area)
{
  int tran_index;
  EH_SEARCH search;
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  const char *classname;
  LOCK tmp_lock;
  LC_FIND_CLASSNAME find = LC_CLASSNAME_EXIST;
  LC_FIND_CLASSNAME allfind = LC_CLASSNAME_EXIST;
  bool allneed_subclasses = false;
  int retry;
  int i, j;
  int n;

  *fetch_area = NULL;

  /*
   * Let's assume the number of classes that are going to be described in the
   * lockhint area.
   */

  *hlock = locator_allocate_lockhint (num_classes, quit_on_errors);
  if (*hlock == NULL)
    {
      return LC_CLASSNAME_ERROR;
    }

  /*
   * Find the class oids of the given classnames.
   */

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);

  for (i = 0;
       i < num_classes && (allfind == LC_CLASSNAME_EXIST
			   || quit_on_errors == false); i++)
    {
      classname = many_classnames[i];
      if (classname == NULL)
	{
	  continue;
	}

      if (many_need_subclasses[i])
	{
	  allneed_subclasses = true;
	}

      n = (*hlock)->num_classes;
      find = LC_CLASSNAME_EXIST;
      retry = 1;

      while (retry)
	{
	  retry = 0;

	  /*
	   * Describe the hinted class
	   */

	  (*hlock)->classes[n].chn = CHN_UNKNOWN_ATCLIENT;
	  (*hlock)->classes[n].lock = many_locks[i];
	  (*hlock)->classes[n].need_subclasses = many_need_subclasses[i];

	  if (csect_enter_as_reader (thread_p,
				     CSECT_LOCATOR_SR_CLASSNAME_TABLE,
				     INF_WAIT) != NO_ERROR)
	    {
	      return LC_CLASSNAME_ERROR;
	    }

	  entry = ((LOCATOR_TMP_CLASSNAME_ENTRY *)
		   mht_get (locator_Mht_classnames, classname));

	  if (entry != NULL)
	    {
	      /*
	       * We can only proceed if the entry belongs to the current transaction,
	       * otherwise, we must lock the class associated with the classname and
	       * retry the operation once the lock is granted.
	       */

	      COPY_OID (&(*hlock)->classes[n].oid, &entry->current.oid);

	      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

	      if (entry->tran_index == tran_index)
		{
		  if (entry->current.action == LC_CLASSNAME_DELETED
		      || entry->current.action == LC_CLASSNAME_DELETED_RENAME)
		    {
		      find = LC_CLASSNAME_DELETED;
		    }
		}
	      else if (entry->current.action != LC_CLASSNAME_EXIST)
		{
		  /*
		   * Do not know the fate of this entry until the transaction is
		   * committed or aborted. Get the lock and retry later on.
		   */
		  if ((*hlock)->classes[n].lock != NULL_LOCK)
		    {
		      tmp_lock = (*hlock)->classes[n].lock;
		    }
		  else
		    {
		      tmp_lock = IS_LOCK;
		    }

		  if (lock_object (thread_p, &(*hlock)->classes[n].oid,
				   oid_Root_class_oid, tmp_lock,
				   LK_UNCOND_LOCK) != LK_GRANTED)
		    {
		      /*
		       * Unable to acquired the lock
		       */
		      find = LC_CLASSNAME_ERROR;
		    }
		  else
		    {
		      /*
		       * Try again
		       * Remove the lock.. since the above was a dirty read
		       */
		      lock_unlock_object (thread_p, &(*hlock)->classes[n].oid,
					  oid_Root_class_oid, tmp_lock, true);
		      retry = 1;
		      continue;
		    }
		}
	    }
	  else
	    {
	      /*
	       * Is there a class with such a name on the permanent classname
	       * hash table ?
	       */
	      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

	      search = ehash_search (thread_p, locator_Eht_classnames,
				     (void *) classname,
				     &(*hlock)->classes[n].oid);
	      if (search != EH_KEY_FOUND)
		{
		  if (search == EH_KEY_NOTFOUND)
		    {
		      find = LC_CLASSNAME_DELETED;
		    }
		  else
		    {
		      find = LC_CLASSNAME_ERROR;
		    }
		}
	      else
		{
		  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE,
				   INF_WAIT) != NO_ERROR)
		    {
		      return LC_CLASSNAME_ERROR;
		    }
		  /* Double check : already cached ? */
		  entry = ((LOCATOR_TMP_CLASSNAME_ENTRY *)
			   mht_get (locator_Mht_classnames, classname));
		  if (entry == NULL)
		    {

		      if (((int) mht_count (locator_Mht_classnames) <
			   MAX_CLASSNAME_CACHE_ENTRIES)
			  || (locator_decache_class_name_entries () ==
			      NO_ERROR))
			{

			  entry = ((LOCATOR_TMP_CLASSNAME_ENTRY *)
				   malloc (sizeof (*entry)));

			  if (entry == NULL)
			    {
			      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				      ER_OUT_OF_VIRTUAL_MEMORY, 1,
				      sizeof (*entry));
			      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
			      return LC_CLASSNAME_ERROR;
			    }

			  entry->name = strdup ((char *) classname);
			  if (entry->name == NULL)
			    {
			      free_and_init (entry);
			      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				      ER_OUT_OF_VIRTUAL_MEMORY, 1,
				      strlen (classname));
			      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
			      return LC_CLASSNAME_ERROR;
			    }

			  entry->tran_index = NULL_TRAN_INDEX;
			  entry->current.action = LC_CLASSNAME_EXIST;
			  COPY_OID (&entry->current.oid,
				    &(*hlock)->classes[n].oid);
			  LSA_SET_NULL (&entry->current.savep_lsa);
			  entry->current.prev = NULL;
			  (void) mht_put (locator_Mht_classnames,
					  entry->name, entry);
			}
		    }
		  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		}
	    }
	}

      if (find == LC_CLASSNAME_EXIST)
	{
	  /*
	   * If the client has guessed the right class_oid, use the cache
	   * coherency number on the client to avoid sending the class object
	   */
	  if (guessed_class_oids != NULL
	      && OID_EQ (&(*hlock)->classes[n].oid, &guessed_class_oids[i]))
	    {
	      (*hlock)->classes[n].chn = guessed_class_chns[i];
	    }

	  n++;
	  (*hlock)->num_classes = n;
	}
      else
	{
	  if (allfind != LC_CLASSNAME_ERROR)
	    {
	      allfind = find;
	    }
	  if (find == LC_CLASSNAME_DELETED)
	    {
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LC_UNKNOWN_CLASSNAME, 1, classname);
	    }
	}
    }

  /*
   * Eliminate any duplicates. Note that we did not want to do above since
   * we did not want to modify the original arrays.
   */

  for (i = 0; i < (*hlock)->num_classes; i++)
    {
      if (OID_ISNULL (&(*hlock)->classes[i].oid))
	{
	  continue;
	}
      /*
       * Is this duplicated ?
       */
      for (j = i + 1; j < (*hlock)->num_classes; j++)
	{
	  if (OID_EQ (&(*hlock)->classes[i].oid, &(*hlock)->classes[j].oid))
	    {
	      /* Duplicate class, merge the lock and the subclass entry */
	      assert ((*hlock)->classes[i].lock >= NULL_LOCK
		      && (*hlock)->classes[j].lock >= NULL_LOCK);
	      (*hlock)->classes[i].lock =
		lock_Conv[(*hlock)->classes[i].lock]
		[(*hlock)->classes[j].lock];
	      assert ((*hlock)->classes[i].lock != NA_LOCK);

	      if ((*hlock)->classes[i].need_subclasses == 0)
		{
		  (*hlock)->classes[i].need_subclasses =
		    (*hlock)->classes[j].need_subclasses;
		}

	      /* Now eliminate the entry */
	      OID_SET_NULL (&(*hlock)->classes[j].oid);
	    }
	}
    }

  /*
   * Do we need to get subclasses ?
   */

  if (allneed_subclasses == true
      && (allfind == LC_CLASSNAME_EXIST || quit_on_errors == false))
    {
      if (locator_guess_sub_classes (thread_p, &(*hlock)) != NO_ERROR)
	{
	  allfind = LC_CLASSNAME_ERROR;
	}
    }

  if (allfind == LC_CLASSNAME_EXIST || quit_on_errors == false)
    {
      if (xlocator_fetch_lockhint_classes (thread_p, (*hlock), fetch_area) !=
	  NO_ERROR)
	{
	  allfind = LC_CLASSNAME_ERROR;
	  if (quit_on_errors == true)
	    {
	      locator_free_lockhint ((*hlock));
	      *hlock = NULL;
	    }
	}
    }
  else
    {
      locator_free_lockhint ((*hlock));
      *hlock = NULL;
    }

  return allfind;
}

/*
 * xlocator_fetch_lockhint_classes () - Lock and fetch a set of classes
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   lockhint(in): Description of hinted classses
 *   fetch_area(in/out): Pointer to area where the objects are placed
 *
 */
int
xlocator_fetch_lockhint_classes (THREAD_ENTRY * thread_p,
				 LC_LOCKHINT * lockhint,
				 LC_COPYAREA ** fetch_area)
{
  LOCATOR_RETURN_NXOBJ nxobj;	/* Description to return next object */
  LC_COPYAREA_DESC prefetch_des;	/* Descriptor for decache of objects
					 * related to transaction isolation
					 * level */
  SCAN_CODE scan = S_SUCCESS;
  int copyarea_length;
  int i;
  int error_code = NO_ERROR;

  *fetch_area = NULL;

  if (lockhint->num_classes <= 0)
    {
      lockhint->num_classes_processed = lockhint->num_classes;
      return NO_ERROR;
    }

  if (lockhint->num_classes_processed == -1)
    {
      /*
       * FIRST CALL.
       * Initialize num of object processed.
       */
      lockhint->num_classes_processed = 0;

      /* Obtain the locks */
      if (lock_classes_lock_hint (thread_p, lockhint) != LK_GRANTED)
	{
	  if (lockhint->quit_on_errors != false)
	    {
	      return ER_FAILED;
	    }
	  else
	    {
	      error_code = ER_FAILED;
	      /* Lock individual classes */
	      for (i = 0; i < lockhint->num_classes; i++)
		{
		  if (OID_ISNULL (&lockhint->classes[i].oid))
		    {
		      continue;
		    }
		  if (lock_object (thread_p, &lockhint->classes[i].oid,
				   oid_Root_class_oid,
				   lockhint->classes[i].lock,
				   LK_UNCOND_LOCK) != LK_GRANTED)
		    {
		      OID_SET_NULL (&lockhint->classes[i].oid);
		    }
		  else
		    {
		      /* We are unable to continue since we lock at least one */
		      error_code = NO_ERROR;
		    }
		}
	      if (error_code != NO_ERROR)
		{
		  return error_code;
		}
	    }
	}
    }

  /*
   * Start a scan cursor for getting the classes
   */

  error_code = heap_scancache_start (thread_p, &nxobj.area_scancache, NULL,
				     NULL, true, false, LOCKHINT_NONE);
  if (error_code != NO_ERROR)
    {
      lock_unlock_classes_lock_hint (thread_p, lockhint);
      return error_code;
    }

  nxobj.ptr_scancache = &nxobj.area_scancache;

  /*
   * Assume that there are not any classes larger than one page. If there are
   * the number of pages is fixed later.
   */

  /* Assume that the needed object can fit in one page */
  copyarea_length = DB_PAGESIZE;

  nxobj.mobjs = NULL;
  nxobj.comm_area = NULL;

  while (scan == S_SUCCESS
	 && (lockhint->num_classes_processed < lockhint->num_classes))
    {
      nxobj.comm_area = locator_allocate_copy_area_by_length (copyarea_length,
							      CLEAR_MEM);
      if (nxobj.comm_area == NULL)
	{
	  (void) heap_scancache_end (thread_p, &nxobj.area_scancache);
	  lock_unlock_classes_lock_hint (thread_p, lockhint);
	  return ER_FAILED;
	}

      nxobj.mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (nxobj.comm_area);
      nxobj.obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (nxobj.mobjs);
      LC_RECDES_IN_COPYAREA (nxobj.comm_area, &nxobj.recdes);
      nxobj.area_offset = 0;
      nxobj.mobjs->num_objs = 0;

      /*
       * Place the classes on the communication area, don't place those classes
       * with correct chns.
       */

      for (i = lockhint->num_classes_processed;
	   scan == S_SUCCESS && i < lockhint->num_classes; i++)
	{
	  if (OID_ISNULL (&lockhint->classes[i].oid)
	      || OID_ISTEMP (&lockhint->classes[i].oid))
	    {
	      lockhint->num_classes_processed += 1;
	      continue;
	    }

	  /* Now return the object */
	  scan = locator_return_object (thread_p, &nxobj,
					&lockhint->classes[i].oid,
					lockhint->classes[i].chn);
	  if (scan == S_SUCCESS)
	    {
	      lockhint->num_classes_processed += 1;
	    }
	  else if (scan == S_DOESNT_FIT && nxobj.mobjs->num_objs == 0)
	    {
	      /*
	       * The first object on the copy area does not fit.
	       * Get a larger area
	       */

	      /* Get the real length of the fetch/copy area */

	      copyarea_length = nxobj.comm_area->length;

	      if ((-nxobj.recdes.length) > copyarea_length)
		{
		  copyarea_length = ((-nxobj.recdes.length) +
				     sizeof (*nxobj.mobjs));
		}
	      else
		{
		  copyarea_length += DB_PAGESIZE;
		}

	      locator_free_copy_area (nxobj.comm_area);
	      scan = S_SUCCESS;
	      break;		/* finish the for */
	    }
	  else
	    if (scan != S_DOESNT_FIT
		&& (scan == S_DOESNT_EXIST
		    || lockhint->quit_on_errors == false))
	    {
	      OID_SET_NULL (&lockhint->classes[i].oid);
	      lockhint->num_classes_processed += 1;
	      scan = S_SUCCESS;
	    }
	}
    }

  /* End the scan cursor */
  error_code = heap_scancache_end (thread_p, &nxobj.area_scancache);
  if (error_code != NO_ERROR)
    {
      /* There was an error.. */
      if (nxobj.mobjs != NULL)
	{
	  locator_free_copy_area (nxobj.comm_area);
	}
      lock_unlock_classes_lock_hint (thread_p, lockhint);
      return error_code;
    }

  if (scan == S_ERROR)
    {
      /* There was an error.. */
      if (nxobj.mobjs != NULL)
	{
	  locator_free_copy_area (nxobj.comm_area);
	}
      lock_unlock_classes_lock_hint (thread_p, lockhint);
      return ER_FAILED;
    }
  else if (nxobj.mobjs != NULL && nxobj.mobjs->num_objs == 0)
    {
      locator_free_copy_area (nxobj.comm_area);
    }
  else
    {
      *fetch_area = nxobj.comm_area;
    }

  if (*fetch_area != NULL)
    {
      prefetch_des.mobjs = nxobj.mobjs;
      prefetch_des.obj = &nxobj.obj;
      prefetch_des.offset = &nxobj.area_offset;
      prefetch_des.recdes = &nxobj.recdes;
      lock_notify_isolation_incons (thread_p, locator_notify_decache,
				    &prefetch_des);
    }

  if (lockhint->num_classes_processed >= lockhint->num_classes)
    {
      lock_unlock_classes_lock_hint (thread_p, lockhint);
    }

  return NO_ERROR;
}

/*
 * xlocator_assign_oid_batch () - Assign a group of permanent oids
 *
 * return:  NO_ERROR if all OK, ER_ status otherwise
 *
 *   oidset(in): LC_OIDSET describing all of the temp oids
 *
 * Note:Permanent oids are assigned to each of the temporary oids
 *              listed in the LC_OIDSET.
 */
int
xlocator_assign_oid_batch (THREAD_ENTRY * thread_p, LC_OIDSET * oidset)
{
  LC_CLASS_OIDSET *class_oidset;
  LC_OIDMAP *oid;
  int error_code = NO_ERROR;

  /* establish a rollback point in case we get an error part way through */
  if (log_start_system_op (thread_p) == NULL)
    {
      return ER_FAILED;
    }

  /* Now assign the OID's stop on the first error */
  for (class_oidset = oidset->classes; class_oidset != NULL;
       class_oidset = class_oidset->next)
    {
      for (oid = class_oidset->oids; oid != NULL; oid = oid->next)
	{
	  error_code = xlocator_assign_oid (thread_p, &class_oidset->hfid,
					    &oid->oid, oid->est_size,
					    &class_oidset->class_oid, NULL);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}
    }

  /* accept the operation */
  log_end_system_op (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER);

  return error_code;

error:
  /* rollback the operation */
  log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
  return error_code;
}

/*
 * locator_increase_catalog_count () -
 *
 * return:
 *
 *   cls_oid(in): Class OID
 *
 * Note:Increase the 'tot_objects' counter of the CLS_INFO
 *        and do update the catalog record in-place.
 */
static void
locator_increase_catalog_count (THREAD_ENTRY * thread_p, OID * cls_oid)
{
  CLS_INFO *cls_infop = NULL;

  /* retrieve the class information */
  cls_infop = catalog_get_class_info (thread_p, cls_oid);

  if (cls_infop == NULL)
    {
      return;
    }

  if (cls_infop->hfid.vfid.fileid < 0 || cls_infop->hfid.vfid.volid < 0)
    {
      /* The class does not have a heap file (i.e. it has no instances);
         so no statistics can be obtained for this class; just set
         'tot_objects' field to 0. */
      /* Is it safe not to initialize the other fields of CLS_INFO? */
      cls_infop->tot_objects = 0;
    }
  else
    {
      /* increase the 'tot_objects' counter */
      cls_infop->tot_objects++;
    }

  /* update the class information to the catalog */
  /* NOTE that tot_objects may not be correct because changes are NOT logged. */
  (void) catalog_update_class_info (thread_p, cls_oid, cls_infop, true);

  catalog_free_class_info (cls_infop);
}

/*
 * locator_decrease_catalog_count  () -
 *
 * return:
 *
 *   cls_oid(in): Class OID
 *
 * Note: Descrease the 'tot_objects' counter of the CLS_INFO
 *        and do update the catalog record in-place.
 */
static void
locator_decrease_catalog_count (THREAD_ENTRY * thread_p, OID * cls_oid)
{
  CLS_INFO *cls_infop = NULL;

  /* retrieve the class information */
  cls_infop = catalog_get_class_info (thread_p, cls_oid);

  if (cls_infop == NULL)
    {
      return;
    }
  if (cls_infop->hfid.vfid.fileid < 0 || cls_infop->hfid.vfid.volid < 0)
    {
      /* The class does not have a heap file (i.e. it has no instances);
         so no statistics can be obtained for this class; just set
         'tot_objects' field to 0. */
      /* Is it an error to delete an instance with no heap file? */
      cls_infop->tot_objects = 0;
    }
  else
    {
      /* decrease the 'tot_objects' counter */
      cls_infop->tot_objects--;
    }

  /* update the class information to the catalog */
  /* NOTE that tot_objects may not be correct because changes are NOT logged. */
  (void) catalog_update_class_info (thread_p, cls_oid, cls_infop, true);

  catalog_free_class_info (cls_infop);
}

/*
 * xrepl_set_info () -
 *
 * return:
 *
 *   repl_info(in):
 */
int
xrepl_set_info (THREAD_ENTRY * thread_p, REPL_INFO * repl_info)
{
  int error_code = NO_ERROR;

  if (db_Enable_replications > 0 && !LOG_CHECK_LOG_APPLIER (thread_p))
    {
      switch (repl_info->repl_info_type)
	{
	case REPL_INFO_TYPE_SCHEMA:
	  error_code =
	    repl_log_insert_schema (thread_p,
				    (REPL_INFO_SCHEMA *) repl_info->info);
	  break;
	default:
	  error_code = ER_REPL_ERROR;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_REPL_ERROR, 1,
		  "can't make repl schema info");
	  break;
	}
    }

  return error_code;
}

/*
 * xrepl_log_get_append_lsa () -
 *
 * return:
 */
LOG_LSA *
xrepl_log_get_append_lsa (void)
{
  return log_get_append_lsa ();
}

/*
 * xlocator_build_fk_object_cache () -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   cls_oid(in):
 *   hfid(in):
 *   key_type(in):
 *   n_attrs(in):
 *   attr_ids(in):
 *   pk_cls_oid(in):
 *   pk_btid(in):
 *   cache_attr_id(in):
 *   fk_name(in):
 */
int
xlocator_build_fk_object_cache (THREAD_ENTRY * thread_p, OID * cls_oid,
				HFID * hfid, TP_DOMAIN * key_type,
				int n_attrs, int *attr_ids,
				OID * pk_cls_oid, BTID * pk_btid,
				int cache_attr_id, char *fk_name)
{
  HEAP_SCANCACHE scan_cache;
  HEAP_CACHE_ATTRINFO attr_info;
  OID oid;
  RECDES peek_recdes;
  DB_VALUE *key_val, tmpval;
  char midxkey_buf[DBVAL_BUFSIZE + MAX_ALIGNMENT], *aligned_midxkey_buf;
  int error_code;

  aligned_midxkey_buf = PTR_ALIGN (midxkey_buf, MAX_ALIGNMENT);

  error_code = heap_scancache_start (thread_p, &scan_cache, hfid, cls_oid,
				     true, false, LOCKHINT_NONE);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = heap_attrinfo_start (thread_p, cls_oid, n_attrs, attr_ids,
				    &attr_info);
  if (error_code != NO_ERROR)
    {
      (void) heap_scancache_end (thread_p, &scan_cache);
      return error_code;
    }

  OID_SET_NULL (&oid);
  oid.volid = hfid->vfid.volid;

  while (heap_next (thread_p, hfid, NULL, &oid, &peek_recdes, &scan_cache,
		    PEEK) == S_SUCCESS)
    {
      if (n_attrs == 1)
	{
	  error_code = heap_attrinfo_read_dbvalues (thread_p, &oid,
						    &peek_recdes, &attr_info);
	  if (error_code != NO_ERROR)
	    {
	      goto end;
	    }
	}

      key_val = heap_attrinfo_generate_key (thread_p, n_attrs, attr_ids, NULL,
					    &attr_info, &peek_recdes, &tmpval,
					    aligned_midxkey_buf);
      if (key_val == NULL)
	{
	  error_code = ER_FAILED;
	  goto end;
	}

      error_code = btree_check_foreign_key (thread_p, cls_oid, hfid, &oid,
					    key_val, n_attrs, pk_cls_oid,
					    pk_btid, cache_attr_id, fk_name);

      if (key_val == &tmpval)
	{
	  pr_clear_value (&tmpval);
	}
    }

end:

  error_code = heap_scancache_end (thread_p, &scan_cache);
  heap_attrinfo_end (thread_p, &attr_info);

  return error_code;
}

/*
 * xlocator_lock_and_fetch_all () - Fetch all class instances that can be locked
 *				    in specified locked time
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap file where the instances of the class are placed
 *   instance_lock(in): Instance lock to aquire
 *   instance_lock_timeout(in): Timeout for instance lock
 *   class_oid(in): Class identifier of the instances to fetch
 *   class_lock(in): Lock to acquire (Set as a side effect to NULL_LOCKID)
 *   nobjects(out): Total number of objects to fetch.
 *   nfetched(out): Current number of object fetched.
 *   nfailed_instance_locks(out): count failed instance locks
 *   last_oid(out): Object identifier of last fetched object
 *   fetch_area(in/out): Pointer to area where the objects are placed
 *
 */
int
xlocator_lock_and_fetch_all (THREAD_ENTRY * thread_p, const HFID * hfid,
			     LOCK * instance_lock,
			     int *instance_lock_timeout,
			     OID * class_oid, LOCK * class_lock,
			     int *nobjects, int *nfetched,
			     int *nfailed_instance_locks,
			     OID * last_oid, LC_COPYAREA ** fetch_area)
{
  LC_COPYAREA_DESC prefetch_des;	/* Descriptor for decache of
					 * objects related to transaction
					 * isolation level */
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in
				 * area */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area  */
  RECDES recdes;		/* Record descriptor for
				 * insertion */
  int offset;			/* Place to store next object in
				 * area */
  int round_length;		/* Length of object rounded to
				 * integer alignment */
  int copyarea_length;
  OID oid;
  HEAP_SCANCACHE scan_cache;
  SCAN_CODE scan;
  int error_code = NO_ERROR;

  if (fetch_area == NULL)
    {
      return ER_FAILED;
    }
  *fetch_area = NULL;

  if (nfailed_instance_locks)
    {
      *nfailed_instance_locks = 0;
    }

  if (OID_ISNULL (last_oid))
    {
      /* FIRST TIME. */

      /* Obtain the desired lock for the class scan */
      if (*class_lock != NULL_LOCK
	  && lock_object (thread_p, class_oid, oid_Root_class_oid,
			  *class_lock, LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  /*
	   * Unable to acquired lock
	   */
	  *class_lock = NULL_LOCK;
	  *nobjects = -1;
	  *nfetched = -1;

	  error_code = ER_FAILED;
	  goto error;
	}

      /* Get statistics */
      last_oid->volid = hfid->vfid.volid;
      last_oid->pageid = NULL_PAGEID;
      last_oid->slotid = NULL_SLOTID;
      /* Estimate the number of objects to be fetched */
      *nobjects = heap_estimate_num_objects (thread_p, hfid);
      *nfetched = 0;
      if (*nobjects == -1)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
    }

  /* Set OID to last fetched object */
  COPY_OID (&oid, last_oid);

  /* Start a scan cursor for getting several classes */
  error_code = heap_scancache_start (thread_p, &scan_cache, hfid, class_oid,
				     true, false, LOCKHINT_NONE);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  /* Assume that the next object can fit in one page */
  copyarea_length = DB_PAGESIZE;

  while (true)
    {
      *fetch_area = locator_allocate_copy_area_by_length (copyarea_length,
							  CLEAR_MEM);
      if (*fetch_area == NULL)
	{
	  (void) heap_scancache_end (thread_p, &scan_cache);
	  error_code = ER_FAILED;
	  goto error;
	}

      mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (*fetch_area);
      LC_RECDES_IN_COPYAREA (*fetch_area, &recdes);
      obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);
      mobjs->num_objs = 0;
      offset = 0;

      while (true)
	{
	  if (instance_lock && (*instance_lock != NULL_LOCK))
	    {
	      int lock_result = 0;

	      scan = heap_next (thread_p, hfid, class_oid, &oid, &recdes,
				&scan_cache, COPY);
	      if (scan != S_SUCCESS)
		{
		  break;
		}

	      if (instance_lock_timeout == NULL)
		{
		  lock_result = lock_object (thread_p, &oid, class_oid,
					     *instance_lock, LK_UNCOND_LOCK);
		}
	      else
		{
		  lock_result =
		    lock_object_waitsecs (thread_p, &oid, class_oid,
					  *instance_lock, LK_UNCOND_LOCK,
					  *instance_lock_timeout);
		}

	      if (lock_result != LK_GRANTED)
		{
		  (*nfailed_instance_locks)++;
		  continue;
		}

	      scan = heap_get (thread_p, &oid, &recdes, &scan_cache, false,
			       NULL_CHN);
	      if (scan != S_SUCCESS)
		{
		  (*nfailed_instance_locks)++;
		  continue;
		}

	    }
	  else
	    {
	      scan = heap_next (thread_p, hfid, class_oid, &oid, &recdes,
				&scan_cache, COPY);
	      if (scan != S_SUCCESS)
		{
		  break;
		}
	    }

	  mobjs->num_objs++;
	  COPY_OID (&obj->oid, &oid);
	  obj->has_index = false;
	  obj->hfid = NULL_HFID;
	  obj->length = recdes.length;
	  obj->offset = offset;
	  obj->operation = LC_FETCH;
	  obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);
	  round_length = DB_ALIGN (recdes.length, MAX_ALIGNMENT);
	  offset += round_length;
	  recdes.data += round_length;
	  recdes.area_size -= round_length + sizeof (*obj);
	}

      if (scan != S_DOESNT_FIT || mobjs->num_objs > 0)
	{
	  break;
	}
      /*
       * The first object does not fit into given copy area
       * Get a larger area
       */

      /* Get the real length of current fetch/copy area */
      copyarea_length = (*fetch_area)->length;
      locator_free_copy_area (*fetch_area);

      /*
       * If the object does not fit even when the copy area seems to be
       * large enough, increase the copy area by at least one page size.
       */

      if ((-recdes.length) > copyarea_length)
	{
	  copyarea_length = (-recdes.length) + sizeof (*mobjs);
	}
      else
	{
	  copyarea_length += DB_PAGESIZE;
	}
    }

  if (scan == S_END)
    {
      /*
       * This is the end of the loop. Indicate the caller that no more calls
       * are needed by setting nobjects and nfetched to the same value.
       */
      error_code = heap_scancache_end (thread_p, &scan_cache);
      if (error_code != NO_ERROR)
	{
	  *nobjects = *nfetched = -1;
	  goto error;
	}

      *nfetched += mobjs->num_objs;
      *nobjects = *nfetched;
      OID_SET_NULL (last_oid);
      if (*class_lock != NULL_LOCK)
	{
	  lock_unlock_object (thread_p, class_oid, oid_Root_class_oid,
			      *class_lock, false);
	}
    }
  else if (scan == S_ERROR)
    {
      /* There was an error.. */
      (void) heap_scancache_end (thread_p, &scan_cache);
      *nobjects = *nfetched = -1;
      error_code = ER_FAILED;
      goto error;
    }
  else if (mobjs->num_objs != 0)
    {
      heap_scancache_end_when_scan_will_resume (thread_p, &scan_cache);
      /* Set the last_oid.. and the number of fetched objects */
      obj = LC_PRIOR_ONEOBJ_PTR_IN_COPYAREA (obj);
      COPY_OID (last_oid, &obj->oid);
      *nfetched += mobjs->num_objs;
      /*
       * If the guess on the number of objects to fetch was low, reset the
       * value, so that the caller continue to call us until the end of the
       * scan
       */
      if (*nobjects <= *nfetched)
	{
	  *nobjects = *nfetched + 10;
	}
    }
  else
    {
      error_code = heap_scancache_end (thread_p, &scan_cache);
      if (error_code != NO_ERROR)
	{
	  goto end;
	}
    }

  if (*fetch_area != NULL)
    {
      prefetch_des.mobjs = mobjs;
      prefetch_des.obj = &obj;
      prefetch_des.offset = &offset;
      prefetch_des.recdes = &recdes;
      lock_notify_isolation_incons (thread_p, locator_notify_decache,
				    &prefetch_des);
    }

end:

  return error_code;

error:
  if (*class_lock != NULL_LOCK)
    {
      lock_unlock_object (thread_p, class_oid, oid_Root_class_oid,
			  *class_lock, false);
    }
  if (*fetch_area != NULL)
    {
      locator_free_copy_area (*fetch_area);
      *fetch_area = NULL;
    }
  return error_code;
}
