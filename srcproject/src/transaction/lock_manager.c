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
 * lock_manager.c - lock management module (at the server)
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <time.h>
#if defined(SOLARIS)
#include <netdb.h>
#endif /* SOLARIS */

#include "xserver_interface.h"
#include "lock_manager.h"
#include "system_parameter.h"
#include "memory_alloc.h"
#include "oid.h"
#include "storage_common.h"
#include "log_manager.h"
#include "transaction_sr.h"
#include "wait_for_graph.h"
#include "critical_section.h"
#include "memory_hash.h"
#include "locator.h"
#include "perf_monitor.h"
#include "page_buffer.h"
#include "message_catalog.h"
#include "environment_variable.h"
#include "log_impl.h"
#include "thread_impl.h"
#include "query_manager.h"

#ifndef DB_NA
#define DB_NA           2
#endif
extern int lock_Comp[11][11];

#if defined (SERVER_MODE)
/* object lock hash function */
#define LK_OBJ_LOCK_HASH(oid)    \
  ((OID_ISTEMP(oid)) ? (unsigned int)(-((oid)->pageid) % lk_Gl.obj_hash_size) :\
                       lock_get_hash_value(oid))

/* uncommitted read isolation level ? */
#define LK_UNCOMMITTED_READ_ISOLATION(isolation)                \
        ((isolation) == TRAN_REP_CLASS_UNCOMMIT_INSTANCE        \
         || (isolation) == TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE)

/* non-2pl lock request ? */
#if 1				/* TODO: M2 */
#define LK_NON2PL_LOCK_REQUEST(res_ptr, iso, lock)              \
        (res_ptr->type == LOCK_RESOURCE_INSTANCE                \
         && LK_UNCOMMITTED_READ_ISOLATION(iso)                  \
         && ((lock) == NS_LOCK || (lock) == S_LOCK))
#else /* 0 */
#define LK_NON2PL_LOCK_REQUEST(res_ptr, iso, lock)              \
        (res_ptr->type == LOCK_RESOURCE_INSTANCE                \
         && LK_UNCOMMITTED_READ_ISOLATION(iso)                  \
         && ((lock) == NS_LOCK || (lock) == S_LOCK || (lock) == U_LOCK))
#endif /* 0 */

/* thread is lock-waiting ? */
#define LK_IS_LOCKWAIT_THREAD(thrd)                             \
        ((thrd)->lockwait != NULL                               \
         && (thrd)->lockwait_state == (int)LOCK_SUSPENDED)

/* transaction wait for only some secs ? */
#define LK_CAN_TIMEOUT(secs)    ((secs) != LK_INFINITE_WAIT)

/* is younger transaction ? */
#define LK_ISYOUNGER(young_tranid, old_tranid) (young_tranid > old_tranid)

/* initialize lock entry as free state */
#define LK_INIT_ENTRY(entry_ptr)                                \
do {    entry_ptr->tran_index = -1;                             \
        entry_ptr->thrd_entry = NULL;                           \
        entry_ptr->res_head = NULL;                             \
        entry_ptr->granted_mode = NULL_LOCK;                    \
        entry_ptr->blocked_mode = NULL_LOCK;                    \
        entry_ptr->next = NULL;                                 \
        entry_ptr->tran_next = NULL;                            \
        entry_ptr->class_entry = NULL;                          \
        entry_ptr->ngranules = 0;                               \
        entry_ptr->history = NULL;                              \
        entry_ptr->recent = NULL;                               \
        entry_ptr->mlk_count = 0;                               \
} while (0)

/* initialize lock entry as granted state */
#define LK_INIT_ENTRY_GRANTED(entry_ptr, tran_index, res, lock) \
do {    entry_ptr->tran_index = (tran_index);                   \
        entry_ptr->res_head = (res);                            \
        entry_ptr->granted_mode = (lock);                       \
        entry_ptr->blocked_mode = NULL_LOCK;                    \
        entry_ptr->count = 1;                                   \
        entry_ptr->next = NULL;                                 \
        entry_ptr->tran_next = NULL;                            \
        entry_ptr->class_entry = NULL;                          \
        entry_ptr->ngranules = 0;                               \
        entry_ptr->history = NULL;                              \
        entry_ptr->recent = NULL;                               \
        entry_ptr->mlk_count = 0;                               \
} while (0)

/* initialize lock entry as blocked state */
#define LK_INIT_ENTRY_BLOCKED(entry_ptr, thread_p, tran_index, res, lock) \
do {    entry_ptr->tran_index = (tran_index);                   \
        entry_ptr->thrd_entry = (thread_p);                     \
        entry_ptr->res_head = (res);                            \
        entry_ptr->granted_mode = NULL_LOCK;                    \
        entry_ptr->blocked_mode = (lock);                       \
        entry_ptr->count = 1;                                   \
        entry_ptr->next = NULL;                                 \
        entry_ptr->tran_next = NULL;                            \
        entry_ptr->class_entry = NULL;                          \
        entry_ptr->ngranules = 0;                               \
        entry_ptr->history = NULL;                              \
        entry_ptr->recent = NULL;                               \
        entry_ptr->mlk_count = 0;                               \
} while (0)

/* initialize lock entry as non2pl state */
#define LK_INIT_ENTRY_NON2PL(entry_ptr, tran_index, res, lock)  \
do {    entry_ptr->tran_index = (tran_index);                   \
        entry_ptr->res_head = (res);                            \
        entry_ptr->granted_mode = (lock);                       \
        entry_ptr->blocked_mode = NULL_LOCK;                    \
        entry_ptr->count = 0;                                   \
        entry_ptr->next = NULL;                                 \
        entry_ptr->tran_next = NULL;                            \
        entry_ptr->class_entry = NULL;                          \
        entry_ptr->ngranules = 0;                               \
        entry_ptr->history = NULL;                              \
        entry_ptr->recent = NULL;                               \
        entry_ptr->mlk_count = 0;                               \
} while (0)

/* initialize lock resource as free state */
#define LK_INIT_RES(res_ptr)                                    \
do {    MUTEX_INIT(((res_ptr)->res_mutex));                     \
        res_ptr->type = LOCK_RESOURCE_OBJECT;                     \
        OID_SET_NULL(&((res_ptr)->oid));                        \
        OID_SET_NULL(&((res_ptr)->class_oid));                  \
        res_ptr->total_holders_mode = NULL_LOCK;                \
        res_ptr->total_waiters_mode = NULL_LOCK;                \
        res_ptr->holder = NULL;                                 \
        res_ptr->waiter = NULL;                                 \
        res_ptr->non2pl = NULL;                                 \
        res_ptr->hash_next = NULL;                              \
} while (0)

/* initialize lock resource as allocated state */
#define LK_INIT_RES_ALLOCATED(res_ptr, id1, id2, lock)          \
  do                                                            \
  {                                                             \
    if (OID_IS_ROOTOID (id1))                                   \
      {                                                         \
          (res_ptr)->type = LOCK_RESOURCE_ROOT_CLASS;             \
        COPY_OID (&((res_ptr)->oid), (id1));                    \
        OID_SET_NULL (&((res_ptr)->class_oid));                 \
      }                                                         \
    else                                                        \
      {                                                         \
        if (id2 == NULL || OID_IS_ROOTOID (id2))                \
          {                                                     \
            (res_ptr)->type = LOCK_RESOURCE_CLASS;                \
            COPY_OID (&((res_ptr)->oid), (id1));                \
            OID_SET_NULL (&((res_ptr)->class_oid));             \
          }                                                     \
        else                                                    \
          {                                                     \
            (res_ptr)->type = LOCK_RESOURCE_INSTANCE;             \
            COPY_OID (&((res_ptr)->oid), (id1));                \
            COPY_OID (&((res_ptr)->class_oid), (id2));          \
          }                                                     \
        }                                                       \
        res_ptr->total_holders_mode = (lock);                   \
        res_ptr->total_waiters_mode = NULL_LOCK;                \
        res_ptr->holder = NULL;                                 \
        res_ptr->waiter = NULL;                                 \
        res_ptr->non2pl = NULL;                                 \
  }                                                             \
  while (0)                                                     \

/* Defines for printing lock activity messages */
#define LK_MSG_LOCK_HELPER(entry, msgnum)                               \
  fprintf(stdout, \
      msgcat_message (MSGCAT_CATALOG_CUBRID, MSGCAT_SET_LOCK, msgnum)), \
      (entry)->tran_index, LOCK_TO_LOCKMODE_STRING((entry)->granted_mode),\
      (entry)->res_head->oid->volid, (entry)->res_head->oid->pageid,    \
      (entry)->oid->slotid);

#define LK_MSG_LOCK_ACQUIRED(entry)                                     \
  LK_MSG_LOCK_HELPER(entry, MSGCAT_LK_OID_LOCK_ACQUIRED)

#define LK_MSG_LOCK_CONVERTED(entry)                                    \
  LK_MSG_LOCK_HELPER(entry, MSGCAT_LK_OID_LOCK_CONVERTED)

#define LK_MSG_LOCK_WAITFOR(entry)                                      \
  LK_MSG_LOCK_HELPER(entry, MSGCAT_LK_OID_LOCK_WAITFOR)

#define LK_MSG_LOCK_RELEASE(entry)                                      \
  LK_MSG_LOCK_HELPER(entry, MSGCAT_LK_OID_LOCK_RELEASE)

#define LK_MSG_LOCK_DEMOTE(entry)                                       \
  LK_MSG_LOCK_HELPER(entry, MSGCAT_LK_OID_LOCK_DEMOTE)

#define RECORD_LOCK_ACQUISITION_HISTORY(entry_ptr, history, lock)       \
  do                                                                    \
  {                                                                     \
    (history)->req_mode = (lock); \
    (history)->next = NULL; \
    (history)->prev = (entry_ptr)->recent; \
    /* append it to end of list */ \
    if ((entry_ptr)->recent == NULL)                                    \
      {                                                                 \
      (entry_ptr)->history = (history); \
      (entry_ptr)->recent = (history); \
    } \
    else                                                                \
      {                                                                 \
      (entry_ptr)->recent->next = (history); \
      (entry_ptr)->recent = (history); \
    } \
  }                                                                     \
  while (0)

#define NEED_LOCK_ACQUISITION_HISTORY(isolation, entry_ptr) \
  ((isolation) == TRAN_REP_CLASS_COMMIT_INSTANCE \
   || (isolation) == TRAN_COMMIT_CLASS_COMMIT_INSTANCE \
   || ((isolation) == TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE \
       && (entry_ptr)->res_head->type == LOCK_RESOURCE_CLASS))

#define EXPAND_WAIT_FOR_ARRAY_IF_NEEDED()                               \
  do                                                                    \
  {                                                                     \
    if (nwaits == max_waits)                                            \
      {                                                                 \
	if (wait_for == wait_for_buf)                                   \
	  {                                                             \
	    t = (int *) malloc (sizeof (int) * max_waits * 2);          \
	    if (t != NULL)                                              \
	      {                                                         \
		memcpy (t, wait_for, sizeof (int) * max_waits);    \
              }                                                         \
          }                                                             \
	else                                                            \
	  {                                                             \
	    t = (int *) realloc (wait_for, sizeof (int) * max_waits * 2);\
          }                                                             \
	if (t != NULL)                                                  \
	  {                                                             \
            wait_for = t;                                               \
            max_waits *= 2;                                             \
          }                                                             \
	else                                                            \
	  {                                                             \
            goto set_error;                                             \
          }                                                             \
      }                                                                 \
  }                                                                     \
  while (0)
#endif /* SERVER_MODE */

#define SET_SCANID_BIT(s, i)    (s[i/8] |= (1 << (i%8)))
#define RESET_SCANID_BIT(s, i)  (s[i/8] &= ~(1 << (i%8)))
#define IS_SCANID_BIT_SET(s, i) (s[i/8] & (1 << (i%8)))
#define RESOURCE_ALLOC_WAIT_TIME 10000	/* 10 msec */

/* type of locking resource */
typedef enum
{
  LOCK_RESOURCE_INSTANCE,	/* An instance resource */
  LOCK_RESOURCE_CLASS,		/* A class resource */
  LOCK_RESOURCE_ROOT_CLASS,	/* A root class resource */
  LOCK_RESOURCE_OBJECT		/* An object resource */
} LOCK_RESOURCE_TYPE;

/* state of suspended threads */
typedef enum
{
  LOCK_SUSPENDED,		/* Thread has been suspended */
  LOCK_RESUMED,			/* Thread has been resumed */
  LOCK_RESUMED_TIMEOUT,		/* Thread has been resumed and notified of
				   lock timeout */
  LOCK_RESUMED_DEADLOCK_TIMEOUT,	/* Thread has been resumed and notified of
					   lock timeout because the current transaction
					   is selected as a deadlock victim */
  LOCK_RESUMED_ABORTED,		/* Thread has been resumed, however
				   it must be aborted because of a deadlock */
  LOCK_RESUMED_ABORTED_FIRST,	/* in case of the first aborted thread */
  LOCK_RESUMED_ABORTED_OTHER,	/* in case of other aborted threads */
  LOCK_RESUMED_INTERRUPT
} LOCK_WAIT_STATE;

/*
 * Message id in the set MSGCAT_SET_LOCK
 * in the message catalog MSGCAT_CATALOG_CUBRID (file cubrid.msg).
 */
#define MSGCAT_LK_NEWLINE                       1
#define MSGCAT_LK_SUSPEND_TRAN                  2
#define MSGCAT_LK_RESUME_TRAN                   3
#define MSGCAT_LK_OID_LOCK_ACQUIRED             4
#define MSGCAT_LK_VPID_LOCK_ACQUIRED            5
#define MSGCAT_LK_OID_LOCK_CONVERTED            6
#define MSGCAT_LK_VPID_LOCK_CONVERTED           7
#define MSGCAT_LK_OID_LOCK_WAITFOR              8
#define MSGCAT_LK_VPID_LOCK_WAITFOR             9
#define MSGCAT_LK_OID_LOCK_RELEASE              10
#define MSGCAT_LK_VPID_LOCK_RELEASE             11
#define MSGCAT_LK_OID_LOCK_DEMOTE               12
#define MSGCAT_LK_VPID_LOCK_DEMOTE              13
#define MSGCAT_LK_RES_OID                       14
#define MSGCAT_LK_RES_ROOT_CLASS_TYPE           15
#define MSGCAT_LK_RES_CLASS_TYPE                16
#define MSGCAT_LK_RES_INSTANCE_TYPE             17
#define MSGCAT_LK_RES_UNKNOWN_TYPE              18
#define MSGCAT_LK_RES_TOTAL_MODE                19
#define MSGCAT_LK_RES_LOCK_COUNT                20
#define MSGCAT_LK_RES_NON_BLOCKED_HOLDER_HEAD   21
#define MSGCAT_LK_RES_BLOCKED_HOLDER_HEAD       22
#define MSGCAT_LK_RES_BLOCKED_WAITER_HEAD       23
#define MSGCAT_LK_RES_NON2PL_RELEASED_HEAD      24
#define MSGCAT_LK_RES_NON_BLOCKED_HOLDER_ENTRY  25
#define MSGCAT_LK_RES_NON_BLOCKED_HOLDER_ENTRY_WITH_GRANULE 26
#define MSGCAT_LK_RES_BLOCKED_HOLDER_ENTRY      27
#define MSGCAT_LK_RES_BLOCKED_HOLDER_ENTRY_WITH_GRANULE 28
#define MSGCAT_LK_RES_BLOCKED_WAITER_ENTRY      29
#define MSGCAT_LK_RES_NON2PL_RELEASED_ENTRY     30
#define MSGCAT_LK_RES_VPID                      31
#define MSGCAT_LK_DUMP_LOCK_TABLE               32
#define MSGCAT_LK_DUMP_TRAN_IDENTIFIERS         33
#define MSGCAT_LK_DUMP_TRAN_ISOLATION           34
#define MSGCAT_LK_DUMP_TRAN_STATE               35
#define MSGCAT_LK_DUMP_TRAN_TIMEOUT_PERIOD      36
#define MSGCAT_LK_DEADLOCK_ABORT_HDR            37
#define MSGCAT_LK_DEADLOCK_ABORT                38
#define MSGCAT_LK_DEADLOCK_TIMEOUT_HDR          39
#define MSGCAT_LK_DEADLOCK_TIMEOUT              40
#define MSGCAT_LK_DEADLOCK_FUN_HDR              41
#define MSGCAT_LK_DEADLOCK_FUN                  42
#define MSGCAT_LK_LASTONE                       43

#if defined(SERVER_MODE)

typedef struct lk_lockinfo LK_LOCKINFO;
struct lk_lockinfo
{
  OID *org_oidp;
  OID oid;
  OID class_oid;
  LOCK lock;
};

/* composite locking for delete and update operation */
typedef struct lk_lockcomp_class LK_LOCKCOMP_CLASS;
struct lk_lockcomp_class
{
  OID class_oid;
  LK_ENTRY *class_lock_ptr;
  int num_inst_oids;
  int max_inst_oids;
  OID *inst_oid_space;
  LK_LOCKCOMP_CLASS *next;
};

typedef struct lk_lockcomp LK_LOCKCOMP;
struct lk_lockcomp
{
  int tran_index;
  int waitsecs;
  LK_ENTRY *root_class_ptr;
  LK_LOCKCOMP_CLASS *class_list;
};

/* TWFG (transaction wait-for graph) entry and edge */
typedef struct lk_WFG_node LK_WFG_NODE;
struct lk_WFG_node
{
  int first_edge;
  int candidate;
  int current;
  int ancestor;
  time_t thrd_wait_stime;
  int tran_edge_seq_num;
  bool checked_by_deadlock_detector;
  bool DL_victim;
};

typedef struct lk_WFG_edge LK_WFG_EDGE;
struct lk_WFG_edge
{
  int to_tran_index;
  int edge_seq_num;
  int holder_flag;
  int next;
  time_t edge_wait_stime;
};

typedef struct lk_deadlock_victim LK_DEADLOCK_VICTIM;
struct lk_deadlock_victim
{
  /* following two fields are used for only global deadlock detection */
  int (*cycle_fun) (int tran_index, void *args);
  void *args;			/* Arguments to be passed to cycle_fun */

  int tran_index;		/* Index of selected victim */
  TRANID tranid;		/* Transaction identifier   */
  int can_timeout;		/* Is abort or timeout      */
};

/*
 * Lock Resource Entry Structure
 */
typedef struct lk_res LK_RES;
struct lk_res
{
  MUTEX_T res_mutex;		/* resource mutex */
  LOCK_RESOURCE_TYPE type;	/* type of resource: class,instance */
  OID oid;
  OID class_oid;
  LOCK total_holders_mode;	/* total mode of the holders */
  LOCK total_waiters_mode;	/* total mode of the waiters */
  LK_ENTRY *holder;		/* lock holder list */
  LK_ENTRY *waiter;		/* lock waiter list */
  LK_ENTRY *non2pl;		/* non2pl list */
  LK_RES *hash_next;		/* for hash chain */
};

/*
 * Lock Hash Entry Structure
 */
typedef struct lk_hash LK_HASH;
struct lk_hash
{
  MUTEX_T hash_mutex;		/* hash mutex of the hash chain */
  LK_RES *hash_next;		/* next resource in the hash chain */
};

/*
 * Lock Entry Block Structure
 */
typedef struct lk_entry_block LK_ENTRY_BLOCK;
struct lk_entry_block
{
  LK_ENTRY_BLOCK *next_block;	/* next lock entry block */
  LK_ENTRY *block;		/* lk_entry block */
  int count;			/* # of entries in lock entry block */
};

/*
 * Lock Resource Block Structure
 */
typedef struct lk_res_block LK_RES_BLOCK;
struct lk_res_block
{
  LK_RES_BLOCK *next_block;	/* next lock resource block */
  LK_RES *block;		/* lk_res block */
  int count;			/* # of entries in lock res block */
};

/*
 * Transaction Lock Entry Structure
 */
typedef struct lk_tran_lock LK_TRAN_LOCK;
struct lk_tran_lock
{
  /* transaction lock hold lists */
  MUTEX_T hold_mutex;		/* mutex for hold lists */
  LK_ENTRY *inst_hold_list;	/* instance lock hold list */
  LK_ENTRY *class_hold_list;	/* class lock hold list */
  LK_ENTRY *root_class_hold;	/* root class lock hold */
  int inst_hold_count;		/* # of entries in inst_hold_list */
  int class_hold_count;		/* # of entries in class_hold_list */

  /* non two phase lock list */
  MUTEX_T non2pl_mutex;		/* mutex for non2pl_list */
  LK_ENTRY *non2pl_list;	/* non2pl list */
  int num_incons_non2pl;	/* # of inconsistent non2pl */

  /* lock escalation related fields */
  bool lock_escalation_on;

  /* locking on manual duration */
  bool is_instant_duration;
};

/*
 * Lock Manager Global Data Structure
 */

typedef struct lk_global_data LK_GLOBAL_DATA;
struct lk_global_data
{
  /* object lock table including hash table */
  int max_obj_locks;		/* max # of object locks */
  int obj_hash_size;		/* size of object hash table */
  LK_HASH *obj_hash_table;	/* object lock hash table */
  MUTEX_T obj_res_block_list_mutex;
  MUTEX_T obj_entry_block_list_mutex;
  MUTEX_T obj_free_res_list_mutex;
  MUTEX_T obj_free_entry_list_mutex;
  LK_RES_BLOCK *obj_res_block_list;	/* lk_res_block list */
  LK_ENTRY_BLOCK *obj_entry_block_list;	/* lk_entry_block list */
  LK_RES *obj_free_res_list;	/* free lk_res list */
  LK_ENTRY *obj_free_entry_list;	/* free lk_entry list */
  int num_obj_res_allocated;

  /* transaction lock table */
  int num_trans;		/* # of transactions */
  LK_TRAN_LOCK *tran_lock_table;	/* transaction lock hold table */

  /* deadlock detection related fields */
  MUTEX_T DL_detection_mutex;
  time_t last_deadlock_run;	/* last deadlock detetion time */
  LK_WFG_NODE *TWFG_node;	/* transaction WFG node */
  LK_WFG_EDGE *TWFG_edge;	/* transaction WFG edge */
  unsigned char *scanid_bitmap;
  int max_TWFG_edge;
  int TWFG_free_edge_idx;
  int global_edge_seq_num;

  /* miscellaneous things */
  short dump_level_when_deadlock;
  short dump_lock_table_count;
  short no_victim_case_count;
  bool verbose_mode;
#if defined(LK_DUMP)
  bool dump_level;
#endif				/* LK_DUMP */
};

LK_GLOBAL_DATA lk_Gl = {
  0, 0, NULL,
  MUTEX_INITIALIZER, MUTEX_INITIALIZER,
  MUTEX_INITIALIZER, MUTEX_INITIALIZER,
  NULL, NULL, NULL, NULL, 0, 0, NULL,
  MUTEX_INITIALIZER, 0, NULL, NULL, NULL, 0, 0, 0,
  0, 0, 0, false
#if defined(LK_DUMP)
    , 0
#endif /* LK_DUMP */
};

/* size of each data structure */
static const int SIZEOF_LK_LOCKINFO = sizeof (LK_LOCKINFO);
static const int SIZEOF_LK_WFG_NODE = sizeof (LK_WFG_NODE);
static const int SIZEOF_LK_WFG_EDGE = sizeof (LK_WFG_EDGE);
static const int SIZEOF_LK_TRAN_LOCK = sizeof (LK_TRAN_LOCK);
/* TODO : change const */
#define SIZEOF_LK_ENTRY (offsetof (LK_ENTRY, scanid_bitset)    \
  + (PRM_LK_MAX_SCANID_BIT / 8))

static const int SIZEOF_LK_RES = sizeof (LK_RES);
static const int SIZEOF_LK_HASH = sizeof (LK_HASH);
static const int SIZEOF_LK_ENTRY_BLOCK = sizeof (LK_ENTRY_BLOCK);
static const int SIZEOF_LK_RES_BLOCK = sizeof (LK_RES_BLOCK);
static const int SIZEOF_LK_ACQOBJ_LOCK = sizeof (LK_ACQOBJ_LOCK);

/* minimum # of locks that are required */
/* TODO : change const */
#define LK_MIN_OBJECT_LOCKS  (MAX_NTRANS * 300)

/* the ratio in the number of lock entries for each entry type */
static const int LK_HASH_RATIO = 8;
static const int LK_RES_RATIO = 1;
static const int LK_ENTRY_RATIO = 5;

/* the lock entry expansion count */
/* TODO : change const */
#define LK_MORE_RES_COUNT  (MAX_NTRANS * 20 * LK_RES_RATIO)
#define LK_MORE_ENTRY_COUNT (MAX_NTRANS * 20 * LK_ENTRY_RATIO)

/* miscellaneous constants */
static const int LK_SLEEP_MAX_COUNT = 3;
#define LK_LOCKINFO_FIXED_COUNT	30
/* TODO : change const */
#define LK_MAX_VICTIM_COUNT  300

/* transaction WFG edge related constants */
static const int LK_MIN_TWFG_EDGE_COUNT = 200;
/* TODO : change const */
#define LK_MID_TWFG_EDGE_COUNT 1000
/* TODO : change const */
#define LK_MAX_TWFG_EDGE_COUNT (MAX_NTRANS * MAX_NTRANS)

#define DEFAULT_WAIT_USERS	10
static const int LK_COMPOSITE_LOCK_OID_INCREMENT = 100;
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
static LK_WFG_EDGE TWFG_edge_block[LK_MID_TWFG_EDGE_COUNT];
static LK_DEADLOCK_VICTIM victims[LK_MAX_VICTIM_COUNT];
static int victim_count;

#else /* !SERVER_MODE */
static int lk_Standalone_has_xlock = 0;

#endif /* !SERVER_MODE */

#if defined(SERVER_MODE)
static unsigned int lock_get_hash_value (const OID * oid);
static int lock_initialize_tran_lock_table (void);
static int lock_initialize_object_hash_table (void);
static int lock_initialize_object_lock_res_list (void);
static int lock_initialize_object_lock_entry_list (void);
static int lock_initialize_deadlock_detection (void);
static int lock_initialize_scanid_bitmap (void);
static int lock_final_scanid_bitmap (void);
static int lock_alloc_scanid_bit (THREAD_ENTRY * thread_p);
static void lock_free_scanid_bit (THREAD_ENTRY * thread_p, int idx);
#if 0				/* TODO: not used */
static void lk_free_all_scanid_bit (void);
#endif
static LK_RES *lock_alloc_resource (void);
static void lock_free_resource (LK_RES * res_ptr);
static int lock_alloc_resource_block (void);
static LK_ENTRY *lock_alloc_entry (void);
static void lock_free_entry (LK_ENTRY * entry_ptr);
static int lock_alloc_entry_block (void);
static int lock_dealloc_resource (LK_RES * res_ptr);
static void lock_insert_into_tran_hold_list (LK_ENTRY * entry_ptr);
static int lock_delete_from_tran_hold_list (LK_ENTRY * entry_ptr);
static void lock_insert_into_tran_non2pl_list (LK_ENTRY * non2pl);
static int lock_delete_from_tran_non2pl_list (LK_ENTRY * non2pl);
static LK_ENTRY *lock_find_class_entry (int tran_index,
					const OID * class_oid);
static LK_ENTRY *lock_find_inst_entry (int tran_index, const OID * inst_oid);
static LK_ENTRY *lock_add_non2pl_lock (LK_RES * res_ptr, int tran_index,
				       LOCK lock);
static void lock_position_holder_entry (LK_RES * res_ptr,
					LK_ENTRY * entry_ptr);
static void lock_set_error_for_timeout (THREAD_ENTRY * thread_p,
					LK_ENTRY * entry_ptr);
static void lock_set_error_for_aborted (LK_ENTRY * entry_ptr);
static LOCK_WAIT_STATE lock_suspend (THREAD_ENTRY * thread_p,
				     LK_ENTRY * entry_ptr, int waitsecs);
static void lock_resume (LK_ENTRY * entry_ptr, int state);
static bool lock_wakeup_deadlock_victim_timeout (int tran_index);
static bool lock_wakeup_deadlock_victim_aborted (int tran_index);
static void lock_grant_blocked_holder (THREAD_ENTRY * thread_p,
				       LK_RES * res_ptr);
static int lock_grant_blocked_waiter (THREAD_ENTRY * thread_p,
				      LK_RES * res_ptr);
static void lock_grant_blocked_waiter_partial (THREAD_ENTRY * thread_p,
					       LK_RES * res_ptr,
					       LK_ENTRY * from_whom);
static int lock_escalate_if_needed (THREAD_ENTRY * thread_p,
				    LK_ENTRY * class_entry, int tran_index,
				    int waitsecs);
static int lock_internal_hold_lock_object_instant (int tran_index,
						   const OID * oid,
						   const OID * class_oid,
						   LOCK lock);
static int lock_internal_perform_lock_object (THREAD_ENTRY * thread_p,
					      int tran_index, const OID * oid,
					      const OID * class_oid,
					      LOCK lock, int waitsecs,
					      LK_ENTRY ** entry_addr_ptr,
					      LK_ENTRY * class_entry);
static void lock_internal_perform_unlock_object (THREAD_ENTRY * thread_p,
						 LK_ENTRY * entry_ptr,
						 int release_flag,
						 int move_to_non2pl);
static int lock_internal_demote_shared_class_lock (THREAD_ENTRY * thread_p,
						   LK_ENTRY * entry_ptr);
static int lock_internal_demote_shared_inst_lock (THREAD_ENTRY * thread_p,
						  LK_ENTRY * entry_ptr);
static int lock_internal_demote_update_inst_lock (THREAD_ENTRY * thread_p,
						  LK_ENTRY * entry_ptr);
static void lock_demote_shared_class_lock (THREAD_ENTRY * thread_p,
					   int tran_index,
					   const OID * class_oid);
static void lock_demote_shared_inst_locks (THREAD_ENTRY * thread_p,
					   int tran_index, int scanid_bit);
static void lock_demote_all_shared_class_locks (THREAD_ENTRY * thread_p,
						int tran_index);
static void lock_unlock_shared_class_lock (THREAD_ENTRY * thread_p,
					   int tran_index,
					   const OID * class_oid);
static void lock_unlock_shared_inst_lock (THREAD_ENTRY * thread_p,
					  int tran_index,
					  const OID * inst_oid);
static void lock_remove_all_class_locks (THREAD_ENTRY * thread_p,
					 int tran_index, LOCK lock);
static void lock_remove_non2pl (LK_ENTRY * non2pl, int tran_index);
static void lock_update_non2pl_list (LK_RES * res_ptr, int tran_index,
				     LOCK lock);
static int lock_add_WFG_edge (int from_tran_index, int to_tran_index,
			      int holder_flag, time_t edge_wait_stime);
static void lock_select_deadlock_victim (THREAD_ENTRY * thread_p, int s,
					 int t);
static void lock_dump_deadlock_victims (THREAD_ENTRY * thread_p);
static int lock_compare_lock_info (const void *lockinfo1,
				   const void *lockinfo2);
static void lock_dump_resource (THREAD_ENTRY * thread_p, FILE * outfp,
				LK_RES * res_ptr);
#if defined (ENABLE_UNUSED_FUNCTION)
static bool lock_check_consistent_resource (THREAD_ENTRY * thread_p,
					    LK_RES * res_ptr);
static bool lock_check_consistent_tran_lock (LK_TRAN_LOCK * tran_lock);
#endif
static int lock_find_scanid_bit (unsigned char *scanid_bit);
static void lock_remove_all_inst_locks_with_scanid (THREAD_ENTRY * thread_p,
						    int tran_index,
						    int scanid_bit,
						    LOCK lock);
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_get_hash_value -
 *
 * return:
 *
 *   oid(in):
 */
static unsigned int
lock_get_hash_value (const OID * oid)
{
  int next_base_slotid, addr;

  if (oid->slotid <= 0)
    {
      /* In an unique index, the OID and ClassOID of the last key are
       *   <root page's volid, root page's pageid, -1> and
       *   <root page's volid, root page's pageid, 0>, recpectively.
       * In a non-unique index, the OID of the last key is
       *   <root page's volid, root page's pageid, -1>
       */
      addr = oid->pageid - oid->slotid;
    }
  else
    {
      next_base_slotid = 2;
      while (next_base_slotid <= oid->slotid)
	{
	  next_base_slotid = next_base_slotid * 2;
	}

      addr = oid->pageid + (lk_Gl.obj_hash_size / next_base_slotid) *
	(2 * oid->slotid - next_base_slotid + 1);
    }

  return (addr % lk_Gl.obj_hash_size);
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group 1: initalize and finalize major structures
 *
 *   - lock_init_tran_lock_table()
 *   - lock_init_object_hash_table()
 *   - lock_init_object_lock_res_list()
 *   - lock_init_object_lock_entry_list()
 *   - lock_init_deadlock_detection()
 *   - lock_init_scanid_bitmap()
 */

#if defined(SERVER_MODE)
/*
 * lock_initialize_tran_lock_table - Initialize the transaction lock hold table.
 *
 * return: error code
 *
 * Note:This function allocates the transaction lock hold table and
 *     initializes the table.
 */
static int
lock_initialize_tran_lock_table (void)
{
  LK_TRAN_LOCK *tran_lock;	/* pointer to transaction hold entry */
  int i;			/* loop variable                     */

  /* initialize the number of transactions */
  lk_Gl.num_trans = MAX_NTRANS;

  /* allocate memory space for transaction lock table */
  lk_Gl.tran_lock_table =
    (LK_TRAN_LOCK *) malloc (SIZEOF_LK_TRAN_LOCK * lk_Gl.num_trans);
  if (lk_Gl.tran_lock_table == (LK_TRAN_LOCK *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (SIZEOF_LK_TRAN_LOCK * lk_Gl.num_trans));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize all the entries of transaction lock table */
  memset (lk_Gl.tran_lock_table, 0, SIZEOF_LK_TRAN_LOCK * lk_Gl.num_trans);
  for (i = 0; i < lk_Gl.num_trans; i++)
    {
      tran_lock = &lk_Gl.tran_lock_table[i];
      MUTEX_INIT (tran_lock->hold_mutex);
      MUTEX_INIT (tran_lock->non2pl_mutex);
    }

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_initialize_object_hash_table - Initializes the object lock hash table
 *
 * return: error code
 *
 * Note:This function initializes an object lock hash table.
 */
static int
lock_initialize_object_hash_table (void)
{
#define LK_INITIAL_OBJECT_LOCK_TABLE_SIZE       10000
  LK_HASH *hash_anchor;		/* pointer to object lock hash entry */
  int i;

  lk_Gl.max_obj_locks = LK_INITIAL_OBJECT_LOCK_TABLE_SIZE;

  /* allocate an object lock hash table */
  if (lk_Gl.max_obj_locks > LK_MIN_OBJECT_LOCKS)
    {
      lk_Gl.obj_hash_size = lk_Gl.max_obj_locks * LK_HASH_RATIO;
    }
  else
    {
      lk_Gl.obj_hash_size = LK_MIN_OBJECT_LOCKS * LK_HASH_RATIO;
    }

  lk_Gl.obj_hash_table =
    (LK_HASH *) malloc (SIZEOF_LK_HASH * lk_Gl.obj_hash_size);
  if (lk_Gl.obj_hash_table == (LK_HASH *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (SIZEOF_LK_HASH * lk_Gl.obj_hash_size));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize all buckets of the object lock hash table */
  for (i = 0; i < lk_Gl.obj_hash_size; i++)
    {
      hash_anchor = &lk_Gl.obj_hash_table[i];
      MUTEX_INIT (hash_anchor->hash_mutex);
      hash_anchor->hash_next = (LK_RES *) NULL;
    }

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_initialize_object_lock_res_list - Initializes the object lock resource list
 *
 * return: error code
 *
 * Note:
 *     This function initializes following two lists.
 *     1. a list of object lock resource block
 *        => each block has object lock resource block.
 *     2. a list of freed object lock resource entries.
 */
static int
lock_initialize_object_lock_res_list (void)
{
  LK_RES_BLOCK *res_block;	/* pointer to lock resource block */
  LK_RES *res_ptr = NULL;	/* pointer to lock entry          */
  int i;

  /* allocate an object lock resource block */
  res_block = (LK_RES_BLOCK *) malloc (SIZEOF_LK_RES_BLOCK);
  if (res_block == (LK_RES_BLOCK *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, SIZEOF_LK_RES_BLOCK);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock resource block */
  res_block->next_block = (LK_RES_BLOCK *) NULL;
  res_block->count = MAX ((lk_Gl.max_obj_locks * LK_RES_RATIO), 1);
  res_block->block = (LK_RES *) malloc (SIZEOF_LK_RES * res_block->count);
  if (res_block->block == (LK_RES *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (SIZEOF_LK_RES * res_block->count));
      free_and_init (res_block);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock resource in the block */
  for (i = 0; i < res_block->count; i++)
    {
      res_ptr = &res_block->block[i];
      LK_INIT_RES (res_ptr);
      res_ptr->hash_next = &res_block->block[i + 1];
    }
  res_ptr->hash_next = (LK_RES *) NULL;

  /* initialize the object lock resource node list */
  MUTEX_INIT (lk_Gl.obj_res_block_list_mutex);
  lk_Gl.obj_res_block_list = res_block;

  /* initialize the object lock resource free list */
  MUTEX_INIT (lk_Gl.obj_free_res_list_mutex);
  lk_Gl.obj_free_res_list = &res_block->block[0];

  lk_Gl.num_obj_res_allocated = 0;

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lockk_initialize_object_lock_entry_list - Initializes the object lock entry list
 *
 * return: error code
 *
 * Note:
 *     This function initializes following two lists.
 *     1. a list of object lock entry block
 *        => each node has object lock entry block.
 *     2. a list of freed object lock entries.
 */
static int
lock_initialize_object_lock_entry_list (void)
{
  LK_ENTRY_BLOCK *entry_block;	/* pointer to lock entry block */
  LK_ENTRY *entry_ptr = NULL;	/* pointer to lock entry  */
  int i;

  /* allocate an object lock entry block */
  entry_block = (LK_ENTRY_BLOCK *) malloc (SIZEOF_LK_ENTRY_BLOCK);
  if (entry_block == (LK_ENTRY_BLOCK *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, SIZEOF_LK_ENTRY_BLOCK);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock entry block */
  entry_block->next_block = (LK_ENTRY_BLOCK *) NULL;
  entry_block->count = MAX ((lk_Gl.max_obj_locks * LK_ENTRY_RATIO), 1);
  entry_block->block =
    (LK_ENTRY *) malloc (SIZEOF_LK_ENTRY * entry_block->count);
  if (entry_block->block == (LK_ENTRY *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (SIZEOF_LK_ENTRY * entry_block->count));
      free_and_init (entry_block);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock entries in the block */
  for (i = 0; i < entry_block->count; i++)
    {
      entry_ptr = (LK_ENTRY *)
	(((char *) entry_block->block) + SIZEOF_LK_ENTRY * i);
      LK_INIT_ENTRY (entry_ptr);
      entry_ptr->next = (LK_ENTRY *) (((char *) entry_ptr) + SIZEOF_LK_ENTRY);
    }
  entry_ptr->next = (LK_ENTRY *) NULL;

  /* initialize the object lock entry block list */
  MUTEX_INIT (lk_Gl.obj_entry_block_list_mutex);
  lk_Gl.obj_entry_block_list = entry_block;

  /* initialize the object lock entry free list */
  MUTEX_INIT (lk_Gl.obj_free_entry_list_mutex);
  lk_Gl.obj_free_entry_list = &entry_block->block[0];

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_initialize_deadlock_detection - Initializes transaction wait-for graph.
 *
 * return: error code
 *
 * Note:This function initializes the transaction waif-for graph.
 */
static int
lock_initialize_deadlock_detection (void)
{
  int i;

  MUTEX_INIT (lk_Gl.DL_detection_mutex);
  lk_Gl.last_deadlock_run = time (NULL);

  /* allocate transaction WFG node table */
  lk_Gl.TWFG_node =
    (LK_WFG_NODE *) malloc (SIZEOF_LK_WFG_NODE * lk_Gl.num_trans);
  if (lk_Gl.TWFG_node == (LK_WFG_NODE *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (SIZEOF_LK_WFG_NODE * lk_Gl.num_trans));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  /* initialize transaction WFG node table */
  for (i = 0; i < lk_Gl.num_trans; i++)
    {
      lk_Gl.TWFG_node[i].DL_victim = false;
      lk_Gl.TWFG_node[i].checked_by_deadlock_detector = false;
      lk_Gl.TWFG_node[i].thrd_wait_stime = 0;
    }

  /* initialize other related fields */
  lk_Gl.TWFG_edge = (LK_WFG_EDGE *) NULL;
  lk_Gl.max_TWFG_edge = 0;
  lk_Gl.TWFG_free_edge_idx = -1;
  lk_Gl.global_edge_seq_num = 0;

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_initialize_scanid_bitmap -
 *
 * return:
 *
 * Note: do not use system transaction(0 tran_index) bitmap
 */
static int
lock_initialize_scanid_bitmap (void)
{
  size_t nbytes = 1 + (MAX_NTRANS - 1) * PRM_LK_MAX_SCANID_BIT / 8;
  lk_Gl.scanid_bitmap = (unsigned char *) malloc (nbytes);
  if (lk_Gl.scanid_bitmap == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  memset (lk_Gl.scanid_bitmap, 0, nbytes);

  return NO_ERROR;
}

/*
 * lock_final_scanid_bitmap -
 *
 * return: NO_ERROR
 */
static int
lock_final_scanid_bitmap (void)
{
  if (lk_Gl.scanid_bitmap)
    {
      free_and_init (lk_Gl.scanid_bitmap);
    }

  return NO_ERROR;
}

/*
 * lock_alloc_scanid_bit -
 *
 * return:
 */
static int
lock_alloc_scanid_bit (THREAD_ENTRY * thread_p)
{
  int i;
  int tran_index;
  unsigned char *scanid_bit;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  scanid_bit = &lk_Gl.scanid_bitmap[tran_index * PRM_LK_MAX_SCANID_BIT / 8];

  if (csect_enter (thread_p, CSECT_SCANID_BITMAP, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }
  for (i = 0; i < PRM_LK_MAX_SCANID_BIT; i++)
    {
      if (!IS_SCANID_BIT_SET (scanid_bit, i))
	{
	  SET_SCANID_BIT (scanid_bit, i);
	  break;
	}
    }
  if (i == PRM_LK_MAX_SCANID_BIT)
    {
      csect_exit (CSECT_SCANID_BITMAP);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NOT_ENOUGH_SCANID_BIT, 1,
	      PRM_LK_MAX_SCANID_BIT + 1);
      return ER_NOT_ENOUGH_SCANID_BIT;
    }
  csect_exit (CSECT_SCANID_BITMAP);
  return i;
}

/*
 * lock_free_scanid_bit -
 *
 * return:
 *
 *   idx(in):
 */
static void
lock_free_scanid_bit (THREAD_ENTRY * thread_p, int idx)
{
  int tran_index;
  unsigned char *scanid_bit;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  scanid_bit = &lk_Gl.scanid_bitmap[tran_index * PRM_LK_MAX_SCANID_BIT / 8];

  if (csect_enter (thread_p, CSECT_SCANID_BITMAP, INF_WAIT) != NO_ERROR)
    {
      return;
    }
  RESET_SCANID_BIT (scanid_bit, idx);
  csect_exit (CSECT_SCANID_BITMAP);
}

#endif /* SERVER_MODE */

/*
 *  Private Functions Group: lock resource and entry management
 *   - lk_alloc_resource()
 *   - lk_free_resource()
 *   - lk_alloc_entry()
 *   - lk_free_entry()
 *   - lk_dealloc_resource()
 */

#if defined(SERVER_MODE)
/*
 * lock_alloc_resource - Allocate a lock resource entry
 *
 * return:  an allocated lock resource entry or NULL
 *
 * Note:This function allocates a lock resource entry and returns it.
 *     At first, it allocates the lock resource entry
 *     from the free list of lock resource entries.
 *     If the free list is empty, this function allocates a new set of
 *     lock resource entries, connects them into the free list and then
 *     allocates one entry from the free list.
 */
static LK_RES *
lock_alloc_resource (void)
{
  int count_try_alloc_entry;
  int count_try_alloc_table;
  LK_RES *res_ptr;
  int rv;

  /* The caller is holding a hash mutex. The reason for holding
   * the hash mutex is to prevent other transactions from
   * allocating lock resource entry on the same lock resource.
   */
  /* 1. allocate a lock resource entry from the free list if possible */
  count_try_alloc_entry = 0;

try_alloc_entry_again:

  MUTEX_LOCK (rv, lk_Gl.obj_free_res_list_mutex);

  if (lk_Gl.obj_free_res_list != (LK_RES *) NULL)
    {
      res_ptr = lk_Gl.obj_free_res_list;
      lk_Gl.obj_free_res_list = res_ptr->hash_next;
      lk_Gl.num_obj_res_allocated++;

      MUTEX_UNLOCK (lk_Gl.obj_free_res_list_mutex);
#if defined(LK_DUMP)
      if (lk_Gl.dump_level >= 2)
	{
	  fprintf (stderr, "LK_DUMP::lk_alloc_resource() = %p\n", res_ptr);
	}
#endif /* LK_DUMP */
      return res_ptr;
    }

  if (count_try_alloc_entry < LK_SLEEP_MAX_COUNT)
    {
      MUTEX_UNLOCK (lk_Gl.obj_free_res_list_mutex);

      count_try_alloc_entry++;

      (void) thread_sleep (0, RESOURCE_ALLOC_WAIT_TIME);
      goto try_alloc_entry_again;
    }

  /* Currently, the current thread is holding
   * both hash_mutex and obj_free_res_list_mutex.
   */
  count_try_alloc_table = 0;

try_alloc_table_again:

  /* 2. if the free list is empty, allocate a lock resource block */
  if (lock_alloc_resource_block () == NO_ERROR)
    {
      /* Now, the lock resource free list is not empty. */
      res_ptr = lk_Gl.obj_free_res_list;
      lk_Gl.obj_free_res_list = res_ptr->hash_next;
      lk_Gl.num_obj_res_allocated++;

      MUTEX_UNLOCK (lk_Gl.obj_free_res_list_mutex);
#if defined(LK_DUMP)
      if (lk_Gl.dump_level >= 2)
	{
	  fprintf (stderr, "LK_DUMP::lk_alloc_resource() = %p\n", res_ptr);
	}
#endif /* LK_DUMP */
      return res_ptr;
    }

  /*
   * Memory allocation fails.: What should we do ??
   *  - notify DBA or applications of current situation.
   */
  if (count_try_alloc_table < LK_SLEEP_MAX_COUNT)
    {
      /* we should notify DBA or applications of insufficient memory space */
      count_try_alloc_table++;

      (void) thread_sleep (0, RESOURCE_ALLOC_WAIT_TIME);
      goto try_alloc_table_again;
    }

  MUTEX_UNLOCK (lk_Gl.obj_free_res_list_mutex);

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 2)
    {
      fprintf (stderr, "LK_DUMP::lk_alloc_resource() = NULL\n");
    }
#endif /* LK_DUMP */
  return (LK_RES *) NULL;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_free_resource - Free the given lock resource entry
 *
 * return:
 *
 *   res_ptr(in):
 *
 * Note:
 *     This functions initializes the lock resource entry as freed state
 *     and returns it into free lock resource entry list.
 */
static void
lock_free_resource (LK_RES * res_ptr)
{
  int rv;
#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 2)
    {
      fprintf (stderr, "LK_DUMP::lk_free_resource(%p)\n", res_ptr);
    }
#endif /* LK_DUMP */
  /* The caller is not holding any mutex. */

  /* set the lock resource entry as free state */
  res_ptr->type = LOCK_RESOURCE_OBJECT;
  OID_SET_NULL (&res_ptr->oid);

  /* connect it into the free list of lock resource entries */
  MUTEX_LOCK (rv, lk_Gl.obj_free_res_list_mutex);

  res_ptr->hash_next = lk_Gl.obj_free_res_list;
  lk_Gl.obj_free_res_list = res_ptr;
  lk_Gl.num_obj_res_allocated--;

  MUTEX_UNLOCK (lk_Gl.obj_free_res_list_mutex);

}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_alloc_resource_block - Allocate a lock resource block
 *
 * return: error code
 *     the reason of failure: memory allocation failure
 *
 * Note:This function allocates a lock resource block which has
 *     a set of lock resource entries, connects the block into the resource
 *     block list, initializes the lock resource entries, and then
 *     connects them into the free list.
 */
static int
lock_alloc_resource_block (void)
{
  LK_RES_BLOCK *res_block;
  int i;
  LK_RES *res_ptr = NULL;
  int rv;

  /* The caller is holding a hash mutex and resource free list mutex */

  /* allocate an object lock resource block */
  res_block = (LK_RES_BLOCK *) malloc (SIZEOF_LK_RES_BLOCK);
  if (res_block == (LK_RES_BLOCK *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (SIZEOF_LK_RES_BLOCK));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  res_block->count = MAX (LK_MORE_RES_COUNT, 1);
  res_block->block = (LK_RES *) malloc (SIZEOF_LK_RES * res_block->count);
  if (res_block->block == (LK_RES *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (SIZEOF_LK_RES * res_block->count));
      free_and_init (res_block);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock resource in the block */
  for (i = 0; i < res_block->count; i++)
    {
      res_ptr = &res_block->block[i];
      LK_INIT_RES (res_ptr);
      res_ptr->hash_next = &res_block->block[i + 1];
    }
  res_ptr->hash_next = (LK_RES *) NULL;

  /* connect the allocated node into the node list */
  MUTEX_LOCK (rv, lk_Gl.obj_res_block_list_mutex);

  res_block->next_block = lk_Gl.obj_res_block_list;
  lk_Gl.obj_res_block_list = res_block;

  MUTEX_UNLOCK (lk_Gl.obj_res_block_list_mutex);

  /* connet the allocated entries into the free list */
  res_ptr->hash_next = lk_Gl.obj_free_res_list;
  lk_Gl.obj_free_res_list = &res_block->block[0];

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_alloc_entry - Allocate a lock entry
 *
 * return: LK_ENTRY *
 *     allocated lock entry or NULL
 *
 * Note:This function allocates a lock entry and returns it.
 *     At first, it allocates the lock entry
 *     from the free list of lock entries.
 *     If the free list is empty, this function allocates
 *     a set of lock entries, connects them into the free list
 *     and then allocates one entry from the free list.
 */
static LK_ENTRY *
lock_alloc_entry (void)
{
  int count_try_alloc_entry;
  int count_try_alloc_table;
  LK_ENTRY *entry_ptr;
  int rv;

  /* The caller is holding a resource mutex */

  /* 1. allocate an lock entry from the free list */
  count_try_alloc_entry = 0;

  MUTEX_LOCK (rv, lk_Gl.obj_free_entry_list_mutex);
  while (lk_Gl.obj_free_entry_list == (LK_ENTRY *) NULL
	 && count_try_alloc_entry < LK_SLEEP_MAX_COUNT)
    {
      MUTEX_UNLOCK (lk_Gl.obj_free_entry_list_mutex);
      count_try_alloc_entry++;

      (void) thread_sleep (0, RESOURCE_ALLOC_WAIT_TIME);
      MUTEX_LOCK (rv, lk_Gl.obj_free_entry_list_mutex);
    }

  if (lk_Gl.obj_free_entry_list != (LK_ENTRY *) NULL)
    {
      entry_ptr = lk_Gl.obj_free_entry_list;
      lk_Gl.obj_free_entry_list = entry_ptr->next;
      MUTEX_UNLOCK (lk_Gl.obj_free_entry_list_mutex);
#if defined(LK_DUMP)
      if (lk_Gl.dump_level >= 2)
	{
	  fprintf (stderr, "LK_DUMP::lk_alloc_entry() = %p\n", entry_ptr);
	}
#endif /* LK_DUMP */
      memset (&entry_ptr->scanid_bitset, 0, PRM_LK_MAX_SCANID_BIT / 8);
      return entry_ptr;
    }

  /* Currently, the current thread is holding
   * both res_mutex and obj_free_entry_list_mutex.
   */
  count_try_alloc_table = 0;

  /* 2. if the free list is empty, allocate a new lock entry block */
  while (lock_alloc_entry_block () != NO_ERROR
	 && count_try_alloc_table < LK_SLEEP_MAX_COUNT)
    {
      /*
       * Memory allocation fails.: What should we do ??
       *  - notify DBA or applications of current situation.
       */

      /* should notify DBA or applications of insufficient memory space */
      count_try_alloc_table++;

      (void) thread_sleep (0, RESOURCE_ALLOC_WAIT_TIME);	/* sleep: 0.0001 second */
    }

  if (count_try_alloc_table < LK_SLEEP_MAX_COUNT)
    {
      /* Now, the free list is not empty. */
      entry_ptr = lk_Gl.obj_free_entry_list;
      lk_Gl.obj_free_entry_list = entry_ptr->next;
      MUTEX_UNLOCK (lk_Gl.obj_free_entry_list_mutex);
#if defined(LK_DUMP)
      if (lk_Gl.dump_level >= 2)
	{
	  fprintf (stderr, "LK_DUMP::lk_alloc_entry() = %p\n", entry_ptr);
	}
#endif /* LK_DUMP */
      memset (&entry_ptr->scanid_bitset, 0, PRM_LK_MAX_SCANID_BIT / 8);
      return entry_ptr;
    }

  MUTEX_UNLOCK (lk_Gl.obj_free_entry_list_mutex);

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 2)
    {
      fprintf (stderr, "LK_DUMP::lk_alloc_entry() = NULL\n");
    }
#endif /* LK_DUMP */
  return (LK_ENTRY *) NULL;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_free_entry - Free the given lock entry
 *
 * return:
 *
 *   entry_ptr(in):
 *
 * Note:This functions initializes the lock entry as freed state
 *     and returns it into free lock entry list.
 */
static void
lock_free_entry (LK_ENTRY * entry_ptr)
{
  int rv;
  LK_ACQUISITION_HISTORY *history, *next;

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 2)
    {
      fprintf (stderr, "LK_DUMP::lk_free_entry(%p)\n", entry_ptr);
    }
#endif /* LK_DUMP */
  /* The caller is holding a resource mutex */

  /* set the lock entry as free state */
  entry_ptr->tran_index = -1;

  history = entry_ptr->history;
  while (history)
    {
      next = history->next;
      free_and_init (history);
      history = next;
    }
  entry_ptr->history = NULL;
  entry_ptr->recent = NULL;

  /* connect it into free entry list */
  MUTEX_LOCK (rv, lk_Gl.obj_free_entry_list_mutex);
  entry_ptr->next = lk_Gl.obj_free_entry_list;
  lk_Gl.obj_free_entry_list = entry_ptr;
  MUTEX_UNLOCK (lk_Gl.obj_free_entry_list_mutex);
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_alloc_entry_block - Allocate a lock entry block
 *
 * return: error code
 *
 * Note:This function allocates a lock entry block which has a set of lock
 *     entries, connects the block into the block list, initializes the lock
 *     entries, and then connects them into the free list.
 */
static int
lock_alloc_entry_block (void)
{
  LK_ENTRY_BLOCK *entry_block;
  LK_ENTRY *entry_ptr = NULL;
  int i, rv;

  /* The caller is holding a resource mutex */

  /* allocate an object lock entry block */
  entry_block = (LK_ENTRY_BLOCK *) malloc (SIZEOF_LK_ENTRY_BLOCK);
  if (entry_block == (LK_ENTRY_BLOCK *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (SIZEOF_LK_ENTRY_BLOCK));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  entry_block->count = MAX (LK_MORE_ENTRY_COUNT, 1);
  entry_block->block =
    (LK_ENTRY *) malloc (SIZEOF_LK_ENTRY * entry_block->count);
  if (entry_block->block == (LK_ENTRY *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (SIZEOF_LK_ENTRY * entry_block->count));
      free_and_init (entry_block);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock entry block */
  for (i = 0; i < entry_block->count; i++)
    {
      entry_ptr = (LK_ENTRY *)
	(((char *) entry_block->block) + SIZEOF_LK_ENTRY * i);
      LK_INIT_ENTRY (entry_ptr);
      entry_ptr->next = (LK_ENTRY *) (((char *) entry_ptr) + SIZEOF_LK_ENTRY);
    }
  entry_ptr->next = (LK_ENTRY *) NULL;

  /* connect the allocated node into the entry block */
  MUTEX_LOCK (rv, lk_Gl.obj_entry_block_list_mutex);
  entry_block->next_block = lk_Gl.obj_entry_block_list;
  lk_Gl.obj_entry_block_list = entry_block;
  MUTEX_UNLOCK (lk_Gl.obj_entry_block_list_mutex);

  /* connect the allocated entries into the free list */
  entry_ptr->next = lk_Gl.obj_free_entry_list;
  lk_Gl.obj_free_entry_list = &entry_block->block[0];

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_dealloc_resource - Deallocate lock resource entry
 *
 * return: error code
 *
 *   res_ptr(in):
 *
 * Note:This function deallocates the given lock resource entry
 *     from lock hash table.
 */
static int
lock_dealloc_resource (LK_RES * res_ptr)
{
  bool res_mutex_hold;
  int ret_val;
  unsigned int hash_index;
  LK_HASH *hash_anchor;
  LK_RES *prev, *curr;
  int rv;

  /* The caller is holding a resource mutex, currently. */
  res_mutex_hold = true;

  /* no holders and no waiters */
  /* remove the lock resource entry from the lock hash chain */
  hash_index = LK_OBJ_LOCK_HASH (&res_ptr->oid);
  hash_anchor = &lk_Gl.obj_hash_table[hash_index];

  /* conditional mutex hold request */
  ret_val = MUTEX_TRYLOCK (hash_anchor->hash_mutex);
  if (ret_val != TRYLOCK_SUCCESS)
    {				/* I could not hold the hash_mutex */
      if (ret_val != TRYLOCK_EBUSY)
	{
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_MUTEX_TRYLOCK, 0);
	  return ER_CSS_PTHREAD_MUTEX_TRYLOCK;
	}
      /* Someone is holding the hash_mutex */
      MUTEX_UNLOCK (res_ptr->res_mutex);
      MUTEX_LOCK (rv, hash_anchor->hash_mutex);

      res_mutex_hold = false;
    }

  /* Now holding hash_mutex. find the resource entry */
  prev = (LK_RES *) NULL;
  curr = hash_anchor->hash_next;
  while (curr != (LK_RES *) NULL)
    {
      if (curr == res_ptr)
	{
	  break;
	}
      prev = curr;
      curr = curr->hash_next;
    }

  if (curr == (LK_RES *) NULL)
    {
      /*
       * Case 1: The lock resource entry does not exist in the hash chain.
       *         This case could be occur when some other transactions got
       *         the lock resource entry and deallocated it in the meanwhile
       *         of releasing the res_mutex and holding the hash_mutex.
       *         The deallocated lock resource entry can be either
       *         1) in the free list of lock resource entries, or
       *         2) in some other lock hash chain.
       */
      /* release all the mutexes */
      MUTEX_UNLOCK (hash_anchor->hash_mutex);
      if (res_mutex_hold == true)	/* This might be always false */
	{
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	}
      return NO_ERROR;
    }

  /*
   * Case 2: The lock resource entry does exist in the hash chain.
   *         The lock resource entry may contain either
   *         1) the OID that the transaction wants to unlock, or
   *         2) some other OID.
   */
  if (res_mutex_hold == false)
    {
      /* hold the res_mutex conditionally */
      ret_val = MUTEX_TRYLOCK (res_ptr->res_mutex);
      if (ret_val != TRYLOCK_SUCCESS)
	{			/* could not hold the res_mutex */
	  if (ret_val != TRYLOCK_EBUSY)
	    {
	      MUTEX_UNLOCK (hash_anchor->hash_mutex);
	      er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_CSS_PTHREAD_MUTEX_TRYLOCK, 0);
	      return ER_CSS_PTHREAD_MUTEX_TRYLOCK;
	    }
	  /* Someone wants to use this: OK, I will quit! */
	  MUTEX_UNLOCK (hash_anchor->hash_mutex);
	  return NO_ERROR;
	}
      /* check if the resource entry is empty again. */
      if (res_ptr->holder != NULL || res_ptr->waiter != NULL ||
	  res_ptr->non2pl != NULL)
	{
	  /* Someone already got this: OK, I will quit! */
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  MUTEX_UNLOCK (hash_anchor->hash_mutex);
	  return NO_ERROR;
	}
    }

  /* I hold hash_mutex and res_mutex
   * remove the resource entry from the hash chain
   */
  if (prev == (LK_RES *) NULL)
    {
      hash_anchor->hash_next = res_ptr->hash_next;
    }
  else
    {
      prev->hash_next = res_ptr->hash_next;
    }

  /* release hash_mutex and res_mutex */
  MUTEX_UNLOCK (hash_anchor->hash_mutex);
  MUTEX_UNLOCK (res_ptr->res_mutex);

  /* initialize the lock descriptor entry as a freed entry
   * This is performed within lk_free_resource()
   * free the lock resource entry
   */
  lock_free_resource (res_ptr);

  return NO_ERROR;
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: transaction lock list related functios
 *   - lk_insert_into_tran_hold_list()
 *   - lk_delete_from_tran_hold_list()
 *   - lk_insert_into_tran_non2pl_list()
 *   - lk_delete_from_tran_non2pl_list()
 */

#if defined(SERVER_MODE)
/*
 * lock_insert_into_tran_hold_list - Insert the given lock entry
 *                      into the transaction lock hold list
 *
 * return: nothing
 *
 *   entry_ptr(in):
 *
 * Note:This function inserts the given lock entry into the transaction lock
 *     hold list. The given lock entry was included in the lock holder
 *     list. That is, The lock is held by the transaction.
 */
static void
lock_insert_into_tran_hold_list (LK_ENTRY * entry_ptr)
{
  LK_TRAN_LOCK *tran_lock;
  int rv;

  /* The caller is holding a resource mutex */

  tran_lock = &lk_Gl.tran_lock_table[entry_ptr->tran_index];
  MUTEX_LOCK (rv, tran_lock->hold_mutex);

  switch (entry_ptr->res_head->type)
    {
    case LOCK_RESOURCE_ROOT_CLASS:
#if defined(CUBRID_DEBUG)
      if (tran_lock->root_class_hold != (LK_ENTRY *) NULL)
	{
	  fprintf (stderr, "lk_insert_into_tran_hold_list() error.. (1)\n");
	}
#endif /* CUBRID_DEBUG */
      entry_ptr->tran_next = tran_lock->root_class_hold;
      tran_lock->root_class_hold = entry_ptr;
      break;
    case LOCK_RESOURCE_CLASS:
#if defined(CUBRID_DEBUG)
      if (tran_lock->class_hold_list != (LK_ENTRY *) NULL)
	{
	  LK_ENTRY *_ptr;
	  _ptr = tran_lock->class_hold_list;
	  while (_ptr != NULL)
	    {
	      if (_ptr->res_head == entry_ptr->res_head)
		{
		  break;
		}
	      _ptr = _ptr->tran_next;
	    }
	  if (_ptr != NULL)
	    {
	      fprintf (stderr,
		       "lk_insert_into_tran_hold_list() error.. (2)\n");
	    }
	}
#endif /* CUBRID_DEBUG */
      entry_ptr->tran_next = tran_lock->class_hold_list;
      tran_lock->class_hold_list = entry_ptr;
      tran_lock->class_hold_count += 1;
      break;
    case LOCK_RESOURCE_INSTANCE:
#if defined(CUBRID_DEBUG)
      if (tran_lock->inst_hold_list != (LK_ENTRY *) NULL)
	{
	  LK_ENTRY *_ptr;
	  _ptr = tran_lock->inst_hold_list;
	  while (_ptr != NULL)
	    {
	      if (_ptr->res_head == entry_ptr->res_head)
		{
		  break;
		}
	      _ptr = _ptr->tran_next;
	    }
	  if (_ptr != NULL)
	    {
	      fprintf (stderr,
		       "lk_insert_into_tran_hold_list() error.. (3)\n");
	    }
	}
#endif /* CUBRID_DEBUG */
      entry_ptr->tran_next = tran_lock->inst_hold_list;
      tran_lock->inst_hold_list = entry_ptr;
      tran_lock->inst_hold_count += 1;
      break;
    default:
      break;
    }
  MUTEX_UNLOCK (tran_lock->hold_mutex);
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_delete_from_tran_hold_list - Delted the given lock entry
 *                      from the transaction lock hold list
 *
 * return: error code
 *
 *   entry_ptr(in):
 *
 * Note:This functions finds the given lock entry in the transaction
 *     lock hold list and then deletes it from the lock hold list.
 */
static int
lock_delete_from_tran_hold_list (LK_ENTRY * entry_ptr)
{
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *prev, *curr;
  int rv;
  int error_code = NO_ERROR;

  /* The caller is holding a resource mutex */

  tran_lock = &lk_Gl.tran_lock_table[entry_ptr->tran_index];
  MUTEX_LOCK (rv, tran_lock->hold_mutex);

  switch (entry_ptr->res_head->type)
    {
    case LOCK_RESOURCE_ROOT_CLASS:
      if (entry_ptr != tran_lock->root_class_hold)
	{			/* does not exist */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LK_NOTFOUND_IN_TRAN_HOLD_LIST, 7,
		  LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
		  "ROOT CLASS", entry_ptr->res_head->oid.volid,
		  entry_ptr->res_head->oid.pageid,
		  entry_ptr->res_head->oid.slotid, entry_ptr->tran_index,
		  (tran_lock->root_class_hold == NULL ? 0 : 1));
	  error_code = ER_LK_NOTFOUND_IN_TRAN_HOLD_LIST;
	}
      else
	{
	  tran_lock->root_class_hold = (LK_ENTRY *) NULL;
	}
      break;

    case LOCK_RESOURCE_CLASS:
      /* find the given class lock entry in the class lock hold list */
      prev = (LK_ENTRY *) NULL;
      curr = tran_lock->class_hold_list;
      while (curr != (LK_ENTRY *) NULL && curr != entry_ptr)
	{
	  prev = curr;
	  curr = curr->tran_next;
	}
      if (curr == (LK_ENTRY *) NULL)
	{			/* not found */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LK_NOTFOUND_IN_TRAN_HOLD_LIST, 7,
		  LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode), "CLASS",
		  entry_ptr->res_head->oid.volid,
		  entry_ptr->res_head->oid.pageid,
		  entry_ptr->res_head->oid.slotid,
		  entry_ptr->tran_index, tran_lock->class_hold_count);
	  error_code = ER_LK_NOTFOUND_IN_TRAN_HOLD_LIST;
	}
      else
	{			/* found */
	  /* disconnect it from the class lock hold list */
	  if (prev == (LK_ENTRY *) NULL)
	    {
	      tran_lock->class_hold_list = curr->tran_next;
	    }
	  else
	    {
	      prev->tran_next = curr->tran_next;
	    }
	  tran_lock->class_hold_count -= 1;
	}
      break;

    case LOCK_RESOURCE_INSTANCE:
      /* find the given instance lock entry in the instance lock hold list */
      prev = (LK_ENTRY *) NULL;
      curr = tran_lock->inst_hold_list;
      while (curr != (LK_ENTRY *) NULL && curr != entry_ptr)
	{
	  prev = curr;
	  curr = curr->tran_next;
	}
      if (curr == (LK_ENTRY *) NULL)
	{			/* not found */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LK_NOTFOUND_IN_TRAN_HOLD_LIST, 7,
		  LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
		  "INSTANCE", entry_ptr->res_head->oid.volid,
		  entry_ptr->res_head->oid.pageid,
		  entry_ptr->res_head->oid.slotid, entry_ptr->tran_index,
		  tran_lock->inst_hold_count);
	  error_code = ER_LK_NOTFOUND_IN_TRAN_HOLD_LIST;
	}
      else
	{
	  /* disconnect it from the instance lock hold list */
	  if (prev == (LK_ENTRY *) NULL)
	    {
	      tran_lock->inst_hold_list = curr->tran_next;
	    }
	  else
	    {
	      prev->tran_next = curr->tran_next;
	    }
	  tran_lock->inst_hold_count -= 1;
	}
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_INVALID_OBJECT_TYPE, 4,
	      entry_ptr->res_head->type,
	      entry_ptr->res_head->oid.volid,
	      entry_ptr->res_head->oid.pageid,
	      entry_ptr->res_head->oid.slotid);
      error_code = ER_LK_INVALID_OBJECT_TYPE;
      break;
    }

  MUTEX_UNLOCK (tran_lock->hold_mutex);

  return error_code;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_insert_into_tran_non2pl_list - Insert the given lock entry
 *                        into the transaction non2pl list
 *
 * return: nothing
 *
 *   non2pl(in):
 *
 * Note:This function inserts the given lock entry into the transaction
 *     non2pl list.
 */
static void
lock_insert_into_tran_non2pl_list (LK_ENTRY * non2pl)
{
  LK_TRAN_LOCK *tran_lock;
  int rv;

  /* The caller is holding a resource mutex */

  tran_lock = &lk_Gl.tran_lock_table[non2pl->tran_index];
  MUTEX_LOCK (rv, tran_lock->non2pl_mutex);

  non2pl->tran_next = tran_lock->non2pl_list;
  tran_lock->non2pl_list = non2pl;
  if (non2pl->granted_mode == INCON_NON_TWO_PHASE_LOCK)
    {
      tran_lock->num_incons_non2pl += 1;
    }

  MUTEX_UNLOCK (tran_lock->non2pl_mutex);
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_delete_from_tran_non2pl_list - Delete the given lock entry
 *                        from the transaction non2pl list
 *
 * return: error code
 *
 *   non2pl(in):
 *
 * Note:This function finds the given lock entry in the transaction
 *     non2pl list and then deletes it from the non2pl list.
 */
static int
lock_delete_from_tran_non2pl_list (LK_ENTRY * non2pl)
{
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *prev, *curr;
  int rv;
  int error_code = NO_ERROR;

  /* The caller is holding a resource mutex */

  tran_lock = &lk_Gl.tran_lock_table[non2pl->tran_index];
  MUTEX_LOCK (rv, tran_lock->non2pl_mutex);

  /* find the given non2pl entry in transaction non2pl list */
  prev = (LK_ENTRY *) NULL;
  curr = tran_lock->non2pl_list;
  while ((curr != (LK_ENTRY *) NULL) && (curr != non2pl))
    {
      prev = curr;
      curr = curr->tran_next;
    }
  if (curr == (LK_ENTRY *) NULL)
    {				/* not found */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LK_NOTFOUND_IN_TRAN_NON2PL_LIST, 5,
	      LOCK_TO_LOCKMODE_STRING (non2pl->granted_mode),
	      (non2pl->res_head != NULL ? non2pl->res_head->oid.volid : -2),
	      (non2pl->res_head != NULL ? non2pl->res_head->oid.pageid : -2),
	      (non2pl->res_head != NULL ? non2pl->res_head->oid.slotid : -2),
	      non2pl->tran_index);
      error_code = ER_LK_NOTFOUND_IN_TRAN_NON2PL_LIST;
    }
  else
    {				/* found */
      /* delete it from the transaction non2pl list */
      if (prev == (LK_ENTRY *) NULL)
	{
	  tran_lock->non2pl_list = curr->tran_next;
	}
      else
	{
	  prev->tran_next = curr->tran_next;
	}

      if (curr->granted_mode == INCON_NON_TWO_PHASE_LOCK)
	{
	  tran_lock->num_incons_non2pl -= 1;
	}
    }
  MUTEX_UNLOCK (tran_lock->non2pl_mutex);

  return error_code;
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: find lock entry from transaction locks
 *   - lk_find_class_entry()
 *   - lk_find_inst_entry()
 */

#if defined(SERVER_MODE)
/*
 * lock_find_class_entry - Find a class lock entry
 *                           in the transaction lock hold list
 *
 * return:
 *
 *   tran_index(in):
 *   class_oid(in):
 *
 * Note:This function finds a class lock entry, whose lock object id
 *     is the given class_oid, in the transaction lock hold list.
 */
static LK_ENTRY *
lock_find_class_entry (int tran_index, const OID * class_oid)
{
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *entry_ptr;
  int rv;

  /* The caller is not holding any mutex */

  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  MUTEX_LOCK (rv, tran_lock->hold_mutex);

  if (OID_IS_ROOTOID (class_oid))
    {
      entry_ptr = tran_lock->root_class_hold;
    }
  else
    {
      entry_ptr = tran_lock->class_hold_list;
      while (entry_ptr != (LK_ENTRY *) NULL)
	{
	  if (OID_EQ (&entry_ptr->res_head->oid, class_oid))
	    {
	      break;
	    }
	  entry_ptr = entry_ptr->tran_next;
	}
    }

  MUTEX_UNLOCK (tran_lock->hold_mutex);
  return entry_ptr;		/* it might be NULL */
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_find_inst_entry -  Find an instance lock entry
 *                          in the transaction lock hold list
 *
 * return:
 *
 *   tran_index(in):
 *   inst_oid(in):
 *
 * Note:This function finds an instance lock entry, whose lock object id
 *     is the given inst_oid, in the transaction lock hold list.
 */
static LK_ENTRY *
lock_find_inst_entry (int tran_index, const OID * inst_oid)
{
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *entry_ptr;
  int rv;

  /* The caller is not holding any mutex */

  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  MUTEX_LOCK (rv, tran_lock->hold_mutex);

  entry_ptr = tran_lock->inst_hold_list;
  while (entry_ptr != (LK_ENTRY *) NULL)
    {
      if (OID_EQ (&entry_ptr->res_head->oid, inst_oid))
	{
	  break;
	}
      entry_ptr = entry_ptr->tran_next;
    }

  MUTEX_UNLOCK (tran_lock->hold_mutex);
  return entry_ptr;		/* it might be NULL */
}
#endif /* SERVER_MODE */


/*
 *  Private Functions Group: lock entry addition related functions
 *   - lk_add_non2pl_lock()
 *   - lk_position_holder_entry()
 */

#if defined(SERVER_MODE)
/*
 * lock_add_non2pl_lock - Add a release lock which has never been acquired
 *
 * return: pointer to the lock entry in non2pl list.
 *
 *   res_ptr(in): pointer to lock resource
 *   tran_index(in): transaction table index
 *   lock(in): the lock mode of non2pl lock
 *
 * Note:Cache a release lock (which has never been acquired) onto the list
 *     of non two phase lock to detect future serializable inconsistencies
 *
 *     Note: This function is used for
 *           TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE
 *           TRAN_REP_CLASS_UNCOMMIT_INSTANCE
 */
static LK_ENTRY *
lock_add_non2pl_lock (LK_RES * res_ptr, int tran_index, LOCK lock)
{
  LK_ENTRY *non2pl;
  LK_TRAN_LOCK *tran_lock;
  int rv;
  int compat;

  /* The caller is holding a resource mutex */

  /* find the non2pl entry of the given transaction */
  non2pl = res_ptr->non2pl;
  while (non2pl != (LK_ENTRY *) NULL)
    {
      if (non2pl->tran_index == tran_index)
	{
	  break;
	}
      non2pl = non2pl->next;
    }

  if (non2pl != (LK_ENTRY *) NULL)
    {
      /* 1. I have a non2pl entry on the lock resource */
      /* reflect the current lock acquisition into the non2pl entry */
      tran_lock = &lk_Gl.tran_lock_table[tran_index];
      MUTEX_LOCK (rv, tran_lock->non2pl_mutex);

      if (non2pl->granted_mode != INCON_NON_TWO_PHASE_LOCK)
	{
	  if (lock == INCON_NON_TWO_PHASE_LOCK)
	    {
	      non2pl->granted_mode = INCON_NON_TWO_PHASE_LOCK;
	      tran_lock->num_incons_non2pl += 1;
	    }
	  else
	    {
	      assert (lock >= NULL_LOCK && non2pl->granted_mode >= NULL_LOCK);
	      compat = lock_Comp[lock][non2pl->granted_mode];
	      assert (compat != DB_NA);

	      if (compat == false)
		{
		  non2pl->granted_mode = INCON_NON_TWO_PHASE_LOCK;
		  tran_lock->num_incons_non2pl += 1;
		}
	      else
		{
		  non2pl->granted_mode =
		    lock_Conv[lock][non2pl->granted_mode];
		  assert (non2pl->granted_mode != NA_LOCK);
		}
	    }
	}

      MUTEX_UNLOCK (tran_lock->non2pl_mutex);
    }
  else
    {				/* non2pl == (LK_ENTRY *)NULL */
      /* 2. I do not have a non2pl entry on the lock resource */
      /* allocate a lock entry, initialize it, and connect it */
      non2pl = lock_alloc_entry ();
      if (non2pl != (LK_ENTRY *) NULL)
	{
	  LK_INIT_ENTRY_NON2PL (non2pl, tran_index, res_ptr, lock);
	  non2pl->next = res_ptr->non2pl;
	  res_ptr->non2pl = non2pl;
	  lock_insert_into_tran_non2pl_list (non2pl);
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ALLOC_RESOURCE,
		  1, "lock heap entry");
	}
    }
  return non2pl;		/* it might be NULL */
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_position_holder_entry - Position given lock entry in the lock
 *                                holder list of given lock resource
 *
 * return:
 *
 *   res_ptr(in):
 *   entry_ptr(in):
 *
 * Note:This function positions the given lock entry
 *     in the lock holder list of the given lock resource
 *     according to Upgrader Positioning Rule(UPR).
 *
 *     NOTE that the granted_mode and blocked_mode of the given lock
 *          entry must be set before this function is called.
 */
static void
lock_position_holder_entry (LK_RES * res_ptr, LK_ENTRY * entry_ptr)
{
  LK_ENTRY *prev, *i;
  LK_ENTRY *ta, *tap;
  LK_ENTRY *tb, *tbp;
  LK_ENTRY *tc, *tcp;
  int compat1, compat2;

  /* find the position where the lock entry to be inserted */
  if (entry_ptr->blocked_mode == NULL_LOCK)
    {
      /* case 1: when block_mode is NULL_LOCK */
      prev = (LK_ENTRY *) NULL;
      i = res_ptr->holder;
      while (i != (LK_ENTRY *) NULL)
	{
	  if (i->blocked_mode == NULL_LOCK)
	    {
	      break;
	    }
	  prev = i;
	  i = i->next;
	}
    }
  else
    {
      /* case 2: when block_mode is not NULL_LOCK */
      /* find ta, tb, tc among other holders */
      ta = tb = tc = (LK_ENTRY *) NULL;
      tap = tbp = tcp = (LK_ENTRY *) NULL;

      prev = (LK_ENTRY *) NULL;
      i = res_ptr->holder;
      while (i != (LK_ENTRY *) NULL)
	{
	  if (i->blocked_mode != NULL_LOCK)
	    {
	      assert (entry_ptr->blocked_mode >= NULL_LOCK
		      && entry_ptr->granted_mode >= NULL_LOCK);
	      assert (i->blocked_mode >= NULL_LOCK
		      && i->granted_mode >= NULL_LOCK);

	      compat1 = lock_Comp[entry_ptr->blocked_mode][i->blocked_mode];
	      assert (compat1 != DB_NA);

	      if (ta == NULL && compat1 == true)
		{
		  ta = i;
		  tap = prev;
		}

	      compat1 = lock_Comp[entry_ptr->blocked_mode][i->granted_mode];
	      assert (compat1 != DB_NA);

	      compat2 = lock_Comp[i->blocked_mode][entry_ptr->granted_mode];
	      assert (compat2 != DB_NA);

	      if (ta == NULL && tb == NULL
		  && compat1 == true && compat2 == false)
		{
		  tb = i;
		  tbp = prev;
		}
	    }
	  else
	    {
	      if (tc == NULL)
		{
		  tc = i;
		  tcp = prev;
		}
	    }
	  prev = i;
	  i = i->next;
	}
      if (ta != NULL)
	{
	  prev = tap;
	}
      else if (tb != NULL)
	{
	  prev = tbp;
	}
      else if (tc != NULL)
	{
	  prev = tcp;
	}
    }

  /* insert the given lock entry into the found position */
  if (prev == NULL)
    {
      entry_ptr->next = res_ptr->holder;
      res_ptr->holder = entry_ptr;
    }
  else
    {
      entry_ptr->next = prev->next;
      prev->next = entry_ptr;
    }
}
#endif /* SERVER_MODE */


/*
 *  Private Functions Group: timeout related functions
 *
 *   - lock_set_error_for_timeout()
 *   - lock_set_error_for_aborted()
 *   - lock_suspend(), lock_resume()
 *   - lock_wakeup_deadlock_victim_timeout()
 *   - lock_wakeup_deadlock_victim_aborted()
 */

#if defined(SERVER_MODE)
/*
 * lock_set_error_for_timeout - Set error for lock timeout
 *
 * return:
 *
 *   entry_ptr(in): pointer to the lock entry for waiting
 *
 * Note:Set error code for lock timeout
 */
static void
lock_set_error_for_timeout (THREAD_ENTRY * thread_p, LK_ENTRY * entry_ptr)
{
  char *client_prog_name;	/* Client program name for transaction  */
  char *client_user_name;	/* Client user name for transaction     */
  char *client_host_name;	/* Client host for transaction          */
  int client_pid;		/* Client process id for transaction    */
  char *waitfor_client_users_default = (char *) "";
  char *waitfor_client_users;	/* Waitfor users                        */
  char *classname;		/* Name of the class                    */
  int i, nwaits, max_waits = DEFAULT_WAIT_USERS;
  int wait_for_buf[DEFAULT_WAIT_USERS];
  int *wait_for = wait_for_buf, *t;
  LK_ENTRY *entry;
  LK_RES *res_ptr = NULL;
  int unit_size = LOG_USERNAME_MAX + MAXHOSTNAMELEN + 20 + 4;
  char *ptr;
  const char *fmt;
  int rv;
  bool free_mutex_flag = false;
  bool isdeadlock_timeout = false;
  int compat1, compat2;

  /* Find the users that transaction is waiting for */
  waitfor_client_users = waitfor_client_users_default;
  nwaits = 0;

  assert (entry_ptr->granted_mode >= NULL_LOCK
	  && entry_ptr->blocked_mode >= NULL_LOCK);

  if (PRM_LK_TIMEOUT_MESSAGE_DUMP_LEVEL == 0)
    {
      /* nothing to do */
    }
  else if (PRM_LK_TIMEOUT_MESSAGE_DUMP_LEVEL == 1)
    {
      /* Dump only the first tran. info. which this tran. is waiting for */
      res_ptr = entry_ptr->res_head;
      wait_for[0] = NULL_TRAN_INDEX;

      MUTEX_LOCK (rv, res_ptr->res_mutex);
      for (entry = res_ptr->holder; entry != NULL; entry = entry->next)
	{
	  if (entry == entry_ptr)
	    {
	      continue;
	    }

	  assert (entry->granted_mode >= NULL_LOCK
		  && entry->blocked_mode >= NULL_LOCK);
	  compat1 = lock_Comp[entry->granted_mode][entry_ptr->blocked_mode];
	  compat2 = lock_Comp[entry->blocked_mode][entry_ptr->blocked_mode];
	  assert (compat1 != DB_NA && compat2 != DB_NA);

	  if (compat1 == false || compat2 == false)
	    {
	      wait_for[nwaits++] = entry->tran_index;
	      break;
	    }
	}

      if (wait_for[0] == NULL_TRAN_INDEX)
	{
	  for (entry = res_ptr->waiter; entry != NULL; entry = entry->next)
	    {
	      if (entry == entry_ptr)
		{
		  continue;
		}

	      assert (entry->granted_mode >= NULL_LOCK
		      && entry->blocked_mode >= NULL_LOCK);
	      compat1 =
		lock_Comp[entry->blocked_mode][entry_ptr->blocked_mode];
	      assert (compat1 != DB_NA);
	      if (compat1 == false)
		{
		  wait_for[nwaits++] = entry->tran_index;
		  break;
		}
	    }
	}
      MUTEX_UNLOCK (res_ptr->res_mutex);
    }
  else if (PRM_LK_TIMEOUT_MESSAGE_DUMP_LEVEL == 2)
    {
      /* Dump all the tran. info. which this tran. is waiting for */
      res_ptr = entry_ptr->res_head;
      wait_for[0] = NULL_TRAN_INDEX;

      MUTEX_LOCK (rv, res_ptr->res_mutex);
      free_mutex_flag = true;
      for (entry = res_ptr->holder; entry != NULL; entry = entry->next)
	{
	  if (entry == entry_ptr)
	    {
	      continue;
	    }

	  assert (entry->granted_mode >= NULL_LOCK
		  && entry->blocked_mode >= NULL_LOCK);
	  compat1 = lock_Comp[entry->granted_mode][entry_ptr->blocked_mode];
	  compat2 = lock_Comp[entry->blocked_mode][entry_ptr->blocked_mode];
	  assert (compat1 != DB_NA && compat2 != DB_NA);

	  if (compat1 == false || compat2 == false)
	    {
	      EXPAND_WAIT_FOR_ARRAY_IF_NEEDED ();
	      wait_for[nwaits++] = entry->tran_index;
	    }
	}

      for (entry = res_ptr->waiter; entry != NULL; entry = entry->next)
	{
	  if (entry == entry_ptr)
	    {
	      continue;
	    }

	  assert (entry->granted_mode >= NULL_LOCK
		  && entry->blocked_mode >= NULL_LOCK);
	  compat1 = lock_Comp[entry->blocked_mode][entry_ptr->blocked_mode];
	  assert (compat1 != DB_NA);

	  if (compat1 == false)
	    {
	      EXPAND_WAIT_FOR_ARRAY_IF_NEEDED ();
	      wait_for[nwaits++] = entry->tran_index;
	    }
	}

      MUTEX_UNLOCK (res_ptr->res_mutex);
      free_mutex_flag = false;
    }

  if (nwaits == 0
      || (waitfor_client_users =
	  (char *) malloc (unit_size * nwaits)) == NULL)
    {
      waitfor_client_users = waitfor_client_users_default;
    }
  else
    {
      for (ptr = waitfor_client_users, i = 0, fmt = "%s@%s|%d";
	   i < nwaits; fmt = ", %s@%s|%d", i++)
	{
	  (void) logtb_find_client_name_host_pid (wait_for[i],
						  &client_prog_name,
						  &client_user_name,
						  &client_host_name,
						  &client_pid);
	  sprintf (ptr, fmt, client_user_name, client_host_name, client_pid);
	  ptr += strlen (ptr);
	}
    }

set_error:

  if (wait_for != wait_for_buf)
    {
      free_and_init (wait_for);
    }

  if (free_mutex_flag)
    {
      MUTEX_UNLOCK (res_ptr->res_mutex);
      free_mutex_flag = false;
    }

  /* get the client information of current transaction */
  (void) logtb_find_client_name_host_pid (entry_ptr->tran_index,
					  &client_prog_name,
					  &client_user_name,
					  &client_host_name, &client_pid);

  if ((entry_ptr->thrd_entry->lockwait_state == LOCK_RESUMED_DEADLOCK_TIMEOUT)
      || (entry_ptr->thrd_entry->lockwait_state ==
	  LOCK_RESUMED_ABORTED_OTHER))
    {
      isdeadlock_timeout = true;
    }

  switch (entry_ptr->res_head->type)
    {
    case LOCK_RESOURCE_ROOT_CLASS:
    case LOCK_RESOURCE_CLASS:
      classname = heap_get_class_name (thread_p, &entry_ptr->res_head->oid);
      if (classname != NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ((isdeadlock_timeout) ? ER_LK_OBJECT_DL_TIMEOUT_CLASS_MSG :
		   ER_LK_OBJECT_TIMEOUT_CLASS_MSG), 7, entry_ptr->tran_index,
		  client_user_name, client_host_name, client_pid,
		  LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
		  classname, waitfor_client_users);
	  free_and_init (classname);
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ((isdeadlock_timeout) ? ER_LK_OBJECT_DL_TIMEOUT_SIMPLE_MSG :
		   ER_LK_OBJECT_TIMEOUT_SIMPLE_MSG), 9, entry_ptr->tran_index,
		  client_user_name, client_host_name, client_pid,
		  LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
		  entry_ptr->res_head->oid.volid,
		  entry_ptr->res_head->oid.pageid,
		  entry_ptr->res_head->oid.slotid, waitfor_client_users);
	}
      break;

    case LOCK_RESOURCE_INSTANCE:
      classname =
	heap_get_class_name (thread_p, &entry_ptr->res_head->class_oid);
      if (classname != NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ((isdeadlock_timeout) ? ER_LK_OBJECT_DL_TIMEOUT_CLASSOF_MSG
		   : ER_LK_OBJECT_TIMEOUT_CLASSOF_MSG), 10,
		  entry_ptr->tran_index, client_user_name, client_host_name,
		  client_pid,
		  LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
		  entry_ptr->res_head->oid.volid,
		  entry_ptr->res_head->oid.pageid,
		  entry_ptr->res_head->oid.slotid, classname,
		  waitfor_client_users);
	  free_and_init (classname);
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ((isdeadlock_timeout) ? ER_LK_OBJECT_DL_TIMEOUT_SIMPLE_MSG :
		   ER_LK_OBJECT_TIMEOUT_SIMPLE_MSG), 9, entry_ptr->tran_index,
		  client_user_name, client_host_name, client_pid,
		  LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
		  entry_ptr->res_head->oid.volid,
		  entry_ptr->res_head->oid.pageid,
		  entry_ptr->res_head->oid.slotid, waitfor_client_users);
	}
      break;
    default:
      break;
    }

  if (waitfor_client_users
      && waitfor_client_users != waitfor_client_users_default)
    {
      free_and_init (waitfor_client_users);
    }
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_set_error_for_aborted - Set error for unilaterally aborted
 *
 * return:
 *
 *   entry_ptr(in): pointer to the entry for waiting
 *
 * Note:set error code for unilaterally aborted deadlock victim
 */
static void
lock_set_error_for_aborted (LK_ENTRY * entry_ptr)
{
  char *client_prog_name;	/* Client user name for transaction  */
  char *client_user_name;	/* Client user name for transaction  */
  char *client_host_name;	/* Client host for transaction       */
  int client_pid;		/* Client process id for transaction */

  (void) logtb_find_client_name_host_pid (entry_ptr->tran_index,
					  &client_prog_name,
					  &client_user_name,
					  &client_host_name, &client_pid);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_UNILATERALLY_ABORTED, 4,
	  entry_ptr->tran_index, client_user_name, client_host_name,
	  client_pid);
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_suspend - Suspend current thread (transaction)
 *
 * return: LOCK_WAIT_STATE (state of resumption)
 *
 *   entry_ptr(in): lock entry for lock waiting
 *   waitsecs(in): lock wait seconds
 */
static LOCK_WAIT_STATE
lock_suspend (THREAD_ENTRY * thread_p, LK_ENTRY * entry_ptr, int waitsecs)
{
  THREAD_ENTRY *p;
  struct timeval tv;
  int client_id;

  /* The caller is holding the thread entry mutex */

  if (lk_Gl.verbose_mode == true)
    {
      char *__client_prog_name;	/* Client program name for transaction */
      char *__client_user_name;	/* Client user name for transaction    */
      char *__client_host_name;	/* Client host for transaction         */
      int __client_pid;		/* Client process id for transaction   */

      fflush (stderr);
      fflush (stdout);
      logtb_find_client_name_host_pid (entry_ptr->tran_index,
				       &__client_prog_name,
				       &__client_user_name,
				       &__client_host_name, &__client_pid);
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOCK,
				       MSGCAT_LK_SUSPEND_TRAN),
	       entry_ptr->thrd_entry->index, entry_ptr->tran_index,
	       __client_prog_name, __client_user_name, __client_host_name,
	       __client_pid);
      fflush (stdout);
    }

  /* register lock wait info. into the thread entry */
  entry_ptr->thrd_entry->lockwait = (void *) entry_ptr;
  gettimeofday (&tv, NULL);
  entry_ptr->thrd_entry->lockwait_stime =
    ((double) tv.tv_sec * 1000000 + tv.tv_usec) / 1000;
  entry_ptr->thrd_entry->lockwait_nsecs = waitsecs;
  entry_ptr->thrd_entry->lockwait_state = (int) LOCK_SUSPENDED;

  lk_Gl.TWFG_node[entry_ptr->tran_index].thrd_wait_stime =
    (time_t) (entry_ptr->thrd_entry->lockwait_stime / 1000);

  /* wakeup the dealock detect thread */
  thread_wakeup_deadlock_detect_thread ();

  /* suspend the worker thread (transaction) */
  thread_suspend_wakeup_and_unlock_entry (entry_ptr->thrd_entry,
					  THREAD_LOCK_SUSPENDED);
  if (entry_ptr->thrd_entry->resume_status == THREAD_RESUME_DUE_TO_INTERRUPT)
    {
      /* a shutdown thread wakes me up */
      return LOCK_RESUMED_INTERRUPT;
    }
  else if (entry_ptr->thrd_entry->resume_status != THREAD_LOCK_RESUMED)
    {
      /* wake up with other reason */
      assert (0);

      return LOCK_RESUMED_INTERRUPT;
    }
  else
    {
      assert (entry_ptr->thrd_entry->resume_status == THREAD_LOCK_RESUMED);
    }

  lk_Gl.TWFG_node[entry_ptr->tran_index].thrd_wait_stime = 0;

  thread_lock_entry (entry_ptr->thrd_entry);
  while (entry_ptr->thrd_entry->tran_next_wait)
    {
      p = entry_ptr->thrd_entry->tran_next_wait;
      entry_ptr->thrd_entry->tran_next_wait = p->tran_next_wait;
      p->tran_next_wait = NULL;
      thread_wakeup (p, THREAD_LOCK_RESUMED);
    }
  thread_unlock_entry (entry_ptr->thrd_entry);

  /* The thread has been awaken
   * Before waking up the thread, the waker cleared the lockwait field
   * of the thread entry and set lockwait_state field of it to the resumed
   * state while holding the thread entry mutex. After the wakeup,
   * no one can update the lockwait releated fields of the thread entry.
   * Therefore, waken-up thread can read the values of lockwait related
   * fields of its own thread entry without holding thread entry mutex.
   */

  switch ((LOCK_WAIT_STATE) (entry_ptr->thrd_entry->lockwait_state))
    {
    case LOCK_RESUMED:
      /* The lock entry has already been moved to the holder list */
      return LOCK_RESUMED;

    case LOCK_RESUMED_ABORTED_FIRST:
      /* The lock entry does exist within the blocked holder list
       * or blocked waiter list. Therefore, current thread must disconnect
       * it from the list.
       */
      if (logtb_is_current_active (thread_p) == true)
	{
	  lock_set_error_for_aborted (entry_ptr);	/* set error code */

	  /* wait until other threads finish their works
	   * A css_server_thread is always running for this transaction.
	   * so, wait until thread_has_threads() becomes 1 (except me)
	   */
	  if (!qmgr_is_thread_executing_async_query (entry_ptr->thrd_entry))
	    {
	      if (thread_has_threads (thread_p, entry_ptr->tran_index,
				      thread_get_client_id (thread_p)) >= 1)
		{
		  logtb_set_tran_index_interrupt (thread_p,
						  entry_ptr->tran_index,
						  true);
		  while (1)
		    {
		      thread_sleep (0, 10000);	/* sleep 10 msec */
		      thread_wakeup_with_tran_index (entry_ptr->tran_index,
						     THREAD_RESUME_DUE_TO_INTERRUPT);

		      client_id = thread_get_client_id (thread_p);
		      if (thread_has_threads (thread_p, entry_ptr->tran_index,
					      client_id) == 0)
			{
			  break;
			}
		    }
		  logtb_set_tran_index_interrupt (thread_p,
						  entry_ptr->tran_index,
						  false);
		}
	    }
	}
      else
	{
	  /* We are already aborting, fall through.
	   * Don't do double aborts that could cause an infinite loop.
	   */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ABORT_TRAN_TWICE, 1,
		  entry_ptr->tran_index);
	  /* er_log_debug(ARG_FILE_LINE, "lk_suspend: Likely a system error."
	     "Trying to abort a transaction twice.\n"); */

	  /* Since we deadlocked during an abort,
	   * forcibly remove all page latches of this transaction and
	   * hope this transaction is the cause of the logjam.
	   * We are hoping that this frees things
	   * just enough to let other transactions continue.
	   * Note it is not be safe to unlock object locks this way.
	   */
	  pgbuf_unfix_all (thread_p);
	}
      return LOCK_RESUMED_ABORTED;

    case LOCK_RESUMED_ABORTED_OTHER:
      /* The lock entry does exist within the blocked holder list
       * or blocked waiter list. Therefore, current thread must diconnect
       * it from the list.
       */
      /* If two or more threads, which were executing for one transaction,
       * are selected as deadlock victims, one of them is charged of the
       * transaction abortion and the other threads are notified of timeout.
       */
      (void) lock_set_error_for_timeout (thread_p, entry_ptr);
      return LOCK_RESUMED_DEADLOCK_TIMEOUT;

    case LOCK_RESUMED_DEADLOCK_TIMEOUT:
      (void) lock_set_error_for_timeout (thread_p, entry_ptr);
      return LOCK_RESUMED_DEADLOCK_TIMEOUT;

    case LOCK_RESUMED_TIMEOUT:
      /* The lock entry does exist within the blocked holder list
       * or blocked waiter list. Therefore, current thread must diconnect
       * it from the list.
       *
       * An error is ONLY set when the caller was willing to wait.
       * entry_ptr->thrd_entry->lockwait_nsecs > 0 */
      (void) lock_set_error_for_timeout (thread_p, entry_ptr);
      return LOCK_RESUMED_TIMEOUT;

    case LOCK_SUSPENDED:
    default:
      /* Probabely, the waiting structure has not been removed
       * from the waiting hash table. May be a system error.
       */
      (void) lock_set_error_for_timeout (thread_p, entry_ptr);
      return LOCK_RESUMED_TIMEOUT;
    }
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lk_resume - Resume the thread (transaction)
 *
 * return:
 *
 *   entry_ptr(in):
 *   state(in): resume state
 */
static void
lock_resume (LK_ENTRY * entry_ptr, int state)
{
  /* The caller is holding the thread entry mutex */
  /* The caller has identified the fact that lockwait is not NULL.
   * that is, the thread is suspended.
   */
  if (lk_Gl.verbose_mode == true)
    {
      char *__client_prog_name;	/* Client program name for transaction */
      char *__client_user_name;	/* Client user name for transaction    */
      char *__client_host_name;	/* Client host for transaction         */
      int __client_pid;		/* Client process id for transaction   */

      fflush (stderr);
      fflush (stdout);
      (void) logtb_find_client_name_host_pid (entry_ptr->tran_index,
					      &__client_prog_name,
					      &__client_user_name,
					      &__client_host_name,
					      &__client_pid);
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOCK,
				       MSGCAT_LK_RESUME_TRAN),
	       entry_ptr->tran_index, entry_ptr->tran_index,
	       __client_prog_name, __client_user_name, __client_host_name,
	       __client_pid);
      fflush (stdout);
    }

  /* Before wake up the thread,
   * clears lockwait field and set lockwait_state with the given state.
   */
  entry_ptr->thrd_entry->lockwait = (void *) NULL;
  entry_ptr->thrd_entry->lockwait_state = (int) state;

  /* wakes up the thread and release the thread entry mutex */
  entry_ptr->thrd_entry->resume_status = THREAD_LOCK_RESUMED;
  COND_SIGNAL (entry_ptr->thrd_entry->wakeup_cond);
  thread_unlock_entry (entry_ptr->thrd_entry);
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_wakeup_deadlock_victim_timeout - Wake up the deadlock victim while notifying timeout
 *
 * return: true  if the transaction is treated as deadlock victim or
 *     false if the transaction is not treated as deadlock victim.
 *              in this case, the transaction has already been waken up
 *              by other threads with other purposes(ex. lock is granted)
 *
 *   tran_index(in): deadlock victim transaction
 *
 * Note:The given transaction was selected as a deadlock victim in the last
 *     deadlock detection. The deadlock victim is waked up and noitified of
 *     timeout by this function if the deadlock victim is still suspended.
 */
static bool
lock_wakeup_deadlock_victim_timeout (int tran_index)
{
  THREAD_ENTRY *thrd_array[10];
  int thrd_count, i;
  THREAD_ENTRY *thrd_ptr;
  bool wakeup_first = false;

  thrd_count = thread_get_lockwait_entry (tran_index, &thrd_array[0]);
  for (i = 0; i < thrd_count; i++)
    {
      thrd_ptr = thrd_array[i];
      (void) thread_lock_entry (thrd_ptr);
      if (thrd_ptr->tran_index == tran_index
	  && LK_IS_LOCKWAIT_THREAD (thrd_ptr))
	{
	  /* wake up the thread while notifying timeout */
	  lock_resume ((LK_ENTRY *) thrd_ptr->lockwait,
		       LOCK_RESUMED_DEADLOCK_TIMEOUT);
	  wakeup_first = true;
	}
      else
	{
	  if (thrd_ptr->lockwait != NULL
	      || thrd_ptr->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, thrd_ptr->lockwait,
		      thrd_ptr->lockwait_state, thrd_ptr->index,
		      thrd_ptr->tid, thrd_ptr->tran_index);
	    }
	  /* The current thread has already been waken up by other threads.
	   * The current thread might be granted the lock. or with any other
	   * reason....... even if it is a thread of the deadlock victim.
	   */
	  /* release the thread entry mutex */
	  (void) thread_unlock_entry (thrd_ptr);
	}
    }
  return wakeup_first;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_wakeup_deadlock_victim_aborted - Wake up the deadlock victim while notifying aborted
 *
 * return: true  if the transaction is treated as deadlock victim or
 *     false if the transaction is not treated as deadlock victim.
 *              in this case, the transaction has already been waken up
 *              by other threads with other purposes(ex. lock is granted)
 *
 *   tran_index(in): deadlock victim transaction
 *
 * Note:The given transaction was selected as a deadlock victim in the last
 *     deadlock detection. The deadlock victim is waked up and noitified of
 *     abortion by this function if the deadlock victim is still suspended.
 */
static bool
lock_wakeup_deadlock_victim_aborted (int tran_index)
{
  THREAD_ENTRY *thrd_array[10];
  int thrd_count, i;
  THREAD_ENTRY *thrd_ptr;
  bool wakeup_first = false;

  thrd_count = thread_get_lockwait_entry (tran_index, &thrd_array[0]);
  for (i = 0; i < thrd_count; i++)
    {
      thrd_ptr = thrd_array[i];
      (void) thread_lock_entry (thrd_ptr);
      if (thrd_ptr->tran_index == tran_index
	  && LK_IS_LOCKWAIT_THREAD (thrd_ptr))
	{
	  /* wake up the thread while notifying deadlock victim */
	  if (wakeup_first == false)
	    {
	      /* The current transaction is really aborted.
	       * Therefore, other threads of the current transaction must quit
	       * their executions and return to client.
	       * Then the first waken-up thread must be charge of the rollback
	       * of the current transaction.
	       */
	      /* set the transaction as deadlock victim */
	      lk_Gl.TWFG_node[tran_index].DL_victim = true;
	      lock_resume ((LK_ENTRY *) thrd_ptr->lockwait,
			   LOCK_RESUMED_ABORTED_FIRST);
	      wakeup_first = true;
	    }
	  else
	    {
	      lock_resume ((LK_ENTRY *) thrd_ptr->lockwait,
			   LOCK_RESUMED_ABORTED_OTHER);
	    }
	}
      else
	{
	  if (thrd_ptr->lockwait != NULL ||
	      thrd_ptr->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, thrd_ptr->lockwait,
		      thrd_ptr->lockwait_state, thrd_ptr->index,
		      thrd_ptr->tid, thrd_ptr->tran_index);
	    }
	  /* The current thread has already been waken up by other threads.
	   * The current thread might have held the lock. or with any other
	   * reason....... even if it is a thread of the deadlock victim.
	   */
	  /* release the thread entry mutex */
	  (void) thread_unlock_entry (thrd_ptr);
	}
    }
  return wakeup_first;
}
#endif /* SERVER_MODE */


/*
 *  Private Functions Group: grant lock requests of blocked threads
 *   - lock_grant_blocked_holder()
 *   - lock_grant_blocked_waiter()
 *   - lock_grant_blocked_waiter_partial()
 */

#if defined(SERVER_MODE)
/*
 * lock_grant_blocked_holder - Grant blocked holders
 *
 * return:
 *
 *   res_ptr(in): This function grants blocked holders whose blocked lock mode is
 *     compatible with all the granted lock mode of non-blocked holders.
 */
static void
lock_grant_blocked_holder (THREAD_ENTRY * thread_p, LK_RES * res_ptr)
{
  LK_ENTRY *prev_check;
  LK_ENTRY *check, *i, *prev;
  LOCK mode;
  int compat;

  /* The caller is holding a resource mutex */

  prev_check = NULL;
  check = res_ptr->holder;
  while (check != NULL && check->blocked_mode != NULL_LOCK)
    {
      /* there are some blocked holders */
      mode = NULL_LOCK;
      for (i = check->next; i != NULL; i = i->next)
	{
	  assert (i->granted_mode >= NULL_LOCK && mode >= NULL_LOCK);
	  mode = lock_Conv[i->granted_mode][mode];
	  assert (mode != NA_LOCK);
	}

      assert (check->blocked_mode >= NULL_LOCK);
      compat = lock_Comp[check->blocked_mode][mode];
      assert (compat != DB_NA);

      if (compat == false)
	{
	  break;		/* stop the granting */
	}

      /* compatible: grant it */

      /* hold the thread entry mutex */
      (void) thread_lock_entry (check->thrd_entry);

      /* check if the thread is still waiting on a lock */
      if (LK_IS_LOCKWAIT_THREAD (check->thrd_entry))
	{
	  /* the thread is still waiting on a lock */

	  /* reposition the lock entry according to UPR */
	  for (prev = check, i = check->next; i != NULL;)
	    {
	      if (i->blocked_mode == NULL_LOCK)
		{
		  break;
		}
	      prev = i;
	      i = i->next;
	    }
	  if (prev != check)
	    {			/* reposition it */
	      /* remove it */
	      if (prev_check == NULL)
		{
		  res_ptr->holder = check->next;
		}
	      else
		{
		  prev_check->next = check->next;
		}
	      /* insert it */
	      check->next = prev->next;
	      prev->next = check;
	    }

	  /* change granted_mode and blocked_mode */
	  check->granted_mode = check->blocked_mode;
	  check->blocked_mode = NULL_LOCK;

	  /* reflect the granted lock in the non2pl list */
	  lock_update_non2pl_list (res_ptr, check->tran_index,
				   check->granted_mode);

	  /* Record number of acquired locks */
	  mnt_lk_acquired_on_objects (thread_p);
#if defined(LK_TRACE_OBJECT)
	  LK_MSG_LOCK_ACQUIRED (entry_ptr);
#endif /* LK_TRACE_OBJECT */
	  /* wake up the blocked holder */
	  lock_resume (check, LOCK_RESUMED);
	}
      else
	{
	  if (check->thrd_entry->lockwait != NULL
	      || check->thrd_entry->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, check->thrd_entry->lockwait,
		      check->thrd_entry->lockwait_state,
		      check->thrd_entry->index, check->thrd_entry->tid,
		      check->thrd_entry->tran_index);
	    }
	  /* The thread is not waiting for a lock, currently.
	   * That is, the thread has already been waked up by timeout,
	   * deadlock victim or interrupt. In this case,
	   * we have nothing to do since the thread itself will remove
	   * this lock entry.
	   */
	  (void) thread_unlock_entry (check->thrd_entry);
	  prev_check = check;
	}

      if (prev_check == NULL)
	{
	  check = res_ptr->holder;
	}
      else
	{
	  check = prev_check->next;
	}
    }

}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_grant_blocked_waiter - Grant blocked waiters
 *
 * return:
 *
 *   res_ptr(in): This function grants blocked waiters whose blocked lock mode is
 *     compatible with the total mode of lock holders.
 */
static int
lock_grant_blocked_waiter (THREAD_ENTRY * thread_p, LK_RES * res_ptr)
{
  LK_ENTRY *prev_check;
  LK_ENTRY *check, *i;
  LOCK mode;
  bool change_total_waiters_mode = false;
  int error_code = NO_ERROR;
  int compat;

  /* The caller is holding a resource mutex */

  prev_check = NULL;
  check = res_ptr->waiter;
  while (check != NULL)
    {
      assert (check->blocked_mode >= NULL_LOCK
	      && res_ptr->total_holders_mode >= NULL_LOCK);
      compat = lock_Comp[check->blocked_mode][res_ptr->total_holders_mode];
      assert (compat != DB_NA);

      if (compat == false)
	{
	  break;		/* stop the granting */
	}

      /* compatible: grant it */
      /* hold the thread entry mutex */
      (void) thread_lock_entry (check->thrd_entry);

      /* check if the thread is still waiting for a lock */
      if (LK_IS_LOCKWAIT_THREAD (check->thrd_entry))
	{
	  /* The thread is still waiting for a lock. */
	  change_total_waiters_mode = true;

	  /* remove the lock entry from the waiter */
	  if (prev_check == NULL)
	    {
	      res_ptr->waiter = check->next;
	    }
	  else
	    {
	      prev_check->next = check->next;
	    }

	  /* change granted_mode and blocked_mode of the entry */
	  check->granted_mode = check->blocked_mode;
	  check->blocked_mode = NULL_LOCK;

	  /* position the lock entry in the holder list */
	  lock_position_holder_entry (res_ptr, check);

	  /* change total_holders_mode */
	  assert (check->granted_mode >= NULL_LOCK
		  && res_ptr->total_holders_mode >= NULL_LOCK);
	  res_ptr->total_holders_mode =
	    lock_Conv[check->granted_mode][res_ptr->total_holders_mode];
	  assert (res_ptr->total_holders_mode != NA_LOCK);

	  /* insert the lock entry into transaction hold list. */
	  lock_insert_into_tran_hold_list (check);

	  /* reflect the granted lock in the non2pl list */
	  lock_update_non2pl_list (res_ptr, check->tran_index,
				   check->granted_mode);

	  /* Record number of acquired locks */
	  mnt_lk_acquired_on_objects (thread_p);
#if defined(LK_TRACE_OBJECT)
	  LK_MSG_LOCK_ACQUIRED (entry_ptr);
#endif /* LK_TRACE_OBJECT */

	  /* wake up the blocked waiter */
	  lock_resume (check, LOCK_RESUMED);
	}
      else
	{
	  if (check->thrd_entry->lockwait != NULL
	      || check->thrd_entry->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, check->thrd_entry->lockwait,
		      check->thrd_entry->lockwait_state,
		      check->thrd_entry->index, check->thrd_entry->tid,
		      check->thrd_entry->tran_index);
	      error_code = ER_LK_STRANGE_LOCK_WAIT;
	    }
	  /* The thread is not waiting on the lock, currently.
	   * That is, the thread has already been waken up
	   * by lock timeout, deadlock victim or interrupt.
	   * In this case, we have nothing to do
	   * since the thread itself will remove this lock entry.
	   */
	  (void) thread_unlock_entry (check->thrd_entry);
	  prev_check = check;
	}

      if (prev_check == NULL)
	{
	  check = res_ptr->waiter;
	}
      else
	{
	  check = prev_check->next;
	}
    }

  if (change_total_waiters_mode == true)
    {
      mode = NULL_LOCK;
      for (i = res_ptr->waiter; i != NULL; i = i->next)
	{
	  assert (i->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
	  mode = lock_Conv[i->blocked_mode][mode];
	  assert (mode != NA_LOCK);
	}
      res_ptr->total_waiters_mode = mode;
    }

  return error_code;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_grant_blocked_waiter_partial - Grant blocked waiters partially
 *
 * return:
 *
 *   res_ptr(in):
 *   from_whom(in):
 *
 * Note:This function grants blocked waiters that are located from from_whom
 *     to the end of waiter list whose blocked lock mode is compatible with
 *     all the blocked mode of the previous lock waiters and the total mode
 *     of lock holders.
 */
static void
lock_grant_blocked_waiter_partial (THREAD_ENTRY * thread_p, LK_RES * res_ptr,
				   LK_ENTRY * from_whom)
{
  LK_ENTRY *prev_check;
  LK_ENTRY *check, *i;
  LOCK mode;
  int compat;

  /* the caller is holding a resource mutex */

  mode = NULL_LOCK;
  prev_check = (LK_ENTRY *) NULL;
  check = res_ptr->waiter;
  while (check != from_whom)
    {
      assert (check->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
      mode = lock_Conv[check->blocked_mode][mode];
      assert (mode != NA_LOCK);

      prev_check = check;
      check = check->next;
    }

  /* check = from_whom; */
  while (check != NULL)
    {
      assert (check->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
      compat = lock_Comp[check->blocked_mode][mode];
      assert (compat != DB_NA);

      if (compat != true)
	{
	  break;
	}

      assert (check->blocked_mode >= NULL_LOCK
	      && res_ptr->total_holders_mode >= NULL_LOCK);
      compat = lock_Comp[check->blocked_mode][res_ptr->total_holders_mode];
      assert (compat != DB_NA);

      if (compat == false)
	{
	  assert (check->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
	  mode = lock_Conv[check->blocked_mode][mode];
	  assert (mode != NA_LOCK);

	  prev_check = check;
	  check = check->next;
	  continue;
	}

      /* compatible: grant it */
      (void) thread_lock_entry (check->thrd_entry);
      if (LK_IS_LOCKWAIT_THREAD (check->thrd_entry))
	{
	  /* the thread is waiting on a lock */
	  /* remove the lock entry from the waiter */
	  if (prev_check == (LK_ENTRY *) NULL)
	    {
	      res_ptr->waiter = check->next;
	    }
	  else
	    {
	      prev_check->next = check->next;
	    }

	  /* change granted_mode and blocked_mode of the entry */
	  check->granted_mode = check->blocked_mode;
	  check->blocked_mode = NULL_LOCK;

	  /* position the lock entry into the holder list */
	  lock_position_holder_entry (res_ptr, check);

	  /* change total_holders_mode */
	  assert (check->granted_mode >= NULL_LOCK
		  && res_ptr->total_holders_mode >= NULL_LOCK);
	  res_ptr->total_holders_mode =
	    lock_Conv[check->granted_mode][res_ptr->total_holders_mode];
	  assert (res_ptr->total_holders_mode != NA_LOCK);

	  /* insert into transaction lock hold list */
	  lock_insert_into_tran_hold_list (check);

	  /* reflect the granted lock in the non2pl list */
	  lock_update_non2pl_list (res_ptr, check->tran_index,
				   check->granted_mode);

	  /* Record number of acquired locks */
	  mnt_lk_acquired_on_objects (thread_p);
#if defined(LK_TRACE_OBJECT)
	  LK_MSG_LOCK_ACQUIRED (entry_ptr);
#endif /* LK_TRACE_OBJECT */

	  /* wake up the blocked waiter (correctness must be checked) */
	  lock_resume (check, LOCK_RESUMED);
	}
      else
	{
	  if (check->thrd_entry->lockwait != NULL
	      || check->thrd_entry->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, check->thrd_entry->lockwait,
		      check->thrd_entry->lockwait_state,
		      check->thrd_entry->index, check->thrd_entry->tid,
		      check->thrd_entry->tran_index);
	    }
	  /* The thread is not waiting on the lock. That is, the thread has
	   * already been waken up by lock timeout, deadlock victim or interrupt.
	   * In this case, we have nothing to do since the thread itself
	   * will remove this lock entry.
	   */
	  (void) thread_unlock_entry (check->thrd_entry);

	  /* change prev_check */
	  assert (check->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
	  mode = lock_Conv[check->blocked_mode][mode];
	  assert (mode != NA_LOCK);

	  prev_check = check;
	}

      if (prev_check == NULL)
	{
	  check = res_ptr->waiter;
	}
      else
	{
	  check = prev_check->next;
	}
    }

  if (check == NULL)
    {
      res_ptr->total_waiters_mode = mode;
    }
  else
    {
      mode = NULL_LOCK;
      for (i = res_ptr->waiter; i != NULL; i = i->next)
	{
	  assert (i->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
	  mode = lock_Conv[i->blocked_mode][mode];
	  assert (mode != NA_LOCK);
	}
      res_ptr->total_waiters_mode = mode;
    }

}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: lock escalation related functions
 *   - lk_lock_escalation_if_needed()
 */

#if defined(SERVER_MODE)
/*
 * lock_escalate_if_needed -
 *
 * return: one of following values
 *     LK_GRANTED
 *     LK_NOTGRANTED_DUE_ABORTED
 *     LK_NOTGRANTED_DUE_TIMEOUT
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   class_entry(in):
 *   tran_index(in):
 *   waitsecs(in):
 *
 * Note:This function check if lock escalation is needed at first.
 *     If lock escalation is needed, that is, an escalation threshold is over,
 *     this function converts instance lock(s) to a class lock and
 *     releases unnecessary instance locks.
 */
static int
lock_escalate_if_needed (THREAD_ENTRY * thread_p, LK_ENTRY * class_entry,
			 int tran_index, int waitsecs)
{
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *inst_entry;
  int s_count, x_count;
  LOCK max_class_lock = NULL_LOCK;	/* escalated class lock mode */
  int granted;
  int rv;

  /* It cannot do lock escalation if class_entry is NULL */
  if (class_entry == NULL)
    {
      return LK_GRANTED;
    }

  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  MUTEX_LOCK (rv, tran_lock->hold_mutex);

  if (tran_lock->lock_escalation_on == true)
    {
      /* An another thread of current transaction is doing lock escalation.
         Therefore, the current thread gives up doing lock escalation.
       */
      MUTEX_UNLOCK (tran_lock->hold_mutex);
      return LK_GRANTED;
    }

  /* check if the lock escalation is needed. */
  if (class_entry->ngranules < PRM_LK_ESCALATION_AT)
    {
      MUTEX_UNLOCK (tran_lock->hold_mutex);
      return LK_GRANTED;
    }

  /* lock escalation should be performed */
  tran_lock->lock_escalation_on = true;

  if (class_entry->granted_mode == NULL_LOCK
      || class_entry->granted_mode == S_LOCK
      || class_entry->granted_mode == X_LOCK)
    {
      /* The class has no instance lock. */
      tran_lock->lock_escalation_on = false;
      MUTEX_UNLOCK (tran_lock->hold_mutex);
      return LK_GRANTED;
    }

  /* class_entry->granted_mode : IS_LOCK, IX_LOCK or SIX_LOCK */

  /* count shared and exclusive instance locks for the class */
  s_count = x_count = 0;
  inst_entry = tran_lock->inst_hold_list;
  for (; inst_entry != (LK_ENTRY *) NULL; inst_entry = inst_entry->tran_next)
    {
      if (!OID_EQ (&class_entry->res_head->oid,
		   &inst_entry->res_head->class_oid))
	{
	  continue;
	}

      switch (inst_entry->granted_mode)
	{
	case NS_LOCK:
	case S_LOCK:
	  s_count++;
	  break;
	case NX_LOCK:
	case U_LOCK:
	case X_LOCK:
	  x_count++;
	  break;
	default:
	  break;
	}
    }

  /* find the class that has the largest number of instance locks */
  if (s_count < x_count)
    {
      max_class_lock = X_LOCK;
    }
  else
    {
      if (class_entry->granted_mode == IX_LOCK)
	{
	  max_class_lock = SIX_LOCK;
	}
      else
	{
	  max_class_lock = S_LOCK;
	}
    }

  MUTEX_UNLOCK (tran_lock->hold_mutex);

  if (max_class_lock != NULL_LOCK)
    {
      /*
       * lock escalation is performed
       * 1. hold a lock on the class with the escalated lock mode
       */
      granted = lock_internal_perform_lock_object (thread_p, tran_index,
						   &class_entry->res_head->
						   oid, (OID *) NULL,
						   max_class_lock, waitsecs,
						   &class_entry, NULL);
      if (granted != LK_GRANTED)
	{
	  /* The reason of the lock request failure:
	   * 1. deadlock victim
	   * 2. lock timeout
	   * 3. interrupt
	   * 4. shortage of lock resource entries
	   * 5. shortage of lock entries
	   */
	  /* reset lock_escalation_on */
	  MUTEX_LOCK (rv, tran_lock->hold_mutex);
	  tran_lock->lock_escalation_on = false;
	  MUTEX_UNLOCK (tran_lock->hold_mutex);
	  return granted;
	}

      /* 2. release original class lock only one time
       *    in order to maintain original class lock count
       */
      lock_internal_perform_unlock_object (thread_p, class_entry, false,
					   true);

      /* 3. release all instance locks of the class */
      /*    it's already been done in lk_internal_lock_object()
       * (void)lock_remove_all_inst_locks(tran_index,
       *                          &class_entry->res_head->oid, max_class_lock);
       */

    }

  /* reset lock_escalation_on */
  MUTEX_LOCK (rv, tran_lock->hold_mutex);
  tran_lock->lock_escalation_on = false;
  MUTEX_UNLOCK (tran_lock->hold_mutex);

  return LK_GRANTED;
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: major functions for locking and unlocking
 *
 *   - lk_internal_lock_object_instant()
 *   - lk_internal_lock_object()
 *   - lk_internal_unlock_object()
 */

#if defined(SERVER_MODE)
/*
 * lock_internal_hold_lock_object_instant - Hold object lock with instant duration
 *
 * return: LK_GRANTED/LK_NOTGRANTED/LK_NOTGRANTED_DUE_ERROR
 *
 *   tran_index(in):
 *   oid(in):
 *   class_oid(in):
 *   lock(in):
 *
 * Note:hold a lock on the given object with instant duration.
 */
static int
lock_internal_hold_lock_object_instant (int tran_index, const OID * oid,
					const OID * class_oid, LOCK lock)
{
  unsigned int hash_index;
  LK_HASH *hash_anchor;
  LK_RES *res_ptr;
  LK_ENTRY *entry_ptr, *i;
  LOCK new_mode;
  LOCK group_mode;
  int rv;
  int compat1, compat2;

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 1)
    {
      fprintf (stderr,
	       "LK_DUMP::lk_internal_lock_object_instant()\n"
	       "  tran(%2d) : oid(%2d|%3d|%3d), class_oid(%2d|%3d|%3d), LOCK(%7s)\n",
	       tran_index,
	       oid->volid, oid->pageid, oid->slotid,
	       class_oid ? class_oid->volid : -1,
	       class_oid ? class_oid->pageid : -1,
	       class_oid ? class_oid->slotid : -1,
	       LOCK_TO_LOCKMODE_STRING (lock));
    }
#endif /* LK_DUMP */

#if defined(SERVER_MODE) && defined(DIAG_DEVEL)
  SET_DIAG_VALUE (diag_executediag, DIAG_OBJ_TYPE_LOCK_REQUEST, 1,
		  DIAG_VAL_SETTYPE_INC, NULL);
#endif /* SERVER_MODE && DIAG_DEVEL */
  if (class_oid != NULL && !OID_IS_ROOTOID (class_oid))
    {
      /* instance lock request */
      /* check if an implicit lock has been acquired */
      if (lock_get_object_lock (class_oid, oid_Root_class_oid, tran_index) >=
	  lock)
	return LK_GRANTED;
    }

  /* check if the lockable object is in object lock table */
  hash_index = LK_OBJ_LOCK_HASH (oid);
  hash_anchor = &lk_Gl.obj_hash_table[hash_index];

  MUTEX_LOCK (rv, hash_anchor->hash_mutex);

  /* find the lockable object in the hash chain */
  res_ptr = hash_anchor->hash_next;
  for (; res_ptr != (LK_RES *) NULL; res_ptr = res_ptr->hash_next)
    {
      if (OID_EQ (&res_ptr->oid, oid))
	break;
    }
  if (res_ptr == (LK_RES *) NULL)
    {
      /* the lockable object is NOT in the hash chain */
      /* the request can be granted */
      MUTEX_UNLOCK (hash_anchor->hash_mutex);
      return LK_GRANTED;
    }

  /* the lockable object exists in the hash chain */
  /* So, check whether I am a holder of the object. */

  /* hold lock resource mutex */
  MUTEX_LOCK (rv, res_ptr->res_mutex);
  /* release lock hash mutex */
  MUTEX_UNLOCK (hash_anchor->hash_mutex);

  /* Note: I am holding resource mutex only */

  /* find the lock entry of current transaction */
  entry_ptr = res_ptr->holder;
  for (; entry_ptr != (LK_ENTRY *) NULL; entry_ptr = entry_ptr->next)
    {
      if (entry_ptr->tran_index == tran_index)
	break;
    }

  /* I am not a lock holder of the lockable object. */
  if (entry_ptr == (LK_ENTRY *) NULL)
    {
      assert (lock >= NULL_LOCK && res_ptr->total_waiters_mode >= NULL_LOCK
	      && res_ptr->total_holders_mode >= NULL_LOCK);

      compat1 = lock_Comp[lock][res_ptr->total_waiters_mode];
      compat2 = lock_Comp[lock][res_ptr->total_holders_mode];
      assert (compat1 != DB_NA && compat2 != DB_NA);

      if (compat1 == true && compat2 == true)
	{
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  return LK_GRANTED;
	}
      else
	{
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  return LK_NOTGRANTED;
	}
    }

  /* I am a lock holder of the lockable object. */
  assert (lock >= NULL_LOCK && entry_ptr->granted_mode >= NULL_LOCK);
  new_mode = lock_Conv[lock][entry_ptr->granted_mode];
  assert (new_mode != NA_LOCK);

  if (new_mode == entry_ptr->granted_mode)
    {
      /* a request with either a less exclusive or an equal mode of lock */
      MUTEX_UNLOCK (res_ptr->res_mutex);
      return LK_GRANTED;
    }
  else
    {
      /* check the compatibility with other holders' granted mode */
      group_mode = NULL_LOCK;
      for (i = res_ptr->holder; i != (LK_ENTRY *) NULL; i = i->next)
	{
	  if (i != entry_ptr)
	    {
	      assert (i->granted_mode >= NULL_LOCK
		      && group_mode >= NULL_LOCK);
	      group_mode = lock_Conv[i->granted_mode][group_mode];
	      assert (group_mode != NA_LOCK);
	    }
	}

      assert (new_mode >= NULL_LOCK && group_mode >= NULL_LOCK);
      compat1 = lock_Comp[new_mode][group_mode];
      assert (compat1 != DB_NA);

      if (compat1 == true)
	{
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  return LK_GRANTED;
	}
      else
	{
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  return LK_NOTGRANTED;
	}
    }
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_internal_perform_lock_object - Performs actual object lock operation
 *
 * return: one of following values
 *              LK_GRANTED
 *              LK_NOTGRANTED_DUE_ABORTED
 *              LK_NOTGRANTED_DUE_TIMEOUT
 *              LK_NOTGRANTED_DUE_ERROR
 *
 *   tran_index(in):
 *   oid(in):
 *   class_oid(in):
 *   lock(in):
 *   waitsecs(in):
 *   entry_addr_ptr(in):
 *   class_entry(in):
 *
 * Note:lock an object whose id is pointed by oid with given lock mode 'lock'.
 *
 *     If cond_flag is true and the object has already been locked
 *     by other transaction, then return LK_NOTGRANTED;
 *     else this transaction is suspended until it can acquire the lock.
 */
static int
lock_internal_perform_lock_object (THREAD_ENTRY * thread_p, int tran_index,
				   const OID * oid, const OID * class_oid,
				   LOCK lock, int waitsecs,
				   LK_ENTRY ** entry_addr_ptr,
				   LK_ENTRY * class_entry)
{
  TRAN_ISOLATION isolation;
  int granted;
  unsigned int hash_index;
  LK_HASH *hash_anchor;
  int ret_val;
  LOCK group_mode, old_mode, new_mode;	/* lock mode */
  LK_RES *res_ptr;
  LK_ENTRY *entry_ptr = (LK_ENTRY *) NULL;
  LK_ENTRY *wait_entry_ptr = (LK_ENTRY *) NULL;
  LK_ENTRY *prev, *curr, *i;
  bool lock_conversion = false;
  THREAD_ENTRY *thrd_entry;
  int rv;
  LK_ACQUISITION_HISTORY *history;
  LK_TRAN_LOCK *tran_lock;
  bool is_instant_duration;
  int compat1, compat2;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  thrd_entry = thread_p;

  new_mode = group_mode = old_mode = NULL_LOCK;
#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 1)
    {
      fprintf (stderr,
	       "LK_DUMP::lk_internal_lock_object()\n"
	       "  tran(%2d) : oid(%2d|%3d|%3d), class_oid(%2d|%3d|%3d), LOCK(%7s) waitsecs(%d)\n",
	       tran_index,
	       oid->volid, oid->pageid, oid->slotid,
	       class_oid ? class_oid->volid : -1,
	       class_oid ? class_oid->pageid : -1,
	       class_oid ? class_oid->slotid : -1,
	       LOCK_TO_LOCKMODE_STRING (lock), waitsecs);
    }
#endif /* LK_DUMP */

  /* isolation */
  isolation = logtb_find_isolation (tran_index);

  /* initialize */
  *entry_addr_ptr = (LK_ENTRY *) NULL;

#if defined(SERVER_MODE) && defined(DIAG_DEVEL)
  SET_DIAG_VALUE (diag_executediag, DIAG_OBJ_TYPE_LOCK_REQUEST, 1,
		  DIAG_VAL_SETTYPE_INC, NULL);
#endif /* SERVER_MODE && DIAG_DEVEL */

  /* get current locking phase */
  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  is_instant_duration = tran_lock->is_instant_duration;

start:

  if (class_oid != NULL && !OID_IS_ROOTOID (class_oid))
    {
      /* instance lock request */
      /* 1. do lock escalation if it is needed. */
      granted =
	lock_escalate_if_needed (thread_p, class_entry, tran_index, waitsecs);
      if (granted != LK_GRANTED)
	{
	  /* The reason for the lock escalation failure
	   * - lock timeout, deadlock victim or interrupt
	   * - shortage of lock resource entries or lock entries
	   */
	  return granted;
	}
      /* 2. check if an implicit lock has been acquired */
      if (lock_get_object_lock (class_oid, oid_Root_class_oid, tran_index) >=
	  lock)
	{
	  mnt_lk_re_requested_on_objects (thread_p);	/* monitoring */
	  return LK_GRANTED;
	}
    }

  /* check if the lockable object is in object lock table */
  hash_index = LK_OBJ_LOCK_HASH (oid);
  hash_anchor = &lk_Gl.obj_hash_table[hash_index];

  /* hold hash_mutex */
  MUTEX_LOCK (rv, hash_anchor->hash_mutex);

  /* find the lockable object in the hash chain */
  res_ptr = hash_anchor->hash_next;
  for (; res_ptr != (LK_RES *) NULL; res_ptr = res_ptr->hash_next)
    {
      if (OID_EQ (&res_ptr->oid, oid))
	{
	  break;
	}
    }

  if (res_ptr == (LK_RES *) NULL)
    {
      /* the lockable object is NOT in the hash chain */
      /* the lock request can be granted. */

      /* allocate a lock resource entry */
      res_ptr = lock_alloc_resource ();
      if (res_ptr == (LK_RES *) NULL)
	{
	  MUTEX_UNLOCK (hash_anchor->hash_mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ALLOC_RESOURCE,
		  1, "lock resource entry");
	  return LK_NOTGRANTED_DUE_ERROR;
	}
      /* initialize the lock resource entry */
      LK_INIT_RES_ALLOCATED (res_ptr, oid, class_oid, NULL_LOCK);

      /* hold res_mutex */
      MUTEX_LOCK (rv, res_ptr->res_mutex);

      /* Note: I am holding hash_mutex and res_mutex. */
      if (LK_NON2PL_LOCK_REQUEST (res_ptr, isolation, lock))
	{
	  /* It might be requested by clients */
	  entry_ptr = lock_add_non2pl_lock (res_ptr, tran_index, lock);
	  if (entry_ptr == (LK_ENTRY *) NULL)
	    {
	      MUTEX_UNLOCK (res_ptr->res_mutex);
	      MUTEX_UNLOCK (hash_anchor->hash_mutex);
	      lock_free_resource (res_ptr);
	      return LK_NOTGRANTED_DUE_ERROR;
	    }
	}
      else
	{
	  /* allocate a lock entry */
	  entry_ptr = lock_alloc_entry ();
	  if (entry_ptr == (LK_ENTRY *) NULL)
	    {
	      MUTEX_UNLOCK (res_ptr->res_mutex);
	      MUTEX_UNLOCK (hash_anchor->hash_mutex);
	      lock_free_resource (res_ptr);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ALLOC_RESOURCE,
		      1, "lock heap entry");
	      return LK_NOTGRANTED_DUE_ERROR;
	    }
	  /* initialize the lock entry as granted state */
	  LK_INIT_ENTRY_GRANTED (entry_ptr, tran_index, res_ptr, lock);
	  if (is_instant_duration)
	    {
	      entry_ptr->mlk_count++;
	    }

	  /* add the lock entry into the holder list */
	  res_ptr->holder = entry_ptr;

	  /* to manage granules */
	  entry_ptr->class_entry = class_entry;
	  if (class_entry)
	    {
	      class_entry->ngranules++;
	    }

	  /* add the lock entry into the transaction hold list */
	  lock_insert_into_tran_hold_list (entry_ptr);

	  res_ptr->total_holders_mode = lock;

	  /* Record number of acquired locks */
	  mnt_lk_acquired_on_objects (thread_p);
#if defined(LK_TRACE_OBJECT)
	  LK_MSG_LOCK_ACQUIRED (entry_ptr);
#endif /* LK_TRACE_OBJECT */

	  if (NEED_LOCK_ACQUISITION_HISTORY (isolation, entry_ptr))
	    {
	      history = (LK_ACQUISITION_HISTORY *)
		malloc (sizeof (LK_ACQUISITION_HISTORY));
	      if (history == NULL)
		{
		  MUTEX_UNLOCK (res_ptr->res_mutex);
		  MUTEX_UNLOCK (hash_anchor->hash_mutex);
		  return LK_NOTGRANTED_DUE_ERROR;
		}

	      RECORD_LOCK_ACQUISITION_HISTORY (entry_ptr, history, lock);
	    }
	}

      /* connect the lock resource entry into the hash chain */
      res_ptr->hash_next = hash_anchor->hash_next;
      hash_anchor->hash_next = res_ptr;

      /* release all mutexes */
      MUTEX_UNLOCK (res_ptr->res_mutex);
      MUTEX_UNLOCK (hash_anchor->hash_mutex);

      *entry_addr_ptr = entry_ptr;
      return LK_GRANTED;
    }

  /* the lockable object exists in the hash chain
   * So, check whether I am a holder of the object.
   */

  /* hold lock resource mutex */
  MUTEX_LOCK (rv, res_ptr->res_mutex);
  /* release lock hash mutex */
  MUTEX_UNLOCK (hash_anchor->hash_mutex);

  /* Note: I am holding res_mutex only */

  /* find the lock entry of current transaction */
  entry_ptr = res_ptr->holder;
  while (entry_ptr != (LK_ENTRY *) NULL)
    {
      if (entry_ptr->tran_index == tran_index)
	{
	  break;
	}
      entry_ptr = entry_ptr->next;
    }

  if (entry_ptr == NULL)
    {
      /* The object exists in the hash chain &
       * I am not a lock holder of the lockable object.
       */
      /* 1. I am not a holder & my request can be granted. */
      assert (lock >= NULL_LOCK && res_ptr->total_waiters_mode >= NULL_LOCK
	      && res_ptr->total_holders_mode >= NULL_LOCK);
      compat1 = lock_Comp[lock][res_ptr->total_waiters_mode];
      compat2 = lock_Comp[lock][res_ptr->total_holders_mode];
      assert (compat1 != DB_NA && compat2 != DB_NA);

      if (compat1 == true && compat2 == true)
	{
	  if (LK_NON2PL_LOCK_REQUEST (res_ptr, isolation, lock))
	    {
	      entry_ptr = lock_add_non2pl_lock (res_ptr, tran_index, lock);
	      if (entry_ptr == (LK_ENTRY *) NULL)
		{
		  MUTEX_UNLOCK (res_ptr->res_mutex);
		  return LK_NOTGRANTED_DUE_ERROR;
		}
	      MUTEX_UNLOCK (res_ptr->res_mutex);
	      *entry_addr_ptr = entry_ptr;
	      return LK_GRANTED;
	    }

	  /* allocate a lock entry */
	  entry_ptr = lock_alloc_entry ();
	  if (entry_ptr == (LK_ENTRY *) NULL)
	    {
	      MUTEX_UNLOCK (res_ptr->res_mutex);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ALLOC_RESOURCE,
		      1, "lock heap entry");
	      return LK_NOTGRANTED_DUE_ERROR;
	    }
	  /* initialize the lock entry as granted state */
	  LK_INIT_ENTRY_GRANTED (entry_ptr, tran_index, res_ptr, lock);
	  if (is_instant_duration)
	    {
	      entry_ptr->mlk_count++;
	    }
	  /* to manage granules */
	  entry_ptr->class_entry = class_entry;
	  if (class_entry)
	    {
	      class_entry->ngranules++;
	    }

	  /* add the lock entry into the holder list */
	  lock_position_holder_entry (res_ptr, entry_ptr);

	  /* change total_holders_mode (total mode of holder list) */
	  assert (lock >= NULL_LOCK
		  && res_ptr->total_holders_mode >= NULL_LOCK);
	  res_ptr->total_holders_mode =
	    lock_Conv[lock][res_ptr->total_holders_mode];
	  assert (res_ptr->total_holders_mode != NA_LOCK);

	  /* add the lock entry into the transaction hold list */
	  lock_insert_into_tran_hold_list (entry_ptr);

	  lock_update_non2pl_list (res_ptr, tran_index, lock);

	  /* Record number of acquired locks */
	  mnt_lk_acquired_on_objects (thread_p);
#if defined(LK_TRACE_OBJECT)
	  LK_MSG_LOCK_ACQUIRED (entry_ptr);
#endif /* LK_TRACE_OBJECT */

	  if (NEED_LOCK_ACQUISITION_HISTORY (isolation, entry_ptr))
	    {
	      history = (LK_ACQUISITION_HISTORY *)
		malloc (sizeof (LK_ACQUISITION_HISTORY));
	      if (history == NULL)
		{
		  MUTEX_UNLOCK (res_ptr->res_mutex);
		  return LK_NOTGRANTED_DUE_ERROR;
		}

	      RECORD_LOCK_ACQUISITION_HISTORY (entry_ptr, history, lock);
	    }

	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  *entry_addr_ptr = entry_ptr;
	  return LK_GRANTED;
	}

      /* 2. I am not a holder & my request cannot be granted. */
      if (LK_NON2PL_LOCK_REQUEST (res_ptr, isolation, lock))
	{
	  entry_ptr = lock_add_non2pl_lock (res_ptr, tran_index,
					    INCON_NON_TWO_PHASE_LOCK);
	  if (entry_ptr == (LK_ENTRY *) NULL)
	    {
	      MUTEX_UNLOCK (res_ptr->res_mutex);
	      return LK_NOTGRANTED_DUE_ERROR;
	    }
	  else
	    {
	      MUTEX_UNLOCK (res_ptr->res_mutex);
	      *entry_addr_ptr = entry_ptr;
	      return LK_GRANTED;
	    }
	}

      if (waitsecs == LK_ZERO_WAIT || waitsecs == LK_FORCE_ZERO_WAIT)
	{
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  if (waitsecs == LK_ZERO_WAIT)
	    {
	      if (entry_ptr == NULL)
		{
		  entry_ptr = lock_alloc_entry ();
		  if (entry_ptr == (LK_ENTRY *) NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_LK_ALLOC_RESOURCE, 1, "lock heap entry");
		      return LK_NOTGRANTED_DUE_ERROR;
		    }
		  LK_INIT_ENTRY_BLOCKED (entry_ptr, thread_p, tran_index,
					 res_ptr, lock);
		  if (is_instant_duration
		      /* && lock_Comp[lock][NULL_LOCK] == true */ )
		    {
		      entry_ptr->mlk_count++;
		    }
		}
	      (void) lock_set_error_for_timeout (thread_p, entry_ptr);

	      lock_free_entry (entry_ptr);
	    }
	  return LK_NOTGRANTED_DUE_TIMEOUT;
	}

      /* check if another thread is waiting for the same resource
       */
      wait_entry_ptr = res_ptr->waiter;
      while (wait_entry_ptr != (LK_ENTRY *) NULL)
	{
	  if (wait_entry_ptr->tran_index == tran_index)
	    {
	      break;
	    }
	  wait_entry_ptr = wait_entry_ptr->next;
	}

      if (wait_entry_ptr != NULL)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_LK_MANY_LOCK_WAIT_TRAN, 1, tran_index);
	  thread_lock_entry (thrd_entry);
	  thread_lock_entry (wait_entry_ptr->thrd_entry);
	  if (wait_entry_ptr->thrd_entry->lockwait == NULL)
	    {
	      /*  */
	      thread_unlock_entry (wait_entry_ptr->thrd_entry);
	      thread_unlock_entry (thrd_entry);
	      MUTEX_UNLOCK (res_ptr->res_mutex);
	      goto start;
	    }

	  thrd_entry->tran_next_wait =
	    wait_entry_ptr->thrd_entry->tran_next_wait;
	  wait_entry_ptr->thrd_entry->tran_next_wait = thrd_entry;

	  thread_unlock_entry (wait_entry_ptr->thrd_entry);
	  MUTEX_UNLOCK (res_ptr->res_mutex);

	  while (1)
	    {
	      thread_suspend_wakeup_and_unlock_entry (thrd_entry,
						      THREAD_LOCK_SUSPENDED);

	      if (entry_ptr->thrd_entry->resume_status ==
		  THREAD_RESUME_DUE_TO_INTERRUPT
		  && entry_ptr->thrd_entry->interrupted)
		{
		  /* In case the shutdown thread wakes me up,
		   * I keep being suspended and waiting until the lock holder
		   * wakes me up, because it is too dangerous to continue
		   * process with a lock not granted.
		   */
		  continue;
		}
	      else if (entry_ptr->thrd_entry->resume_status !=
		       THREAD_LOCK_RESUMED)
		{
		  assert (0);
		}

	      break;
	    }

	  goto start;
	}

      /* allocate a lock entry. */
      entry_ptr = lock_alloc_entry ();
      if (entry_ptr == (LK_ENTRY *) NULL)
	{
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ALLOC_RESOURCE,
		  1, "lock heap entry");
	  return LK_NOTGRANTED_DUE_ERROR;
	}
      /* initialize the lock entry as blocked state */
      LK_INIT_ENTRY_BLOCKED (entry_ptr, thread_p, tran_index, res_ptr, lock);
      if (is_instant_duration)
	{
	  entry_ptr->mlk_count++;
	}

      /* append the lock request at the end of the waiter */
      prev = (LK_ENTRY *) NULL;
      for (i = res_ptr->waiter; i != (LK_ENTRY *) NULL;)
	{
	  prev = i;
	  i = i->next;
	}
      if (prev == (LK_ENTRY *) NULL)
	{
	  res_ptr->waiter = entry_ptr;
	}
      else
	{
	  prev->next = entry_ptr;
	}

      /* change total_waiters_mode (total mode of waiting waiter) */
      assert (lock >= NULL_LOCK && res_ptr->total_waiters_mode >= NULL_LOCK);
      res_ptr->total_waiters_mode =
	lock_Conv[lock][res_ptr->total_waiters_mode];
      assert (res_ptr->total_waiters_mode != NA_LOCK);

      goto blocked;

    }				/* end of a new lock request */

  if (LK_NON2PL_LOCK_REQUEST (res_ptr, isolation, lock))
    {
      if (entry_ptr->granted_mode == NX_LOCK
	  || entry_ptr->granted_mode == X_LOCK)
	{
	  /* The conversioned mode might be the same with the current mode. */
	  /* The only exception case is followings.
	     When the current mode is NX_LOCK and thr request mode is U_LOCK,
	     the conversioned mode will be X_LOCK.
	     In this case, however, the intention of U_LOCK of Uncommitted Read
	     isolation is only having the intent of READ.
	     Therefore, the U_LOCK request of this case can be granted
	     without acquiring it.
	   */
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  mnt_lk_re_requested_on_objects (thread_p);	/* monitoring */
	  *entry_addr_ptr = entry_ptr;
	  return LK_GRANTED;
	}
      /* entry_ptr->granted_mode != NX_LOCK &&
         entry_ptr->granted_mode != X_LOCK
       */
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_LK_FAULT_GRANTED_MODE, 5,
	      res_ptr->oid.volid, res_ptr->oid.pageid, res_ptr->oid.slotid,
	      entry_ptr->tran_index,
	      LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode));
    }

  /* The object exists in the hash chain &
   * I am a lock holder of the lockable object.
   */
  lock_conversion = true;
  old_mode = entry_ptr->granted_mode;
  assert (lock >= NULL_LOCK && entry_ptr->granted_mode >= NULL_LOCK);
  new_mode = lock_Conv[lock][entry_ptr->granted_mode];
  assert (new_mode != NA_LOCK);

  if (new_mode == entry_ptr->granted_mode)
    {
      /* a request with either a less exclusive or an equal mode of lock */
      entry_ptr->count += 1;
      if (is_instant_duration)
	{
	  compat1 = lock_Comp[lock][entry_ptr->granted_mode];
	  assert (compat1 != DB_NA);

	  if ((lock >= IX_LOCK
	       && (entry_ptr->mlk_count == 0
		   && entry_ptr->granted_mode >= IX_LOCK)) && compat1 != true)
	    {
	      /* if the lock is already aquired with incompatible mode by current transaction,
	         remove instant instance locks */
	      lock_stop_instant_lock_mode (thread_p, tran_index, false);
	    }
	  else
	    {
	      entry_ptr->mlk_count++;
	    }
	}

      if (NEED_LOCK_ACQUISITION_HISTORY (isolation, entry_ptr))
	{
	  history = (LK_ACQUISITION_HISTORY *)
	    malloc (sizeof (LK_ACQUISITION_HISTORY));
	  if (history == NULL)
	    {
	      MUTEX_UNLOCK (res_ptr->res_mutex);
	      return LK_NOTGRANTED_DUE_ERROR;
	    }

	  RECORD_LOCK_ACQUISITION_HISTORY (entry_ptr, history, lock);
	}

      MUTEX_UNLOCK (res_ptr->res_mutex);
      mnt_lk_re_requested_on_objects (thread_p);	/* monitoring */
      *entry_addr_ptr = entry_ptr;
      return LK_GRANTED;
    }

  /* check the compatibility with other holders' granted mode */
  group_mode = NULL_LOCK;
  for (i = res_ptr->holder; i != (LK_ENTRY *) NULL; i = i->next)
    {
      if (i != entry_ptr)
	{
	  assert (i->granted_mode >= NULL_LOCK && group_mode >= NULL_LOCK);
	  group_mode = lock_Conv[i->granted_mode][group_mode];
	  assert (group_mode != NA_LOCK);
	}
    }

  assert (new_mode >= NULL_LOCK && group_mode >= NULL_LOCK);
  compat1 = lock_Comp[new_mode][group_mode];
  assert (compat1 != DB_NA);

  if (compat1 == true)
    {
      if (NEED_LOCK_ACQUISITION_HISTORY (isolation, entry_ptr))
	{
	  history = (LK_ACQUISITION_HISTORY *)
	    malloc (sizeof (LK_ACQUISITION_HISTORY));
	  if (history == NULL)
	    {
	      MUTEX_UNLOCK (res_ptr->res_mutex);
	      return LK_NOTGRANTED_DUE_ERROR;
	    }

	  RECORD_LOCK_ACQUISITION_HISTORY (entry_ptr, history, lock);
	}

      entry_ptr->granted_mode = new_mode;
      entry_ptr->count += 1;

      assert (lock >= NULL_LOCK && res_ptr->total_holders_mode >= NULL_LOCK);
      res_ptr->total_holders_mode =
	lock_Conv[lock][res_ptr->total_holders_mode];
      assert (res_ptr->total_holders_mode != NA_LOCK);

      lock_update_non2pl_list (res_ptr, tran_index, lock);
      MUTEX_UNLOCK (res_ptr->res_mutex);

      goto lock_conversion_treatement;
    }

  /* I am a holder & my request cannot be granted. */
  if (waitsecs == LK_ZERO_WAIT || waitsecs == LK_FORCE_ZERO_WAIT)
    {
      MUTEX_UNLOCK (res_ptr->res_mutex);
      if (waitsecs == LK_ZERO_WAIT)
	{
	  (void) lock_set_error_for_timeout (thread_p, entry_ptr);
	}
      return LK_NOTGRANTED_DUE_TIMEOUT;
    }

  /* Upgrader Positioning Rule (UPR) */

  /* check if another thread is waiting for the same resource
   */
  if (entry_ptr->blocked_mode != NULL_LOCK)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_LK_MANY_LOCK_WAIT_TRAN,
	      1, tran_index);
      thread_lock_entry (thrd_entry);
      thread_lock_entry (entry_ptr->thrd_entry);

      if (entry_ptr->thrd_entry->lockwait == NULL)
	{
	  thread_unlock_entry (entry_ptr->thrd_entry);
	  thread_unlock_entry (thrd_entry);
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  goto start;
	}

      thrd_entry->tran_next_wait = entry_ptr->thrd_entry->tran_next_wait;
      entry_ptr->thrd_entry->tran_next_wait = thrd_entry;

      thread_unlock_entry (entry_ptr->thrd_entry);

      MUTEX_UNLOCK (res_ptr->res_mutex);

      thread_suspend_wakeup_and_unlock_entry (thrd_entry,
					      THREAD_LOCK_SUSPENDED);
      if (thrd_entry->resume_status == THREAD_RESUME_DUE_TO_INTERRUPT)
	{
	  /* a shutdown thread wakes me up */
	  return LK_NOTGRANTED_DUE_ERROR;
	}
      else if (thrd_entry->resume_status != THREAD_LOCK_RESUMED)
	{
	  /* wake up with other reason */
	  assert (0);

	  return LK_NOTGRANTED_DUE_ERROR;
	}
      else
	{
	  assert (thrd_entry->resume_status == THREAD_LOCK_RESUMED);
	}

      goto start;
    }

  entry_ptr->blocked_mode = new_mode;
  entry_ptr->count += 1;
  entry_ptr->thrd_entry = thread_p;

  assert (lock >= NULL_LOCK && res_ptr->total_holders_mode >= NULL_LOCK);
  res_ptr->total_holders_mode = lock_Conv[lock][res_ptr->total_holders_mode];
  assert (res_ptr->total_holders_mode != NA_LOCK);

  /* remove the lock entry from the holder list */
  prev = (LK_ENTRY *) NULL;
  curr = res_ptr->holder;
  while ((curr != (LK_ENTRY *) NULL) && (curr != entry_ptr))
    {
      prev = curr;
      curr = curr->next;
    }
  if (prev == (LK_ENTRY *) NULL)
    {
      res_ptr->holder = entry_ptr->next;
    }
  else
    {
      prev->next = entry_ptr->next;
    }

  /* position the lock entry in the holder list according to UPR */
  lock_position_holder_entry (res_ptr, entry_ptr);

blocked:

  /* LK_CANWAIT(waitsecs) : waitsecs > 0 */
  mnt_lk_waited_on_objects (thread_p);
#if defined(LK_TRACE_OBJECT)
  LK_MSG_LOCK_WAITFOR (entry_ptr);
#endif /* LK_TRACE_OBJECT */

  (void) thread_lock_entry (entry_ptr->thrd_entry);
  MUTEX_UNLOCK (res_ptr->res_mutex);
  ret_val = lock_suspend (thread_p, entry_ptr, waitsecs);
  if (ret_val != LOCK_RESUMED)
    {
      /* Following three cases are possible.
       * 1. lock timeout 2. deadlock victim  3. interrupt
       * In any case, current thread must remove the wait info.
       */
      lock_internal_perform_unlock_object (thread_p, entry_ptr, false, false);

      if (ret_val == LOCK_RESUMED_ABORTED)
	{
	  return LK_NOTGRANTED_DUE_ABORTED;
	}
      else if (ret_val == LOCK_RESUMED_INTERRUPT)
	{
	  return LK_NOTGRANTED_DUE_ERROR;
	}
      else			/* LOCK_RESUMED_TIMEOUT || LOCK_SUSPENDED */
	{
	  return LK_NOTGRANTED_DUE_TIMEOUT;
	}
    }

  if (NEED_LOCK_ACQUISITION_HISTORY (isolation, entry_ptr))
    {
      history = (LK_ACQUISITION_HISTORY *)
	malloc (sizeof (LK_ACQUISITION_HISTORY));
      if (history == NULL)
	{
	  return LK_NOTGRANTED_DUE_ERROR;
	}

      RECORD_LOCK_ACQUISITION_HISTORY (entry_ptr, history, lock);
    }

  /* The transaction now got the lock on the object */
lock_conversion_treatement:

  if (entry_ptr->res_head->type == LOCK_RESOURCE_CLASS
      && lock_conversion == true)
    {
      new_mode = entry_ptr->granted_mode;
      switch (old_mode)
	{
	case IS_LOCK:
	  if (new_mode == X_LOCK
	      || ((new_mode == S_LOCK || new_mode == SIX_LOCK)
		  && (isolation == TRAN_REP_CLASS_COMMIT_INSTANCE
		      || isolation == TRAN_COMMIT_CLASS_COMMIT_INSTANCE)))
	    {
	      lock_remove_all_inst_locks (thread_p, tran_index, oid, S_LOCK);
	    }
	  break;

	case IX_LOCK:
	  if (new_mode == SIX_LOCK
	      && (isolation == TRAN_REP_CLASS_COMMIT_INSTANCE
		  || isolation == TRAN_COMMIT_CLASS_COMMIT_INSTANCE))
	    {
	      lock_remove_all_inst_locks (thread_p, tran_index, oid, S_LOCK);
	    }
	  else if (new_mode == X_LOCK)
	    {
	      lock_remove_all_inst_locks (thread_p, tran_index, oid, X_LOCK);
	    }
	  break;

	case SIX_LOCK:
	  /* new_mode == X_LOCK */
	  lock_remove_all_inst_locks (thread_p, tran_index, oid, X_LOCK);
	  break;

	default:
	  break;
	}

      mnt_lk_converted_on_objects (thread_p);
#if defined(LK_TRACE_OBJECT)
      LK_MSG_LOCK_CONVERTED (entry_ptr);
#endif /* LK_TRACE_OBJECT */
    }

  if (lock_conversion == false)
    {
      /* to manage granules */
      entry_ptr->class_entry = class_entry;
      if (class_entry)
	{
	  class_entry->ngranules++;
	}
    }

  *entry_addr_ptr = entry_ptr;
  return LK_GRANTED;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_internal_perform_unlock_object - Performs actual object unlock operation
 *
 * return:
 *
 *   entry_ptr(in):
 *   release_flag(in):
 *   move_to_non2pl(in):
 *
 * Note:Unlock a lock specified by entry_ptr.
 *     Therefore, for the 2 phase locking, the caller must unlock from leaf
 *     to root or atomically all locks to which the transaction is related.
 *
 *     if release_flag is true, release the lock item.
 *     Otherwise, just decrement the lock count for supporting isolation level.
 */
static void
lock_internal_perform_unlock_object (THREAD_ENTRY * thread_p,
				     LK_ENTRY * entry_ptr, int release_flag,
				     int move_to_non2pl)
{
  int tran_index;
  LK_RES *res_ptr;
  LK_ENTRY *i;
  LK_ENTRY *prev, *curr;
  LK_ENTRY *from_whom;
  LOCK mode;
  int rv;

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 1)
    {
      fprintf (stderr,
	       "LK_DUMP::lk_internal_unlock_object()\n"
	       "  tran(%2d) : oid(%2d|%3d|%3d), class_oid(%2d|%3d|%3d), LOCK(%7s)\n",
	       entry_ptr->tran_index,
	       entry_ptr->res_head->oid.volid,
	       entry_ptr->res_head->oid.pageid,
	       entry_ptr->res_head->oid.slotid,
	       entry_ptr->res_head->class_oid.volid,
	       entry_ptr->res_head->class_oid.pageid,
	       entry_ptr->res_head->class_oid.slotid,
	       LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode));
    }
#endif /* LK_DUMP */

  if (entry_ptr == (LK_ENTRY *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_internal_unlock_object", "NULL entry pointer");
      return;
    }

  if (release_flag == false)
    {
      if (entry_ptr->blocked_mode != NULL_LOCK)
	{
	  entry_ptr->count -= 1;	/* still, might be count > 0 */
	}
      else
	{
	  entry_ptr->count -= 1;
	  if (entry_ptr->count > 0)
	    {
	      return;
	    }
	}
    }

  /* hold resource mutex */
  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  res_ptr = entry_ptr->res_head;
  MUTEX_LOCK (rv, res_ptr->res_mutex);

  /* check if the transaction is in the holder list */
  prev = (LK_ENTRY *) NULL;
  curr = res_ptr->holder;
  while (curr != (LK_ENTRY *) NULL)
    {
      if (curr->tran_index == tran_index)
	{
	  break;
	}
      prev = curr;
      curr = curr->next;
    }

  if (curr == (LK_ENTRY *) NULL)
    {
      /* the transaction is not in the holder list, check the waiter. */
      prev = (LK_ENTRY *) NULL;
      curr = res_ptr->waiter;
      while (curr != (LK_ENTRY *) NULL)
	{
	  if (curr->tran_index == tran_index)
	    {
	      break;
	    }
	  prev = curr;
	  curr = curr->next;
	}

      if (curr != (LK_ENTRY *) NULL)
	{
	  /* get the next lock waiter */
	  from_whom = curr->next;

	  /* remove the lock entry from the waiter */
	  if (prev == (LK_ENTRY *) NULL)
	    {
	      res_ptr->waiter = curr->next;
	    }
	  else
	    {
	      prev->next = curr->next;
	    }

	  /* free the lock entry */
	  lock_free_entry (curr);

	  if (from_whom != (LK_ENTRY *) NULL)
	    {
	      /* grant blocked waiter & change total_waiters_mode */
	      lock_grant_blocked_waiter_partial (thread_p, res_ptr,
						 from_whom);
	    }
	  else
	    {
	      /* change only total_waiters_mode */
	      mode = NULL_LOCK;
	      for (i = res_ptr->waiter; i != (LK_ENTRY *) NULL; i = i->next)
		{
		  assert (i->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
		  mode = lock_Conv[i->blocked_mode][mode];
		  assert (mode != NA_LOCK);
		}
	      res_ptr->total_waiters_mode = mode;
	    }
	}
      else
	{
	  /* The transaction is neither the lock holder nor the lock waiter */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_LOST_TRANSACTION, 4,
		  tran_index,
		  res_ptr->oid.volid, res_ptr->oid.pageid,
		  res_ptr->oid.slotid);
	}

      MUTEX_UNLOCK (res_ptr->res_mutex);

      return;
    }

  /* The transaction is in the holder list. Consult the holder list. */

  /* remove the entry from the holder list */
  if (prev == (LK_ENTRY *) NULL)
    {
      res_ptr->holder = curr->next;
    }
  else
    {
      prev->next = curr->next;
    }

  if (release_flag == false && curr->count > 0)
    {
      /* The current transaction was a blocked holder.
       * lock timeout is called or it is selected as a deadlock victim
       */
      curr->blocked_mode = NULL_LOCK;
      lock_position_holder_entry (res_ptr, entry_ptr);
    }
  else
    {
      /* remove the lock entry from the transaction lock hold list */
      (void) lock_delete_from_tran_hold_list (curr);

      /* to manage granules */
      if (curr->class_entry)
	{
	  curr->class_entry->ngranules--;
	}

      /* If it's not the end of transaction, it's a non2pl lock */
      if (release_flag == false && move_to_non2pl == true)
	{
	  (void) lock_add_non2pl_lock (res_ptr, tran_index,
				       curr->granted_mode);
	}
      /* free the lock entry */
      lock_free_entry (curr);
    }

  /* change total_holders_mode */
  mode = NULL_LOCK;
  for (i = res_ptr->holder; i != (LK_ENTRY *) NULL; i = i->next)
    {
      assert (i->granted_mode >= NULL_LOCK && mode >= NULL_LOCK);
      mode = lock_Conv[i->granted_mode][mode];
      assert (mode != NA_LOCK);

      assert (i->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
      mode = lock_Conv[i->blocked_mode][mode];
      assert (mode != NA_LOCK);
    }
  res_ptr->total_holders_mode = mode;

  if (res_ptr->holder == NULL && res_ptr->waiter == NULL)
    {
      if (res_ptr->non2pl == NULL)
	{
	  /* if resource entry is empty, deallocate it. */
	  (void) lock_dealloc_resource (res_ptr);
	}
      else
	{
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	}
    }
  else
    {
      /* grant blocked holders and blocked waiters */
      lock_grant_blocked_holder (thread_p, res_ptr);

      (void) lock_grant_blocked_waiter (thread_p, res_ptr);
      MUTEX_UNLOCK (res_ptr->res_mutex);
    }
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: demote, unlock and remove locks
 *
 *   - lock_internal_demote_shared_class_lock()
 *   - lock_internal_demote_update_inst_lock()
 *   - lock_demote_shared_class_lock()
 *   - lock_demote_all_shared_class_locks()
 *   - lock_unlock_shared_class_lock()
 *   - lock_unlock_shared_inst_lock()
 *   - lock_remove_all_class_locks()
 *   - lock_remove_all_inst_locks()
 */

#if defined(SERVER_MODE)
/*
 * lock_internal_demote_shared_class_lock - Demote the shared class lock
 *
 * return: error code
 *
 *   entry_ptr(in):
 *
 * Note:This function demotes the lock mode of given class lock
 *     if the lock mode is shared lock. After the demotion, this function
 *     grants the blocked requestors if the blocked lock mode is grantable.
 *
 *     demote shared class lock (S_LOCK => IS_LOCK, SIX_LOCK => IX_LOCK)
 */
static int
lock_internal_demote_shared_class_lock (THREAD_ENTRY * thread_p,
					LK_ENTRY * entry_ptr)
{
  LK_RES *res_ptr;		/* lock resource entry pointer */
  LK_ENTRY *check, *i;		/* lock entry pointer     */
  LOCK total_mode;
  int rv;

  /* The caller is not holding any mutex */

  res_ptr = entry_ptr->res_head;
  MUTEX_LOCK (rv, res_ptr->res_mutex);

  /* find the given lock entry in the holder list */
  for (check = res_ptr->holder; check != NULL; check = check->next)
    {
      if (check == entry_ptr)
	{
	  break;
	}
    }
  if (check == (LK_ENTRY *) NULL)
    {				/* not found */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LK_NOTFOUND_IN_LOCK_HOLDER_LIST, 5,
	      LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
	      entry_ptr->tran_index,
	      res_ptr->oid.volid, res_ptr->oid.pageid, res_ptr->oid.slotid);
      MUTEX_UNLOCK (res_ptr->res_mutex);
      return ER_LK_NOTFOUND_IN_LOCK_HOLDER_LIST;
    }

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 1)
    {
      fprintf (stderr, "LK_DUMP::lk_internal_demote_shared_class_lock()\n"
	       "  tran(%2d) : oid(%d|%d|%d), class_oid(%d|%d|%d), LOCK(%7s -> %7s)\n",
	       entry_ptr->tran_index,
	       entry_ptr->res_head->oid.volid,
	       entry_ptr->res_head->oid.pageid,
	       entry_ptr->res_head->oid.slotid,
	       entry_ptr->res_head->class_oid.volid,
	       entry_ptr->res_head->class_oid.pageid,
	       entry_ptr->res_head->class_oid.slotid,
	       LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
	       entry_ptr->granted_mode == S_LOCK ?
	       LOCK_TO_LOCKMODE_STRING (IS_LOCK) :
	       (entry_ptr->granted_mode == X_LOCK ?
		LOCK_TO_LOCKMODE_STRING (IX_LOCK) :
		LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode)));
    }
#endif /* LK_DUMP */

  /* demote the shared class lock(granted mode) of the lock entry */
  switch (check->granted_mode)
    {
    case S_LOCK:
      check->granted_mode = IS_LOCK;
      break;

    case SIX_LOCK:
      check->granted_mode = IX_LOCK;
      break;

    default:
      MUTEX_UNLOCK (res_ptr->res_mutex);
      return NO_ERROR;
    }

  /* change total_holders_mode */
  total_mode = NULL_LOCK;
  for (i = res_ptr->holder; i != NULL; i = i->next)
    {
      assert (i->granted_mode >= NULL_LOCK && total_mode >= NULL_LOCK);
      total_mode = lock_Conv[i->granted_mode][total_mode];
      assert (total_mode != NA_LOCK);

      assert (i->blocked_mode >= NULL_LOCK && total_mode >= NULL_LOCK);
      total_mode = lock_Conv[i->blocked_mode][total_mode];
      assert (total_mode != NA_LOCK);
    }
  res_ptr->total_holders_mode = total_mode;

  /* grant the blocked holders and blocked waiters */
  lock_grant_blocked_holder (thread_p, res_ptr);
  (void) lock_grant_blocked_waiter (thread_p, res_ptr);

  MUTEX_UNLOCK (res_ptr->res_mutex);

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_internal_demote_update_inst_lock - Demote the update instance lock
 *
 * return: error code
 *
 *   entry_ptr(in):
 *
 * Note:This function demotes the lock mode of given instance lock
 *     if the lock mode is update lock. After the demotion, this function
 *     grants the blocked requestors if the blocked lock mode is grantable.
 *
 *     demote update instance lock (U_LOCK => S_LOCK/NS_LOCK)
 */
static int
lock_internal_demote_update_inst_lock (THREAD_ENTRY * thread_p,
				       LK_ENTRY * entry_ptr)
{
  LK_RES *res_ptr;		/* lock resource entry pointer */
  LK_ENTRY *check, *i;		/* lock entry pointer */
  LOCK total_mode;
  int rv;

  /* The caller is not holding any mutex */

  res_ptr = entry_ptr->res_head;
  MUTEX_LOCK (rv, res_ptr->res_mutex);

  /* find the given lock entry in the holder list */
  for (check = res_ptr->holder; check != NULL; check = check->next)
    {
      if (check == entry_ptr)
	{
	  break;
	}
    }
  if (check == (LK_ENTRY *) NULL)
    {				/* not found */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LK_NOTFOUND_IN_LOCK_HOLDER_LIST, 5,
	      LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
	      entry_ptr->tran_index,
	      res_ptr->oid.volid, res_ptr->oid.pageid, res_ptr->oid.slotid);
      MUTEX_UNLOCK (res_ptr->res_mutex);
      return ER_LK_NOTFOUND_IN_LOCK_HOLDER_LIST;
    }

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 1)
    {
      fprintf (stderr, "LK_DUMP::lk_internal_demote_update_inst_lock()\n"
	       "  tran(%2d) : oid(%d|%d|%d), class_oid(%d|%d|%d), LOCK(%7s -> %7s)\n",
	       entry_ptr->tran_index,
	       entry_ptr->res_head->oid.volid,
	       entry_ptr->res_head->oid.pageid,
	       entry_ptr->res_head->oid.slotid,
	       entry_ptr->res_head->class_oid.volid,
	       entry_ptr->res_head->class_oid.pageid,
	       entry_ptr->res_head->class_oid.slotid,
	       LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
	       (entry_ptr->granted_mode == U_LOCK &&
		logtb_find_current_isolation () == TRAN_SERIALIZABLE) ?
	       LOCK_TO_LOCKMODE_STRING (S_LOCK) :
	       (logtb_find_current_isolation () ==
		TRAN_REP_CLASS_REP_INSTANCE ?
		LOCK_TO_LOCKMODE_STRING (NS_LOCK) :
		LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode)));
    }
#endif /* LK_DUMP */

  /* check and update granted mode of the lock entry */
  if (check->granted_mode == U_LOCK)
    {
      switch (logtb_find_current_isolation (thread_p))
	{
	case TRAN_SERIALIZABLE:
	  check->granted_mode = S_LOCK;
	  break;

	case TRAN_REP_CLASS_REP_INSTANCE:
	  check->granted_mode = NS_LOCK;
	  break;

	default:
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  return NO_ERROR;
	}
    }
  else
    {
      MUTEX_UNLOCK (res_ptr->res_mutex);
      return NO_ERROR;
    }

  /* change total_holders_mode */
  total_mode = NULL_LOCK;
  for (i = res_ptr->holder; i != NULL; i = i->next)
    {
      assert (i->granted_mode >= NULL_LOCK && total_mode >= NULL_LOCK);
      total_mode = lock_Conv[i->granted_mode][total_mode];
      assert (total_mode != NA_LOCK);

      assert (i->blocked_mode >= NULL_LOCK && total_mode >= NULL_LOCK);
      total_mode = lock_Conv[i->blocked_mode][total_mode];
      assert (total_mode != NA_LOCK);
    }
  res_ptr->total_holders_mode = total_mode;

  /* grant the blocked holders and the blocked waiters */
  lock_grant_blocked_holder (thread_p, res_ptr);
  (void) lock_grant_blocked_waiter (thread_p, res_ptr);

  MUTEX_UNLOCK (res_ptr->res_mutex);

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_demote_shared_class_lock -  Demote one shared class lock
 *
 * return:
 *
 *   tran_index(in):
 *   class_oid(in):
 *
 * Note:This function finds the lock entry whose lock object id is same
 *     with the given class_oid in the transaction lock hold list. And then,
 *     demote the class lock if the class lock is shared mode.
 */
static void
lock_demote_shared_class_lock (THREAD_ENTRY * thread_p, int tran_index,
			       const OID * class_oid)
{
  LK_ENTRY *entry_ptr;

  /* The caller is not holding any mutex */

  /* demote only one class lock */
  entry_ptr = lock_find_class_entry (tran_index, class_oid);
  if (entry_ptr != (LK_ENTRY *) NULL)
    {
      enum
      { SKIP, DEMOTE, DECREMENT_COUNT };
      int demote = SKIP;

      /* I think there's no need to acquire the mutex here. */
      if (entry_ptr->history)
	{
	  LK_ACQUISITION_HISTORY *prev, *last, *p;

	  last = entry_ptr->recent;
	  prev = last->prev;

	  if ((last->req_mode == S_LOCK || last->req_mode == SIX_LOCK)
	      && (entry_ptr->granted_mode == S_LOCK
		  || entry_ptr->granted_mode == SIX_LOCK))
	    {
	      p = entry_ptr->history;
	      while (p != last)
		{
		  if (p->req_mode == S_LOCK || p->req_mode == SIX_LOCK)
		    {
		      break;
		    }
		  p = p->next;
		}

	      if (p != last)
		{
		  /* do not demote shared class lock */
		}
	      else
		{
		  demote = DEMOTE;
		}
	    }
	  else if (entry_ptr->granted_mode == IS_LOCK
		   && last->req_mode == IS_LOCK && entry_ptr->history != last)
	    {
	      /* has > 1 IS_LOCK */
	      demote = DECREMENT_COUNT;
	    }

	  /* free the last node */
	  if (prev == NULL)
	    {
	      entry_ptr->history = NULL;
	      entry_ptr->recent = NULL;
	    }
	  else
	    {
	      prev->next = NULL;
	      entry_ptr->recent = prev;
	    }
	  free_and_init (last);
	}
      else
	{
	  if (entry_ptr->granted_mode == S_LOCK
	      || entry_ptr->granted_mode == SIX_LOCK)
	    {
	      demote = DEMOTE;
	    }
	}

      if (demote == DEMOTE)
	{
	  (void) lock_internal_demote_shared_class_lock (thread_p, entry_ptr);
	}
      else if (demote == DECREMENT_COUNT
	       || entry_ptr->granted_mode == S_LOCK
	       || entry_ptr->granted_mode == SIX_LOCK)
	{
	  entry_ptr->count--;
	}

    }
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_demote_all_shared_class_locks - Demote all shared class locks
 *
 * return:
 *
 *   tran_index(in):
 *
 * Note:This function demotes all shared class locks that are held
 *     by the given transaction.
 */
static void
lock_demote_all_shared_class_locks (THREAD_ENTRY * thread_p, int tran_index)
{
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *curr, *next;

  /* When this function is called, only one thread is executing
   * for the transaction. (transaction : thread = 1 : 1)
   * Therefore, there is no need to hold tran_lock->hold_mutex.
   */

  tran_lock = &lk_Gl.tran_lock_table[tran_index];

  /* 1. demote general class locks */
  curr = tran_lock->class_hold_list;
  while (curr != (LK_ENTRY *) NULL)
    {
      next = curr->tran_next;
      if (curr->granted_mode == S_LOCK || curr->granted_mode == SIX_LOCK)
	{
	  (void) lock_internal_demote_shared_class_lock (thread_p, curr);
	}
      curr = next;
    }

  /* 2. demote root class lock */
  curr = tran_lock->root_class_hold;
  if (curr != (LK_ENTRY *) NULL)
    {
      if (curr->granted_mode == S_LOCK || curr->granted_mode == SIX_LOCK)
	{
	  (void) lock_internal_demote_shared_class_lock (thread_p, curr);
	}
    }
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_internal_demote_shared_inst_lock -
 *
 * return:
 *
 *   entry_ptr(in):
 *
 * Note:demote S_LOCK -> NS_LOCK for instance in RR isolation level.
 */
static int
lock_internal_demote_shared_inst_lock (THREAD_ENTRY * thread_p,
				       LK_ENTRY * entry_ptr)
{
  LK_RES *res_ptr;		/* lock resource entry pointer */
  LK_ENTRY *check, *i;		/* lock entry pointer */
  LOCK total_mode;
  int rv;

  /* The caller is not holding any mutex */

  res_ptr = entry_ptr->res_head;
  MUTEX_LOCK (rv, res_ptr->res_mutex);

  /* find the given lock entry in the holder list */
  for (check = res_ptr->holder; check != NULL; check = check->next)
    {
      if (check == entry_ptr)
	{
	  break;
	}
    }
  if (check == (LK_ENTRY *) NULL)
    {				/* not found */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LK_NOTFOUND_IN_LOCK_HOLDER_LIST, 5,
	      LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
	      entry_ptr->tran_index,
	      res_ptr->oid.volid, res_ptr->oid.pageid, res_ptr->oid.slotid);
      MUTEX_UNLOCK (res_ptr->res_mutex);
      return ER_LK_NOTFOUND_IN_LOCK_HOLDER_LIST;
    }

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 1)
    {
      fprintf (stderr, "LK_DUMP::lk_internal_demote_shared_inst_lock()\n"
	       "  tran(%2d) : oid(%d|%d|%d), class_oid(%d|%d|%d), LOCK(%7s -> %7s)\n",
	       entry_ptr->tran_index,
	       entry_ptr->res_head->oid.volid,
	       entry_ptr->res_head->oid.pageid,
	       entry_ptr->res_head->oid.slotid,
	       entry_ptr->res_head->class_oid.volid,
	       entry_ptr->res_head->class_oid.pageid,
	       entry_ptr->res_head->class_oid.slotid,
	       LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
	       LOCK_TO_LOCKMODE_STRING (NS_LOCK));
    }
#endif /* LK_DUMP */

  /* check and update granted mode of the lock entry */
  if (check->granted_mode == S_LOCK)
    {
      switch (logtb_find_current_isolation (thread_p))
	{
	case TRAN_REP_CLASS_REP_INSTANCE:
	  check->granted_mode = NS_LOCK;
	  break;

	default:
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  return NO_ERROR;
	}
    }
  else
    {
      MUTEX_UNLOCK (res_ptr->res_mutex);
      return NO_ERROR;
    }

  /* change total_holders_mode */
  total_mode = NULL_LOCK;
  for (i = res_ptr->holder; i != NULL; i = i->next)
    {
      assert (i->granted_mode >= NULL_LOCK && total_mode >= NULL_LOCK);
      total_mode = lock_Conv[i->granted_mode][total_mode];
      assert (total_mode != NA_LOCK);

      assert (i->blocked_mode >= NULL_LOCK && total_mode >= NULL_LOCK);
      total_mode = lock_Conv[i->blocked_mode][total_mode];
      assert (total_mode != NA_LOCK);
    }
  res_ptr->total_holders_mode = total_mode;

  /* grant the blocked holders and the blocked waiters */
  lock_grant_blocked_holder (thread_p, res_ptr);
  (void) lock_grant_blocked_waiter (thread_p, res_ptr);

  MUTEX_UNLOCK (res_ptr->res_mutex);

  return NO_ERROR;
}

/*
 * lock_demote_shared_inst_locks -
 *
 * return:
 *
 *   tran_index(in):
 *   scanid_bit(in):
 */
static void
lock_demote_shared_inst_locks (THREAD_ENTRY * thread_p, int tran_index,
			       int scanid_bit)
{
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *curr, *next;
  int x = 0;
  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  for (curr = tran_lock->inst_hold_list; curr != NULL;)
    {
      next = curr->tran_next;
      if (IS_SCANID_BIT_SET (curr->scanid_bitset, scanid_bit))
	{
	  if (curr->granted_mode == S_LOCK)
	    {
	      (void) lock_internal_demote_shared_inst_lock (thread_p, curr);
	    }
	}
      else
	{
	  x = lock_find_scanid_bit (curr->scanid_bitset);
	  if (x >= 0 && scanid_bit >= 0 && scanid_bit > x)
	    {
	      break;
	    }
	}
      curr = next;
    }
}
#endif /* SERVER_MODE */

/*
 * lk_unlock_shared_class_lock: Unlock one shared class lock
 *
 * return :
 *
 *   tran_index(in):
 *   class_oid(in):
 *
 * Note:
 *     This function finds the lock entry whose lock object id is same
 *     with the given class_oid in the transaction lock hold list. And then,
 *     unlock the class lock if the class lock is shared mode.
 *     If the class lock mode is IS_LOCK or S_LOCK, unlock it,
 *     If the class lock mode is SIX_LOCK, demote it to IX_LOCK.
 */
#if defined(SERVER_MODE)
static void
lock_unlock_shared_class_lock (THREAD_ENTRY * thread_p, int tran_index,
			       const OID * class_oid)
{
  LK_ENTRY *entry_ptr;

  entry_ptr = lock_find_class_entry (tran_index, class_oid);
  if (entry_ptr != (LK_ENTRY *) NULL)
    {
      enum
      { SKIP, UNLOCK, DEMOTE };
      int demote_unlock = SKIP;

      /* I think there's no need to acquire the mutex here. */
      if (entry_ptr->history)
	{
	  LK_ACQUISITION_HISTORY *prev, *last, *p;

	  last = entry_ptr->recent;
	  prev = last->prev;

	  if ((last->req_mode == IS_LOCK
	       || last->req_mode == S_LOCK
	       || last->req_mode == SIX_LOCK)
	      && (entry_ptr->granted_mode == IS_LOCK
		  || entry_ptr->granted_mode == S_LOCK
		  || entry_ptr->granted_mode == SIX_LOCK))
	    {
	      p = entry_ptr->history;
	      while (p != last)
		{
		  if (p->req_mode == IS_LOCK || p->req_mode == S_LOCK
		      || p->req_mode == SIX_LOCK)
		    {
		      break;
		    }
		  p = p->next;
		}

	      if (p != last)
		{
		  /* do not demote or unlock shared class lock */
		}
	      else
		{
		  demote_unlock =
		    (entry_ptr->granted_mode == SIX_LOCK) ? DEMOTE : UNLOCK;
		}
	    }

	  /* free the last node */
	  if (prev == NULL)
	    {
	      entry_ptr->history = NULL;
	      entry_ptr->recent = NULL;
	    }
	  else
	    {
	      prev->next = NULL;
	      entry_ptr->recent = prev;
	    }
	  free_and_init (last);
	}
      else
	{
	  if (entry_ptr->granted_mode == IS_LOCK
	      || entry_ptr->granted_mode == S_LOCK)
	    {
	      demote_unlock = UNLOCK;
	    }
	  else if (entry_ptr->granted_mode == SIX_LOCK)
	    {
	      demote_unlock = DEMOTE;
	    }
	}

      if (demote_unlock == UNLOCK)
	{
	  lock_internal_perform_unlock_object (thread_p, entry_ptr, false,
					       true);
	}
      else if (demote_unlock == DEMOTE)
	{
	  (void) lock_internal_demote_shared_class_lock (thread_p, entry_ptr);
	}
      else if (entry_ptr->granted_mode == IS_LOCK
	       || entry_ptr->granted_mode == S_LOCK
	       || entry_ptr->granted_mode == SIX_LOCK)
	{
	  entry_ptr->count--;
	}

    }
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lk_unlock_shared_inst_lock - Unlock one shared instance lock
 *
 * return:
 *
 *   tran_index(in):
 *   inst_oid(in):
 *
 * Note:This function finds the lock entry whose lock object id is same
 *     with the given inst_oid in the transaction lock hold list. And then,
 *     unlock the instance lock if the instance lock is shared lock.
 */
static void
lock_unlock_shared_inst_lock (THREAD_ENTRY * thread_p, int tran_index,
			      const OID * inst_oid)
{
  LK_ENTRY *entry_ptr;

  /* unlock the shared instance lock (NS_LOCK or S_LOCK) */
  entry_ptr = lock_find_inst_entry (tran_index, inst_oid);
  if (entry_ptr != (LK_ENTRY *) NULL)
    {
      if (entry_ptr->granted_mode <= S_LOCK)
	{
	  lock_internal_perform_unlock_object (thread_p, entry_ptr, false,
					       true);
	}
    }
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_remove_all_class_locks - Remove class locks whose lock mode is lower
 *                        than the given lock mode
 *
 * return:
 *
 *   tran_index(in):
 *   lock(in):
 *
 * Note:This function removes class locks whose lock mode is lower than
 *     the given lock mode.
 */
static void
lock_remove_all_class_locks (THREAD_ENTRY * thread_p, int tran_index,
			     LOCK lock)
{
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *curr, *next;

  /* When this function is called, only one thread is executing
   * for the transaction. (transaction : thread = 1 : 1)
   * Therefore, there is no need to hold tran_lock->hold_mutex.
   */

  tran_lock = &lk_Gl.tran_lock_table[tran_index];

  /* remove class locks if given condition is satisfied */
  curr = tran_lock->class_hold_list;
  while (curr != (LK_ENTRY *) NULL)
    {
      next = curr->tran_next;
      if (curr->granted_mode <= lock)
	{
	  lock_internal_perform_unlock_object (thread_p, curr, true, false);
	}
      curr = next;
    }

  /* remove root class lock if given condition is satisfied */
  curr = tran_lock->root_class_hold;
  if (curr != NULL)
    {
      if (curr->granted_mode <= lock)
	{
	  lock_internal_perform_unlock_object (thread_p, curr, true, false);
	}
    }

}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_remove_all_inst_locks - Remove instance locks whose lock mode is lower
 *                        than the given lock mode
 *
 * return:
 *
 *   tran_index(in):
 *   class_oid(in):
 *   lock(in):
 *
 * Note:This function removes instance locks whose lock mode is lower than
 *     the given lock mode.
 */
void
lock_remove_all_inst_locks (THREAD_ENTRY * thread_p, int tran_index,
			    const OID * class_oid, LOCK lock)
{
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *curr, *next;


  tran_lock = &lk_Gl.tran_lock_table[tran_index];

  /* remove instance locks if given condition is satisfied */
  curr = tran_lock->inst_hold_list;
  while (curr != (LK_ENTRY *) NULL)
    {
      next = curr->tran_next;
      if (class_oid == NULL || OID_ISNULL (class_oid)
	  || OID_EQ (&curr->res_head->class_oid, class_oid))
	{
	  if (curr->granted_mode <= lock)
	    {
	      /* found : the same class_oid and interesting lock mode
	       * --> unlock it.
	       */
	      lock_internal_perform_unlock_object (thread_p, curr, true,
						   false);
	    }
	}
      curr = next;
    }

}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_find_scanid_bit -
 *
 * return:
 *
 *   scanid_bit(in):
 */
static int
lock_find_scanid_bit (unsigned char *scanid_bit)
{
  int i;
  for (i = 0; i < PRM_LK_MAX_SCANID_BIT; i++)
    {
      if (IS_SCANID_BIT_SET (scanid_bit, i))
	{
	  return i;
	}
    }
  return -1;
}

/*
 * lock_remove_all_inst_locks_with_scanid -
 *
 * return:
 *
 *   tran_index(in):
 *   scanid_bit(in):
 *   lock(in):
 *
 * Note:
 */
static void
lock_remove_all_inst_locks_with_scanid (THREAD_ENTRY * thread_p,
					int tran_index, int scanid_bit,
					LOCK lock)
{
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *curr, *next;
  bool unlock;
  int x;


  tran_lock = &lk_Gl.tran_lock_table[tran_index];

  /* remove instance locks if given condition is satisfied */
  curr = tran_lock->inst_hold_list;
  while (curr != (LK_ENTRY *) NULL)
    {
      next = curr->tran_next;
      if (IS_SCANID_BIT_SET (curr->scanid_bitset, scanid_bit)
	  && curr->granted_mode <= lock)
	{
	  unlock = false;

	  /* I think there's no need to acquire the mutex here. */

	  if (curr->history)
	    {
	      LK_ACQUISITION_HISTORY *prev, *last, *p;

	      last = curr->recent;
	      prev = last->prev;
	      if ((last->req_mode == S_LOCK || last->req_mode == NS_LOCK)
		  && (curr->granted_mode == S_LOCK
		      || curr->granted_mode == NS_LOCK))
		{
		  p = curr->history;
		  while (p != last)
		    {
		      if (p->req_mode == S_LOCK || p->req_mode == NS_LOCK)
			{
			  break;
			}
		      p = p->next;
		    }

		  if (p != last)
		    {
		      /* do not unlock shared instance lock */
		    }
		  else
		    {
		      unlock = true;
		    }
		}

	      /* free the last node */
	      if (prev == NULL)
		{
		  curr->history = NULL;
		  curr->recent = NULL;
		}
	      else
		{
		  prev->next = NULL;
		  curr->recent = prev;
		}
	      free_and_init (last);
	    }
	  else
	    {
	      if (curr->granted_mode == S_LOCK
		  || curr->granted_mode == NS_LOCK)
		{
		  unlock = true;
		}
	    }

	  if (unlock)
	    {
	      lock_internal_perform_unlock_object (thread_p, curr, false,
						   false);
	    }
	  else if (curr->granted_mode == S_LOCK
		   || curr->granted_mode == NS_LOCK)
	    {
	      curr->count--;
	    }

	}
      else
	{
	  x = lock_find_scanid_bit (curr->scanid_bitset);
	  if (x >= 0 && scanid_bit >= 0 && scanid_bit > x)
	    {
	      break;
	    }
	}
      curr = next;
    }
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: non two pahse locks
 *
 *   - lk_remove_non2pl()
 *   - lk_update_non2pl_list()
 */

#if defined(SERVER_MODE)
/*
 * lock_remove_non2pl -
 *
 * return:
 *
 *   non2pl(in):
 *   tran_index(in):
 */
static void
lock_remove_non2pl (LK_ENTRY * non2pl, int tran_index)
{
  LK_RES *res_ptr;
  LK_ENTRY *prev, *curr;
  int rv;

  /* The given non2pl entry has already been removed from
   * the transaction non2pl list.
   * Therefore, This function removes the given non2pl entry
   * from the resource non2pl list and then frees the entry.
   */

  res_ptr = non2pl->res_head;
  MUTEX_LOCK (rv, res_ptr->res_mutex);

  /* find the given non2pl in non2pl list of resource entry */
  prev = (LK_ENTRY *) NULL;
  curr = res_ptr->non2pl;
  while (curr != (LK_ENTRY *) NULL)
    {
      if (curr->tran_index == tran_index)
	{
	  break;
	}
      prev = curr;
      curr = curr->next;
    }
  if (curr == (LK_ENTRY *) NULL)
    {				/* not found */
      MUTEX_UNLOCK (res_ptr->res_mutex);
      return;
    }

  /* found : remove it */
  if (prev == (LK_ENTRY *) NULL)
    {
      res_ptr->non2pl = curr->next;
    }
  else
    {
      prev->next = curr->next;
    }
  /* (void)lk_delete_from_tran_non2pl_list(curr); */

  /* free the lock entry */
  lock_free_entry (curr);

  if (res_ptr->holder == NULL && res_ptr->waiter == NULL
      && res_ptr->non2pl == NULL)
    {
      (void) lock_dealloc_resource (res_ptr);
    }
  else
    {
      MUTEX_UNLOCK (res_ptr->res_mutex);
    }

}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_update_non2pl_list -
 *
 * return:
 *
 *   res_ptr(in):
 *   tran_index(in):
 *   lock(in):
 */
static void
lock_update_non2pl_list (LK_RES * res_ptr, int tran_index, LOCK lock)
{
  LK_ENTRY *prev;
  LK_ENTRY *curr;
  LK_ENTRY *next;
  LK_TRAN_LOCK *tran_lock;
  int rv;
  int compat;

  /* The caller is holding a resource mutex */

  prev = (LK_ENTRY *) NULL;
  curr = res_ptr->non2pl;
  while (curr != (LK_ENTRY *) NULL)
    {
      if (curr->tran_index == tran_index)
	{			/* same transaction */
	  /* remove current non2pl entry */
	  next = curr->next;
	  if (prev == (LK_ENTRY *) NULL)
	    {
	      res_ptr->non2pl = curr->next;
	    }
	  else
	    {
	      prev->next = curr->next;
	    }
	  (void) lock_delete_from_tran_non2pl_list (curr);
	  lock_free_entry (curr);
	  curr = next;
	}
      else
	{			/* different transaction */
	  if (curr->granted_mode != INCON_NON_TWO_PHASE_LOCK)
	    {
	      /* The transaction with the released lock must decache the lock
	       * object since an incompatible locks has been acquired.
	       * This implies that the transaction with the released lock
	       * may not be serializable (repeatable read consistent) any longer.
	       */
	      assert (lock >= NULL_LOCK && curr->granted_mode >= NULL_LOCK);
	      compat = lock_Comp[lock][curr->granted_mode];
	      assert (compat != DB_NA);

	      if (compat == false)
		{
		  curr->granted_mode = INCON_NON_TWO_PHASE_LOCK;
		  tran_lock = &lk_Gl.tran_lock_table[curr->tran_index];
		  MUTEX_LOCK (rv, tran_lock->non2pl_mutex);
		  tran_lock->num_incons_non2pl += 1;
		  MUTEX_UNLOCK (tran_lock->non2pl_mutex);
		}
	    }
	  prev = curr;
	  curr = curr->next;
	}
    }

}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: local deadlock detection and resolution
 *
 *   - lk_add_WFG_edge()
 */

#if defined(SERVER_MODE)
/*
 * lock_add_WFG_edge -
 *
 * return: error code
 *
 *   from_tran_index(in): waiting transaction index
 *   to_tran_index(in): waited transaction index
 *   holder_flag(in): true(if to_tran_index is Holder), false(otherwise)
 *   edge_wait_stime(in):
 *
 * Note:add an edge to WFG which represents that
 *     'from_tran_index' transaction waits for 'to_tran_index' transaction.
 */
static int
lock_add_WFG_edge (int from_tran_index, int to_tran_index,
		   int holder_flag, time_t edge_wait_stime)
{
  int prev, curr;
  int i;
  int alloc_idx;
  char *temp_ptr;

  /* check if the transactions has been selected as victims */
  /* Note that the transactions might be old deadlock victims */
  if (lk_Gl.TWFG_node[from_tran_index].DL_victim == true
      || lk_Gl.TWFG_node[to_tran_index].DL_victim == true)
    {
      return NO_ERROR;
    }

  /* increment global edge sequence number */
  lk_Gl.global_edge_seq_num++;

  if (lk_Gl.TWFG_node[from_tran_index].checked_by_deadlock_detector == false)
    {
      /* a new transaction started */
      if (lk_Gl.TWFG_node[from_tran_index].first_edge != -1)
	{
	  prev = -1;
	  curr = lk_Gl.TWFG_node[from_tran_index].first_edge;
	  while (curr != -1)
	    {
	      prev = curr;
	      curr = lk_Gl.TWFG_edge[curr].next;
	    }
	  lk_Gl.TWFG_edge[prev].next = lk_Gl.TWFG_free_edge_idx;
	  lk_Gl.TWFG_free_edge_idx =
	    lk_Gl.TWFG_node[from_tran_index].first_edge;
	  lk_Gl.TWFG_node[from_tran_index].first_edge = -1;
	}
      lk_Gl.TWFG_node[from_tran_index].checked_by_deadlock_detector = true;
      lk_Gl.TWFG_node[from_tran_index].tran_edge_seq_num =
	lk_Gl.global_edge_seq_num;
    }

  if (lk_Gl.TWFG_node[to_tran_index].checked_by_deadlock_detector == false)
    {
      /* a new transaction started */
      if (lk_Gl.TWFG_node[to_tran_index].first_edge != -1)
	{
	  prev = -1;
	  curr = lk_Gl.TWFG_node[to_tran_index].first_edge;
	  while (curr != -1)
	    {
	      prev = curr;
	      curr = lk_Gl.TWFG_edge[curr].next;
	    }
	  lk_Gl.TWFG_edge[prev].next = lk_Gl.TWFG_free_edge_idx;
	  lk_Gl.TWFG_free_edge_idx =
	    lk_Gl.TWFG_node[to_tran_index].first_edge;
	  lk_Gl.TWFG_node[to_tran_index].first_edge = -1;
	}
      lk_Gl.TWFG_node[to_tran_index].checked_by_deadlock_detector = true;
      lk_Gl.TWFG_node[to_tran_index].tran_edge_seq_num =
	lk_Gl.global_edge_seq_num;
    }

  /* NOTE the following description..
   * According to the above code, whenever it is identified that
   * a transaction has been terminated during deadlock detection,
   * the transaction is checked again as a new transaction. And,
   * the current edge is based on the current active transactions.
   */

  if (lk_Gl.TWFG_free_edge_idx == -1)
    {				/* too many WFG edges */
      if (lk_Gl.max_TWFG_edge == LK_MIN_TWFG_EDGE_COUNT)
	{
	  lk_Gl.max_TWFG_edge = LK_MID_TWFG_EDGE_COUNT;
	  for (i = LK_MIN_TWFG_EDGE_COUNT; i < lk_Gl.max_TWFG_edge; i++)
	    {
	      lk_Gl.TWFG_edge[i].to_tran_index = -1;
	      lk_Gl.TWFG_edge[i].next = (i + 1);
	    }
	  lk_Gl.TWFG_edge[lk_Gl.max_TWFG_edge - 1].next = -1;
	  lk_Gl.TWFG_free_edge_idx = LK_MIN_TWFG_EDGE_COUNT;
	}
      else if (lk_Gl.max_TWFG_edge == LK_MID_TWFG_EDGE_COUNT)
	{
	  temp_ptr = (char *) lk_Gl.TWFG_edge;
	  lk_Gl.max_TWFG_edge = LK_MAX_TWFG_EDGE_COUNT;
	  lk_Gl.TWFG_edge =
	    (LK_WFG_EDGE *) malloc (SIZEOF_LK_WFG_EDGE * lk_Gl.max_TWFG_edge);
	  if (lk_Gl.TWFG_edge == (LK_WFG_EDGE *) NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      (SIZEOF_LK_WFG_EDGE * lk_Gl.max_TWFG_edge));
	      return ER_OUT_OF_VIRTUAL_MEMORY;	/* no method */
	    }
	  (void) memcpy ((char *) lk_Gl.TWFG_edge, temp_ptr,
			 (SIZEOF_LK_WFG_EDGE * LK_MID_TWFG_EDGE_COUNT));
	  for (i = LK_MID_TWFG_EDGE_COUNT; i < lk_Gl.max_TWFG_edge; i++)
	    {
	      lk_Gl.TWFG_edge[i].to_tran_index = -1;
	      lk_Gl.TWFG_edge[i].next = (i + 1);
	    }
	  lk_Gl.TWFG_edge[lk_Gl.max_TWFG_edge - 1].next = -1;
	  lk_Gl.TWFG_free_edge_idx = LK_MID_TWFG_EDGE_COUNT;
	}
      else
	{
#if defined(CUBRID_DEBUG)
	  er_log_debug (ARG_FILE_LINE, "So many TWFG edges are used..\n");
#endif /* CUBRID_DEBUG */
	  return ER_FAILED;	/* no method */
	}
    }

  /* allocate free WFG edge */
  alloc_idx = lk_Gl.TWFG_free_edge_idx;
  lk_Gl.TWFG_free_edge_idx = lk_Gl.TWFG_edge[alloc_idx].next;

  /* set WFG edge with given information */
  lk_Gl.TWFG_edge[alloc_idx].to_tran_index = to_tran_index;
  lk_Gl.TWFG_edge[alloc_idx].edge_seq_num = lk_Gl.global_edge_seq_num;
  lk_Gl.TWFG_edge[alloc_idx].holder_flag = holder_flag;
  lk_Gl.TWFG_edge[alloc_idx].edge_wait_stime = edge_wait_stime;

  /* connect the WFG edge into WFG */
  lk_Gl.TWFG_edge[alloc_idx].next =
    lk_Gl.TWFG_node[from_tran_index].first_edge;
  lk_Gl.TWFG_node[from_tran_index].first_edge = alloc_idx;

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_select_deadlock_victim -
 *
 * return:
 *
 *   s(in):
 *   t(in):
 *
 * Note:
 */
static void
lock_select_deadlock_victim (THREAD_ENTRY * thread_p, int s, int t)
{
  LK_WFG_NODE *TWFG_node;
  LK_WFG_EDGE *TWFG_edge;
  TRANID tranid;
  int can_timeout;
  int u, v, w;
  bool false_dd_cycle = false;
  bool lock_holder_found = false;
  bool inact_trans_found = false;
  int tot_WFG_nodes;
#if defined(CUBRID_DEBUG)
  int num_WFG_nodes;
  int WFG_nidx;
  int tran_index_area[20];
  int *tran_index_set = &tran_index_area[0];
#endif

  /* simple notation */
  TWFG_node = lk_Gl.TWFG_node;
  TWFG_edge = lk_Gl.TWFG_edge;

  /*
   * check if current deadlock cycle is false deadlock cycle
   */
  tot_WFG_nodes = 0;
  if (TWFG_node[t].current == -1)
    {
      /* old WFG edge : remove it */
      TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
      false_dd_cycle = true;
    }
  else
    {
      if (TWFG_node[t].checked_by_deadlock_detector == false
	  || TWFG_node[t].thrd_wait_stime == 0
	  || (TWFG_node[t].thrd_wait_stime >
	      TWFG_edge[TWFG_node[t].current].edge_wait_stime))
	{
	  /* old transaction, not lockwait state, or incorrect WFG edge */
	  /* remove all outgoing edges */
	  TWFG_node[t].first_edge = -1;
	  TWFG_node[t].current = -1;
	  /* remove incoming edge */
	  TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	  false_dd_cycle = true;
	}
      else
	{
	  if (TWFG_edge[TWFG_node[s].current].edge_seq_num
	      < TWFG_node[t].tran_edge_seq_num)
	    {
	      /* old WFG edge : remove it */
	      TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	      false_dd_cycle = true;
	    }
	  else
	    {
	      tot_WFG_nodes += 1;
	    }
	}
    }
  for (v = s; v != t;)
    {
      u = lk_Gl.TWFG_node[v].ancestor;
      if (TWFG_node[v].current == -1)
	{
	  /* old WFG edge : remove it */
	  TWFG_edge[TWFG_node[u].current].to_tran_index = -2;
	  false_dd_cycle = true;
	}
      else
	{
	  if (TWFG_node[v].checked_by_deadlock_detector == false
	      || TWFG_node[v].thrd_wait_stime == 0
	      || (TWFG_node[v].thrd_wait_stime >
		  TWFG_edge[TWFG_node[v].current].edge_wait_stime))
	    {
	      /* old transaction, not lockwait state, or incorrect WFG edge */
	      /* remove all outgoing edges */
	      TWFG_node[v].first_edge = -1;
	      TWFG_node[v].current = -1;
	      /* remove incoming edge */
	      TWFG_edge[TWFG_node[u].current].to_tran_index = -2;
	      false_dd_cycle = true;
	    }
	  else
	    {
	      if (TWFG_edge[TWFG_node[u].current].edge_seq_num
		  < TWFG_node[v].tran_edge_seq_num)
		{
		  /* old WFG edge : remove it */
		  TWFG_edge[TWFG_node[u].current].to_tran_index = -2;
		  false_dd_cycle = true;
		}
	      else
		{
		  tot_WFG_nodes += 1;
		}
	    }
	}
      v = u;
    }

  if (false_dd_cycle == true)
    {				/* clear deadlock cycle */
      for (v = s; v != t;)
	{
	  w = TWFG_node[v].ancestor;
	  TWFG_node[v].ancestor = -1;
	  v = w;
	}
      return;
    }

  /*
     Victim Selection Strategy
     1) Must be lock holder.
     2) Must be active transaction.
     3) Prefer a transaction with a closer timeout.
     4) Prefer the youngest transaction.
   */
#if defined(CUBRID_DEBUG)
  num_WFG_nodes = tot_WFG_nodes;
  if (num_WFG_nodes > 20)
    {
      tran_index_set = (int *) malloc (sizeof (int) * num_WFG_nodes);
      if (tran_index_set == NULL)
	{
	  num_WFG_nodes = 20;
	  tran_index_set = &tran_index_area[0];
	}
    }
  WFG_nidx = 0;

  if (TWFG_node[t].checked_by_deadlock_detector == false)
    {
      er_log_debug (ARG_FILE_LINE,
		    "transaction(index=%d) is old in deadlock cycle\n", t);
    }
#endif /* CUBRID_DEBUG */
  if (TWFG_edge[TWFG_node[s].current].holder_flag)
    {
      tranid = logtb_find_tranid (t);
      if (logtb_is_active (thread_p, tranid) == false)
	{
	  victims[victim_count].tran_index = NULL_TRAN_INDEX;
	  inact_trans_found = true;
#if defined(CUBRID_DEBUG)
	  er_log_debug (ARG_FILE_LINE,
			"Inactive transaction is found in a deadlock cycle\n"
			"(tran_index=%d, tranid=%d, state=%s)\n",
			t, tranid, log_state_string (logtb_find_state (t)));
	  tran_index_set[WFG_nidx] = t;
	  WFG_nidx += 1;
#endif /* CUBRID_DEBUG */
	}
      else
	{
	  victims[victim_count].tran_index = t;
	  victims[victim_count].tranid = tranid;
	  victims[victim_count].can_timeout =
	    LK_CAN_TIMEOUT (logtb_find_wait_secs (t));
	  lock_holder_found = true;
	}
    }
  else
    {
      victims[victim_count].tran_index = NULL_TRAN_INDEX;
#if defined(CUBRID_DEBUG)
      tran_index_set[WFG_nidx] = t;
      WFG_nidx += 1;
#endif
    }

  for (v = s; v != t;)
    {
#if defined(CUBRID_DEBUG)
      if (TWFG_node[v].checked_by_deadlock_detector == false)
	{
	  er_log_debug (ARG_FILE_LINE,
			"transaction(index=%d) is old in deadlock cycle\n",
			v);
	}
#endif /* CUBRID_DEBUG */
      if (TWFG_node[v].candidate == true)
	{
	  tranid = logtb_find_tranid (v);
	  if (logtb_is_active (thread_p, tranid) == false)
	    {
	      inact_trans_found = true;
#if defined(CUBRID_DEBUG)
	      er_log_debug (ARG_FILE_LINE,
			    "Inactive transaction is found in a deadlock cycle\n"
			    "(tran_index=%d, tranid=%d, state=%s)\n",
			    v, tranid,
			    log_state_string (logtb_find_state (v)));
	      tran_index_set[WFG_nidx] = v;
	      WFG_nidx += 1;
#endif /* CUBRID_DEBUG */
	    }
	  else
	    {
	      lock_holder_found = true;
	      can_timeout = LK_CAN_TIMEOUT (logtb_find_wait_secs (v));
	      if (victims[victim_count].tran_index == NULL_TRAN_INDEX
		  || (victims[victim_count].can_timeout == false
		      && can_timeout == true)
		  || (victims[victim_count].can_timeout == can_timeout
		      && LK_ISYOUNGER (tranid, victims[victim_count].tranid)))
		{
		  victims[victim_count].tran_index = v;
		  victims[victim_count].tranid = tranid;
		  victims[victim_count].can_timeout = can_timeout;
		}
	    }
	}
#if defined(CUBRID_DEBUG)
      else
	{			/* TWFG_node[v].candidate == false */
	  tran_index_set[WFG_nidx] = v;
	  WFG_nidx += 1;
	}
#endif
      w = TWFG_node[v].ancestor;
      TWFG_node[v].ancestor = -1;
      v = w;
    }

  if (victims[victim_count].tran_index != NULL_TRAN_INDEX)
    {
#if defined(CUBRID_DEBUG)
      if (TWFG_node[victims[victim_count].tran_index].
	  checked_by_deadlock_detector == false)
	{
	  er_log_debug (ARG_FILE_LINE,
			"victim(index=%d) is old in deadlock cycle\n",
			victims[victim_count].tran_index);
	}
#endif /* CUBRID_DEBUG */
      TWFG_node[victims[victim_count].tran_index].current = -1;
      victim_count++;
    }
  else
    {
#if defined(CUBRID_DEBUG)
      er_log_debug (ARG_FILE_LINE, "No victim in deadlock cycle....\n");
      if (lock_holder_foumnd == false)
	{
	  er_log_debug (ARG_FILE_LINE,
			"Any Lock holder is not found in deadlock cycle.\n");
	}
      if (inact_trans_found == true)
	{
	  er_log_debug (ARG_FILE_LINE,
			"Inactive transactions are found in deadlock cycle.\n");
	}
      er_log_debug (ARG_FILE_LINE,
		    "total_edges=%d, free_edge_idx=%d, global_edge_seq=%d\n",
		    lk_Gl.max_TWFG_edge, lk_Gl.TWFG_free_edge_idx,
		    lk_Gl.global_edge_seq_num);
      er_log_debug (ARG_FILE_LINE,
		    "# of WFG nodes in deadlock cycle = %d (%d printed)\n",
		    tot_WFG_nodes, num_WFG_nodes);
      for (WFG_nidx = 0; WFG_nidx < num_WFG_nodes; WFG_nidx++)
	{
	  er_log_debug (ARG_FILE_LINE, "%3d ", tran_index_set[WFG_nidx]);
	  if ((WFG_nidx + 1) == num_WFG_nodes || (WFG_nidx % 10) == 9)
	    {
	      er_log_debug (ARG_FILE_LINE, "\n");
	    }
	}
#endif /* CUBRID_DEBUG */
      /* wait for some short time */
      /* TODO: remove const 1000 */
      thread_sleep (0, 1000);	/* sleep for 1000 micro seconds */
    }
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_dump_deadlock_victims -
 *
 * return:
 */
static void
lock_dump_deadlock_victims (THREAD_ENTRY * thread_p)
{
  int k, count;

  /*
     lk_Gl.dump_level_when_deadlock
     0: Don't print anything about deadlock informatin.
     1: Print only the transaction indexes of deadlock victims.
     2: Number 1 and deadlock solve functions
     3: Number 2 and dump lock tables
     cf) Print only the transactions involved in the cycle and the
     selected victims (not possible at the current time)
   */
  if (lk_Gl.dump_level_when_deadlock == 1)
    {				/* level 1 */
      fprintf (stderr, "..... deadlock victims list begin ......\n");
      fprintf (stderr, "## global_edge_seq_num = %d\n",
	       lk_Gl.global_edge_seq_num);
      fprintf (stderr, "## DL victim count = %d\n", victim_count);
      fprintf (stderr, "## DL victims like followings:\n");
      for (k = 0; k < victim_count; k++)
	{
	  fprintf (stderr, "%3d ", victims[k].tran_index);
	  if ((k % 10) == 9)
	    {
	      fprintf (stderr, "\n");
	    }
	}
      fprintf (stderr, "\n..... deadlock victims list end ......\n\n");
    }
  else
    {				/* level 2 or 3 */
      fprintf (stderr, "..... deadlock victims list begin ......\n");
      fprintf (stderr, "## global_edge_seq_num = %d\n",
	       lk_Gl.global_edge_seq_num);
      fprintf (stderr, "## DL victim count = %d\n", victim_count);
      fprintf (stderr, "## DL victims like followings:\n");
      /* print aborted transactions (deadlock victims) */
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOCK,
				       MSGCAT_LK_DEADLOCK_ABORT_HDR));
      count = 0;
      for (k = 0; k < victim_count; k++)
	{
	  if (!victims[k].can_timeout)
	    {
	      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
					       MSGCAT_SET_LOCK,
					       MSGCAT_LK_DEADLOCK_ABORT),
		       victims[k].tran_index);
	      if ((count % 10) == 9)
		{
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
						   MSGCAT_SET_LOCK,
						   MSGCAT_LK_NEWLINE));
		}
	      count++;
	    }
	}
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOCK, MSGCAT_LK_NEWLINE));
      /* print timeout transactions (deadlock victims) */
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
				       MSGCAT_SET_LOCK,
				       MSGCAT_LK_DEADLOCK_TIMEOUT_HDR));
      count = 0;
      for (k = 0; k < victim_count; k++)
	{
	  if (victims[k].can_timeout)
	    {
	      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
					       MSGCAT_SET_LOCK,
					       MSGCAT_LK_DEADLOCK_TIMEOUT),
		       victims[k].tran_index);
	      if ((count % 10) == 9)
		{
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_CUBRID,
						   MSGCAT_SET_LOCK,
						   MSGCAT_LK_NEWLINE));
		}
	      count++;
	    }
	}
      fprintf (stderr, "\n..... deadlock victims list end ......\n\n");

      /* dump object lock table */
      if (lk_Gl.dump_level_when_deadlock == 3)
	{
	  if (lk_Gl.dump_lock_table_count < 100)
	    {
	      lk_Gl.dump_lock_table_count++;
	    }
	  else
	    {
	      xlock_dump (thread_p, NULL);
	      lk_Gl.dump_lock_table_count = 0;
	    }
	}
    }
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: miscellaneous functions
 *
 *   - lk_lockinfo_compare()
 *   - lk_dump_res()
 *   - lk_consistent_res()
 *   - lk_consistent_tran_lock()
 */

#if defined(SERVER_MODE)
/*
 * lock_compare_lock_info -
 *
 * return:
 *
 *   lockinfo1(in):
 *   lockinfo2(in):
 *
 * Note:compare two OID of lockable objects.
 */
static int
lock_compare_lock_info (const void *lockinfo1, const void *lockinfo2)
{
  const OID *oid1;
  const OID *oid2;

  oid1 = &(((LK_LOCKINFO *) (lockinfo1))->oid);
  oid2 = &(((LK_LOCKINFO *) (lockinfo2))->oid);

  return oid_compare (oid1, oid2);
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_dump_resource - Dump locks acquired on a resource
 *
 * return:
 *
 *   outfp(in): FILE stream where to dump the lock resource entry.
 *   res_ptr(in): pointer to lock resource entry
 *
 * Note:Dump contents of the lock resource entry pointed by res_ptr.
 */
static void
lock_dump_resource (THREAD_ENTRY * thread_p, FILE * outfp, LK_RES * res_ptr)
{
  LK_ENTRY *entry_ptr;
  char *classname;		/* Name of the class */
  int num_holders, num_blocked_holders, num_waiters;
  char time_val[64];

  memset (time_val, 0, sizeof (time_val));

  /* dump object identifier */
  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				  MSGCAT_SET_LOCK,
				  MSGCAT_LK_RES_OID), res_ptr->oid.volid,
	   res_ptr->oid.pageid, res_ptr->oid.slotid);

  /* dump object type related information */
  switch (res_ptr->type)
    {
    case LOCK_RESOURCE_ROOT_CLASS:
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_ROOT_CLASS_TYPE));
      break;
    case LOCK_RESOURCE_CLASS:
      classname = heap_get_class_name (thread_p, &res_ptr->oid);
      if (classname == NULL)
	{
	  /* We must stop processing if an interrupt occurs */
	  if (er_errid () == ER_INTERRUPTED)
	    {
	      return;
	    }
	}
      else
	{
	  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
					  MSGCAT_SET_LOCK,
					  MSGCAT_LK_RES_CLASS_TYPE),
		   classname);
	  free_and_init (classname);
	}
      break;
    case LOCK_RESOURCE_INSTANCE:
      classname = heap_get_class_name (thread_p, &res_ptr->class_oid);
      if (classname == NULL)
	{
	  /* We must stop processing if an interrupt occurs */
	  if (er_errid () == ER_INTERRUPTED)
	    {
	      return;
	    }
	}
      else
	{
	  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
					  MSGCAT_SET_LOCK,
					  MSGCAT_LK_RES_INSTANCE_TYPE),
		   res_ptr->class_oid.volid, res_ptr->class_oid.pageid,
		   res_ptr->class_oid.slotid, classname);
	  free_and_init (classname);
	}
      break;
    default:
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_UNKNOWN_TYPE));
    }

  /* dump total modes of holders and waiters */
  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				  MSGCAT_SET_LOCK,
				  MSGCAT_LK_RES_TOTAL_MODE),
	   LOCK_TO_LOCKMODE_STRING (res_ptr->total_holders_mode),
	   LOCK_TO_LOCKMODE_STRING (res_ptr->total_waiters_mode));

  num_holders = num_blocked_holders = 0;
  if (res_ptr->holder != (LK_ENTRY *) NULL)
    {
      entry_ptr = res_ptr->holder;
      while (entry_ptr != NULL)
	{
	  if (entry_ptr->blocked_mode == NULL_LOCK)
	    {
	      num_holders++;
	    }
	  else
	    {
	      num_blocked_holders++;
	    }
	  entry_ptr = entry_ptr->next;
	}
    }
  num_waiters = 0;
  if (res_ptr->waiter != (LK_ENTRY *) NULL)
    {
      entry_ptr = res_ptr->waiter;
      while (entry_ptr != (LK_ENTRY *) NULL)
	{
	  num_waiters++;
	  entry_ptr = entry_ptr->next;
	}
    }

  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				  MSGCAT_SET_LOCK,
				  MSGCAT_LK_RES_LOCK_COUNT), num_holders,
	   num_blocked_holders, num_waiters);

  /* dump holders */
  if (num_holders > 0)
    {
      /* dump non blocked holders */
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_NON_BLOCKED_HOLDER_HEAD));
      entry_ptr = res_ptr->holder;
      while (entry_ptr != (LK_ENTRY *) NULL)
	{
	  if (entry_ptr->blocked_mode == NULL_LOCK)
	    {
	      if (res_ptr->type == LOCK_RESOURCE_INSTANCE)
		{
		  fprintf (outfp,
			   msgcat_message (MSGCAT_CATALOG_CUBRID,
					   MSGCAT_SET_LOCK,
					   MSGCAT_LK_RES_NON_BLOCKED_HOLDER_ENTRY),
			   "", entry_ptr->tran_index,
			   LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
			   entry_ptr->count);
		}
	      else
		{
		  fprintf (outfp,
			   msgcat_message (MSGCAT_CATALOG_CUBRID,
					   MSGCAT_SET_LOCK,
					   MSGCAT_LK_RES_NON_BLOCKED_HOLDER_ENTRY_WITH_GRANULE),
			   "", entry_ptr->tran_index,
			   LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
			   entry_ptr->count, entry_ptr->ngranules);
		}
	    }
	  entry_ptr = entry_ptr->next;
	}
    }

  if (num_blocked_holders > 0)
    {
      /* dump blocked holders */
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_BLOCKED_HOLDER_HEAD));
      entry_ptr = res_ptr->holder;
      while (entry_ptr != (LK_ENTRY *) NULL)
	{
	  if (entry_ptr->blocked_mode != NULL_LOCK)
	    {
	      time_t stime =
		(time_t) (entry_ptr->thrd_entry->lockwait_stime / 1000);
#if defined(WINDOWS)
	      strcpy (time_val, ctime (&stime));
#else /* WINDOWS */
	      if (ctime_r (&stime, time_val) == NULL)
		{
		  strcpy (time_val, "???");
		}
#endif /* WINDOWS */
	      if (time_val[strlen (time_val) - 1] == '\n')
		{
		  time_val[strlen (time_val) - 1] = 0;
		}
	      if (res_ptr->type == LOCK_RESOURCE_INSTANCE)
		{
		  fprintf (outfp,
			   msgcat_message (MSGCAT_CATALOG_CUBRID,
					   MSGCAT_SET_LOCK,
					   MSGCAT_LK_RES_BLOCKED_HOLDER_ENTRY),
			   "", entry_ptr->tran_index,
			   LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
			   entry_ptr->count, "",
			   LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
			   "", time_val, "",
			   entry_ptr->thrd_entry->lockwait_nsecs);
		}
	      else
		{
		  fprintf (outfp,
			   msgcat_message (MSGCAT_CATALOG_CUBRID,
					   MSGCAT_SET_LOCK,
					   MSGCAT_LK_RES_BLOCKED_HOLDER_ENTRY_WITH_GRANULE),
			   "", entry_ptr->tran_index,
			   LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
			   entry_ptr->count, entry_ptr->ngranules, "",
			   LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
			   "", time_val, "",
			   entry_ptr->thrd_entry->lockwait_nsecs);
		}
	    }
	  entry_ptr = entry_ptr->next;
	}
    }

  /* dump blocked waiters */
  if (res_ptr->waiter != (LK_ENTRY *) NULL)
    {
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_BLOCKED_WAITER_HEAD));
      entry_ptr = res_ptr->waiter;
      while (entry_ptr != (LK_ENTRY *) NULL)
	{
	  time_t stime =
	    (time_t) (entry_ptr->thrd_entry->lockwait_stime / 1000);
#if defined(WINDOWS)
	  strcpy (time_val, ctime (&stime));
#else /* WINDOWS */
	  ctime_r (&stime, time_val);
#endif /* WINDOWS */
	  if (time_val[strlen (time_val) - 1] == '\n')
	    {
	      time_val[strlen (time_val) - 1] = 0;
	    }
	  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
					  MSGCAT_SET_LOCK,
					  MSGCAT_LK_RES_BLOCKED_WAITER_ENTRY),
		   "", entry_ptr->tran_index,
		   LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode), "",
		   time_val, "", entry_ptr->thrd_entry->lockwait_nsecs);
	  entry_ptr = entry_ptr->next;
	}
    }

  /* dump non two phase locks */
  if (res_ptr->non2pl != NULL)
    {
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_NON2PL_RELEASED_HEAD));
      entry_ptr = res_ptr->non2pl;
      while (entry_ptr != (LK_ENTRY *) NULL)
	{
	  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
					  MSGCAT_SET_LOCK,
					  MSGCAT_LK_RES_NON2PL_RELEASED_ENTRY),
		   "", entry_ptr->tran_index,
		   ((entry_ptr->granted_mode ==
		     INCON_NON_TWO_PHASE_LOCK) ? "INCON_NON_TWO_PHASE_LOCK" :
		    LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode)));
	  entry_ptr = entry_ptr->next;
	}
    }
  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				  MSGCAT_SET_LOCK, MSGCAT_LK_NEWLINE));

}
#endif /* SERVER_MODE */

#if defined(ENABLE_UNUSED_FUNCTION)
#if defined(SERVER_MODE)
/*
 * lock_check_consistent_resource - Check if the lock resource entry is consistent
 *
 * return: true/false
 *
 *   res_ptr(in):
 *
 * Note:Verify if a lock entry is consistent. At least one waiter must be
 *     waiting on at least one transaction holding a lock, otherwise,
 *     an inconsistent entry exist. Some waiters may be waiting on waiters.
 */
static bool
lock_check_consistent_resource (THREAD_ENTRY * thread_p, LK_RES * res_ptr)
{
  LOCK total_mode;
  LK_ENTRY *i, *j;
  const char *msg_str;
  int rv;

  /* hold resource mutex */
  MUTEX_LOCK (rv, res_ptr->res_mutex);

  /* check total_holders_mode (total mode of lock holders) */
  total_mode = NULL_LOCK;
  for (i = res_ptr->holder; i != NULL; i = i->next)
    {
      assert (i->granted_mode >= NULL_LOCK && total_mode >= NULL_LOCK);
      total_mode = lock_Conv[i->granted_mode][total_mode];
      assert (total_mode != NA_LOCK);

      assert (i->blocked_mode >= NULL_LOCK && total_mode >= NULL_LOCK);
      total_mode = lock_Conv[i->blocked_mode][total_mode];
      assert (total_mode != NA_LOCK);
    }
  if (total_mode != res_ptr->total_holders_mode)
    {
      msg_str = "lk_consistent_res: total mode of holders is inconsistent.";
      goto inconsistent;
    }

  /* check total_waiters_mode (total mode of lock waiters) */
  total_mode = NULL_LOCK;
  for (i = res_ptr->waiter; i != NULL; i = i->next)
    {
      assert (i->blocked_mode >= NULL_LOCK && total_mode >= NULL_LOCK);
      total_mode = lock_Conv[i->blocked_mode][total_mode];
      assert (total_mode != NA_LOCK);
    }
  if (total_mode != res_ptr->total_waiters_mode)
    {
      msg_str = "lk_consistent_res: total mode of waiters is incons.";
      goto inconsistent;
    }

  /* check holders : lock information */
  for (i = res_ptr->holder; i != NULL; i = i->next)
    {
      /* check res_head */
      if (i->res_head != res_ptr)
	{
	  msg_str = "lk_consistent_res: res_head of a holder is incons.";
	  goto inconsistent;
	}
      /* check tran_index */
      for (j = res_ptr->holder; j != NULL && j != i; j = j->next)
	{
	  if (j->tran_index == i->tran_index)
	    {
	      msg_str =
		"lk_consistent_res: tran_index of a holder is incons. (1)";
	      goto inconsistent;
	    }
	}
      for (j = res_ptr->waiter; j != NULL; j = j->next)
	{
	  if (j->tran_index == i->tran_index)
	    {
	      msg_str =
		"lk_consistent_res: tran_index of a holder is incons. (2)";
	      goto inconsistent;
	    }
	}
      for (j = res_ptr->non2pl; j != NULL; j = j->next)
	{
	  if (j->tran_index == i->tran_index)
	    {
	      msg_str =
		"lk_consistent_res: tran_index of a holder is incons. (3)";
	      goto inconsistent;
	    }
	}
      /* check thrd_entry */
      if (i->blocked_mode != NULL_LOCK)
	{
	  if (i->thrd_entry == NULL)
	    {
	      msg_str =
		"lk_consistent_res: thrd_entry of a blocked holder is incons.";
	      goto inconsistent;
	    }
	}
    }

  /* check lock waiters */
  for (i = res_ptr->waiter; i != NULL; i = i->next)
    {
      /* check res_head */
      if (i->res_head != res_ptr)
	{
	  msg_str = "lk_consistent_res: res_head of a waiter is incons.";
	  goto inconsistent;
	}
      /* check tran_index */
      for (j = res_ptr->holder; j != NULL; j = j->next)
	{
	  if (j->tran_index == i->tran_index)
	    {
	      msg_str =
		"lk_consistent_res: tran_index of a waiter is incons. (1)";
	      goto inconsistent;
	    }
	}
      for (j = res_ptr->waiter; j != NULL && j != i; j = j->next)
	{
	  if (j->tran_index == i->tran_index)
	    {
	      msg_str =
		"lk_consistent_res: tran_index of a waiter is incons. (2)";
	      goto inconsistent;
	    }
	}
      for (j = res_ptr->non2pl; j != NULL; j = j->next)
	{
	  if (j->tran_index == i->tran_index)
	    {
	      msg_str =
		"lk_consistent_res: tran_index of a waiter is incons. (3)";
	      goto inconsistent;
	    }
	}
      /* check lock mode */
      if (i->blocked_mode == NULL_LOCK)
	{
	  msg_str = "lk_consistent_res: blocked_mode of a waiter is incons.";
	  goto inconsistent;
	}
      /* check thrd_entry */
      if (i->thrd_entry == NULL)
	{
	  msg_str = "lk_consistent_res: thrd_entry of a waiter is incons.";
	  goto inconsistent;
	}
    }

  /* check non2pl locks */
  for (i = res_ptr->non2pl; i != NULL; i = i->next)
    {
      /* check tran_index */
      for (j = res_ptr->holder; j != NULL; j = j->next)
	{
	  if (j->tran_index == i->tran_index)
	    {
	      msg_str =
		"lk_consistent_res: tran_index of a non2pl is incons. (1)";
	      goto inconsistent;
	    }
	}
      for (j = res_ptr->waiter; j != NULL; j = j->next)
	{
	  if (j->tran_index == i->tran_index)
	    {
	      msg_str =
		"lk_consistent_res: tran_index of a non2pl is incons. (2)";
	      goto inconsistent;
	    }
	}
      for (j = res_ptr->non2pl; j != NULL && j != i; j = j->next)
	{
	  if (j->tran_index == i->tran_index)
	    {
	      msg_str =
		"lk_consistent_res: tran_index of a non2pl is incons. (3)";
	      goto inconsistent;
	    }
	}
    }

  /* consistent */
  MUTEX_UNLOCK (res_ptr->res_mutex);
  return true;

inconsistent:
  lock_dump_resource (thread_p, stdout, res_ptr);
  MUTEX_UNLOCK (res_ptr->res_mutex);
  er_log_debug (ARG_FILE_LINE, msg_str);
  return false;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_check_consistent_tran_lock - Check consistency of transaction lock info entry
 *
 * return: true/false
 *
 *   tran_lock(in): ponter to the transaction lock information entry
 *
 * Note:check if the given transaction lock information enrtry is consistent.
 */
static bool
lock_check_consistent_tran_lock (LK_TRAN_LOCK * tran_lock)
{
  int count;
  LK_ENTRY *i;
  int rv;

  /* hold transaction hold mutex */
  MUTEX_LOCK (rv, tran_lock->hold_mutex);

  /* check held locks */
  /* check inst_hold_count and class_hold_count */
  for (count = 0, i = tran_lock->inst_hold_list; i != NULL; i = i->tran_next)
    {
      count++;
    }
  if (count != tran_lock->inst_hold_count)
    {
      MUTEX_UNLOCK (tran_lock->hold_mutex);
      er_log_debug (ARG_FILE_LINE,
		    "lk_consistent_tran_lock: inst_hold_count incorrect.");
      return false;
    }
  for (count = 0, i = tran_lock->class_hold_list; i != NULL; i = i->tran_next)
    {
      count++;
    }
  if (count != tran_lock->class_hold_count)
    {
      MUTEX_UNLOCK (tran_lock->hold_mutex);
      er_log_debug (ARG_FILE_LINE,
		    "lk_consistent_tran_lock: class_hold_count incorrect.");
      return false;
    }

  /* release transaction hold mutex */
  MUTEX_UNLOCK (tran_lock->hold_mutex);

  /* hold transaction non2pl mutex */
  MUTEX_LOCK (rv, tran_lock->non2pl_mutex);

  /* check non2pl locks */
  /* check num_incons_non2pl */
  for (count = 0, i = tran_lock->non2pl_list; i != NULL; i = i->tran_next)
    {
      if (i->granted_mode == INCON_NON_TWO_PHASE_LOCK)
	{
	  count++;
	}
    }
  if (count != tran_lock->num_incons_non2pl)
    {
      MUTEX_UNLOCK (tran_lock->non2pl_mutex);
      er_log_debug (ARG_FILE_LINE,
		    "lk_consistent_tran_lock: num_incons_non2pl incorrect.");
      return false;
    }

  /* release transactino non2pl mutex */
  MUTEX_UNLOCK (tran_lock->non2pl_mutex);

  /* consistent */
  return true;
}
#endif /* SERVER_MODE */
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * lock_initialize - Initialize the lock manager
 *
 * return: error code
 *
 *   estimate_nobj_locks(in): estimate_nobj_locks(useless)
 *
 * Note:Initialize the lock manager memory structures.
 */
int
lock_initialize (void)
{
#if !defined (SERVER_MODE)
  lk_Standalone_has_xlock = false;
  return NO_ERROR;
#else /* !SERVER_MODE */
  const char *env_value;
  int error_code = NO_ERROR;

  error_code = lock_initialize_tran_lock_table ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = lock_initialize_object_hash_table ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = lock_initialize_object_lock_res_list ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = lock_initialize_object_lock_entry_list ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = lock_initialize_deadlock_detection ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = lock_initialize_scanid_bitmap ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  /* initialize some parameters */
#if defined(CUBRID_DEBUG)
  lk_Gl.verbose_mode = true;
  lk_Gl.dump_level_when_deadlock = 1;
  lk_Gl.dump_lock_table_count = 0;
  lk_Gl.no_victim_case_count = 0;
#else /* !CUBRID_DEBUG */
  env_value = envvar_get ("LK_VERBOSE_SUSPENDED");
  if (env_value != NULL)
    {
      lk_Gl.verbose_mode = (bool) atoi (env_value);
      if (lk_Gl.verbose_mode != false)
	{
	  lk_Gl.verbose_mode = true;
	}
    }
  lk_Gl.dump_level_when_deadlock = 0;
  env_value = envvar_get ("LK_DUMP_LEVEL_WHEN_DEADLOCK");
  if (env_value != NULL)
    {
      lk_Gl.dump_level_when_deadlock = atoi (env_value);
      if (lk_Gl.dump_level_when_deadlock < 0
	  || lk_Gl.dump_level_when_deadlock > 3)
	{
	  lk_Gl.dump_level_when_deadlock = 1;
	}
    }
  lk_Gl.dump_lock_table_count = 0;
  lk_Gl.no_victim_case_count = 0;
#endif /* !CUBRID_DEBUG */

#if defined(LK_DUMP)
  lk_Gl.dump_level = 0;
  env_value = envvar_get ("LK_DUMP_LEVEL");
  if (env_value != NULL)
    {
      lk_Gl.dump_level = atoi (env_value);
      if (lk_Gl.dump_level < 0 || lk_Gl.dump_level > 3)
	{
	  lk_Gl.dump_level = 0;
	}
    }
#endif /* LK_DUMP */

  return error_code;

error:
  (void) lock_finalize ();
  return error_code;
#endif /* !SERVER_MODE */
}

/*
 * lock_finalize - Finalize the lock manager
 *
 * return: nothing
 *
 * Note:This function finalize the lock manager.
 *     Memory structures of the lock manager are deallocated.
 */
void
lock_finalize (void)
{
#if !defined (SERVER_MODE)
  lk_Standalone_has_xlock = false;
#else /* !SERVER_MODE */
  LK_RES_BLOCK *res_block;	/* pointer to lock resource table node */
  LK_ENTRY_BLOCK *entry_block;	/* pointer to lock entry block */
  LK_TRAN_LOCK *tran_lock;
  LK_HASH *hash_anchor;
  int i;

  /* Release all the locks and awake all transactions */
  /* TODO: Why ? */
  /* transaction deadlock information table */
  /* deallocate memory space for the transaction deadlock info. */
  if (lk_Gl.TWFG_node != (LK_WFG_NODE *) NULL)
    {
      free_and_init (lk_Gl.TWFG_node);
    }

  /* transaction lock information table */
  /* deallocate memory space for transaction lock table */
  if (lk_Gl.tran_lock_table != (LK_TRAN_LOCK *) NULL)
    {
      for (i = 0; i < lk_Gl.num_trans; i++)
	{
	  tran_lock = &lk_Gl.tran_lock_table[i];
	  MUTEX_DESTROY (tran_lock->hold_mutex);
	  MUTEX_DESTROY (tran_lock->non2pl_mutex);
	}
      free_and_init (lk_Gl.tran_lock_table);
    }
  /* reset the number of transactions */
  lk_Gl.num_trans = 0;

  /* object lock entry list */
  /* deallocate memory space for object lock entry block list */
  while (lk_Gl.obj_entry_block_list != (LK_ENTRY_BLOCK *) NULL)
    {
      entry_block = lk_Gl.obj_entry_block_list;
      lk_Gl.obj_entry_block_list = entry_block->next_block;
      if (entry_block->block != (LK_ENTRY *) NULL)
	{
	  free_and_init (entry_block->block);
	}
      free_and_init (entry_block);
    }
  MUTEX_DESTROY (lk_Gl.obj_res_block_list_mutex);
  MUTEX_DESTROY (lk_Gl.obj_free_res_list_mutex);
  /* reset object lock entry block list */
  lk_Gl.obj_free_entry_list = (LK_ENTRY *) NULL;

  /* object lock resource list */
  /* deallocate memory space for object lock resource table node list */
  while (lk_Gl.obj_res_block_list != (LK_RES_BLOCK *) NULL)
    {
      res_block = lk_Gl.obj_res_block_list;
      lk_Gl.obj_res_block_list = res_block->next_block;
      if (res_block->block != (LK_RES *) NULL)
	{
	  for (i = 0; i < res_block->count; i++)
	    {
	      MUTEX_DESTROY (res_block->block[i].res_mutex);
	    }
	  free_and_init (res_block->block);
	}
      free_and_init (res_block);
    }
  MUTEX_DESTROY (lk_Gl.obj_entry_block_list_mutex);
  MUTEX_DESTROY (lk_Gl.obj_free_entry_list_mutex);
  /* reset object lock resource free list */
  lk_Gl.obj_free_res_list = (LK_RES *) NULL;

  /* object lock hash table */
  /* deallocate memory space for object lock hash table */
  if (lk_Gl.obj_hash_table != (LK_HASH *) NULL)
    {
      for (i = 0; i < lk_Gl.obj_hash_size; i++)
	{
	  hash_anchor = &lk_Gl.obj_hash_table[i];
	  MUTEX_DESTROY (hash_anchor->hash_mutex);
	}
      free_and_init (lk_Gl.obj_hash_table);
    }
  /* reset max number of object locks */
  lk_Gl.obj_hash_size = 0;
  lk_Gl.max_obj_locks = 0;

  (void) lock_final_scanid_bitmap ();
#endif /* !SERVER_MODE */
}

/*
 * lock_hold_object_instant - Hold object lock with instant duration
 *
 * return: one of following values
 *     LK_GRANTED
 *     LK_NOTGRANTED
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   oid(in):
 *   class_oid(in):
 *   lock(in):
 */
int
lock_hold_object_instant (THREAD_ENTRY * thread_p, const OID * oid,
			  const OID * class_oid, LOCK lock)
{
#if !defined (SERVER_MODE)
  if (lock == X_LOCK || lock == IX_LOCK || lock == SIX_LOCK)
    {
      lk_Standalone_has_xlock = true;
    }
  return LK_GRANTED;

#else /* !SERVER_MODE */
  int tran_index;
  if (oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_object_instant", "NULL OID pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }
  if (class_oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_object_instant", "NULL ClassOID pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }
  if (lock == NULL_LOCK)
    {
      return LK_GRANTED;
    }

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  return lock_internal_hold_lock_object_instant (tran_index, oid, class_oid,
						 lock);

#endif /* !SERVER_MODE */
}

/*
 * lock_object - Lock an object
 *
 * return: one of following values)
 *     LK_GRANTED
 *     LK_NOTGRANTED_DUE_ABORTED
 *     LK_NOTGRANTED_DUE_TIMEOUT
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   oid(in): Identifier of object(instance, class, root class) to lock
 *   class_oid(in): Identifier of the class instance of the given object
 *   lock(in): Requested lock mode
 *   cond_flag(in):
 *
 */
int
lock_object (THREAD_ENTRY * thread_p, const OID * oid, const OID * class_oid,
	     LOCK lock, int cond_flag)
{
#if !defined (SERVER_MODE)
  if (lock == X_LOCK || lock == IX_LOCK || lock == SIX_LOCK)
    {
      lk_Standalone_has_xlock = true;
    }
  return LK_GRANTED;

#else /* !SERVER_MODE */
  int tran_index;
  int waitsecs;
  TRAN_ISOLATION isolation;
  LOCK new_class_lock;
  LOCK old_class_lock;
  int granted;
  LK_ENTRY *root_class_entry = NULL;
  LK_ENTRY *class_entry = NULL;
  LK_ENTRY *inst_entry = NULL;
  struct timeval start_time, end_time, elapsed_time;

  if (oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lock_object", "NULL OID pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }
  if (class_oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lock_object", "NULL ClassOID pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }


  if (lock == NULL_LOCK)
    {
      return LK_GRANTED;
    }

  if (0 < PRM_MNT_WAITING_THREAD)
    {
      gettimeofday (&start_time, NULL);
    }

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  if (cond_flag == LK_COND_LOCK)	/* conditional request */
    {
      waitsecs = LK_FORCE_ZERO_WAIT;
    }
  else
    {
      waitsecs = logtb_find_wait_secs (tran_index);
    }
  isolation = logtb_find_isolation (tran_index);

  /* check if the given oid is root class oid */
  if (OID_IS_ROOTOID (oid))
    {
      /* case 1 : resource type is LOCK_RESOURCE_ROOT_CLASS
       * acquire a lock on the root class oid.
       * NOTE that in case of acquiring a lock on a class object,
       * the higher lock granule of the class object must not be given.
       */
      if (LK_UNCOMMITTED_READ_ISOLATION (isolation))
	{
	  /* demote S_LOCK of a class to IS_LOCK */
	  if (lock == SIX_LOCK)
	    {
	      lock = IX_LOCK;
	    }
	  else if (lock == S_LOCK)
	    {
	      lock = IS_LOCK;
	    }
	}

      granted =
	lock_internal_perform_lock_object (thread_p, tran_index, oid,
					   (OID *) NULL, lock, waitsecs,
					   &root_class_entry, NULL);

      goto end;
    }

  /* get the intentional lock mode to be acquired on class oid */
  if (lock <= S_LOCK)
    {
      new_class_lock = IS_LOCK;
    }
  else
    {
      new_class_lock = IX_LOCK;
    }

  /* Check if current transaction has already held the class lock.
   * If the class lock is not held, hold the class lock, now.
   */
  class_entry = lock_get_class_lock (class_oid, tran_index);
  old_class_lock = (class_entry) ? class_entry->granted_mode : NULL_LOCK;

  if (OID_IS_ROOTOID (class_oid))
    {
      if (old_class_lock < new_class_lock)
	{
	  granted =
	    lock_internal_perform_lock_object (thread_p, tran_index,
					       class_oid, (OID *) NULL,
					       new_class_lock, waitsecs,
					       &root_class_entry, NULL);
	  if (granted != LK_GRANTED)
	    {
	      goto end;
	    }
	}
      /* case 2 : resource type is LOCK_RESOURCE_CLASS */
      /* acquire a lock on the given class object */
      if (LK_UNCOMMITTED_READ_ISOLATION (isolation))
	{
	  /* demote S_LOCK of a class to IS_LOCK */
	  if (lock == SIX_LOCK)
	    {
	      lock = IX_LOCK;
	    }
	  else if (lock == S_LOCK)
	    {
	      lock = IS_LOCK;
	    }
	}
      /* NOTE that in case of acquiring a lock on a class object,
       * the higher lock granule of the class object must not be given.
       */
      granted =
	lock_internal_perform_lock_object (thread_p, tran_index, oid,
					   (OID *) NULL, lock, waitsecs,
					   &class_entry, root_class_entry);
      goto end;
    }
  else
    {
      if (old_class_lock < new_class_lock)
	{
	  root_class_entry =
	    lock_get_class_lock (oid_Root_class_oid, tran_index);

	  granted =
	    lock_internal_perform_lock_object (thread_p, tran_index,
					       class_oid, (OID *) NULL,
					       new_class_lock, waitsecs,
					       &class_entry,
					       root_class_entry);
	  if (granted != LK_GRANTED)
	    {
	      goto end;
	    }
	}

      /* case 3 : resource type is LOCK_RESOURCE_INSTANCE */
      if (old_class_lock >= lock)
	{			/* already granted on the class level */
	  /* if incompatible old class lock with requested lock,
	     remove instant class locks */
	  lock_stop_instant_lock_mode (thread_p, tran_index, false);
	  granted = LK_GRANTED;
	  goto end;
	}
      /* acquire a lock on the given instance oid */
      if (LK_UNCOMMITTED_READ_ISOLATION (isolation))
	{
	  /* do not hold shared or update locks */
	  if (lock <= S_LOCK || lock == U_LOCK)
	    {
	      granted = LK_GRANTED;
	      goto end;
	    }
	}
      /* NOTE that in case of acquiring a lock on an instance object,
       * the class oid of the intance object must be given.
       */
      granted =
	lock_internal_perform_lock_object (thread_p, tran_index, oid,
					   class_oid, lock, waitsecs,
					   &inst_entry, class_entry);

      goto end;
    }

end:
  if (0 < PRM_MNT_WAITING_THREAD)
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }
  if (MONITOR_WAITING_THREAD (elapsed_time))
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_MNT_WAITING_THREAD, 2, "lock object (lock_object)",
	      PRM_MNT_WAITING_THREAD);
      er_log_debug (ARG_FILE_LINE, "lock_object: %6d.%06d\n",
		    elapsed_time.tv_sec, elapsed_time.tv_usec);
    }

  return granted;
#endif /* !SERVER_MODE */
}

/*
 * lock_object_waitsecs - Lock an object
 *
 * return: one of following values)
 *     LK_GRANTED
 *     LK_NOTGRANTED_DUE_ABORTED
 *     LK_NOTGRANTED_DUE_TIMEOUT
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   oid(in): Identifier of object(instance, class, root class) to lock
 *   class_oid(in): Identifier of the class instance of the given object
 *   lock(in): Requested lock mode
 *   cond_flag(in):
 *   waitsecs(in):
 *
 */
int
lock_object_waitsecs (THREAD_ENTRY * thread_p, const OID * oid,
		      const OID * class_oid, LOCK lock, int cond_flag,
		      int waitsecs)
{
#if !defined (SERVER_MODE)
  if (lock == X_LOCK || lock == IX_LOCK || lock == SIX_LOCK)
    {
      lk_Standalone_has_xlock = true;
    }
  return LK_GRANTED;

#else /* !SERVER_MODE */
  int old_waitsecs = xlogtb_reset_wait_secs (thread_p, waitsecs);
  int lock_result = lock_object (thread_p, oid, class_oid, lock, cond_flag);

  xlogtb_reset_wait_secs (thread_p, old_waitsecs);

  return lock_result;
#endif
}

/*
 * lock_object_on_iscan -
 *
 * return:
 *
 *   oid(in):
 *   class_oid(in):
 *   lock(in):
 *   cond_flag(in):
 *   scanid_bit(in):
 *
 * Note:Same as lock_object except that scanid_bit is given as an argument.
 *  this scanid_bit value is kept in lk_entry structure,
 *  and used in lock_unlock_scan.
 */
int
lock_object_on_iscan (THREAD_ENTRY * thread_p, const OID * oid,
		      const OID * class_oid, LOCK lock, int cond_flag,
		      int scanid_bit)
{
#if !defined (SERVER_MODE)
  if (lock == X_LOCK || lock == IX_LOCK || lock == SIX_LOCK)
    {
      lk_Standalone_has_xlock = true;
    }
  return LK_GRANTED;
#else /* !SERVER_MODE */
  int tran_index;
  int waitsecs;
  TRAN_ISOLATION isolation;
  LOCK new_class_lock;
  LOCK old_class_lock;
  int granted;
  LK_ENTRY *root_class_entry = NULL;
  LK_ENTRY *class_entry = NULL;
  LK_ENTRY *inst_entry = NULL;
  struct timeval start_time, end_time, elapsed_time;

  if (oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lock_object_on_iscan", "NULL OID pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }
  if (class_oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lock_object_on_iscan", "NULL ClassOID pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }


  if (lock == NULL_LOCK)
    {
      return LK_GRANTED;
    }

  if (0 < PRM_MNT_WAITING_THREAD)
    {
      gettimeofday (&start_time, NULL);
    }

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  if (cond_flag == LK_COND_LOCK)	/* conditional request */
    {
      waitsecs = LK_FORCE_ZERO_WAIT;
    }
  else
    {
      waitsecs = logtb_find_wait_secs (tran_index);
    }
  isolation = logtb_find_isolation (tran_index);

  /* check if the given oid is root class oid */
  if (OID_IS_ROOTOID (oid))
    {
      /* case 1 : resource type is LOCK_RESOURCE_ROOT_CLASS
       * acquire a lock on the root class oid.
       * NOTE that in case of acquiring a lock on a class object,
       * the higher lock granule of the class object must not be given.
       */
      if (LK_UNCOMMITTED_READ_ISOLATION (isolation))
	{
	  /* demote S_LOCK of a class to IS_LOCK */
	  if (lock == SIX_LOCK)
	    {
	      lock = IX_LOCK;
	    }
	  else if (lock == S_LOCK)
	    {
	      lock = IS_LOCK;
	    }
	}

      granted =
	lock_internal_perform_lock_object (thread_p, tran_index, oid,
					   (OID *) NULL, lock, waitsecs,
					   &root_class_entry, NULL);
      goto end;
    }

  /* get the intentional lock mode to be acquired on class oid */
  if (lock <= S_LOCK)
    {
      new_class_lock = IS_LOCK;
    }
  else
    {
      new_class_lock = IX_LOCK;
    }

  /* Check if current transaction has already held the class lock.
   * If the class lock is not held, hold the class lock, now.
   */
  class_entry = lock_get_class_lock (class_oid, tran_index);
  old_class_lock = (class_entry) ? class_entry->granted_mode : NULL_LOCK;

  if (OID_IS_ROOTOID (class_oid))
    {
      if (old_class_lock < new_class_lock)
	{
	  granted =
	    lock_internal_perform_lock_object (thread_p, tran_index,
					       class_oid, (OID *) NULL,
					       new_class_lock, waitsecs,
					       &root_class_entry, NULL);
	  if (granted != LK_GRANTED)
	    {
	      goto end;
	    }
	}
      /* case 2 : resource type is LOCK_RESOURCE_CLASS */
      /* acquire a lock on the given class object */
      if (LK_UNCOMMITTED_READ_ISOLATION (isolation))
	{
	  /* demote S_LOCK of a class to IS_LOCK */
	  if (lock == SIX_LOCK)
	    {
	      lock = IX_LOCK;
	    }
	  else if (lock == S_LOCK)
	    {
	      lock = IS_LOCK;
	    }
	}
      /* NOTE that in case of acquiring a lock on a class object,
       * the higher lock granule of the class object must not be given.
       */
      granted =
	lock_internal_perform_lock_object (thread_p, tran_index, oid,
					   (OID *) NULL, lock, waitsecs,
					   &class_entry, root_class_entry);
    }
  else
    {
      if (old_class_lock < new_class_lock)
	{
	  root_class_entry =
	    lock_get_class_lock (oid_Root_class_oid, tran_index);

	  granted =
	    lock_internal_perform_lock_object (thread_p, tran_index,
					       class_oid, (OID *) NULL,
					       new_class_lock, waitsecs,
					       &class_entry,
					       root_class_entry);
	  if (granted != LK_GRANTED)
	    {
	      goto end;
	    }
	}

      /* case 3 : resource type is LOCK_RESOURCE_INSTANCE */
      if (old_class_lock >= lock)
	{			/* already granted on the class level */
	  /* if incompatible old class lock with requested lock,
	     remove instant class locks */
	  lock_stop_instant_lock_mode (thread_p, tran_index, false);
	  granted = LK_GRANTED;
	  goto end;
	}
      /* acquire a lock on the given instance oid */
      if (LK_UNCOMMITTED_READ_ISOLATION (isolation))
	{
	  /* do not hold shared or update locks */
	  if (lock <= S_LOCK)
	    {
	      granted = LK_GRANTED;
	      goto end;
	    }
	}
      /* NOTE that in case of acquiring a lock on an instance object,
       * the class oid of the intance object must be given.
       */
      granted =
	lock_internal_perform_lock_object (thread_p, tran_index, oid,
					   class_oid, lock, waitsecs,
					   &inst_entry, class_entry);
      if (scanid_bit >= 0 && granted == LK_GRANTED && inst_entry != NULL)
	{
	  SET_SCANID_BIT (inst_entry->scanid_bitset, scanid_bit);
	}
    }

end:
  if (0 < PRM_MNT_WAITING_THREAD)
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }
  if (MONITOR_WAITING_THREAD (elapsed_time))
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_MNT_WAITING_THREAD, 2, "lock object (lock_object_on_iscan)",
	      PRM_MNT_WAITING_THREAD);
      er_log_debug (ARG_FILE_LINE, "lock_object_on_iscan: %6d.%06d\n",
		    elapsed_time.tv_sec, elapsed_time.tv_usec);
    }

  return granted;
#endif /* !SERVER_MODE */
}

/*
 * lock_objects_lock_set - Lock many objects
 *
 * return: one of following values
 *     LK_GRANTED
 *     LK_NOTGRANTED_DUE_ABORTED
 *     LK_NOTGRANTED_DUE_TIMEOUT
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   lockset(in/out): Request the lock of many objects
 *
 */
int
lock_objects_lock_set (THREAD_ENTRY * thread_p, LC_LOCKSET * lockset)
{
#if !defined (SERVER_MODE)
  if (lockset->reqobj_class_lock == X_LOCK
      || lockset->reqobj_class_lock == IX_LOCK
      || lockset->reqobj_class_lock == SIX_LOCK
      || lockset->reqobj_inst_lock == X_LOCK)
    {
      lk_Standalone_has_xlock = true;
    }
  return LK_GRANTED;
#else /* !SERVER_MODE */
  int tran_index;
  int waitsecs;
  TRAN_ISOLATION isolation;
  LK_LOCKINFO cls_lockinfo_space[LK_LOCKINFO_FIXED_COUNT];
  LK_LOCKINFO *cls_lockinfo;
  LK_LOCKINFO ins_lockinfo_space[LK_LOCKINFO_FIXED_COUNT];
  LK_LOCKINFO *ins_lockinfo;
  LC_LOCKSET_REQOBJ *reqobjects;	/* Description of one instance to
					 * lock
					 */
  LC_LOCKSET_CLASSOF *reqclasses;	/* Description of one class of a
					 * requested object to lock
					 */
  LOCK reqobj_class_lock;
  LOCK reqobj_inst_lock;
  int cls_count;
  int ins_count;
  int granted, i;
  LK_ENTRY *root_class_entry = NULL;
  LK_ENTRY *class_entry = NULL;
  LK_ENTRY *inst_entry = NULL;
  LOCK intention_mode;
  struct timeval start_time, end_time, elapsed_time;

  if (lockset == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lock_objects_lock_set", "NULL lockset pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }

  if (0 < PRM_MNT_WAITING_THREAD)
    {
      gettimeofday (&start_time, NULL);
    }

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  waitsecs = logtb_find_wait_secs (tran_index);
  isolation = logtb_find_isolation (tran_index);

  /* We do not want to rollback the transaction in the event of a deadlock.
   * For now, let's just wait a long time. If deadlock, the transaction is
   * going to be notified of lock timeout instead of aborted.
   */
  if (lockset->quit_on_errors == false && waitsecs == LK_INFINITE_WAIT)
    {
      waitsecs = INT_MAX;	/* will be notified of lock timeout */
    }

  /* prepare cls_lockinfo and ins_lockinfo array */
  if (lockset->num_reqobjs <= LK_LOCKINFO_FIXED_COUNT)
    {
      cls_lockinfo = &(cls_lockinfo_space[0]);
      ins_lockinfo = &(ins_lockinfo_space[0]);
    }
  else
    {				/* lockset->num_reqobjs > LK_LOCKINFO_FIXED_COUNT */
      cls_lockinfo =
	(LK_LOCKINFO *) db_private_alloc (thread_p,
					  SIZEOF_LK_LOCKINFO *
					  lockset->num_reqobjs);
      if (cls_lockinfo == (LK_LOCKINFO *) NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (SIZEOF_LK_LOCKINFO * lockset->num_reqobjs));
	  return LK_NOTGRANTED_DUE_ERROR;
	}
      ins_lockinfo =
	(LK_LOCKINFO *) db_private_alloc (thread_p,
					  SIZEOF_LK_LOCKINFO *
					  lockset->num_reqobjs);
      if (ins_lockinfo == (LK_LOCKINFO *) NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (SIZEOF_LK_LOCKINFO * lockset->num_reqobjs));
	  db_private_free_and_init (thread_p, cls_lockinfo);
	  return LK_NOTGRANTED_DUE_ERROR;
	}
    }

  reqobjects = lockset->objects;
  reqclasses = lockset->classes;

  /* get reqobj_class_lock and reqobj_inst_lock considering isolation */
  if (LK_UNCOMMITTED_READ_ISOLATION (isolation))
    {
      /* When the desired object is a class...
       * demote S_LOCK of a class to IS_LOCK
       */
      if (lockset->reqobj_class_lock == SIX_LOCK)
	{
	  reqobj_class_lock = IX_LOCK;
	}
      else if (lockset->reqobj_class_lock == S_LOCK)
	{
	  reqobj_class_lock = IS_LOCK;
	}
      else
	{
	  reqobj_class_lock = lockset->reqobj_class_lock;
	}
    }
  else
    {
      reqobj_class_lock = lockset->reqobj_class_lock;
    }
  reqobj_inst_lock = lockset->reqobj_inst_lock;

  /* build cls_lockinfo and ins_lockinfo array */
  cls_count = ins_count = 0;

  for (i = 0; i < lockset->num_reqobjs; i++)
    {
      if (OID_ISNULL (&reqobjects[i].oid)
	  || reqobjects[i].class_index < 0
	  || reqobjects[i].class_index > lockset->num_classes_of_reqobjs)
	{
	  continue;
	}

      if (OID_IS_ROOTOID (&reqclasses[reqobjects[i].class_index].oid))
	{
	  /* requested object: class => build cls_lockinfo[cls_count] */
	  COPY_OID (&cls_lockinfo[cls_count].oid, &reqobjects[i].oid);
	  cls_lockinfo[cls_count].org_oidp = &reqobjects[i].oid;
	  cls_lockinfo[cls_count].lock = reqobj_class_lock;
	  /* increment cls_count */
	  cls_count++;
	}
      else
	{
	  /* requested object: instance => build cls_lockinfo[cls_count] */
	  COPY_OID (&ins_lockinfo[ins_count].oid, &reqobjects[i].oid);
	  COPY_OID (&ins_lockinfo[ins_count].class_oid,
		    &reqclasses[reqobjects[i].class_index].oid);
	  ins_lockinfo[ins_count].org_oidp = &reqobjects[i].oid;
	  ins_lockinfo[ins_count].lock = reqobj_inst_lock;
	  /* increment ins_count */
	  ins_count++;
	}
    }

  if (cls_count > 0)
    {
      /* sort the cls_lockinfo to avoid deadlock */
      if (cls_count > 1)
	{
	  (void) qsort (cls_lockinfo, cls_count, SIZEOF_LK_LOCKINFO,
			lock_compare_lock_info);
	}

      /* hold the locks on the given class objects */
      root_class_entry = lock_get_class_lock (oid_Root_class_oid, tran_index);

      for (i = 0; i < cls_count; i++)
	{
	  /* hold an intention lock on the root class if it is needed. */
	  intention_mode =
	    (cls_lockinfo[i].lock <= S_LOCK ? IS_LOCK : IX_LOCK);
	  if (root_class_entry == NULL
	      || root_class_entry->granted_mode < intention_mode)
	    {
	      granted = lock_internal_perform_lock_object (thread_p,
							   tran_index,
							   oid_Root_class_oid,
							   (OID *) NULL,
							   intention_mode,
							   waitsecs,
							   &root_class_entry,
							   NULL);
	      if (granted != LK_GRANTED)
		{
		  if (lockset->quit_on_errors == false
		      && granted == LK_NOTGRANTED_DUE_TIMEOUT)
		    {
		      OID_SET_NULL (cls_lockinfo[i].org_oidp);
		      continue;
		    }
		  goto error;
		}
	    }

	  /* hold the locks on the given class objects */
	  granted = lock_internal_perform_lock_object (thread_p,
						       tran_index,
						       &cls_lockinfo[i].oid,
						       (OID *) NULL,
						       cls_lockinfo[i].lock,
						       waitsecs, &class_entry,
						       root_class_entry);
	  if (granted != LK_GRANTED)
	    {
	      if (lockset->quit_on_errors == false
		  && granted == LK_NOTGRANTED_DUE_TIMEOUT)
		{
		  OID_SET_NULL (cls_lockinfo[i].org_oidp);
		  continue;
		}
	      goto error;
	    }
	}
    }

  if (ins_count > 0)
    {
      /* sort the ins_lockinfo to avoid deadlock */
      if (ins_count > 1)
	{
	  (void) qsort (ins_lockinfo, ins_count, SIZEOF_LK_LOCKINFO,
			lock_compare_lock_info);
	}

      /* hold the locks on the given instance objects */
      for (i = 0; i < ins_count; i++)
	{
	  /* hold an intention lock on the class if it is needed. */
	  intention_mode = (ins_lockinfo[i].lock <= S_LOCK
			    ? IS_LOCK : IX_LOCK);
	  class_entry = lock_get_class_lock (&ins_lockinfo[i].class_oid,
					     tran_index);
	  if (class_entry == NULL
	      || class_entry->granted_mode < intention_mode)
	    {
	      granted = lock_internal_perform_lock_object (thread_p,
							   tran_index,
							   &ins_lockinfo[i].
							   class_oid,
							   (OID *) NULL,
							   intention_mode,
							   waitsecs,
							   &class_entry,
							   root_class_entry);
	      if (granted != LK_GRANTED)
		{
		  if (lockset->quit_on_errors == false
		      && granted == LK_NOTGRANTED_DUE_TIMEOUT)
		    {
		      OID_SET_NULL (ins_lockinfo[i].org_oidp);
		      continue;
		    }
		  goto error;
		}
	    }

	  /* hold the lock on the given instance */
	  granted = lock_internal_perform_lock_object (thread_p,
						       tran_index,
						       &ins_lockinfo[i].oid,
						       &ins_lockinfo[i].
						       class_oid,
						       ins_lockinfo[i].lock,
						       waitsecs, &inst_entry,
						       class_entry);
	  if (granted != LK_GRANTED)
	    {
	      if (lockset->quit_on_errors == false
		  && granted == LK_NOTGRANTED_DUE_TIMEOUT)
		{
		  OID_SET_NULL (ins_lockinfo[i].org_oidp);
		  continue;
		}
	      goto error;
	    }
	}
    }

  /* release memory space for cls_lockinfo and ins_lockinfo */
  if (cls_lockinfo != &cls_lockinfo_space[0])
    {
      db_private_free_and_init (thread_p, cls_lockinfo);
      db_private_free_and_init (thread_p, ins_lockinfo);
    }

  if (0 < PRM_MNT_WAITING_THREAD)
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }
  if (MONITOR_WAITING_THREAD (elapsed_time))
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_MNT_WAITING_THREAD, 2, "lock object (lock_objects_lock_set)",
	      PRM_MNT_WAITING_THREAD);
      er_log_debug (ARG_FILE_LINE, "lock_objects_lock_set: %6d.%06d\n",
		    elapsed_time.tv_sec, elapsed_time.tv_usec);
    }

  return LK_GRANTED;

error:
  if (cls_lockinfo != &cls_lockinfo_space[0])
    {
      db_private_free_and_init (thread_p, cls_lockinfo);
      db_private_free_and_init (thread_p, ins_lockinfo);
    }
  return granted;
#endif /* !SERVER_MODE */
}

/*
 * lock_scan - Lock for scanning a heap
 *
 * return: one of following values)
 *     LK_GRANTED
 *     LK_NOTGRANTED_DUE_ABORTED
 *     LK_NOTGRANTED_DUE_TIMEOUT
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   class_oid(in): class oid of the instances to be scanned
 *   is_indexscan(in): scan type
 *                      - true  => index scan
 *                      - false => sequential scan
 *   lock_hint(in): lock hint
 *   current_lock(in/out): currently acquired lock
 *   scanid_bit(in): scanid bit for an index scan
 *
 */
int
lock_scan (THREAD_ENTRY * thread_p, const OID * class_oid, bool is_indexscan,
	   int lock_hint, LOCK * current_lock, int *scanid_bit)
{
#if !defined (SERVER_MODE)
  if (lk_Standalone_has_xlock == true)
    {
      *current_lock = X_LOCK;
    }
  else
    {
      *current_lock = S_LOCK;
    }
  return LK_GRANTED;
#else /* !SERVER_MODE */
  int tran_index;
  int waitsecs;
  TRAN_ISOLATION isolation;
  LOCK class_lock;
  int granted;
  LK_ENTRY *root_class_entry = NULL;
  LK_ENTRY *class_entry = NULL;
  struct timeval start_time, end_time, elapsed_time;

  if (class_oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lock_scan", "NULL ClassOID pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }

  if (0 < PRM_MNT_WAITING_THREAD)
    {
      gettimeofday (&start_time, NULL);
    }

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  waitsecs = logtb_find_wait_secs (tran_index);
  isolation = logtb_find_isolation (tran_index);

  /* get the class lock mode to be acquired */
  if (OID_EQ (class_oid, oid_Root_class_oid))
    {
      class_lock = IS_LOCK;
    }
  else
    {
      if (is_indexscan == true)
	{
	  class_lock = IS_LOCK;
	}
      else
	{
	  if (LK_UNCOMMITTED_READ_ISOLATION (isolation)
	      || (lock_hint & LOCKHINT_READ_UNCOMMITTED))
	    {
	      class_lock = IS_LOCK;
	    }
	  else
	    {
	      class_lock = S_LOCK;
	    }
	}
    }

  /* acquire the lock on the class */
  /* NOTE that in case of acquiring a lock on a class object,
   * the higher lock granule of the class object is not given.
   */
  if (is_indexscan && !LK_UNCOMMITTED_READ_ISOLATION (isolation)
      && !(lock_hint & LOCKHINT_READ_UNCOMMITTED))
    {
      *scanid_bit = lock_alloc_scanid_bit (thread_p);
      if (*scanid_bit < 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ALLOC_RESOURCE,
		  1, "scanid bit");
	  return LK_NOTGRANTED_DUE_ERROR;
	}
    }
  root_class_entry = lock_get_class_lock (oid_Root_class_oid, tran_index);
  granted = lock_internal_perform_lock_object (thread_p, tran_index,
					       class_oid, (OID *) NULL,
					       class_lock, waitsecs,
					       &class_entry,
					       root_class_entry);
  if (granted == LK_GRANTED)
    {
      *current_lock = class_lock;
    }
  else
    {
      if (*scanid_bit >= 0)
	{
	  lock_free_scanid_bit (thread_p, *scanid_bit);
	}
      *current_lock = NULL_LOCK;
    }

  if (0 < PRM_MNT_WAITING_THREAD)
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }
  if (MONITOR_WAITING_THREAD (elapsed_time))
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_MNT_WAITING_THREAD, 2, "lock object (lock_scan)",
	      PRM_MNT_WAITING_THREAD);
      er_log_debug (ARG_FILE_LINE, "lock_scan: %6d.%06d\n",
		    elapsed_time.tv_sec, elapsed_time.tv_usec);
    }

  return granted;
#endif /* !SERVER_MODE */
}

/*
 * lock_classes_lock_hint - Lock many classes that has been hinted
 *
 * return: one of following values
 *     LK_GRANTED
 *     LK_NOTGRANTED_DUE_ABORTED
 *     LK_NOTGRANTED_DUE_TIMEOUT
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   lockhint(in): description of hinted classses
 *
 */
int
lock_classes_lock_hint (THREAD_ENTRY * thread_p, LC_LOCKHINT * lockhint)
{
#if !defined (SERVER_MODE)
  int i;

  for (i = 0; i < lockhint->num_classes; i++)
    {
      if (lockhint->classes[i].lock == X_LOCK
	  || lockhint->classes[i].lock == IX_LOCK
	  || lockhint->classes[i].lock == SIX_LOCK)
	{
	  lk_Standalone_has_xlock = true;
	  break;
	}
    }
  return LK_GRANTED;
#else /* !SERVER_MODE */
  int tran_index;
  int waitsecs;
  TRAN_ISOLATION isolation;
  LK_LOCKINFO cls_lockinfo_space[LK_LOCKINFO_FIXED_COUNT];
  LK_LOCKINFO *cls_lockinfo;
  LK_ENTRY *root_class_entry = NULL;
  LK_ENTRY *class_entry = NULL;
  OID *root_oidp;
  LOCK root_lock;
  LOCK intention_mode;
  int cls_count;
  int granted, i;
  struct timeval start_time, end_time, elapsed_time;

  if (lockhint == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lock_classes_lock_hint", "NULL lockhint pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }

  /* If there is nothing to lock, returns */
  if (lockhint->num_classes <= 0)
    {
      return LK_GRANTED;
    }

  if (0 < PRM_MNT_WAITING_THREAD)
    {
      gettimeofday (&start_time, NULL);
    }

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  waitsecs = logtb_find_wait_secs (tran_index);
  isolation = logtb_find_isolation (tran_index);

  /* We do not want to rollback the transaction in the event of a deadlock.
   * For now, let's just wait a long time. If deadlock, the transaction is
   * going to be notified of lock timeout instead of aborted.
   */
  if (lockhint->quit_on_errors == false && waitsecs == LK_INFINITE_WAIT)
    {
      waitsecs = INT_MAX;	/* to be notified of lock timeout  */
    }

  /* prepare cls_lockinfo array */
  if (lockhint->num_classes <= LK_LOCKINFO_FIXED_COUNT)
    {
      cls_lockinfo = &cls_lockinfo_space[0];
    }
  else
    {				/* num_classes > LK_LOCKINFO_FIXED_COUNT */
      cls_lockinfo =
	(LK_LOCKINFO *) db_private_alloc (thread_p,
					  SIZEOF_LK_LOCKINFO *
					  lockhint->num_classes);
      if (cls_lockinfo == (LK_LOCKINFO *) NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (SIZEOF_LK_LOCKINFO * lockhint->num_classes));
	  return LK_NOTGRANTED_DUE_ERROR;
	}
    }

  /* Define the desired locks for all classes */
  /* get class_oids and class_locks */
  cls_count = 0;
  for (i = 0; i < lockhint->num_classes; i++)
    {
      if (OID_ISNULL (&lockhint->classes[i].oid)
	  || lockhint->classes[i].lock == NULL_LOCK)
	{
	  continue;
	}

      if (OID_IS_ROOTOID (&lockhint->classes[i].oid))
	{
	  /* When the given class is the root class */
	  root_oidp = &lockhint->classes[i].oid;
	  root_lock = lockhint->classes[i].lock;
	  if (LK_UNCOMMITTED_READ_ISOLATION (isolation))
	    {
	      /* demote S_LOCK to IS_LOCK */
	      if (root_lock == SIX_LOCK)
		{
		  root_lock = IX_LOCK;
		}
	      else if (root_lock == S_LOCK)
		{
		  root_lock = IS_LOCK;
		}
	    }

	  /* hold an explicit lock on the root class */
	  granted =
	    lock_internal_perform_lock_object (thread_p, tran_index,
					       root_oidp, (OID *) NULL,
					       root_lock, waitsecs,
					       &root_class_entry, NULL);
	  if (granted != LK_GRANTED)
	    {
	      if (lockhint->quit_on_errors == true
		  || granted != LK_NOTGRANTED_DUE_TIMEOUT)
		{
		  goto error;
		}
	      else
		{
		  OID_SET_NULL (root_oidp);
		}
	    }
	}
      else
	{
	  /* build cls_lockinfo[cls_count] */
	  COPY_OID (&cls_lockinfo[cls_count].oid, &lockhint->classes[i].oid);
	  cls_lockinfo[cls_count].org_oidp = &lockhint->classes[i].oid;
	  cls_lockinfo[cls_count].lock = lockhint->classes[i].lock;
	  if (LK_UNCOMMITTED_READ_ISOLATION (isolation))
	    {
	      /* demote S_LOCK to IS_LOCK */
	      if (lockhint->classes[i].lock == SIX_LOCK)
		{
		  cls_lockinfo[cls_count].lock = IX_LOCK;
		}
	      else if (lockhint->classes[i].lock == S_LOCK)
		{
		  cls_lockinfo[cls_count].lock = IS_LOCK;
		}
	    }
	  /* increment cls_count */
	  cls_count++;
	}
    }

  /* sort class oids before hold the locks in order to avoid deadlocks */
  if (cls_count > 1)
    {
      (void) qsort (cls_lockinfo, cls_count, SIZEOF_LK_LOCKINFO,
		    lock_compare_lock_info);
    }

  /* get root class lock mode */
  root_class_entry = lock_get_class_lock (oid_Root_class_oid, tran_index);

  for (i = 0; i < cls_count; i++)
    {
      /* hold the intentional lock on the root class if needed. */
      if (cls_lockinfo[i].lock <= S_LOCK)
	{
	  intention_mode = IS_LOCK;
	}
      else
	{
	  intention_mode = IX_LOCK;
	}

      if (root_class_entry == NULL
	  || root_class_entry->granted_mode < intention_mode)
	{
	  granted = lock_internal_perform_lock_object (thread_p, tran_index,
						       oid_Root_class_oid,
						       (OID *) NULL,
						       intention_mode,
						       waitsecs,
						       &root_class_entry,
						       NULL);
	  if (granted != LK_GRANTED)
	    {
	      if (lockhint->quit_on_errors == false
		  && granted == LK_NOTGRANTED_DUE_TIMEOUT)
		{
		  OID_SET_NULL (cls_lockinfo[i].org_oidp);
		  continue;
		}
	      goto error;
	    }
	}

      /* hold the lock on the given class. */
      granted = lock_internal_perform_lock_object (thread_p, tran_index,
						   &cls_lockinfo[i].oid,
						   (OID *) NULL,
						   cls_lockinfo[i].lock,
						   waitsecs, &class_entry,
						   root_class_entry);

      if (granted != LK_GRANTED)
	{
	  if (lockhint->quit_on_errors == false
	      && granted == LK_NOTGRANTED_DUE_TIMEOUT)
	    {
	      OID_SET_NULL (cls_lockinfo[i].org_oidp);
	      continue;
	    }
	  goto error;
	}
    }

  /* release memory space for cls_lockinfo */
  if (cls_lockinfo != &cls_lockinfo_space[0])
    {
      db_private_free_and_init (thread_p, cls_lockinfo);
    }

  if (0 < PRM_MNT_WAITING_THREAD)
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }
  if (MONITOR_WAITING_THREAD (elapsed_time))
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_MNT_WAITING_THREAD, 2,
	      "lock object (lock_classes_lock_hint)", PRM_MNT_WAITING_THREAD);
      er_log_debug (ARG_FILE_LINE, "lock_classes_lock_hint: %6d.%06d\n",
		    elapsed_time.tv_sec, elapsed_time.tv_usec);
    }

  return LK_GRANTED;

error:
  if (cls_lockinfo != &cls_lockinfo_space[0])
    {
      db_private_free_and_init (thread_p, cls_lockinfo);
    }
  return granted;
#endif /* !SERVER_MODE */
}

/*
 * lock_unlock_object - Unlock an object according to transaction isolation level
 *
 * return: nothing..
 *
 *   oid(in): Identifier of instance to lock
 *   class_oid(in): Identifier of the class of the instance
 *   lock(in): Lock to release
 *   force(in): Unlock the object no matter what it is the isolation level.
 *
 */
void
lock_unlock_object (THREAD_ENTRY * thread_p, const OID * oid,
		    const OID * class_oid, LOCK lock, int force)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int tran_index;		/* transaction table index */
  TRAN_ISOLATION isolation;	/* transaction isolation level */
  LK_ENTRY *entry_ptr;

  if (oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_unlock_object", "NULL OID pointer");
      return;
    }
  if (class_oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_unlock_object", "NULL ClassOID pointer");
      return;
    }

  /* get transaction table index */
  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);

  if (force == true)
    {
      if (OID_IS_ROOTOID (oid) || OID_IS_ROOTOID (class_oid))
	entry_ptr = lock_find_class_entry (tran_index, oid);
      else
	entry_ptr = lock_find_inst_entry (tran_index, oid);

      if (entry_ptr != (LK_ENTRY *) NULL)
	lock_internal_perform_unlock_object (thread_p, entry_ptr, false,
					     true);

      return;
    }

  /* force != true */
  isolation = logtb_find_isolation (tran_index);
  switch (isolation)
    {
    case TRAN_SERIALIZABLE:
    case TRAN_REP_CLASS_REP_INSTANCE:
      break;			/* nothing to do */

    case TRAN_REP_CLASS_COMMIT_INSTANCE:
      /* demote shared class lock or unlock shared instance lock */
      /* The intentional lock on the higher lock granule must be kept */
      if (OID_IS_ROOTOID (oid) || OID_IS_ROOTOID (class_oid))
	lock_demote_shared_class_lock (thread_p, tran_index, oid);
      else			/* instance object */
	lock_unlock_shared_inst_lock (thread_p, tran_index, oid);
      break;

    case TRAN_REP_CLASS_UNCOMMIT_INSTANCE:
      /* shared lock on an instance was never acquired */
      /* only intentional lock was held on class oid */
      break;			/* nothing to do */

    case TRAN_COMMIT_CLASS_COMMIT_INSTANCE:
      /* remove shared class lock or remove shared instance lock */
      if (OID_IS_ROOTOID (oid) || OID_IS_ROOTOID (class_oid))
	lock_unlock_shared_class_lock (thread_p, tran_index, oid);
      else			/* instance lock */
	lock_unlock_shared_inst_lock (thread_p, tran_index, oid);
      if (!OID_IS_ROOTOID (oid))	/* intentional lock */
	lock_unlock_shared_class_lock (thread_p, tran_index, class_oid);
      break;

    case TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE:
      /* Share lock on instance was never acquired */
      /* remove shared class lock */
      if (OID_IS_ROOTOID (oid) || OID_IS_ROOTOID (class_oid))
	lock_unlock_shared_class_lock (thread_p, tran_index, oid);
      if (!OID_IS_ROOTOID (oid))	/* intentional lock */
	lock_unlock_shared_class_lock (thread_p, tran_index, class_oid);
      break;

    default:			/* TRAN_UNKNOWN_ISOLATION */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_UNKNOWN_ISOLATION, 2,
	      isolation, tran_index);
      break;
    }
#endif /* !SERVER_MODE */
}

/*
 * lock_unlock_objects_lock_set - Unlock many objects according to isolation level
 *
 * return: nothing..
 *
 *   lockset(in):
 *
 */
void
lock_unlock_objects_lock_set (THREAD_ENTRY * thread_p, LC_LOCKSET * lockset)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int tran_index;		/* transaction table index */
  TRAN_ISOLATION isolation;	/* transaction isolation level */
  LOCK reqobj_class_unlock;
  LOCK reqobj_inst_unlock;
  OID *oid, *class_oid;
  int i;

  if (lockset == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_unlock_objects_lockset", "NULL lockset pointer");
      return;
    }

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  isolation = logtb_find_isolation (tran_index);

  if (isolation == TRAN_SERIALIZABLE
      || isolation == TRAN_REP_CLASS_REP_INSTANCE)
    {
      return;			/* Nothing to release */
    }

  reqobj_class_unlock = lockset->reqobj_class_lock;
  if (reqobj_class_unlock == X_LOCK)
    {
      reqobj_class_unlock = NULL_LOCK;	/* Don't release the lock */
    }
  reqobj_inst_unlock = lockset->reqobj_inst_lock;

  if (reqobj_inst_unlock == NULL_LOCK && reqobj_class_unlock == NULL_LOCK)
    {
      return;
    }

  for (i = 0; i < lockset->num_reqobjs; i++)
    {
      oid = &lockset->objects[i].oid;
      if (OID_ISNULL (oid) || lockset->objects[i].class_index == -1)
	{
	  continue;
	}

      class_oid = &lockset->classes[lockset->objects[i].class_index].oid;

      if (OID_IS_ROOTOID (class_oid))
	{			/* class object */
	  if (reqobj_class_unlock == NULL_LOCK)
	    {
	      continue;
	    }
	}
      else
	{			/* instance object */
	  if (reqobj_inst_unlock == NULL_LOCK)
	    {
	      continue;
	    }
	}

      switch (isolation)
	{
	case TRAN_REP_CLASS_COMMIT_INSTANCE:
	  /* demote shared class lock or unlock shared instance lock */
	  /* The intentional lock on the higher lock granule must be kept */
	  if (OID_IS_ROOTOID (class_oid))
	    {
	      lock_demote_shared_class_lock (thread_p, tran_index, oid);
	    }
	  else
	    {
	      lock_unlock_shared_inst_lock (thread_p, tran_index, oid);
	    }
	  break;

	case TRAN_REP_CLASS_UNCOMMIT_INSTANCE:
	  /* shared lock on an instance was never acquired */
	  /* Intentional mode was held on the class oid */
	  break;

	case TRAN_COMMIT_CLASS_COMMIT_INSTANCE:
	  /* unlock shared class lock or unlock shared instance lock */
	  /* unlock the intentional lock if it is a shared lock */
	  if (OID_IS_ROOTOID (class_oid))
	    {
	      lock_unlock_shared_class_lock (thread_p, tran_index, oid);
	    }
	  else
	    {
	      lock_unlock_shared_inst_lock (thread_p, tran_index, oid);
	    }
	  lock_unlock_shared_class_lock (thread_p, tran_index, class_oid);
	  break;

	case TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE:
	  /* Shared lock on an instance was never acquired */
	  /* remove shared class lock */
	  /* unlock the intentional lock if it is a shared lock */
	  if (OID_IS_ROOTOID (class_oid))
	    {
	      lock_unlock_shared_class_lock (thread_p, tran_index, oid);
	    }
	  lock_unlock_shared_class_lock (thread_p, tran_index, class_oid);
	  break;

	default:		/* TRAN_UNKNOWN_ISOLATION */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_UNKNOWN_ISOLATION,
		  2, isolation, tran_index);
	  break;
	}
    }
#endif /* !SERVER_MODE */
}

/*
 * lock_unlock_scan - Unlock scanning according to isolation level
 *
 * return: nothing..
 *
 *   class_oid(in): Class of the instance that were scanned
 *   scanid_bit(in): scanid bit for a scan
 *   scan_state(in):
 *
 * Note:Unlock the given "class_oid" scan according to the transaction
 *     isolation level. That is, the lock may not be released at this
 *     moment if the transaction isolation level does not allow it.
 */
void
lock_unlock_scan (THREAD_ENTRY * thread_p, const OID * class_oid,
		  int scanid_bit, bool scan_state)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int tran_index;		/* transaction table index */
  TRAN_ISOLATION isolation;	/* transaction isolation level */

  if (class_oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_unlock_scan", "NULL ClassOID pointer");
      return;
    }

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  isolation = logtb_find_isolation (tran_index);

  switch (isolation)
    {
    case TRAN_SERIALIZABLE:
      if (scanid_bit >= 0)
	{
	  lock_free_scanid_bit (thread_p, scanid_bit);
	}
      break;			/* nothing to do */
    case TRAN_REP_CLASS_REP_INSTANCE:
      if (scanid_bit >= 0)
	{
	  lock_demote_shared_inst_locks (thread_p, tran_index, scanid_bit);
	  lock_free_scanid_bit (thread_p, scanid_bit);
	}
      break;			/* nothing to do */

    case TRAN_REP_CLASS_COMMIT_INSTANCE:
      if (scanid_bit >= 0)
	{
	  lock_remove_all_inst_locks_with_scanid (thread_p, tran_index,
						  scanid_bit, S_LOCK);
	  lock_free_scanid_bit (thread_p, scanid_bit);
	}
      if (scan_state == END_SCAN)
	{
	  lock_demote_shared_class_lock (thread_p, tran_index, class_oid);
	}
      break;

    case TRAN_REP_CLASS_UNCOMMIT_INSTANCE:
      break;			/* nothing to do */

    case TRAN_COMMIT_CLASS_COMMIT_INSTANCE:
      if (scanid_bit >= 0)
	{
	  lock_remove_all_inst_locks_with_scanid (thread_p, tran_index,
						  scanid_bit, S_LOCK);
	  lock_free_scanid_bit (thread_p, scanid_bit);
	}
      if (scan_state == END_SCAN)
	{
	  lock_unlock_shared_class_lock (thread_p, tran_index, class_oid);
	}
      break;

    case TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE:
      if (scan_state == END_SCAN)
	{
	  lock_unlock_shared_class_lock (thread_p, tran_index, class_oid);
	}
      break;

    default:			/* TRAN_UNKNOWN_ISOLATION */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_UNKNOWN_ISOLATION, 2,
	      isolation, tran_index);
      break;
    }
#endif /* !SERVER_MODE */
}


/*
 * lock_unlock_classes_lock_hint - Unlock many hinted classes according to
 *                             transaction isolation level
 *
 * return: nothing..
 *
 *   lockhint(in): Description of hinted classses
 *
 */
void
lock_unlock_classes_lock_hint (THREAD_ENTRY * thread_p,
			       LC_LOCKHINT * lockhint)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int tran_index;		/* transaction table index */
  TRAN_ISOLATION isolation;	/* transaction isolation level */
  int i;

  if (lockhint == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_unlock_classes_lockhint", "NULL lockhint pointer");
      return;
    }

  /* If there is nothing to unlock, returns */
  if (lockhint->num_classes <= 0)
    {
      return;
    }

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  isolation = logtb_find_isolation (tran_index);

  switch (isolation)
    {
    case TRAN_SERIALIZABLE:
    case TRAN_REP_CLASS_REP_INSTANCE:
      return;			/* nothing to do */

    case TRAN_REP_CLASS_COMMIT_INSTANCE:
      /* class: demote shared class locks */
      for (i = 0; i < lockhint->num_classes; i++)
	{
	  if (OID_ISNULL (&lockhint->classes[i].oid)
	      || lockhint->classes[i].lock == NULL_LOCK)
	    {
	      continue;
	    }
	  lock_demote_shared_class_lock (thread_p, tran_index,
					 &lockhint->classes[i].oid);
	}
      return;

    case TRAN_REP_CLASS_UNCOMMIT_INSTANCE:
      return;			/* nothing to do */

    case TRAN_COMMIT_CLASS_COMMIT_INSTANCE:
    case TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE:
      /* class: remove shared class locks */
      for (i = 0; i < lockhint->num_classes; i++)
	{
	  if (OID_ISNULL (&lockhint->classes[i].oid)
	      || lockhint->classes[i].lock == NULL_LOCK)
	    {
	      continue;
	    }
	  lock_unlock_shared_class_lock (thread_p, tran_index,
					 &lockhint->classes[i].oid);
	}
      lock_unlock_shared_class_lock (thread_p, tran_index,
				     oid_Root_class_oid);
      break;

    default:			/* TRAN_UNKNOWN_ISOLATION */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_UNKNOWN_ISOLATION, 2,
	      isolation, tran_index);
      return;
    }
#endif /* !SERVER_MODE */
}

/*
 * lock_unlock_all - Release all locks of current transaction
 *
 * return: nothing
 *
 * Note:Release all locks acquired by the current transaction.
 *
 *      This function must be called at the end of the transaction.
 */
void
lock_unlock_all (THREAD_ENTRY * thread_p)
{
#if !defined (SERVER_MODE)
  lk_Standalone_has_xlock = false;
  pgbuf_unfix_all (thread_p);

  return;
#else /* !SERVER_MODE */
  int tran_index;
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *entry_ptr;
  int rv;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  tran_lock = &lk_Gl.tran_lock_table[tran_index];

  /* remove all instance locks */
  entry_ptr = tran_lock->inst_hold_list;
  while (entry_ptr != (LK_ENTRY *) NULL)
    {
      lock_internal_perform_unlock_object (thread_p, entry_ptr, true, false);
      entry_ptr = tran_lock->inst_hold_list;
    }

  /* remove all class locks */
  entry_ptr = tran_lock->class_hold_list;
  while (entry_ptr != (LK_ENTRY *) NULL)
    {
      lock_internal_perform_unlock_object (thread_p, entry_ptr, true, false);
      entry_ptr = tran_lock->class_hold_list;
    }

  /* remove root class lock */
  entry_ptr = tran_lock->root_class_hold;
  if (entry_ptr != NULL)
    {
      lock_internal_perform_unlock_object (thread_p, entry_ptr, true, false);
    }

  /* remove non2pl locks */
  while (tran_lock->non2pl_list != (LK_ENTRY *) NULL)
    {
      /* remove the non2pl entry from transaction non2pl list */
      entry_ptr = tran_lock->non2pl_list;
      tran_lock->non2pl_list = entry_ptr->tran_next;
      if (entry_ptr->granted_mode == INCON_NON_TWO_PHASE_LOCK)
	{
	  tran_lock->num_incons_non2pl -= 1;
	}
      /* remove the non2pl entry from resource non2pl list and free it */
      lock_remove_non2pl (entry_ptr, tran_index);
    }

  /* communication with deadlock detector */
  if (lk_Gl.TWFG_node[tran_index].checked_by_deadlock_detector)
    {
      lk_Gl.TWFG_node[tran_index].checked_by_deadlock_detector = false;
    }
  if (lk_Gl.TWFG_node[tran_index].DL_victim)
    {
      MUTEX_LOCK (rv, lk_Gl.DL_detection_mutex);
      lk_Gl.TWFG_node[tran_index].DL_victim = false;
      MUTEX_UNLOCK (lk_Gl.DL_detection_mutex);
    }

  pgbuf_unfix_all (thread_p);
#endif /* !SERVER_MODE */
}

/*
 * lock_unlock_by_isolation_level - Release some of the locks of the current
 *                              transaction according to its isolation  level
 *
 * return: nothing
 *
 * Note:Release some of the locks acquired by the current transaction
 *     according to its isolation level.
 */
void
lock_unlock_by_isolation_level (THREAD_ENTRY * thread_p)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int tran_index;		/* transaction table index */
  TRAN_ISOLATION isolation;	/* transaction isolation level */

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  isolation = logtb_find_isolation (tran_index);

  switch (isolation)
    {
    case TRAN_SERIALIZABLE:
    case TRAN_REP_CLASS_REP_INSTANCE:
      return;			/* Nothing to release */

    case TRAN_REP_CLASS_COMMIT_INSTANCE:
    case TRAN_REP_CLASS_UNCOMMIT_INSTANCE:
      /* remove all shared instance locks, demote all shared class locks */
      lock_remove_all_inst_locks (thread_p, tran_index, (OID *) NULL, S_LOCK);
      lock_demote_all_shared_class_locks (thread_p, tran_index);
      return;

    case TRAN_COMMIT_CLASS_COMMIT_INSTANCE:
    case TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE:
      /* remove all shared instance locks, remove all shared class locks */
      lock_remove_all_inst_locks (thread_p, tran_index, (OID *) NULL, S_LOCK);
      lock_remove_all_class_locks (thread_p, tran_index, S_LOCK);
      return;

    default:			/* TRAN_UNKNOWN_ISOLATION */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_UNKNOWN_ISOLATION, 2,
	      isolation, tran_index);
      return;
    }
#endif /* !SERVER_MODE */
}

/*
 * lock_demote_all_update_inst_locks -
 *
 * return: nothing
 */
void
lock_demote_all_update_inst_locks (THREAD_ENTRY * thread_p)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int tran_index;		/* transaction table index */
  TRAN_ISOLATION isolation;	/* transaction isolation level */
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *curr, *next;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  isolation = logtb_find_isolation (tran_index);
  if (LK_UNCOMMITTED_READ_ISOLATION (isolation))
    {
      return;			/* There is no update lock */
    }

  tran_lock = &lk_Gl.tran_lock_table[tran_index];

  switch (isolation)
    {
    case TRAN_SERIALIZABLE:
    case TRAN_REP_CLASS_REP_INSTANCE:
      /* demote U_LOCK to S_LOCK/NS_LOCK */
      for (curr = tran_lock->inst_hold_list; curr != (LK_ENTRY *) NULL;)
	{
	  next = curr->tran_next;
	  if (curr->granted_mode == U_LOCK)
	    {
	      (void) lock_internal_demote_update_inst_lock (thread_p, curr);
	    }
	  curr = next;
	}
      return;

    case TRAN_REP_CLASS_COMMIT_INSTANCE:
    case TRAN_COMMIT_CLASS_COMMIT_INSTANCE:
      /* remove U_LOCK */
      for (curr = tran_lock->inst_hold_list; curr != (LK_ENTRY *) NULL;)
	{
	  next = curr->tran_next;
	  if (curr->granted_mode == U_LOCK)
	    {
	      lock_internal_perform_unlock_object (thread_p, curr, true,
						   false);
	    }
	  curr = next;
	}
      return;

    default:			/* TRAN_UNKNOWN_ISOLATION */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_UNKNOWN_ISOLATION, 2,
	      isolation, tran_index);
      return;
    }
#endif /* !SERVER_MODE */
}

/*
 * lock_get_object_lock - Find the acquired lock mode
 *
 * return:
 *
 *   oid(in): target object ientifier
 *   class_oid(in): class identifier of the target object
 *   tran_index(in): the transaction table index of target transaction.
 *
 * Note:Find the acquired lock on the given object by the given transaction.
 *     Currently, the value of the "tran_index" must not be NULL_TRAN_INDEX.
 *
 *     If this function is changed in order to allow NULL_TRAN_INDEX to be
 *     transferred as the value of the "tran_index", this function will
 *     find the resulting lock acquired by all transactions holding a lock
 *     on the given OID (The most powerful lock, actually a combination of
 *     the locks).
 */
LOCK
lock_get_object_lock (const OID * oid, const OID * class_oid, int tran_index)
{
#if !defined (SERVER_MODE)
  return X_LOCK;
#else /* !SERVER_MODE */
  LOCK lock_mode = NULL_LOCK;	/* return value */
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *entry_ptr;
  int rv;

  if (oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_get_object_lock", "NULL OID pointer");
      return NULL_LOCK;
    }
  if (tran_index == NULL_TRAN_INDEX)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_get_object_lock", "NULL_TRAN_INDEX");
      return NULL_LOCK;
    }

  /* get a pointer to transaction lock info entry */
  tran_lock = &lk_Gl.tran_lock_table[tran_index];

  /*
   * case 1: root class lock
   */
  /* get the granted lock mode acquired on the root class oid */
  if (OID_EQ (oid, oid_Root_class_oid))
    {
      MUTEX_LOCK (rv, tran_lock->hold_mutex);
      if (tran_lock->root_class_hold != (LK_ENTRY *) NULL)
	{
	  lock_mode = tran_lock->root_class_hold->granted_mode;
	}
      MUTEX_UNLOCK (tran_lock->hold_mutex);
      return lock_mode;		/* might be NULL_LOCK */
    }

  /*
   * case 2: general class lock
   */
  /* get the granted lock mode acquired on the given class oid */
  if (class_oid == NULL || OID_EQ (class_oid, oid_Root_class_oid))
    {
      MUTEX_LOCK (rv, tran_lock->hold_mutex);
      entry_ptr = tran_lock->class_hold_list;
      for (; entry_ptr != (LK_ENTRY *) NULL; entry_ptr = entry_ptr->tran_next)
	{
	  if (OID_EQ (&entry_ptr->res_head->oid, oid))
	    {			/* found */
	      lock_mode = entry_ptr->granted_mode;
	      break;
	    }
	}
      MUTEX_UNLOCK (tran_lock->hold_mutex);
      return lock_mode;		/* might be NULL_LOCK */
    }

  MUTEX_LOCK (rv, tran_lock->hold_mutex);
  entry_ptr = tran_lock->class_hold_list;
  for (; entry_ptr != (LK_ENTRY *) NULL; entry_ptr = entry_ptr->tran_next)
    {
      if (OID_EQ (&entry_ptr->res_head->oid, class_oid))
	{			/* found */
	  lock_mode = entry_ptr->granted_mode;
	  break;
	}
    }
  /* If the class lock mode is S_LOCK or X_LOCK,
   * the lock is held on the instance implicitly.
   * In this case, there is no need to check instance lock.
   * If the class lock mode is SIX_LOCK,
   * S_LOCK is held on the instance implicitly.
   * In this case, we must check for a possible X_LOCK on the instance.
   * In other cases, we must check the lock held on the instance.
   */
  if (lock_mode != S_LOCK && lock_mode != X_LOCK)
    {
      if (lock_mode == SIX_LOCK)
	{
	  lock_mode = S_LOCK;
	}
      else
	{
	  lock_mode = NULL_LOCK;
	}
      entry_ptr = tran_lock->inst_hold_list;
      for (; entry_ptr != (LK_ENTRY *) NULL; entry_ptr = entry_ptr->tran_next)
	{
	  if (OID_EQ (&entry_ptr->res_head->oid, oid)
	      && OID_EQ (&entry_ptr->res_head->class_oid, class_oid))
	    {			/* found */
	      lock_mode = entry_ptr->granted_mode;
	      break;
	    }
	}
    }
  MUTEX_UNLOCK (tran_lock->hold_mutex);
  return lock_mode;		/* might be NULL_LOCK */
#endif /* !SERVER_MODE */
}

/*
 * lock_has_xlock - Does transaction have an exclusive lock on any resource ?
 *
 * return:
 *
 * Note:Find if the current transaction has any kind of exclusive lock
 *     on any lock resource.
 */
bool
lock_has_xlock (THREAD_ENTRY * thread_p)
{
#if !defined (SERVER_MODE)
  return lk_Standalone_has_xlock;
#else /* !SERVER_MODE */
  int tran_index;
  LK_TRAN_LOCK *tran_lock;
  LOCK lock_mode;
  LK_ENTRY *entry_ptr;
  int rv;

  /*
   * Exclusive locks in this context mean IX_LOCK, SIX_LOCK and X_LOCK.
   * NOTE that NX_LOCK and U_LOCK are excluded from exclusive locks.
   * Because, NX_LOCK is only for next-key locking and U_LOCK is currently
   * for reading the object.
   */
  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  MUTEX_LOCK (rv, tran_lock->hold_mutex);

  /* 1. check root class lock */
  if (tran_lock->root_class_hold != (LK_ENTRY *) NULL)
    {
      lock_mode = tran_lock->root_class_hold->granted_mode;
      if (lock_mode == X_LOCK || lock_mode == IX_LOCK
	  || lock_mode == SIX_LOCK)
	{
	  MUTEX_UNLOCK (tran_lock->hold_mutex);
	  return true;
	}
    }

  /* 2. check general class locks */
  entry_ptr = tran_lock->class_hold_list;
  while (entry_ptr != (LK_ENTRY *) NULL)
    {
      lock_mode = entry_ptr->granted_mode;
      if (lock_mode == X_LOCK || lock_mode == IX_LOCK
	  || lock_mode == SIX_LOCK)
	{
	  MUTEX_UNLOCK (tran_lock->hold_mutex);
	  return true;
	}
      entry_ptr = entry_ptr->tran_next;
    }

  /* 3. checking instance locks is not needed. According to MGL ptotocol,
   *    an exclusive class lock has been acquired with intention mode
   *    before an exclusive instance is acquired.
   */

  MUTEX_UNLOCK (tran_lock->hold_mutex);
  return false;
#endif /* !SERVER_MODE */
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * lock_has_lock_transaction - Does transaction have any lock on any resource ?
 *
 * return:
 *
 *   tran_index(in):
 *
 * Note:Find if given transaction has any kind of lock.
 *     Used by css_check_for_clients_down() to eliminate needless pinging.
 */
bool
lock_has_lock_transaction (int tran_index)
{
#if !defined (SERVER_MODE)
  return lk_Standalone_has_xlock;
#else /* !SERVER_MODE */
  LK_TRAN_LOCK *tran_lock;
  bool lock_hold;
  int rv;

  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  MUTEX_LOCK (rv, tran_lock->hold_mutex);
  if (tran_lock->root_class_hold != (LK_ENTRY *) NULL
      || tran_lock->class_hold_list != (LK_ENTRY *) NULL
      || tran_lock->inst_hold_list != (LK_ENTRY *) NULL
      || tran_lock->non2pl_list != (LK_ENTRY *) NULL)
    {
      lock_hold = true;
    }
  else
    {
      lock_hold = false;
    }
  MUTEX_UNLOCK (tran_lock->hold_mutex);

  return lock_hold;
#endif /* !SERVER_MODE */
}

/*
 * lock_is_waiting_transaction -
 *
 * return:
 *
 *   tran_index(in):
 */
bool
lock_is_waiting_transaction (int tran_index)
{
#if !defined (SERVER_MODE)
  return false;
#else /* !SERVER_MODE */
  THREAD_ENTRY *thrd_array[10];
  int thrd_count, i;
  THREAD_ENTRY *thrd_ptr;

  thrd_count = thread_get_lockwait_entry (tran_index, &thrd_array[0]);
  for (i = 0; i < thrd_count; i++)
    {
      thrd_ptr = thrd_array[i];
      (void) thread_lock_entry (thrd_ptr);
      if (LK_IS_LOCKWAIT_THREAD (thrd_ptr))
	{
	  (void) thread_unlock_entry (thrd_ptr);
	  return true;
	}
      else
	{
	  if (thrd_ptr->lockwait != NULL ||
	      thrd_ptr->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, thrd_ptr->lockwait,
		      thrd_ptr->lockwait_state, thrd_ptr->index,
		      thrd_ptr->tid, thrd_ptr->tran_index);
	    }
	}
      (void) thread_unlock_entry (thrd_ptr);
    }

  return false;
#endif /* !SERVER_MODE */
}
#endif

/*
 * lock_get_class_lock - Get a pointer to lock heap entry acquired by
 *                        given transaction on given class object
 *
 * return:
 *
 *   class_oid(in): target class object identifier
 *   tran_index(in): target transaction
 *
 * Note:This function finds lock entry acquired by the given transaction
 *     on the given class and then return a pointer to the lock entry.
 */
LK_ENTRY *
lock_get_class_lock (const OID * class_oid, int tran_index)
{
#if !defined (SERVER_MODE)
  return NULL;
#else /* !SERVER_MODE */
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *entry_ptr;
  int rv;

  if (class_oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_get_class_lock_ptr", "NULL ClassOID pointer");
      return NULL;
    }
  if (OID_ISNULL (class_oid))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_get_class_lock_ptr", "NULL_ClassOID");
      return NULL;
    }

  /* get a pointer to transaction lock info entry */
  tran_lock = &lk_Gl.tran_lock_table[tran_index];

  /* case 1: root class lock */
  if (OID_EQ (class_oid, oid_Root_class_oid))
    {
      MUTEX_LOCK (rv, tran_lock->hold_mutex);
      entry_ptr = tran_lock->root_class_hold;
      MUTEX_UNLOCK (tran_lock->hold_mutex);
      return entry_ptr;		/* might be NULL */
    }

  /* case 2: general class lock */
  MUTEX_LOCK (rv, tran_lock->hold_mutex);
  entry_ptr = tran_lock->class_hold_list;
  for (; entry_ptr != (LK_ENTRY *) NULL; entry_ptr = entry_ptr->tran_next)
    {
      if (OID_EQ (&entry_ptr->res_head->oid, class_oid))	/* found */
	{
	  break;
	}
    }
  MUTEX_UNLOCK (tran_lock->hold_mutex);
  return entry_ptr;		/* might be NULL */
#endif /* !SERVER_MODE */
}

/*
 * lock_force_timeout_lock_wait_transactions - All lock-wait transactions
 *                               are forced to timeout
 *
 * return: nothing
 *
 * Note:All lock-waiting transacions are forced to timeout.
 *     For this task, all lock-waiting threads are searched and
 *     then the threads are forced to timeout.
 */
void
lock_force_timeout_lock_wait_transactions (unsigned short stop_phase)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int i;
  THREAD_ENTRY *thrd;
  CSS_CONN_ENTRY *conn_p;

  for (i = 1; i < thread_num_total_threads (); i++)
    {
      thrd = thread_find_entry_by_index (i);

      conn_p = thrd->conn_entry;
      if ((stop_phase > THREAD_WORKER_STOP_PHASE_0 && conn_p == NULL) ||
	  (conn_p && conn_p->stop_phase != stop_phase))
	{
	  continue;
	}

      (void) thread_lock_entry (thrd);
      if (LK_IS_LOCKWAIT_THREAD (thrd))
	{
	  /* wake up the thread */
	  lock_resume ((LK_ENTRY *) thrd->lockwait, LOCK_RESUMED_TIMEOUT);
	}
      else
	{
	  if (thrd->lockwait != NULL
	      || thrd->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, thrd->lockwait,
		      thrd->lockwait_state, thrd->index, thrd->tid,
		      thrd->tran_index);
	    }
	  /* release the thread entry mutex */
	  (void) thread_unlock_entry (thrd);
	}
    }
  return;
#endif /* !SERVER_MODE */
}

/*
 * lock_force_timeout_expired_wait_transactions - Transaction is timeout if its waiting time has
 *                           expired or it is interrupted
 *
 * return: true if the thread was timed out or
 *                       false if the thread was not timed out.
 *
 *   thrd_entry(in): thread entry pointer
 *
 * Note:If the given thread is waiting on a lock to be granted, and
 *     either its expiration time has expired or it is interrupted,
 *     the thread is timed-out.
 *     If NULL is given, it applies to all threads.
 */
bool
lock_force_timeout_expired_wait_transactions (void *thrd_entry)
{
#if !defined (SERVER_MODE)
  return true;
#else /* !SERVER_MODE */
  int i;
  bool ignore;
  THREAD_ENTRY *thrd;

  if (thrd_entry != NULL)
    {
      thrd = (THREAD_ENTRY *) thrd_entry;
      (void) thread_lock_entry (thrd);
      if (LK_IS_LOCKWAIT_THREAD (thrd))
	{
	  struct timeval tv;
	  double etime;
	  (void) gettimeofday (&tv, NULL);
	  etime = ((double) tv.tv_sec * 1000000 + tv.tv_usec) / 1000;
	  if ((LK_CAN_TIMEOUT (thrd->lockwait_nsecs)
	       && etime - thrd->lockwait_stime > thrd->lockwait_nsecs)
	      || logtb_is_interrupted_tran (NULL, true, &ignore,
					    thrd->tran_index))
	    {
	      /* wake up the thread */
	      lock_resume ((LK_ENTRY *) thrd->lockwait, LOCK_RESUMED_TIMEOUT);
	      return true;
	    }
	  else
	    {
	      /* release the thread entry mutex */
	      (void) thread_unlock_entry (thrd);
	      return false;
	    }
	}
      else
	{
	  if (thrd->lockwait != NULL
	      || thrd->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, thrd->lockwait,
		      thrd->lockwait_state, thrd->index, thrd->tid,
		      thrd->tran_index);
	    }
	  /* release the thread entry mutex */
	  (void) thread_unlock_entry (thrd);
	  return false;
	}
    }
  else
    {
      for (i = 1; i < thread_num_total_threads (); i++)
	{
	  thrd = thread_find_entry_by_index (i);
	  (void) thread_lock_entry (thrd);
	  if (LK_IS_LOCKWAIT_THREAD (thrd))
	    {
	      struct timeval tv;
	      double etime;
	      (void) gettimeofday (&tv, NULL);
	      etime = ((double) tv.tv_sec * 1000000 + tv.tv_usec) / 1000;
	      if ((LK_CAN_TIMEOUT (thrd->lockwait_nsecs)
		   && etime - thrd->lockwait_stime > thrd->lockwait_nsecs)
		  || logtb_is_interrupted_tran (NULL, true, &ignore,
						thrd->tran_index))
		{
		  /* wake up the thread */
		  lock_resume ((LK_ENTRY *) thrd->lockwait,
			       LOCK_RESUMED_TIMEOUT);
		}
	      else
		{
		  /* release the thread entry mutex */
		  (void) thread_unlock_entry (thrd);
		}
	    }
	  else
	    {
	      if (thrd->lockwait != NULL
		  || thrd->lockwait_state == (int) LOCK_SUSPENDED)
		{
		  /* some strange lock wait state.. */
		  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			  ER_LK_STRANGE_LOCK_WAIT, 5, thrd->lockwait,
			  thrd->lockwait_state, thrd->index, thrd->tid,
			  thrd->tran_index);
		}
	      /* release the thread entry mutex */
	      (void) thread_unlock_entry (thrd);
	    }
	}
      return true;
    }
#endif /* !SERVER_MODE */
}

/*
 * lock_notify_isolation_incons - Notify of possible inconsistencies (no
 *                             repeatable reads) due to transaction isolation
 *                             level
 *
 * return: nothing.
 *
 *   fun(in): Function to notify
 *   args(in): Extra arguments for function
 *
 * Note:The current transaction is notified of any possible
 *              inconsistencies due to its isolation level. For each possible
 *              inconsistency the given function is called to decache any
 *              copies of the object.
 */
void
lock_notify_isolation_incons (THREAD_ENTRY * thread_p,
			      bool (*fun) (const OID * oid, void *args),
			      void *args)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  TRAN_ISOLATION isolation;
  int tran_index;
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *curr;
  LK_ENTRY *prev;
  bool ret_val;
  int rv;

  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  isolation = logtb_find_isolation (tran_index);
  if (isolation == TRAN_REP_CLASS_REP_INSTANCE
      || isolation == TRAN_SERIALIZABLE)
    {
      return;			/* Nothing was released */
    }

  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  MUTEX_LOCK (rv, tran_lock->non2pl_mutex);

  prev = (LK_ENTRY *) NULL;
  curr = tran_lock->non2pl_list;
  while (tran_lock->num_incons_non2pl > 0 && curr != (LK_ENTRY *) NULL)
    {
      if (curr->granted_mode != INCON_NON_TWO_PHASE_LOCK)
	{
	  prev = curr;
	  curr = curr->tran_next;
	  continue;
	}
      /* curr->granted_mode == INCON_NON_TWO_PHASE_LOCK */
      ret_val = (*fun) (&curr->res_head->oid, args);
      if (ret_val != true)
	{			/* the notification area is full */
	  MUTEX_UNLOCK ((tran_lock->non2pl_mutex));
	  return;		/* stop */
	}

      /* ret_val == true: the non-2pl entry should be freed. */
      /* 1. remove it from transaction non2pl list */
      if (prev == (LK_ENTRY *) NULL)
	{
	  tran_lock->non2pl_list = curr->tran_next;
	}
      else
	{
	  prev->tran_next = curr->tran_next;
	}

      if (curr->granted_mode == INCON_NON_TWO_PHASE_LOCK)
	{
	  tran_lock->num_incons_non2pl -= 1;
	}

      /* 2. remove it from resource non2pl list and free it */
      lock_remove_non2pl (curr, tran_index);
    }

  /* release transaction non2pl mutex */
  MUTEX_UNLOCK (tran_lock->non2pl_mutex);
  return;
#endif /* !SERVER_MODE */
}

/*
 * lock_check_local_deadlock_detection - Check local deadlock detection interval
 *
 * return:
 *
 * Note:check if the local deadlock detection should be performed.
 */
bool
lock_check_local_deadlock_detection (void)
{
#if !defined (SERVER_MODE)
  return false;
#else /* !SERVER_MODE */
  /* check deadlock detection interval */
  if (difftime (time (NULL), lk_Gl.last_deadlock_run)
      < PRM_LK_RUN_DEADLOCK_INTERVAL)
    {
      return false;
    }
  else
    {
      return true;
    }
#endif /* !SERVER_MODE */
}

/*
 * lock_detect_local_deadlock - Run the local deadlock detection
 *
 * return: nothing
 *
 * Note:Run the deadlock detection. For every cycle either timeout or
 *     abort a transaction. The timeout option is always preferred over
 *     the unilaterally abort option. When the unilaterally abort option
 *     is exercised, the youngest transaction in the cycle is selected.
 *     The youngest transaction is hopefully the one that has done less work.
 *
 *     First, allocate heaps for WFG table from local memory.
 *     Check whether deadlock(s) have been occurred or not.
 *
 *     Deadlock detection is peformed via exhaustive loop construction
 *     which indicates the wait-for-relationship.
 *     If deadlock is detected,
 *     the first transaction which enables a cycle
 *     when scanning from the first of object lock table to the last of it.
 *
 *     The deadlock of victims are waken up and aborted by themselves.
 *
 *     Last, free WFG framework.
 */
void
lock_detect_local_deadlock (THREAD_ENTRY * thread_p)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int k, s, t;
  LK_RES_BLOCK *res_block;
  LK_RES *res_ptr;
  LK_ENTRY *hi, *hj;
  LK_WFG_NODE *TWFG_node;
  LK_WFG_EDGE *TWFG_edge;
  int i, rv;
  int compat1, compat2;

  /* initialize deadlock detection related structures */

  /* initialize transaction WFG node table..
   * The current transaction might be old deadlock victim.
   * And, the transaction may have not been aborted, until now.
   * Even if the transaction(old deadlock victim) has not been aborted,
   * set checked_by_deadlock_detector of the transaction to true.
   */
  for (i = 1; i < lk_Gl.num_trans; i++)
    {
      lk_Gl.TWFG_node[i].first_edge = -1;
      lk_Gl.TWFG_node[i].tran_edge_seq_num = 0;
      lk_Gl.TWFG_node[i].checked_by_deadlock_detector = true;
    }

  /* initialize transaction WFG edge table */
  lk_Gl.TWFG_edge = &TWFG_edge_block[0];
  lk_Gl.max_TWFG_edge = LK_MIN_TWFG_EDGE_COUNT;	/* initial value */
  for (i = 0; i < LK_MIN_TWFG_EDGE_COUNT; i++)
    {
      lk_Gl.TWFG_edge[i].to_tran_index = -1;
      lk_Gl.TWFG_edge[i].next = (i + 1);
    }
  lk_Gl.TWFG_edge[lk_Gl.max_TWFG_edge - 1].next = -1;
  lk_Gl.TWFG_free_edge_idx = 0;

  /* initialize global_edge_seq_num */
  lk_Gl.global_edge_seq_num = 0;

  /* initialize victim count */
  victim_count = 0;		/* used as index of victims array */

  /* hold the deadlock detection mutex */
  MUTEX_LOCK (rv, lk_Gl.DL_detection_mutex);

  /* lock-wait edge construction for object lock resource table */
  MUTEX_LOCK (rv, lk_Gl.obj_res_block_list_mutex);

  for (res_block = lk_Gl.obj_res_block_list;
       res_block != NULL; res_block = res_block->next_block)
    {
      for (k = 0; k < res_block->count; k++)
	{
	  /* If the holder list is empty, then the waiter list is also empty.
	   * Therefore, res_block->block[k].waiter == NULL cannot be checked.
	   */
	  if (res_block->block[k].holder == (LK_ENTRY *) NULL)
	    {
	      if (res_block->block[k].waiter == (LK_ENTRY *) NULL)
		{
		  continue;
		}
	    }

	  res_ptr = &res_block->block[k];

	  /* hold resource mutex */
	  MUTEX_LOCK (rv, res_ptr->res_mutex);

	  if (res_ptr->holder == NULL)
	    {
	      if (res_ptr->waiter == NULL)
		{
		  MUTEX_UNLOCK (res_ptr->res_mutex);
		  continue;
		}
	      else
		{
#if defined(CUBRID_DEBUG)
		  FILE *lk_fp;
		  time_t cur_time;

		  lk_fp = fopen ("lock_waiter_only_info.log", "a")
		    if (lk_fp != NULL)
		    {
		      cur_time = time (NULL);
		      fprintf (lk_fp,
			       "##########################################\n");
		      fprintf (lk_fp, "# current time: %s\n",
			       ctime (&cur_time));
		      lock_dump_resource (lk_fp, res_ptr);
		      fprintf (lk_fp,
			       "##########################################\n");
		      fclose (lk_fp);
		    }
#endif /* CUBRID_DEBUG */
		  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			  ER_LK_LOCK_WAITER_ONLY, 1,
			  "lock_waiter_only_info.log");

		  if (res_ptr->total_holders_mode != NULL_LOCK)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_LK_TOTAL_HOLDERS_MODE, 1,
			      res_ptr->total_holders_mode);
		      res_ptr->total_holders_mode = NULL_LOCK;
		    }
		  (void) lock_grant_blocked_waiter (thread_p, res_ptr);
		}
	    }

	  /* among holders */
	  for (hi = res_ptr->holder; hi != NULL; hi = hi->next)
	    {
	      if (hi->blocked_mode == NULL_LOCK)
		{
		  break;
		}
	      for (hj = hi->next; hj != NULL; hj = hj->next)
		{
		  assert (hi->granted_mode >= NULL_LOCK
			  && hi->blocked_mode >= NULL_LOCK);
		  assert (hj->granted_mode >= NULL_LOCK
			  && hj->blocked_mode >= NULL_LOCK);

		  compat1 = lock_Comp[hj->blocked_mode][hi->granted_mode];
		  compat2 = lock_Comp[hj->blocked_mode][hi->blocked_mode];
		  assert (compat1 != DB_NA && compat2 != DB_NA);

		  if (compat1 == false || compat2 == false)
		    {
		      (void) lock_add_WFG_edge (hj->tran_index,
						hi->tran_index, true,
						(time_t) (hj->thrd_entry->
							  lockwait_stime /
							  1000));
		    }

		  compat1 = lock_Comp[hi->blocked_mode][hj->granted_mode];
		  assert (compat1 != DB_NA);

		  if (compat1 == false)
		    {
		      (void) lock_add_WFG_edge (hi->tran_index,
						hj->tran_index, true,
						(time_t) (hi->thrd_entry->
							  lockwait_stime /
							  1000));
		    }
		}
	    }

	  /* from waiters in the waiter to holders */
	  for (hi = res_ptr->holder; hi != NULL; hi = hi->next)
	    {
	      for (hj = res_ptr->waiter; hj != NULL; hj = hj->next)
		{
		  assert (hi->granted_mode >= NULL_LOCK
			  && hi->blocked_mode >= NULL_LOCK);
		  assert (hj->granted_mode >= NULL_LOCK
			  && hj->blocked_mode >= NULL_LOCK);

		  compat1 = lock_Comp[hj->blocked_mode][hi->granted_mode];
		  compat2 = lock_Comp[hj->blocked_mode][hi->blocked_mode];
		  assert (compat1 != DB_NA && compat2 != DB_NA);

		  if (compat1 == false || compat2 == false)
		    {
		      (void) lock_add_WFG_edge (hj->tran_index,
						hi->tran_index, true,
						(time_t) (hj->thrd_entry->
							  lockwait_stime /
							  1000));
		    }
		}
	    }

	  /* from waiters in the waiter to other waiters in the waiter */
	  for (hi = res_ptr->waiter; hi != NULL; hi = hi->next)
	    {
	      for (hj = hi->next; hj != NULL; hj = hj->next)
		{
		  assert (hj->blocked_mode >= NULL_LOCK
			  && hi->blocked_mode >= NULL_LOCK);

		  compat1 = lock_Comp[hj->blocked_mode][hi->blocked_mode];
		  assert (compat1 != DB_NA);

		  if (compat1 == false)
		    {
		      (void) lock_add_WFG_edge (hj->tran_index,
						hi->tran_index, false,
						(time_t) (hj->thrd_entry->
							  lockwait_stime /
							  1000));
		    }
		}
	    }

	  /* release resource mutex */
	  MUTEX_UNLOCK (res_ptr->res_mutex);
	}
    }

  MUTEX_UNLOCK (lk_Gl.obj_res_block_list_mutex);

  /* release DL detection mutex */
  MUTEX_UNLOCK (lk_Gl.DL_detection_mutex);

  /* simple notation for using in the following statements */
  TWFG_node = lk_Gl.TWFG_node;
  TWFG_edge = lk_Gl.TWFG_edge;

  /*
   * deadlock detection and victim selection
   */

  for (k = 1; k < lk_Gl.num_trans; k++)
    {
      TWFG_node[k].current = TWFG_node[k].first_edge;
      TWFG_node[k].ancestor = -1;
    }
  for (k = 1; k < lk_Gl.num_trans; k++)
    {
      if (TWFG_node[k].current == -1)
	{
	  continue;
	}
      s = k;
      TWFG_node[s].ancestor = -2;
      for (; s != -2;)
	{
	  if (TWFG_node[s].checked_by_deadlock_detector == false
	      || TWFG_node[s].thrd_wait_stime == 0
	      || (TWFG_node[s].current != -1
		  && (TWFG_node[s].thrd_wait_stime >
		      TWFG_edge[TWFG_node[s].current].edge_wait_stime)))
	    {
	      /* A new transaction started */
	      TWFG_node[s].first_edge = -1;
	      TWFG_node[s].current = -1;
	    }

	  if (TWFG_node[s].current == -1)
	    {
	      t = TWFG_node[s].ancestor;
	      TWFG_node[s].ancestor = -1;
	      s = t;
	      if (s != -2)
		{
		  TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
		}
	      continue;
	    }

	  t = TWFG_edge[TWFG_node[s].current].to_tran_index;

	  if (t == -2)
	    {			/* old WFG edge */
	      TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
	      continue;
	    }

	  if (TWFG_node[t].current == -1)
	    {
	      TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	      TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
	      continue;
	    }

	  if (TWFG_node[t].checked_by_deadlock_detector == false
	      || TWFG_node[t].thrd_wait_stime == 0
	      || TWFG_node[t].thrd_wait_stime >
	      TWFG_edge[TWFG_node[t].current].edge_wait_stime)
	    {
	      TWFG_node[t].first_edge = -1;
	      TWFG_node[t].current = -1;
	      TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	      TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
	      continue;
	    }

	  if (TWFG_edge[TWFG_node[s].current].edge_seq_num <
	      TWFG_node[t].tran_edge_seq_num)
	    {			/* old WFG edge */
	      TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	      TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
	      continue;
	    }

	  if (TWFG_node[t].ancestor != -1)
	    {
	      /* A deadlock cycle is found */
	      lock_select_deadlock_victim (thread_p, s, t);
	      if (victim_count >= LK_MAX_VICTIM_COUNT)
		{
		  goto final;
		}
	    }
	  else
	    {
	      TWFG_node[t].ancestor = s;
	      TWFG_node[t].candidate =
		TWFG_edge[TWFG_node[s].current].holder_flag;
	    }
	  s = t;
	}
    }

final:

#if defined(SERVER_MODE) && defined(DIAG_DEVEL)
  if (victim_count > 0)
    {
      SET_DIAG_VALUE (diag_executediag, DIAG_OBJ_TYPE_LOCK_DEADLOCK, 1,
		      DIAG_VAL_SETTYPE_INC, NULL);
#if 0				/* ACTIVITY PROFILE */
      ADD_ACTIVITY_DATA (diag_executediag,
			 DIAG_EVENTCLASS_TYPE_SERVER_LOCK_DEADLOCK, "", "",
			 victim_count);
#endif
    }
#endif /* SERVER_MODE && DIAG_DEVEL */

  /* dump deadlock victims according to dump level */
  if (victim_count > 0 && lk_Gl.dump_level_when_deadlock)
    {
      lock_dump_deadlock_victims (thread_p);
    }


  /* Now solve the deadlocks (cycles) by executing the cycle resolution
   * function (e.g., aborting victim)
   */
  for (k = 0; k < victim_count; k++)
    {
      if (victims[k].can_timeout)
	{
	  (void) lock_wakeup_deadlock_victim_timeout (victims[k].tran_index);
	}
      else
	{
	  (void) lock_wakeup_deadlock_victim_aborted (victims[k].tran_index);
	}
    }

  /* deallocate memory space used for deadlock detection */
  if (lk_Gl.max_TWFG_edge > LK_MID_TWFG_EDGE_COUNT)
    {
      free_and_init (lk_Gl.TWFG_edge);
    }

  if (victim_count == 0)
    {
      if (lk_Gl.no_victim_case_count < 60)
	{
	  lk_Gl.no_victim_case_count += 1;
	}
      else
	{
	  int worker_threads = 0;
	  int suspended_threads = 0;
	  int thrd_index;
	  THREAD_ENTRY *thrd_ptr;

	  /* Make sure that we have threads available for another client
	   * to execute, otherwise Panic...
	   */
	  thread_get_info_threads (NULL, &worker_threads, NULL,
				   &suspended_threads);
	  if (worker_threads == suspended_threads)
	    {
	      /* We must timeout at least one thread, so other clients can execute,
	       * otherwise, the server will hang.
	       */
	      thrd_ptr = thread_find_first_lockwait_entry (&thrd_index);
	      while (thrd_ptr != NULL)
		{
		  if (lock_wakeup_deadlock_victim_timeout
		      (thrd_ptr->tran_index) == true)
		    {
		      break;
		    }
		  thrd_ptr = thread_find_next_lockwait_entry (&thrd_index);
		}

	      if (thrd_ptr != NULL)
		{
		  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
			  ER_LK_NOTENOUGH_ACTIVE_THREADS, 3, worker_threads,
			  logtb_get_number_assigned_tran_indices (),
			  thrd_ptr->tran_index);
		}
	    }
	  lk_Gl.no_victim_case_count = 0;
	}
    }

  /* save the last deadlock run time */
  lk_Gl.last_deadlock_run = time (NULL);

  return;
#endif /* !SERVER_MODE */
}

#if 0				/* NOT_USED */
/*
 */

/*
 * lk_global_deadlock_detection: RUN THE GLOBAL DEADLOCK DETECTION
 * arguments:
 * returns/side-effects: nothing
 * Note: Run the deadlock detection. For every cycle either timeout or
 *              abort a transaction. The timeout option is always preferred
 *              over the unilaterally abort option. When the unilaterally
 *              abort option is exercised, the youngest transaction in the
 *              cycle is selected. The youngest transaction is hopefully the
 *              one that has done less work.
 */
void
lk_global_deadlock_detection (void)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int i, j;
  WFG_CYCLE *cycles, *cur_cycle;
  WFG_CYCLE_CASE cycle_case;
  int num_victims;
  int tot_num_victims = 0;
  LK_DEADLOCK_VICTIM victims[LK_MAX_VICTIM_COUNT];
  LK_DEADLOCK_VICTIM *v_p;
  int tran_index;
  TRANID tranid;
  int can_timeout;
  int already_picked;
  int ok;
  int error;
  bool isvictim_tg_waiting;
  bool iscandidate_tg_waiting;

  /* 1. Find all the cycles that are currently involved in the system */
  cycle_case = WFG_CYCLE_YES_PRUNE;
  while (cycle_case == WFG_CYCLE_YES_PRUNE)
    {
      error = wfg_detect_cycle (&cycle_case, &cycles);

      if (error == NO_ERROR
	  && (cycle_case == WFG_CYCLE_YES_PRUNE
	      || cycle_case == WFG_CYCLE_YES))
	{
	  /* There are deadlocks, we must select a victim for each cycle.
	     We try to break a cycle by timing out a transaction whenever
	     is possible.
	     In any other case, we select a victim for an unilaterally abort.
	   */
	  num_victims = 0;
	  for (cur_cycle = cycles;
	       cur_cycle != NULL && num_victims < LK_MAX_VICTIM_COUNT;
	       cur_cycle = cur_cycle->next)
	    {
	      victims[num_victims].tran_index = NULL_TRAN_INDEX;
	      victims[num_victims].can_timeout = false;
	      already_picked = false;

	      /* Pick a victim for next cycle */
	      for (i = 0;
		   i < cur_cycle->num_trans && already_picked == false; i++)
		{
		  tran_index = cur_cycle->waiters[i].tran_index;
		  for (j = 0; j < num_victims; j++)
		    {
		      if (tran_index == victims[j].tran_index)
			{
			  /* A victim for this cycle has already been picked.
			     The index is part of another cycle */
			  already_picked = true;
			  break;
			}
		    }
		  if (already_picked != true)
		    {
		      tranid = logtb_find_tranid (tran_index);
		      can_timeout =
			LK_CAN_TIMEOUT (logtb_find_wait_secs (tran_index));
		      /* Victim selection:
		         1) Avoid unactive transactions.
		         2) Prefer a waiter of TG resources.
		         3) Prefer a transaction with a closer tiemout.
		         4) Prefer the youngest transaction.
		       */
		      /* Have we selected a victim or the currently victim is inactive
		         (i.e., in rollback or commit process), select the new candidate
		         as the victim.
		       */
		      ok = 0;

		      /*
		       * never consider the unactive one as a victim
		       */
		      if (logtb_is_active (tranid) == false)
			continue;

		      if (victims[num_victims].tran_index == NULL_TRAN_INDEX
			  || (logtb_is_active (victims[num_victims].tranid) ==
			      false && logtb_is_active (tranid) != false))
			{
			  ok = 1;
			}
		      else
			{
			  isvictim_tg_waiting =
			    wfg_is_tran_group_waiting (victims[num_victims].
						       tran_index);

			  iscandidate_tg_waiting =
			    wfg_is_tran_group_waiting (tran_index);

			  if (isvictim_tg_waiting != NO_ERROR)
			    {
			      if (iscandidate_tg_waiting == NO_ERROR
				  || (victims[num_victims].can_timeout ==
				      false
				      && can_timeout == true)
				  || (victims[num_victims].can_timeout ==
				      can_timeout
				      && LK_ISYOUNGER (tranid,
						       victims[num_victims].
						       tranid)))
				{
				  ok = 1;
				}
			    }
			  else
			    {
			      if (iscandidate_tg_waiting == NO_ERROR &&
				  ((victims[num_victims].can_timeout ==
				    false
				    && can_timeout == true)
				   || (victims[num_victims].can_timeout ==
				       can_timeout
				       && LK_ISYOUNGER (tranid,
							victims[num_victims].
							tranid))))
				{
				  ok = 1;
				}
			    }
			}

		      if (ok == 1)
			{
			  victims[num_victims].tran_index = tran_index;
			  victims[num_victims].tranid = tranid;
			  victims[num_victims].can_timeout = can_timeout;
			  victims[num_victims].cycle_fun =
			    cur_cycle->waiters[i].cycle_fun;
			  victims[num_victims].args =
			    cur_cycle->waiters[i].args;
			}
		    }
		}
	      if (already_picked != true
		  && victims[num_victims].tran_index != NULL_TRAN_INDEX)
		{
		  num_victims++;
		}
	    }

	  /* Now, solve the deadlocks (cycles)
	     by executing the cycle resolution function (e.g., aborting victim)
	   */
	  for (i = 0; i < num_victims; i++)
	    {
	      *v_p = victims[i];
	      if (v_p->cycle_fun != NULL)
		{
		  /* There is a function to solve the cycle. */
		  if ((*v_p->cycle_fun) (v_p->tran_index,
					 v_p->args) == NO_ERROR)
		    ok = true;
		  else
		    ok = false;
		}
	      else
		{
		  ok = false;
		}

	      /* If a function to break the cycle was not provided or the function
	         failed, the transaction is aborted/timed-out
	       */
	      if (ok == false)
		{
		  if (v_p->can_timeout == false)
		    {
		      if (lock_wakeup_deadlock_victim_aborted
			  (v_p->tran_index) == false)
			msql_tm_abort_detected (v_p->tran_index, NULL);
		    }
		  else
		    {
		      if (lock_wakeup_deadlock_victim_timeout
			  (v_p->tran_index) == false)
			msql_tm_timeout_detected (v_p->tran_index, NULL);
		    }
		}
	    }
	  wfg_free_cycle (cycles);

	  tot_num_victims += num_victims;

	  if (num_victims >= LK_MAX_VICTIM_COUNT)
	    cycle_case = WFG_CYCLE_YES_PRUNE;
	}
    }
#endif /* !SERVER_MODE */
}
#endif /* NOT_USED */

/*
 * lock_reacquire_crash_locks - Reacquire given (exclusive) locks
 *
 * return: returns one value of following three:
 *     (LK_GRANTED, LK_NOTGRANTED_DUE_TIMEOUT, LK_NOTGRANTED_DUE_ERROR)
 *
 *   acqlocks(in): list of locks to be acquired
 *   tran_index(in): transaction index
 *                 whose transaction needs to obtain the given locks
 *
 * Note:This function acquires locks (likely exclusive locks) which were
 *     acquired before a crash on behalf of the specified transaction.
 *
 *     Note: This function should only be called during recovery restart
 *           time. The function does not try to get all or none of the locks
 *           since they have already been granted to the transaction before
 *           the crash. If a lock cannot be granted, an error is set and
 *           returned, however, the fucntion will not stop acquiring the rest
 *           of the indicated locks.
 */
int
lock_reacquire_crash_locks (THREAD_ENTRY * thread_p,
			    LK_ACQUIRED_LOCKS * acqlocks, int tran_index)
{
#if !defined (SERVER_MODE)
  return LK_GRANTED;
#else /* !SERVER_MODE */
  int granted = LK_GRANTED, r;
  unsigned int i;
  LK_ENTRY *dummy_ptr;

  if (acqlocks == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_reacquire_crash_locks", "NULL acqlocks pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }

  /* reacquire given exclusive locks on behalf of the transaction */
  for (i = 0; i < acqlocks->nobj_locks; i++)
    {
      /*
       * lock wait duration       : LK_INFINITE_WAIT
       * conditional lock request : false
       */
      r =
	lock_internal_perform_lock_object (thread_p, tran_index,
					   &acqlocks->obj[i].oid,
					   &acqlocks->obj[i].class_oid,
					   acqlocks->obj[i].lock,
					   LK_INFINITE_WAIT, &dummy_ptr,
					   NULL);
      if (r != LK_GRANTED)
	{
	  er_log_debug (ARG_FILE_LINE,
			"lk_reacquire_crash_locks: The lock cannot be reacquired...");
	  granted = r;
	  continue;
	}
    }
  return granted;
#endif /* !SERVER_MODE */
}

/*
 * lock_unlock_all_shared_get_all_exclusive - Release all shared type locks and
 *                              optionally list the exclusive type locks
 *
 * return: nothing
 *
 *   acqlocks(in/out):Get the list of acquired exclusive locks or NULL
 *
 * Note:Release all shared type locks (i.e., S_LOCK, IS_LOCK, SIX_LOCK
 *     -- demoted to IX_LOCK), and obtain all remianing locks (i.e.,
 *     exclusive locks such as IX_LOCK, X_LOCK).
 *
 *     Note: This function must be called during the two phase commit
 *           protocol of a distributed transaction.
 */
void
lock_unlock_all_shared_get_all_exclusive (THREAD_ENTRY * thread_p,
					  LK_ACQUIRED_LOCKS * acqlocks)
{
#if !defined (SERVER_MODE)
  /* No locks in standalone */
  if (acqlocks != (LK_ACQUIRED_LOCKS *) NULL)
    {
      acqlocks->nobj_locks = 0;
      acqlocks->obj = NULL;
    }
  return;
#else /* !SERVER_MODE */
  int tran_index;
  LK_TRAN_LOCK *tran_lock;
  int idx;
  LK_ENTRY *entry_ptr;
  int rv;

  /* some preparation */
  tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);

  /************************************/
  /* phase 1: unlock all shared locks */
  /************************************/
  lock_demote_all_shared_class_locks (thread_p, tran_index);
  lock_remove_all_inst_locks (thread_p, tran_index, (OID *) NULL, S_LOCK);
  lock_remove_all_class_locks (thread_p, tran_index, S_LOCK);

  /************************************/
  /* phase 2: get all exclusive locks */
  /************************************/
  if (acqlocks != (LK_ACQUIRED_LOCKS *) NULL)
    {
      /* get a pointer to transaction lock info entry */
      tran_lock = &lk_Gl.tran_lock_table[tran_index];

      /* hold transction lock hold mutex */
      MUTEX_LOCK (rv, tran_lock->hold_mutex);

      /* get nobj_locks */
      acqlocks->nobj_locks = (unsigned int) (tran_lock->class_hold_count +
					     tran_lock->inst_hold_count);
      if (tran_lock->root_class_hold != (LK_ENTRY *) NULL)
	{
	  acqlocks->nobj_locks += 1;
	}

      /* allocate momory space for saving exclusive lock information */
      acqlocks->obj =
	(LK_ACQOBJ_LOCK *) malloc (SIZEOF_LK_ACQOBJ_LOCK *
				   acqlocks->nobj_locks);
      if (acqlocks->obj == (LK_ACQOBJ_LOCK *) NULL)
	{
	  MUTEX_UNLOCK (tran_lock->hold_mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (SIZEOF_LK_ACQOBJ_LOCK * acqlocks->nobj_locks));
	  acqlocks->nobj_locks = 0;
	  return;
	}

      /* initialize idx in acqlocks->obj array */
      idx = 0;

      /* collect root class lock information */
      entry_ptr = tran_lock->root_class_hold;
      if (entry_ptr != (LK_ENTRY *) NULL)
	{
	  COPY_OID (&acqlocks->obj[idx].oid, oid_Root_class_oid);
	  OID_SET_NULL (&acqlocks->obj[idx].class_oid);
	  acqlocks->obj[idx].lock = entry_ptr->granted_mode;
	  idx += 1;
	}

      /* collect general class lock information */
      entry_ptr = tran_lock->class_hold_list;
      for (; entry_ptr != (LK_ENTRY *) NULL; entry_ptr = entry_ptr->tran_next)
	{
	  COPY_OID (&acqlocks->obj[idx].oid, &entry_ptr->res_head->oid);
	  COPY_OID (&acqlocks->obj[idx].class_oid, oid_Root_class_oid);
	  acqlocks->obj[idx].lock = entry_ptr->granted_mode;
	  idx += 1;
	}

      /* collect instance lock information */
      entry_ptr = tran_lock->inst_hold_list;
      for (; entry_ptr != (LK_ENTRY *) NULL; entry_ptr = entry_ptr->tran_next)
	{
	  COPY_OID (&acqlocks->obj[idx].oid, &entry_ptr->res_head->oid);
	  COPY_OID (&acqlocks->obj[idx].class_oid,
		    &entry_ptr->res_head->class_oid);
	  acqlocks->obj[idx].lock = entry_ptr->granted_mode;
	  idx += 1;
	}

      /* release transaction lock hold mutex */
      MUTEX_UNLOCK (tran_lock->hold_mutex);
    }
  return;
#endif /* !SERVER_MODE */
}

/*
 * lock_dump_acquired - Dump structure of acquired locks
 *
 * return: nothing
 *
 *   acqlocks(in): The acquired locks
 *
 * Note:Dump the structure of acquired locks
 */
void
lock_dump_acquired (FILE * fp, LK_ACQUIRED_LOCKS * acqlocks)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  unsigned int i;

  /* Dump object locks */
  if (acqlocks->obj != NULL && acqlocks->nobj_locks > 0)
    {
      fprintf (fp, "Object_locks: count = %d\n", acqlocks->nobj_locks);
      for (i = 0; i < acqlocks->nobj_locks; i++)
	{
	  fprintf (fp, "   |%d|%d|%d| %s\n", acqlocks->obj[i].oid.volid,
		   acqlocks->obj[i].oid.pageid, acqlocks->obj[i].oid.slotid,
		   LOCK_TO_LOCKMODE_STRING (acqlocks->obj[i].lock));
	}
    }
#endif /* !SERVER_MODE */
}

/*
 * xlock_dump - Dump the contents of lock table
 *
 * return: nothing
 *
 *   outfp(in): FILE stream where to dump the lock table. If NULL is given,
 *            it is dumped to stdout.
 *
 * Note:Dump the lock and waiting tables for both objects and pages.
 *              That is, the lock activity of the datbase. It may be useful
 *              for finding concurrency problems and locking bottlenecks on
 *              an application, so that you can set the appropiate isolation
 *              level or modify the design of the application.
 */
void
xlock_dump (THREAD_ENTRY * thread_p, FILE * outfp)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  char *client_prog_name;	/* Client program name for tran */
  char *client_user_name;	/* Client user name for tran    */
  char *client_host_name;	/* Client host for tran         */
  int client_pid;		/* Client process id for tran   */
  TRAN_ISOLATION isolation;	/* Isolation for client tran    */
  TRAN_STATE state;
  int waitsecs;
  int old_waitsecs = 0;		/* Old transaction lock wait    */
  int tran_index;
  int num_res;
  LK_RES_BLOCK *res_block;
  int hash_index;
  LK_HASH *hash_anchor;
  LK_RES *res_ptr;
  LK_RES *res_prev;
  int rv;

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				  MSGCAT_SET_LOCK, MSGCAT_LK_NEWLINE));
  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				  MSGCAT_SET_LOCK,
				  MSGCAT_LK_DUMP_LOCK_TABLE),
	   PRM_LK_ESCALATION_AT, PRM_LK_RUN_DEADLOCK_INTERVAL);

  /* Don't get block from anything when dumping object lock table. */
  old_waitsecs = xlogtb_reset_wait_secs (thread_p, LK_FORCE_ZERO_WAIT);

  /* Dump some information about all transactions */
  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				  MSGCAT_SET_LOCK, MSGCAT_LK_NEWLINE));
  for (tran_index = 0; tran_index < lk_Gl.num_trans; tran_index++)
    {
      if (logtb_find_client_name_host_pid (tran_index, &client_prog_name,
					   &client_user_name,
					   &client_host_name,
					   &client_pid) != NO_ERROR)
	{
	  /* Likely this index is not assigned */
	  continue;
	}
      isolation = logtb_find_isolation (tran_index);
      state = logtb_find_state (tran_index);
      waitsecs = logtb_find_wait_secs (tran_index);

      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_DUMP_TRAN_IDENTIFIERS),
	       tran_index, client_prog_name, client_user_name,
	       client_host_name, client_pid);
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_DUMP_TRAN_ISOLATION),
	       log_isolation_string (isolation));
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_DUMP_TRAN_STATE),
	       log_state_string (state));
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_DUMP_TRAN_TIMEOUT_PERIOD),
	       waitsecs);
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_CUBRID,
				      MSGCAT_SET_LOCK, MSGCAT_LK_NEWLINE));
    }

  /* dump object lock table */
  num_res = 0;
  MUTEX_LOCK (rv, lk_Gl.obj_res_block_list_mutex);
  res_block = lk_Gl.obj_res_block_list;
  while (res_block != (LK_RES_BLOCK *) NULL)
    {
      num_res += res_block->count;
      res_block = res_block->next_block;
    }
  MUTEX_UNLOCK (lk_Gl.obj_res_block_list_mutex);

  fprintf (outfp, "Object Lock Table:\n");
  fprintf (outfp, "\tCurrent number of objects which are locked    = %d\n",
	   lk_Gl.num_obj_res_allocated);
  fprintf (outfp,
	   "\tMaximum number of objects which can be locked = %d\n\n",
	   num_res);

  for (hash_index = 0; hash_index < lk_Gl.obj_hash_size; hash_index++)
    {
      hash_anchor = &lk_Gl.obj_hash_table[hash_index];
      MUTEX_LOCK (rv, hash_anchor->hash_mutex);
      res_ptr = hash_anchor->hash_next;
      res_prev = NULL;
      while (res_ptr != (LK_RES *) NULL)
	{
	  MUTEX_LOCK (rv, res_ptr->res_mutex);

	  if (res_ptr->holder == NULL && res_ptr->waiter == NULL
	      && res_ptr->non2pl == NULL)
	    {
	      if (res_prev == (LK_RES *) NULL)
		{
		  hash_anchor->hash_next = res_ptr->hash_next;
		}
	      else
		{
		  res_prev->hash_next = res_ptr->hash_next;
		}

	      MUTEX_UNLOCK (res_ptr->res_mutex);
	      lock_free_resource (res_ptr);

	      if (res_prev == (LK_RES *) NULL)
		{
		  res_ptr = hash_anchor->hash_next;
		  continue;
		}
	      else
		{
		  res_ptr = res_prev->hash_next;
		  continue;
		}
	    }

	  lock_dump_resource (thread_p, outfp, res_ptr);

	  MUTEX_UNLOCK (res_ptr->res_mutex);
	  res_prev = res_ptr;
	  res_ptr = res_ptr->hash_next;
	}
      MUTEX_UNLOCK (hash_anchor->hash_mutex);
    }

  /* Reset the wait back to the way it was */
  (void) xlogtb_reset_wait_secs (thread_p, old_waitsecs);

  return;
#endif /* !SERVER_MODE */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lock_check_consistency - Check consistency of lock table
 *
 * return: nothing
 *
 * Note:Check consistency of lock table.
 *     This function is used for debugging purposes.
 */
void
lock_check_consistency (THREAD_ENTRY * thread_p)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int consistent = true;
  int hash_index;
  LK_HASH *hash_anchor;
  LK_RES *res_ptr;
  int tran_index;
  LK_TRAN_LOCK *tran_lock;
  int rv;

  /* check the consisteny in object lock table */
  for (hash_index = 0; hash_index < lk_Gl.obj_hash_size; hash_index++)
    {
      hash_anchor = &lk_Gl.obj_hash_table[hash_index];
      MUTEX_LOCK (rv, hash_anchor->hash_mutex);
      res_ptr = hash_anchor->hash_next;
      while (res_ptr != (LK_RES *) NULL)
	{
	  if (lock_check_consistent_resource (thread_p, res_ptr) == false)
	    {
	      consistent = false;
	      fprintf (stderr, "lk_consistent: res_ptr is inconsistent.\n");
	      break;
	    }
	  res_ptr = res_ptr->hash_next;
	}
      MUTEX_UNLOCK (hash_anchor->hash_mutex);
      if (consistent == false)
	{
	  return;
	}
    }

  /* check transaction lock information */
  for (tran_index = 0; tran_index < lk_Gl.num_trans; tran_index++)
    {
      tran_lock = &lk_Gl.tran_lock_table[tran_index];
      if (lock_check_consistent_tran_lock (tran_lock) == false)
	{
	  fprintf (stderr, "lk_consistent: tran_lock is inconsistent.\n");
	  consistent = false;
	  break;
	}
    }
  return;
#endif /* !SERVER_MODE */
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * lock_initialize_composite_lock -
 *
 * return: error code
 *
 *   comp_lock(in):
 */
int
lock_initialize_composite_lock (THREAD_ENTRY * thread_p,
				LK_COMPOSITE_LOCK * comp_lock)
{
#if !defined (SERVER_MODE)
  return NO_ERROR;
#else /* !SERVER_MODE */
  LK_LOCKCOMP *lockcomp;

  if ((comp_lock->lockcomp =
       db_private_alloc (thread_p, sizeof (LK_LOCKCOMP))) == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  lockcomp = (LK_LOCKCOMP *) comp_lock->lockcomp;
  lockcomp->tran_index = LOG_FIND_THREAD_TRAN_INDEX (thread_p);
  lockcomp->waitsecs = logtb_find_wait_secs (lockcomp->tran_index);
  lockcomp->class_list = (LK_LOCKCOMP_CLASS *) NULL;
  lockcomp->root_class_ptr = NULL;
  return NO_ERROR;
#endif /* !SERVER_MODE */
}

/*
 * lock_add_composite_lock -
 *
 * return: error code
 *
 *   comp_lock(in):
 *   oid(in):
 *   class_oid(in):
 *   lock(in):
 */
int
lock_add_composite_lock (THREAD_ENTRY * thread_p,
			 LK_COMPOSITE_LOCK * comp_lock, const OID * oid,
			 const OID * class_oid)
{
#if !defined (SERVER_MODE)
  return NO_ERROR;
#else /* !SERVER_MODE */
  LK_LOCKCOMP *lockcomp;
  LK_LOCKCOMP_CLASS *lockcomp_class;
  OID *p;
  int max_oids;

  if (comp_lock->lockcomp == NULL)
    {
      return ER_FAILED;
    }

  lockcomp = (LK_LOCKCOMP *) comp_lock->lockcomp;
  for (lockcomp_class = lockcomp->class_list;
       lockcomp_class != NULL; lockcomp_class = lockcomp_class->next)
    {
      if (OID_EQ (class_oid, &lockcomp_class->class_oid))
	{
	  break;
	}
    }
  if (lockcomp_class == NULL)
    {				/* class is not found */
      /* allocate lockcomp_class */
      lockcomp_class =
	(LK_LOCKCOMP_CLASS *) db_private_alloc (thread_p,
						sizeof (LK_LOCKCOMP_CLASS));
      if (lockcomp_class == NULL)
	{
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      if (lockcomp->root_class_ptr == NULL)
	{
	  lockcomp->root_class_ptr =
	    lock_get_class_lock (oid_Root_class_oid, lockcomp->tran_index);
	}
      /* initialize lockcomp_class */
      COPY_OID (&lockcomp_class->class_oid, class_oid);
      if (lock_internal_perform_lock_object
	  (thread_p, lockcomp->tran_index, class_oid, (OID *) NULL, IX_LOCK,
	   lockcomp->waitsecs, &lockcomp_class->class_lock_ptr,
	   lockcomp->root_class_ptr) != LK_GRANTED)
	{
	  db_private_free_and_init (thread_p, lockcomp_class);
	  return ER_FAILED;
	}
      if (lockcomp_class->class_lock_ptr->granted_mode == X_LOCK)
	{
	  lockcomp_class->inst_oid_space = NULL;
	}
      else
	{
	  if (LK_COMPOSITE_LOCK_OID_INCREMENT < PRM_LK_ESCALATION_AT)
	    {
	      lockcomp_class->max_inst_oids = LK_COMPOSITE_LOCK_OID_INCREMENT;
	    }
	  else
	    {
	      lockcomp_class->max_inst_oids = PRM_LK_ESCALATION_AT;
	    }
	  lockcomp_class->inst_oid_space =
	    (OID *) db_private_alloc (thread_p,
				      sizeof (OID) *
				      lockcomp_class->max_inst_oids);
	  if (lockcomp_class->inst_oid_space == NULL)
	    {
	      db_private_free_and_init (thread_p, lockcomp_class);
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	  lockcomp_class->num_inst_oids = 0;
	}

      /* connect lockcomp_class into the class_list of lockcomp */
      lockcomp_class->next = lockcomp->class_list;
      lockcomp->class_list = lockcomp_class;
    }

  if (lockcomp_class->class_lock_ptr->granted_mode < X_LOCK)
    {
      if (lockcomp_class->num_inst_oids == lockcomp_class->max_inst_oids)
	{
	  if (lockcomp_class->max_inst_oids < PRM_LK_ESCALATION_AT)
	    {
	      if ((lockcomp_class->max_inst_oids
		   + LK_COMPOSITE_LOCK_OID_INCREMENT) < PRM_LK_ESCALATION_AT)
		{
		  max_oids = lockcomp_class->max_inst_oids
		    + LK_COMPOSITE_LOCK_OID_INCREMENT;
		}
	      else
		{
		  max_oids = PRM_LK_ESCALATION_AT;
		}
	      p =
		(OID *) db_private_realloc (thread_p,
					    lockcomp_class->inst_oid_space,
					    sizeof (OID) * max_oids);
	      if (p != NULL)
		{
		  lockcomp_class->inst_oid_space = p;
		  lockcomp_class->max_inst_oids = max_oids;
		}
	      else
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1,
			  sizeof (OID) * (lockcomp_class->max_inst_oids +
					  LK_COMPOSITE_LOCK_OID_INCREMENT));
		  return ER_OUT_OF_VIRTUAL_MEMORY;
		}
	    }
	}
      if (lockcomp_class->num_inst_oids < lockcomp_class->max_inst_oids)
	{
	  COPY_OID (&lockcomp_class->
		    inst_oid_space[lockcomp_class->num_inst_oids], oid);
	  lockcomp_class->num_inst_oids++;
	}
      /* else, lockcomp_class->max_inst_oids equals PRM_LK_ESCALATION_AT.
       * lock escalation will be performed. so no more instance OID is stored.
       */
    }
  return NO_ERROR;
#endif /* !SERVER_MODE */
}

/*
 * lock_finalize_composite_lock -
 *
 * return:
 *
 *   comp_lock(in):
 */
int
lock_finalize_composite_lock (THREAD_ENTRY * thread_p,
			      LK_COMPOSITE_LOCK * comp_lock)
{
#if !defined (SERVER_MODE)
  return LK_GRANTED;
#else /* !SERVER_MODE */
  LK_LOCKCOMP *lockcomp;
  LK_LOCKCOMP_CLASS *lockcomp_class;
  LK_ENTRY *dummy;
  int i, value = LK_GRANTED;

  if (comp_lock->lockcomp == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_final_composite_lock", "NULL comp_lock->lockcomp pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }

  lockcomp = (LK_LOCKCOMP *) comp_lock->lockcomp;
  for (lockcomp_class = lockcomp->class_list;
       lockcomp_class != NULL; lockcomp_class = lockcomp_class->next)
    {
      if (lockcomp_class->class_lock_ptr->granted_mode == X_LOCK
	  || lockcomp_class->num_inst_oids == PRM_LK_ESCALATION_AT)
	{
	  /* hold X_LOCK on the class object */
	  value =
	    lock_internal_perform_lock_object (thread_p, lockcomp->tran_index,
					       &lockcomp_class->class_oid,
					       (OID *) NULL, X_LOCK,
					       lockcomp->waitsecs, &dummy,
					       lockcomp->root_class_ptr);
	  if (value != LK_GRANTED)
	    {
	      break;
	    }
	}
      else
	{
	  /* hold X_LOCKs on the instance objects */
	  for (i = 0; i < lockcomp_class->num_inst_oids; i++)
	    {
	      value =
		lock_internal_perform_lock_object (thread_p,
						   lockcomp->tran_index,
						   &lockcomp_class->
						   inst_oid_space[i],
						   &lockcomp_class->class_oid,
						   X_LOCK, lockcomp->waitsecs,
						   &dummy,
						   lockcomp_class->
						   class_lock_ptr);
	      if (value != LK_GRANTED)
		{
		  break;
		}
	    }
	  if (value != LK_GRANTED)
	    {
	      break;
	    }
	}
    }

  lockcomp->tran_index = NULL_TRAN_INDEX;
  lockcomp->waitsecs = 0;
  while (lockcomp->class_list != NULL)
    {
      lockcomp_class = lockcomp->class_list;
      lockcomp->class_list = lockcomp_class->next;
      if (lockcomp_class->inst_oid_space)
	{
	  db_private_free_and_init (thread_p, lockcomp_class->inst_oid_space);
	}
      db_private_free_and_init (thread_p, lockcomp_class);
    }
  db_private_free_and_init (thread_p, comp_lock->lockcomp);

  return value;
#endif /* !SERVER_MODE */
}

/*
 * lock_abort_composite_lock -
 *
 * return:
 *
 *   comp_lock(in):
 */
void
lock_abort_composite_lock (LK_COMPOSITE_LOCK * comp_lock)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  LK_LOCKCOMP *lockcomp;
  LK_LOCKCOMP_CLASS *lockcomp_class;

  if (comp_lock->lockcomp == NULL)
    {
      return;
    }

  lockcomp = (LK_LOCKCOMP *) comp_lock->lockcomp;
  lockcomp->tran_index = NULL_TRAN_INDEX;
  lockcomp->waitsecs = 0;
  while (lockcomp->class_list != NULL)
    {
      lockcomp_class = lockcomp->class_list;
      lockcomp->class_list = lockcomp_class->next;
      if (lockcomp_class->inst_oid_space)
	{
	  db_private_free_and_init (NULL, lockcomp_class->inst_oid_space);
	}
      db_private_free_and_init (NULL, lockcomp_class);
    }
  db_private_free_and_init (NULL, comp_lock->lockcomp);
#endif /* !SERVER_MODE */
}

/*
 * lock_get_number_object_locks - Number of object lock entries
 *
 * return:
 *
 * Note:Find the number of total object lock entries of all
 *              transactions
 */
unsigned int
lock_get_number_object_locks (void)
{
#if defined(SA_MODE)
  return 0;
#else
  return lk_Gl.num_obj_res_allocated;
#endif

}

/*
 * lock_start_instant_lock_mode -
 *
 * return:
 *
 *   tran_index(in):
 */
void
lock_start_instant_lock_mode (int tran_index)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  LK_TRAN_LOCK *tran_lock;

  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  tran_lock->is_instant_duration = true;
  return;
#endif /* !SERVER_MODE */
}

/*
 * lock_stop_instant_lock_mode -
 *
 * return:
 *
 *   tran_index(in):
 *   need_unlock(in):
 */
void
lock_stop_instant_lock_mode (THREAD_ENTRY * thread_p, int tran_index,
			     bool need_unlock)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *entry_ptr, *next_ptr;
  int count;

  tran_lock = &lk_Gl.tran_lock_table[tran_index];

  if (!tran_lock->is_instant_duration)
    {
      /* if already stopped, return */
      return;
    }

  /* remove instance locks */
  entry_ptr = tran_lock->inst_hold_list;
  while (entry_ptr != (LK_ENTRY *) NULL)
    {
      next_ptr = entry_ptr->tran_next;
      count = entry_ptr->mlk_count;
      entry_ptr->mlk_count = 0;
      if (need_unlock)
	{
	  while (count--)
	    {
	      lock_internal_perform_unlock_object (thread_p, entry_ptr, false,
						   true);
	    }
	}
      entry_ptr = next_ptr;
    }

  /* remove class locks */
  entry_ptr = tran_lock->class_hold_list;
  while (entry_ptr != (LK_ENTRY *) NULL)
    {
      next_ptr = entry_ptr->tran_next;
      count = entry_ptr->mlk_count;
      entry_ptr->mlk_count = 0;
      if (need_unlock)
	{
	  while (count--)
	    {
	      lock_internal_perform_unlock_object (thread_p, entry_ptr, false,
						   true);
	    }
	}
      entry_ptr = next_ptr;
    }

  /* remove root class lock */
  entry_ptr = tran_lock->root_class_hold;
  if (entry_ptr != NULL)
    {
      count = entry_ptr->mlk_count;
      entry_ptr->mlk_count = 0;
      if (need_unlock)
	{
	  while (count--)
	    {
	      lock_internal_perform_unlock_object (thread_p, entry_ptr, false,
						   true);
	    }
	}
    }

  /* change locking phase as normal */
  tran_lock->is_instant_duration = false;
  return;
#endif /* !SERVER_MODE */
}

/*
 * lock_is_instant_lock_mode -
 *
 * return:
 *
 *   tran_index(in):
 */
bool
lock_is_instant_lock_mode (int tran_index)
{
#if !defined (SERVER_MODE)
  return false;
#else /* !SERVER_MODE */
  LK_TRAN_LOCK *tran_lock;

  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  return tran_lock->is_instant_duration;
#endif /* !SERVER_MODE */
}
