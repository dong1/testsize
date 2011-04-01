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
 * object_representation.h - Definitions related to the representation of
 *        objects on disk and in memory.
 *        his file is shared by both the client and server.
 */

#ifndef _OBJECT_REPRESENTATION_H_
#define _OBJECT_REPRESENTATION_H_

#ident "$Id$"

#include <setjmp.h>
#include <time.h>
#include <float.h>
#include <limits.h>
#if !defined(WINDOWS)
#include <netinet/in.h>
#endif /* !WINDOWS */

#include "error_manager.h"
#include "storage_common.h"
#include "oid.h"
#include "dbtype.h"
#include "byte_order.h"
#include "memory_alloc.h"

/*
 * NUMERIC TYPE SIZES
 *
 * These constants define the byte sizes for the fundamental
 * primitives types as represented in memory and on disk.
 * WARNING: The disk size for the "short" type is actually the same
 * as integer since there is no intelligent attribute packing at this
 * time.
 */
#define OR_BYTE_SIZE            1
#define OR_SHORT_SIZE           2
#define OR_INT_SIZE             4
#define OR_BIGINT_SIZE          8
#define OR_FLOAT_SIZE           4
#define OR_DOUBLE_SIZE          8

#define OR_BIGINT_ALIGNED_SIZE  (OR_BIGINT_SIZE + MAX_ALIGNMENT)
#define OR_DOUBLE_ALIGNED_SIZE  (OR_DOUBLE_SIZE + MAX_ALIGNMENT)
#define OR_PTR_ALIGNED_SIZE     (OR_PTR_SIZE + MAX_ALIGNMENT)
#define OR_VALUE_ALIGNED_SIZE(value)   \
  (or_db_value_size (value) + MAX_ALIGNMENT)

/*
 * DISK IDENTIFIER SIZES
 *
 * These constants describe the size and contents of various disk
 * identifiers as they are represented in a communication buffer.
 * The OID can also be used in an attribute value.
 */

#define OR_OID_SIZE             8
#define OR_OID_PAGEID           0
#define OR_OID_SLOTID           4
#define OR_OID_VOLID            6

#define OR_LOID_SIZE		12
#define OR_LOID_VPID_PAGEID	0
#define OR_LOID_VPID_VOLID	4
#define OR_LOID_VFID_VOLID	6
#define OR_LOID_VFID_FILEID	8

#define OR_HFID_SIZE            12
#define OR_HFID_PAGEID          0
#define OR_HFID_VFID_FILEID     4
#define OR_HFID_VFID_VOLID      8

#define OR_BTID_SIZE            10
#define OR_BTID_ALIGNED_SIZE    (OR_BTID_SIZE + OR_SHORT_SIZE)
#define OR_BTID_PAGEID          0
#define OR_BTID_VFID_FILEID     4
#define OR_BTID_VFID_VOLID      8

#define OR_EHID_SIZE            12
#define OR_EHID_VOLID           0
#define OR_EHID_FILEID          4
#define OR_EHID_PAGEID          8

#define OR_LOG_LSA_SIZE         8
#define OR_LOG_LSA_PAGEID       0
#define OR_LOG_LSA_OFFSET       4

/*
 * EXTENDED TYPE SIZES
 *
 * These define the sizes and contents of the primitive types that
 * are not simple numeric types.
 */
#define OR_TIME_SIZE            4
#define OR_UTIME_SIZE           4
#define OR_DATE_SIZE            4

#define OR_DATETIME_SIZE        8
#define OR_DATETIME_DATE        0
#define OR_DATETIME_TIME        4

#define OR_MONETARY_SIZE        12
#define OR_MONETARY_TYPE        0
#define OR_MONETARY_AMOUNT      4
#define OR_ELO_HEADER_SIZE	OR_LOID_SIZE

/* NUMERIC RANGES */
#define OR_MAX_SHORT_UNSIGNED 65535	/* 0xFFFF */
#define OR_MAX_SHORT 32767	/* 0x7FFF */
#define OR_MIN_SHORT -32768	/* 0x8000 */

#define OR_MAX_INT 2147483647	/* 0x7FFFFFFF */
#define OR_MIN_INT -2147483648	/* 0x80000000 */

/* OVERFLOW CHECK MACROS */

#define OR_CHECK_ASSIGN_OVERFLOW(dest, src) \
  ( ((src) > 0 && (dest) < 0) || ((src) < 0 && (dest) > 0) )

#define OR_CHECK_ADD_OVERFLOW(a,b,c) \
  (((a) > 0 && (b) > 0 && (c) < 0) || ((a) < 0 && (b) < 0 && (c) >= 0))
#define OR_CHECK_UNS_ADD_OVERFLOW(a,b,c) 	(c) < (a) || (c) < (b)
#define OR_CHECK_SUB_UNDERFLOW(a,b,c) \
  (((a) < (b) && (c) > 0) || ((a) > (b) && (c) < 0))
#define OR_CHECK_UNS_SUB_UNDERFLOW(a,b,c)	(b) > (a)
#define OR_CHECK_MULT_OVERFLOW(a,b,c)  (((b)==0)?((c)!=0):((c)/(b)!=(a)))

#define OR_CHECK_SHORT_OVERFLOW(i)  ((i) > DB_INT16_MAX || (i) < DB_INT16_MIN )
#define OR_CHECK_INT_OVERFLOW(i)    ((i) > DB_INT32_MAX || (i) < DB_INT32_MIN )
#define OR_CHECK_BIGINT_OVERFLOW(i) ((i) > DB_BIGINT_MAX || (i) < DB_BIGINT_MIN)
#define OR_CHECK_USHRT_OVERFLOW(i)  ((i) > DB_UINT16_MAX || (i) < 0)
#define OR_CHECK_UINT_OVERFLOW(i)   ((i) > DB_UINT32_MAX || (i) < 0)

#define OR_CHECK_FLOAT_OVERFLOW(i)         ((i) > FLT_MAX || (-(i)) > FLT_MAX)
#define OR_CHECK_DOUBLE_OVERFLOW(i)        ((i) > DBL_MAX || (-(i)) > DBL_MAX)

/* PACK/UNPACK MACROS */

#define OR_GET_BYTE(ptr)	(*(unsigned char *)((char *)(ptr)))
#define OR_GET_SHORT(ptr)       ((short)ntohs(*(short *)((char *)(ptr))))
#define OR_GET_INT(ptr) 	((int)ntohl(*(int *)((char *)(ptr))))
#define OR_GET_FLOAT(ptr, value) ntohf((float *)((char *)(ptr)),(float *)value)
#define OR_GET_DOUBLE(ptr,value) ntohd((double *)((char *)(ptr)),(double *)value)
#define OR_GET_STRING(ptr)	((char *)((char *)(ptr)))

#define OR_PUT_BYTE(ptr, val)	(*((unsigned char *)(ptr))=(unsigned char)(val))
#define OR_PUT_SHORT(ptr, val)	(*(short *)((char *)(ptr)) = htons((short)(val)))
#define OR_PUT_INT(ptr, val) 	(*(int *)((char *)(ptr)) = htonl((int)(val)))
#define OR_PUT_FLOAT(ptr, value) htonf((float *)((char *)(ptr)),(float *)(value))
#define OR_PUT_DOUBLE(ptr, value) htond((double *)((char *)(ptr)),(double *)(value))

#define OR_MOVE_MONETARY(src, dst) \
  do { \
      OR_MOVE_DOUBLE(src, dst); \
      ((DB_MONETARY *)dst)->type = ((DB_MONETARY *)src)->type; \
  } while (0)

#if OR_BYTE_ORDER == OR_LITTLE_ENDIAN

#define swap64(x)  ((((unsigned long long) (x)&(0x00000000000000FFULL))<<56) \
                   |(((unsigned long long) (x)&(0xFF00000000000000ULL))>>56) \
                   |(((unsigned long long) (x)&(0x000000000000FF00ULL))<<40) \
                   |(((unsigned long long) (x)&(0x00FF000000000000ULL))>>40) \
                   |(((unsigned long long) (x)&(0x0000000000FF0000ULL))<<24) \
                   |(((unsigned long long) (x)&(0x0000FF0000000000ULL))>>24) \
                   |(((unsigned long long) (x)&(0x00000000FF000000ULL))<<8)  \
                   |(((unsigned long long) (x)&(0x000000FF00000000ULL))>>8))

#else /* OR_BYTE_ORDER == OR_LITTLE_ENDIAN */
#define swap64(x)        (x)
#endif /* OR_BYTE_ORDER == OR_LITTLE_ENDIAN */

#if __WORDSIZE == 32
#define OR_PTR_SIZE             4
#define OR_PUT_PTR(ptr, val)    OR_PUT_INT((ptr), (val))
#define OR_GET_PTR(ptr)         OR_GET_INT((ptr))
#else /* __WORDSIZE == 32 */
#define OR_PTR_SIZE             8
#define OR_PUT_PTR(ptr, val)    (*(UINTPTR *)((char *)(ptr)) = swap64((UINTPTR)val))
#define OR_GET_PTR(ptr)         ((UINTPTR)swap64(*(UINTPTR *)((char *)(ptr))))
#endif /* __WORDSIZE == 32 */

#define OR_INT64_SIZE           8
#define OR_PUT_INT64(ptr, val)  (*(INT64 *)((char *)(ptr)) = swap64((INT64)val))
#define OR_GET_INT64(ptr)       ((INT64)swap64(*(INT64 *)((char *)(ptr))))

/* EXTENDED TYPES */

#define OR_GET_BIGINT(ptr)      OR_GET_INT64(ptr)
#define OR_PUT_BIGINT(ptr, val) OR_PUT_INT64(ptr, val)

#define OR_GET_TIME(ptr, value) \
  *((DB_TIME *)(value)) = OR_GET_INT(ptr)

#define OR_PUT_TIME(ptr, value) \
  OR_PUT_INT(ptr, *((DB_TIME *)(value)))

#define OR_GET_UTIME(ptr, value) \
   *((DB_UTIME *)(value)) = OR_GET_INT(ptr)

#define OR_PUT_UTIME(ptr, value) \
   OR_PUT_INT(ptr,   *((DB_UTIME *)(value)))

#define OR_GET_DATE(ptr, value) \
   *((DB_DATE *)(value)) = OR_GET_INT(ptr)

#define OR_PUT_DATE(ptr, value) \
   OR_PUT_INT(ptr, *((DB_DATE *)(value)))

#define OR_GET_DATETIME(ptr, datetime) \
   do { \
     (datetime)->date = OR_GET_INT(((char *)(ptr)) + OR_DATETIME_DATE); \
     (datetime)->time = OR_GET_INT(((char *)(ptr)) + OR_DATETIME_TIME); \
   } while (0)

#define OR_PUT_DATETIME(ptr, datetime) \
   do { \
     OR_PUT_INT(((char *)ptr) + OR_DATETIME_DATE, (datetime)->date); \
     OR_PUT_INT(((char *)ptr) + OR_DATETIME_TIME, (datetime)->time); \
   } while (0)

#define OR_GET_MONETARY(ptr, value) \
   do { \
     double pack_value; \
     (value)->type = (DB_CURRENCY) OR_GET_INT(((char *)(ptr)) + OR_MONETARY_TYPE); \
     memcpy((char *)(&pack_value), ((char *)(ptr)) + OR_MONETARY_AMOUNT, OR_DOUBLE_SIZE); \
     OR_GET_DOUBLE(&pack_value, &(value)->amount);                                 \
   } while (0)

#define OR_GET_CURRENCY_TYPE(ptr) \
  (DB_CURRENCY) OR_GET_INT(((char *)(ptr)) + OR_MONETARY_TYPE)

#define OR_PUT_MONETARY(ptr, value) \
   do { \
     double pack_value;         \
     OR_PUT_INT(((char *)(ptr)) + OR_MONETARY_TYPE, (int) (value)->type); \
     OR_PUT_DOUBLE(&pack_value, &((value)->amount));                      \
     memcpy(((char *)(ptr)) + OR_MONETARY_AMOUNT, &pack_value, OR_DOUBLE_SIZE); \
   } while (0)

/* DISK IDENTIFIERS */

#define OR_GET_OID(ptr, oid) \
   do { \
     (oid)->pageid = OR_GET_INT(((char *)(ptr)) + OR_OID_PAGEID); \
     (oid)->slotid = OR_GET_SHORT(((char *)(ptr)) + OR_OID_SLOTID); \
     (oid)->volid  = OR_GET_SHORT(((char *)(ptr)) + OR_OID_VOLID); \
   } while (0)

#define OR_PUT_OID(ptr, oid) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_OID_PAGEID, (oid)->pageid); \
     OR_PUT_SHORT(((char *)(ptr)) + OR_OID_SLOTID, (oid)->slotid); \
     OR_PUT_SHORT(((char *)(ptr)) + OR_OID_VOLID, (oid)->volid); \
   } while (0)

#define OR_PUT_NULL_OID(ptr) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_OID_PAGEID,   NULL_PAGEID); \
     OR_PUT_SHORT(((char *)(ptr)) + OR_OID_SLOTID, 0); \
     OR_PUT_SHORT(((char *)(ptr)) + OR_OID_VOLID,  0); \
   } while (0)

#define OR_GET_LOID(ptr, loid) \
   do { \
     (loid)->vpid.pageid = OR_GET_INT(((char *)(ptr)) + OR_LOID_VPID_PAGEID); \
     (loid)->vpid.volid  = OR_GET_SHORT(((char *)(ptr)) + OR_LOID_VPID_VOLID); \
     (loid)->vfid.volid  = OR_GET_SHORT(((char *)(ptr)) + OR_LOID_VFID_VOLID); \
     (loid)->vfid.fileid = OR_GET_INT(((char *)(ptr)) + OR_LOID_VFID_FILEID); \
   } while (0)

#define OR_PUT_LOID(ptr, loid) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_LOID_VPID_PAGEID,  (loid)->vpid.pageid); \
     OR_PUT_SHORT(((char *)(ptr)) + OR_LOID_VPID_VOLID, (loid)->vpid.volid); \
     OR_PUT_SHORT(((char *)(ptr)) + OR_LOID_VFID_VOLID, (loid)->vfid.volid); \
     OR_PUT_INT(((char *)(ptr)) + OR_LOID_VFID_FILEID,  (loid)->vfid.fileid); \
   } while (0)

#define OR_PUT_NULL_LOID(ptr) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_LOID_VPID_PAGEID,  -1); \
     OR_PUT_SHORT(((char *)(ptr)) + OR_LOID_VPID_VOLID, -1); \
     OR_PUT_SHORT(((char *)(ptr)) + OR_LOID_VFID_VOLID, -1); \
     OR_PUT_INT(((char *)(ptr)) + OR_LOID_VFID_FILEID,  -1); \
   } while (0)

#define OR_GET_HFID(ptr, hfid) \
   do { \
     (hfid)->hpgid       = OR_GET_INT(((char *)(ptr)) + OR_HFID_PAGEID); \
     (hfid)->vfid.fileid = OR_GET_INT(((char *)(ptr)) + OR_HFID_VFID_FILEID); \
     (hfid)->vfid.volid  = OR_GET_INT(((char *)(ptr)) + OR_HFID_VFID_VOLID); \
   } while (0)

#define OR_PUT_HFID(ptr, hfid) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_HFID_PAGEID,      (hfid)->hpgid); \
     OR_PUT_INT(((char *)(ptr)) + OR_HFID_VFID_FILEID, (hfid)->vfid.fileid); \
     OR_PUT_INT(((char *)(ptr)) + OR_HFID_VFID_VOLID,  (hfid)->vfid.volid); \
   } while (0)

#define OR_PUT_NULL_HFID(ptr) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_HFID_PAGEID,      -1); \
     OR_PUT_INT(((char *)(ptr)) + OR_HFID_VFID_FILEID, -1); \
     OR_PUT_INT(((char *)(ptr)) + OR_HFID_VFID_VOLID,  -1); \
   } while (0)

#define OR_GET_BTID(ptr, btid) \
   do { \
     (btid)->root_pageid     = OR_GET_INT(((char *)(ptr)) + OR_BTID_PAGEID); \
     (btid)->vfid.fileid = OR_GET_INT(((char *)(ptr)) + OR_BTID_VFID_FILEID); \
     (btid)->vfid.volid  = OR_GET_SHORT(((char *)(ptr)) + OR_BTID_VFID_VOLID); \
   } while (0)

#define OR_PUT_BTID(ptr, btid) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_BTID_PAGEID,       (btid)->root_pageid); \
     OR_PUT_INT(((char *)(ptr)) + OR_BTID_VFID_FILEID,  (btid)->vfid.fileid); \
     OR_PUT_SHORT(((char *)(ptr)) + OR_BTID_VFID_VOLID, (btid)->vfid.volid); \
   } while (0)

#define OR_PUT_NULL_BTID(ptr) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_BTID_PAGEID,       NULL_PAGEID); \
     OR_PUT_INT(((char *)(ptr)) + OR_BTID_VFID_FILEID,  NULL_FILEID); \
     OR_PUT_SHORT(((char *)(ptr)) + OR_BTID_VFID_VOLID, NULL_VOLID); \
   } while (0)

#define OR_GET_EHID(ptr, ehid) \
   do { \
     (ehid)->vfid.volid  = OR_GET_INT(((char *)(ptr)) + OR_EHID_VOLID); \
     (ehid)->vfid.fileid = OR_GET_INT(((char *)(ptr)) + OR_EHID_FILEID); \
     (ehid)->pageid = OR_GET_INT(((char *)(ptr)) + OR_EHID_PAGEID); \
   } while (0)

#define OR_PUT_EHID(ptr, ehid) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_EHID_VOLID,  (ehid)->vfid.volid); \
     OR_PUT_INT(((char *)(ptr)) + OR_EHID_FILEID, (ehid)->vfid.fileid); \
     OR_PUT_INT(((char *)(ptr)) + OR_EHID_PAGEID, (ehid)->pageid); \
   } while (0)

#define OR_GET_LOG_LSA(ptr, lsa) \
   do { \
     (lsa)->pageid  = OR_GET_INT(((char *)(ptr)) + OR_LOG_LSA_PAGEID); \
     (lsa)->offset  = OR_GET_INT(((char *)(ptr)) + OR_LOG_LSA_OFFSET); \
   } while (0)

#define OR_PUT_LOG_LSA(ptr, lsa) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_LOG_LSA_PAGEID, (lsa)->pageid); \
     OR_PUT_INT(((char *)(ptr)) + OR_LOG_LSA_OFFSET, (lsa)->offset); \
   } while (0)

#define OR_PUT_NULL_LOG_LSA(ptr) \
   do { \
     OR_PUT_INT(((char *)(ptr)) + OR_LOG_LSA_PAGEID, -1); \
     OR_PUT_INT(((char *)(ptr)) + OR_LOG_LSA_OFFSET, -1); \
   } while (0)

/*
 * VARIABLE OFFSET TABLE ACCESSORS
 * The variable offset table is present in the headers of objects and sets.
 */

#define OR_VAR_TABLE_SIZE(vars) \
  (((vars) == 0) ? 0 : (OR_INT_SIZE * ((vars) + 1)))

#define OR_VAR_TABLE_ELEMENT(table, index) \
  (&((int *)(table))[(index)])

#define OR_VAR_TABLE_ELEMENT_OFFSET(table, index) \
  (OR_GET_INT(OR_VAR_TABLE_ELEMENT(table, index)))

#define OR_VAR_TABLE_ELEMENT_LENGTH(table, index) \
  ((OR_GET_INT(OR_VAR_TABLE_ELEMENT((table), (index) + 1))) - \
   (OR_GET_INT(OR_VAR_TABLE_ELEMENT((table), (index)))))



/* OBJECT HEADER LAYOUT */
#define OR_HEADER_SIZE  20	/* oid & two integers & flag word */
#define OR_CLASS_OFFSET 0
#define OR_REP_OFFSET   8
#define OR_CHN_OFFSET   12
#define OR_FLAG_OFFSET  16

/* high bit of the repid word is reserved for the bound bit flag,
   need to keep representations from going negative ! */
#define OR_BOUND_BIT_FLAG 0x80000000

/* OBJECT HEADER ACCESS MACROS */

#define OR_GET_CLASS_OID(ptr) ptr

#define OR_GET_REPRID(ptr) \
  ((OR_GET_INT((ptr) + OR_REP_OFFSET)) & ~OR_BOUND_BIT_FLAG)

#define OR_GET_BOUND_BIT_FLAG(ptr) \
  ((OR_GET_INT((ptr) + OR_REP_OFFSET)) & OR_BOUND_BIT_FLAG)

#define OR_GET_CHN(ptr) \
  (OR_GET_INT((ptr) + OR_CHN_OFFSET))

/* VARIABLE OFFSET TABLE ACCESSORS */

#define OR_GET_OBJECT_VAR_TABLE(obj) \
  (int *)(((char *)(obj)) + OR_HEADER_SIZE)

#define OR_VAR_ELEMENT(obj, index) \
  (OR_VAR_TABLE_ELEMENT(OR_GET_OBJECT_VAR_TABLE(obj), index))

#define OR_VAR_OFFSET(obj, index) \
  OR_VAR_TABLE_ELEMENT_OFFSET(OR_GET_OBJECT_VAR_TABLE(obj), index)

#define OR_VAR_IS_NULL(obj, index) \
  (OR_VAR_TABLE_ELEMENT_LENGTH(OR_GET_OBJECT_VAR_TABLE(obj), index)? 0: 1)

#define OR_VAR_LENGTH(length, obj, index, n_variables)            \
  do {                                                            \
    int _this_offset, _next_offset, _temp_offset, _nth_var;       \
    _this_offset = OR_VAR_OFFSET(obj, index);                     \
    _next_offset = OR_VAR_OFFSET(obj, index + 1);                 \
    if ((length = _next_offset - _this_offset) != 0) {            \
      _next_offset = 0;                                           \
      for (_nth_var = 0; _nth_var <= n_variables; _nth_var++) {   \
        _temp_offset = OR_VAR_OFFSET(obj, _nth_var);              \
        if (_temp_offset > _this_offset ) {                       \
          if (_next_offset == 0)                                  \
            _next_offset = _temp_offset;                          \
          else if (_temp_offset < _next_offset)                   \
            _next_offset = _temp_offset;                          \
        }                                                         \
      }                                                           \
      length = _next_offset - _this_offset;                       \
  }                                                               \
} while(0)

/*
 * BOUND BIT ACCESSORS.
 * Note that these are assuming 4 byte integers to avoid a divide operation.
 */

#define OR_BOUND_BIT_WORDS(count) (((count) + 31) >> 5)
#define OR_BOUND_BIT_BYTES(count) ((((count) + 31) >> 5) * 4)

#define OR_BOUND_BIT_MASK(element) (1 << ((int)(element) & 7))

#define OR_GET_BOUND_BIT_BYTE(bitptr, element) \
  ((char *)(bitptr) + ((int)(element) >> 3))

#define OR_GET_BOUND_BIT(bitptr, element) \
  ((*OR_GET_BOUND_BIT_BYTE(bitptr, element)) & OR_BOUND_BIT_MASK(element))

#define OR_GET_BOUND_BITS(obj, nvars, fsize) \
  (char *)(((char *)(obj)) + OR_HEADER_SIZE + OR_VAR_TABLE_SIZE(nvars) + fsize)

/* These are the most useful ones if we're only testing a single attribute */

#define OR_FIXED_ATT_IS_BOUND(obj, nvars, fsize, position) \
  (!OR_GET_BOUND_BIT_FLAG(obj) || \
   OR_GET_BOUND_BIT(OR_GET_BOUND_BITS(obj, nvars, fsize), position))

#define OR_FIXED_ATT_IS_UNBOUND(obj, nvars, fsize, position) \
  (OR_GET_BOUND_BIT_FLAG(obj) && \
   !OR_GET_BOUND_BIT(OR_GET_BOUND_BITS(obj, nvars, fsize), position))

#define OR_ENABLE_BOUND_BIT(bitptr, element) \
  *OR_GET_BOUND_BIT_BYTE(bitptr, element) = \
    *OR_GET_BOUND_BIT_BYTE(bitptr, element) | OR_BOUND_BIT_MASK(element)

#define OR_CLEAR_BOUND_BIT(bitptr, element) \
  *OR_GET_BOUND_BIT_BYTE(bitptr, element) = \
    *OR_GET_BOUND_BIT_BYTE(bitptr, element) & ~OR_BOUND_BIT_MASK(element)

/* ATTRIBUTE LOCATION */
#define OR_FIXED_ATTRIBUTES_OFFSET(nvars) \
  (OR_HEADER_SIZE + OR_VAR_TABLE_SIZE(nvars))

/* SET HEADER */

#define OR_SET_HEADER_SIZE 		8
#define OR_SET_SIZE_OFFSET         	4
/* optional header extension if the full domain is present */
#define OR_SET_DOMAIN_SIZE_OFFSET	8

/* Set header fields.
   These constants are used to construct and decompose the set header word. */
#define OR_SET_TYPE_MASK 	0xFF
#define OR_SET_ETYPE_MASK 	0xFF00
#define OR_SET_ETYPE_SHIFT 	8
#define OR_SET_BOUND_BIT 	0x10000
#define OR_SET_VARIABLE_BIT 	0x20000
#define OR_SET_DOMAIN_BIT	0x40000
#define OR_SET_TAG_BIT		0x80000
#define OR_SET_COMMON_SUB_BIT	0x100000

#define OR_SET_TYPE(setptr) \
  (DB_TYPE) ((OR_GET_INT((char *)(setptr))) & OR_SET_TYPE_MASK)

#define OR_SET_ELEMENT_TYPE(setptr)  \
  (DB_TYPE) ((OR_GET_INT((char *)(setptr)) & OR_SET_ETYPE_MASK) >> \
             OR_SET_ETYPE_SHIFT)

#define OR_SET_HAS_BOUND_BITS(setptr) \
  (OR_GET_INT((char *)(setptr)) & OR_SET_BOUND_BIT)

#define OR_SET_HAS_OFFSET_TABLE(setptr) \
  (OR_GET_INT((char *)(setptr)) & OR_SET_VARIABLE_BIT)

#define OR_SET_HAS_DOMAIN(setptr) \
  (OR_GET_INT((char *)(setptr)) & OR_SET_DOMAIN_BIT)

#define OR_SET_HAS_ELEMENT_TAGS(setptr) \
  (OR_GET_INT((char *)(setptr)) & OR_SET_TAG_BIT)

#define OR_SET_ELEMENT_COUNT(setptr) \
  ((OR_GET_INT((char *)(setptr) + OR_SET_SIZE_OFFSET)))

#define OR_SET_DOMAIN_SIZE(setptr) \
  ((OR_GET_INT((char *)(setptr) + OR_SET_DOMAIN_SIZE_OFFSET)))

/*
 * SET VARIABLE OFFSET TABLE ACCESSORS.
 * Should make sure that the set actually has one before using.
 */
#define OR_GET_SET_VAR_TABLE(setptr) \
  (int *)((char *)(setptr) + OR_SET_HEADER_SIZE)

#define OR_SET_ELEMENT_OFFSET(setptr, element) \
  OR_VAR_TABLE_ELEMENT_OFFSET(OR_GET_SET_VAR_TABLE(setptr), element)

/*
 * SET BOUND BIT ACCESSORS
 *
 * Should make sure that the set actually has these before using.
 * Its essentially the same as OR_GET_SET_VAR_TABLE since these will
 * be in the same position and can't both appear at the same time.
 */

#define OR_GET_SET_BOUND_BITS(setptr) \
  (int *)((char *)(setptr) + OR_SET_HEADER_SIZE)

/* MIDXKEY HEADER */

#define OR_MULTI_BOUND_BIT_WORDS(count)  (((count) + 31) >> 5)
#define OR_MULTI_BOUND_BIT_BYTES(count)  ((((count) + 31) >> 5) * 4)

#define OR_MULTI_BOUND_BIT_MASK(element) (1 << ((int)(element) & 7))

#define OR_MULTI_GET_BOUND_BIT_BYTE(bitptr, element)       \
        ((char *)(bitptr) + ((int)(element) >> 3))

#define OR_MULTI_GET_BOUND_BIT(bitptr, element)            \
        ((*OR_MULTI_GET_BOUND_BIT_BYTE(bitptr, element)) & \
         OR_MULTI_BOUND_BIT_MASK(element))

#define OR_MULTI_GET_BOUND_BITS(bitptr, fsize)             \
        (char *)(((char *)(bitptr)) + fsize)

#define OR_MULTI_ATT_IS_BOUND(bitptr, element)             \
         OR_MULTI_GET_BOUND_BIT(bitptr, element)
#define OR_MULTI_ATT_IS_UNBOUND(bitptr, element)           \
        (!OR_MULTI_GET_BOUND_BIT(bitptr, element))

#define OR_MULTI_ENABLE_BOUND_BIT(bitptr, element)         \
        *OR_MULTI_GET_BOUND_BIT_BYTE(bitptr, element) =    \
          *OR_MULTI_GET_BOUND_BIT_BYTE(bitptr, element) |  \
          OR_MULTI_BOUND_BIT_MASK(element)

#define OR_MULTI_CLEAR_BOUND_BIT(bitptr, element)          \
        *OR_MULTI_GET_BOUND_BIT_BYTE(bitptr, element) =    \
          *OR_MULTI_GET_BOUND_BIT_BYTE(bitptr, element) &  \
          ~OR_MULTI_BOUND_BIT_MASK(element)



/*
 * OR_SUB_HEADER_SIZE
 *
 * Used to tag each substructure.  Same as the object header currently.
 *   class oid, repid, flags
 */
#define OR_SUB_HEADER_SIZE 	OR_OID_SIZE + OR_INT_SIZE + OR_INT_SIZE

/*
 * OR_SUB_DOMAIN_AND_HEADER_SIZE
 *
 * Hack for the class transformer, since we always currently know what the
 * substructure lists contain, this allows us to skip over the packed domain
 * quickly.  Must match the stuff packed by or_put_sub_domain_and_header().
 */
#define OR_SUB_DOMAIN_SIZE	OR_INT_SIZE

/* VARIABLE HEADER */
#define OR_VARIABLE_HEADER_SIZE 4

#define OR_GET_VARIABLE_TYPE(ptr) (OR_GET_INT((int *)(ptr)))



/* class */
#define ORC_CLASS_VAR_ATT_COUNT		16
#define ORC_HFID_FILEID_OFFSET		12
#define ORC_HFID_VOLID_OFFSET		16
#define ORC_HFID_PAGEID_OFFSET		20
#define ORC_REPID_OFFSET		24
#define ORC_FIXED_COUNT_OFFSET		28
#define ORC_VARIABLE_COUNT_OFFSET	32
#define ORC_FIXED_LENGTH_OFFSET		36
#define ORC_ATT_COUNT_OFFSET            40
#define ORC_SHARED_COUNT_OFFSET		48
#define ORC_CLASS_ATTR_COUNT_OFFSET	60
#define ORC_CLASS_FLAGS_OFFSET		64
#define ORC_CLASS_TYPE_OFFSET		68

#define ORC_NAME_INDEX 			0
#define ORC_REPRESENTATIONS_INDEX	2
#define ORC_SUBCLASSES_INDEX		3
#define ORC_SUPERCLASSES_INDEX		4
#define ORC_ATTRIBUTES_INDEX		5
#define ORC_SHARED_ATTRS_INDEX		6
#define ORC_CLASS_ATTRS_INDEX		7
#define ORC_PROPERTIES_INDEX		14
#define ORC_NODE_NAME_INDEX		15

/* attribute*/
#define ORC_ATT_VAR_ATT_COUNT           6
#define ORC_ATT_ID_OFFSET		0
#define ORC_ATT_TYPE_OFFSET		4
#define ORC_ATT_CLASS_OFFSET		16
#define ORC_ATT_FLAG_OFFSET             24
#define ORC_ATT_INDEX_OFFSET		28
#define ORC_ATT_NAME_INDEX		0
#define ORC_ATT_CURRENT_VALUE_INDEX	1
#define ORC_ATT_ORIGINAL_VALUE_INDEX	2
#define ORC_ATT_DOMAIN_INDEX		3

/* representation */
#define ORC_REP_VAR_ATT_COUNT		2
#define ORC_REP_ID_OFFSET		0
#define ORC_REP_FIXED_COUNT_OFFSET	4
#define ORC_REP_VARIABLE_COUNT_OFFSET	8
#define ORC_REP_ATTRIBUTES_INDEX	0

/* rep_attribute */
#define ORC_REPATT_VAR_ATT_COUNT 	1
#define ORC_REPATT_ID_OFFSET		0
#define ORC_REPATT_TYPE_OFFSET		4
#define ORC_REPATT_DOMAIN_INDEX		0

/* domain */
#define ORC_DOMAIN_VAR_ATT_COUNT 	1
#define ORC_DOMAIN_TYPE_OFFSET 		0
#define ORC_DOMAIN_PRECISION_OFFSET 	4
#define ORC_DOMAIN_SCALE_OFFSET 	8
#define ORC_DOMAIN_CODESET_OFFSET 	12
#define ORC_DOMAIN_CLASS_OFFSET 	16
#define ORC_DOMAIN_SETDOMAIN_INDEX	0

/* MEMORY REPRESENTATION STRUCTURES */

#define OR_BINARY_MAX_LENGTH 65535
#define OR_BINARY_LENGTH_MASK 0xFFFF
#define OR_BINARY_PAD_SHIFT  16

typedef struct db_binary DB_BINARY;
struct db_binary
{
  unsigned char *data;
  unsigned int length;
};

/*
 * DB_REFERENCE
 *    This is a common structure header used by DB_SET and DB_ELO.
 *    It encapsulates ownership information that must be maintained
 *    by these two types.  General routines can be written to maintain
 *    ownership information for both types.
 *
 */
typedef struct db_reference DB_REFERENCE;
struct db_reference
{
  struct db_object *handle;
  struct db_object *owner;
  int attribute;
};

/*
 * DB_ELO
 *    This is the run-time state structure for an ELO.
 *    The ELO is part of the implementation of the GLO and is not
 *    used directly by the API.
 *
 */
typedef enum db_elo_type DB_ELO_TYPE;
enum db_elo_type
{
  ELO_NULL,
  ELO_LO,
  ELO_FBO
};

/* Typedef DB_ELO is defined in dbtype.h */
struct db_elo
{
  /* these two fields are kept in the disk representation */
  LOID loid;			/* loid for large object */
  const char *pathname;		/* pathname of file based object (may be shadow) */
  const char *original_pathname;	/* the real file for file based objects */

  /* misc run time information */
  DB_ELO_TYPE type;
};

typedef struct db_set SETREF;
struct db_set
{
  /*
   * a garbage collector ticket is not required for the "owner" field as
   * the entire set references area is registered for scanning in area_grow.
   */
  struct db_object *owner;
  struct db_set *ref_link;
  struct setobj *set;
  char *disk_set;
  DB_DOMAIN *disk_domain;
  int attribute;
  int ref_count;
  int disk_size;
  bool need_clear;
};

/*
 * SETOBJ
 *    This is the primitive set object header.
 */
typedef struct setobj SETOBJ;

/*
 * OR_TYPE_SIZE
 *    Returns the byte size of the disk representation of a particular
 *    type.  Returns -1 if the type is variable and the size cannot
 *    be known from just the type id.
 */
extern int or_Type_sizes[];	/* map of type id to fixed value size */

#define OR_TYPE_SIZE(type) or_Type_sizes[(int)(type)]

/*
 * OR_VARINFO
 *    Memory representation for a variable offset table.  This is build
 *    from a disk offset table, either in an object header or in a
 *    set header.
 */
typedef struct or_varinfo OR_VARINFO;
struct or_varinfo
{
  int offset;
  int length;
};

#if __WORDSIZE == 32

#define OR_ALIGNED_BUF(size) \
union { \
  double dummy; \
  char buf[(size) + MAX_ALIGNMENT]; \
}

#define OR_ALIGNED_BUF_START(abuf) (PTR_ALIGN (abuf.buf, MAX_ALIGNMENT))
#define OR_ALIGNED_BUF_SIZE(abuf) (sizeof(abuf.buf) - MAX_ALIGNMENT)

#else /* __WORDSIZE == 32 */

#define OR_ALIGNED_BUF(size) \
union { \
  double dummy; \
  char buf[(size)]; \
}

#define OR_ALIGNED_BUF_START(abuf) (abuf.buf)
#define OR_ALIGNED_BUF_SIZE(abuf) (sizeof(abuf.buf))

#endif

#define OR_INFINITE_POINTER ((void *)(~((UINTPTR)0UL)))

typedef struct or_buf OR_BUF;
struct or_buf
{
  char *buffer;
  char *ptr;
  char *endptr;
  struct or_fixup *fixups;
  jmp_buf env;
  int error_abort;
};

/* TODO: LP64 check DB_INT32_MAX */

#define OR_BUF_INIT(buf, data, size)                                        \
        (buf).buffer = (buf).ptr = (data);                                  \
        (buf).endptr = ((size) <= 0 || (size) == DB_INT32_MAX) ?            \
                       (char*) OR_INFINITE_POINTER : (data) + (size);               \
        (buf).error_abort = 0;                                              \
        (buf).fixups = NULL;

#define OR_BUF_INIT2(buf, data, size)                                       \
        (buf).buffer = (buf).ptr = (data);                                  \
        (buf).endptr = ((size) <= 0 || (size) == DB_INT32_MAX) ?            \
                       (char*) OR_INFINITE_POINTER : (data) + (size);               \
        (buf).error_abort = 1;                                              \
        (buf).fixups = NULL;

/* Need to translate types of DB_TYPE_OBJECT into DB_TYPE_OID in server-side */
#define OR_PACK_DOMAIN_OBJECT_TO_OID(p, d, o, n)                           \
    or_pack_domain((p),                                                    \
                   (d)->type->id == DB_TYPE_OBJECT ? &tp_Oid_domain : (d), \
                   (o), (n))

extern bool or_isinstance (RECDES * record, OID * class_oid);
extern void or_class_oid (RECDES * record, OID * oid);
extern int or_rep_id (RECDES * record);
extern int or_chn (RECDES * record);
extern char *or_class_name (RECDES * record);
extern char *or_node_name (RECDES * record);

/* Pointer based decoding functions */
extern int or_set_element_offset (char *setptr, int element);

#if defined(ENABLE_UNUSED_FUNCTION)
extern int or_get_bound_bit (char *bound_bits, int element);
extern void or_put_bound_bit (char *bound_bits, int element, int bound);
#endif

/* Data packing functions */
extern char *or_pack_int (char *ptr, int number);
#if defined(ENABLE_UNUSED_FUNCTION)
extern char *or_pack_bigint (char *ptr, DB_BIGINT number);
#endif
extern char *or_pack_int64 (char *ptr, INT64 number);
extern char *or_pack_float (char *ptr, float number);
extern char *or_pack_double (char *ptr, double number);
#if defined(ENABLE_UNUSED_FUNCTION)
extern char *or_pack_time (char *ptr, DB_TIME time);
extern char *or_pack_date (char *ptr, DB_DATE date);
extern char *or_pack_monetary (char *ptr, DB_MONETARY * money);
extern char *or_pack_utime (char *ptr, DB_UTIME utime);
#endif
extern char *or_pack_short (char *ptr, short number);
extern char *or_pack_string (char *ptr, const char *string);
extern char *or_pack_string_with_length (char *ptr, char *string, int length);
extern char *or_pack_errcode (char *ptr, int error);
extern char *or_pack_oid (char *ptr, OID * oid);
extern char *or_pack_loid (char *ptr, LOID * loid);
extern char *or_pack_hfid (const char *ptr, const HFID * hfid);
extern char *or_pack_btid (char *buf, BTID * btid);
extern char *or_pack_ehid (char *buf, EHID * btid);
extern char *or_pack_log_lsa (const char *ptr, const LOG_LSA * lsa);
extern char *or_unpack_log_lsa (char *ptr, LOG_LSA * lsa);
extern char *or_unpack_set (char *ptr, SETOBJ ** set,
			    struct tp_domain *domain);
extern char *or_unpack_setref (char *ptr, DB_SET ** ref);
extern char *or_pack_listid (char *ptr, void *listid);
extern char *or_pack_lock (char *ptr, LOCK lock);
extern char *or_pack_set_header (char *buf, DB_TYPE stype, DB_TYPE etype,
				 int bound_bits, int size);
extern char *or_pack_method_sig_list (char *ptr, void *method_sig_list);
extern char *or_pack_set_node (char *ptr, void *set_node);
#if defined(ENABLE_UNUSED_FUNCTION)
extern char *or_pack_elo (char *ptr, void *elo);
extern char *or_pack_string_array (char *buffer, int count,
				   const char **string_array);
extern char *or_pack_db_value_array (char *buffer, int count, DB_VALUE * val);
extern char *or_pack_int_array (char *buffer, int count, int *int_array);
#endif

/* should be using the or_pack_value family instead ! */
extern char *or_pack_db_value (char *buffer, DB_VALUE * var);
extern char *or_unpack_db_value (char *buffer, DB_VALUE * val);
extern int or_db_value_size (DB_VALUE * var);

/* Data unpacking functions */
extern char *or_unpack_int (char *ptr, int *number);
#if defined(ENABLE_UNUSED_FUNCTION)
extern char *or_unpack_bigint (char *ptr, DB_BIGINT * number);
#endif
extern char *or_unpack_int64 (char *ptr, INT64 * number);
extern char *or_unpack_int_array (char *ptr, int n, int **number_array);
extern char *or_unpack_longint (char *ptr, int *number);
extern char *or_unpack_short (char *ptr, short *number);
extern char *or_unpack_float (char *ptr, float *number);
extern char *or_unpack_double (char *ptr, double *number);
#if defined(ENABLE_UNUSED_FUNCTION)
extern char *or_unpack_time (char *ptr, DB_TIME * time);
extern char *or_unpack_date (char *ptr, DB_DATE * date);
extern char *or_unpack_monetary (char *ptr, DB_MONETARY * money);
extern char *or_unpack_utime (char *ptr, DB_UTIME * utime);
#endif
extern char *or_unpack_string (char *ptr, char **string);
extern char *or_unpack_string_nocopy (char *ptr, char **string);
extern char *or_unpack_errcode (char *ptr, int *error);
extern char *or_unpack_oid (char *ptr, OID * oid);
extern char *or_unpack_oid_array (char *ptr, int n, OID ** oids);
extern char *or_unpack_loid (char *ptr, LOID * loid);
extern char *or_unpack_hfid (char *ptr, HFID * hfid);
extern char *or_unpack_hfid_array (char *ptr, int n, HFID ** hfids);
extern char *or_unpack_btid (char *buf, BTID * btid);
extern char *or_unpack_ehid (char *buf, EHID * btid);
extern char *or_unpack_listid (char *ptr, void **listid_ptr);
extern char *or_unpack_unbound_listid (char *ptr, void **listid_ptr);
extern char *or_unpack_lock (char *ptr, LOCK * lock);
extern char *or_unpack_set_header (char *buf, DB_TYPE * stype,
				   DB_TYPE * etype, int *bound_bits,
				   int *size);

extern char *or_unpack_var_table (char *ptr, int nvars, OR_VARINFO * vars);
extern char *or_unpack_method_sig_list (char *ptr,
					void **method_sig_list_ptr);
extern char *or_unpack_set_node (char *ptr, void *set_node_ptr);
#if defined(ENABLE_UNUSED_FUNCTION)
extern char *or_unpack_string_array (char *buffer, char ***string_array,
				     int *cnt);
extern char *or_unpack_db_value_array (char *buffer, DB_VALUE ** val,
				       int *count);
extern char *or_unpack_elo (char *ptr, void **elo_ptr);
#endif
extern char *or_pack_ptr (char *ptr, UINTPTR ptrval);
extern char *or_unpack_ptr (char *ptr, UINTPTR * ptrval);

/* pack/unpack support functions */
extern int or_packed_string_length (const char *string);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int or_align_length (int length);
#endif /* ENABLE_UNUSED_FUNCTION */
extern int or_packed_varbit_length (int bitlen);
extern int or_packed_varchar_length (int charlen);

/*
 * to avoid circular dependencies, don't require the definition of QFILE_LIST_ID in
 * this file (it references DB_TYPE)
 */
extern int or_listid_length (void *listid);
extern int or_method_sig_list_length (void *method_sig_list_ptr);
extern int or_set_node_length (void *set_node_ptr);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int or_elo_length (void *elo_ptr);
extern int or_packed_string_array_length (int count,
					  const char **string_array);
extern int or_packed_db_value_array_length (int count, DB_VALUE * val);
#endif

extern void or_encode (char *buffer, const char *source, int size);
extern void or_decode (const char *buffer, char *dest, int size);

extern void or_init (OR_BUF * buf, char *data, int length);

/* These are called when overflow/underflow are detected */
extern int or_overflow (OR_BUF * buf);
extern int or_underflow (OR_BUF * buf);
extern void or_abort (OR_BUF * buf);

/* Data packing functions */
extern int or_put_byte (OR_BUF * buf, int num);
extern int or_put_short (OR_BUF * buf, int num);
extern int or_put_int (OR_BUF * buf, int num);
extern int or_put_bigint (OR_BUF * buf, DB_BIGINT num);
extern int or_put_float (OR_BUF * buf, float num);
extern int or_put_double (OR_BUF * buf, double num);
extern int or_put_time (OR_BUF * buf, DB_TIME * timeval);
extern int or_put_utime (OR_BUF * buf, DB_UTIME * timeval);
extern int or_put_date (OR_BUF * buf, DB_DATE * date);
extern int or_put_monetary (OR_BUF * buf, DB_MONETARY * monetary);
extern int or_put_string (OR_BUF * buf, char *string);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int or_put_binary (OR_BUF * buf, DB_BINARY * binary);
#endif
extern int or_put_data (OR_BUF * buf, char *data, int length);
extern int or_put_oid (OR_BUF * buf, OID * oid);
extern int or_put_loid (OR_BUF * buf, LOID * loid);
extern int or_put_varbit (OR_BUF * buf, char *string, int bitlen);
extern int or_put_varchar (OR_BUF * buf, char *string, int charlen);
extern int or_put_align32 (OR_BUF * buf);

/* Data unpacking functions */
extern int or_get_byte (OR_BUF * buf, int *error);
extern int or_get_short (OR_BUF * buf, int *error);
extern int or_get_int (OR_BUF * buf, int *error);
extern DB_BIGINT or_get_bigint (OR_BUF * buf, int *error);
extern float or_get_float (OR_BUF * buf, int *error);
extern double or_get_double (OR_BUF * buf, int *error);
extern int or_get_time (OR_BUF * buf, DB_TIME * timeval);
extern int or_get_utime (OR_BUF * buf, DB_UTIME * timeval);
extern int or_get_date (OR_BUF * buf, DB_DATE * date);
extern int or_put_datetime (OR_BUF * buf, DB_DATETIME * datetimeval);
extern int or_get_datetime (OR_BUF * buf, DB_DATETIME * datetime);
extern int or_get_monetary (OR_BUF * buf, DB_MONETARY * monetary);
extern int or_get_data (OR_BUF * buf, char *data, int length);
extern int or_get_oid (OR_BUF * buf, OID * oid);
extern int or_get_loid (OR_BUF * buf, LOID * loid);

extern int or_skip_varchar_remainder (OR_BUF * buf, int charlen);
extern int or_skip_varchar (OR_BUF * buf);
extern int or_skip_varbit (OR_BUF * buf);
extern int or_skip_varbit_remainder (OR_BUF * buf, int bitlen);

/* Pack/unpack support functions */
extern int or_advance (OR_BUF * buf, int offset);
extern int or_seek (OR_BUF * buf, int psn);
extern int or_pad (OR_BUF * buf, int length);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int or_length_string (char *string);
extern int or_length_binary (DB_BINARY * binary);
#endif

extern int or_get_varchar_length (OR_BUF * buf, int *intval);
extern int or_get_align32 (OR_BUF * buf);
extern int or_get_align64 (OR_BUF * buf);
#if defined(ENABLE_UNUSED_FUNCTION)
extern char *or_get_varchar (OR_BUF * buf, int *length_ptr);
extern char *or_get_varbit (OR_BUF * buf, int *length_ptr);
#endif
extern int or_get_varbit_length (OR_BUF * buf, int *intval);

extern OR_VARINFO *or_get_var_table (OR_BUF * buf, int nvars,
				     char *(*allocator) (int));

/* DOMAIN functions */
extern int or_packed_domain_size (struct tp_domain *domain,
				  int include_classoids);
extern char *or_pack_domain (char *ptr, struct tp_domain *domain,
			     int include_classoids, int is_null);
extern char *or_unpack_domain (char *ptr, struct tp_domain **domain_ptr,
			       int *is_null);
extern int or_put_domain (OR_BUF * buf, struct tp_domain *domain,
			  int include_classoids, int is_null);
extern struct tp_domain *or_get_domain (OR_BUF * buf, struct tp_domain *dom,
					int *is_null);
extern int or_put_sub_domain (OR_BUF * buf);

/* SET functions */
extern void or_packed_set_info (DB_TYPE set_type,
				struct tp_domain *domain,
				int include_domain,
				int *bound_bits,
				int *offset_table,
				int *element_tags, int *element_size);

extern int or_put_set_header (OR_BUF * buf, DB_TYPE set_type,
			      int size, int domain, int bound_bits,
			      int offset_table, int element_tags,
			      int common_sub_header);

extern int or_get_set_header (OR_BUF * buf, DB_TYPE * set_type,
			      int *size, int *domain,
			      int *bound_bits, int *offset_table,
			      int *element_tags, int *common_sub_header);

extern int or_skip_set_header (OR_BUF * buf);

extern int or_packed_set_length (SETOBJ * set, int include_domain);

extern void or_put_set (OR_BUF * buf, SETOBJ * set, int include_domain);

extern SETOBJ *or_get_set (OR_BUF * buf, struct tp_domain *domain);
extern int or_disk_set_size (OR_BUF * buf, struct tp_domain *domain,
			     DB_TYPE * set_type);

/* DB_VALUE functions */
extern int or_packed_value_size (DB_VALUE * value,
				 int collapse_null,
				 int include_domain,
				 int include_domain_classoids);

extern int or_put_value (OR_BUF * buf, DB_VALUE * value,
			 int collapse_null, int include_domain,
			 int include_domain_classoids);

extern int or_get_value (OR_BUF * buf, DB_VALUE * value,
			 struct tp_domain *domain, int expected, bool copy);

extern char *or_pack_value (char *buf, DB_VALUE * value);
extern char *or_pack_mem_value (char *buf, DB_VALUE * value);
extern char *or_unpack_value (char *buf, DB_VALUE * value);
extern char *or_unpack_mem_value (char *buf, DB_VALUE * value);

#endif /* _OBJECT_REPRESENTATION_H_ */
