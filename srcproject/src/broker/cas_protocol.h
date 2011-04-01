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
 * cas_protocol.h -
 */

#ifndef _CAS_PROTOCOL_H_
#define _CAS_PROTOCOL_H_

#ident "$Id$"

#define SRV_CON_CLIENT_INFO_SIZE	10
#define SRV_CON_CLIENT_MAGIC_LEN	5
#define SRV_CON_CLIENT_MAGIC_STR	"CUBRK"
#define SRV_CON_MSG_IDX_CLIENT_TYPE	5
#define SRV_CON_MSG_IDX_MAJOR_VER	6
#define SRV_CON_MSG_IDX_MINOR_VER	7
#define SRV_CON_MSG_IDX_PATCH_VER	8

#define SRV_CON_DBNAME_SIZE		32
#define SRV_CON_DBUSER_SIZE		32
#define SRV_CON_DBPASSWD_SIZE		32
#define SRV_CON_URL_SIZE                512
#define SRV_CON_DB_INFO_SIZE \
        (SRV_CON_DBNAME_SIZE + SRV_CON_DBUSER_SIZE + SRV_CON_DBPASSWD_SIZE + \
         SRV_CON_URL_SIZE)
#define SRV_CON_DB_INFO_SIZE_PRIOR_8_2_0 \
        (SRV_CON_DBNAME_SIZE + SRV_CON_DBUSER_SIZE + SRV_CON_DBPASSWD_SIZE)

typedef enum
{
  CAS_CLIENT_TYPE_MIN = 0,
  CAS_CLIENT_NONE = 0,
  CAS_CLIENT_CCI = 1,
  CAS_CLIENT_ODBC = 2,
  CAS_CLIENT_JDBC = 3,
  CAS_CLIENT_PHP = 4,
  CAS_CLIENT_OLEDB = 5,
  CAS_CLIENT_TYPE_MAX = 5
} CAS_CLIENT_TYPE;

typedef enum
{
  CAS_INFO_STATUS_INACTIVE = 0,
  CAS_INFO_STATUS_ACTIVE = 1
} CAS_INFO_STATUS_TYPE;

typedef enum
{
  CAS_INFO_STATUS = 0,
  CAS_INFO_RESERVED_1 = 1,
  CAS_INFO_RESERVED_2 = 2,
  CAS_INFO_RESERVED_3 = 3
} CAS_INFO_TYPE;

#define CAS_INFO_SIZE			(4)
#define CAS_INFO_RESERVED_DEFAULT	(-1)

#define MSG_HEADER_INFO_SIZE        CAS_INFO_SIZE
#define MSG_HEADER_MSG_SIZE         ((int) sizeof(int))
#define MSG_HEADER_SIZE             (MSG_HEADER_INFO_SIZE +  MSG_HEADER_MSG_SIZE)

#define BROKER_INFO_SIZE			4
#define BROKER_INFO_DBMS_TYPE                   0
#define BROKER_INFO_KEEP_CONNECTION             1
#define BROKER_INFO_STATEMENT_POOLING           2
#define BROKER_INFO_CCI_PCONNECT		3

#define CAS_KEEP_CONNECTION_OFF			0
#define CAS_KEEP_CONNECTION_ON			1

#define CAS_GET_QUERY_INFO_PLAN			1
#define CAS_GET_QUERY_INFO_HISTOGRAM		2

#define CAS_STATEMENT_POOLING_OFF		0
#define CAS_STATEMENT_POOLING_ON		1

#define CCI_PCONNECT_OFF                        0
#define CCI_PCONNECT_ON                         1

#define CAS_REQ_HEADER_JDBC	"JDBC"
#define CAS_REQ_HEADER_ODBC	"ODBC"
#define CAS_REQ_HEADER_PHP	"PHP"
#define CAS_REQ_HEADER_OLEDB	"OLEDB"
#define CAS_REQ_HEADER_CCI	"CCI"

#define CAS_GLO_NEW_LO		1
#define CAS_GLO_NEW_FBO		2

#define CAS_METHOD_USER_ERROR_BASE	-10000

typedef enum t_cas_func_code T_CAS_FUNC_CODE;
enum t_cas_func_code
{
  CAS_FC_END_TRAN = 1,
  CAS_FC_PREPARE = 2,
  CAS_FC_EXECUTE = 3,
  CAS_FC_GET_DB_PARAMETER = 4,
  CAS_FC_SET_DB_PARAMETER = 5,
  CAS_FC_CLOSE_REQ_HANDLE = 6,
  CAS_FC_CURSOR = 7,
  CAS_FC_FETCH = 8,
  CAS_FC_SCHEMA_INFO = 9,
  CAS_FC_OID_GET = 10,
  CAS_FC_OID_PUT = 11,
  CAS_FC_GLO_NEW = 12,
  CAS_FC_GLO_SAVE = 13,
  CAS_FC_GLO_LOAD = 14,
  CAS_FC_GET_DB_VERSION = 15,
  CAS_FC_GET_CLASS_NUM_OBJS = 16,
  CAS_FC_OID_CMD = 17,
  CAS_FC_COLLECTION = 18,
  CAS_FC_NEXT_RESULT = 19,
  CAS_FC_EXECUTE_BATCH = 20,
  CAS_FC_EXECUTE_ARRAY = 21,
  CAS_FC_CURSOR_UPDATE = 22,
  CAS_FC_GET_ATTR_TYPE_STR = 23,
  CAS_FC_GET_QUERY_INFO = 24,
  CAS_FC_GLO_CMD = 25,
  CAS_FC_SAVEPOINT = 26,
  CAS_FC_PARAMETER_INFO = 27,
  CAS_FC_XA_PREPARE = 28,
  CAS_FC_XA_RECOVER = 29,
  CAS_FC_XA_END_TRAN = 30,
  CAS_FC_CON_CLOSE = 31,
  CAS_FC_CHECK_CAS = 32,
  CAS_FC_MAKE_OUT_RS = 33,
  CAS_FC_GET_GENERATED_KEYS = 34,

  CAS_FC_MAX
};

typedef enum t_cci_glo_cmd T_CCI_GLO_CMD;
enum t_cci_glo_cmd
{
  GLO_CMD_FIRST = 1,
  GLO_CMD_READ_DATA = 1,
  GLO_CMD_WRITE_DATA = 2,
  GLO_CMD_INSERT_DATA = 3,
  GLO_CMD_DELETE_DATA = 4,
  GLO_CMD_TRUNCATE_DATA = 5,
  GLO_CMD_APPEND_DATA = 6,
  GLO_CMD_DATA_SIZE = 7,
  GLO_CMD_COMPRESS_DATA = 8,
  GLO_CMD_DESTROY_DATA = 9,
  GLO_CMD_LIKE_SEARCH = 10,
  GLO_CMD_REG_SEARCH = 11,
  GLO_CMD_BINARY_SEARCH = 12,
  GLO_CMD_LAST = GLO_CMD_BINARY_SEARCH
};

#define CAS_CUR_VERSION                 \
	CAS_MAKE_VER(MAJOR_VERSION, MINOR_VERSION, PATCH_NUMBER)
#define CAS_MAKE_VER(MAJOR, MINOR, PATCH)       \
	((T_BROKER_VERSION) (((MAJOR) << 16) | ((MINOR) << 8) | (PATCH)))
typedef int T_BROKER_VERSION;

#endif /* _CAS_PROTOCOL_H_ */
