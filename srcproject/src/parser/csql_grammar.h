/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison GLR parsers in C

   Copyright (C) 2002, 2003, 2004, 2005, 2006 Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     ABSOLUTE_ = 258,
     ACTION = 259,
     ADD = 260,
     ADD_MONTHS = 261,
     AFTER = 262,
     ALIAS = 263,
     ALL = 264,
     ALLOCATE = 265,
     ALTER = 266,
     AND = 267,
     ANY = 268,
     ARE = 269,
     AS = 270,
     ASC = 271,
     ASSERTION = 272,
     ASYNC = 273,
     AT = 274,
     ATTACH = 275,
     ATTRIBUTE = 276,
     AVG = 277,
     BEFORE = 278,
     BEGIN_ = 279,
     BETWEEN = 280,
     BIGINT = 281,
     BIT = 282,
     BIT_LENGTH = 283,
     BITSHIFT_LEFT = 284,
     BITSHIFT_RIGHT = 285,
     BOOLEAN_ = 286,
     BOTH_ = 287,
     BREADTH = 288,
     BY = 289,
     CALL = 290,
     CASCADE = 291,
     CASCADED = 292,
     CASE = 293,
     CAST = 294,
     CATALOG = 295,
     CHANGE = 296,
     CHAR_ = 297,
     CHECK = 298,
     CLASS = 299,
     CLASSES = 300,
     CLOSE = 301,
     CLUSTER = 302,
     COALESCE = 303,
     COLLATE = 304,
     COLLATION = 305,
     COLUMN = 306,
     COMMIT = 307,
     COMP_NULLSAFE_EQ = 308,
     COMPLETION = 309,
     CONNECT = 310,
     CONNECT_BY_ISCYCLE = 311,
     CONNECT_BY_ISLEAF = 312,
     CONNECT_BY_ROOT = 313,
     CONNECTION = 314,
     CONSTRAINT = 315,
     CONSTRAINTS = 316,
     CONTINUE = 317,
     CONVERT = 318,
     CORRESPONDING = 319,
     COUNT = 320,
     CREATE = 321,
     CROSS = 322,
     CURRENT = 323,
     CURRENT_DATE = 324,
     CURRENT_DATETIME = 325,
     CURRENT_TIME = 326,
     CURRENT_TIMESTAMP = 327,
     CURRENT_USER = 328,
     CURSOR = 329,
     CYCLE = 330,
     DATA = 331,
     DATABASE = 332,
     DATA_TYPE = 333,
     Date = 334,
     DATETIME = 335,
     DAY_ = 336,
     DAY_MILLISECOND = 337,
     DAY_SECOND = 338,
     DAY_MINUTE = 339,
     DAY_HOUR = 340,
     DEALLOCATE = 341,
     DECLARE = 342,
     DEFAULT = 343,
     DEFERRABLE = 344,
     DEFERRED = 345,
     DELETE_ = 346,
     DEPTH = 347,
     DESC = 348,
     DESCRIBE = 349,
     DESCRIPTOR = 350,
     DIAGNOSTICS = 351,
     DICTIONARY = 352,
     DIFFERENCE_ = 353,
     DISCONNECT = 354,
     DISTINCT = 355,
     DISTINCTROW = 356,
     DIV = 357,
     DO = 358,
     Domain = 359,
     Double = 360,
     DROP = 361,
     DUPLICATE_ = 362,
     EACH = 363,
     ELSE = 364,
     ELSEIF = 365,
     END = 366,
     EQUALS = 367,
     ESCAPE = 368,
     EVALUATE = 369,
     EXCEPT = 370,
     EXCEPTION = 371,
     EXCLUDE = 372,
     EXEC = 373,
     EXECUTE = 374,
     EXISTS = 375,
     EXTERNAL = 376,
     EXTRACT = 377,
     False = 378,
     FETCH = 379,
     File = 380,
     FIRST = 381,
     FLOAT_ = 382,
     For = 383,
     FOREIGN = 384,
     FOUND = 385,
     FROM = 386,
     FULL = 387,
     FUNCTION = 388,
     GENERAL = 389,
     GET = 390,
     GLOBAL = 391,
     GO = 392,
     GOTO = 393,
     GRANT = 394,
     GROUP_ = 395,
     HAVING = 396,
     HOUR_ = 397,
     HOUR_MILLISECOND = 398,
     HOUR_SECOND = 399,
     HOUR_MINUTE = 400,
     IDENTITY = 401,
     IF = 402,
     IGNORE_ = 403,
     IMMEDIATE = 404,
     IN_ = 405,
     INDEX = 406,
     INDICATOR = 407,
     INHERIT = 408,
     INITIALLY = 409,
     INNER = 410,
     INOUT = 411,
     INPUT_ = 412,
     INSERT = 413,
     INTEGER = 414,
     INTERSECT = 415,
     INTERSECTION = 416,
     INTERVAL = 417,
     INTO = 418,
     IS = 419,
     ISOLATION = 420,
     JOIN = 421,
     KEY = 422,
     LANGUAGE = 423,
     LAST = 424,
     LDB = 425,
     LEADING_ = 426,
     LEAVE = 427,
     LEFT = 428,
     LESS = 429,
     LEVEL = 430,
     LIKE = 431,
     LIMIT = 432,
     LIST = 433,
     LOCAL = 434,
     LOCAL_TRANSACTION_ID = 435,
     LOCALTIME = 436,
     LOCALTIMESTAMP = 437,
     LOOP = 438,
     LOWER = 439,
     MATCH = 440,
     Max = 441,
     METHOD = 442,
     MILLISECOND_ = 443,
     Min = 444,
     MINUTE_ = 445,
     MINUTE_MILLISECOND = 446,
     MINUTE_SECOND = 447,
     MOD = 448,
     MODIFY = 449,
     MODULE = 450,
     Monetary = 451,
     MONTH_ = 452,
     MULTISET = 453,
     MULTISET_OF = 454,
     NA = 455,
     NAMES = 456,
     NATIONAL = 457,
     NATURAL = 458,
     NCHAR = 459,
     NEXT = 460,
     NO = 461,
     NONE = 462,
     NOT = 463,
     Null = 464,
     NULLIF = 465,
     NUMERIC = 466,
     OBJECT = 467,
     OCTET_LENGTH = 468,
     OF = 469,
     OFF_ = 470,
     OID_ = 471,
     ON_ = 472,
     ONLY = 473,
     OPEN = 474,
     OPERATION = 475,
     OPERATORS = 476,
     OPTIMIZATION = 477,
     OPTION = 478,
     OR = 479,
     ORDER = 480,
     OTHERS = 481,
     OUT_ = 482,
     OUTER = 483,
     OUTPUT = 484,
     OVERLAPS = 485,
     PARAMETERS = 486,
     PARTIAL = 487,
     PENDANT = 488,
     POSITION = 489,
     PRECISION = 490,
     PREORDER = 491,
     PREPARE = 492,
     PRESERVE = 493,
     PRIMARY = 494,
     PRIOR = 495,
     Private = 496,
     PRIVILEGES = 497,
     PROCEDURE = 498,
     PROTECTED = 499,
     PROXY = 500,
     QUERY = 501,
     READ = 502,
     REBUILD = 503,
     RECURSIVE = 504,
     REF = 505,
     REFERENCES = 506,
     REFERENCING = 507,
     RELATIVE_ = 508,
     RENAME = 509,
     REPLACE = 510,
     RESIGNAL = 511,
     RESTRICT = 512,
     RETURN = 513,
     RETURNS = 514,
     REVOKE = 515,
     RIGHT = 516,
     ROLE = 517,
     ROLLBACK = 518,
     ROLLUP = 519,
     ROUTINE = 520,
     ROW = 521,
     ROWNUM = 522,
     ROWS = 523,
     SAVEPOINT = 524,
     SCHEMA = 525,
     SCOPE = 526,
     SCROLL = 527,
     SEARCH = 528,
     SECOND_ = 529,
     SECOND_MILLISECOND = 530,
     SECTION = 531,
     SELECT = 532,
     SENSITIVE = 533,
     SEQUENCE = 534,
     SEQUENCE_OF = 535,
     SERIALIZABLE = 536,
     SESSION = 537,
     SESSION_USER = 538,
     SET = 539,
     SET_OF = 540,
     SETEQ = 541,
     SETNEQ = 542,
     SHARED = 543,
     SIBLINGS = 544,
     SIGNAL = 545,
     SIMILAR = 546,
     SIZE_ = 547,
     SmallInt = 548,
     SOME = 549,
     SQL = 550,
     SQLCODE = 551,
     SQLERROR = 552,
     SQLEXCEPTION = 553,
     SQLSTATE = 554,
     SQLWARNING = 555,
     STATISTICS = 556,
     String = 557,
     STRUCTURE = 558,
     SUBCLASS = 559,
     SUBSET = 560,
     SUBSETEQ = 561,
     SUBSTRING_ = 562,
     SUM = 563,
     SUPERCLASS = 564,
     SUPERSET = 565,
     SUPERSETEQ = 566,
     SYS_CONNECT_BY_PATH = 567,
     SYS_DATE = 568,
     SYS_DATETIME = 569,
     SYS_TIME_ = 570,
     SYS_TIMESTAMP = 571,
     SYS_USER = 572,
     SYSTEM_USER = 573,
     TABLE = 574,
     TEMPORARY = 575,
     TEST = 576,
     THEN = 577,
     THERE = 578,
     Time = 579,
     TIMESTAMP = 580,
     TIMEZONE_HOUR = 581,
     TIMEZONE_MINUTE = 582,
     TO = 583,
     TRAILING_ = 584,
     TRANSACTION = 585,
     TRANSLATE = 586,
     TRANSLATION = 587,
     TRIGGER = 588,
     TRIM = 589,
     True = 590,
     TRUNCATE = 591,
     TYPE = 592,
     UNDER = 593,
     Union = 594,
     UNIQUE = 595,
     UNKNOWN = 596,
     UNTERMINATED_STRING = 597,
     UNTERMINATED_IDENTIFIER = 598,
     UPDATE = 599,
     UPPER = 600,
     USAGE = 601,
     USE = 602,
     USER = 603,
     USING = 604,
     Utime = 605,
     VALUE = 606,
     VALUES = 607,
     VARCHAR = 608,
     VARIABLE_ = 609,
     VARYING = 610,
     VCLASS = 611,
     VIEW = 612,
     VIRTUAL = 613,
     VISIBLE = 614,
     WAIT = 615,
     WHEN = 616,
     WHENEVER = 617,
     WHERE = 618,
     WHILE = 619,
     WITH = 620,
     WITHOUT = 621,
     WORK = 622,
     WRITE = 623,
     XOR = 624,
     YEAR_ = 625,
     YEAR_MONTH = 626,
     ZONE = 627,
     DOLLAR_SIGN = 628,
     WON_SIGN = 629,
     YUAN_SIGN = 630,
     RIGHT_ARROW = 631,
     STRCAT = 632,
     COMP_NOT_EQ = 633,
     COMP_GE = 634,
     COMP_LE = 635,
     PARAM_HEADER = 636,
     ACTIVE = 637,
     ADDDATE = 638,
     ANALYZE = 639,
     AUTO_INCREMENT = 640,
     BIT_AND = 641,
     BIT_OR = 642,
     BIT_XOR = 643,
     CACHE = 644,
     COMMITTED = 645,
     COST = 646,
     DATE_ADD = 647,
     DATE_SUB = 648,
     DECREMENT = 649,
     GE_INF_ = 650,
     GE_LE_ = 651,
     GE_LT_ = 652,
     GROUPS = 653,
     GT_INF_ = 654,
     GT_LE_ = 655,
     GT_LT_ = 656,
     HASH = 657,
     IFNULL = 658,
     INACTIVE = 659,
     INCREMENT = 660,
     INF_LE_ = 661,
     INF_LT_ = 662,
     INFINITE_ = 663,
     INSTANCES = 664,
     INVALIDATE = 665,
     ISNULL = 666,
     JAVA = 667,
     LCASE = 668,
     LOCK_ = 669,
     MAXIMUM = 670,
     MAXVALUE = 671,
     MEMBERS = 672,
     MINVALUE = 673,
     NAME = 674,
     NOCYCLE = 675,
     NOCACHE = 676,
     NODE = 677,
     NOMAXVALUE = 678,
     NOMINVALUE = 679,
     PARTITION = 680,
     PARTITIONING = 681,
     PARTITIONS = 682,
     PASSWORD = 683,
     PRINT = 684,
     PRIORITY = 685,
     QUARTER = 686,
     RANGE_ = 687,
     REJECT_ = 688,
     REMOVE = 689,
     REGISTER = 690,
     REORGANIZE = 691,
     REPEATABLE = 692,
     RETAIN = 693,
     REUSE_OID = 694,
     REVERSE = 695,
     SCHEMA_SYNC = 696,
     SERIAL = 697,
     STABILITY = 698,
     START_ = 699,
     STATEMENT = 700,
     STATUS = 701,
     STDDEV = 702,
     STR_TO_DATE = 703,
     SUBDATE = 704,
     SYSTEM = 705,
     THAN = 706,
     TIMEOUT = 707,
     TRACE = 708,
     TRIGGERS = 709,
     UCASE = 710,
     UNCOMMITTED = 711,
     UNREGISTER = 712,
     VARIANCE = 713,
     WEEK = 714,
     WORKSPACE = 715,
     IdName = 716,
     BracketDelimitedIdName = 717,
     BacktickDelimitedIdName = 718,
     DelimitedIdName = 719,
     UNSIGNED_INTEGER = 720,
     UNSIGNED_REAL = 721,
     CHAR_STRING = 722,
     NCHAR_STRING = 723,
     BIT_STRING = 724,
     HEX_STRING = 725,
     CPP_STYLE_HINT = 726,
     C_STYLE_HINT = 727,
     SQL_STYLE_HINT = 728
   };
#endif


/* Copy the first part of user declarations.  */
#line 28 "../../src/parser/csql_grammar.y"

#define YYMAXDEPTH	1000000

/* #define PARSER_DEBUG */

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include <errno.h>

#include "parser.h"
#include "parser_message.h"
#include "dbdef.h"
#include "language_support.h"
#include "environment_variable.h"
#include "transaction_cl.h"
#include "csql_grammar_scan.h"
#include "system_parameter.h"
#define JP_MAXNAME 256
#if defined(WINDOWS)
#define snprintf _snprintf
#endif /* WINDOWS */
#include "memory_alloc.h"

/* Bit mask to be used to check constraints of a column.
 * COLUMN_CONSTRAINT_SHARED_DEFAULT_AI is special-purpose mask
 * to identify duplication of SHARED, DEFAULT and AUTO_INCREMENT.
 */
#define COLUMN_CONSTRAINT_UNIQUE		(0x01)
#define COLUMN_CONSTRAINT_PRIMARY_KEY		(0x02)
#define COLUMN_CONSTRAINT_NULL			(0x04)
#define COLUMN_CONSTRAINT_OTHERS		(0x08)
#define COLUMN_CONSTRAINT_SHARED		(0x10)
#define COLUMN_CONSTRAINT_DEFAULT		(0x20)
#define COLUMN_CONSTRAINT_AUTO_INCREMENT	(0x40)

#define COLUMN_CONSTRAINT_SHARED_DEFAULT_AI	(0x70)


#ifdef PARSER_DEBUG
#define DBG_PRINT printf("rule matched at line: %d\n", __LINE__);
#define PRINT_(a) printf(a)
#define PRINT_1(a, b) printf(a, b)
#define PRINT_2(a, b, c) printf(a, b, c)
#else
#define DBG_PRINT
#define PRINT_(a)
#define PRINT_1(a, b)
#define PRINT_2(a, b, c)
#endif

#define STACK_SIZE	128

typedef struct function_map FUNCTION_MAP;
struct function_map
{
  const char* keyword;
  PT_OP_TYPE op;
};


static FUNCTION_MAP functions[] = {
  {"abs", PT_ABS},
  {"acos", PT_ACOS},
  {"asin", PT_ASIN},
  {"atan", PT_ATAN},
  {"atan2", PT_ATAN2},
  {"bit_count", PT_BIT_COUNT},
  {"ceil", PT_CEIL},
  {"ceiling", PT_CEIL},
  {"char_length", PT_CHAR_LENGTH},
  {"character_length", PT_CHAR_LENGTH},
  {"chr", PT_CHR},
  {"concat", PT_CONCAT},
  {"concat_ws", PT_CONCAT_WS},
  {"cos", PT_COS},
  {"cot", PT_COT},
  {"curtime", PT_SYS_TIME},
  {"curdate", PT_SYS_DATE},
  {"datediff", PT_DATEDIFF},
  {"date_format", PT_DATE_FORMAT},
  {"decode", PT_DECODE},
  {"decr", PT_DECR},
  {"degrees", PT_DEGREES},
  {"drand", PT_DRAND},
  {"drandom", PT_DRANDOM},
  {"exp", PT_EXP},
  {"field", PT_FIELD},
  {"floor", PT_FLOOR},
  {"greatest", PT_GREATEST},
  {"groupby_num", PT_GROUPBY_NUM},
  {"incr", PT_INCR},
  {"inst_num", PT_INST_NUM},
  {"instr", PT_INSTR},
  {"instrb", PT_INSTR},
  {"last_day", PT_LAST_DAY},
  {"length", PT_CHAR_LENGTH},
  {"lengthb", PT_CHAR_LENGTH},
  {"least", PT_LEAST},
  {"list_dbs", PT_LIST_DBS},
  {"locate", PT_LOCATE},
  {"ln", PT_LN},
  {"log2", PT_LOG2},
  {"log10", PT_LOG10},
  {"log", PT_LOG},
  {"lpad", PT_LPAD},
  {"ltrim", PT_LTRIM},
  {"mid", PT_MID},
  {"months_between", PT_MONTHS_BETWEEN},
  {"format", PT_FORMAT},
  {"now", PT_SYS_DATETIME},
  {"nvl", PT_NVL},
  {"nvl2", PT_NVL2},
  {"orderby_num", PT_ORDERBY_NUM},
  {"power", PT_POWER},
  {"pow", PT_POWER},
  {"pi", PT_PI},
  {"radians", PT_RADIANS},
  {"rand", PT_RAND},
  {"random", PT_RANDOM},
  {"reverse", PT_REVERSE},
  {"round", PT_ROUND},
  {"row_count", PT_ROW_COUNT},
  {"rpad", PT_RPAD},
  {"rtrim", PT_RTRIM},
  {"sign", PT_SIGN},
  {"sin", PT_SIN},
  {"sqrt", PT_SQRT},
  {"strcmp", PT_STRCMP},
  {"substr", PT_SUBSTRING},
  {"substrb", PT_SUBSTRING},
  {"tan", PT_TAN},
  {"time_format", PT_TIME_FORMAT},
  {"to_char", PT_TO_CHAR},
  {"to_date", PT_TO_DATE},
  {"to_datetime",PT_TO_DATETIME},
  {"to_number", PT_TO_NUMBER},
  {"to_time", PT_TO_TIME},
  {"to_timestamp", PT_TO_TIMESTAMP},
  {"trunc", PT_TRUNC},
  {"unix_timestamp", PT_UNIX_TIMESTAMP}
};


static int parser_groupby_exception = 0;




/* xxxnum_check: 0 not allowed, no compatibility check
		 1 allowed, compatibility check (search_condition)
		 2 allowed, no compatibility check (select_list) */
static int parser_instnum_check = 0;
static int parser_groupbynum_check = 0;
static int parser_orderbynum_check = 0;
static int parser_within_join_condition = 0;

/* xxx_check: 0 not allowed
              1 allowed */
static int parser_sysconnectbypath_check = 0;
static int parser_prior_check = 0;
static int parser_connectbyroot_check = 0;
static int parser_serial_check = 1;
static int parser_pseudocolumn_check = 1;
static int parser_subquery_check = 1;
static int parser_hostvar_check = 1;

/* check Oracle style outer-join operator: '(+)' */
static bool parser_found_Oracle_outer = false;

/* check sys_date, sys_time, sys_timestamp, sys_datetime local_transaction_id */
static bool parser_si_datetime = false;
static bool parser_si_tran_id = false;

/* check the condition that the statment is not able to be prepared */
static bool parser_cannot_prepare = false;

/* check the condition that the result of a query is not able to be cached */
static bool parser_cannot_cache = false;

/* check if INCR is used legally */
static int parser_select_level = -1;

/* handle inner increment exprs in select list */
static PT_NODE *parser_hidden_incr_list = NULL;

typedef struct {
	PT_NODE* c1;
	PT_NODE* c2;
} container_2;

typedef struct {
	PT_NODE* c1;
	PT_NODE* c2;
	PT_NODE* c3;
} container_3;

typedef struct {
	PT_NODE* c1;
	PT_NODE* c2;
	PT_NODE* c3;
	PT_NODE* c4;
} container_4;

typedef struct {
	PT_NODE* c1;
	PT_NODE* c2;
	PT_NODE* c3;
	PT_NODE* c4;
	PT_NODE* c5;
	PT_NODE* c6;
	PT_NODE* c7;
	PT_NODE* c8;
	PT_NODE* c9;
	PT_NODE* c10;
} container_10;

#define PT_EMPTY INT_MAX

#if defined(WINDOWS)
#define inline
#endif


#define TO_NUMBER(a)			((UINTPTR)(a))
#define FROM_NUMBER(a)			((PT_NODE*)(UINTPTR)(a))


#define SET_CONTAINER_2(a, i, j)		a.c1 = i, a.c2 = j
#define SET_CONTAINER_3(a, i, j, k)		a.c1 = i, a.c2 = j, a.c3 = k
#define SET_CONTAINER_4(a, i, j, k, l)		a.c1 = i, a.c2 = j, a.c3 = k, a.c4 = l

#define CONTAINER_AT_0(a)			(a).c1
#define CONTAINER_AT_1(a)			(a).c2
#define CONTAINER_AT_2(a)			(a).c3
#define CONTAINER_AT_3(a)			(a).c4
#define CONTAINER_AT_4(a)			(a).c5
#define CONTAINER_AT_5(a)			(a).c6
#define CONTAINER_AT_6(a)			(a).c7
#define CONTAINER_AT_7(a)			(a).c8
#define CONTAINER_AT_8(a)			(a).c9
#define CONTAINER_AT_9(a)			(a).c10

#define DOLLAR_SIGN_TEXT	"$"
#define WON_SIGN_TEXT		"\\"
#define YUAN_SIGN_TEXT		"Y"

typedef enum
{
  SERIAL_START,
  SERIAL_INC,
  SERIAL_MAX,
  SERIAL_MIN,
  SERIAL_CYCLE,
  SERIAL_CACHE,
} SERIAL_DEFINE;

void csql_yyerror_explicit(int line, int column);
void csql_yyerror(const char* s);

FUNCTION_MAP* keyword_offset(const char* name);
PT_NODE* keyword_func(const char* name, PT_NODE* args);

static PT_NODE* parser_make_expression(PT_OP_TYPE OP, PT_NODE* arg1, PT_NODE* arg2, PT_NODE* arg3);
static PT_NODE* parser_make_link(PT_NODE* list, PT_NODE* node);
static PT_NODE* parser_make_link_or(PT_NODE* list, PT_NODE* node);



static void parser_save_and_set_cannot_cache(bool value);
static void parser_restore_cannot_cache(void);

static void parser_save_and_set_si_datetime(int value);
static void parser_restore_si_datetime(void);

static void parser_save_and_set_si_tran_id(int value);
static void parser_restore_si_tran_id(void);

static void parser_save_and_set_cannot_prepare(bool value);
static void parser_restore_cannot_prepare(void);

static void parser_save_and_set_wjc(int value);
static void parser_restore_wjc(void);

static void parser_save_and_set_ic(int value);
static void parser_restore_ic(void);

static void parser_save_and_set_gc(int value);
static void parser_restore_gc(void);

static void parser_save_and_set_oc(int value);
static void parser_restore_oc(void);

static void parser_save_and_set_sysc(int value);
static void parser_restore_sysc(void);

static void parser_save_and_set_prc(int value);
static void parser_restore_prc(void);

static void parser_save_and_set_cbrc(int value);
static void parser_restore_cbrc(void);

static void parser_save_and_set_serc(int value);
static void parser_restore_serc(void);

static void parser_save_and_set_pseudoc(int value);
static void parser_restore_pseudoc(void);

static void parser_save_and_set_sqc(int value);
static void parser_restore_sqc(void);

static void parser_save_and_set_hvar(int value);
static void parser_restore_hvar(void);

static void parser_save_found_Oracle_outer(void);
static void parser_restore_found_Oracle_outer(void);

static void parser_save_alter_node(PT_NODE* node);
static PT_NODE* parser_get_alter_node(void);

static void parser_save_attr_def_one(PT_NODE* node);
static PT_NODE* parser_get_attr_def_one(void);

static void parser_push_orderby_node(PT_NODE* node);
static PT_NODE* parser_top_orderby_node(void);
static PT_NODE* parser_pop_orderby_node(void);

static void parser_push_select_stmt_node(PT_NODE* node);
static PT_NODE* parser_top_select_stmt_node(void);
static PT_NODE* parser_pop_select_stmt_node(void);


static void parser_push_hint_node(PT_NODE* node);
static PT_NODE* parser_top_hint_node(void);
static PT_NODE* parser_pop_hint_node(void);

static void parser_push_join_type(int v);
static int parser_top_join_type(void);
static int parser_pop_join_type(void);

static void parser_save_is_reverse(bool v);
static bool parser_get_is_reverse(void);

static void parser_stackpointer_init(void);
static PT_NODE* parser_make_date_lang(int arg_cnt, PT_NODE* arg3);
static void parser_remove_dummy_select(PT_NODE** node);
static int parser_count_list(PT_NODE* list);

static void resolve_alias_in_expr_node(PT_NODE * node, PT_NODE * list);
static void resolve_alias_in_name_node(PT_NODE ** node, PT_NODE * list);

static PT_MISC_TYPE parser_attr_type;

static bool allow_attribute_ordering;

int parse_one_statement (int state);


int g_msg[1024];
int msg_ptr;


#define push_msg(a) _push_msg(a, __LINE__)

void _push_msg(int code, int line);
void pop_msg(void);



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE 
#line 406 "../../src/parser/csql_grammar.y"
{
	int number;
	bool boolean;
	PT_NODE* node;
	char* cptr;
	container_2 c2;
	container_3 c3;
	container_4 c4;
	container_10 c10;
}
/* Line 2616 of glr.c.  */
#line 906 "../../src/parser/csql_grammar.h"
	YYSTYPE;
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif

#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{

  int first_line;
  int first_column;
  int last_line;
  int last_column;

} YYLTYPE;
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


extern YYSTYPE csql_yylval;

extern YYLTYPE csql_yylloc;


