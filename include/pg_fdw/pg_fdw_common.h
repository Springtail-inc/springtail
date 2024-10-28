#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "nodes/pg_list.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"

#define SPRINGTAIL_STARTUP_COST 100 ///< Default startup cost is 100, added to tuple cost

#define SPRINGTAIL_FDW_EXTENSION "springtail_fdw"               ///< Name of FDW extension
#define SPRINGTAIL_FDW_SERVER_NAME "springtail_fdw_server"      ///< Name of foreign server on import schema
#define SPRINGTAIL_FDW_CATALOG_SCHEMA "__pg_springtail_catalog" ///< Name of catalog schema

#define SPRINGTAIL_FDW_DB_ID_OPTION "db_id"                  ///< Option name for database id
#define SPRINGTAIL_FDW_DB_NAME_OPTION "db_name"              ///< Option name for database name
#define SPRINGTAIL_FDW_SCHEMA_XID_OPTION "schema_xid"        ///< Option name for schema xid

/** Plan state created in get rel size */
typedef struct SpringtailPlanState {
    uint64_t tid;
    double   width;
    List    *target_list;       ///< List of target columns (int attno)
    List    *pathkeys;          ///< List of de-parsed path keys (DeparsedSortGroup)
    List    *qual_list;         ///< List of predicate clauses (BaseQual)
    void    *pg_fdw_state;
} SpringtailPlanState;

/** Sort group */
typedef struct DeparsedSortGroup
{
    char          *attname;
    int            attnum;
    bool           reversed;
    bool           nulls_first;
    char          *collate;
    PathKey       *key;
} DeparsedSortGroup;

typedef enum {
    UNSUPPORTED,
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    GREATER_THAN,
    GREATER_THAN_EQUALS,
} QualOpName;

/** Base qual for predicates */
typedef struct BaseQual
{
    AttrNumber     varattno;
    NodeTag        right_type;
    Oid            typeoid;
    QualOpName     op;
    char          *opname;
    bool           isArray;
    bool           useOr;
} BaseQual;
typedef BaseQual *BaseQualPtr;

/** Constant predicate */
typedef struct ConstQual
{
    BaseQual       base;
    Datum          value;
    bool           isnull;
} ConstQual;
typedef ConstQual *ConstQualPtr;

/** Variable predicate */
typedef struct VarQual
{
    BaseQual       base;
    AttrNumber     rightvarattno;
} VarQual;

/** Parameterized predicate */
typedef struct ParamQual
{
    BaseQual       base;
    Expr          *expr;
} ParamQual;


#ifdef __cplusplus
}
#endif
