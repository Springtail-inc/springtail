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

/** Base qual for predicates */
typedef struct BaseQual
{
    AttrNumber     varattno;
    NodeTag        right_type;
    Oid            typeoid;
    char          *opname;
    bool           isArray;
    bool           useOr;
} BaseQual;

/** Constant predicate */
typedef struct ConstQual
{
    BaseQual       base;
    Datum          value;
    bool           isnull;
} ConstQual;

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
