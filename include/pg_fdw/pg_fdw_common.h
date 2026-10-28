#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <postgres.h>
#include <nodes/pg_list.h>
#include <commands/explain.h>
#include <foreign/fdwapi.h>
#include <nodes/makefuncs.h>

#include <pg_fdw/constants.hh>

/** Target list items. */
typedef struct SpringtailTargetColumn {
#if PG_VERSION_NUM < 150000
    Value *attname;
#else
    String *attname;
#endif
    int attnum; ///< The FDW's attribute number for the column
} SpringtailTargetColumn;

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
    /** pg_trgm's "%" similarity operator (btree redesign phase 12) -- the one GIN-eligible
        operator this phase recognizes for gin_trgm_ops columns (see phases/phase-12.md's
        Design notes for why only this operator, not also LIKE/regex, this phase). Never
        reaches a btree range-scan bound comparison the way the ops above do -- PgFdwMgr
        routes it to a GinReadStrategy lookup instead, entirely outside _set_scan_iterators'
        switch, except for the placeholder case that switch needs to stay exhaustive. */
    GIN_SIMILAR,
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
