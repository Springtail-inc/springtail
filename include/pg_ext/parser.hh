#pragma once

#include <pg_ext/common.hh>
#include <pg_ext/export.hh>
#include <pg_ext/node.hh>

enum class CoercionForm {
    COERCE_EXPLICIT_CALL, /* display as a function call */
    COERCE_EXPLICIT_CAST, /* display as an explicit cast */
    COERCE_IMPLICIT_CAST, /* implicit cast, so hide it */
    COERCE_SQL_SYNTAX     /* display with SQL-mandated special syntax */
};

enum class CoercionContext {
    COERCION_IMPLICIT,   /* coercion in context of expression */
    COERCION_ASSIGNMENT, /* coercion in context of assignment */
    COERCION_PLPGSQL,    /* if no assignment cast, use CoerceViaIO */
    COERCION_EXPLICIT    /* explicit cast operation */
};

enum class ParseExprKind {
    EXPR_KIND_NONE = 0,             /* "not in an expression" */
    EXPR_KIND_OTHER,                /* reserved for extensions */
    EXPR_KIND_JOIN_ON,              /* JOIN ON */
    EXPR_KIND_JOIN_USING,           /* JOIN USING */
    EXPR_KIND_FROM_SUBSELECT,       /* sub-SELECT in FROM clause */
    EXPR_KIND_FROM_FUNCTION,        /* function in FROM clause */
    EXPR_KIND_WHERE,                /* WHERE */
    EXPR_KIND_HAVING,               /* HAVING */
    EXPR_KIND_FILTER,               /* FILTER */
    EXPR_KIND_WINDOW_PARTITION,     /* window definition PARTITION BY */
    EXPR_KIND_WINDOW_ORDER,         /* window definition ORDER BY */
    EXPR_KIND_WINDOW_FRAME_RANGE,   /* window frame clause with RANGE */
    EXPR_KIND_WINDOW_FRAME_ROWS,    /* window frame clause with ROWS */
    EXPR_KIND_WINDOW_FRAME_GROUPS,  /* window frame clause with GROUPS */
    EXPR_KIND_SELECT_TARGET,        /* SELECT target list item */
    EXPR_KIND_INSERT_TARGET,        /* INSERT target list item */
    EXPR_KIND_UPDATE_SOURCE,        /* UPDATE assignment source item */
    EXPR_KIND_UPDATE_TARGET,        /* UPDATE assignment target item */
    EXPR_KIND_MERGE_WHEN,           /* MERGE WHEN [NOT] MATCHED condition */
    EXPR_KIND_GROUP_BY,             /* GROUP BY */
    EXPR_KIND_ORDER_BY,             /* ORDER BY */
    EXPR_KIND_DISTINCT_ON,          /* DISTINCT ON */
    EXPR_KIND_LIMIT,                /* LIMIT */
    EXPR_KIND_OFFSET,               /* OFFSET */
    EXPR_KIND_RETURNING,            /* RETURNING */
    EXPR_KIND_VALUES,               /* VALUES */
    EXPR_KIND_VALUES_SINGLE,        /* single-row VALUES (in INSERT only) */
    EXPR_KIND_CHECK_CONSTRAINT,     /* CHECK constraint for a table */
    EXPR_KIND_DOMAIN_CHECK,         /* CHECK constraint for a domain */
    EXPR_KIND_COLUMN_DEFAULT,       /* default value for a table column */
    EXPR_KIND_FUNCTION_DEFAULT,     /* default parameter value for function */
    EXPR_KIND_INDEX_EXPRESSION,     /* index expression */
    EXPR_KIND_INDEX_PREDICATE,      /* index predicate */
    EXPR_KIND_STATS_EXPRESSION,     /* extended statistics expression */
    EXPR_KIND_ALTER_COL_TRANSFORM,  /* transform expr in ALTER COLUMN TYPE */
    EXPR_KIND_EXECUTE_PARAMETER,    /* parameter value in EXECUTE */
    EXPR_KIND_TRIGGER_WHEN,         /* WHEN condition in CREATE TRIGGER */
    EXPR_KIND_POLICY,               /* USING or WITH CHECK expr in policy */
    EXPR_KIND_PARTITION_BOUND,      /* partition bound expression */
    EXPR_KIND_PARTITION_EXPRESSION, /* PARTITION BY expression */
    EXPR_KIND_CALL_ARGUMENT,        /* procedure argument in CALL */
    EXPR_KIND_COPY_WHERE,           /* WHERE condition in COPY FROM */
    EXPR_KIND_GENERATED_COLUMN,     /* generation expression for a column */
    EXPR_KIND_CYCLE_MARK,           /* cycle mark value */
};

struct ParseState {
    ParseExprKind p_expr_kind;
};

extern "C" PGEXT_API Node *coerce_to_target_type(ParseState *pstate,
                                                Node *expr,
                                                Oid exprtype,
                                                Oid targettype,
                                                int32_t targettypmod,
                                                CoercionContext ccontext,
                                                CoercionForm cformat,
                                                int location);

extern "C" PGEXT_API Node *transformExpr(ParseState *pstate, Node *expr, ParseExprKind exprKind);

extern "C" PGEXT_API Node *transformExprRecurse(ParseState *pstate, Node *expr);
