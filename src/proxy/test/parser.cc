#include <stdio.h>
#include <stdlib.h>

extern "C" {
#include "nodes/nodeFuncs.h"
#include "pg_query.h"
#include "pg_query_internal.h"
}

const char *tests[] = {
    "SELECT 1", "SELECT * FROM x WHERE z = 2",
    "SELECT a FROM (SELECT abc from td WHERE y=2) as r where b in (SELECT 4 from tx)"};

int testCount = 3;

void
parse(const char *query)
{
    PgQueryParseResult result;
    result = pg_query_parse(query);

    if (result.error) {
        printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
    } else {
        printf("%s\n", result.parse_tree);
    }

    pg_query_free_parse_result(result);
}

typedef struct ParseContext {
    bool has_select;
    bool in_funccall;
    bool has_funccall;
    bool locking_clause;
    bool set_operation;
    bool set_is_local;
    bool into_clause;
    int table_count;
    char *tables[16];
    int funcname_count;
    char *funcnames[16];
} ParseContext;

bool
walker(Node *node, void *ctx)
{
    if (node == NULL) {
        return false;
    }

    ParseContext *context = (ParseContext *)ctx;

    switch (nodeTag(node)) {
        case T_RawStmt:
            printf("rawstmt\n");
            return walker(((RawStmt *)node)->stmt, context);
        case T_SelectStmt:
            printf("selectstmt\n");
            context->has_select = true;
            break;
        case T_RangeVar:
            printf("rangevar: db=%s, schema=%s, relname=%s\n", ((RangeVar *)node)->catalogname,
                   ((RangeVar *)node)->schemaname, ((RangeVar *)node)->relname);
            context->tables[context->table_count++] = ((RangeVar *)node)->relname;
            break;
        case T_IntoClause:
            // select into does an update
            printf("intoclause\n");
            context->into_clause = true;
            break;
        case T_SetOperationStmt:
            printf("setoperationstmt\n");
            context->set_operation = true;
            raw_expression_tree_walker_impl(((SetOperationStmt *)node)->larg, walker, context);
            return raw_expression_tree_walker_impl(((SetOperationStmt *)node)->rarg, walker,
                                                   context);
        case T_VariableSetStmt:
            printf("variablesetstmt\n");
            printf("name %s, is_local=%d\n", ((VariableSetStmt *)node)->name,
                   ((VariableSetStmt *)node)->is_local);
            // note: there is a VAR_RESET and VAR_RESET_ALL in the kind enum (VariableSetKind)
            context->set_is_local = ((VariableSetStmt *)node)->is_local;
            return false;
        case T_LockingClause:
            printf("lockingclause\n");
            context->locking_clause = true;
            break;
        case T_FuncCall:
            printf("funccall\n");
            context->in_funccall = true;
            raw_expression_tree_walker_impl((Node *)(((FuncCall *)node)->funcname), walker,
                                            context);
            context->in_funccall = false;
            break;
        case T_String:
            if (context->in_funccall) {
                printf("funccall string %s\n", ((String *)node)->sval);
                context->funcnames[context->funcname_count++] = ((String *)node)->sval;
            }
            break;
        default:
            printf("node %d\n", nodeTag(node));
            break;
    }

    return raw_expression_tree_walker_impl(node, walker, context);

    /*
    rawstmtnode 123
    node 1 - list
    node 72 - ResTarget
    node 60 - ColumnRef
    node 1 - list
    node 3 - RangeVar
    node 62 - A_Expr
    node 60 - ColumnRef
    node 63 - A_Const
    node 67 - FuncCall
    node 144 - String
    */
}

void
raw_parse(const char *query)
{
    MemoryContext ctx = NULL;

    ParseContext context;
    memset(&context, 0, sizeof(context));

    ctx = pg_query_enter_memory_context();

    PgQueryInternalParsetreeAndError tree = pg_query_raw_parse(query, PG_QUERY_PARSE_DEFAULT);
    if (tree.error) {
        printf("error %s at %d\n", tree.error->message, tree.error->cursorpos);
    }

    raw_expression_tree_walker_impl((Node *)tree.tree, walker, &context);

    pg_query_exit_memory_context(ctx);
}

int
main()
{
    size_t i;

    for (i = 0; i < testCount; i++) {
        parse(tests[i]);
        raw_parse(tests[i]);
    }

    // Optional, this ensures all memory is freed upon program exit (useful when running Valgrind)
    pg_query_exit();

    return 0;
}
