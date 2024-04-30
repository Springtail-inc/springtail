#include <cassert>

#include <common/logging.hh>

#include <proxy/parser.hh>
#include <proxy/pg_functions.hh>

namespace springtail {

    /* XXX to shutdown we should call:  pg_query_exit(); */

    void
    Parser::_dump_context(StmtContext &context)
    {
        SPDLOG_INFO("Context: location={}, is_var_set_stmt={}, is_var_set_local_stmt={}, is_select_stmt={}, is_prepare_stmt={}, is_execute_stmt={}, is_transaction_stmt={}, is_deallocate_stmt={}, is_unsupported_stmt={}, has_error={}, has_locking={}, has_into={}",
            context.stmt_location, context.is_var_set_stmt, context.is_var_set_local_stmt, context.is_select_stmt,
            context.is_prepare_stmt, context.is_execute_stmt, context.is_transaction_stmt, context.is_deallocate_stmt,
            context.is_unsupported_stmt, context.has_error, context.has_locking, context.has_into);

        for (auto &func : context.functions) {
            SPDLOG_INFO("Function: {}", func);
        }

        for (auto &table : context.tables) {
            SPDLOG_INFO("Table: schema={}, table={}", table.first, table.second);
        }
    }

    void
    Parser::dump_parse_tree(const std::string &query)
    {
        PgQueryParseResult result = pg_query_parse(query.c_str());

        SPDLOG_INFO("{}", query);
        if (result.error) {
            SPDLOG_ERROR("error: {} at {}", result.error->message, result.error->cursorpos);
        } else {
            SPDLOG_INFO("{}", result.parse_tree);
        }

        pg_query_free_parse_result(result);
    }

    std::vector<Parser::StmtContextPtr>
    Parser::parse_query(const std::string &query)
    {
        ParseContext context;

        // parse the query and get back the context
        _parse_query(query, context);

        // if we have a parse error, return false
        if (context.has_error && context.stmts.empty()) {
            // make empty context
            StmtContextPtr res = std::make_shared<StmtContext>(0, 0);
            res->has_error = true;
            res->is_readonly = false;
            return {res};
        }

        // if we have a single statement, we can return early
        if (context.stmts.size() == 1) {
            context.stmts[0]->is_readonly = _is_query_readonly(*context.stmts[0]);
            return context.stmts;
        }

        // otherwise we need to check each statement and break it apart
        // will stop at first false result
        for (auto &stmt: context.stmts) {
            bool is_readable = _is_query_readonly(*stmt);
            stmt->is_readonly = is_readable;
        }

        return context.stmts;
    }

    bool
    Parser::_is_query_readonly(StmtContext &context)
    {
        // check conditions or clauses that indicate updates/writes
        if (context.is_unsupported_stmt || context.has_error || context.has_into || context.has_locking) {
            _dump_context(context);
            return false;
        }

        // go through function names and check if they are safe
        for (auto iter = context.functions.begin(); iter != context.functions.end(); iter++) {
            if (!_is_function_readonly_safe(*iter)) {
                _dump_context(context);
                return false;
            }
        }

        // go through table names and check if they are safe
        for (auto iter = context.tables.begin(); iter != context.tables.end(); iter++) {
            if (!_is_table_readonly_safe(*iter)) {
                _dump_context(context);
                return false;
            }
        }

        // allow local set operations, prepared statements, and transactions
        if (context.is_var_set_local_stmt || context.is_select_stmt || context.is_deallocate_stmt ||
            context.is_prepare_stmt || context.is_execute_stmt || context.is_transaction_stmt) {
            return true;
        }

        _dump_context(context);

        return false;
    }

    bool
    Parser::_is_table_readonly_safe(const std::pair<std::string, std::string> &table)
    {
        if (!table.first.empty()) {
            if (table.first == "pg_catalog" || table.first == "information_schema") {
                return false;
            }
        }

        // XXX need to check which schemas/tables we are replicating

        return true;
    }

    bool
    Parser::_is_function_readonly_safe(const std::string &funcname)
    {
        if (funcname.starts_with("pg_") || funcname.starts_with("inet_") ||
            funcname.starts_with("has_") || funcname.starts_with("acl_") ||
            funcname.starts_with("to_reg") || funcname.starts_with("txid_") ||
            funcname.starts_with("mxid_") || funcname.starts_with("brin_") ||
            funcname.starts_with("gin_") || funcname.starts_with("acl")) {
                return false;
        }

        if (proxy_unsafe_functions.contains(funcname)) {
            //SPDLOG_WARN("Found function {} in unsafe list", funcname);
            return false;
        }

        if (proxy_safe_functions.contains(funcname)) {
            return true;
        }

        SPDLOG_WARN("Didn't find function {} in safe or unsafe list", funcname);

        return false;
    }

    void
    Parser::_parse_query(const std::string &query, ParseContext &context)
    {
        MemoryContext ctx = NULL;

        ctx = pg_query_enter_memory_context();

        PgQueryInternalParsetreeAndError tree = pg_query_raw_parse(query.c_str(), PG_QUERY_PARSE_DEFAULT);
        if (tree.error) {
            SPDLOG_ERROR("Query parse error {} at {}, for query {}", tree.error->message, tree.error->cursorpos, query);
            context.has_error = true;
            return;
        }
        raw_expression_tree_walker_impl((Node*)tree.tree, Parser::_node_walker, &context);

        pg_query_exit_memory_context(ctx);
    }

    /*
     * Walk the tree and extract information, each node type has a different id
     *   node 123 - rawstmtnode
     *   node 1 - list
     *   node 72 - ResTarget
     *   node 60 - ColumnRef
     *   node 1 - list
     *   node 3 - RangeVar
     *   node 62 - A_Expr
     *   node 60 - ColumnRef
     *   node 63 - A_Const
     *   node 67 - FuncCall
     *   node 144 - String
     */
    bool
    Parser::_node_walker(Node *node, void *ctx)
    {
        // no more nodes to walk through
        if (node == NULL) {
            return false;
        }

        // extract parse context
        ParseContext *parse_context = (ParseContext *)ctx;
        StmtContextPtr context = nullptr;

        // if we have a statement context, use it
        if (!parse_context->stmts.empty()) {
            context = parse_context->stmts.back();
        }

        SPDLOG_DEBUG("Parser node: node tag: {}", (int)nodeTag(node));

        switch(nodeTag(node)) {

        case T_RawStmt: // raw stmt, first node in the parse tree; contains the actual statement
            SPDLOG_DEBUG("Parser node: rawstmt: location={}, len={}", ((RawStmt*)node)->stmt_location, ((RawStmt*)node)->stmt_len);

            // in a raw stmt allocate a new stmt context
            context = std::make_shared<StmtContext>(((RawStmt*)node)->stmt_location, ((RawStmt*)node)->stmt_len);
            parse_context->stmts.push_back(context);

            // see if it is worth progressing
            if (!_check_stmt(((RawStmt*)node)->stmt)) {
                context->is_unsupported_stmt = true;
                return false;
            }

            return _node_walker(((RawStmt*)node)->stmt, ctx);

        case T_SelectStmt: // select stmt
            SPDLOG_DEBUG("Parser node: selectstmt");
            context->is_select_stmt = true;
            break;

        case T_VariableSetStmt: // set variable statement; may be local set or not
            SPDLOG_DEBUG("Parser node: variablesetstmt");
            SPDLOG_DEBUG("Parser node: name {}, is_local={}", ((VariableSetStmt*)node)->name,
                                                              ((VariableSetStmt*)node)->is_local);
            // note: there is a VAR_RESET and VAR_RESET_ALL in the kind enum (VariableSetKind)
            if (((VariableSetStmt*)node)->is_local) {
                context->is_var_set_local_stmt = true;
            } else {
                context->is_var_set_stmt = true;
            }
            return false;

        case T_PrepareStmt: { // prepare statement
            PrepareStmt *stmt = (PrepareStmt*)node;

            SPDLOG_DEBUG("Parser node: preparestmt, name={} query node tag={}", stmt->name, (int)nodeTag(stmt->query));

            context->is_prepare_stmt = true;
            context->prepared_name = stmt->name;

            // check query statement type, may be one of: SELECT, UPDATE, DELETE, MERGE, INSERT
            if (!_check_stmt(stmt->query)) {
                context->is_unsupported_stmt = true;
                return false;
            }

            assert(nodeTag(stmt->query) == T_SelectStmt);
            context->is_select_stmt = true;

            return raw_expression_tree_walker_impl(stmt->query, _node_walker, ctx);
        }

        case T_ExecuteStmt: // execute statement
            SPDLOG_DEBUG("Parser node: executestmt: name={}", ((ExecuteStmt*)node)->name);
            context->is_execute_stmt = true;
            context->prepared_name = ((ExecuteStmt*)node)->name;
            return false;

        case T_DeallocateStmt: // deallocate statement
            SPDLOG_DEBUG("Parser node: deallocatestmt: name={}", ((DeallocateStmt*)node)->name);
            context->is_deallocate_stmt = true;
            context->prepared_name = ((DeallocateStmt*)node)->name;
            return false;

        case T_TransactionStmt: { // transaction statement
            SPDLOG_DEBUG("Parser node: transactionstmt");
            TransactionStmt *stmt = (TransactionStmt *)node;
            switch (stmt->kind) {
                case TRANS_STMT_BEGIN:
                case TRANS_STMT_START:
                case TRANS_STMT_COMMIT:
                case TRANS_STMT_ROLLBACK:
                case TRANS_STMT_SAVEPOINT:
                case TRANS_STMT_RELEASE:
                case TRANS_STMT_ROLLBACK_TO:
                    context->is_transaction_stmt = true;

                case TRANS_STMT_PREPARE:
                case TRANS_STMT_COMMIT_PREPARED:
                case TRANS_STMT_ROLLBACK_PREPARED:
                    // not handled right now?
                    break;
            }
            // XXX need to figure out how to handle this
            return false;
        }

        case T_RangeVar: {
            char *catalog = ((RangeVar*)node)->catalogname;
            char *schema = ((RangeVar*)node)->schemaname;
            char *relname = ((RangeVar*)node)->relname;

            SPDLOG_DEBUG("Parser node: rangevar: db={}, schema={}, relname={}",
                catalog == nullptr ? "" : std::string(catalog),
                schema == nullptr ? "" : std::string(schema),
                relname == nullptr ? "" : std::string(relname));

            context->tables.insert({schema == nullptr ? "" : std::string(schema),
                                    relname == nullptr ? "" : std::string(relname)});
            break;
        }

        case T_IntoClause:
            // select into does an update
            SPDLOG_DEBUG("Parser node: intoclause");
            context->has_into = true;
            return false;

        case T_LockingClause:
            SPDLOG_DEBUG("Parser node: lockingclause");
            context->has_locking = true;
            return false;

        case T_FuncCall:
            SPDLOG_DEBUG("Parser node: funccall");
            parse_context->in_funccall = true;
            raw_expression_tree_walker_impl((Node*)(((FuncCall *)node)->funcname), _node_walker, ctx);
            parse_context->in_funccall = false;
            context->functions.insert(parse_context->func_name);
            parse_context->func_name = {};
            break;

        case T_String:
            if (parse_context->in_funccall) {
                // see if we are in a function call; if so this is the function name
                SPDLOG_DEBUG("Parser node: funccall string {}", ((String*)node)->sval);
                char *funcname = ((String*)node)->sval;
                // seen multiple function names in the FuncCall.
                // like: pg_catalog extract
                // omit pg_catalog
                if (strcmp(funcname, "pg_catalog") == 0) {
                    break;
                }
                // otherwise concatenate the function names
                parse_context->func_name = parse_context->func_name.empty() ? funcname :
                    parse_context->func_name + "." + funcname;
            }
            break;

        default:
            SPDLOG_DEBUG("Parser node: node {}", (int)nodeTag(node));
            break;
        }

        try {
            return raw_expression_tree_walker_impl(node, _node_walker, ctx);
        } catch (const std::exception &e) {
            SPDLOG_ERROR("Parser node: exception: {}", e.what());
            context->has_error = true;
            return false;
        }
    }

    bool
    Parser::_check_stmt(Node *node)
    {
        // whitelist the statement types we consider forwarding
        // see: https://github.com/postgres/postgres/blob/d12b4ba1bd3eedd862064cf1dad5ff107c5cba90/src/backend/tcop/utility.c#L127
        switch (nodeTag(node)) {
            // we whitelist these statement types
            case T_SelectStmt:
            case T_VariableSetStmt:
            case T_PrepareStmt:
            case T_ExecuteStmt:
            case T_TransactionStmt:
            case T_DeallocateStmt:
                return true;

            case T_InsertStmt:
            case T_UpdateStmt:
            case T_DeleteStmt:
            case T_MergeStmt:
            case T_DropStmt:
            case T_TruncateStmt:
            case T_CreateStmt:
            case T_AlterTableStmt:
            // NOTE: this list is not exhaustive, thus the fall through
            default:
                SPDLOG_DEBUG("Parser: rejecting statement type {}", (int)nodeTag(node));
                return false;
        }
    }
} // namespace springtail