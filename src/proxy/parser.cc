#include <cassert>
#include <map>
#include <set>

#include <common/logging.hh>

#include <proxy/parser.hh>
#include <proxy/pg_functions.hh>

extern "C" {
    #include "pg_query.h"
    #include "pg_query_internal.h"
    #include "nodes/nodeFuncs.h"
}

namespace springtail {

    /* XXX to shutdown we should call:  pg_query_exit(); */

    // names of StmtContext types
    static std::map<int8_t, std::string> _stmt_names = {
        {Parser::StmtContext::INVALID, "INVALID"},
        {Parser::StmtContext::UNSUPPORTED_STMT, "UNSUPPORTED_STMT"},
        {Parser::StmtContext::VAR_SET_STMT, "VAR_SET_STMT"},
        {Parser::StmtContext::VAR_SET_TRANSACTION_ISOLATION_STMT, "VAR_SET_TRANSACTION_ISOLATION_STMT"},
        {Parser::StmtContext::VAR_SET_TRANSACTION_SNAPSHOT_STMT, "VAR_SET_TRANSACTION_SNAPSHOT_STMT"},
        {Parser::StmtContext::VAR_RESET_STMT, "VAR_RESET_STMT"},
        {Parser::StmtContext::SELECT_STMT, "SELECT_STMT"},
        {Parser::StmtContext::PREPARE_STMT, "PREPARE_STMT"},
        {Parser::StmtContext::EXECUTE_STMT, "EXECUTE_STMT"},
        {Parser::StmtContext::TRANSACTION_BEGIN_STMT, "TRANSACTION_BEGIN_STMT"},
        {Parser::StmtContext::TRANSACTION_OTHER_STMT, "TRANSACTION_OTHER_STMT"},
        {Parser::StmtContext::DEALLOCATE_STMT, "DEALLOCATE_STMT"},
        {Parser::StmtContext::DISCARD_STMT, "DISCARD_STMT"},
        {Parser::StmtContext::DISCARD_ALL_STMT, "DISCARD_ALL_STMT"},
        {Parser::StmtContext::DECLARE_STMT, "DECLARE_STMT"},
        {Parser::StmtContext::CLOSE_STMT, "CLOSE_STMT"},
        {Parser::StmtContext::COPY_TO_STDOUT_STMT, "COPY_TO_STDOUT_STMT"},
        {Parser::StmtContext::FETCH_STMT, "FETCH_STMT"},
        {Parser::StmtContext::LISTEN_STMT, "LISTEN_STMT"},
        {Parser::StmtContext::UNLISTEN_STMT, "UNLISTEN_STMT"},
        {Parser::StmtContext::SAVEPOINT_STMT, "SAVEPOINT_STMT"},
        {Parser::StmtContext::ROLLBACK_TO_SAVEPOINT_STMT, "ROLLBACK_TO_SAVEPOINT_STMT"},
        {Parser::StmtContext::RELEASE_SAVEPOINT_STMT, "RELEASE_SAVEPOINT_STMT"}
    };

    /** Unsafe statements for replicas */
    static std::set<int8_t> _replica_unsafe_stmts = {
        Parser::StmtContext::INVALID,
        Parser::StmtContext::UNSUPPORTED_STMT,
        Parser::StmtContext::LISTEN_STMT,
        Parser::StmtContext::UNLISTEN_STMT,
        Parser::StmtContext::VAR_SET_TRANSACTION_SNAPSHOT_STMT
    };

    void
    Parser::dump_context(const StmtContext &context)
    {
        SPDLOG_INFO("Context: location={}, type={}, name={}, is_read_safe={}\n  has_select_query={}, has_unsupported_query={}, has_locking={}, has_into={}, has_declare_hold={}, has_error={}, has_is_local={}",
            context.stmt_location, _stmt_names[(int8_t)context.type], context.name, context.is_read_safe, context.has_select_query, context.has_unsupported_query, context.has_locking, context.has_into, context.has_declare_hold, context.has_error, context.has_is_local);

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
    Parser::parse_query(const std::string_view query)
    {
        ParseContext context;

        // parse the query and get back the context
        _parse_query(query, context);

        // if we have a parse error, return false
        if (context.has_error && context.stmts.empty()) {
            // make empty context
            SPDLOG_WARN("Parse error in query: {}", query);
            StmtContextPtr res = std::make_shared<StmtContext>(0, 0);
            res->has_error = true;
            res->is_read_safe = false;
            return {res};
        }

        // if we have a single statement, we can return early
        if (context.stmts.size() == 1) {
            context.stmts[0]->is_read_safe = _is_query_readonly(*context.stmts[0]);
            SPDLOG_DEBUG("Single query is_read_safe: {}", context.stmts[0]->is_read_safe);
            return context.stmts;
        }

        // otherwise we need to check each statement and break it apart
        // will stop at first false result
        for (auto &stmt: context.stmts) {
            bool is_readable = _is_query_readonly(*stmt);
            stmt->is_read_safe = is_readable;
        }

        return context.stmts;
    }

    bool
    Parser::_is_query_readonly(const StmtContext &context)
    {
        // debugging
        dump_context(context);

        assert(context.type != StmtContext::INVALID);

        // check conditions or clauses that indicate updates/writes
        if (_replica_unsafe_stmts.contains(context.type) || context.has_error ||
            context.has_into || context.has_locking || context.has_unsupported_query) {
            return false;
        }

        // go through function names and check if they are safe
        for (auto iter = context.functions.begin(); iter != context.functions.end(); iter++) {
            if (!_is_function_readonly_safe(*iter)) {
                return false;
            }
        }

        // go through table names and check if they are safe
        for (auto iter = context.tables.begin(); iter != context.tables.end(); iter++) {
            if (!_is_table_readonly_safe(*iter)) {
                return false;
            }
        }

        // force serializable transactions to be read-only
        if ((context.type == StmtContext::VAR_SET_TRANSACTION_ISOLATION_STMT ||
             context.type == StmtContext::TRANSACTION_BEGIN_STMT) &&
             context.name == "serializable") {
            return false;
        }

        return true;
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
    Parser::_parse_query(const std::string_view query, ParseContext &context)
    {
        MemoryContext ctx = NULL;

        ctx = pg_query_enter_memory_context();

        PgQueryInternalParsetreeAndError tree = pg_query_raw_parse(query.data(), PG_QUERY_PARSE_DEFAULT);
        if (tree.error) {
            SPDLOG_ERROR("Query parse error {} at {}, for query {}", tree.error->message, tree.error->cursorpos, query);
            context.has_error = true;
            return;
        }
        raw_expression_tree_walker_impl((Node*)tree.tree, Parser::_node_walker, &context);

        pg_query_exit_memory_context(ctx);
    }

    /*
     *  Walk the tree and extract information, each node type has a different id
     *  see: postgres/include/nodes/parsenodes.h for node structures
     *  and: postgres/include/nodes/nodetags.h for node tag enum
     *
     *  NOTE: when parsing a node and it has children then use _node_walker() if the child is a stmt/query
     *  use raw_expression_tree_walker_impl() if the child is a list; return false to stop walking the tree
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

        switch(nodeTag(node)) {

        case T_RawStmt: { // raw stmt, first node in the parse tree; contains the actual statement
            RawStmt *stmt = (RawStmt*)node;
            SPDLOG_DEBUG("Parser node: rawstmt: location={}, len={}", stmt->stmt_location, stmt->stmt_len);

            // in a raw stmt allocate a new stmt context
            context = std::make_shared<StmtContext>(stmt->stmt_location, stmt->stmt_len);
            parse_context->stmts.push_back(context);

            // see if it is worth progressing
            if (!_check_stmt(stmt->stmt)) {
                context->type = StmtContext::UNSUPPORTED_STMT;
                return false;
            }

            return _node_walker(stmt->stmt, ctx);
        }

        case T_SelectStmt: // select stmt
            SPDLOG_DEBUG("Parser node: selectstmt");
            if (context->type == StmtContext::INVALID) {
                context->type = StmtContext::SELECT_STMT;
            } else {
                context->has_select_query = true;
            }
            break;

        case T_VariableSetStmt: { // set variable statement; may be local set or not
            SPDLOG_DEBUG("Parser node: variablesetstmt");
            VariableSetStmt *stmt = (VariableSetStmt*)node;
            SPDLOG_DEBUG("Parser node: name {}, is_local={}, kind={}",
                         stmt->name, stmt->is_local, (int)stmt->kind);

            context->has_is_local = stmt->is_local;

            switch (stmt->kind) {
                case VAR_SET_VALUE:
                    context->type = StmtContext::VAR_SET_STMT;
                    _set_string(stmt->name, context->name);
                    break;

                case VAR_SET_DEFAULT:
                case VAR_RESET:
                case VAR_RESET_ALL:
                    context->type = StmtContext::VAR_RESET_STMT;
                    _set_string(stmt->name, context->name);
                    break;

                case VAR_SET_MULTI: {
                    // these will fall through to the defelem node parsing
                    // TRANSACTION sets are local to transaction only
                    std::string name;
                    _set_string(stmt->name, name);

                    if (name == "TRANSACTION") {
                        context->type = StmtContext::VAR_SET_TRANSACTION_ISOLATION_STMT;
                        context->has_is_local = true;
                    } else if (name == "TRANSACTION SNAPSHOT") {
                        context->type = StmtContext::VAR_SET_TRANSACTION_SNAPSHOT_STMT;
                        context->has_is_local = true;
                    } else if (name == "SESSION CHARACTERISTICS") {
                        context->type = StmtContext::VAR_SET_TRANSACTION_ISOLATION_STMT;
                    }

                    return raw_expression_tree_walker_impl((Node*)stmt->args, _node_walker, ctx);
                }

                default:
                    context->type = StmtContext::UNSUPPORTED_STMT;
                    break;
            }

            return false;
        }

        case T_PrepareStmt: { // prepare statement
            PrepareStmt *stmt = (PrepareStmt*)node;

            SPDLOG_DEBUG("Parser node: preparestmt, name={} query node tag={}", stmt->name, (int)nodeTag(stmt->query));

            context->type = StmtContext::PREPARE_STMT;
            _set_string(stmt->name, context->name);

            // check if query statement type is supported;
            // may be one of: SELECT, UPDATE, DELETE, MERGE, INSERT
            if (!_check_stmt(stmt->query)) {
                context->has_unsupported_query = true;
                return false;
            }

            assert(nodeTag(stmt->query) == T_SelectStmt);
            context->has_select_query = true;

            return _node_walker(stmt->query, ctx);
        }

        case T_ExecuteStmt: // execute statement
            SPDLOG_DEBUG("Parser node: executestmt: name={}", ((ExecuteStmt*)node)->name);
            context->type = StmtContext::EXECUTE_STMT;
            _set_string(((ExecuteStmt*)node)->name, context->name);
            return false;

        case T_DeallocateStmt: // deallocate statement
            SPDLOG_DEBUG("Parser node: deallocatestmt: name={}", ((DeallocateStmt*)node)->name);
            context->type = StmtContext::DEALLOCATE_STMT;
            // null name means deallocate all
            _set_string(((DeallocateStmt*)node)->name, context->name);
            return false;

        case T_DiscardStmt: {
            // discard statement
            SPDLOG_DEBUG("Parser node: discardstmt");
            DiscardStmt *stmt = (DiscardStmt*)node;
            context->type = StmtContext::DISCARD_STMT;
            switch (stmt->target) {
                case DISCARD_ALL:
                    context->type = StmtContext::DISCARD_ALL_STMT;
                    break;
                default:
                    // don't care about other discard types
                    break;
            }
            return false;
        }

        case T_DeclareCursorStmt: {
            // declare cursor statement
            SPDLOG_DEBUG("Parser node: declarecursorstmt");
            DeclareCursorStmt *stmt = (DeclareCursorStmt*)node;
            context->type = StmtContext::DECLARE_STMT;
            if (stmt->options & CURSOR_OPT_HOLD) {
                context->has_declare_hold = true;
            }
            _set_string(stmt->portalname, context->name);

            return _node_walker(stmt->query, ctx);
        }

        case T_ClosePortalStmt: {
            // close portal statement
            SPDLOG_DEBUG("Parser node: closeportalstmt");
            ClosePortalStmt *stmt = (ClosePortalStmt*)node;
            context->type = StmtContext::CLOSE_STMT;
            // null name means close all portals
            _set_string(stmt->portalname, context->name);
            return false;
        }

        case T_ListenStmt: {
            // listen statement
            SPDLOG_DEBUG("Parser node: listenstmt");
            ListenStmt *stmt = (ListenStmt*)node;
            context->type = StmtContext::LISTEN_STMT;
            _set_string(stmt->conditionname, context->name);
            return false;
        }

        case T_UnlistenStmt: {
            // unlisten statement
            SPDLOG_DEBUG("Parser node: unlistenstmt");
            UnlistenStmt *stmt = (UnlistenStmt*)node;
            context->type = StmtContext::UNLISTEN_STMT;
            _set_string(stmt->conditionname, context->name);
            return false;
        }

        case T_CopyStmt: {
            // copy statement
            SPDLOG_DEBUG("Parser node: copystmt");
            CopyStmt *stmt = (CopyStmt*)node;
            if (!stmt->is_from && stmt->filename == nullptr) {
                context->type = StmtContext::COPY_TO_STDOUT_STMT;
            } else {
                context->type = StmtContext::UNSUPPORTED_STMT;
            }
            return false;
        }

        case T_TransactionStmt: { // transaction statement
            SPDLOG_DEBUG("Parser node: transactionstmt");
            TransactionStmt *stmt = (TransactionStmt *)node;
            switch (stmt->kind) {
                case TRANS_STMT_BEGIN:
                case TRANS_STMT_START:
                    context->type = StmtContext::TRANSACTION_BEGIN_STMT;
                    return raw_expression_tree_walker_impl((Node*)stmt->options, _node_walker, ctx);
                case TRANS_STMT_SAVEPOINT:
                    context->type = StmtContext::SAVEPOINT_STMT;
                    _set_string(stmt->savepoint_name, context->name);
                    break;
                case TRANS_STMT_RELEASE:
                    context->type = StmtContext::RELEASE_SAVEPOINT_STMT;
                    _set_string(stmt->savepoint_name, context->name);
                    break;
                case TRANS_STMT_ROLLBACK_TO:
                    context->type = StmtContext::ROLLBACK_TO_SAVEPOINT_STMT;
                    _set_string(stmt->savepoint_name, context->name);
                    break;
                case TRANS_STMT_COMMIT:
                case TRANS_STMT_ROLLBACK:
                case TRANS_STMT_PREPARE:
                case TRANS_STMT_COMMIT_PREPARED:
                case TRANS_STMT_ROLLBACK_PREPARED:
                    context->type = StmtContext::TRANSACTION_OTHER_STMT;
                    break;
            }

            return false;
        }

        case T_FetchStmt: {
            // fetch statement or move if stmt->ismove is true, but we don't care
            SPDLOG_DEBUG("Parser node: fetchstmt");
            FetchStmt *stmt = (FetchStmt*)node;
            context->type = StmtContext::FETCH_STMT;
            _set_string(stmt->portalname, context->name);
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

        case T_DefElem: {
            SPDLOG_DEBUG("Parser node: defelem");
            DefElem *elem = (DefElem*)node;
            SPDLOG_DEBUG("Parser node: name={}, action={}, argtype={}", elem->defname, (int)elem->defaction, (int)nodeTag(elem->arg));

            if (context->type != StmtContext::VAR_SET_TRANSACTION_ISOLATION_STMT &&
                context->type != StmtContext::TRANSACTION_BEGIN_STMT) {
                break;
            }

            // only handle SET TRANSACTION ISOLATION LEVEL or
            // SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL
            // this comes from a SET statement, specifically a VariableSetStmt
            if (strcmp("transaction_isolation", elem->defname) == 0) {
                // this is from SET TRANSACTION ISOLATION LEVEL
                assert (nodeTag(elem->arg) == T_A_Const);
                A_Const *aconst = (A_Const*)elem->arg;

                // decode the argument as a string
                if (nodeTag(&aconst->val) == T_String) {
                    SPDLOG_DEBUG("Parser node: defelem arg string: {}", aconst->val.sval.sval);
                    _set_string(aconst->val.sval.sval, context->name);
                }
            } else {
                // set the name of the variable, this is from SET SESSION CHARACTERISTICS...
                _set_string(elem->defname, context->name);
            }

            break;
        }

        case T_A_Const: {
            SPDLOG_DEBUG("Parser node: aconst");
            A_Const *aconst = (A_Const*)node;

            if (context->type != StmtContext::VAR_SET_TRANSACTION_SNAPSHOT_STMT) {
                break;
            }

            if (nodeTag(&aconst->val) == T_String) {
                SPDLOG_DEBUG("Parser node: aconst string: {}", aconst->val.sval.sval);
                _set_string(aconst->val.sval.sval, context->name);
            }

            break;
        }

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

    void
    Parser::_set_string(const char *str, std::string &value)
    {
        if (str != nullptr) {
            value = str;
        } else {
            value = {};
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
            case T_DeclareCursorStmt:
            case T_ClosePortalStmt:
            case T_DiscardStmt:
            case T_ListenStmt:
            case T_UnlistenStmt:
            case T_FetchStmt:
            case T_CopyStmt:
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