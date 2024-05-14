#pragma once

#include <set>
#include <string>
#include <memory>

extern "C" {
    struct Node;
};

namespace springtail {
    class Parser {
    public:
        /**
         * @brief Context for a single statement in a query
         * Contains info about the query and whether it is readonly safe
         */
        struct StmtContext {
            // offset and length of this query if multiple
            int stmt_location;
            int stmt_length;

            // statement types
            enum Type : int8_t {
                INVALID=0,
                UNSUPPORTED_STMT,
                VAR_SET_STMT,
                VAR_SET_TRANSACTION_ISOLATION_STMT,
                VAR_SET_TRANSACTION_SNAPSHOT_STMT,
                VAR_RESET_STMT,
                SELECT_STMT,
                PREPARE_STMT,
                EXECUTE_STMT,
                TRANSACTION_BEGIN_STMT,
                TRANSACTION_OTHER_STMT,
                DEALLOCATE_STMT,
                DISCARD_STMT,
                DISCARD_ALL_STMT,
                DECLARE_STMT,
                CLOSE_STMT,
                COPY_TO_STDOUT_STMT,
                FETCH_STMT,
                LISTEN_STMT,
                UNLISTEN_STMT,
                SAVEPOINT_STMT,
                ROLLBACK_TO_SAVEPOINT_STMT,
                RELEASE_SAVEPOINT_STMT
            } type = INVALID;

            // clauses
            bool has_select_query=false;
            bool has_unsupported_query=false;
            bool has_locking=false;
            bool has_into=false;
            bool has_declare_hold=false;
            bool has_is_local=false;

            // state
            bool has_error=false;
            bool is_read_safe=false;

            /** prepared or portal statement name (for execute, deallocate, prepare) or var */
            std::string name = {};

            /** set of functions */
            std::set<std::string> functions;

            /** set of <schema, tablename> pairs */
            std::set<std::pair<std::string, std::string>> tables;

            StmtContext(int location, int length)
              : stmt_location(location),
                stmt_length(length)
            {}
        };
        using StmtContextPtr = std::shared_ptr<StmtContext>;

        static std::vector<StmtContextPtr> parse_query(const std::string_view query);

        static void dump_parse_tree(const std::string &query);

        static void dump_context(const StmtContext &context);

    private:

        struct ParseContext {
            bool has_error=false;

            // internal state
            bool in_funccall=false;
            std::string func_name = {};

            std::vector<StmtContextPtr> stmts;
        };

        static bool _is_query_readonly(const StmtContext &context);
        static bool _node_walker(Node *node, void *ctx);
        static void _parse_query(const std::string_view query, ParseContext &context);
        static bool _is_function_readonly_safe(const std::string &funcname);
        static bool _is_table_readonly_safe(const std::pair<std::string, std::string> &table);
        static bool _check_stmt(Node *node);
        static void _set_string(const char *str, std::string &dest);
    };
    using ParserPtr = std::shared_ptr<Parser>;
}