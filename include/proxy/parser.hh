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
            bool is_var_set_stmt=false;
            bool is_var_set_local_stmt=false;
            bool is_select_stmt=false;
            bool is_prepare_stmt=false;
            bool is_execute_stmt=false;
            bool is_transaction_stmt=false;
            bool is_deallocate_stmt=false;
            bool is_unsupported_stmt=false;

            // clauses
            bool has_locking=false;
            bool has_into=false;

            // state
            bool has_error=false;
            bool is_readonly=false;

            /** prepared statement name (for execute, deallocate, prepare) */
            std::string prepared_name = {};

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

    private:

        struct ParseContext {
            bool has_error=false;

            // internal state
            bool in_funccall=false;
            std::string func_name = {};

            std::vector<StmtContextPtr> stmts;
        };

        static bool _is_query_readonly(StmtContext &context);
        static bool _node_walker(Node *node, void *ctx);
        static void _parse_query(const std::string_view query, ParseContext &context);
        static bool _is_function_readonly_safe(const std::string &funcname);
        static bool _is_table_readonly_safe(const std::pair<std::string, std::string> &table);
        static bool _check_stmt(Node *node);

        static void _dump_context(StmtContext &context);

    };
    using ParserPtr = std::shared_ptr<Parser>;
}