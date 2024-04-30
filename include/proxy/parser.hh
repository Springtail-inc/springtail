#pragma once

#include <set>
#include <string>
#include <memory>

extern "C" {
    #include "pg_query.h"
    #include "pg_query_internal.h"
    #include "nodes/nodeFuncs.h"
}

namespace springtail {
    class Parser {
    public:

        static bool is_query_readonly(const std::string &query);

    private:
        struct ParseContext {
            bool has_error=false;
            bool has_select=false;
            bool has_insert=false;
            bool has_update=false;
            bool has_delete=false;
            bool has_truncate=false;
            bool in_funccall=false;
            bool has_funccall=false;
            bool locking_clause=false;
            bool set_operation=false;
            bool set_is_local=false;
            bool has_into=false;

            /** set of functions */
            std::set<std::string> functions;

            /** set of <schema, tablename> pairs */
            std::set<std::pair<std::string, std::string>> tables;
        };

        static bool _node_walker(Node *node, void *ctx);
        static void _parse_query(const std::string &query, ParseContext &context);
        static bool _is_function_readonly_safe(const std::string &funcname);
        static bool _is_table_readonly_safe(const std::pair<std::string, std::string> &table);
    };
    using ParserPtr = std::shared_ptr<Parser>;
}