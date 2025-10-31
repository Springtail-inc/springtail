#pragma once

#include <functional>
#include <set>
#include <string>
#include <memory>

extern "C" {
    // from postgres include/nodes/nodes.h
    // don't want to include the whole thing
    struct Node;
};

namespace springtail {
namespace pg_proxy {

    /**
     * @brief Parser for Postgres SQL queries; breaks a SQL query into individual statements
     * and provides information about the query and whether it is supported by a read-only replica.
     */
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
                TRANSACTION_COMMIT_STMT,
                TRANSACTION_ROLLBACK_STMT,
                TRANSACTION_PREPARE_STMT,
                TRANSACTION_PREPARE_END_STMT,
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
            bool has_select_query=false;      ///< has a select query embedded (e.g., a PREPARE with select)
            bool has_unsupported_query=false; ///< has an unsupported query embedded
            bool has_locking=false;           ///< has a locking clause (e.g., FOR UPDATE)
            bool has_into=false;              ///< has an INTO clause
            bool has_declare_hold=false;      ///< has a WITH HOLD clause (for cursors)
            bool has_is_local=false;
            bool has_param_ref=false;
            bool has_set_config_session=false; ///< has set_config function call with session scope

            // state
            bool has_error=false;
            bool is_read_safe=false;

            /** prepared or portal statement name (for execute, deallocate, prepare) or var */
            std::string name = {};

            /** value for set_config session variables */
            std::string set_config_value = {};

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

        /**
         * @brief function typedef for verifying if the table exists in for the give schema
         *
         */
        using VerifyTableFn = std::function<bool (const std::string &, const std::string &)>;
        /** Parse the query into multiple statements */
        static std::vector<StmtContextPtr> parse_query(const std::string_view query, VerifyTableFn verify_table_fn);

        /** Debugging, dump the parse tree */
        static void dump_parse_tree(const std::string &query);

        /** Debugging dump the statement after parsing */
        static void dump_context(const StmtContext &context);

    private:

        /** Structure to hold the state and the resultant set of statements while parsing */
        struct ParseContext {
            bool has_error=false;

            // internal state
            bool in_funccall=false;
            std::string func_name = {};

            std::vector<StmtContextPtr> stmts;
        };

        /** Return true if query is read only */
        static bool _is_query_readonly(const StmtContext &context, VerifyTableFn verify_table_fn);

        /** Iterate through the parse tree, moving to next node */
        static bool _node_walker(Node *node, void *ctx);

        /** Entry to parsing the query, fills in the parse context */
        static void _parse_query(const std::string_view query, ParseContext &context);

        /** Checks if a function is safe for read-replica */
        static bool _is_function_readonly_safe(const std::string &funcname);

        /** Checks if a table is safe for read-replica */
        static bool _is_table_readonly_safe(const std::pair<std::string, std::string> &table, VerifyTableFn verify_table_fn);

        /** Check to see if current parse node should be parsed (i.e., is the statement supported or not) */
        static bool _check_stmt(Node *node);

        /** Convert from internal postgres string in node to a C++ string */
        static void _set_string(const char *str, std::string &dest);
    };
    using ParserPtr = std::shared_ptr<Parser>;
} // namespace pg_proxy
} // namespace springtail