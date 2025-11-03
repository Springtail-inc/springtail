#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include <proxy/client_session.hh>
#include <proxy/database.hh>
#include <proxy/buffer_pool.hh>
#include <common/logging.hh>
#include <common/properties.hh>

namespace springtail::pg_proxy {

/**
 * @brief Test fixture for ClientSession parse_simple_query tests
 */
class ClientParseSimpleQueryTest : public ::testing::Test {
protected:

    static constexpr uint64_t TEST_DB_ID = 12345;
    static constexpr const char* TEST_DB_NAME = "test_database";

    static void SetUpTestSuite() {

        springtail_init_test(LOG_PROXY);

        // Initialize logging for test debugging
        LOG_INFO("Setting up ClientParseSimpleQueryTest");

        // Create a test database and populate it with schema tables
        setup_test_database_with_schema_tables();
    }

    void SetUp() override {
        // Per-test setup can be added here if needed
        LOG_INFO("Setting up test case in ClientParseSimpleQueryTest");
        buffer_pool = BufferPool::get_instance();
        ASSERT_NE(buffer_pool, nullptr);
        database_manager = DatabaseMgr::get_instance();
        ASSERT_NE(database_manager, nullptr);
    }

    void TearDown() override {
        // Clean up any test data
        LOG_INFO("Tearing down ClientParseSimpleQueryTest");
    }

    /**
     * @brief Set up test database with comprehensive schema and table configuration
     */
    static void setup_test_database_with_schema_tables() {
        // Initialize the database manager singleton
        auto db_mgr = DatabaseMgr::get_instance();
        ASSERT_NE(db_mgr, nullptr);

        // Create a test database object
        auto test_database_object = std::make_shared<Database>(TEST_DB_ID, TEST_DB_NAME);

        // Add various schema and table combinations for comprehensive testing
        std::vector<std::pair<std::string, std::string>> schema_table_pairs = {
            // Public schema tables (most common)
            {"public", "users"},
            {"public", "orders"},
            {"public", "products"},
            {"public", "inventory"},
            {"public", "audit_log"},

            // Analytics schema tables
            {"analytics", "user_metrics"},
            {"analytics", "sales_data"},
            {"analytics", "performance_stats"},

            // Admin schema tables
            {"admin", "system_config"},
            {"admin", "user_permissions"},

            // Reporting schema tables
            {"reporting", "daily_summaries"},
            {"reporting", "monthly_reports"},

            // Test schema for special cases
            {"test_schema", "test_table"},
            {"test_schema", "special_chars_table"},

            // Schema with underscores and numbers
            {"data_warehouse_v2", "customer_data"},
            {"data_warehouse_v2", "transaction_history"}
        };

        // Add all schema table pairs to the test database
        test_database_object->add_schema_tables(schema_table_pairs);

        db_mgr->add_database(test_database_object);

        LOG_INFO("Set up test database {} with {} schema-table pairs",
                TEST_DB_NAME, schema_table_pairs.size());
    }

    /**
     * @brief Helper to create a test buffer with minimal content
     * The buffer content doesn't matter for parse_simple_query testing
     */
    BufferPtr create_test_buffer() {
        BufferPtr test_buffer = buffer_pool->get(256);
        // Add minimal buffer content - actual content doesn't matter for parsing
        test_buffer->put_string("SELECT 1;");
        test_buffer->rewind(); // Reset offset for reading
        return test_buffer;
    }

    /**
     * @brief Helper to verify QueryStmt properties
     */
    void verify_query_statement_properties(const QueryStmtPtr& query_statement,
                                         QueryStmt::Type expected_parent_type,
                                         bool expected_read_safe,
                                         size_t expected_children_count) {
        ASSERT_NE(query_statement, nullptr);

        EXPECT_EQ(query_statement->type, expected_parent_type)
            << "Parent query statement type mismatch";

        EXPECT_EQ(query_statement->is_read_safe, expected_read_safe)
            << "Query read safety mismatch";

        EXPECT_EQ(query_statement->children.size(), expected_children_count)
            << "Number of child statements mismatch";
    }

    /**
     * @brief Helper to verify child statement properties
     */
    void verify_child_statement_properties(const QueryStmtPtr& parent_query_statement,
                                         size_t child_index,
                                         QueryStmt::Type expected_child_type,
                                         bool expected_child_read_safe,
                                         const std::string& expected_child_name = "") {
        ASSERT_LT(child_index, parent_query_statement->children.size())
            << "Child index out of bounds";

        const auto& child_statement = parent_query_statement->children[child_index];
        ASSERT_NE(child_statement, nullptr);

        EXPECT_EQ(child_statement->type, expected_child_type)
            << "Child statement type mismatch at index " << child_index;

        EXPECT_EQ(child_statement->is_read_safe, expected_child_read_safe)
            << "Child statement read safety mismatch at index " << child_index;

        if (!expected_child_name.empty()) {
            EXPECT_EQ(child_statement->name, expected_child_name)
                << "Child statement name mismatch at index " << child_index;
        }
    }

    /**
     * @brief Helper to log query statement details for debugging
     */
    void log_query_statement_details(const QueryStmtPtr& query_statement,
                                   const std::string& test_description) {
        LOG_INFO("=== Query Statement Details for: {} ===", test_description);
        LOG_INFO("Parent - Type: {}, ReadSafe: {}, Children: {}",
                static_cast<int>(query_statement->type),
                query_statement->is_read_safe,
                query_statement->children.size());

        for (size_t child_index = 0; child_index < query_statement->children.size(); ++child_index) {
            const auto& child_statement = query_statement->children[child_index];
            LOG_INFO("  Child[{}] - Type: {}, ReadSafe: {}, Name: '{}'",
                    child_index,
                    static_cast<int>(child_statement->type),
                    child_statement->is_read_safe,
                    child_statement->name);
        }
    }

    DatabaseMgr* database_manager;
    BufferPool* buffer_pool;
    uint64_t test_database_id{TEST_DB_ID};
    std::string test_database_name{TEST_DB_NAME};
};

/**
 * @brief Test parsing simple SELECT queries on replicated tables
 */
TEST_F(ClientParseSimpleQueryTest, ParseSimpleSelectQueriesOnReplicatedTables) {
    SCOPED_TRACE("Testing simple SELECT query on replicated table 'users'");

    BufferPtr test_buffer = create_test_buffer();

    // Test simple SELECT on public.users (replicated table)
    std::string_view simple_select_query = "SELECT * FROM users WHERE id = 1";

    QueryStmtPtr query_result = ClientSession::parse_simple_query(
        test_database_id, test_buffer, simple_select_query);

    verify_query_statement_properties(query_result, QueryStmt::SIMPLE_QUERY, true, 1);
    verify_child_statement_properties(query_result, 0, QueryStmt::ANONYMOUS, true);

    log_query_statement_details(query_result, "Simple SELECT on replicated table");
}

/**
 * @brief Test parsing simple SELECT queries with explicit schema
 */
TEST_F(ClientParseSimpleQueryTest, ParseSelectQueriesWithExplicitSchema) {
    SCOPED_TRACE("Testing SELECT query with explicit schema qualification 'analytics.user_metrics'");

    BufferPtr test_buffer = create_test_buffer();

    // Test SELECT with explicit schema on replicated table
    std::string_view schema_qualified_query = "SELECT name, email FROM analytics.user_metrics WHERE created_at > '2023-01-01'";

    QueryStmtPtr query_result = ClientSession::parse_simple_query(
        test_database_id, test_buffer, schema_qualified_query);

    verify_query_statement_properties(query_result, QueryStmt::SIMPLE_QUERY, true, 1);
    verify_child_statement_properties(query_result, 0, QueryStmt::ANONYMOUS, true);

    log_query_statement_details(query_result, "SELECT with explicit schema");
}

/**
 * @brief Test parsing INSERT queries (write operations)
 */
TEST_F(ClientParseSimpleQueryTest, ParseInsertQueriesWriteOperations) {
    SCOPED_TRACE("Testing INSERT query as write operation");

    BufferPtr test_buffer = create_test_buffer();

    // Test INSERT query (should be marked as not read-safe)
    std::string_view insert_query = "INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com')";

    QueryStmtPtr query_result = ClientSession::parse_simple_query(
        test_database_id, test_buffer, insert_query);

    verify_query_statement_properties(query_result, QueryStmt::SIMPLE_QUERY, false, 1);
    verify_child_statement_properties(query_result, 0, QueryStmt::ANONYMOUS, false);

    log_query_statement_details(query_result, "INSERT query (write operation)");
}

/**
 * @brief Test parsing UPDATE queries
 */
TEST_F(ClientParseSimpleQueryTest, ParseUpdateQueriesWriteOperations) {
    SCOPED_TRACE("Testing UPDATE query as write operation");

    BufferPtr test_buffer = create_test_buffer();

    // Test UPDATE query (should be marked as not read-safe)
    std::string_view update_query = "UPDATE products SET price = 19.99 WHERE category = 'electronics'";

    QueryStmtPtr query_result = ClientSession::parse_simple_query(
        test_database_id, test_buffer, update_query);

    verify_query_statement_properties(query_result, QueryStmt::SIMPLE_QUERY, false, 1);
    verify_child_statement_properties(query_result, 0, QueryStmt::ANONYMOUS, false);

    log_query_statement_details(query_result, "UPDATE query (write operation)");
}

/**
 * @brief Test parsing DELETE queries
 */
TEST_F(ClientParseSimpleQueryTest, ParseDeleteQueriesWriteOperations) {
    SCOPED_TRACE("Testing DELETE query as write operation");

    BufferPtr test_buffer = create_test_buffer();

    // Test DELETE query (should be marked as not read-safe)
    std::string_view delete_query = "DELETE FROM audit_log WHERE created_at < '2022-01-01'";

    QueryStmtPtr query_result = ClientSession::parse_simple_query(
        test_database_id, test_buffer, delete_query);

    verify_query_statement_properties(query_result, QueryStmt::SIMPLE_QUERY, false, 1);
    verify_child_statement_properties(query_result, 0, QueryStmt::ANONYMOUS, false);

    log_query_statement_details(query_result, "DELETE query (write operation)");
}

/**
 * @brief Test parsing transaction control statements
 */
TEST_F(ClientParseSimpleQueryTest, ParseTransactionControlStatements) {
    BufferPtr test_buffer = create_test_buffer();

    {
        SCOPED_TRACE("Testing BEGIN transaction statement");

        std::string_view begin_query = "BEGIN";
        QueryStmtPtr begin_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, begin_query);

        verify_query_statement_properties(begin_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(begin_result, 0, QueryStmt::BEGIN, true);

        log_query_statement_details(begin_result, "BEGIN transaction");
    }

    {
        SCOPED_TRACE("Testing COMMIT transaction statement");

        std::string_view commit_query = "COMMIT";
        QueryStmtPtr commit_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, commit_query);

        verify_query_statement_properties(commit_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(commit_result, 0, QueryStmt::COMMIT, true);

        log_query_statement_details(commit_result, "COMMIT transaction");
    }

    {
        SCOPED_TRACE("Testing ROLLBACK transaction statement");

        std::string_view rollback_query = "ROLLBACK";
        QueryStmtPtr rollback_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, rollback_query);

        verify_query_statement_properties(rollback_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(rollback_result, 0, QueryStmt::ROLLBACK, true);

        log_query_statement_details(rollback_result, "ROLLBACK transaction");
    }
}

/**
 * @brief Test parsing SET statements (session configuration)
 */
TEST_F(ClientParseSimpleQueryTest, ParseSetStatements) {
    BufferPtr test_buffer = create_test_buffer();

    {
        SCOPED_TRACE("Testing regular SET statement for work_mem configuration");

        std::string_view set_query = "SET work_mem = '256MB'";
        QueryStmtPtr set_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, set_query);

        verify_query_statement_properties(set_result, QueryStmt::SIMPLE_QUERY, false, 1);
        verify_child_statement_properties(set_result, 0, QueryStmt::SET, false, "work_mem");

        log_query_statement_details(set_result, "SET statement");
    }

    {
        SCOPED_TRACE("Testing SET LOCAL statement for timezone configuration");

        std::string_view set_local_query = "SET LOCAL timezone = 'UTC'";
        QueryStmtPtr set_local_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, set_local_query);

        verify_query_statement_properties(set_local_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(set_local_result, 0, QueryStmt::SET_LOCAL, true, "timezone");

        log_query_statement_details(set_local_result, "SET LOCAL statement");
    }
}

/**
 * @brief Test parsing RESET statements
 */
TEST_F(ClientParseSimpleQueryTest, ParseResetStatements) {
    BufferPtr test_buffer = create_test_buffer();

    {
        SCOPED_TRACE("Testing RESET statement for specific variable work_mem");

        std::string_view reset_query = "RESET work_mem";
        QueryStmtPtr reset_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, reset_query);

        verify_query_statement_properties(reset_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(reset_result, 0, QueryStmt::RESET, true, "work_mem");

        log_query_statement_details(reset_result, "RESET specific variable");
    }

    {
        SCOPED_TRACE("Testing RESET ALL statement for all configuration variables");

        std::string_view reset_all_query = "RESET ALL";
        QueryStmtPtr reset_all_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, reset_all_query);

        verify_query_statement_properties(reset_all_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(reset_all_result, 0, QueryStmt::RESET_ALL, true);

        log_query_statement_details(reset_all_result, "RESET ALL");
    }
}

/**
 * @brief Test parsing cursor-related statements
 */
TEST_F(ClientParseSimpleQueryTest, ParseCursorStatements) {
    BufferPtr test_buffer = create_test_buffer();

    {
        SCOPED_TRACE("Testing DECLARE cursor statement for user_cursor");

        std::string_view declare_query = "DECLARE user_cursor CURSOR FOR SELECT * FROM users";
        QueryStmtPtr declare_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, declare_query);

        verify_query_statement_properties(declare_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(declare_result, 0, QueryStmt::DECLARE, true, "user_cursor");

        log_query_statement_details(declare_result, "DECLARE cursor");
    }

    {
        SCOPED_TRACE("Testing DECLARE cursor WITH HOLD statement for persistent_cursor");

        std::string_view declare_hold_query = "DECLARE persistent_cursor CURSOR WITH HOLD FOR SELECT id FROM products";
        QueryStmtPtr declare_hold_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, declare_hold_query);

        verify_query_statement_properties(declare_hold_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(declare_hold_result, 0, QueryStmt::DECLARE_HOLD, true, "persistent_cursor");

        log_query_statement_details(declare_hold_result, "DECLARE cursor WITH HOLD");
    }

    {
        SCOPED_TRACE("Testing FETCH statement from user_cursor");

        std::string_view fetch_query = "FETCH 10 FROM user_cursor";
        QueryStmtPtr fetch_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, fetch_query);

        verify_query_statement_properties(fetch_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(fetch_result, 0, QueryStmt::FETCH, true, "user_cursor");

        log_query_statement_details(fetch_result, "FETCH from cursor");
    }
}

/**
 * @brief Test parsing multi-statement queries (semicolon separated)
 */
TEST_F(ClientParseSimpleQueryTest, ParseMultiStatementQueries) {
    SCOPED_TRACE("Testing multi-statement query with BEGIN, INSERT, UPDATE, and COMMIT");

    BufferPtr test_buffer = create_test_buffer();

    // Test multiple statements in one query
    std::string_view multi_statement_query =
        "BEGIN; "
        "INSERT INTO users (name) VALUES ('Alice'); "
        "UPDATE users SET email = 'alice@example.com' WHERE name = 'Alice'; "
        "COMMIT;";

    QueryStmtPtr multi_result = ClientSession::parse_simple_query(
        test_database_id, test_buffer, multi_statement_query);

    // Should have 4 child statements and be marked as not read-safe (due to INSERT/UPDATE)
    verify_query_statement_properties(multi_result, QueryStmt::SIMPLE_QUERY, false, 4);

    // Verify each child statement with individual scoped traces
    {
        SCOPED_TRACE("Verifying first child statement (BEGIN) in multi-statement query");
        verify_child_statement_properties(multi_result, 0, QueryStmt::BEGIN, true);
    }
    {
        SCOPED_TRACE("Verifying second child statement (INSERT) in multi-statement query");
        verify_child_statement_properties(multi_result, 1, QueryStmt::ANONYMOUS, false);
    }
    {
        SCOPED_TRACE("Verifying third child statement (UPDATE) in multi-statement query");
        verify_child_statement_properties(multi_result, 2, QueryStmt::ANONYMOUS, false);
    }
    {
        SCOPED_TRACE("Verifying fourth child statement (COMMIT) in multi-statement query");
        verify_child_statement_properties(multi_result, 3, QueryStmt::COMMIT, true);
    }

    log_query_statement_details(multi_result, "Multi-statement query");
}

/**
 * @brief Test parsing prepared statement operations
 */
TEST_F(ClientParseSimpleQueryTest, ParsePreparedStatementOperations) {
    BufferPtr test_buffer = create_test_buffer();

    {
        SCOPED_TRACE("Testing PREPARE statement for user_select prepared statement");

        std::string_view prepare_query = "PREPARE user_select AS SELECT * FROM users WHERE id = $1";
        QueryStmtPtr prepare_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, prepare_query);

        verify_query_statement_properties(prepare_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(prepare_result, 0, QueryStmt::PREPARE, true, "user_select");

        log_query_statement_details(prepare_result, "PREPARE statement");
    }

    {
        SCOPED_TRACE("Testing EXECUTE statement for user_select prepared statement");

        std::string_view execute_query = "EXECUTE user_select(123)";
        QueryStmtPtr execute_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, execute_query);

        verify_query_statement_properties(execute_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(execute_result, 0, QueryStmt::EXECUTE, true, "user_select");

        log_query_statement_details(execute_result, "EXECUTE statement");
    }

    {
        SCOPED_TRACE("Testing DEALLOCATE statement for user_select prepared statement");

        std::string_view deallocate_query = "DEALLOCATE user_select";
        QueryStmtPtr deallocate_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, deallocate_query);

        verify_query_statement_properties(deallocate_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(deallocate_result, 0, QueryStmt::DEALLOCATE, true, "user_select");

        log_query_statement_details(deallocate_result, "DEALLOCATE statement");
    }
}

/**
 * @brief Test parsing LISTEN/UNLISTEN statements
 */
TEST_F(ClientParseSimpleQueryTest, ParseListenUnlistenStatements) {
    BufferPtr test_buffer = create_test_buffer();

    {
        SCOPED_TRACE("Testing LISTEN statement for user_notifications channel");

        std::string_view listen_query = "LISTEN user_notifications";
        QueryStmtPtr listen_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, listen_query);

        verify_query_statement_properties(listen_result, QueryStmt::SIMPLE_QUERY, false, 1);
        verify_child_statement_properties(listen_result, 0, QueryStmt::LISTEN, false, "user_notifications");

        log_query_statement_details(listen_result, "LISTEN statement");
    }

    {
        SCOPED_TRACE("Testing UNLISTEN statement for specific user_notifications channel");

        std::string_view unlisten_query = "UNLISTEN user_notifications";
        QueryStmtPtr unlisten_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, unlisten_query);

        verify_query_statement_properties(unlisten_result, QueryStmt::SIMPLE_QUERY, false, 1);
        verify_child_statement_properties(unlisten_result, 0, QueryStmt::UNLISTEN, false, "user_notifications");

        log_query_statement_details(unlisten_result, "UNLISTEN specific");
    }

    {
        SCOPED_TRACE("Testing UNLISTEN ALL statement for all notification channels");

        std::string_view unlisten_all_query = "UNLISTEN *";
        QueryStmtPtr unlisten_all_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, unlisten_all_query);

        verify_query_statement_properties(unlisten_all_result, QueryStmt::SIMPLE_QUERY, false, 1);
        verify_child_statement_properties(unlisten_all_result, 0, QueryStmt::UNLISTEN_ALL, false);

        log_query_statement_details(unlisten_all_result, "UNLISTEN all");
    }
}

/**
 * @brief Test parsing queries on non-replicated tables (should still work)
 */
TEST_F(ClientParseSimpleQueryTest, ParseQueriesOnNonReplicatedTables) {
    SCOPED_TRACE("Testing SELECT query on non-replicated table non_existent_table");

    BufferPtr test_buffer = create_test_buffer();

    // Test query on a table that doesn't exist in our replicated table list
    std::string_view non_replicated_query = "SELECT * FROM non_existent_table WHERE active = true";

    QueryStmtPtr query_result = ClientSession::parse_simple_query(
        test_database_id, test_buffer, non_replicated_query);

    // Should still parse successfully, but if table is not found it should not be read-safe
    verify_query_statement_properties(query_result, QueryStmt::SIMPLE_QUERY, false, 1);
    verify_child_statement_properties(query_result, 0, QueryStmt::ANONYMOUS, false);

    log_query_statement_details(query_result, "Query on non-replicated table");
}

/**
 * @brief Test parsing savepoint operations
 */
TEST_F(ClientParseSimpleQueryTest, ParseSavepointOperations) {
    BufferPtr test_buffer = create_test_buffer();

    {
        SCOPED_TRACE("Testing SAVEPOINT statement for savepoint sp1");

        std::string_view savepoint_query = "SAVEPOINT sp1";
        QueryStmtPtr savepoint_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, savepoint_query);

        verify_query_statement_properties(savepoint_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(savepoint_result, 0, QueryStmt::SAVEPOINT, true, "sp1");

        log_query_statement_details(savepoint_result, "SAVEPOINT");
    }

    {
        SCOPED_TRACE("Testing ROLLBACK TO SAVEPOINT statement for savepoint sp1");

        std::string_view rollback_to_query = "ROLLBACK TO SAVEPOINT sp1";
        QueryStmtPtr rollback_to_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, rollback_to_query);

        verify_query_statement_properties(rollback_to_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(rollback_to_result, 0, QueryStmt::ROLLBACK_TO_SAVEPOINT, true, "sp1");

        log_query_statement_details(rollback_to_result, "ROLLBACK TO SAVEPOINT");
    }

    {
        SCOPED_TRACE("Testing RELEASE SAVEPOINT statement for savepoint sp1");

        std::string_view release_query = "RELEASE SAVEPOINT sp1";
        QueryStmtPtr release_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, release_query);

        verify_query_statement_properties(release_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(release_result, 0, QueryStmt::RELEASE_SAVEPOINT, true, "sp1");

        log_query_statement_details(release_result, "RELEASE SAVEPOINT");
    }
}

/**
 * @brief Test parsing empty and whitespace-only queries
 */
TEST_F(ClientParseSimpleQueryTest, ParseEmptyAndWhitespaceQueries) {
    BufferPtr test_buffer = create_test_buffer();

    {
        SCOPED_TRACE("Testing completely empty query string");

        std::string_view empty_query = "";
        QueryStmtPtr empty_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, empty_query);

        verify_query_statement_properties(empty_result, QueryStmt::SIMPLE_QUERY, true, 0);

        log_query_statement_details(empty_result, "Empty query");
    }

    {
        SCOPED_TRACE("Testing whitespace-only query string");

        std::string_view whitespace_query = "   \n\t  ";
        QueryStmtPtr whitespace_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, whitespace_query);

        verify_query_statement_properties(whitespace_result, QueryStmt::SIMPLE_QUERY, true, 0);

        log_query_statement_details(whitespace_result, "Whitespace-only query");
    }
}

/**
 * @brief Test parsing SELECT set_config() function calls with proper read safety determination
 */
TEST_F(ClientParseSimpleQueryTest, ParseSelectSetConfigFunctionCalls) {
    BufferPtr test_buffer = create_test_buffer();

    {
        SCOPED_TRACE("Testing SELECT set_config() with safe session variable work_mem");

        std::string_view select_safe_session_config_query =
            "SELECT set_config('work_mem', '512MB', false)";

        QueryStmtPtr safe_session_config_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, select_safe_session_config_query);

        verify_query_statement_properties(safe_session_config_result, QueryStmt::SIMPLE_QUERY, false, 1);
        verify_child_statement_properties(safe_session_config_result, 0, QueryStmt::SET, false, "work_mem");

        log_query_statement_details(safe_session_config_result, "SELECT set_config() safe session variable");
    }

    {
        SCOPED_TRACE("Testing SELECT set_config() with unsafe session variable shared_preload_libraries");

        std::string_view select_unsafe_session_config_query =
            "SELECT set_config('shared_preload_libraries', 'pg_stat_statements', false)";

        QueryStmtPtr unsafe_session_config_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, select_unsafe_session_config_query);

        verify_query_statement_properties(unsafe_session_config_result, QueryStmt::SIMPLE_QUERY, false, 1);
        verify_child_statement_properties(unsafe_session_config_result, 0, QueryStmt::SET, false, "shared_preload_libraries");

        log_query_statement_details(unsafe_session_config_result, "SELECT set_config() unsafe session variable");
    }

    {
        SCOPED_TRACE("Testing SELECT set_config() with user-defined session variable myapp.cache_size");

        std::string_view select_user_defined_session_config_query =
            "SELECT set_config('myapp.cache_size', '100MB', false)";

        QueryStmtPtr user_defined_session_config_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, select_user_defined_session_config_query);

        verify_query_statement_properties(user_defined_session_config_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(user_defined_session_config_result, 0, QueryStmt::SET, true, "myapp.cache_size");

        log_query_statement_details(user_defined_session_config_result, "SELECT set_config() user-defined session variable");
    }

    {
        SCOPED_TRACE("Testing SELECT set_config() with safe transaction-local variable timezone");

        std::string_view select_safe_local_config_query =
            "SELECT set_config('timezone', 'America/New_York', true)";

        QueryStmtPtr safe_local_config_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, select_safe_local_config_query);

        verify_query_statement_properties(safe_local_config_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(safe_local_config_result, 0, QueryStmt::SET_LOCAL, true, "timezone");

        log_query_statement_details(safe_local_config_result, "SELECT set_config() safe local variable");
    }

    {
        SCOPED_TRACE("Testing SELECT set_config() with unsafe transaction-local variable max_connections");

        std::string_view select_unsafe_local_config_query =
            "SELECT set_config('max_connections', '200', true)";

        QueryStmtPtr unsafe_local_config_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, select_unsafe_local_config_query);

        verify_query_statement_properties(unsafe_local_config_result, QueryStmt::SIMPLE_QUERY, false, 1);
        verify_child_statement_properties(unsafe_local_config_result, 0, QueryStmt::SET_LOCAL, false);

        log_query_statement_details(unsafe_local_config_result, "SELECT set_config() unsafe local variable");
    }

    {
        SCOPED_TRACE("Testing SELECT set_config() with user-defined transaction-local variable mycompany.debug_level");

        std::string_view select_user_defined_local_config_query =
            "SELECT set_config('mycompany.debug_level', 'verbose', true)";

        QueryStmtPtr user_defined_local_config_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, select_user_defined_local_config_query);

        verify_query_statement_properties(user_defined_local_config_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(user_defined_local_config_result, 0, QueryStmt::SET_LOCAL, true, "mycompany.debug_level");

        log_query_statement_details(user_defined_local_config_result, "SELECT set_config() user-defined local variable");
    }
/* FIX multi selects with set_config calls
    {
        SCOPED_TRACE("Testing SELECT with multiple safe set_config() function calls");

        std::string_view select_multiple_safe_config_query =
            "SELECT set_config('work_mem', '256MB', false), set_config('statement_timeout', '30s', false)";

        QueryStmtPtr multiple_safe_config_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, select_multiple_safe_config_query);

        verify_query_statement_properties(multiple_safe_config_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(multiple_safe_config_result, 0, QueryStmt::SET, true, "work_mem");

        log_query_statement_details(multiple_safe_config_result, "SELECT multiple safe set_config() calls");
    }
*/
    {
        SCOPED_TRACE("Testing SELECT set_config() with deep namespace user-defined variable");

        std::string_view select_deep_namespace_config_query =
            "SELECT set_config('myapp.module.subsystem.setting', 'enabled', false)";

        QueryStmtPtr deep_namespace_config_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, select_deep_namespace_config_query);

        verify_query_statement_properties(deep_namespace_config_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(deep_namespace_config_result, 0, QueryStmt::SET, true, "myapp.module.subsystem.setting");

        log_query_statement_details(deep_namespace_config_result, "SELECT set_config() deep namespace variable");
    }

    {
        SCOPED_TRACE("Testing SELECT set_config() with insufficient arguments");

        std::string_view select_invalid_set_config_query =
            "SELECT set_config('work_mem', '256MB')";  // Missing third parameter

        QueryStmtPtr invalid_config_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, select_invalid_set_config_query);

        verify_query_statement_properties(invalid_config_result, QueryStmt::SIMPLE_QUERY, false, 1);
        verify_child_statement_properties(invalid_config_result, 0, QueryStmt::ANONYMOUS, false);

        log_query_statement_details(invalid_config_result, "SELECT set_config() insufficient args");
    }

    {
        SCOPED_TRACE("Testing SELECT set_config() with case-sensitive variable DateStyle");

        std::string_view select_case_sensitive_config_query =
            "SELECT set_config('dateSTYLE', 'ISO, MDY', false)";

        QueryStmtPtr case_sensitive_config_result = ClientSession::parse_simple_query(
            test_database_id, test_buffer, select_case_sensitive_config_query);

        verify_query_statement_properties(case_sensitive_config_result, QueryStmt::SIMPLE_QUERY, true, 1);
        verify_child_statement_properties(case_sensitive_config_result, 0, QueryStmt::SET, true, "dateSTYLE");

        log_query_statement_details(case_sensitive_config_result, "SELECT set_config() case-sensitive variable");
    }
}

} // namespace springtail::pg_proxy