#include <gtest/gtest.h>
#include <proxy/history_cache.hh>
#include <common/logging.hh>

namespace springtail::pg_proxy {

/**
 * @brief Test trace entry representing a SQL operation and expected outcome
 */
struct TraceEntry {
    enum Type {
        STATEMENT,     ///< Execute a statement
        SYNC,          ///< Sync transaction with status
        COMMIT_STMT,   ///< Commit statement with success/failure
        CLEAR_STMT     ///< Clear statement cache
    };

    Type type;
    QueryStmt::Type stmt_type = QueryStmt::NONE;
    std::string name;
    std::string data;
    bool is_read_safe = true;
    char xact_status = 'I';  // For SYNC operations
    int completed = 1;       // For COMMIT_STMT operations
    bool success = true;     // For COMMIT_STMT operations

    // Factory methods for readability
    static TraceEntry statement(QueryStmt::Type stmt_type, const std::string& name = "",
                               const std::string& data = "", bool is_read_safe = true) {
        TraceEntry entry;
        entry.type = STATEMENT;
        entry.stmt_type = stmt_type;
        entry.name = name;
        entry.data = data;
        entry.is_read_safe = is_read_safe;
        return entry;
    }

    /**
     * @brief Create a SYNC trace entry
     * @param status Transaction status character: 'I' = idle, 'T' = in transaction, 'E' = in error
     * @return TraceEntry
     */
    static TraceEntry sync(char status) {
        TraceEntry entry;
        entry.type = SYNC;
        entry.xact_status = status;
        return entry;
    }

    static TraceEntry commit_statement(int completed = 1, bool success = true) {
        TraceEntry entry;
        entry.type = COMMIT_STMT;
        entry.completed = completed;
        entry.success = success;
        return entry;
    }

    static TraceEntry clear_statement() {
        TraceEntry entry;
        entry.type = CLEAR_STMT;
        return entry;
    }
};

/**
 * @brief Expected state of caches after trace execution
 */
struct ExpectedCacheState {
    struct SessionEntry {
        QueryStmt::Type type;
        std::string name;
        std::string data;
        bool operator==(const SessionEntry& other) const {
            return type == other.type && name == other.name && data == other.data;
        }
    };

    std::vector<SessionEntry> session_history;
    bool in_error = false;

    void add_session(QueryStmt::Type type, const std::string& name = "", const std::string& data = "") {
        session_history.push_back({type, name, data});
    }
};

/**
 * @brief Test fixture for StatementCache tests
 */
class StatementCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        cache = std::make_unique<StatementCache>();
    }

    void TearDown() override {
        cache.reset();
    }

    /**
     * @brief Execute a test trace against the cache
     */
    void execute_trace(const std::vector<TraceEntry>& trace)
    {
        current_stmt.reset();

        for (const auto& entry : trace) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Executing trace entry type: {}", (int)entry.type);

            switch (entry.type) {
                case TraceEntry::STATEMENT:
                    current_stmt = cache->add(entry.stmt_type, entry.data, entry.is_read_safe, entry.name);
                    break;

                case TraceEntry::SYNC:
                    cache->sync_transaction(entry.xact_status);
                    break;

                case TraceEntry::COMMIT_STMT:
                    if (current_stmt) {
                        cache->commit_statement(current_stmt, entry.completed, entry.success);
                    }
                    break;

                case TraceEntry::CLEAR_STMT:
                    cache->clear_statement();
                    break;
            }
        }
    }

    /**
     * @brief Verify cache state matches expected state using internal interface
     */
    void verify_cache_state(const ExpectedCacheState& expected)
    {
        auto cache_state = cache->get_cache_state();

        // Verify error state
        EXPECT_EQ(cache_state.in_error, expected.in_error)
            << "Error state mismatch: expected " << expected.in_error
            << ", got " << cache_state.in_error;


        // Verify session history
        EXPECT_EQ(cache_state.session_history.size(), expected.session_history.size())
            << "Session history count mismatch: expected " << expected.session_history.size()
            << ", got " << cache_state.session_history.size();

        for (size_t entry_index = 0; entry_index < expected.session_history.size(); ++entry_index) {
            const auto& expected_session_entry = expected.session_history[entry_index];
            if (entry_index < cache_state.session_history.size()) {
                const auto& actual_session_statement = cache_state.session_history[entry_index];

                EXPECT_EQ(actual_session_statement->type, expected_session_entry.type)
                    << "Session history entry " << entry_index << " type mismatch: expected "
                    << (int)expected_session_entry.type << ", got " << (int)actual_session_statement->type;

                EXPECT_EQ(actual_session_statement->name, expected_session_entry.name)
                    << "Session history entry " << entry_index << " name mismatch: expected '"
                    << expected_session_entry.name << "', got '" << actual_session_statement->name << "'";

                if (actual_session_statement->data_type == QueryStmt::SIMPLE && !expected_session_entry.data.empty()) {
                    EXPECT_EQ(actual_session_statement->query(), expected_session_entry.data)
                        << "Session history entry " << entry_index << " data mismatch: expected '"
                        << expected_session_entry.data << "', got '" << actual_session_statement->query() << "'";
                }
            }
        }

        // Log information about cache sizes
        LOG_INFO("Cache state verification complete:");
        LOG_INFO("  Session history size: {}", cache_state.session_history.size());
        LOG_INFO("  Error state: {}", cache_state.in_error);
    }

    /**
     * @brief Dump comprehensive cache contents for debugging
     */
    void dump_cache_contents(const ExpectedCacheState& expected)
    {
        auto cache_state = cache->get_cache_state();

        LOG_INFO("=== Cache Contents Dump (Session State Only) ===");
        LOG_INFO("Error state: {}", cache_state.in_error);

        LOG_INFO("Session History ({} entries):", cache_state.session_history.size());
        for (size_t entry_index = 0; entry_index < cache_state.session_history.size(); ++entry_index) {
            const auto& session_statement = cache_state.session_history[entry_index];
            LOG_INFO("  [{}] type={}, name='{}', data='{}'",
                    entry_index, (int)session_statement->type, session_statement->name,
                    session_statement->data_type == QueryStmt::SIMPLE ? session_statement->query() : "<packet>");
        }

        // dump expected state for comparison
        LOG_INFO("Expected Session History ({} entries):", expected.session_history.size());
        for (size_t entry_index = 0; entry_index < expected.session_history.size(); ++entry_index) {
            const auto& expected_entry = expected.session_history[entry_index];
            LOG_INFO("  [{}] type={}, name='{}', data='{}'",
                    entry_index, (int)expected_entry.type, expected_entry.name, expected_entry.data);
        }

        LOG_INFO("=== End Cache Contents Dump ===");
    }

    /**
     * @brief Helper to verify specific cache sizes for detailed testing
     */
    void verify_cache_sizes(size_t expected_session_size)
    {
        auto cache_state = cache->get_cache_state();
        EXPECT_EQ(cache_state.session_history.size(), expected_session_size)
            << "Session history size mismatch";
    }

    void verify_error_state(bool expected_in_error)
    {
        auto cache_state = cache->get_cache_state();
        EXPECT_EQ(cache_state.in_error, expected_in_error)
            << "Error state mismatch: expected " << expected_in_error
            << ", got " << cache_state.in_error;
    }

    std::unique_ptr<StatementCache> cache;
    QueryStmtPtr current_stmt;
};

/**
 * @brief Test basic SET and RESET operations
 */
TEST_F(StatementCacheTest, BasicSetAndReset)
{
    std::vector<TraceEntry> trace = {
        TraceEntry::statement(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I'),

        TraceEntry::statement(QueryStmt::SET, "timezone", "SET timezone = 'UTC'"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I'),

        TraceEntry::statement(QueryStmt::RESET, "work_mem", "RESET work_mem"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };

    execute_trace(trace);

    ExpectedCacheState expected;
    expected.add_session(QueryStmt::SET, "timezone", "SET timezone = 'UTC'");
    expected.add_session(QueryStmt::RESET, "work_mem", "RESET work_mem");

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test prepared statement lifecycle
 */
TEST_F(StatementCacheTest, PreparedStatements)
{
    std::vector<TraceEntry> trace = {
        // Prepare statement
        TraceEntry::statement(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT * FROM users WHERE id = $1"),
        TraceEntry::commit_statement(),

        // Prepare another statement
        TraceEntry::statement(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT name FROM users"),
        TraceEntry::commit_statement(),

        // Deallocate one statement
        TraceEntry::statement(QueryStmt::DEALLOCATE, "stmt1", "DEALLOCATE stmt1"),
        TraceEntry::commit_statement(),

        TraceEntry::sync('I')
    };

    execute_trace(trace);

    ExpectedCacheState expected;
    expected.add_session(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT name FROM users");
    // the deallocated statement should not be present due to full compaction of the history

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test transaction with rollback
 */
TEST_F(StatementCacheTest, TransactionRollback) {
    std::vector<TraceEntry> trace = {
        // Start transaction
        TraceEntry::statement(QueryStmt::BEGIN, "", "BEGIN"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('T'),

        // Set variable in transaction
        TraceEntry::statement(QueryStmt::SET, "statement_timeout", "SET statement_timeout = '30s'"),
        TraceEntry::commit_statement(),

        // Prepare statement in transaction
        TraceEntry::statement(QueryStmt::PREPARE, "tx_stmt", "PREPARE tx_stmt AS INSERT INTO logs VALUES ($1)"),
        TraceEntry::commit_statement(),

        // Rollback transaction
        TraceEntry::statement(QueryStmt::ROLLBACK, "", "ROLLBACK"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };

    execute_trace(trace);

    ExpectedCacheState expected;
    // Prepared statement should survive rollback
    expected.add_session(QueryStmt::PREPARE, "tx_stmt", "PREPARE tx_stmt AS INSERT INTO logs VALUES ($1)");
    // SET should not survive rollback

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test savepoint operations
 */
TEST_F(StatementCacheTest, SavepointOperations) {
    std::vector<TraceEntry> trace = {
        // Start transaction
        TraceEntry::statement(QueryStmt::BEGIN, "", "BEGIN"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('T'),

        // Set variable
        TraceEntry::statement(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'"),
        TraceEntry::commit_statement(),

        // Create savepoint
        TraceEntry::statement(QueryStmt::SAVEPOINT, "sp1", "SAVEPOINT sp1"),
        TraceEntry::commit_statement(),

        // Set another variable after savepoint
        TraceEntry::statement(QueryStmt::SET, "timezone", "SET timezone = 'PST'"),
        TraceEntry::commit_statement(),

        // Prepare statement after savepoint
        TraceEntry::statement(QueryStmt::PREPARE, "sp_stmt", "PREPARE sp_stmt AS SELECT now()"),
        TraceEntry::commit_statement(),

        // Rollback to savepoint
        TraceEntry::statement(QueryStmt::ROLLBACK_TO_SAVEPOINT, "sp1", "ROLLBACK TO SAVEPOINT sp1"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('T'),

        // Commit transaction
        TraceEntry::statement(QueryStmt::COMMIT, "", "COMMIT"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };

    execute_trace(trace);

    ExpectedCacheState expected;

    // work_mem should survive (was set before savepoint)
    expected.add_session(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");
    // PREPARE statements can't be rolled back
    expected.add_session(QueryStmt::PREPARE, "sp_stmt", "PREPARE sp_stmt AS SELECT now()");
    // timezone should be rolled back (was set after savepoint)

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test DISCARD ALL operation
 */
TEST_F(StatementCacheTest, DiscardAll) {
    std::vector<TraceEntry> trace = {
        // Set up some state
        TraceEntry::statement(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I'),

        TraceEntry::statement(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I'),

        TraceEntry::statement(QueryStmt::LISTEN, "channel1", "LISTEN channel1"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I'),

        // Discard everything
        TraceEntry::statement(QueryStmt::DISCARD_ALL, "", "DISCARD ALL"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };

    execute_trace(trace);

    ExpectedCacheState expected{};
    // Everything should be cleared except the DISCARD ALL itself
    expected.add_session(QueryStmt::DISCARD_ALL, "", "DISCARD ALL");

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test error handling and recovery
 */
TEST_F(StatementCacheTest, ErrorHandling) {
    std::vector<TraceEntry> trace = {
        // Start transaction
        TraceEntry::statement(QueryStmt::BEGIN, "", "BEGIN"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('T'),

        // Set variable
        TraceEntry::statement(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'"),
        TraceEntry::commit_statement(),

        // Simulate error
        TraceEntry::statement(QueryStmt::SET, "invalid_var", "SET invalid_var = 'bad_value'"),
        TraceEntry::commit_statement(1, false),  // Statement failed
        TraceEntry::sync('E'),  // Transaction in error state

        // Transaction should be rolled back due to error
        TraceEntry::sync('I')  // Back to idle
    };

    execute_trace(trace);

    ExpectedCacheState expected;
    expected.in_error = false;  // Should be cleared after sync to 'I'
    // work_mem should be rolled back due to transaction failure

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test local variables (SET LOCAL)
 */
TEST_F(StatementCacheTest, LocalVariables) {
    std::vector<TraceEntry> trace = {
        // Start transaction
        TraceEntry::statement(QueryStmt::BEGIN, "", "BEGIN"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('T'),

        // Set session variable
        TraceEntry::statement(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'"),
        TraceEntry::commit_statement(),

        // Set local variable
        TraceEntry::statement(QueryStmt::SET_LOCAL, "statement_timeout", "SET LOCAL statement_timeout = '10s'"),
        TraceEntry::commit_statement(),

        // Commit transaction
        TraceEntry::statement(QueryStmt::COMMIT, "", "COMMIT"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };

    execute_trace(trace);

    ExpectedCacheState expected;
    // Session variable should survive
    expected.add_session(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");
    // Local variable should not be in session history

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test cursor operations
 */
TEST_F(StatementCacheTest, CursorOperations) {
    std::vector<TraceEntry> trace = {
        // Start transaction
        TraceEntry::statement(QueryStmt::BEGIN, "", "BEGIN"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('T'),

        // Declare cursor
        TraceEntry::statement(QueryStmt::DECLARE, "cursor1", "DECLARE cursor1 CURSOR FOR SELECT * FROM users"),
        TraceEntry::commit_statement(),

        // Fetch from cursor
        TraceEntry::statement(QueryStmt::FETCH, "cursor1", "FETCH 10 FROM cursor1"),
        TraceEntry::commit_statement(),

        // Close cursor
        TraceEntry::statement(QueryStmt::CLOSE, "cursor1", "CLOSE cursor1"),
        TraceEntry::commit_statement(),

        // Commit transaction
        TraceEntry::statement(QueryStmt::COMMIT, "", "COMMIT"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };

    execute_trace(trace);

    ExpectedCacheState expected;
    // Cursors don't persist across transactions (unless WITH HOLD)

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test cursor operations
 */
TEST_F(StatementCacheTest, CursorDeclareHold) {
    std::vector<TraceEntry> trace = {
        // Start transaction
        TraceEntry::statement(QueryStmt::BEGIN, "", "BEGIN"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('T'),

        // Declare cursor
        TraceEntry::statement(QueryStmt::DECLARE_HOLD, "cursor1", "DECLARE cursor1 CURSOR WITH HOLD FOR SELECT * FROM users"),
        TraceEntry::commit_statement(),

        // Fetch from cursor
        TraceEntry::statement(QueryStmt::FETCH, "cursor1", "FETCH 10 FROM cursor1"),
        TraceEntry::commit_statement(),

        // Close cursor
        TraceEntry::statement(QueryStmt::CLOSE, "cursor1", "CLOSE cursor1"),
        TraceEntry::commit_statement(),

        // Commit transaction
        TraceEntry::statement(QueryStmt::COMMIT, "", "COMMIT"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };

    execute_trace(trace);

    ExpectedCacheState expected{};
    // nothing expected since cursor is closed

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test complex scenario with multiple operations
 */
TEST_F(StatementCacheTest, ComplexScenario) {
    std::vector<TraceEntry> trace = {
        // Session setup
        TraceEntry::statement(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I'),

        TraceEntry::statement(QueryStmt::PREPARE, "user_query", "PREPARE user_query AS SELECT * FROM users WHERE id = $1"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I'),

        // Start transaction
        TraceEntry::statement(QueryStmt::BEGIN, "", "BEGIN"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('T'),

        // Transaction operations
        TraceEntry::statement(QueryStmt::SET_LOCAL, "statement_timeout", "SET LOCAL statement_timeout = '30s'"),
        TraceEntry::commit_statement(),

        TraceEntry::statement(QueryStmt::SAVEPOINT, "sp1", "SAVEPOINT sp1"),
        TraceEntry::commit_statement(),

        TraceEntry::statement(QueryStmt::SET, "timezone", "SET timezone = 'UTC'"),
        TraceEntry::commit_statement(),

        TraceEntry::statement(QueryStmt::PREPARE, "temp_stmt", "PREPARE temp_stmt AS SELECT count(*) FROM logs"),
        TraceEntry::commit_statement(),

        // Release savepoint
        TraceEntry::statement(QueryStmt::RELEASE_SAVEPOINT, "sp1", "RELEASE SAVEPOINT sp1"),
        TraceEntry::commit_statement(),

        // Commit transaction
        TraceEntry::statement(QueryStmt::COMMIT, "", "COMMIT"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };

    execute_trace(trace);

    ExpectedCacheState expected;
    expected.add_session(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");
    expected.add_session(QueryStmt::PREPARE, "user_query", "PREPARE user_query AS SELECT * FROM users WHERE id = $1");
    expected.add_session(QueryStmt::SET, "timezone", "SET timezone = 'UTC'");
    expected.add_session(QueryStmt::PREPARE, "temp_stmt", "PREPARE temp_stmt AS SELECT count(*) FROM logs");

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test that verifies cache state transitions during complex operations
 */
TEST_F(StatementCacheTest, CacheStateTransitions) {
    // Initial state should be empty
    verify_cache_sizes(0);  // session_size should be 0
    verify_error_state(false);

    // Add a statement and commit it - should move to session history
    std::vector<TraceEntry> trace = {
        TraceEntry::statement(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };
    execute_trace(trace);
    verify_cache_sizes(1);  // Set should be in session history

    ExpectedCacheState expected;
    expected.add_session(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");

    verify_cache_state(expected);
    dump_cache_contents(expected);
}

/**
 * @brief Test detailed prepared statement operations
 */
TEST_F(StatementCacheTest, DetailedPreparedStatementTest)
{
    // Set replay idx for testing; this preservers the deallocate in session history
    cache->set_session_replay_idx(0, 0);

    std::vector<TraceEntry> trace = {
        // Prepare first statement
        TraceEntry::statement(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT * FROM users"),
        TraceEntry::commit_statement(),

        // Prepare second statement
        TraceEntry::statement(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT count(*) FROM logs"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };
    execute_trace(trace);

    ExpectedCacheState expected;
    expected.add_session(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT * FROM users");
    expected.add_session(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT count(*) FROM logs");

    verify_cache_state(expected);

    // Now deallocate one
    trace = {
        TraceEntry::statement(QueryStmt::DEALLOCATE, "stmt1", "DEALLOCATE stmt1"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };
    execute_trace(trace);

    expected = ExpectedCacheState{};
    expected.add_session(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT * FROM users");
    expected.add_session(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT count(*) FROM logs");
    expected.add_session(QueryStmt::DEALLOCATE, "stmt1", "DEALLOCATE stmt1");

    verify_cache_state(expected);

    // setting the replay index will cause compaction of the session history
    // this will remove the deallocate and prepared statement
    cache->set_session_replay_idx(0, 10);

    trace = {
        TraceEntry::statement(QueryStmt::SET, "set1", "SET my.set1 = 'value1'"),
        TraceEntry::commit_statement(),
        TraceEntry::sync('I')
    };
    execute_trace(trace);

    expected = ExpectedCacheState{};
    expected.add_session(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT count(*) FROM logs");

    dump_cache_contents(expected);
}

}  // namespace springtail::pg_proxy