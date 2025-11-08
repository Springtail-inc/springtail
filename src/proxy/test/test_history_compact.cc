#include <gtest/gtest.h>
#include <proxy/history_cache.hh>
#include <common/logging.hh>

namespace springtail::pg_proxy {

/**
 * @brief Test fixture for HistoryCache compaction tests
 */
class HistoryCacheCompactTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clear any existing state
        original_history_entries.clear();
        expected_compacted_history.clear();
        current_history_index = 1;
    }

    void TearDown() override {
        original_history_entries.clear();
        expected_compacted_history.clear();
    }

    /**
     * @brief Helper to create a QueryStmt for testing
     */
    QueryStmtPtr create_test_statement(QueryStmt::Type statement_type,
                                     const std::string& statement_name = "",
                                     const std::string& query_text = "")
    {
        std::string final_query_text = query_text.empty() ?
            "DEFAULT QUERY FOR " + std::to_string(static_cast<int>(statement_type)) : query_text;

        return std::make_shared<QueryStmt>(statement_type, final_query_text, true, statement_name);
    }

    /**
     * @brief Helper to add a statement to the original history with auto-incrementing index
     */
    uint64_t add_statement_to_original_history(QueryStmt::Type statement_type,
                                             const std::string& statement_name = "",
                                             const std::string& query_text = "") {
        QueryStmtPtr created_statement = create_test_statement(statement_type, statement_name, query_text);
        uint64_t assigned_index = current_history_index++;
        original_history_entries[assigned_index] = created_statement;
        return assigned_index;
    }

    /**
     * @brief Helper to add a statement to the expected compacted history
     */
    void add_statement_to_expected_history(uint64_t history_index,
                                         QueryStmt::Type statement_type,
                                         const std::string& statement_name = "",
                                         const std::string& query_text = "")
    {
        QueryStmtPtr expected_statement = create_test_statement(statement_type, statement_name, query_text);
        expected_compacted_history[history_index] = expected_statement;
    }

    /**
     * @brief Verify that the actual compacted history matches the expected history exactly
     */
    void verify_exact_compacted_history(const std::map<uint64_t, QueryStmtPtr>& actual_compacted_history)
    {
        EXPECT_EQ(actual_compacted_history.size(), expected_compacted_history.size())
            << "Compacted history size mismatch: expected " << expected_compacted_history.size()
            << ", got " << actual_compacted_history.size();

        // Verify each entry in expected history exists in actual history
        for (const auto& [expected_index, expected_statement] : expected_compacted_history) {
            auto actual_iterator = actual_compacted_history.find(expected_index);
            ASSERT_TRUE(actual_iterator != actual_compacted_history.end())
                << "Expected history entry not found at index " << expected_index;

            const auto& actual_statement = actual_iterator->second;

            EXPECT_EQ(actual_statement->type, expected_statement->type)
                << "Statement type mismatch at index " << expected_index
                << ": expected " << static_cast<int>(expected_statement->type)
                << ", got " << static_cast<int>(actual_statement->type);

            EXPECT_EQ(actual_statement->name, expected_statement->name)
                << "Statement name mismatch at index " << expected_index
                << ": expected '" << expected_statement->name
                << "', got '" << actual_statement->name << "'";

            if (actual_statement->data_type == QueryStmt::SIMPLE &&
                expected_statement->data_type == QueryStmt::SIMPLE) {
                EXPECT_EQ(actual_statement->query(), expected_statement->query())
                    << "Statement query mismatch at index " << expected_index
                    << ": expected '" << expected_statement->query()
                    << "', got '" << actual_statement->query() << "'";
            }
        }

        // Verify no unexpected entries exist in actual history
        for (const auto& [actual_index, actual_statement] : actual_compacted_history) {
            auto expected_iterator = expected_compacted_history.find(actual_index);
            EXPECT_TRUE(expected_iterator != expected_compacted_history.end())
                << "Unexpected history entry found at index " << actual_index
                << " with type " << static_cast<int>(actual_statement->type)
                << " and name '" << actual_statement->name << "'";
        }
    }

    /**
     * @brief Helper to dump history contents for debugging test failures
     */
    void dump_history_comparison_for_debugging(const std::map<uint64_t, QueryStmtPtr>& actual_compacted_history)
    {
        LOG_INFO("=== Original History ({} entries) ===", original_history_entries.size());
        for (const auto& [history_index, statement_ptr] : original_history_entries) {
            LOG_INFO("  [{}] type={}, name='{}', query='{}'",
                    history_index, static_cast<int>(statement_ptr->type),
                    statement_ptr->name,
                    statement_ptr->data_type == QueryStmt::SIMPLE ? statement_ptr->query() : "<packet>");
        }

        LOG_INFO("=== Expected Compacted History ({} entries) ===", expected_compacted_history.size());
        for (const auto& [history_index, statement_ptr] : expected_compacted_history) {
            LOG_INFO("  [{}] type={}, name='{}', query='{}'",
                    history_index, static_cast<int>(statement_ptr->type),
                    statement_ptr->name,
                    statement_ptr->data_type == QueryStmt::SIMPLE ? statement_ptr->query() : "<packet>");
        }

        LOG_INFO("=== Actual Compacted History ({} entries) ===", actual_compacted_history.size());
        for (const auto& [history_index, statement_ptr] : actual_compacted_history) {
            LOG_INFO("  [{}] type={}, name='{}', query='{}'",
                    history_index, static_cast<int>(statement_ptr->type),
                    statement_ptr->name,
                    statement_ptr->data_type == QueryStmt::SIMPLE ? statement_ptr->query() : "<packet>");
        }
    }

    std::map<uint64_t, QueryStmtPtr> original_history_entries;
    std::map<uint64_t, QueryStmtPtr> expected_compacted_history;
    uint64_t current_history_index = 1;
};

/**
 * @brief Test compaction of SET statements below replay index
 */
TEST_F(HistoryCacheCompactTest, CompactSetStatementsBelowReplayIndex)
{
    // Add multiple SET statements for the same variable below replay index
    add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");  // Index 1
    add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");  // Index 2
    uint64_t latest_work_mem_index = add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '512MB'");  // Index 3

    uint64_t replay_index = 10; // All statements are below this index

    // Expected: Only the latest SET statement should remain
    add_statement_to_expected_history(latest_work_mem_index, QueryStmt::SET, "work_mem", "SET work_mem = '512MB'");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test that SET statements above replay index are preserved exactly
 */
TEST_F(HistoryCacheCompactTest, PreserveSetStatementsAboveReplayIndex)
{
    // Add SET statements both below and above replay index
    add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");    // Index 1
    add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");    // Index 2
    add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '384MB'");    // Index 3
    uint64_t above_replay_work_mem_index = add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '512MB'");    // Index 4
    uint64_t above_replay_timezone_index = add_statement_to_original_history(QueryStmt::SET, "timezone", "SET timezone = 'UTC'");      // Index 5

    uint64_t replay_index = 2; // Statements 3, 4, 5 are above this index

    // Expected: Latest work_mem from below replay_index plus all statements above replay_index
    add_statement_to_expected_history(above_replay_work_mem_index, QueryStmt::SET, "work_mem", "SET work_mem = '512MB'");
    add_statement_to_expected_history(above_replay_timezone_index, QueryStmt::SET, "timezone", "SET timezone = 'UTC'");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test compaction of PREPARE and DEALLOCATE statements
 */
TEST_F(HistoryCacheCompactTest, CompactPrepareAndDeallocateStatements)
{
    // Add PREPARE statements
    add_statement_to_original_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");  // Index 1
    uint64_t stmt2_prepare_index = add_statement_to_original_history(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT 2");  // Index 2

    // Add DEALLOCATE for stmt1 below replay index
    add_statement_to_original_history(QueryStmt::DEALLOCATE, "stmt1", "DEALLOCATE stmt1");  // Index 3

    uint64_t replay_index = 10; // All statements are below replay index

    // Expected: stmt1 should be completely removed (PREPARE and DEALLOCATE cancel out),
    // stmt2 PREPARE should remain
    add_statement_to_expected_history(stmt2_prepare_index, QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT 2");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test that DEALLOCATE above replay index preserves both statements
 */
TEST_F(HistoryCacheCompactTest, PreserveDeallocateAboveReplayIndex)
{
    // Add PREPARE statement below replay index
    uint64_t stmt1_prepare_index = add_statement_to_original_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");  // Index 1

    // Add DEALLOCATE above replay index
    uint64_t stmt1_deallocate_index = add_statement_to_original_history(QueryStmt::DEALLOCATE, "stmt1", "DEALLOCATE stmt1");        // Index 2

    uint64_t replay_index = 1; // DEALLOCATE is above this index

    // Expected: Both PREPARE and DEALLOCATE should be preserved since DEALLOCATE is above replay_index
    add_statement_to_expected_history(stmt1_prepare_index, QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");
    add_statement_to_expected_history(stmt1_deallocate_index, QueryStmt::DEALLOCATE, "stmt1", "DEALLOCATE stmt1");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test RESET statement compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactResetStatements)
{
    // Add SET statements
    add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");  // Index 1
    uint64_t timezone_set_index = add_statement_to_original_history(QueryStmt::SET, "timezone", "SET timezone = 'UTC'");  // Index 2

    // Add RESET for specific variable below replay index
    uint64_t work_mem_reset_index = add_statement_to_original_history(QueryStmt::RESET, "work_mem", "RESET work_mem");  // Index 3

    uint64_t replay_index = 10; // All statements are below replay index

    // Expected: work_mem SET should be removed by RESET, timezone should remain, RESET should remain
    add_statement_to_expected_history(timezone_set_index, QueryStmt::SET, "timezone", "SET timezone = 'UTC'");
    add_statement_to_expected_history(work_mem_reset_index, QueryStmt::RESET, "work_mem", "RESET work_mem");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test RESET ALL statement compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactResetAllStatements)
{
    // Add multiple SET statements
    add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");  // Index 1
    add_statement_to_original_history(QueryStmt::SET, "timezone", "SET timezone = 'UTC'");   // Index 2
    add_statement_to_original_history(QueryStmt::SET, "log_level", "SET log_level = 'DEBUG'"); // Index 3

    // Add RESET ALL below replay index
    uint64_t reset_all_index = add_statement_to_original_history(QueryStmt::RESET_ALL, "", "RESET ALL");  // Index 4

    uint64_t replay_index = 10; // All statements are below replay index

    // Expected: All SET statements should be removed by RESET ALL, only RESET ALL should remain
    add_statement_to_expected_history(reset_all_index, QueryStmt::RESET_ALL, "", "RESET ALL");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test DEALLOCATE_ALL statement compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactDeallocateAllStatements)
{
    // Add multiple PREPARE statements
    add_statement_to_original_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");  // Index 1
    add_statement_to_original_history(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT 2");  // Index 2
    add_statement_to_original_history(QueryStmt::PREPARE, "stmt3", "PREPARE stmt3 AS SELECT 3");  // Index 3

    // Add DEALLOCATE ALL below replay index
    uint64_t deallocate_all_index = add_statement_to_original_history(QueryStmt::DEALLOCATE_ALL, "", "DEALLOCATE ALL");  // Index 4

    uint64_t replay_index = 10; // All statements are below replay index

    // Expected: All PREPARE statements should be removed by DEALLOCATE ALL, only DEALLOCATE ALL should remain
    add_statement_to_expected_history(deallocate_all_index, QueryStmt::DEALLOCATE_ALL, "", "DEALLOCATE ALL");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test DISCARD_ALL statement compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactDiscardAllStatements)
{
    // Add various statements
    add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");  // Index 1
    add_statement_to_original_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");  // Index 2
    add_statement_to_original_history(QueryStmt::DECLARE_HOLD, "cursor1", "DECLARE cursor1 CURSOR WITH HOLD FOR SELECT 1");  // Index 3
    add_statement_to_original_history(QueryStmt::LISTEN, "channel1", "LISTEN channel1");  // Index 4

    // Add DISCARD ALL below replay index
    uint64_t discard_all_index = add_statement_to_original_history(QueryStmt::DISCARD_ALL, "", "DISCARD ALL");  // Index 5

    uint64_t replay_index = 10; // All statements are below replay index

    // Expected: DISCARD ALL should clear everything except itself
    add_statement_to_expected_history(discard_all_index, QueryStmt::DISCARD_ALL, "", "DISCARD ALL");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test cursor/portal compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactCursorStatements)
{
    // Add cursor declarations
    add_statement_to_original_history(QueryStmt::DECLARE_HOLD, "cursor1", "DECLARE cursor1 CURSOR WITH HOLD FOR SELECT 1");  // Index 1
    add_statement_to_original_history(QueryStmt::FETCH, "cursor1", "FETCH 10 FROM cursor1");  // Index 2
    uint64_t cursor2_declare_index = add_statement_to_original_history(QueryStmt::DECLARE_HOLD, "cursor2", "DECLARE cursor2 CURSOR WITH HOLD FOR SELECT 2");  // Index 3

    // Close cursor1 below replay index
    add_statement_to_original_history(QueryStmt::CLOSE, "cursor1", "CLOSE cursor1");  // Index 4

    uint64_t replay_index = 10; // All statements are below replay index

    // Expected: cursor1 and its FETCH should be removed by CLOSE, cursor2 should remain, CLOSE should remain
    add_statement_to_expected_history(cursor2_declare_index, QueryStmt::DECLARE_HOLD, "cursor2", "DECLARE cursor2 CURSOR WITH HOLD FOR SELECT 2");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test complex mixed scenario with statements above and below replay index
 */
TEST_F(HistoryCacheCompactTest, ComplexMixedScenario)
{
    // Statements below replay index (should be compacted)
    add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");      // Index 1
    uint64_t work_mem_final_index = add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");      // Index 2
    add_statement_to_original_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1"); // Index 3
    add_statement_to_original_history(QueryStmt::DEALLOCATE, "stmt1", "DEALLOCATE stmt1");       // Index 4

    // Statements above replay index (should be preserved)
    uint64_t timezone_set_index = add_statement_to_original_history(QueryStmt::SET, "timezone", "SET timezone = 'UTC'");       // Index 5
    uint64_t stmt2_prepare_index = add_statement_to_original_history(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT 2"); // Index 6
    uint64_t stmt2_deallocate_index = add_statement_to_original_history(QueryStmt::DEALLOCATE, "stmt2", "DEALLOCATE stmt2");       // Index 7

    uint64_t replay_index = 4; // Statements 5, 6, 7 are above this index

    // Expected:
    // - work_mem should have latest value from below replay_index
    // - stmt1 should be completely removed (PREPARE and DEALLOCATE cancel out)
    // - All statements above replay_index should be preserved
    add_statement_to_expected_history(work_mem_final_index, QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");
    add_statement_to_expected_history(timezone_set_index, QueryStmt::SET, "timezone", "SET timezone = 'UTC'");
    add_statement_to_expected_history(stmt2_prepare_index, QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT 2");
    add_statement_to_expected_history(stmt2_deallocate_index, QueryStmt::DEALLOCATE, "stmt2", "DEALLOCATE stmt2");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test empty history compaction
 */
TEST_F(HistoryCacheCompactTest, CompactEmptyHistory)
{
    uint64_t replay_index = 5;

    // Expected: Empty history should remain empty
    // (no statements added to expected_compacted_history)

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

/**
 * @brief Test compaction with replay index of zero
 */
TEST_F(HistoryCacheCompactTest, CompactWithZeroReplayIndex)
{
    add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");  // Index 1
    uint64_t stmt1_prepare_index = add_statement_to_original_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");  // Index 2
    uint64_t work_mem_set_index = add_statement_to_original_history(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");  // Index 3
    uint64_t replay_index = 0; // All statements are above this index

    // The second SET overwrites the first
    add_statement_to_expected_history(stmt1_prepare_index, QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");
    add_statement_to_expected_history(work_mem_set_index, QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");

    auto actual_compacted_history = HistoryCache::compact(replay_index, original_history_entries);

    verify_exact_compacted_history(actual_compacted_history);
    dump_history_comparison_for_debugging(actual_compacted_history);
}

} // namespace springtail::pg_proxy