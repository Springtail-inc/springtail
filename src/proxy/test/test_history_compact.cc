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
        history_entries.clear();
        current_index = 1;
    }

    void TearDown() override {
        history_entries.clear();
    }

    /**
     * @brief Helper to create a QueryStmt for testing
     */
    QueryStmtPtr create_statement(QueryStmt::Type statement_type,
                                const std::string& statement_name = "",
                                const std::string& query_text = "") {
        std::string final_query_text = query_text.empty() ?
            "DEFAULT QUERY FOR " + std::to_string(static_cast<int>(statement_type)) : query_text;

        return std::make_shared<QueryStmt>(statement_type, final_query_text, true, statement_name);
    }

    /**
     * @brief Helper to add a statement to the history with auto-incrementing index
     */
    void add_statement_to_history(QueryStmt::Type statement_type,
                                const std::string& statement_name = "",
                                const std::string& query_text = "") {
        QueryStmtPtr statement_ptr = create_statement(statement_type, statement_name, query_text);
        history_entries[current_index++] = statement_ptr;
    }

    /**
     * @brief Helper to verify that a statement exists in the compacted history
     */
    void expect_statement_exists(const std::map<uint64_t, QueryStmtPtr>& compacted_history,
                               QueryStmt::Type expected_type,
                               const std::string& expected_name = "") {
        bool statement_found = false;
        for (const auto& [history_index, statement_ptr] : compacted_history) {
            if (statement_ptr->type == expected_type && statement_ptr->name == expected_name) {
                statement_found = true;
                break;
            }
        }
        EXPECT_TRUE(statement_found)
            << "Expected statement not found: type=" << static_cast<int>(expected_type)
            << ", name='" << expected_name << "'";
    }

    /**
     * @brief Helper to verify that a statement does not exist in the compacted history
     */
    void expect_statement_not_exists(const std::map<uint64_t, QueryStmtPtr>& compacted_history,
                                   QueryStmt::Type expected_type,
                                   const std::string& expected_name = "") {
        bool statement_found = false;
        for (const auto& [history_index, statement_ptr] : compacted_history) {
            if (statement_ptr->type == expected_type && statement_ptr->name == expected_name) {
                statement_found = true;
                break;
            }
        }
        EXPECT_FALSE(statement_found)
            << "Unexpected statement found: type=" << static_cast<int>(expected_type)
            << ", name='" << expected_name << "'";
    }

    /**
     * @brief Helper to count statements of a specific type in compacted history
     */
    size_t count_statements_by_type(const std::map<uint64_t, QueryStmtPtr>& compacted_history,
                                  QueryStmt::Type statement_type) {
        size_t statement_count = 0;
        for (const auto& [history_index, statement_ptr] : compacted_history) {
            if (statement_ptr->type == statement_type) {
                statement_count++;
            }
        }
        return statement_count;
    }

    /**
     * @brief Helper to dump history contents for debugging
     */
    void dump_history_contents(const std::map<uint64_t, QueryStmtPtr>& history_map,
                             const std::string& history_description) {
        LOG_INFO("=== {} ({} entries) ===", history_description, history_map.size());
        for (const auto& [history_index, statement_ptr] : history_map) {
            LOG_INFO("  [{}] type={}, name='{}', query='{}'",
                    history_index, static_cast<int>(statement_ptr->type),
                    statement_ptr->name,
                    statement_ptr->data_type == QueryStmt::SIMPLE ? statement_ptr->query() : "<packet>");
        }
    }

    std::map<uint64_t, QueryStmtPtr> history_entries;
    uint64_t current_index = 1;
};

/**
 * @brief Test compaction of SET statements below replay index
 */
TEST_F(HistoryCacheCompactTest, CompactSetStatementsBelowReplayIndex) {
    // Add multiple SET statements for the same variable below replay index
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '512MB'");

    uint64_t replay_index = 10; // All statements are below this index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // Should only have the latest SET statement for work_mem
    EXPECT_EQ(count_statements_by_type(compacted_history, QueryStmt::SET), 1);
    expect_statement_exists(compacted_history, QueryStmt::SET, "work_mem");

    // Verify it's the latest value by checking the query content
    bool found_latest_value = false;
    for (const auto& [history_index, statement_ptr] : compacted_history) {
        if (statement_ptr->type == QueryStmt::SET && statement_ptr->name == "work_mem") {
            EXPECT_TRUE(statement_ptr->query().find("512MB") != std::string::npos);
            found_latest_value = true;
            break;
        }
    }
    EXPECT_TRUE(found_latest_value);

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test that SET statements above replay index are preserved
 */
TEST_F(HistoryCacheCompactTest, PreserveSetStatementsAboveReplayIndex) {
    // Add SET statements both below and above replay index
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");    // Index 1
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");    // Index 2
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '512MB'");    // Index 3
    add_statement_to_history(QueryStmt::SET, "timezone", "SET timezone = 'UTC'");      // Index 4

    uint64_t replay_index = 2; // Statements 3 and 4 are above this index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // Should have the latest work_mem from below replay_index plus statements above replay_index
    EXPECT_EQ(count_statements_by_type(compacted_history, QueryStmt::SET), 3);

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test compaction of PREPARE and DEALLOCATE statements
 */
TEST_F(HistoryCacheCompactTest, CompactPrepareAndDeallocateStatements) {
    // Add PREPARE statements
    add_statement_to_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");
    add_statement_to_history(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT 2");

    // Add DEALLOCATE for stmt1 below replay index
    add_statement_to_history(QueryStmt::DEALLOCATE, "stmt1", "DEALLOCATE stmt1");

    uint64_t replay_index = 10; // All statements are below replay index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // stmt1 should be removed due to DEALLOCATE, stmt2 should remain
    expect_statement_not_exists(compacted_history, QueryStmt::PREPARE, "stmt1");
    expect_statement_exists(compacted_history, QueryStmt::PREPARE, "stmt2");
    expect_statement_not_exists(compacted_history, QueryStmt::DEALLOCATE, "stmt1");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test that DEALLOCATE above replay index is preserved
 */
TEST_F(HistoryCacheCompactTest, PreserveDeallocateAboveReplayIndex) {
    // Add PREPARE statement below replay index
    add_statement_to_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");  // Index 1

    // Add DEALLOCATE above replay index
    add_statement_to_history(QueryStmt::DEALLOCATE, "stmt1", "DEALLOCATE stmt1");        // Index 2

    uint64_t replay_index = 1; // DEALLOCATE is above this index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // Both PREPARE and DEALLOCATE should be preserved since DEALLOCATE is above replay_index
    expect_statement_exists(compacted_history, QueryStmt::PREPARE, "stmt1");
    expect_statement_exists(compacted_history, QueryStmt::DEALLOCATE, "stmt1");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test RESET statement compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactResetStatements) {
    // Add SET statements
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");
    add_statement_to_history(QueryStmt::SET, "timezone", "SET timezone = 'UTC'");

    // Add RESET for specific variable below replay index
    add_statement_to_history(QueryStmt::RESET, "work_mem", "RESET work_mem");

    uint64_t replay_index = 10; // All statements are below replay index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // work_mem SET should be removed by RESET, timezone should remain
    expect_statement_not_exists(compacted_history, QueryStmt::SET, "work_mem");
    expect_statement_exists(compacted_history, QueryStmt::SET, "timezone");
    expect_statement_exists(compacted_history, QueryStmt::RESET, "work_mem");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test RESET ALL statement compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactResetAllStatements) {
    // Add multiple SET statements
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");
    add_statement_to_history(QueryStmt::SET, "timezone", "SET timezone = 'UTC'");
    add_statement_to_history(QueryStmt::SET, "log_level", "SET log_level = 'DEBUG'");

    // Add RESET ALL below replay index
    add_statement_to_history(QueryStmt::RESET, "", "RESET ALL");

    uint64_t replay_index = 10; // All statements are below replay index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // All SET statements should be removed by RESET ALL
    EXPECT_EQ(count_statements_by_type(compacted_history, QueryStmt::SET), 0);
    expect_statement_exists(compacted_history, QueryStmt::RESET, "");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test DEALLOCATE_ALL statement compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactDeallocateAllStatements) {
    // Add multiple PREPARE statements
    add_statement_to_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");
    add_statement_to_history(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT 2");
    add_statement_to_history(QueryStmt::PREPARE, "stmt3", "PREPARE stmt3 AS SELECT 3");

    // Add DEALLOCATE ALL below replay index
    add_statement_to_history(QueryStmt::DEALLOCATE_ALL, "", "DEALLOCATE ALL");

    uint64_t replay_index = 10; // All statements are below replay index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // All PREPARE statements should be removed by DEALLOCATE ALL
    EXPECT_EQ(count_statements_by_type(compacted_history, QueryStmt::PREPARE), 0);
    expect_statement_exists(compacted_history, QueryStmt::DEALLOCATE_ALL, "");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test DISCARD_ALL statement compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactDiscardAllStatements) {
    // Add various statements
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");
    add_statement_to_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");
    add_statement_to_history(QueryStmt::DECLARE_HOLD, "cursor1", "DECLARE cursor1 CURSOR WITH HOLD FOR SELECT 1");
    add_statement_to_history(QueryStmt::LISTEN, "channel1", "LISTEN channel1");

    // Add DISCARD ALL below replay index
    add_statement_to_history(QueryStmt::DISCARD_ALL, "", "DISCARD ALL");

    uint64_t replay_index = 10; // All statements are below replay index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // DISCARD ALL should clear everything except itself
    EXPECT_EQ(compacted_history.size(), 1);
    expect_statement_exists(compacted_history, QueryStmt::DISCARD_ALL, "");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test cursor/portal compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactCursorStatements) {
    // Add cursor declarations
    add_statement_to_history(QueryStmt::DECLARE_HOLD, "cursor1", "DECLARE cursor1 CURSOR WITH HOLD FOR SELECT 1");
    add_statement_to_history(QueryStmt::FETCH, "cursor1", "FETCH 10 FROM cursor1");
    add_statement_to_history(QueryStmt::DECLARE_HOLD, "cursor2", "DECLARE cursor2 CURSOR WITH HOLD FOR SELECT 2");

    // Close cursor1 below replay index
    add_statement_to_history(QueryStmt::CLOSE, "cursor1", "CLOSE cursor1");

    uint64_t replay_index = 10; // All statements are below replay index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // cursor1 and its FETCH should be removed by CLOSE, cursor2 should remain
    expect_statement_not_exists(compacted_history, QueryStmt::DECLARE_HOLD, "cursor1");
    expect_statement_not_exists(compacted_history, QueryStmt::FETCH, "cursor1");
    expect_statement_exists(compacted_history, QueryStmt::DECLARE_HOLD, "cursor2");
    expect_statement_not_exists(compacted_history, QueryStmt::CLOSE, "cursor1");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test CLOSE_ALL statement compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactCloseAllStatements) {
    // Add multiple cursor declarations and fetches
    add_statement_to_history(QueryStmt::DECLARE_HOLD, "cursor1", "DECLARE cursor1 CURSOR WITH HOLD FOR SELECT 1");
    add_statement_to_history(QueryStmt::FETCH, "cursor1", "FETCH 10 FROM cursor1");
    add_statement_to_history(QueryStmt::DECLARE_HOLD, "cursor2", "DECLARE cursor2 CURSOR WITH HOLD FOR SELECT 2");
    add_statement_to_history(QueryStmt::FETCH, "cursor2", "FETCH 5 FROM cursor2");

    // Add CLOSE ALL below replay index
    add_statement_to_history(QueryStmt::CLOSE_ALL, "", "CLOSE ALL");

    uint64_t replay_index = 10; // All statements are below replay index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // All cursor-related statements should be removed by CLOSE ALL
    EXPECT_EQ(count_statements_by_type(compacted_history, QueryStmt::DECLARE_HOLD), 0);
    EXPECT_EQ(count_statements_by_type(compacted_history, QueryStmt::FETCH), 0);
    expect_statement_exists(compacted_history, QueryStmt::CLOSE_ALL, "");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test LISTEN and UNLISTEN statement compaction behavior
 */
TEST_F(HistoryCacheCompactTest, CompactListenUnlistenStatements) {
    // Add LISTEN statements
    add_statement_to_history(QueryStmt::LISTEN, "channel1", "LISTEN channel1");
    add_statement_to_history(QueryStmt::LISTEN, "channel2", "LISTEN channel2");
    add_statement_to_history(QueryStmt::LISTEN, "channel3", "LISTEN channel3");

    // UNLISTEN specific channel below replay index
    add_statement_to_history(QueryStmt::UNLISTEN, "channel1", "UNLISTEN channel1");

    uint64_t replay_index = 10; // All statements are below replay index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // channel1 LISTEN should be removed by UNLISTEN, others should remain
    expect_statement_not_exists(compacted_history, QueryStmt::LISTEN, "channel1");
    expect_statement_exists(compacted_history, QueryStmt::LISTEN, "channel2");
    expect_statement_exists(compacted_history, QueryStmt::LISTEN, "channel3");
    expect_statement_exists(compacted_history, QueryStmt::UNLISTEN, "channel1");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test complex mixed scenario with statements above and below replay index
 */
TEST_F(HistoryCacheCompactTest, ComplexMixedScenario) {
    // Statements below replay index (should be compacted)
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");      // Index 1
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '256MB'");      // Index 2
    add_statement_to_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1"); // Index 3
    add_statement_to_history(QueryStmt::DEALLOCATE, "stmt1", "DEALLOCATE stmt1");       // Index 4

    // Statements above replay index (should be preserved)
    add_statement_to_history(QueryStmt::SET, "timezone", "SET timezone = 'UTC'");       // Index 5
    add_statement_to_history(QueryStmt::PREPARE, "stmt2", "PREPARE stmt2 AS SELECT 2"); // Index 6
    add_statement_to_history(QueryStmt::DEALLOCATE, "stmt2", "DEALLOCATE stmt2");       // Index 7

    uint64_t replay_index = 4; // Statements 5, 6, 7 are above this index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // Below replay_index: work_mem should be latest value, stmt1 should be removed by DEALLOCATE
    bool found_work_mem_256mb = false;
    for (const auto& [history_index, statement_ptr] : compacted_history) {
        if (statement_ptr->type == QueryStmt::SET && statement_ptr->name == "work_mem") {
            EXPECT_TRUE(statement_ptr->query().find("256MB") != std::string::npos);
            found_work_mem_256mb = true;
            break;
        }
    }
    EXPECT_TRUE(found_work_mem_256mb);
    expect_statement_not_exists(compacted_history, QueryStmt::PREPARE, "stmt1");
    expect_statement_not_exists(compacted_history, QueryStmt::DEALLOCATE, "stmt1");

    // Above replay_index: all statements should be preserved
    expect_statement_exists(compacted_history, QueryStmt::SET, "timezone");
    expect_statement_exists(compacted_history, QueryStmt::PREPARE, "stmt2");
    expect_statement_exists(compacted_history, QueryStmt::DEALLOCATE, "stmt2");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

/**
 * @brief Test empty history compaction
 */
TEST_F(HistoryCacheCompactTest, CompactEmptyHistory) {
    uint64_t replay_index = 5;

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    EXPECT_TRUE(compacted_history.empty());
}

/**
 * @brief Test compaction with replay index of zero
 */
TEST_F(HistoryCacheCompactTest, CompactWithZeroReplayIndex) {
    add_statement_to_history(QueryStmt::SET, "work_mem", "SET work_mem = '128MB'");
    add_statement_to_history(QueryStmt::PREPARE, "stmt1", "PREPARE stmt1 AS SELECT 1");

    uint64_t replay_index = 0; // All statements are above this index

    auto compacted_history = HistoryCache::compact(replay_index, history_entries);

    // All statements should be preserved since they're all above replay_index
    EXPECT_EQ(compacted_history.size(), history_entries.size());
    expect_statement_exists(compacted_history, QueryStmt::SET, "work_mem");
    expect_statement_exists(compacted_history, QueryStmt::PREPARE, "stmt1");

    dump_history_contents(history_entries, "Original History");
    dump_history_contents(compacted_history, "Compacted History");
}

} // namespace springtail::pg_proxy