#include <algorithm>
#include <span>

#include <common/constants.hh>
#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/json.hh>
#include <common/properties.hh>
#include <common/filesystem.hh>

#include <pg_log_mgr/index_reconciliation_queue_manager.hh>
#include <pg_log_mgr/indexer.hh>
#include <storage/index_engine/gin_helpers.hh>
#include <storage/index_engine/gist_helpers.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/table.hh>

#include <pg_repl/pg_repl_msg.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table_mgr.hh>

#include <test/services.hh>
#include <test/ddl_helpers.hh>
#include <storage/vacuumer.hh>
#include <xid_mgr/xid_mgr_server.hh>

#include <pg_ext/extn_registry.hh>
// pg_ext/extn_registry.hh transitively includes postgresql/server/catalog/pg_type_d.h, which
// #defines INT4OID (and friends) as raw integer literals -- colliding with this file's own,
// pre-existing `static constexpr int32_t INT4OID = 23;` fixture member below. Undefine the
// macro right after the include so the rest of this file's (unrelated, pre-existing) uses of
// the identifier `INT4OID` keep meaning the fixture's own constant, not the macro.
#undef INT4OID

using namespace springtail;
using namespace springtail::committer;
using namespace springtail::test::ddl_helpers;

namespace {

    /**
     * Framework for Indexer testing.
     */
    class Indexer_Test : public testing::Test {
    protected:

        // Called once per testsuite.  Create a table and populate it with data
        static void SetUpTestSuite()
        {
            springtail_init_test();
            test::start_services(true, true, true);

            _db_id = 1;
            _tid = 1000;
            access_xid = 1, target_xid = 2;

            // create the public namespace
            PgMsgNamespace ns_msg;
            ns_msg.oid = 900;
            ns_msg.name = "public";
            sys_tbl_mgr::Server::get_instance()->create_namespace(_db_id, { access_xid, 0 }, ns_msg);
            access_xid++;
            target_xid++;

            _columns = {
                {"col1", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 1, 0, false, true, false},
                {"col2", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 2, 1, false, true, false},
                {"col3", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 3, 2, false, true, false},
                {"col4", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 4, 0, false, false, false},
                {"col5", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 5, 0, false, false, false},
            };

            _index_reconciliation_queue_mgr = std::make_shared<springtail::pg_log_mgr::IndexReconciliationQueueManager>();
            _index_reconciliation_queue_mgr->add_queue(_db_id);
            _indexer = std::make_unique<Indexer>(1, _index_reconciliation_queue_mgr);
        }

        static void TearDownTestSuite() {
            // Cleanup vacuum files
            Vacuumer::get_instance()->cleanup_db(_db_id);

            _index_reconciliation_queue_mgr->remove_queue(_db_id);
            _indexer.reset();
            springtail_shutdown();
        }


        inline static uint64_t _db_id;
        inline static uint64_t _tid;
        inline static uint64_t access_xid, target_xid;

        inline static std::filesystem::path _table_dir;
        inline static std::filesystem::path _base_dir;

        inline static std::vector<PgMsgSchemaColumn> _columns;

        inline static std::vector<std::vector<int32_t>> _data;

        static constexpr uint32_t _secondary_index_id{1234};

        static constexpr int32_t INT4OID = 23;

        inline static std::unique_ptr<Indexer> _indexer;

        inline static std::shared_ptr<springtail::pg_log_mgr::IndexReconciliationQueueManager> _index_reconciliation_queue_mgr;

        void _populate_table_with_data(uint64_t table_id, uint64_t table_xid,
                uint64_t data_xid, int num_rows, int num_cols, int start_value = 0, bool is_update=false) {
            std::vector<std::vector<int32_t>> _data;
            _data.clear();
            _data.reserve(num_rows);

            for (size_t i = 0; i < num_rows; ++i) {
                std::vector<int32_t> row;
                row.reserve(num_cols);
                for (size_t j = 0; j < num_cols; ++j) {
                    row.push_back(start_value + static_cast<int32_t>(i * 10 + j));
                }
                _data.push_back(std::move(row));
            }

            // create a mutable table
            auto mtable = TableMgr::get_instance()->get_mutable_table(_db_id, table_id, table_xid, data_xid);

            // insert a number of rows
            populate_table(mtable, _data, is_update, start_value);

            // finalize the table and update roots
            auto &&metadata = mtable->finalize(true);
            sys_tbl_mgr::Server::get_instance()->update_roots(_db_id, table_id, data_xid, metadata);
        }

        void _create_index(uint64_t table_id, uint64_t index_id, uint64_t index_xid, std::string index_name,
                bool process_requests_in_indexer=true, bool is_unique=false, std::string index_type="btree",
                std::optional<std::vector<PgMsgSchemaColumn>> columns_override=std::nullopt,
                std::vector<std::string> opclasses={}) {
            // Create index at an XID
            std::list<proto::IndexProcessRequest> index_requests;
            auto index_columns = columns_override.has_value()
                    ? *columns_override
                    : std::vector<PgMsgSchemaColumn>(_columns.end() - 2, _columns.end());
            auto create_idx_request = create_index(_db_id, table_id, index_xid, index_id, index_name,
                    index_columns, sys_tbl::IndexNames::State::NOT_READY, is_unique,
                    index_type, opclasses);
            index_requests.push_back(std::move(create_idx_request));

            // Validate index as NOT_READY
            auto index_info = sys_tbl_mgr::Server::get_instance()->get_index_info(_db_id, index_id, {index_xid, constant::MAX_LSN});
            ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(index_info.state()), sys_tbl::IndexNames::State::NOT_READY);

            // Process Index requests
            if (process_requests_in_indexer) {
                _indexer->process_requests(_db_id, index_xid, index_requests);
            }
            sys_tbl_mgr::Server::get_instance()->finalize(_db_id, index_xid, true);
        }

        void _process_index_and_validate(uint64_t index_id, uint64_t index_xid, uint64_t reconcile_xid) {
            // Wait for index build to be completed before triggering reconciliation
            auto max_tries = 5;
            auto try_count = 0;
            auto index_ready_for_reconcile = false;
            while (try_count < max_tries) {
                if (auto request = _index_reconciliation_queue_mgr->pop(_db_id, constant::COORDINATOR_KEEP_ALIVE_TIMEOUT)) {
                    if (request != nullptr) {
                        index_ready_for_reconcile = true;
                        break;
                    }
                }
                try_count++;
            }

            ASSERT_TRUE(index_ready_for_reconcile);

            // Trigger index reconcilation at reconcile_xid
            _indexer->process_index_reconciliation(_db_id, index_xid, reconcile_xid);
            auto index_info = sys_tbl_mgr::Server::get_instance()->get_index_info(_db_id, index_id, {reconcile_xid, constant::MAX_LSN});

            // Validate index as READY at reconcile_xid
            ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(index_info.state()), sys_tbl::IndexNames::State::READY);
        }

    };

    TEST_F(Indexer_Test, Test_EmptyReconcile)
    {
        uint64_t table_id = _tid++;
        uint64_t index_id = _secondary_index_id + 1;
        uint64_t table_xid = access_xid++;
        uint64_t index_xid = access_xid++;

        // Create table
        create_table(_db_id, table_id, table_xid, "test_indexer_table1", _columns);

        // Create index
        _create_index(table_id, index_id, index_xid, "idx_test_indexer_1");

        // Trigger index reconcilation at reconcile_xid
        uint64_t reconcile_xid = access_xid++;
        _process_index_and_validate(index_id, index_xid, reconcile_xid);
    }

    TEST_F(Indexer_Test, Test_ReconcileAfterInserts)
    {
        uint64_t table_id = _tid++;
        uint64_t index_id = _secondary_index_id + 2;
        uint64_t table_xid = access_xid++;
        uint64_t index_xid = access_xid++;
        uint64_t data_xid = access_xid++;

        // Create table
        create_table(_db_id, table_id, table_xid, "test_indexer_table2", _columns);

        // Create index
        _create_index(table_id, index_id, index_xid, "idx_test_indexer_2");

        // Populate table
        _populate_table_with_data(table_id, table_xid, data_xid, 100000, 5);

        // Trigger index reconcilation at reconcile_xid
        uint64_t reconcile_xid = access_xid++;
        _process_index_and_validate(index_id, index_xid, reconcile_xid);
    }

    TEST_F(Indexer_Test, Test_ReconcileAlongInserts)
    {
        uint64_t table_id = _tid++;
        uint64_t index_id = _secondary_index_id + 3;
        uint64_t table_xid = access_xid++;
        uint64_t data_xid1 = access_xid++;
        uint64_t index_xid = access_xid++;
        uint64_t data_xid2 = access_xid++;

        // Create table
        create_table(_db_id, table_id, table_xid, "test_indexer_table3", _columns);

        int num_rows = 2000;
        // Populate table
        _populate_table_with_data(table_id, table_xid, data_xid1, num_rows, 5);

        // Create index
        _create_index(table_id, index_id, index_xid, "idx_test_indexer_3");

        // Populate table
        _populate_table_with_data(table_id, index_xid, data_xid2, num_rows, 5, num_rows + 1);

        // Trigger index reconcilation at reconcile_xid
        uint64_t reconcile_xid = access_xid++;
        _process_index_and_validate(index_id, index_xid, reconcile_xid);
    }

    TEST_F(Indexer_Test, Test_IndexInSchema)
    {
        uint64_t table_id = _tid++;
        uint64_t index_id1 = _secondary_index_id + 4;
        uint64_t index_id2 = _secondary_index_id + 5;
        uint64_t table_xid = access_xid++;
        uint64_t index_xid1 = access_xid++;
        uint64_t index_xid2 = access_xid++;
        uint64_t reconcile_xid1 = access_xid++;
        uint64_t data_xid1 = access_xid++;
        uint64_t reconcile_xid2 = access_xid++;

        // Create table
        create_table(_db_id, table_id, table_xid, "test_indexer_table4", _columns);

        // Create index
        _create_index(table_id, index_id1, index_xid1, "idx_test_indexer_4");

        // Trigger index reconcilation at reconcile_xid
        _process_index_and_validate(index_id1, index_xid1, reconcile_xid1);

        // Create index with older xid
        _create_index(table_id, index_id2, index_xid2, "idx_test_indexer_5");

        // Check if are still seeing the first index in the schema
        auto &&meta = sys_tbl_mgr::Server::get_instance()->get_schema(_db_id, table_id, XidLsn{data_xid1});
        auto it = std::ranges::find_if(meta->indexes,
                [&](auto const& v) { return index_id1 == v.id; });
        ASSERT_TRUE(it != meta->indexes.end());
        ASSERT_EQ(it->state, 1);

        int num_rows = 20;
        // Populate table with newer xid
        _populate_table_with_data(table_id, table_xid, data_xid1, num_rows, 5);

        // Trigger index reconcilation at reconcile_xid
        _process_index_and_validate(index_id2, index_xid2, reconcile_xid2);

        meta = sys_tbl_mgr::Server::get_instance()->get_schema(_db_id, table_id, XidLsn{reconcile_xid2});
        it = std::ranges::find_if(meta->indexes,
                [&](auto const& v) { return index_id1 == v.id; });
        ASSERT_TRUE(it != meta->indexes.end());
        ASSERT_EQ(it->state, 1);

        it = std::ranges::find_if(meta->indexes,
                [&](auto const& v) { return index_id2 == v.id; });
        ASSERT_TRUE(it != meta->indexes.end());
        ASSERT_EQ(it->state, 1);
    }

    TEST_F(Indexer_Test, Test_IndexRecovery)
    {
        uint64_t table_id = _tid++;
        uint64_t index_id1 = _secondary_index_id + 6;
        uint64_t table_xid = access_xid++;
        uint64_t index_xid1 = access_xid++;
        uint64_t reconcile_xid1 = access_xid++;
        uint64_t data_xid1 = access_xid++;

        // Create table
        create_table(_db_id, table_id, table_xid, "test_indexer_table5", _columns);

        // Create index
        _create_index(table_id, index_id1, index_xid1, "idx_test_indexer_5", false);

        auto &&meta = sys_tbl_mgr::Server::get_instance()->get_schema(_db_id, table_id, XidLsn{data_xid1});
        auto it = std::ranges::find_if(meta->indexes,
                [&](auto const& v) { return index_id1 == v.id; });
        ASSERT_TRUE(it != meta->indexes.end());
        ASSERT_EQ(it->state, 0);

        // Recover indexes
        _indexer->recover_indexes(_db_id);

        // Trigger index reconcilation at reconcile_xid
        _process_index_and_validate(index_id1, index_xid1, reconcile_xid1);

        meta = sys_tbl_mgr::Server::get_instance()->get_schema(_db_id, table_id, XidLsn{data_xid1});
        it = std::ranges::find_if(meta->indexes,
                [&](auto const& v) { return index_id1 == v.id; });
        ASSERT_TRUE(it != meta->indexes.end());
        ASSERT_EQ(it->state, 1);
    }

    TEST_F(Indexer_Test, Test_VacuumSecondaryIndex)
    {
        uint64_t table_id = _tid++;
        uint64_t index_id = _secondary_index_id + 7;
        uint64_t table_xid = access_xid++;
        uint64_t index_xid = access_xid++;
        uint64_t data_xid = access_xid++;
        uint64_t reconcile_xid = access_xid++;

        // Enable extents tracking in vacuumer
        Vacuumer::get_instance()->enable_tracking_extents();

        // Create table
        create_table(_db_id, table_id, table_xid, "test_indexer_table6", _columns);

        // Create index
        _create_index(table_id, index_id, index_xid, "idx_test_indexer_6", true, true);

        // Populate table
        _populate_table_with_data(table_id, table_xid, data_xid, 30000, 5);

        // Trigger index reconcilation at reconcile_xid
        _process_index_and_validate(index_id, index_xid, reconcile_xid);

        // Get the table file paths
        auto table_dir = TableMgr::get_instance()->get_table_data_dir(_db_id, table_id, access_xid);
        EXPECT_TRUE(table_dir.has_value());

        auto next_data_xid = access_xid;
        for (int i=0; i < 20; i++) {
            next_data_xid = access_xid++;
            _populate_table_with_data(table_id, next_data_xid, next_data_xid, 30, 5, 0, true);
        }

        Vacuumer::get_instance()->commit_expired_extents(_db_id, constant::LATEST_XID);

        xid_mgr::XidMgrServer::get_instance()->commit_xid(_db_id, 0, next_data_xid, true, 0);

        // Get the blocks count pre-vacuum
        auto index_file = table_dir.value() / fmt::format(constant::INDEX_FILE, index_id);
        auto size_pre_vacuum = fs::get_block_count(index_file);

        // Set global threshold as small and run vacuum
        Vacuumer::get_instance()->set_global_vacuum_threshold(10);
        Vacuumer::get_instance()->run_vacuum_once();

        // Get the blocks count post-vacuum
        auto size_post_vacuum = fs::get_block_count(index_file);

        // Some blocks should be vacuumed
        ASSERT_GT(size_pre_vacuum, size_post_vacuum);

    }

    // Regression test for architecture.md Sec 5.4 case 2 / phase-3.md's look-aside bootstrap
    // audit: two concurrent "first index on this table" builds (indexes on a table that has no
    // secondary index yet) must create exactly one look-aside index, and both builds' own
    // secondary indexes must end up correct -- which, for Table::Iterator::Secondary::update_page,
    // depends on the look-aside index being completely and correctly populated regardless of
    // which of the two builders actually owned building it. Uses a dedicated 2-worker Indexer
    // (the shared fixture's _indexer only has 1 worker, so its two builds would never actually
    // run concurrently) so the two builds genuinely race for look-aside ownership.
    TEST_F(Indexer_Test, Test_ConcurrentFirstIndexBuildsShareLookAside)
    {
        uint64_t table_id = _tid++;
        uint64_t index_id_a = _secondary_index_id + 8;
        uint64_t index_id_b = _secondary_index_id + 9;
        uint64_t table_xid = access_xid++;
        uint64_t index_xid_a = access_xid++;
        uint64_t index_xid_b = access_xid++;

        // Create table and populate with enough rows that each builder's bootstrap/catch-up
        // takes long enough to give the other builder a real chance to observe the tracker entry
        // mid-flight, rather than the race trivially resolving before either starts.
        create_table(_db_id, table_id, table_xid, "test_indexer_table_concurrent_la", _columns);
        _populate_table_with_data(table_id, table_xid, table_xid, 5000, 5);

        auto queue_mgr = std::make_shared<springtail::pg_log_mgr::IndexReconciliationQueueManager>();
        queue_mgr->add_queue(_db_id);
        auto indexer = std::make_unique<Indexer>(2, queue_mgr);

        // Two independent "first index on this table" DDLs -- separate DDL transactions (own
        // XIDs, own reconciliation batches, matching how CREATE INDEX normally arrives one at a
        // time), but submitted back-to-back with no synchronization between them, so Indexer's
        // (two) worker threads genuinely race to build each one concurrently.
        auto index_request_a = create_index(_db_id, table_id, index_xid_a, index_id_a, "idx_concurrent_la_a",
                std::vector<PgMsgSchemaColumn>(_columns.end() - 2, _columns.end() - 1), sys_tbl::IndexNames::State::NOT_READY);
        auto index_request_b = create_index(_db_id, table_id, index_xid_b, index_id_b, "idx_concurrent_la_b",
                std::vector<PgMsgSchemaColumn>(_columns.end() - 1, _columns.end()), sys_tbl::IndexNames::State::NOT_READY);

        indexer->process_requests(_db_id, index_xid_a, {index_request_a});
        sys_tbl_mgr::Server::get_instance()->finalize(_db_id, index_xid_a, true);
        indexer->process_requests(_db_id, index_xid_b, {index_request_b});
        sys_tbl_mgr::Server::get_instance()->finalize(_db_id, index_xid_b, true);

        auto request_a = queue_mgr->pop(_db_id, constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
        ASSERT_NE(request_a, nullptr);
        auto request_b = queue_mgr->pop(_db_id, constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
        ASSERT_NE(request_b, nullptr);

        uint64_t reconcile_xid_a = access_xid++;
        indexer->process_index_reconciliation(_db_id, index_xid_a, reconcile_xid_a);
        uint64_t reconcile_xid_b = access_xid++;
        indexer->process_index_reconciliation(_db_id, index_xid_b, reconcile_xid_b);

        auto info_a = sys_tbl_mgr::Server::get_instance()->get_index_info(_db_id, index_id_a, {reconcile_xid_b, constant::MAX_LSN});
        ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(info_a.state()), sys_tbl::IndexNames::State::READY);
        auto info_b = sys_tbl_mgr::Server::get_instance()->get_index_info(_db_id, index_id_b, {reconcile_xid_b, constant::MAX_LSN});
        ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(info_b.state()), sys_tbl::IndexNames::State::READY);

        // Exactly one look-aside index exists for the table (a single file/root, shared by both
        // now-READY secondary indexes), and it correctly resolves every row for both of them.
        auto table = TableMgr::get_instance()->get_table(_db_id, table_id, reconcile_xid_b);
        ASSERT_NE(table->look_aside_index(), nullptr);

        uint64_t count_a = 0;
        for (auto it = table->begin(index_id_a); it != table->end(index_id_a); ++it) {
            ++count_a;
        }
        ASSERT_EQ(count_a, 5000u);

        uint64_t count_b = 0;
        for (auto it = table->begin(index_id_b); it != table->end(index_id_b); ++it) {
            ++count_b;
        }
        ASSERT_EQ(count_b, 5000u);

        queue_mgr->remove_queue(_db_id);
    }

    // Regression test for round-1 review finding 1: Indexer::_remove_index_key() used to erase
    // _look_aside_build_tracker[table_id] unconditionally, on *any* completing build for that
    // table, not just the one that owns the look-aside bootstrap. A non-owning build finishing
    // (and committing) before the owner finishes its own, longer bootstrap-scan-inclusive build
    // would erase the owner's still-in-flight ownership record, and a third "first index" build
    // arriving in that window would wrongly conclude it must start a second, independent
    // look-aside bootstrap, rather than discovering the first one -- exactly the corruption class
    // architecture.md Sec 5.4 case 2 and this phase's brief were about preventing.
    //
    // Reproduced deterministically here, rather than via timing-dependent concurrency (the
    // existing Test_ConcurrentFirstIndexBuildsShareLookAside's two real worker threads race for
    // ownership, but can't control *commit* ordering, so it doesn't reliably exercise this
    // specific bug -- see the round-1 review). A single-worker Indexer processes DDLs strictly
    // FIFO: the first DDL submitted on a table with no secondary index yet is *always* the
    // look-aside owner, and reconciliation (commit) is an explicit, test-driven step independent
    // of build order -- letting this test force exactly the order the bug needs (the non-owner,
    // B, commits before the owner, A) without depending on thread-scheduling luck. C's catalog
    // row and xid are reserved *before* B is reconciled (so a schema query pinned at C's xid
    // never observes B's later READY transition, exactly as if the Indexer had simply gotten to
    // building C sooner), while handing C's DDL to the Indexer is deliberately delayed until
    // after B commits -- that's the wall-clock race the bug needs, decoupled from xid order.
    // Final-state row-resolution alone would not catch this bug (A's and a spurious second
    // owner's bootstraps are independently complete over an unchanging table, so whichever one's
    // root wins the last update_roots() still resolves every row correctly) -- the assertion
    // that actually catches it is the look-aside file's on-disk size, which doubles if a second,
    // orphaned bootstrap ran.
    TEST_F(Indexer_Test, Test_LookAsideTrackerSurvivesNonOwnerCommit)
    {
        uint64_t table_id = _tid++;
        uint64_t index_id_a = _secondary_index_id + 10;
        uint64_t index_id_b = _secondary_index_id + 11;
        uint64_t index_id_c = _secondary_index_id + 12;
        uint64_t table_xid = access_xid++;

        create_table(_db_id, table_id, table_xid, "test_indexer_table_tracker_survives", _columns);
        _populate_table_with_data(table_id, table_xid, table_xid, 500, 5);

        auto queue_mgr = std::make_shared<springtail::pg_log_mgr::IndexReconciliationQueueManager>();
        queue_mgr->add_queue(_db_id);
        // A single worker: DDLs are processed strictly in submission order, so A (submitted
        // first, on a table with no secondary index yet) is deterministically the look-aside
        // owner, and B (submitted next, before A is reconciled) is deterministically the
        // non-owner that discovers A's tracker entry.
        auto indexer = std::make_unique<Indexer>(1, queue_mgr);

        // A: the look-aside owner. Build it, but don't reconcile (commit) it yet.
        uint64_t index_xid_a = access_xid++;
        auto index_request_a = create_index(_db_id, table_id, index_xid_a, index_id_a, "idx_tracker_a",
                std::vector<PgMsgSchemaColumn>(_columns.end() - 3, _columns.end() - 2), sys_tbl::IndexNames::State::NOT_READY);
        indexer->process_requests(_db_id, index_xid_a, {index_request_a});
        sys_tbl_mgr::Server::get_instance()->finalize(_db_id, index_xid_a, true);
        auto request_a = queue_mgr->pop(_db_id, constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
        ASSERT_NE(request_a, nullptr);

        // B: the non-owner, built (and discovers A's still-uncommitted tracker entry) while A is
        // still outstanding.
        uint64_t index_xid_b = access_xid++;
        auto index_request_b = create_index(_db_id, table_id, index_xid_b, index_id_b, "idx_tracker_b",
                std::vector<PgMsgSchemaColumn>(_columns.end() - 2, _columns.end() - 1), sys_tbl::IndexNames::State::NOT_READY);
        indexer->process_requests(_db_id, index_xid_b, {index_request_b});
        sys_tbl_mgr::Server::get_instance()->finalize(_db_id, index_xid_b, true);
        auto request_b = queue_mgr->pop(_db_id, constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
        ASSERT_NE(request_b, nullptr);

        // C: register the catalog row (NOT_READY) and reserve its xid *now*, in the same batch
        // as A/B and before B is reconciled -- mirroring a real CREATE INDEX statement that was
        // issued around the same time as A/B's, so a schema/roots query pinned at index_xid_c
        // must not see B's *later* READY transition (reconcile_xid_b, assigned below, is a later
        // xid than this). Only the *handoff to the Indexer* (process_requests(), further down)
        // is deliberately delayed until after B commits -- that's the wall-clock race the bug
        // needs, decoupled from xid order.
        uint64_t index_xid_c = access_xid++;
        auto index_request_c = create_index(_db_id, table_id, index_xid_c, index_id_c, "idx_tracker_c",
                std::vector<PgMsgSchemaColumn>(_columns.end() - 1, _columns.end()), sys_tbl::IndexNames::State::NOT_READY);

        // Commit the *non-owner* (B) first -- the exact ordering the bug needs. If
        // _remove_index_key() still erased the tracker unconditionally, this would wrongly clear
        // A's (the owner's) still-in-flight ownership record. Because B's READY transition lands
        // at reconcile_xid_b (a *later* xid than index_xid_c reserved above), C's own build --
        // querying the schema pinned at index_xid_c -- still sees the table as having no READY
        // secondary index yet, exactly as it would have if the Indexer had simply gotten around
        // to building C sooner.
        uint64_t reconcile_xid_b = access_xid++;
        indexer->process_index_reconciliation(_db_id, index_xid_b, reconcile_xid_b);

        // Now hand C's (already-registered, earlier-xid) DDL to the Indexer -- a third "first
        // index" build arriving in the window opened by B's commit. It must still discover A's
        // look-aside index via the surviving tracker entry, not start a second, independent one.
        indexer->process_requests(_db_id, index_xid_c, {index_request_c});
        sys_tbl_mgr::Server::get_instance()->finalize(_db_id, index_xid_c, true);
        auto request_c = queue_mgr->pop(_db_id, constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
        ASSERT_NE(request_c, nullptr);

        uint64_t reconcile_xid_c = access_xid++;
        indexer->process_index_reconciliation(_db_id, index_xid_c, reconcile_xid_c);

        // Finally commit the real owner, A.
        uint64_t reconcile_xid_a = access_xid++;
        indexer->process_index_reconciliation(_db_id, index_xid_a, reconcile_xid_a);

        // The decisive check for the bug itself: if _remove_index_key() still erased the tracker
        // unconditionally, C would have wrongly become a second owner and independently
        // bootstrap-scanned the whole table into its own look-aside tree -- which, because
        // nothing distinguishes its content from A's own (correct) bootstrap in this scenario,
        // is otherwise invisible to the row-resolution assertions below (whichever of A's/C's
        // tree wins the last update_roots() call is, on its own, equally complete). The
        // orphaned second tree's disk footprint is not: a single correct bootstrap of 500 rows
        // measures 8 blocks on this filesystem; a duplicated one measures 16. Assert comfortably
        // below double to catch the duplication without being sensitive to exact block counts.
        auto table_dir = TableMgr::get_instance()->get_table_data_dir(_db_id, table_id, reconcile_xid_a);
        ASSERT_TRUE(table_dir.has_value());
        auto look_aside_file = table_dir.value() / constant::INDEX_LOOK_ASIDE_FILE;
        ASSERT_LT(fs::get_block_count(look_aside_file), 12u)
            << "look-aside file is roughly double the size of a single bootstrap -- "
               "_look_aside_build_tracker was likely erased by a non-owner, letting a third "
               "build start a second, independent look-aside bootstrap";

        auto info_a = sys_tbl_mgr::Server::get_instance()->get_index_info(_db_id, index_id_a, {reconcile_xid_a, constant::MAX_LSN});
        ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(info_a.state()), sys_tbl::IndexNames::State::READY);
        auto info_b = sys_tbl_mgr::Server::get_instance()->get_index_info(_db_id, index_id_b, {reconcile_xid_a, constant::MAX_LSN});
        ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(info_b.state()), sys_tbl::IndexNames::State::READY);
        auto info_c = sys_tbl_mgr::Server::get_instance()->get_index_info(_db_id, index_id_c, {reconcile_xid_a, constant::MAX_LSN});
        ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(info_c.state()), sys_tbl::IndexNames::State::READY);

        // Exactly one look-aside index exists for the table, and it correctly resolves every row
        // for all three now-READY secondary indexes.
        auto table = TableMgr::get_instance()->get_table(_db_id, table_id, reconcile_xid_a);
        ASSERT_NE(table->look_aside_index(), nullptr);

        uint64_t count_a = 0;
        for (auto it = table->begin(index_id_a); it != table->end(index_id_a); ++it) {
            ++count_a;
        }
        ASSERT_EQ(count_a, 500u);

        uint64_t count_b = 0;
        for (auto it = table->begin(index_id_b); it != table->end(index_id_b); ++it) {
            ++count_b;
        }
        ASSERT_EQ(count_b, 500u);

        uint64_t count_c = 0;
        for (auto it = table->begin(index_id_c); it != table->end(index_id_c); ++it) {
            ++count_c;
        }
        ASSERT_EQ(count_c, 500u);

        queue_mgr->remove_queue(_db_id);
    }

    // btree redesign phase 11: a real GIN index build, driven through Indexer's actual
    // process_requests()/_build() pipeline (not a direct GinWriteStrategy unit test -- see
    // src/storage/test/test_gin_strategy.cc for that), reaches READY -- the dispatch point phase
    // 9 left as "not yet implemented" (originally covered by Test_GinDispatchNotImplemented,
    // retired by this phase the same way Test_GistGinDispatchNotImplemented's GIST half was
    // retired by phase 10) now has a real GinWriteStrategy-backed implementation behind it.
    //
    // Uses the real gin_trgm_ops opclass (not a reference opclass): confirmed directly (not
    // assumed), via src/storage/test/test_gin_strategy.cc's own RealOpclassManyRowsIsStable test
    // (150 rows, 10 consecutive runs), that GIN_EXTRACTVALUE's real pg_trgm implementation does
    // not reproduce phase 10's flagged GIST union/penalty instability -- see that test's comment
    // for the full characterization. This test itself stays small (2 rows, matching
    // Test_GistBuildReachesReady's own scale) since it's exercising the real Indexer pipeline
    // end-to-end, not opclass stability itself.
    TEST_F(Indexer_Test, Test_GinBuildReachesReady)
    {
        auto registry = PgExtnRegistry::get_instance();
        registry->init_libraries(_db_id, "gin_trgm_ops", "/usr/lib/postgresql/16/lib/pg_trgm.so");
        PgOpsClass opclass;
        opclass.name = "gin_trgm_ops";
        opclass.access_method = "gin";
        PgOpsClassMethod method;
        method.function_name = "gin_extract_value_trgm";
        method.support_number = GIN_EXTRACTVALUE;
        registry->add_opclass("gin_trgm_ops", opclass, method);

        uint64_t table_id = _tid++;
        uint64_t table_xid = access_xid++;

        constexpr int32_t text_pg_oid = 25; // postgres TEXTOID; named to avoid pg_type_d.h's own macro
        std::vector<PgMsgSchemaColumn> text_columns = {
            {"gin_id", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 1, 0, false, true, false},
            {"gin_val", static_cast<uint8_t>(SchemaType::TEXT), text_pg_oid, std::nullopt, 2, 0, true, false, false},
        };
        create_table(_db_id, table_id, table_xid, "test_indexer_gin_build_table", text_columns);

        uint64_t data_xid = access_xid++;
        auto mtable = TableMgr::get_instance()->get_mutable_table(_db_id, table_id, table_xid, data_xid);
        for (int i = 0; i < 2; ++i) {
            std::vector<ConstFieldPtr> fields;
            fields.push_back(std::make_shared<ConstTypeField<int32_t>>(i + 1));
            fields.push_back(std::make_shared<ConstTypeField<std::string>>(i == 0 ? "apple banana" : "banana cherry"));
            fields.push_back(std::make_shared<ConstTypeField<uint64_t>>(static_cast<uint64_t>(i + 1)));
            mtable->insert(std::make_shared<ValueTuple>(fields), constant::UNKNOWN_EXTENT);
        }
        auto &&metadata = mtable->finalize(true);
        sys_tbl_mgr::Server::get_instance()->update_roots(_db_id, table_id, data_xid, metadata);

        uint64_t gin_index_id = _secondary_index_id + 21;
        uint64_t index_xid = access_xid++;
        _create_index(table_id, gin_index_id, index_xid, "idx_gin_build", true, false, "gin",
                std::vector<PgMsgSchemaColumn>{ text_columns[1] }, { "gin_trgm_ops" });

        uint64_t reconcile_xid = access_xid++;
        _process_index_and_validate(gin_index_id, index_xid, reconcile_xid);

        auto info = sys_tbl_mgr::Server::get_instance()->get_index_info(_db_id, gin_index_id,
                {reconcile_xid, constant::MAX_LSN});
        ASSERT_EQ(info.index_type(), "gin");
        ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(info.state()), sys_tbl::IndexNames::State::READY);
    }

    // btree redesign phase 10: a real GIST index build, driven through Indexer's actual
    // process_requests()/_build() pipeline (not a direct GistWriter unit test -- see
    // src/storage/test/test_gist_strategy.cc for that), reaches READY -- the dispatch point
    // phase 9 left as "not yet implemented" (see Test_GistGinDispatchNotImplemented below) now
    // has a real GistWriter-backed implementation behind it.
    //
    // Deliberately 2 rows (never triggers a split): this phase's own test_gist_strategy.cc found
    // and extensively documented a real, not-fully-root-caused crash in pg_trgm.so's own compiled
    // gtrgm_union()/gtrgm_penalty() under multi-entry/split conditions (confirmed independent of
    // several real pg_ext bugs fixed along the way -- see phases/phase-10.md's Design notes).
    // This test exercises the real Indexer build pipeline for the confirmed-stable few-row case;
    // split/fan-out correctness is covered by test_gist_strategy.cc's own tests against a
    // pure-C++ reference opclass instead, specifically to avoid this flagged crash risk.
    TEST_F(Indexer_Test, Test_GistBuildReachesReady)
    {
        // Register pg_trgm's real gist_trgm_ops opclass support functions the same way
        // test_gist_strategy.cc does (dlopen + dlsym by known contrib/pg_trgm function names) --
        // this is a C++ unit test with no live FDW/primary connection of its own, unlike the real
        // daemon's PgCopyTable::init_pg_extn_registry() path.
        auto registry = PgExtnRegistry::get_instance();
        registry->init_libraries(_db_id, "gist_trgm_ops", "/usr/lib/postgresql/16/lib/pg_trgm.so");
        auto add_opclass_method = [&](int support_number, const std::string &function_name) {
            PgOpsClass opclass;
            opclass.name = "gist_trgm_ops";
            opclass.access_method = "gist";
            PgOpsClassMethod method;
            method.function_name = function_name;
            method.support_number = support_number;
            registry->add_opclass("gist_trgm_ops", opclass, method);
        };
        add_opclass_method(GIST_COMPRESS, "gtrgm_compress");
        add_opclass_method(GIST_UNION, "gtrgm_union");
        add_opclass_method(GIST_PENALTY, "gtrgm_penalty");
        add_opclass_method(GIST_PICKSPLIT, "gtrgm_picksplit");
        add_opclass_method(GIST_CONSISTENT, "gtrgm_consistent");

        uint64_t table_id = _tid++;
        uint64_t table_xid = access_xid++;

        constexpr int32_t text_pg_oid = 25; // postgres TEXTOID; named to avoid pg_type_d.h's own macro
        std::vector<PgMsgSchemaColumn> text_columns = {
            {"gist_id", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 1, 0, false, true, false},
            {"gist_val", static_cast<uint8_t>(SchemaType::TEXT), text_pg_oid, std::nullopt, 2, 0, true, false, false},
        };
        create_table(_db_id, table_id, table_xid, "test_indexer_gist_build_table", text_columns);

        uint64_t data_xid = access_xid++;
        auto mtable = TableMgr::get_instance()->get_mutable_table(_db_id, table_id, table_xid, data_xid);
        for (int i = 0; i < 2; ++i) {
            std::vector<ConstFieldPtr> fields;
            fields.push_back(std::make_shared<ConstTypeField<int32_t>>(i + 1));
            fields.push_back(std::make_shared<ConstTypeField<std::string>>(i == 0 ? "alpha" : "beta"));
            fields.push_back(std::make_shared<ConstTypeField<uint64_t>>(static_cast<uint64_t>(i + 1)));
            mtable->insert(std::make_shared<ValueTuple>(fields), constant::UNKNOWN_EXTENT);
        }
        auto &&metadata = mtable->finalize(true);
        sys_tbl_mgr::Server::get_instance()->update_roots(_db_id, table_id, data_xid, metadata);

        uint64_t gist_index_id = _secondary_index_id + 20;
        uint64_t index_xid = access_xid++;
        _create_index(table_id, gist_index_id, index_xid, "idx_gist_build", true, false, "gist",
                std::vector<PgMsgSchemaColumn>{ text_columns[1] }, { "gist_trgm_ops" });

        uint64_t reconcile_xid = access_xid++;
        _process_index_and_validate(gist_index_id, index_xid, reconcile_xid);

        auto info = sys_tbl_mgr::Server::get_instance()->get_index_info(_db_id, gist_index_id,
                {reconcile_xid, constant::MAX_LSN});
        ASSERT_EQ(info.index_type(), "gist");
        ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(info.state()), sys_tbl::IndexNames::State::READY);
    }

    // btree redesign final-2: phase 10/11 each named, and no later phase closed, the gap that a
    // READY GiST/GIN secondary index is populated once by Indexer's build (above) and never
    // touched again by ordinary per-row DML -- Test_VacuumSecondaryIndex (above) already relies
    // on exactly this working for a plain btree secondary index (DML after _process_index_and_
    // validate()); this is GIN's counterpart, checked against GinReadStrategy directly rather
    // than end-to-end. Uses gin_helpers::TEST_TERM_OPCLASS (whitespace-tokenizing, no dlopen) so
    // the test is self-contained and deterministic. Independently verified to fail without
    // final-2's mutable_table.cc fix (reverted locally and rerun): the post-DML lookups below
    // returned either stale build-time content or nothing at all, confirming the test exercises
    // the gap, not just coincidentally passing.
    TEST_F(Indexer_Test, Test_GinDmlMaintainsReadyIndex)
    {
        uint64_t table_id = _tid++;
        uint64_t table_xid = access_xid++;

        constexpr int32_t text_pg_oid = 25; // postgres TEXTOID; named to avoid pg_type_d.h's own macro
        std::vector<PgMsgSchemaColumn> text_columns = {
            {"gin_id", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 1, 0, false, true, false},
            {"gin_val", static_cast<uint8_t>(SchemaType::TEXT), text_pg_oid, std::nullopt, 2, 0, true, false, false},
        };
        create_table(_db_id, table_id, table_xid, "test_indexer_gin_dml_table", text_columns);

        auto insert_row = [&](MutableTablePtr mtable, int32_t id, const std::string &val, uint64_t internal_row_id) {
            std::vector<ConstFieldPtr> fields;
            fields.push_back(std::make_shared<ConstTypeField<int32_t>>(id));
            fields.push_back(std::make_shared<ConstTypeField<std::string>>(val));
            fields.push_back(std::make_shared<ConstTypeField<uint64_t>>(internal_row_id));
            mtable->insert(std::make_shared<ValueTuple>(fields), constant::UNKNOWN_EXTENT);
        };

        uint64_t data_xid = access_xid++;
        auto mtable = TableMgr::get_instance()->get_mutable_table(_db_id, table_id, table_xid, data_xid);
        insert_row(mtable, 1, "alpha one", 1);
        insert_row(mtable, 2, "alpha two", 2);
        auto &&metadata = mtable->finalize(true);
        sys_tbl_mgr::Server::get_instance()->update_roots(_db_id, table_id, data_xid, metadata);

        uint64_t gin_index_id = _secondary_index_id + 22;
        uint64_t index_xid = access_xid++;
        _create_index(table_id, gin_index_id, index_xid, "idx_gin_dml", true, false, "gin",
                std::vector<PgMsgSchemaColumn>{ text_columns[1] },
                { std::string(gin_helpers::TEST_TERM_OPCLASS) });

        uint64_t reconcile_xid = access_xid++;
        _process_index_and_validate(gin_index_id, index_xid, reconcile_xid);

        auto lookup = [&](uint64_t at_xid, const std::string &term) {
            auto table = TableMgr::get_instance()->get_table(_db_id, table_id, at_xid,
                    {PgExtnRegistry::get_instance()->comparator_func});
            auto gin = table->gin_index(gin_index_id);
            return gin->lookup(std::span<const char>(term.data(), term.size()));
        };

        // Sanity: the build-time rows are already in the index.
        {
            auto ids = lookup(reconcile_xid, "alpha");
            std::ranges::sort(ids);
            ASSERT_EQ(ids.size(), 2u);
            EXPECT_EQ(ids[0], 1u);
            EXPECT_EQ(ids[1], 2u);
        }

        // Ordinary DML after READY, via the same MutableTable path committer.cc uses -- never
        // Indexer. Within one commit batch: DELETE both rows carrying "alpha" (emptying that
        // term's posting list entirely -- gin_write_strategy.cc's own documented "entry row
        // simply stays removed" case), then INSERT a brand-new row that also contains "alpha"
        // (task 6 of this phase's brief: a later insert must resurrect the term correctly, not
        // collide with a half-removed entry).
        uint64_t post_xid = access_xid++;
        auto mtable2 = TableMgr::get_instance()->get_mutable_table(_db_id, table_id, reconcile_xid, post_xid);

        // DELETE row 1 by primary key.
        {
            std::vector<ConstFieldPtr> fields;
            fields.push_back(std::make_shared<ConstTypeField<int32_t>>(1));
            mtable2->remove(std::make_shared<ValueTuple>(fields), constant::UNKNOWN_EXTENT);
        }

        // UPDATE row 2 so its text no longer contains "alpha" but newly contains "gamma".
        {
            std::vector<ConstFieldPtr> fields;
            fields.push_back(std::make_shared<ConstTypeField<int32_t>>(2));
            fields.push_back(std::make_shared<ConstTypeField<std::string>>("gamma two"));
            fields.push_back(std::make_shared<ConstTypeField<uint64_t>>(0)); // ignored by update()
            mtable2->update(std::make_shared<ValueTuple>(fields), constant::UNKNOWN_EXTENT);
        }

        // INSERT a brand-new row that re-introduces "alpha".
        insert_row(mtable2, 3, "alpha three", 3);

        auto &&metadata2 = mtable2->finalize(true);
        sys_tbl_mgr::Server::get_instance()->update_roots(_db_id, table_id, post_xid, metadata2);

        // "alpha" no longer matches rows 1/2 (deleted/updated away) -- only row 3, the fresh
        // insert that resurrected the term after it had been fully emptied.
        {
            auto ids = lookup(post_xid, "alpha");
            ASSERT_EQ(ids.size(), 1u);
            EXPECT_EQ(ids[0], 3u);
        }

        // "gamma" (row 2's new content) and "two" (still row 2's) are both findable.
        EXPECT_EQ(lookup(post_xid, "gamma"), std::vector<uint64_t>{2u});

        // "one" (row 1's old content) is gone entirely -- row 1 was deleted, not just updated.
        EXPECT_TRUE(lookup(post_xid, "one").empty());

        // Reload from disk at a fresh xid (a brand-new Table/MutableTable instance, not the one
        // that performed the mutation) to confirm finalize()'s root capture (this phase's task 4)
        // actually persisted the post-DML state, not just an in-memory artifact of mtable2 itself.
        uint64_t reload_xid = access_xid++;
        EXPECT_EQ(lookup(reload_xid, "alpha"), std::vector<uint64_t>{3u});
    }

    // GiST counterpart of Test_GinDmlMaintainsReadyIndex, immediately above -- see its comment for
    // the full rationale. Uses gist_helpers::TEST_RANGE_OPCLASS (pure-C++, no dlopen) for the same
    // determinism reason Test_GistBuildReachesReady uses the real opclass only for a 2-row,
    // never-split build: this test's DML round would otherwise risk phase 10's documented
    // pg_trgm.so union/penalty instability under a growing tree for no added value here.
    // Independently verified to fail without final-2's mutable_table.cc fix (reverted locally and
    // rerun): the post-DML search() below still returned the deleted row and missed the inserted
    // one.
    TEST_F(Indexer_Test, Test_GistDmlMaintainsReadyIndex)
    {
        uint64_t table_id = _tid++;
        uint64_t table_xid = access_xid++;

        constexpr int32_t text_pg_oid = 25; // postgres TEXTOID; named to avoid pg_type_d.h's own macro
        std::vector<PgMsgSchemaColumn> text_columns = {
            {"gist_id", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 1, 0, false, true, false},
            {"gist_val", static_cast<uint8_t>(SchemaType::TEXT), text_pg_oid, std::nullopt, 2, 0, true, false, false},
        };
        create_table(_db_id, table_id, table_xid, "test_indexer_gist_dml_table", text_columns);

        auto insert_row = [&](MutableTablePtr mtable, int32_t id, const std::string &val, uint64_t internal_row_id) {
            std::vector<ConstFieldPtr> fields;
            fields.push_back(std::make_shared<ConstTypeField<int32_t>>(id));
            fields.push_back(std::make_shared<ConstTypeField<std::string>>(val));
            fields.push_back(std::make_shared<ConstTypeField<uint64_t>>(internal_row_id));
            mtable->insert(std::make_shared<ValueTuple>(fields), constant::UNKNOWN_EXTENT);
        };

        uint64_t data_xid = access_xid++;
        auto mtable = TableMgr::get_instance()->get_mutable_table(_db_id, table_id, table_xid, data_xid);
        insert_row(mtable, 1, "alpha", 1);
        insert_row(mtable, 2, "beta", 2);
        auto &&metadata = mtable->finalize(true);
        sys_tbl_mgr::Server::get_instance()->update_roots(_db_id, table_id, data_xid, metadata);

        uint64_t gist_index_id = _secondary_index_id + 23;
        uint64_t index_xid = access_xid++;
        _create_index(table_id, gist_index_id, index_xid, "idx_gist_dml", true, false, "gist",
                std::vector<PgMsgSchemaColumn>{ text_columns[1] },
                { std::string(gist_helpers::TEST_RANGE_OPCLASS) });

        uint64_t reconcile_xid = access_xid++;
        _process_index_and_validate(gist_index_id, index_xid, reconcile_xid);

        auto find = [&](uint64_t at_xid, const std::string &value) {
            auto table = TableMgr::get_instance()->get_table(_db_id, table_id, at_xid,
                    {PgExtnRegistry::get_instance()->comparator_func});
            auto gist = table->gist_index(gist_index_id);
            return gist->search(value, gist_helpers::TEST_RANGE_STRATEGY_EQ);
        };

        // Sanity: the build-time rows are already in the index.
        ASSERT_EQ(find(reconcile_xid, "alpha").size(), 1u);
        ASSERT_EQ(find(reconcile_xid, "beta").size(), 1u);

        // Ordinary DML after READY, via the same MutableTable path committer.cc uses -- never
        // Indexer: DELETE row 1 ("alpha"), UPDATE row 2 to a new value ("gamma", from "beta"),
        // INSERT a brand-new row ("delta").
        uint64_t post_xid = access_xid++;
        auto mtable2 = TableMgr::get_instance()->get_mutable_table(_db_id, table_id, reconcile_xid, post_xid);

        {
            std::vector<ConstFieldPtr> fields;
            fields.push_back(std::make_shared<ConstTypeField<int32_t>>(1));
            mtable2->remove(std::make_shared<ValueTuple>(fields), constant::UNKNOWN_EXTENT);
        }
        {
            std::vector<ConstFieldPtr> fields;
            fields.push_back(std::make_shared<ConstTypeField<int32_t>>(2));
            fields.push_back(std::make_shared<ConstTypeField<std::string>>("gamma"));
            fields.push_back(std::make_shared<ConstTypeField<uint64_t>>(0)); // ignored by update()
            mtable2->update(std::make_shared<ValueTuple>(fields), constant::UNKNOWN_EXTENT);
        }
        insert_row(mtable2, 3, "delta", 3);

        auto &&metadata2 = mtable2->finalize(true);
        sys_tbl_mgr::Server::get_instance()->update_roots(_db_id, table_id, post_xid, metadata2);

        // "alpha" (deleted row) is gone entirely.
        EXPECT_TRUE(find(post_xid, "alpha").empty());

        // "beta" (updated away) is gone entirely; "gamma" (its replacement) is findable under
        // row 2's internal_row_id, unchanged by the update.
        EXPECT_TRUE(find(post_xid, "beta").empty());
        {
            auto matches = find(post_xid, "gamma");
            ASSERT_EQ(matches.size(), 1u);
            EXPECT_EQ(matches[0].internal_row_id, 2u);
        }

        // "delta" (brand-new insert) is findable.
        {
            auto matches = find(post_xid, "delta");
            ASSERT_EQ(matches.size(), 1u);
            EXPECT_EQ(matches[0].internal_row_id, 3u);
        }

        // Reload from disk at a fresh xid to confirm finalize()'s root capture (this phase's
        // task 4) actually persisted the post-DML state.
        uint64_t reload_xid = access_xid++;
        {
            auto matches = find(reload_xid, "delta");
            ASSERT_EQ(matches.size(), 1u);
            EXPECT_EQ(matches[0].internal_row_id, 3u);
        }
    }

} // namespace
