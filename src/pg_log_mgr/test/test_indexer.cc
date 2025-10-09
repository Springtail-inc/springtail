#include <common/constants.hh>
#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/json.hh>
#include <common/properties.hh>
#include <common/filesystem.hh>

#include <pg_log_mgr/index_reconciliation_queue_manager.hh>
#include <pg_log_mgr/indexer.hh>
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
            auto &&metadata = mtable->finalize();
            sys_tbl_mgr::Server::get_instance()->update_roots(_db_id, table_id, data_xid, metadata);
        }

        void _create_index(uint64_t table_id, uint64_t index_id, uint64_t index_xid, std::string index_name,
                bool process_requests_in_indexer=true, bool is_unique=false) {
            // Create index at an XID
            std::list<proto::IndexProcessRequest> index_requests;
            auto create_idx_request = create_index(_db_id, table_id, index_xid, index_id, index_name,
                    std::vector<PgMsgSchemaColumn>(_columns.end() - 2, _columns.end()), sys_tbl::IndexNames::State::NOT_READY, is_unique);
            index_requests.push_back(std::move(create_idx_request));

            // Validate index as NOT_READY
            auto index_info = sys_tbl_mgr::Server::get_instance()->get_index_info(_db_id, index_id, {index_xid, constant::MAX_LSN});
            ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(index_info.state()), sys_tbl::IndexNames::State::NOT_READY);

            // Process Index requests
            if (process_requests_in_indexer) {
                _indexer->process_requests(_db_id, index_xid, index_requests);
            }
            sys_tbl_mgr::Server::get_instance()->finalize(_db_id, index_xid);
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

        xid_mgr::XidMgrServer::get_instance()->commit_xid(_db_id, 0, next_data_xid, true);

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

} // namespace
