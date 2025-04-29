#include <common/constants.hh>
#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/json.hh>
#include <common/properties.hh>

#include <pg_log_mgr/indexer.hh>
#include <sys_tbl_mgr/table.hh>
#include <sys_tbl_mgr/client.hh>

#include <pg_repl/pg_repl_msg.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table_mgr.hh>

#include <test/services.hh>
#include <test/ddl_helpers.hh>

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
            std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
            runners.emplace();
            runners->emplace_back(std::make_unique<IOMgrRunner>());

            auto service_runners = test::get_services(true, true, true);
            std::move(service_runners.begin(), service_runners.end(), std::back_inserter(runners.value()));
            springtail_init_test(runners);

            _db_id = 1;
            _tid = 1000;
            access_xid = 1, target_xid = 2;

            // create the public namespace
            PgMsgNamespace ns_msg;
            ns_msg.oid = 900;
            ns_msg.name = "public";
            sys_tbl_mgr::Client::get_instance()->create_namespace(_db_id, { access_xid, 0 }, ns_msg);
            access_xid++;
            target_xid++;

            _columns = {
                {"col1", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 1, 0, false, true, false},
                {"col2", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 2, 1, false, true, false},
                {"col3", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 3, 2, false, true, false},
                {"col4", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 4, 0, false, false, false},
                {"col5", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 5, 0, false, false, false},
            };

            _indexer = std::make_unique<Indexer>(1, std::make_shared<ConcurrentQueue<IndexReconcileRequest>>());
        }

        static void TearDownTestSuite() {
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

        void _populate_table_with_data(uint64_t table_id, uint64_t table_xid,
                uint64_t data_xid, int num_rows, int num_cols, int start_value = 0) {
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
            populate_table(mtable, _data);

            // finalize the table and update roots
            auto &&metadata = mtable->finalize();
            TableMgr::get_instance()->update_roots(_db_id, table_id, data_xid, metadata);
        }

        void _create_index(uint64_t table_id, uint64_t index_id, uint64_t index_xid, std::string index_name) {
            // Create index at an XID
            nlohmann::json idx_ddls;
            auto create_idx_ddl = create_index(_db_id, table_id, index_xid, index_id, index_name,
                    std::vector<PgMsgSchemaColumn>(_columns.end() - 2, _columns.end()), sys_tbl::IndexNames::State::NOT_READY);
            idx_ddls.push_back(nlohmann::json::parse(create_idx_ddl));

            // Validate index as NOT_READY
            auto index_info = sys_tbl_mgr::Client::get_instance()->get_index_info(_db_id, index_id, {index_xid, constant::MAX_LSN});
            ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(index_info.state()), sys_tbl::IndexNames::State::NOT_READY);

            // Process Index DDLs
            _indexer->process_ddls(_db_id, index_xid, idx_ddls);
            sys_tbl_mgr::Client::get_instance()->finalize(_db_id, index_xid);
        }

        void _process_index_and_validate(uint64_t index_id, uint64_t index_xid, uint64_t reconcile_xid) {
            // Trigger index reconcilation at reconcile_xid
            _indexer->process_index_reconciliation(_db_id, index_xid, reconcile_xid);
            auto index_info = sys_tbl_mgr::Client::get_instance()->get_index_info(_db_id, index_id, {reconcile_xid, constant::MAX_LSN});

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
} // namespace
