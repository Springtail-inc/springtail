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

using namespace springtail;
using namespace springtail::committer;

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

            _indexer = std::make_unique<Indexer>(1, std::make_shared<ConcurrentQueue<IndexReconcileRequest>>());
        }

        static void TearDownTestSuite() {
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

        static void
        _create_table(uint64_t db_id, uint64_t table_id, uint64_t xid, std::vector<PgMsgSchemaColumn> columns)
        {
            // create a table
            PgMsgTable create_msg;
            create_msg.lsn = 0;
            create_msg.oid = table_id;
            create_msg.xid = xid;
            create_msg.namespace_name = "public";
            create_msg.table = "test_table";
            create_msg.columns = columns;

            TableMgr::get_instance()->create_table(db_id, { xid, 0 }, create_msg);

        }

        static std::string
        _create_index(uint64_t db_id, uint64_t table_id, uint64_t xid, uint64_t index_id, std::vector<PgMsgSchemaColumn> columns)
        {

            PgMsgIndex msg;

            msg.lsn = 0;
            msg.xid = xid;
            msg.namespace_name = "public";
            msg.index = "secondary_index";
            msg.is_unique = false;
            msg.table_oid = table_id;
            msg.oid = index_id;

            int idx_position = 0;
            for(const auto& column: columns) {
                msg.columns.push_back({column.name, column.position, idx_position++});
            }

            XidLsn xid_lsn{xid};

            return sys_tbl_mgr::Client::get_instance()->create_index(db_id, xid_lsn, msg, sys_tbl::IndexNames::State::NOT_READY);

        }

        static void
        _drop_index(uint64_t db_id, uint32_t index_id, uint64_t xid)
        {
            PgMsgDropIndex msg;

            msg.lsn = 0;
            msg.xid = xid;
            msg.namespace_name = "public";
            msg.oid = index_id;

            XidLsn xid_lsn{xid};

            sys_tbl_mgr::Client::get_instance()->drop_index(db_id, xid_lsn, msg);

            sys_tbl_mgr::Client::get_instance()->finalize(db_id, xid);
        }

        std::shared_ptr<Tuple>
        _create_key(const std::string &name)
        {
            auto k = std::make_shared<ConstTypeField<std::string>>(name);
            std::vector<ConstFieldPtr> v({ k });
            return std::make_shared<ValueTuple>(v);
        }

        static std::shared_ptr<Tuple>
        _create_value(const std::vector<int32_t> &data)
        {
            std::vector<ConstFieldPtr> v;

            for (auto &d : data) {
                v.push_back(std::make_shared<ConstTypeField<int32_t>>(d));
            }

            return std::make_shared<ValueTuple>(v);
        }

        static void
        _populate_table(MutableTablePtr mtable)
        {
            // insert data to the tree
            for (int i = 0; i < _data.size(); i++) {
                mtable->insert(_create_value(_data[i]), constant::UNKNOWN_EXTENT);
            }
        }

    };

    TEST_F(Indexer_Test, Test_EmptyReconcile)
    {
        _columns = {
            {"col1", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 1, 0, false, true, false},
            {"col2", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 2, 1, false, true, false},
            {"col3", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 3, 2, false, true, false},
            {"col4", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 4, 0, false, false, false},
            {"col5", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 5, 0, false, false, false},
        };

        // create the table via the table mgr
        _create_table(_db_id, _tid, access_xid, _columns);
        access_xid++;
        target_xid++;

        // Create index at an XID
        uint64_t index_xid = access_xid;
        nlohmann::json idx_ddls;
        auto create_idx_ddl = _create_index(_db_id, _tid, access_xid, _secondary_index_id, std::vector<PgMsgSchemaColumn>(_columns.end() - 2, _columns.end()));
        access_xid++;
        target_xid++;
        idx_ddls.push_back(nlohmann::json::parse(create_idx_ddl));

        // Validate index as NOT_READY
        auto index_info = sys_tbl_mgr::Client::get_instance()->get_index_info(_db_id, _secondary_index_id, {index_xid, constant::MAX_LSN});
        ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(index_info.state()), sys_tbl::IndexNames::State::NOT_READY);

        // Process Index DDLs
        _indexer->process_ddls(_db_id, index_xid, idx_ddls);
        sys_tbl_mgr::Client::get_instance()->finalize(_db_id, access_xid);

        // Trigger index reconcilation at reconcile_xid
        uint64_t reconcile_xid = access_xid;
        _indexer->process_index_reconciliation(_db_id, index_xid, reconcile_xid);
        index_info = sys_tbl_mgr::Client::get_instance()->get_index_info(_db_id, _secondary_index_id, {reconcile_xid, constant::MAX_LSN});

        // Validate index as READY at reconcile_xid
        ASSERT_EQ(static_cast<sys_tbl::IndexNames::State>(index_info.state()), sys_tbl::IndexNames::State::READY);
    }

} // namespace
