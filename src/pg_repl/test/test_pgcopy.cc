#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/json.hh>
#include <common/properties.hh>
#include <common/logging.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>

#include <pg_repl/pg_copy_table.hh>

#include <storage/table.hh>
#include <storage/table_mgr.hh>

#include <sys_tbl_mgr/client.hh>
#include <xid_mgr/xid_mgr_client.hh>

#include <test/services.hh>

using namespace springtail;

namespace {

    class PgCopyTable_Test : public testing::Test {
    protected:
        std::filesystem::path _base_dir;

        void SetUp() override {
            springtail_init();

            _services.init(true);

            nlohmann::json db_config = Properties::get_db_config(db_id);
            auto db_name = db_config["name"].get<std::string>();

            auto p_db = Properties::get_primary_db_config();
            auto host = p_db["host"].get<std::string>();
            auto user = p_db["replication_user"].get<std::string>();
            auto password = p_db["password"].get<std::string>();
            auto port = p_db["port"].get<int>();

            std::string conn_cmd = fmt::format("psql postgresql://{}:{}@{}:{}/{} -f sample.sql", user, password, host, port, db_name);
            SPDLOG_INFO("Connecting to: {}", conn_cmd);
            int err = std::system(conn_cmd.c_str());
            if (err) {
                GTEST_SKIP() << "Postgres load failure, skipping test";
            }
        }

        void TearDown() override {
            _services.shutdown();
        }

        uint64_t db_id = 1;

        test::Services _services{true, true, true};
    };

    TEST_F(PgCopyTable_Test, CopyTable)
    {
        std::string table_name = "test_pgcopy";
        std::string schema_name = "public";

        uint64_t xid = XidMgrClient::get_instance()->get_committed_xid(db_id, 0);

        // perform the table copy
        std::vector<PgCopyResultPtr> res = PgCopyTable::copy_table(db_id, xid+1, schema_name, table_name);
        SPDLOG_DEBUG("Doing copy at: {}", xid+1);
        ASSERT_EQ(res.size(), 1);
        ASSERT_EQ(res[0]->tids.size(), 1);

        uint32_t oid = res[0]->tids[0];
        xid = res[0]->target_xid;

        // apply the system table changes
        auto client = sys_tbl_mgr::Client::get_instance();
        RedisQueue<std::string> sync_table_q(fmt::format(redis::QUEUE_SYNC_TABLE_OPS,
                                                         Properties::get_db_instance_id(), db_id));
        std::string worker_id = "test_worker";
        auto ops_str = sync_table_q.try_pop(worker_id);
        while (ops_str != nullptr) {
            auto json = nlohmann::json::parse(*ops_str);

            // perform the table swap
            // note: we wait to perform this operation in the GC-2 to ensure that all system
            //       table mutations up to this XID have already been applied, otherwise we
            //       could potentially get a stray column added before the swap XID showing
            //       up in the schema since it wouldn't get deleted by the DROP TABLE
            auto create = common::json_to_thrift<sys_tbl_mgr::TableRequest>(json[0]);
            create.xid = xid;
            create.lsn = constant::MAX_LSN - 1;

            auto roots = common::json_to_thrift<sys_tbl_mgr::UpdateRootsRequest>(json[1]);
            roots.xid = xid;

            client->swap_sync_table(create, roots);

            // get the next set of operations
            sync_table_q.commit(worker_id);
            ops_str = sync_table_q.try_pop(worker_id);
        }

        // finalize the system metadata
        client->finalize(db_id, xid);

        // commit the xid
        SPDLOG_DEBUG("Committing xid: {}", xid);
        XidMgrClient::get_instance()->commit_xid(db_id, xid, false);

        // create an access table
        auto table = TableMgr::get_instance()->get_table(db_id, oid, xid);
        auto schema = table->extent_schema();
        auto fields = schema->get_fields();

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        int count = 0;
        std::string prev = "";
        for (auto &row : *table) {
            std::cout << fields->at(1)->get_text(row) << std::endl;

            if (prev != "") {
                ASSERT_GT(fields->at(1)->get_text(row), prev);
            }

            prev = fields->at(1)->get_text(row);
            ++count;
        }
        ASSERT_EQ(count, 5000);
    }
}
