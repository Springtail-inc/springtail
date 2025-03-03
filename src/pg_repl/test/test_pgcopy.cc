#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/redis_types.hh>
#include <common/redis.hh>
#include <pg_repl/pg_copy_table.hh>
#include <proto/pg_copy_table.pb.h>
#include <redis/redis_containers.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/table.hh>
#include <test/services.hh>
#include <xid_mgr/xid_mgr_client.hh>

using namespace springtail;

namespace {

    class PgCopyTable_Test : public testing::Test {
    protected:
        std::filesystem::path _base_dir;

        static void SetUpTestSuite() {
            std::vector<std::unique_ptr<ServiceRunner>> service_runners = test::get_services(true, true, true);
            std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
            runners.emplace();
            std::move(service_runners.begin(), service_runners.end(), std::back_inserter(runners.value()));

            springtail_init_test(runners);

            // create the public namespace
            auto client = sys_tbl_mgr::Client::get_instance();
            auto xid_client = XidMgrClient::get_instance();
            uint64_t target_xid = xid_client->get_committed_xid(1, 0) + 1;

            // create the public namespace in the sys_tbl_mgr
            PgMsgNamespace ns_msg;
            ns_msg.oid = 90000;
            ns_msg.name = "public";
            client->create_namespace(1, XidLsn(target_xid, 0), ns_msg);

            xid_client->commit_xid(1, target_xid, false);
        }

        static void TearDownTestSuite() {
            springtail_shutdown();
        }

        void SetUp() override {
            nlohmann::json db_config = Properties::get_db_config(db_id);
            auto db_name = db_config["name"].get<std::string>();

            std::string host, user, password;
            int port;
            Properties::get_primary_db_config(host, port, user, password);

            std::string conn_cmd = fmt::format("psql postgresql://{}:{}@{}:{}/{} -f sample.sql", user, password, host, port, db_name);
            SPDLOG_INFO("Connecting to: {}", conn_cmd);
            int err = std::system(conn_cmd.c_str());
            if (err) {
                GTEST_SKIP() << "Postgres load failure, skipping test";
            }
        }

        uint64_t db_id = 1;
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

        auto redis = RedisMgr::get_instance()->get_client();
        std::string key = fmt::format(redis::HASH_SYNC_TABLE_OPS,
                                      Properties::get_db_instance_id(), db_id);
        std::vector<std::string> hkeys;
        redis->hkeys(key, std::back_inserter(hkeys));

        for (const std::string &hkey : hkeys) {
            auto &&value = redis->hget(key, hkey);
            proto::CopyTableInfo copy_info;
            if (!copy_info.ParseFromString(*value)) {
                throw Error("Failed to parse CopyTableInfo from string");
            }

            // perform the table swap
            // note: we wait to perform this operation in the GC-2 to ensure that all system
            //       table mutations up to this XID have already been applied, otherwise we
            //       could potentially get a stray column added before the swap XID showing
            //       up in the schema since it wouldn't get deleted by the DROP TABLE
            auto* namespace_req = copy_info.mutable_namespace_req();
            namespace_req->set_xid(xid);
            namespace_req->set_lsn(constant::MAX_LSN - 2);

            auto* create_req = copy_info.mutable_table_req();
            create_req->set_xid(xid);
            create_req->set_lsn(constant::MAX_LSN - 1);

            auto *indexes = copy_info.mutable_index_reqs();
            std::vector<proto::IndexRequest> index_reqs;
            for (auto &index : *indexes) {
                index_reqs.push_back(index);
            }
            auto *roots_req = copy_info.mutable_roots_req();
            roots_req->set_xid(xid);

            // Perform the table swap using the updated copy_info
            client->swap_sync_table(*namespace_req, *create_req, index_reqs, *roots_req);

            // clear the table entry from the hash
            redis->hdel(key, hkey);
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

        // verify stats
        auto &&metadata = client->get_roots(db_id, oid, xid);
        ASSERT_EQ(metadata->stats.row_count, 5000);

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
