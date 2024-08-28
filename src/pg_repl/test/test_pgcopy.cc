#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/json.hh>
#include <common/properties.hh>
#include <common/logging.hh>

#include <pg_repl/pg_copy_table.hh>

#include <storage/table.hh>
#include <storage/table_mgr.hh>

#include <test/services.hh>

using namespace springtail;

namespace {

    class PgCopyTable_Test : public testing::Test {
    protected:
        std::filesystem::path _base_dir;

        void SetUp() override {
            springtail_init();

            _services.init(true);

            auto p_db = Properties::get_primary_db_config();

            host = p_db["host"].get<std::string>();
            user = p_db["replication_user"].get<std::string>();
            password = p_db["password"].get<std::string>();
            port = p_db["port"].get<int>();

            std::string conn_cmd = fmt::format("psql {}://{}:{}@{}:{} -f sample.sql", db_name, user, password, host, port);
            SPDLOG_INFO("Connecting to: {}", conn_cmd);
            int err = std::system(conn_cmd.c_str());
            if (err) {
                GTEST_SKIP() << "Postgres load failure, skipping test";
            }
        }

        void TearDown() override {
            _services.shutdown();
        }

        std::string db_name = "postgres";
        std::string host;
        std::string user;
        std::string password;
        int port;

        test::Services _services{true, true, true};
    };

    TEST_F(PgCopyTable_Test, CopyTable) {
        auto source = std::make_shared<PgCopyTable>(db_name, "public", "test_pgcopy", "");
        source->connect(host, user, password, port);

        // perform the table copy
        XidLsn xid(2, 0);
        uint64_t db_id = 1;
        auto oid = source->copy_to_springtail(db_id, xid);

        // create an access table
        auto table = TableMgr::get_instance()->get_table(db_id, oid, xid.xid, 0);
        auto schema = table->extent_schema();
        auto fields = schema->get_fields();

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        int count = 0;
        std::string prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(fields->at(1)->get_text(row), prev);
            }

            prev = fields->at(1)->get_text(row);
            ++count;
        }
        ASSERT_EQ(count, 5000);
    }
}
