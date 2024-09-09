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

        // perform the table copy
        PgCopyResultPtr res = PgCopyTable::copy_table(db_id, schema_name, table_name);
        ASSERT_EQ(res->tids.size(), 1);

        uint32_t oid = res->tids[0];
        uint64_t xid = res->target_xid;

        // create an access table
        auto table = TableMgr::get_instance()->get_table(db_id, oid, xid, 0);
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
