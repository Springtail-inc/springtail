#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/json.hh>
#include <common/properties.hh>

#include <pg_repl/pg_copy_table.hh>

#include <storage/table.hh>
#include <storage/table_mgr.hh>

using namespace springtail;

namespace {

    class PgCopyTable_Test : public testing::Test {
    protected:
        std::filesystem::path _base_dir;

        void SetUp() override {
            springtail_init();

            // get the base directory for table data
            nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
            Json::get_to<std::filesystem::path>(json, "table_dir", _base_dir,
                                                "/opt/springtail/table");

            // cleanup from failed previous run
            TearDown();

            std::filesystem::create_directories(_base_dir);

            std::cout << "writing to " << _base_dir << "\n";

            int err = std::system("psql postgresql://postgres:springtail@localhost -f sample.sql");
            if (err) {
                GTEST_SKIP() << "Postgres load failure, skipping test";
            }
        }

        void TearDown() override {
            // remove any files created during the run
            std::filesystem::remove_all(_base_dir);
        }

    };

    TEST_F(PgCopyTable_Test, CopyTable) {
        auto source = std::make_shared<PgCopyTable>("postgres", "public", "test_pgcopy", "");
        source->connect("localhost", "postgres", "springtail", 5432);

        // perform the table copy
        uint64_t xid = 2;
        auto oid = source->copy_to_springtail(_base_dir, xid);

        // create an access table
        auto table = TableMgr::get_instance()->get_table(oid, xid, 0);
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
