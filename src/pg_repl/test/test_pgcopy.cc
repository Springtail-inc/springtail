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

            std::filesystem::create_directories(_base_dir);

            std::cout << "writing to " << _base_dir << "\n";

            int err = std::system("psql postgresql://username:password@localhost/springtail_test -f sample.sql");
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
        auto source = std::make_shared<PgCopyTable>("springtail_test", "public", "test_pgcopy", "");
        source->connect("localhost", "username", "password", 5432);

        // perform the table copy
        auto oid = source->copy_to_springtail(_base_dir, 1);

        // create an access table
        auto table = TableMgr::get_instance()->get_table(oid, 1, 0);
        auto schema = table->extent_schema();

        // auto schema = SchemaMgr::get_instance()->get_extent_schema(oid, 1);
        // auto table = std::make_shared<Table>(oid,
        //                                      1,
        //                                      _base_dir / std::to_string(oid),
        //                                      schema->get_sort_keys(),
        //                                      std::vector<std::vector<std::string>>{},
        //                                      std::vector<uint64_t>{ constant::UNKNOWN_EXTENT },
        //                                      schema);
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
