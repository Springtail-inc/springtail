#include <pg_repl/pg_stream_table.hh>
#include <storage/table.hh>
#include <storage/table_mgr.hh>
#include <gtest/gtest.h>
#include <common/common.hh>

#include <ingest/ingest.hh>

using namespace springtail;

namespace {

    class Ingest_Test : public testing::Test {
    protected:
        std::filesystem::path _base_dir;
        std::shared_ptr<ExtentCache> _read_cache;

        void SetUp() override {
            springtail_init();

            _base_dir = std::filesystem::temp_directory_path() / "test_ingest";

            system("psql postgresql://username:password@localhost/springtail_test -f ./sample.sql");
            _read_cache = std::make_shared<LruObjectCache<std::pair<std::filesystem::path, uint64_t>, Extent>>(1024*1024);
        }

        void TearDown() override {
            // remove any files created during the run
            std::filesystem::remove_all(_base_dir);
        }

    };

    TEST_F(Ingest_Test, Write) {
        auto source = std::make_shared<PgStreamTable>("springtail_test", "public", "test_ingest");
        source->connect("localhost", "username", "password", 5432);
        auto ingest = std::make_shared<Ingest>(source, _base_dir);

        auto tbl_mgr = TableMgr::get_instance();

        // create an access table
        TablePtr table = tbl_mgr->get_table(1, 0, 0);
        auto fields = table->extent_schema()->get_fields();

        // auto table = std::make_shared<Table>(1001,
        //                                      table_info->id(),
        //                                      _base_dir / "1001",
        //                                      table_info->primary_key(),
        //                                      std::vector<std::vector<std::string>>(),
        //                                      0,
        //                                      table_info->extent_schema(),
        //                                      _read_cache);

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
