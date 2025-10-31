#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/environment.hh>

#include <storage/cache.hh>
#include <storage/csv_field.hh>

#include <sys_tbl_mgr/table_mgr.hh>
#include <test/services.hh>
#include "common/constants.hh"

using namespace springtail;

namespace {
    struct CacheSize {
        uint64_t data_cache_size;
        uint64_t page_cache_size;
        uint64_t btree_cache_size;
        uint64_t max_extent_per_page;
    };

    void PrintTo(const CacheSize& cacheSize, std::ostream* os) {
        // Customize the output here as needed
        if (cacheSize.data_cache_size < 128) {
            *os << "small_cache";
        } else {
            *os << "large_cache";
        }
    }

    /**
     * Framework for Extent testing.
     */
    class StorageCache_Test : public testing::TestWithParam<CacheSize> {
    protected:
        void SetUp() override {
            // set the cache size from the parameters
            CacheSize sizes = GetParam();
            std::string overrides = std::format("storage.data_cache_size={};storage.page_cache_size={};storage.btree_cache_size={};storage.max_extent_per_page={}",
                                                sizes.data_cache_size, sizes.page_cache_size, sizes.btree_cache_size, sizes.max_extent_per_page);
            ::setenv(environment::ENV_OVERRIDE, overrides.c_str(), 1);

            springtail_init_test(LOG_ALL ^ LOG_STORAGE);
            test::start_services(true, true, false);

            // construct a schema for testing
            std::vector<SchemaColumn> columns({
                    { "table_id", 0, SchemaType::INT64, 0, false },
                    { "name", 1, SchemaType::TEXT, 0, false, 0 },
                    { "offset", 2, SchemaType::INT64, 0, false, 1 },
                    { "index", 3, SchemaType::INT16, 0, false, 2 }
                });
            _schema = std::make_shared<ExtentSchema>(columns, false, false);

            _fields = _schema->get_fields();
            _csv_fields = std::make_shared<FieldArray>();
            for (int i = 0; i < _fields->size() - 1; i++) {
                auto &&field = _fields->at(i);
                _csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
            }

            _base_dir = std::filesystem::temp_directory_path() / "test_cache";
            std::filesystem::remove_all(_base_dir);
            std::filesystem::create_directories(_base_dir);

        }

        void TearDown() override {
            std::filesystem::remove_all(_base_dir);
            springtail_shutdown();
        }

        ExtentSchemaPtr _schema;
        FieldArrayPtr _fields, _csv_fields;
        std::filesystem::path _base_dir;
    };

    TEST_P(StorageCache_Test, Basic) {
        auto cache = StorageCache::get_instance();
        std::filesystem::path file(_base_dir / "Basic");
        uint64_t xid = 1;

        // get() an empty Page
        auto page = cache->get(0, file, constant::UNKNOWN_EXTENT, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);

        // populate data into the Page
        csv::CSVReader reader("test_btree_simple.csv");
        for (auto &r : reader) {
            auto extra = std::make_shared<FieldArray>();
            extra->push_back(std::make_shared<ConstTypeField<int16_t>>(0));

            page->insert(std::make_shared<KeyValueTuple>(_csv_fields, extra, &r), _schema);
        }

        // test operator +=(difference_type)
        {
            ASSERT_GT(page->extent_count(), 1);
            std::string diff_test_txt;
            int count = 0;
            for (auto it = page->begin(); it != page->end(); ++it, ++count) {
                if (count == 4500) {
                    diff_test_txt = _fields->at(1)->get_text(&*it);
                    break;
                }
            }

            auto it = page->begin();
            it += 4500;
            auto txt = _fields->at(1)->get_text(&*it);

            ASSERT_EQ(txt, diff_test_txt);
        }

        ExtentHeader header(ExtentType(), xid++, _schema->row_size(), _schema->field_types(), 0);
        auto &&offsets = page->flush(header);

        // verify the contents
        int count = 0;
        std::string prev = "";
        for (auto offset : offsets) {
            page = cache->get(0, file, offset, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);

            for (auto row : *page.ptr()) {
                if (prev != "") {
                    ASSERT_GT(_fields->at(1)->get_text(&row), prev);
                }

                prev = _fields->at(1)->get_text(&row);
                ++count;
            }
        }
        ASSERT_EQ(count, 5000);
    }

    TEST_P(StorageCache_Test, Insert50K) {
        auto cache = StorageCache::get_instance();
        std::filesystem::path file(_base_dir / "Insert50K");
        uint64_t xid = 1;

        std::vector<uint64_t> offsets;

        // get() an empty Page
        {
            auto page = cache->get(0, file, constant::UNKNOWN_EXTENT, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);

            // populate data into the Page
            for (int i = 0; i < 10; i++) {
                csv::CSVReader reader("test_btree_simple.csv");
                for (auto &&r : reader) {
                    auto extra = std::make_shared<FieldArray>();
                    extra->push_back(std::make_shared<ConstTypeField<int16_t>>(i));

                    page->insert(std::make_shared<KeyValueTuple>(_csv_fields, extra, &r), _schema);
                    (void)cache->validate();
                }
            }

            ExtentHeader header(ExtentType(), xid++, _schema->row_size(), _schema->field_types(), 0);
            offsets = page->flush(header);
        }

        // verify the contents
        int count = 0;
        std::string prev = "";
        for (auto offset : offsets) {
            auto page = cache->get(0, file, offset, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);

            for (auto row : *page.ptr()) {
                if (prev != "") {
                    ASSERT_GE(_fields->at(1)->get_text(&row), prev);
                }

                prev = _fields->at(1)->get_text(&row);
                ++count;
            }
        }
        ASSERT_EQ(count, 50000);
    }

    TEST_P(StorageCache_Test, EvictForDatabase) {
        auto cache = StorageCache::get_instance();
        uint64_t xid = 1;

        // Setup: Create pages for two different databases
        std::filesystem::path db1_file1(_base_dir / "db1_file1");
        std::filesystem::path db1_file2(_base_dir / "db1_file2");
        std::filesystem::path db2_file1(_base_dir / "db2_file1");
        std::filesystem::path db2_file2(_base_dir / "db2_file2");

        std::vector<uint64_t> db1_f1_offsets, db1_f2_offsets;
        std::vector<uint64_t> db2_f1_offsets, db2_f2_offsets;

        // Create and populate pages for database 1
        {
            auto page1 = cache->get(1, db1_file1, constant::UNKNOWN_EXTENT, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);
            auto page2 = cache->get(1, db1_file2, constant::UNKNOWN_EXTENT, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);

            csv::CSVReader reader1("test_btree_simple.csv");
            for (auto &r : reader1) {
                auto extra = std::make_shared<FieldArray>();
                extra->push_back(std::make_shared<ConstTypeField<int16_t>>(1));
                page1->insert(std::make_shared<KeyValueTuple>(_csv_fields, extra, &r), _schema);
            }

            csv::CSVReader reader2("test_btree_simple.csv");
            for (auto &r : reader2) {
                auto extra = std::make_shared<FieldArray>();
                extra->push_back(std::make_shared<ConstTypeField<int16_t>>(2));
                page2->insert(std::make_shared<KeyValueTuple>(_csv_fields, extra, &r), _schema);
            }

            ExtentHeader header(ExtentType(), xid, _schema->row_size(), _schema->field_types(), 0);
            db1_f1_offsets = page1->flush(header);
            db1_f2_offsets = page2->flush(header);
        }

        xid++;

        // Create and populate pages for database 2
        {
            auto page1 = cache->get(2, db2_file1, constant::UNKNOWN_EXTENT, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);
            auto page2 = cache->get(2, db2_file2, constant::UNKNOWN_EXTENT, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);

            csv::CSVReader reader1("test_btree_simple.csv");
            for (auto &r : reader1) {
                auto extra = std::make_shared<FieldArray>();
                extra->push_back(std::make_shared<ConstTypeField<int16_t>>(3));
                page1->insert(std::make_shared<KeyValueTuple>(_csv_fields, extra, &r), _schema);
            }

            csv::CSVReader reader2("test_btree_simple.csv");
            for (auto &r : reader2) {
                auto extra = std::make_shared<FieldArray>();
                extra->push_back(std::make_shared<ConstTypeField<int16_t>>(4));
                page2->insert(std::make_shared<KeyValueTuple>(_csv_fields, extra, &r), _schema);
            }

            ExtentHeader header(ExtentType(), xid, _schema->row_size(), _schema->field_types(), 0);
            db2_f1_offsets = page1->flush(header);
            db2_f2_offsets = page2->flush(header);
        }

        xid++;

        // Pre-eviction verification: All pages should be accessible
        {
            int count = 0;
            for (auto offset : db1_f1_offsets) {
                auto page = cache->get(1, db1_file1, offset, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);
                for (auto row : *page.ptr()) {
                    ASSERT_EQ(_fields->at(3)->get_int16(&row), 1);
                    ++count;
                }
            }
            ASSERT_EQ(count, 5000);

            count = 0;
            for (auto offset : db2_f1_offsets) {
                auto page = cache->get(2, db2_file1, offset, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);
                for (auto row : *page.ptr()) {
                    ASSERT_EQ(_fields->at(3)->get_int16(&row), 3);
                    ++count;
                }
            }
            ASSERT_EQ(count, 5000);

            // Validate and check database counts
            auto db_counts = cache->validate();
            ASSERT_GT(db_counts[1], 0);
            ASSERT_GT(db_counts[2], 0);
        }

        // Evict database 1
        cache->evict_for_database(1);

        // Post-eviction verification
        {
            // Validate that database 1 pages are gone and database 2 pages remain
            auto db_counts = cache->validate();
            ASSERT_EQ(db_counts[1], 0);  // Database 1 should have 0 pages
            ASSERT_GT(db_counts[2], 0);  // Database 2 should still have pages

            // Database 2 pages should still be accessible with correct data
            int count = 0;
            for (auto offset : db2_f1_offsets) {
                auto page = cache->get(2, db2_file1, offset, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);
                for (auto row : *page.ptr()) {
                    ASSERT_EQ(_fields->at(3)->get_int16(&row), 3);
                    ++count;
                }
            }
            ASSERT_EQ(count, 5000);

            count = 0;
            for (auto offset : db2_f2_offsets) {
                auto page = cache->get(2, db2_file2, offset, xid, constant::LATEST_XID, constant::MAX_EXTENT_SIZE);
                for (auto row : *page.ptr()) {
                    ASSERT_EQ(_fields->at(3)->get_int16(&row), 4);
                    ++count;
                }
            }
            ASSERT_EQ(count, 5000);

            db_counts = cache->validate();
            ASSERT_GT(db_counts[2], 0);
        }

        // Test edge cases
        {
            // Evicting database 1 again should be a no-op
            cache->evict_for_database(1);
            auto db_counts = cache->validate();
            ASSERT_EQ(db_counts[1], 0);

            // Evicting non-existent database should be a no-op
            cache->evict_for_database(999);
            db_counts = cache->validate();
            ASSERT_EQ(db_counts[999], 0);
        }
    }

    INSTANTIATE_TEST_CASE_P(StorageCache_Test,
                            StorageCache_Test,
                            ::testing::Values(CacheSize{ 16384, 16384, 512, 16 },
                                              CacheSize{ 32, 32, 2, 4 }));
}
