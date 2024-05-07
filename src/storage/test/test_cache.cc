#include <gtest/gtest.h>

#include <common/common.hh>

#include <storage/cache.hh>
#include <storage/csv_field.hh>
#include <storage/table_mgr.hh>

using namespace springtail;

namespace {
    /**
     * Framework for Extent testing.
     */
    class StorageCache_Test : public testing::Test {
    protected:
        static void SetUpTestSuite() {
            _cleanup_files();
        }

        static void TearDownTestSuite() {
            _cleanup_files();
        }

        void SetUp() override {
            springtail_init();

            // construct a schema for testing
            std::vector<SchemaColumn> columns({
                    { "table_id", 0, SchemaType::INT64, false },
                    { "name", 1, SchemaType::TEXT, false, 0 },
                    { "offset", 2, SchemaType::INT64, false, 1 }
                });
            _schema = std::make_shared<ExtentSchema>(columns);

            std::vector<SchemaColumn> columns2({
                    { "table_id", 0, SchemaType::INT64, false },
                    { "name", 1, SchemaType::TEXT, false, 0 },
                    { "offset", 2, SchemaType::INT64, false },
                    { "index", 3, SchemaType::INT16, false, 1 }
                });
            _schema2 = std::make_shared<ExtentSchema>(columns2);

            _fields = _schema->get_fields();
            _csv_fields = std::make_shared<FieldArray>();
            for (int i = 0; i < _fields->size(); i++) {
                auto &&field = _fields->at(i);
                _csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
            }
        }

        void TearDown() override {

        }

        static void _cleanup_files() {
            std::filesystem::remove("/tmp/test_cache_Basic");
            std::filesystem::remove("/tmp/test_cache_Insert50K");
        }

        ExtentSchemaPtr _schema, _schema2;
        FieldArrayPtr _fields, _csv_fields;
    };

    TEST_F(StorageCache_Test, Basic) {
        auto cache = StorageCache::get_instance();
        std::filesystem::path file("/tmp/test_cache_Basic");
        uint64_t xid = 1;

        // get() an empty Page
        auto page = cache->get(file, constant::UNKNOWN_EXTENT, xid);

        // populate data into the Page
        csv::CSVReader reader("test_btree_simple.csv");
        for (auto &r : reader) {
            page->insert(std::make_shared<FieldTuple>(_csv_fields, r), _schema);
        }

        auto &&offsets = page->flush(xid++, ExtentType());

        // put() the mutated Page
        cache->put(page);
        page = nullptr;

        // verify the contents
        int count = 0;
        std::string prev = "";
        for (auto offset : offsets) {
            page = cache->get(file, offset, xid);

            for (auto row : (*page)) {
                if (prev != "") {
                    ASSERT_GT(_fields->at(1)->get_text(row), prev);
                }

                prev = _fields->at(1)->get_text(row);
                ++count;
            }

            cache->put(page);
        }
        ASSERT_EQ(count, 5000);
    }

    TEST_F(StorageCache_Test, Insert50K) {
        auto cache = StorageCache::get_instance();
        std::filesystem::path file("/tmp/test_cache_Insert50K");
        uint64_t xid = 1;

        // get() an empty Page
        auto page = cache->get(file, constant::UNKNOWN_EXTENT, xid);

        // populate data into the Page
        for (int i = 0; i < 10; i++) {
            csv::CSVReader reader("test_btree_simple.csv");
            for (auto &&r : reader) {
                auto extra = std::make_shared<FieldArray>();
                extra->push_back(std::make_shared<ConstTypeField<int16_t>>(i));

                page->insert(std::make_shared<KeyValueTuple>(_csv_fields, extra, r), _schema2);
            }
        }

        auto &&offsets = page->flush(xid++, ExtentType());

        // put() the mutated Page
        cache->put(page);
        page = nullptr;

        // verify the contents
        int count = 0;
        std::string prev = "";
        for (auto offset : offsets) {
            page = cache->get(file, offset, xid);

            for (auto row : (*page)) {
                if (prev != "") {
                    ASSERT_GE(_fields->at(1)->get_text(row), prev);
                }

                prev = _fields->at(1)->get_text(row);
                ++count;
            }

            cache->put(page);
        }
        ASSERT_EQ(count, 50000);
    }
}
