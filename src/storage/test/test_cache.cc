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
        void SetUp() override {
            std::filesystem::remove_all("/tmp/springtail/table");

            springtail_init();

            // construct a schema for testing
            std::vector<SchemaColumn> columns({
                    { "table_id", 0, SchemaType::UINT64, false },
                    { "name", 1, SchemaType::TEXT, false },
                    { "offset", 2, SchemaType::UINT64, false }
                });
            _schema = std::make_shared<ExtentSchema>(columns);

            _fields = _schema->get_fields();
            _csv_fields = std::make_shared<FieldArray>();
            for (int i = 0; i < _fields->size(); i++) {
                auto &&field = _fields->at(i);
                _csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
            }
        }

        void TearDown() override {
            std::filesystem::remove_all("/tmp/springtail/table");
        }

        ExtentSchemaPtr _schema;
        FieldArrayPtr _fields, _csv_fields;
    };

    TEST_F(StorageCache_Test, Basic) {
        auto cache = StorageCache::get_instance();

        // XID / LSN for operations
        uint64_t xid = 1;
        uint64_t lsn = 0;
        uint32_t table_id = 100000;
        std::filesystem::path file(fmt::format("/tmp/springtail/table/{}/raw", table_id));

        // create a table
        PgMsgTable create_msg;
        create_msg.lsn = 0;
        create_msg.oid = table_id;
        create_msg.xid = xid++;
        create_msg.schema = "public";
        create_msg.table = "test";
        create_msg.columns.push_back({"table_id", "int8", std::nullopt, 0, 0, true, false, false});
        create_msg.columns.push_back({"name", "text", std::nullopt, 1, 0, false, true, false});
        create_msg.columns.push_back({"offset", "int8", "0", 1, 0, false, false, false});

        TableMgr::get_instance()->create_table(xid, lsn, create_msg);

        // get() an empty Page
        auto page = cache->get(file, constant::UNKNOWN_EXTENT, table_id, xid);

        // populate data into the Page
        csv::CSVReader reader("test_btree_simple.csv");
        for (auto &&r : reader) {
            page->insert(std::make_shared<FieldTuple>(_csv_fields, r));
        }

        auto &&offsets = page->flush(xid++, ExtentType(), table_id, constant::INDEX_DATA);

        // put() the mutated Page
        cache->put(page);
        page = nullptr;

        // verify the contents
        int count = 0;
        std::string prev = "";
        for (auto offset : offsets) {
            page = cache->get(file, offset, table_id, xid);

            for (auto row : (*page)) {
                if (prev != "") {
                    ASSERT_GT(_fields->at(1)->get_text(row), prev);
                }

                prev = _fields->at(1)->get_text(row);
                ++count;
            }
        }
        ASSERT_EQ(count, 5000);
    }
}
