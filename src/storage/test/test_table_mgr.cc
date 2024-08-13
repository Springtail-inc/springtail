#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/json.hh>
#include <common/object_cache.hh>
#include <common/properties.hh>
#include <common/threaded_test.hh>

#include <storage/system_tables.hh>
#include <storage/table_mgr.hh>

#include <test/services.hh>

using namespace springtail;

namespace {
    /**
     * Framework for Table and MutableTable testing.
     */
    class TableMgr_Test : public testing::Test {
        void SetUp() override {
            springtail_init();

            auto json = Properties::get(Properties::STORAGE_CONFIG);
            Json::get_to<std::filesystem::path>(json, "table_dir", _table_dir);
            _table_dir = Properties::make_absolute_path(_table_dir);

            _services.init(true);

            std::filesystem::remove_all(_table_dir);
        }

        void TearDown() override {
            _services.shutdown();
        }

        test::Services _services{true, true, false};
    };

    // Tests the schema modification paths
    TEST_F(TableMgr_Test, CreateAlterDrop) {
        // create a table
        PgMsgTable create_msg;
        create_msg.lsn = 0;
        create_msg.oid = 100000;
        create_msg.xid = 2;
        create_msg.schema = "public";
        create_msg.table = "x";
        create_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 0, 0, false, true});
        create_msg.columns.push_back({"col2", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->create_table({2, 0}, create_msg);

        // alter the table's schema
        PgMsgTable alter_msg;
        alter_msg.lsn = 0;
        alter_msg.oid = 100000;
        alter_msg.xid = 3;
        alter_msg.schema = "public";
        alter_msg.table = "x";
        alter_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 0, 0, false, true});
        alter_msg.columns.push_back({"colnew", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->alter_table({3, 0}, alter_msg);

        // drop the table
        PgMsgDropTable drop_msg;
        drop_msg.lsn = 0;
        drop_msg.oid = 100000;
        drop_msg.xid = 4;
        drop_msg.schema = "public";
        drop_msg.table = "x";
        TableMgr::get_instance()->drop_table({4, 0}, drop_msg);

        TableMgr::get_instance()->finalize_metadata(4);

        // verify system table correctness
        auto table = TableMgr::get_instance()->get_table(sys_tbl::TableNames::ID, 5, 0);
        auto fields = table->extent_schema()->get_fields();
        auto row_i = table->begin();

        // verify the name exists at 2 and 3, deleted at 4
        auto tuple = sys_tbl::TableNames::Data::tuple("public", "x", 100000, 2, 0, true);
        auto tuple2 = std::make_shared<FieldTuple>(fields, *row_i);
        ASSERT_TRUE(tuple->equal(*tuple2));
        ++row_i;

        tuple = sys_tbl::TableNames::Data::tuple("public", "x", 100000, 4, 0, false);
        tuple2 = std::make_shared<FieldTuple>(fields, *row_i);
        ASSERT_TRUE(tuple->equal(*tuple2));
        ++row_i;

        ASSERT_TRUE(row_i == table->end());
    }
}
