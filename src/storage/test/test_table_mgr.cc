#include <gtest/gtest.h>
#include <optional>

#include <common/init.hh>
#include <common/json.hh>
#include <common/object_cache.hh>
#include <common/properties.hh>
#include <common/threaded_test.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table_mgr.hh>

#include <test/services.hh>

using namespace springtail;

namespace {
    /**
     * Framework for Table and MutableTable testing.
     */
    class TableMgr_Test : public testing::Test {
    public:
        static void SetUpTestSuite() {
            std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
            runners.emplace();
            runners->emplace_back(std::make_unique<IOMgrRunner>());

            auto service_runners = test::get_services(true, true, false);
            std::move(service_runners.begin(), service_runners.end(), std::back_inserter(runners.value()));

            springtail_init_test(runners);
        }
        static void TearDownTestSuite() {
            springtail_shutdown();
        }
    };

    // Tests the schema modification paths
    TEST_F(TableMgr_Test, CreateAlterDrop) {
        uint64_t db_id = 1;

        // create a schema
        PgMsgNamespace ns_msg;
        ns_msg.lsn = 0;
        ns_msg.xid = 2;
        ns_msg.oid = 90000;
        ns_msg.name = "public";
        sys_tbl_mgr::Client::get_instance()->create_namespace(db_id, {2, 0}, ns_msg);

        // create a table
        PgMsgTable create_msg;
        create_msg.lsn = 1;
        create_msg.oid = 100000;
        create_msg.xid = 2;
        create_msg.namespace_name = "public";
        create_msg.table = "x";
        create_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 0, 0, false, true});
        create_msg.columns.push_back({"col2", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->create_table(db_id, {2, 1}, create_msg);

        // alter the table's schema
        PgMsgTable alter_msg;
        alter_msg.lsn = 0;
        alter_msg.oid = 100000;
        alter_msg.xid = 3;
        alter_msg.namespace_name = "public";
        alter_msg.table = "x";
        alter_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 0, 0, false, true});
        alter_msg.columns.push_back({"colnew", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->alter_table(db_id, {3, 0}, alter_msg);

        // drop the table
        PgMsgDropTable drop_msg;
        drop_msg.lsn = 0;
        drop_msg.oid = 100000;
        drop_msg.xid = 4;
        drop_msg.namespace_name = "public";
        drop_msg.table = "x";
        TableMgr::get_instance()->drop_table(db_id, {4, 0}, drop_msg);

        TableMgr::get_instance()->finalize_metadata(db_id, 4);

        // verify system table correctness
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, 5);
        auto fields = table->extent_schema()->get_fields();
        auto row_i = table->begin();
        auto &&row = *row_i;

        // verify the name exists at 2 and 3, deleted at 4
        auto tuple = sys_tbl::TableNames::Data::tuple(90000, "x", 100000, 2, 1, true, std::nullopt, std::nullopt, std::nullopt);
        auto tuple2 = std::make_shared<FieldTuple>(fields, &row);
        ASSERT_TRUE(tuple->equal_strict(*tuple2));
        ++row_i;
        auto &&next_row = *row_i;

        tuple = sys_tbl::TableNames::Data::tuple(90000, "x", 100000, 4, 0, false, std::nullopt, std::nullopt, std::nullopt);
        tuple2 = std::make_shared<FieldTuple>(fields, &next_row);
        ASSERT_TRUE(tuple->equal_strict(*tuple2));
        ++row_i;

        ASSERT_TRUE(row_i == table->end());
    }
}
