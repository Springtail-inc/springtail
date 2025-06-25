#include <gtest/gtest.h>
#include <iterator>
#include <optional>

#include <common/init.hh>
#include <common/json.hh>
#include <common/logging.hh>
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
        ns_msg.lsn = 1;
        ns_msg.xid = 1;
        ns_msg.oid = 90000;
        ns_msg.name = "public";
        sys_tbl_mgr::Client::get_instance()->create_namespace(db_id, {1, 1}, ns_msg);

        // create a table
        PgMsgTable create_msg;
        create_msg.lsn = 2;
        create_msg.xid = 2;
        create_msg.oid = 100000;
        create_msg.namespace_name = "public";
        create_msg.table = "x";
        create_msg.parent_table_id = 0;
        create_msg.partition_key = "";
        create_msg.partition_bound = "";
        create_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 0, 0, false, true});
        create_msg.columns.push_back({"col2", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->create_table(db_id, {2, 2}, create_msg);

        // alter the table's schema
        PgMsgTable alter_msg;
        alter_msg.lsn = 3;
        alter_msg.oid = 100000;
        alter_msg.xid = 3;
        alter_msg.namespace_name = "public";
        alter_msg.table = "x";
        alter_msg.parent_table_id = 0;
        alter_msg.partition_key = "";
        alter_msg.partition_bound = "";
        alter_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 0, 0, false, true});
        alter_msg.columns.push_back({"colnew", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->alter_table(db_id, {3, 3}, alter_msg);

        // drop the table
        PgMsgDropTable drop_msg;
        drop_msg.lsn = 4;
        drop_msg.oid = 100000;
        drop_msg.xid = 4;
        drop_msg.namespace_name = "public";
        drop_msg.table = "x";
        TableMgr::get_instance()->drop_table(db_id, {4, 4}, drop_msg);

        TableMgr::get_instance()->finalize_metadata(db_id, 4);

        // verify system table correctness
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, 12);
        auto fields = table->extent_schema()->get_fields();
        auto row_i = table->begin();
        auto &&row = *row_i;

        // verify the name exists at 2 and 3, deleted at 4
        auto tuple = sys_tbl::TableNames::Data::tuple(90000, "x", 100000, 2, 2, true, 0, "", "");
        auto tuple2 = std::make_shared<FieldTuple>(fields, &row);

        ASSERT_TRUE(tuple->equal_strict(*tuple2));

        ++row_i;
        auto &&next_row = *row_i;

        tuple = sys_tbl::TableNames::Data::tuple(90000, "x", 100000, 4, 4, false, std::nullopt, std::nullopt, std::nullopt);
        tuple2 = std::make_shared<FieldTuple>(fields, &next_row);

        ASSERT_TRUE(tuple->equal_strict(*tuple2));
    }

    TEST_F(TableMgr_Test, CreateTableWithPartitions) {
        uint64_t db_id = 1;

        // create a schema
        PgMsgNamespace ns_msg;
        ns_msg.lsn = 5;
        ns_msg.xid = 5;
        ns_msg.oid = 90000;
        ns_msg.name = "public";
        sys_tbl_mgr::Client::get_instance()->create_namespace(db_id, {5, 5}, ns_msg);

        // create a table
        PgMsgTable create_parent_table_msg;
        create_parent_table_msg.lsn = 6;
        create_parent_table_msg.xid = 6;
        create_parent_table_msg.oid = 100000;
        create_parent_table_msg.namespace_name = "public";
        create_parent_table_msg.table = "parent_partition_table";
        create_parent_table_msg.parent_table_id = 0;
        create_parent_table_msg.partition_key = "BY LIST (role)";
        create_parent_table_msg.partition_bound = "";
        create_parent_table_msg.columns.push_back({"id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true});
        create_parent_table_msg.columns.push_back({"name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true});
        create_parent_table_msg.columns.push_back({"role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->create_table(db_id, {6, 6}, create_parent_table_msg);

        PgMsgTable create_child_table_msg;
        create_child_table_msg.lsn = 7;
        create_child_table_msg.xid = 7;
        create_child_table_msg.oid = 100001;
        create_child_table_msg.namespace_name = "public";
        create_child_table_msg.table = "child_partition_table";
        create_child_table_msg.parent_table_id = 100000;
        create_child_table_msg.partition_key = "";
        create_child_table_msg.partition_bound = "FOR VALUES IN ('Sibling')";
        create_child_table_msg.columns.push_back({"id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true});
        create_child_table_msg.columns.push_back({"name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true});
        create_child_table_msg.columns.push_back({"role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->create_table(db_id, {7, 7}, create_child_table_msg);

        TableMgr::get_instance()->finalize_metadata(db_id, 7);

        // verify system table correctness
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, 8);
        auto fields = table->extent_schema()->get_fields();
        auto row_i = table->begin();
        std::advance(row_i, 2);
        auto &&row = *row_i;

        auto parent_table_tuple = sys_tbl::TableNames::Data::tuple(90000, "parent_partition_table", 100000, 6, 6, true, 0, "BY LIST (role)", "");
        auto parent_table_row = std::make_shared<FieldTuple>(fields, &row);

        ASSERT_TRUE(parent_table_tuple->equal_strict(*parent_table_row));
        ++row_i;
        auto &&next_row = *row_i;

        auto child_table_tuple = sys_tbl::TableNames::Data::tuple(90000, "child_partition_table", 100001, 7, 7, true, 100000, "", "FOR VALUES IN ('Sibling')");
        auto child_table_row = std::make_shared<FieldTuple>(fields, &next_row);

        ASSERT_TRUE(child_table_tuple->equal_strict(*child_table_row));
        ++row_i;

        ASSERT_TRUE(row_i == table->end());
    }

    TEST_F(TableMgr_Test, CreateAlterTableWithPartitions) {
        uint64_t db_id = 1;

        // create a schema
        PgMsgNamespace ns_msg;
        ns_msg.lsn = 8;
        ns_msg.xid = 8;
        ns_msg.oid = 90000;
        ns_msg.name = "public";
        sys_tbl_mgr::Client::get_instance()->create_namespace(db_id, {8, 8}, ns_msg);

        // create a table
        PgMsgTable create_parent_table_msg;
        create_parent_table_msg.lsn = 9;
        create_parent_table_msg.xid = 9;
        create_parent_table_msg.oid = 100000;
        create_parent_table_msg.namespace_name = "public";
        create_parent_table_msg.table = "parent_partition_alter_table";
        create_parent_table_msg.parent_table_id = 0;
        create_parent_table_msg.partition_key = "BY LIST (role)";
        create_parent_table_msg.partition_bound = "";
        create_parent_table_msg.columns.push_back({"id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true});
        create_parent_table_msg.columns.push_back({"name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true});
        create_parent_table_msg.columns.push_back({"role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->create_table(db_id, {9, 9}, create_parent_table_msg);

        PgMsgTable create_child_table_msg;
        create_child_table_msg.lsn = 10;
        create_child_table_msg.xid = 10;
        create_child_table_msg.oid = 100001;
        create_child_table_msg.namespace_name = "public";
        create_child_table_msg.table = "child_partition_alter_table";
        create_child_table_msg.parent_table_id = 100000;
        create_child_table_msg.partition_key = "";
        create_child_table_msg.partition_bound = "FOR VALUES IN ('Sibling')";
        create_child_table_msg.columns.push_back({"id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true});
        create_child_table_msg.columns.push_back({"name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true});
        create_child_table_msg.columns.push_back({"role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->create_table(db_id, {10, 10}, create_child_table_msg);

        PgMsgTable alter_msg;
        alter_msg.lsn = 11;
        alter_msg.xid = 11;
        alter_msg.oid = 100000;
        alter_msg.namespace_name = "public";
        alter_msg.table = "parent_partition_alter_table";
        alter_msg.parent_table_id = 0;
        alter_msg.partition_key = "BY LIST (role)";
        alter_msg.partition_bound = "";
        alter_msg.columns.push_back({"id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true});
        alter_msg.columns.push_back({"name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true});
        alter_msg.columns.push_back({"role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false});
        alter_msg.columns.push_back({"family", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 2, 0, true, false});

        TableMgr::get_instance()->alter_table(db_id, {11, 11}, alter_msg);

        TableMgr::get_instance()->finalize_metadata(db_id, 11);

        // verify system table correctness
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, 12);
        auto fields = table->extent_schema()->get_fields();
        auto row_i = table->begin();
        std::advance(row_i, 3);
        auto &&row = *row_i;

        auto parent_table_tuple = sys_tbl::TableNames::Data::tuple(90000, "parent_partition_alter_table", 100000, 9, 9, true, 0, "BY LIST (role)", "");
        auto parent_table_row = std::make_shared<FieldTuple>(fields, &row);

        ASSERT_TRUE(parent_table_tuple->equal_strict(*parent_table_row));
        ++row_i;
        ++row_i;
        auto &&next_row = *row_i;

        auto child_table_tuple = sys_tbl::TableNames::Data::tuple(90000, "child_partition_alter_table", 100001, 10, 10, true, 100000, "", "FOR VALUES IN ('Sibling')");
        auto child_table_row = std::make_shared<FieldTuple>(fields, &next_row);

        ASSERT_TRUE(child_table_tuple->equal_strict(*child_table_row));
        ++row_i;

        ASSERT_TRUE(row_i == table->end());
    }

    TEST_F(TableMgr_Test, AttachPartition) {
        uint64_t db_id = 1;

        // create a schema
        PgMsgNamespace ns_msg;
        ns_msg.lsn = 12;
        ns_msg.xid = 12;
        ns_msg.oid = 90000;
        ns_msg.name = "public";
        sys_tbl_mgr::Client::get_instance()->create_namespace(db_id, {12, 12}, ns_msg);

        // create a table
        PgMsgTable create_parent_table_msg;
        create_parent_table_msg.lsn = 13;
        create_parent_table_msg.xid = 13;
        create_parent_table_msg.oid = 400000;
        create_parent_table_msg.namespace_name = "public";
        create_parent_table_msg.table = "parent_partition_attach_table";
        create_parent_table_msg.parent_table_id = 0;
        create_parent_table_msg.partition_key = "BY LIST (role)";
        create_parent_table_msg.partition_bound = "";
        create_parent_table_msg.columns.push_back({"id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true});
        create_parent_table_msg.columns.push_back({"name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true});
        create_parent_table_msg.columns.push_back({"role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->create_table(db_id, {13, 13}, create_parent_table_msg);

        PgMsgTable create_child_table_msg;
        create_child_table_msg.lsn = 14;
        create_child_table_msg.xid = 14;
        create_child_table_msg.oid = 500001;
        create_child_table_msg.namespace_name = "public";
        create_child_table_msg.table = "child_partition_attach_table";
        create_child_table_msg.parent_table_id = 400000;
        create_child_table_msg.partition_key = "";
        create_child_table_msg.partition_bound = "";
        create_child_table_msg.columns.push_back({"id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true});
        create_child_table_msg.columns.push_back({"name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true});
        create_child_table_msg.columns.push_back({"role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false});

        TableMgr::get_instance()->create_table(db_id, {14, 14}, create_child_table_msg);

        // attach partition
        PgMsgAttachPartition attach_partition_msg;
        attach_partition_msg.lsn = 15;
        attach_partition_msg.xid = 15;
        attach_partition_msg.table_id = 500001;
        attach_partition_msg.namespace_name = "public";
        attach_partition_msg.table_name = "child_partition_attach_table";
        attach_partition_msg.partition_name = "child_partition_attach_table";

        attach_partition_msg.partition_data.push_back({"child_partition_attach_table", 500001, "public", 400000, "child_partition_attach_table", "FOR VALUES IN ('Sibling')", 100000});

        TableMgr::get_instance()->attach_partition(db_id, {15, 15}, attach_partition_msg);

        TableMgr::get_instance()->finalize_metadata(db_id, 15);

        // verify system table correctness
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, 16);
        auto fields = table->extent_schema()->get_fields();
        auto row_i = table->begin();
        std::advance(row_i, 6);
        auto &&row = *row_i;

        auto parent_table_tuple = sys_tbl::TableNames::Data::tuple(90000, "parent_partition_attach_table", 400000, 13, 13, true, 0, "BY LIST (role)", "");
        auto parent_table_row = std::make_shared<FieldTuple>(fields, &row);

        ASSERT_TRUE(parent_table_tuple->equal_strict(*parent_table_row));
        ++row_i;
        auto &&next_row = *row_i;

        auto child_table_tuple = sys_tbl::TableNames::Data::tuple(90000, "child_partition_attach_table", 500001, 14, 14, true, 400000, "", "");
        auto child_table_row = std::make_shared<FieldTuple>(fields, &next_row);

        ASSERT_TRUE(child_table_tuple->equal_strict(*child_table_row));
        ++row_i;

        ASSERT_TRUE(row_i == table->end());
    }
}
