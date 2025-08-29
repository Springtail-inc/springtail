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
            springtail_init_test();
            test::start_services(true, true, false);
        }
        static void TearDownTestSuite() {
            springtail_shutdown();
        }
    };

    void
    _print_table(uint64_t db_id, uint64_t xid)
    {
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, 12);
        auto fields = table->extent_schema()->get_fields();
        auto row_i = table->begin();

        std::cout << "\n=== START - PrintTable for XID " << xid << " ===\n";
        // print the entire table
        while (row_i != table->end()) {
            auto &&row = *row_i;
            auto tuple = std::make_shared<FieldTuple>(fields, &row);
            std::cout << tuple->to_string() << "\n";
            ++row_i;
        }
        std::cout << "\n=== END - PrintTable for XID " << xid << " ===\n";
    }

    void
    _compare_tuples(TuplePtr expected, TuplePtr actual){
        std::cout << "\n=== START - Compare Tuples ===\n";
        std::cout << expected->to_string() << "\n";
        std::cout << actual->to_string() << "\n";
        std::cout << "\n=== END - Compare Tuples ===\n";
        ASSERT_TRUE(expected->equal_strict(*actual));
    }

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
        create_msg.rls_enabled = false;
        create_msg.rls_forced = false;
        create_msg.columns.emplace_back("col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 0, 0, false, true);
        create_msg.columns.emplace_back("col2", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 1, 0, true, false);

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
        alter_msg.rls_enabled = false;
        alter_msg.rls_forced = false;
        alter_msg.columns.emplace_back("col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 0, 0, false, true);
        alter_msg.columns.emplace_back("colnew", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 1, 0, true, false);

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

        // create a table
        PgMsgTable create_parent_table_msg;
        create_parent_table_msg.lsn = 5;
        create_parent_table_msg.xid = 5;
        create_parent_table_msg.oid = 100000;
        create_parent_table_msg.namespace_name = "public";
        create_parent_table_msg.table = "parent_partition_table";
        create_parent_table_msg.parent_table_id = 0;
        create_parent_table_msg.partition_key = "BY LIST (role)";
        create_parent_table_msg.partition_bound = "";
        create_parent_table_msg.rls_enabled = false;
        create_parent_table_msg.rls_forced = false;
        create_parent_table_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        create_parent_table_msg.columns.emplace_back("name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true);
        create_parent_table_msg.columns.emplace_back("role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false);

        TableMgr::get_instance()->create_table(db_id, {5, 5}, create_parent_table_msg);

        PgMsgTable create_child_table_msg;
        create_child_table_msg.lsn = 6;
        create_child_table_msg.xid = 6;
        create_child_table_msg.oid = 100001;
        create_child_table_msg.namespace_name = "public";
        create_child_table_msg.table = "child_partition_table";
        create_child_table_msg.parent_table_id = 100000;
        create_child_table_msg.partition_key = "";
        create_child_table_msg.partition_bound = "FOR VALUES IN ('Sibling')";
        create_child_table_msg.rls_enabled = false;
        create_child_table_msg.rls_forced = false;
        create_child_table_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        create_child_table_msg.columns.emplace_back("name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true);
        create_child_table_msg.columns.emplace_back("role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false);

        TableMgr::get_instance()->create_table(db_id, {6, 6}, create_child_table_msg);

        TableMgr::get_instance()->finalize_metadata(db_id, 6);

        // create a table
        PgMsgTable create_parent_alter_table_msg;
        create_parent_alter_table_msg.lsn = 7;
        create_parent_alter_table_msg.xid = 7;
        create_parent_alter_table_msg.oid = 100000;
        create_parent_alter_table_msg.namespace_name = "public";
        create_parent_alter_table_msg.table = "parent_partition_alter_table";
        create_parent_alter_table_msg.parent_table_id = 0;
        create_parent_alter_table_msg.partition_key = "BY LIST (role)";
        create_parent_alter_table_msg.partition_bound = "";
        create_parent_alter_table_msg.rls_enabled = false;
        create_parent_alter_table_msg.rls_forced = false;
        create_parent_alter_table_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        create_parent_alter_table_msg.columns.emplace_back("name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true);
        create_parent_alter_table_msg.columns.emplace_back("role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false);

        TableMgr::get_instance()->create_table(db_id, {7, 7}, create_parent_alter_table_msg);

        PgMsgTable create_child_alter_table_msg;
        create_child_alter_table_msg.lsn = 8;
        create_child_alter_table_msg.xid = 8;
        create_child_alter_table_msg.oid = 100001;
        create_child_alter_table_msg.namespace_name = "public";
        create_child_alter_table_msg.table = "child_partition_alter_table";
        create_child_alter_table_msg.parent_table_id = 100000;
        create_child_alter_table_msg.partition_key = "";
        create_child_alter_table_msg.partition_bound = "FOR VALUES IN ('Sibling')";
        create_child_alter_table_msg.rls_enabled = false;
        create_child_alter_table_msg.rls_forced = false;
        create_child_alter_table_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        create_child_alter_table_msg.columns.emplace_back("name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true);
        create_child_alter_table_msg.columns.emplace_back("role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false);

        TableMgr::get_instance()->create_table(db_id, {8, 8}, create_child_alter_table_msg);

        PgMsgTable alter_parent_alter_table_msg;
        alter_parent_alter_table_msg.lsn = 9;
        alter_parent_alter_table_msg.xid = 9;
        alter_parent_alter_table_msg.oid = 100000;
        alter_parent_alter_table_msg.namespace_name = "public";
        alter_parent_alter_table_msg.table = "parent_partition_alter_table";
        alter_parent_alter_table_msg.parent_table_id = 0;
        alter_parent_alter_table_msg.partition_key = "BY LIST (role)";
        alter_parent_alter_table_msg.partition_bound = "";
        alter_parent_alter_table_msg.rls_enabled = false;
        alter_parent_alter_table_msg.rls_forced = false;
        alter_parent_alter_table_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        alter_parent_alter_table_msg.columns.emplace_back("name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true);
        alter_parent_alter_table_msg.columns.emplace_back("role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false);
        alter_parent_alter_table_msg.columns.emplace_back("family", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 2, 0, true, false);

        TableMgr::get_instance()->alter_table(db_id, {9, 9}, alter_parent_alter_table_msg);

        TableMgr::get_instance()->finalize_metadata(db_id, 10);

        // create a table
        PgMsgTable create_parent_attach_table_msg;
        create_parent_attach_table_msg.lsn = 11;
        create_parent_attach_table_msg.xid = 11;
        create_parent_attach_table_msg.oid = 400000;
        create_parent_attach_table_msg.namespace_name = "public";
        create_parent_attach_table_msg.table = "parent_partition_attach_table";
        create_parent_attach_table_msg.parent_table_id = 0;
        create_parent_attach_table_msg.partition_key = "BY LIST (role)";
        create_parent_attach_table_msg.partition_bound = "";
        create_parent_attach_table_msg.rls_enabled = false;
        create_parent_attach_table_msg.rls_forced = false;
        create_parent_attach_table_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        create_parent_attach_table_msg.columns.emplace_back("name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true);
        create_parent_attach_table_msg.columns.emplace_back("role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false);

        TableMgr::get_instance()->create_table(db_id, {11, 11}, create_parent_attach_table_msg);

        PgMsgTable create_child_attach_table_msg;
        create_child_attach_table_msg.lsn = 12;
        create_child_attach_table_msg.xid = 12;
        create_child_attach_table_msg.oid = 500001;
        create_child_attach_table_msg.namespace_name = "public";
        create_child_attach_table_msg.table = "child_partition_attach_table";
        create_child_attach_table_msg.parent_table_id = 0;
        create_child_attach_table_msg.partition_key = "";
        create_child_attach_table_msg.partition_bound = "";
        create_child_attach_table_msg.rls_enabled = false;
        create_child_attach_table_msg.rls_forced = false;
        create_child_attach_table_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        create_child_attach_table_msg.columns.emplace_back("name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true);
        create_child_attach_table_msg.columns.emplace_back("role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false);

        TableMgr::get_instance()->create_table(db_id, {12, 12}, create_child_attach_table_msg);

        TableMgr::get_instance()->finalize_metadata(db_id, 13);

        // attach partition
        PgMsgAttachPartition attach_partition_msg;
        attach_partition_msg.lsn = 14;
        attach_partition_msg.xid = 14;
        attach_partition_msg.table_id = 400000;
        attach_partition_msg.namespace_name = "public";
        attach_partition_msg.table_name = "parent_partition_attach_table";

        PartitionData data;
        data.table_name = "child_partition_attach_table";
        data.table_id = 500001;
        data.namespace_name = "public";
        data.namespace_id = 9000;
        data.partition_bound = "FOR VALUES IN ('Sibling')";
        data.partition_key = "";
        data.parent_table_id = 400000;
        attach_partition_msg.partition_data.emplace_back(data);

        sys_tbl_mgr::Client::get_instance()->attach_partition(db_id, {14, 14}, attach_partition_msg);

        TableMgr::get_instance()->finalize_metadata(db_id, 15);

        _print_table(db_id, 15);

        // verify system table correctness
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, 15);
        auto fields = table->extent_schema()->get_fields();
        auto row_i = table->begin();

        auto tuple_row = sys_tbl::TableNames::Data::tuple(90000, "x", 100000, 2, 2, true, std::nullopt, std::nullopt, std::nullopt, false, false);
        auto table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        ++row_i;
        tuple_row = sys_tbl::TableNames::Data::tuple(90000, "x", 100000, 4, 4, false, std::nullopt, std::nullopt, std::nullopt, false, false);
        table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        ++row_i;
        tuple_row = sys_tbl::TableNames::Data::tuple(90000, "parent_partition_table", 100000, 5, 5, true, std::nullopt, "BY LIST (role)", std::nullopt, false, false);
        table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        ++row_i;
        tuple_row = sys_tbl::TableNames::Data::tuple(90000, "parent_partition_alter_table", 100000, 7, 7, true, std::nullopt, "BY LIST (role)", std::nullopt, false, false);
        table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        ++row_i;
        tuple_row = sys_tbl::TableNames::Data::tuple(90000, "child_partition_table", 100001, 6, 6, true, 100000, std::nullopt, "FOR VALUES IN ('Sibling')", false, false);
        table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        ++row_i;
        tuple_row = sys_tbl::TableNames::Data::tuple(90000, "child_partition_alter_table", 100001, 8, 8, true, 100000, std::nullopt, "FOR VALUES IN ('Sibling')", false, false);
        table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        ++row_i;
        tuple_row = sys_tbl::TableNames::Data::tuple(90000, "parent_partition_attach_table", 400000, 11, 11, true, std::nullopt, "BY LIST (role)", std::nullopt, false, false);
        table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        ++row_i;++row_i;
        tuple_row = sys_tbl::TableNames::Data::tuple(90000, "child_partition_attach_table", 500001, 14, 14, true, 400000, std::nullopt, "FOR VALUES IN ('Sibling')", false, false);
        table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        ++row_i;
        ASSERT_TRUE(row_i == table->end());
    }

    TEST_F(TableMgr_Test, RlsEnabledAndForcedFlags) {
        uint64_t db_id = 2;

        // Create a schema
        PgMsgNamespace ns_msg;
        ns_msg.lsn = 1;
        ns_msg.xid = 1;
        ns_msg.oid = 91000;
        ns_msg.name = "public";
        sys_tbl_mgr::Client::get_instance()->create_namespace(db_id, {1, 1}, ns_msg);

        // Create a table with RLS enabled and forced
        PgMsgTable create_msg;
        create_msg.lsn = 2;
        create_msg.xid = 2;
        create_msg.oid = 110000;
        create_msg.namespace_name = "public";
        create_msg.table = "rls_table";
        create_msg.parent_table_id = 0;
        create_msg.partition_key = "";
        create_msg.partition_bound = "";
        create_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        create_msg.rls_enabled = true;
        create_msg.rls_forced = true;

        TableMgr::get_instance()->create_table(db_id, {2, 2}, create_msg);
        TableMgr::get_instance()->finalize_metadata(db_id, 2);

        _print_table(db_id, 2);

        // Alter the table to disable RLS enabled and forced
        PgMsgTable alter_msg;
        alter_msg.lsn = 3;
        alter_msg.xid = 3;
        alter_msg.oid = 110000;
        alter_msg.namespace_name = "public";
        alter_msg.table = "rls_table";
        alter_msg.parent_table_id = 0;
        alter_msg.partition_key = "";
        alter_msg.partition_bound = "";
        alter_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        alter_msg.rls_enabled = false;
        alter_msg.rls_forced = false;

        TableMgr::get_instance()->alter_table(db_id, {3, 3}, alter_msg);
        TableMgr::get_instance()->finalize_metadata(db_id, 3);

        _print_table(db_id, 3);

        // Alter the table to enable only rls_enabled
        PgMsgTable alter_msg2;
        alter_msg2.lsn = 4;
        alter_msg2.xid = 4;
        alter_msg2.oid = 110000;
        alter_msg2.namespace_name = "public";
        alter_msg2.table = "rls_table";
        alter_msg2.parent_table_id = 0;
        alter_msg2.partition_key = "";
        alter_msg2.partition_bound = "";
        alter_msg2.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        alter_msg2.rls_enabled = true;
        alter_msg2.rls_forced = false;

        TableMgr::get_instance()->alter_table(db_id, {4, 4}, alter_msg2);
        TableMgr::get_instance()->finalize_metadata(db_id, 4);

        _print_table(db_id, 4);

        // Alter the table to enable only rls_forced
        PgMsgTable alter_msg3;
        alter_msg3.lsn = 5;
        alter_msg3.xid = 5;
        alter_msg3.oid = 110000;
        alter_msg3.namespace_name = "public";
        alter_msg3.table = "rls_table";
        alter_msg3.parent_table_id = 0;
        alter_msg3.partition_key = "";
        alter_msg3.partition_bound = "";
        alter_msg3.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        alter_msg3.rls_enabled = false;
        alter_msg3.rls_forced = true;

        TableMgr::get_instance()->alter_table(db_id, {5, 5}, alter_msg3);
        TableMgr::get_instance()->finalize_metadata(db_id, 5);

        _print_table(db_id, 5);

        // Verify RLS flags are set
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, 5);
        auto fields = table->extent_schema()->get_fields();
        auto row_i = table->begin();

        auto tuple_row = sys_tbl::TableNames::Data::tuple(
            91000, "rls_table", 110000, 2, 2, true, std::nullopt, std::nullopt, std::nullopt, true, true
        );
        auto table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        // Verify RLS flags are now disabled
        ++row_i;
        tuple_row = sys_tbl::TableNames::Data::tuple(
            91000, "rls_table", 110000, 3, 3, true, std::nullopt, std::nullopt, std::nullopt, false, false
        );
        table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        // Verify only rls_enabled is set
        ++row_i;
        tuple_row = sys_tbl::TableNames::Data::tuple(
            91000, "rls_table", 110000, 4, 4, true, std::nullopt, std::nullopt, std::nullopt, true, false
        );
        table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        // Verify only rls_forced is set
        ++row_i;
        tuple_row = sys_tbl::TableNames::Data::tuple(
            91000, "rls_table", 110000, 5, 5, true, std::nullopt, std::nullopt, std::nullopt, false, true
        );
        table_row = std::make_shared<FieldTuple>(fields, &*row_i);
        _compare_tuples(tuple_row, table_row);

        ++row_i;
        ASSERT_TRUE(row_i == table->end());
    }
}
