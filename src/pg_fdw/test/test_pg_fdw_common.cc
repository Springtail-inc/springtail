#include <gtest/gtest.h>

#include <common/init.hh>
#include <pg_fdw/pg_fdw_ddl_common.hh>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/client.hh>
#include <test/services.hh>

using namespace springtail;
using namespace springtail::pg_fdw;
using namespace testing;

namespace {
class PgFdwCommonTest : public testing::Test {
public:
    std::unique_ptr<PgFdwCommon> pg_fdw_common_ = std::make_unique<PgFdwCommon>();

protected:
    static void
    create_table_data(uint64_t db_id)
    {
        PgMsgNamespace ns_msg;
        ns_msg.lsn = 1;
        ns_msg.xid = 1;
        ns_msg.oid = 90000;
        ns_msg.name = "public";

        sys_tbl_mgr::Server::get_instance()->create_namespace(db_id, {1, 1}, ns_msg);

        PgMsgTable create_table_msg;
        create_table_msg.lsn = 2;
        create_table_msg.xid = 2;
        create_table_msg.oid = 500001;
        create_table_msg.namespace_name = "public";
        create_table_msg.table = "parent_partition_table";
        create_table_msg.parent_table_id = 0;
        create_table_msg.partition_key = "BY LIST (role)";
        create_table_msg.partition_bound = "";
        create_table_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        create_table_msg.columns.emplace_back("name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true);
        create_table_msg.columns.emplace_back("role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false);

        sys_tbl_mgr::Server::get_instance()->create_table(db_id, {2, 2}, create_table_msg);

        create_table_msg.lsn = 3;
        create_table_msg.xid = 3;
        create_table_msg.oid = 500002;
        create_table_msg.namespace_name = "public";
        create_table_msg.table = "child_partition_table";
        create_table_msg.parent_table_id = 500001;
        create_table_msg.partition_key = "";
        create_table_msg.partition_bound = "FOR VALUES IN ('Sibling')";
        create_table_msg.columns.emplace_back("id", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 0, 0, false, true);
        create_table_msg.columns.emplace_back("name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 0, 0, false, true);
        create_table_msg.columns.emplace_back("role", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 1, 0, true, false);

        sys_tbl_mgr::Server::get_instance()->create_table(db_id, {3, 3}, create_table_msg);

        PgMsgDropTable drop_msg;
        drop_msg.lsn = 4;
        drop_msg.xid = 4;
        drop_msg.oid = 500002;
        drop_msg.namespace_name = "public";
        drop_msg.table = "child_partition_table";
        sys_tbl_mgr::Server::get_instance()->drop_table(db_id, {4, 4}, drop_msg);

        PgMsgNamespace dummy_ns_msg;
        dummy_ns_msg.lsn = 5;
        dummy_ns_msg.xid = 5;
        dummy_ns_msg.oid = 8888;
        dummy_ns_msg.name = "dummy";

        sys_tbl_mgr::Server::get_instance()->create_namespace(db_id, {5, 5}, dummy_ns_msg);

        PgMsgNamespace drop_ns_msg;
        drop_ns_msg.lsn = 6;
        drop_ns_msg.xid = 6;
        drop_ns_msg.oid = 8888;
        drop_ns_msg.name = "dummy";

        sys_tbl_mgr::Server::get_instance()->drop_namespace(db_id, {6, 6}, drop_ns_msg);

        sys_tbl_mgr::Server::get_instance()->finalize(db_id, 7, true);
    }

    static void SetUpTestSuite() {
        springtail_init_test();
        test::start_services(true, true, false);

        create_table_data(1);
    }
    static void TearDownTestSuite() {
        springtail_shutdown();
    }
};


TEST_F(PgFdwCommonTest, GetParentTableInfo_Success)
{
    const uint64_t db_id = 1;
    const uint64_t schema_xid = 10;
    const uint64_t table_id = 500001;
    const std::string expected_name = "parent_partition_table";
    const uint64_t expected_namespace_id = 90000;

    auto [parent_table_name, parent_namespace_id] = PgFdwCommon::_get_parent_table_info(db_id, schema_xid, table_id);

    EXPECT_EQ(parent_table_name, expected_name);
    EXPECT_EQ(parent_namespace_id, expected_namespace_id);
}

TEST_F(PgFdwCommonTest, GetParentTableInfo_TableNotFound)
{
    const uint64_t db_id = 1;
    const uint64_t schema_xid = 10;
    const uint64_t non_existent_table_id = 9999;

    auto [parent_table_name, parent_namespace_id] = PgFdwCommon::_get_parent_table_info(db_id, schema_xid, non_existent_table_id);

    EXPECT_TRUE(parent_table_name.empty());
    EXPECT_EQ(parent_namespace_id, 0);
}

TEST_F(PgFdwCommonTest, GetParentTableInfo_TableMarkedNonExistent)
{
    const uint64_t db_id = 1;
    const uint64_t schema_xid = 10;
    const uint64_t table_id = 500002;

    auto [parent_table_name, parent_namespace_id] = PgFdwCommon::_get_parent_table_info(db_id, schema_xid, table_id);

    EXPECT_TRUE(parent_table_name.empty());
    EXPECT_EQ(parent_namespace_id, 0);
}

TEST_F(PgFdwCommonTest, GetNamespaceName_Success)
{
    const uint64_t db_id = 1;
    const uint64_t schema_xid = 5;
    const uint64_t namespace_id = 8888;

    auto namespace_name = PgFdwCommon::_get_namespace_name(db_id, schema_xid, namespace_id);

    EXPECT_EQ(namespace_name, "dummy");
}

TEST_F(PgFdwCommonTest, GetNamespaceName_NamespaceNotFound)
{
    const uint64_t db_id = 1;
    const uint64_t schema_xid = 5;
    const uint64_t non_existent_namespace_id = 9999;

    auto namespace_name = PgFdwCommon::_get_namespace_name(db_id, schema_xid, non_existent_namespace_id);

    EXPECT_EQ(namespace_name, "");
}

TEST_F(PgFdwCommonTest, GetNamespaceName_NonExistent)
{
    const uint64_t db_id = 1;
    const uint64_t schema_xid = 6;
    const uint64_t non_existent_namespace_id = 8888;

    auto namespace_name = PgFdwCommon::_get_namespace_name(db_id, schema_xid, non_existent_namespace_id);

    EXPECT_TRUE(namespace_name.empty());
}

TEST_F(PgFdwCommonTest, GetNamespaceName_NamespaceMarkedNonExistent)
{
    const uint64_t db_id = 1;
    const uint64_t schema_xid = 6;
    const uint64_t namespace_id = 1000;

    auto namespace_name = PgFdwCommon::_get_namespace_name(db_id, schema_xid, namespace_id);

    EXPECT_TRUE(namespace_name.empty());
}

TEST_F(PgFdwCommonTest, IterateTableNames_Success)
{
    const uint64_t db_id = 1;
    const uint64_t schema_xid = 6;
    const uint64_t namespace_id = 90000;

    std::map<std::string, PgFdwCommon::TableEntry> table_map;
    std::map<uint64_t, PartitionInfo> table_partition_map;

    const std::set<std::string, std::less<>> &table_set = {};
    // iterate over the table names table and populate the table map
    PgFdwCommon::_iterate_table_names(db_id, schema_xid, namespace_id, false, false, table_set, "public", table_map, table_partition_map);

    EXPECT_EQ(table_partition_map.size(), 2);
    EXPECT_EQ(table_partition_map.at(500001).parent_table_id(), 0);
    EXPECT_EQ(table_partition_map.at(500002).parent_table_id(), 500001);
    EXPECT_EQ(table_partition_map.at(500001).partition_key(), "BY LIST (role)");
    EXPECT_EQ(table_partition_map.at(500002).partition_bound(), "FOR VALUES IN ('Sibling')");
}
}
