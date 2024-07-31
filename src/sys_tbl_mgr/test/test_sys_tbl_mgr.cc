/*
 * Tests the interfaces of the SysTblMgr service.
 */
#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/json.hh>
#include <common/object_cache.hh>
#include <common/properties.hh>
#include <common/threaded_test.hh>

#include <storage/schema.hh>

#include <sys_tbl_mgr/sys_tbl_mgr_client.hh>
#include <sys_tbl_mgr/sys_tbl_mgr_server.hh>

#include <xid_mgr/xid_mgr_server.hh>

using namespace springtail;

namespace {
    /**
     * Framework for Table and MutableTable testing.
     */
    class SysTblMgr_Test : public testing::Test {
    public:
        static void SetUpTestSuite() {
            springtail_init();

            auto json = Properties::get(Properties::STORAGE_CONFIG);
            Json::get_to<std::filesystem::path>(json, "table_dir", _table_dir,
                                                "/tmp/springtail/table");
            std::filesystem::remove_all(_table_dir);

            json = Properties::get(Properties::XID_MGR_CONFIG);
            Json::get_to<std::filesystem::path>(json, "base_path", _xid_dir, "/tmp/xid_mgr");
            std::filesystem::remove_all(_xid_dir);

            _systbl_thread = std::thread([]{
                SysTblMgrServer::startup();
            });

            _xidmgr_thread = std::thread([]{
                XidMgrServer::startup();
            });
        }

        static void TearDownTestSuite() {
            SysTblMgrServer::shutdown();
            _systbl_thread.join();

            XidMgrServer::shutdown();
            _xidmgr_thread.join();

            std::filesystem::remove_all(_table_dir);
        }

        static std::filesystem::path _table_dir, _xid_dir;
        static std::thread _systbl_thread, _xidmgr_thread;
        static XidLsn _xid;
    };

    std::filesystem::path SysTblMgr_Test::_table_dir;
    std::filesystem::path SysTblMgr_Test::_xid_dir;
    std::thread SysTblMgr_Test::_systbl_thread;
    std::thread SysTblMgr_Test::_xidmgr_thread;
    XidLsn SysTblMgr_Test::_xid(1, 0);

    // Tests the schema modification paths
    TEST_F(SysTblMgr_Test, Basic) {
        auto client = SysTblMgrClient::get_instance();
        client->ping();
    }

    // Tests table create / alter / drop
    TEST_F(SysTblMgr_Test, CreateAlterDrop) {
        auto client = SysTblMgrClient::get_instance();
        
        // create the table
        uint64_t tid = 100000;

        PgMsgTable create_msg;
        create_msg.oid = tid;
        create_msg.schema = "public";
        create_msg.table = "x";
        create_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 1, 0, false, true});
        create_msg.columns.push_back({"col2", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 2, 0, true, false});

        client->create_table(_xid, create_msg);

        // alter the table's schema
        ++_xid.lsn;

        PgMsgTable alter_msg;
        alter_msg.oid = tid;
        alter_msg.schema = "public";
        alter_msg.table = "x";
        alter_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 1, 0, false, true});
        alter_msg.columns.push_back({"colnew", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 2, 0, true, false});

        client->alter_table(_xid, alter_msg);

        // verify system table correctness before finalize
        auto &&metadata = client->get_roots(tid, 0);
        assert(metadata.roots.size() == 0);
        assert(metadata.stats.row_count == 0);

        auto &&schema_meta = client->get_schema_info(tid, { 0, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 0);

        schema_meta = client->get_schema_info(tid, { 1, 0 });
        assert(schema_meta.columns.size() == 2);

        schema_meta = client->get_schema_info(tid, { 1, 1 });
        assert(schema_meta.columns.size() == 2);

        schema_meta = client->get_schema_info(tid, { 1, 3 });
        assert(schema_meta.columns.size() == 2);

        // verify correctness after finalize
        client->finalize(_xid.xid);

        metadata = client->get_roots(tid, 1);
        assert(metadata.roots.size() == 1);
        assert(metadata.roots[0] == constant::UNKNOWN_EXTENT);
        assert(metadata.stats.row_count == 0);

        schema_meta = client->get_schema_info(tid, { 0, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 0);

        schema_meta = client->get_schema_info(tid, { 1, 0 });
        assert(schema_meta.columns.size() == 2);

        schema_meta = client->get_schema_info(tid, { 1, 1 });
        assert(schema_meta.columns.size() == 2);

        schema_meta = client->get_schema_info(tid, { 1, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 2);

        // drop the table
        ++_xid.xid;
        _xid.lsn = 0;

        PgMsgDropTable drop_msg;
        drop_msg.oid = tid;
        drop_msg.schema = "public";
        drop_msg.table = "x";

        client->drop_table(_xid, drop_msg);

        // verify system table correctness before finalize
        metadata = client->get_roots(tid, 1);
        assert(metadata.roots.size() == 1);
        assert(metadata.roots[0] == constant::UNKNOWN_EXTENT);
        assert(metadata.stats.row_count == 0);

        schema_meta = client->get_schema_info(tid, { 0, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 0);

        schema_meta = client->get_schema_info(tid, { 1, 1 });
        assert(schema_meta.columns.size() == 2);

        schema_meta = client->get_schema_info(tid, { 1, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 2);

        schema_meta = client->get_schema_info(tid, { 2, 0 });
        assert(schema_meta.columns.size() == 0);

        // verify correctness after finalize
        client->finalize(_xid.xid);
        ++_xid.xid;
        _xid.lsn = 0;

        schema_meta = client->get_schema_info(tid, { 0, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 0);

        schema_meta = client->get_schema_info(tid, { 1, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 2);

        schema_meta = client->get_schema_info(tid, { 2, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 0);

        metadata = client->get_roots(tid, 1);
        assert(metadata.roots.size() == 1);
        assert(metadata.roots[0] == constant::UNKNOWN_EXTENT);
        assert(metadata.stats.row_count == 0);

        metadata = client->get_roots(tid, 2);
        assert(metadata.roots.size() == 0);
        assert(metadata.stats.row_count == 0);
    }

    // Tests interleaving of DDL and DML interactions with the system tables
    TEST_F(SysTblMgr_Test, Complex) {
        auto client = SysTblMgrClient::get_instance();

        // create table
        uint64_t check_xid = _xid.xid;
        uint64_t tid = 100001;

        PgMsgTable create_msg;
        create_msg.oid = tid;
        create_msg.schema = "public";
        create_msg.table = "x";
        create_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 1, 0, false, true});
        create_msg.columns.push_back({"col2", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 2, 0, true, false});

        client->create_table(_xid, create_msg);
        ++_xid.lsn;

        // "add data" to the table
        client->update_roots(tid, _xid.xid, { 0 }, 15);

        // finalize
        client->finalize(_xid.xid);
        ++_xid.xid;
        _xid.lsn = 0;

        // add more data to the table
        client->update_roots(tid, _xid.xid, { 100 }, 30);

        // finalize
        client->finalize(_xid.xid);
        ++_xid.xid;
        _xid.lsn = 0;

        // alter the table
        PgMsgTable alter_msg;
        alter_msg.oid = tid;
        alter_msg.schema = "public";
        alter_msg.table = "x";
        alter_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 1, 0, false, true});
        alter_msg.columns.push_back({"coltwo", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 2, 0, true, false});

        client->alter_table(_xid, alter_msg);
        ++_xid.lsn;

        // finalize
        client->finalize(_xid.xid);
        ++_xid.xid;
        _xid.lsn = 0;

        // alter the table
        alter_msg.oid = tid;
        alter_msg.schema = "public";
        alter_msg.table = "x";
        alter_msg.columns.clear();
        alter_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 1, 0, false, true});
        alter_msg.columns.push_back({"coltwo", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 2, 0, true, false});
        alter_msg.columns.push_back({"col3", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 3, 0, true, false});

        client->alter_table(_xid, alter_msg);
        ++_xid.lsn;

        // alter the table
        alter_msg.oid = tid;
        alter_msg.schema = "public";
        alter_msg.table = "y";
        alter_msg.columns.clear();
        alter_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 1, 0, false, true});
        alter_msg.columns.push_back({"coltwo", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 2, 0, true, false});
        alter_msg.columns.push_back({"col3", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 3, 0, true, false});

        client->alter_table(_xid, alter_msg);
        ++_xid.lsn;

        // finalize
        client->finalize(_xid.xid);
        ++_xid.xid;
        _xid.lsn = 0;

        // drop the table
        PgMsgDropTable drop_msg;
        drop_msg.oid = tid;
        drop_msg.schema = "public";
        drop_msg.table = "y";

        client->drop_table(_xid, drop_msg);

        // finalize
        client->finalize(_xid.xid);
        ++_xid.xid;
        _xid.lsn = 0;

        // verify the data at each step

        // XID 0
        auto &&schema_meta = client->get_schema_info(tid, { check_xid - 1,
                                                            constant::MAX_LSN });
        assert(schema_meta.columns.size() == 0);

        // XID 1
        auto &&metadata = client->get_roots(tid, check_xid);
        assert(metadata.roots.size() == 1);
        assert(metadata.roots[0] == 0);
        assert(metadata.stats.row_count == 15);

        schema_meta = client->get_schema_info(tid, { check_xid, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 2);
        assert(schema_meta.columns[0].name == "col1");
        assert(schema_meta.columns[1].name == "col2");

        // XID 2
        ++check_xid;

        metadata = client->get_roots(tid, check_xid);
        assert(metadata.roots.size() == 1);
        assert(metadata.roots[0] == 100);
        assert(metadata.stats.row_count == 30);

        schema_meta = client->get_schema_info(tid, { check_xid, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 2);
        assert(schema_meta.columns[0].name == "col1");
        assert(schema_meta.columns[1].name == "col2");

        // XID 3
        ++check_xid;

        metadata = client->get_roots(tid, check_xid);
        assert(metadata.roots.size() == 1);
        assert(metadata.roots[0] == 100);
        assert(metadata.stats.row_count == 30);

        schema_meta = client->get_schema_info(tid, { check_xid, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 2);
        assert(schema_meta.columns[0].name == "col1");
        assert(schema_meta.columns[1].name == "coltwo");

        // XID 4
        ++check_xid;

        metadata = client->get_roots(tid, check_xid);
        assert(metadata.roots.size() == 1);
        assert(metadata.roots[0] == 100);
        assert(metadata.stats.row_count == 30);

        schema_meta = client->get_schema_info(tid, { check_xid, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 3);
        assert(schema_meta.columns[0].name == "col1");
        assert(schema_meta.columns[1].name == "coltwo");
        assert(schema_meta.columns[2].name == "col3");

        // XID 5
        ++check_xid;

        metadata = client->get_roots(tid, check_xid);
        assert(metadata.roots.size() == 0);
        assert(metadata.stats.row_count == 0);

        schema_meta = client->get_schema_info(tid, { check_xid, constant::MAX_LSN });
        assert(schema_meta.columns.size() == 0);

        // verify the virtual schema creation at various combinations of access and target XID
    }
}
