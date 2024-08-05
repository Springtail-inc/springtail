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
#include <storage/xid.hh>

#include <sys_tbl_mgr/sys_tbl_mgr_client.hh>

#include <test/services.hh>

using namespace springtail;

namespace {
    /**
     * Framework for Table and MutableTable testing.
     */
    class SysTblMgr_Test : public testing::Test {
    public:
        static void SetUpTestSuite() {
            springtail_init();

            _services.init(true);
        }

        static void TearDownTestSuite() {
            _services.shutdown();
        }

        static test::Services _services;
        static XidLsn _xid;
    };

    test::Services SysTblMgr_Test::_services(true, true, false);
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
        _xid.lsn = 1;

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

        // alter the table
        alter_msg.oid = tid;
        alter_msg.schema = "public";
        alter_msg.table = "y";
        alter_msg.columns.clear();
        alter_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 1, 0, false, true});
        alter_msg.columns.push_back({"coltwo", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 2, 0, false, false});
        alter_msg.columns.push_back({"col3", static_cast<uint8_t>(SchemaType::INT32), 0, "0", 3, 0, true, false});

        client->alter_table(_xid, alter_msg);
        ++_xid.lsn;

        // verify the virtual schema creation from the cache prior to finalize
        auto &&schema_check = client->get_schema_info_with_target(tid, { _xid.xid - 1, constant::MAX_LSN }, _xid);
        assert(schema_check.history.size() == 2);
        assert(schema_check.history[0].update_type == SchemaUpdateType::NULLABLE_CHANGE);
        assert(schema_check.history[1].update_type == SchemaUpdateType::NEW_COLUMN);

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
        XidLsn access_xid(check_xid - 4);
        XidLsn target_xid(check_xid);
        schema_meta = client->get_schema_info_with_target(tid, access_xid, target_xid);

        assert(schema_meta.columns.size() == 2);
        assert(schema_meta.columns[0].name == "col1");
        assert(schema_meta.columns[1].name == "col2");

        assert(schema_meta.history.size() == 6);

        assert(schema_meta.history[0].update_type == SchemaUpdateType::REMOVE_COLUMN);
        assert(schema_meta.history[0].name == "col1");
        assert(schema_meta.history[0].xid == check_xid);

        assert(schema_meta.history[1].update_type == SchemaUpdateType::NAME_CHANGE);
        assert(schema_meta.history[1].name == "coltwo");
        assert(schema_meta.history[1].xid == check_xid - 2);

        assert(schema_meta.history[2].update_type == SchemaUpdateType::NULLABLE_CHANGE);
        assert(schema_meta.history[2].name == "coltwo");
        assert(schema_meta.history[2].xid == check_xid - 1);
        assert(schema_meta.history[2].lsn == 3);

        assert(schema_meta.history[3].update_type == SchemaUpdateType::REMOVE_COLUMN);
        assert(schema_meta.history[3].name == "coltwo");
        assert(schema_meta.history[3].xid == check_xid);

        assert(schema_meta.history[4].update_type == SchemaUpdateType::NEW_COLUMN);
        assert(schema_meta.history[4].name == "col3");
        assert(schema_meta.history[4].xid == check_xid - 1);
        assert(schema_meta.history[4].lsn == 1);

        assert(schema_meta.history[5].update_type == SchemaUpdateType::REMOVE_COLUMN);
        assert(schema_meta.history[5].name == "col3");
        assert(schema_meta.history[5].xid == check_xid);
    }
}
