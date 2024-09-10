/*
 * Tests the interfaces of the SysTblMgr service.
 */
#include <algorithm>
#include <barrier>

#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/json.hh>
#include <common/object_cache.hh>
#include <common/properties.hh>
#include <common/threaded_test.hh>

#include <storage/schema.hh>
#include <storage/xid.hh>

#include <sys_tbl_mgr/client.hh>

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
        static std::mutex _mutex;
        static XidLsn _xid;

    protected:
        XidLsn _next_xid();
        XidLsn _next_lsn();

        void _finalize();
        PgMsgTable _create_table(uint64_t tid, const std::string &name);
        void _drop_table(uint64_t tid, const std::string &name);
        void _alter_table(const PgMsgTable &msg);

        sys_tbl_mgr::Client *_client = sys_tbl_mgr::Client::get_instance();
        uint64_t _db = 1;
    };

    test::Services SysTblMgr_Test::_services(true, true, false);
    XidLsn SysTblMgr_Test::_xid(1, 0);
    std::mutex SysTblMgr_Test::_mutex;

    XidLsn
    SysTblMgr_Test::_next_xid()
    {
        std::unique_lock lock(_mutex);
        XidLsn xid = _xid;

        ++_xid.xid;
        _xid.lsn = 0;

        return xid;
    }

    XidLsn
    SysTblMgr_Test::_next_lsn()
    {
        std::unique_lock lock(_mutex);
        XidLsn xid = _xid;

        ++_xid.lsn;

        return xid;
    }

    void
    SysTblMgr_Test::_finalize()
    {
        auto xid = _next_xid();

        // finalize
        _client->finalize(_db, xid.xid);
    }

    PgMsgTable
    SysTblMgr_Test::_create_table(uint64_t tid,
                                  const std::string &name)
    {
        auto xid = _next_lsn();

        PgMsgTable create_msg;
        create_msg.oid = tid;
        create_msg.schema = "public";
        create_msg.table = name;
        create_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 1, 0, false, true});
        create_msg.columns.push_back({"col2", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 2, 0, true, false});

        _client->create_table(_db, xid, create_msg);

        return create_msg;
    }

    void
    SysTblMgr_Test::_drop_table(uint64_t tid,
                                const std::string &name)
    {
        auto xid = _next_lsn();

        // drop the table
        PgMsgDropTable drop_msg;
        drop_msg.oid = tid;
        drop_msg.schema = "public";
        drop_msg.table = name;

        _client->drop_table(_db, xid, drop_msg);
    }

    void
    SysTblMgr_Test::_alter_table(const PgMsgTable &msg)
    {
        auto xid = _next_lsn();
        _client->alter_table(_db, xid, msg);
    }

    // Tests the schema modification paths
    TEST_F(SysTblMgr_Test, Basic) {
        _client->ping();
    }

    // Tests table create / alter / drop
    TEST_F(SysTblMgr_Test, CreateAlterDrop) {
        uint64_t tid = 100000;

        // create the table
        PgMsgTable &&msg = _create_table(tid, "x");

        // rename col2 => colnew
        msg.columns[1].column_name = "colnew";
        _alter_table(msg);

        // verify system table correctness before finalize
        auto &&metadata = _client->get_roots(_db, tid, 0);
        ASSERT_EQ(metadata.roots.size(), 0);
        ASSERT_EQ(metadata.stats.row_count, 0);

        auto &&schema_meta = _client->get_schema(_db, tid, { 0, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 0);

        schema_meta = _client->get_schema(_db, tid, { 1, 0 });
        ASSERT_EQ(schema_meta.columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, { 1, 1 });
        ASSERT_EQ(schema_meta.columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, { 1, 3 });
        ASSERT_EQ(schema_meta.columns.size(), 2);

        // verify correctness after finalize
        _finalize();

        auto exists = _client->exists(_db, tid, { 1, 0 });
        ASSERT_TRUE(exists);

        metadata = _client->get_roots(_db, tid, 1);
        ASSERT_EQ(metadata.roots.size(), 1);
        ASSERT_EQ(metadata.roots[0], constant::UNKNOWN_EXTENT);
        ASSERT_EQ(metadata.stats.row_count, 0);

        schema_meta = _client->get_schema(_db, tid, { 0, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 0);

        schema_meta = _client->get_schema(_db, tid, { 1, 0 });
        ASSERT_EQ(schema_meta.columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, { 1, 1 });
        ASSERT_EQ(schema_meta.columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, { 1, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 2);

        // drop the table
        _drop_table(tid, "x");

        // verify system table correctness before finalize
        metadata = _client->get_roots(_db, tid, 1);
        ASSERT_EQ(metadata.roots.size(), 1);
        ASSERT_EQ(metadata.roots[0], constant::UNKNOWN_EXTENT);
        ASSERT_EQ(metadata.stats.row_count, 0);

        schema_meta = _client->get_schema(_db, tid, { 0, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 0);

        schema_meta = _client->get_schema(_db, tid, { 1, 1 });
        ASSERT_EQ(schema_meta.columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, { 1, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, { 2, 0 });
        ASSERT_EQ(schema_meta.columns.size(), 0);

        // verify correctness after finalize
        _finalize();

        schema_meta = _client->get_schema(_db, tid, { 0, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 0);

        schema_meta = _client->get_schema(_db, tid, { 1, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, { 2, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 0);

        metadata = _client->get_roots(_db, tid, 1);
        ASSERT_EQ(metadata.roots.size(), 1);
        ASSERT_EQ(metadata.roots[0], constant::UNKNOWN_EXTENT);
        ASSERT_EQ(metadata.stats.row_count, 0);

        metadata = _client->get_roots(_db, tid, 2);
        ASSERT_EQ(metadata.roots.size(), 0);
        ASSERT_EQ(metadata.stats.row_count, 0);
    }

    // Tests interleaving of DDL and DML interactions with the system tables
    TEST_F(SysTblMgr_Test, Complex) {
        uint64_t check_xid = _xid.xid;
        uint64_t tid = 100001;

        // create table
        PgMsgTable &&msg = _create_table(tid, "x");

        // "add data" to the table
        _client->update_roots(_db, tid, _xid.xid, { 0 }, 15);
        _finalize();

        // add more data to the table
        _client->update_roots(_db, tid, _xid.xid, { 100 }, 30);
        _finalize();

        // rename col2 => coltwo
        msg.columns[1].column_name = "coltwo";
        _alter_table(msg);

        _finalize();

        // add a column col3
        msg.columns.push_back({"col3", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 3, 0, true, false});
        _alter_table(msg);

        // rename the table x => y
        msg.table = "y";
        _alter_table(msg);

        // set default value for col3
        msg.columns[2].default_value = "0";
        msg.columns[2].is_nullable = false;
        _alter_table(msg);

        // verify the virtual schema creation from the cache prior to finalize
        auto &&schema_check = _client->get_target_schema(_db, tid, { _xid.xid - 1, constant::MAX_LSN }, _xid);
        ASSERT_EQ(schema_check.history.size(), 2);
        ASSERT_EQ(schema_check.history[0].update_type, SchemaUpdateType::NEW_COLUMN);
        ASSERT_EQ(schema_check.history[1].update_type, SchemaUpdateType::NULLABLE_CHANGE);

        _finalize();

        // drop the table
        _drop_table(tid, "y");
        _finalize();

        // verify the data at each step

        // XID 0
        auto &&schema_meta = _client->get_schema(_db, tid, { check_xid - 1, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 0);

        // XID 1
        auto &&metadata = _client->get_roots(_db, tid, check_xid);
        ASSERT_EQ(metadata.roots.size(), 1);
        ASSERT_EQ(metadata.roots[0], 0);
        ASSERT_EQ(metadata.stats.row_count, 15);

        schema_meta = _client->get_schema(_db, tid, { check_xid, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 2);
        ASSERT_EQ(schema_meta.columns[0].name, "col1");
        ASSERT_EQ(schema_meta.columns[1].name, "col2");

        // XID 2
        ++check_xid;

        metadata = _client->get_roots(_db, tid, check_xid);
        ASSERT_EQ(metadata.roots.size(), 1);
        ASSERT_EQ(metadata.roots[0], 100);
        ASSERT_EQ(metadata.stats.row_count, 30);

        schema_meta = _client->get_schema(_db, tid, { check_xid, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 2);
        ASSERT_EQ(schema_meta.columns[0].name, "col1");
        ASSERT_EQ(schema_meta.columns[1].name, "col2");

        // XID 3
        ++check_xid;

        metadata = _client->get_roots(_db, tid, check_xid);
        ASSERT_EQ(metadata.roots.size(), 1);
        ASSERT_EQ(metadata.roots[0], 100);
        ASSERT_EQ(metadata.stats.row_count, 30);

        schema_meta = _client->get_schema(_db, tid, { check_xid, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 2);
        ASSERT_EQ(schema_meta.columns[0].name, "col1");
        ASSERT_EQ(schema_meta.columns[1].name, "coltwo");

        // XID 4
        ++check_xid;

        metadata = _client->get_roots(_db, tid, check_xid);
        ASSERT_EQ(metadata.roots.size(), 1);
        ASSERT_EQ(metadata.roots[0], 100);
        ASSERT_EQ(metadata.stats.row_count, 30);

        schema_meta = _client->get_schema(_db, tid, { check_xid, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 3);
        ASSERT_EQ(schema_meta.columns[0].name, "col1");
        ASSERT_EQ(schema_meta.columns[1].name, "coltwo");
        ASSERT_EQ(schema_meta.columns[2].name, "col3");

        // XID 5
        ++check_xid;

        metadata = _client->get_roots(_db, tid, check_xid);
        ASSERT_EQ(metadata.roots.size(), 0);
        ASSERT_EQ(metadata.stats.row_count, 0);

        schema_meta = _client->get_schema(_db, tid, { check_xid, constant::MAX_LSN });
        ASSERT_EQ(schema_meta.columns.size(), 0);

        // verify the virtual schema creation at various combinations of access and target XID
        XidLsn access_xid(check_xid - 4);
        XidLsn target_xid(check_xid);
        schema_meta = _client->get_target_schema(_db, tid, access_xid, target_xid);

        ASSERT_EQ(schema_meta.columns.size(), 2);
        ASSERT_EQ(schema_meta.columns[0].name, "col1");
        ASSERT_EQ(schema_meta.columns[1].name, "col2");

        ASSERT_EQ(schema_meta.history.size(), 6);

        ASSERT_EQ(schema_meta.history[0].update_type, SchemaUpdateType::REMOVE_COLUMN);
        ASSERT_EQ(schema_meta.history[0].name, "col1");
        ASSERT_EQ(schema_meta.history[0].xid, check_xid);

        ASSERT_EQ(schema_meta.history[1].update_type, SchemaUpdateType::NAME_CHANGE);
        ASSERT_EQ(schema_meta.history[1].name, "coltwo");
        ASSERT_EQ(schema_meta.history[1].xid, check_xid - 2);

        ASSERT_EQ(schema_meta.history[2].update_type, SchemaUpdateType::REMOVE_COLUMN);
        ASSERT_EQ(schema_meta.history[2].name, "coltwo");
        ASSERT_EQ(schema_meta.history[2].xid, check_xid);

        ASSERT_EQ(schema_meta.history[3].update_type, SchemaUpdateType::NEW_COLUMN);
        ASSERT_EQ(schema_meta.history[3].name, "col3");
        ASSERT_EQ(schema_meta.history[3].xid, check_xid - 1);
        ASSERT_EQ(schema_meta.history[3].lsn, 0);

        ASSERT_EQ(schema_meta.history[4].update_type, SchemaUpdateType::NULLABLE_CHANGE);
        ASSERT_EQ(schema_meta.history[4].name, "col3");
        ASSERT_EQ(schema_meta.history[4].xid, check_xid - 1);
        ASSERT_EQ(schema_meta.history[4].lsn, 2);

        ASSERT_EQ(schema_meta.history[5].update_type, SchemaUpdateType::REMOVE_COLUMN);
        ASSERT_EQ(schema_meta.history[5].name, "col3");
        ASSERT_EQ(schema_meta.history[5].xid, check_xid);
    }

    // Threaded test with interleaving of DDL and DML interactions with the system tables along with
    // metadata retrievals
    TEST_F(SysTblMgr_Test, Threaded) {
        // initialize some schema history for two tables
        std::vector<uint64_t> tids = { 100000, 200000 };

        auto t1msg = _create_table(tids[0], "x");
        auto t2msg = _create_table(tids[1], "y");

        _finalize();

        auto on_completion = [this]() noexcept {
            _finalize();
        };

        std::barrier sync_point(tids.size() * 2, on_completion);
        int loop_count = 50;

        auto work_fn = [&](uint64_t tid, PgMsgTable &msg) {
            for (int i = 0; i < loop_count; i++) {
                msg.columns.push_back({"col3", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 3, 0, true, false});
                _alter_table(msg);

                msg.columns.back().column_name = "colthree";
                _alter_table(msg);

                msg.columns.back().is_nullable = false;
                msg.columns.back().default_value = "0";
                _alter_table(msg);

                msg.columns.back().column_name = "colIII";
                _alter_table(msg);

                msg.columns.pop_back();
                _alter_table(msg);

                sync_point.arrive_and_wait();
            }
        };

        auto get_fn = [&](uint64_t tid) {
            for (int i = 0; i < loop_count; i++) {
                XidLsn access_xid(_xid.xid - 1);

                auto &&result = _client->get_schema(_db, tid, access_xid);
                for (int j = 0; j < 10; ++j) {
                    auto &&result = _client->get_target_schema(_db, tid, access_xid, { _xid.xid, std::max(_xid.lsn - 1, (uint64_t)0) });
                }

                sync_point.arrive_and_wait();
            }
        };

        // construct two threads that will operate on the two tables concurrently and two that will
        // continuously retrieve the schema
        std::vector<std::thread> threads;
        threads.reserve(4);
        threads.emplace_back(std::thread([&]() {
            work_fn(tids[0], t1msg);
        }));
        threads.emplace_back(std::thread([&]() {
            work_fn(tids[1], t2msg);
        }));
        threads.emplace_back(std::thread([&]() {
            get_fn(tids[0]);
        }));
        threads.emplace_back(std::thread([&]() {
            get_fn(tids[1]);
        }));

        for (auto &thread : threads) {
            thread.join();
        }
    }
}
