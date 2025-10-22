/*
 * Tests the interfaces of the SysTblMgr service.
 */
#include <algorithm>
#include <barrier>

#include <postgresql/server/catalog/pg_type_d.h>

#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/constants.hh>
#include <common/filesystem.hh>
#include <common/json.hh>
#include <common/object_cache.hh>
#include <common/properties.hh>
#include <common/threaded_test.hh>

#include <storage/schema.hh>
#include <storage/vacuumer.hh>
#include <storage/xid.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/shm_cache.hh>
#include <sys_tbl_mgr/table.hh>

#include <test/services.hh>

#include <xid_mgr/xid_mgr_server.hh>

using namespace springtail;

namespace {
    /**
     * Framework for Table and MutableTable testing.
     */
    class SysTblMgr_Test : public testing::Test {
    public:
        static void SetUpTestSuite() {
            springtail_init_test(LOG_ALL ^ (LOG_CACHE | LOG_STORAGE));
            test::start_services(true, true, false);

            // create the public namespace
            auto server = sys_tbl_mgr::Server::get_instance();

            // create the public namespace in the sys_tbl_mgr
            PgMsgNamespace ns_msg;
            ns_msg.oid = 90000;
            ns_msg.name = "public";
            server->create_namespace(1, _xid, ns_msg);

            // move to the next XID
            ++_xid.xid;
            _xid.lsn = 0;
        }

        static void TearDownTestSuite() {
            springtail_shutdown();
        }

        static std::mutex _mutex;
        static XidLsn _xid;
        static uint64_t _db;

    protected:
        XidLsn _next_xid();
        XidLsn _next_lsn();

        void _finalize();
        PgMsgTable _create_table(uint64_t tid, const std::string &name);
        void _verify_schema(uint64_t tid, uint64_t index_id);
        void _drop_table(uint64_t tid, const std::string &name);
        void _alter_table(const PgMsgTable &msg);
        PgMsgIndex _create_index(uint64_t tid, const std::string& name, uint64_t index_id);
        PgMsgDropIndex _drop_index(uint64_t index_id);
        void _set_index_state(uint64_t table_id, uint64_t index_id, sys_tbl::IndexNames::State state);
        std::filesystem::path _get_system_table_dir(uint64_t system_table_id);

        sys_tbl_mgr::Client *_client = sys_tbl_mgr::Client::get_instance();
        sys_tbl_mgr::Server *_server = sys_tbl_mgr::Server::get_instance();
    };

    XidLsn SysTblMgr_Test::_xid(1, 0);
    std::mutex SysTblMgr_Test::_mutex;
    uint64_t SysTblMgr_Test::_db = 1;

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
        _server->finalize(_db, xid.xid);
    }

    void SysTblMgr_Test::_set_index_state(uint64_t table_id, uint64_t index_id, sys_tbl::IndexNames::State state)
    {
        auto xid = _next_lsn();
        _server->set_index_state(_db, xid, table_id, index_id, state);
        _client->invalidate_table(_db, table_id, xid);
    }

    std::filesystem::path
    SysTblMgr_Test::_get_system_table_dir(uint64_t system_table_id)
    {
        // Get the table directory from properties
        nlohmann::json storage_config = Properties::get(Properties::STORAGE_CONFIG);
        std::filesystem::path table_dir;
        Json::get_to<std::filesystem::path>(storage_config, "table_dir", table_dir);
        table_dir = Properties::make_absolute_path(table_dir);

        // System tables use snapshot_xid = 1
        // Directory format: {table_dir}/{db_id}/{table_id}-{snapshot_xid}
        return table_helpers::get_table_dir(table_dir, _db, system_table_id, 1);
    }

    PgMsgDropIndex SysTblMgr_Test::_drop_index(uint64_t index_id)
    {
        auto xid = _next_lsn();

        PgMsgDropIndex msg{};

        msg.lsn = xid.lsn;
        msg.xid = xid.xid;
        msg.namespace_name = "public";
        msg.oid = index_id;

        _server->drop_index(_db, xid, msg);
        _client->invalidate_by_index(_db, msg.oid, xid);

        return msg;
    }

    PgMsgIndex SysTblMgr_Test::_create_index(uint64_t tid, const std::string& name, uint64_t index_id) {
        auto xid = _next_lsn();

        std::vector<PgMsgSchemaIndexColumn> columns;
        PgMsgIndex msg{};

        msg.lsn = xid.lsn;
        msg.xid = xid.xid;
        msg.namespace_name = "public";
        msg.index = name;
        msg.is_unique = true;
        msg.table_oid = tid;
        msg.oid = index_id;

        msg.columns.push_back({"col2", 2, 0});
        msg.columns.push_back({"col1", 1, 1});

        _server->create_index(_db, xid, msg, sys_tbl::IndexNames::State::NOT_READY);
        _client->invalidate_table(_db, msg.table_oid, xid);

        return msg;
    }

    PgMsgTable
    SysTblMgr_Test::_create_table(uint64_t tid,
                                  const std::string &name)
    {
        auto xid = _next_lsn();

        PgMsgTable create_msg{};
        create_msg.oid = tid;
        create_msg.namespace_name = "public";
        create_msg.table = name;
        create_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), TEXTOID, "foo", 1, 0, false, true});
        create_msg.columns.push_back({"col2", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 2, 0, true, false});

        _server->create_table(_db, xid, create_msg);

        return create_msg;
    }

    void
    SysTblMgr_Test::_verify_schema(uint64_t tid, uint64_t index_id)
    {
        auto schema_meta = _client->get_schema(_db, tid, _xid);
        // Must have primary, look-aside and the index
        ASSERT_EQ(schema_meta->indexes.size(), 3);

        auto it = std::ranges::find_if(schema_meta->indexes,
                               [&](const auto& index) { return index.id == index_id; });

        ASSERT_NE(it, schema_meta->indexes.end());

        auto idx = *it;
        ASSERT_EQ(idx.columns.size(), 2);
        ASSERT_EQ(idx.state, (uint8_t)sys_tbl::IndexNames::State::NOT_READY);

        // note: column positions start with 1
        ASSERT_EQ(idx.columns[0].idx_position, 0);
        ASSERT_EQ(idx.columns[0].position, 2);

        ASSERT_EQ(idx.columns[1].idx_position, 1);
        ASSERT_EQ(idx.columns[1].position, 1);

        ASSERT_EQ(idx.id, index_id);
    }

    void
    SysTblMgr_Test::_drop_table(uint64_t tid,
                                const std::string &name)
    {
        auto xid = _next_lsn();

        // drop the table
        PgMsgDropTable drop_msg{};
        drop_msg.oid = tid;
        drop_msg.namespace_name = "public";
        drop_msg.table = name;

        _server->drop_table(_db, xid, drop_msg);
        _client->invalidate_table(_db, drop_msg.oid, xid);
    }

    void
    SysTblMgr_Test::_alter_table(const PgMsgTable &msg)
    {
        auto xid = _next_lsn();
        _server->alter_table(_db, xid, msg);
        _client->invalidate_table(_db, msg.oid, xid);
    }

    // Tests the schema modification paths
    TEST_F(SysTblMgr_Test, Basic) {
        _client->ping();
        _finalize();
    }

    // Tests index create
    TEST_F(SysTblMgr_Test, CreateIndex) {
        uint64_t tid = 100003;
        uint64_t index_id = 5000;

        // create the table
        _create_table(tid, "x");
        auto &&schema_meta = _client->get_schema(_db, tid, _xid);
        // should get the cached (hasn't been finalized) primary index and look aside index
        ASSERT_EQ(schema_meta->indexes.size(), 2);
        ASSERT_EQ(schema_meta->indexes[0].columns.size(), 1);
        ASSERT_EQ(schema_meta->indexes[0].state, (uint8_t)sys_tbl::IndexNames::State::READY);
        ASSERT_EQ(schema_meta->indexes[1].columns.size(), 1);
        ASSERT_EQ(schema_meta->indexes[1].state, (uint8_t)sys_tbl::IndexNames::State::READY);
        _finalize();

        schema_meta = _client->get_schema(_db, tid, _xid);
        // must have a primary index
        ASSERT_EQ(schema_meta->indexes.size(), 2);
        ASSERT_EQ(schema_meta->indexes[0].columns.size(), 1);
        ASSERT_EQ(schema_meta->indexes[1].columns.size(), 1);

        PgMsgIndex &&msg = _create_index(tid, "x", index_id);

        schema_meta = _client->get_schema(_db, tid, _xid);
        auto it = std::ranges::find_if(schema_meta->indexes,
                [&](const auto& index) { return index.id == index_id; });
        ASSERT_NE(it, schema_meta->indexes.end());
        auto idx = *it;

        ASSERT_EQ(schema_meta->indexes.size(), 3);

        it = std::ranges::find_if(schema_meta->indexes,
                [&](const auto& index) { return index.id == constant::INDEX_PRIMARY; });
        ASSERT_NE(it, schema_meta->indexes.end());
        auto primary_idx = *it;

        it = std::ranges::find_if(schema_meta->indexes,
                [&](const auto& index) { return index.id == constant::INDEX_LOOK_ASIDE; });
        ASSERT_NE(it, schema_meta->indexes.end());
        auto la_idx = *it;


        ASSERT_EQ(primary_idx.columns.size(), 1);
        ASSERT_EQ(la_idx.columns.size(), 1);
        ASSERT_EQ(idx.state, (uint8_t)sys_tbl::IndexNames::State::NOT_READY);
        ASSERT_EQ(idx.columns[0].idx_position, 0);
        ASSERT_EQ(idx.columns[0].position, 2);
        ASSERT_EQ(idx.columns[1].idx_position, 1);
        ASSERT_EQ(idx.columns[1].position, 1);

        _set_index_state(tid, index_id, sys_tbl::IndexNames::State::READY);
        schema_meta = _client->get_schema(_db, tid, _xid);
        it = std::ranges::find_if(schema_meta->indexes,
                [&](const auto& index) { return index.id == index_id; });
        ASSERT_NE(it, schema_meta->indexes.end());
        idx = *it;
        ASSERT_EQ(idx.state, (uint8_t)sys_tbl::IndexNames::State::READY);

        _set_index_state(tid, index_id, sys_tbl::IndexNames::State::NOT_READY);

        auto info = _server->get_index_info(_db, index_id, _xid);
        ASSERT_EQ(info.id(), index_id);
        ASSERT_EQ(idx.state, (uint8_t)sys_tbl::IndexNames::State::READY);

        _finalize();

        // verify the schema metadata after finalize()
        _verify_schema(tid, index_id);

        info = _server->get_index_info(_db, index_id, _xid);
        ASSERT_EQ(info.id(), index_id);

        // use optional table ID
        info = _server->get_index_info(_db, index_id, _xid, tid);
        ASSERT_EQ(info.id(), index_id);

        info = _server->get_index_info(_db, constant::INDEX_PRIMARY, _xid, tid);
        ASSERT_EQ(info.id(), constant::INDEX_PRIMARY);

        // change the index to the ready state
        _set_index_state(tid, index_id, sys_tbl::IndexNames::State::READY);
        _finalize();
        schema_meta = _client->get_schema(_db, tid, _xid);
        ASSERT_EQ(schema_meta->indexes.size(), 3);

        it = std::ranges::find_if(schema_meta->indexes,
                [&](const auto& index) { return index.id == index_id; });
        ASSERT_NE(it, schema_meta->indexes.end());
        idx = *it;

        ASSERT_EQ(idx.columns.size(), 2);
        ASSERT_EQ(idx.state, (uint8_t)sys_tbl::IndexNames::State::READY);
        ASSERT_EQ(idx.columns[0].idx_position, 0);
        ASSERT_EQ(idx.columns[0].position, 2);
        ASSERT_EQ(idx.columns[1].idx_position, 1);
        ASSERT_EQ(idx.columns[1].position, 1);

        // delete the index
        _set_index_state(tid, index_id, sys_tbl::IndexNames::State::DELETED);
        _finalize();
        schema_meta = _client->get_schema(_db, tid, _xid);
        ASSERT_EQ(schema_meta->indexes.size(), 2);
    }

    // Tests index drop
    TEST_F(SysTblMgr_Test, DropIndex)
    {
        uint64_t tid = 100004;
        uint64_t index_id = 5001;

        // create the table
        _create_table(tid, "x");
        _finalize();

        auto &&schema_meta = _client->get_schema(_db, tid, _xid);

        // must have a primary index and look-aside index
        ASSERT_EQ(schema_meta->indexes.size(), 2);

        // Both primary and look aside will have single column
        // Look aside entry has dummy column as its based on internal_row_id
        ASSERT_EQ(schema_meta->indexes[0].columns.size(), 1);
        ASSERT_EQ(schema_meta->indexes[1].columns.size(), 1);

        PgMsgIndex &&msg = _create_index(tid, "x", index_id);
        _finalize();

        _verify_schema(tid, index_id);

        schema_meta = _client->get_schema(_db, tid, _xid);
        ASSERT_EQ(schema_meta->indexes.size(), 3);

        // drop index
        _drop_index(index_id);

        schema_meta = _client->get_schema(_db, tid, _xid);

        // must have a primary index and deleted index
        // as the deleted index will still be present with
        // the state BEING_DELETED
        //
        ASSERT_EQ(schema_meta->indexes.size(), 3);

        auto it = std::ranges::find_if(schema_meta->indexes,
                               [&](const auto& index) { return index.id == index_id; });

        ASSERT_TRUE(it != schema_meta->indexes.end());
        ASSERT_EQ(it->state, (uint8_t)sys_tbl::IndexNames::State::BEING_DELETED);

        _finalize();

        schema_meta = _client->get_schema(_db, tid, _xid);
        ASSERT_EQ(schema_meta->indexes.size(), 3);
        it = std::ranges::find_if(schema_meta->indexes,
                               [&](const auto& index) { return index.id == index_id; });

        ASSERT_TRUE(it != schema_meta->indexes.end());
        ASSERT_EQ(it->state, (uint8_t)sys_tbl::IndexNames::State::BEING_DELETED);
    }

    // Tests table create / alter / drop
    TEST_F(SysTblMgr_Test, CreateAlterDrop)
    {
        uint64_t tid = 100000;
        uint64_t start_xid = _xid.xid;

        // create the table
        PgMsgTable &&msg = _create_table(tid, "x");

        // rename col2 => colnew
        msg.columns[1].name = "colnew";
        _alter_table(msg);

        // verify system table correctness before finalize
        auto &&metadata = _client->get_roots(_db, tid, start_xid - 1);
        ASSERT_EQ(metadata->roots.size(), 0);
        ASSERT_EQ(metadata->stats.row_count, 0);

        auto &&schema_meta = _client->get_schema(_db, tid, {start_xid - 1, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 0);

        schema_meta = _client->get_schema(_db, tid, {start_xid, 0});
        ASSERT_EQ(schema_meta->columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, {start_xid, 1});
        ASSERT_EQ(schema_meta->columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, {start_xid, 3});
        ASSERT_EQ(schema_meta->columns.size(), 2);

        // verify correctness after finalize
        _finalize();

        auto exists = _client->exists(_db, tid, {start_xid, 0});
        ASSERT_TRUE(exists);

        metadata = _client->get_roots(_db, tid, start_xid);

        // Roots for both Primary and look aside index
        ASSERT_EQ(metadata->roots.size(), 2);
        ASSERT_EQ(metadata->roots[0].extent_id, constant::UNKNOWN_EXTENT);
        ASSERT_EQ(metadata->roots[1].extent_id, constant::UNKNOWN_EXTENT);
        ASSERT_EQ(metadata->stats.row_count, 0);

        schema_meta = _client->get_schema(_db, tid, {start_xid - 1, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 0);

        schema_meta = _client->get_schema(_db, tid, {start_xid, 0});
        ASSERT_EQ(schema_meta->columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, {start_xid, 1});
        ASSERT_EQ(schema_meta->columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, {start_xid, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 2);

        // drop the table
        _drop_table(tid, "x");

        // verify system table correctness before finalize
        metadata = _client->get_roots(_db, tid, start_xid);
        ASSERT_EQ(metadata->roots.size(), 2);
        ASSERT_EQ(metadata->roots[0].extent_id, constant::UNKNOWN_EXTENT);
        ASSERT_EQ(metadata->roots[1].extent_id, constant::UNKNOWN_EXTENT);
        ASSERT_EQ(metadata->stats.row_count, 0);

        schema_meta = _client->get_schema(_db, tid, {start_xid - 1, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 0);

        schema_meta = _client->get_schema(_db, tid, {start_xid, 1});
        ASSERT_EQ(schema_meta->columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, {start_xid, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, {start_xid + 1, 0});
        ASSERT_EQ(schema_meta->columns.size(), 0);

        // verify correctness after finalize
        _finalize();

        schema_meta = _client->get_schema(_db, tid, {start_xid - 1, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 0);

        schema_meta = _client->get_schema(_db, tid, {start_xid, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 2);

        schema_meta = _client->get_schema(_db, tid, {start_xid + 1, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 0);

        metadata = _client->get_roots(_db, tid, start_xid);
        ASSERT_EQ(metadata->roots.size(), 2);
        ASSERT_EQ(metadata->roots[0].extent_id, constant::UNKNOWN_EXTENT);
        ASSERT_EQ(metadata->roots[1].extent_id, constant::UNKNOWN_EXTENT);
        ASSERT_EQ(metadata->stats.row_count, 0);

        metadata = _client->get_roots(_db, tid, start_xid + 1);
        ASSERT_EQ(metadata->roots.size(), 0);
        ASSERT_EQ(metadata->stats.row_count, 0);
    }

    // Tests interleaving of DDL and DML interactions with the system tables
    TEST_F(SysTblMgr_Test, Complex)
    {
        uint64_t check_xid = _xid.xid;
        uint64_t tid = 100001;
        uint64_t index_id = 5003;

        sys_tbl_mgr::ShmCache::remove(sys_tbl_mgr::SHM_CACHE_ROOTS);
        auto cache = std::make_shared<sys_tbl_mgr::ShmCache>(sys_tbl_mgr::SHM_CACHE_ROOTS, 100*1024);
        _client->use_roots_cache(cache);

        // create table
        PgMsgTable &&msg = _create_table(tid, "x");

        // "add data" to the table
        _create_index(tid, "x", index_id);
        _server->update_roots(_db, tid, _xid.xid, {{{0, 0}}, {15}});
        _finalize();

        // add more data to the table
        _server->update_roots(_db, tid, _xid.xid, {{{0, 100}}, {30}});
        _finalize();

        // rename col2 => coltwo
        msg.columns[1].name = "coltwo";
        _alter_table(msg);

        _finalize();

        // add a column col3
        msg.columns.push_back({"colthree", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt,
                               3, 0, true, false});
        _alter_table(msg);

        // rename the table x => y
        msg.table = "y";
        _alter_table(msg);

        // set change the name of column 3
        msg.columns[2].name = "col3";
        _alter_table(msg);

        // verify the virtual schema creation from the cache prior to finalize
        _next_lsn();
        auto schema_check =
            _client->get_target_schema(_db, tid, {_xid.xid - 1, constant::MAX_LSN}, _xid);
#if ENABLE_SCHEMA_MUTATES
        ASSERT_EQ(schema_check->history.size(), 2);
        ASSERT_EQ(schema_check->history[0].update_type, SchemaUpdateType::NEW_COLUMN);
        ASSERT_EQ(schema_check->history[1].update_type, SchemaUpdateType::NAME_CHANGE);
#else
        ASSERT_EQ(schema_check->history.size(), 0);
#endif

        _finalize();

        // drop the table
        _drop_table(tid, "y");
        _finalize();


        // verify the data at each step

        // XID 0
        auto &&schema_meta = _client->get_schema(_db, tid, {check_xid - 1, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 0);

        // XID 1
        auto &&metadata = _client->get_roots(_db, tid, check_xid);
        ASSERT_EQ(metadata->roots.size(), 2);
        auto root_it = std::ranges::find_if(metadata->roots,
                [&](const auto& index_root) { return index_root.index_id == constant::INDEX_PRIMARY; });
        ASSERT_NE(root_it, metadata->roots.end());

        ASSERT_EQ(root_it->index_id, 0);
        ASSERT_EQ(metadata->stats.row_count, 15);

        ASSERT_EQ(cache->size(), 1);
        auto cached_msg = cache->find(_db, tid, check_xid);
        ASSERT_TRUE(cached_msg);


        schema_meta = _client->get_schema(_db, tid, {check_xid, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 2);
        ASSERT_EQ(schema_meta->columns[0].name, "col1");
        ASSERT_EQ(schema_meta->columns[1].name, "col2");
        // Includes primary, look-aside and secondary index
        ASSERT_EQ(schema_meta->indexes.size(), 3);

        // XID 2
        ++check_xid;

        metadata = _client->get_roots(_db, tid, check_xid);
        ASSERT_EQ(metadata->roots.size(), 2);

        root_it = std::ranges::find_if(metadata->roots,
                [&](const auto& index_root) { return index_root.index_id == constant::INDEX_PRIMARY; });
        ASSERT_NE(root_it, metadata->roots.end());

        ASSERT_EQ(root_it->extent_id, 100);
        ASSERT_EQ(metadata->stats.row_count, 30);

        ASSERT_EQ(cache->size(), 2);
        cached_msg = cache->find(_db, tid, check_xid);
        ASSERT_TRUE(cached_msg);

        schema_meta = _client->get_schema(_db, tid, {check_xid, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 2);
        ASSERT_EQ(schema_meta->columns[0].name, "col1");
        ASSERT_EQ(schema_meta->columns[1].name, "col2");
        ASSERT_EQ(schema_meta->indexes.size(), 3);

        // remove the cache now
        _client->use_roots_cache({});
        cache.reset();

        // XID 3
        ++check_xid;

        metadata = _client->get_roots(_db, tid, check_xid);
        ASSERT_EQ(metadata->roots.size(), 2);
        root_it = std::ranges::find_if(metadata->roots,
                [&](const auto& index_root) { return index_root.index_id == constant::INDEX_PRIMARY; });
        ASSERT_NE(root_it, metadata->roots.end());
        ASSERT_EQ(root_it->extent_id, 100);
        ASSERT_EQ(metadata->stats.row_count, 30);

        schema_meta = _client->get_schema(_db, tid, {check_xid, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 2);
        ASSERT_EQ(schema_meta->columns[0].name, "col1");
        ASSERT_EQ(schema_meta->columns[1].name, "coltwo");
        ASSERT_EQ(schema_meta->indexes.size(), 3);

        // XID 4
        ++check_xid;

        metadata = _client->get_roots(_db, tid, check_xid);
        ASSERT_EQ(metadata->roots.size(), 2);
        root_it = std::ranges::find_if(metadata->roots,
                [&](const auto& index_root) { return index_root.index_id == constant::INDEX_PRIMARY; });
        ASSERT_NE(root_it, metadata->roots.end());
        ASSERT_EQ(root_it->extent_id, 100);
        ASSERT_EQ(metadata->stats.row_count, 30);

        schema_meta = _client->get_schema(_db, tid, {check_xid, constant::MAX_LSN});
#if ENABLE_SCHEMA_MUTATES
        ASSERT_EQ(schema_meta->columns.size(), 3);
        ASSERT_EQ(schema_meta->columns[0].name, "col1");
        ASSERT_EQ(schema_meta->columns[1].name, "coltwo");
        ASSERT_EQ(schema_meta->columns[2].name, "col3");
        ASSERT_EQ(schema_meta->indexes.size(), 3);
#else
        ASSERT_EQ(schema_meta->columns.size(), 2);
        ASSERT_EQ(schema_meta->columns[0].name, "col1");
        ASSERT_EQ(schema_meta->columns[1].name, "coltwo");
        ASSERT_EQ(schema_meta->indexes.size(), 3);
#endif

        // XID 5
        ++check_xid;

        metadata = _client->get_roots(_db, tid, check_xid);
        ASSERT_EQ(metadata->roots.size(), 0);
        ASSERT_EQ(metadata->stats.row_count, 0);

        schema_meta = _client->get_schema(_db, tid, {check_xid, constant::MAX_LSN});
        ASSERT_EQ(schema_meta->columns.size(), 0);
        // BEING_DELETED index will be present in the schema
        ASSERT_EQ(schema_meta->indexes.size(), 1);
        auto it = std::ranges::find_if(schema_meta->indexes,
                               [&](const auto& index) { return index.id == index_id; });

        ASSERT_TRUE(it != schema_meta->indexes.end());
        ASSERT_EQ(it->state, (uint8_t)sys_tbl::IndexNames::State::BEING_DELETED);



        // verify the virtual schema creation at various combinations of access and target XID
        XidLsn access_xid(check_xid - 4);
        XidLsn target_xid(check_xid);
        schema_meta = _client->get_target_schema(_db, tid, access_xid, target_xid);

        ASSERT_EQ(schema_meta->columns.size(), 2);
        ASSERT_EQ(schema_meta->columns[0].name, "col1");
        ASSERT_EQ(schema_meta->columns[1].name, "col2");

#if ENABLE_SCHEMA_MUTATES
        ASSERT_EQ(schema_meta->history.size(), 6);

        ASSERT_EQ(schema_meta->history[0].update_type, SchemaUpdateType::REMOVE_COLUMN);
        ASSERT_EQ(schema_meta->history[0].name, "col1");
        ASSERT_EQ(schema_meta->history[0].xid, check_xid);

        ASSERT_EQ(schema_meta->history[1].update_type, SchemaUpdateType::NAME_CHANGE);
        ASSERT_EQ(schema_meta->history[1].name, "coltwo");
        ASSERT_EQ(schema_meta->history[1].xid, check_xid - 2);

        ASSERT_EQ(schema_meta->history[2].update_type, SchemaUpdateType::REMOVE_COLUMN);
        ASSERT_EQ(schema_meta->history[2].name, "coltwo");
        ASSERT_EQ(schema_meta->history[2].xid, check_xid);

        ASSERT_EQ(schema_meta->history[3].update_type, SchemaUpdateType::NEW_COLUMN);
        ASSERT_EQ(schema_meta->history[3].name, "colthree");
        ASSERT_EQ(schema_meta->history[3].xid, check_xid - 1);
        ASSERT_EQ(schema_meta->history[3].lsn, 0);

        ASSERT_EQ(schema_meta->history[4].update_type, SchemaUpdateType::NAME_CHANGE);
        ASSERT_EQ(schema_meta->history[4].name, "col3");
        ASSERT_EQ(schema_meta->history[4].xid, check_xid - 1);
        ASSERT_EQ(schema_meta->history[4].lsn, 2);

        ASSERT_EQ(schema_meta->history[5].update_type, SchemaUpdateType::REMOVE_COLUMN);
        ASSERT_EQ(schema_meta->history[5].name, "col3");
        ASSERT_EQ(schema_meta->history[5].xid, check_xid);
#else
        ASSERT_EQ(schema_meta->history.size(), 3);

        ASSERT_EQ(schema_meta->history[0].update_type, SchemaUpdateType::REMOVE_COLUMN);
        ASSERT_EQ(schema_meta->history[0].name, "col1");
        ASSERT_EQ(schema_meta->history[0].xid, check_xid);

        ASSERT_EQ(schema_meta->history[1].update_type, SchemaUpdateType::NAME_CHANGE);
        ASSERT_EQ(schema_meta->history[1].name, "coltwo");
        ASSERT_EQ(schema_meta->history[1].xid, check_xid - 2);

        ASSERT_EQ(schema_meta->history[2].update_type, SchemaUpdateType::REMOVE_COLUMN);
        ASSERT_EQ(schema_meta->history[2].name, "coltwo");
        ASSERT_EQ(schema_meta->history[2].xid, check_xid);
#endif
    }

    // Threaded test with interleaving of DDL and DML interactions with the system tables along with
    // metadata retrievals
    TEST_F(SysTblMgr_Test, Threaded) {
        // initialize some schema history for two tables
        std::vector<uint64_t> tids = { 200000, 200001 };

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

                msg.columns.back().name = "colthree";
                _alter_table(msg);

                msg.columns.back().is_nullable = false;
                msg.columns.back().default_value = "0";
                _alter_table(msg);

                msg.columns.back().name = "colIII";
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

    // Test that the vacuumer properly cleans up old roots files from system tables
    TEST_F(SysTblMgr_Test, VacuumSystemTableRoots)
    {
        // Enable vacuum tracking
        Vacuumer::get_instance()->enable_tracking_extents();

        // XID 1: Create first user table
        // This will update system tables (TableNames, TableRoots, etc.) and create roots.{xid} files
        uint64_t tid1 = 200001;
        _create_table(tid1, "vacuum_test_table_1");
        _finalize();
        uint64_t xid1 = _xid.xid - 1;

        // XID 2: Create second user table
        uint64_t tid2 = 200002;
        _create_table(tid2, "vacuum_test_table_2");
        _finalize();
        uint64_t xid2 = _xid.xid - 1;

        // XID 3: Drop first table
        _drop_table(tid1, "vacuum_test_table_1");
        _finalize();
        uint64_t xid3 = _xid.xid - 1;

        // XID 4: Create third table
        uint64_t tid3 = 200003;
        _create_table(tid3, "vacuum_test_table_3");
        _finalize();
        uint64_t xid4 = _xid.xid - 1;

        // Get the system table directory (using TableNames as our test subject)
        auto table_names_dir = _get_system_table_dir(sys_tbl::TableNames::ID);
        LOG_INFO("System table directory: {}", table_names_dir.string());

        // Verify roots files exist before vacuum
        auto roots_xid1 = table_names_dir / fmt::format("roots.{}", xid1);
        auto roots_xid2 = table_names_dir / fmt::format("roots.{}", xid2);
        auto roots_xid3 = table_names_dir / fmt::format("roots.{}", xid3);
        auto roots_xid4 = table_names_dir / fmt::format("roots.{}", xid4);

        ASSERT_TRUE(std::filesystem::exists(roots_xid1)) << "Expected roots file for XID " << xid1;
        ASSERT_TRUE(std::filesystem::exists(roots_xid2)) << "Expected roots file for XID " << xid2;
        ASSERT_TRUE(std::filesystem::exists(roots_xid3)) << "Expected roots file for XID " << xid3;
        ASSERT_TRUE(std::filesystem::exists(roots_xid4)) << "Expected roots file for XID " << xid4;

        LOG_INFO("All roots files exist before vacuum: {}, {}, {}, {}", xid1, xid2, xid3, xid4);

        // Set cutoff_xid to xid3 - this means xid1 and xid2 should be cleaned up
        uint64_t cutoff_xid = xid3;
        uint64_t current_xid = xid4 + 1;

        // Commit the XID so that XidMgrServer has the correct last_committed_xid
        // This ensures _get_vacuum_cutoff_xid() returns the expected value
        xid_mgr::XidMgrServer::get_instance()->commit_xid(_db, 1, cutoff_xid, false);

        // Trigger vacuum
        Vacuumer::get_instance()->set_global_vacuum_threshold(10);
        Vacuumer::get_instance()->commit_expired_extents(_db, current_xid);
        Vacuumer::get_instance()->run_vacuum_once();

        LOG_INFO("Vacuum completed with cutoff_xid={}", cutoff_xid);

        // Verify old roots files (< cutoff_xid) are deleted
        ASSERT_FALSE(std::filesystem::exists(roots_xid1))
            << "Expected roots file for XID " << xid1 << " to be deleted (< cutoff_xid=" << cutoff_xid << ")";
        ASSERT_FALSE(std::filesystem::exists(roots_xid2))
            << "Expected roots file for XID " << xid2 << " to be deleted (< cutoff_xid=" << cutoff_xid << ")";

        // Verify recent roots files (>= cutoff_xid) remain
        ASSERT_TRUE(std::filesystem::exists(roots_xid3))
            << "Expected roots file for XID " << xid3 << " to remain (>= cutoff_xid=" << cutoff_xid << ")";
        ASSERT_TRUE(std::filesystem::exists(roots_xid4))
            << "Expected roots file for XID " << xid4 << " to remain (>= cutoff_xid=" << cutoff_xid << ")";

        // Verify the roots symlink still exists
        auto roots_symlink = table_names_dir / "roots";
        ASSERT_TRUE(std::filesystem::exists(roots_symlink)) << "Expected roots symlink to exist";

        LOG_INFO("Vacuum test passed: Old roots files deleted, recent ones preserved");

        // Also verify TableRoots system table
        auto table_roots_dir = _get_system_table_dir(sys_tbl::TableRoots::ID);
        LOG_INFO("Checking TableRoots system table directory: {}", table_roots_dir.string());

        // Verify cleanup happened there too
        ASSERT_FALSE(std::filesystem::exists(table_roots_dir / fmt::format("roots.{}", xid1)));
        ASSERT_FALSE(std::filesystem::exists(table_roots_dir / fmt::format("roots.{}", xid2)));
        // note: no roots file for xid3 because it was a table drop
        ASSERT_TRUE(std::filesystem::exists(table_roots_dir / fmt::format("roots.{}", xid4)));

        LOG_INFO("TableRoots system table also cleaned up correctly");
    }
}
