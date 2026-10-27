/*
 * Tests the interfaces of the SysTblMgr service.
 */
#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <future>
#include <thread>

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
#include <sys_tbl_mgr/system_table_mgr_server.hh>
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
        PgMsgIndex _create_index(uint64_t tid, const std::string& name, uint64_t index_id,
                const std::string& index_type = "btree");
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
        _server->finalize(_db, xid.xid, true);
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

    PgMsgIndex SysTblMgr_Test::_create_index(uint64_t tid, const std::string& name, uint64_t index_id,
            const std::string& index_type) {
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
        msg.index_type = index_type;

        msg.columns.push_back({"col2", 2, 0, "int4_ops"});
        msg.columns.push_back({"col1", 1, 1, "text_ops"});

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

    // Tests that index_type (IndexNames) and per-column opclass (Indexes) -- the btree redesign
    // phase 9 index-kind plumbing -- round-trip through the catalog correctly for every access
    // method, and that an IndexInfo with no index_type set (as for a pre-phase-9 row, or the
    // internally-constructed primary/look-aside role indexes) reads back as BTREE.
    TEST_F(SysTblMgr_Test, CreateIndexAccessMethod)
    {
        uint64_t tid = 100005;

        _create_table(tid, "x");
        _finalize();

        struct Case {
            uint64_t index_id;
            std::string requested_type;
            std::string expected_type;
        };
        std::vector<Case> cases = {
            {5010, "btree", "btree"},
            {5011, "gist", "gist"},
            {5012, "gin", "gin"},
        };

        // Indexes rows are keyed by (table_id, index_id, xid, lsn, position) -- scan the real
        // on-disk table directly, pinned at this write's own xid (mirroring FinalizeVsQueueRace's
        // approach), rather than through get_index_info(), which the "This doesn't return
        // information about index columns" doc comment on Server::_find_index confirms never
        // populates columns at all (pre-existing, independent of this phase).
        for (auto const& c : cases) {
            auto &&msg = _create_index(tid, "idx_" + c.expected_type, c.index_id, c.requested_type);
            _finalize();

            // force a read from the on-disk IndexNames table, not the in-memory per-request
            // cache -- _finalize() clears _index_cache for the whole db (see
            // Server::finalize()/_index_cache.erase(db_id)).
            auto info = _server->get_index_info(_db, c.index_id, _xid, tid);
            ASSERT_EQ(info.id(), c.index_id);
            ASSERT_EQ(info.index_type(), c.expected_type);

            auto indexes_table = SystemTableMgrServer::get_instance()->get_table(_db, sys_tbl::Indexes::ID,
                    msg.xid);
            auto indexes_schema = indexes_table->extent_schema();
            auto idx_table_id_f = indexes_schema->get_field("table_id");
            auto idx_index_id_f = indexes_schema->get_field("index_id");
            auto idx_xid_f = indexes_schema->get_field("xid");
            auto idx_lsn_f = indexes_schema->get_field("lsn");
            auto idx_position_f = indexes_schema->get_field("position");
            auto idx_opclass_f = indexes_schema->get_field("opclass");

            std::map<uint32_t, std::string> opclass_by_position;
            auto search_key = sys_tbl::Indexes::Primary::key_tuple(tid, c.index_id, msg.xid, msg.lsn, 0);
            for (auto row_i = indexes_table->lower_bound(search_key); row_i != indexes_table->end();
                    ++row_i) {
                auto& row = *row_i;
                if (idx_table_id_f->get_uint64(&row) != tid || idx_index_id_f->get_uint64(&row) != c.index_id ||
                        idx_xid_f->get_uint64(&row) != static_cast<uint64_t>(msg.xid) ||
                        idx_lsn_f->get_uint64(&row) != static_cast<uint64_t>(msg.lsn)) {
                    break;
                }
                opclass_by_position[idx_position_f->get_uint32(&row)] = std::string(idx_opclass_f->get_text(&row));
            }

            ASSERT_EQ(opclass_by_position.size(), 2u);
            ASSERT_EQ(opclass_by_position.at(0), "int4_ops");
            ASSERT_EQ(opclass_by_position.at(1), "text_ops");
        }

        // The primary/look-aside role indexes never go through Postgres DDL and so never set
        // index_type on the IndexInfo they're built from (see Server::_set_primary_index/
        // _set_lookaside_index) -- confirm they still read back as "btree" (see
        // index_access_method_from_string()'s doc comment), not an empty/unrecognized string.
        auto primary_info = _server->get_index_info(_db, constant::INDEX_PRIMARY, _xid, tid);
        ASSERT_EQ(primary_info.index_type(), "btree");

        auto look_aside_info = _server->get_index_info(_db, constant::INDEX_LOOK_ASIDE, _xid, tid);
        ASSERT_EQ(look_aside_info.index_type(), "btree");
    }

    // Regression test for round-1 review finding 1: a real "CREATE INDEX ... USING hash(...)"
    // (or spgist, brin, or any other access method this engine has never built) must be rejected
    // the same way Server::_check_index_columns rejects an unsupported column type -- logged and
    // declined (no catalog row, "no_op") -- not thrown. index_access_method_from_string() throws
    // on an unrecognized string by design; Server::create_index()'s call path (reached directly
    // from PgLogReader's DDL-replication thread, with no try/catch anywhere between it and
    // PgLogReader::_msg_worker()'s message loop) must never let that exception escape, or an
    // ordinary hash/spgist/brin index crashes the whole pg_log_mgr daemon via std::terminate().
    TEST_F(SysTblMgr_Test, CreateIndexUnsupportedAccessMethod)
    {
        uint64_t tid = 100006;
        uint64_t index_id = 5013;

        _create_table(tid, "x");
        _finalize();

        auto xid = _next_lsn();
        PgMsgIndex msg{};
        msg.lsn = xid.lsn;
        msg.xid = xid.xid;
        msg.namespace_name = "public";
        msg.index = "idx_hash";
        msg.is_unique = false;
        msg.table_oid = tid;
        msg.oid = index_id;
        msg.index_type = "hash";
        msg.columns.push_back({"col2", 2, 0, ""});

        // Must not throw -- that's the whole bug this test guards against.
        proto::IndexProcessRequest response;
        ASSERT_NO_THROW({
            response = _server->create_index(_db, xid, msg, sys_tbl::IndexNames::State::NOT_READY);
        });
        _client->invalidate_table(_db, msg.table_oid, xid);

        // Declined like any other unsupported-DDL case (mirrors _check_index_columns' own
        // unsupported-column-type rejection, which also surfaces as "no_op") -- no catalog row.
        EXPECT_EQ(response.action(), "no_op");

        _finalize();
        auto info = _server->get_index_info(_db, index_id, _xid, tid);
        EXPECT_EQ(info.id(), 0u);
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
        auto cache = std::make_shared<sys_tbl_mgr::ShmCache>(sys_tbl_mgr::SHM_CACHE_ROOTS, 100*1024, true);
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
        xid_mgr::XidMgrServer::get_instance()->commit_xid(_db, 1, cutoff_xid, false, 0);

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

    // Regression test for architecture.md Sec 5.4 case 1 / phase 4's per-db catalog-mutation
    // queue. Drives PgLogReader-style DDL (alter_table), Indexer-style index-state updates
    // (set_index_state), and Committer-style root updates (update_roots) concurrently against the
    // *same* table_id from three threads -- the three actor types the queue must serialize.
    //
    // A naive "wrap each call with an enter/exit counter and assert the observed concurrency
    // never exceeds 1" design does *not* work here and was tried first: a caller legitimately
    // spends time *blocked*, waiting its turn for an already-held slot, which is indistinguishable
    // from "inside the call" to an enter/exit counter wrapped around the whole call -- so a
    // correctly-serialized queue still shows multiple callers "active" simultaneously (one
    // actually running, the rest blocked), which is not a bug. Confirmed this concretely: an
    // isolated probe tracking only `_acquire_catalog_queue_slot`'s own acquire-to-release window
    // (not the surrounding business call) never showed more than 1 concurrent holder, proving the
    // queue itself was correct the whole time the naive counter was reporting violations.
    //
    // Fixed by testing what actually matters deterministically, the same way
    // `FinalizeVsQueueRace` below does: pre-acquire this db's queue slot directly via
    // `test_only_acquire_catalog_queue_slot()`, then confirm all three real entry points block
    // (never return) while that slot is held, and all three complete once it is released. This
    // directly proves alter_table/set_index_state/update_roots route through the same per-db
    // serialization point, which is the actual property that prevents the corruption/lost-update
    // hazard architecture Sec 5.4 case 1 describes -- without needing to force a specific
    // downstream crash (confirmed separately, by reading mutable_table.cc's
    // _upsert_direct/_upsert_by_lookup, that ordinary small-table mutations mutate an
    // already-cached page via StorageCache and only reach IndexWriter's own guarded methods on a
    // flush/split, which a handful of rows never forces mid-generation -- so a crash-chasing test
    // would be unreliable at this data scale regardless).
    //
    // Confirmed this fails (the entry points return immediately, without blocking, even while the
    // slot is held) when the queue-slot acquisition is temporarily removed from
    // alter_table/set_index_state/update_roots, and passes with it restored -- see
    // phases/phase-4.md for the record of that verification.
    TEST_F(SysTblMgr_Test, ConcurrentMutationsSameTable) {
        uint64_t tid = 400000;
        uint64_t index_id = 400001;

        auto msg = _create_table(tid, "multi_actor");
        _create_index(tid, "multi_actor_idx", index_id);
        _finalize();

        // Hold this db's catalog queue slot directly, forcing all three real entry points below
        // to enqueue-and-wait rather than racing them against each other in real time.
        auto holder = _server->test_only_acquire_catalog_queue_slot(_db);

        std::atomic<bool> alter_table_returned{false};
        std::atomic<bool> set_index_state_returned{false};
        std::atomic<bool> update_roots_returned{false};

        // Actor 1: PgLogReader-style DDL against this table.
        msg.columns.push_back({"extra", static_cast<uint8_t>(SchemaType::INT32), 0,
                               std::nullopt, 3, 0, true, false});
        auto ddl_xid = _next_lsn();
        std::thread ddl_thread([&]() {
            _server->alter_table(_db, ddl_xid, msg);
            alter_table_returned = true;
        });

        // Actor 2: Indexer-style index-state update against this table's index.
        auto index_state_xid = _next_lsn();
        std::thread indexer_thread([&]() {
            _server->set_index_state(_db, index_state_xid, tid, index_id, sys_tbl::IndexNames::State::READY);
            set_index_state_returned = true;
        });

        // Actor 3: Committer-style root update against this same table.
        auto roots_xid = _next_lsn();
        std::thread committer_thread([&]() {
            TableMetadata metadata;
            metadata.roots.push_back({index_id, 0xABCD0001});
            _server->update_roots(_db, tid, roots_xid.xid, metadata);
            update_roots_returned = true;
        });

        // Give all three time to reach their own queue-slot acquisition and enqueue (they must
        // then block behind `holder`, which is still held).
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        EXPECT_FALSE(alter_table_returned.load())
            << "alter_table() returned while another caller held this db's catalog queue slot -- "
               "it did not route through the queue";
        EXPECT_FALSE(set_index_state_returned.load())
            << "set_index_state() returned while another caller held this db's catalog queue slot "
               "-- it did not route through the queue";
        EXPECT_FALSE(update_roots_returned.load())
            << "update_roots() returned while another caller held this db's catalog queue slot -- "
               "it did not route through the queue";

        // Release the held slot: the queue's FIFO ordering now lets the three real calls proceed,
        // one at a time (never concurrently, since they all share one db-scoped slot).
        holder.reset();

        ddl_thread.join();
        indexer_thread.join();
        committer_thread.join();

        ASSERT_TRUE(alter_table_returned.load());
        ASSERT_TRUE(set_index_state_returned.load());
        ASSERT_TRUE(update_roots_returned.load());

        _finalize();

        // Also confirm the table is still in a coherent, queryable state afterward.
        auto schema_meta = _client->get_schema(_db, tid, _xid);
        ASSERT_NE(schema_meta, nullptr);
        auto it = std::ranges::find_if(schema_meta->indexes,
                               [&](const auto& index) { return index.id == index_id; });
        ASSERT_NE(it, schema_meta->indexes.end());
    }

    // Regression test for architecture.md Decisions log entry 9 (finalize()-vs-queue barrier):
    // submits a mutation (M, an update_roots call) and then calls finalize() for the same xid from
    // another thread, before M is known to have drained -- using
    // `test_only_acquire_catalog_queue_slot()` to deterministically hold this db's queue slot so
    // both M's and finalize()'s own acquisitions are forced to enqueue-and-wait behind it (rather
    // than relying on a sleep-based race to create the overlap window). Asserts M's effect lands
    // in the specific xid it was submitted before -- not merely "present somewhere" -- since a
    // test that only checked presence could pass even if M were silently dropped or misapplied to
    // the next generation (see phases/phase-3.md round-1 finding 1 for why that distinction
    // matters). Must fail without the barrier (finalize() not routed through the queue) and pass
    // with it -- verified manually while implementing this phase by temporarily reverting
    // finalize()'s queue-slot acquisition, rebuilding, and confirming this test fails; see
    // phases/phase-4.md for the record of that verification.
    TEST_F(SysTblMgr_Test, FinalizeVsQueueRace) {
        uint64_t tid = 410000;
        uint64_t decoy_tid = 410001;
        _create_table(tid, "race_table");
        _create_table(decoy_tid, "race_decoy_table");
        _finalize();

        // Dirty a *different* table's roots first, without finalizing it, so finalize() below
        // always has *something* to flush/advance regardless of timing. Without this, in the
        // "no barrier" broken scenario finalize() can race ahead of M while `_write[db_id]` is
        // still completely empty, hit its "nothing to finalize" early return (server.cc's
        // `if (md_map.empty()) { ...; return; }`), and do nothing at all -- a no-op that happens
        // not to disturb M's later write and so never demonstrates the hazard. Confirmed this is
        // exactly what happened on the first version of this test (it passed even with the
        // barrier removed) before adding this decoy.
        auto decoy_xid = _next_lsn();
        TableMetadata decoy_metadata;
        decoy_metadata.roots.push_back({constant::INDEX_PRIMARY, 0x1});
        _server->update_roots(_db, decoy_tid, decoy_xid.xid, decoy_metadata);

        // Hold this db's catalog queue slot directly, before M or finalize() are even started, so
        // both are guaranteed to enqueue (in that order) and wait rather than racing in real time.
        auto holder = _server->test_only_acquire_catalog_queue_slot(_db);

        auto target_xid = _next_xid();
        constexpr uint64_t marker_extent_id = 0xABCD1234;

        std::thread mutation_thread([&]() {
            TableMetadata metadata;
            metadata.roots.push_back({constant::INDEX_PRIMARY, marker_extent_id});
            _server->update_roots(_db, tid, target_xid.xid, metadata);
        });

        // Give M's call time to reach its own queue-slot acquisition and enqueue (it will then
        // block behind `holder`, which is still held) before finalize() is submitted.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        std::thread finalize_thread([&]() {
            _server->finalize(_db, target_xid.xid, true);
        });

        // Give finalize()'s own acquisition time to enqueue too (behind M's, since M enqueued
        // first) before releasing the held slot.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        // Release the held slot: the queue's FIFO ordering now requires M to run to completion
        // before finalize() is granted its turn.
        holder.reset();

        mutation_thread.join();
        finalize_thread.join();

        // Verify against the real on-disk TableRoots content pinned at exactly `target_xid.xid`,
        // via `SystemTableMgrServer` directly -- *not* `Server::get_roots()`, which turns out not
        // to discriminate here: `_set_roots_info()` populates `Server::_roots_cache` unconditionally
        // on every `update_roots()` call, so `get_roots()` reflects the latest in-memory write
        // immediately regardless of whether finalize() ever committed it, making it blind to
        // exactly the "landed in the wrong generation" failure mode this test exists to catch
        // (confirmed: an earlier version of this test using `get_roots()` passed even with the
        // barrier removed, for this reason). Reading the real system table pinned at `target_xid`
        // bypasses that cache and shows what was actually committed to that generation.
        auto roots_table = SystemTableMgrServer::get_instance()->get_table(_db, sys_tbl::TableRoots::ID,
                target_xid.xid);
        auto roots_schema = roots_table->extent_schema();
        auto table_id_f = roots_schema->get_field("table_id");
        auto index_id_f = roots_schema->get_field("index_id");
        auto extent_id_f = roots_schema->get_field("extent_id");

        auto search_key = sys_tbl::TableRoots::Primary::key_tuple(tid, constant::INDEX_PRIMARY, target_xid.xid);
        auto row_i = roots_table->inverse_lower_bound(search_key);
        bool found = false;
        if (row_i != roots_table->end()) {
            auto &&row = *row_i;
            if (table_id_f->get_uint64(&row) == tid && index_id_f->get_uint64(&row) == constant::INDEX_PRIMARY) {
                found = (extent_id_f->get_uint64(&row) == marker_extent_id);
            }
        }
        ASSERT_TRUE(found) << "update_roots()'s effect, submitted before finalize(), is missing "
                              "from the on-disk generation it was submitted before -- lost or "
                              "misapplied across the finalize boundary";
    }

    // Regression test for the Server::revert() double-shared-lock fix (architecture.md Sec 11):
    // exercises revert() concurrently with a reader. The bug as originally written couldn't
    // deadlock (both locks were shared, on the same mutex), but confirms the fix -- a genuine
    // exclusive lock on `_read_mutex` plus routing through the catalog queue -- doesn't introduce
    // one, and that concurrent reads neither crash nor observe a torn state while revert() rewrites
    // roots symlinks and removes rows.
    TEST_F(SysTblMgr_Test, RevertConcurrentWithReader) {
        uint64_t tid = 420000;
        _create_table(tid, "revert_table");
        _finalize();
        uint64_t committed_xid = _xid.xid - 1;

        std::atomic<bool> stop{false};
        std::atomic<int> read_count{0};
        std::thread reader([&]() {
            while (!stop.load()) {
                auto schema = _server->get_schema(_db, tid, XidLsn(committed_xid));
                if (schema != nullptr) {
                    ++read_count;
                }
            }
        });

        // give the reader a head start so it is actively contending with revert()
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        _server->revert(_db, committed_xid);

        stop.store(true);
        reader.join();

        // No deadlock/crash above is the primary assertion; confirm the reader made real progress
        // (i.e. this wasn't a no-op race where the reader happened to never run concurrently).
        ASSERT_GT(read_count.load(), 0);

        // the table must still be queryable, coherent, at the reverted (== already-committed) xid
        auto schema_meta = _server->get_schema(_db, tid, XidLsn(committed_xid));
        ASSERT_NE(schema_meta, nullptr);
    }
}
