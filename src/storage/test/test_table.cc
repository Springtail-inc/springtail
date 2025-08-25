#include "common/constants.hh"
#include <sys_tbl_mgr/system_tables.hh>
#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/environment.hh>
#include <common/object_cache.hh>
#include <common/threaded_test.hh>
#include <common/filesystem.hh>

#include <storage/csv_field.hh>
#include <storage/vacuumer.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/user_table.hh>

#include <xid_mgr/xid_mgr_client.hh>
#include <xid_mgr/xid_mgr_server.hh>

#include <test/services.hh>

using namespace springtail;

namespace {
    struct CacheSize {
        uint64_t data_cache_size;
        uint64_t page_cache_size;
        uint64_t btree_cache_size;
        uint64_t max_extent_per_page;
    };

    void PrintTo(const CacheSize& cacheSize, std::ostream* os) {
        // Customize the output here as needed
        if (cacheSize.data_cache_size == 32) {
            *os << "small_cache";
        } else {
            *os << "large_cache";
        }
    }

    /**
     * Framework for Table and MutableTable testing.
     */
    class Table_Test : public testing::TestWithParam<CacheSize> {
    protected:
        //// TestSuite setup / teardown

        static void SetUpTestSuite() {
            // "test/table/{}" is needed for vacuum to retrieve db_id from the path
            _base_dir = std::filesystem::temp_directory_path() / "test" / "table";
            std::filesystem::remove_all(_base_dir);

            springtail_init_test();
            test::start_services(true, true, false);

            auto client = sys_tbl_mgr::Client::get_instance();

            auto xid_client = XidMgrClient::get_instance();
            uint64_t access_xid = xid_client->get_committed_xid(1, 0);
            ASSERT_NE(access_xid, 0);
            uint64_t target_xid = access_xid + 1;

            // create the public namespace in the sys_tbl_mgr
            PgMsgNamespace ns_msg;
            ns_msg.oid = 900;
            ns_msg.name = "public";
            client->create_namespace(_db_id, XidLsn(target_xid, constant::MAX_LSN - 1), ns_msg);

            auto xid_server = xid_mgr::XidMgrServer::get_instance();
            xid_server->commit_xid(1, 1, target_xid, false);
        }

        static void TearDownTestSuite() {
            springtail_shutdown();
            std::filesystem::remove_all(_base_dir);
        }

        //// Individual test setup / teardown

        void SetUp() override {
            // set the cache size from the parameters
            CacheSize sizes = GetParam();
            std::string overrides = std::format("storage.data_cache_size={};storage.page_cache_size={};storage.btree_cache_size={};storage.max_extent_per_page={}",
                                                sizes.data_cache_size, sizes.page_cache_size, sizes.btree_cache_size, sizes.max_extent_per_page);
            ::setenv(environment::ENV_OVERRIDE, overrides.c_str(), 1);

            // construct a schema for testing
            std::vector<SchemaColumn> columns({
                    { "table_id", 0, SchemaType::UINT64, 0, false },
                    { "name", 1, SchemaType::TEXT, 0, false, 0 },
                    { "offset", 2, SchemaType::UINT64, 0, false }
                });
            _schema = std::make_shared<ExtentSchema>(columns);

            _fields = _schema->get_fields();
            _csv_fields = std::make_shared<FieldArray>();
            for (int i = 0; i < _fields->size(); i++) {
                auto &&field = _fields->at(i);
                _csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
            }

            _primary_keys = std::vector<std::string>({"name"});
        }

        ExtentSchemaPtr _schema;
        FieldArrayPtr _fields, _csv_fields;

        std::vector<std::string> _primary_keys;

        static std::filesystem::path _base_dir;
        static uint64_t _db_id;

        void _init_sys_tbls(uint64_t target_xid, uint64_t table_oid, std::string_view name)
        {
            auto client = sys_tbl_mgr::Client::get_instance();

            PgMsgTable tbl_msg;
            tbl_msg.oid = table_oid;
            tbl_msg.table = name;
            tbl_msg.namespace_name = "public";
            tbl_msg.columns = std::vector<PgMsgSchemaColumn>(
                {{"table_id", static_cast<uint8_t>(SchemaType::UINT64), 0, std::nullopt, 1, -1,
                  false, false, false},
                 {"name", static_cast<uint8_t>(SchemaType::TEXT), 0, std::nullopt, 2, 0, false,
                  true, false},
                 {"offset", static_cast<uint8_t>(SchemaType::UINT64), 0, std::nullopt, 3, -1, false,
                  false, false}});

            client->create_table(_db_id, XidLsn(target_xid, constant::MAX_LSN - 1), tbl_msg);

            client->finalize(_db_id, target_xid);
        }

        // secondary keys
        std::vector<Index> _make_keys(uint64_t table_id, const std::vector<TableRoot> &roots)
        {
            int i = 0;
            std::vector<Index> keys;
            for (auto const& v: roots) {
                if (v.index_id == constant::INDEX_PRIMARY) {
                    continue;
                }

                Index idx;
                idx.id = v.index_id;
                idx.table_id = table_id;
                idx.name=_schema->column_order()[i];
                idx.is_unique = false;
                idx.state = static_cast<uint8_t>(sys_tbl::IndexNames::State::READY);
                idx.columns.emplace_back(0, i+1);
                keys.push_back(idx);
                ++i;
            }
            return keys;
        }

        TablePtr
        _create_table(uint64_t table_id, uint64_t xid, const std::vector<TableRoot> &roots)
        {
            TableMetadata tbl_meta;
            tbl_meta.roots = roots;
            tbl_meta.snapshot_xid = 1;

            auto keys = _make_keys(table_id, roots);

            return std::make_shared<UserTable>(_db_id, table_id, xid, _base_dir,
                                           _primary_keys, keys,
                                           tbl_meta, _schema);
        }

        MutableTablePtr
        _create_mtable(uint64_t table_id, uint64_t xid, const std::vector<TableRoot> &roots)
        {
            TableMetadata tbl_meta;
            tbl_meta.roots = roots;
            tbl_meta.snapshot_xid = 1;

            auto keys = _make_keys(table_id, roots);


            return std::make_shared<UserMutableTable>(_db_id, table_id, xid - 1, xid, _base_dir,
                                                  _primary_keys, keys,
                                                  tbl_meta, _schema);
        }

        std::shared_ptr<Tuple>
        _create_key(const std::string &name)
        {
            auto k = std::make_shared<ConstTypeField<std::string>>(name);
            std::vector<ConstFieldPtr> v({ k });
            return std::make_shared<ValueTuple>(v);
        }

        std::shared_ptr<Tuple>
        _create_value(uint64_t table_id, const std::string &name, uint64_t offset)
        {
            auto t = std::make_shared<ConstTypeField<uint64_t>>(table_id);
            auto n = std::make_shared<ConstTypeField<std::string>>(name);
            auto o = std::make_shared<ConstTypeField<uint64_t>>(offset);
            std::vector<ConstFieldPtr> v({ t, n, o });
            return std::make_shared<ValueTuple>(v);
        };

        void
        _populate_table(MutableTablePtr mtable)
        {
            csv::CSVReader reader("test_btree_simple.csv");
            for (auto &&r : reader) {
                // insert data to the tree
                mtable->insert(std::make_shared<FieldTuple>(_csv_fields, &r), constant::UNKNOWN_EXTENT);
            }
        }

        /**
         * Request for multi-threading tests.
         */
        class Request {
        public:
            /** add row constructor */
            Request(MutableTablePtr table,
                    uint64_t xid,
                    uint64_t extent_id,
                    std::vector<TuplePtr> &&tuples)
                : _table(table),
                  _extent_id(extent_id),
                  _tuples(tuples)
            { }

            /**
             * @brief Overload () for execution from worker thread.
             *        Main entry from worker thread
             */
            void operator()() {
                for (auto &tuple : _tuples) {
                    _table->update(tuple, _extent_id);
                }
            }

        private:
            MutableTablePtr _table;
            uint64_t _extent_id;
            std::vector<TuplePtr> _tuples;
        };
        typedef std::shared_ptr<Request> RequestPtr;
    };

    uint64_t Table_Test::_db_id = 1;
    std::filesystem::path Table_Test::_base_dir;

    TEST_P(Table_Test, CreateEmpty) {
        auto client = XidMgrClient::get_instance();
        auto server = xid_mgr::XidMgrServer::get_instance();
        uint64_t access_xid = client->get_committed_xid(1, 0);
        uint64_t target_xid = access_xid + 1;

        // create the namespace and table in the sys_tbl_mgr
        _init_sys_tbls(target_xid, 1000, "test_create_empty");

        // create a mutable table
        TableMetadata metadata;
        metadata.roots = { {0, constant::UNKNOWN_EXTENT}, {1, constant::UNKNOWN_EXTENT} };
        auto mtable = _create_mtable(1000, target_xid, metadata.roots);

        // finalize the empty table
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table
        access_xid = target_xid;
        auto table = _create_table(1000, access_xid, metadata.roots);

        // get a key that doesn't exist since the table is empty
        auto key = _create_key("aaaa");

        // ensure that it appears empty and everything works as expected (find, lower_bound, etc)
        ASSERT_TRUE(table->has_primary());
        ASSERT_TRUE(table->primary_lookup(key) == constant::UNKNOWN_EXTENT);
        ASSERT_TRUE(table->lower_bound(key) == table->end());
        ASSERT_TRUE(table->begin() == table->end());
        ASSERT_TRUE(table->index(0)->begin() == table->index(0)->end());
    }

    TEST_P(Table_Test, Inserts) {
        auto client = XidMgrClient::get_instance();
        auto server = xid_mgr::XidMgrServer::get_instance();
        uint64_t access_xid = client->get_committed_xid(1, 0);
        uint64_t target_xid = access_xid + 1;

        // create the namespace and table in the sys_tbl_mgr
        _init_sys_tbls(target_xid, 1001, "test_inserts");

        // create a mutable table
        TableMetadata metadata;
        metadata.roots = { {0, constant::UNKNOWN_EXTENT}, {1, constant::UNKNOWN_EXTENT} };

        auto mtable = _create_mtable(1001, target_xid, metadata.roots);

        // insert a number of rows
        _populate_table(mtable);

        // finalize the table
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table
        access_xid = target_xid;
        auto table = _create_table(1001, access_xid, metadata.roots);

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        int count = 0;
        std::string prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(&row), prev);
            }

            prev = _fields->at(1)->get_text(&row);
            ++count;
        }
        ASSERT_EQ(count, 5000);

        // verify the secondary index
        auto secondary = table->index(1);

        count = 0;
        uint64_t table_id = 0;
        auto table_id_f = secondary->get_schema()->get_field("table_id");
        for (auto row : *secondary) {
            auto current = table_id_f->get_uint64(&row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000);
    }

    TEST_P(Table_Test, SingleXactMutations) {
        auto client = XidMgrClient::get_instance();
        auto server = xid_mgr::XidMgrServer::get_instance();
        uint64_t access_xid = client->get_committed_xid(1, 0);
        uint64_t target_xid = access_xid + 1;

        // create the namespace and table in the sys_tbl_mgr
        _init_sys_tbls(target_xid, 1002, "test_single_xact_mutations");

        // create a mutable table
        TableMetadata metadata;
        metadata.roots = {{0, constant::UNKNOWN_EXTENT}, {1, constant::UNKNOWN_EXTENT}};
        auto mtable = _create_mtable(1002, target_xid, metadata.roots);

        // insert a number of rows
        _populate_table(mtable);

        // remove rows with unknown positions
        // note: rows with table_id == 1
        std::vector<std::string> keys = {
            "cibeson0",
            "pnisard0",
            "unardi0",
            "asineath0",
            "sfrankland0"
        };
        for (auto &key : keys) {
            mtable->remove(_create_key(key), constant::UNKNOWN_EXTENT);
        }

        // update some row data with unknown positions
        // note: rows with table_id = 6
        std::vector<TuplePtr> update_values = {
            _create_value(6, "ctipton5", 100),
            _create_value(6, "callsepp5" , 100),
            _create_value(6, "mpinwell5" , 100),
            _create_value(6, "oblackborn5", 100),
            _create_value(6, "gnatte5", 100)
        };
        for (auto &value : update_values) {
            mtable->update(value, constant::UNKNOWN_EXTENT);
        }

        // upsert some missing rows with unknown positions
        std::vector<TuplePtr> upsert_values = {
            _create_value(1500, "cibeson0", 1),
            _create_value(1500, "pnisard0", 1),
            _create_value(1500, "unardi0", 1),
            _create_value(1500, "asineath0", 1),
            _create_value(1500, "sfrankland0", 1)
        };
        for (auto &value : upsert_values) {
            mtable->upsert(value, constant::UNKNOWN_EXTENT);
        }

        // upsert some existing rows with unknown positions
        upsert_values = {
            _create_value(3, "tcases2", 103),
            _create_value(3, "lharback2", 103),
            _create_value(3, "ehalpeine2", 103),
            _create_value(3, "astenner2", 103),
            _create_value(3, "dhaggleton2", 103)
        };
        for (auto &value : upsert_values) {
            mtable->upsert(value, constant::UNKNOWN_EXTENT);
        }

        // finalize the table
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table
        access_xid = target_xid;
        auto table = _create_table(1002, access_xid, metadata.roots);

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        int count = 0;
        std::string prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(&row), prev);
            }

            // check that the removes and updates occurred correctly
            uint64_t table_id = _fields->at(0)->get_uint64(&row);
            ASSERT_NE(table_id, 1); // all table_id == 1 should be removed
            if (table_id == 6) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 100); // updates
            }
            if (table_id == 1500) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 1); // upsert inserts
            }
            if (table_id == 3) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 103); // upsert updates
            }

            prev = _fields->at(1)->get_text(&row);
            ++count;
        }
        ASSERT_EQ(count, 5000); // removed 5, upserted 5

        auto secondary = table->index(1);

        count = 0;
        uint64_t table_id = 0;
        auto table_id_f = secondary->get_schema()->get_field("table_id");
        for (auto row : *secondary) {
            auto current = table_id_f->get_uint64(&row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000);
    }

    TEST_P(Table_Test, MultiXactMutations) {
        auto client = XidMgrClient::get_instance();
        auto server = xid_mgr::XidMgrServer::get_instance();
        uint64_t access_xid = client->get_committed_xid(1, 0);
        uint64_t target_xid = access_xid + 1;

        // create the namespace and table in the sys_tbl_mgr
        _init_sys_tbls(target_xid, 1003, "test_multi_xact_mutations");

        // create a mutable table
        TableMetadata metadata;
        metadata.roots = { {0, constant::UNKNOWN_EXTENT}, {1, constant::UNKNOWN_EXTENT} };
        auto mtable = _create_mtable(1003, target_xid, metadata.roots);

        // insert a number of rows
        _populate_table(mtable);

        // finalize the table
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table for lookup
        access_xid = target_xid;
        auto table = _create_table(1003, access_xid, metadata.roots);

        // create a new mutable table with a later XID target
        ++target_xid;
        mtable = _create_mtable(1003, target_xid, metadata.roots);

        // remove rows with unknown positions
        // note: rows with table_id == 1
        std::vector<std::string> keys = {
            "cibeson0",
            "pnisard0",
            "unardi0",
            "asineath0",
            "sfrankland0"
        };
        for (auto &key : keys) {
            mtable->remove(_create_key(key), constant::UNKNOWN_EXTENT);
        }

        // remove rows with known positions
        // note: rows with table_id == 2
        keys = {
            "tdockrell1",
            "gatwater1",
            "llandon1",
            "nmingardi1",
            "lstrapp1"
        };
        for (auto &key : keys) {
            auto &&search_key = _create_key(key);
            uint64_t extent_id = table->primary_lookup(search_key);
            mtable->remove(search_key, extent_id);
        }

        // finalize the table
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table
        access_xid = target_xid;
        table = _create_table(1003, access_xid, metadata.roots);

        // verify the data at this point
        int count = 0;
        std::string prev = "";
        for (auto row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(&row), prev);
            }
            prev = _fields->at(1)->get_text(&row);

            // check that the removes and updates occurred correctly
            uint64_t table_id = _fields->at(0)->get_uint64(&row);
            ASSERT_NE(table_id, 1); // all table_id == 1 should be removed
            ASSERT_NE(table_id, 2); // all table_id == 2 should be removed

            ++count;
        }
        ASSERT_EQ(count, 5000 - 10); // removed 10

        auto secondary = table->index(1);

        count = 0;
        uint64_t table_id = 0;
        auto table_id_f = secondary->get_schema()->get_field("table_id");
        for (auto row : *secondary) {
            auto current = table_id_f->get_uint64(&row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000 - 10);

        // create a new mutable table with a later XID target
        ++target_xid;
        mtable = _create_mtable(1003, target_xid, metadata.roots);

        // update some row data with unknown positions
        // note: rows with table_id = 6
        std::vector<TuplePtr> update_values = {
            _create_value(6, "ctipton5", 100),
            _create_value(6, "callsepp5" , 100),
            _create_value(6, "mpinwell5" , 100),
            _create_value(6, "oblackborn5", 100),
            _create_value(6, "gnatte5", 100)
        };
        for (auto &value : update_values) {
            mtable->update(value, constant::UNKNOWN_EXTENT);
        }

        // update some row data with known positions
        // note: rows with table_id = 7
        update_values = {
            _create_value(7, "dgrgic6", 101),
            _create_value(7, "nbeaglehole6", 101),
            _create_value(7, "trosendorf6", 101),
            _create_value(7, "ldana6", 101),
            _create_value(7, "gridulfo6", 101)
        };
        std::vector<std::string> update_keys = {
            "dgrgic6",
            "nbeaglehole6",
            "trosendorf6",
            "ldana6",
            "gridulfo6"
        };
        for (int i = 0; i < update_keys.size(); i++) {
            auto &&search_key = _create_key(update_keys[i]);
            uint64_t extent_id = table->primary_lookup(search_key);
            mtable->update(update_values[i], extent_id);
        }

        // finalize the table
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table
        access_xid = target_xid;
        table = _create_table(1003, access_xid, metadata.roots);

        // verify the data at this point
        count = 0;
        prev = "";
        for (auto row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(&row), prev);
            }
            prev = _fields->at(1)->get_text(&row);

            // check that the removes and updates occurred correctly
            uint64_t table_id = _fields->at(0)->get_uint64(&row);
            ASSERT_NE(table_id, 1); // all table_id == 1 should be removed
            ASSERT_NE(table_id, 2); // all table_id == 2 should be removed
            if (table_id == 6) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 100); // updates
            }
            if (table_id == 7) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 101); // updates
            }

            ++count;
        }
        ASSERT_EQ(count, 5000 - 10); // removed 10

        secondary = table->index(1);

        count = 0;
        table_id = 0;
        for (auto row : *secondary) {
            auto current = secondary->get_schema()->get_field("table_id")->get_uint64(&row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000 - 10);

        // create a new mutable table with a later XID target
        ++target_xid;
        mtable = _create_mtable(1003, target_xid, metadata.roots);

        // upsert some missing rows with unknown positions
        // note: original rows with table_id = 1
        std::vector<TuplePtr> upsert_values = {
            _create_value(1500, "cibeson0", 1),
            _create_value(1500, "pnisard0", 1),
            _create_value(1500, "unardi0", 1),
            _create_value(1500, "asineath0", 1),
            _create_value(1500, "sfrankland0", 1)
        };
        for (auto &value : upsert_values) {
            mtable->upsert(value, constant::UNKNOWN_EXTENT);
        }

        // upsert some missing rows with known positions
        // note: original rows with table_id = 2
        upsert_values = {
            _create_value(1501, "tdockrell1", 2),
            _create_value(1501, "gatwater1", 2),
            _create_value(1501, "llandon1", 2),
            _create_value(1501, "nmingardi1", 2),
            _create_value(1501, "lstrapp1", 2)
        };
        std::vector<std::string> upsert_keys = {
            "tdockrell1",
            "gatwater1",
            "llandon1",
            "nmingardi1",
            "lstrapp1"
        };
        for (int i = 0; i < upsert_keys.size(); i++) {
            auto &&search_key = _create_key(upsert_keys[i]);
            uint64_t extent_id = table->primary_lookup(search_key);
            mtable->upsert(upsert_values[i], extent_id);
        }

        // finalize the table
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table
        access_xid = target_xid;
        table = _create_table(1003, access_xid, metadata.roots);

        count = 0;
        prev = "";
        for (auto row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(&row), prev);
            }
            prev = _fields->at(1)->get_text(&row);

            // check that the removes and updates occurred correctly
            uint64_t table_id = _fields->at(0)->get_uint64(&row);
            ASSERT_NE(table_id, 1); // all table_id == 1 should be removed
            ASSERT_NE(table_id, 2); // all table_id == 2 should be removed
            if (table_id == 6) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 100); // updates
            }
            if (table_id == 7) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 101); // updates
            }
            if (table_id == 1500) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 1); // upsert inserts
            }
            if (table_id == 1501) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 2); // known upsert inserts
            }

            ++count;
        }
        ASSERT_EQ(count, 5000); // removed 10, upserted 10

        secondary = table->index(1);

        count = 0;
        table_id = 0;
        for (auto row : *secondary) {
            auto current = secondary->get_schema()->get_field("table_id")->get_uint64(&row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000);

        // create a new mutable table with a later XID target
        ++target_xid;
        mtable = _create_mtable(1003, target_xid, metadata.roots);

        // upsert some existing rows with unknown positions
        // note: rows with table_id = 3
        upsert_values = {
            _create_value(3, "tcases2", 103),
            _create_value(3, "lharback2", 103),
            _create_value(3, "ehalpeine2", 103),
            _create_value(3, "astenner2", 103),
            _create_value(3, "dhaggleton2", 103)
        };
        for (auto &value : upsert_values) {
            mtable->upsert(value, constant::UNKNOWN_EXTENT);
        }

        // upsert some existing rows with known positions
        // note: rows with table_id = 4
        upsert_values = {
            _create_value(4, "pblythin3", 104),
            _create_value(4, "kradki3", 104),
            _create_value(4, "sdeery3", 104),
            _create_value(4, "nhessentaler3", 104),
            _create_value(4, "lhiskey3", 104)
        };
        upsert_keys = {
            "pblythin3",
            "kradki3",
            "sdeery3",
            "nhessentaler3",
            "lhiskey3"
        };
        for (int i = 0; i < upsert_keys.size(); i++) {
            auto &&search_key = _create_key(upsert_keys[i]);
            uint64_t extent_id = table->primary_lookup(search_key);
            mtable->upsert(upsert_values[i], extent_id);
        }

        // finalize the table
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table
        access_xid = target_xid;
        table = _create_table(1003, access_xid, metadata.roots);

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        count = 0;
        prev = "";
        for (auto row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(&row), prev);
            }
            prev = _fields->at(1)->get_text(&row);

            // check that the removes and updates occurred correctly
            uint64_t table_id = _fields->at(0)->get_uint64(&row);
            ASSERT_NE(table_id, 1); // all table_id == 1 should be removed
            ASSERT_NE(table_id, 2); // all table_id == 2 should be removed
            if (table_id == 6) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 100); // updates
            }
            if (table_id == 7) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 101); // updates
            }
            if (table_id == 1500) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 1); // upsert inserts
            }
            if (table_id == 1501) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 2); // known upsert inserts
            }
            if (table_id == 3) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 103); // upsert updates
            }
            if (table_id == 4) {
                ASSERT_EQ(_fields->at(2)->get_uint64(&row), 104); // upsert updates
            }

            ++count;
        }
        ASSERT_EQ(count, 5000); // removed 5, upserted 5

        secondary = table->index(1);

        count = 0;
        table_id = 0;
        for (auto row : *secondary) {
            auto current = secondary->get_schema()->get_field("table_id")->get_uint64(&row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000);
    }


    TEST_P(Table_Test, MultiThreadMutations) {
        auto client = XidMgrClient::get_instance();
        auto server = xid_mgr::XidMgrServer::get_instance();
        uint64_t access_xid = client->get_committed_xid(1, 0);
        uint64_t target_xid = access_xid + 1;

        // create the namespace and table in the sys_tbl_mgr
        _init_sys_tbls(target_xid, 1004, "test_multi_thread_mutations");

        // create a mutable table
        TableMetadata metadata;
        metadata.roots = { {0, constant::UNKNOWN_EXTENT}, {1, constant::UNKNOWN_EXTENT} };
        auto mtable = _create_mtable(1004, target_xid, metadata.roots);

        // insert a number of rows
        _populate_table(mtable);

        // finalize the table
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table and identify extents to be mutated
        access_xid = target_xid;
        auto table = _create_table(1004, access_xid, metadata.roots);

        std::map<uint64_t, std::vector<TuplePtr>> tuple_map;

        csv::CSVReader reader2("test_btree_simple.csv");
        int count = 0;
        for (auto &&r : reader2) {
            auto csvtuple = std::make_shared<FieldTuple>(_csv_fields, &r);
            auto search_key = _schema->tuple_subset(csvtuple, _primary_keys);

            uint64_t extent_id = table->primary_lookup(search_key);

            std::string name(search_key->field(0)->get_text(search_key->row()));
            auto value = _create_value(count++, name, 20000);

            tuple_map[extent_id].push_back(value);
        }

        // create a new mutable table with a later XID target
        ++target_xid;
        mtable = _create_mtable(1004, target_xid, metadata.roots);

        // mutate the individual extents in separate concurrent threads
        PhasedThreadTest<Request> tester;

        for (auto &entry : tuple_map) {
            auto request = std::make_shared<Request>(mtable, target_xid, entry.first, std::move(entry.second));
            tester.add_request(request);
        }

        // finalize and verify the table
        tester.set_verify([this, mtable, target_xid, server]() {
            // create an access table
            TableMetadata metadata = mtable->finalize();
            sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
            server->commit_xid(1, 1, target_xid, false);

            auto access_xid = target_xid;
            auto table = _create_table(1004, access_xid, metadata.roots);

            // ensure that it has all of the expected rows through both the primary and secondary index
            // and that everything else works as expected (find, lower_bound, etc)
            int count = 0;
            std::string prev = "";
            for (auto row : *table) {
                if (prev != "") {
                    ASSERT_GT(_fields->at(1)->get_text(&row), prev);
                }
                prev = _fields->at(1)->get_text(&row);

                uint64_t offset = _fields->at(2)->get_uint64(&row);
                ASSERT_EQ(offset, 20000);

                ++count;
            }
            ASSERT_EQ(count, 5000);

            auto secondary = table->index(1);

            count = 0;
            uint64_t table_id = 0;
            auto table_id_f = secondary->get_schema()->get_field("table_id");
            for (auto row : *secondary) {
                auto current = table_id_f->get_uint64(&row);
                ASSERT_LE(table_id, current);
                table_id = current;
                ++count;
            }
            ASSERT_EQ(count, 5000);

        });

        // run the phase using 4 threads (just one phase here)
        tester.run(4);
    }

    TEST_P(Table_Test, SecondaryIndex) {
        auto client = XidMgrClient::get_instance();
        auto server = xid_mgr::XidMgrServer::get_instance();
        uint64_t access_xid = client->get_committed_xid(1, 0);
        uint64_t target_xid = access_xid + 1;

        // create the namespace and table in the sys_tbl_mgr
        _init_sys_tbls(target_xid, 1005, "test_secondary_indexes");

        // create a mutable table
        TableMetadata metadata;

        // this will create two indexes on the first and second columns
        metadata.roots = { {0, constant::UNKNOWN_EXTENT}, {1, constant::UNKNOWN_EXTENT}, {2, constant::UNKNOWN_EXTENT} };

        auto mtable = _create_mtable(1005, target_xid, metadata.roots);

        // insert a number of rows
        _populate_table(mtable);

        // finalize the table
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table
        access_xid = target_xid;
        auto table = _create_table(1005, access_xid, metadata.roots);

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        int counter = 0;
        std::string prev = "";
        for (auto row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(&row), prev);
            }

            prev = _fields->at(1)->get_text(&row);
            ++counter;
        }
        ASSERT_EQ(counter, 5000);

        //define a test set
        int set_size = 0;
        int equal_count = 0;
        uint64_t test_value = 20;

        uint64_t test_value_up = 100000;
        uint64_t test_value_down = 0;


        for (auto row : *table) {
            auto value = _fields->at(0)->get_uint64(&row);
            if (value == test_value) {
                ++equal_count;
            }
            if (value >= test_value) {
                ++set_size;
            }
            // make sure that the up/down test vaulues are out of range
            ASSERT_LT(value, test_value_up);
            ASSERT_GT(value, test_value_down);
        }

        // make sure that that we have enough equal values
        // for testing the bounds functions
        ASSERT_GT(equal_count, 1);

        // use the secondary index to iterate over the same set
        auto k = std::make_shared<ConstTypeField<uint64_t>>(test_value);
        auto key = std::make_shared<ValueTuple>(std::vector<ConstFieldPtr>{ k });

        k = std::make_shared<ConstTypeField<uint64_t>>(test_value_up);
        auto key_up = std::make_shared<ValueTuple>(std::vector<ConstFieldPtr>{ k });

        k = std::make_shared<ConstTypeField<uint64_t>>(test_value_down);
        auto key_down = std::make_shared<ValueTuple>(std::vector<ConstFieldPtr>{ k });

        auto end_it = table->end(1);
        auto begin_it = table->begin(1);
        auto last_it = table->end(1);
        --last_it;

        // test the first index
        {
            // use the secondary index to iterate over the same set
            auto k = std::make_shared<ConstTypeField<uint64_t>>(test_value);
            std::vector<ConstFieldPtr> v({ k });
            auto key = std::make_shared<ValueTuple>(v);

            auto it = table->lower_bound(key, 1);

            int count = 0;
            int equal = 0;
            for (; it != end_it; ++it) {
                auto row = *it;
                auto value = _fields->at(0)->get_uint64(&row);
                if (value == test_value) {
                    ++equal;
                }
                ASSERT_LE(test_value, value);
                ++count;
            }
            ASSERT_EQ(count, set_size);
            ASSERT_EQ(equal, equal_count);
        }
        {
            auto it = table->lower_bound(key_up, 1);
            ASSERT_EQ(it, end_it);
        }
        {
            auto it = table->lower_bound(key_down, 1);
            ASSERT_EQ(it, begin_it);
        }

        // test simple secondary iterator
        {
            auto end_it = table->end(1, true);

            // use the secondary index to iterate over the same set
            auto k = std::make_shared<ConstTypeField<uint64_t>>(test_value);
            std::vector<ConstFieldPtr> v({ k });
            auto key = std::make_shared<ValueTuple>(v);

            auto it = table->lower_bound(key, 1, true);
            auto idx_schema = table->get_index_schema(1);
            auto fields = idx_schema->get_fields();

            int count = 0;
            int equal = 0;
            for (; it != end_it; ++it) {
                auto row = *it;
                auto value = fields->at(0)->get_uint64(&row);
                if (value == test_value) {
                    ++equal;
                }
                ASSERT_LE(test_value, value);
                ++count;
            }
            ASSERT_EQ(count, set_size);
            ASSERT_EQ(equal, equal_count);
        }

        {
            auto it = table->inverse_lower_bound(key, 1);
            int count = 0;
            int equal = 0;
            for (; it != end_it; ++it) {
                auto row = *it;
                auto value = _fields->at(0)->get_uint64(&row);
                if (value == test_value) {
                    ++equal;
                }
                ASSERT_LE(test_value, value);
                ++count;
            }
            ASSERT_EQ(equal, 1);
            ASSERT_EQ(count, set_size - equal_count + 1);
        }
        {
            auto it = table->inverse_lower_bound(key_up, 1);
            ASSERT_EQ(it, last_it);
        }
        {
            auto it = table->inverse_lower_bound(key_down, 1);
            ASSERT_EQ(it, end_it);
        }

        // test the second index
        {
            std::string test_value = "f";

            auto k = std::make_shared<ConstTypeField<std::string>>(test_value);
            std::vector<ConstFieldPtr> v({ k });
            auto key = std::make_shared<ValueTuple>(v);

            auto it = table->lower_bound(key, 2);
            auto end_it = table->end(1);
            for (; it != end_it; ++it) {
                auto row = *it;
                auto txt = _fields->at(1)->get_text(&row);
                ASSERT_LE(test_value[0], txt[0]);
            }
        }
    }

    TEST_P(Table_Test, RemoveAndRepopulate) {
        auto client = XidMgrClient::get_instance();
        auto server = xid_mgr::XidMgrServer::get_instance();
        uint64_t access_xid = client->get_committed_xid(1, 0);
        uint64_t target_xid = access_xid + 1;

        // create the namespace and table in the sys_tbl_mgr
        _init_sys_tbls(target_xid, 1006, "test_remove_repopulate");

        // create a mutable table
        TableMetadata metadata;
        metadata.roots = { {0, constant::UNKNOWN_EXTENT}, {1, constant::UNKNOWN_EXTENT} };
        auto mtable = _create_mtable(1006, target_xid, metadata.roots);

        // insert initial rows
        _populate_table(mtable);

        // remove all rows by iterating through the table
        csv::CSVReader reader("test_btree_simple.csv");
        for (auto &&r : reader) {
            auto csvtuple = std::make_shared<FieldTuple>(_csv_fields, &r);
            auto search_key = _schema->tuple_subset(csvtuple, _primary_keys);
            mtable->remove(search_key, constant::UNKNOWN_EXTENT);
        }

        // repopulate with new data
        _populate_table(mtable);

        // finalize the table after all operations
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, false);

        // create an access table to verify the final state
        access_xid = target_xid;
        auto table = _create_table(1006, access_xid, metadata.roots);

        // verify the data through primary index
        int count = 0;
        std::string prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(&row), prev);
            }
            prev = _fields->at(1)->get_text(&row);
            ++count;
        }
        ASSERT_EQ(count, 5000); // should have the full set of repopulated rows

        // verify through secondary index
        auto secondary = table->index(1);
        count = 0;
        uint64_t table_id = 0;
        auto table_id_f = secondary->get_schema()->get_field("table_id");
        for (auto row : *secondary) {
            auto current = table_id_f->get_uint64(&row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000);
    }

    TEST_P(Table_Test, VaccumTable) {
        auto client = XidMgrClient::get_instance();
        auto server = xid_mgr::XidMgrServer::get_instance();
        uint64_t access_xid = client->get_committed_xid(1, 0);
        uint64_t target_xid = access_xid + 1;
        auto table_id = 1007;

        // Enable extents tracking in vacuumer
        Vacuumer::get_instance()->enable_tracking_extents();

        // create the namespace and table in the sys_tbl_mgr
        _init_sys_tbls(target_xid, table_id, "test_table_vacuum");

        // create a mutable table
        TableMetadata metadata;
        metadata.roots = { {0, constant::UNKNOWN_EXTENT}, {1, constant::UNKNOWN_EXTENT} };
        auto mtable = _create_mtable(table_id, target_xid, metadata.roots);

        // insert initial rows
        _populate_table(mtable);

        // finalize the table after create
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, true);

        // create an access table and identify extents to be mutated
        access_xid = target_xid;
        auto table = _create_table(1007, access_xid, metadata.roots);

        ++target_xid;
        // Create new mtable at target_xid
        mtable = _create_mtable(table_id, target_xid, metadata.roots);

        // update some row data with unknown positions
        // note: rows with table_id = 6
        std::vector<std::string> keys = {
            "ctipton5",
            "dgrgic6",
            "dvale7",
            "jpermain8",
            "aormiston9"
        };

        std::vector<TuplePtr> update_values = {
            _create_value(6, "ctipton5", 100),
            _create_value(7, "dgrgic6" , 100),
            _create_value(8, "dvale7" , 100),
            _create_value(9, "jpermain8", 100),
            _create_value(10, "aormiston9", 100)
        };
        for (size_t i = 0; i < update_values.size(); ++i) {
            auto &&search_key = _create_key(keys[i]);
            uint64_t extent_id = table->primary_lookup(search_key);
            mtable->update(update_values[i], extent_id);
        }

        // finalize the table after update
        metadata = mtable->finalize();
        sys_tbl_mgr::Client::get_instance()->update_roots(mtable->db(), mtable->id(), target_xid, metadata);
        server->commit_xid(1, 1, target_xid, true);

        // repopulate with new data
        //_populate_table(mtable);

        ++target_xid;
        server->commit_xid(1, 1, target_xid, true);

        auto table_dir = table_helpers::get_table_dir(_base_dir, 1, table_id, 1);
        auto table_file = table_dir / fmt::format(constant::DATA_FILE);
        auto size_pre_vacuum = fs::get_block_count(table_file);

        Vacuumer::get_instance()->commit_expired_extents(1, target_xid);

        // Set global threshold as small and run vacuum
        Vacuumer::get_instance()->set_global_vacuum_threshold(10);
        Vacuumer::get_instance()->run_vacuum_once();

        auto size_post_vacuum = fs::get_block_count(table_file);

        ASSERT_GT(size_pre_vacuum, size_post_vacuum);
    }

    INSTANTIATE_TEST_CASE_P(Table_Test,
                            Table_Test,
                            ::testing::Values(CacheSize{ 16384, 16384, 512, 16 },
                                              CacheSize{ 32, 32, 8, 4 }));
}
