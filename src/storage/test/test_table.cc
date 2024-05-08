#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/object_cache.hh>
#include <common/threaded_test.hh>

#include <storage/csv_field.hh>
#include <storage/table.hh>

using namespace springtail;

namespace {

    /**
     * Framework for Table and MutableTable testing.
     */
    class Table_Test : public testing::Test {
    protected:
        void SetUp() override {
            springtail_init();

            // construct a schema for testing
            std::vector<SchemaColumn> columns({
                    { "table_id", 0, SchemaType::UINT64, false },
                    { "name", 1, SchemaType::TEXT, false, 0 },
                    { "offset", 2, SchemaType::UINT64, false }
                });
            _schema = std::make_shared<ExtentSchema>(columns);

            _fields = _schema->get_fields();
            _csv_fields = std::make_shared<FieldArray>();
            for (int i = 0; i < _fields->size(); i++) {
                auto &&field = _fields->at(i);
                _csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
            }

            _read_cache = std::make_shared<LruObjectCache<std::pair<std::filesystem::path, uint64_t>, Extent>>(1024*1024);
            _write_cache = MutableBTree::create_cache(2*1024*1024);

            _primary_keys = std::vector<std::string>({"name"});
            _secondary_keys = { std::vector<std::string>({"table_id"}) };

            _data_cache = std::make_shared<DataCache>(true);

            _base_dir = std::filesystem::temp_directory_path() / "test_table";
            std::filesystem::remove_all(_base_dir);

            std::filesystem::create_directories(_base_dir / "1000");
            std::filesystem::create_directory(_base_dir / "1001");
            std::filesystem::create_directory(_base_dir / "1002");
            std::filesystem::create_directory(_base_dir / "1003");
            std::filesystem::create_directory(_base_dir / "1004");
        }

        void TearDown() override {
            // remove any files created during the run
            std::filesystem::remove_all(_base_dir);
        }

        ExtentSchemaPtr _schema;
        FieldArrayPtr _fields, _csv_fields;

        std::shared_ptr<ExtentCache> _read_cache;
        MutableBTree::PageCachePtr _write_cache;
        std::vector<std::string> _primary_keys;
        std::vector<std::vector<std::string>> _secondary_keys;

        DataCachePtr _data_cache;

        std::filesystem::path _base_dir;

        TablePtr
        _create_table(uint64_t table_id, uint64_t xid, const std::vector<uint64_t> &roots)
        {
            return std::make_shared<Table>(table_id,
                                           xid,
                                           _base_dir / fmt::format("{}", table_id),
                                           _primary_keys,
                                           _secondary_keys,
                                           roots,
                                           _schema);
        }

        MutableTablePtr
        _create_mtable(uint64_t table_id, uint64_t xid, const std::vector<uint64_t> &roots)
        {
            return std::make_shared<MutableTable>(table_id,
                                                  xid,
                                                  roots,
                                                  _base_dir / fmt::format("{}", table_id),
                                                  _primary_keys,
                                                  _secondary_keys,
                                                  _schema,
                                                  _data_cache,
                                                  _write_cache,
                                                  _read_cache);
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
        _populate_table(MutableTablePtr mtable, uint64_t xid)
        {
            csv::CSVReader reader("test_btree_simple.csv");
            for (auto &&r : reader) {
                // insert data to the tree
                mtable->insert(std::make_shared<FieldTuple>(_csv_fields, r), xid, constant::UNKNOWN_EXTENT);
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
                  _xid(xid),
                  _extent_id(extent_id),
                  _tuples(tuples)
            { }

            /**
             * @brief Overload () for execution from worker thread.
             *        Main entry from worker thread
             */
            void operator()() {
                for (auto &tuple : _tuples) {
                    _table->update(tuple, _xid, _extent_id);
                }
            }

        private:
            MutableTablePtr _table;
            uint64_t _xid;
            uint64_t _extent_id;
            std::vector<TuplePtr> _tuples;
        };
        typedef std::shared_ptr<Request> RequestPtr;
    };

    TEST_F(Table_Test, CreateEmpty) {
        // create a mutable table
        std::vector<uint64_t> roots = { constant::UNKNOWN_EXTENT, constant::UNKNOWN_EXTENT };
        auto mtable = _create_mtable(1000, 1, roots);

        // finalize the empty table
        roots = mtable->finalize();

        // create an access table
        auto table = _create_table(1000, 1, roots);

        // get a key that doesn't exist since the table is empty
        auto key = _create_key("aaaa");

        // ensure that it appears empty and everything works as expected (find, lower_bound, etc)
        ASSERT_TRUE(table->has_primary());
        ASSERT_TRUE(table->primary_lookup(key) == constant::UNKNOWN_EXTENT);
        ASSERT_TRUE(table->lower_bound(key) == table->end());
        ASSERT_TRUE(table->begin() == table->end());
        ASSERT_TRUE(table->index(1)->begin() == table->index(1)->end());
    }

    TEST_F(Table_Test, Inserts) {
        // create a mutable table
        std::vector<uint64_t> roots = { constant::UNKNOWN_EXTENT, constant::UNKNOWN_EXTENT };
        auto mtable = _create_mtable(1001, 1, roots);

        // insert a number of rows
        _populate_table(mtable, 1);

        // finalize the table
        roots = mtable->finalize();

        // create an access table
        auto table = _create_table(1001, 1, roots);

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        int count = 0;
        std::string prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(row), prev);
            }

            prev = _fields->at(1)->get_text(row);
            ++count;
        }
        ASSERT_EQ(count, 5000);

        // XXX verify the secondary index
    }

    TEST_F(Table_Test, SingleXactMutations) {
        // create a mutable table
        std::vector<uint64_t> roots = { constant::UNKNOWN_EXTENT, constant::UNKNOWN_EXTENT };
        auto mtable = _create_mtable(1002, 1, roots);

        // insert a number of rows
        _populate_table(mtable, 1);

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
            mtable->remove(_create_key(key), 1, constant::UNKNOWN_EXTENT);
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
            mtable->update(value, 1, constant::UNKNOWN_EXTENT);
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
            mtable->upsert(value, 1, constant::UNKNOWN_EXTENT);
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
            mtable->upsert(value, 1, constant::UNKNOWN_EXTENT);
        }

        // finalize the table
        roots = mtable->finalize();

        // create an access table
        auto table = _create_table(1002, 1, roots);

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        int count = 0;
        std::string prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(row), prev);
            }

            // check that the removes and updates occurred correctly
            uint64_t table_id = _fields->at(0)->get_uint64(row);
            ASSERT_NE(table_id, 1); // all table_id == 1 should be removed
            if (table_id == 6) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 100); // updates
            }
            if (table_id == 1500) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 1); // upsert inserts
            }
            if (table_id == 3) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 103); // upsert updates
            }

            prev = _fields->at(1)->get_text(row);
            ++count;
        }
        ASSERT_EQ(count, 5000); // removed 5, upserted 5

        // verify the secondary index
        auto secondary = table->index(1);

        count = 0;
        uint64_t table_id = 0;
        auto table_id_f = secondary->get_schema()->get_field("table_id");
        for (auto row : *secondary) {
            auto current = table_id_f->get_uint64(row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000);
    }

    TEST_F(Table_Test, MultiXactMutations) {
        uint64_t access_xid, target_xid = 1;

        // create a mutable table
        std::vector<uint64_t> roots = { constant::UNKNOWN_EXTENT, constant::UNKNOWN_EXTENT };
        auto mtable = _create_mtable(1003, target_xid, roots);

        // insert a number of rows
        _populate_table(mtable, 1);

        // finalize the table
        roots = mtable->finalize();
        access_xid = target_xid;

        // create an access table for lookup
        auto table = _create_table(1003, access_xid, roots);

        // create a new mutable table with a later XID target
        ++target_xid;
        mtable = _create_mtable(1003, target_xid, roots);

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
            mtable->remove(_create_key(key), target_xid, constant::UNKNOWN_EXTENT);
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
            mtable->remove(search_key, target_xid, extent_id);
        }

        // finalize the table
        roots = mtable->finalize();
        access_xid = target_xid;

        // create an access table
        table = _create_table(1003, access_xid, roots);

        // verify the data at this point
        int count = 0;
        std::string prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(row), prev);
            }
            prev = _fields->at(1)->get_text(row);

            // check that the removes and updates occurred correctly
            uint64_t table_id = _fields->at(0)->get_uint64(row);
            ASSERT_NE(table_id, 1); // all table_id == 1 should be removed
            ASSERT_NE(table_id, 2); // all table_id == 2 should be removed

            ++count;
        }
        ASSERT_EQ(count, 5000 - 10); // removed 10

        // verify the secondary index
        auto secondary = table->index(1);

        count = 0;
        uint64_t table_id = 0;
        auto table_id_f = secondary->get_schema()->get_field("table_id");
        for (auto row : *secondary) {
            auto current = table_id_f->get_uint64(row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000 - 10);

        // create a new mutable table with a later XID target
        ++target_xid;
        mtable = _create_mtable(1003, target_xid, roots);

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
            mtable->update(value, target_xid, constant::UNKNOWN_EXTENT);
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
            mtable->update(update_values[i], target_xid, extent_id);
        }

        // finalize the table
        roots = mtable->finalize();
        access_xid = target_xid;

        // create an access table
        table = _create_table(1003, access_xid, roots);

        // verify the data at this point
        count = 0;
        prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(row), prev);
            }
            prev = _fields->at(1)->get_text(row);

            // check that the removes and updates occurred correctly
            uint64_t table_id = _fields->at(0)->get_uint64(row);
            ASSERT_NE(table_id, 1); // all table_id == 1 should be removed
            ASSERT_NE(table_id, 2); // all table_id == 2 should be removed
            if (table_id == 6) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 100); // updates
            }
            if (table_id == 7) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 101); // updates
            }

            ++count;
        }
        ASSERT_EQ(count, 5000 - 10); // removed 10

        // verify the secondary index
        secondary = table->index(1);

        count = 0;
        table_id = 0;
        for (auto row : *secondary) {
            auto current = secondary->get_schema()->get_field("table_id")->get_uint64(row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000 - 10);

        // create a new mutable table with a later XID target
        ++target_xid;
        mtable = _create_mtable(1003, target_xid, roots);

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
            mtable->upsert(value, target_xid, constant::UNKNOWN_EXTENT);
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
            mtable->upsert(upsert_values[i], target_xid, extent_id);
        }

        // finalize the table
        roots = mtable->finalize();
        access_xid = target_xid;

        // create an access table
        table = _create_table(1003, access_xid, roots);

        count = 0;
        prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(row), prev);
            }
            prev = _fields->at(1)->get_text(row);

            // check that the removes and updates occurred correctly
            uint64_t table_id = _fields->at(0)->get_uint64(row);
            ASSERT_NE(table_id, 1); // all table_id == 1 should be removed
            ASSERT_NE(table_id, 2); // all table_id == 2 should be removed
            if (table_id == 6) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 100); // updates
            }
            if (table_id == 7) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 101); // updates
            }
            if (table_id == 1500) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 1); // upsert inserts
            }
            if (table_id == 1501) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 2); // known upsert inserts
            }

            ++count;
        }
        ASSERT_EQ(count, 5000); // removed 10, upserted 10

        // verify the secondary index
        secondary = table->index(1);

        count = 0;
        table_id = 0;
        for (auto row : *secondary) {
            auto current = secondary->get_schema()->get_field("table_id")->get_uint64(row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000);

        // create a new mutable table with a later XID target
        ++target_xid;
        mtable = _create_mtable(1003, target_xid, roots);

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
            mtable->upsert(value, target_xid, constant::UNKNOWN_EXTENT);
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
            mtable->upsert(upsert_values[i], target_xid, extent_id);
        }

        // finalize the table
        roots = mtable->finalize();
        access_xid = target_xid;

        // create an access table
        table = _create_table(1003, access_xid, roots);

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        count = 0;
        prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(_fields->at(1)->get_text(row), prev);
            }
            prev = _fields->at(1)->get_text(row);

            // check that the removes and updates occurred correctly
            uint64_t table_id = _fields->at(0)->get_uint64(row);
            ASSERT_NE(table_id, 1); // all table_id == 1 should be removed
            ASSERT_NE(table_id, 2); // all table_id == 2 should be removed
            if (table_id == 6) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 100); // updates
            }
            if (table_id == 7) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 101); // updates
            }
            if (table_id == 1500) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 1); // upsert inserts
            }
            if (table_id == 1501) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 2); // known upsert inserts
            }
            if (table_id == 3) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 103); // upsert updates
            }
            if (table_id == 4) {
                ASSERT_EQ(_fields->at(2)->get_uint64(row), 104); // upsert updates
            }

            ++count;
        }
        ASSERT_EQ(count, 5000); // removed 5, upserted 5

        // verify the secondary index
        secondary = table->index(1);

        count = 0;
        table_id = 0;
        for (auto row : *secondary) {
            auto current = secondary->get_schema()->get_field("table_id")->get_uint64(row);
            ASSERT_LE(table_id, current);
            table_id = current;
            ++count;
        }
        ASSERT_EQ(count, 5000);
    }


    TEST_F(Table_Test, MultiThreadMutations) {
        uint64_t target_xid = 1;

        // create a mutable table
        std::vector<uint64_t> roots = { constant::UNKNOWN_EXTENT, constant::UNKNOWN_EXTENT };
        auto mtable = _create_mtable(1004, target_xid, roots);

        // insert a number of rows
        _populate_table(mtable, target_xid);

        // finalize the table
        roots = mtable->finalize();
        uint64_t access_xid = target_xid;

        // create an access table and identify extents to be mutated
        auto table = _create_table(1004, access_xid, roots);

        std::map<uint64_t, std::vector<TuplePtr>> tuple_map;

        csv::CSVReader reader2("test_btree_simple.csv");
        int count = 0;
        for (auto &&r : reader2) {
            auto csvtuple = std::make_shared<FieldTuple>(_csv_fields, r);
            auto search_key = _schema->tuple_subset(csvtuple, _primary_keys);

            uint64_t extent_id = table->primary_lookup(search_key);

            auto value = _create_value(count++,
                                       search_key->field(0)->get_text(search_key->row()),
                                       20000);

            tuple_map[extent_id].push_back(value);
        }

        // create a new mutable table with a later XID target
        ++target_xid;
        mtable = _create_mtable(1004, target_xid, roots);

        // mutate the individual extents in separate concurrent threads
        PhasedThreadTest<Request> tester;

        for (auto &entry : tuple_map) {
            auto request = std::make_shared<Request>(mtable, target_xid, entry.first, std::move(entry.second));
            tester.add_request(request);
        }

        // finalize and verify the table
        tester.set_verify([this, mtable, target_xid]() {
            // create an access table
            auto roots = mtable->finalize();

            auto table = _create_table(1004, target_xid, roots);

            // ensure that it has all of the expected rows through both the primary and secondary index
            // and that everything else works as expected (find, lower_bound, etc)
            int count = 0;
            std::string prev = "";
            for (auto &row : *table) {
                if (prev != "") {
                    ASSERT_GT(_fields->at(1)->get_text(row), prev);
                }
                prev = _fields->at(1)->get_text(row);

                uint64_t offset = _fields->at(2)->get_uint64(row);
                ASSERT_EQ(offset, 20000);

                ++count;
            }
            ASSERT_EQ(count, 5000);

            // verify the secondary index
            auto secondary = table->index(1);

            count = 0;
            uint64_t table_id = 0;
            auto table_id_f = secondary->get_schema()->get_field("table_id");
            for (auto row : *secondary) {
                auto current = table_id_f->get_uint64(row);
                ASSERT_LE(table_id, current);
                table_id = current;
                ++count;
            }
            ASSERT_EQ(count, 5000);

        });

        // run the phase using 4 threads (just one phase here)
        tester.run(4);
    }

}
