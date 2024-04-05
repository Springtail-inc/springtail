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
                    { "name", 1, SchemaType::TEXT, false },
                    { "offset", 2, SchemaType::UINT64, false }
                });
            _schema = std::make_shared<ExtentSchema>(columns);

            _read_cache = std::make_shared<LruObjectCache<std::pair<std::filesystem::path, uint64_t>, Extent>>(1024*1024);
            _write_cache = MutableBTree::create_cache(2*1024*1024);

            _primary_keys = std::vector<std::string>({"name"});
            _secondary_keys = { std::vector<std::string>({"table_id"}) };

            _data_cache = std::make_shared<DataCache>(true);

            std::filesystem::create_directories("/tmp/test_table/1000");
            std::filesystem::create_directory("/tmp/test_table/1001");
            std::filesystem::create_directory("/tmp/test_table/1002");
        }

        void TearDown() override {
            // remove any files created during the run
            std::filesystem::remove_all("/tmp/test_table");
        }

        ExtentSchemaPtr _schema;
        std::shared_ptr<ExtentCache> _read_cache;
        MutableBTree::PageCachePtr _write_cache;
        std::vector<std::string> _primary_keys;
        std::vector<std::vector<std::string>> _secondary_keys;

        DataCachePtr _data_cache;

        std::shared_ptr<Tuple>
        _create_key(const std::string &name)
        {
            auto key_fields = std::make_shared<FieldArray>(1);
            key_fields->at(0) = std::make_shared<ConstTypeField<std::string>>(name);
            return std::make_shared<FieldTuple>(key_fields, nullptr);
        }

        /**
         * Request for multi-threading tests.
         */
        class Request {
        public:
            enum Type {
                INSERT,
                REMOVE
            };

        public:
            /** add row constructor */
            Request(std::shared_ptr<MutableBTree> btree, const Type &type, TuplePtr tuple)
                : _btree(btree),
                  _type(type),
                  _tuple(tuple)
            { }

            /**
             * @brief Overload () for execution from worker thread.
             *        Main entry from worker thread
             */
            void operator()() {
                switch (_type) {
                case(Type::INSERT):
                    _btree->insert(_tuple);
                    break;
                case (Type::REMOVE):
                    _btree->remove(_tuple);
                    break;
                }
            }

        private:
            std::shared_ptr<MutableBTree> _btree;
            Type _type;
            TuplePtr _tuple;
        };
        typedef std::shared_ptr<Request> RequestPtr;
    };

    TEST_F(Table_Test, CreateEmpty) {
        // create a mutable table
        std::vector<uint64_t> roots = { constant::UNKNOWN_EXTENT, constant::UNKNOWN_EXTENT };
        auto mtable = std::make_shared<MutableTable>(1000,
                                                     1,
                                                     roots,
                                                     "/tmp/test_table/1000",
                                                     _primary_keys,
                                                     _secondary_keys,
                                                     _schema,
                                                     _data_cache,
                                                     _write_cache,
                                                     _read_cache);

        // finalize the empty table
        roots = mtable->finalize();

        // create an access table
        auto table = std::make_shared<Table>(1000,
                                             1,
                                             "/tmp/test_table/1000",
                                             _primary_keys,
                                             _secondary_keys,
                                             roots,
                                             _schema,
                                             _read_cache);

        auto key = _create_key("aaaa");

        // ensure that it appears empty and everything works as expected (find, lower_bound, etc)
        ASSERT_TRUE(table->has_primary());
        ASSERT_TRUE(table->primary_lookup(key) == constant::UNKNOWN_EXTENT);
        ASSERT_TRUE(table->lower_bound(key) == table->end());
        ASSERT_TRUE(table->begin() == table->end());
        ASSERT_TRUE(table->secondary(0)->begin(1) == table->secondary(0)->end());
    }

    TEST_F(Table_Test, Inserts) {
        // create a mutable table
        std::vector<uint64_t> roots = { constant::UNKNOWN_EXTENT, constant::UNKNOWN_EXTENT };
        auto mtable = std::make_shared<MutableTable>(1001,
                                                     1,
                                                     roots,
                                                     "/tmp/test_table/1001",
                                                     _primary_keys,
                                                     _secondary_keys,
                                                     _schema,
                                                     _data_cache,
                                                     _write_cache,
                                                     _read_cache);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        // insert a number of rows
        csv::CSVReader reader("test_btree_simple.csv");
        for (auto &&r : reader) {
            // insert data to the tree
            mtable->insert(std::make_shared<FieldTuple>(csv_fields, r), 1, constant::UNKNOWN_EXTENT);
        }

        // finalize the table
        roots = mtable->finalize();

        // create an access table
        auto table = std::make_shared<Table>(1001,
                                             1,
                                             "/tmp/test_table/1001",
                                             _primary_keys,
                                             _secondary_keys,
                                             roots,
                                             _schema,
                                             _read_cache);

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        int count = 0;
        std::string prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(fields->at(1)->get_text(row), prev);
            }

            prev = fields->at(1)->get_text(row);
            ++count;
        }
        ASSERT_EQ(count, 5000);

        // XXX verify the secondary index
    }

    TEST_F(Table_Test, SingleXactMutations) {
        // create a mutable table
        std::vector<uint64_t> roots = { constant::UNKNOWN_EXTENT, constant::UNKNOWN_EXTENT };
        auto mtable = std::make_shared<MutableTable>(1002,
                                                     1,
                                                     roots,
                                                     "/tmp/test_table/1002",
                                                     _primary_keys,
                                                     _secondary_keys,
                                                     _schema,
                                                     _data_cache,
                                                     _write_cache,
                                                     _read_cache);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        // insert a number of rows
        csv::CSVReader reader("test_btree_simple.csv");
        for (auto &&r : reader) {
            // insert data to the tree
            mtable->insert(std::make_shared<FieldTuple>(csv_fields, r), 1, constant::UNKNOWN_EXTENT);
        }

        // XXX remove some rows
        auto make_key = [](const std::string &key) {
            auto k = std::make_shared<ConstTypeField<std::string>>(key);
            std::vector<ConstFieldPtr> v({ k });
            return std::make_shared<ValueTuple>(v);
        };

        mtable->remove(make_key("iclausnerc5"), 1, constant::UNKNOWN_EXTENT);
        mtable->remove(make_key("cmiltonwhitecb"), 1, constant::UNKNOWN_EXTENT);
        mtable->remove(make_key("rllorentecf"), 1, constant::UNKNOWN_EXTENT);
        mtable->remove(make_key("hdotsonbn"), 1, constant::UNKNOWN_EXTENT);
        mtable->remove(make_key("bbootellbj"), 1, constant::UNKNOWN_EXTENT);

        // XXX update some rows

        // finalize the table
        roots = mtable->finalize();

        // create an access table
        auto table = std::make_shared<Table>(1002,
                                             1,
                                             "/tmp/test_table/1002",
                                             _primary_keys,
                                             _secondary_keys,
                                             roots,
                                             _schema,
                                             _read_cache);

        // ensure that it has all of the inserted rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
        int count = 0;
        std::string prev = "";
        for (auto &row : *table) {
            if (prev != "") {
                ASSERT_GT(fields->at(1)->get_text(row), prev);
            }

            prev = fields->at(1)->get_text(row);
            ++count;
        }
        ASSERT_EQ(count, 5000 - 5);

        // XXX verify the secondary index
    }

    TEST_F(Table_Test, MultiXactMutations) {
        // create a mutable table

        // insert a number of rows

        // finalize the table

        // create a new mutable table with a later XID target

        // remove some rows

        // update some rows

        // finalize the table

        // create an access table

        // ensure that it has all of the expected rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
    }


    TEST_F(Table_Test, MultiThreadMutations) {
        // create a mutable table

        // insert a large number of rows

        // finalize the table

        // create an access table and identify extents to be mutated

        // create a new mutable table with a later XID target

        // mutate the extents in separate concurrent threads

        // finalize the table

        // create an access table

        // ensure that it has all of the expected rows through both the primary and secondary index
        // and that everything else works as expected (find, lower_bound, etc)
    }

}
