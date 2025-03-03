#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/environment.hh>
#include <common/threaded_test.hh>

#include <storage/csv_field.hh>
#include <storage/btree.hh>
#include <storage/mutable_btree.hh>

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
     * Framework for Basic BTree testing.
     */
    class BTree_Test : public testing::TestWithParam<CacheSize> {
    protected:
        void SetUp() override {
            // set the cache size from the parameters
            CacheSize sizes = GetParam();
            std::string overrides = std::format("storage.data_cache_size={};storage.page_cache_size={};storage.btree_cache_size={};storage.max_extent_per_page={}",
                                                sizes.data_cache_size, sizes.page_cache_size, sizes.btree_cache_size, sizes.max_extent_per_page);
            ::setenv(environment::ENV_OVERRIDE, overrides.c_str(), 1);

            springtail_init_test(std::nullopt, LOG_ALL ^ LOG_STORAGE);

            // construct a schema for testing
            std::vector<SchemaColumn> columns({
                    { "table_id", 0, SchemaType::UINT64, 0, false, 1},
                    { "name", 1, SchemaType::TEXT, 0, false, 0 },
                    { "offset", 2, SchemaType::UINT64, 0, false },
                    { "index", 3, SchemaType::UINT16, 0, false, 2 }
                });
            _schema = std::make_shared<ExtentSchema>(columns);
            _keys = std::vector<std::string>({"name", "table_id", "index"});

            _base_dir = std::filesystem::temp_directory_path() / "test_btree";
            std::filesystem::create_directories(_base_dir);

            _table_id_f = _schema->get_field("table_id");
            _name_f = _schema->get_field("name");
            _offset_f = _schema->get_field("offset");

            _fields = _schema->get_fields();
            _csv_fields = std::make_shared<FieldArray>();
            for (int i = 0; i < _fields->size() - 1; i++) {
                auto &&field = _fields->at(i);
                _csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
            }
        }

        void TearDown() override {
            // remove any files created during the run
            std::filesystem::remove_all(_base_dir);
            springtail_shutdown();
        }

        ExtentSchemaPtr _schema;
        FieldPtr _table_id_f, _name_f, _offset_f;
        FieldArrayPtr _fields, _csv_fields;

        std::vector<std::string> _keys;

        std::filesystem::path _base_dir;

        std::shared_ptr<MutableBTree>
        _create_mutable_btree(const std::filesystem::path &name,
                              uint64_t xid)
        {
            // construct a mutable b-tree for inserting data
            auto btree = std::make_shared<MutableBTree>(name, _keys, _schema, xid);

            btree->init_empty();
            return btree;
        }

        std::shared_ptr<MutableBTree>
        _get_mutable_btree(const std::filesystem::path &name,
                           uint64_t xid,
                           uint64_t extent_id)
        {
            // construct a mutable b-tree for inserting data
            auto btree = std::make_shared<MutableBTree>(name, _keys, _schema, xid);
            btree->init(extent_id);
            return btree;
        }

        std::shared_ptr<Tuple>
        _create_key(const std::string &name,
                    uint64_t table_id,
                    uint16_t idx)
        {
            auto key_fields = std::make_shared<FieldArray>(3);
            key_fields->at(0) = std::make_shared<ConstTypeField<std::string>>(name);
            key_fields->at(1) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
            key_fields->at(2) = std::make_shared<ConstTypeField<uint16_t>>(idx);
            return std::make_shared<FieldTuple>(key_fields, nullptr);
        }

        TuplePtr
        _gen_tuple(csv::CSVRow &row, uint16_t idx) {
            auto extra = std::make_shared<FieldArray>();
            extra->push_back(std::make_shared<ConstTypeField<uint16_t>>(idx));

            return std::make_shared<KeyValueTuple>(_csv_fields, extra, row);
        }

        void
        _populate_btree(MutableBTreePtr btree, uint16_t idx) {
            // pull data to insert
            csv::CSVReader reader("test_btree_simple.csv");
            for (auto &&r : reader) {
                // insert data to the tree
                btree->insert(_gen_tuple(r, idx));
            }
        }

        void
        _verify_names(BTreePtr tree, int target) {
            int count = 0;

            std::string prev = "";
            auto ib = tree->begin();
            auto ie = tree->end();
            for (; ib != ie; ++ib) {
                auto row = *ib;
                if (prev != "") {
                    ASSERT_GE(_name_f->get_text(row), prev);
                }

                prev = _name_f->get_text(row);
                ++count;
            }

            ASSERT_EQ(count, target);
        }

        void
        _verify_unique_names(BTreePtr tree, int target) {
            int count = 0;

            std::string prev = "";
            std::map<std::string, int> counts;
            for (auto row : *tree) {
                if (_name_f->get_text(row) < prev) {
                    SPDLOG_ERROR("{} < {}", _name_f->get_text(row), prev);
                }

                if (prev != "") {
                    ASSERT_GE(_name_f->get_text(row), prev);
                }

                prev = _name_f->get_text(row);
                ++counts[prev];
                ++count;
            }

            for (auto &&entry : counts) {
                if (entry.second > 1) {
                    SPDLOG_INFO("{} = {}", entry.first, entry.second);
                }
            }

            ASSERT_EQ(count, target);
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

    TEST_P(BTree_Test, Insert10) {
        uint64_t xid = 1;

        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "Insert10", xid++);

        csv::CSVReader reader("test_btree_simple.csv");

        auto r = reader.begin();
        for (int i = 0; i < 10; i++) {
            // insert data to the tree
            btree->insert(_gen_tuple(*r, 0));
            ++r;
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        // now read the tree back and make sure there are the right number of entries and that they are in-order
        auto tree = std::make_shared<BTree>(_base_dir / "Insert10", xid, _schema, offset);
        _verify_names(tree, 10);

        btree.reset();
        tree.reset();

        StorageCache::get_instance()->validate();
    }

    TEST_P(BTree_Test, InsertAll) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "InsertAll", 1);

        // insert all of the entries from the CSV file
        _populate_btree(btree, 0);

        // finalize the tree
        uint64_t offset = btree->finalize();

        // now read the tree back and make sure there are the right number of entries and that they are in-order
        auto tree = std::make_shared<BTree>(_base_dir / "InsertAll", 1, _schema, offset);
        _verify_names(tree, 5000);
    }

    TEST_P(BTree_Test, Search) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "Search", 1);

        _populate_btree(btree, 0);

        // finalize the tree
        uint64_t offset = btree->finalize();

        // get a pointer to the read-only btree
        auto tree = std::make_shared<BTree>(_base_dir / "Search", 1, _schema, offset);
        auto begin_i = tree->begin();
        auto end_i = tree->end();

        // search for an entry before all of the keys
        {
            auto tuple = _create_key("aaaa", 0, 0);

            auto find_i = tree->find(tuple);
            auto lb_i = tree->lower_bound(tuple);
            auto ffu_i = tree->lower_bound(tuple, true);
            auto iub_i = tree->inverse_upper_bound(tuple);

            ASSERT_TRUE(find_i == end_i);
            ASSERT_TRUE(lb_i == begin_i);
            ASSERT_TRUE(ffu_i == begin_i);
            ASSERT_TRUE(iub_i == end_i);
        }

        // search for the first entry in the tree
        {
            auto tuple = _create_key("aabbatini8y", 323, 0);

            auto find_i = tree->find(tuple);

            ASSERT_EQ(_name_f->get_text(*find_i), "aabbatini8y");
            ASSERT_EQ(_table_id_f->get_uint64(*find_i), 323);
            ASSERT_EQ(_offset_f->get_uint64(*find_i), 6448);

            auto lb_i = tree->lower_bound(tuple);
            auto ffu_i = tree->lower_bound(tuple, true);
            auto iub_i = tree->inverse_upper_bound(tuple);

            ASSERT_EQ(lb_i, find_i);
            ASSERT_EQ(ffu_i, find_i);
            ASSERT_EQ(iub_i, end_i);
        }

        // search for an existing entry in the middle
        {
            auto tuple = _create_key("mplainu", 31, 0);

            auto find_i = tree->find(tuple);

            ASSERT_EQ(_table_id_f->get_uint64(*find_i), 31);
            ASSERT_EQ(_name_f->get_text(*find_i), "mplainu");
            ASSERT_EQ(_offset_f->get_uint64(*find_i), 30122);

            auto lb_i = tree->lower_bound(tuple);
            auto ffu_i = tree->lower_bound(tuple, true);
            auto iub_i = tree->inverse_upper_bound(tuple);

            ASSERT_EQ(lb_i, find_i);
            ASSERT_EQ(ffu_i, find_i);

            --find_i;
            ASSERT_EQ(iub_i, find_i);
        }

        // search for a non-existing entry before the last entry
        {
            auto tuple = _create_key("m", 0, 0);

            auto find_i = tree->find(tuple);
            auto lb_i = tree->lower_bound(tuple);
            auto ffu_i = tree->lower_bound(tuple, true);
            auto iub_i = tree->inverse_upper_bound(tuple);

            ASSERT_EQ(find_i, end_i);

            ASSERT_EQ(_table_id_f->get_uint64(*lb_i), 526);
            ASSERT_EQ(_name_f->get_text(*lb_i), "mabrahimel");
            ASSERT_EQ(_offset_f->get_uint64(*lb_i), 33466);

            ASSERT_EQ(ffu_i, lb_i);

            ASSERT_EQ(_table_id_f->get_uint64(*iub_i), 997);
            ASSERT_EQ(_name_f->get_text(*iub_i), "lzipsellro");
            ASSERT_EQ(_offset_f->get_uint64(*iub_i), 86407);
        }

        // search for a non-existing entry past the last entry
        {
            auto tuple = _create_key("zzzzzzzzzz", 0, 0);

            auto find_i = tree->find(tuple);
            auto lb_i = tree->lower_bound(tuple);
            auto ffu_i = tree->lower_bound(tuple, true);
            auto iub_i = tree->inverse_upper_bound(tuple);

            ASSERT_EQ(find_i, end_i);
            ASSERT_EQ(lb_i, end_i);

            ASSERT_EQ(_table_id_f->get_uint64(*ffu_i), 430);
            ASSERT_EQ(_name_f->get_text(*ffu_i), "zwoolertonbx");
            ASSERT_EQ(_offset_f->get_uint64(*ffu_i), 92729);

            ASSERT_EQ(iub_i, ffu_i);
        }
    }

    TEST_P(BTree_Test, InsertAndRemove) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "InsertAndRemove", 1);

        // populate the tree
        _populate_btree(btree, 0);

        // finalize the tree
        uint64_t offset_1 = btree->finalize();

        // check XID 1 for all entries
        auto tree = std::make_shared<BTree>(_base_dir / "InsertAndRemove", 1, _schema, offset_1);
        _verify_names(tree, 5000);

        // move to the next XID
        btree = _get_mutable_btree(_base_dir / "InsertAndRemove", 2, offset_1);

        // now remove every other row in the csv
        int evens = 0;
        auto reader = csv::CSVReader("test_btree_simple.csv");
        for (auto &&r : reader) {
            // remove data to the tree
            if (evens % 2) {
                btree->remove(_gen_tuple(r, 0));
            }
            ++evens;
        }

        // finalize the tree
        auto offset_2 = btree->finalize();

        // now read the tree back and make sure there are the right number of entries and that they are in-order

        // check XID 1 for all entries
        tree = std::make_shared<BTree>(_base_dir / "InsertAndRemove", 1, _schema, offset_1);
        _verify_names(tree, 5000);

        // check XID 2 for half the entries
        tree = std::make_shared<BTree>(_base_dir / "InsertAndRemove", 2, _schema, offset_2);
        _verify_names(tree, 2500);
    }

    TEST_P(BTree_Test, InsertAndRemoveAll) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "InsertAndRemoveAll", 1);

        // pull data to insert
        _populate_btree(btree, 0);

        // finalize the tree
        uint64_t offset_1 = btree->finalize();

        // check XID 1 for all entries
        auto tree = std::make_shared<BTree>(_base_dir / "InsertAndRemoveAll", 1, _schema, offset_1);
        _verify_names(tree, 5000);

        // set the next XID
        btree = _get_mutable_btree(_base_dir / "InsertAndRemoveAll", 2, offset_1);

        // now remove every row in the csv
        auto reader = csv::CSVReader("test_btree_simple.csv");
        for (auto &r : reader) {
            // remove data from the tree
            btree->remove(_gen_tuple(r, 0));
        }

        // finalize the tree
        auto offset_2 = btree->finalize();
        btree = nullptr;

        // now read the tree back and make sure there are the right number of entries and that they are in-order

        // check XID 1 for all entries
        tree = std::make_shared<BTree>(_base_dir / "InsertAndRemoveAll", 1, _schema, offset_1);
        _verify_names(tree, 5000);

        // check XID 2 for no entries
        tree = std::make_shared<BTree>(_base_dir / "InsertAndRemoveAll", 2, _schema, offset_2);
        _verify_names(tree, 0);
    }

    TEST_P(BTree_Test, InsertMany) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "InsertMany", 1);

        // insert 50k entries
        for (int i = 0; i < 10; i++) {
            _populate_btree(btree, i);
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        auto tree = std::make_shared<BTree>(_base_dir / "InsertMany", 1, _schema, offset);

        // check for all entries
        _verify_names(tree, 50000);
    }

    TEST_P(BTree_Test, ThreadedInserts) {
        PhasedThreadTest<Request> tester;

        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "ThreadedInserts", 1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        // preapare 50k inserts
        for (int i = 0; i < 10; i++) {
            csv::CSVReader reader("test_btree_simple.csv");
            for (auto &&r : reader) {
                auto value_tuple = std::make_shared<ValueTuple>(_gen_tuple(r, i));
                auto request = std::make_shared<Request>(btree, Request::Type::INSERT, value_tuple);

                tester.add_request(request);
            }
        }

        // verify the entries
        tester.set_verify([this, btree]() {
            uint64_t offset = btree->finalize();

            auto tree = std::make_shared<BTree>(_base_dir / "ThreadedInserts", 1, _schema, offset);

            // check for all entries
            _verify_unique_names(tree, 50000);
        });

        // run the phases using 4 threads (just one phase here)
        tester.run(4);
    }

    TEST_P(BTree_Test, ThreadedInsertAndRemove) {
        PhasedThreadTest<Request> tester;

        std::filesystem::path filename = _base_dir / "ThreadedInsertAndRemove";

        // get a mutable btree to perform inserts
        auto btree1 = std::make_shared<MutableBTree>(filename, _keys, _schema, 1);
        btree1->init_empty();

        auto btree2 = std::make_shared<MutableBTree>(filename, _keys, _schema, 2);

        // preapare 5k inserts
        csv::CSVReader reader("test_btree_simple.csv");
        for (auto &&r : reader) {
            auto value_tuple = std::make_shared<ValueTuple>(_gen_tuple(r, 0));
            auto request = std::make_shared<Request>(btree1, Request::Type::INSERT, value_tuple);

            tester.add_request(request);
        }

        // verify the entries
        tester.set_verify([this, filename, btree1, btree2]() {
            uint64_t offset = btree1->finalize();

            auto tree = std::make_shared<BTree>(filename, 1, _schema, offset);

            // check for all entries
            _verify_unique_names(tree, 5000);

            // set the next XID
            btree2->init(offset);
        });

        // move to phase 2
        tester.next_phase();

        // prepare 5k removes
        csv::CSVReader reader2("test_btree_simple.csv");
        for (auto &&r : reader2) {
            auto value_tuple = std::make_shared<ValueTuple>(_gen_tuple(r, 0));
            auto request = std::make_shared<Request>(btree2, Request::Type::REMOVE, value_tuple);

            tester.add_request(request);
        }

        // verify the entries
        tester.set_verify([this, btree2]() {
            uint64_t offset = btree2->finalize();

            auto tree = std::make_shared<BTree>(_base_dir / "ThreadedInsertAndRemove", 2, _schema, offset);

            // check for all entries
            _verify_names(tree, 0);
        });

        // run the phases using 4 threads (just one phase here)
        tester.run(4);
    }

    TEST_P(BTree_Test, ThreadedInsertsTwoFiles) {
        PhasedThreadTest<Request> tester;

        // get mutable btrees to perform inserts
        auto btree1 = _create_mutable_btree(_base_dir / "ThreadedInsertsOne", 1);
        auto btree2 = _create_mutable_btree(_base_dir / "ThreadedInsertsTwo", 1);

        // preapare 100k inserts across two trees
        for (int i = 0; i < 10; i++) {
            csv::CSVReader reader("test_btree_simple.csv");
            for (auto &&r : reader) {
                auto value_tuple = std::make_shared<ValueTuple>(_gen_tuple(r, i));

                auto request1 = std::make_shared<Request>(btree1, Request::Type::INSERT, value_tuple);
                tester.add_request(request1);

                auto request2 = std::make_shared<Request>(btree2, Request::Type::INSERT, value_tuple);
                tester.add_request(request2);
            }
        }

        // verify the entries
        tester.set_verify([this, btree1, btree2]() {
            uint64_t offset1 = btree1->finalize();
            uint64_t offset2 = btree2->finalize();

            // check the first file
            auto tree = std::make_shared<BTree>(_base_dir / "ThreadedInsertsOne", 1, _schema, offset1);

            // check for all entries
            _verify_unique_names(tree, 50000);

            // check the second file
            tree = std::make_shared<BTree>(_base_dir / "ThreadedInsertsTwo", 1, _schema, offset2);

            // check for all entries
            _verify_unique_names(tree, 50000);
        });

        // run the phases using 4 threads (just one phase here)
        tester.run(4);
    }

    INSTANTIATE_TEST_CASE_P(BTree_Test,
                            BTree_Test,
                            ::testing::Values(CacheSize{ 16384, 16384, 512, 16 },
                                              CacheSize{ 32, 32, 8, 4 }));
}
