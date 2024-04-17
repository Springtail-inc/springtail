#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/threaded_test.hh>

#include <storage/csv_field.hh>
#include <storage/btree.hh>
#include <storage/mutable_btree.hh>

using namespace springtail;

namespace {

    /**
     * Framework for Basic BTree testing.
     */
    class BTree_Test : public testing::Test {
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
            _keys = std::vector<std::string>({"name", "table_id"});

            _write_cache = MutableBTree::create_cache(2*1024*1024);

            _base_dir = std::filesystem::temp_directory_path() / "test_btree";
            std::filesystem::create_directories(_base_dir);

            _table_id_f = _schema->get_field("table_id");
            _name_f = _schema->get_field("name");
            _offset_f = _schema->get_field("offset");
        }

        void TearDown() override {
            // remove any files created during the run
            std::filesystem::remove_all(_base_dir);
        }

        ExtentSchemaPtr _schema;
        FieldPtr _table_id_f, _name_f, _offset_f;

        std::shared_ptr<MutableBTree> _write_tree;
        std::shared_ptr<ExtentCache> _read_cache;
        MutableBTree::PageCachePtr _write_cache;
        std::vector<std::string> _keys;

        std::filesystem::path _base_dir;

        std::shared_ptr<MutableBTree>
        _create_mutable_btree(const std::filesystem::path &name,
                              uint64_t xid)
        {
            // construct a mutable b-tree for inserting data
            auto btree = std::make_shared<MutableBTree>(name, _keys, _write_cache, _schema);

            btree->init_empty();
            return btree;
        }

        std::shared_ptr<MutableBTree>
        _get_mutable_btree(const std::filesystem::path &name,
                           uint64_t xid,
                           uint64_t extent_id)
        {
            // construct a mutable b-tree for inserting data
            auto btree = std::make_shared<MutableBTree>(name, _keys, _write_cache, _schema);
            btree->init(extent_id);
            return btree;
        }

        std::shared_ptr<BTree>
        _get_btree(const std::filesystem::path &name,
                   uint64_t extent_id)
        {
            // construct a mutable b-tree for inserting data
            return std::make_shared<BTree>(name, _keys, _schema, _read_cache, 1, extent_id);
        }

        std::shared_ptr<Tuple>
        _create_key(const std::string &name,
                    uint64_t table_id)
        {
            auto key_fields = std::make_shared<FieldArray>(2);
            key_fields->at(0) = std::make_shared<ConstTypeField<std::string>>(name);
            key_fields->at(1) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
            return std::make_shared<FieldTuple>(key_fields, nullptr);
        }

        void
        _verify_names(BTreePtr tree, uint64_t xid, int target) {
            int count = 0;

            std::string prev = "";
            for (auto &&i = tree->begin(xid); i != tree->end(); ++i) {
                if (prev != "") {
                    ASSERT_GT(_name_f->get_text(*i), prev);
                }

                prev = _name_f->get_text(*i);
                ++count;
            }

            ASSERT_EQ(count, target);
        }

        void
        _verify_unique_names(BTreePtr tree, uint64_t xid, int target) {
            int count = 0;

            std::string prev = "";
            std::map<std::string, int> counts;
            for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
                if (_name_f->get_text(*i) < prev) {
                    SPDLOG_ERROR("{} < {}", _name_f->get_text(*i), prev);
                }

                if (prev != "") {
                    ASSERT_GE(_name_f->get_text(*i), prev);
                }

                prev = _name_f->get_text(*i);
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

    TEST_F(BTree_Test, Insert10) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "Insert10", 0);

        // set the XID
        btree->set_xid(1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        csv::CSVReader reader("test_btree_simple.csv");

        auto r = reader.begin();
        for (int i = 0; i < 10; i++) {
            // insert data to the tree
            btree->insert(std::make_shared<FieldTuple>(csv_fields, *r));
            ++r;
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        // now read the tree back and make sure there are the right number of entries and that they are in-order
        auto tree = _get_btree(_base_dir / "Insert10", offset);
        _verify_names(tree, 1, 10);
    }

    TEST_F(BTree_Test, InsertAll) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "InsertAll", 0);

        // set the XID
        btree->set_xid(1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        csv::CSVReader reader("test_btree_simple.csv");

        for (auto &&r : reader) {
            // insert data to the tree
            btree->insert(std::make_shared<FieldTuple>(csv_fields, r));
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        // now read the tree back and make sure there are the right number of entries and that they are in-order
        auto tree = _get_btree(_base_dir / "InsertAll", offset);
        _verify_names(tree, 1, 5000);
    }

    TEST_F(BTree_Test, Search) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "Search", 0);

        // set the XID
        btree->set_xid(1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        csv::CSVReader reader("test_btree_simple.csv");

        for (auto &&r : reader) {
            // insert data to the tree
            btree->insert(std::make_shared<FieldTuple>(csv_fields, r));
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        // get a pointer to the read-only btree
        auto tree = _get_btree(_base_dir / "Search", offset);
        auto begin_i = tree->begin(1);
        auto end_i = tree->end();

        // search for an entry before all of the keys
        {
            auto tuple = _create_key("aaaa", 0);

            auto find_i = tree->find(tuple, 1);
            auto lb_i = tree->lower_bound(tuple, 1);
            auto ffu_i = tree->lower_bound(tuple, 1, true);
            auto iub_i = tree->inverse_upper_bound(tuple, 1);

            ASSERT_TRUE(find_i == end_i);
            ASSERT_TRUE(lb_i == begin_i);
            ASSERT_TRUE(ffu_i == begin_i);
            ASSERT_TRUE(iub_i == end_i);
        }

        // search for the first entry in the tree
        {
            auto tuple = _create_key("aabbatini8y", 323);

            auto find_i = tree->find(tuple, 1);

            ASSERT_EQ(_name_f->get_text(*find_i), "aabbatini8y");
            ASSERT_EQ(_table_id_f->get_uint64(*find_i), 323);
            ASSERT_EQ(_offset_f->get_uint64(*find_i), 6448);

            auto lb_i = tree->lower_bound(tuple, 1);
            auto ffu_i = tree->lower_bound(tuple, 1, true);
            auto iub_i = tree->inverse_upper_bound(tuple, 1);

            ASSERT_EQ(lb_i, find_i);
            ASSERT_EQ(ffu_i, find_i);
            ASSERT_EQ(iub_i, end_i);
        }

        // search for an existing entry in the middle
        {
            auto tuple = _create_key("mplainu", 31);

            auto find_i = tree->find(tuple, 1);

            ASSERT_EQ(_table_id_f->get_uint64(*find_i), 31);
            ASSERT_EQ(_name_f->get_text(*find_i), "mplainu");
            ASSERT_EQ(_offset_f->get_uint64(*find_i), 30122);

            auto lb_i = tree->lower_bound(tuple, 1);
            auto ffu_i = tree->lower_bound(tuple, 1, true);
            auto iub_i = tree->inverse_upper_bound(tuple, 1);

            ASSERT_EQ(lb_i, find_i);
            ASSERT_EQ(ffu_i, find_i);

            --find_i;
            ASSERT_EQ(iub_i, find_i);
        }

        // search for a non-existing entry before the last entry
        {
            auto tuple = _create_key("m", 0);

            auto find_i = tree->find(tuple, 1);
            auto lb_i = tree->lower_bound(tuple, 1);
            auto ffu_i = tree->lower_bound(tuple, 1, true);
            auto iub_i = tree->inverse_upper_bound(tuple, 1);

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
            auto tuple = _create_key("zzzzzzzzzz", 0);

            auto find_i = tree->find(tuple, 1);
            auto lb_i = tree->lower_bound(tuple, 1);
            auto ffu_i = tree->lower_bound(tuple, 1, true);
            auto iub_i = tree->inverse_upper_bound(tuple, 1);

            ASSERT_EQ(find_i, end_i);
            ASSERT_EQ(lb_i, end_i);

            ASSERT_EQ(_table_id_f->get_uint64(*ffu_i), 430);
            ASSERT_EQ(_name_f->get_text(*ffu_i), "zwoolertonbx");
            ASSERT_EQ(_offset_f->get_uint64(*ffu_i), 92729);

            ASSERT_EQ(iub_i, ffu_i);
        }
    }

    TEST_F(BTree_Test, InsertAndRemove) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "InsertAndRemove", 0);

        // set the XID
        btree->set_xid(1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        csv::CSVReader reader("test_btree_simple.csv");

        for (auto &&r : reader) {
            // insert data to the tree
            btree->insert(std::make_shared<FieldTuple>(csv_fields, r));
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        // set the next XID
        btree->set_xid(2);

        // now remove every other row in the csv
        int evens = 0;
        reader = csv::CSVReader("test_btree_simple.csv");
        for (auto &&r : reader) {
            // remove data to the tree
            if (evens % 2) {
                // auto value = std::make_shared<ValueTuple>(std::make_shared<CSVTuple>(r, fields));
                // auto search_key = _schema->tuple_subset(value, _keys);
                // btree->remove(search_key);

                btree->remove(std::make_shared<FieldTuple>(csv_fields, r));
            }
            ++evens;
        }

        // finalize the tree
        offset = btree->finalize();

        // now read the tree back and make sure there are the right number of entries and that they are in-order
        auto tree = _get_btree(_base_dir / "InsertAndRemove", offset);

        // check XID 1 for all entries
        _verify_names(tree, 1, 5000);

        // check XID 2 for half the entries
        _verify_names(tree, 2, 2500);
    }

    TEST_F(BTree_Test, InsertAndRemoveAll) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "InsertAndRemoveAll", 0);

        // set the XID
        btree->set_xid(1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        csv::CSVReader reader("test_btree_simple.csv");

        for (auto &&r : reader) {
            // insert data to the tree
            btree->insert(std::make_shared<FieldTuple>(csv_fields, r));
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        // set the next XID
        btree->set_xid(2);

        // now remove every row in the csv
        reader = csv::CSVReader("test_btree_simple.csv");
        for (auto &&r : reader) {
            // remove data from the tree
            btree->remove(std::make_shared<FieldTuple>(csv_fields, r));
        }

        // finalize the tree
        offset = btree->finalize();

        // now read the tree back and make sure there are the right number of entries and that they are in-order
        auto tree = _get_btree(_base_dir / "InsertAndRemoveAll", offset);

        // check XID 1 for all entries
        _verify_names(tree, 1, 5000);

        // check XID 2 for no entries
        _verify_names(tree, 2, 0);
    }

    TEST_F(BTree_Test, InsertSame) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "InsertSame", 0);

        // set the XID
        btree->set_xid(1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        csv::CSVReader reader("test_btree_simple.csv");

        // insert 5000 of the same row
        auto csv_tuple = std::make_shared<FieldTuple>(csv_fields, *(reader.begin()));
        auto const_tuple = std::make_shared<ValueTuple>(csv_tuple);
        for (int i = 0; i < 5000; ++i) {
            // insert data to the tree
            btree->insert(const_tuple);
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        auto tree = _get_btree(_base_dir / "InsertSame", offset);

        // check for all entries
        int count = 0;

        std::string prev = "";
        for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
            if (prev != "") {
                ASSERT_EQ(_name_f->get_text(*i), prev);
            }

            prev = _name_f->get_text(*i);
            ++count;
        }

        ASSERT_EQ(count, 5000);
    }

    TEST_F(BTree_Test, InsertMany) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "InsertMany", 0);

        // set the XID
        btree->set_xid(1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        // insert 50k entries
        for (int i = 0; i < 10; i++) {
            csv::CSVReader reader("test_btree_simple.csv");
            for (auto &&r : reader) {
                // insert data to the tree
                btree->insert(std::make_shared<FieldTuple>(csv_fields, r));
            }
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        auto tree = _get_btree(_base_dir / "InsertMany", offset);

        // check for all entries
        int count = 0;

        std::string prev = "";
        for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
            if (prev != "") {
                ASSERT_GE(_name_f->get_text(*i), prev);
            }

            prev = _name_f->get_text(*i);
            ++count;
        }

        ASSERT_EQ(count, 50000);
    }

    TEST_F(BTree_Test, ThreadedInserts) {
        PhasedThreadTest<Request> tester;

        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "ThreadedInserts", 0);

        // set the XID
        btree->set_xid(1);

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
                auto tuple = std::make_shared<FieldTuple>(csv_fields, r);
                auto value_tuple = std::make_shared<ValueTuple>(tuple);
                auto request = std::make_shared<Request>(btree, Request::Type::INSERT, value_tuple);

                tester.add_request(request);
            }
        }

        // verify the entries
        tester.set_verify([this, btree]() {
            uint64_t offset = btree->finalize();

            auto tree = _get_btree(_base_dir / "ThreadedInserts", offset);

            // check for all entries
            int count = 0;
            std::string prev = "";
            std::map<std::string, int> counts;
            for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
                if (_name_f->get_text(*i) < prev) {
                    SPDLOG_ERROR("{} < {}", _name_f->get_text(*i), prev);
                }

                if (prev != "") {
                    ASSERT_GE(_name_f->get_text(*i), prev);
                }

                prev = _name_f->get_text(*i);
                ++counts[prev];
                ++count;
            }

            for (auto &&entry : counts) {
                if (entry.second < 10) {
                    SPDLOG_INFO("{} = {}", entry.first, entry.second);
                }
            }
            ASSERT_EQ(count, 50000);
        });

        // run the phases using 4 threads (just one phase here)
        tester.run(4);
    }

    TEST_F(BTree_Test, ThreadedInsertAndRemove) {
        PhasedThreadTest<Request> tester;

        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree(_base_dir / "ThreadedInsertAndRemove", 0);

        // set the XID
        btree->set_xid(1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        // preapare 5k inserts
        csv::CSVReader reader("test_btree_simple.csv");
        for (auto &&r : reader) {
            auto tuple = std::make_shared<FieldTuple>(csv_fields, r);
            auto value_tuple = std::make_shared<ValueTuple>(tuple);
            auto request = std::make_shared<Request>(btree, Request::Type::INSERT, value_tuple);
                
            tester.add_request(request);
        }

        // verify the entries
        tester.set_verify([this, btree]() {
            uint64_t offset = btree->finalize();

            auto tree = _get_btree(_base_dir / "ThreadedInsertAndRemove", offset);

            // check for all entries
            _verify_unique_names(tree, 1, 5000);

            // set the next XID
            btree->set_xid(2);
        });

        // move to phase 2
        tester.next_phase();

        // preapare 5k removes
        csv::CSVReader reader2("test_btree_simple.csv");
        for (auto &&r : reader2) {
            auto tuple = std::make_shared<FieldTuple>(csv_fields, r);
            auto value_tuple = std::make_shared<ValueTuple>(tuple);
            auto request = std::make_shared<Request>(btree, Request::Type::REMOVE, value_tuple);
                
            tester.add_request(request);
        }

        // verify the entries
        tester.set_verify([this, btree]() {
            uint64_t offset = btree->finalize();
            std::cout << offset << std::endl;

            auto tree = _get_btree(_base_dir / "ThreadedInsertAndRemove", offset);

            // check for all entries
            _verify_names(tree, 2, 0);
        });

        // run the phases using 4 threads (just one phase here)
        tester.run(4);
    }

    TEST_F(BTree_Test, ThreadedInsertsTwoFiles) {
        PhasedThreadTest<Request> tester;

        // get mutable btrees to perform inserts
        auto btree1 = _create_mutable_btree(_base_dir / "ThreadedInsertsOne", 0);
        auto btree2 = _create_mutable_btree(_base_dir / "ThreadedInsertsTwo", 0);

        // set the XID
        btree1->set_xid(1);
        btree2->set_xid(1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();

        FieldArrayPtr csv_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            auto &&field = fields->at(i);
            csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
        }

        // preapare 100k inserts across two trees
        for (int i = 0; i < 10; i++) {
            csv::CSVReader reader("test_btree_simple.csv");
            for (auto &&r : reader) {
                auto tuple = std::make_shared<FieldTuple>(csv_fields, r);
                auto value_tuple = std::make_shared<ValueTuple>(tuple);

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
            auto tree = _get_btree(_base_dir / "ThreadedInsertsOne", offset1);

            // check for all entries
            _verify_unique_names(tree, 1, 50000);

            // check the second file
            tree = _get_btree(_base_dir / "ThreadedInsertsTwo", offset2);

            // check for all entries
            _verify_unique_names(tree, 1, 50000);
        });

        // run the phases using 4 threads (just one phase here)
        tester.run(4);
    }
}
