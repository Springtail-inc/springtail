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
            _read_cache = std::make_shared<LruObjectCache<std::pair<uint64_t, uint64_t>, Extent>>(1024*1024);
            _keys = std::vector<std::string>({"name", "table_id"});

            _write_cache = MutableBTree::create_cache(2*1024*1024);
            _file_id = 1;
        }

        void TearDown() override {
            // remove any files created during the run
            IOMgr::get_instance()->remove("/tmp/test_btree_Insert10");
            IOMgr::get_instance()->remove("/tmp/test_btree_InsertAll");
            IOMgr::get_instance()->remove("/tmp/test_btree_InsertAndRemove");
            IOMgr::get_instance()->remove("/tmp/test_btree_InsertAndRemoveAll");
            IOMgr::get_instance()->remove("/tmp/test_btree_InsertSame");
            IOMgr::get_instance()->remove("/tmp/test_btree_InsertMany");
            IOMgr::get_instance()->remove("/tmp/test_btree_ThreadedInserts");
            IOMgr::get_instance()->remove("/tmp/test_btree_ThreadedInsertAndRemove");
            IOMgr::get_instance()->remove("/tmp/test_btree_ThreadedInsertsOne");
            IOMgr::get_instance()->remove("/tmp/test_btree_ThreadedInsertsTwo");
        }

        ExtentSchemaPtr _schema;
        std::shared_ptr<MutableBTree> _write_tree;
        std::shared_ptr<ExtentCache> _read_cache;
        MutableBTree::PageCachePtr _write_cache;
        std::vector<std::string> _keys;
        uint64_t _file_id;
        std::map<std::filesystem::path, uint64_t> _file_id_map;

        std::shared_ptr<MutableBTree>
        _create_mutable_btree(const std::filesystem::path &name,
                              uint64_t xid)
        {
            auto iomgr = IOMgr::get_instance();

            // construct a mutable b-tree for inserting data
            std::shared_ptr<IOHandle> handle = iomgr->open(name, IOMgr::IO_MODE::APPEND, true);
            
            auto btree = std::make_shared<MutableBTree>(handle, _file_id, _keys, _write_cache, _schema);
            _file_id_map[name] = _file_id;
            ++_file_id;

            btree->init_empty();
            return btree;
        }

        std::shared_ptr<MutableBTree>
        _get_mutable_btree(const std::filesystem::path &name,
                           uint64_t xid,
                           uint64_t extent_id)
        {
            auto iomgr = IOMgr::get_instance();

            // construct a mutable b-tree for inserting data
            std::shared_ptr<IOHandle> handle = iomgr->open(name, IOMgr::IO_MODE::APPEND, true);

            auto btree = std::make_shared<MutableBTree>(handle, _file_id_map[name], _keys, _write_cache, _schema);
            btree->init(extent_id);
            return btree;
        }

        std::shared_ptr<BTree>
        _get_btree(const std::filesystem::path &name,
                   uint64_t extent_id)
        {
            auto iomgr = IOMgr::get_instance();

            // construct a mutable b-tree for inserting data
            std::shared_ptr<IOHandle> handle = iomgr->open(name, IOMgr::IO_MODE::READ, true);
            return std::make_shared<BTree>(handle, _file_id_map[name], _keys, _schema,
                                           _read_cache, 1, extent_id);
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
        auto btree = _create_mutable_btree("/tmp/test_btree_Insert10", 0);

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
        auto table_id_f = _schema->get_field("table_id");
        auto name_f = _schema->get_field("name");
        auto offset_f = _schema->get_field("offset");

        auto tree = _get_btree("/tmp/test_btree_Insert10", offset);
        int count = 0;
        std::string prev = "";
        for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
            if (prev != "") {
                ASSERT_GT(name_f->get_text(*i), prev);
            }

            prev = name_f->get_text(*i);
            ++count;
        }
        ASSERT_EQ(count, 10);
    }

    TEST_F(BTree_Test, InsertAll) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree("/tmp/test_btree_InsertAll", 0);

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
        auto table_id_f = _schema->get_field("table_id");
        auto name_f = _schema->get_field("name");
        auto offset_f = _schema->get_field("offset");

        auto tree = _get_btree("/tmp/test_btree_InsertAll", offset);
        int count = 0;
        std::string prev = "";
        for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
            if (prev != "") {
                ASSERT_GT(name_f->get_text(*i), prev);
            }

            prev = name_f->get_text(*i);
            ++count;
        }
        ASSERT_EQ(count, 5000);
    }

    TEST_F(BTree_Test, InsertAndRemove) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree("/tmp/test_btree_InsertAndRemove", 0);

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
        auto table_id_f = _schema->get_field("table_id");
        auto name_f = _schema->get_field("name");
        auto offset_f = _schema->get_field("offset");

        auto tree = _get_btree("/tmp/test_btree_InsertAndRemove", offset);

        // check XID 1 for all entries
        int count = 0;
        std::string prev = "";
        for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
            if (prev != "") {
                ASSERT_GT(name_f->get_text(*i), prev);
            }

            prev = name_f->get_text(*i);
            ++count;
        }
        ASSERT_EQ(count, 5000);

        // check XID 2 for half the entries
        count = 0;
        prev = "";
        for (auto &&i = tree->begin(2); i != tree->end(); ++i) {
            if (prev != "") {
                ASSERT_GT(name_f->get_text(*i), prev);
            }

            prev = name_f->get_text(*i);
            ++count;
        }
        ASSERT_EQ(count, 2500);
    }

    TEST_F(BTree_Test, InsertAndRemoveAll) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree("/tmp/test_btree_InsertAndRemoveAll", 0);

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
        auto table_id_f = _schema->get_field("table_id");
        auto name_f = _schema->get_field("name");
        auto offset_f = _schema->get_field("offset");

        auto tree = _get_btree("/tmp/test_btree_InsertAndRemoveAll", offset);

        // check XID 1 for all entries
        int count = 0;
        std::string prev = "";
        for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
            if (prev != "") {
                ASSERT_GT(name_f->get_text(*i), prev);
            }

            prev = name_f->get_text(*i);
            ++count;
        }
        ASSERT_EQ(count, 5000);

        // check XID 2 for no entries
        count = 0;
        for (auto &&i = tree->begin(2); i != tree->end(); ++i) {
            ++count;
        }
        ASSERT_EQ(count, 0);
    }

    TEST_F(BTree_Test, InsertSame) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree("/tmp/test_btree_InsertSame", 0);

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
        auto &&r = *reader.begin();
        for (int i = 0; i < 5000; ++i) {
            // insert data to the tree
            btree->insert(std::make_shared<FieldTuple>(csv_fields, r));
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        auto tree = _get_btree("/tmp/test_btree_InsertSame", offset);

        auto table_id_f = _schema->get_field("table_id");
        auto name_f = _schema->get_field("name");
        auto offset_f = _schema->get_field("offset");

        // check for all entries
        int count = 0;
        std::string prev = "";
        for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
            if (prev != "") {
                ASSERT_EQ(name_f->get_text(*i), prev);
            }

            prev = name_f->get_text(*i);
            ++count;
        }
        ASSERT_EQ(count, 5000);
    }

    TEST_F(BTree_Test, InsertMany) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree("/tmp/test_btree_InsertMany", 0);

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

        auto tree = _get_btree("/tmp/test_btree_InsertMany", offset);

        auto table_id_f = _schema->get_field("table_id");
        auto name_f = _schema->get_field("name");
        auto offset_f = _schema->get_field("offset");

        // check for all entries
        int count = 0;
        std::string prev = "";
        for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
            if (prev != "") {
                ASSERT_GE(name_f->get_text(*i), prev);
            }

            prev = name_f->get_text(*i);
            ++count;
        }
        ASSERT_EQ(count, 50000);
    }

    TEST_F(BTree_Test, ThreadedInserts) {
        PhasedThreadTest<Request> tester;

        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree("/tmp/test_btree_ThreadedInserts", 0);

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

            auto tree = _get_btree("/tmp/test_btree_ThreadedInserts", offset);

            auto table_id_f = _schema->get_field("table_id");
            auto name_f = _schema->get_field("name");
            auto offset_f = _schema->get_field("offset");

            // check for all entries
            int count = 0;
            std::string prev = "";
            std::map<std::string, int> counts;
            for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
                if (name_f->get_text(*i) < prev) {
                    SPDLOG_ERROR("{} < {}", name_f->get_text(*i), prev);
                }

                if (prev != "") {
                    ASSERT_GE(name_f->get_text(*i), prev);
                }

                prev = name_f->get_text(*i);
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
        auto btree = _create_mutable_btree("/tmp/test_btree_ThreadedInsertAndRemove", 0);

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

            auto tree = _get_btree("/tmp/test_btree_ThreadedInsertAndRemove", offset);

            auto table_id_f = _schema->get_field("table_id");
            auto name_f = _schema->get_field("name");
            auto offset_f = _schema->get_field("offset");

            // check for all entries
            int count = 0;
            std::string prev = "";
            std::map<std::string, int> counts;
            for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
                if (name_f->get_text(*i) < prev) {
                    SPDLOG_ERROR("{} < {}", name_f->get_text(*i), prev);
                }

                if (prev != "") {
                    ASSERT_GE(name_f->get_text(*i), prev);
                }

                prev = name_f->get_text(*i);
                ++counts[prev];
                ++count;
            }

            for (auto &&entry : counts) {
                if (entry.second > 1) {
                    SPDLOG_INFO("{} = {}", entry.first, entry.second);
                }
            }
            ASSERT_EQ(count, 5000);

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

            auto tree = _get_btree("/tmp/test_btree_ThreadedInsertAndRemove", offset);

            auto table_id_f = _schema->get_field("table_id");
            auto name_f = _schema->get_field("name");
            auto offset_f = _schema->get_field("offset");

            // check for all entries
            int count = 0;
            for (auto &&i = tree->begin(2); i != tree->end(); ++i) {
                ++count;
            }
            ASSERT_EQ(count, 0);
        });

        // run the phases using 4 threads (just one phase here)
        tester.run(4);
    }

    TEST_F(BTree_Test, ThreadedInsertsTwoFiles) {
        PhasedThreadTest<Request> tester;

        // get mutable btrees to perform inserts
        auto btree1 = _create_mutable_btree("/tmp/test_btree_ThreadedInsertsOne", 0);
        auto btree2 = _create_mutable_btree("/tmp/test_btree_ThreadedInsertsTwo", 0);

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
            auto tree = _get_btree("/tmp/test_btree_ThreadedInsertsOne", offset1);

            auto table_id_f = _schema->get_field("table_id");
            auto name_f = _schema->get_field("name");
            auto offset_f = _schema->get_field("offset");

            // check for all entries
            int count = 0;
            std::string prev = "";
            std::map<std::string, int> counts;
            for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
                if (name_f->get_text(*i) < prev) {
                    SPDLOG_ERROR("{} < {}", name_f->get_text(*i), prev);
                }

                if (prev != "") {
                    ASSERT_GE(name_f->get_text(*i), prev);
                }

                prev = name_f->get_text(*i);
                ++counts[prev];
                ++count;
            }

            for (auto &&entry : counts) {
                if (entry.second < 10) {
                    SPDLOG_INFO("{} = {}", entry.first, entry.second);
                }
            }
            ASSERT_EQ(count, 50000);

            // check the second file
            tree = _get_btree("/tmp/test_btree_ThreadedInsertsTwo", offset2);

            // check for all entries
            count = 0;
            prev = "";
            counts.clear();
            for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
                if (name_f->get_text(*i) < prev) {
                    SPDLOG_ERROR("{} < {}", name_f->get_text(*i), prev);
                }

                if (prev != "") {
                    ASSERT_GE(name_f->get_text(*i), prev);
                }

                prev = name_f->get_text(*i);
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
}
