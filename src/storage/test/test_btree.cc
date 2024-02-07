#include <gtest/gtest.h>

#include <common/common.hh>
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
        }

        void TearDown() override {
            // remove any files created during the run
            IOMgr::get_instance()->remove("/tmp/test_btree_Insert10");
            IOMgr::get_instance()->remove("/tmp/test_btree_InsertAll");
            IOMgr::get_instance()->remove("/tmp/test_btree_InsertAndRemove");
            IOMgr::get_instance()->remove("/tmp/test_btree_InsertAndRemoveAll");
            IOMgr::get_instance()->remove("/tmp/test_btree_InsertSame");
            IOMgr::get_instance()->remove("/tmp/test_btree_InsertMany");
        }

        ExtentSchemaPtr _schema;
        std::shared_ptr<MutableBTree> _write_tree;
        std::shared_ptr<BTree::ExtentCache> _read_cache;
        std::vector<std::string> _keys;

        std::shared_ptr<MutableBTree>
        _create_mutable_btree(const std::filesystem::path &name,
                              uint64_t xid)
        {
            auto iomgr = IOMgr::get_instance();

            // construct a mutable b-tree for inserting data
            std::shared_ptr<IOHandle> handle = iomgr->open(name, IOMgr::IO_MODE::APPEND, true);
            
            uint64_t cache_size = 1024*1024;

            return std::make_shared<MutableBTree>(handle, _keys, cache_size, _schema);

        }

        std::shared_ptr<MutableBTree>
        _get_mutable_btree(const std::filesystem::path &name,
                           uint64_t xid,
                           uint64_t extent_id)
        {
            auto iomgr = IOMgr::get_instance();

            // construct a mutable b-tree for inserting data
            std::shared_ptr<IOHandle> handle = iomgr->open(name, IOMgr::IO_MODE::APPEND, true);
            uint64_t cache_size = 1024*1024;

            return std::make_shared<MutableBTree>(handle, _keys, cache_size, _schema, extent_id);

        }

        std::shared_ptr<BTree>
        _get_btree(const std::filesystem::path &name,
                   uint64_t extent_id)
        {
            auto iomgr = IOMgr::get_instance();

            // construct a mutable b-tree for inserting data
            std::shared_ptr<IOHandle> handle = iomgr->open(name, IOMgr::IO_MODE::READ, true);
            return std::make_shared<BTree>(handle, 0, _keys, _schema,
                                           _read_cache, 1, extent_id);
        }
    };

    TEST_F(BTree_Test, Insert10) {
        // get a mutable btree to perform inserts
        auto btree = _create_mutable_btree("/tmp/test_btree_Insert10", 0);

        // set the XID
        btree->set_xid(1);

        // pull data to insert
        FieldArrayPtr fields = _schema->get_fields();
        csv::CSVReader reader("test_btree_simple.csv");

        auto r = reader.begin();
        for (int i = 0; i < 10; i++) {
            // insert data to the tree
            btree->insert(std::make_shared<CSVTuple>(*r, fields));
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
        csv::CSVReader reader("test_btree_simple.csv");

        for (auto &&r : reader) {
            // insert data to the tree
            btree->insert(std::make_shared<CSVTuple>(r, fields));
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
        csv::CSVReader reader("test_btree_simple.csv");

        for (auto &&r : reader) {
            // insert data to the tree
            btree->insert(std::make_shared<CSVTuple>(r, fields));
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

                btree->remove(std::make_shared<CSVTuple>(r, fields));
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
        csv::CSVReader reader("test_btree_simple.csv");

        for (auto &&r : reader) {
            // insert data to the tree
            btree->insert(std::make_shared<CSVTuple>(r, fields));
        }

        // finalize the tree
        uint64_t offset = btree->finalize();

        // set the next XID
        btree->set_xid(2);

        // now remove every row in the csv
        reader = csv::CSVReader("test_btree_simple.csv");
        for (auto &&r : reader) {
            // remove data from the tree
            btree->remove(std::make_shared<CSVTuple>(r, fields));
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
        csv::CSVReader reader("test_btree_simple.csv");

        // insert 5000 of the same row
        auto &&r = *reader.begin();
        for (int i = 0; i < 5000; ++i) {
            // insert data to the tree
            btree->insert(std::make_shared<CSVTuple>(r, fields));
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

        // insert 15k entries
        for (int i = 0; i < 10; i++) {
            csv::CSVReader reader("test_btree_simple.csv");
            for (auto &&r : reader) {
                // insert data to the tree
                btree->insert(std::make_shared<CSVTuple>(r, fields));
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
}
