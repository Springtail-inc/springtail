#include <gtest/gtest.h>

#include <common/common.hh>
#include <storage/csv_field.hh>
#include <storage/btree.hh>

using namespace springtail;

namespace {

    /**
     * Framework for Extent testing.
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
            IOMgr::get_instance()->remove("/tmp/test_btree_BasicInsertRemove");
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
            std::shared_ptr<IOHandle> handle = iomgr->open(name, IOMgr::IO_MODE::WRITE, true);
            
            uint64_t cache_size = 1024*1024;

            return std::make_shared<MutableBTree>(handle, _keys, cache_size, _schema, xid);

        }

        std::shared_ptr<MutableBTree>
        _get_mutable_btree(const std::filesystem::path &name,
                           uint64_t xid,
                           uint64_t extent_id)
        {
            auto iomgr = IOMgr::get_instance();

            // construct a mutable b-tree for inserting data
            std::shared_ptr<IOHandle> handle = iomgr->open(name, IOMgr::IO_MODE::WRITE, true);
            uint64_t cache_size = 1024*1024;

            return std::make_shared<MutableBTree>(handle, _keys, cache_size, _schema, xid, extent_id);

        }

        std::shared_ptr<BTree>
        _get_btree(const std::filesystem::path &name,
                   uint64_t extent_id)
        {
            auto iomgr = IOMgr::get_instance();

            // construct a mutable b-tree for inserting data
            std::shared_ptr<IOHandle> handle = iomgr->open(name, IOMgr::IO_MODE::WRITE, true);
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

        // now read the tree back
        auto table_id_f = _schema->get_field("table_id");
        auto name_f = _schema->get_field("name");
        auto offset_f = _schema->get_field("offset");

        auto tree = _get_btree("/tmp/test_btree_Insert10", offset);
        for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
            SPDLOG_INFO("entry: {}, {}, {}",
                        table_id_f->get_uint64(*i),
                        name_f->get_text(*i),
                        offset_f->get_uint64(*i));
        }
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

        // now read the tree back
        auto table_id_f = _schema->get_field("table_id");
        auto name_f = _schema->get_field("name");
        auto offset_f = _schema->get_field("offset");

        auto tree = _get_btree("/tmp/test_btree_InsertAll", offset);
        for (auto &&i = tree->begin(1); i != tree->end(); ++i) {
            SPDLOG_INFO("entry: {}, {}, {}",
                        table_id_f->get_uint64(*i),
                        name_f->get_text(*i),
                        offset_f->get_uint64(*i));
        }
    }

}
