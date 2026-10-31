#include <algorithm>

#include <gtest/gtest.h>

#include <common/init.hh>
#include <storage/csv_field.hh>
#include <storage/index_engine/btree_read_strategy.hh>
#include <storage/index_engine/btree_write_strategy.hh>
#include <storage/index_engine/index_extent_cache.hh>
#include <storage/index_engine/index_reader.hh>
#include <storage/index_engine/index_writer.hh>
#include <storage/index_engine/storage_cache_extent_source.hh>
#include "common/constants.hh"

using namespace springtail;

namespace {
    using BtreeIndexWriter = IndexWriter<BtreeWriteStrategy>;
    using BtreeIndexReader = IndexReader<BtreeReadStrategy>;

    /**
     * Mirrors test_index_writer.cc's IndexWriterTest fixture (same schema/keys/base_dir pattern),
     * but round-trips: trees are built with phase 1's IndexWriter<BtreeWriteStrategy> and read back
     * with this phase's IndexReader<BtreeReadStrategy>, reproducing every test_btree.cc *read*
     * scenario (Search, begin/end/empty-tree edge cases) against that writer, per phases/phase-2.md.
     */
    class IndexReaderTest : public testing::Test {
    protected:
        void SetUp() override {
            springtail_init_test(LOG_ALL ^ LOG_STORAGE);

            std::vector<SchemaColumn> columns({
                    { "table_id", 0, SchemaType::UINT64, 0, false, 1},
                    { "name", 1, SchemaType::TEXT, 0, false, 0 },
                    { "offset", 2, SchemaType::UINT64, 0, false },
                    { "index", 3, SchemaType::UINT16, 0, false, 2 }
                });
            _schema = std::make_shared<ExtentSchema>(columns, ExtensionCallback{}, false, false);
            _keys = std::vector<std::string>({"name", "table_id", "index"});

            _base_dir = std::filesystem::temp_directory_path() / "test_index_reader";
            std::filesystem::create_directories(_base_dir);

            _name_f = _schema->get_field("name");
            _table_id_f = _schema->get_field("table_id");
            _offset_f = _schema->get_field("offset");

            _fields = _schema->get_fields();
            _csv_fields = std::make_shared<FieldArray>();
            for (int i = 0; i < _fields->size() - 1; i++) {
                auto &&field = _fields->at(i);
                _csv_fields->push_back(std::make_shared<CSVField>(field->get_type(), i));
            }

            _cache = std::make_shared<IndexExtentCache>(64 * 1024);
        }

        void TearDown() override {
            std::filesystem::remove_all(_base_dir);
            springtail_shutdown();
        }

        ExtentSchemaPtr _schema;
        FieldPtr _table_id_f, _name_f, _offset_f;
        FieldArrayPtr _fields, _csv_fields;

        std::vector<std::string> _keys;
        std::filesystem::path _base_dir;
        std::shared_ptr<IndexExtentCache> _cache;

        std::shared_ptr<BtreeIndexWriter>
        _create_writer(const std::filesystem::path &name, uint64_t xid,
                       uint64_t max_extent_size = constant::MAX_EXTENT_SIZE)
        {
            auto writer = std::make_shared<BtreeIndexWriter>(
                0, name, _keys, _schema, xid, _cache,
                BtreeWriteStrategy(max_extent_size));
            writer->init_empty();
            return writer;
        }

        std::shared_ptr<BtreeIndexWriter>
        _open_writer(const std::filesystem::path &name, uint64_t xid, uint64_t root_offset,
                     uint64_t max_extent_size = constant::MAX_EXTENT_SIZE)
        {
            auto writer = std::make_shared<BtreeIndexWriter>(
                0, name, _keys, _schema, xid, _cache,
                BtreeWriteStrategy(max_extent_size));
            writer->init(root_offset);
            return writer;
        }

        std::shared_ptr<BtreeIndexReader>
        _open_reader(const std::filesystem::path &name, uint64_t xid, uint64_t root_offset)
        {
            return std::make_shared<BtreeIndexReader>(
                0, name, _keys, _schema, xid, root_offset,
                std::make_shared<StorageCacheExtentSource>(), BtreeReadStrategy());
        }

        TuplePtr
        _gen_tuple(csv::CSVRow &row, uint16_t idx) {
            auto extra = std::make_shared<FieldArray>();
            extra->push_back(std::make_shared<ConstTypeField<uint16_t>>(idx));

            return std::make_shared<KeyValueTuple>(_csv_fields, extra, &row);
        }

        void
        _populate(std::shared_ptr<BtreeIndexWriter> writer, uint16_t idx) {
            csv::CSVReader reader("test_btree_simple.csv");
            for (auto &&r : reader) {
                writer->insert(_gen_tuple(r, idx));
            }
        }

        std::shared_ptr<Tuple>
        _create_key(const std::string &name, uint64_t table_id, uint16_t idx) {
            auto key_fields = std::make_shared<FieldArray>(3);
            key_fields->at(0) = std::make_shared<ConstTypeField<std::string>>(name);
            key_fields->at(1) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
            key_fields->at(2) = std::make_shared<ConstTypeField<uint16_t>>(idx);
            return std::make_shared<FieldTuple>(key_fields, nullptr);
        }

        /** Walks `reader` forward via begin()/operator++()/end(), asserting non-decreasing `name`
            order, and returns the number of rows seen. */
        int
        _count_and_verify_forward(std::shared_ptr<BtreeIndexReader> reader) {
            int count = 0;
            std::string prev;

            for (auto i = reader->begin(); i != reader->end(); ++i) {
                auto row = *i;
                if (!prev.empty()) {
                    EXPECT_GE(_name_f->get_text(&row), prev);
                }
                prev = std::string(_name_f->get_text(&row));
                ++count;
            }

            return count;
        }

        /** Walks `reader` backward from end() via operator--(), asserting non-increasing `name`
            order, down to (and including) begin(), and returns the number of rows seen. Returns 0
            without touching an iterator if the tree is empty (begin() == end()), matching the
            empty-tree edge case begin()/end() must agree on. */
        int
        _count_and_verify_backward(std::shared_ptr<BtreeIndexReader> reader) {
            if (reader->begin() == reader->end()) {
                return 0;
            }

            int count = 0;
            std::string prev;

            auto i = reader->end();
            do {
                --i;
                auto row = *i;
                if (!prev.empty()) {
                    EXPECT_LE(_name_f->get_text(&row), prev);
                }
                prev = std::string(_name_f->get_text(&row));
                ++count;
            } while (i != reader->begin());

            return count;
        }
    };

    TEST_F(IndexReaderTest, EmptyTree) {
        // Note: a writer that's init_empty()'d and finalize()'d with *zero* inserts is not a
        // scenario this test exercises -- production code (Table::_create_index_root and
        // friends, confirmed by reading table.cc/mutable_table.cc) never does that; a brand-new,
        // never-populated tree is represented purely by root_offset == UNKNOWN_EXTENT without
        // ever constructing a Writer/Reader at all (see the UnknownRoot test below), and
        // IndexWriter::finalize() on such a never-dirtied root was found, while writing this
        // test, to return its unflushed temp id rather than a real on-disk extent_id (its root
        // Node is never marked dirty by init_empty() itself) -- a phase 1 gap recorded in
        // phases/phase-2.md rather than fixed here (out of this phase's scope). This test instead
        // covers the scenario that *is* real and already supported: a tree that had rows and had
        // all of them removed, which flushes a real, empty leaf extent (mirrors
        // IndexWriterTest.InsertAndRemoveAll).
        auto writer = _create_writer(_base_dir / "EmptyTree", 1);
        _populate(writer, 0);
        uint64_t offset_1 = writer->finalize();

        auto writer2 = _open_writer(_base_dir / "EmptyTree", 2, offset_1);

        auto csv_reader = csv::CSVReader("test_btree_simple.csv");
        for (auto &r : csv_reader) {
            writer2->remove(_gen_tuple(r, 0));
        }

        uint64_t offset = writer2->finalize();
        ASSERT_TRUE(writer2->empty());

        auto reader = _open_reader(_base_dir / "EmptyTree", 3, offset);

        ASSERT_TRUE(reader->empty());
        ASSERT_TRUE(reader->begin() == reader->end());
        ASSERT_EQ(_count_and_verify_forward(reader), 0);
        ASSERT_EQ(_count_and_verify_backward(reader), 0);

        // find()/lower_bound() on an empty tree must behave like end(), not crash
        auto key = _create_key("anything", 0, 0);
        ASSERT_TRUE(reader->find(key) == reader->end());
        ASSERT_TRUE(reader->lower_bound(key) == reader->end());
        ASSERT_TRUE(reader->lower_bound(key, true) == reader->end());
    }

    TEST_F(IndexReaderTest, UnknownRoot) {
        // a tree that was never even init_empty()'d (root_offset == UNKNOWN_EXTENT) must behave
        // identically to an explicitly-flushed empty tree -- this is the BTree::empty()/begin()
        // UNKNOWN_EXTENT short-circuit, not something only reachable via finalize().
        auto reader = _open_reader(_base_dir / "UnknownRoot", 1, constant::UNKNOWN_EXTENT);

        ASSERT_TRUE(reader->empty());
        ASSERT_TRUE(reader->begin() == reader->end());
    }

    TEST_F(IndexReaderTest, DecrementBegin) {
        auto writer = _create_writer(_base_dir / "DecrementBegin", 1);
        _populate(writer, 0);
        uint64_t offset = writer->finalize();

        auto reader = _open_reader(_base_dir / "DecrementBegin", 1, offset);

        // decrementing begin() walks every ancestor back to its own begin(), finds no earlier
        // sibling anywhere, and lands on end() -- mirroring BTree::Iterator::operator--()'s
        // documented behavior (not an assertion failure).
        auto i = reader->begin();
        --i;
        ASSERT_TRUE(i == reader->end());
    }

    TEST_F(IndexReaderTest, SingleLeafRoot) {
        // a tree small enough that the root is itself the only (leaf) extent -- the shallow case
        // that under-tests iterator boundary conditions if it's the only case covered.
        auto writer = _create_writer(_base_dir / "SingleLeafRoot", 1);

        auto r = *csv::CSVReader("test_btree_simple.csv").begin();
        writer->insert(_gen_tuple(r, 0));

        uint64_t offset = writer->finalize();
        auto reader = _open_reader(_base_dir / "SingleLeafRoot", 1, offset);

        ASSERT_FALSE(reader->empty());
        ASSERT_EQ(_count_and_verify_forward(reader), 1);
        ASSERT_EQ(_count_and_verify_backward(reader), 1);

        auto i = reader->begin();
        ASSERT_TRUE(++i == reader->end());

        auto j = reader->end();
        ASSERT_TRUE(--j == reader->begin());
    }

    TEST_F(IndexReaderTest, InsertAllRoundTrip) {
        auto writer = _create_writer(_base_dir / "InsertAllRoundTrip", 1);
        _populate(writer, 0);

        uint64_t offset = writer->finalize();
        auto reader = _open_reader(_base_dir / "InsertAllRoundTrip", 1, offset);

        ASSERT_EQ(_count_and_verify_forward(reader), 5000);
        ASSERT_EQ(_count_and_verify_backward(reader), 5000);
    }

    TEST_F(IndexReaderTest, InsertAndRemove) {
        auto writer = _create_writer(_base_dir / "InsertAndRemove", 1);
        _populate(writer, 0);

        uint64_t offset_1 = writer->finalize();

        // XID 1 has every entry
        auto reader_1 = _open_reader(_base_dir / "InsertAndRemove", 1, offset_1);
        ASSERT_EQ(_count_and_verify_forward(reader_1), 5000);
        ASSERT_EQ(_count_and_verify_backward(reader_1), 5000);

        auto writer2 = _open_writer(_base_dir / "InsertAndRemove", 2, offset_1);

        int evens = 0;
        auto csv_reader = csv::CSVReader("test_btree_simple.csv");
        for (auto &&r : csv_reader) {
            if (evens % 2) {
                writer2->remove(_gen_tuple(r, 0));
            }
            ++evens;
        }

        uint64_t offset_2 = writer2->finalize();

        // XID 1's reader is unaffected by the second generation's in-memory mutation -- CoW means
        // offset_1's extents are untouched on disk regardless of what writer2 did in memory.
        auto reader_1_again = _open_reader(_base_dir / "InsertAndRemove", 1, offset_1);
        ASSERT_EQ(_count_and_verify_forward(reader_1_again), 5000);

        auto reader_2 = _open_reader(_base_dir / "InsertAndRemove", 2, offset_2);
        ASSERT_EQ(_count_and_verify_forward(reader_2), 2500);
        ASSERT_EQ(_count_and_verify_backward(reader_2), 2500);
    }

    TEST_F(IndexReaderTest, Search) {
        // reproduces test_btree.cc's BTree_Test.Search scenario exactly (src/storage/test/
        // test_btree.cc:269-387), against a tree built by IndexWriter and read by IndexReader.
        auto writer = _create_writer(_base_dir / "Search", 1);
        _populate(writer, 0);

        uint64_t offset = writer->finalize();
        auto tree = _open_reader(_base_dir / "Search", 1, offset);

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
            auto &&row = *find_i;

            ASSERT_EQ(_name_f->get_text(&row), "aabbatini8y");
            ASSERT_EQ(_table_id_f->get_uint64(&row), 323);
            ASSERT_EQ(_offset_f->get_uint64(&row), 6448);

            auto lb_i = tree->lower_bound(tuple);
            auto ffu_i = tree->lower_bound(tuple, true);
            auto iub_i = tree->inverse_upper_bound(tuple);
            auto ilb_i = tree->inverse_lower_bound(tuple);

            ASSERT_EQ(lb_i, find_i);
            ASSERT_EQ(ffu_i, find_i);
            ASSERT_EQ(iub_i, end_i);
            ASSERT_EQ(ilb_i, begin_i);
        }

        // search for an existing entry in the middle
        {
            auto tuple = _create_key("mplainu", 31, 0);

            auto find_i = tree->find(tuple);
            auto &&row = *find_i;

            ASSERT_EQ(_table_id_f->get_uint64(&row), 31);
            ASSERT_EQ(_name_f->get_text(&row), "mplainu");
            ASSERT_EQ(_offset_f->get_uint64(&row), 30122);

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

            auto &&lb_row = *lb_i;
            ASSERT_EQ(_table_id_f->get_uint64(&lb_row), 526);
            ASSERT_EQ(_name_f->get_text(&lb_row), "mabrahimel");
            ASSERT_EQ(_offset_f->get_uint64(&lb_row), 33466);

            ASSERT_EQ(ffu_i, lb_i);

            auto &&iub_row = *iub_i;
            ASSERT_EQ(_table_id_f->get_uint64(&iub_row), 997);
            ASSERT_EQ(_name_f->get_text(&iub_row), "lzipsellro");
            ASSERT_EQ(_offset_f->get_uint64(&iub_row), 86407);
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

            auto &&row = *ffu_i;
            ASSERT_EQ(_table_id_f->get_uint64(&row), 430);
            ASSERT_EQ(_name_f->get_text(&row), "zwoolertonbx");
            ASSERT_EQ(_offset_f->get_uint64(&row), 92729);

            ASSERT_EQ(iub_i, ffu_i);
        }
    }

    // Regression test for the "deeper/chainier tree" bug class phases/phase-1.md's round-1 review
    // found twice (a forgotten piece of traversal state at a boundary condition that only shows up
    // past a shallow 2-leaf tree): reuses phase 1's ReopenThenSplitAgain technique (a 1-byte split
    // threshold, several reopened generations) to force real branch-level splits, then walks the
    // final tree fully forward *and* fully backward, and cross-checks the two walks produce the
    // exact reverse of each other -- not just matching counts, but matching row-for-row order,
    // which would catch operator--() silently landing on the wrong row even if its count happened
    // to still match operator++()'s.
    TEST_F(IndexReaderTest, DeepTreeForwardAndBackward) {
        auto writer = _create_writer(_base_dir / "DeepTreeForwardAndBackward", 1, 1);

        auto row_value = [](const std::string &name, uint64_t table_id, uint16_t idx) {
            auto key_fields = std::make_shared<FieldArray>(4);
            key_fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(table_id);
            key_fields->at(1) = std::make_shared<ConstTypeField<std::string>>(name);
            key_fields->at(2) = std::make_shared<ConstTypeField<uint64_t>>(uint64_t(0));
            key_fields->at(3) = std::make_shared<ConstTypeField<uint16_t>>(idx);
            return std::make_shared<FieldTuple>(key_fields, nullptr);
        };

        uint64_t offset = 0;
        std::vector<std::string> names = { "a", "f", "m", "r", "z" };
        int gen_count = 8;
        for (int gen = 0; gen < gen_count; gen++) {
            auto gen_writer = (gen == 0)
                ? writer
                : _open_writer(_base_dir / "DeepTreeForwardAndBackward", gen + 1, offset, 1);

            for (auto &name : names) {
                gen_writer->insert(row_value(name + std::to_string(gen), 1, 0));
            }

            offset = gen_writer->finalize();
        }

        auto reader = _open_reader(_base_dir / "DeepTreeForwardAndBackward", 1000, offset);

        std::vector<std::string> forward;
        for (auto i = reader->begin(); i != reader->end(); ++i) {
            auto row = *i;
            forward.push_back(std::string(_name_f->get_text(&row)));
        }
        ASSERT_EQ(forward.size(), names.size() * gen_count);
        ASSERT_TRUE(std::is_sorted(forward.begin(), forward.end()));

        std::vector<std::string> backward;
        auto i = reader->end();
        do {
            --i;
            auto row = *i;
            backward.push_back(std::string(_name_f->get_text(&row)));
        } while (i != reader->begin());

        std::reverse(backward.begin(), backward.end());
        ASSERT_EQ(forward, backward);
    }
}
