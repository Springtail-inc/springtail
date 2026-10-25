#include <thread>

#include <gtest/gtest.h>

#include <common/init.hh>
#include <storage/csv_field.hh>
#include <storage/index_engine/btree_write_strategy.hh>
#include <storage/index_engine/index_extent_cache.hh>
#include <storage/index_engine/index_writer.hh>
#include "common/constants.hh"

using namespace springtail;

namespace {
    using BtreeIndexWriter = IndexWriter<BtreeWriteStrategy>;

    /**
     * Mirrors test_btree.cc's BTree_Test fixture, but exercises IndexWriter<BtreeWriteStrategy>
     * directly instead of MutableBTree. No IndexReader exists yet (that's phase 2), so
     * durability/round-trip is verified by opening a *second* IndexWriter against the same file
     * via init(root_offset) and reading it back through its own iterators, rather than via a
     * separate reader class -- exactly the pattern test_btree.cc itself uses to check a second
     * XID's content (_get_mutable_btree()).
     */
    class IndexWriterTest : public testing::Test {
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

            _base_dir = std::filesystem::temp_directory_path() / "test_index_writer";
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
        _create_writer(const std::filesystem::path &name, uint64_t xid)
        {
            auto writer = std::make_shared<BtreeIndexWriter>(
                0, name, _keys, _schema, xid, _cache,
                BtreeWriteStrategy(constant::MAX_EXTENT_SIZE));
            writer->init_empty();
            return writer;
        }

        std::shared_ptr<BtreeIndexWriter>
        _open_writer(const std::filesystem::path &name, uint64_t xid, uint64_t root_offset)
        {
            auto writer = std::make_shared<BtreeIndexWriter>(
                0, name, _keys, _schema, xid, _cache,
                BtreeWriteStrategy(constant::MAX_EXTENT_SIZE));
            writer->init(root_offset);
            return writer;
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

        /** Walk `writer`'s own iterators, asserting rows are in non-decreasing `name` order, and
            return the number of rows seen. */
        int
        _count_and_verify_order(std::shared_ptr<BtreeIndexWriter> writer) {
            int count = 0;
            std::string prev;

            for (auto i = writer->begin(); i != writer->end(); ++i) {
                auto row = *i;
                if (!prev.empty()) {
                    EXPECT_GE(_name_f->get_text(&row), prev);
                }
                prev = std::string(_name_f->get_text(&row));
                ++count;
            }

            return count;
        }
    };

    TEST_F(IndexWriterTest, Insert10) {
        auto writer = _create_writer(_base_dir / "Insert10", 1);

        csv::CSVReader reader("test_btree_simple.csv");
        auto r = reader.begin();
        for (int i = 0; i < 10; i++) {
            writer->insert(_gen_tuple(*r, 0));
            ++r;
        }

        uint64_t offset = writer->finalize();
        ASSERT_NE(offset, constant::UNKNOWN_EXTENT);

        auto reopened = _open_writer(_base_dir / "Insert10", 2, offset);
        ASSERT_EQ(_count_and_verify_order(reopened), 10);
    }

    TEST_F(IndexWriterTest, InsertAll) {
        auto writer = _create_writer(_base_dir / "InsertAll", 1);
        _populate(writer, 0);

        uint64_t offset = writer->finalize();

        auto reopened = _open_writer(_base_dir / "InsertAll", 2, offset);
        ASSERT_EQ(_count_and_verify_order(reopened), 5000);
    }

    TEST_F(IndexWriterTest, InsertAndRemove) {
        auto writer = _create_writer(_base_dir / "InsertAndRemove", 1);
        _populate(writer, 0);

        uint64_t offset_1 = writer->finalize();
        ASSERT_EQ(_count_and_verify_order(writer), 5000);

        // a fresh reader-like writer at the same offset should see the same (durable) content
        auto reread = _open_writer(_base_dir / "InsertAndRemove", 1, offset_1);
        ASSERT_EQ(_count_and_verify_order(reread), 5000);

        auto writer2 = _open_writer(_base_dir / "InsertAndRemove", 2, offset_1);

        int evens = 0;
        auto reader = csv::CSVReader("test_btree_simple.csv");
        for (auto &&r : reader) {
            if (evens % 2) {
                writer2->remove(_gen_tuple(r, 0));
            }
            ++evens;
        }

        uint64_t offset_2 = writer2->finalize();

        // the first generation is untouched by the second writer's in-memory mutation
        ASSERT_EQ(_count_and_verify_order(writer), 5000);

        auto final_reader = _open_writer(_base_dir / "InsertAndRemove", 3, offset_2);
        ASSERT_EQ(_count_and_verify_order(final_reader), 2500);
    }

    TEST_F(IndexWriterTest, InsertAndRemoveAll) {
        auto writer = _create_writer(_base_dir / "InsertAndRemoveAll", 1);
        _populate(writer, 0);

        uint64_t offset_1 = writer->finalize();

        auto writer2 = _open_writer(_base_dir / "InsertAndRemoveAll", 2, offset_1);

        auto reader = csv::CSVReader("test_btree_simple.csv");
        for (auto &r : reader) {
            writer2->remove(_gen_tuple(r, 0));
        }

        uint64_t offset_2 = writer2->finalize();
        ASSERT_TRUE(writer2->empty());

        auto final_reader = _open_writer(_base_dir / "InsertAndRemoveAll", 3, offset_2);
        ASSERT_EQ(_count_and_verify_order(final_reader), 0);
        ASSERT_TRUE(final_reader->empty());
    }

    TEST_F(IndexWriterTest, InsertMany) {
        auto writer = _create_writer(_base_dir / "InsertMany", 1);

        for (int i = 0; i < 10; i++) {
            _populate(writer, i);
        }

        uint64_t offset = writer->finalize();

        auto reopened = _open_writer(_base_dir / "InsertMany", 2, offset);
        ASSERT_EQ(_count_and_verify_order(reopened), 50000);
    }

    TEST_F(IndexWriterTest, LowerBound) {
        auto writer = _create_writer(_base_dir / "LowerBound", 1);
        _populate(writer, 0);

        auto key_fields = std::make_shared<FieldArray>(3);
        key_fields->at(0) = std::make_shared<ConstTypeField<std::string>>(std::string("aabbatini8y"));
        key_fields->at(1) = std::make_shared<ConstTypeField<uint64_t>>(uint64_t(323));
        key_fields->at(2) = std::make_shared<ConstTypeField<uint16_t>>(uint16_t(0));
        auto key = std::make_shared<FieldTuple>(key_fields, nullptr);

        auto i = writer->lower_bound(key, false);
        ASSERT_TRUE(i != writer->end());

        auto row = *i;
        ASSERT_EQ(_name_f->get_text(&row), "aabbatini8y");
        ASSERT_EQ(_table_id_f->get_uint64(&row), 323);
        ASSERT_EQ(_offset_f->get_uint64(&row), 6448);

        // a key past every entry: lower_bound(for_update=false) is end(), for_update=true is last()
        auto key_fields2 = std::make_shared<FieldArray>(3);
        key_fields2->at(0) = std::make_shared<ConstTypeField<std::string>>(std::string("zzzzzzzzzz"));
        key_fields2->at(1) = std::make_shared<ConstTypeField<uint64_t>>(uint64_t(0));
        key_fields2->at(2) = std::make_shared<ConstTypeField<uint16_t>>(uint16_t(0));
        auto key2 = std::make_shared<FieldTuple>(key_fields2, nullptr);

        ASSERT_TRUE(writer->lower_bound(key2, false) == writer->end());
        ASSERT_TRUE(writer->lower_bound(key2, true) != writer->end());
    }

    TEST_F(IndexWriterTest, Truncate) {
        auto writer = _create_writer(_base_dir / "Truncate", 1);
        _populate(writer, 0);

        ASSERT_FALSE(writer->empty());
        writer->truncate();
        ASSERT_TRUE(writer->empty());
        ASSERT_EQ(_count_and_verify_order(writer), 0);

        // the tree must still be usable after truncate()
        writer->insert(_gen_tuple(*csv::CSVReader("test_btree_simple.csv").begin(), 0));
        ASSERT_EQ(_count_and_verify_order(writer), 1);
    }

    TEST_F(IndexWriterTest, TwoFilesConcurrent) {
        // Legitimate under architecture.md Sec 5.4's scoping note: single-writer is a per-tree
        // contract, not a process-wide one. Two different IndexWriter instances on two different
        // files/trees, driven from two different threads, sharing one IndexExtentCache, is
        // exactly the case that cache's own mutex exists for. (Same-tree concurrent access, which
        // test_btree.cc's ThreadedInserts/ThreadedInsertAndRemove exercised against
        // MutableBTree's internal lock-coupling, is intentionally not reproduced here -- see
        // phases/phase-1.md's record of this decision.)
        auto writer1 = _create_writer(_base_dir / "TwoFilesOne", 1);
        auto writer2 = _create_writer(_base_dir / "TwoFilesTwo", 1);

        auto populate_fn = [this](std::shared_ptr<BtreeIndexWriter> writer) {
            for (int i = 0; i < 10; i++) {
                _populate(writer, i);
            }
        };

        std::thread t1(populate_fn, writer1);
        std::thread t2(populate_fn, writer2);
        t1.join();
        t2.join();

        uint64_t offset1 = writer1->finalize();
        uint64_t offset2 = writer2->finalize();

        auto reopened1 = _open_writer(_base_dir / "TwoFilesOne", 2, offset1);
        auto reopened2 = _open_writer(_base_dir / "TwoFilesTwo", 2, offset2);

        ASSERT_EQ(_count_and_verify_order(reopened1), 50000);
        ASSERT_EQ(_count_and_verify_order(reopened2), 50000);
    }

    TEST_F(IndexWriterTest, ReentrancyGuardFires) {
#ifndef NDEBUG
        // The single-writer contract (architecture.md Sec 5.4) is external, not internally
        // locked -- IndexWriter has no mutex of its own. debug_guarded_call() runs its argument
        // under the exact same reentrancy guard every mutating method (insert/remove/truncate/
        // finalize/init/sync) uses, so nesting one call inside another here simulates the
        // violation the guard exists to catch without needing genuine, inherently racy
        // concurrency to reliably trigger it.
        auto writer = _create_writer(_base_dir / "Reentrancy", 1);

        EXPECT_DEATH({
            writer->debug_guarded_call([&]() {
                writer->debug_guarded_call([]() { });
            });
        }, "reentrant");
#else
        GTEST_SKIP() << "reentrancy guard is debug-build only";
#endif
    }
}
