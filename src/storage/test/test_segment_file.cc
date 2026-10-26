#include <algorithm>
#include <atomic>
#include <fstream>
#include <thread>

#include <gtest/gtest.h>

#include <fmt/core.h>

#include <common/environment.hh>
#include <common/init.hh>
#include <storage/index_engine/btree_write_strategy.hh>
#include <storage/index_engine/index_extent_cache.hh>
#include <storage/index_engine/index_writer.hh>
#include <storage/index_engine/segment_set.hh>
#include <storage/vacuumer.hh>
#include <test/services.hh>
#include <xid_mgr/xid_mgr_server.hh>
#include "common/constants.hh"

using namespace springtail;

namespace {
    using BtreeIndexWriter = IndexWriter<BtreeWriteStrategy>;

    std::string
    _read_bytes(const std::filesystem::path &path)
    {
        std::ifstream in(path, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }

    /**
     * Integration-level tests for phase 6's sealed, size-bounded segment files + whole-file
     * reclamation: seal-at-threshold behavior, multi-segment append/read round trip (via
     * IndexWriter -- a real IndexReader round trip over segments is covered separately in
     * test_index_reader.cc), and whole-segment unlink once the reachability cutoff passes it,
     * exercising the real Vacuumer rather than mocking it. Pure, IO-free unit tests of
     * SegmentSet's pack/unpack/naming helpers live in io_test.cc instead (phases/phase-6.md's
     * "Extend io_test.cc for the segment-set layer" task).
     *
     * Each test gets its own fresh db_id (see _next_db_id) so tests are independent of each
     * other's xid_mgr/Vacuumer state despite sharing one xid_mgr service instance (started once
     * for the whole binary) and one live Redis instance (started once for the whole container,
     * per AGENTS.md) -- no ordering assumptions between tests.
     */
    class SegmentFileTest : public testing::Test {
    protected:
        static void SetUpTestSuite() {
            _base_dir = std::filesystem::temp_directory_path() / "test_segment_file" / "table";
            std::filesystem::remove_all(_base_dir);
            std::filesystem::create_directories(_base_dir);

            // Properties::init() (triggered by springtail_init_test(), below) parses
            // SPRINGTAIL_PROPERTIES exactly once and caches the result -- a setenv() call any
            // later than this, e.g. from an individual test's SetUp(), has no effect. Every test
            // in this fixture wants a tiny seal threshold (to force segment rotation on nearly
            // every flush without needing thousands of rows), so set it once, here, for the
            // whole binary.
            _set_seal_threshold(1);

            springtail_init_test();
            test::start_services(/*xid_mgr=*/true, /*sys_tbl_mgr=*/false, /*write_cache=*/false);
        }

        static void TearDownTestSuite() {
            std::filesystem::remove_all(_base_dir);
            springtail_shutdown();
        }

        void SetUp() override {
            _db_id = _next_db_id++;

            // "<base>/table/<db_id>" matches Vacuumer::_get_db_id_from_path()'s "table/<id>"
            // lookup convention (see test_table.cc's identical comment on its own base_dir).
            _table_dir = _base_dir / std::to_string(_db_id);
            std::filesystem::create_directories(_table_dir);

            std::vector<SchemaColumn> columns({
                    { "key", 0, SchemaType::TEXT, 0, false, 0 },
                    { "value", 1, SchemaType::UINT64, 0, false }
                });
            _schema = std::make_shared<ExtentSchema>(columns, ExtensionCallback{}, false, false);
            _keys = { "key" };

            _cache = std::make_shared<IndexExtentCache>(64 * 1024);

            Vacuumer::get_instance()->enable_tracking_extents();
            // XidMgrServer starts a never-before-used db_id's "last committed xid" baseline at
            // 1 (PgXactLogWriter's recovered_xid default) -- the first real commit_xid() call
            // for this db_id must use an xid > 1, not 1 itself.
        }

        void TearDown() override {
            Vacuumer::get_instance()->cleanup_db(_db_id);
        }

        static void _set_seal_threshold(uint64_t bytes) {
            ::setenv(environment::ENV_OVERRIDE,
                    fmt::format("storage.segment_seal_threshold_bytes={}", bytes).c_str(), 1);
        }

        TuplePtr _row(const std::string &key, uint64_t value) {
            auto fields = std::make_shared<FieldArray>();
            fields->push_back(std::make_shared<ConstTypeField<std::string>>(key));
            fields->push_back(std::make_shared<ConstTypeField<uint64_t>>(value));
            return std::make_shared<FieldTuple>(fields, nullptr);
        }

        std::shared_ptr<BtreeIndexWriter>
        _create_writer(const std::filesystem::path &name, uint64_t xid)
        {
            auto writer = std::make_shared<BtreeIndexWriter>(
                _db_id, name, _keys, _schema, xid, _cache,
                BtreeWriteStrategy(constant::MAX_EXTENT_SIZE));
            writer->init_empty();
            return writer;
        }

        std::shared_ptr<BtreeIndexWriter>
        _open_writer(const std::filesystem::path &name, uint64_t xid, uint64_t root_offset)
        {
            auto writer = std::make_shared<BtreeIndexWriter>(
                _db_id, name, _keys, _schema, xid, _cache,
                BtreeWriteStrategy(constant::MAX_EXTENT_SIZE));
            writer->init(root_offset);
            return writer;
        }

        /** Collects every (key, value) pair reachable through `writer`'s own iterators. */
        std::vector<std::pair<std::string, uint64_t>>
        _read_all(std::shared_ptr<BtreeIndexWriter> writer)
        {
            auto key_f = _schema->get_field("key");
            auto value_f = _schema->get_field("value");

            std::vector<std::pair<std::string, uint64_t>> rows;
            for (auto i = writer->begin(); i != writer->end(); ++i) {
                auto row = *i;
                rows.emplace_back(std::string(key_f->get_text(&row)), value_f->get_uint64(&row));
            }
            return rows;
        }

        uint64_t _db_id = 0;
        std::filesystem::path _table_dir;

        ExtentSchemaPtr _schema;
        std::vector<std::string> _keys;
        std::shared_ptr<IndexExtentCache> _cache;

        static std::filesystem::path _base_dir;
        static std::atomic<uint64_t> _next_db_id;
    };

    std::filesystem::path SegmentFileTest::_base_dir;
    std::atomic<uint64_t> SegmentFileTest::_next_db_id{700001};

    TEST_F(SegmentFileTest, SealAtThresholdStartsNewSegment)
    {
        auto path = _table_dir / "0.idx";

        auto writer1 = _create_writer(path, 2);
        writer1->insert(_row("a", 1));
        uint64_t offset1 = writer1->finalize();
        ASSERT_NE(offset1, constant::UNKNOWN_EXTENT);
        ASSERT_EQ(SegmentSet::segment_id_of(offset1), 0u);

        auto segment0_path = SegmentSet::segment_path(path, 0);
        ASSERT_TRUE(std::filesystem::exists(segment0_path));

        // Reopening (any later generation) must resume into a *new* segment, since segment 0
        // already reached the (tiny) seal threshold and must never be written to again.
        auto writer2 = _open_writer(path, 3, offset1);
        writer2->insert(_row("b", 2));
        uint64_t offset2 = writer2->finalize();
        ASSERT_NE(offset2, constant::UNKNOWN_EXTENT);
        EXPECT_GT(SegmentSet::segment_id_of(offset2), SegmentSet::segment_id_of(offset1));

        auto segment1_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset2));
        ASSERT_TRUE(std::filesystem::exists(segment1_path));
        EXPECT_NE(segment0_path, segment1_path);
    }

    TEST_F(SegmentFileTest, SealedSegmentIsNeverRewritten)
    {
        auto path = _table_dir / "0.idx";

        auto writer1 = _create_writer(path, 2);
        writer1->insert(_row("a", 1));
        uint64_t offset1 = writer1->finalize();
        auto segment0_path = SegmentSet::segment_path(path, 0);

        ASSERT_TRUE(std::filesystem::exists(segment0_path));
        auto bytes_before = _read_bytes(segment0_path);

        // Several further generations' worth of mutation, each past the seal threshold again --
        // none of this should ever touch segment 0's bytes again.
        uint64_t offset = offset1;
        for (uint64_t xid = 3; xid < 8; xid++) {
            auto writer = _open_writer(path, xid, offset);
            writer->insert(_row(fmt::format("k{}", xid), xid));
            offset = writer->finalize();
        }

        auto bytes_after = _read_bytes(segment0_path);
        EXPECT_EQ(bytes_before, bytes_after)
            << "a sealed segment's on-disk bytes changed after later generations ran";
    }

    TEST_F(SegmentFileTest, MultiSegmentRoundTrip)
    {
        auto path = _table_dir / "0.idx";

        auto writer1 = _create_writer(path, 2);
        writer1->insert(_row("a", 1));
        uint64_t offset = writer1->finalize();

        std::vector<std::pair<std::string, uint64_t>> expected{ {"a", 1} };

        for (uint64_t xid = 3; xid < 10; xid++) {
            auto writer = _open_writer(path, xid, offset);
            std::string key = fmt::format("k{}", xid);
            writer->insert(_row(key, xid));
            expected.emplace_back(key, xid);
            offset = writer->finalize();
        }

        // Every generation past the first landed in its own segment (threshold == 1 byte), so
        // this exercises a real multi-segment append/read round trip, not just a single file.
        EXPECT_GT(SegmentSet::segment_id_of(offset), 0u);

        auto final_reader = _open_writer(path, 100, offset);
        auto actual = _read_all(final_reader);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(actual, expected);
    }

    TEST_F(SegmentFileTest, FullyDeadSegmentIsUnlinkedButLiveSegmentSurvives)
    {
        auto path = _table_dir / "0.idx";

        // Generation 1: a single-row tree, entirely in segment 0 -- immediately sealed (the
        // threshold is 1 byte).
        auto writer1 = _create_writer(path, 2);
        writer1->insert(_row("a", 1));
        uint64_t offset1 = writer1->finalize();
        auto segment0_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset1));
        ASSERT_TRUE(std::filesystem::exists(segment0_path));

        // Generation 2: add enough rows to the same leaf that the CoW rewrite's logical
        // byte_count() -- the size reported to the vacuumer as an estimate of the superseded
        // extent's size, per IndexWriter::_flush_node()'s existing approximation -- clearly
        // exceeds segment 0's *on-disk* size (which, for one tiny row, is dominated by fixed
        // per-extent/per-vector header overhead, not row content -- a single added row is not
        // enough margin to reliably clear that overhead). This generation's own flush also
        // immediately exceeds the threshold, so its content seals into its own (different,
        // still fully live) segment.
        auto writer2 = _open_writer(path, 3, offset1);
        std::vector<std::pair<std::string, uint64_t>> expected{ {"a", 1} };
        for (int i = 0; i < 20; i++) {
            std::string key = fmt::format("b{:02d}", i);
            writer2->insert(_row(key, i));
            expected.emplace_back(key, i);
        }
        uint64_t offset2 = writer2->finalize();
        auto segment1_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset2));
        ASSERT_NE(segment0_path, segment1_path);
        ASSERT_TRUE(std::filesystem::exists(segment1_path));

        // Advance the committed xid past generation 2's xid, so the vacuum cutoff recognizes
        // segment 0's dead bytes (registered at xid 3) as safely reclaimable (architecture.md
        // Sec 4: no live snapshot can reach them any longer).
        xid_mgr::XidMgrServer::get_instance()->commit_xid(_db_id, 1, 4, false, 0);
        Vacuumer::get_instance()->commit_expired_extents(_db_id, 4);

        Vacuumer::get_instance()->set_global_vacuum_threshold(1);
        Vacuumer::get_instance()->run_vacuum_once();

        EXPECT_FALSE(std::filesystem::exists(segment0_path))
            << "fully-dead segment should have been unlinked";
        EXPECT_TRUE(std::filesystem::exists(segment1_path))
            << "a segment with live content must survive vacuum";

        // The still-live segment must still read back correctly.
        auto reader = _open_writer(path, 100, offset2);
        auto actual = _read_all(reader);
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(actual, expected);
    }

    TEST_F(SegmentFileTest, VacuumOfDeadSegmentDoesNotDisturbConcurrentAppends)
    {
        auto path = _table_dir / "0.idx";

        auto writer1 = _create_writer(path, 2);
        writer1->insert(_row("a", 1));
        uint64_t offset1 = writer1->finalize();
        auto segment0_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset1));

        // See FullyDeadSegmentIsUnlinkedButLiveSegmentSurvives for why this needs enough rows
        // to clear segment 0's on-disk header overhead, not just one more row.
        auto writer2 = _open_writer(path, 3, offset1);
        std::vector<std::pair<std::string, uint64_t>> expected{ {"a", 1} };
        for (int i = 0; i < 20; i++) {
            std::string key = fmt::format("b{:02d}", i);
            writer2->insert(_row(key, i));
            expected.emplace_back(key, i);
        }
        uint64_t offset2 = writer2->finalize();

        xid_mgr::XidMgrServer::get_instance()->commit_xid(_db_id, 1, 4, false, 0);
        Vacuumer::get_instance()->commit_expired_extents(_db_id, 4);
        Vacuumer::get_instance()->set_global_vacuum_threshold(1);

        // One thread repeatedly vacuums (targeting the already-sealed, fully-dead segment 0)
        // while another keeps appending new generations -- each landing in its own new,
        // never-before-touched segment, since the writer only ever appends to its *current*
        // segment, which the vacuumer never considers (sealed segments only, by construction).
        // A short sleep between runs (mirroring the production background thread's own 1s
        // cadence, scaled down) rather than a bare spin loop: hammering run_vacuum_once() as
        // fast as possible exposes an unrelated, pre-existing race in the legacy global-vacuum-
        // file's read-vs-rename handling under that much contention (not something this phase's
        // segment logic introduced or needs to fix), which isn't representative of how the
        // vacuumer actually runs in production.
        std::atomic<bool> stop{false};
        std::thread vacuum_thread([&] {
            while (!stop.load()) {
                Vacuumer::get_instance()->run_vacuum_once();
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        });

        uint64_t offset = offset2;
        for (uint64_t xid = 5; xid < 30; xid++) {
            auto writer = _open_writer(path, xid, offset);
            std::string key = fmt::format("k{}", xid);
            writer->insert(_row(key, xid));
            expected.emplace_back(key, xid);
            offset = writer->finalize();
        }

        stop.store(true);
        vacuum_thread.join();
        Vacuumer::get_instance()->run_vacuum_once();

        EXPECT_FALSE(std::filesystem::exists(segment0_path));

        auto reader = _open_writer(path, 1000, offset);
        auto actual = _read_all(reader);
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(actual, expected);
    }

    /**
     * Regression coverage for phase-6 round-1 review finding 1: the sealed-segment whole-file
     * unlink decision (vacuumer.cc's sealed-segment branch) sums `expire_extent()`'s reported
     * dead-byte sizes and compares against the segment's actual on-disk size. Before the fix,
     * that reported size was the *replacement* extent's logical byte_count() -- an estimate with
     * no required relationship to the *superseded* extent's true size. An inflated estimate
     * could make a segment that still holds other, untouched, live, reachable content look
     * fully dead and get unlinked out from under a reader -- a direct architecture.md Sec 4
     * violation (and the opposite case, an underestimate, is a permanent-leak miss of this
     * phase's own "fully-dead segment is unlinked" acceptance criterion).
     *
     * This fixture deliberately uses the *default* (large) seal threshold, not the tiny
     * threshold SegmentFileTest's other tests use: a real two-plus-leaf split's extents need to
     * land in *one* segment so that segment can hold a mix of genuinely dead and genuinely live
     * content at once, which a tiny threshold (one extent per segment) can never produce.
     * Sealing is triggered explicitly via register_sealed_segment() rather than by crossing the
     * threshold naturally, to isolate "is the dead-byte sum correct" (what this test checks)
     * from "did the size threshold fire" (already covered by SegmentFileTest's other tests).
     */
    class SegmentDeadByteAccountingTest : public testing::Test {
    protected:
        static void SetUpTestSuite() {
            _base_dir = std::filesystem::temp_directory_path() / "test_segment_file_accounting" / "table";
            std::filesystem::remove_all(_base_dir);
            std::filesystem::create_directories(_base_dir);

            springtail_init_test();
            test::start_services(/*xid_mgr=*/true, /*sys_tbl_mgr=*/false, /*write_cache=*/false);
        }

        static void TearDownTestSuite() {
            std::filesystem::remove_all(_base_dir);
            springtail_shutdown();
        }

        void SetUp() override {
            _db_id = _next_db_id++;

            _table_dir = _base_dir / std::to_string(_db_id);
            std::filesystem::create_directories(_table_dir);

            std::vector<SchemaColumn> columns({
                    { "key", 0, SchemaType::TEXT, 0, false, 0 },
                    { "payload", 1, SchemaType::TEXT, 0, false }
                });
            _schema = std::make_shared<ExtentSchema>(columns, ExtensionCallback{}, false, false);
            _keys = { "key" };

            _cache = std::make_shared<IndexExtentCache>(64 * 1024);

            Vacuumer::get_instance()->enable_tracking_extents();
        }

        void TearDown() override {
            Vacuumer::get_instance()->cleanup_db(_db_id);
        }

        TuplePtr _row(const std::string &key, const std::string &payload) {
            auto fields = std::make_shared<FieldArray>();
            fields->push_back(std::make_shared<ConstTypeField<std::string>>(key));
            fields->push_back(std::make_shared<ConstTypeField<std::string>>(payload));
            return std::make_shared<FieldTuple>(fields, nullptr);
        }

        std::shared_ptr<BtreeIndexWriter>
        _create_writer(const std::filesystem::path &name, uint64_t xid, uint64_t max_extent_size)
        {
            auto writer = std::make_shared<BtreeIndexWriter>(
                _db_id, name, _keys, _schema, xid, _cache,
                BtreeWriteStrategy(max_extent_size));
            writer->init_empty();
            return writer;
        }

        std::shared_ptr<BtreeIndexWriter>
        _open_writer(const std::filesystem::path &name, uint64_t xid, uint64_t root_offset,
                     uint64_t max_extent_size)
        {
            auto writer = std::make_shared<BtreeIndexWriter>(
                _db_id, name, _keys, _schema, xid, _cache,
                BtreeWriteStrategy(max_extent_size));
            writer->init(root_offset);
            return writer;
        }

        std::vector<std::string>
        _read_all_keys(std::shared_ptr<BtreeIndexWriter> writer)
        {
            auto key_f = _schema->get_field("key");

            std::vector<std::string> keys;
            for (auto i = writer->begin(); i != writer->end(); ++i) {
                auto row = *i;
                keys.emplace_back(key_f->get_text(&row));
            }
            return keys;
        }

        uint64_t _db_id = 0;
        std::filesystem::path _table_dir;

        ExtentSchemaPtr _schema;
        std::vector<std::string> _keys;
        std::shared_ptr<IndexExtentCache> _cache;

        static std::filesystem::path _base_dir;
        static std::atomic<uint64_t> _next_db_id;
    };

    std::filesystem::path SegmentDeadByteAccountingTest::_base_dir;
    std::atomic<uint64_t> SegmentDeadByteAccountingTest::_next_db_id{800001};

    TEST_F(SegmentDeadByteAccountingTest, PartiallyDeadSegmentWithInflatedEstimateSurvivesVacuum)
    {
        auto path = _table_dir / "0.idx";

        // Generation 1, small split threshold: six short rows across two far-apart key ranges
        // ("a*", "z*") force exactly one split (midpoint = 3 rows), landing "a0,a1,a2" and
        // "z0,z1,z2" on two separate leaves, all under one root, all in segment 0.
        auto writer1 = _create_writer(path, 2, /*max_extent_size=*/60);
        for (int i = 0; i < 3; i++) {
            writer1->insert(_row(fmt::format("a{}", i), "short"));
            writer1->insert(_row(fmt::format("z{}", i), "short"));
        }
        uint64_t offset1 = writer1->finalize();

        auto segment0_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset1));
        ASSERT_TRUE(std::filesystem::exists(segment0_path));

        // Generation 2, deliberately *large* split threshold so this generation's own growth
        // never re-splits: replace "a1"'s payload with a long, highly compressible string
        // (LZ4 shrinks it to a small fraction of its logical length on disk). The CoW rewrite of
        // the "a" leaf is reported to the vacuumer as an estimate of what the *old* "a" leaf's
        // dead size was (IndexWriter::_flush_node(), pre-fix) -- using the *logical*,
        // uncompressed byte_count() of the replacement, which for this payload vastly exceeds
        // its own actual on-disk (compressed) footprint, let alone the old leaf's true size.
        // "z0,z1,z2" are never touched this generation -- still live, still reachable from the
        // new root.
        std::string huge_payload(20000, 'x');
        auto writer2 = _open_writer(path, 3, offset1, /*max_extent_size=*/1000000);
        writer2->remove(_row("a1", "short"));
        writer2->insert(_row("a1", huge_payload));
        uint64_t offset2 = writer2->finalize();
        ASSERT_EQ(SegmentSet::segment_id_of(offset2), SegmentSet::segment_id_of(offset1))
            << "test setup assumption: generation 2 must still land in segment 0 -- this test "
               "uses the default (large) seal threshold, nowhere near crossed by this little "
               "data, specifically so segment 0 ends up holding both dead and live content";

        // Explicitly mark segment 0 sealed/eligible for whole-file reclamation (see class
        // comment for why this is done directly rather than via the size threshold).
        Vacuumer::get_instance()->register_sealed_segment(segment0_path);

        xid_mgr::XidMgrServer::get_instance()->commit_xid(_db_id, 1, 4, false, 0);
        Vacuumer::get_instance()->commit_expired_extents(_db_id, 4);
        Vacuumer::get_instance()->set_global_vacuum_threshold(1);
        Vacuumer::get_instance()->run_vacuum_once();

        EXPECT_TRUE(std::filesystem::exists(segment0_path))
            << "segment 0 still holds the untouched, live 'z0,z1,z2' leaf -- it must survive "
               "vacuum even though the superseded 'a' leaf's *replacement* has a far larger "
               "logical byte_count() than either the original leaf or the replacement's own "
               "actual (compressed) on-disk size (an inflated size estimate must never be "
               "mistaken for 'fully dead')";

        // No data lost either way -- the stronger, always-true property.
        auto reader = _open_writer(path, 100, offset2, /*max_extent_size=*/1000000);
        auto actual = _read_all_keys(reader);
        std::vector<std::string> expected{ "a0", "a1", "a2", "z0", "z1", "z2" };
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(actual, expected);
    }

    /**
     * Regression test for phase-6 round-1 review finding 2: segment_seal_threshold_bytes is
     * operator-controlled config, not an in-process invariant, so a value that would overflow
     * pack()'s 32-bit in-segment-offset budget must be rejected with an always-on CHECK, not a
     * debug-only DCHECK that compiles out in release/production builds (NDEBUG) -- exactly the
     * build a misconfigured value would actually reach in production. Runs in a forked child
     * (ASSERT_DEATH) so the bad config and the resulting abort are isolated from every other
     * test in this binary. Lives here (not io_test.cc, where the rest of SegmentSet's pure
     * unit tests live) because default_seal_threshold_bytes() is out-of-line in segment_set.cc
     * alongside the stateful, Vacuumer-touching methods (see segment_set.hh's class comment),
     * so exercising it needs this binary's springtail_xid_mgr link, not io_test's.
     */
    TEST(SegmentSetConfigTest, SealThresholdAboveMaxOffsetFailsLoudly)
    {
        ASSERT_DEATH(
            {
                ::setenv(environment::ENV_OVERRIDE,
                        "storage.segment_seal_threshold_bytes=5000000000", 1);
                springtail_init_test();
                SegmentSet::default_seal_threshold_bytes();
            },
            "segment_seal_threshold_bytes.*must not exceed");
    }
}
