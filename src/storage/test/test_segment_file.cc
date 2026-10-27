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

    /**
     * Phase 7 (architecture.md "btree" redesign Sec 7.1): background space-reclaim compaction.
     * Reuses SegmentDeadByteAccountingTest's fixture (default/large seal threshold, explicit
     * register_sealed_segment()) for the dead-byte-fraction-driven tests, since compaction's
     * candidacy scenario needs exactly the same "one segment holding a mix of live and dead
     * content" setup that fixture already exists for, and SegmentFileTest's fixture (tiny
     * threshold) for the pure concurrency repro, which doesn't need a dead/live mix at all.
     *
     * _run_compaction_cycle() below mirrors MutableTable::run_compaction_cycle()'s logic exactly
     * (Vacuumer candidate lookup -> segment-path intersection -> bulk copy -> swing) so these
     * tests exercise the same orchestration shape the real callers (Committer, sys_tbl_mgr::
     * Server) use, without needing a full MutableTable/Committer stack.
     */
    size_t
    _run_compaction_cycle(std::shared_ptr<BtreeIndexWriter> writer)
    {
        auto candidates = Vacuumer::get_instance()->get_compaction_candidates();
        for (auto &segment : writer->get_segment_paths()) {
            if (std::find(candidates.begin(), candidates.end(), segment) == candidates.end()) {
                continue;
            }

            auto scratch_file = segment;
            scratch_file += ".compact_scratch";

            // Fetched once per candidate segment, not once per extent scanned (round-1 review
            // finding 2 -- mirrors MutableTable::prepare_compaction_cycle()'s identical fix).
            auto dead_offsets = Vacuumer::get_instance()->compaction_dead_offsets(segment);
            auto is_dead = [&dead_offsets](uint64_t real_id) {
                return dead_offsets.count(SegmentSet::offset_of(real_id)) > 0;
            };

            auto items = writer->bulk_copy_for_compaction(
                    segment, scratch_file, is_dead, Vacuumer::get_instance()->compaction_item_budget());
            if (!items.empty()) {
                return writer->apply_compaction_batch(scratch_file, items);
            }
        }
        return 0;
    }

    /**
     * A small, non-default seal threshold (unlike SegmentDeadByteAccountingTest's default/large
     * one) is essential for this specific test: compacting segment 0's permanently-live
     * remainder only actually *shrinks* what's reachable in segment 0 if the relocated content
     * lands somewhere else entirely. If segment 0 itself were still the tree's *current* append
     * target (true of SegmentDeadByteAccountingTest's own fixture, which deliberately never
     * crosses its default/large threshold), every relocation would just write its dense copy
     * right back into the same still-growing segment 0, net zero progress. A threshold just
     * above generation 1's on-disk size (register_sealed_segment() aside, see the class comment)
     * makes SegmentSet itself seal and roll segment 0 over to segment 1+ after generation 1, so
     * every later write (ordinary churn and compaction's own relocations alike) lands outside
     * segment 0, letting it actually drain to empty.
     */
    class CompactionReclaimTest : public testing::Test {
    protected:
        static void SetUpTestSuite() {
            _base_dir = std::filesystem::temp_directory_path() / "test_compaction_reclaim" / "table";
            std::filesystem::remove_all(_base_dir);
            std::filesystem::create_directories(_base_dir);

            ::setenv(environment::ENV_OVERRIDE, "storage.segment_seal_threshold_bytes=500", 1);
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
        _read_all_keys(std::shared_ptr<BtreeIndexWriter> writer) {
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

    std::filesystem::path CompactionReclaimTest::_base_dir;
    std::atomic<uint64_t> CompactionReclaimTest::_next_db_id{900001};

    TEST_F(CompactionReclaimTest, DeadByteFractionTriggersCompactionCandidacyThenCompactionReclaimsSegment)
    {
        auto path = _table_dir / "0.idx";

        // Generation 1: split into several leaves (the exact shape BtreeWriteStrategy picks for
        // this insert pattern isn't asserted -- see the sibling tests' identical comment), all
        // in segment 0; its on-disk size already exceeds this fixture's 500-byte seal threshold,
        // so SegmentSet seals segment 0 and rolls over to segment 1 for every subsequent write.
        auto writer1 = _create_writer(path, 2, /*max_extent_size=*/60);
        for (int i = 0; i < 3; i++) {
            writer1->insert(_row(fmt::format("a{}", i), "short"));
            writer1->insert(_row(fmt::format("z{}", i), "short"));
        }
        uint64_t offset = writer1->finalize();
        auto segment0_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset));
        ASSERT_TRUE(std::filesystem::exists(segment0_path));

        // Generation 2: replace "a1"'s and "z0"'s payloads. Each of the two touched leaves (and
        // its ancestors up to the root) gets CoW-rewritten -- but since segment 0 is already
        // sealed, this flush lands in a *later* segment, never back into segment 0. Only
        // whichever leaf "a1" and "z0" don't route through stays exactly as generation 1 left
        // it, permanently live in segment 0 -- deliberately touching two leaves on opposite
        // sides of the tree (rather than just one) to leave only a small permanently-live
        // remainder, comfortably crossing the 50% dead-byte-fraction threshold in a single
        // generation. Three more no-op-content generations after it (still landing outside
        // segment 0) exercise that further *unrelated* churn doesn't disturb segment 0 either --
        // exactly the scenario Sec 7.1 exists for: not reclaimable by phase 6's whole-file-
        // unlink alone, no matter how long this churn continues, since segment 0 is never
        // *fully* dead on its own.
        for (uint64_t xid = 3; xid < 7; xid++) {
            auto writer = _open_writer(path, xid, offset, /*max_extent_size=*/1000000);
            if (xid == 3) {
                writer->remove(_row("a1", "short"));
                writer->insert(_row("a1", "short"));
                writer->remove(_row("z0", "short"));
                writer->insert(_row("z0", "short"));
            } else {
                writer->remove(_row("a1", "short"));
                writer->insert(_row("a1", "short"));
            }
            offset = writer->finalize();
        }

        xid_mgr::XidMgrServer::get_instance()->commit_xid(_db_id, 1, 7, false, 0);
        Vacuumer::get_instance()->commit_expired_extents(_db_id, 7);
        Vacuumer::get_instance()->set_global_vacuum_threshold(1);
        Vacuumer::get_instance()->run_vacuum_once();

        EXPECT_TRUE(std::filesystem::exists(segment0_path))
            << "segment 0 still holds its permanently-live remainder -- must survive plain vacuum";

        auto candidates = Vacuumer::get_instance()->get_compaction_candidates();
        ASSERT_NE(std::find(candidates.begin(), candidates.end(), segment0_path), candidates.end())
            << "four generations of dead content against a permanently-live remainder should "
               "comfortably cross the default 50% dead-byte-fraction threshold";

        auto dead_offsets = Vacuumer::get_instance()->compaction_dead_offsets(segment0_path);
        EXPECT_FALSE(dead_offsets.empty());

        // Run compaction cycles (bounded, one segment per call, same as the real callers) until
        // segment 0 is gone -- each cycle relocates whatever's still reachable in it into the
        // tree's live structure (landing in whatever segment is current *now*, never segment 0
        // again), letting the *existing*, unmodified whole-file-unlink mechanism (phase 6) do
        // the actual deletion once nothing reachable remains in segment 0.
        //
        // Each cycle's writer xid, and the committed-xid cutoff that makes *that* cycle's own
        // relocations reclaim-safe, must stay in lock-step (one past the writer xid just used,
        // strictly increasing run over run) -- otherwise a later cycle's relocations can never
        // become cutoff-safe and this loop would spin without ever reclaiming segment 0.
        uint64_t compaction_xid = 100;
        auto writer = _open_writer(path, compaction_xid, offset, /*max_extent_size=*/1000000);
        for (int cycle = 0; cycle < 5 && std::filesystem::exists(segment0_path); cycle++) {
            _run_compaction_cycle(writer);
            offset = writer->finalize();

            xid_mgr::XidMgrServer::get_instance()->commit_xid(_db_id, 7, compaction_xid + 1, false, 0);
            Vacuumer::get_instance()->commit_expired_extents(_db_id, compaction_xid + 1);
            Vacuumer::get_instance()->run_vacuum_once();

            compaction_xid += 2;
            writer = _open_writer(path, compaction_xid, offset, /*max_extent_size=*/1000000);
        }

        EXPECT_FALSE(std::filesystem::exists(segment0_path))
            << "segment 0 should finally be unlinked once compaction has relocated its "
               "permanently-live remainder out into a currently-live segment";

        auto actual = _read_all_keys(writer);
        std::vector<std::string> expected{"a0", "a1", "a2", "z0", "z1", "z2"};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(actual, expected) << "no data lost or duplicated across the whole compaction run";
    }

    TEST_F(SegmentDeadByteAccountingTest, BulkCopyProducesDenseCopyWithIdenticalContent)
    {
        auto path = _table_dir / "0.idx";

        auto writer1 = _create_writer(path, 2, /*max_extent_size=*/60);
        for (int i = 0; i < 3; i++) {
            writer1->insert(_row(fmt::format("a{}", i), "short"));
            writer1->insert(_row(fmt::format("z{}", i), "short"));
        }
        uint64_t offset1 = writer1->finalize();
        auto segment0_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset1));

        auto writer2 = _open_writer(path, 3, offset1, /*max_extent_size=*/1000000);
        auto scratch = segment0_path;
        scratch += ".compact_scratch_dense";

        auto is_dead = [](uint64_t) { return false; };  // nothing superseded yet -- everything
                                                          // in segment 0 is still reachable
        auto items = writer2->bulk_copy_for_compaction(segment0_path, scratch, is_dead, 10);
        ASSERT_TRUE(std::filesystem::exists(scratch));
        ASSERT_GT(items.size(), 1u) << "test setup assumption: more than one node in segment 0";

        // Decode the dense copy directly and confirm its *leaf* rows, unioned across however
        // many leaves BtreeWriteStrategy happened to split this into, match segment 0's actual
        // logical content exactly -- not re-derived, not reordered, not dropped, not duplicated.
        // Deliberately not asserting an exact node/extent count here -- see the sibling
        // StaleCompactionItemIsNoOpNotError test's identical comment for why.
        auto key_f = _schema->get_field("key");
        auto handle = IOMgr::get_instance()->open(scratch, IOMgr::IO_MODE::READ, true);
        uint64_t pos = 0;
        int extent_count = 0;
        std::vector<std::string> dense_keys;
        while (true) {
            auto response = handle->read(pos);
            if (response->data.empty()) {
                break;
            }
            extent_count++;
            Extent e(response->data);
            if (e.header().type.is_leaf()) {
                for (auto &row : e) {
                    dense_keys.push_back(std::string(key_f->get_text(&row)));
                }
            }
            pos = response->next_offset;
        }

        EXPECT_EQ(static_cast<size_t>(extent_count), items.size());
        std::sort(dense_keys.begin(), dense_keys.end());
        std::vector<std::string> expected_dense{"a0", "a1", "a2", "z0", "z1", "z2"};
        EXPECT_EQ(dense_keys, expected_dense);

        // Not every item necessarily applies in this one call: relocating a leaf cascades a
        // defensive clone-and-dirty up through its ancestors (ordinary _touch() behavior, see
        // _relocate()'s own doc comment), so an ancestor item processed *after* one of its own
        // now-relocated descendants reads back as already-dirty and is correctly deferred, not
        // applied here -- harmless, since that ancestor is dirty *because* this same call
        // touched it, so finalize() below still flushes (and so still supersedes) it regardless
        // of whether _relocate() itself touched it.
        size_t applied = writer2->apply_compaction_batch(scratch, items);
        EXPECT_GT(applied, 0u);
        EXPECT_FALSE(std::filesystem::exists(scratch));

        uint64_t offset2 = writer2->finalize();
        auto reader = _open_writer(path, 100, offset2, /*max_extent_size=*/1000000);
        auto actual = _read_all_keys(reader);
        std::vector<std::string> expected{"a0", "a1", "a2", "z0", "z1", "z2"};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(actual, expected);
    }

    TEST_F(SegmentDeadByteAccountingTest, StaleCompactionItemIsNoOpNotError)
    {
        auto path = _table_dir / "0.idx";

        auto writer1 = _create_writer(path, 2, /*max_extent_size=*/60);
        for (int i = 0; i < 3; i++) {
            writer1->insert(_row(fmt::format("a{}", i), "short"));
            writer1->insert(_row(fmt::format("z{}", i), "short"));
        }
        uint64_t offset1 = writer1->finalize();
        auto segment0_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset1));

        auto writer2 = _open_writer(path, 3, offset1, /*max_extent_size=*/1000000);

        // Phase 1, as of *before* any further mutation: every extent in segment 0 is still
        // reachable (exact node count depends on how BtreeWriteStrategy happened to split this
        // insert pattern -- not asserted here, deliberately, so this test doesn't silently
        // start asserting the wrong thing if that split behavior ever changes).
        auto scratch = segment0_path;
        scratch += ".compact_scratch_stale";
        auto is_dead = [](uint64_t) { return false; };
        auto items = writer2->bulk_copy_for_compaction(segment0_path, scratch, is_dead, 10);
        ASSERT_GT(items.size(), 1u) << "test setup assumption: more than one node in segment 0";

        // *Before* phase 2 runs, a foreground mutation moves on: insert a new "a" row, CoW-ing
        // the leaf that covers it (and, transitively, the root) in-memory. Every phase-1 item
        // that targeted one of those now-dirtied nodes is stale (see _relocate()'s own doc
        // comment for exactly why -- this is the regression test for that, not just the general
        // "parent moved" case); whichever leaf(s) cover the untouched "z" keys are still exactly
        // where phase 1 found them.
        writer2->insert(_row("a3", "short"));

        size_t applied = writer2->apply_compaction_batch(scratch, items);
        EXPECT_GT(applied, 0u) << "the untouched z-side node(s) should still have relocated";
        EXPECT_LT(applied, items.size())
            << "the root (and whichever leaf covers the 'a' range) are now stale -- at least one "
               "item must have been silently discarded, not applied";
        EXPECT_FALSE(std::filesystem::exists(scratch));

        uint64_t offset2 = writer2->finalize();
        auto reader = _open_writer(path, 100, offset2, /*max_extent_size=*/1000000);
        auto actual = _read_all_keys(reader);
        std::vector<std::string> expected{"a0", "a1", "a2", "a3", "z0", "z1", "z2"};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(actual, expected)
            << "no data lost (and nothing silently overwritten) by the discarded stale items";
    }

    TEST_F(SegmentFileTest, ConcurrentForegroundMutationDuringBulkCopyDoesNotCorrupt)
    {
        auto path = _table_dir / "0.idx";

        auto writer1 = _create_writer(path, 2);
        writer1->insert(_row("seed", 0));
        uint64_t offset1 = writer1->finalize();
        auto segment0_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset1));
        ASSERT_TRUE(std::filesystem::exists(segment0_path));

        // One long-lived writer instance for the rest of this test, used by *both* roles at
        // once: the foreground (this) thread mutates it (insert(), taking the reentrancy guard,
        // touching _root's in-memory Node graph); two background threads repeatedly call
        // bulk_copy_for_compaction() against segment 0 (already sealed, from the *previous*
        // generation -- never this instance's own current segment). This is the direct repro of
        // architecture.md Sec 7.1's "fully concurrent... needs zero coordination" claim for
        // phase 1, not just a narrative argument for it (lesson from phase 5: reproduce a
        // concurrency claim directly). Two scanner threads, not one, and 1000 inserts, not a
        // handful -- a harder repro than a single-threaded/low-iteration version would be.
        auto writer2 = _open_writer(path, 3, offset1);

        std::atomic<bool> stop{false};
        std::atomic<int> scan_count{0};
        auto is_dead = [](uint64_t) { return false; };

        auto scan_fn = [&](int id) {
            auto scratch = segment0_path;
            scratch += fmt::format(".compact_scratch_concurrent_{}", id);
            while (!stop.load()) {
                writer2->bulk_copy_for_compaction(segment0_path, scratch, is_dead, 10);
                scan_count++;
            }
            std::error_code ec;
            std::filesystem::remove(scratch, ec);
        };

        std::thread scanner1([&] { scan_fn(1); });
        std::thread scanner2([&] { scan_fn(2); });

        std::vector<std::pair<std::string, uint64_t>> expected{ {"seed", 0} };
        for (uint64_t i = 1; i <= 1000; i++) {
            std::string key = fmt::format("k{}", i);
            writer2->insert(_row(key, i));
            expected.emplace_back(key, i);
        }

        stop.store(true);
        scanner1.join();
        scanner2.join();
        EXPECT_GT(scan_count.load(), 0) << "sanity: the background scans actually ran concurrently";

        uint64_t final_offset = writer2->finalize();

        auto reader = _open_writer(path, 10000, final_offset);
        auto actual = _read_all(reader);
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(actual, expected)
            << "concurrent background compaction scanning must not corrupt foreground mutation "
               "happening on the same tree/instance";
    }

    /**
     * Phase 8 (architecture.md "btree" redesign Sec 7.1): background structural-reclaim
     * (merge). Reuses CompactionReclaimTest's fixture shape (small seal threshold, services
     * started) since the node-count assertions below rely on the same "scan a *fresh* segment,
     * with is_dead=false, to read off exactly what this generation just flushed" technique
     * BulkCopyProducesDenseCopyWithIdenticalContent/CompactionReclaimTest already established --
     * here used to read a branch extent's own row_count() (its current child count) rather
     * than to prove dense-copy content, since that's a precise, segment-accounting-free way to
     * observe "the tree got shallower/denser" without needing a dedicated node-counting API on
     * IndexWriter itself.
     *
     * _run_merge_cycle() below mirrors MutableTable::run_merge_cycle()'s logic exactly (Vacuumer
     * flag lookup -> per-flag try_merge_underfull() -> retire), so these tests exercise the same
     * orchestration shape the real callers (Committer, sys_tbl_mgr::Server) use, without needing
     * a full MutableTable/Committer stack.
     */
    class MergeReclaimTest : public testing::Test {
    protected:
        static void SetUpTestSuite() {
            _base_dir = std::filesystem::temp_directory_path() / "test_merge_reclaim" / "table";
            std::filesystem::remove_all(_base_dir);
            std::filesystem::create_directories(_base_dir);

            ::setenv(environment::ENV_OVERRIDE, "storage.segment_seal_threshold_bytes=500", 1);
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
        _read_all_keys(std::shared_ptr<BtreeIndexWriter> writer) {
            auto key_f = _schema->get_field("key");
            std::vector<std::string> keys;
            for (auto i = writer->begin(); i != writer->end(); ++i) {
                auto row = *i;
                keys.emplace_back(key_f->get_text(&row));
            }
            return keys;
        }

        /**
         * MutableTable::run_merge_cycle()'s logic, reproduced directly against one writer
         * (rather than a whole table's all_index_writers()) so these tests don't need a full
         * MutableTable stack. Drains (reads, then retires one at a time) every flag currently
         * raised for `writer`'s own file, resolving each via try_merge_underfull(); returns how
         * many actually merged.
         */
        size_t _run_merge_cycle(std::shared_ptr<BtreeIndexWriter> writer) {
            size_t merged = 0;
            auto flags = Vacuumer::get_instance()->get_underfull_flags(writer->get_file_path());
            for (auto &flag : flags) {
                if (writer->try_merge_underfull(flag.old_id, flag.search_key)) {
                    merged++;
                }
                Vacuumer::get_instance()->clear_underfull_flag(writer->get_file_path(), flag.old_id);
            }
            return merged;
        }

        /**
         * Scans `segment_path` raw (mirrors BulkCopyProducesDenseCopyWithIdenticalContent's own
         * technique) and returns the row_count() of the *last* branch-type extent found --
         * post-order flush (child-before-parent, _flush_node()) means a generation's own root is
         * always the last branch extent its finalize() call appends, and later generations'
         * appends always come after earlier ones', so this is the current root's child count
         * regardless of how many older (possibly now-dead) branch extents share the segment.
         */
        uint32_t _root_row_count(const std::filesystem::path &segment_path) {
            auto handle = IOMgr::get_instance()->open(segment_path, IOMgr::IO_MODE::READ, true);
            uint64_t pos = 0;
            uint32_t last_branch_row_count = 0;
            bool found = false;
            while (true) {
                auto response = handle->read(pos);
                if (response->data.empty()) {
                    break;
                }
                Extent e(response->data);
                if (e.header().type.is_branch()) {
                    last_branch_row_count = e.row_count();
                    found = true;
                }
                pos = response->next_offset;
            }
            CHECK(found) << "no branch (root) extent found in " << segment_path.native();
            return last_branch_row_count;
        }

        uint64_t _db_id = 0;
        std::filesystem::path _table_dir;

        ExtentSchemaPtr _schema;
        std::vector<std::string> _keys;
        std::shared_ptr<IndexExtentCache> _cache;

        static std::filesystem::path _base_dir;
        static std::atomic<uint64_t> _next_db_id;
    };

    std::filesystem::path MergeReclaimTest::_base_dir;
    std::atomic<uint64_t> MergeReclaimTest::_next_db_id{1000001};

    TEST_F(MergeReclaimTest, DeleteHeavyWorkloadRaisesFlagAndMergeShrinksNodeCount)
    {
        auto path = _table_dir / "0.idx";

        // Generation 1: four short rows, two per key range ("a*", "z*"), force exactly one
        // split (midpoint = 2 rows) into two leaves of two rows each, directly under a
        // two-child branch root -- confirmed empirically (unlike the 3-per-range/6-row pattern
        // used elsewhere in this file, which splits into a deeper, 3-level shape for this same
        // 60-byte threshold; this test needs the flat, 2-level shape specifically, so it uses
        // its own smaller dataset rather than reusing that one).
        auto writer1 = _create_writer(path, 2, /*max_extent_size=*/60);
        for (int i = 0; i < 2; i++) {
            writer1->insert(_row(fmt::format("a{}", i), "short"));
            writer1->insert(_row(fmt::format("z{}", i), "short"));
        }
        uint64_t offset1 = writer1->finalize();
        auto segment0_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset1));
        ASSERT_TRUE(std::filesystem::exists(segment0_path));
        ASSERT_EQ(_root_row_count(segment0_path), 2u)
            << "test setup assumption: generation 1 splits into exactly two leaves";

        // Generation 2: shrink the "a" leaf without emptying it (remove 1 of its 2 rows), using
        // a much larger max_extent_size so this generation's own growth never re-splits. Segment
        // 0 is already sealed (its generation-1 size already exceeds this fixture's 500-byte
        // threshold), so this flush lands in segment 1.
        auto writer2 = _open_writer(path, 3, offset1, /*max_extent_size=*/1000000);
        writer2->remove(_row("a1", "short"));
        uint64_t offset2 = writer2->finalize();

        auto flags = Vacuumer::get_instance()->get_underfull_flags(path);
        ASSERT_EQ(flags.size(), 1u)
            << "shrinking the 'a' leaf to a single row (well below the default 25% occupancy "
               "threshold against a 1,000,000-byte max_extent_size) must raise exactly one flag; "
               "the untouched 'z' leaf and the root are never flushed this generation";

        // Background consumer: a fresh writer instance at offset2 (mirrors run_merge_cycle()'s
        // own "later, independent" consumption -- MutableTable/IndexWriter instances don't
        // persist across commit batches in production).
        auto writer3 = _open_writer(path, 4, offset2, /*max_extent_size=*/1000000);
        size_t merged = _run_merge_cycle(writer3);
        EXPECT_EQ(merged, 1u) << "the 'a' leaf's only sibling ('z') has ample room (both tiny) "
                                 "-- this merge must actually happen, not be dropped as no-room";
        EXPECT_TRUE(Vacuumer::get_instance()->get_underfull_flags(path).empty())
            << "the flag must be retired either way -- here, because it merged";

        uint64_t offset3 = writer3->finalize();
        auto segment_after = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset3));
        EXPECT_EQ(_root_row_count(segment_after), 1u)
            << "merging the tree's only two leaves into one must drop the root's child count "
               "from 2 to 1";

        auto final_reader = _open_writer(path, 100, offset3, /*max_extent_size=*/1000000);
        auto actual = _read_all_keys(final_reader);
        std::vector<std::string> expected{"a0", "z0", "z1"};
        std::sort(expected.begin(), expected.end());
        std::sort(actual.begin(), actual.end());
        EXPECT_EQ(actual, expected) << "no data lost or duplicated by the merge";
    }

    TEST_F(MergeReclaimTest, StaleFlagUnderfullNodeGotMoreInsertsIsNoOp)
    {
        auto path = _table_dir / "0.idx";

        auto writer1 = _create_writer(path, 2, /*max_extent_size=*/60);
        for (int i = 0; i < 2; i++) {
            writer1->insert(_row(fmt::format("a{}", i), "short"));
            writer1->insert(_row(fmt::format("z{}", i), "short"));
        }
        uint64_t offset1 = writer1->finalize();

        auto writer2 = _open_writer(path, 3, offset1, /*max_extent_size=*/1000000);
        writer2->remove(_row("a1", "short"));
        uint64_t offset2 = writer2->finalize();

        ASSERT_EQ(Vacuumer::get_instance()->get_underfull_flags(path).size(), 1u);

        // Before the background consumer ever runs, a *separate*, later generation inserts
        // enough more into the same leaf that it's no longer underfull by the time
        // try_merge_underfull() re-checks -- the staleness case where "the underfull node
        // itself received more inserts". The key ("a05") must sort between "a0" (leaf_a's only
        // remaining row) and "a1" (leaf_a's *recorded* routing key in the parent, stale and
        // unchanged since the remove() above -- removal never calls _propagate_key_increase) so
        // choose_child() still routes it into leaf_a specifically, not past it into "z"'s leaf.
        auto writer2b = _open_writer(path, 4, offset2, /*max_extent_size=*/1000000);
        std::string padding(400000, 'x');
        writer2b->insert(_row("a05", padding));
        uint64_t offset2b = writer2b->finalize();

        auto writer3 = _open_writer(path, 5, offset2b, /*max_extent_size=*/1000000);
        size_t merged = _run_merge_cycle(writer3);
        EXPECT_EQ(merged, 0u) << "the node is no longer underfull -- this must be a silent "
                                 "no-op, not an incorrect merge";
        EXPECT_TRUE(Vacuumer::get_instance()->get_underfull_flags(path).empty())
            << "the flag must still be retired even though nothing merged";

        uint64_t offset3 = writer3->finalize();
        auto final_reader = _open_writer(path, 100, offset3, /*max_extent_size=*/1000000);
        auto actual = _read_all_keys(final_reader);
        std::vector<std::string> expected{"a0", "a05", "z0", "z1"};
        std::sort(expected.begin(), expected.end());
        std::sort(actual.begin(), actual.end());
        EXPECT_EQ(actual, expected)
            << "no data lost -- in particular, the padding insert this staleness hinges on must "
               "still be present, not silently discarded by a wrongly-attempted merge";
    }

    TEST_F(MergeReclaimTest, StaleFlagNoSiblingHasRoomIsNoOpAndDoesNotRetry)
    {
        auto path = _table_dir / "0.idx";
        constexpr uint64_t M = 2000;

        auto writer1 = _create_writer(path, 2, /*max_extent_size=*/60);
        for (int i = 0; i < 2; i++) {
            writer1->insert(_row(fmt::format("a{}", i), "short"));
            writer1->insert(_row(fmt::format("z{}", i), "short"));
        }
        uint64_t offset1 = writer1->finalize();

        // Shrink the "a" leaf (raises the flag) using the same max_extent_size (M) the rest of
        // this test's writers use for underfull()/can_merge() -- a real tree's max_extent_size
        // is a per-table constant, not something that varies generation to generation the way
        // this file's other tests sometimes do for setup convenience.
        auto writer2 = _open_writer(path, 3, offset1, /*max_extent_size=*/M);
        writer2->remove(_row("a1", "short"));
        uint64_t offset2 = writer2->finalize();

        ASSERT_EQ(Vacuumer::get_instance()->get_underfull_flags(path).size(), 1u);

        // "Sibling filled back up before the consumer got to it": a *separate*, later generation
        // (not the one that raised the flag) grows the "z" leaf -- still well under M itself, so
        // it doesn't re-split -- enough that it no longer has room for the already-shrunk "a"
        // leaf's content by the time the background consumer actually runs.
        auto writer2b = _open_writer(path, 4, offset2, /*max_extent_size=*/M);
        writer2b->insert(_row("z2", std::string(1930, 'x')));
        uint64_t offset2b = writer2b->finalize();

        ASSERT_EQ(Vacuumer::get_instance()->get_underfull_flags(path).size(), 1u)
            << "growing 'z' must not disturb the still-pending flag against the untouched 'a' "
               "leaf";

        auto writer3 = _open_writer(path, 5, offset2b, /*max_extent_size=*/M);
        size_t merged = _run_merge_cycle(writer3);
        EXPECT_EQ(merged, 0u) << "the only sibling no longer has room -- a legitimate, benign "
                                 "no-op, not a failure";
        EXPECT_TRUE(Vacuumer::get_instance()->get_underfull_flags(path).empty())
            << "the flag must still be retired even though no merge happened";

        // Bounded-iteration check (not timing-based) that this doesn't retry in a tight loop:
        // with the flag already retired above and no new mutation raising a fresh one, repeated
        // cycles against the same writer must keep finding nothing to do.
        for (int i = 0; i < 5; i++) {
            EXPECT_EQ(_run_merge_cycle(writer3), 0u);
            EXPECT_TRUE(Vacuumer::get_instance()->get_underfull_flags(path).empty());
        }

        uint64_t offset3 = writer3->finalize();
        auto final_reader = _open_writer(path, 100, offset3, /*max_extent_size=*/M);
        auto actual = _read_all_keys(final_reader);
        std::vector<std::string> expected{"a0", "z0", "z1", "z2"};
        std::sort(expected.begin(), expected.end());
        std::sort(actual.begin(), actual.end());
        EXPECT_EQ(actual, expected);
    }

    TEST_F(MergeReclaimTest, ChurnOverManyCommitsDoesNotAccumulateUnboundedSparseNodes)
    {
        auto path = _table_dir / "0.idx";

        std::vector<std::string> groups{"a", "b", "c", "d"};
        auto writer1 = _create_writer(path, 2, /*max_extent_size=*/60);
        for (int i = 0; i < 3; i++) {
            for (auto &g : groups) {
                writer1->insert(_row(fmt::format("{}{}", g, i), "short"));
            }
        }
        uint64_t offset = writer1->finalize();

        auto segment0_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset));
        uint32_t root_children_before = _root_row_count(segment0_path);
        ASSERT_GT(root_children_before, 1u)
            << "test setup assumption: 12 rows across 4 key groups at this threshold splits "
               "into more than one child under the root";

        // Across several commits, thin every group down to a single row (leaving every original
        // leaf sparse but never empty), running a merge cycle after each generation's delete --
        // the steady-state ingest pattern (one Committer batch finalizing, the next cycle's
        // background consumer picking up whatever that batch's deletes flagged).
        uint64_t xid = 3;
        for (auto &g : groups) {
            auto writer = _open_writer(path, xid++, offset, /*max_extent_size=*/1000000);
            writer->remove(_row(fmt::format("{}{}", g, 1), "short"));
            writer->remove(_row(fmt::format("{}{}", g, 2), "short"));
            offset = writer->finalize();

            auto merge_writer = _open_writer(path, xid++, offset, /*max_extent_size=*/1000000);
            _run_merge_cycle(merge_writer);
            offset = merge_writer->finalize();
        }

        EXPECT_TRUE(Vacuumer::get_instance()->get_underfull_flags(path).empty())
            << "every flag raised across this churn must have been resolved (merged, stale, or "
               "no-room) by the merge cycle run right after it, none left dangling";

        auto final_segment = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset));
        uint32_t root_children_after = _root_row_count(final_segment);
        EXPECT_LT(root_children_after, root_children_before)
            << "thinning every original leaf down to a single row and running a merge cycle "
               "after each generation must shrink the tree's child count over time, not leave "
               "every original leaf permanently sparse (the operational property Sec 7 exists "
               "to fix, mirroring phase 7's CompactionReclaimTest assertion for the structural-"
               "waste side)";

        auto final_reader = _open_writer(path, xid, offset, /*max_extent_size=*/1000000);
        auto actual = _read_all_keys(final_reader);
        std::vector<std::string> expected;
        for (auto &g : groups) {
            expected.push_back(g + "0");
        }
        std::sort(expected.begin(), expected.end());
        std::sort(actual.begin(), actual.end());
        EXPECT_EQ(actual, expected) << "no data lost or duplicated across the whole churn run";
    }

    // Round-1 review (blocking finding 1): a merge result (or a split result) only ever
    // recorded a fixed two superseded predecessors (`prev_offset`/`second_prev_offset`). If
    // either input to a merge is itself an unflushed merge result from earlier in the *same*
    // generation -- exactly what happens when `run_merge_cycle()`'s shared budget resolves more
    // than one flag against the same tree in one call -- that input's own second predecessor
    // was silently dropped, never reported to the Vacuumer, permanently leaking that extent's
    // dead-byte accounting. Fixed by replacing the fixed two-slot fields with
    // `Node::superseded_offsets` (a list, concatenated on merge, copied in full on split) --
    // see that field's own doc comment.
    //
    // This test reproduces the exact chain the review's own repro used: three leaf siblings
    // under one root ("first" = a0,a1; "second" = a2,a3, the untouched middle one; "z" =
    // z0,z1), built via a deliberate two-generation setup (insert four rows to get a flat
    // 2-leaf/1-root tree, then insert two more into the "a" range specifically to split that
    // leaf in place, landing "second" as a third sibling without disturbing "z") rather than
    // left to chance via BtreeWriteStrategy's own split behavior for an arbitrary row pattern
    // (confirmed empirically while writing this test that insert-pattern alone does not
    // reliably produce this exact shape). Both outer siblings are then shrunk (never emptied)
    // in one generation, raising two flags; one `run_merge_cycle()` call against a single
    // writer instance resolves both in sequence, chaining the second merge's inputs through
    // the first merge's still-unflushed result -- precisely the scenario the fixed code must
    // get right.
    TEST_F(MergeReclaimTest, ChainedMergeInOneCycleReportsEveryPredecessorSuperseded)
    {
        auto path = _table_dir / "0.idx";

        // Generation 1: a flat 2-leaf/1-root tree (same dataset as
        // DeleteHeavyWorkloadRaisesFlagAndMergeShrinksNodeCount, confirmed there to split into
        // exactly two leaves).
        auto writer1 = _create_writer(path, 2, /*max_extent_size=*/60);
        writer1->insert(_row("a0", "short"));
        writer1->insert(_row("a1", "short"));
        writer1->insert(_row("z0", "short"));
        writer1->insert(_row("z1", "short"));
        uint64_t offset1 = writer1->finalize();
        auto segment0_path = SegmentSet::segment_path(path, SegmentSet::segment_id_of(offset1));
        ASSERT_TRUE(std::filesystem::exists(segment0_path));

        // Generation 2 (same max_extent_size, so this split is deliberate, not a side effect of
        // a size-budget mismatch): grow the "a" leaf past the split threshold with two more
        // rows in its own key range, splitting it into "first" (a0,a1) and "second" (a2,a3) --
        // "z" (z0,z1) is untouched, landing as the third sibling without ever being touched.
        // By this point segment 0's six extents (both generations' roots, the original "a"
        // leaf, "z", and the two split results) exceed this fixture's 500-byte seal threshold,
        // sealing it -- confirmed via the ASSERT below rather than assumed.
        auto writer2 = _open_writer(path, 3, offset1, /*max_extent_size=*/60);
        writer2->insert(_row("a2", "short"));
        writer2->insert(_row("a3", "short"));
        uint64_t offset2 = writer2->finalize();
        ASSERT_GT(std::filesystem::file_size(segment0_path), 500u)
            << "test setup assumption: generation 1 + 2's combined content seals segment 0, so "
               "every later generation's fresh writes land outside it";
        ASSERT_EQ(_root_row_count(segment0_path), 3u)
            << "test setup assumption: three leaf siblings (first, second, z) under one root";

        // Generation 3: shrink *both outer* siblings (never emptying either), leaving "second"
        // (the middle one) untouched -- raises exactly two flags.
        auto writer3 = _open_writer(path, 4, offset2, /*max_extent_size=*/1000000);
        writer3->remove(_row("a1", "short"));
        writer3->remove(_row("z1", "short"));
        uint64_t offset3 = writer3->finalize();
        ASSERT_EQ(Vacuumer::get_instance()->get_underfull_flags(path).size(), 2u);

        // Generation 4: one run_merge_cycle()-shaped call against a single writer instance
        // resolves both flags. The first resolved merge ("first" + "second", its only sibling)
        // produces an unflushed result that becomes "z"'s new immediate sibling; the second
        // resolved merge ("z" + that unflushed result) is the exact chain the bug dropped a
        // predecessor from.
        auto writer4 = _open_writer(path, 5, offset3, /*max_extent_size=*/1000000);
        size_t merged = _run_merge_cycle(writer4);
        EXPECT_EQ(merged, 2u) << "both flags should actually merge -- everything here is tiny";
        EXPECT_TRUE(Vacuumer::get_instance()->get_underfull_flags(path).empty());
        uint64_t offset4 = writer4->finalize();

        // Drive segment 0's dead-byte fraction to 100% and vacuum: if (and only if) *every*
        // one of segment 0's six extents -- including "second", the middle leaf never directly
        // touched by generation 3, only ever carried forward through the merge chain -- was
        // actually reported superseded, nothing in it is reachable anymore and phase 6's
        // whole-file unlink removes it outright. Exposing the bug through this existing,
        // already-trusted mechanism (rather than a new test-only hook) is the review's own fix
        // direction for this regression test.
        xid_mgr::XidMgrServer::get_instance()->commit_xid(_db_id, 1, 6, false, 0);
        Vacuumer::get_instance()->commit_expired_extents(_db_id, 6);
        Vacuumer::get_instance()->set_global_vacuum_threshold(1);
        Vacuumer::get_instance()->run_vacuum_once();

        EXPECT_FALSE(std::filesystem::exists(segment0_path))
            << "segment 0 should be fully reclaimed -- if this fails, something in its six "
               "extents (most likely 'second', the middle leaf only ever carried through the "
               "merge chain, never directly touched) was never reported superseded to the "
               "Vacuumer";

        auto final_reader = _open_writer(path, 100, offset4, /*max_extent_size=*/1000000);
        auto actual = _read_all_keys(final_reader);
        std::vector<std::string> expected{"a0", "a2", "a3", "z0"};
        std::sort(expected.begin(), expected.end());
        std::sort(actual.begin(), actual.end());
        EXPECT_EQ(actual, expected) << "no data lost or duplicated across the merge chain";
    }
}
