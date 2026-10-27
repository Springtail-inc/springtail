#include <algorithm>
#include <set>

#include <gtest/gtest.h>

#include <common/init.hh>
#include <pg_ext/extn_registry.hh>
#include <storage/index_engine/gist_reader.hh>
#include <storage/index_engine/gist_writer.hh>
#include <storage/index_engine/storage_cache_extent_source.hh>

using namespace springtail;

namespace {
    // pg_trgm's EqualStrategyNumber (contrib/pg_trgm/trgm.h) -- the exact-text-match strategy its
    // gtrgm_consistent() supports, used here instead of a plain btree "=" strategy number.
    constexpr uint16_t TRGM_STRATEGY_EQ = 11;

    /**
     * Registers pg_trgm's real gist_trgm_ops opclass support functions directly (dlopen + dlsym
     * by their known, stable contrib/pg_trgm function names -- confirmed against the real source
     * under external/vcpkg's libpq buildtree, see gist_helpers.hh's Design notes), bypassing the
     * live-Postgres-connection-driven PgExtnRegistry::init_libraries()/add_opclass() path
     * (PgCopyTable::_load_extn_opclasses()) a real daemon uses: this is a C++ unit test with no
     * live FDW/primary connection of its own, and no catalog to query pg_opclass/pg_proc against,
     * so it reconstructs just enough of what that path would have produced by hand.
     *
     * pg_trgm, not btree_gist, despite btree_gist's gist_text_ops being the more "obvious" choice
     * for a plain TEXT column: confirmed directly (not guessed) that btree_gist.so's dlopen()
     * fails in this environment -- `nm -D --undefined-only` shows it references dozens of
     * Postgres's own builtin type-comparator functions (bttextcmp, date_cmp, interval_cmp,
     * macaddr_cmp, enum_cmp, ...) that only exist inside a real `postgres` backend process and
     * are not, and are not meant to be, reimplemented by pg_ext's emulation layer. pg_trgm.so's
     * undefined symbols, by contrast, are all satisfied by springtail_pgext (confirmed the same
     * way) -- matching why cube/hstore/pg_trgm, not btree_gist/btree_gin, are the three
     * extensions already configured in system.json.settings' extension_config today.
     */
    void register_gist_trgm_opclass(const std::string &opclass_name) {
        auto registry = PgExtnRegistry::get_instance();
        registry->init_libraries(0, opclass_name, "/usr/lib/postgresql/16/lib/pg_trgm.so");

        auto add = [&](int support_number, const std::string &function_name) {
            PgOpsClass opclass;
            opclass.name = opclass_name;
            opclass.access_method = "gist";

            PgOpsClassMethod method;
            method.function_name = function_name;
            method.support_number = support_number;

            registry->add_opclass(opclass_name, opclass, method);
        };

        add(GIST_COMPRESS, "gtrgm_compress");
        add(GIST_UNION, "gtrgm_union");
        add(GIST_PENALTY, "gtrgm_penalty");
        add(GIST_PICKSPLIT, "gtrgm_picksplit");
        add(GIST_CONSISTENT, "gtrgm_consistent");
    }

    /**
     * GistWriter/GistReader tests, mirroring test_index_writer.cc/test_index_reader.cc's direct-
     * engine-construction style (no Indexer/Table involved -- see src/pg_log_mgr/test/
     * test_indexer.cc's Indexer_Test.Test_GistBuild for the full Indexer-driven build, and
     * python/testing/test_cases/basic/GIST_Index_Build_And_Search.sql for the real
     * CREATE INDEX ... USING gist(...) DDL path).
     *
     * Two opclasses are used, deliberately, for two different purposes -- see
     * phases/phase-10.md's Design notes for the full writeup:
     *
     * - gist_trgm_ops (real, via pg_trgm.so, dlopen'd/registered exactly as a real daemon would)
     *   for SingleEntryRoundTrips/LeafEntriesReferenceRowsByInternalRowIdOnly: confirmed stable,
     *   proves the real opclass-interop path (gist_helpers.cc's ABI-calling glue) genuinely
     *   works end-to-end for compress+consistent against production Postgres C code.
     *
     * - gist_helpers::TEST_RANGE_OPCLASS (a built-in, pure-C++ reference opclass, no dlopen/ABI
     *   calls at all -- see gist_helpers.cc's test_range_ops namespace) for
     *   SplitProducesNonDegenerateShape/PredicateFanOutAcrossOverlappingLeaves: these two tests
     *   need *correct* picksplit/union/penalty dynamics over many entries, and a confirmed, not
     *   fully root-caused crash in pg_trgm.so's own compiled gtrgm_union()/gtrgm_penalty() (found
     *   and extensively investigated during this phase -- three real, independent pg_ext bugs
     *   fixed along the way: a missing-FmgrInfo null deref, SET_VARSIZE_4B's missing bit-shift,
     *   and a BlockMap std::map key-collision use-after-free, documented in
     *   phases/phase-10.md's Design notes) makes the real opclass unusable for any multi-entry
     *   build. The reference opclass isolates "is GistWriter/GistReader's own engine logic
     *   correct" from "is the real-opclass ABI-interop layer reliable" -- this phase's actual
     *   deliverable is the former.
     */
    class GistStrategyTest : public testing::Test {
    protected:
        static void SetUpTestSuite() {
            springtail_init_test(LOG_ALL ^ LOG_STORAGE);
            register_gist_trgm_opclass("gist_trgm_ops");
        }

        static void TearDownTestSuite() {
            springtail_shutdown();
        }

        void SetUp() override {
            _base_dir = std::filesystem::temp_directory_path() / "test_gist_strategy";
            std::filesystem::create_directories(_base_dir);
            _cache = std::make_shared<IndexExtentCache>(64 * 1024);
        }

        void TearDown() override {
            std::filesystem::remove_all(_base_dir);
        }

        std::shared_ptr<GistWriter> _create_writer(const std::filesystem::path &name,
                                                   const std::string &opclass,
                                                   uint64_t max_extent_size = 512) {
            auto writer = std::make_shared<GistWriter>(0, name, /*xid=*/1,
                    GistWriteStrategy({opclass}, max_extent_size), _cache);
            writer->init_empty();
            return writer;
        }

        std::shared_ptr<GistReader> _open_reader(const std::filesystem::path &name, uint64_t root_offset,
                                                 const std::string &opclass) {
            return std::make_shared<GistReader>(0, name, /*xid=*/1, root_offset,
                    std::make_shared<StorageCacheExtentSource>(), GistReadStrategy({opclass}));
        }

        /** value/internal_row_id -> a TuplePtr matching GistWriter::insert()'s calling
            convention (field 0 = the indexed column's raw value, field 1 = internal_row_id) --
            the same shape Indexer::_build()'s real scan-and-insert loop constructs. */
        TuplePtr _row(const std::string &value, uint64_t internal_row_id) {
            auto fields = std::make_shared<FieldArray>();
            fields->push_back(std::make_shared<ConstTypeField<std::string>>(value));
            fields->push_back(std::make_shared<ConstTypeField<uint64_t>>(internal_row_id));
            return std::make_shared<FieldTuple>(fields, nullptr);
        }

        std::string _text(const std::string &value) {
            return gist_helpers::make_text_query(value);
        }

        std::filesystem::path _base_dir;
        std::shared_ptr<IndexExtentCache> _cache;
    };

    TEST_F(GistStrategyTest, SingleEntryRoundTrips) {
        auto writer = _create_writer(_base_dir / "single", "gist_trgm_ops");
        writer->insert(_row("hello", 1));

        auto leaves = writer->debug_dump_leaves();
        ASSERT_EQ(leaves.size(), 1u);
        EXPECT_EQ(leaves[0].entry.internal_row_id, 1u);

        auto root_offset = writer->finalize();
        auto reader = _open_reader(_base_dir / "single", root_offset, "gist_trgm_ops");

        auto all = reader->all_leaves();
        ASSERT_EQ(all.size(), 1u);
        EXPECT_EQ(all[0].internal_row_id, 1u);

        size_t leaves_visited = 0;
        auto matches = reader->search(_text("hello"), TRGM_STRATEGY_EQ, &leaves_visited);
        ASSERT_EQ(matches.size(), 1u);
        EXPECT_EQ(matches[0].internal_row_id, 1u);
        EXPECT_EQ(leaves_visited, 1u);

        EXPECT_TRUE(reader->search(_text("goodbye"), TRGM_STRATEGY_EQ).empty());
    }

    /** Forces real picksplit()/union() calls (via the pure-C++ reference opclass -- see this
        fixture's own class comment for why) via a small max_extent_size, and asserts the *shape*
        of the result (more than one leaf, every row present exactly once, no leaf left
        degenerately empty) -- not just "every inserted row is still findable somewhere"
        (phases/phase-10.md's "Behavioral notes": a row-presence-only assertion can't tell
        correct split from a degenerate one). */
    TEST_F(GistStrategyTest, SplitProducesNonDegenerateShape) {
        auto writer = _create_writer(_base_dir / "split", gist_helpers::TEST_RANGE_OPCLASS.data(),
                                     /*max_extent_size=*/120);

        std::vector<std::string> values = {
            "apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew",
            "kiwi", "lemon", "mango", "nectarine", "olive", "papaya", "quince", "raspberry"
        };
        for (size_t i = 0; i < values.size(); ++i) {
            writer->insert(_row(values[i], i + 1));
        }

        auto leaves = writer->debug_dump_leaves();
        ASSERT_EQ(leaves.size(), values.size()) << "every inserted row must survive the split(s)";

        std::set<uint64_t> distinct_leaf_nodes;
        std::set<uint64_t> seen_row_ids;
        for (auto &leaf : leaves) {
            distinct_leaf_nodes.insert(leaf.leaf_node_id);
            EXPECT_TRUE(seen_row_ids.insert(leaf.entry.internal_row_id).second)
                    << "row " << leaf.entry.internal_row_id << " duplicated across leaves";
        }

        // A single leaf holding all 16 rows comfortably exceeds the 120-byte max_extent_size, so
        // needs_split() must have fired more than once -- assert the tree actually grew past one
        // leaf, not just that rows are individually still findable.
        EXPECT_GT(distinct_leaf_nodes.size(), 2u);

        auto root_offset = writer->finalize();
        auto reader = _open_reader(_base_dir / "split", root_offset, gist_helpers::TEST_RANGE_OPCLASS.data());
        EXPECT_EQ(reader->all_leaves().size(), values.size());

        for (size_t i = 0; i < values.size(); ++i) {
            auto matches = reader->search(values[i], gist_helpers::TEST_RANGE_STRATEGY_EQ);
            ASSERT_EQ(matches.size(), 1u) << "exact lookup for " << values[i];
            EXPECT_EQ(matches[0].internal_row_id, i + 1);
        }
    }

    /** The central fan-out case (phases/phase-10.md Sec 4): a predicate that overlaps more than
        one leaf must visit (and return matches from) every qualifying leaf, not just a single
        best-penalty path. Forces several splits (many leaves, each with a disjoint lexicographic
        sub-range -- the reference opclass's picksplit() is a clean sort-and-bisect, so individual
        leaf ranges don't overlap each other), then issues a *wide* overlap-strategy range query
        that spans several leaves' sub-ranges -- exactly the real-world shape of GiST fan-out
        (an overlap/range predicate against multiple partitions), not reliant on any compression
        artifact the way pg_trgm's own exact-match recheck is. */
    TEST_F(GistStrategyTest, PredicateFanOutAcrossOverlappingLeaves) {
        auto writer = _create_writer(_base_dir / "fanout", gist_helpers::TEST_RANGE_OPCLASS.data(),
                                     /*max_extent_size=*/90);

        std::vector<std::string> values = {
            "apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew",
            "kiwi", "lemon", "mango", "nectarine", "olive", "papaya", "quince", "raspberry"
        };
        for (size_t i = 0; i < values.size(); ++i) {
            writer->insert(_row(values[i], i + 1));
        }

        auto leaves = writer->debug_dump_leaves();
        std::set<uint64_t> distinct_leaf_nodes;
        for (auto &leaf : leaves) {
            distinct_leaf_nodes.insert(leaf.leaf_node_id);
        }
        ASSERT_GT(distinct_leaf_nodes.size(), 2u) << "need several leaves for a meaningful fan-out query";

        auto root_offset = writer->finalize();
        auto reader = _open_reader(_base_dir / "fanout", root_offset, gist_helpers::TEST_RANGE_OPCLASS.data());

        // A wide range spanning most of the alphabet -- guaranteed to overlap several leaves'
        // disjoint sub-ranges regardless of exactly where picksplit() drew the boundaries.
        auto query = gist_helpers::make_test_range_query("cherry", "papaya");

        size_t leaves_visited = 0;
        auto matches = reader->search(query, gist_helpers::TEST_RANGE_STRATEGY_OVERLAPS, &leaves_visited);

        std::set<uint64_t> matched_ids;
        for (auto &m : matches) {
            matched_ids.insert(m.internal_row_id);
        }

        // Every value lexicographically within ["cherry", "papaya"] must come back -- cherry
        // (idx 2) through papaya (idx 13) inclusive.
        for (size_t i = 2; i <= 13; ++i) {
            EXPECT_TRUE(matched_ids.contains(i + 1)) << "expected " << values[i] << " (row " << i + 1 << ") in range";
        }
        // Values clearly outside the range must not come back.
        EXPECT_FALSE(matched_ids.contains(1)) << "apple is outside [cherry, papaya]";
        EXPECT_FALSE(matched_ids.contains(16)) << "raspberry is outside [cherry, papaya]";

        ASSERT_GT(leaves_visited, 1u)
                << "a range spanning most of the alphabet must visit more than one leaf -- "
                   "otherwise this test isn't actually exercising fan-out";
    }

    /** architecture.md Sec 5.5, generalized to GiST: leaf entries carry internal_row_id, never a
        physical (extent_id, row_id) location -- confirmed directly by construction (GistEntry has
        no location field at all) rather than merely asserted in a comment. A data-page flush
        relocating physical rows is exactly what the look-aside index exists to absorb instead of
        the GiST tree itself; this test only needs to show the GiST leaf's own content is
        location-free, which it is by the type GistEntry::internal_row_id is the only identity a
        leaf stores. */
    TEST_F(GistStrategyTest, LeafEntriesReferenceRowsByInternalRowIdOnly) {
        auto writer = _create_writer(_base_dir / "lookaside", "gist_trgm_ops");
        writer->insert(_row("stable-key", 42));

        auto leaves = writer->debug_dump_leaves();
        ASSERT_EQ(leaves.size(), 1u);
        EXPECT_EQ(leaves[0].entry.internal_row_id, 42u);
        // GistEntry (gist_helpers.hh) has exactly `keys` (the compressed indexed value) and
        // `internal_row_id`/`child_id` -- no extent_id/row_id/physical-location field exists on
        // the type at all, so there is nothing here for a data-page flush to ever need to update.
    }
}
