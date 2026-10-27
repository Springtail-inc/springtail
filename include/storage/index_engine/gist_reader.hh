#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/index_engine/extent_source.hh>
#include <storage/index_engine/gist_read_strategy.hh>
#include <storage/index_engine/segment_set.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * GistReader: the Reader-side GiST engine (architecture.md "btree" redesign Sec 5.1/5.3,
     * phase 10). Structurally separate from `IndexReader<Strategy>` for the same reason
     * `GistWriter` is separate from `IndexWriter<Strategy>` -- see gist_read_strategy.hh's header
     * comment: a `consistent()`-driven predicate search can fan out into multiple children at one
     * branch, which `IndexReader<Strategy>::Iterator`'s single-frame-stack, single-path design
     * cannot represent.
     *
     * Immutable except for set_root_and_xid() (mirrors IndexReader's identical rebase hook).
     * Reuses only the mechanical primitives IndexReader also reuses: ExtentSource (ordinary
     * extent-content fetch, decompression/caching all handled underneath, untouched by this
     * class) and SegmentSet (real-id decoding).
     */
    class GistReader {
    public:
        GistReader(uint64_t database_id,
                  const std::filesystem::path &file,
                  uint64_t xid,
                  uint64_t root_offset,
                  ExtentSourcePtr source,
                  GistReadStrategy strategy);

        /**
         * GIST_CONSISTENT-driven predicate search on column 0 (single-column scope, matching
         * this phase's own single-column test opclass -- see gist_helpers.hh's picksplit comment
         * for the same multi-column scope note). Recurses into *every* branch child whose entry
         * is consistent() with (query_bytes, strategy_num) -- true fan-out, not a single best
         * child -- and returns every matching leaf entry's internal_row_id, materialized rather
         * than lazily iterated (see this class's own header comment for why). Query-time qual
         * translation from a real Postgres predicate into (query_bytes, strategy_num) is phase
         * 12's job; this phase's own tests build query_bytes directly via
         * gist_helpers::make_text_query().
         */
        std::vector<gist_helpers::GistEntry> search(const std::string &query_bytes, uint16_t strategy_num,
                                                    size_t *branches_visited = nullptr) const;

        /** Every leaf entry in the tree, unfiltered -- this phase's look-aside-indirection test
            (and any other test needing to see every row without a predicate) uses this instead
            of search(), the same way IndexReader::begin()..end() is a plain unfiltered scan. */
        std::vector<gist_helpers::GistEntry> all_leaves() const;

        bool empty() const;

        uint64_t get_xid() const {
            return _xid;
        }

        uint64_t get_root_offset() const {
            return _root_offset;
        }

        std::filesystem::path get_file_path() const {
            return _file;
        }

        /** Mirrors IndexReader::set_root_and_xid() exactly -- see its comment. */
        void set_root_and_xid(uint64_t new_xid, uint64_t new_root_offset) {
            _xid = new_xid;
            _root_offset = new_root_offset;
        }

    private:
        std::shared_ptr<Extent> _get_extent(uint64_t extent_id) const;

        void _search_recursive(uint64_t extent_id, const std::string &query_bytes, uint16_t strategy_num,
                               std::vector<gist_helpers::GistEntry> &out, size_t *leaves_visited) const;
        void _collect_all(uint64_t extent_id, std::vector<gist_helpers::GistEntry> &out) const;

        uint64_t _database_id;
        std::filesystem::path _file;
        GistReadStrategy _strategy;
        ExtentSourcePtr _source;

        uint64_t _xid;
        uint64_t _root_offset;

        ExtentSchemaPtr _leaf_schema;
        ExtentSchemaPtr _branch_schema;
        FieldPtr _branch_child_f;
    };

    using GistReaderPtr = std::shared_ptr<GistReader>;
}
