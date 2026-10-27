#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <unordered_set>
#include <vector>

#include <common/constants.hh>
#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/index_engine/gist_write_strategy.hh>
#include <storage/index_engine/index_extent_cache.hh>
#include <storage/index_engine/segment_set.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * GistWriter: the Writer-side GiST engine (architecture.md "btree" redesign Sec 5.1, phase 10).
     * Structurally separate from `IndexWriter<Strategy>` -- not a new `Strategy` type parameter of
     * it -- per gist_write_strategy.hh's header comment and phases/phase-10.md's Design notes:
     * `IndexWriter<Strategy>`'s own mutation/split bodies assume a totally-ordered, positionally-
     * split tree, which GiST isn't. Reuses only the mechanical, tree-shape-unaware primitives
     * `IndexWriter<Strategy>` also reuses: `IndexExtentCache` (allocate/flush), `SegmentSet`
     * (real-id packing, segment rollover), and the on-disk extent format.
     *
     * Single-writer, no internal locking, same contract as IndexWriter (architecture.md Sec 5.4) --
     * every mutating method here is exclusive-access-by-caller-contract, not internally
     * synchronized.
     *
     * Deliberately out of this phase's scope (architecture.md Sec 7.1, this phase's own "Explicitly
     * out of scope"): background space-reclaim compaction and structural-reclaim merge. GistWriter
     * has no bulk_copy_for_compaction()/apply_compaction_batch()/try_merge_underfull() counterparts
     * at all -- a GiST tree's segments are only ever reclaimed by the whole-tree TRUNCATE a DROP
     * INDEX already performs, exactly like a brand-new (not yet exercised by compaction) btree tree
     * would be before phase 7 existed.
     */
    class GistWriter {
    private:
        struct Node;
        using NodePtr = std::shared_ptr<Node>;

        struct Node {
            uint64_t id = constant::UNKNOWN_EXTENT;
            std::vector<uint64_t> superseded_offsets;
            bool is_branch = false;
            std::shared_ptr<Extent> extent;
            bool dirty = false;
            std::vector<NodePtr> children;
        };

        /** Mirrors IndexWriter::ReentrancyGuard exactly -- see its comment. */
        class ReentrancyGuard {
        public:
            explicit ReentrancyGuard(bool &flag) : _flag(flag) {
#ifndef NDEBUG
                CHECK(!_flag) << "GistWriter: reentrant or concurrent call detected on a "
                                 "single-writer instance";
                _flag = true;
#endif
            }
            ~ReentrancyGuard() {
#ifndef NDEBUG
                _flag = false;
#endif
            }
            ReentrancyGuard(const ReentrancyGuard &) = delete;
            ReentrancyGuard &operator=(const ReentrancyGuard &) = delete;
        private:
            bool &_flag;
        };

    public:
        GistWriter(uint64_t database_id,
                  const std::filesystem::path &file,
                  uint64_t xid,
                  GistWriteStrategy strategy,
                  std::shared_ptr<IndexExtentCache> cache);

        GistWriter() = delete;

        void init_empty();
        void init(uint64_t root_offset);

        /** `value`'s fields 0..num_columns()-1 are the indexed column(s)' raw (pre-compression)
            values; field num_columns() is the row's internal_row_id (architecture.md Sec 5.5 --
            never a physical location). Same calling convention IndexWriter::insert() uses for a
            secondary btree index, so Indexer::_build()'s existing scan-and-insert loop works
            against either engine unchanged (see indexer.hh's AnyIndexRoot). */
        void insert(TuplePtr value);

        /** Removes the leaf entry matching `value`'s internal_row_id (field num_columns()) --
            not a key lookup, since GiST leaf entries have no total order to search by. A full
            recursive tree visit (every branch child, not consistent()-pruned) -- correct, not
            optimized; see gist_reader.hh's search() for the consistent()-pruned equivalent used
            on the read side, where pruning doesn't risk missing a result since fan-out always
            visits every plausibly-matching child. */
        void remove(TuplePtr value);

        void truncate();

        uint64_t finalize(bool fs_sync = true);

        bool empty() {
            return _root->extent->empty();
        }

        void sync();

        std::filesystem::path get_file_path() const {
            return _file;
        }

        std::vector<std::filesystem::path> get_segment_paths() const {
            return _segments.all_paths();
        }

        /** Every leaf row currently in the tree, for tests that need to pin down actual tree
            shape/placement (phases/phase-10.md's "Behavioral notes": a row-presence-only
            assertion can't tell correct descent from a degenerate split). Not a production read
            path -- query-time search is GistReader's job (phase 12 wires it to real FDW quals). */
        struct LeafRow {
            uint64_t leaf_node_id;
            gist_helpers::GistEntry entry;
        };
        std::vector<LeafRow> debug_dump_leaves();

    private:
        void _init_empty_impl();
        void _sync_impl();

        NodePtr _get_child(NodePtr parent, uint64_t id);
        std::vector<NodePtr> _choose_subtree_path(const gist_helpers::GistEntry &incoming);
        void _touch(std::vector<NodePtr> &path, size_t i);

        /** Every entry currently in `node` (leaf entries if `node` is a leaf, branch/union
            entries -- each carrying its child_id -- if `node` is a branch). */
        std::vector<gist_helpers::GistEntry> _entries_of(NodePtr node);

        /** Mechanical inverse-ish of IndexWriter::_split_node(): picksplit()'s arbitrary
            partition, realized as two brand-new extents built by appending each side's entries
            directly (never a positional copy_range()), with `node->children` partitioned by
            which side each child's branch row landed on -- see write_strategy.hh's own framing
            of this exact problem and phases/phase-10.md's Design notes for why this (rebuild from
            picksplit's partition) was chosen over reordering `node->extent` in place to fake a
            positional split. */
        std::pair<NodePtr, NodePtr> _split_node(NodePtr node);

        /** Appends a branch row for `child` into `parent`, with `child`'s current union key --
            used both by ordinary split propagation and by the brand-new-root case. */
        void _append_branch_row(NodePtr parent, NodePtr child);

        /** Refreshes every ancestor's recorded row for its child, bottom-up, to the child's
            *current* union -- GiST's analogue of IndexWriter::_propagate_key_increase(), but
            recomputing from scratch via GIST_UNION rather than a cheap "is the new key bigger"
            comparison, since GiST keys have no total order to compare by. */
        void _adjust_ancestor_keys(std::vector<NodePtr> &path);

        /** Bottom-up split cascade, mirroring IndexWriter::_propagate_splits()'s loop shape. */
        void _propagate_splits(std::vector<NodePtr> &path);

        /** Mirrors IndexWriter::_remove_empty_leaf() -- cascades an emptied leaf's removal
            upward, demoting the root to an empty leaf if the whole tree empties, and refreshes
            the first surviving ancestor's key once the cascade stops. */
        void _remove_empty_leaf(std::vector<NodePtr> &path);

        void _flush_node(NodePtr node, std::unordered_set<uint64_t> &reported_expired);

        /** Full recursive tree visit (every branch child, no consistent()-based pruning -- GiST
            leaf/branch entries have no total order to prune a removal search by without risking
            a false negative) looking for the leaf entry matching `target_row_id`. Builds `path`
            (root..current) as it recurses so a match can _touch()/remove it in place. */
        bool _remove_from_subtree(NodePtr node, uint64_t target_row_id, std::vector<NodePtr> &path);

        void _collect_leaves(NodePtr node, std::vector<LeafRow> &out);

        uint64_t _database_id;
        std::filesystem::path _file;
        GistWriteStrategy _strategy;
        std::shared_ptr<IndexExtentCache> _cache;
        SegmentSet _segments;

        /** Same reserved-range hazard class as IndexWriter::TEMP_ID_BASE -- a not-yet-flushed
            node needs a stable in-memory identity disjoint from real, persisted extent ids. Each
            GistWriter instance owns a tree-local counter, same as IndexWriter's; no cross-tree
            collision risk since ids are only ever compared within one tree's own id space. */
        static constexpr uint64_t TEMP_ID_BASE = uint64_t(1) << 63;

        NodePtr _root;
        uint64_t _xid;
        uint64_t _next_temp_id = TEMP_ID_BASE;
        bool _in_call = false;

        ExtentSchemaPtr _leaf_schema;
        ExtentSchemaPtr _branch_schema;
        MutableFieldPtr _branch_child_f;
        FieldPtr _leaf_row_id_f;
    };

    using GistWriterPtr = std::shared_ptr<GistWriter>;
}
