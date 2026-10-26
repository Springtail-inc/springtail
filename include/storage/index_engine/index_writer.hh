#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <unordered_set>
#include <vector>

#include <common/constants.hh>
#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/index_engine/index_extent_cache.hh>
#include <storage/index_engine/segment_set.hh>
#include <storage/index_engine/write_strategy.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * IndexWriter<Strategy>: the engine half of the "btree" redesign (architecture.md Sec 5.1,
     * 5.4), replacing MutableBTree. Public surface mirrors MutableBTree's today (init_empty(),
     * init(), insert(), remove(), truncate(), finalize(), get_key_fields(), empty(), sync(),
     * get_file_path(), and the begin()/last()/lower_bound()/end() iterators) so that phase 3/4's
     * call-site swap is mechanical; the constructor differs because it must additionally take the
     * Strategy and the shared IndexExtentCache, neither of which MutableBTree's constructor ever
     * needed, and drops `max_extent_size` -- the engine itself never uses that value (only the
     * btree-specific split threshold inside BtreeWriteStrategy does), so keeping it here would
     * leak a btree-specific knob into the generic engine for no reason; callers configure it on
     * the Strategy they construct instead.
     *
     * Single-writer, no internal locking (architecture.md Sec 5.4): every mutating method asserts
     * (in debug builds) that it isn't being re-entered, but the class has no mutex anywhere in its
     * own tree-mutation code -- exclusive access for the duration of any call is an external
     * contract the owning component (future phases: Table/MutableTable/Server) must uphold, not
     * something this class coordinates internally. Nothing here adds a lock or a "wait in case a
     * reader might still be looking at this" step -- architecture.md Sec 4's reachability
     * invariant already rules that out.
     *
     * Since phase 7 (architecture.md Sec 7.1), also exposes background space-reclaim
     * compaction's two phases as narrow primitives: bulk_copy_for_compaction() (phase 1) is the
     * one deliberate exception to "every public method needs this tree's single-writer
     * exclusivity" -- it touches only this tree's (already thread-safe) extent cache and
     * construction-time-fixed schema state, never _root/_segments/_in_call, so it's safe to call
     * without that exclusivity; apply_compaction_batch() (phase 2) is an ordinary mutating call
     * like insert()/remove() and needs it like any other.
     */
    template <typename Strategy>
    requires WriteStrategy<Strategy>
    class IndexWriter {
    private:
        struct Node;
        using NodePtr = std::shared_ptr<Node>;

        /**
         * One node of the in-memory, copy-on-write tree being built up this generation. Mirrors
         * MutableBTree::Page, minus every mutex/future/flush-waiter -- there is nothing to
         * coordinate with, since a Writer is single-threaded by contract.
         */
        struct Node {
            /** Stable identity used as this node's entry in its parent's branch row, and as the
                key its parent's `children` list is searched by during descent: the on-disk
                extent_id this node was read from, or (if created fresh, e.g. a split result) a
                synthetic id assigned from IndexWriter::_next_temp_id's reserved range (see
                TEMP_ID_BASE -- this must never be allowed to collide with a real, on-disk
                extent_id, round-1 review). Replaced with the real, on-disk extent_id once this
                node is flushed. Since phase 6, a real id is a SegmentSet::pack()'d
                (segment_id, in-segment offset) pair, not a plain tree-wide byte offset -- see
                TEMP_ID_BASE's comment and segment_set.hh. */
            uint64_t id = constant::UNKNOWN_EXTENT;

            /** The on-disk extent_id this node's content supersedes once flushed (becomes
                ExtentHeader::prev_offset); UNKNOWN_EXTENT if this node (or its entire chain of
                in-memory splits) never corresponded to on-disk content. */
            uint64_t prev_offset = constant::UNKNOWN_EXTENT;

            /** A *second* superseded predecessor, used only by background structural-reclaim
                merge (phase 8): a merge result combines two existing siblings' content, so it
                has two predecessors to report superseded once it flushes, not one -- unlike
                prev_offset, this has no on-disk header counterpart (nothing reads it back; see
                _flush_node()'s comment on why that's fine), it exists purely so _flush_node()'s
                existing Vacuumer-reporting step can report both without a second Node-level
                field class of its own. UNKNOWN_EXTENT for every node except a merge result. */
            uint64_t second_prev_offset = constant::UNKNOWN_EXTENT;

            ExtentType type;

            /** The key recorded for this node in its parent's branch row; unset while this node
                is the tree's current root. */
            ValueTuplePtr prev_key;

            std::shared_ptr<Extent> extent;
            ExtentSchemaPtr schema;

            bool dirty = false;

            /** Materialized (touched-this-generation) descendants, used only to order finalize()'s
                child-before-parent flush walk -- never consulted for anything else, since any
                node not in here is either untouched-and-clean (safe to leave on disk as-is) or
                has already been flushed. */
            std::vector<NodePtr> children;
        };

        /** Per-call reentrancy guard (the checklist's debug-build-only reentrancy guard):
            IndexWriter has no mutex, so this is the only thing standing between "single writer by
            contract" and silent corruption if that contract is ever violated. */
        class ReentrancyGuard {
        public:
            explicit ReentrancyGuard(bool &flag)
                : _flag(flag)
            {
#ifndef NDEBUG
                CHECK(!_flag) << "IndexWriter: reentrant or concurrent call detected on a "
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
        IndexWriter(uint64_t database_id,
                   const std::filesystem::path &file,
                   const std::vector<std::string> &keys,
                   ExtentSchemaPtr schema,
                   uint64_t xid,
                   std::shared_ptr<IndexExtentCache> cache,
                   Strategy strategy,
                   const ExtensionCallback &extension_callback = {});

        IndexWriter() = delete;

        /** Initialize an empty tree. */
        void init_empty();

        /** Initialize a tree with a given (already on-disk) root offset. */
        void init(uint64_t root_offset);

        /** Insert a new entry into the tree. */
        void insert(TuplePtr value);

        /** Remove the entry matching `value`'s key from the tree. */
        void remove(TuplePtr value);

        /** Truncate the tree, discarding all in-memory (non-finalized) mutations. */
        void truncate();

        /** Flush all dirty extents to disk and return the new root's extent_id. */
        uint64_t finalize(bool fs_sync = true);

        FieldArrayPtr get_key_fields() {
            return std::make_shared<FieldArray>(_leaf_keys->begin(), _leaf_keys->end());
        }

        bool empty() {
            return _root->extent->empty();
        }

        /** Write the index to disk (fsync). */
        void sync();

        std::filesystem::path get_file_path() const {
            return _file;
        }

        /**
         * One still-reachable extent found by bulk_copy_for_compaction()'s segment scan
         * (architecture.md "btree" redesign Sec 7.1, phase 1): its real id as of the scan, and
         * the max key recorded in its own content (used by apply_compaction_batch()'s phase 2
         * to re-locate wherever it's currently referenced from, the same way an ordinary
         * mutation's key-based descent would -- see the .cc for why this routes correctly
         * through any ancestor CoW-rewrites/splits that happened in between, as long as the
         * extent's own row range -- immutable once sealed -- hasn't changed).
         */
        struct CompactionItem {
            uint64_t old_id;
            ValueTuplePtr search_key;
        };

        /**
         * Background space-reclaim compaction, phase 1 (architecture.md Sec 7.1): scan
         * `segment_file` (one of this tree's own, already-sealed segments that the caller's
         * Vacuumer-side bookkeeping judged partially dead) for every extent not already
         * reported dead, and copy each one's content verbatim into a fresh, dense
         * `scratch_file` (freshly created here; never a real segment of this tree -- phase 2
         * reads it back and it is deleted afterward regardless of outcome). Stops after
         * `item_budget` copies (phase-7.md's Throttling task: small batches, never one large
         * pass).
         *
         * Touches only this tree's extent cache (already internally thread-safe across trees)
         * and schema/key-field state set once at construction and never reassigned after --
         * never `_root`, `_segments`, or `_in_call`. Unlike every other public method here,
         * this is therefore safe to call without this tree's single-writer exclusivity
         * (architecture.md Sec 7.1: "fully concurrent... needs zero coordination") and does not
         * take the reentrancy guard for that reason. In this implementation it is in practice
         * still called from the same thread that owns this tree's ordinary mutation turn (the
         * Decisions log's resolution for where compaction work is claimed), but nothing about
         * this method itself requires that.
         *
         * @param segment_file One of this tree's own sealed segments (get_segment_paths()).
         * @param scratch_file Destination for the dense copy; removed and recreated here.
         * @param is_dead Returns true if a given real id is already known superseded -- see
         *        Vacuumer::compaction_dead_offsets(), decoded against `segment_file`'s own
         *        segment_id by the caller.
         * @param item_budget Max extents to copy this call.
         * @return One CompactionItem per extent copied, in the order copied (matches
         *         `scratch_file`'s own append order -- apply_compaction_batch() relies on this).
         */
        std::vector<CompactionItem> bulk_copy_for_compaction(
                const std::filesystem::path &segment_file,
                const std::filesystem::path &scratch_file,
                const std::function<bool(uint64_t)> &is_dead,
                size_t item_budget) const;

        /**
         * Background space-reclaim compaction, phase 2 (architecture.md Sec 7.1): re-validate
         * and relocate each of `items` (bulk_copy_for_compaction()'s output, phase 1) from
         * `scratch_file` into this tree's *current* live structure, through the ordinary
         * single-writer mutation path -- same exclusivity contract as insert()/remove(), the
         * caller must already hold it (a Committer worker's claimed table_id turn, or a
         * system-table catalog-queue callback; see Decisions log entry 2). An item whose old_id
         * is no longer referenced anywhere in the live tree (the parent moved on, or the
         * pointer is simply gone) is skipped, not an error -- the phase-1 copy was wasted but
         * harmless work. Does not call finalize() -- whatever this relocates rides along with
         * the caller's own already-scheduled finalize() for this generation, exactly like any
         * other mutation this generation. Always removes `scratch_file` before returning,
         * regardless of how many items actually applied.
         * @return How many of `items` were actually relocated (<= items.size()).
         */
        size_t apply_compaction_batch(const std::filesystem::path &scratch_file,
                                      const std::vector<CompactionItem> &items);

        /**
         * Background structural-reclaim (merge), phase 8 (architecture.md Sec 7.1): resolve one
         * underfull flag (Vacuumer::UnderfullFlag -- same shape as CompactionItem, re-used here
         * as `old_id`/`search_key` directly rather than a dedicated type, since the two fields
         * mean exactly the same thing in both cases) against this tree's *current* live
         * structure. Unlike compaction, this is a single phase, not a prepare/apply split
         * (phase-8.md OQ2): the background lookup here -- re-find the node, ask its parent for
         * a sibling -- is itself small and local (architecture.md's own framing), so there is
         * no slow, lock-free-safe scan to run outside this tree's single-writer exclusivity the
         * way compaction's segment scan needed. Needs that exclusivity throughout, exactly like
         * insert()/remove().
         *
         * Re-validates before doing anything: descends from `_root` via `search_key` (same
         * mechanism _relocate() uses) to find the branch row whose child pointer is `old_id`.
         * Not found -- the node moved on or is gone -- is a silent no-op, same as _relocate()'s
         * identical case. If found, re-checks occupancy against the node's *current* extent
         * (which may already include whatever this generation's own foreground mutation did to
         * it since the flag was raised) -- no longer underfull is also a silent no-op, not an
         * error (unlike apply_compaction_batch(), merge never needs an "already dirty this
         * generation" special case for the flagged node itself: it always reads the node's
         * live, current content to build the combined result, never blindly overwrites with a
         * stale precomputed copy the way a compaction relocation does).
         *
         * If still underfull, checks both the immediately preceding and following sibling rows
         * in the parent (whichever exist) for one with enough room (WriteStrategy::can_merge())
         * -- not required to check both for correctness, just improves the odds of finding
         * room. If one qualifies, combines the two extents into one new node (the mechanical
         * inverse of _split_node(), including carrying over both halves' `children` the same
         * way _split_node()'s own partition step does -- see its comment), drops one child row
         * from the parent, and marks the parent (and every ancestor) dirty for this
         * generation's finalize() to flush -- exactly like an ordinary mutation. If no sibling
         * currently has room, that is a legitimate, stable, benign outcome (architecture.md Sec
         * 7.1), not a failure: returns false with no mutation performed. Either way, retiring
         * the flag itself is the caller's responsibility (Vacuumer::clear_underfull_flag()) --
         * this method only ever reports whether a merge actually happened.
         *
         * @return True if a merge was performed; false if the flag was stale or no sibling had
         *         room (the caller should still retire the flag either way).
         */
        bool try_merge_underfull(uint64_t old_id, ValueTuplePtr search_key);

        /** Every segment file this tree could have ever written to (0..current, inclusive) --
            phase 6's segment-set abstraction means a tree's content can now span more than one
            physical file; callers that need to fsync or enumerate every on-disk file backing
            this tree (MutableTable::get_table_files()'s durability path) must use this instead
            of get_file_path() alone. */
        std::vector<std::filesystem::path> get_segment_paths() const {
            return _segments.all_paths();
        }

        /**
         * Test-only hook: run `fn` under the exact same reentrancy guard every mutating method
         * above uses. Lets test_index_writer.cc assert the debug-build guard fires on a nested
         * (simulated reentrant) call without needing genuine, inherently racy concurrency to
         * reliably trigger it -- not for production use.
         */
        template <typename Fn>
        void debug_guarded_call(Fn &&fn) {
            ReentrancyGuard guard(_in_call);
            fn();
        }

        /** Forward iterator over leaf rows, in key order. Exists so this phase's tests can verify
            tree contents without a Reader (phase 2) -- a real read path is IndexReader's job. */
        class Iterator {
            friend IndexWriter;

        private:
            struct Frame {
                NodePtr node;
                Extent::Iterator pos;
            };

            Iterator(IndexWriter *writer, std::vector<Frame> frames)
                : _writer(writer),
                  _frames(std::move(frames))
            { }

        public:
            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;
            using reference         = const Extent::Row &;

            Iterator() = default;

            reference operator*() const {
                return *_frames.back().pos;
            }

            Iterator &operator++() {
                ++_frames.back().pos;
                if (_frames.back().pos != _frames.back().node->extent->end()) {
                    return *this;
                }

                // leaf exhausted -- walk up looking for the next branch entry to follow
                _frames.pop_back();
                while (!_frames.empty()) {
                    ++_frames.back().pos;
                    if (_frames.back().pos != _frames.back().node->extent->end()) {
                        _writer->_descend_leftmost(_frames);
                        return *this;
                    }
                    _frames.pop_back();
                }

                // exhausted the entire tree
                return *this;
            }

            bool operator==(const Iterator &rhs) const {
                return _frames.empty() == rhs._frames.empty() &&
                       (_frames.empty() || (_frames.back().node == rhs._frames.back().node &&
                                            _frames.back().pos == rhs._frames.back().pos));
            }

        private:
            IndexWriter *_writer = nullptr;
            std::vector<Frame> _frames;
        };

        Iterator begin();
        Iterator last();
        Iterator lower_bound(TuplePtr search_key, bool for_update);
        Iterator end() {
            return Iterator(this, {});
        }

    private:
        void _init_schemas(ExtentSchemaPtr schema,
                           const std::vector<std::string> &keys,
                           const ExtensionCallback &extension_callback);

        /** Shared logic behind the public init_empty() and truncate() -- a plain, non-guarded
            helper so truncate() can call it without tripping its own reentrancy guard. */
        void _init_empty_impl();

        /** Shared logic behind the public sync() and finalize(fs_sync=true). */
        void _sync_impl();

        /** Find (materializing from disk if necessary) the already-touched-or-clean child of
            `parent` whose branch row's child-pointer field is `id`. */
        NodePtr _get_child(NodePtr parent, uint64_t id);

        /** Build the root-to-leaf path for `key`, materializing nodes along the way. */
        std::vector<NodePtr> _find_leaf_path(TuplePtr key);

        /** Mark `path[i]` dirty, along with every not-yet-dirty ancestor up to and including the
            root (CoW means flushing path[i] gives it a new extent_id, which means every ancestor
            whose branch row points at it, directly or transitively, must also be rewritten --
            see the .cc). Clones each newly-dirtied node's extent into a private, exclusively-
            owned copy first (IndexExtentCache's clean reads are shared and must never be mutated
            in place) and registers it in its parent's `children` list. A no-op once `path[i]` is
            already dirty. */
        void _touch(std::vector<NodePtr> &path, size_t i);

        /** Generic (schema-driven, not strategy-specific -- see write_strategy.hh's framing)
            lower_bound over one extent's rows by sort key. */
        Extent::Iterator _lower_bound_rows(Extent &extent, ExtentSchemaPtr schema, TuplePtr key);

        /** Insert `value` (already laid out per `schema`'s column order) into `extent` in sorted
            position, asserting the key doesn't already exist -- used for both leaf rows and
            branch rows (a branch row is just a (keys..., child offset) tuple). */
        void _insert_row(std::shared_ptr<Extent> extent, ExtentSchemaPtr schema, TuplePtr value);

        /** Remove the row matching `key` from `extent`. */
        void _remove_row(std::shared_ptr<Extent> extent, ExtentSchemaPtr schema, TuplePtr key);

        /** Build a branch row (keys... + child offset) for `child` and insert it into `parent`. */
        void _insert_branch_row(NodePtr parent, NodePtr child);

        /** Build a stable ValueTuple key from the last row of `extent`, projected onto
            `key_fields` -- mirrors MutableBTree::Page::set_cache_page()'s repeated pattern. */
        ValueTuplePtr _make_value_key(Extent &extent, MutableFieldArrayPtr key_fields) const;

        /** Split `node` in two via the strategy's split_point(), returning the replacement pair;
            `node` itself is discarded (never flushed, never referenced again). If `node` is a
            branch, also partitions its `children` between the two results by which half's rows
            each child's row ended up in -- otherwise a child that was itself already split
            earlier this generation (and so is only reachable via `node->children`, not yet
            patched into a real extent_id) would be silently dropped from the flush walk. */
        std::pair<NodePtr, NodePtr> _split_node(NodePtr node);

        /** try_merge_underfull()'s mechanical worker, the inverse of _split_node(): combine
            `first` and `second` (the lower- and higher-keyed of an already-confirmed-mergeable
            sibling pair) into one new node, carrying over both halves' `children` the same way
            _split_node()'s own partition step does (phase-8.md OQ4), and update `parent`
            (`path.back()`) to drop both source rows and insert one for the merged result.
            Touches `path` (root-to-parent) up front via _touch(), exactly like insert()/
            remove() touch their own path before mutating anything. */
        void _merge_nodes(std::vector<NodePtr> &path, NodePtr parent, NodePtr first, NodePtr second);

        /** After inserting `new_key` into `path.back()`, walk ancestors upward refreshing each
            one's recorded routing key for its child along `path` wherever `new_key` exceeds what
            is currently recorded there -- see the .cc for why this is needed for routing
            correctness (not just split/remove bookkeeping). Stops as soon as one level is
            already >= new_key. */
        void _propagate_key_increase(std::vector<NodePtr> &path, TuplePtr new_key);

        /** After mutating path.back(), split it (and propagate the split upward through
            `path`, rebuilding the root if necessary) for as long as the strategy says the
            resulting node still needs splitting. */
        void _propagate_splits(std::vector<NodePtr> &path);

        /** After removing the last row from path.back(), cascade the resulting empty-node
            removal upward through `path`, demoting the root to an empty leaf if the whole tree
            becomes empty. */
        void _remove_empty_leaf(std::vector<NodePtr> &path);

        /** Recursive, post-order (child-before-parent) flush of every dirty node reachable from
            `node`, patching each parent's branch row with its child's real, now-known extent_id
            as each child finishes flushing. Also notifies the Vacuumer that each flushed node's
            prev_offset is now superseded, de-duplicated against `reported_expired` (scoped to one
            finalize() call) since sibling split results can share the same prev_offset -- see the
            .cc for the full reasoning. */
        void _flush_node(NodePtr node, std::unordered_set<uint64_t> &reported_expired);

        /** _flush_node()'s per-predecessor Vacuumer-reporting step, factored out so a merge
            result (two predecessors: prev_offset and second_prev_offset) and an ordinary
            split/touch result (one: prev_offset) share the same exact-size-reporting and
            reported_expired-dedup logic. A no-op if `prev_offset` is UNKNOWN_EXTENT or already
            reported this finalize() call. */
        void _report_superseded(uint64_t prev_offset, std::unordered_set<uint64_t> &reported_expired);

        /** apply_compaction_batch()'s per-item worker: if `old_id` is still referenced -- either
            as the tree's own root, or as some current node's branch-row child pointer, found by
            descending from the root via `search_key` exactly as an ordinary mutation would --
            adopt `dense_copy` as that node's content and mark it (and every ancestor up to the
            root) dirty for the next finalize() to flush, reporting `old_id` itself as superseded
            in the process (mirrors _flush_node()'s own prev_offset reporting, since nothing else
            ever flushes `old_id`'s own content again to do that for us). Returns false, doing
            nothing, if `old_id` isn't found -- see the .cc for why `search_key`-based descent
            still finds the right (possibly since-CoW-rewritten-or-split) current node, as long
            as `old_id`'s own row range -- immutable once sealed -- hasn't changed. Also returns
            false (rather than overwriting) if the node found is *already* dirty from this same
            generation's own foreground mutation -- a node's `id` field isn't rewritten until it
            actually flushes, so an already-touched node can still read back as `id == old_id`;
            blindly adopting `dense_copy` there would silently discard whatever that foreground
            mutation already did to it (caught concretely, not just reasoned about, while writing
            this method's own regression test). */
        bool _relocate(uint64_t old_id, TuplePtr search_key, std::shared_ptr<Extent> dense_copy);

        /** Descend from `frames.back()` leftmost down to a leaf, appending frames along the way
            (used by Iterator::operator++() after moving to a new branch entry, and by begin()). */
        void _descend_leftmost(std::vector<typename Iterator::Frame> &frames);

        uint64_t _database_id;
        std::filesystem::path _file;
        std::vector<std::string> _sort_keys;

        std::shared_ptr<IndexExtentCache> _cache;
        Strategy _strategy;

        /** Which segment is current, and when it seals and rolls over (phase 6) -- see
            segment_set.hh. Constructed fresh at TEMP_ID_BASE==segment 0 for a brand-new tree;
            init()'s resume_from() catches it up to whichever segment an already-on-disk root
            lives in (and past it, if that segment was already sealed by an earlier
            generation). */
        SegmentSet _segments;

        /**
         * Not-yet-flushed nodes (split results, a brand-new root, a brand-new empty tree) need a
         * stable identity before they have a real on-disk extent_id, used both as the value
         * written into a parent's branch row and as the key _get_child() searches a parent's
         * `children` by. Through phase 5, a real extent_id was a plain append-only file byte
         * offset (IOFile::reserve_offset()) and so started at 0 and grew with file size -- small
         * values for a long time. A temp-id counter that also started at 0 would collide with
         * real ids the moment a second (or later) generation's IndexWriter reopens an existing
         * tree and splits again: _get_child()'s and _flush_node()'s lookups match an id by
         * *value*, with no other way to tell "this is a temp id" from "this is a real, untouched
         * sibling's id" apart (round-1 review finding 1). Reserving the top half of the uint64_t
         * space for temp ids makes that collision structurally impossible.
         *
         * Since phase 6 (sealed, size-bounded segment files), a real id is no longer a plain
         * tree-wide byte offset -- it is SegmentSet::pack(segment_id, in-segment offset), which
         * fills exactly the remaining 63 bits below this reservation (segment_set.hh's
         * static_assert enforces this at compile time), so the same disjointness still holds by
         * construction: no packed real id can ever reach 2^63 regardless of how many segments a
         * tree accumulates over its lifetime or how large any one segment grows.
         */
        static constexpr uint64_t TEMP_ID_BASE = uint64_t(1) << 63;

        NodePtr _root;
        uint64_t _xid;
        bool _finalized = true;
        uint64_t _next_temp_id = TEMP_ID_BASE;
        bool _in_call = false;

        ExtensionCallback _extension_callback;

        ExtentSchemaPtr _leaf_schema;
        MutableFieldArrayPtr _leaf_fields;
        MutableFieldArrayPtr _leaf_keys;

        ExtentSchemaPtr _branch_schema;
        MutableFieldArrayPtr _branch_keys;
        MutableFieldPtr _branch_child_f;
    };
}
