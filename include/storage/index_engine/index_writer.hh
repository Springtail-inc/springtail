#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <unordered_set>
#include <vector>

#include <common/constants.hh>
#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/index_engine/index_extent_cache.hh>
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
                node is flushed. */
            uint64_t id = constant::UNKNOWN_EXTENT;

            /** The on-disk extent_id this node's content supersedes once flushed (becomes
                ExtentHeader::prev_offset); UNKNOWN_EXTENT if this node (or its entire chain of
                in-memory splits) never corresponded to on-disk content. */
            uint64_t prev_offset = constant::UNKNOWN_EXTENT;

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
        ValueTuplePtr _make_value_key(Extent &extent, MutableFieldArrayPtr key_fields);

        /** Split `node` in two via the strategy's split_point(), returning the replacement pair;
            `node` itself is discarded (never flushed, never referenced again). If `node` is a
            branch, also partitions its `children` between the two results by which half's rows
            each child's row ended up in -- otherwise a child that was itself already split
            earlier this generation (and so is only reachable via `node->children`, not yet
            patched into a real extent_id) would be silently dropped from the flush walk. */
        std::pair<NodePtr, NodePtr> _split_node(NodePtr node);

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

        /** Descend from `frames.back()` leftmost down to a leaf, appending frames along the way
            (used by Iterator::operator++() after moving to a new branch entry, and by begin()). */
        void _descend_leftmost(std::vector<typename Iterator::Frame> &frames);

        uint64_t _database_id;
        std::filesystem::path _file;
        std::vector<std::string> _sort_keys;

        std::shared_ptr<IndexExtentCache> _cache;
        Strategy _strategy;

        /**
         * Not-yet-flushed nodes (split results, a brand-new root, a brand-new empty tree) need a
         * stable identity before they have a real on-disk extent_id, used both as the value
         * written into a parent's branch row and as the key _get_child() searches a parent's
         * `children` by. Real extent_ids are plain append-only file byte offsets (IOFile::
         * reserve_offset()) and so start at 0 and grow with file size -- small values for a long
         * time. A temp-id counter that also started at 0 would collide with real ids the moment a
         * second (or later) generation's IndexWriter reopens an existing tree and splits again:
         * _get_child()'s and _flush_node()'s lookups match an id by *value*, with no other way to
         * tell "this is a temp id" from "this is a real, untouched sibling's id" apart (round-1
         * review finding 1). Reserving the top half of the uint64_t space for temp ids makes that
         * collision structurally impossible -- no real file will ever reach a 2^63-byte offset.
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
