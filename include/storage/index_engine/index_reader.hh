#pragma once

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

#include <common/constants.hh>
#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/index_engine/extent_source.hh>
#include <storage/index_engine/read_strategy.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * IndexReader<Strategy>: the engine half of the "btree" redesign's read side (architecture.md
     * Sec 5.1, 5.3), replacing BTree. Public surface mirrors BTree's today (begin(), end(),
     * lower_bound(), upper_bound(), inverse_upper_bound(), inverse_lower_bound(), find(),
     * get_schema(), empty(), get_xid(), get_file_path(), get_root_offset(), set_root_and_xid())
     * closely enough that Table::Iterator's trackers need only mechanical changes in phase 3; the
     * constructor differs because it must additionally take the Strategy and an ExtentSource,
     * mirroring IndexWriter's constructor shape rather than BTree's, and drops `max_extent_size`
     * for the same reason IndexWriter did -- the engine itself never uses that value (see
     * storage_cache_extent_source.hh for where it's still needed, and why).
     *
     * Immutable except for set_root_and_xid() (architecture.md Sec 5.3: a Reader is pinned to a
     * specific committed xid/root; set_root_and_xid() is the cheap in-place rebase phase 3's
     * Table::advance_to_xid needs, mirroring BTree::set_root_and_xid() exactly). No internal
     * locking of its own -- every call reads already-sealed, immutable extents through the
     * ExtentSource seam, which owns whatever thread-safety it needs (today, StorageCache's own).
     *
     * Templated on Strategy (not type-erased), matching phase 1's IndexWriter<Strategy> decision
     * for the same reason, arguably more so here -- see read_strategy.hh's header comment for the
     * full reasoning and the deliberate contrast with ExtentSource, which *is* type-erased.
     */
    template <typename Strategy>
    requires ReadStrategy<Strategy>
    class IndexReader {
    public:
        /**
         * A bidirectional iterator over the tree's leaf rows, in key order. Matches BTree::
         * Iterator's contract: dereferences to `const Extent::Row&`, operator==/!= compare by
         * "same extent instance at the same row" (not extent *content* -- see the class comment
         * on Frame below), copyable, default-constructible.
         *
         * Design choice (one of this phase's two open questions): a frame-stack
         * (std::vector<Frame>), not an explicit parent-pointer chain the way BTree::Node has one.
         * IndexWriter::Iterator (phase 1) already established a forward-only frame-stack for its
         * own tests; this extends the same shape to support operator--() rather than switching to
         * BTree::Node's parent-shared_ptr style, since a frame stack already holds everything
         * operator--() needs (every ancestor's extent and current row position) without a second
         * representation of the same path. Copying an Iterator copies the vector (each element a
         * cheap shared_ptr bump plus a lightweight Extent::Iterator), which is no more expensive
         * than copying BTree::Node's single shared_ptr parent-chain head.
         */
        class Iterator {
            friend IndexReader;

        private:
            /**
             * One level of the root-to-leaf (or root-to-current-position) path. Equality compares
             * `extent` by shared_ptr identity, not content: two Iterators that read the same
             * logical extent through two separate ExtentSource::get() calls are only guaranteed to
             * compare equal if the underlying provider hands back the same object for the same
             * key while it's resident (true of StorageCacheExtentSource today, via StorageCache's
             * own extent-identity caching -- see storage_cache_extent_source.cc) -- the exact same
             * "same instance, not same content" caveat BTree::Node::operator== already carries
             * (comparing SafePagePtr identity, per the brief).
             */
            struct Frame {
                std::shared_ptr<Extent> extent;
                ExtentSchemaPtr schema;
                Extent::Iterator pos;

                bool operator==(const Frame &rhs) const {
                    return extent == rhs.extent && pos == rhs.pos;
                }
            };

            Iterator(const IndexReader *reader, std::vector<Frame> frames)
                : _reader(reader),
                  _frames(std::move(frames))
            { }

        public:
            using iterator_category = std::bidirectional_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;
            using reference         = const Extent::Row &;

            Iterator() = default;

            reference operator*() const { return *(_frames.back().pos); }
            pointer operator->() const { return &(*(_frames.back().pos)); }

            /** Moves to the next entry in the tree, in-order. */
            Iterator &operator++() {
                assert(!_frames.empty());

                ++_frames.back().pos;
                if (_frames.back().pos != _frames.back().extent->end()) {
                    return *this;
                }

                // leaf exhausted -- walk up looking for the next branch entry to follow
                _frames.pop_back();
                while (!_frames.empty()) {
                    ++_frames.back().pos;
                    if (_frames.back().pos != _frames.back().extent->end()) {
                        _reader->_descend_leftmost(_frames);
                        return *this;
                    }
                    _frames.pop_back();
                }

                // exhausted the entire tree -- now equals end()
                return *this;
            }

            Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

            /** Moves to the previous entry. If currently at end(), moves to the last entry in the
                tree. Decrementing begin() (the very first entry) walks every ancestor's position
                back to its own begin(), finds no earlier sibling anywhere, and lands back on
                end() -- mirroring BTree::Iterator::operator--()'s exact behavior, not an
                assertion failure. */
            Iterator &operator--() {
                if (_frames.empty()) {
                    assert(_reader != nullptr && _reader->_root_offset != constant::UNKNOWN_EXTENT);

                    auto root = _reader->_get_extent(_reader->_root_offset);
                    ExtentSchemaPtr schema = root->header().type.is_branch() ? _reader->_branch_schema
                                                                             : _reader->_leaf_schema;
                    _frames.push_back({ root, schema, root->last() });
                    _reader->_descend_rightmost(_frames);

                    return *this;
                }

                if (_frames.back().pos != _frames.back().extent->begin()) {
                    --_frames.back().pos;
                    return *this;
                }

                _frames.pop_back();
                while (!_frames.empty()) {
                    if (_frames.back().pos != _frames.back().extent->begin()) {
                        --_frames.back().pos;
                        _reader->_descend_rightmost(_frames);
                        return *this;
                    }
                    _frames.pop_back();
                }

                // went past begin() on the whole tree -- mirrors BTree::Iterator::operator--,
                // which returns end() (an assertion failure further up is the caller's bug, not
                // this method's -- decrementing begin() is documented as an error on both)
                return *this;
            }

            Iterator operator--(int) { Iterator tmp = *this; --(*this); return tmp; }

            friend bool operator==(const Iterator &a, const Iterator &b) {
                return a._frames.empty() == b._frames.empty() &&
                       (a._frames.empty() || a._frames.back() == b._frames.back());
            }

            friend bool operator!=(const Iterator &a, const Iterator &b) { return !(a == b); }

        private:
            const IndexReader *_reader = nullptr;
            std::vector<Frame> _frames;
        };

        IndexReader(uint64_t database_id,
                    const std::filesystem::path &file,
                    const std::vector<std::string> &keys,
                    ExtentSchemaPtr schema,
                    uint64_t xid,
                    uint64_t root_offset,
                    ExtentSourcePtr source,
                    Strategy strategy,
                    const ExtensionCallback &extension_callback = {});

        IndexReader() = delete;

        Iterator begin() const;

        Iterator end() const {
            return Iterator(this, {});
        }

        Iterator lower_bound(TuplePtr search_key, bool for_update = false) const;
        Iterator upper_bound(TuplePtr search_key, bool for_update = false) const;
        Iterator inverse_upper_bound(TuplePtr search_key) const;
        Iterator inverse_lower_bound(TuplePtr search_key) const;
        Iterator find(TuplePtr search_key) const;

        ExtentSchemaPtr get_schema() const {
            return _leaf_schema;
        }

        bool empty() const;

        uint64_t get_xid() const {
            return _xid;
        }

        std::filesystem::path get_file_path() const {
            return _file;
        }

        uint64_t get_root_offset() const {
            return _root_offset;
        }

        /** Cheap in-place rebase to a new committed xid/root -- phase 3's Table::advance_to_xid
            needs this to keep working without reconstructing the Reader (schema/strategy/source
            are unchanged), mirroring BTree::set_root_and_xid() exactly. */
        void set_root_and_xid(uint64_t new_xid, uint64_t new_root_offset) {
            _xid = new_xid;
            _root_offset = new_root_offset;
        }

    private:
        std::shared_ptr<Extent> _get_extent(uint64_t extent_id) const {
            return _source->get(_database_id, _file, extent_id, _xid);
        }

        ExtentSchemaPtr _schema_for(const std::shared_ptr<Extent> &extent) const {
            return extent->header().type.is_branch() ? _branch_schema : _leaf_schema;
        }

        /** Descend from `frames.back()` leftmost down to a leaf, appending frames along the way
            (used by begin() and by operator++() after moving to a new branch entry). */
        void _descend_leftmost(std::vector<typename Iterator::Frame> &frames) const;

        /** Descend from `frames.back()` rightmost (via last()) down to a leaf, appending frames
            along the way (used by operator--() after moving to a new branch entry, including the
            end()-to-last-entry case). */
        void _descend_rightmost(std::vector<typename Iterator::Frame> &frames) const;

        uint64_t _database_id;
        std::filesystem::path _file;
        std::vector<std::string> _sort_keys;

        ExtentSourcePtr _source;
        Strategy _strategy;

        uint64_t _xid;
        uint64_t _root_offset;

        ExtentSchemaPtr _leaf_schema;
        FieldArrayPtr _leaf_keys;

        ExtentSchemaPtr _branch_schema;
        FieldPtr _branch_child_f;

        ExtensionCallback _extension_callback;
    };
}
