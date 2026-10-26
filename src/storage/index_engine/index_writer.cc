#include <algorithm>
#include <unordered_set>

#include <absl/log/check.h>

#include <storage/index_engine/btree_write_strategy.hh>
#include <storage/index_engine/index_writer.hh>
#include <storage/io.hh>
#include <storage/io_mgr.hh>
#include <storage/vacuumer.hh>

namespace springtail {
    template <typename Strategy>
    requires WriteStrategy<Strategy>
    IndexWriter<Strategy>::IndexWriter(uint64_t database_id,
                                       const std::filesystem::path &file,
                                       const std::vector<std::string> &keys,
                                       ExtentSchemaPtr schema,
                                       uint64_t xid,
                                       std::shared_ptr<IndexExtentCache> cache,
                                       Strategy strategy,
                                       const ExtensionCallback &extension_callback)
        : _database_id(database_id),
          _file(file),
          _sort_keys(keys),
          _cache(std::move(cache)),
          _strategy(std::move(strategy)),
          _segments(file),
          _xid(xid),
          _extension_callback(extension_callback)
    {
        _init_schemas(schema, keys, extension_callback);
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_init_schemas(ExtentSchemaPtr schema,
                                        const std::vector<std::string> &keys,
                                        const ExtensionCallback &extension_callback)
    {
        _leaf_schema = schema;
        _leaf_fields = _leaf_schema->get_mutable_fields();
        _leaf_keys = _leaf_schema->get_mutable_fields(keys);

        SchemaColumn child(constant::BTREE_CHILD_FIELD, 0, SchemaType::UINT64, 0, false);
        _branch_schema = _leaf_schema->create_index_schema(keys, { child }, keys, extension_callback);

        _branch_keys = _branch_schema->get_mutable_fields(keys);
        _branch_child_f = _branch_schema->get_mutable_field(constant::BTREE_CHILD_FIELD);
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_init_empty_impl()
    {
        CHECK(_root == nullptr);

        ExtentHeader header(ExtentType(false, true), _xid, _leaf_schema->row_size(),
                            _leaf_schema->field_types());
        auto extent = _cache->allocate(header);

        auto root = std::make_shared<Node>();
        root->id = _next_temp_id++;
        root->prev_offset = constant::UNKNOWN_EXTENT;
        root->type = ExtentType(false, true);
        root->extent = extent;
        root->schema = _leaf_schema;

        _root = root;
        _finalized = false;
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::init_empty()
    {
        ReentrancyGuard guard(_in_call);
        _init_empty_impl();
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::init(uint64_t root_offset)
    {
        ReentrancyGuard guard(_in_call);
        CHECK(_root == nullptr);
        DCHECK_LT(root_offset, TEMP_ID_BASE) << "root_offset is a real on-disk extent id, which "
                                                "must never land in the reserved temp-id range";

        auto segment_path = SegmentSet::segment_path(_file, SegmentSet::segment_id_of(root_offset));
        auto extent = _cache->get(_database_id, segment_path, SegmentSet::offset_of(root_offset));
        ExtentSchemaPtr schema = extent->header().type.is_branch() ? _branch_schema : _leaf_schema;

        auto root = std::make_shared<Node>();
        root->id = root_offset;
        root->prev_offset = root_offset;
        root->type = extent->header().type;
        root->extent = extent;
        root->schema = schema;

        _root = root;
        _finalized = false;

        _segments.resume_from(root_offset);
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    typename IndexWriter<Strategy>::NodePtr
    IndexWriter<Strategy>::_get_child(NodePtr parent, uint64_t id)
    {
        for (auto &child : parent->children) {
            if (child->id == id) {
                return child;
            }
        }

        // Every id reaching this point was read straight out of an on-disk branch row (an
        // untouched child never registered in `parent->children`), so it must be a real
        // extent_id -- never a temp id, which would mean some split result's row was never
        // patched to its real id before being flushed (see TEMP_ID_BASE's comment).
        DCHECK_LT(id, TEMP_ID_BASE);

        auto segment_path = SegmentSet::segment_path(_file, SegmentSet::segment_id_of(id));
        auto extent = _cache->get(_database_id, segment_path, SegmentSet::offset_of(id));
        ExtentSchemaPtr schema = extent->header().type.is_branch() ? _branch_schema : _leaf_schema;
        MutableFieldArrayPtr key_fields = (schema == _branch_schema) ? _branch_keys : _leaf_keys;

        auto node = std::make_shared<Node>();
        node->id = id;
        node->prev_offset = id;
        node->type = extent->header().type;
        node->extent = extent;
        node->schema = schema;
        node->prev_key = _make_value_key(*extent, key_fields);

        return node;
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    ValueTuplePtr
    IndexWriter<Strategy>::_make_value_key(Extent &extent, MutableFieldArrayPtr key_fields)
    {
        auto row_i = extent.last();
        auto row = *row_i;
        auto key = std::make_shared<MutableTuple>(key_fields, &row);
        return std::make_shared<ValueTuple>(key);
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    std::vector<typename IndexWriter<Strategy>::NodePtr>
    IndexWriter<Strategy>::_find_leaf_path(TuplePtr key)
    {
        std::vector<NodePtr> path{ _root };

        while (path.back()->type.is_branch()) {
            NodePtr cur = path.back();
            auto row_i = _strategy.choose_child(cur->extent, cur->schema, key);
            uint64_t id = _branch_child_f->get_uint64(&*row_i);
            path.push_back(_get_child(cur, id));
        }

        return path;
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_touch(std::vector<NodePtr> &path, size_t i)
    {
        // Every ancestor from path[i] up to (and including) the root must end up dirty, not just
        // path[i] itself: CoW means flushing path[i] assigns it a brand-new extent_id, which
        // means its parent's branch row pointing at it must be patched -- which means the parent
        // must *also* be reflushed to a new location, cascading all the way to the root. Walk
        // upward marking every not-yet-dirty ancestor, stopping as soon as one is already dirty
        // (everything above it is too, by this same invariant).
        for (size_t k = i + 1; k-- > 0; ) {
            NodePtr node = path[k];
            if (node->dirty) {
                break;
            }

            // Copy-on-write: `node->extent` may still be the exact object IndexExtentCache handed
            // back (and may hand back again, to some other writer on some other tree, or to a
            // sibling generation of this same tree -- see test_index_writer.cc's
            // InsertAndRemove). Clone it into a private copy before any mutation touches it,
            // mirroring StorageCache::DataCache::_extract()'s "copy before marking dirty"
            // behavior.
            node->extent = std::make_shared<Extent>(*node->extent);
            node->dirty = true;

            if (k > 0) {
                path[k - 1]->children.push_back(node);
            }
        }
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    Extent::Iterator
    IndexWriter<Strategy>::_lower_bound_rows(Extent &extent, ExtentSchemaPtr schema, TuplePtr key)
    {
        return std::ranges::lower_bound(extent, *key,
                                        [](const Tuple &lhs, const Tuple &rhs) {
                                            return lhs.less_than(rhs);
                                        },
                                        [&schema](const Extent::Row &row) {
                                            return FieldTuple(schema->get_sort_fields(), &row);
                                        });
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_insert_row(std::shared_ptr<Extent> extent, ExtentSchemaPtr schema, TuplePtr value)
    {
        auto fields = schema->get_mutable_fields();

        if (extent->empty()) {
            auto row = extent->append();
            MutableTuple(fields, &row).assign(value);
            return;
        }

        TuplePtr key = schema->tuple_subset(value, schema->get_sort_keys());
        auto row_i = _lower_bound_rows(*extent, schema, key);

        CHECK(row_i == extent->end() ||
             !FieldTuple(schema->get_sort_fields(), &*row_i).equal_strict(*key));

        auto row = extent->insert(row_i);
        MutableTuple(fields, &row).assign(value);
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_remove_row(std::shared_ptr<Extent> extent, ExtentSchemaPtr schema, TuplePtr key)
    {
        auto row_i = _lower_bound_rows(*extent, schema, key);
        CHECK(row_i != extent->end());
        DCHECK(FieldTuple(schema->get_sort_fields(), &*row_i).equal_strict(*key));

        extent->remove(row_i);
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_insert_branch_row(NodePtr parent, NodePtr child)
    {
        auto value = std::make_shared<FieldArray>();
        value->push_back(std::make_shared<ConstTypeField<uint64_t>>(child->id));

        auto kv = std::make_shared<KeyValueTuple>(child->prev_key->fields(), value, child->prev_key->row());

        _insert_row(parent->extent, parent->schema, kv);
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    std::pair<typename IndexWriter<Strategy>::NodePtr, typename IndexWriter<Strategy>::NodePtr>
    IndexWriter<Strategy>::_split_node(NodePtr node)
    {
        uint32_t split_at = _strategy.split_point(node->extent);
        uint32_t total = node->extent->row_count();

        // a split result can never be the root -- strip the root flag, if set
        ExtentType child_type(node->type.is_branch());
        ExtentHeader header(child_type, _xid, node->schema->row_size(), node->schema->field_types());

        auto first_extent = _cache->copy_range(node->extent, node->schema, 0, split_at, header);
        auto second_extent = _cache->copy_range(node->extent, node->schema, split_at,
                                                total - split_at, header);

        MutableFieldArrayPtr key_fields = (node->schema == _branch_schema) ? _branch_keys : _leaf_keys;

        auto first = std::make_shared<Node>();
        first->id = _next_temp_id++;
        first->prev_offset = node->prev_offset;
        first->type = child_type;
        first->extent = first_extent;
        first->schema = node->schema;
        first->dirty = true;
        first->prev_key = _make_value_key(*first_extent, key_fields);

        auto second = std::make_shared<Node>();
        second->id = _next_temp_id++;
        second->prev_offset = node->prev_offset;
        second->type = child_type;
        second->extent = second_extent;
        second->schema = node->schema;
        second->dirty = true;
        second->prev_key = _make_value_key(*second_extent, key_fields);

        // A branch's `children` tracks its materialized (touched-or-split-but-not-yet-flushed)
        // descendants, keyed by whatever id each one's row in `node->extent` currently holds
        // (possibly a temp id, if that descendant was itself already split earlier this
        // generation). copy_range() above duplicated those rows verbatim into `first`/`second`,
        // but *not* the children bookkeeping -- every descendant must be handed to whichever of
        // `first`/`second` now holds its row, or _flush_node()'s traversal can never reach it
        // again: its temp id would be copied as-is into whichever of `first`/`second` eventually
        // gets flushed, instead of being patched to a real extent_id, and reading the tree back
        // later would try to resolve that stale temp id as if it were a real on-disk offset.
        for (auto &child : node->children) {
            auto row_i = std::ranges::find_if(*node->extent, [this, &child](const Extent::Row &row) {
                return _branch_child_f->get_uint64(&row) == child->id;
            });
            CHECK(row_i != node->extent->end());

            uint32_t index = static_cast<uint32_t>(row_i - node->extent->begin());
            NodePtr &new_parent = (index < split_at) ? first : second;
            new_parent->children.push_back(child);
        }

        return { first, second };
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_propagate_splits(std::vector<NodePtr> &path)
    {
        int i = static_cast<int>(path.size()) - 1;

        while (i >= 0 && _strategy.needs_split(path[i]->extent)) {
            NodePtr node = path[i];
            auto split_result = _split_node(node);
            NodePtr first = split_result.first;
            NodePtr second = split_result.second;

            if (i == 0) {
                // the root itself split -- build a brand-new branch root over {first, second}.
                // The new root has exactly two entries, so it can't itself be oversized.
                ExtentHeader header(ExtentType(true, true), _xid, _branch_schema->row_size(),
                                    _branch_schema->field_types());
                auto root_extent = _cache->allocate(header);

                auto root = std::make_shared<Node>();
                root->id = _next_temp_id++;
                root->prev_offset = constant::UNKNOWN_EXTENT;
                root->type = ExtentType(true, true);
                root->extent = root_extent;
                root->schema = _branch_schema;
                root->dirty = true;
                root->children = { first, second };

                _insert_branch_row(root, first);
                _insert_branch_row(root, second);

                _root = root;
                return;
            }

            NodePtr parent = path[i - 1];
            // already dirty: insert()/remove() touched the whole path up front (see _touch())

            // Remove node's old row, located by its *child pointer* (node->id), not by the key it
            // was recorded under. node->prev_key is a cached snapshot of node's max key from
            // whenever node was last (re)created (materialized from disk, or produced by an
            // earlier split this generation) -- it is only ever used to *write* a branch row
            // (_insert_branch_row), never refreshed when further inserts/removes into node change
            // its actual current max without node itself splitting again. Searching for that
            // stale key via lower_bound can land past node's real row (or miss it outright) once
            // enough intervening churn has moved other siblings' keys around it -- confirmed via a
            // direct repro mirroring Table_Test.MultiThreadMutations' workload (every row's
            // secondary-index key reassigned, many of them, in an order decorrelated from the
            // original insert order, within one generation). node->id is node's one stable,
            // never-stale identity (see TEMP_ID_BASE's comment), so locate the row by it instead,
            // mirroring _get_child()'s and _split_node()'s children-partition loop's own pattern.
            auto row_i = std::ranges::find_if(*parent->extent, [this, &node](const Extent::Row &row) {
                return _branch_child_f->get_uint64(&row) == node->id;
            });
            CHECK(row_i != parent->extent->end());
            parent->extent->remove(row_i);

            std::erase(parent->children, node);
            parent->children.push_back(first);
            parent->children.push_back(second);

            _insert_branch_row(parent, first);
            _insert_branch_row(parent, second);

            --i;
        }
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_remove_empty_leaf(std::vector<NodePtr> &path)
    {
        size_t i = path.size() - 1;

        while (i > 0) {
            NodePtr node = path[i];
            NodePtr parent = path[i - 1];
            bool parent_is_root = (i == 1);
            // already dirty: insert()/remove() touched the whole path up front (see _touch())

            // Locate node's row by its child pointer, not its (possibly stale) recorded key --
            // see _propagate_splits()'s identical fix and comment for why.
            auto row_i = std::ranges::find_if(*parent->extent, [this, &node](const Extent::Row &row) {
                return _branch_child_f->get_uint64(&row) == node->id;
            });
            CHECK(row_i != parent->extent->end());
            parent->extent->remove(row_i);
            std::erase(parent->children, node);

            if (!parent->extent->empty()) {
                break;
            }

            if (parent_is_root) {
                // demote the root to an empty leaf, mirroring
                // MutableBTree::Page::remove(is_root=true)'s behavior
                parent->type = ExtentType(false, true);
                parent->schema = _leaf_schema;
                break;
            }

            --i;
        }
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_propagate_key_increase(std::vector<NodePtr> &path, TuplePtr new_key)
    {
        // A branch row's recorded key is a routing upper bound for its child's subtree, written
        // once (by _insert_branch_row(), from the child's prev_key at that moment) and otherwise
        // never touched. If a later insert raises a child's own actual max past what's recorded
        // -- unremarkable on its own, see node->prev_key's class comment -- *routing* breaks the
        // moment that child is not its parent's last entry: choose_child()'s lower_bound(new key)
        // would then stop at whichever sibling's still-larger recorded key comes first, landing
        // the new row in the *wrong* leaf (one with no actual reason to reject it, so nothing
        // crashes -- it silently becomes unreachable from that key's own correct descent path).
        // Keep every ancestor's recorded key in lock-step with the actual max as it grows, so
        // choose_child()'s ordinary (non-fallback) path always lands in the right subtree.
        for (size_t i = path.size(); i-- > 1; ) {
            NodePtr node = path[i];
            NodePtr parent = path[i - 1];

            auto row_i = std::ranges::find_if(*parent->extent, [this, &node](const Extent::Row &row) {
                return _branch_child_f->get_uint64(&row) == node->id;
            });
            CHECK(row_i != parent->extent->end());

            Extent::Row row = *row_i;
            FieldTuple recorded(parent->schema->get_sort_fields(), &row);
            if (!recorded.less_than(*new_key)) {
                // already >= new_key at this level (and so, transitively, at every level above
                // it was set correctly before) -- nothing further to propagate.
                break;
            }

            MutableTuple(_branch_keys, &row).assign(*new_key);
            node->prev_key = std::make_shared<ValueTuple>(new_key);
        }
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::insert(TuplePtr value)
    {
        ReentrancyGuard guard(_in_call);
        CHECK(_root != nullptr);
        CHECK(!_finalized);

        TuplePtr key = _leaf_schema->tuple_subset(value, _sort_keys);
        auto path = _find_leaf_path(key);

        _touch(path, path.size() - 1);
        _insert_row(path.back()->extent, path.back()->schema, value);
        _propagate_key_increase(path, key);

        _propagate_splits(path);
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::remove(TuplePtr value)
    {
        ReentrancyGuard guard(_in_call);
        CHECK(_root != nullptr);
        CHECK(!_finalized);

        TuplePtr key = _leaf_schema->tuple_subset(value, _sort_keys);
        auto path = _find_leaf_path(key);

        _touch(path, path.size() - 1);
        _remove_row(path.back()->extent, path.back()->schema, key);

        if (path.back()->extent->empty() && path.size() > 1) {
            _remove_empty_leaf(path);
        }
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::truncate()
    {
        ReentrancyGuard guard(_in_call);
        CHECK(_root != nullptr);

        _root = nullptr;
        _init_empty_impl();
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_flush_node(NodePtr node, std::unordered_set<uint64_t> &reported_expired)
    {
        for (auto &child : node->children) {
            uint64_t old_id = child->id;

            _flush_node(child, reported_expired);

            // patch this node's branch row with the child's real, now-known extent_id
            auto row_i = std::ranges::find_if(*node->extent, [this, old_id](const Extent::Row &row) {
                return _branch_child_f->get_uint64(&row) == old_id;
            });
            CHECK(row_i != node->extent->end());

            Extent::Row row = *row_i;
            _branch_child_f->set_uint64(&row, child->id);
        }

        node->extent->header() = ExtentHeader(node->type, _xid, node->schema->row_size(),
                                              node->schema->field_types(), node->prev_offset);

        auto segment_path = _segments.current_path();
        uint64_t within_offset = _cache->flush(_database_id, segment_path, node->extent);
        uint64_t real_id = SegmentSet::pack(_segments.current_segment_id(), within_offset);
        DCHECK_LT(real_id, TEMP_ID_BASE);

        // Notify the vacuumer that node->prev_offset is now superseded (architecture.md "btree"
        // redesign Sec 4's reachability invariant: once nothing points at it, it's reclaimable).
        // Phase 1 deliberately left this unwired (nothing referenced IndexWriter in production
        // yet) and flagged exactly this phase as the one to wire it up, and to decide how to
        // de-duplicate: a split's "first"/"second" results both inherit the same prev_offset
        // (_split_node), and if both independently flush (e.g. across different calls this
        // generation, or one now and one after a later re-split), reporting prev_offset more than
        // once would double-count it in the vacuumer's bookkeeping. reported_expired is scoped to
        // one finalize() call and dedupes within it; chained splits *across* generations can't
        // double-report the same prev_offset, since by the next generation it's already a real,
        // different on-disk id.
        //
        // Since phase 6, prev_offset is itself a packed (segment_id, offset) real id (every real
        // id is, post phase-6), so it must be decoded into its own segment's path before being
        // reported -- the vacuumer's dead-byte bookkeeping is keyed by literal, openable file
        // path (see vacuumer.cc), not by the tree's logical base path.
        //
        // The reported size must be prev_offset's own *exact* on-disk size, not an estimate --
        // phase 6 round-1 review (finding 1) caught that this used to report node->extent->
        // byte_count() (the *new*, replacement extent's logical byte count, mirroring
        // StorageCache::DataCache::_flush_and_update_extent()'s identical approximation), which
        // has no required relationship to prev_offset's true size: an inflated estimate could
        // push the vacuumer's whole-segment dead-byte sum (vacuumer.cc) up to the segment's file
        // size while other, untouched, live extents still remain in it, causing it to be
        // unlinked out from under a reader that can still reach them (architecture.md Sec 4
        // violation) -- "approximate size is harmless for hole-punch" (the original, still
        // correct, use of this approximation) does not carry over to "approximate size safely
        // gates an irreversible whole-file delete". IndexExtentCache already tracks every
        // extent's exact on-disk size for its own LRU bookkeeping; prev_offset's entry is still
        // cache-resident here in the overwhelmingly common case (this same node was read via
        // _get_child()/init() earlier this generation, and _touch() clones into a private copy
        // rather than mutating the cached original in place), so this is a cache hit, not an
        // extra disk read, except in the rare case the entry was evicted under memory pressure
        // in between, where re-reading it is the correct (if slightly more costly) fallback.
        if (node->prev_offset != constant::UNKNOWN_EXTENT &&
                reported_expired.insert(node->prev_offset).second) {
            auto prev_segment_path = SegmentSet::segment_path(
                    _file, SegmentSet::segment_id_of(node->prev_offset));
            uint32_t prev_size = 0;
            _cache->get(_database_id, prev_segment_path, SegmentSet::offset_of(node->prev_offset),
                       &prev_size);
            Vacuumer::get_instance()->expire_extent(prev_segment_path,
                                                    SegmentSet::offset_of(node->prev_offset),
                                                    prev_size, _xid);
        }

        _segments.advance_after_flush();

        node->id = real_id;
        node->dirty = false;
        node->children.clear();
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    uint64_t
    IndexWriter<Strategy>::finalize(bool fs_sync)
    {
        ReentrancyGuard guard(_in_call);
        CHECK(_root != nullptr);

        if (_root->dirty) {
            std::unordered_set<uint64_t> reported_expired;
            _flush_node(_root, reported_expired);
        }

        if (fs_sync) {
            _sync_impl();
        }

        _finalized = true;

        // A root that was init_empty()'d and never subsequently touched (never marked dirty) was
        // never flushed above, so _root->id is still its synthetic, never-on-disk temp id
        // (TEMP_ID_BASE range) rather than a real extent_id -- returning it verbatim would leak a
        // temp id to a caller expecting a durable root offset (phase-2.md's deviation 3, found
        // reachable in phase 3 via MutableTable::finalize() unconditionally finalizing
        // _primary_index for a brand-new, still-empty table). Mirrors MutableBTree::_flush_root()'s
        // existing behavior for the same case: a page that `!is_dirty()` is returned as-is, which
        // for a freshly init_empty()'d page still carries extent_id == UNKNOWN_EXTENT.
        if (_root->id >= TEMP_ID_BASE) {
            return constant::UNKNOWN_EXTENT;
        }

        return _root->id;
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_sync_impl()
    {
        // Every segment this tree has actually written to, not just the base file -- a segment
        // sealed and rolled past earlier this generation (or an earlier one) still needs its
        // bytes durable before finalize() returns, same as the current one.
        for (auto &path : _segments.all_paths()) {
            auto handle = IOMgr::get_instance()->open(path, IOMgr::IO_MODE::APPEND, true);
            handle->sync();
        }
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::sync()
    {
        ReentrancyGuard guard(_in_call);
        _sync_impl();
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    void
    IndexWriter<Strategy>::_descend_leftmost(std::vector<typename IndexWriter<Strategy>::Iterator::Frame> &frames)
    {
        while (frames.back().node->type.is_branch()) {
            auto &top = frames.back();
            uint64_t id = _branch_child_f->get_uint64(&*top.pos);
            NodePtr child = _get_child(top.node, id);
            frames.push_back({ child, child->extent->begin() });
        }
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    typename IndexWriter<Strategy>::Iterator
    IndexWriter<Strategy>::begin()
    {
        CHECK(_root != nullptr);

        std::vector<typename Iterator::Frame> frames{ { _root, _root->extent->begin() } };
        _descend_leftmost(frames);

        if (frames.back().pos == frames.back().node->extent->end()) {
            return end();
        }

        return Iterator(this, std::move(frames));
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    typename IndexWriter<Strategy>::Iterator
    IndexWriter<Strategy>::last()
    {
        CHECK(_root != nullptr);

        std::vector<typename Iterator::Frame> frames{ { _root, _root->extent->empty() ? _root->extent->end()
                                                                                       : _root->extent->last() } };

        while (frames.back().node->type.is_branch()) {
            auto &top = frames.back();
            uint64_t id = _branch_child_f->get_uint64(&*top.pos);
            NodePtr child = _get_child(top.node, id);
            frames.push_back({ child, child->extent->empty() ? child->extent->end() : child->extent->last() });
        }

        if (frames.back().pos == frames.back().node->extent->end()) {
            return end();
        }

        return Iterator(this, std::move(frames));
    }

    template <typename Strategy>
    requires WriteStrategy<Strategy>
    typename IndexWriter<Strategy>::Iterator
    IndexWriter<Strategy>::lower_bound(TuplePtr search_key, bool for_update)
    {
        CHECK(_root != nullptr);

        std::vector<typename Iterator::Frame> frames;
        NodePtr cur = _root;

        while (cur->type.is_branch()) {
            auto row_i = _strategy.choose_child(cur->extent, cur->schema, search_key);
            frames.push_back({ cur, row_i });
            uint64_t id = _branch_child_f->get_uint64(&*row_i);
            cur = _get_child(cur, id);
        }

        auto row_i = _lower_bound_rows(*cur->extent, cur->schema, search_key);
        frames.push_back({ cur, row_i });

        if (row_i != cur->extent->end()) {
            return Iterator(this, std::move(frames));
        }

        // search_key is past every entry in this leaf -- advance to the next leaf, exactly like
        // Iterator::operator++()'s rollover
        frames.pop_back();
        while (!frames.empty()) {
            ++frames.back().pos;
            if (frames.back().pos != frames.back().node->extent->end()) {
                _descend_leftmost(frames);
                return Iterator(this, std::move(frames));
            }
            frames.pop_back();
        }

        // search_key is past every entry in the whole tree
        if (for_update) {
            return last();
        }
        return end();
    }

    // Explicit instantiation -- the only IndexWriter specialization this phase builds. Phases 10
    // (GiST) and 11 (GIN) each add their own `template class IndexWriter<TheirStrategy>;` line
    // here when they introduce a new WriteStrategy.
    template class IndexWriter<BtreeWriteStrategy>;
}
