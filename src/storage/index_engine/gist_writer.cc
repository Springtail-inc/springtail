#include <algorithm>

#include <absl/log/check.h>
#include <fmt/format.h>

#include <storage/index_engine/gist_writer.hh>
#include <storage/io.hh>
#include <storage/io_mgr.hh>

namespace springtail {
    using gist_helpers::GistEntry;

    GistWriter::GistWriter(uint64_t database_id, const std::filesystem::path &file, uint64_t xid,
                           GistWriteStrategy strategy, std::shared_ptr<IndexExtentCache> cache)
        : _database_id(database_id),
          _file(file),
          _strategy(std::move(strategy)),
          _cache(std::move(cache)),
          _segments(file),
          _xid(xid)
    {
        // Schema construction is shared with GistReader (gist_helpers::build_leaf_schema/
        // build_branch_schema) -- the two engines must agree on exactly this on-disk layout.
        _leaf_schema = gist_helpers::build_leaf_schema(_strategy.opclass_names());
        _branch_schema = gist_helpers::build_branch_schema(_strategy.opclass_names());

        _branch_child_f = _branch_schema->get_mutable_field(constant::BTREE_CHILD_FIELD);
        _leaf_row_id_f = _leaf_schema->get_field(constant::INTERNAL_ROW_ID);
    }

    void GistWriter::_init_empty_impl() {
        CHECK(_root == nullptr);

        ExtentHeader header(ExtentType(false, true), _xid, _leaf_schema->row_size(), _leaf_schema->field_types());
        auto extent = _cache->allocate(header);

        auto root = std::make_shared<Node>();
        root->id = _next_temp_id++;
        root->is_branch = false;
        root->extent = extent;

        _root = root;
    }

    void GistWriter::init_empty() {
        ReentrancyGuard guard(_in_call);
        _init_empty_impl();
    }

    void GistWriter::init(uint64_t root_offset) {
        ReentrancyGuard guard(_in_call);
        CHECK(_root == nullptr);
        DCHECK_LT(root_offset, TEMP_ID_BASE);

        auto segment_path = SegmentSet::segment_path(_file, SegmentSet::segment_id_of(root_offset));
        auto extent = _cache->get(_database_id, segment_path, SegmentSet::offset_of(root_offset));

        auto root = std::make_shared<Node>();
        root->id = root_offset;
        root->superseded_offsets = { root_offset };
        root->is_branch = extent->header().type.is_branch();
        root->extent = extent;

        _root = root;
        _segments.resume_from(root_offset);
    }

    void GistWriter::truncate() {
        ReentrancyGuard guard(_in_call);
        CHECK(_root != nullptr);
        _root = nullptr;
        _init_empty_impl();
    }

    GistWriter::NodePtr GistWriter::_get_child(NodePtr parent, uint64_t id) {
        for (auto &child : parent->children) {
            if (child->id == id) {
                return child;
            }
        }

        DCHECK_LT(id, TEMP_ID_BASE);
        auto segment_path = SegmentSet::segment_path(_file, SegmentSet::segment_id_of(id));
        auto extent = _cache->get(_database_id, segment_path, SegmentSet::offset_of(id));

        auto node = std::make_shared<Node>();
        node->id = id;
        node->superseded_offsets = { id };
        node->is_branch = extent->header().type.is_branch();
        node->extent = extent;

        return node;
    }

    std::vector<gist_helpers::GistEntry> GistWriter::_entries_of(NodePtr node) {
        std::vector<GistEntry> out;
        out.reserve(node->extent->row_count());

        ExtentSchemaPtr schema = node->is_branch ? _branch_schema : _leaf_schema;
        for (auto it = node->extent->begin(); it != node->extent->end(); ++it) {
            out.push_back(node->is_branch ? _strategy.read_branch(*it, schema)
                                          : _strategy.read_leaf(*it, schema));
        }
        return out;
    }

    std::vector<GistWriter::NodePtr> GistWriter::_choose_subtree_path(const GistEntry &incoming) {
        std::vector<NodePtr> path{ _root };

        while (path.back()->is_branch) {
            NodePtr cur = path.back();

            double best_penalty = 0.0;
            uint64_t best_child_id = constant::UNKNOWN_EXTENT;

            for (auto it = cur->extent->begin(); it != cur->extent->end(); ++it) {
                GistEntry branch_entry = _strategy.read_branch(*it, _branch_schema);
                double p = _strategy.penalty(branch_entry, incoming);
                if (best_child_id == constant::UNKNOWN_EXTENT || p < best_penalty) {
                    best_penalty = p;
                    best_child_id = branch_entry.child_id;
                }
            }

            CHECK_NE(best_child_id, constant::UNKNOWN_EXTENT) << "GistWriter: branch node has no rows";
            path.push_back(_get_child(cur, best_child_id));
        }

        return path;
    }

    void GistWriter::_touch(std::vector<NodePtr> &path, size_t i) {
        for (size_t k = i + 1; k-- > 0; ) {
            NodePtr node = path[k];
            if (node->dirty) {
                break;
            }

            node->extent = std::make_shared<Extent>(*node->extent);
            node->dirty = true;

            if (k > 0) {
                path[k - 1]->children.push_back(node);
            }
        }
    }

    std::pair<GistWriter::NodePtr, GistWriter::NodePtr> GistWriter::_split_node(NodePtr node) {
        auto entries = _entries_of(node);
        std::vector<size_t> left_idx, right_idx;
        _strategy.picksplit(entries, left_idx, right_idx);

        ExtentSchemaPtr schema = node->is_branch ? _branch_schema : _leaf_schema;

        auto build_side = [&](const std::vector<size_t> &idx) {
            ExtentHeader header(ExtentType(node->is_branch), _xid, schema->row_size(), schema->field_types());
            auto extent = _cache->allocate(header);

            for (size_t i : idx) {
                auto row = extent->append();
                if (node->is_branch) {
                    _strategy.write_branch(row, entries[i], entries[i].child_id, schema);
                } else {
                    _strategy.write_leaf(row, entries[i], schema);
                }
            }

            auto n = std::make_shared<Node>();
            n->id = _next_temp_id++;
            n->superseded_offsets = node->superseded_offsets;
            n->is_branch = node->is_branch;
            n->extent = extent;
            n->dirty = true;
            return n;
        };

        NodePtr first = build_side(left_idx);
        NodePtr second = build_side(right_idx);

        if (node->is_branch) {
            std::unordered_set<uint64_t> left_child_ids;
            for (size_t i : left_idx) {
                left_child_ids.insert(entries[i].child_id);
            }
            for (auto &child : node->children) {
                (left_child_ids.contains(child->id) ? first : second)->children.push_back(child);
            }
        }

        return { first, second };
    }

    void GistWriter::_append_branch_row(NodePtr parent, NodePtr child) {
        auto key = _strategy.union_entries(_entries_of(child));
        auto row = parent->extent->append();
        _strategy.write_branch(row, key, child->id, _branch_schema);
    }

    void GistWriter::_adjust_ancestor_keys(std::vector<NodePtr> &path) {
        for (size_t i = path.size(); i-- > 1; ) {
            NodePtr node = path[i];
            NodePtr parent = path[i - 1];

            auto new_key = _strategy.union_entries(_entries_of(node));

            auto row_i = std::ranges::find_if(*parent->extent, [this, &node](const Extent::Row &row) {
                return _branch_child_f->get_uint64(&row) == node->id;
            });
            CHECK(row_i != parent->extent->end());

            Extent::Row row = *row_i;
            _strategy.write_branch(row, new_key, node->id, _branch_schema);
        }
    }

    void GistWriter::_propagate_splits(std::vector<NodePtr> &path) {
        int i = static_cast<int>(path.size()) - 1;

        while (i >= 0 && _strategy.needs_split(path[i]->extent)) {
            NodePtr node = path[i];
            auto [first, second] = _split_node(node);

            if (i == 0) {
                ExtentHeader header(ExtentType(true, true), _xid, _branch_schema->row_size(),
                                    _branch_schema->field_types());
                auto root_extent = _cache->allocate(header);

                auto root = std::make_shared<Node>();
                root->id = _next_temp_id++;
                root->is_branch = true;
                root->extent = root_extent;
                root->dirty = true;
                root->children = { first, second };

                _append_branch_row(root, first);
                _append_branch_row(root, second);

                _root = root;
                return;
            }

            NodePtr parent = path[i - 1];

            auto row_i = std::ranges::find_if(*parent->extent, [this, &node](const Extent::Row &row) {
                return _branch_child_f->get_uint64(&row) == node->id;
            });
            CHECK(row_i != parent->extent->end());
            parent->extent->remove(row_i);

            std::erase(parent->children, node);
            parent->children.push_back(first);
            parent->children.push_back(second);

            _append_branch_row(parent, first);
            _append_branch_row(parent, second);

            --i;
        }
    }

    void GistWriter::insert(TuplePtr value) {
        ReentrancyGuard guard(_in_call);
        CHECK(_root != nullptr);

        GistEntry incoming = _strategy.compress_leaf(value);
        incoming.internal_row_id = value->field(static_cast<int>(_strategy.num_columns()))
                ->get_uint64(value->row());

        auto path = _choose_subtree_path(incoming);
        _touch(path, path.size() - 1);

        auto row = path.back()->extent->append();
        _strategy.write_leaf(row, incoming, _leaf_schema);

        _adjust_ancestor_keys(path);
        _propagate_splits(path);
    }

    void GistWriter::_remove_empty_leaf(std::vector<NodePtr> &path) {
        size_t i = path.size() - 1;

        while (i > 0) {
            NodePtr node = path[i];
            NodePtr parent = path[i - 1];
            bool parent_is_root = (i == 1);

            auto row_i = std::ranges::find_if(*parent->extent, [this, &node](const Extent::Row &row) {
                return _branch_child_f->get_uint64(&row) == node->id;
            });
            CHECK(row_i != parent->extent->end());
            parent->extent->remove(row_i);
            std::erase(parent->children, node);

            if (!parent->extent->empty()) {
                std::vector<NodePtr> surviving(path.begin(), path.begin() + static_cast<long>(i));
                _adjust_ancestor_keys(surviving);
                return;
            }

            if (parent_is_root) {
                parent->is_branch = false;
                return;
            }

            --i;
        }
    }

    bool GistWriter::_remove_from_subtree(NodePtr node, uint64_t target_row_id, std::vector<NodePtr> &path) {
        path.push_back(node);

        if (!node->is_branch) {
            auto row_i = std::ranges::find_if(*node->extent, [this, target_row_id](const Extent::Row &row) {
                return _leaf_row_id_f->get_uint64(&row) == target_row_id;
            });
            if (row_i == node->extent->end()) {
                path.pop_back();
                return false;
            }

            _touch(path, path.size() - 1);

            // _touch() may have cloned path.back()'s extent -- re-find the row in the (possibly
            // new) current extent object rather than reuse row_i, which was bound to whichever
            // extent object existed before the clone.
            auto row_i2 = std::ranges::find_if(*path.back()->extent, [this, target_row_id](const Extent::Row &row) {
                return _leaf_row_id_f->get_uint64(&row) == target_row_id;
            });
            CHECK(row_i2 != path.back()->extent->end());
            path.back()->extent->remove(row_i2);

            if (path.back()->extent->empty() && path.size() > 1) {
                _remove_empty_leaf(path);
            } else if (path.size() > 1) {
                _adjust_ancestor_keys(path);
            }

            path.pop_back();
            return true;
        }

        for (auto it = node->extent->begin(); it != node->extent->end(); ++it) {
            uint64_t child_id = _branch_child_f->get_uint64(&*it);
            NodePtr child = _get_child(node, child_id);
            if (_remove_from_subtree(child, target_row_id, path)) {
                path.pop_back();
                return true;
            }
        }

        path.pop_back();
        return false;
    }

    void GistWriter::remove(TuplePtr value) {
        ReentrancyGuard guard(_in_call);
        CHECK(_root != nullptr);

        uint64_t target_row_id = value->field(static_cast<int>(_strategy.num_columns()))
                ->get_uint64(value->row());

        std::vector<NodePtr> path;
        _remove_from_subtree(_root, target_row_id, path);
    }

    void GistWriter::_flush_node(NodePtr node, std::unordered_set<uint64_t> &reported_expired) {
        for (auto &child : node->children) {
            uint64_t old_id = child->id;
            _flush_node(child, reported_expired);

            auto row_i = std::ranges::find_if(*node->extent, [this, old_id](const Extent::Row &row) {
                return _branch_child_f->get_uint64(&row) == old_id;
            });
            CHECK(row_i != node->extent->end());

            Extent::Row row = *row_i;
            _branch_child_f->set_uint64(&row, child->id);
        }

        ExtentSchemaPtr schema = node->is_branch ? _branch_schema : _leaf_schema;
        uint64_t header_prev_offset = node->superseded_offsets.empty()
                ? constant::UNKNOWN_EXTENT : node->superseded_offsets.front();
        node->extent->header() = ExtentHeader(ExtentType(node->is_branch, node == _root), _xid,
                                              schema->row_size(), schema->field_types(), header_prev_offset);

        auto segment_path = _segments.current_path();
        uint64_t within_offset = _cache->flush(_database_id, segment_path, node->extent);
        uint64_t real_id = SegmentSet::pack(_segments.current_segment_id(), within_offset);
        DCHECK_LT(real_id, TEMP_ID_BASE);

        // Background space-reclaim compaction (architecture.md Sec 7.1) is explicitly out of
        // this phase's scope for GiST -- superseded_offsets is tracked (for a possible future
        // phase to wire up) but never reported to the Vacuumer here, unlike
        // IndexWriter::_flush_node()'s _report_superseded() calls. A GiST tree's segments are
        // only ever reclaimed today by the whole-tree truncate a DROP INDEX performs.
        _segments.advance_after_flush();

        node->id = real_id;
        node->dirty = false;
        node->children.clear();
    }

    uint64_t GistWriter::finalize(bool fs_sync) {
        ReentrancyGuard guard(_in_call);
        CHECK(_root != nullptr);

        if (_root->dirty) {
            std::unordered_set<uint64_t> reported_expired;
            _flush_node(_root, reported_expired);
        }

        if (fs_sync) {
            _sync_impl();
        }

        if (_root->id >= TEMP_ID_BASE) {
            return constant::UNKNOWN_EXTENT;
        }
        return _root->id;
    }

    void GistWriter::_sync_impl() {
        for (auto &path : _segments.all_paths()) {
            auto handle = IOMgr::get_instance()->open(path, IOMgr::IO_MODE::APPEND, true);
            handle->sync();
        }
    }

    void GistWriter::sync() {
        ReentrancyGuard guard(_in_call);
        _sync_impl();
    }

    void GistWriter::_collect_leaves(NodePtr node, std::vector<LeafRow> &out) {
        if (!node->is_branch) {
            for (auto it = node->extent->begin(); it != node->extent->end(); ++it) {
                out.push_back({ node->id, _strategy.read_leaf(*it, _leaf_schema) });
            }
            return;
        }

        for (auto it = node->extent->begin(); it != node->extent->end(); ++it) {
            uint64_t child_id = _branch_child_f->get_uint64(&*it);
            _collect_leaves(_get_child(node, child_id), out);
        }
    }

    std::vector<GistWriter::LeafRow> GistWriter::debug_dump_leaves() {
        std::vector<LeafRow> out;
        if (_root) {
            _collect_leaves(_root, out);
        }
        return out;
    }
}
