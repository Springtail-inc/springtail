#include <storage/index_engine/btree_read_strategy.hh>
#include <storage/index_engine/index_reader.hh>

namespace springtail {
    template <typename Strategy>
    requires ReadStrategy<Strategy>
    IndexReader<Strategy>::IndexReader(uint64_t database_id,
                                       const std::filesystem::path &file,
                                       const std::vector<std::string> &keys,
                                       ExtentSchemaPtr schema,
                                       uint64_t xid,
                                       uint64_t root_offset,
                                       ExtentSourcePtr source,
                                       Strategy strategy,
                                       const ExtensionCallback &extension_callback)
        : _database_id(database_id),
          _file(file),
          _sort_keys(keys),
          _source(std::move(source)),
          _strategy(std::move(strategy)),
          _xid(xid),
          _root_offset(root_offset),
          _leaf_schema(schema),
          _extension_callback(extension_callback)
    {
        _leaf_keys = _leaf_schema->get_fields(keys);

        SchemaColumn child(constant::BTREE_CHILD_FIELD, 0, SchemaType::UINT64, 0, false);
        _branch_schema = _leaf_schema->create_index_schema(keys, { child }, keys, extension_callback);

        _branch_child_f = _branch_schema->get_field(constant::BTREE_CHILD_FIELD);
    }

    template <typename Strategy>
    requires ReadStrategy<Strategy>
    void
    IndexReader<Strategy>::_descend_leftmost(std::vector<typename Iterator::Frame> &frames) const
    {
        while (frames.back().extent->header().type.is_branch()) {
            auto &top = frames.back();
            uint64_t child_id = _branch_child_f->get_uint64(&*top.pos);
            auto child = _get_extent(child_id);
            frames.push_back({ child, _schema_for(child), child->begin() });
        }
    }

    template <typename Strategy>
    requires ReadStrategy<Strategy>
    void
    IndexReader<Strategy>::_descend_rightmost(std::vector<typename Iterator::Frame> &frames) const
    {
        while (frames.back().extent->header().type.is_branch()) {
            auto &top = frames.back();
            uint64_t child_id = _branch_child_f->get_uint64(&*top.pos);
            auto child = _get_extent(child_id);
            frames.push_back({ child, _schema_for(child), child->last() });
        }
    }

    template <typename Strategy>
    requires ReadStrategy<Strategy>
    typename IndexReader<Strategy>::Iterator
    IndexReader<Strategy>::begin() const
    {
        if (_root_offset == constant::UNKNOWN_EXTENT) {
            return end();
        }

        auto root = _get_extent(_root_offset);
        if (root->empty()) {
            return end();
        }

        std::vector<typename Iterator::Frame> frames{ { root, _schema_for(root), root->begin() } };
        _descend_leftmost(frames);

        return Iterator(this, std::move(frames));
    }

    template <typename Strategy>
    requires ReadStrategy<Strategy>
    typename IndexReader<Strategy>::Iterator
    IndexReader<Strategy>::lower_bound(TuplePtr search_key, bool for_update) const
    {
        if (_root_offset == constant::UNKNOWN_EXTENT) {
            return end();
        }

        auto current = _get_extent(_root_offset);
        if (current->empty()) {
            return end();
        }

        std::vector<typename Iterator::Frame> frames;

        while (current->header().type.is_branch()) {
            ExtentSchemaPtr schema = _branch_schema;
            auto child_i = _strategy.lower_bound(current, schema, search_key);
            if (child_i == current->end()) {
                if (for_update) {
                    child_i = current->last();
                } else {
                    return end();
                }
            }

            uint64_t child_id = _branch_child_f->get_uint64(&*child_i);
            auto child = _get_extent(child_id);

            frames.push_back({ current, schema, child_i });
            current = std::move(child);
        }

        ExtentSchemaPtr leaf_schema = _leaf_schema;
        auto leaf_i = _strategy.lower_bound(current, leaf_schema, search_key);
        if (leaf_i == current->end()) {
            if (for_update) {
                leaf_i = current->last();
            } else {
                return end();
            }
        }

        frames.push_back({ current, leaf_schema, leaf_i });
        return Iterator(this, std::move(frames));
    }

    template <typename Strategy>
    requires ReadStrategy<Strategy>
    typename IndexReader<Strategy>::Iterator
    IndexReader<Strategy>::upper_bound(TuplePtr search_key, bool for_update) const
    {
        if (_root_offset == constant::UNKNOWN_EXTENT) {
            return end();
        }

        auto current = _get_extent(_root_offset);
        if (current->empty()) {
            return end();
        }

        std::vector<typename Iterator::Frame> frames;

        while (current->header().type.is_branch()) {
            ExtentSchemaPtr schema = _branch_schema;
            auto child_i = _strategy.upper_bound(current, schema, search_key);
            if (child_i == current->end()) {
                if (for_update) {
                    child_i = current->last();
                } else {
                    return end();
                }
            }

            uint64_t child_id = _branch_child_f->get_uint64(&*child_i);
            auto child = _get_extent(child_id);

            frames.push_back({ current, schema, child_i });
            current = std::move(child);
        }

        ExtentSchemaPtr leaf_schema = _leaf_schema;
        auto leaf_i = _strategy.upper_bound(current, leaf_schema, search_key);
        if (leaf_i == current->end()) {
            if (for_update) {
                leaf_i = current->last();
            } else {
                return end();
            }
        }

        frames.push_back({ current, leaf_schema, leaf_i });
        return Iterator(this, std::move(frames));
    }

    template <typename Strategy>
    requires ReadStrategy<Strategy>
    typename IndexReader<Strategy>::Iterator
    IndexReader<Strategy>::inverse_upper_bound(TuplePtr search_key) const
    {
        if (empty()) {
            return end();
        }

        Iterator &&i = lower_bound(search_key);
        --i;

        return i;
    }

    template <typename Strategy>
    requires ReadStrategy<Strategy>
    typename IndexReader<Strategy>::Iterator
    IndexReader<Strategy>::inverse_lower_bound(TuplePtr search_key) const
    {
        auto &&i = upper_bound(search_key);

        if (i == begin()) {
            return end();
        }

        return --i;
    }

    template <typename Strategy>
    requires ReadStrategy<Strategy>
    typename IndexReader<Strategy>::Iterator
    IndexReader<Strategy>::find(TuplePtr search_key) const
    {
        auto i = lower_bound(search_key);
        if (i == end()) {
            return i;
        }

        auto key = std::make_shared<FieldTuple>(_leaf_keys, &*i);

        if (search_key->less_than(key)) {
            return end();
        }

        return i;
    }

    template <typename Strategy>
    requires ReadStrategy<Strategy>
    bool
    IndexReader<Strategy>::empty() const
    {
        if (_root_offset == constant::UNKNOWN_EXTENT) {
            return true;
        }

        return _get_extent(_root_offset)->empty();
    }

    // Explicit instantiation -- the only IndexReader specialization this phase builds. Phases 10
    // (GiST) and 11 (GIN) each add their own `template class IndexReader<TheirStrategy>;` line
    // here when they introduce a new ReadStrategy.
    template class IndexReader<BtreeReadStrategy>;
}
