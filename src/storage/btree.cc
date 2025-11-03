#include <common/constants.hh>
#include <storage/btree.hh>

namespace springtail {
    BTree::BTree(uint64_t database_id,
                 const std::filesystem::path &file,
                 uint64_t xid,
                 ExtentSchemaPtr schema,
                 uint64_t root_offset,
                 uint64_t max_extent_size,
                 const ExtensionCallback &extension_callback)
        : _database_id(database_id),
          _file(file),
          _xid(xid),
          _leaf_schema(schema),
          _root_offset(root_offset),
          _max_extent_size(max_extent_size)
    {
        // get the sort keys of the leaf extents
        auto keys = _leaf_schema->get_sort_keys();

        // construct the field tuples for the leaf nodes
        _leaf_keys = _leaf_schema->get_sort_fields();

        // construct the schema for the branches
        // note: don't need a valid sql_type for the internal nodes since they aren't exposed
        SchemaColumn child(constant::BTREE_CHILD_FIELD, 0, SchemaType::UINT64, 0, false);
        _branch_schema = _leaf_schema->create_schema(keys, { child }, keys, extension_callback);

        // construct the field tuples for the branch nodes
        _branch_keys = _branch_schema->get_fields(keys);
        _branch_child_f = _branch_schema->get_field(constant::BTREE_CHILD_FIELD);
    }

    BTree::Iterator
    BTree::begin() const
    {
        // check if we don't have a root
        if (_root_offset == constant::UNKNOWN_EXTENT) {
            return end();
        }

        // read the root
        LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Get root page: {}", _root_offset);
        auto root = StorageCache::get_instance()->get(_database_id, _file, _root_offset, _xid, constant::LATEST_XID, _max_extent_size);

        // check if the root is empty
        if (root->empty()) {
            LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Return empty root");
            return end();
        }

        // create a node for the root
        auto node = std::make_shared<Node>(std::move(root));

        // iterate down to the leaf
        while (node->page->header().type.is_branch()) {
            // get the offset for the child
            uint64_t child_id = _branch_child_f->get_uint64(&*(node->row_i));

            // read the extent
            LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Get child page: {}", child_id);
            auto child = StorageCache::get_instance()->get(_database_id, _file, child_id, _xid, constant::LATEST_XID, _max_extent_size);

            // create a node for the child an move to it
            auto begin = child->begin();
            node = std::make_shared<Node>(std::move(child), begin, node);
        }

        LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Return begin");
        return Iterator(this, node);
    }

    BTree::Iterator
    BTree::lower_bound(TuplePtr search_key,
                       bool for_update) const
    {
        // check if we don't have a root
        if (_root_offset == constant::UNKNOWN_EXTENT) {
            return end();
        }

        // read the root
        auto current = StorageCache::get_instance()->get(_database_id, _file, _root_offset, _xid, constant::LATEST_XID, _max_extent_size);

        // check if the root is empty
        if (current->empty()) {
            return end();
        }

        // iterate through the levels until we find a leaf node
        NodePtr node = nullptr;
        while (current->header().type.is_branch()) {
            // perform a lower-bound check to find the appropriate child branch
            auto child_i = current->lower_bound(search_key, this->_branch_schema);
            if (child_i == current->end()) {
                if (for_update) {
                    child_i = current->last();
                } else {
                    return end();
                }
            }

            // retrieve the child offset
            uint64_t extent_id = _branch_child_f->get_uint64(&*child_i);

            // read the child extent
            auto child = StorageCache::get_instance()->get(_database_id, _file, extent_id, _xid, constant::LATEST_XID, _max_extent_size);

            // recurse to the child
            node = std::make_shared<Node>(std::move(current), child_i, node);
            current = std::move(child);
        }

        // now find the entry in the leaf node
        auto leaf_i = current->lower_bound(search_key, this->_leaf_schema);
        if (leaf_i == current->end()) {
            if (for_update) {
                leaf_i = current->last();
            } else {
                return end();
            }
        }

        node = std::make_shared<Node>(std::move(current), leaf_i, node);
        return Iterator(this, node);
    }

    BTree::Iterator
    BTree::upper_bound(TuplePtr search_key,
                       bool for_update) const
    {
        // check if we don't have a root
        if (_root_offset == constant::UNKNOWN_EXTENT) {
            return end();
        }

        // read the root
        auto current = StorageCache::get_instance()->get(_database_id, _file, _root_offset, _xid, constant::LATEST_XID, _max_extent_size);

        // check if the root is empty
        if (current->empty()) {
            return end();
        }

        // iterate through the levels until we find a leaf node
        NodePtr node = nullptr;
        while (current->header().type.is_branch()) {
            // perform a lower-bound check to find the appropriate child branch
            auto child_i = current->upper_bound(search_key, this->_branch_schema);
            if (child_i == current->end()) {
                if (for_update) {
                    child_i = current->last();
                } else {
                    return end();
                }
            }

            // retrieve the child offset
            uint64_t extent_id = _branch_child_f->get_uint64(&*child_i);

            // read the child extent
            auto child = StorageCache::get_instance()->get(_database_id, _file, extent_id, _xid, constant::LATEST_XID, _max_extent_size);

            // recurse to the child
            node = std::make_shared<Node>(std::move(current), child_i, node);
            current = std::move(child);
        }

        // now find the entry in the leaf node
        auto leaf_i = current->upper_bound(search_key, this->_leaf_schema);
        if (leaf_i == current->end()) {
            if (for_update) {
                leaf_i = current->last();
            } else {
                return end();
            }
        }

        node = std::make_shared<Node>(std::move(current), leaf_i, node);
        return Iterator(this, node);
    }

    BTree::Iterator
    BTree::inverse_upper_bound(TuplePtr search_key) const
    {
        // check for empty() case
        if (empty()) {
            return end();
        }

        // find the first entry <= the key
        Iterator &&i = lower_bound(search_key);

        // go to the previous entry
        --i;

        return i;
    }

    BTree::Iterator
    BTree::inverse_lower_bound(TuplePtr search_key) const
    {
        auto &&i = upper_bound(search_key);

        if (i == begin()) {
            return end();
        }

        return --i;
    }

    BTree::Iterator
    BTree::find(TuplePtr search_key) const
    {
        // find the lower_bound based on the search key
        auto i = lower_bound(search_key);
        if (i == end()) {
            return i; // not found, return end()
        }

        // generate the key of the provided row
        auto key = std::make_shared<FieldTuple>(_leaf_keys, &*i);

        // if the search key is < found key from lower_bound, then does not exist
        if (search_key->less_than(key)) {
            return end();
        }

        // found
        return i;
    }
}
