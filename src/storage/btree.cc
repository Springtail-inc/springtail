#include <storage/btree_common.hh>
#include <storage/btree.hh>

namespace springtail {
    BTree::BTree(std::shared_ptr<IOHandle> handle,
                 uint64_t file_id,
                 const std::vector<std::string> &keys,
                 std::shared_ptr<ExtentSchema> schema,
                 std::shared_ptr<ExtentCache> cache,
                 uint64_t min_xid,
                 uint64_t root_offset)
        : _handle(handle),
          _file_id(file_id),
          _keys(keys),
          _leaf_schema(schema),
          _cache(cache)
    {
        // construct the field tuples for the leaf nodes
        _leaf_keys = _leaf_schema->get_fields(keys);

        // construct the schema for the branches
        SchemaColumn child(BTREE_CHILD_FIELD, 0, SchemaType::UINT64, false);
        _branch_schema = _leaf_schema->create_schema(keys, { child });

        // construct the field tuples for the branch nodes
        _branch_keys = _branch_schema->get_fields(keys);
        _branch_child_f = _branch_schema->get_field(BTREE_CHILD_FIELD);

        // read the roots back to the min XID
        ExtentPtr root;
        uint64_t prev_offset;
        do {
            root = _read_extent(root_offset);
            _roots[root->header().xid] = root;

            prev_offset = root_offset;
            root_offset = root->header().prev_offset;

            // note: the first available XID will point to itself, so we know we can't go earlier
            //       when we see that case
        } while (root->header().xid > min_xid && root_offset != prev_offset);
    }

    void
    BTree::add_root(uint64_t root_offset)
    {
        // read the provided root from disk
        auto root = _read_extent(root_offset);

        // update the map of roots
        _roots[root->header().xid] = root;
    }

    void
    BTree::set_min_xid(uint64_t min_xid)
    {
        // note: we keep the _roots in reverse order to allow for use of lower_bound() when
        // searching and removing entries

        // find the root for the given min_xid
        auto pos = _roots.lower_bound(min_xid);

        // we must keep that entry, so move to the next lower entry (kept in reverse XID order)
        ++pos;

        // remove all of the entries we no longer need
        _roots.erase(pos, _roots.end());
    }

    BTree::Iterator
    BTree::begin(uint64_t xid) const
    {
        // find the root
        auto root = _find_root(xid);
        uint64_t tree_xid = root->header().xid;

        // if the root doesn't exist or is empty, return end()
        if (root == nullptr || root->empty()) {
            return end();
        }

        // create a node for the root
        auto node = std::make_shared<Node>(root);

        // iterate down to the leaf
        while (node->extent->header().type.is_branch()) {
            // get the offset for the child
            uint64_t child_id = _branch_child_f->get_uint64(*(node->row_i));

            // read the extent
            ExtentPtr child = _read_extent(child_id);

            // create a node for the child an move to it
            node = std::make_shared<Node>(child, child->begin(), node);
        }

        return Iterator(this, node, tree_xid);
    }

    BTree::Iterator
    BTree::lower_bound(TuplePtr search_key,
                       uint64_t xid) const
    {
        // find the correct root based on the XID
        auto root = _find_root(xid);
        if (root == nullptr || root->empty()) {
            return end();
        }

        // iterate through the levels until we find a leaf node
        ExtentPtr current = root;
        NodePtr node = nullptr;
        while (current->type().is_branch()) {
            // perform a lower-bound check to find the appropriate child branch
            auto child_i = std::lower_bound(current->begin(), current->end(), search_key,
                                            [this](const Extent::Row &row, TuplePtr key)
                                            {
                                                return this->_branch_keys->bind(row)->less_than(key);
                                            });
            if (child_i == current->end()) {
                return end();
            }

            // retrieve the child offset
            uint64_t extent_id = _branch_child_f->get_uint64(*child_i);

            // read the child extent
            ExtentPtr child = _read_extent(extent_id);

            // recurse to the child
            node = std::make_shared<Node>(current, child_i, node);
            current = child;
        }

        // now find the entry in the leaf node
        auto leaf_i = std::lower_bound(current->begin(), current->end(), search_key,
                                       [this](const Extent::Row &row, TuplePtr key)
                                       {
                                           return this->_leaf_keys->bind(row)->less_than(key);
                                       });
        if (leaf_i == current->end()) {
            return end();
        }

        node = std::make_shared<Node>(current, leaf_i, node);
        return Iterator(this, node, xid);
    }

    BTree::Iterator
    BTree::find(TuplePtr search_key,
                uint64_t xid) const
    {
        // find the lower_bound based on the search key
        auto i = lower_bound(search_key, xid);
        if (i == end()) {
            return i; // not found, return end()
        }

        // generate the key of the provided row
        auto &&key = _leaf_keys->bind(*i);

        // if the search key is < found key from lower_bound, then does not exist
        if (search_key->less_than(key)) {
            return end();
        }

        // found
        return i;
    }

    ExtentPtr
    BTree::_read_extent(uint64_t extent_id) const
    {
        std::pair<uint64_t, uint64_t> cache_id(_file_id, extent_id);

        // first check the cache
        ExtentPtr extent = _cache->get(cache_id);
        if (extent != nullptr) {
            return extent;
        }

        // then read from the file
        auto response = _handle->read(extent_id);

        // unpack the header to determine the extent type
        ExtentHeader header(response->data[0]);

        // construct the extent
        if (header.type.is_branch()) {
            extent = std::make_shared<Extent>(_branch_schema, response->data);
        } else {
            extent = std::make_shared<Extent>(_leaf_schema, response->data);
        }

        // store into the cache
        _cache->insert(cache_id, extent, extent->byte_count());

        return extent;
    }

    ExtentPtr
    BTree::_find_root(uint64_t xid) const
    {
        // if the tree is empty, no root
        if (_roots.empty()) {
            return nullptr;
        }

        // find the right root for the requested XID
        auto &&root_i = _roots.lower_bound(xid);
        ExtentPtr root = root_i->second;

        return root;
    }
}
