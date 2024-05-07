#pragma once

#include <boost/container_hash/hash.hpp>

#include <storage/cache.hh>
#include <storage/field.hh>
#include <storage/schema.hh>

namespace springtail {

    /**
     * A read-only b+tree constructed out of Extent objects.  Uses a FieldArray as the sort-key for
     * the tree which must be a subset of columns provided in a Schema for the BTree.  The leaf
     * nodes of the BTree contain the actual data specified by the Schema.  The branch nodes contain
     * only the sort-key columns, and an offset to the child extent.
     *
     * It uses an externally provided extent cache to store read extents.  It provides O(log n)
     * interfaces for find(), lower_bound(), and upper_bound(), which all return forward iterators.
     */
    class BTree {
    private:
        /**
         * An object that represents an extent as well as it's path from the root to that extent.
         * Used for tree traversal.
         */
        class Node {
        public:
            StorageCache::PagePtr page; ///< A pointer to the extent.
            StorageCache::Page::Iterator row_i; ///< An iterator to the entry in the parent that points to this extent.
            std::shared_ptr<Node> parent; ///< A pointer to the Node representing the parent extent.

            Node(StorageCache::PagePtr p)
                : page(p),
                  row_i(p->begin()),
                  parent(nullptr)
            { }

            Node(StorageCache::PagePtr page, StorageCache::Page::Iterator i, std::shared_ptr<Node> p)
                : page(page),
                  row_i(std::move(i)),
                  parent(p)
            { }

            bool operator==(const Node& rhs) const { return (page == rhs.page && row_i == rhs.row_i); }
        };
        typedef std::shared_ptr<Node> NodePtr;

    public:
        /**
         * An iterator object for traversing the BTree.
         */
        class Iterator {
            friend BTree;

        private:
            const BTree * const _btree; ///< A pointer to the BTree this iterator is associated with.
            NodePtr _node; ///< A Node representing a leaf extent and the path to it from the root.

            Iterator(const BTree * const btree, NodePtr node)
                : _btree(btree), _node(node)
            { }

        public:
            using iterator_category = std::bidirectional_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;  // or also value_type*
            using reference         = const Extent::Row &;  // or also value_type&

            reference operator*() const { return *(_node->row_i); }
            pointer operator->() { return &(*(_node->row_i)); }

            /**
             * Moves to the next entry in the tree.  Performs an in-order traversal of the tree to find leaf nodes.
             */
            Iterator& operator++() {
                // can't iterate forward on end()
                assert(_node != nullptr);

                // move to the next row in the leaf extent
                ++_node->row_i;
                if (_node->row_i != _node->page->end()) {
                    return *this;
                }

                // if at the end of the extent, traverse up the tree to find the next entry
                uint32_t depth = 0;

                // go up the tree
                while (_node->row_i == _node->page->end()) {
                    // iterate up to the parent
                    ++depth;
                    _node = _node->parent;

                    // if we were at the end of the root extent, then no more entries
                    if (_node == nullptr) {
                        return *this;
                    }

                    // move to the next entry in the parent
                    ++(_node->row_i);
                }

                // now go back down the tree
                while (depth > 0) {
                    // read the child's extent ID
                    uint64_t extent_id = _btree->_branch_child_f->get_uint64(*(_node->row_i));

                    // read the child extent
                    auto child = _btree->_read_extent(extent_id);

                    --depth;
                    _node = std::make_shared<Node>(child, child->begin(), _node);
                }

                // return the iterator
                return *this;
            }

            Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

            /**
             * Moves to the previous entry in the tree. If currently at end() then moves to the last
             * entry in the tree.  If currently at begin() then error.
             */
            Iterator &operator--() {
                // special case for end()
                if (_node == nullptr) {
                    // create a node for the root
                    _node = std::make_shared<Node>(_btree->_root, _btree->_root->last(), nullptr);

                    // iterate down to the leaf
                    while (_node->page->header().type.is_branch()) {
                        // get the offset for the child
                        uint64_t child_id = _btree->_branch_child_f->get_uint64(*(_node->row_i));

                        // read the extent
                        auto child = _btree->_read_extent(child_id);

                        // create a node for the child an move to it
                        _node = std::make_shared<Node>(child, child->last(), _node);
                    }

                    return *this;
                }

                // if this is not the first entry in the extent, go to the previous entry
                if (_node->row_i != _node->page->begin()) {
                    --(_node->row_i);
                    return *this;
                }

                // iterate up until we are no longer at a begin()
                uint32_t depth = 0;
                while (_node->row_i == _node->page->begin()) {
                    // iterate up to the parent
                    ++depth;
                    _node = _node->parent;

                    // if we went past the root then we were already at begin(), so return end()
                    if (_node == nullptr) {
                        return *this;
                    }
                }

                // move to the previous entry in the branch
                --(_node->row_i);

                // iterate down the last entry at each level
                while (depth > 0) {
                    // read the child's extent ID
                    uint64_t extent_id = _btree->_branch_child_f->get_uint64(*(_node->row_i));

                    // read the child extent
                    auto child = _btree->_read_extent(extent_id);

                    --depth;
                    _node = std::make_shared<Node>(child, child->last(), _node);
                }

                return *this;
            }

            friend bool operator==(const Iterator& a, const Iterator& b) {
                if ((a._node == nullptr) != (b._node == nullptr)) {
                    return false;
                }

                return (a._node == nullptr && b._node == nullptr) || (*a._node == *b._node);
            }

            friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }
        };

    public:
        /**
         * Constructs a new BTree object.
         *
         * @param file The path to the file, used to differentiate records in the shared cache.
         * @param keys A list of keys from the schema that are used to sort the entries of the tree.
         * @param schema The schema of the leaf entries of the tree.
         * @param cache A pointer to the shared cache of extents.
         * @param min_xid The earliest XID referencable by this BTree.
         * @param root_offset The offset of the root extent in the file.
         */
        BTree(const std::filesystem::path &file,
              uint64_t xid,
              ExtentSchemaPtr schema,
              uint64_t root_offset);

        /**
         * Returns an Iterator to the beginning entry of the tree for a given XID.
         */
        Iterator begin() const;

        /**
         * Returns an Iterator that represents the ending entry of the tree.  Used to identify when
         * tree traversal is complete.
         */
        Iterator end() const {
            return Iterator(this, nullptr);
        }

        /**
         * Returns an iterator to the first entry at a given XID that has a key that is greater than
         * or equal to the provided search_key.  Returns end() if there is no such entry.
         *
         * @param search_key The key we are searching for in the tree.
         * @param xid The XID at which we are searching.

         * @param for_update If true, then will return an iterator to the last element in the tree rather than end()
         */
        Iterator lower_bound(TuplePtr search_key, bool for_update = false) const;

        /**
         * Returns an iterator to the first entry at a given XID that has a key that is strictly
         * less than the provided search_key.  Returns end() if there is no such entry.
         *
         * @param search_key The key we are searching for in the tree.
         * @param xid The XID at which we are searching.
         */
        Iterator inverse_upper_bound(TuplePtr search_key) const;

        /**
         * Returns an iterator to the first entry at a given XID that has a key that is equal to the
         * provided search_key.  Returns end() if there is no such entry.
         *
         * @param search_key The key we are searching for in the tree.
         * @param xid The XID at which we are searching.
         */
        Iterator find(TuplePtr search_key) const;

        /**
         * Returns the leaf schema of this tree.
         */
        ExtentSchemaPtr get_schema() const
        {
            return _leaf_schema;
        }

    private:
        /** Inverted comparison to ensure XID map is sorted in descending order. */
        class ReverseCompare {
        public:
            bool operator()(const uint64_t &lhs, const uint64_t &rhs) const {
                return lhs > rhs;
            }
        };

        /** The path to the file. */
        std::filesystem::path _file;

        uint64_t _xid;

        /** The schema for the leaf nodes. */
        std::shared_ptr<ExtentSchema> _leaf_schema;
        FieldArrayPtr _leaf_keys; ///< The leaf fields that make up the key of the tree.

        /** The schema for the branch nodes. */
        std::shared_ptr<ExtentSchema> _branch_schema;
        FieldArrayPtr _branch_keys; ///< The branch fields that make up the key of the tree.
        FieldPtr _branch_child_f; ///< The branch field holding the child extent offset.

        StorageCache::PagePtr _root; ///< The root of the btree.

    private:
        /**
         * Reads a Page object from the cache for the provided extent ID.
         *
         * @param extent_id The ID of the extent to read.
         */
        StorageCache::PagePtr _read_extent(uint64_t extent_id) const;
    };

    typedef std::shared_ptr<BTree> BTreePtr;
}
