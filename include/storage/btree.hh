#pragma once

#include <shared_mutex>

#include <boost/container_hash/hash.hpp>

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
    public:
        typedef ObjectCache<std::pair<uint64_t, uint64_t>, Extent> ExtentCache;

        /** Inverted comparison to ensure XID map is in descending order. */
        class ReverseCompare {
        public:
            bool operator()(const uint64_t &lhs, const uint64_t &rhs) const {
                return lhs > rhs;
            }
        };

    private:
        /** The underlying data file. */
        std::shared_ptr<IOHandle> _handle;

        /** The ID of the file. */
        uint64_t _file_id;

        /** The column names of the sort keys. */
        std::vector<std::string> _keys;

        /** The schema for the leaf nodes. */
        std::shared_ptr<ExtentSchema> _leaf_schema;
        FieldArrayPtr _leaf_fields;
        FieldArrayPtr _leaf_keys;

        /** The schema for the branch nodes. */
        std::shared_ptr<ExtentSchema> _branch_schema;
        FieldArrayPtr _branch_keys;
        FieldPtr _branch_child_f;

        /** The roots of the tree.  Maps from XID -> Extent. */
        std::map<uint64_t, ExtentPtr, ReverseCompare> _roots;

        /** A cache for extents of the BTree. Maps from <file_id, extent_id> => Extent. */
        std::shared_ptr<ExtentCache> _cache;

    private:
        /** Reads an extent.  First checks the cache, then reads from disk. */
        ExtentPtr
        _read_extent(uint64_t extent_id)
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

    private:
        class Node {
        public:
            ExtentPtr extent;
            Extent::Iterator row_i;
            std::shared_ptr<Node> parent;

            Node(ExtentPtr e)
                : extent(e),
                  row_i(e->begin()),
                  parent(nullptr)
            { }

            Node(ExtentPtr e, Extent::Iterator i, std::shared_ptr<Node> p)
                : extent(e),
                  row_i(i),
                  parent(p)
            { }

            Node(const Node &n)
                : extent(n.extent),
                  row_i(n.row_i),
                  parent(n.parent)
            { }

            bool operator==(const Node& rhs) { return (extent == rhs.extent && row_i == rhs.row_i); }
        };

        ExtentPtr
        _find_root(uint64_t xid)
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

    public:
        class Iterator {
            friend BTree;

        private:
            BTree * const _btree;
            std::shared_ptr<Node> _node;
            uint64_t _xid;

            Iterator(BTree * const btree, std::shared_ptr<Node> node, uint64_t xid)
                : _btree(btree), _node(node), _xid(xid)
            { }

        public:
            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;  // or also value_type*
            using reference         = const Extent::Row &;  // or also value_type&

            reference operator*() const { return *(_node->row_i); }
            pointer operator->() { return &(*(_node->row_i)); }

            Iterator& operator++() {
                // can't iterate forward on end()
                assert(_node != nullptr);

                // move to the next row in the leaf extent
                ++_node->row_i;
                if (_node->row_i != _node->extent->end()) {
                    return *this;
                }

                // if at the end of the extent, traverse up the tree to find the next entry
                uint32_t depth = 0;

                // go up the tree
                while (_node->row_i == _node->extent->end()) {
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
                    ExtentPtr child = _btree->_read_extent(extent_id);

                    --depth;
                    _node = std::make_shared<Node>(child, child->begin(), _node);
                }

                // return the iterator
                return *this;
            }

            Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

            friend bool operator==(const Iterator& a, const Iterator& b) {
                return (a._node == nullptr && b._node == nullptr) || (*a._node == *b._node);
            }

            friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }

            /**
             * Returns the XID that this iterator is operating at.
             */
            uint64_t xid() const {
                return _xid;
            }
        };

    public:
        BTree(std::shared_ptr<IOHandle> handle,
              uint64_t file_id,
              const std::vector<std::string> &keys,
              std::shared_ptr<ExtentSchema> schema,
              std::shared_ptr<ExtentCache> cache,
              uint64_t min_xid,
              uint64_t root_offset=0)
            : _handle(handle),
              _file_id(file_id),
              _keys(keys),
              _leaf_schema(schema),
              _cache(cache)
        {
            // construct the field tuples for the leaf nodes
            _leaf_fields = _leaf_schema->get_fields();
            _leaf_keys = _leaf_schema->get_fields(keys);

            // construct the schema for the branches
            SchemaColumn child("child", 0, SchemaType::UINT64, false);
            _branch_schema = _leaf_schema->create_schema(keys, { child });

            // construct the field tuples for the branch nodes
            _branch_keys = _branch_schema->get_fields(keys);
            _branch_child_f = _branch_schema->get_field("child");

            // read the roots if a root exists
            if (root_offset) {
                ExtentPtr root;
                do {
                    root = _read_extent(root_offset);
                    _roots[root->header().xid] = root;

                    root_offset = root->header().prev_offset;
                } while (root->header().xid > min_xid && root_offset > 0);
            }
        }

        void add_root(uint64_t root_offset)
        {
            auto root = _read_extent(root_offset);
            _roots[root->header().xid] = root;
        }

        void set_min_xid(uint64_t min_xid)
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

        Iterator begin(uint64_t xid)
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

        Iterator end()
        {
            return Iterator(this, nullptr, 0);
        }

        Iterator lower_bound(TuplePtr search_key, uint64_t xid)
        {
            // find the correct root based on the XID
            auto root = _find_root(xid);
            if (root == nullptr || root->empty()) {
                return end();
            }

            // iterate through the levels until we find a leaf node
            ExtentPtr current = root;
            std::shared_ptr<Node> node = nullptr;
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

#if 0
        // XXX unclear we need upper_bound()
        Iterator upper_bound(TuplePtr search_key, uint64_t xid)
        {
            // find the correct root based on the XID
            auto root = _find_root(xid);
            if (root == nullptr || root->empty()) {
                return end();
            }

            // iterate through the levels until we find a leaf node
            ExtentPtr current = root;
            std::shared_ptr<Node> node = nullptr;
            while (current->type().is_branch()) {
                // upper-bound on the branch entries to find the correct child
                auto child_i = std::upper_bound(current->begin(), current->end(), search_key,
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
            auto leaf_i = std::upper_bound(current->begin(), current->end(), search_key,
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

#endif

        Iterator find(TuplePtr search_key, uint64_t xid)
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
    };


    /**
     * A b+tree constructed out of Extent objects.  Uses a FieldArray as the sort-key for the tree
     * which must be a subset of columns provided in a Schema for the BTree.  The leaf nodes of the
     * BTree contain the actual data specified by the Schema.  The branch nodes contain only the
     * sort-key columns, and an offset to the child extent.
     *
     * The MutableBTree is the object interface capable of modifying the btree through insert() and
     * remove() operations.  Note that it doesn't provide update() since that will be implemented
     * via a remove() + insert() since the full row is always provided in the replication log.  All
     * modifications are applied at a given XID which is specified via the set_xid() function.
     * Modifications must be applied in XID order to ensure that the modified extents represent a
     * valid snapshot.  Trying to apply changes from an XID that does not represent the currently
     * set XID will result in an error.  Similarly, trying to set the XID to an earlier XID will
     * also result in an error.
     *
     * It uses an internal page cache up to a specified maximum size.  Once modification operations
     * are complete, they must be flushed to disk using finalize(), which also returns the offset of
     * the new root of the tree.  After this operation, no further modifications can be made at the
     * current XID.
     *
     * The MutableBTree also provides O(log n) interfaces for find(), lower_bound(), and
     * upper_bound(), which all return forward iterators.
     */
    class MutableBTree {
    private:
        static const uint32_t MAX_EXTENT_SIZE = 128 * 1024;
        static const uint32_t MAX_EXTENT_COUNT = 16;

        /**
         * The Page objects represent a virtual Extent, which may hold either a single Extent
         * object, or potentially multiple Extent objects which are the result of split() operations
         * caused by modifications to the original Extent object.  Page objects may be marked as
         * dirty, indicating that they need to be written to a new location and update their parent
         * branch pointers accordingly.
         */
        class Page {
        public:
            class Iterator {
                friend Page;

            private:
                const Page *_page;
                std::vector<ExtentPtr>::iterator _extent_i;
                Extent::Iterator _row_i;

            private:
                Iterator(const Page *p, std::vector<ExtentPtr>::iterator e, Extent::Iterator r)
                    : _page(p), _extent_i(e), _row_i(r)
                { }

            public:
                using iterator_category = std::forward_iterator_tag;
                using difference_type   = std::ptrdiff_t;
                using value_type        = const Extent::Row;
                using pointer           = const Extent::Row *;  // or also value_type*
                using reference         = const Extent::Row &;  // or also value_type&

                reference operator*() const { return *(_row_i); }
                pointer operator->() { return &(*(_row_i)); }

                Iterator& operator++() {
                    ++_row_i;
                    if (_row_i == (*_extent_i)->end()) {
                        ++_extent_i;
                        if (_extent_i == _page->_extents.end()) {
                            return *this;
                        }

                        _row_i = (*_extent_i)->begin();
                    }

                    return *this;
                }

                Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

                friend bool operator==(const Iterator& a, const Iterator& b) {
                    return
                        (a._extent_i == a._page->_extents.end() &&
                         b._extent_i == b._page->_extents.end()) ||
                        (a._extent_i == b._extent_i &&
                         a._row_i == b._row_i);
                }

                friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }
            };

        private:
            /**
             * Checks if the extent has exceeded the max size.  If so, splits the extent in half.
             * Must have exclusive access to the page.
             */
            bool
            _check_split(std::vector<ExtentPtr>::iterator i)
            {
                ExtentPtr e = *i;

                // if the size of the extent is below the threshold, or it can't be split because
                // it's a single row, then return immediately
                if (e->byte_count() < MAX_EXTENT_SIZE || e->row_count() == 1) {
                    return false;
                }

                // perform the split if the referenced extent size exceeds the max
                auto &&pair = e->split();
                this->size -= e->byte_count();

                // remove the old extent
                i = _extents.erase(i);

                // insert the two new extents; insert() occurs before the provided iterator, so inserted in reverse order
                i = _extents.insert(i, pair.second);
                _extents.insert(i, pair.first);

                this->size += pair.first->byte_count() + pair.second->byte_count();
                return true;
            }

        public:
            /** For constructing an empty root. */
            Page(ExtentPtr extent,
                 MutableFieldArrayPtr key_fields)
                : extent_id(0),
                  type(false, true),
                  flushed(false),
                  _dirty(false),
                  _key_fields(key_fields),
                  _parent(nullptr)
            {
                _extents.push_back(extent);
                size = extent->byte_count();
                _prev_xid = extent->header().xid;
            }

            Page(uint64_t extent_id)
                : extent_id(extent_id)
            { }

            Page(uint64_t extent_id, ValueTuplePtr v, ExtentPtr e, MutableFieldArrayPtr key_fields)
                : extent_id(extent_id),
                  type(e->type()),
                  prev_key(v),
                  flushed(false),
                  _dirty(false),
                  _key_fields(key_fields)
            {
                _extents.push_back(e);
                size = e->byte_count();
                _prev_xid = e->header().xid;
            }

            Iterator
            begin()
            {
                return Iterator(this, _extents.begin(), _extents.front()->begin());
            }

            Iterator
            end()
            {
                return Iterator(this, _extents.end(), _extents.back()->end());
            }

            Extent::Row back() const
            {
                return _extents.back()->back();
            }

            Iterator
            lower_bound(TuplePtr search_key)
            {
                // find the extent that might contain the search key
                auto &&i = std::lower_bound(_extents.begin(), _extents.end(), search_key,
                                            [this](const ExtentPtr &extent, TuplePtr key) {
                                                return this->_key_fields->bind(extent->back())->less_than(key);
                                            });
                if (i == _extents.end()) {
                    return this->end();
                }

                // try to find the row within the extent
                ExtentPtr e = *i;
                auto &&row_i = std::lower_bound(e->begin(), e->end(), search_key,
                                                [this](const Extent::Row &row, TuplePtr key) {
                                                    return this->_key_fields->bind(row)->less_than(key);
                                                });
                if (row_i == e->end()) {
                    return this->end();
                }

                // construct an iterator to this entry
                return Iterator(this, i, row_i);
            }

            Iterator
            find(TuplePtr search_key)
            {
                // use lower_bound() to do a binary search for the entry
                auto &&i = this->lower_bound(search_key);

                // if the key is < the returned row, then there is no matching entry, return end()
                if (search_key < _key_fields->bind(*i)) {
                    return this->end();
                }

                // return the Iterator
                return i;
            }

            void update_branch(std::shared_ptr<Page> old_page,
                               std::vector<std::shared_ptr<Page>> new_pages,
                               MutableFieldPtr offset_f)
            {
                // remove the old child page from the entries
                // note: key should always exist since it's a pointer to an existing child
                auto &&page_i = this->find(old_page->prev_key);
                assert(page_i != this->end());
                ExtentPtr e = *(page_i._extent_i);

                // remove the old page while updating the page size
                this->size -= e->byte_count();
                e->remove(page_i._row_i);
                this->size += e->byte_count();

                // mark the page dirty
                _dirty = true;

                // add the new child pages to the entries
                // note: add them in reverse order so we can keep inserting at the same position
                for (auto &&i = new_pages.rbegin(); i != new_pages.rend(); i++) {
                    auto &&page = *i;

                    // add an entry at the current position
                    this->size -= e->byte_count();
                    Extent::MutableRow row = e->insert(page_i._row_i);

                    _key_fields->bind(row)->assign(page->prev_key);
                    offset_f->set_uint64(row, page->extent_id);

                    this->size += e->byte_count();
                    
                    // check if we need to split the extent
                    bool did_split = _check_split(page_i._extent_i);
                    if (did_split) {
                        // re-find the insert position
                        page_i = this->find(page->prev_key);
                    }
                }
            }

            /**
             * Adds an entry to this page.  Assumes that the page is locked for exclusive access.
             */
            void insert(TuplePtr search_key,
                        TuplePtr value,
                        MutableFieldArrayPtr fields)
            {
                // find the position to perform the insert
                auto &&i = this->lower_bound(search_key);

                // insert a row into the extent
                ExtentPtr e = *(i._extent_i);
                uint32_t old_size = e->byte_count();
                Extent::MutableRow row = e->insert(i._row_i);
                uint32_t new_size = e->byte_count();

                // update the size of the page
                this->size = this->size - old_size + new_size;

                // save the value into the row
                fields->bind(row)->assign(value);

                // mark this page as dirty
                _dirty = true;

                // perform a split of the extent if necessary
                _check_split(i._extent_i);
            }
            
            void
            remove(TuplePtr search_key)
            {
                // find the position to perform the remove
                auto &&i = this->find(search_key);
                if (i == this->end()) {
                    return; // no entry to remove
                }
                ExtentPtr e = *(i._extent_i);

                // temporarily remove the size of this extent from the page size
                this->size -= e->byte_count();

                // remove the row from the extent
                // note: if this needs to remove all matching rows, then additional logic is required
                e->remove(i._row_i);

                // mark this page as dirty
                _dirty = true;

                // update the page size
                this->size += e->byte_count();

                // if the extent is empty, remove it from the set
                if (e->empty()) {
                    if (_extents.size() > 1) {
                        _extents.erase(i._extent_i);
                    }
                }

                // XXX re-merge extents if possible?
                // note: we don't perform cross-Page merge here as it's handled during GC vacuum
                // check if the size of this extent and the one before / after would make the threshold for a single extent
            }

            /**
             * Flushes the contents of the page and updates the parent with the new pages.  Assumes
             * that the caller is holding an exclusive lock on both this page and it's parent and
             * that neither are marked flushed.
             */
            std::vector<std::shared_ptr<Page>>
            flush(std::shared_ptr<IOHandle> handle, uint64_t xid)
            {
                std::vector<PagePtr> new_pages;

                // write all of the extents in parallel using async_flush()
                std::vector<std::future<std::shared_ptr<IOResponseAppend>>> futures;
                for (auto &&e : _extents) {
                    // set the extent header correctly
                    e->header().xid = xid;
                    e->header().prev_offset = this->extent_id;
                    e->header().type = this->type;

                    futures.push_back(e->async_flush(handle));
                }

                // as the writes complete, create the new pages
                for (int i = 0; i < futures.size(); i++) {
                    auto &&response = futures[i].get();

                    ValueTuplePtr key = std::make_shared<ValueTuple>(_key_fields->bind(_extents[i]->back()));
                    PagePtr page = std::make_shared<Page>(response->offset, key, _extents[i], _key_fields);

                    new_pages.push_back(page);
                }

                return new_pages;
            }

            std::shared_ptr<Page>
            parent()
            {
                std::unique_lock lock(_children_mutex);
                return _parent;
            }

            void
            set_parent(std::shared_ptr<Page> parent)
            {
                std::unique_lock lock(_children_mutex);
                _parent = parent;
            }

            void
            add_child(std::shared_ptr<Page> child)
            {
                std::unique_lock lock(_children_mutex);
                _children[child->extent_id] = child;
            }

            void
            remove_child(uint64_t extent_id)
            {
                std::unique_lock lock(_children_mutex);
                _children.erase(extent_id);
            }

            bool
            has_children()
            {
                std::unique_lock lock(_children_mutex);
                return !_children.empty();
            }

            std::shared_ptr<Page>
            first_child() const
            {
                // note: must be called when there are children
                assert(!_children.empty());

                std::unique_lock lock(_children_mutex);
                return _children.begin()->second;
            }

            bool
            check_flush() const
            {
                return (_extents.size() > MAX_EXTENT_COUNT);
            }

            bool
            empty() const
            {
                return (_extents.size() == 1 && _extents.front()->empty());
            }

            bool
            is_dirty() const
            {
                return _dirty;
            }

            uint64_t
            xid() const
            {
                return _prev_xid;
            }

            void
            set_extent(ExtentPtr extent,
                       MutableFieldArrayPtr key_fields)
            {
                // note: page should be empty when this is called
                assert(_extents.size() == 0);

                // put the extent into the list
                _extents.push_back(extent);
                this->size = extent->byte_count();

                // set the type based on the extent header
                this->type = extent->type();

                // set the old XID based on the extent header
                _prev_xid = extent->header().xid;

                // set the key fields for the page
                _key_fields = key_fields;

                // if the page is not the root then we store the key of the entry we used to find this page
                if (!this->type.is_root()) {
                    // force a copy of the values into a ValueTuple
                    this->prev_key = std::make_shared<ValueTuple>(key_fields->bind(extent->back()));
                }
            }

        public:
            uint64_t extent_id; ///< The offset of the previous extent this page is based on.  Won't change once the page is fully constructed.
            ExtentType type; ///< The type of this page.  Won't change once the page is fully constructed, so safe to read without holding the mutex.
            ValueTuplePtr prev_key; ///< The key in the parent of the previous extent this page is based on.  Won't change once the page is fully constructed.

            // the following are mutex protected
            mutable std::shared_mutex mutex; ///< A shared mutex for lock management.
            bool flushed; ///< This page has already been flushed to disk and replaced with a new page.
            uint32_t size; ///< The size of the page.

        private:
            uint64_t _prev_xid; ///< The xid of the original extent this page is based on.
            bool _dirty; ///< Flag indicating if data has been modified since the last write.
            MutableFieldArrayPtr _key_fields; ///< The fields representing the key in the btree.
            std::vector<ExtentPtr> _extents; ///< A list of extents that will be written out when this page is flushed.

            // the following are protected by a separate mutex, must be holding the primary mutex at least shared.
            mutable std::shared_mutex _children_mutex; ///< A mutex to protect the map of children.  Can be acquired unique while sharing the primary mutex.
            std::map<uint64_t, std::shared_ptr<Page>> _children; ///< The set of children of this page that are part of a modified path through the tree.
            std::shared_ptr<Page> _parent; ///< Pointer to the parent page.  Won't change once the page is fully constructed.
        };

        /** Pointer typedef for MutableBTree::Page. */
        typedef std::shared_ptr<Page> PagePtr;

        /**
         * Cache of Page objects.  Works with the BTree page locks to enable thread-safe access.
         */
        class PageCache {
        public:
            /** Cache entry typedef, tuple of PagePtr, LRU iterator, usage count, page size */
            typedef std::tuple<PagePtr, typename std::list<PagePtr>::iterator, int, uint32_t> LookupEntry;

            std::unordered_map<uint64_t, LookupEntry, boost::hash<uint64_t>> lookup;
            std::list<PagePtr> lru;

            uint64_t size;
            uint64_t max_size;

            std::shared_mutex mutex;

        public:
            PageCache(uint64_t max_size)
                : size(0),
                  max_size(max_size)
            { }
        };

    private:
        PageCache _cache;
        std::shared_ptr<IOHandle> _handle;
        std::vector<std::string> _sort_keys;

        mutable std::shared_mutex _mutex;
        PagePtr _root;
        uint64_t _xid;
        bool _finalized;

        /** The schema for the leaf nodes. */
        std::shared_ptr<ExtentSchema> _leaf_schema;
        MutableFieldArrayPtr _leaf_fields;
        MutableFieldArrayPtr _leaf_keys;

        /** The schema for the branch nodes. */
        std::shared_ptr<ExtentSchema> _branch_schema;
        MutableFieldArrayPtr _branch_keys;
        MutableFieldPtr _branch_child_f;

    private:
        struct Node {
            const std::shared_ptr<Node> parent;
            const PagePtr page;

            Node(std::shared_ptr<Node> parent, PagePtr page)
                : parent(parent),
                  page(page)
            { }
        };
        typedef std::shared_ptr<Node> NodePtr;

    private:
        //// CACHE OPERATIONS

        PagePtr
        _cache_get(uint64_t extent_id)
        {
            // find the entry if it exists
            auto &&i = _cache.lookup.find(extent_id);
            if (i == _cache.lookup.end()) {
                return nullptr;
            }
            PageCache::LookupEntry &entry = i->second;

            // remove the page from the LRU list if on the list
            if (std::get<2>(entry) == 0) {
                _cache.lru.erase(std::get<1>(entry));
            }

            // increment the usage counter
            ++std::get<2>(entry);

            // return the page
            return std::get<0>(entry);
        }

        void
        _cache_release(PagePtr page)
        {
            // find the entry; must exist in the cache since can't be chosen for eviction while it is in use
            auto &&i = _cache.lookup.find(page->extent_id);
            assert(i != _cache.lookup.end());

            // decrement the usage count
            int &usage = std::get<2>(i->second);
            --usage;

            // if zero then add to the LRU list
            if (usage == 0) {
                _cache.lru.push_front(page);
                std::get<1>(i->second) = _cache.lru.begin();
            }
        }

        void
        _cache_insert_empty(PagePtr page)
        {
            // place the page into the cache, but not into the LRU queue
            _cache.lookup.insert_or_assign(page->extent_id, PageCache::LookupEntry(page, _cache.lru.end(), 1, 0));
        }

        void
        _cache_insert(PagePtr page, std::unique_lock<std::shared_mutex> &cache_lock)
        {
            // place the page into the cache, but not into the LRU queue
            _cache_insert_empty(page);

            // update the size of the cache
            _cache_update_size(page, cache_lock);
        }

        void
        _cache_update_size(PagePtr page, std::unique_lock<std::shared_mutex> &cache_lock)
        {
            // update the size of the cache given the page size
            auto &&i = _cache.lookup.find(page->extent_id);
            assert(i != _cache.lookup.end());
            PageCache::LookupEntry &entry = i->second;

            // update the size of the cache based on the new size of the entry
            _cache.size = _cache.size - std::get<3>(entry) + page->size;

            // evict pages until the size is under the watermark
            while (_cache.size > _cache.max_size) {
                // if somehow there are no evictable pages, print a warning and return
                if (_cache.lru.empty()) {
                    // XXX print warning
                    std::cout << "ERROR" << std::endl;
                    return;
                }

                // get a page
                // note: coming off the LRU we know that no one else is currently using this page
                PagePtr page = _cache.lru.back();
                _cache.lru.pop_back();

                // check if we need to try another entry
                // note: if the page is the root we can't evict; if the page has potentially dirty
                //       children, must evict them first
                if (page->type.is_root() || page->has_children()) {
                    _cache.lru.push_front(page);
                    std::get<1>(entry) = _cache.lru.begin();
                    continue;
                }

                // check if we can remove immediately
                // note: if the page was already flushed, or has never been accessed since being
                //       created from a flush, then can evict immedately
                if (page->flushed || !page->parent()) {
                    _cache.lookup.erase(page->extent_id);
                    _cache.size -= page->size;
                    continue;
                }

                // currently looks evictable and requires a flush; try to acquire the parent page lock
                std::unique_lock parent_lock(page->parent()->mutex, std::try_to_lock);

                // if we weren't able to lock the parent, try another page
                // XXX should we just block until we get the lock? there's a potential live-lock
                //     here from never being able to get a page
                if (!parent_lock.owns_lock()) {
                    _cache.lru.push_front(page);
                    std::get<1>(entry) = _cache.lru.begin();
                    continue;
                }

                // lock the page; should never block since the page isn't being used
                std::unique_lock page_lock(page->mutex, std::try_to_lock);
                assert(page_lock.owns_lock());

                // we know we will proceed with the flush at this point, so can update the cache size
                _cache.size -= page->size;
                    
                // unlock the cache for others to proceed
                cache_lock.unlock();

                // flush the page
                // note: we don't cache the newly created pages since we are trying to free space
                _flush_page_internal(page, page->parent());

                // re-lock the cache
                cache_lock.lock();

                // evict the page from the cache and continue
                _cache_evict(page->extent_id);
            }
        }

        void
        _cache_evict(uint64_t extent_id)
        {
            // find the entry if it exists
            auto &&i = _cache.lookup.find(extent_id);
            if (i == _cache.lookup.end()) {
                return;
            }

            // retrieve the value from the lookup entry
            PageCache::LookupEntry &entry = i->second;
            PagePtr page = std::get<0>(entry);

            // note: no need to flush the page since a direct eviction should only come after
            //       flushing a page; also, page shouldn't be on LRU list since it is in-use by
            //       the caller
            assert(page->flushed);
            assert(std::get<2>(entry) > 0);

            // remove the entry from the cache
            _cache.size -= std::get<3>(entry);
            _cache.lookup.erase(i);
        }


        //// INTERNAL OPERATIONS

        void
        _read_page_internal(PagePtr page)
        {
            // now populate the page
            auto response = _handle->read(page->extent_id);

            // unpack the header to determine the extent type
            ExtentHeader header(response->data[0]);

            // construct the extent
            ExtentPtr extent;
            MutableFieldArrayPtr key_fields;
            if (header.type.is_branch()) {
                extent = std::make_shared<Extent>(_branch_schema, response->data);
                key_fields = _branch_keys;
            } else {
                extent = std::make_shared<Extent>(_leaf_schema, response->data);
                key_fields = _leaf_keys;
            }

            // put the extent data into the page
            page->set_extent(extent, key_fields);
        }

        /**
         * Read an extent from disk and store it as a Page in the cache.
         */
        PagePtr
        _read_page(uint64_t extent_id,
                   PagePtr parent,
                   std::shared_lock<std::shared_mutex> &parent_lock,
                   std::unique_lock<std::shared_mutex> &leaf_lock)
        {
            // note: when entering, parent_lock is held and leaf_lock not held

            // lock the cache
            std::unique_lock cache_lock(_cache.mutex);

            // check if the entry already exists
            PagePtr page = _cache_get(extent_id);
            if (page != nullptr) {
                // get the appropriate lock
                if (page->type.is_branch()) {
                    std::shared_lock child_lock(page->mutex);

                    // update the parent/child inter-link; may already be set but won't change
                    page->set_parent(parent);
                    parent->add_child(page);

                    // release the parent page back to the cache
                    _cache_release(parent);

                    // release the parent lock and replace with child lock
                    parent_lock = std::move(child_lock);
                } else {
                    leaf_lock = std::unique_lock(page->mutex);

                    // update the parent/child inter-link; may already be set but won't change
                    page->set_parent(parent);
                    parent->add_child(page);

                    // release the parent page back to the cache
                    _cache_release(parent);

                    // release the parent lock
                    parent_lock.unlock();
                }

                return page;
            }

            // no page in the cache, so construct a page that we can populate
            page = std::make_shared<Page>(extent_id);

            // get an exclusive lock the new page
            std::unique_lock page_lock(page->mutex);

            // place the page into the cache, but not into the LRU queue
            // note: this placeholder could be found by other threads, but will block since we will
            //       hold an exclusive lock on the page
            _cache_insert_empty(page);

            // update the page book-keeping
            page->set_parent(parent);
            parent->add_child(page);

            // release the parent page back to the cache
            _cache_release(parent);

            // release the parent lock
            parent_lock.release();

            // release the cache for other threads until we have populated the data for this page
            // note: the exclusive lock on the page will prevent others from progressing even if
            //       they try to access this page
            cache_lock.unlock();

            // read extent from disk and update the page metadata
            _read_page_internal(page);
             
            // now re-acquire the cache lock to update the size of the cache
            cache_lock.lock();

            // update the size of the page in the cache; may cause evictions
            _cache_update_size(page, cache_lock);

            // check which kind of lock we need to hold on the page
            if (page->type.is_branch()) {
                page_lock.unlock();
                parent_lock = std::shared_lock(page->mutex);
            } else {
                leaf_lock = std::move(page_lock);
            }

            // return the page
            return page;
        }

        /**
         * Find the leaf that *could* hold the provided key.  This will always return a pointer to a
         * Page since it is used for key insertion.
         */
        // XXX we could (1) pass in the base Node here to allow search starting from any node and (2) use a template for the lock type... these would allow us to use this for read iteration
        NodePtr _find_leaf(TuplePtr key, std::unique_lock<std::shared_mutex> &leaf_lock)
        {
            // safe because the root pointer can't change if we are holding the btree lock
            NodePtr node = std::make_shared<Node>(nullptr, _root);

            // if the root is the leaf, lock and return
            if (_root->type.is_leaf()) {
                leaf_lock = std::unique_lock(_root->mutex);
                return node;
            }

            // otherwise, get a shared lock and traverse
            std::shared_lock lock(_root->mutex);

            // iterate through the levels until we find a leaf page
            while (node->page->type.is_branch()) {
                // use a lower_bound() check to find the appropriate child branch
                auto &&i = node->page->lower_bound(key);

                // note: we know the tree isn't empty since the root would be a leaf in that case
                Extent::Row row = (i == node->page->end()) ? node->page->back() : *i;

                // retrieve the child offset
                uint64_t extent_id = _branch_child_f->get_uint64(row);

                // read the child page; will handle updating the locks
                PagePtr child = _read_page(extent_id, node->page, lock, leaf_lock);

                // handle the locking based on the page type
                if (child->type.is_branch()) {
                    // acquire shared lock on the child
                    std::shared_lock child_lock(child->mutex);

                    // update the parent/child inter-link; may already be set but won't change
                    child->set_parent(node->page);
                    node->page->add_child(child);

                    // release the parent back to the cache for eventual eviction
                    _cache_release(node->page);

                    // unlock the parent and shift to the child
                    lock.unlock();
                    lock = std::move(child_lock);
                } else {
                    leaf_lock = std::unique_lock(child->mutex);

                    // update the parent/child inter-link; may already be set but won't change
                    child->set_parent(node->page);
                    node->page->add_child(child);

                    // release the parent back to the cache for eventual eviction
                    _cache_release(node->page);
                }

                // recurse to the child
                node = std::make_shared<Node>(node, child);
            }

            // releases the direct parent's shared lock here
            return node;
        }

        std::vector<PagePtr>
        _flush_page_internal(PagePtr page,
                             PagePtr parent)
        {
            // flush the page's extent(s) to disk
            // note: it would be nice if we could perform the disk writes without holding the parent
            //       mutex, but then there is a potential race condition between updating the
            //       parent's pointers and flushing the parent.  It might be possible with more
            //       complex logic.
            std::vector<PagePtr> &&new_pages = page->flush(_handle, _xid);

            // update the parent's pointers
            parent->update_branch(page, new_pages, _branch_child_f);

            // remove the page from the parent's children
            parent->remove_child(page->extent_id);

            // mark the page as flushed
            page->flushed = true;

            return new_pages;
        }

        void
        _flush_page(PagePtr page, PagePtr parent)
        {
            // acquire exclusive access to the page
            std::unique_lock page_lock(page->mutex);

            // if the page was already flushed, then nothing to do
            if (page->flushed) {
                return;
            }

            // flush the children first
            // note: safe to traverse the children like this because we are holding exclusive lock
            //       on the page
            while (page->has_children()) {
                PagePtr child = page->first_child();

                // this will remove the child from the page's children during the flush
                // note: we are currently holding an exlusive lock on the child's parent (this
                //       page), so safe to call
                _flush_page(child, page);
            }

            // if the page was not modified while flushing children, then no need to flush it
            // note: this could occur if pages were accessed for read
            if (!page->is_dirty()) {
                return;
            }

            // check if the page is empty
            if (page->empty()) {
                // if so then we need to process a removal on the parent
                _remove_page_internal(page, parent);

                // evict the removed page from the cache
                std::unique_lock cache_lock(_cache.mutex);
                _cache_evict(page->extent_id);
            } else {
                // perform the actual page flush
                std::vector<PagePtr> &&new_pages = _flush_page_internal(page, parent);

                // evict the page from the cache and add the new pages
                std::unique_lock cache_lock(_cache.mutex);

                _cache_evict(page->extent_id);
                for (auto &&new_page : new_pages) {
                    _cache_insert(new_page, cache_lock);
                }
            }
        }

        PagePtr
        _flush_root(PagePtr page)
        {
            // acquire exclusive access to the page
            std::unique_lock page_lock(page->mutex);

            // if the page was flushed, then nothing to do
            if (page->flushed) {
                return nullptr;
            }

            // flush the children first
            // note: safe to traverse the children like this because we are holding
            //       exclusive lock on the page
            while (page->has_children()) {
                PagePtr child = page->first_child();

                // this will remove the child from the page's children during the flush
                // note: we are currently holding an exlusive lock on the child's parent (this
                //       page), so safe to call
                _flush_page(child, page);
            }

            // check if this page has actually been modified by the child flushes
            if (!page->is_dirty()) {
                return page;
            }

            // flush the page's extent(s) to disk
            // note: it would be nice if we could perform the disk writes without holding the parent
            //       mutex, but then there is a potential race condition between updating the
            //       parent's pointers and flushing the parent.  It might be possible with more
            //       complex logic.
            std::vector<PagePtr> &&new_pages = page->flush(_handle, _xid);

            // check if we need to create a new root above this page
            PagePtr new_root;
            if (new_pages.size() > 1) {
                // construct the new root's extent
                ExtentPtr extent = std::make_shared<Extent>(_branch_schema, ExtentType(true, true), _xid);

                // add pointers to the new root for each new page
                for (PagePtr child : new_pages) {
                    Extent::MutableRow row = extent->append();
                    _branch_keys->bind(row)->assign(_branch_keys->bind(child->back()));
                }

                // write the new extent to disk
                auto &&future = extent->async_flush(_handle);

                // construct a new root page based on the new extent
                uint64_t extent_id = future.get()->offset;
                ValueTuplePtr key = std::make_shared<ValueTuple>(_branch_keys->bind(extent->back()));
                PagePtr new_root = std::make_shared<Page>(extent_id, key, extent, _branch_keys);

                // note: we don't need to add these new pages as children because they are clean
                //       pages that haven't been traversed for modification
            } else {
                // if there's just one page, it will replace the root
                new_root = new_pages[0];
                new_pages.pop_back();
            }

            // mark the old root as flushed
            page->flushed = true;

            // evict the old root from the cache and add the new pages
            // note: no one should be using this page since we have exclusive lock on both the
            //       parent and the page, meaning no one is holding the page and no one could have
            //       found the page again after releasing it
            std::unique_lock cache_lock(_cache.mutex);

            _cache_evict(page->extent_id);
            for (auto &&new_page : new_pages) {
                _cache_insert(new_page, cache_lock);
            }

            // add the new root to the cache
            _cache_insert(new_root, cache_lock);

            return new_root;
        }

        void
        _lock_and_flush_root(PagePtr root)
        {
            // lock the btree for exclusive access
            std::unique_lock lock(_mutex);

            // check if this is still the root of the tree or if it's already been flushed
            if (root != _root) {
                return;
            }

            // now flush the root
            PagePtr new_root = _flush_root(root);

            // set the root pointer for the BTree
            // note; if returned null then was flushed by another thread
            if (new_root != nullptr) {
                _root = new_root;
            }
        }

        void
        _lock_and_flush_page(NodePtr node)
        {
            // acquire exclusive access to the parent page
            PagePtr parent = node->parent->page;
            std::unique_lock lock(parent->mutex);

            // if the parent has already been flushed, then by definition this page was also flushed
            if (parent->flushed) {
                return;
            }

            // now that we are holding the parent, flush the page
            _flush_page(node->page, parent);

            // check if the parent needs to be flushed
            if (parent->check_flush()) {
                lock.unlock();

                // check if the parent being flushed is the root of the tree
                if (parent == nullptr) {
                    // will re-acquire an exclusive lock on tree and then flush the root and update it
                    _lock_and_flush_root(parent);
                } else {
                    // will lock the parent's parent and then lock and flush the parent
                    _lock_and_flush_page(node->parent);
                }
            } else {
                // otherwise, update the parent's size in the cache
                std::unique_lock cache_lock(_cache.mutex);
                _cache_update_size(parent, cache_lock);
            }
        }

        void
        _remove_page_internal(PagePtr page,
                              PagePtr parent)
        {
            // remove the page from the parent
            parent->remove(page->prev_key);

            // mark the page flushed
            page->flushed = true;
        }

        void
        _remove_page(PagePtr page,
                     PagePtr parent)
        {
            // acquire exclusive access to the page
            std::unique_lock page_lock(page->mutex);

            // if the page was already flushed / removed, then do nothing
            if (page->flushed) {
                return;
            }

            // make sure the page still should be removed, there may have been another thread that
            // added an entry before the page was re-locked
            if (!page->empty()) {
                return;
            }

            // perform the removal
            _remove_page_internal(page, parent);

            // evict the page from the cache
            std::unique_lock cache_lock(_cache.mutex);
            _cache_evict(page->extent_id);
        }

        void
        _lock_and_remove_page(NodePtr node)
        {
            // acquire exclusive access to the parent page
            PagePtr parent = node->parent->page;
            std::unique_lock lock(parent->mutex);

            // if the parent was flushed, then the removal would have been processed during the
            // page's flush() operation
            if (parent->flushed) {
                return;
            }

            // now that we are holding the parent, try to remove the page
            _remove_page(node->page, parent);

            // the parent would have gotten smaller, so no need to flush, but if the parent is empty
            // then we can remove it as well
            if (parent->empty()) {
                lock.unlock();

                if (node->parent->parent != nullptr) {
                    _lock_and_remove_page(node->parent);
                }
            }
        }

    public:
        MutableBTree(std::shared_ptr<IOHandle> handle,
                     const std::vector<std::string> &keys,
                     uint64_t root_offset,
                     uint64_t cache_size,
                     std::shared_ptr<ExtentSchema> schema,
                     uint64_t xid)
            : _cache(cache_size),
              _handle(handle),
              _sort_keys(keys),
              _finalized(true),
              _leaf_schema(schema)
        {
            // construct the field tuples for the leaf nodes
            _leaf_fields = _leaf_schema->get_mutable_fields();
            _leaf_keys = _leaf_schema->get_mutable_fields(keys);

            // construct the schema for the branches
            SchemaColumn child("child", 0, SchemaType::UINT64, false);
            _branch_schema = _leaf_schema->create_schema(keys, { child });

            // construct the field tuples for the branch nodes
            _branch_keys = _branch_schema->get_mutable_fields(keys);
            _branch_child_f = _branch_schema->get_mutable_field("child");

            // read the root page
            if (root_offset) {
                // construct an empty page to populate
                _root = std::make_shared<Page>(root_offset);

                // read the root
                _read_page_internal(_root);

                // add the root to the cache
                std::unique_lock cache_lock(_cache.mutex);
                _cache_insert(_root, cache_lock);

                // note: passed XID should match the XID from the extent header
                assert(_root->xid() == xid);
            } else {
                // construct an empty extent at the provided XID
                ExtentPtr extent = std::make_shared<Extent>(schema, ExtentType(false, true), xid);

                // create an empty root page
                _root = std::make_shared<Page>(extent, _leaf_keys);
            }

            // set the XID based on the root
            _xid = _root->xid();
        }

        /**
         * Mark the BTree as making modifications for a given target XID.
         */
        void set_xid(uint64_t xid)
        {
            // acquire an exclusive lock on the tree
            std::unique_lock lock(_mutex);

            // update the target XID and remove the finalized flag
            _xid = xid;
            _finalized = false;
        }

        /**
         * Inserts a new entry into the tree.
         */
        void insert(TuplePtr value)
        {
            // acquire a shared lock on the btree
            std::shared_lock tree_lock(_mutex);

            // make sure that we can modify this tree
            assert(!_finalized);

            // get the search key for this value
            TuplePtr search_key = _leaf_schema->tuple_subset(value, _sort_keys);

            // traverse the tree to find the correct leaf page along with the pages along the path
            // to it from the root, get exclusive lock on the leaf
            std::unique_lock<std::shared_mutex> lock;
            NodePtr node = _find_leaf(search_key, lock);

            // once we've traversed the tree we can release the tree lock
            tree_lock.unlock();

            // insert the value into the page
            node->page->insert(search_key, value, _leaf_fields);

            // check if this page needs to be flushed
            if (node->page->check_flush()) {
                // unlock the page
                lock.unlock();

                // check if this is the root
                if (node->parent == nullptr) {
                    // will exclusive lock the tree and flush the root
                    _lock_and_flush_root(node->page);
                } else {
                    // will first lock the parent, then lock the page and flush it
                    _lock_and_flush_page(node);
                }
            } else {
                // need to update the cache with the updated size of the page
                std::unique_lock cache_lock(_cache.mutex);
                _cache_update_size(node->page, cache_lock);
            }
        }

        /**
         * Removes the entry at the provided position.
         */
        void remove(TuplePtr value)
        {
            // acquire a shared lock on the btree
            std::shared_lock tree_lock(_mutex);

            // make sure that we can modify this tree
            assert(!_finalized);

            // get the search key for this value
            TuplePtr search_key = _leaf_schema->tuple_subset(value, _sort_keys);
            // auto &&search_key = value.get_fields(_sort_key);

            // traverse the tree to find the appropriate page to insert into
            std::unique_lock<std::shared_mutex> lock;
            NodePtr node = _find_leaf(search_key, lock);

            // once we've traversed the tree we can release the tree lock
            tree_lock.unlock();

            // insert the value into the page
            node->page->remove(value);

            // check if this page needs to be removed
            if (node->page->empty()) {
                // unlock the page
                lock.unlock();

                // check if this is a non-root page
                if (node->parent != nullptr) {
                    _lock_and_remove_page(node);
                }
            } else {
                // need to update the cache with the updated size of the page
                std::unique_lock cache_lock(_cache.mutex);
                _cache_update_size(node->page, cache_lock);
            }
        }

        /**
         * Commits all changes to disk, marking all of the written extents as being valid at the
         * current XID for the BTree -- meaning that all modifications for that XID have been
         * completed.  Further operations on the BTree without setting the XID forward should result
         * in errors.
         */
        void finalize()
        {
            // lock the tree
            std::unique_lock lock(_mutex);

            // flush from the root of the tree
            _lock_and_flush_root(_root);

            // mark the tree as finalized so that additional changes can't be made at this XID
            _finalized = true;
        }
    };
}




#if 0
    public:
        class Iterator {
        private:
            const MutableBTree * const _btree;
            std::shared_ptr<Node> _node;

        public:
            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;  // or also value_type*
            using reference         = const Extent::Row &;  // or also value_type&

            reference operator*() const { return *(_node->row); }
            pointer operator->() { return &(*(_node->row)); }

            Iterator& operator++() {
                ++(_node->row);
                if (_node->row != _node->page->end()) {
                    return *this;
                }
                    
                // if at the end of the extent, traverse up the tree to find the next entry
                uint32_t depth = 0;

                // go up the tree
                while (_node->row == _node->page->end()) {
                    // iterate up to the parent
                    ++depth;
                    _node = _node->parent;

                    // if we were at the end of the root extent, then no more entries
                    if (_node == nullptr) {
                        return *this;
                    }

                    // move to the next entry in the parent
                    ++(_node->row);
                }

                // now go back down the tree
                while (depth > 0) {
                    // read the child's extent ID
                    uint64_t extent_id = _btree->_branch_child_f->get_uint64(*(_node->row));

                    // read the child extent
                    PagePtr child = _btree->_read_page(extent_id);

                    --depth;
                    _node = std::make_shared<Node>(child, _node, child->begin());
                }

                return *this;
            }

            Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

            friend bool operator==(const Iterator& a, const Iterator& b) {
                return
                    (a._node == nullptr && b._node == nullptr) ||
                    (a._node->page == a._node->page &&
                     a._node->row == a._node->row);
            }

            friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }
        };

#endif
