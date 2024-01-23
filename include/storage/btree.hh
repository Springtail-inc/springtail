#pragma once

namespace springtail {

    /**
     * A read-only b+tree constructed out of Extent objects.  Uses a FieldTuple as the sort-key for
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
            bool operator()(const uint64_t &lhs, const uint64_t &rhs) {
                return lhs > rhs;
            }
        };

    private:
        /** The underlying data file. */
        IOHandle _handle;

        /** The ID of the file. */
        uint64_t _file_id;

        /** A cache for extents of the BTree. Maps from <file_id, extent_id> => Extent. */
        std::shared_ptr<ExtentCache> _cache;

        /** The column names of the sort keys. */
        std::vector<std::string> _keys;

        /** The schema for the leaf nodes. */
        std::shared_ptr<ExtentSchema> _leaf_schema;
        FieldTuple &_leaf_fields;
        FieldTuple &_leaf_keys;

        /** The schema for the branch nodes. */
        std::shared_ptr<ExtentSchema> _branch_schema;
        FieldTuple &_branch_keys;
        std::shared_ptr<Field> _branch_child_f;

        /** The roots of the tree.  Maps from XID -> Extent. */
        std::map<uint64_t, ExtentPtr, ReverseCompare> _roots;

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
            auto response = _handle.read(extent_id);

            // unpack the header to determine the extent type
            ExtentHeader header(response.data[0]);

            // construct the extent
            if (header.type == ExtentType::BRANCH) {
                extent = std::make_shared<Extent>(_branch_schema, response.data);
            } else {
                extent = std::make_shared<Extent>(_leaf_schema, response.data);
            }

            // store into the cache
            _cache->insert(cache_id, extent, extent->size());

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
        private:
            const Btree * const _btree;
            std::shared_ptr<Node> _node;
            uint64_t _xid;

            Iterator(const Btree * const btree, std::shared_ptr<Node> node, uint64_t xid)
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
                    _node = std::make_shared<Node>(child, _node, child->begin());
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
        BTree(const std::filesystem::path &file,
              uint64_t file_id,
              std::shared_ptr<Schema> schema,
              const std::vector<std::string> &keys,
              uint64_t root_offset=0,
              std::shared_ptr<ExtentCache> cache,
              uint64_t min_xid)
            : _file_id(file_id),
              _leaf_schema(schema),
              _keys(keys),
              _cache(cache)
        {
            // open the file handle
            _handle = IOMgr::get_instance()->open(file, IOMgr::READ, true);

            // construct the field tuples for the leaf nodes
            _leaf_fields = _leaf_schema->get_fields();
            _leaf_keys = _leaf_schema->get_fields(keys);

            // construct the schema for the branches
            SchemaColumn child("child", 0, SchemaType::UINT64, false);
            _branch_schema = _leaf_schema->create_schema(sort_keys, { child });

            // construct the field tuples for the branch nodes
            _branch_keys = _branch_schema->get_fields(keys);
            _branch_child_f = _branch_schema->get_field("child");

            // read the roots if a root exists
            if (root_offset) {
                do {
                    auto root = _read_extent(root_offset);
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
            while (node->extent->header().type != ExtentType::LEAF) {
                // get the offset for the child
                uint64_t child_id = _branch_child_f->get_uint64(*(node->row_i));

                // read the extent
                ExtentPtr child = _read_extent(child_id);

                // create a node for the child an move to it
                node = std::make_shared<Node>(child, child->begin(), node);
            }

            return Iterator(this, node);
        }

        Iterator end()
        {
            return Iterator(this, nullptr);
        }

        Iterator lower_bound(const Tuple &search_key, uint64_t xid)
        {
            // find the correct root based on the XID
            auto root = _find_root(xid);
            if (root == nullptr || root->empty()) {
                return end();
            }

            // iterate through the levels until we find a leaf node
            ExtentPtr current = root;
            std::shared_ptr<Node> node = nullptr;
            while (current->type() == ExtentType::BRANCH) {
                // perform a lower-bound check to find the appropriate child branch
                auto child_i = std::lower_bound(current->begin(), current->end(), search_key,
                                                [](const Extent::Row &row, const Tuple &key)
                                                {
                                                    return (_branch_keys.bind(row) < key);
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
                                           [](const Extent::Row &row, const Tuple &key)
                                           {
                                               return (_leaf_keys.bind(row) < key);
                                           });
            if (leaf_i == current->end()) {
                return end();
            }

            node = std::make_shared<Node>(current, leaf_i, node);
            return Iterator(this, node);
        }

        // XXX unclear we need upper_bound()
        Iterator upper_bound(const Tuple &search_key, uint64_t xid)
        {
            // find the correct root based on the XID
            auto root = _find_root(xid);
            if (root == nullptr || root->empty()) {
                return end();
            }

            // iterate through the levels until we find a leaf node
            ExtentPtr current = root;
            std::shared_ptr<Node> node = nullptr;
            while (current->type() == Extent::BTREE_BRANCH) {
                // upper-bound on the branch entries to find the correct child
                auto child_i = std::upper_bound(current->begin(), current->end(), search_key,
                                                [](const Extent::Row &row, const Tuple &key)
                                                {
                                                    return (_branch_keys.bind(row) < key);
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
                                           [](const Extent::Row &row, const Tuple &key)
                                           {
                                               return (_leaf_keys.bind(row) < key);
                                           });
            if (leaf_i == current->end()) {
                return end();
            }

            node = std::make_shared<Node>(current, leaf_i, node);
            return Iterator(this, node);
        }

        Iterator find(const Tuple &search_key, uint64_t xid)
        {
            // find the lower_bound based on the search key
            auto i = lower_bound(search_key, xid);
            if (i == end()) {
                return i; // not found, return end()
            }

            // generate the key of the provided row
            auto &&key = _leaf_keys.bind(*i);

            // if the search key is < found key from lower_bound, then does not exist
            if (search_key < key) {
                return end();
            }

            // found
            return i;
        }
    };


    /**
     * A b+tree constructed out of Extent objects.  Uses a FieldTuple as the sort-key for the tree
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
        /**
         * The Page objects represent a virtual Extent, which may hold either a single Extent
         * object, or potentially multiple Extent objects which are the result of split() operations
         * caused by modifications to the original Extent object.  Page objects may be marked as
         * dirty, indicating that they need to be written to a new location and update their parent
         * branch pointers accordingly.
         */
        class Page : public std::enable_shared_from_this<Page> {
        public:
            class Iterator {
                friend Page;

            private:
                const Page * const _page;
                std::vector<ExtentPtr>::iterator _extent_i;
                Extent::Iterator _row_i;

            private:
                Iterator(const Page * const p, const std::vector<ExtentPtr>::iterator e, Extent::Iterator r)
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
                    if (_row_i == _extent_i->end()) {
                        ++_extent_i;
                        if (_extent_i == _page->_extents.end()) {
                            return *this;
                        }

                        _row_i = _extent_i->begin();
                    }

                    return *this;
                }

                Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

                friend bool operator==(const Iterator& a, const Iterator& b) {
                    return
                        (a._extent_i == _page->_extents.end() &&
                         b._extent_i == _page->_extents.end()) ||
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
            void
            _check_split(const std::vector<ExtentPtr>::iterator &i)
            {
                // perform the split if the referenced extent size exceeds the max
                if (i->byte_count() > BTREE_MAX_EXTENT_SIZE && i->row_count() > 1) {
                    auto &&pair = i->split();

                    // remove the old extent
                    i = _extents.remove(i);

                    // insert the two new extents; insert() occurs before the provided iterator, so inserted in reverse order
                    i = _extents.insert(i, pair.second);
                    _extents.insert(i, pair.first);
                }
            }

        public:
            Page(uint64_t offset, ExtentPtr e, PagePtr p)
                : _dirty(false),
                  _prev_offset(offset),
                  _parent(p)
            {
                _extents.push_back(e);
            }

            Iterator
            begin() const
            {
                return Iterator(this, _extents.begin(), _extents.front()->begin());
            }

            Iterator
            end() const
            {
                return Iterator(this, _extents.end(), _extents.back()->end());
            }

            ExtentType type() const
            {
                return _extents.front()->type();
            }

            Row back() const
            {
                return _extents.back()->back();
            }

            Iterator
            lower_bound(const Tuple &search_key) const
            {
                // find the extent that might contain the search key
                auto &&i = std::lower_bound(_extents.begin(), _extents.end(), search_key,
                                            [](const ExtentPtr &extent, const Tuple &key) {
                                                return _key_fields.bind(extent->back()) < key;
                                            });
                if (i == _extents.end()) {
                    return end();
                }

                // try to find the row within the extent
                auto &&row_i = std::lower_bound(i->begin(), i->end(), search_key,
                                                [](const Extent::Row &row, const Tuple &key) {
                                                    return _key_fields.bind(row) < key;
                                                });
                if (row_i == i->end()) {
                    return end();
                }

                // construct an iterator to this entry
                return Iterator(this, i, row_i);
            }

            Iterator
            find(const Tuple &search_key) const
            {
                // use lower_bound() to do a binary search for the entry
                auto &&i = this->lower_bound(search_key);

                // if the key is < the returned row, then there is no matching entry, return end()
                if (key < _key_fields.bind(*i)) {
                    return end();
                }

                // construct an Iterator to this entry
                return Iterator(this, i, row_i);
            }

            /**
             * Adds an entry to this page.  Assumes that the page is locked for exclusive access.
             */
            void insert(const Tuple &search_key, const Tuple &value)
            {
                // find the position to perform the insert
                auto &&i = this->lower_bound(search_key);

                // insert a row into the extent
                uint32_t old_size = i->_extent_i->byte_count();
                MutableRow row = i->_extent_i->insert(i->_row_i);
                uint32_t new_size = i->_extent_i->byte_count();

                // update the size of the page
                this->size = this->size - old_size + new_size;

                // save the value into the row
                _fields.bind(row) = value;

                // mark this page as dirty
                _dirty = true;

                // perform a split of the extent if necessary
                _check_split(i->_extent_i);
            }
            
            void
            remove(const Tuple &search_key)
            {
                // find the position to perform the remove
                auto &&i = this->find(search_key);
                if (i == this->end()) {
                    return; // no entry to remove
                }

                // remove the row from the extent
                // note: if this needs to remove all matching rows, then additional logic is required
                i->_extent_i->remove(i->_row_i);

                // mark this page as dirty
                _dirty = true;

                // if the extent is empty, remove it from the set
                if (i->_extent_i->empty()) {
                    if (_extents.size() > 1) {
                        _extents.erase(i->_extent_i);
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
            std::vector<PagePtr>
            flush(const IOHandle &handle,
                  const FieldTuple &keys)
            {
                std::vector<PagePtr> new_pages;

                // write all of the extents in parallel using async_flush()
                std::vector<std::future<std::shared_ptr<IOResponseAppend>>> futures;
                for (auto &&e : _extents) {
                    futures.push_back(e->async_flush(handle));
                }

                // as the writes complete, create the new pages
                for (int i = 0; i < futures.size(); i++) {
                    auto &&response = futures[i].get();

                    ConstTuple key = keys.bind(_extent[i].back());
                    PagePtr page = std::make_shared<Page>(response.offset, key, _extent[i]);

                    new_pages.push_back(page);
                }

                return new_pages;
            }

            void
            add_child(PagePtr child)
            {
                std::unique_lock lock(children_mutex);
                _children[child->extent_id] = child;
            }

            void
            remove_child(uint64_t extent_id)
            {
                std::unique_lock lock(children_mutex);
                _children.erase(extent_id);
            }

            void
            check_flush()
            {
                return (_extents.size() > MAX_EXTENT_COUNT);
            }

            void
            empty()
            {
                return (_extents.size() == 1 && _extents.front()->empty());
            }

        public:
            const ExtentType type; ///< The type of this page.  Won't change once the page is constructed, so safe to read without holding the mutex.
            const uint64_t extent_id; ///< The offset of the previous extent this page is based on.  Won't change once the page is constructed.
            const ConstTuple prev_key; ///< The key in the parent of the previous extent this page is based on.  Won't change once the page is constructed.
            const PagePtr parent; ///< Pointer to the parent page.  Won't change once the page is constructed.

            // the following are mutex protected
            mutable std::shared_mutex mutex; ///< A shared mutex for lock management.
            bool dirty; ///< Flag indicating if data has been modified since the last write.
            bool flushed; ///< This page has already been flushed to disk and replaced with a new page.
            std::vector<ExtentPtr> extents; ///< A list of extents that will be written out when this page is flushed.

            // the following are protected by a separate mutex, must be holding the primary mutex at least shared.
            mutable std::shared_mutex children_mutex; ///< A mutex to protect the map of children.  Can be acquired unique while sharing the primary mutex.
            std::map<uint64_t, PagePtr> children; ///< The set of children of this page that are part of a modified path through the tree.
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

            std::unordered_map<uint64_t, LookupEntry, Hash> lookup;
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

        mutable std::shared_mutex _mutex;
        PagePtr _root;
        uint64_t _xid;
        bool _finalized;

        /** The schema for the leaf nodes. */
        std::shared_ptr<ExtentSchema> _leaf_schema;
        FieldTuple &_leaf_fields;
        FieldTuple &_leaf_keys;

        /** The schema for the branch nodes. */
        std::shared_ptr<ExtentSchema> _branch_schema;
        FieldTuple &_branch_keys;
        std::shared_ptr<Field> _branch_child_f;

    private:
        struct Node {
            const NodePtr parent;
            const PagePtr page;

            Node(NodePtr parent, PagePtr page)
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
            _cache.lookup.insert_or_assign(extent_id, { page, _cache.lru.end(), 1, 0 });
        }

        void
        _cache_insert(PagePtr page, std::unique_lock &cache_lock)
        {
            // place the page into the cache, but not into the LRU queue
            _cache_insert_empty(page);

            // update the size of the cache
            _cache_update_size(page, cache_lock);
        }

        void
        _cache_update_size(PagePtr page, std::unique_lock &cache_lock)
        {
            // update the size of the cache given the page size
            auto &&i = _cache.lookup.find(page->extent_id);
            assert(i != _cache.lookup.end());
            LookupEntry &entry = i->second;

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
                PagePtr page = _cache.lru.pop_back();

                // check if we need to try another entry
                // note: if the page is the root we can't evict; if the page has potentially dirty
                //       children, must evict them first
                if (page->is_root || !page->children.empty()) {
                    _cache.lru.push_front();
                    continue;
                }

                // check if we can remove immediately
                // note: if the page was already flushed, or has never been accessed since being
                //       created from a flush, then can evict immedately
                if (page->flushed || !page->parent) {
                    _lookup.erase(page->extent_id);
                    _size -= page->size;
                    continue;
                }

                // currently looks evictable and requires a flush; try to acquire the parent page lock
                std::unique_lock parent_lock(page->parent->mutex, std::try_to_lock);

                // if we weren't able to lock the parent, try another page
                // XXX should we just block until we get the lock? there's a potential live-lock here
                if (!parent_lock.owns_lock()) {
                    _cache.lru.push_front();
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
                _flush_page_internal(page, page->parent);

                // re-lock the cache to proceed with evictions
                cache_lock.lock();
            }
        }

        void
        _cache_evict(uint64_t extent_id)
        {
            // find the entry if it exists
            auto &&i = _cache.lookup.find(id);
            if (i == _cache.lookup.end()) {
                return;
            }

            // retrieve the value from the lookup entry
            PageCache::LookupEntry &entry = *(i->second);
            PagePtr value = std::get<1>(entry);

            // note: no need to flush the page since a direct eviction should only come after
            //       flushing a page; also, page shouldn't be on LRU list since it is in-use by
            //       the caller
            assert(std::get<0>(entry)->flushed);
            assert(std::get<2>(entry) > 0);

            // remove the entry from the cache
            _cache.size -= std::get<3>(entry);
            _cache.lookup.erase(i);

            return value;
        }


        //// INTERNAL OPERATIONS

        /**
         * Read an extent from disk and store it as a Page in the cache.
         */
        PagePtr
        _read_page(uint64_t extent_id,
                   PagePtr parent,
                   const Tuple &page_key,
                   std::shared_lock &parent_lock,
                   std::unique_lock &leaf_lock)
        {
            // note: when entering, parent_lock is held and leaf_lock not held

            // lock the cache
            std::unique_lock lock(_cache.mutex);

            // check if the entry already exists
            PagePtr page = _cache_get(extent_id);
            if (page != nullptr) {
                // get the appropriate lock
                if (page->type == ExtentType::BTREE_BRANCH) {
                    std::shared_lock child_lock(page->mutex);

                    // update the parent/child inter-link; may already be set but won't change
                    child->set_parent(node->page);
                    node->page->add_child(child);

                    // release the parent page back to the cache
                    _cache_release(parent);

                    // release the parent lock and replace with child lock
                    parent_lock = child_lock;
                } else {
                    leaf_lock = std::unique_lock(page->mutex);

                    // update the parent/child inter-link; may already be set but won't change
                    child->set_parent(node->page);
                    node->page->add_child(child);

                    // release the parent page back to the cache
                    _cache_release(parent);

                    // release the parent lock
                    parent_lock.unlock();
                }

                return page;
            }

            // no page in the cache, so construct a page that we can populate
            page = std::make_shared<Page>(extent_id);

            if (!parent) {
                // flag the root here
                page->is_root = true;
            }

            // place the page into the cache, but not into the LRU queue
            // note: this placeholder could be found by other threads, but will block since we will
            //       hold an exclusive lock on the page
            _cache_insert_empty(page);

            // get an exclusive lock the new page
            std::unique_lock page_lock(page->mutex);

            // update the page book-keeping
            child->set_parent(node->page);
            node->page->add_child(child);

            // release the parent page back to the cache
            _cache_release(parent);

            // release the parent lock
            parent_lock.release();

            // release the cache for other threads until we have populated the data for this page
            // note: the exclusive lock on the page will prevent others from progressing even if
            //       they try to access this page
            lock.unlock();

            // now populate the page
            auto response = _handle.read(extent_id);

            // unpack the header to determine the extent type
            ExtentHeader header(response.data[0]);

            // construct the extent
            ExtentPtr extent;
            if (header.type == ExtentType::BRANCH) {
                extent = std::make_shared<Extent>(_branch_schema, response.data);
            } else {
                extent = std::make_shared<Extent>(_leaf_schema, response.data);
            }

            // put the extent data into the page
            page->_extents.push_back(extent);

            // if the page is not the root then we store the key of the entry we used to find this page
            if (!page->is_root) {
                page->_prev_key = page_key;
            }
             
            // now re-acquire the cache lock to update the size of the cache
            lock.lock();

            // update the size of the page in the cache; may cause evictions
            _cache_update_size(page);

            // check which kind of lock we need to hold on the page
            if (page->type == ExtentType::BTREE_BRANCH) {
                page_lock.unlock();
                parent_lock = std::shared_lock(page->mutex);
            } else {
                leaf_lock = page_lock;
            }

            // return the page
            return page;
        }

        /**
         * Find the leaf that *could* hold the provided key.  This will always return a pointer to a
         * Page since it is used for key insertion.
         */
        // XXX we could (1) pass in the base Node here to allow search starting from any node and (2) use a template for the lock type... these would allow us to use this for read iteration
        NodePtr _find_leaf(const Tuple &key, std::unique_lock &leaf_lock)
        {
            // safe because the root pointer can't change if we are holding the btree lock
            NodePtr node = std::make_shared<Node>(nullptr, _root);

            // if the root is the leaf, lock and return
            if (_root->type == ExtentType::BTREE_LEAF) {
                leaf_lock = std::unique_lock(_root->mutex);
                return node;
            }

            // otherwise, get a shared lock and traverse
            std::shared_lock lock(_root->mutex);

            // iterate through the levels until we find a leaf page
            while (node->page->type == ExtentType::BTREE_BRANCH) {
                // use a lower_bound() check to find the appropriate child branch
                auto &&i = node->page->lower_bound(key);

                // note: we know the tree isn't empty since the root would be a leaf in that case
                Extent::Row row = (i == node->page->end()) ? node->page->back() : *i;

                // retrieve the child offset
                uint64_t extent_id = _branch_child_f->get_uint64(row);

                // read the child page; will handle updating the locks
                PagePtr child = _read_page(extent_id, node->page, _branch_keys.bind(row),
                                           lock, leaf_lock);

                // handle the locking based on the page type
                if (child->type == ExtentType::BTREE_BRANCH) {
                    // acquire shared lock on the child
                    std::shared_lock child_lock(child->mutex);

                    // update the parent/child inter-link; may already be set but won't change
                    child->set_parent(node->page);
                    node->page->add_child(child);

                    // release the parent back to the cache for eventual eviction
                    _cache_release(node->page);

                    // unlock the parent and shift to the child
                    lock.unlock();
                    lock = child_lock;
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
            const FieldTuple &keys = (page->type == ExtentType::BTREE_BRANCH)
                ? _branch_keys
                : _leaf_keys;
            std::vector<PagePtr> &&new_pages = page->flush(_handle, keys);

            // update the parent's pointers
            parent->update_branch(page, new_pages);

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
            // note: safe to traverse the children without children_lock because we are holding
            //       exclusive lock on the page
            while (!page->children.empty()) {
                PagePtr child = page->children->begin().second;

                // this will remove the child from the page's children during the flush
                // note: we are currently holding an exlusive lock on the child's parent (this
                //       page), so safe to call
                _flush_page(child, page);
            }

            // if the page was not modified while flushing children, then no need to flush it
            // note: this could occur if pages were accessed for read
            if (!page->dirty) {
                return;
            }

            // check if the page is empty
            if (page->empty()) {
                // if so then we need to process a removal on the parent
                _remove_page_internal(page, parent);
            } else {
                // perform the actual page flush
                std::vector<PagePtr> &&new_pages = _flush_page_internal(page, parent);
            }

            // evict the page from the cache and add the new pages
            std::unique_lock cache_lock(_cache.mutex);

            _cache_evict(page->extent_id);
            for (auto &&new_page : new_pages) {
                _cache_insert(new_page, cache_lock);
            }
        }

        PagePtr
        _flush_root(PagePtr page)
        {
            // acquire exclusive access to the page
            std::unique_lock page_lock(page->mutex);

            // if the page was flushed, then nothing to do
            if (page->flushed) {
                return;
            }

            // flush the children first
            // note: safe to traverse the children without children_lock because we are holding
            //       exclusive lock on the page
            while (!page->children.empty()) {
                PagePtr child = page->children->begin().second;

                // this will remove the child from the page's children during the flush
                // note: we are currently holding an exlusive lock on the child's parent (this
                //       page), so safe to call
                _flush_page(child, page);
            }

            // check if this page has actually been modified by the child flushes
            if (!page->dirty) {
                return page;
            }

            // flush the page's extent(s) to disk
            // note: it would be nice if we could perform the disk writes without holding the parent
            //       mutex, but then there is a potential race condition between updating the
            //       parent's pointers and flushing the parent.  It might be possible with more
            //       complex logic.
            const FieldTuple &keys = (page->type == ExtentType::BTREE_BRANCH)
                ? _branch_keys
                : _leaf_keys;
            std::vector<PagePtr> &&new_pages = page->flush(_handle, keys);

            // check if we need to create a new root above this page
            PagePtr new_root;
            if (new_pages.size() > 1) {
                // construct the new root's extent
                ExtentPtr extent = std::make_shared<Extent>(_branch_schema, ExtentType::BTREE_BRANCH, _xid);

                // add pointers to the new root for each new page
                for (PagePtr &&child : new_pages) {
                    Extent::MutableRow row = extent->append();
                    _branch_keys.bind(row) = keys.bind(child->last_row());
                }

                // write the new extent to disk
                auto &&future = extent->async_flush(_handle);

                // construct a new root page based on the new extent
                uint64_t extent_id = future.get().offset;
                Tuple &&key = _branch_keys.bind(extent.back());
                PagePtr new_root = std::make_shared<Page>(extent_id, key, extent);

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
            _root = new_root;
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
                    _lock_and_flush_page(parent_node);
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
            parent->remove(page->key);

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
        MutableBTree(std::filesystem::path &file,
                     uint64_t root_offset,
                     uint64_t cache_size,
                     std::shared_ptr<Schema> schema)
            : _cache(cache_size),
              _finalized(true),
              _leaf_schema(schema)
        {
            // open the file handle
            _handle = IOMgr::get_instance()->open(file, IOMgr::READ, true);

            // read the root
            _root = _read_page(root_offset, nullptr, ConstTuple());

            // set the XID based on the root
            _xid = _root->extents[0]->header().xid;

            // construct the field tuples for the leaf nodes
            _leaf_fields = _leaf_schema->get_fields();
            _leaf_keys = _leaf_schema->get_fields(keys);

            // construct the schema for the branches
            SchemaColumn child("child", 0, SchemaType::UINT64, false);
            _branch_schema = _leaf_schema->create_schema(sort_keys, { child });

            // construct the field tuples for the branch nodes
            _branch_keys = _branch_schema->get_fields(keys);
            _branch_child_f = _branch_schema->get_field("child");
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
        void insert(const Tuple &value)
        {
            // acquire a shared lock on the btree
            std::shared_lock tree_lock(_mutex);

            // make sure that we can modify this tree
            assert(!_finalized);

            // get the search key for this value
            Tuple &&search_key = value.get_fields(_sort_key);

            // traverse the tree to find the correct leaf page along with the pages along the path
            // to it from the root, get exclusive lock on the leaf
            std::unique_lock lock;
            NodePtr node = _find_leaf(search_key, lock);

            // once we've traversed the tree we can release the tree lock
            tree_lock.unlock();

            // insert the value into the page
            node->page->insert(search_key, value);

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
        void remove(const Tuple &value)
        {
            // acquire a shared lock on the btree
            std::shared_lock tree_lock(_mutex);

            // make sure that we can modify this tree
            assert(!_finalized);

            // get the search key for this value
            auto &&search_key = value.get_fields(_sort_key);

            // traverse the tree to find the appropriate page to insert into
            std::unique_lock lock;
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
