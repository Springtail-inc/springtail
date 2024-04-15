#pragma once

#include <storage/extent.hh>
#include <storage/field.hh>

namespace springtail {

    /**
     * A storage-backed B+Tree for holding temporary data.  Supports both read operations and
     * mutation operations, but is not versioned like the BTree and doesn't support high-concurrency
     * like the MutableBTree.  It's can be used for things like temporary tables or arbitrary-sized
     * data pages.  Currently used by the DataCache to represent in-memory extents during mutation.
     *
     * The TempBTree utilizes exclusive locking on all mutation operations, making it thread-safe but less
     * performant that a MutableBTree.
     */
    class TempBTree {
    private:
        class Page {
        public:
            Page(ExtentPtr extent)
            {
                // XXX
                _extents.push_back(extent);
            }

        public:
            class Iterator {
                friend Page;

            protected:
                Iterator(Page *page,
                         std::vector<ExtentPtr>::iterator page_i,
                         Extent::Iterator extent_i)
                    : _page(page),
                      _page_i(page_i),
                      _extent_i(extent_i)
                { }

            public:
                using iterator_category = std::forward_iterator_tag;
                using difference_type   = std::ptrdiff_t;
                using value_type        = const Extent::Row;
                using pointer           = const Extent::Row *;  // or also value_type*
                using reference         = const Extent::Row &;  // or also value_type&

                reference operator*() const { return *(_extent_i); }
                pointer operator->() { return &(*(_extent_i)); }

                Iterator(const Iterator &rhs)
                    : _page(rhs._page),
                      _page_i(rhs._page_i),
                      _extent_i(rhs._extent_i)
                { }

                Iterator &operator++() {
                    // move to the next row
                    ++_extent_i;

                    // if we're at the end of the extent, move the beginning of the next extent
                    if (_extent_i == (*_page_i)->end()) {
                        ++_page_i;

                        // if this was the last extent, set to end()
                        if (_page_i == _page->_extents.end()) {
                            _page_i = std::prev(_page->_extents.end());
                            return *this;
                        }

                        _extent_i = (*_page_i)->begin();
                    }

                    return *this;
                }

                bool operator==(const Iterator &rhs) const {
                    return (_page == rhs._page &&
                            _page_i == rhs._page_i &&
                            _extent_i == rhs._extent_i);
                }

            private:
                Page *_page;
                std::vector<ExtentPtr>::iterator _page_i;
                Extent::Iterator _extent_i;
            };

            Iterator begin() {
                return Iterator(this, _extents.begin(), _extents[0]->begin());
            }

            Iterator end() {
                return Iterator(this, std::prev(_extents.end()), _extents.back()->end());
            }

            Iterator lower_bound(TuplePtr key) {
                // find the extent that could hold the key
                auto &&i = std::lower_bound(_extents.begin(), _extents.end(), key,
                                            [this](const ExtentPtr &extent, TuplePtr key) {
                                                return MutableTuple(this->_key_fields,
                                                                    extent->back()).less_than(key);
                                            });
                if (i == _extents.end()) {
                    return this->end();
                }
                auto &extent = *i;

                // find the row that holds a value <= key
                auto &&j = std::lower_bound(extent->begin(), extent->end(), key,
                                            [this](const Extent::Row &row, TuplePtr key) {
                                                return MutableTuple(this->_key_fields, row).less_than(key);
                                            });
                if (j == extent->end()) {
                    return this->end();
                }

                // return an iterator to this position
                return Iterator(this, i, j);
            }

            bool is_leaf() {
                // an empty page must always be a leaf
                if (_extents.empty()) {
                    return true;
                }

                // check the extent header of the first extent in the page
                return _extents[0]->type().is_leaf();
            }

            bool
            empty()
            {
                return _extents.empty() || (_extents.size() == 1 && _extents[0]->empty());
            }

        public:
            void insert(const Iterator &pos,
                        TuplePtr value)
            {
                auto extent = *pos._page_i;
                auto row = extent->insert(pos._extent_i);
                MutableTuple(_fields, row).assign(value);

                _check_split(pos._page_i);
            }

            void remove(const Iterator &pos)
            {
                auto extent = *pos._page_i;
                extent->remove(pos._extent_i);

                // XXX check for merge?
            }

            void update(const Iterator &pos,
                        TuplePtr value)
            {
                auto extent = *pos._page_i;
                MutableTuple(_fields, *pos._extent_i).assign(value);

                _check_split(pos._page_i);
            }

        protected:
            bool _check_split(std::vector<ExtentPtr>::iterator pos) {
                ExtentPtr e = *pos;

                // check if we need to split based on the size
                // note: can't split a single row, no matter how large
                if (e->byte_count() < MAX_EXTENT_SIZE || e->row_count() == 1) {
                    return false;
                }

                // perform the split
                auto &&pair = e->split();
                _size -= e->byte_count();

                // remove the old extent
                pos = _extents.erase(pos);

                // insert the new extents
                pos = _extents.insert(pos, pair.second);
                _extents.insert(pos, pair.first);

                // update the size of this page
                _size += pair.first->byte_count() + pair.second->byte_count();
                return true;
            }

        private:
            static constexpr uint32_t MAX_EXTENT_SIZE = 64 * 1024;

            std::vector<ExtentPtr> _extents;
            MutableFieldArrayPtr _key_fields;
            MutableFieldArrayPtr _fields;
            uint32_t _size;
        };
        typedef std::shared_ptr<Page> PagePtr;

    public:
        void
        insert(TuplePtr value)
        {
            boost::unique_lock lock(_mutex);

            auto key = _get_key_from_value(value);
            auto path = _find_leaf(key);
            path->page->insert(path->page_i, value);
        }

        void
        remove(TuplePtr key)
        {
            boost::unique_lock lock(_mutex);

            auto path = _find_leaf(key);
            path->page->remove(path->page_i);
        }

        void
        update(TuplePtr value)
        {
            boost::unique_lock lock(_mutex);

            auto key = _get_key_from_value(value);
            auto path = _find_leaf(key);
            path->page->update(path->page_i, value);
        }

    private:
        class Path {
        public:
            std::shared_ptr<Path> parent;
            PagePtr page;
            Page::Iterator page_i;

            Path(const std::shared_ptr<Path> parent, PagePtr page, Page::Iterator page_i)
                : parent(parent), page(page), page_i(page_i)
            { }

            Path(const Path &path)
                : page(path.page),
                  page_i(path.page_i)
            {
                if (path.parent != nullptr) {
                    // make a copy of the parent
                    parent = std::make_shared<Path>(*path.parent);
                }
            }

            friend bool operator==(const Path &lhs, const Path &rhs)
            {
                return (lhs.page == rhs.page && lhs.page_i == rhs.page_i);
            }
        };
        typedef std::shared_ptr<Path> PathPtr;

    public:
        class Iterator {
            friend TempBTree;

        protected:
            Iterator(TempBTree *btree, PathPtr path)
                : _btree(btree),
                  _path(path)
            { }

        public:
            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;  // or also value_type*
            using reference         = const Extent::Row &;  // or also value_type&

            reference operator*() const { return *(_path->page_i); }
            pointer operator->() { return &(*(_path->page_i)); }

            Iterator(const Iterator &rhs)
                : _btree(rhs._btree)
            {
                // make a copy of the path
                _path = std::make_shared<Path>(*rhs._path);
            }

            Iterator &operator++() {
                // move to the next entry in the leaf
                ++_path->page_i;
                if (_path->page_i != _path->page->end()) {
                    return *this;
                }

                // if we are at the end of the current leaf, recurse up the tree
                uint32_t depth = 0;
                while (_path->page_i == _path->page->end()) {
                    _path = _path->parent;
                    if (_path == nullptr) {
                        return *this;
                    }

                    ++_path->page_i;
                    ++depth;
                }

                // recurse back down the tree
                while (depth > 0) {
                    uint64_t extent_id = _btree->_child_id_f->get_uint64(*(_path->page_i));
                    auto page = _btree->_read_page(extent_id);
                    _path = std::make_shared<Path>(_path, page, page->begin());
                    --depth;
                }

                return *this;
            }

            friend bool operator==(const Iterator& a, const Iterator& b) {
                if (a._path == nullptr && b._path == nullptr) {
                    return true;
                } else if (a._path == nullptr || b._path == nullptr) {
                    return false;
                }
                    
                return (*a._path == *b._path);
            }
            
        private:
            TempBTree *_btree;
            PathPtr _path;
        };

        Iterator
        begin()
        {
            boost::shared_lock lock(_mutex);

            auto path = _find_first_leaf();
            return Iterator(this, path);
        }

        Iterator
        end()
        {
            return Iterator(this, nullptr);
        }

        Iterator
        lower_bound(TuplePtr key)
        {
            boost::shared_lock lock(_mutex);

            auto path = _find_leaf(key);
            return Iterator(this, path);
        }

        bool
        empty()
        {
            boost::shared_lock lock(_mutex);
            return _root->empty();
        }

        FieldArrayPtr get_key_fields() {
            return _leaf_schema->get_fields(_key_columns);
        }


    protected:
        TuplePtr _get_key_from_value(TuplePtr value) {
            return _leaf_schema->tuple_subset(value, _key_columns);
        }

        PathPtr _find_first_leaf() {
            auto pos = _root->begin();
            auto path = std::make_shared<Path>(nullptr, _root, pos);

            while (!path->page->is_leaf()) {
                uint64_t extent_id = _child_id_f->get_uint64(*pos);

                auto page = _read_page(extent_id);
                pos = page->begin();

                path = std::make_shared<Path>(path, page, pos);
            }

            return path;
        }

        PathPtr _find_leaf(TuplePtr key) {
            auto pos = _root->lower_bound(key);
            auto path = std::make_shared<Path>(nullptr, _root, pos);

            while (!path->page->is_leaf()) {
                uint64_t extent_id = _child_id_f->get_uint64(*pos);

                auto page = _read_page(extent_id);
                pos = page->lower_bound(key);

                path = std::make_shared<Path>(path, page, pos);
            }

            return path;
        }

        PagePtr _read_page(uint64_t extent_id) {
            // check the cache for the page
            auto page = _cache->get(extent_id);
            if (page != nullptr) {
                return page;
            }

            // read the extent from disk and create a page for it
            auto response = _handle->read(extent_id);
            
            // unpack the header to determine the extent type
            ExtentHeader header(response->data[0]);

            // construct the extent
            ExtentPtr extent;
            if (header.type.is_branch()) {
                extent = std::make_shared<Extent>(_branch_schema, response->data);
            } else {
                extent = std::make_shared<Extent>(_leaf_schema, response->data);
            }

            // XXX need a way to evict pages from the cache to local storage

            page = std::make_shared<Page>(extent);
            _cache->insert(extent_id, page);

            return page;
        };

    private:
        boost::shared_mutex _mutex;
        PagePtr _root;
        std::vector<std::string> _key_columns;
        FieldPtr _child_id_f;
        ExtentSchemaPtr _leaf_schema;
        ExtentSchemaPtr _branch_schema;
        std::shared_ptr<ObjectCache<uint64_t, Page>> _cache;
        std::shared_ptr<IOHandle> _handle;
    };
    typedef std::shared_ptr<TempBTree> TempBTreePtr;
}
