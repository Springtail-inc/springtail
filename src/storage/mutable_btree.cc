#include <storage/mutable_btree.hh>

namespace springtail {

    MutableBTree::MutableBTree(std::shared_ptr<IOHandle> handle,
                               const std::vector<std::string> &keys,
                               uint64_t cache_size,
                               ExtentSchemaPtr schema)
        : _cache(cache_size),
          _handle(handle),
          _sort_keys(keys),
          _xid(0),
          _finalized(true)
    {
        // initialize the schema information
        _init_schemas(schema, keys);

        // construct an empty extent at XID 0 -- an invalid XID
        // note: this extent will be replaced after mutations to the tree are performed following
        //       a call to set_xid()
        ExtentPtr extent = std::make_shared<Extent>(schema, ExtentType(false, true), 0);

        // create an empty root page
        // note: the implementation depends on a root always existing, but we still consider
        //       the tree finalized at the provided XID until a new target XID is set
        _root = std::make_shared<Page>(extent, _leaf_keys);

        // add the root to the cache
        std::unique_lock cache_lock(_cache.mutex);
        _cache_insert(_root, cache_lock, false);
    }

    MutableBTree::MutableBTree(std::shared_ptr<IOHandle> handle,
                               const std::vector<std::string> &keys,
                               uint64_t cache_size,
                               std::shared_ptr<ExtentSchema> schema,
                               uint64_t root_offset)
        : _cache(cache_size),
          _handle(handle),
          _sort_keys(keys),
          _finalized(true),
          _leaf_schema(schema)
    {
        // initialize the schema information
        _init_schemas(schema, keys);

        // construct an empty page to populate
        _root = std::make_shared<Page>(root_offset);

        // read the root
        _read_page_internal(_root);

        // add the root to the cache
        std::unique_lock cache_lock(_cache.mutex);
        _cache_insert(_root, cache_lock, false);

        // set the XID based on the root
        _xid = _root->xid();
    }

    void
    MutableBTree::set_xid(uint64_t xid)
    {
        // acquire an exclusive lock on the tree
        std::unique_lock lock(_mutex);

        // update the target XID and remove the finalized flag
        _xid = xid;
        _finalized = false;
    }

    void
    MutableBTree::insert(TuplePtr value)
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

        // release the nodes back to the cache
        // note: we don't call release on the root since it's never acquired via _cache_get()
        while (node->parent != nullptr) {
            _cache_release(node->page);
            node = node->parent;
        }
    }

    void
    MutableBTree::remove(TuplePtr value)
    {
        // acquire a shared lock on the btree
        std::shared_lock tree_lock(_mutex);

        // make sure that we can modify this tree
        assert(!_finalized);

        // get the search key for this value
        TuplePtr search_key = _leaf_schema->tuple_subset(value, _sort_keys);
        // auto &&search_key = value.get_fields(_sort_key);

        // traverse the tree to find the appropriate page to remove from
        std::unique_lock<std::shared_mutex> lock;
        NodePtr node = _find_leaf(search_key, lock);

        // once we've traversed the tree we can release the tree lock
        tree_lock.unlock();

        // remove the value from the page
        node->page->remove(search_key);

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

        // release the nodes back to the cache
        // note: we don't call release on the root since it's never acquired via _cache_get()
        while (node->parent != nullptr) {
            _cache_release(node->page);
            node = node->parent;
        }
    }

    uint64_t
    MutableBTree::finalize()
    {
        // acquire an exclusive lock on the tree here
        std::unique_lock lock(_mutex);

        // flush from the root of the tree
        // note: we call _flush_root() directly since we want to hold the tree mutex for longer than
        //       just the flush call
        PagePtr new_root = _flush_root(_root);
        if (new_root != nullptr) {
            _root = new_root;
        }

        // sync the file
        _handle->sync();

        // mark the tree as finalized so that additional changes can't be made at this XID
        _finalized = true;

        // return the new extent_id of the root
        return _root->extent_id;
    }

    bool
    MutableBTree::Page::_check_split(std::vector<ExtentPtr>::iterator pos)
    {
        ExtentPtr e = *pos;

        // if the size of the extent is below the threshold, or it can't be split because
        // it's a single row, then return immediately
        if (e->byte_count() < MAX_EXTENT_SIZE || e->row_count() == 1) {
            return false;
        }

        // perform the split if the referenced extent size exceeds the max
        auto &&pair = e->split();
        this->size -= e->byte_count();

        // remove the old extent
        pos = _extents.erase(pos);

        // insert the two new extents; insert() occurs before the provided iterator, so inserted in reverse order
        pos = _extents.insert(pos, pair.second);
        _extents.insert(pos, pair.first);

        this->size += pair.first->byte_count() + pair.second->byte_count();
        return true;
    }

    MutableBTree::Page::Iterator
    MutableBTree::Page::lower_bound(TuplePtr search_key)
    {
        // if the page is empty then return end()
        if (_extents.size() == 1 && _extents.front()->empty()) {
            return this->end();
        }

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

    MutableBTree::Page::Iterator
    MutableBTree::Page::find(TuplePtr search_key)
    {
        // use lower_bound() to do a binary search for the entry
        auto &&i = this->lower_bound(search_key);
        if (i == this->end()) {
            return i;
        }

        // if the key is < the returned row, then there is no matching entry, return end()
        if (search_key->less_than(_key_fields->bind(*i))) {
            return this->end();
        }

        // return the Iterator
        return i;
    }

    void
    MutableBTree::Page::update_branch(std::shared_ptr<Page> old_page,
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

    void
    MutableBTree::Page::insert(TuplePtr search_key,
                               TuplePtr value,
                               MutableFieldArrayPtr fields)
    {
        // mark this page as dirty
        _dirty = true;

        // handle the empty page case
        if (this->empty()) {
            ExtentPtr e = _extents.front();

            // add a row
            Extent::MutableRow row = e->append();
                    
            // save the value into the row
            fields->bind(row)->assign(value);

            // update the page size
            this->size = e->byte_count();

            return;
        }

        // find the position to perform the insert
        auto &&i = this->lower_bound(search_key);

        // set the correct iterators
        ExtentPtr e = *i._extent_i;
        uint32_t old_size = e->byte_count();

        // add a row, if at end() will append
        Extent::MutableRow row = e->insert(i._row_i);

        // save the value into the row
        fields->bind(row)->assign(value);

        // update the size of the page
        uint32_t new_size = e->byte_count();
        this->size = this->size - old_size + new_size;

        // check split of the last extent in the vector
        _check_split(i._extent_i);
    }

    void
    MutableBTree::Page::remove(TuplePtr search_key)
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
        // note: if this needs to remove all matching rows, then additional logic is
        //       required; currently only removes a single row
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
        // note: we don't perform cross-Page merge here as it's handled during GC vacuum but
        //       we could check if the size of this extent and the one before / after would
        //       make the threshold for a single extent
    }

    std::vector<std::shared_ptr<MutableBTree::Page>>
    MutableBTree::Page::flush_empty_root(std::shared_ptr<IOHandle> handle,
                                         uint64_t xid)
    {
        // since this is an empty root, ensure that it is now a leaf node
        ExtentType type(false, true);

        // update the extent header
        _extents[0]->header().xid = xid;
        _extents[0]->header().prev_offset = this->extent_id;
        _extents[0]->header().type = type;

        // write the empty extent
        auto &&response = _extents[0]->async_flush(handle).get();

        // construct the page this way to ensure we don't try to extract a prev_key
        PagePtr page = std::make_shared<Page>(response->offset);
        page->set_extent(_extents[0], _key_fields);

        return std::vector<PagePtr>({page});
    }

    std::vector<std::shared_ptr<MutableBTree::Page>>
    MutableBTree::Page::flush(std::shared_ptr<IOHandle> handle,
                              uint64_t xid)
    {
        std::vector<PagePtr> new_pages;

        // if the root was split, then remove the root flag from the type
        ExtentType type = (_extents.size() > 1)
            ? ExtentType(this->type.is_branch())
            : this->type;

        // write all of the extents in parallel using async_flush()
        std::vector<std::future<std::shared_ptr<IOResponseAppend>>> futures;
        for (auto &&e : _extents) {
            // set the extent header correctly
            e->header().xid = xid;
            e->header().prev_offset = this->extent_id;
            e->header().type = type;

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

    void
    MutableBTree::Page::set_extent(ExtentPtr extent,
                                   MutableFieldArrayPtr key_fields)
    {
        // note: page should be empty when this is called
        assert(_extents.size() == 0);

        // put the extent into the list
        _extents.push_back(extent);
        this->size = extent->byte_count();

        // set the type based on the extent header
        this->type = extent->type();

        // set the key fields for the page
        _key_fields = key_fields;

        // if the page is not the root then we store the key of the entry we used to find this page
        if (!this->type.is_root()) {
            // force a copy of the values into a ValueTuple
            this->prev_key = std::make_shared<ValueTuple>(key_fields->bind(extent->back()));
        }
    }

    MutableBTree::PagePtr
    MutableBTree::_cache_get(uint64_t extent_id)
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
    MutableBTree::_cache_release(PagePtr page)
    {
        // find the entry; must exist in the cache since can't be chosen for eviction while it is in use
        auto &&i = _cache.lookup.find(page->extent_id);

        // it's possible that the page was evicted due to a flush, in which case it will no
        // longer be in the cache
        if (i == _cache.lookup.end()) {
            return;
        }

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
    MutableBTree::_cache_insert_empty(PagePtr page)
    {
        // place the page into the cache, but not into the LRU queue
        // note: set the use count to 1 since the caller is actively using the page
        _cache.lookup.insert_or_assign(page->extent_id,
                                       PageCache::LookupEntry(page,
                                                              _cache.lru.end(),
                                                              1, // use count
                                                              0)); // size
    }

    void
    MutableBTree::_cache_insert(PagePtr page,
                                std::unique_lock<std::shared_mutex> &cache_lock,
                                bool release)
    {
        // place the page into the cache, but not into the LRU queue
        _cache_insert_empty(page);

        // update the size of the cache
        _cache_update_size(page, cache_lock);

        // release the page back if it's not going to be in use
        if (release) {
            _cache_release(page);
        }
    }

    void
    MutableBTree::_cache_update_size(PagePtr page,
                                     std::unique_lock<std::shared_mutex> &cache_lock)
    {
        // update the size of the cache given the page size
        auto &&i = _cache.lookup.find(page->extent_id);
        assert(i != _cache.lookup.end());
        PageCache::LookupEntry &entry = i->second;

        // update the size of the cache based on the new size of the entry
        _cache.size = _cache.size - std::get<3>(entry) + page->size;
        std::get<3>(entry) = page->size;

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
            // note: if the page is not dirty, was already flushed, or has never been accessed since being
            //       created from a flush, then can evict immedately
            if (!page->is_dirty() || page->flushed || !page->parent()) {
                _cache.lookup.erase(page->extent_id);
                _cache.size -= page->size;
                continue;
            }

            // currently looks evictable and requires a flush; try to acquire the parent page lock
            std::unique_lock parent_lock(page->parent()->mutex, std::try_to_lock);

            // if we weren't able to lock the parent, try another page
            // note: may not be safe to block to acquire parent since we are holding the cache lock
            // XXX should we just block until we get the lock? there's a potential live-lock
            //     here from never being able to get a page
            if (!parent_lock.owns_lock()) {
                _cache.lru.push_front(page);
                std::get<1>(entry) = _cache.lru.begin();
                continue;
            }

            // lock the page; should never block since the page isn't being used (came off the LRU)
            std::unique_lock page_lock(page->mutex, std::try_to_lock);
            assert(page_lock.owns_lock());

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
    MutableBTree::_cache_evict(uint64_t extent_id)
    {
        // find the entry if it exists
        auto &&i = _cache.lookup.find(extent_id);
        if (i == _cache.lookup.end()) {
            return; // not an error since someone else may have evicted
        }

        // retrieve the value from the lookup entry
        PageCache::LookupEntry &entry = i->second;
        PagePtr page = std::get<0>(entry);

        // note: no need to flush the page since a direct eviction should only come after
        //       flushing a page
        assert(page->flushed);

        // during a finalize() the entries are flushed without being acquired out of the cache,
        // so in that case we also need to remove it from the LRU list
        if (std::get<2>(entry) == 0) {
            _cache.lru.erase(std::get<1>(entry));
        }

        // remove the entry from the cache
        _cache.size -= std::get<3>(entry);
        _cache.lookup.erase(i);
    }

    void
    MutableBTree::_read_page_internal(PagePtr page)
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

    MutableBTree::PagePtr
    MutableBTree::_read_page(uint64_t extent_id,
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

                // release the parent lock and replace with child lock
                parent_lock = std::move(child_lock);
            } else {
                leaf_lock = std::unique_lock(page->mutex);

                // update the parent/child inter-link; may already be set but won't change
                page->set_parent(parent);
                parent->add_child(page);

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

    MutableBTree::NodePtr
    MutableBTree::_find_leaf(TuplePtr key,
                             std::unique_lock<std::shared_mutex> &leaf_lock)
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

            // recurse to the child
            node = std::make_shared<Node>(node, child);
        }

        // releases the direct parent's shared lock here
        return node;
    }

    std::vector<MutableBTree::PagePtr>
    MutableBTree::_flush_page_internal(PagePtr page,
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
    MutableBTree::_flush_page(PagePtr page,
                              PagePtr parent)
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

    MutableBTree::PagePtr
    MutableBTree::_flush_root(PagePtr page)
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

        std::vector<PagePtr> &&new_pages = (page->empty())
            ? page->flush_empty_root(_handle, _xid)
            : page->flush(_handle, _xid);

        // check if we need to create a new root above this page
        PagePtr new_root;
        if (new_pages.size() > 1) {
            // construct the new root's extent
            ExtentPtr extent = std::make_shared<Extent>(_branch_schema, ExtentType(true, true), _xid);
            extent->header().prev_offset = page->extent_id;

            // add pointers to the new root for each new page
            for (PagePtr child : new_pages) {
                Extent::MutableRow row = extent->append();
                _branch_keys->bind(row)->assign(child->index_key());
                _branch_child_f->set_uint64(row, child->extent_id);
            }

            // write the new extent to disk
            auto &&future = extent->async_flush(_handle);

            // construct a new root page based on the new extent
            uint64_t extent_id = future.get()->offset;
            ValueTuplePtr key = std::make_shared<ValueTuple>(_branch_keys->bind(extent->back()));
            new_root = std::make_shared<Page>(extent_id, key, extent, _branch_keys);

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
    MutableBTree::_lock_and_flush_root(PagePtr root)
    {
        // acquire an exclusive lock on the tree here
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
    MutableBTree::_lock_and_flush_page(NodePtr node)
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
    MutableBTree::_remove_page_internal(PagePtr page,
                                        PagePtr parent)
    {
        // remove the page from the parent
        parent->remove(page->prev_key);

        // remove the page from the parent's children
        parent->remove_child(page->extent_id);

        // mark the page flushed
        page->flushed = true;
    }

    void
    MutableBTree::_remove_page(PagePtr page,
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
    MutableBTree::_lock_and_remove_page(NodePtr node)
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

    void
    MutableBTree::_init_schemas(ExtentSchemaPtr schema,
                                const std::vector<std::string> &keys)
    {
        _leaf_schema = schema;

        // construct the field tuples for the leaf nodes
        _leaf_fields = _leaf_schema->get_mutable_fields();
        _leaf_keys = _leaf_schema->get_mutable_fields(keys);

        // construct the schema for the branches
        SchemaColumn child("child", 0, SchemaType::UINT64, false);
        _branch_schema = _leaf_schema->create_schema(keys, { child });

        // construct the field tuples for the branch nodes
        _branch_keys = _branch_schema->get_mutable_fields(keys);
        _branch_child_f = _branch_schema->get_mutable_field("child");
    }
}
