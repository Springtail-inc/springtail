#include <storage/btree_common.hh>
#include <storage/mutable_btree.hh>

namespace springtail {

    MutableBTree::MutableBTree(std::shared_ptr<IOHandle> handle,
                               uint64_t file_id,
                               const std::vector<std::string> &keys,
                               PageCachePtr cache,
                               ExtentSchemaPtr schema)
        : _cache(cache),
          _handle(handle),
          _file_id(file_id),
          _sort_keys(keys),
          _xid(0),
          _finalized(true)
    {
        // initialize the schema information
        _init_schemas(schema, keys);
    }

    void
    MutableBTree::init_empty()
    {
        // must not have already called init() or init_empty()
        assert(_root == nullptr);

        // construct an empty extent at XID 0 -- an invalid XID
        // note: this extent will be replaced after mutations to the tree are performed following
        //       a call to set_xid()
        ExtentPtr extent = std::make_shared<Extent>(_leaf_schema, ExtentType(false, true), 0);

        // create an empty root page
        // note: the implementation depends on a root always existing, but we still consider
        //       the tree finalized at the provided XID until a new target XID is set
        _root = std::make_shared<Page>(shared_from_this(), extent, _leaf_keys);

        // add the root to the cache
        // note: we do not release it, leaving it's use-count permanently at 1
        boost::unique_lock cache_lock(_cache->mutex);
        _cache_insert(_root, cache_lock, false);
    }

    void
    MutableBTree::init(uint64_t root_offset)
    {
        // must not have already called init() or init_empty()
        assert(_root == nullptr);

        // construct an empty page to populate
        _root = std::make_shared<Page>(shared_from_this(), root_offset);

        // read the root
        _read_page_internal(_root);

        // set the XID based on the root
        _xid = _root->xid();

        // add the root to the cache
        // note: we do not release it, leaving it's use-count permanently at 1
        boost::unique_lock cache_lock(_cache->mutex);
        _cache_insert(_root, cache_lock, false);
    }

    void
    MutableBTree::set_xid(uint64_t xid)
    {
        // acquire an exclusive lock on the tree
        boost::unique_lock lock(_mutex);

        // update the target XID and remove the finalized flag
        _xid = xid;
        _finalized = false;
    }

    void
    MutableBTree::insert(TuplePtr value)
    {
        // must have called init() or init_empty()
        assert(_root != nullptr);

        // acquire a shared lock on the btree
        boost::shared_lock tree_lock(_mutex);

        // make sure that we can modify this tree
        assert(!_finalized);

        // get the search key for this value
        TuplePtr search_key = _leaf_schema->tuple_subset(value, _sort_keys);

        // traverse the tree to find the correct leaf page along with the pages along the path
        // to it from the root, get exclusive lock on the leaf
        NodePtr node = _find_leaf(search_key);
        PagePtr parent = (node->parent) ? node->parent->page : nullptr;

        // insert the value into the page
        boost::unique_lock page_lock(node->page->mutex);
        node->page->insert(search_key, value, _leaf_fields);

        // check if this page needs to be flushed
        bool do_flush = node->page->check_flush();
        page_lock.unlock();

        if (do_flush) {
            // check if this is the root
            if (parent == nullptr) {
                // XXX is this safe?
                tree_lock.unlock();

                // will exclusive lock the tree and flush the root
                _lock_and_flush_root(node);
            } else {
                // will first lock the parent, then lock the page and flush it
                _lock_and_flush_page(node);
            }
        }

        boost::unique_lock cache_lock(_cache->mutex);
        if (!do_flush) {
            // need to update the cache with the updated size of the page
            _cache_update_size(node->page, cache_lock);
        }

        // release the nodes back to the cache
        // note: we don't call release on the root since it's never acquired via _cache_get()
        while (node->parent != nullptr) {
            _cache_release(node->page);

            // note: the auto-destructor will implicitly unlock the node's lock if one is held
            node = node->parent;
        }
    }

    void
    MutableBTree::remove(TuplePtr value)
    {
        // must have called init() or init_empty()
        assert(_root != nullptr);

        // acquire a shared lock on the btree
        boost::shared_lock tree_lock(_mutex);

        // make sure that we can modify this tree
        assert(!_finalized);

        // get the search key for this value
        TuplePtr search_key = _leaf_schema->tuple_subset(value, _sort_keys);
        // auto &&search_key = value.get_fields(_sort_key);

        // traverse the tree to find the appropriate page to remove from
        NodePtr node = _find_leaf(search_key);

        // remove the value from the page
        boost::unique_lock page_lock(node->page->mutex);
        node->page->remove(search_key);

        // check if this page needs to be removed
        bool is_empty = node->page->empty();
        page_lock.unlock();

        if (is_empty) {
            // check if this is a non-root page
            if (node->parent != nullptr) {
                _lock_and_remove_page(node);
            }
        }

        boost::unique_lock cache_lock(_cache->mutex);
        if (!is_empty) {
            // need to update the cache with the updated size of the page
            _cache_update_size(node->page, cache_lock);
        }

        // release the nodes back to the cache
        // note: we don't call release on the root since it's never acquired via _cache_get()
        while (node->parent != nullptr) {
            _cache_release(node->page);

            // note: the auto-destructor will implicitly unlock the node's lock if one is held
            node = node->parent;
        }
    }

    uint64_t
    MutableBTree::finalize()
    {
        // must have called init() or init_empty()
        assert(_root != nullptr);

        // acquire an exclusive lock on the tree here
        boost::unique_lock lock(_mutex);

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
        if (this->empty()) {
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
            SPDLOG_INFO("Failed to remove entry");
            search_key->print();
            return; // no such entry found
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

        // note: we currently don't perform cross-Page merge here as it's handled during GC vacuum
        //       but we could check if the size of this extent and the one before / after would make
        //       the threshold for a single extent.  One potentially better approach is to keep the
        //       size of the child in the pointer and then do split / merge as we traverse down the
        //       tree rather than cleaning up afterward.
    }

    std::vector<std::shared_ptr<MutableBTree::Page>>
    MutableBTree::Page::flush_empty_root(uint64_t xid)
    {
        // note: we must be holding the disk_mutex, so no need to lock the access mutex

        // since this is an empty root, ensure that it is now a leaf node
        ExtentType type(false, true);

        // update the extent header
        _extents[0]->header().xid = xid;
        _extents[0]->header().prev_offset = this->extent_id;
        _extents[0]->header().type = type;

        // write the empty extent
        auto &&response = _extents[0]->async_flush(_btree->_handle).get();

        // construct the page this way to ensure we don't try to extract a prev_key
        PagePtr page = std::make_shared<Page>(_btree, response->offset);
        page->set_extent(_extents[0], _key_fields);

        return std::vector<PagePtr>({page});
    }

    std::vector<std::shared_ptr<MutableBTree::Page>>
    MutableBTree::Page::flush(uint64_t xid)
    {
        // note: we must be holding the disk_mutex, so no need to lock the access mutex
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

            futures.push_back(e->async_flush(_btree->_handle));
        }

        // as the writes complete, create the new pages
        for (int i = 0; i < futures.size(); i++) {
            auto &&response = futures[i].get();

            ValueTuplePtr key = std::make_shared<ValueTuple>(_key_fields->bind(_extents[i]->back()));
            PagePtr page = std::make_shared<Page>(_btree, response->offset,
                                                  key, _extents[i], _key_fields);

            new_pages.push_back(page);
        }

        return new_pages;
    }

    void
    MutableBTree::Page::set_extent(ExtentPtr extent,
                                   MutableFieldArrayPtr key_fields)
    {
        // note: we must be holding the disk_mutex, so no need to lock the access mutex

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
        auto &&i = _cache->lookup.find({_file_id, extent_id});
        if (i == _cache->lookup.end()) {
            return nullptr;
        }
        PageCache::LookupEntry &entry = i->second;

        // remove the page from the LRU list if on the list
        if (std::get<2>(entry) == 0) {
            _cache->lru.erase(std::get<1>(entry));
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
        auto &&i = _cache->lookup.find(page->get_lookup_id());

        // it's possible that the page was evicted due to a flush, in which case it will no
        // longer be in the cache
        if (i == _cache->lookup.end()) {
            return;
        }

        // decrement the usage count
        int &usage = std::get<2>(i->second);
        --usage;

        // if zero then add to the LRU list
        if (usage == 0) {
            _cache->lru.push_front(page);
            std::get<1>(i->second) = _cache->lru.begin();
        }
    }

    void
    MutableBTree::_cache_insert_empty(PagePtr page)
    {
        // place the page into the cache, but not into the LRU queue
        // note: set the use count to 1 since the caller is actively using the page
        PageCache::LookupEntry entry{ page, _cache->lru.end(), 1, 0 };
        _cache->lookup.insert_or_assign(page->get_lookup_id(), std::move(entry));
    }

    void
    MutableBTree::_cache_insert(PagePtr page,
                                boost::unique_lock<boost::shared_mutex> &cache_lock,
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
    MutableBTree::_cache_update_size(PagePtr update_page,
                                     boost::unique_lock<boost::shared_mutex> &cache_lock)
    {
        // update the size of the cache given the page size
        auto &&i = _cache->lookup.find(update_page->get_lookup_id());
        assert(i != _cache->lookup.end());
        PageCache::LookupEntry &entry = i->second;

        // update the size of the cache based on the new size of the entry
        // note: atomic access to the size
        uint32_t page_size = update_page->size;

        _cache->size = _cache->size - std::get<3>(entry) + page_size;
        std::get<3>(entry) = page_size;

        // evict pages until the size is under the watermark
        while (_cache->size > _cache->max_size) {
            // if somehow there are no evictable pages, print a warning and return
            if (_cache->lru.empty()) {
                assert(0); // XXX we should never hit this case
            }

            // get a page
            // note: coming off the LRU we know that no one else is currently using this page
            PagePtr page = _cache->lru.back();
            _cache->lru.pop_back();

            // find the hash entry and set the usage count
            auto &lru_entry = _cache->lookup.find(page->get_lookup_id())->second;
            std::get<2>(lru_entry) = 1;

            // save the entry size to reduce the cache size after the page flush
            uint32_t entry_size = std::get<3>(lru_entry);

            // check if we need to try another entry
            // note: if the page is the root we can't evict; if the page has potentially dirty
            //       children, must evict them first
            if (page->type.is_root() || page->has_children()) {
                _cache->lru.push_front(page);
                std::get<1>(lru_entry) = _cache->lru.begin();
                std::get<2>(lru_entry) = 0;
                continue;
            }

            // check if we can remove immediately
            // note: if the page is not dirty, was already flushed, or has never been accessed since being
            //       created from a flush, then can evict immedately
            PagePtr parent = page->parent();
            if (!page->is_dirty() || page->flushed || parent == nullptr) {
                _cache->lookup.erase(page->get_lookup_id());
                _cache->size -= entry_size;
                continue;
            }

            // unlock the cache for others to proceed
            cache_lock.unlock();

            // scoped for locks
            {
                // currently looks evictable and requires a flush; acquire the parent disk_mutex to
                // ensure it doesn't get flushed
                boost::shared_lock parent_lock(parent->disk_mutex);

                // lock the page's disk_mutex since we are about to flush
                // note: should never block since we removed the entry from the cache while it wasn't in use
                boost::unique_lock page_lock(page->disk_mutex);

                // check if the page was flushed while we were acquiring locks
                if (!page->flushed) {
                    // flush the page
                    // note: we don't cache the newly created pages since we are trying to free space
                    _flush_page_internal(page, parent);
                }
            }

            // re-lock the cache and continue
            cache_lock.lock();

            // re-find the cache entry
            auto &&i = _cache->lookup.find(page->get_lookup_id());

            // free the space before the next check
            _cache->size -= std::get<3>(i->second);

            // remove the page from the cache now that it has been replaced
            // note: we don't do this earlier since a reader could come in requesting the page and
            //       then there would be a race which could result in the page being re-read from
            //       disk and modified, resulting in invalid behavior
            _cache->lookup.erase(i);
        }
    }

    void
    MutableBTree::_cache_evict(uint64_t extent_id)
    {
        // find the entry if it exists
        auto &&i = _cache->lookup.find({_file_id, extent_id});
        if (i == _cache->lookup.end()) {
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
            _cache->lru.erase(std::get<1>(entry));
        }

        // remove the entry from the cache
        _cache->size -= std::get<3>(entry);
        _cache->lookup.erase(i);
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

    MutableBTree::NodePtr
    MutableBTree::_read_page(uint64_t extent_id,
                             NodePtr parent,
                             boost::shared_lock<boost::shared_mutex> &parent_lock)
    {
        // lock the cache
        boost::unique_lock cache_lock(_cache->mutex);

        // check if the entry already exists
        PagePtr page = _cache_get(extent_id);
        if (page != nullptr) {
            // no longer need to access the cache, release the lock
            cache_lock.unlock();

            // once we have a pointer to the page we can release the parent
            parent_lock.unlock();

            // note: acquiring the disk_mutex of the new page ensures that the page data has been
            //       populated before continuing
            NodePtr node = std::make_shared<Node>(parent, page);

            // if the page got flushed while we were acquiring the disk_mutex, return and retry
            if (page->flushed) {
                // note: implicitly unlocks the disk_mutex
                return nullptr;
            }

            // update the parent/child inter-link; may already be set but won't change
            page->set_parent(parent->page);
            parent->page->add_child(page);

            // return the new node
            return node;
        }

        // no page in the cache, so construct a page that we can populate
        page = std::make_shared<Page>(shared_from_this(), extent_id);

        // prevent others from using the page until the data is available
        // note: this will never block
        boost::unique_lock data_lock(page->disk_mutex);

        // place the page into the cache, but not into the LRU queue
        // note: this placeholder could be found by other threads, but will block since we will
        //       hold an exclusive lock on the page
        _cache_insert_empty(page);

        // release the cache for other threads until we have populated the data for this page
        // note: the exclusive lock on the page will prevent others from progressing even if
        //       they try to access this page
        cache_lock.unlock();

        // once we have a pointer to the page we can release the parent
        parent_lock.unlock();

        // update the page book-keeping
        page->set_parent(parent->page);
        parent->page->add_child(page);

        // read extent from disk and update the page metadata
        _read_page_internal(page);

        // data has been read, so can let others proceed by downgrading the lock
        NodePtr node = std::make_shared<Node>(parent, page, std::move(data_lock));
             
        // now re-acquire the cache lock to update the size of the cache
        cache_lock.lock();

        // update the size of the page in the cache; may cause evictions
        _cache_update_size(page, cache_lock);

        // return the node for the new page
        return node;
    }

    MutableBTree::NodePtr
    MutableBTree::_find_leaf(TuplePtr key)
    {
        // safe because the root pointer can't change if we are holding the btree lock
        NodePtr node = std::make_shared<Node>(nullptr, _root);

        // if the root is the leaf, lock and return
        if (_root->type.is_leaf()) {
            return node;
        }

        // iterate through the levels until we find a leaf page
        while (node->page->type.is_branch()) {
            // lock page for read access
            boost::shared_lock page_lock(node->page->mutex);

            // use a lower_bound() check to find the appropriate child branch
            auto &&i = node->page->lower_bound(key);

            // note: we know the tree isn't empty since the root would be a leaf in that case
            Extent::Row row = (i == node->page->end()) ? node->page->back() : *i;

            // retrieve the child offset
            uint64_t extent_id = _branch_child_f->get_uint64(row);

            // read the child page; will handle updating the locks
            // note: this will release the page_lock once it has a pointer to the child in memory
            NodePtr child = _read_page(extent_id, node, page_lock);

            // if the child was flushed while we were reading the page, we need to re-find it in the parent
            if (!child) {
                continue;
            }

            // recurse to the child
            node = child;
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
        std::vector<PagePtr> &&new_pages = page->flush(_xid);

        // lock the parent for exclusive access
        boost::unique_lock parent_lock(parent->mutex);

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
        // acquire exclusive access to the page to write to disk
        boost::unique_lock data_lock(page->disk_mutex);

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
            boost::unique_lock cache_lock(_cache->mutex);
            _cache_evict(page->extent_id);
        } else {
            // perform the actual page flush
            std::vector<PagePtr> &&new_pages = _flush_page_internal(page, parent);

            // evict the page from the cache and add the new pages
            boost::unique_lock cache_lock(_cache->mutex);

            _cache_evict(page->extent_id);
            for (auto &&new_page : new_pages) {
                _cache_insert(new_page, cache_lock);
            }
        }
    }

    MutableBTree::PagePtr
    MutableBTree::_flush_root(PagePtr page)
    {
        // acquire exclusive access to the root page
        boost::unique_lock data_lock(page->disk_mutex);

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
            ? page->flush_empty_root(_xid)
            : page->flush(_xid);

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
            new_root = std::make_shared<Page>(shared_from_this(), extent_id, key, extent, _branch_keys);

            // note: we don't need to add these new pages as children because they are clean
            //       pages that haven't been traversed for modification
        } else {
            // if there's just one page, it will replace the root
            new_root = new_pages[0];
            new_pages.pop_back();
        }

        // mark the old root as flushed
        page->flushed = true;

        // evict the old root and add the new pages to the cache since they are no longer roots
        // note: no one should be using this page since we have exclusive lock on both the
        //       parent and the page, meaning no one is holding the page and no one could have
        //       found the page again after releasing it
        boost::unique_lock cache_lock(_cache->mutex);

        _cache_evict(page->extent_id);
        for (auto &&new_page : new_pages) {
            _cache_insert(new_page, cache_lock);
        }

        // insert the new root with a use-count of 1
        _cache_insert(new_root, cache_lock, false);

        return new_root;
    }

    void
    MutableBTree::_lock_and_flush_root(NodePtr node)
    {
        // release the shared disk_mutex on the root since we want to flush it
        node->lock.unlock();

        // acquire an exclusive lock on the tree here
        // XXX is this required?  feels like it is, but is it safe?
        boost::unique_lock lock(_mutex);

        // check if this is still the root of the tree or if it's already been flushed
        if (node->page != _root) {
            return;
        }

        // now flush the root
        PagePtr new_root = _flush_root(node->page);

        // set the root pointer for the BTree
        // note; if returned null then was flushed by another thread
        if (new_root != nullptr) {
            _root = new_root;
        }
    }

    void
    MutableBTree::_lock_and_flush_page(NodePtr node)
    {
        PagePtr parent = node->parent->page;

        // note: we are holding a shared lock on the parent's disk_mutex already
        // if the parent has already been flushed, then by definition this page was also flushed
        if (parent->flushed) {
            return;
        }

        // release the shared lock on the disk_mutex since we want to flush the page
        node->lock.unlock();

        // now that we are holding the parent, flush the page
        _flush_page(node->page, parent);

        // move to the parent
        node = node->parent;

        // check if the parent needs to be flushed
        if (node->page->check_flush()) {
            // check if the parent being flushed is the root of the tree
            if (node->parent == nullptr) {
                // will re-acquire an exclusive lock on tree and then flush the root and update it
                _lock_and_flush_root(node);
            } else {
                // will lock the parent's parent and then lock and flush the parent
                _lock_and_flush_page(node);
            }
        } else {
            // otherwise, update the parent's size in the cache
            boost::unique_lock cache_lock(_cache->mutex);
            _cache_update_size(node->page, cache_lock);
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
        boost::unique_lock lock(page->disk_mutex);

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
        boost::unique_lock cache_lock(_cache->mutex);
        _cache_evict(page->extent_id);
    }

    void
    MutableBTree::_lock_and_remove_page(NodePtr node)
    {
        // note: already have the parent page pinned with shared lock on disk_mutex
        PagePtr parent = node->parent->page;

        // if the parent was flushed, then the removal would have been processed during the
        // page's flush() operation
        if (parent->flushed) {
            return;
        }

        // release the shared lock on the disk_mutex to allow for exclusive access
        node->lock.unlock();

        // now that we are holding the parent, try to remove the page
        _remove_page(node->page, parent);

        // the parent would have gotten smaller, so no need to flush, but if the parent is empty
        // then we can remove it as well
        if (parent->empty()) {
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
        SchemaColumn child(BTREE_CHILD_FIELD, 0, SchemaType::UINT64, false);
        _branch_schema = _leaf_schema->create_schema(keys, { child });

        // construct the field tuples for the branch nodes
        _branch_keys = _branch_schema->get_mutable_fields(keys);
        _branch_child_f = _branch_schema->get_mutable_field(BTREE_CHILD_FIELD);
    }
}
