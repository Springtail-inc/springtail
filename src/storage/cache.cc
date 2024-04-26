#include <storage/cache.hh>

namespace springtail {

    /* static member initialization must happen outside of class */
    StorageCache* StorageCache::_instance = {nullptr};
    boost::mutex StorageCache::_instance_mutex;

    StorageCache *
    StorageCache::get_instance()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new StorageCache();
        }

        return _instance;
    }

    void
    StorageCache::shutdown()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    StorageCache::PagePtr
    StorageCache::get(const std::filesystem::path &file,
                      uint64_t extent_id,
                      uint64_t access_xid,
                      uint64_t target_xid,
                      uint64_t table_id)
    {
        // note: target_xid must be at or beyond the access_xid
        assert(target_xid >= access_xid);

        boost::unique_lock lock(_mutex);

        // if target is the same as access, get the page and return it
        if (target_xid == access_xid || target_xid == constant::LATEST_XID) {
            return _get_page(file, extent_id, access_xid);
        }

        // if the target is ahead of the access, but there is no provided table_id then it means the
        // caller is going to perform the mutations (for non-data extents)
        if (table_id == 0) {
            // check if we have a page at the requested target_xid already in the cache, if so return it
            PagePtr page = _try_get_page(file, extent_id, target_xid);
            if (page != nullptr) {
                return page;
            }

            // XXX create a copy of the page at the access_xid and then add it to the cache at the target_xid

            return page;
        }

        // note: from here forward, we know we are dealing with a roll-forward table data page
        assert(0);
#if 0
        // XXX take ownership of the roll-forward process, or block until it's complete

        // check if there are pending changes we need to apply to this page
        bool has_changes = gc::extent_handler->check_changes(table_id, extent_id, access_xid, target_xid);

        // get the page at the access_xid
        auto page = _get_page(file, extent_id, access_xid);

        // no changes, so can just return the the access_xid as the target_xid page
        if (!has_changes) {
            // XXX mark the page as valid through the target_xid

            return page;
        }

        // XXX construct a copy of the page that can be used for roll-forward

        // pass the page to the garbage collector to apply pending changes
        page = gc::apply_changes(table_id, page, access_xid, target_xid);

        // XXX cache the new page valid from the target_xid and return to the caller

        return page;
#endif
    }

    StorageCache::PagePtr
    StorageCache::_try_get_page(const std::filesystem::path &file,
                                uint64_t extent_id,
                                uint64_t xid)
    {
        CacheKey key(file, extent_id);

        // check for the key in the hash map
        auto write_i = _page_cache.find(key);
        if (write_i == _page_cache.end()) {
            return nullptr;
        }

        // check for the xid in the XID map
        auto page_i = write_i->second.lower_bound(xid);
        if (page_i == write_i->second.end()) {
            return nullptr;
        }

        // check if the page is valid through the requested xid
        auto page = page_i->second.first;
        if (!page->check_xid_valid(xid)) {
            return nullptr;
        }

        // if the page is on the LRU list, remove it
        if (page_i->second.second != _page_lru.end()) {
            _page_lru.erase(page_i->second.second);
        }

        // increment it's use count
        ++(page->use_count);

        return page;
    }

    StorageCache::PagePtr
    StorageCache::_get_page(const std::filesystem::path &file,
                            uint64_t extent_id,
                            uint64_t xid)
    {
        // check if the page already exists in the cache
        PagePtr page = _try_get_page(file, extent_id, xid);
        if (page != nullptr) {
            return page;
        }

        // not in the cache, create the page object with the given <file, extent_id> valid at the requested XID
        page = std::make_shared<Page>(file, extent_id, xid, xid);

        // add it to the cache; note: use count starts at 1
        _page_cache[page->key()].insert({xid, WriteCacheValue(page, _page_lru.end()) });

        // return it
        return page;
    }
    
    void
    StorageCache::put(PagePtr page)
    {
        boost::unique_lock lock(_mutex);

        // find the cache entry
        auto page_i = _page_cache.find(page->key());
        assert(page_i != _page_cache.end());

        // decrement it's use count
        --(page->use_count);

        // if the page has no users, place it onto the back of the LRU list
        if (page->use_count == 0) {
            page_i->second[page->xid()].second = _page_lru.insert(_page_lru.end(), page);
        }
    }

    StorageCache::Page::Page(const std::filesystem::path &file,
                             uint64_t extent_id,
                             uint64_t start_xid,
                             uint64_t end_xid)
        : _is_dirty(false),
          _file(file),
          _extent_id(extent_id),
          _start_xid(start_xid),
          _end_xid(end_xid)
    {
        // single un-modified extent 
        _extents.push_back(_extent_id);
    }

    std::vector<uint64_t>
    StorageCache::Page::flush(uint64_t flush_xid, ExtentType type, uint64_t table_id, uint64_t index_id)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = false;

        std::vector<uint64_t> offsets;
        for (auto &var : _extents) {
            if (std::holds_alternative<CacheExtentPtr>(var)) {
                auto &e = std::get<CacheExtentPtr>(var);

                // update the extent header
                e->header().type = type;
                e->header().xid = flush_xid;
                e->header().prev_offset = _extent_id;

                // XXX do we need to set these?  they should already be set correctly I think
                e->header().table_id = table_id;
                e->header().index_id = index_id;

                // append the extent to the file
                // XXX could do these asynchronously to get better parallelism when there are multiple extents
                uint64_t extent_id = e->flush();

                // return the clean extent back to the read cache
                StorageCache::get_instance()->_read_cache.reinsert(e);

                // XXX need to track the cache size bookkeeping

                // save the extent ID of the now-unmodified extent
                var = extent_id;
            }

            offsets.push_back(std::get<uint64_t>(var));
        }
        return offsets;
    }

    uint64_t
    StorageCache::Page::reduce_size(uint64_t count)
    {
        // XXX should maintain an LRU of the dirty extents
        return 0;
    }

    StorageCache::Page::Iterator
    StorageCache::Page::lower_bound(TuplePtr tuple)
    {
        boost::shared_lock lock(_mutex);

        // perform a lower-bound check to find the appropriate extent
        auto extent_i = std::ranges::lower_bound(_extents, *tuple,
                                                 [](const Tuple &lhs, const Tuple &rhs) {
                                                     return lhs.less_than(rhs);
                                                 },
                                                 [this](const ExtentVar &var) {
                                                     SafeExtent extent(_file, var);
                                                     return FieldTuple(this->_sort_fields, (*extent)->back());
                                                 });
        if (extent_i == _extents.end()) {
            return end();
        }

        SafeExtent extent(_file, *extent_i);

        // perform a lower-bound check to find the appropriate row within the extent
        auto row_i = std::ranges::lower_bound(**extent, *tuple,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [this](const Extent::Row &row) {
                                                  return FieldTuple(this->_sort_fields, row);
                                              });

        // note: shouldn't be possible to hit end() given the above lower_bound() check to find the extent
        assert(row_i != (*extent)->end());

        return Iterator(this, extent_i, std::move(extent), row_i);
    }

    void
    StorageCache::Page::insert(TuplePtr tuple)
    {
        boost::shared_lock lock(_mutex);
        _is_dirty = true;

        // extract the key to find the insert position
        auto key = _schema->tuple_subset(tuple, _sort_keys);

        // find the extent to modify via lower_bound
        auto extent_i = std::ranges::lower_bound(_extents, *key,
                                                 [](const Tuple &lhs, const Tuple &rhs) {
                                                     return lhs.less_than(rhs);
                                                 },
                                                 [this](const ExtentVar &var) {
                                                     SafeExtent extent(_file, var);
                                                     return FieldTuple(this->_sort_fields, (*extent)->back());
                                                 });
        if (extent_i == _extents.end()) {
            extent_i = --_extents.end();
        }

        // make sure that we've got a mutable version of the extent
        SafeExtent extent(_file, *extent_i, true);

        // find the insert position in the extent
        auto row_i = std::ranges::lower_bound(**extent, *key,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [this](const Extent::Row &row) {
                                                  return FieldTuple(_sort_fields, row);
                                              });

        // note: row's key should *not* match the tuple's key
        assert(!FieldTuple(_sort_fields, *row_i).equal(*key));

        // insert the tuple into the extent
        auto row = (*extent)->insert(row_i);
        MutableTuple((*extent)->schema()->get_mutable_fields(), row).assign(tuple);

        // XXX check for split
    }

    void
    StorageCache::Page::append(TuplePtr tuple)
    {
        boost::shared_lock lock(_mutex);
        _is_dirty = true;

        // retrieve the last extent
        auto &var = _extents.back();
        SafeExtent extent(_file, var, true);

        // append a row
        auto row = (*extent)->append();

        // set the value
        MutableTuple((*extent)->schema()->get_mutable_fields(), row).assign(tuple);

        // XXX check for split
    }

    void
    StorageCache::Page::upsert(TuplePtr tuple)
    {
        boost::shared_lock lock(_mutex);
        _is_dirty = true;

        // extract the key to find the insert position
        auto key = _schema->tuple_subset(tuple, _sort_keys);

        // find the extent to modify via lower_bound
        auto extent_i = std::ranges::lower_bound(_extents, *key,
                                                 [](const Tuple &lhs, const Tuple &rhs) {
                                                     return lhs.less_than(rhs);
                                                 },
                                                 [this](const ExtentVar &var) {
                                                     SafeExtent extent(_file, var);
                                                     return FieldTuple(_sort_fields, (*extent)->back());
                                                 });
        if (extent_i == _extents.end()) {
            extent_i = --_extents.end();
        }

        // make sure that we've got a mutable version of the extent
        SafeExtent extent(_file, *extent_i, true);

        // find the insert position in the extent
        auto row_i = std::ranges::lower_bound(**extent, *key,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [this](const Extent::Row &row) {
                                                  return FieldTuple(_sort_fields, row);
                                              });

        // see if the row's key matches the tuple's key
        if (row_i != (*extent)->end() && FieldTuple(_sort_fields, *row_i).equal(*key)) {
            // update the existing row
            MutableTuple((*extent)->schema()->get_mutable_fields(), *row_i).assign(tuple);
        } else {
            // insert the tuple into the extent
            auto row = (*extent)->insert(row_i);
            MutableTuple((*extent)->schema()->get_mutable_fields(), row).assign(tuple);
        }

        // XXX check for split
    }

    void
    StorageCache::Page::update(TuplePtr tuple)
    {
        boost::shared_lock lock(_mutex);
        _is_dirty = true;

        // extract the key to find the insert position
        auto key = _schema->tuple_subset(tuple, _sort_keys);

        // find the extent to modify via lower_bound
        auto extent_i = std::ranges::lower_bound(_extents, *key,
                                                 [](const Tuple &lhs, const Tuple &rhs) {
                                                     return lhs.less_than(rhs);
                                                 },
                                                 [this](const ExtentVar &var) {
                                                     SafeExtent extent(_file, var);
                                                     return FieldTuple(_sort_fields, (*extent)->back());
                                                 });
        // note: key should exist
        assert(extent_i == _extents.end());

        // make sure that we've got a mutable version of the extent
        SafeExtent extent(_file, *extent_i, true);

        // find the insert position in the extent
        auto row_i = std::ranges::lower_bound(**extent, *key,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [this](const Extent::Row &row) {
                                                  return FieldTuple(_sort_fields, row);
                                              });

        // note: row's key should match the tuple's key
        assert(FieldTuple(_sort_fields, *row_i).equal(*key));

        // update the existing row
        MutableTuple((*extent)->schema()->get_mutable_fields(), *row_i).assign(tuple);

        // XXX check for split
    }

    void
    StorageCache::Page::remove(TuplePtr key)
    {
        boost::shared_lock lock(_mutex);
        _is_dirty = true;

        // find the extent to modify via lower_bound
        auto extent_i = std::ranges::lower_bound(_extents, *key,
                                                 [](const Tuple &lhs, const Tuple &rhs) {
                                                     return lhs.less_than(rhs);
                                                 },
                                                 [this](const ExtentVar &var) {
                                                     SafeExtent extent(_file, var);
                                                     return FieldTuple(_sort_fields, (*extent)->back());
                                                 });
        // note: key should exist
        assert(extent_i == _extents.end());

        // make sure that we've got a mutable version of the extent
        SafeExtent extent(_file, *extent_i, true);

        // find the insert position in the extent
        auto row_i = std::ranges::lower_bound(**extent, *key,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [this](const Extent::Row &row) {
                                                  return FieldTuple(_sort_fields, row);
                                              });

        // note: row's key should match the tuple's key
        assert(FieldTuple((*extent)->schema()->get_fields(), *row_i).equal(*key));

        // remove the row
        (*extent)->remove(row_i);

        // XXX check for merge or entire extent removal
    }

    StorageCache::CacheExtentPtr
    StorageCache::ReadCache::get(const std::filesystem::path &file,
                                 uint64_t extent_id)
    {
        CacheKey key(file, extent_id);

        boost::unique_lock lock(_mutex);

        CacheExtentPtr extent = nullptr;
        while (extent == nullptr) {
            // search for the requested extent
            auto cache_i = _cache.find(key);
            if (cache_i != _cache.end()) {
                extent = cache_i->second.first;

                // remove the entry from the LRU list
                _lru.erase(cache_i->second.second);
                cache_i->second.second = _lru.end();

                // update the use count
                extent->increment_use();

                // exit the loop
                continue;
            }

            // extent not cached, check if someone is reading it from disk
            auto io_i = _io_map.find(key);
            if (io_i != _io_map.end()) {
                // wait for the read to complete
                auto &cv = io_i->second;
                cv.wait(lock);

                // note: try to retrieve from the cache again
            } else {
                // add the condition variable to the IO map
                _io_map[key];

                // unlock before IO
                lock.unlock();

                // read the extent
                auto handle = IOMgr::get_instance()->open(file, IOMgr::READ, true);
                auto response = handle->read(extent_id);
                extent = std::make_shared<CacheExtent>(response->data, file, extent_id);

                // reacquire the lock once IO complete
                lock.lock();

                // make space in the cache for the extent
                _make_space(extent->byte_count());

                // insert the extent into the cache
                // note: we don't place into the LRU list since the extent will be in-use
                _cache.insert({ key, { extent, _lru.end() } });
                _size += extent->byte_count();

                // notify the other callers waiting for this extent
                _io_map[key].notify_all();
            }
        }

        // return the extent
        return extent;
    }

    void
    StorageCache::ReadCache::put(CacheExtentPtr extent)
    {
        boost::unique_lock lock(_mutex);

        // release the extent
        _release(extent);
    }

    StorageCache::CacheExtentPtr
    StorageCache::ReadCache::extract(CacheExtentPtr extent)
    {
        boost::unique_lock lock(_mutex);

        // check if the caller is the only user
        if (extent->use_count() > 1) {
            // there are other users, so we need to return a copy of this extent
            auto new_extent = std::make_shared<CacheExtent>(*extent);

            // make space in the cache for the new page extent
            _make_space(new_extent->byte_count());

            // release the existing extent back to the cache
            _release(extent);
        } else {
            // this is the only user, so we can evict this extent and return it
            // note: no need to adjust the LRU queue since the extent cannot be on it
            _cache.erase(extent->key());

            // reduce the total cache size so that the space can be used by the dirty pages
            _max_size -= extent->byte_count();
        }

        // return the extent to the page
        return extent;
    }

    void
    StorageCache::ReadCache::reinsert(CacheExtentPtr extent)
    {
        boost::unique_lock lock(_mutex);

        // places this extent into the read cache
        _cache.insert({ extent->key(), { extent, _lru.end() } });

        // update the sizes of the cache
        _max_size += extent->byte_count();
        _size += extent->byte_count();

        // release the extent
        _release(extent);
    }

    uint32_t
    StorageCache::ReadCache::borrow_space(uint32_t count)
    {
        boost::unique_lock lock(_mutex);

        // evict extents to try to create "count" bytes of space in the cache
        _make_space(count);

        // if we have fewer than "count" bytes free, reduce the count to what is available
        if (_max_size - _size < count) {
            count = static_cast<uint32_t>(_max_size - _size);
        }

        // reduce the max size of the cache to account for the borrowed space
        _max_size -= count;

        return count;
    }

    void
    StorageCache::ReadCache::_make_space(uint32_t count)
    {
        // evict extents until we have created at least "count" space
        while (!_lru.empty() && (_max_size - _size) < count) {
            auto extent = _lru.front();

            // remove from the cache
            _lru.pop_front();
            _cache.erase(extent->key());

            // account for the space created
            _size -= extent->byte_count();
        }
    }

    void
    StorageCache::ReadCache::_release(CacheExtentPtr extent)
    {
        // reduce the use count
        extent->decrement_use();

        // if the use count is zero, place into the LRU list
        if (extent->use_count() == 0) {
            _cache[extent->key()] = { extent, _lru.insert(_lru.end(), extent) };
        }
    }
}
