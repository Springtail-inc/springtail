#include <common/json.hh>
#include <common/properties.hh>

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

    StorageCache::StorageCache()
    {
        // get the cache size
        uint64_t size;
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        Json::get_to<uint64_t>(json, "cache_size", size, 16384);

        _data_cache = std::make_shared<DataCache>(size);
        _page_cache = std::make_shared<PageCache>(size);
    }

    StorageCache::SafePagePtr
    StorageCache::get(const std::filesystem::path &file,
                      uint64_t extent_id,
                      uint64_t access_xid,
                      uint64_t target_xid,
                      bool do_rollforward,
                      SafePagePtr::FlushCb cb )
    {
        // note: target_xid must be at or beyond the access_xid
        assert(target_xid >= access_xid);
        if (target_xid == constant::LATEST_XID) {
            target_xid = access_xid;
        }

        // if the extent ID is UNKNOWN, then we will get an empty page for the file
        if (extent_id == constant::UNKNOWN_EXTENT) {
            return {_page_cache.get(), _page_cache->get_empty(file, target_xid), cb};
        }

        // if target is the same as access, get the page and return it
        if (target_xid == access_xid) {
            return {_page_cache.get(), _page_cache->get(file, extent_id, access_xid, target_xid), cb};
        }

        // if the target is ahead of the access, but there is roll-forward request, then it means
        // the caller is going to perform the roll-forward mutations (for non-data extents)
        if (!do_rollforward) {
            // note: we know that the provided extent_id is valid at the access_xid, so we get the
            //       page at the target_xid using that original extent_id so that the caller can
            //       modify it from that point forward
            return {_page_cache.get(), _page_cache->get(file, extent_id, access_xid, target_xid), cb};
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

    void
    StorageCache::flush(const std::filesystem::path &file)
    {
        _page_cache->flush_file(file);
    }

    void
    StorageCache::drop_for_truncate(const std::filesystem::path &file)
    {
        _page_cache->drop_file(file);
    }


    StorageCache::PagePtr
    StorageCache::PageCache::get(const std::filesystem::path &file,
                                 uint64_t extent_id,
                                 uint64_t access_xid,
                                 uint64_t target_xid)
    {
        assert(extent_id != constant::UNKNOWN_EXTENT);

        SPDLOG_DEBUG_MODULE(LOG_CACHE, "{}, {}, {}, {}", file, extent_id, access_xid, target_xid);

        boost::unique_lock lock(_mutex);

        // check if the page already exists in the cache for the given target XID
        PagePtr page = _try_get(file, extent_id, target_xid);
        if (page != nullptr) {
            SPDLOG_DEBUG_MODULE(LOG_CACHE, "Found in cache");
            return page;
        }

        // XXX eventually use the access_xid and extent_id to get the proper set of extents to start
        //     from; for now we assume that the single extent_id *is* the full list of extents for
        //     the access XID and that the query nodes won't perform any roll-forward on their own.

        // note: not in the cache, need to create a new Page
        return _create(file, extent_id, target_xid, { extent_id });
    }

    StorageCache::PagePtr
    StorageCache::PageCache::get_empty(const std::filesystem::path &file,
                                       uint64_t xid)
    {
        SPDLOG_DEBUG_MODULE(LOG_CACHE, "{}, {}", file, xid);
        boost::unique_lock lock(_mutex);

        _make_page_space(1);
        return std::make_shared<Page>(file, xid);
    }

    void
    StorageCache::PageCache::put(PagePtr page,
                                 std::function<bool(std::shared_ptr<Page>)> flush_callback)
    {
        boost::unique_lock lock(_mutex);

        // set the flush callback for the page if it doesn't have one yet
        if (flush_callback && !page->_flush_callback) {
            page->_register_flush(flush_callback);

            auto &list = _flush_list[page->_file];
            page->_flush_pos = list.insert(list.end(), page);
        }

        if (page->_extent_id == constant::UNKNOWN_EXTENT) {
            // note: we cannot release a dirty page with no original extent ID back to the cache unless it has a flush callback
            assert(!page->_is_dirty || page->_flush_callback);

            // release the space back to the cache
            --_size;
            return;
        }

        // release the page back to the cache
       _put(page);
    }

    void
    StorageCache::PageCache::flush_file(const std::filesystem::path &file)
    {
        boost::unique_lock lock(_mutex);

        // go through the dirty page list for the file
        auto file_i = _flush_list.find(file);
        if (file_i == _flush_list.end()) {
            return; // no dirty pages
        }
        auto &flush_pages = file_i->second;

        bool done = false;
        auto page_i = flush_pages.begin();

        // if the list is empty, remove it from the map and return
        if (page_i == flush_pages.end()) {
            _flush_list.erase(file_i);
            return;
        }

        while (!done) {
            auto page = *page_i;

            // remove the page from the flush list
            flush_pages.erase(page->_flush_pos);
            auto callback = page->_flush_callback;
            page->_register_flush(nullptr);

            lock.unlock();

            // note: we currently assume that the page has a flush callback
            assert(callback);

            // issue the flush callback
            bool success = callback(page);

            // note: we currently assume that the flush callback must succeed here
            assert(success);

            lock.lock();

            // get the next dirty page
            page_i = flush_pages.begin();
            if (page_i == flush_pages.end()) {
                _flush_list.erase(file);
                done = true; // no more dirty pages, exit the loop
            }
        }

        // flush list for the file must be empty, so remove it
        _flush_list.erase(file);
    }

    void
    StorageCache::PageCache::drop_file(const std::filesystem::path &file)
    {
        boost::unique_lock lock(_mutex);

        // go through the dirty page list for the file
        auto file_i = _flush_list.find(file);
        if (file_i == _flush_list.end()) {
            return; // no dirty pages
        }
        auto &drop_pages = file_i->second;

        bool done = false;
        auto page_i = drop_pages.begin();

        // if the list is empty, remove it from the map and return
        if (page_i == drop_pages.end()) {
            _flush_list.erase(file_i);
            return;
        }

        auto data_cache = StorageCache::get_instance()->_data_cache;
        while (!done) {
            auto page = *page_i;

            // clear the associated dirty extents from the cache
            for (auto &ref : page->_extents) {
                if (ref.second) {
                    auto extent = data_cache->get(ref.first);
                    data_cache->drop_dirty(extent);
                    data_cache->put(extent);
                }
            }

            // remove the page from the flush list
            drop_pages.erase(page->_flush_pos);

            // get the next dirty page
            page_i = drop_pages.begin();
            if (page_i == drop_pages.end()) {
                _flush_list.erase(file);
                done = true; // no more dirty pages, exit the loop
            }
        }

        // flush list for the file must be empty, so remove it
        _flush_list.erase(file);
    }

    void
    StorageCache::PageCache::_put(PagePtr page)
    {
        // decrement it's use count
        --(page->_use_count);

        // if the page has no users, place it onto the back of the LRU list
        if (page->_use_count == 0) {
            page->_lru_pos = _lru.insert(_lru.end(), page);
        }
    }

    StorageCache::PagePtr
    StorageCache::PageCache::_create(const std::filesystem::path &file,
                                     uint64_t extent_id,
                                     uint64_t xid,
                                     const std::vector<uint64_t> &offsets)
    {
        SPDLOG_DEBUG_MODULE(LOG_CACHE, "{}, {}, {}, {}", file, extent_id, xid, offsets.size());

        // make space for the page; evict if we need to make space
        _make_page_space(1);

        // create the page object with the given <file, extent_id> valid at the requested XID
        auto page = std::make_shared<Page>(file, extent_id, xid, xid, offsets);

        // add it to the cache; note: use count starts at 1
        _cache[page->key()].insert({ xid, page });

        // return it
        return page;
    }

    void
    StorageCache::PageCache::_try_evict(PagePtr page)
    {
        // issue the associated callback for the page's eviction
        bool success = true;
        if (page->_flush_callback) {
            // clear the page from the flush list
            _flush_list[page->_file].erase(page->_flush_pos);
            auto callback = page->_flush_callback;
            page->_flush_callback = nullptr;

            boost::unique_lock lock(_mutex, boost::adopt_lock);
            lock.unlock();

            success = callback(page);

            lock.lock();
            lock.release();
        }

        if (!success || page->_use_count > 1) {
            // if page can't be evicted then release the page back to the cache
            _put(page);
            return;
        }

        // remove the page from the cache
        auto cache_i = _cache.find(page->key());
        cache_i->second.erase(page->xid());
        if (cache_i->second.empty()) {
            _cache.erase(cache_i);
        }

        // update the sizes
        _size -= page->_extents.size();
    }

    void
    StorageCache::PageCache::_make_page_space(uint32_t space_needed)
    {
        // if there is space in the cache, utilize it
        while (_size + space_needed > _max_size) {
            // try to use any space that is available
            if (_size < _max_size) {
                space_needed -= _max_size - _size;
                _size = _max_size;
            }

            // evict a page from the LRU and then check the sizes again
            auto page = _lru.front();
            _lru.pop_front();

            // take ownership of this page for the eviction
            ++(page->_use_count);

            // try to evict the page
            _try_evict(page);

            // note: once we've performed an eviction, try again to get the space we need
        }

        // at this point we know there is enough space in the cache for the remaining size
        _size += space_needed;
    }

    StorageCache::PagePtr
    StorageCache::PageCache::_try_get(const std::filesystem::path &file,
                                      uint64_t extent_id,
                                      uint64_t xid)
    {
        CacheKey key(file, extent_id);

        // check for the key in the hash map
        auto write_i = _cache.find(key);
        if (write_i == _cache.end()) {
            return nullptr;
        }

        // check for the xid in the XID map
        auto page_i = write_i->second.lower_bound(xid);
        if (page_i == write_i->second.end()) {
            return nullptr;
        }

        // check if the page is valid through the requested xid
        auto page = page_i->second;
        if (!page->check_xid_valid(xid)) {
            return nullptr;
        }

        // if the page is on the LRU list, remove it
        if (page->_use_count == 0) {
            _lru.erase(page->_lru_pos);
        }

        // increment it's use count
        ++(page->_use_count);

        return page;
    }

    StorageCache::Page::Page(const std::filesystem::path &file,
                             uint64_t extent_id,
                             uint64_t start_xid,
                             uint64_t end_xid,
                             const std::vector<uint64_t> &offsets)
        : _use_count(1),
          _is_dirty(false),
          _file(file),
          _extent_id(extent_id),
          _start_xid(start_xid),
          _end_xid(end_xid)
    {
        for (auto offset : offsets) {
            _extents.push_back({ offset, false });
        }
    }

    StorageCache::Page::Page(const std::filesystem::path &file,
                             uint64_t xid)
        : _use_count(1),
          _is_dirty(true),
          _file(file),
          _extent_id(constant::UNKNOWN_EXTENT),
          _start_xid(xid),
          _end_xid(xid)
    {
        // intentionally empty
    }

    uint64_t
    StorageCache::Page::flush_empty(const ExtentHeader &header)
    {
        auto cache = StorageCache::get_instance();

        boost::unique_lock lock(_mutex);
        _is_dirty = false;

        // note: page must be empty
        assert(_extents.empty());

        // create an empty extent
        auto extent = cache->_data_cache->get_empty(_file, header);

        _extents.push_back({ extent->cache_id(), true });
        cache->_data_cache->put(extent);

        // now perform the usual flush()
        auto &&offsets = _flush(header);

        return offsets[0];
    }

    std::vector<uint64_t>
    StorageCache::Page::flush(const ExtentHeader &header)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = false;

        // note: if the page is empty, we should be calling flush_empty()
        assert(!_extents.empty());

        // perform the usual flush()
        return _flush(header);
    }

    StorageCache::Page::Iterator
    StorageCache::Page::lower_bound(TuplePtr tuple, ExtentSchemaPtr schema)
    {
        boost::shared_lock lock(_mutex);

        // perform a lower-bound check to find the appropriate extent
        // note: we don't use std::ranges::lower_bound() here because the projection causes the
        //       SafeExtent to go out of scope before it is used in the comparison
        auto extent_i = std::lower_bound(_extents.begin(), _extents.end(), *tuple,
                                         [this, &schema](const ExtentRef &ref, const Tuple &key) {
                                             SafeExtent extent(_file, ref);
                                             return FieldTuple(schema->get_sort_fields(), (*extent)->back()).less_than(key);
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
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), row);
                                              });

        // note: shouldn't be possible to hit end() given the above lower_bound() check to find the extent
        assert(row_i != (*extent)->end());

        return Iterator(this, extent_i, std::move(extent), row_i);
    }

    StorageCache::Page::Iterator
    StorageCache::Page::upper_bound(TuplePtr tuple, ExtentSchemaPtr schema)
    {
        boost::shared_lock lock(_mutex);

        // perform a upper-bound check to find the appropriate extent
        // note: we don't use std::ranges::upper_bound() here because the projection causes the
        //       SafeExtent to go out of scope before it is used in the comparison
        auto extent_i = std::upper_bound(_extents.begin(), _extents.end(), *tuple,
                                 [this, &schema](const Tuple &key, const ExtentRef &ref) {
                                     SafeExtent extent(_file, ref);
                                     auto tuple = FieldTuple(schema->get_sort_fields(), (*extent)->back());
                                     return key.less_than(tuple);
                                 });

        if (extent_i == _extents.end()) {
            return end();
        }

        SafeExtent extent(_file, *extent_i);

        // perform a upper-bound check to find the appropriate row within the extent
        auto row_i = std::ranges::upper_bound(**extent, *tuple,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), row);
                                              });

        // note: shouldn't be possible to hit end() given the above upper_bound() check to find the extent
        assert(row_i != (*extent)->end());

        return Iterator(this, extent_i, std::move(extent), row_i);
    }

    StorageCache::Page::Iterator
    StorageCache::Page::inverse_lower_bound(TuplePtr tuple, ExtentSchemaPtr schema)
    {
        boost::shared_lock lock(_mutex);

        // check if the page is empty
        if (empty()) {
            return end();
        }

        // perform a lower-bound to find the row with a key <= the provided tuple
        auto i = lower_bound(tuple, schema);
        if (i == end()) {
            --i;
            return i;
        }

        // if the key is equal, return it
        auto key = FieldTuple(schema->get_sort_fields(), *i);
        if (tuple->equal(key)) {
            return i;
        }

        // if we are at the first entry, nothing before it
        if (i == begin()) {
            return end();
        }

        // go to the previous entry
        --i;
        return i;
    }

    StorageCache::Page::Iterator
    StorageCache::Page::at(uint32_t index)
    {
        // iterate through the extents to find the requested index in the page
        for (auto extent_i = _extents.begin(); extent_i != _extents.end(); ++extent_i) {
            SafeExtent extent(_file, *extent_i);

            uint32_t row_count = (*extent)->row_count();
            if (index < row_count) {
                // construct the iterator to the requested position and return it
                return Iterator(this, extent_i, std::move(extent), (*extent)->at(index));
            }

            index -= row_count;
        }

        // index is beyond the end of the page
        return end();
    }

    void
    StorageCache::Page::insert(TuplePtr tuple,
                               ExtentSchemaPtr schema)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = true;

        // if the page is empty, create an empty extent to back it
        if (_extents.empty()) {
            // create an empty extent
            auto cache = StorageCache::get_instance();

            // XXX we should get some kind of RAII object to avoid losing the cache slot on a thrown exception
            ExtentHeader header(ExtentType(), _end_xid, schema->row_size());
            auto extent = cache->_data_cache->get_empty(_file, header);
            _extents.push_back({ extent->cache_id(), true });

            // insert the tuple into the extent
            auto row = extent->append();
            MutableTuple(schema->get_mutable_fields(), row).assign(tuple);

            // release back to the cache
            cache->_data_cache->put(extent);
            return;
        }

        // extract the key to find the insert position
        auto key = schema->tuple_subset(tuple, schema->get_sort_keys());

        // find the extent to modify via lower_bound
        auto extent_i = std::lower_bound(_extents.begin(), _extents.end(), *key,
                                         [this, &schema](const ExtentRef &ref, const Tuple &key) {
                                             SafeExtent extent(_file, ref);
                                             return FieldTuple(schema->get_sort_fields(), (*extent)->back()).less_than(key);
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
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), row);
                                              });

        // note: row's key should *not* match the tuple's key
        assert(row_i == (*extent)->end() ||
               !FieldTuple(schema->get_sort_fields(), *row_i).equal(*key));

        // insert the tuple into the extent
        auto row = (*extent)->insert(row_i);
        MutableTuple(schema->get_mutable_fields(), row).assign(tuple);

        // check for split
        _check_split(extent_i, *extent, schema);
    }

    void
    StorageCache::Page::append(TuplePtr tuple,
                               ExtentSchemaPtr schema)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = true;

        // if the page is empty, create an empty extent to back it
        if (_extents.empty()) {
            // create an empty extent
            auto cache = StorageCache::get_instance();

            // XXX we should get some kind of RAII object to avoid losing the cache slot on a thrown exception
            ExtentHeader header(ExtentType(), _end_xid, schema->row_size());
            auto extent = cache->_data_cache->get_empty(_file, header);
            _extents.push_back({ extent->cache_id(), true });

            // insert the tuple into the extent
            auto row = extent->append();
            MutableTuple(schema->get_mutable_fields(), row).assign(tuple);

            // release back to the cache
            cache->_data_cache->put(extent);
            return;
        }

        // retrieve the last extent
        auto extent_i = --_extents.end();
        SafeExtent extent(_file, *extent_i, true);

        // append a row
        auto row = (*extent)->append();

        // set the value
        MutableTuple(schema->get_mutable_fields(), row).assign(tuple);

        // check for split
        _check_split(extent_i, *extent, schema);
    }

    bool
    StorageCache::Page::upsert(TuplePtr tuple,
                               ExtentSchemaPtr schema)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = true;

        // if the page is empty, create an empty extent to back it
        if (_extents.empty()) {
            // create an empty extent
            auto cache = StorageCache::get_instance();

            // XXX we should get some kind of RAII object to avoid losing the cache slot on a thrown exception
            ExtentHeader header(ExtentType(), _end_xid, schema->row_size());
            auto extent = cache->_data_cache->get_empty(_file, header);
            _extents.push_back({ extent->cache_id(), true });

            // insert the tuple into the extent
            auto row = extent->append();
            MutableTuple(schema->get_mutable_fields(), row).assign(tuple);

            // release back to the cache
            cache->_data_cache->put(extent);
            return true;
        }

        // extract the key to find the insert position
        auto key = schema->tuple_subset(tuple, schema->get_sort_keys());

        // find the extent to modify via lower_bound
        auto extent_i = std::lower_bound(_extents.begin(), _extents.end(), *key,
                                         [this, &schema](const ExtentRef &ref, const Tuple &key) {
                                             SafeExtent extent(_file, ref);
                                             return FieldTuple(schema->get_sort_fields(), (*extent)->back()).less_than(key);
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
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), row);
                                              });

        // see if the row's key matches the tuple's key
        bool did_insert = false;
        if (row_i != (*extent)->end() && FieldTuple(schema->get_sort_fields(), *row_i).equal(*key)) {
            // update the existing row
            MutableTuple(schema->get_mutable_fields(), *row_i).assign(tuple);
            did_insert = true;
        } else {
            // insert the tuple into the extent
            auto row = (*extent)->insert(row_i);
            MutableTuple(schema->get_mutable_fields(), row).assign(tuple);
        }

        // check for split
        _check_split(extent_i, *extent, schema);

        // indicate if an insert occurred or not
        return did_insert;
    }

    void
    StorageCache::Page::update(TuplePtr tuple,
                               ExtentSchemaPtr schema)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = true;

        // extract the key to find the insert position
        auto key = schema->tuple_subset(tuple, schema->get_sort_keys());

        // find the extent to modify via lower_bound
        auto extent_i = std::lower_bound(_extents.begin(), _extents.end(), *key,
                                         [this, &schema](const ExtentRef &ref, const Tuple &key) {
                                             SafeExtent extent(_file, ref);
                                             return FieldTuple(schema->get_sort_fields(), (*extent)->back()).less_than(key);
                                         });
        // note: key should exist
        assert(extent_i != _extents.end());

        // make sure that we've got a mutable version of the extent
        SafeExtent extent(_file, *extent_i, true);

        // find the update position in the extent
        auto row_i = std::ranges::lower_bound(**extent, *key,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), row);
                                              });

        // note: row's key should match the tuple's key
        assert(FieldTuple(schema->get_sort_fields(), *row_i).equal(*key));

        // update the existing row
        MutableTuple(schema->get_mutable_fields(), *row_i).assign(tuple);

        // check for split
        _check_split(extent_i, *extent, schema);
    }

    void
    StorageCache::Page::remove(TuplePtr key,
                               ExtentSchemaPtr schema)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = true;

        // find the extent to modify via lower_bound
        auto extent_i = std::lower_bound(_extents.begin(), _extents.end(), *key,
                                         [this, &schema](const ExtentRef &ref, const Tuple &key) {
                                             SafeExtent extent(_file, ref);
                                             return FieldTuple(schema->get_sort_fields(), (*extent)->back()).less_than(key);
                                         });
        // note: key should exist
        assert(extent_i != _extents.end());

        // make sure that we've got a mutable version of the extent
        SafeExtent extent(_file, *extent_i, true);

        // find the insert position in the extent
        auto row_i = std::ranges::lower_bound(**extent, *key,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), row);
                                              });

        // note: row's key should match the tuple's key
        assert(FieldTuple(schema->get_sort_fields(), *row_i).equal(*key));

        // remove the row
        (*extent)->remove(row_i);

        // if the extent has become empty, remove it from the page
        if ((*extent)->empty()) {
            StorageCache::get_instance()->_data_cache->drop_dirty(*extent);
            _extents.erase(extent_i);
        }
    }

    void
    StorageCache::Page::remove(const Iterator &pos)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = true;

        // make sure that we've got a mutable version of the extent
        SafeExtent extent(_file, *pos._extent_i, true);

        // remove the row
        (*extent)->remove(pos._row);

        // if the extent has become empty, remove it from the page
        if ((*extent)->empty()) {
            StorageCache::get_instance()->_data_cache->drop_dirty(*extent);
            _extents.erase(pos._extent_i);
        }
    }

    void
    StorageCache::Page::convert(VirtualSchemaPtr schema,
                                ExtentSchemaPtr target_schema)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = true;

        auto cache = StorageCache::get_instance();

        auto target_fields = target_schema->get_mutable_fields();
        auto source_fields = schema->get_fields();

        // go through each extent within the page and create a copy of it based on the new schema
        std::vector<ExtentRef> new_extents;
        for (auto &ref : _extents) {
            // get a new extent
            auto new_extent = cache->_data_cache->get_empty(_file, header());

            // get the old extent
            SafeExtent old_extent(_file, ref);

            // copy the data
            for (auto &row : **old_extent) {
                FieldTuple source_tuple(source_fields, row);

                auto new_row = new_extent->append();
                MutableTuple(target_fields, new_row).assign(source_tuple);
            }

            new_extents.push_back({ new_extent->cache_id(), true });

            // release the new extent back to the cache
            cache->_data_cache->put(new_extent);
        }

        // replace the old extent with the new one in the page
        _extents = std::move(new_extents);
    }

    void
    StorageCache::Page::_check_split(std::vector<ExtentRef>::iterator pos,
                                     CacheExtentPtr extent,
                                     ExtentSchemaPtr schema)
    {
        // if the size of the extent is below the threshold, or it can't be split because
        // it's a single row, then return immediately
        if (extent->byte_count() < constant::MAX_EXTENT_SIZE || extent->row_count() == 1) {
            return;
        }

        // split the extent
        auto &&pair = StorageCache::get_instance()->_data_cache->split(extent, schema);

        // remove the old extent reference
        pos = _extents.erase(pos);

        // insert the two new extents; insert() occurs before the provided iterator, so inserted in reverse order
        pos = _extents.insert(pos, pair.second);
        _extents.insert(pos, pair.first);
    }

    std::vector<uint64_t>
    StorageCache::Page::_flush(const ExtentHeader &header)
    {
        auto cache = StorageCache::get_instance();

        std::vector<uint64_t> offsets;
        for (auto &ref : _extents) {
            // check if the reference is a cache ID
            if (ref.second) {
                // retrieve the extent; should always have a cache ID given the if-condition
                SafeExtent e(_file, ref);

                // update the extent header
                (*e)->header() = header;

                // append the extent to the file
                // XXX should do these asynchronously to get better parallelism when there are multiple extents
                cache->_data_cache->flush(*e);

                // return the clean extent back to the read cache
                cache->_data_cache->reinsert(*e);

                // save the extent ID of the now-unmodified extent
                ref.first = (*e)->key().second;
                ref.second = false;

                SPDLOG_INFO("Flushing extent {} -- new extent {}", _extent_id, ref.first);
            }

            offsets.push_back(ref.first);
        }

        return offsets;
    }

    // DATA CACHE

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::get(const std::filesystem::path &file,
                                 uint64_t extent_id)
    {
        // note: must get_empty() UNKNOWN extents
        assert(extent_id != constant::UNKNOWN_EXTENT);

        CacheKey key(file, extent_id);
        boost::unique_lock lock(_mutex);

        // call the internal get() helper
        return _get_clean(key);
    }

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::get(uint64_t cache_id,
                                 bool mark_dirty)
    {
        boost::unique_lock lock(_mutex);

        CacheExtentPtr extent = nullptr;

        // find the extent using the unique cache_id
        auto dirty_i = _dirty_cache.find(cache_id);
        if (dirty_i == _dirty_cache.end()) {
            // not in memory, so need to retrieve from disk
            auto key_i = _cache_id_map.find(cache_id);
            assert(key_i != _cache_id_map.end());

            extent = _read_extent(key_i->second, [this, cache_id](CacheExtentPtr extent) {
                // mark it as mutable
                extent->_state = CacheExtent::State::MUTABLE;

                // add it to the dirty cache
                _dirty_cache[cache_id] = extent;
            });
        } else {
            extent = dirty_i->second;

            // if the extent is being flushed, must block until complete
            if (extent->_state == CacheExtent::State::FLUSHING) {
                // mark ourselves as a user of the extent to prevent eviction post-flush()
                ++(extent->_use_count);

                // wait for the flush to complete and then return the extent
                auto cv = extent->_flush_cv;
                cv->wait(lock);

                return extent;
            }
        }

        // remove from the dirty_lru, if on it
        if (extent->_use_count == 0) {
            // extent must be MUTABLE or DIRTY if being retrieved by cache ID
            assert(extent->_state ==  CacheExtent::State::DIRTY ||
                   extent->_state ==  CacheExtent::State::MUTABLE);

            if (extent->_state == CacheExtent::State::MUTABLE) {
                _clean_lru.erase(extent->_pos);
            } else {
                _dirty_lru.erase(extent->_pos);
            }
        }

        // increase the use-count
        ++(extent->_use_count);

        // optionally mark the extent as DIRTY
        if (mark_dirty) {
            extent->_state = CacheExtent::State::DIRTY;
        }

        return extent;
    }

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::get_empty(const std::filesystem::path &file,
                                       const ExtentHeader &header)
    {
        boost::unique_lock lock(_mutex);

        // make space for the new extent
        _make_extent_space();

        // create an empty extent
        auto extent = std::make_shared<CacheExtent>(header, file);

        // assign the extent a unique cache ID and add it to the dirty cache
        _gen_cache_id(extent);
        _dirty_cache.insert({ extent->_cache_id, extent });

        return extent;
    }

    void
    StorageCache::DataCache::put(CacheExtentPtr extent)
    {
        boost::unique_lock lock(_mutex);

        // release the extent
        _release(extent);
    }

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::extract(const std::filesystem::path &file,
                                     uint64_t extent_id)
    {
        CacheKey key(file, extent_id);
        boost::unique_lock lock(_mutex);

        // get the clean extent from the cache
        auto extent = _get_clean(key);

        // check if the caller is the only user
        if (extent->_use_count == 1) {
            // this is the only user, so we can simply convert the extent to DIRTY
            // note: no need to adjust the LRU queue since the extent cannot be on it
            _clean_cache.erase(extent->key());

            // mark the extent as DIRTY
            extent->_state = CacheExtent::State::DIRTY;

            // assign the extent a unique cache ID and add it to the dirty cache
            _gen_cache_id(extent);
            _dirty_cache.insert({ extent->_cache_id, extent });

            return extent;
        }

        // make space in the cache for a new dirty extent owned by a page
        _make_extent_space();

        // there are other users, so we need to return a copy of this extent
        auto new_extent = std::make_shared<CacheExtent>(*extent);

        // release the original extent since we were holding it for use when calling
        _release(extent);

        // assign the new extent a unique cache ID
        _gen_cache_id(new_extent);

        // place the new extent into the dirty cache
        _dirty_cache.insert({ new_extent->_cache_id, new_extent });

        // return the extent to the page
        return new_extent;
    }

    void
    StorageCache::DataCache::reinsert(CacheExtentPtr extent)
    {
        boost::unique_lock lock(_mutex);

        // note: the extent must be MUTABLE (not DIRTY) and not in-use by others when reinsert()'d
        assert(extent->_state == CacheExtent::State::MUTABLE);
        assert(extent->_use_count == 1);

        // find the cache extent
        auto dirty_i = _dirty_cache.find(extent->_cache_id);
        assert(dirty_i != _dirty_cache.end());

        // mark the extent CLEAN
        extent->_state = CacheExtent::State::CLEAN;
        extent->_cache_id = 0;

        // move from the dirty cache to the clean cache
        _dirty_cache.erase(dirty_i);
        _clean_cache.insert({ extent->key(), extent });

        // clear the cache ID
        _cache_id_map.erase(extent->_cache_id);
    }

    void
    StorageCache::DataCache::flush(CacheExtentPtr extent)
    {
        boost::unique_lock lock(_mutex);

        // call the internal flush() helper
        _flush(extent);
    }

    void
    StorageCache::DataCache::drop_dirty(CacheExtentPtr extent)
    {
        boost::unique_lock lock(_mutex);

        // note: extent must be dirty for this to be a valid operation
        assert(extent->_state == CacheExtent::State::DIRTY);

        // evict the extent from the cache
        _dirty_cache.erase(extent->_cache_id);
        _cache_id_map.erase(extent->_cache_id);

        // mark the extent as no longer valid to ensure it doesn't get released back into the cache
        // by a concurrent user
        extent->_state = CacheExtent::State::INVALID;
    }

    std::pair<StorageCache::ExtentRef, StorageCache::ExtentRef>
    StorageCache::DataCache::split(CacheExtentPtr extent, ExtentSchemaPtr schema)
    {
        boost::unique_lock lock(_mutex);

        // note: extent must be DIRTY with a mutation that caused the split
        assert(extent->_state == CacheExtent::State::DIRTY);

        // make space for two new extents
        _make_extent_space();
        _make_extent_space();

        // XXX this is extremely inefficient right now... results in two data copies
        // split the provided extent in two
        auto &&pair = extent->split(schema);

        // create CacheExtent objects from the two halves
        auto first = std::make_shared<CacheExtent>(std::move(*(pair.first)), *extent);
        auto second = std::make_shared<CacheExtent>(std::move(*(pair.second)), *extent);

        // remove the old extent from the cache
        _dirty_cache.erase(extent->_cache_id);
        _cache_id_map.erase(extent->_cache_id);
        extent->_state = CacheExtent::State::INVALID;

        // place the new extents into the cache
        _gen_cache_id(first);
        _dirty_cache.insert({ first->_cache_id, first });
        _release(first);

        _gen_cache_id(second);
        _dirty_cache.insert({ second->_cache_id, second });
        _release(second);

        return std::pair<ExtentRef, ExtentRef>({ first->_cache_id, true },
                                               { second->_cache_id, true });
    }

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::_get_clean(const CacheKey &key)
    {
        CacheExtentPtr extent = nullptr;
        while (extent == nullptr) {
            // search for the requested extent
            auto cache_i = _clean_cache.find(key);
            if (cache_i != _clean_cache.end()) {
                extent = cache_i->second;

                // remove the entry from the LRU list
                if (extent->_use_count == 0) {
                    _clean_lru.erase(extent->_pos);
                    extent->_pos = _clean_lru.end();
                }

                // update the use count
                ++extent->_use_count;

                // exit the loop
                continue;
            }

            // extent not cached, so read from disk
            // note: may return nullptr, indicating someone else just read the extent from disk and
            //       that we should check the cache again
            extent = _read_extent(key, [this, key](CacheExtentPtr extent) {
                // insert the extent into the cache
                // note: we don't place into the LRU list since the extent will be in-use
                _clean_cache.insert({ key, extent });
            });
        }

        return extent;
    }

    void
    StorageCache::DataCache::_flush(CacheExtentPtr extent)
    {
        // if already flushing, wait for completion
        if (extent->_state == CacheExtent::State::FLUSHING) {
            boost::unique_lock lock(_mutex, boost::adopt_lock);

            auto cv = extent->_flush_cv;
            cv->wait(lock);

            lock.release();
            return;
        }

        // if the extent isn't DIRTY, don't need to flush
        if (extent->_state != CacheExtent::State::DIRTY) {
            return;
        }

        // mark the extent as FLUSHING so that other callers will block until flush complete
        extent->_state = CacheExtent::State::FLUSHING;
        extent->_flush_cv = std::make_shared<boost::condition_variable>();

        // perform the flush
        {
            boost::unique_lock lock(_mutex, boost::adopt_lock);
            lock.unlock();

            auto handle = IOMgr::get_instance()->open(extent->_file, IOMgr::IO_MODE::APPEND, true);
            auto response = extent->async_flush(handle);

            // XXX we could do this asynchronously and return a future that completes when the extent ID
            //     becomes available... should be safe to do so since we are already putting the extent
            //     into an exclusive FLUSHING state
            extent->_extent_id = response.get()->offset;

            lock.lock();
            lock.release();
        }

        // update the cache ID as pointing to the new extent ID
        _cache_id_map[extent->_cache_id] = extent->key();

        // mark as MUTABLE, place on the clean LRU
        extent->_state = CacheExtent::State::MUTABLE;

        // notify anyone waiting
        extent->_flush_cv->notify_all();
        extent->_flush_cv = nullptr;
    }

    void
    StorageCache::DataCache::_make_extent_space()
    {
        // check if there is space in the cache
        if (_size < _max_size) {
            ++_size; // take up a slot and return
            return;
        }

        // if the clean LRU list is empty, need to clean a page
        while (_clean_lru.empty()) {
            // choose an extent we can clean
            auto dirty = _dirty_lru.front();
            _dirty_lru.pop_front();

            // become a user of the extent
            ++(dirty->_use_count);

            // flush the extent
            _flush(dirty);

            // release the extent back to the cache
            _release(dirty);
        }

        // evict an extent to make space
        auto extent = _clean_lru.front();
        _clean_lru.pop_front();
        _clean_cache.erase(extent->key());
    }

    void
    StorageCache::DataCache::_release(CacheExtentPtr extent)
    {
        // if the extent is invalid, do not place back into the cache
        if (extent->_state == CacheExtent::State::INVALID) {
            --_size;
            return;
        }

        // reduce the use count
        --(extent->_use_count);

        // if still in use, return
        if (extent->_use_count > 0) {
            return;
        }

        // note: shouldn't be possible to call _release() when FLUSHING
        assert(extent->_state != CacheExtent::State::FLUSHING);

        // if the use count is zero, place into the appropriate LRU list
        if (extent->_state == CacheExtent::State::DIRTY) {
            extent->_pos = _dirty_lru.insert(_dirty_lru.end(), extent);
        } else {
            extent->_pos = _clean_lru.insert(_clean_lru.end(), extent);
        }
    }

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::_read_extent(const CacheKey &key,
                                          std::function<void(CacheExtentPtr)> callback)
    {
        // extent not cached, check if someone is reading it from disk
        auto io_i = _io_map.find(key);
        if (io_i != _io_map.end()) {
            boost::unique_lock lock(_mutex, boost::adopt_lock);

            // wait for the read to complete
            auto cv = io_i->second;
            cv->wait(lock);

            // note: try to retrieve from the cache again
            lock.release();
            return nullptr;
        }

        // add the condition variable to the IO map
        auto cv = std::make_shared<boost::condition_variable>();
        _io_map[key] = cv;

        // make space for a new extent in the cache
        _make_extent_space();

        // unlock before IO
        boost::unique_lock lock(_mutex, boost::adopt_lock);
        lock.unlock();

        // read the extent
        auto handle = IOMgr::get_instance()->open(key.first, IOMgr::READ, true);
        auto response = handle->read(key.second);
        auto extent = std::make_shared<CacheExtent>(response->data, key.first, key.second);

        // reacquire the lock once IO complete
        lock.lock();
        lock.release();

        // callback to initialize state before returning the extent to the callers
        callback(extent);

        // notify the other callers waiting for this extent
        cv->notify_all();

        // remove the condition variable from the map
        _io_map.erase(key);

        return extent;
    }

    void
    StorageCache::DataCache::_gen_cache_id(CacheExtentPtr extent)
    {
        // note: start with 1 as the initial cache ID
        extent->_cache_id = ++_next_cache_id;

        if (extent->_extent_id != constant::UNKNOWN_EXTENT) {
            _cache_id_map[extent->_cache_id] = extent->key();
        }
    }
}
