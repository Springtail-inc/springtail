#include <storage/cache.hh>

#include <absl/log/log.h>
#include <absl/log/check.h>
#include <functional>
#include <memory>
#include <unordered_map>

#include <common/json.hh>
#include <common/open_telemetry.hh>
#include <common/properties.hh>

#include <sys_tbl_mgr/system_tables.hh>

//#define SPRINGTAIL_INCLUDE_TIME_TRACES 1
#include <common/time_trace.hh>

namespace springtail {

/* Thread-local variable to identify the background flushing thread. */
thread_local bool StorageCache::PageCache::_is_cleaner_thread = false;

    StorageCache::~StorageCache()
    {
        LOG_INFO("StorageCache delete");
    }

    StorageCache::StorageCache() : Singleton<StorageCache>(ServiceId::StorageCacheId)
    {
        LOG_INFO("StorageCache create");

        // get the cache size
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        uint64_t data_size = Json::get_or<uint64_t>(json, "data_cache_size", 16384);
        uint64_t page_size = Json::get_or<uint64_t>(json, "page_cache_size", 16384);

        _data_cache = std::make_shared<DataCache>(data_size);
        _page_cache = std::make_shared<PageCache>(page_size);

        int metrics_update_freq = Json::get_or<int>(json, "metrics_update_freq_sec", 10);
        _metric_counters = std::make_unique<MetricCounters>(
                std::unordered_map<std::string, std::string>{}, metrics_update_freq);
    }

    StorageCache::SafePagePtr
    StorageCache::get(uint64_t database_id,
                      const std::filesystem::path &file,
                      uint64_t extent_id,
                      uint64_t access_xid,
                      uint64_t target_xid,
                      uint64_t max_extent_size,
                      bool do_rollforward,
                      SafePagePtr::FlushCb flush_cb )
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "GET file {} eid {} xid {} txid {}",
                            file, extent_id, access_xid, target_xid);

        // note: target_xid must be at or beyond the access_xid
        DCHECK_GE(target_xid, access_xid);
        if (target_xid == constant::LATEST_XID) {
            target_xid = access_xid;
        }

        _metric_counters->increment<metrics::StorageCache::GetCalls>();

        // if the extent ID is UNKNOWN, then we will get an empty page for the file
        if (extent_id == constant::UNKNOWN_EXTENT) {
            return {_page_cache.get(), _page_cache->get_empty(database_id, file, target_xid, max_extent_size), std::move(flush_cb)};
        }

        // if target is the same as access, get the page and return it
        if (target_xid == access_xid) {
            return {_page_cache.get(), _page_cache->get(database_id, file, extent_id, access_xid, target_xid, max_extent_size), std::move(flush_cb)};
        }

        // if the target is ahead of the access, but there is roll-forward request, then it means
        // the caller is going to perform the roll-forward mutations (for non-data extents)
        if (!do_rollforward) {
            // note: we know that the provided extent_id is valid at the access_xid, so we get the
            //       page at the target_xid using that original extent_id so that the caller can
            //       modify it from that point forward
            return {_page_cache.get(), _page_cache->get(database_id, file, extent_id, access_xid, target_xid, max_extent_size), std::move(flush_cb)};
        }

        // note: from here forward, we know we are dealing with a roll-forward table data page
        LOG(FATAL) << "Roll-forward table data page";

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

    uint64_t
    StorageCache::flush(const std::filesystem::path &file)
    {
        auto end_offset = _page_cache->flush_file(file);
        _metric_counters->increment<metrics::StorageCache::FlushCalls>();
        return end_offset;
    }

    void
    StorageCache::drop_for_truncate(const std::filesystem::path &file)
    {
        _page_cache->drop_file(file);
        _metric_counters->increment<metrics::StorageCache::DropCalls>();
    }

    void
    StorageCache::evict_for_database(uint64_t database_id)
    {
        _page_cache->evict_for_database(database_id);
    }


    StorageCache::PagePtr
    StorageCache::PageCache::get(uint64_t database_id,
                                 const std::filesystem::path &file,
                                 uint64_t extent_id,
                                 uint64_t access_xid,
                                 uint64_t target_xid,
                                 uint64_t max_extent_size)
    {
        DCHECK(extent_id != constant::UNKNOWN_EXTENT);

        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "{}, {}, {}, {}", file, extent_id, access_xid, target_xid);

        boost::unique_lock lock(_mutex);

        // check if the page already exists in the cache for the given target XID
        PagePtr page = _try_get(file, extent_id, target_xid);
        if (page != nullptr) {
            StorageCache::get_instance()->_metric_counters->increment<metrics::StorageCache::GetCalls>();
            LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "Found in cache");
            return page;
        }

        StorageCache::get_instance()->_metric_counters->increment<metrics::StorageCache::CacheMisses>();

        // XXX eventually use the access_xid and extent_id to get the proper set of extents to start
        //     from; for now we assume that the single extent_id *is* the full list of extents for
        //     the access XID and that the query nodes won't perform any roll-forward on their own.

        // note: not in the cache, need to create a new Page
        return _create(database_id, file, extent_id, target_xid, { extent_id }, max_extent_size);
    }

    StorageCache::PagePtr
    StorageCache::PageCache::get_empty(uint64_t database_id,
                                       const std::filesystem::path &file,
                                       uint64_t xid, uint64_t max_extent_size)
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "{}, {}", file, xid);
        boost::unique_lock lock(_mutex);

        _make_page_space();
        return std::make_shared<Page>(database_id, file, xid, max_extent_size);
    }

    void
    StorageCache::PageCache::put(PagePtr page,
                                 std::function<bool(std::shared_ptr<Page>)> flush_callback)
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "PUT file {} eid {} s_xid {} e_xid {}",
                            page->_file, page->_extent_id, page->_start_xid, page->_end_xid);

        StorageCache::get_instance()->_metric_counters->increment<metrics::StorageCache::PutCalls>();
        boost::unique_lock lock(_mutex);

        // set the flush callback for the page if it doesn't have one yet
        if (flush_callback && !page->_flush_callback) {
            page->_register_flush(flush_callback);

            auto &list = _flush_list[page->_file];
            page->_flush_pos = list.insert(list.end(), page);
        }

        if (page->_extent_id == constant::UNKNOWN_EXTENT) {
            // note: we cannot release a dirty page with no original extent ID back to the cache unless it has a flush callback
            DCHECK(!page->_is_dirty || page->_flush_callback);

            // release the space back to the cache
            --_size;
            return;
        }

        // release the page back to the cache
       _put(page);
    }

    void
    StorageCache::PageCache::evict(PagePtr page)
    {
        boost::unique_lock lock(_mutex);
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "EVICT file {} eid {} s_xid {} e_xid {}",
                            page->_file, page->_extent_id, page->_start_xid, page->_end_xid);

        // page must be an unwritten dirty page
        CHECK_EQ(page->_extent_id, constant::UNKNOWN_EXTENT);
        CHECK(page->_is_dirty);

        // release the space back to the cache
        --_size;
    }

    uint64_t
    StorageCache::PageCache::flush_file(const std::filesystem::path &file)
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::flush_file file={}", file);
        boost::unique_lock lock(_mutex);

        StorageCache::get_instance()->_metric_counters->increment<metrics::StorageCache::FlushCalls>();

        const auto start_time = std::chrono::system_clock::now();

        //Get the end offset of data file for the table
        //to be returned if nothing to flush
        uint64_t end_offset = 0;
        if (std::filesystem::exists(file)) {
            end_offset = std::filesystem::file_size(file);
        }

        // go through the dirty page list for the file
        auto file_i = _flush_list.find(file);
        if (file_i == _flush_list.end()) {
            // no dirty pages
            return end_offset;
        }
        auto &flush_pages = file_i->second;

        bool done = false;
        auto page_i = flush_pages.begin();

        // if the list is empty, remove it from the map and return
        if (page_i == flush_pages.end()) {
            _flush_list.erase(file_i);
            return end_offset;
        }

        while (!done) {
            auto page = *page_i;

            // make sure that this page won't be selected for eviction while performing this flush
            if (page->_use_count == 0) {
                if (page->_is_dirty) {
                    _dirty_lru.erase(page->_lru_pos);
                } else {
                    _clean_lru.erase(page->_lru_pos);
                }
            }
            ++(page->_use_count);

            // check if the page is currently flushing
            if (page->_is_flushing) {
                // wait for the flushing to complete
                page->_flush_cond.wait(lock, [&page](){ return !page->_is_flushing; });
            } else {
                // mark the page as flushing
                page->_is_flushing = true;
                auto callback = page->_flush_callback;

                lock.unlock();

                // note: we currently assume that the page has a flush callback
                CHECK(callback);

                // issue the flush callback
                bool success = callback(page);

                // note: we currently assume that the flush callback must succeed here
                CHECK(success);

                lock.lock();

                // remove the page from the flush list
                flush_pages.erase(page->_flush_pos);
                page->_register_flush(nullptr);

                // signal that the flushing is complete
                page->_is_flushing = false;
                page->_flush_cond.notify_all();
            }

            // release the flushed page back to the cache?
            _put(page);

            // get the next dirty page
            page_i = flush_pages.begin();
            if (page_i == flush_pages.end()) {
                _flush_list.erase(file);
                done = true; // no more dirty pages, exit the loop
            }
        }

        auto duration = std::chrono::system_clock::now() - start_time;
        open_telemetry::OpenTelemetry::get_instance()->record_histogram(STORAGE_CACHE_FLUSH_LATENCIES,
            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());

        // flush list for the file must be empty, so remove it
        _flush_list.erase(file);

        //Get the end offset of data file for the table
        return std::filesystem::file_size(file);
    }

    void
    StorageCache::PageCache::drop_file(const std::filesystem::path &file)
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::drop_file file={}", file);
        boost::unique_lock lock(_mutex);

        StorageCache::get_instance()->_metric_counters->increment<metrics::StorageCache::DropCalls>();

        const auto start_time = std::chrono::system_clock::now();

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
                auto extent = ref.lock_cached();
                CHECK(extent); //must be in the dirty cache
                CHECK_EQ(extent->state(), CacheExtent::State::DIRTY);
                data_cache->drop_dirty(extent);
                data_cache->put(extent);
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

        const auto duration = std::chrono::system_clock::now() - start_time;
        open_telemetry::OpenTelemetry::get_instance()->record_histogram(STORAGE_CACHE_DROP_LATENCIES,
            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());

        // flush list for the file must be empty, so remove it
        _flush_list.erase(file);
    }

void
StorageCache::PageCache::background_cleaner()
{
    // set the thread name
    pthread_setname_np(pthread_self(), "cache-cleaner");

    // mark is as the flushing thread
    _is_cleaner_thread = true;

    auto timeout = boost::chrono::seconds(constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
    while (!_shutdown_cleaner) {
        // hold the cache lock
        boost::unique_lock lock(_mutex);

        // wake up when the dirty cache gets too full
        _cleaner_cond.wait_for(lock, timeout, [this]() {
            return _shutdown_cleaner || _dirty_lru.size() > _low_dirty_pages;
        });

        while (_dirty_lru.size() > _low_dirty_pages) {
            // flush dirty pages
            auto page = _dirty_lru.front();
            _dirty_lru.pop_front();

            // take ownership of this page for the eviction
            DCHECK_EQ(page->_use_count, 0);
            ++(page->_use_count);

            // try to evict the page
            _try_evict_dirty(page);

            // once we cross a threshold, wake up waiting threads
            if (_waiting_for_cleaner && _dirty_lru.size() < _high_dirty_pages) {
                // wake up anyone waiting
                _waiting_for_cleaner = false;
                _cleaner_block.notify_all();
            }
        }
    }
}

    void
    StorageCache::PageCache::_put(PagePtr page)
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::_put file={} eid={} s_xid={} use_count={}",
                  page->_file, page->_extent_id, page->_start_xid, page->_use_count.load());
        // decrement it's use count
        --(page->_use_count);

        // if the page has no users, place it onto the back of the LRU list
        if (page->_use_count == 0) {
            if (page->_is_dirty) {
                page->_lru_pos = _dirty_lru.insert(_dirty_lru.end(), page);
            } else {
                page->_lru_pos = _clean_lru.insert(_clean_lru.end(), page);
            }
        }
    }

    StorageCache::PagePtr
    StorageCache::PageCache::_create(uint64_t database_id,
                                     const std::filesystem::path &file,
                                     uint64_t extent_id,
                                     uint64_t xid,
                                     const std::vector<uint64_t> &offsets,
                                     uint64_t max_extent_size)
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "{}, {}, {}, {}", file, extent_id, xid, offsets.size());

        // create the page object with the given <file, extent_id> valid at the requested XID
        auto page = std::make_shared<Page>(database_id, file, extent_id, xid, xid, offsets, max_extent_size);

        // add it to the cache; note: use count starts at 1
        _cache[page->key()][xid] = page;

        // register this file with the database
        _database_files[database_id].insert(file);

        // make space for the page; evict if we need to make space
        // note: we do this after creating the Page to avoid a race where two people might create
        //       the same page since they both don't find it in the cache
        _make_page_space();

        // return it
        return page;
    }

    void
    StorageCache::PageCache::_try_evict_dirty(PagePtr page)
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::_try_evict_dirty file={} eid={} s_xid={} use_count={} is_dirty={}",
                  page->_file, page->_extent_id, page->_start_xid, page->_use_count.load(), page->_is_dirty);
        // issue the associated callback for the page's eviction
        bool success = true;
        if (page->_flush_callback && page->_is_dirty) {
            boost::unique_lock lock(_mutex, boost::adopt_lock);

            // check if the page is currently flushing
            if (page->_is_flushing) {
                // wait for the flushing to complete
                page->_flush_cond.wait(lock, [&page](){ return !page->_is_flushing; });
            } else {
                // mark the page as flushing
                page->_is_flushing = true;
                auto callback = page->_flush_callback;

                // mark the page as clean pre-emptively in case someone else gets the page after
                // flush and re-dirties it
                page->_is_dirty = false;
                lock.unlock();

                success = callback(page);

                lock.lock();
                lock.release();

                // clear the page from the flush list
                _flush_list[page->_file].erase(page->_flush_pos);
                page->_register_flush(nullptr);

                // signal that the flushing is complete
                page->_is_flushing = false;
                page->_flush_cond.notify_all();
            }
        } else {
            page->_is_dirty = false; // mark the page as clean so that we can evict it
        }

        if (!success || page->_use_count > 1 || page->_is_dirty) {
            // if page can't be evicted then release the page back to the cache
            _put(page);
            return;
        }

        // evict the now-clean page
        _evict_clean(page);
    }

    void
    StorageCache::PageCache::_evict_clean(PagePtr page)
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::_evict_clean file={} eid={} s_xid={} use_count={}",
                  page->_file, page->_extent_id, page->_start_xid, page->_use_count.load());
        DCHECK(page->_use_count == 1);
        DCHECK(!page->_is_dirty);

        // remove the page from the cache
        auto cache_i = _cache.find(page->key());
        cache_i->second.erase(page->xid());
        if (cache_i->second.empty()) {
            _cache.erase(cache_i);
        }

        // update the size
        --_size;
    }

    void
    StorageCache::PageCache::_make_page_space()
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::_make_page_space size={} max_size={} dirty_lru_size={} clean_lru_size={}",
                  _size, _max_size, _dirty_lru.size(), _clean_lru.size());
        // check if there's vacant space in the cache
        if (_size < _max_size) {
            ++_size;
            return;
        }

        if (!_is_cleaner_thread) {
            // if we have too many dirty pages, we must block until the cleaner catches up
            if (_dirty_lru.size() > _low_dirty_pages) {
                // wake up the background cleaner
                _cleaner_cond.notify_one();

                // if we have too many dirty pages, we must block
                if (_dirty_lru.size() > _max_dirty_pages) {
                    _waiting_for_cleaner = true;
                }

                // if we have to wait, block until the cleaner wakes us
                if (_waiting_for_cleaner) {
                    boost::unique_lock lock(_mutex, boost::adopt_lock);
                    _cleaner_block.wait(lock, [this]() { return !_waiting_for_cleaner; });
                    lock.release(); // release the lock so it doesn't get unlocked
                }
            }
        }
        DCHECK(!_clean_lru.empty());

        // evict a page from the LRU
        auto page = _clean_lru.front();
        _clean_lru.pop_front();

        // take ownership of this page for the eviction
        DCHECK_EQ(page->_use_count, 0);
        ++(page->_use_count);

        // evict the page
        _evict_clean(page);

        // re-aquire the space
        ++_size;
    }

    StorageCache::PagePtr
    StorageCache::PageCache::_try_get(const std::filesystem::path &file,
                                      uint64_t extent_id,
                                      uint64_t xid)
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::_try_get file={} eid={} xid={}", file, extent_id, xid);
        TIME_TRACE_SCOPED(time_trace::traces, cache__try_get_total);

        // check for the key in the hash map
        std::pair<uint64_t, const std::string&> key{extent_id, file.native()};
        auto write_i = _cache.find(key);
        if (write_i == _cache.end()) {
            LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::_try_get file={} eid={} xid={} - NOT FOUND in cache",
                      file, extent_id, xid);
            return nullptr;
        }

        // check for the xid in the XID map
        auto page_i = write_i->second.lower_bound(xid);
        if (page_i == write_i->second.end()) {
            LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::_try_get file={} eid={} xid={} - XID map end()",
                      file, extent_id, xid);
            return nullptr;
        }

        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::_try_get file={} eid={} xid={} - found page_xid={} page_ptr={} page_valid={}",
                  file, extent_id, xid, page_i->first,
                  static_cast<void*>(page_i->second.get()),
                  page_i->second != nullptr);

        // check if the page is valid through the requested xid
        if (!page_i->second->check_xid_valid(xid)) {
            LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG2, "PageCache::_try_get file={} eid={} xid={} - check_xid_valid FAILED",
                      file, extent_id, xid);
            return nullptr;
        }

        // if the page is on the LRU list, remove it
        if (page_i->second->_use_count == 0) {
            TIME_TRACE_SCOPED(time_trace::traces, cache__try_get__lru_erase);
            if (page_i->second->_is_dirty) {
                _dirty_lru.erase(page_i->second->_lru_pos);
            } else {
                _clean_lru.erase(page_i->second->_lru_pos);
            }
        }

        // increment it's use count
        ++(page_i->second->_use_count);

        return page_i->second;
    }

    StorageCache::Page::Page(uint64_t database_id,
                             const std::filesystem::path &file,
                             uint64_t extent_id,
                             uint64_t start_xid,
                             uint64_t end_xid,
                             const std::vector<uint64_t> &offsets,
                             uint64_t max_extent_size)
        : _use_count(1),
          _is_dirty(false),
          _database_id(database_id),
          _file(file),
          _extent_id(extent_id),
          _start_xid(start_xid),
          _end_xid(end_xid),
          _max_extent_size(max_extent_size),
          _is_flushing(false)
    {
        for (auto offset : offsets) {
            _extents.push_back({ offset, true });
        }
    }

    StorageCache::Page::Page(uint64_t database_id,
                             const std::filesystem::path &file,
                             uint64_t xid, uint64_t max_extent_size)
        : _use_count(1),
          _is_dirty(false),
          _database_id(database_id),
          _file(file),
          _extent_id(constant::UNKNOWN_EXTENT),
          _start_xid(xid),
          _end_xid(xid),
          _max_extent_size(max_extent_size),
          _is_flushing(false)
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
        CHECK(_extents.empty());

        // create an empty extent
        auto extent = cache->_data_cache->get_empty(_file, header);

        _extents.emplace_back(extent->cache_id(), false, extent);

        cache->_data_cache->put(extent);

        // now perform the usual flush()
        auto &&offsets = _flush(header);

        return offsets[0];
    }


    std::future<std::vector<uint64_t>>
    StorageCache::Page::async_flush(const ExtentHeader &header, std::function<void(std::vector<uint64_t>)> callback)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = false;

        // note: if the page is empty, we should be calling flush_empty()
        CHECK(!_extents.empty());

        return _async_flush(std::move(header), std::move(callback));
    }

    std::vector<uint64_t>
    StorageCache::Page::flush(const ExtentHeader &header)
    {
        auto flush_future = async_flush(std::move(header));
        return flush_future.get();
    }

    StorageCache::Page::Iterator
    StorageCache::Page::lower_bound(TuplePtr tuple, ExtentSchemaPtr schema)
    {
        // perform a lower-bound check to find the appropriate extent
        // note: we don't use std::ranges::lower_bound() here because the projection causes the
        //       SafeExtent to go out of scope before it is used in the comparison
        auto extent_i = std::lower_bound(_extents.begin(), _extents.end(), *tuple,
                                         [this, &schema](const ExtentRef &ref, const Tuple &key) {
                                             auto extent = ref.make_safe_extent(_file);
                                             auto &&row = (*extent)->back();
                                             return FieldTuple(schema->get_sort_fields(), &row).less_than(key);
                                         });
        if (extent_i == _extents.end()) {
            return end();
        }

        auto extent = extent_i->make_safe_extent(_file);

        // perform a lower-bound check to find the appropriate row within the extent
        auto row_i = std::ranges::lower_bound(**extent, *tuple,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), &row);
                                              });

        // note: shouldn't be possible to hit end() given the above lower_bound() check to find the extent
        CHECK(row_i != (*extent)->end());

        return Iterator(this, extent_i, std::move(extent), row_i);
    }

    StorageCache::Page::Iterator
    StorageCache::Page::upper_bound(TuplePtr tuple, ExtentSchemaPtr schema)
    {
        // perform a upper-bound check to find the appropriate extent
        // note: we don't use std::ranges::upper_bound() here because the projection causes the
        //       SafeExtent to go out of scope before it is used in the comparison
        auto extent_i = std::upper_bound(_extents.begin(), _extents.end(), *tuple,
                                 [this, &schema](const Tuple &key, const ExtentRef &ref) {
                                     auto extent = ref.make_safe_extent(_file);
                                     auto &&row = (*extent)->back();
                                     auto tuple = FieldTuple(schema->get_sort_fields(), &row);
                                     return key.less_than(tuple);
                                 });

        if (extent_i == _extents.end()) {
            return end();
        }

        auto extent = extent_i->make_safe_extent(_file);

        // perform a upper-bound check to find the appropriate row within the extent
        auto row_i = std::ranges::upper_bound(**extent, *tuple,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), &row);
                                              });

        // note: shouldn't be possible to hit end() given the above upper_bound() check to find the extent
        CHECK(row_i != (*extent)->end());

        return Iterator(this, extent_i, std::move(extent), row_i);
    }

    StorageCache::Page::Iterator
    StorageCache::Page::inverse_lower_bound(TuplePtr tuple, ExtentSchemaPtr schema)
    {
        boost::shared_lock lock(_mutex);

        // check if the page is empty
        if (_empty()) {
            return end();
        }

        // perform a lower-bound to find the row with a key <= the provided tuple
        auto i = lower_bound(tuple, schema);
        if (i == end()) {
            --i;
            return i;
        }

        // if the key is equal, return it
        auto key = FieldTuple(schema->get_sort_fields(), &*i);
        if (tuple->equal_strict(key)) {
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
            auto extent = extent_i->make_safe_extent(_file);

            uint32_t row_count = (*extent)->row_count();
            if (index < row_count) {
                // construct the iterator to the requested position and return it
                return Iterator(this, std::move(extent_i), std::move(extent), (*extent)->at(index));
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

        // if the page is empty, do an _append() which handles the empty extent case
        if (_empty()) {
            _append(tuple, schema);
            return;
        }

        // extract the key to find the insert position
        auto key = schema->tuple_subset(tuple, schema->get_sort_keys());

        // find the extent to modify via lower_bound
        auto extent_i = std::lower_bound(_extents.begin(), _extents.end(), *key,
                                         [this, &schema](const ExtentRef &ref, const Tuple &key) {
                                             auto extent = ref.make_safe_extent(_file);
                                             auto &&row = (*extent)->back();
                                             return FieldTuple(schema->get_sort_fields(), &row).less_than(key);
                                         });

        if (extent_i == _extents.end()) {
            extent_i = --_extents.end();
        }

        // make sure that we've got a mutable version of the extent
        auto extent = extent_i->make_dirty_safe_extent(_file);

        // find the insert position in the extent
        auto row_i = std::ranges::lower_bound(**extent, *key,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), &row);
                                              });

        // note: row's key should *not* match the tuple's key
        CHECK(row_i == (*extent)->end() ||
               !FieldTuple(schema->get_sort_fields(), &*row_i).equal_strict(*key));

        // insert the tuple into the extent
        auto row = (*extent)->insert(row_i);
        MutableTuple(schema->get_mutable_fields(), &row).assign(tuple);

        // check for split
        _check_split(extent_i, *extent, schema);
    }

    void
    StorageCache::Page::append(TuplePtr tuple,
                               ExtentSchemaPtr schema)
    {
        boost::unique_lock lock(_mutex);
        _is_dirty = true;

        // perform the internal append
        _append(tuple, schema);
    }

    void
    StorageCache::Page::_append(TuplePtr tuple,
                                ExtentSchemaPtr schema)
    {
        // if the page is empty, create an empty extent to back it
        if (_extents.empty()) {
            // create an empty extent
            ExtentHeader header(ExtentType(), _end_xid, schema->row_size(), schema->field_types());
            auto extent = SafeExtent(_file, std::move(header));
            _extents.emplace_back(extent.get_ref());

            // insert the tuple into the extent
            auto row = (*extent)->append();
            MutableTuple(schema->get_mutable_fields(), &row).assign(tuple);
            return;
        }

        // retrieve the last extent
        auto extent_i = --_extents.end();

        auto extent = extent_i->make_dirty_safe_extent(_file);

        // append a row
        auto row = (*extent)->append();

        // set the value
        MutableTuple(schema->get_mutable_fields(), &row).assign(tuple);

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
            ExtentHeader header(ExtentType(), _end_xid, schema->row_size(), schema->field_types());
            auto extent = SafeExtent(_file, std::move(header));
            _extents.emplace_back(extent.get_ref());

            // insert the tuple into the extent
            auto row = (*extent)->append();
            MutableTuple(schema->get_mutable_fields(), &row).assign(tuple);

            return true;
        }

        // extract the key to find the insert position
        auto key = schema->tuple_subset(tuple, schema->get_sort_keys());

        // find the extent to modify via lower_bound
        auto extent_i = std::lower_bound(_extents.begin(), _extents.end(), *key,
                                         [this, &schema](const ExtentRef &ref, const Tuple &key) {
                                             auto extent = ref.make_safe_extent(_file);
                                             auto &&row = (*extent)->back();
                                             return FieldTuple(schema->get_sort_fields(), &row).less_than(key);
                                         });
        if (extent_i == _extents.end()) {
            extent_i = --_extents.end();
        }

        // make sure that we've got a mutable version of the extent
        auto extent = extent_i->make_dirty_safe_extent(_file);

        // find the insert position in the extent
        auto row_i = std::ranges::lower_bound(**extent, *key,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), &row);
                                              });

        // see if the row's key matches the tuple's key
        bool did_insert = false;
        if (row_i != (*extent)->end() && FieldTuple(schema->get_sort_fields(), &*row_i).equal_strict(*key)) {
            // update the existing row
            auto row = *row_i;
            MutableTuple(schema->get_mutable_fields(), &row).assign(tuple);
            did_insert = true;
        } else {
            // insert the tuple into the extent
            auto &&row = (*extent)->insert(row_i);
            MutableTuple(schema->get_mutable_fields(), &row).assign(tuple);
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
                                             auto extent = ref.make_safe_extent(_file);
                                             auto &&row = (*extent)->back();
                                             return FieldTuple(schema->get_sort_fields(), &row).less_than(key);
                                         });
        // note: key should exist
        CHECK(extent_i != _extents.end());

        // make sure that we've got a mutable version of the extent
        auto extent = extent_i->make_dirty_safe_extent(_file);

        // find the update position in the extent
        auto row_i = std::ranges::lower_bound(**extent, *key,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), &row);
                                              });
        CHECK(row_i != (**extent).end());

        // note: row's key should match the tuple's key
        DCHECK(FieldTuple(schema->get_sort_fields(), &*row_i).equal_strict(*key));

        // update the existing row
        auto row = *row_i;
        MutableTuple(schema->get_mutable_fields(), &row).assign(tuple);

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
                                             auto extent = ref.make_safe_extent(_file);
                                             auto &&row = (*extent)->back();
                                             return FieldTuple(schema->get_sort_fields(), &row).less_than(key);
                                         });
        // note: key should exist
        CHECK(extent_i != _extents.end());

        // make sure that we've got a mutable version of the extent
        auto extent = extent_i->make_dirty_safe_extent(_file);

        // find the insert position in the extent
        auto row_i = std::ranges::lower_bound(**extent, *key,
                                              [](const Tuple &lhs, const Tuple &rhs) {
                                                  return lhs.less_than(rhs);
                                              },
                                              [&schema](const Extent::Row &row) {
                                                  return FieldTuple(schema->get_sort_fields(), &row);
                                              });

        // note: row's key should match the tuple's key
        DCHECK(FieldTuple(schema->get_sort_fields(), const_cast<Extent::Row *>(&*row_i)).equal_strict(*key));

        // remove the row
        (*extent)->remove(row_i);

        // if the extent has become empty, remove it from the page
        if ((*extent)->empty()) {
            StorageCache::get_instance()->_data_cache->drop_dirty(*extent);
            _extents.erase(extent_i);
        }
    }

    bool
    StorageCache::Page::try_remove_by_scan(TuplePtr value,
                                           ExtentSchemaPtr schema)
    {
        bool found = false;

        boost::unique_lock lock(_mutex);
        auto fields = schema->get_fields();

        auto extent_i = _extents.begin();
        uint32_t row_pos;
        while (!found && extent_i != _extents.end()) {
            auto extent = extent_i->make_safe_extent(_file);
            for (row_pos = 0; row_pos < (*extent)->row_count(); ++row_pos) {
                auto row = *((*extent)->at(row_pos));
                if (value->equal_strict(FieldTuple(fields, &row))) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                ++extent_i;
            }
        }

        if (!found) {
            return false;
        }

        _is_dirty = true;

        // mark the extent as dirty and remove the row from it
        auto extent = extent_i->make_dirty_safe_extent(_file);
        auto it = (*extent)->at(row_pos);
        (*extent)->remove(it);

        // if the extent has become empty, remove it from the page
        if ((*extent)->empty()) {
            StorageCache::get_instance()->_data_cache->drop_dirty(*extent);
            _extents.erase(extent_i);
        }

        return true;
    }

    void
    StorageCache::Page::convert(VirtualSchemaPtr schema,
                                ExtentSchemaPtr target_schema,
                                uint64_t target_xid)
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
            ExtentHeader new_header(_header().type, target_xid, target_schema->row_size(), target_schema->field_types());
            auto new_extent = cache->_data_cache->get_empty(_file, new_header);

            // get the old extent
            auto old_extent = ref.make_safe_extent(_file);

            LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "{}@{} (size: {}) to {}@{} (size: {})",
                                (*old_extent)->extent_id(),
                                (*old_extent)->header().xid,
                                (*old_extent)->header().row_size,
                                new_extent->extent_id(),
                                new_extent->header().xid,
                                new_extent->header().row_size);

            // copy the data
            for (auto &row : **old_extent) {
                FieldTuple source_tuple(source_fields, &row);

                auto new_row = new_extent->append();
                MutableTuple(target_fields, &new_row).assign(source_tuple);
            }

            new_extents.emplace_back(new_extent->cache_id(), false, new_extent);

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
        if (extent->byte_count() < _max_extent_size || extent->row_count() == 1) {
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

    std::future<std::vector<uint64_t>>
    StorageCache::Page::_async_flush(const ExtentHeader &header,
                                     std::function<void(const std::vector<uint64_t> &)> callback)
    {
        auto promise = std::make_shared<FlushPromise>();
        auto future = promise->get_future();

        /* Only one person can flush at a time.  Also removes the need to hold the Page mutex on the
            async callback since we know it won't be accessed. */
        {
            boost::unique_lock lock(_async_flush_mutex);
            if (_is_async_flushing) {
                _async_flush_waiters.push_back(promise);
                return future;
            } else {
                _is_async_flushing = true;
            }
        }

        auto cache = StorageCache::get_instance();
        auto counter = std::make_shared<std::atomic<int>>(0);
        auto result = std::make_shared<std::vector<uint64_t>>(_extents.size());

        struct FlushTask {
            SafeExtent e;
            int pos;
        };

        // collect the information about the IO to perform
        std::vector<FlushTask> tasks;
        for (int i = 0; i < _extents.size(); ++i) {
            auto &ref = _extents[i];
            if (ref.is_clean()) {
                // if clean, populate the extent ID into it's position
                result->at(i) = ref.id();
            } else {
                // XXX if the extent was already flushed to disk in the background, we don't
                //     actually need to read it in again here, we just need to get the extent ID
                auto &&e = ref.make_safe_extent(_file);
                if ((*e)->state() == CacheExtent::State::DIRTY) {
                    // update the extent header
                    (*e)->header() = header;

                    // increment the counter of dirty pages and add it to the task list
                    ++(*counter);
                    tasks.push_back({ e, i });
                } else {
                    // bring MUTABLE extents to CLEAN
                    if ((*e)->state() == CacheExtent::State::MUTABLE) {
                        // return the now MUTABLE extent back to the read cache
                        cache->_data_cache->reinsert(*e);
                    }

                    ref = e.get_ref();
                    result->at(i) = ref.id();
                }
            }
        }

        // perform the IO
        bool perform_flush = (*counter > 0);
        if (perform_flush) {
            // unlock while we issue the IO
            boost::unique_lock lock(_mutex, boost::adopt_lock);
            lock.unlock();

            for (auto &task : tasks) {
                cache->_data_cache->async_flush(*task.e, [this,
                                                          task = std::move(task),
                                                          result,
                                                          counter,
                                                          promise,
                                                          callback]() mutable {
                    auto cache = StorageCache::get_instance();

                    // bring MUTABLE extents to CLEAN
                    if ((*task.e)->state() == CacheExtent::State::MUTABLE) {
                        // return the now MUTABLE extent back to the read cache
                        cache->_data_cache->reinsert(*task.e);
                    }

                    // update the reference with the details of the new extent
                    _extents[task.pos] = task.e.get_ref();

                    // store the new extent location
                    result->at(task.pos) = _extents[task.pos].id();

                    // reduce the outstanding count
                    --(*counter);

                    // if this was the last outstanding, complete the promise
                    if (!*counter) {
                        // no longer flushing, release the page and notify any waiter
                        {
                            boost::unique_lock lock(_async_flush_mutex);
                            _is_async_flushing = false;
                            for (auto &waiter : _async_flush_waiters) {
                                waiter->set_value({}); // note: no returned value to secondary waiters
                            }
                            _async_flush_waiters.clear();
                        }

                        if (callback) {
                            callback(*result);
                        }

                        promise->set_value(std::move(*result));
                    }
                });
            }

            // reacquire the lock
            lock.lock();
            lock.release();
        } else {
            // unlock while we issue the callback
            boost::unique_lock lock(_mutex, boost::adopt_lock);
            lock.unlock();

            // no longer flushing, release the page and notify any waiter
            {
                boost::unique_lock lock(_async_flush_mutex);
                _is_async_flushing = false;
                for (auto &waiter : _async_flush_waiters) {
                    waiter->set_value({});
                }
                _async_flush_waiters.clear();
            }

            // if there was no IO to perform, complete immediately
            if (callback) {
                callback(*result);
            }

            promise->set_value(std::move(*result));

            // reacquire the lock
            lock.lock();
            lock.release();
        }

        // return the future
        return future;
    }

    std::vector<uint64_t>
    StorageCache::Page::_flush(const ExtentHeader &header)
    {
        // issue the flush request
        auto flush_future = _async_flush(header);

        // unlock while we wait for completion
        boost::unique_lock lock(_mutex, boost::adopt_lock);
        lock.unlock();

        // wait for completion
        auto ids = flush_future.get();

        // re-acquire the lock and return
        lock.lock();
        lock.release();

        return ids;
    }

    // DATA CACHE

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::get(const std::filesystem::path &file,
                                 const ExtentRef &ref,
                                 bool mark_dirty)
    {
        boost::unique_lock lock(_mutex);

        // check if the the reference is valid
        if (ref.is_direct()) {
            // get it via the direct pointer
            return _use_direct(ref.lock_cached(), mark_dirty);
        }

        // note: from here we may invoke a disk IO if we have a cache miss

        // if the ref is of a DIRTY/MUTABLE page
        if (!ref.is_clean()) {
            // retrieve it from the dirty cache
            return _get(ref.id(), mark_dirty);
        }

        // if the ref is of a CLEAN page
        auto extent = _get_clean(file, ref.id());

        if (mark_dirty) {
            // we create a DIRTY copy of it by calling extract()
            extent = _extract(extent);
        }

        return extent;
    }

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::_get(uint64_t cache_id,
                                  bool mark_dirty)
    {
        CacheExtentPtr extent = nullptr;

        // find the extent using the unique cache_id
        auto dirty_i = _dirty_cache.find(cache_id);
        if (dirty_i == _dirty_cache.end()) {
            // not in memory, so need to retrieve from disk
            auto key_i = _cache_id_map.find(cache_id);
            DCHECK(key_i != _cache_id_map.end());
            const auto& [_, value] = *key_i;

            // note: no one should know about the cache ID except for the owning page, so this
            //       should never return nullptr since there should never be two concurrent readers
            extent = _read_extent(value.second, value.first, [this, cache_id](CacheExtentPtr extent) {
                // mark it as mutable since it comes from the dirty cache
                extent->_state = CacheExtent::State::MUTABLE;
                extent->_cache_id = cache_id;

                // add it to the dirty cache
                _dirty_cache[cache_id] = extent;
            });
        } else {
            extent = dirty_i->second;

            // if the extent is being flushed, must block until complete
            if (extent->_state == CacheExtent::State::FLUSHING) {
                _wait_for_flush(extent);
                return extent;
            }

            // remove from the dirty_lru, if on it
            if (extent->_use_count == 0) {
                // extent must be MUTABLE or DIRTY if being retrieved by cache ID
                CHECK(extent->_state ==  CacheExtent::State::DIRTY ||
                       extent->_state ==  CacheExtent::State::MUTABLE);

                if (extent->_state == CacheExtent::State::MUTABLE) {
                    _clean_lru.erase(extent->_pos);
                } else {
                    _dirty_lru.erase(extent->_pos);
                }
            }

            // increase the use-count
            ++(extent->_use_count);
        }

        // optionally mark the extent as DIRTY
        if (mark_dirty) {
            extent->_state = CacheExtent::State::DIRTY;
        }

        return extent;
    }

void
StorageCache::DataCache::_wait_for_flush(const CacheExtentPtr& extent)
{
    DCHECK_GT(extent->_use_count, 0);

    // mark ourselves as a user of the extent to prevent eviction post-flush()
    ++(extent->_use_count);

    do {
        std::promise<void> promise;
        std::future<void> future = promise.get_future();

        // register the promise with the ongoing extent flush
        extent->_flush_waiters.push_back({ std::move(promise), nullptr });
        boost::unique_lock lock(_mutex, boost::adopt_lock);
        lock.unlock();

        // wait for the flush to complete
        future.get();

        // reacquire the lock
        lock.lock();
        lock.release();
    } while (extent->_state == CacheExtent::State::FLUSHING);
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
    StorageCache::DataCache::_extract(const CacheExtentPtr &extent)
    {
        // check if we are the only user
        if (extent->_use_count == 1) {
            // this is the only user, so we can pass the data of this extent to a new DIRTY extent
            // -- we don't re-use this CacheExtent since it might be attached to a clean Page
            auto new_extent = std::make_shared<CacheExtent>(*extent);

            // note: no need to adjust the LRU queue since the extent cannot be on it
            _clean_cache.erase(extent->key());

            // mark the extent as DIRTY
            new_extent->_state = CacheExtent::State::DIRTY;

            // assign the extent a unique cache ID and add it to the dirty cache
            _gen_cache_id(new_extent);
            _dirty_cache.insert({ new_extent->_cache_id, new_extent });

            return new_extent;
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

        // note: the extent must be MUTABLE (not DIRTY) and when reinsert()'d
        DCHECK_EQ(extent->_state, CacheExtent::State::MUTABLE);

        // note: now with async IO, a flush might occur but still be holding the extent when a flush
        //       on the page holding it occurs, causing the use_count to be 2
        DCHECK_GT(extent->_use_count, 0);

        // find the cache extent
        auto dirty_i = _dirty_cache.find(extent->_cache_id);
        CHECK(dirty_i != _dirty_cache.end());

        // mark the extent CLEAN
        extent->_state = CacheExtent::State::CLEAN;

        // move from the dirty cache to the clean cache
        _dirty_cache.erase(dirty_i);
        _clean_cache.insert({ extent->key(), extent });

        // clear the cache ID
        _cache_id_map.erase(extent->_cache_id);
        extent->_cache_id = 0;
    }

    void
    StorageCache::DataCache::flush(CacheExtentPtr extent)
    {
        // call the internal async_flush() helper
        boost::unique_lock lock(_mutex);
        auto &&future = _async_flush(extent);
        lock.unlock();

        // block for completion
        future.get();
    }

    std::future<void>
    StorageCache::DataCache::async_flush(CacheExtentPtr extent,
                                         std::function<void()> callback)
    {
        boost::unique_lock lock(_mutex);

        // call the internal flush() helper
        return _async_flush(extent, std::move(callback));
    }

    void
    StorageCache::DataCache::drop_dirty(CacheExtentPtr extent)
    {
        boost::unique_lock lock(_mutex);

        // note: extent must be dirty for this to be a valid operation
        CHECK_EQ(extent->_state, CacheExtent::State::DIRTY);
        CHECK_EQ(extent->_use_count, 1);

        // evict the extent from the cache
        _dirty_cache.erase(extent->_cache_id);
        _cache_id_map.erase(extent->_cache_id);

        // mark the extent as no longer valid to ensure it doesn't get released back into the cache
        // by a concurrent user
        extent->_state = CacheExtent::State::INVALID;
    }

    void
    StorageCache::DataCache::invalidate_clean(CacheExtentPtr extent)
    {
        boost::unique_lock lock(_mutex);

        // note: extent must be CLEAN or MUTABLE for this to be a valid operation
        CHECK(extent->_state == CacheExtent::State::CLEAN ||
              extent->_state == CacheExtent::State::MUTABLE);
        CHECK_EQ(extent->_use_count, 1);

        // remove from the appropriate cache
        if (extent->_state == CacheExtent::State::CLEAN) {
            _clean_cache.erase(extent->key());
        } else {
            // MUTABLE extents are in the dirty_cache
            _dirty_cache.erase(extent->_cache_id);
            _cache_id_map.erase(extent->_cache_id);
        }

        // mark the extent as no longer valid to ensure it doesn't get released back into the cache
        // by a concurrent user
        extent->_state = CacheExtent::State::INVALID;
    }

    std::pair<StorageCache::ExtentRef, StorageCache::ExtentRef>
    StorageCache::DataCache::split(CacheExtentPtr extent, ExtentSchemaPtr schema)
    {
        boost::unique_lock lock(_mutex);

        // note: extent must be DIRTY with a mutation that caused the split
        CHECK_EQ(extent->_state, CacheExtent::State::DIRTY);

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

        return std::pair<ExtentRef, ExtentRef>({ first->_cache_id, false, first },
                                               { second->_cache_id, false, second });
    }

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::_get_clean(const std::filesystem::path& file, uint64_t extent_id)
    {

        std::pair<uint64_t, const std::string&> key{extent_id, file.native()};
        CacheExtentPtr extent = nullptr;

        while (extent == nullptr) {
            // search for the requested extent
            const auto cache_i = _clean_cache.find(key);
            if (cache_i != _clean_cache.end()) {
                extent = cache_i->second;

                // remove the entry from the LRU list
                if (extent->_use_count == 0) {
                    {
                        TIME_TRACE_SCOPED(time_trace::traces, cache_get_clean_lru_erase);
                        _clean_lru.erase(extent->_pos);
                    }
                    extent->_pos = {};
                }

                // update the use count
                ++extent->_use_count;

                // exit the loop
                break;
            }

            // extent not cached, so read from disk
            // note: may return nullptr, indicating someone else just read the extent from disk and
            //       that we should check the cache again
            //
            extent = _read_extent(file, extent_id, [this, extent_id, &file](CacheExtentPtr ext) {
                    // insert the extent into the cache
                    // note: we don't place into the LRU list since the extent will be in-use
                    _clean_cache.insert({ {extent_id, file.native()}, ext });
                    });
        }

        return extent;
    }

    std::future<void>
    StorageCache::DataCache::_async_flush(CacheExtentPtr extent,
                                          std::function<void()> callback)
    {
        // construct a promise and a future to return
        std::promise<void> promise;
        std::future<void> future = promise.get_future();

        // if already flushing, wait for completion
        if (extent->_state == CacheExtent::State::FLUSHING) {
            // add ourselves to the waitlist for the flush
            extent->_flush_waiters.push_back({ std::move(promise), std::move(callback) });
            return future;
        }

        // if the extent isn't DIRTY, don't need to flush
        if (extent->_state != CacheExtent::State::DIRTY) {
            if (callback) {
                callback();
            }
            promise.set_value();
            return future;
        }

        // mark the extent as FLUSHING so that other callers will block until flush complete
        extent->_state = CacheExtent::State::FLUSHING;

        // add ourselves to the flush waiters
        extent->_flush_waiters.push_back({ std::move(promise), std::move(callback) });

        // unlock while we issue the IO
        boost::unique_lock lock(_mutex, boost::adopt_lock);
        lock.unlock();

        // issue the IO
        auto handle = IOMgr::get_instance()->open(extent->_file, IOMgr::IO_MODE::APPEND, true);

        extent->async_flush(handle, [this, extent](std::shared_ptr<IOResponseAppend> response) {
            _flush_and_update_extent(extent, response);
        });

        // reacquire the lock
        lock.lock();
        lock.release();

        return future;
    }

    void
    StorageCache::DataCache::_flush_and_update_extent(CacheExtentPtr extent,
                                                      std::shared_ptr<IOResponseAppend> flush_response)
    {
        // lock the cache
        boost::unique_lock lock(this->_mutex);

        // on-disk size of the originating extent
        uint64_t prev_extent_size = extent->_extent_size;

        // notify the vacuumer of the now-expired extent
        if (extent->header().prev_offset != constant::UNKNOWN_EXTENT) {
            StorageCache::get_instance()->call_extent_expire_notify_fun(
                    extent->_file, extent->header().prev_offset,
                    prev_extent_size, extent->header().xid);
        }

        extent->_extent_id = flush_response->offset;
        extent->_extent_size = flush_response->next_offset - flush_response->offset;

        // update the cache ID as pointing to the new extent ID
        this->_cache_id_map[extent->_cache_id] = extent->key();

        // mark as MUTABLE so it's placed on the clean LRU
        extent->_state = CacheExtent::State::MUTABLE;
        DCHECK_GT(extent->_use_count, 0);

        // remove the waiters from the extent and unlock
        auto waiters = std::move(extent->_flush_waiters);
        lock.unlock();

        // notify the waiters
        for (auto &waiter : waiters) {
            if (waiter.callback) {
                waiter.callback();
            }
            waiter.promise.set_value();
        }
    }

    void
    StorageCache::DataCache::_flush(CacheExtentPtr extent)
    {
        // perform an async flush and wait for the returned future
        auto &&future = _async_flush(extent);

        // unlock while we wait for completion
        boost::unique_lock lock(_mutex, boost::adopt_lock);
        lock.unlock();

        // wait for completion
        future.get();

        // re-lock and return
        lock.lock();
        lock.release();
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

        if (extent->_state == CacheExtent::State::CLEAN) {
            //TODO: consider changing to splice() that should be more efficient
            _clean_cache.erase(extent->key());
        } else {
            CHECK_EQ(extent->_state, CacheExtent::State::MUTABLE);
            _dirty_cache.erase(extent->_cache_id);
        }
    }

    void
    StorageCache::DataCache::_release(CacheExtentPtr extent)
    {
        // if the extent is invalid, do not place back into the cache
        if (extent->_state == CacheExtent::State::INVALID) {
            --_size;
            return;
        }

        DCHECK(extent->_use_count);

        // reduce the use count
        --(extent->_use_count);

        // if still in use, return
        if (extent->_use_count > 0) {
            return;
        }

        // note: shouldn't be possible to call _release() when FLUSHING
        DCHECK(extent->_state != CacheExtent::State::FLUSHING);

        // if the use count is zero, place into the appropriate LRU list
        if (extent->_state == CacheExtent::State::DIRTY) {
            extent->_pos = _dirty_lru.insert(_dirty_lru.end(), extent);
        } else {
            extent->_pos = _clean_lru.insert(_clean_lru.end(), extent);
        }
    }

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::_read_extent(const std::filesystem::path& file, uint64_t extent_id,
                                          std::function<void(CacheExtentPtr)> callback)
    {
        std::pair<uint64_t, const std::string&> key{extent_id, file.native()};

        // extent not cached, check if someone is reading it from disk
        auto io_i = _io_map.find(key);
        if (io_i != _io_map.end()) {
            boost::unique_lock lock(_mutex, boost::adopt_lock);

            // wait for the read to complete
            auto entry = io_i->second;
            entry->cv.wait(lock, [&entry](){ return entry->signaled; });

            // see if we should remove the entry
            --entry->counter;
            if (entry->counter == 0) {
                _io_map.erase(io_i);
            }

            // note: try to retrieve from the cache again
            lock.release();
            return nullptr;
        }

        // add the condition variable to the IO map
        auto entry = std::make_shared<IoCv>();
        _io_map[key] = entry;

        // make space for a new extent in the cache
        _make_extent_space();

        // unlock before IO
        boost::unique_lock lock(_mutex, boost::adopt_lock);
        lock.unlock();

        // read the extent
        auto handle = IOMgr::get_instance()->open(file, IOMgr::READ, true);
        auto response = handle->read(extent_id);
        auto extent = std::make_shared<CacheExtent>(response->data, file, extent_id,
                                                    response->next_offset - response->offset);

        // reacquire the lock once IO complete
        lock.lock();
        lock.release();

        // callback to initialize state before returning the extent to the callers
        callback(extent);

        // notify the other callers waiting for this extent
        entry->cv.notify_all();

        // remove the condition variable from the map
        entry->signaled = true;
        --entry->counter;
        if (entry->counter == 0) {
            _io_map.erase(key);
        }

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

    StorageCache::CacheExtentPtr
    StorageCache::DataCache::_use_direct(const CacheExtentPtr& extent,
                                         bool mark_dirty)
    {
        // if the extent is being flushed, must block until complete
        if (extent->_state == CacheExtent::State::FLUSHING) {
            // wait for the flush to complete
            _wait_for_flush(extent);
        } else {
            // check if we are the only user of the extent
            if (extent->_use_count == 0) {
                // need to remove from the appropriate LRU list
                if (extent->_state == CacheExtent::State::CLEAN ||
                    extent->_state == CacheExtent::State::MUTABLE) {
                    _clean_lru.erase(extent->_pos);
                } else if (extent->_state == CacheExtent::State::DIRTY) {
                    _dirty_lru.erase(extent->_pos);
                }
            }

            // mark ourselves as a user of the extent
            ++(extent->_use_count);
        }

        // if we aren't asking for a dirty extent, or it's already dirty / mutable, we can return immediately
        if (!mark_dirty ||
            extent->_state == CacheExtent::State::DIRTY ||
            extent->_state == CacheExtent::State::MUTABLE) {
            if (mark_dirty) {
                extent->_state = CacheExtent::State::DIRTY;
            }
            return extent;
        }

        // sanity check that the extent is not in the dirty cache
        CHECK(_dirty_cache.find(extent->_cache_id) == _dirty_cache.end());

        // extract the extent from the clean cache into the dirty cache
        return _extract(extent);
    }

    StorageCache::SafeExtent::SafeExtent(const std::filesystem::path &file,
            const ExtentRef &ref,
            bool mark_dirty)
    {
        _extent = StorageCache::get_instance()->_data_cache->get(file, ref, mark_dirty);
        DCHECK(_extent);
    }

    void
    StorageCache::PageCache::evict_for_database(uint64_t database_id)
    {
        LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "PageCache::evict_for_database database_id={}", database_id);

        // Collect all pages for this database and get the file list while holding the lock
        std::list<PagePtr> pages_to_evict;
        std::set<std::filesystem::path> files;
        {
            boost::unique_lock lock(_mutex);

            // Iterate through the entire _cache to find pages for this database
            for (auto &cache_entry : _cache) {
                for (auto &xid_entry : cache_entry.second) {
                    auto page = xid_entry.second;
                    if (page->_database_id == database_id) {
                        pages_to_evict.push_back(page);
                    }
                }
            }

            // Get the files associated with this database
            auto db_i = _database_files.find(database_id);
            if (db_i != _database_files.end()) {
                files = db_i->second;
            }
        }

        // Now process the pages without holding the cache lock
        auto data_cache = StorageCache::get_instance()->_data_cache;
        for (auto &page : pages_to_evict) {
            // clear the associated extents from the cache
            for (auto &ref : page->_extents) {
                auto extent = ref.lock_cached();
                if (extent) {
                    if (extent->state() == CacheExtent::State::DIRTY) {
                        data_cache->drop_dirty(extent);
                        data_cache->put(extent);
                    } else if (extent->state() == CacheExtent::State::CLEAN ||
                              extent->state() == CacheExtent::State::MUTABLE) {
                        data_cache->invalidate_clean(extent);
                        data_cache->put(extent);
                    }
                }
            }
        }

        // Re-acquire the lock to evict the Page objects themselves
        {
            boost::unique_lock lock(_mutex);

            for (auto &page : pages_to_evict) {
                // Evict the Page object from the PageCache
                auto cache_i = _cache.find(page->key());
                if (cache_i != _cache.end()) {
                    cache_i->second.erase(page->xid());
                    if (cache_i->second.empty()) {
                        _cache.erase(cache_i);
                    }
                }

                // Page must not be in use for safe eviction
                DCHECK_EQ(page->_use_count, 0);

                // Remove from LRU list
                if (page->_is_dirty) {
                    _dirty_lru.erase(page->_lru_pos);
                } else {
                    _clean_lru.erase(page->_lru_pos);
                }

                // Decrement the size counter
                --_size;
            }

            // Clean up the flush list for all files associated with this database
            for (const auto &file : files) {
                auto file_i = _flush_list.find(file);
                if (file_i != _flush_list.end()) {
                    // Remove all pages for this database from the flush list
                    auto &flush_pages = file_i->second;
                    for (auto page_i = flush_pages.begin(); page_i != flush_pages.end(); ) {
                        if ((*page_i)->_database_id == database_id) {
                            page_i = flush_pages.erase(page_i);
                        } else {
                            ++page_i;
                        }
                    }

                    // If the flush list for this file is now empty, remove it
                    if (flush_pages.empty()) {
                        _flush_list.erase(file_i);
                    }
                }
            }

            // Remove the database from the tracking map
            _database_files.erase(database_id);
        }
    }

}
