#include <absl/log/check.h>

#include <common/json.hh>
#include <common/properties.hh>
#include <storage/field.hh>
#include <storage/index_engine/index_extent_cache.hh>
#include <storage/io.hh>
#include <storage/io_mgr.hh>
#include <storage/io_request.hh>

namespace springtail {
    IndexExtentCache::IndexExtentCache(uint64_t max_size)
        : _max_size(max_size)
    { }

    std::shared_ptr<IndexExtentCache>
    IndexExtentCache::get_instance()
    {
        static std::shared_ptr<IndexExtentCache> instance = std::make_shared<IndexExtentCache>();
        return instance;
    }

    std::shared_ptr<Extent>
    IndexExtentCache::get(uint64_t database_id,
                         const std::filesystem::path &file,
                         uint64_t extent_id,
                         uint32_t *size_out)
    {
        Key key{ file, extent_id };

        std::unique_lock lock(_mutex);

        auto i = _lookup.find(key);
        if (i != _lookup.end()) {
            // move to the front of the LRU list (most-recently-used)
            _lru.erase(i->second.lru_pos);
            _lru.push_front(key);
            i->second.lru_pos = _lru.begin();
            if (size_out != nullptr) {
                *size_out = i->second.size;
            }
            return i->second.extent;
        }

        // not cached -- perform the (decompressing, via IOHandle) disk read without holding the
        // lock
        lock.unlock();

        auto handle = IOMgr::get_instance()->open(file, IOMgr::READ, true);
        auto response = handle->read(extent_id);
        CHECK(response->is_success()) << "IndexExtentCache: failed to read extent " << extent_id
                                      << " from file " << file.native()
                                      << ", status: " << response->status;

        auto extent = std::make_shared<Extent>(response->data);
        uint32_t extent_size = response->next_offset - response->offset;

        lock.lock();

        // someone else may have raced us to populate the same entry; keep whichever is already
        // there rather than double-count its size
        i = _lookup.find(key);
        if (i != _lookup.end()) {
            _lru.erase(i->second.lru_pos);
            _lru.push_front(key);
            i->second.lru_pos = _lru.begin();
            if (size_out != nullptr) {
                *size_out = i->second.size;
            }
            return i->second.extent;
        }

        _lru.push_front(key);
        _lookup.insert({ key, Entry{ extent, extent_size, _lru.begin() } });
        _size += extent_size;

        if (size_out != nullptr) {
            *size_out = extent_size;
        }

        _evict_locked(lock);

        return extent;
    }

    std::shared_ptr<Extent>
    IndexExtentCache::allocate(const ExtentHeader &header) const
    {
        return std::make_shared<Extent>(header);
    }

    std::shared_ptr<Extent>
    IndexExtentCache::copy_range(std::shared_ptr<Extent> src,
                                ExtentSchemaPtr schema,
                                uint32_t start,
                                uint32_t count,
                                const ExtentHeader &header) const
    {
        auto dst = std::make_shared<Extent>(header);
        append_range(dst, src, schema, start, count);
        return dst;
    }

    void
    IndexExtentCache::append_range(std::shared_ptr<Extent> dst,
                                   std::shared_ptr<Extent> src,
                                   ExtentSchemaPtr schema,
                                   uint32_t start,
                                   uint32_t count) const
    {
        auto mutable_fields = schema->get_mutable_fields();
        auto fields = schema->get_fields();

        for (uint32_t i = start; i < start + count; i++) {
            Extent::Row &&dst_row = dst->append();

            MutableTuple tuple(mutable_fields, &dst_row);
            tuple.assign(FieldTuple(fields, &*(src->at(i))));
        }
    }

    uint64_t
    IndexExtentCache::flush(uint64_t database_id,
                           const std::filesystem::path &file,
                           std::shared_ptr<Extent> extent)
    {
        auto handle = IOMgr::get_instance()->open(file, IOMgr::IO_MODE::APPEND, true);
        auto response = extent->async_flush(handle).get();
        CHECK(response->is_success()) << "IndexExtentCache: failed to flush extent to file "
                                      << file.native() << ", status: " << response->status;

        uint64_t extent_id = response->offset;
        uint32_t extent_size = response->next_offset - response->offset;

        Key key{ file, extent_id };

        std::unique_lock lock(_mutex);

        // publish the just-flushed (already decompressed, in hand) extent into the clean cache so
        // a subsequent read of the same extent_id is a cache hit
        _lru.push_front(key);
        _lookup.insert({ key, Entry{ extent, extent_size, _lru.begin() } });
        _size += extent_size;

        _evict_locked(lock);

        return extent_id;
    }

    void
    IndexExtentCache::evict(const std::filesystem::path &file, uint64_t extent_id)
    {
        Key key{ file, extent_id };

        std::lock_guard lock(_mutex);

        auto i = _lookup.find(key);
        if (i == _lookup.end()) {
            return;
        }

        _size -= i->second.size;
        _lru.erase(i->second.lru_pos);
        _lookup.erase(i);
    }

    uint64_t
    IndexExtentCache::size() const
    {
        std::lock_guard lock(_mutex);
        return _size;
    }

    void
    IndexExtentCache::_evict_locked(std::unique_lock<std::mutex> &lock)
    {
        while (_size > _max_size && !_lru.empty()) {
            const Key &victim = _lru.back();
            auto i = _lookup.find(victim);
            CHECK(i != _lookup.end());

            _size -= i->second.size;
            _lookup.erase(i);
            _lru.pop_back();
        }
    }
}
