#include <common/constants.hh>
#include <storage/cache.hh>
#include <storage/index_engine/storage_cache_extent_source.hh>

namespace springtail {
    StorageCacheExtentSource::StorageCacheExtentSource(uint64_t max_extent_size)
        : _max_extent_size(max_extent_size)
    { }

    std::shared_ptr<Extent>
    StorageCacheExtentSource::get(uint64_t database_id,
                                  const std::filesystem::path &file,
                                  uint64_t extent_id,
                                  uint64_t xid)
    {
        // access_xid == target_xid (LATEST_XID resolves to access_xid, cache.cc's get()) means
        // StorageCache::get() never attempts roll-forward -- the target_xid == access_xid branch
        // short-circuits before do_rollforward (left at its default, false) is even consulted.
        // Confirmed by reading cache.cc, matching every call site in btree.cc exactly.
        auto page = StorageCache::get_instance()->get(database_id, file, extent_id, xid,
                                                       constant::LATEST_XID, _max_extent_size);
        return page->extent();
    }
}
