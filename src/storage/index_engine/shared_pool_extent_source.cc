#include <common/constants.hh>
#include <storage/cache.hh>
#include <storage/index_engine/shared_pool_extent_source.hh>

namespace springtail {
SharedPoolExtentSource::SharedPoolExtentSource(uint64_t max_extent_size)
    : _max_extent_size(max_extent_size)
{
}

std::shared_ptr<Extent>
SharedPoolExtentSource::get(uint64_t database_id,
                            const std::filesystem::path &file,
                            uint64_t extent_id,
                            uint64_t xid)
{
    SharedExtentPool::Key key{file.native(), extent_id};

    // Only called at all if the pool doesn't already have `key` published -- see
    // SharedExtentPool::get()'s OQ6 handling. Identical to StorageCacheExtentSource::get():
    // access_xid == target_xid means StorageCache::get() never attempts roll-forward.
    auto fetch = [database_id, &file, extent_id, xid, this]() {
        auto page = StorageCache::get_instance()->get(database_id, file, extent_id, xid,
                                                      constant::LATEST_XID, _max_extent_size);
        return page->extent();
    };

    return SharedExtentPool::get_instance()->get(key, fetch);
}
}  // namespace springtail
