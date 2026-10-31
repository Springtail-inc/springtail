#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>

#include <common/constants.hh>
#include <storage/extent.hh>
#include <storage/index_engine/extent_source.hh>
#include <storage/index_engine/shared_extent_pool.hh>

namespace springtail {
/**
 * Phase 5's `ExtentSource` implementation (architecture.md "btree" redesign Sec 6), replacing
 * phase 2's `StorageCacheExtentSource` at IndexReader's three construction sites in
 * src/sys_tbl_mgr/table.cc. On a miss, falls back to exactly the same
 * `StorageCache::get_instance()->get(...)` call `StorageCacheExtentSource` makes -- this class
 * doesn't change how a single process reads/decompresses an extent from disk, only how many
 * *other* processes on the same host have to do that work themselves for the same extent (see
 * shared_extent_pool.hh for where the actual sharing happens).
 *
 * `max_extent_size` has the identical "dead weight on this read-only path, kept only because
 * the API underneath still requires a value" status `StorageCacheExtentSource` documents --
 * unchanged by this class, since the fallback path is the same `StorageCache::get()` call.
 */
class SharedPoolExtentSource : public ExtentSource {
public:
    explicit SharedPoolExtentSource(uint64_t max_extent_size = constant::MAX_EXTENT_SIZE);

    std::shared_ptr<Extent> get(uint64_t database_id,
                                const std::filesystem::path &file,
                                uint64_t extent_id,
                                uint64_t xid) override;

private:
    uint64_t _max_extent_size;
};
}  // namespace springtail
