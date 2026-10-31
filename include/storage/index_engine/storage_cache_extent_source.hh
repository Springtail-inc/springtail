#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>

#include <common/constants.hh>
#include <storage/extent.hh>
#include <storage/index_engine/extent_source.hh>

namespace springtail {
    /**
     * Phase 2's only ExtentSource implementation: wraps today's global StorageCache singleton
     * (include/storage/cache.hh), exactly as BTree does today. Phase 5 replaces this with a
     * shared-memory buffer pool implementation (architecture.md Sec 6); IndexReader's own code
     * does not change when that happens -- see extent_source.hh.
     */
    class StorageCacheExtentSource : public ExtentSource {
    public:
        /**
         * `max_extent_size` exists only to satisfy StorageCache::get()'s parameter list. It feeds
         * StorageCache::Page::_check_split()'s mutation-only split-threshold check
         * (src/storage/cache.cc:1298), which this class's read-only calls (access_xid ==
         * target_xid, never do_rollforward -- see extent_source.hh) never reach. Confirmed dead
         * weight on this path by reading cache.cc, the same way it's dead weight on BTree's
         * identical copy of this parameter today (see phases/phase-2.md) -- kept only because the
         * API underneath still requires a value.
         */
        explicit StorageCacheExtentSource(uint64_t max_extent_size = constant::MAX_EXTENT_SIZE);

        std::shared_ptr<Extent> get(uint64_t database_id,
                                    const std::filesystem::path &file,
                                    uint64_t extent_id,
                                    uint64_t xid) override;

    private:
        uint64_t _max_extent_size;
    };
}
