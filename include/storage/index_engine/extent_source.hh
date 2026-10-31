#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>

#include <storage/extent.hh>

namespace springtail {
    /**
     * The Reader-side extent-content-provider seam (architecture.md "btree" redesign, Sec 5.1,
     * 6): IndexReader is constructed with one of these rather than calling
     * StorageCache::get_instance() directly, so phase 5's shared-memory buffer pool can be swapped
     * in later as a second implementation without any change to IndexReader's own traversal code.
     * Phase 2 ships exactly one implementation -- StorageCacheExtentSource
     * (storage_cache_extent_source.hh) -- wrapping today's global StorageCache singleton, exactly
     * as BTree does today.
     *
     * Deliberately *not* templated the way ReadStrategy/WriteStrategy are (see read_strategy.hh's
     * header comment for the contrast): fetching one extent is an I/O-bound operation (a cache
     * lookup today, a shared-memory-pool lookup in phase 5), not a per-row comparison on the hot
     * path, so the cost of one virtual call per extent fetch is immaterial -- and runtime
     * polymorphism here is exactly what lets a caller (or phase 5) swap the implementation without
     * recompiling IndexReader itself.
     *
     * No target-xid/roll-forward parameters: confirmed by reading StorageCache::get()'s own
     * roll-forward logic (cache.cc) that a Reader pinned to one committed xid/root
     * (architecture.md Sec 5.3) never triggers roll-forward on this path -- every call BTree makes
     * today passes access_xid as both the access and target xid (via constant::LATEST_XID, which
     * StorageCache::get() resolves back to access_xid before even checking do_rollforward), so
     * there is nothing for this seam to carry through. See storage_cache_extent_source.cc for where
     * this was re-derived against the current code rather than assumed from the planning docs.
     */
    class ExtentSource {
    public:
        virtual ~ExtentSource() = default;

        /**
         * Returns the (decompressed) content of the extent at `extent_id` in `file`, as read at
         * `xid`.
         */
        virtual std::shared_ptr<Extent> get(uint64_t database_id,
                                            const std::filesystem::path &file,
                                            uint64_t extent_id,
                                            uint64_t xid) = 0;
    };

    using ExtentSourcePtr = std::shared_ptr<ExtentSource>;
}
