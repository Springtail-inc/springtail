#pragma once

#include <cstdint>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <storage/extent.hh>
#include <storage/schema.hh>

namespace springtail {
    /**
     * The Writer-side process-local extent cache (architecture.md "btree" redesign, Sec 8).
     * Deliberately a *separate*, simpler cache from the read side's StorageCache: process-local
     * heap memory (no shared-memory segment), ordinary mutex, no tree-shape awareness
     * whatsoever -- it knows how to allocate/read/copy/write extents, nothing about pages,
     * splitting, or branch/leaf distinctions. Every such decision lives in IndexWriter and its
     * WriteStrategy (write_strategy.hh) instead.
     *
     * Compression: IOHandle (storage/io.hh, storage/io_file.hh) already compresses on
     * append/write and decompresses transparently on read -- this cache calls IOHandle exactly
     * as StorageCache does and never touches a compressor itself. There is nothing for this
     * phase to "replicate" from StorageCache::Page on that front (phase-1 OQ2): the compression
     * boundary is already one layer further down than the cache layer, and both caches sit above
     * it identically.
     *
     * Clean vs. dirty: only *clean* (already-flushed, on-disk) extents are tracked in this
     * cache's keyed, evictable lookup table, the same way StorageCache's read side works. An
     * in-progress (dirty, not-yet-flushed) extent has no stable (file, extent_id) key to evict or
     * look up by -- it doesn't exist on disk yet -- and the single-writer-per-tree invariant
     * (architecture.md Sec 5.4) means nothing but the one IndexWriter mutating it ever needs to
     * find it, so IndexWriter simply holds dirty extents directly (in its own in-memory node
     * tree) until the moment it flushes them through this cache. This cache's "support" for dirty
     * extents is therefore procedural -- allocate()/copy_range() hand IndexWriter fresh Extent
     * objects to mutate -- rather than a second keyed map; see phases/phase-1.md for this
     * phase's record of the decision.
     */
    class IndexExtentCache {
    public:
        /** Default cache size (bytes of decompressed clean-extent content) if not configured via
            Properties::get(Properties::STORAGE_CONFIG)'s "index_writer_cache_size". */
        static constexpr uint64_t DEFAULT_MAX_SIZE = 16 * 1024 * 1024;

        explicit IndexExtentCache(uint64_t max_size = DEFAULT_MAX_SIZE);

        /**
         * Read (or return the cached, already-decompressed copy of) the extent at `extent_id` in
         * `file`. Purely mechanical: performs the on-disk IO via IOHandle and returns the result,
         * with no awareness of what the extent's rows mean.
         */
        std::shared_ptr<Extent> get(uint64_t database_id,
                                    const std::filesystem::path &file,
                                    uint64_t extent_id);

        /** Allocate a brand-new, empty, writable extent with the given header. */
        std::shared_ptr<Extent> allocate(const ExtentHeader &header) const;

        /**
         * Copy `count` rows starting at `start` from `src` (laid out per `schema`) into a
         * brand-new extent with the given header. Purely mechanical row movement -- never
         * decides *whether or where* to split; that's WriteStrategy::needs_split()/
         * split_point()'s job.
         */
        std::shared_ptr<Extent> copy_range(std::shared_ptr<Extent> src,
                                           ExtentSchemaPtr schema,
                                           uint32_t start,
                                           uint32_t count,
                                           const ExtentHeader &header) const;

        /**
         * Flush a dirty extent to `file`, returning its newly assigned extent_id. The flushed
         * extent (now immutable, decompressed, in hand) is published into the clean cache so a
         * subsequent read of the same extent_id doesn't have to immediately re-read what was
         * just written.
         */
        uint64_t flush(uint64_t database_id,
                      const std::filesystem::path &file,
                      std::shared_ptr<Extent> extent);

        /** Drop `extent_id` from the clean cache. Memory hygiene only, never a correctness
            precondition (architecture.md Sec 6's reasoning applies identically here). */
        void evict(const std::filesystem::path &file, uint64_t extent_id);

        /** Total bytes of decompressed clean-extent content currently cached. */
        uint64_t size() const;

    private:
        struct Key {
            std::filesystem::path file;
            uint64_t extent_id;

            bool operator==(const Key &rhs) const {
                return extent_id == rhs.extent_id && file == rhs.file;
            }
        };

        struct KeyHash {
            std::size_t operator()(const Key &key) const {
                return std::hash<std::string>()(key.file.native()) ^ (std::hash<uint64_t>()(key.extent_id) << 1);
            }
        };

        struct Entry {
            std::shared_ptr<Extent> extent;
            uint32_t size;
            std::list<Key>::iterator lru_pos;
        };

        void _evict_locked(std::unique_lock<std::mutex> &lock);

        mutable std::mutex _mutex;
        std::unordered_map<Key, Entry, KeyHash> _lookup;
        std::list<Key> _lru;
        uint64_t _size = 0;
        uint64_t _max_size;
    };
}
