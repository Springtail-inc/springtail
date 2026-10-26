#pragma once

#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include <storage/extent.hh>

namespace springtail {
/**
 * Phase 5's implementation of architecture.md Sec 6: a shared-memory region of decompressed
 * extent content, shared by every Postgres backend process on a host, populated lazily
 * (first backend to need an extent decompresses and publishes, every other backend gets a
 * pinned reference to the same published copy), LRU-evicted under memory pressure, clean-only.
 *
 * This header documents the resolution of every open question the phase-5 brief called out by
 * name; see phases/phase-5.md's "Open questions resolved here" section for the short version.
 *
 * --- OQ3: new primitive, not ShmCache/MsgCache ---
 * `sys_tbl_mgr::ShmCache`/`MsgCache<Traits>` (include/sys_tbl_mgr/{shm_cache,msg_cache}.hh) is
 * an existing boost::interprocess-backed cache, but a near-miss, not a fit: its `find()`
 * returns `std::optional<std::string>` -- a copy out of shared memory on every call, never a
 * pointer/handle into the shared region -- and its eviction has no pin-counting concept at
 * all (`evict_locked()` unconditionally pops the LRU tail). Both are exactly what this phase
 * needs and MsgCache doesn't have. Retrofitting pin-counting into the generic
 * `MsgCache<Traits>` template risks destabilizing its one production user today --
 * `write_cache`'s `SHM_CACHE_EXTENTS`, which architecture.md Sec 11 puts out of scope for this
 * whole redesign. So this class is new code. It *does* reuse the proven technique (not the
 * code): a named `boost::interprocess::managed_shared_memory` segment, a
 * `boost::multi_index_container` (sequenced + hashed) for LRU, and `ShmCache`'s free-space-
 * ratio-based eviction trigger (`check_free_space`/`FREE_MEM_LIMIT`/`FREE_MEM_WATERMARK`).
 *
 * --- OQ2: what "pinned pointer into the same shared copy" means here ---
 * `Extent` (storage/extent.hh) holds `std::shared_ptr<std::vector<char>>` and
 * `std::shared_ptr<VariableData>` -- ordinary process-local-heap containers with process-local
 * pointers. There is no portable way to make a `std::vector<char>` alias externally-owned
 * shared-memory bytes without copying (`std::vector` always owns and allocates through its own
 * allocator; it has no "borrow a buffer" constructor) short of reworking `Extent`/
 * `VariableData` to use shared-memory-aware allocators/offset pointers throughout -- the
 * brief's rejected-as-too-invasive option (b). This class takes option (a) instead: the shared
 * region holds only raw decompressed bytes (an `ExtentHeader::pack()` blob plus an
 * `Extent::serialize()` blob -- precisely the on-disk/wire shape `Extent` already round-trips
 * through, so no `Extent`/`VariableData` change was needed at all); every process that finds an
 * already-published entry pays one copy of those bytes into a fresh process-local `Extent`
 * (via `Extent::deserialize()`). Stated plainly, since the brief asks for it to be stated
 * rather than silently assumed: **this eliminates the redundant decompression (the expensive,
 * CPU-bound part architecture.md Sec 6 is primarily worried about) but does not eliminate
 * resident-memory duplication** -- N backend processes that each touch a hot extent still end
 * up with N process-local copies of its decompressed bytes, just without N redundant
 * decompressions to produce them. Eliminating the memory duplication too would require option
 * (b)'s `Extent` rework, which is out of scope here.
 *
 * The "pin" therefore only needs to protect the shared entry for the duration of copying its
 * bytes out (get()'s hit path) -- once a process has its own `Extent`, it never touches shared
 * memory for that read again. This is narrower than `StorageCache::Page`'s pin (held for the
 * Page's whole lifetime) by design, not by oversight.
 *
 * --- Identity invariant carried over from StorageCache (restated for this model) ---
 * `IndexReader::Iterator::Frame` (index_reader.hh) compares extents by `shared_ptr` identity,
 * not content, and documents that this is only sound if the provider "hands back the same
 * object for the same key while it's resident". `StorageCacheExtentSource` gets this for free
 * from `StorageCache`'s own weak_ptr-based `ExtentRef`, via a lock held across its *entire*
 * lookup-miss-create sequence (`StorageCache::PageCache::get()`, cache.cc) -- same-process
 * concurrent misses fully serialize there, so two callers can never independently construct two
 * different objects for the same key. This class's cross-process lock *cannot* do the same (it
 * must release during `fetch()`, a potentially slow disk read, or every other process -- not
 * just this one's other threads -- would block on one key's I/O; see the OQ6 section below), so
 * same-process identity needs its own, separate mechanism: a process-local `(file, extent_id) ->
 * weak_ptr<Extent>` map (`_local_cache` below) for the already-resolved common case, plus a
 * process-local single-flight gate (`_local_inflight`, `std::shared_future`-based) so that if a
 * second same-process caller asks for a key while a first caller is still resolving it (shared-
 * pool lookup, or `fetch()` itself), the second caller *waits for and reuses* the first caller's
 * result instead of independently resolving its own, distinct `Extent` object. Round-1 review
 * (phase-5.md) caught this as a real gap: the two call sites that populate `_local_cache` (the
 * shared-pool hit branch and the publish branch) raced with no synchronization between them
 * beyond protecting the map's own internal structure -- not the same thing as guaranteeing which
 * caller's object wins is the *only* object in play. `get()`'s single-flight gate closes that:
 * every same-process caller for a given key, concurrent or not, is guaranteed to observe exactly
 * one call into the shared-pool-level resolution logic (`_get_and_publish()`) and exactly one
 * resulting `Extent` object, regardless of thread count. This lives on `SharedExtentPool` itself,
 * not on `SharedPoolExtentSource`, so every `ExtentSource` built on top of one process's pool
 * instance shares the same identity map/gate, matching `StorageCache::get_instance()`'s
 * single-process-singleton scope.
 *
 * --- OQ6: synchronizing "first backend publishes, others wait" ---
 * Decompression of *different* keys never blocks on each other: the pool's one mutex is held
 * only for actual map bookkeeping (lookup, pin-count/LRU update, insert, evict) -- never across a
 * `fetch()` call, a byte-level copy of a published entry's content, or `Extent::serialize()`'s
 * CPU-bound packing work. (Round-1 review caught both of the latter two: an earlier revision held
 * the lock across the hit path's byte copy and the publish path's `pack_extent()` call, which are
 * not map operations and do not need mutual exclusion -- a published entry's content never
 * changes after publish, so copying it out is safe to do unlocked once pinned, and serializing a
 * process-local `Extent` into bytes needs no shared state at all. Both were moved out of the
 * locked section; only the unavoidable copy of already-serialized bytes *into* the shared
 * container at insert time remains locked, since that's the actual shared-memory mutation other
 * processes' lookups must never observe half-written.) Decompression of the *same* key by two
 * racing processes is NOT a first-wins/discard-the-loser race: the first process to miss marks
 * the key "in flight" (under the mutex) before calling `fetch()`; any other process that sees the
 * key in flight polls (briefly sleeping, not holding the mutex) until the publisher finishes,
 * then reads the published entry instead of also calling `fetch()`. This guarantees exactly one
 * `fetch()` call per key per "miss episode", not just "usually one" -- load-bearing for the
 * multi-process test's "assert exactly one decompression occurs" assertion. A stale in-flight
 * marker (publisher process crashed after marking in-flight, before publishing -- see the
 * crash-safety note below) is reclaimed by any waiter once it's older than
 * `STALE_INFLIGHT_SECONDS`, which also bounds how long a crash can wedge a key.
 *
 * --- Crash safety (restating the lifetime invariant for the cross-process model) ---
 * Extent identity and content are never mutated after publish (clean-only, architecture.md Sec
 * 5.3) -- a process reading a published entry is safe regardless of what any other process
 * does concurrently, including dying. The only state a crash can corrupt is bookkeeping: (1) a
 * crash while holding the pool's `named_sharable_mutex` -- boost::interprocess's named mutexes
 * are not crash-robust, so a crash mid-bookkeeping could wedge the pool for every other
 * process. This is an existing, accepted risk already present in `ShmCache` (same mutex type,
 * same short-held-critical-section usage) -- not a new one introduced here, and the held
 * window is the same kind of short map operation `ShmCache` already accepts. (2) a crash while
 * a key is marked in-flight (after releasing the mutex, before publishing) -- handled
 * explicitly above via the staleness timeout, specifically *because* this is new exposure this
 * phase introduces (unlike plain `ShmCache`, which has no in-flight concept at all). Roll-
 * forward is never reachable on this read path, exactly as `StorageCacheExtentSource` already
 * established -- unaffected by any of the above.
 *
 * --- Cache key shape (OQ5 in the brief's numbering) ---
 * `(file, extent_id)` only, no `database_id` (already encoded in `file`'s path, see
 * `Table::_table_dir`) and no `segment_id` (segment files don't exist yet -- phase 6). Nothing
 * here precludes a future `segment_id` field; nothing here was built anticipating one either.
 */
class SharedExtentPool {
public:
    /** Default shared-memory segment name if not configured. All backend processes on a host
        must use the same name to actually share a pool -- this is why it comes from
        Properties::STORAGE_CONFIG rather than being per-process-random. */
    static constexpr char DEFAULT_NAME[] = "springtail.extent_pool";

    /** Default total shared-memory segment size in bytes if not configured via
        Properties::get(Properties::STORAGE_CONFIG)'s "shared_extent_pool_size_bytes". 16MiB
        comfortably holds a few hundred extents at today's 64KiB MAX_EXTENT_SIZE -- an
        implementer's starting default (architecture.md Sec 10 OQ7), not a measured figure.
        Deliberately conservative: /dev/shm is a size-capped tmpfs mount (not just bounded by
        overall host RAM), shared by every other named-shared-memory user on the host
        (ShmCache's SHM_CACHE_*, Postgres's own shared memory segments) -- production should
        size this up via config once real headroom is known, rather than this class assuming
        a large slice of /dev/shm is free by default. */
    static constexpr uint64_t DEFAULT_SIZE_BYTES = 16ULL * 1024 * 1024;

    /** How long an "in-flight" (being decompressed/published) marker may persist before
        another waiter assumes the original publisher died and takes over. Bounds how long a
        crashed backend can wedge a single key; see the crash-safety note above. */
    static constexpr uint32_t STALE_INFLIGHT_SECONDS = 10;

    /** Cache key: (file, extent_id) -- see the class comment's "Cache key shape" section. */
    struct Key {
        std::string file;
        uint64_t extent_id;

        bool operator==(const Key &rhs) const
        {
            return extent_id == rhs.extent_id && file == rhs.file;
        }
    };

    struct KeyHash {
        size_t operator()(const Key &k) const
        {
            return std::hash<std::string>()(k.file) ^ (std::hash<uint64_t>()(k.extent_id) << 1);
        }
    };

    using FetchFn = std::function<std::shared_ptr<Extent>()>;

    /**
     * Process-wide instance attached to the one shared pool for this host, named/sized from
     * Properties::get(Properties::STORAGE_CONFIG) ("shared_extent_pool_name"/
     * "shared_extent_pool_size_bytes"). Lazily-constructed function-local static, mirroring
     * IndexExtentCache::get_instance() rather than the heavier Singleton<T>/ServiceId
     * machinery -- this is a plain utility with no thread/shutdown lifecycle of its own.
     */
    static std::shared_ptr<SharedExtentPool> get_instance();

    /** Attaches to (creating if necessary) the named shared-memory segment. Exposed directly
        for tests that need an isolated, uniquely-named pool rather than the process-wide
        singleton. */
    SharedExtentPool(std::string name, uint64_t size_bytes);

    ~SharedExtentPool();

    SharedExtentPool(const SharedExtentPool &) = delete;
    SharedExtentPool &operator=(const SharedExtentPool &) = delete;

    /**
     * Returns the decompressed content of `key`. If another process (or this one, for a
     * different key) has already published it, returns a fresh process-local `Extent` built
     * from the published bytes without calling `fetch`. Otherwise calls `fetch()` -- exactly
     * once per key per miss episode across every process attached to this pool, see the class
     * comment's OQ6 section -- and publishes the result for everyone else.
     *
     * Provides the same-object-identity guarantee `IndexReader` needs across repeat calls for
     * the same key within one process, including concurrent same-process calls racing on a key
     * that's not yet resolved locally -- see the class comment's "Identity invariant" section.
     */
    std::shared_ptr<Extent> get(const Key &key, const FetchFn &fetch);

    /** Number of entries currently published (resident), for tests. */
    size_t entry_count() const;

    /** True if `key` is currently published, regardless of pin state. For tests. */
    bool contains(const Key &key) const;

    /**
     * RAII pin on a published entry, preventing its eviction until released. Ordinary
     * `get()` callers never need this directly -- `get()` only ever holds a pin for the
     * duration of copying bytes out of an existing entry (see the class comment's OQ2
     * section). This is exposed so tests can hold a pin open across deliberately-induced
     * eviction pressure and assert the pinned entry survives it.
     */
    class Pin {
    public:
        Pin() = default;
        Pin(const Pin &) = delete;
        Pin &operator=(const Pin &) = delete;
        Pin(Pin &&other) noexcept;
        Pin &operator=(Pin &&other) noexcept;
        ~Pin();

        bool valid() const { return _pool != nullptr; }

    private:
        friend class SharedExtentPool;
        Pin(SharedExtentPool *pool, Key key) : _pool(pool), _key(std::move(key)) {}

        void _release();

        SharedExtentPool *_pool = nullptr;
        Key _key;
    };

    /** Pins `key` if it is currently published. Returns nullopt if not published. For tests
        (see Pin). */
    std::optional<Pin> try_pin(const Key &key);

    /** Removes the named shared-memory segment and its associated mutex. For tests that need
        a clean pool between cases -- never called by production code, since the segment is
        deliberately long-lived for the host's uptime (see shared_extent_pool.cc). */
    static void remove(const std::string &name);

private:
    void _unpin(const Key &key);

    /** The shared-pool-level resolution `get()` delegates to once it has established (via the
        process-local single-flight gate, see `_local_mutex`/`_local_inflight` below) that this
        call is the *only* same-process caller resolving `key` right now: the actual shared-pool
        hit/miss/publish logic, including the OQ6 in-flight handshake with other processes. */
    std::shared_ptr<Extent> _get_and_publish(const Key &key, const FetchFn &fetch);

    class Impl;
    std::unique_ptr<Impl> _impl;

    // Process-local (file, extent_id) -> weak_ptr<Extent> map, restoring the same-object
    // identity IndexReader's Iterator::Frame depends on for repeat same-process lookups. See
    // the class comment's "Identity invariant" section. Owned here (not
    // SharedPoolExtentSource) so every ExtentSource built on top of the same process's pool
    // instance benefits, matching StorageCache's single-process-singleton scope exactly.
    mutable std::mutex _local_mutex;
    std::unordered_map<Key, std::weak_ptr<Extent>, KeyHash> _local_cache;

    // Process-local single-flight gate: while a key is present here, some thread in this
    // process is already inside _get_and_publish() resolving it. Any other same-process thread
    // that finds a key here waits on the shared_future instead of independently resolving its
    // own, distinct Extent object -- see the class comment's "Identity invariant" section and
    // round-1 review Finding 2. Protected by the same _local_mutex as _local_cache (not a
    // separate mutex): every transition that touches one of these two maps always needs to look
    // at the other to decide what to do (found in cache? found in-flight? neither?), so one
    // mutex covering both avoids a second lock-ordering axis to reason about.
    std::unordered_map<Key, std::shared_future<std::shared_ptr<Extent>>, KeyHash> _local_inflight;

    std::shared_ptr<Extent> _local_cache_find(const Key &key);
    void _local_cache_store(const Key &key, const std::shared_ptr<Extent> &extent);
};

using SharedExtentPoolPtr = std::shared_ptr<SharedExtentPool>;
}  // namespace springtail
