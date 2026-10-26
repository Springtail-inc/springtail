#include <chrono>
#include <thread>

#include <boost/functional/hash.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/named_sharable_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <storage/index_engine/shared_extent_pool.hh>

namespace springtail {

namespace ipc = boost::interprocess;
namespace bmi = boost::multi_index;

namespace {
// if free memory size goes below the limit, we start evictions until we reach the watermark.
// Mirrors sys_tbl_mgr::ShmCache's identical constants/technique (see shared_extent_pool.hh's
// class comment, OQ3) -- proven already by that class's own tests.
constexpr double FREE_MEM_LIMIT = 0.3;
constexpr double FREE_MEM_WATERMARK = 0.5;

ipc::permissions
unrestricted_permissions()
{
    ipc::permissions p;
    p.set_unrestricted();
    return p;
}
}  // namespace

class SharedExtentPool::Impl {
public:
    template <typename T>
    using Alloc = ipc::allocator<T, ipc::managed_shared_memory::segment_manager>;

    using ShmString = ipc::basic_string<char, std::char_traits<char>, Alloc<char>>;
    using ShmBytes = ipc::vector<char, Alloc<char>>;

    struct ShmKey {
        ShmString file;
        uint64_t extent_id;

        bool operator<(const ShmKey &rhs) const
        {
            if (extent_id != rhs.extent_id) {
                return extent_id < rhs.extent_id;
            }
            return file < rhs.file;
        }

        bool operator==(const ShmKey &rhs) const
        {
            return extent_id == rhs.extent_id && file == rhs.file;
        }
    };

    struct ShmKeyHash {
        size_t operator()(const ShmKey &k) const
        {
            size_t h = boost::hash_range(k.file.begin(), k.file.end());
            boost::hash_combine(h, k.extent_id);
            return h;
        }
    };

    struct Entry {
        ShmBytes header;
        ShmBytes body;
        uint32_t pin_count = 0;

        Entry(const Alloc<char> &alloc) : header(alloc), body(alloc) {}
    };

    using EntryMap =
        ipc::map<ShmKey, Entry, std::less<ShmKey>, Alloc<std::pair<const ShmKey, Entry>>>;

    // LRU: sequenced index keeps most-recently-used at the front; hashed index gives O(1)
    // lookup/relocate by key. Same technique as sys_tbl_mgr::MsgCache::Lru (see
    // shared_extent_pool.hh's OQ3 note) -- new instantiation, not shared code.
    using Lru = bmi::multi_index_container<
        ShmKey,
        bmi::indexed_by<bmi::sequenced<>, bmi::hashed_unique<bmi::identity<ShmKey>, ShmKeyHash>>,
        Alloc<ShmKey>>;

    // Markers for keys currently being decompressed/published by some process, so every other
    // process asking for the same key waits instead of redundantly decompressing (OQ6). Value is
    // seconds-since-epoch when the marker was created, used to reclaim a marker whose publisher
    // died before publishing (see STALE_INFLIGHT_SECONDS).
    using InflightMap =
        ipc::map<ShmKey, int64_t, std::less<ShmKey>, Alloc<std::pair<const ShmKey, int64_t>>>;

    using Mutex = ipc::named_sharable_mutex;
    using Lock = ipc::scoped_lock<Mutex>;
    using SharableLockT = ipc::sharable_lock<Mutex>;

    /**
     * Attaches to (creating if necessary) the named segment, recovering once from a leftover
     * segment that `open_or_create` can't actually use -- e.g. one created under a different
     * `shared_extent_pool_size_bytes` by a previous process (boost::interprocess's open_or_create
     * never resizes an existing segment to match a new request) or left truncated/corrupt by a
     * prior crash. Removing and recreating is safe even if another process is racing to do the
     * same thing: the loser of that race simply attaches to whatever the winner just created.
     * A second failure (e.g. /dev/shm genuinely too small for `size_bytes` -- the actual failure
     * this recovery path was written to diagnose and unblock, not just paper over) is a real,
     * unrecoverable error and propagates.
     */
    static ipc::managed_shared_memory open_shm(const std::string &name, uint64_t size_bytes)
    {
        try {
            return ipc::managed_shared_memory(ipc::open_or_create, name.c_str(), size_bytes,
                                              nullptr, unrestricted_permissions());
        } catch (const ipc::interprocess_exception &) {
            ipc::shared_memory_object::remove(name.c_str());
            return ipc::managed_shared_memory(ipc::open_or_create, name.c_str(), size_bytes,
                                              nullptr, unrestricted_permissions());
        }
    }

    Impl(std::string name, uint64_t size_bytes)
        : _name(std::move(name)),
          _shm(open_shm(_name, size_bytes)),
          _mutex(ipc::open_or_create, (_name + ".mutex").c_str(), unrestricted_permissions()),
          _byte_alloc(_shm.get_segment_manager())
    {
        _entries = _shm.find_or_construct<EntryMap>("entries")(
            EntryMap::allocator_type(_shm.get_segment_manager()));
        _lru = _shm.find_or_construct<Lru>("lru")(Lru::allocator_type(_shm.get_segment_manager()));
        _inflight = _shm.find_or_construct<InflightMap>("inflight")(
            InflightMap::allocator_type(_shm.get_segment_manager()));
    }

    ShmKey make_shm_key(const Key &key)
    {
        return ShmKey{ShmString(key.file.c_str(), key.file.size(), _byte_alloc), key.extent_id};
    }

    static Key from_shm_key(const ShmKey &sk)
    {
        return Key{std::string(sk.file.c_str(), sk.file.size()), sk.extent_id};
    }

    static int64_t now_seconds()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    /** Evicts least-recently-used, unpinned entries until free space clears the watermark, or
        until nothing left is evictable (every remaining entry is pinned). Must be called with
        `_mutex` already held. */
    void evict_for_space_locked()
    {
        auto free_size = _shm.get_free_memory();
        auto total_size = _shm.get_size();
        if (static_cast<double>(free_size) > static_cast<double>(total_size) * FREE_MEM_LIMIT) {
            return;
        }

        auto &seq = _lru->get<0>();
        while (true) {
            free_size = _shm.get_free_memory();
            if (static_cast<double>(free_size) >
                static_cast<double>(total_size) * FREE_MEM_WATERMARK) {
                return;
            }

            // walk from the back (least-recently-used) looking for an unpinned entry to evict.
            bool evicted = false;
            for (auto it = seq.rbegin(); it != seq.rend(); ++it) {
                auto entry_it = _entries->find(*it);
                if (entry_it == _entries->end() || entry_it->second.pin_count > 0) {
                    continue;
                }
                _entries->erase(entry_it);
                seq.erase(std::next(it).base());
                evicted = true;
                break;
            }

            if (!evicted) {
                // everything resident is pinned -- nothing more we can do right now.
                return;
            }
        }
    }

    /** Moves `key` to the front (most-recently-used end) of the LRU sequence. Must be called
        with `_mutex` already held. `key` must already be present in `_lru`. */
    void touch_locked(const ShmKey &key)
    {
        auto &hashed = _lru->get<1>();
        auto it = hashed.find(key);
        if (it == hashed.end()) {
            return;
        }
        auto &seq = _lru->get<0>();
        seq.relocate(seq.begin(), _lru->project<0>(it));
    }

    std::string _name;
    ipc::managed_shared_memory _shm;
    Mutex _mutex;
    Alloc<char> _byte_alloc;

    EntryMap *_entries;
    Lru *_lru;
    InflightMap *_inflight;
};

SharedExtentPool::Pin::Pin(Pin &&other) noexcept : _pool(other._pool), _key(std::move(other._key))
{
    other._pool = nullptr;
}

SharedExtentPool::Pin &
SharedExtentPool::Pin::operator=(Pin &&other) noexcept
{
    if (this == &other) {
        return *this;
    }
    _release();
    _pool = other._pool;
    _key = std::move(other._key);
    other._pool = nullptr;
    return *this;
}

SharedExtentPool::Pin::~Pin() { _release(); }

void
SharedExtentPool::Pin::_release()
{
    if (_pool != nullptr) {
        _pool->_unpin(_key);
        _pool = nullptr;
    }
}

SharedExtentPool::SharedExtentPool(std::string name, uint64_t size_bytes)
    : _impl(std::make_unique<Impl>(std::move(name), size_bytes))
{
}

SharedExtentPool::~SharedExtentPool() = default;

std::shared_ptr<SharedExtentPool>
SharedExtentPool::get_instance()
{
    static std::shared_ptr<SharedExtentPool> instance = [] {
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        std::string name = Json::get_or<std::string>(json, "shared_extent_pool_name", DEFAULT_NAME);
        uint64_t size_bytes =
            Json::get_or<uint64_t>(json, "shared_extent_pool_size_bytes", DEFAULT_SIZE_BYTES);
        return std::make_shared<SharedExtentPool>(std::move(name), size_bytes);
    }();
    return instance;
}

void
SharedExtentPool::remove(const std::string &name)
{
    ipc::shared_memory_object::remove(name.c_str());
    Impl::Mutex::remove((name + ".mutex").c_str());
}

namespace {
/** Packs an in-process Extent into the two raw byte blobs the pool stores -- an
    ExtentHeader::pack() blob and an Extent::serialize() blob -- the same shape Extent already
    round-trips through for on-disk/wire transfer, so no Extent/VariableData change is needed
    (see shared_extent_pool.hh's OQ2 note). */
void
pack_extent(Extent &extent, std::vector<char> &header_out, std::string &body_out)
{
    header_out = extent.header().pack();
    body_out = extent.serialize();
}

/** Reverses pack_extent(): builds a fresh, process-local Extent from the two blobs. */
std::shared_ptr<Extent>
unpack_extent(const std::vector<char> &header_bytes, const std::string &body_bytes)
{
    auto header_vec = std::make_shared<std::vector<char>>(header_bytes);
    ExtentHeader header(header_vec);
    auto extent = std::make_shared<Extent>(header);
    extent->deserialize(std::string_view(body_bytes.data(), body_bytes.size()));
    return extent;
}
}  // namespace

std::shared_ptr<Extent>
SharedExtentPool::get(const Key &key, const FetchFn &fetch)
{
    if (auto local = _local_cache_find(key)) {
        return local;
    }

    for (;;) {
        auto shm_key = _impl->make_shm_key(key);

        {
            Impl::Lock lock(_impl->_mutex);

            auto it = _impl->_entries->find(shm_key);
            if (it != _impl->_entries->end()) {
                it->second.pin_count++;
                _impl->touch_locked(shm_key);

                std::vector<char> header_bytes(it->second.header.begin(), it->second.header.end());
                std::string body_bytes(it->second.body.begin(), it->second.body.end());

                lock.unlock();

                auto extent = unpack_extent(header_bytes, body_bytes);
                _unpin(key);
                _local_cache_store(key, extent);
                return extent;
            }

            auto inflight_it = _impl->_inflight->find(shm_key);
            if (inflight_it != _impl->_inflight->end()) {
                if (Impl::now_seconds() - inflight_it->second < STALE_INFLIGHT_SECONDS) {
                    // someone else is decompressing/publishing this key -- wait for them rather
                    // than redundantly decompressing ourselves (OQ6: exactly one fetch() per key
                    // per miss episode, not "usually one").
                    lock.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                // the original publisher never finished (crashed?) -- reclaim the marker and
                // take over publishing ourselves (see shared_extent_pool.hh's crash-safety note).
                _impl->_inflight->erase(inflight_it);
            }

            _impl->_inflight->emplace(shm_key, Impl::now_seconds());
        }

        // decompress OUTSIDE the lock -- this is the (potentially slow, disk-bound) part other
        // keys' lookups/publishes must never block on.
        std::shared_ptr<Extent> extent;
        try {
            extent = fetch();
        } catch (...) {
            Impl::Lock lock(_impl->_mutex);
            _impl->_inflight->erase(shm_key);
            throw;
        }

        {
            Impl::Lock lock(_impl->_mutex);
            _impl->_inflight->erase(shm_key);

            if (_impl->_entries->find(shm_key) == _impl->_entries->end()) {
                std::vector<char> header_bytes;
                std::string body_bytes;
                pack_extent(*extent, header_bytes, body_bytes);

                try {
                    _impl->evict_for_space_locked();

                    auto result =
                        _impl->_entries->emplace(shm_key, Impl::Entry(_impl->_byte_alloc));
                    auto &entry = result.first->second;
                    entry.header.assign(header_bytes.begin(), header_bytes.end());
                    entry.body.assign(body_bytes.begin(), body_bytes.end());

                    auto &seq = _impl->_lru->get<0>();
                    seq.push_front(shm_key);
                } catch (const std::exception &e) {
                    // out of shared-memory space and nothing evictable (e.g. everything else
                    // resident is pinned) -- degrade gracefully: this process still returns the
                    // decompressed result to its caller, it just isn't published for others this
                    // time. Not a correctness issue, same "hygiene, not a dependency" framing
                    // architecture.md Sec 6 applies to reclamation ordering.
                    LOG_WARN(
                        "SharedExtentPool: failed to publish entry ({}), continuing unpublished",
                        e.what());
                    _impl->_entries->erase(shm_key);
                }
            }
        }

        _local_cache_store(key, extent);
        return extent;
    }
}

void
SharedExtentPool::_unpin(const Key &key)
{
    auto shm_key = _impl->make_shm_key(key);
    Impl::Lock lock(_impl->_mutex);
    auto it = _impl->_entries->find(shm_key);
    if (it != _impl->_entries->end() && it->second.pin_count > 0) {
        it->second.pin_count--;
    }
}

size_t
SharedExtentPool::entry_count() const
{
    Impl::SharableLockT lock(_impl->_mutex);
    return _impl->_entries->size();
}

bool
SharedExtentPool::contains(const Key &key) const
{
    Impl::SharableLockT lock(_impl->_mutex);
    return _impl->_entries->find(_impl->make_shm_key(key)) != _impl->_entries->end();
}

std::optional<SharedExtentPool::Pin>
SharedExtentPool::try_pin(const Key &key)
{
    auto shm_key = _impl->make_shm_key(key);
    Impl::Lock lock(_impl->_mutex);
    auto it = _impl->_entries->find(shm_key);
    if (it == _impl->_entries->end()) {
        return std::nullopt;
    }
    it->second.pin_count++;
    _impl->touch_locked(shm_key);
    return Pin(this, key);
}

std::shared_ptr<Extent>
SharedExtentPool::_local_cache_find(const Key &key)
{
    std::lock_guard<std::mutex> lock(_local_mutex);
    auto it = _local_cache.find(key);
    if (it == _local_cache.end()) {
        return nullptr;
    }
    auto extent = it->second.lock();
    if (!extent) {
        _local_cache.erase(it);
    }
    return extent;
}

void
SharedExtentPool::_local_cache_store(const Key &key, const std::shared_ptr<Extent> &extent)
{
    std::lock_guard<std::mutex> lock(_local_mutex);

    // opportunistic hygiene sweep of expired weak_ptrs -- see shared_extent_pool.hh's note on
    // this map never growing unbounded in practice (phase 2's "grep every field for actual
    // read-usage"/phase 1's "audit every piece of bookkeeping" lessons, applied to this new
    // bookkeeping structure).
    if (_local_cache.size() > 4096) {
        for (auto it = _local_cache.begin(); it != _local_cache.end();) {
            if (it->second.expired()) {
                it = _local_cache.erase(it);
            } else {
                ++it;
            }
        }
    }

    _local_cache[key] = extent;
}

}  // namespace springtail
