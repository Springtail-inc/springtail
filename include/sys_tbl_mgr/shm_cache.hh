#pragma once

#include <chrono>
#include <optional>
#include <shared_mutex>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/named_sharable_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/functional/hash.hpp>

#include <sys_tbl_mgr/msg_cache.hh>

namespace springtail::sys_tbl_mgr {

namespace ipc = boost::interprocess;

// global cache names
static constexpr char SHM_CACHE_ROOTS[] = "springtail.roots";
static constexpr char SHM_CACHE_SCHEMAS[] = "springtail.schemas";
static constexpr char SHM_CACHE_USERTYPES[] = "springtail.usertypes";

/**
 * Find XID from history using schema XID as the key.
 * @param history The history container.
 * @param schema_xid The target schema XID.
 * @param last_xid The latest committed XID.
 */
template<typename T>
uint64_t get_committed_xid_from_history(const T& history, uint64_t schema_xid, uint64_t last_xid)
{
    auto pos_i = std::ranges::upper_bound(
        history,
        schema_xid,
        [] (uint64_t a, uint64_t b) {
            return a < b;
        },
        [] (const T::value_type& entry) {
            return entry.schema_xid;
        }
    );

    if (pos_i == history.end()) {
        return last_xid;
    }
    auto target_xid = pos_i->latest_real_commit_xid;
    DCHECK(target_xid != 0);

    if (target_xid > last_xid) {
        return last_xid;
    }

    return target_xid;
}


/**
 * A generic interprocess cache. The cache is intended for caching serialized
 * object/table metadata. The metadata strings are keyed by the database ID,
 * object ID and XID/LSN.
 */
class ShmCache 
{
    using Xid = uint64_t;

    // Traits for the MsgCache
    struct Traits {
        template <typename T>
            using Alloc = ipc::allocator<T, ipc::managed_shared_memory::segment_manager>;

        // serialized (like protobuf) message stored in the cache
        using Value = ipc::vector<char, Alloc<char>>;

        // containers used in the cache
        template<typename Message>
            using Messages = ipc::vector<Message, Alloc<Message>>;

        template<typename Key, typename Message>
            using Cache = ipc::map<Key, 
                  Messages<Message>,
                  std::less<Key>,
                  Alloc<std::pair<const Key, Messages<Message>>>>;

        // synchronization
        using Mutex = ipc::named_sharable_mutex;
        using Lock = ipc::scoped_lock<Mutex>;
        using SharableLock = ipc::sharable_lock<Mutex>;
    };
    using GenericCache = MsgCache<Traits>;

    template <typename T>
    using Alloc = Traits::Alloc<T>;

public:

    /**
     * In order for get_committed_xid() to return a valid XID, update_committed_xid() 
     * or keep_alive() must be called at least once every XID_KEEP_ALIVE_PERIOD.
     */
    static constexpr std::chrono::duration XID_KEEP_ALIVE_PERIOD = std::chrono::milliseconds(60);

    /*
     * Create a new cache with the given name. If a cache with
     * the name already exists, it will throw.
     * @param name The global cache name.
     * @param size The cache size in bytes.
     */

    ShmCache(std::string name, size_t size);
    /*
     * Open a cache with the give name. If the cache hasn't been created,
     * it will throw.
     * @param name The global cache name.
     */
    explicit ShmCache(std::string name);

    ~ShmCache();

    /**
     * Returns number of elements in the cache.
     */
    size_t size() const {
        return _msg_cache.size();
    }

    /**
     * Mark the object as dropped.
     * @param db The DB ID.
     * @param obj_id The object ID.
     * @param xid The XID/LSN.
     */
    bool mark_dropped(DbId db, ObjId obj_id, XidLsn xid)
    {
        return _msg_cache.insert(db, obj_id, xid, "", true);
    }

    /**
     * Mark the object as dropped (overload for Xid only).
     * @param db The DB ID.
     * @param obj_id The object ID.
     * @param xid The XID.
     */
    bool mark_dropped(DbId db, ObjId obj_id, Xid xid)
    {
        return mark_dropped(db, obj_id, XidLsn{xid, 0});
    }

    /**
     * Cache the string message.
     * @param db The DB ID.
     * @param obj_id The object ID.
     * @param xid The XID/LSN.
     * @param msg The message to cache. Usually it's a serialized proto message.
     * @return true if the element has been actually inserted
     *         and false if it was already in the cache.
     */
    bool
    insert(DbId db, ObjId obj_id, const XidLsn& xid, std::string_view msg)
    {
        check_free_space_locked();
        return _msg_cache.insert(db, obj_id, xid, msg, false);
    }

    /**
     * Cache the string message (overload for Xid only).
     * @param db The DB ID.
     * @param obj_id The object ID.
     * @param xid The XID.
     * @param msg The message to cache. Usually it's a serialized proto message.
     * @return true if the element has been actually inserted
     *         and false if it was already in the cache.
     */
    bool
    insert(DbId db, ObjId obj_id, Xid xid, std::string_view msg)
    {
        return insert(db, obj_id, XidLsn{xid, 0}, msg);
    }

    /**
     * Get the cached string if present based on a key.
     * @param db The DB ID.
     * @param obj_id The object ID.
     * @param xid The XID/LSN.
     * @return The cached string.
     */
    std::optional<std::string> find(DbId db, ObjId obj_id, const XidLsn& xid)
    {
        return _msg_cache.find(db, obj_id, xid);
    }

    /**
     * Get the cached string if present based on a key (overload for Xid only).
     * @param db The DB ID.
     * @param obj_id The object ID.
     * @param xid The XID.
     * @return The cached string.
     */
    std::optional<std::string> find(DbId db, ObjId obj_id, Xid xid)
    {
        return find(db, obj_id, XidLsn{xid, 0});
    }

    /**
     * This will update committed XID and set _xid_committed_time to now().
     * @param db The DB ID.
     * @param xid The XID.
     * @param has_schema_changes Indicates if the XID includes schema changes. 
     */
    void update_committed_xid(DbId db, Xid xid, bool has_schema_changes);

    /** 
     * Cleanup committed history of schema changes
     */ 
    void cleanup_xid_history();

    /** 
     * Delete XID history for a DB
     */
    void delete_xid_history(DbId db);

    /**
     * This must be called periodically (see XID_KEEP_ALIVE_PERIOD).
     * to keep the committed XID as being up to date.
     */
    void keep_alive();

    /**
     * Return true if the cache is alive (i.e. keep_alive() has been called
     * recently enough).
     */
    bool is_alive();

    /**
     * Return the last committed Xid if it is up to date or false otherwise.
     * The function will check that now() - _xid_commit_time < XID_KEEP_ALIVE_PERIOD.
     * @param db The DB ID.
     * @param schema_xid The last known schema XID.
     */
    std::optional<Xid> get_committed_xid(DbId db, Xid schema_xid);


    /**
     * Return all objects that are tracked by the cache.
     * The least used objects will be at the front.
     */
    std::vector<ObjId> get_db_objects(DbId db, bool exclude_dropped=true)
    {
        return _msg_cache.get_db_objects(db, exclude_dropped);
    }

    /**
     * This will mark the system resources associated with the cache 
     * as deleted.  The existing cache clients will continue to work
     * using the ghosted cache. Creating another ShmCache with the 
     * same name will create an new empty cache.
     */
    static void remove(const std::string& name);

private:
    void _init();

    // if free memory size goes below the limit, we start evictions
    // until we reach the watermark
    constexpr static double FREE_MEM_LIMIT = 0.3; // 30%
    constexpr static double FREE_MEM_WATERMARK = 0.5; // 50%

    /**
     * This will verify that the cache has free space as defined by
     * FREE_MEM_LIMIT and FREE_MEM_WATERMARK parameters.
     * if the free memory size goes below the limit, we start evictions
     * until we reach the watermark
     */
    void check_free_space_locked();

    using String = GenericCache::Value;
    using Key = GenericCache::Key;
    using Message = GenericCache::Message; 
    using Messages = GenericCache::Messages;
    using LruKey = GenericCache::LruKey; 
    using Lru = GenericCache::Lru; 
    using Mutex = GenericCache::Mutex; 
    using CacheContainer = GenericCache::Cache;

    std::string _name;
    bool _created;
    ipc::managed_shared_memory _shm;
    Mutex _mutex;
    Messages::allocator_type _messages_alloc;
    String::allocator_type _string_alloc;
    GenericCache _msg_cache;

    // _cache and _lru are named objects in the shared memory.
    // They are allocated or opened by the ipc allocators.
    // The objects are deleted when the shared memory is deleted.
    CacheContainer* _cache;
    Lru* _lru;

    // Xid updates
    using Time = std::chrono::time_point<std::chrono::high_resolution_clock>;
    using XidMap = ipc::map<DbId, Xid, std::less<DbId>, Alloc<std::pair<const DbId, Xid>>>;
    Time* _xid_commit_time;
    XidMap* _committed_xid_map;

    struct XidHistoryEntry {
        Xid schema_xid;
        Xid latest_real_commit_xid;
    };
    using XidHistory = ipc::vector<XidHistoryEntry, Alloc<XidHistoryEntry>>;
    using XidHistoryMap = ipc::map<DbId, XidHistory, std::less<DbId>, Alloc<std::pair<const DbId, XidHistory>>>;

    XidHistory::allocator_type _history_alloc;
    XidHistoryMap* _xid_history_map;
};

}  // namespace springtail::sys_tbl_mgr
