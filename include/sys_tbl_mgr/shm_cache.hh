#pragma once

#include <chrono>
#include <optional>
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

namespace springtail::sys_tbl_mgr {

namespace ipc = boost::interprocess;
namespace bmi = boost::multi_index;

// global cache names
static constexpr char SHM_CACHE_ROOTS[] = "springtail.roots";

using DbId = uint64_t;
using TableId = uint64_t;
using Xid = uint64_t;


/**
 * A generic interprocess cache. The cache is intended for caching serialized 
 * table metadata. The metadata strings are keyed by the database ID, 
 * table ID and XID.
 */
class ShmCache
{
public:

    /**
     * In order for get_committed_xid() to return a valid XID, update_committed_xid() must
     * be called at least once every XID_KEEP_ALIVE_PERIOD.
     */
    static constexpr std::chrono::duration XID_KEEP_ALIVE_PERIOD = std::chrono::milliseconds(6);

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
    size_t size() const;

    /** 
     * Cache the string message.
     * @param db The DB ID.
     * @param tid The table ID.
     * @param xid The XID.
     * @param msg The message to cache. Usually it's a serialized proto message.
     * @return true if the element has been actually inserted 
     *         and false if it was already in the cache.
     */
    bool insert(DbId db, TableId tid, Xid xid, const std::string& msg);

    /** 
     * Get the cached string if present based on a key.
     * @param db The DB ID.
     * @param tid The table ID.
     * @param xid The XID.
     * @return The cached string.
     */
    std::optional<std::string> find(DbId db, TableId tid, Xid xid);

    /**
     * This will mark the system resources associated with the cache 
     * as deleted.  The existing cache clients will continue to work
     * using the ghosted cache. Creating another ShmCache with the 
     * same name will create an new empty cache.
     */
    static void remove(const std::string& name);

    /**
     * This will update committed XID and set _xid_committed_time to now().
     * @param db The DB ID.
     * @param xid The XID.
     */
    void update_committed_xid(DbId db, Xid xid);

    /**
     * This must be called periodically (see XID_KEEP_ALIVE_PERIOD).
     * to keep the committed XID as being up to date.
     */
    void keep_alive();

    /**
     * Return the last committed Xid if it is up to date or false otherwise.
     * The function will check that now() - _xid_commit_time < XID_KEEP_ALIVE_PERIOD.
     */
    std::optional<Xid> get_committed_xid(DbId db);

    bool is_alive();


    /**
     * Return all tables that are tracked by the cache.
     */
    std::vector<TableId> get_db_tables(DbId db);

private:
    void _init();

    // if free memory size goes below the limit, we start evictions
    // until we reach the watermark
    constexpr static double FREE_MEM_LIMIT = 0.3; // 30%
    constexpr static double FREE_MEM_WATERMARK = 0.5; // 50%
                                                      //

    /**
     * This will verify that the cache has free space as defined by
     * FREE_MEM_LIMIT and FREE_MEM_WATERMARK parameters.
     * if the free memory size goes below the limit, we start evictions
     * until we reach the watermark
     */
    void check_free_space_locked();

    void evict_locked();

    template <typename T>
    using Alloc = ipc::allocator<T, ipc::managed_shared_memory::segment_manager>;

    using String =  ipc::vector<char, Alloc<char>>;

    using Key = std::pair<DbId, TableId>;

    struct Message
    {
        explicit Message(const String::allocator_type& al) 
            :msg{al}
        {}
        Xid xid = 0;
        String msg;
    };

    struct LruKey
    {
        DbId db;
        TableId tid;
        Xid xid;
        bool operator==(const LruKey& rhs) const = default;
    };
    struct LruHashFunc
    {
        size_t operator()(const LruKey& k) const
        {
            using Tuple = std::tuple<DbId, TableId, Xid>;
            Tuple t{k.db, k.tid, k.xid};
            return boost::hash<Tuple>{}(t);
        }
    };

    using Lru = bmi::multi_index_container<
        LruKey,
        bmi::indexed_by<
            bmi::sequenced<>, // this will keep the insertion order
            bmi::hashed_unique<bmi::identity<LruKey>, LruHashFunc> //no duplicates
        >, 
        Alloc<LruKey> >;


    //ordered by Message::xid
    using Messages = ipc::vector<Message, Alloc<Message>>;
    using Cache = ipc::map<Key, Messages, std::less<Key>, Alloc<std::pair<const Key, Messages>>>;
    using Mutex = ipc::named_sharable_mutex;
    

    std::string _name;
    bool _created;
    mutable Mutex _mutex;
    ipc::managed_shared_memory _shm;
    Messages::allocator_type _messages_alloc;
    String::allocator_type _string_alloc;

    // _cache and _lru are named objects in the shared memory.
    // They are allocated or opened by the ipc allocators.
    // The objects are deleted when the shared memory is deleted.
    Cache* _cache;
    Lru* _lru;

    // Xid updates
    using Time = std::chrono::time_point<std::chrono::high_resolution_clock>;
    using XidMap = ipc::map<DbId, Xid, std::less<DbId>, Alloc<std::pair<const DbId, Xid>>>;

    Time* _xid_commit_time;
    XidMap* _committed_xid_map;
};

}  // namespace springtail::sys_tbl_mgr
