#include <optional>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
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

using DbId = uint64_t;
using TabId = uint64_t;
using Xid = uint64_t;


/**
 * A generic interprocess cache. The cache is intended for caching serialized 
 * table metadata. The metadata strings are keyed by the database ID, 
 * table ID and XID.
 */
class ShmCache
{
public:
    /*
     * Create a new cache with the given name. If a cache with
     * the name already exists, it will throw.
     */

    ShmCache(std::string name, size_t size);
    /*
     * Open a cache with the give name. If the cache hasn't been created,
     * it will throw.
     */
    explicit ShmCache(std::string name);

    ~ShmCache();

    /**
     * Returns number of elements in the cache.
     */
    size_t size() const;

    /** 
     * Cache the string.
     * @return true if the element has been actually inserted 
     *         and false if it was alredy in the cache.
     */
    bool insert(DbId db, TabId tid, Xid xid, const std::string& msg);

    /** 
     * Get the cached string if present based on a key.
     */
    std::optional<std::string> find(DbId db, TabId tid, Xid xid);

    /**
     * This will mark the system resources associated with the cache 
     * as deleted.  The existing cache clients will continue to work
     * using the ghosted cache. Creating another ShmCache with the 
     * same name will create an new empty cache.
     */
    static void remove(const std::string& name);

private:
    // if free memory size goes below the limit, we start evictions
    // until we reach the watermark
    constexpr static double FREE_MEM_LIMIT = 0.3; // 30%
    constexpr static double FREE_MEM_WATERMARK = 0.5; // 50%

    /**
     * This will verify that the cache has free space as defined by
     * FREE_MEM_LIMIT and FREE_MEM_WATERMARK parameters.
    // if the free memory size goes below the limit, we start evictions
    // until we reach the watermark
     */
    void check_free_space_locked();

    void evict_locked();

    template <typename T>
    using Alloc = ipc::allocator<T, ipc::managed_shared_memory::segment_manager>;

    using String =  ipc::basic_string<char, std::char_traits<char>, Alloc<char>>;

    using Key = std::pair<DbId, TabId>;

    struct Message
    {
        explicit Message(const String::allocator_type& al) 
            :xid{0},
            msg{al}
        {}
        Xid xid;
        String msg;
    };

    struct LruKey
    {
        DbId db;
        TabId tid;
        Xid xid;
        bool operator==(const LruKey& rhs) const {
            return db == rhs.db && tid == rhs.tid && xid == rhs.xid;
        }
    };
    struct LruHashFunc
    {
        size_t operator()(const LruKey& k) const
        {
            using Tuple = std::tuple<DbId, TabId, Xid>;
            Tuple t{k.db, k.tid, k.xid};
            return boost::hash<Tuple>{}(t);
        }
    };

    using Lru = bmi::multi_index_container<
        LruKey,
        bmi::indexed_by<
            bmi::sequenced<> , // this will keep the insertion order
            bmi::hashed_unique<bmi::identity<LruKey>, LruHashFunc> //no duplicates
        >, 
        Alloc<LruKey> >;


    //ordered by Message::xid
    using Messages = ipc::vector<Message, Alloc<Message>>;
    using Cache = ipc::map<Key, Messages, std::less<Key>, Alloc<std::pair<const Key, Messages>>>;

    std::string _name;
    bool _created;
    mutable ipc::named_mutex _mutex;
    ipc::managed_shared_memory _shm;
    Messages::allocator_type _messages_alloc;
    String::allocator_type _string_alloc;
    Cache* _cache;
    Lru* _lru;
};

}  // namespace springtail::sys_tbl_mgr
