#include <absl/log/check.h>
#include <bits/ranges_algo.h>
#include <chrono>
#include <sys_tbl_mgr/shm_cache.hh>

using namespace springtail::sys_tbl_mgr;

ShmCache::ShmCache(std::string name, size_t size)
    :_name{std::move(name)},
    _created{true},
    _mutex{ipc::create_only, (_name + std::string(".mutex")).c_str()},
    _shm{ipc::create_only, _name.c_str(), size},
    _messages_alloc{_shm.get_segment_manager()},
    _string_alloc{_shm.get_segment_manager()}
{
    auto free_size = _shm.get_free_memory();
    CHECK(free_size <=  size);
    _init();
}

ShmCache::ShmCache(std::string name)
    :_name{std::move(name)},
    _created{false},
    _mutex{ipc::open_only, (_name + std::string(".mutex")).c_str()},
    _shm{ipc::open_only, _name.c_str()},
    _messages_alloc{_shm.get_segment_manager()},
    _string_alloc{_shm.get_segment_manager()}
{
    _init();
}

ShmCache::~ShmCache() 
{
    if (_created) {
        remove(_name);
    }
}

void 
ShmCache::_init() 
{
    CHECK(_shm.check_sanity());

    _cache = _shm.find_or_construct<Cache>("cache")(
            Cache::allocator_type(_shm.get_segment_manager()));
    CHECK(_cache);
    _lru = _shm.find_or_construct<Lru>("lru")(
            Lru::allocator_type(_shm.get_segment_manager()));
    CHECK(_lru);

    _xid_commit_time = _shm.find_or_construct<Time>("commit_time")();
    CHECK(_xid_commit_time);

    _committed_xid_map = _shm.find_or_construct<XidMap>("committed_xid")(
            XidMap::allocator_type(_shm.get_segment_manager()));

    CHECK(_committed_xid_map);
}


void 
ShmCache::remove(const std::string& name)
{
    ipc::shared_memory_object::remove(name.c_str());
    Mutex::remove((name + std::string(".mutex")).c_str());
}
    
void 
ShmCache::update_committed_xid(DbId db, Xid xid) 
{
    ipc::scoped_lock<Mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );
    CHECK(lock.owns());

    *_xid_commit_time = std::chrono::high_resolution_clock::now();
    (*_committed_xid_map)[db] = xid;
}

void 
ShmCache::keep_alive() 
{
    ipc::scoped_lock<Mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );
    CHECK(lock.owns());

    *_xid_commit_time = std::chrono::high_resolution_clock::now();
}

std::optional<Xid> 
ShmCache::get_committed_xid(DbId db)
{
    ipc::sharable_lock<Mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );
    CHECK(lock.owns());

    if (*_xid_commit_time == Time()) {
        return {};
    }

    if ( (std::chrono::high_resolution_clock::now() -  *_xid_commit_time) < XID_KEEP_ALIVE_PERIOD) {
        return {};
    }

    auto it = _committed_xid_map->find(db);
    if (it == _committed_xid_map->end()) {
        return {};
    }
    return it->second;
}

size_t
ShmCache::size() const
{
    ipc::sharable_lock<Mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );

    CHECK(lock.owns());

    return _lru->size();
}

std::vector<TableId> 
ShmCache::get_db_tables(DbId db)
{
    std::vector<TableId> r;

    ipc::sharable_lock<Mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );

    CHECK(lock.owns());

    auto& seq_idx = _lru->get<0>();
    for (auto const& v: seq_idx) {
        if (v.db == db ) {
            r.push_back(v.tid);
        }
    }

    return r;
}

bool 
ShmCache::insert(DbId db, TableId tid, Xid xid, const std::string& msg)
{
    Key k{db, tid};

    Message item(_string_alloc);
    item.xid = xid;

    auto cmp = [](const auto& a, const auto& b) {return a.xid < b.xid;};

    ipc::scoped_lock<Mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );

    CHECK(lock.owns());

    auto it = _cache->find(k);
    if (it != _cache->end() && std::ranges::binary_search(it->second, item, cmp)) {
        return false;
    }

    bool key_exists = it != _cache->end();

    check_free_space_locked();

    while (true) {
        try {
            if (!key_exists) {
                it = (*_cache).emplace(k, Messages(_messages_alloc)).first;
                key_exists = true;
            }

            CHECK(it != _cache->end());

            item.msg = msg.c_str();

            if (!it->second.empty() && (--it->second.end())->xid < xid) {
                it->second.push_back(item);
            } else {
                it->second.push_back(item);
                std::ranges::sort(it->second, cmp);
            }
            LruKey lk{db, tid, xid};
            _lru->push_front(lk);
            break;
        } catch (const boost::interprocess::bad_alloc&) {

            // restore invariants
            if (key_exists) {
                auto itt = std::ranges::lower_bound(it->second, item,
                        [](const auto& a, const auto& b) { return a.xid < b.xid; } );
                if (itt != it->second.end()) {
                    it->second.erase(itt);
                }
            }

            auto it_lru = _lru->get<1>().find(LruKey{db, tid, xid});
            if (it_lru != _lru->get<1>().end()) {
                _lru->get<1>().erase(it_lru);
            }

            // if it fails the memory is probably too small.
            CHECK(!_lru->empty());

            evict_locked();
        }
    }
    return true;
}

std::optional<std::string>
ShmCache::find(DbId db, TableId tid, Xid xid)
{
    std::string ret;
    LruKey lk{db, tid, xid};

    { //read-only portion
        ipc::sharable_lock<Mutex> lock(_mutex,
                std::chrono::system_clock::now() + std::chrono::seconds(5)
                );
        CHECK(lock.owns());

        auto it = _cache->find({db, tid});
        if (it == _cache->end()) {
            return {};
        }

        Message item(_string_alloc);
        item.xid = xid;

        auto itt = std::ranges::lower_bound(it->second, item,
                [](const auto& a, const auto& b) { return a.xid < b.xid; } );
        if (itt == it->second.end()) {
            return {};
        }

        if (itt->xid != xid) {
            return {};
        }
        ret = std::string(itt->msg.c_str(), itt->msg.size());

        //check if the item is near the top of LRU
        size_t top_cnt = static_cast<double>(_lru->size())*0.1; //in the top 10%
        //Note: if the number of items in LRU "too small" (<10 elements) then
        //top_cnt=0 and we'll move to the top. It should be fine actually.
        
        auto& seq_idx = _lru->get<0>();
        size_t i = 0;
        for (auto it=seq_idx.begin(); i != top_cnt && it != seq_idx.end(); ++it, ++i)
        {
            if (*it == lk) {
                return ret;
            }
        }
    }
    
    // this will move the element to the LRU front
    // preserving the insertion sequence.
    {
        ipc::scoped_lock<Mutex> lock(_mutex,
                std::chrono::system_clock::now() + std::chrono::seconds(5)
                );
        CHECK(lock.owns());

        auto it_lru = _lru->get<1>().find(lk);
        if (it_lru != _lru->get<1>().end()) {
            auto& seq_idx = _lru->get<0>();
            seq_idx.relocate(seq_idx.begin(), _lru->project<0>(it_lru));
        }
    }

    return ret;
}

void
ShmCache::check_free_space_locked()
{
    auto free_size = _shm.get_free_memory();
    if (static_cast<double>(free_size) > static_cast<double>(_shm.get_size())*FREE_MEM_LIMIT) {
        return;
    }

    while (true) {
        free_size = _shm.get_free_memory();
        if (static_cast<double>(free_size) > static_cast<double>(_shm.get_size())*FREE_MEM_WATERMARK) {
            break;
        }

        evict_locked();
    }
}

void 
ShmCache::evict_locked()
{
    auto key = _lru->back();
    auto it = _cache->find({key.db, key.tid});
    CHECK(it != _cache->end());

    Message msg(_string_alloc);
    msg.xid = key.xid;

    auto itt = std::ranges::lower_bound(it->second, msg,
            [](const auto& a, const auto& b) { return a.xid < b.xid; } );
    CHECK(itt != it->second.end());
    it->second.erase(itt);
    it->second.shrink_to_fit();

    if (it->second.empty()) {
        _cache->erase(it);
    }

    _lru->pop_back();
}

