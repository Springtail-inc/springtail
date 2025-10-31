#include <absl/log/check.h>
#include <bits/ranges_algo.h>
#include <sys_tbl_mgr/shm_cache.hh>
#include <common/logging.hh>

using namespace springtail::sys_tbl_mgr;

ShmCache::ShmCache(std::string name, size_t size)
    :_name{std::move(name)},
    _created{true},
    _shm{ipc::create_only, _name.c_str(), size, nullptr,
        []{ipc::permissions  p; p.set_unrestricted(); return p;}()},
    _mutex{ipc::create_only, (_name + std::string(".mutex")).c_str(), []{ipc::permissions  p; p.set_unrestricted(); return p;}() },
    _messages_alloc{_shm.get_segment_manager()},
    _string_alloc{_shm.get_segment_manager()},
    _msg_cache(_mutex, _messages_alloc, _string_alloc),
    _history_alloc{_shm.get_segment_manager()}
{
    LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "ShmCache open: {} - {}", _name, size);
    auto free_size = _shm.get_free_memory();
    CHECK(free_size <=  size);
    _init();
}

ShmCache::ShmCache(std::string name)
    :_name{std::move(name)},
    _created{false},
    _shm{ipc::open_only, _name.c_str()},
    _mutex{ipc::open_only, (_name + std::string(".mutex")).c_str()},
    _messages_alloc{_shm.get_segment_manager()},
    _string_alloc{_shm.get_segment_manager()},
    _msg_cache(_mutex, _messages_alloc, _string_alloc),
    _history_alloc{_shm.get_segment_manager()}
{
    LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "ShmCache open: {}", _name);
    _init();
}

ShmCache::~ShmCache()
{
    LOG_DEBUG(LOG_CACHE, LOG_LEVEL_DEBUG1, "ShmCache deleted: {} - {}", _name, _created);
    if (_created) {
        try {
            remove(_name);
        } catch (...) {
            LOG_ERROR("Failed to remove shared memory cache '{}'", _name);
        }
    }
}

void
ShmCache::_init()
{
    CHECK(_shm.check_sanity());

    _msg_cache.set_containers(
            _shm.find_or_construct<CacheContainer>("cache")(
                CacheContainer::allocator_type(_shm.get_segment_manager())),
            _shm.find_or_construct<Lru>("lru")(
                    Lru::allocator_type(_shm.get_segment_manager()))
            );

    _xid_commit_time = _shm.find_or_construct<Time>("commit_time")();
    CHECK(_xid_commit_time);

    _committed_xid_map = _shm.find_or_construct<XidMap>("committed_xid")(
            XidMap::allocator_type(_shm.get_segment_manager()));
    CHECK(_committed_xid_map);

    _xid_history_map = _shm.find_or_construct<XidHistoryMap>("xid_history")(
            XidHistoryMap::allocator_type(_shm.get_segment_manager()));
    CHECK(_xid_history_map);
}


void
ShmCache::remove(const std::string& name)
{
    if (!ipc::shared_memory_object::remove(name.c_str())) {
        LOG_ERROR("Failed to remove shared memory '{}'", name);
    }
    Mutex::remove((name + std::string(".mutex")).c_str());
}

void
ShmCache::update_committed_xid(DbId db, Xid xid, bool has_schema_changes)
{
    ipc::scoped_lock<Mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );
    CHECK(lock.owns());
    check_free_space_locked();

    *_xid_commit_time = std::chrono::high_resolution_clock::now();
    if (has_schema_changes) {
        Xid last_xid = 0;
        if (_committed_xid_map->find(db) != _committed_xid_map->end()) {
            last_xid = (*_committed_xid_map)[db];
        }
        // put the schema change xid and last committed xid into history
        auto it = _xid_history_map->find(db);
        if (it == _xid_history_map->end()) {
            it = _xid_history_map->emplace(xid, XidHistory{_history_alloc}).first;
        }
        it->second.emplace_back(xid, last_xid);
    }
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

bool
ShmCache::is_alive()
{
    ipc::sharable_lock<Mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );
    CHECK(lock.owns());
    if ( (std::chrono::high_resolution_clock::now() -  *_xid_commit_time) > XID_KEEP_ALIVE_PERIOD) {
        return false;
    }
    return true;
}

std::optional<Xid>
ShmCache::get_committed_xid(DbId db, Xid schema_xid)
{
    ipc::sharable_lock<Mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );
    CHECK(lock.owns());

    if (*_xid_commit_time == Time()) {
        return {};
    }

    if ( (std::chrono::high_resolution_clock::now() -  *_xid_commit_time) > XID_KEEP_ALIVE_PERIOD) {
        return {};
    }

    auto it = _committed_xid_map->find(db);
    if (it == _committed_xid_map->end()) {
        return {};
    }
    return it->second;
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

        _msg_cache.evict();
    }
}

