#include <iostream>
#include <absl/log/check.h>
#include <bits/ranges_algo.h>
#include <sys_tbl_mgr/shm_cache.hh>

using namespace springtail::sys_tbl_mgr;

ShmCache::ShmCache(std::string name, size_t size)
    :_name{std::move(name)},
    _mutex{ipc::open_or_create, (_name + std::string(".mutex")).c_str()},
    _shm{ipc::open_or_create, _name.c_str(), size},
    _messages_alloc{_shm.get_segment_manager()},
    _string_alloc{_shm.get_segment_manager()}
{
    auto free_size = _shm.get_free_memory();
    CHECK(free_size <=  size);
    CHECK(_shm.check_sanity());

    _cache = _shm.find_or_construct<Cache>("cache")(
            Cache::allocator_type(_shm.get_segment_manager()));
    _lru = _shm.find_or_construct<Lru>("lru")(
            Lru::allocator_type(_shm.get_segment_manager()));
}

ShmCache::~ShmCache() 
{
    remove(_name);
}

void 
ShmCache::remove(const std::string& name)
{
    ipc::shared_memory_object::remove(name.c_str());
    ipc::named_sharable_mutex::remove((name + std::string(".mutex")).c_str());
}

void 
ShmCache::insert(DbId db, TabId tid, Xid xid, const std::string& msg)
{
    Key k{db, tid};

    Message item(_string_alloc);
    item.xid = xid;

    auto cmp = [](const auto& a, const auto& b) {return a.xid < b.xid;};

    ipc::scoped_lock<ipc::named_sharable_mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );

    CHECK(lock.owns());

    auto it = _cache->find(k);
    if (it != _cache->end() && std::ranges::binary_search(it->second, item, cmp)) {
        return;
    }

    check_free_space_locked();

    if (it == _cache->end()) {
        it = (*_cache).emplace(k, Messages(_messages_alloc)).first;
    }

    CHECK(it != _cache->end());

    item.msg = msg.c_str();

    while (true) {
        try {
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
            auto itt = std::ranges::lower_bound(it->second, item,
                    [](const auto& a, const auto& b) { return a.xid < b.xid; } );

            if (itt != it->second.end()) {
                it->second.erase(itt);
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
}

std::optional<std::string>
ShmCache::find(DbId db, TabId tid, Xid xid)
{
    ipc::sharable_lock<ipc::named_sharable_mutex> lock(_mutex,
            std::chrono::system_clock::now() + std::chrono::seconds(5)
            );
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

    LruKey lk{db, tid, xid};
    _lru->push_front(lk);

    return std::string(itt->msg.c_str(), itt->msg.size());
}

void
ShmCache::check_free_space_locked()
{
    auto free_size = _shm.get_free_memory();
    if (static_cast<double>(free_size) > static_cast<double>(_shm.get_size())*FREE_SIZE_LIMIT) {
        return;
    }

    while (true) {
        auto free_size = _shm.get_free_memory();
        if (static_cast<double>(free_size) > static_cast<double>(_shm.get_size())*FREE_SIZE_WATERMARK) {
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

