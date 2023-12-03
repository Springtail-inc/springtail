#include <common/properties.hh>
#include <common/logging.hh>
#include <common/redis.hh>

#include <write_cache/write_cache.hh>

namespace springtail {
    /* static initialization must happen outside of class */
    WriteCache* WriteCache::_instance {nullptr};
    std::mutex WriteCache::_instance_mutex;

    WriteCache *
    WriteCache::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new WriteCache();
        }

        return _instance;
    }

    WriteCache::WriteCache()
    {}

    void
    WriteCache::insert_row(uint64_t tid, uint64_t extid, uint64_t xid, 
                           const std::string &pkey, const std::string &data)
    {
        
    }

    void
    WriteCache::shutdown()
    {
        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }


}