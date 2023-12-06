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
    WriteCache::shutdown()
    {
        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    void 
    WriteCache::start_gc(uint64_t xid)
    {
    }
    
    void 
    WriteCache::complete_gc(uint64_t xid)
    {
    }

    void 
    WriteCache::table_change(uint64_t tid, uint64_t xid, uint64_t LSN, TableOp op)
    {
    }

    std::vector<WriteCache::TableChange> 
    WriteCache::fetch_table_changes(uint64_t tid, uint64_t xid)
    {
        return std::vector<TableChange>();
    }

    void 
    WriteCache::insert_row(uint64_t tid, uint64_t eid, 
                           uint64_t xid, uint64_t LSN,
                           const std::string_view &pkey, 
                           const std::string_view &data)
    {
    }

    void 
    WriteCache::update_row(uint64_t tid, uint64_t old_eid, uint64_t new_eid,
                           uint64_t xid, uint64_t LSN,
                           const std::string_view &old_pkey, 
                           const std::string_view &new_pkey, 
                           const std::string_view &data)
    {
    }
    
    void
    WriteCache::delete_row(uint64_t tid, uint64_t eid, 
                           uint64_t xid, uint64_t LSN,
                           std::string_view &pkey)
    {
    }
    
    std::vector<uint64_t>
    WriteCache::fetch_tables(uint64_t xid, int count, int offset)
    {
        return std::vector<uint64_t>();
    }

    std::vector<uint64_t>
    WriteCache::fetch_extents(uint64_t tid, uint64_t xid, int count, int offset)
    {
        return std::vector<uint64_t>();
    }
    
    std::vector<uint64_t>
    WriteCache::fetch_rows(uint64_t tid, uint64_t eid, uint64_t xid, int count, int offset)
    {
        return std::vector<uint64_t>();
    }
    
    std::shared_ptr<WriteCache::RowData>
    WriteCache::fetch_row(uint64_t tid, uint64_t uid, uint64_t rid)
    {
        return std::make_shared<RowData>();
    }

    void 
    WriteCache::clean_extent(uint64_t tid, uint64_t eid, uint64_t xid)
    {
    }

    void 
    WriteCache::evict(uint64_t xid)
    {
    }
}

