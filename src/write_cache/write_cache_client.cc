#include <nlohmann/json.hpp>

#include <common/properties.hh>
#include <common/logging.hh>
#include <common/json.hh>

#include <write_cache/write_cache_client.hh>

namespace springtail {
    /* static initialization must happen outside of class */
    WriteCacheClient* WriteCacheClient::_instance {nullptr};
    std::mutex WriteCacheClient::_instance_mutex;

    WriteCacheClient *
    WriteCacheClient::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new WriteCacheClient();
        }

        return _instance;
    }

    WriteCacheClient::WriteCacheClient()
    {
        nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;        
        
        if (!Json::get_to(json, "client", client_json)) {
            throw Error("Write cache client settings not found");
        }

        if (!Json::get_to(json, "server", server_json)) {
            throw Error("Write cache server settings not found");
        }

        // init channel pool
        int max_connections;
        int io_threads;
        std::string host;
        Json::get_to<int>(client_json, "connections", max_connections, 8);
        Json::get_to<int>(client_json, "io_threads", io_threads, 1);
        if (!Json::get_to<std::string>(server_json, "host", host)) {
            throw Error("Host not found in write_cache.server settings");
        }

        // create zmq context and connection pool
        _context = std::make_shared<zmq::context_t>(io_threads);
        _socket_pool = std::make_shared<ZmqSocketPool>(_context, host, max_connections/2, max_connections);
    }

    void
    WriteCacheClient::shutdown()
    {
         std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    void 
    WriteCacheClient::insert_table_change(uint64_t tid, uint64_t xid, uint64_t LSN, WriteCache::TableOp op)
    {      
      
    }

    void 
    WriteCacheClient::insert_row(uint64_t tid, uint64_t eid, 
                                 uint64_t xid, uint64_t LSN,
                                 const std::string_view &pkey, 
                                 const std::string_view &data)
    {

    }

    void 
    WriteCacheClient::update_row(uint64_t tid, uint64_t old_eid, uint64_t new_eid,
                                 uint64_t xid, uint64_t LSN,
                                 const std::string_view &old_pkey, 
                                 const std::string_view &new_pkey, 
                                 const std::string_view &data)
    {

    }

    void
    WriteCacheClient::delete_row(uint64_t tid, uint64_t eid, 
                                 uint64_t xid, uint64_t LSN,
                                 const std::string_view &pkey)
    {
    }

    void 
    WriteCacheClient::clean_extent(uint64_t tid, uint64_t eid, uint64_t xid)
    {      
    }

    void 
    WriteCacheClient::evict(uint64_t xid)
    {      
    }            

    std::vector<std::shared_ptr<WriteCache::TableChange>> 
    WriteCacheClient::fetch_table_changes(uint64_t tid, uint64_t xid)
    {            
        std::vector<std::shared_ptr<WriteCache::TableChange>> changes;
        return changes;
    }                  

    std::vector<uint64_t>
    WriteCacheClient::fetch_tables(uint64_t xid, int count, uint64_t offset)
    {      
        std::vector<uint64_t> tables;
        return tables;

    }            

    std::vector<uint64_t>
    WriteCacheClient::fetch_extents(uint64_t tid, uint64_t xid, int count, uint64_t offset)
    {      
        std::vector<uint64_t> extents;
        return extents;        
    }
    
    std::vector<uint64_t>
    WriteCacheClient::fetch_rows(uint64_t tid, uint64_t eid, uint64_t xid, int count, uint64_t offset)
    {
        std::vector<uint64_t> rows;
        return rows;
    }

    std::shared_ptr<WriteCache::RowData>
    WriteCacheClient::fetch_row(uint64_t tid, uint64_t eid, uint64_t rid, uint64_t xid)
    {
        return std::make_shared<WriteCache::RowData>();            
    }


}

int main (void)
{

}