#include <nlohmann/json.hpp>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TCompactProtocol.h>

#include <common/properties.hh>
#include <common/logging.hh>
#include <common/json.hh>
#include <common/object_cache.hh>
#include <common/common.hh>

#include "ThriftWriteCache.h"

#include <write_cache/write_cache_client.hh>
#include <write_cache/write_cache_client_factory.hh>

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
        
        // fetch properties for the write cache client
        if (!Json::get_to(json, "client", client_json)) {
            throw Error("Write cache client settings not found");
        }

        if (!Json::get_to(json, "server", server_json)) {
            throw Error("Write cache server settings not found");
        }

        // init channel pool
        int max_connections;
        int port;
        std::string server;
        Json::get_to<int>(client_json, "connections", max_connections, 8);
        Json::get_to<int>(server_json, "port", port, 55051);        

        if (!Json::get_to<std::string>(client_json, "server", server)) {
            throw Error("Host not found in write_cache.server settings");
        }

        // construct the thrift client pool.  
        // First argument is a factory object that constructs a thrift clients
        // using the host and port from above
        _thrift_client_pool = std::make_shared<ObjectPool<thrift::ThriftWriteCacheClient>>(
            std::make_shared<ThriftObjectFactory>(server, port),
            max_connections/2,
            max_connections
        );
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

    // exposed client service interface below

    void
    WriteCacheClient::ping()
    {
        ThriftClient c = _get_client();
        thrift::Status result;

        c.client->ping(result);

        std::cout << "Ping got: " << result.message << std::endl;
        return;
    }

    void 
    WriteCacheClient::add_table_changes(uint64_t tid, std::vector<TableChange> changes)
    {      
        ThriftClient c = _get_client();
        thrift::Status result;

        std::vector<thrift::TableChange> request;

        c.client->add_table_changes(result, request);

        return;
    }

    void 
    WriteCacheClient::add_rows(uint64_t tid, uint64_t eid, RowOp op, std::vector<RowData> rows)
    {
        ThriftClient c = _get_client();
        thrift::Status result;

        thrift::AddRowRequest request;

        c.client->add_rows(result, request);

        return;
    }
      
    std::vector<WriteCacheClient::TableChange>
    WriteCacheClient::fetch_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {            
        ThriftClient c = _get_client();
        
        thrift::GetTableChangeRequest request;
        std::vector<thrift::TableChange> response;

        c.client->get_table_changes(response, request);

        std::vector<TableChange> changes;
        return changes;
    }                  

    std::vector<uint64_t>
    WriteCacheClient::list_tables(uint64_t start_xid, uint64_t end_xid, int count, uint64_t &cursor)
    {      
        ThriftClient c = _get_client();
        std::vector<uint64_t> tables;

        thrift::ListTablesRequest request;
        thrift::ListTablesResponse response;

        c.client->list_tables(response, request);

        return tables;

    }            

    std::vector<uint64_t>
    WriteCacheClient::list_extents(uint64_t tid, uint64_t start_xid, uint64_t end_xid, 
                                   int count, uint64_t &cursor)
    {      
        ThriftClient c = _get_client();
        std::vector<uint64_t> extents;
        
        thrift::ListExtentsRequest request;
        thrift::ListExtentsResponse response;

        c.client->list_extents(response, request);
        
        return extents;        
    }

    std::vector<WriteCacheClient::RowData>
    WriteCacheClient::fetch_rows(uint64_t tid, uint64_t eid, uint64_t start_xid, 
                                 uint64_t end_xid, int count, uint64_t &cursor)
    {
        ThriftClient c = _get_client();
        std::vector<RowData> rows;
        
        thrift::GetRowsRequest request;
        thrift::GetRowsResponse response;

        c.client->get_rows(response, request);

        return rows;
    }

    void 
    WriteCacheClient::evict_extent(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid)
    {   
        ThriftClient c = _get_client();
        thrift::Status result;

        thrift::EvictExtentRequest request;

        c.client->evict_extent(result, request);
    }
}

int main (void)
{
    springtail::springtail_init();

    springtail::WriteCacheClient *client = springtail::WriteCacheClient::get_instance();
    client->ping();

    return 0;
}