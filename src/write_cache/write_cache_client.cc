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


namespace springtail {

    /**
     * @brief Object pool factory for thrift cache client objects
     */
    class ThriftObjectFactory : public ObjectPoolFactory<thrift::ThriftWriteCacheClient> 
    {
    public:
        ThriftObjectFactory(const std::string &server, int port) : _server(server), _port(port) {}

        /**
         * @brief Allocate a new client; transport is not connected
         * @return std::shared_ptr<thrift::ThriftWriteCacheClient> 
         */
        std::shared_ptr<thrift::ThriftWriteCacheClient> allocate() override
        {
            std::shared_ptr<apache::thrift::transport::TTransport> socket = 
                std::make_shared<apache::thrift::transport::TSocket>(_server, _port);
            std::shared_ptr<apache::thrift::transport::TTransport> transport = 
                std::make_shared<apache::thrift::transport::TFramedTransport>(socket);
            std::shared_ptr<apache::thrift::protocol::TProtocol> protocol = 
                std::make_shared<apache::thrift::protocol::TCompactProtocol>(transport);
            std::shared_ptr<thrift::ThriftWriteCacheClient> client = 
                std::make_shared<thrift::ThriftWriteCacheClient>(protocol);
            return client;
        }

        /**
         * @brief The get callback from the object pool.  Check that transport is connected
         *        before returning.
         * @param client 
         */
        void get_cb(std::shared_ptr<thrift::ThriftWriteCacheClient> client) override
        {
            // validate that the transport is connected
            std::shared_ptr<apache::thrift::protocol::TProtocol> proto = client->getOutputProtocol();
            if (proto->getTransport()->isOpen()) {
                return;
            }
            proto->getTransport()->open();
        }

        /**
         * @brief Deallocation callback; close transport
         * @param client 
         */
        void deallocate(std::shared_ptr<thrift::ThriftWriteCacheClient> client) override
        {
            std::shared_ptr<apache::thrift::protocol::TProtocol> proto = client->getOutputProtocol();
            if (!proto->getTransport()->isOpen()) {
                return;
            }
            proto->getTransport()->close();
        }

    private:
        std::string _server;
        int _port;
    };

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
        int port;
        std::string server;
        Json::get_to<int>(client_json, "connections", max_connections, 8);
        Json::get_to<int>(server_json, "port", port, 55051);        

        if (!Json::get_to<std::string>(client_json, "server", server)) {
            throw Error("Host not found in write_cache.server settings");
        }

        // construct the thrift client pool.  
        // First argument is a lambda that constructs a new thrift client; capturing the host and port from above
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

    // thrift client service interface below

    void
    WriteCacheClient::ping()
    {
        ThriftClient c = _get_client();
        thrift::Status result;

        c.client->ping(result);

        std::cout << "Ping got: " << result.message << std::endl;
    }

    void 
    WriteCacheClient::insert_table_change(uint64_t tid, uint64_t xid, uint64_t LSN, WriteCache::TableOp op)
    {      
      
    }

    void 
    WriteCacheClient::insert_row(uint64_t tid, uint64_t eid, 
                                 uint64_t xid, uint64_t LSN,
                                 const std::string_view &pkey_data, 
                                 const std::string_view &row_data)
    {

        return;
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
    springtail::springtail_init();

    springtail::WriteCacheClient *client = springtail::WriteCacheClient::get_instance();
    client->ping();

    return 0;
}