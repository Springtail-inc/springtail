#include <iostream>
#include <mutex>
#include <memory>

#include <nlohmann/json.hpp>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/server/TThreadPoolServer.h>

#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>

#include "ThriftWriteCache.h"

#include <write_cache/write_cache_server.hh>
#include <write_cache/write_cache_service.hh>

namespace springtail {

    /* static initialization must happen outside of class */
    WriteCacheServer* WriteCacheServer::_instance {nullptr};
    std::mutex WriteCacheServer::_instance_mutex;

    WriteCacheServer *
    WriteCacheServer::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new WriteCacheServer();
        }

        return _instance;
    }

    WriteCacheServer::WriteCacheServer() : _service(std::make_shared<WriteCacheService>())
    {
        nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;        
        
        if (!Json::get_to(json, "server", server_json)) {
            throw Error("Write cache server settings not found");
        }

        if (!Json::get_to<std::string>(server_json, "host", _server_host)) {
            throw Error("Write cache 'server.host' setting not found");
        }

        Json::get_to<int>(server_json, "port", _port, 55051);

        Json::get_to<int>(server_json, "worker_threads", _worker_thread_count, 8);
    }
  
    /** 
     * Startup thrift threaded server
     */
    void
    WriteCacheServer::startup()
    {
        // create a thread manager with right number of worker threads
        std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager =
          apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(_worker_thread_count);

        threadManager->threadFactory(std::make_shared<apache::thrift::concurrency::ThreadFactory>());
        threadManager->start();

        apache::thrift::server::TThreadPoolServer server(
            std::make_shared<ThriftWriteCacheProcessorFactory>(std::make_shared<ThriftWriteCacheCloneFactory>()),
            std::make_shared<apache::thrift::transport::TServerSocket>(_port),
            std::make_shared<apache::thrift::transport::TBufferedTransportFactory>(),
            std::make_shared<apache::thrift::protocol::TCompactProtocolFactory>(),
            threadManager
        );

        server.serve();
    }

    void
    WriteCacheServer::shutdown()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }  
}

int main (void)
{

}