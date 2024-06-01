#include <iostream>
#include <mutex>
#include <memory>

#include <nlohmann/json.hpp>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/server/TThreadPoolServer.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>

#include <thrift/write_cache/ThriftWriteCache.h>

#include <write_cache/write_cache_server.hh>
#include <write_cache/write_cache_service.hh>
#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail {

    /* static initialization must happen outside of class */
    WriteCacheServer* WriteCacheServer::_instance {nullptr};

    std::once_flag WriteCacheServer::_init_flag;

    WriteCacheServer *
    WriteCacheServer::_init()
    {
        _instance = new WriteCacheServer();
        return _instance;
    }

    WriteCacheServer::WriteCacheServer()
    {
        nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;

        if (!Json::get_to<nlohmann::json>(json, "server", server_json)) {
            throw Error("Write cache server settings not found");
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
            std::make_shared<thrift::write_cache::ThriftWriteCacheProcessorFactory>(std::make_shared<ThriftWriteCacheCloneFactory>()),
            std::make_shared<apache::thrift::transport::TServerSocket>(_port),
            std::make_shared<apache::thrift::transport::TFramedTransportFactory>(),
            std::make_shared<apache::thrift::protocol::TCompactProtocolFactory>(),
            threadManager
        );

        server.serve();
    }

    void
    WriteCacheServer::_shutdown()
    {
        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }
}
