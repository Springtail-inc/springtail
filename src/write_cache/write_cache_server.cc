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

    WriteCacheServer::WriteCacheServer()
    {
        nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;

        if (!Json::get_to<nlohmann::json>(json, "server", server_json)) {
            throw Error("Write cache server settings not found");
        }

        _port = Json::get_or<int>(server_json, "port", 55051);
        _worker_thread_count = Json::get_or<int>(server_json, "worker_threads", 8);
    }

    /**
     * Startup thrift threaded server
     */
    void
    WriteCacheServer::_startup()
    {
        SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "WriteCacheServer: creating thread manager with {} threads", _worker_thread_count);

        // initialize the index
        _indexes = {};

        // create a thread manager with right number of worker threads
        _thread_manager = apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(_worker_thread_count);

        // use thread factory with attached threads
        _thread_manager->threadFactory(std::make_shared<apache::thrift::concurrency::ThreadFactory>());
        _thread_manager->start();

        std::shared_ptr<apache::thrift::transport::TServerSocket> server_socket = std::make_shared<apache::thrift::transport::TServerSocket>(_port);

        _server = std::make_shared<apache::thrift::server::TThreadPoolServer>(
            std::make_shared<thrift::write_cache::ThriftWriteCacheProcessorFactory>(std::make_shared<ThriftWriteCacheCloneFactory>()),
            server_socket,
            std::make_shared<apache::thrift::transport::TFramedTransportFactory>(),
            std::make_shared<apache::thrift::protocol::TCompactProtocolFactory>(),
            _thread_manager
        );

        _server->serve();
    }

    void
    WriteCacheServer::_internal_shutdown()
    {
        stop();
    }
}
