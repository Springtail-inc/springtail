#include <iostream>
#include <mutex>
#include <memory>

#include <nlohmann/json.hpp>

#include <thrift/transport/TBufferTransports.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TNonblockingServerSocket.h>

// #include <thrift/transport/TSocket.h>
// #include <thrift/transport/TBufferTransports.h>
// #include <thrift/protocol/TCompactProtocol.h>
// #include <thrift/server/TThreadPoolServer.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>

#include <thrift/sys_tbl_mgr/Service.h>

#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/service.hh>

namespace at = apache::thrift;

namespace springtail::sys_tbl_mgr {

    Server::Server()
    {
        nlohmann::json json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;

        if (!Json::get_to<nlohmann::json>(json, "server", server_json)) {
            throw Error("SysTblMgr server settings not found");
        }

        _port = Json::get_or<int>(server_json, "port", 55053);
        _worker_thread_count = Json::get_or<int>(server_json, "worker_threads", 8);
    }

    /**
     * Startup thrift threaded server
     */
    void
    Server::_startup()
    {
        SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "Server: creating thread manager with {} threads", _worker_thread_count);
        _thread_manager = apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(_worker_thread_count);

        // use thread factory with attached threads
        _thread_manager->threadFactory(std::make_shared<apache::thrift::concurrency::ThreadFactory>());
        _thread_manager->start();

        std::shared_ptr<apache::thrift::transport::TNonblockingServerSocket> server_socket = std::make_shared<apache::thrift::transport::TNonblockingServerSocket>(_port);

        _server = std::make_shared<apache::thrift::server::TNonblockingServer>(
            std::make_shared<ServiceProcessorFactory>(std::make_shared<ServiceCloneFactory>()),
            std::make_shared<apache::thrift::protocol::TBinaryProtocolFactory>(),
            server_socket,
            _thread_manager
        );

        _server->serve();
    }

    void
    Server::_internal_shutdown()
    {
        stop();
    }
}
