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

#include <thrift/sys_tbl_mgr/Service.h>

#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/service.hh>

namespace at = apache::thrift;

namespace springtail::sys_tbl_mgr {

    /* static initialization must happen outside of class */
    Server* Server::_instance {nullptr};

    std::once_flag Server::_init_flag;
    std::once_flag Server::_shutdown_flag;

    Server *
    Server::_init()
    {
        _instance = new Server();
        return _instance;
    }

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
        // create a thread manager with right number of worker threads
        std::shared_ptr<at::concurrency::ThreadManager> threadManager =
            at::concurrency::ThreadManager::newSimpleThreadManager(_worker_thread_count);

        threadManager->threadFactory(std::make_shared<at::concurrency::ThreadFactory>());
        threadManager->start();

        _server = std::make_shared<at::server::TThreadPoolServer>(
            std::make_shared<ServiceProcessorFactory>(std::make_shared<ServiceCloneFactory>()),
            std::make_shared<at::transport::TServerSocket>(_port),
            std::make_shared<at::transport::TFramedTransportFactory>(),
            std::make_shared<at::protocol::TCompactProtocolFactory>(),
            threadManager
        );

        _server->serve();
    }

    void
    Server::_shutdown()
    {
        if (_instance != nullptr) {
            _instance->_server->stop();
        }
    }
}
