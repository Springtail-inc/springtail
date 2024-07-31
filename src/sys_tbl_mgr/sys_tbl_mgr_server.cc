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

#include <thrift/sys_tbl_mgr/ThriftSysTblMgr.h>

#include <sys_tbl_mgr/sys_tbl_mgr_server.hh>
#include <sys_tbl_mgr/sys_tbl_mgr_service.hh>

namespace springtail {

    /* static initialization must happen outside of class */
    SysTblMgrServer* SysTblMgrServer::_instance {nullptr};

    std::once_flag SysTblMgrServer::_init_flag;
    std::once_flag SysTblMgrServer::_shutdown_flag;

    SysTblMgrServer *
    SysTblMgrServer::_init()
    {
        _instance = new SysTblMgrServer();
        return _instance;
    }

    SysTblMgrServer::SysTblMgrServer()
    {
        nlohmann::json json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;

        if (!Json::get_to<nlohmann::json>(json, "server", server_json)) {
            throw Error("SysTblMgr server settings not found");
        }

        Json::get_to<int>(server_json, "port", _port, 55053);
        Json::get_to<int>(server_json, "worker_threads", _worker_thread_count, 8);
    }

    /**
     * Startup thrift threaded server
     */
    void
    SysTblMgrServer::_startup()
    {
        // create a thread manager with right number of worker threads
        std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager =
          apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(_worker_thread_count);

        threadManager->threadFactory(std::make_shared<apache::thrift::concurrency::ThreadFactory>());
        threadManager->start();

        _server = std::make_shared<apache::thrift::server::TThreadPoolServer>(
            std::make_shared<thrift::sys_tbl_mgr::ThriftSysTblMgrProcessorFactory>(std::make_shared<ThriftSysTblMgrCloneFactory>()),
            std::make_shared<apache::thrift::transport::TServerSocket>(_port),
            std::make_shared<apache::thrift::transport::TFramedTransportFactory>(),
            std::make_shared<apache::thrift::protocol::TCompactProtocolFactory>(),
            threadManager
        );

        _server->serve();
    }

    void
    SysTblMgrServer::_shutdown()
    {
        if (_instance != nullptr) {
            _instance->_server->stop();
        }
    }
}
