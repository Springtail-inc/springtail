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

#include "ThriftXidMgr.h"

#include <xid_mgr/xid_mgr_server.hh>
#include <xid_mgr/xid_mgr_service.hh>

namespace springtail {

    /* static initialization must happen outside of class */
    XidMgrServer* XidMgrServer::_instance {nullptr};

    XidMgrServer *
    XidMgrServer::_init()
    {
        _instance = new XidMgrServer();
        return _instance;
    }

    XidMgrServer::XidMgrServer()
    {
        nlohmann::json json = Properties::get(Properties::XID_MGR_CONFIG);
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
    XidMgrServer::startup()
    {
        // create a thread manager with right number of worker threads
        std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager =
          apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(_worker_thread_count);

        threadManager->threadFactory(std::make_shared<apache::thrift::concurrency::ThreadFactory>());
        threadManager->start();

        apache::thrift::server::TThreadPoolServer server(
            std::make_shared<thrift::ThriftXidMgrProcessorFactory>(std::make_shared<ThriftXidMgrCloneFactory>()),
            std::make_shared<apache::thrift::transport::TServerSocket>(_port),
            std::make_shared<apache::thrift::transport::TFramedTransportFactory>(),
            std::make_shared<apache::thrift::protocol::TCompactProtocolFactory>(),
            threadManager
        );

        server.serve();
    }

    void
    XidMgrServer::_shutdown()
    {
        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    std::pair<uint64_t, uint64_t>
    XidMgrServer::get_xid_range(uint64_t xid)
    {
        return {xid+1,xid+101};
    }

    void
    XidMgrServer::commit_xid(uint64_t xid)
    {
        return;
    }

    uint64_t
    XidMgrServer::get_committed_xid()
    {
        return 0;
    }

}