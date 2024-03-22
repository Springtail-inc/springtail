#include <iostream>
#include <mutex>
#include <shared_mutex>
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

#include <thrift/xid_mgr/ThriftXidMgr.h>

#include <xid_mgr/xid_mgr_server.hh>
#include <xid_mgr/xid_mgr_service.hh>

namespace springtail {

    /* static initialization must happen outside of class */
    XidMgrServer* XidMgrServer::_instance {nullptr};

    std::once_flag XidMgrServer::_init_flag;

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

        std::string base_path;
        Json::get_to<std::string>(json, "base_path", base_path, "/xid_mgr");
        _base_path = std::filesystem::path(base_path);
        if (!std::filesystem::exists(_base_path)) {
            std::filesystem::create_directories(_base_path);
        }

        _fd = open((_base_path / std::filesystem::path(XID_MGR_COMMIT_FILE)).c_str(), O_RDWR | O_CREAT, 0644);
        if (_fd < 0) {
            throw Error("Failed to open xid_mgr file");
        }
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

    void
    XidMgrServer::_write_committed_xid(uint64_t xid)
    {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        if (xid <= _committed_xid) {
            return;
        }
        _committed_xid = xid;
        lseek(_fd, 0, SEEK_SET);
        write(_fd, &xid, sizeof(xid));
    }

    uint64_t
    XidMgrServer::_read_committed_xid()
    {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        lseek(_fd, 0, SEEK_SET);
        int res = read(_fd, &_committed_xid, sizeof(_committed_xid));
        if (res == 0) {
            _committed_xid = 0;
        }
        return _committed_xid;
    }

    void
    XidMgrServer::commit_xid(uint64_t xid)
    {
        _write_committed_xid(xid);
        return;
    }

    uint64_t
    XidMgrServer::get_committed_xid()
    {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        if (_committed_xid == -1) {
            lock.unlock();
            return _read_committed_xid();
        }
        return _committed_xid;
    }

}