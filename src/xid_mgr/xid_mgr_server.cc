#include <fcntl.h>
#include <sys/stat.h>

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
    std::once_flag XidMgrServer::_shutdown_flag;

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
            throw Error("Xid Manager configuration missing server section");
        }

        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrServer: config: {}", server_json.dump());

        Json::get_to<int>(server_json, "port", _port, 55051);
        Json::get_to<int>(server_json, "worker_threads", _worker_thread_count, 8);

        std::string base_path;
        Json::get_to<std::string>(server_json, "base_path", base_path);
        _base_path = Properties::make_absolute_path(base_path);

        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrServer: base_path: {}", _base_path.string());

        if (!std::filesystem::exists(_base_path)) {
            std::filesystem::create_directories(_base_path);
        }

        _fd = ::open((_base_path / std::filesystem::path(XID_MGR_COMMIT_FILE)).c_str(), O_RDWR | O_CREAT, 0644);
        if (_fd < 0) {
            throw Error("Failed to open xid_mgr file");
        }
    }

    /**
     * Startup thrift threaded server; called by the static startup().
     */
    void
    XidMgrServer::_startup()
    {
        // create a thread manager with right number of worker threads
        std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager =
          apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(_worker_thread_count);

        threadManager->threadFactory(std::make_shared<apache::thrift::concurrency::ThreadFactory>());
        threadManager->start();

        _server = std::make_shared<apache::thrift::server::TThreadPoolServer>(
            std::make_shared<thrift::xid_mgr::ThriftXidMgrProcessorFactory>(std::make_shared<ThriftXidMgrCloneFactory>()),
            std::make_shared<apache::thrift::transport::TServerSocket>(_port),
            std::make_shared<apache::thrift::transport::TFramedTransportFactory>(),
            std::make_shared<apache::thrift::protocol::TCompactProtocolFactory>(),
            threadManager
        );

        // read the current XID from disk before we start serving
        _read_committed_xid();

        _server->serve();
    }

    void
    XidMgrServer::_shutdown()
    {
        if (_instance != nullptr) {
            _instance->_server->stop();
        }
    }

    void
    XidMgrServer::_write_committed_xid(uint64_t xid)
    {
        if (xid <= _committed_xid) {
            return;
        }
        _committed_xid = xid;
        lseek(_fd, 0, SEEK_SET);
        write(_fd, &xid, sizeof(xid));
        fsync(_fd);

        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrServer: write committed xid: {}", xid);
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
        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrServer: read committed xid: {}", _committed_xid);
        return _committed_xid;
    }

    void
    XidMgrServer::commit_xid(uint64_t xid, bool has_schema_changes)
    {
        std::unique_lock lock(_mutex);

        // write the XID to disk
        _write_committed_xid(xid);

        // if the XID contains schema changes, add it to the history
        if (has_schema_changes) {
            _history.push_back(xid);
        }

        // XXX check if we can clean up history?  or do we need a background thread for that?
    }

    uint64_t
    XidMgrServer::get_committed_xid(uint64_t schema_xid)
    {
        std::shared_lock lock(_mutex);

        // if schema XID is zero then we always return the most recent committed XID
        if (schema_xid == 0) {
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrServer: get committed xid: {}", _committed_xid);
            return _committed_xid;
        }

        // check the schema change history
        auto pos_i = std::ranges::upper_bound(_history, schema_xid);
        if (pos_i == _history.end()) {
            // if the schema XID is ahead of the history, return the most recent commited XID
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrServer: get committed xid: {}", _committed_xid);
            return _committed_xid;
        }

        // if we found an entry in the history, return the XID directly before that
        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrServer: xid limited by schema_xid: {}", (*pos_i) - 1);
        return (*pos_i) - 1;
    }

}
