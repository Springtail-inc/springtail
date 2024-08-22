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

namespace springtail::xid_mgr {

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

        // iterate over all files in the base path creating partitions
        _load_partitions();
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

        _server->serve();
    }

    void
    XidMgrServer::_load_partitions()
    {
        std::set<int> partition_ids;

        // iterate over all files in the base path
        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrServer: loading partitions from {}", _base_path.string());

        for (const auto &entry : std::filesystem::directory_iterator(_base_path)) {
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrServer: found file {}", entry.path().string());

            if (!entry.is_regular_file()) {
                continue;
            }

            // check if filename is a partition file
            std::string filename = entry.path().filename().string();
            if (filename.find(Partition::PARTITION_FILE_PREFIX) != 0) {
                continue;
            }

            // remove prefix from filename to get id
            int id = std::stoi(filename.substr(strlen(Partition::PARTITION_FILE_PREFIX)));

            // insert into set
            partition_ids.insert(id);
        }

        // iterate set in order to load partitions in order
        for (int id : partition_ids) {
            // create a partition and load it
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrServer: loading partition {}", id);

            PartitionPtr partition = std::make_shared<Partition>(_base_path, id);
            partition->load();

            // add partition to list and map
            _partitions.push_back(partition);

            // load db_ids into map
            for (const auto &db_id : partition->get_db_ids()) {
                _partition_map[db_id] = partition;
            }
        }
    }

    void
    XidMgrServer::_shutdown()
    {
        if (_instance != nullptr) {
            _instance->_internal_shutdown();
            _instance->_server->stop();
        }
    }

    void
    XidMgrServer::_internal_shutdown()
    {
        std::unique_lock lock(_mutex);
        // iterate over partitions and shutdown
        for (auto &partition : _partitions) {
            partition->shutdown();
        }
    }

    PartitionPtr
    XidMgrServer::_get_partition(uint64_t db_id, bool create)
    {
        // assumes caller has lock

        auto it = _partition_map.find(db_id);
        if (it != _partition_map.end()) {
            // found a partition, return it
            return it->second;
        }

        // not doing a create, so return nullptr
        if (!create) {
            return nullptr;
        }

        // at this point we didn't find a partition for this db_id
        // so must allocate one, either by creating a new partition
        // or by reusing an existing partition that has space

        if (!_partitions.empty()) {
            // see if the last partition has space
            // we use the number of db_ids in the map modulo the
            // number of entries in a partition to determine if we
            // need to create a new partition; we do this to avoid
            // a race condition where a partition is not full, but
            // by the time we return and use it, it is full.
            if (_partition_map.size() % Partition::MAX_ENTRIES != 0) {
                PartitionPtr partition = _partitions.back();
                _partition_map[db_id] = partition;
                return partition;
            }
        }

        // create a new partition
        PartitionPtr partition = std::make_shared<Partition>(_base_path, _partitions.size());
        _partition_map[db_id] = partition;
        _partitions.push_back(partition);

        return partition;
    }

    uint64_t
    XidMgrServer::get_committed_xid(uint64_t db_id, uint64_t schema_xid)
    {
        PartitionPtr partition;
        // first try to get partition without write lock
        std::shared_lock lock(_mutex);
        partition = _get_partition(db_id, false);
        lock.unlock();

        // if partition is null
        if (partition == nullptr) {
            return 0;
        }

        return partition->get_committed_xid(db_id, schema_xid);
    }

    void
    XidMgrServer::commit_xid(uint64_t db_id, uint64_t xid, bool has_schema_changes)
    {
        PartitionPtr partition;
        // first try to get partition without write lock
        std::shared_lock rd_lock(_mutex);
        partition = _get_partition(db_id, false);

        if (partition != nullptr) {
            // shared lock held for update
            partition->commit_xid(db_id, xid, has_schema_changes);

            return;
        }

        rd_lock.unlock();

        // if partition is null, then get it with write lock to create new partition
        // we hold the lock during the commit to preserve space in the partition
        std::unique_lock wr_lock(_mutex);
        partition = _get_partition(db_id, true);

        // exclusive lock held for insert/create
        partition->commit_xid(db_id, xid, has_schema_changes);

        return;
    }
}
