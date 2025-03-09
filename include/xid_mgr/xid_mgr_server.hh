#pragma once

#include <shared_mutex>
#include <filesystem>
#include <vector>

#include <common/service_register.hh>
#include <common/singleton.hh>
#include <grpc/grpc_server_manager.hh>
#include <xid_mgr/xid_partition.hh>
#include <xid_mgr/xid_mgr_service.hh>

namespace springtail::xid_mgr {

class XidMgrServer : public Singleton<XidMgrServer> {
    friend class Singleton<XidMgrServer>;

public:
    void startup();

    /**
     * @brief commit up to and including given xid
     * @param db_id database id
     * @param xid xid to commit
     */
    void commit_xid(uint64_t db_id, uint64_t xid, bool has_schema_changes);

    /**
     * @brief Record a DDL change without doing a commit.  Used for table sync operations.
     * @param db_id database id
     * @param xid xid to commit
     */
    void record_ddl_change(uint64_t db_id, uint64_t xid);

    /**
     * @brief Get the latest committed xid object
     * @param db_id database id
     * @param schema_xid last known schema xid
     * @return uint64_t
     */
    uint64_t get_committed_xid(uint64_t db_id, uint64_t schema_xid);

private:
    XidMgrServer();
    ~XidMgrServer() override = default;

    std::unique_ptr<GrpcXidMgrService> _service;

    springtail::GrpcServerManager _grpc_server_manager;

    /** base path */
    std::filesystem::path _base_path;

    std::shared_mutex _mutex;

    /** list of partitions */
    std::vector<PartitionPtr> _partitions;

    /** map of db_id to partitions */
    std::map<uint64_t, PartitionPtr> _partition_map;

    /**
     * @brief Get a partition based on a db_id, optionally create it
     * @param db_id database id
     * @param create whether to create the partition if it doesn't exist
     * @return PartitionPtr
     */
    PartitionPtr _get_partition(uint64_t db_id, bool create);

    /**
     * @brief Load partitions from base path
     */
    void _load_partitions();

    void _internal_shutdown() override;
};

class XidMgrRunner : public ServiceRunner {
public:
    XidMgrRunner(bool commit_starting_xid, uint64_t db_id, uint64_t starting_xid) :
        ServiceRunner("XidMgr"),
        _commit_starting_xid(commit_starting_xid),
        _db_id(db_id), _starting_xid(starting_xid) {}

    bool start() override {
        if (_commit_starting_xid) {
            xid_mgr::XidMgrServer::get_instance()->commit_xid(_db_id, _starting_xid, false);
        }
        xid_mgr::XidMgrServer::get_instance()->startup();
        return true;
    }

    void stop() override {
        xid_mgr::XidMgrServer::shutdown();
    }

private:
    bool _commit_starting_xid;
    uint64_t _db_id;
    uint64_t _starting_xid;
};

}  // namespace springtail::xid_mgr
