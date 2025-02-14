#pragma once

#include <shared_mutex>
#include <vector>
#include <filesystem>

#include <common/singleton.hh>

#include <thrift/xid_mgr/ThriftXidMgr.h>
#include <thrift/common/thrift_server.hh>

#include <xid_mgr/xid_partition.hh>
#include <xid_mgr/xid_mgr_service.hh>

namespace springtail::xid_mgr {
    /**
     * @class XidMgrServer
     * @brief This class represents a server for managing transaction IDs (XIDs).
     *        It provides functionality to allocate XID ranges, commit XIDs, and retrieve the latest committed XID.
     */
    class XidMgrServer final :
            public springtail::thrift::Server<XidMgrServer,
                                            thrift::xid_mgr::ThriftXidMgrProcessorFactory,
                                            ThriftXidMgrService,
                                            thrift::xid_mgr::ThriftXidMgrIfFactory,
                                            thrift::xid_mgr::ThriftXidMgrIf>,
            public Singleton<XidMgrServer>
    {
        friend class Singleton<XidMgrServer>;
    public:
        // interfaces from thrift

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

    protected:
        void _internal_shutdown() override;

    private:
        /**
         * @brief Construct a new XidMgr object
         */
        XidMgrServer();

        /**
         * @brief Destroy the XidMgr object; shouldn't be called directly use shutdown()
         */
         ~XidMgrServer() override = default;

        // Vector containing the xid mgr methods
        std::vector<std::pair<std::string, std::string>> _xid_mgr_counter_metrics = {
            {"xid_mgr_commit_xid_calls", "Total number of XID commits"},
            {"xid_mgr_record_ddl_change_calls", "Total number of XID record DDL change calls"},
            {"xid_mgr_get_partition_calls", "Total number of XID get partition calls"},
            {"xid_mgr_get_committed_xid_calls", "Total number of XID get committed xid calls"}
        };

        std::vector<std::pair<std::string, std::string>> _xid_mgr_histogram_metrics = {
            {"xid_mgr_commit_xid_latencies", "Latency of XID commits"},
            {"xid_mgr_record_ddl_change_latencies", "Latency of XID record DDL change calls"}
        };

        /**
         * @brief Register metrics for monitoring XID manager operations
         * 
         * Registers counters for tracking various XID manager operations like:
         * - Number of XID commits
         * - Number of DDL change records
         * - Number of partition lookups
         * - Number of committed XID lookups
         */
        void _register_metrics();

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
    };

} // namespace springtail
