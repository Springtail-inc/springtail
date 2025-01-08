#pragma once

#include <mutex>
#include <shared_mutex>
#include <memory>
#include <vector>
#include <string>
#include <filesystem>

#include <common/singleton.hh>

#include <thrift/server/TServer.h>
#include <thrift/concurrency/ThreadManager.h>


#include <xid_mgr/xid_partition.hh>

namespace springtail::xid_mgr {

    /**
     * @class XidMgrServer
     * @brief This class represents a server for managing transaction IDs (XIDs).
     *        It provides functionality to allocate XID ranges, commit XIDs, and retrieve the latest committed XID.
     */
    class XidMgrServer final : public Singleton<XidMgrServer>
    {
        friend class Singleton<XidMgrServer>;
    public:

        void startup();

        void stop() {
            _server->stop();
            _thread_manager->stop();
        }

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

        /** number of worker threads */
        int _worker_thread_count;

        /** server port */
        int _port;

        /** base path */
        std::filesystem::path _base_path;

        /** The thrift server. */
        std::shared_ptr<apache::thrift::server::TServer> _server;

        /** thread manager that is used by the server */
        std::shared_ptr<apache::thrift::concurrency::ThreadManager> _thread_manager = {nullptr};

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
