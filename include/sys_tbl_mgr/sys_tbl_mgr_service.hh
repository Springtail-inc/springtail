#include <iostream>

#include <boost/thread.hpp>
#include <thrift/transport/TSocket.h>

#include <common/logging.hh>
#include <storage/table.hh>
#include <thrift/sys_tbl_mgr/ThriftSysTblMgr.h>

namespace springtail {

    /**
     * @brief This is the implementation of the ThriftSysTblMgrIf that is generated
     *        from the .thrift file.  It contains the service (handler) for actually
     *        implementing the remote procedure calls.
     */
    class ThriftSysTblMgrService : public thrift::sys_tbl_mgr::ThriftSysTblMgrIf
    {
    public:
        /**
         * @brief getInstance() of singleton; create if it doesn't exist.
         * @return instance of ThriftSysTblMgrService
         */
        static ThriftSysTblMgrService *get_instance();

        /**
         * @brief Shutdown the singleton.
         */
        static void shutdown();

    public:
        ThriftSysTblMgrService();

        void ping(thrift::sys_tbl_mgr::Status& _return) override;

        void create_table(thrift::sys_tbl_mgr::Status& _return,
                          const thrift::sys_tbl_mgr::TableRequest &request) override;

        void alter_table(thrift::sys_tbl_mgr::Status& _return,
                         const thrift::sys_tbl_mgr::TableRequest &request) override;

        void drop_table(thrift::sys_tbl_mgr::Status& _return,
                        const thrift::sys_tbl_mgr::DropTableRequest &request) override;

        void update_roots(thrift::sys_tbl_mgr::Status& _return,
                          const thrift::sys_tbl_mgr::UpdateRootsRequest &request) override;

        void finalize(thrift::sys_tbl_mgr::Status& _return,
                      const thrift::sys_tbl_mgr::FinalizeRequest &request) override;

        void get_roots(thrift::sys_tbl_mgr::GetRootsResponse& _return,
                       const thrift::sys_tbl_mgr::GetRootsRequest &request) override;

        void get_schema(thrift::sys_tbl_mgr::GetSchemaResponse& _return,
                        const thrift::sys_tbl_mgr::GetSchemaRequest &request) override;

        void get_target_schema(thrift::sys_tbl_mgr::GetSchemaResponse& _return,
                               const thrift::sys_tbl_mgr::GetTargetSchemaRequest &request) override;

    private:
        struct TableInfo {
            uint64_t id;
            uint64_t xid;
            uint64_t lsn;
            std::string schema;
            std::string name;
            bool exists;
        };
        using TableInfoPtr = std::shared_ptr<TableInfo>;

        using RootsInfoPtr = std::shared_ptr<thrift::sys_tbl_mgr::GetRootsResponse>;
        using SchemaInfoPtr = std::shared_ptr<thrift::sys_tbl_mgr::GetSchemaResponse>;

    private:
        TableInfoPtr _get_table_info(uint64_t table_id, const XidLsn &xid);
        void _set_table_info(TableInfoPtr table_info);
        void _clear_table_info();

        RootsInfoPtr _get_roots_info(uint64_t table_id, const XidLsn &xid);
        void _set_roots_info(uint64_t table_id, const XidLsn &xid, RootsInfoPtr roots_info);
        void _clear_roots_info();

        SchemaInfoPtr _get_schema_info(uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);
        std::map<uint32_t, thrift::sys_tbl_mgr::TableColumn> _read_schema_columns(uint64_t table_id, const XidLsn &access_xid);
        void _apply_schema_cache_history(uint64_t table_id, const XidLsn &xid, std::map<uint32_t, thrift::sys_tbl_mgr::TableColumn> &columns);
        std::vector<thrift::sys_tbl_mgr::ColumnHistory> _read_schema_history(uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);
        std::vector<thrift::sys_tbl_mgr::ColumnHistory> _get_schema_cache_history(uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);
        void _set_schema_info(uint64_t table_id, const std::vector<thrift::sys_tbl_mgr::ColumnHistory> &columns);
        void _clear_schema_info();
        thrift::sys_tbl_mgr::ColumnHistory _generate_update(const std::vector<thrift::sys_tbl_mgr::TableColumn> &old_schema,
                                                            const std::vector<thrift::sys_tbl_mgr::TableColumn> &new_schema,
                                                            const XidLsn &xid);


        TablePtr _get_system_table(uint64_t table_id);
        MutableTablePtr _get_mutable_system_table(uint64_t table_id);

    private:
        static ThriftSysTblMgrService *_instance; ///< static instance (singleton)
        static boost::mutex _instance_mutex; ///< protects lookup/creation of singleton _instance

        /** To protect the internal data structures. */
        boost::shared_mutex _mutex;

        /**
         * Locked for read when accessing the read-only tables.  Unique lock when finalizing and
         * swapping the XID.
         */
        boost::shared_mutex _read_mutex;

        /**
         * Locked for read when accessing the mutable tables.  Unique lock when finalizing and
         * swapping the XID.
         */
        boost::shared_mutex _write_mutex;

        /** XID at which the service is currently reading data. */
        XidLsn _access_xid;

        /** XID to which the service is currently committing data. */
        uint64_t _target_xid;

        /** The read-only interface for the system tables to service get requests. */
        std::map<uint64_t, TablePtr> _read;

        /** The write-only interface for the system tables to service mutations. */
        std::map<uint64_t, MutableTablePtr> _write;

        /**
         * Cache of unapplied table info changes.
         * Stored as a map of Table ID -> XID/LSN (in reverse order) -> TableInfo
         */
        std::map<uint64_t, std::map<XidLsn, TableInfoPtr, std::greater<XidLsn>>> _table_cache;

        /**
         * Cache of unapplied table roots/stats changes.
         * Stored as a map of Table ID -> XID/LSN (in reverse order) -> RootsInfo
         */
        std::map<uint64_t, std::map<XidLsn, RootsInfoPtr, std::greater<XidLsn>>> _roots_cache;

        /**
         * Cache of unapplied schema changes.
         * Stored as a map of Table ID -> Column ID -> vector<ColumnHistory> (in ascending XID/LSN order)
         * Using vector because there may be multiple entries at the same XID/LSN on table create.
         */
        std::map<uint64_t, std::map<uint32_t, std::vector<thrift::sys_tbl_mgr::ColumnHistory>>> _schema_cache;
    };


    /**
     * @brief Private helper class to override handler creation;
     *        can be used to store per connection state or log incoming connections
     */
    class ThriftSysTblMgrCloneFactory : virtual public thrift::sys_tbl_mgr::ThriftSysTblMgrIfFactory {
        public:
            ~ThriftSysTblMgrCloneFactory() override = default;

            /**
             * @brief Override the thrift getHandler call, allows for logging
             * @param connInfo Thrift connection info object
             * @return thrift::sys_tbl_mgr::ThriftSysTblMgrIf*
             */
            thrift::sys_tbl_mgr::ThriftSysTblMgrIf*
            getHandler(const apache::thrift::TConnectionInfo &connInfo) override
            {
                std::shared_ptr<apache::thrift::transport::TSocket> sock =
                    std::dynamic_pointer_cast<apache::thrift::transport::TSocket>(connInfo.transport);

                SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "Incoming connection\n");
                SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "\tSocketInfo: {}\n", sock->getSocketInfo());
                SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "\tPeerHost: {}\n", sock->getPeerHost());
                SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "\tPeerAddress: {}\n", sock->getPeerAddress());
                SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "\tPeerPort: {}\n", sock->getPeerPort());

                return ThriftSysTblMgrService::get_instance();
            }

            void
            releaseHandler(thrift::sys_tbl_mgr::ThriftSysTblMgrIf *handler) override {
                // delete handler;
            }
    };
}
