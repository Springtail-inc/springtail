#include <iostream>

#include <boost/thread.hpp>
#include <thrift/transport/TSocket.h>

#include <common/logging.hh>
#include <sys_tbl_mgr/table.hh>
#include <thrift/sys_tbl_mgr/Service.h>

namespace springtail::sys_tbl_mgr {

    /**
     * This is the implementation of the ThriftSysTblMgrIf that is generated from the .thrift file.
     * It contains the service (handler) for actually implementing the remote procedure calls.
     *
     * The Service object maintains specialized caches of system metadata for uncommitted changes to
     * the system tables that are used to retrieve uncommitted metadata changes needed by in-flight
     * data mutations since there is currently no way to query a MutableTable.  We currently don't
     * cache any metadata once it's written to disk, instead relying on the StorageCache to keep
     * table extents in-memory for fast retrieval.
     */
    class Service : public ServiceIf
    {
    public:
        /**
         * @brief getInstance() of singleton; create if it doesn't exist.
         * @return instance of ThriftSysTblMgrService
         */
        static Service *get_instance();

        /**
         * @brief Shutdown the singleton.
         */
        static void shutdown();

    public:
        /** Simple interface to help ensure that the server is still running. */
        void ping(Status& _return) override;

        /** Creates a table within the system tables. */
        void create_table(DDLStatement& _return, const TableRequest &request) override;

        /** Alters a table within the system tables. */
        void alter_table(DDLStatement& _return, const TableRequest &request) override;

        /** Drops a table within the system tables.  Note that this will not update the roots of the
            table, just the metadata to indicate that a drop occurred at the given XID/LSN. */
        void drop_table(DDLStatement& _return, const DropTableRequest &request) override;

        /** Updates the roots extents of the indexes of the table as well as the table stats. */
        void update_roots(Status& _return, const UpdateRootsRequest &request) override;

        /** Finalizes the metadata to disk up to at least the provided XID.  Note: may contain
            changes from later XIDs as well, which is fine given we always query the metadata at a
            specific point in the XID/LSN stream. */
        void finalize(Status& _return, const FinalizeRequest &request) override;

        /** Retrieve the root extents and stats for a given table. */
        void get_roots(GetRootsResponse& _return, const GetRootsRequest &request) override;

        /** Retrieve the schema information for a given table at a given XID/LSN. */
        void get_schema(GetSchemaResponse& _return, const GetSchemaRequest &request) override;

        /** Retrieve the schema information for a given table at a given XID/LSN along with the
            history of any changes to bring it to a target XID/LSN. */
        void get_target_schema(GetSchemaResponse& _return, const GetTargetSchemaRequest &request) override;

        /** Returns a boolean indicating if the table exists at a given XID. */
        bool exists(const ExistsRequest &request) override;

        /** Performs a drop (if needed), create, and update_roots for a given table to swap it's
            newly synced data into place at a given XID.  Returns a JSON array of DDL statements to
            update the FDWs. */
        void swap_sync_table(DDLStatement &_return, const TableRequest &create, const UpdateRootsRequest &roots) override;

    private:
        // CACHE FOR NAMES

        /**
         * An in-memory representation of the table_names system table used for caching.
         */
        struct TableInfo {
            uint64_t id; ///< The table ID.
            uint64_t xid; ///< The XID at which this entry becomes valid.
            uint64_t lsn; ///< The LSN at which this entry becomes valid.
            std::string schema; ///< The schema/namespace of the table.
            std::string name; ///< The name of the table.
            bool exists; ///< A flag indicating if the table exists at this point.
            TableInfo(uint64_t id, uint64_t xid, uint64_t lsn, const std::string &schema, const std::string &name, bool exists)
                : id(id), xid(xid), lsn(lsn), schema(schema), name(name), exists(exists)
            { }
            TableInfo() = default;
        };
        using TableInfoPtr = std::shared_ptr<TableInfo>;

        /**
         * Retrieve the TableInfo either from the cache or from the system tables if not available.
         * @param table_id The ID of the table.
         * @param xid The XID/LSN at which we are querying.
         */
        TableInfoPtr _get_table_info(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Stores the TableInfo, performing a write-through in the cache and the system tables.  We
         * don't finalize the system tables so they may remain dirty in the StorageCache.  Nothing
         * from the cache is not evicted until _clear_table_info() is called.
         * @param table_info The metadata to update.
         */
        void _set_table_info(uint64_t db_id, TableInfoPtr table_info);

        /**
         * Clears the cache of TableInfo objects.  Called by finalize() once the system tables are
         * all committed to disk.
         */
        void _clear_table_info(uint64_t db_id);


        // CACHE FOR ROOTS / STATS

        /** We use the thrift object response as the cache data for the roots/stats. */
        using RootsInfoPtr = std::shared_ptr<GetRootsResponse>;

        /**
         * Retrieve the RootsInfo either from the cache or from the system tables if not available.
         * @param table_id The ID of the table.
         * @param xid The XID/LSN at which we are querying.
         */
        RootsInfoPtr _get_roots_info(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Stores the RootsInfo, performing a write-through in the cache and the system tables.  We
         * don't finalize the system tables so they may remain dirty in the StorageCache.  Nothing
         * from the cache is not evicted until _clear_roots_info() is called.
         * @param table_info The metadata to update.
         */
        void _set_roots_info(uint64_t db_id, uint64_t table_id, const XidLsn &xid, RootsInfoPtr roots_info);

        /**
         * Clears the cache of TableInfo objects.  Called by finalize() once the system tables are
         * all committed to disk.
         */
        void _clear_roots_info(uint64_t db_id);


        // CACHE FOR SCHEMA

        /** We use the thrift object response as the cache data for the Schema information. */
        using SchemaInfoPtr = std::shared_ptr<GetSchemaResponse>;

        /**
         * Retrieve the SchemaInfo either from the cache or from the system tables if not available.
         * @param table_id The ID of the table.
         * @param access_xid The XID/LSN at which we are querying.
         * @param target_xid The XID/LSN up to which we should return a history of changes from the access_xid.
         */
        SchemaInfoPtr _get_schema_info(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

        /**
         * Records the provided column data, performing a write-through in the cache and the system
         * tables.  We don't finalize the system tables so they may remain dirty in the
         * StorageCache.  Nothing from the cache is not evicted until _clear_schema_info() is
         * called.
         * @param table_id The table that the schema is for.
         * @param columns The set of column data to record.
         */
        void _set_schema_info(uint64_t db_id, uint64_t table_id, const std::vector<ColumnHistory> &columns);

        /**
         * Clears the cache of schema data.  Called by finalize() once the system tables are
         * all committed to disk.
         */
        void _clear_schema_info(uint64_t db_id);

        /**
         * Helper function to read the full set of columns for a table from the on-disk system tables.
         * @param table_id The table for which we are constructing a schema.
         * @param access_xid The XID/LSN at which we are querying the schema.
         */
        std::map<uint32_t, TableColumn> _read_schema_columns(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid);

        /**
         * Helper function to apply any in-memory changes to the schema columns that might be
         * required to bring them up-to-date to the provided XID/LSN.
         * @param table_id The table for which we are constructing a schema.
         * @param xid The XID/LSN at which we are querying the schema.
         * @param columns A set of columns already constructed by calling _read_schema_columns()
         *                that will be updated by this function.
         */
        void _apply_schema_cache_history(uint64_t db_id, uint64_t table_id, const XidLsn &xid, std::map<uint32_t, TableColumn> &columns);

        /**
         * Helper function to read any schema changes recorded between the provided access_xid and
         * target_xid from the on-disk system tables.
         * @param table_id The table for which we are constructing a schema.
         * @param access_xid The XID/LSN at which we are constructing a schema.
         * @param target_xid The XID/LSN up to which we are capturing changes to that schema.
         */
        std::vector<ColumnHistory> _read_schema_history(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

        /**
         * Helper function to read any schema changes recorded between the provided access_xid and
         * target_xid from the in-memory cache.  Note: this should never overlap with the on-disk
         * structures since we drop the cache once the system tables are finalized.
         * @param table_id The table for which we are constructing a schema.
         * @param access_xid The XID/LSN at which we are constructing a schema.
         * @param target_xid The XID/LSN up to which we are capturing changes to that schema.
         */
        std::vector<ColumnHistory> _get_schema_cache_history(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

        /**
         * Helper function to extract a change entry for a schema by comparing the old and new
         * schemas from before and after the ALTER TABLE statement.
         * @param old_schema The schema before the alteration.
         * @param new_schema The schema after the alteration.
         * @param xid The XID/LSN at which the alteration occurred.
         */
        ColumnHistory _generate_update(const std::vector<TableColumn> &old_schema,
                                       const std::vector<TableColumn> &new_schema,
                                       const XidLsn &xid,
                                       nlohmann::json &ddl);


        // HELPER FUNCTIONS

        /**
         * Retrieves a read-only Table interface for a given system table.
         * @param table_id The ID of the system table.
         */
        TablePtr _get_system_table(uint64_t db_id, uint64_t table_id);

        /**
         * Retrieves a write-only MutableTable interface for a given system table.
         * @param table_id The ID of the system table.
         */
        MutableTablePtr _get_mutable_system_table(uint64_t db_id, uint64_t table_id);

        /**
         * Performs a create_table() assuming that the correct locks are already held.
         */
        nlohmann::json _create_table(const TableRequest &request);

        /**
         * Performs a drop_table() assuming that the correct locks are already held.
         */
        nlohmann::json _drop_table(const DropTableRequest &request);

        /**
         * Performs an update_roots() assuming that the correct locks are already held.
         */
        void _update_roots(const UpdateRootsRequest &request);


        /**
         * Retrieve the current read XID for a db.
         */
        XidLsn _get_read_xid(uint64_t db_id);

        /**
         * Retrieve the current write XID for a db.
         */
        uint64_t _get_write_xid(uint64_t db_id);

        /**
         * Set the read and write XIDs.
         */
        void _set_xids(uint64_t db_id, const XidLsn &read_xid, uint64_t write_xid);

        // VARIABLES

        static Service *_instance; ///< static instance (singleton)
        static boost::mutex _instance_mutex; ///< protects lookup/creation of singleton _instance

        /** To protect the internal data structures. */
        boost::shared_mutex _mutex;

        /** Mutex to protect the XID maps. */
        boost::mutex _xid_mutex;

        /** XID at which the service is currently reading system table data for a given DB. */
        std::map<uint64_t, XidLsn> _read_xid;

        /** XID to which the service is currently committing system table data for a given DB. */
        std::map<uint64_t, uint64_t> _write_xid;

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

        /** The read-only interface for the system tables to service get requests. */
        std::map<uint64_t, std::map<uint64_t, TablePtr>> _read;

        /** The write-only interface for the system tables to service mutations. */
        std::map<uint64_t, std::map<uint64_t, MutableTablePtr>> _write;

        /**
         * Cache of unapplied table info changes.
         * Stored as a map of DB -> Table ID -> XID/LSN (in reverse order) -> TableInfo
         */
        std::map<uint64_t,
                 std::map<uint64_t,
                          std::map<XidLsn,
                                   TableInfoPtr,
                                   std::greater<XidLsn>>>> _table_cache;

        /**
         * Cache of unapplied table roots/stats changes.
         * Stored as a map of DB -> Table ID -> XID/LSN (in reverse order) -> RootsInfo
         */
        std::map<uint64_t,
                 std::map<uint64_t,
                          std::map<XidLsn,
                                   RootsInfoPtr,
                                   std::greater<XidLsn>>>> _roots_cache;

        /**
         * Cache of unapplied schema changes.
         * Stored as a map of DB -> Table ID -> Column ID -> vector<ColumnHistory> (in ascending XID/LSN order)
         * Using vector because there may be multiple entries at the same XID/LSN on table create.
         */
        std::map<uint64_t,
                 std::map<uint64_t,
                          std::map<uint32_t,
                                   std::vector<ColumnHistory>>>> _schema_cache;
    };


    /**
     * @brief Private helper class to override handler creation;
     *        can be used to store per connection state or log incoming connections
     */
    class ServiceCloneFactory : virtual public ServiceIfFactory {
    public:
        ~ServiceCloneFactory() override = default;

        /**
         * @brief Override the thrift getHandler call, allows for logging
         * @param connInfo Thrift connection info object
         * @return thrift::sys_tbl_mgr::ThriftSysTblMgrIf*
         */
        ServiceIf *
        getHandler(const apache::thrift::TConnectionInfo &connInfo) override
        {
            std::shared_ptr<apache::thrift::transport::TSocket> sock =
                std::dynamic_pointer_cast<apache::thrift::transport::TSocket>(connInfo.transport);

            SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "Incoming connection");
            SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "\tSocketInfo: {}", sock->getSocketInfo());
            SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "\tPeerHost: {}", sock->getPeerHost());
            SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "\tPeerAddress: {}", sock->getPeerAddress());
            SPDLOG_DEBUG_MODULE(LOG_SYS_TBL_MGR, "\tPeerPort: {}", sock->getPeerPort());

            return Service::get_instance();
        }

        void
        releaseHandler(ServiceIf *handler) override {
            // delete handler;
        }
    };
}
