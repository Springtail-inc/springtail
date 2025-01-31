#pragma once

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
    class Service final: public ServiceIf, public Singleton<Service>
    {
        friend class Singleton<Service>;
    public:
        /** Simple interface to help ensure that the server is still running. */
        void ping(Status& _return) override;

        /** Creates an index within the system tables. */
        void create_index(DDLStatement& _return, const IndexRequest &request) override;

        /** Drops an index within the system tables. */
        void drop_index(DDLStatement& _return, const DropIndexRequest &request) override;

        /** Set the state of the index within the system tables. */
        void set_index_state(Status& _return, const SetIndexStateRequest &request) override;

        /** Get the index info. */
        void get_index_info(IndexInfo& _return, const GetIndexInfoRequest &request) override;

        /** Creates a table within the system tables. */
        void create_table(DDLStatement& _return, const TableRequest &request) override;

        /** Alters a table within the system tables. */
        void alter_table(DDLStatement& _return, const TableRequest &request) override;

        /** Drops a table within the system tables.  Note that this will not update the roots of the
            table, just the metadata to indicate that a drop occurred at the given XID/LSN. */
        void drop_table(DDLStatement& _return, const DropTableRequest &request) override;

        /** Creates a namespace in the system tables. */
        void create_namespace(DDLStatement &_return, const NamespaceRequest &request) override;

        /** Renames a namespace in the system tables. */
        void alter_namespace(DDLStatement &_return, const NamespaceRequest &request) override;

        /** Drops a namespace in the system tables. */
        void drop_namespace(DDLStatement &_return, const NamespaceRequest &request) override;

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
        void swap_sync_table(DDLStatement &_return,
                             const NamespaceRequest &namespace_req,
                             const TableRequest &create_req,
                             const std::vector<IndexRequest> &index_reqs,
                             const UpdateRootsRequest &roots_req) override;

    private:
        Service() = default;
        ~Service() override = default;

        // CACHE FOR NAMES

        /**
         * An in-memory representation of the table_names system table used for caching.
         */
        struct TableCacheRecord {
            uint64_t id; ///< The table ID.
            uint64_t xid; ///< The XID at which this entry becomes valid.
            uint64_t lsn; ///< The LSN at which this entry becomes valid.
            uint64_t namespace_id; ///< The ID of the schema/namespace of the table.
            std::string name; ///< The name of the table.
            bool exists; ///< A flag indicating if the table exists at this point.
            TableCacheRecord(uint64_t id, uint64_t xid, uint64_t lsn, uint64_t namespace_id, const std::string &name, bool exists)
                : id(id), xid(xid), lsn(lsn), namespace_id(namespace_id), name(name), exists(exists)
            { }
            TableCacheRecord() = default;
        };
        using TableCacheRecordPtr = std::shared_ptr<TableCacheRecord>;

        /**
         * Retrieve the TableCacheRecord either from the cache or from the system tables if not available.
         * @param table_id The ID of the table.
         * @param xid The XID/LSN at which we are querying.
         */
        TableCacheRecordPtr _get_table_info(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Stores the TableCacheRecord, performing a write-through in the cache and the system tables.  We
         * don't finalize the system tables so they may remain dirty in the StorageCache.  Nothing
         * from the cache is not evicted until _clear_table_info() is called.
         * @param table_info The metadata to update.
         */
        void _set_table_info(uint64_t db_id, TableCacheRecordPtr table_info);

        /**
         * Clears the cache of TableCacheRecord objects.  Called by finalize() once the system tables are
         * all committed to disk.
         */
        void _clear_table_info(uint64_t db_id);


        // CACHE FOR ROOTS / STATS

        /** We use the thrift object response as the cache data for the roots/stats. */
        using RootsCacheRecordPtr = std::shared_ptr<GetRootsResponse>;

        /**
         * Retrieve the RootsCacheRecord either from the cache or from the system tables if not available.
         * @param table_id The ID of the table.
         * @param xid The XID/LSN at which we are querying.
         */
        RootsCacheRecordPtr _get_roots_info(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Stores the RootsCacheRecord, performing a write-through in the cache and the system tables.  We
         * don't finalize the system tables so they may remain dirty in the StorageCache.  Nothing
         * from the cache is not evicted until _clear_roots_info() is called.
         * @param table_info The metadata to update.
         */
        void _set_roots_info(uint64_t db_id, uint64_t table_id, const XidLsn &xid, RootsCacheRecordPtr roots_info);

        /**
         * Clears the cache of TableCacheRecord objects.  Called by finalize() once the system tables are
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
         * @param table_name The table name.
         * @param schema The table schema.
         * @param columns The set of column data to record.
         */
        void _set_schema_info(uint64_t db_id,
                              uint64_t table_id,
                              uint64_t namespace_id,
                              const std::string &table_name,
                              const std::vector<ColumnHistory> &columns);

        /**
         * Set primary index information according to the current state 
         * of the table columns.
         * @param db_id The database ID.
         * @param table_id The table that the schema is for.
         * @param schema The table schema.
         * @param table_name The table name.
         */
        void _set_primary_index(uint64_t db_id,
                                uint64_t namespace_id,
                                uint64_t table_id,
                                const std::string &schema,
                                const std::string &table_name,
                                const XidLsn &xid);

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
        void _read_schema_columns(SchemaInfoPtr info, uint64_t db_id, uint64_t table_id, const XidLsn &access_xid);

        /**
         * Helper function to read the table indexes.
         * @param db_id The datebase ID.
         * @param table_id The table for which we are constructing a schema.
         * @param access_xid The XID/LSN at which we are querying the schema.
         */
        void _read_schema_indexes(SchemaInfoPtr schema_info,
                                  uint64_t db_id, uint64_t table_id, const XidLsn &access_xid);

        /**
         * Helper function to apply any in-memory changes to the schema columns that might be
         * required to bring them up-to-date to the provided XID/LSN.
         * @param table_id The table for which we are constructing a schema.
         * @param xid The XID/LSN at which we are querying the schema.
         * @param columns A set of columns already constructed by calling _read_schema_columns()
         *                that will be updated by this function.
         */
        void _apply_schema_cache_history(SchemaInfoPtr info, uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Helper function to apply any in-memory changes to the indexes that might be
         * required to bring them up-to-date to the provided XID/LSN.
         * @param[out] info The schema indexes that will be updated.
         * @param db_id The database ID.
         * @param table_id The table for which we are constructing a schema.
         * @param xid The XID/LSN at which we are querying the schema.
         */
        void _apply_index_cache_history(SchemaInfoPtr info, uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Helper function to read any schema changes recorded between the provided access_xid and
         * target_xid from the on-disk system tables.
         * @param table_id The table for which we are constructing a schema.
         * @param access_xid The XID/LSN at which we are constructing a schema.
         * @param target_xid The XID/LSN up to which we are capturing changes to that schema.
         */
        void _read_schema_history(SchemaInfoPtr info, uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

        /**
         * Helper function to read any schema changes recorded between the provided access_xid and
         * target_xid from the in-memory cache.  Note: this should never overlap with the on-disk
         * structures since we drop the cache once the system tables are finalized.
         * @param table_id The table for which we are constructing a schema.
         * @param access_xid The XID/LSN at which we are constructing a schema.
         * @param target_xid The XID/LSN up to which we are capturing changes to that schema.
         */
        void _get_schema_cache_history(SchemaInfoPtr info, uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

        /**
         * Helper function to extract a change entry for a schema by comparing the old and new
         * schemas from before and after the ALTER TABLE statement.
         * @param old_schema The schema before the alteration.
         * @param new_schema The schema after the alteration.
         * @param xid The XID/LSN at which the alteration occurred.
         */
        ColumnHistory _generate_update(const std::map<int32_t, TableColumn> &old_schema,
                                       const std::vector<TableColumn> &new_schema,
                                       const XidLsn &xid,
                                       nlohmann::json &ddl);


        // CACHE FOR NAMESPACES

        /**
         * Entry for the namespace cache.
         */
        struct NamespaceCacheRecord {
            uint64_t id;
            std::string name;
            bool exists;

            NamespaceCacheRecord(uint64_t id, std::string_view name, bool exists)
                : id(id), name(name), exists(exists)
            { }
            NamespaceCacheRecord() = default;
        };
        using NamespaceCacheRecordPtr = std::shared_ptr<NamespaceCacheRecord>;

        /**
         * Read the namespace info from the NamespaceNames system table given it's ID and an
         * XID/LSN.
         */
        NamespaceCacheRecordPtr _get_namespace_info(uint64_t db_id, uint64_t namespace_id, const XidLsn &xid);

        /**
         * Read the namespace info from the NamespaceNames system table given it's name and an
         * XID/LSN.
         */
        NamespaceCacheRecordPtr _get_namespace_info(uint64_t db_id, const std::string &name, const XidLsn &xid);


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
         * Writes the index info int the system table.
         *
         * @param xid The XID/LSN at which the transaction occured.
         * @param db_id The database ID.
         * @param tab_id The ID of the user table.
         * @param index_id The index ID.
         * @param keys The index keys consisting of (column index position, column position in the table).
         */
        void _write_index(const XidLsn& xid, uint64_t db_id, uint64_t tab_id, uint64_t index_id, const std::map<uint32_t, uint32_t>& keys);

        /**
         * Performs a create_index() assuming that the correct locks are already held.
         */
        nlohmann::json _create_index(const IndexRequest &request);

        /**
         * Performs a drop_index() assuming that the correct locks are already held.
         * @param xid The XID/LSN at which the transaction occurred.
         * @param db_id The database ID.
         * @param index_id The index ID.
         * @param tid The optional table ID that the index belongs to. When the index is dropped PG
         *            trigger provides the index ID but there doesn't seem to be a way to extract the corresponding
         *            table ID (see pg_event_trigger_dropped_objects). This should not be a problem because
         *            the index ID's are guaranteed to be unique and so the table ID is optional.
         *            There is a special case when tid is required. We construct primary indexes in create table
         *            using the column attributes and assign the same index ID=constant::PRIMARY_INDEX to all primary
         *            indexes and so tid is required for PRIMARY_INDEX.
         */
        void _drop_index(const XidLsn& xid, uint64_t db_id, uint64_t index_id, std::optional<uint64_t> tid=std::nullopt);

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

        /** Performs an set_index_state() assuming that the correct locks are already held.
         */
        bool _set_index_state(const SetIndexStateRequest &request);

        /** Performs an get_index_info() assuming that the correct locks are already held.
         */
        IndexInfo _get_index_info(const GetIndexInfoRequest &request);

        /** This doesn't return information about index columns
         */
        std::optional<std::tuple<IndexInfo, uint64_t, XidLsn>> _find_index(
            uint64_t db_id, uint64_t index_id, const XidLsn &xid, std::optional<uint64_t> tid);

        /**
         * Helper for updating the namespace_names table.
         */
        nlohmann::json _mutate_namespace(uint64_t db, uint64_t ns_id, std::optional<std::string> name,
                                         const XidLsn &xid, bool exists);

        std::optional<std::pair<IndexInfo, XidLsn>> _find_cached_index(uint64_t db_id, uint64_t index_id,
                const XidLsn& xid, std::optional<uint64_t> tid);

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
         * Cache of unapplied namespace changes by namespace ID.
         * Stored as a map of DB -> Namespace ID -> XID/LSN (in reverse order) -> NamespaceInfo
         */
        std::map<uint64_t,
                 std::map<uint64_t,
                          std::map<XidLsn,
                                   NamespaceCacheRecordPtr,
                                   std::greater<XidLsn>>>> _namespace_id_cache;

        /**
         * Cache of unapplied namespace changes by namespace name.
         * Stored as a map of DB -> Namespace name -> XID/LSN (in reverse order) -> NamespaceInfo
         */
        std::map<uint64_t,
                 std::map<std::string,
                          std::map<XidLsn,
                                   NamespaceCacheRecordPtr,
                                   std::greater<XidLsn>>>> _namespace_name_cache;

        /**
         * Cache of unapplied table info changes.
         * Stored as a map of DB -> Table ID -> XID/LSN (in reverse order) -> TableCacheRecord
         */
        std::map<uint64_t,
                 std::map<uint64_t,
                          std::map<XidLsn,
                                   TableCacheRecordPtr,
                                   std::greater<XidLsn>>>> _table_cache;

        /**
         * Cache of unapplied table roots/stats changes.
         * Stored as a map of DB -> Table ID -> XID/LSN (in reverse order) -> RootsCacheRecord
         */
        std::map<uint64_t,
                 std::map<uint64_t,
                          std::map<XidLsn,
                                   RootsCacheRecordPtr,
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
        /**
         * Cache of unapplied index changes.
         * Stored as a map of DB -> Table ID -> (vector<IndexCacheItem>) (in ascending XID/LSN order)
         */
        struct IndexCacheItem
        {
            XidLsn xid;
            IndexInfo info;
        };
        // The map is keyed the index ID.
        // The cache items  are in ascending XID order
        using TableIndexCache = std::map<uint64_t, std::vector<IndexCacheItem>>;

        // index cache per table
        using TableId = uint64_t;
        using TableIndexMap = std::map<TableId, TableIndexCache>;

        // index cache per DB
        using DbId = uint64_t;
        std::map<DbId, TableIndexMap> _index_cache;
    };
}
