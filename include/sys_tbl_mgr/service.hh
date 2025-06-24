#pragma once

#include <memory>
#include <map>

#include <common/logging.hh>
#include <common/singleton.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <grpcpp/grpcpp.h>
#include <proto/sys_tbl_mgr.grpc.pb.h>
#include <sys_tbl_mgr/table.hh>

namespace springtail::sys_tbl_mgr {

/**
 * This is the implementation of the gRPC SysTblMgr service defined in sys_tbl_mgr.proto.
 * It contains the service (handler) for actually implementing the remote procedure calls.
 *
 * The Service object maintains specialized caches of system metadata for uncommitted changes to
 * the system tables that are used to retrieve uncommitted metadata changes needed by in-flight
 * data mutations since there is currently no way to query a MutableTable.  We currently don't
 * cache any metadata once it's written to disk, instead relying on the StorageCache to keep
 * table extents in-memory for fast retrieval.
 */
class Service final : public proto::SysTblMgr::Service, public Singleton<Service> {
    friend class Singleton<Service>;

public:
    /** Simple interface to help ensure that the server is still running. */
    grpc::Status Ping(grpc::ServerContext* context,
                      const google::protobuf::Empty* request,
                      google::protobuf::Empty* response) override;

    /** Creates an index within the system tables. */
    grpc::Status CreateIndex(grpc::ServerContext* context,
                             const proto::IndexRequest* request,
                             proto::IndexProcessRequest* response) override;

    /** Drops an index within the system tables. */
    grpc::Status DropIndex(grpc::ServerContext* context,
                           const proto::DropIndexRequest* request,
                           proto::IndexProcessRequest* response) override;

    /** Set the state of the index within the system tables. */
    grpc::Status SetIndexState(grpc::ServerContext* context,
                               const proto::SetIndexStateRequest* request,
                               google::protobuf::Empty* response) override;

    /** Get the index info. */
    grpc::Status GetIndexInfo(grpc::ServerContext* context,
                              const proto::GetIndexInfoRequest* request,
                              proto::IndexInfo* response) override;

    /** Creates a table within the system tables. */
    grpc::Status CreateTable(grpc::ServerContext* context,
                             const proto::TableRequest* request,
                             proto::DDLStatement* response) override;

    /** Alters a table within the system tables. */
    grpc::Status AlterTable(grpc::ServerContext* context,
                            const proto::TableRequest* request,
                            proto::DDLStatement* response) override;

    /** Drops a table within the system tables.  Note that this will not update the roots of the
        table, just the metadata to indicate that a drop occurred at the given XID/LSN. */
    grpc::Status DropTable(grpc::ServerContext* context,
                           const proto::DropTableRequest* request,
                           proto::DDLStatement* response) override;

    /** Creates a namespace in the system tables. */
    grpc::Status CreateNamespace(grpc::ServerContext* context,
                                 const proto::NamespaceRequest* request,
                                 proto::DDLStatement* response) override;

    /** Renames a namespace in the system tables. */
    grpc::Status AlterNamespace(grpc::ServerContext* context,
                                const proto::NamespaceRequest* request,
                                proto::DDLStatement* response) override;

    /** Drops a namespace in the system tables. */
    grpc::Status DropNamespace(grpc::ServerContext* context,
                               const proto::NamespaceRequest* request,
                               proto::DDLStatement* response) override;


    /** Creates a user defined type in the system tables. */
    grpc::Status CreateUserType(grpc::ServerContext* context,
                                const proto::UserTypeRequest* request,
                                proto::DDLStatement* response) override;

    /** Renames a user defined type in the system tables. */
    grpc::Status AlterUserType(grpc::ServerContext* context,
                               const proto::UserTypeRequest* request,
                               proto::DDLStatement* response) override;

    /** Drops a user defined type in the system tables. */
    grpc::Status DropUserType(grpc::ServerContext* context,
                              const proto::UserTypeRequest* request,
                              proto::DDLStatement* response) override;

    /** Retrieves the user defined type at a given XID/LSN. */
    grpc::Status GetUserType(grpc::ServerContext* context,
                             const proto::GetUserTypeRequest* request,
                             proto::GetUserTypeResponse* response) override;

    /** Attaches a partition to a table. */
    grpc::Status AttachPartition(grpc::ServerContext* context,
                                 const proto::AttachPartitionRequest* request,
                                 proto::DDLStatement* response) override;

    /** Detaches a partition from a table. */
    grpc::Status DetachPartition(grpc::ServerContext* context,
                                 const proto::DetachPartitionRequest* request,
                                 proto::DDLStatement* response) override;

    /** Updates the roots extents of the indexes of the table as well as the table stats. */
    grpc::Status UpdateRoots(grpc::ServerContext* context,
                             const proto::UpdateRootsRequest* request,
                             google::protobuf::Empty* response) override;

    /** Finalizes the metadata to disk up to at least the provided XID.  Note: may contain
        changes from later XIDs as well, which is fine given we always query the metadata at a
        specific point in the XID/LSN stream. */
    grpc::Status Finalize(grpc::ServerContext* context,
                          const proto::FinalizeRequest* request,
                          google::protobuf::Empty* response) override;

    /** Retrieve the root extents and stats for a given table. */
    grpc::Status GetRoots(grpc::ServerContext* context,
                          const proto::GetRootsRequest* request,
                          proto::GetRootsResponse* response) override;

    /** Retrieve the schema information for a given table at a given XID/LSN. */
    grpc::Status GetSchema(grpc::ServerContext* context,
                           const proto::GetSchemaRequest* request,
                           proto::GetSchemaResponse* response) override;

    /** Retrieve the schema information for a given table at a given XID/LSN along with the
        history of any changes to bring it to a target XID/LSN. */
    grpc::Status GetTargetSchema(grpc::ServerContext* context,
                                 const proto::GetTargetSchemaRequest* request,
                                 proto::GetSchemaResponse* response) override;

    /** Returns NOT_FOUND if the table does not exist at a given XID. */
    grpc::Status Exists(grpc::ServerContext* context,
                        const proto::ExistsRequest* request,
                        google::protobuf::Empty* response) override;

    /** Performs a drop (if needed), create, and update_roots for a given table to swap it's
        newly synced data into place at a given XID.  Returns a JSON array of DDL statements to
        update the FDWs. */
    grpc::Status SwapSyncTable(grpc::ServerContext* context,
                               const proto::SwapSyncTableRequest* request,
                               proto::DDLStatement* response) override;

    /** Reverts the system tables of the provided db to the provided XID.  To avoid trying to write
        data at an already committed XID, this causes the removal of rows with an XID beyond the
        committed XID to be applied into the in-memory mutable tables, which will be committed with
        the next finalize.  This should be safe because the other operations won't return data for
        XIDs beyond the requested XID, and nothing should be requested beyond the committed XID. */
    grpc::Status Revert(grpc::ServerContext* context,
                        const proto::RevertRequest* request,
                        google::protobuf::Empty* response) override;

    /**
     * Get the list of indexes which are to be built/deleted, which will be
     * used to complete the index commits while recovery
     */
    grpc::Status GetUnfinishedIndexesInfo(grpc::ServerContext* context,
            const proto::GetUnfinishedIndexesInfoRequest* request,
            proto::IndexesInfo* response) override;

private:
    Service() = default;
    ~Service() override = default;

    // CACHE FOR NAMES

    /**
     * An in-memory representation of the table_names system table used for caching.
     */
    struct TableCacheRecord {
        uint64_t id;            ///< The table ID.
        uint64_t xid;           ///< The XID at which this entry becomes valid.
        uint64_t lsn;           ///< The LSN at which this entry becomes valid.
        uint64_t namespace_id;  ///< The ID of the schema/namespace of the table.
        std::string name;       ///< The name of the table.
        bool exists;            ///< A flag indicating if the table exists at this point.
        std::optional<uint64_t> parent_table_id;  ///< The parent table ID for partitioned tables (INVALID_TABLE if not set)
        std::optional<std::string> partition_key;  ///< The partition key expression for partitioned tables.
        std::optional<std::string> partition_bound;  ///< The partition bound expression for partitioned tables.
        TableCacheRecord(uint64_t id,
                         uint64_t xid,
                         uint64_t lsn,
                         uint64_t namespace_id,
                         const std::string& name,
                         bool exists,
                         std::optional<uint64_t> parent_table_id,
                         const std::optional<std::string> &partition_key,
                         const std::optional<std::string> &partition_bound)
            : id(id), xid(xid), lsn(lsn), namespace_id(namespace_id), name(name), exists(exists),
              parent_table_id(parent_table_id), partition_key(partition_key), partition_bound(partition_bound)
        {
        }
        TableCacheRecord(uint64_t id,
            uint64_t xid,
            uint64_t lsn,
            uint64_t namespace_id,
            const std::string& name,
            bool exists)
            : id(id), xid(xid), lsn(lsn), namespace_id(namespace_id), name(name), exists(exists)
        {
        }
        TableCacheRecord() = default;
    };
    using TableCacheRecordPtr = std::shared_ptr<TableCacheRecord>;

    /**
     * Retrieve the TableCacheRecord either from the cache or from the system tables if not
     * available.
     * @param db_id The ID of the database.
     * @param table_id The ID of the table.
     * @param xid The XID/LSN at which we are querying.
     */
    TableCacheRecordPtr _get_table_info(uint64_t db_id, uint64_t table_id, const XidLsn& xid);

    /**
     * Stores the TableCacheRecord, performing a write-through in the cache and the system tables.
     * We don't finalize the system tables so they may remain dirty in the StorageCache.  Nothing
     * from the cache is not evicted until _clear_table_info() is called.
     * @param table_info The metadata to update.
     */
    void _set_table_info(uint64_t db_id, TableCacheRecordPtr table_info);

    /**
     * Clears the cache of TableCacheRecord objects.  Called by finalize() once the system tables
     * are all committed to disk.
     */
    void _clear_table_info(uint64_t db_id);

    /**
     * Clears the cache of namespace objects.  Called by finalize().
     * */
    void _clear_namespace_info(uint64_t db_id);

    // CACHE FOR ROOTS / STATS

    /** We use the thrift object response as the cache data for the roots/stats. */
    using RootsCacheRecordPtr = std::shared_ptr<proto::GetRootsResponse>;

    /**
     * Retrieve the RootsCacheRecord either from the cache or from the system tables if not
     * available.
     * @param table_id The ID of the table.
     * @param xid The XID/LSN at which we are querying.
     */
    RootsCacheRecordPtr _get_roots_info(uint64_t db_id, uint64_t table_id, const XidLsn& xid);

    /**
     * Stores the RootsCacheRecord, performing a write-through in the cache and the system tables.
     * We don't finalize the system tables so they may remain dirty in the StorageCache.  Nothing
     * from the cache is not evicted until _clear_roots_info() is called.
     * @param table_info The metadata to update.
     */
    void _set_roots_info(uint64_t db_id,
                         uint64_t table_id,
                         const XidLsn& xid,
                         RootsCacheRecordPtr roots_info);

    /**
     * Clears the cache of TableCacheRecord objects.  Called by finalize() once the system tables
     * are all committed to disk.
     */
    void _clear_roots_info(uint64_t db_id);

    // CACHE FOR SCHEMA

    /** We use the thrift object response as the cache data for the Schema information. */
    using SchemaInfoPtr = std::shared_ptr<proto::GetSchemaResponse>;

    /**
     * Retrieve the SchemaInfo either from the cache or from the system tables if not available.
     * @param table_id The ID of the table.
     * @param access_xid The XID/LSN at which we are querying.
     * @param target_xid The XID/LSN up to which we should return a history of changes from the
     * access_xid.
     */
    SchemaInfoPtr _get_schema_info(uint64_t db_id,
                                   uint64_t table_id,
                                   const XidLsn& access_xid,
                                   const XidLsn& target_xid);

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
                          const std::string& table_name,
                          const std::vector<proto::ColumnHistory>& columns);

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
                            const std::string& schema,
                            const std::string& table_name,
                            const XidLsn& xid);

    /**
     * Clears the cache of schema data.  Called by finalize() once the system tables are
     * all committed to disk.
     */
    void _clear_schema_info(uint64_t db_id);

    /**
     * @brief Helper function to populate index columns for a given index (proto::IndexInfo)
     * @param db_id Database ID
     * @param info proto::IndexInfo - Index info from IndexNames table
     * @param index_xid XidLsn to fetch columns for index
     */
    void _populate_index_columns(uint64_t db_id, proto::IndexInfo& info, XidLsn index_xid);

    /**
     * Helper function to read the full set of columns for a table from the on-disk system tables.
     * @param table_id The table for which we are constructing a schema.
     * @param access_xid The XID/LSN at which we are querying the schema.
     */
    void _read_schema_columns(SchemaInfoPtr info,
                              uint64_t db_id,
                              uint64_t table_id,
                              const XidLsn& access_xid);

    /**
     * Helper function to read the table indexes.
     * @param db_id The datebase ID.
     * @param table_id The table for which we are constructing a schema.
     * @param access_xid The XID/LSN at which we are querying the schema.
     */
    void _read_schema_indexes(SchemaInfoPtr schema_info,
                              uint64_t db_id,
                              uint64_t table_id,
                              const XidLsn& access_xid);

    /**
     * Helper function to apply any in-memory changes to the schema columns that might be
     * required to bring them up-to-date to the provided XID/LSN.
     * @param table_id The table for which we are constructing a schema.
     * @param xid The XID/LSN at which we are querying the schema.
     * @param columns A set of columns already constructed by calling _read_schema_columns()
     *                that will be updated by this function.
     */
    void _apply_schema_cache_history(SchemaInfoPtr info,
                                     uint64_t db_id,
                                     uint64_t table_id,
                                     const XidLsn& xid);

    /**
     * Helper function to apply any in-memory changes to the indexes that might be
     * required to bring them up-to-date to the provided XID/LSN.
     * @param[out] info The schema indexes that will be updated.
     * @param db_id The database ID.
     * @param table_id The table for which we are constructing a schema.
     * @param xid The XID/LSN at which we are querying the schema.
     */
    void _apply_index_cache_history(SchemaInfoPtr info,
                                    uint64_t db_id,
                                    uint64_t table_id,
                                    const XidLsn& xid);

    /**
     * Helper function to read any schema changes recorded between the provided access_xid and
     * target_xid from the on-disk system tables.
     * @param table_id The table for which we are constructing a schema.
     * @param access_xid The XID/LSN at which we are constructing a schema.
     * @param target_xid The XID/LSN up to which we are capturing changes to that schema.
     */
    void _read_schema_history(SchemaInfoPtr info,
                              uint64_t db_id,
                              uint64_t table_id,
                              const XidLsn& access_xid,
                              const XidLsn& target_xid);

    /**
     * Helper function to read any schema changes recorded between the provided access_xid and
     * target_xid from the in-memory cache.  Note: this should never overlap with the on-disk
     * structures since we drop the cache once the system tables are finalized.
     * @param table_id The table for which we are constructing a schema.
     * @param access_xid The XID/LSN at which we are constructing a schema.
     * @param target_xid The XID/LSN up to which we are capturing changes to that schema.
     */
    void _get_schema_cache_history(SchemaInfoPtr info,
                                   uint64_t db_id,
                                   uint64_t table_id,
                                   const XidLsn& access_xid,
                                   const XidLsn& target_xid);

    /**
     * Helper function to generate the history events for the child partition tables.
     * @param request The table request for which we are generating the history events.
     * @param history The history event to be generated.
     *
     * @return A vector of ColumnHistory objects containing the history events for the child partition tables.
     */
    nlohmann::json _generate_partition_updates(const proto::TableRequest& request,
                                               const proto::ColumnHistory& history);

    /**
     * Helper function to extract a change entry for a schema by comparing the old and new
     * schemas from before and after the ALTER TABLE statement.
     * @param old_schema The schema before the alteration.
     * @param new_schema The schema after the alteration.
     * @param xid The XID/LSN at which the alteration occurred.
     * @param ddl The JSON object to which the DDL statement will be added.
     */
    proto::ColumnHistory _generate_update(
        const google::protobuf::RepeatedPtrField<proto::TableColumn>& old_schema,
        const google::protobuf::RepeatedPtrField<proto::TableColumn>& new_schema,
        const XidLsn& xid,
        nlohmann::json& ddl);

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
        {
        }
        NamespaceCacheRecord() = default;
    };
    using NamespaceCacheRecordPtr = std::shared_ptr<NamespaceCacheRecord>;

    /**
     * Read the namespace info from the NamespaceNames system table given it's ID and an
     * XID/LSN.
     */
    NamespaceCacheRecordPtr _get_namespace_info(uint64_t db_id,
                                                uint64_t namespace_id,
                                                const XidLsn& xid,
                                                bool check_exists = true);

    /**
     * Read the namespace info from the NamespaceNames system table given it's name and an
     * XID/LSN.
     */
    NamespaceCacheRecordPtr _get_namespace_info(uint64_t db_id,
                                                const std::string& name,
                                                const XidLsn& xid,
                                                bool check_exists = true);

    /**
     * Cache entry for the user defined types.
     */
    struct UserTypeCacheRecord {
        uint64_t id;
        uint64_t namespace_id;
        std::string name;
        std::string value_json;
        uint8_t type;
        bool exists;

        UserTypeCacheRecord(uint64_t id, std::string_view name, uint64_t ns_id,
                            uint8_t type, std::string_view value_json, bool exists)
            : id(id), namespace_id(ns_id), name(name), value_json(value_json), type(type), exists(exists)
        {
        }
        UserTypeCacheRecord() = default;
    };
    using UserTypeCacheRecordPtr = std::shared_ptr<UserTypeCacheRecord>;

    /**
     * Read the user defined type info from the UserType system table given it's ID and an
     * XID/LSN.
     */
    UserTypeCacheRecordPtr _get_usertype_info(uint64_t db_id,
        uint64_t type_id,
        const XidLsn& xid);

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
     * @param keys The index keys consisting of (column index position, column position in the
     * table).
     */
    void _write_index(const XidLsn& xid,
                      uint64_t db_id,
                      uint64_t tab_id,
                      uint64_t index_id,
                      const std::map<uint32_t, uint32_t>& keys);

    /**
     * Performs a create_index() assuming that the correct locks are already held.
     */
    proto::IndexInfo _create_index(const proto::IndexRequest& request);

    /**
     * Performs a drop_index() assuming that the correct locks are already held.
     * @param xid The XID/LSN at which the transaction occurred.
     * @param db_id The database ID.
     * @param index_id The index ID.
     * @param tid The optional table ID that the index belongs to. When the index is dropped PG
     * trigger provides the index ID but there doesn't seem to be a way to extract the
     * corresponding table ID (see pg_event_trigger_dropped_objects). This should not be a
     * problem because the index ID's are guaranteed to be unique and so the table ID is
     * optional. There is a special case when tid is required. We construct primary indexes in
     * create table using the column attributes and assign the same index ID=constant::PRIMARY_INDEX
     * to all primary indexes and so tid is required for PRIMARY_INDEX.
     * @param index_state Accepts BEING_DELETED or DELETED
     */
    void _drop_index(const XidLsn& xid,
                     uint64_t db_id,
                     uint64_t index_id,
                     std::optional<uint64_t> tid = std::nullopt,
                     sys_tbl::IndexNames::State index_state = sys_tbl::IndexNames::State::DELETED);

    /**
     * Performs a create_table() assuming that the correct locks are already held.
     */
    nlohmann::json _create_table(const proto::TableRequest& request);

    /**
     * Performs a drop_table() assuming that the correct locks are already held.
     */
    nlohmann::json _drop_table(const proto::DropTableRequest& request);

    /**
     * Performs an update_roots() assuming that the correct locks are already held.
     */
    void _update_roots(const proto::UpdateRootsRequest& request);

    /** Performs an set_index_state() assuming that the correct locks are already held.
     */
    bool _set_index_state(const proto::SetIndexStateRequest& request);

    /**
     * @brief Upserts index name entry with the give index info
     * @param db_id            Database ID
     * @param index_info       proto::IndexInfo containing the index details
     * @param xid              XidLsn entry at which index is mutated
     * @param keys             Index keys
     * @param is_primary_index Indicates if its primary or secondary index
     * @return bool indicating the upsert is successful or not
     */
    bool _upsert_index_name(uint64_t db_id, const proto::IndexInfo& index_info, const XidLsn& xid,
            const std::map<uint32_t, uint32_t>& keys, bool is_primary_index=false);

    /** Performs an get_index_info() assuming that the correct locks are already held.
     */
    proto::IndexInfo _get_index_info(const proto::GetIndexInfoRequest& request);


    /**
     * @brief Get the list of indexes which are to be built/deleted for the db,
     * assuming correct locks are already held
     *
     * @param db_id Database ID
     * @return IndexesInfo
     */
    proto::IndexesInfo _get_unfinished_indexes_info(uint64_t db_id);

    /** This doesn't return information about index columns
     */
    std::optional<std::tuple<proto::IndexInfo, uint64_t, XidLsn>> _find_index(
        uint64_t db_id, uint64_t index_id, const XidLsn& xid, std::optional<uint64_t> tid);

    /**
     * Helper for updating the namespace_names table.
     */
    nlohmann::json _mutate_namespace(uint64_t db,
                                     uint64_t ns_id,
                                     std::optional<std::string> name,
                                     const XidLsn& xid,
                                     bool exists);

    std::optional<std::pair<proto::IndexInfo, XidLsn>> _find_cached_index(
        uint64_t db_id, uint64_t index_id, const XidLsn& xid, std::optional<uint64_t> tid);

    /**
     * @brief Helper for updating the usertype table.
     * @return nlohmann::json DDL json for ddl mgr
     */
    nlohmann::json _mutate_usertype(uint64_t db_id,
                                    uint64_t type_id,
                                    const std::string &name,
                                    uint64_t ns_id,
                                    int8_t type,
                                    const std::string &value_json,
                                    const XidLsn xid,
                                    bool active);

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
    void _set_xids(uint64_t db_id, const XidLsn& read_xid, uint64_t write_xid);

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
    using XidLsnToNamespaceInfoMap =
        std::map<XidLsn, NamespaceCacheRecordPtr, std::greater<XidLsn>>;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, XidLsnToNamespaceInfoMap>>
        _namespace_id_cache;

    /**
     * Cache of unapplied namespace changes by namespace name.
     * Stored as a map of DB -> Namespace name -> XID/LSN (in reverse order) -> NamespaceInfo
     */
    std::unordered_map<uint64_t, std::unordered_map<std::string, XidLsnToNamespaceInfoMap>>
        _namespace_name_cache;

    /**
     * Cache of unapplied user defined type changes by type ID.
     * Stored as a map of DB -> Type ID -> XID/LSN (in reverse order) -> UserTypeInfo
     */
    using XidLsnToUserTypeInfoMap =
        std::map<XidLsn, UserTypeCacheRecordPtr, std::greater<XidLsn>>;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, XidLsnToUserTypeInfoMap>>
        _usertype_id_cache;

    /**
     * Cache of unapplied table info changes.
     * Stored as a map of DB -> Table ID -> XID/LSN (in reverse order) -> TableCacheRecord
     */
    using XidLsnToTableInfoMap = std::map<XidLsn, TableCacheRecordPtr, std::greater<XidLsn>>;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, XidLsnToTableInfoMap>> _table_cache;

    /**
     * Cache of unapplied table roots/stats changes.
     * Stored as a map of DB -> Table ID -> XID/LSN (in reverse order) -> RootsCacheRecord
     */
    using XidLsnToRootsInfoMap = std::map<XidLsn, RootsCacheRecordPtr, std::greater<XidLsn>>;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, XidLsnToRootsInfoMap>> _roots_cache;

    /**
     * Cache of unapplied schema changes.
     * Stored as a map of DB -> Table ID -> Column ID -> vector<ColumnHistory> (in ascending XID/LSN
     * order) Using vector because there may be multiple entries at the same XID/LSN on table
     * create.
     */
    using ColumnIdToInfoMap = std::map<uint32_t, std::vector<proto::ColumnHistory>>;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, ColumnIdToInfoMap>> _schema_cache;

    /**
     * Cache of unapplied index changes.
     * Stored as a map of DB -> Table ID -> (vector<IndexCacheItem>) (in ascending XID/LSN order)
     */
    struct IndexCacheItem {
        XidLsn xid;
        proto::IndexInfo info;
    };
    // The map is keyed the index ID.
    // The cache items are in ascending XID order
    using TableIndexCache = std::map<uint64_t, std::vector<IndexCacheItem>>;

    // index cache per table
    using TableId = uint64_t;
    using TableIndexMap = std::map<TableId, TableIndexCache>;

    // index cache per DB
    using DbId = uint64_t;
    std::map<DbId, TableIndexMap> _index_cache;

    // Enum cache per DB
    using EnumId = uint64_t;
    using EnumIndex = float;
    using EnumTypeCache = std::unordered_map<DbId,
        std::unordered_map<EnumIndex,
            std::map<std::string, float>>>;
};
}  // namespace springtail::sys_tbl_mgr
