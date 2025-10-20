#pragma once

#include <common/init.hh>
#include <common/singleton.hh>

#include <grpc/grpc_server_manager.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <storage/xid.hh>
#include <sys_tbl_mgr/schema_cache.hh>
#include <sys_tbl_mgr/service.hh>

namespace springtail::sys_tbl_mgr {

class Server final : public Singleton<Server>
{
    friend class Singleton<Server>;
    friend class Service;
public:

    /**
     * @brief Create table API cal
     */
    std::string
    create_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg);

    /**
     * @brief Alter table API cal
     */
    std::string
    alter_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg);

    /**
     * @brief Drop table API cal
     */
    std::string
    drop_table(uint64_t db_id, const XidLsn &xid, const PgMsgDropTable &msg);

    /**
     * @brief Create namespace API cal
     */
    std::string
    create_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg);

    /**
     * @brief Alter namespace API cal
     */
    std::string
    alter_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg);

    /**
     * @brief Drop namespace API cal
     */
    std::string
    drop_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg);

    /**
     * @brief Create user type API cal
     */
    std::string
    create_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg);

    /**
     * @brief Alter user type API cal
     */
    std::string
    alter_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg);

    /**
     * @brief Drop user type API cal
     */
    std::string
    drop_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg);

    /**
     * @brief Attach partition API cal
     */
    std::string
    attach_partition(uint64_t db_id, const XidLsn &xid, const PgMsgAttachPartition &msg);

    /**
     * @brief Detach partition API cal
     */
    std::string
    detach_partition(uint64_t db_id, const XidLsn &xid, const PgMsgDetachPartition &msg);

    /**
     * @brief Create index API cal
     */
    proto::IndexProcessRequest
    create_index(uint64_t db_id, const XidLsn &xid, const PgMsgIndex &msg, sys_tbl::IndexNames::State state);

    /**
     * @brief Set index state API cal
     */
    void
    set_index_state(uint64_t db_id, const XidLsn &xid, uint64_t table_id, uint64_t index_id, sys_tbl::IndexNames::State state);

    /**
     * @brief Get index info API cal
     */
    proto::IndexInfo
    get_index_info(uint64_t db_id, uint64_t index_id, const XidLsn &xid, std::optional<uint64_t> tid = std::nullopt);

    /**
     * @brief Get unfinished indexes info API cal
     */
    proto::IndexesInfo
    get_unfinished_indexes_info(uint64_t db_id);

    /**
     * @brief Drop index API cal
     */
    proto::IndexProcessRequest
    drop_index(uint64_t db_id, const XidLsn &xid, const PgMsgDropIndex &msg);

    /**
     * @brief Update roots API cal
     */
    void
    update_roots(uint64_t db_id, uint64_t table_id, uint64_t xid, const TableMetadata &metadata);

    /**
     * @brief Finalize API cal
     */
    void
    finalize(uint64_t db_id, uint64_t xid);

    /**
     * @brief Revert API cal
     */
    void
    revert(uint64_t db_id, uint64_t xid);

    /**
     * @brief Get roots API cal
     */
    TableMetadataPtr
    get_roots(uint64_t db_id, uint64_t table_id, uint64_t xid);

    /**
     * @brief Get schema API cal
     */
    std::shared_ptr<const SchemaMetadata>
    get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

    /**
     * @brief Get target schema API cal
     */
    SchemaMetadataPtr
    get_target_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

    /**
     * @brief Table exists API cal
     */
    bool
    exists(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

    /**
     * @brief Swap and sync table  API cal
     */
    std::string
    swap_sync_table(const proto::NamespaceRequest &namespace_req,
                    const proto::TableRequest &create_req,
                    const std::vector<proto::IndexRequest> &index_reqs,
                    const proto::UpdateRootsRequest &roots_req);

    /**
     * @brief Get user type API cal
     */
    std::shared_ptr<UserType>
    get_usertype(uint64_t db_id, uint64_t type_id, const XidLsn &xid);

    /**
     * Invalidates the schema entry for a given table from a given XID/LSN
     */
    void invalidate_table(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

    /**
     * Invalidates all of the tables of a db in the schema cache.
     */
    void invalidate_db(uint64_t db_id, const XidLsn &xid);

    /**
     * Remove the data stored by the server for the given database id
     */
    void remove_db(uint64_t db_id);

private:
    Server();
    ~Server() override = default;

    std::unique_ptr<Service> _service;              ///< service object that can receive and process client API calls
    GrpcServerManager _grpc_server_manager;         ///< GRPC manager

    /**
     * @brief Shutdown function called by the singleton pattern
     *
     */
    void _internal_shutdown() override;

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
        bool rls_enabled;       ///< A flag indicating if RLS is enabled for this table.
        bool rls_forced;        ///< A flag indicating if RLS is forced for this table.
        bool exists;            ///< A flag indicating if the table exists at this point.
        std::optional<uint64_t> parent_table_id;  ///< The parent table ID for partitioned tables (INVALID_TABLE if not set)
        std::optional<std::string> partition_key;  ///< The partition key expression for partitioned tables.
        std::optional<std::string> partition_bound;  ///< The partition bound expression for partitioned tables.
        TableCacheRecord(uint64_t id,
                         uint64_t xid,
                         uint64_t lsn,
                         uint64_t namespace_id,
                         const std::string& name,
                         std::optional<uint64_t> parent_table_id,
                         const std::optional<std::string> &partition_key,
                         const std::optional<std::string> &partition_bound,
                         bool rls_enabled,
                         bool rls_forced,
                         bool exists)
            : id(id), xid(xid), lsn(lsn), namespace_id(namespace_id), name(name),
              rls_enabled(rls_enabled), rls_forced(rls_forced), exists(exists),
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
    proto::IndexInfo _create_index(const proto::IndexRequest& request, bool &created);

    /**
     * @brief Check the columns of an index to ensure they have suppoerted type.
     *
     * @param db_id - database id
     * @param index_info - index information
     * @param keys - list of index keys
     * @param xid - transaction id
     * @return true - all index keys have supported type
     * @return false - some index keys have unsupported type
     */
    bool _check_index_columns(uint64_t db_id, const proto::IndexInfo & index_info, const std::map<uint32_t, uint32_t> & keys, XidLsn xid);

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
                     sys_tbl::IndexNames::State index_state = sys_tbl::IndexNames::State::DELETED,
                     std::optional<std::reference_wrapper<proto::IndexInfo>> dropped_index_info_ref = std::nullopt);

    /**
     * Performs a create_table() assuming that the correct locks are already held.
     */
    nlohmann::json _create_table(const proto::TableRequest& request);

    /**
     * Performs a drop_table() assuming that the correct locks are already held.
     *
     * @param request   Drop table request
     * @param is_resync To indicate if table drop is due to resync
     *
     * @return ddl to be applied
     */
    nlohmann::json _drop_table(const proto::DropTableRequest& request, bool is_resync=false);

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
     * @brief Helper function to extract the modified partition details to either attach or detach the
     * partition to the parent table.
     * @param db_id The database ID.
     * @param xid The XID/LSN at which the alteration occurred.
     * @param table_id The ID of the parent table.
     * @param partition_data The partition data.
     * @param partition_map The partition map.
     * @param is_attached Whether the partition is being attached or detached.
     * @return std::vector<uint64_t> The modified partition details.
     */
    std::vector<uint64_t> _get_modified_partition_details(uint64_t db_id,
                                const XidLsn &xid,
                                uint64_t table_id,
                                const google::protobuf::RepeatedPtrField<proto::PartitionData> &partition_data,
                                std::unordered_map<uint64_t, std::pair<std::string, std::string>> *partition_map,
                                bool is_attached);

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

    /** Cache for Schema objects. */
    std::shared_ptr<SchemaCache> _schema_object_cache;
};

}  // namespace springtail::sys_tbl_mgr
