#pragma once

#include <common/init.hh>
#include <sys_tbl_mgr/table_mgr_base.hh>
#include <sys_tbl_mgr/mutable_table.hh>

namespace springtail {

    /**
     * Singleton for managing the table metadata.  Handles table metadata mutations and provides
     * interfaces for retrieving a table object at a given XID as well as a mutable table object for
     * applying data mutations.
     *
     * To make sure that the system tables are accurate for use in GC-2 or roll-forward, we must
     * ensure that all system table mutations (aside from table stats) are performed and finalized
     * to the target XID as part of GC-1.  Then in GC-2 the system tables can be accessed via the
     * read-only Table interfaces using the target XID.
     */
    class TableMgr : public Singleton<TableMgr>, public TableMgrBase
    {
        friend class Singleton<TableMgr>;
    public:
        /**
         * Read the table metadata for the requested table ID.  Note that Table objects's are always
         * constructed at lsn == MAX_LSN within the provided xid.
         */
        virtual TablePtr get_table(uint64_t db_id, uint64_t table_id, uint64_t xid, const ExtensionCallback &extension_callback = {}) override;

        /**
         * Returns the MutableTable interface for the requested table ID.
         */
        MutableTablePtr get_mutable_table(uint64_t db_id, uint64_t table_id, uint64_t access_xid, uint64_t target_xid, const ExtensionCallback &extension_callback = {});

        /**
         * Returns a MutableTable that can be used to populate a new snapshot of the given table.
         * @param db_id The database of the table.
         * @param table_id The OID of the table.
         * @param snapshot_xid The XID at which the snapshot is being captured.  Extents will be
         *                     written at this XID, however, the data itself may not be made
         *                     available until a later stable XID.
         * @param schema The ExtentSchema of the table.
         */
        MutableTablePtr get_snapshot_table(uint64_t db_id, uint64_t table_id, uint64_t snapshot_xid, ExtentSchemaPtr schema, const std::vector<Index>& secondary_keys, const ExtensionCallback &extension_callback = {}, const OpClassHandler &opclass_handler = {});

        /**
         * @brief Get table data dir for a table_id
         *
         * @param db_id    Database ID
         * @param table_id Table ID
         * @param xid      XID for which table data file is located
         * @return Table data dir path
         */
        std::optional<std::filesystem::path> get_table_data_dir(uint64_t db_id, uint64_t table_id, uint64_t xid);

        /**
         * Retrieve the column metadata for a given table at a given XID/LSN.
         * Map from column ID/column position to column metadata.
         */
        virtual std::map<uint32_t, SchemaColumn>
        get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid) override;

        /**
         * Retrieves the schema for the table at a given XID.
         */
        virtual std::shared_ptr<Schema>
        get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid) override;

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        virtual std::shared_ptr<ExtentSchema>
        get_extent_schema(uint64_t db_id, uint64_t table_id,
                          const XidLsn &xid, const ExtensionCallback &extension_callback = {},
                          bool allow_undefined = false, bool include_internal_row_id = true) override;

        /**
         * Get an index schema for a secondary index at the provided XID.
         * This creates a schema suitable for index extents (includes __extent_id and __row_id fields).
         */
        std::shared_ptr<ExtentSchema>
        get_index_schema(uint64_t db_id, uint64_t table_id, uint64_t index_id,
                        const std::vector<uint32_t>& index_columns, const XidLsn &xid,
                        const ExtensionCallback &extension_callback = {});

        /**
         * Get a PgLogReader batch schema at the provided XID (cached version).
         * This retrieves/caches a schema suitable for batching mutations (includes __springtail_op and __springtail_lsn fields).
         */
        std::shared_ptr<ExtentSchema>
        get_pg_log_batch_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid,
                                const ExtensionCallback &extension_callback = {});

        /**
         * Create a PgLogReader batch schema from a provided base schema (non-cached version).
         * This creates a schema suitable for batching mutations without caching it.
         * Use this for CREATE/ALTER TABLE scenarios where the base schema doesn't exist in the cache yet.
         */
        std::shared_ptr<ExtentSchema>
        create_pg_log_batch_schema(ExtentSchemaPtr base_schema,
                                   const ExtensionCallback &extension_callback = {});

        /**
         * Get the look-aside index schema.
         * This returns a singleton schema for the look-aside index that maps __internal_row_id to (__extent_id, __row_id).
         * The schema is the same for all tables since it's a fixed mapping.
         */
        std::shared_ptr<ExtentSchema>
        get_look_aside_schema();

        /**
         * Get the roots schema.
         * This returns a singleton schema for table roots metadata.
         * The schema is the same for all tables since it's a fixed structure.
         */
        std::shared_ptr<ExtentSchema>
        get_roots_schema();

    private:
        /**
         * @brief Construct a new TableMgr object
         */
        TableMgr() : Singleton<TableMgr>(ServiceId::TableMgrId) {}
        ~TableMgr() override {
            // Clear the schema cache before destruction to avoid static destruction order issues
            // No lock needed in destructor - no other threads can be accessing this object
            _extent_schema_cache._entries.clear();
            _extent_schema_cache._lru_list.clear();
            _extent_schema_cache._schema_change_xids.clear();
        }
    };

    /**
     * @brief This class is for user non-mutable table access on the server side. It uses table manager functions
     *      to get schema and extended schema, that access the server directly, bypassing client API calls.
     *
     */
    class UserTable : public Table, public std::enable_shared_from_this<UserTable> {
    public:
        using Table::schema;

        /**
         * UserTable constructor.
         */
        UserTable(uint64_t db_id,
                  uint64_t table_id,
                  uint64_t xid,
                  const std::filesystem::path &table_base,
                  const std::vector<std::string> &primary_key,
                  const std::vector<Index> &secondary,
                  const TableMetadata &metadata,
                  ExtentSchemaPtr schema,
                  const ExtensionCallback &extension_callback = {}) :
            Table(db_id, table_id, xid, table_base, primary_key, secondary, metadata, schema, extension_callback) {}

        /**
         * Retrieves the schema for the table at a given XID.
         */
        virtual ExtentSchemaPtr extent_schema() const override
        {
            return TableMgr::get_instance()->get_extent_schema(_db_id, _id, XidLsn(_xid));
        }

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        virtual SchemaPtr schema(uint64_t extent_xid) const override
        {
            return TableMgr::get_instance()->get_schema(_db_id, _id, XidLsn(extent_xid), XidLsn(_xid));
        }

    };

    /**
     * @brief This class is for user mutable table access on the server side.
     *
     */
    class UserMutableTable: public MutableTable,
                            public std::enable_shared_from_this<UserMutableTable> {

    public:
        /**
         * System mutable table constructor.
         */
        UserMutableTable(uint64_t db_id,
                         uint64_t table_id,
                         uint64_t access_xid,
                         uint64_t target_xid,
                         const std::filesystem::path &table_base,
                         const std::vector<std::string> &primary_key,
                         const std::vector<Index> &secondary,
                         const TableMetadata &metadata,
                         ExtentSchemaPtr schema,
                         ExtentSchemaPtr schema_without_row_id,
                         const ExtensionCallback &extension_callback = {},
                         const OpClassHandler &opclass_handler = {},
                         bool bypass_schema_cache = false) :
            MutableTable(db_id, table_id, access_xid, target_xid, table_base, primary_key,
                         secondary, metadata, schema, schema_without_row_id, extension_callback, opclass_handler, bypass_schema_cache) {}

        /**
         * Truncates the table, removing the callback of any mutated pages in the cache, clearing
         * all of the indexes, and marking the roots to be cleared in the system tables.
         */
        virtual void
        truncate() override;
    };

} // springtail
