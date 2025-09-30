#pragma once

#include <common/init.hh>
#include <sys_tbl_mgr/table_mgr_base.hh>

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
        virtual TablePtr get_table(uint64_t db_id, uint64_t table_id, uint64_t xid) override;

        /**
         * Returns the MutableTable interface for the requested table ID.
         */
        MutableTablePtr get_mutable_table(uint64_t db_id, uint64_t table_id, uint64_t access_xid, uint64_t target_xid, bool for_gc = false);

        /**
         * Returns a MutableTable that can be used to populate a new snapshot of the given table.
         * @param db_id The database of the table.
         * @param table_id The OID of the table.
         * @param snapshot_xid The XID at which the snapshot is being captured.  Extents will be
         *                     written at this XID, however, the data itself may not be made
         *                     available until a later stable XID.
         * @param schema The ExtentSchema of the table.
         */
        MutableTablePtr get_snapshot_table(uint64_t db_id, uint64_t table_id, uint64_t snapshot_xid, ExtentSchemaPtr schema, const std::vector<Index>& secondary_keys);

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
                          const XidLsn &xid, bool allow_undefined = false) override;

    private:
        /**
         * @brief Construct a new TableMgr object
         */
        TableMgr() : Singleton<TableMgr>(ServiceId::TableMgrId) {}
        ~TableMgr() override = default;
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
                  ExtentSchemaPtr schema) :
            Table(db_id, table_id, xid, table_base, primary_key, secondary, metadata, schema) {}

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
                         bool for_gc = false) :
            MutableTable(db_id, table_id, access_xid, target_xid, table_base, primary_key,
                         secondary, metadata, schema, for_gc) {}

        /**
         * Truncates the table, removing the callback of any mutated pages in the cache, clearing
         * all of the indexes, and marking the roots to be cleared in the system tables.
         */
        virtual void
        truncate() override;
    };

} // springtail
