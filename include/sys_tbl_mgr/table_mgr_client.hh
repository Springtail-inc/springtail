#pragma once

#include <common/init.hh>
#include <sys_tbl_mgr/table_mgr_base.hh>

namespace springtail {
    class TableMgrClient : public Singleton<TableMgrClient>, public TableMgrBase
    {
        friend class Singleton<TableMgrClient>;
    public:
        /**
         * Read the table metadata for the requested table ID.  Note that Table objects's are always
         * constructed at lsn == MAX_LSN within the provided xid.
         */
        virtual TablePtr
        get_table(uint64_t db_id, uint64_t table_id, uint64_t xid) override;

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
         * @brief Construct a new Table Mgr Client singleton object
         *
         */
        TableMgrClient() : Singleton<TableMgrClient>(ServiceId::TableMgrClientId) {}
        ~TableMgrClient() override = default;
    };

    /**
     * @brief This class is for representing user tables on the client side. It will use TableMgrClient
     *      to get schema and extent schema. In turn TblMgrClient will use system table manager client calls
     *      to obtain information for constructing the table.
     *
     */
    class UserClientTable : public Table, public std::enable_shared_from_this<UserClientTable> {
    public:
        using Table::schema;

        /**
         * UserTable constructor.
         */
        UserClientTable(uint64_t db_id,
                        uint64_t table_id,
                        uint64_t xid,
                        const std::filesystem::path &table_base,
                        const std::vector<std::string> &primary_key,
                        const std::vector<Index> &secondary,
                        const TableMetadataPtr metadata,
                        ExtentSchemaPtr schema) :
            Table(db_id, table_id, xid, table_base, primary_key, secondary, metadata, schema) {}

        /**
         * Retrieves the schema for the table at a given XID.
         */
        virtual ExtentSchemaPtr extent_schema() const override
        {
            return TableMgrClient::get_instance()->get_extent_schema(_db_id, _id, XidLsn(_xid));
        }

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        virtual SchemaPtr schema(uint64_t extent_xid) const override
        {
            return TableMgrClient::get_instance()->get_schema(_db_id, _id, XidLsn(extent_xid), XidLsn(_xid));
        }

    };

} // springtail