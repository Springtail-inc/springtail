#pragma once

#include <boost/thread.hpp>

#include <common/constants.hh>
#include <common/init.hh>

#include <storage/schema.hh>
#include <storage/xid.hh>

namespace springtail {

    class ExtentType;

    /** Interface for accessing all of the schemas for a specific table.  This includes retrieving
     *  the data schema, primary and secondary index schemas, and data in the write cache -- all at
     *  a specific XID. */
    class SchemaMgr : public Singleton<SchemaMgr>
    {
        friend class Singleton<SchemaMgr>;
    public:

        /**
         * Retrieve the column metadata for a given table at a given XID/LSN.
         * Map from column ID/column position to column metadata.
         */
        std::map<uint32_t, SchemaColumn> get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Retrieve the schema for a given table at a given point in time.
         * @param db_id The database ID of the table.
         * @param table_id The ID of the table being requested.
         * @param extent_xid The XID of the extent being processed.
         * @param target_xid The XID that the query is executing at.
         * @param lsn An optional LSN (logical sequence number) which tells you which schema changes within a given XID to apply up through.
         */
        std::shared_ptr<Schema> get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

        /**
         * Retrieve an ExtentSchema for a given table at a given XID that can be used for writing /
         * updating the extent.  This function assumes we are retrieving the schema of the table's
         * underlying data.
         * @param db_id The database ID of the table.
         * @param table_id The table we need the schema for.
         * @param xid The XID/LSN that we need the schema at. Defaults to the MAX_LSN, providing the
         *            schema at the point after all changes in the XID have been applied.
         */
        std::shared_ptr<ExtentSchema> get_extent_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid,
                                                        bool allow_undefined = false, bool with_internal_row_id = false);

        /**
         * @brief Get the usertype object
         * @param db_id database id
         * @param type_id user defined type id
         * @param xid The XID/LSN we need the schema at.
         * @return std::shared_ptr<UserType>
         */
        UserTypePtr get_usertype(uint64_t db_id, uint64_t type_id, const XidLsn &xid);

    private:
        /**
         * @brief Construct a new SchemaMgr object
         */
        SchemaMgr();

        ~SchemaMgr() override = default;

        /** Helper to convert schema column to map */
        std::map<uint32_t, SchemaColumn> _convert_columns(const std::vector<SchemaColumn> &columns);
    };

} // springtail
