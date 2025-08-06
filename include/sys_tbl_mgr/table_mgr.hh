#pragma once

#include <boost/thread.hpp>

#include <common/init.hh>
#include <sys_tbl_mgr/table.hh>

namespace springtail {

namespace table_helpers {

/** Helper for constructing table paths. */
std::filesystem::path get_table_dir(const std::filesystem::path &base,
                                    uint64_t db_id,
                                    uint64_t table_id,
                                    uint64_t snapshot_xid);

}

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
    class TableMgr : public Singleton<TableMgr>
    {
        friend class Singleton<TableMgr>;
    public:
        /**
         * Read the table metadata for the requested table ID.  Note that Table objects's are always
         * constructed at lsn == MAX_LSN within the provided xid.
         */
        TablePtr get_table(uint64_t db_id, uint64_t table_id, uint64_t xid);

        /**
         * Returns a boolean indicating if the table exists at a given xid/lsn.
         */
        bool exists(uint64_t db_id, uint64_t table_id, uint64_t xid, uint64_t lsn=constant::MAX_LSN);

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

        // Functions for managing system metadata

        /**
         * Create a new table.
         */
        void create_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg);

        /**
         * Alters a table's schema.
         */
        void alter_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg);

        /**
         * Drops a table.
         */
        void drop_table(uint64_t db_id, const XidLsn &xid, const PgMsgDropTable &msg);

        /**
         * Update the roots of a table.
         */
        void update_roots(uint64_t db_id, uint64_t table_id, uint64_t target_xid,
                          const TableMetadata &metadata);

        /**
         * Truncates the table, removing the callback of any mutated pages in the cache, clearing
         * all of the indexes, and marking the roots to be cleared in the system tables.
         */
        void truncate_table(MutableTablePtr table);

        /**
         * Finalize all outstanding system metadata mutations.
         */
        void finalize_metadata(uint64_t db_id, uint64_t xid);

        /**
         * @brief Get table data dir for a table_id
         *
         * @param db_id    Database ID
         * @param table_id Table ID
         * @param xid      XID for which table data file is located
         * @return Table data dir path
         */
        std::filesystem::path get_table_data_dir(uint64_t db_id, uint64_t table_id, uint64_t xid);

        /**
         * Retrieves the schema for the table at a given XID.
         */
        ExtentSchemaPtr
        get_extent_schema(TablePtr table);

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        SchemaPtr
        get_schema(TablePtr table, uint64_t extent_xid);

    private:
        /**
         * @brief Construct a new TableMgr object
         */
        TableMgr();
        ~TableMgr() override = default;

        boost::shared_mutex _mutex; ///< Protects access to the table manager.
        std::filesystem::path _table_base; ///< The base directory for individual table directories.
    };

} // springtail
