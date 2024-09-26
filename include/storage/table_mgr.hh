#pragma once

#include <boost/thread.hpp>

#include <storage/table.hh>

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
    class TableMgr {
    public:
        /**
         * @brief getInstance() of singleton TableMgr; create if it doesn't exist.
         * @return instance of TableMgr
         */
        static TableMgr *get_instance();

        /**
         * @brief Shutdown the TableMgr singleton.
         */
        static void shutdown();

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
        MutableTablePtr get_snapshot_table(uint64_t db_id, uint64_t table_id, uint64_t snapshot_xid, ExtentSchemaPtr schema);

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
         * Finalize all outstanding system metadata mutations.
         */
        void finalize_metadata(uint64_t db_id, uint64_t xid);

    private:
        static TableMgr *_instance; ///< static instance (singleton)
        static boost::mutex _instance_mutex; ///< protects lookup/creation of singleton _instance

        /**
         * @brief Construct a new TableMgr object
         */
        TableMgr();

        /**
         * Construct a system table.
         */
        TablePtr _get_system_table(uint64_t db_id, uint64_t table_id, uint64_t xid);

        /**
         * Construct a mutable system table.
         */
        MutableTablePtr _get_mutable_system_table(uint64_t db_id, uint64_t table_id, uint64_t access_xid, uint64_t target_xid);

    private:
        // singleton; delete copy constructor
        TableMgr(const TableMgr &) = delete;
        void operator=(const TableMgr &) = delete;

        boost::shared_mutex _mutex; ///< Protects access to the table manager.
        std::filesystem::path _table_base; ///< The base directory for individual table directories.
    };
}
