#pragma once

#include <boost/thread.hpp>

#include <storage/table.hh>

namespace springtail {

    //// SYSTEM TABLE HELPERS

    /**
     * Singleton for managing the table metadata.  Handles table metadata mutations and provides
     * interfaces for retrieving a table object at a given XID as well as a mutable table object for
     * applying data mutations.
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
         * Read the table metadata for the requested table ID.
         */
        TablePtr get_table(uint64_t table_id, uint64_t xid, uint64_t lsn);

        /**
         * Returns the MutableTable interface for the requested table ID.
         */
        MutableTablePtr get_mutable_table(uint64_t table_id, uint64_t xid);

        /**
         * Create a new table.
         */
        void create_table(uint64_t xid, uint64_t lsn, const PgMsgTable &msg);

        /**
         * Alters a table's schema.
         */
        void alter_table(uint64_t xid, uint64_t lsn, const PgMsgTable &msg);

        /**
         * Drops a table.
         */
        void drop_table(uint64_t xid, uint64_t lsn, const PgMsgDropTable &msg);

    protected:
        static TableMgr *_instance; ///< static instance (singleton)
        static boost::mutex _instance_mutex; ///< protects lookup/creation of singleton _instance

        /**
         * @brief Construct a new TableMgr object
         */
        TableMgr();

        /**
         * Construct a system table.
         */
        TablePtr _get_system_table(uint64_t table_id, uint64_t xid);

        /**
         * Construct a mutable system table.
         */
        MutableTablePtr _get_mutable_system_table(uint64_t table_id, uint64_t xid);

        /**
         * Retrieve the namespace and name of the table at a given xid/lsn.
         */
        std::pair<std::string, std::string> _get_table_name(uint64_t table_id, uint64_t xid, uint64_t lsn);

        /**
         * Converts a Postgres type to a Springtail type.
         */
        SchemaType _convert_pg_type(const std::string &pg_type);

    private:
        // singleton; delete copy constructor
        TableMgr(const TableMgr &) = delete;
        void operator=(const TableMgr &) = delete;

        boost::shared_mutex _mutex; ///< Protects access to the table manager.

        ExtentCachePtr _read_cache; ///< A cache of clean extents.
        DataCachePtr _data_cache; ///< The cache of table data extents.
        MutableBTree::PageCachePtr _write_cache; ///< The mutable btree cache.
        std::filesystem::path _table_base; ///< The base directory for individual table directories.
    };
}
