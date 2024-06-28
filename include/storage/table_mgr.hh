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
        MutableTablePtr get_mutable_table(uint64_t table_id, uint64_t access_xid, uint64_t target_xid, bool for_gc = false);

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

        /**
         * Update the roots of a table.
         */
        void update_roots(uint64_t table_id, uint64_t access_xid, uint64_t target_xid, const std::vector<uint64_t> &roots);

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
        TablePtr _get_system_table(uint64_t table_id, uint64_t xid);

        /**
         * Construct a mutable system table.
         */
        MutableTablePtr _get_mutable_system_table(uint64_t table_id, uint64_t access_xid, uint64_t target_xid);

        /**
         * Retrieve the namespace and name of the table at a given xid/lsn.
         */
        std::pair<std::string, std::string> _get_table_name(uint64_t table_id, uint64_t xid, uint64_t lsn);

        /**
         * Converts a Postgres type to a Springtail type.
         */
        SchemaType _convert_pg_type(const std::string &pg_type);

        /**
         * Find the roots of a given table from the TableRoots system table.
         */
        std::vector<uint64_t> _find_roots(uint64_t table_id, uint64_t xid);

    private:
        // singleton; delete copy constructor
        TableMgr(const TableMgr &) = delete;
        void operator=(const TableMgr &) = delete;

        boost::shared_mutex _mutex; ///< Protects access to the table manager.
        std::filesystem::path _table_base; ///< The base directory for individual table directories.
    };
}
