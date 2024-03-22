#pragma once

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
         * Read the table metadata for the requested table ID.
         */
        TablePtr get_table(uint64_t table_id, uint64_t xid, uint64_t lsn);

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
         * @brief Destroy the TableMgr object
         */
        ~TableMgr(){};

    private:

        // singleton; delete copy constructor
        TableMgr(const TableMgr &) = delete;
        void operator=(const TableMgr &) = delete;
    };
}
