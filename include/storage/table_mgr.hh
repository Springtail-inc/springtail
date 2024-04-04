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
         * Returns the MutableTable interface for the requested table ID.  Always operates on the
         * most recent data, so doesn't require an XID.
         */
        MutableTablePtr get_mutable_table(uint64_t table_id);

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
        TableMgr()
        { }

        /**
         * @brief Destroy the TableMgr object
         */
        ~TableMgr(){};

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

        boost::shared_mutex _mutex;
        std::map<uint64_t, TablePtr> _system_tables;
        std::map<uint64_t, MutableTablePtr> _mutable_system_tables;
        std::shared_ptr<ObjectCache<uint64_t, Table>> _table_cache;
        std::shared_ptr<IOHandle> _handle;
    };
}
