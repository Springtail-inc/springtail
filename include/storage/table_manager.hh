#pragma once

namespace springtail {

    //// SYSTEM TABLE HELPERS

    /**
     * Singleton for managing the table metadata.  Handles table metadata mutations and provides
     * interfaces for retrieving a table object at a given XID as well as a mutable table object for
     * applying data mutations.
     */
    class TableManager {
    public:
        TableManager()
        {
            // XXX initialize the system tables
        }

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

    private:
    };
}
