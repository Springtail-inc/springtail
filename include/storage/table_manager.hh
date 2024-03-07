#pragma once

namespace springtail {

    class TableManager {
    public:
        TableManager()
        { }

        /**
         * Read the table metadata for the requested table ID.
         */
        TablePtr get_table(uint64_t table_id);

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
    };

}
