#include <storage/table_manager.hh>

namespace springtail {
    TablePtr
    TableManager::get_table(uint64_t table_id)
    {
        // check the system tables

        // check the table cache

        // read the table metadata from the appropriate system table
    }

    void
    TableManager::create_table(uint64_t xid,
                               uint64_t lsn,
                               const PgMsgTable &msg)
    {
        // get the appropriate system tables

        // check if the table already exists at the given XID+LSN

        // if not, create the table details within the appropriate system tables

        // 1) add row to "tables" with the table metadata, starting at XID+LSN, no ending, or ending when the next entry starts?  Do we want to keep end in the entries?
        // 2) add a row to "schemas" for each column
        // 3) if there's a primary key, add a row to "primary_indexes" for each key column
    }

    void
    TableManager::alter_table(uint64_t xid,
                              uint64_t lsn,
                              const PgMsgTable &msg)
    {
        // get the appropriate system tables

        // confirm the table exists at the given XID+LSN

        // update the modified table details within the appropriate system tables

        // 1) if the table name is changed, add a row to the "tables" with the new name
        // 2) determine the set of changes between the provided schema and the prior schema
        // 3) add a row to the "schemas" for the modified column
        // 4) add a row to the "schemas_history" for the modification

        // 5) XXX if there's a primary key change, what do we do?  we'll need to re-build the entire table
    }

    void
    TableManager::drop_table(uint64_t xid,
                             uint64_t lsn,
                             const PgMsgDropTable &msg)
    {
        // confirm the table exists at the given XID+LSN

        // update the system tables to represent that the table was dropped from XID+LSN

        // 1) update the "tables" with a drop entry
        //    note: the full GC should evict these entries once the data has been brought forward
        //          and we can assume the table_id won't be re-used until that operation is complete
    }
}
