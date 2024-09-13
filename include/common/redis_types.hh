#pragma once

namespace springtail::redis {

    ///// Config DB paths accessed via Properties::

    /**
     * Config for database instance: var db_instance_id (hashset)
     * args: db_instance_id
     */
    static constexpr char DB_INSTANCE_CONFIG[] = "instance_config:{}";

    /**
     * Config for database within instance: var db_instance_id (hashset)
     * args: db_instance_id
     */
    static constexpr char DB_CONFIG[] = "db_config:{}";

    /**
     * Config for FDWs args: <db instance id>
     * args: db_instance_id
     */
    static constexpr char HASH_FDW[] = "fdw:{}";

    /**
     * State hash for database instance
     * args: db_instance_id
     */
    static constexpr char DB_INSTANCE_STATE[] = "instance_state:{}";

    //// Data DB (1) accessed via RedisClient::

    //// Postgres redis key prefixes.  Value defs in: pg_log_mgr/pg_redis_xact.hh

    /**
     * Queue between pg log mgr and gc for transaction processing
     * args: <db_instance_id>
     */
    static constexpr char QUEUE_PG_TRANSACTIONS[] = "queue:pg_xact:{}";

    /**
     * Maintains a mapping from each XID to the Table OIDs it mutates.
     * Populated by the PgLogMgr and utilized by the gc::LogParser::Backlog.
     * There is one sorted set maintained per database.
     *
     * args: <db_instance_id>, <db_id>
     */
    static constexpr char SET_PG_OID_XIDS[] = "set:pg_xid_oids:{}:{}";

    /**
     * Queue used within the GC-1 to communicate between the LogParser::Dispatcher and the
     * LogParser::Reader.
     *
     * args: <db_instance_id>
     */
    static constexpr char QUEUE_GC1_READER[] = "queue:gc1_reader:{}";

    /**
     * Queue between the GC-1 and GC-2.  Passes an XidReady object.
     * args: <db_instance_id>
     */
    static constexpr char QUEUE_GC_XID_READY[] = "queue:gc_xid_ready:{}";

    //// For RedisDDL

    /**
     * Queue of DDL operations for a given XID
     * args: <db_instance_id>, <db_id>, <xid>
     */
    static constexpr char QUEUE_DDL_XID[] = "queue:ddl:xid:{}:{}:{}";

    /**
     * Queue of DDL changes for the FDW to process.
     * args: <db_instance_id>, <fdw_id>
     */
    static constexpr char QUEUE_DDL_FDW[] = "queue:ddl:fdw:{}:{}";

    /**
     * Hash set of schema_xids per FDW
     * hash key: <fdw_id>, value: <schema_xid>
     * args: <db_instance_id>
     */
    static constexpr char HASH_DDL_FDW[] = "hash:ddl:fdw:{}";

    //// For Log Mgr Table Sync

    /**
     * Table sync hash set, key is the table OID/TID, value is 'xmin:xmax:xid,xid,xid...'
     * args: <db_instance_id>, <db_id>
     */
    static constexpr char HASH_SYNC_TABLE_STATE[] = "set:sync_table_state:{}:{}";

    /**
     * Queue for table sync requests; value is the table OID/TID
     * args: <db_instance_id>, <db_id>
     */
    static constexpr char QUEUE_SYNC_TABLES[] = "queue:sync_tables:{}:{}";

    /**
     * Key / value for log mgr resync point; value is filename:offset
     * args: <db_instance_id>, <db_id>
     */
    static constexpr char STRING_LOG_RESYNC[] = "string:log_resync:{}:{}";
}
