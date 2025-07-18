#pragma once

namespace springtail::redis {

    ///// Config DB paths accessed via Properties (DB=0)::

    /**
     * Config for database instance: var db_instance_id (hashset)
     * args: db_instance_id
     */
    static constexpr char DB_INSTANCE_CONFIG[] = "{}:instance_config";

    /**
     * Config for database within instance: var db_instance_id (hashset)
     * args: db_instance_id
     */
    static constexpr char DB_CONFIG[] = "{}:db_config";

    /**
     * Config for FDWs args: <db instance id>
     * args: db_instance_id
     */
    static constexpr char HASH_FDW[] = "{}:fdw";

    /**
     * Set of FDW IDs
     * args: <db instance id>
     */
    static constexpr char SET_FDW_IDS[] = "{}:fdw_ids";

    /**
     * State hash for database instance
     * args: db_instance_id
     */
    static constexpr char DB_INSTANCE_STATE[] = "{}:instance_state";

    //// Data DB (1) accessed via RedisClient::

    //// Postgres redis key prefixes.  Value defs in: pg_log_mgr/pg_redis_xact.hh

    //// For RedisDDL

    /**
     * Queue of DDL operations for a given XID coming out of the GC1 LogParser
     * args: <db_instance_id>, <db_id>, <xid>
     */
    static constexpr char QUEUE_DDL_XID[] = "{}:queue:ddl:xid:{}:{}";

    /**
     * Queue of DDL index operations for a given XID coming out of the GC1 LogParser
     * args: <db_instance_id>, <db_id>, <xid>
     */
    static constexpr char QUEUE_INDEX_DDL_XID[] = "{}:queue:index:ddl:xid:{}:{}";

    /**
     * HASH of pre-commit DDL operations.  Stored with a key of "db_id:xid"
     * args: <db_instance_id>
     */
    static constexpr char HASH_DDL_PRECOMMIT[] = "{}:hash:ddl:pc";

    /**
     * Queue of DDL changes for the FDW to process coming out of the GC2 Committer
     * args: <db_instance_id>, <fdw_id>
     */
    static constexpr char QUEUE_DDL_FDW[] = "{}:queue:ddl:fdw:{}";

    /**
     * Hash set of schema_xids per FDW
     * args: <db_instance_id>
     * hash key: <db_id>:<fdw_id>, value: <schema_xid>
     */
    static constexpr char HASH_DDL_FDW[] = "{}:hash:ddl:fdw";

    //// For Log Mgr Table Sync

    /**
     * Queue for table sync requests; value is the table OID/TID
     * args: <db_instance_id>, <db_id>
     */
    static constexpr char QUEUE_SYNC_TABLES[] = "{}:queue:sync_tables:{}";

    /**
     * Key / value for log mgr resync point; value is filename:offset
     * args: <db_instance_id>, <db_id>
     */
    static constexpr char STRING_LOG_RESYNC[] = "{}:string:log_resync:{}";

    /**
     * Hash table holding the table operations (drop, create and update_roots) for each table sync.
     * Each entry in the hash is stored as a JSON array of JSON objects.
     * args: <db_instance_id>, <db_id>
     */
    static constexpr char HASH_SYNC_TABLE_OPS[] = "{}:hash:sync_table_ops:{}";

    //// For coordinator

    /**
     * Hash set for tracking liveness of a daemon
     * Used by coordinator.py
     * args: <db_instance_id>
     * key: <daemon_type>:<thread_id>, value: <timestamp>
     */
    static constexpr char HASH_LIVENESS[] = "{}:hash:liveness";

    /**
     * Pub/sub for notifying the coordinator of a dead daemon
     * Used by coordinator.py
     * args: <db_instance_id>
     * value: <daemon_type>:<thread_id>
     */
    static constexpr char PUBSUB_LIVENESS_NOTIFY[] = "{}:pubsub:liveness_notify";

    //// For Redis table cache
    /**
     * Set holding schema.table names for each db id
     * args: <db_instance_id>, <db_id>
     * value: quoted(schema).quoted(table)
     */
    static constexpr char SET_DB_TABLES[] = "{}:set:db_tables:{}";

    /**
     * Pub/sub for notifying the proxy of a table change or addition
     * args: <db_instance_id>
     * msg: <db_id>:<add|remove>:<schema>:<table>
     * see RedisDbTables::decode_pubsub_msg()
     */
    static constexpr char PUBSUB_DB_TABLE_CHANGES[] = "{}:pubsub:db_table_changes";

    /**
     * HASH of pre-commit DDL operations for index mutations.  Stored with a key of "db_id:xid"
     * args: <db_instance_id>
     */
    static constexpr char HASH_DDL_INDEX_PRECOMMIT[] = "{}:hash:idx:ddl:pc";

    /**
     * Hash set of excluded items for a given db_instance_id
     * args: <db_instance_id>
     * key: <table_oid>, value: <json>
     */
    static constexpr char HASH_INVALID_TABLES[] = "{}:hash:invalid_tables";

    /**
     * Hash of min XIDs per fdw:db_id for a given db_instance_id
     * args: <db_instance_id>
     * key: <fdw_id>:<db_id>, value: <min xid>
     */
    static constexpr char HASH_MIN_XID[] = "{}:fdw_min_xids";

    /**
     * Set holding index XIDs for each db id
     * args: <db_instance_id>, <db_id>
     * value: xid
     */
    static constexpr char SET_DB_INDEX_XIDS[] = "{}:set:db_index_xids:{}";
}
