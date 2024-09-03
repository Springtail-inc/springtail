#pragma once

namespace springtail::redis {
    // Redis system key prefix
    static constexpr char SYSTEM_PREFIX[] = "instance_config:";

    // Postgres redis key prefixes.  Value defs in: pg_log_mgr/pg_redis_xact.hh
    static constexpr char QUEUE_PG_TRANSACTIONS[] = "queue:pg_xact:";

    /**
     * Maintains a mapping from each XID to the Table OIDs it mutates.
     * Populated by the PgLogMgr and utilized by the gc::LogParser::Backlog.
     * There is one sorted set maintained per database.
     *
     * args: <db_inst_id>, <db_id>
     */
    static constexpr char SET_PG_OID_XIDS[] = "set:pg_xid_oids:{}:{}";

    static constexpr char QUEUE_GC_XID_READY[] = "queue:gc_xid_ready:";

    static constexpr char MUTEX_SYS_TBL[] = "mutex:sys_tbl";

    // FDW config args: <db instance id>
    static constexpr char HASH_FDW[] = "fdw:{}";

    // Config for database instance: var db_instance_id
    static constexpr char DB_INSTANCE_CONFIG[] = "instance_config:{}";

    // Config for database within instance: var db_instance_id, db_config_id
    static constexpr char DB_CONFIG[] = "db_config:{}:{}";

    // State hash for database instance: var db_instance_id
    static constexpr char DB_INSTANCE_STATE[] = "instance_state:{}";

    // DB state key for database within DB_INSTANCE_STATE: var db_id
    static constexpr char KEY_DB_STATE[] = "db_state:{}";

    //// For RedisDDL

    // args: <db_inst_id>, <db_id>, <xid>
    static constexpr char QUEUE_DDL_XID[] = "queue:ddl:xid:{}:{}:{}";

    // args: <db_instance_id>, <fdw_id>
    static constexpr char QUEUE_DDL_FDW[] = "queue:ddl:fdw:{}:{}";

    // args: <db_instance_id> -- hash key: fdw_id
    static constexpr char HASH_DDL_FDW[] = "hash:ddl:fdw:{}";



}
