#pragma once

namespace springtail::redis {
    // Redis system key prefix
    static constexpr char SYSTEM_PREFIX[] = "instance_config:";

    // Postgres redis key prefixes.  Value defs in: pg_log_mgr/pg_redis_xact.hh
    static constexpr char QUEUE_PG_TRANSACTIONS[] = "queue:pg_xact:";
    static constexpr char SET_PG_OID_XIDS[] = "set:pg_xid_oids:";

    static constexpr char QUEUE_GC_XID_READY[] = "queue:gc_xid_ready:";
}
