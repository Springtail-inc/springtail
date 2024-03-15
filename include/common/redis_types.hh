#pragma once

namespace springtail {
    namespace redis {
        // Postgres redis key prefixes.  Value defs in: pg_log_mgr/pg_redis_xact.hh
        static constexpr char QUEUE_PG_TRANSACTIONS[] = "queue:pg_xact:";
        static constexpr char SET_PG_OID_XIDS[] = "set:pg_xid_oids:";
    }
}