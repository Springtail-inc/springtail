#pragma once

namespace springtail {
    namespace redis {
        // Postgres redis key prefixes.  Value defs in: pg_log_mgr/pg_redis_xact.hh
        static constexpr char QUEUE_PG_TRANSACTIONS[] = "queue:pg_xact";
        static constexpr char SET_PG_OID_XIDS[] = "set:pg_xid_oids";

        static constexpr char QUEUE_GC_XID_READY[] = "queue:gc_xid_ready";

        static constexpr char MUTEX_SYS_TBL[] = "mutex:sys_tbl";

        // args: <db instance id>
        static constexpr char HASH_FDW[] = "fdw";

        //// For RedisDDL

        // args: <xid>
        static constexpr char QUEUE_DDL_XID[] = "queue:ddl:xid:{}";

        // args: <fdw_id>
        static constexpr char QUEUE_DDL_FDW[] = "queue:ddl:fdw:{}";

        // args: none
        static constexpr char HASH_DDL_FDW[] = "hash:ddl:fdw";
    }
}
