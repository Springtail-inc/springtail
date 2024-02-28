#pragma once

#include <unistd.h>
#include <thread>

#include <common/redis.hh>
#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_xact_log_writer.hh>
#include <pg_log_mgr/pg_redis_xact.hh>

namespace springtail {
    class PgXactHandler {
    public:
        PgXactHandler(const std::filesystem::path &base_path);

        /**
         * @brief Process transaction entry
         * @param xact transaction entry
         */
        void process(const PgTransactionPtr xact);

    private:
        std::filesystem::path _base_path;

        PgXactLogWriterPtr _logger;  ///< logger to write out xid log

        uint64_t _db_id=1;           ///< db id
        uint64_t _next_xid=0;        ///< next xid in xid range

        RedisQueue<PgRedisXactValue> _redis_queue; ///< redis queue for GC
        RedisSortedSet<PgRedisOidValue> _oid_set;  ///< redis sorted set for oid to xid mapping

        /** Create logger to write out xid log */
        void _create_logger();

        /** Allocate xid */
        uint64_t _allocate_xid();
    };
}
