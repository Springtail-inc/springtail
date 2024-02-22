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
        void process(const PgReplMsgStream::PgTransactionPtr xact);

    private:
        std::filesystem::path _base_path;

        PgXactLogWriterPtr _logger;

        /** database id */
        uint64_t _db_id=1;

        /** next xid in xid range allocated by xid mgr */
        uint64_t _next_xid=0;

        /** redis queue */
        RedisQueue<PgRedisXactValue> _redis_queue;

        /** redis sorted set */
        RedisSortedSet<PgRedisOidValue> _oid_set;

        /** Create logger to write out xid log */
        void _create_logger();

        /** Allocate xid */
        uint64_t _allocate_xid();
    };
}
