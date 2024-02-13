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

        /** next xid in xid range allocated by xid mgr */
        uint64_t _next_xid;
        /** last xid in xid range allocated by xid mgr */
        uint64_t _last_xid;

        RedisQueue<PgRedisXactValue> _redis_queue;

        void _create_logger();

        uint64_t _allocate_xid();
    };
}
