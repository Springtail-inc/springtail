#pragma once

#include <unistd.h>
#include <thread>
#include <filesystem>

#include <common/redis.hh>
#include <common/filesystem.hh>

#include <pg_repl/pg_repl_msg.hh>
#include <pg_log_mgr/pg_xact_log_writer.hh>
#include <pg_log_mgr/pg_redis_xact.hh>

namespace springtail {
    class PgXactHandler {
    public:
        static constexpr char const * const LOG_PREFIX = "pg_log_xact_";
        static constexpr char const * const LOG_SUFFIX = ".log";

        PgXactHandler(const std::filesystem::path &base_dir);

        /**
         * @brief Process transaction entry
         * @param xact transaction entry
         */
        void process(const PgTransactionPtr xact);


        /**
         * @brief Get the next logfile path based on current path
         * @param path current path
         * @return std::filesystem::path
         */
        std::filesystem::path get_next_logfile(std::filesystem::path &path) {
            return fs::get_next_file(path, LOG_PREFIX, LOG_SUFFIX);
        }

        /**
         * @brief Get the redis xacts object -- for testing
         * @param start start index (0)
         * @param end   end index (-1)
         * @return std::vector<PgRedisXactValue>
         */
        std::vector<PgRedisXactValue> get_redis_xacts(long long start=0, long long end=-1);

    private:
        std::filesystem::path _base_dir;
        std::filesystem::path _current_logfile;

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
