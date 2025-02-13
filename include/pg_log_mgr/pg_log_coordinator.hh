#pragma once

#include <map>
#include <mutex>

#include <common/counter.hh>
#include <common/singleton.hh>

#include <pg_log_mgr/pg_log_mgr.hh>

namespace springtail::pg_log_mgr {

    class PgLogCoordinator final : public Singleton<PgLogCoordinator> {
    public:
        /** Wait for shutdown of dbs */
        void wait_shutdown() { _shutdown_counter.wait(); }

        /** Notify coordinator to shutdown */
        void notify_shutdown() { _shutdown_counter.decrement(); }

        /**
         * @brief Initialization function
         *
         */
        void init();

    private:
        friend class Singleton<PgLogCoordinator>;
        PgLogCoordinator();
        ~PgLogCoordinator() = default;

        Counter _shutdown_counter;
        std::mutex _mutex;                         ///< mutex for _log_mgrs map
        std::map<uint64_t, PgLogMgrPtr> _log_mgrs; ///< map of db_id to log mgr
        RedisCache::RedisChangeWatcherPtr _cache_watcher; ///> redis cache callback object
        std::string _repl_log;                     ///< common part of replication log path
        std::string _trans_log;                    ///< common part of transcation log path
        uint64_t _db_instance_id;                  ///< database instance id
        std::string _host;                         ///< host name for connecting to database
        std::string _user_name;                    ///< user name for connecting to database
        std::string _password;                     ///< password for connecting to database
        int _port;                                 ///< port for connecting to database
        std::thread _write_cache_thread;           ///< thread for the write cache thrift interface

        /**
         * @brief Function for performing shutdown that is called by Singleton
         *
         */
        void _internal_shutdown() override;

        /**
         * @brief Add database and start its own log manager
         *
         * @param db_id - database id
         */
        void _add_database(uint64_t db_id);

        /**
         * @brief Remove database and stop appropriate log manager
         *
         * @param db_id - database id
         */
        void _remove_database(uint64_t db_id);
    };
}
