#pragma once

#include <map>
#include <mutex>

#include <common/service_register.hh>
#include <common/singleton.hh>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/committer.hh>

namespace springtail::pg_log_mgr {

    class PgLogCoordinator final : public Singleton<PgLogCoordinator> {
    public:
        /**
         * @brief Initialization function
         *
         */
        void init();

    private:
        friend class Singleton<PgLogCoordinator>;
        PgLogCoordinator();
        ~PgLogCoordinator() = default;

        std::mutex _mutex;                         ///< mutex for _log_mgrs map
        std::map<uint64_t, PgLogMgrPtr> _log_mgrs; ///< map of db_id to log mgr
        RedisCache::RedisChangeWatcherPtr _cache_watcher; ///> redis cache callback object
        std::shared_ptr<committer::Committer> _committer;
        std::thread _committer_thread;
        std::string _repl_log;                     ///< common part of replication log path
        std::string _trans_log;                    ///< common part of transcation log path
        std::string _host;                         ///< host name for connecting to database
        std::string _user_name;                    ///< user name for connecting to database
        std::string _password;                     ///< password for connecting to database
        uint64_t _db_instance_id;                  ///< database instance id
        int _port;                                 ///< port for connecting to database

        std::shared_ptr<ConcurrentQueue<committer::XidReady>> _committer_queue;

        /*
         * @brief A queue for indexer to notify committer to trigger index reconciliation
         **/
        std::shared_ptr<ConcurrentQueue<std::string>> _index_recon_queue;

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

    class PgLogCoordinatorRunner : public ServiceRunner {
    public:
        PgLogCoordinatorRunner() : ServiceRunner("PgLogCoordinator") {}

        bool start() override
        {
            pg_log_mgr::PgLogCoordinator::get_instance()->init();
            return true;
        }

        void stop() override
        {
            pg_log_mgr::PgLogCoordinator::shutdown();
        }
    };
}
