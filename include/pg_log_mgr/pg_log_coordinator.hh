#pragma once

#include <common/init.hh>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/committer.hh>

namespace springtail::pg_log_mgr {

    class PgLogCoordinator final : public Singleton<PgLogCoordinator>
    {
    public:
        /**
         * @brief Initialization function
         *
         */
        void init();

        /**
         * @brief Cleanup database directory for given database id
         *
         * @param db_id database id
         */
        void
        cleanup_database_dir(uint64_t db_id);

        /**
         * @brief Get this object relevant data in json format.
         *
         * @return nlohmann::json
         */
        nlohmann::json get_stats();

    private:
        friend class Singleton<PgLogCoordinator>;
        PgLogCoordinator();
        virtual ~PgLogCoordinator() override = default;

        std::mutex _mutex;                         ///< mutex for _log_mgrs map
        std::map<uint64_t, PgLogMgrPtr> _log_mgrs; ///< map of db_id to log mgr
        std::map<uint64_t, std::string> _db_states; ///< map of db_id to db state
        RedisCache::RedisChangeWatcherPtr _db_id_watcher; ///> redis cache callback object for db list change
        RedisCache::RedisChangeWatcherPtr _db_state_watcher; ///> redis cache callback object for db ids change
        std::shared_ptr<committer::Committer> _committer;
        std::thread _committer_thread;
        std::string _repl_log;                     ///< common part of replication log path
        std::string _trans_log;                    ///< common part of transcation log path
        std::string _host;                         ///< host name for connecting to database
        std::string _user_name;                    ///< user name for connecting to database
        std::string _password;                     ///< password for connecting to database
        uint64_t _db_instance_id;                  ///< database instance id
        uint64_t _log_size_rollover_threshold;     ///< log size rollover threshold
        int _port;                                 ///< port for connecting to database
        bool _archive_logs{false};                 ///< flag to turn on log archiving

        std::shared_ptr<ConcurrentQueue<committer::XidReady>> _committer_queue;

        /*
         * @brief Index reconciliation manager to access the index reconciliation queues
         **/
        std::shared_ptr<IndexReconciliationQueueManager> _index_reconciliation_queue_mgr;

        /**
         * @brief shared_ptr to the index requests manager to get
         * index requests (create/drop) for an XID per db
         */
        std::shared_ptr<IndexRequestsManager> _index_requests_mgr;

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
} // springtail::pg_log_mgr
