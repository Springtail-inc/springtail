#pragma once

#include <map>
#include <mutex>
#include <memory>
#include <atomic>

#include <pg_log_mgr/pg_log_mgr.hh>

namespace springtail::pg_log_mgr {

    class PgLogCoordinator {
    public:
        /** Get singleton instance */
        static PgLogCoordinator* get_instance() {
            std::call_once(_init_flag, _init);
            return _instance;
        }

        /**
         * Shutdown singleton -- initiates shutdown
         * must call wait_shutdown() to complete
         */
        static void shutdown() {
            std::call_once(_shutdown_flag, _shutdown);
        }

        /** Add a database to the coordinator */
        void add_database(uint64_t db_id);

        /** Remove database */
        void remove_database(uint64_t db_id);

        /** Wait for shutdown of dbs */
        void wait_shutdown();

    private:
        PgLogCoordinator() {};
        PgLogCoordinator(const PgLogCoordinator&) = delete;
        PgLogCoordinator& operator=(const PgLogCoordinator&) = delete;

        static PgLogCoordinator* _instance;
        static std::once_flag _init_flag;
        static std::once_flag _shutdown_flag;

        static PgLogCoordinator* _init();    ///< Initialize singleton
        static void _shutdown();             ///< Shutdown singleton

        std::atomic<bool> _shutting_down {false};     ///< shutdown flag, set in shutdown()
        std::atomic<bool> _shutdown_complete {false}; ///< shutdown complete flag, set in wait_shutdown()
        std::mutex _shutdown_mutex;                   ///< shutdown mutex
        std::condition_variable _shutdown_cv;         ///< shutdown condition variable

        std::mutex _mutex;                            ///< mutex for _log_mgrs map
        std::map<uint64_t, PgLogMgrPtr> _log_mgrs;    ///< map of db_id to log mgr

        /** Internal instance shutdown */
        void _internal_shutdown();
    };
}
