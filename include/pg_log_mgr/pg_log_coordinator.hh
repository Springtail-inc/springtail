#pragma once

#include <map>
#include <mutex>
#include <memory>
#include <atomic>

#include <common/counter.hh>
#include <common/singleton.hh>

#include <pg_log_mgr/pg_log_mgr.hh>

namespace springtail::pg_log_mgr {

    class PgLogCoordinator final : public Singleton<PgLogCoordinator> {
    public:

        /** Add a database to the coordinator */
        void add_database(uint64_t db_id);

        /** Remove database */
        void remove_database(uint64_t db_id);

        /** Wait for shutdown of dbs */
        void wait_shutdown() { _shutdown_counter.wait(); }

        /** Notify coordinator to shutdown */
        void notify_shutdown() { _shutdown_counter.decrement(); }

    private:
        friend class Singleton<PgLogCoordinator>;
        PgLogCoordinator() : _shutdown_counter(1) {}
        ~PgLogCoordinator() = default;

        Counter _shutdown_counter;
        std::mutex _mutex;                            ///< mutex for _log_mgrs map
        std::map<uint64_t, PgLogMgrPtr> _log_mgrs;    ///< map of db_id to log mgr

        /** Internal instance shutdown */
        void _internal_shutdown();
    };
}
