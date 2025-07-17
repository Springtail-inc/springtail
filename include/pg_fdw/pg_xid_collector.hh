#pragma once

#include <sys/epoll.h>

#include <common/init.hh>

#include <pg_fdw/pg_xid_collector_common.hh>

namespace springtail::pg_fdw {

    /**
     * @brief This class performs xid collection service for all FDW processes.
     *
     */
    class PgXidCollector : public Singleton<PgXidCollector>
    {
        friend class Singleton<PgXidCollector>;
    public:
        static constexpr int        LOOP_MAX_EVENTS = 50;
        static constexpr int        LOOP_TIMEOUT_MSEC = 1'000;
        static constexpr uint64_t   REDIS_UPDATE_INTERVAL_MSEC = 5'000;

        /**
         * @brief This function updates related data structures when an FDW process death
         *          is detected.
         *
         * @param pid - process id
         */
        void on_fdw_death(pid_t pid) noexcept;

        /**
         * @brief This function performs an update of related data structures when
         *          an FDW process sends an update.
         *
         * @param process_id - process id
         * @param db_id      - database id
         * @param xid        - transaction id
         */
        void on_update(pid_t process_id, uint64_t db_id, uint64_t xid) noexcept;

    protected:
        /**
         * @brief Construct a new Pg Xid Collector object
         *
         */
        PgXidCollector() noexcept;

        /**
         * @brief Destroy the Pg Xid Collector object (deafault)
         *
         */
        virtual ~PgXidCollector() override = default;

        /**
         * @brief This function runs the main process loop in a thread started by Singleton
         *          class.
         *
         */
        virtual void _internal_run() override;

        /**
         * @brief This function is run by a thread that sends periodic updates to redis.
         *
         * @param st - stop token
         */
        void _redis_thread_run(std::stop_token st);

        /**
         * @brief Internal FDW process data used by the main loop.
         *
         */
        struct pid_data {
            int fd;                         /// process file descriptor
            struct epoll_event event_data;  /// epoll event data
        };

        /**
         * @brief mutex that controlls access to db and xid data
         *
         */
        std::shared_mutex _data_mutex;

        /**
         * @brief map from process id to database id, transaction id pair
         *
         */
        std::map<pid_t, std::pair<uint64_t, uint64_t>> _pid_to_db_id_xid;

        /**
         * @brief map from database id to the map from transaction id to the number of processes
         *          that are know to use this xid at the moment
         *
         */
        std::map<uint64_t, std::map<uint64_t, uint64_t>> _db_id_to_xid_to_count;

        std::jthread _redis_thread;     /// thread that runs redis
        std::string _fdw_id;            /// FDW id
        std::string _socket_name;       /// name of the abstract unix domain socket
        std::string _redis_hash_name;   /// name of the redis hash to update
    };

} // springtail::pg_fdw