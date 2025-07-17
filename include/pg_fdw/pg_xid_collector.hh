#pragma once

#include <sys/epoll.h>

#include <common/init.hh>

#include <pg_fdw/pg_xid_collector_common.hh>

namespace springtail::pg_fdw {

    class PgXidCollector : public Singleton<PgXidCollector>
    {
        friend class Singleton<PgXidCollector>;
    public:
        static constexpr int        LOOP_MAX_EVENTS = 50;
        static constexpr int        LOOP_TIMEOUT_MSEC = 1'000;
        static constexpr uint64_t   REDIS_UPDATE_INTERVAL_MSEC = 5'000;

        void on_fdw_death(pid_t pid) noexcept;
        void on_update(pid_t process_id, uint64_t db_id, uint64_t xid) noexcept;

    protected:
        PgXidCollector() noexcept;
        virtual ~PgXidCollector() override = default;

        virtual void _internal_run() override;
        void _redis_thread_run(std::stop_token st);

        struct pid_data {
            int fd;
            struct epoll_event event_data;
        };

        std::shared_mutex _data_mutex;
        std::map<pid_t, std::pair<uint64_t, uint64_t>> _pid_to_db_id_xid;
        std::map<uint64_t, std::map<uint64_t, uint64_t>> _db_id_to_xid_to_count;

        std::jthread _redis_thread;
        std::string _fdw_id;
        std::string _socket_name;
        std::string _redis_hash_name;
    };

} // springtail::pg_fdw