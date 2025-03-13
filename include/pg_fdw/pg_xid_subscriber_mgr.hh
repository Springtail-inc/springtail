#pragma once

#include <queue>
#include <thread>
#include <common/properties.hh>
#include <common/service_register.hh>
#include <sys_tbl_mgr/shm_cache.hh>
#include <sys_tbl_mgr/client.hh>

namespace springtail::pg_fdw {
    /*
    class PgXidSubscriberMgr final : public Singleton<PgXidSubscriberMgr>
    {
        friend class Singleton<PgXidSubscriberMgr>;
    public:

    };
    */

    struct PgXidSubscriberMgr
    {
        using DbId = uint64_t;
        using TableId = uint64_t;

        explicit PgXidSubscriberMgr(size_t cache_size) :
            _cache_size{cache_size}
        {
            _t = std::make_unique<std::jthread>([this](std::stop_token st) { task(st); });
        }
        ~PgXidSubscriberMgr();

    private:
        size_t _cache_size;
        static constexpr size_t _worker_count = 4;

        sys_tbl_mgr::Client *_client;
        std::shared_ptr<sys_tbl_mgr::ShmCache> _cache;

        std::condition_variable_any _cv;
        std::mutex _m;
        std::queue<std::pair<DbId, TableId>> _populate_queue;

        void _enqueue_populate_job(DbId db, uint64_t xid);
        // running in a thread
        void _populate_worker(std::stop_token st);

        // this the main thread
        std::unique_ptr<std::jthread> _t;
        void task(std::stop_token st);
    };

    struct PgXidSubscriberRunner : public ServiceRunner
    {
        PgXidSubscriberRunner() : ServiceRunner("PgXidSubscriber")
        {}

        bool start() override;
        void stop() override;

    private:
        std::unique_ptr<PgXidSubscriberMgr> _mgr;
    };

    /*
    class PgXidSubscriberMgr final : public Singleton<PgXidSubscriberMgr>
    {
        friend class Singleton<PgXidSubscriberMgr>;
    public:

    };
    */
}
