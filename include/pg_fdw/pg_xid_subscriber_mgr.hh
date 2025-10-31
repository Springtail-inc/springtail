#pragma once

#include <chrono>
#include <queue>
#include <thread>
#include <common/properties.hh>
#include <common/init.hh>
#include <sys_tbl_mgr/shm_cache.hh>
#include <sys_tbl_mgr/client.hh>

namespace springtail::pg_fdw {

    /**
     * This type manages subscriptions to push notifications of XID commits.
     * When the next XID is committed by the Xid manager it pushes a notification
     * to its subscribers. PgXidSubscriberMgr gets notifications and
     * populates the IPC cache for table roots. The cache is used by pg_fdw
     * that runs in a separate process.
     * Note: the cache is populated by worker threads. So that the main subscriber
     * isn't blocked.
     */
    class PgXidSubscriberMgr final : public Singleton<PgXidSubscriberMgr>
    {
        friend class Singleton<PgXidSubscriberMgr>;
    public:
        using DbId = uint64_t;
        using TableId = uint64_t;
        using Xid = uint64_t;

        void init(size_t cache_size, size_t worker_count);

        static void start();

    private:
        PgXidSubscriberMgr() : Singleton<PgXidSubscriberMgr>(ServiceId::PgXidSubscriberMgrId) {}
        ~PgXidSubscriberMgr();

        struct XidHistoryCleaner
        {
            static constexpr std::chrono::microseconds CLEANER_INTERVAL{500};

            explicit XidHistoryCleaner(std::shared_ptr<sys_tbl_mgr::ShmCache> cache)
                : _cache(std::move(cache))
            {
                _t = std::make_unique<std::jthread>([this](std::stop_token st) { _task(st); });
            }
            XidHistoryCleaner(const XidHistoryCleaner&&) = delete;

        private:
            void _task(std::stop_token st);

            std::shared_ptr<sys_tbl_mgr::ShmCache> _cache;
            std::condition_variable_any _cv;
            std::mutex _m;

            std::unique_ptr<std::jthread> _t;
        };


        size_t _cache_size;
        size_t _worker_count = 4;

        std::shared_ptr<sys_tbl_mgr::ShmCache> _cache;

        std::condition_variable_any _cv;
        std::mutex _m;
        std::queue<std::pair<DbId, Xid>> _populate_queue;

        /** The function is called when there is a new push notification.
         * The queue contains requests to worker threads to
         * populate the ICP cache.
         */
        void _enqueue_populate_job(DbId db, uint64_t xid);

        //**  running in a thread
        void _populate_worker(std::stop_token st);

        // this the main thread
        std::unique_ptr<std::jthread> _t;
        void task(std::stop_token st);
    };
}
