#include <chrono>
#include <pg_fdw/pg_xid_subscriber_mgr.hh>
#include <common/properties.hh>
#include <nlohmann/json.hpp>
#include <common/json.hh>
#include <xid_mgr/xid_mgr_client.hh>
#include <xid_mgr/xid_mgr_subscriber.hh>
#include <sys_tbl_mgr/client.hh>
#include <common/coordinator.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

PgXidSubscriberMgr::PgXidSubscriberMgr(size_t cache_size, size_t worker_count) :
    _cache_size{cache_size},
    _worker_count{worker_count}
{
    LOG_DEBUG(LOG_XID_MGR, "PgXidSubscriberMgr creating {}, {}", _cache_size, _worker_count);
    _t = std::make_unique<std::jthread>([this](std::stop_token st) { task(st); });
}

PgXidSubscriberMgr::~PgXidSubscriberMgr()
{
    LOG_DEBUG(LOG_XID_MGR, "PgXidSubscriberMgr deleted");
}

void
PgXidSubscriberMgr::task(std::stop_token st)
{
    LOG_DEBUG(LOG_XID_MGR, "PgXidSubscriberMgr task starting");

    static constexpr char const * const XID_SUBSCRIBER_WORKER_ID = "xid_subscriber";

    auto coordinator = Coordinator::get_instance();
    auto& keep_alive = coordinator->register_thread(Coordinator::DaemonType::XID_SUBSCRIBER, XID_SUBSCRIBER_WORKER_ID);

    // remove old cache if any and create a new one
    sys_tbl_mgr::ShmCache::remove(sys_tbl_mgr::SHM_CACHE_ROOTS);
    _cache = std::make_shared<sys_tbl_mgr::ShmCache>(sys_tbl_mgr::SHM_CACHE_ROOTS, _cache_size);

    auto client = sys_tbl_mgr::Client::get_instance();
    // Client should cache get_roots() responses now
    client->use_roots_cache(_cache);

    // Flag indicating the connection status of XidMgrSubscriber
    // to the XidMgr server.
    std::atomic<bool> connected = false;

    // XID subscriber callbacks
    auto on_push = [this](DbId db, uint64_t xid) {
        // when we get an XID push notification, we pass it to the workers
        // and return immediately. A worker calls get_roots() that will
        // attempt to populate the cache.
        LOG_DEBUG(LOG_XID_MGR, "XID push notification {} - {}", db, xid);
        _cache->update_committed_xid(db, xid);
        _enqueue_populate_job(db, xid);
    };
    auto on_disconnect = [&connected]() {
        LOG_DEBUG(LOG_XID_MGR, "XidMgrSubscriber disconnected");
        connected = false;
    };

    std::unique_ptr<XidMgrSubscriber> subscriber;

    // the worker threads are responsible for fetching
    // sys table data when the next xid is committed
    std::vector<std::jthread> workers;
    for (auto i = 0; i != _worker_count; ++i) {
        workers.emplace_back([this](std::stop_token wst) { _populate_worker(wst); });
    }

    // keep alive
    auto cache_keep_alive = sys_tbl_mgr::ShmCache::XID_KEEP_ALIVE_PERIOD / 3;
    auto coordinator_keep_alive = std::chrono::milliseconds(1000*constant::COORDINATOR_KEEP_ALIVE_TIMEOUT) / 2;
    auto loop_time_period = std::min(cache_keep_alive, coordinator_keep_alive);

    XidMgrClient *xid_client = XidMgrClient::get_instance();

    while(!st.stop_requested()) {

        // mark alive with coordinator
        Coordinator::mark_alive(keep_alive);

        if (connected == false) {
            if (subscriber) {
                // GRPC is supposed to delete it after cancel()
                auto p = subscriber.release();
                CHECK(p);
                // try to reconnect after 200ms
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            connected = true;
            subscriber = std::make_unique<XidMgrSubscriber>(xid_client->get_channel(),
                    XidMgrSubscriber::Callbacks{on_push, on_disconnect});
        }
        std::this_thread::sleep_for(loop_time_period);
        _cache->keep_alive();
    }
    if (subscriber) {
        // GRPC is supposed to delete it after cancel()
        auto p = subscriber.release();
        if (connected) {
            p->cancel();
        }
    }
    LOG_DEBUG(LOG_XID_MGR, "PgXidSubscriberMgr thread stopping");
    workers.clear();
    client = sys_tbl_mgr::Client::get_instance();
    client->use_roots_cache({});
    _cache.reset();
}

void
PgXidSubscriberMgr::_enqueue_populate_job(DbId db, uint64_t xid)
{
    {
        std::scoped_lock g(_m);
        _populate_queue.push({db, xid});
    }
    // notify workers
    _cv.notify_all();
}

void
PgXidSubscriberMgr::_populate_worker(std::stop_token st)
{
    while(!st.stop_requested()) {
        // get the next work item
        decltype(_populate_queue)::value_type item;
        {
            std::unique_lock g(_m);
            if (!_cv.wait(g, st, [this]{ return !_populate_queue.empty(); })) {
                break;
            }
            item = _populate_queue.front();
            _populate_queue.pop();
        }
        auto [db, xid] = item;
        auto table_ids = _cache->get_db_tables(db);
        for (auto tid: table_ids) {
            XidLsn x{xid};

            auto client = sys_tbl_mgr::Client::get_instance();
            if (!client->exists(db, tid, x)) {
                // After the table is marked as dropped
                // the above call to get_db_tables()
                // won't return it. So exists() should be called
                // only once after the table is dropped.
                _cache->mark_dropped(db, tid, xid);
                continue;
            }
            // the client will cache data in _cache
            client->get_roots(db, tid, xid);
            if (st.stop_requested()) {
                break;
            }
        }
    }
}

bool
PgXidSubscriberRunner::start()
{
    nlohmann::json json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);

    uint64_t roots_cache_size = 0;
    Json::get_to<uint64_t>(json, "roots_shm_cache_size", roots_cache_size);

    LOG_DEBUG(LOG_XID_MGR, "PgXidSubscriberRunner starting with cache size {}", roots_cache_size);

    if (!roots_cache_size) {
        LOG_ERROR("Bad cache size, terminating PgXidSubscriberRunner");
        return false;
    }

    json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
    nlohmann::json rpc_json;

    // fetch RPC properties for the sys_tbl_mgr server
    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        LOG_ERROR("SysTblMgr RPC settings are not found, terminating PgXidSubscriberRunner");
        return false;
    }

    // The worker threads as used to make RPC requests to the sys table service while
    // populate the cache. We use the same number or threads as there are in the RPC in service.
    auto worker_count = Json::get_or<size_t>(rpc_json, "server_worker_threads", 1);

    _mgr = std::make_unique<PgXidSubscriberMgr>(roots_cache_size, worker_count);

    return true;
}

void
PgXidSubscriberRunner::stop()
{
    _mgr.reset();
}
