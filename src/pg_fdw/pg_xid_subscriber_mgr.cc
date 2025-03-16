#include <pg_fdw/pg_xid_subscriber_mgr.hh>
#include <common/properties.hh>
#include <nlohmann/json.hpp>
#include <common/json.hh>
#include <xid_mgr/xid_mgr_client.hh>
#include <xid_mgr/xid_mgr_subscriber.hh>
#include <sys_tbl_mgr/client.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

PgXidSubscriberMgr::PgXidSubscriberMgr(size_t cache_size) :
    _cache_size{cache_size}
{
    SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "PgXidSubscriberMgr creating {}", _cache_size);
    _t = std::make_unique<std::jthread>([this](std::stop_token st) { task(st); });
}

PgXidSubscriberMgr::~PgXidSubscriberMgr()
{
    SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "PgXidSubscriberMgr deleted");
}

void
PgXidSubscriberMgr::task(std::stop_token st)
{
    // remove old cache if any and create a new one
    sys_tbl_mgr::ShmCache::remove(sys_tbl_mgr::SHM_CACHE_ROOTS);
    _cache = std::make_shared<sys_tbl_mgr::ShmCache>(sys_tbl_mgr::SHM_CACHE_ROOTS, _cache_size);

    _client = sys_tbl_mgr::Client::get_instance();
    // Client should cache get_roots() responses now
    _client->use_roots_cache(_cache);

    // Flag indicating the connection status of XidMgrSubscriber
    // to the XidMgr server.
    std::atomic<bool> connected = false;

    // XID subscriber callbacks
    auto on_push = [this](DbId db, uint64_t xid) {
        // when we get an XID push notification, we pass it to the workers
        // and return immediately.  A worker calls get_roots() that will 
        // attempt to populate the cache.
        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XID push notification {} - {}", db, xid);
        _cache->update_committed_xid(db, xid);
        _enqueue_populate_job(db, xid);
    };
    auto on_disconnect = [&connected]() { 
        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "XidMgrSubscriber disconnected");
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
    auto period = sys_tbl_mgr::ShmCache::XID_KEEP_ALIVE_PERIOD / 3;

    XidMgrClient *xid_client = XidMgrClient::get_instance();

    while(!st.stop_requested()) {
        if (connected == false) { 
            if (subscriber) {
                // GRPC is supposed to delete it after cancel()
                auto p = subscriber.release();
                p->cancel();
                // try to reconnect after 200ms
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            connected = true;
            subscriber = std::make_unique<XidMgrSubscriber>(xid_client->get_channel(), 
                    XidMgrSubscriber::Callbacks{on_push, on_disconnect});
        }
        std::this_thread::sleep_for(period);
        _cache->keep_alive();
    }
    if (subscriber) {
        // GRPC is supposed to delete it after cancel()
        auto p = subscriber.release();
        p->cancel();
    }
    SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "PgXidSubscriberMgr thread stopping");
    workers.clear();
    _client->use_roots_cache({});
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
        std::unique_lock g(_m);
        if (!_cv.wait(g, st, [this]{ return !_populate_queue.empty(); })) {
            break;
        }
        auto [db, tid] = _populate_queue.front();
        _populate_queue.pop();
        auto table_ids = _cache->get_db_tables(db);
        for (auto v: table_ids) {
            // the client will cache data in _cache
            _client->get_roots(db, v, tid);
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

    if (!roots_cache_size) {
        SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Bad cache size");
        return false;
    }

    _mgr = std::make_unique<PgXidSubscriberMgr>(roots_cache_size);

    return true;
}

void 
PgXidSubscriberRunner::stop()
{
    _mgr.reset();
}
