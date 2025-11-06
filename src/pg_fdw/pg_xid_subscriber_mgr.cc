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

void
PgXidSubscriberMgr::init(size_t roots_cache_size, size_t schema_cache_size, size_t worker_count)
{
    _roots_cache_size = roots_cache_size;
    _schema_cache_size = schema_cache_size;
    _worker_count = worker_count;
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "creating {}, {}, {}", _roots_cache_size, _schema_cache_size, _worker_count);
    _t = std::make_unique<std::jthread>([this](std::stop_token st) { task(st); });
    pthread_setname_np(_t->native_handle(), "PgXidSubscriber");
}

PgXidSubscriberMgr::~PgXidSubscriberMgr()
{
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "PgXidSubscriberMgr deleted");
}

void
PgXidSubscriberMgr::task(std::stop_token st)
{
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "task starting");

    static constexpr char const * const XID_SUBSCRIBER_WORKER_ID = "xid_subscriber";

    auto coordinator = Coordinator::get_instance();
    auto& keep_alive = coordinator->register_thread(Coordinator::DaemonType::XID_SUBSCRIBER, XID_SUBSCRIBER_WORKER_ID);

    // remove old cache if any and create a new one
    sys_tbl_mgr::ShmCache::remove(sys_tbl_mgr::SHM_CACHE_ROOTS);
    _cache = std::make_shared<sys_tbl_mgr::ShmCache>(sys_tbl_mgr::SHM_CACHE_ROOTS, _roots_cache_size);

    sys_tbl_mgr::ShmCache::remove(sys_tbl_mgr::SHM_CACHE_SCHEMAS);
    _schema_cache = std::make_shared<sys_tbl_mgr::ShmCache>(sys_tbl_mgr::SHM_CACHE_SCHEMAS, _schema_cache_size);

    auto client = sys_tbl_mgr::Client::get_instance();
    // Client should cache get_roots() responses now
    client->use_roots_cache(_cache);
    client->use_schema_cache(_schema_cache);

    // Flag indicating the connection status of XidMgrSubscriber
    // to the XidMgr server.
    std::atomic<bool> connected = false;

    // XID subscriber callbacks
    auto on_push = [this](DbId db, uint64_t xid) {
        // when we get an XID push notification, we pass it to the workers
        // and return immediately. A worker calls get_roots() and get_schema() that will
        // attempt to populate the caches.
        LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "XID push notification {} - {}", db, xid);
        _cache->update_committed_xid(db, xid);
        _schema_cache->update_committed_xid(db, xid);
        _enqueue_populate_job(db, xid);
    };
    auto on_disconnect = [&connected]() {
        LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "XidMgrSubscriber disconnected");
        connected = false;
    };

    std::unique_ptr<XidMgrSubscriber> subscriber;

    // the worker threads are responsible for fetching
    // sys table data when the next xid is committed
    std::vector<std::jthread> workers;
    for (auto i = 0; i != _worker_count; ++i) {
        std::string thread_name = fmt::format("XidSubWorker_{}", i);
        workers.emplace_back([this](std::stop_token wst) { _populate_worker(wst); });
        pthread_setname_np(workers.back().native_handle(), thread_name.c_str());
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
                subscriber.reset();
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            connected = true;
            subscriber = std::make_unique<XidMgrSubscriber>(xid_client->get_channel(),
                    XidMgrSubscriber::Callbacks{on_push, on_disconnect});
            subscriber->start();
        }
        std::this_thread::sleep_for(loop_time_period);
        _cache->keep_alive();
        _schema_cache->keep_alive();
    }
    subscriber.reset();
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "PgXidSubscriberMgr thread stopping");
    workers.clear();
    client = sys_tbl_mgr::Client::get_instance();
    client->use_roots_cache({});
    client->use_schema_cache({});
    _cache.reset();
    _schema_cache.reset();
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
                _schema_cache->mark_dropped(db, tid, x);
                continue;
            }
            // the client will cache data in _cache and _schema_cache
            client->get_roots(db, tid, xid);
            client->get_schema(db, tid, x);
            if (st.stop_requested()) {
                break;
            }
        }
    }
}

void
PgXidSubscriberMgr::start()
{
    nlohmann::json json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);

    size_t roots_cache_size = 0;
    Json::get_to<size_t>(json, "roots_shm_cache_size", roots_cache_size);

    size_t schema_cache_size = 0;
    Json::get_to<size_t>(json, "schema_shm_cache_size", schema_cache_size);

    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "PgXidSubscriberRunner starting with cache size {}", roots_cache_size);

    CHECK(roots_cache_size) << "Bad cache size, terminating PgXidSubscriberRunner";

    json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
    nlohmann::json rpc_json;

    // fetch RPC properties for the sys_tbl_mgr server
    CHECK(Json::get_to(json, "rpc_config", rpc_json)) << "SysTblMgr RPC settings are not found, terminating PgXidSubscriberRunner";

    // The worker threads as used to make RPC requests to the sys table service while
    // populate the cache. We use the same number or threads as there are in the RPC in service.
    auto worker_count = Json::get_or<size_t>(rpc_json, "server_worker_threads", 1);

    get_instance()->init(roots_cache_size, schema_cache_size, worker_count);
}
