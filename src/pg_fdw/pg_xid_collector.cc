#include <common/json.hh>
#include <pg_fdw/pg_xid_collector.hh>

namespace springtail::pg_fdw {

    PgXidCollector::PgXidCollector() noexcept :
        Singleton<PgXidCollector>(),
        _loop(std::make_unique<ipc::EventLoop>(LOOP_MAX_EVENTS, LOOP_TIMEOUT_MSEC))
    {
        _fdw_id = Properties::get_fdw_id();
        nlohmann::json fdw_config;
        fdw_config = Properties::get_fdw_config(_fdw_id);
        _socket_name = Json::get_or<std::string_view>(fdw_config, "collector_socket", DEFAULT_SOCKET_NAME);

        _unix_socket_watcher = std::make_unique<UnixSocketWatcher>(_socket_name);
    }

    PgXidCollector::~PgXidCollector() noexcept
    {
        _unix_socket_watcher->stop();
        _unix_socket_watcher.reset();
        _redis_update_timer.stop();
        _term_signal_watcher.stop();
        _active_fdws.clear();
        _dead_fdws.clear();
        DCHECK(_loop.use_count() == 1) << "Lost pointer somewhere";
        _loop.reset();
    }

    void PgXidCollector::run() noexcept
    {
        _term_signal_watcher.start(_loop);
        _redis_update_timer.start(_loop);
        _unix_socket_watcher->start(_loop);
        _loop->run();
    }

    void PgXidCollector::on_update(pid_t process_id, uint64_t db_id, uint64_t xid) noexcept
    {
        auto it = _active_fdws.find(process_id);
        if (it == _active_fdws.end()) {
            auto result = _active_fdws.emplace(process_id, std::make_shared<FdwPidWatcher>(process_id));
            DCHECK(result.second);
            result.first->second->start(_loop);
        }

        _pid_to_db_xid.insert_or_assign(process_id, std::make_pair(db_id, xid));
        _db_pid_to_xid[db_id][process_id] = xid;
    }

    void PgXidCollector::on_fdw_death(pid_t pid) noexcept
    {
        auto pid_node = _pid_to_db_xid.extract(pid);
        DCHECK(!pid_node.empty()) << "Process id " << pid << " is not found";
        auto [db_id, xid] = pid_node.mapped();
        _db_pid_to_xid[db_id].erase(pid);

        auto it = _active_fdws.find(pid);
        CHECK (it != _active_fdws.end()) << "Something is wrong! Can't find process watcher";
        // move into _dead_fdws
        _dead_fdws.emplace(pid, std::move(it->second));
        // remove from _active_fdws
        _active_fdws.erase(it);
    }

    void PgXidCollector::update_and_clean() noexcept
    {
        // cleanup dead watchers
        LOG_INFO("Cleaning up {} stale pid(s)", _dead_fdws.size());
        _dead_fdws.clear();

        for (auto [db_id, pid_to_xid]: _db_pid_to_xid) {
            auto it = std::min_element(
                pid_to_xid.begin(), pid_to_xid.end(),
                [](const auto &a, const auto &b) {
                    return a.second < b.second;
                });

            if (it != pid_to_xid.end()) {
                uint64_t xid = it->second;
                // TODO: set db_id and xid in redis
                LOG_INFO("Updating redis with db_id: {}, xid: {}", db_id, xid);
            }
        }

    }

} // springtail::pg_fdw