#include <common/json.hh>
#include <common/redis_types.hh>

#include <pg_fdw/pg_xid_collector.hh>

namespace springtail::pg_fdw {

    PgXidCollector::PgXidCollector() noexcept :
        Singleton<PgXidCollector>(),
        _loop(std::make_unique<ipc::EventLoop>(LOOP_MAX_EVENTS, LOOP_TIMEOUT_MSEC)),
        _msg_queue(-1)
    {
        uint64_t db_instance_id = Properties::get_db_instance_id();
        _redis_hash_name = fmt::format(redis::HASH_MIN_XID, db_instance_id);
        _fdw_id = Properties::get_fdw_id();
        nlohmann::json fdw_config;
        fdw_config = Properties::get_fdw_config(_fdw_id);
        _socket_name = Json::get_or<std::string_view>(fdw_config, "collector_socket", DEFAULT_SOCKET_NAME);

        _unix_socket_watcher = std::make_unique<UnixSocketWatcher>(_socket_name);

        _msg_queue_thread = std::thread(&PgXidCollector::_thread_run, this);
    }

    PgXidCollector::~PgXidCollector() noexcept
    {
        _msg_queue_thread.join();
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

        if (_db_pid_to_xid.size() == 0) {
            return;
        }

        MinXidsMsgPtr message = std::make_shared<MinXidMsg>();
        message->reserve(_db_pid_to_xid.size());
        for (auto [db_id, pid_to_xid]: _db_pid_to_xid) {
            auto it = std::min_element(
                pid_to_xid.begin(), pid_to_xid.end(),
                [](const auto &a, const auto &b) {
                    return a.second < b.second;
                });

            if (it != pid_to_xid.end()) {
                uint64_t xid = it->second;
                std::string key = fmt::format("{}:{}", _fdw_id, db_id);
                std::string value = std::to_string(xid);
                message->push_back(std::make_pair(key, value));
                LOG_INFO("Updating redis with key: {}, value: {}", key, value);
            }
        }
        _msg_queue.push(message);
    }

    void PgXidCollector::_internal_shutdown()
    {
        _msg_queue.shutdown(true);
    }

    void PgXidCollector::_thread_run()
    {
        while (!_msg_queue.empty() || !_msg_queue.is_shutdown()) {
            MinXidsMsgPtr message = _msg_queue.pop();
            if (message == nullptr) {
                continue;
            }
            RedisClientPtr client = RedisMgr::get_instance()->get_client();
            client->hmset(_redis_hash_name, message->begin(), message->end());
        }
    }
} // springtail::pg_fdw