#include <string>
#include <fmt/core.h>
#include <chrono>
#include <map>

#include <common/common.hh>
#include <common/coordinator.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/properties.hh>

namespace springtail {

    Coordinator::Coordinator()
    {
        _db_instance_id = Properties::get_db_instance_id();
    }

    void
    Coordinator::_internal_shutdown()
    {
        // Clean up any resources if needed
    }

    void
    Coordinator::_internal_run()
    {
        while (!_is_shutting_down()) {
            RedisClientPtr redis = RedisMgr::get_instance()->get_client();
            std::string key = fmt::format(redis::HASH_LIVENESS, _db_instance_id);

            // Collect all timestamps under the mutex
            std::map<std::string, std::string> updates;
            {
                std::unique_lock lock(_threads_mutex);
                for (const auto& [hkey, timestamp] : _thread_timestamps) {
                    updates[hkey] = fmt::format("{}", timestamp.load());
                }
            }

            // Update Redis in a single round-trip
            if (!updates.empty()) {
                redis->hmset(key, updates.begin(), updates.end());
            }

            // Sleep for 1 second
            std::this_thread::sleep_for(BACKGROUND_THREAD_SLEEP_DURATION);
        }
    }

    std::atomic<uint64_t>&
    Coordinator::register_thread(DaemonType type, const std::string &thread_id)
    {
        std::string hkey = fmt::format("{}:{}", enum_to_integral(type), thread_id);
        auto epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        std::unique_lock lock(_threads_mutex);
        auto [it, inserted] = _thread_timestamps.try_emplace(hkey, epoch_ms);
        return it->second;
    }

    std::atomic<uint64_t>&
    Coordinator::find_thread(DaemonType type, const std::string &thread_id)
    {
        std::string hkey = fmt::format("{}:{}", enum_to_integral(type), thread_id);

        std::unique_lock lock(_threads_mutex);
        return _thread_timestamps.at(hkey);
    }

    void
    Coordinator::unregister_thread(DaemonType type, const std::string &thread_id)
    {
        std::string hkey = fmt::format("{}:{}", enum_to_integral(type), thread_id);

        std::unique_lock lock(_threads_mutex);
        _thread_timestamps.erase(hkey);

        // Also remove from Redis
        RedisClientPtr redis = RedisMgr::get_instance()->get_client();
        std::string key = fmt::format(redis::HASH_LIVENESS, _db_instance_id);
        redis->hdel(key, hkey);
    }

    void
    Coordinator::unregister_threads(DaemonType type,
                                    const std::vector<std::string> &threads)
    {
        if (threads.empty()) {
            return;
        }

        std::unique_lock lock(_threads_mutex);
        for (const auto& thread_id : threads) {
            std::string hkey = fmt::format("{}:{}", enum_to_integral(type), thread_id);
            _thread_timestamps.erase(hkey);
        }

        // Also remove from Redis
        RedisClientPtr redis = RedisMgr::get_instance()->get_client();
        std::string key = fmt::format(redis::HASH_LIVENESS, _db_instance_id);
        redis->hdel(key, threads.begin(), threads.end());
    }

    std::vector<std::string>
    Coordinator::get_threads(DaemonType type)
    {
        std::vector<std::string> keys;
        std::string prefix = fmt::format("{}:", enum_to_integral(type));

        std::unique_lock lock(_threads_mutex);
        for (const auto& [hkey, _] : _thread_timestamps) {
            if (hkey.starts_with(prefix)) {
                // remove the daemon type prefix
                keys.push_back(hkey.substr(hkey.find(':') + 1));
            }
        }

        return keys;
    }

    void
    Coordinator::kill_daemon(DaemonType type, const std::string &thread_id)
    {
        std::string hkey = fmt::format("{}:{}", enum_to_integral(type), thread_id);

        std::unique_lock lock(_threads_mutex);
        if (auto it = _thread_timestamps.find(hkey); it != _thread_timestamps.end()) {
            it->second.store(0);
        }

        RedisClientPtr redis = RedisMgr::get_instance()->get_client();
        std::string key = fmt::format(redis::HASH_LIVENESS, _db_instance_id);
        redis->hset(key, hkey, "0");

        // notify the coordinator immediately
        Properties::publish_liveness_notification(hkey);
    }
}
