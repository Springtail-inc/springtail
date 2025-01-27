#include <fmt/core.h>

#include <string>

#include <common/common.hh>
#include <common/coordinator.hh>
#include <common/properties.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>

namespace springtail {

Coordinator *Coordinator::_instance = nullptr;
std::once_flag Coordinator::_init_flag;

Coordinator::Coordinator() { _db_instance_id = Properties::get_db_instance_id(); }

void
Coordinator::_init()
{
    _instance = new Coordinator();
}

void
Coordinator::register_thread(DaemonType type, const std::string &thread_id)
{
    _set_liveness(type, thread_id, true);
}

void
Coordinator::unregister_thread(DaemonType type, const std::string &thread_id)
{
    RedisClientPtr redis = RedisMgr::get_instance()->get_client();
    std::string key = fmt::format(redis::HASH_LIVENESS, _db_instance_id);
    std::string hkey = fmt::format("{}:{}", enum_to_integral(type), thread_id);
    redis->hdel(key, hkey);
}

void
Coordinator::unregister_threads(DaemonType type, const std::vector<std::string> &threads)
{
    if (threads.empty()) {
        return;
    }

    RedisClientPtr redis = RedisMgr::get_instance()->get_client();
    std::string key = fmt::format(redis::HASH_LIVENESS, _db_instance_id);
    redis->hdel(key, threads.begin(), threads.end());
}

std::vector<std::string>
Coordinator::get_threads(DaemonType type)
{
    std::vector<std::string> all_keys, keys;

    RedisClientPtr redis = RedisMgr::get_instance()->get_client();
    std::string key = fmt::format(redis::HASH_LIVENESS, _db_instance_id);
    redis->hkeys(key, std::back_inserter(all_keys));

    // retrieve just the keys for the daemon we care about
    for (auto &&key : all_keys) {
        if (key.starts_with(fmt::format("{}:", enum_to_integral(type)))) {
            // remove the daemon type prefix
            keys.push_back(key.substr(key.find(':') + 1));
        }
    }

    return keys;
}

void
Coordinator::mark_alive(DaemonType type, const std::string &thread_id)
{
    _set_liveness(type, thread_id, true);
}

void
Coordinator::kill_daemon(DaemonType type, const std::string &thread_id)
{
    _set_liveness(type, thread_id, false);
}

void
Coordinator::_set_liveness(DaemonType type, const std::string &thread_id, bool alive)
{
    RedisClientPtr redis = RedisMgr::get_instance()->get_client();
    std::string key = fmt::format(redis::HASH_LIVENESS, _db_instance_id, (uint8_t)type, thread_id);
    std::string hkey = fmt::format("{}:{}", enum_to_integral(type), thread_id);
    auto epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
    std::string value = fmt::format("{}", alive ? epoch_ms : 0);

    // set the liveness value in the hash
    redis->hset(key, hkey, value);

    if (!alive) {
        // notify the coordinator
        Properties::publish_liveness_notification(hkey);
    }
}
}  // namespace springtail
