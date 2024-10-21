#include <string>
#include <fmt/core.h>

#include <common/common.hh>
#include <common/coordinator.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/properties.hh>

namespace springtail {

    Coordinator *Coordinator::_instance = nullptr;
    std::once_flag Coordinator::_init_flag;

    Coordinator::Coordinator()
    {
        _db_instance_id = Properties::get_db_instance_id();
    }

    void Coordinator::_init() {
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
        std::string key = fmt::format(redis::HASH_LIVENESS, _db_instance_id, (uint8_t)type, thread_id);
        std::string hkey = fmt::format("{}:{}", enum_to_integral(type), thread_id);
        redis->hdel(key, hkey);
    }

    void
    Coordinator::set_liveness(DaemonType type, const std::string &thread_id)
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
        auto epoch_ms =  std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        std::string value = fmt::format("{}", alive ? epoch_ms : 0);

        // set the liveness value in the hash
        redis->hset(key, hkey, value);

        if (!alive) {
            // notify the coordinator
            std::string notify_key = fmt::format(redis::QUEUE_LIVENESS_NOTIFY, _db_instance_id);
            redis->rpush(notify_key, hkey);
        }
    }
}