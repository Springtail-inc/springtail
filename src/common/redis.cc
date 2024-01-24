#include <mutex>
#include <iostream>

#include <sw/redis++/redis++.h>

#include <common/redis.hh>
#include <common/json.hh>
#include <common/properties.hh>
#include <common/logging.hh>
#include <common/common.hh>

namespace springtail {
    /* static initialization must happen outside of class */
    RedisMgr* RedisMgr::_instance {nullptr};
    std::mutex RedisMgr::_instance_mutex;

    RedisMgr *
    RedisMgr::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new RedisMgr();
        }

        return _instance;
    }

    RedisMgr::RedisMgr()
    {
        nlohmann::json json = Properties::get(Properties::REDIS_CONFIG);
        sw::redis::ConnectionOptions connect_options;
        sw::redis::ConnectionPoolOptions pool_options;

        int keep_alive_secs;

        Json::get_to<std::string>(json, "host", connect_options.host, "localhost");
        Json::get_to<std::string>(json, "user", connect_options.user, "user");
        Json::get_to<std::string>(json, "password", connect_options.password);
        Json::get_to<int>(json, "port", connect_options.port, 6379);
        Json::get_to<int>(json, "db", connect_options.db, 0);
        Json::get_to<int>(json, "keep_alive_sec", keep_alive_secs, 30);

        connect_options.keep_alive_s = std::chrono::seconds(keep_alive_secs);
        connect_options.keep_alive = true;
        connect_options.resp = 3;

        nlohmann::json pool_json;
        if (!Json::get_to(json, "pool", pool_json)) {
            throw Error("Redis connection pool settings not found");
        }

        int pool_size;
        int max_idle_secs;
        int max_connection_lifetime_secs;
        Json::get_to(pool_json, "connections", pool_size, 30);
        Json::get_to(pool_json, "max_idle_secs", max_idle_secs, 0);
        Json::get_to(pool_json, "max_connection_lifetime_secs", max_connection_lifetime_secs, 0);

        pool_options.size = pool_size;
        pool_options.connection_idle_time = std::chrono::seconds(max_idle_secs);
        pool_options.connection_lifetime = std::chrono::seconds(max_connection_lifetime_secs);

        _redis = std::make_shared<RedisClient>(connect_options, pool_options);

        SPDLOG_INFO("Connected to redis server: {}", connect_options.host);
    }

    void
    RedisMgr::shutdown()
    {
        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }
}

/* Example usage:
    springtail::RedisMgr *redis_mgr = springtail::RedisMgr::get_instance();
    std::shared_ptr<springtail::RedisClient> redis_client = redis_mgr->get_client();
    std::cout << redis_client->ping() << std::endl;
*/