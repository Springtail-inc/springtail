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
        std::unique_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new RedisMgr();
        }

        return _instance;
    }

    RedisMgr::RedisMgr()
    {
        nlohmann::json json = Properties::get(Properties::REDIS_CONFIG);

        int keep_alive_secs;

        Json::get_to<std::string>(json, "host", _connect_options.host, "localhost");
        Json::get_to<std::string>(json, "user", _connect_options.user, "user");
        Json::get_to<std::string>(json, "password", _connect_options.password);
        Json::get_to<int>(json, "port", _connect_options.port, 6379);
        Json::get_to<int>(json, "db", _connect_options.db, REDIS_DATA_DB);
        Json::get_to<int>(json, "keep_alive_sec", keep_alive_secs, 30);

        _connect_options.keep_alive_s = std::chrono::seconds(keep_alive_secs);
        _connect_options.keep_alive = true;
        _connect_options.resp = 3;
        _connect_options.socket_timeout = std::chrono::milliseconds(0);

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

        _pool_options.size = pool_size;
        _pool_options.connection_idle_time = std::chrono::seconds(max_idle_secs);
        _pool_options.connection_lifetime = std::chrono::seconds(max_connection_lifetime_secs);

        _redis = std::make_shared<RedisClient>(_connect_options, _pool_options);

        SPDLOG_INFO("Connected to redis server: {}", _connect_options.host);
    }

    void
    RedisMgr::shutdown()
    {
        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    RedisMgr::SubscriberPtr
    RedisMgr::get_subscriber(int timeoutsecs, bool config_db)
    {
        sw::redis::ConnectionOptions connect_options;

        // get config db from redis
        if (config_db) {
            nlohmann::json json = Properties::get(Properties::REDIS_CONFIG);
            int config_db;
            Json::get_to<int>(json, "config_db", config_db);
            connect_options.db = config_db;
        } else {
            connect_options.db = _connect_options.db;
        }

        // create new redis connection for use with subscriber
        // copy the connection options, change the db to the config db
        connect_options.host = _connect_options.host;
        connect_options.port = _connect_options.port;
        connect_options.user = _connect_options.user;
        connect_options.password = _connect_options.password;
        connect_options.keep_alive = _connect_options.keep_alive;
        connect_options.keep_alive_s = _connect_options.keep_alive_s;
        connect_options.resp = _connect_options.resp;

        // this is the real timeout for the subscriber consume() call
        connect_options.socket_timeout = std::chrono::seconds(timeoutsecs);

        auto redis = std::make_shared<sw::redis::Redis>(connect_options);
        return std::make_shared<sw::redis::Subscriber>(redis->subscriber());
    }
}

/* Example usage:
    springtail::RedisMgr *redis_mgr = springtail::RedisMgr::get_instance();
    std::shared_ptr<springtail::RedisClient> redis_client = redis_mgr->get_client();
    std::cout << redis_client->ping() << std::endl;
*/