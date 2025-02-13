#include <mutex>

#include <sw/redis++/redis++.h>

#include <common/redis.hh>
#include <common/json.hh>
#include <common/properties.hh>
#include <common/logging.hh>
#include <common/common.hh>

namespace springtail {

    sw::redis::ConnectionOptions RedisMgr::_get_connect_options(bool config_db) {
        std::unique_lock lock(_connect_options_mutex);
        int db_id = (config_db)? _config_db_id : _data_db_id;
        if (_connect_options.db != db_id) {
            sw::redis::ConnectionOptions connect_options_clone = _connect_options;
            connect_options_clone.db = db_id;
            return connect_options_clone;
        }
        return _connect_options;
    }

    std::tuple<int, RedisClientPtr> RedisMgr::create_client(bool config_db) {
        sw::redis::ConnectionOptions     connect_options = _get_connect_options(config_db);
        sw::redis::ConnectionPoolOptions pool_options;

        nlohmann::json json = Properties::get(Properties::REDIS_CONFIG);
        nlohmann::json pool_json;
        if (!Json::get_to(json, "pool", pool_json)) {
            throw Error("Redis connection pool settings not found");
        }

        int pool_size = Json::get_or<int>(pool_json, "connections", 30);
        int max_idle_secs = Json::get_or<int>(pool_json, "max_idle_secs", 0);
        int max_connection_lifetime_secs = Json::get_or<int>(pool_json, "max_connection_lifetime_secs", 0);

        pool_options.size = pool_size;
        pool_options.connection_idle_time = std::chrono::seconds(max_idle_secs);
        pool_options.connection_lifetime = std::chrono::seconds(max_connection_lifetime_secs);

        RedisClientPtr client = std::make_shared<RedisClient>(connect_options, pool_options);
        SPDLOG_INFO("Connected to redis server: {}", connect_options.host);
        return std::make_tuple(connect_options.db, client);
    }

    RedisMgr::RedisMgr()
    {
        std::unique_lock lock(_connect_options_mutex);

        nlohmann::json json = Properties::get(Properties::REDIS_CONFIG);

        _connect_options.host = Json::get_or<std::string>(json, "host", "localhost");
        _connect_options.user = Json::get_or<std::string>(json, "user", "user");
        _connect_options.password = Json::get_or<std::string>(json, "password", "");
        _connect_options.port = Json::get_or<int>(json, "port", 6379);
        _config_db_id = Json::get_or<int>(json, "config_db", RedisMgr::REDIS_DATA_DB);
        _data_db_id = Json::get_or<int>(json, "db", RedisMgr::REDIS_DATA_DB);
        // For now set it to something
        _connect_options.db = _data_db_id;
        int keep_alive_secs = Json::get_or<int>(json, "keep_alive_sec", 30);
        bool ssl_enabled = Json::get_or<bool>(json, "ssl", false);

        _connect_options.keep_alive_s = std::chrono::seconds(keep_alive_secs);
        _connect_options.keep_alive = true;
        _connect_options.resp = 3;
        _connect_options.socket_timeout = std::chrono::milliseconds(0);
        _connect_options.tls.enabled = ssl_enabled;
        lock.unlock();

        int db_id;
        tie(db_id, _redis) = create_client(false);
    }

    RedisMgr::SubscriberPtr
    RedisMgr::get_subscriber(int timeoutsecs, bool config_db)
    {
        sw::redis::ConnectionOptions connect_options = _get_connect_options(config_db);

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
