#include <mutex>
#include <iostream>

#include <sw/redis++/redis++.h>

#include <common/redis.hh>
#include <common/json.hh>
#include <common/properties.hh>
#include <common/logging.hh>
#include <common/common.hh>

namespace springtail {

    sw::redis::ConnectionOptions RedisMgr::get_connect_options(bool config_db) {
        static sw::redis::ConnectionOptions connect_options = {};
        static bool inited = false;
        static std::mutex connect_options_mutex;
        static int config_db_id;
        static int data_db_id;

        std::unique_lock lock(connect_options_mutex);
        if (inited) {
            int db_id = (config_db)? config_db_id : data_db_id;
            if (connect_options.db != db_id) {
                sw::redis::ConnectionOptions connect_options_clone = connect_options;
                connect_options_clone.db = db_id;
                return connect_options_clone;
            }
            return connect_options;
        } else {
            nlohmann::json json = Properties::get(Properties::REDIS_CONFIG);

            connect_options.host = Json::get_or<std::string>(json, "host", "localhost");
            connect_options.user = Json::get_or<std::string>(json, "user", "user");
            connect_options.password = Json::get_or<std::string>(json, "password", "");
            connect_options.port = Json::get_or<int>(json, "port", 6379);
            if (config_db) {
                config_db_id = Json::get_or<int>(json, "config_db", RedisMgr::REDIS_DATA_DB);
            } else {
                data_db_id = Json::get_or<int>(json, "db", RedisMgr::REDIS_DATA_DB);
            }
            connect_options.db = (config_db)? config_db_id : data_db_id;
            int keep_alive_secs = Json::get_or<int>(json, "keep_alive_sec", 30);
            bool ssl_enabled = Json::get_or<bool>(json, "ssl", false);

            connect_options.keep_alive_s = std::chrono::seconds(keep_alive_secs);
            connect_options.keep_alive = true;
            connect_options.resp = 3;
            connect_options.socket_timeout = std::chrono::milliseconds(0);
            connect_options.tls.enabled = ssl_enabled;

            nlohmann::json pool_json;
            if (!Json::get_to(json, "pool", pool_json)) {
                throw Error("Redis connection pool settings not found");
            }
            inited = true;
        }
        return connect_options;
    }

    std::tuple<int, RedisClientPtr> RedisMgr::create_client(bool config_db) {
        sw::redis::ConnectionOptions     connect_options = get_connect_options(config_db);
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
        tie(_db_id, _redis) = create_client(false);
    }

    RedisMgr::SubscriberPtr
    RedisMgr::get_subscriber(int timeoutsecs, bool config_db)
    {
        sw::redis::ConnectionOptions connect_options = get_connect_options(config_db);

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