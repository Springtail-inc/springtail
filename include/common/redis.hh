#pragma once

#include <mutex>
#include <memory>
#include <string>

#include <sw/redis++/redis++.h>

#include <common/exception.hh>
#include <common/singleton.hh>

namespace springtail {

    /** Redis exception type */
    class RedisError : public Error {
    public:
        RedisError() { }
        RedisError(const std::string &error)
            : Error(error)
        { }
    };

    /** Redis not found exception */
    class RedisNotFoundError : public RedisError {
        // constructor to take in a string
    public:
        RedisNotFoundError(const std::string &error)
            : RedisError(error)
        { }
        const char *what() const noexcept {
            return "Key not found";
        }
    };

    /**
     * @brief Redis connection wrapper, derives from Redis object
     *        For adding future functionality; redis::Redis is threadsafe
     */
    class RedisClient : public sw::redis::Redis
    {
    public:
        virtual ~RedisClient() {};

        /**
         * @brief Construct a new Redis Client with connection options
         * @param options connection options for redis
         */
        RedisClient(sw::redis::ConnectionOptions options) :
            sw::redis::Redis(options)
        {}

        /**
         * @brief Construct a new Redis Client object with connection options and pool
         * @param options connection options
         * @param pool_options connection pool options
         */
        RedisClient(sw::redis::ConnectionOptions options,
                    sw::redis::ConnectionPoolOptions pool_options) :
            sw::redis::Redis(options, pool_options)
        {}
    };
    using RedisClientPtr = std::shared_ptr<RedisClient>;

    /**
     * @brief Singleton redis client
     */
    class RedisMgr : public Singleton<RedisMgr> {
        friend class Singleton<RedisMgr>;
    public:
        using SubscriberPtr = std::shared_ptr<sw::redis::Subscriber>;

        static constexpr int REDIS_CONFIG_DB = 0;
        static constexpr int REDIS_DATA_DB = 1;

        /**
         * @brief Get the redis client object
         * @return sw::redis::Redis
         */
        inline RedisClientPtr get_client() {
            return _redis;
        }

        /**
         * Get a subscriber object to be used for pub/sub
         * Creates a new connection to Redis, so should be reusued
         * Caller must catch sw::redis::TimeoutError
         * @param timeout_secs timeout in seconds
         * @param config_db if true, use the config db
         * @return SubscriberPtr
         */
        static SubscriberPtr get_subscriber(int timeout_secs=5,  bool config_db=true);

        static std::tuple<int, RedisClientPtr> create_client(bool config_db);

    protected:
        /** Redis client wrapper around client object */
        RedisClientPtr _redis;

        RedisMgr();

        ~RedisMgr() override = default;

    private:
        int _db_id;
        static inline bool _inited = false;
        static inline int _config_db_id;
        static inline int _data_db_id;
        static inline std::mutex _connect_options_mutex;
        static inline sw::redis::ConnectionOptions _connect_options = {};

        static sw::redis::ConnectionOptions _get_connect_options(bool config_db=true);
    };
}
