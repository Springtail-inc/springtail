#pragma once

#include <mutex>
#include <memory>
#include <string>
#include <type_traits>

#include <sw/redis++/redis++.h>

#include <common/exception.hh>

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

        /** Helper to set key expiry */
    };
    using RedisClientPtr = std::shared_ptr<RedisClient>;

    /**
     * @brief Singleton redis client
     */
    class RedisMgr {
    public:
        using SubscriberPtr = std::shared_ptr<sw::redis::Subscriber>;

        static constexpr int REDIS_CONFIG_DB = 0;
        static constexpr int REDIS_DATA_DB = 1;

        /**
         * @brief Get the singleton instance object
         * @return RedisMgr*
         */
        static RedisMgr *get_instance();

        /**
         * @brief Shutdown Redis Mgr and connection pool
         */
        void shutdown();

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
        SubscriberPtr get_subscriber(int timeout_secs=5,  bool config_db=true);

    protected:
        /** internal singleton instance */
        static RedisMgr *_instance;

        /** mutex protecting _instance creation */
        static std::mutex _instance_mutex;

        /** Redis client wrapper around client object */
        RedisClientPtr _redis;

        RedisMgr();

        ~RedisMgr() {}

    private:
        sw::redis::ConnectionOptions     _connect_options;
        sw::redis::ConnectionPoolOptions _pool_options;

        // delete copy constructor
        RedisMgr(const RedisMgr &)       = delete;
        void operator=(const RedisMgr &) = delete;
    };
}
