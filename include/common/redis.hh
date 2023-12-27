#pragma once

#include <mutex>
#include <memory>

#include <sw/redis++/redis++.h>
#include <nlohmann/json.hpp>

#include <common/json.hh>

namespace springtail {
    /**
     * @brief Redis connection wrapper, derives from Redis object
     *        For adding future functionality
     */
    class RedisClient : public sw::redis::Redis 
    {
    public:
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

    /**
     * @brief Singleton redis client
     */
    class RedisMgr {
    public:
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
        inline std::shared_ptr<RedisClient> get_client() {
            return _redis;
        }

    protected:
        /** internal singleton instance */
        static RedisMgr *_instance;

        /** mutex protecting _instance creation */
        static std::mutex _instance_mutex;

        /** Redis client wrapper around client object */
        std::shared_ptr<RedisClient> _redis;

        RedisMgr();

        ~RedisMgr() {}

    private:
        // delete copy constructor
        RedisMgr(const RedisMgr &)       = delete;
        void operator=(const RedisMgr &) = delete;
    };
}