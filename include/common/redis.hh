#pragma once

#include <mutex>
#include <memory>
#include <string>
#include <optional>
#include <type_traits>
#include <vector>

#include <sw/redis++/redis++.h>
#include <nlohmann/json.hpp>

#include <common/json.hh>

namespace springtail {
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

    /**
     * @brief Redis queue, uses RedisMgr client
     * @tparam T value type, should implement std::string serialize().
     */
    template<typename T>
    class RedisQueue {
    public:
        /**
         * @brief Push item onto queue (list)
         * @param key list key
         * @param value value to queue
         * @return uint64_t items on list
         */
        uint64_t push(const std::string &key, const T &value)
        {
            std::string value_string = value.serialize();
            std::cout << "Pushing: key=" << key << ", value=" << value_string << "\n";
            return RedisMgr::get_instance()->get_client()->rpush(key, value_string);
        }

        /**
         * @brief Pop item from queue (list)
         * @param key list key
         * @param timeout_secs timeout in seconds (0=block forever)
         * @return true if value was received, false if timedout
         */
        std::shared_ptr<T> pop(const std::string &key, uint64_t timeout_secs=0)
        {
            // returns an optional pair, no value if timeout first=key, second=value
            auto &&res = RedisMgr::get_instance()->get_client()->blpop(key, timeout_secs);
            if (res) {
                return std::make_shared<T>(res->second);
            }
            return nullptr;
        }

        /** list all values in redis queue */
        std::vector<T> range(const std::string &key, long long start=0, long long end=-1)
        {
            std::vector<std::string> values;
            RedisMgr::get_instance()->get_client()->lrange(key, start, end, std::back_inserter(values));
            std::vector<T> result;
            for (auto &value: values) {
                result.push_back(T(value));
            }
            return result;
        }
    };

    template<typename T>
    class RedisSortedSet {
    public:
        /**
         * @brief Add item to set
         * @param key set key
         * @param value value to add
         * @return uint64_t number of items in set
         */
        #include <vector>

        uint64_t add(const std::string &key, const T &value, const uint64_t score=0)
        {
            std::string value_string = value.serialize();
            return RedisMgr::get_instance()->get_client()->zadd(key, value_string, score);
        }

        /**
         * @brief Remove item from set
         * @param key set key
         * @param value value to remove
         * @return uint64_t number of items removed
         */
        uint64_t remove(const std::string &key, const T &value)
        {
            std::string value_string = value.serialize();
            return RedisMgr::get_instance()->get_client()->zrem(key, value_string);
        }

        /**
         * @brief Get items in set by index
         * @param key set key
         * @param start start index (0 for first item, -1 for last item)
         * @param stop stop index (inclusive, -1 for all items)
         * @return std::vector<T> set items
         */
        std::vector<T> get(const std::string &key, const uint64_t start=0, uint64_t stop=-1)
        {
            std::vector<std::string> values;
            RedisMgr::get_instance()->get_client()->zrange(key, 0, -1, std::back_inserter(values));
            std::vector<T> result;
            for (auto &value: values) {
                result.push_back(T(value));
            }
            return result;
        }

        /**
         * @brief Get items in a set by score
         * @param key set key
         * @param min minimum score (inclusive)
         * @param max maximum score (inclusive)
         * @return std::vector<T>
         */
        std::vector<T> get_by_score(const std::string &key, const uint64_t min, const uint64_t max)
        {
            std::vector<std::string> values;
            sw::redis::BoundedInterval<double> interval(min, max, sw::redis::BoundType::CLOSED);
            RedisMgr::get_instance()->get_client()->zrangebyscore(key, interval, std::back_inserter(values));
            std::vector<T> result;
            for (auto &value: values) {
                result.push_back(T(value));
            }
            return result;
        }
    };

}