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

        /** Helper to set key expiry */
    };
    using RedisClientPtr = std::shared_ptr<RedisClient>;

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
        inline RedisClientPtr get_client() {
            return _redis;
        }

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
        // delete copy constructor
        RedisMgr(const RedisMgr &)       = delete;
        void operator=(const RedisMgr &) = delete;
    };

    /**
     * @brief Redis queue, uses RedisMgr client
     *
     * Implements a producer / consumer queue in Redis with optional two-phase commit.  The
     * producer(s) calls push() to add entries to the queue.  Consumer(s) call pop() to consume an
     * entry off of the queue.  Once processing of the entry is complete, the consumer calls
     * commit() to complete the operation or abort() to fail the operation and return it to the
     * queue.  If two-phase commit is not required, a consumer can also call pop_and_commit() which
     * will atomically perform a pop() and commit().
     *
     * @tparam T value type, should implement std::string serialize().
     */
    template<typename T>
    class RedisQueue {
    public:
        explicit RedisQueue(const std::string &key)
            : _key(key),
              _redis(RedisMgr::get_instance()->get_client())
        { }

        uint64_t size()
        {
            return _redis->llen(_key);
        }

        /**
         * @brief Push item onto queue.
         * @param key list key
         * @param value value to queue
         * @return uint64_t items on list
         */
        uint64_t push(const T &value)
        {
            return _redis->lpush(_key, value.serialize());
        }

        /**
         * @brief Pop item from queue (list).
         *
         * To support a two-phase commit, we move the item from the primary queue to separate list
         * for the specific worker.  When the work for an item is complete, then the worker must
         * call commit() to commit the work item and remove it from it's list.  Or call abort() to
         * return the work item to the primary queue.
         *
         * @param worker_id The unique ID of the worker.
         * @param timeout_sec timeout in seconds (0=block forever)
         * @return A pointer to the value if a value was received, nullptr if timedout
         */
        std::shared_ptr<T> pop(const std::string &worker_id, uint64_t timeout_sec=0)
        {
            std::string worker_key = fmt::format("{}:{}", _key, worker_id);

            // remove from the main queue and move to the worker queue
            auto &&res = _redis->brpoplpush(_key, worker_key, timeout_sec);
            // note: blmove() not available yet in redis++
            // auto &&res = _redis->blmove(_key, worker_key, "RIGHT", "LEFT", timeout_sec);
            if (res) {
                return std::make_shared<T>(*res);
            }
            return nullptr;
        }

        /**
         * @brief Commit a worker's active item.
         *
         * Removes the worker's item from it's separate list to complete the two-phase commit.
         *
         * @param worker_id The unique ID of the worker.
         */
        void commit(const std::string &worker_id)
        {
            std::string worker_key = fmt::format("{}:{}", _key, worker_id);
            _redis->rpop(worker_key);
        }

        /**
         * @brief Abort a worker's active item.
         *
         * Removes the worker's item from it's separate list and returns it to the central queue to
         * be re-processed.  Places the work item at the front of the queue to ensure it is worked
         * on next.
         *
         * @param worker_id The unique ID of the worker.
         */
        void abort(const std::string &worker_id)
        {
            std::string worker_key = fmt::format("{}:{}", _key, worker_id);
            _redis->rpoplpush(worker_key, _key);
            // note: lmove() not available yet in redis++
            // _redis->lmove(worker_key, _key, "RIGHT", "LEFT");
        }

        /**
         * @brief Logically performs a commmit() and then a push() of that element onto another queue.
         */
        void commit_and_move(const std::string &worker_id,
                             const std::string &queue)
        {
            std::string worker_key = fmt::format("{}:{}", _key, worker_id);
            _redis->rpoplpush(worker_key, queue);
        }

        /**
         * @brief Logically performs a pop() and complete() in a single operation.
         *
         * @param timeout_sec timeout in seconds (0=block forever)
         * @return A pointer to the value if a value was received, nullptr if timedout
         */
        std::shared_ptr<T> pop_and_commit(uint64_t timeout_sec=0)
        {
            // returns an optional pair, no value if timeout first=key, second=value
            auto &&res = _redis->brpop(_key, timeout_sec);
            if (res) {
                return std::make_shared<T>(res->second);
            }
            return nullptr;
        }

        /**
         * List all the values in redis queue.  Note that they are returned in reverse insert order
         * due to the use of brpoplpush().  We can change this behavior if redisplusplus implements
         * lmove() and blmove() calls available in Redis 6.2.
         */
        std::vector<T> range(long long start=0, long long end=-1)
        {
            std::vector<std::string> values;
            _redis->lrange(_key, start, end, std::back_inserter(values));
            std::vector<T> result;
            for (auto &value: values) {
                result.push_back(T(value));
            }
            return result;
        }

        /**
         * @brief Clear the queue.
         */
        void clear()
        {
            _redis->del(_key);
        }

    private:
        std::string _key;       ///< The unique key within Redis for this queue.
        RedisClientPtr _redis; ///< A connection to Redis.
    };

    /**
     * Implements an interface to maintain a sorted set of unique values within Redis.  Each value
     * is assigned a score when added, which defines it's position within the set.
     */
    template<typename T>
    class RedisSortedSet {
    public:
        explicit RedisSortedSet(const std::string &key)
            : _key(key)
        { }

        /**
         * @brief Add item to set
         * @param value value to add
         * @return uint64_t number of items in set
         */
        uint64_t add(const T &value, const uint64_t score=0)
        {
            std::string value_string = value.serialize();
            return RedisMgr::get_instance()->get_client()->zadd(_key, value_string, score);
        }

        /**
         * @brief Remove item from set
         * @param value value to remove
         * @return uint64_t number of items removed
         */
        uint64_t remove(const T &value)
        {
            std::string value_string = value.serialize();
            return RedisMgr::get_instance()->get_client()->zrem(_key, value_string);
        }

        /**
         * @brief Get items in set by index
         * @param start start index (0 for first item, -1 for last item)
         * @param stop stop index (inclusive, -1 for all items)
         * @return std::vector<T> set items
         */
        std::vector<T> get(const uint64_t start=0, uint64_t stop=-1)
        {
            std::vector<std::string> values;
            RedisMgr::get_instance()->get_client()->zrange(_key, start, stop, std::back_inserter(values));
            std::vector<T> result;
            for (auto &value: values) {
                result.push_back(T(value));
            }
            return result;
        }

        /**
         * @brief Get items in a set by score
         * @param min minimum score (inclusive)
         * @param max maximum score (inclusive)
         * @return std::vector<T>
         */
        std::vector<T> get_by_score(const uint64_t min, const uint64_t max=-1)
        {
            std::vector<std::string> values;

            if (max >= 0) {
                sw::redis::BoundedInterval<double> interval(min, max, sw::redis::BoundType::CLOSED);
                RedisMgr::get_instance()->get_client()->zrangebyscore(_key, interval, std::back_inserter(values));
            } else {
                sw::redis::LeftBoundedInterval<double> interval(min, sw::redis::BoundType::RIGHT_OPEN);
                RedisMgr::get_instance()->get_client()->zrangebyscore(_key, interval, std::back_inserter(values));
            }

            std::vector<T> result;
            for (auto &value: values) {
                result.push_back(T(value));
            }
            return result;
        }

        void remove_by_score(const uint64_t min, const uint64_t max=-1)
        {
            sw::redis::BoundedInterval<double> interval(min, max, sw::redis::BoundType::CLOSED);
            RedisMgr::get_instance()->get_client()->zremrangebyscore(_key, interval);
        }

    private:
        std::string _key; ///< The key of the sorted set.
    };

}
