#pragma once

#include <mutex>
#include <memory>
#include <string>
#include <optional>
#include <type_traits>

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
        RedisQueue(const std::string &key)
            : _key(key),
              _redis(RedisMgr::get_instance()->get_client())
        { }

        /**
         * @brief Push item onto queue.
         * @param key list key
         * @param value value to queue
         * @return uint64_t items on list
         */
        uint64_t push(const T &value)
        {
            return _redis->rpush(_key, value.serialize());
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
         * @param timeout_secs timeout in seconds (0=block forever)
         * @return A pointer to the value if a value was received, nullptr if timedout
         */
        std::shared_ptr<T> pop(uint64_t worker_id, uint64_t timeout_secs=0)
        {
            std::string worker_key = fmt::format("{}:{}", _key, worker_id);

            // remove from the main queue and move to the worker queue
            auto &&res = _redis->blmove(_key, worker_key, "LEFT", "RIGHT", timeout_secs);
            if (res) {
                return std::make_shared<T>(res->second);
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
        void commit(uint64_t worker_id)
        {
            std::string worker_key = fmt::format("{}:{}", _key, worker_id);
            _redis->lpop(worker_key);
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
        void abort(uint64_t worker_id)
        {
            std::string worker_key = fmt::format("{}:{}", _key, worker_id);
            _redis->lmove(worker_key, _key, "LEFT", "LEFT");
        }

        /**
         * @brief Logically performs a pop() and complete() in a single operation.
         *
         * @param timeout_secs timeout in seconds (0=block forever)
         * @return A pointer to the value if a value was received, nullptr if timedout
         */
        std::shared_ptr<T> pop_and_commit(uint64_t timeout_sec=0)
        {
            // returns an optional pair, no value if timeout first=key, second=value
            auto &&res = _redis->blpop(key, timeout_secs);
            if (res) {
                return std::make_shared<T>(res->second);
            }
            return nullptr;
        }

    private:
        std::string _key; ///< The unique key within Redis for this queue.
        std::shared_ptr<RedisClient> _redis; ///< A connection to Redis.
    };
}
