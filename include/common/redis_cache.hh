#pragma once

#include <shared_mutex>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include <sw/redis++/redis++.h>

#include <common/common.hh>
#include <common/prefix_tree.hh>
#include <common/redis.hh>

namespace springtail {
    /**
     * @brief Class represents an implementation of the redis storage caching mechanism
     *
     */
    class RedisCache {
    public:

        /**
         * @brief enumeration for redis top-level key associated types
         *
         */
        enum RedisType {
            REDIS_TYPE_STRING,      ///> string key
            REDIS_TYPE_HASH,        ///> hash key
            REDIS_TYPE_SET,         ///> set key
            REDIS_TYPE_NONE,        ///> the key type becomes none after it has been removed
            REDIS_TYPE_UNSUPPORTED  ///> all the other key types that are not supported by the cache
        };

        /**
         * @brief Construct a new Redis Cache object
         *
         * @param config_db - specify if this is for config or data database
         *              database is determined internally based on this flag
         */
        explicit RedisCache(bool config_db);

        /**
         * @brief Destroy the Redis Cache object
         *
         */
        ~RedisCache();

        /**
         * @brief This class is for holding callback function. Multiple callbacks will be
         *      stored in the prefix tree. To receive change notifications, other classes
         *      need to inherit from this class and override change_callback function.
         *
         */
        class RedisCacheChangeCallback {
        public:
            /**
             * @brief Destroy the Redis Cache Change Callback object
             *
             */
            virtual ~RedisCacheChangeCallback() = default;

            /**
             * @brief Callback function that should be overriden to receive notifications
             *
             */
            virtual void change_callback(const std::string &, const nlohmann::json &) = 0;
        };

        using RedisCacheChangeCallbackPtr = std::shared_ptr<RedisCacheChangeCallback>;

        /**
         * @brief This is another callback helper class. When there is no class that can meaningfully inherit from
         *      RedisCacheChangeCallback, this class, RedisChangeWatcher, can be used to pass a callback function
         *      for redis database notifications. This callback function can also be changed any time without
         *      removing callback object from redis cache and adding a new one back.
         *
         */
        class RedisChangeWatcher : public RedisCacheChangeCallback {
        public:
            /**
             * @brief Construct a new Redis Change Watcher object
             *
             * @param func - callback function
             */
            explicit RedisChangeWatcher(std::function<void(const std::string &path, const nlohmann::json &new_value)> func) : _cb(func) {}
            /**
             * @brief Destroy the Redis Change Watcher object
             *
             */
            ~RedisChangeWatcher() override = default;

            /**
             * @brief Callback that overrides the callback function from the base class
             *
             * @param path - notification path
             * @param new_value - new json value
             */
            void change_callback(const std::string &path, const nlohmann::json &new_value) override {
                SPDLOG_DEBUG("!!! Received notification; path: \"{}\"; new value: {}", path, new_value.dump(4));
                _cb(path, new_value);
            }

            /**
             * @brief Set a different callback function
             *
             * @param func - callback function
             */
            void set_cb(std::function<void(const std::string &path, const nlohmann::json &new_value)> func) {
                _cb = func;
            }
        private:
            /**
             * @brief Callback function storage
             *
             */
            std::function<void(const std::string &, const nlohmann::json &)> _cb;
        };

        using RedisChangeWatcherPtr = std::shared_ptr<RedisChangeWatcher>;


        /**
         * @brief Get the value of json object at the specified path.
         *      The path will be turned into a json_pointer, format should be:
         *          "<top level key without instance id and semicolon>/<rest of the path>"
         *      Path items are separated by "/".
         *      If the top level key is for a string: then it can be just the first key
         *      If that string represented as json, then <rest of the path> can be a path into this
         *          json document.
         *      If the top level key is for a set: then <rest of the path> can be empty to get the whole
         *          set or it should contain item id 0, 1, 2, etc. to address individual elements of the set,
         *          that can be followed by path into specific set item
         *      If the top level key is for a hash: then the first item in <rest of the path> should be
         *          a key into that hash followed by whatever we need to retrieve from json object
         *
         * @param path - path into json document that complies with the specified criteria
         * @return nlohmann::json
         */
        nlohmann::json get_value(const std::string &path);

        /**
         * @brief Set the value of the object
         *      NOTE: This function does not support adding adding top level key,
         *          but it can remove it if you remove the last item in the top level array or hash,
         *          set string to an empty json object.
         *          We should also be able to change it to support adding functionality for top
         *          level keys if so desired.
         *
         * @param path - path into json document
         * @param value - json value that it should be set to
         * @return true - on success
         * @return false - on failure
         */
        bool set_value(const std::string &path, const nlohmann::json &value);

        /**
         * @brief Add callback for the specified path notifications
         *
         * @param path - path into json document
         * @param cb - callback class derived from RedisCacheChangeCallback
         */
        void add_callback(const std::string &path, const RedisCacheChangeCallbackPtr &cb);

        /**
         * @brief Remove callback for the specified path notifications
         *
         * @param path - path into json document
         * @param cb - callback class derived from RedisCacheChangeCallback
         */
        void remove_callback(const std::string &path, const RedisCacheChangeCallbackPtr &cb);

        /**
         * @brief Get the total number of callbacks registered with RedisCache for the specified path.
         *
         * @param path - path into json document
         * @return size_t - number of callbacks
         */
        size_t get_callback_count(const std::string &path);

        /**
         * @brief Dump all the redis values stored by RedisCache
         *
         */
        void dump();

        /**
         * @brief This function uses redis client object to send publish notifications to redis. This
         *      function is needed only temporarily till we transition to using redis notifications
         *      used by the cache.
         *
         * @param channel_template - channel pattern
         * @param message - message to send
         * @return long long - return value from redis client API call
         */
        long long publish(const std::string &channel_template, const std::string_view &message);

        /**
         * @brief This function modifies input vectors by doing subtracting intersection from the union of these
         *      vector elements. Each vector is left with the elements not found in both. The vectors are first
         *      sorted, then if they are market as unique, all non-unique elements are removed. After that
         *      the common elements are removed.
         *
         * @tparam T - object type for vector container
         * @param u - first vector
         * @param v - second vector
         * @param u_unique - is first vector unique
         * @param v_unique - is second vector unique
         */
        template<typename T>
        static void
        array_diff(std::vector<T> &u, std::vector<T> &v, bool u_unique, bool v_unique) {
            std::sort(u.begin(), u.end());
            std::sort(v.begin(), v.end());

            // remove unique elements if required
            if (u_unique) {
                auto last = std::unique(u.begin(), u.end());
                u.erase(last, u.end());
            }
            if (v_unique) {
                auto last = std::unique(v.begin(), v.end());
                v.erase(last, v.end());
            }

            uint32_t i = 0;
            uint32_t j = 0;

            while ((u.begin() + i) != u.end() && (v.begin() + j) != v.end()) {
                if (u[i] == v[j]) {
                    u.erase(u.begin() + i);
                    v.erase(v.begin() + j);
                } else if (u[i] < v[j]) {
                    i++;
                } else {
                    j++;
                }
            }
        }

    private:
        std::atomic<bool> _shutdown = false;        ///> flag to signal to subscriber thread to shutdown
        std::shared_mutex _storage_mutex;           ///> mutex for _storage and _old_storage access
        nlohmann::json _storage;                    ///> json document that holds all the values stored in redis
        nlohmann::json _old_storage;                ///> old storage is used when we change something in storage
        std::shared_mutex _callback_mutex;          ///> mutex for _callbacks access
        PrefixNode<RedisCacheChangeCallbackPtr> _callbacks; ///> prefix tree for storing callback objects
        uint64_t _instance_id;                      ///> database instance id
        int _db_id;                                 ///> redis database id

        std::string _subscribe_pattern;         ///< subscriber pattern
        std::thread _subscriber_thread;         ///< subscriber thread
        std::thread::id _id;                    ///< subscriber thread id
        std::atomic<bool>  _init_finished{false};   ///< finished initialization flag

        using SubscriberPtr = std::shared_ptr<sw::redis::Subscriber>;
        RedisClientPtr _client;                 ///< redis client to read from and write to redis
        SubscriberPtr _subscriber;              ///< redis subscriber used to listen to redis notifications

        /**
         * @brief Convert string value to redis type enum
         *
         * @param type_string - string value of the type
         * @return RedisType - redis type enum value
         */
        static RedisType
        _string_to_type(const std::string& type_string)
        {
            static std::map<std::string, RedisType> string_to_type_map = {
                {"string",  REDIS_TYPE_STRING},
                {"hash",    REDIS_TYPE_HASH},
                {"set",     REDIS_TYPE_SET},
                {"none",    REDIS_TYPE_NONE}
            };
            if (string_to_type_map.contains(type_string)) {
                return string_to_type_map[type_string];
            }
            return REDIS_TYPE_UNSUPPORTED;
       }

        /**
         * @brief Internal function for extracting values from json object for given json pointer
         *
         * @param json_ptr - json pointer
         * @param json_object - json object
         * @return std::optional<std::reference_wrapper<const nlohmann::json>> - optional return json value
         */
        static inline std::optional<std::reference_wrapper<const nlohmann::json>>
        _get_value(const nlohmann::json::json_pointer &json_ptr, const nlohmann::json &json_object)
        {
            try {
                return json_object.at(json_ptr);
            } catch (const nlohmann::json::out_of_range& e) {
                return {};
            } catch (const nlohmann::json::parse_error& e) {
                return {};
            }
        }

        /**
         * @brief Internal function for extracting values from json object for given json pointer string
         *
         * @param json_ptr - json pointer
         * @param json_object - json object
         * @return std::optional<std::reference_wrapper<const nlohmann::json>> - optional return json value
         */
        static inline std::optional<std::reference_wrapper<const nlohmann::json>>
        _get_value(const std::string &path, const nlohmann::json &json_object)
        {
            nlohmann::json::json_pointer jptr(path);
            return _get_value(jptr, json_object);
        }

        /**
         * @brief Process notification for given pattern, channel, and message. This function is called by
         *          the subscriber thread.
         *
         * @param pattern - notification pattern
         * @param channel - notification channel
         * @param msg - notification message
         */
        void _process_notification(const std::string &pattern, const std::string &channel, const std::string &msg);

        /**
         * @brief Process meta notifications. This function allows to syncronize the start of consume
         *          thread and the main thread by receiving server acknowledgment of channel subscription.
         *
         * @param type - subscriber message type
         * @param channel - subscriber channel
         * @param num - some id
         */
        void _process_meta(sw::redis::Subscriber::MsgType type, sw::redis::OptionalString channel, long long num);

        /**
         * @brief Process diff for the given top-level path
         *
         * @param diff - json diff
         * @param top_level_path - path string
         */
        void _process_diff(const nlohmann::json &diff, const std::string &top_level_path, std::unique_lock<std::shared_mutex> &storage_lock);

        /**
         * @brief Initialize storage from redis database
         *
         */
        void _init_storage();

        /**
         * @brief Run subscriber thread.
         *
         */
        void _run();

        /**
         * @brief Read the value for the given top level key from redis
         *
         * @param key
         * @return std::tuple<nlohmann::json, RedisType>
         */
        std::tuple<nlohmann::json, RedisType> _read_key_value(const std::string &key);

        /**
         * @brief Check if this path goes into an array object for the give json pointer and json storage.
         *
         * @param json_ptr - json pointer
         * @param storage - json storage
         * @return true
         * @return false
         */
        static inline bool
        _is_array_path(const nlohmann::json::json_pointer &json_ptr, const nlohmann::json &storage)
        {
            std::optional<std::reference_wrapper<const nlohmann::json>> json_object_old = _get_value(json_ptr, storage);
            if (json_object_old.has_value() && json_object_old.value().get().type() != nlohmann::json::value_t::array) {
                return false;
            }
            return true;
        }

        /**
         * @brief Figure out the difference between two json arrays. Ensure uniqueness per array. When uniqueness flag turned
         *      on for the array, all non-unique elements will be removed from this array before comparing it to the other array.
         *
         * @param arr1 - first json array
         * @param arr2 - second json array
         * @param arr1_unique - first json array uniqueness
         * @param arr2_unique - second json array uniqueness
         * @return std::pair<std::vector<std::string>, std::vector<std::string>> - the first element of the pair contains
         *          the elements of the first array that are not in the second array, the second element of the pair contains
         *          the elements of the second array that are not in the first array
         */
        static inline std::pair<std::vector<std::string>, std::vector<std::string>>
        _array_diff(const nlohmann::json &arr1, const nlohmann::json &arr2, bool arr1_unique = true, bool arr2_unique = true);

        /**
         * @brief Find if the given json pointer has array in the json path it points to in the give storage
         *
         * @param json_ptr - json pointer
         * @param storage - json storage to check for array
         * @return true
         * @return false
         */
        static bool
        _has_array_in_path(const nlohmann::json::json_pointer &json_ptr, const nlohmann::json &storage)
        {
            nlohmann::json::json_pointer local_json_ptr = json_ptr;
            while (true) {
                std::optional<std::reference_wrapper<const nlohmann::json>> json_object = _get_value(local_json_ptr, storage);
                if (json_object.has_value() && json_object.value().get().type() == nlohmann::json::value_t::array) {
                    return true;
                }
                if (!local_json_ptr.empty()) {
                    local_json_ptr = local_json_ptr.parent_pointer();
                } else {
                    break;
                }
            }
            return false;
        }

        /**
         * @brief Find if the given json pointer has array in the json path it points to in the give storage
         *
         * @param path - json pointer in string form
         * @param storage - json storage to check for array
         * @return true
         * @return false
         */
        static inline bool
        _has_array_in_path(const std::string &path, const nlohmann::json &storage)
        {
            nlohmann::json::json_pointer json_ptr(path);
            return _has_array_in_path(json_ptr, storage);
        }

        /**
         * @brief Find out if json object pointed by the given json pointer is inside an array
         *
         * @param path - json pointer as string
         * @param storage - json object for look up
         * @return true
         * @return false
         */
        static inline bool
        _inside_array_path(const std::string &path, const nlohmann::json &storage)
        {
            nlohmann::json::json_pointer json_ptr(path);
            return _has_array_in_path(json_ptr.parent_pointer(), storage);
        }

        /**
         * @brief Get the json pointer in string form to the first array from the top if found
         *
         * @param path - json pointer
         * @param storage - json object for lookup
         * @return std::string - json pointer to the first array inside the path
         */
        static inline std::string
        _get_array_path(const std::string &path, const nlohmann::json &storage);

        /**
         * @brief Convert json object to an appropriate string
         *
         * @param json_value - json value
         * @return std::string - output string
         */
        static inline std::string
        _json_to_string(const nlohmann::json &json_value);

        /**
         * @brief Covert string value to json that is either object or array or string.
         *      No other types are allowed.
         *
         * @param string_value - input value
         * @return nlohmann::json
         */
        static inline nlohmann::json
        _string_to_json(const std::string &string_value);
    };
};