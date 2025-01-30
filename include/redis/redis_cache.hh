#pragma once

#include <algorithm>
#include <shared_mutex>
#include <thread>

#include <nlohmann/json.hpp>

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
         *      NOTE: This function does not support adding or removing a top level key, but not its removal
         *          We should be able to change it to support adding functionality, but not removal.
         *          To remove a top-level key a separate function is needed.
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
        void add_callback(const std::string &path, const std::shared_ptr<RedisCacheChangeCallback> &cb);

        /**
         * @brief Remove callback for the specified path notifications
         *
         * @param path - path into json document
         * @param cb - callback class derived from RedisCacheChangeCallback
         */
        void remove_callback(const std::string &path, const std::shared_ptr<RedisCacheChangeCallback> &cb);

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

    private:
        std::atomic<bool> _shutdown = false;        ///> flag to signal to subscriber thread to shutdown
        std::shared_mutex _storage_mutex;           ///> mutex for _storage and _old_storage access
        nlohmann::json _storage;                    ///> json document that holds all the values stored in redis
        nlohmann::json _old_storage;                ///> old storage is used when we change something in storage
        std::shared_mutex _callback_mutex;          ///> mutex for _callbacks access
        PrefixNode<std::shared_ptr<RedisCacheChangeCallback>> _callbacks; ///> prefix tree for storing callback objects
        uint64_t _instance_id;                      ///> database instance id
        int _db_id;                                 ///> redis database id

        std::string _subscribe_pattern;         ///< subscriber pattern
        std::thread _subscriber_thread;         ///< subscriber thread
        std::thread::id _id;                    ///< subscriber thread id

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
         * @brief Set the value of specified json object at specified json path to specified json value
         *
         * @param json_ptr - json pointer
         * @param json_object - json object
         * @param json_value - json value
         * @return true - on success
         * @return false - on failure
         */
        static inline bool
        _set_value(const nlohmann::json::json_pointer &json_ptr, nlohmann::json &json_object, const nlohmann::json &json_value)
        {
            try {
                json_object[json_ptr] = json_value;
                return true;
            } catch (const nlohmann::json::out_of_range& e) {
                return false;
            } catch (const nlohmann::json::parse_error& e) {
                return false;
            }
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
         * @brief Process diff for the given top-level path
         *
         * @param diff - json diff
         * @param top_level_path - path string
         */
        void _process_diff(const nlohmann::json &diff, const std::string &top_level_path);

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
         * @brief Check if this path goes into an array object for the given json pointer.
         *      Both _storage and _old_storage are examined.
         *
         * @param json_ptr - json pointer
         * @return true
         * @return false
         */
        inline bool
        _is_array_path(const nlohmann::json::json_pointer &json_ptr)
        {
            std::optional<std::reference_wrapper<const nlohmann::json>> json_object_old = _get_value(json_ptr, _old_storage);
            if (json_object_old.has_value() && json_object_old.value().get().type() != nlohmann::json::value_t::array) {
                return false;
            }
            std::optional<std::reference_wrapper<const nlohmann::json>> json_object_new = _get_value(json_ptr, _storage);
            if (json_object_new.has_value() && json_object_new.value().get().type() != nlohmann::json::value_t::array) {
                return false;
            }
            return true;
        }

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
         * @brief Check if there is an array inside of json path
         *
         * @param path - json path
         * @return true
         * @return false
         */
        inline bool
        _check_array_in_path(const std::string &path)
        {
            nlohmann::json::json_pointer json_ptr(path);

            while (true) {
                if (_is_array_path(json_ptr)) {
                    return true;
                }
                if (!json_ptr.empty()) {
                    json_ptr = json_ptr.parent_pointer();
                } else {
                    break;
                }
            }
            return false;
        }

        /**
         * @brief This function performs finds a json value for the given path and performs callback using callback object
         *
         * @param path - path to json object
         * @param prefix - prefix string
         * @param cb_object - callback object
         */
        inline void
        _perform_callback(const std::string &path, const std::string &prefix, std::shared_ptr<springtail::RedisCache::RedisCacheChangeCallback> cb_object) {
            std::string item_path = path.substr(prefix.length());
            std::optional<std::reference_wrapper<const nlohmann::json>> json_optional_object = _get_value(path, _storage);
            if (json_optional_object.has_value()) {
                cb_object->change_callback(item_path, json_optional_object.value().get());
            } else {
                cb_object->change_callback(item_path, {});
            }
        }

        /**
         * @brief Verify if the specified path is in json object
         *
         * @param path - json path
         * @param storage - storage json object
         * @return true
         * @return false
         */
        static inline bool
        _check_storage(const std::string &path, const nlohmann::json &storage)
        {
            nlohmann::json::json_pointer path_ptr(path);
            if (storage.contains(path_ptr)) {
                std::deque<std::string> json_path_queue;
                common::split_string("/", path.substr(1), json_path_queue);
                std::string storage_path;

                while (!json_path_queue.empty()) {
                    const std::string &path_item = json_path_queue.front();
                    storage_path += "/" + path_item;
                    json_path_queue.pop_front();
                    nlohmann::json::json_pointer storage_json_ptr(storage_path);
                    if (_is_array_path(storage_json_ptr, storage)) {
                        if (!json_path_queue.empty()) {
                            return false;
                        }
                    }
                }
                return true;
            }
            return false;
        }

        /**
         * @brief Figure out the condition for triggering path notification for given path
         *
         * @param path - json path
         * @return true
         * @return false
         */
        inline bool
        _notify_path(const std::string &path)
        {
            if (_check_storage(path, _old_storage) || _check_storage(path, _storage)) {
                return true;
            }
            return false;
        }

        /**
         * @brief Find out if the item exists in the given json array.
         *
         * @param json_array - json array
         * @param json_element - json element
         * @return true
         * @return false
         */
        static inline bool
        _find_in_array(const nlohmann::json &json_array, const nlohmann::json &json_element)
        {
            assert(json_array.type() == nlohmann::json::value_t::array);
            nlohmann::json::const_iterator it = std::find_if(json_array.begin(), json_array.end(),
                [&json_element](const nlohmann::json& item) {
                    return item == json_element;
                });
            return (it != json_array.end());
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
        _array_diff(const nlohmann::json &arr1, const nlohmann::json &arr2, bool arr1_unique = true, bool arr2_unique = true) {
            assert(arr1.type() == nlohmann::json::value_t::array);
            assert(arr2.type() == nlohmann::json::value_t::array);

            std::vector<std::string> u, v;
            for (uint32_t i = 0; i < arr1.size(); i++) {
                u.push_back(nlohmann::to_string(arr1[i]));
            }
            for (uint32_t i = 0; i < arr2.size(); i++) {
                v.push_back(nlohmann::to_string(arr2[i]));
            }
            std::sort(u.begin(), u.end());
            std::sort(v.begin(), v.end());

            // remove unique elements if required
            if (arr1_unique) {
                auto last = std::unique(u.begin(), u.end());
                u.erase(last, u.end());
            }
            if (arr2_unique) {
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
            return std::make_pair(u, v);
        }
    };
};