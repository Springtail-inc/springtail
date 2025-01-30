#pragma once

#include <algorithm>
#include <shared_mutex>
#include <thread>

#include <nlohmann/json.hpp>

#include <common/prefix_tree.hh>
#include <common/redis.hh>

namespace springtail {
    class RedisCache {
    public:
        enum RedisType {
            REDIS_TYPE_STRING,
            REDIS_TYPE_HASH,
            REDIS_TYPE_SET,
            REDIS_TYPE_NONE,    // For removed top-level keys
            REDIS_TYPE_UNSUPPORTED
        };

        RedisCache(bool config_db);
        ~RedisCache();

        class RedisCacheChangeCallback {
        public:
            virtual ~RedisCacheChangeCallback() = default;
            virtual void change_callback(const std::string &, const nlohmann::json &) = 0;
        };

        // The path will be turned into json_pointer, format should be:
        //      "<top level key without instance id and semicolon>/<rest of the path>"
        // Path items are separated by "/".
        // If the top level key is for a string: then it can be just the first key
        // If that string represented as json, then <rest of the path> can be a path in this
        //      json document
        // If the top level key is for a set: then it <rest of the path> can be empty to get the whole
        //      set or it should contain item id 0, 1, 2, etc. to address elements of the set,
        //      followed by path into specific set item
        // If the top level key is for a hash: then the first item in <rest of the path> should be
        //      a key into that hash followed by whatever we need to retrieve from json object
        nlohmann::json get_value(const std::string &path);

        // only adding or removing a single top-level array element at a time is supported
        // removing of top-level keys is not supported
        // adding removing keys for a top-level hash is not supported
        bool set_value(const std::string &path, const nlohmann::json &value);
        void add_callback(const std::string &path, const std::shared_ptr<RedisCacheChangeCallback> &cb);
        void remove_callback(const std::string &path, const std::shared_ptr<RedisCacheChangeCallback> &cb);
        size_t get_callback_count(const std::string &path);
        void dump();

    private:
        std::atomic<bool> _shutdown = false;
        std::shared_mutex _storage_mutex;
        nlohmann::json _storage;
        nlohmann::json _old_storage;
        std::shared_mutex _callback_mutex;
        PrefixNode<std::shared_ptr<RedisCacheChangeCallback>> _callbacks;
        uint64_t _instance_id;
        int _db_id;

        std::string _subscribe_pattern;         ///< subscriber pattern
        std::thread _subscriber_thread;         ///< subscriber thread
        std::thread::id _id;                    ///< subscriber thread id

        using SubscriberPtr = std::shared_ptr<sw::redis::Subscriber>;
        RedisClientPtr _client;
        SubscriberPtr _subscriber;

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

        // get json from storage without locking, the locking is done by the calling function
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

        static inline std::optional<std::reference_wrapper<const nlohmann::json>>
        _get_value(const std::string &path, const nlohmann::json &json_object)
        {
            nlohmann::json::json_pointer jptr(path);
            return _get_value(jptr, json_object);
        }

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

        void _process_notification(const std::string &pattern, const std::string &channel, const std::string &msg);
        void _process_diff(const nlohmann::json &diff, const std::string &top_level_path);
        void _init_storage();
        void _run();
        // perform redis reads on sepecific top level key
        std::tuple<nlohmann::json, RedisType> _read_key_value(const std::string &key);

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

        inline bool
        _is_array_path(const nlohmann::json::json_pointer &json_ptr, const nlohmann::json &storage)
        {
            std::optional<std::reference_wrapper<const nlohmann::json>> json_object_old = _get_value(json_ptr, storage);
            if (json_object_old.has_value() && json_object_old.value().get().type() != nlohmann::json::value_t::array) {
                return false;
            }
            return true;
        }

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

        inline bool
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

        inline bool
        _notify_path(const std::string &path)
        {
            if (_check_storage(path, _old_storage) || _check_storage(path, _storage)) {
                return true;
            }
            return false;
        }

        inline bool
        _find_in_array(const nlohmann::json &json_array, const nlohmann::json &json_element)
        {
            nlohmann::json::const_iterator it = std::find_if(json_array.begin(), json_array.end(),
                [&json_element](const nlohmann::json& item) {
                    return item == json_element;
                });
            return (it != json_array.end());
        }

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