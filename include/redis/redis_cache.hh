#pragma once

#include <functional>
#include <list>
#include <map>
#include <queue>
#include <shared_mutex>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include <common/redis.hh>

namespace springtail {
    // TODO: this is an implementation of a prefix tree. This should probably go to common/prefix_tree.hh
    template <typename T>
    class TreeNode {
    public:
        TreeNode() {}

        // the values won't be compared to previously added values
        // if the same value is added twice, then it will be there twice and
        // would have to be removed twice
        void add_item(std::deque<std::string> &node_path, const T &value) {
            if (node_path.empty()) {
                _values.push_back(value);
            } else {
                std::string top_key = node_path.front();
                node_path.pop_front();
                if (!_children.contains(top_key)) {
                    _children[top_key] = std::make_shared<TreeNode<T>>();
                }
                _children[top_key]->add_item(node_path, value);
            }
        }

        // if the path is wrong and points to non-existing branches, nothing will happen
        void remove_item(std::deque<std::string> &node_path, const T &value) {
            if (node_path.empty()) {
                auto iter = std::find(_values.begin(), _values.end(), value);
                if (iter != _values.end()) {
                    _values.erase(iter);
                }
            } else {
                std::string top_key = node_path.front();
                node_path.pop_front();
                if (_children.contains(top_key)) {
                    _children[top_key]->remove_item(node_path, value);
                }
            }
        }

        void collect_items(std::vector<std::pair<std::string, T>> &out_queue, const std::string path, std::deque<std::string> &node_path,
                std::function<bool (const T&)> select_fun) {
            // get the values from the current node
            for (const auto & value: _values) {
                if (select_fun(value)) {
                    out_queue.push_back(std::make_pair(path, value));
                }
            }
            if (node_path.empty()) {
                // once the in_queue is empty, walk the tree depth first
                for (const auto &child: _children) {
                    std::string next_path = path + "/" + child.first;
                    child.second->collect_items(out_queue, next_path, node_path, select_fun);
                }
            } else {
                // as long as the in_queue is not empty, follow the path
                std::string top_key = node_path.front();
                node_path.pop_front();
                if (_children.contains(top_key)) {
                    std::string next_path = path + "/" + top_key;
                    _children[top_key]->collect_items(out_queue, next_path, node_path, select_fun);
                }
            }
        }

        size_t count_items(std::deque<std::string> &node_path, std::function<bool (const T&)> select_fun) {
            size_t count = 0;
            for (const auto & value: _values) {
                if (select_fun(value)) {
                    count++;
                }
            }
            if (node_path.empty()) {
                // once the the in_queue is empty, walk the tree depth first
                for (const auto &child: _children) {
                    count += child.second->count_items(node_path, select_fun);
                }
            } else {
                // as long as the in_queue is not empty, follow the path
                std::string top_key = node_path.front();
                node_path.pop_front();
                if (_children.contains(top_key)) {
                    count += _children[top_key]->count_items(node_path, select_fun);
                }
            }
            return count;
        }

        size_t size() {
            std::deque<std::string> node_path = {};
            return count_items(node_path, [](const T&){ return true; });
        }
    private:
        std::map<std::string, std::shared_ptr<TreeNode<T>>> _children;
        std::list<T> _values;
    };

    class RedisCache {
    public:
        enum Action: uint32_t {
            INVALID = 0x00,
            ADD     = 0x01,
            REMOVE  = 0x02,
            REPLACE = 0x04
        };

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
            virtual void change_callback(const std::string &, Action, const nlohmann::json &) = 0;
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

        // TODO: implement set_values()
        // void set_value(const nlohmann::json::json_pointer &jptr, const nlohmann::json &value);
        void add_callback(const std::string &path, uint32_t action_mask, std::shared_ptr<RedisCacheChangeCallback> cb);
        void remove_callback(const std::string &path, uint32_t action_mask, std::shared_ptr<RedisCacheChangeCallback> cb);
        size_t get_callback_count(const std::string &path, uint32_t action_mask);
        size_t get_callback_total_count(const std::string &path);
        void dump();

    private:
        std::atomic<bool> _shutdown = false;
        std::shared_mutex _storage_mutex;
        nlohmann::json _storage;
        std::shared_mutex _callback_mutex;
        TreeNode<std::pair<uint32_t, std::shared_ptr<RedisCacheChangeCallback>>> _callbacks;
        // TODO: not really sure if I need it, commented out for now
        // std::map<std::string, RedisType> _key_type;
        uint64_t _instance_id;
        int _db_id;

        std::string _subscribe_pattern;         ///< subscriber pattern
        std::thread _subscriber_thread;         ///< subscriber thread
        std::thread::id _id;                    ///< subscriber thread id

        using SubscriberPtr = std::shared_ptr<sw::redis::Subscriber>;
        RedisClientPtr _client;
        SubscriberPtr _subscriber;

        RedisType _string_to_type(const std::string& type_string)
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

        Action _string_to_action(const std::string& action_string)
        {
            static std::map<std::string, Action> string_to_action_map = {
                {"add",     ADD},
                {"remove",  REMOVE},
                {"replace", REPLACE}
            };
            if (string_to_action_map.contains(action_string)) {
                return string_to_action_map[action_string];
            }
            return INVALID;
        }

        // get json from storage without locking, the locking is done by the calling function
        nlohmann::json _get_value(const std::string &path)
        {
            nlohmann::json::json_pointer jptr(path);
            try {
                return _storage.at(jptr);
            } catch (const nlohmann::json::out_of_range& e) {
                return {};
            }
        }

        void _process_notification(const std::string &pattern, const std::string &channel, const std::string &msg);
        void _init_storage();
        void _run();

        // perform redis reads on sepecific top level key
        std::tuple<nlohmann::json, RedisType> _read_key_value(const std::string &key);
    };
};