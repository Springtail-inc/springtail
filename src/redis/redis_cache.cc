#include <functional>
#include <set>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <common/common.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <redis/redis_cache.hh>

namespace springtail {

RedisCache::RedisCache(bool config_db)
{
    _instance_id = Properties::get_db_instance_id();

    tie(_db_id, _client) = RedisMgr::create_client(config_db);
    _subscriber = RedisMgr::get_subscriber(1, config_db);

    _subscribe_pattern = "__keyspace@" + std::to_string(_db_id) + "__:" + std::to_string(_instance_id) + ":*";
    _subscriber->psubscribe(_subscribe_pattern);
    _subscriber->on_pmessage([this](const std::string &pattern, const std::string &channel, const std::string &msg) {
        this->_process_notification(pattern, channel, msg);
    });

    _init_storage();

    _subscriber_thread = std::thread(&RedisCache::_run, this);
}

RedisCache::~RedisCache()
{
    SPDLOG_INFO("Stopping subscriber thread {}", _id);
    _shutdown = true;
    _subscriber_thread.join();
    SPDLOG_INFO("Joined subscriber thread {}", _id);
    _subscriber->punsubscribe(_subscribe_pattern);
}

void
RedisCache::_process_notification(const std::string &pattern, const std::string &channel, const std::string &msg)
{
    // msg contains action performed on the data: hset, hdel, sadd, etc.
    // channel will contain the key that fits the pattern, it needs to be extracted
    SPDLOG_INFO("received notification: pattern: {}; channel: {}; msg = {}", pattern, channel, msg);

    // extract the key from the notification
    std::string key;
    auto pos = channel.find(':');
    if (pos != std::string::npos) {
        key = channel.substr(pos + 1);
    }
    if (key.empty()) {
        return;
    }

    // lock the storage
    std::unique_lock storage_lock(_storage_mutex);
    SPDLOG_INFO("{}: entered", __FUNCTION__);

    // get the new value of the key from redis (if the key is removed, it can be nullptr)
    nlohmann::json new_key_value;
    RedisType new_key_type;
    tie(new_key_value, new_key_type) = _read_key_value(key);

    SPDLOG_INFO("key: {}, new_value: {}", key, new_key_value.dump(4));

    // get the diff and update storage
    nlohmann::json key_value_diff = nullptr;
    _old_storage = _storage;
    std::string top_level_path = "/" + key;
    if (_storage.contains(key) && new_key_value != nullptr) {
        _storage[key] = new_key_value;
        key_value_diff = nlohmann::json::diff(_old_storage[key], new_key_value);
    } else {
        top_level_path = "";
        if (new_key_value != nullptr) {
            _storage[key] = new_key_value;
        } else {
            _storage.erase(key);
        }
        key_value_diff = nlohmann::json::diff(_old_storage, _storage);
    }
    _process_diff(key_value_diff, top_level_path);
    SPDLOG_INFO("{}: exited", __FUNCTION__);
}

void
RedisCache::_process_diff(const nlohmann::json &diff, const std::string &top_level_path)
{
    SPDLOG_INFO("key_value_diff: {}", diff.dump(4));
    std::string prefix = "/" + std::to_string(_instance_id) + ":";
    // lock callback storage
    std::shared_lock callback_lock(_callback_mutex);

    std::set<std::string> diff_paths;
    for (nlohmann::json::const_iterator it = diff.begin(); it != diff.end(); it++) {
        const std::string &op = (*it)["op"];
        const std::string &path = (*it)["path"];

        // complete the path
        std::string storage_path = top_level_path + path;

        if (_has_array_in_path(storage_path, _storage)) {
            // stub out everything under array and add it only once
            std::string array_path = _get_array_path(storage_path, _storage);
            if (!diff_paths.contains(array_path)) {
                diff_paths.insert(array_path);
            }
        } else if (_has_array_in_path(storage_path, _old_storage)) {
            std::string array_path = _get_array_path(storage_path, _old_storage);
            if (!diff_paths.contains(array_path)) {
                diff_paths.insert(array_path);
            }
        } else {
            diff_paths.insert(storage_path);
        }
    }

    std::vector<std::pair<std::string, std::shared_ptr<RedisCacheChangeCallback>>> all_callbacks;
    for (const auto &diff_path: diff_paths) {
        std::vector<std::pair<std::string, std::shared_ptr<RedisCacheChangeCallback>>> path_callbacks;
        std::deque<std::string> json_path_queue;
        common::split_string("/", diff_path.substr(1), json_path_queue);
        _callbacks.collect_items(path_callbacks, "", json_path_queue,
            [](const std::string &tree_path, const std::shared_ptr<RedisCacheChangeCallback>& cb) {
                return true;
            });
        all_callbacks.insert(all_callbacks.end(), std::make_move_iterator(path_callbacks.begin()), std::make_move_iterator(path_callbacks.end()));
    }

    for (std::vector<std::pair<std::string, std::shared_ptr<RedisCacheChangeCallback>>>::const_iterator it = all_callbacks.begin();
            it != all_callbacks.end(); it++) {
        const std::string &path = it->first;
        std::shared_ptr<RedisCacheChangeCallback> cb_object = it->second;
        std::string item_path = path.substr(prefix.length());

        std::optional<std::reference_wrapper<const nlohmann::json>> new_json_object = _get_value(path, _storage);
        std::optional<std::reference_wrapper<const nlohmann::json>> old_jsonl_object = _get_value(path, _old_storage);
        const nlohmann::json &empty_value = {};

        bool new_inside_array = _inside_array_path(path, _storage);
        bool old_inside_array = _inside_array_path(path, _old_storage);
        if (new_json_object.has_value()) {
            if (new_inside_array) {
                if (old_jsonl_object.has_value() && !old_inside_array) {
                    // non-array item got removed and replaced with array
                    cb_object->change_callback(item_path, empty_value);
                }
            } else {
                cb_object->change_callback(item_path, new_json_object.value().get());
            }
        } else {
            if (old_jsonl_object.has_value() && !old_inside_array) {
                // non-array item got removed and replaced with array
                cb_object->change_callback(item_path, empty_value);
            }
        }
    }
}

size_t
RedisCache::get_callback_count(const std::string &path)
{
    std::deque<std::string> json_path_queue = {};
    if (!path.empty()) {
        std::string json_path = std::to_string(_instance_id) + ":" + path;
        common::split_string("/", json_path, json_path_queue);
    }

    return _callbacks.count_items(json_path_queue, "",
        [](const std::string &path, const std::shared_ptr<RedisCacheChangeCallback> &cb_pair) {
            return true;
        });
}

std::tuple<nlohmann::json, RedisCache::RedisType>
RedisCache::_read_key_value(const std::string &key)
{
    nlohmann::json key_value = nullptr;

    std::string key_type_str = _client->type(key);
    auto key_type = _string_to_type(key_type_str);
    switch (key_type) {
        case REDIS_TYPE_STRING:
        {
            std::optional<std::string> key_stored_value = _client->get(key);
            if (!key_stored_value.has_value()) {
                SPDLOG_ERROR("No value found for key {}", key);
            } else {
                if (nlohmann::json::accept(key_stored_value.value())) {
                    key_value = nlohmann::json::parse(key_stored_value.value());
                } else {
                    key_value = key_stored_value.value();
                }
            }
            break;
        }
        case REDIS_TYPE_HASH:
        {
            unsigned long long cursor = 0;
            do {
                std::map<std::string, std::string> hash_data;
                cursor = _client->hscan(key, cursor, std::inserter(hash_data, hash_data.begin()));
                for (auto [hash_key, hash_value]: hash_data) {
                    if (nlohmann::json::accept(hash_value)) {
                        key_value[hash_key] = nlohmann::json::parse(hash_value);
                    } else {
                        key_value[hash_key] = hash_value;
                    }
                }
            } while (cursor != 0);
            break;
        }
        case REDIS_TYPE_SET:
        {
            unsigned long long cursor = 0;
            do {
                std::vector<std::string> set_data;
                cursor = _client->sscan(key, cursor, std::inserter(set_data, set_data.begin()));
                for (auto set_value: set_data) {
                    if (nlohmann::json::accept(set_value)) {
                        key_value.push_back(nlohmann::json::parse(set_value));
                    } else {
                        key_value.push_back(set_value);
                    }
                }
            } while (cursor != 0);
            std::sort(key_value.begin(), key_value.end());
            break;
        }
        case REDIS_TYPE_NONE:
            break;
        default:
            SPDLOG_ERROR("Unsupported type {} for key {}", key_type_str, key);
    }
    return std::make_tuple(key_value, key_type);
}

void
RedisCache::_init_storage()
{
    // scan all the keys
    std::string key_pattern = std::to_string(_instance_id) + ":*";
    long long cursor = 0;
    do {
        std::unordered_set<std::string> keys;
        cursor = _client->scan(cursor, key_pattern, std::inserter(keys, keys.begin()));

        // fill the storage
        for (auto key: keys) {
            nlohmann::json key_value;
            RedisType key_type;
            tie(key_value, key_type) = _read_key_value(key);
            if (key_value != nullptr) {
                _storage[key] = key_value;
            }
        }
    } while (cursor != 0);
}

void
RedisCache::_run()
{
    _id = std::this_thread::get_id();
    SPDLOG_DEBUG("Started RedisCache subscriber thread {}", _id);
    while (!_shutdown) {
        try {
            // consume from subscriber, timeout is set above
            _subscriber->consume();
        } catch (const sw::redis::TimeoutError &e) {
            // timeout, check for shutdown
            continue;
        } catch (const sw::redis::Error &e) {
            SPDLOG_ERROR("Error consuming from redis: {} on thread {}\n", e.what(), _id);
            break;
        }
    }
    SPDLOG_DEBUG("Ended RedisCache subscriber thread {}", _id);
}

void
RedisCache::dump()
{
    std::shared_lock lock(_storage_mutex);
    SPDLOG_INFO(_storage.dump(4));
}

nlohmann::json
RedisCache::get_value(const std::string &path)
{
    std::string json_path = "/" + std::to_string(_instance_id) + ":" + path;
    std::shared_lock lock(_storage_mutex);
    std::optional<std::reference_wrapper<const nlohmann::json>> json_optional_object = _get_value(json_path, _storage);
    if (json_optional_object.has_value() ) {
        return json_optional_object.value().get();
    }
    return {};
}

bool
RedisCache::set_value(const std::string &path, const nlohmann::json &value)
{
    std::unique_lock lock(_storage_mutex);
    SPDLOG_INFO("{}: entered", __FUNCTION__);

    std::string json_path = "/" + std::to_string(_instance_id) + ":" + path;
    nlohmann::json::json_pointer json_path_ptr(json_path);
    std::deque<std::string> json_path_queue;
    common::split_string("/", json_path.substr(1), json_path_queue);

    std::string redis_key = json_path_queue.front();
    json_path_queue.pop_front();

    _old_storage = _storage;
    if (!_set_value(json_path_ptr, _storage, value)) {
        return false;
    }

    std::string key_type_str = _client->type(redis_key);
    auto key_type = _string_to_type(key_type_str);
    bool ret = false;
    switch (key_type)
    {
        case REDIS_TYPE_STRING:
        {
            std::optional<std::reference_wrapper<const nlohmann::json>> json_optional_object = _get_value("/" + redis_key, _storage);
            if (json_optional_object.has_value()) {
                ret = _client->set(redis_key, nlohmann::to_string(json_optional_object.value().get()));
            }
            break;
        }
        case REDIS_TYPE_HASH:
        {
            std::string hash_key = json_path_queue.front();
            json_path_queue.pop_front();
            std::optional<std::reference_wrapper<const nlohmann::json>> json_optional_object =
                _get_value("/" + redis_key + "/" + hash_key, _storage);
            if (json_optional_object.has_value()) {
                _client->hset(redis_key, hash_key, nlohmann::to_string(json_optional_object.value().get()));
                ret = true;
            }
            break;
        }
        case REDIS_TYPE_SET:
        {
            std::optional<std::reference_wrapper<const nlohmann::json>> json_optional_object_new =
                _get_value("/" + redis_key, _storage);
            std::optional<std::reference_wrapper<const nlohmann::json>> json_optional_object_old =
                _get_value("/" + redis_key, _old_storage);
            if (json_optional_object_new.has_value() && json_optional_object_old.has_value()) {
                const nlohmann::json &json_object_new = json_optional_object_new.value().get();
                const nlohmann::json &json_object_old = json_optional_object_old.value().get();

                auto [old_vector, new_vector] = _array_diff(json_object_old, json_object_new);
                for (auto old_element: old_vector) {
                    _client->srem(redis_key, old_element);
                    ret = true;
                }
                for (auto new_element: new_vector) {
                    _client->sadd(redis_key, new_element);
                    ret = true;
                }
            }
            break;
        }
        // creation of new redis keys is not supported
        case REDIS_TYPE_NONE:
            break;
        default:
            SPDLOG_ERROR("Unsupported type {} for key {}", key_type_str, redis_key);
    }

    if (ret) {
        // generate diff and process it
        nlohmann::json key_value_diff = nlohmann::json::diff(_old_storage, _storage);
        _process_diff(key_value_diff, "");
    } else {
        SPDLOG_INFO("Storage update failed: reverting the changes");
        _storage = _old_storage;
    }
    SPDLOG_INFO("{}: exited", __FUNCTION__);

    return ret;
}

void
RedisCache::add_callback(const std::string &path, const std::shared_ptr<RedisCacheChangeCallback> &cb)
{
    std::deque<std::string> json_path_queue = {};
    if (!path.empty()) {
        // do not add leading "/" because we are going to use it as a delimiter
        std::string json_path = std::to_string(_instance_id) + ":" + path;
        SPDLOG_INFO("adding callback for json_path = {}", json_path);
        common::split_string("/", json_path, json_path_queue);
    } else {
        SPDLOG_INFO("adding callback for empty json_path");
    }

    std::unique_lock lock(_callback_mutex);
    _callbacks.add_item(json_path_queue, cb);
}

void
RedisCache::remove_callback(const std::string &path, const std::shared_ptr<RedisCacheChangeCallback> &cb)
{
    std::deque<std::string> json_path_queue = {};
    if (!path.empty()) {
        // do not add leading "/" because we are going to use it as a delimiter
        std::string json_path = std::to_string(_instance_id) + ":" + path;
        SPDLOG_INFO("removing callback for json_path = {}", json_path);
        common::split_string("/", json_path, json_path_queue);
    } else {
        SPDLOG_INFO("removing callback for empty json_path");
    }

    std::unique_lock lock(_callback_mutex);
    _callbacks.remove_item(json_path_queue, cb);
}

};