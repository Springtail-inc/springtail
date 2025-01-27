#include <set>
#include <thread>

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

    // get the new value of the key from redis (if the key is removed, it can be nullptr)
    nlohmann::json new_key_value;
    RedisType new_key_type;
    tie(new_key_value, new_key_type) = _read_key_value(key);

    SPDLOG_INFO("key: {}, new_value: {}", key, new_key_value.dump(4));
    std::string storage_prefix = "/" + std::to_string(_instance_id) + ":";

    // lock the storage
    std::unique_lock storage_lock(_storage_mutex);

    // get the diff and update storage
    nlohmann::json key_value_diff = nullptr;
    nlohmann::json old_storage = _storage;
    std::string top_level_path = "/" + key;
    if (_storage.contains(key) && new_key_value != nullptr) {
        _storage[key] = new_key_value;
        key_value_diff = nlohmann::json::diff(old_storage[key], new_key_value);
    } else {
        top_level_path = "";
        if (new_key_value != nullptr) {
            _storage[key] = new_key_value;
        } else {
            _storage.erase(key);
        }
        key_value_diff = nlohmann::json::diff(old_storage, _storage);
    }
    SPDLOG_INFO("key_value_diff: {}", key_value_diff.dump(4));

    // TODO: not sure if it is needed, commented out for now
    // store key type
    // _key_type[key] = new_key_type;

    // find matching callbacks and call them
    // lock callback map
    std::shared_lock callback_lock(_callback_mutex);

    // TODO: need a different way to handle changes inside an array
    // iterate through the diffs
    for(nlohmann::json::iterator it = key_value_diff.begin(); it != key_value_diff.end(); it++) {
        // TODO: changes on an array element should notify only the parent, notifications
        //      of the individual children nodes should be skipped
        //      to do that, move new and old array into an std::set and do a diff between two sets
        //      adjust action accordingly, sent only the top level notification and break the loop
        // extract operation and path
        const std::string &op = (*it)["op"];
        const std::string &path = (*it)["path"];

        // complete the path
        std::string storage_path = top_level_path + path;

        // handle reference to the last array element
        // replace the last character "-" with the index of the last element
        if (storage_path.ends_with("/-")) {
            nlohmann::json::json_pointer storage_path_ptr(storage_path.substr(0, storage_path.length() - 2));
            nlohmann::json json_array = _storage.at(storage_path_ptr);
            size_t num_elements = json_array.size();
            storage_path.replace(storage_path.length() - 1, 1, std::to_string(num_elements - 1));
        }

        // convert diff operation to Action enum
        Action action = _string_to_action(op);

        // find all callbacks to call
        std::vector<std::pair<std::string, std::pair<uint32_t, std::shared_ptr<RedisCacheChangeCallback>>>> callback_queue;
        std::deque<std::string> json_path_queue;
        common::split_string("/", storage_path.substr(1), json_path_queue);
        _callbacks.collect_items(callback_queue, "", json_path_queue, [&action](const std::pair<uint32_t, std::shared_ptr<RedisCacheChangeCallback>> &cb_pair) {
            if ((cb_pair.first & action) != 0) {
                return true;
            }
            return false;
        });

        // callbacks are executed
        nlohmann::json value = _get_value(storage_path);
        for (auto item: callback_queue) {
            const std::string &path = item.first;
            nlohmann::json::json_pointer path_ptr(path);
            // only notify if the data was already present and got removed or
            // the data was not there and got added or
            // the data was already present and got changed
            if (old_storage.contains(path_ptr) || _storage.contains(path_ptr)) {
                std::string item_path = path.substr(storage_prefix.length());
                nlohmann::json item_value = _get_value(path);
                item.second.second->change_callback(item_path, action, item_value);
            }
        }
    }
}

size_t
RedisCache::get_callback_count(const std::string &path, uint32_t action_mask) {
    std::deque<std::string> json_path_queue = {};
    if (!path.empty()) {
        std::string json_path = std::to_string(_instance_id) + ":" + path;
        common::split_string("/", json_path, json_path_queue);
    }

    return _callbacks.count_items(json_path_queue, [&action_mask](const std::pair<uint32_t, std::shared_ptr<RedisCacheChangeCallback>> &cb_pair) {
        if ((action_mask & cb_pair.first) == action_mask) {
            return true;
        }
        return false;
    });
}

size_t
RedisCache::get_callback_total_count(const std::string &path) {
    std::deque<std::string> json_path_queue = {};
    if (!path.empty()) {
        std::string json_path = std::to_string(_instance_id) + ":" + path;
        common::split_string("/", json_path, json_path_queue);
    }

    return _callbacks.count_items(json_path_queue, [](const std::pair<uint32_t, std::shared_ptr<RedisCacheChangeCallback>> &cb_pair){ return true; });
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
                std::cout << "read string value: " << key_stored_value.value() << std::endl;
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
    // std::cout << "key_pattern = " << key_pattern << std::endl;
    long long cursor = 0;
    do {
        std::unordered_set<std::string> keys;
        // std::cout << "scanning database" << std::endl;
        cursor = _client->scan(cursor, key_pattern, std::inserter(keys, keys.begin()));
        // std::cout << "done scanning: cursor = " << cursor << std::endl;

        // fill the storage
        for (auto key: keys) {
            nlohmann::json key_value;
            RedisType key_type;
            tie(key_value, key_type) = _read_key_value(key);
            if (key_value != nullptr) {
                _storage[key] = key_value;
                // _key_type[key] = key_type;
            }
        }
    } while (cursor != 0);
}

void
RedisCache::_run() {
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
    return _get_value(json_path);
}

/*
void
RedisCache::set_value(const nlohmann::json::json_pointer &jptr, const nlohmann::json &value)
{
    std::unique_lock lock(_storage_mutex);
    std::string pointer_string = jptr.to_string();
    // split the string with '/' delimiter
    // take the first item and prepend _instance_id + ':'
    // get item type
    // switch on item type
    //      for string: put the rest of the pointer together and patch
    //                  apply change to redis
    //      for hash: get the next item, use it as hash key
    //              put the rest of the pointer together and patch
    //              change the value in redis
    //      for set: get the next item as id
    //              put the rest of the pointer together and patch
    //              change value in redis

}
*/

void
RedisCache::add_callback(const std::string &path, uint32_t action_mask, std::shared_ptr<RedisCacheChangeCallback> cb)
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
    _callbacks.add_item(json_path_queue, std::make_pair(action_mask, cb));
}

void
RedisCache::remove_callback(const std::string &path, uint32_t action_mask, std::shared_ptr<RedisCacheChangeCallback> cb)
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
    _callbacks.remove_item(json_path_queue, std::make_pair(action_mask, cb));
}


};