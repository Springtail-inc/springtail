#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/counter.hh>
#include <common/redis_cache.hh>

using namespace springtail;

namespace {
    class RedisCache_Test : public testing::Test
    {
    protected:
        static void SetUpTestSuite()
        {
            // springtail_init();
            // NOTE: the reason for not using springtail_init() is because I am using valgrind to run
            //      this unit test and to ensure that no memory is leaked, corrupted, or not cleaned up
            //      properly. Unfortunately, springtail_init() brings a lot of noise that I did not want
            //      to deal with at the moment.
            Properties::get_instance()->init(true);
            Properties::get_instance()->init_cache();
            init_exception();
        }

        static void TearDownTestSuite()
        {
            RedisMgr::shutdown();
            Properties::shutdown();
        }
        void SetUp() override
        {
            _cache = std::make_shared<RedisCache>(true);
            tie(_db_id, _test_client) = RedisMgr::get_instance()->create_client(true);

            std::string string_key = make_key_string("top_level_string");
            std::string hash_key = make_key_string("top_level_hash");
            std::string array_key = make_key_string("top_level_array");

            _test_client->del(string_key);
            _test_client->del(hash_key);
            _test_client->del(array_key);
        }

        void TearDown() override
        {
            _test_client.reset();
            _cache.reset();
        }
        std::shared_ptr<RedisCache> _cache = nullptr;
        RedisClientPtr _test_client;
        int _db_id;

        std::string make_key_string(std::string key) {
            uint64_t instance_id = Properties::get_db_instance_id();
            return std::to_string(instance_id) + ":" + key;
        }
    };

    TEST_F(RedisCache_Test, DatabaseCompare)
    {
        nlohmann::json storage = _cache->get_value("/");
        for (const auto &item : storage.items()) {
            std::string type = _test_client->type(item.key());
            if (type == "string") {
                std::optional<std::string> stored_string = _test_client->get(item.key());
                EXPECT_TRUE(stored_string.has_value());
                EXPECT_EQ(stored_string.value(), nlohmann::to_string(storage[item.key()]));
            }
            if (type == "hash") {
                std::unordered_map<std::string, std::string> results;
                _test_client->hgetall(item.key(), std::inserter(results, results.begin()));
                EXPECT_EQ(results.size(), item.value().size());
                for (const auto &[key, value]: results) {
                    EXPECT_EQ(value, nlohmann::to_string(item.value()[key]));
                }
            }
            if (type == "set") {
                std::vector<std::string> results;
                _test_client->smembers(item.key(), std::inserter(results, results.begin()));
                EXPECT_EQ(results.size(), item.value().size());
                for (uint32_t i = 0; i < results.size(); i++) {
                    EXPECT_EQ(results[i], nlohmann::to_string(item.value()[i]));
                }
            }
        }
    }

    TEST_F(RedisCache_Test, TestSimpleChange)
    {
        int connections = 0;
        nlohmann::json::json_pointer pointer("/xid_mgr/rpc_config/client_connections");
        std::string system_settings_key = "instance_config/system_settings";
        // create callback class
        Counter c(0);
        auto redis_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [&c, &connections, &pointer, &system_settings_key](const std::string &path, const nlohmann::json &new_value) {
                EXPECT_EQ(path, system_settings_key);
                EXPECT_EQ(connections, new_value.at(pointer));
                c.decrement();
            });

        // add callback
        _cache->add_callback(system_settings_key, redis_watcher);

        // get value from cache
        nlohmann::json system_settings_value = _cache->get_value(system_settings_key);
        connections = system_settings_value.at(pointer);
        connections++;
        system_settings_value.at(pointer) = connections;

        // update value in database using test client
        std::string key_value = make_key_string("instance_config");
        std::string value_string = nlohmann::to_string(system_settings_value);
        c.increment();
        _test_client->hset(key_value, "system_settings", value_string);

        // Wait for notification
        c.wait();

        // update value in the database again
        connections--;
        system_settings_value.at(pointer) = connections;
        value_string = nlohmann::to_string(system_settings_value);
        c.increment();
        _test_client->hset(key_value, "system_settings", value_string);

        // wait for notification
        c.wait();

        _cache->remove_callback(system_settings_key, redis_watcher);
    }

    TEST_F(RedisCache_Test, TestMultipleCallbacks)
    {
        EXPECT_EQ(_cache->get_callback_count(""), 0);

        auto redis_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [] (const std::string &path, const nlohmann::json &new_value) {
                // emtpy callback for now, will be reset later on
            });

        _cache->add_callback("db_config", redis_watcher);
        EXPECT_EQ(_cache->get_callback_count(""), 1);
        _cache->add_callback("db_config", redis_watcher);
        EXPECT_EQ(_cache->get_callback_count(""), 2);
        _cache->add_callback("db_config", redis_watcher);
        EXPECT_EQ(_cache->get_callback_count(""), 3);

        EXPECT_EQ(redis_watcher, redis_watcher);

        _cache->remove_callback("db_config", redis_watcher);
        EXPECT_EQ(_cache->get_callback_count(""), 2);
        _cache->remove_callback("db_config", redis_watcher);
        EXPECT_EQ(_cache->get_callback_count(""), 1);
        _cache->remove_callback("db_config", redis_watcher);
        EXPECT_EQ(_cache->get_callback_count(""), 0);


        _cache->add_callback("db_config", redis_watcher);
        _cache->add_callback("instance_state",  redis_watcher);
        _cache->add_callback("instance_config", redis_watcher);
        _cache->add_callback("fdws",            redis_watcher);
        _cache->add_callback("fdw_ids",         redis_watcher);

        EXPECT_EQ(_cache->get_callback_count(""), 5);

        _cache->add_callback("db_config/1",                  redis_watcher);
        _cache->add_callback("db_config/1/include",          redis_watcher);
        _cache->add_callback("db_config/1/include/schemas",  redis_watcher);
        _cache->add_callback("db_config/1/name",             redis_watcher);
        _cache->add_callback("db_config/1/publication_name", redis_watcher);
        _cache->add_callback("db_config/1/replication_slot", redis_watcher);

        EXPECT_EQ(_cache->get_callback_count(""), 11);

        _cache->add_callback("instance_state/1",  redis_watcher);
        _cache->add_callback("fdw_ids/0",         redis_watcher);

        EXPECT_EQ(_cache->get_callback_count(""), 13);

        _cache->add_callback("fdw/1",            redis_watcher);
        _cache->add_callback("fdw/1/db_prefix",  redis_watcher);
        _cache->add_callback("fdw/1/fdw_user",   redis_watcher);
        _cache->add_callback("fdw/1/host",       redis_watcher);
        _cache->add_callback("fdw/1/port",       redis_watcher);

        EXPECT_EQ(_cache->get_callback_count(""), 18);

        _cache->add_callback("instance_config/system_settings",     redis_watcher);
        _cache->add_callback("instance_config/primary_db",          redis_watcher);
        _cache->add_callback("instance_config/hostname:proxy",      redis_watcher);
        _cache->add_callback("instance_config/id",                  redis_watcher);
        _cache->add_callback("instance_config/database_ids",        redis_watcher);
        _cache->add_callback("instance_config/hostname:ingestion",  redis_watcher);

        EXPECT_EQ(_cache->get_callback_count(""), 24);

        _cache->add_callback("instance_config/system_settings/fs",          redis_watcher);
        _cache->add_callback("instance_config/system_settings/iopool",      redis_watcher);
        _cache->add_callback("instance_config/system_settings/log_mgr",     redis_watcher);
        _cache->add_callback("instance_config/system_settings/logging",     redis_watcher);
        _cache->add_callback("instance_config/system_settings/org",         redis_watcher);
        _cache->add_callback("instance_config/system_settings/otel",        redis_watcher);
        _cache->add_callback("instance_config/system_settings/proxy",       redis_watcher);
        _cache->add_callback("instance_config/system_settings/storage",     redis_watcher);
        _cache->add_callback("instance_config/system_settings/sys_tbl_mgr", redis_watcher);
        _cache->add_callback("instance_config/system_settings/write_cache", redis_watcher);
        _cache->add_callback("instance_config/system_settings/xid_mgr",     redis_watcher);

        EXPECT_EQ(_cache->get_callback_count(""), 35);

        _cache->add_callback("instance_config/primary_db/host",             redis_watcher);
        _cache->add_callback("instance_config/primary_db/port",             redis_watcher);
        _cache->add_callback("instance_config/primary_db/replication_user", redis_watcher);

        EXPECT_EQ(_cache->get_callback_count(""), 38);

        _cache->add_callback("instance_config/database_ids/0",  redis_watcher);

        EXPECT_EQ(_cache->get_callback_count(""), 39);

        _cache->add_callback("instance_config/system_settings/xid_mgr/rpc_config/client_connections", redis_watcher);

        EXPECT_EQ(_cache->get_callback_count(""), 40);

        int connections = 0;
        // create callback class
        Counter c(0);
        redis_watcher->set_cb([&c, &connections](const std::string &path, const nlohmann::json &new_value) {
            if (!new_value.is_null()) {
                if (path == "instance_config") {
                    // less specific path match
                    EXPECT_EQ(connections, new_value.at("/system_settings/xid_mgr/rpc_config/client_connections"_json_pointer));
                } else if (path == "instance_config/system_settings") {
                    // less specific path match
                    EXPECT_EQ(connections, new_value.at("/xid_mgr/rpc_config/client_connections"_json_pointer));
                } else if (path == "instance_config/system_settings/xid_mgr") {
                    // less specific path match
                    EXPECT_EQ(connections, new_value.at("/rpc_config/client_connections"_json_pointer));
                } else if (path == "instance_config/system_settings/xid_mgr/rpc_config/client_connections")  {
                    // exact path match of the updated value
                    EXPECT_EQ(connections, new_value);
                } else {
                    throw Error();
                }
            }
            c.decrement();
        });

        // get value from cache
        std::string system_settings_key = "instance_config/system_settings";
        nlohmann::json system_settings_value = _cache->get_value(system_settings_key);
        nlohmann::json::json_pointer pointer("/xid_mgr/rpc_config/client_connections");
        connections = system_settings_value.at(pointer);
        connections++;
        system_settings_value.at(pointer) = connections;

        // update value in database using test client
        std::string key_value = make_key_string("instance_config");
        std::string value_string = nlohmann::to_string(system_settings_value);
        c.increment_by(4);
        _test_client->hset(key_value, "system_settings", value_string);

        // Wait for notification
        c.wait();

        // update value in the database again
        connections--;
        system_settings_value.at(pointer) = connections;
        value_string = nlohmann::to_string(system_settings_value);
        c.increment_by(4);
        _test_client->hset(key_value, "system_settings", value_string);

        // wait for notification
        c.wait();
    }

    TEST_F(RedisCache_Test, TestTopLevelString) {
        Counter c(0);
        auto redis_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [&c] (const std::string &path, const nlohmann::json &new_value) {
                c.decrement();
        });

        std::string string_key = make_key_string("top_level_string");
        std::string initial_value = "initial string value";
        std::string new_string_value = "new string value";
        nlohmann::json json_value = {
            {"key1", "value1"},
            {"key2", "value2"},
        };
        std::string json_value_string = nlohmann::to_string(json_value);
        nlohmann::json new_json_value = {
            {"key1", "new_value1"},
            {"key2", "value2"},
        };
        std::string new_json_value_string = nlohmann::to_string(new_json_value);

        _cache->add_callback("top_level_string", redis_watcher);
        _cache->add_callback("top_level_string/key1", redis_watcher);
        _cache->add_callback("top_level_string/key2", redis_watcher);

        c.increment();
        // create string
        _test_client->set(string_key, initial_value);
        c.wait();

        // update string to another string
        c.increment();
        _test_client->set(string_key, new_string_value);
        c.wait();

        // update string with json value
        c.increment_by(3);
        _test_client->set(string_key, json_value_string);
        c.wait();

        // update string with new json value
        c.increment_by(2);
        _test_client->set(string_key, new_json_value_string);
        c.wait();

        // set it back to string
        c.increment_by(3);
        _test_client->set(string_key, initial_value);
        c.wait();

        // delete key
        c.increment();
        _test_client->del(string_key);
        c.wait();
    }

    TEST_F(RedisCache_Test, TestTopLevelHash) {
        Counter c(0);
        auto redis_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [&c] (const std::string &path, const nlohmann::json &new_value) {
                c.decrement();
        });

        nlohmann::json hash_values = R"({
                "top_key1": "value1",
                "top_key2": "value2",
                "top_key3": {
                    "bottom_key1": "value1",
                    "bottom_key2": "value2"
                },
                "top_key4": [1, 2, 3, 4]
        })"_json;

        std::string hash_key = make_key_string("top_level_hash");
        _cache->add_callback("top_level_hash", redis_watcher);
        _cache->add_callback("top_level_hash/top_key1", redis_watcher);
        _cache->add_callback("top_level_hash/top_key2", redis_watcher);
        _cache->add_callback("top_level_hash/top_key3", redis_watcher);
        _cache->add_callback("top_level_hash/top_key3/bottom_key1", redis_watcher);
        _cache->add_callback("top_level_hash/top_key3/bottom_key2", redis_watcher);
        _cache->add_callback("top_level_hash/top_key4", redis_watcher);

        // the next four cb paths are expected to be ignored
        _cache->add_callback("top_level_hash/top_key4/0", redis_watcher);
        _cache->add_callback("top_level_hash/top_key4/1", redis_watcher);
        _cache->add_callback("top_level_hash/top_key4/3", redis_watcher);
        _cache->add_callback("top_level_hash/top_key4/4", redis_watcher);

        _cache->add_callback("top_level_hash/top_key4/key1", redis_watcher);

        _cache->add_callback("top_level_hash/top_key5", redis_watcher);

        // add string
        c.increment_by(2);
        _test_client->hset(hash_key, "top_key1", hash_values["top_key1"].get<std::string>());
        c.wait();

        // add string
        c.increment_by(2);
        _test_client->hset(hash_key, "top_key2", hash_values["top_key2"].get<std::string>());
        c.wait();

        // add hash
        c.increment_by(4);
        _test_client->hset(hash_key, "top_key3", nlohmann::to_string(hash_values["top_key3"]));
        c.wait();

        // add array
        c.increment_by(2);
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // replace first key by a different string
        c.increment_by(2);
        _test_client->hset(hash_key, "top_key1", "new_value1");
        c.wait();

        // replace second key by a different string
        c.increment_by(2);
        _test_client->hset(hash_key, "top_key2", "new_value2");
        c.wait();

        // change hash value
        c.increment_by(3);
        hash_values.at("/top_key3/bottom_key1"_json_pointer) = "new_value1";
        _test_client->hset(hash_key, "top_key3", nlohmann::to_string(hash_values["top_key3"]));
        c.wait();

        // change hash value
        c.increment_by(3);
        hash_values.at("/top_key3/bottom_key2"_json_pointer) = "new_value2";
        _test_client->hset(hash_key, "top_key3", nlohmann::to_string(hash_values["top_key3"]));
        c.wait();

        // delete hash
        nlohmann::json top_key3_value = hash_values["top_key3"];
        c.increment_by(4);
        _test_client->hdel(hash_key, "top_key3");
        c.wait();

        // add hash back
        c.increment_by(4);
        _test_client->hset(hash_key, "top_key3", nlohmann::to_string(top_key3_value));
        c.wait();

        // Replace hash by string
        c.increment_by(4);
        _test_client->hset(hash_key, "top_key3", "some string for top_key3");
        c.wait();

        // assign different string to a former hash
        c.increment_by(2);
        _test_client->hset(hash_key, "top_key3", "some other string for top_key3");
        c.wait();

        // Replace string by hash
        c.increment_by(4);
        _test_client->hset(hash_key, "top_key3", nlohmann::to_string(top_key3_value));
        c.wait();

        // change array value
        nlohmann::json top_key4_value = hash_values["top_key4"];
        c.increment_by(2);
        hash_values.at("/top_key4/0"_json_pointer) = 5;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // change array value
        c.increment_by(2);
        hash_values.at("/top_key4/1"_json_pointer) = 6;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // remove array value
        c.increment_by(2);
        hash_values.at("top_key4").erase(0);
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // add array value at the beginning of the array
        c.increment_by(2);
        hash_values.at("top_key4").insert(hash_values.at("top_key4").begin(), 1);
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // add hash to an array
        c.increment_by(2);
        hash_values["top_key4"][4] = R"({
            "item_key1": "item_value1",
            "item_key2": "item_value2",
            "item_key3": "item_value3",
            "item_key4": "item_value4"
        })"_json;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // remove first element from the array
        c.increment_by(2);
        hash_values.at("top_key4").erase(0);
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // restore original array values
        c.increment_by(2);
        hash_values["top_key4"] = top_key4_value;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // Replace array by hash
        c.increment_by(3);
        hash_values["top_key4"] = R"({
            "key1": "item1",
            "key2": "item2",
            "key3": "item2"
        })"_json;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // restore original array values
        c.increment_by(3);
        hash_values["top_key4"] = top_key4_value;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // Replace array by a string
        c.increment_by(2);
        hash_values["top_key4"] = R"("string for top_key4")"_json;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // restore original array values
        c.increment_by(2);
        hash_values["top_key4"] = top_key4_value;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // Replace array by hash with keys identical to array indices
        c.increment_by(4);
        hash_values["top_key4"] = R"({
            "0": "item1",
            "1": "item2",
            "2": "item2"
        })"_json;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // restore original array values
        c.increment_by(4);
        hash_values["top_key4"] = top_key4_value;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        // add string to the top level hash
        c.increment_by(2);
        _test_client->hset(hash_key, "top_key5", "value5");
        c.wait();

        // remove string from the top level hash
        c.increment_by(2);
        _test_client->hdel(hash_key, "top_key5");
        c.wait();

        // remove the key of the hash
        c.increment_by(7);
        _test_client->del(hash_key);
        c.wait();
    }

    TEST_F(RedisCache_Test, TestTopLevelArray) {
        Counter c(0);
        auto redis_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [&c] (const std::string &path, const nlohmann::json &new_value) {
                c.decrement();
        });

        nlohmann::json array_values = R"([
            "value1",
            "value2",
            "value3",
            "value4"
        ])"_json;

        std::string array_key = make_key_string("top_level_array");
        _cache->add_callback("top_level_array", redis_watcher);
        _cache->add_callback("top_level_array/0", redis_watcher);
        _cache->add_callback("top_level_array/1", redis_watcher);
        _cache->add_callback("top_level_array/2", redis_watcher);
        _cache->add_callback("top_level_array/3", redis_watcher);
        _cache->add_callback("top_level_array/4", redis_watcher);
        _cache->add_callback("top_level_array/5", redis_watcher);

        c.increment_by(1);
        _test_client->sadd(array_key, array_values[0].get<std::string>());
        c.wait();

        c.increment_by(1);
        _test_client->sadd(array_key, array_values[1].get<std::string>());
        c.wait();

        c.increment_by(1);
        _test_client->sadd(array_key, array_values[2].get<std::string>());
        c.wait();

        c.increment_by(1);
        _test_client->sadd(array_key, array_values[3].get<std::string>());
        c.wait();

        c.increment_by(1);
        _test_client->srem(array_key, array_values[0].get<std::string>());
        c.wait();

        // replace array with string
        c.increment_by(1);
        _test_client->set(array_key, "some string");
        c.wait();

        c.increment_by(1);
        _test_client->del(array_key);
        c.wait();
    }

    TEST_F(RedisCache_Test, TestRedisChange) {
        Counter c(0);
        auto redis_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [&c] (const std::string &path, const nlohmann::json &new_value) {
                c.decrement();
        });

        // NOTE: The array values are here in sorted order. Redis may reorder array values when it stores
        //      them. When we read them back, we sort them before putting it back into storage. Also,
        //      redis would not allow duplicates, so we need to make sure this does not happen when
        //      we set arrays.
        nlohmann::json array_values = R"([
            "value1",
            "value2",
            "value3",
            "value4"
        ])"_json;

        nlohmann::json hash_values = R"({
            "top_key1": "value1",
            "top_key2": "value2",
            "top_key3": {
                "bottom_key1": "value1",
                "bottom_key2": "value2"
            },
            "top_key4": [1, 2, 3, 4],
            "top_key5": "value5"
        })"_json;

        nlohmann::json string_value = {
            {"key1", "value1"},
            {"key2", "value2"},
        };

        std::string string_key = make_key_string("top_level_string");
        std::string hash_key = make_key_string("top_level_hash");
        std::string array_key = make_key_string("top_level_array");

        _cache->add_callback("top_level_array", redis_watcher);
        _cache->add_callback("top_level_string", redis_watcher);
        _cache->add_callback("top_level_hash", redis_watcher);

        c.increment();
        _test_client->set(string_key, nlohmann::to_string(string_value));
        c.wait();

        c.increment();
        _test_client->hset(hash_key, "top_key1", hash_values["top_key1"].get<std::string>());
        c.wait();
        c.increment();
        _test_client->hset(hash_key, "top_key2", hash_values["top_key2"].get<std::string>());
        c.wait();
        c.increment();
        _test_client->hset(hash_key, "top_key3", nlohmann::to_string(hash_values["top_key3"]));
        c.wait();
        c.increment();
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();
        c.increment();
        _test_client->hset(hash_key, "top_key5", hash_values["top_key5"].get<std::string>());
        c.wait();

        c.increment();
        _test_client->sadd(array_key, array_values[0].get<std::string>());
        c.wait();
        c.increment();
        _test_client->sadd(array_key, array_values[1].get<std::string>());
        c.wait();
        c.increment();
        _test_client->sadd(array_key, array_values[2].get<std::string>());
        c.wait();
        c.increment();
        _test_client->sadd(array_key, array_values[3].get<std::string>());
        c.wait();

        c.increment();
        string_value["key3"] = "value3";
        EXPECT_TRUE(_cache->set_value("top_level_string", string_value));
        c.wait();

        nlohmann::json stored_string_value = _cache->get_value("top_level_string");
        EXPECT_EQ(string_value, stored_string_value);

        c.increment();
        hash_values["top_key1"] = "new value1";
        EXPECT_TRUE(_cache->set_value("top_level_hash/top_key1", hash_values["top_key1"]));
        c.wait();
        nlohmann::json stored_hash_values = _cache->get_value("top_level_hash");
        EXPECT_EQ(hash_values, stored_hash_values);

        c.increment();
        hash_values["top_key2"] = "new value2";
        EXPECT_TRUE(_cache->set_value("top_level_hash/top_key2", hash_values["top_key2"]));
        c.wait();
        stored_hash_values = _cache->get_value("top_level_hash");
        EXPECT_EQ(hash_values, stored_hash_values);

        c.increment();
        hash_values["top_key3"]["bottom_key3"] = "new value3";
        EXPECT_TRUE(_cache->set_value("top_level_hash/top_key3/bottom_key3", hash_values["top_key3"]["bottom_key3"]));
        c.wait();
        stored_hash_values = _cache->get_value("top_level_hash");
        EXPECT_EQ(hash_values, stored_hash_values);

        c.increment();
        hash_values["top_key4"][4] = 5;
        EXPECT_TRUE(_cache->set_value("top_level_hash/top_key4/4", hash_values["top_key4"][4]));
        c.wait();
        stored_hash_values = _cache->get_value("top_level_hash");
        EXPECT_EQ(hash_values, stored_hash_values);

        // test add
        c.increment();
        array_values = _cache->get_value("top_level_array");
        array_values[4] = "value5";
        ASSERT_TRUE(_cache->set_value("top_level_array/4", array_values[4]));
        c.wait();
        nlohmann::json stored_array_values = _cache->get_value("top_level_array");
        EXPECT_EQ(array_values, stored_array_values);

        // test remove
        c.increment();
        array_values = _cache->get_value("top_level_array");
        array_values.erase(0);
        ASSERT_TRUE(_cache->set_value("top_level_array", array_values));
        c.wait();
        stored_array_values = _cache->get_value("top_level_array");
        EXPECT_EQ(array_values, stored_array_values);

        // test add duplicate
        array_values = _cache->get_value("top_level_array");
        array_values[4] = "value2";
        ASSERT_FALSE(_cache->set_value("top_level_array/4", array_values[4]));
        stored_array_values = _cache->get_value("top_level_array");
        EXPECT_NE(array_values, stored_array_values);

        SPDLOG_INFO("started cleanup");
        c.increment();
        _test_client->del(string_key);
        c.wait();

        c.increment();
        _test_client->del(hash_key);
        c.wait();

        c.increment();
        _test_client->del(array_key);
        c.wait();
    }

    TEST_F(RedisCache_Test, TestArrayRemoval) {
        Counter c(0);
        auto redis_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [&c] (const std::string &path, const nlohmann::json &new_value) {
                c.decrement();
        });

        std::string fdw_ids_clone = "fdw_ids_clone";
        std::string array_key = make_key_string(fdw_ids_clone);

        _cache->add_callback(fdw_ids_clone, redis_watcher);

        nlohmann::json array_values = {
            "1",
            "2",
            "3"
        };

        c.increment();
        _test_client->sadd(array_key, array_values[0].get<std::string>());
        c.wait();

        EXPECT_EQ(array_values.type(), nlohmann::json::value_t::array);
        EXPECT_EQ(array_values[0].type(), nlohmann::json::value_t::string);

        nlohmann::json array_element = _cache->get_value(fdw_ids_clone + "/0");
        EXPECT_EQ(array_element.type(), nlohmann::json::value_t::string);

        c.increment();
        _cache->set_value(fdw_ids_clone + "/1", array_values[1]);
        c.wait();

        c.increment();
        _cache->set_value(fdw_ids_clone + "/2", array_values[2]);
        c.wait();

        array_values.erase(0);
        c.increment();
        _cache->set_value(fdw_ids_clone, array_values);
        c.wait();

        array_values.erase(0);
        c.increment();
        _cache->set_value(fdw_ids_clone, array_values);
        c.wait();

        array_values.erase(0);
        c.increment();
        _cache->set_value(fdw_ids_clone, array_values);
        c.wait();

        array_values = {
            "1",
            "2",
            "3"
        };
        c.increment();
        _test_client->sadd(array_key, array_values[0].get<std::string>());
        c.wait();

        c.increment();
        _cache->set_value(fdw_ids_clone, array_values);
        c.wait();
    }

    TEST_F(RedisCache_Test, TestHashRemoval) {
        Counter c(0);
        auto redis_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [&c] (const std::string &path, const nlohmann::json &new_value) {
                c.decrement();
        });

        std::string fdw_clone = "fdw_clone";
        std::string hash_key = make_key_string(fdw_clone);

        _cache->add_callback(fdw_clone, redis_watcher);

        nlohmann::json first_element = {
                { "db_prefix", "replica_" },
                { "fdw_user", "springtail" },
                { "host", "localhost" },
                { "port", 5436}
        };

        nlohmann::json hash_values = nlohmann::json::object({
            { "1", {
                { "db_prefix", "replica_" },
                { "fdw_user", "springtail" },
                { "password", "springtail" },
                { "host", "localhost" },
                { "port", 5431}
            } },
            { "2", {
                { "db_prefix", "replica_" },
                { "fdw_user", "springtail" },
                { "password", "springtail" },
                { "host", "localhost" },
                { "port", 5432}
            } },
            { "3", {
                { "db_prefix", "replica_" },
                { "fdw_user", "springtail" },
                { "password", "springtail" },
                { "host", "localhost" },
                { "port", 5433}
            } }
        });

        c.increment();
        _test_client->hset(hash_key, "1", nlohmann::to_string(first_element));
        c.wait();

        c.increment();
        _cache->set_value(fdw_clone + "/1", hash_values["1"]);
        c.wait();

        c.increment();
        _cache->set_value(fdw_clone + "/2", hash_values["2"]);
        c.wait();

        c.increment();
        _cache->set_value(fdw_clone + "/3", hash_values["3"]);
        c.wait();

        nlohmann::json empty;
        c.increment();
        _cache->set_value(fdw_clone + "/1", empty);
        c.wait();

        c.increment();
        _cache->set_value(fdw_clone + "/2", empty);
        c.wait();

        c.increment();
        _cache->set_value(fdw_clone + "/3", empty);
        c.wait();
    }

    TEST_F(RedisCache_Test, TestStringRemoval) {
        Counter c(0);
        auto redis_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [&c] (const std::string &path, const nlohmann::json &new_value) {
                c.decrement();
        });

        std::string string_key = "string_key";
        std::string redis_string_key = make_key_string(string_key);

        _cache->add_callback(string_key, redis_watcher);

        nlohmann::json json_string = "test string";
        nlohmann::json json_new_string = "some different test string";

        c.increment();
        _test_client->set(redis_string_key, json_string.get<std::string>());
        c.wait();

        c.increment();
        _cache->set_value(string_key, json_new_string);
        c.wait();

        nlohmann::json empty;
        c.increment();
        _cache->set_value(string_key, empty);
        c.wait();

    }
};
