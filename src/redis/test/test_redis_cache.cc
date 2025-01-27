#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/counter.hh>
#include <common/tracing.hh>
#include <redis/redis_cache.hh>

using namespace springtail;

namespace {
    class RedisCache_Test : public testing::Test {
    protected:
        static void SetUpTestSuite()
        {
            // springtail_init();
            // NOTE: the reason for not using springtail_init() is bacause I am using valgrind to run
            //      this unit test and to ensure that no memory is leaked, corrupted, or not cleaned up
            //      properly. Unfortunately, springtail_init() brings a lot of noise that I did not want
            //      to deal with at the moment.
            Properties::init(true);
            init_exception();
        }

        static void TearDownTestSuite()
        {
            Properties::shutdown();
        }
        void SetUp() override
        {
            _cache = std::make_shared<RedisCache>(true);
            tie(_db_id, _test_client) = RedisMgr::create_client(true);
        }

        void TearDown() override
        {
            _test_client.reset();
            _cache.reset();
        }
        std::shared_ptr<RedisCache> _cache = nullptr;
        RedisClientPtr _test_client;
        int _db_id;

        class RedisChangeWatcher : public RedisCache::RedisCacheChangeCallback {
        public:
            RedisChangeWatcher(std::function<void(const std::string &path, RedisCache::Action action, const nlohmann::json &new_value)> func) : _cb(func) {}
            ~RedisChangeWatcher() override = default;

            void change_callback(const std::string &path, RedisCache::Action action, const nlohmann::json &new_value) override {
                SPDLOG_INFO("!!! Received notification; path: \"{}\"; action: {}; value: {}", path, (uint32_t)(action), new_value.dump(4));
                _cb(path, action, new_value);
            }

            void set_cb(std::function<void(const std::string &path, RedisCache::Action action, const nlohmann::json &new_value)> func) {
                _cb = func;
            }
        private:
            std::function<void(const std::string &, RedisCache::Action, const nlohmann::json &)> _cb;
        };

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
        std::shared_ptr<RedisChangeWatcher> redis_watcher = std::make_shared<RedisChangeWatcher>(
            [&c, &connections, &pointer, &system_settings_key](const std::string &path, RedisCache::Action action, const nlohmann::json &new_value) {
                c.decrement();
                EXPECT_EQ(path, system_settings_key);
                EXPECT_EQ(action, RedisCache::REPLACE);
                EXPECT_EQ(connections, new_value.at(pointer));
            });

        // add callback
        uint32_t action_mask = RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE;
        _cache->add_callback(system_settings_key, action_mask, redis_watcher);

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

        _cache->remove_callback(system_settings_key, action_mask, redis_watcher);
    }

    TEST_F(RedisCache_Test, TestMultipleCallbacks)
    {
        size_t cb_count = _cache->get_callback_count("", RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE);
        EXPECT_EQ(cb_count, 0);

        std::shared_ptr<RedisChangeWatcher> redis_watcher = std::make_shared<RedisChangeWatcher>([] (const std::string &path, RedisCache::Action action, const nlohmann::json &new_value) {
        });

        _cache->add_callback("db_config", RedisCache::ADD, redis_watcher);
        _cache->add_callback("db_config", RedisCache::REMOVE, redis_watcher);
        _cache->add_callback("db_config", RedisCache::REPLACE, redis_watcher);
        _cache->add_callback("db_config", (RedisCache::ADD | RedisCache::REMOVE), redis_watcher);
        _cache->add_callback("db_config", (RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("db_config", (RedisCache::REPLACE | RedisCache::ADD), redis_watcher);
        _cache->add_callback("db_config", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        EXPECT_EQ(_cache->get_callback_count("", RedisCache::ADD), 4);
        EXPECT_EQ(_cache->get_callback_count("", RedisCache::REMOVE), 4);
        EXPECT_EQ(_cache->get_callback_count("", RedisCache::REPLACE), 4);
        EXPECT_EQ(_cache->get_callback_count("", (RedisCache::ADD | RedisCache::REMOVE)), 2);
        EXPECT_EQ(_cache->get_callback_count("", (RedisCache::REMOVE | RedisCache::REPLACE)), 2);
        EXPECT_EQ(_cache->get_callback_count("", (RedisCache::REPLACE | RedisCache::ADD)), 2);
        EXPECT_EQ(_cache->get_callback_count("", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE)), 1);

        EXPECT_EQ(_cache->get_callback_total_count(""), 7);
        EXPECT_EQ(_cache->get_callback_total_count("db_config"), 7);

        EXPECT_EQ(std::make_pair(RedisCache::ADD, redis_watcher), std::make_pair(RedisCache::ADD, redis_watcher));

        _cache->remove_callback("db_config", RedisCache::ADD, redis_watcher);
        _cache->remove_callback("db_config", RedisCache::REMOVE, redis_watcher);
        _cache->remove_callback("db_config", RedisCache::REPLACE, redis_watcher);
        _cache->remove_callback("db_config", (RedisCache::ADD | RedisCache::REMOVE), redis_watcher);
        _cache->remove_callback("db_config", (RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->remove_callback("db_config", (RedisCache::REPLACE | RedisCache::ADD), redis_watcher);

        EXPECT_EQ(_cache->get_callback_total_count(""), 1);

        _cache->add_callback("instance_state",  (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("fdws",            (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("fdw_ids",         (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        EXPECT_EQ(_cache->get_callback_total_count(""), 5);

        _cache->add_callback("db_config/1",                  (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("db_config/1/include",          (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("db_config/1/include/schemas",  (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("db_config/1/name",             (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("db_config/1/publication_name", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("db_config/1/replication_slot", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        EXPECT_EQ(_cache->get_callback_total_count(""), 11);

        _cache->add_callback("instance_state/1",  (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("fdw_ids/0",         (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        EXPECT_EQ(_cache->get_callback_total_count(""), 13);

        _cache->add_callback("fdw/1",            (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("fdw/1/db_prefix",  (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("fdw/1/fdw_user",   (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("fdw/1/host",       (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("fdw/1/port",       (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        EXPECT_EQ(_cache->get_callback_total_count(""), 18);

        _cache->add_callback("instance_config/system_settings",     (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/primary_db",          (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/hostname:proxy",      (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/id",                  (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/database_ids",        (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/hostname:ingestion",  (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        EXPECT_EQ(_cache->get_callback_total_count(""), 24);

        _cache->add_callback("instance_config/system_settings/fs",          (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/system_settings/iopool",      (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/system_settings/log_mgr",     (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/system_settings/logging",     (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/system_settings/org",         (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/system_settings/otel",        (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/system_settings/proxy",       (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/system_settings/storage",     (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/system_settings/sys_tbl_mgr", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/system_settings/write_cache", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/system_settings/xid_mgr",     (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        EXPECT_EQ(_cache->get_callback_total_count(""), 35);

        _cache->add_callback("instance_config/primary_db/host",             (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/primary_db/port",             (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("instance_config/primary_db/replication_user", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        EXPECT_EQ(_cache->get_callback_total_count(""), 38);

        _cache->add_callback("instance_config/database_ids/0",  (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        EXPECT_EQ(_cache->get_callback_total_count(""), 39);

        _cache->add_callback("instance_config/system_settings/xid_mgr/rpc_config/client_connections", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        EXPECT_EQ(_cache->get_callback_total_count(""), 40);

        int connections = 0;
        // create callback class
        Counter c(0);
        redis_watcher->set_cb([&c, &connections](const std::string &path, RedisCache::Action action, const nlohmann::json &new_value) {
                EXPECT_EQ(action, RedisCache::REPLACE);
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
        std::shared_ptr<RedisChangeWatcher> redis_watcher = std::make_shared<RedisChangeWatcher>(
            [&c] (const std::string &path, RedisCache::Action action, const nlohmann::json &new_value) {
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

        _cache->add_callback("top_level_string", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_string/key1", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_string/key2", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

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
        std::shared_ptr<RedisChangeWatcher> redis_watcher = std::make_shared<RedisChangeWatcher>(
            [&c] (const std::string &path, RedisCache::Action action, const nlohmann::json &new_value) {
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
        _cache->add_callback("top_level_hash", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key1", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key2", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key3", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key3/bottom_key1", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key3/bottom_key2", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key4", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key4/0", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key4/1", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key4/3", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key4/4", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_hash/top_key5", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        c.increment_by(2);
        _test_client->hset(hash_key, "top_key1", nlohmann::to_string(hash_values["top_key1"]));
        c.wait();

        c.increment_by(2);
        _test_client->hset(hash_key, "top_key2", nlohmann::to_string(hash_values["top_key2"]));
        c.wait();

        c.increment_by(4);
        _test_client->hset(hash_key, "top_key3", nlohmann::to_string(hash_values["top_key3"]));
        c.wait();

        c.increment_by(5);
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        c.increment_by(2);
        _test_client->hset(hash_key, "top_key1", "new_value1");
        c.wait();

        c.increment_by(2);
        _test_client->hset(hash_key, "top_key2", "new_value2");
        c.wait();

        c.increment_by(3);
        hash_values.at("/top_key3/bottom_key1"_json_pointer) = "new_value1";
        _test_client->hset(hash_key, "top_key3", nlohmann::to_string(hash_values["top_key3"]));
        c.wait();

        c.increment_by(3);
        hash_values.at("/top_key3/bottom_key2"_json_pointer) = "new_value2";
        _test_client->hset(hash_key, "top_key3", nlohmann::to_string(hash_values["top_key3"]));
        c.wait();

        nlohmann::json top_key3_value = hash_values["top_key3"];
        c.increment_by(4);
        _test_client->hdel(hash_key, "top_key3");
        c.wait();

        c.increment_by(4);
        _test_client->hset(hash_key, "top_key3", nlohmann::to_string(top_key3_value));
        c.wait();

        nlohmann::json top_key4_value = hash_values["top_key4"];
        c.increment_by(3);
        hash_values.at("/top_key4/0"_json_pointer) = 5;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        c.increment_by(3);
        hash_values.at("/top_key4/1"_json_pointer) = 6;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        c.increment_by(11);
        hash_values.at("top_key4").erase(0);
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        c.increment_by(11);
        hash_values.at("top_key4").insert(hash_values.at("top_key4").begin(), 1);
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        c.increment_by(3);
        hash_values["top_key4"][4] = R"({
            "item_key1": "item_value1",
            "item_key2": "item_value2",
            "item_key3": "item_value3",
            "item_key4": "item_value4"
        })"_json;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        c.increment_by(14);
        hash_values.at("top_key4").erase(0);
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        c.increment_by(11);
        hash_values["top_key4"] = top_key4_value;
        _test_client->hset(hash_key, "top_key4", nlohmann::to_string(hash_values["top_key4"]));
        c.wait();

        c.increment_by(2);
        _test_client->hset(hash_key, "top_key5", "value5");
        c.wait();

        c.increment_by(2);
        _test_client->hdel(hash_key, "top_key5");
        c.wait();

        c.increment_by(10);
        _test_client->del(hash_key);
        c.wait();
    }

    TEST_F(RedisCache_Test, TestTopLevelArray) {
        Counter c(0);
        std::shared_ptr<RedisChangeWatcher> redis_watcher = std::make_shared<RedisChangeWatcher>(
            [&c] (const std::string &path, RedisCache::Action action, const nlohmann::json &new_value) {
                c.decrement();
        });

        nlohmann::json array_values = R"([
            "value1",
            "value2",
            "value3",
            "value4"
        ])"_json;

        std::string array_key = make_key_string("top_level_array");
        _cache->add_callback("top_level_array", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_array/0", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_array/1", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_array/2", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_array/3", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_array/4", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);
        _cache->add_callback("top_level_array/5", (RedisCache::ADD | RedisCache::REMOVE | RedisCache::REPLACE), redis_watcher);

        c.increment_by(2);
        _test_client->sadd(array_key, nlohmann::to_string(array_values[0]));
        c.wait();

        c.increment_by(4);
        _test_client->sadd(array_key, nlohmann::to_string(array_values[1]));
        c.wait();

        c.increment_by(2);
        _test_client->sadd(array_key, nlohmann::to_string(array_values[2]));
        c.wait();

        c.increment_by(2);
        _test_client->sadd(array_key, nlohmann::to_string(array_values[3]));
        c.wait();

        c.increment_by(6);
        _test_client->srem(array_key, nlohmann::to_string(array_values[0]));
        c.wait();

        c.increment_by(4);
        _test_client->del(array_key);
        c.wait();
    }
};