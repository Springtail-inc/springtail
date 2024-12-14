#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/json.hh>
#include <common/redis.hh>
#include <proxy/database.hh>

using namespace springtail;
using namespace springtail::pg_proxy;

namespace {
    class DatabaseMgr_Test : public testing::Test {
    protected:
        static void SetUpTestSuite()
        {
            springtail_init();

            DatabaseMgr::get_instance()->init();
            _data_client = RedisMgr::get_instance()->get_client();

            sw::redis::ConnectionOptions con_opt;
            nlohmann::json json = Properties::get(Properties::REDIS_CONFIG);
            int keep_alive_secs;

            Json::get_to<std::string>(json, "host", con_opt.host, "localhost");
            Json::get_to<std::string>(json, "user", con_opt.user, "user");
            Json::get_to<std::string>(json, "password", con_opt.password);
            Json::get_to<int>(json, "port", con_opt.port, 6379);
            Json::get_to<int>(json, "config_db", con_opt.db, RedisMgr::REDIS_CONFIG_DB);
            Json::get_to<int>(json, "keep_alive_sec", keep_alive_secs, 30);

            con_opt.keep_alive_s = std::chrono::seconds(keep_alive_secs);
            con_opt.keep_alive = true;
            con_opt.resp = 3;
            con_opt.socket_timeout = std::chrono::milliseconds(0);

            sw::redis::ConnectionPoolOptions pool_opt;
            pool_opt.size = 1;
            pool_opt.connection_idle_time = std::chrono::seconds(60);
            pool_opt.connection_lifetime = std::chrono::seconds(0);

            _config_client = std::make_shared<RedisClient>(con_opt, pool_opt);
        }
        static void TearDownTestSuite()
        {
            DatabaseMgr::shutdown();
        }
        static inline RedisClientPtr _config_client;
        static inline RedisClientPtr _data_client;

        void _add_database(uint64_t db_id, const std::string &name)
        {
            EXPECT_FALSE(DatabaseMgr::get_instance()->get_database_id(name).has_value());
            EXPECT_FALSE(DatabaseMgr::get_instance()->is_database_replicated(name));
            EXPECT_FALSE(DatabaseMgr::get_instance()->is_database_ready(db_id));

            uint64_t instance_id =Properties::get_db_instance_id();
            auto ts = _config_client->transaction(false, false);

            // hset instance_config:1234 database_ids "[\"1\", \"2\"]"
            std::string key = fmt::format(redis::DB_INSTANCE_CONFIG, instance_id);
            std::map<uint64_t, std::string> databases = Properties::get_databases();
            databases[db_id] = name;
            std::string db_ids_str = "[";
            for(const auto& pair : databases) {
                db_ids_str += "\"" + std::to_string(pair.first) + "\",";
            }
            db_ids_str.pop_back();
            db_ids_str += "]";
            ts.hset(key, "database_ids",  db_ids_str);

            std::string msg = fmt::format("{}:{}", redis::db_state_change::REDIS_ACTION_ADD, db_id);
            ts.publish(fmt::format(redis::PUBSUB_DB_CONFIG_CHANGES, instance_id), msg);

            // hset instance_state:1234 2 running
            key = fmt::format(redis::DB_INSTANCE_STATE, instance_id);
            std::string db_id_str = std::to_string(db_id);
            ts.hset(key, db_id_str, "running");
            msg = fmt::format("{}:running", db_id);
            ts.publish(fmt::format(redis::PUBSUB_DB_STATE_CHANGES, instance_id), msg);

            // hset db_config:1234 2 "{\"name\": \"springtail_1\", \"replication_slot\": \"springtail_slot\", \"publication_name\": \"springtail_pub\", \"include\": {\"schemas\": [\"*\"]}}"
            key = fmt::format(redis::DB_CONFIG, instance_id);
            std::string name_str = fmt::format("\"name\": \"{}\"", name);
            std::string replication_slot = fmt::format("\"replication_slot\": \"{}_slot\"", name);
            std::string publication_name = fmt::format("\"publication_name\": \"{}_pub\"", name);
            std::string config_str = "{" + name_str + ", " + replication_slot + ", " + publication_name + ", \"include\": {\"schemas\": [\"*\"]}}";
            ts.hset(key, db_id_str, config_str);
            ts.exec();

            sleep(1);

            EXPECT_TRUE(DatabaseMgr::get_instance()->get_database_id(name).has_value());
            EXPECT_EQ(DatabaseMgr::get_instance()->get_database_id(name).value(), db_id);
            EXPECT_TRUE(DatabaseMgr::get_instance()->is_database_replicated(name));
            EXPECT_TRUE(DatabaseMgr::get_instance()->is_database_ready(db_id));
        }

        void _remove_database(uint64_t db_id, const std::string &name)
        {
            EXPECT_TRUE(DatabaseMgr::get_instance()->get_database_id(name).has_value());
            EXPECT_EQ(DatabaseMgr::get_instance()->get_database_id(name).value(), db_id);
            EXPECT_TRUE(DatabaseMgr::get_instance()->is_database_replicated(name));
            EXPECT_TRUE(DatabaseMgr::get_instance()->is_database_ready(db_id));

            uint64_t instance_id =Properties::get_db_instance_id();
            auto ts = _config_client->transaction(false, false);

            // hset instance_config:1234 database_ids "[\"1\"]"
            std::string key = fmt::format(redis::DB_INSTANCE_CONFIG, instance_id);
            std::map<uint64_t, std::string> databases = Properties::get_databases();
            databases.erase(db_id);
            std::string db_ids_str = "[";
            for(const auto& pair : databases) {
                db_ids_str += "\"" + std::to_string(pair.first) + "\",";
            }
            db_ids_str.pop_back();
            db_ids_str += "]";
            ts.hset(key, "database_ids",  db_ids_str);

            std::string msg = fmt::format("{}:{}", redis::db_state_change::REDIS_ACTION_REMOVE, db_id);
            ts.publish(fmt::format(redis::PUBSUB_DB_CONFIG_CHANGES, instance_id), msg);

            // hdel instance_state:1234 2
            key = fmt::format(redis::DB_INSTANCE_STATE, instance_id);
            std::string db_id_str = std::to_string(db_id);
            ts.hdel(key, db_id_str);

            // hdel db_config:1234 2
            key = fmt::format(redis::DB_CONFIG, instance_id);
            ts.hdel(key, db_id_str);
            ts.exec();

            sleep(1);

            EXPECT_FALSE(DatabaseMgr::get_instance()->get_database_id(name).has_value());
            EXPECT_FALSE(DatabaseMgr::get_instance()->is_database_replicated(name));
            EXPECT_FALSE(DatabaseMgr::get_instance()->is_database_ready(db_id));
        }

        void _add_table(uint64_t db_id, const std::string &schema, const std::string &table, bool test = true)
        {
            if (test) {
                EXPECT_FALSE(DatabaseMgr::get_instance()->is_table_replicated(db_id, "public", schema, table));
            }
            auto ts = _data_client->transaction(false, false);
            RedisDbTables::add_table(ts, db_id, table, schema);
            ts.exec();
            if (test) {
                sleep(1);
                EXPECT_TRUE(DatabaseMgr::get_instance()->is_table_replicated(db_id, "public", schema, table));
            }
        }

        void _remove_table(uint64_t db_id, const std::string &schema, const std::string &table, bool test = true)
        {
            if (test) {
                EXPECT_TRUE(DatabaseMgr::get_instance()->is_table_replicated(db_id, "public", schema, table));
            }
            auto ts = _data_client->transaction(false, false);
            RedisDbTables::remove_table(ts, db_id, table, schema);
            ts.exec();
            if (test) {
                sleep(1);
                EXPECT_FALSE(DatabaseMgr::get_instance()->is_table_replicated(db_id, "public", schema, table));
            }
        }
    };

    TEST_F(DatabaseMgr_Test, TestAddRemoveDatabaseSchemaAndTable) {
        std::string schema1("test_schema1");
        std::string schema2("test_schema2");
        std::string table1("test_table1");
        std::string table2("test_table2");
        _add_table(1, schema1, table1);
        _add_table(1, schema1, table2);
        _add_table(1, schema2, table1);
        _add_table(1, schema2, table2);
        _remove_table(1, schema1, table1);
        _remove_table(1, schema1, table2);
        _remove_table(1, schema2, table1);
        _remove_table(1, schema2, table2);
    }

    TEST_F(DatabaseMgr_Test, TestAddRemoveDatabase) {
        _add_database(2, "springtail_1");
        _remove_database(2, "springtail_1");
    }

    TEST_F(DatabaseMgr_Test, TestAddRemoveDatabaseTablesAfter) {
        // add tables after adding database and clean them up after database removal
        _add_database(2, "springtail_1");

        std::string schema1("test_schema1");
        std::string schema2("test_schema2");
        std::string table1("test_table1");
        std::string table2("test_table2");
        _add_table(2, schema1, table1);
        _add_table(2, schema1, table2);
        _add_table(2, schema2, table1);
        _add_table(2, schema2, table2);

        _remove_database(2, "springtail_1");

        EXPECT_FALSE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema1, table1));
        EXPECT_FALSE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema1, table2));
        EXPECT_FALSE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema2, table1));
        EXPECT_FALSE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema2, table2));

        // clean up redis
        _remove_table(2, schema1, table1, false);
        _remove_table(2, schema1, table2, false);
        _remove_table(2, schema2, table1, false);
        _remove_table(2, schema2, table2, false);
    }

    TEST_F(DatabaseMgr_Test, TestAddRemoveDatabaseTablesBefore) {
        // add tables after before database and remove them before database removal
        std::string schema1("test_schema1");
        std::string schema2("test_schema2");
        std::string table1("test_table1");
        std::string table2("test_table2");
        _add_table(2, schema1, table1, false);
        _add_table(2, schema1, table2, false);
        _add_table(2, schema2, table1, false);
        _add_table(2, schema2, table2, false);

        EXPECT_FALSE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema1, table1));
        EXPECT_FALSE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema1, table2));
        EXPECT_FALSE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema2, table1));
        EXPECT_FALSE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema2, table2));

        _add_database(2, "springtail_1");

        EXPECT_TRUE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema1, table1));
        EXPECT_TRUE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema1, table2));
        EXPECT_TRUE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema2, table1));
        EXPECT_TRUE(DatabaseMgr::get_instance()->is_table_replicated(2, "public", schema2, table2));

        _remove_table(2, schema1, table1);
        _remove_table(2, schema1, table2);
        _remove_table(2, schema2, table1);
        _remove_table(2, schema2, table2);

        _remove_database(2, "springtail_1");
    }
}