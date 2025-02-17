#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <common/common.hh>
#include <common/json.hh>
#include <common/redis.hh>
#include <proxy/database.hh>

using namespace springtail;
using namespace springtail::pg_proxy;
using namespace testing;

namespace {
    class DatabaseMgr_Test : public testing::Test {
    protected:
        static void SetUpTestSuite()
        {
            springtail_init();

            DatabaseMgr::get_instance()->init();

            _data_client = RedisMgr::get_instance()->get_client();

            int db_id;
            tie(db_id, _config_client) = RedisMgr::get_instance()->create_client(true);

        }
        static void TearDownTestSuite()
        {
            DatabaseMgr::shutdown();
            RedisMgr::shutdown();
        }
        static inline RedisClientPtr _config_client;
        static inline RedisClientPtr _data_client;

        void _add_database(uint64_t db_id, const std::string &name)
        {
            ASSERT_THAT(DatabaseMgr::get_instance()->get_database_id(name), Eq(std::nullopt));
            EXPECT_FALSE(DatabaseMgr::get_instance()->is_database_replicated(name));
            EXPECT_FALSE(DatabaseMgr::get_instance()->is_database_ready(db_id));

            uint64_t instance_id = Properties::get_db_instance_id();
            auto ts = _config_client->transaction(false, false);

            // hset 1234:instance_state 2 running
            std::string key = fmt::format(redis::DB_INSTANCE_STATE, instance_id);
            std::string db_id_str = std::to_string(db_id);
            ts.hset(key, db_id_str, redis::db_state_change::REDIS_STATE_RUNNING);

            // hset 1234:db_config 2 "{\"name\": \"springtail_1\", \"replication_slot\": \"springtail_slot\", \"publication_name\": \"springtail_pub\", \"include\": {\"schemas\": [\"*\"]}}"
            key = fmt::format(redis::DB_CONFIG, instance_id);
            std::string name_str = fmt::format("\"name\": \"{}\"", name);
            std::string replication_slot = fmt::format("\"replication_slot\": \"{}_slot\"", name);
            std::string publication_name = fmt::format("\"publication_name\": \"{}_pub\"", name);
            std::string config_str = "{" + name_str + ", " + replication_slot + ", " + publication_name + ", \"include\": {\"schemas\": [\"*\"]}}";
            ts.hset(key, db_id_str, config_str);

            // hset 1234:instance_config database_ids "[\"1\", \"2\"]"
            key = fmt::format(redis::DB_INSTANCE_CONFIG, instance_id);
            std::map<uint64_t, std::string> databases = Properties::get_databases();
            databases[db_id] = name;
            std::string db_ids_str = "[";
            for(const auto& pair : databases) {
                db_ids_str += "\"" + std::to_string(pair.first) + "\",";
            }
            db_ids_str.pop_back();
            db_ids_str += "]";
            ts.hset(key, "database_ids",  db_ids_str);

            ts.exec();

            sleep(1);

            ASSERT_THAT(DatabaseMgr::get_instance()->get_database_id(name), Optional(db_id));
            EXPECT_TRUE(DatabaseMgr::get_instance()->is_database_replicated(name));
            EXPECT_TRUE(DatabaseMgr::get_instance()->is_database_ready(db_id));
        }

        void _remove_database(uint64_t db_id, const std::string &name)
        {
            ASSERT_THAT(DatabaseMgr::get_instance()->get_database_id(name), Optional(db_id));
            EXPECT_TRUE(DatabaseMgr::get_instance()->is_database_replicated(name));
            EXPECT_TRUE(DatabaseMgr::get_instance()->is_database_ready(db_id));

            uint64_t instance_id =Properties::get_db_instance_id();
            auto ts = _config_client->transaction(false, false);

            // hset 1234:instance_config database_ids "[\"1\"]"
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

            // hdel :1234:instance_state 2
            key = fmt::format(redis::DB_INSTANCE_STATE, instance_id);
            std::string db_id_str = std::to_string(db_id);
            ts.hdel(key, db_id_str);

            // hdel 1234:db_config 2
            key = fmt::format(redis::DB_CONFIG, instance_id);
            ts.hdel(key, db_id_str);
            ts.exec();

            sleep(1);

            ASSERT_THAT(DatabaseMgr::get_instance()->get_database_id(name), Eq(std::nullopt));
            EXPECT_FALSE(DatabaseMgr::get_instance()->is_database_replicated(name));
            EXPECT_FALSE(DatabaseMgr::get_instance()->is_database_ready(db_id));
        }

        void _change_database_state(uint64_t db_id, redis::db_state_change::DBState state) {
            uint64_t instance_id = Properties::get_db_instance_id();
            std::string key = fmt::format(redis::DB_INSTANCE_STATE, instance_id);
            std::string db_id_str = std::to_string(db_id);
            std::string db_state_str = redis::db_state_change::db_state_to_name[state];
            _config_client->hset(key, db_id_str, db_state_str);

            sleep(1);

            if (state != redis::db_state_change::DB_STATE_RUNNING) {
                EXPECT_FALSE(DatabaseMgr::get_instance()->is_database_ready(db_id));
            } else {
                EXPECT_TRUE(DatabaseMgr::get_instance()->is_database_ready(db_id));
            }
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

    TEST_F(DatabaseMgr_Test, TestChangeReplicatedDatabaseState) {
        _change_database_state(1, redis::db_state_change::DB_STATE_STARTUP);
        _change_database_state(1, redis::db_state_change::DB_STATE_RUNNING);
        _change_database_state(1, redis::db_state_change::DB_STATE_SYNCING);
        _change_database_state(1, redis::db_state_change::DB_STATE_RUNNING);
    }
}