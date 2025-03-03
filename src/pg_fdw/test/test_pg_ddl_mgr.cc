#include <filesystem>
#include <cstdio>
#include <memory>

#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/json.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <pg_fdw/pg_ddl_mgr.hh>
#include <test/services.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

namespace {
    static constexpr char VERIFY_TABLE_EXISTS[] =
        "SELECT foreign_table_schema, foreign_table_name "
        "FROM information_schema.foreign_tables "
        "WHERE foreign_table_schema = '{}' and foreign_table_name = '{}'";


    static std::string exec_popen(const std::string& command)
    {
        std::string result;
        char buffer[128];

        using Deleter = int(*)(FILE*);
        std::unique_ptr<FILE, Deleter> pipe(popen(command.c_str(), "r"), &pclose);

        if (!pipe) {
            throw std::runtime_error("popen() failed!");
        }

        // generate result
        while (fgets(buffer, sizeof(buffer), pipe.get()) != nullptr) {
            result += buffer;
        }
        SPDLOG_INFO("exec_popen: {}", result);

        // remove new line
        std::erase_if(result, [](char c) { return c == '\n'; });

        return result;
    }

    class PgDDLMgr_Test : public testing::Test {
    private:
        /** Check for the existence of springtail files for fdw */
        static bool _check_pg_config()
        {
            try {
                std::string pg_config_dir = exec_popen("pg_config --sharedir");
                pg_config_dir += "/extension/";

                std::string springtail_sql_file = pg_config_dir + "springtail_fdw--1.0.sql";
                std::string springtail_control_file = pg_config_dir + "springtail_fdw.control";

                bool sql_file_found = std::filesystem::exists(springtail_sql_file);
                bool control_file_found = std::filesystem::exists(springtail_control_file);
                if (sql_file_found && control_file_found) {
                    return true;
                }
            } catch (...) {
                SPDLOG_ERROR("Failed to find postgres config files for springtail");
            }

            return false;
        }

    public:
        static void SetUpTestSuite() {
            if (!_check_pg_config()) {
                _skip_test = true;
                GTEST_SKIP() << "Postgres replica config problem, skipping test";
            }

            std::vector<std::unique_ptr<ServiceRunner>> service_runners = test::get_services(true, false, true);
            std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
            runners.emplace();
            std::move(service_runners.begin(), service_runners.end(), std::back_inserter(runners.value()));

            // Add PgDDLMgrRunner
            std::string username{"springtail"};
            std::string password{"springtail"};
            std::string socket_hostname{"/var/run/postgresql"};
            runners->emplace_back(new PgDDLMgrRunner(username, password, socket_hostname));

            springtail_init(runners);

            _fdw_id_str = Properties::get_fdw_id();

            // set schemas to public in config
            RedisClientPtr redis_config_client;
            int redis_db_id;
            std::tie(redis_db_id, redis_config_client) = RedisMgr::get_instance()->create_client(true);
            nlohmann::json db_config = Properties::get_instance()->get_db_config(std::stoi(_db_id_str));
            db_config["/include/schemas/0"_json_pointer] = "public";
            std::string key = std::to_string(Properties::get_instance()->get_db_instance_id()) + ":db_config";
            redis_config_client->hset(key, _db_id_str, nlohmann::to_string(db_config));

            _pg_ddl_mgr_thread = std::thread(&PgDDLMgr::run, PgDDLMgr::get_instance());

            // set up connection to the database
            _create_replica_connection();
        }

        static void TearDownTestSuite() {
            if (_skip_test) {
                return;
            }
            if (_conn != nullptr) {
                _conn->disconnect();
            }
            PgDDLMgr::get_instance()->notify_shutdown();
            if (_pg_ddl_mgr_thread.has_value()) {
                _pg_ddl_mgr_thread.value().join();
            }

            springtail_shutdown();
        }
    protected:
        static void _create_replica_connection() {
            _conn = std::make_shared<LibPqConnection>();
            std::string db_name = Properties::get_db_name(std::stoi(_db_id_str));
            nlohmann::json fdw_config;
            fdw_config = Properties::get_fdw_config(_fdw_id_str);
            std::string hostname;
            std::string username;
            std::string password;
            int port;
            std::string db_prefix = fdw_config.at("db_prefix").get<std::string>();

            Json::get_to<std::string>(fdw_config, "host", hostname);
            Json::get_to<std::string>(fdw_config, "fdw_user", username);
            Json::get_to<std::string>(fdw_config, "password", password);
            Json::get_to<int>(fdw_config, "port", port);

            _conn->connect(hostname, db_prefix + db_name, username, password, port, false);
        }

        void SetUp() override {
            _redis_client_data = RedisMgr::get_instance()->get_client();
            _subscriber = RedisMgr::get_instance()->get_subscriber(1, false);
        }
        void TearDown() override {
            // placeholder, left empty for now
        }

        static inline std::optional<std::thread> _pg_ddl_mgr_thread;
        static inline std::string _db_id_str{"1"};
        static inline std::string _fdw_id_str{"1"};
        static inline LibPqConnectionPtr _conn;
        static inline bool _skip_test{false};

        RedisClientPtr _redis_client_data;
        RedisMgr::SubscriberPtr _subscriber;
        std::string _fdw_key{fmt::format(redis::QUEUE_DDL_FDW, Properties::get_db_instance_id(), _fdw_id_str)};

        nlohmann::json _create_table_ddl(uint64_t xid, const std::string &schema_name, const std::string &table_name,
                    uint32_t table_oid, std::vector<std::tuple<std::string, uint32_t, bool>> columns) {
            nlohmann::json create_table_json = {
                { "db_id", 1 },
                { "xid", xid },
                { "ddls", nlohmann::json::array(
                    {
                    {
                            { "action", "create" },
                            { "schema", schema_name },
                            { "table", table_name },
                            // any number
                            { "tid", table_oid },
                            { "columns", nlohmann::json::array({}) }
                        }
                    })
                }
            };
            for (auto item: columns) {
                std::string name = std::get<0>(item);
                uint32_t type_oid = std::get<1>(item);
                bool nullable = std::get<2>(item);
                nlohmann::json column_json = {
                    {"name", name},
                    // any number in numerical order
                    {"type", type_oid},
                    {"nullable", nullable}
                };
                create_table_json["/ddls/0/columns"_json_pointer].push_back(column_json);
            }
            return create_table_json;
        }

        void _verify_table_exists(const std::string &schema_name, const std::string &table_name) {
            std::string query = fmt::format(VERIFY_TABLE_EXISTS, schema_name, table_name);
            _conn->exec(query);
            int rows = _conn->ntuples();
            EXPECT_EQ(rows, 1);
            std::string result_schema_name = _conn->get_string(0, 0);
            std::string result_table_name = _conn->get_string(0, 1);
            EXPECT_EQ(result_schema_name, schema_name);
            EXPECT_EQ(result_table_name, table_name);
        }

        class RedisNotification {
        public:
            explicit RedisNotification(RedisMgr::SubscriberPtr subscriber) : _subscriber(subscriber) {
                _subscriber->psubscribe(_pattern);
                _subscriber->on_pmessage(
                    [this](const std::string &pattern, const std::string &channel, const std::string &msg) {
                        std::vector<std::string> channel_parts;
                        common::split_string(":", channel, channel_parts);
                        EXPECT_EQ(channel_parts[1], std::to_string(Properties::get_db_instance_id()));
                        if (msg == "del") {
                            if (channel_parts.size() == 7 && channel_parts[2] == "queue" && channel_parts[6] == "active") {
                                EXPECT_EQ(channel_parts[5], _fdw_id_str);
                                _done = true;
                            }
                        }
                    }
                );
            }
            void wait_for_notifications() {
                while (!_done) {
                    try {
                        // consume from subscriber, timeout is set above
                        _subscriber->consume();
                    } catch (const sw::redis::TimeoutError &e) {
                        // timeout, check for shutdown
                        continue;
                    }
                }
                _subscriber->punsubscribe(_pattern);
            }
            void reset() {
                _done = false;
            }
        private:
            RedisMgr::SubscriberPtr _subscriber;
            std::atomic<bool> _done{false};
            std::string _pattern{"__keyspace@1__:" + std::to_string(Properties::get_db_instance_id()) + ":*"};
        };
    };

    TEST_F(PgDDLMgr_Test, TestCreateTable) {
        RedisNotification redis_notification(_subscriber);
        std::vector<std::tuple<std::string, uint32_t, bool>> columns = {
            {"col1", 25, true},
            {"col2", 16, false}
        };
        nlohmann::json json_ddl = _create_table_ddl(12, "public", "test", 1, columns);

        _redis_client_data->lpush(_fdw_key, nlohmann::to_string(json_ddl));
        redis_notification.wait_for_notifications();

        _verify_table_exists("public", "test");
    }
};