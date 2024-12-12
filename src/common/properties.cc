#include <string>
#include <stdexcept>
#include <fstream>
#include <iostream>

#include <nlohmann/json.hpp>

#include <common/exception.hh>
#include <common/environment.hh>
#include <common/properties.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/json.hh>
#include <common/logging.hh>

namespace springtail {

    Properties* Properties::_instance {nullptr};
    std::once_flag Properties::_init_flag;

    nlohmann::json
    Properties::get(const std::string &key)
    {
        if (_instance == nullptr) {
            throw Error("Error Properties instance not initialized");
        }
        return _instance->_json[key];
    }

    void
    Properties::_read_environment()
    {
        // iterate through the environment and add to json config
        for (auto &variable: environment::Variables) {
            const char *env_var = std::getenv(std::get<0>(variable));
            environment::Type type = std::get<1>(variable);
            const char *json_obj_name = std::get<2>(variable);
            const char *json_key_name = std::get<3>(variable);

            if (env_var != nullptr) {
                switch (type) {
                    case environment::STR:
                        _json[json_obj_name][json_key_name] = env_var;
                        break;
                    case environment::UINT32:
                        _json[json_obj_name][json_key_name] = std::stoul(env_var);
                        break;
                    case environment::UINT64:
                        _json[json_obj_name][json_key_name] = std::stoull(env_var);
                        break;
                }
            } else {
                _json[json_obj_name][json_key_name] = nullptr;
            }
        }
    }

    void
    Properties::_create_redis_client()
    {
        // verify we have the redis config
        if (_json.contains(REDIS_CONFIG) == false) {
            throw Error("Error missing redis config in environment\nTry setting SPRINGTAIL_PROPERTIES_FILE to settings.json");
        }

        nlohmann::json redis_config = _json[REDIS_CONFIG];
        if (redis_config.contains("host") == false ||
            redis_config["host"].is_null() ||
            redis_config.contains("port") == false ||
            redis_config["port"].is_null() ||
            redis_config.contains("config_db") == false ||
            redis_config["config_db"].is_null() ||
            redis_config.contains("db") == false ||
            redis_config["db"].is_null()) {
            throw Error("Error missing redis config in environment\nTry setting SPRINGTAIL_PROPERTIES_FILE to settings.json");            throw Error("Error missing redis config in environment");
        }

        // extract redis config from json
        std::string hostname = redis_config["host"];
        int port = redis_config["port"];
        std::string user = redis_config["user"];
        std::string password; // password is optional
        Json::get_to<std::string>(redis_config, "password", password);
        int config_db = redis_config["config_db"];

        // create connection options for config db
        sw::redis::ConnectionOptions connect_options;
        connect_options.host = hostname;
        connect_options.port = port;
        connect_options.user = user;
        connect_options.password = password;
        connect_options.db = config_db;

        // create redis client just for accessing config db
        _redis_config_client = std::make_shared<RedisClient>(connect_options);
    }

    void
    Properties::_read_redis_properties()
    {
        _create_redis_client();

        // check for db_instance_id in org config and extract
        // this is set by the environment variable
        if (_json[ORG_CONFIG]["db_instance_id"].is_null()) {
            throw Error("Error missing db_instance_id in redis");
        }

        uint64_t db_instance_id;
        Json::get_to<uint64_t>(_json[ORG_CONFIG], "db_instance_id", db_instance_id);

        // read the system properties from redis
        std::string db_instance_key = std::format(redis::DB_INSTANCE_CONFIG, std::to_string(db_instance_id));
        std::string system_key = "system_settings";

        // read the system properties from redis
        std::optional<std::string> system_value = _redis_config_client->hget(db_instance_key, system_key);
        if (!system_value.has_value()) {
            throw Error("Error missing system settings in redis");
        }

        // convert system_value to json and merge to _json
        nlohmann::json system_json = nlohmann::json::parse(system_value.value());

        _json.merge_patch(system_json);
    }

    void
    Properties::_load_redis(const std::string &config_file)
    {
        // check if file exists
        if (!std::filesystem::exists(std::filesystem::path(config_file))) {
            throw Error("Error missing config file");
        }

        // Load the system settings into json
        std::ifstream file(config_file);
        nlohmann::json system_json;
        file >> system_json;

        // Extract system config
        _json[LOGGING_CONFIG] = system_json["logging"];
        _json[IOPOOL_CONFIG] = system_json["iopool"];
        _json[WRITE_CACHE_CONFIG] = system_json["write_cache"];
        _json[XID_MGR_CONFIG] = system_json["xid_mgr"];
        _json[STORAGE_CONFIG] = system_json["storage"];
        _json[REDIS_CONFIG] = system_json["redis"];
        _json[LOG_MGR_CONFIG] = system_json["log_mgr"];
        _json[SYS_TBL_MGR_CONFIG] = system_json["sys_tbl_mgr"];
        _json[ORG_CONFIG] = system_json["org"];
        _json[FS_CONFIG] = system_json["fs"];

        // get the redis client
        _create_redis_client();

        // Clear the Redis data and config databases
        _redis_config_client->flushdb();

        // set db instance id
        uint64_t db_instance_id = system_json["org"]["db_instance_id"].get<uint64_t>();
        std::string db_instance_key = "instance_config:" + std::to_string(db_instance_id);
        _redis_config_client->hset(db_instance_key, "id", std::to_string(db_instance_id));

        // set primary db
        nlohmann::json db_instance_json = system_json["db_instances"][std::to_string(db_instance_id)];
        _redis_config_client->hset(db_instance_key, "primary_db", db_instance_json["primary_db"].dump());

        // Set the hostnames for ingestion and proxy instances
        _redis_config_client->hset(db_instance_key, "hostname:ingestion", db_instance_json["hostname:ingestion"].get<std::string>());
        _redis_config_client->hset(db_instance_key, "hostname:proxy", db_instance_json["hostname:proxy"].get<std::string>());

        // Set the database ids
        std::string db_ids = db_instance_json["database_ids"].dump();
        _redis_config_client->hset(db_instance_key, "database_ids", db_ids);

        // Setup system settings
        _redis_config_client->hset(db_instance_key, "system_settings", _json.dump());

        // Setup db_config
        for (const auto& db_id : db_instance_json["database_ids"]) {
            nlohmann::json db_json = system_json["databases"][db_id.get<std::string>()];
            std::string db_key = "db_config:" + std::to_string(db_instance_id);
            _redis_config_client->hset(db_key, db_id.get<std::string>(), db_json.dump());

            // Set state; default to initialize
            std::string db_state_key = "instance_state:" + std::to_string(db_instance_id);
            _redis_config_client->hset(db_state_key, db_id.get<std::string>(), "initialize");
        }

        // Create hset for fdws
        std::string fdw_key = "fdw:" + std::to_string(db_instance_id);
        for (const auto& fdw_id : system_json["fdws"].items()) {
            std::string fdw_json_str = fdw_id.value().dump();
            _redis_config_client->hset(fdw_key, fdw_id.key(), fdw_json_str);
        }
    }

    Properties::Properties()
    {
        // check for an override properties file;
        // if it exists use it rather than reading the config from redis
        const char *file = std::getenv(environment::PROPERTIES_FILE_OVERRIDE);
        if (file != nullptr) {
            SPDLOG_INFO("Properties override file: {}", file);

            // read the system properties from the configuration file
            _load_redis(file);
        } else {
            // read the base config from the environment
            _read_environment();

            // read the system properties from redis
            _read_redis_properties();
        }

        // check for overrides in the environment variables
        // Note: overrides will only apply to config in _json
        const char *var = std::getenv(environment::ENV_OVERRIDE);
        if (var == nullptr) {
            return; // no overrides, exit
        }

        // overrides are in the form <key>=<val>;<key2>=<val2> where keys are a dot-separated
        // path within the json configuration
        std::string_view props(var);
        std::size_t pos = 0;
        while (pos < props.size()) {
            auto *item = &_json;
            std::string_view key, val;
            std::size_t start = pos;
            std::string next_token = ".=";
            do {
                std::size_t token = props.find_first_of(next_token, start);
                if (token == std::string_view::npos) {
                    token = props.size();
                }

                if (token == props.size() || props[token] == ';') {
                    assert(key.size());
                    val = props.substr(start, token - start);

                    // set the overridden value
                    // note: all override values are stored as strings
                    (*item)[key] = val;
                } else if (props[token] == '=') {
                    key = props.substr(start, token - start);
                    next_token = ";";
                } else {
                    assert(props[token] == '.');
                    key = props.substr(start, token - start);
                    item = &((*item)[key]);
                }

                start = token + 1;
            } while (val.empty() && start < props.size());

            // set the position to the start of the next entry
            pos = start;
        }
    }

    std::map<uint64_t, std::string>
    Properties::get_databases()
    {
        std::map<uint64_t, std::string> dbnames;

        // get the db_instance_id (initially set from env or system.json)
        uint64_t db_instance_id = get_db_instance_id();

        // lookup the db ids from the db_instance config
        std::string db_instance_key = std::format(redis::DB_INSTANCE_CONFIG, db_instance_id);

        // get the database_ids from the db_instance_key
        nlohmann::json db_ids;

        RedisClientPtr redis_client = _get_redis_client();
        std::optional<std::string> db_id_str = redis_client->hget(db_instance_key, "database_ids");
        if (!db_id_str.has_value()) {
            throw RedisNotFoundError("Error missing database_ids in redis");
        }

        // convert to json
        db_ids = nlohmann::json::parse(db_id_str.value());

        // iterate through the db_ids and get the db_config_id
        for (auto &db_id_json: db_ids) {
            // get db config based on db_id
            uint64_t db_id;
            if (db_id_json.is_string()) {
                std::string db_id_str = db_id_json.get<std::string>();
                db_id = std::stoull(db_id_str);
            } else if (db_id_json.is_number()) {
                db_id = db_id_json.get<uint64_t>();
            } else {
                throw Error("Error invalid db_id type");
            }
            nlohmann::json db_config = get_db_config(db_id);
            assert (db_config.contains("name"));
            dbnames[db_id] = db_config["name"];
        }

        return dbnames;
    }

    nlohmann::json
    Properties::get_db_config(uint64_t db_id)
    {
        // get the db_instance_id (initially set from env or system.json)
        uint64_t db_instance_id = get_db_instance_id();

        // otherwise, we are using redis
        RedisClientPtr redis_client = _get_redis_client();

        // get the redis client and lookup the db ids from the db_instance config
        std::string db_config_key = std::format(redis::DB_CONFIG, db_instance_id);
        std::optional<std::string> db_config_str = redis_client->hget(db_config_key, std::to_string(db_id));
        if (!db_config_str.has_value()) {
            throw RedisNotFoundError("Error missing db_config_id in redis");
        }

        // convert to json
        return nlohmann::json::parse(db_config_str.value());
    }

    std::string
    Properties::get_db_state(uint64_t db_id)
    {
        // get the db_instance_id (initially set from env or system.json)
        uint64_t db_instance_id = get_db_instance_id();

        // need to use redis
        RedisClientPtr redis_client = _get_redis_client();

        // get the redis client and lookup the db ids from the db_instance config
        std::string db_instance_state_hash = std::format(redis::DB_INSTANCE_STATE, db_instance_id);
        std::optional<std::string> db_state_str = redis_client->hget(db_instance_state_hash, std::to_string(db_id));
        if (!db_state_str.has_value()) {
            throw RedisNotFoundError("Error missing db_state in redis");
        }

        return db_state_str.value();
    }

    void
    Properties::set_db_state(uint64_t db_id, const std::string &state)
    {
        // get the db_instance_id (initially set from env or system.json)
        uint64_t db_instance_id = get_db_instance_id();

        // need to use redis
        RedisClientPtr redis_client = _get_redis_client();

        // get the redis client and lookup the db ids from the db_instance config
        std::string db_instance_state_hash = std::format(redis::DB_INSTANCE_STATE, db_instance_id);
        redis_client->hset(db_instance_state_hash, std::to_string(db_id), state);

        // publish the state
        redis_client->publish(fmt::format(redis::PUBSUB_DB_STATE_CHANGES, db_instance_id), fmt::format("{}:{}", db_id, state));
    }

    void
    Properties::publish_liveness_notification(const std::string &msg)
    {
        // get the db_instance_id (initially set from env or system.json)
        uint64_t db_instance_id = get_db_instance_id();

        // need to use redis
        RedisClientPtr redis_client = _get_redis_client();

        // publish the msg
        redis_client->publish(std::format(redis::PUBSUB_LIVENESS_NOTIFY, db_instance_id), msg);
    }

    std::string
    Properties::get_db_name(uint64_t db_id)
    {
        nlohmann::json db_config = get_db_config(db_id);
        assert (db_config.contains("name"));

        return db_config["name"];
    }

    std::vector<std::string>
    Properties::get_fdw_ids()
    {
        // get the db_instance_id (initially set from env or system.json)
        uint64_t db_instance_id = get_db_instance_id();

        // get the redis client and lookup the db ids from the db_instance config
        RedisClientPtr redis_client = _get_redis_client();
        std::string fdw_key = std::format(redis::HASH_FDW, db_instance_id);

        std::vector<std::string> fdw_ids;
        redis_client->hkeys(fdw_key, std::back_inserter(fdw_ids));

        return fdw_ids;
    }

    nlohmann::json
    Properties::_get_primary_db_config()
    {
        // get the db_instance_id (initially set from env or system.json)
        uint64_t db_instance_id = get_db_instance_id();

        // get the org config and see if there is a replication_user_password
        nlohmann::json org = _instance->_json[ORG_CONFIG];
        std::string replication_user_password;
        if (org.contains("replication_user_password")) {
            Json::get_to<std::string>(org, "replication_user_password", replication_user_password);
        }

        // otherwise, we are using redis
        RedisClientPtr redis_client = _get_redis_client();

        // get the redis client and lookup the db ids from the db_instance config
        std::string db_instance_key = std::format(redis::DB_INSTANCE_CONFIG, db_instance_id);
        std::optional<std::string> db_instance_str = redis_client->hget(db_instance_key, "primary_db");
        if (!db_instance_str.has_value()) {
            throw RedisNotFoundError("Error missing db_instance_id in redis");
        }

        // convert to json and add replication user password
        nlohmann::json json = nlohmann::json::parse(db_instance_str.value());
        if (!replication_user_password.empty()) {
            json["password"] = replication_user_password;
        }
        return json;
    }

    void Properties::get_primary_db_config(std::string &host, int &port, std::string &user, std::string &password) {
        auto primary_config = Properties::_get_primary_db_config();

        auto optional_host = Json::get<std::string>(primary_config, "host");
        auto optional_port = Json::get<uint16_t>(primary_config, "port");
        auto optional_user = Json::get<std::string>(primary_config, "replication_user");
        auto optional_password = Json::get<std::string>(primary_config, "password");

        if (optional_host.has_value() && optional_port.has_value() && optional_user.has_value() && optional_password.has_value()) {
            host = optional_host.value();
            port = optional_port.value();
            user = optional_user.value();
            password = optional_password.value();
        } else {
            SPDLOG_ERROR("Could not find the value for primary database either host, port, user, or password");
            throw Error();
        }
    }

    nlohmann::json
    Properties::get_fdw_config(const std::string &fdw_id)
    {
        // get the db_instance_id (initially set from env or system.json)
        uint64_t db_instance_id = get_db_instance_id();

        // otherwise, we are using redis
        RedisClientPtr redis_client = _get_redis_client();

        // get the redis client and lookup the db ids from the db_instance config
        std::string fdw_key = std::format(redis::HASH_FDW, db_instance_id);

        std::optional<std::string> fdw_config_str = redis_client->hget(fdw_key, fdw_id);
        if (!fdw_config_str.has_value()) {
            throw RedisNotFoundError("Error missing fdw_config in redis");
        }

        // get the org config and see if there is a fdw_user_password
        nlohmann::json org = _instance->_json[ORG_CONFIG];
        std::string fdw_user_password;
        if (org.contains("fdw_user_password")) {
            Json::get_to<std::string>(org, "fdw_user_password", fdw_user_password);
        }

        // convert to json
        nlohmann::json json = nlohmann::json::parse(fdw_config_str.value());

        // set the fdw password if it exists
        if (!fdw_user_password.empty()) {
            json["password"] = fdw_user_password;
        }

        return json;
    }

    std::string
    Properties::_get_ingestion_hostname()
    {
        uint64_t db_instance_id = get_db_instance_id();

        RedisClientPtr redis_client = _get_redis_client();
        std::optional<std::string> hostname = redis_client->hget(std::format(redis::DB_INSTANCE_CONFIG, db_instance_id), "hostname:ingestion");
        if (!hostname.has_value()) {
            throw RedisNotFoundError("Error missing hostname::ingestion in redis");
        }
        return hostname.value();
    }

    std::string
    Properties::get_pid_path()
    {
        nlohmann::json props = Properties::get(Properties::LOGGING_CONFIG);
        std::string pid_path;
        Json::get_to<std::string>(props, Properties::PID_PATH, pid_path, "/var/springtail/pids");
        return pid_path;
    }

    void
    Properties::init()
    {
        if (_instance == nullptr) {
            // once init
            std::call_once(_init_flag, []() {
                _instance = new Properties();
            });
        }
    }
}
