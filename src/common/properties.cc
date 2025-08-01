#include <string>
#include <fstream>

#include <absl/log/check.h>
#include <nlohmann/json.hpp>

#include <common/exception.hh>
#include <common/environment.hh>
#include <common/properties.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/common.hh>

namespace {
    void patch_json(nlohmann::json& out, const nlohmann::json& in) 
    {
        for (auto it = in.begin(); it != in.end(); ++it) {
            if (it->is_object()) {
                auto oit = out.find(it.key());
                if (oit == out.end()) {
                    out[it.key()] = *it;
                } else {
                    patch_json(*oit, *it);
                }
            } else {
                out[it.key()] = *it;
            }
        }
    }
}
namespace springtail {

    nlohmann::json
    Properties::get(const std::string &key)
    {
        return get_instance()->_json[key];
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

            std::string env_var_name = std::get<0>(variable);
            std::string env_var_value = env_var == nullptr ? "null" : env_var;

            LOG_INFO("Reading environment variable: {}={}", env_var_name, env_var_value);

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
                    case environment::BOOL:
                        _json[json_obj_name][json_key_name] = common::to_bool(env_var);
                        break;
                }
            } else {
                _json[json_obj_name][json_key_name] = nullptr;
            }
        }
    }

    RedisClientPtr
    Properties::_create_redis_client()
    {
        // verify we have the redis config
        if (_json.contains(REDIS_CONFIG) == false) {
            LOG_ERROR("Error creating redis client, config not found");
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
                LOG_ERROR("Error creating redis, config: {}", _json[REDIS_CONFIG].dump());
                throw Error("Error missing redis config in environment\nTry setting SPRINGTAIL_PROPERTIES_FILE to settings.json");            throw Error("Error missing redis config in environment");
        }

        // extract redis config from json
        std::string hostname = redis_config["host"];
        int port = redis_config["port"];
        std::string user = redis_config["user"];
        std::string password; // password is optional
        Json::get_to<std::string>(redis_config, "password", password);
        int config_db = redis_config["config_db"];

        // check if ssl is enabled
        bool ssl_enabled = Json::get_or<bool>(_json[REDIS_CONFIG], "ssl", false);

        // create connection options for config db
        sw::redis::ConnectionOptions connect_options;
        connect_options.host = hostname;
        connect_options.port = port;
        connect_options.user = user;
        connect_options.password = password;
        connect_options.db = config_db;
        connect_options.resp = 3;
        connect_options.socket_timeout = std::chrono::milliseconds(0);
        connect_options.keep_alive = true;
        connect_options.keep_alive_s = std::chrono::seconds(30);
        connect_options.tls.enabled = ssl_enabled;

        sw::redis::ConnectionPoolOptions pool_options;
        pool_options.size = 5;
        pool_options.connection_idle_time = std::chrono::seconds(0);
        pool_options.connection_lifetime = std::chrono::seconds(60);

        // create redis client just for accessing config db
        return std::make_shared<RedisClient>(connect_options, pool_options);
    }

    void
    Properties::_read_redis_properties()
    {
        RedisClientPtr redis_client = _create_redis_client();

        // check for db_instance_id in org config and extract
        // this is set by the environment variable
        if (_json[ORG_CONFIG]["db_instance_id"].is_null()) {
            throw Error("Error missing db_instance_id in redis");
        }

        uint64_t db_instance_id;
        Json::get_to<uint64_t>(_json[ORG_CONFIG], "db_instance_id", db_instance_id);

        // read the system properties from redis
        std::string db_instance_key = std::format(redis::DB_INSTANCE_CONFIG, db_instance_id);
        std::string system_key = "system_settings";

        // read the system properties from redis
        std::optional<std::string> system_value = redis_client->hget(db_instance_key, system_key);
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

        // patch the config
        const char *patch = std::getenv(environment::ENV_OVERRIDE_JSON);
        if (patch) {
            std::stringstream ss(patch);
            nlohmann::json override_json;
            ss >> override_json;
            patch_json(system_json, override_json);
        }

        // Extract system config
        _json[LOGGING_CONFIG] = system_json["logging"];
        _json[IOPOOL_CONFIG] = system_json["iopool"];
        _json[WRITE_CACHE_CONFIG] = system_json["write_cache"];
        _json[STORAGE_CONFIG] = system_json["storage"];
        _json[REDIS_CONFIG] = system_json["redis"];
        _json[LOG_MGR_CONFIG] = system_json["log_mgr"];
        _json[SYS_TBL_MGR_CONFIG] = system_json["sys_tbl_mgr"];
        _json[ORG_CONFIG] = system_json["org"];
        _json[FS_CONFIG] = system_json["fs"];
        _json[PROXY_CONFIG] = system_json["proxy"];
        _json[OTEL_CONFIG] = system_json["otel"];

        // get the redis client
        RedisClientPtr redis_client = _create_redis_client();

        // Clear the Redis data and config databases
        redis_client->flushall();

        // set db instance id
        uint64_t db_instance_id = system_json["org"]["db_instance_id"].get<uint64_t>();
        std::string db_instance_key = fmt::format(redis::DB_INSTANCE_CONFIG, db_instance_id);
        redis_client->hset(db_instance_key, "id", std::to_string(db_instance_id));

        // set primary db
        nlohmann::json db_instance_json = system_json["db_instances"][std::to_string(db_instance_id)];
        redis_client->hset(db_instance_key, "primary_db", db_instance_json["primary_db"].dump());

        // Set the hostnames for ingestion and proxy instances
        redis_client->hset(db_instance_key, "hostname:ingestion", db_instance_json["hostname:ingestion"].get<std::string>());
        redis_client->hset(db_instance_key, "hostname:proxy", db_instance_json["hostname:proxy"].get<std::string>());

        // Set the database ids
        std::string db_ids = db_instance_json["database_ids"].dump();
        redis_client->hset(db_instance_key, "database_ids", db_ids);

        // Setup system settings
        redis_client->hset(db_instance_key, "system_settings", _json.dump());

        // Setup db_config
        for (const auto& db_id : db_instance_json["database_ids"]) {
            nlohmann::json db_json = system_json["databases"][db_id.get<std::string>()];
            std::string db_key = fmt::format(redis::DB_CONFIG, db_instance_id);
            redis_client->hset(db_key, db_id.get<std::string>(), db_json.dump());

            // Set state; default to initialize
            std::string db_state_key = fmt::format(redis::DB_INSTANCE_STATE, db_instance_id);
            redis_client->hset(db_state_key, db_id.get<std::string>(), "initialize");
        }

        // Create hset for fdws
        std::string fdw_key = fmt::format(redis::HASH_FDW, db_instance_id);
        std::string fdw_id_key = fmt::format(redis::SET_FDW_IDS, db_instance_id);
        for (const auto& fdw_id : system_json["fdws"].items()) {
            std::string fdw_json_str = fdw_id.value().dump();
            redis_client->hset(fdw_key, fdw_id.key(), fdw_json_str);
            redis_client->sadd(fdw_id_key, fdw_id.key());
        }
    }

    void
    Properties::set_env_from_file(const char *config_file)
    {
        // check if file exists
        if (!std::filesystem::exists(std::filesystem::path(config_file))) {
            throw Error("Error missing config file");
        }

        // Load the system settings into json
        std::ifstream file(config_file);
        nlohmann::json system_json;
        file >> system_json;

        // iterate through the environment and add to json config
        for (auto &variable: environment::Variables) {
            const char *env_var = std::get<0>(variable);
            environment::Type type = std::get<1>(variable);
            const char *json_obj_name = std::get<2>(variable);
            const char *json_key_name = std::get<3>(variable);

            // lookup the value in the system settings using object and key
            if (system_json.contains(json_obj_name) && system_json[json_obj_name].contains(json_key_name)) {

                if (system_json[json_obj_name][json_key_name].is_null()) {
                    ::setenv(env_var, "", 1);
                    continue;
                }

                switch (type) {
                    case environment::STR: {
                        std::string val = system_json[json_obj_name][json_key_name].get<std::string>();
                        ::setenv(env_var, val.c_str(), 1);
                        break;
                    }
                    case environment::UINT32: {
                        uint32_t val = system_json[json_obj_name][json_key_name].get<uint32_t>();
                        ::setenv(env_var, std::to_string(val).c_str(), 1);
                        break;
                    }
                    case environment::UINT64: {
                        uint64_t val = system_json[json_obj_name][json_key_name].get<uint64_t>();
                        ::setenv(env_var, std::to_string(val).c_str(), 1);
                        break;
                    }

                    case environment::BOOL: {
                        bool val = system_json[json_obj_name][json_key_name].get<bool>();
                        ::setenv(env_var, val ? "1" : "0", 1);
                        break;
                    }
                }
            }
        }
    }

    void
    Properties::init(bool load_redis)
    {
        // check for an override properties file;
        // if it exists use it rather than reading the config from redis
        const char *file = std::getenv(environment::PROPERTIES_FILE_OVERRIDE);
        if (file != nullptr) {
            LOG_INFO("Properties override file: {}", file);

            const char *load_override = std::getenv(environment::LOAD_OVERRIDE);
            if (load_override != nullptr) {
                load_redis = common::to_bool(load_override);
            }

            if (load_redis) {
                // read the system properties from the configuration file
                _load_redis(file);
            } else {
                // otherwise set the environment variables from the file
                set_env_from_file(file);
            }
        }

        if (!load_redis || file == nullptr) {
            LOG_INFO("Loading properties from environment/redis");
            // read the base config from the environment
            _read_environment();

            // read the system properties from redis
            _read_redis_properties();
        }

        // check for overrides in the environment variables
        // Note: overrides will only apply to config in _json
        const char *var = std::getenv(environment::ENV_OVERRIDE);

        if (var != nullptr) {
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
                        CHECK_NE(key.size(), 0);
                        val = props.substr(start, token - start);

                        // set the overridden value
                        // note: all override values are stored as strings
                        (*item)[key] = val;
                    } else if (props[token] == '=') {
                        key = props.substr(start, token - start);
                        next_token = ";";
                    } else {
                        CHECK_EQ(props[token], '.');
                        key = props.substr(start, token - start);
                        item = &((*item)[key]);
                    }

                    start = token + 1;
                } while (val.empty() && start < props.size());

                // set the position to the start of the next entry
                pos = start;
            }
        }
    }

    std::map<uint64_t, std::string>
    Properties::_get_databases()
    {
        nlohmann::json db_ids = _cache->get_value(DATABASE_IDS_PATH);
        if (db_ids.empty()) {
            throw RedisNotFoundError("Error missing database_ids in redis");
        }

        std::map<uint64_t, std::string> dbnames;

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
            nlohmann::json db_config = _get_db_config(db_id);
            CHECK(db_config.contains("name"));
            dbnames[db_id] = db_config["name"];
        }

        return dbnames;
    }

    std::vector<uint64_t>
    Properties::get_database_ids(const nlohmann::json &json_db_ids)
    {
        std::vector<uint64_t> db_ids;
        for (auto &json_db_id: json_db_ids) {
            uint64_t db_id;
            if (json_db_id.is_string()) {
                std::string db_id_str = json_db_id.get<std::string>();
                db_id = std::stoull(db_id_str);
            } else if (json_db_id.is_number()) {
                db_id = json_db_id.get<uint64_t>();
            } else {
                throw Error("Error invalid db_id type");
            }
            db_ids.push_back(db_id);
        }
        return db_ids;
    }

    std::vector<std::string>
    Properties::get_include_schemas(uint64_t db_id)
    {
        std::vector<std::string> include_schemas;

        auto db_config = Properties::get_db_config(db_id);
        auto include_json = db_config["include"];
        if (include_json.contains("schemas") && include_json["schemas"].is_array()) {
            include_schemas = include_json["schemas"];
        }
        if (std::ranges::find(include_schemas, "*") != include_schemas.end()) {
            // empty means include all schemas
            return {};
        }
        return include_schemas;
    }

    nlohmann::json
    Properties::_get_db_config(uint64_t db_id)
    {
        nlohmann::json db_config = _cache->get_value("db_config/" + std::to_string(db_id));
        if (db_config.empty()) {
            throw RedisNotFoundError("Error missing db_config_id in redis");
        }
        return db_config;
    }

    std::string
    Properties::_get_db_state(uint64_t db_id)
    {
        nlohmann::json db_state = _cache->get_value("instance_state/" + std::to_string(db_id));
        if (db_state.empty()) {
            throw RedisNotFoundError("Error missing db_state in redis");
        }
        CHECK(db_state.type() == nlohmann::json::value_t::string);
        return db_state.get<std::string>();
    }

    void
    Properties::_set_db_state(uint64_t db_id, const std::string &state)
    {
        nlohmann::json json_state(state);
        _cache->set_value("instance_state/" + std::to_string(db_id), json_state);
    }

    void
    Properties::_publish_liveness_notification(const std::string &msg)
    {
        _cache->publish(redis::PUBSUB_LIVENESS_NOTIFY, msg);
    }

    std::string
    Properties::_get_db_name(uint64_t db_id)
    {
        nlohmann::json db_config = _get_db_config(db_id);
        CHECK(db_config.contains("name"));

        return db_config["name"];
    }

    uint64_t
    Properties::_get_db_id(const std::string &db_name)
    {
        std::map<uint64_t, std::string> dbs = _get_databases();
        for (auto &[dbs_id, dbs_name]: dbs) {
            if (dbs_name == db_name) {
                return dbs_id;
            }
        }
        CHECK(false) << "Could not find database name " << db_name;
    }

    std::vector<std::string>
    Properties::_get_fdw_ids()
    {
        nlohmann::json fdw_ids_json = _cache->get_value("fdw_ids");
        if (fdw_ids_json.empty()) {
            throw RedisNotFoundError("Error missing fdw_ids in redis");
        }
        CHECK(fdw_ids_json.type() == nlohmann::json::value_t::array);
        std::vector<std::string> fdw_ids;
        for (auto fdw_id_json: fdw_ids_json) {
            CHECK(fdw_id_json.type() == nlohmann::json::value_t::string);
            fdw_ids.push_back(fdw_id_json.get<std::string>());
        }
        return fdw_ids;
    }

    nlohmann::json
    Properties::_get_primary_db_config()
    {
        nlohmann::json primary_db_config = _cache->get_value("instance_config/primary_db");
        if (primary_db_config.empty()) {
            throw RedisNotFoundError("Error missing db_instance_id in redis");
        }

        // in production, moving away from replication_user creds in redis/env
        if (!primary_db_config.contains("replication_user")) {
            // pull from aws secrets mgr, this updates _json[ORG_CONFIG]
            // with replication_user and replication_user_password
            _set_replication_user_from_aws();
        }

        // get the org config and see if there is a replication_user_password
        nlohmann::json org = _json[ORG_CONFIG];
        if (org.contains("replication_user_password")) {
            std::string replication_user_password;
            Json::get_to<std::string>(org, "replication_user_password", replication_user_password);
            primary_db_config["password"] = replication_user_password;
        }

        if (org.contains("replication_user")) {
            std::string replication_user;
            Json::get_to<std::string>(org, "replication_user", replication_user);
            primary_db_config["replication_user"] = replication_user;
        }

        return primary_db_config;
    }

    void
    Properties::_get_primary_db_config(std::string &host, int &port, std::string &user, std::string &password)
    {
        auto primary_config = _get_primary_db_config();

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
            LOG_ERROR("Could not find the value for primary database either host, port, user, or password");
            throw Error();
        }
    }

    nlohmann::json
    Properties::_get_fdw_config(const std::string &fdw_id)
    {
        nlohmann::json fdw_config = _cache->get_value("fdw/" + fdw_id);
        if (fdw_config.empty()) {
            throw RedisNotFoundError("Error missing fdw_config in redis");
        }

        // get the org config and see if there is a fdw_user_password
        nlohmann::json org = _json[ORG_CONFIG];

        std::string fdw_user_password;
        if (org.contains("fdw_user_password")) {
            Json::get_to<std::string>(org, "fdw_user_password", fdw_user_password);
        }

        // set the fdw password if it exists
        if (!fdw_user_password.empty()) {
            fdw_config["password"] = fdw_user_password;
        }
        return fdw_config;
    }

    std::string
    Properties::_get_ingestion_hostname()
    {
        nlohmann::json json_hostname = _cache->get_value("instance_config/hostname:ingestion");
        if (json_hostname.empty()) {
            throw RedisNotFoundError("Error missing hostname::ingestion in redis");
        }
        CHECK(json_hostname.type() == nlohmann::json::value_t::string);
        return json_hostname.get<std::string>();
    }

    std::string
    Properties::get_pid_path()
    {
        nlohmann::json props = _json[Properties::LOGGING_CONFIG];
        std::string pid_path = Json::get_or<std::string>(props, Properties::PID_PATH, "/var/springtail/pids");
        return pid_path;
    }

    void
    Properties::_set_replication_user_from_aws()
    {
        uint64_t org_id;
        uint64_t account_id;
        uint64_t db_instance_id;

        Json::get_to<uint64_t>(_json[ORG_CONFIG], "organization_id", org_id);
        Json::get_to<uint64_t>(_json[ORG_CONFIG], "account_id", account_id);
        Json::get_to<uint64_t>(_json[ORG_CONFIG], "db_instance_id", db_instance_id);

        std::string key = fmt::format(AwsHelper::DB_USERS_SECRET, org_id, account_id, db_instance_id);

        if (_aws_helper == nullptr) {
            _aws_helper = std::make_shared<AwsHelper>();
        }

        nlohmann::json secret = _aws_helper->get_secret(key);
        CHECK(secret.is_array());

        // iterate through json and find role="replication"
        for (auto &user: secret) {
            CHECK(user.is_object());
            CHECK(user.contains("role"));
            if (user["role"] == "replication") {
                _json[ORG_CONFIG]["replication_user"] = user["username"];
                _json[ORG_CONFIG]["replication_user_password"] = user["password"];
                break;
            }
        }
    }

}
