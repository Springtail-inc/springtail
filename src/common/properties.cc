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

        // create connection options for config db
        sw::redis::ConnectionOptions connect_options;
        connect_options.host = hostname;
        connect_options.port = port;
        connect_options.user = user;
        connect_options.password = password;
        connect_options.db = 0; // config DB hard-coded to 0

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
        std::string db_instance_key = redis::SYSTEM_PREFIX + std::to_string(db_instance_id);
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

    Properties::Properties()
    {
        // read the base config from the environment
        _read_environment();

        // check for an override properties file;
        // if it exists use it rather than reading the config from redis
        const char *file = std::getenv(environment::PROPERTIES_FILE_OVERRIDE);

        if (file != nullptr) {
            std::cout << "Properties override file: " << file << std::endl;
        }
        if (file && std::filesystem::exists(std::filesystem::path(file))) {
            // read the system properties from the configuration file
            std::ifstream ifs(file);
            _json.merge_patch(nlohmann::json::parse(ifs));
            _properties_file_override = true;
        } else {
            // read the system properties from redis
            _read_redis_properties();
        }

        // check for overrides in the environment variables
        const char *var = std::getenv(environment::ENV_OVERRIDE);
        if (var == nullptr) {
            return; // no overrides, exit
        }

        // note: overrides are in the form <key>=<val>;<key2>=<val2> where keys are a dot-separated
        //       path within the json configuration
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
        // see if we are using the properties file override
        if (_instance != nullptr && _instance->_properties_file_override) {
            db_ids = _instance->_json["db_instances"][std::to_string(db_instance_id)]["database_ids"];
        } else {
            // otherwise, we are using redis
            RedisClientPtr redis_client = _get_redis_client();
            std::optional<std::string> db_id_str = redis_client->hget(db_instance_key, "database_ids");
            if (!db_id_str.has_value()) {
                throw Error("Error missing database_ids in redis");
            }

            // convert to json
            db_ids = nlohmann::json::parse(db_id_str.value());
        }

        // iterate through the db_ids and get the db_config_id
        for (auto &db_id_json: db_ids) {
            // get db config based on db_id
            uint64_t db_id = db_id_json.get<uint64_t>();
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

        // see if we are using the properties file override
        if (_instance != nullptr && _instance->_properties_file_override) {
            return _instance->_json["databases"][std::to_string(db_id)];
        }

        // otherwise, we are using redis
        RedisClientPtr redis_client = _get_redis_client();

        // get the redis client and lookup the db ids from the db_instance config
        std::string db_config_key = std::format(redis::DB_CONFIG, db_instance_id, db_id);
        std::optional<std::string> db_config_str = redis_client->get(db_config_key);
        if (!db_config_str.has_value()) {
            throw Error("Error missing db_config_id in redis");
        }

        // convert to json
        return nlohmann::json::parse(db_config_str.value());
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

        // see if we are using the properties file override
        if (_instance != nullptr && _instance->_properties_file_override) {
            std::string fdw_id;
            Json::get_to<std::string>(_instance->_json[ORG_CONFIG], "fdw_id", fdw_id);
            return {fdw_id};
        }

        // get the redis client and lookup the db ids from the db_instance config
        RedisClientPtr redis_client = _get_redis_client();
        std::string fdw_key = std::format(redis::HASH_FDW, db_instance_id);

        std::vector<std::string> fdw_ids;
        redis_client->hkeys(fdw_key, std::back_inserter(fdw_ids));

        return fdw_ids;
    }

    nlohmann::json
    Properties::get_primary_db_config()
    {
        // get the db_instance_id (initially set from env or system.json)
        uint64_t db_instance_id = get_db_instance_id();

        // see if we are using the properties file override
        if (_instance != nullptr && _instance->_properties_file_override) {
            return _instance->_json["db_instances"][std::to_string(db_instance_id)]["primary_db"];
        }

        // otherwise, we are using redis
        RedisClientPtr redis_client = _get_redis_client();

        // get the redis client and lookup the db ids from the db_instance config
        std::string db_instance_key = std::format(redis::DB_INSTANCE_CONFIG, db_instance_id);
        std::optional<std::string> db_instance_str = redis_client->hget(db_instance_key, "primary_db");
        if (!db_instance_str.has_value()) {
            throw Error("Error missing db_instance_id in redis");
        }

        // convert to json
        return nlohmann::json::parse(db_instance_str.value());
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

            std::vector<std::string> fdw_ids;
            redis_client->hgetall(fdw_key, std::back_inserter(fdw_ids));
            for (auto &fdw : fdw_ids) {
                SPDLOG_WARN("Found FDW: {}", fdw);
            }

            throw Error("Error missing fdw_config in redis");
        }

        // convert to json
        return nlohmann::json::parse(fdw_config_str.value());
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
