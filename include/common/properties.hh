#pragma once

#include <cassert>
#include <filesystem>
#include <map>
#include <string>
#include <vector>
#include <iostream>

#include <nlohmann/json.hpp>

#include <common/redis.hh>

#ifndef SPRINGTAIL_PROPERTIES
#define SPRINGTAIL_PROPERTIES = "system.json"
#endif

namespace springtail {

    /**
     * @brief Properties singleton object, initialized in springtail_init()
     */
    class Properties {
    public:
        /** Redis config section in properties file */
        static inline constexpr char REDIS_CONFIG[] = "redis";
        /** IOPool config section in properties file */
        static inline constexpr char IOPOOL_CONFIG[] = "iopool";
        /** Write cache config */
        static inline constexpr char WRITE_CACHE_CONFIG[] = "write_cache";
        /** XID mgr config */
        static inline constexpr char XID_MGR_CONFIG[] = "xid_mgr";
        /** Storage config section */
        static inline constexpr char STORAGE_CONFIG[] = "storage";
        /** Logging config section */
        static inline constexpr char LOGGING_CONFIG[] = "logging";
        /** SysTbl mgr section */
        static inline constexpr char SYS_TBL_MGR_CONFIG[] = "sys_tbl_mgr";
        /** Log mgr section */
        static inline constexpr char LOG_MGR_CONFIG[] = "log_mgr";
        /** Org configuration section */
        static inline constexpr char ORG_CONFIG[] = "org";
        /** FS configuration section */
        static inline constexpr char FS_CONFIG[] = "fs";

        /**
         * @brief Get JSON object from a key
         * @param key key to lookup
         * @return nlohmann::json resulting json object
         */
        static nlohmann::json get(const std::string &key);

        /**
         * @brief Init _instance and read from redis
         */
        static void init();

        /** Helper to get db instance id */
        static uint64_t get_db_instance_id() {
            assert (_instance != nullptr);
            assert (_instance->_json.contains(ORG_CONFIG));
            assert (_instance->_json[ORG_CONFIG].contains("db_instance_id"));
            return _instance->_json[ORG_CONFIG]["db_instance_id"];
        }

        /** Helper to get fs mount point */
        static std::string get_mount_point() {
            assert (_instance != nullptr);
            assert (_instance->_json.contains(FS_CONFIG));
            assert (_instance->_json[FS_CONFIG].contains("mount_point"));
            return _instance->_json[FS_CONFIG]["mount_point"];
        }

        /** Helper to convert relative path to absolute based on mount point */
        static std::filesystem::path
        make_absolute_path(const std::string &path) {
            assert (_instance != nullptr);
            assert (!get_mount_point().empty());
            return std::filesystem::path(get_mount_point()) / path;
        }

        static std::string get_fdw_id() {
            assert (_instance != nullptr);
            assert (_instance->_json.contains(ORG_CONFIG));
            assert (_instance->_json[ORG_CONFIG].contains("fdw_id"));
            return _instance->_json[ORG_CONFIG]["fdw_id"];
        }

        /** Helper to get set of database names from Redis for this db instance */
        static std::map<uint64_t, std::string> get_databases();

        /** Helper to get database name from Redis for db id */
        static std::string get_db_name(uint64_t db_id);

        /** Helper to get set of FDW ids from Redis */
        static std::vector<std::string> get_fdw_ids();

        /** Helper to get db config for given database */
        static nlohmann::json get_db_config(uint64_t db_id);

        /** Helper to get primary db json for current db instance */
        static nlohmann::json get_primary_db_config();

        /** Helper to get db state */
        static std::string get_db_state(uint64_t db_id);

        /** Helper to set db state */
        static void set_db_state(uint64_t db_id, const std::string &state);

        /** Helper to get fdw config */
        static nlohmann::json get_fdw_config(const std::string &fdw_id);

    private:
        /** static _instance singleton */
        static Properties *_instance;

        /** once init flag */
        static std::once_flag _init_flag;

        /** json containing parsed settings file */
        nlohmann::json _json;

        /** properties file override env is set */
        bool _properties_file_override = false;

        /**
         * @brief Construct a new Properties object
         */
        Properties();

        /**
         * @brief Read the environment variables into base config
         */
        void _read_environment();

        /**
         * @brief Read system properties from redis
         */
        void _read_redis_properties();

        /**
         * @brief Create redis client from config for config db
         */
        void _create_redis_client();

        /**
         * @brief Get config redis client
         */
        static RedisClientPtr _get_redis_client() {
            assert(_instance != nullptr);
            if (_instance->_redis_config_client == nullptr) {
                _instance->_create_redis_client();
            }
            assert(_instance->_redis_config_client != nullptr);
            return _instance->_redis_config_client;
        }

        // delete move constructor
        Properties(const Properties &)     = delete;
        void operator=(const Properties &) = delete;

        /** Redis client connected to config db */
        RedisClientPtr _redis_config_client = nullptr;
    };
}
