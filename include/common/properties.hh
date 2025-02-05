#pragma once

#include <cassert>
#include <filesystem>
#include <map>
#include <string>
#include <vector>
#include <iostream>

#include <nlohmann/json.hpp>

#include <common/redis_cache.hh>
#include <common/singleton.hh>

#ifndef SPRINGTAIL_PROPERTIES
#define SPRINGTAIL_PROPERTIES = "system.json"
#endif

namespace springtail {

    /**
     * @brief Properties singleton object, initialized in springtail_init()
     */
    class Properties : public Singleton<Properties> {
        friend class Singleton<Properties>;
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
        /** PID path in logging config */
        static inline constexpr char PID_PATH[] = "pid_path";
        /** Proxy configuration section */
        static inline constexpr char PROXY_CONFIG[] = "proxy";
        /** Open telemetry configuration section */
        static inline constexpr char OTEL_CONFIG[] = "otel";

        /**
         * @brief Get JSON object from a key
         * @param key key to lookup
         * @return nlohmann::json resulting json object
         */
        static nlohmann::json get(const std::string &key);

        /**
         * @brief Init _instance and read from redis
         */
        void init(bool load_redis);

        /** Helper to get db instance id */
        static inline uint64_t get_db_instance_id() {
            _assert_instance();
            return  get_instance()->_get_db_instance_id();
        }

        /** Helper to get fs mount point */
        static inline std::string get_mount_point() {
            _assert_instance();
            return get_instance()->_get_mount_point();
        }

        /** Helper to convert relative path to absolute based on mount point */
        static inline std::filesystem::path
        make_absolute_path(const std::string &path) {
            _assert_instance();
            return get_instance()->_make_absolute_path(path);
        }

        static inline std::string get_fdw_id() {
            _assert_instance();
            return get_instance()->_get_fdw_id();
        }

        /** Helper to get set of database names from Redis for this db instance */
        static inline std::map<uint64_t, std::string> get_databases() {
            return get_instance()->_get_databases();
        }

        /** Helper to get database name from Redis for db id */
        static inline std::string get_db_name(uint64_t db_id) {
            return get_instance()->_get_db_name(db_id);
        }

        /** Helper to get set of FDW ids from Redis */
        static inline std::vector<std::string> get_fdw_ids() {
            return get_instance()->_get_fdw_ids();
        }

        /** Helper to get db config for given database */
        static inline nlohmann::json get_db_config(uint64_t db_id) {
            return get_instance()->_get_db_config(db_id);
        }

        /** Helper to get host, port, user, and password for the primary database */
        static inline void get_primary_db_config(std::string &host, int &port, std::string &user, std::string &password) {
            get_instance()->_get_primary_db_config(host, port, user, password);
        }

        /** Helper to get db state */
        static inline std::string get_db_state(uint64_t db_id) {
            return  get_instance()->_get_db_state(db_id);
        }

        /** Helper to set db state */
        static inline void set_db_state(uint64_t db_id, const std::string &state) {
            get_instance()->_set_db_state(db_id,state);
        }

        /** Helper to get fdw config */
        static inline nlohmann::json get_fdw_config(const std::string &fdw_id) {
            return get_instance()->_get_fdw_config(fdw_id);
        }

        /** Helper to get pid path */
        std::string get_pid_path();

        /** Helper to send notification on liveness pubsub */
        static inline void publish_liveness_notification(const std::string &msg) {
            get_instance()->_publish_liveness_notification(msg);
        }

        /** Helper to get xid mgr hostname */
        static inline std::string get_xid_mgr_hostname() { return get_instance()->_get_ingestion_hostname(); }

        /** Helper to get sys tbl mgr hostname */
        static inline std::string get_sys_tbl_mgr_hostname() { return get_instance()->_get_ingestion_hostname(); }

        /** Helper to get write cache hostname */
        static inline std::string get_write_cache_hostname() { return get_instance()->_get_ingestion_hostname(); }

        /** Helper to set env vars from config file */
        static void set_env_from_file(const char *config_file);

        std::shared_ptr<RedisCache> get_cache() { return _cache; }

    private:
        /** json containing parsed settings file */
        nlohmann::json _json;

        std::shared_ptr<RedisCache> _cache;

        /**
         * @brief Construct a new Properties object
         */
        Properties() = default;

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
         * @brief Load redis config from file
         * @param config_file path to config file
         */
        void _load_redis(const std::string &config_file);

        /**
         * @brief Get config redis client
         */
        RedisClientPtr _get_redis_client() {
            _assert_instance();
            if (_redis_config_client == nullptr) {
                _create_redis_client();
            }
            assert(_redis_config_client != nullptr);
            return _redis_config_client;
        }

        uint64_t _get_db_instance_id() {
            assert (_json.contains(ORG_CONFIG));
            assert (_json[ORG_CONFIG].contains("db_instance_id"));
            return _json[ORG_CONFIG]["db_instance_id"];
        }

        std::string _get_mount_point() {
            assert (_json.contains(FS_CONFIG));
            assert (_json[FS_CONFIG].contains("mount_point"));
            return _json[FS_CONFIG]["mount_point"];
        }

        std::filesystem::path
        _make_absolute_path(const std::string &path) {
            assert (!_get_mount_point().empty());
            return std::filesystem::path(_get_mount_point()) / path;
        }

        std::string _get_fdw_id() {
            assert (_json.contains(ORG_CONFIG));
            assert (_json[ORG_CONFIG].contains("fdw_id"));
            return _json[ORG_CONFIG]["fdw_id"];
        }

        std::map<uint64_t, std::string> _get_databases();

        std::string _get_db_name(uint64_t db_id);

        std::vector<std::string> _get_fdw_ids();

        nlohmann::json _get_db_config(uint64_t db_id);

        void _get_primary_db_config(std::string &host, int &port, std::string &user, std::string &password);

        std::string _get_db_state(uint64_t db_id);

        void _set_db_state(uint64_t db_id, const std::string &state);

        nlohmann::json _get_fdw_config(const std::string &fdw_id);

        void _publish_liveness_notification(const std::string &msg);

        /**
         * @brief Get the hostname for the ingestion instance machine
         */
        std::string _get_ingestion_hostname();

        /** Helper to get primary db json for current db instance */
        nlohmann::json _get_primary_db_config();

        /** Redis client connected to config db */
        RedisClientPtr _redis_config_client = nullptr;
    };
}
