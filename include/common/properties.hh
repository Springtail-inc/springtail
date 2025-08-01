#pragma once

#include <cassert>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <common/aws.hh>
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

        /** Redis notification path for database ids */
        static inline constexpr char DATABASE_IDS_PATH[] = "instance_config/database_ids";
        /** Redis notification path for database states */
        static inline constexpr char DATABASE_STATE_PATH[] = "instance_state";

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

        /**
         * @brief Function for separate redis cache initialization
         */
        void init_cache() { _cache = std::make_shared<RedisCache>(true); }

        /** Helper to get db instance id */
        static inline uint64_t get_db_instance_id() {
            _assert_instance();
            return  get_instance()->_get_db_instance_id();
        }

        /** Helper to get organization id */
        static inline std::string get_organization_id() {
            _assert_instance();
            return get_instance()->_get_organization_id();
        }

        /** Helper to get account id */
        static inline std::string get_account_id() {
            _assert_instance();
            return get_instance()->_get_account_id();
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

        /** Helper to get fdw ids */
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

        /** Helper to get database id from Redis for db name */
        static inline uint64_t get_db_id(const std::string &db_name) {
            return get_instance()->_get_db_id(db_name);
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

        /**
         * @brief Get access to redis cache in case the user need to make
         *          the changes to config database
         *
         * @return std::shared_ptr<RedisCache> - RedisCache shared pointer
         */
        std::shared_ptr<RedisCache> get_cache() { return _cache; }

        /**
         * @brief Utility function to get database ids vector from passed json object
         *
         * @param json_db_ids - json object with an array of database ids
         * @return std::vector<uint64_t> - vector with database ids
         */
        static std::vector<uint64_t> get_database_ids(const nlohmann::json &json_db_ids);

    private:
        /** json containing parsed settings file */
        nlohmann::json _json;

        /** RedisCache object */
        std::shared_ptr<RedisCache> _cache;

        /** AWS for secrets mgr */
        std::shared_ptr<AwsHelper> _aws_helper;

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
          *
          * @return RedisClientPtr - shared pointer to redis client
          */
        RedisClientPtr _create_redis_client();

        /**
         * @brief Load redis config from file
         * @param config_file path to config file
         */
        void _load_redis(const std::string &config_file);

        /**
         * @brief Set the replication user variables from AWS secrets manager
         */
        void _set_replication_user_from_aws();

        /**
         * @brief Internal get database instance id
         *
         * @return uint64_t
         */
        uint64_t _get_db_instance_id() {
            assert (_json.contains(ORG_CONFIG));
            assert (_json[ORG_CONFIG].contains("db_instance_id"));
            return _json[ORG_CONFIG]["db_instance_id"];
        }

        /**
         * @brief Internal get organization id
         *
         * @return std::string
         */
        std::string _get_organization_id() {
            assert (_json.contains(ORG_CONFIG));
            assert (_json[ORG_CONFIG].contains("organization_id"));
            return _json[ORG_CONFIG]["organization_id"];
        }

        /**
         * @brief Internal get account id
         *
         * @return std::string
         */
        std::string _get_account_id() {
            assert (_json.contains(ORG_CONFIG));
            assert (_json[ORG_CONFIG].contains("account_id"));
            return _json[ORG_CONFIG]["account_id"];
        }

        /**
         * @brief Internal get mount point
         *
         * @return std::string
         */
        std::string _get_mount_point() {
            assert (_json.contains(FS_CONFIG));
            assert (_json[FS_CONFIG].contains("mount_point"));
            return _json[FS_CONFIG]["mount_point"];
        }

        /**
         * @brief Get absolute path for given file path
         *
         * @param path - file path
         * @return std::filesystem::path
         */
        std::filesystem::path
        _make_absolute_path(const std::string &path) {
            assert (!_get_mount_point().empty());
            return std::filesystem::path(_get_mount_point()) / path;
        }

        /**
         * @brief Internal get fdw id
         *
         * @return std::string
         */
        std::string _get_fdw_id() {
            assert (_json.contains(ORG_CONFIG));
            assert (_json[ORG_CONFIG].contains("fdw_id"));
            return _json[ORG_CONFIG]["fdw_id"];
        }

        /**
         * @brief Internal get databases
         *
         * @return std::map<uint64_t, std::string>
         */
        std::map<uint64_t, std::string> _get_databases();

        /**
         * @brief Internal get database name for a given database
         *
         * @param db_id - database id
         * @return std::string
         */
        std::string _get_db_name(uint64_t db_id);

        /**
         * @brief Internal get database id for a given database name
         *
         * @param db_name- database name
         * @return uint64_t
         */
        uint64_t _get_db_id(const std::string &db_name);

         /**
         * @brief Internal get fdw ids
         *
         * @return std::vector<std::string>
         */
        std::vector<std::string> _get_fdw_ids();

        /**
         * @brief Internal get database config for the given database id
         *
         * @param db_id - database id
         * @return nlohmann::json
         */
        nlohmann::json _get_db_config(uint64_t db_id);

        /**
         * @brief Internal get primary database config and store them in input arguments
         *
         * @param host - host name
         * @param port - port value
         * @param user - user name
         * @param password - user password
         */
        void _get_primary_db_config(std::string &host, int &port, std::string &user, std::string &password);

        /**
         * @brief Internal get database state for given database id
         *
         * @param db_id - database id
         * @return std::string - state string
         */
        std::string _get_db_state(uint64_t db_id);

        /**
         * @brief Internal set database state for given database id
         *
         * @param db_id - database id
         * @param state - database state
         */
        void _set_db_state(uint64_t db_id, const std::string &state);

        /**
         * @brief Internal get fdw config for given fdw id
         *
         * @param fdw_id - fdw id
         * @return nlohmann::json - fdw config
         */
        nlohmann::json _get_fdw_config(const std::string &fdw_id);

        /**
         * @brief Internal publish liveness notification message
         *
         * @param msg - message tp publish
         */
        void _publish_liveness_notification(const std::string &msg);

        /**
         * @brief Get the hostname for the ingestion instance machine
         */
        std::string _get_ingestion_hostname();

        /** Helper to get primary db json for current db instance */
        nlohmann::json _get_primary_db_config();
    };
}
