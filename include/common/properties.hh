#pragma once
#include <nlohmann/json.hpp>

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

        /** Helper to get set of database names from Redis for this db instance */
        static std::vector<std::string> get_database_names();

    private:
        /** static _instance singleton */
        static Properties *_instance;

        /** json containing parsed settings file */
        nlohmann::json _json;

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

        // delete move constructor
        Properties(const Properties &)     = delete;
        void operator=(const Properties &) = delete;
    };
}
