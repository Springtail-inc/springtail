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

        /** Default properties file, defined by make -D option, or macro above */
        static inline constexpr char SPRINGTAIL_PROPERTIES_FILE[] = SPRINGTAIL_PROPERTIES;

        /**
         * @brief Get JSON object from a key
         * @param key key to lookup
         * @return nlohmann::json resulting json object
         */
        static nlohmann::json get(const std::string &key);

        /**
         * @brief Init _instance and read in properties file
         * @param file file containing json properties
         */
        static void init(const std::string &file);

    private:
        /** static _instance singleton */
        static Properties *_instance;

        /** json containing parsed settings file */
        nlohmann::json _json;

        /**
         * @brief Construct a new Properties object
         * @param file string pointing to system.json file
         */
        Properties(const std::string &file);

        // delete move constructor
        Properties(const Properties &)     = delete;
        void operator=(const Properties &) = delete;
    };
}
