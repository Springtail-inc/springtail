#pragma once

#include <fmt/core.h>
#include <nlohmann/json.hpp>

#include <common/properties.hh>
#include <common/redis_types.hh>

namespace springtail {
    /**
     * Helper Redis class to add schemas and tables list per db
     * Used by the proxy to validate which tables are being replicated
     * Populated by the GC when the ddl changes are committed
     */
    class RedisDbTables {
    public:
        /**
         * @brief Add a schema, table pair to the db tables set
         * @param db_id db id
         * @param table_name table name
         * @param schema_name schema name
         */
        static void add_table(sw::redis::Transaction &ts, uint64_t db_id, const std::string &table_name, const std::string &schema_name)
        {
            std::string key = fmt::format(redis::SET_DB_TABLES, Properties::get_db_instance_id(), db_id);

            // escape the strings and add to set
            std::string value = fmt::format("{}:{}", _escape_string(table_name), _escape_string(schema_name));
            ts.sadd(key, value);
        }

        /**
         * @brief  Remove a schema, table pair from the redis db tables set
         * @param db_id db id
         * @param table_name table name
         * @param schema_name schema name
         */
        static void remove_table(sw::redis::Transaction &ts, uint64_t db_id, const std::string &table_name, const std::string &schema_name)
        {
            std::string key = fmt::format(redis::SET_DB_TABLES, Properties::get_db_instance_id(), db_id);

            // escape the strings and add to set
            std::string value = fmt::format("{}:{}", _escape_string(table_name), _escape_string(schema_name));
            ts.srem(key, value);
        }

        /**
         * @brief Check if a schema, table pair exists in the db tables set
         * @param db_id db id
         * @param table_name table name
         * @param schema_name schema name
         * @return true if exists; false otherwise
         */
        static bool lookup_table(uint64_t db_id, const std::string &table_name, const std::string &schema_name)
        {
            std::string key = fmt::format(redis::SET_DB_TABLES, Properties::get_db_instance_id(), db_id);

            // escape the strings and add to set
            std::string value = fmt::format("{}:{}", _escape_string(table_name), _escape_string(schema_name));
            return RedisMgr::get_instance()->get_client()->sismember(key, value);
        }

    private:
        // private constructor
        RedisDbTables() = delete;

        /**
         * @brief Simple escape a string for json
         * @param str string to escape
         * @return escaped string
         */
        static std::string _escape_string(const std::string &str)
        {
            nlohmann::json j = str;
            return j.dump();
        }
    };
}