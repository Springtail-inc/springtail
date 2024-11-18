#pragma once

#include <fmt/core.h>
#include <nlohmann/json.hpp>

#include <common/common.hh>
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
            std::string schema = common::escape_quoted_string(schema_name);
            std::string table = common::escape_quoted_string(table_name);
            std::string value = fmt::format("{}:{}", schema, table);
            ts.sadd(key, value);
            ts.publish(fmt::format(redis::PUBSUB_DB_TABLE_CHANGES, Properties::get_db_instance_id()), fmt::format("{}:add:{}:{}", db_id, schema_name, table_name));
        }

        /**
         * @brief  Remove a schema, table pair from the redis db tables set
         * @param db_id db id
         * @param table_name table name
         * @param schema_name schema name
         */
        static void remove_table(sw::redis::Transaction &ts, uint64_t db_id, const std::string &table_name, const std::string &schema_name)
        {
            uint64_t instance_id = Properties::get_db_instance_id();
            std::string key = fmt::format(redis::SET_DB_TABLES, Properties::get_db_instance_id(), db_id);
            std::string schema = common::escape_quoted_string(schema_name);
            std::string table = common::escape_quoted_string(table_name);
            std::string value = fmt::format("{}:{}", schema, table);
            ts.srem(key, value);
            ts.publish(fmt::format(redis::PUBSUB_DB_TABLE_CHANGES, instance_id), fmt::format("{}:remove:{}:{}", db_id, schema_name, table_name));
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
            uint64_t instance_id = Properties::get_db_instance_id();
            std::string key = fmt::format(redis::SET_DB_TABLES, instance_id, db_id);
            std::string value = fmt::format("{}:{}", common::escape_quoted_string(schema_name), common::escape_quoted_string(table_name));
            return RedisMgr::get_instance()->get_client()->sismember(key, value);
        }

        /**
         * @brief Decode a pubsub message into db_id, action, schema, table
         * @param msg pubsub message
         * @param db_id db id (output)
         * @param action action, one of add, remove (output)
         * @param schema schema name (output)
         * @param table table name (output)
         */
        static void decode_pubsub_msg(const std::string &msg, uint64_t &db_id, std::string &action, std::string &schema, std::string &table)
        {
            std::vector<std::string> parts;
            common::split_quoted_string(':', msg, parts);
            db_id = stoull(parts[0]);
            action = parts[1];
            schema = common::unescape_quoted_string(parts[2]);
            table = common::unescape_quoted_string(parts[3]);
        }

        /**
         * @brief Get the list of known database schema and table pairs
         *
         * @param instance_id - database instance id
         * @param db_id - database id
         * @param out - output schema:table values
         */
        static void get_tables(const uint64_t instance_id, const uint64_t db_id, std::vector<std::pair<std::string, std::string>> &out)
        {
            std::string key = fmt::format(redis::SET_DB_TABLES, instance_id, db_id);
            std::vector<std::string> schema_table_pairs;
            RedisMgr::get_instance()->get_client()->smembers(key, std::back_inserter(schema_table_pairs));
            for (const auto &pair: schema_table_pairs) {
                std::vector<std::string> parts;
                common::split_quoted_string(':', pair, parts);
                out.push_back(std::pair(common::unescape_quoted_string(parts[0]), common::unescape_quoted_string(parts[1])));
            }
        }
    private:
        // private constructor
        RedisDbTables() = delete;
    };
}