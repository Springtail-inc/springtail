#pragma once

#include <cstdint>
#include <nlohmann/json.hpp>

#include <common/properties.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>

namespace springtail
{
    /** The available types for fields. */
    enum class SchemaType : uint8_t {
        TEXT = 1, // text, varchar, bpchar
        UINT64,
        INT64, // int8, money, time, timestamp, timestamptz, xid8
        UINT32,
        INT32, // int4, cid, date, xid
        UINT16,
        INT16, // int2
        UINT8,
        INT8, // char
        BOOLEAN, // bool
        FLOAT64, // float8
        FLOAT32, // float4
        BINARY // all other types
    };

    /**
     * @brief Convert the postgres type system (also known as typeoid) to SchemaTypes.
     * @param pg_type postgres type such as BOOLOID
     */
    SchemaType convert_pg_type(int32_t pg_type);

    class TableValidator {
        public:
        /**
        * @brief Populates the redis cache with the information about invalid tables. This is used to
        *        validate the invalid tables in the systems which are skipped for replication and also
        *        can be exposed to the users to understand why certain tables aren't replicated
        *
        * @param table_oid The table oid which has the invalid columns
        * @param table_info JSON field containing meta info about the invalid columns
        */
        static void populate_invalid_tables_in_redis(uint64_t table_oid,
                                              const nlohmann::json &table_info);

        /**
        * @brief Check if the table has any invalid columns
        *
        * @param table_oid The table oid which has the invalid columns
        * @return true/false based on whether the table is invalid
        */
        static bool check_if_table_is_invalid_in_redis(uint64_t table_oid);

        /**
        * @brief Clears the table in Redis, making it valid again
        *
        * @param table_oid The table oid which has the invalid columns
        */
        static void clear_invalid_table_in_redis(uint64_t table_oid);

        /**
        * @brief Validate the DDL operation and get the list of invalid columns
        *
        * @param namespace_name Namespace name
        * @param table_oid OID of the table
        * @param columns Vector of original columns as part of the DDL
        * @return JSON object containing the list of invalid columns with meta information
        */
        template <typename E>
        static nlohmann::json
        _validate_ddl_and_get_invalid_columns(const std::string &namespace_name,
                                              uint64_t table_oid,
                                              std::vector<E> columns)
        {
            auto invalid_columns = nlohmann::json::array();

            // Validate if the table has an invalid column
            for (const auto& column : columns) {
                if ( column.is_generated || column.is_non_standard_collation || !column.is_user_defined_type ){
                    invalid_columns.push_back({
                        {"name", column.name},
                        {"type_name", column.type_name},
                        {"collation", column.collation}
                    });
                    SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "VALIDATE_DDL: Invalid column: name={}, tid={}", column.name, table_oid);
                }
            }

            return invalid_columns;
        }
    };
}
