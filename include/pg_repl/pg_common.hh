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

    /**
     * @brief Populates the redis cache with the information about invalid tables. This is used to
     *        validate the invalid tables in the systems which are skipped for replication and also
     *        can be exposed to the users to understand why certain tables aren't replicated
     *
     * @param table_oid The table oid which has the invalid columns
     * @param table_info JSON field containing meta info about the invalid columns
     */
    void populate_invalid_tables_in_redis(uint64_t table_oid,
                                           const nlohmann::json &table_info);

    /**
     * @brief Check if the table has any invalid columns
     *
     * @param table_oid The table oid which has the invalid columns
     * @return true/false based on whether the table is invalid
     */
    bool check_if_table_is_invalid_in_redis(uint64_t table_oid);

    /**
     * @brief Clears the table in Redis, making it valid again
     *
     * @param table_oid The table oid which has the invalid columns
     */
    void clear_invalid_table_in_redis(uint64_t table_oid);
}
