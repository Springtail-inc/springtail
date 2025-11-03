#pragma once

#include <cstdint>

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
        NUMERIC, // numeric data type
        EXTENSION, // all extension types
        BINARY // all other types
    };

    /**
     * @brief Convert the postgres type system (also known as typeoid) to SchemaTypes.
     * @param pg_type postgres type such as BOOLOID
     * @param pg_type_category category of the postgres type
     */
    SchemaType convert_pg_type(int32_t pg_type, char pg_type_category);
}
