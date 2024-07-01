#pragma once

#include <cstdint>
#include <map>

namespace springtail {

    /** The available types for fields. */
    enum class SchemaType : uint8_t {
        TEXT = 1, // bool, bytea
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
}
