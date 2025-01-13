#pragma once

#include <cstdint>
#include <map>

namespace springtail {

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

    inline std::ostream& operator<<(std::ostream& os, SchemaType type) {
        switch (type) {
            case SchemaType::TEXT: return os << "TEXT";
            case SchemaType::UINT64: return os << "UINT64";
            case SchemaType::INT64: return os << "INT64";
            case SchemaType::UINT32: return os << "UINT32";
            case SchemaType::INT32: return os << "INT32";
            case SchemaType::UINT16: return os << "UINT16";
            case SchemaType::INT16: return os << "INT16";
            case SchemaType::UINT8: return os << "UINT8";
            case SchemaType::INT8: return os << "INT8";
            case SchemaType::BOOLEAN: return os << "BOOLEAN";
            case SchemaType::FLOAT64: return os << "FLOAT64";
            case SchemaType::FLOAT32: return os << "FLOAT32";
            case SchemaType::BINARY: return os << "BINARY";
            default: return os << "UNKNOWN";
        }
    }
}
