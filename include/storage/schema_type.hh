#pragma once

#include <cstdint>
#include <map>

namespace springtail {

    /** The available types for fields. */
    enum class SchemaType : uint8_t {
        TEXT = 1,
        UINT64,
        INT64,
        UINT32,
        INT32,
        UINT16,
        INT16,
        UINT8,
        INT8,
        BOOLEAN,
        FLOAT64,
        FLOAT32,
        DECIMAL128,
        BINARY,
        TIME,
        DATE,
        TIMESTAMP
    };

    inline SchemaType strToSchemaType(std::string string) {
        const std::map<std::string, SchemaType> map = {
            {"TEXT", SchemaType::TEXT},
            {"UINT64", SchemaType::UINT64},
            {"INT64", SchemaType::INT64},
            {"UINT32", SchemaType::UINT32},
            {"INT32", SchemaType::INT32},
            {"UINT16", SchemaType::UINT16},
            {"INT16", SchemaType::INT16},
            {"UINT8", SchemaType::UINT8},
            {"INT8", SchemaType::INT8},
            {"BOOLEAN", SchemaType::BOOLEAN},
            {"FLOAT64", SchemaType::FLOAT64},
            {"FLOAT32", SchemaType::FLOAT32},
            {"DECIMAL128", SchemaType::DECIMAL128},
            {"BINARY", SchemaType::BINARY},
            {"TIME", SchemaType::TIME},
            {"DATE", SchemaType::DATE},
            {"TIMESTAMP", SchemaType::TIMESTAMP}
        };
        return map.at(string);
    }
}
