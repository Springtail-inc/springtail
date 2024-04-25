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
}
