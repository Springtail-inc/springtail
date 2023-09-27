#pragma once

#include <cstdint>

namespace st_storage {

    /** The available types for fields. */
    enum class SchemaType : uint8_t {
        ARRAY = 1,
        TEXT = 3,
        UINT64 = 4,
        INT64 = 5,
        UINT32 = 6,
        INT32 = 7,
        UINT16 = 8,
        INT16 = 9,
        UINT8 = 10,
        INT8 = 11,
        BOOLEAN = 12,
        DECIMAL128 = 13,
        FLOAT64 = 14,
        FLOAT32 = 15,
        BINARY = 16
    };

}
