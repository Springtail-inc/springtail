#pragma once

#include <ostream>
#include <pg_repl/pg_common.hh>

namespace springtail {

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

    inline std::string to_string(SchemaType type) {
        switch (type) {
            case SchemaType::TEXT: return "TEXT";
            case SchemaType::UINT64: return "UINT64";
            case SchemaType::INT64: return "INT64";
            case SchemaType::UINT32: return "UINT32";
            case SchemaType::INT32: return "INT32";
            case SchemaType::UINT16: return "UINT16";
            case SchemaType::INT16: return "INT16";
            case SchemaType::UINT8: return "UINT8";
            case SchemaType::INT8: return "INT8";
            case SchemaType::BOOLEAN: return "BOOLEAN";
            case SchemaType::FLOAT64: return "FLOAT64";
            case SchemaType::FLOAT32: return "FLOAT32";
            case SchemaType::BINARY: return "BINARY";
            default: return "UNKNOWN";
        }
    }
}
