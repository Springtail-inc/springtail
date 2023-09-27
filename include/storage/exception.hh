#pragma once

#include <common/exception.hh>

namespace st_storage {
    /** Parent of schema exceptions. */
    class SchemaError : public st_common::Error {

    };

    class ColumnExistsSchemaError : public SchemaError {
        const char *what() {
            return "A column already exists with this name.";
        }
    };

    class ColumnNotExistsSchemaError : public SchemaError {
        const char *what() {
            return "The requested column does not exist.";
        }
    };
}
