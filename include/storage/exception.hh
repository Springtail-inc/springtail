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

    /** Parent of storage errors. */
    class StorageError : public st_common::Error {

    };

    class OpenFileStorageError : public StorageError {
        const char *what() {
            return "Unable to open file.";
        }
    };

    class CreateFileStorageError : public StorageError {
        const char *what() {
            return "Unable to create file.";
        }
    };

    class ReadFileStorageError : public StorageError {
        const char *what() {
            return "Error to reading file.";
        }
    };

    class ValidationStorageError : public StorageError {
        const char *what() {
            return "Validation error reading file."
        }
    };
}
