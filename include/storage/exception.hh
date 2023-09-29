#pragma once

#include <common/exception.hh>

namespace springtail {
    /** For type conversion and invalid type errors. */
    class TypeError : public Error {
    public:
        TypeError() { }
        TypeError(const std::string &error)
            : Error(error)
        { }
    };

    /** Parent of schema exceptions. */
    class SchemaError : public Error {

    };

    class ColumnExistsSchemaError : public SchemaError {
    public:
        const char *what() const noexcept {
            return "A column already exists with this name.";
        }
    };

    class ColumnNotExistsSchemaError : public SchemaError {
    public:
        const char *what() const noexcept {
            return "The requested column does not exist.";
        }
    };

    class BadTypeSchemaError : public SchemaError {
    public:
        const char *what() const noexcept {
            return "The requested column type is unsupported.";
        }
    };

    /** Parent of storage errors. */
    class StorageError : public Error {

    };

    class OpenFileStorageError : public StorageError {
    public:
        const char *what() const noexcept {
            return "Unable to open file.";
        }
    };

    class CreateFileStorageError : public StorageError {
    public:
        const char *what() const noexcept {
            return "Unable to create file.";
        }
    };

    class ReadFileStorageError : public StorageError {
    public:
        const char *what() const noexcept {
            return "Error to reading file.";
        }
    };

    class ValidationStorageError : public StorageError {
    public:
        const char *what() const noexcept {
            return "Validation error reading file.";
        }
    };

    class CompressionStorageError : public StorageError {
    public:
        const char *what() const noexcept {
            return "Unable to compress the data.";
        }
    };

    class DecompressionStorageError : public StorageError {
    public:
        const char *what() const noexcept {
            return "Unable to decompress the data.";
        }
    };

    class FieldStorageError : public StorageError {
    public:
        const char *what() const noexcept {
            return "Error accessing extent field.";
        }
    };
}
