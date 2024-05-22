#pragma once

#include <common/exception.hh>

namespace springtail {
    /** For type conversion and invalid type errors. */
    class TypeError : public Error {
    public:
        TypeError() { }
        explicit TypeError(const std::string &error)
            : Error(error)
        { }
    };

    /** For schema manipulation errors. */
    class SchemaError : public Error {
    public:
        SchemaError() { }
        explicit SchemaError(const std::string &error)
            : Error(error)
        { }
    };

    /** For storage access errors. */
    class StorageError : public Error {
    public:
        StorageError() { }
        explicit StorageError(const std::string &error)
            : Error(error)
        { }
    };

    /** For validation errors when accessing data. */
    class ValidationError : public Error {
    public:
        ValidationError() { }
        explicit ValidationError(const std::string &error)
            : Error(error)
        { }
    };
}
