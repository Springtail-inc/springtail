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

    /** For schema manipulation errors. */
    class SchemaError : public Error {
    public:
        SchemaError() { }
        SchemaError(const std::string &error)
            : Error(error)
        { }
    };

    /** For storage access errors. */
    class StorageError : public Error {
    public:
        StorageError() { }
        StorageError(const std::string &error)
            : Error(error)
        { }
    };

    /** For validation errors when accessing data. */
    class ValidationError : public Error {
    public:
        ValidationError() { }
        ValidationError(const std::string &error)
            : Error(error)
        { }
    };

    /** For errors with the logical block map. */
    class LogicalBlockError : public Error {
    public:
        LogicalBlockError() { }
        LogicalBlockError(const std::string &error)
            : Error(error)
        { }
    }
}
