#pragma once

#include <common/exception.hh>

namespace springtail::pg_fdw {
    /** For errors in the FDW. */
    class FdwError : public Error {
    public:
        FdwError() = default;
        explicit FdwError(const std::string &error)
            : Error(error)
        { }
    };

    class DDLError : public Error {
    public:
        DDLError() = default;
        explicit DDLError(const std::string &error)
            : Error(error)
        { }
    };

    class DDLSqlSyncError : public DDLError {
    public:
        DDLSqlSyncError() = default;
        explicit DDLSqlSyncError(const std::string &error)
            : DDLError(error)
        { }
    };
}
