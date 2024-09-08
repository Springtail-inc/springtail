#pragma once

#include <common/exception.hh>

namespace springtail::pg_fdw {
    /** For errors in the FDW. */
    class FdwError : public Error {
    public:
        FdwError() { }
        explicit FdwError(const std::string &error)
            : Error(error)
        { }
    };
}
