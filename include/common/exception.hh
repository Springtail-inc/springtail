#pragma once

#include <cpptrace/cpptrace.hpp>

namespace springtail {
    /**
     * Base error class for exceptions.  Uses cpptrace for stack tracing.
     */
    class Error : public std::exception {
    private:
        cpptrace::stacktrace _trace;
        std::string _error;

    public:
        /** Captures the stack trace on construction. */
        Error()
        {
            _trace = cpptrace::generate_trace();
        }

        /** Captures the stack trace on construction. */
        explicit Error(const std::string &error)
            : _error(error)
        {
            _trace = cpptrace::generate_trace();
        }

        /** Prints the backtrace captured by this exception. */
        void log_backtrace() const;

        /** Return the provided error string. */
        const char *what() const noexcept override
        {
            return _error.data();
        }
    };

    /** parent connection error */
    class PgConnectionError : public Error {
    public:
        PgConnectionError() = default;
        explicit PgConnectionError(const std::string &error)
            : Error(error)
        { }
    };

    class PgNotConnectedError : public PgConnectionError {
        const char *what() const noexcept {
            return "The connection is closed";
        }
    };

    class PgAlreadyConnectedError : public PgConnectionError {
        const char *what() const noexcept {
            return "Already connected";
        }
    };

    class PgQueryError : public PgConnectionError {
        const char *what() const noexcept {
            return "An error occurred executing the query";
        }
    };

    class PgNoResultError : public PgConnectionError {
        const char *what() const noexcept {
            return "No query result found";
        }
    };

} // namespace springtail
