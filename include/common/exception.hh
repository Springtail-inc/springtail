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

    /**
     * Intialize the exception and backtrace handling.
     */
    void init_exception(void);
}
