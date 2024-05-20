#pragma once

#include <backward.hpp>

// declaration of signal handling variable
namespace backward {
    extern backward::SignalHandling sh;
}

namespace springtail {
    /** 
     * Base error class for exceptions.  Uses backward for stack tracing.
     */
    class Error : public std::exception {
    private:
        backward::StackTrace _trace;
        std::string _error;

    public:
        /** Captures the stack trace on construction. */
        Error()
        {
            _trace.load_here();
        }

        /** Captures the stack trace on construction. */
        explicit Error(const std::string &error)
            : _error(error)
        {
            _trace.load_here();
        }

        /** Prints the backtrace captured by this exception. */
        void print_trace()
        {
            backward::Printer printer;
            printer.print(_trace);
        }

        /** Return the provided error string. */
        const char *what() const noexcept override
        {
            return _error.data();
        }
    };
}
