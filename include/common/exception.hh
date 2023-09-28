#pragma once

#include <backward.hpp>

// declaration of signal handling variable
namespace backward {
    extern backward::SignalHandling sh;
}

namespace st_common {
    /** 
     * Base error class for exceptions.  Uses backward for stack tracing.
     */
    class Error : public std::exception {
    private:
        backward::StackTrace _trace;

    public:
        /** Captures the stack trace on construction. */
        Error() {
            _trace.load_here();
        }

        /** Prints the backtrace captured by this exception. */
        void print_trace() {
            backward::Printer printer;
            printer.print(_trace);
        }
    };
}
