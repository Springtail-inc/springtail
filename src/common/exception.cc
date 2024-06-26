#include <csignal>

#include <common/exception.hh>
#include <common/logging.hh>

namespace {
    void
    backtrace_handler(int signo)
    {
        // note: may be unsafe -- for known safe method, see cpptrace's signal_demo.cpp

        auto trace = cpptrace::generate_trace();

        std::stringstream ss;
        trace.print(ss);

        SPDLOG_ERROR("Backtrace from signal {}:\n{}", signo, ss.str());
        exit(-1);
    }
}

namespace springtail {
    void
    init_exception()
    {
        // register the signal handlers for backtraces
        std::vector<int> signals{
            SIGABRT, // Abort signal from abort(3)
            SIGBUS,  // Bus error (bad memory access)
            SIGFPE,  // Floating point exception
            SIGILL,  // Illegal Instruction
            SIGIOT,  // IOT trap. A synonym for SIGABRT
            SIGQUIT, // Quit from keyboard
            SIGSEGV, // Invalid memory reference
            SIGSYS,  // Bad argument to routine (SVr4)
            SIGTRAP, // Trace/breakpoint trap
            SIGXCPU, // CPU time limit exceeded (4.2BSD)
            SIGXFSZ, // File size limit exceeded (4.2BSD)
#if defined(__APPLE__)
            SIGEMT, // emulation instruction executed
#endif
        };

        for (int s : signals) {
            std::signal(s, backtrace_handler);
        }
    }

    void
    Error::log_backtrace() const
    {
        std::stringstream ss;
        _trace.print(ss);

        SPDLOG_ERROR("Backtrace:\n{}", ss.str());
    }
}
