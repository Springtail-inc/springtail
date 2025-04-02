#include <csignal>

#include <common/exception.hh>
#include <common/logging.hh>

namespace {
    void
    backtrace_handler(int signo)
    {
        // attempt to flush the log before we try to capture the backtrace in case something goes wrong
        spdlog::default_logger()->flush();

        // note: may be unsafe -- for known safe method, see cpptrace's signal_demo.cpp
        auto trace = cpptrace::generate_trace();

        std::stringstream ss;
        trace.print(ss);

        LOG_ERROR(springtail::LOG_ALL, "Backtrace from signal {}:\n{}", signo, ss.str());
        signal(signo, SIG_DFL);
        raise(signo);
    }
}

namespace springtail {
    ExceptionRunner::ExceptionRunner() : ServiceRunner("Exception"),
        _signals({
        #if defined(__APPLE__)
            SIGEMT, // emulation instruction executed
        #endif
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
            SIGXFSZ // File size limit exceeded (4.2BSD)
        }) {}

    bool ExceptionRunner::start() {
        for (int s : _signals) {
            std::signal(s, backtrace_handler);
        }
        return true;
    }

    void ExceptionRunner::stop() {
        for (int s : _signals) {
            std::signal(s, SIG_DFL);
        }
    }

    void
    Error::log_backtrace() const
    {
        std::stringstream ss;
        _trace.print(ss);

        LOG_ERROR(LOG_ALL, "Backtrace:\n{}", ss.str());
    }
}
