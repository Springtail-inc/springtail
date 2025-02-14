#pragma once

#include <common/timer.hh>
#include <common/logging.hh>
#include <common/tracing.hh>
#include <absl/log/check.h>

namespace springtail {

namespace time_trace {

    using Name = std::string;

    /**
     * @brief Single trace stats.
     */
    struct Trace
    {
        size_t start_count = 0;
        Timer timer;
    };

    /**
     * @brief An array of traces.
     */
    struct FlatTrace
    {
        using Item = std::pair<Name, Trace>;
        std::vector<Item> trace;

        FlatTrace() = default;

        FlatTrace(const FlatTrace&) = delete;
        FlatTrace& operator=(const FlatTrace&) = delete;

        void start(Name name);
        void stop(const Name& name);
        void reset();
        std::string format();
    };

}
}

#if defined(SPRINGTAIL_INCLUDE_TIME_TRACES)
    #define TIME_TRACE_CREATE(trace) time_trace::FlatTrace trace
    #define TIME_TRACE_START(trace, name) trace.start(std::move(name))
    #define TIME_TRACE_STOP(trace, name) trace.stop(std::move(name))
    #define TIME_TRACE_RESET(trace) trace.reset();
    #define TIME_TRACE_LOG(trace) SPDLOG_INFO(trace.format());
#else
    #define TIME_TRACE_CREATE(trace)
    #define TIME_TRACE_START(trace, name)
    #define TIME_TRACE_STOP(trace, name)
    #define TIME_TRACE_RESET(trace)
    #define TIME_TRACE_LOG(trace)
#endif

