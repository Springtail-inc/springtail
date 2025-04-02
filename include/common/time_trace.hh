#pragma once

#include <absl/log/check.h>

#include <common/logging.hh>
#include <common/timer.hh>
#include <common/tracing.hh>

namespace springtail::time_trace {

/**
 * @brief Single trace stats.
 */
struct Trace {
    size_t start_count = 0;
    Timer timer;

    void start()
    {
        ++start_count;
        timer.start();
    }

    void stop() { timer.stop(); }

    void reset()
    {
        start_count = 0;
        timer.reset();
    }

    Trace &operator+=(const Trace &rhs)
    {
        start_count += rhs.start_count;
        timer += rhs.timer;
        return *this;
    }
};

/**
 * @brief An array of traces.  Reports results based on registration order.
 */
struct FlatTraceSet {
    using Item = std::pair<std::string, Trace>;
    std::vector<Item> traces;

    FlatTraceSet() = default;

    FlatTraceSet(const FlatTraceSet &) = delete;
    FlatTraceSet &operator=(const FlatTraceSet &) = delete;

    void init(std::string_view name);
    void update(std::string_view name, const Trace &trace);

    void reset();
    std::string format();
};

}  // namespace springtail::time_trace

#if defined(SPRINGTAIL_INCLUDE_TIME_TRACES)

#define TIME_TRACESET(trace_set) time_trace::FlatTraceSet trace_set
#define TIME_TRACESET_INIT(trace_set, name) trace_set.init(name)
#define TIME_TRACESET_UPDATE(trace_set, name, trace) trace_set.update(name, trace)
#define TIME_TRACESET_RESET(trace_set) trace_set.reset();
#define TIME_TRACESET_LOG(trace_set) LOG_INFO(LOG_ALL, "{}", trace_set.format());

#define TIME_TRACE(trace) time_trace::Trace trace
#define TIME_TRACE_START(trace) trace.start()
#define TIME_TRACE_STOP(trace) trace.stop()

#else

#define TIME_TRACESET(trace_set)
#define TIME_TRACESET_INIT(trace_set, name)
#define TIME_TRACESET_UPDATE(trace_set, name, trace)
#define TIME_TRACESET_RESET(trace_set)
#define TIME_TRACESET_LOG(trace_set)

#define TIME_TRACE(trace)
#define TIME_TRACE_START(trace)
#define TIME_TRACE_STOP(trace)

#endif
