#pragma once

#include <absl/log/check.h>
#include <list>

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
    using List = std::list<Item>;
    List traces;

    FlatTraceSet() = default;

    FlatTraceSet(const FlatTraceSet &) = delete;
    FlatTraceSet &operator=(const FlatTraceSet &) = delete;

    Trace& init(std::string_view name);
    void update(std::string_view name, const Trace &trace);
    Trace& find(std::string_view name);

    void reset();
    std::string format();
};

struct scoped_trace
{
    Trace& _t;

    scoped_trace(Trace& t) : _t{t} {
        _t.start();
    }

    ~scoped_trace() {
        _t.stop();
    }
};

/** 
 * This is a place for sharing traces across translation units.
 * The variable is instantiated in time_trace.cc of the common lib.
 */
extern time_trace::FlatTraceSet traces;

/**
 * @brief Initialize a new trace in the given set. This function is intended
 * to be used for initializing static traces within a scope (see TIME_TRACE_SCOPED).
 */
inline time_trace::Trace& create_trace(FlatTraceSet& set, std::string_view n)
{
    return set.init(n);
}

}  // namespace springtail::time_trace

#if defined(SPRINGTAIL_INCLUDE_TIME_TRACES)

#define TIME_TRACESET(trace_set) time_trace::FlatTraceSet trace_set
#define TIME_TRACESET_INIT(trace_set, name) trace_set.init(name)
#define TIME_TRACESET_UPDATE(trace_set, name, trace) trace_set.update(name, trace)
#define TIME_TRACESET_RESET(trace_set) trace_set.reset();
#define TIME_TRACESET_LOG(trace_set) SPDLOG_INFO(trace_set.format());

#define TIME_TRACE(trace) time_trace::Trace trace
#define TIME_TRACE_START(trace) trace.start()
#define TIME_TRACE_STOP(trace) trace.stop()

#define TIME_TRACE_SCOPED(trace_set, name) static time_trace::Trace& tr = time_trace::create_trace(trace_set, name); time_trace::scoped_trace s(tr);

#else

#define TIME_TRACESET(trace_set)
#define TIME_TRACESET_INIT(trace_set, name)
#define TIME_TRACESET_UPDATE(trace_set, name, trace)
#define TIME_TRACESET_RESET(trace_set)
#define TIME_TRACESET_LOG(trace_set)

#define TIME_TRACE(trace)
#define TIME_TRACE_START(trace)
#define TIME_TRACE_STOP(trace)

#define TIME_TRACE_SCOPED(trace_set, name)

#endif
