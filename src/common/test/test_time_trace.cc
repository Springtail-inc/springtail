#include <gtest/gtest.h>
#include <thread>

#define SPRINGTAIL_INCLUDE_TIME_TRACES 1

#include <common/time_trace.hh>

using namespace springtail;

TEST(TimeTraceTest, Basic)
{
    using namespace std::chrono_literals;

    TIME_TRACE(one);
    TIME_TRACE_START(one);
    std::this_thread::sleep_for(100ms);

    TIME_TRACE(two);
    TIME_TRACE_START(two);
    std::this_thread::sleep_for(100ms);
    TIME_TRACE_STOP(two);

    TIME_TRACE_START(two);
    std::this_thread::sleep_for(100ms);
    TIME_TRACE_STOP(two);

    TIME_TRACE(three);
    TIME_TRACE_START(three);
    std::this_thread::sleep_for(100ms);
    TIME_TRACE_STOP(three);

    TIME_TRACE_STOP(one);

    TIME_TRACESET(traces);
    TIME_TRACESET_UPDATE(traces, "One", one);
    TIME_TRACESET_UPDATE(traces, "Two", two);
    TIME_TRACESET_UPDATE(traces, "Three", three);
    TIME_TRACESET_LOG(traces);

    EXPECT_EQ(traces.traces[0].first, "One");
    EXPECT_EQ(traces.traces[0].second.start_count, 1);
    EXPECT_GE(traces.traces[0].second.timer.elapsed_ms(), 400ms);
    EXPECT_LE(traces.traces[0].second.timer.elapsed_ms(), 420ms);
    EXPECT_EQ(traces.traces[1].first, "Two");
    EXPECT_EQ(traces.traces[1].second.start_count, 2);
    EXPECT_GE(traces.traces[1].second.timer.elapsed_ms(), 200ms);
    EXPECT_EQ(traces.traces[2].first, "Three");
    EXPECT_EQ(traces.traces[2].second.start_count, 1);
    EXPECT_GE(traces.traces[2].second.timer.elapsed_ms(), 100ms);
    EXPECT_LE(traces.traces[2].second.timer.elapsed_ms(), 120ms);
}
