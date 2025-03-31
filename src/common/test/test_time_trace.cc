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

    auto it = traces.traces.begin();
    EXPECT_EQ(it->first, "One");
    EXPECT_EQ(it->second.start_count, 1);
    EXPECT_GE(it->second.timer.elapsed_ms(), 400ms);
    EXPECT_LE(it->second.timer.elapsed_ms(), 420ms);

    ++it;
    EXPECT_EQ(it->first, "Two");
    EXPECT_EQ(it->second.start_count, 2);
    EXPECT_GE(it->second.timer.elapsed_ms(), 200ms);

    ++it;
    EXPECT_EQ(it->first, "Three");
    EXPECT_EQ(it->second.start_count, 1);
    EXPECT_GE(it->second.timer.elapsed_ms(), 100ms);
    EXPECT_LE(it->second.timer.elapsed_ms(), 120ms);

    for (int i=0; i != 3; ++i) {
        TIME_TRACE_SCOPED(traces, "scope");
        std::this_thread::sleep_for(100ms);
    }

    auto tr = traces.find("scope");
    EXPECT_GE(tr.timer.elapsed_ms(), 300ms);
    EXPECT_EQ(tr.start_count, 3);
}
