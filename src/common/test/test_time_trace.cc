#include <gtest/gtest.h>
#include <thread>

#include <common/time_trace.hh>

using namespace springtail;

TEST(TimeTraceTest, Basic)
{
    using namespace std::chrono_literals;

    time_trace::FlatTrace trace{};

    trace.start("One");
    std::this_thread::sleep_for(100ms);

    trace.start("Two");
    std::this_thread::sleep_for(100ms);
    trace.stop("Two");

    trace.start("Two");
    std::this_thread::sleep_for(100ms);
    trace.stop("Two");

    trace.start("Three");
    std::this_thread::sleep_for(100ms);
    trace.stop("Three");

    trace.stop("One");

    auto s = trace.format();

    EXPECT_EQ(trace.trace[0].second.start_count, 1);
    EXPECT_GE(trace.trace[0].second.timer.elapsed_ms(), 400ms);
    EXPECT_LE(trace.trace[0].second.timer.elapsed_ms(), 420ms);
    EXPECT_EQ(trace.trace[1].second.start_count, 2);
    EXPECT_GE(trace.trace[1].second.timer.elapsed_ms(), 200ms);
    EXPECT_EQ(trace.trace[2].second.start_count, 1);
    EXPECT_GE(trace.trace[2].second.timer.elapsed_ms(), 100ms);
    EXPECT_LE(trace.trace[2].second.timer.elapsed_ms(), 120ms);
}
