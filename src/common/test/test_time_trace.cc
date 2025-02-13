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

    EXPECT_EQ(trace._trace[0].second._start_count, 1);
    EXPECT_TRUE(trace._trace[0].second._timer.elapsed_ms() > 400ms);
    EXPECT_EQ(trace._trace[1].second._start_count, 2);
    EXPECT_TRUE(trace._trace[1].second._timer.elapsed_ms() > 200ms);
    EXPECT_EQ(trace._trace[2].second._start_count, 1);
    EXPECT_TRUE(trace._trace[2].second._timer.elapsed_ms() > 100ms);
}
