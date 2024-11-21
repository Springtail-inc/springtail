#include <chrono>
#include <future>
#include <thread>

#include <gtest/gtest.h>

#include <common/counter.hh>

using namespace springtail;

TEST(CounterTest, SimpleTest) {
    Counter counter(8);

    // get the current time
    auto start = std::chrono::system_clock::now();

    // construct two threads that will do concurrent decrement
    std::thread t1([&counter]() {
        for (int i = 0; i < 4; ++i) {
            counter.decrement();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    std::thread t2([&counter]() {
        for (int i = 0; i < 4; ++i) {
            counter.decrement();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    // create a thread that will wait for the counter to hit zero
    auto future = std::async(std::launch::async, [&counter]() {
        counter.wait();
    });

    // should not take 3 seconds
    EXPECT_TRUE(future.wait_until(start + std::chrono::seconds(3)) == std::future_status::timeout);

    // should not take 5 seconds (3+2)
    EXPECT_FALSE(future.wait_until(start + std::chrono::seconds(5)) == std::future_status::timeout);

    // cleanup the threads
    t1.join();
    t2.join();
}
