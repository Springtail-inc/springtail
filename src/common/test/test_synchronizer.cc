#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include <common/state_synchronizer.hh>

using namespace springtail::common;

enum class TestState { INITIAL, RUNNING, PAUSED, FINISHED };

class StateSynchronizerTest : public ::testing::Test {
protected:
    StateSynchronizer<TestState> sync{TestState::INITIAL};
};

/** Verifies initial state set correctly */
TEST_F(StateSynchronizerTest, InitialState)
{
    EXPECT_EQ(sync.get(), TestState::INITIAL);
    EXPECT_TRUE(sync.is(TestState::INITIAL));
}

/** Tests set state method */
TEST_F(StateSynchronizerTest, SetAndGet)
{
    sync.set(TestState::RUNNING);
    EXPECT_EQ(sync.get(), TestState::RUNNING);
    EXPECT_TRUE(sync.is(TestState::RUNNING));
}

/** Tests wait for state method, state set in thread */
TEST_F(StateSynchronizerTest, WaitForState)
{
    std::thread t([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sync.set(TestState::RUNNING);
    });

    sync.wait_for_state(TestState::RUNNING);
    EXPECT_EQ(sync.get(), TestState::RUNNING);

    t.join();
}

/** Tests wait for state method, state set in thread */
TEST_F(StateSynchronizerTest, WaitForStateMultiple)
{
    std::thread t([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sync.set(TestState::RUNNING);
    });

    sync.wait_for_state({TestState::RUNNING});
    EXPECT_EQ(sync.get(), TestState::RUNNING);

    t.join();
}

/** Tests wait and set method, state set in thread */
TEST_F(StateSynchronizerTest, WaitAndSet)
{
    std::thread t([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sync.set(TestState::RUNNING);
    });

    sync.wait_and_set(TestState::RUNNING, TestState::PAUSED);
    EXPECT_EQ(sync.get(), TestState::PAUSED);

    t.join();
}

/** Tests test and set method */
TEST_F(StateSynchronizerTest, TestAndSet)
{
    EXPECT_TRUE(sync.test_and_set(TestState::INITIAL, TestState::RUNNING));
    EXPECT_EQ(sync.get(), TestState::RUNNING);

    EXPECT_FALSE(sync.test_and_set(TestState::INITIAL, TestState::PAUSED));
    EXPECT_EQ(sync.get(), TestState::RUNNING);
}

/** Tests multiple waiters */
TEST_F(StateSynchronizerTest, MultipleWaiters)
{
    const int num_threads = 5;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            sync.wait_for_state(TestState::FINISHED);
            EXPECT_EQ(sync.get(), TestState::FINISHED);
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    sync.set(TestState::FINISHED);

    for (auto& t : threads) {
        t.join();
    }
}

/** Tests concurrent set and wait with multiple threads */
TEST_F(StateSynchronizerTest, ConcurrentSetAndWait)
{
    const int num_set_threads = 3;
    const int num_wait_threads = 3;
    std::vector<std::thread> set_threads;
    std::vector<std::thread> wait_threads;

    for (int i = 0; i < num_wait_threads; ++i) {
        wait_threads.emplace_back([&]() {
            sync.wait_for_state(TestState::FINISHED);
            EXPECT_EQ(sync.get(), TestState::FINISHED);
        });
    }

    for (int i = 0; i < num_set_threads; ++i) {
        set_threads.emplace_back([&]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 100));
            sync.set(TestState::RUNNING);
            std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 100));
            sync.set(TestState::PAUSED);
            std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 100));
            sync.set(TestState::FINISHED);
        });
    }

    for (auto& t : set_threads) {
        t.join();
    }
    for (auto& t : wait_threads) {
        t.join();
    }

    EXPECT_EQ(sync.get(), TestState::FINISHED);
}