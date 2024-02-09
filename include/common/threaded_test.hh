#pragma once

#include <memory>
#include <vector>
#include <type_traits>

#include <common/thread_pool.hh>
#include <gtest/gtest.h>

namespace springtail {

    /** Thread test state interface for ThreadedTest */
    template <class ThreadRequest>
    class ThreadTestState : public testing::Test {
        using ThreadRequestPtr = std::shared_ptr<ThreadRequest>;
    public:
        // testing::Test provides SetUp() and TearDown virtual methods
        virtual ~ThreadTestState() {}
        virtual std::vector<ThreadRequestPtr> get_requests() = 0;
        virtual void verify() = 0;
        virtual void TestBody() {}
    };

    /**
     * @brief Threaded test class; provide a test state class and test request class
     * @tparam TestState class derived from ThreadTestState; implements core test
     * @tparam TestStateRequest must have operator() overloaded;
     *         implements the requests executed by the threads
     */
    template <class TestState, class TestStateRequest>
    class ThreadedTest {
        using TestStatePtr = std::shared_ptr<TestState>;
        using TestStateRequestPtr = std::shared_ptr<TestStateRequest>;
    public:
        /**
         * @brief Construct a new Threaded Test object
         * @param num_threads number of concurrent threads
         */
        ThreadedTest(int num_threads) :
            _num_threads(num_threads), _state()
        {
            static_assert(std::is_base_of_v<ThreadTestState<TestStateRequest>, TestState>,
                          "Template param must be derived from ThreadTestState class");
        }

        /** Start the test; runs the main loop until no more requests exist */
        void start()
        {
            ;

            while (true) {
                // get next set of requests
                std::vector<TestStateRequestPtr> &&requests = _state.get_requests();
                if (requests.size() == 0) {
                    break;
                }

                // create thread pool for this set of tests
                _thread_pool = std::make_shared<ThreadPool<TestStateRequest>>(_num_threads);

                // issue the requests
                _thread_pool->queue(requests);

                // shutdown pool (acts as a barrier)
                _thread_pool->shutdown();
                _thread_pool = nullptr;

                // verify state
                ASSERT_NO_FATAL_FAILURE(_state.verify());
            }
        }


    private:
        int _num_threads;

        std::shared_ptr<ThreadPool<TestStateRequest>> _thread_pool{nullptr};

        TestState _state;
    };
}
