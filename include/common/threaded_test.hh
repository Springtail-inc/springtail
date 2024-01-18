#pragma once

#include <memory>
#include <vector>
#include <type_traits>

#include <common/thread_pool.hh>

namespace springtail {

    /** Thread test state interface for ThreadedTest */
    template <class ThreadRequest>
    class ThreadTestState {
        using ThreadRequestPtr = std::shared_ptr<ThreadRequest>;
    public:
        virtual ~ThreadTestState() {}
        virtual void init() = 0;
        virtual std::vector<ThreadRequestPtr> get_requests() = 0;
        virtual bool verify() = 0;
        virtual void shutdown() = 0;
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
            _num_threads(num_threads),
            _state_ptr(std::make_shared<TestState>())
        {
            static_assert(std::is_base_of_v<ThreadTestState<TestStateRequest>, TestState>,
                          "Template param must be derived from ThreadTestState class");
        }

        /** Start the test; runs the main loop until no more requests exist */
        void start()
        {
            _state_ptr->init();
            std::vector<TestStateRequestPtr> requests;

            while (true) {
                // get next set of requests
                requests = _state_ptr->get_requests();
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
                if (!_state_ptr->verify()) {
                    break;
                }
            }

            _state_ptr->shutdown();
        }


    private:
        int _num_threads;

        std::shared_ptr<ThreadPool<TestStateRequest>> _thread_pool{nullptr};

        TestStatePtr _state_ptr;
    };
}