#pragma once

#include <map>
#include <memory>
#include <vector>
#include <type_traits>

#include <common/thread_pool.hh>

#include <gtest/gtest.h>

namespace springtail {

    /**
     * Allows a test to easily implement a multi-phase test in which each phase consists of a queue
     * of operations over which a configurable number of workers execute in parallel.  After each
     * phase is a verification step.
     *
     * To use this class, the test should create a request object and provide it as the template
     * parameter.  During test construction, the requests and verification are also set up in
     * phases.  First the requests are populated in the current phase by repeatedly calling
     * add_request().  Then the verification function for the phase is provided by calling
     * set_verify().  To add an additional phase, you call next_phase() and repeat the steps of
     * add_request() and set_verify().  This can be done any number of times.  Once all of the
     * phases have been defined, the test calls run(), which will execute the sequence of phases
     * in-order.
     */
    template <class ThreadRequest>
    class PhasedThreadTest {
        using ThreadRequestPtr = std::shared_ptr<ThreadRequest>;
    public:
        PhasedThreadTest()
            : _phase(0)
        { }

        void next_phase() {
            ++_phase;
        }

        void add_request(ThreadRequestPtr request) {
            _requests[_phase].push_back(request);
        }

        void set_verify(std::function<void()> verify) {
            _verifiers.insert({_phase, verify});
        }

        void run(int thread_count) {
            for (int i = 0; i <= _phase; i++) {
                // create thread pool for this set of tests
                auto thread_pool = std::make_shared<ThreadPool<ThreadRequest>>(thread_count);

                // issue the requests
                thread_pool->queue(_requests[i]);

                // shutdown pool (acts as a barrier)
                thread_pool->shutdown();

                // verify state
                ASSERT_NO_FATAL_FAILURE(_verifiers[i]());
            }
        }

    private:
        int _phase;
        std::map<int, std::vector<ThreadRequestPtr>> _requests;
        std::map<int, std::function<void()>> _verifiers;
    };
}
