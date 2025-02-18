#pragma once
#include <cassert>
#include <cstdint>
#include <boost/thread.hpp>

namespace springtail {

    /**
     * Synchronization counter.  Thread-safe increment and decrement along with a wait() function
     * which blocks the caller until the counter becomes zero.
     */
    class Counter {
    public:
        /** Default constructor. */
        Counter() = default;

        /** No copy constructor. */
        Counter(const Counter &c) = delete;

        /** No move constructor. */
        Counter(Counter &&c) = delete;

        /** Constructor with initial counter value. */
        explicit Counter(uint64_t count)
            : _count(count)
        { }

        /** Increments the counter. */
        void increment() {
            boost::unique_lock lock(_mutex);
            ++_count;
        }

        /**
         * @brief Increment the counter by given number
         *
         * @param incr - increment value
         */
        void increment_by(uint64_t incr) {
            boost::unique_lock lock(_mutex);
            _count += incr;
        }

        /** Decrements the counter. */
        void decrement() {
            boost::unique_lock lock(_mutex);
            --_count;
            assert(_count >= 0);
            if (_count == 0) {
                _cv.notify_all();
            }
        }

        /** Waits until the counter is zero. */
        void wait() {
            boost::unique_lock lock(_mutex);
            _cv.wait(lock, [this]{ return this->_count == 0; });
        }

        uint64_t get_count() const {
            boost::unique_lock lock(_mutex);
            return _count;
        }

    private:
        boost::condition_variable _cv; ///< Condition variable for waiting.
        mutable boost::mutex _mutex; ///< Access mutex to provide thread-safety.
        int64_t _count; ///< The underlying counter.
    };
    typedef std::shared_ptr<Counter> CounterPtr;

}
