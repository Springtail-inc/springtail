#pragma once

#include <chrono>

namespace springtail {

    /**
     * Tracks elapsed time.  Repeated calls to start()/stop() will result in cumulative elapsed time.
     */
    class Timer {
    private:
        using clock = std::chrono::steady_clock;
        using duration = clock::duration;
        std::chrono::time_point<clock> _begin; ///< Track when the timer started.
        std::chrono::time_point<clock> _end; ///< Track when the timer stopped.

        duration _total_elapsed; ///< Total time while the timer was active.

    public:
        Timer()
            : _total_elapsed(0)
        { }

        /**
         * Start the timer.
         */
        void
        start()
        {
            _begin = clock::now();
        }

        /**
         * Stop the timer and record the additional elapsed time.
         */
        void
        stop()
        {
            _end = clock::now();
            _total_elapsed += _end - _begin;
        }

        /**
         * Resets the elapsed time back to zero.
         */
        void
        reset()
        {
            _total_elapsed = duration(0);
        }

        /**
         * Retrieve the elapsed time.
         *
         * @return Elapsed time in milliseconds.
         */
        std::chrono::milliseconds
        elapsed_ms() const
        {
            return std::chrono::duration_cast<std::chrono::milliseconds>(_total_elapsed);
        }
    };

}
