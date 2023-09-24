#include <chrono>

namespace springtail {
    namespace common {

        /**
         * Tracks elapsed time.  Repeated calls to start()/stop() will result in cumulative elapsed time.
         */
        class Timer {
        private:
            std::chrono::time_point<std::chrono::system_clock> _begin; ///< Track when the timer started.
            std::chrono::time_point<std::chrono::system_clock> _end; ///< Track when the timer stopped.

            std::chrono::milliseconds _total_elapsed; ///< Total time while the timer was active.

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
                _begin = std::chrono::system_clock::now();
            }

            /**
             * Stop the timer and record the additional elapsed time.
             */
            void
            stop()
            {
                _end = std::chrono::system_clock::now();
                _total_elapsed += std::chrono::duration_cast<std::chrono::milliseconds>(_end - _begin);
            }

            /**
             * Resets the elapsed time back to zero.
             */
            void
            reset()
            {
                _total_elapsed = std::chrono::milliseconds(0);
            }

            /**
             * Retrieve the elapsed time.
             *
             * @return Elapsed time in milliseconds.
             */
            std::chrono::milliseconds
            elapsed_ms()
            {
                return _total_elapsed;
            }
        };

    }
}
