namespace springtail {
    namespace common {

        /**
         * Timer class
         */
        class Timer {
        private:
            std::chrono::time_point<std::chrono::system_clock> _begin;
            std::chrono::time_point<std::chrono::system_clock> _end;

            std::chrono::milliseconds _total_elapsed;

        public:
            Timer()
                : _total_elapsed(0)
            { }

            void
            start()
            {
                _begin = std::chrono::system_clock::now();
            }

            void
            stop()
            {
                _end = std::chrono::system_clock::now();
                _total_elapsed += std::chrono::duration_cast<std::chrono::milliseconds>(_end - _begin);
            }

            std::chrono::milliseconds
            elapsed_ms()
            {
                return _total_elapsed;
            }
        };

    }
}
