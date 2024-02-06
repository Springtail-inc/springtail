#pragma once

#include <sys/time.h>

namespace springtail {
    void springtail_init();

    namespace common {
        /**
         * @brief Get the time in milliseconds
         * @return uint64_t time
         */
        static inline uint64_t get_time_in_millis()
        {
            struct timeval t;
            gettimeofday(&t, nullptr);
            return (uint64_t)t.tv_sec * 1000 + t.tv_usec / 1000;
        }
    }
}
