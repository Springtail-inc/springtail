#pragma once

#include <unistd.h>
#include <sys/time.h>

#include <cstdio>
#include <csignal>
#include <string>
#include <vector>

#include <common/logging.hh>
#include <common/properties.hh>
#include <common/exception.hh>

namespace springtail {
    void springtail_init(uint32_t logging_mask = LOG_ALL);

    namespace common {
        /**
         * @brief Get the time in milliseconds
         * @return uint64_t time
         */
        static inline
        uint64_t get_time_in_millis()
        {
            struct timeval t;
            gettimeofday(&t, nullptr);
            return (uint64_t)t.tv_sec * 1000 + t.tv_usec / 1000;
        }

        /**
         * @brief Split a string based on a delimiter
         * @param delimiter delimiter string (e.g., ":")
         * @param string_value string to split (e.g., "this:is:a:string")
         * @param outvec output vector of strings
         */
        static inline void
        split_string(const std::string &delimiter,
                     const std::string &string_value,
                     std::vector<std::string> &outvec)
        {
            size_t start_pos = 0, end_pos = 0;
            std::string token;
            // iterate through string getting substring from past last delimiter
            while ((end_pos = string_value.find(delimiter, start_pos)) != std::string::npos) {
                token = string_value.substr(start_pos, end_pos - start_pos);
                outvec.push_back(std::move(token));
                start_pos = end_pos + delimiter.size();
            }
            // handle last token
            if (start_pos < string_value.size()) {
                token = string_value.substr(start_pos);
                outvec.push_back(std::move(token));
            }
        }

        /**
         * Turns the current process into a background daemon, storing it's process ID into the
         * provided file.
         * @param pid_filename The path of the file in which to store the PID.
         */
        void daemonize(const std::filesystem::path &pid_filename);
    }
}
