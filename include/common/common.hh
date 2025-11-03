#pragma once

#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>

#include <algorithm>
#include <cassert>
#include <csignal>
#include <cstdio>
#include <deque>
#include <string>
#include <vector>
#include <sstream>

namespace springtail {
    /**
     * @brief Get integral value from enum
     * @tparam E enum type
     * @param e enum value
     * @return std::underlying_type<E>::type
     */
    template<typename E>
    constexpr auto enum_to_integral(E e) -> typename std::underlying_type<E>::type
    {
        return static_cast<typename std::underlying_type<E>::type>(e);
    }

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
         * @brief Get the time in microseconds
         * @return uint64_t time in microseconds since epoch
         */
        static inline
        uint64_t get_time_in_micros()
        {
            struct timeval t;
            gettimeofday(&t, nullptr);
            return (uint64_t)t.tv_sec * 1000000 + t.tv_usec;
        }

        /**
         * @brief Joins strings into a single string with a delimiter between them.
         * @param delimiter delimiter string (e.g., ",")
         * @param first An iterator to the first element to include
         * @param last An iterator past the end of the last element to include
         */
        template<class ForwardIt>
        std::string
        join_string(const std::string &delimiter,
                    ForwardIt first,
                    ForwardIt last)
        {
            std::ostringstream buf;
            while (first != last) {
                buf << *first;
                ++first;
                if (first != last) {
                    buf << delimiter;
                }
            }
            return buf.str();
        }

        /**
         * @brief Joins strings into a single string with a delimiter between them.
         * @param delimiter delimiter string (e.g., ",")
         * @param vec vector of strings to join
         */
        static inline std::string
        join_string(const std::string &delimiter,
                    const std::vector<std::string> &vec)
        {
            return join_string(delimiter, vec.begin(), vec.end());
        }

        /**
         * @brief Split a string based on a delimiter
         * @param delimiter delimiter string (e.g., ":")
         * @param string_value string to split (e.g., "this:is:a:string")
         * @param outvec output vector of strings
         */
        static inline void
        split_string(std::string_view delimiter,
                     std::string_view string_value,
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
         * @brief Split a string based on a delimiter
         * @param delimiter delimiter string (e.g., ":")
         * @param string_value string to split (e.g., "this:is:a:string")
         * @param out_queue output queue of strings
         */
        static inline void
        split_string(const std::string &delimiter,
                     const std::string &string_value,
                     std::deque<std::string> &out_queue)
        {
            std::vector<std::string> out_vector;
            split_string(delimiter, string_value, out_vector);
            std::move(out_vector.begin(), out_vector.end(), std::back_inserter(out_queue));
        }

        /**
         * @brief Escape a string with quotes in it, returns a quoted string with quotes escaped
         * @param input input string
         * @return std::string
         */
        static inline std::string
        escape_quoted_string(const std::string &input)
        {
            std::string result;
            result.reserve(input.length() + 2);  // Reserve space for quotes and potential escapes

            // Add opening quote
            result.push_back('"');

            // Process each character
            for (char c : input) {
                if (c == '"') {
                    result.push_back('\\');
                }
                result.push_back(c);
            }

            // Add closing quote
            result.push_back('"');

            return result;
        }

        /**
         * @brief Unescape a quoted string, returns a string with quotes unescaped
         * @param input input string
         * @return std::string
         */
        static inline std::string
        unescape_quoted_string(const std::string &input)
        {
            std::string result;
            result.reserve(input.length());

            bool in_quotes = false;
            for (size_t i = 0; i < input.length(); ++i) {
                char c = input[i];

                if (c == '"' && (i == 0 || input[i-1] != '\\')) {
                    // Unescaped quote - toggle quote mode
                    in_quotes = !in_quotes;
                    continue;
                }

                if (c == '\\' && in_quotes && i + 1 < input.length() && input[i+1] == '"') {
                    // Escaped quote - skip the escape character
                    c = '"';
                    ++i;
                }
                result.push_back(c);
            }

            assert(!in_quotes);

            return result;
        }

        /**
         * @brief Split a delimited string into a vector of quoted strings
         * @param delimiter delimiter char
         * @param input input string
         * @param std::vector<std::string> output vector
         */
        static inline void
        split_quoted_string(const char delimiter,
                            const std::string &input,
                            std::vector<std::string> &result)
        {
            std::string current;
            bool in_quotes = false;

            for (size_t i = 0; i < input.length(); ++i) {
                char c = input[i];

                if (c == '"' && (i == 0 || input[i-1] != '\\')) {
                    // Unescaped quote - toggle quote mode
                    in_quotes = !in_quotes;
                    current += c;
                }
                else if (c == delimiter && !in_quotes) {
                    // Delimiter found outside quotes - split here
                    result.push_back(current);
                    current.clear();
                }
                else {
                    // Regular character or escaped quote
                    current += c;
                }
            }

            // Add the last part
            if (!current.empty()) {
                result.push_back(current);
            }

            assert(!in_quotes);

            return;
        }

        static inline
        bool to_bool(std::string str)
        {
            if (str.empty()) {
                return false;
            }
            std::transform(str.begin(), str.end(), str.begin(), ::tolower);
            if (str == "1" || str == "true" || str == "yes" || str == "t" || str == "y") {
                return true;
            }
            return false;
        }

        static inline
        std::string to_lower(const std::string& str)
        {
            std::string lower_str = str;
            std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(), ::tolower);
            return lower_str;
        }

        static inline std::string
        trim(const std::string& str)
        {
            // Find the first non-whitespace character
            auto start = std::find_if_not(str.begin(), str.end(), [](unsigned char c) {
                return std::isspace(c);
            });

            // Find the last non-whitespace character
            auto end = std::find_if_not(str.rbegin(), str.rend(), [](unsigned char c) {
                return std::isspace(c);
            }).base();

            // Return the trimmed string
            return (start < end) ? std::string(start, end) : std::string();
        }
    }
}
