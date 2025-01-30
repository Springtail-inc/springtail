#pragma once

#include <unistd.h>
#include <sys/time.h>

#include <cstdio>
#include <csignal>
#include <string>
#include <vector>
#include <optional>

#include <common/logging.hh>
#include <common/properties.hh>
#include <common/exception.hh>

#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TJSONProtocol.h>

namespace springtail {
    /**
     * @brief Initialize the springtail system
     * @param log_filename log filename override
     * @param daemon_pid if set, daemonize the process and store the pid in the provided file
     * @param logging_mask logging mask override
     */
    void springtail_init(const std::optional<std::string> &log_filename = std::nullopt,
                         const std::optional<std::string> &daemon_pid = std::nullopt,
                         const std::optional<uint32_t> &logging_mask = std::nullopt);

    /**
     * @brief Initialize the springtail system
     * @param log_filename log filename override
     * @param logging_mask logging mask override
     */
    void springtail_init(const std::string &log_filename,
                         uint32_t logging_mask);


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

        template <typename T>
        nlohmann::json
        thrift_to_json(const T &obj)
        {
            auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
            apache::thrift::protocol::TJSONProtocol protocol(buffer);

            // serialize the object
            obj.write(&protocol);

            return nlohmann::json::parse(buffer->getBufferAsString());
        }

        template <typename T>
        nlohmann::json
        thrift_vector_to_json(const std::vector<T> &vec_obj)
        {
            nlohmann::json jsonArray = nlohmann::json::array();

            for (const auto &obj : vec_obj) {
                // Convert the buffer content to a string
                auto jsonObj = thrift_to_json(obj);

                // Parse and add to the JSON array
                jsonArray.push_back(jsonObj);
            }
            return jsonArray;
        }

        template <typename T>
        T
        json_to_thrift(const nlohmann::json &json)
        {
            std::string data = json.dump();
            auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>
                (const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(data.c_str())),
                 data.size());
            apache::thrift::protocol::TJSONProtocol protocol(buffer);

            // deserialize the object
            T obj;
            obj.read(&protocol);

            return obj;
        }

        template <typename T>
        std::vector<T>
        json_to_thrift_vector(const nlohmann::json &json)
        {
            std::vector<T> result;
            if (json.is_null() || !json.is_array()) {
                return result; // Return an empty vector
            }
            std::string data = json.dump();
            auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
            apache::thrift::protocol::TJSONProtocol protocol(buffer);

            for (const auto& jsonElement : json) {
                buffer->resetBuffer();

                auto jsonStr = jsonElement.dump();

                buffer->write(reinterpret_cast<const uint8_t*>(jsonStr.c_str()), jsonStr.size());

                // Create a new Thrift object and read from the protocol
                T obj;
                obj.read(&protocol);
                result.push_back(obj);
            }

            return result;
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
