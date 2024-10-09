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
    }
}
