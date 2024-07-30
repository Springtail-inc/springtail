#pragma once

#include <memory>
#include <string_view>
#include <chrono>
#include <vector>
#include <mutex>

#include <spdlog/details/fmt_helper.h>
#include <spdlog/formatter.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>

#include <proxy/buffer_pool.hh>

namespace springtail::pg_proxy {

    /**
     * Binary logger for writing binary data to a file
     * Uses special formatter to write binary data to a file
     */
    class Logger {

    protected:
        /** Binary formatter for logging binary data */
        class BinaryFormatter : public spdlog::formatter {
        public:
            BinaryFormatter() {}

            ~BinaryFormatter() override = default;

            std::unique_ptr<spdlog::formatter> clone() const override {
                return std::make_unique<BinaryFormatter>();
            }

            void format(const spdlog::details::log_msg &msg, spdlog::memory_buf_t &dest) override {
                auto *buf_ptr = msg.payload.data();
                dest.append(buf_ptr, buf_ptr + msg.payload.size());
            }
        };

    public:
        enum MSG_TYPE : uint8_t {
            FROM_CLIENT=1,
            FROM_REPLICA=2,
            FROM_PRIMARY=3,
            TO_CLIENT=4,
            TO_REPLICA=5,
            TO_PRIMARY=6
        };

        /**
         * @brief Construct a new Logger object
         * @param log_path Path to the log file
         * @param max_size Maximum size of the log file
         * @param max_files Maximum number of log files
         */
        Logger(const std::string &log_path, int max_size, int max_files)
        {
            _file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(log_path, max_size, max_files);
            auto binary_formatter = std::make_unique<BinaryFormatter>();
            _file_sink->set_formatter(std::move(binary_formatter));
            _file_sink->set_level(spdlog::level::info);
        }

        void log_data(MSG_TYPE type, uint32_t session_id, uint64_t seq_id,
                      char code, int32_t data_length,
                      const char *data, bool final=true)
        {
            char header[4 + 8 + 8 + 4 + 4];
            auto ts = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
            int64_t timestamp = ts.count();

            // generate header:
            // 1B: type
            // 1B: final flag -- 1 if the last part in the msg
            // 1B: pg op code ('\O' if pipelined buffers)
            // 1B: padding unused
            // 8B: timestamp
            // 8B: sequence id
            // 4B: session id
            // 4B: length of the message (buffer)
            int off = 0;
            header[off] = type;
            off += 1;
            header[off] = final ? 1 : 0;
            off += 1;
            header[off] = code;
            off += 1;
            // padding
            header[off] = 0;
            off += 1;
            std::copy_n(reinterpret_cast<char *>(&timestamp), 8, header + off);
            off += 8;
            std::copy_n(reinterpret_cast<char *>(&seq_id), 8, header + off);
            off += 8;
            std::copy_n(reinterpret_cast<char *>(&session_id), 4, header + off);
            off += 4;
            std::copy_n(reinterpret_cast<char *>(&data_length), 4, header + off);
            off += 4;

            // write out the header and then the buffer
            std::vector<std::string_view> parts;
            parts.push_back(std::string_view(header, sizeof(header)));
            parts.push_back(std::string_view(data, data_length));
            log(parts);
        }

        /**
         * @brief Log a message to the file
         * @param msg Message to log
         */
        void log(const std::vector<std::string_view> &msg) {
            spdlog::details::log_msg log_msg;
            log_msg.level = spdlog::level::info;

            std::unique_lock<std::mutex> lock(_mutex);
            for (const auto &part : msg) {
                log_msg.payload = part;
                _file_sink->log(log_msg);
            }
        }

        /**
         * @brief Flush the log file
         */
        void flush() {
            _file_sink->flush();
        }

    private:
        /** File sink */
        std::shared_ptr<spdlog::sinks::rotating_file_sink_mt> _file_sink;
        std::mutex _mutex;
    };
    using LoggerPtr = std::shared_ptr<Logger>;

} // namespace springtail::pg_proxy