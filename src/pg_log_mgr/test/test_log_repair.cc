#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <iostream>
#include <stdio.h>

#include <common/init.hh>
#include <common/logging.hh>
#include <common/exception.hh>


#include <pg_repl/pg_msg_log_gen.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_msg_stream.hh>

#include <sys_tbl_mgr/client.hh>

#include <test/services.hh>

using namespace springtail;

namespace {
    class LogRepair_Test : public ::testing::Test {
    protected:
        static constexpr char const * const LOG_FILE = "/tmp/test_log_reader.log";
        static constexpr char const * const LOG_FILE_TRUNC = "/tmp/test_log_reader.log.trunc";
        static constexpr char const * const JSON_FILE = "src/pg_log_mgr/test/test_reader.json";

        static void SetUpTestSuite() {
            std::vector<std::unique_ptr<ServiceRunner>> service_runners;
            service_runners.emplace_back(std::make_unique<ExceptionRunner>());
            springtail_init_custom(service_runners);

            // check if json command file exists
            if (!std::filesystem::exists(JSON_FILE)) {
                throw Error(std::format("File not found: {}", JSON_FILE));
            }

            // parse the json file and write to log file
            PgLogGenJson log_gen(LOG_FILE);
            log_gen.parse_commands(JSON_FILE);
        }

        static void TearDownTestSuite() {
            springtail_shutdown();
            // remove the log file
            std::filesystem::remove(LOG_FILE);
        }

        void SetUp() override {
            _file_size = std::filesystem::file_size(LOG_FILE);
        }

        void TearDown() override {
            // remove truncated log file if exists
            if (std::filesystem::exists(LOG_FILE_TRUNC)) {
                std::filesystem::remove(LOG_FILE_TRUNC);
            }
        }

        /**
         * Process json command file stored into log file; return last header
         * return mapping offset to commit LSN; offset is ending offset after commit
         */
        std::map<std::uint32_t, LSN_t>
        process_json_cmd_file(const std::filesystem::path &json_file)
        {
            std::map<std::uint32_t, LSN_t> offset_lsn_map;

            // open the new log file
            _fp = ::fopen(LOG_FILE, "r");
            if (_fp == nullptr) {
                throw Error(std::format("Failed to open file: {}", LOG_FILE));
            }

            // iterate through the messages in the log file
            PgMsgStreamHeader header {};
            int offset = 0;

            char msg_buffer[1024];

            // should have full messages
            while (offset < _file_size) {
                // read the header
                _last_header_offset = offset;

                char buffer[PgMsgStreamHeader::SIZE];
                int rc = ::fread(buffer, 1, PgMsgStreamHeader::SIZE, _fp);
                EXPECT_EQ(rc, PgMsgStreamHeader::SIZE);
                header = PgMsgStreamHeader(buffer);

                offset += rc;

                if (header.msg_length > sizeof(msg_buffer)) {
                    throw Error(std::format("Message length {} exceeds buffer size {}",
                                            header.msg_length, sizeof(msg_buffer)));
                }

                rc = ::fread(msg_buffer, 1, header.msg_length, _fp);
                EXPECT_EQ(rc, header.msg_length);
                offset += rc;

                if (msg_buffer[0] == pg_msg::MSG_COMMIT ||
                    msg_buffer[0] == pg_msg::MSG_STREAM_COMMIT) {
                    LSN_t commit_lsn = PgMsgStreamReader::get_commit_lsn(msg_buffer);
                    offset_lsn_map[offset] = commit_lsn;
                }
            }

            ::fclose(_fp);
            _fp = nullptr;

            return offset_lsn_map;
        }

        LSN_t
        find_last_commit_lsn(const std::map<uint32_t, LSN_t> &map, uint32_t offset)
        {
            if (map.empty()) {
                return INVALID_LSN;
            }

            auto it = map.lower_bound(offset);
            if (it == map.end()) {
                // all keys < offset, take the last one
                --it;
            } else if (it->first > offset) {
                if (it == map.begin()) {
                    return INVALID_LSN;
                }
                --it;
            }

            return it->second;
        }

        FILE *_fp = nullptr;
        size_t _file_size = 0;
        size_t _last_header_offset = 0;
    };

    TEST_F(LogRepair_Test, ProcessLogFile)
    {
        process_json_cmd_file(JSON_FILE);
    }

    TEST_F(LogRepair_Test, RepairLogFile)
    {
        auto map = process_json_cmd_file(JSON_FILE);

        // repair the log file
        auto last_lsn = PgMsgStreamReader::scan_log(1, LOG_FILE, false);
        ASSERT_EQ(last_lsn, find_last_commit_lsn(map, _file_size) + 1);
    }

    TEST_F(LogRepair_Test, RepairTruncateLogFile)
    {
        auto map = process_json_cmd_file(JSON_FILE);

        // truncate the log file to partially remove the last message; leaving header + part of message
        auto new_size = _last_header_offset + PgMsgStreamHeader::SIZE + 5;

        // make a copy of the file
        std::filesystem::path tmp_file = std::filesystem::path(LOG_FILE_TRUNC);
        std::filesystem::copy_file(LOG_FILE, tmp_file, std::filesystem::copy_options::overwrite_existing);

        // truncate the file
        std::filesystem::resize_file(tmp_file, new_size);

        // repair the log file
        auto last_lsn = PgMsgStreamReader::scan_log(1, LOG_FILE_TRUNC, true);

        ASSERT_EQ(last_lsn, find_last_commit_lsn(map, new_size) + 1);
    }

    TEST_F(LogRepair_Test, RepairTruncate2LogFile)
    {
        auto map = process_json_cmd_file(JSON_FILE);

        // truncate the log file to fully remove the last message; leaving the header
        auto new_size = _last_header_offset + PgMsgStreamHeader::SIZE;

        // make a copy of the file
        std::filesystem::path tmp_file = std::filesystem::path(LOG_FILE_TRUNC);
        std::filesystem::copy_file(LOG_FILE, tmp_file, std::filesystem::copy_options::overwrite_existing);

        // truncate the file
        std::filesystem::resize_file(tmp_file, new_size);

        // repair the log file
        auto last_lsn = PgMsgStreamReader::scan_log(1, LOG_FILE_TRUNC, true);

        ASSERT_EQ(last_lsn, find_last_commit_lsn(map, new_size) + 1);
    }

    TEST_F(LogRepair_Test, RepairTruncate3LogFile)
    {
        auto map = process_json_cmd_file(JSON_FILE);

        // truncate the log file to fully remove the last message; leaving part of the header
        auto new_size = _last_header_offset + 5;

        // make a copy of the file
        std::filesystem::path tmp_file = std::filesystem::path(LOG_FILE_TRUNC);
        std::filesystem::copy_file(LOG_FILE, tmp_file, std::filesystem::copy_options::overwrite_existing);

        // truncate the file
        std::filesystem::resize_file(tmp_file, new_size);

        // repair the log file
        auto last_lsn = PgMsgStreamReader::scan_log(1, LOG_FILE_TRUNC, true);

        // for log gen, the start_lsn is the same as the previous end_lsn
        ASSERT_EQ(last_lsn, find_last_commit_lsn(map, new_size) + 1);
    }
} // namespace