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
        static constexpr char const * const JSON_FILE = "test_reader.json";

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
            // create a new log file
        }

        void TearDown() override {
            // remove truncated log file if exists
            if (std::filesystem::exists(LOG_FILE_TRUNC)) {
                std::filesystem::remove(LOG_FILE_TRUNC);
            }
        }

        /** Process json command file stored into log file; return last header */
        PgMsgStreamHeader process_json_cmd_file(const std::filesystem::path &json_file)
        {
            // open the new log file
            _fp = ::fopen(LOG_FILE, "r");
            if (_fp == nullptr) {
                throw Error(std::format("Failed to open file: {}", LOG_FILE));
            }

            // iterate through the messages in the log file
            PgMsgStreamHeader header {};
            auto file_size = std::filesystem::file_size(LOG_FILE);
            int offset = 0;

            // should have full messages
            while (offset < file_size) {
                // read the header
                char buffer[PgMsgStreamHeader::SIZE];
                int rc = ::fread(buffer, 1, PgMsgStreamHeader::SIZE, _fp);
                EXPECT_EQ(rc, PgMsgStreamHeader::SIZE);
                header = PgMsgStreamHeader(buffer);
                LOG_INFO("Read header: {}", header.to_string());
                offset += rc;

                // seek to the start of the message
                offset += header.msg_length;
                rc = ::fseek(_fp, offset, SEEK_SET);
                EXPECT_EQ(rc, 0);
            }

            ::fclose(_fp);
            _fp = nullptr;

            return header;
        }

        FILE *_fp = nullptr;
    };

    TEST_F(LogRepair_Test, ProcessLogFile)
    {
        process_json_cmd_file(JSON_FILE);
    }

    TEST_F(LogRepair_Test, RepairLogFile)
    {
        PgMsgStreamHeader header = process_json_cmd_file(JSON_FILE);

        // repair the log file
        auto last_lsn = PgMsgStreamReader::scan_log(1, LOG_FILE, false);
        ASSERT_EQ(last_lsn, header.end_lsn + 1);
    }

    TEST_F(LogRepair_Test, RepairTruncateLogFile)
    {
        PgMsgStreamHeader header = process_json_cmd_file(JSON_FILE);

        // truncate the log file to partially remove the last message
        auto file_size = std::filesystem::file_size(LOG_FILE);
        auto new_size = file_size - header.msg_length + 5;

        // make a copy of the file
        std::filesystem::path tmp_file = std::filesystem::path(LOG_FILE_TRUNC);
        std::filesystem::copy_file(LOG_FILE, tmp_file, std::filesystem::copy_options::overwrite_existing);

        // truncate the file
        std::filesystem::resize_file(tmp_file, new_size);

        // repair the log file
        auto last_lsn = PgMsgStreamReader::scan_log(1, LOG_FILE_TRUNC, true);

        // for log gen, the start_lsn is the same as the previous end_lsn
        ASSERT_EQ(last_lsn, header.start_lsn);
    }

    TEST_F(LogRepair_Test, RepairTruncate2LogFile)
    {
        PgMsgStreamHeader header = process_json_cmd_file(JSON_FILE);

        // truncate the log file to fully remove the last message; leaving the header
        auto file_size = std::filesystem::file_size(LOG_FILE);
        auto new_size = file_size - header.msg_length;

        // make a copy of the file
        std::filesystem::path tmp_file = std::filesystem::path(LOG_FILE_TRUNC);
        std::filesystem::copy_file(LOG_FILE, tmp_file, std::filesystem::copy_options::overwrite_existing);

        // truncate the file
        std::filesystem::resize_file(tmp_file, new_size);

        // repair the log file
        auto last_lsn = PgMsgStreamReader::scan_log(1, LOG_FILE_TRUNC, true);

        // for log gen, the start_lsn is the same as the previous end_lsn
        ASSERT_EQ(last_lsn, header.start_lsn);
    }

    TEST_F(LogRepair_Test, RepairTruncate3LogFile)
    {
        PgMsgStreamHeader header = process_json_cmd_file(JSON_FILE);

        // truncate the log file to fully remove the last message; leaving part of the header
        auto file_size = std::filesystem::file_size(LOG_FILE);
        auto new_size = file_size - header.msg_length - 3;

        // make a copy of the file
        std::filesystem::path tmp_file = std::filesystem::path(LOG_FILE_TRUNC);
        std::filesystem::copy_file(LOG_FILE, tmp_file, std::filesystem::copy_options::overwrite_existing);

        // truncate the file
        std::filesystem::resize_file(tmp_file, new_size);

        // repair the log file
        auto last_lsn = PgMsgStreamReader::scan_log(1, LOG_FILE_TRUNC, true);

        // for log gen, the start_lsn is the same as the previous end_lsn
        ASSERT_EQ(last_lsn, header.start_lsn);
    }
} // namespace