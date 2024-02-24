#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <iostream>

#include <stdio.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/exception.hh>
#include <common/concurrent_queue.hh>

#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/pg_log_reader.hh>
#include <pg_log_mgr/pg_log_gen.hh>
#include <pg_repl/pg_repl_msg.hh>

using namespace springtail;

namespace {

    class LogReader_Test : public ::testing::Test {
    protected:
        static constexpr char const * const LOG_FILE = "/tmp/test_log_reader.log";

        void SetUp() override {
            // code here will execute just before the test ensues
            springtail_init();

            // create a new log file
            _log_file = std::filesystem::path(LOG_FILE);
        }

        void TearDown() override {
            // code here will be called just after the test completes
            // ok to through exceptions from here if need be

            // close the file
            if (_fp != nullptr) {
                ::fclose(_fp);
            }

            // remove the log file
            std::filesystem::remove(_log_file);
        }

        /** Process json command file stored into log file*/
        void process_json_cmd_file(const std::filesystem::path &json_file)
        {
            // check if json command file exists
            if (!std::filesystem::exists(json_file)) {
                throw Error("File not found: " + json_file.string());
            }

            // parse the json file and write to log file
            PgLogGenJson log_gen(_log_file);
            log_gen.parse_commands(json_file);
            _xact_list = log_gen.get_xact_list();
            // PgLogGen::dump_file(_log_file);

            // if the file was open then close it
            if (_fp != nullptr) {
                ::fclose(_fp);
            }

            // open the new log file
            _fp = ::fopen(_log_file.c_str(), "r");
            if (_fp == nullptr) {
                throw Error("Failed to open file: " + _log_file.string());
            }
        }

        /** Read the header from the log file, return message length, set offset */
        uint32_t read_header(uint64_t &offset)
        {
            // read in the file from *fp until eof
            char buffer[PgLogWriter::PG_LOG_HDR_BYTES];
            if (::fread(buffer, PgLogWriter::PG_LOG_HDR_BYTES, 1, _fp) <= 0) {
                return 0;
            }

            // decode header
            PgLogWriter::PgLogHeader header(buffer);
            SPDLOG_DEBUG("{}\n", header.to_string());
            EXPECT_EQ(header.magic, PgLogWriter::PG_LOG_MAGIC);

            offset = ::ftell(_fp) - PgLogWriter::PG_LOG_HDR_BYTES;

            ::fseek(_fp, header.msg_length, SEEK_CUR);

            return header.msg_length;
        }

        FILE *_fp = nullptr;
        std::filesystem::path _log_file{LOG_FILE};
        PgLogReader::PgTransactionQueuePtr _queue = std::make_shared<ConcurrentQueue<PgReplMsgStream::PgTransaction>>();
        PgLogReader _log_reader{_queue};
        std::vector<PgReplMsgStream::PgTransactionPtr> _xact_list;
    };

    TEST_F(LogReader_Test, ProcessLog)
    {
        // create a new log file
        process_json_cmd_file(std::filesystem::path("test_reader.json"));

        // read the header
        uint64_t offset = 0;
        uint32_t msg_length;
        while ((msg_length = read_header(offset)) > 0) {
            // process the log
            _log_reader.process_log(_log_file, offset, 1);
        }

        ASSERT_EQ(_xact_list.size(), 6);

        // pop items from queue and validate
        PgReplMsgStream::PgTransactionPtr xact;
        PgReplMsgStream::PgTransactionPtr xact_cmp;

        // validate the transactions based on what the log generator created
        for (int i = 0; i < _xact_list.size(); i++) {
            xact = _queue->pop();
            xact_cmp = _xact_list[i];
            ASSERT_NE(xact, nullptr);
            EXPECT_EQ(xact->xid, xact_cmp->xid);
            EXPECT_EQ(xact->begin_offset, xact_cmp->begin_offset);
            EXPECT_EQ(xact->commit_offset, xact_cmp->commit_offset);
        }

        EXPECT_TRUE(_queue->empty());
    }

}