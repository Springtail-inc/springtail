#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <iostream>

#include <stdio.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/exception.hh>

#include <pg_repl/pg_msg_log_gen.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_msg_stream.hh>

using namespace springtail;

namespace {

    class MsgStreamReader_Test : public ::testing::Test {
    protected:
        static constexpr char const * const LOG_FILE = "/tmp/test_log_reader.log";

        void SetUp() override {
            // code here will execute just before the test ensues
            springtail_init();

            // create a new log file for output log
            _log_file = std::filesystem::path(LOG_FILE);

            // parse json commands and write to log file
            process_json_cmd_file(std::filesystem::path("test_msgs.json"));
        }

        void TearDown() override {
            // code here will be called just after the test completes
            // ok to through exceptions from here if need be

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
            _json_cmds = log_gen.get_json_cmds();
        }

        FILE *_fp = nullptr;
        std::filesystem::path _log_file{LOG_FILE};
        std::vector<nlohmann::json> _json_cmds;
    };

    TEST_F(MsgStreamReader_Test, SkipBlock)
    {
        bool eob=false, eos=false;
        char filter[] = {};

        PgMsgStreamReader reader(_log_file);

        int count = 0;
        while (!eob) {
            // read next message
            PgMsgPtr msg = reader.read_message(filter, eos, eob);
            EXPECT_EQ(msg, nullptr);
            count++;
        }
        EXPECT_EQ(count, 6);
    }

    TEST_F(MsgStreamReader_Test, Offset)
    {
        bool eob=false, eos=false;
        char filter[] = { };

        PgMsgStreamReader reader(_log_file);

        // read next message
        PgMsgPtr msg = reader.read_message(filter, eos, eob);
        EXPECT_EQ(msg, nullptr);
        EXPECT_EQ(reader.header_offset(), 0);
        uint64_t offset = reader.block_end_offset();
        EXPECT_GT(offset, 28);
        EXPECT_NE(offset, reader.offset());

        reader.set_file(_log_file, offset);
        msg = reader.read_message(filter, eos, eob);
        EXPECT_EQ(msg, nullptr);
        EXPECT_EQ(reader.header_offset(), offset);
    }

    TEST_F(MsgStreamReader_Test, SkipStream)
    {
        bool eob=false, eos=false;
        char filter[] = {};

        PgMsgStreamReader reader(_log_file);

        int count = 0;
        while (!eos) {
            // read next message
            PgMsgPtr msg = reader.read_message(filter, eos, eob);
            EXPECT_EQ(msg, nullptr);
            count++;
        }
        EXPECT_EQ(count, 26); // 25 commands + final check for eof
    }

    TEST_F(MsgStreamReader_Test, Decode)
    {
        bool eob=false, eos=false;

        PgMsgStreamReader reader(_log_file);

        int count = 0;
        while (!eos) {
            PgMsgPtr msg = reader.read_message(reader.ALL_MESSAGES, eos, eob);
            if (msg == nullptr) {
                EXPECT_EQ(count, 25);
                continue;
            }
            nlohmann::json &j = _json_cmds[count];
            switch(msg->msg_type) {
                case PgMsgEnum::BEGIN:
                    EXPECT_EQ(j["cmd"], "begin");
                    break;
                case PgMsgEnum::COMMIT:
                    EXPECT_EQ(j["cmd"], "commit");
                    break;
                case PgMsgEnum::STREAM_START:
                    EXPECT_EQ(j["cmd"], "stream start");
                    break;
                case PgMsgEnum::STREAM_COMMIT:
                    EXPECT_EQ(j["cmd"], "stream commit");
                    break;
                case PgMsgEnum::STREAM_ABORT:
                    EXPECT_EQ(j["cmd"], "stream abort");
                    break;
                case PgMsgEnum::STREAM_STOP:
                    EXPECT_EQ(j["cmd"], "stream stop");
                    break;
                case PgMsgEnum::CREATE_TABLE:
                    EXPECT_EQ(j["cmd"], "create table");
                    break;
                case PgMsgEnum::ALTER_TABLE:
                    EXPECT_EQ(j["cmd"], "alter table");
                    break;
                case PgMsgEnum::DROP_TABLE:
                    EXPECT_EQ(j["cmd"], "drop table");
                    break;
                case PgMsgEnum::INSERT:
                    EXPECT_EQ(j["cmd"], "insert");
                    break;
                case PgMsgEnum::UPDATE:
                    EXPECT_EQ(j["cmd"], "update");
                    break;
                case PgMsgEnum::DELETE:
                    EXPECT_EQ(j["cmd"], "delete");
                    break;
                case PgMsgEnum::TRUNCATE:
                    EXPECT_EQ(j["cmd"], "truncate");
                    break;
                default:
                    std::cout << "Unknown message type: " << j["cmd"] << std::endl;
                    EXPECT_TRUE(false);
                    break;
            }
            count++;
        }
    }

} // namespace


