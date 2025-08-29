#include <gtest/gtest.h>
#include <filesystem>
#include <iostream>

#include <stdio.h>

#include <common/init.hh>
#include <common/logging.hh>
#include <common/exception.hh>
#include <common/environment.hh>

#include <pg_repl/pg_msg_log_gen.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_msg_stream.hh>

using namespace springtail;

namespace {

    class MsgStreamReader_Test : public ::testing::Test {
    protected:
        static constexpr char const * const LOG_FILE = "/tmp/test_log_reader.log";
        static void SetUpTestSuite()
        {
            // Add two fake databeses with custom include.schemas config
            std::string js = 
            "{ "
            "\"db_instances\": {"
                "\"1234\": {"
                    "\"database_ids\": [\"1\", \"2\", \"3\"]"
                "}"
            "},"
            "\"databases\": "
                "{\"2\": {\"name\": \"springtail\","
                    "\"replication_slot\": \"springtail_slot\","
                    "\"publication_name\": \"springtail_pub\","
                    "\"include\": {"
                        "\"schemas\": [\"test\"]}},"
                "\"3\": {\"name\": \"springtail\","
                    "\"replication_slot\": \"springtail_slot\","
                    "\"publication_name\": \"springtail_pub\","
                    "\"include\": {"
                        "\"schemas\": [\"public\"]}}"
                "}"
            "}";

            ::setenv(environment::ENV_OVERRIDE_JSON, js.c_str(), 1);

            // code here will execute just before the test ensues
            springtail_init_test();
        }

        static void TearDownTestSuite()
        {
            springtail_shutdown();
        }

        void SetUp() override {
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
        bool eos=false;

        PgMsgStreamReader reader({}, _log_file);

        int count = 0;
        while (count < 7) {
            // read next message
            PgMsgPtr msg = reader.read_message({}, eos);
            EXPECT_EQ(msg, nullptr);
            count++;
        }
        EXPECT_EQ(count, 7);
    }

    TEST_F(MsgStreamReader_Test, Offset)
    {
        bool eos=false;
        PgMsgStreamReader reader({}, _log_file);

        // read next message
        PgMsgPtr msg = reader.read_message({}, eos);
        EXPECT_EQ(msg, nullptr);
        EXPECT_EQ(reader.message_offset(), 0);
    }

    TEST_F(MsgStreamReader_Test, SkipStream)
    {
        bool eos=false;

        PgMsgStreamReader reader({}, _log_file);

        int count = 0;
        while (!eos) {
            // read next message
            PgMsgPtr msg = reader.read_message({}, eos);
            EXPECT_EQ(msg, nullptr);
            count++;
        }
        EXPECT_EQ(count, 29); // 27 commands + final check for eof
    }

    TEST_F(MsgStreamReader_Test, Decode)
    {
        bool eos=false;

        PgMsgStreamReader reader(1, _log_file);

        // ignore create/drop/alter/create index for any namespace but "test"
        PgMsgStreamReader ns_reader(2, _log_file);

        // public namespace is hardcoded in the test json
        PgMsgStreamReader pub_reader(3, _log_file);

        int count = 0;
        while (!eos) {
            PgMsgPtr msg = reader.read_message(reader.ALL_MESSAGES, eos);
            if (msg == nullptr) {
                EXPECT_EQ(count, 28);
                continue;
            }
            PgMsgPtr test_msg = ns_reader.read_message(reader.ALL_MESSAGES, eos);
            PgMsgPtr pub_msg = pub_reader.read_message(reader.ALL_MESSAGES, eos);

            nlohmann::json &j = _json_cmds[count];
            switch(msg->msg_type) {
                case PgMsgEnum::BEGIN:
                    EXPECT_EQ(j["cmd"], "begin");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::COMMIT:
                    EXPECT_EQ(j["cmd"], "commit");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::STREAM_START:
                    EXPECT_EQ(j["cmd"], "stream start");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::STREAM_COMMIT:
                    EXPECT_EQ(j["cmd"], "stream commit");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::STREAM_ABORT:
                    EXPECT_EQ(j["cmd"], "stream abort");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::STREAM_STOP:
                    EXPECT_EQ(j["cmd"], "stream stop");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::CREATE_TABLE:
                    EXPECT_EQ(j["cmd"], "create table");
                    EXPECT_EQ(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::ALTER_TABLE:
                    EXPECT_EQ(j["cmd"], "alter table");
                    EXPECT_EQ(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::DROP_TABLE:
                    EXPECT_EQ(j["cmd"], "drop table");
                    EXPECT_EQ(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::CREATE_INDEX:
                    EXPECT_EQ(j["cmd"], "create index");
                    EXPECT_EQ(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::INSERT:
                    EXPECT_EQ(j["cmd"], "insert");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::UPDATE:
                    EXPECT_EQ(j["cmd"], "update");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::DELETE:
                    EXPECT_EQ(j["cmd"], "delete");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::TRUNCATE:
                    EXPECT_EQ(j["cmd"], "truncate");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::ATTACH_PARTITION:
                    EXPECT_EQ(j["cmd"], "attach partition");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
                    break;
                case PgMsgEnum::DETACH_PARTITION:
                    EXPECT_EQ(j["cmd"], "detach partition");
                    EXPECT_NE(test_msg, nullptr);
                    EXPECT_NE(pub_msg, nullptr);
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
