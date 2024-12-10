#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <iostream>

#include <stdio.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/exception.hh>
#include <common/concurrent_queue.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>

#include <redis/redis_containers.hh>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/pg_log_reader.hh>
#include <pg_repl/pg_msg_log_gen.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_msg_stream.hh>

using namespace springtail;
using namespace springtail::pg_log_mgr;

namespace {

    /**
     * @brief Wrapper class for testing
     */
    class TestLogMgr : public PgLogMgr {
    public:
        using PgLogMgr::PgLogMgr;
        TestLogMgr(const std::filesystem::path &repl_log_path,
                   const std::filesystem::path &xact_log_path)
            : PgLogMgr(repl_log_path, xact_log_path) // XXX NOTE: hardcodes db_id=1
        {
        }

        PgLogWriterPtr create_log_writer()
        {
            return _create_repl_logger();
        }

        PgXactLogWriterPtr create_xact_log_writer()
        {
            return _create_xact_logger();
        }

        void process_xact(PgTransactionPtr xact)
        {
            _process_xact(xact);
        }
    };

    class LogReader_Test : public ::testing::Test {
    protected:
        static constexpr char const * const LOG_FILE = "/tmp/test_log_reader.log";
        static constexpr char const * const JSON_FILE = "test_reader.json";
        static constexpr char const * const XACT_LOG_DIR = "/tmp/test_xact_log";

        void SetUp() override {
            // code here will execute just before the test ensues
            springtail_init();

            // create a new log file
            _log_file = std::filesystem::path(LOG_FILE);

            // make a directory in /tmp/
            std::filesystem::create_directory(XACT_LOG_DIR);

            // init log mgr, must come after springtail_init() due to redis system
            // property initialization
            _log_mgr = std::make_shared<TestLogMgr>(XACT_LOG_DIR, XACT_LOG_DIR);
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

            // remove the directory
            std::filesystem::remove_all(XACT_LOG_DIR);

            // clean up redis
            if (using_redis) {
                RedisMgr::get_instance()->get_client()->flushdb();
            }
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
            char buffer[PgMsgStreamHeader::SIZE];
            if (::fread(buffer, PgMsgStreamHeader::SIZE, 1, _fp) <= 0) {
                return 0;
            }

            // decode header
            PgMsgStreamHeader header(buffer);
            SPDLOG_DEBUG("{}\n", header.to_string());
            EXPECT_EQ(header.magic, PgMsgStreamHeader::PG_LOG_MAGIC);

            offset = ::ftell(_fp) - PgMsgStreamHeader::SIZE;

            ::fseek(_fp, header.msg_length, SEEK_CUR);

            return header.msg_length;
        }

        bool using_redis = false;
        FILE *_fp = nullptr;
        std::filesystem::path _log_file{LOG_FILE};
        PgLogReader::PgTransactionQueuePtr _queue = std::make_shared<ConcurrentQueue<PgTransaction>>();
        PgLogReader _log_reader{1, _queue}; // note: hard-codes DB ID as 1
        std::vector<PgTransactionPtr> _xact_list;
        std::shared_ptr<TestLogMgr> _log_mgr;
    };

    TEST_F(LogReader_Test, ProcessLog)
    {
        // create a new log file
        process_json_cmd_file(std::filesystem::path(JSON_FILE));

        // read the header
        uint64_t offset = 0;
        uint32_t msg_length;
        while ((msg_length = read_header(offset)) > 0) {
            // process the log
            _log_reader.process_log(_log_file, offset, 1);
        }

        for (int i = 0; i < _xact_list.size(); i++) {
            PgTransactionPtr xact = _xact_list[i];
            std::cout << fmt::format("xact: {}\n", xact->type);
        }

        // xact list is from the log generator
        ASSERT_EQ(_xact_list.size(), _queue->size());

        // pop items from queue and validate
        PgTransactionPtr xact;
        PgTransactionPtr xact_cmp;

        // validate the transactions based on what the log generator created
        for (int i = 0; i < _xact_list.size(); i++) {
            xact = _queue->pop();      // pop from queue from log reader
            xact_cmp = _xact_list[i];  // from log generator
            ASSERT_NE(xact, nullptr);
            EXPECT_EQ(xact->type, xact_cmp->type);
            EXPECT_EQ(xact->xid, xact_cmp->xid);
            EXPECT_EQ(xact->begin_offset, xact_cmp->begin_offset);
            if (xact->type == PgTransaction::TYPE_COMMIT) {
                EXPECT_EQ(xact->commit_offset, xact_cmp->commit_offset);
                EXPECT_EQ(xact->oids.size(), xact_cmp->oids.size());
                EXPECT_EQ(xact->oids, xact_cmp->oids);
            }
        }

        EXPECT_TRUE(_queue->empty());
    }

    TEST_F(LogReader_Test, XactHandling)
    {
        // See if redis is enabled
        try {
            RedisMgr::get_instance()->get_client()->ping();
            using_redis = true;
        } catch (const std::exception &e) {
            using_redis = false;
            GTEST_SKIP() << "Redis is not running, skipping test";
        }

        // init the redis queue
        RedisQueue<PgXactMsg> queue(fmt::format(redis::QUEUE_PG_TRANSACTIONS,
                                                Properties::get_db_instance_id()));

        queue.clear();

        // initialize the log mgr
        PgXactLogWriterPtr xact_writer = _log_mgr->create_xact_log_writer();

        // create a new log file
        process_json_cmd_file(std::filesystem::path(JSON_FILE));

        std::vector<PgTransactionPtr> xact_list;
        uint64_t offset = 0;
        uint32_t msg_length;

        // loop reading the header and process the log
        while ((msg_length = read_header(offset)) > 0) {
            // process the log
            _log_reader.process_log(_log_file, offset, 1);

            // process the transactions resulting from log message block
            while (!_queue->empty()) {
                PgTransactionPtr xact = _queue->pop();

                // process the transaction
                _log_mgr->process_xact(xact);

                // store it for comparison if of type commit
                if (xact->type == PgTransaction::TYPE_COMMIT) {
                    xact_list.push_back(xact);
                }
            }
        }

        // fetch the redis xacts and compare
        auto xacts = queue.range(0, -1);
        std::reverse(xacts.begin(), xacts.end()); // note: data stored in reverse order in redis

        ASSERT_EQ(xact_list.size(), xacts.size());

        for (int i = 0; i < xact_list.size(); i++) {
            auto &xact_msg = std::get<PgXactMsg::XactMsg>(xacts[i].msg);
            EXPECT_EQ(xact_msg.pg_xid, xact_list[i]->xid);
            EXPECT_EQ(xact_msg.xid, xact_list[i]->springtail_xid);
            EXPECT_EQ(xact_msg.begin_offset, xact_list[i]->begin_offset);
            EXPECT_EQ(xact_msg.begin_path, xact_list[i]->begin_path);
            EXPECT_EQ(xact_msg.commit_offset, xact_list[i]->commit_offset);
            EXPECT_EQ(xact_msg.commit_path, xact_list[i]->commit_path);
            EXPECT_EQ(xact_msg.aborted_xids.size(), xact_list[i]->aborted_xids.size());
        }
    }

} // namespace
