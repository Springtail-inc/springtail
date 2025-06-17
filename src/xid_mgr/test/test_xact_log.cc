#include <gtest/gtest.h>
#include <vector>

#include <common/init.hh>

#include <xid_mgr/pg_xact_log_reader.hh>
#include <xid_mgr/pg_xact_log_writer.hh>

using namespace springtail;
using namespace springtail::xid_mgr;

namespace {
    class XactLogReadWrite_Test : public testing::Test {
    protected:
        static void SetUpTestSuite()
        {
            std::vector<std::unique_ptr<ServiceRunner>> service_runners;
            service_runners.emplace_back(std::make_unique<ExceptionRunner>());
            service_runners.emplace_back(std::make_unique<LoggingRunner>(std::nullopt, std::nullopt, std::nullopt));
            springtail_init_custom(service_runners);
        }

        static void TearDownTestSuite()
        {
            springtail_shutdown();
        }

        static inline const std::filesystem::path log_path{"/tmp/test_xlog_mmap"};

        void SetUp() override
        {
            // remove the directory
            std::filesystem::remove_all(log_path);
            // make a directory in /tmp/
            std::filesystem::create_directory(log_path);
        }

        void TearDown() override
        {
            // remove the directory
            std::filesystem::remove_all(log_path);
        }

        static uint64_t get_log_timestamp()
        {
            // Get the current time from the system clock
            auto now = std::chrono::system_clock::now();

            // Convert the current time to time since epoch
            auto duration = now.time_since_epoch();

            // Convert duration to milliseconds
            return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        }
    };

    TEST_F(XactLogReadWrite_Test, SimpleTest) {
        // no files has been written yet
        {
            PgXactLogReader reader(log_path);
            ASSERT_FALSE(reader.begin());
            ASSERT_FALSE(reader.next());
        }

        // write first
        {
            PgXactLogWriter writer(log_path);
            uint64_t timestamp = get_log_timestamp();
            writer.rotate(timestamp);

            writer.log(12, 11, true);
            writer.log(13, 12, true);
        }

        // call next() without calling begin()
        {
            PgXactLogReader reader(log_path);
            ASSERT_FALSE(reader.next());
            ASSERT_FALSE(reader.next());
        }

        {
            PgXactLogReader reader(log_path);

            // normal call flow
            ASSERT_TRUE(reader.begin());

            ASSERT_EQ(reader.get_pg_xid(), 12);
            ASSERT_EQ(reader.get_xid(), 11);

            ASSERT_TRUE(reader.next());

            ASSERT_EQ(reader.get_pg_xid(), 13);
            ASSERT_EQ(reader.get_xid(), 12);

            // call next twice after the end of data is reached
            ASSERT_FALSE(reader.next());
            ASSERT_FALSE(reader.next());

            // call begin() twice
            ASSERT_TRUE(reader.begin());
            ASSERT_EQ(reader.get_pg_xid(), 12);
            ASSERT_EQ(reader.get_xid(), 11);

            ASSERT_TRUE(reader.begin());
            ASSERT_EQ(reader.get_pg_xid(), 12);
            ASSERT_EQ(reader.get_xid(), 11);
        }
    }

    TEST_F(XactLogReadWrite_Test, MultiplePagesTest)
    {
        uint32_t iterations = 640;
        // write first
        {
            uint64_t initial_xid = 10;
            PgXactLogWriter writer(log_path);
            uint64_t timestamp = get_log_timestamp();
            writer.rotate(timestamp);

            // write 2.5 pages of data
            for (uint32_t i = 0; i < iterations; i++) {
                uint32_t pg_xid = initial_xid;
                writer.log(pg_xid, (++initial_xid), true);
            }
        }

        {
            PgXactLogReader reader(log_path);
            uint64_t initial_xid = 10;

            ASSERT_TRUE(reader.begin());
            do {
                ASSERT_EQ(reader.get_pg_xid(), initial_xid);
                ASSERT_EQ(reader.get_xid(), initial_xid + 1);
                initial_xid++;
            } while(reader.next());
            ASSERT_EQ(initial_xid, iterations + 10);
        }
    }

    TEST_F(XactLogReadWrite_Test, MultipleFilesTest)
    {
        std::vector<uint32_t> iterations{640, 512, 384, 256, 128};
        for (auto current_iterations: iterations) {
            uint64_t current_xid = 10;
            {
                PgXactLogWriter writer(log_path);

                for (uint32_t i = 0; i < 3; i++) {
                    uint64_t timestamp = get_log_timestamp();
                    writer.rotate(timestamp);
                    for (uint32_t j = 0; j < current_iterations; j++) {
                        uint32_t pg_xid = current_xid;
                        writer.log(pg_xid, (++current_xid), true);
                    }
                    usleep(250000);
                }
            }
            {
                PgXactLogReader reader(log_path);
                uint64_t last_xid = 10;

                ASSERT_TRUE(reader.begin());
                do {
                    ASSERT_EQ(reader.get_pg_xid(), last_xid);
                    ASSERT_EQ(reader.get_xid(), ++last_xid);
                } while(reader.next());
                ASSERT_EQ(last_xid, current_iterations * 3 + 10);
                ASSERT_EQ(last_xid, current_xid);
            }
            std::filesystem::remove_all(log_path);
        }
    }

    TEST_F(XactLogReadWrite_Test, EmptyPageTest)
    {
        // test empty first page
        {
            PgXactLogWriter writer(log_path);

            for (uint32_t i = 0; i < 3; i++) {
                uint64_t timestamp = get_log_timestamp();
                writer.rotate(timestamp);
                usleep(500000);
            }
            writer.log(12, 11, true);
            writer.log(13, 12, true);
        }
        PgXactLogReader reader(log_path);

        // normal call flow
        ASSERT_TRUE(reader.begin());

        ASSERT_EQ(reader.get_pg_xid(), 12);
        ASSERT_EQ(reader.get_xid(), 11);

        ASSERT_TRUE(reader.next());

        ASSERT_EQ(reader.get_pg_xid(), 13);
        ASSERT_EQ(reader.get_xid(), 12);

        ASSERT_FALSE(reader.next());
    }

    TEST_F(XactLogReadWrite_Test, SimpleRecoveryTest)
    {
        uint64_t last_timestamp = 0;
        uint32_t iterations = 384;
        uint64_t current_xid = 10;
        {
            PgXactLogWriter writer(log_path);

            for (uint32_t i = 0; i < 3; i++) {
                last_timestamp = get_log_timestamp();
                writer.rotate(last_timestamp);
                for (uint32_t j = 0; j < iterations; j++) {
                    uint32_t pg_xid = current_xid;
                    writer.log(pg_xid, (++current_xid), true);
                }
                usleep(250000);
            }
        }
        ASSERT_EQ(current_xid, iterations * 3 + 10);
        {
            PgXactLogWriter::set_last_xid_in_storage(log_path, current_xid - 20, true);
            PgXactLogReader reader(log_path);
            uint64_t found_xid = 10;
            ASSERT_TRUE(reader.begin());
            do {
                ASSERT_EQ(reader.get_pg_xid(), found_xid);
                ASSERT_EQ(reader.get_xid(), found_xid + 1);
                found_xid++;
            } while(reader.next());
            ASSERT_EQ(found_xid, current_xid - 20);
        }
        {
            current_xid -= 20;
            PgXactLogWriter writer(log_path);
            writer.rotate(last_timestamp);
            for (uint32_t j = 0; j < iterations; j++) {
                uint32_t pg_xid = current_xid;
                writer.log(pg_xid, (++current_xid), true);
            }
        }
        {
            PgXactLogReader reader(log_path);
            uint64_t last_xid = 10;

            ASSERT_TRUE(reader.begin());
            do {
                ASSERT_EQ(reader.get_pg_xid(), last_xid);
                ASSERT_EQ(reader.get_xid(), ++last_xid);
            } while(reader.next());
            ASSERT_EQ(last_xid, iterations * 4 - 10);
            ASSERT_EQ(last_xid, current_xid);
        }
    }

    TEST_F(XactLogReadWrite_Test, MultipleLogRecoveryTest)
    {
        uint64_t first_timestamp = 0;
        uint64_t second_timestamp = 0;
        uint64_t third_timestamp = 0;
        uint64_t current_xid = 10;
        {
            PgXactLogWriter writer(log_path);

            // first log file
            first_timestamp = get_log_timestamp();
            writer.rotate(first_timestamp);
            writer.log(101, 11, true);
            writer.log(102, 12, true);
            writer.log(103, 13, true);
            writer.log(104, 14, true);
            writer.log(105, 15, true);
            usleep(250000);

            // second log file
            second_timestamp = get_log_timestamp();
            writer.rotate(second_timestamp);
            usleep(250000);

            // third log file
            third_timestamp = get_log_timestamp();
            writer.rotate(third_timestamp);
            writer.log(106, 16, true);
            writer.log(107, 17, true);
            writer.log(108, 18, true);
            writer.log(109, 19, true);
            writer.log(110, 20, true);
        }

        current_xid = 13;
        PgXactLogWriter::set_last_xid_in_storage(log_path, current_xid, true);
        {
            PgXactLogReader reader(log_path);
            ASSERT_TRUE(reader.begin());
            ASSERT_EQ(reader.get_pg_xid(), 101);
            ASSERT_EQ(reader.get_xid(), 11);
            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 102);
            ASSERT_EQ(reader.get_xid(), 12);
            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 103);
            ASSERT_EQ(reader.get_xid(), 13);
            ASSERT_FALSE(reader.next());
        }
        {
            PgXactLogWriter writer(log_path);

            writer.rotate(first_timestamp);
            writer.log(105, 15, true);
            writer.log(106, 16, true);

            writer.rotate(second_timestamp);
            writer.log(110, 20, true);
            writer.log(111, 21, true);
            writer.log(113, 23, true);
            writer.log(115, 25, true);

            writer.rotate(third_timestamp);
            writer.log(120, 30, true);
            writer.log(121, 31, true);
            writer.log(123, 33, true);
            writer.log(125, 35, true);
        }
        {
            PgXactLogReader reader(log_path);

            ASSERT_TRUE(reader.begin());
            ASSERT_EQ(reader.get_pg_xid(), 101);
            ASSERT_EQ(reader.get_xid(), 11);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 102);
            ASSERT_EQ(reader.get_xid(), 12);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 103);
            ASSERT_EQ(reader.get_xid(), 13);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 105);
            ASSERT_EQ(reader.get_xid(), 15);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 106);
            ASSERT_EQ(reader.get_xid(), 16);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 110);
            ASSERT_EQ(reader.get_xid(), 20);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 111);
            ASSERT_EQ(reader.get_xid(), 21);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 113);
            ASSERT_EQ(reader.get_xid(), 23);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 115);
            ASSERT_EQ(reader.get_xid(), 25);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 120);
            ASSERT_EQ(reader.get_xid(), 30);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 121);
            ASSERT_EQ(reader.get_xid(), 31);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 123);
            ASSERT_EQ(reader.get_xid(), 33);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 125);
            ASSERT_EQ(reader.get_xid(), 35);

            ASSERT_FALSE(reader.next());
        }
    }

    TEST_F(XactLogReadWrite_Test, LogRecoveryWithMissingXidsTest)
    {
        uint64_t log_timestamp = 0;

        {
            PgXactLogWriter writer(log_path);

            log_timestamp = get_log_timestamp();
            writer.rotate(log_timestamp);

            writer.log(101, 11, true);
            writer.log(102, 12, true);
            writer.log(103, 13, true);
            writer.log(104, 14, true);
            writer.log(106, 16, true);
            writer.log(107, 17, true);
            writer.log(108, 18, true);
            writer.log(109, 19, true);
            writer.log(110, 20, true);
        }
        PgXactLogWriter::set_last_xid_in_storage(log_path, 15, true);
        {
            PgXactLogReader reader(log_path);

            ASSERT_TRUE(reader.begin());
            ASSERT_EQ(reader.get_pg_xid(), 101);
            ASSERT_EQ(reader.get_xid(), 11);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 102);
            ASSERT_EQ(reader.get_xid(), 12);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 103);
            ASSERT_EQ(reader.get_xid(), 13);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 104);
            ASSERT_EQ(reader.get_xid(), 14);

            ASSERT_FALSE(reader.next());
        }

        {
            // recover from the xid that does not exist
            PgXactLogWriter writer(log_path);
            writer.rotate(log_timestamp);

            writer.log(111, 21, true);
            writer.log(112, 22, true);
            writer.log(113, 23, true);
            writer.log(114, 24, true);
            writer.log(115, 25, true);
        }
        {
            PgXactLogReader reader(log_path);

            ASSERT_TRUE(reader.begin());
            ASSERT_EQ(reader.get_pg_xid(), 101);
            ASSERT_EQ(reader.get_xid(), 11);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 102);
            ASSERT_EQ(reader.get_xid(), 12);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 103);
            ASSERT_EQ(reader.get_xid(), 13);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 104);
            ASSERT_EQ(reader.get_xid(), 14);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 111);
            ASSERT_EQ(reader.get_xid(), 21);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 112);
            ASSERT_EQ(reader.get_xid(), 22);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 113);
            ASSERT_EQ(reader.get_xid(), 23);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 114);
            ASSERT_EQ(reader.get_xid(), 24);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 115);
            ASSERT_EQ(reader.get_xid(), 25);

            ASSERT_FALSE(reader.next());
        }
    }

    TEST_F(XactLogReadWrite_Test, LogRecoveryWithCleanupTest)
    {
        uint64_t log_timestamp = 0;

        {
            PgXactLogWriter writer(log_path);

            log_timestamp = get_log_timestamp();
            writer.rotate(log_timestamp);

            writer.log(101, 11, true);
            writer.log(102, 12, true);
            writer.log(103, 13, true);
            writer.log(104, 14, true);
            writer.log(106, 16, true);
            writer.log(107, 17, true);
            writer.log(108, 18, true);
            writer.log(109, 19, false);
            writer.log(110, 20, false);
            writer.flush();
        }
        PgXactLogWriter::set_last_xid_in_storage(log_path, std::numeric_limits<uint64_t>::max(), true);
        {
            PgXactLogWriter writer(log_path);
            EXPECT_EQ(writer.get_last_xid(), 18);
        }
        {
            PgXactLogReader reader(log_path);

            ASSERT_TRUE(reader.begin());
            ASSERT_EQ(reader.get_pg_xid(), 101);
            ASSERT_EQ(reader.get_xid(), 11);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 102);
            ASSERT_EQ(reader.get_xid(), 12);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 103);
            ASSERT_EQ(reader.get_xid(), 13);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 104);
            ASSERT_EQ(reader.get_xid(), 14);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 106);
            ASSERT_EQ(reader.get_xid(), 16);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 107);
            ASSERT_EQ(reader.get_xid(), 17);

            ASSERT_TRUE(reader.next());
            ASSERT_EQ(reader.get_pg_xid(), 108);
            ASSERT_EQ(reader.get_xid(), 18);

            ASSERT_FALSE(reader.next());
        }
    }
}