#include <gtest/gtest.h>
#include <filesystem>

#include <pg_log_mgr/pg_xact_log_reader.hh>
#include <pg_log_mgr/pg_xact_log_writer.hh>

using namespace springtail;

namespace {
    class XactLogRW_Test : public testing::Test {
    protected:
        void SetUp() override {
            // remove the directory
            std::filesystem::remove_all("/tmp/test_xlog");
            // make a directory in /tmp/
            std::filesystem::create_directory("/tmp/test_xlog");
        }

        void TearDown() override {
            // remove the directory
            std::filesystem::remove_all("/tmp/test_xlog/");
        }
    };

    TEST_F(XactLogRW_Test, XactLogWriter) {
        std::filesystem::path p = "/tmp/test_xlog/test_1.log";
        PgXactLogWriter writer(p);

        PgTransactionPtr xact = std::make_shared<PgTransaction>();
        xact->type = PgTransaction::TYPE_COMMIT;
        xact->xid = 1;
        xact->begin_offset = 1;
        xact->commit_offset = 100;
        xact->springtail_xid = 5;
        xact->xact_lsn = 1000;
        xact->begin_path = std::filesystem::path("/tmp/test_xlog/test_1.log");
        xact->commit_path = std::filesystem::path("/tmp/test_xlog/test_2.log");
        xact->oids.insert(5);
        xact->oids.insert(7);
        xact->aborted_xids.insert(10);
        xact->aborted_xids.insert(20);

        writer.log_commit(xact);

        PgTransactionPtr xact2 = std::make_shared<PgTransaction>();
        xact2->type = PgTransaction::TYPE_STREAM_START;
        xact2->xid = 2;
        xact2->begin_offset = 8;
        xact2->xact_lsn = 2000;
        xact2->begin_path = std::filesystem::path("/tmp/test_xlog/test_3.log");

        writer.log_stream_msg(xact2);

        writer.close();

        // read the log file and verify
        PgXactLogReader reader("/tmp/test_xlog", "test_", ".log");
        reader.scan_all_files(0);

        auto xacts = reader.get_xact_list();
        ASSERT_EQ(xacts.size(), 1);

        ASSERT_EQ(xacts[0]->type, PgTransaction::TYPE_COMMIT);
        ASSERT_EQ(xacts[0]->xid, 1);
        ASSERT_EQ(xacts[0]->begin_offset, 1);
        ASSERT_EQ(xacts[0]->commit_offset, 100);
        ASSERT_EQ(xacts[0]->springtail_xid, 5);
        ASSERT_EQ(xacts[0]->xact_lsn, 1000);
        ASSERT_EQ(xacts[0]->begin_path, std::filesystem::path("/tmp/test_xlog/test_1.log"));
        ASSERT_EQ(xacts[0]->commit_path, std::filesystem::path("/tmp/test_xlog/test_2.log"));
        ASSERT_EQ(xacts[0]->oids.size(), 2);
        ASSERT_EQ(xacts[0]->oids.count(5), 1);
        ASSERT_EQ(xacts[0]->oids.count(7), 1);
        ASSERT_EQ(xacts[0]->aborted_xids.size(), 2);
        ASSERT_EQ(xacts[0]->aborted_xids.count(10), 1);
        ASSERT_EQ(xacts[0]->aborted_xids.count(20), 1);

        auto xact_map = reader.get_stream_map();
        ASSERT_EQ(xact_map.size(), 1);
        ASSERT_EQ(xact_map[2]->type, PgTransaction::TYPE_STREAM_START);
        ASSERT_EQ(xact_map[2]->xid, 2);
        ASSERT_EQ(xact_map[2]->begin_offset, 8);
        ASSERT_EQ(xact_map[2]->xact_lsn, 2000);
        ASSERT_EQ(xact_map[2]->begin_path, std::filesystem::path("/tmp/test_xlog/test_3.log"));
    }

}