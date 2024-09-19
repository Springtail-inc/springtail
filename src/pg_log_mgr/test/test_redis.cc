#include <gtest/gtest.h>
#include <set>
#include <string>
#include <iostream>

#include <common/common.hh>

#include <pg_log_mgr/pg_redis_xact.hh>

using namespace springtail;
using namespace springtail::pg_log_mgr;

namespace {

    TEST(PgRedisXactValue_Test, Constructor) {
        std::filesystem::path begin_path = "/tmp/begin.log";
        std::filesystem::path commit_path = "/tmp/commit.log";
        uint64_t db_id = 1;
        uint64_t begin_offset = 100;
        uint64_t commit_offset = 200;
        uint64_t xact_lsn = 1000;
        uint64_t xid = 5;
        uint32_t pg_xid = 100;
        std::set<uint32_t> aborted_xids = {10, 20};

        PgXactMsg xact_msg(begin_path, commit_path, db_id, begin_offset, commit_offset,
                              xact_lsn, xid, pg_xid, aborted_xids);

        auto &xact = std::get<PgXactMsg::XactMsg>(xact_msg.msg);

        ASSERT_EQ(xact.begin_path, begin_path);
        ASSERT_EQ(xact.commit_path, commit_path);
        ASSERT_EQ(xact.db_id, db_id);
        ASSERT_EQ(xact.begin_offset, begin_offset);
        ASSERT_EQ(xact.commit_offset, commit_offset);
        ASSERT_EQ(xact.xact_lsn, xact_lsn);
        ASSERT_EQ(xact.xid, xid);
        ASSERT_EQ(xact.pg_xid, pg_xid);
        ASSERT_EQ(xact.aborted_xids, aborted_xids);
    }

    TEST(PgRedisXactValue_Test, ToString)
    {
        std::filesystem::path begin_path = "/tmp/begin.log";
        std::filesystem::path commit_path = "/tmp/commit.log";
        uint64_t db_id = 1;
        uint64_t begin_offset = 100;
        uint64_t commit_offset = 200;
        uint64_t xact_lsn = 1000;
        uint64_t xid = 5;
        uint32_t pg_xid = 100;
        std::set<uint32_t> aborted_xids = {10, 20};

        PgXactMsg xact_msg(begin_path, commit_path, db_id, begin_offset, commit_offset,
                           xact_lsn, xid, pg_xid, aborted_xids);

        std::string str = static_cast<std::string>(xact_msg);

        std::cout << str << std::endl;

        PgXactMsg xact_msg2(str);
        auto &xact2 = std::get<PgXactMsg::XactMsg>(xact_msg2.msg);

        ASSERT_EQ(xact2.begin_path, begin_path);
        ASSERT_EQ(xact2.commit_path, commit_path);
        ASSERT_EQ(xact2.db_id, db_id);
        ASSERT_EQ(xact2.begin_offset, begin_offset);
        ASSERT_EQ(xact2.commit_offset, commit_offset);
        ASSERT_EQ(xact2.xact_lsn, xact_lsn);
        ASSERT_EQ(xact2.xid, xid);
        ASSERT_EQ(xact2.pg_xid, pg_xid);
        ASSERT_EQ(xact2.aborted_xids, aborted_xids);
    }
}
