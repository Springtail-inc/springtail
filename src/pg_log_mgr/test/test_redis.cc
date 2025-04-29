#include <gtest/gtest.h>
#include <set>

#include <common/common.hh>

#include <pg_log_mgr/pg_redis_xact.hh>

using namespace springtail;
using namespace springtail::pg_log_mgr;

namespace {

    TEST(PgRedisXactValue_Test, Constructor) {
        uint64_t db_id = 1;
        uint64_t xact_lsn = 1000;
        uint64_t xid = 5;
        uint32_t pg_xid = 100;
        std::set<uint32_t> aborted_xids = {10, 20};

        PgXactMsg xact_msg(db_id, xact_lsn, xid, pg_xid, aborted_xids);

        auto &xact = std::get<PgXactMsg::XactMsg>(xact_msg.msg);

        ASSERT_EQ(xact.db_id, db_id);
        ASSERT_EQ(xact.xact_lsn, xact_lsn);
        ASSERT_EQ(xact.xid, xid);
        ASSERT_EQ(xact.pg_xid, pg_xid);
        ASSERT_EQ(xact.aborted_xids, aborted_xids);
    }
}
