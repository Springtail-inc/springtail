#include <gtest/gtest.h>

#include <common/init.hh>
#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_redis_xact.hh>

using namespace springtail;
using namespace springtail::pg_log_mgr;

namespace {
    class XactLogRW_Test : public testing::Test {
    protected:
        static void SetUpTestSuite() {
            std::vector<std::unique_ptr<ServiceRunner>> service_runners;
            service_runners.emplace_back(std::make_unique<ExceptionRunner>());
            springtail_init_custom(service_runners);
        }

        static void TearDownTestSuite() {
            springtail_shutdown();
        }

        void SetUp() override {
            // nothing to do here
        }

        void TearDown() override {
            // nothing to do here
        }
    };

    TEST_F(XactLogRW_Test, XactTestMsg)
    {
        PgXactMsg msg(1, 1000, 5, 1, {10, 20});

        PgXactMsg::XactMsg msg3 = std::get<PgXactMsg::XactMsg>(msg.msg);

        ASSERT_EQ(msg.type, PgXactMsg::Type::XACT_MSG);
        ASSERT_EQ(msg3.db_id, 1);
        ASSERT_EQ(msg3.xact_lsn, 1000);
        ASSERT_EQ(msg3.xid, 5);
        ASSERT_EQ(msg3.pg_xid, 1);
        ASSERT_EQ(msg3.aborted_xids.size(), 2);

        PgCopyResultPtr copy_res = std::make_shared<PgCopyResult>(43534);
        copy_res->set_snapshot(234598, "1234:2345:3456,7893");
        copy_res->add_table(std::make_shared<PgCopyResult::TableInfo>(54, nullptr, nullptr));
        copy_res->add_table(std::make_shared<PgCopyResult::TableInfo>(67, nullptr, nullptr));

        PgXactMsg msg4(1, copy_res);
        PgXactMsg::TableSyncMsg msg6 = std::get<PgXactMsg::TableSyncMsg>(msg4.msg);

        ASSERT_EQ(msg4.type, PgXactMsg::Type::TABLE_SYNC_MSG);
        ASSERT_EQ(msg6.db_id, 1);
        ASSERT_EQ(msg6.target_xid, 43534);
        ASSERT_EQ(msg6.xmin, 1234);
        ASSERT_EQ(msg6.xmax, 2345);
        ASSERT_EQ(msg6.tids.size(), 2);
        ASSERT_EQ(msg6.tids[0]->table_id, 54);
        ASSERT_EQ(msg6.tids[1]->table_id, 67);
        ASSERT_EQ(msg6.xips.size(), 2);
        ASSERT_EQ(msg6.xips[0], 3456);
        ASSERT_EQ(msg6.xips[1], 7893);
        ASSERT_EQ(msg6.pg_xid, 234598);
    }
}
