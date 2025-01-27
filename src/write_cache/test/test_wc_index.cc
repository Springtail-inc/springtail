#include <fmt/core.h>
#include <gtest/gtest.h>

#include <cassert>

#include <common/common.hh>
#include <common/threaded_test.hh>
#include <common/tracking_allocator.hh>
#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_index_node.hh>
#include <write_cache/write_cache_table_set.hh>

using namespace springtail;

namespace {
class WriteCacheIndexTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        // Init springtail
        springtail_init();

        // Initialize WriteCacheIndex with 4 partitions
        index = std::make_shared<WriteCacheIndex>(4);
    }

    WriteCacheIndexPtr index;
};

TEST_F(WriteCacheIndexTest, AddExtent)
{
    uint64_t tid = 1;
    uint64_t pg_xid = 100;
    uint64_t xid = 150;
    uint64_t lsn = 200;
    ExtentHeader header(ExtentType(), xid, 100);
    ExtentPtr data = std::make_shared<Extent>(header);

    index->add_extent(tid, pg_xid, lsn, data);
    index->commit(pg_xid, xid);

    uint64_t cursor = 0;
    auto extents = index->get_extents(tid, xid, 1, cursor);
    ASSERT_EQ(extents.size(), 1);
    EXPECT_EQ(cursor, 1);
    EXPECT_EQ(extents[0]->xid, xid);
    EXPECT_EQ(extents[0]->xid_seq, lsn);
    EXPECT_EQ(extents[0]->data, data);
}

TEST_F(WriteCacheIndexTest, CommitAndGetTids)
{
    uint64_t tid = 1;
    uint64_t pg_xid = 100;
    uint64_t xid = 150;
    uint64_t lsn = 200;

    ExtentHeader header(ExtentType(), xid, 100);
    ExtentPtr data = std::make_shared<Extent>(header);

    index->add_extent(tid, pg_xid, lsn, data);
    index->commit(pg_xid, xid);

    uint64_t cursor = 0;
    auto tids = index->get_tids(xid, 1, cursor);
    EXPECT_EQ(cursor, 1);
    ASSERT_EQ(tids.size(), 1);
    EXPECT_EQ(tids[0], tid);
}

TEST_F(WriteCacheIndexTest, DropTable)
{
    uint64_t tid = 1;
    uint64_t pg_xid = 100;
    ExtentHeader header(ExtentType(), pg_xid, 100);
    ExtentPtr data = std::make_shared<Extent>(header);

    index->add_extent(tid, pg_xid, 200, data);
    index->drop_table(tid, pg_xid);

    uint64_t cursor = 0;
    auto extents = index->get_extents(tid, pg_xid, 1, cursor);
    EXPECT_EQ(cursor, 0);
    EXPECT_TRUE(extents.empty());
}

TEST_F(WriteCacheIndexTest, Abort)
{
    uint64_t tid = 1;
    uint64_t pg_xid = 100;
    ExtentHeader header(ExtentType(), pg_xid, 100);
    ExtentPtr data = std::make_shared<Extent>(header);

    index->add_extent(tid, pg_xid, 200, data);
    index->abort(pg_xid);

    uint64_t cursor = 0;
    auto extents = index->get_extents(tid, pg_xid, 1, cursor);
    EXPECT_EQ(cursor, 0);
    EXPECT_TRUE(extents.empty());
}

TEST_F(WriteCacheIndexTest, EvictTable)
{
    uint64_t tid = 1;
    uint64_t xid = 200;
    uint64_t pg_xid = 100;
    ExtentHeader header(ExtentType(), xid, 100);
    ExtentPtr data = std::make_shared<Extent>(header);

    index->add_extent(tid, pg_xid, 200, data);
    index->commit(pg_xid, xid);

    uint64_t cursor = 0;
    auto extents = index->get_extents(tid, xid, 1, cursor);
    EXPECT_EQ(extents.size(), 1);

    index->evict_table(tid, xid);

    cursor = 0;
    extents = index->get_extents(tid, xid, 1, cursor);
    EXPECT_TRUE(extents.empty());
}

TEST_F(WriteCacheIndexTest, EvictXid)
{
    uint64_t tid = 1;
    uint64_t xid = 200;
    uint64_t pg_xid = 100;
    ExtentHeader header(ExtentType(), xid, 100);
    ExtentPtr data = std::make_shared<Extent>(header);

    index->add_extent(tid, pg_xid, 200, data);
    index->commit(pg_xid, xid);

    uint64_t cursor = 0;
    auto extents = index->get_extents(tid, xid, 1, cursor);
    EXPECT_EQ(extents.size(), 1);

    index->evict_xid(xid);
    index->commit(pg_xid, xid);

    cursor = 0;
    extents = index->get_extents(tid, xid, 1, cursor);
    EXPECT_TRUE(extents.empty());
}

void
add_extent_thread(WriteCacheIndexPtr index,
                  uint64_t tid,
                  uint64_t pg_xid,
                  uint64_t lsn_start,
                  uint64_t count,
                  std::mutex &mtx)
{
    for (uint64_t i = 0; i < count; ++i) {
        ExtentHeader header(ExtentType(), 200 + i, 100);
        ExtentPtr data = std::make_shared<Extent>(header);
        {
            std::unique_lock<std::mutex> lock(mtx);
            index->add_extent(tid, pg_xid, lsn_start + i, data);
        }
    }
}

TEST_F(WriteCacheIndexTest, AddExtentParallel)
{
    uint64_t tid = 1;
    uint64_t pg_xid = 100;
    uint64_t xid = 150;
    uint64_t lsn_start = 200;
    uint64_t count = 50;

    std::mutex mtx;
    std::vector<std::thread> threads;

    // Launch multiple threads to add extents
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back(add_extent_thread, index, tid, pg_xid, lsn_start + i * count, count,
                             std::ref(mtx));
    }

    // Join all threads
    for (auto &t : threads) {
        t.join();
    }

    index->commit(pg_xid, xid);

    // Verify the data
    std::vector<WriteCacheIndexExtentPtr> fetched_extents;
    uint64_t cursor = 0;
    while (true) {
        auto extents = index->get_extents(tid, xid, 4, cursor);
        if (extents.empty()) {
            break;
        }
        EXPECT_LE(extents.size(), 4);
        fetched_extents.insert(fetched_extents.end(), extents.begin(), extents.end());
    }

    ASSERT_EQ(fetched_extents.size(), 4 * count);

    for (uint64_t i = 0; i < 4 * count; ++i) {
        EXPECT_EQ(fetched_extents[i]->xid, xid);
        EXPECT_EQ(fetched_extents[i]->xid_seq, lsn_start + i);
    }
}

TEST_F(WriteCacheIndexTest, AddAndFetchMultipleTids)
{
    uint64_t pg_xid = 100;
    uint64_t lsn_start = 200;
    uint64_t count = 100;
    uint64_t num_tids = 100;
    uint64_t xid = 150;

    // Add extents with different table IDs (tid)
    for (uint64_t tid = 1; tid <= num_tids; ++tid) {
        for (uint64_t i = 0; i < count; ++i) {
            ExtentHeader header(ExtentType(), 200 + i, 100);
            ExtentPtr data = std::make_shared<Extent>(header);
            index->add_extent(tid, pg_xid, lsn_start + i, data);
        }
    }

    index->commit(pg_xid, xid);

    // Fetch tids using multiple get_tids() calls with the cursor
    uint64_t cursor = 0;
    std::vector<uint64_t> fetched_tids;
    while (true) {
        auto tids = index->get_tids(xid, 3, cursor);
        if (tids.empty()) {
            break;
        }
        EXPECT_LE(tids.size(), 3);
        fetched_tids.insert(fetched_tids.end(), tids.begin(), tids.end());
    }

    // Verify the fetched tids
    ASSERT_EQ(fetched_tids.size(), num_tids);
    for (uint64_t tid = 1; tid <= num_tids; ++tid) {
        EXPECT_NE(std::find(fetched_tids.begin(), fetched_tids.end(), tid), fetched_tids.end());
    }
}

TEST_F(WriteCacheIndexTest, MultiXidExtents)
{
    // test multiple transactions w/multiple pg_xids
    // fetching extents using cursor
    uint64_t tid = 1;
    uint64_t pg_xid = 100;
    uint64_t xid = 150;
    uint64_t lsn = 200;
    int xid_count = 7;
    ExtentHeader header(ExtentType(), xid, 100);
    ExtentPtr data = std::make_shared<Extent>(header);

    std::vector<uint64_t> pg_xids;
    for (int i = 0; i < xid_count; i++) {
        index->add_extent(tid, pg_xid + i, lsn + i, data);
        pg_xids.push_back(pg_xid + i);
    }

    index->commit(pg_xids, xid);

    uint64_t cursor = 0;
    std::vector<WriteCacheIndexExtentPtr> fetched_extents;
    while (true) {
        auto extents = index->get_extents(tid, xid, 2, cursor);
        if (extents.empty()) {
            break;
        }
        EXPECT_LE(extents.size(), 2);
        fetched_extents.insert(fetched_extents.end(), extents.begin(), extents.end());
    }
    ASSERT_EQ(fetched_extents.size(), xid_count);
    EXPECT_EQ(cursor, xid_count);
    for (int i = 0; i < xid_count; i++) {
        EXPECT_EQ(fetched_extents[i]->xid, xid);
        EXPECT_EQ(fetched_extents[i]->xid_seq, lsn + i);
        EXPECT_EQ(fetched_extents[i]->data, data);
    }
}

TEST_F(WriteCacheIndexTest, MultiXidTids)
{
    // test multiple transactions w/multiple pg_xids
    // fetching tids using cursor
    uint64_t tid = 1;
    uint64_t pg_xid = 100;
    uint64_t xid = 150;
    uint64_t lsn = 200;
    int xid_count = 7;
    ExtentHeader header(ExtentType(), xid, 100);
    ExtentPtr data = std::make_shared<Extent>(header);

    std::vector<uint64_t> pg_xids;
    for (int i = 0; i < xid_count; i++) {
        index->add_extent(tid + i, pg_xid + i, lsn + i, data);
        pg_xids.push_back(pg_xid + i);
    }

    index->commit(pg_xids, xid);

    uint64_t cursor = 0;
    std::set<uint64_t> fetched_tids;
    while (true) {
        auto tids = index->get_tids(xid, 2, cursor);
        if (tids.empty()) {
            break;
        }
        EXPECT_LE(tids.size(), 2);
        fetched_tids.insert(tids.begin(), tids.end());
    }
    ASSERT_EQ(fetched_tids.size(), xid_count);
    EXPECT_EQ(cursor, xid_count);
    int i = 0;
    for (auto f_tid : fetched_tids) {
        EXPECT_EQ(f_tid, tid + (i++));
    }
}

}  // namespace