/*
 * Tests the behavior of the SchemaCache.
 */
#include <gtest/gtest.h>

#include <common/common.hh>
#include <sys_tbl_mgr/schema_cache.hh>
#include <test/services.hh>

using namespace springtail;

namespace {
/**
 * Framework for SchemaCache testing.
 */
class SchemaCache_Test : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        springtail_init();

        _services.init();
    }

    static void TearDownTestSuite() { _services.shutdown(); }

    static test::Services _services;

protected:
    using Key = std::tuple<uint64_t, uint64_t, XidLsn>;

    SchemaMetadataPtr _make_metadata(const XidLsn &xid, std::vector<std::string> columns)
    {
        auto md = std::make_shared<SchemaMetadata>();
        if (xid.xid < 4) {
            md->access_range = XidRange(xid, XidLsn(xid.xid + 1));
            md->target_range = XidRange(XidLsn{4}, XidLsn{constant::LATEST_XID});
        } else {
            md->access_range = XidRange(xid, XidLsn{constant::LATEST_XID});
        }

        int pos = 0;
        for (const auto &column : columns) {
            md->columns.push_back({column, ++pos, SchemaType::INT32, 23, false});
        }

        return md;
    }

    void _verify_columns(SchemaMetadataPtr md, const Key &key)
    {
        auto expected = _schema_map[key];

        ASSERT_EQ(md->columns.size(), expected->columns.size());
        for (int i = 0; i < md->columns.size(); ++i) {
            ASSERT_EQ(md->columns[i].name, expected->columns[i].name);
            ASSERT_EQ(md->columns[i].position, expected->columns[i].position);
        }
    }

    void SetUp() override
    {
        // populate the dummy test set
        for (uint64_t i = 0; i < 100; i++) {
            auto xid = XidLsn{1};
            auto key = Key{1, 10000 + i, xid};
            _schema_map[key] = _make_metadata(xid, {fmt::format("x{}", i), fmt::format("y{}", i)});

            xid = XidLsn{2};
            key = Key{1, 10000 + i, xid};
            _schema_map[key] = _make_metadata(xid, {fmt::format("x{}x", i), fmt::format("y{}", i)});

            xid = XidLsn{3};
            key = Key{1, 10000 + i, xid};
            _schema_map[key] = _make_metadata(
                xid, {fmt::format("x{}x", i), fmt::format("y{}", i), fmt::format("z{}", i)});

            xid = XidLsn{4};
            key = Key{1, 10000 + i, xid};
            _schema_map[key] = _make_metadata(
                xid, {fmt::format("x{}", i), fmt::format("y{}", i), fmt::format("z{}", i)});
        }
    }

    void TearDown() override { _schema_map.clear(); }

    std::map<Key, SchemaMetadataPtr> _schema_map;  // a dummy test set
};

test::Services SchemaCache_Test::_services(false, false, false);

// Tests single-threaded behaviors of access schema get()
TEST_F(SchemaCache_Test, BasicTable)
{
    // create a very small cache
    sys_tbl_mgr::SchemaCache cache(4);

    // request various schemas and verify them -- verify the cache miss count
    int miss_count = 0;
    for (uint64_t i = 0; i < 100; ++i) {
        for (int j = 0; j < 4; ++j) {
            auto md = cache.get(1, 10000 + i, XidLsn{4},
                                [this, &miss_count](uint64_t db, uint64_t tid, const XidLsn &xid) {
                                    ++miss_count;
                                    return _schema_map[Key{db, tid, xid}];
                                });
            _verify_columns(md, {1, 10000 + i, XidLsn{4}});
        }
    }
    ASSERT_EQ(miss_count, 100);
}

TEST_F(SchemaCache_Test, BasicTableHitPattern)
{
    // create a very small cache
    sys_tbl_mgr::SchemaCache cache(4);

    // request various schemas and verify them -- verify the cache miss count
    int miss_count = 0;
    for (uint64_t i = 0; i < 100; ++i) {
        for (uint64_t j = (i >= 4) ? i - 3 : 0; j <= i; ++j) {
            auto md = cache.get(1, 10000 + j, XidLsn{4},
                                [this, &miss_count](uint64_t db, uint64_t tid, const XidLsn &xid) {
                                    ++miss_count;
                                    return _schema_map[Key{db, tid, xid}];
                                });
            _verify_columns(md, {1, 10000 + j, XidLsn{4}});
        }
    }
    ASSERT_EQ(miss_count, 100);
}

TEST_F(SchemaCache_Test, BasicTableMissPattern)
{
    // create a very small cache
    sys_tbl_mgr::SchemaCache cache(4);

    // request various schemas and verify them -- verify the cache miss count
    int miss_count = 0;
    for (uint64_t j = 0; j < 4; ++j) {
        for (uint64_t i = 0; i < 100; ++i) {
            auto md = cache.get(1, 10000 + i, XidLsn{4},
                                [this, &miss_count](uint64_t db, uint64_t tid, const XidLsn &xid) {
                                    ++miss_count;
                                    return _schema_map[Key{db, tid, xid}];
                                });
            _verify_columns(md, {1, 10000 + i, XidLsn{4}});
        }
    }
    ASSERT_EQ(miss_count, 400);
}

// test multiple tables at multiple XIDs
TEST_F(SchemaCache_Test, BasicTableXid)
{
    // create a very small cache
    sys_tbl_mgr::SchemaCache cache(4);

    // request various schemas and verify them -- verify the cache miss count
    int miss_count = 0;
    for (uint64_t i = 0; i < 100; ++i) {
        for (uint64_t j = 1; j < 5; ++j) {
            auto md = cache.get(1, 10000 + i, XidLsn{j},
                                [this, &miss_count](uint64_t db, uint64_t tid, const XidLsn &xid) {
                                    ++miss_count;
                                    return _schema_map[Key{db, tid, xid}];
                                });
            _verify_columns(md, {1, 10000 + i, XidLsn{j}});
        }
    }
    ASSERT_EQ(miss_count, 400);
}

// test multiple tables at multiple XIDs with cache hits
TEST_F(SchemaCache_Test, BasicTableXidHitPattern)
{
    // create a very small cache
    sys_tbl_mgr::SchemaCache cache(4);

    // request various schemas and verify them -- verify the cache miss count
    int miss_count = 0;
    for (uint64_t i = 0; i < 100; ++i) {
        for (uint64_t j = 1; j < 5; ++j) {
            for (int k = 0; k < 4; ++k) {
                auto md =
                    cache.get(1, 10000 + i, XidLsn{j},
                              [this, &miss_count](uint64_t db, uint64_t tid, const XidLsn &xid) {
                                  ++miss_count;
                                  return _schema_map[Key{db, tid, xid}];
                              });
                _verify_columns(md, {1, 10000 + i, XidLsn{j}});
            }
        }
    }
    ASSERT_EQ(miss_count, 1300);
}

}  // namespace
