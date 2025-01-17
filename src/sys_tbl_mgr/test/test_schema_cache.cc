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
        static void SetUpTestSuite() {
            springtail_init();

            _services.init(true);
        }

        static void TearDownTestSuite() {
            _services.shutdown();
        }

        static test::Services _services;

    protected:
        void SetUp() override {
            // populate the dummy test set
            for (uint64_t i = 0; i < 100; i++) {
                sys_tbl_mgr::SchemaCache::Key key{1, 10000 + i, XidLsn{1}};
                auto md = std::make_shared<SchemaMetadata>();
                md->access_range = XidRange(key.xid, XidLsn(key.xid.xid + 1));
                md->target_range = XidRange(XidLsn{4}, XidLsn(constant::LATEST_XID));
                md->columns.push_back({
                        fmt::format("x{}", i),
                        1, SchemaType::INT32, 23, false
                    });
                md->columns.push_back({
                        fmt::format("y{}", i),
                        2, SchemaType::INT32, 23, false
                    });

                _schema_map[key] = md;

                key.xid = XidLsn{2};
                md = std::make_shared<SchemaMetadata>();
                md->access_range = XidRange(key.xid, XidLsn(key.xid.xid + 1));
                md->target_range = XidRange(XidLsn(4), XidLsn(constant::LATEST_XID));
                md->columns.push_back({
                        fmt::format("x{}x", i),
                        1, SchemaType::INT32, 23, false
                    });
                md->columns.push_back({
                        fmt::format("y{}", i),
                        2, SchemaType::INT32, 23, false
                    });

                _schema_map[key] = md;

                key.xid = XidLsn{3};
                md = std::make_shared<SchemaMetadata>();
                md->access_range = XidRange(key.xid, XidLsn(key.xid.xid + 1));
                md->target_range = XidRange(XidLsn(4), XidLsn(constant::LATEST_XID));
                md->columns.push_back({
                        fmt::format("x{}x", i),
                        1, SchemaType::INT32, 23, false
                    });
                md->columns.push_back({
                        fmt::format("y{}", i),
                        2, SchemaType::INT32, 23, false
                    });
                md->columns.push_back({
                        fmt::format("z{}", i),
                        3, SchemaType::INT32, 23, false
                    });
                _schema_map[key] = md;

                key.xid = XidLsn{4};
                md = std::make_shared<SchemaMetadata>();
                md->access_range = XidRange(key.xid, XidLsn(constant::LATEST_XID));
                md->columns.push_back({
                        fmt::format("x{}", i),
                        1, SchemaType::INT32, 23, false
                    });
                md->columns.push_back({
                        fmt::format("y{}", i),
                        2, SchemaType::INT32, 23, false
                    });
                md->columns.push_back({
                        fmt::format("z{}", i),
                        3, SchemaType::INT32, 23, false
                    });
                _schema_map[key] = md;
            }
        }

        void TearDown() override {
            _schema_map.clear();
        }

        std::map<sys_tbl_mgr::SchemaCache::Key, SchemaMetadataPtr> _schema_map; // a dummy test set
    };

    test::Services SchemaCache_Test::_services(false, false, false);


    // Tests single-threaded behaviors of access schema get()
    TEST_F(SchemaCache_Test, BasicTable) {
        // create a very small cache
        sys_tbl_mgr::SchemaCache cache(4, 0);

        // request various schemas and verify them -- verify the cache miss count
        int miss_count = 0;
        for (uint64_t i = 0; i < 100; ++i) {
            for (int j = 0; j < 4; ++j) {
                auto md = cache.get(sys_tbl_mgr::SchemaCache::Key{1, 10000 + i, XidLsn{1}},
                                    [this, &miss_count](const sys_tbl_mgr::SchemaCache::Key &key) {
                                        ++miss_count;
                                        return _schema_map[key];
                                    });
                ASSERT_EQ(md->columns.size(), 2);
                ASSERT_EQ(md->columns[0].name, fmt::format("x{}", i));
                ASSERT_EQ(md->columns[0].position, 1);
                ASSERT_EQ(md->columns[1].name, fmt::format("y{}", i));
                ASSERT_EQ(md->columns[1].position, 2);
            }
        }
        ASSERT_EQ(miss_count, 100);
    }

    TEST_F(SchemaCache_Test, BasicTableHitPattern) {
        // create a very small cache
        sys_tbl_mgr::SchemaCache cache(4, 0);

        // request various schemas and verify them -- verify the cache miss count
        int miss_count = 0;
        for (uint64_t i = 0; i < 100; ++i) {
            for (uint64_t j = (i >= 4) ? i - 3 : 0; j <= i; ++j) {
                auto md = cache.get(sys_tbl_mgr::SchemaCache::Key{1, 10000 + j, XidLsn{1}},
                                    [this, &miss_count](const sys_tbl_mgr::SchemaCache::Key &key) {
                                        ++miss_count;
                                        return _schema_map[key];
                                    });
                ASSERT_EQ(md->columns.size(), 2);
                ASSERT_EQ(md->columns[0].name, fmt::format("x{}", j));
                ASSERT_EQ(md->columns[0].position, 1);
                ASSERT_EQ(md->columns[1].name, fmt::format("y{}", j));
                ASSERT_EQ(md->columns[1].position, 2);
            }
        }
        ASSERT_EQ(miss_count, 100);
    }

    TEST_F(SchemaCache_Test, BasicTableMissPattern) {
        // create a very small cache
        sys_tbl_mgr::SchemaCache cache(4, 0);

        // request various schemas and verify them -- verify the cache miss count
        int miss_count = 0;
        for (uint64_t j = 0; j < 4; ++j) {
            for (uint64_t i = 0; i < 100; ++i) {
                auto md = cache.get(sys_tbl_mgr::SchemaCache::Key{1, 10000 + i, XidLsn{1}},
                                    [this, &miss_count](const sys_tbl_mgr::SchemaCache::Key &key) {
                                        ++miss_count;
                                        return _schema_map[key];
                                    });
                ASSERT_EQ(md->columns.size(), 2);
                ASSERT_EQ(md->columns[0].name, fmt::format("x{}", i));
                ASSERT_EQ(md->columns[0].position, 1);
                ASSERT_EQ(md->columns[1].name, fmt::format("y{}", i));
                ASSERT_EQ(md->columns[1].position, 2);
            }
        }
        ASSERT_EQ(miss_count, 400);
    }

    // test multiple tables at multiple XIDs
    TEST_F(SchemaCache_Test, BasicTableXid) {
        // create a very small cache
        sys_tbl_mgr::SchemaCache cache(4, 0);

        // request various schemas and verify them -- verify the cache miss count
        int miss_count = 0;
        for (uint64_t i = 0; i < 100; ++i) {
            for (uint64_t j = 1; j < 5; ++j) {
                auto md = cache.get(sys_tbl_mgr::SchemaCache::Key{1, 10000 + i, XidLsn{j}},
                                    [this, &miss_count](const sys_tbl_mgr::SchemaCache::Key &key) {
                                        ++miss_count;
                                        return _schema_map[key];
                                    });
                switch (j) {
                case 1:
                    ASSERT_EQ(md->columns.size(), 2);
                    ASSERT_EQ(md->columns[0].name, fmt::format("x{}", i));
                    break;
                case 2:
                    ASSERT_EQ(md->columns.size(), 2);
                    ASSERT_EQ(md->columns[0].name, fmt::format("x{}x", i));
                    break;
                case 3:
                    ASSERT_EQ(md->columns.size(), 3);
                    ASSERT_EQ(md->columns[0].name, fmt::format("x{}x", i));
                    ASSERT_EQ(md->columns[2].name, fmt::format("z{}", i));
                    ASSERT_EQ(md->columns[2].position, 3);
                    break;
                case 4:
                    ASSERT_EQ(md->columns.size(), 3);
                    ASSERT_EQ(md->columns[0].name, fmt::format("x{}", i));
                    ASSERT_EQ(md->columns[2].name, fmt::format("z{}", i));
                    ASSERT_EQ(md->columns[2].position, 3);
                    break;
                }

                ASSERT_EQ(md->columns[0].position, 1);
                ASSERT_EQ(md->columns[1].name, fmt::format("y{}", i));
                ASSERT_EQ(md->columns[1].position, 2);
            }
        }
        ASSERT_EQ(miss_count, 400);
    }

    // test multiple tables at multiple XIDs with cache hits
    TEST_F(SchemaCache_Test, BasicTableXidHitPattern) {
        // create a very small cache
        sys_tbl_mgr::SchemaCache cache(4, 0);

        // request various schemas and verify them -- verify the cache miss count
        int miss_count = 0;
        for (uint64_t i = 0; i < 100; ++i) {
            for (uint64_t j = 1; j < 5; ++j) {
                for (int k = 0; k < 4; ++k) {
                    auto md = cache.get(sys_tbl_mgr::SchemaCache::Key{1, 10000 + i, XidLsn{j}},
                                        [this, &miss_count](const sys_tbl_mgr::SchemaCache::Key &key) {
                                            ++miss_count;
                                            return _schema_map[key];
                                        });
                    switch (j) {
                    case 1:
                        ASSERT_EQ(md->columns.size(), 2);
                        ASSERT_EQ(md->columns[0].name, fmt::format("x{}", i));
                        break;
                    case 2:
                        ASSERT_EQ(md->columns.size(), 2);
                        ASSERT_EQ(md->columns[0].name, fmt::format("x{}x", i));
                        break;
                    case 3:
                        ASSERT_EQ(md->columns.size(), 3);
                        ASSERT_EQ(md->columns[0].name, fmt::format("x{}x", i));
                        ASSERT_EQ(md->columns[2].name, fmt::format("z{}", i));
                        ASSERT_EQ(md->columns[2].position, 3);
                        break;
                    case 4:
                        ASSERT_EQ(md->columns.size(), 3);
                        ASSERT_EQ(md->columns[0].name, fmt::format("x{}", i));
                        ASSERT_EQ(md->columns[2].name, fmt::format("z{}", i));
                        ASSERT_EQ(md->columns[2].position, 3);
                        break;
                    }

                    ASSERT_EQ(md->columns[0].position, 1);
                    ASSERT_EQ(md->columns[1].name, fmt::format("y{}", i));
                    ASSERT_EQ(md->columns[1].position, 2);
                }
            }
        }
        ASSERT_EQ(miss_count, 400);
    }

}
