#include <gtest/gtest.h>

#include <common/environment.hh>
#include <pg_repl/pg_common.hh>
#include <write_cache/write_cache_server.hh>

using namespace springtail;

namespace {
    class WriteCacheServerTest : public ::testing::Test {
    public:
        static void SetUpTestSuite() {
            // override directory and watermark settings
            std::string disk_storage_dir = std::format("write_cache.disk_storage_dir={}", "/tmp/write_cache");
            std::string memory_high_watermark = std::format("write_cache.memory_high_watermark={}", 8192);
            std::string memory_low_watermark = std::format("write_cache.memory_low_watermark={}", 4096);
            std::string overrides = disk_storage_dir + ";" + memory_high_watermark + ";" + memory_low_watermark;
            ::setenv(environment::ENV_OVERRIDE, overrides.c_str(), 1);

            // Init springtail
            springtail_init_test();

            logging::Logger::get_instance()->set_debug_level(LOG_LEVEL_DEBUG4);
        }

        static void TearDownTestSuite() {
            springtail_shutdown();
        }
    protected:
        uint64_t _last_pg_xid{1};
        uint64_t _last_lsn{1};
        uint64_t _last_xid{1};
        std::vector<uint8_t> _field_types{
            static_cast<uint8_t>(SchemaType::UINT64),
            static_cast<uint8_t>(SchemaType::UINT64),
            static_cast<uint8_t>(SchemaType::UINT64),
            static_cast<uint8_t>(SchemaType::UINT64)
        };

        ExtentPtr
        _create_extent()
        {
            ++_last_lsn;
            std::cout << "creating extent with xid = " << _last_lsn << std::endl;
            ExtentPtr new_extent = std::make_shared<Extent>(
                ExtentType(),
                _last_lsn,
                256,
                _field_types
            );
            new_extent->append();
            new_extent->append();
            // new_extent->append();
            // new_extent->append();
            EXPECT_EQ(new_extent->byte_count(), 512);
            return new_extent;
        }

        void
        _add_extents(uint64_t db_id, const std::vector<uint64_t> &table_ids, uint64_t pg_xids_per_table)
        {
            ExtentPtr new_extent = nullptr;
            WriteCacheServer *server = WriteCacheServer::get_instance();
            for (uint64_t tid: table_ids) {
                for (uint64_t i = 0; i < pg_xids_per_table; ++i) {
                    ++_last_pg_xid;
                    new_extent = _create_extent();
                    server->add_extent(db_id, tid, _last_pg_xid, new_extent->header().xid, new_extent);
                    new_extent = _create_extent();
                    server->add_extent(db_id, tid, _last_pg_xid, new_extent->header().xid, new_extent);
                }
            }
        }

        void
        _add_extents_with_last_pg_xid(uint64_t db_id, const std::vector<uint64_t> &table_ids)
        {
            ++_last_pg_xid;
            ExtentPtr new_extent = nullptr;
            WriteCacheServer *server = WriteCacheServer::get_instance();
            for (uint64_t tid: table_ids) {
                new_extent = _create_extent();
                server->add_extent(db_id, tid, _last_pg_xid, new_extent->header().xid, new_extent);
                new_extent = _create_extent();
                server->add_extent(db_id, tid, _last_pg_xid, new_extent->header().xid, new_extent);
            }
        }
    };

    TEST_F(WriteCacheServerTest, NormalFlowTest)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        // 1. Add extents
        // multiple pg_xids, multiple tables for the same pg_xid, multiple lsns (one per extent)
        // pg_xid = 2
        _add_extents_with_last_pg_xid(1, {1, 2, 3, 4, 5});
        // pg_xid = 3
        _add_extents_with_last_pg_xid(1, {1, 2, 3, 4, 5});
        // pg_xid = 4
        _add_extents_with_last_pg_xid(1, {1, 2, 3, 4, 5});
        // pg_xid = 5
        _add_extents_with_last_pg_xid(1, {1, 2, 3, 4, 5});
        // pg_xid = 6
        _add_extents_with_last_pg_xid(1, {1, 2, 3, 4, 5});

        nlohmann::json stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 8704);
        EXPECT_TRUE(stats["store to disk"]);

        // 2. Drop one of the tables
        server->drop_table(1, 1, 2);
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 7680);
        EXPECT_TRUE(stats["store to disk"]);

        server->drop_table(1, 5, _last_pg_xid);
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 7680);
        EXPECT_TRUE(stats["store to disk"]);

        // 3. Abort transactions (single pg_xid, multiple pg_xid)
        server->abort(1, 2);
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 3584);
        EXPECT_FALSE(stats["store to disk"]);

        server->abort(1, {3, 4});
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 0);
        EXPECT_FALSE(stats["store to disk"]);

        // 4. Add more extents
        // pg_xid = 7
        _add_extents_with_last_pg_xid(1, {1, 2, 3, 4, 5});
        // pg_xid = 8
        _add_extents_with_last_pg_xid(1, {1, 2, 3, 4, 5});
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 8704);
        EXPECT_TRUE(stats["store to disk"]);

        // 5. Commit (single pg_xid, multiple pg_xids)
        WriteCacheTableSet::Metadata md;
        // xid = 2
        server->commit(1, ++_last_xid, 6, md);
        // xid = 3
        server->commit(1, ++_last_xid, {7, 8}, md);
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 8704);
        EXPECT_TRUE(stats["store to disk"]);

        // 6. List tables
        uint64_t cursor = 0;
        std::vector<uint64_t> tables =
            server->list_tables(1, 2, 1, cursor);
        EXPECT_EQ(tables.size(), 1);
        tables = server->list_tables(1, 2, 2, cursor);
        EXPECT_EQ(tables.size(), 2);

        // 7. Get extents (a single extent, multiple extents)
        cursor = 0;
        std::vector<WriteCacheIndexExtentPtr> extents =
            server->get_extents(1, 1, 2, 10, cursor, md);
        EXPECT_EQ(extents.size(), 2);

        cursor = 0;
        extents = server->get_extents(1, 1, 3, 10, cursor, md);
        EXPECT_EQ(extents.size(), 4);

        // 8. Evict xid
        server->evict_xid(1, 2);
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 8704);
        EXPECT_TRUE(stats["store to disk"]);

        // 9. Evict table
        server->evict_table(1, 1, 3);
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 6656);
        EXPECT_TRUE(stats["store to disk"]);

        // 10. Cleanup
        server->drop_database(1);
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 0);
        EXPECT_FALSE(stats["store to disk"]);
    }

    TEST_F(WriteCacheServerTest, DropDatabaseTest)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        // 1. Populate data in multiple databases
        _add_extents(1, {1, 2, 3, 4, 5}, 5);
        nlohmann::json stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 8704);

        _add_extents(2, {1, 2, 3, 4, 5}, 5);
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 8704);

        // 2. Drop one of the databases
        server->drop_database(2);
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 8704);

        server->drop_database(1);
        stats = server->get_memory_stats();
        std::cout << "Stats: " << stats.dump(4) << std::endl;
        EXPECT_EQ(stats["current memory"], 0);
    }
}