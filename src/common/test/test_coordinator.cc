#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <optional>

#include <fmt/core.h>

#include <common/common.hh>
#include <common/coordinator.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>

using namespace springtail;

class CoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        springtail_init();

        // See if redis is enabled
        try {
            RedisMgr::get_instance()->get_client()->ping();

            // cleanup in case of previous killed run
            TearDown();
        } catch (const std::exception &e) {
            GTEST_SKIP() << "Redis is not running, skipping test";
        }

        coordinator = Coordinator::get_instance();
        _instance_id = Properties::get_db_instance_id();
        _now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }

    void TearDown() override {
        // cleanout the liveness hash, and the notify queue
        RedisClientPtr redis = RedisMgr::get_instance()->get_client();
        std::string key = fmt::format(redis::HASH_LIVENESS, _instance_id);
        redis->del(key);
        std::string notify_key = fmt::format(redis::QUEUE_LIVENESS_NOTIFY, _instance_id);
        redis->del(notify_key);
    }

    /**
     * @brief Check if redis is empty for liveness
     */
    void check_redis_empty()
    {
        RedisClientPtr redis = RedisMgr::get_instance()->get_client();
        std::string key = fmt::format(redis::HASH_LIVENESS, _instance_id);
        std::vector<std::string> values;
        redis->hkeys(key, std::back_inserter(values));
        EXPECT_EQ(values.size(), 0);
    }

    /**
     * @brief Check the redis state for a daemon
     * @param type daemon type
     * @param thread_id thread id
     * @param alive true if the daemon is alive
     */
    void check_redis(Coordinator::DaemonType type, uint64_t thread_id, bool alive)
    {
        RedisClientPtr redis = RedisMgr::get_instance()->get_client();
        std::string key = fmt::format(redis::HASH_LIVENESS, _instance_id);
        std::string hkey = fmt::format("{}:{}", enum_to_integral(type), thread_id);

        // get the liveness value for the daemon:thread_id
        std::optional<std::string> value = redis->hget(key, hkey);
        ASSERT_TRUE(value.has_value());
        if (alive) {
            // if alive, value should be later than now (as set in the StartUp)
            EXPECT_GE(std::stoull(*value), _now);
        } else {
            // if dead, should be 0
            EXPECT_EQ(value, "0");

            // Check the queue for the dead daemon
            std::vector<std::string> values;
            std::string notify_key = fmt::format(redis::QUEUE_LIVENESS_NOTIFY, _instance_id);
            redis->lrange(notify_key, 0, -1, std::back_inserter(values));
            EXPECT_EQ(values.size(), 1);
            EXPECT_EQ(values[0], hkey);
        }
    }

    Coordinator* coordinator;
    uint64_t _instance_id;
    uint64_t _now;
};

TEST_F(CoordinatorTest, SingletonInstanceTest) {
    Coordinator* instance1 = Coordinator::get_instance();
    Coordinator* instance2 = Coordinator::get_instance();
    EXPECT_EQ(instance1, instance2);
}

TEST_F(CoordinatorTest, RegisterUnregisterThreadTest) {
    uint64_t thread_id = 1;

    coordinator->register_thread(Coordinator::LOG_MGR, thread_id);
    check_redis(Coordinator::LOG_MGR, thread_id, true);

    coordinator->unregister_thread(Coordinator::LOG_MGR, thread_id);
    check_redis_empty();
}

TEST_F(CoordinatorTest, SetLivenessTest) {
    uint64_t thread_id = 1;

    coordinator->register_thread(Coordinator::WRITE_CACHE, thread_id);
    coordinator->set_liveness(Coordinator::WRITE_CACHE, thread_id);

    check_redis(Coordinator::WRITE_CACHE, thread_id, true);
}

TEST_F(CoordinatorTest, KillDaemonTest) {
    uint64_t thread_id = 1;

    coordinator->register_thread(Coordinator::XID_MGR, thread_id);
    coordinator->kill_daemon(Coordinator::XID_MGR, thread_id);

    check_redis(Coordinator::XID_MGR, thread_id, false);
}

TEST_F(CoordinatorTest, MultipleThreadsTest) {
    uint64_t thread_id1 = 1;
    uint64_t thread_id2 = 2;

    coordinator->register_thread(Coordinator::DDL_MGR, thread_id1);
    coordinator->register_thread(Coordinator::GC_MGR, thread_id2);

    coordinator->set_liveness(Coordinator::DDL_MGR, thread_id1);
    coordinator->set_liveness(Coordinator::GC_MGR, thread_id2);

    check_redis(Coordinator::DDL_MGR, thread_id1, true);
    check_redis(Coordinator::GC_MGR, thread_id2, true);

    coordinator->kill_daemon(Coordinator::DDL_MGR, thread_id1);
    check_redis(Coordinator::DDL_MGR, thread_id1, false);

    coordinator->unregister_thread(Coordinator::GC_MGR, thread_id2);
    coordinator->unregister_thread(Coordinator::DDL_MGR, thread_id1);
    check_redis_empty();
}
