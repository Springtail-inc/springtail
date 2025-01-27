#include <fmt/core.h>
#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <thread>

#include <common/common.hh>
#include <common/coordinator.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>

using namespace springtail;

class CoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        springtail_init();

        _instance_id = Properties::get_db_instance_id();

        // See if redis is enabled
        try {
            RedisMgr::get_instance()->get_client()->ping();

            // cleanup in case of previous killed run
            TearDown();

            // setup the pubsub for liveness notifications
            _subscriber = RedisMgr::get_instance()->get_subscriber(5);
            std::string channel = fmt::format(redis::PUBSUB_LIVENESS_NOTIFY, _instance_id);
            _subscriber->on_message(
                [&](const std::string &channel, const std::string &msg) { this->_msg = msg; });
            _subscriber->subscribe(channel);
            _subscriber->consume();  // consume the unsubscribe message

            _subscriber->subscribe(channel);
            _subscriber->consume();  // consume the subscribe message

        } catch (const std::exception &e) {
            GTEST_SKIP() << "Redis is not running, skipping test";
        }

        _coordinator = Coordinator::get_instance();
        _now = std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
    }

    void TearDown() override
    {
        // cleanout the liveness hash, and the notify queue
        RedisClientPtr redis = RedisMgr::get_instance()->get_client();
        std::string key = fmt::format(redis::HASH_LIVENESS, _instance_id);
        redis->del(key);
        _subscriber = nullptr;
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
    void check_redis(Coordinator::DaemonType type, const std::string &thread_id, bool alive)
    {
        RedisClientPtr redis = RedisMgr::get_instance()->get_client();
        std::string key = fmt::format(redis::HASH_LIVENESS, _instance_id);
        std::string hkey = fmt::format("{}:{}", enum_to_integral(type), thread_id);

        // get the liveness value for the daemon:thread_id
        std::optional<std::string> value = redis->hget(key, hkey);
        ASSERT_TRUE(value.has_value());
        if (alive) {
            // if alive, value should be later than now (as set in the StartUp)
            ASSERT_GE(std::stoull(*value), _now);
        } else {
            // if dead, should be 0
            ASSERT_EQ(value, "0");

            // Check the pubsub for the notification message
            try {
                _subscriber->consume();
                ASSERT_EQ(_msg, hkey);
            } catch (const sw::redis::TimeoutError &e) {
                FAIL() << "Timeout waiting for notification message";
            }
        }
    }

    Coordinator *_coordinator;
    RedisMgr::SubscriberPtr _subscriber;
    uint64_t _instance_id;
    uint64_t _now;
    std::string _msg;  ///< message from the pubsub
};

TEST_F(CoordinatorTest, SingletonInstanceTest)
{
    Coordinator *instance1 = Coordinator::get_instance();
    Coordinator *instance2 = Coordinator::get_instance();
    EXPECT_EQ(instance1, instance2);
}

TEST_F(CoordinatorTest, RegisterUnregisterThreadTest)
{
    std::string thread_id = "1";

    _coordinator->register_thread(Coordinator::LOG_MGR, thread_id);
    check_redis(Coordinator::LOG_MGR, thread_id, true);

    _coordinator->unregister_thread(Coordinator::LOG_MGR, thread_id);
    check_redis_empty();
}

TEST_F(CoordinatorTest, SetLivenessTest)
{
    std::string thread_id = "1";

    _coordinator->register_thread(Coordinator::WRITE_CACHE, thread_id);
    _coordinator->mark_alive(Coordinator::WRITE_CACHE, thread_id);

    check_redis(Coordinator::WRITE_CACHE, thread_id, true);
}

TEST_F(CoordinatorTest, KillDaemonTest)
{
    std::string thread_id = "1";

    _coordinator->register_thread(Coordinator::XID_MGR, thread_id);
    _coordinator->kill_daemon(Coordinator::XID_MGR, thread_id);

    check_redis(Coordinator::XID_MGR, thread_id, false);
}

TEST_F(CoordinatorTest, MultipleThreadsTest)
{
    std::string thread_id1 = "1";
    std::string thread_id2 = "2";

    _coordinator->register_thread(Coordinator::DDL_MGR, thread_id1);
    _coordinator->register_thread(Coordinator::GC_MGR, thread_id2);

    _coordinator->mark_alive(Coordinator::DDL_MGR, thread_id1);
    _coordinator->mark_alive(Coordinator::GC_MGR, thread_id2);

    check_redis(Coordinator::DDL_MGR, thread_id1, true);
    check_redis(Coordinator::GC_MGR, thread_id2, true);

    _coordinator->kill_daemon(Coordinator::DDL_MGR, thread_id1);
    check_redis(Coordinator::DDL_MGR, thread_id1, false);

    _coordinator->unregister_thread(Coordinator::GC_MGR, thread_id2);
    _coordinator->unregister_thread(Coordinator::DDL_MGR, thread_id1);
    check_redis_empty();
}
