#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <redis/pubsub_thread.hh>

using namespace springtail;

namespace {
class RedisPubSub_Test : public testing::TestWithParam<bool> {
protected:
    static void SetUpTestSuite()
    {
        springtail_init();

        // See if redis is enabled
        try {
            RedisMgr::get_instance()->get_client()->ping();

        } catch (const std::exception &e) {
            GTEST_SKIP() << "Redis is not running, skipping test";
        }
    }
    static void TearDownTestSuite() { RedisMgr::get_instance()->shutdown(); }

protected:
    std::mutex _data_mutex;
    std::condition_variable _data_cv;
};

TEST_P(RedisPubSub_Test, SingleSubscriberTest)
{
    bool config_db = GetParam();
    PubSubThread pubsub_thread(1, config_db);
    std::string initial_msg = "this is initialization message";
    std::string received_msg;
    std::string channel = fmt::format(redis::PUBSUB_DB_STATE_CHANGES, "5050");

    pubsub_thread.add_subscriber(
        channel,
        [this, &received_msg, &channel, &initial_msg]() {
            SPDLOG_DEBUG("Initialization for channel: {}", channel);
            std::unique_lock data_lock(_data_mutex);
            received_msg = initial_msg;
            data_lock.unlock();
        },
        [this, &received_msg, &channel, &initial_msg](const std::string &msg) {
            SPDLOG_DEBUG("Received message : {}, on channel: {}", msg, channel);
            std::unique_lock data_lock(_data_mutex);
            ASSERT_EQ(received_msg, initial_msg);
            received_msg = msg;
            _data_cv.notify_one();
            data_lock.unlock();
        });

    pubsub_thread.start();
    while (!pubsub_thread.is_up()) {
        sleep(1);
    }

    std::string publish_msg("very important information");
    RedisMgr::get_instance()->get_client()->publish(channel, publish_msg);

    std::unique_lock data_lock(_data_mutex);
    while (received_msg == initial_msg) {
        _data_cv.wait(data_lock);
    }

    ASSERT_EQ(received_msg, publish_msg);
    data_lock.unlock();

    pubsub_thread.shutdown();
}

TEST_P(RedisPubSub_Test, MultipleSubscribersTest)
{
    bool config_db = GetParam();
    PubSubThread pubsub_thread(1, config_db);
    std::string initial_msg = "this is initialization message";
    std::string received_msg = initial_msg;
    std::string channels[] = {fmt::format(redis::DB_INSTANCE_CONFIG, "5050"),
                              fmt::format(redis::DB_CONFIG, "5050"),
                              fmt::format(redis::HASH_FDW, "5050"),
                              fmt::format(redis::DB_INSTANCE_STATE, "5050"),
                              fmt::format(redis::PUBSUB_FDW_CHANGES, "5050"),
                              fmt::format(redis::PUBSUB_DB_CONFIG_CHANGES, "5050"),
                              fmt::format(redis::PUBSUB_DB_STATE_CHANGES, "5050"),
                              fmt::format(redis::QUEUE_GC_XID_READY, "5050"),
                              fmt::format(redis::QUEUE_DDL_XID, "5050", "4242", "2222"),
                              fmt::format(redis::HASH_DDL_PRECOMMIT, "5050"),
                              fmt::format(redis::QUEUE_DDL_FDW, "5050", "4242"),
                              fmt::format(redis::HASH_DDL_FDW, "5050"),
                              fmt::format(redis::QUEUE_SYNC_TABLES, "5050", "4242"),
                              fmt::format(redis::STRING_LOG_RESYNC, "5050", "4242"),
                              fmt::format(redis::HASH_SYNC_TABLE_OPS, "5050", "4242"),
                              fmt::format(redis::HASH_LIVENESS, "5050"),
                              fmt::format(redis::PUBSUB_LIVENESS_NOTIFY, "5050"),
                              fmt::format(redis::SET_DB_TABLES, "5050", "4242"),
                              fmt::format(redis::PUBSUB_DB_TABLE_CHANGES, "5050"),
                              ""};

    for (std::string *channel = channels; *channel != ""; channel++) {
        pubsub_thread.add_subscriber(
            *channel,
            [this, &received_msg, channel, &initial_msg]() {
                SPDLOG_DEBUG("Initialization for channel: {}", *channel);
            },
            [this, &received_msg, channel, &initial_msg](const std::string &msg) {
                SPDLOG_DEBUG("Received message : {}, on channel: {}", msg, *channel);
                std::unique_lock data_lock(_data_mutex);
                ASSERT_EQ(received_msg, initial_msg);
                received_msg = msg;
                _data_cv.notify_one();
                data_lock.unlock();
            });
    }
    pubsub_thread.start();
    while (!pubsub_thread.is_up()) {
        sleep(1);
    }

    for (std::string *channel = channels; *channel != ""; channel++) {
        std::string publish_msg =
            fmt::format("very important information for channel: {}", *channel);
        RedisMgr::get_instance()->get_client()->publish(*channel, publish_msg);

        std::unique_lock data_lock(_data_mutex);
        while (received_msg == initial_msg) {
            _data_cv.wait(data_lock);
        }

        ASSERT_EQ(received_msg, publish_msg);
        ASSERT_NE(received_msg.find(*channel), std::string::npos);
        initial_msg = received_msg;
        data_lock.unlock();
    }

    pubsub_thread.shutdown();
}

INSTANTIATE_TEST_CASE_P(RedisPubSub_Test, RedisPubSub_Test, ::testing::Values(false, true));

}  // namespace
