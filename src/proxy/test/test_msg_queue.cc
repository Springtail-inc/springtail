#include <gtest/gtest.h>

#include <proxy/session_msg.hh>

using namespace springtail;
using namespace springtail::pg_proxy;

namespace {

    class SessionMsgQueueTest : public ::testing::Test {
    protected:
        void SetUp() override {
            queue = std::make_shared<SessionMsgQueue<SessionMsgPtr>>();
        }

        std::shared_ptr<SessionMsgQueue<SessionMsgPtr>> queue;
        std::deque<SessionMsgPtr> batch;
    };

    TEST_F(SessionMsgQueueTest, PushToBatch) {
        auto msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY);
        batch.push_back(msg);
        queue->push_batch(std::move(batch));

        ASSERT_TRUE(queue->load_processing_batch());
        auto it = queue->processing_batch_start();
        ASSERT_NE(it, queue->processing_batch_end());
        EXPECT_EQ((*it)->type(), SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY);
    }

    TEST_F(SessionMsgQueueTest, PushBatch) {
        auto msg1 = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY);
        auto msg2 = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_PARSE);
        batch.push_back(msg1);
        batch.push_back(msg2);
        queue->push_batch(batch);
        batch.clear();

        msg1 = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_BIND);
        msg2 = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_DESCRIBE);
        batch.push_back(msg1);
        batch.push_back(msg2);
        queue->push_batch(std::move(batch));

        ASSERT_TRUE(queue->load_processing_batch());
        auto it = queue->processing_batch_start();
        ASSERT_NE(it, queue->processing_batch_end());
        EXPECT_EQ((*it)->type(), SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY);
        ++it;
        ASSERT_NE(it, queue->processing_batch_end());
        EXPECT_EQ((*it)->type(), SessionMsg::MSG_CLIENT_SERVER_PARSE);
    }

    TEST_F(SessionMsgQueueTest, LoadProcessingBatch) {
        auto msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY);
        batch.push_back(msg);
        queue->push_batch(std::move(batch));

        ASSERT_TRUE(queue->load_processing_batch());
        auto msgopt = queue->front_processing_msg();
        ASSERT_TRUE(msgopt.has_value());
        ASSERT_EQ(msgopt.value(), msg);
        msgopt = queue->pop_processing_msg();
        ASSERT_EQ(msgopt.has_value(), true);
        EXPECT_FALSE(queue->load_processing_batch()); // No more batches to load
    }

    TEST_F(SessionMsgQueueTest, PopProcessingMsg) {
        auto msg1 = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY);
        auto msg2 = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_PARSE);
        batch.push_back(msg1);
        batch.push_back(msg2);
        queue->push_batch(std::move(batch));

        ASSERT_TRUE(queue->load_processing_batch());
        auto popped_msg = queue->pop_processing_msg();
        ASSERT_TRUE(popped_msg.has_value());
        EXPECT_EQ(popped_msg.value()->type(), SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY);
        popped_msg = queue->pop_processing_msg();
        ASSERT_TRUE(popped_msg.has_value());
        EXPECT_EQ(popped_msg.value()->type(), SessionMsg::MSG_CLIENT_SERVER_PARSE);

        EXPECT_EQ(queue->pop_processing_msg(), std::nullopt); // No more messages to pop
        EXPECT_TRUE(queue->processing_empty());
    }

    TEST_F(SessionMsgQueueTest, HasProcessingMsg) {
        auto msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY);
        batch.push_back(msg);
        queue->push_batch(std::move(batch));

        ASSERT_TRUE(queue->load_processing_batch());
        EXPECT_FALSE(queue->processing_empty());
        queue->pop_processing_msg();
        EXPECT_TRUE(queue->processing_empty());
    }

} // namespace
