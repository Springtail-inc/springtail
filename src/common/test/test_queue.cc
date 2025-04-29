#include <memory>
#include <algorithm>
#include <gtest/gtest.h>

#include <common/concurrent_queue.hh>

using namespace springtail;

struct QueueEntry {
    int a;
    QueueEntry(int a) : a(a) {}
};

TEST(QueueTest, SimpleTest) {
    ConcurrentQueue<QueueEntry> queue{};
    queue.push(std::make_shared<QueueEntry>(1));
    queue.push(std::make_shared<QueueEntry>(2));
    queue.push(std::make_shared<QueueEntry>(3));
    queue.push(std::make_shared<QueueEntry>(4));
    queue.push(std::make_shared<QueueEntry>(5));

    EXPECT_EQ(queue.size(), 5);

    auto entry = queue.pop();
    EXPECT_EQ(entry->a, 1);
    entry = queue.pop();
    EXPECT_EQ(entry->a, 2);
    entry = queue.pop();
    EXPECT_EQ(entry->a, 3);
    entry = queue.pop();
    EXPECT_EQ(entry->a, 4);
    entry = queue.pop();
    EXPECT_EQ(entry->a, 5);
}

TEST(QueueTest, ArrayTest) {
    ConcurrentQueue<QueueEntry> queue{};
    std::vector<std::shared_ptr<QueueEntry>> entries;
    entries.push_back(std::make_shared<QueueEntry>(1));
    entries.push_back(std::make_shared<QueueEntry>(2));
    entries.push_back(std::make_shared<QueueEntry>(3));
    entries.push_back(std::make_shared<QueueEntry>(4));
    entries.push_back(std::make_shared<QueueEntry>(5));
    queue.push(entries);

    auto entry = queue.pop();
    EXPECT_EQ(entry->a, 1);
    entry = queue.pop();
    EXPECT_EQ(entry->a, 2);
    entry = queue.pop();
    EXPECT_EQ(entry->a, 3);
    entry = queue.pop();
    EXPECT_EQ(entry->a, 4);
    entry = queue.pop();
    EXPECT_EQ(entry->a, 5);
}

TEST(QueueTest, ThreadedTestArrayBound) {
    ConcurrentQueue<QueueEntry> queue{3};
    std::vector<std::shared_ptr<QueueEntry>> entries;
    entries.push_back(std::make_shared<QueueEntry>(1));
    entries.push_back(std::make_shared<QueueEntry>(2));
    entries.push_back(std::make_shared<QueueEntry>(3));
    entries.push_back(std::make_shared<QueueEntry>(4));
    entries.push_back(std::make_shared<QueueEntry>(5));

    std::thread t1([&queue, &entries](){
        queue.push(entries);
    });

    std::thread t2([&queue](){
        auto entry = queue.pop();
        EXPECT_EQ(entry->a, 1);
        entry = queue.pop();
        EXPECT_EQ(entry->a, 2);
        entry = queue.pop();
        EXPECT_EQ(entry->a, 3);
        entry = queue.pop();
        EXPECT_EQ(entry->a, 4);
        entry = queue.pop();
        EXPECT_EQ(entry->a, 5);
    });

    t1.join();
    t2.join();
}

TEST(QueueTest, ThreadedTestBound) {
    ConcurrentQueue<QueueEntry> queue{3};

    std::thread t1([&queue](){
        queue.push(std::make_shared<QueueEntry>(1));
        queue.push(std::make_shared<QueueEntry>(2));
        queue.push(std::make_shared<QueueEntry>(3));
        queue.push(std::make_shared<QueueEntry>(4));
        queue.push(std::make_shared<QueueEntry>(5));
    });

    std::thread t2([&queue](){
        auto entry = queue.pop();
        EXPECT_EQ(entry->a, 1);
        entry = queue.pop();
        EXPECT_EQ(entry->a, 2);
        entry = queue.pop();
        EXPECT_EQ(entry->a, 3);
        entry = queue.pop();
        EXPECT_EQ(entry->a, 4);
        entry = queue.pop();
        EXPECT_EQ(entry->a, 5);
    });

    t1.join();
    t2.join();
}

TEST(QueueTest, ThreadedTestBlockAndDrain) {
    ConcurrentQueue<QueueEntry> queue{4};  // Small bounded queue

    std::thread t1([&queue]() {
        queue.push(std::make_shared<QueueEntry>(1));
        queue.push(std::make_shared<QueueEntry>(2));
        queue.push(std::make_shared<QueueEntry>(3));
        queue.push(std::make_shared<QueueEntry>(4));
        queue.push(std::make_shared<QueueEntry>(5));
        queue.push(std::make_shared<QueueEntry>(6));
    });

    std::thread t2([&queue]() {
        queue.push(std::make_shared<QueueEntry>(7));
        queue.push(std::make_shared<QueueEntry>(8));
        queue.push(std::make_shared<QueueEntry>(9));
        queue.push(std::make_shared<QueueEntry>(10));
        queue.push(std::make_shared<QueueEntry>(11));
        queue.push(std::make_shared<QueueEntry>(12));
    });

    // Sleep to let push threads fill queue and possibly block
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Now pop everything
    std::vector<int> popped_values;
    for (int i = 0; i < 12; ++i) {
        auto entry = queue.pop(1);
        ASSERT_NE(entry, nullptr);
        popped_values.push_back(entry->a);
    }

    t1.join();
    t2.join();

    // Sort the popped values
    std::ranges::sort(popped_values);

    // Validate popped values
    ASSERT_EQ(popped_values, (std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}));
}
