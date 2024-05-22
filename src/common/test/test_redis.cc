#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/redis.hh>
#include <common/threaded_test.hh>

using namespace springtail;

namespace {
    /**
     * Framework for Redis testing.
     */
    class Redis_Test : public testing::Test {
    protected:
        void SetUp() override {
            _has_redis = false;

            springtail_init();

            // See if redis is enabled
            try {
                RedisMgr::get_instance()->get_client()->ping();
                _has_redis = true;

                // cleanup in case of previous killed run
                TearDown();
            } catch (const std::exception &e) {
                GTEST_SKIP() << "Redis is not running, skipping test";
            }
        }

        void TearDown() override {
            if (_has_redis) {
                RedisMgr::get_instance()->get_client()->del("test_queue");
                RedisMgr::get_instance()->get_client()->del("test_queue:a");
                RedisMgr::get_instance()->get_client()->del("test_queue:b");
                RedisMgr::get_instance()->get_client()->del("test_set");
            }
        }

        class QueueEntry {
        public:
            explicit QueueEntry(const std::string &value) {
                _value = std::stoull(value);
            }

            std::string serialize() const {
                return std::to_string(_value);
            }

            uint64_t value() const {
                return _value;
            }

        private:
            uint64_t _value;
        };

        class SetEntry {
        public:
            explicit SetEntry(const std::string &value) {
                _value = value;
            }

            std::string serialize() const {
                return _value;
            }

            std::string value() const {
                return _value;
            }

        private:
            std::string _value;
        };

        bool _has_redis;
    };

    // tests the basic RedisQueue functionality
    TEST_F(Redis_Test, SimpleQueue) {
        RedisQueue<QueueEntry> queue("test_queue");

        // populate queue
        ASSERT_EQ(queue.push(QueueEntry("100")), 1);
        ASSERT_EQ(queue.push(QueueEntry("101")), 2);
        ASSERT_EQ(queue.push(QueueEntry("102")), 3);
        ASSERT_EQ(queue.push(QueueEntry("103")), 4);

        // test popping everything off
        ASSERT_EQ(queue.pop_and_commit()->value(), 100);
        ASSERT_EQ(queue.pop_and_commit()->value(), 101);
        ASSERT_EQ(queue.pop_and_commit()->value(), 102);
        ASSERT_EQ(queue.pop_and_commit()->value(), 103);

        // test popping empty queue
        ASSERT_EQ(queue.pop_and_commit(1), nullptr);
    }

    // tests the two-phase commit RedisQueue functionality
    TEST_F(Redis_Test, SimpleTwoPhaseQueue) {
        RedisQueue<QueueEntry> queue("test_queue");

        // populate queue
        ASSERT_EQ(queue.push(QueueEntry("100")), 1);
        ASSERT_EQ(queue.push(QueueEntry("101")), 2);
        ASSERT_EQ(queue.push(QueueEntry("102")), 3);
        ASSERT_EQ(queue.push(QueueEntry("103")), 4);

        // test the range query
        // note: Values are returned in reverse order due to the required use of brpoplpush().  We
        //       can change this behavior if redisplusplus implements the new lmove() and blmove()
        //       calls available in redis 6.2
        auto values = queue.range();
        ASSERT_EQ(values.size(), 4);
        int v = 103;
        for (auto value : values) {
            ASSERT_EQ(value.value(), v);
            --v;
        }

        // get items as two separate workers
        ASSERT_EQ(queue.pop("a")->value(), 100);
        ASSERT_EQ(queue.pop("b")->value(), 101);

        // commit "a" and get another value
        queue.commit("a");
        ASSERT_EQ(queue.pop("a")->value(), 102);

        // abort "b" (putting it back into the queue) and get another item
        queue.abort("b");
        ASSERT_EQ(queue.pop("b")->value(), 103);

        // abort "a"
        queue.abort("a");

        // validate the remainder of the queue
        ASSERT_EQ(queue.pop_and_commit()->value(), 101);
        ASSERT_EQ(queue.pop_and_commit()->value(), 102);

        // test poping empty queue with a worker
        ASSERT_EQ(queue.pop("a", 1), nullptr);
    }

    // tests the basic RedisSortedSet functionality
    TEST_F(Redis_Test, SimpleSortedSet) {
        RedisSortedSet<SetEntry> set("test_set");

        // populate set
        ASSERT_EQ(set.add(SetEntry("four"), 4 * 2), 1);
        ASSERT_EQ(set.add(SetEntry("one"), 1 * 2), 1);
        ASSERT_EQ(set.add(SetEntry("three"), 3 * 2), 1);
        ASSERT_EQ(set.add(SetEntry("two"), 2 * 2), 1);

        // test get()
        auto values = set.get(0, 2);
        ASSERT_EQ(values.size(), 3);
        ASSERT_EQ(values[0].value(), "one");
        ASSERT_EQ(values[1].value(), "two");
        ASSERT_EQ(values[2].value(), "three");

        // test get_by_score()
        values = set.get_by_score(4);
        ASSERT_EQ(values.size(), 3);
        ASSERT_EQ(values[0].value(), "two");
        ASSERT_EQ(values[1].value(), "three");
        ASSERT_EQ(values[2].value(), "four");

        // test remove()
        ASSERT_EQ(set.remove(SetEntry("one")), 1);
        ASSERT_EQ(set.remove(SetEntry("five")), 0);

        // verify that one was removed
        values = set.get(0);
        ASSERT_EQ(values.size(), 3);
        ASSERT_EQ(values[0].value(), "two");
        ASSERT_EQ(values[1].value(), "three");
        ASSERT_EQ(values[2].value(), "four");

        // verify that we don't see any values with score < 4
        values = set.get_by_score(0, 3);
        ASSERT_EQ(values.size(), 0);

        // verify the re-ordering on re-insert with updated score
        set.add(SetEntry("three"), 10);
        values = set.get(0);
        ASSERT_EQ(values.size(), 3);
        ASSERT_EQ(values[0].value(), "two");
        ASSERT_EQ(values[1].value(), "four");
        ASSERT_EQ(values[2].value(), "three");
    }
}
