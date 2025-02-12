#include <gtest/gtest.h>
#include <common/common.hh>

#include <pg_fdw/multi-queue-thread-manager.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

namespace {
    class MultiQueueThreadManager_Test : public testing::Test {
    public:
        static void SetUpTestSuite() {
            // Properties::get_instance()->init(true);
            init_exception();
        }
        static void TearDownTestSuite() {
            // Properties::shutdown();
        }
    protected:
        void SetUp() override {
            _thread_manager.start();
        }
        void TearDown() override {
            _thread_manager.notify_shutdown();
            _thread_manager.shutdown();
        }
        MultiQueueThreadManager _thread_manager{4};

        void _multi_queue_test(size_t iter_count, size_t queue_count) {
            std::shared_ptr<std::queue<size_t>[]> in_queues(new std::queue<size_t>[queue_count]);
            std::shared_ptr<std::queue<size_t>[]> out_queues(new std::queue<size_t>[queue_count]);

            // generate random numbers
            std::vector<size_t> random_input;
            random_input.reserve(iter_count);

            // Seed for random number generation
            std::random_device dev;
            std::mt19937 rng(dev());

            // Define the range
            // Generates numbers between 1 and max uint32_t
            std::uniform_int_distribution<std::mt19937::result_type> dist(1, std::numeric_limits<uint32_t>::max());

            // Generate and print a random number
            for (size_t i = 0; i < iter_count; i++) {
                uint32_t random_number = dist(rng);
                random_input.push_back(random_number);
            }

            std::atomic<size_t> cb_completed_count = 0;
            // Create callbacks
            for (auto number: random_input) {
                uint32_t queue_id = number % queue_count;
                in_queues[queue_id].push(number);
                std::queue<size_t> &out_queue_ref = out_queues[queue_id];
                MultiQueueRequestPtr request = std::make_shared<MultiQueueRequest>(queue_id,
                    [number, &out_queue_ref, &cb_completed_count]() {
                        out_queue_ref.push(number);
                        cb_completed_count++;
                        cb_completed_count.notify_one();
                    }
                );
                _thread_manager.queue_request(request);
            }
            while (cb_completed_count != iter_count) {
                size_t cb_completed_count_old = cb_completed_count;
                cb_completed_count.wait(cb_completed_count_old);
            }
            for (size_t i = 0; i < queue_count; i++) {
                while(!in_queues[i].empty()) {
                    EXPECT_FALSE(out_queues[i].empty());
                    EXPECT_EQ(in_queues[i].front(), out_queues[i].front());
                    in_queues[i].pop();
                    out_queues[i].pop();
                }
            }
        }
    };

    TEST_F(MultiQueueThreadManager_Test, TestSingleRequest) {
        std::atomic<uint32_t> value = 0;
        MultiQueueRequestPtr request = std::make_shared<MultiQueueRequest>(1,
            [&value]() {
                value++;
                value.notify_one();
            }
        );
        _thread_manager.queue_request(request);
        value.wait(0);
        EXPECT_EQ(value, 1);
    }

    TEST_F(MultiQueueThreadManager_Test, TestMultipleRequests) {
        _multi_queue_test(200, 2);
    }
    TEST_F(MultiQueueThreadManager_Test, TestManyRequest) {
        _multi_queue_test(100000, 32);
    }
};