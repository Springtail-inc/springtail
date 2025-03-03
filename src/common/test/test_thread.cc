#include <memory>
#include <mutex>

#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/thread_pool.hh>

using namespace springtail;

namespace {
    class TestRequest;

    class TestPool : public testing::Test {
    public:
        void add(int a) {
            std::unique_lock lock{mutex};
            sum += a;
        }

    protected:
        static void SetUpTestSuite() {
            springtail_init_test();
        }

        static void TearDownTestSuite() {
            springtail_shutdown();
        }

        void SetUp() override {
            pool = std::make_shared<ThreadPool<TestRequest>>(2);
        }

        void TearDown() override {
        }

        std::shared_ptr<ThreadPool<TestRequest>> pool;
        std::mutex mutex;
        int sum = 0;
    };

    class TestRequest {
    public:
        TestRequest(int a, std::function<void(int)> fn)
            : _a(a), _fn(fn)
        {}

        void operator()() {
            _fn(_a);
        }

    private:
        int _a;
        std::function<void(int)> _fn;
    };

    TEST_F(TestPool, QueueTest)
    {
        pool->queue(std::make_shared<TestRequest>(1, [this](int a){ add(a); }));
        pool->queue(std::make_shared<TestRequest>(2, [this](int a){ add(a); }));
        pool->queue(std::make_shared<TestRequest>(3, [this](int a){ add(a); }));
        pool->queue(std::make_shared<TestRequest>(4, [this](int a){ add(a); }));
        pool->shutdown();

        ASSERT_EQ(sum, 10);
    }
}