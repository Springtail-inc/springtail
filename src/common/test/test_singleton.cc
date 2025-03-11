#include <gtest/gtest.h>
#include <common/common.hh>
#include <common/logging.hh>
#include <common/singleton.hh>

using namespace springtail;

namespace {
    class TestSingleton : public Singleton<TestSingleton> {
        friend class Singleton<TestSingleton>;
    public:
        TestSingleton() = default;
        ~TestSingleton() = default;

        void set_value(int value) {
            _value = value;
        }

        int get_value() {
            return _value;
        }

    private:
        int _value = 0;

    protected:
        void _internal_shutdown() override {
            _value = 0;
        }
    };

    class TestSingleton2 : public Singleton<TestSingleton2> {
            friend class Singleton<TestSingleton2>;
    public:
        TestSingleton2() = default;
        ~TestSingleton2() = default;

        void set_value(int value) {
            _value = value;
        }

        int get_value() {
            return _value;
        }

    private:
        int _value = 0;

    protected:
        void _internal_shutdown() override {
            _value = 0;
        }
    };

    TEST(SingletonTest, SingletonTest) {
        TestSingleton::get_instance()->set_value(10);
        TestSingleton2::get_instance()->set_value(20);

        EXPECT_EQ(TestSingleton::get_instance()->get_value(), 10);
        EXPECT_EQ(TestSingleton2::get_instance()->get_value(), 20);

        TestSingleton::shutdown();
        TestSingleton2::shutdown();
    }
}