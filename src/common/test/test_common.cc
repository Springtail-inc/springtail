#include <gtest/gtest.h>
#include <common/common.hh>
#include <common/logging.hh>

using namespace springtail;

TEST(CommonTest, SplitString) {
    std::string str = "a,b,c";
    std::vector<std::string> result;
    common::split_string(",", str, result);
    ASSERT_EQ(result.size(), 3);
    ASSERT_EQ(result[0], "a");
    ASSERT_EQ(result[1], "b");
    ASSERT_EQ(result[2], "c");
}

TEST(CommonTest, SplitString2) {
    std::string str = "a,b,c,";
    std::vector<std::string> result;
    common::split_string(",", str, result);
    ASSERT_EQ(result.size(), 3);
    ASSERT_EQ(result[0], "a");
    ASSERT_EQ(result[1], "b");
    ASSERT_EQ(result[2], "c");
}

TEST(LoggingTest, Logging) {
    springtail::springtail_init();
    SPDLOG_DEBUG_MODULE(0x01, "Test log message, no args");
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 1 arg {}", 1);
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 2 args {} {}", 1, 2);
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 3 args {} {} {}", 1, 2, 3);
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 4 args {} {} {} {}", 1, 2, 3, 4);
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 5 args {} {} {} {} {}", 1, 2, 3, 4, 5);
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 6 args {} {} {} {} {} {}", 1, 2, 3, 4, 5, 6);
}
