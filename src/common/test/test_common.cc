#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/logging.hh>

using namespace springtail;

TEST(CommonTest, SplitString)
{
    std::string str = "a,b,c";
    std::vector<std::string> result;
    common::split_string(",", str, result);
    ASSERT_EQ(result.size(), 3);
    ASSERT_EQ(result[0], "a");
    ASSERT_EQ(result[1], "b");
    ASSERT_EQ(result[2], "c");
}

TEST(CommonTest, SplitString2)
{
    std::string str = "a,b,c,";
    std::vector<std::string> result;
    common::split_string(",", str, result);
    ASSERT_EQ(result.size(), 3);
    ASSERT_EQ(result[0], "a");
    ASSERT_EQ(result[1], "b");
    ASSERT_EQ(result[2], "c");
}

TEST(LoggingTest, Logging)
{
    springtail::springtail_init();
    SPDLOG_DEBUG_MODULE(0x01, "Test log message, no args");
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 1 arg {}", 1);
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 2 args {} {}", 1, 2);
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 3 args {} {} {}", 1, 2, 3);
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 4 args {} {} {} {}", 1, 2, 3, 4);
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 5 args {} {} {} {} {}", 1, 2, 3, 4, 5);
    SPDLOG_DEBUG_MODULE(0x01, "Test log message: 6 args {} {} {} {} {} {}", 1, 2, 3, 4, 5, 6);
}

TEST(CommonTest, EscapeQuotedString)
{
    std::string str = "a\"b";
    std::string result = common::escape_quoted_string(str);
    ASSERT_EQ(result, "\"a\\\"b\"");
    ASSERT_EQ(common::unescape_quoted_string(result), str);

    str = "\"quoted\"";
    result = common::escape_quoted_string(str);
    ASSERT_EQ(result, "\"\\\"quoted\\\"\"");
    ASSERT_EQ(common::unescape_quoted_string(result), str);

    str = "a\"bc\"d";
    result = common::escape_quoted_string(str);
    ASSERT_EQ(result, "\"a\\\"bc\\\"d\"");
    ASSERT_EQ(common::unescape_quoted_string(result), str);
}

TEST(CommonTest, SplitQuotedString)
{
    std::string str = "a,b,c";
    std::vector<std::string> result;
    common::split_quoted_string(',', str, result);
    ASSERT_EQ(result.size(), 3);
    ASSERT_EQ(result[0], "a");
    ASSERT_EQ(result[1], "b");
    ASSERT_EQ(result[2], "c");
    result.clear();

    str = "a,\"b,c\"";
    common::split_quoted_string(',', str, result);
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result[0], "a");
    ASSERT_EQ(result[1], "\"b,c\"");
    result.clear();

    common::split_quoted_string(':', "\"hello\":b:c", result);
    ASSERT_EQ(result.size(), 3);
    ASSERT_EQ(result[0], "\"hello\"");
    ASSERT_EQ(result[1], "b");
    ASSERT_EQ(result[2], "c");
}