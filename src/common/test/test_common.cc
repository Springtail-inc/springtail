#include <gtest/gtest.h>
#include <common/init.hh>
#include <common/logging.hh>
#include <common/json.hh>
#include <common/circular_buffer.hh>

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
    springtail_init_test();
    LOG_DEBUG(0x01, "Test log message, no args");
    LOG_DEBUG(0x01, "Test log message: 1 arg {}", 1);
    LOG_DEBUG(0x01, "Test log message: 2 args {} {}", 1, 2);
    LOG_DEBUG(0x01, "Test log message: 3 args {} {} {}", 1, 2, 3);
    LOG_DEBUG(0x01, "Test log message: 4 args {} {} {} {}", 1, 2, 3, 4);
    LOG_DEBUG(0x01, "Test log message: 5 args {} {} {} {} {}", 1, 2, 3, 4, 5);
    LOG_DEBUG(0x01, "Test log message: 6 args {} {} {} {} {} {}", 1, 2, 3, 4, 5, 6);
    springtail_shutdown();
}

TEST(CommonTest, EscapeQuotedString) {
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

TEST(CommonTest, SplitQuotedString) {
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

TEST(JsonTest, Json)
{
    nlohmann::json json = R"(
        {
            "key1": "value1",
            "key2": 2,
            "key3": true,
            "key4": "false",
            "key5": "45",
            "key6": "s"
        }
    )"_json;
    ASSERT_EQ(Json::get_or<std::string>(json, "key1", "test"), "value1");
    ASSERT_EQ(Json::get_or<int>(json, "key2", 0), 2);
    ASSERT_EQ(Json::get_or<bool>(json, "key3", false), true);
    ASSERT_EQ(Json::get_or<bool>(json, "key4", true), false);
    ASSERT_EQ(Json::get_or<int>(json, "key5", 0), 45);
    ASSERT_EQ(Json::get_or<std::string>(json, "key5", "0"), "45");
    ASSERT_EQ(Json::get_or<char>(json, "key6", 'a'), 's');
}

TEST(CommonTest, CircularBuffer) {
    CircularBuffer<int> cb{3};

    cb.put(2);
    ASSERT_EQ(cb.size(), 1);
    ASSERT_EQ(cb.next(), 2);
    ASSERT_EQ(cb.size(), 0);
    ASSERT_EQ(cb.empty(), true);

    //this should force the loop around
    cb.put(2);
    cb.put(3);
    cb.put(4);
    ASSERT_EQ(cb.next(), 2);
    ASSERT_EQ(cb.next(), 3);
    ASSERT_EQ(cb.next(), 4);
    ASSERT_EQ(cb.size(), 0);

    cb.put(10);
    ASSERT_EQ(cb.size(), 1);
    ASSERT_EQ(cb.empty(), false);
    ASSERT_EQ(cb.next(), 10);
    ASSERT_EQ(cb.size(), 0);
}
