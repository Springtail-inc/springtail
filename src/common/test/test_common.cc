#include <gtest/gtest.h>
#include <common/init.hh>
#include <common/logging.hh>
#include <common/json.hh>
#include <common/circular_buffer.hh>
#include <common/event_frequency.hh>

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
    LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1, "Test log message, no args");
    LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1, "Test log message: 1 arg {}", 1);
    LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1, "Test log message: 2 args {} {}", 1, 2);
    LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1, "Test log message: 3 args {} {} {}", 1, 2, 3);
    LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1, "Test log message: 4 args {} {} {} {}", 1, 2, 3, 4);
    LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1, "Test log message: 5 args {} {} {} {} {}", 1, 2, 3, 4, 5);
    LOG_DEBUG(LOG_PG_REPL, LOG_LEVEL_DEBUG1, "Test log message: 6 args {} {} {} {} {} {}", 1, 2, 3, 4, 5, 6);
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

TEST(CommonTest, EventFrequency) {
    using clock = std::chrono::steady_clock;

    {
        EventFrequency<20> ef;


        auto start = clock::now();
        for (size_t i = 0; i != 15; ++i) {
            ef.event();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        auto end = clock::now();

        double exp_freq = 15.0 / std::chrono::duration<double>(end - start).count();
        auto f = ef.frequency();
        // the multipliers are to account for some timing variations of the sleep_for
        // and clock resolutions
        ASSERT_TRUE(f > exp_freq*0.9 && f < exp_freq*1.1);

        start = clock::now();
        for (size_t i = 0; i != 90; ++i) {
            ef.event();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        end = clock::now();
    
        exp_freq = 90.0 / std::chrono::duration<double>(end - start).count();
        f = ef.frequency();
        ASSERT_TRUE(f > exp_freq*0.9 && f < exp_freq*1.1);

        for (size_t i = 0; i != 10; ++i) {
            ef.event();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        auto f1 = ef.frequency();
        ASSERT_LT(f1, f);

        start = clock::now();
        for (size_t i = 0; i != 120; ++i) {
            ef.event();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        end = clock::now();

        exp_freq = 120.0 / std::chrono::duration<double>(end - start).count();
        f = ef.frequency();
        // the multipliers are to account for some timing variations of the sleep_for
        // and clock resolutions
        ASSERT_TRUE(f > exp_freq*0.8 && f < exp_freq*1.2);
    }

    // corner case
    {
        EventFrequency<2> ef;

        ef.event();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        ef.event();

        auto f = ef.frequency();

        ASSERT_TRUE(f > 0.9 && f < 1.1);

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        ef.event();

        f = ef.frequency();

        ASSERT_TRUE(f > 0.9 && f < 1.1);
    }
}
