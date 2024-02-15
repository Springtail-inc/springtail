#include <gtest/gtest.h>
#include <common/common.hh>

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
