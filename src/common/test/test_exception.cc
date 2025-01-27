#include <gtest/gtest.h>

// Test to test whether exceptions are thrown and caught correctly
// Some GCC versions have a bug where the exception is not caught
TEST(ExceptionTest, TestException)
{
    bool caught = false;
    try {
        throw std::runtime_error("test exception");
    } catch (const std::runtime_error &e) {
        EXPECT_STREQ(e.what(), "test exception");
        caught = true;
    }
    EXPECT_TRUE(caught);
}