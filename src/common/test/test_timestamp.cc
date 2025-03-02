#include <gtest/gtest.h>

#include <chrono>
#include <common/timestamp.hh>
#include <sstream>

using namespace springtail;

TEST(TimestampTest, DefaultConstructor)
{
    PostgresTimestamp ts;
    EXPECT_EQ(ts.micros(), 0);
}

TEST(TimestampTest, ExplicitConstructor)
{
    PostgresTimestamp ts(1000000);  // 1 second after epoch
    EXPECT_EQ(ts.micros(), 1000000);
}

TEST(TimestampTest, SystemTimeConversion)
{
    // Create timestamp for 2020-01-01 00:00:00 UTC
    auto sys_time = std::chrono::system_clock::from_time_t(1577836800);  // 2020-01-01 UTC

    auto ts = PostgresTimestamp::from_system_time(sys_time);
    auto converted = ts.to_system_time();

    // Should round-trip correctly
    EXPECT_EQ(std::chrono::system_clock::to_time_t(sys_time),
              std::chrono::system_clock::to_time_t(converted));
}

TEST(TimestampTest, Comparison)
{
    PostgresTimestamp ts1(1000000);
    PostgresTimestamp ts2(2000000);
    PostgresTimestamp ts3(1000000);

    EXPECT_LT(ts1, ts2);
    EXPECT_GT(ts2, ts1);
    EXPECT_EQ(ts1, ts3);
    EXPECT_NE(ts1, ts2);
}

TEST(TimestampTest, FormattingBasic)
{
    std::stringstream ss;
    ss << PostgresTimestamp(0);
    EXPECT_EQ(ss.str(), "2000-01-01 00:00:00.000000 UTC");
}

TEST(TimestampTest, FormattingComplex)
{
    // Create timestamp for 2020-01-01 12:34:56.789012 UTC
    auto sys_time = std::chrono::system_clock::from_time_t(0);  // 1970-01-01 00:00:00 UTC
    sys_time += std::chrono::hours(24);                         // 1 day
    sys_time += std::chrono::hours(2);                          // 2 hours
    sys_time += std::chrono::minutes(34);                       // 34 minutes
    sys_time += std::chrono::seconds(56);                       // 56 seconds
    sys_time += std::chrono::microseconds(789012);              // 789012 microseconds

    auto ts = PostgresTimestamp::from_system_time(sys_time);

    std::stringstream ss;
    ss << ts;

    EXPECT_EQ(ss.str(), "1970-01-02 02:34:56.789012 UTC");
}

TEST(TimestampTest, UnixNanoseconds)
{
    // Test zero timestamp (2000-01-01 00:00:00 UTC)
    PostgresTimestamp zero(0);
    EXPECT_EQ(zero.to_unix_ns(), PostgresTimestamp::POSTGRES_TO_UNIX_EPOCH_MICROS * 1000LL);

    // Test one second after PostgreSQL epoch
    PostgresTimestamp one_sec(1000000);  // 1 second after PG epoch
    EXPECT_EQ(one_sec.to_unix_ns(), (PostgresTimestamp::POSTGRES_TO_UNIX_EPOCH_MICROS + 1000000LL) * 1000LL);
}
