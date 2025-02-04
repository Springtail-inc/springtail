#include <time.h>

#include <chrono>
#include <common/timestamp.hh>
#include <ctime>
#include <iomanip>

namespace springtail {

std::ostream&
operator<<(std::ostream& os, const PostgresTimestamp& ts)
{
    auto tp = ts.to_system_time();
    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                      tp - std::chrono::system_clock::from_time_t(tt))
                      .count();

    struct tm tm_buf;
    gmtime_r(&tt, &tm_buf);
    os << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S") << std::format(".{:06d} UTC", micros);
    return os;
}

std::chrono::system_clock::time_point
PostgresTimestamp::to_system_time() const
{
    auto unix_micros = _micros + POSTGRES_TO_UNIX_EPOCH_MICROS;
    return std::chrono::system_clock::from_time_t(0) + std::chrono::microseconds(unix_micros);
}

PostgresTimestamp
PostgresTimestamp::from_system_time(std::chrono::system_clock::time_point tp)
{
    auto unix_micros = std::chrono::duration_cast<std::chrono::microseconds>(
                           tp - std::chrono::system_clock::from_time_t(0))
                           .count();
    return PostgresTimestamp(unix_micros - POSTGRES_TO_UNIX_EPOCH_MICROS);
}

}  // namespace springtail
