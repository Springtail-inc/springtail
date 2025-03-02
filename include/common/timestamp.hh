#pragma once

#include <chrono>
#include <cstdint>

namespace springtail {

/**
 * @brief Class representing a PostgreSQL timestamp, which uses microseconds since 2000-01-01 as its
 * epoch
 */
class PostgresTimestamp {
public:
    // PostgreSQL epoch is 2000-01-01
    static constexpr int64_t POSTGRES_TO_UNIX_EPOCH_MICROS = 946684800000000LL;

    /**
     * @brief Default constructor, initializes timestamp to Jan 1, 2000
     */
    PostgresTimestamp() : _micros(0) {}

    /**
     * @brief Construct from PostgreSQL microseconds timestamp
     */
    explicit PostgresTimestamp(int64_t pg_micros) : _micros(pg_micros) {}

    /**
     * @brief Get the raw PostgreSQL microseconds value
     */
    int64_t micros() const { return _micros; }

    std::strong_ordering operator<=>(const PostgresTimestamp& other) const = default;

    /**
     * @brief Convert to system_clock time_point
     */
    std::chrono::system_clock::time_point to_system_time() const;

    /**
     * @brief Create from system_clock time_point
     */
    static PostgresTimestamp from_system_time(std::chrono::system_clock::time_point tp);

    /**
     * @brief Stream operator to print human-readable timestamp
     */
    friend std::ostream& operator<<(std::ostream& os, const PostgresTimestamp& ts);

    /**
     * @brief Convert to Unix nanoseconds (nanoseconds since 1970-01-01)
     */
    int64_t to_unix_ns() const;

private:
    int64_t _micros;  ///< Microseconds since 2000-01-01
};

}  // namespace springtail
