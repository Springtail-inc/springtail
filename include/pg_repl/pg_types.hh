#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <sys/time.h>

namespace springtail
{
    /** Defined by CMake pass in with -D */
    #if !defined(CXX_BYTE_ORDER)
    #error Error CXX_BYTE_ORDER not defined
    #endif

    #ifndef UINT64CONST
    #define UINT64CONST(c) UINT64_C(c)
    #endif

    #if CXX_BYTE_ORDER == BIG_ENDIAN

    #define pg_hton16(x)        (x)
    #define pg_hton32(x)        (x)
    #define pg_hton64(x)        (x)

    #define pg_ntoh16(x)        (x)
    #define pg_ntoh32(x)        (x)
    #define pg_ntoh64(x)        (x)

    #else

    // From: pg_bswap.h
    static inline uint16_t
    pg_bswap16(uint16_t x)
    {
        return
            ((x << 8) & 0xff00) |
            ((x >> 8) & 0x00ff);
    }

    static inline uint32_t
    pg_bswap32(uint32_t x)
    {
        return
            ((x << 24) & 0xff000000) |
            ((x << 8) & 0x00ff0000) |
            ((x >> 8) & 0x0000ff00) |
            ((x >> 24) & 0x000000ff);
    }

    static inline uint64_t
    pg_bswap64(uint64_t x)
    {
        return
            ((x << 56) & UINT64CONST(0xff00000000000000)) |
            ((x << 40) & UINT64CONST(0x00ff000000000000)) |
            ((x << 24) & UINT64CONST(0x0000ff0000000000)) |
            ((x << 8) & UINT64CONST(0x000000ff00000000)) |
            ((x >> 8) & UINT64CONST(0x00000000ff000000)) |
            ((x >> 24) & UINT64CONST(0x0000000000ff0000)) |
            ((x >> 40) & UINT64CONST(0x000000000000ff00)) |
            ((x >> 56) & UINT64CONST(0x00000000000000ff));
    }

    #define pg_hton16(x)        pg_bswap16(x)
    #define pg_hton32(x)        pg_bswap32(x)
    #define pg_hton64(x)        pg_bswap64(x)

    #define pg_ntoh16(x)        pg_bswap16(x)
    #define pg_ntoh32(x)        pg_bswap32(x)
    #define pg_ntoh64(x)        pg_bswap64(x)

    #endif  /* CXX_BYTE_ORDER == BIG_ENDIAN */

    /** Log sequence number */
    typedef uint64_t LSN_t;

    /** Invalid LSN is 0 */
    static const LSN_t INVALID_LSN = 0;

    /**
     * @brief Copy 64bit int into buffer in network format
     *
     * @param i value to copy into buffer
     * @param buffer pointer to preallocated buffer
     */
    static inline void sendint64(int64_t i, char *buffer)
    {
         int64_t n64 = pg_hton64(i);
         std::memcpy(buffer, &n64, 8);
    }

    /**
     * @brief Copy 64bit int into buffer in network format
     *
     * @param i value to copy into buffer
     * @param buffer pointer to preallocated buffer
     */
    static inline void sendint32(int32_t i, char *buffer)
    {
         int32_t n32 = pg_hton32(i);
         std::memcpy(buffer, &n32, 4);
    }

    /**
     * @brief Copy 16bit int into buffer in network format
     *
     * @param i value to copy into buffer
     * @param buffer pointer to preallocated buffer
     */
    static inline void sendint16(int16_t i, char *buffer)
    {
         int16_t n16 = pg_hton16(i);
         std::memcpy(buffer, &n16, 2);
    }

    /**
     * @brief Copy 64 bit int from buffer from network format to host format
     *
     * @param buffer buffer containing data
     * @return 64 bit int
     */
    static inline int64_t recvint64(const char *buffer)
    {
        int64_t n64;
        std::memcpy(&n64, buffer, 8);
        n64 = pg_ntoh64(n64);
        return n64;
    }


    /**
     * @brief Copy 32 bit int from buffer from network format to host format
     *
     * @param buffer buffer containing data
     * @return 32 bit int
     */
    static inline int32_t recvint32(const char *buffer)
    {
        int32_t n32;
        std::memcpy(&n32, buffer, 4);
        n32 = pg_ntoh32(n32);
        return n32;
    }


    /**
     * @brief Copy 16 bit int from buffer from network format to host format
     *
     * @param buffer buffer containing data
     * @return 16 bit int
     */
    static inline int16_t recvint16(const char *buffer)
    {
        int16_t n16;
        std::memcpy(&n16, buffer, 2);
        n16 = pg_ntoh16(n16);
        return n16;
    }

    static inline int16_t recvint16(std::fstream &stream)
    {
        char buffer[2];
        stream.read(buffer, 2);
        return recvint16(buffer);
    }

    static inline int32_t recvint32(std::fstream &stream)
    {
        char buffer[4];
        stream.read(buffer, 4);
        return recvint32(buffer);
    }

    static inline int64_t recvint64(std::fstream &stream)
    {
        char buffer[8];
        stream.read(buffer, 8);
        return recvint16(buffer);
    }

    static inline int8_t recvint8(std::fstream &stream)
    {
        char buffer;
        stream.read(&buffer, 1);
        return buffer;
    }


    /** postgres command uses time as defined since 01/01/2000 00:00:00
        this is the number of msec from 1970 to 2000 */
    static const int64_t MSEC_SINCE_Y2K = 946684800000L;

    /**
     * @brief Get number milliseconds since 01/01/2000 00:00
     * @return number of milliseconds
     */
    static inline int64_t get_pgtime_in_millis()
    {
        struct timeval t;
        gettimeofday(&t, nullptr);
        int64_t now = (int64_t)t.tv_sec * 1000 + t.tv_usec / 1000;
        return now - MSEC_SINCE_Y2K;
    }
}
