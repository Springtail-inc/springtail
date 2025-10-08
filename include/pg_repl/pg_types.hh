#pragma once

#include <fstream>
#include <cstdint>
#include <cstring>
#include <sys/time.h>

namespace springtail
{
    #define BOOLOID      16
    #define CHAROID      18
    #define NAMEOID      19
    #define INT8OID      20
    #define INT2OID      21
    #define INT4OID      23
    #define TEXTOID      25
    #define OIDOID       26
    #define TIDOID       27
    #define FLOAT4OID    700
    #define FLOAT8OID    701
    #define REGTYPEOID   2206
    #define CSTRINGOID   2275

    #define  TYPALIGN_CHAR			'c' /* char alignment (i.e. unaligned) */
    #define  TYPALIGN_SHORT			's' /* short alignment (typically 2 bytes) */
    #define  TYPALIGN_INT			'i' /* int alignment (typically 4 bytes) */
    #define  TYPALIGN_DOUBLE		'd' /* double alignment (often 8 bytes) */

    /** Defined by CMake pass in with -D */
    #if !defined(CXX_BYTE_ORDER)
    #error Error CXX_BYTE_ORDER not defined
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
        return __builtin_bswap16(x);
    }

    static inline uint32_t
    pg_bswap32(uint32_t x)
    {
        return __builtin_bswap32(x);
    }

    static inline uint64_t
    pg_bswap64(uint64_t x)
    {
        return __builtin_bswap64(x);
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
    static inline void sendint64(int64_t i, char * const buffer)
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
    static inline void sendint32(int32_t i, char * const buffer)
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
    static inline void sendint16(int16_t i, char * const buffer)
    {
         int16_t n16 = pg_hton16(i);
         std::memcpy(buffer, &n16, 2);
    }

    /**
     * @brief Copy 8bit int/char into buffer
     * @param i value to copy into buffer
     * @param buffer pointer to preallocated buffer
     */
    static inline void sendint8(int8_t i, char * const buffer)
    {
        *buffer = i;
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

    /**
     * @brief Copy 8 bit int from buffer
     *
     * @param buffer buffer containing data
     * @return 8 bit int
     */
    static inline int8_t recvint8(const char *buffer)
    {
        return *buffer;
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
        return recvint64(buffer);
    }

    static inline int8_t recvint8(std::fstream &stream)
    {
        char buffer;
        stream.read(&buffer, 1);
        return buffer;
    }


    /** postgres command uses time as defined since 01/01/2000 00:00:00
        this is the number of msec from 1970 to 2000 */
    static const int64_t MSEC_SINCE_Y2K = 946684800000LL;

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
