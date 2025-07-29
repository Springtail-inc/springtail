#include <dlfcn.h>
#include <pg_ext/string.hh>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdarg>
#include <cwchar>

char *lowerstr(const char *str) {
    if (!str) return nullptr;

    size_t len = std::strlen(str);
    char *result = (char *)std::malloc(len + 1);
    if (!result) return nullptr;

    for (size_t i = 0; i < len; i++) {
        result[i] = std::tolower((unsigned char)str[i]);
    }

    result[len] = '\0';
    return result;
}

char *lowerstr_with_len(const char *str, int len) {
    if (!str || len <= 0) return nullptr;

    char *result = (char *)std::malloc(len + 1);
    if (!result) return nullptr;

    for (int i = 0; i < len; i++) {
        result[i] = std::tolower((unsigned char)str[i]);
    }

    result[len] = '\0';
    return result;
}

char *upperstr(const char *str) {
    if (!str) return nullptr;

    size_t len = std::strlen(str);
    char *result = (char *)std::malloc(len + 1);
    if (!result) return nullptr;

    for (size_t i = 0; i < len; i++) {
        result[i] = std::toupper((unsigned char)str[i]);
    }

    result[len] = '\0';
    return result;
}

char *upperstr_with_len(const char *str, int len) {
    if (!str || len <= 0) return nullptr;

    char *result = (char *)std::malloc(len + 1);
    if (!result) return nullptr;

    for (int i = 0; i < len; i++) {
        result[i] = std::toupper((unsigned char)str[i]);
    }

    result[len] = '\0';
    return result;
}

int pg_mblen(const char *mbstr) {
    unsigned char c = (unsigned char)*mbstr;

    if (c < 0x80) return 1;
    else if ((c & 0xE0) == 0xC0) return 2;
    else if ((c & 0xF0) == 0xE0) return 3;
    else if ((c & 0xF8) == 0xF0) return 4;

    // Invalid UTF-8 lead byte
    return 1;
}

int pg_snprintf(char *str, size_t count, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    int result = vsnprintf(str, count, fmt, args);
    va_end(args);
    return result;
}

int pg_database_encoding_max_length(void) {
    return 4;
}

int pg_mb2wchar_with_len(const char *from, wchar_t *to, int len) {
    int count = 0;
    const unsigned char *s = (const unsigned char *)from;

    while (len > 0 && *s) {
        int mblen = pg_mblen((const char *)s);
        if (mblen > len) break; // prevent buffer overrun

        uint32_t wc = 0;
        if (mblen == 1) {
            wc = s[0];
        } else if (mblen == 2) {
            wc = ((s[0] & 0x1F) << 6) | (s[1] & 0x3F);
        } else if (mblen == 3) {
            wc = ((s[0] & 0x0F) << 12) | ((s[1] & 0x3F) << 6) | (s[2] & 0x3F);
        } else if (mblen == 4) {
            wc = ((s[0] & 0x07) << 18) |
                 ((s[1] & 0x3F) << 12) |
                 ((s[2] & 0x3F) << 6) |
                 (s[3] & 0x3F);
        }

        to[count++] = wc;
        s += mblen;
        len -= mblen;
    }

    return count;
}

int pg_wchar2mb_with_len(const wchar_t *from, char *to, int len) {
    int bytes_written = 0;

    for (int i = 0; i < len; i++) {
        uint32_t wc = from[i];
        if (wc <= 0x7F) {
            *to++ = (char)wc;
            bytes_written += 1;
        } else if (wc <= 0x7FF) {
            *to++ = 0xC0 | ((wc >> 6) & 0x1F);
            *to++ = 0x80 | (wc & 0x3F);
            bytes_written += 2;
        } else if (wc <= 0xFFFF) {
            *to++ = 0xE0 | ((wc >> 12) & 0x0F);
            *to++ = 0x80 | ((wc >> 6) & 0x3F);
            *to++ = 0x80 | (wc & 0x3F);
            bytes_written += 3;
        } else if (wc <= 0x10FFFF) {
            *to++ = 0xF0 | ((wc >> 18) & 0x07);
            *to++ = 0x80 | ((wc >> 12) & 0x3F);
            *to++ = 0x80 | ((wc >> 6) & 0x3F);
            *to++ = 0x80 | (wc & 0x3F);
            bytes_written += 4;
        }
        // else: invalid, ignore or insert replacement char
    }

    return bytes_written;
}

int
t_isalnum(const char *ptr)
{
    return std::isalnum(*ptr);
}

char *str_tolower(const char *buff, size_t nbytes, Oid collid) {
    return lowerstr_with_len(buff, nbytes);
}

char *str_toupper(const char *buff, size_t nbytes, Oid collid) {
    return upperstr_with_len(buff, nbytes);
}
