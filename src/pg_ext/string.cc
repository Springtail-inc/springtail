#include <pg_ext/string.hh>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdarg>
#include <cwchar>

char *lowerstr(const char *str) {
    size_t len = strlen(str);
    char *result = static_cast<char*>(malloc(len + 1));
    if (!result) return NULL;

    for (size_t i = 0; i < len; ++i)
        result[i] = tolower((unsigned char)str[i]);
    result[len] = '\0';

    return result;
}

char *lowerstr_with_len(const char *str, int len) {
    char *result = static_cast<char*>(malloc(len + 1));
    if (!result) return NULL;

    for (int i = 0; i < len; ++i)
        result[i] = tolower((unsigned char)str[i]);
    result[len] = '\0';

    return result;
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

int pg_mblen(const char *mbstr) {
    return mblen(mbstr, MB_CUR_MAX);
}

int pg_mb2wchar_with_len(const char *mbstr, wchar_t *wstr, int len) {
    int wlen = 0;
    while (len > 0) {
        wchar_t wc;
        int clen = mbtowc(&wc, mbstr, len);
        if (clen == -1)
            return -1; // error
        if (wlen >= MB_CUR_MAX)
            return -1; // too many characters
        wstr[wlen] = wc;
        wlen++;
    }
    return wlen;
}

void *pg_detoast_datum(const void *toast_datum) {
    return (void *)toast_datum;
}

void *pg_detoast_datum_packed(const void *toast_datum) {
    return (void *)toast_datum;
}
