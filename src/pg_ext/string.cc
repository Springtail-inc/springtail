#include <pg_ext/string.hh>
#include <pg_ext/memory.hh>

#include <cassert>
#include <cctype>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdarg>
#include <cwchar>
#include <new>
#include <stdexcept>

#include <common/logging.hh>

#define HIGHBIT (0x80)
#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch) & HIGHBIT)

#define appendStringInfoCharMacro(str, ch) \
    (((str)->len + 1 >= (str)->maxlen)     \
         ? appendStringInfoChar(str, ch)   \
         : (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))

char *lowerstr(const char *str) {
    return lowerstr_with_len(str, std::strlen(str));
}

static inline unsigned char tolower_ascii(unsigned char c)
{
    if (c >= 'A' && c <= 'Z') return (unsigned char)(c + 32);
    return c;
}

int
pg_mblen(const char *mbstr)
{
    const unsigned char lead = *(const unsigned char *)mbstr;
    int len = (lead < 0x80)             ? 1
              : ((lead & 0xE0) == 0xC0) ? 2
              : ((lead & 0xF0) == 0xE0) ? 3
              : ((lead & 0xF8) == 0xF0) ? 4
                                        : 1;

    return len;
}

char *lowerstr_with_len(const char *str, int len) {
    if (!str || len <= 0) {
        char *empty = (char *)palloc(1);
        if (empty) empty[0] = '\0';
        return empty;
    }

    char *buf = (char *)palloc((size_t)len + 1);  // +1 for null terminator
    if (!buf) return NULL;

    int i = 0, o = 0;

    while (i < len) {
        int m = pg_mblen(str + i);

        // Ensure we don’t overread beyond len
        if (m <= 0 || i + m > len) {
            m = 1;
        }

        if (m == 1) {
            // ASCII character — lowercase it
            unsigned char c = (unsigned char)str[i];
            buf[o++] = (char)tolower_ascii(c);
        } else {
            // Multibyte sequence — copy as-is
            memcpy(buf + o, str + i, m);
            o += m;
        }

        i += m;
    }

    buf[o] = '\0';

    return buf;
}

char *upperstr(const char *str) {
    return upperstr_with_len(str, std::strlen(str));
}

char *upperstr_with_len(const char *str, int len) {
    if (!str || len <= 0) {
        return nullptr;
    }

    char *result = (char *)palloc(len + 1);
    if (!result) return nullptr;

    for (size_t i = 0; i < len; i++) {
        result[i] = std::toupper((unsigned char)str[i]);
    }

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

int pg_mbstrlen_with_len(const char *mbstr, int len)
{
    if (!mbstr || len <= 0) return 0;
    int i = 0, chars = 0;
    while (i < len) {
        int m = pg_mblen(mbstr + i);
        if (m <= 0) m = 1;
        if (i + m > len) break;   // don't step past end
        i += m;
        chars++;
    }
    return chars;
}

int t_isalnum(const char *ptr) {
    if (!ptr || !*ptr) return 0;
    return t_isalpha(ptr) || t_isdigit(ptr);
}

int t_isspace(const char *p) {
    if (!p || !*p) return 0;
    unsigned char c = (unsigned char)*p;
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

int t_isdigit(const char *p) {
    if (!p || !*p) return 0;
    unsigned char c = (unsigned char)*p;
    return c >= '0' && c <= '9';
}

int t_isalpha(const char *p) {
    if (!p || !*p) return 0;
    unsigned char c = (unsigned char)*p;
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

char *str_tolower(const char *buff, size_t nbytes, Oid collid) {
    return lowerstr_with_len(buff, nbytes);
}

char *str_toupper(const char *buff, size_t nbytes, Oid collid) {
    return upperstr_with_len(buff, nbytes);
}

char *pstrdup(const char *in) {
    return strdup(in);
}

void
resetStringInfo(StringInfo str)
{
	str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}

void
initStringInfo(StringInfo str)
{
	int			size = 1024;	/* initial default buffer size */

	str->data = (char *) malloc(size);
	str->maxlen = size;
	resetStringInfo(str);
}

void
enlargeStringInfo(StringInfo str, int needed)
{
    int         newlen;
    int         required = str->len + needed + 1;  // +1 for null terminator

    if (needed < 0 ||
        needed > (INT_MAX - str->len - 1))
    {
        throw std::runtime_error("invalid string buffer enlargement request");
    }

    /* Do we have enough space already? */
    if (required <= str->maxlen)
        return;

    /*
     * We don't want to allocate just a little more space with each append;
     * for efficiency, double the buffer size each time it overflows.
     * Actually, we might need to more than double it if 'needed' is big...
     */
    newlen = 2 * str->maxlen;
    while (needed > (newlen - str->len - 1))
        newlen = 2 * newlen;

    /* Clamp to MaxAllocSize in case we went past it */
    if (newlen > (1024 * 1024 * 1024))  /* 1GB max for safety */
    {
        newlen = 1024 * 1024 * 1024;
        if (needed > (newlen - str->len - 1))
            throw std::runtime_error("string buffer size exceeds maximum allowed");
    }

    /* Now realloc the buffer */
    str->data = (char *)realloc(str->data, newlen);
    if (!str->data)
        throw std::bad_alloc();

    str->maxlen = newlen;
}

int
appendStringInfoVA(StringInfo str, const char *fmt, va_list args)
{
    int avail;
    size_t nprinted;

    assert(str != NULL);

    avail = str->maxlen - str->len;
    if (avail < 16) {
        return 32;
    }

    nprinted = vsnprintf(str->data + str->len, (size_t)avail, fmt, args);

    if (nprinted < (size_t)avail) {
        str->len += (int)nprinted;
        return 0;
    }

    str->data[str->len] = '\0';

    return (int)nprinted;
}

void
appendStringInfo(StringInfo str, const char *fmt, ...)
{
    int save_errno = errno;

    for (;;) {
        va_list args;
        int needed;

        /* Try to format the data. */
        errno = save_errno;
        va_start(args, fmt);
        needed = appendStringInfoVA(str, fmt, args);
        va_end(args);

        if (needed == 0) {
            break; /* success */
        }

        /* Increase the buffer size and try again. */
        enlargeStringInfo(str, needed);
    }
}

void
appendBinaryStringInfo(StringInfo str, const void *data, int datalen)
{
    assert(str != NULL);

    /* Make more room if needed */
    enlargeStringInfo(str, datalen);

    /* OK, append the data */
    memcpy(str->data + str->len, data, datalen);

    /* Null-terminate */
    str->data[str->len] = '\0';
}

void
appendBinaryStringInfoNT(StringInfo str, const void *data, int datalen)
{
	assert(str != NULL);

	/* Make more room if needed */
	enlargeStringInfo(str, datalen);

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
}

char *
pg_strerror_r(int errnum, char *buf, size_t buflen)
{
    return strerror_r(errnum, buf, buflen);
}

void appendStringInfoChar(StringInfo str, char ch) {
    if (str->len + 1 > str->maxlen) {
        enlargeStringInfo(str, 1);
    }
    str->data[str->len++] = ch;
    str->data[str->len] = '\0';
}

void pq_begintypsend(StringInfo str) {
    initStringInfo(str);
}

void appendStringInfoString(StringInfo str, const char *s) {
    if (str->len + strlen(s) + 1 > str->maxlen) {
        enlargeStringInfo(str, strlen(s) + 1);
    }
    std::strcat(str->data, s);
    str->len += strlen(s);
}

int
errposition(int cursorpos)
{
    // XXX Stubbed for now
    return 0; /* return value does not matter */
}

int
parser_errposition(ParseState *pstate, int location)
{
    int pos;

    /* No-op if location was not provided */
    if (location < 0) {
        return 0;
    }
    /* Can't do anything if source text is not available */
    if (pstate == NULL || pstate->p_sourcetext == NULL) {
        return 0;
    }
    /* Convert offset to character number */
    pos = pg_mbstrlen_with_len(pstate->p_sourcetext, location) + 1;
    /* And pass it to the ereport mechanism */
    return errposition(pos);
}

bool
scanner_isspace(char ch)
{
    /* This must match scan.l's list of {space} characters */
    if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f') {
        return true;
    }
    return false;
}

text
*cstring_to_text_with_len(const char *s, int len){
    text *result = (text *)palloc(len + VARHDRSZ);

    SET_VARSIZE(result, len + VARHDRSZ);
    memcpy(VARDATA(result), s, len);

    return result;
}

int
pg_strcasecmp(const char *s1, const char *s2)
{
    for (;;) {
        unsigned char ch1 = (unsigned char)*s1++;
        unsigned char ch2 = (unsigned char)*s2++;

        if (ch1 != ch2) {
            if (ch1 >= 'A' && ch1 <= 'Z') {
                ch1 += 'a' - 'A';
            } else if (IS_HIGHBIT_SET(ch1) && isupper(ch1)) {
                ch1 = tolower(ch1);
            }

            if (ch2 >= 'A' && ch2 <= 'Z') {
                ch2 += 'a' - 'A';
            } else if (IS_HIGHBIT_SET(ch2) && isupper(ch2)) {
                ch2 = tolower(ch2);
            }

            if (ch1 != ch2) {
                return (int)ch1 - (int)ch2;
            }
        }
        if (ch1 == 0) {
            break;
        }
    }
    return 0;
}

int
strncasecmp(const char *s1, const char *s2, size_t n)
{
	while (n-- > 0)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
				ch1 = tolower(ch1);

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
				ch2 = tolower(ch2);

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}
