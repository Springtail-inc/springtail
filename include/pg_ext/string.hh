#pragma once

#include <pg_ext/common.hh>
#include <pg_ext/export.hh>
#include <pg_ext/varatt.hh>

#include <cstdarg>
#include <cstddef>
#include <cstdint>

#ifdef strerror_r
#undef strerror_r
#endif
#ifdef pg_strerror_r
#undef pg_strerror_r
#endif

constexpr int MAX_SAFE_LEN = 1024 * 1024;

using text = struct varlena;
struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
};

using StringInfo = StringInfoData *;
using bytea = struct varlena;

struct ParseState {
    const char *p_sourcetext;
};

#define appendStringInfoCharMacro(str, ch) \
    (((str)->len + 1 >= (str)->maxlen)     \
         ? appendStringInfoChar(str, ch)   \
         : (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))

//// EXPORTED INTERFACES
extern "C" PGEXT_API void initStringInfo(StringInfo str);
extern "C" PGEXT_API void appendBinaryStringInfo(StringInfo str, const void *data, int datalen);
extern "C" PGEXT_API void appendBinaryStringInfoNT(StringInfo str, const void *data, int datalen);
extern "C" PGEXT_API void appendStringInfoString(StringInfo str, const char *s);
extern "C" PGEXT_API void appendStringInfoChar(StringInfo str, char ch);
extern "C" PGEXT_API void appendStringInfo(StringInfo str, const char *fmt, ...);
extern "C" PGEXT_API int appendStringInfoVA(StringInfo str, const char *fmt, va_list args);
extern "C" PGEXT_API void enlargeStringInfo(StringInfo str, int needed);
extern "C" PGEXT_API void resetStringInfo(StringInfo str);

extern "C" PGEXT_API int parser_errposition(ParseState *pstate, int location);
extern "C" PGEXT_API bool scanner_isspace(char ch);
extern "C" PGEXT_API char *pg_strerror_r(int errnum, char *buf, size_t buflen);

extern "C" PGEXT_API text *cstring_to_text_with_len(const char *s, int len);

extern "C" PGEXT_API char *lowerstr(const char *str);
extern "C" PGEXT_API char *lowerstr_with_len(const char *str, int len);
extern "C" PGEXT_API char *upperstr(const char *str);
extern "C" PGEXT_API char *upperstr_with_len(const char *str, int len);
extern "C" PGEXT_API int pg_snprintf(char *str, size_t count, const char *fmt, ...);
extern "C" PGEXT_API int pg_database_encoding_max_length(void);
extern "C" PGEXT_API int pg_mblen(const char *mbstr);
extern "C" PGEXT_API int pg_mbstrlen_with_len(const char *mbstr, int len);
extern "C" PGEXT_API int pg_mb2wchar_with_len(const char *mbstr, wchar_t *wstr, int len);
extern "C" PGEXT_API int pg_wchar2mb_with_len(const wchar_t *wstr, char *mbstr, int len);
extern "C" PGEXT_API int t_isalnum(const char *ptr);
extern "C" PGEXT_API int t_isspace(const char *p);
extern "C" PGEXT_API int t_isdigit(const char *p);
extern "C" PGEXT_API int t_isalpha(const char *p);
extern "C" PGEXT_API char *str_tolower(const char *buff, size_t nbytes, Oid collid);
extern "C" PGEXT_API char *str_toupper(const char *buff, size_t nbytes, Oid collid);
extern "C" PGEXT_API char *pstrdup(const char *in);

extern "C" PGEXT_API int pg_strcasecmp(const char *s1, const char *s2);
