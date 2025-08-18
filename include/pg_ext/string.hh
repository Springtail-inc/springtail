#pragma once

#include <pg_ext/export.hh>
#include <pg_ext/memory.hh>
#include <pg_ext/fmgr.hh>

#include <cstddef>

//// EXPORTED INTERFACES
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
