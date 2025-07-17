#pragma once

#include <cstddef>
#include "export.hh"

//// EXPORTED INTERFACES

extern "C" PGEXT_API char *lowerstr(const char *str);
extern "C" PGEXT_API char *lowerstr_with_len(const char *str, int len);
extern "C" PGEXT_API int pg_snprintf(char *str, size_t count, const char *fmt, ...);
extern "C" PGEXT_API int pg_database_encoding_max_length(void);
extern "C" PGEXT_API int pg_mblen(const char *mbstr);
extern "C" PGEXT_API int pg_mb2wchar_with_len(const char *mbstr, wchar_t *wstr, int len);
extern "C" PGEXT_API void *pg_detoast_datum(const void *toast_datum);
extern "C" PGEXT_API void *pg_detoast_datum_packed(const void *toast_datum);
