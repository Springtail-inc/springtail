#pragma once

#include <pg_ext/export.hh>

#include <signal.h>
#include <cstdio>

extern "C" PGEXT_API volatile sig_atomic_t InterruptPending;
extern "C" PGEXT_API void ProcessInterrupts();


extern "C" PGEXT_API bool errstart(int elevel,
                                   const char *domain);
extern "C" PGEXT_API bool errstart_cold(int elevel,
                                        const char *domain);
extern "C" PGEXT_API void errfinish(const char *filename, int lineno, const char *funcname);
extern "C" PGEXT_API int errcode(int sqlerrcode);
extern "C" PGEXT_API int errmsg(const char *fmt, ...);
extern "C" PGEXT_API int errmsg_internal(const char *fmt, ...);
extern "C" PGEXT_API int pg_fprintf(FILE *stream, const char *fmt,...);
extern "C" PGEXT_API bool GetDefaultCharSignedness(void);

extern "C" PGEXT_API void errsave_finish(const char *filename, int lineno, const char *funcname);
extern "C" PGEXT_API void errdetail(const char *fmt, ...);
extern "C" PGEXT_API void errsave_start(const char *filename, int lineno, const char *funcname);
