#include <pg_ext/export.hh>
#include <pg_ext/string.hh>
#include <pg_ext/varatt.hh>

extern "C" PGEXT_API void pq_sendfloat8(StringInfo str, double value);
extern "C" PGEXT_API void pq_begintypsend(StringInfo str);
extern "C" PGEXT_API bytea *pq_endtypsend(StringInfo buf);
extern "C" PGEXT_API void pq_sendtext(StringInfo buf, const char *str, int slen);
extern "C" PGEXT_API uint32_t pq_getmsgint(StringInfo str, int size);
extern "C" PGEXT_API char *pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes);
extern "C" PGEXT_API double pq_getmsgfloat8(StringInfo msg);
