#pragma once
#include <pg_ext/export.hh>

#include <cstddef>
#include <cstdint>

typedef struct {
    int32_t length;
    int32_t elemtype;
    char data[];
} ArrayType;

typedef struct varlena bytea;

typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;

#define ARR_ELEMTYPE(a) ((a)->elemtype)

extern "C" PGEXT_API ArrayType *construct_array_builtin(const void *elems, int nelems, uint32_t elmtype, size_t elem_size);
extern "C" PGEXT_API int ArrayGetNItems(int ndim, const int *dims);
extern "C" PGEXT_API bool array_contains_nulls(ArrayType *array);
extern "C" PGEXT_API void enlargeStringInfo(StringInfo str, int needed);
extern "C" PGEXT_API void initStringInfo(StringInfo str);
extern "C" PGEXT_API void appendStringInfoChar(StringInfo str, char ch);

extern "C" PGEXT_API int pq_getmsgint(StringInfo str, int size);
extern "C" PGEXT_API void pq_begintypsend(StringInfo str);
extern "C" PGEXT_API double pq_getmsgfloat8(StringInfo msg);
extern "C" PGEXT_API void pq_sendfloat8(StringInfo str, double value);
extern "C" PGEXT_API bytea *pq_endtypsend(StringInfo buf);

extern "C" PGEXT_API void appendStringInfoString(StringInfo str, const char *s);
