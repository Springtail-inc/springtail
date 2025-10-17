#pragma once

#include <pg_ext/common.hh>
#include <pg_ext/export.hh>

#include <cstddef>
#include <cstdint>

struct ArrayType {
    int32_t vl_len_;    /* varlena header (do not touch directly!) */
    int ndim;         /* # of dimensions */
    int32_t dataoffset; /* offset to data, or 0 if no bitmap */
    Oid elemtype;     /* element type OID */
};

#define ARR_SIZE(a) VARSIZE(a)
#define ARR_NDIM(a) ((a)->ndim)
#define ARR_HASNULL(a) ((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a) ((a)->elemtype)

#define ARR_DIMS(a) ((int *)(((char *)(a)) + sizeof(ArrayType)))
#define ARR_LBOUND(a) \
		((int *) (((char *) (a)) + sizeof(ArrayType) + \
				  sizeof(int) * ARR_NDIM(a)))

#define AllocSizeIsValid(size) ((Size) (size) <= MaxAllocSize)

#define ARR_NULLBITMAP(a)                                                                          \
    (ARR_HASNULL(a) ? (uint8_t *)(((char *)(a)) + sizeof(ArrayType) + 2 * sizeof(int) * ARR_NDIM(a)) \
                    : (uint8_t *)nullptr)

#define ARR_OVERHEAD_NONULLS(ndims) MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_OVERHEAD_WITHNULLS(ndims, nitems) MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims) + ((nitems) + 7) / 8)

#define ARR_DATA_OFFSET(a) \
		(ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))
#define ARR_DATA_PTR(a) \
		(((char *) (a)) + ARR_DATA_OFFSET(a))

#define fetchatt(A,T) fetch_att(T, (A)->attbyval, (A)->attlen)

extern "C" PGEXT_API ArrayType *construct_array_builtin(Datum *elems, int nelems, Oid elmtype);
extern "C" PGEXT_API int ArrayGetNItems(int ndim, const int *dims);
extern "C" PGEXT_API bool array_contains_nulls(ArrayType *array);
extern "C" PGEXT_API ArrayType *construct_empty_array(uint32_t elmtype);
extern "C" PGEXT_API ArrayType *construct_md_array(Datum *elems, bool *nulls, int ndims, int *dims, int *lbs, Oid elmtype, int elmlen, bool elmbyval, char elmalign);
extern "C" PGEXT_API void deconstruct_array_builtin(ArrayType *array, Oid elmtype, Datum **elemsp, bool **nullsp, int *nelemsp);
