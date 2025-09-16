#pragma once

#include <cstdint>

#define FLEXIBLE_ARRAY_MEMBER
typedef uint32_t Oid;
typedef uintptr_t Datum;
#define InvalidOid (Oid(0))

typedef union {
    struct /* Normal varlena (4-byte length) */
    {
        uint32_t va_header;
        char va_data[FLEXIBLE_ARRAY_MEMBER];
    } va_4byte;
    struct /* Compressed-in-line format */
    {
        uint32_t va_header;
        uint32_t va_tcinfo;                    /* Original data size (excludes header) and
                                              * compression method; see va_extinfo */
        char va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
    } va_compressed;
} varattrib_4b;

#define VARHDRSZ ((int32_t)sizeof(int32_t))
#define SET_VARSIZE_4B(PTR, len) (((varattrib_4b *)(PTR))->va_4byte.va_header = (len) & 0x3FFFFFFF)
#define SET_VARSIZE(PTR, len) SET_VARSIZE_4B(PTR, len)

#define VARDATA_4B(PTR) (((varattrib_4b *)(PTR))->va_4byte.va_data)
#define VARDATA(PTR) VARDATA_4B(PTR)
