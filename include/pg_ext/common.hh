#pragma once

#include <cstddef>
#include <cstdint>

typedef uint32_t Oid;
typedef uintptr_t Datum;
typedef size_t Size;

#define InvalidOid (Oid(0))
#define FLEXIBLE_ARRAY_MEMBER

#define TYPEALIGN(ALIGNVAL,LEN)  \
(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define NAMEDATALEN 64
#define MAXDIM 6
#define MAXIMUM_ALIGNOF 8
#define ALIGNOF_DOUBLE 8
#define ALIGNOF_INT 4
#define ALIGNOF_LONG 8
#define ALIGNOF_PG_INT128_TYPE 16
#define ALIGNOF_SHORT 2

#define SHORTALIGN(LEN)			TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(ALIGNOF_INT, (LEN))
#define LONGALIGN(LEN)			TYPEALIGN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

#define AssertMacro(condition)	((void)true)
