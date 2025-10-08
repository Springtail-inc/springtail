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

constexpr int32_t NAMEDATALEN = 64;
constexpr int32_t MAXDIM = 6;
constexpr int32_t MAXIMUM_ALIGNOF = 8;
constexpr int32_t ALIGNOF_DOUBLE = 8;
constexpr int32_t ALIGNOF_INT = 4;
constexpr int32_t ALIGNOF_LONG = 8;
constexpr int32_t ALIGNOF_PG_INT128_TYPE = 16;
constexpr int32_t ALIGNOF_SHORT = 2;

#define SHORTALIGN(LEN)			TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(ALIGNOF_INT, (LEN))
#define LONGALIGN(LEN)			TYPEALIGN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

#define AssertMacro(condition)	((void)true)
