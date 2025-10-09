#pragma once

#include <cstddef>
#include <cstdint>

typedef uint32_t Oid;
typedef uintptr_t Datum;
typedef size_t Size;
typedef uint32_t CommandId;

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

#define MaxAllocSize ((Size) 0x3fffffff)

#define AssertMacro(condition)	((void)true)

/*
 * Make a const pointer appear non-const when needed (e.g., for lexer APIs
 * that require mutable char*). In C++, use const_cast to satisfy the parser
 * and avoid GCC C-only builtins. In C, fall back to a plain cast.
 */
#ifdef __cplusplus
#define unconstify(underlying_type, expr) \
    (const_cast<underlying_type>(expr))
#else
#define unconstify(underlying_type, expr) \
    ((underlying_type) (expr))
#endif

#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

/*
 * Max
 *		Return the maximum of two numbers.
 */
 #define Max(x, y)		((x) > (y) ? (x) : (y))

 /*
  * Min
  *		Return the minimum of two numbers.
  */
 #define Min(x, y)		((x) < (y) ? (x) : (y))
