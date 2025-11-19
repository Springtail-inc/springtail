#pragma once

#include <cstddef>
#include <cstdint>

#include <common/logging.hh>
#include <postgresql/server/catalog/pg_type_d.h>

using Oid = uint32_t;
using regproc = Oid;
using Datum = uintptr_t;
using Size = size_t;
using CommandId = uint32_t;
using TransactionId = uint32_t;
using Index = uint32_t;

#define InvalidOid (Oid(0))
#define FLEXIBLE_ARRAY_MEMBER
#define INT64CONST(x)  (x##L)

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

#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))
#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)

#define MaxAllocSize ((Size) 0x3fffffff)
#define is_digit(c) ((unsigned)(c) - '0' <= 9)

#define AssertMacro(condition)	((void)true)

constexpr int MAXPGPATH = 1024;
constexpr int PG_BINARY = 0;
#define INITIALIZE(x)	((x) = 0)

static const uint8_t pg_leftmost_one_pos[256] = {
	0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
	4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
	5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
	5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
};

static const char DIGIT_TABLE[200] = {
	'0', '0', '0', '1', '0', '2', '0', '3', '0', '4', '0', '5', '0', '6', '0', '7', '0', '8', '0', '9',
	'1', '0', '1', '1', '1', '2', '1', '3', '1', '4', '1', '5', '1', '6', '1', '7', '1', '8', '1', '9',
	'2', '0', '2', '1', '2', '2', '2', '3', '2', '4', '2', '5', '2', '6', '2', '7', '2', '8', '2', '9',
	'3', '0', '3', '1', '3', '2', '3', '3', '3', '4', '3', '5', '3', '6', '3', '7', '3', '8', '3', '9',
	'4', '0', '4', '1', '4', '2', '4', '3', '4', '4', '4', '5', '4', '6', '4', '7', '4', '8', '4', '9',
	'5', '0', '5', '1', '5', '2', '5', '3', '5', '4', '5', '5', '5', '6', '5', '7', '5', '8', '5', '9',
	'6', '0', '6', '1', '6', '2', '6', '3', '6', '4', '6', '5', '6', '6', '6', '7', '6', '8', '6', '9',
	'7', '0', '7', '1', '7', '2', '7', '3', '7', '4', '7', '5', '7', '6', '7', '7', '7', '8', '7', '9',
	'8', '0', '8', '1', '8', '2', '8', '3', '8', '4', '8', '5', '8', '6', '8', '7', '8', '8', '8', '9',
	'9', '0', '9', '1', '9', '2', '9', '3', '9', '4', '9', '5', '9', '6', '9', '7', '9', '8', '9', '9'
};

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
 #define ExtMax(x, y)		((x) > (y) ? (x) : (y))

 /*
  * Min
  *		Return the minimum of two numbers.
  */
 #define ExtMin(x, y)		((x) < (y) ? (x) : (y))
