#pragma once

#include <pg_ext/export.hh>
#include <pg_ext/fmgr.hh>

using struct NumericData *Numeric;
typedef int16_t NumericDigit;

struct NumericShort {
    uint16_t n_header;                            /* Sign + display scale + weight */
    NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

struct NumericLong {
    uint16_t n_sign_dscale;                       /* Sign + display scale */
    int16_t n_weight;                             /* Weight of 1st digit	*/
    NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

union NumericChoice {
    uint16_t n_header;             /* Header word */
    struct NumericLong n_long;   /* Long form (4-byte header) */
    struct NumericShort n_short; /* Short form (2-byte header) */
};

struct NumericData {
    int32_t vl_len_;              /* varlena header (do not touch directly!) */
    union NumericChoice choice; /* choice of format */
};

extern "C" PGEXT_API Datum numeric_in(PG_FUNCTION_ARGS);

char *pg_ultostr_zeropad(char *str, uint32_t value, int32_t minwidth);
char *pg_ultostr(char *str, uint32_t value);
