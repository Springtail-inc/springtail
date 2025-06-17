#pragma once

#include <arpa/inet.h>
#include <stdint.h>
#include <string.h>

#include <span>

#include <fmt/ranges.h>

#include <common/logging.hh>

namespace springtail::numeric {

    // TODO: fix comments
    typedef int16_t NumericDigit;

    struct NumericShort
    {
        uint16_t     n_header;      /* Sign + display scale + weight */
        NumericDigit n_data[];      /* Digits */
    };

    struct NumericLong
    {
        uint16_t        n_sign_dscale;  /* Sign + display scale */
        int16_t         n_weight;       /* Weight of 1st digit*/
        NumericDigit    n_data[];       /* Digits */
    };

    union NumericChoice
    {
        uint16_t            n_header;   /* Header word */
        struct NumericLong  n_long;     /* Long form (4-byte header) */
        struct NumericShort n_short;    /* Short form (2-byte header) */
    };

    #define NBASE                   10000
    #define HALF_NBASE              5000
    #define DEC_DIGITS              4       /* decimal digits per NBASE digit */
    #define NUMERIC_DSCALE_MASK     0x3FFF

    #define NUMERIC_HDRSZ       (sizeof(int32_t) + sizeof(uint16_t) + sizeof(int16_t))
    #define NUMERIC_HDRSZ_SHORT (sizeof(int32_t) + sizeof(uint16_t))

    /*
     * Interpretation of high bits.
     */

     #define NUMERIC_SIGN_MASK      0xC000
     #define NUMERIC_POS            0x0000
     #define NUMERIC_NEG            0x4000
     #define NUMERIC_SHORT          0x8000
     #define NUMERIC_SPECIAL        0xC000

    /*
     * Definitions for special values (NaN, positive infinity, negative infinity).
     *
     * The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
     * infinity, 11 for negative infinity.  (This makes the sign bit match where
     * it is in a short-format value, though we make no use of that at present.)
     * We could mask off the remaining bits before testing the active bits, but
     * currently those bits must be zeroes, so masking would just add cycles.
     */
    #define NUMERIC_EXT_SIGN_MASK   0xF000/* high bits plus NaN/Inf flag bits */
    #define NUMERIC_NAN             0xC000
    #define NUMERIC_PINF            0xD000
    #define NUMERIC_NINF            0xF000
    #define NUMERIC_INF_SIGN_MASK   0x2000

    /*
     * Short format definitions.
     */
    #define NUMERIC_SHORT_SIGN_MASK         0x2000
    #define NUMERIC_SHORT_DSCALE_MASK       0x1F80
    #define NUMERIC_SHORT_DSCALE_SHIFT      7
    #define NUMERIC_SHORT_DSCALE_MAX        (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
    #define NUMERIC_SHORT_WEIGHT_SIGN_MASK  0x0040
    #define NUMERIC_SHORT_WEIGHT_MASK       0x003F
    #define NUMERIC_SHORT_WEIGHT_MAX        NUMERIC_SHORT_WEIGHT_MASK
    #define NUMERIC_SHORT_WEIGHT_MIN        (-(NUMERIC_SHORT_WEIGHT_MASK+1))

    struct StringInfoData
    {
        char   *data;
        int     len;
        int     maxlen;
        int     cursor;

        void copybytes(void *buf, int datalen);
        unsigned int getint(int b);
    };

    typedef StringInfoData *StringInfo;

    class TypeMod {
    public:
        TypeMod(int32_t typmod) : _typmod(typmod) {}

        /*
         * Because of the offset, valid numeric typmods are at least VARHDRSZ
         */
        bool is_valid() const
        {
            return (_typmod >= static_cast<int32_t>(sizeof(int32_t)));
        }

        /*
         * precision() - Extract the precision from a numeric typmod.
         */
        int precision() const
        {
            return ((_typmod - static_cast<int32_t>(sizeof(int32_t))) >> 16) & 0xffff;
        }

        /*
         * scale() - Extract the scale from a numeric typmod
         */
        int scale() const
        {
            return (((_typmod - static_cast<int32_t>(sizeof(int32_t))) & 0x7ff) ^ 1024) - 1024;
        }
    private:
        int32_t _typmod;
    };

    // Protocol storage format for numeric values
    typedef struct NumericVar
    {
        int         ndigits;        /* # of digits in digits[] - can be 0! */
        int         weight;         /* weight of first digit */
        int         sign;           /* NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF */
        int         dscale;         /* display scale */
        NumericDigit *buf;          /* start of palloc'd space for digits[] */
        NumericDigit *digits;       /* base-NBASE digits */

        static constexpr int round_powers[4] = {0, 1000, 100, 10};

        void alloc(int number_of_digits);
        void free();
        void trunc(int rscale);
        /*
         * round
         *
         * Round the value of a variable to no more than rscale decimal digits
         * after the decimal point.  NOTE: we allow rscale < 0 here, implying
         * rounding before the decimal point.
         */
        void round(int rscale);
        /*
         * apply_typmod() -
         *
         * Do bounds checking and rounding according to the specified typmod.
         * Note that this is only applied to normal finite values.
         *
         * Returns true on success, false on failure (if escontext points to an
         * ErrorSaveContext; otherwise errors are thrown).
         */
        bool apply_typmod(const TypeMod &typmod);
    } NumericVar;

    // Disk storage format for numeric values
    struct NumericData
    {
        int32_t             vl_len_;/* varlena header (do not touch directly!) */
        union NumericChoice choice; /* choice of format */

        void set_varsize(int len)
        {
            vl_len_ = len << 2;
        }
        int32_t varsize() const
        {
            return (vl_len_ >> 2) & 0x3FFFFFFF;
        }
        static bool can_be_short(int16_t weight, uint16_t dscale)
        {
            return ((dscale) <= NUMERIC_SHORT_DSCALE_MAX && (weight) <= NUMERIC_SHORT_WEIGHT_MAX && (weight) >= NUMERIC_SHORT_WEIGHT_MIN);
        }

        bool header_is_short() const
        {
            return (choice.n_header & 0x8000) != 0;
        }
        size_t  header_size() const
        {
            return sizeof(int32_t) + sizeof(uint16_t) +
                   (header_is_short() ? 0 : sizeof(int16_t));
        }
        NumericDigit *digits() const
        {
            return const_cast<NumericDigit *>(header_is_short() ? choice.n_short.n_data : choice.n_long.n_data);
        }
        void digits(NumericDigit *in_digits, int n)
        {
            NumericDigit *out_digits = digits();
            CHECK(ndigits() == n);
            if (n > 0) {
                memcpy(out_digits, in_digits, n * sizeof(NumericDigit));
            }
        }
        int ndigits() const
        {
            return (varsize() - header_size()) / sizeof(NumericDigit);
        }
        uint16_t ext_flagbits() const
        {
            return choice.n_header & NUMERIC_EXT_SIGN_MASK;
        }
        uint16_t flagbits() const
        {
            return choice.n_header & NUMERIC_SIGN_MASK;
        }
        bool is_short() const
        {
            return flagbits() == NUMERIC_SHORT;
        }
        bool is_special() const
        {
            return flagbits() == NUMERIC_SPECIAL;
        }
        bool is_nan() const
        {
            return choice.n_header == NUMERIC_NAN;
        }
        bool is_pinf() const
        {
            return choice.n_header == NUMERIC_PINF;
        }
        bool is_ninf() const
        {
            return choice.n_header == NUMERIC_NINF;
        }
        bool is_inf() const
        {
            return (choice.n_header & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF;
        }
        uint16_t sign() const
        {
            return is_short() ?
                ((choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ?
                NUMERIC_NEG : NUMERIC_POS) :
                (is_special() ? ext_flagbits() : flagbits());
        }
        uint16_t dscale() const
        {
            return header_is_short() ?
                ((choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT) :
                (choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK);
        }
        int16_t weight() const
        {
            return header_is_short() ?
                ((choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) |
                    (choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) : choice.n_long.n_weight;
        }
        /*
         * dump() - Dump a value in the db storage format for debugging
         */
        void dump(const char *str);

        std::string to_string() const;

        /*
         * apply_typmod_special() -
         *
         * Do bounds checking according to the specified typmod, for an Inf or NaN.
         * For convenience of most callers, the value is presented in packed form.
         *
         * Returns true on success, false on failure (if escontext points to an
         * ErrorSaveContext; otherwise errors are thrown).
         */
        bool apply_typmod_special(const TypeMod &typmod);

        /*
         * make_numeric() -
         *
         * Create the packed db numeric format in palloc()'d memory from
         * a variable.  This will handle NaN and Infinity cases.
         */
        static NumericData *make_numeric(const NumericVar *var);

        static void free_numeric(NumericData *make_numeric);

        /*
         * recv - converts external binary format to numeric
         *
         * External format is a sequence of int16's:
         * ndigits, weight, sign, dscale, NumericDigits.
         */
        static NumericData *recv(StringInfo buf, uint32_t typmod);

        /* ----------
         * cmp_abs_common() -
         *
         * Main routine of cmp_abs(). This function can be used by both
         * NumericVar and Numeric.
         * ----------
         */
        static int cmp_abs_common(  const NumericDigit *var1digits, int var1ndigits, int var1weight,
                                    const NumericDigit *var2digits, int var2ndigits, int var2weight);

        /*
         * cmp_var_common() -
         *
         * Main routine of cmp_var(). This function can be used by both
         * NumericVar and Numeric.
         */
        static int cmp_var_common(  const NumericDigit *var1digits, int var1ndigits, int var1weight, int var1sign,
                                    const NumericDigit *var2digits, int var2ndigits, int var2weight, int var2sign);

        static int cmp(const NumericData *num1, const NumericData *num2);
    };

    typedef struct NumericData *Numeric;

    static inline Numeric numeric_receive(const char *buf, int32_t length, uint32_t typmod)
    {
        StringInfoData info;
        info.data = const_cast<char *>(buf);
        info.len = length;
        info.maxlen = length;
        info.cursor = 0;
        return NumericData::recv(&info, typmod);
    }

    static inline int numeric_compare(const std::span<const char> &lhs, const std::span<const char> &rhs)
    {
        return NumericData::cmp(reinterpret_cast<const NumericData *>(lhs.data()), reinterpret_cast<const NumericData *>(rhs.data()));
    }

} // namespace springtail::numeric