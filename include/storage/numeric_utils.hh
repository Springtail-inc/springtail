#pragma once

#include <stdint.h>
#include <string>

#include <absl/log/check.h>

// This code was ported over from
// https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/numeric.c
// and
// https://github.com/postgres/postgres/blob/master/src/include/utils/numeric.h

namespace springtail::numeric {

    using NumericDigit = int16_t;

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

    constexpr uint16_t  NBASE                   = 10000;
    constexpr uint16_t  HALF_NBASE              = 5000;
    constexpr uint16_t  DEC_DIGITS              = 4;    /* decimal digits per NBASE digit */
    constexpr uint16_t  NUMERIC_DSCALE_MASK     = 0x3FFF;

    constexpr size_t NUMERIC_HDRSZ              = (sizeof(int32_t) + sizeof(uint16_t) + sizeof(int16_t));
    constexpr size_t NUMERIC_HDRSZ_SHORT        = (sizeof(int32_t) + sizeof(uint16_t));

    /*
     * Interpretation of high bits.
     */
    constexpr uint16_t NUMERIC_SIGN_MASK   = 0xC000;
    constexpr uint16_t NUMERIC_POS         = 0x0000;
    constexpr uint16_t NUMERIC_NEG         = 0x4000;
    constexpr uint16_t NUMERIC_SHORT       = 0x8000;
    constexpr uint16_t NUMERIC_SPECIAL     = 0xC000;

    /*
     * Definitions for special values (NaN, positive infinity, negative infinity).
     *
     * The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
     * infinity, 11 for negative infinity.  (This makes the sign bit match where
     * it is in a short-format value, though we make no use of that at present.)
     * We could mask off the remaining bits before testing the active bits, but
     * currently those bits must be zeroes, so masking would just add cycles.
     */
    constexpr uint16_t NUMERIC_EXT_SIGN_MASK    = 0xF000;
    constexpr uint16_t NUMERIC_NAN              = 0xC000;
    constexpr uint16_t NUMERIC_PINF             = 0xD000;
    constexpr uint16_t NUMERIC_NINF             = 0xF000;
    constexpr uint16_t NUMERIC_INF_SIGN_MASK    = 0x2000;

    /*
     * Short format definitions.
     */
    constexpr uint16_t NUMERIC_SHORT_SIGN_MASK          = 0x2000;
    constexpr uint16_t NUMERIC_SHORT_DSCALE_MASK        = 0x1F80;
    constexpr uint16_t NUMERIC_SHORT_DSCALE_SHIFT       = 7;
    constexpr uint16_t NUMERIC_SHORT_DSCALE_MAX         = (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT);
    constexpr uint16_t NUMERIC_SHORT_WEIGHT_SIGN_MASK   = 0x0040;
    constexpr uint16_t NUMERIC_SHORT_WEIGHT_MASK        = 0x003F;
    constexpr uint16_t NUMERIC_SHORT_WEIGHT_MAX         = NUMERIC_SHORT_WEIGHT_MASK;
    constexpr uint16_t NUMERIC_SHORT_WEIGHT_MIN         = (-(NUMERIC_SHORT_WEIGHT_MASK+1));

    /**
     * @brief Input structure for parcing data from the buffer
     *
     */
    struct StringInfoData
    {
        char   *_data;      ///< character buffer
        int     _len;       ///< data length
        int     _maxlen;    ///< maximum data length
        int     _cursor;    ///< current cursor position

        /**
         * @brief Copy bytes into buffer
         *
         * @param buf - buffer pointer
         * @param datalen - number of bytes to copy
         */
        void copybytes(void *buf, int datalen);

        /**
         * @brief Extract in value from the input
         *
         * @param b - number of bytes
         * @return unsigned int - integer value/LOG
         */
        unsigned int getint(int b);
    };

    typedef StringInfoData *StringInfo;

    /**
     * @brief Class for handling typemod operations. This has been ported for completeness.
     *  At the moment, we pass 0 as typmode value which makes typmode invalid and none of
     *  the precision() and scale() functions end up being used.
     *
     */
    class TypeMod {
    public:
        TypeMod(int32_t typmod) : _typmod(typmod) {}

        /**
         * @brief Verify if typmod value is valid.
         *      Because of the offset, valid numeric typmods are at least VARHDRSZ.
         *
         * @return true
         * @return false
         */
        bool is_valid() const
        {
            return (_typmod >= static_cast<int32_t>(sizeof(int32_t)));
        }

        /**
         * @brief Extract the precision from a numeric typmod.
         *
         * @return int
         */
        int precision() const
        {
            return ((_typmod - static_cast<int32_t>(sizeof(int32_t))) >> 16) & 0xffff;
        }

        /**
         * @brief Extract the scale from a numeric typmod
         *
         * @return int
         */
        int scale() const
        {
            return (((_typmod - static_cast<int32_t>(sizeof(int32_t))) & 0x7ff) ^ 1024) - 1024;
        }
    private:
        int32_t _typmod;    ///< typmod value
    };

    struct NumericVarLiteral
    {
        int             _ndigits;               ///< # of digits in digits[] - can be 0!
        int             _weight;                ///< weight of first digit
        int             _sign;                  ///< NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF
        int             _dscale;                ///< display scale
        NumericDigit *  _digits;                ///< base-NBASE digits

        constexpr NumericVarLiteral(
            int ndigits,
            int weight,
            int sign,
            int dscale,
            NumericDigit *digits
        ) :
            _ndigits(ndigits),
            _weight(weight),
            _sign(sign),
            _dscale(dscale),
            _digits(digits) {}
    };

    /**
     * @brief This structure represents protocol level storage format of the numeric value.
     *
     */
    struct NumericVar : public NumericVarLiteral
    {
        std::unique_ptr<NumericDigit[]>  _buf{nullptr};  ///< start of alloc'd space for digits[]

        explicit NumericVar(int ndigits = 0) :
            NumericVarLiteral(ndigits, 0, 0, 0, nullptr)
        {
            CHECK(ndigits >= 0);
            if (ndigits > 0) {
                alloc(ndigits);
            }
        }

        NumericVar(const NumericVarLiteral &num_lit) :
            NumericVarLiteral(num_lit) {}

        /**
         * @brief Powers for rounding numeric variables
         *
         */
        static constexpr int round_powers[4] = {0, 1000, 100, 10};

        /**
         * @brief Allocate buffer for the given number of digits
         *
         * @param number_of_digits - size of the buffer in terms of digits
         */
        void alloc(int number_of_digits);

        /**
         * @brief Truncate (towards zero) the value of a variable at rscale decimal digits
         *          after the decimal point.
         *  NOTE: we allow rscale < 0 here, implying truncation before the decimal point.
         *
         * @param rscale - rscale value
         */
        void trunc(int rscale);

        /**
         * @brief Round the value of a variable to no more than rscale decimal digits
         *      after the decimal point.
         *  NOTE: we allow rscale < 0 here, implying rounding before the decimal point.
         *
         * @param rscale - rscale value
         */
        void round(int rscale);

        /**
         * @brief Do bounds checking and rounding according to the specified typmod.
         * NOTE: that this is only applied to normal finite values.
         * NOTE: we do not pass a valid typmod value anywhere in our code, so this function
         *      does for the most part nothing.
         *
         * @param typmod - typmod value to apply
         */
        void apply_typmod(const TypeMod &typmod);

        /**
         * @brief Strip any leading and trailing zeroes from a numeric variable
         *
         */
        void strip();

        /**
         * @brief Convert a var to text representation.
         *	The var is displayed to the number of digits indicated by its dscale.
         *
         * @return std::string - string representing numeric variable
         */
        std::string to_string() const;

        /**
         * @brief Parse a string and put the number into a variable
         * This function does not handle leading or trailing spaces.
         *
         * @param str - input string
         */
        void from_string(std::string_view str);

        /**
         * @brief Write this numeric value to the debug log with the given message
         *
         * @param str - message to print together with the value
         */
        void dump(const std::string &str) const;

        /**
         * @brief Convert variable to debug string that prints all elements of the variable
         *
         * @return std::string - returns debug string
         */
        std::string to_debug_string() const;

    };

    /**
     * @brief This structure represents a numeric value in disc storage format.
     *
     */
    struct NumericData
    {
        int32_t             _vl_len_;   ///< varlena header (do not touch directly!)
        union NumericChoice _choice;    ///< choice of format

        /**
         * @brief Allocate numeric data value
         *
         * @param size - size of numeric data value
         * @return std::shared_ptr<NumericData> - shared pointer to allocated data
         */
        static std::shared_ptr<NumericData> alloc(size_t size)
        {
            return std::shared_ptr<NumericData>(
                reinterpret_cast<NumericData *>(
                    new unsigned char[size]()),
                [](NumericData *ptr) {
                    delete[] reinterpret_cast<unsigned char *>(ptr);
                }
            );
        }

        /**
         * @brief Set the varsize of numeric object
         *
         * @param len
         */
        void set_varsize(int len)
        {
            _vl_len_ = len << 2;
        }

        /**
         * @brief Get varsize of numeric object
         *
         * @return int32_t - size of numeric object
         */
        int32_t varsize() const
        {
            return (_vl_len_ >> 2) & 0x3FFFFFFF;
        }

        /**
         * @brief Check if numeric data can have short representation or require a long one
         *
         * @param weight - weight of the numeric object
         * @param dscale - dscale of the numeric object
         * @return true     - use short representation
         * @return false    - use long representation
         */
        static bool can_be_short(int16_t weight, uint16_t dscale)
        {
            return ((dscale) <= NUMERIC_SHORT_DSCALE_MAX && (weight) <= NUMERIC_SHORT_WEIGHT_MAX && (weight) >= NUMERIC_SHORT_WEIGHT_MIN);
        }

        /**
         * @brief Check if the numeric object uses short format
         *
         * @return true     - uses short format
         * @return false    - uses long format
         */
        bool header_is_short() const
        {
            return (_choice.n_header & NUMERIC_SHORT) != 0;
        }

        /**
         * @brief Get size of the numeric object header
         *
         * @return size_t - header size
         */
        size_t  header_size() const
        {
            return sizeof(int32_t) + sizeof(uint16_t) + (header_is_short() ? 0 : sizeof(int16_t));
        }

        /**
         * @brief Get digits array of the numeric data
         *
         * @return NumericDigit* - digits pointer
         */
        NumericDigit *digits() const
        {
            return const_cast<NumericDigit *>(header_is_short() ? _choice.n_short.n_data : _choice.n_long.n_data);
        }

        /**
         * @brief Set digit values of the numeric data
         *
         * @param in_digits - input digits array
         * @param n         - number of digits to copy
         */
        void digits(NumericDigit *in_digits, int n)
        {
            NumericDigit *out_digits = digits();
            CHECK(ndigits() == n);
            if (n > 0) {
                memcpy(out_digits, in_digits, n * sizeof(NumericDigit));
            }
        }

        /**
         * @brief Number of digits that this numeric data holds or can hold
         *
         * @return int - number of digits
         */
        int ndigits() const
        {
            return (varsize() - header_size()) / sizeof(NumericDigit);
        }

        /**
         * @brief Get extended flagbits from the header
         *
         * @return uint16_t - extended flagbits
         */
        uint16_t ext_flagbits() const
        {
            return _choice.n_header & NUMERIC_EXT_SIGN_MASK;
        }

        /**
         * @brief Get sign flagbits from the header
         *
         * @return uint16_t - sign flagbits
         */
        uint16_t flagbits() const
        {
            return _choice.n_header & NUMERIC_SIGN_MASK;
        }

        /**
         * @brief Check if the numeric value has short representation
         *
         * @return true     - short
         * @return false    - long
         */
        bool is_short() const
        {
            return flagbits() == NUMERIC_SHORT;
        }

        /**
         * @brief Check if numeric value is special
         *
         * @return true     - special number
         * @return false    - normal number
         */
        bool is_special() const
        {
            return flagbits() == NUMERIC_SPECIAL;
        }

        /**
         * @brief Check if this numeric value represents NaN
         *
         * @return true     - NaN
         * @return false    - not NaN
         */
        bool is_nan() const
        {
            return _choice.n_header == NUMERIC_NAN;
        }

        /**
         * @brief Check if this numeric value represents positive infinity
         *
         * @return true     - positive infinity
         * @return false    - not positive infinity
         */
        bool is_pinf() const
        {
            return _choice.n_header == NUMERIC_PINF;
        }

        /**
         * @brief Check if this numeric value represents negative infinity
         *
         * @return true     - negative infinity
         * @return false    - not negative infinity
         */
         bool is_ninf() const
        {
            return _choice.n_header == NUMERIC_NINF;
        }

        /**
         * @brief Check if this numeric value represents infinity, either positive or negative
         *
         * @return true     - infinity
         * @return false    - not infinity
         */
         bool is_inf() const
        {
            return (_choice.n_header & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF;
        }

        /**
         * @brief Get the sign of this numeric value
         *
         * @return uint16_t
         */
        uint16_t sign() const
        {
            return is_short() ?
                ((_choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ?
                    NUMERIC_NEG : NUMERIC_POS) :
                    (is_special() ? ext_flagbits() : flagbits());
        }

        /**
         * @brief Get dscale of this numeric value
         *
         * @return uint16_t - dscale
         */
        uint16_t dscale() const
        {
            return header_is_short() ?
                ((_choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT) :
                (_choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK);
        }

        /**
         * @brief Get weight of this numeric value
         *
         * @return int16_t - weight
         */
        int16_t weight() const
        {
            return header_is_short() ?
                ((_choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) |
                    (_choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) : _choice.n_long.n_weight;
        }

        /**
         * @brief Write this numeric value to the debug log with the given message
         *
         * @param str - message to print together with the value
         */
         void dump(const std::string &str) const;

         /**
          * @brief Convert this numeric value to the debug string
          *
          * @return std::string - debug string
          */
        std::string to_debug_string() const;

        /**
         * @brief Convert this numeric value to a string
         *
         * @return std::string - numeric value in string format
         */
        std::string to_string() const;

        /**
         * @brief Do bounds checking according to the specified typmod, for an Inf or NaN.
         * For convenience of most callers, the value is presented in packed form.
         *
         * @param typmod
         */
        void apply_typmod_special(const TypeMod &typmod);

        /**
         * @brief Initialize a variable from packed db format. The digits array is not
         *	copied, which saves some cycles when the resulting var is not modified.
         *	Also, there's no need to call free.
         *
         * @return const NumericVar - converted numeric variable
         */
        const NumericVar to_var() const;

        /**
         * @brief Create the packed db numeric format in alloc()'d memory from
         * a variable.  This will handle NaN and Infinity cases.
         *
         * @param var
         * @return NumericData*
         */
        static std::shared_ptr<NumericData> make_numeric(const NumericVar &var);

        /**
         * @brief Converts external binary format to numeric
         *
         * @param buf       - input buffer
         * @param typmod    - typmod of the value
         * @return NumericData* - allocated numeric value
         */
        static std::shared_ptr<NumericData> recv(StringInfo buf, const TypeMod &typmod);

        /**
         * @brief Main routine of absolute comparison. This function can be used by both
         *          NumericVar and NumericData.
         *
         * @param var1digits    - var1 digits
         * @param var1ndigits   - var1 number of digits
         * @param var1weight    - var1 weight
         * @param var2digits    - var2 digits
         * @param var2ndigits   - var2 number of digits
         * @param var2weight    - var2 weight
         * @return int  - -1 - less, 0 - equal, 1 - greater
         */
        static int cmp_abs_common(  const NumericDigit *var1digits, int var1ndigits, int var1weight,
                                    const NumericDigit *var2digits, int var2ndigits, int var2weight);

        /**
         * @brief Main routine of cmp(). This function can be used by both NumericVar and Numeric.
         *
         * @param var1digits    - var1 digits
         * @param var1ndigits   - var1 number of digits
         * @param var1weight    - var1 weight
         * @param var1sign      - var1 sign
         * @param var2digits    - var2 digits
         * @param var2ndigits   - var2 number of digits
         * @param var2weight    - var2 weight
         * @param var2sign      - var2 sign
         * @return int  - -1 - less, 0 - equal, 1 - greater
         */
        static int cmp_common(  const NumericDigit *var1digits, int var1ndigits, int var1weight, int var1sign,
                                    const NumericDigit *var2digits, int var2ndigits, int var2weight, int var2sign);

        /**
         * @brief Compare two numeric values.
         *
         * @param num1  - numeric value 1
         * @param num2  - numeric value 2
         * @return int  - -1 - less, 0 - equal, 1 - greater
         */
        static int cmp(const std::shared_ptr<NumericData> num1, const std::shared_ptr<NumericData> num2);

        /**
         * @brief Convert numeric string respresentation into numeric on disc representation
         *
         * @param str               - input string
         * @param typmod            - typmod value
         * @return NumericData*     - newly created numeric data value
         */
        static std::shared_ptr<NumericData> from_string(std::string_view str, const TypeMod &typmod);
    };

    using Numeric = struct NumericData *;

    /**
     * @brief Read numeric data from input buffer with the given typmod
     *
     * @param buf       - input buffer
     * @param length    - input length
     * @param typmod    - typmod value
     * @return Numeric  - newly created numeric value
     */
    static inline std::shared_ptr<NumericData> numeric_receive(const char *buf, int32_t length, int32_t typmod)
    {
        StringInfoData info;
        info._data = const_cast<char *>(buf);
        info._len = length;
        info._maxlen = length;
        info._cursor = 0;
        return NumericData::recv(&info, typmod);
    }

} // namespace springtail::numeric