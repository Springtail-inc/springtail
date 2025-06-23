#include <arpa/inet.h>

#include <fmt/ranges.h>
#include <string>

#include <common/logging.hh>
#include <storage/numeric_utils.hh>

namespace springtail::numeric {

    static constexpr NumericVar const_nan =
        {0, 0, NUMERIC_NAN, 0, NULL, NULL};

    static constexpr NumericVar const_pinf =
        {0, 0, NUMERIC_PINF, 0, NULL, NULL};

    static constexpr NumericVar const_ninf =
        {0, 0, NUMERIC_NINF, 0, NULL, NULL};

    static bool
    ichar_equals(char a, char b)
    {
        return std::tolower(static_cast<unsigned char>(a)) ==
                std::tolower(static_cast<unsigned char>(b));
    }

    static bool
    inequals(std::string_view lhs, std::string_view rhs, size_t len)
    {
        for (int i = 0; i < len; ++i)
        {
            if (!ichar_equals(lhs[i], rhs[i]))
            {
                return false;
            }
        }
        return true;
    }

    static std::string
    sing_to_string(int sign)
    {
        std::string sign_str;
        switch (sign) {
            case NUMERIC_POS:
            sign_str = "POS";
            break;
        case NUMERIC_NEG:
            sign_str = "NEG";
            break;
        case NUMERIC_NAN:
            sign_str = "NaN";
            break;
        case NUMERIC_PINF:
            sign_str = "Infinity";
            break;
        case NUMERIC_NINF:
            sign_str = "-Infinity";
            break;
        default:
            sign_str = fmt::format("SIGN=0x{:x}", sign);
            break;
        }
        return sign_str;
    }

    void
    StringInfoData::copybytes(void *buf, int datalen)
    {
        CHECK (!(datalen < 0 || datalen > (len - cursor)))
            << "Numeric: insufficient data left in message";
        memcpy(buf, &data[cursor], datalen);
        cursor += datalen;
    }

    unsigned int
    StringInfoData::getint(int b)
    {
        unsigned int result;
        unsigned char n8;
        uint16_t n16;
        uint32_t n32;

        switch (b)
        {
            case 1:
                copybytes(&n8, 1);
                result = n8;
                break;
            case 2:
                copybytes(&n16, 2);
                result = ntohs(n16);
                break;
            case 4:
                copybytes(&n32, 4);
                result = ntohl(n32);
                break;
            default:
                CHECK(false) << "Numeric: unsupported integer size " << b;
                break;
        }
        return result;
    }

    void
    NumericVar::alloc(int number_of_digits)
    {
        if (buf != nullptr) {
            ::free(buf);
        }
        buf = reinterpret_cast<NumericDigit *>(malloc((number_of_digits + 1) * sizeof(NumericDigit)));
        /* spare digit for rounding */
        buf[0] = 0;
        digits = buf + 1;
        ndigits = number_of_digits;
    }

    void
    NumericVar::free()
    {
        if (buf != NULL) {
            ::free(buf);
        }
        buf = nullptr;
        digits = nullptr;
        sign = NUMERIC_NAN;
    }

    void
    NumericVar::NumericVar::trunc(int rscale)
    {
        int di;
        int number_of_digits;

        dscale = rscale;

        /* decimal digits wanted */
        di = (weight + 1) * DEC_DIGITS + rscale;

        /*
        * If di <= 0, the value loses all digits.
        */
        if (di <= 0)
        {
            ndigits = 0;
            weight = 0;
            sign = NUMERIC_POS;
        }
        else
        {
            /* NBASE digits wanted */
            number_of_digits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

            if (number_of_digits <= ndigits)
            {
                ndigits = number_of_digits;

                /* 0, or number of decimal digits to keep in last NBASE digit */
                di %= DEC_DIGITS;

                if (di > 0)
                {
                    /* Must truncate within last NBASE digit */
                    int extra, pow10;

                    pow10 = round_powers[di];
                    extra = digits[--number_of_digits] % pow10;
                    digits[number_of_digits] -= extra;
                }
            }
        }
    }

    void
    NumericVar::round(int rscale)
    {
        int di;
        int number_of_digits;
        int carry;

        dscale = rscale;

        /* decimal digits wanted */
        di = (weight + 1) * DEC_DIGITS + rscale;

        /*
         * If di = 0, the value loses all digits, but could round up to 1 if its
         * first extra digit is >= 5.  If di < 0 the result must be 0.
         */
        if (di < 0)
        {
            ndigits = 0;
            weight = 0;
            sign = NUMERIC_POS;
        }
        else
        {
            /* NBASE digits wanted */
            number_of_digits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

            /* 0, or number of decimal digits to keep in last NBASE digit */
            di %= DEC_DIGITS;

            if (number_of_digits < ndigits ||
                (number_of_digits == ndigits && di > 0))
            {
                ndigits = number_of_digits;

                if (di == 0)
                    carry = (digits[number_of_digits] >= HALF_NBASE) ? 1 : 0;
                else
                {
                    /* Must round within last NBASE digit */
                    int extra, pow10;

                    pow10 = round_powers[di];
                    extra = digits[--number_of_digits] % pow10;
                    digits[number_of_digits] -= extra;
                    carry = 0;
                    if (extra >= pow10 / 2)
                    {
                        pow10 += digits[number_of_digits];
                        if (pow10 >= NBASE)
                        {
                            pow10 -= NBASE;
                            carry = 1;
                        }
                        digits[number_of_digits] = pow10;
                    }
                }

                /* Propagate carry if needed */
                while (carry)
                {
                    carry += digits[--number_of_digits];
                    if (carry >= NBASE)
                    {
                        digits[number_of_digits] = carry - NBASE;
                        carry = 1;
                    }
                    else
                    {
                        digits[number_of_digits] = carry;
                        carry = 0;
                    }
                }

                if (number_of_digits < 0)
                {
                    /* better not have added > 1 digit */
                    CHECK(number_of_digits == -1);
                    CHECK(digits > buf);
                    digits--;
                    ndigits++;
                    weight++;
                }
            }
        }
    }

    void
    NumericVar::apply_typmod(const TypeMod &typmod)
    {
        /* Do nothing if we have an invalid typmod */
        if (!typmod.is_valid())
            return;

        int precision = typmod.precision();
        int scale = typmod.scale();
        int maxdigits = precision - scale;

        /* Round to target scale (and set var->dscale) */
        round(scale);

        /* but don't allow var->dscale to be negative */
        if (dscale < 0)
            dscale = 0;

        /*
         * Check for overflow - note we can't do this before rounding, because
         * rounding could raise the weight.  Also note that the var's weight could
         * be inflated by leading zeroes, which will be stripped before storage
         * but perhaps might not have been yet. In any case, we must recognize a
         * true zero, whose weight doesn't mean anything.
         */
        int ddigits = (weight + 1) * DEC_DIGITS;
        if (ddigits > maxdigits)
        {
            /* Determine true weight; and check for all-zero result */
            for (int i = 0; i < ndigits; i++)
            {
                NumericDigit dig = digits[i];

                if (dig)
                {
                    /* Adjust for any high-order decimal zero digits */
                    if (dig < 10)
                        ddigits -= 3;
                    else if (dig < 100)
                        ddigits -= 2;
                    else if (dig < 1000)
                        ddigits -= 1;

                    CHECK (ddigits <= maxdigits) <<
                        "Numeric: numeric field overflow; A field with precision " << precision <<
                        ", scale " << scale <<
                        " must round to an absolute value less than " << (maxdigits ? "10^" : "") <<
                        (maxdigits ? maxdigits : 1) << ".";
                    break;
                }
                ddigits -= DEC_DIGITS;
            }
        }
    }

    void
    NumericVar::strip()
    {
        /* Strip leading zeroes */
        while (ndigits > 0 && *digits == 0)
        {
            digits++;
            weight--;
            ndigits--;
        }

        /* Strip trailing zeroes */
        while (ndigits > 0 && digits[ndigits - 1] == 0)
        {
            ndigits--;
        }

        /* If it's zero, normalize the sign and weight */
        if (ndigits == 0)
        {
            sign = NUMERIC_POS;
            weight = 0;
        }
    }

    std::string
    NumericVar::to_string()
    {
        std::string result;
        int d;
        NumericDigit dig;
        NumericDigit d1;

        /*
         * Allocate space for the result.
         *
         * i is set to the # of decimal digits before decimal point. dscale is the
         * # of decimal digits we will print after decimal point. We may generate
         * as many as DEC_DIGITS-1 excess digits at the end, and in addition we
         * need room for sign, decimal point, null terminator.
         */
        int i = (weight + 1) * DEC_DIGITS;
        if (i <= 0)
            i = 1;

        int size = i + dscale + DEC_DIGITS - 1 + 2; // +2 for sign and null terminator
        result.reserve(size);

        /*
         * Output a dash for negative values
         */
        if (sign == NUMERIC_NEG)
            result += '-';

        /*
         * Output all digits before the decimal point
         */
        if (weight < 0)
        {
            d = weight + 1;
            result += '0';
        }
        else
        {
            for (d = 0; d <= weight; d++)
            {
                dig = (d < ndigits) ? digits[d] : 0;
                /* In the first digit, suppress extra leading decimal zeroes */
                {
                    bool putit = (d > 0);

                    d1 = dig / 1000;
                    dig -= d1 * 1000;
                    putit |= (d1 > 0);
                    if (putit)
                    {
                        result += (d1 + '0');
                    }
                    d1 = dig / 100;
                    dig -= d1 * 100;
                    putit |= (d1 > 0);
                    if (putit)
                    {
                        result += (d1 + '0');
                    }
                    d1 = dig / 10;
                    dig -= d1 * 10;
                    putit |= (d1 > 0);
                    if (putit)
                    {
                        result += (d1 + '0');
                    }
                    result += (dig + '0');
                }
            }
        }

        /*
         * If requested, output a decimal point and all the digits that follow it.
         * We initially put out a multiple of DEC_DIGITS digits, then truncate if
         * needed.
         */
        if (dscale > 0)
        {
            result += '.';
            for (i = 0; i < dscale; d++, i += DEC_DIGITS)
            {
                dig = (d >= 0 && d < ndigits) ? digits[d] : 0;
                d1 = dig / 1000;
                dig -= d1 * 1000;
                result += (d1 + '0');
                d1 = dig / 100;
                dig -= d1 * 100;
                result += (d1 + '0');
                d1 = dig / 10;
                dig -= d1 * 10;
                result += (d1 + '0');
                result = (dig + '0');
            }
        }

        /*
         * terminate the string and return it
         */
        result += '\0';
        return result;
    }

    void
    NumericVar::from_string(const std::string_view &str)
    {
        int new_sign = NUMERIC_POS;
        size_t len = str.length();
        const char *cp = str.data();
        const char *cp_end = str.data() + str.length();

        CHECK(cp != cp_end);
        /*
         * We first parse the string to extract decimal digits and determine the
         * correct decimal weight.  Then convert to NBASE representation.
         */
        switch (*cp)
        {
            case '+':
                new_sign = NUMERIC_POS;
                cp++;
                break;

            case '-':
                new_sign = NUMERIC_NEG;
                cp++;
                break;
        }

        CHECK(cp != cp_end);

        bool have_dp = false;
        if (*cp == '.')
        {
            have_dp = true;
            cp++;
            CHECK(cp != cp_end);
        }

        CHECK(isdigit((unsigned char) *cp)) << "Numeric: invalid numeric string: " << str;

        size_t alloc_size = len - (cp - str.data()) + DEC_DIGITS * 2;
        unsigned char *decdigits = (unsigned char *) malloc(alloc_size);

        /* leading padding for digit alignment later */
        memset(decdigits, 0, DEC_DIGITS);
        int i = DEC_DIGITS;

        int dweight = -1;
        while (cp != cp_end)
        {
            if (isdigit((unsigned char) *cp))
            {
                decdigits[i++] = *cp++ - '0';
                if (!have_dp)
                {
                    dweight++;
                }
                else
                {
                    dscale++;
                }
            }
            else if (*cp == '.')
            {
                CHECK(!have_dp) << "Numeric: invalid numeric string: " << str;
                have_dp = true;
                cp++;
                CHECK(cp != cp_end);
                /* decimal point must not be followed by underscore */
                CHECK (*cp != '_') << "Numeric: invalid numeric string: " << str;
            }
            else if (*cp == '_')
            {
                /* underscore must be followed by more digits */
                cp++;
                CHECK(cp != cp_end);
                CHECK(isdigit((unsigned char) *cp)) << "Numeric: invalid numeric string: " << str;
            }
            else {
                break;
            }
        }

        int ddigits = i - DEC_DIGITS;
        /* trailing padding for digit alignment later */
        memset(decdigits + i, 0, DEC_DIGITS - 1);

        int new_dscale = 0;
        /* Handle exponent, if any */
        if (cp != cp_end && (*cp == 'e' || *cp == 'E'))
        {
            int64_t		exponent = 0;
            bool		neg = false;

            /*
             * At this point, dweight and dscale can't be more than about
             * INT_MAX/2 due to the MaxAllocSize limit on string length, so
             * constraining the exponent similarly should be enough to prevent
             * integer overflow in this function.  If the value is too large to
             * fit in storage format, make_result() will complain about it later;
             * for consistency use the same ereport errcode/text as make_result().
             */

            /* exponent sign */
            cp++;
            CHECK(cp != cp_end);
            if (*cp == '+')
            {
                cp++;
            }
            else if (*cp == '-')
            {
                neg = true;
                cp++;
            }

            /* exponent digits */
            CHECK(isdigit((unsigned char) *cp)) << "Numeric: invalid numeric string: " << str;

            while (cp != cp_end)
            {
                if (isdigit((unsigned char) *cp))
                {
                    exponent = exponent * 10 + (*cp++ - '0');
                    CHECK(exponent <= INT32_MAX / 2) << "Numeric: invalid numeric string: " << str;
                }
                else if (*cp == '_')
                {
                    /* underscore must be followed by more digits */
                    cp++;
                    CHECK(cp != cp_end);
                    CHECK(isdigit((unsigned char) *cp)) << "Numeric: invalid numeric string: " << str;
                }
                else
                {
                    break;
                }
            }

            if (neg)
            {
                exponent = -exponent;
            }

            dweight += (int) exponent;
            new_dscale -= (int) exponent;
            if (new_dscale < 0)
            {
                new_dscale = 0;
            }
        }

        /*
         * Okay, convert pure-decimal representation to base NBASE.  First we need
         * to determine the converted weight and ndigits.  offset is the number of
         * decimal zeroes to insert before the first given digit to have a
         * correctly aligned first NBASE digit.
         */
        int new_weight;
        if (dweight >= 0)
            new_weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
        else
            new_weight = -((-dweight - 1) / DEC_DIGITS + 1);

        int offset = (new_weight + 1) * DEC_DIGITS - (dweight + 1);
        int new_ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

        alloc(new_ndigits);
        sign = new_sign;
        weight = new_weight;
        dscale = new_dscale;

        i = DEC_DIGITS - offset;
        NumericDigit *new_digits = digits;

        while (new_ndigits-- > 0)
        {
            *new_digits++ = ((decdigits[i] * 10 + decdigits[i + 1]) * 10 +
                            decdigits[i + 2]) * 10 + decdigits[i + 3];
            i += DEC_DIGITS;
        }

        ::free(decdigits);

        /* Strip any leading/trailing zeroes, and normalize weight if zero */
        strip();
    }

    void
    NumericVar::dump(const std::string &str)
    {
        LOG_DEBUG(LOG_STORAGE, "{}: {} ({})", str, this->to_debug_string(), this->to_string());
    }

    std::string
    NumericVar::to_debug_string() const
    {
        std::vector<int16_t> digits_array(digits, digits + ndigits);
        std::string sign_str = sing_to_string(sign);
        std::string out = fmt::format("VAR w={} d={} nd={} {} \"{:04}\"",
            weight, dscale, ndigits, sign_str, fmt::join(digits_array, " "));
        return out;
    }

    std::string
    NumericData::to_debug_string() const
    {
        const NumericDigit *numeric_digits = digits();
        int numeric_ndigits = ndigits();

        std::vector<int16_t> digits_array(numeric_digits, numeric_digits + numeric_ndigits);
        std::string sign_str = sing_to_string(sign());
        std::string out = fmt::format("NUMERIC w={} d={} nd={} {} \"{:04}\"",
            weight(), dscale(), numeric_ndigits, sign_str, fmt::join(digits_array, " "));
        return out;
    }

    void NumericData::dump(const std::string &str)
    {
        LOG_DEBUG(LOG_STORAGE, "{}: {} ({})", str, this->to_debug_string(), this->to_string());
    }

    std::string NumericData::to_string() const
    {
        /*
         * Handle NaN and infinities
         */
        if (is_special())
        {
            if (is_pinf())
            {
                return "Infinity";
            }
            else if (is_ninf())
            {
                return "-Infinity";
            }
            else if (is_nan())
            {
                return "NaN";
            }
        }

        /*
         * Get the number in the variable format.
         */
        NumericVar	x = to_var();
        return x.to_string();
    }

    const NumericVar
    NumericData::to_var() const
    {
        NumericVar result;
        result.ndigits = ndigits();
        result.weight = weight();
        result.sign = sign();
        result.dscale = dscale();
        result.digits = digits();
        /* digits array is not palloc'd */
        result.buf = nullptr;
        return result;
    }

    void
    NumericData::apply_typmod_special(const TypeMod &typmod)
    {
        /* caller error if not */
        CHECK(is_special()) << "NumericData does not contain a special value";

        /*
         * NaN is allowed regardless of the typmod; that's rather dubious perhaps,
         * but it's a longstanding behavior.  Inf is rejected if we have any
         * typmod restriction, since an infinity shouldn't be claimed to fit in
         * any finite number of digits.
         */
        /* Do nothing if we have a default typmod (-1) */
        CHECK(is_nan() || !typmod.is_valid()) <<
            "Numeric: numeric field overflow; A field with precision " << typmod.precision() <<
            ", scale" <<  typmod.scale() << "cannot hold an infinite value.";
    }

    NumericData *
    NumericData::make_numeric(const NumericVar *var)
    {
        NumericData     *result;
        NumericDigit    *digits = var->digits;
        int             weight = var->weight;
        int             sign = var->sign;

        if ((sign & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL)
        {
            /*
             * Verify valid special value.  This could be just an Assert, perhaps,
             * but it seems worthwhile to expend a few cycles to ensure that we
             * never write any nonzero reserved bits to disk.
             */
            CHECK(sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF) <<
                "Numeric: invalid numeric sign value " << std::hex << sign << std::dec;

            result = static_cast<NumericData *>(::malloc(NUMERIC_HDRSZ_SHORT));

            result->set_varsize(NUMERIC_HDRSZ_SHORT);
            result->choice.n_header = sign;
            /* the header word is all we need */
            return result;
        }

        int n = var->ndigits;

        /* truncate leading zeroes */
        while (n > 0 && *digits == 0)
        {
            digits++;
            weight--;
            n--;
        }
        /* truncate trailing zeroes */
        while (n > 0 && digits[n - 1] == 0)
            n--;

        /* If zero result, force to weight=0 and positive sign */
        if (n == 0)
        {
            weight = 0;
            sign = NUMERIC_POS;
        }

        /* Build the result */
        if (NumericData::can_be_short(weight, var->dscale))
        {
            int len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
            result = static_cast<NumericData *>(::malloc(len));
            result->set_varsize(len);
            result->choice.n_short.n_header =
                (sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT)
                | (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT)
                | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)
                | (weight & NUMERIC_SHORT_WEIGHT_MASK);
        }
        else
        {
            int len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
            result = static_cast<NumericData *>(::malloc(len));
            result->set_varsize(len);
            result->choice.n_long.n_sign_dscale = sign | (var->dscale & NUMERIC_DSCALE_MASK);
            result->choice.n_long.n_weight = weight;
        }

        result->digits(digits, n);

        /* Check for overflow of int16 fields */
        CHECK(result->weight() == weight && result->dscale() == var->dscale) <<
            "Numeric: value overflows numeric format";
        return result;
    }

    void
    NumericData::free_numeric(NumericData *make_numeric)
    {
        if (make_numeric != nullptr) {
            ::free(make_numeric);
        }
    }

    NumericData *
    NumericData::recv(StringInfo buf, const TypeMod &typmod)
    {
        NumericVar  value;
        NumericData *res;

        memset(&value, 0, sizeof(NumericVar));

        int len = (uint16_t) buf->getint(sizeof(uint16_t));

        value.alloc(len);

        value.weight = (int16_t) buf->getint(sizeof(int16_t));
        /* we allow any int16 for weight --- OK? */

        value.sign = (uint16_t) buf->getint(sizeof(uint16_t));
        CHECK(value.sign == NUMERIC_POS ||
            value.sign == NUMERIC_NEG ||
            value.sign == NUMERIC_NAN ||
            value.sign == NUMERIC_PINF ||
            value.sign == NUMERIC_NINF) << "Numeric: invalid sign in external \"numeric\" value";

        value.dscale = (uint16_t) buf->getint(sizeof(uint16_t));
        CHECK((value.dscale & NUMERIC_DSCALE_MASK) == value.dscale) <<
            "Numeric: invalid scale in external \"numeric\" value";

        for (int i = 0; i < len; i++)
        {
            NumericDigit d = buf->getint(sizeof(NumericDigit));

            CHECK (d >= 0 && d < NBASE) <<
                "Numeric: invalid digit in external \"numeric\" value";
            value.digits[i] = d;
        }

        /*
         * If the given dscale would hide any digits, truncate those digits away.
         * We could alternatively throw an error, but that would take a bunch of
         * extra code (about as much as trunc() involves), and it might cause
         * client compatibility issues.  Be careful not to apply trunc() to
         * special values, as it could do the wrong thing; we don't need it
         * anyway, since make_result will ignore all but the sign field.
         *
         * After doing that, be sure to check the typmod restriction.
         */
        if (value.sign == NUMERIC_POS || value.sign == NUMERIC_NEG)
        {
            value.trunc(value.dscale);
            value.apply_typmod(typmod);
            res = NumericData::make_numeric(&value);
        }
        else
        {
            /* apply_typmod_special wants us to make the Numeric first */
            res = NumericData::make_numeric(&value);
            res->apply_typmod_special(typmod);
        }

        value.free();
        return res;
    }

    int
    NumericData::cmp_abs_common(const NumericDigit *var1digits, int var1ndigits, int var1weight,
                                const NumericDigit *var2digits, int var2ndigits, int var2weight)
    {
        int i1 = 0;
        int i2 = 0;

        /* Check any digits before the first common digit */

        while (var1weight > var2weight && i1 < var1ndigits)
        {
            if (var1digits[i1++] != 0)
                return 1;
            var1weight--;
        }
        while (var2weight > var1weight && i2 < var2ndigits)
        {
            if (var2digits[i2++] != 0)
                return -1;
            var2weight--;
        }

        /* At this point, either w1 == w2 or we've run out of digits */

        if (var1weight == var2weight)
        {
            while (i1 < var1ndigits && i2 < var2ndigits)
            {
                int stat = var1digits[i1++] - var2digits[i2++];

                if (stat)
                {
                    if (stat > 0)
                        return 1;
                    return -1;
                }
            }
        }
        /*
         * At this point, we've run out of digits on one side or the other; so any
         * remaining nonzero digits imply that side is larger
         */
        while (i1 < var1ndigits)
        {
            if (var1digits[i1++] != 0)
                return 1;
        }
        while (i2 < var2ndigits)
        {
            if (var2digits[i2++] != 0)
                return -1;
        }

        return 0;
    }

    int NumericData::cmp_var_common(const NumericDigit *var1digits, int var1ndigits, int var1weight, int var1sign,
                                    const NumericDigit *var2digits, int var2ndigits, int var2weight, int var2sign)
    {
        if (var1ndigits == 0)
        {
            if (var2ndigits == 0)
                return 0;
            if (var2sign == NUMERIC_NEG)
                return 1;
            return -1;
        }
        if (var2ndigits == 0)
        {
            if (var1sign == NUMERIC_POS)
                return 1;
            return -1;
        }

        if (var1sign == NUMERIC_POS)
        {
            if (var2sign == NUMERIC_NEG)
                return 1;
            return cmp_abs_common(var1digits, var1ndigits, var1weight,
                                var2digits, var2ndigits, var2weight);
        }

        if (var2sign == NUMERIC_POS)
            return -1;

        return cmp_abs_common(var2digits, var2ndigits, var2weight,
                            var1digits, var1ndigits, var1weight);
    }

    NumericData *
    NumericData::numeric_from_string(const std::string_view &str, const TypeMod &typmod)
    {
        Numeric res;
        const char *cp = str.data();
        const char *cp_end = str.data() + str.length();

        /* Skip leading spaces */
        while (cp != cp_end)
        {
            if (!isspace((unsigned char) *cp))
                break;
            cp++;
        }

        /*
         * Process the number's sign. This duplicates logic in set_var_from_str(),
         * but it's worth doing here, since it simplifies the handling of
         * infinities and non-decimal integers.
         */
        int sign = NUMERIC_POS;

        if (*cp == '+')
        {
            cp++;
        }
        else if (*cp == '-')
        {
            sign = NUMERIC_NEG;
            cp++;
        }

        CHECK(cp != cp_end) << "Numeric: invalid numeric string: " << str;

        /*
         * Check for NaN and infinities.  We recognize the same strings allowed by
         * float8in().
         *
         * Since all other legal inputs have a digit or a decimal point after the
         * sign, we need only check for NaN/infinity if that's not the case.
         */
        if (!isdigit((unsigned char) *cp) && *cp != '.')
        {
            /*
             * The number must be NaN or infinity; anything else can only be a
             * syntax error. Note that NaN mustn't have a sign.
             */
            if (((cp_end - cp) >= 3) && inequals(cp, "NaN", 3))
            {
                res = make_numeric(&const_nan);
                cp += 3;
            }
            else if (((cp_end - cp) >= 8) && inequals(cp, "Infinity", 8) == 0)
            {
                res = make_numeric(sign == NUMERIC_POS ? &const_pinf : &const_ninf);
                cp += 8;
            }
            else if (((cp_end - cp) >= 3) && inequals(cp, "inf", 3) == 0)
            {
                res = make_numeric(sign == NUMERIC_POS ? &const_pinf : &const_ninf);
                cp += 3;
            }
            else
            {
                CHECK(false) << "Numeric: invalid numeric string: " << str;
            }

            /*
             * Check for trailing junk; there should be nothing left but spaces.
             *
             * We intentionally do this check before applying the typmod because
             * we would like to throw any trailing-junk syntax error before any
             * semantic error resulting from apply_typmod_special().
             */
            while (cp < cp_end)
            {
                CHECK(isspace((unsigned char) *cp));
                cp++;
            }

            res->apply_typmod_special(typmod);
        }
        else
        {
            /*
            * We have a normal numeric value, which may be a non-decimal integer
            * or a regular decimal number.
            */
            NumericVar  value;
            int         base;

            memset(&value, 0, sizeof(NumericVar));

            /*
             * Determine the number's base by looking for a non-decimal prefix
             * indicator ("0x", "0o", or "0b").
             */
            if (cp[0] == '0')
            {
                switch (cp[1])
                {
                    case 'x':
                    case 'X':
                        base = 16;
                        break;
                    case 'o':
                    case 'O':
                        base = 8;
                        break;
                    case 'b':
                    case 'B':
                        base = 2;
                        break;
                    default:
                        base = 10;
                }
            }
            else
            {
                base = 10;
            }

            // TODO: this case is not supported, otherwise way more functionality has
            //      to be ported over
            CHECK(base == 10) << "Numeric: non-decimal integer parsing is not supported yet";

            /* Parse the rest of the number and apply the sign */
            std::string_view num_str = str.substr(cp - str.data());
            value.from_string(num_str);
            value.sign = sign;
            value.apply_typmod(typmod);

            res = NumericData::make_numeric(&value);
            value.free();
        }
        return res;
    }

    int
    NumericData::cmp(const NumericData *num1, const NumericData *num2)
    {
        int result;

        /*
         * We consider all NANs to be equal and larger than any non-NAN (including
         * Infinity).  This is somewhat arbitrary; the important thing is to have
         * a consistent sort order.
         */
        if (num1->is_special())
        {
            if (num1->is_nan())
            {
                if (num2->is_nan())
                    result = 0;/* NAN = NAN */
                else
                    result = 1;/* NAN > non-NAN */
            }
            else if (num1->is_pinf())
            {
                if (num2->is_nan())
                    result = -1;/* PINF < NAN */
                else if (num2->is_pinf())
                    result = 0;/* PINF = PINF */
                else
                    result = 1;/* PINF > anything else */
            }
            else/* num1 must be NINF */
            {
                if (num2->is_ninf())
                    result = 0;/* NINF = NINF */
                else
                    result = -1;/* NINF < anything else */
            }
        }
        else if (num2->is_special())
        {
            if (num2->is_ninf())
                result = 1;/* normal > NINF */
            else
                result = -1;/* normal < NAN or PINF */
        }
        else
        {
            result = cmp_var_common(num1->digits(), num1->ndigits(),
                                    num1->weight(), num1->sign(),
                                    num2->digits(), num2->ndigits(),
                                    num2->weight(), num2->sign());
        }
        return result;
    }

} // namespace springtail::numeric