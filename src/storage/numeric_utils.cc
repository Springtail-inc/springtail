#include <string>
#include <storage/numeric_utils.hh>

namespace springtail::numeric {

    void
    StringInfoData::copybytes(void *buf, int datalen)
    {
        if (datalen < 0 || datalen > (len - cursor)) {
            LOG_ERROR("Numeric: insufficient data left in message");
            DCHECK(false);
        }
        memcpy(buf, &data[cursor], datalen);
        cursor += datalen;
    }

    unsigned int
    StringInfoData:: getint(int b)
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
                LOG_ERROR("Numeric: unsupported integer size {}", b);
                DCHECK(false);
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
        buf[0] = 0;/* spare digit for rounding */
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
                    CHECK(number_of_digits == -1);/* better not have added > 1 digit */
                    CHECK(digits > buf);
                    digits--;
                    ndigits++;
                    weight++;
                }
            }
        }
    }

    bool
    NumericVar::apply_typmod(const TypeMod &typmod)
    {
        /* Do nothing if we have an invalid typmod */
        if (!typmod.is_valid())
            return true;


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

                    if (ddigits > maxdigits)
                    {
                        LOG_ERROR("Numeric: numeric field overflow; A field with precision {}, scale {} must round to an absolute value less than {}{}.",
                            precision, scale, maxdigits ? "10^" : "", maxdigits ? maxdigits : 1);
                        DCHECK(false);
                    }
                    break;
                }
                ddigits -= DEC_DIGITS;
            }
        }
        return true;
    }

    std::string
    NumericData::to_string() const
    {
        const NumericDigit *numeric_digits = digits();
        int numeric_ndigits = ndigits();

        std::vector<int16_t> digits_array(numeric_digits, numeric_digits + numeric_ndigits);
        std::string sign_str;
        switch(sign())
        {
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
                sign_str = fmt::format("SIGN=0x{:x}", sign());
                break;
        }
        std::string out = fmt::format("NUMERIC w={} d={} {} {:04}",
            weight(), dscale(), sign_str, fmt::join(digits_array, ""));
        return out;
    }

    void NumericData::dump(const char *str)
    {
        LOG_DEBUG(LOG_STORAGE, "{}: {}", str, this->to_string());
    }

    bool NumericData::apply_typmod_special(const TypeMod &typmod)
    {
        CHECK(is_special());/* caller error if not */

        /*
         * NaN is allowed regardless of the typmod; that's rather dubious perhaps,
         * but it's a longstanding behavior.  Inf is rejected if we have any
         * typmod restriction, since an infinity shouldn't be claimed to fit in
         * any finite number of digits.
         */
        if (is_nan()) {
            return true;
        }

        /* Do nothing if we have a default typmod (-1) */
        if (!typmod.is_valid()) {
            return true;
        }

        int precision = typmod.precision();
        int scale = typmod.scale();

        LOG_ERROR("Numeric: numeric field overflow; A field with precision {}, scale {} cannot hold an infinite value.",
                    precision, scale);
        DCHECK(false);
    }

    NumericData *NumericData::make_numeric(const NumericVar *var)
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
            if (!(sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF))
            {
                LOG_ERROR("Numeric: invalid numeric sign value 0x{:x}", sign);
                DCHECK(false);
            }

            result = static_cast<NumericData *>(::malloc(NUMERIC_HDRSZ_SHORT));

            result->set_varsize(NUMERIC_HDRSZ_SHORT);
            result->choice.n_header = sign;
            /* the header word is all we need */

            result->dump("make_result()");
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
        if (result->weight() != weight || result->dscale() != var->dscale)
        {
            LOG_ERROR("Numeric: alue overflows numeric format");
            DCHECK(false);
        }

        result->dump("make_result()");
        return result;
    }

    void NumericData::free_numeric(NumericData *make_numeric)
    {
        if (make_numeric != nullptr) {
            ::free(make_numeric);
        }
    }

    NumericData *NumericData::recv(StringInfo buf, uint32_t typmod)
    {
        NumericVar  value;
        NumericData *res;
        TypeMod typmod_obj(typmod);

        memset(&value, 0, sizeof(NumericVar));

        int len = (uint16_t) buf->getint(sizeof(uint16_t));

        value.alloc(len);

        value.weight = (int16_t) buf->getint(sizeof(int16_t));
        /* we allow any int16 for weight --- OK? */

        value.sign = (uint16_t) buf->getint(sizeof(uint16_t));
        if (!(value.sign == NUMERIC_POS ||
            value.sign == NUMERIC_NEG ||
            value.sign == NUMERIC_NAN ||
            value.sign == NUMERIC_PINF ||
            value.sign == NUMERIC_NINF))
        {
            LOG_ERROR("Numeric: invalid sign in external \"numeric\" value");
            DCHECK(false);
        }

        value.dscale = (uint16_t) buf->getint(sizeof(uint16_t));
        if ((value.dscale & NUMERIC_DSCALE_MASK) != value.dscale) {
            LOG_ERROR("Numeric: invalid scale in external \"numeric\" value");
            DCHECK(false);
        }

        for (int i = 0; i < len; i++)
        {
            NumericDigit d = buf->getint(sizeof(NumericDigit));

            if (d < 0 || d >= NBASE)
            {
                LOG_ERROR("Numeric: invalid digit in external \"numeric\" value");
                DCHECK(false);
            }
            value.digits[i] = d;
        }

        /*
         * If the given dscale would hide any digits, truncate those digits away.
         * We could alternatively throw an error, but that would take a bunch of
         * extra code (about as much as trunc_var involves), and it might cause
         * client compatibility issues.  Be careful not to apply trunc_var to
         * special values, as it could do the wrong thing; we don't need it
         * anyway, since make_result will ignore all but the sign field.
         *
         * After doing that, be sure to check the typmod restriction.
         */
        if (value.sign == NUMERIC_POS ||
            value.sign == NUMERIC_NEG)
        {
            value.trunc(value.dscale);

            (void) value.apply_typmod(typmod);

            res = NumericData::make_numeric(&value);
        }
        else
        {
            /* apply_typmod_special wants us to make the Numeric first */
            res = NumericData::make_numeric(&value);

            (void) res->apply_typmod_special(typmod);
        }

        value.free();

        return res;
    }

    int NumericData::cmp_abs_common(const NumericDigit *var1digits, int var1ndigits, int var1weight,
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

    int NumericData::cmp(const NumericData *num1, const NumericData *num2)
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