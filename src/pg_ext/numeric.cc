#include <pg_ext/numeric.hh>

Datum numeric_in(FunctionCallInfo fcinfo) {
    // XXX Stubbed for now
    return 0;
}

int
pg_leftmost_one_pos32(uint32_t word)
{
#ifdef HAVE__BUILTIN_CLZ
	assert(word != 0);

	return 31 - __builtin_clz(word);
#elif defined(_MSC_VER)
	unsigned long result;
	bool		non_zero;

	non_zero = _BitScanReverse(&result, word);
	assert(non_zero);
	return (int) result;
#else
	int			shift = 32 - 8;

	assert(word != 0);

	while ((word >> shift) == 0)
		shift -= 8;

	return shift + pg_leftmost_one_pos[(word >> shift) & 255];
#endif							/* HAVE__BUILTIN_CLZ */
}

int
decimalLength32(const uint32_t v)
{
	int			t = 0;
	static const uint32_t PowersOfTen[] = {
		1, 10, 100,
		1000, 10000, 100000,
		1000000, 10000000, 100000000,
		1000000000
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos32(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

int
pg_ultoa_n(uint32_t value, char *a)
{
	int			olength = 0,
				i = 0;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength32(value);

	/* Compute the result string. */
	while (value >= 10000)
	{
		const uint32_t c = value - 10000 * (value / 10000);
		const uint32_t c0 = (c % 100) << 1;
		const uint32_t c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value >= 100)
	{
		const uint32_t c = (value % 100) << 1;

		char	   *pos = a + olength - i;

		value /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value >= 10)
	{
		const uint32_t c = value << 1;

		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
	{
		*a = (char) ('0' + value);
	}

	return olength;
}

char *
pg_ultostr(char *str, uint32_t value)
{
	int len = pg_ultoa_n(value, str);

	return str + len;
}

char *
pg_ultostr_zeropad(char *str, uint32_t value, int32_t minwidth)
{
	int			len = 0;

	assert(minwidth > 0);

	if (value < 100 && minwidth == 2)	/* Short cut for common case */
	{
		memcpy(str, DIGIT_TABLE + value * 2, 2);
		return str + 2;
	}

	len = pg_ultoa_n(value, str);
	if (len >= minwidth)
		return str + len;

	memmove(str + minwidth - len, str, len);
	memset(str, '0', minwidth - len);
	return str + minwidth;
}
