#include <pg_ext/bit.hh>

int pg_popcount(const char* buf, int len)
{
    auto count = 0;
    for (int i = 0; i < len; ++i)
    {
        count += pg_number_of_ones[static_cast<uint8_t>(buf[i])];
    }
    return count;
}

uint64_t pg_popcount_slow(const char *buf, int bytes)
{
	auto popcnt = 0ULL;
	while (bytes--)
    popcnt += pg_number_of_ones[(unsigned char) *buf++];
	return popcnt;
}

uint64_t pg_popcount_optimized(const char *buf, int bytes)
{
	return pg_popcount_slow(buf, bytes);
}

uint64_t pg_popcount64_slow(uint64_t word)
{
	auto popcnt = 0ULL;
	while (word)
	{
		popcnt += word & 1;
		word >>= 1;
	}
	return popcnt;
}

uint64_t pg_popcount64(const char *buf, int bytes)
{
	auto popcnt = 0ULL;

    const auto *words = (const uint64_t *) buf;

    while (bytes >= 8)
    {
        popcnt += pg_popcount64_slow(*words++);
        bytes -= 8;
    }

    buf = (const char *) words;

		/* Process any remaining bytes */
	while (bytes--)
    popcnt += pg_number_of_ones[(unsigned char) *buf++];

	return popcnt;
}
