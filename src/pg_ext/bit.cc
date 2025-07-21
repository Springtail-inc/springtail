#include <pg_ext/bit.hh>

uint8_t pg_number_of_ones(uint8_t b)
{
    uint8_t count = 0;
    while (b)
    {
        count += b & 1;
        b >>= 1;
    }
    return count;
}

int pg_popcount(const char* buf, int len)
{
    int count = 0;
    for (int i = 0; i < len; ++i)
    {
        count += pg_number_of_ones(static_cast<uint8_t>(buf[i]));
    }
    return count;
}
