#pragma once

#include <cstdint>
#include <pg_ext/export.hh>

extern PGEXT_API int pg_popcount(const char* buf, int len);
extern PGEXT_API uint8_t pg_number_of_ones(uint8_t b);
