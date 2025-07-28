#pragma once

#include <cstdint>
#include <pg_ext/export.hh>

extern "C" PGEXT_API int pg_popcount(const char* buf, int len);
extern "C" PGEXT_API uint8_t pg_number_of_ones(uint8_t b);
