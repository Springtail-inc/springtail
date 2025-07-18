#pragma once

#include <sys/types.h>
#include <pg_ext/export.hh>


extern PGEXT_API const u_int8_t pg_leftmost_one_pos[256];
extern PGEXT_API const u_int8_t pg_rightmost_one_pos[256];
extern PGEXT_API const u_int8_t pg_number_of_ones[256];
