#pragma once

#include <pg_ext/export.hh>

#include <cstddef>
#include <cstdlib>

extern "C" PGEXT_API void pg_qsort(void* base, size_t nel, size_t elsize, int (*cmp)(const void*, const void*));
