#pragma once
#include <cstddef>
#include <cstdint>
#include <pg_ext/export.hh>

typedef struct {
    int32_t length;
    int32_t elemtype;
    char data[];
} ArrayType;

extern "C" PGEXT_API ArrayType *construct_array_builtin(const void *elems, int nelems, uint32_t elmtype, size_t elem_size);
