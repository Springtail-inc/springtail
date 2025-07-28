#include <pg_ext/array.hh>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

ArrayType *construct_array_builtin(const void *elems, int nelems, uint32_t elmtype, size_t elem_size) {
    size_t total_size = sizeof(ArrayType) + nelems * elem_size;
    auto *arr = (ArrayType *) std::malloc(total_size);
    arr->length = nelems;
    arr->elemtype = elmtype;
    std::memcpy(arr->data, elems, nelems * elem_size);
    return arr;
}
