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

ArrayType *construct_empty_array(uint32_t elmtype) {
    size_t total_size = sizeof(ArrayType);
    auto *arr = (ArrayType *) std::malloc(total_size);
    arr->length = 0;
    arr->elemtype = elmtype;
    return arr;
}

ArrayType *construct_md_array(const void *elems, int nelems, uint32_t elmtype, size_t elem_size, int ndim, const int *dims) {
    size_t total_size = sizeof(ArrayType) + nelems * elem_size;
    auto *arr = (ArrayType *) std::malloc(total_size);
    arr->length = nelems;
    arr->elemtype = elmtype;
    std::memcpy(arr->data, elems, nelems * elem_size);
    return arr;
}

void deconstruct_array_builtin(ArrayType *array, Oid elmtype, Datum **elemsp, bool **nullsp, int *nelemsp) {
    // XXX Stubbed for now
}

int ArrayGetNItems(int ndim, const int *dims) {
    int nitems = 1;
    for (int i = 0; i < ndim; i++) {
        nitems *= dims[i];
    }
    return nitems;
}

bool array_contains_nulls(ArrayType *array) {
    for (int i = 0; i < array->length; i++) {
        if (array->data[i] == '\0') {
            return true;
        }
    }
    return false;
}
