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

void enlargeStringInfo(StringInfo str, int needed) {
    if (str->maxlen == 0) {
        str->data = (char *) std::malloc(needed);
        str->maxlen = needed;
    } else {
        str->data = (char *) std::realloc(str->data, str->maxlen + needed);
        str->maxlen += needed;
    }
    str->len = 0;
    str->cursor = 0;
}

int pq_getmsgint(StringInfo str, int size) {
    int value = 0;
    for (int i = 0; i < size; i++) {
        value = (value << 8) | (unsigned char) str->data[str->cursor++];
    }
    return value;
}

void initStringInfo(StringInfo str) {
    int size = 1024;
    str->data = (char *) std::malloc(size);
    str->maxlen = size;
    str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}

void appendStringInfoChar(StringInfo str, char ch) {
    if (str->len + 1 > str->maxlen) {
        enlargeStringInfo(str, 1);
    }
    str->data[str->len++] = ch;
    str->data[str->len] = '\0';
}

void pq_begintypsend(StringInfo str) {
    initStringInfo(str);
}

double pq_getmsgfloat8(StringInfo msg) {
    // XXX Stubbed for now
    return 0.0;
}

void pq_sendfloat8(StringInfo str, double value) {
    // XXX Stubbed for now
}

bytea *pq_endtypsend(StringInfo buf) {
    // XXX Stubbed for now
    return nullptr;
}

void appendStringInfoString(StringInfo str, const char *s) {
    if (str->len + strlen(s) + 1 > str->maxlen) {
        enlargeStringInfo(str, strlen(s) + 1);
    }
    std::strcat(str->data, s);
    str->len += strlen(s);
}
