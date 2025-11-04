#include <pg_ext/sort.hh>

void pg_qsort(void* base, size_t nel, size_t elsize, int (*cmp)(const void*, const void*))
{
    std::qsort(base, nel, elsize, cmp);
}
