#pragma once
#include <pg_ext/export.hh>
#include <pg_ext/string.hh>

extern "C" PGEXT_API char * float8out_internal(double num);
extern "C" PGEXT_API double float8in_internal(char *num, char **endptr_p,
    const char *type_name, const char *orig_string,
    struct Node *escontext);
