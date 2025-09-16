#pragma once

#include <pg_ext/export.hh>
#include <pg_ext/common.hh>

#define TYPTYPE_COMPOSITE 'c' /* composite (e.g., table's rowtype) */
#define TYPTYPE_DOMAIN 'd'    /* domain over another type */

extern "C" PGEXT_API bool type_is_rowtype(Oid typid);
extern "C" PGEXT_API char get_typtype(Oid typid);
extern "C" PGEXT_API Oid getBaseType(Oid typid);
