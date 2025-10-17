#pragma once

#include <pg_ext/common.hh>
#include <pg_ext/export.hh>
#include <pg_ext/node.hh>
#include <pg_ext/fmgr.hh>

struct DomainIOData {
    Oid domain_type;
    /* Data needed to call base type's input function */
    Oid typiofunc;
    Oid typioparam;
    int32_t typtypmod;
    FmgrInfo proc;
    // /* Reference to cached list of constraint items to check */
    // DomainConstraintRef constraint_ref;
    // /* Context for evaluating CHECK constraints in */
    // pgext::ExprContext *econtext;
    /* Memory context this cache is in */
    MemoryContext mcxt;
};

extern "C" PGEXT_API void domain_check(Oid domainoid, Datum value, bool isnull);
extern "C" PGEXT_API void domain_check_input(Datum value,
                                             bool isnull,
                                             DomainIOData *my_extra,
                                             Node *escontext);
