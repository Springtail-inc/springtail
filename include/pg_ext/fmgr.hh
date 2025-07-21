#pragma once

#include <cstdint>

// Collation is often just a 4-byte OID in Postgres
typedef uint32_t Oid;
typedef uintptr_t Datum;
typedef struct FunctionCallInfoData* FunctionCallInfo;
typedef Datum (*PGFunction)(FunctionCallInfo fcinfo);

struct FunctionCallInfoData
{
    PGFunction   fn;
    void*         fn_extra;
    void*         context;
    Oid           fn_collation;
    bool*         argnull;
    Datum         args[2];
    int           nargs;
};

// Function interfaces
Datum DirectFunctionCall2(PGFunction func, Datum arg1, Datum arg2);
Datum DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2);
Datum get_fn_opclass_options(FunctionCallInfo fcinfo);
bool has_fn_opclass_options(FunctionCallInfo fcinfo);
