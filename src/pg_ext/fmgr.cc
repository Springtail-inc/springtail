#include <cstring>
#include <iostream>

#include <common/logging.hh>
#include <pg_ext/fmgr.hh>
#include <pg_ext/node.hh>
#include <pg_ext/type.hh>

Datum
DirectFunctionCall1(PGFunction func, Datum arg1)
{
    LOCAL_FCINFO(fcinfo, 1);

    InitFunctionCallInfoData(*fcinfo, nullptr, 1, 0, nullptr, nullptr);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;

    return func(fcinfo);
}

Datum
DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1)
{
    LOCAL_FCINFO(fcinfo, 1);

    InitFunctionCallInfoData(*fcinfo, nullptr, 1, collation, nullptr, nullptr);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;

    Datum result = (*func)(fcinfo);

    return result;
}

Datum
DirectFunctionCall2(PGFunction func, Datum arg1, Datum arg2)
{
    LOCAL_FCINFO(fcinfo, 2);

    InitFunctionCallInfoData(*fcinfo, nullptr, 2, 0, nullptr, nullptr);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = arg2;
    fcinfo->args[1].isnull = false;

    return func(fcinfo);
}

Datum
DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2)
{
    LOCAL_FCINFO(fcinfo, 2);

    InitFunctionCallInfoData(*fcinfo, nullptr, 2, collation, nullptr, nullptr);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = arg2;
    fcinfo->args[1].isnull = false;

    Datum result = (*func)(fcinfo);

    return result;
}

char *
OidOutputFunctionCall(Oid function_oid, Datum value)
{
    // XXX Stubbed for now
    return nullptr;
}

Datum DirectFunctionCall3(PGFunction func, Datum arg1, Datum arg2, Datum arg3)
{
    // NOSONAR - nargs must be 3 here: this previously allocated/declared only 2 args and never
    // wrote arg3 into fcinfo->args[2] at all, so any 3-arg callee reading its 3rd argument (e.g.
    // GIST_PENALTY's output pointer, or a receive function's typmod) read uninitialized stack
    // memory past the end of the 2-slot LOCAL_FCINFO buffer. Found while wiring GIST_PENALTY
    // (btree redesign phase 10) through this call -- a real, pre-existing bug, not introduced
    // here; every other caller of DirectFunctionCall3 today happens to invoke a function that
    // never reads its 3rd argument, so this never manifested before.
    LOCAL_FCINFO(fcinfo, 3);

    InitFunctionCallInfoData(*fcinfo, nullptr, 3, 0, nullptr, nullptr);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = arg2;
    fcinfo->args[1].isnull = false;
    fcinfo->args[2].value = arg3;
    fcinfo->args[2].isnull = false;

    return func(fcinfo);
}

Datum
DirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3)
{
    // NOSONAR - see DirectFunctionCall3's comment above; the same undersized-buffer/missing-arg3
    // bug existed here too.
    LOCAL_FCINFO(fcinfo, 3);

    InitFunctionCallInfoData(*fcinfo, nullptr, 3, collation, nullptr, nullptr);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = arg2;
    fcinfo->args[1].isnull = false;
    fcinfo->args[2].value = arg3;
    fcinfo->args[2].isnull = false;

    Datum result = (*func)(fcinfo);

    return result;
}

Datum
DirectFunctionCall4(PGFunction func, Datum arg1, Datum arg2, Datum arg3, Datum arg4)
{
    LOCAL_FCINFO(fcinfo, 4);

    InitFunctionCallInfoData(*fcinfo, nullptr, 4, 0, nullptr, nullptr);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = arg2;
    fcinfo->args[1].isnull = false;
    fcinfo->args[2].value = arg3;
    fcinfo->args[2].isnull = false;
    fcinfo->args[3].value = arg4;
    fcinfo->args[3].isnull = false;

    return func(fcinfo);
}

Datum
DirectFunctionCall5(PGFunction func, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5)
{
    // Added for GIST_CONSISTENT (btree redesign phase 10), whose real Postgres signature is
    // (GISTENTRY *entry, Datum query, StrategyNumber strategy, Oid subtype, bool *recheck) -- 5
    // args -- and had no DirectFunctionCallN counterpart here before.
    LOCAL_FCINFO(fcinfo, 5);

    InitFunctionCallInfoData(*fcinfo, nullptr, 5, 0, nullptr, nullptr);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = arg2;
    fcinfo->args[1].isnull = false;
    fcinfo->args[2].value = arg3;
    fcinfo->args[2].isnull = false;
    fcinfo->args[3].value = arg4;
    fcinfo->args[3].isnull = false;
    fcinfo->args[4].value = arg5;
    fcinfo->args[4].isnull = false;

    return func(fcinfo);
}

Datum
get_fn_opclass_options(FmgrInfo *fcinfo)
{
    if (!fcinfo || !fcinfo->fn_extra) {
        return (Datum)0;  // return 0/null equivalent
    }

    return reinterpret_cast<Datum>(fcinfo->fn_extra);
}

bool
has_fn_opclass_options(FmgrInfo *fcinfo)
{
    return fcinfo && fcinfo->fn_extra != nullptr;
}

char *
strdup_cxx(const std::string &str)
{
    auto *result = static_cast<char *>(palloc(str.size() + 1));
    if (result) {
        std::memcpy(result, str.c_str(), str.size() + 1);
    }
    return result;
}

Datum
FunctionCall1(FmgrInfo *flinfo, Datum arg1)
{
    LOCAL_FCINFO(fcinfo, 1);
    Datum result;

    InitFunctionCallInfoData(*fcinfo, flinfo, 1, 0, nullptr, nullptr);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;

    result = FunctionCallInvoke(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo->isnull) {
        std::cerr << "FunctionCall1Coll: null result" << std::endl;
    }

    return result;
}

Datum
InputFunctionCall(FmgrInfo *flinfo, char *str, Oid typioparam, int32_t typmod)
{
    LOCAL_FCINFO(fcinfo, 3);
    Datum result;

    if (str == nullptr && flinfo->fn_strict) {
        return (Datum)0; /* just return null result */
    }

    InitFunctionCallInfoData(*fcinfo, flinfo, 3, InvalidOid, nullptr, nullptr);

    fcinfo->args[0].value = CStringGetDatum(str);
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = ObjectIdGetDatum(typioparam);
    fcinfo->args[1].isnull = false;
    fcinfo->args[2].value = Int32GetDatum(typmod);
    fcinfo->args[2].isnull = false;

    result = FunctionCallInvoke(fcinfo);

    /* Should get null result if and only if str is NULL */
    if (str == nullptr) {
        if (!fcinfo->isnull) {
            LOG_ERROR("input function {} returned non-NULL", flinfo->fn_oid);
        }
    } else {
        if (fcinfo->isnull) {
            LOG_ERROR("input function {} returned NULL", flinfo->fn_oid);
        }
    }

    return result;
}

char *
OutputFunctionCall(FmgrInfo *flinfo, Datum val)
{
    return DatumGetCString(FunctionCall1(flinfo, val));
}

TypeFuncClass
get_call_result_type(FunctionCallInfo fcinfo, Oid *resultTypeId, TupleDesc *resultTupleDesc)
{
    return TypeFuncClass::TYPEFUNC_SCALAR;
}

Oid
get_call_expr_argtype(Node *expr, int argnum)
{
    // XXX Stubbed for now
    return InvalidOid;
}

Oid
get_fn_expr_argtype(FmgrInfo *flinfo, int argnum)
{
    if (!flinfo || !flinfo->fn_expr) {
        return InvalidOid;
    }

    return get_call_expr_argtype(flinfo->fn_expr, argnum);
}

void
getTypeInputInfo(Oid type, Oid *typInput, Oid *typIOParam)
{
    // XXX Stubbed for now
}

FuncCallContext *
per_MultiFuncCall(PG_FUNCTION_ARGS)
{
    FuncCallContext *retval = (FuncCallContext *)fcinfo->flinfo->fn_extra;

    return retval;
}

void
end_MultiFuncCall(PG_FUNCTION_ARGS, FuncCallContext *funcctx)
{
    // XXX Stubbed for now
}

FuncCallContext *
init_MultiFuncCall(PG_FUNCTION_ARGS)
{
    // XXX Stubbed for now
    return nullptr;
}

void
fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, MemoryContext mcxt)
{
    // XXX Stubbed for now
}

void getTypeOutputInfo(Oid type, Oid *funcOid, bool *typIsVarlena)
{
    // XXX Stubbed for now
}

void
getTypeBinaryInputInfo(Oid type, Oid *typReceive, Oid *typIOParam)
{
    // XXX Stubbed for now
}
