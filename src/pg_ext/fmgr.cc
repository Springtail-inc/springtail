#include <pg_ext/fmgr.hh>

Datum DirectFunctionCall2(PGFunction func, Datum arg1, Datum arg2)
{
    FunctionCallInfoData fcinfo;
    fcinfo.fn = func;
    fcinfo.fn_extra = nullptr;
    fcinfo.context = nullptr;
    fcinfo.fn_collation = 0; // Default collation
    fcinfo.nargs = 2;
    fcinfo.args[0] = arg1;
    fcinfo.args[1] = arg2;
    fcinfo.argnull = nullptr;

    return func(&fcinfo);
}

Datum DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2)
{
    FunctionCallInfoData fcinfo;
    fcinfo.fn = func;
    fcinfo.fn_extra = nullptr;
    fcinfo.context = nullptr;
    fcinfo.fn_collation = collation;
    fcinfo.nargs = 2;
    fcinfo.args[0] = arg1;
    fcinfo.args[1] = arg2;
    fcinfo.argnull = nullptr;

    return func(&fcinfo);
}

Datum get_fn_opclass_options(FunctionCallInfo fcinfo)
{
    if (!fcinfo || !fcinfo->fn_extra)
        return (Datum)0; // return 0/null equivalent

    return reinterpret_cast<Datum>(fcinfo->fn_extra);
}

bool has_fn_opclass_options(FunctionCallInfo fcinfo)
{
    return fcinfo && fcinfo->fn_extra != nullptr;
}
