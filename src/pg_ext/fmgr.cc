#include <cstring>
#include <stdexcept>
#include <pg_ext/fmgr.hh>

Datum DirectFunctionCall2(PGFunction func, Datum arg1, Datum arg2)
{
    LOCAL_FCINFO(fcinfo, 2);

    InitFunctionCallInfoData(*fcinfo, NULL, 2, 0, NULL, NULL);

    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = arg2;
    fcinfo->args[1].isnull = false;

    return func(fcinfo);
}

Datum DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2)
{
    LOCAL_FCINFO(fcinfo, 2);

    InitFunctionCallInfoData(*fcinfo, NULL, 2, collation, NULL, NULL);

	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = arg2;
	fcinfo->args[1].isnull = false;

	Datum result = (*func) (fcinfo);

    return result;
}

Datum get_fn_opclass_options(FmgrInfo *fcinfo)
{
    if (!fcinfo || !fcinfo->fn_extra)
        return (Datum)0; // return 0/null equivalent

    return reinterpret_cast<Datum>(fcinfo->fn_extra);
}

bool has_fn_opclass_options(FmgrInfo *fcinfo)
{
    return fcinfo && fcinfo->fn_extra != nullptr;
}

struct varlena *
pg_detoast_datum(struct varlena *datum)
{
    if (!datum){
        return nullptr;
    }
    return datum;
}

struct varlena *
pg_detoast_datum_packed(struct varlena *datum)
{
    return datum;
}

void getTypeOutputInfo(Oid type, Oid *funcOid, bool *typIsVarlena){
    std::cout << "getTypeOutputInfo: " << type << std::endl;
    auto it = _type_output_registry.find(type);
    if (it == _type_output_registry.end())
        throw std::runtime_error("getTypeOutputInfo: unknown type OID");

    if (funcOid)
        *funcOid = it->second.output_func;
    if (typIsVarlena)
        *typIsVarlena = it->second.is_varlena;
}

char* strdup_cxx(const std::string& str) {
    char* result = static_cast<char*>(malloc(str.size() + 1));
    if (result) {
        std::memcpy(result, str.c_str(), str.size() + 1);
    }
    return result;
}

// INT4OID (int32)
char* int4out(void* datum) {
    int32_t val = *reinterpret_cast<int32_t*>(datum);
    return strdup_cxx(std::to_string(val));
}

// FLOAT8OID (double)
char* float8out(void* datum) {
    double val = *reinterpret_cast<double*>(datum);
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%.17g", val);  // match PG precision
    return strdup_cxx(buf);
}

// BOOLOID
char* boolout(void* datum) {
    bool val = *reinterpret_cast<bool*>(datum);
    return strdup_cxx(val ? "t" : "f");
}

// OIDOID (uint32)
char* oidout(void* datum) {
    uint32_t val = *reinterpret_cast<uint32_t*>(datum);
    return strdup_cxx(std::to_string(val));
}

// NAMEOID (char[64] usually)
char* nameout(void* datum) {
    return strdup_cxx(reinterpret_cast<const char*>(datum));
}

// TEXTOID (struct varlena*)
char* textout(void* datum) {
    // Simplified for standalone: assume null-terminated
    return strdup_cxx(reinterpret_cast<const char*>(datum));
}

// VARCHAROID
char* varcharout(void* datum) {
    return textout(datum);  // Same as text for most use
}

// BPCHAROID (blank-padded char, strip trailing spaces)
char* bpcharout(void* datum) {
    std::string str(reinterpret_cast<const char*>(datum));
    size_t end = str.find_last_not_of(' ');
    if (end != std::string::npos)
        str.resize(end + 1);
    else
        str.clear();
    return strdup_cxx(str);
}

char* byteaout(void* datum) {
    // Postgres formats bytea as: "\\xABCD..."
    const uint8_t* data = reinterpret_cast<const uint8_t*>(datum);
    size_t len = /* figure out size some way, or pass it separately */ 0;

    char* out = static_cast<char*>(malloc(2 + len * 2 + 1));
    char* ptr = out;
    *ptr++ = '\\';
    *ptr++ = 'x';
    for (size_t i = 0; i < len; ++i) {
        std::sprintf(ptr, "%02x", data[i]);
        ptr += 2;
    }
    *ptr = '\0';
    return out;
}

PGFunction lookup_pgfunction_by_oid(Oid oid)
{
    return nullptr;
}

typedef char *Pointer;
static inline Pointer
DatumGetPointer(Datum X)
{
	return (Pointer) X;
}

static inline char *
DatumGetCString(Datum X)
{
	return (char *) DatumGetPointer(X);
}

Datum
FunctionCall1(FmgrInfo *flinfo,Datum arg1)
{
	LOCAL_FCINFO(fcinfo, 1);
	Datum		result;

	InitFunctionCallInfoData(*fcinfo, flinfo, 1, 0, NULL, NULL);

	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;

	result = FunctionCallInvoke(fcinfo);

	/* Check for null result, since caller is clearly not expecting one */
	if (fcinfo->isnull)
		std::cerr << "FunctionCall1Coll: null result" << std::endl;

	return result;
}

char *
OutputFunctionCall(FmgrInfo *flinfo, Datum val)
{
	return DatumGetCString(FunctionCall1(flinfo, val));
}

const char* OidOutputFunctionCall(Oid function_oid, Datum value)
{
    return nullptr;
}
