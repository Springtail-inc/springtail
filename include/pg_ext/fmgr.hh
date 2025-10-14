#pragma once

#include <pg_ext/common.hh>
#include <pg_ext/export.hh>
#include <pg_ext/memory.hh>
#include <pg_ext/toast.hh>
#include <pg_ext/varatt.hh>
#include <pg_ext/heaptuple.hh>

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo

struct NullableDatum {
    Datum value;
    bool isnull;
};

typedef char *Pointer;

static inline Pointer
DatumGetPointer(Datum X)
{
    return (Pointer)X;
}

static inline char *
DatumGetCString(Datum X)
{
    return (char *)DatumGetPointer(X);
}

static inline char
DatumGetChar(Datum X)
{
	return (char) X;
}

static inline int16_t
DatumGetInt16(Datum X)
{
	return (int16_t) X;
}

static inline int32_t
DatumGetInt32(Datum X)
{
	return (int32_t) X;
}

static inline int64_t
DatumGetInt64(Datum X)
{
	return (int64_t) X;
}

static inline Datum
PointerGetDatum(const void *X)
{
    return (Datum)X;
}

static inline Datum
Int64GetDatum(uint64_t X)
{
    return (Datum)X;
}

static inline Datum
Int32GetDatum(uint32_t X)
{
    return (Datum)X;
}

static inline Datum
Int16GetDatum(uint16_t X)
{
    return (Datum)X;
}

static inline Datum
ObjectIdGetDatum(Oid X)
{
    return (Datum)X;
}

static inline bool
DatumGetBool(Datum X)
{
    return (bool)X;
}

static inline Datum
CStringGetDatum(const char *str)
{
    return (Datum)str;
}

static inline Datum
CharGetDatum(char X)
{
	return (Datum) X;
}

#define att_addlength_pointer(cur_offset, attlen, attptr) \
( \
	((attlen) > 0) ? \
	( \
		(cur_offset) + (attlen) \
	) \
	: (((attlen) == -1) ? \
	( \
		(cur_offset) + VARSIZE_ANY(attptr) \
	) \
	: \
	( \
		AssertMacro((attlen) == -2), \
		(cur_offset) + (strlen((char *) (attptr)) + 1) \
	)) \
)

#define att_addlength_datum(cur_offset, attlen, attdatum) \
	att_addlength_pointer(cur_offset, attlen, DatumGetPointer(attdatum))
#define att_align_nominal(cur_offset, attalign) \
( \
    ((attalign) == TYPALIGN_INT) ? INTALIGN(cur_offset) : \
        (((attalign) == TYPALIGN_CHAR) ? (uintptr_t) (cur_offset) : \
        (((attalign) == TYPALIGN_DOUBLE) ? DOUBLEALIGN(cur_offset) : \
        ( \
            AssertMacro((attalign) == TYPALIGN_SHORT), \
            SHORTALIGN(cur_offset) \
        ))) \
)

/* Type categories for get_call_result_type and siblings */
typedef enum TypeFuncClass {
    TYPEFUNC_SCALAR,           /* scalar result type */
    TYPEFUNC_COMPOSITE,        /* determinable rowtype result */
    TYPEFUNC_COMPOSITE_DOMAIN, /* domain over determinable rowtype result */
    TYPEFUNC_RECORD,           /* indeterminate rowtype result */
    TYPEFUNC_OTHER             /* bogus type, eg pseudotype */
} TypeFuncClass;

typedef struct FunctionCallInfoBaseData *FunctionCallInfo;
typedef Datum (*PGFunction)(FunctionCallInfo fcinfo);

typedef struct Node *fmNodePtr;
typedef struct FmgrInfo {
    PGFunction fn_addr;           /* pointer to function or handler to be called */
    Oid fn_oid;                   /* OID of function (NOT of handler, if any) */
    short fn_nargs;               /* number of input args (0..FUNC_MAX_ARGS) */
    bool fn_strict;               /* function is "strict" (NULL in => NULL out) */
    bool fn_retset;               /* function returns a set */
    unsigned char fn_stats;       /* collect stats if track_functions > this */
    void *fn_extra;               /* extra space for use by handler */
    MemoryContext fn_mcxt; /* memory context to store fn_extra in */
    fmNodePtr fn_expr;            /* expression parse tree for call, or NULL */
} FmgrInfo;

constexpr int32_t FIELDNO_FUNCTIONCALLINFODATA_ISNULL = 4;
constexpr int32_t FIELDNO_FUNCTIONCALLINFODATA_ARGS = 6;
constexpr int32_t FIELDNO_HEAPTUPLEDATA_DATA = 3;

typedef struct FunctionCallInfoBaseData {
    FmgrInfo *flinfo;     /* ptr to lookup info used for this call */
    fmNodePtr context;    /* pass info about context of call */
    fmNodePtr resultinfo; /* pass or return extra info about result */
    Oid fncollation;      /* collation for function to use */
    bool isnull; /* function must set true if result is NULL */
    short nargs; /* # arguments actually passed */
    NullableDatum args[FLEXIBLE_ARRAY_MEMBER];
} FunctionCallInfoBaseData;

struct FunctionCallInfoData {
    FmgrInfo fn;
    void *fn_extra;
    void *context;
    Oid fn_collation;
    bool *argnull;
    Datum args[2];
    int nargs;
};

typedef struct FuncCallContext {
    uint64_t call_cntr;

    uint64_t max_calls;
    void *user_fctx;

    AttInMetadata *attinmeta;

    MemoryContext multi_call_memory_ctx;
    TupleDesc tuple_desc;

} FuncCallContext;

#define SizeForFunctionCallInfo(nargs) \
    (offsetof(FunctionCallInfoBaseData, args) + sizeof(NullableDatum) * (nargs))

#define LOCAL_FCINFO(name, nargs)                                        \
    /* use union with FunctionCallInfoBaseData to guarantee alignment */ \
    union {                                                              \
        FunctionCallInfoBaseData fcinfo;                          \
        /* ensure enough space for nargs args is available */            \
        char fcinfo_data[SizeForFunctionCallInfo(nargs)];                \
    } name##data;                                                        \
    FunctionCallInfo name = &name##data.fcinfo

#define InitFunctionCallInfoData(Fcinfo, Flinfo, Nargs, Collation, Context, Resultinfo) \
    do {                                                                                \
        (Fcinfo).flinfo = (Flinfo);                                                     \
        (Fcinfo).context = (Context);                                                   \
        (Fcinfo).resultinfo = (Resultinfo);                                             \
        (Fcinfo).fncollation = (Collation);                                             \
        (Fcinfo).isnull = false;                                                        \
        (Fcinfo).nargs = (Nargs);                                                       \
    } while (0)

#define FunctionCallInvoke(fcinfo) ((*(fcinfo)->flinfo->fn_addr)(fcinfo))

// Function interfaces
extern "C" PGEXT_API Datum DirectFunctionCall1(PGFunction func, Datum arg1);
extern "C" PGEXT_API Datum DirectFunctionCall1Coll(PGFunction func,
                                                   Oid collation,
                                                   Datum arg1);
extern "C" PGEXT_API Datum DirectFunctionCall2(PGFunction func, Datum arg1, Datum arg2);
extern "C" PGEXT_API Datum DirectFunctionCall2Coll(PGFunction func,
                                                   Oid collation,
                                                   Datum arg1,
                                                   Datum arg2);
extern "C" PGEXT_API Datum DirectFunctionCall3(PGFunction func,
                                               Datum arg1,
                                               Datum arg2,
                                               Datum arg3);
extern "C" PGEXT_API Datum DirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3);
extern "C" PGEXT_API Datum InputFunctionCall(FmgrInfo *flinfo,
                                             char *str,
                                             Oid typioparam,
                                             int32_t typmod);
extern "C" PGEXT_API Datum get_fn_opclass_options(FmgrInfo *fcinfo);
extern "C" PGEXT_API bool has_fn_opclass_options(FmgrInfo *fcinfo);
extern "C" PGEXT_API PGFunction lookup_pgfunction_by_oid(Oid oid);
extern "C" PGEXT_API char *OidOutputFunctionCall(Oid function_oid, Datum value);
extern "C" PGEXT_API char *OutputFunctionCall(FmgrInfo *flinfo, Datum val);
extern "C" PGEXT_API void getTypeOutputInfo(Oid type, Oid *funcOid, bool *typIsVarlena);
extern "C" PGEXT_API void getTypeInputInfo(Oid type, Oid *typInput, Oid *typIOParam);
extern "C" PGEXT_API TypeFuncClass get_call_result_type(FunctionCallInfo fcinfo,
                                                               Oid *resultTypeId,
                                                               TupleDesc *resultTupleDesc);
extern "C" PGEXT_API Datum OidFunctionCall3Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3);
extern "C" PGEXT_API Oid get_fn_expr_argtype(FmgrInfo *flinfo, int argnum);
extern "C" PGEXT_API FuncCallContext *per_MultiFuncCall(FunctionCallInfo fcinfo);
extern "C" PGEXT_API void fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, MemoryContext mcxt);

// Type interfaces
extern "C" PGEXT_API void end_MultiFuncCall(PG_FUNCTION_ARGS, FuncCallContext *funcctx);
extern "C" PGEXT_API FuncCallContext *init_MultiFuncCall(PG_FUNCTION_ARGS);
extern "C" PGEXT_API void getTypeBinaryInputInfo(Oid type, Oid *typReceive, Oid *typIOParam);
