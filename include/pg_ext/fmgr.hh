#pragma once

#include <pg_ext/common.hh>
#include <pg_ext/export.hh>
#include <pg_ext/memory.hh>
#include <pg_ext/type.hh>
#include <pg_ext/toast.hh>
#include <pg_ext/varatt.hh>

#define PG_FUNCTION_ARGS pgext::FunctionCallInfo fcinfo

namespace pgext {
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
	att_addlength_pointer(cur_offset, attlen, pgext::DatumGetPointer(attdatum))
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

typedef struct {
    // XXX Stubbed for now
} FormData_pg_attribute;

typedef int16_t AttrNumber;

typedef struct ConstrCheck {
    char *ccname;
    char *ccbin; /* nodeToString representation of expr */
    bool ccvalid;
    bool ccnoinherit; /* this is a non-inheritable constraint */
} ConstrCheck;

typedef struct AttrDefault {
    AttrNumber adnum;
    char *adbin; /* nodeToString representation of expr */
} AttrDefault;

typedef struct TupleConstr {
    AttrDefault *defval;         /* array */
    ConstrCheck *check;          /* array */
    struct AttrMissing *missing; /* missing attributes values, NULL if none */
    uint16_t num_defval;
    uint16_t num_check;
    bool has_not_null;
    bool has_generated_stored;
} TupleConstr;

typedef struct TupleDescData {
    int natts;           /* number of attributes in the tuple */
    Oid tdtypeid;        /* composite type ID for tuple type */
    int32_t tdtypmod;    /* typmod for tuple type */
    int tdrefcount;      /* reference count, or -1 if not counting */
    TupleConstr *constr; /* constraints, or NULL if none */
    /* attrs[N] is the description of Attribute Number N+1 */
    FormData_pg_attribute attrs[FLEXIBLE_ARRAY_MEMBER];
} TupleDescData;
typedef struct TupleDescData *TupleDesc;

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
    pgext::MemoryContext fn_mcxt; /* memory context to store fn_extra in */
    fmNodePtr fn_expr;            /* expression parse tree for call, or NULL */
} FmgrInfo;

typedef struct FunctionCallInfoBaseData {
    FmgrInfo *flinfo;     /* ptr to lookup info used for this call */
    fmNodePtr context;    /* pass info about context of call */
    fmNodePtr resultinfo; /* pass or return extra info about result */
    Oid fncollation;      /* collation for function to use */
#define FIELDNO_FUNCTIONCALLINFODATA_ISNULL 4
    bool isnull; /* function must set true if result is NULL */
    short nargs; /* # arguments actually passed */
#define FIELDNO_FUNCTIONCALLINFODATA_ARGS 6
    pgext::NullableDatum args[FLEXIBLE_ARRAY_MEMBER];
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

typedef struct AttInMetadata {
    TupleDesc tupdesc;
    FmgrInfo *attinfuncs;
    Oid *attioparams;
    int32_t *atttypmods;
} AttInMetadata;

typedef struct FuncCallContext {
    uint64_t call_cntr;

    uint64_t max_calls;
    void *user_fctx;

    AttInMetadata *attinmeta;

    MemoryContext multi_call_memory_ctx;
    TupleDesc tuple_desc;

} FuncCallContext;

}  // namespace pgext

#define SizeForFunctionCallInfo(nargs) \
    (offsetof(pgext::FunctionCallInfoBaseData, args) + sizeof(pgext::NullableDatum) * (nargs))

#define LOCAL_FCINFO(name, nargs)                                        \
    /* use union with FunctionCallInfoBaseData to guarantee alignment */ \
    union {                                                              \
        pgext::FunctionCallInfoBaseData fcinfo;                          \
        /* ensure enough space for nargs args is available */            \
        char fcinfo_data[SizeForFunctionCallInfo(nargs)];                \
    } name##data;                                                        \
    pgext::FunctionCallInfo name = &name##data.fcinfo

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
extern "C" PGEXT_API Datum DirectFunctionCall1(pgext::PGFunction func, Datum arg1);
extern "C" PGEXT_API Datum DirectFunctionCall1Coll(pgext::PGFunction func,
                                                   Oid collation,
                                                   Datum arg1);
extern "C" PGEXT_API Datum DirectFunctionCall2(pgext::PGFunction func, Datum arg1, Datum arg2);
extern "C" PGEXT_API Datum DirectFunctionCall2Coll(pgext::PGFunction func,
                                                   Oid collation,
                                                   Datum arg1,
                                                   Datum arg2);
extern "C" PGEXT_API Datum DirectFunctionCall3(pgext::PGFunction func,
                                               Datum arg1,
                                               Datum arg2,
                                               Datum arg3);
extern "C" PGEXT_API Datum DirectFunctionCall3Coll(pgext::PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3);
extern "C" PGEXT_API Datum InputFunctionCall(pgext::FmgrInfo *flinfo,
                                             char *str,
                                             Oid typioparam,
                                             int32_t typmod);
extern "C" PGEXT_API Datum get_fn_opclass_options(pgext::FmgrInfo *fcinfo);
extern "C" PGEXT_API bool has_fn_opclass_options(pgext::FmgrInfo *fcinfo);
extern "C" PGEXT_API pgext::PGFunction lookup_pgfunction_by_oid(Oid oid);
extern "C" PGEXT_API char *OidOutputFunctionCall(Oid function_oid, Datum value);
extern "C" PGEXT_API char *OutputFunctionCall(pgext::FmgrInfo *flinfo, Datum val);
extern "C" PGEXT_API void getTypeOutputInfo(Oid type, Oid *funcOid, bool *typIsVarlena);
extern "C" PGEXT_API void getTypeInputInfo(Oid type, Oid *typInput, Oid *typIOParam);
extern "C" PGEXT_API pgext::TypeFuncClass get_call_result_type(pgext::FunctionCallInfo fcinfo,
                                                               Oid *resultTypeId,
                                                               pgext::TupleDesc *resultTupleDesc);
extern "C" PGEXT_API Datum OidFunctionCall3Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3);
extern "C" PGEXT_API Oid get_fn_expr_argtype(pgext::FmgrInfo *flinfo, int argnum);
extern "C" PGEXT_API pgext::FuncCallContext *per_MultiFuncCall(pgext::FunctionCallInfo fcinfo);
extern "C" PGEXT_API void fmgr_info_cxt(Oid functionId, pgext::FmgrInfo *finfo, pgext::MemoryContext mcxt);

// Type interfaces
extern "C" PGEXT_API pgext::TupleDesc lookup_rowtype_tupdesc_domain(Oid type_id,
                                                                    int32_t typmod,
                                                                    bool noError);
extern "C" PGEXT_API void end_MultiFuncCall(PG_FUNCTION_ARGS, pgext::FuncCallContext *funcctx);
extern "C" PGEXT_API pgext::FuncCallContext *init_MultiFuncCall(PG_FUNCTION_ARGS);

struct HeapTupleHeaderData {};
typedef struct HeapTupleHeaderData *HeapTupleHeader;

struct BlockIdData {
    uint16_t bi_hi;
    uint16_t bi_lo;
};

struct ItemPointerData {
    BlockIdData ip_blkid;
    uint16_t ip_posid;
};
typedef ItemPointerData *ItemPointer;

struct HeapTupleData {
    uint32_t t_len;         /* length of *t_data */
    ItemPointerData t_self; /* SelfItemPointer */
    Oid t_tableOid;         /* table the tuple came from */
#define FIELDNO_HEAPTUPLEDATA_DATA 3
    HeapTupleHeader t_data; /* -> tuple header and data */
};
typedef struct HeapTupleData *HeapTuple;

extern "C" PGEXT_API void DecrTupleDescRefCount(pgext::TupleDesc tupdesc);
extern "C" PGEXT_API pgext::TupleDesc BlessTupleDesc(pgext::TupleDesc tupdesc);
extern "C" PGEXT_API Datum HeapTupleHeaderGetDatum(HeapTupleHeader tuple);
extern "C" PGEXT_API HeapTuple heap_form_tuple(pgext::TupleDesc tupleDescriptor, Datum *values, bool *isnull);
extern "C" PGEXT_API void heap_deform_tuple(HeapTuple tuple, pgext::TupleDesc tupleDesc, Datum *values, bool *isnull);
