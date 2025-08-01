#pragma once

#include <cstdint>
#include <unordered_map>
#include <pg_ext/export.hh>
#include <pg_ext/memory.hh>

// VARATT START
#define FLEXIBLE_ARRAY_MEMBER

typedef union
{
	struct
	{
		uint32_t		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
} varattrib_4b;

typedef struct
{
	uint8_t		va_header;
	char		va_data[FLEXIBLE_ARRAY_MEMBER];
} varattrib_1b;

#define VARDATA_4B(PTR)		(((varattrib_4b *) (PTR))->va_4byte.va_data)
#define VARDATA_1B(PTR)		(((varattrib_1b *) (PTR))->va_data)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x80)

#define VARSIZE_4B(PTR) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	(((varattrib_1b *) (PTR))->va_header & 0x7F)

#define VARDATA_ANY(PTR) \
	 (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))

#define VARSIZE_ANY(PTR) \
	VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
	  VARSIZE_4B(PTR)
#define VARHDRSZ ((uint32_t) sizeof(uint32_t))
#define VARHDRSZ_SHORT (offsetof(varattrib_1b, va_data))

#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR)-VARHDRSZ_SHORT : \
	  VARSIZE_4B(PTR)-VARHDRSZ)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (((uint8_t) (len)) << 1) | 0x01)

// VARATT END


// Collation is often just a 4-byte OID in Postgres
typedef uint32_t Oid;
typedef uintptr_t Datum;
typedef struct FunctionCallInfoBaseData *FunctionCallInfo;
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

struct TypeInfo {
    Oid output_func;
    bool is_varlena;
};

static std::unordered_map<Oid, TypeInfo> _type_output_registry = {
    { 23, {1001, false} },   // INT4OID → int4out
    { 25, {1002, true} },    // TEXTOID → textout
    { 1043, {1003, true} },  // VARCHAROID → varcharout
};
// XXX Add support for User defined types

typedef struct NullableDatum
{
#define FIELDNO_NULLABLE_DATUM_DATUM 0
	Datum		value;
#define FIELDNO_NULLABLE_DATUM_ISNULL 1
	bool		isnull;
	/* due to alignment padding this could be used for flags for free */
} NullableDatum;

typedef struct Node *fmNodePtr;
typedef struct FmgrInfo
{
	PGFunction	fn_addr;		/* pointer to function or handler to be called */
	Oid			fn_oid;			/* OID of function (NOT of handler, if any) */
	short		fn_nargs;		/* number of input args (0..FUNC_MAX_ARGS) */
	bool		fn_strict;		/* function is "strict" (NULL in => NULL out) */
	bool		fn_retset;		/* function returns a set */
	unsigned char fn_stats;		/* collect stats if track_functions > this */
	void	   *fn_extra;		/* extra space for use by handler */
	pgext::MemoryContext fn_mcxt;		/* memory context to store fn_extra in */
	fmNodePtr	fn_expr;		/* expression parse tree for call, or NULL */
} FmgrInfo;

#define FLEXIBLE_ARRAY_MEMBER

typedef struct FunctionCallInfoBaseData
{
	FmgrInfo   *flinfo;			/* ptr to lookup info used for this call */
	fmNodePtr	context;		/* pass info about context of call */
	fmNodePtr	resultinfo;		/* pass or return extra info about result */
	Oid			fncollation;	/* collation for function to use */
#define FIELDNO_FUNCTIONCALLINFODATA_ISNULL 4
	bool		isnull;			/* function must set true if result is NULL */
	short		nargs;			/* # arguments actually passed */
#define FIELDNO_FUNCTIONCALLINFODATA_ARGS 6
	NullableDatum args[FLEXIBLE_ARRAY_MEMBER];
} FunctionCallInfoBaseData;

struct FunctionCallInfoData
{
    FmgrInfo      fn;
    void*         fn_extra;
    void*         context;
    Oid           fn_collation;
    bool*         argnull;
    Datum         args[2];
    int           nargs;
};

#define SizeForFunctionCallInfo(nargs) \
	(offsetof(FunctionCallInfoBaseData, args) + \
	 sizeof(NullableDatum) * (nargs))

#define LOCAL_FCINFO(name, nargs) \
	/* use union with FunctionCallInfoBaseData to guarantee alignment */ \
	union \
	{ \
		FunctionCallInfoBaseData fcinfo; \
		/* ensure enough space for nargs args is available */ \
		char fcinfo_data[SizeForFunctionCallInfo(nargs)]; \
	} name##data; \
	FunctionCallInfo name = &name##data.fcinfo

#define InitFunctionCallInfoData(Fcinfo, Flinfo, Nargs, Collation, Context, Resultinfo) \
	do { \
		(Fcinfo).flinfo = (Flinfo); \
		(Fcinfo).context = (Context); \
		(Fcinfo).resultinfo = (Resultinfo); \
		(Fcinfo).fncollation = (Collation); \
		(Fcinfo).isnull = false; \
		(Fcinfo).nargs = (Nargs); \
	} while (0)

#define FunctionCallInvoke(fcinfo)	((* (fcinfo)->flinfo->fn_addr) (fcinfo))

// Function interfaces
extern "C" PGEXT_API Datum DirectFunctionCall2(PGFunction func, Datum arg1, Datum arg2);
extern "C" PGEXT_API Datum DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2);
extern "C" PGEXT_API Datum get_fn_opclass_options(FmgrInfo *fcinfo);
extern "C" PGEXT_API bool has_fn_opclass_options(FmgrInfo *fcinfo);
extern "C" PGEXT_API PGFunction lookup_pgfunction_by_oid(Oid oid);
extern "C" PGEXT_API const char* OidOutputFunctionCall(Oid function_oid, Datum value);
extern "C" PGEXT_API void getTypeOutputInfo(Oid type, Oid *funcOid, bool *typIsVarlena);
extern "C" PGEXT_API struct varlena *pg_detoast_datum(struct varlena *datum);
extern "C" PGEXT_API struct varlena *pg_detoast_datum_packed(struct varlena *datum);
