#pragma once

#include <pg_ext/export.hh>
#include <pg_ext/memory.hh>

#include <cstdint>
#include <unordered_map>

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

#define OidFunctionCall3(functionId, arg1, arg2, arg3) \
	OidFunctionCall3Coll(functionId, InvalidOid, arg1, arg2, arg3)

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
// #define VARHDRSZ ((uint32_t) sizeof(uint32_t))
#define VARHDRSZ_SHORT (offsetof(varattrib_1b, va_data))

#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR)-VARHDRSZ_SHORT : \
	  VARSIZE_4B(PTR)-VARHDRSZ)
#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32_t) (len)) << 2))
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
    { 16, { 1242, false } }, // BOOLOID → boolout
    { 17, { 140, false } },  // BYTEAOID → byteaout
    { 18, { 114, false } },  // CHAROID → charout
    { 19, { 30, false } },   // NAMEOID → nameout
    { 20, { 23, false } },   // INT8OID → int8out
    { 21, { 1082, false } }, // INT2OID → int2out
    { 22, { 1700, false } }, // BPCHAROID → bpcharout
    { 23, { 1001, false } }, // INT4OID → int4out
    { 24, { 25, false } },   // OIDOID → oidout
    { 25, { 1002, true } },  // TEXTOID → textout
    { 26, { 295, false } },  // VARCHAROID → varcharout
    { 600, { 1263, false } }, // POINTOID → pointout
    { 700, { 1004, false } }, // FLOAT4OID → float4out
    { 701, { 1184, false } }, // FLOAT8OID → float8out
    { 1042, { 1007, false } }, // BPCHAROID → bpcharout
    { 1043, { 1003, true } }, // VARCHAROID → varcharout
    { 1082, { 1005, false } }, // DATEOID → dateout
    { 1083, { 1006, false } }, // TIMEOID → timeout
    { 1114, { 1184, false } }, // TIMESTAMPOID → timestamptzout
    { 1184, { 1185, false } }, // TIMESTAMPTZOID → timestamptzout
    { 1266, { 1270, false } }, // TIMETZOID → timetzout
    { 1270, { 1271, false } }, // ABSTIMEOID → abstimeout
    { 1700, { 1701, false } }, // CIRCLEOID → circleout
    { 1790, { 1791, false } }, // MACADDROID → macaddrout
    { 2202, { 2203, false } }, // BITOID → bitout
    { 2203, { 2204, false } }, // VARBITOID → varbitout
    { 2249, { 2250, false } }, // UUIDOID → uuidout
    { 2950, { 2951, false } }, // XMLEXTERNALOID → xmlexternalout
    { 2951, { 2952, false } }, // XMLOID → xmlout
    { 3802, { 3803, false } }, // PG_LSNOID → pg_lsnout
    { 1033, { 1034, false } }, // JSONOID → jsonout
    { 2952, { 2953, false } }, // XMLARRAYOID → xmlarrayout
    { 1999, { 2000, false } }, // CIDROID → cidrout
    { 2000, { 2001, false } }, // INETOID → inetout
    { 2070, { 2071, false } }, // MACADDR8OID → macaddr8out
    { 2281, { 2282, false } }, // INTERVALOID → intervalout
};
// XXX Add support for User defined types

namespace pgext {
    struct NullableDatum {
        Datum value;
        bool  isnull;
        // due to alignment padding this could be used for flags for free
    };

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
} // namespace pgext

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
	pgext::NullableDatum args[FLEXIBLE_ARRAY_MEMBER];
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
	 sizeof(pgext::NullableDatum) * (nargs))

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
extern "C" PGEXT_API Datum DirectFunctionCall1(PGFunction func, Datum arg1);
extern "C" PGEXT_API Datum DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1);
extern "C" PGEXT_API Datum DirectFunctionCall2(PGFunction func, Datum arg1, Datum arg2);
extern "C" PGEXT_API Datum DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2);
extern "C" PGEXT_API Datum DirectFunctionCall3(PGFunction func, Datum arg1, Datum arg2, Datum arg3);
extern "C" PGEXT_API Datum DirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3);
extern "C" PGEXT_API Datum get_fn_opclass_options(FmgrInfo *fcinfo);
extern "C" PGEXT_API bool has_fn_opclass_options(FmgrInfo *fcinfo);
extern "C" PGEXT_API PGFunction lookup_pgfunction_by_oid(Oid oid);
extern "C" PGEXT_API const char* OidOutputFunctionCall(Oid function_oid, Datum value);
extern "C" PGEXT_API void getTypeOutputInfo(Oid type, Oid *funcOid, bool *typIsVarlena);
extern "C" PGEXT_API struct varlena *pg_detoast_datum(struct varlena *datum);
extern "C" PGEXT_API struct varlena *pg_detoast_datum_packed(struct varlena *datum);

extern "C" PGEXT_API Datum OidFunctionCall3Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3);

// Standard PostgreSQL type OIDs (from postgres.h)
#define BOOLOID 16
#define BYTEAOID 17
#define CHAROID 18
#define NAMEOID 19
#define INT8OID 20
#define INT2OID 21
#define INT2VECTOROID 22
#define INT4OID 23
#define REGPROCOID 24
#define TEXTOID 25
#define OIDOID 26
#define TIDOID 27
#define XIDOID 28
#define CIDOID 29
#define JSONOID 114
#define XMLOID 142
#define XID8OID 5069
#define POINTOID 600
#define LSEGOID 601
#define PATHOID 602
#define BOXOID 603
#define POLYGONOID 604
#define FLOAT4OID 700    // float4 (single precision)
#define FLOAT8OID 701    // float8 (double precision)
#define UNKNOWNOID 705
#define CIRCLEOID 718
#define CASHOID 790
#define MACADDROID 829
#define INETOID 869
#define CIDROID 650
#define INT4ARRAYOID 1007
#define TEXTARRAYOID 1009
#define FLOAT4ARRAYOID 1021
#define FLOAT8ARRAYOID 1022
#define BPCHAROID 1042   // blank-padded char, char(N)
#define VARCHAROID 1043  // varchar(N)
#define DATEOID 1082
#define TIMEOID 1083
#define TIMESTAMPOID 1114
#define TIMESTAMPTZOID 1184
#define INTERVALOID 1186
#define TIMETZOID 1266
#define BITOID 1560
#define VARBITOID 1562
#define NUMERICOID 1700  // numeric/decimal
#define REFCURSOROID 1790
#define REGPROCEDUREOID 2202
#define REGOPEROID 2203
#define REGOPERATOROID 2204
#define REGCLASSOID 2205
#define REGTYPEOID 2206
#define REGROLEOID 4096
#define REGNAMESPACEOID 4089
#define UUIDOID 2950
#define JSONBOID 3802
#define INT4RANGEOID 3904
#define NUMRANGEOID 3906
#define TSRANGEOID 3908
#define TSTZRANGEOID 3910
#define DATERANGEOID 3912
#define INT8RANGEOID 3926
#define JSONPATHOID 4072
