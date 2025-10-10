#pragma once

#include <cstdint>

#include <pg_ext/export.hh>
#include <pg_ext/common.hh>

// Forward declare FmgrInfo
typedef struct FmgrInfo FmgrInfo;

/*
 * information stored in t_infomask:
 */
 #define HEAP_HASNULL			0x0001	/* has null attribute(s) */
 #define HEAP_HASVARWIDTH		0x0002	/* has variable-width attribute(s) */
 #define HEAP_HASEXTERNAL		0x0004	/* has external stored attribute(s) */
 #define HEAP_HASOID_OLD			0x0008	/* has an object-id field */
 #define HEAP_XMAX_KEYSHR_LOCK	0x0010	/* xmax is a key-shared locker */
 #define HEAP_COMBOCID			0x0020	/* t_cid is a combo CID */
 #define HEAP_XMAX_EXCL_LOCK		0x0040	/* xmax is exclusive locker */
 #define HEAP_XMAX_LOCK_ONLY		0x0080	/* xmax, if valid, is only a locker */

#define HeapTupleHeaderHasExternal(tup) \
		(((tup)->t_infomask & HEAP_HASEXTERNAL) != 0)

struct BlockIdData {
    uint16_t bi_hi;
    uint16_t bi_lo;
};

struct ItemPointerData {
    BlockIdData ip_blkid;
    uint16_t ip_posid;
};
typedef ItemPointerData *ItemPointer;

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

typedef struct AttInMetadata {
    TupleDesc tupdesc;
    FmgrInfo *attinfuncs;
    Oid *attioparams;
    int32_t *atttypmods;
} AttInMetadata;

typedef struct HeapTupleData *HeapTuple;

typedef struct HeapTupleFields
{
	TransactionId t_xmin;		/* inserting xact ID */
	TransactionId t_xmax;		/* deleting or locking xact ID */

	union
	{
		CommandId	t_cid;		/* inserting or deleting command ID, or both */
		TransactionId t_xvac;	/* old-style VACUUM FULL xact ID */
	}			t_field3;
} HeapTupleFields;

typedef struct DatumTupleFields
{
	int32_t		datum_len_;		/* varlena header (do not touch directly!) */

	int32_t		datum_typmod;	/* -1, or identifier of a record type */

	Oid			datum_typeid;	/* composite type OID, or RECORDOID */

	/*
	 * datum_typeid cannot be a domain over composite, only plain composite,
	 * even if the datum is meant as a value of a domain-over-composite type.
	 * This is in line with the general principle that CoerceToDomain does not
	 * change the physical representation of the base type value.
	 *
	 * Note: field ordering is chosen with thought that Oid might someday
	 * widen to 64 bits.
	 */
} DatumTupleFields;

struct HeapTupleHeaderData
{
	union
	{
		HeapTupleFields t_heap;
		DatumTupleFields t_datum;
	}			t_choice;

	ItemPointerData t_ctid;		/* current TID of this or newer tuple (or a
								 * speculative insertion token) */

	/* Fields below here must match MinimalTupleData! */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2 2
	uint16_t		t_infomask2;	/* number of attributes + various flags */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK 3
	uint16_t		t_infomask;		/* various flag bits, see below */

#define FIELDNO_HEAPTUPLEHEADERDATA_HOFF 4
	uint8_t		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

#define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
    uint8_t		t_bits[FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
};
typedef struct HeapTupleHeaderData *HeapTupleHeader;

struct HeapTupleData {
    uint32_t t_len;         /* length of *t_data */
    ItemPointerData t_self; /* SelfItemPointer */
    Oid t_tableOid;         /* table the tuple came from */
    HeapTupleHeader t_data; /* -> tuple header and data */
};

extern "C" PGEXT_API void DecrTupleDescRefCount(TupleDesc tupdesc);
extern "C" PGEXT_API TupleDesc BlessTupleDesc(TupleDesc tupdesc);
extern "C" PGEXT_API Datum HeapTupleHeaderGetDatum(HeapTupleHeader tuple);
extern "C" PGEXT_API HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull);
extern "C" PGEXT_API void heap_deform_tuple(HeapTuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull);
extern "C" PGEXT_API TupleDesc lookup_rowtype_tupdesc_domain(Oid type_id, int32_t typmod, bool noError);
