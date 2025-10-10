#pragma once

#include <pg_ext/export.hh>
#include <pg_ext/common.hh>
#include <pg_ext/heaptuple.hh>
#include <pg_ext/domains.hh>
#include <pg_ext/list.hh>

// Forward declare FmgrInfo
typedef struct FmgrInfo FmgrInfo;

#define TYPTYPE_COMPOSITE 'c' /* composite (e.g., table's rowtype) */
#define TYPTYPE_DOMAIN 'd'    /* domain over another type */

#define TYPECACHE_DOMAIN_BASE_INFO			0x01000

struct DomainConstraintCache
{
	List	   *constraints;	/* list of DomainConstraintState nodes */
	MemoryContext dccContext;	/* memory context holding all associated data */
	long		dccRefCount;	/* number of references to this struct */
};

typedef struct TypeCacheEntry
{
	/* typeId is the hash lookup key and MUST BE FIRST */
	Oid			type_id;		/* OID of the data type */
	uint32_t	type_id_hash;	/* hashed value of the OID */

	/* some subsidiary information copied from the pg_type row */
	int16_t		typlen;
	bool		typbyval;
	char		typalign;
	char		typstorage;
	char		typtype;
	Oid			typrelid;
	Oid			typsubscript;
	Oid			typelem;
	Oid			typcollation;

	/*
	 * Information obtained from opfamily entries
	 *
	 * These will be InvalidOid if no match could be found, or if the
	 * information hasn't yet been requested.  Also note that for array and
	 * composite types, typcache.c checks that the contained types are
	 * comparable or hashable before allowing eq_opr etc to become set.
	 */
	Oid			btree_opf;		/* the default btree opclass' family */
	Oid			btree_opintype; /* the default btree opclass' opcintype */
	Oid			hash_opf;		/* the default hash opclass' family */
	Oid			hash_opintype;	/* the default hash opclass' opcintype */
	Oid			eq_opr;			/* the equality operator */
	Oid			lt_opr;			/* the less-than operator */
	Oid			gt_opr;			/* the greater-than operator */
	Oid			cmp_proc;		/* the btree comparison function */
	Oid			hash_proc;		/* the hash calculation function */
	Oid			hash_extended_proc; /* the extended hash calculation function */

	/*
	 * Pre-set-up fmgr call info for the equality operator, the btree
	 * comparison function, and the hash calculation function.  These are kept
	 * in the type cache to avoid problems with memory leaks in repeated calls
	 * to functions such as array_eq, array_cmp, hash_array.  There is not
	 * currently a need to maintain call info for the lt_opr or gt_opr.
	 */
	FmgrInfo	eq_opr_finfo;
	FmgrInfo	cmp_proc_finfo;
	FmgrInfo	hash_proc_finfo;
	FmgrInfo	hash_extended_proc_finfo;

	/*
	 * Tuple descriptor if it's a composite type (row type).  NULL if not
	 * composite or information hasn't yet been requested.  (NOTE: this is a
	 * reference-counted tupledesc.)
	 *
	 * To simplify caching dependent info, tupDesc_identifier is an identifier
	 * for this tupledesc that is unique for the life of the process, and
	 * changes anytime the tupledesc does.  Zero if not yet determined.
	 */
	TupleDesc	tupDesc;
	uint64_t		tupDesc_identifier;

	/*
	 * Fields computed when TYPECACHE_RANGE_INFO is requested.  Zeroes if not
	 * a range type or information hasn't yet been requested.  Note that
	 * rng_cmp_proc_finfo could be different from the element type's default
	 * btree comparison function.
	 */
	struct TypeCacheEntry *rngelemtype; /* range's element type */
	Oid			rng_collation;	/* collation for comparisons, if any */
	FmgrInfo	rng_cmp_proc_finfo; /* comparison function */
	FmgrInfo	rng_canonical_finfo;	/* canonicalization function, if any */
	FmgrInfo	rng_subdiff_finfo;	/* difference function, if any */

	/*
	 * Fields computed when TYPECACHE_MULTIRANGE_INFO is required.
	 */
	struct TypeCacheEntry *rngtype; /* multirange's range underlying type */

	/*
	 * Domain's base type and typmod if it's a domain type.  Zeroes if not
	 * domain, or if information hasn't been requested.
	 */
	Oid			domainBaseType;
	int32_t		domainBaseTypmod;

	/*
	 * Domain constraint data if it's a domain type.  NULL if not domain, or
	 * if domain has no constraints, or if information hasn't been requested.
	 */
	DomainConstraintCache *domainData;

	/* Private data, for internal use of typcache.c only */
	int			flags;			/* flags about what we've computed */

	/*
	 * Private information about an enum type.  NULL if not enum or
	 * information hasn't been requested.
	 */
	struct TypeCacheEnumData *enumData;

	/* We also maintain a list of all known domain-type cache entries */
	struct TypeCacheEntry *nextDomain;
} TypeCacheEntry;

extern "C" PGEXT_API bool type_is_rowtype(Oid typid);
extern "C" PGEXT_API char get_typtype(Oid typid);
extern "C" PGEXT_API Oid getBaseType(Oid typid);
