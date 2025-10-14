#pragma once

#include <pg_ext/export.hh>
#include <pg_ext/common.hh>
#include <pg_ext/heaptuple.hh>
#include <pg_ext/domains.hh>
#include <pg_ext/list.hh>

// Forward declare FmgrInfo
typedef struct FmgrInfo FmgrInfo;

constexpr uint32_t TYPECACHE_DOMAIN_BASE_INFO = 0x01000;

struct NameData
{
	char data[NAMEDATALEN];
};

// XXX Defaults skipped
struct Form_pg_type {
	Oid			oid;			/* oid */

	/* type name */
	NameData	typname;

	/* OID of namespace containing this type */
	Oid			typnamespace;

	/* type owner */
	Oid			typowner;

	/*
	 * For a fixed-size type, typlen is the number of bytes we use to
	 * represent a value of this type, e.g. 4 for an int4.  But for a
	 * variable-length type, typlen is negative.  We use -1 to indicate a
	 * "varlena" type (one that has a length word), -2 to indicate a
	 * null-terminated C string.
	 */
	int16_t		typlen;

	/*
	 * typbyval determines whether internal Postgres routines pass a value of
	 * this type by value or by reference.  typbyval had better be false if
	 * the length is not 1, 2, or 4 (or 8 on 8-byte-Datum machines).
	 * Variable-length types are always passed by reference. Note that
	 * typbyval can be false even if the length would allow pass-by-value; for
	 * example, type macaddr8 is pass-by-ref even when Datum is 8 bytes.
	 */
	bool		typbyval;

	/*
	 * typtype is 'b' for a base type, 'c' for a composite type (e.g., a
	 * table's rowtype), 'd' for a domain, 'e' for an enum type, 'p' for a
	 * pseudo-type, or 'r' for a range type. (Use the TYPTYPE macros below.)
	 *
	 * If typtype is 'c', typrelid is the OID of the class' entry in pg_class.
	 */
	char		typtype;

	/*
	 * typcategory and typispreferred help the parser distinguish preferred
	 * and non-preferred coercions.  The category can be any single ASCII
	 * character (but not \0).  The categories used for built-in types are
	 * identified by the TYPCATEGORY macros below.
	 */

	/* arbitrary type classification */
	char		typcategory;

	/* is type "preferred" within its category? */
	bool		typispreferred;

	/*
	 * If typisdefined is false, the entry is only a placeholder (forward
	 * reference).  We know the type's name and owner, but not yet anything
	 * else about it.
	 */
	bool		typisdefined;

	/* delimiter for arrays of this type */
	char		typdelim;

	/* associated pg_class OID if a composite type, else 0 */
	Oid			typrelid;

	/*
	 * Type-specific subscripting handler.  If typsubscript is 0, it means
	 * that this type doesn't support subscripting.  Note that various parts
	 * of the system deem types to be "true" array types only if their
	 * typsubscript is array_subscript_handler.
	 */
	regproc		typsubscript;

	/*
	 * If typelem is not 0 then it identifies another row in pg_type, defining
	 * the type yielded by subscripting.  This should be 0 if typsubscript is
	 * 0.  However, it can be 0 when typsubscript isn't 0, if the handler
	 * doesn't need typelem to determine the subscripting result type.  Note
	 * that a typelem dependency is considered to imply physical containment
	 * of the element type in this type; so DDL changes on the element type
	 * might be restricted by the presence of this type.
	 */
	Oid			typelem;

	/*
	 * If there is a "true" array type having this type as element type,
	 * typarray links to it.  Zero if no associated "true" array type.
	 */
	Oid			typarray;

	/*
	 * I/O conversion procedures for the datatype.
	 */

	/* text format (required) */
	regproc		typinput;
	regproc		typoutput;

	/* binary format (optional) */
	regproc		typreceive;
	regproc		typsend;

	/*
	 * I/O functions for optional type modifiers.
	 */
	regproc		typmodin;
	regproc		typmodout;

	/*
	 * Custom ANALYZE procedure for the datatype (0 selects the default).
	 */
	regproc		typanalyze;

	/* ----------------
	 * typalign is the alignment required when storing a value of this
	 * type.  It applies to storage on disk as well as most
	 * representations of the value inside Postgres.  When multiple values
	 * are stored consecutively, such as in the representation of a
	 * complete row on disk, padding is inserted before a datum of this
	 * type so that it begins on the specified boundary.  The alignment
	 * reference is the beginning of the first datum in the sequence.
	 *
	 * 'c' = CHAR alignment, ie no alignment needed.
	 * 's' = SHORT alignment (2 bytes on most machines).
	 * 'i' = INT alignment (4 bytes on most machines).
	 * 'd' = DOUBLE alignment (8 bytes on many machines, but by no means all).
	 * (Use the TYPALIGN macros below for these.)
	 *
	 * See include/access/tupmacs.h for the macros that compute these
	 * alignment requirements.  Note also that we allow the nominal alignment
	 * to be violated when storing "packed" varlenas; the TOAST mechanism
	 * takes care of hiding that from most code.
	 *
	 * NOTE: for types used in system tables, it is critical that the
	 * size and alignment defined in pg_type agree with the way that the
	 * compiler will lay out the field in a struct representing a table row.
	 * ----------------
	 */
	char		typalign;

	/* ----------------
	 * typstorage tells if the type is prepared for toasting and what
	 * the default strategy for attributes of this type should be.
	 *
	 * 'p' PLAIN	  type not prepared for toasting
	 * 'e' EXTERNAL   external storage possible, don't try to compress
	 * 'x' EXTENDED   try to compress and store external if required
	 * 'm' MAIN		  like 'x' but try to keep in main tuple
	 * (Use the TYPSTORAGE macros below for these.)
	 *
	 * Note that 'm' fields can also be moved out to secondary storage,
	 * but only as a last resort ('e' and 'x' fields are moved first).
	 * ----------------
	 */
	char		typstorage;

	/*
	 * This flag represents a "NOT NULL" constraint against this datatype.
	 *
	 * If true, the attnotnull column for a corresponding table column using
	 * this datatype will always enforce the NOT NULL constraint.
	 *
	 * Used primarily for domain types.
	 */
	bool		typnotnull;

	/*
	 * Domains use typbasetype to show the base (or domain) type that the
	 * domain is based on.  Zero if the type is not a domain.
	 */
	Oid			typbasetype;

	/*
	 * Domains use typtypmod to record the typmod to be applied to their base
	 * type (-1 if base type does not use a typmod).  -1 if this type is not a
	 * domain.
	 */
	int32_t		typtypmod;

	/*
	 * typndims is the declared number of dimensions for an array domain type
	 * (i.e., typbasetype is an array type).  Otherwise zero.
	 */
	int32_t		typndims;

	/*
	 * Collation: 0 if type cannot use collations, nonzero (typically
	 * DEFAULT_COLLATION_OID) for collatable base types, possibly some other
	 * OID for domains over collatable types
	 */
	Oid			typcollation;

#ifdef CATALOG_VARLEN			/* variable-length fields start here */

	/*
	 * If typdefaultbin is not NULL, it is the nodeToString representation of
	 * a default expression for the type.  Currently this is only used for
	 * domains.
	 */
	pg_node_tree typdefaultbin;

	/*
	 * typdefault is NULL if the type has no associated default value. If
	 * typdefaultbin is not NULL, typdefault must contain a human-readable
	 * version of the default expression represented by typdefaultbin. If
	 * typdefaultbin is NULL and typdefault is not, then typdefault is the
	 * external representation of the type's default value, which may be fed
	 * to the type's input converter to produce a constant.
	 */
	text		typdefault;

	/*
	 * Access permissions
	 */
	aclitem		typacl[1];
#endif
};

struct DomainConstraintCache
{
	List	   *constraints;	/* list of DomainConstraintState nodes */
	MemoryContext dccContext;	/* memory context holding all associated data */
	long		dccRefCount;	/* number of references to this struct */
};

struct TypeCacheEntry
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
};

extern "C" PGEXT_API bool type_is_rowtype(Oid typid);
extern "C" PGEXT_API char get_typtype(Oid typid);
extern "C" PGEXT_API Oid getBaseType(Oid typid);
