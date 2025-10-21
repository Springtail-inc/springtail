#pragma once

#include <pg_ext/export.hh>
#include <pg_ext/common.hh>

enum class vartag_external
{
	VARTAG_INDIRECT = 1,
	VARTAG_EXPANDED_RO = 2,
	VARTAG_EXPANDED_RW = 3,
	VARTAG_ONDISK = 18
};

union varattrib_4b
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32_t		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
	struct						/* Compressed-in-line format */
	{
		uint32_t		va_header;
		uint32_t		va_tcinfo;	/* Original data size (excludes header) and
								 * compression method; see va_extinfo */
		char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
	}			va_compressed;
};

struct varattrib_1b
{
	uint8_t		va_header;
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
};

struct varattrib_1b_e
{
	uint8_t		va_header;		/* Always 0x80 or 0x01 */
	uint8_t		va_tag;			/* Type of datum */
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
};

struct varatt_indirect
{
	struct varlena *pointer;	/* Pointer to in-memory varlena */
};

struct varatt_external
{
    int32_t	va_rawsize;		/* Original data size (includes header) */
    uint32_t	va_extinfo;		/* External saved size (without header) and
									 * compression method */
    Oid			va_valueid;		/* Unique ID of value within TOAST table */
    Oid			va_toastrelid;	/* RelID of TOAST table containing it */
};

struct ExpandedObjectHeader
{
	// /* Pointer to methods required for object type */
	// const ExpandedObjectMethods *eoh_methods;
	// pgext::MemoryContext eoh_context;
	// /* Standard R/W TOAST pointer for this object is kept here */
	// char		eoh_rw_ptr[EXPANDED_POINTER_SIZE];

	// /* Standard R/O TOAST pointer for this object is kept here */
	// char		eoh_ro_ptr[EXPANDED_POINTER_SIZE];
};

#define EXPANDED_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(varatt_expanded))


struct varatt_expanded
{
	ExpandedObjectHeader *eohptr;
};


#define VARTAG_IS_EXPANDED(tag) \
	((static_cast<int>(tag) & ~1) == static_cast<int>(vartag_external::VARTAG_EXPANDED_RO))
#define VARTAG_SIZE(tag) \
	((tag) == vartag_external::VARTAG_INDIRECT ? sizeof(varatt_indirect) : \
	 VARTAG_IS_EXPANDED(tag) ? sizeof(varatt_expanded) : \
	 (tag) == vartag_external::VARTAG_ONDISK ? sizeof(varatt_external) : \
	 (AssertMacro(false), 0))

#define VARHDRSZ_EXTERNAL		offsetof(varattrib_1b_e, va_data)
#define VARHDRSZ_COMPRESSED		offsetof(varattrib_4b, va_compressed.va_data)
#define VARHDRSZ_SHORT			offsetof(varattrib_1b, va_data)

#define VARHDRSZ ((int32_t)sizeof(int32_t))
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x80)
#define VARTAG_1B_E(PTR) \
    (((varattrib_1b_e *) (PTR))->va_tag)
#define VARTAG_EXTERNAL(PTR) static_cast<vartag_external>(VARTAG_1B_E(PTR))
#define VARSIZE_EXTERNAL(PTR) (VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))
#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARATT_IS_1B(PTR) \
    ((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define VARSIZE_ANY(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
	  VARSIZE_4B(PTR)))

#define SET_VARSIZE_4B(PTR, len) (((varattrib_4b *)(PTR))->va_4byte.va_header = (len) & 0x3FFFFFFF)
#define SET_VARSIZE(PTR, len) SET_VARSIZE_4B(PTR, len)

#define VARDATA_4B(PTR) (((varattrib_4b *)(PTR))->va_4byte.va_data)
#define VARDATA(PTR) VARDATA_4B(PTR)
