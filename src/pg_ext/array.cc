#include <pg_ext/array.hh>
#include <pg_ext/fmgr.hh>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <pg_repl/pg_types.hh>
#include <common/logging.hh>

bool
pg_add_s32_overflow(int32_t a, int32_t b, int32_t *result)
{
    int64_t		res = (int64_t) a + (int64_t) b;

	if (res > INT32_MAX || res < INT32_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int32_t) res;
	return false;
}

bool
ArrayCheckBoundsSafe(int ndim, const int *dims, const int *lb, struct Node *escontext)
{
    int i;

    for (i = 0; i < ndim; i++) {
        int32_t sum;

        if (pg_add_s32_overflow(dims[i], lb[i], &sum)) {
            LOG_ERROR("array lower bound is too large: %d", lb[i]);
        }
    }

    return true;
}


void
store_att_byval(void *T, Datum newdatum, int attlen)
{
	switch (attlen)
	{
		case sizeof(char):
			*(char *) T = pgext::DatumGetChar(newdatum);
			break;
		case sizeof(int16_t):
			*(int16_t *) T = pgext::DatumGetInt16(newdatum);
			break;
		case sizeof(int32_t):
			*(int32_t *) T = pgext::DatumGetInt32(newdatum);
			break;
		default:
			LOG_ERROR("unsupported byval length: %d", attlen);
	}
}

void
ArrayCheckBounds(int ndim, const int *dims, const int *lb)
{
    (void)ArrayCheckBoundsSafe(ndim, dims, lb, NULL);
}

int
ArrayCastAndSet(Datum src,
				int typlen,
				bool typbyval,
				char typalign,
				char *dest)
{
	int			inc;

	if (typlen > 0)
	{
		if (typbyval)
			store_att_byval(dest, src, typlen);
		else
			memmove(dest, pgext::DatumGetPointer(src), typlen);
		inc = att_align_nominal(typlen, typalign);
	}
	else
	{
		assert(!typbyval);
		inc = att_addlength_datum(0, typlen, src);
		memmove(dest, pgext::DatumGetPointer(src), inc);
		inc = att_align_nominal(inc, typalign);
	}

	return inc;
}

void
CopyArrayEls(ArrayType *array,
			 Datum *values,
			 bool *nulls,
			 int nitems,
			 int typlen,
			 bool typbyval,
			 char typalign,
			 bool freedata)
{
	char	   *p = ARR_DATA_PTR(array);
	uint8_t	   *bitmap = ARR_NULLBITMAP(array);
	int			bitval = 0;
	int			bitmask = 1;
	int			i;

	if (typbyval)
		freedata = false;

	for (i = 0; i < nitems; i++)
	{
		if (nulls && nulls[i])
		{
			if (!bitmap)		/* shouldn't happen */
				LOG_ERROR("null array element where not supported");
			/* bitmap bit stays 0 */
		}
		else
		{
			bitval |= bitmask;
			p += ArrayCastAndSet(values[i], typlen, typbyval, typalign, p);
			if (freedata)
				pfree(pgext::DatumGetPointer(values[i]));
		}
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				*bitmap++ = bitval;
				bitval = 0;
				bitmask = 1;
			}
		}
	}

	if (bitmap && bitmask != 1)
		*bitmap = bitval;
}

ArrayType *
construct_md_array(Datum *elems,
                   bool *nulls,
                   int ndims,
                   int *dims,
                   int *lbs,
                   Oid elmtype,
                   int elmlen,
                   bool elmbyval,
                   char elmalign)
{
    ArrayType *result;
    bool hasnulls;
    int32_t nbytes;
    int32_t dataoffset;
    int i;
    int nelems;

    if (ndims < 0) { /* we do allow zero-dimension arrays */
        LOG_ERROR("invalid number of dimensions: %d", ndims);
    }
    if (ndims > MAXDIM) {
        LOG_ERROR("number of array dimensions (%d) exceeds the maximum allowed (%d)",
                               ndims, MAXDIM);
    }

    /* This checks for overflow of the array dimensions */
    nelems = ArrayGetNItems(ndims, dims);
    ArrayCheckBounds(ndims, dims, lbs);

    /* if ndims <= 0 or any dims[i] == 0, return empty array */
    if (nelems <= 0) {
        return construct_empty_array(elmtype);
    }

    /* compute required space */
    nbytes = 0;
    hasnulls = false;
    for (i = 0; i < nelems; i++) {
        if (nulls && nulls[i]) {
            hasnulls = true;
            continue;
        }
        /* make sure data is not toasted */
        if (elmlen == -1) {
            elems[i] = pgext::PointerGetDatum(PG_DETOAST_DATUM(elems[i]));
        }
        nbytes = att_addlength_datum(nbytes, elmlen, elems[i]);
        nbytes = att_align_nominal(nbytes, elmalign);
        /* check for overflow of total request */
        if (!AllocSizeIsValid(nbytes)) {
            LOG_ERROR("array size exceeds the maximum allowed (%d)", (int)MaxAllocSize);
        }
    }

    /* Allocate and initialize result array */
    if (hasnulls) {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nelems);
        nbytes += dataoffset;
    } else {
        dataoffset = 0; /* marker for no null bitmap */
        nbytes += ARR_OVERHEAD_NONULLS(ndims);
    }
    result = (ArrayType *)palloc0(nbytes);
    SET_VARSIZE(result, nbytes);
    result->ndim = ndims;
    result->dataoffset = dataoffset;
    result->elemtype = elmtype;
    memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
    memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));

    CopyArrayEls(result, elems, nulls, nelems, elmlen, elmbyval, elmalign, false);

    return result;
}

ArrayType *
construct_array(Datum *elems, int nelems, Oid elmtype, int elmlen, bool elmbyval, char elmalign)
{
    int dims[1];
    int lbs[1];

    dims[0] = nelems;
    lbs[0] = 1;

    return construct_md_array(elems, NULL, 1, dims, lbs, elmtype, elmlen, elmbyval, elmalign);
}

ArrayType *
construct_array_builtin(Datum *elems, int nelems, Oid elmtype)
{
    int elmlen;
    bool elmbyval;
    char elmalign;

    switch (elmtype) {
        case CHAROID:
            elmlen = 1;
            elmbyval = true;
            elmalign = TYPALIGN_CHAR;
            break;

        case CSTRINGOID:
            elmlen = -2;
            elmbyval = false;
            elmalign = TYPALIGN_CHAR;
            break;

        case FLOAT4OID:
            elmlen = sizeof(float);
            elmbyval = true;
            elmalign = TYPALIGN_INT;
            break;

        case INT2OID:
            elmlen = sizeof(int16_t);
            elmbyval = true;
            elmalign = TYPALIGN_SHORT;
            break;

        case INT4OID:
            elmlen = sizeof(int32_t);
            elmbyval = true;
            elmalign = TYPALIGN_INT;
            break;

        case INT8OID:
            elmlen = sizeof(int64_t);
            elmbyval = true;
            elmalign = TYPALIGN_DOUBLE;
            break;

        case NAMEOID:
            elmlen = NAMEDATALEN;
            elmbyval = false;
            elmalign = TYPALIGN_CHAR;
            break;

        case OIDOID:
        case REGTYPEOID:
            elmlen = sizeof(Oid);
            elmbyval = true;
            elmalign = TYPALIGN_INT;
            break;

        case TEXTOID:
            elmlen = -1;
            elmbyval = false;
            elmalign = TYPALIGN_INT;
            break;

        case TIDOID:
            elmlen = sizeof(ItemPointerData);
            elmbyval = false;
            elmalign = TYPALIGN_SHORT;
            break;

        default:
            LOG_ERROR("type %u not supported by construct_array_builtin()", elmtype);
            /* keep compiler quiet */
            elmlen = 0;
            elmbyval = false;
            elmalign = 0;
    }

    return construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);
}

ArrayType *
construct_empty_array(Oid elmtype)
{
	ArrayType  *result;

	result = (ArrayType *) palloc0(sizeof(ArrayType));
	SET_VARSIZE(result, sizeof(ArrayType));
	result->ndim = 0;
	result->dataoffset = 0;
	result->elemtype = elmtype;
	return result;
}

static inline Datum
fetch_att(const void *T, bool attbyval, int attlen)
{
	if (attbyval)
	{
		switch (attlen)
		{
			case sizeof(char):
				return pgext::CharGetDatum(*((const char *) T));
			case sizeof(int16_t):
				return pgext::Int16GetDatum(*((const int16_t *) T));
			case sizeof(int32_t):
				return pgext::Int32GetDatum(*((const int32_t *) T));
			default:
				LOG_ERROR("unsupported byval length: %d", attlen);
				return 0;
		}
	}
	else
		return pgext::PointerGetDatum(T);
}

void
deconstruct_array(ArrayType *array,
				  Oid elmtype,
				  int elmlen, bool elmbyval, char elmalign,
				  Datum **elemsp, bool **nullsp, int *nelemsp)
{
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	char	   *p;
	uint8_t	   *bitmap;
	int			bitmask;
	int			i;

	assert(ARR_ELEMTYPE(array) == elmtype);

	nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
	*elemsp = elems = (Datum *) palloc(nelems * sizeof(Datum));
	if (nullsp)
		*nullsp = nulls = (bool *) palloc0(nelems * sizeof(bool));
	else
		nulls = NULL;
	*nelemsp = nelems;

	p = ARR_DATA_PTR(array);
	bitmap = ARR_NULLBITMAP(array);
	bitmask = 1;

	for (i = 0; i < nelems; i++)
	{
		/* Get source element, checking for NULL */
		if (bitmap && (*bitmap & bitmask) == 0)
		{
			elems[i] = (Datum) 0;
			if (nulls)
				nulls[i] = true;
			else
				LOG_ERROR("null array element not allowed in this context");
		}
		else
		{
			elems[i] = fetch_att(p, elmbyval, elmlen);
			p = att_addlength_pointer(p, elmlen, p);
			p = (char *) att_align_nominal(p, elmalign);
		}

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}
}

void
deconstruct_array_builtin(ArrayType *array, Oid elmtype, Datum **elemsp, bool **nullsp, int *nelemsp)
{
    int elmlen;
    bool elmbyval;
    char elmalign;

    switch (elmtype) {
        case CHAROID:
            elmlen = 1;
            elmbyval = true;
            elmalign = TYPALIGN_CHAR;
            break;

        case CSTRINGOID:
            elmlen = -2;
            elmbyval = false;
            elmalign = TYPALIGN_CHAR;
            break;

        case FLOAT8OID:
            elmlen = sizeof(double);
            elmbyval = true;
            elmalign = TYPALIGN_DOUBLE;
            break;

        case INT2OID:
            elmlen = sizeof(int16_t);
            elmbyval = true;
            elmalign = TYPALIGN_SHORT;
            break;

        case OIDOID:
            elmlen = sizeof(Oid);
            elmbyval = true;
            elmalign = TYPALIGN_INT;
            break;

        case TEXTOID:
            elmlen = -1;
            elmbyval = false;
            elmalign = TYPALIGN_INT;
            break;

        case TIDOID:
            elmlen = sizeof(ItemPointerData);
            elmbyval = false;
            elmalign = TYPALIGN_SHORT;
            break;

        default:
            LOG_ERROR("type %u not supported by deconstruct_array_builtin()", elmtype);
            /* keep compiler quiet */
            elmlen = 0;
            elmbyval = false;
            elmalign = 0;
    }

    deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, elemsp, nullsp, nelemsp);
}

int ArrayGetNItems(int ndim, const int *dims) {
    int nitems = 1;
    for (int i = 0; i < ndim; i++) {
        nitems *= dims[i];
    }
    return nitems;
}

bool
array_contains_nulls(ArrayType *array)
{
    int nelems;
    uint8_t *bitmap;
    int bitmask;

    /* Easy answer if there's no null bitmap */
    if (!ARR_HASNULL(array)) {
        return false;
    }

    nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

    bitmap = ARR_NULLBITMAP(array);

    /* check whole bytes of the bitmap byte-at-a-time */
    while (nelems >= 8) {
        if (*bitmap != 0xFF) {
            return true;
        }
        bitmap++;
        nelems -= 8;
    }

    /* check last partial byte */
    bitmask = 1;
    while (nelems > 0) {
        if ((*bitmap & bitmask) == 0) {
            return true;
        }
        bitmask <<= 1;
        nelems--;
    }

    return false;
}
