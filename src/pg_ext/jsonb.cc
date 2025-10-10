#include <pg_ext/jsonb.hh>
#include <pg_ext/string.hh>

static int
lengthCompareJsonbString(const char *val1, int len1, const char *val2, int len2)
{
	if (len1 == len2)
		return memcmp(val1, val2, len1);
	else
		return len1 > len2 ? 1 : -1;
}

static int
lengthCompareJsonbStringValue(const void *a, const void *b)
{
	const JsonbValue *va = (const JsonbValue *) a;
	const JsonbValue *vb = (const JsonbValue *) b;

	assert(va->type == jbvString);
	assert(vb->type == jbvString);

	return lengthCompareJsonbString(va->val.string.val, va->val.string.len,
									vb->val.string.val, vb->val.string.len);
}

static int
lengthCompareJsonbPair(const void *a, const void *b, void *binequal)
{
	const JsonbPair *pa = (const JsonbPair *) a;
	const JsonbPair *pb = (const JsonbPair *) b;
	int			res;

	res = lengthCompareJsonbStringValue(&pa->key, &pb->key);
	if (res == 0 && binequal)
		*((bool *) binequal) = true;

	/*
	 * Guarantee keeping order of equal pair.  Unique algorithm will prefer
	 * first element as value.
	 */
	if (res == 0)
		res = (pa->order > pb->order) ? -1 : 1;

	return res;
}

void
uniqueifyJsonbObject(JsonbValue *object, bool unique_keys, bool skip_nulls)
{
	bool		hasNonUniq = false;

	assert(object->type == jbvObject);

	if (object->val.object.nPairs > 1)
		qsort_r(object->val.object.pairs, object->val.object.nPairs, sizeof(JsonbPair),
				  lengthCompareJsonbPair, &hasNonUniq);

	if (hasNonUniq && unique_keys)
		LOG_ERROR("duplicate JSON object key value");

	if (hasNonUniq || skip_nulls)
	{
		JsonbPair  *ptr,
				   *res;

		while (skip_nulls && object->val.object.nPairs > 0 &&
			   object->val.object.pairs->value->type == jbvNull)
		{
			/* If skip_nulls is true, remove leading items with null */
			object->val.object.pairs++;
			object->val.object.nPairs--;
		}

		if (object->val.object.nPairs > 0)
		{
			ptr = object->val.object.pairs + 1;
			res = object->val.object.pairs;

			while (ptr - object->val.object.pairs < object->val.object.nPairs)
			{
				/* Avoid copying over duplicate or null */
				if (lengthCompareJsonbStringValue(ptr, res) != 0 &&
					(!skip_nulls || ptr->value->type != jbvNull))
				{
					res++;
					if (ptr != res)
						memcpy(res, ptr, sizeof(JsonbPair));
				}
				ptr++;
			}

			object->val.object.nPairs = res + 1 - object->val.object.pairs;
		}
	}
}

static void
appendKey(JsonbParseState *pstate, JsonbValue *string)
{
	JsonbValue *object = &pstate->contVal;

	assert(object->type == jbvObject);
	assert(string->type == jbvString);

	if (object->val.object.nPairs >= JSONB_MAX_PAIRS)
		LOG_ERROR("number of jsonb object pairs exceeds the maximum allowed (%zu)", JSONB_MAX_PAIRS);

	if (object->val.object.nPairs >= pstate->size)
	{
		pstate->size *= 2;
		object->val.object.pairs = (JsonbPair *) repalloc(object->val.object.pairs,
													sizeof(JsonbPair) * pstate->size);
	}

	object->val.object.pairs[object->val.object.nPairs].key = string;
	object->val.object.pairs[object->val.object.nPairs].order = object->val.object.nPairs;
}

/*
 * pushJsonbValue() worker:  Append a pair value to state when generating a
 * Jsonb
 */
static void
appendValue(JsonbParseState *pstate, JsonbValue *scalarVal)
{
	JsonbValue *object = &pstate->contVal;

	assert(object->type == jbvObject);

	object->val.object.pairs[object->val.object.nPairs++].value = scalarVal;
}

/*
 * pushJsonbValue() worker:  Append an element to state when generating a Jsonb
 */
static void
appendElement(JsonbParseState *pstate, JsonbValue *scalarVal)
{
	JsonbValue *array = &pstate->contVal;

	assert(array->type == jbvArray);

	if (array->val.array.nElems >= JSONB_MAX_ELEMS)
		LOG_ERROR("number of jsonb array elements exceeds the maximum allowed (%zu)", JSONB_MAX_ELEMS);

	if (array->val.array.nElems >= pstate->size)
	{
		pstate->size *= 2;
		array->val.array.elems = (JsonbValue* )repalloc(array->val.array.elems,
										  sizeof(JsonbValue) * pstate->size);
	}

	array->val.array.elems[array->val.array.nElems++] = *scalarVal;
}


static JsonbParseState *
pushState(JsonbParseState **pstate)
{
	JsonbParseState *ns = (JsonbParseState *) palloc(sizeof(JsonbParseState));

	ns->next = *pstate;
	ns->unique_keys = false;
	ns->skip_nulls = false;

	return ns;
}

static JsonbValue *
pushJsonbValueScalar(JsonbParseState **pstate, JsonbIteratorToken seq,
					 JsonbValue *scalarVal)
{
	JsonbValue *result = nullptr;

	switch (seq)
	{
		case WJB_BEGIN_ARRAY:
			assert(!scalarVal || scalarVal->val.array.rawScalar);
			*pstate = pushState(pstate);
			result = &(*pstate)->contVal;
			(*pstate)->contVal.type = jbvArray;
			(*pstate)->contVal.val.array.nElems = 0;
			(*pstate)->contVal.val.array.rawScalar = (scalarVal &&
													  scalarVal->val.array.rawScalar);
			if (scalarVal && scalarVal->val.array.nElems > 0)
			{
				/* Assume that this array is still really a scalar */
				assert(scalarVal->type == jbvArray);
				(*pstate)->size = scalarVal->val.array.nElems;
			}
			else
			{
				(*pstate)->size = 4;
			}
			(*pstate)->contVal.val.array.elems = (JsonbValue *) palloc(sizeof(JsonbValue) *
																(*pstate)->size);
			break;
		case WJB_BEGIN_OBJECT:
			assert(!scalarVal);
			*pstate = pushState(pstate);
			result = &(*pstate)->contVal;
			(*pstate)->contVal.type = jbvObject;
			(*pstate)->contVal.val.object.nPairs = 0;
			(*pstate)->size = 4;
			(*pstate)->contVal.val.object.pairs = (JsonbPair *) palloc(sizeof(JsonbPair) *
																 (*pstate)->size);
			break;
		case WJB_KEY:
			assert(scalarVal->type == jbvString);
			appendKey(*pstate, scalarVal);
			break;
		case WJB_VALUE:
			assert(IsAJsonbScalar(scalarVal));
			appendValue(*pstate, scalarVal);
			break;
		case WJB_ELEM:
			assert(IsAJsonbScalar(scalarVal));
			appendElement(*pstate, scalarVal);
			break;
		case WJB_END_OBJECT:
			uniqueifyJsonbObject(&(*pstate)->contVal,
								 (*pstate)->unique_keys,
								 (*pstate)->skip_nulls);
			/* fall through! */
		case WJB_END_ARRAY:
			/* Steps here common to WJB_END_OBJECT case */
			assert(!scalarVal);
			result = &(*pstate)->contVal;

			/*
			 * Pop stack and push current array/object as value in parent
			 * array/object
			 */
			*pstate = (*pstate)->next;
			if (*pstate)
			{
				switch ((*pstate)->contVal.type)
				{
					case jbvArray:
						appendElement(*pstate, result);
						break;
					case jbvObject:
						appendValue(*pstate, result);
						break;
					default:
						LOG_ERROR("invalid jsonb container type");
				}
			}
			break;
		default:
			LOG_ERROR("unrecognized jsonb sequential processing token");
	}

	return result;
}

static JsonbIterator *
iteratorFromContainer(JsonbContainer *container, JsonbIterator *parent)
{
	JsonbIterator *it;

	it = (JsonbIterator *) palloc0(sizeof(JsonbIterator));
	it->container = container;
	it->parent = parent;
	it->nElems = JsonContainerSize(container);

	/* Array starts just after header */
	it->children = container->children;

	switch (container->header & (JB_FARRAY | JB_FOBJECT))
	{
		case JB_FARRAY:
			it->dataProper =
				(char *) it->children + it->nElems * sizeof(JEntry);
			it->isScalar = JsonContainerIsScalar(container);
			/* This is either a "raw scalar", or an array */
			assert(!it->isScalar || it->nElems == 1);

			it->state = JBI_ARRAY_START;
			break;

		case JB_FOBJECT:
			it->dataProper =
				(char *) it->children + it->nElems * sizeof(JEntry) * 2;
			it->state = JBI_OBJECT_START;
			break;

		default:
			LOG_ERROR("unknown type of jsonb container");
	}

	return it;
}

JsonbIterator *
JsonbIteratorInit(JsonbContainer *container)
{
	return iteratorFromContainer(container, nullptr);
}

static JsonbIterator *
freeAndGetParent(JsonbIterator *it)
{
	JsonbIterator *v = it->parent;

	pfree(it);
	return v;
}

uint32_t
getJsonbOffset(const JsonbContainer *jc, int index)
{
	uint32_t		offset = 0;
	int			i;

	/*
	 * Start offset of this entry is equal to the end offset of the previous
	 * entry.  Walk backwards to the most recent entry stored as an end
	 * offset, returning that offset plus any lengths in between.
	 */
	for (i = index - 1; i >= 0; i--)
	{
		offset += JBE_OFFLENFLD(jc->children[i]);
		if (JBE_HAS_OFF(jc->children[i]))
			break;
	}

	return offset;
}

uint32_t
getJsonbLength(const JsonbContainer *jc, int index)
{
	uint32_t		off;
	uint32_t		len;

	/*
	 * If the length is stored directly in the JEntry, just return it.
	 * Otherwise, get the begin offset of the entry, and subtract that from
	 * the stored end+1 offset.
	 */
	if (JBE_HAS_OFF(jc->children[index]))
	{
		off = getJsonbOffset(jc, index);
		len = JBE_OFFLENFLD(jc->children[index]) - off;
	}
	else
		len = JBE_OFFLENFLD(jc->children[index]);

	return len;
}

void
fillJsonbValue(JsonbContainer *container, int index,
			   char *base_addr, uint32_t offset,
			   JsonbValue *result)
{
	JEntry		entry = container->children[index];

	if (JBE_ISNULL(entry))
	{
		result->type = jbvNull;
	}
	else if (JBE_ISSTRING(entry))
	{
		result->type = jbvString;
		result->val.string.val = base_addr + offset;
		result->val.string.len = getJsonbLength(container, index);
		assert(result->val.string.len >= 0);
	}
	else if (JBE_ISNUMERIC(entry))
	{
		result->type = jbvNumeric;
		result->val.numeric = (Numeric) (base_addr + INTALIGN(offset));
	}
	else if (JBE_ISBOOL_TRUE(entry))
	{
		result->type = jbvBool;
		result->val.boolean = true;
	}
	else if (JBE_ISBOOL_FALSE(entry))
	{
		result->type = jbvBool;
		result->val.boolean = false;
	}
	else
	{
		assert(JBE_ISCONTAINER(entry));
		result->type = jbvBinary;
		/* Remove alignment padding from data pointer and length */
		result->val.binary.data = (JsonbContainer *) (base_addr + INTALIGN(offset));
		result->val.binary.len = getJsonbLength(container, index) -
			(INTALIGN(offset) - offset);
	}
}

JsonbIteratorToken
JsonbIteratorNext(JsonbIterator **it, JsonbValue *val, bool skipNested)
{
	if (*it == nullptr)
	{
		val->type = jbvNull;
		return WJB_DONE;
	}

	/*
	 * When stepping into a nested container, we jump back here to start
	 * processing the child. We will not recurse further in one call, because
	 * processing the child will always begin in JBI_ARRAY_START or
	 * JBI_OBJECT_START state.
	 */
recurse:
	switch ((*it)->state)
	{
		case JBI_ARRAY_START:
			/* Set v to array on first array call */
			val->type = jbvArray;
			val->val.array.nElems = (*it)->nElems;

			/*
			 * v->val.array.elems is not actually set, because we aren't doing
			 * a full conversion
			 */
			val->val.array.rawScalar = (*it)->isScalar;
			(*it)->curIndex = 0;
			(*it)->curDataOffset = 0;
			(*it)->curValueOffset = 0;	/* not actually used */
			/* Set state for next call */
			(*it)->state = JBI_ARRAY_ELEM;
			return WJB_BEGIN_ARRAY;

		case JBI_ARRAY_ELEM:
			if ((*it)->curIndex >= (*it)->nElems)
			{
				/*
				 * All elements within array already processed.  Report this
				 * to caller, and give it back original parent iterator (which
				 * independently tracks iteration progress at its level of
				 * nesting).
				 */
				*it = freeAndGetParent(*it);
				val->type = jbvNull;
				return WJB_END_ARRAY;
			}

			fillJsonbValue((*it)->container, (*it)->curIndex,
						   (*it)->dataProper, (*it)->curDataOffset,
						   val);

			JBE_ADVANCE_OFFSET((*it)->curDataOffset,
							   (*it)->children[(*it)->curIndex]);
			(*it)->curIndex++;

			if (!IsAJsonbScalar(val) && !skipNested)
			{
				/* Recurse into container. */
				*it = iteratorFromContainer(val->val.binary.data, *it);
				goto recurse;
			}
			else
			{
				/*
				 * Scalar item in array, or a container and caller didn't want
				 * us to recurse into it.
				 */
				return WJB_ELEM;
			}

		case JBI_OBJECT_START:
			/* Set v to object on first object call */
			val->type = jbvObject;
			val->val.object.nPairs = (*it)->nElems;

			/*
			 * v->val.object.pairs is not actually set, because we aren't
			 * doing a full conversion
			 */
			(*it)->curIndex = 0;
			(*it)->curDataOffset = 0;
			(*it)->curValueOffset = getJsonbOffset((*it)->container,
												   (*it)->nElems);
			/* Set state for next call */
			(*it)->state = JBI_OBJECT_KEY;
			return WJB_BEGIN_OBJECT;

		case JBI_OBJECT_KEY:
			if ((*it)->curIndex >= (*it)->nElems)
			{
				/*
				 * All pairs within object already processed.  Report this to
				 * caller, and give it back original containing iterator
				 * (which independently tracks iteration progress at its level
				 * of nesting).
				 */
				*it = freeAndGetParent(*it);
				val->type = jbvNull;
				return WJB_END_OBJECT;
			}
			else
			{
				/* Return key of a key/value pair.  */
				fillJsonbValue((*it)->container, (*it)->curIndex,
							   (*it)->dataProper, (*it)->curDataOffset,
							   val);
				if (val->type != jbvString)
					LOG_ERROR("unexpected jsonb type as object key");

				/* Set state for next call */
				(*it)->state = JBI_OBJECT_VALUE;
				return WJB_KEY;
			}

		case JBI_OBJECT_VALUE:
			/* Set state for next call */
			(*it)->state = JBI_OBJECT_KEY;

			fillJsonbValue((*it)->container, (*it)->curIndex + (*it)->nElems,
						   (*it)->dataProper, (*it)->curValueOffset,
						   val);

			JBE_ADVANCE_OFFSET((*it)->curDataOffset,
							   (*it)->children[(*it)->curIndex]);
			JBE_ADVANCE_OFFSET((*it)->curValueOffset,
							   (*it)->children[(*it)->curIndex + (*it)->nElems]);
			(*it)->curIndex++;

			/*
			 * Value may be a container, in which case we recurse with new,
			 * child iterator (unless the caller asked not to, by passing
			 * skipNested).
			 */
			if (!IsAJsonbScalar(val) && !skipNested)
			{
				*it = iteratorFromContainer(val->val.binary.data, *it);
				goto recurse;
			}
			else
				return WJB_VALUE;
	}

	LOG_ERROR("invalid jsonb iterator state");
	/* satisfy compilers that don't know that elog(ERROR) doesn't return */
	val->type = jbvNull;
	return WJB_DONE;
}

JsonbValue *
pushJsonbValue(JsonbParseState **pstate, JsonbIteratorToken seq,
			   JsonbValue *jbval)
{
	JsonbIterator *it;
	JsonbValue *res = nullptr;
	JsonbValue v;
	JsonbIteratorToken tok;
	int i;

	if (jbval && (seq == WJB_ELEM || seq == WJB_VALUE) && jbval->type == jbvObject)
	{
		pushJsonbValue(pstate, WJB_BEGIN_OBJECT, nullptr);
		for (i = 0; i < jbval->val.object.nPairs; i++)
		{
			pushJsonbValue(pstate, WJB_KEY, jbval->val.object.pairs[i].key);
			pushJsonbValue(pstate, WJB_VALUE, jbval->val.object.pairs[i].value);
		}

		return pushJsonbValue(pstate, WJB_END_OBJECT, nullptr);
	}

	if (jbval && (seq == WJB_ELEM || seq == WJB_VALUE) && jbval->type == jbvArray)
	{
		pushJsonbValue(pstate, WJB_BEGIN_ARRAY, nullptr);
		for (i = 0; i < jbval->val.array.nElems; i++)
		{
			pushJsonbValue(pstate, WJB_ELEM, &jbval->val.array.elems[i]);
		}

		return pushJsonbValue(pstate, WJB_END_ARRAY, nullptr);
	}

	if (!jbval || (seq != WJB_ELEM && seq != WJB_VALUE) ||
		jbval->type != jbvBinary)
	{
		/* drop through */
		return pushJsonbValueScalar(pstate, seq, jbval);
	}

	/* unpack the binary and add each piece to the pstate */
	it = JsonbIteratorInit(jbval->val.binary.data);

	if ((jbval->val.binary.data->header & JB_FSCALAR) && *pstate)
	{
		tok = JsonbIteratorNext(&it, &v, true);
		assert(tok == WJB_BEGIN_ARRAY);
		assert(v.type == jbvArray && v.val.array.rawScalar);

		tok = JsonbIteratorNext(&it, &v, true);
		assert(tok == WJB_ELEM);

		res = pushJsonbValueScalar(pstate, seq, &v);

		tok = JsonbIteratorNext(&it, &v, true);
		assert(tok == WJB_END_ARRAY);
		assert(it == nullptr);

		return res;
	}

	while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
		res = pushJsonbValueScalar(pstate, tok,
								   tok < WJB_BEGIN_ARRAY ||
								   (tok == WJB_BEGIN_ARRAY &&
									v.val.array.rawScalar) ? &v : nullptr);

	return res;
}

char *
JsonEncodeDateTime(char *buf, Datum value, Oid typid, const int *tzp)
{
	if (!buf)
		buf = (char *) palloc(MAXDATELEN + 1);

	switch (typid)
	{
		case DATEOID:
			{
				DateADT		date;
				struct pg_tm tm;

				date = DatumGetDateADT(value);

				/* Same as date_out(), but forcing DateStyle */
				if (DATE_NOT_FINITE(date))
					EncodeSpecialDate(date, buf);
				else
				{
					j2date(date + POSTGRES_EPOCH_JDATE,
						   &(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
					EncodeDateOnly(&tm, USE_XSD_DATES, buf);
				}
			}
			break;
		case TIMEOID:
			{
				TimeADT		time = DatumGetTimeADT(value);
				struct pg_tm tt,
						   *tm = &tt;
				fsec_t		fsec;

				/* Same as time_out(), but forcing DateStyle */
				time2tm(time, tm, &fsec);
				EncodeTimeOnly(tm, fsec, false, 0, USE_XSD_DATES, buf);
			}
			break;
		case TIMETZOID:
			{
				TimeTzADT  *time = DatumGetTimeTzADTP(value);
				struct pg_tm tt, *tm = &tt;
				fsec_t		fsec;
				int			tz;

				/* Same as timetz_out(), but forcing DateStyle */
				timetz2tm(time, tm, &fsec, &tz);
				EncodeTimeOnly(tm, fsec, true, tz, USE_XSD_DATES, buf);
			}
			break;
		case TIMESTAMPOID:
			{
				Timestamp	timestamp;
				struct pg_tm tm;
				fsec_t		fsec;

				timestamp = DatumGetTimestamp(value);
				/* Same as timestamp_out(), but forcing DateStyle */
				if (TIMESTAMP_NOT_FINITE(timestamp))
					EncodeSpecialTimestamp(timestamp, buf);
				else if (timestamp2tm(timestamp, nullptr, &tm, &fsec, nullptr, nullptr) == 0)
					EncodeDateTime(&tm, fsec, false, 0, nullptr, USE_XSD_DATES, buf);
				else
					LOG_ERROR("timestamp out of range");
			}
			break;
		case TIMESTAMPTZOID:
			{
				TimestampTz timestamp;
				struct pg_tm tm;
				int			tz;
				fsec_t		fsec;
				const char *tzn = nullptr;

				timestamp = DatumGetTimestampTz(value);

				/*
				 * If a time zone is specified, we apply the time-zone shift,
				 * convert timestamptz to pg_tm as if it were without a time
				 * zone, and then use the specified time zone for converting
				 * the timestamp into a string.
				 */
				if (tzp)
				{
					tz = *tzp;
					timestamp -= (TimestampTz) tz * USECS_PER_SEC;
				}

				/* Same as timestamptz_out(), but forcing DateStyle */
				if (TIMESTAMP_NOT_FINITE(timestamp))
					EncodeSpecialTimestamp(timestamp, buf);
				else if (timestamp2tm(timestamp, tzp ? nullptr : &tz, &tm, &fsec,
									  tzp ? nullptr : &tzn, nullptr) == 0)
				{
					if (tzp)
						tm.tm_isdst = 1;	/* set time-zone presence flag */

					EncodeDateTime(&tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
				}
				else
					LOG_ERROR("timestamp out of range");
			}
			break;
		default:
			LOG_ERROR("unknown jsonb value datetime type oid %u", typid);
			return nullptr;
	}

	return buf;
}

JsonParseErrorType
json_lex_number(JsonLexContext *lex, char *s,
				bool *num_err, int *total_len)
{
	bool		error = false;
	int			len = s - lex->input;

	/* Part (1): leading sign indicator. */
	/* Caller already did this for us; so do nothing. */

	/* Part (2): parse main digit string. */
	if (len < lex->input_length && *s == '0')
	{
		s++;
		len++;
	}
	else if (len < lex->input_length && *s >= '1' && *s <= '9')
	{
		do
		{
			s++;
			len++;
		} while (len < lex->input_length && *s >= '0' && *s <= '9');
	}
	else
		error = true;

	/* Part (3): parse optional decimal portion. */
	if (len < lex->input_length && *s == '.')
	{
		s++;
		len++;
		if (len == lex->input_length || *s < '0' || *s > '9')
			error = true;
		else
		{
			do
			{
				s++;
				len++;
			} while (len < lex->input_length && *s >= '0' && *s <= '9');
		}
	}

	/* Part (4): parse optional exponent. */
	if (len < lex->input_length && (*s == 'e' || *s == 'E'))
	{
		s++;
		len++;
		if (len < lex->input_length && (*s == '+' || *s == '-'))
		{
			s++;
			len++;
		}
		if (len == lex->input_length || *s < '0' || *s > '9')
			error = true;
		else
		{
			do
			{
				s++;
				len++;
			} while (len < lex->input_length && *s >= '0' && *s <= '9');
		}
	}

	/*
	 * Check for trailing garbage.  As in json_lex(), any alphanumeric stuff
	 * here should be considered part of the token for error-reporting
	 * purposes.
	 */
	for (; len < lex->input_length && JSON_ALPHANUMERIC_CHAR(*s); s++, len++)
		error = true;

	if (total_len != nullptr)
		*total_len = len;

	if (num_err != nullptr)
	{
		/* let the caller handle any error */
		*num_err = error;
	}
	else
	{
		/* return token endpoint */
		lex->prev_token_terminator = lex->token_terminator;
		lex->token_terminator = s;
		/* handle error if any */
		if (error)
			return JSON_INVALID_TOKEN;
	}

	return JSON_SUCCESS;
}

bool
IsValidJsonNumber(const char *str, int len)
{
	bool		numeric_error;
	int			total_len;
	JsonLexContext dummy_lex;

	if (len <= 0)
		return false;

	/*
	 * json_lex_number expects a leading  '-' to have been eaten already.
	 *
	 * having to cast away the constness of str is ugly, but there's not much
	 * easy alternative.
	 */
	if (*str == '-')
	{
		dummy_lex.input = unconstify(char *, str) + 1;
		dummy_lex.input_length = len - 1;
	}
	else
	{
		dummy_lex.input = unconstify(char *, str);
		dummy_lex.input_length = len;
	}

	json_lex_number(&dummy_lex, dummy_lex.input, &numeric_error, &total_len);

	return (!numeric_error) && (total_len == dummy_lex.input_length);
}

int
reserveFromBuffer(StringInfo buffer, int len)
{
	int			offset;

	/* Make more room if needed */
	enlargeStringInfo(buffer, len);

	/* remember current offset */
	offset = buffer->len;

	/* reserve the space */
	buffer->len += len;

	/*
	 * Keep a trailing null in place, even though it's not useful for us; it
	 * seems best to preserve the invariants of StringInfos.
	 */
	buffer->data[buffer->len] = '\0';

	return offset;
}

void
copyToBuffer(StringInfo buffer, int offset, const char *data, int len)
{
	memcpy(buffer->data + offset, data, len);
}


void
appendToBuffer(StringInfo buffer, const char *data, int len)
{
	int			offset;

	offset = reserveFromBuffer(buffer, len);
	copyToBuffer(buffer, offset, data, len);
}

short
padBufferToInt(StringInfo buffer)
{
	int			padlen,
				p,
				offset;

	padlen = INTALIGN(buffer->len) - buffer->len;

	offset = reserveFromBuffer(buffer, padlen);

	/* padlen must be small, so this is probably faster than a memset */
	for (p = 0; p < padlen; p++)
		buffer->data[offset + p] = '\0';

	return padlen;
}

void
convertJsonbValue(StringInfo buffer, JEntry *header, JsonbValue *val, int level)
{
	if (!val)
		return;

	/*
	 * A JsonbValue passed as val should never have a type of jbvBinary, and
	 * neither should any of its sub-components. Those values will be produced
	 * by convertJsonbArray and convertJsonbObject, the results of which will
	 * not be passed back to this function as an argument.
	 */

	if (IsAJsonbScalar(val))
		convertJsonbScalar(buffer, header, val);
	else if (val->type == jbvArray)
		convertJsonbArray(buffer, header, val, level);
	else if (val->type == jbvObject)
		convertJsonbObject(buffer, header, val, level);
	else
		LOG_ERROR("unknown type of jsonb container to convert");
}

void
convertJsonbArray(StringInfo buffer, JEntry *header, JsonbValue *val, int level)
{
	int			base_offset;
	int			jentry_offset;
	int			i;
	int			totallen;
	uint32_t		containerhead;
	int			nElems = val->val.array.nElems;

	/* Remember where in the buffer this array starts. */
	base_offset = buffer->len;

	/* Align to 4-byte boundary (any padding counts as part of my data) */
	padBufferToInt(buffer);

	/*
	 * Construct the header Jentry and store it in the beginning of the
	 * variable-length payload.
	 */
	containerhead = nElems | JB_FARRAY;
	if (val->val.array.rawScalar)
	{
		assert(nElems == 1);
		assert(level == 0);
		containerhead |= JB_FSCALAR;
	}

	appendToBuffer(buffer, (char *) &containerhead, sizeof(uint32_t));

	/* Reserve space for the JEntries of the elements. */
	jentry_offset = reserveFromBuffer(buffer, sizeof(JEntry) * nElems);

	totallen = 0;
	for (i = 0; i < nElems; i++)
	{
		JsonbValue *elem = &val->val.array.elems[i];
		int			len;
		JEntry		meta;

		/*
		 * Convert element, producing a JEntry and appending its
		 * variable-length data to buffer
		 */
		convertJsonbValue(buffer, &meta, elem, level + 1);

		len = JBE_OFFLENFLD(meta);
		totallen += len;

		/*
		 * Bail out if total variable-length data exceeds what will fit in a
		 * JEntry length field.  We check this in each iteration, not just
		 * once at the end, to forestall possible integer overflow.
		 */
		if (totallen > JENTRY_OFFLENMASK)
			LOG_ERROR("total size of jsonb array elements exceeds the maximum of %d bytes", JENTRY_OFFLENMASK);

		/*
		 * Convert each JB_OFFSET_STRIDE'th length to an offset.
		 */
		if ((i % JB_OFFSET_STRIDE) == 0)
			meta = (meta & JENTRY_TYPEMASK) | totallen | JENTRY_HAS_OFF;

		copyToBuffer(buffer, jentry_offset, (char *) &meta, sizeof(JEntry));
		jentry_offset += sizeof(JEntry);
	}

	/* Total data size is everything we've appended to buffer */
	totallen = buffer->len - base_offset;

	/* Check length again, since we didn't include the metadata above */
	if (totallen > JENTRY_OFFLENMASK)
		LOG_ERROR("total size of jsonb array elements exceeds the maximum of %d bytes", JENTRY_OFFLENMASK);

	/* Initialize the header of this node in the container's JEntry array */
	*header = JENTRY_ISCONTAINER | totallen;
}

void
convertJsonbObject(StringInfo buffer, JEntry *header, JsonbValue *val, int level)
{
	int			base_offset;
	int			jentry_offset;
	int			i;
	int			totallen;
	uint32_t		containerheader;
	int			nPairs = val->val.object.nPairs;

	/* Remember where in the buffer this object starts. */
	base_offset = buffer->len;

	/* Align to 4-byte boundary (any padding counts as part of my data) */
	padBufferToInt(buffer);

	/*
	 * Construct the header Jentry and store it in the beginning of the
	 * variable-length payload.
	 */
	containerheader = nPairs | JB_FOBJECT;
	appendToBuffer(buffer, (char *) &containerheader, sizeof(uint32_t));

	/* Reserve space for the JEntries of the keys and values. */
	jentry_offset = reserveFromBuffer(buffer, sizeof(JEntry) * nPairs * 2);

	/*
	 * Iterate over the keys, then over the values, since that is the ordering
	 * we want in the on-disk representation.
	 */
	totallen = 0;
	for (i = 0; i < nPairs; i++)
	{
		JsonbPair  *pair = &val->val.object.pairs[i];
		int			len;
		JEntry		meta;

		/*
		 * Convert key, producing a JEntry and appending its variable-length
		 * data to buffer
		 */
		convertJsonbScalar(buffer, &meta, pair->key);

		len = JBE_OFFLENFLD(meta);
		totallen += len;

		/*
		 * Bail out if total variable-length data exceeds what will fit in a
		 * JEntry length field.  We check this in each iteration, not just
		 * once at the end, to forestall possible integer overflow.
		 */
		if (totallen > JENTRY_OFFLENMASK)
			LOG_ERROR("total size of jsonb object elements exceeds the maximum of %d bytes", JENTRY_OFFLENMASK);

		/*
		 * Convert each JB_OFFSET_STRIDE'th length to an offset.
		 */
		if ((i % JB_OFFSET_STRIDE) == 0)
			meta = (meta & JENTRY_TYPEMASK) | totallen | JENTRY_HAS_OFF;

		copyToBuffer(buffer, jentry_offset, (char *) &meta, sizeof(JEntry));
		jentry_offset += sizeof(JEntry);
	}
	for (i = 0; i < nPairs; i++)
	{
		JsonbPair  *pair = &val->val.object.pairs[i];
		int			len;
		JEntry		meta;

		/*
		 * Convert value, producing a JEntry and appending its variable-length
		 * data to buffer
		 */
		convertJsonbValue(buffer, &meta, pair->value, level + 1);

		len = JBE_OFFLENFLD(meta);
		totallen += len;

		/*
		 * Bail out if total variable-length data exceeds what will fit in a
		 * JEntry length field.  We check this in each iteration, not just
		 * once at the end, to forestall possible integer overflow.
		 */
		if (totallen > JENTRY_OFFLENMASK)
			LOG_ERROR("total size of jsonb object elements exceeds the maximum of %d bytes", JENTRY_OFFLENMASK);

		/*
		 * Convert each JB_OFFSET_STRIDE'th length to an offset.
		 */
		if (((i + nPairs) % JB_OFFSET_STRIDE) == 0)
			meta = (meta & JENTRY_TYPEMASK) | totallen | JENTRY_HAS_OFF;

		copyToBuffer(buffer, jentry_offset, (char *) &meta, sizeof(JEntry));
		jentry_offset += sizeof(JEntry);
	}

	/* Total data size is everything we've appended to buffer */
	totallen = buffer->len - base_offset;

	/* Check length again, since we didn't include the metadata above */
	if (totallen > JENTRY_OFFLENMASK)
		LOG_ERROR("total size of jsonb object elements exceeds the maximum of %d bytes", JENTRY_OFFLENMASK);

	/* Initialize the header of this node in the container's JEntry array */
	*header = JENTRY_ISCONTAINER | totallen;
}

void
convertJsonbScalar(StringInfo buffer, JEntry *header, JsonbValue *scalarVal)
{
	int			numlen;
	short		padlen;

	switch (scalarVal->type)
	{
		case jbvNull:
			*header = JENTRY_ISNULL;
			break;

		case jbvString:
			appendToBuffer(buffer, scalarVal->val.string.val, scalarVal->val.string.len);

			*header = scalarVal->val.string.len;
			break;

		case jbvNumeric:
			numlen = VARSIZE_ANY(scalarVal->val.numeric);
			padlen = padBufferToInt(buffer);

			appendToBuffer(buffer, (char *) scalarVal->val.numeric, numlen);

			*header = JENTRY_ISNUMERIC | (padlen + numlen);
			break;

		case jbvBool:
			*header = (scalarVal->val.boolean) ?
				JENTRY_ISBOOL_TRUE : JENTRY_ISBOOL_FALSE;
			break;

		case jbvDatetime:
			{
				char		buf[MAXDATELEN + 1];
				size_t		len;

				JsonEncodeDateTime(buf,
								   scalarVal->val.datetime.value,
								   scalarVal->val.datetime.typid,
								   &scalarVal->val.datetime.tz);
				len = strlen(buf);
				appendToBuffer(buffer, buf, len);

				*header = len;
			}
			break;

		default:
			LOG_ERROR("invalid jsonb scalar type");
	}
}

Jsonb *
convertToJsonb(JsonbValue *val)
{
	StringInfoData buffer;
	JEntry		jentry;
	Jsonb	   *res;

	/* Should not already have binary representation */
	assert(val->type != jbvBinary);

	/* Allocate an output buffer. It will be enlarged as needed */
	initStringInfo(&buffer);

	/* Make room for the varlena header */
	reserveFromBuffer(&buffer, VARHDRSZ);

	convertJsonbValue(&buffer, &jentry, val, 0);

	/*
	 * Note: the JEntry of the root is discarded. Therefore the root
	 * JsonbContainer struct must contain enough information to tell what kind
	 * of value it is.
	 */

	res = (Jsonb *) buffer.data;

	SET_VARSIZE(res, buffer.len);

	return res;
}

Jsonb *
JsonbValueToJsonb(JsonbValue *val)
{
	Jsonb	   *out;

	if (IsAJsonbScalar(val))
	{
		/* Scalar value */
		JsonbParseState *pstate = nullptr;
		JsonbValue *res;
		JsonbValue	scalarArray;

		scalarArray.type = jbvArray;
		scalarArray.val.array.rawScalar = true;
		scalarArray.val.array.nElems = 1;

		pushJsonbValue(&pstate, WJB_BEGIN_ARRAY, &scalarArray);
		pushJsonbValue(&pstate, WJB_ELEM, val);
		res = pushJsonbValue(&pstate, WJB_END_ARRAY, nullptr);

		out = convertToJsonb(res);
	}
	else if (val->type == jbvObject || val->type == jbvArray)
	{
		out = convertToJsonb(val);
	}
	else
	{
		assert(val->type == jbvBinary);
		out = (Jsonb *) palloc(VARHDRSZ + val->val.binary.len);
		SET_VARSIZE(out, VARHDRSZ + val->val.binary.len);
		memcpy(VARDATA(out), val->val.binary.data, val->val.binary.len);
	}

	return out;
}

void
escape_json(StringInfo buf, const char *str)
{
    const char *p;

    appendStringInfoCharMacro(buf, '"');
    for (p = str; *p; p++) {
        switch (*p) {
            case '\b':
                appendStringInfoString(buf, "\\b");
                break;
            case '\f':
                appendStringInfoString(buf, "\\f");
                break;
            case '\n':
                appendStringInfoString(buf, "\\n");
                break;
            case '\r':
                appendStringInfoString(buf, "\\r");
                break;
            case '\t':
                appendStringInfoString(buf, "\\t");
                break;
            case '"':
                appendStringInfoString(buf, "\\\"");
                break;
            case '\\':
                appendStringInfoString(buf, "\\\\");
                break;
            default:
                if ((unsigned char)*p < ' ') {
                    appendStringInfo(buf, "\\u%04x", (int)*p);
                } else {
                    appendStringInfoCharMacro(buf, *p);
                }
                break;
        }
    }
    appendStringInfoCharMacro(buf, '"');
}
