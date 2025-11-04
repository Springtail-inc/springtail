#include <pg_ext/heaptuple.hh>

void
assign_record_type_typmod(TupleDesc tupdesc)
{
    // XXX Stubbed for now
}

TupleDesc BlessTupleDesc(TupleDesc tupdesc)
{
    // RECORDOID XXX Stubbed for now
    if (tupdesc->tdtypeid == -1 && tupdesc->tdtypmod < 0) {
        assign_record_type_typmod(tupdesc);
    }

    return tupdesc;
}

Datum
HeapTupleHeaderGetDatum(HeapTupleHeader tuple)
{
	Datum result = 0;
	// XXX Stubbed for now
	return result;
}

HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull)
{
    // XXX Stubbed for now
    return nullptr;
}

TupleDesc
lookup_rowtype_tupdesc_domain(Oid type_id, int32_t typmod, bool noError)
{
    // XXX Stubbed for now
    return nullptr;
}

void
DecrTupleDescRefCount(TupleDesc tupdesc)
{
    // XXX Stubbed for now
}

void
heap_deform_tuple(HeapTuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull)
{
    // XXX Stubbed for now
}
