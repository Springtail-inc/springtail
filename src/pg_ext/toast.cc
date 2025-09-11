#include <pg_ext/toast.hh>

struct varlena *
pg_detoast_datum(struct varlena *datum)
{
    // XXX Stubbed for now
    if (!datum) {
        return nullptr;
    }
    return datum;
}

struct varlena *
pg_detoast_datum_packed(struct varlena *datum)
{
    // XXX Stubbed for now
    return datum;
}

struct varlena *
pg_detoast_datum_copy(struct varlena *datum)
{
    // XXX Stubbed for now
    return datum;
}
