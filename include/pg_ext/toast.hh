#pragma once

#include <pg_ext/export.hh>

#define PG_DETOAST_DATUM(datum) \
	pg_detoast_datum((struct varlena *) pgext::DatumGetPointer(datum))

extern "C" PGEXT_API struct varlena *pg_detoast_datum(struct varlena *datum);
extern "C" PGEXT_API struct varlena *pg_detoast_datum_packed(struct varlena *datum);
extern "C" PGEXT_API struct varlena *pg_detoast_datum_copy(struct varlena *datum);
