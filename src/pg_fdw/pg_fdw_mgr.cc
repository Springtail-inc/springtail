#include <pg_fdw/pg_fdw_mgr.hh>

extern "C" {
    #include <postgres.h>
    #include <postgres_ext.h>
    #include <utils/builtins.h>
}

namespace springtail {
    PgFdwMgr* PgFdwMgr::_instance {nullptr};

    std::once_flag PgFdwMgr::_init_flag;

    PgFdwMgr*
    PgFdwMgr::_init()
    {
        _instance = new PgFdwMgr();
        return _instance;
    }

    PgFdwState *
    PgFdwMgr::fdw_begin(uint64_t tid)
    {
        uint64_t xid = XidMgrClient::get_instance()->get_committed_xid();
        TablePtr table = TableMgr::get_instance()->get_table(tid, xid, constant::MAX_LSN);

        PgFdwState *state = new PgFdwState{table, tid, xid, table->begin()};
        return state;
    }

    bool
    PgFdwMgr::fdw_iterate_scan(PgFdwState *state, Datum *values, bool *nulls)
    {
        // check iterator is valid
        if (!state->iter.has_value()) {
            return false;
        }

        // check if iterator is at end
        if (*state->iter == state->table->end()) {
            return false;
        }

        // get current row
        Extent::Row row = *(*state->iter);
        // iterate through fields
        for (size_t i = 0; i < state->fields->size(); i++) {
            // get field
            FieldPtr field = state->fields->at(i);
            // set value
            values[i] = _get_datum_from_field(field, row);
            // set null
            nulls[i] = field->is_null(row);
        }

        // increment iterator
        (*state->iter)++;

        return true;
    }

    Datum
    PgFdwMgr::_get_datum_from_field(FieldPtr field, const Extent::Row &row)
    {
        switch (field->get_type()) {
            case SchemaType::INT64:
                return Int64GetDatum(field->get_int64(row));
            case SchemaType::UINT64:
                return UInt64GetDatum(field->get_uint64(row));
            case SchemaType::INT32:
                return Int32GetDatum(field->get_int32(row));
            case SchemaType::UINT32:
                return UInt32GetDatum(field->get_uint32(row));
            case SchemaType::INT16:
                return Int16GetDatum(field->get_int16(row));
            case SchemaType::UINT16:
                return UInt16GetDatum(field->get_uint16(row));
            case SchemaType::INT8:
                return Int8GetDatum(field->get_int8(row));
            case SchemaType::UINT8:
                return UInt8GetDatum(field->get_uint8(row));
            case SchemaType::BOOLEAN:
                return BoolGetDatum(field->get_bool(row));
            case SchemaType::FLOAT64:
                return Float8GetDatum(field->get_float64(row));
            case SchemaType::FLOAT32:
                return Float4GetDatum(field->get_float32(row));
            case SchemaType::TEXT: {
                char *duped_str = pstrdup(field->get_text(row).c_str());
                return CStringGetTextDatum(duped_str);
            }

            // XXX no getters in field for
            case SchemaType::TIMESTAMP:
            case SchemaType::DATE:
            case SchemaType::TIME:
            case SchemaType::DECIMAL128:

            default:
                return 0;
        }
    }
}