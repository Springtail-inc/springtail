#include <sstream>
#include <pg_fdw/pg_fdw_mgr.hh>

extern "C" {
    #include <postgres.h>
    #include <utils/numeric.h>
}

/** Wrapper around PgFdwMgr class for use in C code */

using namespace springtail::pg_fdw;

namespace {
    std::string _get_value_string(const ConstQual& qual)
    {
        if (qual.isnull) {
            return "NULL";
        }

        std::ostringstream ss;

        switch (qual.base.typeoid) {
            case MONEYOID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            case TIMEOID:
            case INT8OID:
                ss << DatumGetInt64(qual.value);
                break;

            case DATEOID:
            case INT4OID:
                ss << DatumGetInt32(qual.value);
                break;

            case INT2OID:
                ss << DatumGetInt16(qual.value);
                break;
            case FLOAT8OID:
                ss << DatumGetFloat8(qual.value);
                break;
            case FLOAT4OID:
                ss << DatumGetFloat4(qual.value);
                break;
            case BOOLOID:
                ss << DatumGetBool(qual.value);
                break;
            case CHAROID:
                ss << DatumGetBool(qual.value);
                break;
            case UUIDOID: {
                const char* p = reinterpret_cast<const char*>(DatumGetPointer(qual.value));
                for (int i = 0; i != 16; ++i) {
                    ss << p[i];
                }
                ss << "::UUID";
                break;
            }
            case VARCHAROID:
            case TEXTOID: {
                const char *str = TextDatumGetCString(qual.value);
                ss << "'" << str << "'";
                break;
            }
            case NUMERICOID: // DECIMAL(x,y)
                {
                    auto v = DatumGetNumeric(qual.value);
                    ss << numeric_normalize(v);
                    ss << "::NUMERIC";
                }
                break;
            default:
                // handle enum user defined type
                if (qual.base.typeoid >= FirstNormalObjectId) {
                    Oid oid = DatumGetObjectId(qual.value);
                    ss << oid << "::USER";
                    break;
                }
                break;
        }
        return ss.str();
    }

    const std::map<QualOpName, std::string>& _op_symbols()
    {
        static const std::map<QualOpName, std::string> ops =
        {
            {UNSUPPORTED, "~~~"},
            {EQUALS, "="},
            {NOT_EQUALS, "!="},
            {LESS_THAN, "<"},
            {LESS_THAN_EQUALS, "<="},
            {GREATER_THAN, ">"},
            {GREATER_THAN_EQUALS, ">="}
        };
        return ops;
    }
}

extern "C" {
    #include <postgres.h>
    typedef struct List List;

    /** Get PgFdwMgr singleton */
    PgFdwMgr *
    get_fdw_mgr()
    {
        return PgFdwMgr::get_instance();
    }

    /** Init call */
    void
    fdw_init(const char *config_file_path)
    {
        PgFdwMgr::fdw_init(config_file_path);
    }

    /** Create state for this table and transaction */
    void *
    fdw_create_state(uint64_t db_id, uint64_t tid, uint64_t pg_xid, uint64_t schema_xid)
    {
        return get_fdw_mgr()->fdw_create_state(db_id, tid, pg_xid, schema_xid);
    }

    /** Begin scan wrapper */
    void
    fdw_begin_scan(void *state, int num_attrs, Form_pg_attribute* attrs, List *target_list, List *qual_list, List *sortgroup)
    {
        if (state) {
            return get_fdw_mgr()->fdw_begin_scan(static_cast<PgFdwState*>(state), num_attrs, attrs, target_list, qual_list, sortgroup);
        }
    }

    /** Iterate scan wrapper */
    bool
    fdw_iterate_scan(void *state, Datum *values, bool *nulls, bool *eos)
    {
        if (state) {
            return get_fdw_mgr()->fdw_iterate_scan(static_cast<PgFdwState*>(state), values, nulls, eos);
        }
        return false;
    }

    /** End scan wrapper */
    void
    fdw_end_scan(void *state)
    {
        if (state) {
            get_fdw_mgr()->fdw_end_scan(static_cast<PgFdwState*>(state));
        }
    }

    /** Reset scan wrapper */
    void
    fdw_reset_scan(void *state)
    {
        if (state) {
            get_fdw_mgr()->fdw_reset_scan(static_cast<PgFdwState*>(state));
        }
    }

    /** Import foreign schema wrapper */
    List *
    fdw_import_foreign_schema(const char *server, const char *schema,
                              const List *table_list, bool exclude, bool limit,
                              uint64_t db_id, const char *db_name, uint64_t schema_xid)
    {
        return get_fdw_mgr()->fdw_import_foreign_schema(server, schema,
                                                        table_list, exclude, limit,
                                                        db_id, db_name, schema_xid);
    }

    /** Helper return true if table is sortable by sort group */
    List *
    fdw_can_sort(SpringtailPlanState *planstate, List *sortgroup)
    {
        return get_fdw_mgr()->fdw_can_sort(planstate, sortgroup);
    }

    /** Get list of path keys (key name, num rows) */
    List *
    fdw_get_path_keys(SpringtailPlanState *planstate)
    {
        return get_fdw_mgr()->fdw_get_path_keys(planstate);
    }

    void
    fdw_get_rel_size(SpringtailPlanState *planstate, List *target_list, List *qual_list, double *rows, int *width)
    {
        get_fdw_mgr()->fdw_get_rel_size(planstate, target_list, qual_list, rows, width);
    }

    void
    fdw_commit_rollback(uint64_t pg_xid, bool commit)
    {
        get_fdw_mgr()->fdw_commit_rollback(pg_xid, commit);
    }

    void 
    fdw_explain_scan(ForeignScanState *node, ExplainState *es)
    {
        const PgFdwState* state = static_cast<PgFdwState*>(node->fdw_state);
        auto v = get_fdw_mgr()->fdw_explain_scan(state);

        for (auto const& [name, value]: v) {
            ExplainPropertyText(name.c_str(), value.c_str(), es);
        }

        // collect quals
        std::ostringstream ss;

        ss.str("");

        for (const ConstQual *qual: state->filtered_quals) {
            const auto& column = state->columns.at(state->attr_map.at(qual->base.varattno));
            (ss.tellp()? ss << ", ": ss) <<
                std::format("({} {} {})", column.name, _op_symbols().at(qual->base.op), _get_value_string(*qual));
        }

        if (!ss.str().empty()) {
            ExplainPropertyText("   Filters", ss.str().c_str(), es);
        }
    }
}

