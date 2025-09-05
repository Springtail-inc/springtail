#include <sstream>
#include <pg_fdw/pg_fdw_mgr.hh>
#include <pg_fdw/pg_fdw_plan_state.hh>

/** Wrapper around PgFdwMgr class for use in C code */

using namespace springtail::pg_fdw;

extern "C" {
    #include <postgres.h>
    typedef struct List List;

    /** Get PgFdwMgr singleton */
    static inline PgFdwMgr *
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

    void
    fdw_start(const char *db_name, bool ddl_connection)
    {
        PgFdwMgr::get_instance()->init(db_name, ddl_connection);
    }

    /** Exit call */
    void
    fdw_exit()
    {
        PgFdwMgr::fdw_exit();
    }

    /** Create state for this table and transaction */
    void *
    fdw_create_state(uint64_t db_id, uint64_t tid, uint64_t pg_xid, uint64_t schema_xid)
    {
        return get_fdw_mgr()->fdw_create_state(db_id, tid, pg_xid, schema_xid);
    }

    /** Begin scan wrapper */
    void*
    fdw_begin_scan_x(List* state, int num_attrs, Form_pg_attribute* attrs,  List *quals)
    {
        SpringtailPlanState ps{state};
        return get_fdw_mgr()->fdw_begin_scan_x(&ps, num_attrs, attrs, quals);
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
    fdw_reset_scan(void *state, List *qual_list)
    {
        if (state) {
            get_fdw_mgr()->fdw_reset_scan(static_cast<PgFdwState*>(state), qual_list);
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
    fdw_can_sort(List* planstate, void* state, List *sortgroup, List* quals)
    {
        SpringtailPlanState ps{planstate};
        return get_fdw_mgr()->fdw_can_sort(&ps, (PgFdwState*)state, sortgroup, quals);
    }

    /** Get list of path keys (key name, num rows) */
    List* 
    fdw_get_path_keys_x(List* planstate, void *scan_state)
    {
        SpringtailPlanState ps{planstate};
        return get_fdw_mgr()->fdw_get_path_keys_x(&ps, (PgFdwState*)scan_state);
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
    }

    uint64_t
    fdw_get_rel_width(void* state)
    {
        SpringtailPlanState ps{(List*)state};
        return ps.get_rel_width();
    }

    void
    fdw_add_target(void* state, char* name, int16 attr)
    {
        SpringtailPlanState ps{(List*)state};
        ps.add_target_column(std::string(name), attr); 
    }

    void
    fdw_get_rel_size_x(List *state, List *qual_list, List* join_quals, double *rows, int *width)
    {
        SpringtailPlanState ps{state};
        get_fdw_mgr()->fdw_get_rel_size_x(&ps, qual_list, join_quals, rows, width);
    }

    void 
    fdw_set_qual_state(List* state, int i, bool ignore)
    {
        SpringtailPlanState ps{state};
        ps.set_qual_state(i, ignore);
    }

    bool
    fdw_is_qual_ignored(List* state, int i)
    {
        SpringtailPlanState ps{state};
        return ps.get_qual_state(i) == 0?true:false;
    }

    void* 
    fdw_create_scan_state(List* planstate, List *qual_list, List* join_quals)
    {
        SpringtailPlanState ps{planstate};
        return get_fdw_mgr()->create_scan_state(&ps, qual_list, join_quals);
    }

    void 
    fdw_delete_scan_state(void *state)
    {
        delete static_cast<PgFdwState*>(state);
    }
}

