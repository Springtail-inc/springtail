#include <pg_fdw/pg_fdw_mgr.hh>

/** Wrapper around PgFdwMgr class for use in C code */

using namespace springtail::pg_fdw;

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
    fdw_create_state(uint64_t tid, uint64_t pg_xid)
    {
        return get_fdw_mgr()->fdw_create_state(tid, pg_xid);
    }

    /** Begin scan wrapper */
    void
    fdw_begin_scan(void *state, List *target_list, List *qual_list, List *sortgroup)
    {
        if (state) {
            return get_fdw_mgr()->fdw_begin_scan(static_cast<PgFdwState*>(state), target_list, qual_list, sortgroup);
        }
    }

    /** Iterate scan wrapper */
    bool
    fdw_iterate_scan(void *state, int num_attrs, Form_pg_attribute *attrs,
                     Datum *values, bool *nulls, bool *eos)
    {
        if (state) {
            return get_fdw_mgr()->fdw_iterate_scan(static_cast<PgFdwState*>(state), num_attrs, attrs, values, nulls, eos);
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
}