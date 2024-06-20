#include <pg_fdw/pg_fdw_mgr.hh>

/** Wrapper around PgFdwMgr class for use in C code */

using namespace springtail;

extern "C" {
    #include <postgres.h>

    /** Get PgFdwMgr singleton */
    PgFdwMgr *
    get_fdw_mgr() {
        return PgFdwMgr::get_instance();
    }

    /** Begin scan wrapper */
    void *
    fdw_begin_scan(uint64_t tid) {
        return get_fdw_mgr()->fdw_begin(tid);
    }

    /** Iterate scan wrapper */
    bool
    fdw_iterate_scan(void *state, Datum *values, bool *nulls) {
        if (state) {
            return get_fdw_mgr()->fdw_iterate_scan(static_cast<PgFdwState*>(state), values, nulls);
        }
        return false;
    }

    /** End scan wrapper */
    void
    fdw_end_scan(void *state) {
        if (state) {
            get_fdw_mgr()->fdw_end(static_cast<PgFdwState*>(state));
        }
    }

    /** Reset scan wrapper */
    void
    fdw_reset_scan(void *state) {
        if (state) {
            get_fdw_mgr()->fdw_reset_scan(static_cast<PgFdwState*>(state));
        }
    }

    /** Import foreign schema wrapper */
    List *
    fdw_import_foreign_schema(const char *server, const char *schema, const List *table_list, bool exclude, bool limit) {
        return get_fdw_mgr()->fdw_import_foreign_schema(server, schema, table_list, exclude, limit);
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
}