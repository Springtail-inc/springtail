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
    fdw_begin_scan(PgFdwMgr *instance, uint64_t tid) {
        if (instance) {
            return instance->fdw_begin(tid);
        }
        return nullptr;
    }

    /** Iterate scan wrapper */
    bool
    fdw_iterate_scan(PgFdwMgr *instance, void *state, Datum *values, bool *nulls) {
        if (instance && state) {
            return instance->fdw_iterate_scan(static_cast<PgFdwState*>(state), values, nulls);
        }
        return false;
    }

    /** End scan wrapper */
    void
    fdw_end_scan(PgFdwMgr *instance, void *state) {
        if (instance && state) {
            instance->fdw_end(static_cast<PgFdwState*>(state));
        }
    }

    /** Reset scan wrapper */
    void
    fdw_reset_scan(PgFdwMgr *instance, void *state) {
        if (instance && state) {
            instance->fdw_reset_scan(static_cast<PgFdwState*>(state));
        }
    }
}