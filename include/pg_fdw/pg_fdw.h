#pragma once
/** C wrappers for C++ PgFdwMgr singleton access */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <postgres.h>

/** Forward definition of PgFdwMgr */
struct PgFdwMgr;

/** Get fdw mgr singleton instance */
struct PgFdwMgr* get_fdw_mgr();

/** Begin scan */
void *fdw_begin_scan(struct PgFdwMgr *instance, uint64_t tid);

/** End scan -- cleanup state */
void fdw_end_scan(struct PgFdwMgr *instance, void *state);

/** Iterate scan -- get next row */
bool fdw_iterate_scan(struct PgFdwMgr *instance, void *state, Datum *values, bool *nulls);

/** Reset scan */
void fdw_reset_scan(struct PgFdwMgr *instance, void *state);

#ifdef __cplusplus
}
#endif