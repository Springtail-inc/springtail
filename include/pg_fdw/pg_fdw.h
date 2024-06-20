#pragma once
/** C wrappers for C++ PgFdwMgr singleton access */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

#include <pg_fdw/pg_fdw_common.h>

/** Forward definition of PgFdwMgr */
typedef struct PgFdwMgr PgFdwMgr;

/** Get fdw mgr singleton instance */
PgFdwMgr* get_fdw_mgr();

/** Begin scan */
void *fdw_begin_scan(uint64_t tid);

/** End scan -- cleanup state */
void fdw_end_scan(void *state);

/** Iterate scan -- get next row */
bool fdw_iterate_scan(void *state, Datum *values, bool *nulls);

/** Reset scan */
void fdw_reset_scan(void *state);

/** Import foreign schema */
List *fdw_import_foreign_schema(const char *server, const char *schema,
                                const List *table_list, bool exclude, bool limit);


//// Called from path_util.c

/** Helper to get estimate of row width/number of rows */
void fdw_get_rel_size(SpringtailPlanState *state, List *target_list, List *qual_list, double *rows, int *width);

/** Helper return sub-list of sortable columns if table is sortable by sort group */
List *fdw_can_sort(SpringtailPlanState *state, List *sortgroup);

/** Helper to get list of path keys (key name, num rows) */
List *fdw_get_path_keys(SpringtailPlanState *state);

#ifdef __cplusplus
}
#endif