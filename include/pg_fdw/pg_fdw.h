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

/** Init call, pass in config file path */
void fdw_init(const char *config_file);

/** Create state */
void *fdw_create_state(uint64_t tid, uint64_t pg_xid);

/** Begin scan */
void *fdw_begin_scan(void *stat, List *target_list, List *qual_list, List *sortgroup);

/** End scan -- cleanup state */
void fdw_end_scan(void *state);

/** Iterate scan -- get next row */
bool fdw_iterate_scan(void *state, int num_attrs, Form_pg_attribute *attrs, Datum *values, bool *nulls);

/** Reset scan */
void fdw_reset_scan(void *state);

/** Import foreign schema */
List *fdw_import_foreign_schema(const char *server, const char *schema,
                                const List *table_list, bool exclude, bool limit);

/** Commit or rollback a transaction, remove the XID mappings */
void fdw_commit_rollback(uint64_t pg_xid, bool commit);


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