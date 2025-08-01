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

/** Start call, pass database name and ddl connection flag */
void fdw_start(const char *db_name, bool ddl_connection);

/** Exit call */
void fdw_exit();

/** Create state */
void *fdw_create_state(uint64_t db_id, uint64_t tid, uint64_t pg_xid, uint64_t schema_xid);

/** Begin scan */
void *fdw_begin_scan(void *stat, int num_attrs, Form_pg_attribute* attrs,  List *target_list, List *qual_list);

/** End scan -- cleanup state */
void fdw_end_scan(void *state);

/** Iterate scan -- get next row */
bool fdw_iterate_scan(void *state, Datum *values, bool *nulls, bool *eos);

/** Reset scan */
void fdw_reset_scan(void *state, List *qual_list);

/** Import foreign schema */
List *fdw_import_foreign_schema(const char *server, const char *schema,
                                const List *table_list, bool exclude, bool limit,
                                uint64_t db_id, const char *db_name, uint64_t schema_xid);

/** Commit or rollback a transaction, remove the XID mappings */
void fdw_commit_rollback(uint64_t pg_xid, bool commit);


//// Called from path_util.c

/** Helper to get estimate of row width/number of rows */
void fdw_get_rel_size(SpringtailPlanState *state, List *target_list, List *qual_list, double *rows, int *width);

/** Helper return sub-list of sortable columns if table is sortable by sort group */
List *fdw_can_sort(SpringtailPlanState *state, List *sortgroup);

/** Helper to get list of path keys (key name, num rows) */
List *fdw_get_path_keys(SpringtailPlanState *state);

/** Explain scan */
void fdw_explain_scan(ForeignScanState *node, struct ExplainState *es);

#ifdef __cplusplus
}
#endif
