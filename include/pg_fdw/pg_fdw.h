#pragma once
/** C wrappers for C++ PgFdwMgr singleton access */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <postgres.h>

/** Forward definition of PgFdwMgr */
typedef struct PgFdwMgr PgFdwMgr;

/** Forward definition of List */
typedef struct List List;

/** Get fdw mgr singleton instance */
PgFdwMgr* get_fdw_mgr();

/** Begin scan */
void *fdw_begin_scan(PgFdwMgr *instance, uint64_t tid);

/** End scan -- cleanup state */
void fdw_end_scan(PgFdwMgr *instance, void *state);

/** Iterate scan -- get next row */
bool fdw_iterate_scan(PgFdwMgr *instance, void *state, Datum *values, bool *nulls);

/** Reset scan */
void fdw_reset_scan(PgFdwMgr *instance, void *state);

/** Import foreign schema */
List *fdw_import_foreign_schema(PgFdwMgr *instance, const char *server, const char *schema,
                                const List *table_list, bool exclude, bool limit);

#ifdef __cplusplus
}
#endif