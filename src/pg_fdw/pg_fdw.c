#include <sys/stat.h>
#include <unistd.h>

#include <pg_fdw/pg_fdw.h>

#include "postgres.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/memutils.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

typedef struct SpringtailFdwTableOptions {
    uint64_t tid;
} SpringtailFdwTableOptions;

/*
 * FDW callback routines
 */
static void springtail_GetForeignRelSize(PlannerInfo *root,
                                   RelOptInfo *baserel,
                                   Oid foreigntableid);

static void springtail_GetForeignPaths(PlannerInfo *root,
                                 RelOptInfo *baserel,
                                 Oid foreigntableid);

static ForeignScan *springtail_GetForeignPlan(PlannerInfo *root,
                                        RelOptInfo *baserel,
                                        Oid foreigntableid,
                                        ForeignPath *best_path,
                                        List *tlist,
                                        List *scan_clauses,
                                        Plan *outer_plan);

static void springtail_ExplainForeignScan(ForeignScanState *node, ExplainState *es);

static void springtail_BeginForeignScan(ForeignScanState *node, int eflags);

static TupleTableSlot *springtail_IterateForeignScan(ForeignScanState *node);

static void springtail_ReScanForeignScan(ForeignScanState *node);

static void springtail_EndForeignScan(ForeignScanState *node);

static bool springtail_AnalyzeForeignTable(Relation relation,
                                     AcquireSampleRowsFunc *func,
                                     BlockNumber *totalpages);

static List *springtail_ImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);


// Split function definition
void
split_uint64(uint64_t input, uint32_t *low, uint32_t *high)
{
    *low = (uint32_t)(input & 0xFFFFFFFF);          // Extract the lower 32 bits
    *high = (uint32_t)((input >> 32) & 0xFFFFFFFF); // Extract the upper 32 bits
}

// Combine function definition
uint64_t
combine_uint32(uint32_t low, uint32_t high)
{
    return ((uint64_t)high << 32) | low; // Combine the upper 32 bits and lower 32 bits
}

/*
 * Foreign-data wrapper handler function
 * Exported function to create the FDW handler
 */
PG_FUNCTION_INFO_V1(springtail_fdw_handler);
Datum
springtail_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    /* Functions for scanning foreign tables */
    fdwroutine->GetForeignRelSize = springtail_GetForeignRelSize;
    fdwroutine->GetForeignPaths = springtail_GetForeignPaths;
    fdwroutine->GetForeignPlan = springtail_GetForeignPlan;
    fdwroutine->BeginForeignScan = springtail_BeginForeignScan;
    fdwroutine->IterateForeignScan = springtail_IterateForeignScan;
    fdwroutine->ReScanForeignScan = springtail_ReScanForeignScan;
    fdwroutine->EndForeignScan = springtail_EndForeignScan;

    fdwroutine->ImportForeignSchema = springtail_ImportForeignSchema;

    /* Support functions for EXPLAIN/ANALYZE */
    fdwroutine->ExplainForeignScan = springtail_ExplainForeignScan;
    fdwroutine->AnalyzeForeignTable = springtail_AnalyzeForeignTable;

    PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER
 * Exported function to validate the options
 */
PG_FUNCTION_INFO_V1(springtail_fdw_validator);
Datum
springtail_fdw_validator(PG_FUNCTION_ARGS)
{
    /* no-op */
    ArrayType  *options;
    Oid         catalog = InvalidOid;

    if (PG_NARGS() == 2)
    {
        options = PG_GETARG_ARRAYTYPE_P(0);
        catalog = PG_GETARG_OID(1);
    }
    PG_RETURN_VOID();
}

/**
 * @brief Update baserel->rows, and possibly baserel->width and
 *        baserel->tuples, with an estimated result set size for a
 *        scan of baserel
 * @param root
 * @param baserel
 * @param foreigntableid
 */
static void
springtail_GetForeignRelSize(PlannerInfo *root,
                             RelOptInfo *baserel,
                             Oid foreigntableid)
{
    /* Could do some sanity checks on the table.
       E.g.,
       Relation rel = table_open(foreigntableid, NoLock);
       if (rel->rd_att->natts != 1) {
               ereport(ERROR,
                       errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
                       errmsg("incorrect schema for tutorial_fdw table %s: table must have exactly one column", NameStr(rel->rd_rel->relname)));
       }
       Oid typid = rel->rd_att->attrs[0].atttypid;
       if (typid != INT4OID) {
               ereport(ERROR,
                       errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                       errmsg("incorrect schema for tutorial_fdw table %s: table column must have type int", NameStr(rel->rd_rel->relname)));
       }
       table_close(rel, NoLock);

       baserel->fdw_private = opts; // can store private data here
*/

    // Can store options on create table and access them here
    ForeignTable *ft = GetForeignTable(foreigntableid);
    ListCell *cell;
    int64_t tid = -1; // XXX is there an invalid TID?

    // look through the options (provided during CREATE FOREIGN TABLE)
    foreach(cell, ft->options) {
        DefElem *def = lfirst_node(DefElem, cell);
        if (strcmp("tid", def->defname) == 0) {
            char *tidstr = defGetString(def);
            tid = atoll(tidstr);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                            errmsg("invalid option \"%s\"", def->defname),
                            errhint("Invalid option for table from table options")));
        }
    }

    if (tid == -1) {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                            errmsg("invalid option \"oid\""),
                            errhint("Invalid oid for table from table options")));
    }

    // store the tid in the fdw_private field of the baserel
    SpringtailFdwTableOptions *opts = (SpringtailFdwTableOptions *)palloc(sizeof(SpringtailFdwTableOptions));
    opts->tid = tid;
    baserel->fdw_private = opts;
}

/**
 * @brief Update baserel->pathlist to include ways of accessing baserel
 *        Typically adds ForeignPath *s created with create_foreignscan_path
 *        to baserel using add_path. Each added path will include a cost
 *        estimate, a rows estimate and potentially outer dependencies.
 * @param root
 * @param baserel
 * @param foreigntableid
 */
static void
springtail_GetForeignPaths(PlannerInfo *root,
                           RelOptInfo *baserel,
                           Oid foreigntableid)
{
    Path *path = (Path *)create_foreignscan_path(root, baserel,
        NULL,              /* default pathtarget */
        baserel->rows,     /* rows */
        1,                 /* startup cost */
        1 + baserel->rows, /* total cost */
        NIL,               /* no pathkeys */
        NULL,              /* no required outer relids */
        NULL,              /* no fdw_outerpath */
        NIL);              /* no fdw_private */
    add_path(baserel, path);
}

/**
 * @brief Creates a ForeignScan * for the given ForeignPath *.
 *        base_path was created by GetForeignPaths and has been chosen
 *        by the planner as the access path for the query.
 * @param root
 * @param baserel
 * @param foreigntableid
 * @param best_path
 * @param tlist
 * @param scan_clauses
 * @param outer_plan
 * @return ForeignScan*
 */
static ForeignScan *
springtail_GetForeignPlan(PlannerInfo *root,
                          RelOptInfo *baserel,
                          Oid foreigntableid,
                          ForeignPath *best_path,
                          List *tlist,
                          List *scan_clauses,
                          Plan *outer_plan)
{
    SpringtailFdwTableOptions *opts = (SpringtailFdwTableOptions *)baserel->fdw_private;

    // postgres only supports integer value for lists, so to be safe we split the oid
    uint32_t low, high;
    split_uint64(opts->tid, &low, &high);
    List *fdw_private = list_make2(makeInteger(low), makeInteger(high));

    /* build a List * of the clause field of the passed in scan_clauses,
       which are a list of RestrictInfo * nodes. */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    return make_foreignscan(tlist,
        scan_clauses,
        baserel->relid,
        NIL, /* no expressions we will evaluate */
        fdw_private, /* private data */
        NIL, /* no custom tlist; our scan tuple looks like tlist */
        NIL, /* no quals we will recheck */
        outer_plan);
}

/**
 * @brief Initialize initial state; allocate and init
 * @param node
 * @param eflags
 */
static void
springtail_BeginForeignScan(ForeignScanState *node, int eflags)
{
    // extract tid from fdw_private
    ForeignScan *fs = (ForeignScan *)node->ss.ps.plan;
    uint32_t low = intVal(linitial(fs->fdw_private));
    uint32_t high = intVal(lsecond(fs->fdw_private));
    uint64_t tid = combine_uint32(low, high);

    // allocate and initialize state
    struct PgFdwMgr *mgr = get_fdw_mgr();

    // allocate and initialize state
    node->fdw_state = fdw_begin_scan(mgr, tid);

    /* Do nothing in EXPLAIN */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }

    return;
}

/**
 * @brief Retrieve next row from the result, or clear tuple slot to indicate EOF
 * @param node
 * @return TupleTableSlot*
 */
static TupleTableSlot *
springtail_IterateForeignScan(ForeignScanState *node)
{
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);

    void *state = node->fdw_state;
    struct PgFdwMgr *mgr = get_fdw_mgr();

    // get next row, if true it was filled in successfully
    // if false we return the empty slot
    if (!fdw_iterate_scan(mgr, state, slot->tts_values, slot->tts_isnull)) {
        return slot;
    }

    ExecStoreVirtualTuple(slot);
    return slot;
}

/**
 * @brief Restart scan from beginning
 * @param node
 */
static void
springtail_ReScanForeignScan(ForeignScanState *node)
{
    // reset state to beginning of table
    struct PgFdwMgr *mgr = get_fdw_mgr();
    void *state = node->fdw_state;
    fdw_reset_scan(mgr, state);
}

/**
 * @brief End scan, cleanup state
 * @param node
 */
static void
springtail_EndForeignScan(ForeignScanState *node)
{
    // cleanup fdw_state
    struct PgFdwMgr *mgr = get_fdw_mgr();
    void *state = node->fdw_state;
    fdw_end_scan(mgr, state);
    node->fdw_state = NULL;
}

static void
springtail_ExplainForeignScan(ForeignScanState *node, ExplainState *es)
{

}

static bool
springtail_AnalyzeForeignTable(Relation relation,
                               AcquireSampleRowsFunc *func,
                               BlockNumber *totalpages)
{
    return false;
}


static List *
springtail_ImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    ForeignServer *server = GetForeignServer(serverOid);
    List      *commands = NIL;
    List      *table_list = NIL;
    bool       limit_to_list = false;
    bool       except_list = false;

    PgFdwMgr *mgr = get_fdw_mgr();

    /* Apply restrictions for LIMIT TO and EXCEPT */
	if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO) {
        limit_to_list = true;
        table_list = stmt->table_list;
    } else if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT) {
        except_list = true;
        table_list = stmt->table_list;
    }

    return fdw_import_foreign_schema(mgr, server->servername, stmt->remote_schema, table_list, except_list, limit_to_list);
}