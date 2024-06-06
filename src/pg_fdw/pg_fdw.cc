#include <sys/stat.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

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
//#include "optimizer/var.h"
#include "utils/memutils.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

typedef struct SpringtailFdwEState {
    uint64_t xid;
    uint64_t oid;
} SpringtailFdwState;

typedef struct SpringtailFdwTableOptions {
    uint64_t oid;
} SpringtailFdwTableOptions;

/*
 * SQL functions
 */
extern Datum springtail_fdw_handler(PG_FUNCTION_ARGS);
extern Datum springtail_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(springtail_fdw_handler);
PG_FUNCTION_INFO_V1(springtail_fdw_validator);

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


// Split function definition
void split_uint64(uint64_t input, uint32_t *low, uint32_t *high) {
    *low = (uint32_t)(input & 0xFFFFFFFF);          // Extract the lower 32 bits
    *high = (uint32_t)((input >> 32) & 0xFFFFFFFF); // Extract the upper 32 bits
}

// Combine function definition
uint64_t combine_uint32(uint32_t low, uint32_t high) {
    return ((uint64_t)high << 32) | low; // Combine the upper 32 bits and lower 32 bits
}

/*
 * Foreign-data wrapper handler function
 */
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

	/* Support functions for EXPLAIN/ANALYZE */
    fdwroutine->ExplainForeignScan = springtail_ExplainForeignScan;
    fdwroutine->AnalyzeForeignTable = springtail_AnalyzeForeignTable;

    PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER
 * USER MAPPING or FOREIGN TABLE that uses springtail_fdw.
 */
Datum
springtail_fdw_validator(PG_FUNCTION_ARGS)
{
    /* no-op */
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
void
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
    // store the oid in the fdw_private field of the baserel
    ForeignTable *ft = GetForeignTable(foreigntableid);
    ListCell *cell;
    int64_t oid = -1;
    foreach(cell, ft->options) {
        DefElem *def = lfirst_node(DefElem, cell);
        if (strcmp("oid", def->defname) == 0) {
            oid = defGetInt64(def);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                            errmsg("invalid option \"%s\"", def->defname),
                            errhint("Invalid option for table from table options")));
        }
    }

    if (oid == -1) {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                            errmsg("invalid option \"oid\""),
                            errhint("Invalid oid for table from table options")));
    }

    SpringtailFdwTableOptions *opts = (SpringtailFdwTableOptions *)palloc(sizeof(SpringtailFdwTableOptions));
    opts->oid = oid;
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
void
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
ForeignScan *
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
    split_uint64(opts->oid, &low, &high);
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
void
springtail_BeginForeignScan(ForeignScanState *node, int eflags)
{
    // allocate and initialize state
    SpringtailFdwState *state = (SpringtailFdwState *)palloc0(sizeof(SpringtailFdwState));
    node->fdw_state = state;

    // extract oid from fdw_private
    ForeignScan *fs = (ForeignScan *)node->ss.ps.plan;
    uint32_t low = intVal(linitial(fs->fdw_private));
    uint32_t high = intVal(lsecond(fs->fdw_private));

    state->oid = combine_uint32(low, high);

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
TupleTableSlot *
springtail_IterateForeignScan(ForeignScanState *node)
{
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);

    // SpringtailFdwState *state = (SpringtailFdwState *)node->fdw_state;

    // slot->tts-isnull[i] = false/true
    // slot->tts_values[i] = Datum (Int32GetDatum())

    /* E.g
        slot->tts_isnull[0] = false;
        slot->tts_values[0] = Int32GetDatum(state->current);
        ExecStoreVirtualTuple(slot);
        incr state
    */

    return slot;
}

/**
 * @brief Restart scan from beginning
 * @param node
 */
void
springtail_ReScanForeignScan(ForeignScanState *node)
{
    //SpringtailFdwState *state = (SpringtailFdwState*)node->fdw_state;
    // reset state to beginning of table
}

/**
 * @brief End scan, cleanup state
 * @param node
 */
void
springtail_EndForeignScan(ForeignScanState *node)
{

}

void
springtail_ExplainForeignScan(ForeignScanState *node, ExplainState *es)
{

}

bool
springtail_AnalyzeForeignTable(Relation relation,
                               AcquireSampleRowsFunc *func,
                               BlockNumber *totalpages)
{
    return false;
}

#ifdef __cplusplus
}
#endif