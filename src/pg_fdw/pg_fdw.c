#include <sys/stat.h>
#include <unistd.h>

#include <pg_fdw/pg_fdw.h>

#include "postgres.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
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
#include "utils/guc.h"

PG_MODULE_MAGIC;

// define from storage/constants.hh
#define INVALID_TABLE 0

// exposed from and defined in multicorn_util.c
extern List *
multicorn_getForeignPaths(PlannerInfo *root,
                          RelOptInfo *baserel,
                          Oid foreigntableid,
                          SpringtailPlanState *planstate);

extern void
multicorn_getRelSize(PlannerInfo *root,
                     RelOptInfo *baserel,
                     Oid foreigntableid,
                     SpringtailPlanState *planstate);

extern List *
multicorn_buildSimpleQualList(ForeignScanState *node);

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


/** Split uint64 into 2 uint32s function definition */
static void
split_uint64(uint64_t input, uint32_t *low, uint32_t *high)
{
    *low = (uint32_t)(input & 0xFFFFFFFF);          // Extract the lower 32 bits
    *high = (uint32_t)((input >> 32) & 0xFFFFFFFF); // Extract the upper 32 bits
}

/** Combine 2 uint32s into a uint64 function definition */
static uint64_t
combine_uint32(uint32_t low, uint32_t high)
{
    return ((uint64_t)high << 32) | low; // Combine the upper 32 bits and lower 32 bits
}


/** Transaction commit/abort callback */
static void
fdw_xact_callback(XactEvent event, void *arg)
{
    FullTransactionId pg_xid = GetCurrentFullTransactionIdIfAny();
    if (!FullTransactionIdIsValid(pg_xid)) {
        return;
    }

    switch (event)
    {
        case XACT_EVENT_COMMIT:
            elog(DEBUG1, "Transaction committed: %lu", pg_xid.value);
            fdw_commit_rollback(pg_xid.value, true);
            break;
        case XACT_EVENT_ABORT:
            elog(DEBUG1, "Transaction aborted: %lu", pg_xid.value);
            fdw_commit_rollback(pg_xid.value, false);
            break;
        default:
            break;
    }
}

/* Register the transaction callback */
void
_PG_init(void)
{
    // Register the transaction commit/rollback callback
    RegisterXactCallback(fdw_xact_callback, NULL);

    // Define the configuration file path
    char *fdw_config_file_path = NULL;
    DefineCustomStringVariable(
        "springtail_fdw.config_file_path",
        "Path to the FDW configuration file",
        NULL,
        &fdw_config_file_path,
        "/tmp/system.json",
        PGC_SUSET,
        0,
        NULL,
        NULL,
        NULL
    );

    // Initialize the FDW; springtail_init()
    fdw_init(fdw_config_file_path);
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
 * @brief Update baserel->rows, and possibly baserel->reltarget->width and
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
    int64_t tid = INVALID_TABLE;

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

    if (tid == INVALID_TABLE) {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                            errmsg("invalid option \"oid\""),
                            errhint("Invalid oid for table from table options")));
    }

    // create the plan state
    SpringtailPlanState *planstate = (SpringtailPlanState *)palloc0(sizeof(SpringtailPlanState));
    planstate->tid = tid;

    // Get the postgres transaction id, and create the internal state
    FullTransactionId pg_xid = GetCurrentFullTransactionId();
    planstate->pg_fdw_state = fdw_create_state(tid, pg_xid.value);

    // store the plan state in the baserel
    baserel->fdw_private = planstate;

    // get the estimate of the number of rows and width of the table
   multicorn_getRelSize(root, baserel, foreigntableid, planstate);
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
    SpringtailPlanState *state = (SpringtailPlanState *)baserel->fdw_private;

    // get the foreign paths -- call helper to set them up
    multicorn_getForeignPaths(root, baserel, foreigntableid, state);
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
    SpringtailPlanState *state = (SpringtailPlanState *)baserel->fdw_private;

    // postgres only supports integer value for lists, so to be safe we split the oid
    uint32_t low, high;
    split_uint64(state->tid, &low, &high);
    List *fdw_private = best_path->fdw_private;

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
 * @brief Initialize initial scan,
 * @param node
 * @param eflags
 */
static void
springtail_BeginForeignScan(ForeignScanState *node, int eflags)
{
    // extract plan state and set the fdw state on the scan node
    ForeignScan *fs = (ForeignScan *)node->ss.ps.plan;
    List *fdw_private = fs->fdw_private;
    SpringtailPlanState *planstate = (SpringtailPlanState *)linitial(fdw_private);

    node->fdw_state = planstate->pg_fdw_state;

    /* XXX Do nothing in EXPLAIN */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }

    // build a simple qual list against constants only
    List *qual_list = multicorn_buildSimpleQualList(node);

    /* NOTE from Multicorn multicorn.c */
    /* Those list must be copied, because their memory context can become */
	/* invalid during the execution (in particular with the cursor interface) */
    /* The copy occurs within the fdw_begin_scan() call */
    fdw_begin_scan(planstate->pg_fdw_state, planstate->target_list, qual_list, planstate->deparsed_pathkeys);

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

    // get next row, if true it was filled in successfully
    // if false we return the empty slot
    if (!fdw_iterate_scan(state, slot->tts_values, slot->tts_isnull)) {
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
    void *state = node->fdw_state;
    fdw_reset_scan(state);
}

/**
 * @brief End scan, cleanup state
 * @param node
 */
static void
springtail_EndForeignScan(ForeignScanState *node)
{
    // cleanup fdw_state
    void *state = node->fdw_state;
    fdw_end_scan(state);
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

    /* Apply restrictions for LIMIT TO and EXCEPT */
    if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO) {
        limit_to_list = true;
        table_list = stmt->table_list;
    } else if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT) {
        except_list = true;
        table_list = stmt->table_list;
    }

    return fdw_import_foreign_schema(server->servername, stmt->remote_schema, table_list, except_list, limit_to_list);
}
