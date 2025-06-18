
/**
* Heavily borrowed from Multicorn / multicorn2 FDW wrapper source code.
* https://github.com/pgsql-io/multicorn2/blob/main/src
* Original license below:
*
* Portions: Copyright (c) 2021-2024, Lussier
* Portions: Copyright (c) 2013, Kozea
*
* Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.
*
* IN NO EVENT SHALL LUSSIER OR KOZEA BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF KOZEA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*
* KOZEA SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND KOZEA HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
*/

#include "postgres.h"
#include "optimizer/optimizer.h"
#include "optimizer/clauses.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/subselect.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_operator.h"
#include "mb/pg_wchar.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "parser/parsetree.h"
#include "fmgr.h"
#include "common/hashfn.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "pg_config.h"

#include <pg_fdw/pg_fdw.h>

/*
 *	Test wheter an attribute identified by its relid and attno
 *	is present in a list of restrictinfo
 */
static bool
isAttrInRestrictInfo(Index relid, AttrNumber attno, RestrictInfo *restrictinfo)
{
    List	   *vars = pull_var_clause((Node *) restrictinfo->clause,
                                        PVC_RECURSE_AGGREGATES|
                                        PVC_RECURSE_PLACEHOLDERS);
    ListCell   *lc;

    foreach(lc, vars)
    {
        Var		   *var = (Var *) lfirst(lc);

        if (var->varno == relid && var->varattno == attno)
        {
            return true;
        }

    }
    return false;
}

static List *
clausesInvolvingAttr(Index relid, AttrNumber attnum,
                     EquivalenceClass *ec)
{
    List	   *clauses = NULL;

    /*
     * If there is only one member, then the equivalence class is either for
     * an outer join, or a desired sort order. So we better leave it
     * untouched.
     */
    if (ec->ec_members->length > 1)
    {
        ListCell   *ri_lc;

        foreach(ri_lc, ec->ec_sources)
        {
            RestrictInfo *ri = (RestrictInfo *) lfirst(ri_lc);

            if (isAttrInRestrictInfo(relid, attnum, ri))
            {
                clauses = lappend(clauses, ri);
            }
        }
    }
    return clauses;
}

static List *
findPaths(PlannerInfo *root,
          RelOptInfo *baserel,
          List *possiblePaths,
          int startupCost,
          List *apply_pathkeys,
          List *deparsed_pathkeys)
{
    List	   *result = NULL;
    ListCell   *lc;

    foreach(lc, possiblePaths)
    {
        List	   *item = lfirst(lc);
        List	   *attrnos = linitial(item);
        uint64_t    nbrows = ((Const *) lsecond(item))->constvalue;
        double		cost = (double)((Const *) lthird(item))->constvalue;
        List	   *allclauses = NULL;
        Bitmapset  *outer_relids = NULL;
        ListCell   *attno_lc;

        /* Armed with this knowledge, look for a join condition */
        /* matching the path list. */
        /* Every key must be present in either, a join clause or an */
        /* equivalence_class. */
        foreach(attno_lc, attrnos)
        {
            AttrNumber	attnum = lfirst_int(attno_lc);
            ListCell   *lc;
            List	   *clauses = NULL;

            /* Look in the equivalence classes. */
            foreach(lc, root->eq_classes)
            {
                EquivalenceClass *ec = (EquivalenceClass *) lfirst(lc);
                List	   *ec_clauses = clausesInvolvingAttr(baserel->relid,
                                                              attnum,
                                                              ec);

                clauses = list_concat(clauses, ec_clauses);
                if (ec_clauses != NIL)
                {
                    outer_relids = bms_union(outer_relids, ec->ec_relids);
                }
            }
            /* Do the same thing for the outer joins */
            foreach(lc, list_union(root->left_join_clauses,
                                   root->right_join_clauses))
            {
                Node *node = (Node *) lfirst(lc);

                RestrictInfo *ri;

#if PG_VERSION_NUM >= 160000
                OuterJoinClauseInfo *ojcinfo;

                if (nodeTag(node) != T_OuterJoinClauseInfo) //NOSONAR reason: third-party code
                {
                    elog(ERROR, "join clause was not a T_OuterJoinClauseInfo; but was a %d", nodeTag(node));
                    continue;
                }

                ojcinfo = (OuterJoinClauseInfo *) node;
                node = (Node *) ojcinfo->rinfo;
#endif

                if (nodeTag(node) != T_RestrictInfo) //NOSONAR reason: third-party code
                {
                    elog(ERROR, "join clause was not a T_RestrictInfo; but was a %d", nodeTag(node));
                    continue;
                }
                ri = (RestrictInfo *) node;

                if (isAttrInRestrictInfo(baserel->relid, attnum, ri)) //NOSONAR reason: third-party code
                {
                    clauses = lappend(clauses, ri);
                    outer_relids = bms_union(outer_relids,
                                             ri->outer_relids);

                }
            }
            /* We did NOT find anything for this key, bail out */
            if (clauses == NIL)
            {
                allclauses = NULL;
                break;
            }
            else
            {
                allclauses = list_concat(allclauses, clauses);
            }
        }
        /* Every key has a corresponding restriction, we can build */
        /* the parameterized path and add it to the plan. */
        if (allclauses != NIL)
        {
            Bitmapset  *req_outer = bms_difference(outer_relids,
                                         bms_make_singleton(baserel->relid));
            ParamPathInfo *ppi;
            ForeignPath *foreignPath;

            if (!bms_is_empty(req_outer))
            {
                ppi = makeNode(ParamPathInfo);
                ppi->ppi_req_outer = req_outer;
                ppi->ppi_rows = nbrows;
                ppi->ppi_clauses = list_concat(ppi->ppi_clauses, allclauses);
                /* Add a simple parameterized path */
                foreignPath = create_foreignscan_path(
                                                      root, baserel,
                                                       NULL,  /* default pathtarget */
                                                      nbrows,
                                                      startupCost,
                                                      (double)nbrows * baserel->reltarget->width * cost,
                                                      NIL, /* no pathkeys */
                                                      NULL,
                                                      NULL,
                                                      NULL);

                foreignPath->path.param_info = ppi;
                result = lappend(result, foreignPath);
            }
        }
    }
    return result;
}

static Expr *
multicorn_get_em_expr(EquivalenceClass *ec, RelOptInfo *rel)
{
    ListCell   *lc_em;

    foreach(lc_em, ec->ec_members)
    {
        EquivalenceMember *em = lfirst(lc_em);

        if (bms_equal(em->em_relids, rel->relids))
        {
            /*
             * If there is more than one equivalence member whose Vars are
             * taken entirely from this relation, we'll be content to choose
             * any one of those.
             */
            return em->em_expr;
        }
    }

    /* We didn't find any suitable equivalence class expression */
    return NULL;
}

/*
 * Deparse a list of PathKey and return a list of MulticornDeparsedSortGroup.
 * This function will return data iif all the PathKey belong to the current
 * foreign table.
 */
static List *
deparse_sortgroup(PlannerInfo *root, Oid foreigntableid, RelOptInfo *rel)
{
    List *result = NULL;
    ListCell   *lc;

    /* return empty list if no pathkeys for the PlannerInfo */
    if (! root->query_pathkeys)
        return NIL;

    foreach(lc,root->query_pathkeys)
    {
        PathKey *key = (PathKey *) lfirst(lc);
        DeparsedSortGroup *md = palloc0(sizeof(DeparsedSortGroup));
        EquivalenceClass *ec = key->pk_eclass;
        Expr *expr;
        bool found = false;

        if ((expr = multicorn_get_em_expr(ec, rel)))
        {
            md->reversed = (key->pk_strategy == BTGreaterStrategyNumber);
            md->nulls_first = key->pk_nulls_first;
            md->key = key;

            if (IsA(expr, Var))
            {
                Var *var = (Var *) expr;
                 //(Name) strdup(get_attname(foreigntableid, var->varattno, true));
                md->attname = get_attname(foreigntableid, var->varattno, true);
                md->attnum = var->varattno;
                found = true;
            }
            /* ORDER BY clauses having a COLLATE option will be RelabelType */
            else if (IsA(expr, RelabelType) &&
                    IsA(((RelabelType *) expr)->arg, Var))
            {
                Var *var = (Var *)((RelabelType *) expr)->arg;
                Oid collid = ((RelabelType *) expr)->resultcollid;

                if (collid == DEFAULT_COLLATION_OID) {
                    md->collate = NULL;
                } else {
                    md->collate = get_collation_name(collid); //(Name) strdup(get_collation_name(collid));
                }
                md->attname = get_attname(foreigntableid, var->varattno, true); //(Name) strdup(get_attname(foreigntableid, var->varattno, true));
                md->attnum = var->varattno;
                found = true;
            }
        }

        if (found)
            result = lappend(result, md);
        else
        {
            /* pfree() current entry */
            pfree(md);
            /* pfree() all previous entries */
            while ((lc = list_head(result)) != NULL)
            {
                md = (DeparsedSortGroup *) lfirst(lc);
                result = list_delete_ptr(result, md);
                pfree(md);
            }
            break;
        }
    }

    return result;
}

/*
 * Given a list of MulticornDeparsedSortGroup and a MulticornPlanState,
 * construct a list of PathKey and MulticornDeparsedSortGroup that belongs to
 * the FDW and that the FDW say it can enforce.
 */
static void
computeDeparsedSortGroup(List *deparsed,
        void *planstate,
        List **apply_pathkeys,
        List **deparsed_pathkeys)
{
    List		*sortable_fields = NULL;
    ListCell	*lc, *lc2;

    /* Both lists should be empty */
    Assert(*apply_pathkeys == NIL);
    Assert(*deparsed_pathkeys == NIL);

    /* Don't ask FDW if nothing to sort */
    if (deparsed == NIL)
        return;

    sortable_fields = fdw_can_sort(planstate, deparsed);

    /* Don't go further if FDW can't enforce any sort */
    if (sortable_fields == NIL)
        return;

    foreach(lc, sortable_fields)
    {
        DeparsedSortGroup *sortable_md = (DeparsedSortGroup *) lfirst(lc);
        foreach(lc2, deparsed)
        {
            DeparsedSortGroup *wanted_md = lfirst(lc2);

            if (sortable_md->attnum == wanted_md->attnum)
            {
                *apply_pathkeys = lappend(*apply_pathkeys, wanted_md->key);
                *deparsed_pathkeys = lappend(*deparsed_pathkeys, wanted_md);
            }
        }
    }
}

/* For support qual opnames, convert to enum */
static QualOpName
getOpEnum(char *opname)
{
    if (strcmp(opname, "=") == 0) {
        return EQUALS;
    } else if (strcmp(opname, "<>") == 0) {
        return NOT_EQUALS;
    } else if (strcmp(opname, "<") == 0) {
        return LESS_THAN;
    } else if (strcmp(opname, "<=") == 0) {
        return LESS_THAN_EQUALS;
    } else if (strcmp(opname, ">") == 0) {
        return GREATER_THAN;
    } else if (strcmp(opname, ">=") == 0) {
        return GREATER_THAN_EQUALS;
    }
    return UNSUPPORTED;
}

/*
 *	Build an opaque "qual" object.
 */
static BaseQual *
makeQual(AttrNumber varattno, char *opname, Expr *value, bool isarray,
         bool useOr)
{
    BaseQual *qual;

    elog(DEBUG3, "begin makeQual() opname '%s': type '%d'", opname, value->type);
    switch (value->type)
    {
        case T_Const:
            elog(DEBUG3, "T_Const");
            qual = palloc0(sizeof(ConstQual));
            qual->right_type = T_Const;
            qual->typeoid = ((Const *) value)->consttype;
            ((ConstQual *) qual)->value = ((Const *) value)->constvalue;
            ((ConstQual *) qual)->isnull = ((Const *) value)->constisnull;
            break;
        case T_Var:
            elog(DEBUG3, "T_Var");
            qual = palloc0(sizeof(VarQual));
            qual->right_type = T_Var;
            ((VarQual *) qual)->rightvarattno = ((Var *) value)->varattno;
            break;
        default:
            elog(DEBUG3, "default");
            qual = palloc0(sizeof(ParamQual));
            qual->right_type = T_Param;
            ((ParamQual *) qual)->expr = value;
            qual->typeoid = InvalidOid;
            break;
    }
    qual->varattno = varattno;
    qual->opname = opname;
    qual->op = getOpEnum(opname);
    qual->isArray = isarray;
    qual->useOr = useOr;
    elog(DEBUG3, "makeQual() opname '%s': right_type '%d', op:'%d'", opname, qual->right_type, qual->op);
    return qual;
}

static char *
getOperatorString(Oid opoid)
{
    HeapTuple	tp;
    Form_pg_operator operator;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for operator %u", opoid);
    operator = (Form_pg_operator) GETSTRUCT(tp);
    ReleaseSysCache(tp);
    return NameStr(operator->oprname);
}

/*
 * Returns the node of interest from a node.
 */
static Node *
unnestClause(Node *node)
{
    switch (node->type)
    {
        case T_RelabelType:
            return (Node *) ((RelabelType *) node)->arg;
        case T_ArrayCoerceExpr:
            return (Node *) ((ArrayCoerceExpr *) node)->arg;
        default:
            return node;
    }
}

/*
 * Swaps the operands if needed / possible, so that left is always a node
 * belonging to the baserel and right is either:
 *	- a Const
 *	- a Param
 *	- a Var from another relation
 */
static ScalarArrayOpExpr *
canonicalScalarArrayOpExpr(ScalarArrayOpExpr *opExpr,
                           Relids base_relids)
{
    Oid			operatorid = opExpr->opno;
    Node	   *l,
               *r;
    ScalarArrayOpExpr *result = NULL;
    HeapTuple	tp;
    Form_pg_operator op;

    /* Only treat binary operators for now. */
    if (list_length(opExpr->args) == 2)
    {
        l = unnestClause(list_nth(opExpr->args, 0));
        r = unnestClause(list_nth(opExpr->args, 1));
        tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(operatorid));
        if (!HeapTupleIsValid(tp))
            elog(ERROR, "cache lookup failed for operator %u", operatorid);
        op = (Form_pg_operator) GETSTRUCT(tp);
        ReleaseSysCache(tp);
        if (IsA(l, Var) &&bms_is_member(((Var *) l)->varno, base_relids)
            && ((Var *) l)->varattno >= 1)
        {
            result = makeNode(ScalarArrayOpExpr);
            result->opno = operatorid;
            result->opfuncid = op->oprcode;
            result->useOr = opExpr->useOr;
            result->args = lappend(result->args, l);
            result->args = lappend(result->args, r);
            result->location = opExpr->location;

        }
    }
    return result;
}

static void
swapOperandsAsNeeded(Node **left, Node **right, Oid *opoid,
                     Relids base_relids)
{
    HeapTuple	tp;
    Form_pg_operator op;
    Node	   *l = *left,
               *r = *right;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(*opoid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for operator %u", *opoid);
    op = (Form_pg_operator) GETSTRUCT(tp);
    ReleaseSysCache(tp);
    /* Right is already a var. */
    /* If "left" is a Var from another rel, and right is a Var from the */
    /* target rel, swap them. */
    /* Same thing is left is not a var at all. */
    /* To swap them, we have to lookup the commutator operator. */
    if (IsA(r, Var))
    {
        Var		   *rvar = (Var *) r;

        if (!IsA(l, Var) ||
            (!bms_is_member(((Var *) l)->varno, base_relids) &&
             bms_is_member(rvar->varno, base_relids)))
        {
            /* If the operator has no commutator operator, */
            /* bail out. */
            if (op->oprcom == 0)
            {
                return;
            }
            {
                *left = r;
                *right = l;
                *opoid = op->oprcom;
            }
        }
    }
}


/*
 * Swaps the operands if needed / possible, so that left is always a node
 * belonging to the baserel and right is either:
 *	- a Const
 *	- a Param
 *	- a Var from another relation
 */
static OpExpr *
canonicalOpExpr(OpExpr *opExpr, Relids base_relids)
{
    Oid			operatorid = opExpr->opno;
    Node	   *l,
               *r;
    OpExpr	   *result = NULL;

    /* Only treat binary operators for now. */
    if (list_length(opExpr->args) == 2)
    {
        l = unnestClause(list_nth(opExpr->args, 0));
        r = unnestClause(list_nth(opExpr->args, 1));
        swapOperandsAsNeeded(&l, &r, &operatorid, base_relids);
        if (IsA(l, Var) &&bms_is_member(((Var *) l)->varno, base_relids)
            && ((Var *) l)->varattno >= 1)
        {
            result = (OpExpr *) make_opclause(operatorid,
                                              opExpr->opresulttype,
                                              opExpr->opretset,
                                              (Expr *) l, (Expr *) r,
                                              opExpr->opcollid,
                                              opExpr->inputcollid);
        }
    }
    return result;
}

/*
 *	Build an intermediate value representation for an OpExpr,
 *	and append it to the corresponding list (quals, or params).
 *
 *	The quals list consist of list of the form:
 *
 *	- Const key: the column index in the cinfo array
 *	- Const operator: the operator representation
 *	- Var or Const value: the value.
 */
static void
extractClauseFromOpExpr(PlannerInfo *root,
                        Relids base_relids,
                        OpExpr *op,
                        List **quals)
{
    Var		   *left;
    Expr	   *right;

    /* Use a "canonical" version of the op expression, to ensure that the */
    /* left operand is a Var on our relation. */
    op = canonicalOpExpr(op, base_relids);
    if (op)
    {
        left = list_nth(op->args, 0);
        right = list_nth(op->args, 1);
        /* Do not add it if it either contains a mutable function, or makes */
        /* self references in the right hand side. */
        if (!(contain_volatile_functions((Node *) right) ||
              bms_is_subset(base_relids, pull_varnos(root, (Node *) right))))
        {
            *quals = lappend(*quals, makeQual(left->varattno,
                                              getOperatorString(op->opno),
                                              right, false, false));
        }
    }
}

static void
extractClauseFromScalarArrayOpExpr(PlannerInfo *root,
                                   Relids base_relids,
                                   ScalarArrayOpExpr *op,
                                   List **quals)
{
    Var		   *left;
    Expr	   *right;

    op = canonicalScalarArrayOpExpr(op, base_relids);
    if (op)
    {
        left = list_nth(op->args, 0);
        right = list_nth(op->args, 1);
        if (!(contain_volatile_functions((Node *) right) ||
              bms_is_subset(base_relids, pull_varnos(root, (Node *) right)))) // XXX planner info missing
        {
            *quals = lappend(*quals, makeQual(left->varattno,
                                              getOperatorString(op->opno),
                                              right, true,
                                              op->useOr));
        }
    }
}

/*
 *	Convert a "NullTest" (IS NULL, or IS NOT NULL)
 *	to a suitable intermediate representation.
 */
static void
extractClauseFromNullTest(PlannerInfo *root,
                          Relids base_relids,
                          NullTest *node,
                          List **quals)
{
    if (node->argisrow) {
        // this is a null check for entire row, not handled right now
        return;
    }

    if (IsA(node->arg, Var))
    {
        Var		   *var = (Var *) node->arg;
        BaseQual   *result;
        char	   *opname = NULL;

        if (var->varattno < 1)
        {
            return;
        }
        if (node->nulltesttype == IS_NULL)
        {
            opname = "=";
        }
        else
        {
            opname = "<>";
        }
        result = makeQual(var->varattno, opname,
                          (Expr *) makeNullConst(INT4OID, -1, InvalidOid),
                          false,
                          false);
        *quals = lappend(*quals, result);
    }
}

static void
extractClauseFromBoolTest(PlannerInfo *root,
                          Relids base_relids,
                          BooleanTest *node,
                          List **quals)
{
    if (IsA(node->arg, Var))
    {
        Var		   *var = (Var *) node->arg;
        BaseQual   *result;
        char	   *opname = NULL;

        if (var->varattno < 1)
        {
            return;
        }
        switch (node->booltesttype) {
            case IS_NOT_FALSE:
            case IS_TRUE:
                opname = "=";
                break;
            case IS_FALSE:
            case IS_NOT_TRUE:
                opname = "<>";
                break;
            case IS_UNKNOWN:
                opname = "=";
                break;
            case IS_NOT_UNKNOWN:
                opname = "<>";
                break;
        }

        if (node->booltesttype == IS_UNKNOWN || node->booltesttype == IS_NOT_UNKNOWN) {
            result = makeQual(var->varattno, opname,
                              (Expr *) makeNullConst(INT4OID, -1, InvalidOid),
                              false,
                              false);
        } else {
            result = makeQual(var->varattno, opname,
                              (Expr *) makeBoolConst(node->booltesttype == IS_TRUE, false),
                              false,
                              false);
        }
        *quals = lappend(*quals, result);
    }
}

/*
 * Extract conditions that can be pushed down, as well as the parameters.
 *
 */
static void
extractRestrictions(PlannerInfo *root,
                    Relids base_relids,
                    Expr *node,
                    List **quals)
{
    switch (nodeTag(node))
    {
        case T_OpExpr:
            extractClauseFromOpExpr(root, base_relids,
                                    (OpExpr *) node, quals);
            break;
        case T_NullTest:
            extractClauseFromNullTest(root, base_relids,
                                      (NullTest *) node, quals);
            break;
        case T_ScalarArrayOpExpr:
            extractClauseFromScalarArrayOpExpr(root, base_relids,
                                               (ScalarArrayOpExpr *) node,
                                               quals);
            break;
        case T_BooleanTest:
            extractClauseFromBoolTest(root, base_relids,
                                      (BooleanTest *) node, quals);
            break;

        default:
            /*
             * Note: multicorn issue: 8 https://github.com/pgsql-io/multicorn2/issues/8 on OR / AND NOT clauses
             * - As per the OP, and some tests I performed, a T_BoolOpExpr occurs when the where clause
             * qualifier has the form of an OR, or AND NOT (and, I suspect, anything other than an AND) [1].
             * - The analogous code in Postgres' contrib.postgres_fdw implmentation has - not
             * surprisingly - a recursive expansion of the qualifier where we have code for 3 specific
             * cases (equality, test for NULL and IN).
             */
            {
                ereport(LOG,
                        (errmsg("unsupported expression for "
                                "extractClauseFrom"),
                         errdetail("nodetag: %d, %s", (int)nodeTag(node),
                                   nodeToString(node))));
            }
            break;
    }
}

/*
 * The list of needed columns (represented by their respective vars)
 * is pulled from:
 *	- the targetcolumns
 *	- the restrictinfo
 */
static List *
extractColumns(List *reltargetlist, List *restrictinfolist)
{
    ListCell   *lc;
    List	   *columns = NULL;
    int			i = 0;

    foreach(lc, reltargetlist)
    {
        List	   *targetcolumns;
        Node	   *node = (Node *) lfirst(lc);

        targetcolumns = pull_var_clause(node, PVC_RECURSE_AGGREGATES| PVC_RECURSE_PLACEHOLDERS);
        columns = list_union(columns, targetcolumns);
        i++;
    }
    foreach(lc, restrictinfolist)
    {
        List	   *targetcolumns;
        RestrictInfo *node = (RestrictInfo *) lfirst(lc);

        targetcolumns = pull_var_clause((Node *) node->clause, PVC_RECURSE_AGGREGATES| PVC_RECURSE_PLACEHOLDERS);
        columns = list_union(columns, targetcolumns);
    }
    return columns;
}

/*
 *	Returns a "Value" node containing the string name of the column from a var.
 */
#if PG_VERSION_NUM < 150000
Value *
#else
String *
#endif
colnameFromVar(Var *var, PlannerInfo *root)
{
	RangeTblEntry *rte = rte = planner_rt_fetch(var->varno, root);
	char	   *attname = get_attname(rte->relid, var->varattno, false);

	if (attname == NULL)
	{
		return NULL;
	}
	else
	{
		return makeString(attname);
	}
}


void
multicorn_getRelSize(PlannerInfo *root,
                     RelOptInfo *baserel,
                     Oid foreigntableid,
                     SpringtailPlanState *planstate)
{
    double rows;
    int width;

    ForeignTable *ftable = GetForeignTable(foreigntableid);
    ListCell   *lc;
    bool		needWholeRow = false;
    TupleDesc	desc;

    /* Initialize the conversion info array */
    {
        Relation	rel = RelationIdGetRelation(ftable->relid);

        desc = RelationGetDescr(rel);
        needWholeRow = rel->trigdesc && rel->trigdesc->trig_insert_after_row;
        RelationClose(rel);
    }

    // We store both the attnum and the attname in the target_list so that we can generate a mapping
    // between the local FDW attnum and the Springtail column position.
    if (needWholeRow)
    {
        int			i;

        for (i = 0; i < desc->natts; i++)
        {
            Form_pg_attribute att = TupleDescAttr(desc, i);

            if (!att->attisdropped)
            {
                // save the local attno and the attname
                SpringtailTargetColumn *target = (SpringtailTargetColumn *)palloc0(sizeof(SpringtailTargetColumn));
                target->attname = makeString(NameStr(att->attname));
                target->attnum = att->attnum;

                planstate->target_list = lappend(planstate->target_list, target);
            }
        }
    }
    else
    {
        /* Pull "var" clauses to build an appropriate target list */
        foreach(lc, extractColumns(baserel->reltarget->exprs, baserel->baserestrictinfo))
        {
            Var *var = (Var *) lfirst(lc);

            // save the local attno and the attname
            SpringtailTargetColumn *target = (SpringtailTargetColumn *)palloc0(sizeof(SpringtailTargetColumn));
            target->attname = colnameFromVar(var, root);
            target->attnum = var->varattno;

            if (target->attname != NULL && strVal(target->attname) != NULL) {
                planstate->target_list = lappend(planstate->target_list, target);
            }
        }
    }
    /* Extract the restrictions from the plan. */
    foreach(lc, baserel->baserestrictinfo)
    {
        extractRestrictions(root, baserel->relids,
                            ((RestrictInfo *) lfirst(lc))->clause,
                            &planstate->qual_list);
    }

    fdw_get_rel_size(planstate, planstate->target_list, planstate->qual_list, &rows, &width);

    baserel->rows = rows;
    baserel->reltarget->width = width;
    planstate->width = width;
}

List *
multicorn_getForeignPaths(PlannerInfo *root,
                          RelOptInfo *baserel,
                          Oid foreigntableid,
                          SpringtailPlanState *planstate)
{
    List				*paths; /* List of ForeignPath */
    ListCell		    *lc;

    /* These lists are used to handle sort pushdown */
    List				*apply_pathkeys = NULL;
    List				*deparsed_pathkeys = NULL;

    List *fdw_private = list_make1((void *)planstate);

    /* Extract a friendly version of the pathkeys. */
    /* Returns a List of a Lists<attnum, rows> */
    List *possiblePaths = fdw_get_path_keys(planstate); // see pathKeys()

    /* Try to find parameterized paths */
    paths = findPaths(root, baserel, possiblePaths, SPRINGTAIL_STARTUP_COST,
                      apply_pathkeys, deparsed_pathkeys);

    /* Add a simple default path */
    paths = lappend(paths, create_foreignscan_path(root, baserel,
        NULL,  /* default pathtarget */
        baserel->rows,
        SPRINGTAIL_STARTUP_COST,
        baserel->rows * baserel->reltarget->width,
        NIL,		/* no pathkeys */
        NULL,
        NULL,
        NULL));


    /* Handle sort pushdown */
    if (root->query_pathkeys)
    {
        List* deparsed = deparse_sortgroup(root, foreigntableid, baserel);

        if (deparsed)
        {
            /* Update the sort_*_pathkeys lists if needed */
            computeDeparsedSortGroup(deparsed, planstate, &apply_pathkeys,
                    &deparsed_pathkeys);
        }
    }
    /* Add each ForeignPath previously found */
    foreach(lc, paths)
    {
        ForeignPath *path = (ForeignPath *) lfirst(lc);

        /* Add the path without modification */
        add_path(baserel, (Path *) path);

        /* Add the path with sort pusdown if possible */
        if (apply_pathkeys && deparsed_pathkeys)
        {
            ForeignPath *newpath;

            newpath = create_foreignscan_path(root, baserel,
                NULL,  /* default pathtarget */
                path->path.rows,
                path->path.startup_cost, path->path.total_cost,
                apply_pathkeys, NULL,
                NULL,
#if PG_VERSION_NUM >= 170000
                NULL,
#endif
                (void *) deparsed_pathkeys);

            newpath->path.param_info = path->path.param_info;
            add_path(baserel, (Path *) newpath);
        }
    }
}


ForeignScan *
multicorn_getForeignPlan(PlannerInfo *root,
                         RelOptInfo *baserel,
                         Oid foreigntableid,
                         ForeignPath *best_path,
                         List *tlist,
                         List *scan_clauses,
                         Plan *outer_plan,
                         SpringtailPlanState *planstate)
{
    Index		scan_relid = baserel->relid;
    ListCell   *lc;
    best_path->path.pathtarget->width = planstate->width;
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    /* Extract the quals coming from a parameterized path, if any */
    if (best_path->path.param_info)
    {
        foreach(lc, scan_clauses)
        {
            extractRestrictions(
                root,
                baserel->relids,
                (Expr *) lfirst(lc),
                &planstate->qual_list);
        }
    }
    planstate->pathkeys = (List *) best_path->fdw_private;

    List *fdw_private = list_make1((void *)planstate);

    elog(LOG, "multicorn_getForeignPlan()");

    return make_foreignscan(tlist,
                            scan_clauses,
                            scan_relid,
                            scan_clauses,   /* no expressions to evaluate */
                            fdw_private,    /* private data */
                            NULL,           /* no custom tlist */
                            NULL,           /* All quals are meant to be rechecked */
                            NULL);
}

List *
multicorn_buildSimpleQualList(ForeignScanState *node)
{

    ForeignScan *fs = (ForeignScan *)node->ss.ps.plan;
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    List        *result = NIL;
    List        *qual_list = NIL;
    ListCell    *lc;

    // Modified from multicorn multicorn.c multicornBeginForeignScan()
    // extract qual_list
    foreach(lc, fs->fdw_exprs)
    {
        extractRestrictions(
            NULL,
            bms_make_singleton(fs->scan.scanrelid),
                            ((Expr *) lfirst(lc)),
                            &qual_list);
    }

    // Modified from multicorn python.c execute()
    // simplify qual list to those with constants
    foreach(lc, qual_list)
    {
        BaseQual   *qual = lfirst(lc);
        ConstQual  *newqual = NULL;
        bool		isNull;
        ExprState  *expr_state = NULL;

        switch (qual->right_type)
        {
            case T_Param:
                expr_state = ExecInitExpr(((ParamQual *) qual)->expr,
                                          (PlanState *) node);
                newqual = palloc0(sizeof(ConstQual));
                newqual->base.right_type = T_Const;
                newqual->base.varattno = qual->varattno;
                newqual->base.opname = qual->opname;
                newqual->base.op = getOpEnum(qual->opname);
                newqual->base.isArray = qual->isArray;
                newqual->base.useOr = qual->useOr;
                newqual->value = ExecEvalExpr(expr_state, econtext, &isNull);
                newqual->base.typeoid = ((Param*) ((ParamQual *) qual)->expr)->paramtype;
                newqual->isnull = isNull;
                break;
            case T_Const:
                newqual = (ConstQual *) qual;
                break;
            default:
                break;
        }

        if (newqual != NULL) {
            result = lappend(result, newqual);
        }
    }

    return result;
}
