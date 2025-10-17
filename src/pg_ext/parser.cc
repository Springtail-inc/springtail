#include <cassert>
#include <pg_ext/parser.hh>

Node *coerce_to_target_type(ParseState *pstate,
                            Node *expr,
                            Oid exprtype,
                            Oid targettype,
                            int32_t targettypmod,
                            CoercionContext ccontext,
                            CoercionForm cformat,
                            int location)
{
    return nullptr;
}

Node *
transformExpr(ParseState *pstate, Node *expr, ParseExprKind exprKind)
{
    Node *result;
    ParseExprKind sv_expr_kind;

    /* Save and restore identity of expression type we're parsing */
    assert(exprKind != ParseExprKind::EXPR_KIND_NONE);

    sv_expr_kind = pstate->p_expr_kind;
    pstate->p_expr_kind = exprKind;

    result = transformExprRecurse(pstate, expr);

    pstate->p_expr_kind = sv_expr_kind;

    return result;
}

Node *transformExprRecurse(ParseState *pstate, Node *expr)
{
    // XXX Stubbed for now
    return nullptr;
}
