#pragma once

#include <pg_ext/export.hh>
#include <pg_ext/common.hh>

enum class NodeTag {
    T_Invalid,
    T_List,
    T_IntList,
    T_OidList,
    T_XidList
};

struct Node {
    NodeTag type;
};

#define nodeTag(nodeptr) (((const Node*)(nodeptr))->type)

extern "C" PGEXT_API int exprLocation(Node *node);
extern "C" PGEXT_API Oid exprType(const Node *expr);
