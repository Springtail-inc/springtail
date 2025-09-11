#pragma once

#include <pg_ext/export.hh>
#include <pg_ext/common.hh>

typedef enum NodeTag { T_Invalid, T_List, T_IntList, T_OidList, T_XidList } NodeTag;

typedef struct Node {
    NodeTag type;
} Node;

extern "C" PGEXT_API int exprLocation(Node *node);
extern "C" PGEXT_API Oid exprType(const Node *expr);
