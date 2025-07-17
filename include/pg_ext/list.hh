#pragma once

#include "export.hh"

#define FLEXIBLE_ARRAY_MEMBER

typedef unsigned int 	Oid;
typedef unsigned int 	TransactionId;

typedef enum NodeTag
{
    T_Invalid = 0,
} NodeTag;

typedef union ListCell
{
	void	   *ptr_value;
	int			int_value;
	Oid			oid_value;
	TransactionId xid_value;
} ListCell;

typedef struct List
{
	NodeTag		type;			/* T_List, T_IntList, T_OidList, or T_XidList */
	int			length;			/* number of elements currently present */
	int			max_length;		/* allocated length of elements[] */
	ListCell   *elements;		/* re-allocatable array of cells */
	/* We may allocate some cells along with the List header: */
	ListCell	initial_elements[FLEXIBLE_ARRAY_MEMBER];
	/* If elements == initial_elements, it's not a separate allocation */
} List;

//// EXPORTED INTERFACES
extern "C" PGEXT_API List *lappend(List *list, void *datum);
extern "C" PGEXT_API List *list_concat(List *list1, List *list2);
extern "C" PGEXT_API List *list_delete_cell(List *list, ListCell *cell);
extern "C" PGEXT_API void list_free(List *list);
extern "C" PGEXT_API List *list_make1_impl(List *list, void *datum);
