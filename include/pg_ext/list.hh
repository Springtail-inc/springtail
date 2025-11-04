#pragma once

#include <pg_ext/export.hh>
#include <pg_ext/node.hh>
#include <pg_ext/common.hh>

#define FLEXIBLE_ARRAY_MEMBER
constexpr int LIST_INITIAL_ALLOC = 4;

union ListCell // NOSONAR: XXX Need to use std::variant
{
	void	   *ptr_value;
	int			int_value;
	Oid			oid_value;
	TransactionId xid_value;
};

struct List
{
	NodeTag		type;			/* T_List, T_IntList, T_OidList, or T_XidList */
	int			length;			/* number of elements currently present */
	int			max_length;		/* allocated length of elements[] */
	ListCell   *elements;		/* re-allocatable array of cells */
	/* We may allocate some cells along with the List header: */
	ListCell	initial_elements[FLEXIBLE_ARRAY_MEMBER];
	/* If elements == initial_elements, it's not a separate allocation */
};


struct ForEachState
{
	const List *l;				/* list we're looping through */
	int			i;				/* current element index */
};

#define lfirst(lc) ((lc)->ptr_value)

#define foreach(cell, lst)	\
	for (ForEachState cell##__state = {(lst), 0}; \
		 (cell##__state.l != nullptr && \
		  cell##__state.i < cell##__state.l->length) ? \
		 (cell = &cell##__state.l->elements[cell##__state.i], true) : \
		 (cell = nullptr, false); \
		 cell##__state.i++)

//// EXPORTED INTERFACES
extern "C" PGEXT_API List *lappend(List *list, void *datum);
extern "C" PGEXT_API List *list_concat(List *list1, List *list2);
extern "C" PGEXT_API List *list_delete_cell(List *list, const ListCell *cell);
extern "C" PGEXT_API void list_free(List *list);
extern "C" PGEXT_API List *list_make1_impl(List *list, void *datum);
extern "C" PGEXT_API List *list_delete_nth_cell(List *list, int n);
