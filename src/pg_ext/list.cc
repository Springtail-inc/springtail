#include <pg_ext/list.hh>
#include <cstdlib>

List *list_make1_impl(void *datum) {
    return NULL;
}

List *lappend(List *list, void *datum) {
    return NULL;
}

List *list_concat(List *list1, List *list2) {
    return NULL;
}

List *list_delete_cell(List *list, ListCell *cell_to_delete) {
    return NULL;
}

void list_free(List *list) {
}
