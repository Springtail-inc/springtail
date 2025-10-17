#include <pg_ext/list.hh>
#include <pg_ext/node.hh>
#include <pg_ext/memory.hh>
#include <cstring>

static inline int list_cell_bytes(int count) {
    return count * sizeof(ListCell);
}

static List *allocate_list(NodeTag type, int initial_capacity) {
    size_t total_size = sizeof(List) + list_cell_bytes(initial_capacity);
    auto *list = (List *)palloc(total_size);
    if (!list) return nullptr;
    list->type = type;
    list->length = 0;
    list->max_length = initial_capacity;
    list->elements = list->initial_elements;

    return list;
}

List *list_make1_impl(List *list, void *datum) {
    if (!list) {
        list = allocate_list(NodeTag::T_Invalid, LIST_INITIAL_ALLOC);
        if (!list) return nullptr;
    }

    list->elements[0].ptr_value = datum;
    list->length = 1;

    return list;
}

List *lappend(List *list, void *datum) {
    if (!list) {
        return list_make1_impl(list, datum);
    }

    // Resize if needed
    if (list->length >= list->max_length) {
        int new_capacity = list->max_length * 2;
        ListCell *new_elements = nullptr;

        if (list->elements == list->initial_elements) {
            // First growth: allocate a new buffer and copy existing inline elements
            new_elements = (ListCell *) palloc(list_cell_bytes(new_capacity));
            std::memcpy(new_elements, list->elements, list_cell_bytes(list->length));
            list->elements = new_elements;
        } else {
            // Subsequent growth: resize the existing buffer
            new_elements = (ListCell *) repalloc(list->elements, list_cell_bytes(new_capacity));
            list->elements = new_elements;
        }

        list->max_length = new_capacity;
    }

    list->elements[list->length++].ptr_value = datum;
    return list;
}

List *list_concat(List *list1, List *list2) {
    if (!list1) return list2;
    if (!list2) return list1;

    for (int i = 0; i < list2->length; ++i) {
        lappend(list1, list2->elements[i].ptr_value);
    }

    return list1;
}

List *list_delete_cell(List *list, const ListCell *cell_to_delete) {
    if (!list || !cell_to_delete) return list;

    auto index = -1;
    for (int i = 0; i < list->length; ++i) {
        if (&list->elements[i] == cell_to_delete) {
            index = i;
            break;
        }
    }

    if (index >= 0) {
        for (int j = index; j < list->length - 1; ++j) {
            list->elements[j] = list->elements[j + 1];
        }
        list->length--;
    }

    return list;
}

void list_free(List *list) {
    if (!list) return;

    if (list->elements != list->initial_elements) {
        pfree(list->elements);
    }

    pfree(list);
}

List *list_delete_nth_cell(List *list, int n) {
    if (!list || n < 0 || n >= list->length) return list;

    for (int i = n; i < list->length - 1; ++i) {
        list->elements[i] = list->elements[i + 1];
    }
    list->length--;

    return list;
}
