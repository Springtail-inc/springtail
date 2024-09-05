#include <stdint.h>
#include <stdbool.h>
#include <postgres.h>
#include <varatt.h>
#include <utils/builtins.h>
#include <access/htup_details.h>
#include <nodes/nodes.h>
#include <nodes/value.h>
#include <nodes/primnodes.h>

static inline Node *
newNode_mock(size_t size, NodeTag tag)
{
    Node       *result;

    Assert(size >= sizeof(Node));   /* need the tag, at least */
    result = (Node *) malloc(size); // modified from palloc0 to malloc
    memset(result, 0, size);
    result->type = tag;

    return result;
}

#define makeNode_mock(_type_)        ((_type_ *) newNode_mock(sizeof(_type_),T_##_type_))

// These files are not being linked in, from libpq so we define them here
Integer *makeInteger(int i) {
     Integer    *v = makeNode_mock(Integer);

     v->ival = i;
     return v;
 }

/** Dummy function so that we can link with pg_fdw_mgr.cc */
const char *quote_identifier(const char *ident) {
    return ident;
}

 static List *
 new_list(NodeTag type, int min_size)
 {
     List       *newlist;
     int         max_size;

     Assert(min_size > 0);
     max_size = min_size;

     newlist = (List *) malloc(offsetof(List, initial_elements) +
                               max_size * sizeof(ListCell));
     newlist->type = type;
     newlist->length = min_size;
     newlist->max_length = max_size;
     newlist->elements = newlist->initial_elements;

     return newlist;
 }


static void enlarge_list(List *list,
                         int min_size)
{
    int         new_max_len;

    Assert(min_size > list->max_length);    /* else we shouldn't be here */
    /* As above, don't allocate anything extra */
    new_max_len = min_size;

    if (list->elements == list->initial_elements)
    {
        /*
        * Replace original in-line allocation with a separate palloc block.
        * Ensure it is in the same memory context as the List header.  (The
        * previous List implementation did not offer any guarantees about
        * keeping all list cells in the same context, but it seems reasonable
        * to create such a guarantee now.)
        */
        list->elements = (ListCell *)
            malloc(new_max_len * sizeof(ListCell));
        memcpy(list->elements, list->initial_elements,
            list->length * sizeof(ListCell));

        /*
        * We must not move the list header, so it's unsafe to try to reclaim
        * the initial_elements[] space via repalloc.  In debugging builds,
        * however, we can clear that space and/or mark it inaccessible.
        * (wipe_mem includes VALGRIND_MAKE_MEM_NOACCESS.)
        */
        //free(list->initial_elements);
    }
    else
    {
        /* Normally, let repalloc deal with enlargement */
        list->elements = (ListCell *) realloc(list->elements,
                                            new_max_len * sizeof(ListCell));
    }

    list->max_length = new_max_len;
}


static void
new_tail_cell(List *list)
{
    /* Enlarge array if necessary */
    if (list->length >= list->max_length)
        enlarge_list(list, list->length + 1);
    list->length++;
}

/** Dummy lappend, does nothing */
List *lappend(List *list, void *datum) {
     if (list == NIL)
         list = new_list(T_List, 1);
     else
         new_tail_cell(list);

     llast(list) = datum;

     return list;
}

/** Dummy function so that we can link with pg_fdw_mgr.cc */
text *
cstring_to_text(const char *s)
{
    int len = strlen(s);
    text *result = (text *) palloc(VARHDRSZ + len + 1);
    SET_VARSIZE(result, VARHDRSZ + len);
    memcpy(VARDATA(result), s, len);

    return result;
}

char *
text_to_cstring(const text *t)
{
    int len;
    char *result;

    len = VARSIZE_ANY_EXHDR(t);
    result = (char *) palloc(len + 1);
    memcpy(result, VARDATA_ANY(t), len);
    result[len] = '\0';
    return result;
}

Const *makeConst(Oid consttype,
                    int32 consttypmod,
                    Oid constcollid,
                    int constlen,
                    Datum constvalue,
                    bool constisnull,
                    bool constbyval)
{
    return NULL; // XXX not impl
}

List *list_append_unique_int(List *list, int datum) {
    return list; // XXX not impl
}

bool errstart(int elevel, const char *domain) {
    return false;
}

void errfinish(const char *filename,
                int  lineno,
                const char *funcname) {}

int errmsg_internal(const char *fmt, ...) { return 0; }

bool errstart_cold(int elevel, const char* domain) { return false; }

Datum OidFunctionCall3Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3) {
    return (Datum)0;
}

void ReleaseSysCache(HeapTuple tuple) { }

HeapTuple SearchSysCache1(int cacheId, Datum key1) {
    return (HeapTuple)NULL;
}

// For background worker
void ProcessInterrupts(void) {}

bool InterruptPending = false;

void BackgroundWorkerUnblockSignals(void) {}

void die(int postgres_signal_arg) {}

void proc_exit(int code) {}

bool PostmasterIsAliveInternal(void) { return true; }