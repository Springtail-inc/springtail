#include <pg_ext/hash.hh>

HTAB *hash_create(const char *tabname,
                  long nelem,
                  const HASHCTL *info,
                  int flags) {
    return NULL;
}

void *hash_search(HTAB *hashp,
                   const void *keyPtr,
                   HASHACTION action,
                   bool *foundPtr) {
    return NULL;
}

long hash_get_num_entries(HTAB *hashp) {
    return 0;
}

void hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp) {
    status->hashp = hashp;
    status->curBucket = 0;
    status->curEntry = NULL;
}

void *hash_seq_search(HASH_SEQ_STATUS *status) {
    return NULL;
}
