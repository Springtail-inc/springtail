#pragma once

#include <pg_ext/export.hh>

#include <cstddef>
#include <cstdint>
#include <string>
#include <cstring>
#include <vector>

/* hash_search operations */
typedef enum {
    HASH_FIND,
    HASH_ENTER,
    HASH_REMOVE
} HASHACTION;

struct HashEntry {
    void *key;
    void *value;
    bool occupied;
};

struct HTAB {
    std::string name;
    size_t keysize;
    size_t entrysize;
    uint64_t (*hash_func)(const void *, size_t);
    bool (*match_func)(const void *, const void *, size_t);
    std::vector<HashEntry> table;
    size_t count;
};

typedef struct HASHCTL {
    size_t keysize;
    size_t entrysize;
    uint64_t (*hash_func)(const void *key, size_t keysize);
    bool (*match_func)(const void *key1, const void *key2, size_t keysize);
} HASHCTL;

typedef struct HASH_SEQ_STATUS {
    HTAB *htab;
    size_t position;
} HASH_SEQ_STATUS;

//// EXPORTED INTERFACES
extern "C" PGEXT_API HTAB *hash_create(const char *tabname, int nelem, const HASHCTL *info);
extern "C" PGEXT_API void *hash_search(HTAB *htab, const void *key, HASHACTION action, bool *found);
extern "C" PGEXT_API void hash_seq_init(HASH_SEQ_STATUS *status, HTAB *htab);
extern "C" PGEXT_API void *hash_seq_search(HASH_SEQ_STATUS *status);
extern "C" PGEXT_API long hash_get_num_entries(HTAB *htab);
extern "C" PGEXT_API uint32_t hash_bytes(const unsigned char *k, int keylen);
