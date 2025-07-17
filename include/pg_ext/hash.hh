#pragma once

#include "memory.hh"
#include "export.hh"

/* hash_search operations */
typedef enum { HASH_FIND, HASH_ENTER, HASH_REMOVE, HASH_ENTER_NULL } HASHACTION;

typedef struct HTAB HTAB;

typedef struct HASHHDR HASHHDR;

/* Parameter data structure for hash_create */
/* Only those fields indicated by hash_flags need be set */
typedef struct HASHCTL
{
    /* Used if HASH_PARTITION flag is set: */
    long        num_partitions; /* # partitions (must be power of 2) */
    /* Used if HASH_SEGMENT flag is set: */
    long        ssize;          /* segment size */
    /* Used if HASH_DIRSIZE flag is set: */
    long        dsize;          /* (initial) directory size */
    long        max_dsize;      /* limit to dsize if dir size is limited */
    /* Used if HASH_ELEM flag is set (which is now required): */
    size_t      keysize;        /* hash key length in bytes */
    size_t      entrysize;      /* total user element size in bytes */
    /* Used if HASH_FUNCTION flag is set: */
    uint32 (*hash)(const void *key, size_t keysize);         /* hash function */
    /* Used if HASH_COMPARE flag is set: */
    int (*match)(const void *key1, const void *key2, size_t keysize);      /* key comparison function */
    /* Used if HASH_KEYCOPY flag is set: */
    void (*keycopy)(void *dest, const void *src, size_t keysize);       /* key copying function */
    /* Used if HASH_ALLOC flag is set: */
    void (*alloc)(void *dest, const void *src, size_t keysize);        /* memory allocator */
    /* Used if HASH_CONTEXT flag is set: */
    pgext::MemoryContext hcxt;         /* memory context to use for allocations */
    /* Used if HASH_SHARED_MEM flag is set: */
    HASHHDR    *hctl;           /* location of header in shared mem */
} HASHCTL;

typedef struct HASHELEMENT {
    struct HASHELEMENT *link; /* link to next entry in same bucket */
    uint32 hashvalue;         /* hash function result for this entry */
} HASHELEMENT;

typedef struct {
    HTAB *hashp;
    uint32 curBucket;      /* index of current bucket */
    HASHELEMENT *curEntry; /* current entry in bucket */
} HASH_SEQ_STATUS;

//// EXPORTED INTERFACES
extern "C" PGEXT_API HTAB *hash_create(const char *tabname,
                                       long nelem,
                                       const HASHCTL *info,
                                       int flags);
extern "C" PGEXT_API void *hash_search(HTAB *hashp,
                                       const void *keyPtr,
                                       HASHACTION action,
                                       bool *foundPtr);
extern "C" PGEXT_API long hash_get_num_entries(HTAB *hashp);
extern "C" PGEXT_API void hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp);
extern "C" PGEXT_API void *hash_seq_search(HASH_SEQ_STATUS *status);
