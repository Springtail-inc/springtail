#pragma once

#include <pg_ext/export.hh>

#include <cstddef>
#include <cstdint>
#include <string>
#include <cstring>
#include <vector>

/* hash_search operations */
enum class HASHACTION {
    HASH_FIND,
    HASH_ENTER,
    HASH_REMOVE
};

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

struct HASHCTL {
    size_t keysize;
    size_t entrysize;
    uint64_t (*hash_func)(const void *key, size_t keysize);
    bool (*match_func)(const void *key1, const void *key2, size_t keysize);
};

struct HASH_SEQ_STATUS {
    HTAB *htab;
    size_t position;
};

#define UINT32_ALIGN_MASK (sizeof(uint32_t) - 1)
#define rot(x,k) (x << k) | (x >> (32 - k))

#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);	c += b; \
  b -= a;  b ^= rot(a, 6);	a += c; \
  c -= b;  c ^= rot(b, 8);	b += a; \
  a -= c;  a ^= rot(c,16);	c += b; \
  b -= a;  b ^= rot(a,19);	a += c; \
  c -= b;  c ^= rot(b, 4);	b += a; \
}
#define final(a,b,c) \
{ \
  c ^= b; c -= rot(b,14); \
  a ^= c; a -= rot(c,11); \
  b ^= a; b -= rot(a,25); \
  c ^= b; c -= rot(b,16); \
  a ^= c; a -= rot(c, 4); \
  b ^= a; b -= rot(a,14); \
  c ^= b; c -= rot(b,24); \
}

//// EXPORTED INTERFACES
extern "C" PGEXT_API HTAB *hash_create(const char *tabname, int nelem, const HASHCTL *info);
extern "C" PGEXT_API void *hash_search(HTAB *htab, const void *key, HASHACTION action, bool *found);
extern "C" PGEXT_API void hash_seq_init(HASH_SEQ_STATUS *status, HTAB *htab);
extern "C" PGEXT_API void *hash_seq_search(HASH_SEQ_STATUS *status);
extern "C" PGEXT_API long hash_get_num_entries(HTAB *htab);
extern "C" PGEXT_API uint32_t hash_bytes(const unsigned char *k, int keylen);
extern "C" PGEXT_API uint64_t hash_bytes_extended(const unsigned char *k,
                                                  int keylen,
                                                  uint64_t seed);
