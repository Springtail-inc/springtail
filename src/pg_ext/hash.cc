#include <pg_ext/hash.hh>
#include <pg_ext/memory.hh>

static uint64_t default_hash_func(const void *key, size_t len) {
    uint64_t hash = 14695981039346656037uLL;
    const auto *data = (const unsigned char *)key;
    for (size_t i = 0; i < len; ++i)
        hash = (hash ^ data[i]) * 1099511628211uLL;
    return hash;
}

static bool default_match_func(const void *a, const void *b, size_t len) {
    return std::memcmp(a, b, len) == 0;
}

HTAB *hash_create(const char *tabname, int nelem, const HASHCTL *info) {
    auto htab = std::make_shared<HTAB>();

	auto df_hash_func = default_hash_func;
	auto df_match_func = default_match_func;
	if ( df_hash_func == nullptr)
		df_hash_func = &default_hash_func;
	if ( df_match_func == nullptr)
		df_match_func = &default_match_func;

    htab->name = tabname ? tabname : "";
    htab->keysize = info ? info->keysize : sizeof(void *);
    htab->entrysize = info ? info->entrysize : sizeof(void *);
    htab->hash_func = info && info->hash_func ? info->hash_func : df_hash_func;
    htab->match_func = info && info->match_func ? info->match_func : df_match_func;
    htab->table.resize(nelem * 2);
    htab->count = 0;
    return htab.get();
}

void *hash_search(HTAB *htab, const void *key, HASHACTION action, bool *found) {
    uint64_t hash = htab->hash_func(key, htab->keysize);
    size_t index = hash % htab->table.size();

    for (size_t i = 0; i < htab->table.size(); ++i) {
        size_t pos = (index + i) % htab->table.size();
        auto &entry = htab->table[pos];

        if (entry.occupied && htab->match_func(entry.key, key, htab->keysize)) {
            if (found) *found = true;
            return entry.value;
        }

        if (!entry.occupied) {
            if (action == HASHACTION::HASH_ENTER) {
                entry.key = palloc(htab->keysize);
                std::memcpy(entry.key, key, htab->keysize);

                entry.value = palloc(htab->entrysize);
                entry.occupied = true;
                htab->count++;

                if (found) *found = false;
                return entry.value;
            } else if (action == HASHACTION::HASH_FIND || action == HASHACTION::HASH_REMOVE) {
                if (found) *found = false;
                return nullptr;
            }
        }
    }

    if (found) *found = false;
    return nullptr;
}

void hash_seq_init(HASH_SEQ_STATUS *status, HTAB *htab) {
    status->htab = htab;
    status->position = 0;
}

void *hash_seq_search(HASH_SEQ_STATUS *status) {
    auto &htab = *status->htab;
    while (status->position < htab.table.size()) {
        auto &entry = htab.table[status->position++];
        if (entry.occupied)
            return entry.value;
    }
    return nullptr;
}

long hash_get_num_entries(HTAB *htab) {
    return static_cast<long>(htab->count);
}

uint32_t hash_bytes(const unsigned char *k, int keylen) {
	auto df_hash_func = default_hash_func;
	if ( df_hash_func == nullptr)
		df_hash_func = &default_hash_func;
    return df_hash_func(k, keylen);
}

uint64_t
hash_bytes_extended(const unsigned char *k, int keylen, uint64_t seed)
{
    auto a = 0u;
    auto b = 0u;
    auto c = 0u;
    auto len = 0u;

	/* Set up the internal state */
    len = keylen;
    a = b = c = 0x9e3779b9u + len + 3923095u;

	/* If the seed is non-zero, use it to perturb the internal state. */
	if (seed != 0)
	{
		/*
		 * In essence, the seed is treated as part of the data being hashed,
		 * but for simplicity, we pretend that it's padded with four bytes of
		 * zeroes so that the seed constitutes a 12-byte chunk.
		 */
		a += (uint32_t) (seed >> 32);
		b += (uint32_t) seed;
		mix(a, b, c);
	}

	/* If the source pointer is word-aligned, we use word-wide fetches */
	if (((uintptr_t) k & UINT32_ALIGN_MASK) == 0)
	{
		/* Code path for aligned source data */
		const auto *ka = (const uint32_t *) k;

		/* handle most of the key */
		while (len >= 12)
		{
			a += ka[0];
			b += ka[1];
			c += ka[2];
			mix(a, b, c);
			ka += 3;
			len -= 12;
		}

		/* handle the last 11 bytes */
		k = (const unsigned char *) ka;
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32_t) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32_t) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32_t) k[8] << 24);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32_t) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32_t) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32_t) k[4] << 24);
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32_t) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32_t) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32_t) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32_t) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32_t) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32_t) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32_t) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32_t) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32_t) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32_t) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
#ifdef WORDS_BIGENDIAN
			a += (k[3] + ((uint32_t) k[2] << 8) + ((uint32_t) k[1] << 16) + ((uint32_t) k[0] << 24));
			b += (k[7] + ((uint32_t) k[6] << 8) + ((uint32_t) k[5] << 16) + ((uint32_t) k[4] << 24));
			c += (k[11] + ((uint32_t) k[10] << 8) + ((uint32_t) k[9] << 16) + ((uint32_t) k[8] << 24));
#else							/* !WORDS_BIGENDIAN */
			a += (k[0] + ((uint32_t) k[1] << 8) + ((uint32_t) k[2] << 16) + ((uint32_t) k[3] << 24));
			b += (k[4] + ((uint32_t) k[5] << 8) + ((uint32_t) k[6] << 16) + ((uint32_t) k[7] << 24));
			c += (k[8] + ((uint32_t) k[9] << 8) + ((uint32_t) k[10] << 16) + ((uint32_t) k[11] << 24));
#endif							/* WORDS_BIGENDIAN */
			mix(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32_t) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32_t) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32_t) k[8] << 24);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += k[7];
				/* fall through */
			case 7:
				b += ((uint32_t) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32_t) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32_t) k[4] << 24);
				/* fall through */
			case 4:
				a += k[3];
				/* fall through */
			case 3:
				a += ((uint32_t) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32_t) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32_t) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32_t) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32_t) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32_t) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ((uint32_t) k[7] << 24);
				/* fall through */
			case 7:
				b += ((uint32_t) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32_t) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ((uint32_t) k[3] << 24);
				/* fall through */
			case 3:
				a += ((uint32_t) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32_t) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}

	final(a, b, c);

	/* report the result */
	return ((uint64_t) b << 32) | c;
}
