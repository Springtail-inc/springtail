/*
 * Various things to allow source files from postgresql code to be
 * used in pgbouncer.  pgbouncer's system.h needs to be included
 * before this.
 */

/* from c.h */

#ifndef _POSTGRES_COMPAT_H_
#define _POSTGRES_COMPAT_H_

#include <string.h>
#include <stdint.h>
#include <arpa/inet.h>

#ifdef __cplusplus
extern "C" {
#endif

#define int8 int8_t
#define uint8 uint8_t
#define uint16 uint16_t
#define uint32 uint32_t

#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))

#ifndef pg_hton32
#define pg_hton32(x) htonl(x)
#endif

#define pg_attribute_noreturn() _NORETURN

#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)


/* sha2.h compat */
#define pg_sha256_ctx struct sha256_ctx
#define PG_SHA256_BLOCK_LENGTH SHA256_BLOCK_SIZE
#define PG_SHA256_DIGEST_LENGTH SHA256_DIGEST_LENGTH
#define pg_sha256_init(ctx) sha256_reset(ctx)
#define pg_sha256_update(ctx, data, len) sha256_update(ctx, data, len)
#define pg_sha256_final(ctx, dst) sha256_final(ctx, dst)


/* define this to use non-server code paths */
#define FRONTEND

#ifdef __cplusplus
}   /* extern "C" */
#endif

#endif /* _POSTGRES_COMPAT_H_ */
