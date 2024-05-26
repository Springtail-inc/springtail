#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdbool.h>
#include <errno.h>
#include <limits.h>
#include <string.h>

#ifdef LINUX
#include <bsd/stdlib.h>
#include <bsd/string.h>
#endif

#ifndef _SYSTEM_H_
#define _SYSTEM_H_

#ifdef __cplusplus
extern "C" {
#endif

#define Assert(x) assert(x)

/* wrapped for getting random bytes */
static inline void get_random_bytes(uint8_t *dest, int len)
{
	arc4random_buf(dest, len);
}

#ifdef __cplusplus
}	/* extern "C" */
#endif

#endif
