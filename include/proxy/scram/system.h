#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdbool.h>
#include <errno.h>
#include <limits.h>
#include <string.h>

#ifndef _SYSTEM_H_
#define _SYSTEM_H_

#define Assert(x) assert(x)

#define MD5_PASSWD_LEN  35

/* wrapped for getting random bytes */
void get_random_bytes(uint8_t *dest, int len)
{
	arc4random_buf(dest, len);
}

#endif
