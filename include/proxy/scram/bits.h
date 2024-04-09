/*
 * Copyright (c) 2009  Marko Kreen
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/** @file
 * Bit arithmetics.
 *
 * - is_power_of_2
 * - ffs, ffsl, ffsll
 * - fls, flsl, flsll
 * - rol16, rol32, rol64
 * - ror16, ror32, ror64
 */
#ifndef _USUAL_BITS_H_
#define _USUAL_BITS_H_

/*
 * Single-eval and type-safe rol/ror
 */

/** Rotate 16-bit int to left */
static inline uint16_t rol16(uint16_t v, int s)
{
	return (v << s) | (v >> (16 - s));
}
/** Rotate 32-bit int to left */
static inline uint32_t rol32(uint32_t v, int s)
{
	return (v << s) | (v >> (32 - s));
}
/** Rotate 64-bit int to left */
static inline uint64_t rol64(uint64_t v, int s)
{
	return (v << s) | (v >> (64 - s));
}

/** Rotate 16-bit int to right */
static inline uint16_t ror16(uint16_t v, int s) { return rol16(v, 16 - s); }

/** Rotate 32-bit int to right */
static inline uint32_t ror32(uint32_t v, int s) { return rol32(v, 32 - s); }

/** Rotate 64-bit int to right */
static inline uint64_t ror64(uint64_t v, int s) { return rol64(v, 64 - s); }

#endif
