/* Listpack -- A lists of strings serialization format
 *
 * This file implements the specification you can find at:
 *
 *  https://github.com/antirez/listpack
 *
 * Copyright (c) 2017-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include <stdint.h>
#include <limits.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "listpack.h"
#include "listpack_malloc.h"
#include "redisassert.h"
#include "util.h"

/* listpack 头部大小：32 位总字节数 + 16 位元素数量 = 6 字节。 */
#define LP_HDR_SIZE 6       /* 32 bit total len + 16 bit number of elements. */
/* 头部元素数量字段无法表示的值（元素数超过 UINT16_MAX 时使用）。 */
#define LP_HDR_NUMELE_UNKNOWN UINT16_MAX
/* 整数编码最大长度（1 字节类型 + 8 字节数据 = 9 字节）。 */
#define LP_MAX_INT_ENCODING_LEN 9
/* backlen 反向长度字段最大占用 5 字节。 */
#define LP_MAX_BACKLEN_SIZE 5
/* lpEncodeGetType 返回值：元素被编码为整数。 */
#define LP_ENCODING_INT 0
/* lpEncodeGetType 返回值：元素被编码为字符串。 */
#define LP_ENCODING_STRING 1

/* 7 位无符号整数编码：取值范围 0~127，仅占 1 字节。 */
#define LP_ENCODING_7BIT_UINT 0
#define LP_ENCODING_7BIT_UINT_MASK 0x80
/* 判断首字节是否表示 7 位无符号整数。 */
#define LP_ENCODING_IS_7BIT_UINT(byte) (((byte)&LP_ENCODING_7BIT_UINT_MASK)==LP_ENCODING_7BIT_UINT)
/* 7 位整数条目总字节数：1 字节数据 + 1 字节 backlen。 */
#define LP_ENCODING_7BIT_UINT_ENTRY_SIZE 2

/* 6 位字符串编码：长度 < 64，使用首字节低 6 位表示长度。 */
#define LP_ENCODING_6BIT_STR 0x80
#define LP_ENCODING_6BIT_STR_MASK 0xC0
/* 判断首字节是否表示 6 位字符串编码。 */
#define LP_ENCODING_IS_6BIT_STR(byte) (((byte)&LP_ENCODING_6BIT_STR_MASK)==LP_ENCODING_6BIT_STR)

/* 13 位有符号整数编码：取值范围 -4096~4095，占 2 字节。 */
#define LP_ENCODING_13BIT_INT 0xC0
#define LP_ENCODING_13BIT_INT_MASK 0xE0
#define LP_ENCODING_IS_13BIT_INT(byte) (((byte)&LP_ENCODING_13BIT_INT_MASK)==LP_ENCODING_13BIT_INT)
/* 13 位整数条目总字节数：1 字节编码 + 1 字节数据 + 1 字节 backlen = 3。 */
#define LP_ENCODING_13BIT_INT_ENTRY_SIZE 3

/* 12 位字符串编码：长度 < 4096，2 字节头部 + 字符串。 */
#define LP_ENCODING_12BIT_STR 0xE0
#define LP_ENCODING_12BIT_STR_MASK 0xF0
#define LP_ENCODING_IS_12BIT_STR(byte) (((byte)&LP_ENCODING_12BIT_STR_MASK)==LP_ENCODING_12BIT_STR)

/* 16 位有符号整数编码：取值范围 -32768~32767，占 3 字节。 */
#define LP_ENCODING_16BIT_INT 0xF1
#define LP_ENCODING_16BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_16BIT_INT(byte) (((byte)&LP_ENCODING_16BIT_INT_MASK)==LP_ENCODING_16BIT_INT)
#define LP_ENCODING_16BIT_INT_ENTRY_SIZE 4

/* 24 位有符号整数编码：取值范围 -8388608~8388607，占 4 字节。 */
#define LP_ENCODING_24BIT_INT 0xF2
#define LP_ENCODING_24BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_24BIT_INT(byte) (((byte)&LP_ENCODING_24BIT_INT_MASK)==LP_ENCODING_24BIT_INT)
#define LP_ENCODING_24BIT_INT_ENTRY_SIZE 5

/* 32 位有符号整数编码：取值范围 -2147483648~2147483647，占 5 字节。 */
#define LP_ENCODING_32BIT_INT 0xF3
#define LP_ENCODING_32BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_32BIT_INT(byte) (((byte)&LP_ENCODING_32BIT_INT_MASK)==LP_ENCODING_32BIT_INT)
#define LP_ENCODING_32BIT_INT_ENTRY_SIZE 6

/* 64 位有符号整数编码：占 9 字节。 */
#define LP_ENCODING_64BIT_INT 0xF4
#define LP_ENCODING_64BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_64BIT_INT(byte) (((byte)&LP_ENCODING_64BIT_INT_MASK)==LP_ENCODING_64BIT_INT)
#define LP_ENCODING_64BIT_INT_ENTRY_SIZE 10

/* 32 位字符串编码：长度 >= 4096，5 字节头部 + 字符串。 */
#define LP_ENCODING_32BIT_STR 0xF0
#define LP_ENCODING_32BIT_STR_MASK 0xFF
#define LP_ENCODING_IS_32BIT_STR(byte) (((byte)&LP_ENCODING_32BIT_STR_MASK)==LP_ENCODING_32BIT_STR)

/* listpack 结束标记字节 0xFF。 */
#define LP_EOF 0xFF

/* 从首字节低 6 位读取 6 位字符串的长度（最大 63）。 */
#define LP_ENCODING_6BIT_STR_LEN(p) ((p)[0] & 0x3F)
/* 从前两字节读取 12 位字符串的长度（最大 4095）。 */
#define LP_ENCODING_12BIT_STR_LEN(p) ((((p)[0] & 0xF) << 8) | (p)[1])
/* 从 5 字节头部读取 32 位字符串的长度。 */
#define LP_ENCODING_32BIT_STR_LEN(p) (((uint32_t)(p)[1]<<0) | \
                                      ((uint32_t)(p)[2]<<8) | \
                                      ((uint32_t)(p)[3]<<16) | \
                                      ((uint32_t)(p)[4]<<24))

/* 从 listpack 头部的 4 字节读取总字节数（小端序）。 */
#define lpGetTotalBytes(p)           (((uint32_t)(p)[0]<<0) | \
                                      ((uint32_t)(p)[1]<<8) | \
                                      ((uint32_t)(p)[2]<<16) | \
                                      ((uint32_t)(p)[3]<<24))

/* 从 listpack 头部的 2 字节读取元素数量（小端序）。 */
#define lpGetNumElements(p)          (((uint32_t)(p)[4]<<0) | \
                                      ((uint32_t)(p)[5]<<8))
/* 将总字节数 v 以小端序写入 listpack 头部的 4 字节。 */
#define lpSetTotalBytes(p,v) do { \
    (p)[0] = (v)&0xff; \
    (p)[1] = ((v)>>8)&0xff; \
    (p)[2] = ((v)>>16)&0xff; \
    (p)[3] = ((v)>>24)&0xff; \
} while(0)

/* 将元素数量 v 以小端序写入 listpack 头部的 2 字节。 */
#define lpSetNumElements(p,v) do { \
    (p)[4] = (v)&0xff; \
    (p)[5] = ((v)>>8)&0xff; \
} while(0)

/* 校验指针 'p' 处于 listpack 内部。
 * 所有返回 listpack 元素指针的函数都应确保该元素合法。
 * 一般而言 lpNext、lpDelete 等函数假定输入指针已经经过校验
 * （因为它通常是其它函数的返回值）。 */
#define ASSERT_INTEGRITY(lp, p) do { \
    assert((p) >= (lp)+LP_HDR_SIZE && (p) < (lp)+lpGetTotalBytes((lp))); \
} while (0)

/* 与上述宏类似，但会校验整个元素长度，而不只校验指针本身。 */
#define ASSERT_INTEGRITY_LEN(lp, p, len) do { \
    assert((p) >= (lp)+LP_HDR_SIZE && (p)+(len) < (lp)+lpGetTotalBytes((lp))); \
} while (0)

/* 校验单个 listpack 条目的合法性（静态内联前向声明）。 */
static inline void lpAssertValidEntry(unsigned char* lp, size_t lpbytes, unsigned char *p);

/* 出于安全考虑，禁止 listpack 超过 1GB，以避免 Total Bytes 头部字段溢出。 */
#define LISTPACK_MAX_SAFETY_SIZE (1<<30)
/* 判断给当前 listpack 追加 add 字节是否会超出 LISTPACK_MAX_SAFETY_SIZE。
 * 返回 1 表示安全，0 表示不安全。 */
int lpSafeToAdd(unsigned char* lp, size_t add) {
    size_t len = lp? lpGetTotalBytes(lp): 0;
    if (len + add > LISTPACK_MAX_SAFETY_SIZE)
        return 0;
    return 1;
}

/* Convert a string into a signed 64 bit integer.
 * The function returns 1 if the string could be parsed into a (non-overflowing)
 * signed 64 bit int, 0 otherwise. The 'value' will be set to the parsed value
 * when the function returns success.
 *
 * Note that this function demands that the string strictly represents
 * a int64 value: no spaces or other characters before or after the string
 * representing the number are accepted, nor zeroes at the start if not
 * for the string "0" representing the zero number.
 *
 * Because of its strictness, it is safe to use this function to check if
 * you can convert a string into a long long, and obtain back the string
 * from the number without any loss in the string representation. *
 *
 * -----------------------------------------------------------------------------
 *
 * Credits: this function was adapted from the Redis source code, file
 * "utils.c", function string2ll(), and is copyright:
 *
 * Copyright(C) 2011, Pieter Noordhuis
 * Copyright(C) 2011-current, Redis Ltd.
 *
 * The function is released under the BSD 3-clause license.
 */
int lpStringToInt64(const char *s, unsigned long slen, int64_t *value) {
    const char *p = s;
    unsigned long plen = 0;
    int negative = 0;
    uint64_t v;

    /* 长度为空或超过 LONG_STR_SIZE 时直接放弃，不可能是整数。 */
    if (slen == 0 || slen >= LONG_STR_SIZE)
        return 0;

    /* 特殊情况：唯一的字符是 '0'。 */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return 1;
    }

    if (p[0] == '-') {
        negative = 1;
        p++; plen++;

        /* 只有负号的情况视为非法。 */
        if (plen == slen)
            return 0;
    }

    /* 第一位必须是 1-9，否则整个字符串只能是 "0"。 */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0]-'0';
        p++; plen++;
    } else {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (UINT64_MAX / 10)) /* 溢出检查。 */
            return 0;
        v *= 10;

        if (v > (UINT64_MAX - (p[0]-'0'))) /* 溢出检查。 */
            return 0;
        v += p[0]-'0';

        p++; plen++;
    }

    /* 如果还有未消费的字符，说明字符串尾部还有非法字符，返回失败。 */
    if (plen < slen)
        return 0;

    if (negative) {
        if (v > ((uint64_t)(-(INT64_MIN+1))+1)) /* 负数溢出检查。 */
            return 0;
        if (value != NULL) *value = -v;
    } else {
        if (v > INT64_MAX) /* 正数溢出检查。 */
            return 0;
        if (value != NULL) *value = v;
    }
    return 1;
}

/* Create a new, empty listpack.
 * On success the new listpack is returned, otherwise an error is returned.
 * Pre-allocate at least `capacity` bytes of memory,
 * over-allocated memory can be shrunk by `lpShrinkToFit`.
 * */
unsigned char *lpNew(size_t capacity) {
    unsigned char *lp = lp_malloc(capacity > LP_HDR_SIZE+1 ? capacity : LP_HDR_SIZE+1);
    if (lp == NULL) return NULL;
    lpSetTotalBytes(lp,LP_HDR_SIZE+1);
    lpSetNumElements(lp,0);
    lp[LP_HDR_SIZE] = LP_EOF;
    return lp;
}

/* Free the specified listpack. */
void lpFree(unsigned char *lp) {
    lp_free(lp);
}

/* Shrink the memory to fit. */
unsigned char* lpShrinkToFit(unsigned char *lp) {
    size_t size = lpGetTotalBytes(lp);
    if (size < lp_malloc_size(lp)) {
        return lp_realloc(lp, size);
    } else {
        return lp;
    }
}

/* 将整数 v 的编码形式存入 intenc 缓冲区。
 * 如果 intenc 不为 NULL 则写入编码后的字节；
 * 如果 enclen 不为 NULL 则通过它返回编码占用的字节数。 */
static inline void lpEncodeIntegerGetType(int64_t v, unsigned char *intenc, uint64_t *enclen) {
    if (v >= 0 && v <= 127) {
        /* 单字节 0-127 整数编码。 */
        if (intenc != NULL) intenc[0] = v;
        if (enclen != NULL) *enclen = 1;
    } else if (v >= -4096 && v <= 4095) {
        /* 13 位有符号整数编码。 */
        if (v < 0) v = ((int64_t)1<<13)+v;
        if (intenc != NULL) {
            intenc[0] = (v>>8)|LP_ENCODING_13BIT_INT;
            intenc[1] = v&0xff;
        }
        if (enclen != NULL) *enclen = 2;
    } else if (v >= -32768 && v <= 32767) {
        /* 16 位有符号整数编码。 */
        if (v < 0) v = ((int64_t)1<<16)+v;
        if (intenc != NULL) {
            intenc[0] = LP_ENCODING_16BIT_INT;
            intenc[1] = v&0xff;
            intenc[2] = v>>8;
        }
        if (enclen != NULL) *enclen = 3;
    } else if (v >= -8388608 && v <= 8388607) {
        /* 24 位有符号整数编码。 */
        if (v < 0) v = ((int64_t)1<<24)+v;
        if (intenc != NULL) {
            intenc[0] = LP_ENCODING_24BIT_INT;
            intenc[1] = v&0xff;
            intenc[2] = (v>>8)&0xff;
            intenc[3] = v>>16;
        }
        if (enclen != NULL) *enclen = 4;
    } else if (v >= -2147483648 && v <= 2147483647) {
        /* 32 位有符号整数编码。 */
        if (v < 0) v = ((int64_t)1<<32)+v;
        if (intenc != NULL) {
            intenc[0] = LP_ENCODING_32BIT_INT;
            intenc[1] = v&0xff;
            intenc[2] = (v>>8)&0xff;
            intenc[3] = (v>>16)&0xff;
            intenc[4] = v>>24;
        }
        if (enclen != NULL) *enclen = 5;
    } else {
        /* 64 位有符号整数编码。 */
        uint64_t uv = v;
        if (intenc != NULL) {
            intenc[0] = LP_ENCODING_64BIT_INT;
            intenc[1] = uv&0xff;
            intenc[2] = (uv>>8)&0xff;
            intenc[3] = (uv>>16)&0xff;
            intenc[4] = (uv>>24)&0xff;
            intenc[5] = (uv>>32)&0xff;
            intenc[6] = (uv>>40)&0xff;
            intenc[7] = (uv>>48)&0xff;
            intenc[8] = uv>>56;
        }
        if (enclen != NULL) *enclen = 9;
    }
}

/* 判断长度为 size 的元素 ele 能否被编码为整数：
 * - 可以时返回 LP_ENCODING_INT，并把整数编码写入 intenc 缓冲区；
 * - 不可以时返回 LP_ENCODING_STRING。
 *
 * 不论返回哪种编码，都会通过 enclen 返回编码后占用的字节数。 */
static inline int lpEncodeGetType(unsigned char *ele, uint32_t size, unsigned char *intenc, uint64_t *enclen) {
    int64_t v;
    if (lpStringToInt64((const char*)ele, size, &v)) {
        lpEncodeIntegerGetType(v, intenc, enclen);
        return LP_ENCODING_INT;
    } else {
        if (size < 64) *enclen = 1+size;
        else if (size < 4096) *enclen = 2+size;
        else *enclen = 5+(uint64_t)size;
        return LP_ENCODING_STRING;
    }
}

/* 将代表前一个元素长度 l 的反向变长字段编码写入 buf。
 * 返回编码使用的字节数（1~5）。若 buf 为 NULL 则只返回需要的字节数。 */
static inline unsigned long lpEncodeBacklen(unsigned char *buf, uint64_t l) {
    if (l <= 127) {
        if (buf) buf[0] = l;
        return 1;
    } else if (l < 16383) {
        if (buf) {
            buf[0] = l>>7;
            buf[1] = (l&127)|128;
        }
        return 2;
    } else if (l < 2097151) {
        if (buf) {
            buf[0] = l>>14;
            buf[1] = ((l>>7)&127)|128;
            buf[2] = (l&127)|128;
        }
        return 3;
    } else if (l < 268435455) {
        if (buf) {
            buf[0] = l>>21;
            buf[1] = ((l>>14)&127)|128;
            buf[2] = ((l>>7)&127)|128;
            buf[3] = (l&127)|128;
        }
        return 4;
    } else {
        if (buf) {
            buf[0] = l>>28;
            buf[1] = ((l>>21)&127)|128;
            buf[2] = ((l>>14)&127)|128;
            buf[3] = ((l>>7)&127)|128;
            buf[4] = (l&127)|128;
        }
        return 5;
    }
}

/* 解码反向长度字段并返回结果。
 * 若编码异常（占用超过 5 字节），返回 UINT64_MAX 以标记错误。 */
static inline uint64_t lpDecodeBacklen(unsigned char *p) {
    uint64_t val = 0;
    uint64_t shift = 0;
    do {
        val |= (uint64_t)(p[0] & 127) << shift;
        if (!(p[0] & 128)) break;
        shift += 7;
        p--;
        if (shift > 28) return UINT64_MAX;
    } while(1);
    return val;
}

/* 将指针 s 指向的长度为 len 的字符串元素编码写入目标缓冲区 buf。
 * 调用本函数前应保证 buf 有足够空间（通常先调用 lpEncodeGetType() 评估长度）。 */
static inline void lpEncodeString(unsigned char *buf, unsigned char *s, uint32_t len) {
    if (len < 64) {
        buf[0] = len | LP_ENCODING_6BIT_STR;
        memcpy(buf+1,s,len);
    } else if (len < 4096) {
        buf[0] = (len >> 8) | LP_ENCODING_12BIT_STR;
        buf[1] = len & 0xff;
        memcpy(buf+2,s,len);
    } else {
        buf[0] = LP_ENCODING_32BIT_STR;
        buf[1] = len & 0xff;
        buf[2] = (len >> 8) & 0xff;
        buf[3] = (len >> 16) & 0xff;
        buf[4] = (len >> 24) & 0xff;
        memcpy(buf+5,s,len);
    }
}

/* 返回 listpack 元素 p 的编码总长度（包括编码字节、长度字节和数据本身）。
 * 如果元素编码非法，返回 0。
 * 注意：该函数可能会访问额外字节（12 位和 32 位字符串场景），
 * 因此只能用于已经经过校验的指针（通过 lpCurrentEncodedSizeBytes 或
 * ASSERT_INTEGRITY_LEN 校验，或来自其它已做校验的函数返回值）。 */
static inline uint32_t lpCurrentEncodedSizeUnsafe(unsigned char *p) {
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) return 1;
    if (LP_ENCODING_IS_6BIT_STR(p[0])) return 1+LP_ENCODING_6BIT_STR_LEN(p);
    if (LP_ENCODING_IS_13BIT_INT(p[0])) return 2;
    if (LP_ENCODING_IS_16BIT_INT(p[0])) return 3;
    if (LP_ENCODING_IS_24BIT_INT(p[0])) return 4;
    if (LP_ENCODING_IS_32BIT_INT(p[0])) return 5;
    if (LP_ENCODING_IS_64BIT_INT(p[0])) return 9;
    if (LP_ENCODING_IS_12BIT_STR(p[0])) return 2+LP_ENCODING_12BIT_STR_LEN(p);
    if (LP_ENCODING_IS_32BIT_STR(p[0])) return 5+LP_ENCODING_32BIT_STR_LEN(p);
    if (p[0] == LP_EOF) return 1;
    return 0;
}

/* 返回 listpack 元素 p 编码头所占的字节数
 * （仅包含编码字节和长度字节，不包含元素数据本身）。
 * 如果元素编码非法，返回 0。 */
static inline uint32_t lpCurrentEncodedSizeBytes(unsigned char *p) {
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) return 1;
    if (LP_ENCODING_IS_6BIT_STR(p[0])) return 1;
    if (LP_ENCODING_IS_13BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_16BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_24BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_32BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_64BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_12BIT_STR(p[0])) return 2;
    if (LP_ENCODING_IS_32BIT_STR(p[0])) return 5;
    if (p[0] == LP_EOF) return 1;
    return 0;
}

/* 跳过当前元素并返回下一个元素的位置。
 * 当 p 已经指向 listpack 末尾的 EOF 元素时不应调用本函数；
 * 不过作为 lpNext() 的内部实现，即使遇到 EOF 它也不会返回 NULL。 */
unsigned char *lpSkip(unsigned char *p) {
    unsigned long entrylen = lpCurrentEncodedSizeUnsafe(p);
    entrylen += lpEncodeBacklen(NULL,entrylen);
    p += entrylen;
    return p;
}

/* 若 p 指向 listpack 中的某个元素，调用 lpNext() 会返回
 * 下一个元素（右侧）的指针；若 p 已经指向最后一个元素，则返回 NULL。 */
unsigned char *lpNext(unsigned char *lp, unsigned char *p) {
    assert(p);
    p = lpSkip(p);
    if (p[0] == LP_EOF) return NULL;
    lpAssertValidEntry(lp, lpBytes(lp), p);
    return p;
}

/* 若 p 指向 listpack 中的某个元素，调用 lpPrev() 会返回
 * 上一个元素（左侧）的指针；若 p 已经指向第一个元素，则返回 NULL。 */
unsigned char *lpPrev(unsigned char *lp, unsigned char *p) {
    assert(p);
    if (p-lp == LP_HDR_SIZE) return NULL;
    p--; /* 定位到上一个元素的 backlen 首字节。 */
    uint64_t prevlen = lpDecodeBacklen(p);
    prevlen += lpEncodeBacklen(NULL,prevlen);
    p -= prevlen-1; /* 定位到上一个元素的首字节。 */
    lpAssertValidEntry(lp, lpBytes(lp), p);
    return p;
}

/* 返回 listpack 第一个元素的指针；若 listpack 为空则返回 NULL。 */
unsigned char *lpFirst(unsigned char *lp) {
    unsigned char *p = lp + LP_HDR_SIZE; /* 跳过头部。 */
    if (p[0] == LP_EOF) return NULL;
    lpAssertValidEntry(lp, lpBytes(lp), p);
    return p;
}

/* 返回 listpack 最后一个元素的指针；若 listpack 为空则返回 NULL。 */
unsigned char *lpLast(unsigned char *lp) {
    unsigned char *p = lp+lpGetTotalBytes(lp)-1; /* 定位到 EOF 元素。 */
    return lpPrev(lp,p); /* 当 EOF 是唯一元素时返回 NULL。 */
}

/* 返回 listpack 中元素的数量。
 * 优先使用头部缓存的 numele 字段；若该字段已经溢出（=LP_HDR_NUMELE_UNKNOWN）
 * 则需要扫描整个 listpack 才能得到准确数量。
 * 作为副作用，若扫描结果仍在头部 numele 字段可表示的范围内，
 * 会把新值写回头部缓存字段。 */
unsigned long lpLength(unsigned char *lp) {
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN) return numele;

    /* listpack 中元素过多，需要扫描以获取准确总数。 */
    uint32_t count = 0;
    unsigned char *p = lpFirst(lp);
    while(p) {
        count++;
        p = lpNext(lp,p);
    }

    /* 若新统计的 count 仍在头部 numele 字段可表示的范围内，把它写回头部。 */
    if (count < LP_HDR_NUMELE_UNKNOWN) lpSetNumElements(lp,count);
    return count;
}

/* 返回 listpack 中 p 所指元素的值。
 *
 * 函数行为取决于传入的 intbuf 参数。
 * 当 intbuf 为 NULL 时：
 * - 元素被编码为整数，则函数返回 NULL，并通过 count 返回整数值；
 * - 元素被编码为字符串，则返回指向 listpack 内部存储的字符串指针，
 *   并通过 count 返回字符串长度。
 *
 * 当 intbuf 指向调用者提供的缓冲区（至少 LP_INTBUF_SIZE 字节）时：
 * 函数总是按字符串的形式返回元素（即返回字符串指针并设置 count 为长度），
 * 但若元素实际是整数，则会用 intbuf 来保存该整数的字符串表示。
 *
 * 用户应根据用途选择调用形式：若要直接获取整数值，再传入缓冲区并
 * 转换回数字是毫无意义的。
 *
 * 若 entry_size 不为 NULL，则通过它返回该元素的整体长度
 * （包括编码字节、长度字节、数据本身和 backlen 字节）。
 *
 * 若调用方传入了一个错误编码的 listpack（无法正常解析），
 * 函数会返回一个看上去像是 12345678900000000 + <无法识别的字节>
 * 形式的整数。这可以被视作一种出错提示。考虑到本库的使用场景各异，
 * 在这种场景下直接崩溃并不合适。
 *
 * 同样地，由于 listpack 通常被认为是合法的，本函数不返回错误码，
 * 否则会带来非常高的 API 成本。 */
static inline unsigned char *lpGetWithSize(unsigned char *p, int64_t *count, unsigned char *intbuf, uint64_t *entry_size) {
    int64_t val;
    uint64_t uval, negstart, negmax;

    assert(p); /* 给 valgrind 的断言（避免空指针解引用）。 */
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) {
        negstart = UINT64_MAX; /* 7 位整数始终为非负数。 */
        negmax = 0;
        uval = p[0] & 0x7f;
        if (entry_size) *entry_size = LP_ENCODING_7BIT_UINT_ENTRY_SIZE;
    } else if (LP_ENCODING_IS_6BIT_STR(p[0])) {
        *count = LP_ENCODING_6BIT_STR_LEN(p);
        if (entry_size) *entry_size = 1 + *count + lpEncodeBacklen(NULL, *count + 1);
        return p+1;
    } else if (LP_ENCODING_IS_13BIT_INT(p[0])) {
        uval = ((p[0]&0x1f)<<8) | p[1];
        negstart = (uint64_t)1<<12;
        negmax = 8191;
        if (entry_size) *entry_size = LP_ENCODING_13BIT_INT_ENTRY_SIZE;
    } else if (LP_ENCODING_IS_16BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8;
        negstart = (uint64_t)1<<15;
        negmax = UINT16_MAX;
        if (entry_size) *entry_size = LP_ENCODING_16BIT_INT_ENTRY_SIZE;
    } else if (LP_ENCODING_IS_24BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8 |
               (uint64_t)p[3]<<16;
        negstart = (uint64_t)1<<23;
        negmax = UINT32_MAX>>8;
        if (entry_size) *entry_size = LP_ENCODING_24BIT_INT_ENTRY_SIZE;
    } else if (LP_ENCODING_IS_32BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8 |
               (uint64_t)p[3]<<16 |
               (uint64_t)p[4]<<24;
        negstart = (uint64_t)1<<31;
        negmax = UINT32_MAX;
        if (entry_size) *entry_size = LP_ENCODING_32BIT_INT_ENTRY_SIZE;
    } else if (LP_ENCODING_IS_64BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8 |
               (uint64_t)p[3]<<16 |
               (uint64_t)p[4]<<24 |
               (uint64_t)p[5]<<32 |
               (uint64_t)p[6]<<40 |
               (uint64_t)p[7]<<48 |
               (uint64_t)p[8]<<56;
        negstart = (uint64_t)1<<63;
        negmax = UINT64_MAX;
        if (entry_size) *entry_size = LP_ENCODING_64BIT_INT_ENTRY_SIZE;
    } else if (LP_ENCODING_IS_12BIT_STR(p[0])) {
        *count = LP_ENCODING_12BIT_STR_LEN(p);
        if (entry_size) *entry_size = 2 + *count + lpEncodeBacklen(NULL, *count + 2);
        return p+2;
    } else if (LP_ENCODING_IS_32BIT_STR(p[0])) {
        *count = LP_ENCODING_32BIT_STR_LEN(p);
        if (entry_size) *entry_size = 5 + *count + lpEncodeBacklen(NULL, *count + 5);
        return p+5;
    } else {
        uval = 12345678900000000ULL + p[0];
        negstart = UINT64_MAX;
        negmax = 0;
    }

    /* 只有整数编码才会走到这段代码。
     * 使用二进制补码规则把无符号值转换为有符号值。 */
    if (uval >= negstart) {
        /* 这一段三步转换可以避免无符号到有符号转换时的未定义行为。 */
        uval = negmax-uval;
        val = uval;
        val = -val-1;
    } else {
        val = uval;
    }

    /* 根据 intbuf 是否为 NULL，决定返回整数的字符串表示还是整数值本身。 */
    if (intbuf) {
        *count = ll2string((char*)intbuf,LP_INTBUF_SIZE,(long long)val);
        return intbuf;
    } else {
        *count = val;
        return NULL;
    }
}

/* 获取 listpack 元素值（不返回 entry_size 的简化版本）。 */
unsigned char *lpGet(unsigned char *p, int64_t *count, unsigned char *intbuf) {
    return lpGetWithSize(p, count, intbuf, NULL);
}

/* lpGet() 的便捷包装，能够直接拿到元素的字符串值或整数值。
 * 返回 NULL 时通过 *lval 返回整数值；
 * 否则返回指向 listpack 内部字符串的指针，并通过 *slen 返回字符串长度。 */
unsigned char *lpGetValue(unsigned char *p, unsigned int *slen, long long *lval) {
    unsigned char *vstr;
    int64_t ele_len;

    vstr = lpGet(p, &ele_len, NULL);
    if (vstr) {
        *slen = ele_len;
    } else {
        *lval = ele_len;
    }
    return vstr;
}

/* This is just a wrapper to lpGet() that is able to get an integer from an entry directly.
 * Returns 1 and stores the integer in 'lval' if the entry is an integer.
 * Returns 0 if the entry is a string. */
int lpGetIntegerValue(unsigned char *p, long long *lval) {
    int64_t ele_len;
    if (!lpGet(p, &ele_len, NULL)) {
        *lval = ele_len;
        return 1;
    }
    return 0;
}

/* 使用自定义比较回调查找 listpack 中的元素。
 *
 * cmp 是比较回调：返回 0 时表示当前元素命中，会被返回；
 * user 会被透传给 cmp。skip 表示每次比较之间要跳过的元素数。
 * 找不到时返回 NULL。 */
unsigned char *lpFindCb(unsigned char *lp, unsigned char *p,
                        void *user, lpCmp cmp, unsigned int skip)
{
    int skipcnt = 0;
    unsigned char *value;
    int64_t ll;
    uint64_t entry_size = 123456789; /* 初始化以避免编译器告警。 */
    uint32_t lp_bytes = lpBytes(lp);

    if (!p)
        p = lpFirst(lp);

    while (p) {
        if (skipcnt == 0) {
            value = lpGetWithSize(p, &ll, NULL, &entry_size);
            if (value) {
                /* 在访问元素值前，确保它没有越过 listpack 的边界。 */
                assert(p >= lp + LP_HDR_SIZE && p + entry_size < lp + lp_bytes);
            }

            if (cmp(lp, p, user, value, ll) == 0)
                return p;

            /* 重置 skip 计数。 */
            skipcnt = skip;
            p += entry_size;
        } else {
            /* 跳过元素。 */
            skipcnt--;

            /* 直接移动到下一个条目；不使用 lpNext 以避免 lpNext
             * 中的 lpAssertValidEntry 调用 lpBytes 造成性能下降。 */
            p = lpSkip(p);
        }

        /* 下一次调用 lpGetWithSize 时最多会读取 p 之后 8 个字节，
         * 因此只在必要时才使用较慢的校验路径。 */
        if (p + 8 >= lp + lp_bytes)
            lpAssertValidEntry(lp, lp_bytes, p);
        else
            assert(p >= lp + LP_HDR_SIZE && p < lp + lp_bytes);
        if (p[0] == LP_EOF) break;
    }

    return NULL;
}

/* 传递给 lpFindCmp 的参数结构体：保存查找目标及其整数编码状态。 */
struct lpFindArg {
    unsigned char *s; /* 要查找的字符串项。 */
    uint32_t slen;    /* 要查找的字符串项长度。 */
    int vencoding;
    int64_t vll;
};

/* 用于 lpFind 的元素比较回调。 */
static inline int lpFindCmp(const unsigned char *lp, unsigned char *p,
                            void *user, unsigned char *s, long long slen) {
    (void) lp;
    (void) p;
    struct lpFindArg *arg = user;

    if (s) {
        if (slen == arg->slen && memcmp(arg->s, s, slen) == 0) {
            return 0;
        }
    } else {
        /* 判断要查找的字段是否能被编码为整数。
         * 只在第一次比较时进行此判断，之后 vencoding 会被置为非 0，
         * vll 会被设置为对应的整数值。 */
        if (arg->vencoding == 0) {
            /* 若可以编码为整数则置 1，否则置为 UCHAR_MAX，
             * 这样后续比较就不会再重复尝试。 */
            if (arg->slen >= 32 || arg->slen == 0 || !lpStringToInt64((const char*)arg->s, arg->slen, &arg->vll)) {
                arg->vencoding = UCHAR_MAX;
            } else {
                arg->vencoding = 1;
            }
        }

        /* 仅当 vencoding != UCHAR_MAX 时才比较，因为
         * 如果字段无法被编码为整数，就不可能匹配到整数值。 */
        if (arg->vencoding != UCHAR_MAX && slen == arg->vll) {
            return 0;
        }
    }

    return 1;
}

/* 在 listpack 中查找等于 s（长度 slen）的元素。
 * 每次比较之间跳过 skip 个元素。
 * 找不到时返回 NULL。 */
unsigned char *lpFind(unsigned char *lp, unsigned char *p, unsigned char *s,
                      uint32_t slen, unsigned int skip)
{
    struct lpFindArg arg = {
        .s = s,
        .slen = slen
    };
    return lpFindCb(lp, p, &arg, lpFindCmp, skip);
}

/* 在 listpack 的指定位置 p 处插入、删除或替换元素。
 * p 必须是由 lpFirst()、lpLast()、lpNext()、lpPrev() 或 lpSeek() 取得的合法指针。
 *
 * 通过 where 参数指定操作：插入到 p 之前（LP_BEFORE）、插入到 p 之后（LP_AFTER）、
 * 或替换 p 所指的元素（LP_REPLACE）。
 *
 * - 若 elestr 和 eleint 都为 NULL，则删除 p 所指的元素；
 * - 若 eleint 非 NULL，则 size 是 eleint 缓冲区的字节数，
 *   函数会把该 64 位整数插入或替换；
 * - 若 elestr 非 NULL，则 size 是 elestr 的长度，
 *   函数会把该字符串插入或替换。
 *
 * 失败时（内存不足或新长度超过 2^32-1）返回 NULL；
 * 成功时返回新的 listpack 指针（传入的旧指针不再有效）。
 *
 * 若 newp 非 NULL，则在调用结束时 *newp 会被设置为新插入元素的地址，
 * 以便继续使用 lpNext()、lpPrev() 进行遍历。
 *
 * 对于删除操作，*newp 会被设置为被删元素右侧的下一个元素；
 * 若被删元素已是最后一个，则 *newp 为 NULL。 */
unsigned char *lpInsert(unsigned char *lp, unsigned char *elestr, unsigned char *eleint,
                        uint32_t size, unsigned char *p, int where, unsigned char **newp)
{
    unsigned char intenc[LP_MAX_INT_ENCODING_LEN];
    unsigned char backlen[LP_MAX_BACKLEN_SIZE];

    uint64_t enclen; /* 编码后元素的长度。 */
    int delete = (elestr == NULL && eleint == NULL);

    /* 删除操作本质上是用一个零长元素替换原元素，
     * 因此无论调用方传入什么 where，都强制设置为 LP_REPLACE。 */
    if (delete) where = LP_REPLACE;

    /* 如果需要在当前元素之后插入，则直接跳到下一个元素（可能是 EOF），
     * 然后按 LP_BEFORE 的逻辑处理。这样函数实际上只需要处理
     * LP_BEFORE 和 LP_REPLACE 两种情况。 */
    if (where == LP_AFTER) {
        p = lpSkip(p);
        where = LP_BEFORE;
        ASSERT_INTEGRITY(lp, p);
    }

    /* 记下元素 p 相对于 lp 的偏移，以便在可能的 realloc 之后
     * 重新计算其地址。 */
    unsigned long poff = p-lp;

    int enctype;
    if (elestr) {
        /* 调用 lpEncodeGetType() 时，如果该元素可以表示为整数，
         * 则会把整数编码写入 intenc 缓冲区，并返回 LP_ENCODING_INT。
         * 否则返回 LP_ENCODING_STR，稍后需要调用 lpEncodeString()
         * 把字符串实际写入目标位置。
         *
         * 不论返回何种编码，都会通过 enclen 返回编码后占用的字节数。 */
        enctype = lpEncodeGetType(elestr,size,intenc,&enclen);
        if (enctype == LP_ENCODING_INT) eleint = intenc;
    } else if (eleint) {
        enctype = LP_ENCODING_INT;
        enclen = size; /* size 此时是编码后整数元素的字节数。 */
    } else {
        enctype = -1;
        enclen = 0;
    }

    /* 还需要把可反向解析的长度字段（backlen）也编码到条目末尾，
     * 以便从 listpack 尾部向头部遍历。 */
    unsigned long backlen_size = (!delete) ? lpEncodeBacklen(backlen,enclen) : 0;
    uint64_t old_listpack_bytes = lpGetTotalBytes(lp);
    uint32_t replaced_len  = 0;
    if (where == LP_REPLACE) {
        replaced_len = lpCurrentEncodedSizeUnsafe(p);
        replaced_len += lpEncodeBacklen(NULL,replaced_len);
        ASSERT_INTEGRITY_LEN(lp, p, replaced_len);
    }

    uint64_t new_listpack_bytes = old_listpack_bytes + enclen + backlen_size
                                  - replaced_len;
    if (new_listpack_bytes > UINT32_MAX) return NULL;

    /* 现在需要重新分配内存以腾出空间（或在 LP_REPLACE 且新元素
     * 更小时缩小分配）。如果最终分配会变大，则在 memmove 之前 realloc；
     * 否则在 memmove 之后再 realloc。 */

    unsigned char *dst = lp + poff; /* realloc 后可能需要更新。 */

    /* 扩容前 realloc：我们需要更多空间。 */
    if (new_listpack_bytes > old_listpack_bytes &&
        new_listpack_bytes > lp_malloc_size(lp)) {
        if ((lp = lp_realloc(lp,new_listpack_bytes)) == NULL) return NULL;
        dst = lp + poff;
    }

    /* 通过 memmove 让出恰好需要的空间用于存储新元素。 */
    if (where == LP_BEFORE) {
        memmove(dst+enclen+backlen_size,dst,old_listpack_bytes-poff);
    } else { /* LP_REPLACE。 */
        memmove(dst+enclen+backlen_size,
                dst+replaced_len,
                old_listpack_bytes-poff-replaced_len);
    }

    /* 缩容后 realloc：释放多余空间。 */
    if (new_listpack_bytes < old_listpack_bytes) {
        if ((lp = lp_realloc(lp,new_listpack_bytes)) == NULL) return NULL;
        dst = lp + poff;
    }

    /* 写入条目。 */
    if (newp) {
        *newp = dst;
        /* 对于删除操作，若 dst 已指向 EOF 元素，则把 *newp 设为 NULL。 */
        if (delete && dst[0] == LP_EOF) *newp = NULL;
    }
    if (!delete) {
        if (enctype == LP_ENCODING_INT) {
            memcpy(dst,eleint,enclen);
        } else if (elestr) {
            lpEncodeString(dst,elestr,size);
        } else {
            redis_unreachable();
        }
        dst += enclen;
        memcpy(dst,backlen,backlen_size);
        dst += backlen_size;
    }

    /* 更新头部字段。 */
    if (where != LP_REPLACE || delete) {
        uint32_t num_elements = lpGetNumElements(lp);
        if (num_elements != LP_HDR_NUMELE_UNKNOWN) {
            if (!delete)
                lpSetNumElements(lp,num_elements+1);
            else
                lpSetNumElements(lp,num_elements-1);
        }
    }
    lpSetTotalBytes(lp,new_listpack_bytes);

#if 0
    /* 这段代码通常处于关闭状态：它的作用是强制 listpack 在每次修改之后
     * 都返回一个新的指针，即使原分配已经足够。这有助于发现
     * 调用方忘记更新 listpack 引用变量的 bug。 */
    unsigned char *oldlp = lp;
    lp = lp_malloc(new_listpack_bytes);
    memcpy(lp,oldlp,new_listpack_bytes);
    if (newp) {
        unsigned long offset = (*newp)-oldlp;
        *newp = lp + offset;
    }
    /* 确保旧分配中的内容变成“垃圾”以便尽早暴露使用已释放内存的 bug。 */
    memset(oldlp,'A',new_listpack_bytes);
    lp_free(oldlp);
#endif

    return lp;
}

/* Insert the specified elements with 'entries' and 'len' at the specified
 * position 'p', with 'p' being a listpack element pointer obtained with
 * lpFirst(), lpLast(), lpNext(), lpPrev() or lpSeek().
 *
 * This is similar to lpInsert() but allows you to insert batch of entries in
 * one call. This function is more efficient than inserting entries one by one
 * as it does single realloc()/memmove() calls for all the entries.
 *
 * In each listpackEntry, if 'sval' is  not null, it is assumed entry is string
 * and 'sval' and 'slen' will be used. Otherwise, 'lval' will be used to append
 * the integer entry.
 *
 * The elements are inserted before or after the element pointed by 'p'
 * depending on the 'where' argument, that can be LP_BEFORE or LP_AFTER.
 *
 * If 'newp' is not NULL, at the end of a successful call '*newp' will be set
 * to the address of the element just added, so that it will be possible to
 * continue an interaction with lpNext() and lpPrev().
 *
 * Returns NULL on out of memory or when the listpack total length would exceed
 * the max allowed size of 2^32-1, otherwise the new pointer to the listpack
 * holding the new element is returned (and the old pointer passed is no longer
 * considered valid). */
unsigned char *lpBatchInsert(unsigned char *lp, unsigned char *p, int where,
                             listpackEntry *entries, unsigned int len,
                             unsigned char **newp)
{
    assert(where == LP_BEFORE || where == LP_AFTER);
    assert(entries != NULL && len > 0);

    struct listpackInsertEntry {
        int enctype;
        uint64_t enclen;
        unsigned char intenc[LP_MAX_INT_ENCODING_LEN];
        unsigned char backlen[LP_MAX_BACKLEN_SIZE];
        unsigned long backlen_size;
    };

    uint64_t addedlen = 0;       /* 所有新元素编码后的总长度。 */
    struct listpackInsertEntry tmp[3];  /* 编码后条目的本地缓冲区。 */
    struct listpackInsertEntry *enc = tmp;

    if (len > sizeof(tmp) / sizeof(struct listpackInsertEntry)) {
        /* 当 len 超过本地缓冲区大小时，在堆上分配。 */
        enc = zmalloc(len * sizeof(struct listpackInsertEntry));
    }

    /* 如果需要在当前元素之后插入，则直接跳到下一个元素（可能是 EOF），
     * 然后按 LP_BEFORE 的逻辑处理。这样函数实际上只处理 LP_BEFORE。 */
    if (where == LP_AFTER) {
        p = lpSkip(p);
        where = LP_BEFORE;
        ASSERT_INTEGRITY(lp, p);
    }

    for (unsigned int i = 0; i < len; i++) {
        listpackEntry *e = &entries[i];
        if (e->sval) {
           /* 调用 lpEncodeGetType()：如果元素能表示为整数，
            * 则把整数编码存入 intenc 并返回 LP_ENCODING_INT；
            * 否则返回 LP_ENCODING_STR，后续需要调用
            * lpEncodeString() 真正把字符串写到目标位置。
            *
            * 不论哪种编码，enclen 都会被设置为编码后的字节数。 */
            enc[i].enctype = lpEncodeGetType(e->sval, e->slen,
                                             enc[i].intenc, &enc[i].enclen);
        } else {
            enc[i].enctype = LP_ENCODING_INT;
            lpEncodeIntegerGetType(e->lval, enc[i].intenc, &enc[i].enclen);
        }
        addedlen += enc[i].enclen;

        /* 还需要把可反向解析的长度（backlen）也编码到条目末尾，
         * 以便从 listpack 尾部向头部遍历。 */
        enc[i].backlen_size = lpEncodeBacklen(enc[i].backlen, enc[i].enclen);
        addedlen += enc[i].backlen_size;
    }

    uint64_t old_listpack_bytes = lpGetTotalBytes(lp);
    uint64_t new_listpack_bytes = old_listpack_bytes + addedlen;
    if (new_listpack_bytes > UINT32_MAX) return NULL;

    /* 记下元素 p 相对于 lp 的偏移，以便在 realloc 后重新计算其地址。 */
    unsigned long poff = p-lp;
    unsigned char *dst = lp + poff; /* realloc 后可能需要更新。 */

    /* 扩容前 realloc：我们需要更多空间。 */
    if (new_listpack_bytes > old_listpack_bytes &&
        new_listpack_bytes > lp_malloc_size(lp)) {
        if ((lp = lp_realloc(lp,new_listpack_bytes)) == NULL) return NULL;
        dst = lp + poff;
    }

    /* 通过 memmove 腾出精确空间用于存储新元素。 */
    memmove(dst+addedlen,dst,old_listpack_bytes-poff);

    for (unsigned int i = 0; i < len; i++) {
        listpackEntry *ent = &entries[i];

        if (newp)
            *newp = dst;

        if (enc[i].enctype == LP_ENCODING_INT)
            memcpy(dst, enc[i].intenc, enc[i].enclen);
        else
            lpEncodeString(dst, ent->sval, ent->slen);

        dst += enc[i].enclen;
        memcpy(dst, enc[i].backlen, enc[i].backlen_size);
        dst += enc[i].backlen_size;
    }

    /* 更新头部字段。 */
    uint32_t num_elements = lpGetNumElements(lp);
    if (num_elements != LP_HDR_NUMELE_UNKNOWN) {
        if ((int64_t) len > (int64_t) LP_HDR_NUMELE_UNKNOWN - (int64_t) num_elements)
            lpSetNumElements(lp, LP_HDR_NUMELE_UNKNOWN);
        else
            lpSetNumElements(lp,num_elements + len);
    }
    lpSetTotalBytes(lp,new_listpack_bytes);
    if (enc != tmp) lp_free(enc);

    return lp;
}

/* lpInsert() 的便捷包装，直接使用字符串作为待插入元素。 */
unsigned char *lpInsertString(unsigned char *lp, unsigned char *s, uint32_t slen,
                              unsigned char *p, int where, unsigned char **newp)
{
    return lpInsert(lp, s, NULL, slen, p, where, newp);
}

/* lpInsert() 的便捷包装，直接使用 64 位整数作为待插入元素。 */
unsigned char *lpInsertInteger(unsigned char *lp, long long lval, unsigned char *p, int where, unsigned char **newp) {
    uint64_t enclen; /* 编码后元素的长度。 */
    unsigned char intenc[LP_MAX_INT_ENCODING_LEN];

    lpEncodeIntegerGetType(lval, intenc, &enclen);
    return lpInsert(lp, NULL, intenc, enclen, p, where, newp);
}

/* 将字符串元素 s（长度 slen）插入到 listpack 的头部。 */
unsigned char *lpPrepend(unsigned char *lp, unsigned char *s, uint32_t slen) {
    unsigned char *p = lpFirst(lp);
    if (!p) return lpAppend(lp, s, slen);
    return lpInsert(lp, s, NULL, slen, p, LP_BEFORE, NULL);
}

/* 将整数元素 lval 插入到 listpack 的头部。 */
unsigned char *lpPrependInteger(unsigned char *lp, long long lval) {
    unsigned char *p = lpFirst(lp);
    if (!p) return lpAppendInteger(lp, lval);
    return lpInsertInteger(lp, lval, p, LP_BEFORE, NULL);
}

/* 将字符串元素 ele（长度 size）追加到 listpack 的尾部。
 * 内部通过 lpInsert() 实现，因此返回值含义与 lpInsert() 相同。 */
unsigned char *lpAppend(unsigned char *lp, unsigned char *ele, uint32_t size) {
    uint64_t listpack_bytes = lpGetTotalBytes(lp);
    unsigned char *eofptr = lp + listpack_bytes - 1;
    return lpInsert(lp,ele,NULL,size,eofptr,LP_BEFORE,NULL);
}

/* 将整数元素 lval 追加到 listpack 的尾部。 */
unsigned char *lpAppendInteger(unsigned char *lp, long long lval) {
    uint64_t listpack_bytes = lpGetTotalBytes(lp);
    unsigned char *eofptr = lp + listpack_bytes - 1;
    return lpInsertInteger(lp, lval, eofptr, LP_BEFORE, NULL);
}

/* Append batch of entries to the listpack.
 *
 * This call is more efficient than multiple lpAppend() calls as it only does
 * a single realloc() for all the given entries.
 *
 * In each listpackEntry, if 'sval' is  not null, it is assumed entry is string
 * and 'sval' and 'slen' will be used. Otherwise, 'lval' will be used to append
 * the integer entry. */
unsigned char *lpBatchAppend(unsigned char *lp, listpackEntry *entries, unsigned long len) {
    uint64_t listpack_bytes = lpGetTotalBytes(lp);
    unsigned char *eofptr = lp + listpack_bytes - 1;
    return lpBatchInsert(lp, eofptr, LP_BEFORE, entries, len, NULL);
}

/* lpInsert() 的便捷包装，直接使用字符串替换当前元素。
 * 返回新的 listpack 指针，并通过 *p 更新当前游标位置。 */
unsigned char *lpReplace(unsigned char *lp, unsigned char **p, unsigned char *s, uint32_t slen) {
    return lpInsert(lp, s, NULL, slen, *p, LP_REPLACE, p);
}

/* lpInsertInteger() 的便捷包装，直接使用 64 位整数替换当前元素。
 * 返回新的 listpack 指针，并通过 *p 更新当前游标位置。 */
unsigned char *lpReplaceInteger(unsigned char *lp, unsigned char **p, long long lval) {
    return lpInsertInteger(lp, lval, *p, LP_REPLACE, p);
}

/* 删除 p 所指的元素并返回新 listpack。
 * 若 newp 非 NULL，则通过它返回被删元素右侧的下一个元素指针；
 * 若被删元素已是最后一个，则 *newp 为 NULL。 */
unsigned char *lpDelete(unsigned char *lp, unsigned char *p, unsigned char **newp) {
    return lpInsert(lp,NULL,NULL,0,p,LP_REPLACE,newp);
}

/* 从 *p 所指的元素起，连续删除 num 个元素，并更新 *p。 */
unsigned char *lpDeleteRangeWithEntry(unsigned char *lp, unsigned char **p, unsigned long num) {
    size_t bytes = lpBytes(lp);
    unsigned long deleted = 0;
    unsigned char *eofptr = lp + bytes - 1;
    unsigned char *first, *tail;
    first = tail = *p;

    if (num == 0) return lp;  /* 没有要删除的元素，立即返回。 */

    /* 找到待删除区段之后的下一个条目作为 tail。
     * 由于数据可能已损坏，lpLength 不可靠，因此不能简单把 num 当作元素数。 */
    while (num--) {
        deleted++;
        tail = lpSkip(tail);
        if (tail[0] == LP_EOF) break;
        lpAssertValidEntry(lp, bytes, tail);
    }

    /* 记下元素 first 相对于 lp 的偏移，以便在 realloc 后重定位。 */
    unsigned long poff = first-lp;

    /* 把 tail 之后的剩余内容前移以覆盖被删除的区间。 */
    memmove(first, tail, eofptr - tail + 1);
    lpSetTotalBytes(lp, bytes - (tail - first));
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN)
        lpSetNumElements(lp, numele-deleted);
    lp = lpShrinkToFit(lp);

    /* 把 *p 重新指向 first 所在的位置；若已指向 EOF 则置为 NULL。 */
    *p = lp+poff;
    if ((*p)[0] == LP_EOF) *p = NULL;

    return lp;
}

/* 删除 listpack 中从 index 起的 num 个元素。 */
unsigned char *lpDeleteRange(unsigned char *lp, long index, unsigned long num) {
    unsigned char *p;
    uint32_t numele = lpGetNumElements(lp);

    if (num == 0) return lp; /* 没有要删除的元素，立即返回。 */
    if ((p = lpSeek(lp, index)) == NULL) return lp;

    /* 如果已知删除范围超出 listpack 末尾，可以直接移动 EOF 标记，
     * 而无需遍历所有元素。
     * 但如果无法确定元素总数，就避免调用 lpLength，以免多一次完整扫描。
     *
     * 注意 index 自身可能溢出，但在 seek 后使用的是已规范化的值，
     * 因此后续运算不会再次溢出。 */
    if (numele != LP_HDR_NUMELE_UNKNOWN && index < 0) index = (long)numele + index;
    if (numele != LP_HDR_NUMELE_UNKNOWN && (numele - (unsigned long)index) <= num) {
        p[0] = LP_EOF;
        lpSetTotalBytes(lp, p - lp + 1);
        lpSetNumElements(lp, index);
        lp = lpShrinkToFit(lp);
    } else {
        lp = lpDeleteRangeWithEntry(lp, &p, num);
    }

    return lp;
}

/* 批量删除 ps 数组中的 count 个元素并返回新 listpack。
 * 数组中元素的顺序必须与它们在 listpack 中的顺序一致。 */
unsigned char *lpBatchDelete(unsigned char *lp, unsigned char **ps, unsigned long count) {
    if (count == 0) return lp;
    unsigned char *dst = ps[0];
    size_t total_bytes = lpGetTotalBytes(lp);
    unsigned char *lp_end = lp + total_bytes; /* 指向 EOF 之后的位置。 */
    assert(lp_end[-1] == LP_EOF);
    /*
     * ----+--------+-----------+--------+---------+-----+---+
     * ... | 删除   | 保留      | 删除   | 保留    | ... |EOF|
     * ... |xxxxxxxx|           |xxxxxxxx|         | ... |   |
     * ----+--------+-----------+--------+---------+-----+---+
     *     ^        ^           ^                            ^
     *     |        |           |                            |
     *     ps[i]    |           ps[i+1]                      |
     *     skip     keep_start  keep_end                     lp_end
     *
     * 循环把 keep_start 到 keep_end 之间的字节 memmove 到 dst。
     */
    for (unsigned long i = 0; i < count; i++) {
        unsigned char *skip = ps[i];
        assert(skip != NULL && skip[0] != LP_EOF);
        unsigned char *keep_start = lpSkip(skip);
        unsigned char *keep_end;
        if (i + 1 < count) {
            keep_end = ps[i + 1];
            /* 相邻元素都被删除时，二者之间没有需要保留的内容。 */
            if (keep_start == keep_end) continue;
        } else {
            /* 保留 listpack 中剩余的全部内容（包括 EOF 标记）。 */
            keep_end = lp_end;
        }
        assert(keep_end > keep_start);
        size_t bytes_to_keep = keep_end - keep_start;
        memmove(dst, keep_start, bytes_to_keep);
        dst += bytes_to_keep;
    }
    /* 更新总字节数和元素数量。 */
    size_t deleted_bytes = lp_end - dst;
    total_bytes -= deleted_bytes;
    assert(lp[total_bytes - 1] == LP_EOF);
    lpSetTotalBytes(lp, total_bytes);
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN) lpSetNumElements(lp, numele - count);
    return lpShrinkToFit(lp);
}

/* Merge listpacks 'first' and 'second' by appending 'second' to 'first'.
 *
 * NOTE: The larger listpack is reallocated to contain the new merged listpack.
 * Either 'first' or 'second' can be used for the result.  The parameter not
 * used will be free'd and set to NULL.
 *
 * After calling this function, the input parameters are no longer valid since
 * they are changed and free'd in-place.
 *
 * The result listpack is the contents of 'first' followed by 'second'.
 *
 * On failure: returns NULL if the merge is impossible.
 * On success: returns the merged listpack (which is expanded version of either
 * 'first' or 'second', also frees the other unused input listpack, and sets the
 * input listpack argument equal to newly reallocated listpack return value. */
unsigned char *lpMerge(unsigned char **first, unsigned char **second) {
    /* 任一参数为 NULL 时无法合并，直接返回 NULL。 */
    if (first == NULL || *first == NULL || second == NULL || *second == NULL)
        return NULL;

    /* 不能把同一个 listpack 合并到自身。 */
    if (*first == *second)
        return NULL;

    size_t first_bytes = lpBytes(*first);
    unsigned long first_len = lpLength(*first);

    size_t second_bytes = lpBytes(*second);
    unsigned long second_len = lpLength(*second);

    int append;
    unsigned char *source, *target;
    size_t target_bytes, source_bytes;
    /* 选择较大的 listpack 作为 realloc 目标，以便原地扩展。
     * 同时记录是向目标 listpack 追加还是前置。 */
    if (first_bytes >= second_bytes) {
        /* 保留 first，把 second 追加到 first 后面。 */
        target = *first;
        target_bytes = first_bytes;
        source = *second;
        source_bytes = second_bytes;
        append = 1;
    } else {
        /* 否则保留 second，把 first 前置到 second 前面。 */
        target = *second;
        target_bytes = second_bytes;
        source = *first;
        source_bytes = first_bytes;
        append = 0;
    }

    /* 计算合并后的总字节数（需要扣掉一对头部和 EOF 标记）。 */
    unsigned long long lpbytes = (unsigned long long)first_bytes + second_bytes - LP_HDR_SIZE - 1;
    assert(lpbytes < UINT32_MAX); /* 更大的值无法被存储。 */
    unsigned long lplength = first_len + second_len;

    /* 合并后的元素数量上限为 UINT16_MAX。 */
    lplength = lplength < UINT16_MAX ? lplength : UINT16_MAX;

    /* 把 target 扩展到新大小，然后追加或前置 source。 */
    target = lp_realloc(target, lpbytes);
    if (append) {
        /* append == 向 target 追加。 */
        /* 把 source 复制到 target 末尾（覆盖原来的 EOF）：
         *   [TARGET - END, SOURCE - HEADER] */
        memcpy(target + target_bytes - 1,
               source + LP_HDR_SIZE,
               source_bytes - LP_HDR_SIZE);
    } else {
        /* !append == 向 target 前置。 */
        /* 把 target 的内容后移 source-END 大小的距离，
         * 然后把 source 复制到腾出的位置：
         *   [SOURCE - END, TARGET - HEADER] */
        memmove(target + source_bytes - 1,
                target + LP_HDR_SIZE,
                target_bytes - LP_HDR_SIZE);
        memcpy(target, source, source_bytes - 1);
    }

    lpSetNumElements(target, lplength);
    lpSetTotalBytes(target, lpbytes);

    /* 释放并清空未被 realloc 的那个 listpack。 */
    if (append) {
        lp_free(*second);
        *second = NULL;
        *first = target;
    } else {
        lp_free(*first);
        *first = NULL;
        *second = target;
    }

    return target;
}

/* 复制一个完全相同的 listpack。 */
unsigned char *lpDup(unsigned char *lp) {
    size_t lpbytes = lpBytes(lp);
    unsigned char *newlp = lp_malloc(lpbytes);
    memcpy(newlp, lp, lpbytes);
    return newlp;
}

/* 返回 listpack 整体占用的字节数。 */
size_t lpBytes(unsigned char *lp) {
    return lpGetTotalBytes(lp);
}

/* 返回将整数 lval 编码进 listpack 后所需的字节数。 */
size_t lpEntrySizeInteger(long long lval) {
    uint64_t enclen;
    lpEncodeIntegerGetType(lval, NULL, &enclen);
    unsigned long backlen = lpEncodeBacklen(NULL, enclen);
    return enclen + backlen;
}

/* 估算由 rep 个相同整数 lval 构成的 listpack 的总字节数。 */
size_t lpEstimateBytesRepeatedInteger(long long lval, unsigned long rep) {
    return LP_HDR_SIZE + lpEntrySizeInteger(lval) * rep + 1;
}

/* 按索引定位 listpack 中的元素，并返回该元素的指针。
 * 正向索引：从头部起 0 基元素下标；
 * 负向索引：从尾部起定位（-1 表示最后一个元素，-2 表示倒数第二个，依此类推）。
 * 越界时返回 NULL。 */
unsigned char *lpSeek(unsigned char *lp, long index) {
    int forward = 1; /* 默认正向遍历。 */

    /* 根据 listpack 长度和目标元素位置决定正向或反向遍历。
     * 如果无法以 O(1) 时间拿到元素总数，则总是从左向右遍历。 */
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN) {
        if (index < 0) index = (long)numele+index;
        if (index < 0) return NULL; /* 规范化后仍为负，说明越界。 */
        if (index >= (long)numele) return NULL; /* 反方向越界。 */
        /* 如果目标元素位于 listpack 后半段，则改为从右向左遍历。 */
        if (index > (long)numele/2) {
            forward = 0;
            /* 从右向左遍历时使用负索引，因此把 index 转换为负数形式。 */
            index -= numele;
        }
    } else {
        /* 当元素总数未知时，对于负索引总是从右向左遍历。 */
        if (index < 0) forward = 0;
    }

    /* 正向 / 反向遍历分别基于 lpNext() / lpPrev() 实现。 */
    if (forward) {
        unsigned char *ele = lpFirst(lp);
        while (index > 0 && ele) {
            ele = lpNext(lp,ele);
            index--;
        }
        return ele;
    } else {
        unsigned char *ele = lpLast(lp);
        while (index < -1 && ele) {
            ele = lpPrev(lp,ele);
            index++;
        }
        return ele;
    }
}

/* 与 lpFirst 类似但不做完整性断言，专供紧接着调用 lpValidateNext 时使用。 */
unsigned char *lpValidateFirst(unsigned char *lp) {
    unsigned char *p = lp + LP_HDR_SIZE; /* 跳过头部。 */
    if (p[0] == LP_EOF) return NULL;
    return p;
}

/* 校验单个 listpack 条目的合法性，并把指针推进到下一个条目。
 * 入参 pp 是当前记录的引用，调用结束后会被推进。
 * 返回 1 表示合法，0 表示非法。 */
int lpValidateNext(unsigned char *lp, unsigned char **pp, size_t lpbytes) {
#define OUT_OF_RANGE(p) ( \
        (p) < lp + LP_HDR_SIZE || \
        (p) > lp + lpbytes - 1)
    unsigned char *p = *pp;
    if (!p)
        return 0;

    /* 在访问 p 之前先确保其处于合法范围内。 */
    if (OUT_OF_RANGE(p))
        return 0;

    if (*p == LP_EOF) {
        *pp = NULL;
        return 1;
    }

    /* 检查是否可以读取编码长度。 */
    uint32_t lenbytes = lpCurrentEncodedSizeBytes(p);
    if (!lenbytes)
        return 0;

    /* 确保编码后的条目长度不会越过 listpack 边界。 */
    if (OUT_OF_RANGE(p + lenbytes))
        return 0;

    /* 获取条目长度以及编码后的 backlen。 */
    unsigned long entrylen = lpCurrentEncodedSizeUnsafe(p);
    unsigned long encodedBacklen = lpEncodeBacklen(NULL,entrylen);
    entrylen += encodedBacklen;

    /* 确保整个条目不会越过 listpack 边界。 */
    if (OUT_OF_RANGE(p + entrylen))
        return 0;

    /* 移动到下一个条目。 */
    p += entrylen;

    /* 校验条目末尾的反向长度字段是否与条目开头的编码长度一致。 */
    uint64_t prevlen = lpDecodeBacklen(p-1);
    if (prevlen + encodedBacklen != entrylen)
        return 0;

    *pp = p;
    return 1;
#undef OUT_OF_RANGE
}

/* 断言 p 所指的条目完全位于 listpack 分配范围内。 */
static inline void lpAssertValidEntry(unsigned char* lp, size_t lpbytes, unsigned char *p) {
    assert(lpValidateNext(lp, &p, lpbytes));
}

/* 校验 listpack 数据结构的完整性。
 * deep = 0 时仅校验头部的完整性；
 * deep = 1 时会逐个条目扫描，并可调用 entry_cb 对每个条目做额外校验。 */
int lpValidateIntegrity(unsigned char *lp, size_t size, int deep,
                        listpackValidateEntryCB entry_cb, void *cb_userdata) {
    /* 检查能否读取头部（以及 EOF）。 */
    if (size < LP_HDR_SIZE + 1)
        return 0;

    /* 头部编码的总字节数必须与分配的 size 相等。 */
    size_t bytes = lpGetTotalBytes(lp);
    if (bytes != size)
        return 0;

    /* 最后一个字节必须是 EOF 结束标记。 */
    if (lp[size-1] != LP_EOF)
        return 0;

    if (!deep)
        return 1;

    /* 逐个校验条目。 */
    uint32_t count = 0;
    uint32_t numele = lpGetNumElements(lp);
    unsigned char *p = lp + LP_HDR_SIZE;
    while(p && p[0] != LP_EOF) {
        unsigned char *prev = p;

        /* 先校验并把 p 推进到下一个条目，避免损坏的 listpack 导致回调崩溃。 */
        if (!lpValidateNext(lp, &p, bytes))
            return 0;

        /* 可选地调用调用方提供的回调对条目进行额外校验。 */
        if (entry_cb && !entry_cb(prev, numele, cb_userdata))
            return 0;

        count++;
    }

    /* 确保 p 正好指向 listpack 的末尾。 */
    if (p != lp + size - 1)
        return 0;

    /* 检查头部中的元素数量是否正确。 */
    if (numele != LP_HDR_NUMELE_UNKNOWN && numele != count)
        return 0;

    return 1;
}

/* 比较 p 所指的条目与字符串 s（长度 slen）是否相等，相等返回 1。 */
unsigned int lpCompare(unsigned char *p, unsigned char *s, uint32_t slen) {
    unsigned char *value;
    int64_t sz;
    if (p[0] == LP_EOF) return 0;

    value = lpGet(p, &sz, NULL);
    if (value) {
        return (slen == sz) && memcmp(value,s,slen) == 0;
    } else {
        /* 使用 lpStringToInt64() 把字符串 s 转成整数再和 sz 比较，
         * 比起把整数转回字符串再 memcmp 要快得多。 */
        int64_t sval;
        if (lpStringToInt64((const char*)s, slen, &sval))
            return sz == sval;
    }

    return 0;
}

/* uint compare for qsort */
static int uintCompare(const void *a, const void *b) {
    return (*(unsigned int *) a - *(unsigned int *) b);
}

/* 把字符串（val/len）或整数（lval）保存到 listpackEntry 结构 dest 中的辅助函数。 */
static inline void lpSaveValue(unsigned char *val, unsigned int len, int64_t lval, listpackEntry *dest) {
    dest->sval = val;
    dest->slen = len;
    dest->lval = lval;
}

/* 随机选择一对 key 和 value。
 * total_count 是预先计算的 listpack 长度的一半（避免每次调用 lpLength）。
 * key 和 val 用于存放随机抽取的键值对；若不需要 value 可把 val 置为 NULL。
 * tuple_len 表示单个逻辑项由多少个连续 entry 组成。
 * 当 listpack 以 key-value 形式存储时，tuple_len 应为 2；
 * 若是 key-value-... (n_entries) 形式则相应增大。 */
void lpRandomPair(unsigned char *lp, unsigned long total_count,
                  listpackEntry *key, listpackEntry *val, int tuple_len)
{
    unsigned char *p;

    assert(tuple_len >= 2);

    /* 避免在损坏的 listpack 上出现除零。 */
    assert(total_count);

    int r = (rand() % total_count) * tuple_len;
    assert((p = lpSeek(lp, r)));
    key->sval = lpGetValue(p, &(key->slen), &(key->lval));

    if (!val)
        return;
    assert((p = lpNext(lp, p)));
    val->sval = lpGetValue(p, &(val->slen), &(val->lval));
}

/* 随机选择 count 个元素并存入 entries 数组。
 * 调用方需要保证 entries 至少有 count 个 listpackEntry 的空间。
 * 结果顺序是随机的，且可能出现重复。 */
void lpRandomEntries(unsigned char *lp, unsigned int count, listpackEntry *entries) {
    struct pick {
        unsigned int index;
        unsigned int order;
    } *picks = lp_malloc(count * sizeof(struct pick));
    unsigned int total_size = lpLength(lp);
    assert(total_size);
    for (unsigned int i = 0; i < count; i++) {
        picks[i].index = rand() % total_size;
        picks[i].order = i;
    }

    /* 按 index 排序。 */
    qsort(picks, count, sizeof(struct pick), uintCompare);

    /* 按 index 顺序遍历 listpack，并把元素值写回 entries 数组中原来的位置。 */
    unsigned char *p = lpFirst(lp);
    unsigned int j = 0; /* listpack 中的当前位置。 */
    for (unsigned int i = 0; i < count; i++) {
        /* 推进 listpack 指针直到第 picks[i].index 个元素。 */
        while (j < picks[i].index) {
            p = lpNext(lp, p);
            j++;
        }
        int storeorder = picks[i].order;
        unsigned int len = 0;
        long long llval = 0;
        unsigned char *str = lpGetValue(p, &len, &llval);
        lpSaveValue(str, len, llval, &entries[storeorder]);
    }
    lp_free(picks);
}

/* 随机选择 count 对 key/value 并写入 keys 和 vals。
 * 抽取顺序是随机的，且可能重复（不保证唯一）。
 * vals 可为 NULL，此时跳过 value 的保存。
 * tuple_len 含义同 lpRandomPair。 */
void lpRandomPairs(unsigned char *lp, unsigned int count, listpackEntry *keys, listpackEntry *vals, int tuple_len) {
    unsigned char *p, *key, *value;
    unsigned int klen = 0, vlen = 0;
    long long klval = 0, vlval = 0;

    assert(tuple_len >= 2);

    /* 注意：index 成员必须放在最前面，因为它会被 uintCompare 访问。 */
    typedef struct {
        unsigned int index;
        unsigned int order;
    } rand_pick;
    rand_pick *picks = lp_malloc(sizeof(rand_pick)*count);
    unsigned int total_size = lpLength(lp)/tuple_len;

    /* 避免在损坏的 listpack 上出现除零。 */
    assert(total_size);

    /* 生成一批随机索引（允许重复）。 */
    for (unsigned int i = 0; i < count; i++) {
        /* 生成 key 所在的 entry 索引。 */
        picks[i].index = (rand() % total_size) * tuple_len;
        /* 记录本次随机抽取对应的原始顺序。 */
        picks[i].order = i;
    }

    /* 按索引排序。 */
    qsort(picks, count, sizeof(rand_pick), uintCompare);

    /* 按索引顺序从 listpack 中取出元素，并按原始顺序写回输出数组。 */
    unsigned int lpindex = picks[0].index, pickindex = 0;
    p = lpSeek(lp, lpindex);
    while (p && pickindex < count) {
        key = lpGetValue(p, &klen, &klval);
        assert((p = lpNext(lp, p)));
        value = lpGetValue(p, &vlen, &vlval);
        while (pickindex < count && lpindex == picks[pickindex].index) {
            int storeorder = picks[pickindex].order;
            lpSaveValue(key, klen, klval, &keys[storeorder]);
            if (vals)
                lpSaveValue(value, vlen, vlval, &vals[storeorder]);
             pickindex++;
        }
        lpindex += tuple_len;

        for (int i = 0; i < tuple_len - 1; i++) {
            p = lpNext(lp, p);
        }
    }

    lp_free(picks);
}

/* 随机选择 count 对唯一的（不重复的）key/value 写入 keys 和 vals。
 * vals 可为 NULL。
 * tuple_len 含义同 lpRandomPair。
 * 当 listpack 中数据不足时，返回值可能小于请求的 count。 */
unsigned int lpRandomPairsUnique(unsigned char *lp, unsigned int count,
                                 listpackEntry *keys, listpackEntry *vals,
                                 int tuple_len)
{
    assert(tuple_len >= 2);

    unsigned char *p, *key;
    unsigned int klen = 0;
    long long klval = 0;
    unsigned int total_size = lpLength(lp)/tuple_len;
    unsigned int index = 0;
    if (count > total_size)
        count = total_size;

    p = lpFirst(lp);
    unsigned int picked = 0, remaining = count;
    while (picked < count && p) {
        assert((p = lpNextRandom(lp, p, &index, remaining, tuple_len)));
        key = lpGetValue(p, &klen, &klval);
        lpSaveValue(key, klen, klval, &keys[picked]);
        assert((p = lpNext(lp, p)));
        index++;
        if (vals) {
            key = lpGetValue(p, &klen, &klval);
            lpSaveValue(key, klen, klval, &vals[picked]);
        }
        p = lpNext(lp, p);
        remaining--;
        picked++;
        index++;
    }
    return picked;
}

/* 在保持整体只遍历一遍的前提下，按概率选出下一个唯一的随机元素。
 * 调用时假定还需要在从起始元素 p（含）到 list 末尾之间挑选
 * remaining 个不重复的元素。
 * index 需要按当前起始元素 p 的零基索引进行初始化，
 * 函数返回时会被更新为所选元素的零基索引。
 * tuple_len 表示一个逻辑项由多少个连续 entry 组成，
 * 例如当 listpack 表示 key-value 对时应设为 2，此时只会挑选偶数索引的元素。
 *
 * 注意：本函数可能返回入参 p 本身。若想跳过已返回的元素，
 * 需要在每次调用 lpNextRandom() 之后调用 lpNext() 或 lpDelete()。
 * 用法示例：
 *
 *     assert(remaining <= lpLength(lp));
 *     p = lpFirst(lp);
 *     i = 0;
 *     while (remaining > 0) {
 *         p = lpNextRandom(lp, p, &i, remaining--, 1);
 *
 *         // ... 使用 p ...
 *
 *         p = lpNext(lp, p);
 *         i++;
 *     }
 */
unsigned char *lpNextRandom(unsigned char *lp, unsigned char *p, unsigned int *index,
                            unsigned int remaining, int tuple_len)
{
    assert(tuple_len > 0);
    /* 为了只遍历一次，每次尝试挑选一个成员时，挑选它的概率等于
     * 剩余需要挑选的数量 / 还未访问的元素数量。这样可以让
     * 每个成员被选中的概率相等。 */
    unsigned int i = *index;
    unsigned int total_size = lpLength(lp);
    while (i < total_size && p != NULL) {
        if (i % tuple_len != 0) {
            p = lpNext(lp, p);
            i++;
            continue;
        }

        /* 是否挑选当前元素？ */
        unsigned int available = (total_size - i) / tuple_len;
        double randomDouble = ((double)rand()) / RAND_MAX;
        double threshold = ((double)remaining) / available;
        if (randomDouble <= threshold) {
            *index = i;
            return p;
        }

        p = lpNext(lp, p);
        i++;
    }

    return NULL;
}

/* 打印 listpack 的调试信息（用于 DEBUG 命令）。 */
void lpRepr(unsigned char *lp) {
    unsigned char *p, *vstr;
    int64_t vlen;
    unsigned char intbuf[LP_INTBUF_SIZE];
    int index = 0;

    printf("{total bytes %zu} {num entries %lu}\n", lpBytes(lp), lpLength(lp));
        
    p = lpFirst(lp);
    while(p) {
        uint32_t encoded_size_bytes = lpCurrentEncodedSizeBytes(p);
        uint32_t encoded_size = lpCurrentEncodedSizeUnsafe(p);
        unsigned long back_len = lpEncodeBacklen(NULL, encoded_size);
        printf(
            "{\n"
                "\taddr: 0x%08lx,\n"
                "\tindex: %2d,\n"
                "\toffset: %1lu,\n"
                "\thdr+entrylen+backlen: %2lu,\n"
                "\thdrlen: %3u,\n"
                "\tbacklen: %2lu,\n"
                "\tpayload: %1u\n",
            (long unsigned)p,
            index,
            (unsigned long) (p-lp),
            encoded_size + back_len,
            encoded_size_bytes,
            back_len,
            encoded_size - encoded_size_bytes);
        printf("\tbytes: ");
        for (unsigned int i = 0; i < (encoded_size + back_len); i++) {
            printf("%02x|",p[i]);
        }
        printf("\n");

        vstr = lpGet(p, &vlen, intbuf);
        printf("\t[str]");
        if (vlen > 40) {
            if (fwrite(vstr, 40, 1, stdout) == 0) perror("fwrite");
            printf("...");
        } else {
            if (fwrite(vstr, vlen, 1, stdout) == 0) perror("fwrite");
        }
        printf("\n}\n");
        index++;
        p = lpNext(lp, p);
    }
    printf("{end}\n\n");
}

#ifdef REDIS_TEST

#include <sys/time.h>
#include "adlist.h"
#include "sds.h"
#include "testhelp.h"

/* 测试代码用宏：避免未使用参数告警；打印测试用例标题。 */
#define UNUSED(x) (void)(x)
#define TEST(name) printf("test — %s\n", name);

/* 测试用的混合字符串列表。 */
char *mixlist[] = {"hello", "foo", "quux", "1024"};
/* 测试用的整数与字符串混合列表。 */
char *intlist[] = {"4294967296", "-100", "100", "128000",
                   "non integer", "much much longer non integer"};

/* 创建一个混合内容的 listpack（用于大多数测试）。 */
static unsigned char *createList(void) {
    unsigned char *lp = lpNew(0);
    lp = lpAppend(lp, (unsigned char*)mixlist[1], strlen(mixlist[1]));
    lp = lpAppend(lp, (unsigned char*)mixlist[2], strlen(mixlist[2]));
    lp = lpPrepend(lp, (unsigned char*)mixlist[0], strlen(mixlist[0]));
    lp = lpAppend(lp, (unsigned char*)mixlist[3], strlen(mixlist[3]));
    return lp;
}

/* 创建一个包含整数与字符串的 listpack。 */
static unsigned char *createIntList(void) {
    unsigned char *lp = lpNew(0);
    lp = lpAppend(lp, (unsigned char*)intlist[2], strlen(intlist[2]));
    lp = lpAppend(lp, (unsigned char*)intlist[3], strlen(intlist[3]));
    lp = lpPrepend(lp, (unsigned char*)intlist[1], strlen(intlist[1]));
    lp = lpPrepend(lp, (unsigned char*)intlist[0], strlen(intlist[0]));
    lp = lpAppend(lp, (unsigned char*)intlist[4], strlen(intlist[4]));
    lp = lpAppend(lp, (unsigned char*)intlist[5], strlen(intlist[5]));
    return lp;
}

/* 获取当前微秒级时间戳（用于性能基准测试）。 */
static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

/* 压力测试：在指定位置反复进行 push+pop 操作。 */
static void stress(int pos, int num, int maxsize, int dnum) {
    int i, j, k;
    unsigned char *lp;
    char posstr[2][5] = { "HEAD", "TAIL" };
    long long start;
    for (i = 0; i < maxsize; i+=dnum) {
        lp = lpNew(0);
        for (j = 0; j < i; j++) {
            lp = lpAppend(lp, (unsigned char*)"quux", 4);
        }

        /* 在指定位置执行 num 次 push + pop。 */
        start = usec();
        for (k = 0; k < num; k++) {
            if (pos == 0) {
                lp = lpPrepend(lp, (unsigned char*)"quux", 4);
            } else {
                lp = lpAppend(lp, (unsigned char*)"quux", 4);

            }
            lp = lpDelete(lp, lpFirst(lp), NULL);
        }
        printf("List size: %8d, bytes: %8zu, %dx push+pop (%s): %6lld usec\n",
               i, lpBytes(lp), num, posstr[pos], usec()-start);
        lpFree(lp);
    }
}

/* 弹出 listpack 的头部或尾部元素并打印其值。 */
static unsigned char *pop(unsigned char *lp, int where) {
    unsigned char *p, *vstr;
    int64_t vlen;

    p = lpSeek(lp, where == 0 ? 0 : -1);
    vstr = lpGet(p, &vlen, NULL);
    if (where == 0)
        printf("Pop head: ");
    else
        printf("Pop tail: ");

    if (vstr) {
        if (vlen && fwrite(vstr, vlen, 1, stdout) == 0) perror("fwrite");
    } else {
        printf("%lld", (long long)vlen);
    }

    printf("\n");
    return lpDelete(lp, p, &p);
}

/* 在 target 中生成随机字符串，长度在 [min, max] 区间内。 */
static int randstring(char *target, unsigned int min, unsigned int max) {
    int p = 0;
    int len = min+rand()%(max-min+1);
    int minval, maxval;
    switch(rand() % 3) {
    case 0:
        minval = 0;
        maxval = 255;
    break;
    case 1:
        minval = 48;
        maxval = 122;
    break;
    case 2:
        minval = 48;
        maxval = 52;
    break;
    default:
        assert(NULL);
    }

    while(p < len)
        target[p++] = minval+rand()%(maxval-minval+1);
    return len;
}

/* 断言 p 所指条目与字符串 s（长度 slen）相等。 */
static void verifyEntry(unsigned char *p, unsigned char *s, size_t slen) {
    assert(lpCompare(p, s, slen));
}

/* lpValidateIntegrity 在深度校验时使用的回调：
 * 断言当前条目与 mixlist 中按顺序的字符串相等。 */
static int lpValidation(unsigned char *p, unsigned int head_count, void *userdata) {
    UNUSED(p);
    UNUSED(head_count);

    int ret;
    long *count = userdata;
    ret = lpCompare(p, (unsigned char *)mixlist[*count], strlen(mixlist[*count]));
    (*count)++;
    return ret;
}

/* 测试用：lpFindCb 的字符串/整数比较回调。 */
static int lpFindCbCmp(const unsigned char *lp, unsigned char *p, void *user, unsigned char *s, long long slen) {
    assert(lp);
    assert(p);

    char *n = user;

    if (!s) {
        int64_t sval;
        if (lpStringToInt64((const char*)n, strlen(n), &sval))
            return slen == sval ? 0 : 1;
    } else {
        if (strlen(n) == (size_t) slen && memcmp(n, s, slen) == 0)
            return 0;
    }

    return 1;
}

/* listpack 单元测试入口：覆盖创建、插入、删除、查找、批量操作、
 * 随机访问、完整性校验等主要功能，并附带性能基准测试。 */
int listpackTest(int argc, char *argv[], int flags) {
    UNUSED(argc);
    UNUSED(argv);

    int i;
    unsigned char *lp, *p, *vstr;
    int64_t vlen;
    unsigned char intbuf[LP_INTBUF_SIZE];
    int accurate = (flags & REDIS_TEST_ACCURATE);

    TEST("Create int list") {
        lp = createIntList();
        assert(lpLength(lp) == 6);
        lpFree(lp);
    }

    TEST("Create list") {
        lp = createList();
        assert(lpLength(lp) == 4);
        lpFree(lp);
    }

    TEST("Test lpPrepend") {
        lp = lpNew(0);
        lp = lpPrepend(lp, (unsigned char*)"abc", 3);
        lp = lpPrepend(lp, (unsigned char*)"1024", 4);
        verifyEntry(lpSeek(lp, 0), (unsigned char*)"1024", 4);
        verifyEntry(lpSeek(lp, 1), (unsigned char*)"abc", 3);
        lpFree(lp);
    }

    TEST("Test lpPrependInteger") {
        lp = lpNew(0);
        lp = lpPrependInteger(lp, 127);
        lp = lpPrependInteger(lp, 4095);
        lp = lpPrependInteger(lp, 32767);
        lp = lpPrependInteger(lp, 8388607);
        lp = lpPrependInteger(lp, 2147483647);
        lp = lpPrependInteger(lp, 9223372036854775807);
        verifyEntry(lpSeek(lp, 0), (unsigned char*)"9223372036854775807", 19);
        verifyEntry(lpSeek(lp, -1), (unsigned char*)"127", 3);
        lpFree(lp);
    }

    TEST("Get element at index") {
        lp = createList();
        verifyEntry(lpSeek(lp, 0), (unsigned char*)"hello", 5);
        verifyEntry(lpSeek(lp, 3), (unsigned char*)"1024", 4);
        verifyEntry(lpSeek(lp, -1), (unsigned char*)"1024", 4);
        verifyEntry(lpSeek(lp, -4), (unsigned char*)"hello", 5);
        assert(lpSeek(lp, 4) == NULL);
        assert(lpSeek(lp, -5) == NULL);
        lpFree(lp);
    }
    
    TEST("Pop list") {
        lp = createList();
        lp = pop(lp, 1);
        lp = pop(lp, 0);
        lp = pop(lp, 1);
        lp = pop(lp, 1);
        lpFree(lp);
    }

    TEST("Get element at index") {
        lp = createList();
        verifyEntry(lpSeek(lp, 0), (unsigned char*)"hello", 5);
        verifyEntry(lpSeek(lp, 3), (unsigned char*)"1024", 4);
        verifyEntry(lpSeek(lp, -1), (unsigned char*)"1024", 4);
        verifyEntry(lpSeek(lp, -4), (unsigned char*)"hello", 5);
        assert(lpSeek(lp, 4) == NULL);
        assert(lpSeek(lp, -5) == NULL);
        lpFree(lp);
    }

    TEST("Iterate list from 0 to end") {
        lp = createList();
        p = lpFirst(lp);
        i = 0;
        while (p) {
            verifyEntry(p, (unsigned char*)mixlist[i], strlen(mixlist[i]));
            p = lpNext(lp, p);
            i++;
        }
        lpFree(lp);
    }
    
    TEST("Iterate list from 1 to end") {
        lp = createList();
        i = 1;
        p = lpSeek(lp, i);
        while (p) {
            verifyEntry(p, (unsigned char*)mixlist[i], strlen(mixlist[i]));
            p = lpNext(lp, p);
            i++;
        }
        lpFree(lp);
    }
    
    TEST("Iterate list from 2 to end") {
        lp = createList();
        i = 2;
        p = lpSeek(lp, i);
        while (p) {
            verifyEntry(p, (unsigned char*)mixlist[i], strlen(mixlist[i]));
            p = lpNext(lp, p);
            i++;
        }
        lpFree(lp);
    }
    
    TEST("Iterate from back to front") {
        lp = createList();
        p = lpLast(lp);
        i = 3;
        while (p) {
            verifyEntry(p, (unsigned char*)mixlist[i], strlen(mixlist[i]));
            p = lpPrev(lp, p);
            i--;
        }
        lpFree(lp);
    }
    
    TEST("Iterate from back to front, deleting all items") {
        lp = createList();
        p = lpLast(lp);
        i = 3;
        while ((p = lpLast(lp))) {
            verifyEntry(p, (unsigned char*)mixlist[i], strlen(mixlist[i]));
            lp = lpDelete(lp, p, &p);
            assert(p == NULL);
            i--;
        }
        lpFree(lp);
    }

    TEST("Delete whole listpack when num == -1");
    {
        lp = createList();
        lp = lpDeleteRange(lp, 0, -1);
        assert(lpLength(lp) == 0);
        assert(lp[LP_HDR_SIZE] == LP_EOF);
        assert(lpBytes(lp) == (LP_HDR_SIZE + 1));
        zfree(lp);

        lp = createList();
        unsigned char *ptr = lpFirst(lp);
        lp = lpDeleteRangeWithEntry(lp, &ptr, -1);
        assert(lpLength(lp) == 0);
        assert(lp[LP_HDR_SIZE] == LP_EOF);
        assert(lpBytes(lp) == (LP_HDR_SIZE + 1));
        zfree(lp);
    }

    TEST("Delete whole listpack with negative index");
    {
        lp = createList();
        lp = lpDeleteRange(lp, -4, 4);
        assert(lpLength(lp) == 0);
        assert(lp[LP_HDR_SIZE] == LP_EOF);
        assert(lpBytes(lp) == (LP_HDR_SIZE + 1));
        zfree(lp);

        lp = createList();
        unsigned char *ptr = lpSeek(lp, -4);
        lp = lpDeleteRangeWithEntry(lp, &ptr, 4);
        assert(lpLength(lp) == 0);
        assert(lp[LP_HDR_SIZE] == LP_EOF);
        assert(lpBytes(lp) == (LP_HDR_SIZE + 1));
        zfree(lp);
    }

    TEST("Delete inclusive range 0,0");
    {
        lp = createList();
        lp = lpDeleteRange(lp, 0, 1);
        assert(lpLength(lp) == 3);
        assert(lpSkip(lpLast(lp))[0] == LP_EOF); /* check set LP_EOF correctly */
        zfree(lp);

        lp = createList();
        unsigned char *ptr = lpFirst(lp);
        lp = lpDeleteRangeWithEntry(lp, &ptr, 1);
        assert(lpLength(lp) == 3);
        assert(lpSkip(lpLast(lp))[0] == LP_EOF); /* check set LP_EOF correctly */
        zfree(lp);
    }

    TEST("Delete inclusive range 0,1");
    {
        lp = createList();
        lp = lpDeleteRange(lp, 0, 2);
        assert(lpLength(lp) == 2);
        verifyEntry(lpFirst(lp), (unsigned char*)mixlist[2], strlen(mixlist[2]));
        zfree(lp);

        lp = createList();
        unsigned char *ptr = lpFirst(lp);
        lp = lpDeleteRangeWithEntry(lp, &ptr, 2);
        assert(lpLength(lp) == 2);
        verifyEntry(lpFirst(lp), (unsigned char*)mixlist[2], strlen(mixlist[2]));
        zfree(lp);
    }

    TEST("Delete inclusive range 1,2");
    {
        lp = createList();
        lp = lpDeleteRange(lp, 1, 2);
        assert(lpLength(lp) == 2);
        verifyEntry(lpFirst(lp), (unsigned char*)mixlist[0], strlen(mixlist[0]));
        zfree(lp);

        lp = createList();
        unsigned char *ptr = lpSeek(lp, 1);
        lp = lpDeleteRangeWithEntry(lp, &ptr, 2);
        assert(lpLength(lp) == 2);
        verifyEntry(lpFirst(lp), (unsigned char*)mixlist[0], strlen(mixlist[0]));
        zfree(lp);
    }
    
    TEST("Delete with start index out of range");
    {
        lp = createList();
        lp = lpDeleteRange(lp, 5, 1);
        assert(lpLength(lp) == 4);
        zfree(lp);
    }

    TEST("Delete with num overflow");
    {
        lp = createList();
        lp = lpDeleteRange(lp, 1, 5);
        assert(lpLength(lp) == 1);
        verifyEntry(lpFirst(lp), (unsigned char*)mixlist[0], strlen(mixlist[0]));
        zfree(lp);

        lp = createList();
        unsigned char *ptr = lpSeek(lp, 1);
        lp = lpDeleteRangeWithEntry(lp, &ptr, 5);
        assert(lpLength(lp) == 1);
        verifyEntry(lpFirst(lp), (unsigned char*)mixlist[0], strlen(mixlist[0]));
        zfree(lp);
    }

    TEST("Batch append") {
        listpackEntry ent[6] = {
                {.sval = (unsigned char*)mixlist[0], .slen = strlen(mixlist[0])},
                {.sval = (unsigned char*)mixlist[1], .slen = strlen(mixlist[1])},
                {.sval = (unsigned char*)mixlist[2], .slen = strlen(mixlist[2])},
                {.lval = 4294967296},
                {.sval = (unsigned char*)mixlist[3], .slen = strlen(mixlist[3])},
                {.lval = -100}
        };

        lp = lpNew(0);
        lp = lpBatchAppend(lp, ent, 2);
        verifyEntry(lpSeek(lp, 0), ent[0].sval, ent[0].slen);
        verifyEntry(lpSeek(lp, 1), ent[1].sval, ent[1].slen);
        assert(lpLength(lp) == 2);

        lp = lpBatchAppend(lp, &ent[2], 1);
        verifyEntry(lpSeek(lp, 0), ent[0].sval, ent[0].slen);
        verifyEntry(lpSeek(lp, 1), ent[1].sval, ent[1].slen);
        verifyEntry(lpSeek(lp, 2), ent[2].sval, ent[2].slen);
        assert(lpLength(lp) == 3);

        lp = lpDeleteRange(lp, 1, 1);
        verifyEntry(lpSeek(lp, 0), ent[0].sval, ent[0].slen);
        verifyEntry(lpSeek(lp, 1), ent[2].sval, ent[2].slen);
        assert(lpLength(lp) == 2);

        lp = lpBatchAppend(lp, &ent[3], 3);
        verifyEntry(lpSeek(lp, 0), ent[0].sval, ent[0].slen);
        verifyEntry(lpSeek(lp, 1), ent[2].sval, ent[2].slen);
        verifyEntry(lpSeek(lp, 2), (unsigned char*) "4294967296", 10);
        verifyEntry(lpSeek(lp, 3), ent[4].sval, ent[4].slen);
        verifyEntry(lpSeek(lp, 4), (unsigned char*) "-100", 4);
        assert(lpLength(lp) == 5);

        lp = lpDeleteRange(lp, 1, 3);
        verifyEntry(lpSeek(lp, 0), ent[0].sval, ent[0].slen);
        verifyEntry(lpSeek(lp, 1), (unsigned char*) "-100", 4);
        assert(lpLength(lp) == 2);

        lpFree(lp);
    }

    TEST("Batch insert") {
        lp = lpNew(0);
        listpackEntry ent[6] = {
                {.sval = (unsigned char*)mixlist[0], .slen = strlen(mixlist[0])},
                {.sval = (unsigned char*)mixlist[1], .slen = strlen(mixlist[1])},
                {.sval = (unsigned char*)mixlist[2], .slen = strlen(mixlist[2])},
                {.lval = 4294967296},
                {.sval = (unsigned char*)mixlist[3], .slen = strlen(mixlist[3])},
                {.lval = -100}
        };

        lp = lpBatchAppend(lp, ent, 4);
        assert(lpLength(lp) == 4);
        verifyEntry(lpSeek(lp, 0), ent[0].sval, ent[0].slen);
        verifyEntry(lpSeek(lp, 1), ent[1].sval, ent[1].slen);
        verifyEntry(lpSeek(lp, 2), ent[2].sval, ent[2].slen);
        verifyEntry(lpSeek(lp, 3), (unsigned char*)"4294967296", 10);

        /* 以 LP_BEFORE 方式插入。 */
        p = lpSeek(lp, 3);
        lp = lpBatchInsert(lp, p, LP_BEFORE, &ent[4], 2, &p);
        verifyEntry(p, (unsigned char*)"-100", 4);
        assert(lpLength(lp) == 6);
        verifyEntry(lpSeek(lp, 0), ent[0].sval, ent[0].slen);
        verifyEntry(lpSeek(lp, 1), ent[1].sval, ent[1].slen);
        verifyEntry(lpSeek(lp, 2), ent[2].sval, ent[2].slen);
        verifyEntry(lpSeek(lp, 3), ent[4].sval, ent[4].slen);
        verifyEntry(lpSeek(lp, 4), (unsigned char*)"-100", 4);
        verifyEntry(lpSeek(lp, 5), (unsigned char*)"4294967296", 10);

        lp = lpDeleteRange(lp, 1, 2);
        assert(lpLength(lp) == 4);
        verifyEntry(lpSeek(lp, 0), ent[0].sval, ent[0].slen);
        verifyEntry(lpSeek(lp, 1), ent[4].sval, ent[4].slen);
        verifyEntry(lpSeek(lp, 2), (unsigned char*)"-100", 4);
        verifyEntry(lpSeek(lp, 3), (unsigned char*)"4294967296", 10);

        /* 以 LP_AFTER 方式插入。 */
        p = lpSeek(lp, 0);
        lp = lpBatchInsert(lp, p, LP_AFTER, &ent[1], 2, &p);
        verifyEntry(p, ent[2].sval, ent[2].slen);
        assert(lpLength(lp) == 6);
        verifyEntry(lpSeek(lp, 0), ent[0].sval, ent[0].slen);
        verifyEntry(lpSeek(lp, 1), ent[1].sval, ent[1].slen);
        verifyEntry(lpSeek(lp, 2), ent[2].sval, ent[2].slen);
        verifyEntry(lpSeek(lp, 3), ent[4].sval, ent[4].slen);
        verifyEntry(lpSeek(lp, 4), (unsigned char*)"-100", 4);
        verifyEntry(lpSeek(lp, 5), (unsigned char*)"4294967296", 10);

        lp = lpDeleteRange(lp, 2, 4);
        assert(lpLength(lp) == 2);
        p = lpSeek(lp, 1);
        lp = lpBatchInsert(lp, p, LP_AFTER, &ent[2], 1, &p);
        verifyEntry(p, ent[2].sval, ent[2].slen);
        assert(lpLength(lp) == 3);
        verifyEntry(lpSeek(lp, 0), ent[0].sval, ent[0].slen);
        verifyEntry(lpSeek(lp, 1), ent[1].sval, ent[1].slen);
        verifyEntry(lpSeek(lp, 2), ent[2].sval, ent[2].slen);

        lpFree(lp);
    }

    TEST("Batch delete") {
        unsigned char *lp = createList(); /* char *mixlist[] = {"hello", "foo", "quux", "1024"} */
        assert(lpLength(lp) == 4); /* 前置条件。 */
        unsigned char *p0 = lpFirst(lp),
            *p1 = lpNext(lp, p0),
            *p2 = lpNext(lp, p1),
            *p3 = lpNext(lp, p2);
        unsigned char *ps[] = {p0, p1, p3};
        lp = lpBatchDelete(lp, ps, 3);
        assert(lpLength(lp) == 1);
        verifyEntry(lpFirst(lp), (unsigned char*)mixlist[2], strlen(mixlist[2]));
        assert(lpValidateIntegrity(lp, lpBytes(lp), 1, NULL, NULL) == 1);
        lpFree(lp);
    }

    TEST("Delete foo while iterating") {
        lp = createList();
        p = lpFirst(lp);
        while (p) {
            if (lpCompare(p, (unsigned char*)"foo", 3)) {
                lp = lpDelete(lp, p, &p);
            } else {
                p = lpNext(lp, p);
            }
        }
        lpFree(lp);
    }

    TEST("Replace with same size") {
        lp = createList(); /* "hello", "foo", "quux", "1024" */
        unsigned char *orig_lp = lp;
        p = lpSeek(lp, 0);
        lp = lpReplace(lp, &p, (unsigned char*)"zoink", 5);
        p = lpSeek(lp, 3);
        lp = lpReplace(lp, &p, (unsigned char*)"y", 1);
        p = lpSeek(lp, 1);
        lp = lpReplace(lp, &p, (unsigned char*)"65536", 5);
        p = lpSeek(lp, 0);
        assert(!memcmp((char*)p,
                       "\x85zoink\x06"
                       "\xf2\x00\x00\x01\x04" /* 65536 as int24 */
                       "\x84quux\05" "\x81y\x02" "\xff",
                       22));
        assert(lp == orig_lp); /* no reallocations have happened */
        lpFree(lp);
    }

    TEST("Replace with different size") {
        lp = createList(); /* "hello", "foo", "quux", "1024" */
        p = lpSeek(lp, 1);
        lp = lpReplace(lp, &p, (unsigned char*)"squirrel", 8);
        p = lpSeek(lp, 0);
        assert(!strncmp((char*)p,
                        "\x85hello\x06" "\x88squirrel\x09" "\x84quux\x05"
                        "\xc4\x00\x02" "\xff",
                        27));
        lpFree(lp);
    }

    TEST("Regression test for >255 byte strings") {
        char v1[257] = {0}, v2[257] = {0};
        memset(v1,'x',256);
        memset(v2,'y',256);
        lp = lpNew(0);
        lp = lpAppend(lp, (unsigned char*)v1 ,strlen(v1));
        lp = lpAppend(lp, (unsigned char*)v2 ,strlen(v2));

        /* 再次弹出值并与原始内容进行比较。 */
        p = lpFirst(lp);
        vstr = lpGet(p, &vlen, NULL);
        assert(strncmp(v1, (char*)vstr, vlen) == 0);
        p = lpSeek(lp, 1);
        vstr = lpGet(p, &vlen, NULL);
        assert(strncmp(v2, (char*)vstr, vlen) == 0);
        lpFree(lp);
    }

    TEST("Create long list and check indices") {
        lp = lpNew(0);
        char buf[32];
        int i,len;
        for (i = 0; i < 1000; i++) {
            len = snprintf(buf, sizeof(buf), "%d", i);
            lp = lpAppend(lp, (unsigned char*)buf, len);
        }
        for (i = 0; i < 1000; i++) {
            p = lpSeek(lp, i);
            vstr = lpGet(p, &vlen, NULL);
            assert(i == vlen);

            p = lpSeek(lp, -i-1);
            vstr = lpGet(p, &vlen, NULL);
            assert(999-i == vlen);
        }
        lpFree(lp);
    }

    TEST("Compare strings with listpack entries") {
        lp = createList();
        p = lpSeek(lp,0);
        assert(lpCompare(p,(unsigned char*)"hello",5));
        assert(!lpCompare(p,(unsigned char*)"hella",5));

        p = lpSeek(lp,3);
        assert(lpCompare(p,(unsigned char*)"1024",4));
        assert(!lpCompare(p,(unsigned char*)"1025",4));
        lpFree(lp);
    }

    TEST("lpMerge two empty listpacks") {
        unsigned char *lp1 = lpNew(0);
        unsigned char *lp2 = lpNew(0);

        /* Merge two empty listpacks, get empty result back. */
        lp1 = lpMerge(&lp1, &lp2);
        assert(lpLength(lp1) == 0);
        zfree(lp1);
    }

    TEST("lpMerge two listpacks - first larger than second") {
        unsigned char *lp1 = createIntList();
        unsigned char *lp2 = createList();

        size_t lp1_bytes = lpBytes(lp1);
        size_t lp2_bytes = lpBytes(lp2);
        unsigned long lp1_len = lpLength(lp1);
        unsigned long lp2_len = lpLength(lp2);

        unsigned char *lp3 = lpMerge(&lp1, &lp2);
        assert(lp3 == lp1);
        assert(lp2 == NULL);
        assert(lpLength(lp3) == (lp1_len + lp2_len));
        assert(lpBytes(lp3) == (lp1_bytes + lp2_bytes - LP_HDR_SIZE - 1));
        verifyEntry(lpSeek(lp3, 0), (unsigned char*)"4294967296", 10);
        verifyEntry(lpSeek(lp3, 5), (unsigned char*)"much much longer non integer", 28);
        verifyEntry(lpSeek(lp3, 6), (unsigned char*)"hello", 5);
        verifyEntry(lpSeek(lp3, -1), (unsigned char*)"1024", 4);
        zfree(lp3);
    }

    TEST("lpMerge two listpacks - second larger than first") {
        unsigned char *lp1 = createList();
        unsigned char *lp2 = createIntList();

        size_t lp1_bytes = lpBytes(lp1);
        size_t lp2_bytes = lpBytes(lp2);
        unsigned long lp1_len = lpLength(lp1);
        unsigned long lp2_len = lpLength(lp2);

        unsigned char *lp3 = lpMerge(&lp1, &lp2);
        assert(lp3 == lp2);
        assert(lp1 == NULL);
        assert(lpLength(lp3) == (lp1_len + lp2_len));
        assert(lpBytes(lp3) == (lp1_bytes + lp2_bytes - LP_HDR_SIZE - 1));
        verifyEntry(lpSeek(lp3, 0), (unsigned char*)"hello", 5);
        verifyEntry(lpSeek(lp3, 3), (unsigned char*)"1024", 4);
        verifyEntry(lpSeek(lp3, 4), (unsigned char*)"4294967296", 10);
        verifyEntry(lpSeek(lp3, -1), (unsigned char*)"much much longer non integer", 28);
        zfree(lp3);
    }

    TEST("lpNextRandom normal usage") {
        /* 构造一些测试数据。 */
        unsigned char *lp = lpNew(0);
        unsigned char buf[100] = "asdf";
        unsigned int size = 100;
        for (size_t i = 0; i < size; i++) {
            lp = lpAppend(lp, buf, i);
        }
        assert(lpLength(lp) == size);

        /* 对每一种可能的子集大小，挑选一个子集元素进行测试。 */
        for (unsigned int count = 0; count <= size; count++) {
            unsigned int remaining = count;
            unsigned char *p = lpFirst(lp);
            unsigned char *prev = NULL;
            unsigned index = 0;
            while (remaining > 0) {
                assert(p != NULL);
                p = lpNextRandom(lp, p, &index, remaining--, 1);
                assert(p != NULL);
                assert(p != prev);
                prev = p;
                p = lpNext(lp, p);
                index++;
            }
        }
        lpFree(lp);
    }

    TEST("lpNextRandom corner cases") {
        unsigned char *lp = lpNew(0);
        unsigned i = 0;

        /* 从空 listpack 中挑选应返回 NULL。 */
        assert(lpNextRandom(lp, NULL, &i, 2, 1) == NULL);

        /* 添加若干元素并取得它们在 listpack 中的指针。 */
        lp = lpAppend(lp, (unsigned char *)"abc", 3);
        lp = lpAppend(lp, (unsigned char *)"def", 3);
        lp = lpAppend(lp, (unsigned char *)"ghi", 3);
        assert(lpLength(lp) == 3);
        unsigned char *p0 = lpFirst(lp);
        unsigned char *p1 = lpNext(lp, p0);
        unsigned char *p2 = lpNext(lp, p1);
        assert(lpNext(lp, p2) == NULL);

        /* 挑选 0 个元素时应返回 NULL。 */
        i = 0; assert(lpNextRandom(lp, lpFirst(lp), &i, 0, 1) == NULL);

        /* 全部挑选时应返回所有元素。 */
        i = 0; assert(lpNextRandom(lp, p0, &i, 3, 1) == p0 && i == 0);
        i = 1; assert(lpNextRandom(lp, p1, &i, 2, 1) == p1 && i == 1);
        i = 2; assert(lpNextRandom(lp, p2, &i, 1, 1) == p2 && i == 2);

        /* 只剩一个元素时要求挑选多个应返回最后一个。 */
        i = 2; assert(lpNextRandom(lp, p2, &i, 42, 1) == p2 && i == 2);

        /* 挑选所有偶数索引应得到 p0 和 p2。 */
        i = 0; assert(lpNextRandom(lp, p0, &i, 10, 2) == p0 && i == 0);
        i = 1; assert(lpNextRandom(lp, p1, &i, 10, 2) == p2 && i == 2);

        /* 即使索引非法也不应崩溃。 */
        for (int j = 0; j < 100; j++) {
            unsigned char *p;
            switch (j % 4) {
            case 0: p = p0; break;
            case 1: p = p1; break;
            case 2: p = p2; break;
            case 3: p = NULL; break;
            }
            i = j % 7;
            unsigned int remaining = j % 5;
            p = lpNextRandom(lp, p, &i, remaining, 1);
            assert(p == p0 || p == p1 || p == p2 || p == NULL);
        }
        lpFree(lp);
    }

    TEST("Random pair with one element") {
        listpackEntry key, val;
        unsigned char *lp = lpNew(0);
        lp = lpAppend(lp, (unsigned char*)"abc", 3);
        lp = lpAppend(lp, (unsigned char*)"123", 3);
        lpRandomPair(lp, 1, &key, &val, 2);
        assert(memcmp(key.sval, "abc", key.slen) == 0);
        assert(val.lval == 123);
        lpFree(lp);
    }

    TEST("Random pair with many elements") {
        listpackEntry key, val;
        unsigned char *lp = lpNew(0);
        lp = lpAppend(lp, (unsigned char*)"abc", 3);
        lp = lpAppend(lp, (unsigned char*)"123", 3);
        lp = lpAppend(lp, (unsigned char*)"456", 3);
        lp = lpAppend(lp, (unsigned char*)"def", 3);
        lpRandomPair(lp, 2, &key, &val, 2);
        if (key.sval) {
            assert(!memcmp(key.sval, "abc", key.slen));
            assert(key.slen == 3);
            assert(val.lval == 123);
        }
        if (!key.sval) {
            assert(key.lval == 456);
            assert(!memcmp(val.sval, "def", val.slen));
        }
        lpFree(lp);
    }

    TEST("Random pair with tuple_len 3") {
        listpackEntry key, val;
        unsigned char *lp = lpNew(0);
        lp = lpAppend(lp, (unsigned char*)"abc", 3);
        lp = lpAppend(lp, (unsigned char*)"123", 3);
        lp = lpAppend(lp, (unsigned char*)"xxx", 3);
        lp = lpAppend(lp, (unsigned char*)"456", 3);
        lp = lpAppend(lp, (unsigned char*)"def", 3);
        lp = lpAppend(lp, (unsigned char*)"xxx", 3);
        lp = lpAppend(lp, (unsigned char*)"281474976710655", 15);
        lp = lpAppend(lp, (unsigned char*)"789", 3);
        lp = lpAppend(lp, (unsigned char*)"xxx", 3);

        for (int i = 0; i < 5; i++) {
            lpRandomPair(lp, 3, &key, &val, 3);
            if (key.sval) {
                if (!memcmp(key.sval, "abc", key.slen)) {
                    assert(key.slen == 3);
                    assert(val.lval == 123);
                } else {
                    assert(0);
                };
            }
            if (!key.sval) {
                if (key.lval == 456)
                    assert(!memcmp(val.sval, "def", val.slen));
                else if (key.lval == 281474976710655LL)
                    assert(val.lval == 789);
                else
                    assert(0);
            }
        }

        lpFree(lp);
    }

    TEST("Random pairs with one element") {
        int count = 5;
        unsigned char *lp = lpNew(0);
        listpackEntry *keys = zmalloc(sizeof(listpackEntry) * count);
        listpackEntry *vals = zmalloc(sizeof(listpackEntry) * count);

        lp = lpAppend(lp, (unsigned char*)"abc", 3);
        lp = lpAppend(lp, (unsigned char*)"123", 3);
        lpRandomPairs(lp, count, keys, vals, 2);
        assert(memcmp(keys[4].sval, "abc", keys[4].slen) == 0);
        assert(vals[4].lval == 123);
        zfree(keys);
        zfree(vals);
        lpFree(lp);
    }

    TEST("Random pairs with many elements") {
        int count = 5;
        lp = lpNew(0);
        listpackEntry *keys = zmalloc(sizeof(listpackEntry) * count);
        listpackEntry *vals = zmalloc(sizeof(listpackEntry) * count);

        lp = lpAppend(lp, (unsigned char*)"abc", 3);
        lp = lpAppend(lp, (unsigned char*)"123", 3);
        lp = lpAppend(lp, (unsigned char*)"456", 3);
        lp = lpAppend(lp, (unsigned char*)"def", 3);
        lpRandomPairs(lp, count, keys, vals, 2);
        for (int i = 0; i < count; i++) {
            if (keys[i].sval) {
                assert(!memcmp(keys[i].sval, "abc", keys[i].slen));
                assert(keys[i].slen == 3);
                assert(vals[i].lval == 123);
            }
            if (!keys[i].sval) {
                assert(keys[i].lval == 456);
                assert(!memcmp(vals[i].sval, "def", vals[i].slen));
            }
        }
        zfree(keys);
        zfree(vals);
        lpFree(lp);
    }

    TEST("Random pairs with many elements and tuple_len 3") {
        int count = 5;
        lp = lpNew(0);
        listpackEntry *keys = zcalloc(sizeof(listpackEntry) * count);
        listpackEntry *vals = zcalloc(sizeof(listpackEntry) * count);

        lp = lpAppend(lp, (unsigned char*)"abc", 3);
        lp = lpAppend(lp, (unsigned char*)"123", 3);
        lp = lpAppend(lp, (unsigned char*)"xxx", 3);
        lp = lpAppend(lp, (unsigned char*)"456", 3);
        lp = lpAppend(lp, (unsigned char*)"def", 3);
        lp = lpAppend(lp, (unsigned char*)"xxx", 3);
        lp = lpAppend(lp, (unsigned char*)"281474976710655", 15);
        lp = lpAppend(lp, (unsigned char*)"789", 3);
        lp = lpAppend(lp, (unsigned char*)"xxx", 3);

        lpRandomPairs(lp, count, keys, vals, 3);
        for (int i = 0; i < count; i++) {
            if (keys[i].sval) {
                if (!memcmp(keys[i].sval, "abc", keys[i].slen)) {
                    assert(keys[i].slen == 3);
                    assert(vals[i].lval == 123);
                } else {
                    assert(0);
                };
            }
            if (!keys[i].sval) {
                if (keys[i].lval == 456)
                    assert(!memcmp(vals[i].sval, "def", vals[i].slen));
                else if (keys[i].lval == 281474976710655LL)
                    assert(vals[i].lval == 789);
                else
                    assert(0);
            }
        }

        zfree(keys);
        zfree(vals);
        lpFree(lp);
    }

    TEST("Random pairs unique with one element") {
        unsigned picked;
        int count = 5;
        lp = lpNew(0);
        listpackEntry *keys = zmalloc(sizeof(listpackEntry) * count);
        listpackEntry *vals = zmalloc(sizeof(listpackEntry) * count);

        lp = lpAppend(lp, (unsigned char*)"abc", 3);
        lp = lpAppend(lp, (unsigned char*)"123", 3);
        picked = lpRandomPairsUnique(lp, count, keys, vals, 2);
        assert(picked == 1);
        assert(memcmp(keys[0].sval, "abc", keys[0].slen) == 0);
        assert(vals[0].lval == 123);
        zfree(keys);
        zfree(vals);
        lpFree(lp);
    }

    TEST("Random pairs unique with many elements") {
        unsigned picked;
        int count = 5;
        lp = lpNew(0);
        listpackEntry *keys = zmalloc(sizeof(listpackEntry) * count);
        listpackEntry *vals = zmalloc(sizeof(listpackEntry) * count);

        lp = lpAppend(lp, (unsigned char*)"abc", 3);
        lp = lpAppend(lp, (unsigned char*)"123", 3);
        lp = lpAppend(lp, (unsigned char*)"456", 3);
        lp = lpAppend(lp, (unsigned char*)"def", 3);
        picked = lpRandomPairsUnique(lp, count, keys, vals, 2);
        assert(picked == 2);
        for (int i = 0; i < 2; i++) {
            if (keys[i].sval) {
                assert(!memcmp(keys[i].sval, "abc", keys[i].slen));
                assert(keys[i].slen == 3);
                assert(vals[i].lval == 123);
            }
            if (!keys[i].sval) {
                assert(keys[i].lval == 456);
                assert(!memcmp(vals[i].sval, "def", vals[i].slen));
            }
        }
        zfree(keys);
        zfree(vals);
        lpFree(lp);
    }

    TEST("Random pairs unique with many elements and tuple_len 3") {
        unsigned picked;
        int count = 5;
        lp = lpNew(0);
        listpackEntry *keys = zmalloc(sizeof(listpackEntry) * count);
        listpackEntry *vals = zmalloc(sizeof(listpackEntry) * count);

        lp = lpAppend(lp, (unsigned char*)"abc", 3);
        lp = lpAppend(lp, (unsigned char*)"123", 3);
        lp = lpAppend(lp, (unsigned char*)"xxx", 3);
        lp = lpAppend(lp, (unsigned char*)"456", 3);
        lp = lpAppend(lp, (unsigned char*)"def", 3);
        lp = lpAppend(lp, (unsigned char*)"xxx", 3);
        lp = lpAppend(lp, (unsigned char*)"281474976710655", 15);
        lp = lpAppend(lp, (unsigned char*)"789", 3);
        lp = lpAppend(lp, (unsigned char*)"xxx", 3);
        picked = lpRandomPairsUnique(lp, count, keys, vals, 3);
        assert(picked == 3);
        for (int i = 0; i < 3; i++) {
            if (keys[i].sval) {
                if (!memcmp(keys[i].sval, "abc", keys[i].slen)) {
                    assert(keys[i].slen == 3);
                    assert(vals[i].lval == 123);
                } else {
                    assert(0);
                };
            }
            if (!keys[i].sval) {
                if (keys[i].lval == 456)
                    assert(!memcmp(vals[i].sval, "def", vals[i].slen));
                else if (keys[i].lval == 281474976710655LL)
                    assert(vals[i].lval == 789);
                else
                    assert(0);
            }
        }
        zfree(keys);
        zfree(vals);
        lpFree(lp);
    }

    TEST("push various encodings") {
        lp = lpNew(0);

        /* 通过 lpAppend 推入整数编码的元素。 */
        lp = lpAppend(lp, (unsigned char*)"127", 3);
        assert(LP_ENCODING_IS_7BIT_UINT(lpLast(lp)[0]));
        lp = lpAppend(lp, (unsigned char*)"4095", 4);
        assert(LP_ENCODING_IS_13BIT_INT(lpLast(lp)[0]));
        lp = lpAppend(lp, (unsigned char*)"32767", 5);
        assert(LP_ENCODING_IS_16BIT_INT(lpLast(lp)[0]));
        lp = lpAppend(lp, (unsigned char*)"8388607", 7);
        assert(LP_ENCODING_IS_24BIT_INT(lpLast(lp)[0]));
        lp = lpAppend(lp, (unsigned char*)"2147483647", 10);
        assert(LP_ENCODING_IS_32BIT_INT(lpLast(lp)[0]));
        lp = lpAppend(lp, (unsigned char*)"9223372036854775807", 19);
        assert(LP_ENCODING_IS_64BIT_INT(lpLast(lp)[0]));

        /* 通过 lpAppendInteger 推入整数编码的元素。 */
        lp = lpAppendInteger(lp, 127);
        assert(LP_ENCODING_IS_7BIT_UINT(lpLast(lp)[0]));
        verifyEntry(lpLast(lp), (unsigned char*)"127", 3);
        lp = lpAppendInteger(lp, 4095);
        verifyEntry(lpLast(lp), (unsigned char*)"4095", 4);
        assert(LP_ENCODING_IS_13BIT_INT(lpLast(lp)[0]));
        lp = lpAppendInteger(lp, 32767);
        verifyEntry(lpLast(lp), (unsigned char*)"32767", 5);
        assert(LP_ENCODING_IS_16BIT_INT(lpLast(lp)[0]));
        lp = lpAppendInteger(lp, 8388607);
        verifyEntry(lpLast(lp), (unsigned char*)"8388607", 7);
        assert(LP_ENCODING_IS_24BIT_INT(lpLast(lp)[0]));
        lp = lpAppendInteger(lp, 2147483647);
        verifyEntry(lpLast(lp), (unsigned char*)"2147483647", 10);
        assert(LP_ENCODING_IS_32BIT_INT(lpLast(lp)[0]));
        lp = lpAppendInteger(lp, 9223372036854775807);
        verifyEntry(lpLast(lp), (unsigned char*)"9223372036854775807", 19);
        assert(LP_ENCODING_IS_64BIT_INT(lpLast(lp)[0]));

        /* 字符串编码测试。 */
        unsigned char *str = zmalloc(65535);
        memset(str, 0, 65535);
        lp = lpAppend(lp, (unsigned char*)str, 63);
        assert(LP_ENCODING_IS_6BIT_STR(lpLast(lp)[0]));
        lp = lpAppend(lp, (unsigned char*)str, 4095);
        assert(LP_ENCODING_IS_12BIT_STR(lpLast(lp)[0]));
        lp = lpAppend(lp, (unsigned char*)str, 65535);
        assert(LP_ENCODING_IS_32BIT_STR(lpLast(lp)[0]));
        zfree(str);
        lpFree(lp);
    }

    TEST("Test lpFind") {
        lp = createList();
        assert(lpFind(lp, lpFirst(lp), (unsigned char*)"abc", 3, 0) == NULL);
        verifyEntry(lpFind(lp, lpFirst(lp), (unsigned char*)"hello", 5, 0), (unsigned char*)"hello", 5);
        verifyEntry(lpFind(lp, lpFirst(lp), (unsigned char*)"1024", 4, 0), (unsigned char*)"1024", 4);
        lpFree(lp);
    }

    TEST("Test lpFindCb") {
        lp = createList(); /* "hello", "foo", "quux", "1024" */
        assert(lpFindCb(lp, lpFirst(lp), "abc", lpFindCbCmp, 0) == NULL);
        verifyEntry(lpFindCb(lp, NULL, "hello", lpFindCbCmp, 0), (unsigned char*)"hello", 5);
        verifyEntry(lpFindCb(lp, NULL, "1024", lpFindCbCmp, 0), (unsigned char*)"1024", 4);
        verifyEntry(lpFindCb(lp, NULL, "quux", lpFindCbCmp, 0), (unsigned char*)"quux", 4);
        verifyEntry(lpFindCb(lp, NULL, "foo", lpFindCbCmp, 0), (unsigned char*)"foo", 3);
        lpFree(lp);

        lp = lpNew(0);
        assert(lpFindCb(lp, lpFirst(lp), "hello", lpFindCbCmp, 0) == NULL);
        assert(lpFindCb(lp, lpFirst(lp), "1024", lpFindCbCmp, 0) == NULL);
        lpFree(lp);
    }

    TEST("Test lpValidateIntegrity") {
        lp = createList();
        long count = 0;
        assert(lpValidateIntegrity(lp, lpBytes(lp), 1, lpValidation, &count) == 1);
        lpFree(lp);
    }

    TEST("Test number of elements exceeds LP_HDR_NUMELE_UNKNOWN") {
        lp = lpNew(0);
        for (int i = 0; i < LP_HDR_NUMELE_UNKNOWN + 1; i++)
            lp = lpAppend(lp, (unsigned char*)"1", 1);

        assert(lpGetNumElements(lp) == LP_HDR_NUMELE_UNKNOWN);
        assert(lpLength(lp) == LP_HDR_NUMELE_UNKNOWN+1);

        lp = lpDeleteRange(lp, -2, 2);
        assert(lpGetNumElements(lp) == LP_HDR_NUMELE_UNKNOWN);
        assert(lpLength(lp) == LP_HDR_NUMELE_UNKNOWN-1);
        assert(lpGetNumElements(lp) == LP_HDR_NUMELE_UNKNOWN-1); /* update length after lpLength */
        lpFree(lp);
    }

    TEST("Test number of elements exceeds LP_HDR_NUMELE_UNKNOWN with batch insert") {
        listpackEntry ent[2] = {
                {.sval = (unsigned char*)mixlist[0], .slen = strlen(mixlist[0])},
                {.sval = (unsigned char*)mixlist[1], .slen = strlen(mixlist[1])}
        };

        lp = lpNew(0);
        for (int i = 0; i < (LP_HDR_NUMELE_UNKNOWN/2) + 1; i++)
            lp = lpBatchAppend(lp, ent, 2);

        assert(lpGetNumElements(lp) == LP_HDR_NUMELE_UNKNOWN);
        assert(lpLength(lp) == LP_HDR_NUMELE_UNKNOWN+1);

        lp = lpDeleteRange(lp, -2, 2);
        assert(lpGetNumElements(lp) == LP_HDR_NUMELE_UNKNOWN);
        assert(lpLength(lp) == LP_HDR_NUMELE_UNKNOWN-1);
        assert(lpGetNumElements(lp) == LP_HDR_NUMELE_UNKNOWN-1); /* update length after lpLength */
        lpFree(lp);
    }

    TEST("Stress with random payloads of different encoding") {
        unsigned long long start = usec();
        int i,j,len,where;
        unsigned char *p;
        char buf[1024];
        int buflen;
        list *ref;
        listNode *refnode;

        int iteration = accurate ? 20000 : 20;
        for (i = 0; i < iteration; i++) {
            lp = lpNew(0);
            ref = listCreate();
            listSetFreeMethod(ref,(void (*)(void*))sdsfree);
            len = rand() % 256;

            /* 创建 listpack 与参考列表。 */
            for (j = 0; j < len; j++) {
                where = (rand() & 1) ? 0 : 1;
                if (rand() % 2) {
                    buflen = randstring(buf,1,sizeof(buf)-1);
                } else {
                    switch(rand() % 3) {
                    case 0:
                        buflen = snprintf(buf,sizeof(buf),"%lld",(0LL + rand()) >> 20);
                        break;
                    case 1:
                        buflen = snprintf(buf,sizeof(buf),"%lld",(0LL + rand()));
                        break;
                    case 2:
                        buflen = snprintf(buf,sizeof(buf),"%lld",(0LL + rand()) << 20);
                        break;
                    default:
                        assert(NULL);
                    }
                }

                /* 把元素加入 listpack。 */
                if (where == 0) {
                    lp = lpPrepend(lp, (unsigned char*)buf, buflen);
                } else {
                    lp = lpAppend(lp, (unsigned char*)buf, buflen);
                }

                /* 把元素加入参考列表。 */
                if (where == 0) {
                    listAddNodeHead(ref,sdsnewlen(buf, buflen));
                } else if (where == 1) {
                    listAddNodeTail(ref,sdsnewlen(buf, buflen));
                } else {
                    assert(NULL);
                }
            }

            assert(listLength(ref) == lpLength(lp));
            for (j = 0; j < len; j++) {
                /* 用朴素方式获取元素，与 Tcl 测试套件中的压力测试保持一致。 */
                p = lpSeek(lp,j);
                refnode = listIndex(ref,j);

                vstr = lpGet(p, &vlen, intbuf);
                assert(memcmp(vstr,listNodeValue(refnode),vlen) == 0);
            }
            lpFree(lp);
            listRelease(ref);
        }
        printf("Done. usec=%lld\n\n", usec()-start);
    }

    TEST("Stress with variable listpack size") {
        unsigned long long start = usec();
        int maxsize = accurate ? 16384 : 16;
        stress(0,100000,maxsize,256);
        stress(1,100000,maxsize,256);
        printf("Done. usec=%lld\n\n", usec()-start);
    }

    /* 基准测试。 */
    {
        int iteration = accurate ? 100000 : 100;
        lp = lpNew(0);
        TEST("Benchmark lpAppend") {
            unsigned long long start = usec();
            for (int i=0; i<iteration; i++) {
                char buf[4096] = "asdf";
                lp = lpAppend(lp, (unsigned char*)buf, 4);
                lp = lpAppend(lp, (unsigned char*)buf, 40);
                lp = lpAppend(lp, (unsigned char*)buf, 400);
                lp = lpAppend(lp, (unsigned char*)buf, 4000);
                lp = lpAppend(lp, (unsigned char*)"1", 1);
                lp = lpAppend(lp, (unsigned char*)"10", 2);
                lp = lpAppend(lp, (unsigned char*)"100", 3);
                lp = lpAppend(lp, (unsigned char*)"1000", 4);
                lp = lpAppend(lp, (unsigned char*)"10000", 5);
                lp = lpAppend(lp, (unsigned char*)"100000", 6);
            }
            printf("Done. usec=%lld\n", usec()-start);
        }

        TEST("Benchmark lpFind string") {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                unsigned char *fptr = lpFirst(lp);
                fptr = lpFind(lp, fptr, (unsigned char*)"nothing", 7, 1);
            }
            printf("Done. usec=%lld\n", usec()-start);
        }

        TEST("Benchmark lpFind number") {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                unsigned char *fptr = lpFirst(lp);
                fptr = lpFind(lp, fptr, (unsigned char*)"99999", 5, 1);
            }
            printf("Done. usec=%lld\n", usec()-start);
        }

        TEST("Benchmark lpSeek") {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                lpSeek(lp, 99999);
            }
            printf("Done. usec=%lld\n", usec()-start);
        }

        TEST("Benchmark lpValidateIntegrity") {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                lpValidateIntegrity(lp, lpBytes(lp), 1, NULL, NULL);
            }
            printf("Done. usec=%lld\n", usec()-start);
        }

        TEST("Benchmark lpCompare with string") {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                unsigned char *eptr = lpSeek(lp,0);
                while (eptr != NULL) {
                    lpCompare(eptr,(unsigned char*)"nothing",7);
                    eptr = lpNext(lp,eptr);
                }
            }
            printf("Done. usec=%lld\n", usec()-start);
        }

        TEST("Benchmark lpCompare with number") {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                unsigned char *eptr = lpSeek(lp,0);
                while (eptr != NULL) {
                    lpCompare(lp, (unsigned char*)"99999", 5);
                    eptr = lpNext(lp,eptr);
                }
            }
            printf("Done. usec=%lld\n", usec()-start);
        }

        lpFree(lp);
    }

    return 0;
}

#endif
