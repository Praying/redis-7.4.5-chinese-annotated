/*
 * Copyright (C) 2013 Mark Adler
 * Originally by: crc64.c Version 1.4  16 Dec 2013  Mark Adler
 * Modifications by Matt Stancliff <matt@genges.com>:
 *   - removed CRC64-specific behavior
 *   - added generation of lookup tables by parameters
 *   - removed inversion of CRC input/result
 *   - removed automatic initialization in favor of explicit initialization

  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the author be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution.

  Mark Adler
  madler@alumni.caltech.edu
 */

/*
 * crcspeed.c - CRC 加速计算模块
 *
 * 本模块实现了基于查找表的 CRC 高速计算算法
 * （slice-by-8 技术）。支持 64 位和 16 位 CRC，
 * 并根据系统字节序自动选择小端或大端实现。
 * 原始作者：Mark Adler，修改者：Matt Stancliff。
 */

#include "crcspeed.h"

/* 填充 CRC 常量查找表（小端序，64 位 CRC）。 */
void crcspeed64little_init(crcfn64 crcfn, uint64_t table[8][256]) {
    uint64_t crc;

    /* 为所有单字节序列生成 CRC 值 */
    for (int n = 0; n < 256; n++) {
        unsigned char v = n;
        table[0][n] = crcfn(0, &v, 1);
    }

    /* 生成嵌套 CRC 表，用于后续的 slice-by-8 查找 */
    for (int n = 0; n < 256; n++) {
        crc = table[0][n];
        for (int k = 1; k < 8; k++) {
            crc = table[0][crc & 0xff] ^ (crc >> 8);
            table[k][n] = crc;
        }
    }
}

/* 填充 CRC 常量查找表（小端序，16 位 CRC）。 */
void crcspeed16little_init(crcfn16 crcfn, uint16_t table[8][256]) {
    uint16_t crc;

    /* 为所有单字节序列生成 CRC 值 */
    for (int n = 0; n < 256; n++) {
        table[0][n] = crcfn(0, &n, 1);
    }

    /* 生成嵌套 CRC 表，用于后续的 slice-by-8 查找 */
    for (int n = 0; n < 256; n++) {
        crc = table[0][n];
        for (int k = 1; k < 8; k++) {
            crc = table[0][(crc >> 8) & 0xff] ^ (crc << 8);
            table[k][n] = crc;
        }
    }
}

/* 反转 64 位字中的字节顺序。 */
static inline uint64_t rev8(uint64_t a) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap64(a);
#else
    uint64_t m;

    m = UINT64_C(0xff00ff00ff00ff);
    a = ((a >> 8) & m) | (a & m) << 8;
    m = UINT64_C(0xffff0000ffff);
    a = ((a >> 16) & m) | (a & m) << 16;
    return a >> 32 | a << 32;
#endif
}

/* 此函数在大端架构上调用一次，用于初始化
 * CRC 查找表。 */
void crcspeed64big_init(crcfn64 fn, uint64_t big_table[8][256]) {
    /* 先创建小端序表，然后反转所有条目。 */
    crcspeed64little_init(fn, big_table);
    for (int k = 0; k < 8; k++) {
        for (int n = 0; n < 256; n++) {
            big_table[k][n] = rev8(big_table[k][n]);
        }
    }
}

/* 为大端架构初始化 16 位 CRC 查找表。 */
void crcspeed16big_init(crcfn16 fn, uint16_t big_table[8][256]) {
    /* 先创建小端序表，然后反转所有条目。 */
    crcspeed16little_init(fn, big_table);
    for (int k = 0; k < 8; k++) {
        for (int n = 0; n < 256; n++) {
            big_table[k][n] = rev8(big_table[k][n]);
        }
    }
}

/* 在小端架构上一次处理多个字节来计算非反转的 CRC。
 * 如需反转 CRC，应在调用前后分别进行反转。
 * 64 位 CRC = 一次处理 8 个字节。 */
uint64_t crcspeed64little(uint64_t little_table[8][256], uint64_t crc,
                          void *buf, size_t len) {
    unsigned char *next = buf;

    /* 逐字节处理，直到指针达到 8 字节对齐 */
    while (len && ((uintptr_t)next & 7) != 0) {
        crc = little_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
        len--;
    }

    /* 快速中间处理，每次循环处理 8 字节（已对齐！） */
    while (len >= 8) {
        crc ^= *(uint64_t *)next;
        crc = little_table[7][crc & 0xff] ^
              little_table[6][(crc >> 8) & 0xff] ^
              little_table[5][(crc >> 16) & 0xff] ^
              little_table[4][(crc >> 24) & 0xff] ^
              little_table[3][(crc >> 32) & 0xff] ^
              little_table[2][(crc >> 40) & 0xff] ^
              little_table[1][(crc >> 48) & 0xff] ^
              little_table[0][crc >> 56];
        next += 8;
        len -= 8;
    }

    /* 处理剩余字节（不超过 8 个） */
    while (len) {
        crc = little_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
        len--;
    }

    return crc;
}

/* 在小端架构上计算 16 位 CRC，每次处理 8 字节。 */
uint16_t crcspeed16little(uint16_t little_table[8][256], uint16_t crc,
                          void *buf, size_t len) {
    unsigned char *next = buf;

    /* 逐字节处理，直到指针达到 8 字节对齐 */
    while (len && ((uintptr_t)next & 7) != 0) {
        crc = little_table[0][((crc >> 8) ^ *next++) & 0xff] ^ (crc << 8);
        len--;
    }

    /* 快速中间处理，每次循环处理 8 字节（已对齐！） */
    while (len >= 8) {
        uint64_t n = *(uint64_t *)next;
        crc = little_table[7][(n & 0xff) ^ ((crc >> 8) & 0xff)] ^
              little_table[6][((n >> 8) & 0xff) ^ (crc & 0xff)] ^
              little_table[5][(n >> 16) & 0xff] ^
              little_table[4][(n >> 24) & 0xff] ^
              little_table[3][(n >> 32) & 0xff] ^
              little_table[2][(n >> 40) & 0xff] ^
              little_table[1][(n >> 48) & 0xff] ^
              little_table[0][n >> 56];
        next += 8;
        len -= 8;
    }

    /* 处理剩余字节（不超过 8 个） */
    while (len) {
        crc = little_table[0][((crc >> 8) ^ *next++) & 0xff] ^ (crc << 8);
        len--;
    }

    return crc;
}

/* 在大端架构上一次处理 8 字节来计算非反转的 CRC。
 */
uint64_t crcspeed64big(uint64_t big_table[8][256], uint64_t crc, void *buf,
                       size_t len) {
    unsigned char *next = buf;

    crc = rev8(crc);
    while (len && ((uintptr_t)next & 7) != 0) {
        crc = big_table[0][(crc >> 56) ^ *next++] ^ (crc << 8);
        len--;
    }

    while (len >= 8) {
        crc ^= *(uint64_t *)next;
        crc = big_table[0][crc & 0xff] ^
              big_table[1][(crc >> 8) & 0xff] ^
              big_table[2][(crc >> 16) & 0xff] ^
              big_table[3][(crc >> 24) & 0xff] ^
              big_table[4][(crc >> 32) & 0xff] ^
              big_table[5][(crc >> 40) & 0xff] ^
              big_table[6][(crc >> 48) & 0xff] ^
              big_table[7][crc >> 56];
        next += 8;
        len -= 8;
    }

    while (len) {
        crc = big_table[0][(crc >> 56) ^ *next++] ^ (crc << 8);
        len--;
    }

    return rev8(crc);
}

/* 警告：在大端架构上完全未经测试，可能存在问题。 */
uint16_t crcspeed16big(uint16_t big_table[8][256], uint16_t crc_in, void *buf,
                       size_t len) {
    unsigned char *next = buf;
    uint64_t crc = crc_in;

    crc = rev8(crc);
    while (len && ((uintptr_t)next & 7) != 0) {
        crc = big_table[0][((crc >> (56 - 8)) ^ *next++) & 0xff] ^ (crc >> 8);
        len--;
    }

    while (len >= 8) {
        uint64_t n = *(uint64_t *)next;
        crc = big_table[0][(n & 0xff) ^ ((crc >> (56 - 8)) & 0xff)] ^
              big_table[1][((n >> 8) & 0xff) ^ (crc & 0xff)] ^
              big_table[2][(n >> 16) & 0xff] ^
              big_table[3][(n >> 24) & 0xff] ^
              big_table[4][(n >> 32) & 0xff] ^
              big_table[5][(n >> 40) & 0xff] ^
              big_table[6][(n >> 48) & 0xff] ^
              big_table[7][n >> 56];
        next += 8;
        len -= 8;
    }

    while (len) {
        crc = big_table[0][((crc >> (56 - 8)) ^ *next++) & 0xff] ^ (crc >> 8);
        len--;
    }

    return rev8(crc);
}

/* 返回 buf[0..len-1] 的 CRC 值，使用传入的查找表
 * 每次处理 8 字节。根据系统字节序自动选择
 * 小端或大端处理例程。 */
uint64_t crcspeed64native(uint64_t table[8][256], uint64_t crc, void *buf,
                          size_t len) {
    uint64_t n = 1;

    return *(char *)&n ? crcspeed64little(table, crc, buf, len)
                       : crcspeed64big(table, crc, buf, len);
}

uint16_t crcspeed16native(uint16_t table[8][256], uint16_t crc, void *buf,
                          size_t len) {
    uint64_t n = 1;

    return *(char *)&n ? crcspeed16little(table, crc, buf, len)
                       : crcspeed16big(table, crc, buf, len);
}

/* 根据系统架构初始化 CRC 查找表。 */
void crcspeed64native_init(crcfn64 fn, uint64_t table[8][256]) {
    uint64_t n = 1;

    *(char *)&n ? crcspeed64little_init(fn, table)
                : crcspeed64big_init(fn, table);
}

void crcspeed16native_init(crcfn16 fn, uint16_t table[8][256]) {
    uint64_t n = 1;

    *(char *)&n ? crcspeed16little_init(fn, table)
                : crcspeed16big_init(fn, table);
}
