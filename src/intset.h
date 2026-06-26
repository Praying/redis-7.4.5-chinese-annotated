/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-current, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __INTSET_H
#define __INTSET_H
#include <stdint.h>

/* intset 整数集合结构体
 *
 * intset 是 Redis 中用于以紧凑方式保存一组已排序（升序）整数的数据结构。
 * 其特点：
 *  1. 元素按从小到大顺序排列，便于二分查找；
 *  2. 根据保存整数的范围，动态选择最合适的编码（int16/int32/int64），
 *     以尽可能节省内存；
 *  3. 一旦需要放入更大范围的整数，会整体升级到更宽的编码。
 */
typedef struct intset {
    uint32_t encoding;  /* 编码方式：INTSET_ENC_INT16/INT32/INT64 之一 */
    uint32_t length;    /* 当前已保存的整数个数 */
    int8_t contents[];  /* 灵活数组成员，按 encoding 指定的字节宽度
                         * 保存 length 个整数，内存连续布局 */
} intset;

/* 创建一个空的 intset，默认采用 INTSET_ENC_INT16 编码。
 * 返回值：指向新创建的 intset 指针。 */
intset *intsetNew(void);

/* 向 intset 中插入一个整数。
 * 参数：
 *   is     - 目标 intset；
 *   value  - 待插入的整数；
 *   success- 输出参数，非空时：1 表示插入成功，0 表示该值已存在。
 * 返回值：插入后的 intset 指针（可能因升级而重新分配）。 */
intset *intsetAdd(intset *is, int64_t value, uint8_t *success);

/* 从 intset 中删除指定的整数。
 * 参数：
 *   is     - 目标 intset；
 *   value  - 待删除的整数；
 *   success- 输出参数，非空时：1 表示删除成功，0 表示该值不存在。
 * 返回值：删除后的 intset 指针（可能因缩容而重新分配）。 */
intset *intsetRemove(intset *is, int64_t value, int *success);

/* 判断 intset 中是否包含指定整数。
 * 返回值：1 表示存在，0 表示不存在。 */
uint8_t intsetFind(intset *is, int64_t value);

/* 随机返回 intset 中的一个元素。调用方需保证 intset 非空。 */
int64_t intsetRandom(intset *is);

/* 返回 intset 中最大的元素（因有序，即末位元素）。 */
int64_t intsetMax(intset *is);

/* 返回 intset 中最小的元素（因有序，即首位元素）。 */
int64_t intsetMin(intset *is);

/* 取出 pos 位置上的元素写入 *value。
 * 返回值：1 表示 pos 合法，0 表示越界。 */
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value);

/* 返回 intset 中的元素个数。 */
uint32_t intsetLen(const intset *is);

/* 返回 intset 占用的总字节数（含 header 与 contents）。 */
size_t intsetBlobLen(intset *is);

/* 校验 intset 的数据完整性（用于 RDB 加载等场景）。
 * 参数：
 *   p   - 指向 intset 起始地址的字节流；
 *   size- 字节流总长度；
 *   deep- 0 时只校验 header；1 时还会校验元素无重复且升序。
 * 返回值：1 表示通过校验，0 表示不通过。 */
int intsetValidateIntegrity(const unsigned char *is, size_t size, int deep);

#ifdef REDIS_TEST
/* intset 模块的单元测试入口，仅在定义 REDIS_TEST 时编译。 */
int intsetTest(int argc, char *argv[], int flags);
#endif

#endif // __INTSET_H
