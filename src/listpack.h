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

#ifndef __LISTPACK_H
#define __LISTPACK_H

#include <stdlib.h>
#include <stdint.h>

/* 用于将整数转换为字符串的缓冲区大小。
 * -2^63 的十进制表示需要 20 位数字，加上字符串结尾的空字符共 21 字节。 */
#define LP_INTBUF_SIZE 21 /* 20 digits of -2^63 + 1 null term = 21. */

/* lpInsert() 中 where 参数的可用取值： */
#define LP_BEFORE 0  /* 在元素之前插入 */
#define LP_AFTER 1   /* 在元素之后插入 */
#define LP_REPLACE 2 /* 替换该元素 */

/* listpack 中的每个条目要么是字符串，要么是整数。 */
typedef struct {
    /* 当作为字符串使用时，通过 sval 和 slen 提供字符串及其长度。 */
    unsigned char *sval;
    uint32_t slen;
    /* 当作为整数使用时，sval 为 NULL，整数值由 lval 保存。 */
    long long lval;
} listpackEntry;

/* 创建一个新的空 listpack，预分配至少 capacity 字节。
 * 成功返回 listpack 指针，失败返回 NULL。 */
unsigned char *lpNew(size_t capacity);
/* 释放指定的 listpack。 */
void lpFree(unsigned char *lp);
/* 将 listpack 的内存收缩到刚好容纳当前数据的大小。 */
unsigned char* lpShrinkToFit(unsigned char *lp);
/* 在 listpack 中位置 p 处插入字符串 s（长度 slen）。
 * where 可取 LP_BEFORE / LP_AFTER / LP_REPLACE。
 * 若 newp 非空，*newp 指向插入/替换后元素的位置。 */
unsigned char *lpInsertString(unsigned char *lp, unsigned char *s, uint32_t slen,
                              unsigned char *p, int where, unsigned char **newp);
/* 在 listpack 中位置 p 处插入整数 lval。
 * where 可取 LP_BEFORE / LP_AFTER / LP_REPLACE。
 * 若 newp 非空，*newp 指向插入/替换后元素的位置。 */
unsigned char *lpInsertInteger(unsigned char *lp, long long lval,
                               unsigned char *p, int where, unsigned char **newp);
/* 将字符串元素 s 插入到 listpack 的头部。 */
unsigned char *lpPrepend(unsigned char *lp, unsigned char *s, uint32_t slen);
/* 将整数元素 lval 插入到 listpack 的头部。 */
unsigned char *lpPrependInteger(unsigned char *lp, long long lval);
/* 将字符串元素 ele 追加到 listpack 的尾部。 */
unsigned char *lpAppend(unsigned char *lp, unsigned char *s, uint32_t slen);
/* 将整数元素 lval 追加到 listpack 的尾部。 */
unsigned char *lpAppendInteger(unsigned char *lp, long long lval);
/* 使用字符串 s 替换 listpack 中 *p 所指的元素，并更新 *p。 */
unsigned char *lpReplace(unsigned char *lp, unsigned char **p, unsigned char *s, uint32_t slen);
/* 使用 64 位整数 lval 替换 listpack 中 *p 所指的元素，并更新 *p。 */
unsigned char *lpReplaceInteger(unsigned char *lp, unsigned char **p, long long lval);
/* 删除 listpack 中 p 所指的元素。
 * 若 newp 非空，*newp 指向被删元素右侧的下一个元素，
 * 若被删元素已是最后一个则 *newp 为 NULL。 */
unsigned char *lpDelete(unsigned char *lp, unsigned char *p, unsigned char **newp);
/* 从 *p 指定的元素开始，连续删除 num 个元素，并更新 *p。 */
unsigned char *lpDeleteRangeWithEntry(unsigned char *lp, unsigned char **p, unsigned long num);
/* 从 listpack 的 index 位置起，删除 num 个元素。 */
unsigned char *lpDeleteRange(unsigned char *lp, long index, unsigned long num);
/* 批量追加 entries 数组中的 len 个元素到 listpack 尾部。
 * 与多次 lpAppend 相比只会触发一次 realloc，效率更高。 */
unsigned char *lpBatchAppend(unsigned char *lp, listpackEntry *entries, unsigned long len);
/* 在 listpack 中 p 处批量插入 entries 数组中的 len 个元素，
 * where 可取 LP_BEFORE 或 LP_AFTER。若 newp 非空则指向首插入元素。 */
unsigned char *lpBatchInsert(unsigned char *lp, unsigned char *p, int where,
                             listpackEntry *entries, unsigned int len, unsigned char **newp);
/* 批量删除 ps 数组中指定的 count 个元素（按 listpack 中的顺序传入）。 */
unsigned char *lpBatchDelete(unsigned char *lp, unsigned char **ps, unsigned long count);
/* 将 second 合并到 first 之后（或将 first 合并到 second 之前）。
 * 选较大的那个 listpack 进行 realloc，并释放另一个。 */
unsigned char *lpMerge(unsigned char **first, unsigned char **second);
/* 复制一份 listpack。 */
unsigned char *lpDup(unsigned char *lp);
/* 返回 listpack 中的元素数量。 */
unsigned long lpLength(unsigned char *lp);
/* 获取 p 所指元素的当前值：
 * - 整数时通过 *count 返回值并返回 NULL；
 * - 字符串时返回指向内部存储的指针，*count 为字符串长度。
 * 若 intbuf 非空，整数会被转成字符串写入 intbuf 并返回该缓冲区。 */
unsigned char *lpGet(unsigned char *p, int64_t *count, unsigned char *intbuf);
/* 获取 p 所指元素的值：
 * - 字符串时返回指针并通过 *slen 返回长度；
 * - 整数时返回 NULL 并通过 *lval 返回值。 */
unsigned char *lpGetValue(unsigned char *p, unsigned int *slen, long long *lval);
/* 当 p 指向整数元素时，将值存入 *lval 并返回 1；否则返回 0。 */
int lpGetIntegerValue(unsigned char *p, long long *lval);
/* 从 p 开始查找等于字符串 s（长度 slen）的元素，命中返回其指针，否则返回 NULL。
 * skip 表示每比较一次后跳过的元素数（用于重复元素的查找）。 */
unsigned char *lpFind(unsigned char *lp, unsigned char *p, unsigned char *s, uint32_t slen, unsigned int skip);
/* lpFindCb 的比较回调函数类型：相等返回 0，否则非 0。 */
typedef int (*lpCmp)(const unsigned char *lp, unsigned char *p, void *user, unsigned char *s, long long slen);
/* 通过自定义比较回调 cmp 在 listpack 中查找元素。
 * skip 表示每次比较之间跳过的元素个数。 */
unsigned char *lpFindCb(unsigned char *lp, unsigned char *p, void *user, lpCmp cmp, unsigned int skip);
/* 返回 listpack 第一个元素的指针，若 listpack 为空则返回 NULL。 */
unsigned char *lpFirst(unsigned char *lp);
/* 返回 listpack 最后一个元素的指针，若 listpack 为空则返回 NULL。 */
unsigned char *lpLast(unsigned char *lp);
/* 返回 p 所指元素的下一个元素指针；若 p 已是最后一个则返回 NULL。 */
unsigned char *lpNext(unsigned char *lp, unsigned char *p);
/* 返回 p 所指元素的上一个元素指针；若 p 已是第一个则返回 NULL。 */
unsigned char *lpPrev(unsigned char *lp, unsigned char *p);
/* 返回整个 listpack 占用的字节数。 */
size_t lpBytes(unsigned char *lp);
/* 返回将整数 lval 编码进 listpack 后所占用的字节数。 */
size_t lpEntrySizeInteger(long long lval);
/* 估算由 rep 个相同整数 lval 构成的 listpack 的总字节数。 */
size_t lpEstimateBytesRepeatedInteger(long long lval, unsigned long rep);
/* 按索引定位元素：正数从头部开始，负数从尾部开始（-1 表示最后一个）。
 * 越界时返回 NULL。 */
unsigned char *lpSeek(unsigned char *lp, long index);
/* lpValidateIntegrity 在深度校验每个元素时调用的回调类型。
 * 返回 1 表示元素有效，返回 0 表示无效。 */
typedef int (*listpackValidateEntryCB)(unsigned char *p, unsigned int head_count, void *userdata);
/* 校验 listpack 的结构完整性。
 * deep=0 仅校验头部；deep=1 会逐个元素扫描，并可调用 entry_cb 校验每个元素。 */
int lpValidateIntegrity(unsigned char *lp, size_t size, int deep,
                        listpackValidateEntryCB entry_cb, void *cb_userdata);
/* 返回 listpack 第一个元素的指针（不做完整性校验）。 */
unsigned char *lpValidateFirst(unsigned char *lp);
/* 校验 *pp 所指元素的有效性，并将 *pp 推进到下一个元素。返回 1 表示有效，0 表示无效。 */
int lpValidateNext(unsigned char *lp, unsigned char **pp, size_t lpbytes);
/* 比较 p 所指元素与字符串 s（长度 slen）是否相等，相等返回 1，否则返回 0。 */
unsigned int lpCompare(unsigned char *p, unsigned char *s, uint32_t slen);
/* 随机选择一对 key/value。
 * total_count 是 listpack 长度的一半（已预算好）。
 * tuple_len 表示一个逻辑项由几个连续 entry 组成（key-value 时为 2）。 */
void lpRandomPair(unsigned char *lp, unsigned long total_count,
                  listpackEntry *key, listpackEntry *val, int tuple_len);
/* 随机选择 count 对 key/value，结果按随机顺序写入 keys/vals。
 * vals 可为 NULL。tuple_len 同 lpRandomPair。 */
void lpRandomPairs(unsigned char *lp, unsigned int count,
                   listpackEntry *keys, listpackEntry *vals, int tuple_len);
/* 随机选择 count 对唯一的（不重复的）key/value，返回实际挑选的数量。
 * 若 listpack 中数据不足，返回值小于 count。 */
unsigned int lpRandomPairsUnique(unsigned char *lp, unsigned int count,
                                 listpackEntry *keys, listpackEntry *vals, int tuple_len);
/* 随机选择 count 个元素，结果按随机顺序写入 entries（可能有重复）。 */
void lpRandomEntries(unsigned char *lp, unsigned int count, listpackEntry *entries);
/* 配合唯一随机采样使用：从 p 起向前迭代，直到按概率挑中一个元素。
 * *index 表示 p 对应的零基索引，并在返回时更新为挑中元素的索引。 */
unsigned char *lpNextRandom(unsigned char *lp, unsigned char *p, unsigned int *index,
                            unsigned int remaining, int tuple_len);
/* 判断给当前 listpack 追加 add 字节是否会超过安全上限 (1<<30)。 */
int lpSafeToAdd(unsigned char* lp, size_t add);
/* 打印 listpack 的调试信息（用于 DEBUG 命令）。 */
void lpRepr(unsigned char *lp);

#ifdef REDIS_TEST
int listpackTest(int argc, char *argv[], int flags);
#endif

#endif
