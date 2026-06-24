/* 哈希表（Hash Table）实现。
 *
 * 本文件实现了内存中的哈希表，支持插入/删除/替换/查找/获取随机元素等操作。
 * 哈希表在需要时会自动调整大小，使用2的幂次方大小的表，冲突通过链地址法处理。
 *
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#ifndef __DICT_H
#define __DICT_H

#include "mt19937-64.h"
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

#define DICT_OK 0
#define DICT_ERR 1

/* 哈希表参数 */
#define HASHTABLE_MIN_FILL        8      /* 哈希表最小填充率 12.5%(100/8)，当元素数量低于此比例时触发收缩 */

typedef struct dictEntry dictEntry; /* opaque */
typedef struct dict dict;

/* dictType 结构体定义了 dict 的类型特定行为，通过回调函数实现多态。
 * 每种数据类型（如 redisDb、expires 等）都有自己的 dictType 实例。 */
typedef struct dictType {
    /* 回调函数 */
    uint64_t (*hashFunction)(const void *key);          /* 哈希函数，计算键的哈希值 */
    void *(*keyDup)(dict *d, const void *key);          /* 键的复制函数（可选） */
    void *(*valDup)(dict *d, const void *obj);          /* 值的复制函数（可选） */
    int (*keyCompare)(dict *d, const void *key1, const void *key2); /* 键的比较函数 */
    void (*keyDestructor)(dict *d, void *key);          /* 键的析构/释放函数 */
    void (*valDestructor)(dict *d, void *obj);          /* 值的析构/释放函数 */
    int (*resizeAllowed)(size_t moreMem, double usedRatio); /* 是否允许调整大小（用于内存限制） */
    /* 在 dict 初始化/rehash 开始时调用（旧表和新表已经创建） */
    void (*rehashingStarted)(dict *d);
    /* 在 dict 初始化/rehash 所有条目从旧表迁移到新表完成时调用。
     * 两个哈希表仍然存在，在此回调之后才被清理。 */
    void (*rehashingCompleted)(dict *d);
    /* 当字典的 bucket 数量发生变化时调用。
     * `delta` 参数为正表示增加，为负表示减少。 */
    void (*bucketChanged)(dict *d, long long delta);
    /* 允许 dict 携带额外的调用者自定义元数据。
     * 分配 dict 时，额外内存被初始化为 0。 */
    size_t (*dictMetadataBytes)(dict *d);

    /* 用户数据指针，可被回调函数使用 */
    void *userdata;

    /* 标志位 */
    /* 'no_value' 标志：如果设置，表示不使用值（即 dict 作为集合使用）。
     * 设置此标志后，无法访问 dictEntry 的值，也无法使用 dictSetKey()。
     * 条目的元数据也不能使用。 */
    unsigned int no_value:1;
    /* 如果 no_value = 1 且所有键都是奇数（LSB=1），设置 keys_are_odd = 1
     * 可以启用额外优化：无需分配 dictEntry 即可存储键。 */
    unsigned int keys_are_odd:1;
    /* TODO: 添加 'keys_are_even' 标志，在该标志设置时使用类似的优化。 */
    /* 有时我们需要以某种方式存储键，但以另一种方式查找，而无需任何转换。
     * 例如，键可能存储为一个结构体（同时表示其他内容），但查找仅通过
     * 指向以 null 结尾的字符串的指针进行。通过提供额外的哈希/比较函数，
     * dict 支持这种用法。此时我们有：
     * - hashFunction()：期望以 null 结尾的 C 字符串
     * - storedHashFunction()：期望结构体
     * 类似地，两个比较函数的工作方式也不同：
     * - keyCompare()：第一个参数视为 C 字符串指针，另一个视为结构体
     * - storedKeyCompare()：检查两个结构体形式的键指针是否相同
     *
     * 要指示使用 stored-key 类型的键，在调用任何接受键参数的 dict 函数之前
     * 调用 dictUseStoredKeyApi(1)，完成后再次调用 dictUseStoredKeyApi(0)。
     *
     * 如果不需要支持此功能，将两个函数都设置为 NULL。 */
    uint64_t (*storedHashFunction)(const void *key);    /* 存储键的哈希函数 */
    int (*storedKeyCompare)(dict *d, const void *key1, const void *key2); /* 存储键的比较函数 */

    /* 可选回调：当 dict 被销毁时调用 */
    void (*onDictRelease)(dict *d);
} dictType;

/* 根据指数计算哈希表大小：size = 1 << exp，exp == -1 时大小为 0 */
#define DICTHT_SIZE(exp) ((exp) == -1 ? 0 : (unsigned long)1<<(exp))
/* 计算哈希表大小的掩码，用于取模运算：mask = size - 1 */
#define DICTHT_SIZE_MASK(exp) ((exp) == -1 ? 0 : (DICTHT_SIZE(exp))-1)

/* dict 主结构体：Redis 的核心哈希表实现。
 * 使用两个哈希表（ht_table[0] 和 ht_table[1]）实现渐进式 rehash。
 * rehash 期间，ht_table[0] 是旧表，ht_table[1] 是新表。 */
struct dict {
    dictType *type;             /* 类型特定的回调函数 */

    dictEntry **ht_table[2];    /* 两个哈希表（桶数组），rehash 期间同时使用 */
    unsigned long ht_used[2];   /* 每个哈希表中已使用的桶数量 */

    long rehashidx;             /* rehash 进度索引，-1 表示没有进行 rehash */

    /* 将较小的变量放在末尾以获得最优（最小）的结构体填充 */
    unsigned pauserehash : 15;  /* 如果 > 0，rehash 被暂停（有迭代器在使用） */

    unsigned useStoredKeyApi : 1; /* 是否使用 storedKey API，参见 storedHashFunction 的注释 */
    signed char ht_size_exp[2]; /* 哈希表大小的指数（size = 1 << exp） */
    int16_t pauseAutoResize;    /* 如果 > 0，禁止自动调整大小（< 0 表示编码错误） */
    void *metadata[];           /* 柔性数组：可选的调用者自定义元数据 */
};

/* dict 迭代器结构体。
 * 如果 safe 设置为 1，则是安全迭代器，可以在迭代过程中调用 dictAdd、
 * dictFind 等 dict 操作函数。否则是非安全迭代器，迭代期间只能调用 dictNext()。 */
typedef struct dictIterator {
    dict *d;                    /* 被迭代的 dict 指针 */
    long index;                 /* 当前桶索引 */
    int table, safe;            /* table: 当前正在迭代的表（0或1）; safe: 是否为安全迭代器 */
    dictEntry *entry, *nextEntry; /* 当前条目和下一个条目（防止当前条目被删除） */
    /* 非安全迭代器的指纹，用于检测误操作（在迭代期间修改 dict） */
    unsigned long long fingerprint;
} dictIterator;

/* dict 统计信息结构体，用于调试和性能分析 */
typedef struct dictStats {
    int htidx;                  /* 哈希表索引（0 或 1） */
    unsigned long buckets;      /* 非空桶的数量 */
    unsigned long maxChainLen;  /* 最长链长度 */
    unsigned long totalChainLen; /* 总链长度（所有元素数） */
    unsigned long htSize;       /* 哈希表总大小 */
    unsigned long htUsed;       /* 已使用的桶数量 */
    unsigned long *clvector;    /* 链长度分布向量，clvector[i] 表示链长度为 i 的桶数量 */
} dictStats;

/* dict 扫描回调函数类型：遍历 dict 时对每个条目调用 */
typedef void (dictScanFunction)(void *privdata, const dictEntry *de);
/* 内存碎片整理的重分配函数类型 */
typedef void *(dictDefragAllocFunction)(void *ptr);
/* 碎片整理函数集合，用于主动碎片整理（active defrag） */
typedef struct {
    dictDefragAllocFunction *defragAlloc; /* 重分配 dictEntry 等结构体 */
    dictDefragAllocFunction *defragKey;   /* 重分配键（可选） */
    dictDefragAllocFunction *defragVal;   /* 重分配值（可选） */
} dictDefragFunctions;

/* 每个哈希表的初始大小 */
#define DICT_HT_INITIAL_EXP      2
#define DICT_HT_INITIAL_SIZE     (1<<(DICT_HT_INITIAL_EXP))

/* ------------------------------- 宏定义 ------------------------------------*/

/* 释放条目的值（调用 valDestructor 回调） */
#define dictFreeVal(d, entry) do {                     \
    if ((d)->type->valDestructor)                      \
        (d)->type->valDestructor((d), dictGetVal(entry)); \
   } while(0)

/* 释放条目的键（调用 keyDestructor 回调） */
#define dictFreeKey(d, entry) \
    if ((d)->type->keyDestructor) \
        (d)->type->keyDestructor((d), dictGetKey(entry))

/* 比较两个键：如果有 keyCompare 回调则使用回调，否则直接比较指针 */
#define dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? \
        (d)->type->keyCompare((d), key1, key2) : \
        (key1) == (key2))

/* 获取 dict 的元数据指针 */
#define dictMetadata(d) (&(d)->metadata)
/* 获取 dict 元数据的大小 */
#define dictMetadataSize(d) ((d)->type->dictMetadataBytes \
                             ? (d)->type->dictMetadataBytes(d) : 0)

/* 获取 dict 的总桶数（两个表的桶数之和） */
#define dictBuckets(d) (DICTHT_SIZE((d)->ht_size_exp[0])+DICTHT_SIZE((d)->ht_size_exp[1]))
/* 获取 dict 中的元素总数 */
#define dictSize(d) ((d)->ht_used[0]+(d)->ht_used[1])
/* 判断 dict 是否为空 */
#define dictIsEmpty(d) ((d)->ht_used[0] == 0 && (d)->ht_used[1] == 0)
/* 判断 dict 是否正在进行 rehash */
#define dictIsRehashing(d) ((d)->rehashidx != -1)
/* 暂停 rehash（引用计数 +1，有迭代器在使用时调用） */
#define dictPauseRehashing(d) ((d)->pauserehash++)
/* 恢复 rehash（引用计数 -1） */
#define dictResumeRehashing(d) ((d)->pauserehash--)
/* 判断 rehash 是否被暂停 */
#define dictIsRehashingPaused(d) ((d)->pauserehash > 0)
/* 暂停自动调整大小（引用计数 +1） */
#define dictPauseAutoResize(d) ((d)->pauseAutoResize++)
/* 恢复自动调整大小（引用计数 -1） */
#define dictResumeAutoResize(d) ((d)->pauseAutoResize--)
/* 设置是否使用 storedKey API */
#define dictUseStoredKeyApi(d, flag) ((d)->useStoredKeyApi = (flag))

/* 如果 unsigned long 能存储 64 位数字，使用 64 位伪随机数生成器 */
#if ULONG_MAX >= 0xffffffffffffffff
#define randomULong() ((unsigned long) genrand64_int64())
#else
#define randomULong() random()
#endif

/* dict 调整大小的策略枚举 */
typedef enum {
    DICT_RESIZE_ENABLE,     /* 允许调整大小 */
    DICT_RESIZE_AVOID,      /* 尽量避免调整大小（fork 子进程期间使用），但在紧急情况下仍允许 */
    DICT_RESIZE_FORBID,     /* 完全禁止调整大小 */
} dictResizeEnable;

/* =============================== API 函数声明 =============================== */

/* 创建和销毁 */
dict *dictCreate(dictType *type);               /* 创建一个新的 dict */
void dictTypeAddMeta(dict **d, dictType *typeWithMeta); /* 为 dict 添加元数据支持 */

/* 扩展和收缩 */
int dictExpand(dict *d, unsigned long size);    /* 扩展哈希表到指定大小 */
int dictTryExpand(dict *d, unsigned long size); /* 尝试扩展，失败时返回 DICT_ERR */
int dictShrink(dict *d, unsigned long size);    /* 收缩哈希表到指定大小 */

/* 添加和查找 */
int dictAdd(dict *d, void *key, void *val);     /* 添加键值对，键已存在则失败 */
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing); /* 低级添加，返回条目供调用者设置值 */
void *dictFindPositionForInsert(dict *d, const void *key, dictEntry **existing); /* 查找插入位置 */
dictEntry *dictInsertAtPosition(dict *d, void *key, void *position); /* 在指定位置插入 */
dictEntry *dictAddOrFind(dict *d, void *key);   /* 添加或查找已存在的键 */
int dictReplace(dict *d, void *key, void *val); /* 添加或覆盖已有键的值 */

/* 删除 */
int dictDelete(dict *d, const void *key);       /* 删除键值对 */
dictEntry *dictUnlink(dict *d, const void *key); /* 断开链接但不释放（用于延迟释放） */
void dictFreeUnlinkedEntry(dict *d, dictEntry *he); /* 释放之前 unlink 的条目 */

/* 两阶段删除（用于避免重复查找） */
dictEntry *dictTwoPhaseUnlinkFind(dict *d, const void *key, dictEntry ***plink, int *table_index);
void dictTwoPhaseUnlinkFree(dict *d, dictEntry *he, dictEntry **plink, int table_index);

/* 释放整个 dict */
void dictRelease(dict *d);

/* 查找 */
dictEntry * dictFind(dict *d, const void *key); /* 查找键对应的条目 */
void *dictFetchValue(dict *d, const void *key); /* 查找并返回键对应的值 */

/* 自动调整大小的判断 */
int dictShrinkIfNeeded(dict *d);                /* 判断是否需要收缩 */
int dictExpandIfNeeded(dict *d);                /* 判断是否需要扩展 */

/* 设置条目的键和值 */
void dictSetKey(dict *d, dictEntry* de, void *key);
void dictSetVal(dict *d, dictEntry *de, void *val);
void dictSetSignedIntegerVal(dictEntry *de, int64_t val);
void dictSetUnsignedIntegerVal(dictEntry *de, uint64_t val);
void dictSetDoubleVal(dictEntry *de, double val);

/* 递增条目的值 */
int64_t dictIncrSignedIntegerVal(dictEntry *de, int64_t val);
uint64_t dictIncrUnsignedIntegerVal(dictEntry *de, uint64_t val);
double dictIncrDoubleVal(dictEntry *de, double val);

/* 获取条目的元数据、键和值 */
void *dictEntryMetadata(dictEntry *de);
void *dictGetKey(const dictEntry *de);
void *dictGetVal(const dictEntry *de);
int64_t dictGetSignedIntegerVal(const dictEntry *de);
uint64_t dictGetUnsignedIntegerVal(const dictEntry *de);
double dictGetDoubleVal(const dictEntry *de);
double *dictGetDoubleValPtr(dictEntry *de);

/* 内存使用统计 */
size_t dictMemUsage(const dict *d);             /* dict 的内存使用量（不含键值） */
size_t dictEntryMemUsage(void);                 /* 单个 dictEntry 的大小 */

/* 迭代器 */
dictIterator *dictGetIterator(dict *d);         /* 获取非安全迭代器 */
dictIterator *dictGetSafeIterator(dict *d);     /* 获取安全迭代器 */
void dictInitIterator(dictIterator *iter, dict *d);      /* 初始化非安全迭代器 */
void dictInitSafeIterator(dictIterator *iter, dict *d);  /* 初始化安全迭代器 */
void dictResetIterator(dictIterator *iter);     /* 重置迭代器 */
dictEntry *dictNext(dictIterator *iter);        /* 获取下一个条目 */
void dictReleaseIterator(dictIterator *iter);   /* 释放迭代器 */

/* 随机键获取 */
dictEntry *dictGetRandomKey(dict *d);           /* 获取一个随机键 */
dictEntry *dictGetFairRandomKey(dict *d);       /* 获取一个更均匀分布的随机键 */
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count); /* 获取多个随机键 */

/* 统计信息 */
void dictGetStats(char *buf, size_t bufsize, dict *d, int full); /* 获取统计信息字符串 */

/* 哈希函数 */
uint64_t dictGenHashFunction(const void *key, size_t len);       /* 生成哈希值 */
uint64_t dictGenCaseHashFunction(const unsigned char *buf, size_t len); /* 生成不区分大小写的哈希值 */

/* 清空和配置 */
void dictEmpty(dict *d, void(callback)(dict*)); /* 清空 dict */
void dictSetResizeEnabled(dictResizeEnable enable); /* 设置全局调整大小策略 */

/* rehash 操作 */
int dictRehash(dict *d, int n);                 /* 执行 N 步增量 rehash */
int dictRehashMicroseconds(dict *d, uint64_t us); /* 在指定微秒数内执行 rehash */

/* 哈希函数种子 */
void dictSetHashFunctionSeed(uint8_t *seed);    /* 设置哈希函数种子 */
uint8_t *dictGetHashFunctionSeed(void);         /* 获取哈希函数种子 */

/* 迭代扫描（支持 rehash 期间的安全遍历） */
unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, void *privdata);
unsigned long dictScanDefrag(dict *d, unsigned long v, dictScanFunction *fn, dictDefragFunctions *defragfns, void *privdata);

/* 辅助函数 */
uint64_t dictGetHash(dict *d, const void *key); /* 获取键的哈希值 */
dictEntry *dictFindEntryByPtrAndHash(dict *d, const void *oldptr, uint64_t hash); /* 通过指针和哈希查找 */
void dictRehashingInfo(dict *d, unsigned long long *from_size, unsigned long long *to_size); /* 获取 rehash 信息 */

size_t dictGetStatsMsg(char *buf, size_t bufsize, dictStats *stats, int full);
dictStats* dictGetStatsHt(dict *d, int htidx, int full);
void dictCombineStats(dictStats *from, dictStats *into);
void dictFreeStats(dictStats *stats);

#ifdef REDIS_TEST
int dictTest(int argc, char *argv[], int flags);
#endif

#endif /* __DICT_H */
