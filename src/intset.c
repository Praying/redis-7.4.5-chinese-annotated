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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "intset.h"
#include "zmalloc.h"
#include "endianconv.h"
#include "redisassert.h"

/* Note that these encodings are ordered, so:
 * INTSET_ENC_INT16 < INTSET_ENC_INT32 < INTSET_ENC_INT64. */
/* intset 的三种编码常量：分别对应 16 位、32 位、64 位整数。
 * 注意：它们的数值（sizeof 结果）是有序的：
 *   INTSET_ENC_INT16 < INTSET_ENC_INT32 < INTSET_ENC_INT64
 * 这一顺序在升级判断、宏比较时被依赖。 */
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))

/* Return the required encoding for the provided value. */
/* 根据给定的整数值 v，返回能容纳该值所需的最小编码。
 * 取值范围：
 *   - 落在 [INT16_MIN, INT16_MAX] 区间 -> INTSET_ENC_INT16；
 *   - 落在 [INT32_MIN, INT32_MAX] 区间 -> INTSET_ENC_INT32；
 *   - 其他（需用 64 位表示）       -> INTSET_ENC_INT64。 */
static uint8_t _intsetValueEncoding(int64_t v) {
    if (v < INT32_MIN || v > INT32_MAX)
        // 超出 int32 范围，必须使用 64 位编码
        return INTSET_ENC_INT64;
    else if (v < INT16_MIN || v > INT16_MAX)
        // 超出 int16 范围但仍在 int32 范围内，使用 32 位编码
        return INTSET_ENC_INT32;
    else
        // 在 int16 范围内即可表示，使用最节省的 16 位编码
        return INTSET_ENC_INT16;
}

/* Return the value at pos, given an encoding. */
/* 按指定编码 enc，从 intset 的 contents 中读取 pos 位置上的元素。
 * 会根据当前主机的字节序做必要的字节翻转（memrev*ifbe），
 * 以保证 intset 在大端 / 小端机器上的内容布局一致。 */
static int64_t _intsetGetEncoded(intset *is, int pos, uint8_t enc) {
    int64_t v64;
    int32_t v32;
    int16_t v16;

    if (enc == INTSET_ENC_INT64) {
        // 64 位编码：拷贝 8 字节后做字节序修正
        memcpy(&v64,((int64_t*)is->contents)+pos,sizeof(v64));
        memrev64ifbe(&v64);
        return v64;
    } else if (enc == INTSET_ENC_INT32) {
        // 32 位编码：拷贝 4 字节后做字节序修正，再符号扩展为 int64
        memcpy(&v32,((int32_t*)is->contents)+pos,sizeof(v32));
        memrev32ifbe(&v32);
        return v32;
    } else {
        // 16 位编码：拷贝 2 字节后做字节序修正，再符号扩展为 int64
        memcpy(&v16,((int16_t*)is->contents)+pos,sizeof(v16));
        memrev16ifbe(&v16);
        return v16;
    }
}

/* Return the value at pos, using the configured encoding. */
/* 按 intset 自身 encoding 字段读取 pos 位置上的元素。
 * is->encoding 在磁盘 / 跨主机场景使用大端存储，
 * 这里先通过 intrev32ifbe 转换回本机字节序再使用。 */
static int64_t _intsetGet(intset *is, int pos) {
    return _intsetGetEncoded(is,pos,intrev32ifbe(is->encoding));
}

/* Set the value at pos, using the configured encoding. */
/* 将 value 写入 intset 的 pos 位置，宽度由 is->encoding 决定。
 * 由于 contents[] 是按本机字节序写入的，intset 在落盘前
 * 需要再做一次 memrev*ifbe 以保证大端布局。 */
static void _intsetSet(intset *is, int pos, int64_t value) {
    uint32_t encoding = intrev32ifbe(is->encoding);

    if (encoding == INTSET_ENC_INT64) {
        // 64 位写入：直接存储并按需做字节序翻转
        ((int64_t*)is->contents)[pos] = value;
        memrev64ifbe(((int64_t*)is->contents)+pos);
    } else if (encoding == INTSET_ENC_INT32) {
        // 32 位写入：截断到 int32 后存储并做字节序翻转
        ((int32_t*)is->contents)[pos] = value;
        memrev32ifbe(((int32_t*)is->contents)+pos);
    } else {
        // 16 位写入：截断到 int16 后存储并做字节序翻转
        ((int16_t*)is->contents)[pos] = value;
        memrev16ifbe(((int16_t*)is->contents)+pos);
    }
}

/* Create an empty intset. */
/* 创建一个空的 intset：仅分配 intset header 本身的内存，
 * 初始编码为最省的 INTSET_ENC_INT16，长度为 0。
 * contents 区域随着元素增删再由 intsetResize 调整。 */
intset *intsetNew(void) {
    intset *is = zmalloc(sizeof(intset));
    is->encoding = intrev32ifbe(INTSET_ENC_INT16);
    is->length = 0;
    return is;
}

/* Resize the intset */
/* 调整 intset 的 contents 区域，使能容纳 len 个元素。
 * 实际所需字节数 = len * 单个元素的字节宽度。
 * 重新分配时连同 header 一起 zrealloc，以保持单块连续内存布局。 */
static intset *intsetResize(intset *is, uint32_t len) {
    uint64_t size = (uint64_t)len*intrev32ifbe(is->encoding);
    // 防止 len * encoding 溢出后导致分配过小
    assert(size <= SIZE_MAX - sizeof(intset));
    is = zrealloc(is,sizeof(intset)+size);
    return is;
}

/* Search for the position of "value". Return 1 when the value was found and
 * sets "pos" to the position of the value within the intset. Return 0 when
 * the value is not present in the intset and sets "pos" to the position
 * where "value" can be inserted. */
/* 在有序 intset 中查找 value。
 * 返回值：
 *   1 - 找到，并通过 *pos 返回其在 intset 中的下标；
 *   0 - 未找到，并通过 *pos 返回可保持有序的插入位置。
 * 由于 intset 元素按升序排列，这里使用二分查找。 */
static uint8_t intsetSearch(intset *is, int64_t value, uint32_t *pos) {
    int min = 0, max = intrev32ifbe(is->length)-1, mid = -1;
    int64_t cur = -1;

    /* The value can never be found when the set is empty */
    // 集合为空时不可能命中，插入位置固定为 0
    if (intrev32ifbe(is->length) == 0) {
        if (pos) *pos = 0;
        return 0;
    } else {
        /* Check for the case where we know we cannot find the value,
         * but do know the insert position. */
        // 提前判断两端，避免无意义的二分：value 大于最大值时
        // 只能追加在末尾；小于最小值时只能插在头部。
        if (value > _intsetGet(is,max)) {
            if (pos) *pos = intrev32ifbe(is->length);
            return 0;
        } else if (value < _intsetGet(is,0)) {
            if (pos) *pos = 0;
            return 0;
        }
    }

    while(max >= min) {
        // 使用无符号位移避免 (min+max) 溢出
        mid = ((unsigned int)min + (unsigned int)max) >> 1;
        cur = _intsetGet(is,mid);
        if (value > cur) {
            // 目标值在右半区
            min = mid+1;
        } else if (value < cur) {
            // 目标值在左半区
            max = mid-1;
        } else {
            // 命中，跳出循环
            break;
        }
    }

    if (value == cur) {
        // 找到元素，pos 指向元素下标
        if (pos) *pos = mid;
        return 1;
    } else {
        // 未找到，pos 指向保持有序的插入位置
        if (pos) *pos = min;
        return 0;
    }
}

/* Upgrades the intset to a larger encoding and inserts the given integer. */
/* 将 intset 的编码升级到能容纳 value 的更宽编码，并顺带把 value 插入。
 * 由于 value 必然落在当前已有区间之外（否则无需升级），
 * 因此它要么比所有元素都小（插到头部），要么比所有元素都大（追加到尾部）。 */
static intset *intsetUpgradeAndAdd(intset *is, int64_t value) {
    uint8_t curenc = intrev32ifbe(is->encoding);
    uint8_t newenc = _intsetValueEncoding(value);
    int length = intrev32ifbe(is->length);
    int prepend = value < 0 ? 1 : 0;

    /* First set new encoding and resize */
    // 先把 header 的 encoding 升级，再按新编码多分配 1 个元素的空间
    is->encoding = intrev32ifbe(newenc);
    is = intsetResize(is,intrev32ifbe(is->length)+1);

    /* Upgrade back-to-front so we don't overwrite values.
     * Note that the "prepend" variable is used to make sure we have an empty
     * space at either the beginning or the end of the intset. */
    // 从尾到头逐元素搬迁，避免新编码下尚未写入的位置被覆盖；
    // prepend=1 时搬迁到 length+1（首部留空），否则搬迁到 length+0（尾部留空）。
    while(length--)
        _intsetSet(is,length+prepend,_intsetGetEncoded(is,length,curenc));

    /* Set the value at the beginning or the end. */
    // 把新 value 放到预留出来的空位上
    if (prepend)
        _intsetSet(is,0,value);
    else
        _intsetSet(is,intrev32ifbe(is->length),value);
    is->length = intrev32ifbe(intrev32ifbe(is->length)+1);
    return is;
}

/* 将 intset 中 [from, length) 区间整体移动到以 to 为起点的位置。
 * 主要用于插入 / 删除时在 contents 内部腾出或回收元素位置。
 * 使用 memmove 处理 src/dst 区域重叠的情况。 */
static void intsetMoveTail(intset *is, uint32_t from, uint32_t to) {
    void *src, *dst;
    uint32_t bytes = intrev32ifbe(is->length)-from;
    uint32_t encoding = intrev32ifbe(is->encoding);

    if (encoding == INTSET_ENC_INT64) {
        // 64 位编码：按 8 字节为单位移动
        src = (int64_t*)is->contents+from;
        dst = (int64_t*)is->contents+to;
        bytes *= sizeof(int64_t);
    } else if (encoding == INTSET_ENC_INT32) {
        // 32 位编码：按 4 字节为单位移动
        src = (int32_t*)is->contents+from;
        dst = (int32_t*)is->contents+to;
        bytes *= sizeof(int32_t);
    } else {
        // 16 位编码：按 2 字节为单位移动
        src = (int16_t*)is->contents+from;
        dst = (int16_t*)is->contents+to;
        bytes *= sizeof(int16_t);
    }
    memmove(dst,src,bytes);
}

/* Insert an integer in the intset */
/* 向 intset 中插入一个整数（自动维持有序与最小编码）。
 * 若 value 已存在则不重复插入，并通过 *success=0 通知调用方。 */
intset *intsetAdd(intset *is, int64_t value, uint8_t *success) {
    uint8_t valenc = _intsetValueEncoding(value);
    uint32_t pos;
    if (success) *success = 1;

    /* Upgrade encoding if necessary. If we need to upgrade, we know that
     * this value should be either appended (if > 0) or prepended (if < 0),
     * because it lies outside the range of existing values. */
    // 所需编码比当前编码更宽：必须升级，并直接由 intsetUpgradeAndAdd
    // 完成"升级 + 插入"两件事，该路径一定成功。
    if (valenc > intrev32ifbe(is->encoding)) {
        /* This always succeeds, so we don't need to curry *success. */
        return intsetUpgradeAndAdd(is,value);
    } else {
        /* Abort if the value is already present in the set.
         * This call will populate "pos" with the right position to insert
         * the value when it cannot be found. */
        // 当前编码已足够容纳 value；先查重，命中则直接返回。
        if (intsetSearch(is,value,&pos)) {
            if (success) *success = 0;
            return is;
        }

        // 扩容一个位置，然后把 [pos, length) 的尾部整体后移一位
        is = intsetResize(is,intrev32ifbe(is->length)+1);
        if (pos < intrev32ifbe(is->length)) intsetMoveTail(is,pos,pos+1);
    }

    // 在腾出的空位上写入新 value
    _intsetSet(is,pos,value);
    is->length = intrev32ifbe(intrev32ifbe(is->length)+1);
    return is;
}

/* Delete integer from intset */
/* 从 intset 中删除指定的整数（若存在）。
 * 注意：intset 永远不会"降级"编码，只调整元素个数与 contents 长度。 */
intset *intsetRemove(intset *is, int64_t value, int *success) {
    uint8_t valenc = _intsetValueEncoding(value);
    uint32_t pos;
    if (success) *success = 0;

    if (valenc <= intrev32ifbe(is->encoding) && intsetSearch(is,value,&pos)) {
        uint32_t len = intrev32ifbe(is->length);

        /* We know we can delete */
        if (success) *success = 1;

        /* Overwrite value with tail and update length */
        // 把 [pos+1, length) 区间前移一位以覆盖被删元素
        if (pos < (len-1)) intsetMoveTail(is,pos+1,pos);
        // 缩容一个元素的位置
        is = intsetResize(is,len-1);
        is->length = intrev32ifbe(len-1);
    }
    return is;
}

/* Determine whether a value belongs to this set */
/* 判断 value 是否属于该 intset。
 * 快速短路：value 所需编码比 intset 编码更宽时一定不存在。 */
uint8_t intsetFind(intset *is, int64_t value) {
    uint8_t valenc = _intsetValueEncoding(value);
    return valenc <= intrev32ifbe(is->encoding) && intsetSearch(is,value,NULL);
}

/* Return random member */
/* 随机返回 intset 中的一个元素。
 * 调用前应保证 intset 非空；空集合会通过 assert 在 rand()%len 中
 * 触发除零失败，从而尽早暴露损坏的 intset。 */
int64_t intsetRandom(intset *is) {
    uint32_t len = intrev32ifbe(is->length);
    assert(len); /* avoid division by zero on corrupt intset payload. */
    return _intsetGet(is,rand()%len);
}

/* Return the largest member. */
/* 返回 intset 中的最大元素（因升序有序，即末位元素）。 */
int64_t intsetMax(intset *is) {
    uint32_t len = intrev32ifbe(is->length);
    return _intsetGet(is, len - 1);
}

/* Return the smallest member. */
/* 返回 intset 中的最小元素（因升序有序，即首位元素）。 */
int64_t intsetMin(intset *is) {
    return _intsetGet(is, 0);
}

/* Get the value at the given position. When this position is
 * out of range the function returns 0, when in range it returns 1. */
/* 按下标取元素：pos 合法时通过 *value 返回元素并返回 1；
 * 越界时返回 0 且不修改 *value。 */
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value) {
    if (pos < intrev32ifbe(is->length)) {
        *value = _intsetGet(is,pos);
        return 1;
    }
    return 0;
}

/* Return intset length */
/* 返回 intset 中保存的元素个数。 */
uint32_t intsetLen(const intset *is) {
    return intrev32ifbe(is->length);
}

/* Return intset blob size in bytes. */
/* 返回整个 intset 占用字节数 = header 大小 + 元素总字节数。
 * 该值也是 RDB 写入 / 读取时该 intset 的"载荷"长度。 */
size_t intsetBlobLen(intset *is) {
    return sizeof(intset)+(size_t)intrev32ifbe(is->length)*intrev32ifbe(is->encoding);
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we make sure there are no duplicate or out of order records. */
/* 校验 intset 数据的完整性，常用于 RDB / AOF 加载时防御恶意 / 损坏文件。
 * 校验内容（按顺序）：
 *   1) 缓冲区能容纳完整的 intset header；
 *   2) encoding 字段是合法的三种编码之一；
 *   3) length 字段与缓冲区大小匹配（所有元素都落在缓冲区内）；
 *   4) length 不为 0（intset 编码后至少含 1 个元素）；
 *   5) 当 deep=1 时，元素严格升序且无重复。 */
int intsetValidateIntegrity(const unsigned char *p, size_t size, int deep) {
    intset *is = (intset *)p;
    /* check that we can actually read the header. */
    // 缓冲区太小，连 header 都放不下，直接判为非法
    if (size < sizeof(*is))
        return 0;

    uint32_t encoding = intrev32ifbe(is->encoding);

    size_t record_size;
    if (encoding == INTSET_ENC_INT64) {
        record_size = INTSET_ENC_INT64;
    } else if (encoding == INTSET_ENC_INT32) {
        record_size = INTSET_ENC_INT32;
    } else if (encoding == INTSET_ENC_INT16){
        record_size = INTSET_ENC_INT16;
    } else {
        // 非法编码
        return 0;
    }

    /* check that the size matches (all records are inside the buffer). */
    // 用 length 和 record_size 反推出的总大小应与 size 完全一致
    uint32_t count = intrev32ifbe(is->length);
    if (sizeof(*is) + count*record_size != size)
        return 0;

    /* check that the set is not empty. */
    // 加载得到的 intset 元素数不应为 0
    if (count==0)
        return 0;

    if (!deep)
        return 1;

    /* check that there are no dup or out of order records. */
    // 深度校验：相邻元素必须严格递增（隐含无重复）
    int64_t prev = _intsetGet(is,0);
    for (uint32_t i=1; i<count; i++) {
        int64_t cur = _intsetGet(is,i);
        if (cur <= prev)
            return 0;
        prev = cur;
    }

    return 1;
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include <time.h>

#if 0
/* 调试用：按 intset 顺序打印所有元素。 */
static void intsetRepr(intset *is) {
    for (uint32_t i = 0; i < intrev32ifbe(is->length); i++) {
        printf("%lld\n", (uint64_t)_intsetGet(is,i));
    }
    printf("\n");
}

/* 调试用：打印错误信息并退出。 */
static void error(char *err) {
    printf("%s\n", err);
    exit(1);
}
#endif

/* 单元测试辅助：打印 "OK" 表示本小节通过。 */
static void ok(void) {
    printf("OK\n");
}

/* 单元测试辅助：返回当前时间（微秒），用于性能测试。 */
static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

/* 单元测试辅助：构造一个含有 size 个随机元素的 intset，
 * 每个元素的随机位宽由 bits 控制（>32 时使用 64 位随机数）。 */
static intset *createSet(int bits, int size) {
    uint64_t mask = (1<<bits)-1;
    uint64_t value;
    intset *is = intsetNew();

    for (int i = 0; i < size; i++) {
        if (bits > 32) {
            // 位宽大于 32 时，rand() 一次不够，用 rand()*rand() 扩展
            value = (rand()*rand()) & mask;
        } else {
            value = rand() & mask;
        }
        is = intsetAdd(is,value,NULL);
    }
    return is;
}

/* 单元测试辅助：检查 intset 元素严格升序。 */
static void checkConsistency(intset *is) {
    for (uint32_t i = 0; i < (intrev32ifbe(is->length)-1); i++) {
        uint32_t encoding = intrev32ifbe(is->encoding);

        if (encoding == INTSET_ENC_INT16) {
            int16_t *i16 = (int16_t*)is->contents;
            assert(i16[i] < i16[i+1]);
        } else if (encoding == INTSET_ENC_INT32) {
            int32_t *i32 = (int32_t*)is->contents;
            assert(i32[i] < i32[i+1]);
        } else {
            int64_t *i64 = (int64_t*)is->contents;
            assert(i64[i] < i64[i+1]);
        }
    }
}

#define UNUSED(x) (void)(x)
/* intset 模块的单元测试入口，覆盖：
 *  - 编码选择函数 _intsetValueEncoding 的边界值；
 *  - 基本增删与极值取值；
 *  - 大量随机插入；
 *  - 编码升级路径（int16 -> int32 -> int64，正向/负向触发）；
 *  - 大数量下的查找 / 增删性能与一致性。 */
int intsetTest(int argc, char **argv, int flags) {
    uint8_t success;
    int i;
    intset *is;
    srand(time(NULL));

    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    // 用典型边界值校验 _intsetValueEncoding 的编码选择
    printf("Value encodings: "); {
        assert(_intsetValueEncoding(-32768) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(+32767) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(-32769) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+32768) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483648) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+2147483647) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483649) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+2147483648) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(-9223372036854775808ull) ==
                    INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+9223372036854775807ull) ==
                    INTSET_ENC_INT64);
        ok();
    }

    // 基本插入：覆盖去重、最大/最小取值
    printf("Basic adding: "); {
        is = intsetNew();
        is = intsetAdd(is,5,&success); assert(success);
        is = intsetAdd(is,6,&success); assert(success);
        is = intsetAdd(is,4,&success); assert(success);
        is = intsetAdd(is,4,&success); assert(!success);
        assert(6 == intsetMax(is));
        assert(4 == intsetMin(is));
        ok();
        zfree(is);
    }

    // 大量随机插入：验证长度统计与有序性
    printf("Large number of random adds: "); {
        uint32_t inserts = 0;
        is = intsetNew();
        for (i = 0; i < 1024; i++) {
            is = intsetAdd(is,rand()%0x800,&success);
            if (success) inserts++;
        }
        assert(intrev32ifbe(is->length) == inserts);
        checkConsistency(is);
        ok();
        zfree(is);
    }

    // 编码升级：int16 -> int32，分别用正、负值触发
    printf("Upgrade from int16 to int32: "); {
        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is,32));
        assert(intsetFind(is,65535));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,-65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is,32));
        assert(intsetFind(is,-65535));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    // 编码升级：int16 -> int64，分别用正、负值触发
    printf("Upgrade from int16 to int64: "); {
        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,32));
        assert(intsetFind(is,4294967295));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,-4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,32));
        assert(intsetFind(is,-4294967295));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    // 编码升级：int32 -> int64，分别用正、负值触发
    printf("Upgrade from int32 to int64: "); {
        is = intsetNew();
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is,4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,65535));
        assert(intsetFind(is,4294967295));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is,-4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,65535));
        assert(intsetFind(is,-4294967295));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    // 压力测试：大量二分查找的性能
    printf("Stress lookups: "); {
        long num = 100000, size = 10000;
        int i, bits = 20;
        long long start;
        is = createSet(bits,size);
        checkConsistency(is);

        start = usec();
        for (i = 0; i < num; i++) intsetSearch(is,rand() % ((1<<bits)-1),NULL);
        printf("%ld lookups, %ld element set, %lldusec\n",
               num,size,usec()-start);
        zfree(is);
    }

    // 压力测试：插入与删除交替进行，验证去重 / 缩容 / 查找
    printf("Stress add+delete: "); {
        int i, v1, v2;
        is = intsetNew();
        for (i = 0; i < 0xffff; i++) {
            v1 = rand() % 0xfff;
            is = intsetAdd(is,v1,NULL);
            assert(intsetFind(is,v1));

            v2 = rand() % 0xfff;
            is = intsetRemove(is,v2,NULL);
            assert(!intsetFind(is,v2));
        }
        checkConsistency(is);
        ok();
        zfree(is);
    }

    return 0;
}
#endif
