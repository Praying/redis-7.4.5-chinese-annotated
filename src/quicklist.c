/* quicklist.c - A doubly linked list of listpacks
 * quicklist.c - 由 listpack 组成的双向链表实现
 *
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must start the above copyright notice,
 *     this quicklist of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this quicklist of conditions and the following disclaimer in the
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
#include <string.h> /* for memcpy */
#include <limits.h>
#include "quicklist.h"
#include "zmalloc.h"
#include "config.h"
#include "listpack.h"
#include "util.h" /* for ll2string */
#include "lzf.h"
#include "redisassert.h"

#ifndef REDIS_STATIC
/* REDIS_STATIC 宏：用于在 Redis 构建系统中标记仅本翻译单元可见的函数 */
#define REDIS_STATIC static
#endif

/* Optimization levels for size-based filling.
 * Note that the largest possible limit is 64k, so even if each record takes
 * just one byte, it still won't overflow the 16 bit count field.
 *
 * 基于大小填充的优化级别。
 * 注意：最大限制为 64k，因此即使每条记录只占 1 字节，
 * 也不会溢出 16 位的 count 字段。 */
static const size_t optimization_level[] = {4096, 8192, 16384, 32768, 65536};

/* This is for test suite development purposes only, 0 means disabled.
 * 此变量仅用于测试套件的开发目的，0 表示禁用。 */
static size_t packed_threshold = 0;

/* set threshold for PLAIN nodes for test suit, the real limit is based on `fill`
 * 为测试套件设置 PLAIN 节点的阈值；真实限制由 `fill` 决定。 */
int quicklistSetPackedThreshold(size_t sz) {
    /* Don't allow threshold to be set above or even slightly below 4GB
     * 不允许将阈值设置为 4GB 及以上（甚至略低于 4GB 也不行） */
    if (sz > (1ull<<32) - (1<<20)) {
        return 0;
    }
    packed_threshold = sz;
    return 1;
}

/* Maximum size in bytes of any multi-element listpack.
 * Larger values will live in their own isolated listpacks.
 * This is used only if we're limited by record count. when we're limited by
 * size, the maximum limit is bigger, but still safe.
 * 8k is a recommended / default size limit
 *
 * 任意多元素 listpack 的最大字节数。
 * 超过此值的元素会单独存放在自己的 listpack 中。
 * 该限制仅在按记录数限制时使用；按大小限制时，最大限制更大，但同样安全。
 * 8k 是推荐/默认的大小限制。 */
#define SIZE_SAFETY_LIMIT 8192

/* Maximum estimate of the listpack entry overhead.
 * Although in the worst case(sz < 64), we will waste 6 bytes in one
 * quicklistNode, but can avoid memory waste due to internal fragmentation
 * when the listpack exceeds the size limit by a few bytes (e.g. being 16388).
 *
 * listpack 条目开销的最大估计值。
 * 尽管在最坏情况下（sz < 64）会在一个 quicklistNode 中浪费 6 字节，
 * 但可以避免当 listpack 仅超出大小限制几个字节（例如 16388）时
 * 因内存内部碎片导致的浪费。 */
#define SIZE_ESTIMATE_OVERHEAD 8

/* Minimum listpack size in bytes for attempting compression.
 * 尝试压缩的 listpack 最小字节数。 */
#define MIN_COMPRESS_BYTES 48

/* Minimum size reduction in bytes to store compressed quicklistNode data.
 * This also prevents us from storing compression if the compression
 * resulted in a larger size than the original data.
 *
 * 存储压缩 quicklistNode 数据所需的最小压缩收益（字节）。
 * 这也用于阻止当压缩后的体积反而大于原始数据时仍存储压缩数据。 */
#define MIN_COMPRESS_IMPROVE 8

/* If not verbose testing, remove all debug printing.
 * 若未启用详细测试模式，则去除所有调试打印。 */
#ifndef REDIS_TEST_VERBOSE
/* 未启用详细测试时，调试宏 D 等价于空操作 */
#define D(...)
#else
/* 详细测试模式下，D(...) 会在标准输出中打印文件名、函数名、行号及消息 */
#define D(...)                                                                 \
    do {                                                                       \
        printf("%s:%s:%d:\t", __FILE__, __func__, __LINE__);                   \
        printf(__VA_ARGS__);                                                   \
        printf("\n");                                                          \
    } while (0)
#endif

/* Bookmarks forward declarations
 * 书签相关函数的前向声明 */
#define QL_MAX_BM ((1 << QL_BM_BITS)-1)
quicklistBookmark *_quicklistBookmarkFindByName(quicklist *ql, const char *name);
quicklistBookmark *_quicklistBookmarkFindByNode(quicklist *ql, quicklistNode *node);
void _quicklistBookmarkDelete(quicklist *ql, quicklistBookmark *bm);

/* _quicklistSplitNode：根据 offset/after 将节点拆分为两个节点 */
REDIS_STATIC quicklistNode *_quicklistSplitNode(quicklistNode *node, int offset, int after);
/* _quicklistMergeNodes：尝试合并 center 节点与其相邻节点 */
REDIS_STATIC quicklistNode *_quicklistMergeNodes(quicklist *quicklist, quicklistNode *center);

/* Simple way to give quicklistEntry structs default values with one call.
 * 通过一次调用为 quicklistEntry 结构体设置默认值（统一初始化）。 */
#define initEntry(e)                                                           \
    do {                                                                       \
        (e)->zi = (e)->value = NULL;                                           \
        (e)->longval = -123456789;                                             \
        (e)->quicklist = NULL;                                                 \
        (e)->node = NULL;                                                      \
        (e)->offset = 123456789;                                               \
        (e)->sz = 0;                                                           \
    } while (0)

/* Reset the quicklistIter to prevent it from being used again after
 * insert, replace, or other against quicklist operation.
 * 重置 quicklistIter，防止其在插入、替换等写操作之后被再次使用。 */
#define resetIterator(iter)                                                    \
    do {                                                                       \
        (iter)->current = NULL;                                                \
        (iter)->zi = NULL;                                                     \
    } while (0)

/* Create a new quicklist.
 * Free with quicklistRelease().
 * 创建一个新的 quicklist。
 * 需要使用 quicklistRelease() 进行释放。 */
quicklist *quicklistCreate(void) {
    struct quicklist *quicklist;

    quicklist = zmalloc(sizeof(*quicklist));        // 分配 quicklist 结构体内存
    quicklist->head = quicklist->tail = NULL;       // 头尾指针初始化为空
    quicklist->len = 0;                             // 节点数清零
    quicklist->count = 0;                           // 元素总数清零
    quicklist->compress = 0;                        // 默认不压缩
    quicklist->fill = -2;                           // 默认 fill 为 -2
    quicklist->bookmark_count = 0;                  // 书签数清零
    return quicklist;
}

#define COMPRESS_MAX ((1 << QL_COMP_BITS)-1)
/* 设置 quicklist 的压缩深度（两端不压缩的节点数），
 * 同时将值约束在合法范围 [0, COMPRESS_MAX] 之内。 */
void quicklistSetCompressDepth(quicklist *quicklist, int compress) {
    if (compress > COMPRESS_MAX) {
        compress = COMPRESS_MAX;
    } else if (compress < 0) {
        compress = 0;
    }
    quicklist->compress = compress;
}

#define FILL_MAX ((1 << (QL_FILL_BITS-1))-1)
/* 设置 quicklist 的填充因子 fill，
 * 将值约束在合法范围 [-5, FILL_MAX] 之内。 */
void quicklistSetFill(quicklist *quicklist, int fill) {
    if (fill > FILL_MAX) {
        fill = FILL_MAX;
    } else if (fill < -5) {
        fill = -5;
    }
    quicklist->fill = fill;
}

/* 同时设置 quicklist 的 fill 和 compress 选项。 */
void quicklistSetOptions(quicklist *quicklist, int fill, int compress) {
    quicklistSetFill(quicklist, fill);
    quicklistSetCompressDepth(quicklist, compress);
}

/* Create a new quicklist with some default parameters.
 * 创建一个带默认参数的 quicklist。
 *   fill：填充因子
 *   compress：压缩深度 */
quicklist *quicklistNew(int fill, int compress) {
    quicklist *quicklist = quicklistCreate();
    quicklistSetOptions(quicklist, fill, compress);
    return quicklist;
}

/* 创建一个空的 quicklistNode（默认使用 RAW + PACKED 容器）。 */
REDIS_STATIC quicklistNode *quicklistCreateNode(void) {
    quicklistNode *node;
    node = zmalloc(sizeof(*node));           // 分配节点内存
    node->entry = NULL;                      // 尚未挂载数据
    node->count = 0;                         // 元素数清零
    node->sz = 0;                            // 数据大小清零
    node->next = node->prev = NULL;          // 前后指针置空
    node->encoding = QUICKLIST_NODE_ENCODING_RAW;      // 默认未压缩
    node->container = QUICKLIST_NODE_CONTAINER_PACKED;  // 默认使用 listpack 容器
    node->recompress = 0;                    // 不需要重压缩
    node->dont_compress = 0;                 // 允许压缩
    return node;
}

/* Return cached quicklist count
 * 返回 quicklist 中缓存的元素总数。 */
unsigned long quicklistCount(const quicklist *ql) { return ql->count; }

/* Free entire quicklist.
 * 释放整个 quicklist，包括所有节点、entry 数据和书签。 */
void quicklistRelease(quicklist *quicklist) {
    unsigned long len;
    quicklistNode *current, *next;

    current = quicklist->head;        // 从头节点开始
    len = quicklist->len;             // 缓存的节点数
    while (len--) {
        next = current->next;         // 先保存后继节点

        zfree(current->entry);        // 释放节点中的 listpack 数据
        quicklist->count -= current->count;  // 扣减元素总数

        zfree(current);               // 释放节点结构体

        quicklist->len--;             // 节点数减一
        current = next;               // 继续处理下一个节点
    }
    quicklistBookmarksClear(quicklist);  // 清除所有书签
    zfree(quicklist);                    // 释放 quicklist 本身
}

/* Compress the listpack in 'node' and update encoding details.
 * Returns 1 if listpack compressed successfully.
 * Returns 0 if compression failed or if listpack too small to compress.
 *
 * 压缩 node 中的 listpack，并更新编码相关字段。
 * 成功压缩返回 1，压缩失败或 listpack 过小则返回 0。 */
REDIS_STATIC int __quicklistCompressNode(quicklistNode *node) {
#ifdef REDIS_TEST
    node->attempted_compress = 1;  // 标记为已尝试压缩（仅测试模式下）
#endif
    if (node->dont_compress) return 0;  // 显式禁止压缩则直接返回

    /* validate that the node is neither
     * tail nor head (it has prev and next)
     * 校验：被压缩的节点既不是头也不是尾（它必须同时具有 prev 和 next）*/
    assert(node->prev && node->next);

    node->recompress = 0;  // 准备压缩，标记不需要重压缩
    /* Don't bother compressing small values
     * 数据太小时不必尝试压缩 */
    if (node->sz < MIN_COMPRESS_BYTES)
        return 0;

    quicklistLZF *lzf = zmalloc(sizeof(*lzf) + node->sz);

    /* Cancel if compression fails or doesn't compress small enough
     * 压缩失败或压缩效果不明显时取消压缩 */
    if (((lzf->sz = lzf_compress(node->entry, node->sz, lzf->compressed,
                                 node->sz)) == 0) ||
        lzf->sz + MIN_COMPRESS_IMPROVE >= node->sz) {
        /* lzf_compress aborts/rejects compression if value not compressible.
         * lzf_compress 在数据不可压缩时会中止/拒绝压缩。 */
        zfree(lzf);
        return 0;
    }
    lzf = zrealloc(lzf, sizeof(*lzf) + lzf->sz);  // 按实际压缩大小重分配
    zfree(node->entry);                             // 释放原始未压缩数据
    node->entry = (unsigned char *)lzf;             // 指向压缩后的 LZF 数据
    node->encoding = QUICKLIST_NODE_ENCODING_LZF;   // 标记为已压缩
    return 1;
}

/* Compress only uncompressed nodes.
 * 仅当节点未压缩时执行压缩操作。 */
#define quicklistCompressNode(_node)                                           \
    do {                                                                       \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_RAW) {     \
            __quicklistCompressNode((_node));                                  \
        }                                                                      \
    } while (0)

/* Uncompress the listpack in 'node' and update encoding details.
 * Returns 1 on successful decode, 0 on failure to decode.
 *
 * 解压 node 中的 listpack，并更新编码相关字段。
 * 解压成功返回 1，失败返回 0。 */
REDIS_STATIC int __quicklistDecompressNode(quicklistNode *node) {
#ifdef REDIS_TEST
    node->attempted_compress = 0;  // 标记为本次未尝试压缩（仅测试模式）
#endif
    node->recompress = 0;          // 重置重压缩标志

    void *decompressed = zmalloc(node->sz);              // 按原始大小分配解压缓冲区
    quicklistLZF *lzf = (quicklistLZF *)node->entry;     // 解析 LZF 头
    if (lzf_decompress(lzf->compressed, lzf->sz, decompressed, node->sz) == 0) {
        /* Someone requested decompress, but we can't decompress.  Not good.
         * 有人请求解压但解压失败。这很糟糕。 */
        zfree(decompressed);
        return 0;
    }
    zfree(lzf);                              // 释放 LZF 容器
    node->entry = decompressed;               // 指向解压后的原始数据
    node->encoding = QUICKLIST_NODE_ENCODING_RAW;  // 标记为未压缩
    return 1;
}

/* Decompress only compressed nodes.
 * 仅当节点已压缩时才执行解压操作。 */
#define quicklistDecompressNode(_node)                                         \
    do {                                                                       \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_LZF) {     \
            __quicklistDecompressNode((_node));                                \
        }                                                                      \
    } while (0)

/* Force node to not be immediately re-compressible
 * 强制解压节点，并标记为用完后需要重新压缩。 */
#define quicklistDecompressNodeForUse(_node)                                   \
    do {                                                                       \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_LZF) {     \
            __quicklistDecompressNode((_node));                                \
            (_node)->recompress = 1;                                           \
        }                                                                      \
    } while (0)

/* Extract the raw LZF data from this quicklistNode.
 * Pointer to LZF data is assigned to '*data'.
 * Return value is the length of compressed LZF data.
 *
 * 从 quicklistNode 中提取原始 LZF 压缩数据。
 * 压缩数据指针通过 *data 返回，函数返回压缩数据的字节长度。 */
size_t quicklistGetLzf(const quicklistNode *node, void **data) {
    quicklistLZF *lzf = (quicklistLZF *)node->entry;  // 解析 LZF 头
    *data = lzf->compressed;                          // 输出压缩数据指针
    return lzf->sz;                                   // 返回压缩数据长度
}

/* 判断 quicklist 是否允许压缩（compress != 0 表示开启压缩） */
#define quicklistAllowsCompression(_ql) ((_ql)->compress != 0)

/* Force 'quicklist' to meet compression guidelines set by compress depth.
 * The only way to guarantee interior nodes get compressed is to iterate
 * to our "interior" compress depth then compress the next node we find.
 * If compress depth is larger than the entire list, we return immediately.
 *
 * 强制 quicklist 满足由 compress depth 设定的压缩规则。
 * 保证内部节点被压缩的唯一方法是：先迭代到 "内部" 压缩深度，
 * 然后压缩我们遇到的下一个节点。
 * 如果压缩深度大于整个列表长度，则直接返回。 */
REDIS_STATIC void __quicklistCompress(const quicklist *quicklist,
                                      quicklistNode *node) {
    if (quicklist->len == 0) return;  // 空列表无需处理

    /* The head and tail should never be compressed (we should not attempt to recompress them)
     * 头节点和尾节点永远不应该被压缩（也不应该尝试重新压缩它们） */
    assert(quicklist->head->recompress == 0 && quicklist->tail->recompress == 0);

    /* If length is less than our compress depth (from both sides),
     * we can't compress anything.
     * 如果列表长度小于两侧的压缩深度之和，则无法对任何节点进行压缩。 */
    if (!quicklistAllowsCompression(quicklist) ||
        quicklist->len < (unsigned int)(quicklist->compress * 2))
        return;

#if 0
    /* Optimized cases for small depth counts
     * 针对较小压缩深度的优化分支（当前未启用） */
    if (quicklist->compress == 1) {
        quicklistNode *h = quicklist->head, *t = quicklist->tail;
        quicklistDecompressNode(h);
        quicklistDecompressNode(t);
        if (h != node && t != node)
            quicklistCompressNode(node);
        return;
    } else if (quicklist->compress == 2) {
        quicklistNode *h = quicklist->head, *hn = h->next, *hnn = hn->next;
        quicklistNode *t = quicklist->tail, *tp = t->prev, *tpp = tp->prev;
        quicklistDecompressNode(h);
        quicklistDecompressNode(hn);
        quicklistDecompressNode(t);
        quicklistDecompressNode(tp);
        if (h != node && hn != node && t != node && tp != node) {
            quicklistCompressNode(node);
        }
        if (hnn != t) {
            quicklistCompressNode(hnn);
        }
        if (tpp != h) {
            quicklistCompressNode(tpp);
        }
        return;
    }
#endif

    /* Iterate until we reach compress depth for both sides of the list.a
     * Note: because we do length checks at the *top* of this function,
     *       we can skip explicit null checks below. Everything exists.
     * 从两端同时迭代，直到到达压缩深度为止。
     * 注：因为函数顶部已做长度检查，下方不再需要显式空指针检查。 */
    quicklistNode *forward = quicklist->head;   // 从头开始的正向指针
    quicklistNode *reverse = quicklist->tail;   // 从尾开始的反向指针
    int depth = 0;                              // 当前深度
    int in_depth = 0;                           // 标记 node 是否处于压缩深度内
    while (depth++ < quicklist->compress) {
        quicklistDecompressNode(forward);   // 解压当前正向节点
        quicklistDecompressNode(reverse);   // 解压当前反向节点

        if (forward == node || reverse == node)
            in_depth = 1;  // 目标节点已落在压缩深度范围内

        /* We passed into compress depth of opposite side of the quicklist
         * so there's no need to compress anything and we can exit.
         * 正反向迭代已穿越过彼此，说明已没有可压缩空间，可以直接退出。 */
        if (forward == reverse || forward->next == reverse)
            return;

        forward = forward->next;     // 正向前进
        reverse = reverse->prev;     // 反向后退
    }

    if (!in_depth)
        quicklistCompressNode(node);  // 节点不在压缩深度内，可以压缩

    /* At this point, forward and reverse are one node beyond depth
     * 此时 forward 和 reverse 正好处于压缩深度之外的那一个节点 */
    quicklistCompressNode(forward);  // 压缩正向首个超深度节点
    quicklistCompressNode(reverse);  // 压缩反向首个超深度节点
}

/* This macro is used to compress a node.
 *
 * If the 'recompress' flag of the node is true, we compress it directly without
 * checking whether it is within the range of compress depth.
 * However, it's important to ensure that the 'recompress' flag of head and tail
 * is always false, as we always assume that head and tail are not compressed.
 *
 * If the 'recompress' flag of the node is false, we check whether the node is
 * within the range of compress depth before compressing it.
 *
 * 该宏用于压缩一个节点。
 *   - 如果节点的 'recompress' 标志为 true，则直接压缩，不检查它是否处于压缩深度内。
 *     但需要确保头尾节点的 'recompress' 始终为 false，
 *     因为我们总是假定头尾节点不会被压缩。
 *   - 如果 'recompress' 为 false，则在压缩前检查节点是否处于压缩深度内。 */
#define quicklistCompress(_ql, _node)                                          \
    do {                                                                       \
        if ((_node)->recompress)                                               \
            quicklistCompressNode((_node));                                    \
        else                                                                   \
            __quicklistCompress((_ql), (_node));                               \
    } while (0)

/* If we previously used quicklistDecompressNodeForUse(), just recompress.
 * 如果之前调用过 quicklistDecompressNodeForUse()，则直接重新压缩。 */
#define quicklistRecompressOnly(_node)                                         \
    do {                                                                       \
        if ((_node)->recompress)                                               \
            quicklistCompressNode((_node));                                    \
    } while (0)

/* Insert 'new_node' after 'old_node' if 'after' is 1.
 * Insert 'new_node' before 'old_node' if 'after' is 0.
 * Note: 'new_node' is *always* uncompressed, so if we assign it to
 *       head or tail, we do not need to uncompress it.
 *
 * 将 new_node 插入到链表：
 *   - 当 after == 1 时，插入到 old_node 之后；
 *   - 当 after == 0 时，插入到 old_node 之前。
 * 注意：new_node 总是未压缩的，因此如果它被设置为头或尾，无需先解压。 */
REDIS_STATIC void __quicklistInsertNode(quicklist *quicklist,
                                        quicklistNode *old_node,
                                        quicklistNode *new_node, int after) {
    if (after) {
        new_node->prev = old_node;
        if (old_node) {
            new_node->next = old_node->next;
            if (old_node->next)
                old_node->next->prev = new_node;  // 维护后继的前向指针
            old_node->next = new_node;            // 维护 old_node 的后继
        }
        if (quicklist->tail == old_node)
            quicklist->tail = new_node;           // 更新尾节点
    } else {
        new_node->next = old_node;
        if (old_node) {
            new_node->prev = old_node->prev;
            if (old_node->prev)
                old_node->prev->next = new_node;  // 维护前驱的后向指针
            old_node->prev = new_node;            // 维护 old_node 的前驱
        }
        if (quicklist->head == old_node)
            quicklist->head = new_node;           // 更新头节点
    }
    /* If this insert creates the only element so far, initialize head/tail.
     * 如果本次插入使链表从空变为非空，则同时初始化头尾指针。 */
    if (quicklist->len == 0) {
        quicklist->head = quicklist->tail = new_node;
    }

    /* Update len first, so in __quicklistCompress we know exactly len
     * 先更新 len，以便 __quicklistCompress 能准确获知链表长度 */
    quicklist->len++;

    if (old_node)
        quicklistCompress(quicklist, old_node);   // 检查 old_node 是否需要压缩

    quicklistCompress(quicklist, new_node);       // 检查 new_node 是否需要压缩
}

/* Wrappers for node inserting around existing node.
 * 在指定节点前后插入新节点的封装函数。 */
REDIS_STATIC void _quicklistInsertNodeBefore(quicklist *quicklist,
                                             quicklistNode *old_node,
                                             quicklistNode *new_node) {
    __quicklistInsertNode(quicklist, old_node, new_node, 0);
}

REDIS_STATIC void _quicklistInsertNodeAfter(quicklist *quicklist,
                                            quicklistNode *old_node,
                                            quicklistNode *new_node) {
    __quicklistInsertNode(quicklist, old_node, new_node, 1);
}

/* 判断 listpack 大小是否满足安全限制（不超过 SIZE_SAFETY_LIMIT） */
#define sizeMeetsSafetyLimit(sz) ((sz) <= SIZE_SAFETY_LIMIT)

/* Calculate the size limit of the quicklist node based on negative 'fill'.
 * 根据负的 fill 值计算 quicklist 节点的大小上限。
 * 当 fill < 0 时，-fill-1 作为 optimization_level 数组的下标。 */
static size_t quicklistNodeNegFillLimit(int fill) {
    assert(fill < 0);
    size_t offset = (-fill) - 1;
    size_t max_level = sizeof(optimization_level) / sizeof(*optimization_level);
    if (offset >= max_level) offset = max_level - 1;  // 防止越界
    return optimization_level[offset];
}

/* Calculate the size limit or length limit of the quicklist node
 * based on 'fill', and is also used to limit list listpack.
 *
 * 根据 fill 计算 quicklist 节点的大小上限和/或元素数上限，
 * 同时也用于限制 list 内部 listpack 的容量。
 *   - fill >= 0 时使用元素数限制；
 *   - fill <  0 时使用大小限制。 */
void quicklistNodeLimit(int fill, size_t *size, unsigned int *count) {
    *size = SIZE_MAX;    // 默认不限制大小
    *count = UINT_MAX;   // 默认不限制元素数

    if (fill >= 0) {
        /* Ensure that one node have at least one entry
         * 保证每个节点至少包含一个元素 */
        *count = (fill == 0) ? 1 : fill;
    } else {
        *size = quicklistNodeNegFillLimit(fill);
    }
}

/* Check if the limit of the quicklist node has been reached to determine if
 * insertions, merges or other operations that would increase the size of
 * the node can be performed.
 * Return 1 if exceeds the limit, otherwise 0.
 *
 * 检查是否已经达到 quicklist 节点的限制，以判断是否可以执行
 * 插入、合并或其他会增大节点体积的操作。
 * 超出限制返回 1，否则返回 0。 */
int quicklistNodeExceedsLimit(int fill, size_t new_sz, unsigned int new_count) {
    size_t sz_limit;
    unsigned int count_limit;
    quicklistNodeLimit(fill, &sz_limit, &count_limit);  // 获取 fill 对应的限制

    if (likely(sz_limit != SIZE_MAX)) {
        return new_sz > sz_limit;     // 按大小判断
    } else if (count_limit != UINT_MAX) {
        /* when we reach here we know that the limit is a size limit (which is
         * safe, see comments next to optimization_level and SIZE_SAFETY_LIMIT)
         * 走到这里时说明限制为大小限制（安全，参见 optimization_level 和
         * SIZE_SAFETY_LIMIT 处的注释） */
        if (!sizeMeetsSafetyLimit(new_sz)) return 1;  // 安全阈值兜底
        return new_count > count_limit;                // 按元素数判断
    }

    redis_unreachable();
}

/* Determines whether a given size qualifies as a large element based on a threshold
 * determined by the 'fill'. If the size is considered large, it will be stored in
 * a plain node.
 *
 * 根据 fill 所确定的阈值判断给定大小是否为大元素。
 * 如果视为大元素，则会单独存放在一个 PLAIN 节点中。 */
static int isLargeElement(size_t sz, int fill) {
    if (unlikely(packed_threshold != 0)) return sz >= packed_threshold;
    if (fill >= 0)
        return !sizeMeetsSafetyLimit(sz);  // 超过安全大小即视为大元素
    else
        return sz > quicklistNodeNegFillLimit(fill);
}

/* 判断节点 node 是否允许继续插入新元素。
 * 节点为 NULL、已经是 PLAIN 节点、或者新元素为大元素时返回 0。 */
REDIS_STATIC int _quicklistNodeAllowInsert(const quicklistNode *node,
                                           const int fill, const size_t sz) {
    if (unlikely(!node))
        return 0;

    if (unlikely(QL_NODE_IS_PLAIN(node) || isLargeElement(sz, fill)))
        return 0;  // PLAIN 节点或大元素不允许插入

    /* Estimate how many bytes will be added to the listpack by this one entry.
     * We prefer an overestimation, which would at worse lead to a few bytes
     * below the lowest limit of 4k (see optimization_level).
     * Note: No need to check for overflow below since both `node->sz` and
     * `sz` are to be less than 1GB after the plain/large element check above.
     *
     * 估算将这一条新 entry 加入 listpack 后增加的字节数。
     * 我们倾向于高估，最坏情况下只会比 4k 的下限少几个字节（见 optimization_level）。
     * 注：通过上面的 PLAIN/大元素检查后，`node->sz` 和 `sz` 均小于 1GB，
     * 因此无需在下方检查整数溢出。 */
    size_t new_sz = node->sz + sz + SIZE_ESTIMATE_OVERHEAD;
    if (unlikely(quicklistNodeExceedsLimit(fill, new_sz, node->count + 1)))
        return 0;
    return 1;
}

/* 判断相邻的两个节点 a 和 b 是否允许合并。
 * PLAIN 节点、或者合并后会超过 fill 限制时返回 0。 */
REDIS_STATIC int _quicklistNodeAllowMerge(const quicklistNode *a,
                                          const quicklistNode *b,
                                          const int fill) {
    if (!a || !b)
        return 0;

    if (unlikely(QL_NODE_IS_PLAIN(a) || QL_NODE_IS_PLAIN(b)))
        return 0;

    /* approximate merged listpack size (- 7 to remove one listpack
     * header/trailer, see LP_HDR_SIZE and LP_EOF)
     * 估算合并后的 listpack 大小（减去 7 字节用于去掉一个 listpack 的头/尾） */
    unsigned int merge_sz = a->sz + b->sz - 7;
    if (unlikely(quicklistNodeExceedsLimit(fill, merge_sz, a->count + b->count)))
        return 0;
    return 1;
}

/* 通过调用 lpBytes() 同步更新节点的 sz 字段 */
#define quicklistNodeUpdateSz(node)                                            \
    do {                                                                       \
        (node)->sz = lpBytes((node)->entry);                                   \
    } while (0)

/* 根据 container 类型创建一个新节点。
 *   - PLAIN：直接分配 sz 字节并拷贝 value；
 *   - PACKED：通过 listpack 接口在新建的 listpack 中预置一个元素。 */
static quicklistNode* __quicklistCreateNode(int container, void *value, size_t sz) {
    quicklistNode *new_node = quicklistCreateNode();
    new_node->container = container;
    if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
        new_node->entry = zmalloc(sz);
        memcpy(new_node->entry, value, sz);
    } else {
        new_node->entry = lpPrepend(lpNew(0), value, sz);
    }
    new_node->sz = sz;
    new_node->count++;
    return new_node;
}

/* 在 old_node 前后插入一个 PLAIN 节点（after 控制方向）。 */
static void __quicklistInsertPlainNode(quicklist *quicklist, quicklistNode *old_node,
                                       void *value, size_t sz, int after)
{
    quicklistNode *new_node = __quicklistCreateNode(QUICKLIST_NODE_CONTAINER_PLAIN, value, sz);
    __quicklistInsertNode(quicklist, old_node, new_node, after);
    quicklist->count++;
}

/* Add new entry to head node of quicklist.
 *
 * Returns 0 if used existing head.
 * Returns 1 if new head created.
 *
 * 向 quicklist 头节点添加一个新条目。
 *   - 如果复用了原头节点返回 0；
 *   - 如果创建了新的头节点返回 1。 */
int quicklistPushHead(quicklist *quicklist, void *value, size_t sz) {
    quicklistNode *orig_head = quicklist->head;  // 记录原始头节点

    if (unlikely(isLargeElement(sz, quicklist->fill))) {
        // 大元素以 PLAIN 节点形式插入到当前头节点之前
        __quicklistInsertPlainNode(quicklist, quicklist->head, value, sz, 0);
        return 1;
    }

    if (likely(
            _quicklistNodeAllowInsert(quicklist->head, quicklist->fill, sz))) {
        // 头节点空间充足：直接前插到 listpack
        quicklist->head->entry = lpPrepend(quicklist->head->entry, value, sz);
        quicklistNodeUpdateSz(quicklist->head);   // 同步 sz
    } else {
        // 头节点已满：新建节点并放在原头节点之前
        quicklistNode *node = quicklistCreateNode();
        node->entry = lpPrepend(lpNew(0), value, sz);

        quicklistNodeUpdateSz(node);
        _quicklistInsertNodeBefore(quicklist, quicklist->head, node);
    }
    quicklist->count++;            // 总元素数加一
    quicklist->head->count++;      // 头节点元素数加一
    return (orig_head != quicklist->head);  // 判断是否新建了头节点
}

/* Add new entry to tail node of quicklist.
 *
 * Returns 0 if used existing tail.
 * Returns 1 if new tail created.
 *
 * 向 quicklist 尾节点添加一个新条目。
 *   - 如果复用了原尾节点返回 0；
 *   - 如果创建了新的尾节点返回 1。 */
int quicklistPushTail(quicklist *quicklist, void *value, size_t sz) {
    quicklistNode *orig_tail = quicklist->tail;  // 记录原始尾节点
    if (unlikely(isLargeElement(sz, quicklist->fill))) {
        // 大元素以 PLAIN 节点形式插入到尾节点之后
        __quicklistInsertPlainNode(quicklist, quicklist->tail, value, sz, 1);
        return 1;
    }

    if (likely(
            _quicklistNodeAllowInsert(quicklist->tail, quicklist->fill, sz))) {
        // 尾节点空间充足：直接追加到 listpack
        quicklist->tail->entry = lpAppend(quicklist->tail->entry, value, sz);
        quicklistNodeUpdateSz(quicklist->tail);   // 同步 sz
    } else {
        // 尾节点已满：新建节点并放在原尾节点之后
        quicklistNode *node = quicklistCreateNode();
        node->entry = lpAppend(lpNew(0), value, sz);

        quicklistNodeUpdateSz(node);
        _quicklistInsertNodeAfter(quicklist, quicklist->tail, node);
    }
    quicklist->count++;            // 总元素数加一
    quicklist->tail->count++;      // 尾节点元素数加一
    return (orig_tail != quicklist->tail);  // 判断是否新建了尾节点
}

/* Create new node consisting of a pre-formed listpack.
 * Used for loading RDBs where entire listpacks have been stored
 * to be retrieved later.
 *
 * 创建一个由预制 listpack 组成的新节点。
 * 用于加载 RDB 文件（其中整个 listpack 已被保存）的情况。
 *   - zl：所引用的 listpack 数据（所有权交由 quicklist 管理） */
void quicklistAppendListpack(quicklist *quicklist, unsigned char *zl) {
    quicklistNode *node = quicklistCreateNode();

    node->entry = zl;                              // 直接挂载 listpack
    node->count = lpLength(node->entry);          // 通过 lpLength 统计元素数
    node->sz = lpBytes(zl);                        // 通过 lpBytes 统计字节数

    _quicklistInsertNodeAfter(quicklist, quicklist->tail, node);
    quicklist->count += node->count;               // 累加总元素数
}

/* Create new node consisting of a pre-formed plain node.
 * Used for loading RDBs where entire plain node has been stored
 * to be retrieved later.
 * data - the data to add (pointer becomes the responsibility of quicklist)
 *
 * 创建一个由预制 PLAIN 节点组成的新节点。
 * 用于加载 RDB 文件（其中整个 PLAIN 节点已被保存）的情况。
 *   - data：要添加的数据（指针所有权交由 quicklist 管理） */
void quicklistAppendPlainNode(quicklist *quicklist, unsigned char *data, size_t sz) {
    quicklistNode *node = quicklistCreateNode();

    node->entry = data;                                       // 直接挂载数据
    node->count = 1;                                          // PLAIN 节点只有 1 个元素
    node->sz = sz;                                            // 数据字节大小
    node->container = QUICKLIST_NODE_CONTAINER_PLAIN;          // 标记为 PLAIN 节点

    _quicklistInsertNodeAfter(quicklist, quicklist->tail, node);
    quicklist->count += node->count;
}

/* 当节点元素数为 0 时，自动从 quicklist 中删除该节点，
 * 并将节点指针置空。 */
#define quicklistDeleteIfEmpty(ql, n)                                          \
    do {                                                                       \
        if ((n)->count == 0) {                                                 \
            __quicklistDelNode((ql), (n));                                     \
            (n) = NULL;                                                        \
        }                                                                      \
    } while (0)

/* 从 quicklist 中彻底删除一个节点，并维护链表结构与书签一致性。 */
REDIS_STATIC void __quicklistDelNode(quicklist *quicklist,
                                     quicklistNode *node) {
    /* Update the bookmark if any
     * 如有书签指向该节点，先更新书签 */
    quicklistBookmark *bm = _quicklistBookmarkFindByNode(quicklist, node);
    if (bm) {
        bm->node = node->next;
        /* if the bookmark was to the last node, delete it.
         * 如果书签原本指向最后一个节点，则删除该书签 */
        if (!bm->node)
            _quicklistBookmarkDelete(quicklist, bm);
    }

    if (node->next)
        node->next->prev = node->prev;   // 维护后继节点的前向指针
    if (node->prev)
        node->prev->next = node->next;   // 维护前驱节点的后向指针

    if (node == quicklist->tail) {
        quicklist->tail = node->prev;    // 更新尾节点
    }

    if (node == quicklist->head) {
        quicklist->head = node->next;    // 更新头节点
    }

    /* Update len first, so in __quicklistCompress we know exactly len
     * 先更新 len，以便 __quicklistCompress 能准确获知链表长度 */
    quicklist->len--;
    quicklist->count -= node->count;

    /* If we deleted a node within our compress depth, we
     * now have compressed nodes needing to be decompressed.
     * 如果在压缩深度内删除了一个节点，则可能需要重新调整压缩状态 */
    __quicklistCompress(quicklist, NULL);

    zfree(node->entry);   // 释放节点中的数据
    zfree(node);          // 释放节点结构体
}

/* Delete one entry from list given the node for the entry and a pointer
 * to the entry in the node.
 *
 * Note: quicklistDelIndex() *requires* uncompressed nodes because you
 *       already had to get *p from an uncompressed node somewhere.
 *
 * Returns 1 if the entire node was deleted, 0 if node still exists.
 * Also updates in/out param 'p' with the next offset in the listpack.
 *
 * 在指定 listpack 节点中删除某一条目（由 *p 定位）。
 * 注意：调用方必须保证节点是未压缩的（因为 *p 只能从未压缩节点获得）。
 *   - 整个节点被删除时返回 1；
 *   - 节点仍存在时返回 0。
 * 同时会通过 *p 输出下一条目的偏移。 */
REDIS_STATIC int quicklistDelIndex(quicklist *quicklist, quicklistNode *node,
                                   unsigned char **p) {
    int gone = 0;

    if (unlikely(QL_NODE_IS_PLAIN(node))) {
        // PLAIN 节点只能整体删除
        __quicklistDelNode(quicklist, node);
        return 1;
    }
    node->entry = lpDelete(node->entry, *p, p);  // 在 listpack 中删除条目
    node->count--;
    if (node->count == 0) {
        // 节点已空，删除整个节点
        gone = 1;
        __quicklistDelNode(quicklist, node);
    } else {
        quicklistNodeUpdateSz(node);             // 同步 sz
    }
    quicklist->count--;
    /* If we deleted the node, the original node is no longer valid
     * 如果节点已被删除，原始 node 指针已失效 */
    return gone ? 1 : 0;
}

/* Delete one element represented by 'entry'
 *
 * 'entry' stores enough metadata to delete the proper position in
 * the correct listpack in the correct quicklist node.
 *
 * 删除由 entry 表示的某一条目。
 * entry 保存了足够元数据，可定位到正确的 listpack 中的正确位置。 */
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry) {
    quicklistNode *prev = entry->node->prev;   // 当前节点的前驱
    quicklistNode *next = entry->node->next;   // 当前节点的后继
    int deleted_node = quicklistDelIndex((quicklist *)entry->quicklist,
                                         entry->node, &entry->zi);

    /* after delete, the zi is now invalid for any future usage.
     * 删除后，zi 已不再有效 */
    iter->zi = NULL;

    /* If current node is deleted, we must update iterator node and offset.
     * 如果当前节点被删除，需要更新迭代器的 node 与 offset。 */
    if (deleted_node) {
        if (iter->direction == AL_START_HEAD) {
            iter->current = next;
            iter->offset = 0;
        } else if (iter->direction == AL_START_TAIL) {
            iter->current = prev;
            iter->offset = -1;
        }
    }
    /* else if (!deleted_node), no changes needed.
     * we already reset iter->zi above, and the existing iter->offset
     * doesn't move again because:
     *   - [1, 2, 3] => delete offset 1 => [1, 3]: next element still offset 1
     *   - [1, 2, 3] => delete offset 0 => [2, 3]: next element still offset 0
     *  if we deleted the last element at offset N and now
     *  length of this listpack is N-1, the next call into
     *  quicklistNext() will jump to the next node.
     * 否则（节点未删除），无需更新：
     *   - 上方已将 iter->zi 置空；
     *   - 现有 iter->offset 仍指向正确位置（参考以上示例）；
     *   - 若删的是最后一个元素，下次调用 quicklistNext() 会自动跳到下一节点。 */
}

/* Replace quicklist entry by 'data' with length 'sz'.
 * 将 entry 指向的条目替换为 data/sz。 */
void quicklistReplaceEntry(quicklistIter *iter, quicklistEntry *entry,
                           void *data, size_t sz)
{
    quicklist* quicklist = iter->quicklist;
    quicklistNode *node = entry->node;
    unsigned char *newentry;

    if (likely(!QL_NODE_IS_PLAIN(entry->node) && !isLargeElement(sz, quicklist->fill) &&
        (newentry = lpReplace(entry->node->entry, &entry->zi, data, sz)) != NULL))
    {
        // 普通情况：直接在 listpack 内替换
        entry->node->entry = newentry;
        quicklistNodeUpdateSz(entry->node);
        /* quicklistNext() and quicklistGetIteratorEntryAtIdx() provide an uncompressed node
         * quicklistNext() 和 quicklistGetIteratorEntryAtIdx() 都保证传入未压缩节点 */
        quicklistCompress(quicklist, entry->node);
    } else if (QL_NODE_IS_PLAIN(entry->node)) {
        // 节点本身是 PLAIN 节点
        if (isLargeElement(sz, quicklist->fill)) {
            // 仍是大元素：原地替换数据
            zfree(entry->node->entry);
            entry->node->entry = zmalloc(sz);
            entry->node->sz = sz;
            memcpy(entry->node->entry, data, sz);
            quicklistCompress(quicklist, entry->node);
        } else {
            // 变成普通元素：在原 PLAIN 节点后插入新节点，再删除原 PLAIN 节点
            quicklistInsertAfter(iter, entry, data, sz);
            __quicklistDelNode(quicklist, entry->node);
        }
    } else { /* The node is full or data is a large element
              * 节点已满，或者新数据是大元素 */
        quicklistNode *split_node = NULL, *new_node;
        node->dont_compress = 1; /* Prevent compression in __quicklistInsertNode()
                                  * 防止 __quicklistInsertNode() 内部触发压缩 */

        /* If the entry is not at the tail, split the node at the entry's offset.
         * 如果要替换的不是尾位置，则在 entry 的偏移处对节点进行拆分。 */
        if (entry->offset != node->count - 1 && entry->offset != -1)
            split_node = _quicklistSplitNode(node, entry->offset, 1);

        /* Create a new node and insert it after the original node.
         * If the original node was split, insert the split node after the new node.
         * 创建新节点并插入到原节点之后；
         * 如果发生过拆分，则再将拆分出的节点插入到新节点之后。 */
        new_node = __quicklistCreateNode(isLargeElement(sz, quicklist->fill) ?
            QUICKLIST_NODE_CONTAINER_PLAIN : QUICKLIST_NODE_CONTAINER_PACKED, data, sz);
        __quicklistInsertNode(quicklist, node, new_node, 1);
        if (split_node) __quicklistInsertNode(quicklist, new_node, split_node, 1);
        quicklist->count++;

        /* Delete the replaced element.
         * 删除被替换的旧元素 */
        if (entry->node->count == 1) {
            // 节点只含 1 个元素，直接删除整个节点
            __quicklistDelNode(quicklist, entry->node);
        } else {
            unsigned char *p = lpSeek(entry->node->entry, -1);
            quicklistDelIndex(quicklist, entry->node, &p);
            entry->node->dont_compress = 0; /* Re-enable compression
                                              * 重新允许压缩 */
            new_node = _quicklistMergeNodes(quicklist, new_node);
            /* We can't know if the current node and its sibling nodes are correctly compressed,
             * and we don't know if they are within the range of compress depth, so we need to
             * use quicklistCompress() for compression, which checks if node is within compress
             * depth before compressing.
             * 由于无法确定当前节点及其兄弟节点是否已正确压缩，也不知它们
             * 是否在压缩深度范围内，因此需要使用 quicklistCompress()，
             * 它会在压缩前先检查节点是否处于压缩深度内。 */
            quicklistCompress(quicklist, new_node);
            quicklistCompress(quicklist, new_node->prev);
            if (new_node->next) quicklistCompress(quicklist, new_node->next);
        }
    }

    /* In any case, we reset iterator to forbid use of iterator after insert.
     * Notice: iter->current has been compressed above.
     * 不论哪种情况都重置迭代器，禁止在插入后继续使用。
     * 注意：iter->current 在上面已经被压缩。 */
    resetIterator(iter);
}

/* Replace quicklist entry at offset 'index' by 'data' with length 'sz'.
 *
 * Returns 1 if replace happened.
 * Returns 0 if replace failed and no changes happened.
 *
 * 将 quicklist 中下标为 index 的条目替换为 data/sz。
 *   - 替换成功返回 1；
 *   - 替换失败（未发生变更）返回 0。 */
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data,
                            size_t sz) {
    quicklistEntry entry;
    quicklistIter *iter = quicklistGetIteratorEntryAtIdx(quicklist, index, &entry);
    if (likely(iter)) {
        quicklistReplaceEntry(iter, &entry, data, sz);
        quicklistReleaseIterator(iter);
        return 1;
    } else {
        return 0;
    }
}

/* Given two nodes, try to merge their listpacks.
 *
 * This helps us not have a quicklist with 3 element listpacks if
 * our fill factor can handle much higher levels.
 *
 * Note: 'a' must be to the LEFT of 'b'.
 *
 * After calling this function, both 'a' and 'b' should be considered
 * unusable.  The return value from this function must be used
 * instead of re-using any of the quicklistNode input arguments.
 *
 * Returns the input node picked to merge against or NULL if
 * merging was not possible.
 *
 * 尝试合并两个相邻节点的 listpack。
 * 这样做可以避免出现仅含 3 个元素的 listpack，让 fill 因子发挥更大作用。
 * 注意：'a' 必须在 'b' 的左侧。
 * 调用此函数后，'a' 和 'b' 都视为不可用；必须使用本函数的返回值。
 *   - 返回被选中的保留节点；
 *   - 合并失败返回 NULL。 */
REDIS_STATIC quicklistNode *_quicklistListpackMerge(quicklist *quicklist,
                                                    quicklistNode *a,
                                                    quicklistNode *b) {
    D("Requested merge (a,b) (%u, %u)", a->count, b->count);

    quicklistDecompressNode(a);   // 确保 a 已解压
    quicklistDecompressNode(b);   // 确保 b 已解压
    if ((lpMerge(&a->entry, &b->entry))) {
        /* We merged listpacks! Now remove the unused quicklistNode.
         * 成功合并 listpack！现在删除不再需要的 quicklistNode。 */
        quicklistNode *keep = NULL, *nokeep = NULL;
        if (!a->entry) {
            nokeep = a;
            keep = b;
        } else if (!b->entry) {
            nokeep = b;
            keep = a;
        }
        keep->count = lpLength(keep->entry);
        quicklistNodeUpdateSz(keep);
        keep->recompress = 0; /* Prevent 'keep' from being recompressed if
                               * it becomes head or tail after merging.
                               * 防止 keep 在合并后成为头/尾时被重新压缩 */

        nokeep->count = 0;
        __quicklistDelNode(quicklist, nokeep);
        quicklistCompress(quicklist, keep);
        return keep;
    } else {
        /* else, the merge returned NULL and nothing changed.
         * 合并返回 NULL，状态未变。 */
        return NULL;
    }
}

/* Attempt to merge listpacks within two nodes on either side of 'center'.
 *
 * We attempt to merge:
 *   - (center->prev->prev, center->prev)
 *   - (center->next, center->next->next)
 *   - (center->prev, center)
 *   - (center, center->next)
 *
 * Returns the new 'center' after merging.
 *
 * 尝试在 center 节点及其两侧的相邻节点之间两两合并 listpack：
 *   - (center->prev->prev, center->prev)
 *   - (center->next, center->next->next)
 *   - (center->prev, center)
 *   - (center, center->next)
 * 返回合并后充当新 'center' 的节点。 */
REDIS_STATIC quicklistNode *_quicklistMergeNodes(quicklist *quicklist, quicklistNode *center) {
    int fill = quicklist->fill;
    quicklistNode *prev, *prev_prev, *next, *next_next, *target;
    prev = prev_prev = next = next_next = target = NULL;

    if (center->prev) {
        prev = center->prev;
        if (center->prev->prev)
            prev_prev = center->prev->prev;
    }

    if (center->next) {
        next = center->next;
        if (center->next->next)
            next_next = center->next->next;
    }

    /* Try to merge prev_prev and prev */
    if (_quicklistNodeAllowMerge(prev, prev_prev, fill)) {
        _quicklistListpackMerge(quicklist, prev_prev, prev);
        prev_prev = prev = NULL; /* they could have moved, invalidate them.
                                   * 它们可能已移动，置空以示失效。 */
    }

    /* Try to merge next and next_next */
    if (_quicklistNodeAllowMerge(next, next_next, fill)) {
        _quicklistListpackMerge(quicklist, next, next_next);
        next = next_next = NULL; /* they could have moved, invalidate them. */
    }

    /* Try to merge center node and previous node */
    if (_quicklistNodeAllowMerge(center, center->prev, fill)) {
        target = _quicklistListpackMerge(quicklist, center->prev, center);
        center = NULL; /* center could have been deleted, invalidate it.
                        * center 可能已被删除，将其置空。 */
    } else {
        /* else, we didn't merge here, but target needs to be valid below.
         * 此处未发生合并，但 target 在后续需要保持有效。 */
        target = center;
    }

    /* Use result of center merge (or original) to merge with next node.
     * 使用 center 合并后的结果（或 center 本身）与下一个节点再次尝试合并。 */
    if (_quicklistNodeAllowMerge(target, target->next, fill)) {
        target = _quicklistListpackMerge(quicklist, target, target->next);
    }
    return target;
}

/* Split 'node' into two parts, parameterized by 'offset' and 'after'.
 *
 * The 'after' argument controls which quicklistNode gets returned.
 * If 'after'==1, returned node has elements after 'offset'.
 *                input node keeps elements up to 'offset', including 'offset'.
 * If 'after'==0, returned node has elements up to 'offset'.
 *                input node keeps elements after 'offset', including 'offset'.
 *
 * Or in other words:
 * If 'after'==1, returned node will have elements after 'offset'.
 *                The returned node will have elements [OFFSET+1, END].
 *                The input node keeps elements [0, OFFSET].
 * If 'after'==0, returned node will keep elements up to but not including 'offset'.
 *                The returned node will have elements [0, OFFSET-1].
 *                The input node keeps elements [OFFSET, END].
 *
 * The input node keeps all elements not taken by the returned node.
 *
 * Returns newly created node or NULL if split not possible.
 *
 * 将 node 拆分为两个部分，由 offset 和 after 共同控制：
 *   after == 1：
 *     - 返回的节点包含 [OFFSET+1, END] 范围的元素；
 *     - 原节点保留 [0, OFFSET] 范围的元素。
 *   after == 0：
 *     - 返回的节点包含 [0, OFFSET-1] 范围的元素；
 *     - 原节点保留 [OFFSET, END] 范围的元素。
 * 原节点始终保留未被返回节点取走的元素。
 * 拆分成功返回新节点，否则返回 NULL。 */
REDIS_STATIC quicklistNode *_quicklistSplitNode(quicklistNode *node, int offset,
                                                int after) {
    size_t zl_sz = node->sz;

    quicklistNode *new_node = quicklistCreateNode();
    new_node->entry = zmalloc(zl_sz);

    /* Copy original listpack so we can split it
     * 复制原 listpack，以便后续分别从中删除不同区段 */
    memcpy(new_node->entry, node->entry, zl_sz);

    /* Need positive offset for calculating extent below. */
    if (offset < 0) offset = node->count + offset;

    /* Ranges to be trimmed: -1 here means "continue deleting until the list ends"
     * 待裁剪的范围：此处的 -1 表示 "一直删除到列表末尾" */
    int orig_start = after ? offset + 1 : 0;
    int orig_extent = after ? -1 : offset;
    int new_start = after ? 0 : offset;
    int new_extent = after ? offset + 1 : -1;

    D("After %d (%d); ranges: [%d, %d], [%d, %d]", after, offset, orig_start,
      orig_extent, new_start, new_extent);

    node->entry = lpDeleteRange(node->entry, orig_start, orig_extent);
    node->count = lpLength(node->entry);
    quicklistNodeUpdateSz(node);

    new_node->entry = lpDeleteRange(new_node->entry, new_start, new_extent);
    new_node->count = lpLength(new_node->entry);
    quicklistNodeUpdateSz(new_node);

    D("After split lengths: orig (%d), new (%d)", node->count, new_node->count);
    return new_node;
}

/* Insert a new entry before or after existing entry 'entry'.
 *
 * If after==1, the new value is inserted after 'entry', otherwise
 * the new value is inserted before 'entry'.
 *
 * 在 entry 指向的位置前后插入一个新条目。
 *   - after == 1：插入到 entry 之后；
 *   - after == 0：插入到 entry 之前。 */
REDIS_STATIC void _quicklistInsert(quicklistIter *iter, quicklistEntry *entry,
                                   void *value, const size_t sz, int after)
{
    quicklist *quicklist = iter->quicklist;
    int full = 0, at_tail = 0, at_head = 0, avail_next = 0, avail_prev = 0;
    int fill = quicklist->fill;
    quicklistNode *node = entry->node;
    quicklistNode *new_node = NULL;

    if (!node) {
        /* we have no reference node, so let's create only node in the list
         * 没有参考节点：作为列表中唯一的节点直接创建 */
        D("No node given!");
        if (unlikely(isLargeElement(sz, quicklist->fill))) {
            __quicklistInsertPlainNode(quicklist, quicklist->tail, value, sz, after);
            return;
        }
        new_node = quicklistCreateNode();
        new_node->entry = lpPrepend(lpNew(0), value, sz);
        __quicklistInsertNode(quicklist, NULL, new_node, after);
        new_node->count++;
        quicklist->count++;
        return;
    }

    /* Populate accounting flags for easier boolean checks later
     * 计算后续布尔判断所需的各项标记 */
    if (!_quicklistNodeAllowInsert(node, fill, sz)) {
        D("Current node is full with count %d with requested fill %d",
          node->count, fill);
        full = 1;
    }

    if (after && (entry->offset == node->count - 1 || entry->offset == -1)) {
        D("At Tail of current listpack");
        at_tail = 1;
        if (_quicklistNodeAllowInsert(node->next, fill, sz)) {
            D("Next node is available.");
            avail_next = 1;
        }
    }

    if (!after && (entry->offset == 0 || entry->offset == -(node->count))) {
        D("At Head");
        at_head = 1;
        if (_quicklistNodeAllowInsert(node->prev, fill, sz)) {
            D("Prev node is available.");
            avail_prev = 1;
        }
    }

    if (unlikely(isLargeElement(sz, quicklist->fill))) {
        if (QL_NODE_IS_PLAIN(node) || (at_tail && after) || (at_head && !after)) {
            __quicklistInsertPlainNode(quicklist, node, value, sz, after);
        } else {
            quicklistDecompressNodeForUse(node);
            new_node = _quicklistSplitNode(node, entry->offset, after);
            quicklistNode *entry_node = __quicklistCreateNode(QUICKLIST_NODE_CONTAINER_PLAIN, value, sz);
            __quicklistInsertNode(quicklist, node, entry_node, after);
            __quicklistInsertNode(quicklist, entry_node, new_node, after);
            quicklist->count++;
        }
        return;
    }

    /* Now determine where and how to insert the new element
     * 根据各种组合情况决定具体的插入位置和方式 */
    if (!full && after) {
        D("Not full, inserting after current position.");
        quicklistDecompressNodeForUse(node);
        node->entry = lpInsertString(node->entry, value, sz, entry->zi, LP_AFTER, NULL);
        node->count++;
        quicklistNodeUpdateSz(node);
        quicklistRecompressOnly(node);
    } else if (!full && !after) {
        D("Not full, inserting before current position.");
        quicklistDecompressNodeForUse(node);
        node->entry = lpInsertString(node->entry, value, sz, entry->zi, LP_BEFORE, NULL);
        node->count++;
        quicklistNodeUpdateSz(node);
        quicklistRecompressOnly(node);
    } else if (full && at_tail && avail_next && after) {
        /* If we are: at tail, next has free space, and inserting after:
         *   - insert entry at head of next node.
         * 处于"节点已满 + 在尾位置 + 下一节点有空 + 向后插入"：
         *   直接把新元素前插到下一节点。 */
        D("Full and tail, but next isn't full; inserting next node head");
        new_node = node->next;
        quicklistDecompressNodeForUse(new_node);
        new_node->entry = lpPrepend(new_node->entry, value, sz);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        quicklistRecompressOnly(new_node);
        quicklistRecompressOnly(node);
    } else if (full && at_head && avail_prev && !after) {
        /* If we are: at head, previous has free space, and inserting before:
         *   - insert entry at tail of previous node.
         * 处于"节点已满 + 在头位置 + 上一节点有空 + 向前插入"：
         *   直接把新元素追加到上一节点尾部。 */
        D("Full and head, but prev isn't full, inserting prev node tail");
        new_node = node->prev;
        quicklistDecompressNodeForUse(new_node);
        new_node->entry = lpAppend(new_node->entry, value, sz);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        quicklistRecompressOnly(new_node);
        quicklistRecompressOnly(node);
    } else if (full && ((at_tail && !avail_next && after) ||
                        (at_head && !avail_prev && !after))) {
        /* If we are: full, and our prev/next has no available space, then:
         *   - create new node and attach to quicklist
         * 节点已满且相邻节点也无空间：
         *   创建新节点并挂到当前节点一侧。 */
        D("\tprovisioning new node...");
        new_node = quicklistCreateNode();
        new_node->entry = lpPrepend(lpNew(0), value, sz);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        __quicklistInsertNode(quicklist, node, new_node, after);
    } else if (full) {
        /* else, node is full we need to split it. */
        /* covers both after and !after cases
         * 其他"已满"场景：拆分当前节点。
         * 该分支同时处理 after 与 !after 两种情况。 */
        D("\tsplitting node...");
        quicklistDecompressNodeForUse(node);
        new_node = _quicklistSplitNode(node, entry->offset, after);
        if (after)
            new_node->entry = lpPrepend(new_node->entry, value, sz);
        else
            new_node->entry = lpAppend(new_node->entry, value, sz);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        __quicklistInsertNode(quicklist, node, new_node, after);
        _quicklistMergeNodes(quicklist, node);
    }

    quicklist->count++;

    /* In any case, we reset iterator to forbid use of iterator after insert.
     * Notice: iter->current has been compressed in _quicklistInsert().
     * 不论哪种情况都重置迭代器，禁止在插入后继续使用。
     * 注意：iter->current 在 _quicklistInsert() 内部已经被压缩。 */
    resetIterator(iter);
}

void quicklistInsertBefore(quicklistIter *iter, quicklistEntry *entry,
                           void *value, const size_t sz)
{
    _quicklistInsert(iter, entry, value, sz, 0);
}

void quicklistInsertAfter(quicklistIter *iter, quicklistEntry *entry,
                          void *value, const size_t sz)
{
    _quicklistInsert(iter, entry, value, sz, 1);
}

/* Delete a range of elements from the quicklist.
 *
 * elements may span across multiple quicklistNodes, so we
 * have to be careful about tracking where we start and end.
 *
 * Returns 1 if entries were deleted, 0 if nothing was deleted.
 *
 * 删除 quicklist 中的一段元素。
 * 待删除元素可能跨越多个 quicklistNode，因此需要仔细跟踪起止位置。
 *   - 有元素被删除返回 1；
 *   - 没有元素被删除返回 0。 */
int quicklistDelRange(quicklist *quicklist, const long start,
                      const long count) {
    if (count <= 0)
        return 0;

    unsigned long extent = count; /* range is inclusive of start position
                                   * 区间包含 start 位置 */

    if (start >= 0 && extent > (quicklist->count - start)) {
        /* if requesting delete more elements than exist, limit to list size.
         * 若要删除的元素超过列表实际剩余数量，将其限制为列表大小。 */
        extent = quicklist->count - start;
    } else if (start < 0 && extent > (unsigned long)(-start)) {
        /* else, if at negative offset, limit max size to rest of list.
         * 若 start 为负，将最大删除数限制为列表剩余长度。 */
        extent = -start; /* c.f. LREM -29 29; just delete until end.
                          * 参考 LREM -29 29 的语义，一直删到列表末尾。 */
    }

    quicklistIter *iter = quicklistGetIteratorAtIdx(quicklist, AL_START_TAIL, start);
    if (!iter)
        return 0;

    D("Quicklist delete request for start %ld, count %ld, extent: %ld", start,
      count, extent);
    quicklistNode *node = iter->current;
    long offset = iter->offset;
    quicklistReleaseIterator(iter);

    /* iterate over next nodes until everything is deleted.
     * 不断迭代相邻节点，直至全部删除完成。 */
    while (extent) {
        quicklistNode *next = node->next;

        unsigned long del;
        int delete_entire_node = 0;
        if (offset == 0 && extent >= node->count) {
            /* If we are deleting more than the count of this node, we
             * can just delete the entire node without listpack math.
             * 删除数量超过当前节点的元素数，无需做 listpack 层面的计算。 */
            delete_entire_node = 1;
            del = node->count;
        } else if (offset >= 0 && extent + offset >= node->count) {
            /* If deleting more nodes after this one, calculate delete based
             * on size of current node.
             * 若删除范围延伸到本节点之后，按本节点剩余元素数计算删除数。 */
            del = node->count - offset;
        } else if (offset < 0) {
            /* If offset is negative, we are in the first run of this loop
             * and we are deleting the entire range
             * from this start offset to end of list.  Since the Negative
             * offset is the number of elements until the tail of the list,
             * just use it directly as the deletion count.
             * offset 为负表示这是循环的第一次迭代，
             * 正在删除从起始偏移到列表末尾的整段范围。
             * 负偏移的相反数即为到列表末尾的元素数，可直接作为删除数。 */
            del = -offset;

            /* If the positive offset is greater than the remaining extent,
             * we only delete the remaining extent, not the entire offset.
             * 若正向偏移大于剩余 extent，则只删除剩余 extent，而不是全部。 */
            if (del > extent)
                del = extent;
        } else {
            /* else, we are deleting less than the extent of this node, so
             * use extent directly.
             * 其他情况：删除数量小于本节点剩余 extent，直接使用 extent。 */
            del = extent;
        }

        D("[%ld]: asking to del: %ld because offset: %d; (ENTIRE NODE: %d), "
          "node count: %u",
          extent, del, offset, delete_entire_node, node->count);

        if (delete_entire_node || QL_NODE_IS_PLAIN(node)) {
            __quicklistDelNode(quicklist, node);   // 直接删除整个节点
        } else {
            quicklistDecompressNodeForUse(node);
            node->entry = lpDeleteRange(node->entry, offset, del);
            quicklistNodeUpdateSz(node);
            node->count -= del;
            quicklist->count -= del;
            quicklistDeleteIfEmpty(quicklist, node);
            if (node)
                quicklistRecompressOnly(node);
        }

        extent -= del;

        node = next;

        offset = 0;
    }
    return 1;
}

/* compare between a two entries
 * 比较 quicklistEntry 与给定的字节序列 p2/p2_len。 */
int quicklistCompare(quicklistEntry* entry, unsigned char *p2, const size_t p2_len) {
    if (unlikely(QL_NODE_IS_PLAIN(entry->node))) {
        return ((entry->sz == p2_len) && (memcmp(entry->value, p2, p2_len) == 0));
    }
    return lpCompare(entry->zi, p2, p2_len);
}

/* Returns a quicklist iterator 'iter'. After the initialization every
 * call to quicklistNext() will return the next element of the quicklist.
 *
 * 返回一个 quicklist 迭代器 iter。
 * 初始化后，每次调用 quicklistNext() 都会返回 quicklist 中的下一个元素。 */
quicklistIter *quicklistGetIterator(quicklist *quicklist, int direction) {
    quicklistIter *iter;

    iter = zmalloc(sizeof(*iter));

    if (direction == AL_START_HEAD) {
        iter->current = quicklist->head;
        iter->offset = 0;
    } else if (direction == AL_START_TAIL) {
        iter->current = quicklist->tail;
        iter->offset = -1;
    }

    iter->direction = direction;       // 记录迭代方向
    iter->quicklist = quicklist;       // 记录所属 quicklist

    iter->zi = NULL;                   // 当前位置指针置空

    return iter;
}

/* Initialize an iterator at a specific offset 'idx' and make the iterator
 * return nodes in 'direction' direction.
 *
 * 在指定偏移 idx 处初始化一个迭代器，并让该迭代器按 direction 方向遍历。 */
quicklistIter *quicklistGetIteratorAtIdx(quicklist *quicklist,
                                         const int direction,
                                         const long long idx)
{
    quicklistNode *n;
    unsigned long long accum = 0;
    unsigned long long index;
    int forward = idx < 0 ? 0 : 1; /* < 0 -> reverse, 0+ -> forward
                                    * 负数表示反向，非负数表示正向 */

    index = forward ? idx : (-idx) - 1;
    if (index >= quicklist->count)
        return NULL;

    /* Seek in the other direction if that way is shorter.
     * 若反方向更短，则改从反方向进行查找。 */
    int seek_forward = forward;
    unsigned long long seek_index = index;
    if (index > (quicklist->count - 1) / 2) {
        seek_forward = !forward;
        seek_index = quicklist->count - 1 - index;
    }

    n = seek_forward ? quicklist->head : quicklist->tail;
    while (likely(n)) {
        if ((accum + n->count) > seek_index) {
            break;  // 找到目标节点
        } else {
            D("Skipping over (%p) %u at accum %lld", (void *)n, n->count,
              accum);
            accum += n->count;
            n = seek_forward ? n->next : n->prev;
        }
    }

    if (!n)
        return NULL;

    /* Fix accum so it looks like we seeked in the other direction.
     * 修正 accum，使其看起来是从另一方向查找到的。 */
    if (seek_forward != forward) accum = quicklist->count - n->count - accum;

    D("Found node: %p at accum %llu, idx %llu, sub+ %llu, sub- %llu", (void *)n,
      accum, index, index - accum, (-index) - 1 + accum);

    quicklistIter *iter = quicklistGetIterator(quicklist, direction);
    iter->current = n;
    if (forward) {
        /* forward = normal head-to-tail offset.
         * 正向：从头到尾的标准偏移。 */
        iter->offset = index - accum;
    } else {
        /* reverse = need negative offset for tail-to-head, so undo
         * the result of the original index = (-idx) - 1 above.
         * 反向：从尾到头需要负偏移，因此撤销之前 index = (-idx) - 1 的处理。 */
        iter->offset = (-index) - 1 + accum;
    }

    return iter;
}

/* Release iterator.
 * If we still have a valid current node, then re-encode current node.
 *
 * 释放迭代器。
 * 若迭代器仍指向一个有效节点，则重新压缩该节点。 */
void quicklistReleaseIterator(quicklistIter *iter) {
    if (!iter) return;
    if (iter->current)
        quicklistCompress(iter->quicklist, iter->current);

    zfree(iter);
}

/* Get next element in iterator.
 *
 * Note: You must NOT insert into the list while iterating over it.
 * You *may* delete from the list while iterating using the
 * quicklistDelEntry() function.
 * If you insert into the quicklist while iterating, you should
 * re-create the iterator after your addition.
 *
 * iter = quicklistGetIterator(quicklist,<direction>);
 * quicklistEntry entry;
 * while (quicklistNext(iter, &entry)) {
 *     if (entry.value)
 *          [[ use entry.value with entry.sz ]]
 *     else
 *          [[ use entry.longval ]]
 * }
 *
 * Populates 'entry' with values for this iteration.
 * Returns 0 when iteration is complete or if iteration not possible.
 * If return value is 0, the contents of 'entry' are not valid.
 *
 * 获取迭代器的下一个元素。
 * 注意：迭代过程中不得插入新元素（否则需重建迭代器）；
 *       可以使用 quicklistDelEntry() 在迭代中删除元素。
 * 每次调用会用当前元素的数据填充 entry。
 *   - 迭代完成或不可继续时返回 0（此时 entry 内容无效）；
 *   - 还有元素可读时返回 1。 */
int quicklistNext(quicklistIter *iter, quicklistEntry *entry) {
    initEntry(entry);

    if (!iter) {
        D("Returning because no iter!");
        return 0;
    }

    entry->quicklist = iter->quicklist;
    entry->node = iter->current;

    if (!iter->current) {
        D("Returning because current node is NULL");
        return 0;
    }

    unsigned char *(*nextFn)(unsigned char *, unsigned char *) = NULL;
    int offset_update = 0;

    int plain = QL_NODE_IS_PLAIN(iter->current);
    if (!iter->zi) {
        /* If !zi, use current index.
         * 若 zi 为空，使用当前 index 重新定位。 */
        quicklistDecompressNodeForUse(iter->current);
        if (unlikely(plain))
            iter->zi = iter->current->entry;
        else
            iter->zi = lpSeek(iter->current->entry, iter->offset);
    } else if (unlikely(plain)) {
        iter->zi = NULL;
    } else {
        /* else, use existing iterator offset and get prev/next as necessary.
         * 否则：根据迭代方向获取前一个/后一个元素。 */
        if (iter->direction == AL_START_HEAD) {
            nextFn = lpNext;
            offset_update = 1;
        } else if (iter->direction == AL_START_TAIL) {
            nextFn = lpPrev;
            offset_update = -1;
        }
        iter->zi = nextFn(iter->current->entry, iter->zi);
        iter->offset += offset_update;
    }

    entry->zi = iter->zi;
    entry->offset = iter->offset;

    if (iter->zi) {
        if (unlikely(plain)) {
            // PLAIN 节点：直接用整个 entry 作为值
            entry->value = entry->node->entry;
            entry->sz = entry->node->sz;
            return 1;
        }
        /* Populate value from existing listpack position
         * 通过 listpack 当前指针位置填充 entry */
        unsigned int sz = 0;
        entry->value = lpGetValue(entry->zi, &sz, &entry->longval);
        entry->sz = sz;
        return 1;
    } else {
        /* We ran out of listpack entries.
         * Pick next node, update offset, then re-run retrieval.
         * 当前 listpack 已遍历完。
         * 切换到下一节点，更新 offset 后再次尝试取值。 */
        quicklistCompress(iter->quicklist, iter->current);
        if (iter->direction == AL_START_HEAD) {
            /* Forward traversal */
            D("Jumping to start of next node");
            iter->current = iter->current->next;
            iter->offset = 0;
        } else if (iter->direction == AL_START_TAIL) {
            /* Reverse traversal */
            D("Jumping to end of previous node");
            iter->current = iter->current->prev;
            iter->offset = -1;
        }
        iter->zi = NULL;
        return quicklistNext(iter, entry);   // 递归取值
    }
}

/* Sets the direction of a quicklist iterator.
 * 重新设置 quicklist 迭代器的方向。 */
void quicklistSetDirection(quicklistIter *iter, int direction) {
    iter->direction = direction;
}

/* Duplicate the quicklist.
 * On success a copy of the original quicklist is returned.
 *
 * The original quicklist both on success or error is never modified.
 *
 * Returns newly allocated quicklist.
 *
 * 复制一个 quicklist。
 * 成功时返回原 quicklist 的一个副本。
 * 原 quicklist 无论成功或出错都不会被修改。
 * 返回新分配的 quicklist。 */
quicklist *quicklistDup(quicklist *orig) {
    quicklist *copy;

    copy = quicklistNew(orig->fill, orig->compress);   // 创建新 quicklist

    for (quicklistNode *current = orig->head; current;   // 遍历原链表
         current = current->next) {
        quicklistNode *node = quicklistCreateNode();

        if (current->encoding == QUICKLIST_NODE_ENCODING_LZF) {
            // 复制压缩节点：连同 LZF 头一并复制
            quicklistLZF *lzf = (quicklistLZF *)current->entry;
            size_t lzf_sz = sizeof(*lzf) + lzf->sz;
            node->entry = zmalloc(lzf_sz);
            memcpy(node->entry, current->entry, lzf_sz);
        } else if (current->encoding == QUICKLIST_NODE_ENCODING_RAW) {
            // 复制未压缩节点
            node->entry = zmalloc(current->sz);
            memcpy(node->entry, current->entry, current->sz);
        }

        node->count = current->count;            // 复制元素数
        copy->count += node->count;               // 累加到新 quicklist
        node->sz = current->sz;                   // 复制 sz
        node->encoding = current->encoding;       // 复制 encoding
        node->container = current->container;     // 复制 container

        _quicklistInsertNodeAfter(copy, copy->tail, node);   // 挂到新链表尾部
    }

    /* copy->count must equal orig->count here
     * 此处 copy->count 应等于 orig->count */
    return copy;
}

/* Populate 'entry' with the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range 0 is returned.
 *
 * Returns an iterator at a specific offset 'idx' if element found
 * Returns NULL if element not found
 *
 * 用指定下标处的元素填充 entry。
 * 下标从 0 开始：0 表示头，1 表示头之后，依次类推。
 * 负数从尾部计数：-1 表示最后一个元素，-2 表示倒数第二个，依次类推。
 * 若下标越界则返回 0。
 *   - 找到元素时返回一个指向该位置的迭代器；
 *   - 未找到时返回 NULL。 */
quicklistIter *quicklistGetIteratorEntryAtIdx(quicklist *quicklist, const long long idx,
                                              quicklistEntry *entry)
{
    quicklistIter *iter = quicklistGetIteratorAtIdx(quicklist, AL_START_TAIL, idx);
    if (!iter) return NULL;
    assert(quicklistNext(iter, entry));
    return iter;
}

/* 当尾节点为 PLAIN 节点时，将尾节点旋转为头节点。 */
static void quicklistRotatePlain(quicklist *quicklist) {
    quicklistNode *new_head = quicklist->tail;        // 新头 = 原尾
    quicklistNode *new_tail = quicklist->tail->prev;  // 新尾 = 原尾的前驱
    quicklist->head->prev = new_head;                 // 原头前驱指向新头
    new_tail->next = NULL;                            // 新尾的后继置空
    new_head->next = quicklist->head;                 // 新头后继指向原头
    new_head->prev = NULL;                            // 新头前驱置空
    quicklist->head = new_head;                       // 更新头指针
    quicklist->tail = new_tail;                       // 更新尾指针
}

/* Rotate quicklist by moving the tail element to the head.
 * 旋转 quicklist：将尾元素移动到头部。 */
void quicklistRotate(quicklist *quicklist) {
    if (quicklist->count <= 1)
        return;  // 元素数过少无需旋转

    if (unlikely(QL_NODE_IS_PLAIN(quicklist->tail))) {
        quicklistRotatePlain(quicklist);
        return;
    }

    /* First, get the tail entry
     * 首先获取尾条目 */
    unsigned char *p = lpSeek(quicklist->tail->entry, -1);
    unsigned char *value, *tmp;
    long long longval;
    unsigned int sz;
    char longstr[32] = {0};
    tmp = lpGetValue(p, &sz, &longval);

    /* If value found is NULL, then lpGet populated longval instead
     * 若返回的 value 为 NULL，说明 lpGet 已填充 longval */
    if (!tmp) {
        /* Write the longval as a string so we can re-add it
         * 将 longval 转为字符串，以便重新加入 */
        sz = ll2string(longstr, sizeof(longstr), longval);
        value = (unsigned char *)longstr;
    } else if (quicklist->len == 1) {
        /* Copy buffer since there could be a memory overlap when move
         * entity from tail to head in the same listpack.
         * 由于在同一个 listpack 中从尾移到头时可能存在内存重叠，
         * 因此需要复制一份 buffer。 */
        value = zmalloc(sz);
        memcpy(value, tmp, sz);
    } else {
        value = tmp;
    }

    /* Add tail entry to head (must happen before tail is deleted).
     * 将尾条目添加到头部（必须在删除尾条目之前完成）。 */
    quicklistPushHead(quicklist, value, sz);

    /* If quicklist has only one node, the head listpack is also the
     * tail listpack and PushHead() could have reallocated our single listpack,
     * which would make our pre-existing 'p' unusable.
     * 若 quicklist 仅有一个节点，那么头 listpack 同时也是尾 listpack，
     * PushHead() 可能已经重新分配了这个 listpack，导致原 'p' 失效。 */
    if (quicklist->len == 1) {
        p = lpSeek(quicklist->tail->entry, -1);
    }

    /* Remove tail entry.
     * 删除尾条目。 */
    quicklistDelIndex(quicklist, quicklist->tail, &p);
    if (value != (unsigned char*)longstr && value != tmp)
        zfree(value);
}

/* pop from quicklist and return result in 'data' ptr.  Value of 'data'
 * is the return value of 'saver' function pointer if the data is NOT a number.
 *
 * If the quicklist element is a long long, then the return value is returned in
 * 'sval'.
 *
 * Return value of 0 means no elements available.
 * Return value of 1 means check 'data' and 'sval' for values.
 * If 'data' is set, use 'data' and 'sz'.  Otherwise, use 'sval'.
 *
 * 从 quicklist 中弹出一个元素并通过 'data' 指针返回结果。
 * 若该元素不是数字，则 'data' 指向 'saver' 回调函数的返回值。
 * 若该元素是 long long，则其值由 'sval' 返回。
 *   - 返回 0 表示无元素；
 *   - 返回 1 表示请检查 'data' 和 'sval'：
 *     若 'data' 非空，使用 'data' 和 'sz'；否则使用 'sval'。 */
int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data,
                       size_t *sz, long long *sval,
                       void *(*saver)(unsigned char *data, size_t sz)) {
    unsigned char *p;
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    int pos = (where == QUICKLIST_HEAD) ? 0 : -1;

    if (quicklist->count == 0)
        return 0;

    if (data)
        *data = NULL;                  // 初始化返回值
    if (sz)
        *sz = 0;
    if (sval)
        *sval = -123456789;            // 使用魔数表示未设置

    quicklistNode *node;
    if (where == QUICKLIST_HEAD && quicklist->head) {
        node = quicklist->head;        // 弹出头部
    } else if (where == QUICKLIST_TAIL && quicklist->tail) {
        node = quicklist->tail;        // 弹出尾部
    } else {
        return 0;
    }

    /* The head and tail should never be compressed
     * 头尾节点永远不应该被压缩 */
    assert(node->encoding != QUICKLIST_NODE_ENCODING_LZF);

    if (unlikely(QL_NODE_IS_PLAIN(node))) {
        // PLAIN 节点：直接取出整个 entry
        if (data)
            *data = saver(node->entry, node->sz);
        if (sz)
            *sz = node->sz;
        quicklistDelIndex(quicklist, node, NULL);
        return 1;
    }

    p = lpSeek(node->entry, pos);
    vstr = lpGetValue(p, &vlen, &vlong);
    if (vstr) {
        // 字符串元素
        if (data)
            *data = saver(vstr, vlen);
        if (sz)
            *sz = vlen;
    } else {
        // 整型元素
        if (data)
            *data = NULL;
        if (sval)
            *sval = vlong;
    }
    quicklistDelIndex(quicklist, node, &p);
    return 1;
}

/* Return a malloc'd copy of data passed in
 * 返回入参 data 的一个 malloc 副本。 */
REDIS_STATIC void *_quicklistSaver(unsigned char *data, size_t sz) {
    unsigned char *vstr;
    if (data) {
        vstr = zmalloc(sz);
        memcpy(vstr, data, sz);
        return vstr;
    }
    return NULL;
}

/* Default pop function
 *
 * Returns malloc'd value from quicklist
 *
 * 默认的弹出函数。
 * 从 quicklist 弹出一个由 malloc 分配的值。 */
int quicklistPop(quicklist *quicklist, int where, unsigned char **data,
                 size_t *sz, long long *slong) {
    unsigned char *vstr = NULL;
    size_t vlen = 0;
    long long vlong = 0;
    if (quicklist->count == 0)
        return 0;
    int ret = quicklistPopCustom(quicklist, where, &vstr, &vlen, &vlong,
                                 _quicklistSaver);
    if (data)
        *data = vstr;                  // 输出字符串值
    if (slong)
        *slong = vlong;                // 输出长整型值
    if (sz)
        *sz = vlen;                    // 输出字符串长度
    return ret;
}

/* Wrapper to allow argument-based switching between HEAD/TAIL pop
 * 封装函数：根据参数在头部/尾部之间切换 push 行为。 */
void quicklistPush(quicklist *quicklist, void *value, const size_t sz,
                   int where) {
    /* The head and tail should never be compressed (we don't attempt to decompress them)
     * 头尾节点永远不应被压缩（我们不会尝试解压它们） */
    if (quicklist->head)
        assert(quicklist->head->encoding != QUICKLIST_NODE_ENCODING_LZF);
    if (quicklist->tail)
        assert(quicklist->tail->encoding != QUICKLIST_NODE_ENCODING_LZF);

    if (where == QUICKLIST_HEAD) {
        quicklistPushHead(quicklist, value, sz);
    } else if (where == QUICKLIST_TAIL) {
        quicklistPushTail(quicklist, value, sz);
    }
}

/* Print info of quicklist which is used in debugCommand.
 * 打印 quicklist 的调试信息，供 debugCommand 使用。 */
void quicklistRepr(unsigned char *ql, int full) {
    int i = 0;
    quicklist *quicklist  = (struct quicklist*) ql;
    printf("{count : %ld}\n", quicklist->count);
    printf("{len : %ld}\n", quicklist->len);
    printf("{fill : %d}\n", quicklist->fill);
    printf("{compress : %d}\n", quicklist->compress);
    printf("{bookmark_count : %d}\n", quicklist->bookmark_count);
    quicklistNode* node = quicklist->head;

    while(node != NULL) {
        printf("{quicklist node(%d)\n", i++);
        printf("{container : %s, encoding: %s, size: %zu, count: %d, recompress: %d, attempted_compress: %d}\n",
               QL_NODE_IS_PLAIN(node) ? "PLAIN": "PACKED",
               (node->encoding == QUICKLIST_NODE_ENCODING_RAW) ? "RAW": "LZF",
               node->sz,
               node->count,
               node->recompress,
               node->attempted_compress);

        if (full) {
            quicklistDecompressNode(node);
            if (node->container == QUICKLIST_NODE_CONTAINER_PACKED) {
                printf("{ listpack:\n");
                lpRepr(node->entry);
                printf("}\n");

            } else if (QL_NODE_IS_PLAIN(node)) {
                printf("{ entry : %s }\n", node->entry);
            }
            printf("}\n");
            quicklistRecompressOnly(node);
        }
        node = node->next;
    }
}

/* Create or update a bookmark in the list which will be updated to the next node
 * automatically when the one referenced gets deleted.
 * Returns 1 on success (creation of new bookmark or override of an existing one).
 * Returns 0 on failure (reached the maximum supported number of bookmarks).
 * NOTE: use short simple names, so that string compare on find is quick.
 * NOTE: bookmark creation may re-allocate the quicklist, so the input pointer
         may change and it's the caller responsibility to update the reference.
 *
 * 创建或更新一个书签。
 * 当书签指向的节点被删除时，书签会自动更新到下一个节点。
 *   - 成功（创建新书签或覆盖已有书签）返回 1；
 *   - 失败（书签数量已达上限）返回 0。
 * 注意：建议使用简短的名字，便于在查找时快速比较。
 * 注意：书签创建过程中可能对 quicklist 进行 realloc，传入的指针可能改变，
 *       调用方需自行更新引用。 */
int quicklistBookmarkCreate(quicklist **ql_ref, const char *name, quicklistNode *node) {
    quicklist *ql = *ql_ref;
    if (ql->bookmark_count >= QL_MAX_BM)
        return 0;                                          // 达到书签数量上限
    quicklistBookmark *bm = _quicklistBookmarkFindByName(ql, name);
    if (bm) {
        bm->node = node;                                   // 覆盖已有书签
        return 1;
    }
    // 重新分配空间以容纳新的书签
    ql = zrealloc(ql, sizeof(quicklist) + (ql->bookmark_count+1) * sizeof(quicklistBookmark));
    *ql_ref = ql;
    ql->bookmarks[ql->bookmark_count].node = node;
    ql->bookmarks[ql->bookmark_count].name = zstrdup(name);
    ql->bookmark_count++;
    return 1;
}

/* Find the quicklist node referenced by a named bookmark.
 * When the bookmarked node is deleted the bookmark is updated to the next node,
 * and if that's the last node, the bookmark is deleted (so find returns NULL).
 *
 * 查找命名书签所引用的 quicklist 节点。
 * 当书签指向的节点被删除时，书签会自动更新到下一个节点；
 * 若已无下一节点，则书签被删除（find 返回 NULL）。 */
quicklistNode *quicklistBookmarkFind(quicklist *ql, const char *name) {
    quicklistBookmark *bm = _quicklistBookmarkFindByName(ql, name);
    if (!bm) return NULL;
    return bm->node;
}

/* Delete a named bookmark.
 * returns 0 if bookmark was not found, and 1 if deleted.
 * Note that the bookmark memory is not freed yet, and is kept for future use.
 *
 * 删除一个命名书签。
 *   - 未找到时返回 0；
 *   - 已删除时返回 1。
 * 注意：被删除书签的内存尚未真正释放，保留供将来复用。 */
int quicklistBookmarkDelete(quicklist *ql, const char *name) {
    quicklistBookmark *bm = _quicklistBookmarkFindByName(ql, name);
    if (!bm)
        return 0;
    _quicklistBookmarkDelete(ql, bm);
    return 1;
}

/* 按名称查找书签：线性扫描书签数组；未找到返回 NULL。 */
quicklistBookmark *_quicklistBookmarkFindByName(quicklist *ql, const char *name) {
    unsigned i;
    for (i=0; i<ql->bookmark_count; i++) {
        if (!strcmp(ql->bookmarks[i].name, name)) {
            return &ql->bookmarks[i];
        }
    }
    return NULL;
}

/* 按节点查找书签：线性扫描书签数组；未找到返回 NULL。 */
quicklistBookmark *_quicklistBookmarkFindByNode(quicklist *ql, quicklistNode *node) {
    unsigned i;
    for (i=0; i<ql->bookmark_count; i++) {
        if (ql->bookmarks[i].node == node) {
            return &ql->bookmarks[i];
        }
    }
    return NULL;
}

/* 实际删除一个书签：释放名称、压缩数组，并更新 bookmark_count。 */
void _quicklistBookmarkDelete(quicklist *ql, quicklistBookmark *bm) {
    int index = bm - ql->bookmarks;
    zfree(bm->name);
    ql->bookmark_count--;
    memmove(bm, bm+1, (ql->bookmark_count - index)* sizeof(*bm));
    /* NOTE: We do not shrink (realloc) the quicklist yet (to avoid resonance,
     * it may be re-used later (a call to realloc may NOP).
     * 注意：此处暂不通过 realloc 收缩 quicklist（避免与后续 realloc 抖动），
     * 该内存可在将来复用（realloc 可能为 NOP）。 */
}

/* 清除 quicklist 中的所有书签：仅释放各书签的 name 字符串。 */
void quicklistBookmarksClear(quicklist *ql) {
    while (ql->bookmark_count)
        zfree(ql->bookmarks[--ql->bookmark_count].name);
    /* NOTE: We do not shrink (realloc) the quick list. main use case for this
     * function is just before releasing the allocation.
     * 注意：此处不通过 realloc 收缩 quicklist。
     * 该函数的主要用途是在释放 quicklist 之前清理书签。 */
}

/* The rest of this file is test cases and test helpers.
 * 文件余下部分为测试用例和测试辅助函数。 */
#ifdef REDIS_TEST
#include <stdint.h>
#include <sys/time.h>
#include "testhelp.h"
#include <stdlib.h>

/* 在测试中输出一条错误消息（带格式） */
#define yell(str, ...) printf("ERROR! " str "\n\n", __VA_ARGS__)

/* 通用错误计数器宏：自增 err */
#define ERROR                                                                  \
    do {                                                                       \
        printf("\tERROR!\n");                                                  \
        err++;                                                                 \
    } while (0)

/* 输出文件/函数/行号 + 错误消息，并自增 err */
#define ERR(x, ...)                                                            \
    do {                                                                       \
        printf("%s:%s:%d:\t", __FILE__, __func__, __LINE__);                   \
        printf("ERROR! " x "\n", __VA_ARGS__);                                 \
        err++;                                                                 \
    } while (0)

/* 打印一个测试用例的名称 */
#define TEST(name) printf("test — %s\n", name);
/* 打印一个带格式化参数的测试用例名称 */
#define TEST_DESC(name, ...) printf("test — " name "\n", __VA_ARGS__);

/* 详细日志开关（默认关闭） */
#define QL_TEST_VERBOSE 0

/* 抑制未使用参数警告 */
#define UNUSED(x) (void)(x)
/* 打印 quicklist 调试信息（仅在 QL_TEST_VERBOSE=1 时真正输出） */
static void ql_info(quicklist *ql) {
#if QL_TEST_VERBOSE
    printf("Container length: %lu\n", ql->len);
    printf("Container size: %lu\n", ql->count);
    if (ql->head)
        printf("\t(zsize head: %lu)\n", lpLength(ql->head->entry));
    if (ql->tail)
        printf("\t(zsize tail: %lu)\n", lpLength(ql->tail->entry));
    printf("\n");
#else
    UNUSED(ql);
#endif
}

/* Return the UNIX time in microseconds
 * 返回以微秒为单位的 UNIX 时间。 */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec) * 1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds
 * 返回以毫秒为单位的 UNIX 时间。 */
static long long mstime(void) { return ustime() / 1000; }

/* Iterate over an entire quicklist.
 * Print the list if 'print' == 1.
 *
 * Returns physical count of elements found by iterating over the list.
 *
 * 遍历整个 quicklist。
 * 当 print==1 时打印列表内容。
 * 返回通过遍历实际得到的元素数量。 */
static int _itrprintr(quicklist *ql, int print, int forward) {
    quicklistIter *iter =
        quicklistGetIterator(ql, forward ? AL_START_HEAD : AL_START_TAIL);
    quicklistEntry entry;
    int i = 0;
    int p = 0;
    quicklistNode *prev = NULL;
    while (quicklistNext(iter, &entry)) {
        if (entry.node != prev) {
            /* Count the number of list nodes too
             * 顺便统计 list 节点数 */
            p++;
            prev = entry.node;
        }
        if (print) {
            int size = (entry.sz > (1<<20)) ? 1<<20 : entry.sz;
            printf("[%3d (%2d)]: [%.*s] (%lld)\n", i, p, size,
                   (char *)entry.value, entry.longval);
        }
        i++;
    }
    quicklistReleaseIterator(iter);
    return i;
}
/* 正向遍历 quicklist 的便捷封装 */
static int itrprintr(quicklist *ql, int print) {
    return _itrprintr(ql, print, 1);
}

/* 反向遍历 quicklist 的便捷封装 */
static int itrprintr_rev(quicklist *ql, int print) {
    return _itrprintr(ql, print, 0);
}

/* 校验宏：累加 _ql_verify 返回的错误数到 err */
#define ql_verify(a, b, c, d, e)                                               \
    do {                                                                       \
        err += _ql_verify((a), (b), (c), (d), (e));                            \
    } while (0)

/* 验证压缩规则：两端不压缩、中间压缩。
 * 若发现节点编码状态不符合预期，则输出 yell 并累加 errors。 */
static int _ql_verify_compress(quicklist *ql) {
    int errors = 0;
    if (quicklistAllowsCompression(ql)) {
        quicklistNode *node = ql->head;
        unsigned int low_raw = ql->compress;
        unsigned int high_raw = ql->len - ql->compress;

        for (unsigned int at = 0; at < ql->len; at++, node = node->next) {
            if (node && (at < low_raw || at >= high_raw)) {
                if (node->encoding != QUICKLIST_NODE_ENCODING_RAW) {
                    yell("Incorrect compression: node %d is "
                         "compressed at depth %d ((%u, %u); total "
                         "nodes: %lu; size: %zu; recompress: %d)",
                         at, ql->compress, low_raw, high_raw, ql->len, node->sz,
                         node->recompress);
                    errors++;
                }
            } else {
                if (node->encoding != QUICKLIST_NODE_ENCODING_LZF &&
                    !node->attempted_compress) {
                    yell("Incorrect non-compression: node %d is NOT "
                         "compressed at depth %d ((%u, %u); total "
                         "nodes: %lu; size: %zu; recompress: %d; attempted: %d)",
                         at, ql->compress, low_raw, high_raw, ql->len, node->sz,
                         node->recompress, node->attempted_compress);
                    errors++;
                }
            }
        }
    }
    return errors;
}

/* Verify list metadata matches physical list contents.
 * 验证 quicklist 的元数据是否与实际内容一致。 */
static int _ql_verify(quicklist *ql, uint32_t len, uint32_t count,
                      uint32_t head_count, uint32_t tail_count) {
    int errors = 0;

    ql_info(ql);
    if (len != ql->len) {
        yell("quicklist length wrong: expected %d, got %lu", len, ql->len);
        errors++;
    }

    if (count != ql->count) {
        yell("quicklist count wrong: expected %d, got %lu", count, ql->count);
        errors++;
    }

    int loopr = itrprintr(ql, 0);
    if (loopr != (int)ql->count) {
        yell("quicklist cached count not match actual count: expected %lu, got "
             "%d",
             ql->count, loopr);
        errors++;
    }

    int rloopr = itrprintr_rev(ql, 0);
    if (loopr != rloopr) {
        yell("quicklist has different forward count than reverse count!  "
             "Forward count is %d, reverse count is %d.",
             loopr, rloopr);
        errors++;
    }

    if (ql->len == 0 && !errors) {
        return errors;
    }

    if (ql->head && head_count != ql->head->count &&
        head_count != lpLength(ql->head->entry)) {
        yell("quicklist head count wrong: expected %d, "
             "got cached %d vs. actual %lu",
             head_count, ql->head->count, lpLength(ql->head->entry));
        errors++;
    }

    if (ql->tail && tail_count != ql->tail->count &&
        tail_count != lpLength(ql->tail->entry)) {
        yell("quicklist tail count wrong: expected %d, "
             "got cached %u vs. actual %lu",
             tail_count, ql->tail->count, lpLength(ql->tail->entry));
        errors++;
    }

    errors += _ql_verify_compress(ql);
    return errors;
}

/* Release iterator and verify compress correctly.
 * 释放迭代器并验证压缩状态是否正确。 */
static void ql_release_iterator(quicklistIter *iter) {
    quicklist *ql = NULL;
    if (iter) ql = iter->quicklist;
    quicklistReleaseIterator(iter);
    if (ql) assert(!_ql_verify_compress(ql));
}

/* Generate new string concatenating integer i against string 'prefix'
 * 生成 "前缀 + 整数" 的测试字符串。 */
static char *genstr(char *prefix, int i) {
    static char result[64] = {0};
    snprintf(result, sizeof(result), "%s%d", prefix, i);
    return result;
}

/* 用随机字符填充 target 缓冲区。
 * 字符集随机选自 a-z / 0-9 / A-Z。 */
static void randstring(unsigned char *target, size_t sz) {
    size_t p = 0;
    int minval, maxval;
    switch(rand() % 3) {
    case 0:
        minval = 'a';
        maxval = 'z';
    break;
    case 1:
        minval = '0';
        maxval = '9';
    break;
    case 2:
        minval = 'A';
        maxval = 'Z';
    break;
    default:
        assert(NULL);  // 理论上不可达
    }

    while(p < sz)
        target[p++] = minval+rand()%(maxval-minval+1);
}

/* main test, but callable from other files
 * quicklist 单元测试入口；可被其他文件调用。 */
int quicklistTest(int argc, char *argv[], int flags) {
    UNUSED(argc);
    UNUSED(argv);

    int accurate = (flags & REDIS_TEST_ACCURATE);
    unsigned int err = 0;
    int optimize_start =
        -(int)(sizeof(optimization_level) / sizeof(*optimization_level));

    printf("Starting optimization offset at: %d\n", optimize_start);

    int options[] = {0, 1, 2, 3, 4, 5, 6, 10};
    int fills[] = {-5, -4, -3, -2, -1, 0,
                   1, 2, 32, 66, 128, 999};
    size_t option_count = sizeof(options) / sizeof(*options);
    int fill_count = (int)(sizeof(fills) / sizeof(*fills));
    long long runtime[option_count];

    for (int _i = 0; _i < (int)option_count; _i++) {
        printf("Testing Compression option %d\n", options[_i]);
        long long start = mstime();
        quicklistIter *iter;

        TEST("create list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("add to tail of empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "hello", 6);
            /* 1 for head and 1 for tail because 1 node = head = tail
             * 头尾都是同一个节点，所以 head=tail=1 */
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST("add to head of empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            /* 1 for head and 1 for tail because 1 node = head = tail
             * 头尾都是同一个节点，所以 head=tail=1 */
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST_DESC("add to tail 5x at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 5; i++)
                    quicklistPushTail(ql, genstr("hello", i), 32);
                if (ql->count != 5)
                    ERROR;
                if (fills[f] == 32)
                    ql_verify(ql, 1, 5, 5, 5);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("add to head 5x at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 5; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                if (ql->count != 5)
                    ERROR;
                if (fills[f] == 32)
                    ql_verify(ql, 1, 5, 5, 5);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("add to tail 500x at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i), 64);
                if (ql->count != 500)
                    ERROR;
                if (fills[f] == 32)
                    ql_verify(ql, 16, 500, 32, 20);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("add to head 500x at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                if (ql->count != 500)
                    ERROR;
                if (fills[f] == 32)
                    ql_verify(ql, 16, 500, 20, 32);
                quicklistRelease(ql);
            }
        }

        TEST("rotate empty") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistRotate(ql);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("Comprassion Plain node") {
        for (int f = 0; f < fill_count; f++) {
            size_t large_limit = (fills[f] < 0) ? quicklistNodeNegFillLimit(fills[f]) + 1 : SIZE_SAFETY_LIMIT + 1;

            char buf[large_limit];
            quicklist *ql = quicklistNew(fills[f], 1);
            for (int i = 0; i < 500; i++) {
                /* Set to 256 to allow the node to be triggered to compress,
                 * if it is less than 48(nocompress), the test will be successful.
                 * 设为 256 以允许节点被触发压缩；
                 * 若小于 48（nocompress 阈值）则测试一定会通过。 */
                snprintf(buf, sizeof(buf), "hello%d", i);
                quicklistPushHead(ql, buf, large_limit);
            }

            quicklistIter *iter = quicklistGetIterator(ql, AL_START_TAIL);
            quicklistEntry entry;
            int i = 0;
            while (quicklistNext(iter, &entry)) {
                assert(QL_NODE_IS_PLAIN(entry.node));
                snprintf(buf, sizeof(buf), "hello%d", i);
                if (strcmp((char *)entry.value, buf))
                    ERR("value [%s] didn't match [%s] at position %d",
                        entry.value, buf, i);
                i++;
            }
            ql_release_iterator(iter);
            quicklistRelease(ql);
        }
        }

        TEST("NEXT plain node") {
        for (int f = 0; f < fill_count; f++) {
            size_t large_limit = (fills[f] < 0) ? quicklistNodeNegFillLimit(fills[f]) + 1 : SIZE_SAFETY_LIMIT + 1;
            quicklist *ql = quicklistNew(fills[f], options[_i]);

            char buf[large_limit];
            memcpy(buf, "plain", 5);
            quicklistPushHead(ql, buf, large_limit);
            quicklistPushHead(ql, buf, large_limit);
            quicklistPushHead(ql, "packed3", 7);
            quicklistPushHead(ql, "packed4", 7);
            quicklistPushHead(ql, buf, large_limit);

            quicklistEntry entry;
            quicklistIter *iter = quicklistGetIterator(ql, AL_START_TAIL);

            while(quicklistNext(iter, &entry) != 0) {
                if (QL_NODE_IS_PLAIN(entry.node))
                    assert(!memcmp(entry.value, "plain", 5));
                else
                    assert(!memcmp(entry.value, "packed", 6));
            }
            ql_release_iterator(iter);
            quicklistRelease(ql);
        }
        }

        TEST("rotate plain node ") {
        for (int f = 0; f < fill_count; f++) {
            size_t large_limit = (fills[f] < 0) ? quicklistNodeNegFillLimit(fills[f]) + 1 : SIZE_SAFETY_LIMIT + 1;

            unsigned char *data = NULL;
            size_t sz;
            long long lv;
            int i =0;
            quicklist *ql = quicklistNew(fills[f], options[_i]);
            char buf[large_limit];
            memcpy(buf, "hello1", 6);
            quicklistPushHead(ql, buf, large_limit);
            memcpy(buf, "hello4", 6);
            quicklistPushHead(ql, buf, large_limit);
            memcpy(buf, "hello3", 6);
            quicklistPushHead(ql, buf, large_limit);
            memcpy(buf, "hello2", 6);
            quicklistPushHead(ql, buf, large_limit);
            quicklistRotate(ql);

            for(i = 1 ; i < 5; i++) {
                assert(QL_NODE_IS_PLAIN(ql->tail));
                quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
                int temp_char = data[5];
                zfree(data);
                assert(temp_char == ('0' + i));
            }

            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }
        }

        TEST("rotate one val once") {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                quicklistPushHead(ql, "hello", 6);
                quicklistRotate(ql);
                /* Ignore compression verify because listpack is
                 * too small to compress.
                 * 不进行压缩校验，因为 listpack 太小无法被压缩。 */
                ql_verify(ql, 1, 1, 1, 1);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("rotate 500 val 5000 times at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                quicklistPushHead(ql, "900", 3);
                quicklistPushHead(ql, "7000", 4);
                quicklistPushHead(ql, "-1200", 5);
                quicklistPushHead(ql, "42", 2);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 64);
                ql_info(ql);
                for (int i = 0; i < 5000; i++) {
                    ql_info(ql);
                    quicklistRotate(ql);
                }
                if (fills[f] == 1)
                    ql_verify(ql, 504, 504, 1, 1);
                else if (fills[f] == 2)
                    ql_verify(ql, 252, 504, 2, 2);
                else if (fills[f] == 32)
                    ql_verify(ql, 16, 504, 32, 24);
                quicklistRelease(ql);
            }
        }

        TEST("pop empty") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPop(ql, QUICKLIST_HEAD, NULL, NULL, NULL);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop 1 string from 1") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            char *populate = genstr("hello", 331);
            quicklistPushHead(ql, populate, 32);
            unsigned char *data;
            size_t sz;
            long long lv;
            ql_info(ql);
            assert(quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv));
            assert(data != NULL);
            assert(sz == 32);
            if (strcmp(populate, (char *)data)) {
                int size = sz;
                ERR("Pop'd value (%.*s) didn't equal original value (%s)", size,
                    data, populate);
            }
            zfree(data);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 1 number from 1") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "55513", 5);
            unsigned char *data;
            size_t sz;
            long long lv;
            ql_info(ql);
            assert(quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv));
            assert(data == NULL);
            assert(lv == 55513);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 500 from 500") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_info(ql);
            for (int i = 0; i < 500; i++) {
                unsigned char *data;
                size_t sz;
                long long lv;
                int ret = quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
                assert(ret == 1);
                assert(data != NULL);
                assert(sz == 32);
                if (strcmp(genstr("hello", 499 - i), (char *)data)) {
                    int size = sz;
                    ERR("Pop'd value (%.*s) didn't equal original value (%s)",
                        size, data, genstr("hello", 499 - i));
                }
                zfree(data);
            }
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 5000 from 500") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            for (int i = 0; i < 5000; i++) {
                unsigned char *data;
                size_t sz;
                long long lv;
                int ret = quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
                if (i < 500) {
                    assert(ret == 1);
                    assert(data != NULL);
                    assert(sz == 32);
                    if (strcmp(genstr("hello", 499 - i), (char *)data)) {
                        int size = sz;
                        ERR("Pop'd value (%.*s) didn't equal original value "
                            "(%s)",
                            size, data, genstr("hello", 499 - i));
                    }
                    zfree(data);
                } else {
                    assert(ret == 0);
                }
            }
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("iterate forward over 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
            quicklistEntry entry;
            int i = 499, count = 0;
            while (quicklistNext(iter, &entry)) {
                char *h = genstr("hello", i);
                if (strcmp((char *)entry.value, h))
                    ERR("value [%s] didn't match [%s] at position %d",
                        entry.value, h, i);
                i--;
                count++;
            }
            if (count != 500)
                ERR("Didn't iterate over exactly 500 elements (%d)", i);
            ql_verify(ql, 16, 500, 20, 32);
            ql_release_iterator(iter);
            quicklistRelease(ql);
        }

        TEST("iterate reverse over 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            quicklistIter *iter = quicklistGetIterator(ql, AL_START_TAIL);
            quicklistEntry entry;
            int i = 0;
            while (quicklistNext(iter, &entry)) {
                char *h = genstr("hello", i);
                if (strcmp((char *)entry.value, h))
                    ERR("value [%s] didn't match [%s] at position %d",
                        entry.value, h, i);
                i++;
            }
            if (i != 500)
                ERR("Didn't iterate over exactly 500 elements (%d)", i);
            ql_verify(ql, 16, 500, 20, 32);
            ql_release_iterator(iter);
            quicklistRelease(ql);
        }

        TEST("insert after 1 element") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            quicklistEntry entry;
            iter = quicklistGetIteratorEntryAtIdx(ql, 0, &entry);
            quicklistInsertAfter(iter, &entry, "abc", 4);
            ql_release_iterator(iter);
            ql_verify(ql, 1, 2, 2, 2);

            /* verify results */
            iter = quicklistGetIteratorEntryAtIdx(ql, 0, &entry);
            int sz = entry.sz;
            if (strncmp((char *)entry.value, "hello", 5)) {
                ERR("Value 0 didn't match, instead got: %.*s", sz,
                    entry.value);
            }
            ql_release_iterator(iter);

            iter = quicklistGetIteratorEntryAtIdx(ql, 1, &entry);
            sz = entry.sz;
            if (strncmp((char *)entry.value, "abc", 3)) {
                ERR("Value 1 didn't match, instead got: %.*s", sz,
                    entry.value);
            }
            ql_release_iterator(iter);
            quicklistRelease(ql);
        }

        TEST("insert before 1 element") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            quicklistEntry entry;
            iter = quicklistGetIteratorEntryAtIdx(ql, 0, &entry);
            quicklistInsertBefore(iter, &entry, "abc", 4);
            ql_release_iterator(iter);
            ql_verify(ql, 1, 2, 2, 2);

            /* verify results */
            iter = quicklistGetIteratorEntryAtIdx(ql, 0, &entry);
            int sz = entry.sz;
            if (strncmp((char *)entry.value, "abc", 3)) {
                ERR("Value 0 didn't match, instead got: %.*s", sz,
                    entry.value);
            }
            ql_release_iterator(iter);

            iter = quicklistGetIteratorEntryAtIdx(ql, 1, &entry);
            sz = entry.sz;
            if (strncmp((char *)entry.value, "hello", 5)) {
                ERR("Value 1 didn't match, instead got: %.*s", sz,
                    entry.value);
            }
            ql_release_iterator(iter);
            quicklistRelease(ql);
        }

        TEST("insert head while head node is full") {
            quicklist *ql = quicklistNew(4, options[_i]);
            for (int i = 0; i < 10; i++)
                quicklistPushTail(ql, genstr("hello", i), 6);
            quicklistSetFill(ql, -1);
            quicklistEntry entry;
            iter = quicklistGetIteratorEntryAtIdx(ql, -10, &entry);
            char buf[4096] = {0};
            quicklistInsertBefore(iter, &entry, buf, 4096);
            ql_release_iterator(iter);
            ql_verify(ql, 4, 11, 1, 2);
            quicklistRelease(ql);
        }

        TEST("insert tail while tail node is full") {
            quicklist *ql = quicklistNew(4, options[_i]);
            for (int i = 0; i < 10; i++)
                quicklistPushHead(ql, genstr("hello", i), 6);
            quicklistSetFill(ql, -1);
            quicklistEntry entry;
            iter = quicklistGetIteratorEntryAtIdx(ql, -1, &entry);
            char buf[4096] = {0};
            quicklistInsertAfter(iter, &entry, buf, 4096);
            ql_release_iterator(iter);
            ql_verify(ql, 4, 11, 2, 1);
            quicklistRelease(ql);
        }

        TEST_DESC("insert once in elements while iterating at compress %d",
                  options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                quicklistPushTail(ql, "abc", 3);
                quicklistSetFill(ql, 1);
                quicklistPushTail(ql, "def", 3); /* force to unique node */
                quicklistSetFill(ql, f);
                quicklistPushTail(ql, "bob", 3); /* force to reset for +3 */
                quicklistPushTail(ql, "foo", 3);
                quicklistPushTail(ql, "zoo", 3);

                itrprintr(ql, 0);
                /* insert "bar" before "bob" while iterating over list. */
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
                quicklistEntry entry;
                while (quicklistNext(iter, &entry)) {
                    if (!strncmp((char *)entry.value, "bob", 3)) {
                        /* Insert as fill = 1 so it spills into new node. */
                        quicklistInsertBefore(iter, &entry, "bar", 3);
                        break; /* didn't we fix insert-while-iterating? */
                    }
                }
                ql_release_iterator(iter);
                itrprintr(ql, 0);

                /* verify results */
                iter = quicklistGetIteratorEntryAtIdx(ql, 0, &entry);
                int sz = entry.sz;

                if (strncmp((char *)entry.value, "abc", 3))
                    ERR("Value 0 didn't match, instead got: %.*s", sz,
                        entry.value);
                ql_release_iterator(iter);

                iter = quicklistGetIteratorEntryAtIdx(ql, 1, &entry);
                if (strncmp((char *)entry.value, "def", 3))
                    ERR("Value 1 didn't match, instead got: %.*s", sz,
                        entry.value);
                ql_release_iterator(iter);

                iter = quicklistGetIteratorEntryAtIdx(ql, 2, &entry);
                if (strncmp((char *)entry.value, "bar", 3))
                    ERR("Value 2 didn't match, instead got: %.*s", sz,
                        entry.value);
                ql_release_iterator(iter);

                iter = quicklistGetIteratorEntryAtIdx(ql, 3, &entry);
                if (strncmp((char *)entry.value, "bob", 3))
                    ERR("Value 3 didn't match, instead got: %.*s", sz,
                        entry.value);
                ql_release_iterator(iter);

                iter = quicklistGetIteratorEntryAtIdx(ql, 4, &entry);
                if (strncmp((char *)entry.value, "foo", 3))
                    ERR("Value 4 didn't match, instead got: %.*s", sz,
                        entry.value);
                ql_release_iterator(iter);

                iter = quicklistGetIteratorEntryAtIdx(ql, 5, &entry);
                if (strncmp((char *)entry.value, "zoo", 3))
                    ERR("Value 5 didn't match, instead got: %.*s", sz,
                        entry.value);
                ql_release_iterator(iter);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("insert [before] 250 new in middle of 500 elements at compress %d",
                  options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i), 32);
                for (int i = 0; i < 250; i++) {
                    quicklistEntry entry;
                    iter = quicklistGetIteratorEntryAtIdx(ql, 250, &entry);
                    quicklistInsertBefore(iter, &entry, genstr("abc", i), 32);
                    ql_release_iterator(iter);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 25, 750, 32, 20);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("insert [after] 250 new in middle of 500 elements at compress %d",
                  options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                for (int i = 0; i < 250; i++) {
                    quicklistEntry entry;
                    iter = quicklistGetIteratorEntryAtIdx(ql, 250, &entry);
                    quicklistInsertAfter(iter, &entry, genstr("abc", i), 32);
                    ql_release_iterator(iter);
                }

                if (ql->count != 750)
                    ERR("List size not 750, but rather %ld", ql->count);

                if (fills[f] == 32)
                    ql_verify(ql, 26, 750, 20, 32);
                quicklistRelease(ql);
            }
        }

        TEST("duplicate empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            ql_verify(ql, 0, 0, 0, 0);
            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 0, 0, 0, 0);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        TEST("duplicate list of 1 element") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, genstr("hello", 3), 32);
            ql_verify(ql, 1, 1, 1, 1);
            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 1, 1, 1, 1);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        TEST("duplicate list of 500") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 16, 500, 20, 32);

            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 16, 500, 20, 32);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        for (int f = 0; f < fill_count; f++) {
            TEST_DESC("index 1,200 from 500 list at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                iter = quicklistGetIteratorEntryAtIdx(ql, 1, &entry);
                if (strcmp((char *)entry.value, "hello2") != 0)
                    ERR("Value: %s", entry.value);
                ql_release_iterator(iter);

                iter = quicklistGetIteratorEntryAtIdx(ql, 200, &entry);
                if (strcmp((char *)entry.value, "hello201") != 0)
                    ERR("Value: %s", entry.value);
                ql_release_iterator(iter);
                quicklistRelease(ql);
            }

            TEST_DESC("index -1,-2 from 500 list at fill %d at compress %d",
                      fills[f], options[_i]) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                iter = quicklistGetIteratorEntryAtIdx(ql, -1, &entry);
                if (strcmp((char *)entry.value, "hello500") != 0)
                    ERR("Value: %s", entry.value);
                ql_release_iterator(iter);

                iter = quicklistGetIteratorEntryAtIdx(ql, -2, &entry);
                if (strcmp((char *)entry.value, "hello499") != 0)
                    ERR("Value: %s", entry.value);
                ql_release_iterator(iter);
                quicklistRelease(ql);
            }

            TEST_DESC("index -100 from 500 list at fill %d at compress %d",
                      fills[f], options[_i]) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                iter = quicklistGetIteratorEntryAtIdx(ql, -100, &entry);
                if (strcmp((char *)entry.value, "hello401") != 0)
                    ERR("Value: %s", entry.value);
                ql_release_iterator(iter);
                quicklistRelease(ql);
            }

            TEST_DESC("index too big +1 from 50 list at fill %d at compress %d",
                      fills[f], options[_i]) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 50; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                int sz = entry.sz;
                iter = quicklistGetIteratorEntryAtIdx(ql, 50, &entry);
                if (iter)
                    ERR("Index found at 50 with 50 list: %.*s", sz,
                        entry.value);
                ql_release_iterator(iter);
                quicklistRelease(ql);
            }
        }

        TEST("delete range empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistDelRange(ql, 5, 20);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete range of entire node in list of one node") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 32; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 1, 32, 32, 32);
            quicklistDelRange(ql, 0, 32);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete range of entire node with overflow counts") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 32; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 1, 32, 32, 32);
            quicklistDelRange(ql, 0, 128);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete middle 100 of 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, 200, 100);
            ql_verify(ql, 14, 400, 32, 20);
            quicklistRelease(ql);
        }

        TEST("delete less than fill but across nodes") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, 60, 10);
            ql_verify(ql, 16, 490, 32, 20);
            quicklistRelease(ql);
        }

        TEST("delete negative 1 from 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, -1, 1);
            ql_verify(ql, 16, 499, 32, 19);
            quicklistRelease(ql);
        }

        TEST("delete negative 1 from 500 list with overflow counts") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, -1, 128);
            ql_verify(ql, 16, 499, 32, 19);
            quicklistRelease(ql);
        }

        TEST("delete negative 100 from 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            quicklistDelRange(ql, -100, 100);
            ql_verify(ql, 13, 400, 32, 16);
            quicklistRelease(ql);
        }

        TEST("delete -10 count 5 from 50 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 50; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 2, 50, 32, 18);
            quicklistDelRange(ql, -10, 5);
            ql_verify(ql, 2, 45, 32, 13);
            quicklistRelease(ql);
        }

        TEST("numbers only list read") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "1111", 4);
            quicklistPushTail(ql, "2222", 4);
            quicklistPushTail(ql, "3333", 4);
            quicklistPushTail(ql, "4444", 4);
            ql_verify(ql, 1, 4, 4, 4);
            quicklistEntry entry;
            iter = quicklistGetIteratorEntryAtIdx(ql, 0, &entry);
            if (entry.longval != 1111)
                ERR("Not 1111, %lld", entry.longval);
            ql_release_iterator(iter);

            iter = quicklistGetIteratorEntryAtIdx(ql, 1, &entry);
            if (entry.longval != 2222)
                ERR("Not 2222, %lld", entry.longval);
            ql_release_iterator(iter);

            iter = quicklistGetIteratorEntryAtIdx(ql, 2, &entry);
            if (entry.longval != 3333)
                ERR("Not 3333, %lld", entry.longval);
            ql_release_iterator(iter);

            iter = quicklistGetIteratorEntryAtIdx(ql, 3, &entry);
            if (entry.longval != 4444)
                ERR("Not 4444, %lld", entry.longval);
            ql_release_iterator(iter);

            iter = quicklistGetIteratorEntryAtIdx(ql, 4, &entry);
            if (iter)
                ERR("Index past elements: %lld", entry.longval);
            ql_release_iterator(iter);
            
            iter = quicklistGetIteratorEntryAtIdx(ql, -1, &entry);
            if (entry.longval != 4444)
                ERR("Not 4444 (reverse), %lld", entry.longval);
            ql_release_iterator(iter);

            iter = quicklistGetIteratorEntryAtIdx(ql, -2, &entry);
            if (entry.longval != 3333)
                ERR("Not 3333 (reverse), %lld", entry.longval);
            ql_release_iterator(iter);

            iter = quicklistGetIteratorEntryAtIdx(ql, -3, &entry);
            if (entry.longval != 2222)
                ERR("Not 2222 (reverse), %lld", entry.longval);
            ql_release_iterator(iter);
            
            iter = quicklistGetIteratorEntryAtIdx(ql, -4, &entry);
            if (entry.longval != 1111)
                ERR("Not 1111 (reverse), %lld", entry.longval);
            ql_release_iterator(iter);
            
            iter = quicklistGetIteratorEntryAtIdx(ql, -5, &entry);
            if (iter)
                ERR("Index past elements (reverse), %lld", entry.longval);
            ql_release_iterator(iter);
            quicklistRelease(ql);
        }

        TEST("numbers larger list read") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            char num[32];
            long long nums[5000];
            for (int i = 0; i < 5000; i++) {
                nums[i] = -5157318210846258176 + i;
                int sz = ll2string(num, sizeof(num), nums[i]);
                quicklistPushTail(ql, num, sz);
            }
            quicklistPushTail(ql, "xxxxxxxxxxxxxxxxxxxx", 20);
            quicklistEntry entry;
            for (int i = 0; i < 5000; i++) {
                iter = quicklistGetIteratorEntryAtIdx(ql, i, &entry);
                if (entry.longval != nums[i])
                    ERR("[%d] Not longval %lld but rather %lld", i, nums[i],
                        entry.longval);
                entry.longval = 0xdeadbeef;
                ql_release_iterator(iter);
            }
            iter = quicklistGetIteratorEntryAtIdx(ql, 5000, &entry);
            if (strncmp((char *)entry.value, "xxxxxxxxxxxxxxxxxxxx", 20))
                ERR("String val not match: %s", entry.value);
            ql_verify(ql, 157, 5001, 32, 9);
            ql_release_iterator(iter);
            quicklistRelease(ql);
        }

        TEST("numbers larger list read B") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "99", 2);
            quicklistPushTail(ql, "98", 2);
            quicklistPushTail(ql, "xxxxxxxxxxxxxxxxxxxx", 20);
            quicklistPushTail(ql, "96", 2);
            quicklistPushTail(ql, "95", 2);
            quicklistReplaceAtIndex(ql, 1, "foo", 3);
            quicklistReplaceAtIndex(ql, -1, "bar", 3);
            quicklistRelease(ql);
        }

        TEST_DESC("lrem test at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                char *words[] = {"abc", "foo", "bar",  "foobar", "foobared",
                                 "zap", "bar", "test", "foo"};
                char *result[] = {"abc", "foo",  "foobar", "foobared",
                                  "zap", "test", "foo"};
                char *resultB[] = {"abc",      "foo", "foobar",
                                   "foobared", "zap", "test"};
                for (int i = 0; i < 9; i++)
                    quicklistPushTail(ql, words[i], strlen(words[i]));

                /* lrem 0 bar */
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
                quicklistEntry entry;
                int i = 0;
                while (quicklistNext(iter, &entry)) {
                    if (quicklistCompare(&entry, (unsigned char *)"bar", 3)) {
                        quicklistDelEntry(iter, &entry);
                    }
                    i++;
                }
                ql_release_iterator(iter);

                /* check result of lrem 0 bar */
                iter = quicklistGetIterator(ql, AL_START_HEAD);
                i = 0;
                while (quicklistNext(iter, &entry)) {
                    /* Result must be: abc, foo, foobar, foobared, zap, test,
                     * foo */
                    int sz = entry.sz;
                    if (strncmp((char *)entry.value, result[i], entry.sz)) {
                        ERR("No match at position %d, got %.*s instead of %s",
                            i, sz, entry.value, result[i]);
                    }
                    i++;
                }
                ql_release_iterator(iter);

                quicklistPushTail(ql, "foo", 3);

                /* lrem -2 foo */
                iter = quicklistGetIterator(ql, AL_START_TAIL);
                i = 0;
                int del = 2;
                while (quicklistNext(iter, &entry)) {
                    if (quicklistCompare(&entry, (unsigned char *)"foo", 3)) {
                        quicklistDelEntry(iter, &entry);
                        del--;
                    }
                    if (!del)
                        break;
                    i++;
                }
                ql_release_iterator(iter);

                /* check result of lrem -2 foo */
                /* (we're ignoring the '2' part and still deleting all foo
                 * because
                 * we only have two foo) */
                iter = quicklistGetIterator(ql, AL_START_TAIL);
                i = 0;
                size_t resB = sizeof(resultB) / sizeof(*resultB);
                while (quicklistNext(iter, &entry)) {
                    /* Result must be: abc, foo, foobar, foobared, zap, test,
                     * foo */
                    int sz = entry.sz;
                    if (strncmp((char *)entry.value, resultB[resB - 1 - i],
                                sz)) {
                        ERR("No match at position %d, got %.*s instead of %s",
                            i, sz, entry.value, resultB[resB - 1 - i]);
                    }
                    i++;
                }

                ql_release_iterator(iter);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("iterate reverse + delete at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                quicklistPushTail(ql, "abc", 3);
                quicklistPushTail(ql, "def", 3);
                quicklistPushTail(ql, "hij", 3);
                quicklistPushTail(ql, "jkl", 3);
                quicklistPushTail(ql, "oop", 3);

                quicklistEntry entry;
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_TAIL);
                int i = 0;
                while (quicklistNext(iter, &entry)) {
                    if (quicklistCompare(&entry, (unsigned char *)"hij", 3)) {
                        quicklistDelEntry(iter, &entry);
                    }
                    i++;
                }
                ql_release_iterator(iter);

                if (i != 5)
                    ERR("Didn't iterate 5 times, iterated %d times.", i);

                /* Check results after deletion of "hij"
                 * 检查删除 "hij" 后的结果 */
                iter = quicklistGetIterator(ql, AL_START_HEAD);
                i = 0;
                char *vals[] = {"abc", "def", "jkl", "oop"};
                while (quicklistNext(iter, &entry)) {
                    if (!quicklistCompare(&entry, (unsigned char *)vals[i],
                                          3)) {
                        ERR("Value at %d didn't match %s\n", i, vals[i]);
                    }
                    i++;
                }
                ql_release_iterator(iter);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("iterator at index test at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 760; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }

                quicklistEntry entry;
                quicklistIter *iter =
                    quicklistGetIteratorAtIdx(ql, AL_START_HEAD, 437);
                int i = 437;
                while (quicklistNext(iter, &entry)) {
                    if (entry.longval != nums[i])
                        ERR("Expected %lld, but got %lld", entry.longval,
                            nums[i]);
                    i++;
                }
                ql_release_iterator(iter);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("ltrim test A at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 32; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 1, 32, 32, 32);
                /* ltrim 25 53 (keep [25,32] inclusive = 7 remaining) */
                quicklistDelRange(ql, 0, 25);
                quicklistDelRange(ql, 0, 0);
                quicklistEntry entry;
                for (int i = 0; i < 7; i++) {
                    iter = quicklistGetIteratorEntryAtIdx(ql, i, &entry);
                    if (entry.longval != nums[25 + i])
                        ERR("Deleted invalid range!  Expected %lld but got "
                            "%lld",
                            entry.longval, nums[25 + i]);
                    ql_release_iterator(iter);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 1, 7, 7, 7);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("ltrim test B at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                /* Force-disable compression because our 33 sequential
                 * integers don't compress and the check always fails. */
                quicklist *ql = quicklistNew(fills[f], QUICKLIST_NOCOMPRESS);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++) {
                    nums[i] = i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                /* ltrim 5 16 (keep [5,16] inclusive = 12 remaining) */
                quicklistDelRange(ql, 0, 5);
                quicklistDelRange(ql, -16, 16);
                if (fills[f] == 32)
                    ql_verify(ql, 1, 12, 12, 12);
                quicklistEntry entry;

                iter = quicklistGetIteratorEntryAtIdx(ql, 0, &entry);
                if (entry.longval != 5)
                    ERR("A: longval not 5, but %lld", entry.longval);
                ql_release_iterator(iter);

                iter = quicklistGetIteratorEntryAtIdx(ql, -1, &entry);
                if (entry.longval != 16)
                    ERR("B! got instead: %lld", entry.longval);
                quicklistPushTail(ql, "bobobob", 7);
                ql_release_iterator(iter);

                iter = quicklistGetIteratorEntryAtIdx(ql, -1, &entry);
                int sz = entry.sz;
                if (strncmp((char *)entry.value, "bobobob", 7))
                    ERR("Tail doesn't match bobobob, it's %.*s instead",
                        sz, entry.value);
                ql_release_iterator(iter);

                for (int i = 0; i < 12; i++) {
                    iter = quicklistGetIteratorEntryAtIdx(ql, i, &entry);
                    if (entry.longval != nums[5 + i])
                        ERR("Deleted invalid range!  Expected %lld but got "
                            "%lld",
                            entry.longval, nums[5 + i]);
                    ql_release_iterator(iter);
                }
                quicklistRelease(ql);
            }
        }

        TEST_DESC("ltrim test C at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                /* ltrim 3 3 (keep [3,3] inclusive = 1 remaining) */
                quicklistDelRange(ql, 0, 3);
                quicklistDelRange(ql, -29,
                                  4000); /* make sure not loop forever */
                if (fills[f] == 32)
                    ql_verify(ql, 1, 1, 1, 1);
                quicklistEntry entry;
                iter = quicklistGetIteratorEntryAtIdx(ql, 0, &entry);
                if (entry.longval != -5157318210846258173)
                    ERROR;
                ql_release_iterator(iter);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("ltrim test D at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                quicklistDelRange(ql, -12, 3);
                if (ql->count != 30)
                    ERR("Didn't delete exactly three elements!  Count is: %lu",
                        ql->count);
                quicklistRelease(ql);
            }
        }

        long long stop = mstime();
        runtime[_i] = stop - start;
    }

    /* Run a longer test of compression depth outside of primary test loop. */
    int list_sizes[] = {250, 251, 500, 999, 1000};
    long long start = mstime();
    int list_count = accurate ? (int)(sizeof(list_sizes) / sizeof(*list_sizes)) : 1;
    for (int list = 0; list < list_count; list++) {
        TEST_DESC("verify specific compression of interior nodes with %d list ",
                  list_sizes[list]) {
            for (int f = 0; f < fill_count; f++) {
                for (int depth = 1; depth < 40; depth++) {
                    /* skip over many redundant test cases */
                    quicklist *ql = quicklistNew(fills[f], depth);
                    for (int i = 0; i < list_sizes[list]; i++) {
                        quicklistPushTail(ql, genstr("hello TAIL", i + 1), 64);
                        quicklistPushHead(ql, genstr("hello HEAD", i + 1), 64);
                    }

                    for (int step = 0; step < 2; step++) {
                        /* test remove node */
                        if (step == 1) {
                            for (int i = 0; i < list_sizes[list] / 2; i++) {
                                unsigned char *data;
                                assert(quicklistPop(ql, QUICKLIST_HEAD, &data,
                                                    NULL, NULL));
                                zfree(data);
                                assert(quicklistPop(ql, QUICKLIST_TAIL, &data,
                                                    NULL, NULL));
                                zfree(data);
                            }
                        }
                        quicklistNode *node = ql->head;
                        unsigned int low_raw = ql->compress;
                        unsigned int high_raw = ql->len - ql->compress;

                        for (unsigned int at = 0; at < ql->len;
                            at++, node = node->next) {
                            if (at < low_raw || at >= high_raw) {
                                if (node->encoding != QUICKLIST_NODE_ENCODING_RAW) {
                                    ERR("Incorrect compression: node %d is "
                                        "compressed at depth %d ((%u, %u); total "
                                        "nodes: %lu; size: %zu)",
                                        at, depth, low_raw, high_raw, ql->len,
                                        node->sz);
                                }
                            } else {
                                if (node->encoding != QUICKLIST_NODE_ENCODING_LZF) {
                                    ERR("Incorrect non-compression: node %d is NOT "
                                        "compressed at depth %d ((%u, %u); total "
                                        "nodes: %lu; size: %zu; attempted: %d)",
                                        at, depth, low_raw, high_raw, ql->len,
                                        node->sz, node->attempted_compress);
                                }
                            }
                        }
                    }

                    quicklistRelease(ql);
                }
            }
        }
    }
    long long stop = mstime();

    printf("\n");
    for (size_t i = 0; i < option_count; i++)
        printf("Test Loop %02d: %0.2f seconds.\n", options[i],
               (float)runtime[i] / 1000);
    printf("Compressions: %0.2f seconds.\n", (float)(stop - start) / 1000);
    printf("\n");

    TEST("bookmark get updated to next item") {
        quicklist *ql = quicklistNew(1, 0);
        quicklistPushTail(ql, "1", 1);
        quicklistPushTail(ql, "2", 1);
        quicklistPushTail(ql, "3", 1);
        quicklistPushTail(ql, "4", 1);
        quicklistPushTail(ql, "5", 1);
        assert(ql->len==5);
        /* add two bookmarks, one pointing to the node before the last. */
        assert(quicklistBookmarkCreate(&ql, "_dummy", ql->head->next));
        assert(quicklistBookmarkCreate(&ql, "_test", ql->tail->prev));
        /* test that the bookmark returns the right node, delete it and see that the bookmark points to the last node */
        assert(quicklistBookmarkFind(ql, "_test") == ql->tail->prev);
        assert(quicklistDelRange(ql, -2, 1));
        assert(quicklistBookmarkFind(ql, "_test") == ql->tail);
        /* delete the last node, and see that the bookmark was deleted. */
        assert(quicklistDelRange(ql, -1, 1));
        assert(quicklistBookmarkFind(ql, "_test") == NULL);
        /* test that other bookmarks aren't affected */
        assert(quicklistBookmarkFind(ql, "_dummy") == ql->head->next);
        assert(quicklistBookmarkFind(ql, "_missing") == NULL);
        assert(ql->len==3);
        quicklistBookmarksClear(ql); /* for coverage */
        assert(quicklistBookmarkFind(ql, "_dummy") == NULL);
        quicklistRelease(ql);
    }

    TEST("bookmark limit") {
        int i;
        quicklist *ql = quicklistNew(1, 0);
        quicklistPushHead(ql, "1", 1);
        for (i=0; i<QL_MAX_BM; i++)
            assert(quicklistBookmarkCreate(&ql, genstr("",i), ql->head));
        /* when all bookmarks are used, creation fails */
        assert(!quicklistBookmarkCreate(&ql, "_test", ql->head));
        /* delete one and see that we can now create another */
        assert(quicklistBookmarkDelete(ql, "0"));
        assert(quicklistBookmarkCreate(&ql, "_test", ql->head));
        /* delete one and see that the rest survive */
        assert(quicklistBookmarkDelete(ql, "_test"));
        for (i=1; i<QL_MAX_BM; i++)
            assert(quicklistBookmarkFind(ql, genstr("",i)) == ql->head);
        /* make sure the deleted ones are indeed gone */
        assert(!quicklistBookmarkFind(ql, "0"));
        assert(!quicklistBookmarkFind(ql, "_test"));
        quicklistRelease(ql);
    }

    if (flags & REDIS_TEST_LARGE_MEMORY) {
        TEST("compress and decompress quicklist listpack node") {
            quicklistNode *node = quicklistCreateNode();
            node->entry = lpNew(0);

            /* Just to avoid triggering the assertion in __quicklistCompressNode(),
             * it disables the passing of quicklist head or tail node. */
            node->prev = quicklistCreateNode();
            node->next = quicklistCreateNode();
            
            /* Create a rand string */
            size_t sz = (1 << 25); /* 32MB per one entry */
            unsigned char *s = zmalloc(sz);
            randstring(s, sz);

            /* Keep filling the node, until it reaches 1GB */
            for (int i = 0; i < 32; i++) {
                node->entry = lpAppend(node->entry, s, sz);
                quicklistNodeUpdateSz(node);

                long long start = mstime();
                assert(__quicklistCompressNode(node));
                assert(__quicklistDecompressNode(node));
                printf("Compress and decompress: %zu MB in %.2f seconds.\n",
                       node->sz/1024/1024, (float)(mstime() - start) / 1000);
            }

            zfree(s);
            zfree(node->prev);
            zfree(node->next);
            zfree(node->entry);
            zfree(node);
        }

#if ULONG_MAX >= 0xffffffffffffffff
        TEST("compress and decomress quicklist plain node large than UINT32_MAX") {
            size_t sz = (1ull << 32);
            unsigned char *s = zmalloc(sz);
            randstring(s, sz);
            memcpy(s, "helloworld", 10);
            memcpy(s + sz - 10, "1234567890", 10);

            quicklistNode *node = __quicklistCreateNode(QUICKLIST_NODE_CONTAINER_PLAIN, s, sz);

            /* Just to avoid triggering the assertion in __quicklistCompressNode(),
             * it disables the passing of quicklist head or tail node. */
            node->prev = quicklistCreateNode();
            node->next = quicklistCreateNode();

            long long start = mstime();
            assert(__quicklistCompressNode(node));
            assert(__quicklistDecompressNode(node));
            printf("Compress and decompress: %zu MB in %.2f seconds.\n",
                   node->sz/1024/1024, (float)(mstime() - start) / 1000);

            assert(memcmp(node->entry, "helloworld", 10) == 0);
            assert(memcmp(node->entry + sz - 10, "1234567890", 10) == 0);
            zfree(node->prev);
            zfree(node->next);
            zfree(node->entry);
            zfree(node);
        }
#endif
    }

    if (!err)
        printf("ALL TESTS PASSED!\n");
    else
        ERR("Sorry, not all tests passed!  In fact, %d tests failed.", err);

    return err;
}
#endif
