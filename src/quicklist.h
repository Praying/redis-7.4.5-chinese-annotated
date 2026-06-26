/* quicklist.h - A generic doubly linked quicklist implementation
 * quicklist.h - 通用双向链表 quicklist 的实现
 *
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
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

#include <stdint.h> // for UINTPTR_MAX（用于 UINTPTR_MAX 平台位数判断）

#ifndef __QUICKLIST_H__
#define __QUICKLIST_H__

/* Node, quicklist, and Iterator are the only data structures used currently.
 * 目前只使用 Node、quicklist 和 Iterator 这三种数据结构。 */

/* quicklistNode is a 32 byte struct describing a listpack for a quicklist.
 * We use bit fields keep the quicklistNode at 32 bytes.
 * count: 16 bits, max 65536 (max lp bytes is 65k, so max count actually < 32k).
 * encoding: 2 bits, RAW=1, LZF=2.
 * container: 2 bits, PLAIN=1 (a single item as char array), PACKED=2 (listpack with multiple items).
 * recompress: 1 bit, bool, true if node is temporary decompressed for usage.
 * attempted_compress: 1 bit, boolean, used for verifying during testing.
 * dont_compress: 1 bit, boolean, used for preventing compression of entry.
 * extra: 9 bits, free for future use; pads out the remainder of 32 bits
 *
 * quicklistNode 是一个 32 字节的结构体，用于描述 quicklist 中的一个 listpack。
 * 我们使用位域（bit fields）以保持 quicklistNode 始终占用 32 字节。
 *   count: 16 位，最大 65536（lp 字节最大为 65k，所以 count 实际上小于 32k）
 *   encoding: 2 位，RAW=1，LZF=2
 *   container: 2 位，PLAIN=1（单个 item 的字符数组），PACKED=2（包含多个 item 的 listpack）
 *   recompress: 1 位，布尔值，节点是否为了使用而被临时解压过
 *   attempted_compress: 1 位，布尔值，仅在测试时用于验证
 *   dont_compress: 1 位，布尔值，防止稍后还要使用的 entry 被压缩
 *   extra: 9 位，保留备用，凑齐 32 位剩余部分 */
typedef struct quicklistNode {
    struct quicklistNode *prev;   // 前驱节点指针
    struct quicklistNode *next;   // 后继节点指针
    unsigned char *entry;         // 指向 listpack 数据（或压缩后的 LZF 数据）
    size_t sz;                    /* entry size in bytes：entry 的字节大小 */
    unsigned int count : 16;      /* count of items in listpack：listpack 中的元素数 */
    unsigned int encoding : 2;    /* RAW==1 or LZF==2：编码方式（RAW 或 LZF） */
    unsigned int container : 2;   /* PLAIN==1 or PACKED==2：容器类型（PLAIN 或 PACKED） */
    unsigned int recompress : 1;  /* was this node previous compressed?：节点此前是否被压缩过 */
    unsigned int attempted_compress : 1; /* node can't compress; too small：节点无法压缩（太小） */
    unsigned int dont_compress : 1; /* prevent compression of entry that will be used later：阻止稍后还要使用的 entry 被压缩 */
    unsigned int extra : 9;       /* more bits to steal for future usage：备用位，可供将来使用 */
} quicklistNode;

/* quicklistLZF is a 8+N byte struct holding 'sz' followed by 'compressed'.
 * 'sz' is byte length of 'compressed' field.
 * 'compressed' is LZF data with total (compressed) length 'sz'
 * NOTE: uncompressed length is stored in quicklistNode->sz.
 * When quicklistNode->entry is compressed, node->entry points to a quicklistLZF
 *
 * quicklistLZF 是一个 8+N 字节的结构体，包含 'sz' 字段后跟 'compressed' 字段。
 * 'sz' 是 'compressed' 字段的字节长度。
 * 'compressed' 是总（压缩）长度为 'sz' 的 LZF 压缩数据。
 * 注：未压缩的长度存储在 quicklistNode->sz 中。
 * 当 quicklistNode->entry 被压缩时，node->entry 指向一个 quicklistLZF */
typedef struct quicklistLZF {
    size_t sz;            /* LZF size in bytes：LZF 数据的字节大小 */
    char compressed[];    // LZF 压缩数据（柔性数组）
} quicklistLZF;

/* Bookmarks are padded with realloc at the end of of the quicklist struct.
 * They should only be used for very big lists if thousands of nodes were the
 * excess memory usage is negligible, and there's a real need to iterate on them
 * in portions.
 * When not used, they don't add any memory overhead, but when used and then
 * deleted, some overhead remains (to avoid resonance).
 * The number of bookmarks used should be kept to minimum since it also adds
 * overhead on node deletion (searching for a bookmark to update).
 *
 * 书签（bookmark）通过 realloc 追加到 quicklist 结构体的末尾。
 * 它们只应该用于非常大的列表（数千个节点时，多余的内存开销可忽略），
 * 并且确实需要分块迭代时才使用。
 * 不使用书签时，不会增加额外的内存开销；但当使用后再删除时，
 * 仍会保留一些开销（以避免共振 realloc 抖动）。
 * 书签的使用数量应保持在最小值，因为它会增加节点删除时的开销
 * （需要搜索相应的书签以更新）。 */
typedef struct quicklistBookmark {
    quicklistNode *node;  // 书签指向的 quicklist 节点
    char *name;           // 书签的名称
} quicklistBookmark;

#if UINTPTR_MAX == 0xffffffff
/* 32-bit：32 位平台 */
/* 32 位系统下 fill 字段可用的位数为 14 */
#   define QL_FILL_BITS 14
/* 32 位系统下 compress 字段可用的位数为 14 */
#   define QL_COMP_BITS 14
/* 32 位系统下 bookmark_count 字段可用的位数为 4 */
#   define QL_BM_BITS 4
#elif UINTPTR_MAX == 0xffffffffffffffff
/* 64-bit：64 位平台 */
/* 64 位系统下 fill 字段可用的位数为 16 */
#   define QL_FILL_BITS 16
/* 64 位系统下 compress 字段可用的位数为 16 */
#   define QL_COMP_BITS 16
/* 64 位系统下 bookmark_count 字段可用的位数为 4。
 * 实际上可以分配更多位，但为了避免性能下降，限制用户使用过多书签。 */
#   define QL_BM_BITS 4 /* we can encode more, but we rather limit the user
                           since they cause performance degradation. */
#else
/* 未知位数平台，直接报错 */
#   error unknown arch bits count
#endif

/* quicklist is a 40 byte struct (on 64-bit systems) describing a quicklist.
 * 'count' is the number of total entries.
 * 'len' is the number of quicklist nodes.
 * 'compress' is: 0 if compression disabled, otherwise it's the number
 *                of quicklistNodes to leave uncompressed at ends of quicklist.
 * 'fill' is the user-requested (or default) fill factor.
 * 'bookmarks are an optional feature that is used by realloc this struct,
 *      so that they don't consume memory when not used.
 *
 * quicklist 是一个 40 字节（在 64 位系统上）的结构体，用于描述整个 quicklist。
 * 'count' 是所有条目的总数。
 * 'len' 是 quicklist 节点的数量。
 * 'compress' 为 0 表示禁用压缩；否则表示在 quicklist 两端保留不压缩的节点数。
 * 'fill' 是用户请求的（或默认的）填充因子。
 * 'bookmarks' 是可选功能，通过 realloc 调整本结构体大小来支持，
 *      因而在未使用时不会占用内存。 */
typedef struct quicklist {
    quicklistNode *head;            // 链表头节点
    quicklistNode *tail;            // 链表尾节点
    unsigned long count;            /* total count of all entries in all listpacks：所有 listpack 中元素的总数 */
    unsigned long len;              /* number of quicklistNodes：quicklist 节点的数量 */
    signed int fill : QL_FILL_BITS;       /* fill factor for individual nodes：单节点的填充因子 */
    unsigned int compress : QL_COMP_BITS; /* depth of end nodes not to compress;0=off：两端不压缩的节点深度，0 表示关闭压缩 */
    unsigned int bookmark_count: QL_BM_BITS; // 已存在的书签数量
    quicklistBookmark bookmarks[];        // 书签数组（柔性数组，使用时按需分配）
} quicklist;

/* quicklist 迭代器结构体 */
typedef struct quicklistIter {
    quicklist *quicklist;     // 所迭代的 quicklist
    quicklistNode *current;   // 当前所在的 quicklist 节点
    unsigned char *zi;        /* points to the current element：指向当前元素的指针 */
    long offset;              /* offset in current listpack：在当前 listpack 中的偏移 */
    int direction;            // 迭代方向（AL_START_HEAD 或 AL_START_TAIL）
} quicklistIter;

/* quicklist 条目结构体，用于描述迭代器当前指向的元素 */
typedef struct quicklistEntry {
    const quicklist *quicklist;   // 所属 quicklist
    quicklistNode *node;          // 元素所在的 quicklist 节点
    unsigned char *zi;            // 元素在 listpack 中的位置指针
    unsigned char *value;         // 元素的值（字符串场景下使用）
    long long longval;            // 元素的长整型值（数值场景下使用）
    size_t sz;                    // 元素的字节大小
    int offset;                   // 元素在 listpack 中的偏移
} quicklistEntry;

/* quicklist 操作位置：头部 */
#define QUICKLIST_HEAD 0
/* quicklist 操作位置：尾部 */
#define QUICKLIST_TAIL -1

/* quicklist node encodings：quicklist 节点的编码方式 */
/* 节点未压缩，使用原始字节保存 */
#define QUICKLIST_NODE_ENCODING_RAW 1
/* 节点已使用 LZF 压缩 */
#define QUICKLIST_NODE_ENCODING_LZF 2

/* quicklist compression disable：禁用 quicklist 压缩的标志值 */
#define QUICKLIST_NOCOMPRESS 0

/* quicklist node container formats：quicklist 节点的容器类型 */
/* 单个元素直接以字符数组存储 */
#define QUICKLIST_NODE_CONTAINER_PLAIN 1
/* 多个元素以 listpack 紧凑存储 */
#define QUICKLIST_NODE_CONTAINER_PACKED 2

/* 判断节点是否为 PLAIN 节点（单元素字符数组） */
#define QL_NODE_IS_PLAIN(node) ((node)->container == QUICKLIST_NODE_CONTAINER_PLAIN)

/* 判断 quicklistNode 是否已使用 LZF 压缩 */
#define quicklistNodeIsCompressed(node)                                        \
    ((node)->encoding == QUICKLIST_NODE_ENCODING_LZF)

/* Prototypes：函数原型声明 */

/* 创建一个空的 quicklist（默认值），使用 quicklistRelease() 释放 */
quicklist *quicklistCreate(void);
/* 创建一个新的 quicklist，并指定 fill 和 compress 参数 */
quicklist *quicklistNew(int fill, int compress);
/* 设置 quicklist 的压缩深度（两端不压缩的节点数） */
void quicklistSetCompressDepth(quicklist *quicklist, int compress);
/* 设置 quicklist 的填充因子 fill */
void quicklistSetFill(quicklist *quicklist, int fill);
/* 同时设置 quicklist 的 fill 和 compress */
void quicklistSetOptions(quicklist *quicklist, int fill, int compress);
/* 释放整个 quicklist，包括所有节点和书签 */
void quicklistRelease(quicklist *quicklist);
/* 向 quicklist 头部插入一个元素；返回 0 表示复用了原头节点，返回 1 表示新建了头节点 */
int quicklistPushHead(quicklist *quicklist, void *value, const size_t sz);
/* 向 quicklist 尾部插入一个元素；返回 0 表示复用了原尾节点，返回 1 表示新建了尾节点 */
int quicklistPushTail(quicklist *quicklist, void *value, const size_t sz);
/* 根据 where 参数将元素插入到 quicklist 头部或尾部 */
void quicklistPush(quicklist *quicklist, void *value, const size_t sz,
                   int where);
/* 追加一个已存在的 listpack 作为 quicklist 的新节点（用于 RDB 加载） */
void quicklistAppendListpack(quicklist *quicklist, unsigned char *zl);
/* 追加一个 PLAIN 节点（单元素）到 quicklist 尾部（用于 RDB 加载） */
void quicklistAppendPlainNode(quicklist *quicklist, unsigned char *data, size_t sz);
/* 在 entry 指向的位置之后插入新元素 */
void quicklistInsertAfter(quicklistIter *iter, quicklistEntry *entry,
                          void *value, const size_t sz);
/* 在 entry 指向的位置之前插入新元素 */
void quicklistInsertBefore(quicklistIter *iter, quicklistEntry *entry,
                           void *value, const size_t sz);
/* 删除 entry 指向的元素，必要时删除所在节点 */
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry);
/* 将 entry 指向的条目替换为新的 data/sz 内容 */
void quicklistReplaceEntry(quicklistIter *iter, quicklistEntry *entry,
                           void *data, size_t sz);
/* 替换 quicklist 指定下标处的条目；返回 1 表示替换成功，返回 0 表示失败（无变化） */
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data,
                            const size_t sz);
/* 删除 quicklist 中从 start 开始的 stop 个元素；返回 1 表示有删除，0 表示无操作 */
int quicklistDelRange(quicklist *quicklist, const long start, const long stop);
/* 创建一个 quicklist 迭代器，direction 指定迭代方向 */
quicklistIter *quicklistGetIterator(quicklist *quicklist, int direction);
/* 创建一个指向指定下标 idx 处的迭代器，direction 为迭代方向 */
quicklistIter *quicklistGetIteratorAtIdx(quicklist *quicklist,
                                         int direction, const long long idx);
/* 创建一个指向指定下标 idx 处元素的迭代器，并填充 entry */
quicklistIter *quicklistGetIteratorEntryAtIdx(quicklist *quicklist, const long long index,
                                              quicklistEntry *entry);
/* 让迭代器前进到下一个元素；返回 0 表示迭代结束 */
int quicklistNext(quicklistIter *iter, quicklistEntry *entry);
/* 重新设置迭代器的迭代方向 */
void quicklistSetDirection(quicklistIter *iter, int direction);
/* 释放迭代器（若当前节点未压缩则进行压缩处理） */
void quicklistReleaseIterator(quicklistIter *iter);
/* 深拷贝 quicklist；返回新分配的 quicklist */
quicklist *quicklistDup(quicklist *orig);
/* 将 quicklist 的尾元素移动到头部 */
void quicklistRotate(quicklist *quicklist);
/* 自定义弹出操作；可指定 saver 回调；返回 0 表示无元素，1 表示成功 */
int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data,
                       size_t *sz, long long *sval,
                       void *(*saver)(unsigned char *data, size_t sz));
/* 默认的弹出操作；返回 0 表示无元素，1 表示成功 */
int quicklistPop(quicklist *quicklist, int where, unsigned char **data,
                 size_t *sz, long long *slong);
/* 返回 quicklist 中所有元素的总数量（缓存值） */
unsigned long quicklistCount(const quicklist *ql);
/* 比较 quicklistEntry 与给定的 p2/p2_len 是否相等 */
int quicklistCompare(quicklistEntry *entry, unsigned char *p2, const size_t p2_len);
/* 提取节点中 LZF 压缩的原始数据；通过 *data 返回指针，返回值为压缩后长度 */
size_t quicklistGetLzf(const quicklistNode *node, void **data);
/* 根据 fill 计算节点的 size 和 count 上限 */
void quicklistNodeLimit(int fill, size_t *size, unsigned int *count);
/* 判断 new_sz/new_count 是否超过 fill 所定义的节点限制；返回 1 表示已超出 */
int quicklistNodeExceedsLimit(int fill, size_t new_sz, unsigned int new_count);
/* 打印 quicklist 调试信息（用于 debugCommand） */
void quicklistRepr(unsigned char *ql, int full);

/* bookmarks：书签相关函数 */
/* 创建或更新一个命名书签；返回 1 表示成功，0 表示书签数量已达上限 */
int quicklistBookmarkCreate(quicklist **ql_ref, const char *name, quicklistNode *node);
/* 删除一个命名书签；返回 1 表示已删除，0 表示未找到 */
int quicklistBookmarkDelete(quicklist *ql, const char *name);
/* 查找一个命名书签对应的 quicklist 节点；未找到返回 NULL */
quicklistNode *quicklistBookmarkFind(quicklist *ql, const char *name);
/* 清除 quicklist 中的所有书签 */
void quicklistBookmarksClear(quicklist *ql);
/* 设置 PLAIN 节点的阈值（仅测试套件使用） */
int quicklistSetPackedThreshold(size_t sz);

#ifdef REDIS_TEST
/* quicklist 的单元测试入口 */
int quicklistTest(int argc, char *argv[], int flags);
#endif

/* Directions for iterators：迭代器方向 */
/* 从表头向表尾迭代 */
#define AL_START_HEAD 0
/* 从表尾向表头迭代 */
#define AL_START_TAIL 1

#endif /* __QUICKLIST_H__ */
