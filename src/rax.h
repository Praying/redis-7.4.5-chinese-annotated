/* Rax -- A radix tree implementation.
 *
 * Copyright (c) 2017-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#ifndef RAX_H
#define RAX_H

#include <stdint.h>

/* 如下是本文件所实现的基数树（radix tree）表示，在依次插入字符串 "foo"、
 * "foobar" 和 "footer" 之后形成的结构。当节点表示基数树中的某个 key 时，
 * 我们用 [] 将其括起来，否则用 () 括起来。
 *
 * 这是原始（未压缩）的表示形式：
 *
 *              (f) ""
 *                \
 *                (o) "f"
 *                  \
 *                  (o) "fo"
 *                    \
 *                  [t   b] "foo"
 *                  /     \
 *         "foot" (e)     (a) "foob"
 *                /         \
 *      "foote" (r)         (r) "fooba"
 *              /             \
 *    "footer" []             [] "foobar"
 *
 * 然而，本实现采用了一种非常常见的优化：连续的、只有一个子节点的节点会被
 * "压缩" 到节点本身中，作为一个字符串，每个字符代表下一级子节点，而只提供
 * 指向表示最后一个字符的节点的链接。因此上面的表示会被转换成：
 *
 *                  ["foo"] ""
 *                     |
 *                  [t   b] "foo"
 *                  /     \
 *        "foot" ("er")    ("ar") "foob"
 *                 /          \
 *       "footer" []          [] "foobar"
 *
 * 不过这种优化也让实现变得稍微复杂一些。例如在上面的基数树中再加入 key
 * "first" 时，就需要执行 "节点分裂"（node splitting）操作，因为 "foo" 前缀
 * 不再是由一个个单子节点连续组成的。下面是该事件发生后产生的节点分裂结果：
 *
 *
 *                    (f) ""
 *                    /
 *                 (i o) "f"
 *                 /   \
 *    "firs"  ("rst")  (o) "fo"
 *              /        \
 *    "first" []       [t   b] "foo"
 *                     /     \
 *           "foot" ("er")    ("ar") "foob"
 *                    /          \
 *          "footer" []          [] "foobar"
 *
 * 类似地，在删除之后，如果又产生了一条只有一个子节点的节点链（该链还不能
 * 包含表示 key 的节点），就必须将其再次压缩回单个节点。
 *
 */

/* 单个 rax 节点允许的最大 size（用于压缩字符串长度） */
#define RAX_NODE_MAX_SIZE ((1<<29)-1)
/* rax 树节点结构体 */
typedef struct raxNode {
    uint32_t iskey:1;     /* 该节点是否包含一个 key */
    uint32_t isnull:1;    /* 关联的 value 是否为 NULL（不存储 value） */
    uint32_t iscompr:1;   /* 该节点是否为压缩节点 */
    uint32_t size:29;     /* 子节点数量，或压缩字符串的长度 */
    /* 数据布局如下：
     *
     * 如果节点未被压缩，则有 'size' 个字节（每个子节点字符一个）以及 'size'
     * 个 raxNode 指针（每个指向一个子节点）。注意字符并不存储在子节点中，
     * 而是存储在父节点的边上：
     *
     * [header iscompr=0][abc][a-ptr][b-ptr][c-ptr](value-ptr?)
     *
     * 如果节点是压缩的（iscompr 位为 1），则该节点只有一个子节点。在这种情况下，
     * 数据段起始处的 'size' 个字节表示一串连续链接的节点序列，但序列中只
     * 有最后一个节点真正被表示为一个节点，并由当前压缩节点指向它。
     *
     * [header iscompr=1][xyz][z-ptr](value-ptr?)
     *
     * 压缩与非压缩节点都可以在基数树的任意层级表示一个带有关联数据的 key
     *（而不只是终端节点）。
     *
     * 如果节点关联了一个 key（iskey=1）且 value 不为 NULL（isnull=0），那么
     * 在指向子节点的 raxNode 指针之后，还会有一个额外的 value 指针（如上面
     * 表示中所示的 "value-ptr" 字段）。
     */
    unsigned char data[];
} raxNode;

/* rax 基数树根结构体 */
typedef struct rax {
    raxNode *head;       /* 头节点（根节点） */
    uint64_t numele;      /* 树中保存的 key 数量 */
    uint64_t numnodes;    /* 树中节点总数 */
    void *metadata[];     /* 用户自定义的元数据 */
} rax;

/* raxLowWalk() 使用的栈数据结构，用于（可选地）向调用者返回父节点列表。
 * 为了节省空间，节点中并不保存 "parent" 字段，因此需要时使用这个辅助栈。 */
#define RAX_STACK_STATIC_ITEMS 32
/* rax 栈结构体 */
typedef struct raxStack {
    void **stack; /* 指向 static_items 或堆上分配的数组 */
    size_t items, maxitems; /* 已包含的元素数量与总容量 */
    /* 最多 RAXSTACK_STACK_ITEMS 个元素时避免在堆上分配，改用这个静态指针数组 */
    void *static_items[RAX_STACK_STATIC_ITEMS];
    int oom; /* 在某次入栈时是否曾因 OOM（内存不足）而失败 */
} raxStack;

/* 迭代器使用的可选回调函数，用于在每个 rax 节点（包括不表示 key 的节点）
 * 上得到通知。如果回调返回 true，表示回调已经修改了迭代器结构中的节点指针，
 * 迭代器的实现需要在基数树内部替换该指针。这允许回调重新分配节点，以执行
 * 普通应用通常不需要的非常特殊的操作。
 *
 * 该回调用于对基数树结构进行非常底层的分析，扫描每一个可能的节点（根节点除外），
 * 或者重新分配节点以减少内存分配的碎片化（这是 Redis 中该回调的应用场景）。
 *
 * 该回调目前仅在正向迭代（raxNext）中支持。 */
typedef int (*raxNodeCallback)(raxNode **noderef);

/* 基数树迭代器的状态被封装在该数据结构中 */
#define RAX_ITER_STATIC_LEN 128
#define RAX_ITER_JUST_SEEKED (1<<0) /* 迭代器刚刚被 seek 过。第一次迭代返回当前
                                       元素，然后清除该标志 */
#define RAX_ITER_EOF (1<<1)    /* 已到达迭代末尾 */
#define RAX_ITER_SAFE (1<<2)   /* 安全迭代器，允许在迭代过程中进行修改操作，
                                  但速度较慢 */
/* rax 迭代器结构体 */
typedef struct raxIterator {
    int flags;               /* 迭代器标志位 */
    rax *rt;                 /* 正在迭代的基数树 */
    unsigned char *key;      /* 当前 key 字符串 */
    void *data;              /* 与当前 key 关联的数据 */
    size_t key_len;          /* 当前 key 的长度 */
    size_t key_max;          /* 当前 key 缓冲区可容纳的最大长度 */
    unsigned char key_static_string[RAX_ITER_STATIC_LEN]; /* 静态 key 缓冲区 */
    raxNode *node;           /* 当前节点，仅用于非安全迭代 */
    raxStack stack;          /* 非安全迭代时使用的栈 */
    raxNodeCallback node_cb; /* 可选的节点回调函数，通常为 NULL */
} raxIterator;

/* 导出的 API 接口 */
rax *raxNew(void);
rax *raxNewWithMetadata(int metaSize);
int raxInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old);
int raxTryInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old);
int raxRemove(rax *rax, unsigned char *s, size_t len, void **old);
int raxFind(rax *rax, unsigned char *s, size_t len, void **value);
void raxFree(rax *rax);
void raxFreeWithCallback(rax *rax, void (*free_callback)(void*));
void raxFreeWithCbAndContext(rax *rax,
                             void (*free_callback)(void *item, void *ctx),
                             void *ctx);
void raxStart(raxIterator *it, rax *rt);
int raxSeek(raxIterator *it, const char *op, unsigned char *ele, size_t len);
int raxNext(raxIterator *it);
int raxPrev(raxIterator *it);
int raxRandomWalk(raxIterator *it, size_t steps);
int raxCompare(raxIterator *iter, const char *op, unsigned char *key, size_t key_len);
void raxStop(raxIterator *it);
int raxEOF(raxIterator *it);
void raxShow(rax *rax);
uint64_t raxSize(rax *rax);
unsigned long raxTouch(raxNode *n);
void raxSetDebugMsg(int onoff);

/* 内部 API：节点回调可能需要以底层方式访问 rax 节点，因此该函数也被导出。 */
void raxSetData(raxNode *n, void *data);

#endif
