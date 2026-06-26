/* Rax -- A radix tree implementation.
 *
 * Copyright (c) 2017-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>
#include "rax.h"
#include "redisassert.h"

#ifndef RAX_MALLOC_INCLUDE
#define RAX_MALLOC_INCLUDE "rax_malloc.h"
#endif

#include RAX_MALLOC_INCLUDE

/* -------------------------------- Debugging ------------------------------ */

/* 调试用：在标准输出中显示给定节点的信息。 */
void raxDebugShowNode(const char *msg, raxNode *n);

/* 通过编译时开启 RAX_DEBUG_MSG 宏来打开/关闭调试消息。
 * 当 RAX_DEBUG_MSG 被定义时，默认情况下 Rax 操作会向标准输出打印大量的调试
 * 信息；不过你仍然可以通过调用 raxSetDebugMsg() 动态地打开或关闭调试消息，
 * 这样可以在你怀疑某个操作引发 bug 时再按需启用。 */
#ifdef RAX_DEBUG_MSG
#define debugf(...)                                                            \
    if (raxDebugMsg) {                                                         \
        printf("%s:%s:%d:\t", __FILE__, __func__, __LINE__);                   \
        printf(__VA_ARGS__);                                                   \
        fflush(stdout);                                                        \
    }

#define debugnode(msg,n) raxDebugShowNode(msg,n)
#else
#define debugf(...)
#define debugnode(msg,n)
#endif

/* 如果定义了 RAX_DEBUG_MSG，默认会输出调试信息。 */
static int raxDebugMsg = 1;

/* 当调试消息被启用时，可以通过该函数动态地打开/关闭它们。默认状态下它们是
 * 启用的；将状态置为 0 可以禁用，置为 1 重新启用。 */
void raxSetDebugMsg(int onoff) {
    raxDebugMsg = onoff;
}

/* ------------------------- raxStack functions --------------------------
 * raxStack 是一个简单的指针栈，在元素数量达到一定阈值后，能够从栈上分配的
 * 静态数组切换到动态堆分配。它用于在遍历基数树时保存父节点列表，从而实现
 * 需要向上回溯树结构的某些操作。
 * ------------------------------------------------------------------------- */

/* 初始化栈。 */
static inline void raxStackInit(raxStack *ts) {
    ts->stack = ts->static_items;
    ts->items = 0;
    ts->maxitems = RAX_STACK_STATIC_ITEMS;
    ts->oom = 0;
}

/* 将一个元素压入栈中，成功返回 1，内存不足时返回 0。 */
static inline int raxStackPush(raxStack *ts, void *ptr) {
    if (ts->items == ts->maxitems) {
        if (ts->stack == ts->static_items) {
            ts->stack = rax_malloc(sizeof(void*)*ts->maxitems*2);
            if (ts->stack == NULL) {
                ts->stack = ts->static_items;
                ts->oom = 1;
                errno = ENOMEM;
                return 0;
            }
            memcpy(ts->stack,ts->static_items,sizeof(void*)*ts->maxitems);
        } else {
            void **newalloc = rax_realloc(ts->stack,sizeof(void*)*ts->maxitems*2);
            if (newalloc == NULL) {
                ts->oom = 1;
                errno = ENOMEM;
                return 0;
            }
            ts->stack = newalloc;
        }
        ts->maxitems *= 2;
    }
    ts->stack[ts->items] = ptr;
    ts->items++;
    return 1;
}

/* 从栈中弹出一个元素，如果没有元素可弹则返回 NULL。 */
static inline void *raxStackPop(raxStack *ts) {
    if (ts->items == 0) return NULL;
    ts->items--;
    return ts->stack[ts->items];
}

/* 返回栈顶元素，但并不实际弹出它。 */
static inline void *raxStackPeek(raxStack *ts) {
    if (ts->items == 0) return NULL;
    return ts->stack[ts->items-1];
}

/* 若栈使用了堆分配，则释放该栈。 */
static inline void raxStackFree(raxStack *ts) {
    if (ts->stack != ts->static_items) rax_free(ts->stack);
}

/* ----------------------------------------------------------------------------
 * Radix tree implementation
 * --------------------------------------------------------------------------*/

/* 返回一个具有 'nodesize' 大小的节点其字符段所需的填充字节数。
 * 这个填充是为了将子节点指针存储到对齐的地址上。注意我们在节点大小上加了 4，
 * 因为节点带有 4 字节的头部。 */
#define raxPadding(nodesize) ((sizeof(void*)-(((nodesize)+4) % sizeof(void*))) & (sizeof(void*)-1))

/* 返回节点中最后一个子节点指针的地址。对于压缩节点，这就是唯一的子节点指针。 */
#define raxNodeLastChildPtr(n) ((raxNode**) ( \
    ((char*)(n)) + \
    raxNodeCurrentLength(n) - \
    sizeof(raxNode*) - \
    (((n)->iskey && !(n)->isnull) ? sizeof(void*) : 0) \
))

/* 返回第一个子节点指针的地址。 */
#define raxNodeFirstChildPtr(n) ((raxNode**) ( \
    (n)->data + \
    (n)->size + \
    raxPadding((n)->size)))

/* 返回节点当前的总长度。注意其中第二行计算的是字符数组之后的填充，
 * 这部分填充是为了让指针存储在对齐的地址上。 */
#define raxNodeCurrentLength(n) ( \
    sizeof(raxNode)+(n)->size+ \
    raxPadding((n)->size)+ \
    ((n)->iscompr ? sizeof(raxNode*) : sizeof(raxNode*)*(n)->size)+ \
    (((n)->iskey && !(n)->isnull)*sizeof(void*)) \
)

/* 分配一个新的非压缩节点，并指定其子节点数量。
 * 如果 datafield 为 true，则分配的空间也会大到足以容纳关联的数据指针。
 * 返回新节点的指针。内存不足时返回 NULL。 */
raxNode *raxNewNode(size_t children, int datafield) {
    size_t nodesize = sizeof(raxNode)+children+raxPadding(children)+
                      sizeof(raxNode*)*children;
    if (datafield) nodesize += sizeof(void*);
    raxNode *node = rax_malloc(nodesize);
    if (node == NULL) return NULL;
    node->iskey = 0;
    node->isnull = 0;
    node->iscompr = 0;
    node->size = children;
    return node;
}

/* 分配一个新的 rax（基数树）并返回其指针。内存不足时函数返回 NULL。 */
rax *raxNew(void) {
    return raxNewWithMetadata(0);
}

/* 分配一个带有自定义元数据的新 rax。 */
rax *raxNewWithMetadata(int metaSize) {
    rax *rax = rax_malloc(sizeof(*rax) + metaSize);
    if (rax == NULL) return NULL;
    rax->numele = 0;
    rax->numnodes = 1;
    rax->head = raxNewNode(0, 0);
    if (rax->head == NULL) {
        rax_free(rax);
        return NULL;
    } else {
        return rax;
    }
}

/* 重新分配节点的空间，以容纳用于存储项的辅助数据字段。
 * 内存不足时返回 NULL。 */
raxNode *raxReallocForData(raxNode *n, void *data) {
    if (data == NULL) return n; /* 不需要重新分配，只需将 isnull 设为 1 */
    size_t curlen = raxNodeCurrentLength(n);
    return rax_realloc(n,curlen+sizeof(void*));
}

/* 将节点的辅助数据设置为指定的指针。 */
void raxSetData(raxNode *n, void *data) {
    n->iskey = 1;
    if (data != NULL) {
        n->isnull = 0;
        void **ndata = (void**)
            ((char*)n+raxNodeCurrentLength(n)-sizeof(void*));
        memcpy(ndata,&data,sizeof(data));
    } else {
        n->isnull = 1;
    }
}

/* 获取节点的辅助数据。 */
void *raxGetData(raxNode *n) {
    if (n->isnull) return NULL;
    void **ndata =(void**)((char*)n+raxNodeCurrentLength(n)-sizeof(void*));
    void *data;
    memcpy(&data,ndata,sizeof(data));
    return data;
}

/* 向节点 'n' 添加一个表示字符 'c' 的新子节点，并通过引用返回它的新指针
 * 以及子节点指针。此外，'***parentlink' 会被填充为指向新子节点存储位置的
 * raxNode 二级指针，这对于在子节点被重新分配时让调用者替换子节点指针非常有用。
 *
 * 成功时返回新的父节点指针（由于 realloc 它可能会改变，因此调用者应丢弃 'n'
 * 并使用新返回值）。内存不足时返回 NULL，并且原节点仍然有效。 */
raxNode *raxAddChild(raxNode *n, unsigned char c, raxNode **childptr, raxNode ***parentlink) {
    assert(n->iscompr == 0);

    size_t curlen = raxNodeCurrentLength(n);
    n->size++;
    size_t newlen = raxNodeCurrentLength(n);
    n->size--; /* For now restore the original size. We'll update it only on
                  success at the end. */

    /* Alloc the new child we will link to 'n'. */
    raxNode *child = raxNewNode(0,0);
    if (child == NULL) return NULL;

    /* Make space in the original node. */
    raxNode *newn = rax_realloc(n,newlen);
    if (newn == NULL) {
        rax_free(child);
        return NULL;
    }
    n = newn;

    /* After the reallocation, we have up to 8/16 (depending on the system
     * pointer size, and the required node padding) bytes at the end, that is,
     * the additional char in the 'data' section, plus one pointer to the new
     * child, plus the padding needed in order to store addresses into aligned
     * locations.
     *
     * So if we start with the following node, having "abde" edges.
     *
     * Note:
     * - We assume 4 bytes pointer for simplicity.
     * - Each space below corresponds to one byte
     *
     * [HDR*][abde][Aptr][Bptr][Dptr][Eptr]|AUXP|
     *
     * After the reallocation we need: 1 byte for the new edge character
     * plus 4 bytes for a new child pointer (assuming 32 bit machine).
     * However after adding 1 byte to the edge char, the header + the edge
     * characters are no longer aligned, so we also need 3 bytes of padding.
     * In total the reallocation will add 1+4+3 bytes = 8 bytes:
     *
     * (Blank bytes are represented by ".")
     *
     * [HDR*][abde][Aptr][Bptr][Dptr][Eptr]|AUXP|[....][....]
     *
     * Let's find where to insert the new child in order to make sure
     * it is inserted in-place lexicographically. Assuming we are adding
     * a child "c" in our case pos will be = 2 after the end of the following
     * loop. */
    int pos;
    for (pos = 0; pos < n->size; pos++) {
        if (n->data[pos] > c) break;
    }

    /* Now, if present, move auxiliary data pointer at the end
     * so that we can mess with the other data without overwriting it.
     * We will obtain something like that:
     *
     * [HDR*][abde][Aptr][Bptr][Dptr][Eptr][....][....]|AUXP|
     */
    unsigned char *src, *dst;
    if (n->iskey && !n->isnull) {
        src = ((unsigned char*)n+curlen-sizeof(void*));
        dst = ((unsigned char*)n+newlen-sizeof(void*));
        memmove(dst,src,sizeof(void*));
    }

    /* Compute the "shift", that is, how many bytes we need to move the
     * pointers section forward because of the addition of the new child
     * byte in the string section. Note that if we had no padding, that
     * would be always "1", since we are adding a single byte in the string
     * section of the node (where now there is "abde" basically).
     *
     * However we have padding, so it could be zero, or up to 8.
     *
     * Another way to think at the shift is, how many bytes we need to
     * move child pointers forward *other than* the obvious sizeof(void*)
     * needed for the additional pointer itself. */
    size_t shift = newlen - curlen - sizeof(void*);

    /* We said we are adding a node with edge 'c'. The insertion
     * point is between 'b' and 'd', so the 'pos' variable value is
     * the index of the first child pointer that we need to move forward
     * to make space for our new pointer.
     *
     * To start, move all the child pointers after the insertion point
     * of shift+sizeof(pointer) bytes on the right, to obtain:
     *
     * [HDR*][abde][Aptr][Bptr][....][....][Dptr][Eptr]|AUXP|
     */
    src = n->data+n->size+
          raxPadding(n->size)+
          sizeof(raxNode*)*pos;
    memmove(src+shift+sizeof(raxNode*),src,sizeof(raxNode*)*(n->size-pos));

    /* Move the pointers to the left of the insertion position as well. Often
     * we don't need to do anything if there was already some padding to use. In
     * that case the final destination of the pointers will be the same, however
     * in our example there was no pre-existing padding, so we added one byte
     * plus three bytes of padding. After the next memmove() things will look
     * like that:
     *
     * [HDR*][abde][....][Aptr][Bptr][....][Dptr][Eptr]|AUXP|
     */
    if (shift) {
        src = (unsigned char*) raxNodeFirstChildPtr(n);
        memmove(src+shift,src,sizeof(raxNode*)*pos);
    }

    /* Now make the space for the additional char in the data section,
     * but also move the pointers before the insertion point to the right
     * by shift bytes, in order to obtain the following:
     *
     * [HDR*][ab.d][e...][Aptr][Bptr][....][Dptr][Eptr]|AUXP|
     */
    src = n->data+pos;
    memmove(src+1,src,n->size-pos);

    /* We can now set the character and its child node pointer to get:
     *
     * [HDR*][abcd][e...][Aptr][Bptr][....][Dptr][Eptr]|AUXP|
     * [HDR*][abcd][e...][Aptr][Bptr][Cptr][Dptr][Eptr]|AUXP|
     */
    n->data[pos] = c;
    n->size++;
    src = (unsigned char*) raxNodeFirstChildPtr(n);
    raxNode **childfield = (raxNode**)(src+sizeof(raxNode*)*pos);
    memcpy(childfield,&child,sizeof(child));
    *childptr = child;
    *parentlink = childfield;
    return n;
}

/* 将节点 'n'（必须是一个没有任何子节点的节点）转换为一个压缩节点，该压缩
 * 节点表示一串逐个链接、且每个节点恰好只有一个子节点的节点序列。节点可以
 * 是一个 key，也可以不是：它的这个属性以及可能存在的关联值都会被保留。
 *
 * 该函数还会返回一个子节点，因为压缩链的最后一个节点不能属于压缩链本身：
 * 它没有子节点，而我们只能压缩恰好只有一个子节点的中间节点。 */
raxNode *raxCompressNode(raxNode *n, unsigned char *s, size_t len, raxNode **child) {
    assert(n->size == 0 && n->iscompr == 0);
    void *data = NULL; /* Initialized only to avoid warnings. */
    size_t newsize;

    debugf("Compress node: %.*s\n", (int)len,s);

    /* Allocate the child to link to this node. */
    *child = raxNewNode(0,0);
    if (*child == NULL) return NULL;

    /* Make space in the parent node. */
    newsize = sizeof(raxNode)+len+raxPadding(len)+sizeof(raxNode*);
    if (n->iskey) {
        data = raxGetData(n); /* To restore it later. */
        if (!n->isnull) newsize += sizeof(void*);
    }
    raxNode *newn = rax_realloc(n,newsize);
    if (newn == NULL) {
        rax_free(*child);
        return NULL;
    }
    n = newn;

    n->iscompr = 1;
    n->size = len;
    memcpy(n->data,s,len);
    if (n->iskey) raxSetData(n,data);
    raxNode **childfield = raxNodeLastChildPtr(n);
    memcpy(childfield,child,sizeof(*child));
    return n;
}

/* 底层函数：在树中查找长度为 'len' 字节的字符串 's'。函数返回已经处理过的
 * key 字符数：如果返回值等于 'len'，则表示已找到对应字符串所在的节点
 * （但该节点未必是 key——比如当 node->iskey 为 0，或者我们停在压缩节点
 *  中间位置导致 'splitpos' 不为零时）。
 *
 * 如果返回值不等于 'len'，则表示在遍历过程中因为字符不匹配而提前终止。
 *
 * 搜索终止时所在的节点（无论是因为完整处理完字符串还是因为提前终止）
 * 会通过引用方式由 '*stopnode' 返回（当传入的指针非 NULL 时）。该节点
 * 在其父节点中的指针链接通过 '*plink' 返回（当非 NULL 时）。最后，如果
 * 搜索在压缩节点中终止，'*splitpos' 会返回终止位置在压缩节点中的下标，
 * 这对于在插入时知道从何处分裂节点非常有用。
 *
 * 注意，当我们因为完全匹配而停在压缩节点中间位置时，本函数会返回一个
 * 等于 'len' 的长度（key 的全部字符都匹配了），同时返回一个始终为正的
 * *splitpos（表示当前压缩节点中最后一个匹配字符之后紧邻的字符的下标）。
 *
 * 而当我们停在压缩节点但 *splitpos 为零时，则表示当前节点本身就表示该
 * key（即不需要压缩节点中的任何字符来表示该 key，仅其全部父节点即可）。 */
static inline size_t raxLowWalk(rax *rax, unsigned char *s, size_t len, raxNode **stopnode, raxNode ***plink, int *splitpos, raxStack *ts) {
    raxNode *h = rax->head;
    raxNode **parentlink = &rax->head;

    size_t i = 0; /* Position in the string. */
    size_t j = 0; /* Position in the node children (or bytes if compressed).*/
    while(h->size && i < len) {
        debugnode("Lookup current node",h);
        unsigned char *v = h->data;

        if (h->iscompr) {
            for (j = 0; j < h->size && i < len; j++, i++) {
                if (v[j] != s[i]) break;
            }
            if (j != h->size) break;
        } else {
            /* Even when h->size is large, linear scan provides good
             * performances compared to other approaches that are in theory
             * more sounding, like performing a binary search. */
            for (j = 0; j < h->size; j++) {
                if (v[j] == s[i]) break;
            }
            if (j == h->size) break;
            i++;
        }

        if (ts) raxStackPush(ts,h); /* Save stack of parent nodes. */
        raxNode **children = raxNodeFirstChildPtr(h);
        if (h->iscompr) j = 0; /* Compressed node only child is at index 0. */
        memcpy(&h,children+j,sizeof(h));
        parentlink = children+j;
        j = 0; /* If the new node is non compressed and we do not
                  iterate again (since i == len) set the split
                  position to 0 to signal this node represents
                  the searched key. */
    }
    debugnode("Lookup stop node is",h);
    if (stopnode) *stopnode = h;
    if (plink) *plink = parentlink;
    if (splitpos && h->iscompr) *splitpos = j;
    return i;
}

/* 插入大小为 'len' 的元素 's'，并将指针 'data' 作为其辅助数据。
 * 如果元素已经存在，则仅当 'overwrite' 设为 1 时才会更新其关联数据，
 * 并返回 0；否则将元素插入并返回 1。内存不足时函数也返回 0，但同时将
 * errno 设为 ENOMEM；其他情况下 errno 会被置为 0。
 */
int raxGenericInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old, int overwrite) {
    size_t i;
    int j = 0; /* Split position. If raxLowWalk() stops in a compressed
                  node, the index 'j' represents the char we stopped within the
                  compressed node, that is, the position where to split the
                  node for insertion. */
    raxNode *h, **parentlink;

    debugf("### Insert %.*s with value %p\n", (int)len, s, data);
    i = raxLowWalk(rax,s,len,&h,&parentlink,&j,NULL);

    /* If i == len we walked following the whole string. If we are not
     * in the middle of a compressed node, the string is either already
     * inserted or this middle node is currently not a key, but can represent
     * our key. We have just to reallocate the node and make space for the
     * data pointer. */
    if (i == len && (!h->iscompr || j == 0 /* not in the middle if j is 0 */)) {
        debugf("### Insert: node representing key exists\n");
        /* Make space for the value pointer if needed. */
        if (!h->iskey || (h->isnull && overwrite)) {
            h = raxReallocForData(h,data);
            if (h) memcpy(parentlink,&h,sizeof(h));
        }
        if (h == NULL) {
            errno = ENOMEM;
            return 0;
        }

        /* Update the existing key if there is already one. */
        if (h->iskey) {
            if (old) *old = raxGetData(h);
            if (overwrite) raxSetData(h,data);
            errno = 0;
            return 0; /* Element already exists. */
        }

        /* Otherwise set the node as a key. Note that raxSetData()
         * will set h->iskey. */
        raxSetData(h,data);
        rax->numele++;
        return 1; /* Element inserted. */
    }

    /* If the node we stopped at is a compressed node, we need to
     * split it before to continue.
     *
     * Splitting a compressed node have a few possible cases.
     * Imagine that the node 'h' we are currently at is a compressed
     * node containing the string "ANNIBALE" (it means that it represents
     * nodes A -> N -> N -> I -> B -> A -> L -> E with the only child
     * pointer of this node pointing at the 'E' node, because remember that
     * we have characters at the edges of the graph, not inside the nodes
     * themselves.
     *
     * In order to show a real case imagine our node to also point to
     * another compressed node, that finally points at the node without
     * children, representing 'O':
     *
     *     "ANNIBALE" -> "SCO" -> []
     *
     * When inserting we may face the following cases. Note that all the cases
     * require the insertion of a non compressed node with exactly two
     * children, except for the last case which just requires splitting a
     * compressed node.
     *
     * 1) Inserting "ANNIENTARE"
     *
     *               |B| -> "ALE" -> "SCO" -> []
     *     "ANNI" -> |-|
     *               |E| -> (... continue algo ...) "NTARE" -> []
     *
     * 2) Inserting "ANNIBALI"
     *
     *                  |E| -> "SCO" -> []
     *     "ANNIBAL" -> |-|
     *                  |I| -> (... continue algo ...) []
     *
     * 3) Inserting "AGO" (Like case 1, but set iscompr = 0 into original node)
     *
     *            |N| -> "NIBALE" -> "SCO" -> []
     *     |A| -> |-|
     *            |G| -> (... continue algo ...) |O| -> []
     *
     * 4) Inserting "CIAO"
     *
     *     |A| -> "NNIBALE" -> "SCO" -> []
     *     |-|
     *     |C| -> (... continue algo ...) "IAO" -> []
     *
     * 5) Inserting "ANNI"
     *
     *     "ANNI" -> "BALE" -> "SCO" -> []
     *
     * The final algorithm for insertion covering all the above cases is as
     * follows.
     *
     * ============================= ALGO 1 =============================
     *
     * For the above cases 1 to 4, that is, all cases where we stopped in
     * the middle of a compressed node for a character mismatch, do:
     *
     * Let $SPLITPOS be the zero-based index at which, in the
     * compressed node array of characters, we found the mismatching
     * character. For example if the node contains "ANNIBALE" and we add
     * "ANNIENTARE" the $SPLITPOS is 4, that is, the index at which the
     * mismatching character is found.
     *
     * 1. Save the current compressed node $NEXT pointer (the pointer to the
     *    child element, that is always present in compressed nodes).
     *
     * 2. Create "split node" having as child the non common letter
     *    at the compressed node. The other non common letter (at the key)
     *    will be added later as we continue the normal insertion algorithm
     *    at step "6".
     *
     * 3a. IF $SPLITPOS == 0:
     *     Replace the old node with the split node, by copying the auxiliary
     *     data if any. Fix parent's reference. Free old node eventually
     *     (we still need its data for the next steps of the algorithm).
     *
     * 3b. IF $SPLITPOS != 0:
     *     Trim the compressed node (reallocating it as well) in order to
     *     contain $splitpos characters. Change child pointer in order to link
     *     to the split node. If new compressed node len is just 1, set
     *     iscompr to 0 (layout is the same). Fix parent's reference.
     *
     * 4a. IF the postfix len (the length of the remaining string of the
     *     original compressed node after the split character) is non zero,
     *     create a "postfix node". If the postfix node has just one character
     *     set iscompr to 0, otherwise iscompr to 1. Set the postfix node
     *     child pointer to $NEXT.
     *
     * 4b. IF the postfix len is zero, just use $NEXT as postfix pointer.
     *
     * 5. Set child[0] of split node to postfix node.
     *
     * 6. Set the split node as the current node, set current index at child[1]
     *    and continue insertion algorithm as usually.
     *
     * ============================= ALGO 2 =============================
     *
     * For case 5, that is, if we stopped in the middle of a compressed
     * node but no mismatch was found, do:
     *
     * Let $SPLITPOS be the zero-based index at which, in the
     * compressed node array of characters, we stopped iterating because
     * there were no more keys character to match. So in the example of
     * the node "ANNIBALE", adding the string "ANNI", the $SPLITPOS is 4.
     *
     * 1. Save the current compressed node $NEXT pointer (the pointer to the
     *    child element, that is always present in compressed nodes).
     *
     * 2. Create a "postfix node" containing all the characters from $SPLITPOS
     *    to the end. Use $NEXT as the postfix node child pointer.
     *    If the postfix node length is 1, set iscompr to 0.
     *    Set the node as a key with the associated value of the new
     *    inserted key.
     *
     * 3. Trim the current node to contain the first $SPLITPOS characters.
     *    As usually if the new node length is just 1, set iscompr to 0.
     *    Take the iskey / associated value as it was in the original node.
     *    Fix the parent's reference.
     *
     * 4. Set the postfix node as the only child pointer of the trimmed
     *    node created at step 1.
     */

    /* ------------------------- ALGORITHM 1 --------------------------- */
    if (h->iscompr && i != len) {
        debugf("ALGO 1: Stopped at compressed node %.*s (%p)\n",
            h->size, h->data, (void*)h);
        debugf("Still to insert: %.*s\n", (int)(len-i), s+i);
        debugf("Splitting at %d: '%c'\n", j, ((char*)h->data)[j]);
        debugf("Other (key) letter is '%c'\n", s[i]);

        /* 1: Save next pointer. */
        raxNode **childfield = raxNodeLastChildPtr(h);
        raxNode *next;
        memcpy(&next,childfield,sizeof(next));
        debugf("Next is %p\n", (void*)next);
        debugf("iskey %d\n", h->iskey);
        if (h->iskey) {
            debugf("key value is %p\n", raxGetData(h));
        }

        /* Set the length of the additional nodes we will need. */
        size_t trimmedlen = j;
        size_t postfixlen = h->size - j - 1;
        int split_node_is_key = !trimmedlen && h->iskey && !h->isnull;
        size_t nodesize;

        /* 2: Create the split node. Also allocate the other nodes we'll need
         *    ASAP, so that it will be simpler to handle OOM. */
        raxNode *splitnode = raxNewNode(1, split_node_is_key);
        raxNode *trimmed = NULL;
        raxNode *postfix = NULL;

        if (trimmedlen) {
            nodesize = sizeof(raxNode)+trimmedlen+raxPadding(trimmedlen)+
                       sizeof(raxNode*);
            if (h->iskey && !h->isnull) nodesize += sizeof(void*);
            trimmed = rax_malloc(nodesize);
        }

        if (postfixlen) {
            nodesize = sizeof(raxNode)+postfixlen+raxPadding(postfixlen)+
                       sizeof(raxNode*);
            postfix = rax_malloc(nodesize);
        }

        /* OOM? Abort now that the tree is untouched. */
        if (splitnode == NULL ||
            (trimmedlen && trimmed == NULL) ||
            (postfixlen && postfix == NULL))
        {
            rax_free(splitnode);
            rax_free(trimmed);
            rax_free(postfix);
            errno = ENOMEM;
            return 0;
        }
        splitnode->data[0] = h->data[j];

        if (j == 0) {
            /* 3a: Replace the old node with the split node. */
            if (h->iskey) {
                void *ndata = raxGetData(h);
                raxSetData(splitnode,ndata);
            }
            memcpy(parentlink,&splitnode,sizeof(splitnode));
        } else {
            /* 3b: Trim the compressed node. */
            trimmed->size = j;
            memcpy(trimmed->data,h->data,j);
            trimmed->iscompr = j > 1 ? 1 : 0;
            trimmed->iskey = h->iskey;
            trimmed->isnull = h->isnull;
            if (h->iskey && !h->isnull) {
                void *ndata = raxGetData(h);
                raxSetData(trimmed,ndata);
            }
            raxNode **cp = raxNodeLastChildPtr(trimmed);
            memcpy(cp,&splitnode,sizeof(splitnode));
            memcpy(parentlink,&trimmed,sizeof(trimmed));
            parentlink = cp; /* Set parentlink to splitnode parent. */
            rax->numnodes++;
        }

        /* 4: Create the postfix node: what remains of the original
         * compressed node after the split. */
        if (postfixlen) {
            /* 4a: create a postfix node. */
            postfix->iskey = 0;
            postfix->isnull = 0;
            postfix->size = postfixlen;
            postfix->iscompr = postfixlen > 1;
            memcpy(postfix->data,h->data+j+1,postfixlen);
            raxNode **cp = raxNodeLastChildPtr(postfix);
            memcpy(cp,&next,sizeof(next));
            rax->numnodes++;
        } else {
            /* 4b: just use next as postfix node. */
            postfix = next;
        }

        /* 5: Set splitnode first child as the postfix node. */
        raxNode **splitchild = raxNodeLastChildPtr(splitnode);
        memcpy(splitchild,&postfix,sizeof(postfix));

        /* 6. Continue insertion: this will cause the splitnode to
         * get a new child (the non common character at the currently
         * inserted key). */
        rax_free(h);
        h = splitnode;
    } else if (h->iscompr && i == len) {
    /* ------------------------- ALGORITHM 2 --------------------------- */
        debugf("ALGO 2: Stopped at compressed node %.*s (%p) j = %d\n",
            h->size, h->data, (void*)h, j);

        /* Allocate postfix & trimmed nodes ASAP to fail for OOM gracefully. */
        size_t postfixlen = h->size - j;
        size_t nodesize = sizeof(raxNode)+postfixlen+raxPadding(postfixlen)+
                          sizeof(raxNode*);
        if (data != NULL) nodesize += sizeof(void*);
        raxNode *postfix = rax_malloc(nodesize);

        nodesize = sizeof(raxNode)+j+raxPadding(j)+sizeof(raxNode*);
        if (h->iskey && !h->isnull) nodesize += sizeof(void*);
        raxNode *trimmed = rax_malloc(nodesize);

        if (postfix == NULL || trimmed == NULL) {
            rax_free(postfix);
            rax_free(trimmed);
            errno = ENOMEM;
            return 0;
        }

        /* 1: Save next pointer. */
        raxNode **childfield = raxNodeLastChildPtr(h);
        raxNode *next;
        memcpy(&next,childfield,sizeof(next));

        /* 2: Create the postfix node. */
        postfix->size = postfixlen;
        postfix->iscompr = postfixlen > 1;
        postfix->iskey = 1;
        postfix->isnull = 0;
        memcpy(postfix->data,h->data+j,postfixlen);
        raxSetData(postfix,data);
        raxNode **cp = raxNodeLastChildPtr(postfix);
        memcpy(cp,&next,sizeof(next));
        rax->numnodes++;

        /* 3: Trim the compressed node. */
        trimmed->size = j;
        trimmed->iscompr = j > 1;
        trimmed->iskey = 0;
        trimmed->isnull = 0;
        memcpy(trimmed->data,h->data,j);
        memcpy(parentlink,&trimmed,sizeof(trimmed));
        if (h->iskey) {
            void *aux = raxGetData(h);
            raxSetData(trimmed,aux);
        }

        /* Fix the trimmed node child pointer to point to
         * the postfix node. */
        cp = raxNodeLastChildPtr(trimmed);
        memcpy(cp,&postfix,sizeof(postfix));

        /* Finish! We don't need to continue with the insertion
         * algorithm for ALGO 2. The key is already inserted. */
        rax->numele++;
        rax_free(h);
        return 1; /* Key inserted. */
    }

    /* We walked the radix tree as far as we could, but still there are left
     * chars in our string. We need to insert the missing nodes. */
    while(i < len) {
        raxNode *child;

        /* If this node is going to have a single child, and there
         * are other characters, so that that would result in a chain
         * of single-childed nodes, turn it into a compressed node. */
        if (h->size == 0 && len-i > 1) {
            debugf("Inserting compressed node\n");
            size_t comprsize = len-i;
            if (comprsize > RAX_NODE_MAX_SIZE)
                comprsize = RAX_NODE_MAX_SIZE;
            raxNode *newh = raxCompressNode(h,s+i,comprsize,&child);
            if (newh == NULL) goto oom;
            h = newh;
            memcpy(parentlink,&h,sizeof(h));
            parentlink = raxNodeLastChildPtr(h);
            i += comprsize;
        } else {
            debugf("Inserting normal node\n");
            raxNode **new_parentlink;
            raxNode *newh = raxAddChild(h,s[i],&child,&new_parentlink);
            if (newh == NULL) goto oom;
            h = newh;
            memcpy(parentlink,&h,sizeof(h));
            parentlink = new_parentlink;
            i++;
        }
        rax->numnodes++;
        h = child;
    }
    raxNode *newh = raxReallocForData(h,data);
    if (newh == NULL) goto oom;
    h = newh;
    if (!h->iskey) rax->numele++;
    raxSetData(h,data);
    memcpy(parentlink,&h,sizeof(h));
    return 1; /* Element inserted. */

oom:
    /* This code path handles out of memory after part of the sub-tree was
     * already modified. Set the node as a key, and then remove it. However we
     * do that only if the node is a terminal node, otherwise if the OOM
     * happened reallocating a node in the middle, we don't need to free
     * anything. */
    if (h->size == 0) {
        h->isnull = 1;
        h->iskey = 1;
        rax->numele++; /* Compensate the next remove. */
        assert(raxRemove(rax,s,i,NULL) != 0);
    }
    errno = ENOMEM;
    return 0;
}

/* 覆盖式插入：仅是 raxGenericInsert() 的一个包装，当存在相同 key 时
 * 会更新元素。 */
int raxInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old) {
    return raxGenericInsert(rax,s,len,data,old,1);
}

/* 非覆盖式插入函数：当存在相同 key 的元素时，不会更新其值，且函数返回 0。
 * 同样只是 raxGenericInsert() 的一个包装。 */
int raxTryInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old) {
    return raxGenericInsert(rax,s,len,data,old,0);
}

/* 在 rax 中查找某个 key：找到则返回 1，否则返回 0。
 * 如果找到对应项且传入的 'value' 是非 NULL 指针，则会在该地址写入与
 * 该 key 关联的值。 */
int raxFind(rax *rax, unsigned char *s, size_t len, void **value) {
    raxNode *h;

    debugf("### Lookup: %.*s\n", (int)len, s);
    int splitpos = 0;
    size_t i = raxLowWalk(rax,s,len,&h,NULL,&splitpos,NULL);
    if (i != len || (h->iscompr && splitpos != 0) || !h->iskey)
        return 0;
    if (value != NULL) *value = raxGetData(h);
    return 1;
}

/* 返回 'parent' 节点中存储指定 'child' 指针的内存地址，以便调用者在需要时
 * 用另一个指针更新它。该函数假定能够找到匹配项，否则属于未定义行为（它将
 * 在没有任何边界检查的情况下继续扫描内存）。 */
raxNode **raxFindParentLink(raxNode *parent, raxNode *child) {
    raxNode **cp = raxNodeFirstChildPtr(parent);
    raxNode *c;
    while(1) {
        memcpy(&c,cp,sizeof(c));
        if (c == child) break;
        cp++;
    }
    return cp;
}

/* 底层的子节点移除函数。返回移除子节点之后的节点指针。注意本函数不会
 * 修复父节点在其祖父节点中的指针，因此这一任务由调用者负责。
 * 该函数永远不会因内存不足而失败。 */
raxNode *raxRemoveChild(raxNode *parent, raxNode *child) {
    debugnode("raxRemoveChild before", parent);
    /* If parent is a compressed node (having a single child, as for definition
     * of the data structure), the removal of the child consists into turning
     * it into a normal node without children. */
    if (parent->iscompr) {
        void *data = NULL;
        if (parent->iskey) data = raxGetData(parent);
        parent->isnull = 0;
        parent->iscompr = 0;
        parent->size = 0;
        if (parent->iskey) raxSetData(parent,data);
        debugnode("raxRemoveChild after", parent);
        return parent;
    }

    /* Otherwise we need to scan for the child pointer and memmove()
     * accordingly.
     *
     * 1. To start we seek the first element in both the children
     *    pointers and edge bytes in the node. */
    raxNode **cp = raxNodeFirstChildPtr(parent);
    raxNode **c = cp;
    unsigned char *e = parent->data;

    /* 2. Search the child pointer to remove inside the array of children
     *    pointers. */
    while(1) {
        raxNode *aux;
        memcpy(&aux,c,sizeof(aux));
        if (aux == child) break;
        c++;
        e++;
    }

    /* 3. Remove the edge and the pointer by memmoving the remaining children
     *    pointer and edge bytes one position before. */
    int taillen = parent->size - (e - parent->data) - 1;
    debugf("raxRemoveChild tail len: %d\n", taillen);
    memmove(e,e+1,taillen);

    /* Compute the shift, that is the amount of bytes we should move our
     * child pointers to the left, since the removal of one edge character
     * and the corresponding padding change, may change the layout.
     * We just check if in the old version of the node there was at the
     * end just a single byte and all padding: in that case removing one char
     * will remove a whole sizeof(void*) word. */
    size_t shift = ((parent->size+4) % sizeof(void*)) == 1 ? sizeof(void*) : 0;

    /* Move the children pointers before the deletion point. */
    if (shift)
        memmove(((char*)cp)-shift,cp,(parent->size-taillen-1)*sizeof(raxNode**));

    /* Move the remaining "tail" pointers at the right position as well. */
    size_t valuelen = (parent->iskey && !parent->isnull) ? sizeof(void*) : 0;
    memmove(((char*)c)-shift,c+1,taillen*sizeof(raxNode**)+valuelen);

    /* 4. Update size. */
    parent->size--;

    /* realloc the node according to the theoretical memory usage, to free
     * data if we are over-allocating right now. */
    raxNode *newnode = rax_realloc(parent,raxNodeCurrentLength(parent));
    if (newnode) {
        debugnode("raxRemoveChild after", newnode);
    }
    /* Note: if rax_realloc() fails we just return the old address, which
     * is valid. */
    return newnode ? newnode : parent;
}

/* 删除指定的项。如果该项被找到并删除则返回 1，否则返回 0。 */
int raxRemove(rax *rax, unsigned char *s, size_t len, void **old) {
    raxNode *h;
    raxStack ts;

    debugf("### Delete: %.*s\n", (int)len, s);
    raxStackInit(&ts);
    int splitpos = 0;
    size_t i = raxLowWalk(rax,s,len,&h,NULL,&splitpos,&ts);
    if (i != len || (h->iscompr && splitpos != 0) || !h->iskey) {
        raxStackFree(&ts);
        return 0;
    }
    if (old) *old = raxGetData(h);
    h->iskey = 0;
    rax->numele--;

    /* If this node has no children, the deletion needs to reclaim the
     * no longer used nodes. This is an iterative process that needs to
     * walk the three upward, deleting all the nodes with just one child
     * that are not keys, until the head of the rax is reached or the first
     * node with more than one child is found. */

    int trycompress = 0; /* Will be set to 1 if we should try to optimize the
                            tree resulting from the deletion. */

    if (h->size == 0) {
        debugf("Key deleted in node without children. Cleanup needed.\n");
        raxNode *child = NULL;
        while(h != rax->head) {
            child = h;
            debugf("Freeing child %p [%.*s] key:%d\n", (void*)child,
                (int)child->size, (char*)child->data, child->iskey);
            rax_free(child);
            rax->numnodes--;
            h = raxStackPop(&ts);
             /* If this node has more then one child, or actually holds
              * a key, stop here. */
            if (h->iskey || (!h->iscompr && h->size != 1)) break;
        }
        if (child) {
            debugf("Unlinking child %p from parent %p\n",
                (void*)child, (void*)h);
            raxNode *new = raxRemoveChild(h,child);
            if (new != h) {
                raxNode *parent = raxStackPeek(&ts);
                raxNode **parentlink;
                if (parent == NULL) {
                    parentlink = &rax->head;
                } else {
                    parentlink = raxFindParentLink(parent,h);
                }
                memcpy(parentlink,&new,sizeof(new));
            }

            /* If after the removal the node has just a single child
             * and is not a key, we need to try to compress it. */
            if (new->size == 1 && new->iskey == 0) {
                trycompress = 1;
                h = new;
            }
        }
    } else if (h->size == 1) {
        /* If the node had just one child, after the removal of the key
         * further compression with adjacent nodes is potentially possible. */
        trycompress = 1;
    }

    /* Don't try node compression if our nodes pointers stack is not
     * complete because of OOM while executing raxLowWalk() */
    if (trycompress && ts.oom) trycompress = 0;

    /* Recompression: if trycompress is true, 'h' points to a radix tree node
     * that changed in a way that could allow to compress nodes in this
     * sub-branch. Compressed nodes represent chains of nodes that are not
     * keys and have a single child, so there are two deletion events that
     * may alter the tree so that further compression is needed:
     *
     * 1) A node with a single child was a key and now no longer is a key.
     * 2) A node with two children now has just one child.
     *
     * We try to navigate upward till there are other nodes that can be
     * compressed, when we reach the upper node which is not a key and has
     * a single child, we scan the chain of children to collect the
     * compressible part of the tree, and replace the current node with the
     * new one, fixing the child pointer to reference the first non
     * compressible node.
     *
     * Example of case "1". A tree stores the keys "FOO" = 1 and
     * "FOOBAR" = 2:
     *
     *
     * "FOO" -> "BAR" -> [] (2)
     *           (1)
     *
     * After the removal of "FOO" the tree can be compressed as:
     *
     * "FOOBAR" -> [] (2)
     *
     *
     * Example of case "2". A tree stores the keys "FOOBAR" = 1 and
     * "FOOTER" = 2:
     *
     *          |B| -> "AR" -> [] (1)
     * "FOO" -> |-|
     *          |T| -> "ER" -> [] (2)
     *
     * After the removal of "FOOTER" the resulting tree is:
     *
     * "FOO" -> |B| -> "AR" -> [] (1)
     *
     * That can be compressed into:
     *
     * "FOOBAR" -> [] (1)
     */
    if (trycompress) {
        debugf("After removing %.*s:\n", (int)len, s);
        debugnode("Compression may be needed",h);
        debugf("Seek start node\n");

        /* Try to reach the upper node that is compressible.
         * At the end of the loop 'h' will point to the first node we
         * can try to compress and 'parent' to its parent. */
        raxNode *parent;
        while(1) {
            parent = raxStackPop(&ts);
            if (!parent || parent->iskey ||
                (!parent->iscompr && parent->size != 1)) break;
            h = parent;
            debugnode("Going up to",h);
        }
        raxNode *start = h; /* Compression starting node. */

        /* Scan chain of nodes we can compress. */
        size_t comprsize = h->size;
        int nodes = 1;
        while(h->size != 0) {
            raxNode **cp = raxNodeLastChildPtr(h);
            memcpy(&h,cp,sizeof(h));
            if (h->iskey || (!h->iscompr && h->size != 1)) break;
            /* Stop here if going to the next node would result into
             * a compressed node larger than h->size can hold. */
            if (comprsize + h->size > RAX_NODE_MAX_SIZE) break;
            nodes++;
            comprsize += h->size;
        }
        if (nodes > 1) {
            /* If we can compress, create the new node and populate it. */
            size_t nodesize =
                sizeof(raxNode)+comprsize+raxPadding(comprsize)+sizeof(raxNode*);
            raxNode *new = rax_malloc(nodesize);
            /* An out of memory here just means we cannot optimize this
             * node, but the tree is left in a consistent state. */
            if (new == NULL) {
                raxStackFree(&ts);
                return 1;
            }
            new->iskey = 0;
            new->isnull = 0;
            new->iscompr = 1;
            new->size = comprsize;
            rax->numnodes++;

            /* Scan again, this time to populate the new node content and
             * to fix the new node child pointer. At the same time we free
             * all the nodes that we'll no longer use. */
            comprsize = 0;
            h = start;
            while(h->size != 0) {
                memcpy(new->data+comprsize,h->data,h->size);
                comprsize += h->size;
                raxNode **cp = raxNodeLastChildPtr(h);
                raxNode *tofree = h;
                memcpy(&h,cp,sizeof(h));
                rax_free(tofree); rax->numnodes--;
                if (h->iskey || (!h->iscompr && h->size != 1)) break;
            }
            debugnode("New node",new);

            /* Now 'h' points to the first node that we still need to use,
             * so our new node child pointer will point to it. */
            raxNode **cp = raxNodeLastChildPtr(new);
            memcpy(cp,&h,sizeof(h));

            /* Fix parent link. */
            if (parent) {
                raxNode **parentlink = raxFindParentLink(parent,start);
                memcpy(parentlink,&new,sizeof(new));
            } else {
                rax->head = new;
            }

            debugf("Compressed %d nodes, %d total bytes\n",
                nodes, (int)comprsize);
        }
    }
    raxStackFree(&ts);
    return 1;
}

/* raxFree() 的核心：以深度优先的方式扫描树，并释放所有找到的节点。 */
void raxRecursiveFree(rax *rax, raxNode *n, void (*free_callback)(void*)) {
    debugnode("free traversing",n);
    int numchildren = n->iscompr ? 1 : n->size;
    raxNode **cp = raxNodeLastChildPtr(n);
    while(numchildren--) {
        raxNode *child;
        memcpy(&child,cp,sizeof(child));
        raxRecursiveFree(rax,child,free_callback);
        cp--;
    }
    debugnode("free depth-first",n);
    if (free_callback && n->iskey && !n->isnull)
        free_callback(raxGetData(n));
    rax_free(n);
    rax->numnodes--;
}

/* 与 raxRecursiveFree() 相同，但带有 context 参数。 */
void raxRecursiveFreeWithCtx(rax *rax, raxNode *n,
                            void (*free_callback)(void *item, void *ctx), void *ctx) {
    debugnode("free traversing",n);
    int numchildren = n->iscompr ? 1 : n->size;
    raxNode **cp = raxNodeLastChildPtr(n);
    while(numchildren--) {
        raxNode *child;
        memcpy(&child,cp,sizeof(child));
        raxRecursiveFreeWithCtx(rax,child,free_callback, ctx);
        cp--;
    }
    debugnode("free depth-first",n);
    if (free_callback && n->iskey && !n->isnull)
        free_callback(raxGetData(n), ctx);
    rax_free(n);
    rax->numnodes--;
}

/* 释放整棵基数树，并通过指定的回调函数来释放关联的辅助数据。 */
void raxFreeWithCallback(rax *rax, void (*free_callback)(void*)) {
    raxRecursiveFree(rax,rax->head,free_callback);
    assert(rax->numnodes == 0);
    rax_free(rax);
}

/* 释放整棵基数树，并通过指定的回调函数来释放关联的辅助数据。 */
void raxFreeWithCbAndContext(rax *rax,
                             void (*free_callback)(void *item, void *ctx), void *ctx) {
    raxRecursiveFreeWithCtx(rax,rax->head,free_callback,ctx);
    assert(rax->numnodes == 0);
    rax_free(rax);
}

/* 释放整棵基数树。 */
void raxFree(rax *rax) {
    raxFreeWithCallback(rax,NULL);
}

/* ------------------------------- Iterator --------------------------------- */

/* 初始化一个 Rax 迭代器。该调用应当只执行一次以完成迭代器的初始化，并且
 * 必须紧跟着调用一次 raxSeek()，否则 raxPrev()/raxNext() 只会返回 EOF。 */
void raxStart(raxIterator *it, rax *rt) {
    it->flags = RAX_ITER_EOF; /* No crash if the iterator is not seeked. */
    it->rt = rt;
    it->key_len = 0;
    it->key = it->key_static_string;
    it->key_max = RAX_ITER_STATIC_LEN;
    it->data = NULL;
    it->node_cb = NULL;
    raxStackInit(&it->stack);
}

/* 在迭代器 'it' 的当前 key 字符串末尾追加若干字符。这是用于实现迭代器的
 * 底层函数，不对用户开放。内存不足时返回 0，否则返回 1。 */
int raxIteratorAddChars(raxIterator *it, unsigned char *s, size_t len) {
    if (len == 0) return 1;
    if (it->key_max < it->key_len+len) {
        unsigned char *old = (it->key == it->key_static_string) ? NULL :
                                                                  it->key;
        size_t new_max = (it->key_len+len)*2;
        it->key = rax_realloc(old,new_max);
        if (it->key == NULL) {
            it->key = (!old) ? it->key_static_string : old;
            errno = ENOMEM;
            return 0;
        }
        if (old == NULL) memcpy(it->key,it->key_static_string,it->key_len);
        it->key_max = new_max;
    }
    /* Use memmove since there could be an overlap between 's' and
     * it->key when we use the current key in order to re-seek. */
    memmove(it->key+it->key_len,s,len);
    it->key_len += len;
    return 1;
}

/* 从迭代器当前 key 的右侧移除指定数量的字符。 */
void raxIteratorDelChars(raxIterator *it, size_t count) {
    it->key_len -= count;
}

/* 向下一个元素执行一次迭代步骤。步骤结束时，迭代器的 key 将表示新的当前 key。
 * 如果由于已没有更多元素而无法再向指定方向前进，则将迭代器标记为 RAX_ITER_EOF。
 *
 * 如果 'noup' 为 true，函数会直接从当前节点开始扫描字典序更小的子节点，
 * 并假定当前节点已经是最后一个 key 节点的父节点，因此会跳过第一次回到
 * 父节点的操作。该选项被 raxSeek() 在使用 ">" 或 "<" 操作符定位一个
 * 不存在的元素时使用：在那种情况下起始节点并不是一个 key，因此我们从一个
 * 不表示已设置 key 的节点开始扫描。
 *
 * 函数成功时返回 1，内存不足时返回 0。 */
int raxIteratorNextStep(raxIterator *it, int noup) {
    if (it->flags & RAX_ITER_EOF) {
        return 1;
    } else if (it->flags & RAX_ITER_JUST_SEEKED) {
        it->flags &= ~RAX_ITER_JUST_SEEKED;
        return 1;
    }

    /* Save key len, stack items and the node where we are currently
     * so that on iterator EOF we can restore the current key and state. */
    size_t orig_key_len = it->key_len;
    size_t orig_stack_items = it->stack.items;
    raxNode *orig_node = it->node;

    while(1) {
        int children = it->node->iscompr ? 1 : it->node->size;
        if (!noup && children) {
            debugf("GO DEEPER\n");
            /* Seek the lexicographically smaller key in this subtree, which
             * is the first one found always going towards the first child
             * of every successive node. */
            if (!raxStackPush(&it->stack,it->node)) return 0;
            raxNode **cp = raxNodeFirstChildPtr(it->node);
            if (!raxIteratorAddChars(it,it->node->data,
                it->node->iscompr ? it->node->size : 1)) return 0;
            memcpy(&it->node,cp,sizeof(it->node));
            /* Call the node callback if any, and replace the node pointer
             * if the callback returns true. */
            if (it->node_cb && it->node_cb(&it->node))
                memcpy(cp,&it->node,sizeof(it->node));
            /* For "next" step, stop every time we find a key along the
             * way, since the key is lexicographically smaller compared to
             * what follows in the sub-children. */
            if (it->node->iskey) {
                it->data = raxGetData(it->node);
                return 1;
            }
        } else {
            /* If we finished exploring the previous sub-tree, switch to the
             * new one: go upper until a node is found where there are
             * children representing keys lexicographically greater than the
             * current key. */
            while(1) {
                int old_noup = noup;

                /* Already on head? Can't go up, iteration finished. */
                if (!noup && it->node == it->rt->head) {
                    it->flags |= RAX_ITER_EOF;
                    it->stack.items = orig_stack_items;
                    it->key_len = orig_key_len;
                    it->node = orig_node;
                    return 1;
                }
                /* If there are no children at the current node, try parent's
                 * next child. */
                unsigned char prevchild = it->key[it->key_len-1];
                if (!noup) {
                    it->node = raxStackPop(&it->stack);
                } else {
                    noup = 0;
                }
                /* Adjust the current key to represent the node we are
                 * at. */
                int todel = it->node->iscompr ? it->node->size : 1;
                raxIteratorDelChars(it,todel);

                /* Try visiting the next child if there was at least one
                 * additional child. */
                if (!it->node->iscompr && it->node->size > (old_noup ? 0 : 1)) {
                    raxNode **cp = raxNodeFirstChildPtr(it->node);
                    int i = 0;
                    while (i < it->node->size) {
                        debugf("SCAN NEXT %c\n", it->node->data[i]);
                        if (it->node->data[i] > prevchild) break;
                        i++;
                        cp++;
                    }
                    if (i != it->node->size) {
                        debugf("SCAN found a new node\n");
                        raxIteratorAddChars(it,it->node->data+i,1);
                        if (!raxStackPush(&it->stack,it->node)) return 0;
                        memcpy(&it->node,cp,sizeof(it->node));
                        /* Call the node callback if any, and replace the node
                         * pointer if the callback returns true. */
                        if (it->node_cb && it->node_cb(&it->node))
                            memcpy(cp,&it->node,sizeof(it->node));
                        if (it->node->iskey) {
                            it->data = raxGetData(it->node);
                            return 1;
                        }
                        break;
                    }
                }
            }
        }
    }
}

/* 在当前节点所在的子树中查找最大的 key。内存不足时返回 0，否则返回 1。
 * 这是下方不同迭代函数的辅助函数。 */
int raxSeekGreatest(raxIterator *it) {
    while(it->node->size) {
        if (it->node->iscompr) {
            if (!raxIteratorAddChars(it,it->node->data,
                it->node->size)) return 0;
        } else {
            if (!raxIteratorAddChars(it,it->node->data+it->node->size-1,1))
                return 0;
        }
        raxNode **cp = raxNodeLastChildPtr(it->node);
        if (!raxStackPush(&it->stack,it->node)) return 0;
        memcpy(&it->node,cp,sizeof(it->node));
    }
    return 1;
}

/* 类似 raxIteratorNextStep()，但实现的是向字典序前一个元素迭代的步骤。
 * 'noup' 选项的效果与 raxIteratorNextStep() 中类似。 */
int raxIteratorPrevStep(raxIterator *it, int noup) {
    if (it->flags & RAX_ITER_EOF) {
        return 1;
    } else if (it->flags & RAX_ITER_JUST_SEEKED) {
        it->flags &= ~RAX_ITER_JUST_SEEKED;
        return 1;
    }

    /* Save key len, stack items and the node where we are currently
     * so that on iterator EOF we can restore the current key and state. */
    size_t orig_key_len = it->key_len;
    size_t orig_stack_items = it->stack.items;
    raxNode *orig_node = it->node;

    while(1) {
        int old_noup = noup;

        /* Already on head? Can't go up, iteration finished. */
        if (!noup && it->node == it->rt->head) {
            it->flags |= RAX_ITER_EOF;
            it->stack.items = orig_stack_items;
            it->key_len = orig_key_len;
            it->node = orig_node;
            return 1;
        }

        unsigned char prevchild = it->key[it->key_len-1];
        if (!noup) {
            it->node = raxStackPop(&it->stack);
        } else {
            noup = 0;
        }

        /* Adjust the current key to represent the node we are
         * at. */
        int todel = it->node->iscompr ? it->node->size : 1;
        raxIteratorDelChars(it,todel);

        /* Try visiting the prev child if there is at least one
         * child. */
        if (!it->node->iscompr && it->node->size > (old_noup ? 0 : 1)) {
            raxNode **cp = raxNodeLastChildPtr(it->node);
            int i = it->node->size-1;
            while (i >= 0) {
                debugf("SCAN PREV %c\n", it->node->data[i]);
                if (it->node->data[i] < prevchild) break;
                i--;
                cp--;
            }
            /* If we found a new subtree to explore in this node,
             * go deeper following all the last children in order to
             * find the key lexicographically greater. */
            if (i != -1) {
                debugf("SCAN found a new node\n");
                /* Enter the node we just found. */
                if (!raxIteratorAddChars(it,it->node->data+i,1)) return 0;
                if (!raxStackPush(&it->stack,it->node)) return 0;
                memcpy(&it->node,cp,sizeof(it->node));
                /* Seek sub-tree max. */
                if (!raxSeekGreatest(it)) return 0;
            }
        }

        /* Return the key: this could be the key we found scanning a new
         * subtree, or if we did not find a new subtree to explore here,
         * before giving up with this node, check if it's a key itself. */
        if (it->node->iskey) {
            it->data = raxGetData(it->node);
            return 1;
        }
    }
}

/* 将迭代器定位到指定元素处。
 * 如果因为语法错误或内存不足导致定位失败则返回 0，否则返回 1。
 * 当因为内存不足而返回 0 时，errno 会被设置为 ENOMEM。 */
int raxSeek(raxIterator *it, const char *op, unsigned char *ele, size_t len) {
    int eq = 0, lt = 0, gt = 0, first = 0, last = 0;

    it->stack.items = 0; /* Just resetting. Initialized by raxStart(). */
    it->flags |= RAX_ITER_JUST_SEEKED;
    it->flags &= ~RAX_ITER_EOF;
    it->key_len = 0;
    it->node = NULL;

    /* Set flags according to the operator used to perform the seek. */
    if (op[0] == '>') {
        gt = 1;
        if (op[1] == '=') eq = 1;
    } else if (op[0] == '<') {
        lt = 1;
        if (op[1] == '=') eq = 1;
    } else if (op[0] == '=') {
        eq = 1;
    } else if (op[0] == '^') {
        first = 1;
    } else if (op[0] == '$') {
        last = 1;
    } else {
        errno = 0;
        return 0; /* Error. */
    }

    /* If there are no elements, set the EOF condition immediately and
     * return. */
    if (it->rt->numele == 0) {
        it->flags |= RAX_ITER_EOF;
        return 1;
    }

    if (first) {
        /* Seeking the first key greater or equal to the empty string
         * is equivalent to seeking the smaller key available. */
        return raxSeek(it,">=",NULL,0);
    }

    if (last) {
        /* Find the greatest key taking always the last child till a
         * final node is found. */
        it->node = it->rt->head;
        if (!raxSeekGreatest(it)) return 0;
        assert(it->node->iskey);
        it->data = raxGetData(it->node);
        return 1;
    }

    /* We need to seek the specified key. What we do here is to actually
     * perform a lookup, and later invoke the prev/next key code that
     * we already use for iteration. */
    int splitpos = 0;
    size_t i = raxLowWalk(it->rt,ele,len,&it->node,NULL,&splitpos,&it->stack);

    /* Return OOM on incomplete stack info. */
    if (it->stack.oom) return 0;

    if (eq && i == len && (!it->node->iscompr || splitpos == 0) &&
        it->node->iskey)
    {
        /* We found our node, since the key matches and we have an
         * "equal" condition. */
        if (!raxIteratorAddChars(it,ele,len)) return 0; /* OOM. */
        it->data = raxGetData(it->node);
    } else if (lt || gt) {
        /* Exact key not found or eq flag not set. We have to set as current
         * key the one represented by the node we stopped at, and perform
         * a next/prev operation to seek. */
        raxIteratorAddChars(it, ele, i-splitpos);

        /* We need to set the iterator in the correct state to call next/prev
         * step in order to seek the desired element. */
        debugf("After initial seek: i=%d len=%d key=%.*s\n",
            (int)i, (int)len, (int)it->key_len, it->key);
        if (i != len && !it->node->iscompr) {
            /* If we stopped in the middle of a normal node because of a
             * mismatch, add the mismatching character to the current key
             * and call the iterator with the 'noup' flag so that it will try
             * to seek the next/prev child in the current node directly based
             * on the mismatching character. */
            if (!raxIteratorAddChars(it,ele+i,1)) return 0;
            debugf("Seek normal node on mismatch: %.*s\n",
                (int)it->key_len, (char*)it->key);

            it->flags &= ~RAX_ITER_JUST_SEEKED;
            if (lt && !raxIteratorPrevStep(it,1)) return 0;
            if (gt && !raxIteratorNextStep(it,1)) return 0;
            it->flags |= RAX_ITER_JUST_SEEKED; /* Ignore next call. */
        } else if (i != len && it->node->iscompr) {
            debugf("Compressed mismatch: %.*s\n",
                (int)it->key_len, (char*)it->key);
            /* In case of a mismatch within a compressed node. */
            int nodechar = it->node->data[splitpos];
            int keychar = ele[i];
            it->flags &= ~RAX_ITER_JUST_SEEKED;
            if (gt) {
                /* If the key the compressed node represents is greater
                 * than our seek element, continue forward, otherwise set the
                 * state in order to go back to the next sub-tree. */
                if (nodechar > keychar) {
                    if (!raxIteratorNextStep(it,0)) return 0;
                } else {
                    if (!raxIteratorAddChars(it,it->node->data,it->node->size))
                        return 0;
                    if (!raxIteratorNextStep(it,1)) return 0;
                }
            }
            if (lt) {
                /* If the key the compressed node represents is smaller
                 * than our seek element, seek the greater key in this
                 * subtree, otherwise set the state in order to go back to
                 * the previous sub-tree. */
                if (nodechar < keychar) {
                    if (!raxSeekGreatest(it)) return 0;
                    it->data = raxGetData(it->node);
                } else {
                    if (!raxIteratorAddChars(it,it->node->data,it->node->size))
                        return 0;
                    if (!raxIteratorPrevStep(it,1)) return 0;
                }
            }
            it->flags |= RAX_ITER_JUST_SEEKED; /* Ignore next call. */
        } else {
            debugf("No mismatch: %.*s\n",
                (int)it->key_len, (char*)it->key);
            /* If there was no mismatch we are into a node representing the
             * key, (but which is not a key or the seek operator does not
             * include 'eq'), or we stopped in the middle of a compressed node
             * after processing all the key. Continue iterating as this was
             * a legitimate key we stopped at. */
            it->flags &= ~RAX_ITER_JUST_SEEKED;
            if (it->node->iscompr && it->node->iskey && splitpos && lt) {
                /* If we stopped in the middle of a compressed node with
                 * perfect match, and the condition is to seek a key "<" than
                 * the specified one, then if this node is a key it already
                 * represents our match. For instance we may have nodes:
                 *
                 * "f" -> "oobar" = 1 -> "" = 2
                 *
                 * Representing keys "f" = 1, "foobar" = 2. A seek for
                 * the key < "foo" will stop in the middle of the "oobar"
                 * node, but will be our match, representing the key "f".
                 *
                 * So in that case, we don't seek backward. */
                it->data = raxGetData(it->node);
            } else {
                if (gt && !raxIteratorNextStep(it,0)) return 0;
                if (lt && !raxIteratorPrevStep(it,0)) return 0;
            }
            it->flags |= RAX_ITER_JUST_SEEKED; /* Ignore next call. */
        }
    } else {
        /* If we are here just eq was set but no match was found. */
        it->flags |= RAX_ITER_EOF;
        return 1;
    }
    return 1;
}

/* 在迭代器 'it' 范围内前进到下一个元素。
 * 如果达到 EOF（或内存不足），则返回 0，否则返回 1。
 * 如果是因为 OOM 而返回 0，则 errno 会被设置为 ENOMEM。 */
int raxNext(raxIterator *it) {
    if (!raxIteratorNextStep(it,0)) {
        errno = ENOMEM;
        return 0;
    }
    if (it->flags & RAX_ITER_EOF) {
        errno = 0;
        return 0;
    }
    return 1;
}

/* 在迭代器 'it' 范围内回退到上一个元素。
 * 如果达到 EOF（或内存不足），则返回 0，否则返回 1。
 * 如果是因为 OOM 而返回 0，则 errno 会被设置为 ENOMEM。 */
int raxPrev(raxIterator *it) {
    if (!raxIteratorPrevStep(it,0)) {
        errno = ENOMEM;
        return 0;
    }
    if (it->flags & RAX_ITER_EOF) {
        errno = 0;
        return 0;
    }
    return 1;
}

/* 从迭代器当前位置开始执行一次随机游走。
 * 如果树为空或内存不足则返回 0；否则返回 1，并将迭代器设置为经过 'steps'
 * 步随机游走后到达的节点。如果 'steps' 参数为 0，则随机游走的步数会取
 * 一个介于 1 到元素数量对数的两倍之间的随机值。
 *
 * 注意：如果用此函数从基数树中生成随机元素，期望分布可能并不理想。当树
 * 不稀疏时，随机游走可以产生较好的随机元素，但对于基数树而言，某些 key
 * 出现的频率会比其他 key 高得多。不过至少该函数最终应当能够遍历到每一个
 * 可能的元素。 */
int raxRandomWalk(raxIterator *it, size_t steps) {
    if (it->rt->numele == 0) {
        it->flags |= RAX_ITER_EOF;
        return 0;
    }

    if (steps == 0) {
        size_t fle = 1+floor(log(it->rt->numele));
        fle *= 2;
        steps = 1 + rand() % fle;
    }

    raxNode *n = it->node;
    while(steps > 0 || !n->iskey) {
        int numchildren = n->iscompr ? 1 : n->size;
        int r = rand() % (numchildren+(n != it->rt->head));

        if (r == numchildren) {
            /* Go up to parent. */
            n = raxStackPop(&it->stack);
            int todel = n->iscompr ? n->size : 1;
            raxIteratorDelChars(it,todel);
        } else {
            /* Select a random child. */
            if (n->iscompr) {
                if (!raxIteratorAddChars(it,n->data,n->size)) return 0;
            } else {
                if (!raxIteratorAddChars(it,n->data+r,1)) return 0;
            }
            raxNode **cp = raxNodeFirstChildPtr(n)+r;
            if (!raxStackPush(&it->stack,n)) return 0;
            memcpy(&n,cp,sizeof(n));
        }
        if (n->iskey) steps--;
    }
    it->node = n;
    it->data = raxGetData(it->node);
    return 1;
}

/* 根据指定的操作符，将迭代器当前指向的 key 与指定 key 进行比较。
 * 如果比较结果为真则返回 1，否则返回 0。 */
int raxCompare(raxIterator *iter, const char *op, unsigned char *key, size_t key_len) {
    int eq = 0, lt = 0, gt = 0;

    if (op[0] == '=' || op[1] == '=') eq = 1;
    if (op[0] == '>') gt = 1;
    else if (op[0] == '<') lt = 1;
    else if (op[1] != '=') return 0; /* Syntax error. */

    size_t minlen = key_len < iter->key_len ? key_len : iter->key_len;
    int cmp = memcmp(iter->key,key,minlen);

    /* Handle == */
    if (lt == 0 && gt == 0) return cmp == 0 && key_len == iter->key_len;

    /* Handle >, >=, <, <= */
    if (cmp == 0) {
        /* Same prefix: longer wins. */
        if (eq && key_len == iter->key_len) return 1;
        else if (lt) return iter->key_len < key_len;
        else if (gt) return iter->key_len > key_len;
        else return 0; /* Avoid warning, just 'eq' is handled before. */
    } else if (cmp > 0) {
        return gt ? 1 : 0;
    } else /* (cmp < 0) */ {
        return lt ? 1 : 0;
    }
}

/* 释放迭代器。 */
void raxStop(raxIterator *it) {
    if (it->key != it->key_static_string) rax_free(it->key);
    raxStackFree(&it->stack);
}

/* 返回迭代器是否处于 EOF 状态。当 raxSeek() 未能定位到合适的元素（导致
 * raxNext() 或 raxPrev() 将返回 0），或者在使用 raxNext()/raxPrev() 迭代
 * 过程中到达 EOF 条件时，就会处于该状态。 */
int raxEOF(raxIterator *it) {
    return it->flags & RAX_ITER_EOF;
}

/* 返回基数树中保存的元素数量。 */
uint64_t raxSize(rax *rax) {
    return rax->numele;
}

/* ----------------------------- Introspection ------------------------------ */

/* 该函数主要用于调试和学习目的。它会将一棵树以 ASCII 表示输出到标准输出，
 * 列出所有节点以及其中包含的 key。
 *
 * 表示形式如下：
 *
 *  "foobar" （压缩节点）
 *  [abc] （带有三个子节点的普通节点）
 *  [abc]=0x12345678 （节点是 key，指向 value 0x12345678）
 *  [] （普通的空节点）
 *
 *  子节点会按缩进换行的形式展示，每个子节点以 "`-(x)" 作为前缀，其中
 *  "x" 是边上的字节。
 *
 *  [abc]
 *   `-(a) "ladin"
 *   `-(b) [kj]
 *   `-(c) []
 *
 *  不过当节点只有一个子节点时，会改用如下表示：
 *
 *  [abc] -> "ladin" -> []
 */

/* raxShow() 的实际实现。 */
void raxRecursiveShow(int level, int lpad, raxNode *n) {
    char s = n->iscompr ? '"' : '[';
    char e = n->iscompr ? '"' : ']';

    int numchars = printf("%c%.*s%c", s, n->size, n->data, e);
    if (n->iskey) {
        numchars += printf("=%p",raxGetData(n));
    }

    int numchildren = n->iscompr ? 1 : n->size;
    /* Note that 7 and 4 magic constants are the string length
     * of " `-(x) " and " -> " respectively. */
    if (level) {
        lpad += (numchildren > 1) ? 7 : 4;
        if (numchildren == 1) lpad += numchars;
    }
    raxNode **cp = raxNodeFirstChildPtr(n);
    for (int i = 0; i < numchildren; i++) {
        char *branch = " `-(%c) ";
        if (numchildren > 1) {
            printf("\n");
            for (int j = 0; j < lpad; j++) putchar(' ');
            printf(branch,n->data[i]);
        } else {
            printf(" -> ");
        }
        raxNode *child;
        memcpy(&child,cp,sizeof(child));
        raxRecursiveShow(level+1,lpad,child);
        cp++;
    }
}

/* 展示一棵树，其格式如上方注释所述。 */
void raxShow(rax *rax) {
    raxRecursiveShow(0,0,rax->head);
    putchar('\n');
}

/* 由 debugnode() 宏调用，用于展示给定节点的信息。 */
void raxDebugShowNode(const char *msg, raxNode *n) {
    if (raxDebugMsg == 0) return;
    printf("%s: %p [%.*s] key:%u size:%u children:",
        msg, (void*)n, (int)n->size, (char*)n->data, n->iskey, n->size);
    int numcld = n->iscompr ? 1 : n->size;
    raxNode **cldptr = raxNodeLastChildPtr(n) - (numcld-1);
    while(numcld--) {
        raxNode *child;
        memcpy(&child,cldptr,sizeof(child));
        cldptr++;
        printf("%p ", (void*)child);
    }
    printf("\n");
    fflush(stdout);
}

/* 遍历访问树中的所有节点并返回一个校验和。这对于让 Valgrind 在读取数据
 * 结构时检测是否存在错误非常有用。
 *
 * 该函数曾经在一次较大的重构之后被用来定位 Rax 中的 bug，所采用的方法如下：
 *
 * 1. 在 Valgrind 下运行 rax-test，并增加一个 printf()，以便我们能从模糊
 *    测试器中看到当前正处于循环的哪一次迭代。
 * 2. 在 rax-test.c 中由模糊测试器修改基数树之后，每处都调用一次 raxTouch()。
 * 3. 这样一旦某个操作破坏了树，raxTouch() 会立刻（通过 Valgrind）检测到。
 *    我们可以添加更多调用来缩小出错时的状态范围。
 * 4. 此时一个比较好的做法是在树被破坏的那一刻之前立即启用 Rax 的调试消息，
 *    以观察究竟发生了什么。
 */
unsigned long raxTouch(raxNode *n) {
    debugf("Touching %p\n", (void*)n);
    unsigned long sum = 0;
    if (n->iskey) {
        sum += (unsigned long)raxGetData(n);
    }

    int numchildren = n->iscompr ? 1 : n->size;
    raxNode **cp = raxNodeFirstChildPtr(n);
    int count = 0;
    for (int i = 0; i < numchildren; i++) {
        if (numchildren > 1) {
            sum += (long)n->data[i];
        }
        raxNode *child;
        memcpy(&child,cp,sizeof(child));
        if (child == (void*)0x65d1760) count++;
        if (count > 1) exit(1);
        sum += raxTouch(child);
        cp++;
    }
    return sum;
}
