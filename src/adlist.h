/* adlist.h - A generic doubly linked list implementation
 * adlist.h - 通用双向链表实现
 *
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#ifndef __ADLIST_H__
#define __ADLIST_H__

/* Node, List, and Iterator are the only data structures used currently.
 * 目前只使用 Node、List 和 Iterator 这三种数据结构。 */

/* listNode：双向链表节点结构体 */
typedef struct listNode {
    struct listNode *prev;   // 前驱节点指针
    struct listNode *next;   // 后继节点指针
    void *value;             // 节点所保存的值（任意类型指针）
} listNode;

/* listIter：链表迭代器结构体 */
typedef struct listIter {
    listNode *next;          // 迭代器当前指向的下一个节点
    int direction;           // 迭代方向（AL_START_HEAD 或 AL_START_TAIL）
} listIter;

/* list：链表结构体
 * 包含头尾节点、长度以及三个可选的回调函数 */
typedef struct list {
    listNode *head;                              // 链表头节点
    listNode *tail;                              // 链表尾节点
    void *(*dup)(void *ptr);                     // 节点值复制函数（可选）
    void (*free)(void *ptr);                     // 节点值释放函数（可选）
    int (*match)(void *ptr, void *key);          // 节点值比较函数（可选）
    unsigned long len;                           // 链表节点数量
} list;

/* Functions implemented as macros
 * 以下函数通过宏实现 */

/* 返回链表节点数量 */
#define listLength(l) ((l)->len)
/* 返回链表头节点 */
#define listFirst(l) ((l)->head)
/* 返回链表尾节点 */
#define listLast(l) ((l)->tail)
/* 返回节点 n 的前驱节点 */
#define listPrevNode(n) ((n)->prev)
/* 返回节点 n 的后继节点 */
#define listNextNode(n) ((n)->next)
/* 返回节点 n 存储的值 */
#define listNodeValue(n) ((n)->value)

/* 设置链表 l 的节点值复制函数为 m */
#define listSetDupMethod(l,m) ((l)->dup = (m))
/* 设置链表 l 的节点值释放函数为 m */
#define listSetFreeMethod(l,m) ((l)->free = (m))
/* 设置链表 l 的节点值比较函数为 m */
#define listSetMatchMethod(l,m) ((l)->match = (m))

/* 获取链表 l 的节点值复制函数 */
#define listGetDupMethod(l) ((l)->dup)
/* 获取链表 l 的节点值释放函数 */
#define listGetFreeMethod(l) ((l)->free)
/* 获取链表 l 的节点值比较函数 */
#define listGetMatchMethod(l) ((l)->match)

/* Prototypes
 * 函数原型声明 */

/* 创建一个新的空 list。O(1) */
list *listCreate(void);
/* 释放整个 list 及其所有节点 */
void listRelease(list *list);
/* 清空 list 中的所有元素，但保留 list 结构本身 */
void listEmpty(list *list);
/* 将一个新节点添加到 list 的头部 */
list *listAddNodeHead(list *list, void *value);
/* 将一个新节点添加到 list 的尾部 */
list *listAddNodeTail(list *list, void *value);
/* 在 old_node 之前或之后插入一个新节点（由 after 控制） */
list *listInsertNode(list *list, listNode *old_node, void *value, int after);
/* 从 list 中删除指定节点 */
void listDelNode(list *list, listNode *node);
/* 获取一个 list 迭代器，direction 指定迭代方向 */
listIter *listGetIterator(list *list, int direction);
/* 返回迭代器的下一个节点 */
listNode *listNext(listIter *iter);
/* 释放迭代器 */
void listReleaseIterator(listIter *iter);
/* 复制整个 list */
list *listDup(list *orig);
/* 在 list 中按 key 查找匹配的节点 */
listNode *listSearchKey(list *list, void *key);
/* 返回 list 中指定下标的节点（支持负数下标） */
listNode *listIndex(list *list, long index);
/* 将迭代器 li 重置到 list 头部方向 */
void listRewind(list *list, listIter *li);
/* 将迭代器 li 重置到 list 尾部方向 */
void listRewindTail(list *list, listIter *li);
/* 将 list 的尾节点移动到头部 */
void listRotateTailToHead(list *list);
/* 将 list 的头节点移动到尾部 */
void listRotateHeadToTail(list *list);
/* 将 list o 的所有元素追加到 list l 的末尾 */
void listJoin(list *l, list *o);
/* 初始化一个未挂入任何 list 的节点 */
void listInitNode(listNode *node, void *value);
/* 将节点 node 链接到 list 的头部（要求 node 已经分配） */
void listLinkNodeHead(list *list, listNode *node);
/* 将节点 node 链接到 list 的尾部（要求 node 已经分配） */
void listLinkNodeTail(list *list, listNode *node);
/* 将节点 node 从 list 中解除链接（不释放节点内存） */
void listUnlinkNode(list *list, listNode *node);

/* Directions for iterators
 * 迭代器的迭代方向 */

/* 从表头向表尾迭代 */
#define AL_START_HEAD 0
/* 从表尾向表头迭代 */
#define AL_START_TAIL 1

#endif /* __ADLIST_H__ */
