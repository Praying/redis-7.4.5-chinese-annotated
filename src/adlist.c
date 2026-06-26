/* adlist.c - A generic doubly linked list implementation
 * adlist.c - 通用双向链表实现
 *
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */


#include <stdlib.h>
#include "adlist.h"
#include "zmalloc.h"

/* Create a new list. The created list can be freed with
 * listRelease(), but private value of every node need to be freed
 * by the user before to call listRelease(), or by setting a free method using
 * listSetFreeMethod.
 *
 * 创建一个新的 list。创建出的 list 可以使用 listRelease() 释放，
 * 但每个节点的私有 value 需要用户在调用 listRelease() 之前自行释放，
 * 或者通过 listSetFreeMethod 设置一个释放回调。
 *
 * On error, NULL is returned. Otherwise the pointer to the new list.
 * 出错时返回 NULL，否则返回指向新 list 的指针。 */
list *listCreate(void)
{
    struct list *list;

    if ((list = zmalloc(sizeof(*list))) == NULL)
        return NULL;
    // 初始化头尾节点为 NULL，长度为 0，回调函数均为 NULL
    list->head = list->tail = NULL;
    list->len = 0;
    list->dup = NULL;
    list->free = NULL;
    list->match = NULL;
    return list;
}

/* Remove all the elements from the list without destroying the list itself.
 * 移除 list 中的所有元素，但不销毁 list 本身。 */
void listEmpty(list *list)
{
    unsigned long len;
    listNode *current, *next;

    current = list->head;
    len = list->len;
    // 遍历所有节点，释放 value（若设置了 free 回调）和节点本身
    while(len--) {
        next = current->next;
        if (list->free) list->free(current->value);
        zfree(current);
        current = next;
    }
    // 重置 list 为空
    list->head = list->tail = NULL;
    list->len = 0;
}

/* Free the whole list.
 * 释放整个 list。
 *
 * This function can't fail.
 * 该函数不会失败。 */
void listRelease(list *list)
{
    if (!list)
        return;
    // 先清空所有节点，再释放 list 结构本身
    listEmpty(list);
    zfree(list);
}

/* Add a new node to the list, to head, containing the specified 'value'
 * pointer as value.
 * 将一个新节点添加到 list 的头部，节点中保存指定的 'value' 指针。
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * 出错时返回 NULL，且不执行任何操作（即 list 保持不变）。
 * On success the 'list' pointer you pass to the function is returned.
 * 成功时返回传入的 'list' 指针。 */
list *listAddNodeHead(list *list, void *value)
{
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    // 设置节点值并将其链接到 list 头部
    node->value = value;
    listLinkNodeHead(list, node);
    return list;
}

/*
 * Add a node that has already been allocated to the head of list
 * 将一个已经分配好的节点添加到 list 的头部。
 */
void listLinkNodeHead(list* list, listNode *node) {
    if (list->len == 0) {
        // list 为空时，该节点同时作为头尾节点
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        // 将节点插入到原 head 之前
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    list->len++;
}

/* Add a new node to the list, to tail, containing the specified 'value'
 * pointer as value.
 * 将一个新节点添加到 list 的尾部，节点中保存指定的 'value' 指针。
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * 出错时返回 NULL，且不执行任何操作（即 list 保持不变）。
 * On success the 'list' pointer you pass to the function is returned.
 * 成功时返回传入的 'list' 指针。 */
list *listAddNodeTail(list *list, void *value)
{
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    // 设置节点值并将其链接到 list 尾部
    node->value = value;
    listLinkNodeTail(list, node);
    return list;
}

/*
 * Add a node that has already been allocated to the tail of list
 * 将一个已经分配好的节点添加到 list 的尾部。
 */
void listLinkNodeTail(list *list, listNode *node) {
    if (list->len == 0) {
        // list 为空时，该节点同时作为头尾节点
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        // 将节点插入到原 tail 之后
        node->prev = list->tail;
        node->next = NULL;
        list->tail->next = node;
        list->tail = node;
    }
    list->len++;
}

/* 在 old_node 之前或之后插入一个值为 value 的新节点。
 * after 为真表示在 old_node 之后插入，为假表示在之前插入。
 * 失败时返回 NULL，成功时返回 list 指针。 */
list *listInsertNode(list *list, listNode *old_node, void *value, int after) {
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    node->value = value;
    if (after) {
        // 在 old_node 之后插入
        node->prev = old_node;
        node->next = old_node->next;
        if (list->tail == old_node) {
            // 若 old_node 是尾节点，更新 tail
            list->tail = node;
        }
    } else {
        // 在 old_node 之前插入
        node->next = old_node;
        node->prev = old_node->prev;
        if (list->head == old_node) {
            // 若 old_node 是头节点，更新 head
            list->head = node;
        }
    }
    // 调整相邻节点的指针
    if (node->prev != NULL) {
        node->prev->next = node;
    }
    if (node->next != NULL) {
        node->next->prev = node;
    }
    list->len++;
    return list;
}

/* Remove the specified node from the specified list.
 * 从指定的 list 中删除指定的节点。
 * The node is freed. If free callback is provided the value is freed as well.
 * 节点内存会被释放。若提供了 free 回调，节点的 value 也会被释放。
 *
 * This function can't fail.
 * 该函数不会失败。 */
void listDelNode(list *list, listNode *node)
{
    // 先将节点从 list 中解链
    listUnlinkNode(list, node);
    if (list->free) list->free(node->value);
    zfree(node);
}

/*
 * Remove the specified node from the list without freeing it.
 * 将指定节点从 list 中移除，但不会释放该节点的内存。
 */
void listUnlinkNode(list *list, listNode *node) {
    if (node->prev)
        node->prev->next = node->next;
    else
        // node 是头节点时，更新 head
        list->head = node->next;
    if (node->next)
        node->next->prev = node->prev;
    else
        // node 是尾节点时，更新 tail
        list->tail = node->prev;

    // 清空 node 的指针字段
    node->next = NULL;
    node->prev = NULL;

    list->len--;
}

/* Returns a list iterator 'iter'. After the initialization every
 * call to listNext() will return the next element of the list.
 * 返回一个 list 迭代器 'iter'。初始化完成后，每次调用 listNext()
 * 都将返回 list 中的下一个元素。
 *
 * This function can't fail.
 * 该函数不会失败。 */
listIter *listGetIterator(list *list, int direction)
{
    listIter *iter;

    if ((iter = zmalloc(sizeof(*iter))) == NULL) return NULL;
    // 根据迭代方向设置迭代器的起始位置
    if (direction == AL_START_HEAD)
        iter->next = list->head;
    else
        iter->next = list->tail;
    iter->direction = direction;
    return iter;
}

/* Release the iterator memory
 * 释放迭代器占用的内存 */
void listReleaseIterator(listIter *iter) {
    zfree(iter);
}

/* Create an iterator in the list private iterator structure
 * 在 list 私有的迭代器结构中创建一个迭代器（重置为从头开始） */
void listRewind(list *list, listIter *li) {
    li->next = list->head;
    li->direction = AL_START_HEAD;
}

/* 重置迭代器 li 为从 list 尾部开始向头部方向迭代 */
void listRewindTail(list *list, listIter *li) {
    li->next = list->tail;
    li->direction = AL_START_TAIL;
}

/* Return the next element of an iterator.
 * 返回迭代器的下一个元素。
 * It's valid to remove the currently returned element using
 * listDelNode(), but not to remove other elements.
 * 可以使用 listDelNode() 删除当前返回的元素，
 * 但不可以删除其他元素。
 *
 * The function returns a pointer to the next element of the list,
 * 该函数返回 list 中下一个元素的指针，
 * or NULL if there are no more elements, so the classical usage
 * pattern is:
 * 若没有更多元素则返回 NULL，因此典型的使用模式为：
 *
 * iter = listGetIterator(list,<direction>);
 * while ((node = listNext(iter)) != NULL) {
 *     doSomethingWith(listNodeValue(node));
 * }
 *
 * */
listNode *listNext(listIter *iter)
{
    listNode *current = iter->next;

    if (current != NULL) {
        // 根据迭代方向推进 iter->next
        if (iter->direction == AL_START_HEAD)
            iter->next = current->next;
        else
            iter->next = current->prev;
    }
    return current;
}

/* Duplicate the whole list. On out of memory NULL is returned.
 * 复制整个 list。内存不足时返回 NULL。
 * On success a copy of the original list is returned.
 * 成功时返回原始 list 的副本。
 *
 * The 'Dup' method set with listSetDupMethod() function is used
 * to copy the node value. Otherwise the same pointer value of
 * the original node is used as value of the copied node.
 * 节点值通过 listSetDupMethod() 设置的 'Dup' 方法进行拷贝。
 * 否则，副本节点的 value 将直接使用原节点的指针值。
 *
 * The original list both on success or error is never modified.
 * 无论成功还是出错，原始 list 都不会被修改。 */
list *listDup(list *orig)
{
    list *copy;
    listIter iter;
    listNode *node;

    if ((copy = listCreate()) == NULL)
        return NULL;
    // 复制 list 的三个回调函数指针
    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;
    listRewind(orig, &iter);
    while((node = listNext(&iter)) != NULL) {
        void *value;

        // 若设置了 dup 回调，则使用 dup 拷贝节点值
        if (copy->dup) {
            value = copy->dup(node->value);
            if (value == NULL) {
                listRelease(copy);
                return NULL;
            }
        } else {
            // 否则直接使用原节点的指针
            value = node->value;
        }

        if (listAddNodeTail(copy, value) == NULL) {
            /* Free value if dup succeed but listAddNodeTail failed.
             * 若 dup 成功但 listAddNodeTail 失败，则释放 value。 */
            if (copy->free) copy->free(value);

            listRelease(copy);
            return NULL;
        }
    }
    return copy;
}

/* Search the list for a node matching a given key.
 * 在 list 中查找与给定 key 匹配的节点。
 * The match is performed using the 'match' method
 * set with listSetMatchMethod(). If no 'match' method
 * is set, the 'value' pointer of every node is directly
 * compared with the 'key' pointer.
 * 匹配通过 listSetMatchMethod() 设置的 'match' 方法进行。
 * 若未设置 'match' 方法，则直接将每个节点的 'value' 指针
 * 与 'key' 指针进行比较。
 *
 * On success the first matching node pointer is returned
 * (search starts from head). If no matching node exists
 * NULL is returned.
 * 成功时返回第一个匹配的节点指针（从 head 开始查找）。
 * 若没有匹配节点则返回 NULL。 */
listNode *listSearchKey(list *list, void *key)
{
    listIter iter;
    listNode *node;

    listRewind(list, &iter);
    while((node = listNext(&iter)) != NULL) {
        if (list->match) {
            // 使用 match 回调进行匹配
            if (list->match(node->value, key)) {
                return node;
            }
        } else {
            // 直接比较指针值
            if (key == node->value) {
                return node;
            }
        }
    }
    return NULL;
}

/* Return the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range NULL is returned.
 * 返回指定下标（从 0 开始）处的元素，其中 0 表示 head，
 * 1 表示 head 之后的元素，依此类推。负数下标用于从尾部
 * 开始计数，-1 表示最后一个元素，-2 表示倒数第二个，
 * 依此类推。若下标越界则返回 NULL。 */
listNode *listIndex(list *list, long index) {
    listNode *n;

    if (index < 0) {
        // 负数下标：从 tail 向 head 方向查找
        index = (-index)-1;
        n = list->tail;
        while(index-- && n) n = n->prev;
    } else {
        // 非负下标：从 head 向 tail 方向查找
        n = list->head;
        while(index-- && n) n = n->next;
    }
    return n;
}

/* Rotate the list removing the tail node and inserting it to the head.
 * 将 list 的尾节点移除并插入到头部。 */
void listRotateTailToHead(list *list) {
    if (listLength(list) <= 1) return;

    /* Detach current tail
     * 摘除当前尾节点 */
    listNode *tail = list->tail;
    list->tail = tail->prev;
    list->tail->next = NULL;
    /* Move it as head
     * 将其移动到头部 */
    list->head->prev = tail;
    tail->prev = NULL;
    tail->next = list->head;
    list->head = tail;
}

/* Rotate the list removing the head node and inserting it to the tail.
 * 将 list 的头节点移除并插入到尾部。 */
void listRotateHeadToTail(list *list) {
    if (listLength(list) <= 1) return;

    listNode *head = list->head;
    /* Detach current head
     * 摘除当前头节点 */
    list->head = head->next;
    list->head->prev = NULL;
    /* Move it as tail
     * 将其移动到尾部 */
    list->tail->next = head;
    head->next = NULL;
    head->prev = list->tail;
    list->tail = head;
}

/* Add all the elements of the list 'o' at the end of the
 * list 'l'. The list 'other' remains empty but otherwise valid.
 * 将 list 'o' 的所有元素追加到 list 'l' 的末尾。
 * list 'o' 之后保持为空，但本身仍是一个有效的 list。 */
void listJoin(list *l, list *o) {
    if (o->len == 0) return;

    // 将 o 的 head 与 l 的 tail 相互连接
    o->head->prev = l->tail;

    if (l->tail)
        l->tail->next = o->head;
    else
        l->head = o->head;

    l->tail = o->tail;
    l->len += o->len;

    /* Setup other as an empty list.
     * 将 o 重置为空 list。 */
    o->head = o->tail = NULL;
    o->len = 0;
}

/* Initializes the node's value and sets its pointers
 * so that it is initially not a member of any list.
 * 初始化节点的 value，并将指针字段置空，
 * 使其初始状态下不属于任何 list。
 */
void listInitNode(listNode *node, void *value) {
    node->prev = NULL;
    node->next = NULL;
    node->value = value;
}
