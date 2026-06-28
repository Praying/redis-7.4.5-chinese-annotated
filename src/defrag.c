/*
 * 主动内存碎片整理
 * 尝试查找需要重新分配的键/值内存分配，以减少外部碎片。
 * 通过扫描键空间（keyspace），对每个持有的指针尝试询问分配器
 * 将其移动到新地址是否有助于减少碎片。
 *
 * Copyright (c) 2020-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include <stddef.h>

#ifdef HAVE_DEFRAG

/* 碎片整理上下文，保存私有数据和当前处理的 slot */
typedef struct defragCtx {
    void *privdata;
    int slot;
} defragCtx;

/* Pub/Sub 碎片整理上下文 */
typedef struct defragPubSubCtx {
    kvstore *pubsub_channels;
    dict *(*clientPubSubChannels)(client*);
} defragPubSubCtx;

/* 此方法被添加到 jemalloc 中，以帮助我们了解
 * 哪些指针值得移动，哪些不值得 */
int je_get_defrag_hint(void* ptr);

/* 通用内存分配的碎片整理辅助函数。
 *
 * 如果分配未被移动，返回 NULL。
 * 返回非 NULL 值时，旧指针已被释放，不应再访问。 */
void* activeDefragAlloc(void *ptr) {
    size_t size;
    void *newptr;
    if(!je_get_defrag_hint(ptr)) {
        server.stat_active_defrag_misses++;
        return NULL;
    }
    /* 将此分配移动到新的分配地址。
     * 确保不使用线程缓存，以免释放的指针被重新分配回来 */
    size = zmalloc_usable_size(ptr);
    newptr = zmalloc_no_tcache(size);
    memcpy(newptr, ptr, size);
    zfree_no_tcache(ptr);
    server.stat_active_defrag_hits++;
    return newptr;
}

/* SDS 字符串的碎片整理辅助函数
 *
 * 如果分配未被移动，返回 NULL。
 * 返回非 NULL 值时，旧指针已被释放，不应再访问。 */
sds activeDefragSds(sds sdsptr) {
    void* ptr = sdsAllocPtr(sdsptr);
    void* newptr = activeDefragAlloc(ptr);
    if (newptr) {
        size_t offset = sdsptr - (char*)ptr;
        sdsptr = (char*)newptr + offset;
        return sdsptr;
    }
    return NULL;
}

/* hfield 字符串的碎片整理辅助函数
 *
 * 如果分配未被移动，返回 NULL。
 * 返回非 NULL 值时，旧指针已被释放，不应再访问。 */
hfield activeDefragHfield(hfield hf) {
    void *ptr = hfieldGetAllocPtr(hf);
    void *newptr = activeDefragAlloc(ptr);
    if (newptr) {
        size_t offset = hf - (char*)ptr;
        hf = (char*)newptr + offset;
        return hf;
    }
    return NULL;
}

/* 带期望引用计数的 robj 和/或字符串对象的碎片整理辅助函数。
 *
 * 类似于 activeDefragStringOb，但要求调用方传入期望的引用计数。
 * 在某些情况下，调用方需要更新引用计数不为 1 的 robj，
 * 此时调用方必须显式传入引用计数，否则不会执行碎片整理。
 * 注意调用方负责更新 robj 的所有其他引用。 */
robj *activeDefragStringObEx(robj* ob, int expected_refcount) {
    robj *ret = NULL;
    if (ob->refcount!=expected_refcount)
        return NULL;

    /* 尝试整理 robj（仅在非 EMBSTR 类型时，EMBSTR 在下面处理）。 */
    if (ob->type!=OBJ_STRING || ob->encoding!=OBJ_ENCODING_EMBSTR) {
        if ((ret = activeDefragAlloc(ob))) {
            ob = ret;
        }
    }

    /* 尝试整理字符串对象 */
    if (ob->type == OBJ_STRING) {
        if(ob->encoding==OBJ_ENCODING_RAW) {
            sds newsds = activeDefragSds((sds)ob->ptr);
            if (newsds) {
                ob->ptr = newsds;
            }
        } else if (ob->encoding==OBJ_ENCODING_EMBSTR) {
            /* SDS 嵌入在对象分配中，计算偏移量
             * 并在新分配中更新指针。 */
            long ofs = (intptr_t)ob->ptr - (intptr_t)ob;
            if ((ret = activeDefragAlloc(ob))) {
                ret->ptr = (void*)((intptr_t)ret + ofs);
            }
        } else if (ob->encoding!=OBJ_ENCODING_INT) {
            serverPanic("Unknown string encoding");
        }
    }
    return ret;
}

/* robj 和/或字符串对象的碎片整理辅助函数
 *
 * 如果分配未被移动，返回 NULL。
 * 返回非 NULL 值时，旧指针已被释放，不应再访问。 */
robj *activeDefragStringOb(robj* ob) {
    return activeDefragStringObEx(ob, 1);
}

/* Lua 脚本的碎片整理辅助函数
 *
 * 如果分配未被移动，返回 NULL。
 * 返回非 NULL 值时，旧指针已被释放，不应再访问。 */
luaScript *activeDefragLuaScript(luaScript *script) {
    luaScript *ret = NULL;

    /* 尝试整理脚本结构体 */
    if ((ret = activeDefragAlloc(script))) {
        script = ret;
    }

    /* 尝试整理实际的脚本对象 */
    robj *ob = activeDefragStringOb(script->body);
    if (ob) script->body = ob;

    return ret;
}

/* dict 主要分配（dict 结构体和哈希表）的碎片整理辅助函数。
 * 接收 dict* 的指针，当 dict 结构体本身被移动时
 * 返回新的 dict*。
 *
 * 如果分配未被移动，返回 NULL。
 * 返回非 NULL 值时，旧指针已被释放，不应再访问。 */
dict *dictDefragTables(dict *d) {
    dict *ret = NULL;
    dictEntry **newtable;
    /* 处理 dict 结构体 */
    if ((ret = activeDefragAlloc(d)))
        d = ret;
    /* 处理第一个哈希表 */
    if (!d->ht_table[0]) return ret; /* 已创建但未使用 */
    newtable = activeDefragAlloc(d->ht_table[0]);
    if (newtable)
        d->ht_table[0] = newtable;
    /* 处理第二个哈希表 */
    if (d->ht_table[1]) {
        newtable = activeDefragAlloc(d->ht_table[1]);
        if (newtable)
            d->ht_table[1] = newtable;
    }
    return ret;
}

/* zslDefrag 使用的内部函数 */
void zslUpdateNode(zskiplist *zsl, zskiplistNode *oldnode, zskiplistNode *newnode, zskiplistNode **update) {
    int i;
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == oldnode)
            update[i]->level[i].forward = newnode;
    }
    serverAssert(zsl->header!=oldnode);
    if (newnode->level[0].forward) {
        serverAssert(newnode->level[0].forward->backward==oldnode);
        newnode->level[0].forward->backward = newnode;
    } else {
        serverAssert(zsl->tail==oldnode);
        zsl->tail = newnode;
    }
}

/* 有序集合的碎片整理辅助函数。
 * 更新 robj 指针，整理跳表结构体，并返回新的分值引用。
 * 我们不能访问 oldele 指针（甚至跳表中存储的指针也不行），
 * 因为它已被释放。newele 可以为 NULL，此时只需整理跳表，
 * 无需更新 obj 指针。
 * 返回值非 NULL 时，表示必须在 dict 记录中更新的分值引用。 */
double *zslDefrag(zskiplist *zsl, double score, sds oldele, sds newele) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x, *newx;
    int i;
    sds ele = newele? newele: oldele;

    /* find the skiplist node referring to the object that was moved,
     * and all pointers that need to be updated if we'll end up moving the skiplist node. */
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            x->level[i].forward->ele != oldele && /* make sure not to access the
                                                     ->obj pointer if it matches
                                                     oldele */
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                sdscmp(x->level[i].forward->ele,ele) < 0)))
            x = x->level[i].forward;
        update[i] = x;
    }

    /* update the robj pointer inside the skip list record. */
    x = x->level[0].forward;
    serverAssert(x && score == x->score && x->ele==oldele);
    if (newele)
        x->ele = newele;

    /* try to defrag the skiplist record itself */
    newx = activeDefragAlloc(x);
    if (newx) {
        zslUpdateNode(zsl, x, newx, update);
        return &newx->score;
    }
    return NULL;
}

/* 有序集合的碎片整理辅助函数。
 * 整理单个 dict entry 的键名及对应的跳表结构体 */
void activeDefragZsetEntry(zset *zs, dictEntry *de) {
    sds newsds;
    double* newscore;
    sds sdsele = dictGetKey(de);
    if ((newsds = activeDefragSds(sdsele)))
        dictSetKey(zs->dict, de, newsds);
    newscore = zslDefrag(zs->zsl, *(double*)dictGetVal(de), sdsele, newsds);
    if (newscore) {
        dictSetVal(zs->dict, de, newscore);
    }
}

/* SDS dict 值类型常量 */
#define DEFRAG_SDS_DICT_NO_VAL 0          /* 无值（仅键） */
#define DEFRAG_SDS_DICT_VAL_IS_SDS 1      /* 值是 SDS 字符串 */
#define DEFRAG_SDS_DICT_VAL_IS_STROB 2    /* 值是 robj 字符串对象 */
#define DEFRAG_SDS_DICT_VAL_VOID_PTR 3    /* 值是通用指针 */
#define DEFRAG_SDS_DICT_VAL_LUA_SCRIPT 4  /* 值是 Lua 脚本 */

void activeDefragSdsDictCallback(void *privdata, const dictEntry *de) {
    UNUSED(privdata);
    UNUSED(de);
}

void activeDefragHfieldDictCallback(void *privdata, const dictEntry *de) {
    dict *d = privdata;
    hfield newhf, hf = dictGetKey(de);

    if (hfieldGetExpireTime(hf) == EB_EXPIRE_TIME_INVALID) {
        /* 如果 hfield 没有 TTL，直接进行碎片整理。 */
        newhf = activeDefragHfield(hf);
    } else {
        /* Update its reference in the ebucket while defragging it. */
        ebuckets *eb = hashTypeGetDictMetaHFE(d);
        newhf = ebDefragItem(eb, &hashFieldExpireBucketsType, hf, (ebDefragFunction *)activeDefragHfield);
    }
    if (newhf) {
        /* 释放指针后无法在 dict 中搜索该键，因为无法再进行
         * 字符串比较，但可以通过键哈希和指针找到 entry。 */
        dictUseStoredKeyApi(d, 1);
        uint64_t hash = dictGetHash(d, newhf);
        dictUseStoredKeyApi(d, 0);
        dictEntry *de = dictFindEntryByPtrAndHash(d, hf, hash);
        serverAssert(de);
        dictSetKey(d, de, newhf);
    }
}

/* 整理具有 SDS 键和可选值（ptr、SDS 或 robj 字符串）的 dict */
void activeDefragSdsDict(dict* d, int val_type) {
    unsigned long cursor = 0;
    dictDefragFunctions defragfns = {
        .defragAlloc = activeDefragAlloc,
        .defragKey = (dictDefragAllocFunction *)activeDefragSds,
        .defragVal = (val_type == DEFRAG_SDS_DICT_VAL_IS_SDS ? (dictDefragAllocFunction *)activeDefragSds :
                      val_type == DEFRAG_SDS_DICT_VAL_IS_STROB ? (dictDefragAllocFunction *)activeDefragStringOb :
                      val_type == DEFRAG_SDS_DICT_VAL_VOID_PTR ? (dictDefragAllocFunction *)activeDefragAlloc :
                      val_type == DEFRAG_SDS_DICT_VAL_LUA_SCRIPT ? (dictDefragAllocFunction *)activeDefragLuaScript :
                      NULL)
    };
    do {
        cursor = dictScanDefrag(d, cursor, activeDefragSdsDictCallback,
                                &defragfns, NULL);
    } while (cursor != 0);
}

/* 整理具有 hfield 键和 SDS 值的 dict。 */
void activeDefragHfieldDict(dict *d) {
    unsigned long cursor = 0;
    dictDefragFunctions defragfns = {
        .defragAlloc = activeDefragAlloc,
        .defragKey = NULL, /* 将在 activeDefragHfieldDictCallback 中进行碎片整理。 */
        .defragVal = (dictDefragAllocFunction *)activeDefragSds
    };
    do {
        cursor = dictScanDefrag(d, cursor, activeDefragHfieldDictCallback,
                                &defragfns, d);
    } while (cursor != 0);
}

/* 整理包含 ptr、SDS 或 robj 字符串值的列表 */
void activeDefragList(list *l, int val_type) {
    listNode *ln, *newln;
    for (ln = l->head; ln; ln = ln->next) {
        if ((newln = activeDefragAlloc(ln))) {
            if (newln->prev)
                newln->prev->next = newln;
            else
                l->head = newln;
            if (newln->next)
                newln->next->prev = newln;
            else
                l->tail = newln;
            ln = newln;
        }
        if (val_type == DEFRAG_SDS_DICT_VAL_IS_SDS) {
            sds newsds, sdsele = ln->value;
            if ((newsds = activeDefragSds(sdsele)))
                ln->value = newsds;
        } else if (val_type == DEFRAG_SDS_DICT_VAL_IS_STROB) {
            robj *newele, *ele = ln->value;
            if ((newele = activeDefragStringOb(ele)))
                ln->value = newele;
        } else if (val_type == DEFRAG_SDS_DICT_VAL_VOID_PTR) {
            void *newptr, *ptr = ln->value;
            if ((newptr = activeDefragAlloc(ptr)))
                ln->value = newptr;
        }
    }
}

void activeDefragQuickListNode(quicklist *ql, quicklistNode **node_ref) {
    quicklistNode *newnode, *node = *node_ref;
    unsigned char *newzl;
    if ((newnode = activeDefragAlloc(node))) {
        if (newnode->prev)
            newnode->prev->next = newnode;
        else
            ql->head = newnode;
        if (newnode->next)
            newnode->next->prev = newnode;
        else
            ql->tail = newnode;
        *node_ref = node = newnode;
    }
    if ((newzl = activeDefragAlloc(node->entry)))
        node->entry = newzl;
}

void activeDefragQuickListNodes(quicklist *ql) {
    quicklistNode *node = ql->head;
    while (node) {
        activeDefragQuickListNode(ql, &node);
        node = node->next;
    }
}

/* 当值包含大量元素时，我们希望稍后处理而非作为主字典扫描
 * 的一部分。这是为了防止处理大型对象时产生延迟尖峰。 */
void defragLater(redisDb *db, dictEntry *kde) {
    sds key = sdsdup(dictGetKey(kde));
    listAddNodeTail(db->defrag_later, key);
}

/* 如果不需要更多工作返回 0，如果时间用完但还有更多工作则返回 1。 */
long scanLaterList(robj *ob, unsigned long *cursor, long long endtime) {
    quicklist *ql = ob->ptr;
    quicklistNode *node;
    long iterations = 0;
    int bookmark_failed = 0;
    serverAssert(ob->type == OBJ_LIST && ob->encoding == OBJ_ENCODING_QUICKLIST);

    if (*cursor == 0) {
        /* 如果 cursor 为 0，开始新的迭代 */
        node = ql->head;
    } else {
        node = quicklistBookmarkFind(ql, "_AD");
        if (!node) {
            /* 如果书签被删除，说明已到达末尾。 */
            *cursor = 0;
            return 0;
        }
        node = node->next;
    }

    (*cursor)++;
    while (node) {
        activeDefragQuickListNode(ql, &node);
        server.stat_active_defrag_scanned++;
        if (++iterations > 128 && !bookmark_failed) {
            if (ustime() > endtime) {
                if (!quicklistBookmarkCreate(&ql, "_AD", node)) {
                    bookmark_failed = 1;
                } else {
                    ob->ptr = ql; /* bookmark creation may have re-allocated the quicklist */
                    return 1;
                }
            }
            iterations = 0;
        }
        node = node->next;
    }
    quicklistBookmarkDelete(ql, "_AD");
    *cursor = 0;
    return bookmark_failed? 1: 0;
}

/* 延迟扫描有序集合的数据结构 */
typedef struct {
    zset *zs;
} scanLaterZsetData;

void scanLaterZsetCallback(void *privdata, const dictEntry *_de) {
    dictEntry *de = (dictEntry*)_de;
    scanLaterZsetData *data = privdata;
    activeDefragZsetEntry(data->zs, de);
    server.stat_active_defrag_scanned++;
}

void scanLaterZset(robj *ob, unsigned long *cursor) {
    serverAssert(ob->type == OBJ_ZSET && ob->encoding == OBJ_ENCODING_SKIPLIST);
    zset *zs = (zset*)ob->ptr;
    dict *d = zs->dict;
    scanLaterZsetData data = {zs};
    dictDefragFunctions defragfns = {.defragAlloc = activeDefragAlloc};
    *cursor = dictScanDefrag(d, *cursor, scanLaterZsetCallback, &defragfns, &data);
}

/* 当所有工作都在 dictDefragFunctions 中完成时使用的扫描回调。 */
void scanCallbackCountScanned(void *privdata, const dictEntry *de) {
    UNUSED(privdata);
    UNUSED(de);
    server.stat_active_defrag_scanned++;
}

void scanLaterSet(robj *ob, unsigned long *cursor) {
    serverAssert(ob->type == OBJ_SET && ob->encoding == OBJ_ENCODING_HT);
    dict *d = ob->ptr;
    dictDefragFunctions defragfns = {
        .defragAlloc = activeDefragAlloc,
        .defragKey = (dictDefragAllocFunction *)activeDefragSds
    };
    *cursor = dictScanDefrag(d, *cursor, scanCallbackCountScanned, &defragfns, NULL);
}

void scanLaterHash(robj *ob, unsigned long *cursor) {
    serverAssert(ob->type == OBJ_HASH && ob->encoding == OBJ_ENCODING_HT);
    dict *d = ob->ptr;
    dictDefragFunctions defragfns = {
        .defragAlloc = activeDefragAlloc,
        .defragKey = NULL, /* 将在 activeDefragHfieldDictCallback 中进行碎片整理。 */
        .defragVal = (dictDefragAllocFunction *)activeDefragSds
    };
    *cursor = dictScanDefrag(d, *cursor, activeDefragHfieldDictCallback, &defragfns, d);
}

void defragQuicklist(redisDb *db, dictEntry *kde) {
    robj *ob = dictGetVal(kde);
    quicklist *ql = ob->ptr, *newql;
    serverAssert(ob->type == OBJ_LIST && ob->encoding == OBJ_ENCODING_QUICKLIST);
    if ((newql = activeDefragAlloc(ql)))
        ob->ptr = ql = newql;
    if (ql->len > server.active_defrag_max_scan_fields)
        defragLater(db, kde);  /* 元素过多，延迟处理 */
    else
        activeDefragQuickListNodes(ql);
}

/* 整理跳表编码的有序集合 */
void defragZsetSkiplist(redisDb *db, dictEntry *kde) {
    robj *ob = dictGetVal(kde);
    zset *zs = (zset*)ob->ptr;
    zset *newzs;
    zskiplist *newzsl;
    dict *newdict;
    dictEntry *de;
    struct zskiplistNode *newheader;
    serverAssert(ob->type == OBJ_ZSET && ob->encoding == OBJ_ENCODING_SKIPLIST);
    if ((newzs = activeDefragAlloc(zs)))
        ob->ptr = zs = newzs;
    if ((newzsl = activeDefragAlloc(zs->zsl)))
        zs->zsl = newzsl;
    if ((newheader = activeDefragAlloc(zs->zsl->header)))
        zs->zsl->header = newheader;
    if (dictSize(zs->dict) > server.active_defrag_max_scan_fields)
        defragLater(db, kde);
    else {
        dictIterator *di = dictGetIterator(zs->dict);
        while((de = dictNext(di)) != NULL) {
            activeDefragZsetEntry(zs, de);
        }
        dictReleaseIterator(di);
    }
    /* 整理 dict 结构体和哈希表 */
    if ((newdict = dictDefragTables(zs->dict)))
        zs->dict = newdict;
}

/* 整理哈希表编码的哈希对象 */
void defragHash(redisDb *db, dictEntry *kde) {
    robj *ob = dictGetVal(kde);
    dict *d, *newd;
    serverAssert(ob->type == OBJ_HASH && ob->encoding == OBJ_ENCODING_HT);
    d = ob->ptr;
    if (dictSize(d) > server.active_defrag_max_scan_fields)
        defragLater(db, kde);
    else
        activeDefragHfieldDict(d);
    /* 整理 dict 结构体和哈希表 */
    if ((newd = dictDefragTables(ob->ptr)))
        ob->ptr = newd;
}

/* 整理哈希表编码的集合对象 */
void defragSet(redisDb *db, dictEntry *kde) {
    robj *ob = dictGetVal(kde);
    dict *d, *newd;
    serverAssert(ob->type == OBJ_SET && ob->encoding == OBJ_ENCODING_HT);
    d = ob->ptr;
    if (dictSize(d) > server.active_defrag_max_scan_fields)
        defragLater(db, kde);
    else
        activeDefragSdsDict(d, DEFRAG_SDS_DICT_NO_VAL);
    /* 整理 dict 结构体和哈希表 */
    if ((newd = dictDefragTables(ob->ptr)))
        ob->ptr = newd;
}

/* 基数树（radix tree）迭代器的碎片整理回调，
 * 对每个节点调用，用于整理节点的内存分配。 */
int defragRaxNode(raxNode **noderef) {
    raxNode *newnode = activeDefragAlloc(*noderef);
    if (newnode) {
        *noderef = newnode;
        return 1;
    }
    return 0;
}

/* 如果不需要更多工作返回 0，如果时间用完但还有更多工作则返回 1。 */
int scanLaterStreamListpacks(robj *ob, unsigned long *cursor, long long endtime) {
    static unsigned char last[sizeof(streamID)];
    raxIterator ri;
    long iterations = 0;
    serverAssert(ob->type == OBJ_STREAM && ob->encoding == OBJ_ENCODING_STREAM);

    stream *s = ob->ptr;
    raxStart(&ri,s->rax);
    if (*cursor == 0) {
        /* 如果 cursor 为 0，开始新的迭代 */
        defragRaxNode(&s->rax->head);
        /* 在 seek 之前设置迭代器节点回调，以覆盖
         * 从起始到第一个条目之间处理的初始节点 */
        ri.node_cb = defragRaxNode;
        raxSeek(&ri,"^",NULL,0);
    } else {
        /* 如果 cursor 非零，定位到静态变量 'last' 处 */
        if (!raxSeek(&ri,">", last, sizeof(last))) {
            *cursor = 0;
            raxStop(&ri);
            return 0;
        }
        /* 在 seek 之后设置迭代器节点回调，以避免
         * 覆盖此前已处理的初始节点 */
        ri.node_cb = defragRaxNode;
    }

    (*cursor)++;
    while (raxNext(&ri)) {
        void *newdata = activeDefragAlloc(ri.data);
        if (newdata)
            raxSetData(ri.node, ri.data=newdata);
        server.stat_active_defrag_scanned++;
        if (++iterations > 128) {
            if (ustime() > endtime) {
                serverAssert(ri.key_len==sizeof(last));
                memcpy(last,ri.key,ri.key_len);
                raxStop(&ri);
                return 1;
            }
            iterations = 0;
        }
    }
    raxStop(&ri);
    *cursor = 0;
    return 0;
}

/* 可选回调，用于整理每个 rax 元素（不包括元素指针本身） */
typedef void *(raxDefragFunction)(raxIterator *ri, void *privdata);

/* 整理基数树，包括：
 * 1) rax 结构体
 * 2) rax 节点
 * 3) rax 条目数据（仅在指定 defrag_data 时）
 * 4) 对每个元素调用回调，允许回调返回元素的新指针 */
void defragRadixTree(rax **raxref, int defrag_data, raxDefragFunction *element_cb, void *element_cb_data) {
    raxIterator ri;
    rax* rax;
    if ((rax = activeDefragAlloc(*raxref)))
        *raxref = rax;
    rax = *raxref;
    raxStart(&ri,rax);
    ri.node_cb = defragRaxNode;
    defragRaxNode(&rax->head);
    raxSeek(&ri,"^",NULL,0);
    while (raxNext(&ri)) {
        void *newdata = NULL;
        if (element_cb)
            newdata = element_cb(&ri, element_cb_data);
        if (defrag_data && !newdata)
            newdata = activeDefragAlloc(ri.data);
        if (newdata)
            raxSetData(ri.node, ri.data=newdata);
    }
    raxStop(&ri);
}

/* 待确认条目（Pending Entry）的碎片整理上下文 */
typedef struct {
    streamCG *cg;
    streamConsumer *c;
} PendingEntryContext;

void* defragStreamConsumerPendingEntry(raxIterator *ri, void *privdata) {
    PendingEntryContext *ctx = privdata;
    streamNACK *nack = ri->data, *newnack;
    nack->consumer = ctx->c; /* 更新 nack 指向消费者的指针 */
    newnack = activeDefragAlloc(nack);
    if (newnack) {
        /* 更新消费者组指向 nack 的指针 */
        void *prev;
        raxInsert(ctx->cg->pel, ri->key, ri->key_len, newnack, &prev);
        serverAssert(prev==nack);
    }
    return newnack;
}

void* defragStreamConsumer(raxIterator *ri, void *privdata) {
    streamConsumer *c = ri->data;
    streamCG *cg = privdata;
    void *newc = activeDefragAlloc(c);
    if (newc) {
        c = newc;
    }
    sds newsds = activeDefragSds(c->name);
    if (newsds)
        c->name = newsds;
    if (c->pel) {
        PendingEntryContext pel_ctx = {cg, c};
        defragRadixTree(&c->pel, 0, defragStreamConsumerPendingEntry, &pel_ctx);
    }
    return newc; /* 如果 c 未被整理则返回 NULL */
}

void* defragStreamConsumerGroup(raxIterator *ri, void *privdata) {
    streamCG *cg = ri->data;
    UNUSED(privdata);
    if (cg->consumers)
        defragRadixTree(&cg->consumers, 0, defragStreamConsumer, cg);
    if (cg->pel)
        defragRadixTree(&cg->pel, 0, NULL, NULL);
    return NULL;
}

void defragStream(redisDb *db, dictEntry *kde) {
    robj *ob = dictGetVal(kde);
    serverAssert(ob->type == OBJ_STREAM && ob->encoding == OBJ_ENCODING_STREAM);
    stream *s = ob->ptr, *news;

    /* 处理主结构体 */
    if ((news = activeDefragAlloc(s)))
        ob->ptr = s = news;

    if (raxSize(s->rax) > server.active_defrag_max_scan_fields) {
        rax *newrax = activeDefragAlloc(s->rax);
        if (newrax)
            s->rax = newrax;
        defragLater(db, kde);
    } else
        defragRadixTree(&s->rax, 1, NULL, NULL);

    if (s->cgroups)
        defragRadixTree(&s->cgroups, 1, defragStreamConsumerGroup, NULL);
}

/* 整理模块键。可以立即执行或调度到稍后处理。
 * 返回已整理的指针数量。
 */
void defragModule(redisDb *db, dictEntry *kde) {
    robj *obj = dictGetVal(kde);
    serverAssert(obj->type == OBJ_MODULE);
    robj keyobj;
    initStaticStringObject(keyobj, dictGetKey(kde));
    if (!moduleDefragValue(&keyobj, obj, db->id))
        defragLater(db, kde);
}

/* 对主字典中扫描到的每个键，此函数将尝试整理
 * 其包含的所有各种指针。 */
void defragKey(defragCtx *ctx, dictEntry *de) {
    sds keysds = dictGetKey(de);
    robj *newob, *ob = dictGetVal(de);
    unsigned char *newzl;
    sds newsds;
    redisDb *db = ctx->privdata;
    int slot = ctx->slot;
    /* 尝试整理键名。 */
    newsds = activeDefragSds(keysds);
    if (newsds) {
        kvstoreDictSetKey(db->keys, slot, de, newsds);
        if (kvstoreDictSize(db->expires, slot)) {
            /* 释放指针后无法在 db->expires 中搜索该键，因为无法再进行
             * 字符串比较，但可以通过键哈希和指针找到 entry。 */
            uint64_t hash = kvstoreGetHash(db->expires, newsds);
            dictEntry *expire_de = kvstoreDictFindEntryByPtrAndHash(db->expires, slot, keysds, hash);
            if (expire_de) kvstoreDictSetKey(db->expires, slot, expire_de, newsds);
        }

        /* 更新键在 dict 元数据或 listpackEx 中的引用。 */
        if (unlikely(ob->type == OBJ_HASH))
            hashTypeUpdateKeyRef(ob, newsds);
    }

    /* 尝试整理 robj 和/或字符串值。 */
    if (unlikely(ob->type == OBJ_HASH && hashTypeGetMinExpire(ob, 0) != EB_EXPIRE_TIME_INVALID)) {
        /* 在碎片整理的同时更新其在 ebucket 中的引用。 */
        newob = ebDefragItem(&db->hexpires, &hashExpireBucketsType, ob,
                             (ebDefragFunction *)activeDefragStringOb);
    } else {
        /* 如果 dict 没有元数据，直接进行碎片整理。 */
        newob = activeDefragStringOb(ob);
    }
    if (newob) {
        kvstoreDictSetVal(db->keys, slot, de, newob);
        ob = newob;
    }

    if (ob->type == OBJ_STRING) {
        /* 已在 activeDefragStringOb 中处理。 */
    } else if (ob->type == OBJ_LIST) {
        if (ob->encoding == OBJ_ENCODING_QUICKLIST) {
            defragQuicklist(db, de);
        } else if (ob->encoding == OBJ_ENCODING_LISTPACK) {
            if ((newzl = activeDefragAlloc(ob->ptr)))
                ob->ptr = newzl;
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (ob->type == OBJ_SET) {
        if (ob->encoding == OBJ_ENCODING_HT) {
            defragSet(db, de);
        } else if (ob->encoding == OBJ_ENCODING_INTSET ||
                   ob->encoding == OBJ_ENCODING_LISTPACK)
        {
            void *newptr, *ptr = ob->ptr;
            if ((newptr = activeDefragAlloc(ptr)))
                ob->ptr = newptr;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (ob->type == OBJ_ZSET) {
        if (ob->encoding == OBJ_ENCODING_LISTPACK) {
            if ((newzl = activeDefragAlloc(ob->ptr)))
                ob->ptr = newzl;
        } else if (ob->encoding == OBJ_ENCODING_SKIPLIST) {
            defragZsetSkiplist(db, de);
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (ob->type == OBJ_HASH) {
        if (ob->encoding == OBJ_ENCODING_LISTPACK) {
            if ((newzl = activeDefragAlloc(ob->ptr)))
                ob->ptr = newzl;
        } else if (ob->encoding == OBJ_ENCODING_LISTPACK_EX) {
            listpackEx *newlpt, *lpt = (listpackEx*)ob->ptr;
            if ((newlpt = activeDefragAlloc(lpt)))
                ob->ptr = lpt = newlpt;
            if ((newzl = activeDefragAlloc(lpt->lp)))
                lpt->lp = newzl;
        } else if (ob->encoding == OBJ_ENCODING_HT) {
            defragHash(db, de);
        } else {
            serverPanic("Unknown hash encoding");
        }
    } else if (ob->type == OBJ_STREAM) {
        defragStream(db, de);
    } else if (ob->type == OBJ_MODULE) {
        defragModule(db, de);
    } else {
        serverPanic("Unknown object type");
    }
}

/* 主数据库字典的碎片整理扫描回调。 */
void defragScanCallback(void *privdata, const dictEntry *de) {
    long long hits_before = server.stat_active_defrag_hits;
    defragKey((defragCtx*)privdata, (dictEntry*)de);
    if (server.stat_active_defrag_hits != hits_before)
        server.stat_active_defrag_key_hits++;
    else
        server.stat_active_defrag_key_misses++;
    server.stat_active_defrag_scanned++;
}

/* 从 jemalloc 获取碎片率的工具函数。
 * 关键在于仅比较属于 jemalloc 的堆映射，跳过 jemalloc
 * 保留的备用映射。由于我们使用此碎片率来决定是否应采取
 * 碎片整理操作，误判会导致碎片整理器浪费大量 CPU
 * 却无法获得任何效果。 */
float getAllocatorFragmentation(size_t *out_frag_bytes) {
    size_t resident, active, allocated, frag_smallbins_bytes;
    zmalloc_get_allocator_info(1, &allocated, &active, &resident, NULL, NULL, &frag_smallbins_bytes);

    if (server.lua_arena != UINT_MAX) {
        size_t lua_resident, lua_active, lua_allocated, lua_frag_smallbins_bytes;
        zmalloc_get_allocator_info_by_arena(server.lua_arena, 0, &lua_allocated, &lua_active, &lua_resident, &lua_frag_smallbins_bytes);
        resident -= lua_resident;
        active -= lua_active;
        allocated -= lua_allocated;
        frag_smallbins_bytes -= lua_frag_smallbins_bytes;
    }

    /* 计算碎片率：small bins 中浪费的内存（可碎片整理的部分）
     * 占总已分配内存（包括 large bins）的比例。
     * 这样计算是因为如果大部分内存使用来自 large bins，
     * 即使显示高百分比，对用户来说实际浪费的内存并不多。 */
    float frag_pct = (float)frag_smallbins_bytes / allocated * 100;
    float rss_pct = ((float)resident / allocated)*100 - 100;
    size_t rss_bytes = resident - allocated;
    if(out_frag_bytes)
        *out_frag_bytes = frag_smallbins_bytes;
    serverLog(LL_DEBUG,
        "allocated=%zu, active=%zu, resident=%zu, frag=%.2f%% (%.2f%% rss), frag_bytes=%zu (%zu rss)",
        allocated, active, resident, frag_pct, rss_pct, frag_smallbins_bytes, rss_bytes);
    return frag_pct;
}

/* Pub/Sub 字典的碎片整理扫描回调。 */
void defragPubsubScanCallback(void *privdata, const dictEntry *de) {
    defragCtx *ctx = privdata;
    defragPubSubCtx *pubsub_ctx = ctx->privdata;
    kvstore *pubsub_channels = pubsub_ctx->pubsub_channels;
    robj *newchannel, *channel = dictGetKey(de);
    dict *newclients, *clients = dictGetVal(de);

    /* 尝试整理频道名称。 */
    serverAssert(channel->refcount == (int)dictSize(clients) + 1);
    newchannel = activeDefragStringObEx(channel, dictSize(clients) + 1);
    if (newchannel) {
        kvstoreDictSetKey(pubsub_channels, ctx->slot, (dictEntry*)de, newchannel);

        /* 频道名称由客户端的 pubsub(shard) 和服务端的 pubsub(shard) 共享，
         * 整理频道名称后，需要更新客户端字典中的引用。 */
        dictIterator *di = dictGetIterator(clients);
        dictEntry *clientde;
        while((clientde = dictNext(di)) != NULL) {
            client *c = dictGetKey(clientde);
            dictEntry *pubsub_channel = dictFind(pubsub_ctx->clientPubSubChannels(c), newchannel);
            serverAssert(pubsub_channel);
            dictSetKey(pubsub_ctx->clientPubSubChannels(c), pubsub_channel, newchannel);
        }
        dictReleaseIterator(di);
    }

    /* 尝试整理作为值部分存储的客户端字典。 */
    if ((newclients = dictDefragTables(clients)))
        kvstoreDictSetVal(pubsub_channels, ctx->slot, (dictEntry*)de, newclients);

    server.stat_active_defrag_scanned++;
}

/* 我们可能需要整理其他全局变量，一个小分配可能占据分配器的
 * 整个运行区域。虽然很小，但整理它们仍然很重要。 */
void defragOtherGlobals(void) {

    /* 还有更多指针可以整理（如客户端 argv、输出/AOF 缓冲区等），
     * 但我们假设这些大多是短生命周期的，只需要整理
     * 长期保持静态的分配。 */
    activeDefragSdsDict(evalScriptsDict(), DEFRAG_SDS_DICT_VAL_LUA_SCRIPT);
    moduleDefragGlobals();
    kvstoreDictLUTDefrag(server.pubsub_channels, dictDefragTables);
    kvstoreDictLUTDefrag(server.pubsubshard_channels, dictDefragTables);
}

/* 返回 0 表示可能还需要更多工作（参见非零 cursor），
 * 返回 1 表示时间用完但还有更多工作需要完成。 */
int defragLaterItem(dictEntry *de, unsigned long *cursor, long long endtime, int dbid) {
    if (de) {
        robj *ob = dictGetVal(de);
        if (ob->type == OBJ_LIST && ob->encoding == OBJ_ENCODING_QUICKLIST) {
            return scanLaterList(ob, cursor, endtime);
        } else if (ob->type == OBJ_SET && ob->encoding == OBJ_ENCODING_HT) {
            scanLaterSet(ob, cursor);
        } else if (ob->type == OBJ_ZSET && ob->encoding == OBJ_ENCODING_SKIPLIST) {
            scanLaterZset(ob, cursor);
        } else if (ob->type == OBJ_HASH && ob->encoding == OBJ_ENCODING_HT) {
            scanLaterHash(ob, cursor);
        } else if (ob->type == OBJ_STREAM && ob->encoding == OBJ_ENCODING_STREAM) {
            return scanLaterStreamListpacks(ob, cursor, endtime);
        } else if (ob->type == OBJ_MODULE) {
            robj keyobj;
            initStaticStringObject(keyobj, dictGetKey(de));
            return moduleLateDefrag(&keyobj, ob, cursor, endtime, dbid);
        } else {
            *cursor = 0; /* 自调度延迟处理以来，对象类型/编码可能已改变 */
        }
    } else {
        *cursor = 0; /* 对象可能已被删除 */
    }
    return 0;
}

/* 服务于 defragLaterStep 的静态变量，用于从上次停止的位置继续扫描键。 */
static sds defrag_later_current_key = NULL;
static unsigned long defrag_later_cursor = 0;

/* 如果不需要更多工作返回 0，如果时间用完但还有更多工作则返回 1。 */
int defragLaterStep(redisDb *db, int slot, long long endtime) {
    unsigned int iterations = 0;
    unsigned long long prev_defragged = server.stat_active_defrag_hits;
    unsigned long long prev_scanned = server.stat_active_defrag_scanned;
    long long key_defragged;

    do {
        /* if we're not continuing a scan from the last call or loop, start a new one */
        if (!defrag_later_cursor) {
            listNode *head = listFirst(db->defrag_later);

            /* 移动到下一个键 */
            if (defrag_later_current_key) {
                serverAssert(defrag_later_current_key == head->value);
                listDelNode(db->defrag_later, head);
                defrag_later_cursor = 0;
                defrag_later_current_key = NULL;
            }

            /* 如果到达最后一个则停止。 */
            head = listFirst(db->defrag_later);
            if (!head)
                return 0;

            /* 开始新的键 */
            defrag_later_current_key = head->value;
            defrag_later_cursor = 0;
        }

        /* 每次进入此函数都需要重新从 dict 中获取键（如果它仍然存在） */
        dictEntry *de = kvstoreDictFind(db->keys, slot, defrag_later_current_key);
        key_defragged = server.stat_active_defrag_hits;
        do {
            int quit = 0;
            if (defragLaterItem(de, &defrag_later_cursor, endtime,db->id))
                quit = 1; /* 时间用完，我们没有完成所有工作 */

            /* 每 16 次扫描迭代、512 次指针重分配或 64 个字段
             * （如果一个哈希桶中有大量指针或正在 rehashing），
             * 检查是否达到时间限制。 */
            if (quit || (++iterations > 16 ||
                            server.stat_active_defrag_hits - prev_defragged > 512 ||
                            server.stat_active_defrag_scanned - prev_scanned > 64)) {
                if (quit || ustime() > endtime) {
                    if(key_defragged != server.stat_active_defrag_hits)
                        server.stat_active_defrag_key_hits++;
                    else
                        server.stat_active_defrag_key_misses++;
                    return 1;
                }
                iterations = 0;
                prev_defragged = server.stat_active_defrag_hits;
                prev_scanned = server.stat_active_defrag_scanned;
            }
        } while(defrag_later_cursor);
        if(key_defragged != server.stat_active_defrag_hits)
            server.stat_active_defrag_key_hits++;
        else
            server.stat_active_defrag_key_misses++;
    } while(1);
}

#define INTERPOLATE(x, x1, x2, y1, y2) ( (y1) + ((x)-(x1)) * ((y2)-(y1)) / ((x2)-(x1)) )
#define LIMIT(y, min, max) ((y)<(min)? min: ((y)>(max)? max: (y)))

/* 决定是否需要碎片整理，以及投入多少 CPU 资源 */
void computeDefragCycles(void) {
    size_t frag_bytes;
    float frag_pct = getAllocatorFragmentation(&frag_bytes);
    /* 如果尚未运行且低于阈值，则退出。 */
    if (!server.active_defrag_running) {
        if(frag_pct < server.active_defrag_threshold_lower || frag_bytes < server.active_defrag_ignore_bytes)
            return;
    }

    /* 根据当前碎片率和配置计算碎片整理的自适应激进程度。 */
    int cpu_pct = INTERPOLATE(frag_pct,
            server.active_defrag_threshold_lower,
            server.active_defrag_threshold_upper,
            server.active_defrag_cycle_min,
            server.active_defrag_cycle_max);
    cpu_pct = LIMIT(cpu_pct,
            server.active_defrag_cycle_min,
            server.active_defrag_cycle_max);

    /* 通常允许在扫描期间增加激进程度，但不降低它，
     * 因为碎片率下降时不应降低激进程度。
     * 但当配置发生变更时，应重新评估。 */
    if (cpu_pct > server.active_defrag_running ||
        server.active_defrag_configuration_changed)
    {
        server.active_defrag_running = cpu_pct;
        server.active_defrag_configuration_changed = 0;
        serverLog(LL_VERBOSE,
            "Starting active defrag, frag=%.0f%%, frag_bytes=%zu, cpu=%d%%",
            frag_pct, frag_bytes, cpu_pct);
    }
}

/* 从 serverCron 执行增量碎片整理工作。
 * 工作方式类似于 activeExpireCycle，
 * 即在多次调用之间执行增量工作。 */
void activeDefragCycle(void) {
    static int slot = -1;
    static int current_db = -1;
    static int defrag_later_item_in_progress = 0;
    static int defrag_stage = 0;
    static unsigned long defrag_cursor = 0;
    static redisDb *db = NULL;
    static long long start_scan, start_stat;
    unsigned int iterations = 0;
    unsigned long long prev_defragged = server.stat_active_defrag_hits;
    unsigned long long prev_scanned = server.stat_active_defrag_scanned;
    long long start, timelimit, endtime;
    mstime_t latency;
    int all_stages_finished = 0;
    int quit = 0;

    if (!server.active_defrag_enabled) {
        if (server.active_defrag_running) {
            /* 如果在运行中途禁用了主动碎片整理，下次从头开始。 */
            server.active_defrag_running = 0;
            server.active_defrag_configuration_changed = 0;
            if (db)
                listEmpty(db->defrag_later);
            defrag_later_current_key = NULL;
            defrag_later_cursor = 0;
            current_db = -1;
            defrag_stage = 0;
            defrag_cursor = 0;
            slot = -1;
            defrag_later_item_in_progress = 0;
            db = NULL;
            goto update_metrics;
        }
        return;
    }

    if (hasActiveChildProcess())
        return; /* 在 fork 存在时进行碎片整理只会造成损害。 */

    /* 每秒检查一次碎片率是否足以启动扫描或提高激进程度。 */
    run_with_period(1000) {
        computeDefragCycles();
    }

    /* 通常每秒检查一次，但当配置发生变更时，
     * 希望尽快检查。 */
    if (server.active_defrag_configuration_changed) {
        computeDefragCycles();
        server.active_defrag_configuration_changed = 0;
    }

    if (!server.active_defrag_running)
        return;

    /* 参见 activeExpireCycle 了解时间限制的处理方式。 */
    start = ustime();
    timelimit = 1000000*server.active_defrag_running/server.hz/100;
    if (timelimit <= 0) timelimit = 1;
    endtime = start + timelimit;
    latencyStartMonitor(latency);

    dictDefragFunctions defragfns = {.defragAlloc = activeDefragAlloc};
    do {
        /* if we're not continuing a scan from the last call or loop, start a new one */
        if (!defrag_stage && !defrag_cursor && (slot < 0)) {
            /* 在移动到下一个数据库之前，先完成上一个数据库的遗留工作 */
            if (db && defragLaterStep(db, slot, endtime)) {
                quit = 1; /* 时间用完，我们没有完成所有工作 */
                break; /* 退出函数，下次循环继续 */
            }

            /* 移动到下一个数据库，如果到达最后一个则停止。 */
            if (++current_db >= server.dbnum) {
                /* 整理不属于数据库/键的其他项目 */
                defragOtherGlobals();

                long long now = ustime();
                size_t frag_bytes;
                float frag_pct = getAllocatorFragmentation(&frag_bytes);
                serverLog(LL_VERBOSE,
                    "Active defrag done in %dms, reallocated=%d, frag=%.0f%%, frag_bytes=%zu",
                    (int)((now - start_scan)/1000), (int)(server.stat_active_defrag_hits - start_stat), frag_pct, frag_bytes);

                start_scan = now;
                current_db = -1;
                defrag_stage = 0;
                defrag_cursor = 0;
                slot = -1;
                defrag_later_item_in_progress = 0;
                db = NULL;
                server.active_defrag_running = 0;

                computeDefragCycles(); /* 如果需要另一次扫描，立即开始 */
                if (server.active_defrag_running != 0 && ustime() < endtime)
                    continue;
                break;
            }
            else if (current_db==0) {
                /* 从第一个数据库开始扫描。 */
                start_scan = ustime();
                start_stat = server.stat_active_defrag_hits;
            }

            db = &server.db[current_db];
            kvstoreDictLUTDefrag(db->keys, dictDefragTables);
            kvstoreDictLUTDefrag(db->expires, dictDefragTables);
            defrag_stage = 0;
            defrag_cursor = 0;
            slot = -1;
            defrag_later_item_in_progress = 0;
        }

        /* 此结构体数组保存所有碎片整理阶段的参数。 */
        typedef struct defragStage {
            kvstore *kvs;
            dictScanFunction *scanfn;
            void *privdata;
        } defragStage;
        /* 阶段 0: 键空间, 阶段 1: 过期字典,
         * 阶段 2: pub/sub 频道, 阶段 3: 分片 pub/sub 频道 */
        defragStage defrag_stages[] = {
            {db->keys, defragScanCallback, db},
            {db->expires, scanCallbackCountScanned, NULL},
            {server.pubsub_channels, defragPubsubScanCallback,
                &(defragPubSubCtx){server.pubsub_channels, getClientPubSubChannels}},
            {server.pubsubshard_channels, defragPubsubScanCallback,
                &(defragPubSubCtx){server.pubsubshard_channels, getClientPubSubShardChannels}},
        };
        do {
            int num_stages = sizeof(defrag_stages) / sizeof(defrag_stages[0]);
            serverAssert(defrag_stage < num_stages);
            defragStage *current_stage = &defrag_stages[defrag_stage];

            /* 在扫描下一个桶之前，检查是否有前一个桶遗留的大型键需要扫描 */
            if (defragLaterStep(db, slot, endtime)) {
                quit = 1; /* 时间用完，我们没有完成所有工作 */
                break; /* 退出函数，下次循环继续 */
            }

            if (!defrag_later_item_in_progress) {
                /* 从前一阶段继续碎片整理。
                 * 如果 slot 为 -1，表示此阶段从第一个非空 slot 开始。 */
                if (slot == -1) slot = kvstoreGetFirstNonEmptyDictIndex(current_stage->kvs);
                defrag_cursor = kvstoreDictScanDefrag(current_stage->kvs, slot, defrag_cursor,
                    current_stage->scanfn, &defragfns, &(defragCtx){current_stage->privdata, slot});
            }

            if (!defrag_cursor) {
                /* 仅在常规和大型对象扫描完成后才移动到下一个 slot。 */
                if (listLength(db->defrag_later) > 0) {
                    defrag_later_item_in_progress = 1;
                    continue;
                }

                /* 移动到当前阶段的下一个 slot。如果到达末尾，进入下一阶段。 */
                if ((slot = kvstoreGetNextNonEmptyDictIndex(current_stage->kvs, slot)) == -1)
                    defrag_stage++;
                defrag_later_item_in_progress = 0;
            }

            /* 检查所有碎片整理阶段是否已完成。
             * 如果是，标记为完成并重置阶段计数器以进入下一个数据库。 */
            if (defrag_stage == num_stages) {
                all_stages_finished = 1;
                defrag_stage = 0;
            }
    
            /* 每 16 次扫描迭代、512 次指针重分配或 64 个键
             * （如果一个哈希桶中有大量指针或正在 rehashing），
             * 检查是否达到时间限制。
             * 但无论如何，在此循环中不要开始新的数据库，
             * 因为最后一个数据库之后需要调用 defragOtherGlobals，
             * 它必须在一个周期内完成 */
            if (all_stages_finished ||
                ++iterations > 16 ||
                server.stat_active_defrag_hits - prev_defragged > 512 ||
                server.stat_active_defrag_scanned - prev_scanned > 64)
            {
                /* 如果所有阶段完成或超时则退出。 */
                if (all_stages_finished || ustime() > endtime) {
                    quit = 1;
                    break;
                }
                iterations = 0;
                prev_defragged = server.stat_active_defrag_hits;
                prev_scanned = server.stat_active_defrag_scanned;
            }
        } while(!all_stages_finished && !quit);
    } while(!quit);

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("active-defrag-cycle",latency);

update_metrics:
    if (server.active_defrag_running > 0) {
        if (server.stat_last_active_defrag_time == 0)
            elapsedStart(&server.stat_last_active_defrag_time);
    } else if (server.stat_last_active_defrag_time != 0) {
        server.stat_total_active_defrag_time += elapsedUs(server.stat_last_active_defrag_time);
        server.stat_last_active_defrag_time = 0;
    }
}

#else /* HAVE_DEFRAG */

void activeDefragCycle(void) {
    /* 尚未实现。 */
}

void *activeDefragAlloc(void *ptr) {
    UNUSED(ptr);
    return NULL;
}

robj *activeDefragStringOb(robj *ob) {
    UNUSED(ob);
    return NULL;
}

#endif
