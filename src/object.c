/* Redis Object implementation.
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

/*
 * Redis Object（对象）系统实现
 *
 * 本文件实现了 Redis 的核心对象系统，包括：
 * - 各种类型对象的创建和释放（string、list、set、hash、sorted set、stream、module）
 * - 对象编码转换和优化（如 RAW/EMBSTR/INT 编码之间的转换）
 * - 引用计数管理（refcount）
 * - LRU/LFU 淘汰信息管理
 * - 内存使用量计算和内存诊断
 * - OBJECT 和 MEMORY 命令的实现
 *
 * Redis 中每个值都被封装为一个 robj（Redis Object）结构体，
 * 包含类型（type）、编码（encoding）、引用计数（refcount）等元信息。
 */

#include "server.h"
#include "functions.h"
#include "intset.h"  /* 紧凑的整数集合结构 */
#include <math.h>
#include <ctype.h>

#ifdef __CYGWIN__
#define strtold(a,b) ((long double)strtod((a),(b)))
#endif

/* ===================== 对象的创建和解析 ==================== */

/* 创建一个新的 Redis 对象。
 * 参数 type 为对象类型（如 OBJ_STRING、OBJ_LIST 等），
 * 参数 ptr 为指向实际数据的指针。
 * 默认编码为 OBJ_ENCODING_RAW，引用计数初始化为 1。 */
robj *createObject(int type, void *ptr) {
    robj *o = zmalloc(sizeof(*o));
    o->type = type;
    o->encoding = OBJ_ENCODING_RAW;
    o->ptr = ptr;
    o->refcount = 1;
    o->lru = 0;
    return o;
}

/* 初始化对象的 LRU 或 LFU 信息。
 * 如果当前淘汰策略使用 LFU，则设置 LFU 计数器（高 16 位为时间，低 8 位为访问频率）；
 * 否则设置 LRU 时钟值（用于 LRU 淘汰算法计算空闲时间）。
 * 共享对象（refcount == OBJ_SHARED_REFCOUNT）不需要初始化，直接返回。 */
void initObjectLRUOrLFU(robj *o) {
    if (o->refcount == OBJ_SHARED_REFCOUNT)
        return;
    /* 根据当前淘汰策略设置 LRU 时钟（分钟精度）或 LFU 计数器 */
    if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        o->lru = (LFUGetTimeInMinutes() << 8) | LFU_INIT_VAL;
    } else {
        o->lru = LRU_CLOCK();
    }
    return;
}

/* 设置对象的引用计数为特殊值 OBJ_SHARED_REFCOUNT，使其成为"共享对象"。
 * incrRefCount 和 decrRefCount 会检测到这个特殊值而不会修改引用计数，
 * 这样可以从不同线程安全地访问共享对象（如小整数），无需互斥锁。
 *
 * 创建共享对象的常见模式：
 * robj *myobject = makeObjectShared(createObject(...));
 */
robj *makeObjectShared(robj *o) {
    serverAssert(o->refcount == 1);
    o->refcount = OBJ_SHARED_REFCOUNT;
    return o;
}

/* 创建一个编码为 OBJ_ENCODING_RAW 的字符串对象，
 * 即普通的字符串对象，o->ptr 指向一个有效的 sds 字符串。 */
robj *createRawStringObject(const char *ptr, size_t len) {
    return createObject(OBJ_STRING, sdsnewlen(ptr,len));
}

/* 创建一个编码为 OBJ_ENCODING_EMBSTR 的字符串对象，
 * 即嵌入式字符串对象：sds 字符串与对象本身分配在同一块内存中。
 * 这种编码方式减少了内存分配次数和内存碎片，提高了缓存命中率。
 * 注意：EMBSTR 编码的字符串是不可修改的。 */
robj *createEmbeddedStringObject(const char *ptr, size_t len) {
    /* 一次性分配 robj + sdshdr8 + 字符串数据 + \0 终止符 */
    robj *o = zmalloc(sizeof(robj)+sizeof(struct sdshdr8)+len+1);
    struct sdshdr8 *sh = (void*)(o+1);

    o->type = OBJ_STRING;
    o->encoding = OBJ_ENCODING_EMBSTR;
    o->ptr = sh+1;  /* 指向 sdshdr8 之后的 buf 区域 */
    o->refcount = 1;
    o->lru = 0;

    sh->len = len;
    sh->alloc = len;
    sh->flags = SDS_TYPE_8;
    if (ptr == SDS_NOINIT)
        sh->buf[len] = '\0';
    else if (ptr) {
        memcpy(sh->buf,ptr,len);
        sh->buf[len] = '\0';
    } else {
        memset(sh->buf,0,len+1);
    }
    return o;
}

/* 创建字符串对象：如果长度小于等于 OBJ_ENCODING_EMBSTR_SIZE_LIMIT（44字节），
 * 使用 EMBSTR 编码（对象和字符串在同一块内存中），否则使用 RAW 编码。
 *
 * 当前限制为 44 字节，是为了确保最大的 EMBSTR 字符串对象
 * 仍然能放入 jemalloc 的 64 字节内存块中。 */
#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 44
robj *createStringObject(const char *ptr, size_t len) {
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT)
        return createEmbeddedStringObject(ptr,len);
    else
        return createRawStringObject(ptr,len);
}

/* 与 createRawStringObject 相同，但在分配失败时返回 NULL 而不是崩溃 */
robj *tryCreateRawStringObject(const char *ptr, size_t len) {
    sds str = sdstrynewlen(ptr,len);
    if (!str) return NULL;
    return createObject(OBJ_STRING, str);
}

/* 与 createStringObject 相同，但在分配失败时返回 NULL 而不是崩溃 */
robj *tryCreateStringObject(const char *ptr, size_t len) {
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT)
        return createEmbeddedStringObject(ptr,len);
    else
        return tryCreateRawStringObject(ptr,len);
}

/* 根据指定的标志，从 long long 值创建字符串对象。
 * 标志控制是否允许使用共享对象或整数编码。 */
#define LL2STROBJ_AUTO 0       /* 自动创建最优的字符串对象 */
#define LL2STROBJ_NO_SHARED 1  /* 禁止使用共享对象 */
#define LL2STROBJ_NO_INT_ENC 2 /* 禁止使用整数编码 */
robj *createStringObjectFromLongLongWithOptions(long long value, int flag) {
    robj *o;

    if (value >= 0 && value < OBJ_SHARED_INTEGERS && flag == LL2STROBJ_AUTO) {
        /* 值在共享整数范围内且允许使用共享对象，直接返回预分配的共享整数 */
        o = shared.integers[value];
    } else {
        if ((value >= LONG_MIN && value <= LONG_MAX) && flag != LL2STROBJ_NO_INT_ENC) {
            /* 值在 long 范围内且允许整数编码，使用 OBJ_ENCODING_INT 编码
             * 将整数值直接存储在 ptr 指针中，无需额外分配内存 */
            o = createObject(OBJ_STRING, NULL);
            o->encoding = OBJ_ENCODING_INT;
            o->ptr = (void*)((long)value);
        } else {
            /* 值超出范围或禁止整数编码，转换为字符串后创建对象 */
            char buf[LONG_STR_SIZE];
            int len = ll2string(buf, sizeof(buf), value);
            o = createStringObject(buf, len);
        }
    }
    return o;
}

/* createStringObjectFromLongLongWithOptions 的包装函数，
 * 总是尽可能使用共享对象。 */
robj *createStringObjectFromLongLong(long long value) {
    return createStringObjectFromLongLongWithOptions(value, LL2STROBJ_AUTO);
}

/* 为键空间中的值创建字符串对象时，避免返回共享整数。
 * 当对象用作键空间中的值时（例如 INCR 命令），如果 Redis 配置了
 * 基于 LFU/LRU 的淘汰策略，每个键需要独立的 LFU/LRU 信息，
 * 因此不能使用共享对象（共享对象的 LRU 字段是共享的）。 */
robj *createStringObjectFromLongLongForValue(long long value) {
    if (server.maxmemory == 0 || !(server.maxmemory_policy & MAXMEMORY_FLAG_NO_SHARED_INTEGERS)) {
        /* 如果淘汰策略允许，仍然可以返回共享整数 */
        return createStringObjectFromLongLongWithOptions(value, LL2STROBJ_AUTO);
    } else {
        return createStringObjectFromLongLongWithOptions(value, LL2STROBJ_NO_SHARED);
    }
}

/* 创建一个包含 sds 的字符串对象。这意味着它不能使用整数编码（OBJ_ENCODING_INT），
 * 将始终使用 EMBSTR 编码类型。 */
robj *createStringObjectFromLongLongWithSds(long long value) {
    return createStringObjectFromLongLongWithOptions(value, LL2STROBJ_NO_INT_ENC);
}

/* 从 long double 值创建字符串对象。
 * 如果 humanfriendly 为非零值，不使用指数格式并去除末尾的零，
 * 但这会导致精度损失。否则使用指数格式，不修改 snprintf() 的输出。
 * 'humanfriendly' 选项用于 INCRBYFLOAT 和 HINCRBYFLOAT 命令。 */
robj *createStringObjectFromLongDouble(long double value, int humanfriendly) {
    char buf[MAX_LONG_DOUBLE_CHARS];
    int len = ld2string(buf,sizeof(buf),value,humanfriendly? LD_STR_HUMAN: LD_STR_AUTO);
    return createStringObject(buf,len);
}

/* 复制字符串对象，保证返回的对象与原始对象具有相同的编码。
 * 复制小整数对象（或包含小整数表示的字符串对象）时，
 * 总是返回一个新的非共享对象（refcount == 1）。
 * 返回的对象的引用计数始终设置为 1。 */
robj *dupStringObject(const robj *o) {
    robj *d;

    serverAssert(o->type == OBJ_STRING);

    switch(o->encoding) {
    case OBJ_ENCODING_RAW:
        return createRawStringObject(o->ptr,sdslen(o->ptr));
    case OBJ_ENCODING_EMBSTR:
        return createEmbeddedStringObject(o->ptr,sdslen(o->ptr));
    case OBJ_ENCODING_INT:
        d = createObject(OBJ_STRING, NULL);
        d->encoding = OBJ_ENCODING_INT;
        d->ptr = o->ptr;
        return d;
    default:
        serverPanic("Wrong encoding.");
        break;
    }
}

/* 创建一个 quicklist 编码的 list 对象。
 * fill 参数控制每个节点的最大容量，compress 参数控制压缩深度。 */
robj *createQuicklistObject(int fill, int compress) {
    quicklist *l = quicklistNew(fill, compress);
    robj *o = createObject(OBJ_LIST,l);
    o->encoding = OBJ_ENCODING_QUICKLIST;
    return o;
}

/* 创建一个 listpack 编码的 list 对象。
 * listpack 是一种紧凑的序列化结构，适用于小列表。 */
robj *createListListpackObject(void) {
    unsigned char *lp = lpNew(0);
    robj *o = createObject(OBJ_LIST,lp);
    o->encoding = OBJ_ENCODING_LISTPACK;
    return o;
}

/* 创建一个使用 hashtable（字典）编码的 set 对象。 */
robj *createSetObject(void) {
    dict *d = dictCreate(&setDictType);
    robj *o = createObject(OBJ_SET,d);
    o->encoding = OBJ_ENCODING_HT;
    return o;
}

/* 创建一个使用 intset（整数集合）编码的 set 对象。
 * intset 适用于所有元素都是整数的小集合。 */
robj *createIntsetObject(void) {
    intset *is = intsetNew();
    robj *o = createObject(OBJ_SET,is);
    o->encoding = OBJ_ENCODING_INTSET;
    return o;
}

/* 创建一个使用 listpack 编码的 set 对象。 */
robj *createSetListpackObject(void) {
    unsigned char *lp = lpNew(0);
    robj *o = createObject(OBJ_SET, lp);
    o->encoding = OBJ_ENCODING_LISTPACK;
    return o;
}

/* 创建一个使用 listpack 编码的 hash 对象。
 * hash 对象初始时使用 listpack 编码，当元素过多时会转换为 hashtable 编码。 */
robj *createHashObject(void) {
    unsigned char *zl = lpNew(0);
    robj *o = createObject(OBJ_HASH, zl);
    o->encoding = OBJ_ENCODING_LISTPACK;
    return o;
}

/* 创建一个使用 skiplist 编码的 sorted set（有序集合）对象。
 * zset 同时使用跳表（skiplist）和字典（dict）来实现，
 * 跳表用于范围查询，字典用于按成员名快速查找分数。 */
robj *createZsetObject(void) {
    zset *zs = zmalloc(sizeof(*zs));
    robj *o;

    zs->dict = dictCreate(&zsetDictType);
    zs->zsl = zslCreate();
    o = createObject(OBJ_ZSET,zs);
    o->encoding = OBJ_ENCODING_SKIPLIST;
    return o;
}

/* 创建一个使用 listpack 编码的 sorted set 对象。
 * 适用于小型有序集合。 */
robj *createZsetListpackObject(void) {
    unsigned char *lp = lpNew(0);
    robj *o = createObject(OBJ_ZSET,lp);
    o->encoding = OBJ_ENCODING_LISTPACK;
    return o;
}

/* 创建一个 stream（流）对象。 */
robj *createStreamObject(void) {
    stream *s = streamNew();
    robj *o = createObject(OBJ_STREAM,s);
    o->encoding = OBJ_ENCODING_STREAM;
    return o;
}

/* 创建一个 module（模块）对象。
 * mt 为模块类型定义，value 为模块自定义的数据。 */
robj *createModuleObject(moduleType *mt, void *value) {
    moduleValue *mv = zmalloc(sizeof(*mv));
    mv->type = mt;
    mv->value = value;
    return createObject(OBJ_MODULE,mv);
}

/* 释放字符串对象。只有 RAW 编码需要释放 sds 内存，
 * EMBSTR 编码与 robj 在同一块内存中，随 robj 一起释放，
 * INT 编码不指向额外内存。 */
void freeStringObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}

/* 释放 list 对象。根据编码类型释放对应的底层数据结构。 */
void freeListObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistRelease(o->ptr);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        lpFree(o->ptr);
    } else {
        serverPanic("Unknown list encoding type");
    }
}

/* 释放 set 对象。根据编码类型释放字典、intset 或 listpack。 */
void freeSetObject(robj *o) {
    switch (o->encoding) {
    case OBJ_ENCODING_HT:
        dictRelease((dict*) o->ptr);
        break;
    case OBJ_ENCODING_INTSET:
    case OBJ_ENCODING_LISTPACK:
        zfree(o->ptr);
        break;
    default:
        serverPanic("Unknown set encoding type");
    }
}

/* 释放 sorted set 对象。skiplist 编码需要同时释放字典和跳表。 */
void freeZsetObject(robj *o) {
    zset *zs;
    switch (o->encoding) {
    case OBJ_ENCODING_SKIPLIST:
        zs = o->ptr;
        dictRelease(zs->dict);
        zslFree(zs->zsl);
        zfree(zs);
        break;
    case OBJ_ENCODING_LISTPACK:
        zfree(o->ptr);
        break;
    default:
        serverPanic("Unknown sorted set encoding");
    }
}

/* 释放 hash 对象。委托给 hashTypeFree 处理。 */
void freeHashObject(robj *o) {
    hashTypeFree(o);
}

/* 释放 module 对象。调用模块注册的 free 回调释放模块数据。 */
void freeModuleObject(robj *o) {
    moduleValue *mv = o->ptr;
    mv->type->free(mv->value);
    zfree(mv);
}

/* 释放 stream 对象。 */
void freeStreamObject(robj *o) {
    freeStream(o->ptr);
}

/* 增加对象的引用计数。
 * 特殊引用计数（OBJ_SHARED_REFCOUNT 和 OBJ_STATIC_REFCOUNT）不受影响：
 * - OBJ_SHARED_REFCOUNT：共享对象，引用计数不可变
 * - OBJ_STATIC_REFCOUNT：栈上分配的对象，不应被保留 */
void incrRefCount(robj *o) {
    if (o->refcount < OBJ_FIRST_SPECIAL_REFCOUNT) {
        o->refcount++;
    } else {
        if (o->refcount == OBJ_SHARED_REFCOUNT) {
            /* 无需操作：共享对象的引用计数不可变 */
        } else if (o->refcount == OBJ_STATIC_REFCOUNT) {
            serverPanic("You tried to retain an object allocated in the stack");
        }
    }
}

/* 减少对象的引用计数。当引用计数降为 0 时，释放对象及其底层数据。
 * 特殊引用计数（OBJ_SHARED_REFCOUNT）不会被减少。 */
void decrRefCount(robj *o) {
    if (o->refcount == 1) {
        /* 引用计数为 1，说明没有其他引用，释放对象 */
        switch(o->type) {
        case OBJ_STRING: freeStringObject(o); break;
        case OBJ_LIST: freeListObject(o); break;
        case OBJ_SET: freeSetObject(o); break;
        case OBJ_ZSET: freeZsetObject(o); break;
        case OBJ_HASH: freeHashObject(o); break;
        case OBJ_MODULE: freeModuleObject(o); break;
        case OBJ_STREAM: freeStreamObject(o); break;
        default: serverPanic("Unknown object type"); break;
        }
        zfree(o);
    } else {
        if (o->refcount <= 0) serverPanic("decrRefCount against refcount <= 0");
        if (o->refcount != OBJ_SHARED_REFCOUNT) o->refcount--;
    }
}

/* 释放 sds 字符串的物理内存页面。参见 dismissObject() 的说明。 */
void dismissSds(sds s) {
    dismissMemory(sdsAllocPtr(s), sdsAllocSize(s));
}

/* 释放字符串对象的物理内存页面。参见 dismissObject() 的说明。 */
void dismissStringObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_RAW) {
        dismissSds(o->ptr);
    }
}

/* 释放 list 对象的物理内存页面。参见 dismissObject() 的说明。
 * 只有当平均节点大小大于页面大小时才遍历所有节点，
 * 因为只有在这种情况下才有可能释放出物理页面。 */
void dismissListObject(robj *o, size_t size_hint) {
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *ql = o->ptr;
        serverAssert(ql->len != 0);
        /* 只有当平均节点大小大于页面大小时才遍历所有节点，
         * 这样才有较高的概率真正释放出物理页面。 */
        if (size_hint / ql->len >= server.page_size) {
            quicklistNode *node = ql->head;
            while (node) {
                if (quicklistNodeIsCompressed(node)) {
                    dismissMemory(node->entry, ((quicklistLZF*)node->entry)->sz);
                } else {
                    dismissMemory(node->entry, node->sz);
                }
                node = node->next;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        dismissMemory(o->ptr, lpBytes((unsigned char*)o->ptr));
    } else {
        serverPanic("Unknown list encoding type");
    }
}

/* 释放 set 对象的物理内存页面。参见 dismissObject() 的说明。 */
void dismissSetObject(robj *o, size_t size_hint) {
    if (o->encoding == OBJ_ENCODING_HT) {
        dict *set = o->ptr;
        serverAssert(dictSize(set) != 0);
        /* 只有当平均成员大小大于页面大小时才遍历所有节点 */
        if (size_hint / dictSize(set) >= server.page_size) {
            dictEntry *de;
            dictIterator *di = dictGetIterator(set);
            while ((de = dictNext(di)) != NULL) {
                dismissSds(dictGetKey(de));
            }
            dictReleaseIterator(di);
        }

        /* 释放哈希表本身的内存页面 */
        dismissMemory(set->ht_table[0], DICTHT_SIZE(set->ht_size_exp[0])*sizeof(dictEntry*));
        dismissMemory(set->ht_table[1], DICTHT_SIZE(set->ht_size_exp[1])*sizeof(dictEntry*));
    } else if (o->encoding == OBJ_ENCODING_INTSET) {
        dismissMemory(o->ptr, intsetBlobLen((intset*)o->ptr));
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        dismissMemory(o->ptr, lpBytes((unsigned char *)o->ptr));
    } else {
        serverPanic("Unknown set encoding type");
    }
}

/* 释放 sorted set 对象的物理内存页面。参见 dismissObject() 的说明。 */
void dismissZsetObject(robj *o, size_t size_hint) {
    if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        zskiplist *zsl = zs->zsl;
        serverAssert(zsl->length != 0);
        /* 只有当平均成员大小大于页面大小时才遍历跳表节点 */
        if (size_hint / zsl->length >= server.page_size) {
            zskiplistNode *zn = zsl->tail;
            while (zn != NULL) {
                dismissSds(zn->ele);
                zn = zn->backward;
            }
        }

        /* 释放哈希表本身的内存页面 */
        dict *d = zs->dict;
        dismissMemory(d->ht_table[0], DICTHT_SIZE(d->ht_size_exp[0])*sizeof(dictEntry*));
        dismissMemory(d->ht_table[1], DICTHT_SIZE(d->ht_size_exp[1])*sizeof(dictEntry*));
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        dismissMemory(o->ptr, lpBytes((unsigned char*)o->ptr));
    } else {
        serverPanic("Unknown zset encoding type");
    }
}

/* 释放 hash 对象的物理内存页面。参见 dismissObject() 的说明。 */
void dismissHashObject(robj *o, size_t size_hint) {
    if (o->encoding == OBJ_ENCODING_HT) {
        dict *d = o->ptr;
        serverAssert(dictSize(d) != 0);
        /* 只有当平均字段/值大小大于页面大小时才遍历所有字段 */
        if (size_hint / dictSize(d) >= server.page_size) {
            dictEntry *de;
            dictIterator *di = dictGetIterator(d);
            while ((de = dictNext(di)) != NULL) {
                /* 只释放值的内存，因为字段名通常很小 */
                dismissSds(dictGetVal(de));
            }
            dictReleaseIterator(di);
        }

        /* 释放哈希表本身的内存页面 */
        dismissMemory(d->ht_table[0], DICTHT_SIZE(d->ht_size_exp[0])*sizeof(dictEntry*));
        dismissMemory(d->ht_table[1], DICTHT_SIZE(d->ht_size_exp[1])*sizeof(dictEntry*));
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        dismissMemory(o->ptr, lpBytes((unsigned char*)o->ptr));
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = o->ptr;
        dismissMemory(lpt->lp, lpBytes((unsigned char*)lpt->lp));
    } else {
        serverPanic("Unknown hash encoding type");
    }
}

/* 释放 stream 对象的物理内存页面。参见 dismissObject() 的说明。
 * 只遍历 stream 的条目（entries），虽然 size_hint 可能包含
 * 序列化的消费者组信息，但通常 stream 条目占据了大部分空间。 */
void dismissStreamObject(robj *o, size_t size_hint) {
    stream *s = o->ptr;
    rax *rax = s->rax;
    if (raxSize(rax) == 0) return;

    /* 只有当平均条目大小大于页面大小时才遍历所有条目 */
    if (size_hint / raxSize(rax) >= server.page_size) {
        raxIterator ri;
        raxStart(&ri,rax);
        raxSeek(&ri,"^",NULL,0);
        while (raxNext(&ri)) {
            dismissMemory(ri.data, lpBytes(ri.data));
        }
        raxStop(&ri);
    }
}

/* 在 fork 子进程中创建快照时，主进程和子进程共享相同的物理内存页面。
 * 当主进程因写操作修改键时，会触发写时复制（CoW），消耗物理内存。
 * 在子进程中，序列化键和值之后，数据不会再被访问，
 * 因此为了减少不必要的 CoW，我们尝试将内存释放回操作系统。
 * 参见 dismissMemory()。
 *
 * 由于遍历复杂数据类型的所有节点/字段/成员/条目的开销很大，
 * 只有当估计的单个分配的平均大小大于操作系统页面大小时才遍历并释放。
 * 'size_hint' 是序列化值的大小。这种方法不太准确，
 * 但可以减少对可能不会释放任何内存的复杂数据类型的不必要遍历。 */
void dismissObject(robj *o, size_t size_hint) {
    /* 如果启用了透明大页面（THP），madvise(MADV_DONTNEED) 可能不起作用 */
    if (server.thp_enabled) return;

    /* 目前只在 Linux 上使用 jemalloc 时才使用 zmadvise_dontneed，
     * 因此在其他情况下避免无意义的遍历。 */
#if defined(USE_JEMALLOC) && defined(__linux__)
    if (o->refcount != 1) return;
    switch(o->type) {
        case OBJ_STRING: dismissStringObject(o); break;
        case OBJ_LIST: dismissListObject(o, size_hint); break;
        case OBJ_SET: dismissSetObject(o, size_hint); break;
        case OBJ_ZSET: dismissZsetObject(o, size_hint); break;
        case OBJ_HASH: dismissHashObject(o, size_hint); break;
        case OBJ_STREAM: dismissStreamObject(o, size_hint); break;
        default: break;
    }
#else
    UNUSED(o); UNUSED(size_hint);
#endif
}

/* decrRefCount 的 void* 参数版本，可作为数据结构的 free 方法使用。
 * 适用于期望 'void free_object(void*)' 原型的场景。 */
void decrRefCountVoid(void *o) {
    decrRefCount(o);
}

/* 检查对象的类型是否与期望的类型匹配。
 * NULL 被视为空键，不报错。
 * 如果类型不匹配，向客户端发送 WRONGTYPE 错误并返回 1。 */
int checkType(client *c, robj *o, int type) {
    /* NULL 被视为空键 */
    if (o && o->type != type) {
        addReplyErrorObject(c,shared.wrongtypeerr);
        return 1;
    }
    return 0;
}

/* 检查 sds 字符串是否可以表示为 long long 整数。 */
int isSdsRepresentableAsLongLong(sds s, long long *llval) {
    return string2ll(s,sdslen(s),llval) ? C_OK : C_ERR;
}

/* 检查字符串对象是否可以表示为 long long 整数。
 * INT 编码的对象直接取出指针中的值，其他编码则解析字符串。 */
int isObjectRepresentableAsLongLong(robj *o, long long *llval) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    if (o->encoding == OBJ_ENCODING_INT) {
        if (llval) *llval = (long) o->ptr;
        return C_OK;
    } else {
        return isSdsRepresentableAsLongLong(o->ptr,llval);
    }
}

/* 优化字符串对象中的 SDS 字符串，释放多余的空闲空间。
 * 当 SDS 末尾有超过 10% 的空闲空间时进行优化。
 * 字符串可能在以下情况下有空闲空间：
 * 1. 当参数长度大于 PROTO_MBULK_BIG_ARG 时，查询缓冲区可能被直接用作 SDS 字符串
 * 2. 使用 Lua 的参数缓存机制时
 * 3. 从 RM_TrimStringAllocation 调用时（trim_small_values 为 true） */
void trimStringObjectIfNeeded(robj *o, int trim_small_values) {
    if (o->encoding != OBJ_ENCODING_RAW) return;
    size_t len = sdslen(o->ptr);
    if (len >= PROTO_MBULK_BIG_ARG ||
        trim_small_values||
        (server.executing_client && server.executing_client->flags & CLIENT_SCRIPT && len < LUA_CMD_OBJCACHE_MAX_LEN)) {
        if (sdsavail(o->ptr) > len/10) {
            o->ptr = sdsRemoveFreeSpace(o->ptr, 0);
        }
    }
}

/* 尝试对字符串对象进行编码优化以节省空间。
 * 优化策略（按优先级）：
 * 1. 如果字符串可以表示为小整数（0-9999），使用共享整数对象
 * 2. 如果字符串可以表示为 long 整数，使用 INT 编码
 * 3. 如果字符串较短（<=44字节），使用 EMBSTR 编码
 * 4. 否则尝试优化 SDS 字符串的内存分配 */
robj *tryObjectEncodingEx(robj *o, int try_trim) {
    long value;
    sds s = o->ptr;
    size_t len;

    /* 确保这是字符串对象，这是本函数唯一处理的类型。
     * 其他类型使用内存高效的编码表示，但由实现该类型的命令处理。 */
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);

    /* 只对 RAW 或 EMBSTR 编码的对象尝试特殊编码，
     * 即只有仍然是实际字符数组的对象才进行优化。 */
    if (!sdsEncodedObject(o)) return o;

    /* 对共享对象进行编码是不安全的：共享对象可能在 Redis 的"对象空间"
     * 中的任何地方被共享，可能出现在无法处理优化后编码的地方。
     * 我们只在键空间中作为值时才处理它们。 */
     if (o->refcount > 1) return o;

    /* 检查是否可以将此字符串表示为 long 整数。
     * 长度超过 20 个字符的字符串肯定不能表示为 32 位或 64 位整数。 */
    len = sdslen(s);
    if (len <= 20 && string2l(s,len,&value)) {
        /* 此对象可以编码为 long。尝试使用共享对象。
         * 注意：当使用 maxmemory 时避免使用共享整数，
         * 因为每个对象需要有私有的 LRU 字段才能使 LRU 算法正常工作。 */
        if ((server.maxmemory == 0 ||
            !(server.maxmemory_policy & MAXMEMORY_FLAG_NO_SHARED_INTEGERS)) &&
            value >= 0 &&
            value < OBJ_SHARED_INTEGERS)
        {
            /* 使用共享整数对象，释放当前对象 */
            decrRefCount(o);
            return shared.integers[value];
        } else {
            if (o->encoding == OBJ_ENCODING_RAW) {
                /* RAW 编码转换为 INT 编码，直接将值存储在指针中 */
                sdsfree(o->ptr);
                o->encoding = OBJ_ENCODING_INT;
                o->ptr = (void*) value;
                return o;
            } else if (o->encoding == OBJ_ENCODING_EMBSTR) {
                /* EMBSTR 编码无法原地转换，创建新的对象 */
                decrRefCount(o);
                return createStringObjectFromLongLongForValue(value);
            }
        }
    }

    /* 如果字符串较短且仍然是 RAW 编码，尝试使用更高效的 EMBSTR 编码。
     * 在这种表示中，对象和 SDS 字符串分配在同一块内存中，
     * 以节省空间和减少缓存未命中。 */
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT) {
        robj *emb;

        if (o->encoding == OBJ_ENCODING_EMBSTR) return o;
        emb = createEmbeddedStringObject(s,sdslen(s));
        decrRefCount(o);
        return emb;
    }

    /* 无法进一步优化编码...
     * 最后尝试优化 SDS 字符串本身的内存分配 */
    if (try_trim)
        trimStringObjectIfNeeded(o, 0);

    /* 返回原始对象 */
    return o;
}

/* tryObjectEncodingEx 的便捷包装，总是尝试 trim */
robj *tryObjectEncoding(robj *o) {
    return tryObjectEncodingEx(o, 1);
}

/* 获取编码对象的解码版本（作为新对象返回）。
 * 如果对象已经是 RAW 编码（sds 字符串），则只增加引用计数。
 * 如果是 INT 编码，将整数值转换为字符串后创建新对象。
 * 注意：返回的对象的引用计数总是 >= 1，调用者需要在使用完后调用 decrRefCount。 */
robj *getDecodedObject(robj *o) {
    robj *dec;

    if (sdsEncodedObject(o)) {
        /* 已经是字符串编码，直接增加引用计数 */
        incrRefCount(o);
        return o;
    }
    if (o->type == OBJ_STRING && o->encoding == OBJ_ENCODING_INT) {
        /* INT 编码，将整数转换为字符串 */
        char buf[32];

        ll2string(buf,32,(long)o->ptr);
        dec = createStringObject(buf,strlen(buf));
        return dec;
    } else {
        serverPanic("Unknown encoding type");
    }
}

/* 根据标志使用 strcmp() 或 strcoll() 比较两个字符串对象。
 * 注意：对象可能是整数编码的。在这种情况下，使用 ll2string() 在栈上
 * 获取数字的字符串表示并比较字符串，这比调用 getDecodedObject() 快得多。
 *
 * 重要说明：当使用 REDIS_COMPARE_BINARY 时，使用二进制安全的比较。 */

#define REDIS_COMPARE_BINARY (1<<0)  /* 二进制比较 */
#define REDIS_COMPARE_COLL (1<<1)    /* 本地化排序比较 */

int compareStringObjectsWithFlags(const robj *a, const robj *b, int flags) {
    serverAssertWithInfo(NULL,a,a->type == OBJ_STRING && b->type == OBJ_STRING);
    char bufa[128], bufb[128], *astr, *bstr;
    size_t alen, blen, minlen;

    if (a == b) return 0;
    if (sdsEncodedObject(a)) {
        astr = a->ptr;
        alen = sdslen(astr);
    } else {
        alen = ll2string(bufa,sizeof(bufa),(long) a->ptr);
        astr = bufa;
    }
    if (sdsEncodedObject(b)) {
        bstr = b->ptr;
        blen = sdslen(bstr);
    } else {
        blen = ll2string(bufb,sizeof(bufb),(long) b->ptr);
        bstr = bufb;
    }
    if (flags & REDIS_COMPARE_COLL) {
        /* 使用本地化排序比较（受 locale 影响） */
        return strcoll(astr,bstr);
    } else {
        /* 使用二进制比较（字节级比较） */
        int cmp;

        minlen = (alen < blen) ? alen : blen;
        cmp = memcmp(astr,bstr,minlen);
        if (cmp == 0) return alen-blen;
        return cmp;
    }
}

/* 使用二进制比较的包装函数 */
int compareStringObjects(const robj *a, const robj *b) {
    return compareStringObjectsWithFlags(a,b,REDIS_COMPARE_BINARY);
}

/* 使用本地化排序比较的包装函数 */
int collateStringObjects(const robj *a, const robj *b) {
    return compareStringObjectsWithFlags(a,b,REDIS_COMPARE_COLL);
}

/* 比较两个字符串对象是否相等，相等返回 1，否则返回 0。
 * 此函数比 (compareStringObject(a,b) == 0) 更快，
 * 因为它对整数编码的对象进行了额外优化：
 * 如果两个对象都是 INT 编码，直接比较指针值即可。 */
int equalStringObjects(robj *a, robj *b) {
    if (a->encoding == OBJ_ENCODING_INT &&
        b->encoding == OBJ_ENCODING_INT){
        /* 如果两个字符串都是整数编码，直接比较存储的 long 值 */
        return a->ptr == b->ptr;
    } else {
        return compareStringObjects(a,b) == 0;
    }
}

/* 获取字符串对象的长度。
 * sds 编码直接返回 sdslen，INT 编码返回数字的位数。 */
size_t stringObjectLen(robj *o) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    if (sdsEncodedObject(o)) {
        return sdslen(o->ptr);
    } else {
        return sdigits10((long)o->ptr);
    }
}

/* 从字符串对象中提取 double 值。
 * NULL 对象返回 0，sds 编码解析字符串，INT 编码直接转换。 */
int getDoubleFromObject(const robj *o, double *target) {
    double value;

    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
        if (sdsEncodedObject(o)) {
            if (!string2d(o->ptr, sdslen(o->ptr), &value))
                return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
    *target = value;
    return C_OK;
}

/* 从字符串对象中提取 double 值，失败时向客户端回复错误信息。 */
int getDoubleFromObjectOrReply(client *c, robj *o, double *target, const char *msg) {
    double value;
    if (getDoubleFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not a valid float");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

/* 从字符串对象中提取 long double 值。 */
int getLongDoubleFromObject(robj *o, long double *target) {
    long double value;

    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
        if (sdsEncodedObject(o)) {
            if (!string2ld(o->ptr, sdslen(o->ptr), &value))
                return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
    *target = value;
    return C_OK;
}

/* 从字符串对象中提取 long double 值，失败时向客户端回复错误信息。 */
int getLongDoubleFromObjectOrReply(client *c, robj *o, long double *target, const char *msg) {
    long double value;
    if (getLongDoubleFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not a valid float");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

/* 从字符串对象中提取 long long 值。 */
int getLongLongFromObject(robj *o, long long *target) {
    long long value;

    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
        if (sdsEncodedObject(o)) {
            if (string2ll(o->ptr,sdslen(o->ptr),&value) == 0) return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
    if (target) *target = value;
    return C_OK;
}

/* 从字符串对象中提取 long long 值，失败时向客户端回复错误信息。 */
int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg) {
    long long value;
    if (getLongLongFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not an integer or out of range");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

/* 从字符串对象中提取 long 值，超出范围时向客户端回复错误信息。 */
int getLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg) {
    long long value;

    if (getLongLongFromObjectOrReply(c, o, &value, msg) != C_OK) return C_ERR;
    if (value < LONG_MIN || value > LONG_MAX) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is out of range");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

/* 从字符串对象中提取 long 值，并检查是否在 [min, max] 范围内。
 * 超出范围时向客户端回复错误信息。 */
int getRangeLongFromObjectOrReply(client *c, robj *o, long min, long max, long *target, const char *msg) {
    if (getLongFromObjectOrReply(c, o, target, msg) != C_OK) return C_ERR;
    if (*target < min || *target > max) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyErrorFormat(c,"value is out of range, value must between %ld and %ld", min, max);
        }
        return C_ERR;
    }
    return C_OK;
}

/* 从字符串对象中提取正的 long 值（>= 0）。 */
int getPositiveLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg) {
    if (msg) {
        return getRangeLongFromObjectOrReply(c, o, 0, LONG_MAX, target, msg);
    } else {
        return getRangeLongFromObjectOrReply(c, o, 0, LONG_MAX, target, "value is out of range, must be positive");
    }
}

/* 从字符串对象中提取 int 值。 */
int getIntFromObjectOrReply(client *c, robj *o, int *target, const char *msg) {
    long value;

    if (getRangeLongFromObjectOrReply(c, o, INT_MIN, INT_MAX, &value, msg) != C_OK)
        return C_ERR;

    *target = value;
    return C_OK;
}

/* 将编码常量转换为可读的字符串名称。
 * 用于 OBJECT ENCODING 命令和调试输出。 */
char *strEncoding(int encoding) {
    switch(encoding) {
    case OBJ_ENCODING_RAW: return "raw";
    case OBJ_ENCODING_INT: return "int";
    case OBJ_ENCODING_HT: return "hashtable";
    case OBJ_ENCODING_QUICKLIST: return "quicklist";
    case OBJ_ENCODING_LISTPACK: return "listpack";
    case OBJ_ENCODING_LISTPACK_EX: return "listpackex";
    case OBJ_ENCODING_INTSET: return "intset";
    case OBJ_ENCODING_SKIPLIST: return "skiplist";
    case OBJ_ENCODING_EMBSTR: return "embstr";
    case OBJ_ENCODING_STREAM: return "stream";
    default: return "unknown";
    }
}

/* =========================== 内存自省 ========================= */


/* 估算用于存储 Stream ID 的基数树（radix tree）的内存使用量。
 *
 * 注意：准确估算基数树的大小并不简单，因此我们近似计算：
 * 每个键（ID）考虑 16 字节的数据开销，然后加上节点数量，
 * 再加上数据和子指针的一些开销。这个"秘诀"是通过检查
 * 实际工作负载创建的平均基数树，然后调整常数来得到
 * 与实际内存使用大致匹配的数字。
 *
 * 实际上，节点和键的数量可能因插入速度不同而有所差异，
 * 这会影响基数树压缩前缀的能力。 */
size_t streamRadixTreeMemoryUsage(rax *rax) {
    size_t size = sizeof(*rax);
    size = rax->numele * sizeof(streamID);
    size += rax->numnodes * sizeof(raxNode);
    /* 加上辅助数据指针、子节点等的固定开销 */
    size += rax->numnodes * sizeof(long)*30;
    return size;
}

/* 返回键的值在 RAM 中占用的字节数。
 * 注意：返回的值只是近似值，特别是对于聚合数据类型，
 * 只检查 "sample_size" 个元素并取平均值来估算总大小。 */
#define OBJ_COMPUTE_SIZE_DEF_SAMPLES 5 /* 默认采样数量 */
size_t objectComputeSize(robj *key, robj *o, size_t sample_size, int dbid) {
    dict *d;
    dictIterator *di;
    struct dictEntry *de;
    size_t asize = 0, elesize = 0, samples = 0;

    if (o->type == OBJ_STRING) {
        if(o->encoding == OBJ_ENCODING_INT) {
            asize = sizeof(*o);
        } else if(o->encoding == OBJ_ENCODING_RAW) {
            asize = sdsZmallocSize(o->ptr)+sizeof(*o);
        } else if(o->encoding == OBJ_ENCODING_EMBSTR) {
            asize = zmalloc_size((void *)o);
        } else {
            serverPanic("Unknown string encoding");
        }
    } else if (o->type == OBJ_LIST) {
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            quicklist *ql = o->ptr;
            quicklistNode *node = ql->head;
            asize = sizeof(*o)+sizeof(quicklist);
            do {
                elesize += sizeof(quicklistNode)+zmalloc_size(node->entry);
                samples++;
            } while ((node = node->next) && samples < sample_size);
            asize += (double)elesize/samples*ql->len;
        } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
            asize = sizeof(*o)+zmalloc_size(o->ptr);
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (o->type == OBJ_SET) {
        if (o->encoding == OBJ_ENCODING_HT) {
            d = o->ptr;
            di = dictGetIterator(d);
            asize = sizeof(*o)+sizeof(dict)+(sizeof(struct dictEntry*)*dictBuckets(d));
            while((de = dictNext(di)) != NULL && samples < sample_size) {
                sds ele = dictGetKey(de);
                elesize += dictEntryMemUsage() + sdsZmallocSize(ele);
                samples++;
            }
            dictReleaseIterator(di);
            if (samples) asize += (double)elesize/samples*dictSize(d);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
            asize = sizeof(*o)+zmalloc_size(o->ptr);
        } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
            asize = sizeof(*o)+zmalloc_size(o->ptr);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        if (o->encoding == OBJ_ENCODING_LISTPACK) {
            asize = sizeof(*o)+zmalloc_size(o->ptr);
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            d = ((zset*)o->ptr)->dict;
            zskiplist *zsl = ((zset*)o->ptr)->zsl;
            zskiplistNode *znode = zsl->header->level[0].forward;
            asize = sizeof(*o)+sizeof(zset)+sizeof(zskiplist)+sizeof(dict)+
                    (sizeof(struct dictEntry*)*dictBuckets(d))+
                    zmalloc_size(zsl->header);
            while(znode != NULL && samples < sample_size) {
                elesize += sdsZmallocSize(znode->ele);
                elesize += dictEntryMemUsage()+zmalloc_size(znode);
                samples++;
                znode = znode->level[0].forward;
            }
            if (samples) asize += (double)elesize/samples*dictSize(d);
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        if (o->encoding == OBJ_ENCODING_LISTPACK) {
            asize = sizeof(*o)+zmalloc_size(o->ptr);
        } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
            listpackEx *lpt = o->ptr;
            asize = sizeof(*o) + zmalloc_size(lpt) + zmalloc_size(lpt->lp);
        } else if (o->encoding == OBJ_ENCODING_HT) {
            d = o->ptr;
            di = dictGetIterator(d);
            asize = sizeof(*o)+sizeof(dict)+(sizeof(struct dictEntry*)*dictBuckets(d));
            while((de = dictNext(di)) != NULL && samples < sample_size) {
                hfield ele = dictGetKey(de);
                sds ele2 = dictGetVal(de);
                elesize += hfieldZmallocSize(ele) + sdsZmallocSize(ele2);
                elesize += dictEntryMemUsage();
                samples++;
            }
            dictReleaseIterator(di);
            if (samples) asize += (double)elesize/samples*dictSize(d);
        } else {
            serverPanic("Unknown hash encoding");
        }
    } else if (o->type == OBJ_STREAM) {
        stream *s = o->ptr;
        asize = sizeof(*o)+sizeof(*s);
        asize += streamRadixTreeMemoryUsage(s->rax);

        /* Now we have to add the listpacks. The last listpack is often non
         * complete, so we estimate the size of the first N listpacks, and
         * use the average to compute the size of the first N-1 listpacks, and
         * finally add the real size of the last node. */
        raxIterator ri;
        raxStart(&ri,s->rax);
        raxSeek(&ri,"^",NULL,0);
        size_t lpsize = 0, samples = 0;
        while(samples < sample_size && raxNext(&ri)) {
            unsigned char *lp = ri.data;
            /* Use the allocated size, since we overprovision the node initially. */
            lpsize += zmalloc_size(lp);
            samples++;
        }
        if (s->rax->numele <= samples) {
            asize += lpsize;
        } else {
            if (samples) lpsize /= samples; /* Compute the average. */
            asize += lpsize * (s->rax->numele-1);
            /* No need to check if seek succeeded, we enter this branch only
             * if there are a few elements in the radix tree. */
            raxSeek(&ri,"$",NULL,0);
            raxNext(&ri);
            /* Use the allocated size, since we overprovision the node initially. */
            asize += zmalloc_size(ri.data);
        }
        raxStop(&ri);

        /* Consumer groups also have a non trivial memory overhead if there
         * are many consumers and many groups, let's count at least the
         * overhead of the pending entries in the groups and consumers
         * PELs. */
        if (s->cgroups) {
            raxStart(&ri,s->cgroups);
            raxSeek(&ri,"^",NULL,0);
            while(raxNext(&ri)) {
                streamCG *cg = ri.data;
                asize += sizeof(*cg);
                asize += streamRadixTreeMemoryUsage(cg->pel);
                asize += sizeof(streamNACK)*raxSize(cg->pel);

                /* For each consumer we also need to add the basic data
                 * structures and the PEL memory usage. */
                raxIterator cri;
                raxStart(&cri,cg->consumers);
                raxSeek(&cri,"^",NULL,0);
                while(raxNext(&cri)) {
                    streamConsumer *consumer = cri.data;
                    asize += sizeof(*consumer);
                    asize += sdslen(consumer->name);
                    asize += streamRadixTreeMemoryUsage(consumer->pel);
                    /* Don't count NACKs again, they are shared with the
                     * consumer group PEL. */
                }
                raxStop(&cri);
            }
            raxStop(&ri);
        }
    } else if (o->type == OBJ_MODULE) {
        asize = moduleGetMemUsage(key, o, sample_size, dbid);
    } else {
        serverPanic("Unknown object type");
    }
    return asize;
}

/* 释放 getMemoryOverheadData() 返回的数据结构。 */
void freeMemoryOverheadData(struct redisMemOverhead *mh) {
    zfree(mh->db);
    zfree(mh);
}

/* 返回一个填充了内存开销信息的 redisMemOverhead 结构体，
 * 用于 MEMORY STATS 和 INFO 命令。
 * 返回的结构体指针应通过 freeMemoryOverheadData() 释放。 */
struct redisMemOverhead *getMemoryOverheadData(void) {
    int j;
    size_t mem_total = 0;
    size_t mem = 0;
    size_t zmalloc_used = zmalloc_used_memory();
    struct redisMemOverhead *mh = zcalloc(sizeof(*mh));

    mh->total_allocated = zmalloc_used;
    mh->startup_allocated = server.initial_memory_usage;
    mh->peak_allocated = server.stat_peak_memory;
    mh->total_frag =
        (float)server.cron_malloc_stats.process_rss / server.cron_malloc_stats.zmalloc_used;
    mh->total_frag_bytes =
        server.cron_malloc_stats.process_rss - server.cron_malloc_stats.zmalloc_used;
    /* Starting with redis 7.4, the lua memory is part of the total memory usage
     * of redis, and that includes RSS and all other memory metrics. We only want
     * to deduct it from active defrag. */
    size_t frag_smallbins_bytes =
        server.cron_malloc_stats.allocator_frag_smallbins_bytes - server.cron_malloc_stats.lua_allocator_frag_smallbins_bytes;
    size_t allocated =
        server.cron_malloc_stats.allocator_allocated - server.cron_malloc_stats.lua_allocator_allocated;
    mh->allocator_frag = (float)frag_smallbins_bytes / allocated + 1;
    mh->allocator_frag_bytes = frag_smallbins_bytes;
    mh->allocator_rss =
        (float)server.cron_malloc_stats.allocator_resident / server.cron_malloc_stats.allocator_active;
    mh->allocator_rss_bytes =
        server.cron_malloc_stats.allocator_resident - server.cron_malloc_stats.allocator_active;
    mh->rss_extra =
        (float)server.cron_malloc_stats.process_rss / server.cron_malloc_stats.allocator_resident;
    mh->rss_extra_bytes =
        server.cron_malloc_stats.process_rss - server.cron_malloc_stats.allocator_resident;

    mem_total += server.initial_memory_usage;

    /* Replication backlog and replicas share one global replication buffer,
     * only if replication buffer memory is more than the repl backlog setting,
     * we consider the excess as replicas' memory. Otherwise, replication buffer
     * memory is the consumption of repl backlog. */
    if (listLength(server.slaves) &&
        (long long)server.repl_buffer_mem > server.repl_backlog_size)
    {
        mh->clients_slaves = server.repl_buffer_mem - server.repl_backlog_size;
        mh->repl_backlog = server.repl_backlog_size;
    } else {
        mh->clients_slaves = 0;
        mh->repl_backlog = server.repl_buffer_mem;
    }
    if (server.repl_backlog) {
        /* The approximate memory of rax tree for indexed blocks. */
        mh->repl_backlog +=
            server.repl_backlog->blocks_index->numnodes * sizeof(raxNode) +
            raxSize(server.repl_backlog->blocks_index) * sizeof(void*);
    }
    mem_total += mh->repl_backlog;
    mem_total += mh->clients_slaves;

    /* Computing the memory used by the clients would be O(N) if done
     * here online. We use our values computed incrementally by
     * updateClientMemoryUsage(). */
    mh->clients_normal = server.stat_clients_type_memory[CLIENT_TYPE_MASTER]+
                         server.stat_clients_type_memory[CLIENT_TYPE_PUBSUB]+
                         server.stat_clients_type_memory[CLIENT_TYPE_NORMAL];
    mem_total += mh->clients_normal;

    mh->cluster_links = server.stat_cluster_links_memory;
    mem_total += mh->cluster_links;

    mem = 0;
    if (server.aof_state != AOF_OFF) {
        mem += sdsZmallocSize(server.aof_buf);
    }
    mh->aof_buffer = mem;
    mem_total+=mem;

    mem = evalScriptsMemory();
    mh->lua_caches = mem;
    mem_total+=mem;
    mh->functions_caches = functionsMemoryOverhead();
    mem_total+=mh->functions_caches;

    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        if (!kvstoreNumAllocatedDicts(db->keys)) continue;

        unsigned long long keyscount = kvstoreSize(db->keys);

        mh->total_keys += keyscount;
        mh->db = zrealloc(mh->db,sizeof(mh->db[0])*(mh->num_dbs+1));
        mh->db[mh->num_dbs].dbid = j;

        mem = kvstoreMemUsage(db->keys) +
              keyscount * sizeof(robj);
        mh->db[mh->num_dbs].overhead_ht_main = mem;
        mem_total+=mem;

        mem = kvstoreMemUsage(db->expires);
        mh->db[mh->num_dbs].overhead_ht_expires = mem;
        mem_total+=mem;

        mh->num_dbs++;

        mh->overhead_db_hashtable_lut += kvstoreOverheadHashtableLut(db->keys);
        mh->overhead_db_hashtable_lut += kvstoreOverheadHashtableLut(db->expires);
        mh->overhead_db_hashtable_rehashing += kvstoreOverheadHashtableRehashing(db->keys);
        mh->overhead_db_hashtable_rehashing += kvstoreOverheadHashtableRehashing(db->expires);
        mh->db_dict_rehashing_count += kvstoreDictRehashingCount(db->keys);
        mh->db_dict_rehashing_count += kvstoreDictRehashingCount(db->expires);
    }

    mh->overhead_total = mem_total;
    mh->dataset = zmalloc_used - mem_total;
    mh->peak_perc = (float)zmalloc_used*100/mh->peak_allocated;

    /* Metrics computed after subtracting the startup memory from
     * the total memory. */
    size_t net_usage = 1;
    if (zmalloc_used > mh->startup_allocated)
        net_usage = zmalloc_used - mh->startup_allocated;
    mh->dataset_perc = (float)mh->dataset*100/net_usage;
    mh->bytes_per_key = mh->total_keys ? (mh->dataset / mh->total_keys) : 0;

    return mh;
}

/* "MEMORY allocator-stats" 的辅助函数，用作 jemalloc 统计输出的回调。 */
void inputCatSds(void *result, const char *str) {
    /* result 实际上是 (sds *)，这里重新转换类型 */
    sds *info = (sds *)result;
    *info = sdscat(*info, str);
}

/* 实现 MEMORY DOCTOR 命令。对 Redis 内存状况进行人类可读的分析。
 * 检测各种内存问题并生成诊断报告。 */
sds getMemoryDoctorReport(void) {
    int empty = 0;          /* 实例为空或几乎为空 */
    int big_peak = 0;       /* 内存峰值远大于当前使用量 */
    int high_frag = 0;      /* 高碎片率 */
    int high_alloc_frag = 0;/* 分配器碎片率高 */
    int high_proc_rss = 0;  /* 进程 RSS 开销高 */
    int high_alloc_rss = 0; /* 分配器 RSS 开销高 */
    int big_slave_buf = 0;  /* 副本缓冲区过大 */
    int big_client_buf = 0; /* 客户端缓冲区过大 */
    int many_scripts = 0;   /* 脚本缓存中脚本过多 */
    int num_reports = 0;
    struct redisMemOverhead *mh = getMemoryOverheadData();

    if (mh->total_allocated < (1024*1024*5)) {
        empty = 1;
        num_reports++;
    } else {
        /* Peak is > 150% of current used memory? */
        if (((float)mh->peak_allocated / mh->total_allocated) > 1.5) {
            big_peak = 1;
            num_reports++;
        }

        /* Fragmentation is higher than 1.4 and 10MB ?*/
        if (mh->total_frag > 1.4 && mh->total_frag_bytes > 10<<20) {
            high_frag = 1;
            num_reports++;
        }

        /* External fragmentation is higher than 1.1 and 10MB? */
        if (mh->allocator_frag > 1.1 && mh->allocator_frag_bytes > 10<<20) {
            high_alloc_frag = 1;
            num_reports++;
        }

        /* Allocator rss is higher than 1.1 and 10MB ? */
        if (mh->allocator_rss > 1.1 && mh->allocator_rss_bytes > 10<<20) {
            high_alloc_rss = 1;
            num_reports++;
        }

        /* Non-Allocator rss is higher than 1.1 and 10MB ? */
        if (mh->rss_extra > 1.1 && mh->rss_extra_bytes > 10<<20) {
            high_proc_rss = 1;
            num_reports++;
        }

        /* Clients using more than 200k each average? */
        long numslaves = listLength(server.slaves);
        long numclients = listLength(server.clients)-numslaves;
        if (mh->clients_normal / numclients > (1024*200)) {
            big_client_buf = 1;
            num_reports++;
        }

        /* Slaves using more than 10 MB each? */
        if (numslaves > 0 && mh->clients_slaves > (1024*1024*10)) {
            big_slave_buf = 1;
            num_reports++;
        }

        /* Too many scripts are cached? */
        if (dictSize(evalScriptsDict()) > 1000) {
            many_scripts = 1;
            num_reports++;
        }
    }

    sds s;
    if (num_reports == 0) {
        s = sdsnew(
        "Hi Sam, I can't find any memory issue in your instance. "
        "I can only account for what occurs on this base.\n");
    } else if (empty == 1) {
        s = sdsnew(
        "Hi Sam, this instance is empty or is using very little memory, "
        "my issues detector can't be used in these conditions. "
        "Please, leave for your mission on Earth and fill it with some data. "
        "The new Sam and I will be back to our programming as soon as I "
        "finished rebooting.\n");
    } else {
        s = sdsnew("Sam, I detected a few issues in this Redis instance memory implants:\n\n");
        if (big_peak) {
            s = sdscat(s," * Peak memory: In the past this instance used more than 150% the memory that is currently using. The allocator is normally not able to release memory after a peak, so you can expect to see a big fragmentation ratio, however this is actually harmless and is only due to the memory peak, and if the Redis instance Resident Set Size (RSS) is currently bigger than expected, the memory will be used as soon as you fill the Redis instance with more data. If the memory peak was only occasional and you want to try to reclaim memory, please try the MEMORY PURGE command, otherwise the only other option is to shutdown and restart the instance.\n\n");
        }
        if (high_frag) {
            s = sdscatprintf(s," * High total RSS: This instance has a memory fragmentation and RSS overhead greater than 1.4 (this means that the Resident Set Size of the Redis process is much larger than the sum of the logical allocations Redis performed). This problem is usually due either to a large peak memory (check if there is a peak memory entry above in the report) or may result from a workload that causes the allocator to fragment memory a lot. If the problem is a large peak memory, then there is no issue. Otherwise, make sure you are using the Jemalloc allocator and not the default libc malloc. Note: The currently used allocator is \"%s\".\n\n", ZMALLOC_LIB);
        }
        if (high_alloc_frag) {
            s = sdscatprintf(s," * High allocator fragmentation: This instance has an allocator external fragmentation greater than 1.1. This problem is usually due either to a large peak memory (check if there is a peak memory entry above in the report) or may result from a workload that causes the allocator to fragment memory a lot. You can try enabling 'activedefrag' config option.\n\n");
        }
        if (high_alloc_rss) {
            s = sdscatprintf(s," * High allocator RSS overhead: This instance has an RSS memory overhead is greater than 1.1 (this means that the Resident Set Size of the allocator is much larger than the sum what the allocator actually holds). This problem is usually due to a large peak memory (check if there is a peak memory entry above in the report), you can try the MEMORY PURGE command to reclaim it.\n\n");
        }
        if (high_proc_rss) {
            s = sdscatprintf(s," * High process RSS overhead: This instance has non-allocator RSS memory overhead is greater than 1.1 (this means that the Resident Set Size of the Redis process is much larger than the RSS the allocator holds). This problem may be due to Lua scripts or Modules.\n\n");
        }
        if (big_slave_buf) {
            s = sdscat(s," * Big replica buffers: The replica output buffers in this instance are greater than 10MB for each replica (on average). This likely means that there is some replica instance that is struggling receiving data, either because it is too slow or because of networking issues. As a result, data piles on the master output buffers. Please try to identify what replica is not receiving data correctly and why. You can use the INFO output in order to check the replicas delays and the CLIENT LIST command to check the output buffers of each replica.\n\n");
        }
        if (big_client_buf) {
            s = sdscat(s," * Big client buffers: The clients output buffers in this instance are greater than 200K per client (on average). This may result from different causes, like Pub/Sub clients subscribed to channels bot not receiving data fast enough, so that data piles on the Redis instance output buffer, or clients sending commands with large replies or very large sequences of commands in the same pipeline. Please use the CLIENT LIST command in order to investigate the issue if it causes problems in your instance, or to understand better why certain clients are using a big amount of memory.\n\n");
        }
        if (many_scripts) {
            s = sdscat(s," * Many scripts: There seem to be many cached scripts in this instance (more than 1000). This may be because scripts are generated and `EVAL`ed, instead of being parameterized (with KEYS and ARGV), `SCRIPT LOAD`ed and `EVALSHA`ed. Unless `SCRIPT FLUSH` is called periodically, the scripts' caches may end up consuming most of your memory.\n\n");
        }
        s = sdscat(s,"I'm here to keep you safe, Sam. I want to help you.\n");
    }
    freeMemoryOverheadData(mh);
    return s;
}

/* 根据 server.maxmemory_policy 设置对象的 LRU/LFU 信息。
 * lfu_freq 参数仅在策略为 MAXMEMORY_FLAG_LFU 时相关。
 * lru_idle 和 lru_clock 参数仅在策略为 MAXMEMORY_FLAG_LRU 时相关。
 * 任何一个或两个参数都可能 < 0，在这种情况下不设置任何值。
 * 成功设置返回 1，未设置返回 0。 */
int objectSetLRUOrLFU(robj *val, long long lfu_freq, long long lru_idle,
                       long long lru_clock, int lru_multiplier) {
    if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        if (lfu_freq >= 0) {
            serverAssert(lfu_freq <= 255);
            /* LFU 编码：高 16 位为时间（分钟），低 8 位为访问频率 */
            val->lru = (LFUGetTimeInMinutes()<<8) | lfu_freq;
            return 1;
        }
    } else if (lru_idle >= 0) {
        /* 提供的 LRU 空闲时间单位为秒。根据此 Redis 实例编译时的
         * LRU 时钟分辨率进行缩放（通常为 1000 毫秒，
         * 所以下面的语句将展开为 lru_idle*1000/1000）。 */
        lru_idle = lru_idle*lru_multiplier/LRU_CLOCK_RESOLUTION;
        long lru_abs = lru_clock - lru_idle; /* 绝对访问时间 */
        /* 如果 LRU 字段下溢（因为 lru_clock 是循环时钟），
         * 需要将其重新变为正值。这由 estimateObjectIdleTime 中的
         * 解包代码处理。例如，想象 lru_clock 循环的那天
         * （大约每 6 个月发生一次），它变成一个低值（如 10），
         * 那么 lru_idle 为 1000 时应该接近 LRU_CLOCK_MAX。 */
        if (lru_abs < 0)
            lru_abs += LRU_CLOCK_MAX;
        val->lru = lru_abs;
        return 1;
    }
    return 0;
}

/* ======================= OBJECT 和 MEMORY 命令 =================== */

/* OBJECT 命令的辅助函数。查找键但不修改 LRU 或其他参数。
 * 使用 LOOKUP_NOTOUCH 标志避免更新访问时间。 */
robj *objectCommandLookup(client *c, robj *key) {
    return lookupKeyReadWithFlags(c->db,key,LOOKUP_NOTOUCH|LOOKUP_NONOTIFY);
}

/* 查找键，如果不存在则回复错误。 */
robj *objectCommandLookupOrReply(client *c, robj *key, robj *reply) {
    robj *o = objectCommandLookup(c,key);
    if (!o) addReplyOrErrorObject(c, reply);
    return o;
}

/* OBJECT 命令允许检查 Redis 对象的内部信息。
 * 用法：OBJECT <refcount|encoding|idletime|freq> <key>
 * - REFCOUNT：返回值的引用计数
 * - ENCODING：返回内部编码类型
 * - IDLETIME：返回键的空闲时间（秒）
 * - FREQ：返回键的访问频率指数 */
void objectCommand(client *c) {
    robj *o;

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"ENCODING <key>",
"    Return the kind of internal representation used in order to store the value",
"    associated with a <key>.",
"FREQ <key>",
"    Return the access frequency index of the <key>. The returned integer is",
"    proportional to the logarithm of the recent access frequency of the key.",
"IDLETIME <key>",
"    Return the idle time of the <key>, that is the approximated number of",
"    seconds elapsed since the last access to the key.",
"REFCOUNT <key>",
"    Return the number of references of the value associated with the specified",
"    <key>.",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"refcount") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.null[c->resp]))
                == NULL) return;
        addReplyLongLong(c,o->refcount);
    } else if (!strcasecmp(c->argv[1]->ptr,"encoding") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.null[c->resp]))
                == NULL) return;
        addReplyBulkCString(c,strEncoding(o->encoding));
    } else if (!strcasecmp(c->argv[1]->ptr,"idletime") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.null[c->resp]))
                == NULL) return;
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            addReplyError(c,"An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            return;
        }
        addReplyLongLong(c,estimateObjectIdleTime(o)/1000);
    } else if (!strcasecmp(c->argv[1]->ptr,"freq") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.null[c->resp]))
                == NULL) return;
        if (!(server.maxmemory_policy & MAXMEMORY_FLAG_LFU)) {
            addReplyError(c,"An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            return;
        }
        /* LFUDecrAndReturn should be called
         * in case of the key has not been accessed for a long time,
         * because we update the access time only
         * when the key is read or overwritten. */
        addReplyLongLong(c,LFUDecrAndReturn(o));
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* MEMORY 命令提供 Redis 内存自省能力的完整接口。
 * 用法：
 * - MEMORY DOCTOR：返回内存问题报告
 * - MALLOC-STATS：返回内存分配器的内部统计
 * - PURGE：尝试清除脏页以回收内存
 * - STATS：返回服务器内存使用信息
 * - USAGE <key> [SAMPLES <count>]：返回键及其值占用的内存字节数 */
void memoryCommand(client *c) {
    if (!strcasecmp(c->argv[1]->ptr,"help") && c->argc == 2) {
        const char *help[] = {
"DOCTOR",
"    Return memory problems reports.",
"MALLOC-STATS",
"    Return internal statistics report from the memory allocator.",
"PURGE",
"    Attempt to purge dirty pages for reclamation by the allocator.",
"STATS",
"    Return information about the memory usage of the server.",
"USAGE <key> [SAMPLES <count>]",
"    Return memory in bytes used by <key> and its value. Nested values are",
"    sampled up to <count> times (default: 5, 0 means sample all).",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"usage") && c->argc >= 3) {
        /* MEMORY USAGE <key> [SAMPLES <count>]
         * 计算指定键在内存中占用的字节数 */
        dictEntry *de;
        long long samples = OBJ_COMPUTE_SIZE_DEF_SAMPLES;
        for (int j = 3; j < c->argc; j++) {
            if (!strcasecmp(c->argv[j]->ptr,"samples") &&
                j+1 < c->argc)
            {
                if (getLongLongFromObjectOrReply(c,c->argv[j+1],&samples,NULL)
                     == C_ERR) return;
                if (samples < 0) {
                    addReplyErrorObject(c,shared.syntaxerr);
                    return;
                }
                if (samples == 0) samples = LLONG_MAX;
                j++; /* 跳过选项参数 */
            } else {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }
        if ((de = dbFind(c->db, c->argv[2]->ptr)) == NULL) {
            addReplyNull(c);
            return;
        }
        /* 计算值的内存占用 + 键名的内存占用 + dictEntry 的内存占用 */
        size_t usage = objectComputeSize(c->argv[2],dictGetVal(de),samples,c->db->id);
        usage += sdsZmallocSize(dictGetKey(de));
        usage += dictEntryMemUsage();
        addReplyLongLong(c,usage);
    } else if (!strcasecmp(c->argv[1]->ptr,"stats") && c->argc == 2) {
        /* MEMORY STATS：返回服务器内存使用的详细统计信息 */
        struct redisMemOverhead *mh = getMemoryOverheadData();

        addReplyMapLen(c,31+mh->num_dbs);

        addReplyBulkCString(c,"peak.allocated");
        addReplyLongLong(c,mh->peak_allocated);

        addReplyBulkCString(c,"total.allocated");
        addReplyLongLong(c,mh->total_allocated);

        addReplyBulkCString(c,"startup.allocated");
        addReplyLongLong(c,mh->startup_allocated);

        addReplyBulkCString(c,"replication.backlog");
        addReplyLongLong(c,mh->repl_backlog);

        addReplyBulkCString(c,"clients.slaves");
        addReplyLongLong(c,mh->clients_slaves);

        addReplyBulkCString(c,"clients.normal");
        addReplyLongLong(c,mh->clients_normal);

        addReplyBulkCString(c,"cluster.links");
        addReplyLongLong(c,mh->cluster_links);

        addReplyBulkCString(c,"aof.buffer");
        addReplyLongLong(c,mh->aof_buffer);

        addReplyBulkCString(c,"lua.caches");
        addReplyLongLong(c,mh->lua_caches);

        addReplyBulkCString(c,"functions.caches");
        addReplyLongLong(c,mh->functions_caches);

        for (size_t j = 0; j < mh->num_dbs; j++) {
            char dbname[32];
            snprintf(dbname,sizeof(dbname),"db.%zd",mh->db[j].dbid);
            addReplyBulkCString(c,dbname);
            addReplyMapLen(c,2);

            addReplyBulkCString(c,"overhead.hashtable.main");
            addReplyLongLong(c,mh->db[j].overhead_ht_main);

            addReplyBulkCString(c,"overhead.hashtable.expires");
            addReplyLongLong(c,mh->db[j].overhead_ht_expires);
        }

        addReplyBulkCString(c,"overhead.db.hashtable.lut");
        addReplyLongLong(c, mh->overhead_db_hashtable_lut);

        addReplyBulkCString(c,"overhead.db.hashtable.rehashing");
        addReplyLongLong(c, mh->overhead_db_hashtable_rehashing);

        addReplyBulkCString(c,"overhead.total");
        addReplyLongLong(c,mh->overhead_total);

        addReplyBulkCString(c,"db.dict.rehashing.count");
        addReplyLongLong(c, mh->db_dict_rehashing_count);

        addReplyBulkCString(c,"keys.count");
        addReplyLongLong(c,mh->total_keys);

        addReplyBulkCString(c,"keys.bytes-per-key");
        addReplyLongLong(c,mh->bytes_per_key);

        addReplyBulkCString(c,"dataset.bytes");
        addReplyLongLong(c,mh->dataset);

        addReplyBulkCString(c,"dataset.percentage");
        addReplyDouble(c,mh->dataset_perc);

        addReplyBulkCString(c,"peak.percentage");
        addReplyDouble(c,mh->peak_perc);

        addReplyBulkCString(c,"allocator.allocated");
        addReplyLongLong(c,server.cron_malloc_stats.allocator_allocated);

        addReplyBulkCString(c,"allocator.active");
        addReplyLongLong(c,server.cron_malloc_stats.allocator_active);

        addReplyBulkCString(c,"allocator.resident");
        addReplyLongLong(c,server.cron_malloc_stats.allocator_resident);

        addReplyBulkCString(c,"allocator.muzzy");
        addReplyLongLong(c,server.cron_malloc_stats.allocator_muzzy);

        addReplyBulkCString(c,"allocator-fragmentation.ratio");
        addReplyDouble(c,mh->allocator_frag);

        addReplyBulkCString(c,"allocator-fragmentation.bytes");
        addReplyLongLong(c,mh->allocator_frag_bytes);

        addReplyBulkCString(c,"allocator-rss.ratio");
        addReplyDouble(c,mh->allocator_rss);

        addReplyBulkCString(c,"allocator-rss.bytes");
        addReplyLongLong(c,mh->allocator_rss_bytes);

        addReplyBulkCString(c,"rss-overhead.ratio");
        addReplyDouble(c,mh->rss_extra);

        addReplyBulkCString(c,"rss-overhead.bytes");
        addReplyLongLong(c,mh->rss_extra_bytes);

        addReplyBulkCString(c,"fragmentation"); /* this is the total RSS overhead, including fragmentation */
        addReplyDouble(c,mh->total_frag); /* it is kept here for backwards compatibility */

        addReplyBulkCString(c,"fragmentation.bytes");
        addReplyLongLong(c,mh->total_frag_bytes);

        freeMemoryOverheadData(mh);
    } else if (!strcasecmp(c->argv[1]->ptr,"malloc-stats") && c->argc == 2) {
        /* MEMORY MALLOC-STATS：返回内存分配器的内部统计信息 */
#if defined(USE_JEMALLOC)
        sds info = sdsempty();
        je_malloc_stats_print(inputCatSds, &info, NULL);
        addReplyVerbatim(c,info,sdslen(info),"txt");
        sdsfree(info);
#else
        addReplyBulkCString(c,"Stats not supported for the current allocator");
#endif
    } else if (!strcasecmp(c->argv[1]->ptr,"doctor") && c->argc == 2) {
        /* MEMORY DOCTOR：返回内存诊断报告 */
        sds report = getMemoryDoctorReport();
        addReplyVerbatim(c,report,sdslen(report),"txt");
        sdsfree(report);
    } else if (!strcasecmp(c->argv[1]->ptr,"purge") && c->argc == 2) {
        /* MEMORY PURGE：尝试清除脏页以回收内存 */
        if (jemalloc_purge() == 0)
            addReply(c, shared.ok);
        else
            addReplyError(c, "Error purging dirty pages");
    } else {
        addReplySubcommandSyntaxError(c);
    }
}
