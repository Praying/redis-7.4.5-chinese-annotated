/* lazyfree.c - 异步释放（懒释放）实现
 *
 * 本文件实现了 Redis 的异步释放机制。当对象包含大量元素时，
 * 同步释放会阻塞主线程，导致延迟抖动。通过将释放操作提交给
 * 后台 I/O 线程（bio.c），可以在不阻塞客户端请求的情况下
 * 回收内存。
 *
 * 核心思路：
 * 1. lazyfreeGetFreeEffort() 评估释放代价
 * 2. 若代价超过 LAZYFREE_THRESHOLD，提交给后台线程异步释放
 * 3. 否则直接同步释放，避免线程调度开销 */

#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "functions.h"
#include "cluster.h"
#include "ebuckets.h"

/* 待异步释放的对象计数 */
static redisAtomic size_t lazyfree_objects = 0;
/* 已完成异步释放的对象计数 */
static redisAtomic size_t lazyfreed_objects = 0;

/* 从 lazyfree 后台线程释放对象。
 * 本质就是调用 decrRefCount()，并更新待释放/已释放对象计数。 */
void lazyfreeFreeObject(void *args[]) {
    robj *o = (robj *) args[0];
    decrRefCount(o);
    atomicDecr(lazyfree_objects,1);
    atomicIncr(lazyfreed_objects,1);
}

/* 从 lazyfree 后台线程释放整个数据库。
 * 这里的 kvstore 指针是主线程在逻辑删除数据库时
 * 替换出来的旧数据结构。 */
void lazyfreeFreeDatabase(void *args[]) {
    kvstore *da1 = args[0];
    kvstore *da2 = args[1];
    ebuckets oldHfe = args[2];
    ebDestroy(&oldHfe, &hashExpireBucketsType, NULL);
    size_t numkeys = kvstoreSize(da1);
    kvstoreRelease(da1);
    kvstoreRelease(da2);
    atomicDecr(lazyfree_objects,numkeys);
    atomicIncr(lazyfreed_objects,numkeys);

#if defined(USE_JEMALLOC)
    /* 仅清除当前线程的 tcache 缓存。
     * 忽略返回值，因为 tcache 被禁用时此调用会失败。 */
    je_mallctl("thread.tcache.flush", NULL, NULL, NULL, 0);

    jemalloc_purge();
#endif
}

/* 释放键追踪表（用于客户端追踪 / tracking 功能）。 */
void lazyFreeTrackingTable(void *args[]) {
    rax *rt = args[0];
    size_t len = rt->numele;
    freeTrackingRadixTree(rt);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* 释放错误统计的 rax 树。 */
void lazyFreeErrors(void *args[]) {
    rax *errors = args[0];
    size_t len = errors->numele;
    raxFreeWithCallback(errors, zfree);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* 释放 lua_scripts 字典及相关 LRU 列表和 Lua 解释器。 */
void lazyFreeLuaScripts(void *args[]) {
    dict *lua_scripts = args[0];
    list *lua_scripts_lru_list = args[1];
    lua_State *lua = args[2];
    long long len = dictSize(lua_scripts);
    freeLuaScriptsSync(lua_scripts, lua_scripts_lru_list, lua);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* 释放函数库上下文（functions ctx）及引擎字典。 */
void lazyFreeFunctionsCtx(void *args[]) {
    functionsLibCtx *functions_lib_ctx = args[0];
    dict *engs = args[1];
    size_t len = functionsLibCtxFunctionsLen(functions_lib_ctx);
    functionsLibCtxFree(functions_lib_ctx);
    len += dictSize(engs);
    dictRelease(engs);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* 释放复制积压缓冲区（replication backlog）中引用的内存。 */
void lazyFreeReplicationBacklogRefMem(void *args[]) {
    list *blocks = args[0];
    rax *index = args[1];
    long long len = listLength(blocks);
    len += raxSize(index);
    listRelease(blocks);
    raxFree(index);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* 返回当前待异步释放的对象数量。 */
size_t lazyfreeGetPendingObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfree_objects,aux);
    return aux;
}

/* 返回已完成异步释放的对象数量。 */
size_t lazyfreeGetFreedObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfreed_objects,aux);
    return aux;
}

/* 重置异步释放统计计数器。 */
void lazyfreeResetStats(void) {
    atomicSet(lazyfreed_objects,0);
}

/* 返回释放一个对象所需的代价（工作量）。
 * 返回值不一定等于对象实际的内存分配次数，
 * 而是一个与之成正比的估算值。
 *
 * 对于字符串类型，始终返回 1。
 *
 * 对于由哈希表或其他数据结构表示的聚合对象，
 * 返回对象所包含的元素数量。
 *
 * 由单次分配组成的对象始终报告为 1，
 * 即使它在逻辑上由多个元素构成。
 *
 * 对于列表类型，返回其底层 quicklist 的节点数。 */
size_t lazyfreeGetFreeEffort(robj *key, robj *obj, int dbid) {
    if (obj->type == OBJ_LIST && obj->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *ql = obj->ptr;
        return ql->len;
    } else if (obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT) {
        dict *ht = obj->ptr;
        return dictSize(ht);
    } else if (obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST){
        zset *zs = obj->ptr;
        return zs->zsl->length;
    } else if (obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT) {
        dict *ht = obj->ptr;
        return dictSize(ht);
    } else if (obj->type == OBJ_STREAM) {
        size_t effort = 0;
        stream *s = obj->ptr;

        /* 尽力估算，保持常量时间复杂度。
         * Stream 中每个 rax 宏节点对应一次内存分配。 */
        effort += s->rax->numnodes;

        /* 每个消费者组是一次分配，其 PEL（待确认条目列表）
         * 中的每个条目也是。使用第一个组的 PEL 大小
         * 作为所有其他组的估算值。 */
        if (s->cgroups && raxSize(s->cgroups)) {
            raxIterator ri;
            streamCG *cg;
            raxStart(&ri,s->cgroups);
            raxSeek(&ri,"^",NULL,0);
            /* 至少存在一个消费者组，因此以下断言应始终成立。 */
            serverAssert(raxNext(&ri));
            cg = ri.data;
            effort += raxSize(s->cgroups)*(1+raxSize(cg->pel));
            raxStop(&ri);
        }
        return effort;
    } else if (obj->type == OBJ_MODULE) {
        size_t effort = moduleGetFreeEffort(key, obj, dbid);
        /* 如果模块的 free_effort 返回 0，
         * 则默认使用异步释放（按最大代价处理）。 */
        return effort == 0 ? ULONG_MAX : effort;
    } else {
        return 1; /* 其他类型均为单次分配。 */
    }
}

/* 如果对象包含足够多的分配，则将其放入懒释放列表
 * 而非同步释放。懒释放列表由 bio.c 的后台线程回收。
 * 如果对象仅由少量分配组成，异步释放反而更慢
 * （线程调度开销大于释放本身的开销），
 * 因此低于此阈值时直接同步释放。 */
#define LAZYFREE_THRESHOLD 64

/* 释放一个对象。如果对象足够大，则异步释放。 */
void freeObjAsync(robj *key, robj *obj, int dbid) {
    size_t free_effort = lazyfreeGetFreeEffort(key,obj,dbid);
    /* 注意：如果对象是共享的（refcount > 1），则无法立即回收。
     * 这种情况很少见，但 Redis 核心代码中偶尔会调用
     * incrRefCount() 保护对象，然后再调用 dbDelete()。 */
    if (free_effort > LAZYFREE_THRESHOLD && obj->refcount == 1) {
        atomicIncr(lazyfree_objects,1);
        bioCreateLazyFreeJob(lazyfreeFreeObject,1,obj);
    } else {
        decrRefCount(obj);
    }
}

/* 异步清空一个 Redis 数据库。
 * 实际做法是创建一组新的空哈希表替换旧的，
 * 然后将旧的哈希表调度给后台线程懒释放。 */
void emptyDbAsync(redisDb *db) {
    int slot_count_bits = 0;
    int flags = KVSTORE_ALLOCATE_DICTS_ON_DEMAND;
    if (server.cluster_enabled) {
        slot_count_bits = CLUSTER_SLOT_MASK_BITS;
        flags |= KVSTORE_FREE_EMPTY_DICTS;
    }
    kvstore *oldkeys = db->keys, *oldexpires = db->expires;
    ebuckets oldHfe = db->hexpires;
    db->keys = kvstoreCreate(&dbDictType, slot_count_bits, flags);
    db->expires = kvstoreCreate(&dbExpiresDictType, slot_count_bits, flags);
    db->hexpires = ebCreate();
    atomicIncr(lazyfree_objects, kvstoreSize(oldkeys));
    bioCreateLazyFreeJob(lazyfreeFreeDatabase, 3, oldkeys, oldexpires, oldHfe);
}

/* 释放键追踪表。如果表足够大，则异步释放。 */
void freeTrackingRadixTreeAsync(rax *tracking) {
    /* 此 rax 只存储键不存储值，所以用 numnodes 作为判断依据。 */
    if (tracking->numnodes > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects,tracking->numele);
        bioCreateLazyFreeJob(lazyFreeTrackingTable,1,tracking);
    } else {
        freeTrackingRadixTree(tracking);
    }
}

/* 释放错误统计的 rax 树。如果树足够大，则异步释放。 */
void freeErrorsRadixTreeAsync(rax *errors) {
    /* 此 rax 只存储键不存储值，所以用 numnodes 作为判断依据。 */
    if (errors->numnodes > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects,errors->numele);
        bioCreateLazyFreeJob(lazyFreeErrors,1,errors);
    } else {
        raxFreeWithCallback(errors, zfree);
    }
}

/* 释放 lua_scripts 字典和 LRU 列表。
 * 如果字典足够大，则异步释放（包括关闭 Lua 解释器）。 */
void freeLuaScriptsAsync(dict *lua_scripts, list *lua_scripts_lru_list, lua_State *lua) {
    if (dictSize(lua_scripts) > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects,dictSize(lua_scripts));
        bioCreateLazyFreeJob(lazyFreeLuaScripts,3,lua_scripts,lua_scripts_lru_list,lua);
    } else {
        freeLuaScriptsSync(lua_scripts, lua_scripts_lru_list, lua);
    }
}

/* 释放函数库上下文。如果包含足够多的函数，则异步释放。 */
void freeFunctionsAsync(functionsLibCtx *functions_lib_ctx, dict *engs) {
    if (functionsLibCtxFunctionsLen(functions_lib_ctx) > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects,functionsLibCtxFunctionsLen(functions_lib_ctx)+dictSize(engs));
        bioCreateLazyFreeJob(lazyFreeFunctionsCtx,2,functions_lib_ctx,engs);
    } else {
        functionsLibCtxFree(functions_lib_ctx);
        dictRelease(engs);
    }
}

/* 释放复制积压缓冲区引用的缓冲区块和 rax 索引。 */
void freeReplicationBacklogRefMemAsync(list *blocks, rax *index) {
    if (listLength(blocks) > LAZYFREE_THRESHOLD ||
        raxSize(index) > LAZYFREE_THRESHOLD)
    {
        atomicIncr(lazyfree_objects,listLength(blocks)+raxSize(index));
        bioCreateLazyFreeJob(lazyFreeReplicationBacklogRefMem,2,blocks,index);
    } else {
        listRelease(blocks);
        raxFree(index);
    }
}
