/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "cluster.h"
#include "atomicvar.h"
#include "latency.h"
#include "script.h"
#include "functions.h"

#include <signal.h>
#include <ctype.h>
#include "bio.h"

/*-----------------------------------------------------------------------------
 * C-level DB API（数据库底层 C 语言 API）
 * 这些函数提供了 Redis 数据库的核心操作接口，包括 key 的查找、添加、
 * 删除、过期处理等基础功能，是上层命令实现的基石。
 *----------------------------------------------------------------------------*/

/* expireIfNeeded 函数的标志位 */
#define EXPIRE_FORCE_DELETE_EXPIRED 1  /* 强制删除已过期的 key（即使在 replica 上也执行删除） */
#define EXPIRE_AVOID_DELETE_EXPIRED 2  /* 仅检查过期状态，但不实际删除 key（避免传播删除操作） */

/* expireIfNeeded 函数的返回值 */
typedef enum {
    KEY_VALID = 0, /* key 仍然有效：可能是未过期的易失性 key、持久化 key、或不存在的 key */
    KEY_EXPIRED,   /* key 已逻辑过期但尚未被删除 */
    KEY_DELETED    /* key 已过期且已被删除 */
} keyStatus;

keyStatus expireIfNeeded(redisDb *db, robj *key, int flags);
int keyIsExpired(redisDb *db, robj *key);
static void dbSetValue(redisDb *db, robj *key, robj *val, int overwrite, dictEntry *de);

/* 当对象被访问时更新 LFU（最不经常使用）计数器。
 * 首先，如果已到达衰减时间，则递减计数器。
 * 然后以对数方式递增计数器，并更新访问时间。
 * LFU 策略通过 log(freq) + 时间衰减 来估计访问热度，用于内存淘汰决策。 */
void updateLFU(robj *val) {
    unsigned long counter = LFUDecrAndReturn(val);
    counter = LFULogIncr(counter);
    val->lru = (LFUGetTimeInMinutes()<<8) | counter;
}

/* 查找一个 key 用于读或写操作，如果在指定数据库中未找到则返回 NULL。
 * 此函数实现了 lookupKeyRead()、lookupKeyWrite() 及其 WithFlags() 变体的核心逻辑。
 *
 * 调用此函数的副作用：
 *
 * 1. 如果 key 已到达 TTL（生存时间），则使其过期（删除）。
 * 2. 更新 key 的最后访问时间。
 * 3. 更新全局 key 命中/未命中统计（在 INFO 命令中报告）。
 * 4. 如果启用了 keyspace notification（键空间通知），则在 key 未命中时触发 "keymiss" 通知。
 *
 * 标志位（flags）可以改变此命令的行为：
 *
 *  LOOKUP_NONE（或 0）：无特殊标志。
 *  LOOKUP_NOTOUCH：不修改 key 的最后访问时间。
 *  LOOKUP_NONOTIFY：在 key 未命中时不触发 keyspace 事件。
 *  LOOKUP_NOSTATS：不递增 key 命中/未命中计数器。
 *  LOOKUP_WRITE：为写操作准备 key（即使在 replica 上也删除已过期的 key，
 *                使用独立的 keyspace 统计和事件（TODO））。
 *  LOOKUP_NOEXPIRE：执行过期检查，但避免删除 key，这样就不需要传播删除操作。
 *
 * 注意：如果 key 已逻辑过期但仍然存在，且当前是 replica 且未设置 LOOKUP_WRITE 标志，
 * 此函数也会返回 NULL。即使 key 的过期由 master 驱动，我们也可以在 replica 上
 * 正确报告 key 已过期，即使 master 通过复制链接中的 DEL 操作延迟过期处理。 */
robj *lookupKey(redisDb *db, robj *key, int flags) {
    dictEntry *de = dbFind(db, key->ptr);
    robj *val = NULL;
    if (de) {
        val = dictGetVal(de);
        /* 在 replica 上强制删除已过期的 key 会导致 replica 与 master 不一致。
         * 我们在只读 replica 上禁止此操作，但在可写 replica 上必须允许，
         * 以确保写命令的行为一致性。
         *
         * 即使在只读命令期间也可能设置 WRITE 标志，因为该命令可能触发
         * 模块执行额外的写操作。 */
        int is_ro_replica = server.masterhost && server.repl_slave_ro;
        int expire_flags = 0;
        if (flags & LOOKUP_WRITE && !is_ro_replica)
            expire_flags |= EXPIRE_FORCE_DELETE_EXPIRED;
        if (flags & LOOKUP_NOEXPIRE)
            expire_flags |= EXPIRE_AVOID_DELETE_EXPIRED;
        if (expireIfNeeded(db, key, expire_flags) != KEY_VALID) {
            /* The key is no longer valid. */
            val = NULL;
        }
    }

    if (val) {
        /* 更新用于老化算法的访问时间。
         * 如果存在子进程正在执行保存操作，则不更新，因为这会触发大量的写时复制（copy-on-write）。 */
        if (server.current_client && server.current_client->flags & CLIENT_NO_TOUCH &&
            server.current_client->cmd->proc != touchCommand)
            flags |= LOOKUP_NOTOUCH;
        if (!hasActiveChildProcess() && !(flags & LOOKUP_NOTOUCH)){
            if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
                /* 使用 LFU（最不经常使用）淘汰策略 */
                updateLFU(val);
            } else {
                /* 使用 LRU（最近最少使用）淘汰策略 */
                val->lru = LRU_CLOCK();
            }
        }

        if (!(flags & (LOOKUP_NOSTATS | LOOKUP_WRITE)))
            server.stat_keyspace_hits++;
        /* TODO: 为 WRITE 操作使用独立的命中统计 */
    } else {
        /* key 未命中，触发 keyspace 通知 */
        if (!(flags & (LOOKUP_NONOTIFY | LOOKUP_WRITE)))
            notifyKeyspaceEvent(NOTIFY_KEY_MISS, "keymiss", key, db->id);
        if (!(flags & (LOOKUP_NOSTATS | LOOKUP_WRITE)))
            server.stat_keyspace_misses++;
        /* TODO: 为 WRITE 操作使用独立的未命中统计和通知事件 */
    }

    return val;
}

/* 查找一个 key 用于读操作，如果在指定数据库中未找到则返回 NULL。
 *
 * 此 API 不应在获取与 key 关联的对象后对其进行写操作时使用，
 * 仅用于只读操作。
 *
 * 此函数等价于 lookupKey()。使用此函数而不是直接调用 lookupKey()
 * 的目的是明确表示该操作的目的是读取 key。 */
robj *lookupKeyReadWithFlags(redisDb *db, robj *key, int flags) {
    serverAssert(!(flags & LOOKUP_WRITE));
    return lookupKey(db, key, flags);
}

/* 类似于 lookupKeyReadWithFlags()，但不使用任何标志，这是最常见的情况。 */
robj *lookupKeyRead(redisDb *db, robj *key) {
    return lookupKeyReadWithFlags(db,key,LOOKUP_NONE);
}

/* 查找一个 key 用于写操作，作为副作用，如果需要，会在 key 的 TTL 到期时使其过期。
 * 等价于带 LOOKUP_WRITE 标志的 lookupKey()。
 *
 * 如果 key 存在，返回关联的 value 对象；如果 key 在指定数据库中不存在，返回 NULL。 */
robj *lookupKeyWriteWithFlags(redisDb *db, robj *key, int flags) {
    return lookupKey(db, key, flags | LOOKUP_WRITE);
}

robj *lookupKeyWrite(redisDb *db, robj *key) {
    return lookupKeyWriteWithFlags(db, key, LOOKUP_NONE);
}

/* 查找一个 key 用于读操作，如果 key 不存在则向客户端返回指定的 reply。
 * 常用于需要在 key 不存在时返回错误或特定值的命令实现中。 */
robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply) {
    robj *o = lookupKeyRead(c->db, key);
    if (!o) addReplyOrErrorObject(c, reply);
    return o;
}

/* 查找一个 key 用于写操作，如果 key 不存在则向客户端返回指定的 reply。 */
robj *lookupKeyWriteOrReply(client *c, robj *key, robj *reply) {
    robj *o = lookupKeyWrite(c->db, key);
    if (!o) addReplyOrErrorObject(c, reply);
    return o;
}

/* 将 key 添加到数据库。调用者负责递增 value 的引用计数（如果需要的话）。
 *
 * 如果 update_if_existing 参数为 false，当 key 已存在时程序会中止；
 * 如果为 true，则可以回退到 dbOverwrite（覆盖写入）。 */
static dictEntry *dbAddInternal(redisDb *db, robj *key, robj *val, int update_if_existing) {
    dictEntry *existing;
    int slot = getKeySlot(key->ptr);
    dictEntry *de = kvstoreDictAddRaw(db->keys, slot, key->ptr, &existing);
    if (update_if_existing && existing) {
        /* key 已存在且允许更新，覆盖其值 */
        dbSetValue(db, key, val, 1, existing);
        return existing;
    }
    serverAssertWithInfo(NULL, key, de != NULL);
    /* 复制 key 的 SDS 字符串（kvstore 拥有该副本的所有权） */
    kvstoreDictSetKey(db->keys, slot, de, sdsdup(key->ptr));
    /* 初始化对象的 LRU/LFU 时钟信息 */
    initObjectLRUOrLFU(val);
    kvstoreDictSetVal(db->keys, slot, de, val);
    /* 通知有新的 key 可用（用于解除阻塞的客户端） */
    signalKeyAsReady(db, key, val->type);
    /* 触发 keyspace 通知 */
    notifyKeyspaceEvent(NOTIFY_NEW,"new",key,db->id);
    return de;
}

/* 将 key 添加到数据库的公共接口。如果 key 已存在则中止程序。 */
dictEntry *dbAdd(redisDb *db, robj *key, robj *val) {
    return dbAddInternal(db, key, val, 0);
}

/* 返回 key 的哈希槽（hash slot），集群模式下返回实际的槽位号，非集群模式下返回 0。
 * 与 getKeySlot 的区别是：此函数不使用 current_client 中缓存的槽位，
 * 始终计算 CRC 哈希。适用于需要为用户未请求的 key 计算槽位的场景（如淘汰 eviction）。 */
int calculateKeySlot(sds key) {
    return server.cluster_enabled ? keyHashSlot(key, (int) sdslen(key)) : 0;
}

/* 返回 key 所属的哈希槽对应的字典索引。集群模式下返回实际槽位，否则返回 0。
 * 这是一个性能优化：使用当前命令中预设的 slot id，避免重复计算 key 的哈希值。
 * 此优化仅在 current_client 的 `CLIENT_EXECUTING_COMMAND` 标志被设置时生效，
 * 该标志只在 `call` 方法执行命令期间被设置。其他请求 key 槽位的流程会回退到 calculateKeySlot。 */
int getKeySlot(sds key) {
    if (server.current_client && server.current_client->slot >= 0 && server.current_client->flags & CLIENT_EXECUTING_COMMAND) {
        debugServerAssertWithInfo(server.current_client, NULL, calculateKeySlot(key)==server.current_client->slot);
        return server.current_client->slot;
    }
    return calculateKeySlot(key);
}

/* 这是 dbAdd() 的特殊版本，仅用于从 RDB 文件加载 key 时：
 * key 以 SDS 字符串形式传递，函数会保留该字符串（调用者无需释放）。
 *
 * 此外，如果 key 已存在，函数不会中止程序（给调用者更多控制权），
 * 也不会触发 key ready 信号（在此上下文中没有意义）。
 *
 * 如果 key 成功添加到数据库，返回 1（函数获得 SDS 字符串的所有权）；
 * 否则返回 0，由调用者负责释放 SDS 字符串。 */
int dbAddRDBLoad(redisDb *db, sds key, robj *val) {
    int slot = getKeySlot(key);
    dictEntry *de = kvstoreDictAddRaw(db->keys, slot, key, NULL);
    if (de == NULL) return 0;
    initObjectLRUOrLFU(val);
    kvstoreDictSetVal(db->keys, slot, de, val);
    return 1;
}

/* 用新值覆盖已存在的 key。新值的引用计数递增由调用者负责。
 * 此函数不会修改已存在的 key 的过期时间。
 *
 * 'overwrite' 标志指示这是否作为 key 的完整替换的一部分（可理解为删除+替换，
 * 此时需要发送删除信号），还是仅更新现有 key 的值（当 overwrite 为 false 时）。
 *
 * dictEntry 输入是可选的，如果已有的话可以传入以避免重复查找。
 *
 * 如果 key 不存在，程序会中止。 */
static void dbSetValue(redisDb *db, robj *key, robj *val, int overwrite, dictEntry *de) {
    int slot = getKeySlot(key->ptr);
    if (!de) de = kvstoreDictFind(db->keys, slot, key->ptr);
    serverAssertWithInfo(NULL,key,de != NULL);
    robj *old = dictGetVal(de);

    /* 保留旧对象的 LRU 时钟信息，避免访问时间被重置 */
    val->lru = old->lru;

    if (overwrite) {
        /* RM_StringDMA 可能调用 dbUnshareStringValue 从而释放 val，
         * 因此需要递增 old 的引用计数以保留它 */
        incrRefCount(old);
        /* 虽然 key 并未真正从数据库中删除，我们将覆盖视为 unlink+add 两步操作，
         * 因此仍需要调用模块的 unlink 回调。 */
        moduleNotifyKeyUnlink(key,old,db->id,DB_FLAG_KEY_OVERWRITE);
        /* 尝试解除阻塞的模块客户端或使用阻塞 XREADGROUP 的客户端 */
        signalDeletedKeyAsReady(db,key,old->type);
        decrRefCount(old);
        /* 由于 RM_StringDMA 可能修改 old，需要重新获取 */
        old = dictGetVal(de);
    }
    kvstoreDictSetVal(db->keys, slot, de, val);

    /* 如果是带字段过期时间（HFE）的 hash 对象，需要从全局 HFE 数据结构中移除 */
    if (old->type == OBJ_HASH)
        hashTypeRemoveFromExpires(&db->hexpires, old);

    /* 根据 lazyfree 配置决定同步或异步释放旧对象 */
    if (server.lazyfree_lazy_server_del) {
        freeObjAsync(key,old,db->id);
    } else {
        decrRefCount(old);
    }
}

/* 用新值替换已存在的 key，仅替换值而不触发任何事件（如 keyspace 通知）。 */
void dbReplaceValue(redisDb *db, robj *key, robj *val) {
    dbSetValue(db, key, val, 0, NULL);
}

/* 高层 Set 操作。此函数可用于将 key 设置为新对象，无论 key 是否已存在。
 *
 * 此函数执行以下操作：
 * 1) 递增 value 对象的引用计数。
 * 2) 通知正在 WATCH（监视）目标 key 的客户端。
 * 3) 重置 key 的过期时间（使 key 变为持久化的），
 *    除非在 flags 中启用了 'SETKEY_KEEPTTL'。
 * 4) key 的查找可以在此接口外部进行，通过 'SETKEY_ALREADY_EXIST' 或
 *    'SETKEY_DOESNT_EXIST' 标志传入查找结果。
 *
 * 数据库中所有新 key 都应通过此接口创建。
 * 如果操作在没有明确客户端的上下文中执行，client 'c' 参数可以设置为 NULL。 */
void setKey(client *c, redisDb *db, robj *key, robj *val, int flags) {
    int keyfound = 0;

    if (flags & SETKEY_ALREADY_EXIST)
        keyfound = 1;
    else if (flags & SETKEY_ADD_OR_UPDATE)
        keyfound = -1;
    else if (!(flags & SETKEY_DOESNT_EXIST))
        keyfound = (lookupKeyWrite(db,key) != NULL);

    if (!keyfound) {
        dbAdd(db,key,val);
    } else if (keyfound<0) {
        dbAddInternal(db,key,val,1);
    } else {
        dbSetValue(db,key,val,1,NULL);
    }
    incrRefCount(val);
    if (!(flags & SETKEY_KEEPTTL)) removeExpire(db,key);
    if (!(flags & SETKEY_NO_SIGNAL)) signalModifiedKey(c,db,key);
}

/* 返回一个随机 key（Redis 对象形式）。如果数据库中没有 key，返回 NULL。
 *
 * 此函数确保返回的 key 不是已过期的。 */
robj *dbRandomKey(redisDb *db) {
    dictEntry *de;
    int maxtries = 100;
    int allvolatile = kvstoreSize(db->keys) == kvstoreSize(db->expires);

    while(1) {
        sds key;
        robj *keyobj;
        int randomSlot = kvstoreGetFairRandomDictIndex(db->keys);
        de = kvstoreDictGetFairRandomKey(db->keys, randomSlot);
        if (de == NULL) return NULL;

        key = dictGetKey(de);
        keyobj = createStringObject(key,sdslen(key));
        if (dbFindExpires(db, key)) {
            if (allvolatile && (server.masterhost || isPausedActions(PAUSE_ACTION_EXPIRE)) && --maxtries == 0) {
                /* 如果数据库中所有 key 都设置了过期时间，在 slave 上可能出现
                 * 所有 key 都已逻辑过期的情况。此时函数无法停止，因为
                 * expireIfNeeded() 返回 false（不删除），dictGetFairRandomKey()
                 * 也不会返回 NULL（仍有 key 可返回）。
                 * 为防止无限循环，我们设置最大重试次数，如果满足无限循环的条件，
                 * 最终返回一个可能已过期的 key 名称。 */
                return keyobj;
            }
            if (expireIfNeeded(db,keyobj,0) != KEY_VALID) {
                decrRefCount(keyobj);
                continue; /* search for another key. This expired. */
            }
        }
        return keyobj;
    }
}

/* 同步和异步删除的辅助函数。
 * 根据 async 参数决定是否异步释放 value 对象的内存。
 * 使用两阶段 unlink（two-phase unlink）以避免在模块回调期间出现竞态条件。 */
int dbGenericDelete(redisDb *db, robj *key, int async, int flags) {
    dictEntry **plink;
    int table;
    int slot = getKeySlot(key->ptr);
    dictEntry *de = kvstoreDictTwoPhaseUnlinkFind(db->keys, slot, key->ptr, &plink, &table);
    if (de) {
        robj *val = dictGetVal(de);

        /* 如果是带字段过期时间（HFE）的 hash 对象，先从全局 HFE 数据结构中移除 */
        if (val->type == OBJ_HASH)
            hashTypeRemoveFromExpires(&db->hexpires, val);

        /* RM_StringDMA 可能调用 dbUnshareStringValue 从而释放 val，
         * 因此需要递增引用计数以保留 val */
        incrRefCount(val);
        /* 通知模块该 key 已从数据库中取消链接 */
        moduleNotifyKeyUnlink(key,val,db->id,flags);
        /* 尝试解除阻塞的模块客户端或使用阻塞 XREADGROUP 的客户端 */
        signalDeletedKeyAsReady(db,key,val->type);
        /* 必须在 freeObjAsync 之前调用 decr，否则引用计数可能大于 1，
         * 导致 freeObjAsync 无法实际释放内存 */
        decrRefCount(val);
        if (async) {
            /* 异步释放 value 对象的内存 */
            freeObjAsync(key, dictGetVal(de), db->id);
            kvstoreDictSetVal(db->keys, slot, de, NULL);
        }
        /* 从 expires 字典中删除条目不会释放 key 的 SDS 字符串，
         * 因为它与主字典共享同一份 SDS。 */
        kvstoreDictDelete(db->expires, slot, key->ptr);

        kvstoreDictTwoPhaseUnlinkFree(db->keys, slot, de, plink, table);
        return 1;
    } else {
        return 0;
    }
}

/* 从数据库中同步删除 key、value 及关联的过期条目（如果有的话）。 */
int dbSyncDelete(redisDb *db, robj *key) {
    return dbGenericDelete(db, key, 0, DB_FLAG_KEY_DELETED);
}

/* 从数据库中删除 key、value 及关联的过期条目。
 * 如果 value 包含大量内存分配，可能会异步释放。 */
int dbAsyncDelete(redisDb *db, robj *key) {
    return dbGenericDelete(db, key, 1, DB_FLAG_KEY_DELETED);
}

/* 删除 key 的包装函数，行为取决于 Redis 的 lazy free 配置。
 * 根据 server.lazyfree_lazy_server_del 配置决定同步或异步删除。 */
int dbDelete(redisDb *db, robj *key) {
    return dbGenericDelete(db, key, server.lazyfree_lazy_server_del, DB_FLAG_KEY_DELETED);
}

/* 准备对存储在 'key' 处的字符串对象进行破坏性修改，
 * 用于实现 SETBIT 或 APPEND 等命令。
 *
 * 对象通常已准备好被修改，除非满足以下两个条件之一：
 *
 * 1) 对象 'o' 是共享的（引用计数 > 1），我们不想影响其他使用者。
 * 2) 对象的编码不是 "RAW"（原始字符串编码）。
 *
 * 如果函数发现对象满足上述条件之一（或两者），则会在指定数据库的 'key' 处
 * 存储一个非共享/非编码的字符串对象副本。否则直接返回对象 'o' 本身。
 *
 * 使用方式：
 *
 * 对象 'o' 是调用者通过在 'db' 中查找 'key' 已经获得的，使用模式如下：
 *
 * o = lookupKeyWrite(db,key);
 * if (checkType(c,o,OBJ_STRING)) return;
 * o = dbUnshareStringValue(db,key,o);
 *
 * 此时调用者可以安全地修改对象，例如使用 sdscat() 追加数据，或其他操作。
 */
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o) {
    serverAssert(o->type == OBJ_STRING);
    if (o->refcount != 1 || o->encoding != OBJ_ENCODING_RAW) {
        robj *decoded = getDecodedObject(o);
        o = createRawStringObject(decoded->ptr, sdslen(decoded->ptr));
        decrRefCount(decoded);
        dbReplaceValue(db,key,o);
    }
    return o;
}

/* 从数据库结构中移除所有 key。dbarray 参数不一定是服务器的主数据库
 * （可以是临时数据库 tempDb）。
 *
 * dbnum 参数：
 *   -1 表示清空所有数据库，
 *   其他值表示只清空指定索引的单个数据库。
 *
 * 函数返回从数据库中移除的 key 数量。 */
long long emptyDbStructure(redisDb *dbarray, int dbnum, int async,
                           void(callback)(dict*))
{
    long long removed = 0;
    int startdb, enddb;

    if (dbnum == -1) {
        startdb = 0;
        enddb = server.dbnum-1;
    } else {
        startdb = enddb = dbnum;
    }

    for (int j = startdb; j <= enddb; j++) {
        removed += kvstoreSize(dbarray[j].keys);
        if (async) {
            emptyDbAsync(&dbarray[j]);
        } else {
            /* 在删除 hash 之前先销毁全局 HFE（hash field expiration）数据结构，
             * 因为 ebuckets 数据结构嵌入在存储的对象中。 */
            ebDestroy(&dbarray[j].hexpires, &hashExpireBucketsType, NULL);
            kvstoreEmpty(dbarray[j].keys, callback);
            kvstoreEmpty(dbarray[j].expires, callback);
        }
        /* 由于数据库的所有 key 都已移除，重置平均 TTL */
        dbarray[j].avg_ttl = 0;
        dbarray[j].expires_cursor = 0;
    }

    return removed;
}

/* 从 Redis 服务器的所有数据库中移除所有数据（key 和函数库）。
 * 如果提供了 callback，函数会不时调用它以通知处理进度。
 *
 * dbnum 参数：
 *   -1 表示清空所有数据库，
 *   其他值表示只清空指定编号的单个数据库。
 *
 * flags 参数：
 *   EMPTYDB_NO_FLAGS：无特殊标志（同步清空）。
 *   EMPTYDB_ASYNC：在不同线程中释放内存，函数尽快返回。
 *   EMPTYDB_NOFUNCTIONS：指定不删除函数库。
 *
 * 成功时返回从数据库中移除的 key 数量。
 * 如果 DB 编号超出范围，返回 -1 并将 errno 设置为 EINVAL。 */
long long emptyData(int dbnum, int flags, void(callback)(dict*)) {
    int async = (flags & EMPTYDB_ASYNC);
    int with_functions = !(flags & EMPTYDB_NOFUNCTIONS);
    RedisModuleFlushInfoV1 fi = {REDISMODULE_FLUSHINFO_VERSION,!async,dbnum};
    long long removed = 0;

    if (dbnum < -1 || dbnum >= server.dbnum) {
        errno = EINVAL;
        return -1;
    }

    /* 触发 flushdb 模块事件 */
    moduleFireServerEvent(REDISMODULE_EVENT_FLUSHDB,
                          REDISMODULE_SUBEVENT_FLUSHDB_START,
                          &fi);

    /* 确保被 WATCH 监视的 key 会受到 FLUSH* 命令的影响。
     * 注意：需要在 key 仍然存在时调用此函数。 */
    signalFlushedDb(dbnum, async);

    /* 清空 Redis 数据库结构 */
    removed = emptyDbStructure(server.db, dbnum, async, callback);

    if (dbnum == -1) flushSlaveKeysWithExpireList();

    if (with_functions) {
        serverAssert(dbnum == -1);
        functionsLibCtxClearCurrent(async);
    }

    /* 也触发结束事件。注意：如果 flush 是异步的，此事件几乎会在
     * 开始事件之后立即触发。 */
    moduleFireServerEvent(REDISMODULE_EVENT_FLUSHDB,
                          REDISMODULE_SUBEVENT_FLUSHDB_END,
                          &fi);

    return removed;
}

/* 在 replica 上初始化临时数据库，用于无盘复制（diskless replication）期间。
 * 临时数据库用于接收从 master 传播过来的数据，之后再与主数据库交换。 */
redisDb *initTempDb(void) {
    int slot_count_bits = 0;
    int flags = KVSTORE_ALLOCATE_DICTS_ON_DEMAND;
    if (server.cluster_enabled) {
        slot_count_bits = CLUSTER_SLOT_MASK_BITS;
        flags |= KVSTORE_FREE_EMPTY_DICTS;
    }
    redisDb *tempDb = zcalloc(sizeof(redisDb)*server.dbnum);
    for (int i=0; i<server.dbnum; i++) {
        tempDb[i].id = i;
        tempDb[i].keys = kvstoreCreate(&dbDictType, slot_count_bits, flags);
        tempDb[i].expires = kvstoreCreate(&dbExpiresDictType, slot_count_bits, flags);
        tempDb[i].hexpires = ebCreate();
    }

    return tempDb;
}

/* 丢弃临时数据库 tempDb。此操作可能较慢（类似 FLUSHALL），但始终是异步的。 */
void discardTempDb(redisDb *tempDb, void(callback)(dict*)) {
    int async = 1;

    /* 释放临时数据库 */
    emptyDbStructure(tempDb, -1, async, callback);
    for (int i=0; i<server.dbnum; i++) {
        /* 在删除 hash 之前先销毁全局 HFE 数据结构，
         * 因为 ebuckets 数据结构嵌入在存储的对象中。 */
        ebDestroy(&tempDb[i].hexpires, &hashExpireBucketsType, NULL);
        kvstoreRelease(tempDb[i].keys);
        kvstoreRelease(tempDb[i].expires);
    }

    zfree(tempDb);
}

/* 选择客户端要操作的数据库。id 范围为 [0, server.dbnum-1]。 */
int selectDb(client *c, int id) {
    if (id < 0 || id >= server.dbnum)
        return C_ERR;
    c->db = &server.db[id];
    return C_OK;
}

/* 返回服务器所有数据库中 key 的总数 */
long long dbTotalServerKeyCount(void) {
    long long total = 0;
    int j;
    for (j = 0; j < server.dbnum; j++) {
        total += kvstoreSize(server.db[j].keys);
    }
    return total;
}

/*-----------------------------------------------------------------------------
 * 键空间变更钩子（Hooks for key space changes）
 *
 * 每当数据库中的 key 被修改时，会调用 signalModifiedKey() 函数。
 * 每当数据库被清空时，会调用 signalFlushDb() 函数。
 *
 * 这些钩子用于维护 WATCH 机制和客户端缓存跟踪（client-side caching）的一致性。
 *----------------------------------------------------------------------------*/

/* 当 key 被修改时调用此函数，通知相关的 WATCH 监视器和客户端缓存跟踪。
 * 注意：如果 key 的修改不在客户端上下文中执行，'c' 参数可以为 NULL。 */
void signalModifiedKey(client *c, redisDb *db, robj *key) {
    touchWatchedKey(db,key);         /* 通知 WATCH 该 key 的客户端，使其事务失败 */
    trackingInvalidateKey(c,key,1);  /* 使客户端缓存跟踪中该 key 失效 */
}

/* 当数据库被清空时调用此函数，处理所有受影响的 WATCH 和客户端缓存跟踪。 */
void signalFlushedDb(int dbid, int async) {
    int startdb, enddb;
    if (dbid == -1) {
        startdb = 0;
        enddb = server.dbnum-1;
    } else {
        startdb = enddb = dbid;
    }

    for (int j = startdb; j <= enddb; j++) {
        /* 扫描并处理因数据库清空而需要解除阻塞的 XREADGROUP 客户端 */
        scanDatabaseForDeletedKeys(&server.db[j], NULL);
        /* 通知所有在此数据库上被 WATCH 的 key */
        touchAllWatchedKeysInDb(&server.db[j], NULL);
    }

    /* 使客户端缓存跟踪中所有 key 失效 */
    trackingInvalidateKeysOnFlush(async);

    /* 注意：此方法中的更改也可能在 swapMainDbWithTempDb 中发生，
     * 在那里我们执行类似的调用，但有细微差异，因为它不是简单的清空数据库。 */
}

/*-----------------------------------------------------------------------------
 * 与类型无关的键空间操作命令
 * 这些命令不依赖于 key 的具体数据类型，直接操作键空间。
 *----------------------------------------------------------------------------*/

/* 返回 FLUSHALL 和 FLUSHDB 命令应使用的 emptyData() 调用标志。
 *
 * sync：同步清空数据库。
 * async：异步清空数据库。
 * 无选项：根据 lazyfree-lazy-user-flush 配置值决定同步或异步。
 *
 * 成功时返回 C_OK 并将标志存储在 *flags 中，
 * 否则返回 C_ERR 并向客户端发送错误信息。 */
int getFlushCommandFlags(client *c, int *flags) {
    /* Parse the optional ASYNC option. */
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"sync")) {
        *flags = EMPTYDB_NO_FLAGS;
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"async")) {
        *flags = EMPTYDB_ASYNC;
    } else if (c->argc == 1) {
        *flags = server.lazyfree_lazy_user_flush ? EMPTYDB_ASYNC : EMPTYDB_NO_FLAGS;
    } else {
        addReplyErrorObject(c,shared.syntaxerr);
        return C_ERR;
    }
    return C_OK;
}

/* 清空服务器的所有数据集，并在配置了持久化时保存新的 RDB 快照。 */
void flushAllDataAndResetRDB(int flags) {
    server.dirty += emptyData(-1,flags,NULL);
    if (server.child_type == CHILD_TYPE_RDB) killRDBChild();
    if (server.saveparamslen > 0) {
        rdbSaveInfo rsi, *rsiptr;
        rsiptr = rdbPopulateSaveInfo(&rsi);
        rdbSave(SLAVE_REQ_NONE,server.rdb_filename,rsiptr,RDBFLAGS_NONE);
    }

#if defined(USE_JEMALLOC)
    /* jemalloc 5 在没有流量时不会将页面归还给操作系统。
     * 对于大型数据库，flushdb 本身就会阻塞较长时间，所以额外的开销影响不大，
     * 这样可以使 flush 和 purge（清除）操作同步完成。 */
    if (!(flags & EMPTYDB_ASYNC)) {
        /* 仅清除当前线程的缓存。
         * 忽略返回值，因为当 tcache 被禁用时此调用会失败。 */
        je_mallctl("thread.tcache.flush", NULL, NULL, NULL, 0);

        jemalloc_purge();  /* 强制 jemalloc 归还内存给操作系统 */
    }
#endif
}

/* 优化的 FLUSHALL/FLUSHDB SYNC 命令，在 lazyfree 线程中完成后调用的回调函数。 */
/* lazyfree 线程完成 FLUSHALL/FLUSHDB 同步优化后的回调函数。 */
void flushallSyncBgDone(uint64_t client_id) {

    client *c = lookupClientByID(client_id);

    /* 验证客户端是否仍然存在 */
    if (!(c && c->flags & CLIENT_BLOCKED)) return;

    /* 更新 current_client（被调用的函数可能依赖它） */
    client *old_client = server.current_client;
    server.current_client = c;

    /* 不更新 blocked_us，因为命令在后台由 lazy_free 线程处理 */
    updateStatsOnUnblock(c, 0 /*blocked_us*/, elapsedUs(c->bstate.lazyfreeStartTime), 0);

    /* lazyfree 后台作业始终成功 */
    addReply(c, shared.ok);

    /* 将客户端标记为已解除阻塞 */
    unblockClient(c, 1);

    /* FLUSH 命令已完成。执行 resetClient() 并更新复制偏移量。 */
    commandProcessed(c);

    /* flush 完成后，更新客户端的内存使用量 */
    updateClientMemUsageAndBucket(c);

    /* 恢复 current_client */
    server.current_client = old_client;
}

/* FLUSHALL 和 FLUSHDB 命令的公共实现。
 * isFlushAll: 1 表示 FLUSHALL（清空所有数据库），0 表示 FLUSHDB（清空当前数据库）。
 *
 * 优化策略：对于 SYNC 模式，如果可能，将其转换为阻塞式 ASYNC 执行，
 * 这样客户端会阻塞等待直到 lazyfree 线程完成所有清理工作。 */
void flushCommandCommon(client *c, int isFlushAll) {
    int blocking_async = 0; /* 是否将 FLUSHALL/FLUSHDB SYNC 转换为阻塞式 ASYNC 执行 */
    int flags;
    if (getFlushCommandFlags(c,&flags) == C_ERR) return;

    /* 对于 SYNC 模式，检查是否可以优化为后台阻塞式 ASYNC 执行 */
    if ((!(flags & EMPTYDB_ASYNC)) && (!(c->flags & CLIENT_AVOID_BLOCKING_ASYNC_FLUSH))) {
        /* 转换为 ASYNC 模式执行 */
        flags |= EMPTYDB_ASYNC;
        blocking_async = 1;
    }

    if (isFlushAll)
        flushAllDataAndResetRDB(flags | EMPTYDB_NOFUNCTIONS);
    else
        server.dirty += emptyData(c->db->id,flags | EMPTYDB_NOFUNCTIONS,NULL);

    /* 没有 forceCommandPropagation 的话，当数据库已经为空时，
     * FLUSHALL/FLUSHDB 不会被复制到 replica 也不会写入 AOF。 */
    forceCommandPropagation(c, PROPAGATE_REPL | PROPAGATE_AOF);

    /* 如果是阻塞式 ASYNC，阻塞客户端并将完成作业请求添加到 BIO lazyfree 工作线程队列。
     * 只有在队列中所有之前的 pending lazyfree 作业处理完成后，才会调用回调并回复 OK。 */
    if (blocking_async) {
        /* 记录后台作业从开始到完成的耗时，作为 flush 命令的 elapsed time */
        elapsedStart(&c->bstate.lazyfreeStartTime);

        c->bstate.timeout = 0;
        blockClient(c,BLOCKED_LAZYFREE);
        bioCreateCompRq(BIO_WORKER_LAZY_FREE, flushallSyncBgDone, c->id);
    } else {
        addReply(c, shared.ok);
    }
#if defined(USE_JEMALLOC)
    /* jemalloc 5 在没有流量时不会将页面归还给操作系统。
     * 对于大型数据库，flushdb 本身就会阻塞较长时间，所以额外的开销影响不大，
     * 这样可以使 flush 和 purge 操作同步完成。
     *
     * 注意：仅对 FLUSHDB 的同步流程执行 purge。FLUSHALL 的同步流程已在
     * flushAllDataAndResetRDB 中处理。异步流程将在稍后才执行 purge。 */
    if ((!isFlushAll) && (!(flags & EMPTYDB_ASYNC))) {
        /* 仅清除当前线程的缓存。
         * 忽略返回值，因为当 tcache 被禁用时此调用会失败。 */
        je_mallctl("thread.tcache.flush", NULL, NULL, NULL, 0);

        jemalloc_purge();
    }
#endif
}

/* FLUSHALL [SYNC|ASYNC]
 * 清空整个服务器的数据集（所有数据库）。 */
void flushallCommand(client *c) {
    flushCommandCommon(c, 1);
}

/* FLUSHDB [SYNC|ASYNC]
 * 清空当前 SELECT 的 Redis 数据库。 */
void flushdbCommand(client *c) {
    flushCommandCommon(c, 0);
}

/* DEL 和 UNLINK 命令的通用实现。
 * lazy: 1 表示异步删除（UNLINK），0 表示同步删除（DEL）。
 * DEL 是同步删除，UNLINK 是异步删除，后者对于大对象更高效。 */
void delGenericCommand(client *c, int lazy) {
    int numdel = 0, j;

    for (j = 1; j < c->argc; j++) {
        if (expireIfNeeded(c->db,c->argv[j],0) == KEY_DELETED)
            continue;
        int deleted  = lazy ? dbAsyncDelete(c->db,c->argv[j]) :
                              dbSyncDelete(c->db,c->argv[j]);
        if (deleted) {
            signalModifiedKey(c,c->db,c->argv[j]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,
                "del",c->argv[j],c->db->id);
            server.dirty++;
            numdel++;
        }
    }
    addReplyLongLong(c,numdel);
}

/* DEL key [key ...] 命令。根据 lazyfree_lazy_user_del 配置决定同步或异步删除。 */
void delCommand(client *c) {
    delGenericCommand(c,server.lazyfree_lazy_user_del);
}

/* UNLINK key [key ...] 命令。始终异步删除。 */
void unlinkCommand(client *c) {
    delGenericCommand(c,1);
}

/* EXISTS key1 key2 ... key_N 命令。
 * 返回值是存在的 key 数量。使用 LOOKUP_NOTOUCH 避免修改 key 的访问时间。 */
void existsCommand(client *c) {
    long long count = 0;
    int j;

    for (j = 1; j < c->argc; j++) {
        if (lookupKeyReadWithFlags(c->db,c->argv[j],LOOKUP_NOTOUCH)) count++;
    }
    addReplyLongLong(c,count);
}

/* SELECT dbid 命令。选择要操作的数据库编号。
 * 集群模式下只允许选择 DB 0。 */
void selectCommand(client *c) {
    int id;

    if (getIntFromObjectOrReply(c, c->argv[1], &id, NULL) != C_OK)
        return;

    if (server.cluster_enabled && id != 0) {
        addReplyError(c,"SELECT is not allowed in cluster mode");
        return;
    }
    if (selectDb(c,id) == C_ERR) {
        addReplyError(c,"DB index is out of range");
    } else {
        addReply(c,shared.ok);
    }
}

/* RANDOMKEY 命令。从当前数据库中随机返回一个 key（不删除）。 */
void randomkeyCommand(client *c) {
    robj *key;

    if ((key = dbRandomKey(c->db)) == NULL) {
        addReplyNull(c);
        return;
    }

    addReplyBulk(c,key);
    decrRefCount(key);
}

/* KEYS pattern 命令。返回匹配给定模式的所有 key。
 * 警告：在大型数据库上执行 KEYS 会阻塞服务器很长时间，生产环境应使用 SCAN 代替。 */
void keysCommand(client *c) {
    dictEntry *de;
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern), allkeys, pslot = -1;
    unsigned long numkeys = 0;
    void *replylen = addReplyDeferredLen(c);
    allkeys = (pattern[0] == '*' && plen == 1);
    /* 集群模式下，如果模式不是 "*"，尝试计算模式所属的槽位以优化遍历 */
    if (server.cluster_enabled && !allkeys) {
        pslot = patternHashSlot(pattern, plen);
    }
    kvstoreDictIterator *kvs_di = NULL;
    kvstoreIterator *kvs_it = NULL;
    if (pslot != -1) {
        if (!kvstoreDictSize(c->db->keys, pslot)) {
            /* 请求的槽位为空，直接返回空结果 */
            setDeferredArrayLen(c,replylen,0);
            return;
        }
        /* 只遍历特定槽位的字典（集群模式优化） */
        kvs_di = kvstoreGetDictSafeIterator(c->db->keys, pslot);
    } else {
        /* 遍历所有槽位的字典 */
        kvs_it = kvstoreIteratorInit(c->db->keys);
    }
    robj keyobj;
    while ((de = kvs_di ? kvstoreDictIteratorNext(kvs_di) : kvstoreIteratorNext(kvs_it)) != NULL) {
        sds key = dictGetKey(de);

        if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            initStaticStringObject(keyobj, key);
            if (!keyIsExpired(c->db, &keyobj)) {
                addReplyBulkCBuffer(c, key, sdslen(key));
                numkeys++;
            }
        }
        if (c->flags & CLIENT_CLOSE_ASAP)
            break;
    }
    if (kvs_di)
        kvstoreReleaseDictIterator(kvs_di);
    if (kvs_it)
        kvstoreIteratorRelease(kvs_it);
    setDeferredArrayLen(c,replylen,numkeys);
}

/* 字典扫描回调函数使用的数据结构。
 * 用于在 SCAN/SSCAN/HSCAN/ZSCAN 命令中收集扫描结果。 */
typedef struct {
    list *keys;              /* 从字典中收集的元素列表 */
    robj *o;                 /* 必须是 hash/set/zset 对象，NULL 表示扫描当前数据库 */
    long long type;          /* 扫描数据库时的特定类型过滤（LLONG_MAX 表示不过滤） */
    sds pattern;             /* 模式字符串，NULL 表示无模式匹配 */
    long sampled;            /* 累计采样的 key 数量（用于限制扫描量） */
    int no_values;           /* 设为 1 表示仅返回 key，不返回 value */
    size_t (*strlen)(char *s); /* 字符串长度函数：(o->type == OBJ_HASH) ? hfieldlen : sdslen */
} scanData;

/* 扫描命令中用于比较对象类型的辅助函数。
 * 对于模块类型，使用特殊的一元负号标识进行比较。 */
int objectTypeCompare(robj *o, long long target) {
    if (o->type != OBJ_MODULE) {
        if (o->type != target) 
            return 0;
        else 
            return 1;
    }
    /* module type compare */
    long long mt = (long long)REDISMODULE_TYPE_SIGN(((moduleValue *)o->ptr)->type->id);
    if (target != -mt)
        return 0;
    else 
        return 1;
}
/* 此回调函数被 scanGenericCommand 使用，用于将字典迭代器返回的元素收集到列表中。
 * 根据对象类型（数据库 key、SET、HASH、ZSET）以不同方式提取 key 和 value。 */
void scanCallback(void *privdata, const dictEntry *de) {
    scanData *data = (scanData *)privdata;
    list *keys = data->keys;
    robj *o = data->o;
    sds val = NULL;
    void *key = NULL;  /* if OBJ_HASH then key is of type `hfield`. Otherwise, `sds` */
    data->sampled++;

    /* o and typename can not have values at the same time. */
    serverAssert(!((data->type != LLONG_MAX) && o));

    /* Filter an element if it isn't the type we want. */
    /* TODO: uncomment in redis 8.0
    if (!o && data->type != LLONG_MAX) {
        robj *rval = dictGetVal(de);
        if (!objectTypeCompare(rval, data->type)) return;
    }*/

    /* Filter element if it does not match the pattern. */
    void *keyStr = dictGetKey(de);
    if (data->pattern) {
        if (!stringmatchlen(data->pattern, sdslen(data->pattern), keyStr, data->strlen(keyStr), 0)) {
            return;
        }
    }

    if (o == NULL) {
        key = keyStr;
    } else if (o->type == OBJ_SET) {
        key = keyStr;
    } else if (o->type == OBJ_HASH) {
        key = keyStr;
        val = dictGetVal(de);

        /* If field is expired, then ignore */
        if (hfieldIsExpired(key))
            return;

    } else if (o->type == OBJ_ZSET) {
        char buf[MAX_LONG_DOUBLE_CHARS];
        int len = ld2string(buf, sizeof(buf), *(double *)dictGetVal(de), LD_STR_AUTO);
        key = sdsdup(keyStr);
        val = sdsnewlen(buf, len);
    } else {
        serverPanic("Type not handled in SCAN callback.");
    }

    listAddNodeTail(keys, key);
    if (val && !data->no_values) listAddNodeTail(keys, val);
}

/* 尝试解析存储在对象 'o' 中的 SCAN 游标：
 * 如果游标有效，将其作为无符号整数存储到 *cursor 并返回 C_OK。
 * 否则返回 C_ERR 并向客户端发送错误信息。 */
int parseScanCursorOrReply(client *c, robj *o, unsigned long long *cursor) {
    if (!string2ull(o->ptr, cursor)) {
        addReplyError(c, "invalid cursor");
        return C_ERR;
    }
    return C_OK;
}

/* 对象类型名称数组，索引与 OBJ_STRING/OBJ_LIST/OBJ_SET/OBJ_ZSET/OBJ_HASH/OBJ_STREAM 对应 */
char *obj_type_name[OBJ_TYPE_MAX] = {
    "string", 
    "list", 
    "set", 
    "zset", 
    "hash", 
    NULL, /* module type is special */
    "stream"
};

/* 扫描命令中用于从字符串获取对象类型的辅助函数。
 * 返回类型索引（如 OBJ_STRING 等），模块类型返回负数标识，未找到返回 LLONG_MAX。 */
long long getObjectTypeByName(char *name) {

    for (long long i = 0; i < OBJ_TYPE_MAX; i++) {
        if (obj_type_name[i] && !strcasecmp(name, obj_type_name[i])) {
            return i;
        }
    }

    moduleType *mt = moduleTypeLookupModuleByNameIgnoreCase(name);
    if (mt != NULL) return -(REDISMODULE_TYPE_SIGN(mt->id));

    return LLONG_MAX;
}

/* 获取对象的类型名称字符串。NULL 对象返回 "none"，模块类型返回模块注册的名称。 */
char *getObjectTypeName(robj *o) {
    if (o == NULL) {
        return "none";
    }

    serverAssert(o->type >= 0 && o->type < OBJ_TYPE_MAX);

    if (o->type == OBJ_MODULE) {
        moduleValue *mv = o->ptr;
        return mv->type->name;
    } else {
        return obj_type_name[o->type];
    }
}

/* 此函数实现了 SCAN、HSCAN、SSCAN 和 ZSCAN 命令的核心逻辑。
 *
 * 参数说明：
 *   o: 如果传入对象，则必须是 Hash、Set 或 Zset 对象；
 *      如果为 NULL，命令将操作当前数据库的字典。
 *   cursor: 游标值，用于迭代。
 *
 * 当 'o' 不为 NULL 时，函数假设客户端参数向量中的第一个参数是 key，
 * 因此在解析选项之前会跳过它。
 *
 * 对于 Hash 对象，函数返回每个元素的 field 和 value。
 *
 * SCAN 命令使用游标迭代方式，可以在不阻塞服务器的情况下增量遍历大型数据集。
 * 返回的游标值用于下次迭代，返回 0 表示迭代完成。 */
void scanGenericCommand(client *c, robj *o, unsigned long long cursor) {
    int isKeysHfield = 0;
    int i, j;
    listNode *node;
    long count = 10;
    sds pat = NULL;
    sds typename = NULL;
    long long type = LLONG_MAX;
    int patlen = 0, use_pattern = 0, no_values = 0;
    dict *ht;

    /* Object must be NULL (to iterate keys names), or the type of the object
     * must be Set, Sorted Set, or Hash. */
    serverAssert(o == NULL || o->type == OBJ_SET || o->type == OBJ_HASH ||
                o->type == OBJ_ZSET);

    /* Set i to the first option argument. The previous one is the cursor. */
    i = (o == NULL) ? 2 : 3; /* Skip the key argument if needed. */

    /* Step 1: Parse options. */
    while (i < c->argc) {
        j = c->argc - i;
        if (!strcasecmp(c->argv[i]->ptr, "count") && j >= 2) {
            if (getLongFromObjectOrReply(c, c->argv[i+1], &count, NULL)
                != C_OK)
            {
                return;
            }

            if (count < 1) {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }

            i += 2;
        } else if (!strcasecmp(c->argv[i]->ptr, "match") && j >= 2) {
            pat = c->argv[i+1]->ptr;
            patlen = sdslen(pat);

            /* The pattern always matches if it is exactly "*", so it is
             * equivalent to disabling it. */
            use_pattern = !(patlen == 1 && pat[0] == '*');

            i += 2;
        } else if (!strcasecmp(c->argv[i]->ptr, "type") && o == NULL && j >= 2) {
            /* SCAN for a particular type only applies to the db dict */
            typename = c->argv[i+1]->ptr;
            type = getObjectTypeByName(typename);
            if (type == LLONG_MAX) {
                /* TODO: uncomment in redis 8.0
                addReplyErrorFormat(c, "unknown type name '%s'", typename);
                return; */
            }
            i+= 2;
        } else if (!strcasecmp(c->argv[i]->ptr, "novalues")) {
            if (!o || o->type != OBJ_HASH) {
                addReplyError(c, "NOVALUES option can only be used in HSCAN");
                return;
            }
            no_values = 1;
            i++;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* 步骤 2：迭代集合。
     *
     * 注意：如果对象使用 listpack、intset 或其他非哈希表编码，
     * 我们可以确定它只包含少量元素。因此为了避免维护状态，
     * 我们在单次调用中返回对象中的所有元素，并将游标设为 0
     * 以表示迭代结束。 */

    /* 处理哈希表编码的情况 */
    ht = NULL;
    if (o == NULL) {
        ht = NULL;
    } else if (o->type == OBJ_SET && o->encoding == OBJ_ENCODING_HT) {
        ht = o->ptr;
    } else if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT) {
        isKeysHfield = 1;
        ht = o->ptr;
    } else if (o->type == OBJ_ZSET && o->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        ht = zs->dict;
    }

    list *keys = listCreate();
    /* 为收集的 key 列表设置释放回调。
     * 对于主键空间字典，以及扫描使用哈希表编码的 key 时（有 'ht'），
     * 不需要定义释放方法，因为列表中的字符串只是 dictEntry 中指针的浅拷贝。
     * 扫描使用其他编码（如 listpack）的 key 时，需要释放添加到列表的临时字符串。
     * 上述规则的例外是 ZSET，即使扫描 dict 也会分配临时字符串。 */
    if (o && (!ht || o->type == OBJ_ZSET)) {
        listSetFreeMethod(keys, (void (*)(void*))sdsfree);
    }

    /* 主字典扫描或使用哈希表的数据结构 */
    if (!o || ht) {
        /* 将最大迭代次数设置为指定 COUNT 的 10 倍，
         * 这样即使哈希表处于病态状态（非常稀疏），也能避免阻塞太久，
         * 代价是可能返回很少或没有元素。 */
        long maxiterations = count*10;

        /* 我们传递 scanData 结构给回调，它包含以下字段：
         * 1. data.keys：用于添加新元素的列表；
         * 2. data.o：包含字典的对象，以便以类型相关的方式获取更多数据；
         * 3. data.type：数据库扫描时的指定类型过滤，LLONG_MAX 表示不需要类型匹配；
         * 4. data.pattern：模式字符串；
         * 5. data.sampled：最大迭代限制，用于处理空字典或大量空桶的情况，
         *    对于非空桶，需要限制采样数量以防止因过滤太多 key 导致长时间阻塞；
         * 6. data.no_values：控制是否返回 value，还是仅返回 key。 */
        scanData data = {
            .keys = keys,
            .o = o,
            .type = type,
            .pattern = use_pattern ? pat : NULL,
            .sampled = 0,
            .no_values = no_values,
            .strlen = (isKeysHfield) ? hfieldlen : sdslen,
        };

        /* 模式匹配可能将所有匹配的 key 限制在一个集群槽位中 */
        int onlydidx = -1;
        if (o == NULL && use_pattern && server.cluster_enabled) {
            onlydidx = patternHashSlot(pat, patlen);
        }
        do {
            /* 集群模式下每个槽位有独立的字典。
             * 如果游标为 0，应该尝试探索下一个非空槽位。 */
            if (o == NULL) {
                cursor = kvstoreScan(c->db->keys, cursor, onlydidx, scanCallback, NULL, &data);
            } else {
                cursor = dictScan(ht, cursor, scanCallback, &data);
            }
        } while (cursor && maxiterations-- && data.sampled < count);
    } else if (o->type == OBJ_SET) {
        char *str;
        char buf[LONG_STR_SIZE];
        size_t len;
        int64_t llele;
        setTypeIterator *si = setTypeInitIterator(o);
        while (setTypeNext(si, &str, &len, &llele) != -1) {
            if (str == NULL) {
                len = ll2string(buf, sizeof(buf), llele);
            }
            char *key = str ? str : buf;
            if (use_pattern && !stringmatchlen(pat, sdslen(pat), key, len, 0)) {
                continue;
            }
            listAddNodeTail(keys, sdsnewlen(key, len));
        }
        setTypeReleaseIterator(si);
        cursor = 0;
    } else if ((o->type == OBJ_HASH || o->type == OBJ_ZSET) &&
               o->encoding == OBJ_ENCODING_LISTPACK)
    {
        unsigned char *p = lpFirst(o->ptr);
        unsigned char *str;
        int64_t len;
        unsigned char intbuf[LP_INTBUF_SIZE];

        while(p) {
            str = lpGet(p, &len, intbuf);
            /* point to the value */
            p = lpNext(o->ptr, p);
            if (use_pattern && !stringmatchlen(pat, sdslen(pat), (char *)str, len, 0)) {
                /* jump to the next key/val pair */
                p = lpNext(o->ptr, p);
                continue;
            }
            /* add key object */
            listAddNodeTail(keys, sdsnewlen(str, len));
            /* add value object */
            if (!no_values) {
                str = lpGet(p, &len, intbuf);
                listAddNodeTail(keys, sdsnewlen(str, len));
            }
            p = lpNext(o->ptr, p);
        }
        cursor = 0;
    } else if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        int64_t len;
        long long expire_at;
        unsigned char *lp = hashTypeListpackGetLp(o);
        unsigned char *p = lpFirst(lp);
        unsigned char *str, *val;
        unsigned char intbuf[LP_INTBUF_SIZE];

        while (p) {
            str = lpGet(p, &len, intbuf);
            p = lpNext(lp, p);
            val = p; /* Keep pointer to value */

            p = lpNext(lp, p);
            serverAssert(p && lpGetIntegerValue(p, &expire_at));

            if (hashTypeIsExpired(o, expire_at) ||
               (use_pattern && !stringmatchlen(pat, sdslen(pat), (char *)str, len, 0)))
            {
                /* jump to the next key/val pair */
                p = lpNext(lp, p);
                continue;
            }

            /* add key object */
            listAddNodeTail(keys, sdsnewlen(str, len));
            /* add value object */
            if (!no_values) {
                str = lpGet(val, &len, intbuf);
                listAddNodeTail(keys, sdsnewlen(str, len));
            }
            p = lpNext(lp, p);
        }
        cursor = 0;
    } else {
        serverPanic("Not handled encoding in SCAN.");
    }

    /* Step 3: Filter the expired keys */
    if (o == NULL && listLength(keys)) {
        robj kobj;
        listIter li;
        listNode *ln;
        listRewind(keys, &li);
        while ((ln = listNext(&li))) {
            sds key = listNodeValue(ln);
            initStaticStringObject(kobj, key);
            /* Filter an element if it isn't the type we want. */
            /* TODO: remove this in redis 8.0 */
            if (typename) {
                robj* typecheck = lookupKeyReadWithFlags(c->db, &kobj, LOOKUP_NOTOUCH|LOOKUP_NONOTIFY);
                if (!typecheck || !objectTypeCompare(typecheck, type)) {
                    listDelNode(keys, ln);
                }
                continue;
            }
            if (expireIfNeeded(c->db, &kobj, 0) != KEY_VALID) {
                listDelNode(keys, ln);
            }
        }
    }

    /* Step 4: Reply to the client. */
    addReplyArrayLen(c, 2);
    addReplyBulkLongLong(c,cursor);

    unsigned long long idx = 0;
    addReplyArrayLen(c, listLength(keys));
    while ((node = listFirst(keys)) != NULL) {
        void *key = listNodeValue(node);
        /* For HSCAN, list will contain keys value pairs unless no_values arg
         * was given. We should call mstrlen for the keys only. */
        int hfieldkey = isKeysHfield && (no_values || (idx++ % 2 == 0));
        addReplyBulkCBuffer(c, key, hfieldkey ? mstrlen(key) : sdslen(key));
        listDelNode(keys, node);
    }

    listRelease(keys);
}

/* The SCAN command completely relies on scanGenericCommand. */
void scanCommand(client *c) {
    unsigned long long cursor;
    if (parseScanCursorOrReply(c,c->argv[1],&cursor) == C_ERR) return;
    scanGenericCommand(c,NULL,cursor);
}

void dbsizeCommand(client *c) {
    addReplyLongLong(c,kvstoreSize(c->db->keys));
}

void lastsaveCommand(client *c) {
    addReplyLongLong(c,server.lastsave);
}

void typeCommand(client *c) {
    robj *o;
    o = lookupKeyReadWithFlags(c->db,c->argv[1],LOOKUP_NOTOUCH);
    addReplyStatus(c, getObjectTypeName(o));
}

void shutdownCommand(client *c) {
    int flags = SHUTDOWN_NOFLAGS;
    int abort = 0;
    for (int i = 1; i < c->argc; i++) {
        if (!strcasecmp(c->argv[i]->ptr,"nosave")) {
            flags |= SHUTDOWN_NOSAVE;
        } else if (!strcasecmp(c->argv[i]->ptr,"save")) {
            flags |= SHUTDOWN_SAVE;
        } else if (!strcasecmp(c->argv[i]->ptr, "now")) {
            flags |= SHUTDOWN_NOW;
        } else if (!strcasecmp(c->argv[i]->ptr, "force")) {
            flags |= SHUTDOWN_FORCE;
        } else if (!strcasecmp(c->argv[i]->ptr, "abort")) {
            abort = 1;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }
    if ((abort && flags != SHUTDOWN_NOFLAGS) ||
        (flags & SHUTDOWN_NOSAVE && flags & SHUTDOWN_SAVE))
    {
        /* Illegal combo. */
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    if (abort) {
        if (abortShutdown() == C_OK)
            addReply(c, shared.ok);
        else
            addReplyError(c, "No shutdown in progress.");
        return;
    }

    if (!(flags & SHUTDOWN_NOW) && c->flags & CLIENT_DENY_BLOCKING) {
        addReplyError(c, "SHUTDOWN without NOW or ABORT isn't allowed for DENY BLOCKING client");
        return;
    }

    if (!(flags & SHUTDOWN_NOSAVE) && isInsideYieldingLongCommand()) {
        /* Script timed out. Shutdown allowed only with the NOSAVE flag. See
         * also processCommand where these errors are returned. */
        if (server.busy_module_yield_flags && server.busy_module_yield_reply) {
            addReplyErrorFormat(c, "-BUSY %s", server.busy_module_yield_reply);
        } else if (server.busy_module_yield_flags) {
            addReplyErrorObject(c, shared.slowmoduleerr);
        } else if (scriptIsEval()) {
            addReplyErrorObject(c, shared.slowevalerr);
        } else {
            addReplyErrorObject(c, shared.slowscripterr);
        }
        return;
    }

    blockClientShutdown(c);
    if (prepareForShutdown(flags) == C_OK) exit(0);
    /* If we're here, then shutdown is ongoing (the client is still blocked) or
     * failed (the client has received an error). */
}

void renameGenericCommand(client *c, int nx) {
    robj *o;
    long long expire;
    int samekey = 0;
    uint64_t minHashExpireTime = EB_EXPIRE_TIME_INVALID;

    /* When source and dest key is the same, no operation is performed,
     * if the key exists, however we still return an error on unexisting key. */
    if (sdscmp(c->argv[1]->ptr,c->argv[2]->ptr) == 0) samekey = 1;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr)) == NULL)
        return;

    if (samekey) {
        addReply(c,nx ? shared.czero : shared.ok);
        return;
    }

    incrRefCount(o);
    expire = getExpire(c->db,c->argv[1]);
    if (lookupKeyWrite(c->db,c->argv[2]) != NULL) {
        if (nx) {
            decrRefCount(o);
            addReply(c,shared.czero);
            return;
        }
        /* Overwrite: delete the old key before creating the new one
         * with the same name. */
        dbDelete(c->db,c->argv[2]);
    }
    dictEntry *de = dbAdd(c->db, c->argv[2], o);
    if (expire != -1) setExpire(c,c->db,c->argv[2],expire);

    /* If hash with expiration on fields then remove it from global HFE DS and
     * keep next expiration time. Otherwise, dbDelete() will remove it from the
     * global HFE DS and we will lose the expiration time. */
    if (o->type == OBJ_HASH)
        minHashExpireTime = hashTypeRemoveFromExpires(&c->db->hexpires, o);

    dbDelete(c->db,c->argv[1]);

    /* If hash with HFEs, register in db->hexpires */
    if (minHashExpireTime != EB_EXPIRE_TIME_INVALID)
        hashTypeAddToExpires(c->db, dictGetKey(de), o, minHashExpireTime);

    signalModifiedKey(c,c->db,c->argv[1]);
    signalModifiedKey(c,c->db,c->argv[2]);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"rename_from",
        c->argv[1],c->db->id);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"rename_to",
        c->argv[2],c->db->id);
    server.dirty++;
    addReply(c,nx ? shared.cone : shared.ok);
}

void renameCommand(client *c) {
    renameGenericCommand(c,0);
}

void renamenxCommand(client *c) {
    renameGenericCommand(c,1);
}

void moveCommand(client *c) {
    robj *o;
    redisDb *src, *dst;
    int srcid, dbid;
    long long expire;
    uint64_t hashExpireTime = EB_EXPIRE_TIME_INVALID;

    if (server.cluster_enabled) {
        addReplyError(c,"MOVE is not allowed in cluster mode");
        return;
    }

    /* Obtain source and target DB pointers */
    src = c->db;
    srcid = c->db->id;

    if (getIntFromObjectOrReply(c, c->argv[2], &dbid, NULL) != C_OK)
        return;

    if (selectDb(c,dbid) == C_ERR) {
        addReplyError(c,"DB index is out of range");
        return;
    }
    dst = c->db;
    selectDb(c,srcid); /* Back to the source DB */

    /* If the user is moving using as target the same
     * DB as the source DB it is probably an error. */
    if (src == dst) {
        addReplyErrorObject(c,shared.sameobjecterr);
        return;
    }

    /* Check if the element exists and get a reference */
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (!o) {
        addReply(c,shared.czero);
        return;
    }
    expire = getExpire(c->db,c->argv[1]);

    /* Return zero if the key already exists in the target DB */
    if (lookupKeyWrite(dst,c->argv[1]) != NULL) {
        addReply(c,shared.czero);
        return;
    }
    dictEntry *dstDictEntry = dbAdd(dst,c->argv[1],o);
    if (expire != -1) setExpire(c,dst,c->argv[1],expire);

    /* If hash with expiration on fields, remove it from global HFE DS and keep
     * aside registered expiration time. Must be before deletion of the object.
     * hexpires (ebuckets) embed in stored items its structure. */
    if (o->type == OBJ_HASH)
        hashExpireTime = hashTypeRemoveFromExpires(&src->hexpires, o);

    incrRefCount(o);

    /* OK! key moved, free the entry in the source DB */
    dbDelete(src,c->argv[1]);

    /* If object of type hash with expiration on fields. Taken care to add the
     * hash to hexpires of `dst` only after dbDelete(). */
    if (hashExpireTime != EB_EXPIRE_TIME_INVALID)
        hashTypeAddToExpires(dst, dictGetKey(dstDictEntry), o, hashExpireTime);

    signalModifiedKey(c,src,c->argv[1]);
    signalModifiedKey(c,dst,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_GENERIC,
                "move_from",c->argv[1],src->id);
    notifyKeyspaceEvent(NOTIFY_GENERIC,
                "move_to",c->argv[1],dst->id);

    server.dirty++;
    addReply(c,shared.cone);
}

void copyCommand(client *c) {
    robj *o;
    redisDb *src, *dst;
    int srcid, dbid;
    long long expire;
    int j, replace = 0, delete = 0;

    /* Obtain source and target DB pointers 
     * Default target DB is the same as the source DB 
     * Parse the REPLACE option and targetDB option. */
    src = c->db;
    dst = c->db;
    srcid = c->db->id;
    dbid = c->db->id;
    for (j = 3; j < c->argc; j++) {
        int additional = c->argc - j - 1;
        if (!strcasecmp(c->argv[j]->ptr,"replace")) {
            replace = 1;
        } else if (!strcasecmp(c->argv[j]->ptr, "db") && additional >= 1) {
            if (getIntFromObjectOrReply(c, c->argv[j+1], &dbid, NULL) != C_OK)
                return;

            if (selectDb(c, dbid) == C_ERR) {
                addReplyError(c,"DB index is out of range");
                return;
            }
            dst = c->db;
            selectDb(c,srcid); /* Back to the source DB */
            j++; /* Consume additional arg. */
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    if ((server.cluster_enabled == 1) && (srcid != 0 || dbid != 0)) {
        addReplyError(c,"Copying to another database is not allowed in cluster mode");
        return;
    }

    /* If the user select the same DB as
     * the source DB and using newkey as the same key
     * it is probably an error. */
    robj *key = c->argv[1];
    robj *newkey = c->argv[2];
    if (src == dst && (sdscmp(key->ptr, newkey->ptr) == 0)) {
        addReplyErrorObject(c,shared.sameobjecterr);
        return;
    }

    /* Check if the element exists and get a reference */
    o = lookupKeyRead(c->db, key);
    if (!o) {
        addReply(c,shared.czero);
        return;
    }
    expire = getExpire(c->db,key);

    /* Return zero if the key already exists in the target DB. 
     * If REPLACE option is selected, delete newkey from targetDB. */
    if (lookupKeyWrite(dst,newkey) != NULL) {
        if (replace) {
            delete = 1;
        } else {
            addReply(c,shared.czero);
            return;
        }
    }

    /* Duplicate object according to object's type. */
    robj *newobj;
    uint64_t minHashExpire = EB_EXPIRE_TIME_INVALID; /* HFE feature */
    switch(o->type) {
        case OBJ_STRING: newobj = dupStringObject(o); break;
        case OBJ_LIST: newobj = listTypeDup(o); break;
        case OBJ_SET: newobj = setTypeDup(o); break;
        case OBJ_ZSET: newobj = zsetDup(o); break;
        case OBJ_HASH: newobj = hashTypeDup(o, newkey->ptr, &minHashExpire); break;
        case OBJ_STREAM: newobj = streamDup(o); break;
        case OBJ_MODULE:
            newobj = moduleTypeDupOrReply(c, key, newkey, dst->id, o);
            if (!newobj) return;
            break;
        default:
            addReplyError(c, "unknown type object");
            return;
    }

    if (delete) {
        dbDelete(dst,newkey);
    }

    dictEntry *deCopy = dbAdd(dst,newkey,newobj);

    /* if key with expiration then set it */
    if (expire != -1)
        setExpire(c, dst, newkey, expire);

    /* If minExpiredField was set, then the object is hash with expiration
     * on fields and need to register it in global HFE DS */
    if (minHashExpire != EB_EXPIRE_TIME_INVALID)
        hashTypeAddToExpires(dst, dictGetKey(deCopy), newobj, minHashExpire);

    /* OK! key copied */
    signalModifiedKey(c,dst,c->argv[2]);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"copy_to",c->argv[2],dst->id);

    server.dirty++;
    addReply(c,shared.cone);
}

/* Helper function for dbSwapDatabases(): scans the list of keys that have
 * one or more blocked clients for B[LR]POP or other blocking commands
 * and signal the keys as ready if they are of the right type. See the comment
 * where the function is used for more info. */
void scanDatabaseForReadyKeys(redisDb *db) {
    dictEntry *de;
    dictIterator *di = dictGetSafeIterator(db->blocking_keys);
    while((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        dictEntry *kde = dbFind(db, key->ptr);
        if (kde) {
            robj *value = dictGetVal(kde);
            signalKeyAsReady(db, key, value->type);
        }
    }
    dictReleaseIterator(di);
}

/* Since we are unblocking XREADGROUP clients in the event the
 * key was deleted/overwritten we must do the same in case the
 * database was flushed/swapped. */
void scanDatabaseForDeletedKeys(redisDb *emptied, redisDb *replaced_with) {
    dictEntry *de;
    dictIterator *di = dictGetSafeIterator(emptied->blocking_keys);
    while((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        int existed = 0, exists = 0;
        int original_type = -1, curr_type = -1;

        dictEntry *kde = dbFind(emptied, key->ptr);
        if (kde) {
            robj *value = dictGetVal(kde);
            original_type = value->type;
            existed = 1;
        }

        if (replaced_with) {
            kde = dbFind(replaced_with, key->ptr);
            if (kde) {
                robj *value = dictGetVal(kde);
                curr_type = value->type;
                exists = 1;
            }
        }
        /* We want to try to unblock any client using a blocking XREADGROUP */
        if ((existed && !exists) || original_type != curr_type)
            signalDeletedKeyAsReady(emptied, key, original_type);
    }
    dictReleaseIterator(di);
}

/* Swap two databases at runtime so that all clients will magically see
 * the new database even if already connected. Note that the client
 * structure c->db points to a given DB, so we need to be smarter and
 * swap the underlying referenced structures, otherwise we would need
 * to fix all the references to the Redis DB structure.
 *
 * Returns C_ERR if at least one of the DB ids are out of range, otherwise
 * C_OK is returned. */
int dbSwapDatabases(int id1, int id2) {
    if (id1 < 0 || id1 >= server.dbnum ||
        id2 < 0 || id2 >= server.dbnum) return C_ERR;
    if (id1 == id2) return C_OK;
    redisDb aux = server.db[id1];
    redisDb *db1 = &server.db[id1], *db2 = &server.db[id2];

    /* Swapdb should make transaction fail if there is any
     * client watching keys */
    touchAllWatchedKeysInDb(db1, db2);
    touchAllWatchedKeysInDb(db2, db1);

    /* Try to unblock any XREADGROUP clients if the key no longer exists. */
    scanDatabaseForDeletedKeys(db1, db2);
    scanDatabaseForDeletedKeys(db2, db1);

    /* Swap hash tables. Note that we don't swap blocking_keys,
     * ready_keys and watched_keys, since we want clients to
     * remain in the same DB they were. */
    db1->keys = db2->keys;
    db1->expires = db2->expires;
    db1->hexpires = db2->hexpires;
    db1->avg_ttl = db2->avg_ttl;
    db1->expires_cursor = db2->expires_cursor;

    db2->keys = aux.keys;
    db2->expires = aux.expires;
    db2->hexpires = aux.hexpires;
    db2->avg_ttl = aux.avg_ttl;
    db2->expires_cursor = aux.expires_cursor;

    /* Now we need to handle clients blocked on lists: as an effect
     * of swapping the two DBs, a client that was waiting for list
     * X in a given DB, may now actually be unblocked if X happens
     * to exist in the new version of the DB, after the swap.
     *
     * However normally we only do this check for efficiency reasons
     * in dbAdd() when a list is created. So here we need to rescan
     * the list of clients blocked on lists and signal lists as ready
     * if needed. */
    scanDatabaseForReadyKeys(db1);
    scanDatabaseForReadyKeys(db2);
    return C_OK;
}

/* Logically, this discards (flushes) the old main database, and apply the newly loaded
 * database (temp) as the main (active) database, the actual freeing of old database
 * (which will now be placed in the temp one) is done later. */
void swapMainDbWithTempDb(redisDb *tempDb) {
    for (int i=0; i<server.dbnum; i++) {
        redisDb aux = server.db[i];
        redisDb *activedb = &server.db[i], *newdb = &tempDb[i];

        /* Swapping databases should make transaction fail if there is any
         * client watching keys. */
        touchAllWatchedKeysInDb(activedb, newdb);

        /* Try to unblock any XREADGROUP clients if the key no longer exists. */
        scanDatabaseForDeletedKeys(activedb, newdb);

        /* Swap hash tables. Note that we don't swap blocking_keys,
         * ready_keys and watched_keys, since clients 
         * remain in the same DB they were. */
        activedb->keys = newdb->keys;
        activedb->expires = newdb->expires;
        activedb->hexpires = newdb->hexpires;
        activedb->avg_ttl = newdb->avg_ttl;
        activedb->expires_cursor = newdb->expires_cursor;

        newdb->keys = aux.keys;
        newdb->expires = aux.expires;
        newdb->hexpires = aux.hexpires;
        newdb->avg_ttl = aux.avg_ttl;
        newdb->expires_cursor = aux.expires_cursor;

        /* Now we need to handle clients blocked on lists: as an effect
         * of swapping the two DBs, a client that was waiting for list
         * X in a given DB, may now actually be unblocked if X happens
         * to exist in the new version of the DB, after the swap.
         *
         * However normally we only do this check for efficiency reasons
         * in dbAdd() when a list is created. So here we need to rescan
         * the list of clients blocked on lists and signal lists as ready
         * if needed. */
        scanDatabaseForReadyKeys(activedb);
    }

    trackingInvalidateKeysOnFlush(1);
    flushSlaveKeysWithExpireList();
}

/* SWAPDB db1 db2 */
void swapdbCommand(client *c) {
    int id1, id2;

    /* Not allowed in cluster mode: we have just DB 0 there. */
    if (server.cluster_enabled) {
        addReplyError(c,"SWAPDB is not allowed in cluster mode");
        return;
    }

    /* Get the two DBs indexes. */
    if (getIntFromObjectOrReply(c, c->argv[1], &id1,
        "invalid first DB index") != C_OK)
        return;

    if (getIntFromObjectOrReply(c, c->argv[2], &id2,
        "invalid second DB index") != C_OK)
        return;

    /* Swap... */
    if (dbSwapDatabases(id1,id2) == C_ERR) {
        addReplyError(c,"DB index is out of range");
        return;
    } else {
        RedisModuleSwapDbInfo si = {REDISMODULE_SWAPDBINFO_VERSION,id1,id2};
        moduleFireServerEvent(REDISMODULE_EVENT_SWAPDB,0,&si);
        server.dirty++;
        addReply(c,shared.ok);
    }
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

int removeExpire(redisDb *db, robj *key) {
    return kvstoreDictDelete(db->expires, getKeySlot(key->ptr), key->ptr) == DICT_OK;
}

/* Set an expire to the specified key. If the expire is set in the context
 * of an user calling a command 'c' is the client, otherwise 'c' is set
 * to NULL. The 'when' parameter is the absolute unix time in milliseconds
 * after which the key will no longer be considered valid. */
void setExpire(client *c, redisDb *db, robj *key, long long when) {
    dictEntry *kde, *de, *existing;

    /* Reuse the sds from the main dict in the expire dict */
    int slot = getKeySlot(key->ptr);
    kde = kvstoreDictFind(db->keys, slot, key->ptr);
    serverAssertWithInfo(NULL,key,kde != NULL);
    de = kvstoreDictAddRaw(db->expires, slot, dictGetKey(kde), &existing);
    if (existing) {
        dictSetSignedIntegerVal(existing, when);
    } else {
        dictSetSignedIntegerVal(de, when);
    }

    int writable_slave = server.masterhost && server.repl_slave_ro == 0;
    if (c && writable_slave && !(c->flags & CLIENT_MASTER))
        rememberSlaveKeyWithExpire(db,key);
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
long long getExpire(redisDb *db, robj *key) {
    dictEntry *de;

    if ((de = dbFindExpires(db, key->ptr)) == NULL)
        return -1;

    return dictGetSignedIntegerVal(de);
}

/* Delete the specified expired key and propagate expire. */
void deleteExpiredKeyAndPropagate(redisDb *db, robj *keyobj) {
    mstime_t expire_latency;
    latencyStartMonitor(expire_latency);
    dbGenericDelete(db,keyobj,server.lazyfree_lazy_expire,DB_FLAG_KEY_EXPIRED);
    latencyEndMonitor(expire_latency);
    latencyAddSampleIfNeeded("expire-del",expire_latency);
    notifyKeyspaceEvent(NOTIFY_EXPIRED,"expired",keyobj,db->id);
    signalModifiedKey(NULL, db, keyobj);
    propagateDeletion(db,keyobj,server.lazyfree_lazy_expire);
    server.stat_expiredkeys++;
}

/* Propagate an implicit key deletion into replicas and the AOF file.
 * When a key was deleted in the master by eviction, expiration or a similar
 * mechanism a DEL/UNLINK operation for this key is sent
 * to all the replicas and the AOF file if enabled.
 *
 * This way the key deletion is centralized in one place, and since both
 * AOF and the replication link guarantee operation ordering, everything
 * will be consistent even if we allow write operations against deleted
 * keys.
 *
 * This function may be called from:
 * 1. Within call(): Example: Lazy-expire on key access.
 *    In this case the caller doesn't have to do anything
 *    because call() handles server.also_propagate(); or
 * 2. Outside of call(): Example: Active-expire, eviction, slot ownership changed.
 *    In this the caller must remember to call
 *    postExecutionUnitOperations, preferably just after a
 *    single deletion batch, so that DEL/UNLINK will NOT be wrapped
 *    in MULTI/EXEC */
void propagateDeletion(redisDb *db, robj *key, int lazy) {
    robj *argv[2];

    argv[0] = lazy ? shared.unlink : shared.del;
    argv[1] = key;
    incrRefCount(argv[0]);
    incrRefCount(argv[1]);

    /* If the master decided to delete a key we must propagate it to replicas no matter what.
     * Even if module executed a command without asking for propagation. */
    int prev_replication_allowed = server.replication_allowed;
    server.replication_allowed = 1;
    alsoPropagate(db->id,argv,2,PROPAGATE_AOF|PROPAGATE_REPL);
    server.replication_allowed = prev_replication_allowed;

    decrRefCount(argv[0]);
    decrRefCount(argv[1]);
}

/* Check if the key is expired. */
int keyIsExpired(redisDb *db, robj *key) {
    /* Don't expire anything while loading. It will be done later. */
    if (server.loading) return 0;

    mstime_t when = getExpire(db,key);
    mstime_t now;

    if (when < 0) return 0; /* No expire for this key */

    now = commandTimeSnapshot();

    /* The key expired if the current (virtual or real) time is greater
     * than the expire time of the key. */
    return now > when;
}

/* This function is called when we are going to perform some operation
 * in a given key, but such key may be already logically expired even if
 * it still exists in the database. The main way this function is called
 * is via lookupKey*() family of functions.
 *
 * The behavior of the function depends on the replication role of the
 * instance, because by default replicas do not delete expired keys. They
 * wait for DELs from the master for consistency matters. However even
 * replicas will try to have a coherent return value for the function,
 * so that read commands executed in the replica side will be able to
 * behave like if the key is expired even if still present (because the
 * master has yet to propagate the DEL).
 *
 * In masters as a side effect of finding a key which is expired, such
 * key will be evicted from the database. Also this may trigger the
 * propagation of a DEL/UNLINK command in AOF / replication stream.
 *
 * On replicas, this function does not delete expired keys by default, but
 * it still returns KEY_EXPIRED if the key is logically expired. To force deletion
 * of logically expired keys even on replicas, use the EXPIRE_FORCE_DELETE_EXPIRED
 * flag. Note though that if the current client is executing
 * replicated commands from the master, keys are never considered expired.
 *
 * On the other hand, if you just want expiration check, but need to avoid
 * the actual key deletion and propagation of the deletion, use the
 * EXPIRE_AVOID_DELETE_EXPIRED flag.
 *
 * The return value of the function is KEY_VALID if the key is still valid.
 * The function returns KEY_EXPIRED if the key is expired BUT not deleted,
 * or returns KEY_DELETED if the key is expired and deleted. */
keyStatus expireIfNeeded(redisDb *db, robj *key, int flags) {
    if (server.lazy_expire_disabled) return KEY_VALID;
    if (!keyIsExpired(db,key)) return KEY_VALID;

    /* If we are running in the context of a replica, instead of
     * evicting the expired key from the database, we return ASAP:
     * the replica key expiration is controlled by the master that will
     * send us synthesized DEL operations for expired keys. The
     * exception is when write operations are performed on writable
     * replicas.
     *
     * Still we try to return the right information to the caller,
     * that is, KEY_VALID if we think the key should still be valid,
     * KEY_EXPIRED if we think the key is expired but don't want to delete it at this time.
     *
     * When replicating commands from the master, keys are never considered
     * expired. */
    if (server.masterhost != NULL) {
        if (server.current_client && (server.current_client->flags & CLIENT_MASTER)) return KEY_VALID;
        if (!(flags & EXPIRE_FORCE_DELETE_EXPIRED)) return KEY_EXPIRED;
    }

    /* In some cases we're explicitly instructed to return an indication of a
     * missing key without actually deleting it, even on masters. */
    if (flags & EXPIRE_AVOID_DELETE_EXPIRED)
        return KEY_EXPIRED;

    /* If 'expire' action is paused, for whatever reason, then don't expire any key.
     * Typically, at the end of the pause we will properly expire the key OR we
     * will have failed over and the new primary will send us the expire. */
    if (isPausedActionsWithUpdate(PAUSE_ACTION_EXPIRE)) return KEY_EXPIRED;

    /* The key needs to be converted from static to heap before deleted */
    int static_key = key->refcount == OBJ_STATIC_REFCOUNT;
    if (static_key) {
        key = createStringObject(key->ptr, sdslen(key->ptr));
    }
    /* Delete the key */
    deleteExpiredKeyAndPropagate(db,key);
    if (static_key) {
        decrRefCount(key);
    }
    return KEY_DELETED;
}

/* CB passed to kvstoreExpand.
 * The purpose is to skip expansion of unused dicts in cluster mode (all
 * dicts not mapped to *my* slots) */
static int dbExpandSkipSlot(int slot) {
    return !clusterNodeCoversSlot(getMyClusterNode(), slot);
}

/*
 * This functions increases size of the main/expires db to match desired number.
 * In cluster mode resizes all individual dictionaries for slots that this node owns.
 *
 * Based on the parameter `try_expand`, appropriate dict expand API is invoked.
 * if try_expand is set to 1, `dictTryExpand` is used else `dictExpand`.
 * The return code is either `DICT_OK`/`DICT_ERR` for both the API(s).
 * `DICT_OK` response is for successful expansion. However ,`DICT_ERR` response signifies failure in allocation in
 * `dictTryExpand` call and in case of `dictExpand` call it signifies no expansion was performed.
 */
static int dbExpandGeneric(kvstore *kvs, uint64_t db_size, int try_expand) {
    int ret;
    if (server.cluster_enabled) {
        /* We don't know exact number of keys that would fall into each slot, but we can
         * approximate it, assuming even distribution, divide it by the number of slots. */
        int slots = getMyShardSlotCount();
        if (slots == 0) return C_OK;
        db_size = db_size / slots;
        ret = kvstoreExpand(kvs, db_size, try_expand, dbExpandSkipSlot);
    } else {
        ret = kvstoreExpand(kvs, db_size, try_expand, NULL);
    }

    return ret? C_OK : C_ERR;
}

int dbExpand(redisDb *db, uint64_t db_size, int try_expand) {
    return dbExpandGeneric(db->keys, db_size, try_expand);
}

int dbExpandExpires(redisDb *db, uint64_t db_size, int try_expand) {
    return dbExpandGeneric(db->expires, db_size, try_expand);
}

static dictEntry *dbFindGeneric(kvstore *kvs, void *key) {
    return kvstoreDictFind(kvs, getKeySlot(key), key);
}

dictEntry *dbFind(redisDb *db, void *key) {
    return dbFindGeneric(db->keys, key);
}

dictEntry *dbFindExpires(redisDb *db, void *key) {
    return dbFindGeneric(db->expires, key);
}

unsigned long long dbSize(redisDb *db) {
    return kvstoreSize(db->keys);
}

unsigned long long dbScan(redisDb *db, unsigned long long cursor, dictScanFunction *scan_cb, void *privdata) {
    return kvstoreScan(db->keys, cursor, -1, scan_cb, NULL, privdata);
}

/* -----------------------------------------------------------------------------
 * API to get key arguments from commands
 * ---------------------------------------------------------------------------*/

/* Prepare the getKeysResult struct to hold numkeys, either by using the
 * pre-allocated keysbuf or by allocating a new array on the heap.
 *
 * This function must be called at least once before starting to populate
 * the result, and can be called repeatedly to enlarge the result array.
 */
keyReference *getKeysPrepareResult(getKeysResult *result, int numkeys) {
    /* GETKEYS_RESULT_INIT initializes keys to NULL, point it to the pre-allocated stack
     * buffer here. */
    if (!result->keys) {
        serverAssert(!result->numkeys);
        result->keys = result->keysbuf;
    }

    /* Resize if necessary */
    if (numkeys > result->size) {
        if (result->keys != result->keysbuf) {
            /* We're not using a static buffer, just (re)alloc */
            result->keys = zrealloc(result->keys, numkeys * sizeof(keyReference));
        } else {
            /* We are using a static buffer, copy its contents */
            result->keys = zmalloc(numkeys * sizeof(keyReference));
            if (result->numkeys)
                memcpy(result->keys, result->keysbuf, result->numkeys * sizeof(keyReference));
        }
        result->size = numkeys;
    }

    return result->keys;
}

/* Returns a bitmask with all the flags found in any of the key specs of the command.
 * The 'inv' argument means we'll return a mask with all flags that are missing in at least one spec. */
int64_t getAllKeySpecsFlags(struct redisCommand *cmd, int inv) {
    int64_t flags = 0;
    for (int j = 0; j < cmd->key_specs_num; j++) {
        keySpec *spec = cmd->key_specs + j;
        flags |= inv? ~spec->flags : spec->flags;
    }
    return flags;
}

/* Fetch the keys based of the provided key specs. Returns the number of keys found, or -1 on error.
 * There are several flags that can be used to modify how this function finds keys in a command.
 * 
 * GET_KEYSPEC_INCLUDE_NOT_KEYS: Return 'fake' keys as if they were keys.
 * GET_KEYSPEC_RETURN_PARTIAL:   Skips invalid and incomplete keyspecs but returns the keys
 *                               found in other valid keyspecs. 
 */
int getKeysUsingKeySpecs(struct redisCommand *cmd, robj **argv, int argc, int search_flags, getKeysResult *result) {
    long j, i, last, first, step;
    keyReference *keys;
    serverAssert(result->numkeys == 0); /* caller should initialize or reset it */

    for (j = 0; j < cmd->key_specs_num; j++) {
        keySpec *spec = cmd->key_specs + j;
        serverAssert(spec->begin_search_type != KSPEC_BS_INVALID);
        /* Skip specs that represent 'fake' keys */
        if ((spec->flags & CMD_KEY_NOT_KEY) && !(search_flags & GET_KEYSPEC_INCLUDE_NOT_KEYS)) {
            continue;
        }

        first = 0;
        if (spec->begin_search_type == KSPEC_BS_INDEX) {
            first = spec->bs.index.pos;
        } else if (spec->begin_search_type == KSPEC_BS_KEYWORD) {
            int start_index = spec->bs.keyword.startfrom > 0 ? spec->bs.keyword.startfrom : argc+spec->bs.keyword.startfrom;
            int end_index = spec->bs.keyword.startfrom > 0 ? argc-1: 1;
            for (i = start_index; i != end_index; i = start_index <= end_index ? i + 1 : i - 1) {
                if (i >= argc || i < 1)
                    break;
                if (!strcasecmp((char*)argv[i]->ptr,spec->bs.keyword.keyword)) {
                    first = i+1;
                    break;
                }
            }
            /* keyword not found */
            if (!first) {
                continue;
            }
        } else {
            /* unknown spec */
            goto invalid_spec;
        }

        if (spec->find_keys_type == KSPEC_FK_RANGE) {
            step = spec->fk.range.keystep;
            if (spec->fk.range.lastkey >= 0) {
                last = first + spec->fk.range.lastkey;
            } else {
                if (!spec->fk.range.limit) {
                    last = argc + spec->fk.range.lastkey;
                } else {
                    serverAssert(spec->fk.range.lastkey == -1);
                    last = first + ((argc-first)/spec->fk.range.limit + spec->fk.range.lastkey);
                }
            }
        } else if (spec->find_keys_type == KSPEC_FK_KEYNUM) {
            step = spec->fk.keynum.keystep;
            long long numkeys;
            if (spec->fk.keynum.keynumidx >= argc)
                goto invalid_spec;

            sds keynum_str = argv[first + spec->fk.keynum.keynumidx]->ptr;
            if (!string2ll(keynum_str,sdslen(keynum_str),&numkeys) || numkeys < 0) {
                /* Unable to parse the numkeys argument or it was invalid */
                goto invalid_spec;
            }

            first += spec->fk.keynum.firstkey;
            last = first + (long)numkeys-1;
        } else {
            /* unknown spec */
            goto invalid_spec;
        }

        /* First or last is out of bounds, which indicates a syntax error */
        if (last >= argc || last < first || first >= argc) {
            goto invalid_spec;
        }

        int count = ((last - first)+1);
        keys = getKeysPrepareResult(result, result->numkeys + count);

        for (i = first; i <= last; i += step) {
            if (i >= argc || i < first) {
                /* Modules commands, and standard commands with a not fixed number
                 * of arguments (negative arity parameter) do not have dispatch
                 * time arity checks, so we need to handle the case where the user
                 * passed an invalid number of arguments here. In this case we
                 * return no keys and expect the command implementation to report
                 * an arity or syntax error. */
                if (cmd->flags & CMD_MODULE || cmd->arity < 0) {
                    continue;
                } else {
                    serverPanic("Redis built-in command declared keys positions not matching the arity requirements.");
                }
            }
            keys[result->numkeys].pos = i;
            keys[result->numkeys].flags = spec->flags;
            result->numkeys++;
        }

        /* Handle incomplete specs (only after we added the current spec
         * to `keys`, just in case GET_KEYSPEC_RETURN_PARTIAL was given) */
        if (spec->flags & CMD_KEY_INCOMPLETE) {
            goto invalid_spec;
        }

        /* Done with this spec */
        continue;

invalid_spec:
        if (search_flags & GET_KEYSPEC_RETURN_PARTIAL) {
            continue;
        } else {
            result->numkeys = 0;
            return -1;
        }
    }

    return result->numkeys;
}

/* Return all the arguments that are keys in the command passed via argc / argv. 
 * This function will eventually replace getKeysFromCommand.
 *
 * The command returns the positions of all the key arguments inside the array,
 * so the actual return value is a heap allocated array of integers. The
 * length of the array is returned by reference into *numkeys.
 * 
 * Along with the position, this command also returns the flags that are
 * associated with how Redis will access the key.
 *
 * 'cmd' must be point to the corresponding entry into the redisCommand
 * table, according to the command name in argv[0]. */
int getKeysFromCommandWithSpecs(struct redisCommand *cmd, robj **argv, int argc, int search_flags, getKeysResult *result) {
    /* The command has at least one key-spec not marked as NOT_KEY */
    int has_keyspec = (getAllKeySpecsFlags(cmd, 1) & CMD_KEY_NOT_KEY);
    /* The command has at least one key-spec marked as VARIABLE_FLAGS */
    int has_varflags = (getAllKeySpecsFlags(cmd, 0) & CMD_KEY_VARIABLE_FLAGS);

    /* We prefer key-specs if there are any, and their flags are reliable. */
    if (has_keyspec && !has_varflags) {
        int ret = getKeysUsingKeySpecs(cmd,argv,argc,search_flags,result);
        if (ret >= 0)
            return ret;
        /* If the specs returned with an error (probably an INVALID or INCOMPLETE spec),
         * fallback to the callback method. */
    }

    /* Resort to getkeys callback methods. */
    if (cmd->flags & CMD_MODULE_GETKEYS)
        return moduleGetCommandKeysViaAPI(cmd,argv,argc,result);

    /* We use native getkeys as a last resort, since not all these native getkeys provide
     * flags properly (only the ones that correspond to INVALID, INCOMPLETE or VARIABLE_FLAGS do.*/
    if (cmd->getkeys_proc)
        return cmd->getkeys_proc(cmd,argv,argc,result);
    return 0;
}

/* This function returns a sanity check if the command may have keys. */
int doesCommandHaveKeys(struct redisCommand *cmd) {
    return cmd->getkeys_proc ||                                 /* has getkeys_proc (non modules) */
        (cmd->flags & CMD_MODULE_GETKEYS) ||                    /* module with GETKEYS */
        (getAllKeySpecsFlags(cmd, 1) & CMD_KEY_NOT_KEY);        /* has at least one key-spec not marked as NOT_KEY */
}

/* A simplified channel spec table that contains all of the redis commands
 * and which channels they have and how they are accessed. */
typedef struct ChannelSpecs {
    redisCommandProc *proc; /* Command procedure to match against */
    uint64_t flags;         /* CMD_CHANNEL_* flags for this command */
    int start;              /* The initial position of the first channel */
    int count;              /* The number of channels, or -1 if all remaining
                             * arguments are channels. */
} ChannelSpecs;

ChannelSpecs commands_with_channels[] = {
    {subscribeCommand, CMD_CHANNEL_SUBSCRIBE, 1, -1},
    {ssubscribeCommand, CMD_CHANNEL_SUBSCRIBE, 1, -1},
    {unsubscribeCommand, CMD_CHANNEL_UNSUBSCRIBE, 1, -1},
    {sunsubscribeCommand, CMD_CHANNEL_UNSUBSCRIBE, 1, -1},
    {psubscribeCommand, CMD_CHANNEL_PATTERN | CMD_CHANNEL_SUBSCRIBE, 1, -1},
    {punsubscribeCommand, CMD_CHANNEL_PATTERN | CMD_CHANNEL_UNSUBSCRIBE, 1, -1},
    {publishCommand, CMD_CHANNEL_PUBLISH, 1, 1},
    {spublishCommand, CMD_CHANNEL_PUBLISH, 1, 1},
    {NULL,0} /* Terminator. */
};

/* Returns 1 if the command may access any channels matched by the flags
 * argument. */
int doesCommandHaveChannelsWithFlags(struct redisCommand *cmd, int flags) {
    /* If a module declares get channels, we are just going to assume
     * has channels. This API is allowed to return false positives. */
    if (cmd->flags & CMD_MODULE_GETCHANNELS) {
        return 1;
    }
    for (ChannelSpecs *spec = commands_with_channels; spec->proc != NULL; spec += 1) {
        if (cmd->proc == spec->proc) {
            return !!(spec->flags & flags);
        }
    }
    return 0;
}

/* Return all the arguments that are channels in the command passed via argc / argv. 
 * This function behaves similar to getKeysFromCommandWithSpecs, but with channels 
 * instead of keys.
 * 
 * The command returns the positions of all the channel arguments inside the array,
 * so the actual return value is a heap allocated array of integers. The
 * length of the array is returned by reference into *numkeys.
 * 
 * Along with the position, this command also returns the flags that are
 * associated with how Redis will access the channel.
 *
 * 'cmd' must be point to the corresponding entry into the redisCommand
 * table, according to the command name in argv[0]. */
int getChannelsFromCommand(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    keyReference *keys;
    /* If a module declares get channels, use that. */
    if (cmd->flags & CMD_MODULE_GETCHANNELS) {
        return moduleGetCommandChannelsViaAPI(cmd, argv, argc, result);
    }
    /* Otherwise check the channel spec table */
    for (ChannelSpecs *spec = commands_with_channels; spec != NULL; spec += 1) {
        if (cmd->proc == spec->proc) {
            int start = spec->start;
            int stop = (spec->count == -1) ? argc : start + spec->count;
            if (stop > argc) stop = argc;
            int count = 0;
            keys = getKeysPrepareResult(result, stop - start);
            for (int i = start; i < stop; i++ ) {
                keys[count].pos = i;
                keys[count++].flags = spec->flags;
            }
            result->numkeys = count;
            return count;
        }
    }
    return 0;
}

/* The base case is to use the keys position as given in the command table
 * (firstkey, lastkey, step).
 * This function works only on command with the legacy_range_key_spec,
 * all other commands should be handled by getkeys_proc. 
 * 
 * If the commands keyspec is incomplete, no keys will be returned, and the provided
 * keys function should be called instead.
 * 
 * NOTE: This function does not guarantee populating the flags for 
 * the keys, in order to get flags you should use getKeysUsingKeySpecs. */
int getKeysUsingLegacyRangeSpec(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int j, i = 0, last, first, step;
    keyReference *keys;
    UNUSED(argv);

    if (cmd->legacy_range_key_spec.begin_search_type == KSPEC_BS_INVALID) {
        result->numkeys = 0;
        return 0;
    }

    first = cmd->legacy_range_key_spec.bs.index.pos;
    last = cmd->legacy_range_key_spec.fk.range.lastkey;
    if (last >= 0)
        last += first;
    step = cmd->legacy_range_key_spec.fk.range.keystep;

    if (last < 0) last = argc+last;

    int count = ((last - first)+1);
    keys = getKeysPrepareResult(result, count);

    for (j = first; j <= last; j += step) {
        if (j >= argc || j < first) {
            /* Modules commands, and standard commands with a not fixed number
             * of arguments (negative arity parameter) do not have dispatch
             * time arity checks, so we need to handle the case where the user
             * passed an invalid number of arguments here. In this case we
             * return no keys and expect the command implementation to report
             * an arity or syntax error. */
            if (cmd->flags & CMD_MODULE || cmd->arity < 0) {
                result->numkeys = 0;
                return 0;
            } else {
                serverPanic("Redis built-in command declared keys positions not matching the arity requirements.");
            }
        }
        keys[i].pos = j;
        /* Flags are omitted from legacy key specs */
        keys[i++].flags = 0;
    }
    result->numkeys = i;
    return i;
}

/* Return all the arguments that are keys in the command passed via argc / argv.
 *
 * The command returns the positions of all the key arguments inside the array,
 * so the actual return value is a heap allocated array of integers. The
 * length of the array is returned by reference into *numkeys.
 *
 * 'cmd' must be point to the corresponding entry into the redisCommand
 * table, according to the command name in argv[0].
 *
 * This function uses the command table if a command-specific helper function
 * is not required, otherwise it calls the command-specific function. */
int getKeysFromCommand(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    if (cmd->flags & CMD_MODULE_GETKEYS) {
        return moduleGetCommandKeysViaAPI(cmd,argv,argc,result);
    } else if (cmd->getkeys_proc) {
        return cmd->getkeys_proc(cmd,argv,argc,result);
    } else {
        return getKeysUsingLegacyRangeSpec(cmd,argv,argc,result);
    }
}

/* Free the result of getKeysFromCommand. */
void getKeysFreeResult(getKeysResult *result) {
    if (result && result->keys != result->keysbuf)
        zfree(result->keys);
}

/* Helper function to extract keys from following commands:
 * COMMAND [destkey] <num-keys> <key> [...] <key> [...] ... <options>
 *
 * eg:
 * ZUNION <num-keys> <key> <key> ... <key> <options>
 * ZUNIONSTORE <destkey> <num-keys> <key> <key> ... <key> <options>
 *
 * 'storeKeyOfs': destkey index, 0 means destkey not exists.
 * 'keyCountOfs': num-keys index.
 * 'firstKeyOfs': firstkey index.
 * 'keyStep': the interval of each key, usually this value is 1.
 * 
 * The commands using this function have a fully defined keyspec, so returning flags isn't needed. */
int genericGetKeys(int storeKeyOfs, int keyCountOfs, int firstKeyOfs, int keyStep,
                    robj **argv, int argc, getKeysResult *result) {
    int i, num;
    keyReference *keys;

    num = atoi(argv[keyCountOfs]->ptr);
    /* Sanity check. Don't return any key if the command is going to
     * reply with syntax error. (no input keys). */
    if (num < 1 || num > (argc - firstKeyOfs)/keyStep) {
        result->numkeys = 0;
        return 0;
    }

    int numkeys = storeKeyOfs ? num + 1 : num;
    keys = getKeysPrepareResult(result, numkeys);
    result->numkeys = numkeys;

    /* Add all key positions for argv[firstKeyOfs...n] to keys[] */
    for (i = 0; i < num; i++) {
        keys[i].pos = firstKeyOfs+(i*keyStep);
        keys[i].flags = 0;
    } 

    if (storeKeyOfs) {
        keys[num].pos = storeKeyOfs;
        keys[num].flags = 0;
    } 
    return result->numkeys;
}

int sintercardGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 1, 2, 1, argv, argc, result);
}

int zunionInterDiffStoreGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(1, 2, 3, 1, argv, argc, result);
}

int zunionInterDiffGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 1, 2, 1, argv, argc, result);
}

int evalGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 2, 3, 1, argv, argc, result);
}

int functionGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 2, 3, 1, argv, argc, result);
}

int lmpopGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 1, 2, 1, argv, argc, result);
}

int blmpopGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 2, 3, 1, argv, argc, result);
}

int zmpopGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 1, 2, 1, argv, argc, result);
}

int bzmpopGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 2, 3, 1, argv, argc, result);
}

/* Helper function to extract keys from the SORT RO command.
 *
 * SORT <sort-key>
 *
 * The second argument of SORT is always a key, however an arbitrary number of
 * keys may be accessed while doing the sort (the BY and GET args), so the
 * key-spec declares incomplete keys which is why we have to provide a concrete
 * implementation to fetch the keys.
 *
 * This command declares incomplete keys, so the flags are correctly set for this function */
int sortROGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    keyReference *keys;
    UNUSED(cmd);
    UNUSED(argv);
    UNUSED(argc);

    keys = getKeysPrepareResult(result, 1);
    keys[0].pos = 1; /* <sort-key> is always present. */
    keys[0].flags = CMD_KEY_RO | CMD_KEY_ACCESS;
    result->numkeys = 1;
    return result->numkeys;
}

/* Helper function to extract keys from the SORT command.
 *
 * SORT <sort-key> ... STORE <store-key> ...
 *
 * The first argument of SORT is always a key, however a list of options
 * follow in SQL-alike style. Here we parse just the minimum in order to
 * correctly identify keys in the "STORE" option. 
 * 
 * This command declares incomplete keys, so the flags are correctly set for this function */
int sortGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, j, num, found_store = 0;
    keyReference *keys;
    UNUSED(cmd);

    num = 0;
    keys = getKeysPrepareResult(result, 2); /* Alloc 2 places for the worst case. */
    keys[num].pos = 1; /* <sort-key> is always present. */
    keys[num++].flags = CMD_KEY_RO | CMD_KEY_ACCESS;

    /* Search for STORE option. By default we consider options to don't
     * have arguments, so if we find an unknown option name we scan the
     * next. However there are options with 1 or 2 arguments, so we
     * provide a list here in order to skip the right number of args. */
    struct {
        char *name;
        int skip;
    } skiplist[] = {
        {"limit", 2},
        {"get", 1},
        {"by", 1},
        {NULL, 0} /* End of elements. */
    };

    for (i = 2; i < argc; i++) {
        for (j = 0; skiplist[j].name != NULL; j++) {
            if (!strcasecmp(argv[i]->ptr,skiplist[j].name)) {
                i += skiplist[j].skip;
                break;
            } else if (!strcasecmp(argv[i]->ptr,"store") && i+1 < argc) {
                /* Note: we don't increment "num" here and continue the loop
                 * to be sure to process the *last* "STORE" option if multiple
                 * ones are provided. This is same behavior as SORT. */
                found_store = 1;
                keys[num].pos = i+1; /* <store-key> */
                keys[num].flags = CMD_KEY_OW | CMD_KEY_UPDATE;
                break;
            }
        }
    }
    result->numkeys = num + found_store;
    return result->numkeys;
}

/* This command declares incomplete keys, so the flags are correctly set for this function */
int migrateGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, j, num, first;
    keyReference *keys;
    UNUSED(cmd);

    /* Assume the obvious form. */
    first = 3;
    num = 1;

    /* But check for the extended one with the KEYS option. */
    struct {
        char* name;
        int skip;
    } skip_keywords[] = {       
        {"copy", 0},
        {"replace", 0},
        {"auth", 1},
        {"auth2", 2},
        {NULL, 0}
    };
    if (argc > 6) {
        for (i = 6; i < argc; i++) {
            if (!strcasecmp(argv[i]->ptr, "keys")) {
                if (sdslen(argv[3]->ptr) > 0) {
                    /* This is a syntax error. So ignore the keys and leave
                     * the syntax error to be handled by migrateCommand. */
                    num = 0; 
                } else {
                    first = i + 1;
                    num = argc - first;
                }
                break;
            }
            for (j = 0; skip_keywords[j].name != NULL; j++) {
                if (!strcasecmp(argv[i]->ptr, skip_keywords[j].name)) {
                    i += skip_keywords[j].skip;
                    break;
                }
            }
        }
    }

    keys = getKeysPrepareResult(result, num);
    for (i = 0; i < num; i++) {
        keys[i].pos = first+i;
        keys[i].flags = CMD_KEY_RW | CMD_KEY_ACCESS | CMD_KEY_DELETE;
    } 
    result->numkeys = num;
    return num;
}

/* Helper function to extract keys from following commands:
 * GEORADIUS key x y radius unit [WITHDIST] [WITHHASH] [WITHCOORD] [ASC|DESC]
 *                             [COUNT count] [STORE key|STOREDIST key]
 * GEORADIUSBYMEMBER key member radius unit ... options ...
 * 
 * This command has a fully defined keyspec, so returning flags isn't needed. */
int georadiusGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, num;
    keyReference *keys;
    UNUSED(cmd);

    /* Check for the presence of the stored key in the command */
    int stored_key = -1;
    for (i = 5; i < argc; i++) {
        char *arg = argv[i]->ptr;
        /* For the case when user specifies both "store" and "storedist" options, the
         * second key specified would override the first key. This behavior is kept
         * the same as in georadiusCommand method.
         */
        if ((!strcasecmp(arg, "store") || !strcasecmp(arg, "storedist")) && ((i+1) < argc)) {
            stored_key = i+1;
            i++;
        }
    }
    num = 1 + (stored_key == -1 ? 0 : 1);

    /* Keys in the command come from two places:
     * argv[1] = key,
     * argv[5...n] = stored key if present
     */
    keys = getKeysPrepareResult(result, num);

    /* Add all key positions to keys[] */
    keys[0].pos = 1;
    keys[0].flags = 0;
    if(num > 1) {
         keys[1].pos = stored_key;
         keys[1].flags = 0;
    }
    result->numkeys = num;
    return num;
}

/* XREAD [BLOCK <milliseconds>] [COUNT <count>] [GROUP <groupname> <ttl>]
 *       STREAMS key_1 key_2 ... key_N ID_1 ID_2 ... ID_N
 *
 * This command has a fully defined keyspec, so returning flags isn't needed. */
int xreadGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, num = 0;
    keyReference *keys;
    UNUSED(cmd);

    /* We need to parse the options of the command in order to seek the first
     * "STREAMS" string which is actually the option. This is needed because
     * "STREAMS" could also be the name of the consumer group and even the
     * name of the stream key. */
    int streams_pos = -1;
    for (i = 1; i < argc; i++) {
        char *arg = argv[i]->ptr;
        if (!strcasecmp(arg, "block")) {
            i++; /* Skip option argument. */
        } else if (!strcasecmp(arg, "count")) {
            i++; /* Skip option argument. */
        } else if (!strcasecmp(arg, "group")) {
            i += 2; /* Skip option argument. */
        } else if (!strcasecmp(arg, "noack")) {
            /* Nothing to do. */
        } else if (!strcasecmp(arg, "streams")) {
            streams_pos = i;
            break;
        } else {
            break; /* Syntax error. */
        }
    }
    if (streams_pos != -1) num = argc - streams_pos - 1;

    /* Syntax error. */
    if (streams_pos == -1 || num == 0 || num % 2 != 0) {
        result->numkeys = 0;
        return 0;
    }
    num /= 2; /* We have half the keys as there are arguments because
                 there are also the IDs, one per key. */

    keys = getKeysPrepareResult(result, num);
    for (i = streams_pos+1; i < argc-num; i++) {
        keys[i-streams_pos-1].pos = i;
        keys[i-streams_pos-1].flags = 0; 
    } 
    result->numkeys = num;
    return num;
}

/* Helper function to extract keys from the SET command, which may have
 * a read flag if the GET argument is passed in. */
int setGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    keyReference *keys;
    UNUSED(cmd);

    keys = getKeysPrepareResult(result, 1);
    keys[0].pos = 1; /* We always know the position */
    result->numkeys = 1;

    for (int i = 3; i < argc; i++) {
        char *arg = argv[i]->ptr;
        if ((arg[0] == 'g' || arg[0] == 'G') &&
            (arg[1] == 'e' || arg[1] == 'E') &&
            (arg[2] == 't' || arg[2] == 'T') && arg[3] == '\0')
        {
            keys[0].flags = CMD_KEY_RW | CMD_KEY_ACCESS | CMD_KEY_UPDATE;
            return 1;
        }
    }

    keys[0].flags = CMD_KEY_OW | CMD_KEY_UPDATE;
    return 1;
}

/* Helper function to extract keys from the BITFIELD command, which may be
 * read-only if the BITFIELD GET subcommand is used. */
int bitfieldGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    keyReference *keys;
    int readonly = 1;
    UNUSED(cmd);

    keys = getKeysPrepareResult(result, 1);
    keys[0].pos = 1; /* We always know the position */
    result->numkeys = 1;

    for (int i = 2; i < argc; i++) {
        int remargs = argc - i - 1; /* Remaining args other than current. */
        char *arg = argv[i]->ptr;
        if (!strcasecmp(arg, "get") && remargs >= 2) {
            i += 2;
        } else if ((!strcasecmp(arg, "set") || !strcasecmp(arg, "incrby")) && remargs >= 3) {
            readonly = 0;
            i += 3;
            break;
        } else if (!strcasecmp(arg, "overflow") && remargs >= 1) {
            i += 1;
        } else {
            readonly = 0; /* Syntax error. safer to assume non-RO. */
            break;
        }
    }

    if (readonly) {
        keys[0].flags = CMD_KEY_RO | CMD_KEY_ACCESS;
    } else {
        keys[0].flags = CMD_KEY_RW | CMD_KEY_ACCESS | CMD_KEY_UPDATE;
    }
    return 1;
}
