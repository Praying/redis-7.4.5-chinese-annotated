/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

/*
 * cluster.c 包含了集群实现的公共部分,
 * 即在任何 Redis 集群实现之间共享的代码。
 */

#include "server.h"
#include "cluster.h"

#include <ctype.h>

/* -----------------------------------------------------------------------------
 * Key space handling（键空间处理）
 * -------------------------------------------------------------------------- */

/* 如果能推断出给定的 glob 风格 pattern（如 util.c 中 stringmatchlen() 所实现的）
 * 只能匹配属于单个 slot 的键，则返回该 slot；否则返回 -1。*/
int patternHashSlot(char *pattern, int length) {
    int s = -1; /* index of the first '{' */

    for (int i = 0; i < length; i++) {
        if (pattern[i] == '*' || pattern[i] == '?' || pattern[i] == '[') {
            /* Wildcard or character class found. Keys can be in any slot. */
            return -1;
        } else if (pattern[i] == '\\') {
            /* Escaped character. Computing slot in this case is not
             * implemented. We would need a temp buffer. */
            return -1;
        } else if (s == -1 && pattern[i] == '{') {
            /* Opening brace '{' found. */
            s = i;
        } else if (s >= 0 && pattern[i] == '}' && i == s + 1) {
            /* Empty tag '{}' found. The whole key is hashed. Ignore braces. */
            s = -2;
        } else if (s >= 0 && pattern[i] == '}') {
            /* Non-empty tag '{...}' found. Hash what's between braces. */
            return crc16(pattern + s + 1, i - s - 1) & 0x3FFF;
        }
    }

    /* The pattern matches a single key. Hash the whole pattern. */
    return crc16(pattern, length) & 0x3FFF;
}

ConnectionType *connTypeOfCluster(void) {
    if (server.tls_cluster) {
        return connectionTypeTls();
    }

    return connectionTypeTcp();
}

/* -----------------------------------------------------------------------------
 * DUMP, RESTORE and MIGRATE commands（DUMP、RESTORE 与 MIGRATE 命令）
 * -------------------------------------------------------------------------- */

/* 生成对象 'o' 的 DUMP 格式表示，并将其追加到 'rio' 指向的 io 流中。
 * 该函数不会失败。*/
void createDumpPayload(rio *payload, robj *o, robj *key, int dbid) {
    unsigned char buf[2];
    uint64_t crc;

    /* 以类似 RDB 的格式序列化对象。它由一个对象类型字节及随后序列化的对象组成。
     * RESTORE 命令能够识别这种格式。*/
    rioInitWithBuffer(payload,sdsempty());
    serverAssert(rdbSaveObjectType(payload,o));
    serverAssert(rdbSaveObject(payload,o,key,dbid));

    /* 写入尾部，其结构如下：
     * ----------------+---------------------+---------------+
     * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
     * ----------------+---------------------+---------------+
     * RDB version 和 CRC 都是小端字节序。
     */

    /* RDB version */
    buf[0] = RDB_VERSION & 0xff;
    buf[1] = (RDB_VERSION >> 8) & 0xff;
    payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr,buf,2);

    /* CRC64 */
    crc = crc64(0,(unsigned char*)payload->io.buffer.ptr,
                sdslen(payload->io.buffer.ptr));
    memrev64ifbe(&crc);
    payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr,&crc,8);
}

/* 校验 dump payload 的 RDB 版本是否与本 Redis 实例匹配，且校验和是否正确。
 * 如果 DUMP payload 看起来有效则返回 C_OK，否则返回 C_ERR。
 * 如果 rdbver_ptr 不为 NULL，则使用从输入缓冲区中读取到的值填充它。*/
int verifyDumpPayload(unsigned char *p, size_t len, uint16_t *rdbver_ptr) {
    unsigned char *footer;
    uint16_t rdbver;
    uint64_t crc;

    /* 至少应该包含 2 字节的 RDB 版本号和 8 字节的 CRC64。*/
    if (len < 10) return C_ERR;
    footer = p+(len-10);

    /* 设置并校验 RDB 版本号。*/
    rdbver = (footer[1] << 8) | footer[0];
    if (rdbver_ptr) {
        *rdbver_ptr = rdbver;
    }
    if (rdbver > RDB_VERSION) return C_ERR;

    if (server.skip_checksum_validation)
        return C_OK;

    /* 校验 CRC64 */
    crc = crc64(0,p,len-8);
    memrev64ifbe(&crc);
    return (memcmp(&crc,footer+2,8) == 0) ? C_OK : C_ERR;
}

/* DUMP keyname
 * DUMP 实际上并没有被 Redis Cluster 使用，但它显然是 RESTORE 的对偶命令，
 * 对于不同的应用场景可能非常有用。*/
void dumpCommand(client *c) {
    robj *o;
    rio payload;

    /* 检查键是否在这里。*/
    if ((o = lookupKeyRead(c->db,c->argv[1])) == NULL) {
        addReplyNull(c);
        return;
    }

    /* 创建 DUMP 编码表示。*/
    createDumpPayload(&payload,o,c->argv[1],c->db->id);

    /* 发送给客户端 */
    addReplyBulkSds(c,payload.io.buffer.ptr);
    return;
}

/* RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
 * 恢复 key 的序列化的值，可指定 TTL 及各种可选参数。*/
void restoreCommand(client *c) {
    long long ttl, lfu_freq = -1, lru_idle = -1, lru_clock = -1;
    rio payload;
    int j, type, replace = 0, absttl = 0;
    robj *obj;

    /* 解析额外的选项 */
    for (j = 4; j < c->argc; j++) {
        int additional = c->argc-j-1;
        if (!strcasecmp(c->argv[j]->ptr,"replace")) {
            replace = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"absttl")) {
            absttl = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"idletime") && additional >= 1 &&
                   lfu_freq == -1)
        {
            if (getLongLongFromObjectOrReply(c,c->argv[j+1],&lru_idle,NULL)
                != C_OK) return;
            if (lru_idle < 0) {
                addReplyError(c,"Invalid IDLETIME value, must be >= 0");
                return;
            }
            lru_clock = LRU_CLOCK();
            j++; /* 消费额外的参数。*/
        } else if (!strcasecmp(c->argv[j]->ptr,"freq") && additional >= 1 &&
                   lru_idle == -1)
        {
            if (getLongLongFromObjectOrReply(c,c->argv[j+1],&lfu_freq,NULL)
                != C_OK) return;
            if (lfu_freq < 0 || lfu_freq > 255) {
                addReplyError(c,"Invalid FREQ value, must be >= 0 and <= 255");
                return;
            }
            j++; /* 消费额外的参数。*/
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* 确保该 key 在这里不存在…… */
    robj *key = c->argv[1];
    if (!replace && lookupKeyWrite(c->db,key) != NULL) {
        addReplyErrorObject(c,shared.busykeyerr);
        return;
    }

    /* 检查 TTL 值是否合理 */
    if (getLongLongFromObjectOrReply(c,c->argv[2],&ttl,NULL) != C_OK) {
        return;
    } else if (ttl < 0) {
        addReplyError(c,"Invalid TTL value, must be >= 0");
        return;
    }

    /* 校验 RDB 版本与数据校验和。*/
    if (verifyDumpPayload(c->argv[3]->ptr,sdslen(c->argv[3]->ptr),NULL) == C_ERR)
    {
        addReplyError(c,"DUMP payload version or checksum are wrong");
        return;
    }

    rioInitWithBuffer(&payload,c->argv[3]->ptr);
    if (((type = rdbLoadObjectType(&payload)) == -1) ||
        ((obj = rdbLoadObject(type,&payload,key->ptr,c->db->id,NULL)) == NULL))
    {
        addReplyError(c,"Bad data format");
        return;
    }

    /* 如果需要，移除旧的 key。*/
    int deleted = 0;
    if (replace)
        deleted = dbDelete(c->db,key);

    if (ttl && !absttl) ttl+=commandTimeSnapshot();
    if (ttl && checkAlreadyExpired(ttl)) {
        if (deleted) {
            robj *aux = server.lazyfree_lazy_server_del ? shared.unlink : shared.del;
            rewriteClientCommandVector(c, 2, aux, key);
            signalModifiedKey(c,c->db,key);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
            server.dirty++;
        }
        decrRefCount(obj);
        addReply(c, shared.ok);
        return;
    }

    /* 创建 key 并设置 TTL（如果有）*/
    dictEntry *de = dbAdd(c->db,key,obj);

    /* 如果 minExpiredField 已被设置，则该对象是一个字段带过期时间的哈希，
     * 需要将其注册到全局的 HFE 数据结构中 */
    if (obj->type == OBJ_HASH) {
        uint64_t minExpiredField = hashTypeGetMinExpire(obj, 1);
        if (minExpiredField != EB_EXPIRE_TIME_INVALID)
            hashTypeAddToExpires(c->db, dictGetKey(de), obj, minExpiredField);
    }

    if (ttl) {
        setExpire(c,c->db,key,ttl);
        if (!absttl) {
            /* 将 TTL 作为绝对时间戳进行传播 */
            robj *ttl_obj = createStringObjectFromLongLong(ttl);
            rewriteClientCommandArgument(c,2,ttl_obj);
            decrRefCount(ttl_obj);
            rewriteClientCommandArgument(c,c->argc,shared.absttl);
        }
    }
    objectSetLRUOrLFU(obj,lfu_freq,lru_idle,lru_clock,1000);
    signalModifiedKey(c,c->db,key);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"restore",key,c->db->id);
    addReply(c,shared.ok);
    server.dirty++;
}
/* MIGRATE socket cache implementation（MIGRATE 套接字缓存实现）。
 *
 * 我们维护一个 host:ip 到最近用于连接该实例的 TCP 套接字的映射。
 * 当缓存的套接字数量达到上限时会关闭部分套接字；
 * 同时 serverCron() 也会清理那些闲置超过数秒的缓存套接字。*/
#define MIGRATE_SOCKET_CACHE_ITEMS 64 /* max num of items in the cache.（缓存中的最大条目数）*/
#define MIGRATE_SOCKET_CACHE_TTL 10 /* close cached sockets after 10 sec.（缓存套接字在 10 秒后关闭）*/

typedef struct migrateCachedSocket {
    connection *conn;
    long last_dbid;
    time_t last_use_time;
} migrateCachedSocket;

/* 返回一个包含连接到目标实例的 TCP 套接字的 migrateCachedSocket，
 * 可能直接返回一个缓存的套接字。
 *
 * 如果无法建立连接，本函数负责向客户端发送错误信息，这种情况下返回 -1。
 * 否则在成功时返回该套接字，调用者使用后不应尝试释放它。
 *
 * 如果调用者在使用套接字时检测到错误，应调用 migrateCloseSocket()，
 * 这样下次就会重新创建连接。*/
migrateCachedSocket* migrateGetSocket(client *c, robj *host, robj *port, long timeout) {
    connection *conn;
    sds name = sdsempty();
    migrateCachedSocket *cs;

    /* 检查该 ip:port 对是否已有缓存的套接字。*/
    name = sdscatlen(name,host->ptr,sdslen(host->ptr));
    name = sdscatlen(name,":",1);
    name = sdscatlen(name,port->ptr,sdslen(port->ptr));
    cs = dictFetchValue(server.migrate_cached_sockets,name);
    if (cs) {
        sdsfree(name);
        cs->last_use_time = server.unixtime;
        return cs;
    }

    /* 没有缓存的套接字，创建一个新的。*/
    if (dictSize(server.migrate_cached_sockets) == MIGRATE_SOCKET_CACHE_ITEMS) {
        /* 条目过多，随机丢弃一个。*/
        dictEntry *de = dictGetRandomKey(server.migrate_cached_sockets);
        cs = dictGetVal(de);
        connClose(cs->conn);
        zfree(cs);
        dictDelete(server.migrate_cached_sockets,dictGetKey(de));
    }

    /* 创建连接 */
    conn = connCreate(connTypeOfCluster());
    if (connBlockingConnect(conn, host->ptr, atoi(port->ptr), timeout)
        != C_OK) {
        addReplyError(c,"-IOERR error or timeout connecting to the client");
        connClose(conn);
        sdsfree(name);
        return NULL;
    }
    connEnableTcpNoDelay(conn);

    /* 添加到缓存中并返回给调用者。*/
    cs = zmalloc(sizeof(*cs));
    cs->conn = conn;

    cs->last_dbid = -1;
    cs->last_use_time = server.unixtime;
    dictAdd(server.migrate_cached_sockets,name,cs);
    return cs;
}

/* 释放一个 MIGRATE 缓存的连接。*/
void migrateCloseSocket(robj *host, robj *port) {
    sds name = sdsempty();
    migrateCachedSocket *cs;

    name = sdscatlen(name,host->ptr,sdslen(host->ptr));
    name = sdscatlen(name,":",1);
    name = sdscatlen(name,port->ptr,sdslen(port->ptr));
    cs = dictFetchValue(server.migrate_cached_sockets,name);
    if (!cs) {
        sdsfree(name);
        return;
    }

    connClose(cs->conn);
    zfree(cs);
    dictDelete(server.migrate_cached_sockets,name);
    sdsfree(name);
}

void migrateCloseTimedoutSockets(void) {
    dictIterator *di = dictGetSafeIterator(server.migrate_cached_sockets);
    dictEntry *de;

    while((de = dictNext(di)) != NULL) {
        migrateCachedSocket *cs = dictGetVal(de);

        if ((server.unixtime - cs->last_use_time) > MIGRATE_SOCKET_CACHE_TTL) {
            connClose(cs->conn);
            zfree(cs);
            dictDelete(server.migrate_cached_sockets,dictGetKey(de));
        }
    }
    dictReleaseIterator(di);
}

/* MIGRATE host port key dbid timeout [COPY | REPLACE | AUTH password |
 *         AUTH2 username password]
 * 将 key 迁移到指定的 host:port 上。
 *
 * 多 key 形式的语法：
 *
 * MIGRATE host port "" dbid timeout [COPY | REPLACE | AUTH password |
 *         AUTH2 username password] KEYS key1 key2 ... keyN */
void migrateCommand(client *c) {
    migrateCachedSocket *cs;
    int copy = 0, replace = 0, j;
    char *username = NULL;
    char *password = NULL;
    long timeout;
    long dbid;
    robj **ov = NULL; /* Objects to migrate. */
    robj **kv = NULL; /* Key names. */
    robj **newargv = NULL; /* Used to rewrite the command as DEL ... keys ... */
    rio cmd, payload;
    int may_retry = 1;
    int write_error = 0;
    int argv_rewritten = 0;

    /* 为了支持 KEYS 选项，需要使用以下额外的状态变量。*/
    int first_key = 3; /* Argument index of the first key. */
    int num_keys = 1;  /* By default only migrate the 'key' argument. */

    /* 解析额外的选项 */
    for (j = 6; j < c->argc; j++) {
        int moreargs = (c->argc-1) - j;
        if (!strcasecmp(c->argv[j]->ptr,"copy")) {
            copy = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"replace")) {
            replace = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"auth")) {
            if (!moreargs) {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
            j++;
            password = c->argv[j]->ptr;
            redactClientCommandArgument(c,j);
        } else if (!strcasecmp(c->argv[j]->ptr,"auth2")) {
            if (moreargs < 2) {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
            username = c->argv[++j]->ptr;
            redactClientCommandArgument(c,j);
            password = c->argv[++j]->ptr;
            redactClientCommandArgument(c,j);
        } else if (!strcasecmp(c->argv[j]->ptr,"keys")) {
            if (sdslen(c->argv[3]->ptr) != 0) {
                addReplyError(c,
                              "When using MIGRATE KEYS option, the key argument"
                              " must be set to the empty string");
                return;
            }
            first_key = j+1;
            num_keys = c->argc - j - 1;
            break; /* All the remaining args are keys. */
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* 基本健全性检查 */
    if (getLongFromObjectOrReply(c,c->argv[5],&timeout,NULL) != C_OK ||
        getLongFromObjectOrReply(c,c->argv[4],&dbid,NULL) != C_OK)
    {
        return;
    }
    if (timeout <= 0) timeout = 1000;

    /* 检查 key 是否存在。如果至少存在一个待迁移的 key 就执行迁移；
     * 如果所有 key 都缺失，则回复 "NOKEY" 以告知调用者没有可迁移的数据。
     * 这种情况我们不会返回错误，因为它通常属于正常情形，
     * 比如 key 刚刚过期。*/
    ov = zrealloc(ov,sizeof(robj*)*num_keys);
    kv = zrealloc(kv,sizeof(robj*)*num_keys);
    int oi = 0;

    for (j = 0; j < num_keys; j++) {
        if ((ov[oi] = lookupKeyRead(c->db,c->argv[first_key+j])) != NULL) {
            kv[oi] = c->argv[first_key+j];
            oi++;
        }
    }
    num_keys = oi;
    if (num_keys == 0) {
        zfree(ov); zfree(kv);
        addReplySds(c,sdsnew("+NOKEY\r\n"));
        return;
    }

    try_again:
    write_error = 0;

    /* Connect（建立连接）*/
    cs = migrateGetSocket(c,c->argv[1],c->argv[2],timeout);
    if (cs == NULL) {
        zfree(ov); zfree(kv);
        return; /* error sent to the client by migrateGetSocket()
                   （错误已由 migrateGetSocket() 发送给客户端）*/
    }

    rioInitWithBuffer(&cmd,sdsempty());

    /* Authentication（认证）*/
    if (password) {
        int arity = username ? 3 : 2;
        serverAssertWithInfo(c,NULL,rioWriteBulkCount(&cmd,'*',arity));
        serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"AUTH",4));
        if (username) {
            serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,username,
                                                           sdslen(username)));
        }
        serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,password,
                                                       sdslen(password)));
    }

    /* 如果目标数据库不是当前已选中的数据库，则发送 SELECT 命令。*/
    int select = cs->last_dbid != dbid; /* Should we emit SELECT? */
    if (select) {
        serverAssertWithInfo(c,NULL,rioWriteBulkCount(&cmd,'*',2));
        serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"SELECT",6));
        serverAssertWithInfo(c,NULL,rioWriteBulkLongLong(&cmd,dbid));
    }

    int non_expired = 0; /* Number of keys that we'll find non expired.
                            Note that serializing large keys may take some time
                            so certain keys that were found non expired by the
                            lookupKey() function, may be expired later. */

    /* 创建 RESTORE payload 并生成调用命令所需的协议。*/
    for (j = 0; j < num_keys; j++) {
        long long ttl = 0;
        long long expireat = getExpire(c->db,kv[j]);

        if (expireat != -1) {
            ttl = expireat-commandTimeSnapshot();
            if (ttl < 0) {
                continue;
            }
            if (ttl < 1) ttl = 1;
        }

        /* 将合法（未过期）的键和值重新放置到数组中相邻的位置，
         * 以便清除那些在第一次查找中存在但第二次查找时已过期的键所留下的空洞。*/
        ov[non_expired] = ov[j];
        kv[non_expired++] = kv[j];

        serverAssertWithInfo(c,NULL,
                             rioWriteBulkCount(&cmd,'*',replace ? 5 : 4));

        if (server.cluster_enabled)
            serverAssertWithInfo(c,NULL,
                                 rioWriteBulkString(&cmd,"RESTORE-ASKING",14));
        else
            serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"RESTORE",7));
        serverAssertWithInfo(c,NULL,sdsEncodedObject(kv[j]));
        serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,kv[j]->ptr,
                                                       sdslen(kv[j]->ptr)));
        serverAssertWithInfo(c,NULL,rioWriteBulkLongLong(&cmd,ttl));

        /* 输出 payload 参数，即使用 DUMP 格式序列化的对象。*/
        createDumpPayload(&payload,ov[j],kv[j],dbid);
        serverAssertWithInfo(c,NULL,
                             rioWriteBulkString(&cmd,payload.io.buffer.ptr,
                                                sdslen(payload.io.buffer.ptr)));
        sdsfree(payload.io.buffer.ptr);

        /* 如果 REPLACE 被指定为 MIGRATE 的选项，则将其添加到 RESTORE 命令中。*/
        if (replace)
            serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"REPLACE",7));
    }

    /* 修正实际迁移的 key 数量。*/
    num_keys = non_expired;

    /* 以 64K 大小的块将查询转发到另一个节点。*/
    errno = 0;
    {
        sds buf = cmd.io.buffer.ptr;
        size_t pos = 0, towrite;
        int nwritten = 0;

        while ((towrite = sdslen(buf)-pos) > 0) {
            towrite = (towrite > (64*1024) ? (64*1024) : towrite);
            nwritten = connSyncWrite(cs->conn,buf+pos,towrite,timeout);
            if (nwritten != (signed)towrite) {
                write_error = 1;
                goto socket_err;
            }
            pos += nwritten;
        }
    }

    char buf0[1024]; /* Auth reply.（认证回复）*/
    char buf1[1024]; /* Select reply.（选择数据库回复）*/
    char buf2[1024]; /* Restore reply.（RESTORE 回复）*/

    /* Read the AUTH reply if needed.（如有必要，读取 AUTH 命令的回复）*/
    if (password && connSyncReadLine(cs->conn, buf0, sizeof(buf0), timeout) <= 0)
        goto socket_err;

    /* Read the SELECT reply if needed.（如有必要，读取 SELECT 命令的回复）*/
    if (select && connSyncReadLine(cs->conn, buf1, sizeof(buf1), timeout) <= 0)
        goto socket_err;

    /* Read the RESTORE replies.（读取 RESTORE 命令的回复）*/
    int error_from_target = 0;
    int socket_error = 0;
    int del_idx = 1; /* Index of the key argument for the replicated DEL op. */

    /* 分配新的参数向量，用于替换当前的命令，
     * 以便在没有给定 COPY 选项的情况下将 MIGRATE 作为 DEL 命令进行传播。
     * 我们分配 num_keys+1 个空间，因为额外的那个参数是 "DEL" 命令名本身。*/
    if (!copy) newargv = zmalloc(sizeof(robj*)*(num_keys+1));

    for (j = 0; j < num_keys; j++) {
        if (connSyncReadLine(cs->conn, buf2, sizeof(buf2), timeout) <= 0) {
            socket_error = 1;
            break;
        }
        if ((password && buf0[0] == '-') ||
            (select && buf1[0] == '-') ||
            buf2[0] == '-')
        {
            /* 出错时假定 last_dbid 已不再有效。*/
            if (!error_from_target) {
                cs->last_dbid = -1;
                char *errbuf;
                if (password && buf0[0] == '-') errbuf = buf0;
                else if (select && buf1[0] == '-') errbuf = buf1;
                else errbuf = buf2;

                error_from_target = 1;
                addReplyErrorFormat(c,"Target instance replied with error: %s",
                                    errbuf+1);
            }
        } else {
            if (!copy) {
                /* 没有指定 COPY 选项：删除本地 key，并通知变化。*/
                dbDelete(c->db,kv[j]);
                signalModifiedKey(c,c->db,kv[j]);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",kv[j],c->db->id);
                server.dirty++;

                /* Populate the argument vector to replace the old one.
                 * （填充参数向量以替换旧的参数向量）*/
                newargv[del_idx++] = kv[j];
                incrRefCount(kv[j]);
            }
        }
    }

    /* 出现套接字错误时，如果希望重试，则在重写命令向量之前立即重试。
     * 我们仅在确认没有任何数据被处理过，且读取第一个回复就失败
     * （j == 0 测试）时才进行重试。*/
    if (!error_from_target && socket_error && j == 0 && may_retry &&
        errno != ETIMEDOUT)
    {
        goto socket_err; /* A retry is guaranteed because of tested conditions.*/
    }

    /* 在出现套接字错误时，关闭迁移套接字，因为我们仍然保留着
     * ARGV 中原始的 host/port。之后原始命令可能会被改写为 DEL，
     * 那时再来关闭就会太晚。*/
    if (socket_error) migrateCloseSocket(c->argv[1],c->argv[2]);

    if (!copy) {
        /* 将 MIGRATE 翻译为 DEL 用于复制/AOF。注意，我们仅针对那些
         * 已从接收端 Redis 服务器收到确认的 key 执行此操作，
         * 索引使用 del_idx。*/
        if (del_idx > 1) {
            newargv[0] = createStringObject("DEL",3);
            /* Note that the following call takes ownership of newargv.
             * （注意，下面的调用会接管 newargv 的所有权）*/
            replaceClientCommandVector(c,del_idx,newargv);
            argv_rewritten = 1;
        } else {
            /* No key transfer acknowledged, no need to rewrite as DEL.
             * （没有已确认传输的 key，无需改写为 DEL）*/
            zfree(newargv);
        }
        newargv = NULL; /* Make it safe to call zfree() on it in the future. */
    }

    /* If we are here and a socket error happened, we don't want to retry.
     * Just signal the problem to the client, but only do it if we did not
     * already queue a different error reported by the destination server. */
    if (!error_from_target && socket_error) {
        may_retry = 0;
        goto socket_err;
    }

    if (!error_from_target) {
        /* Success! Update the last_dbid in migrateCachedSocket, so that we can
         * avoid SELECT the next time if the target DB is the same. Reply +OK.
         *
         * Note: If we reached this point, even if socket_error is true
         * still the SELECT command succeeded (otherwise the code jumps to
         * socket_err label. */
        cs->last_dbid = dbid;
        addReply(c,shared.ok);
    } else {
        /* On error we already sent it in the for loop above, and set
         * the currently selected socket to -1 to force SELECT the next time. */
    }

    sdsfree(cmd.io.buffer.ptr);
    zfree(ov); zfree(kv); zfree(newargv);
    return;

/* On socket errors we try to close the cached socket and try again.
 * It is very common for the cached socket to get closed, if just reopening
 * it works it's a shame to notify the error to the caller.
 * （出现套接字错误时，我们尝试关闭缓存的套接字并重新尝试。
 *  缓存套接字被关闭是常见情况，如果只是重新打开就能恢复，
 *  那么向调用者通知错误就显得没有必要。）*/
    socket_err:
    /* Cleanup we want to perform in both the retry and no retry case.
     * Note: Closing the migrate socket will also force SELECT next time.
     * （在重试和不重试两种情况下都需要执行的清理工作。
     *  注意：关闭迁移套接字也会强制在下一次重新 SELECT。）*/
    sdsfree(cmd.io.buffer.ptr);

    /* If the command was rewritten as DEL and there was a socket error,
     * we already closed the socket earlier. While migrateCloseSocket()
     * is idempotent, the host/port arguments are now gone, so don't do it
     * again.
     * （如果命令已经被改写为 DEL 并且发生了套接字错误，
     *  我们之前已经关闭过套接字。虽然 migrateCloseSocket() 是幂等的，
     *  但 host/port 参数现在已经消失了，所以不要再次关闭。）*/
    if (!argv_rewritten) migrateCloseSocket(c->argv[1],c->argv[2]);
    zfree(newargv);
    newargv = NULL; /* This will get reallocated on retry.
                      （该参数向量会在重试时重新分配）*/

    /* Retry only if it's not a timeout and we never attempted a retry
     * (or the code jumping here did not set may_retry to zero).
     * （仅在不是超时错误且之前未尝试过重试时才进行重试，
     *  或者跳转到这里时未将 may_retry 设为 0。）*/
    if (errno != ETIMEDOUT && may_retry) {
        may_retry = 0;
        goto try_again;
    }

    /* Cleanup we want to do if no retry is attempted.
     * （如果不打算重试，则执行此处的清理工作）*/
    zfree(ov); zfree(kv);
    addReplyErrorSds(c, sdscatprintf(sdsempty(),
                                     "-IOERR error or timeout %s to target instance",
                                     write_error ? "writing" : "reading"));
    return;
}

/* Cluster node sanity check. Returns C_OK if the node id
 * is valid an C_ERR otherwise.
 * （集群节点 ID 的健全性检查。如果 node id 合法则返回 C_OK，否则返回 C_ERR。）*/
int verifyClusterNodeId(const char *name, int length) {
    if (length != CLUSTER_NAMELEN) return C_ERR;
    for (int i = 0; i < length; i++) {
        if (name[i] >= 'a' && name[i] <= 'z') continue;
        if (name[i] >= '0' && name[i] <= '9') continue;
        return C_ERR;
    }
    return C_OK;
}

int isValidAuxChar(int c) {
    return isalnum(c) || (strchr("!#$%&()*+:;<>?@[]^{|}~", c) == NULL);
}

int isValidAuxString(char *s, unsigned int length) {
    for (unsigned i = 0; i < length; i++) {
        if (!isValidAuxChar(s[i])) return 0;
    }
    return 1;
}

void clusterCommandMyId(client *c) {
    char *name = clusterNodeGetName(getMyClusterNode());
    if (name) {
        addReplyBulkCBuffer(c,name, CLUSTER_NAMELEN);
    } else {
        addReplyError(c, "No ID yet");
    }
}

char* getMyClusterId(void) {
    return clusterNodeGetName(getMyClusterNode());
}

void clusterCommandMyShardId(client *c) {
    char *sid = clusterNodeGetShardId(getMyClusterNode());
    if (sid) {
        addReplyBulkCBuffer(c,sid, CLUSTER_NAMELEN);
    } else {
        addReplyError(c, "No shard ID yet");
    }
}

/* When a cluster command is called, we need to decide whether to return TLS info or
 * non-TLS info by the client's connection type. However if the command is called by
 * a Lua script or RM_call, there is no connection in the fake client, so we use
 * server.current_client here to get the real client if available. And if it is not
 * available (modules may call commands without a real client), we return the default
 * info, which is determined by server.tls_cluster.
 * （当一个集群命令被调用时，我们需要根据客户端的连接类型决定是返回 TLS 信息还是
 *  非 TLS 信息。但是，如果该命令是通过 Lua 脚本或 RM_call 调用的，
 *  fake client 中并没有连接，因此这里使用 server.current_client 字段来获取真实
 *  的客户端（如果可用的话）。如果仍然不可用（比如模块可能在没有真实客户端的
 *  情况下调用命令），则返回由 server.tls_cluster 决定的默认信息。）*/
static int shouldReturnTlsInfo(void) {
    if (server.current_client && server.current_client->conn) {
        return connIsTLS(server.current_client->conn);
    } else {
        return server.tls_cluster;
    }
}

unsigned int countKeysInSlot(unsigned int slot) {
    return kvstoreDictSize(server.db->keys, slot);
}

void clusterCommandHelp(client *c) {
    const char *help[] = {
            "COUNTKEYSINSLOT <slot>",
            "    Return the number of keys in <slot>.",
            "GETKEYSINSLOT <slot> <count>",
            "    Return key names stored by current node in a slot.",
            "INFO",
            "    Return information about the cluster.",
            "KEYSLOT <key>",
            "    Return the hash slot for <key>.",
            "MYID",
            "    Return the node id.",
            "MYSHARDID",
            "    Return the node's shard id.",
            "NODES",
            "    Return cluster configuration seen by node. Output format:",
            "    <id> <ip:port@bus-port[,hostname]> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
            "REPLICAS <node-id>",
            "    Return <node-id> replicas.",
            "SLOTS",
            "    Return information about slots range mappings. Each range is made of:",
            "    start, end, master and replicas IP addresses, ports and ids",
            "SHARDS",
            "    Return information about slot range mappings and the nodes associated with them.",
            NULL
    };

    addExtendedReplyHelp(c, help, clusterCommandExtendedHelp());
}

void clusterCommand(client *c) {
    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        clusterCommandHelp(c);
    } else  if (!strcasecmp(c->argv[1]->ptr,"nodes") && c->argc == 2) {
        /* CLUSTER NODES */
        /* Report TLS ports to TLS client, and report non-TLS port to non-TLS client.
         * （向 TLS 客户端返回 TLS 端口，向非 TLS 客户端返回非 TLS 端口。）*/
        sds nodes = clusterGenNodesDescription(c, 0, shouldReturnTlsInfo());
        addReplyVerbatim(c,nodes,sdslen(nodes),"txt");
        sdsfree(nodes);
    } else if (!strcasecmp(c->argv[1]->ptr,"myid") && c->argc == 2) {
        /* CLUSTER MYID */
        clusterCommandMyId(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"myshardid") && c->argc == 2) {
        /* CLUSTER MYSHARDID */
        clusterCommandMyShardId(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"slots") && c->argc == 2) {
        /* CLUSTER SLOTS */
        clusterCommandSlots(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"shards") && c->argc == 2) {
        /* CLUSTER SHARDS */
        clusterCommandShards(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"info") && c->argc == 2) {
        /* CLUSTER INFO */

        sds info = genClusterInfoString();

        /* Produce the reply protocol.（生成回复协议）*/
        addReplyVerbatim(c,info,sdslen(info),"txt");
        sdsfree(info);
    } else if (!strcasecmp(c->argv[1]->ptr,"keyslot") && c->argc == 3) {
        /* CLUSTER KEYSLOT <key> */
        sds key = c->argv[2]->ptr;

        addReplyLongLong(c,keyHashSlot(key,sdslen(key)));
    } else if (!strcasecmp(c->argv[1]->ptr,"countkeysinslot") && c->argc == 3) {
        /* CLUSTER COUNTKEYSINSLOT <slot> */
        long long slot;

        if (getLongLongFromObjectOrReply(c,c->argv[2],&slot,NULL) != C_OK)
            return;
        if (slot < 0 || slot >= CLUSTER_SLOTS) {
            addReplyError(c,"Invalid slot");
            return;
        }
        addReplyLongLong(c,countKeysInSlot(slot));
    } else if (!strcasecmp(c->argv[1]->ptr,"getkeysinslot") && c->argc == 4) {
        /* CLUSTER GETKEYSINSLOT <slot> <count> */
        long long maxkeys, slot;

        if (getLongLongFromObjectOrReply(c,c->argv[2],&slot,NULL) != C_OK)
            return;
        if (getLongLongFromObjectOrReply(c,c->argv[3],&maxkeys,NULL)
            != C_OK)
            return;
        if (slot < 0 || slot >= CLUSTER_SLOTS || maxkeys < 0) {
            addReplyError(c,"Invalid slot or number of keys");
            return;
        }

        unsigned int keys_in_slot = countKeysInSlot(slot);
        unsigned int numkeys = maxkeys > keys_in_slot ? keys_in_slot : maxkeys;
        addReplyArrayLen(c,numkeys);
        kvstoreDictIterator *kvs_di = NULL;
        dictEntry *de = NULL;
        kvs_di = kvstoreGetDictIterator(server.db->keys, slot);
        for (unsigned int i = 0; i < numkeys; i++) {
            de = kvstoreDictIteratorNext(kvs_di);
            serverAssert(de != NULL);
            sds sdskey = dictGetKey(de);
            addReplyBulkCBuffer(c, sdskey, sdslen(sdskey));
        }
        kvstoreReleaseDictIterator(kvs_di);
    } else if ((!strcasecmp(c->argv[1]->ptr,"slaves") ||
                !strcasecmp(c->argv[1]->ptr,"replicas")) && c->argc == 3) {
        /* CLUSTER SLAVES <NODE ID> */
        /* CLUSTER REPLICAS <NODE ID> */
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr, sdslen(c->argv[2]->ptr));
        int j;

        /* Lookup the specified node in our table. */
        if (!n) {
            addReplyErrorFormat(c,"Unknown node %s", (char*)c->argv[2]->ptr);
            return;
        }

        if (clusterNodeIsSlave(n)) {
            addReplyError(c,"The specified node is not a master");
            return;
        }

        /* Report TLS ports to TLS client, and report non-TLS port to non-TLS client.
         * （向 TLS 客户端返回 TLS 端口，向非 TLS 客户端返回非 TLS 端口）*/
        addReplyArrayLen(c, clusterNodeNumSlaves(n));
        for (j = 0; j < clusterNodeNumSlaves(n); j++) {
            sds ni = clusterGenNodeDescription(c, clusterNodeGetSlave(n, j), shouldReturnTlsInfo());
            addReplyBulkCString(c,ni);
            sdsfree(ni);
        }
    } else if(!clusterCommandSpecial(c)) {
        addReplySubcommandSyntaxError(c);
        return;
    }
}

/* Return the pointer to the cluster node that is able to serve the command.
 * For the function to succeed the command should only target either:
 *
 * 1) A single key (even multiple times like RPOPLPUSH mylist mylist).
 * 2) Multiple keys in the same hash slot, while the slot is stable (no
 *    resharding in progress).
 *
 * On success the function returns the node that is able to serve the request.
 * If the node is not 'myself' a redirection must be performed. The kind of
 * redirection is specified setting the integer passed by reference
 * 'error_code', which will be set to CLUSTER_REDIR_ASK or
 * CLUSTER_REDIR_MOVED.
 *
 * When the node is 'myself' 'error_code' is set to CLUSTER_REDIR_NONE.
 *
 * If the command fails NULL is returned, and the reason of the failure is
 * provided via 'error_code', which will be set to:
 *
 * CLUSTER_REDIR_CROSS_SLOT if the request contains multiple keys that
 * don't belong to the same hash slot.
 *
 * CLUSTER_REDIR_UNSTABLE if the request contains multiple keys
 * belonging to the same slot, but the slot is not stable (in migration or
 * importing state, likely because a resharding is in progress).
 *
 * CLUSTER_REDIR_DOWN_UNBOUND if the request addresses a slot which is
 * not bound to any node. In this case the cluster global state should be
 * already "down" but it is fragile to rely on the update of the global state,
 * so we also handle it here.
 *
 * CLUSTER_REDIR_DOWN_STATE and CLUSTER_REDIR_DOWN_RO_STATE if the cluster is
 * down but the user attempts to execute a command that addresses one or more keys.
 *
 * 返回能够处理该命令的集群节点指针。要使该函数成功，命令必须只针对以下两种情况之一：
 *
 *  1) 单一 key（即使是多次出现，例如 RPOPLPUSH mylist mylist）。
 *  2) 处于同一 hash slot 中的多个 key，且该 slot 处于稳定状态（没有正在进行的
 *     重新分片）。
 *
 *  成功时，该函数返回能够处理请求的节点。如果该节点不是 'myself'，则必须执行重定向。
 *  重定向的类型由通过引用传入的整型参数 'error_code' 指定，可被设置为
 *  CLUSTER_REDIR_ASK 或 CLUSTER_REDIR_MOVED。
 *
 *  当节点为 'myself' 时，'error_code' 被设置为 CLUSTER_REDIR_NONE。
 *
 *  如果命令执行失败，则返回 NULL，并通过 'error_code' 给出失败原因，可能的取值包括：
 *
 *  CLUSTER_REDIR_CROSS_SLOT —— 请求包含多个不属于同一 hash slot 的 key。
 *  CLUSTER_REDIR_UNSTABLE —— 请求包含属于同一 slot 的多个 key，
 *                              但该 slot 不稳定（处于迁移或导入状态，
 *                              通常是因为正在进行重新分片）。
 *  CLUSTER_REDIR_DOWN_UNBOUND —— 请求访问的 slot 没有绑定到任何节点。
 *                              这种情况下集群的全局状态应该是 "down"，
 *                              但依赖全局状态的更新并不稳妥，所以这里也直接处理。
 *  CLUSTER_REDIR_DOWN_STATE 和 CLUSTER_REDIR_DOWN_RO_STATE —— 集群处于 down 状态，
 *                              但用户尝试执行一个访问一个或多个 key 的命令。*/
clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, uint64_t cmd_flags, int *error_code) {
    clusterNode *myself = getMyClusterNode();
    clusterNode *n = NULL;
    robj *firstkey = NULL;
    int multiple_keys = 0;
    multiState *ms, _ms;
    multiCmd mc;
    int i, slot = 0, migrating_slot = 0, importing_slot = 0, missing_keys = 0,
            existing_keys = 0;
    int pubsubshard_included = 0; /* Flag to indicate if a pubsub shard cmd is included. */

    /* Allow any key to be set if a module disabled cluster redirections.
     * （如果模块禁用了集群重定向，则允许设置任何 key。）*/
    if (server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)
        return myself;

    /* Set error code optimistically for the base case.
     * （为基本用例乐观地设置 error_code）*/
    if (error_code) *error_code = CLUSTER_REDIR_NONE;

    /* Modules can turn off Redis Cluster redirection: this is useful
     * when writing a module that implements a completely different
     * distributed system.
     * （模块可以关闭 Redis Cluster 重定向：当编写实现完全不同的分布式系统的模块时，
     *  这个功能很有用。）*/

    /* We handle all the cases as if they were EXEC commands, so we have
     * a common code path for everything
     * （我们将所有情况都视为 EXEC 命令，这样我们可以为所有情况提供统一的代码路径）*/
    if (cmd->proc == execCommand) {
        /* If CLIENT_MULTI flag is not set EXEC is just going to return an
         * error.
         * （如果未设置 CLIENT_MULTI 标志，EXEC 只会返回一个错误。）*/
        if (!(c->flags & CLIENT_MULTI)) return myself;
        ms = &c->mstate;
    } else {
        /* In order to have a single codepath create a fake Multi State
         * structure if the client is not in MULTI/EXEC state, this way
         * we have a single codepath below.
         * （为了保持单一代码路径，如果客户端不在 MULTI/EXEC 状态，
         *  我们创建一个伪的 Multi State 结构，从而保证下方代码路径一致。）*/
        ms = &_ms;
        _ms.commands = &mc;
        _ms.count = 1;
        mc.argv = argv;
        mc.argc = argc;
        mc.cmd = cmd;
    }

    /* Check that all the keys are in the same hash slot, and obtain this
     * slot and the node associated.
     * （检查所有 key 是否位于同一 hash slot，并获取该 slot 以及对应的节点。）*/
    for (i = 0; i < ms->count; i++) {
        struct redisCommand *mcmd;
        robj **margv;
        int margc, numkeys, j;
        keyReference *keyindex;

        mcmd = ms->commands[i].cmd;
        margc = ms->commands[i].argc;
        margv = ms->commands[i].argv;

        /* Only valid for sharded pubsub as regular pubsub can operate on any node and bypasses this layer. */
        if (!pubsubshard_included &&
            doesCommandHaveChannelsWithFlags(mcmd, CMD_CHANNEL_PUBLISH | CMD_CHANNEL_SUBSCRIBE))
        {
            pubsubshard_included = 1;
        }

        getKeysResult result = GETKEYS_RESULT_INIT;
        numkeys = getKeysFromCommand(mcmd,margv,margc,&result);
        keyindex = result.keys;

        for (j = 0; j < numkeys; j++) {
            robj *thiskey = margv[keyindex[j].pos];
            int thisslot = keyHashSlot((char*)thiskey->ptr,
                                       sdslen(thiskey->ptr));

            if (firstkey == NULL) {
                /* This is the first key we see. Check what is the slot
                 * and node.
                 * （这是我们看到的第一个 key。检查它属于哪个 slot 和哪个节点。）*/
                firstkey = thiskey;
                slot = thisslot;
                n = getNodeBySlot(slot);

                /* Error: If a slot is not served, we are in "cluster down"
                 * state. However the state is yet to be updated, so this was
                 * not trapped earlier in processCommand(). Report the same
                 * error to the client.
                 * （错误：如果一个 slot 没有被服务，则我们处于 "cluster down"
                 *  状态。但是该状态尚未被更新，因此之前在 processCommand()
                 *  中并未捕获。向客户端报告同样的错误。）*/
                if (n == NULL) {
                    getKeysFreeResult(&result);
                    if (error_code)
                        *error_code = CLUSTER_REDIR_DOWN_UNBOUND;
                    return NULL;
                }

                /* If we are migrating or importing this slot, we need to check
                 * if we have all the keys in the request (the only way we
                 * can safely serve the request, otherwise we return a TRYAGAIN
                 * error). To do so we set the importing/migrating state and
                 * increment a counter for every missing key.
                 * （如果当前正在迁移或导入这个 slot，则需要检查请求中涉及的所有
                 *  key 是否都已存在（这是安全处理请求的唯一方式，否则我们将返回
                 *  TRYAGAIN 错误）。为此我们设置导入/迁移状态，并对每个缺失的
                 *  key 进行计数。）*/
                if (n == myself &&
                    getMigratingSlotDest(slot) != NULL)
                {
                    migrating_slot = 1;
                } else if (getImportingSlotSource(slot) != NULL) {
                    importing_slot = 1;
                }
            } else {
                /* If it is not the first key/channel, make sure it is exactly
                 * the same key/channel as the first we saw.
                 * （如果不是第一个 key/channel，确保它与第一次看到的 key/channel 完全相同。）*/
                if (slot != thisslot) {
                    /* Error: multiple keys from different slots.
                     * （错误：来自不同 slot 的多个 key。）*/
                    getKeysFreeResult(&result);
                    if (error_code)
                        *error_code = CLUSTER_REDIR_CROSS_SLOT;
                    return NULL;
                }
                if (importing_slot && !multiple_keys && !equalStringObjects(firstkey,thiskey)) {
                    /* Flag this request as one with multiple different
                     * keys/channels when the slot is in importing state.
                     * （当 slot 处于导入状态时，将该请求标记为包含多个不同 key/channel 的请求。）*/
                    multiple_keys = 1;
                }
            }

            /* Migrating / Importing slot? Count keys we don't have.
             * If it is pubsubshard command, it isn't required to check
             * the channel being present or not in the node during the
             * slot migration, the channel will be served from the source
             * node until the migration completes with CLUSTER SETSLOT <slot>
             * NODE <node-id>.
             * （正在迁移/导入的 slot？统计本地缺失的 key。
             *  如果是 pubsub shard 命令，则在 slot 迁移期间不必检查节点中是否存在该
             *  channel；在通过 CLUSTER SETSLOT <slot> NODE <node-id> 完成迁移之前，
             *  该 channel 仍由源节点提供服务。）*/
            int flags = LOOKUP_NOTOUCH | LOOKUP_NOSTATS | LOOKUP_NONOTIFY | LOOKUP_NOEXPIRE;
            if ((migrating_slot || importing_slot) && !pubsubshard_included)
            {
                if (lookupKeyReadWithFlags(&server.db[0], thiskey, flags) == NULL) missing_keys++;
                else existing_keys++;
            }
        }
        getKeysFreeResult(&result);
    }

    /* No key at all in command? then we can serve the request
     * without redirections or errors in all the cases.
     * （命令中完全没有 key？那么在所有情况下我们都可以处理该请求，
     *  无需重定向或报错。）*/
    if (n == NULL) return myself;

    /* Cluster is globally down but we got keys? We only serve the request
     * if it is a read command and when allow_reads_when_down is enabled.
     * （集群整体处于 down 状态但我们仍然收到了带 key 的请求？只有在它是读命令，
     *  并且开启了 allow_reads_when_down 时我们才会处理它。）*/
    if (!isClusterHealthy()) {
        if (pubsubshard_included) {
            if (!server.cluster_allow_pubsubshard_when_down) {
                if (error_code) *error_code = CLUSTER_REDIR_DOWN_STATE;
                return NULL;
            }
        } else if (!server.cluster_allow_reads_when_down) {
            /* The cluster is configured to block commands when the
             * cluster is down. */
            if (error_code) *error_code = CLUSTER_REDIR_DOWN_STATE;
            return NULL;
        } else if (cmd_flags & CMD_WRITE) {
            /* The cluster is configured to allow read only commands */
            if (error_code) *error_code = CLUSTER_REDIR_DOWN_RO_STATE;
            return NULL;
        } else {
            /* Fall through and allow the command to be executed:
             * this happens when server.cluster_allow_reads_when_down is
             * true and the command is not a write command */
        }
    }

    /* Return the hashslot by reference. */
    if (hashslot) *hashslot = slot;

    /* MIGRATE always works in the context of the local node if the slot
     * is open (migrating or importing state). We need to be able to freely
     * move keys among instances in this case. */
    if ((migrating_slot || importing_slot) && cmd->proc == migrateCommand)
        return myself;

    /* If we don't have all the keys and we are migrating the slot, send
     * an ASK redirection or TRYAGAIN.
     * （如果本地不拥有所有 key，且正在迁移 slot，则发送 ASK 重定向或 TRYAGAIN。）*/
    if (migrating_slot && missing_keys) {
        /* If we have keys but we don't have all keys, we return TRYAGAIN
         * （如果只拥有部分 key，则返回 TRYAGAIN）*/
        if (existing_keys) {
            if (error_code) *error_code = CLUSTER_REDIR_UNSTABLE;
            return NULL;
        } else {
            if (error_code) *error_code = CLUSTER_REDIR_ASK;
            return getMigratingSlotDest(slot);
        }
    }

    /* If we are receiving the slot, and the client correctly flagged the
     * request as "ASKING", we can serve the request. However if the request
     * involves multiple keys and we don't have them all, the only option is
     * to send a TRYAGAIN error.
     * （如果我们正在接收 slot，并且客户端正确地将该请求标记为 "ASKING"，
     *  则可以处理该请求。但是如果请求涉及多个 key 而我们并未全部拥有，
     *  唯一的选择就是返回 TRYAGAIN 错误。）*/
    if (importing_slot &&
        (c->flags & CLIENT_ASKING || cmd_flags & CMD_ASKING))
    {
        if (multiple_keys && missing_keys) {
            if (error_code) *error_code = CLUSTER_REDIR_UNSTABLE;
            return NULL;
        } else {
            return myself;
        }
    }

    /* Handle the read-only client case reading from a slave: if this
     * node is a slave and the request is about a hash slot our master
     * is serving, we can reply without redirection.
     * （处理从节点读取的只读客户端场景：如果本节点是从节点，
     *  且请求访问的是其主节点正在服务的 hash slot，
     *  则可以直接响应而无需重定向。）*/
    int is_write_command = (cmd_flags & CMD_WRITE) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_WRITE));
    if (((c->flags & CLIENT_READONLY) || pubsubshard_included) &&
        !is_write_command &&
        clusterNodeIsSlave(myself) &&
        clusterNodeGetSlaveof(myself) == n)
    {
        return myself;
    }

    /* Base case: just return the right node. However, if this node is not
     * myself, set error_code to MOVED since we need to issue a redirection.
     * （基本用例：仅返回正确的节点。但如果该节点不是 myself，
     *  则将 error_code 设为 MOVED，因为我们需要发出重定向。）*/
    if (n != myself && error_code) *error_code = CLUSTER_REDIR_MOVED;
    return n;
}

/* Send the client the right redirection code, according to error_code
 * that should be set to one of CLUSTER_REDIR_* macros.
 *
 * If CLUSTER_REDIR_ASK or CLUSTER_REDIR_MOVED error codes
 * are used, then the node 'n' should not be NULL, but should be the
 * node we want to mention in the redirection. Moreover hashslot should
 * be set to the hash slot that caused the redirection.
 *
 * 根据 error_code 向客户端发送相应的重定向码，
 * error_code 应被设置为 CLUSTER_REDIR_* 宏之一。
 *
 * 如果使用 CLUSTER_REDIR_ASK 或 CLUSTER_REDIR_MOVED 错误码，
 * 则节点 'n' 不应为空，必须是我们希望在重定向中提到的节点。
 * 同时 hashslot 应被设置为引起该重定向的 hash slot。*/
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code) {
    if (error_code == CLUSTER_REDIR_CROSS_SLOT) {
        addReplyError(c,"-CROSSSLOT Keys in request don't hash to the same slot");
    } else if (error_code == CLUSTER_REDIR_UNSTABLE) {
        /* The request spawns multiple keys in the same slot,
         * but the slot is not "stable" currently as there is
         * a migration or import in progress.
         * （请求涉及同一 slot 中的多个 key，
         *  但该 slot 当前并不 "稳定"，因为正在进行迁移或导入。）*/
        addReplyError(c,"-TRYAGAIN Multiple keys request during rehashing of slot");
    } else if (error_code == CLUSTER_REDIR_DOWN_STATE) {
        addReplyError(c,"-CLUSTERDOWN The cluster is down");
    } else if (error_code == CLUSTER_REDIR_DOWN_RO_STATE) {
        addReplyError(c,"-CLUSTERDOWN The cluster is down and only accepts read commands");
    } else if (error_code == CLUSTER_REDIR_DOWN_UNBOUND) {
        addReplyError(c,"-CLUSTERDOWN Hash slot not served");
    } else if (error_code == CLUSTER_REDIR_MOVED ||
               error_code == CLUSTER_REDIR_ASK)
    {
        /* Report TLS ports to TLS client, and report non-TLS port to non-TLS client.
         * （向 TLS 客户端返回 TLS 端口，向非 TLS 客户端返回非 TLS 端口）*/
        int port = clusterNodeClientPort(n, shouldReturnTlsInfo());
        addReplyErrorSds(c,sdscatprintf(sdsempty(),
                                        "-%s %d %s:%d",
                                        (error_code == CLUSTER_REDIR_ASK) ? "ASK" : "MOVED",
                                        hashslot, clusterNodePreferredEndpoint(n), port));
    } else {
        serverPanic("getNodeByQuery() unknown error.");
    }
}

/* This function is called by the function processing clients incrementally
 * to detect timeouts, in order to handle the following case:
 *
 * 1) A client blocks with BLPOP or similar blocking operation.
 * 2) The master migrates the hash slot elsewhere or turns into a slave.
 * 3) The client may remain blocked forever (or up to the max timeout time)
 *    waiting for a key change that will never happen.
 *
 * If the client is found to be blocked into a hash slot this node no
 * longer handles, the client is sent a redirection error, and the function
 * returns 1. Otherwise 0 is returned and no operation is performed.
 *
 * 该函数由处理客户端的增量函数调用以检测超时，用于处理以下情况：
 *
 *  1) 客户端通过 BLPOP 或类似的阻塞操作进入阻塞状态。
 *  2) 主节点将该 hash slot 迁移到其他地方或变为从节点。
 *  3) 客户端可能因此永远（或直到最大超时时间）阻塞，
 *     等待一个永远不会发生的 key 变更。
 *
 *  如果发现客户端阻塞在某个 hash slot 上，而本节点已不再处理该 slot，
 *  则向客户端发送重定向错误并返回 1；否则返回 0，不进行任何操作。*/
int clusterRedirectBlockedClientIfNeeded(client *c) {
    clusterNode *myself = getMyClusterNode();
    if (c->flags & CLIENT_BLOCKED &&
        (c->bstate.btype == BLOCKED_LIST ||
         c->bstate.btype == BLOCKED_ZSET ||
         c->bstate.btype == BLOCKED_STREAM ||
         c->bstate.btype == BLOCKED_MODULE))
    {
        dictEntry *de;
        dictIterator *di;

        /* If the cluster is down, unblock the client with the right error.
         * If the cluster is configured to allow reads on cluster down, we
         * still want to emit this error since a write will be required
         * to unblock them which may never come.
         * （如果集群处于 down 状态，则以相应的错误解除客户端的阻塞。
         *  即使集群配置为在 down 状态下允许读操作，我们仍然要发出该错误，
         *  因为解除阻塞需要写入操作，而该写入可能永远不会发生。）*/
        if (!isClusterHealthy()) {
            clusterRedirectClient(c,NULL,0,CLUSTER_REDIR_DOWN_STATE);
            return 1;
        }

        /* If the client is blocked on module, but not on a specific key,
         * don't unblock it (except for the CLUSTER_FAIL case above).
         * （如果客户端因模块而阻塞，但未阻塞在特定 key 上，
         *  则不要解除它的阻塞（除上述 CLUSTER_FAIL 情况外）。）*/
        if (c->bstate.btype == BLOCKED_MODULE && !moduleClientIsBlockedOnKeys(c))
            return 0;

        /* All keys must belong to the same slot, so check first key only.
         * （所有 key 必须属于同一 slot，因此只检查第一个 key。）*/
        di = dictGetIterator(c->bstate.keys);
        if ((de = dictNext(di)) != NULL) {
            robj *key = dictGetKey(de);
            int slot = keyHashSlot((char*)key->ptr, sdslen(key->ptr));
            clusterNode *node = getNodeBySlot(slot);

            /* if the client is read-only and attempting to access key that our
             * replica can handle, allow it.
             * （如果客户端是只读的，并且尝试访问我们的副本可以处理的 key，则允许。）*/
            if ((c->flags & CLIENT_READONLY) &&
                !(c->lastcmd->flags & CMD_WRITE) &&
                clusterNodeIsSlave(myself) && clusterNodeGetSlaveof(myself) == node)
            {
                node = myself;
            }

            /* We send an error and unblock the client if:
             * 1) The slot is unassigned, emitting a cluster down error.
             * 2) The slot is not handled by this node, nor being imported.
             * （在以下情况下我们发送错误并解除客户端阻塞：
             *  1) slot 未被分配，发出 cluster down 错误；
             *  2) slot 不由本节点处理，也不在导入中。）*/
            if (node != myself && getImportingSlotSource(slot) == NULL)
            {
                if (node == NULL) {
                    clusterRedirectClient(c,NULL,0,
                                          CLUSTER_REDIR_DOWN_UNBOUND);
                } else {
                    clusterRedirectClient(c,node,slot,
                                          CLUSTER_REDIR_MOVED);
                }
                dictReleaseIterator(di);
                return 1;
            }
        }
        dictReleaseIterator(di);
    }
    return 0;
}

/* Returns an indication if the replica node is fully available
 * and should be listed in CLUSTER SLOTS response.
 * Returns 1 for available nodes, 0 for nodes that have
 * not finished their initial sync, in failed state, or are
 * otherwise considered not available to serve read commands.
 *
 * 返回一个副本节点是否完全可用、并应被列入 CLUSTER SLOTS 响应中的指示。
 * 对于可用节点返回 1，对于尚未完成初次同步、处于失败状态，
 * 或因其他原因被视为无法处理读命令的节点返回 0。*/
static int isReplicaAvailable(clusterNode *node) {
    if (clusterNodeIsFailing(node)) {
        return 0;
    }
    long long repl_offset = clusterNodeReplOffset(node);
    if (clusterNodeIsMyself(node)) {
        /* Nodes do not update their own information
         * in the cluster node list.
         * （节点不会在集群节点列表中更新自身的信息。）*/
        repl_offset = replicationGetSlaveOffset();
    }
    return (repl_offset != 0);
}

void addNodeToNodeReply(client *c, clusterNode *node) {
    char* hostname = clusterNodeHostname(node);
    addReplyArrayLen(c, 4);
    if (server.cluster_preferred_endpoint_type == CLUSTER_ENDPOINT_TYPE_IP) {
        addReplyBulkCString(c, clusterNodeIp(node));
    } else if (server.cluster_preferred_endpoint_type == CLUSTER_ENDPOINT_TYPE_HOSTNAME) {
        if (hostname != NULL && hostname[0] != '\0') {
            addReplyBulkCString(c, hostname);
        } else {
            addReplyBulkCString(c, "?");
        }
    } else if (server.cluster_preferred_endpoint_type == CLUSTER_ENDPOINT_TYPE_UNKNOWN_ENDPOINT) {
        addReplyNull(c);
    } else {
        serverPanic("Unrecognized preferred endpoint type");
    }

    /* Report TLS ports to TLS client, and report non-TLS port to non-TLS client.
     * （向 TLS 客户端返回 TLS 端口，向非 TLS 客户端返回非 TLS 端口）*/
    addReplyLongLong(c, clusterNodeClientPort(node, shouldReturnTlsInfo()));
    addReplyBulkCBuffer(c, clusterNodeGetName(node), CLUSTER_NAMELEN);

    /* Add the additional endpoint information, this is all the known networking information
     * that is not the preferred endpoint. Note the logic is evaluated twice so we can
     * correctly report the number of additional network arguments without using a deferred
     * map, an assertion is made at the end to check we set the right length.
     * （添加额外的端点信息，即所有非首选端点的已知网络信息。
     *  注意，这段逻辑会被执行两次，以便能够正确报告额外的网络参数数量，
     *  而无需使用延迟映射；最后会有一个断言来检查是否设置了正确的长度。）*/
    int length = 0;
    if (server.cluster_preferred_endpoint_type != CLUSTER_ENDPOINT_TYPE_IP) {
        length++;
    }
    if (server.cluster_preferred_endpoint_type != CLUSTER_ENDPOINT_TYPE_HOSTNAME
        && hostname != NULL && hostname[0] != '\0')
    {
        length++;
    }
    addReplyMapLen(c, length);

    if (server.cluster_preferred_endpoint_type != CLUSTER_ENDPOINT_TYPE_IP) {
        addReplyBulkCString(c, "ip");
        addReplyBulkCString(c, clusterNodeIp(node));
        length--;
    }
    if (server.cluster_preferred_endpoint_type != CLUSTER_ENDPOINT_TYPE_HOSTNAME
        && hostname != NULL && hostname[0] != '\0')
    {
        addReplyBulkCString(c, "hostname");
        addReplyBulkCString(c, hostname);
        length--;
    }
    serverAssert(length == 0);
}

void addNodeReplyForClusterSlot(client *c, clusterNode *node, int start_slot, int end_slot) {
    int i, nested_elements = 3; /* slots (2) + master addr (1) */
    for (i = 0; i < clusterNodeNumSlaves(node); i++) {
        if (!isReplicaAvailable(clusterNodeGetSlave(node, i))) continue;
        nested_elements++;
    }
    addReplyArrayLen(c, nested_elements);
    addReplyLongLong(c, start_slot);
    addReplyLongLong(c, end_slot);
    addNodeToNodeReply(c, node);

    /* Remaining nodes in reply are replicas for slot range
     * （回复中剩余的节点是该 slot 范围的副本）*/
    for (i = 0; i < clusterNodeNumSlaves(node); i++) {
        /* This loop is copy/pasted from clusterGenNodeDescription()
         * with modifications for per-slot node aggregation.
         * （此循环是从 clusterGenNodeDescription() 复制/粘贴而来，
         *  并针对按 slot 聚合节点进行了修改。）*/
        if (!isReplicaAvailable(clusterNodeGetSlave(node, i))) continue;
        addNodeToNodeReply(c, clusterNodeGetSlave(node, i));
        nested_elements--;
    }
    serverAssert(nested_elements == 3); /* Original 3 elements
                                         * （最初的 3 个元素）*/
}

void clusterCommandSlots(client * c) {
    /* Format: 1) 1) start slot
     *            2) end slot
     *            3) 1) master IP
     *               2) master port
     *               3) node ID
     *            4) 1) replica IP
     *               2) replica port
     *               3) node ID
     *           ... continued until done
     */
    clusterNode *n = NULL;
    int num_masters = 0, start = -1;
    void *slot_replylen = addReplyDeferredLen(c);

    for (int i = 0; i <= CLUSTER_SLOTS; i++) {
        /* Find start node and slot id.
         * （查找起始节点和 slot id）*/
        if (n == NULL) {
            if (i == CLUSTER_SLOTS) break;
            n = getNodeBySlot(i);
            start = i;
            continue;
        }

        /* Add cluster slots info when occur different node with start
         * or end of slot.
         * （当遇到与起始节点不同的节点，或到达 slot 末尾时，
         *  添加集群 slot 信息。）*/
        if (i == CLUSTER_SLOTS || n != getNodeBySlot(i)) {
            addNodeReplyForClusterSlot(c, n, start, i-1);
            num_masters++;
            if (i == CLUSTER_SLOTS) break;
            n = getNodeBySlot(i);
            start = i;
        }
    }
    setDeferredArrayLen(c, slot_replylen, num_masters);
}

/* -----------------------------------------------------------------------------
 * Cluster functions related to serving / redirecting clients
 * （与为客户端服务/重定向相关的集群函数）
 * -------------------------------------------------------------------------- */

/* The ASKING command is required after a -ASK redirection.
 * The client should issue ASKING before to actually send the command to
 * the target instance. See the Redis Cluster specification for more
 * information.
 * （ASKING 命令在收到 -ASK 重定向之后必须由客户端发送。
 *  客户端应在实际向目标实例发送命令之前发送 ASKING。
 *  更多信息请参阅 Redis Cluster 规范。）*/
void askingCommand(client *c) {
    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }
    c->flags |= CLIENT_ASKING;
    addReply(c,shared.ok);
}

/* The READONLY command is used by clients to enter the read-only mode.
 * In this mode slaves will not redirect clients as long as clients access
 * with read-only commands to keys that are served by the slave's master.
 * （READONLY 命令用于让客户端进入只读模式。
 *  在此模式下，只要客户端使用只读命令访问由从节点主节点提供服务的 key，
 *  从节点就不会对客户端进行重定向。）*/
void readonlyCommand(client *c) {
    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }
    c->flags |= CLIENT_READONLY;
    addReply(c,shared.ok);
}

/* The READWRITE command just clears the READONLY command state.
 * （READWRITE 命令仅用于清除 READONLY 命令所设置的只读状态。）*/
void readwriteCommand(client *c) {
    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }
    c->flags &= ~CLIENT_READONLY;
    addReply(c,shared.ok);
}
