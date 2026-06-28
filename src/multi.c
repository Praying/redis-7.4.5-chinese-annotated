/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"

/* ================================ MULTI/EXEC ============================== */
/*
 * MULTI/EXEC 事务实现
 *
 * MULTI/EXEC 是 Redis 提供的事务机制。MULTI 命令开启一个事务块，
 * 之后的所有命令会被排队，直到 EXEC 命令触发时才依次执行。
 * DISCARD 可以取消事务。WATCH 提供乐观锁（CAS）支持，
 * 在 EXEC 执行前检测被监视的键是否被其他客户端修改过。
 */

/* 初始化客户端的 MULTI/EXEC 事务状态 */
void initClientMultiState(client *c) {
    c->mstate.commands = NULL;
    c->mstate.count = 0;
    c->mstate.cmd_flags = 0;
    c->mstate.cmd_inv_flags = 0;
    c->mstate.argv_len_sums = 0;
    c->mstate.alloc_count = 0;
}

/* 释放与 MULTI/EXEC 事务状态相关的所有资源 */
void freeClientMultiState(client *c) {
    int j;

    for (j = 0; j < c->mstate.count; j++) {
        int i;
        multiCmd *mc = c->mstate.commands+j;

        for (i = 0; i < mc->argc; i++)
            decrRefCount(mc->argv[i]);
        zfree(mc->argv);
    }
    zfree(c->mstate.commands);
}

/* 将新命令加入 MULTI 事务命令队列 */
void queueMultiCommand(client *c, uint64_t cmd_flags) {
    multiCmd *mc;

    /* 如果事务已经被中止，没有意义再浪费内存。
     * 当客户端以流水线方式发送命令，或者没有读取之前的
     * 响应而没注意到事务已经被中止时，这个检查很有用。 */
    if (c->flags & (CLIENT_DIRTY_CAS|CLIENT_DIRTY_EXEC))
        return;
    if (c->mstate.count == 0) {
        /* 假设使用 multi/exec 的客户端至少会执行两条命令，
         * 因此默认分配大小为 2。 */
        c->mstate.commands = zmalloc(sizeof(multiCmd)*2);
        c->mstate.alloc_count = 2;
    }
    if (c->mstate.count == c->mstate.alloc_count) {
        c->mstate.alloc_count = c->mstate.alloc_count < INT_MAX/2 ? c->mstate.alloc_count*2 : INT_MAX;
        c->mstate.commands = zrealloc(c->mstate.commands, sizeof(multiCmd)*(c->mstate.alloc_count));
    }
    mc = c->mstate.commands+c->mstate.count;
    mc->cmd = c->cmd;
    mc->argc = c->argc;
    mc->argv = c->argv;
    mc->argv_len = c->argv_len;

    c->mstate.count++;
    c->mstate.cmd_flags |= cmd_flags;
    c->mstate.cmd_inv_flags |= ~cmd_flags;
    c->mstate.argv_len_sums += c->argv_len_sum + sizeof(robj*)*c->argc;

    /* 重置客户端的参数，因为已经拷贝到 mstate 中，
     * 不应再从 c 中引用它们。 */
    c->argv = NULL;
    c->argc = 0;
    c->argv_len_sum = 0;
    c->argv_len = 0;
}

/* 丢弃当前事务，释放所有资源并取消所有 WATCH */
void discardTransaction(client *c) {
    freeClientMultiState(c);
    initClientMultiState(c);
    c->flags &= ~(CLIENT_MULTI|CLIENT_DIRTY_CAS|CLIENT_DIRTY_EXEC);
    unwatchAllKeys(c);
}

/* 将事务标记为 DIRTY_EXEC，使后续的 EXEC 失败。
 * 每当命令入队出错时都应调用此函数。 */
void flagTransaction(client *c) {
    if (c->flags & CLIENT_MULTI)
        c->flags |= CLIENT_DIRTY_EXEC;
}

/* MULTI 命令：开启事务，将客户端标记为 MULTI 状态 */
void multiCommand(client *c) {
    if (c->flags & CLIENT_MULTI) {
        addReplyError(c,"MULTI calls can not be nested");
        return;
    }
    c->flags |= CLIENT_MULTI;

    addReply(c,shared.ok);
}

/* DISCARD 命令：取消事务，释放所有排队的命令 */
void discardCommand(client *c) {
    if (!(c->flags & CLIENT_MULTI)) {
        addReplyError(c,"DISCARD without MULTI");
        return;
    }
    discardTransaction(c);
    addReply(c,shared.ok);
}

/* 中止事务，并附带特定的错误消息。
 * 事务始终以 -EXECABORT 错误中止，以便客户端知道服务器
 * 已退出 MULTI 状态，同时也会包含中止的实际原因。
 * 注意：'error' 可能以 \r\n 结尾也可能不以。参见 addReplyErrorFormat。 */
void execCommandAbort(client *c, sds error) {
    discardTransaction(c);

    if (error[0] == '-') error++;
    addReplyErrorFormat(c, "-EXECABORT Transaction discarded because of: %s", error);

    /* 向等待 MONITOR 数据的客户端发送 EXEC。之前已经发送了 MULTI，
     * 但没有发送任何排队的命令，现在发送 EXEC 表明事务已结束。 */
    replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
}

/* EXEC 命令：执行事务中所有排队的命令 */
void execCommand(client *c) {
    int j;
    robj **orig_argv;
    int orig_argc, orig_argv_len;
    struct redisCommand *orig_cmd;

    if (!(c->flags & CLIENT_MULTI)) {
        addReplyError(c,"EXEC without MULTI");
        return;
    }

    /* 如果监视的键已过期，则不允许执行 EXEC */
    if (isWatchedKeyExpired(c)) {
        c->flags |= (CLIENT_DIRTY_CAS);
    }

    /* 检查是否需要中止 EXEC，原因可能有：
     * 1) 某些被 WATCH 监视的键被修改了。
     * 2) 在命令入队过程中发生了错误。
     * 第一种情况失败时返回空数组（严格来说不是错误，
     * 而是一种特殊行为），第二种情况返回 EXECABORT 错误。 */
    if (c->flags & (CLIENT_DIRTY_CAS | CLIENT_DIRTY_EXEC)) {
        if (c->flags & CLIENT_DIRTY_EXEC) {
            addReplyErrorObject(c, shared.execaborterr);
        } else {
            addReply(c, shared.nullarray[c->resp]);
        }

        discardTransaction(c);
        return;
    }

    uint64_t old_flags = c->flags;

    /* 不允许在事务中使用阻塞命令 */
    c->flags |= CLIENT_DENY_BLOCKING;

    /* 执行所有排队的命令 */
    unwatchAllKeys(c); /* 尽早取消监视，避免浪费 CPU 周期 */

    server.in_exec = 1;

    orig_argv = c->argv;
    orig_argv_len = c->argv_len;
    orig_argc = c->argc;
    orig_cmd = c->cmd;
    addReplyArrayLen(c,c->mstate.count);
    for (j = 0; j < c->mstate.count; j++) {
        c->argc = c->mstate.commands[j].argc;
        c->argv = c->mstate.commands[j].argv;
        c->argv_len = c->mstate.commands[j].argv_len;
        c->cmd = c->realcmd = c->mstate.commands[j].cmd;

        /* 在执行时也会检查 ACL 权限，以防在命令入队后
         * 权限发生了变化。 */
        int acl_errpos;
        int acl_retval = ACLCheckAllPerm(c,&acl_errpos);
        if (acl_retval != ACL_OK) {
            char *reason;
            switch (acl_retval) {
            case ACL_DENIED_CMD:
                reason = "no permission to execute the command or subcommand";
                break;
            case ACL_DENIED_KEY:
                reason = "no permission to touch the specified keys";
                break;
            case ACL_DENIED_CHANNEL:
                reason = "no permission to access one of the channels used "
                         "as arguments";
                break;
            default:
                reason = "no permission";
                break;
            }
            addACLLogEntry(c,acl_retval,ACL_LOG_CTX_MULTI,acl_errpos,NULL,NULL);
            addReplyErrorFormat(c,
                "-NOPERM ACLs rules changed between the moment the "
                "transaction was accumulated and the EXEC call. "
                "This command is no longer allowed for the "
                "following reason: %s", reason);
        } else {
            if (c->id == CLIENT_ID_AOF)
                call(c,CMD_CALL_NONE);
            else
                call(c,CMD_CALL_FULL);

            serverAssert((c->flags & CLIENT_BLOCKED) == 0);
        }

        /* 命令可能修改 argc/argv，需要恢复 mstate 中的值 */
        c->mstate.commands[j].argc = c->argc;
        c->mstate.commands[j].argv = c->argv;
        c->mstate.commands[j].argv_len = c->argv_len;
        c->mstate.commands[j].cmd = c->cmd;
    }

    // 恢复之前的 DENY_BLOCKING 标志值
    if (!(old_flags & CLIENT_DENY_BLOCKING))
        c->flags &= ~CLIENT_DENY_BLOCKING;

    c->argv = orig_argv;
    c->argv_len = orig_argv_len;
    c->argc = orig_argc;
    c->cmd = c->realcmd = orig_cmd;
    discardTransaction(c);

    server.in_exec = 0;
}

/* ===================== WATCH (MULTI/EXEC 的 CAS 乐观锁) ==================
 *
 * 实现使用每个数据库的哈希表，将键映射到监视该键的客户端列表。
 * 当某个键即将被修改时，可以将所有相关客户端标记为脏（dirty）。
 *
 * 每个客户端也维护一个被监视键的列表，这样当客户端被释放
 * 或者调用 UNWATCH 时可以取消对这些键的监视。 */

/* watchedKey 结构体同时存在于两个链表中：
 * client->watched_keys 链表和 db->watched_keys 字典
 * （字典的每个值是一个 watchedKey 结构体的链表）。
 * 客户端结构体中的链表是普通链表，每个节点的值是指向 watchedKey 的指针。
 * 而 db->watched_keys 中的链表有所不同：此结构体中内嵌的 listnode 成员
 * 是字典中的节点，该 listnode 的值指向所属链表，通过结构体成员偏移计算
 * 可以从 listnode 得到 watchedKey 结构体。
 * 这样做是为了在从链表中删除时避免调用 listSearchKey 和 dictFind。 */
typedef struct watchedKey {
    listNode node;
    robj *key;
    redisDb *db;
    client *client;
    unsigned expired:1; /* 标记是否监视的是已过期的键 */
} watchedKey;

/* 将 watchedKey 链接到监视该键的客户端列表中 */
static inline void watchedKeyLinkToClients(list *clients, watchedKey *wk) {
    wk->node.value = clients; /* 将 value 指回链表 */
    listLinkNodeTail(clients, &wk->node); /* 链接内嵌的节点 */
}

/* 获取监视该键的客户端列表 */
static inline list *watchedKeyGetClients(watchedKey *wk) {
    return listNodeValue(&wk->node); /* 内嵌节点的 value 指回链表 */
}

/* 获取在监视该键的客户端列表中代表 wk->client 的节点，
 * 实际上就是内嵌的节点本身 */
static inline listNode *watchedKeyGetClientNode(watchedKey *wk) {
    return &wk->node;
}

/* 监视指定的键 */
void watchForKey(client *c, robj *key) {
    list *clients = NULL;
    listIter li;
    listNode *ln;
    watchedKey *wk;

    if (listLength(c->watched_keys) == 0) server.watching_clients++;

    /* 检查是否已经在监视这个键 */
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {
        wk = listNodeValue(ln);
        if (wk->db == c->db && equalStringObjects(key,wk->key))
            return; /* 该键已经在监视中 */
    }
    /* 此数据库中尚未监视此键，添加它 */
    clients = dictFetchValue(c->db->watched_keys,key);
    if (!clients) {
        clients = listCreate();
        dictAdd(c->db->watched_keys,key,clients);
        incrRefCount(key);
    }
    /* 将新键添加到此客户端的监视键列表中 */
    wk = zmalloc(sizeof(*wk));
    wk->key = key;
    wk->client = c;
    wk->db = c->db;
    wk->expired = keyIsExpired(c->db, key);
    incrRefCount(key);
    listAddNodeTail(c->watched_keys, wk);
    watchedKeyLinkToClients(clients, wk);
}

/* 取消监视此客户端监视的所有键。
 * 清除 EXEC 脏标志由调用者负责。 */
void unwatchAllKeys(client *c) {
    listIter li;
    listNode *ln;

    if (listLength(c->watched_keys) == 0) return;
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {
        list *clients;
        watchedKey *wk;

        /* 从监视该键的客户端列表中移除此客户端的 wk */
        wk = listNodeValue(ln);
        clients = watchedKeyGetClients(wk);
        serverAssertWithInfo(c,NULL,clients != NULL);
        listUnlinkNode(clients, watchedKeyGetClientNode(wk));
        /* 如果这是最后一个客户端，则删除整个条目 */
        if (listLength(clients) == 0)
            dictDelete(wk->db->watched_keys, wk->key);
        /* 从 client->watched 列表中移除此监视键 */
        listDelNode(c->watched_keys,ln);
        decrRefCount(wk->key);
        zfree(wk);
    }
    server.watching_clients--;
}

/* 遍历 watched_keys 列表，查找已过期的键。
 * 在调用 WATCH 时就已经过期的键会被忽略。 */
int isWatchedKeyExpired(client *c) {
    listIter li;
    listNode *ln;
    watchedKey *wk;
    if (listLength(c->watched_keys) == 0) return 0;
    listRewind(c->watched_keys,&li);
    while ((ln = listNext(&li))) {
        wk = listNodeValue(ln);
        if (wk->expired) continue; /* 调用 WATCH 时已过期 */
        if (keyIsExpired(wk->db, wk->key)) return 1;
    }

    return 0;
}

/* "触碰"一个键，使监视该键的客户端在下次 EXEC 时失败 */
void touchWatchedKey(redisDb *db, robj *key) {
    list *clients;
    listIter li;
    listNode *ln;

    if (dictSize(db->watched_keys) == 0) return;
    clients = dictFetchValue(db->watched_keys, key);
    if (!clients) return;

    /* 将所有监视此键的客户端标记为 CLIENT_DIRTY_CAS */
    listRewind(clients,&li);
    while((ln = listNext(&li))) {
        watchedKey *wk = redis_member2struct(watchedKey, node, ln);
        client *c = wk->client;

        if (wk->expired) {
            /* 调用 WATCH 时该键已过期 */
            if (db == wk->db &&
                equalStringObjects(key, wk->key) &&
                dbFind(db, key->ptr) == NULL)
            {
                /* 已过期的键被删除，逻辑上没有变化。
                 * 清除标志。已删除的键不会标记为过期。 */
                wk->expired = 0;
                goto skip_client;
            }
            break;
        }

        c->flags |= CLIENT_DIRTY_CAS;
        /* 既然客户端已被标记为脏，就没有必要在键再次被修改时
         * 重复执行此操作（或一直保留内存开销直到 EXEC）。 */
        unwatchAllKeys(c);

    skip_client:
        continue;
    }
}

/* 当数据库被清空时，将该数据库所有客户端标记为 CLIENT_DIRTY_CAS。
 * 可能发生在以下场景：
 * FLUSHDB、FLUSHALL、SWAPDB、无盘复制成功完成。
 *
 * replaced_with：对于 SWAPDB，如果键存在于任一数据库中，
 * 则 WATCH 应失效；仅当两个数据库中都不存在时才跳过。 */
void touchAllWatchedKeysInDb(redisDb *emptied, redisDb *replaced_with) {
    listIter li;
    listNode *ln;
    dictEntry *de;

    if (dictSize(emptied->watched_keys) == 0) return;

    dictIterator *di = dictGetSafeIterator(emptied->watched_keys);
    while((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        int exists_in_emptied = dbFind(emptied, key->ptr) != NULL;
        if (exists_in_emptied ||
            (replaced_with && dbFind(replaced_with, key->ptr) != NULL))
        {
            list *clients = dictGetVal(de);
            if (!clients) continue;
            listRewind(clients,&li);
            while((ln = listNext(&li))) {
                watchedKey *wk = redis_member2struct(watchedKey, node, ln);
                if (wk->expired) {
                    if (!replaced_with || !dbFind(replaced_with, key->ptr)) {
                        /* 过期的键已被删除，逻辑上无变化。
                         * 清除标志。已删除的键不标记为过期。 */
                        wk->expired = 0;
                        continue;
                    } else if (keyIsExpired(replaced_with, key)) {
                        /* 过期的键仍然保持过期状态 */
                        continue;
                    }
                } else if (!exists_in_emptied && keyIsExpired(replaced_with, key)) {
                    /* 不存在的键被替换为已过期的键 */
                    wk->expired = 1;
                    continue;
                }
                client *c = wk->client;
                c->flags |= CLIENT_DIRTY_CAS;
                /* 注意：我们可以对特定客户端调用 unwatchAllKeys 来减少
                 * 迭代次数，但这可能释放迭代器当前持有的 next 指针，
                 * 导致释放后使用（use-after-free）问题。 */
            }
        }
    }
    dictReleaseIterator(di);
}

/* WATCH 命令：监视一个或多个键，在事务执行前检测它们是否被修改 */
void watchCommand(client *c) {
    int j;

    if (c->flags & CLIENT_MULTI) {
        addReplyError(c,"WATCH inside MULTI is not allowed");
        return;
    }
    /* 如果客户端已经被标记为脏，监视没有意义 */
    if (c->flags & CLIENT_DIRTY_CAS) {
        addReply(c,shared.ok);
        return;
    }
    for (j = 1; j < c->argc; j++)
        watchForKey(c,c->argv[j]);
    addReply(c,shared.ok);
}

/* UNWATCH 命令：取消监视所有键 */
void unwatchCommand(client *c) {
    unwatchAllKeys(c);
    c->flags &= (~CLIENT_DIRTY_CAS);
    addReply(c,shared.ok);
}

/* 计算 MULTI/EXEC 事务状态的内存开销 */
size_t multiStateMemOverhead(client *c) {
    size_t mem = c->mstate.argv_len_sums;
    /* 加上监视键的开销。注意：这不包括被监视键本身的内存，
     * 因为它们不是按客户端管理的。 */
    mem += listLength(c->watched_keys) * (sizeof(listNode) + sizeof(watchedKey));
    /* 为排队的事务命令预留的内存 */
    mem += c->mstate.alloc_count * sizeof(multiCmd);
    return mem;
}
