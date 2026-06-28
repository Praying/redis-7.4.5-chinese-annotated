/* blocked.c - 阻塞操作的通用支持，如 BLPOP 和 WAIT。
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * ---------------------------------------------------------------------------
 *
 * API 说明：
 *
 * blockClient() 在客户端上设置 CLIENT_BLOCKED 标志，并将指定的
 * 阻塞类型 'btype' 字段设置为 BLOCKED_* 宏之一。
 *
 * unblockClient() 解除客户端阻塞，执行以下操作：
 * 1) 调用特定于 btype 的函数来清理状态。
 * 2) 通过取消 CLIENT_BLOCKED 标志来解除客户端阻塞。
 * 3) 将客户端放入刚解除阻塞的客户端列表中，该列表会在
 *    beforeSleep() 事件循环回调中尽快处理，这样如果有查询
 *    缓冲区需要处理，我们就会处理它。这也是必需的，因为否则
 *    不会触发 'readable' 事件，我们已经读取了待处理的命令。
 *    我们还设置 CLIENT_UNBLOCKED 标志来记住客户端在
 *    unblocked_clients 列表中。
 *
 * processUnblockedClients() 在 beforeSleep() 函数内部调用，
 * 用于处理来自解除阻塞的客户端的查询缓冲区，并将客户端从
 * blocked_clients 队列中移除。
 *
 * replyToBlockedClientTimedOut() 由 cron 函数在被阻塞的客户端
 * 达到指定超时时调用（如果超时设置为 0，则不处理超时）。
 * 通常只需要向客户端发送回复。
 *
 * 实现新的阻塞操作类型时，应修改 unblockClient() 和
 * replyToBlockedClientTimedOut() 以处理这两个函数的 btype
 * 特定行为。如果阻塞操作等待某些键改变状态，还应更新
 * clusterRedirectBlockedClientIfNeeded() 函数。
 */

#include "server.h"
#include "slowlog.h"
#include "latency.h"
#include "monotonic.h"

/* 前向声明 */
static void unblockClientWaitingData(client *c);
static void handleClientsBlockedOnKey(readyList *rl);
static void unblockClientOnKey(client *c, robj *key);
static void moduleUnblockClientOnKey(client *c, robj *key);
static void releaseBlockedEntry(client *c, dictEntry *de, int remove_key);

void initClientBlockingState(client *c) {
    c->bstate.btype = BLOCKED_NONE;
    c->bstate.timeout = 0;
    c->bstate.keys = dictCreate(&objectKeyHeapPointerValueDictType);
    c->bstate.numreplicas = 0;
    c->bstate.reploffset = 0;
    c->bstate.unblock_on_nokey = 0;
    c->bstate.async_rm_call_handle = NULL;
}

/* 为特定操作类型阻塞客户端。一旦设置了 CLIENT_BLOCKED 标志，
 * 客户端查询缓冲区将不再被处理，而是累积起来，当客户端
 * 解除阻塞时才会被处理。 */
void blockClient(client *c, int btype) {
    /* 主客户端永远不应被阻塞，除非是 pause 或 module */
    serverAssert(!(c->flags & CLIENT_MASTER &&
                   btype != BLOCKED_MODULE &&
                   btype != BLOCKED_LAZYFREE &&
                   btype != BLOCKED_POSTPONE));

    c->flags |= CLIENT_BLOCKED;
    c->bstate.btype = btype;
    if (!(c->flags & CLIENT_MODULE)) server.blocked_clients++; /* 仅统计普通客户端的阻塞数，不统计模块客户端 */
    server.blocked_clients_by_type[btype]++;
    addClientToTimeoutTable(c);
}

/* 通常当客户端因处理某个命令时被阻塞而解除阻塞时，它会尝试
 * 重新处理该命令，从而更新统计数据。但是，如果客户端超时或
 * 模块阻塞的客户端正在解除阻塞，命令将不会被重新处理，
 * 我们需要手动更新统计数据。
 * 此函数将更新 commandstats、slowlog 和 monitors。 */
void updateStatsOnUnblock(client *c, long blocked_us, long reply_us, int had_errors){
    const ustime_t total_cmd_duration = c->duration + blocked_us + reply_us;
    c->lastcmd->microseconds += total_cmd_duration;
    c->lastcmd->calls++;
    server.stat_numcommands++;
    if (had_errors)
        c->lastcmd->failed_calls++;
    if (server.latency_tracking_enabled)
        updateCommandLatencyHistogram(&(c->lastcmd->latency_histogram), total_cmd_duration*1000);
    /* 如果需要，将命令记录到慢查询日志中。 */
    slowlogPushCurrentCommand(c, c->lastcmd, total_cmd_duration);
    c->duration = 0;
    /* 记录回复耗时事件。 */
    latencyAddSampleIfNeeded("command-unblocking",reply_us/1000);
}

/* 此函数在事件循环的 beforeSleep() 函数中调用，
 * 用于处理阻塞操作后解除阻塞的客户端的待处理输入缓冲区。 */
void processUnblockedClients(void) {
    listNode *ln;
    client *c;

    while (listLength(server.unblocked_clients)) {
        ln = listFirst(server.unblocked_clients);
        serverAssert(ln != NULL);
        c = ln->value;
        listDelNode(server.unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;

        if (c->flags & CLIENT_MODULE) {
            if (!(c->flags & CLIENT_BLOCKED)) {
                moduleCallCommandUnblockedHandler(c);
            }
            continue;
        }

        /* 处理输入缓冲区中的剩余数据，除非客户端再次被阻塞。
         * 实际上 processInputBuffer() 会在执行前检查客户端
         * 是否被阻塞，但情况可能会变化，这样写在概念上更正确。 */
        if (!(c->flags & CLIENT_BLOCKED)) {
            /* 如果有待处理的命令，立即执行它。 */
            if (processPendingCommandAndInputBuffer(c) == C_ERR) {
                c = NULL;
            }
        }
        beforeNextClient(c);
    }
}

/* 此函数将安排客户端在安全时间重新处理。
 *
 * 当客户端因某种原因被阻塞（阻塞操作、CLIENT PAUSE 或其他原因）
 * 时很有用，因为它可能累积了一些需要尽快处理的查询缓冲区：
 *
 * 1. 当客户端被阻塞时，其可读处理器仍然活跃。
 * 2. 但在这种情况下，它只将数据放入查询缓冲区，而不会像往常一样
 *    在有足够数据时解析或执行查询（因为客户端被阻塞了……
 *    所以我们无法执行命令）。
 * 3. 当客户端解除阻塞时，如果没有此函数，客户端必须写入一些查询
 *    才能让可读处理器最终调用 processQueryBuffer*()。
 * 4. 使用此函数，我们可以将客户端放入队列中，在安全时间
 *    处理准备执行的查询。
 */
void queueClientForReprocessing(client *c) {
    /* 客户端可能因之前的阻塞操作已在解除阻塞列表中，
     * 不要多次将其添加回列表。 */
    if (!(c->flags & CLIENT_UNBLOCKED)) {
        c->flags |= CLIENT_UNBLOCKED;
        listAddNodeTail(server.unblocked_clients,c);
    }
}

/* 根据客户端阻塞的操作类型，调用正确的函数来解除客户端阻塞。 */
void unblockClient(client *c, int queue_for_reprocessing) {
    if (c->bstate.btype == BLOCKED_LIST ||
        c->bstate.btype == BLOCKED_ZSET ||
        c->bstate.btype == BLOCKED_STREAM) {
        unblockClientWaitingData(c);
    } else if (c->bstate.btype == BLOCKED_WAIT || c->bstate.btype == BLOCKED_WAITAOF) {
        unblockClientWaitingReplicas(c);
    } else if (c->bstate.btype == BLOCKED_MODULE) {
        if (moduleClientIsBlockedOnKeys(c)) unblockClientWaitingData(c);
        unblockClientFromModule(c);
    } else if (c->bstate.btype == BLOCKED_POSTPONE) {
        listDelNode(server.postponed_clients,c->postponed_list_node);
        c->postponed_list_node = NULL;
    } else if (c->bstate.btype == BLOCKED_SHUTDOWN) {
        /* 无需特殊清理。 */
    } else if (c->bstate.btype == BLOCKED_LAZYFREE) {
        /* 无需特殊清理。 */
    } else {
        serverPanic("Unknown btype in unblockClient().");
    }

    /* 为新查询重置客户端，除非客户端有待处理的命令，
     * 或者关闭操作被取消且我们仍在 processCommand 流程中 */
    if (!(c->flags & CLIENT_PENDING_COMMAND) && c->bstate.btype != BLOCKED_SHUTDOWN) {
        freeClientOriginalArgv(c);
        /* 非按键阻塞的客户端不会被重新处理，所以我们必须在此调用
         * reqresAppendResponse（对于按键阻塞的客户端，会调用
         * unblockClientOnKey，最终调用 processCommand，
         * 再调用 reqresAppendResponse） */
        reqresAppendResponse(c);
        resetClient(c);
    }

    /* 清除标志，并将客户端放入解除阻塞列表中，
     * 以便尽快处理其查询缓冲区中的新命令。 */
    if (!(c->flags & CLIENT_MODULE)) server.blocked_clients--; /* 我们仅统计普通客户端的阻塞数，不统计模块客户端 */
    server.blocked_clients_by_type[c->bstate.btype]--;
    c->flags &= ~CLIENT_BLOCKED;
    c->bstate.btype = BLOCKED_NONE;
    c->bstate.unblock_on_nokey = 0;
    removeClientFromTimeoutTable(c);
    if (queue_for_reprocessing) queueClientForReprocessing(c);
}

/* 当被阻塞的客户端超时时调用此函数，以向其发送某种回复。
 * 此函数调用后，将以同一客户端为参数调用 unblockClient()。 */
void replyToBlockedClientTimedOut(client *c) {
    if (c->bstate.btype == BLOCKED_LAZYFREE) {
        addReply(c, shared.ok); /* lazy-free 没有失败的理由 */
    } else if (c->bstate.btype == BLOCKED_LIST ||
        c->bstate.btype == BLOCKED_ZSET ||
        c->bstate.btype == BLOCKED_STREAM) {
        addReplyNullArray(c);
        updateStatsOnUnblock(c, 0, 0, 0);
    } else if (c->bstate.btype == BLOCKED_WAIT) {
        addReplyLongLong(c,replicationCountAcksByOffset(c->bstate.reploffset));
    } else if (c->bstate.btype == BLOCKED_WAITAOF) {
        addReplyArrayLen(c,2);
        addReplyLongLong(c,server.fsynced_reploff >= c->bstate.reploffset);
        addReplyLongLong(c,replicationCountAOFAcksByOffset(c->bstate.reploffset));
    } else if (c->bstate.btype == BLOCKED_MODULE) {
        moduleBlockedClientTimedOut(c);
    } else {
        serverPanic("Unknown btype in replyToBlockedClientTimedOut().");
    }
}

/* 如果一个或多个客户端因 SHUTDOWN 命令而阻塞，此函数
 * 向它们发送错误回复并解除阻塞。 */
void replyToClientsBlockedOnShutdown(void) {
    if (server.blocked_clients_by_type[BLOCKED_SHUTDOWN] == 0) return;
    listNode *ln;
    listIter li;
    listRewind(server.clients, &li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        if (c->flags & CLIENT_BLOCKED && c->bstate.btype == BLOCKED_SHUTDOWN) {
            addReplyError(c, "Errors trying to SHUTDOWN. Check logs.");
            unblockClient(c, 1);
        }
    }
}

/* 批量解除客户端阻塞，因为实例中发生了某些变化使得阻塞不再安全。
 * 例如，在实例从主节点转变为从节点时，阻塞在列表操作中的客户端
 * 是不安全的，因此当主节点变为从节点时会调用此函数。
 *
 * 语义是向客户端发送 -UNBLOCKED 错误，同时断开连接。 */
void disconnectAllBlockedClients(void) {
    listNode *ln;
    listIter li;

    listRewind(server.clients,&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);

        if (c->flags & CLIENT_BLOCKED) {
            /* POSTPONEd 客户端是例外，当它们被解除阻塞时，
             * 命令处理将从头开始，命令将被执行或拒绝。
             * （不同于 LIST 阻塞的客户端，其命令已在进行中。 */
            if (c->bstate.btype == BLOCKED_POSTPONE)
                continue;

            if (c->bstate.btype == BLOCKED_LAZYFREE) {
                addReply(c, shared.ok); /* lazy-free 没有失败的理由 */
                updateStatsOnUnblock(c, 0, 0, 0);
                c->flags &= ~CLIENT_PENDING_COMMAND;
                unblockClient(c, 1);
            } else {

                unblockClientOnError(c,
                                     "-UNBLOCKED force unblock from blocking operation, "
                                     "instance state changed (master -> replica?)");
            }
            c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        }
    }
}

/* 此函数应在单个命令、MULTI/EXEC 块或 Lua 脚本在客户端调用后
 * 完成执行时由 Redis 调用。它处理在所有需要阻塞直到特定键
 * 可用的场景中被阻塞的客户端。
 *
 * 所有至少有一个客户端阻塞的信号就绪的键都被累积到
 * server.ready_keys 列表中。此函数将遍历该列表并相应地
 * 服务客户端。注意该函数会反复迭代（例如由于服务 BLMOVE，
 * 我们可能有新的阻塞客户端需要服务，因为 BLMOVE 的 PUSH 侧）。
 *
 * 此函数通常是"公平的"，即使用 FIFO 行为服务客户端。
 * 但在某些边缘情况下这种公平性会被违反，即当我们在同一键上
 * 同时有客户端阻塞在有序集合和列表中（客户端这样做确实很奇怪！）。
 * 因为不匹配的客户端（与当前键类型相比阻塞在不同类型上）会被
 * 移动到链表的另一端。但只要键开始只用于单一类型（几乎所有
 * Redis 应用都会这样做），该函数就已经是公平的。 */
void handleClientsBlockedOnKeys(void) {

    /* 如果我们已经在解除客户端阻塞的过程中，不应进行递归调用，
     * 以防止破坏公平性。 */
    static int in_handling_blocked_clients = 0;
    if (in_handling_blocked_clients)
        return;
    in_handling_blocked_clients = 1;

    /* 此函数仅在 also_propagate 处于基本状态时调用
     * （即不是从 call()、模块上下文等调用的） */
    serverAssert(server.also_propagate.numops == 0);

    /* 如果被解除阻塞的命令导致另一个命令也被解除阻塞
     * （如 BLMOVE），则新解除阻塞的命令将立即被处理，
     * 而不是等待稍后处理。 */
    while(listLength(server.ready_keys) != 0) {
        list *l;

        /* 将 server.ready_keys 指向一个新列表，并在本地保存当前列表。
         * 这样当我们运行旧列表时，可以自由调用 signalKeyAsReady()，
         * 它可能在处理 BLMOVE 阻塞的客户端时向 server.ready_keys
         * 推入新元素。 */
        l = server.ready_keys;
        server.ready_keys = listCreate();

        while(listLength(l) != 0) {
            listNode *ln = listFirst(l);
            readyList *rl = ln->value;

            /* 首先从 db->ready_keys 中移除此键，以便我们可以
             * 安全地对此键调用 signalKeyAsReady()。 */
            dictDelete(rl->db->ready_keys,rl->key);

            handleClientsBlockedOnKey(rl);

            /* 释放此项。 */
            decrRefCount(rl->key);
            zfree(rl);
            listDelNode(l,ln);
        }
        listRelease(l); /* 此时新列表已在原位。 */
    }
    in_handling_blocked_clients = 0;
}

/* 为指定键设置客户端的阻塞模式，使用指定的超时时间。
 * 'type' 参数为 BLOCKED_LIST、BLOCKED_ZSET 或 BLOCKED_STREAM，
 * 取决于等待空键以唤醒客户端的操作类型。客户端对 'keys' 参数中
 * 的所有 'numkeys' 个键进行阻塞。
 * 一旦 'keys' 值中的某个键被更新，客户端将被解除阻塞。
 * unblock_on_nokey 参数可用于强制客户端在键变为不可用时
 * 也被解除阻塞，无论是类型更改（覆盖）、删除还是 swapdb */
void blockForKeys(client *c, int btype, robj **keys, int numkeys, mstime_t timeout, int unblock_on_nokey) {
    dictEntry *db_blocked_entry, *db_blocked_existing_entry, *client_blocked_entry;
    list *l;
    int j;

    if (!(c->flags & CLIENT_REPROCESSING_COMMAND)) {
        /* 如果客户端正在重新处理命令，我们不设置超时，
         * 因为我们需要保留客户端的原始超时时间。 */
        c->bstate.timeout = timeout;
    }

    for (j = 0; j < numkeys; j++) {
        /* 如果键已存在于字典中，则忽略它。 */
        if (!(client_blocked_entry = dictAddRaw(c->bstate.keys,keys[j],NULL))) {
            continue;
        }
        incrRefCount(keys[j]);

        /* 在另一"侧"，映射键 -> 客户端 */
        db_blocked_entry = dictAddRaw(c->db->blocking_keys,keys[j], &db_blocked_existing_entry);

        /* 如果 key[j] 还没有阻塞的客户端，需要创建新列表 */
        if (db_blocked_entry != NULL) {
            l = listCreate();
            dictSetVal(c->db->blocking_keys, db_blocked_entry, l);
            incrRefCount(keys[j]);
        } else {
            l = dictGetVal(db_blocked_existing_entry);
        }
        listAddNodeTail(l,c);
        dictSetVal(c->bstate.keys,client_blocked_entry,listLast(l));

        /* 如果客户端希望在键被删除时被唤醒（如 XREADGROUP），
         * 我们需要将键添加到 blocking_keys_unblock_on_nokey 中 */
        if (unblock_on_nokey) {
            db_blocked_entry = dictAddRaw(c->db->blocking_keys_unblock_on_nokey, keys[j], &db_blocked_existing_entry);
            if (db_blocked_entry) {
                incrRefCount(keys[j]);
                dictSetUnsignedIntegerVal(db_blocked_entry, 1);
            } else {
                dictIncrUnsignedIntegerVal(db_blocked_existing_entry, 1);
            }
        }
    }
    c->bstate.unblock_on_nokey = unblock_on_nokey;
    /* 目前我们假设键阻塞需要重新处理命令。
     * 但对于模块来说，它们有不同的处理重新处理的方式，
     * 不需要设置待处理命令标志 */
    if (btype != BLOCKED_MODULE)
        c->flags |= CLIENT_PENDING_COMMAND;
    blockClient(c,btype);
}

/* 辅助函数，用于解除在阻塞操作（如 BLPOP）中等待的客户端的阻塞。
 * unblockClient() 的内部函数 */
static void unblockClientWaitingData(client *c) {
    dictEntry *de;
    dictIterator *di;

    if (dictSize(c->bstate.keys) == 0)
        return;

    di = dictGetIterator(c->bstate.keys);
    /* 客户端可能等待多个键，因此对每个键都解除阻塞。 */
    while((de = dictNext(di)) != NULL) {
        releaseBlockedEntry(c, de, 0);
    }
    dictReleaseIterator(di);
    dictEmpty(c->bstate.keys, NULL);
}

static blocking_type getBlockedTypeByType(int type) {
    switch (type) {
        case OBJ_LIST: return BLOCKED_LIST;
        case OBJ_ZSET: return BLOCKED_ZSET;
        case OBJ_MODULE: return BLOCKED_MODULE;
        case OBJ_STREAM: return BLOCKED_STREAM;
        default: return BLOCKED_NONE;
    }
}

/* 如果指定的键有客户端阻塞等待列表推入，此函数将把键引用
 * 放入 server.ready_keys 列表中。注意 db->ready_keys 是一个
 * 哈希表，允许我们在脚本或 MULTI/EXEC 上下文中多次推入时
 * 避免将同一键反复放入列表。
 *
 * 该列表最终将由 handleClientsBlockedOnKeys() 处理 */
static void signalKeyAsReadyLogic(redisDb *db, robj *key, int type, int deleted) {
    readyList *rl;

    /* 快速返回。 */
    int btype = getBlockedTypeByType(type);
    if (btype == BLOCKED_NONE) {
        /* 该类型永远不会阻塞。 */
        return;
    }
    if (!server.blocked_clients_by_type[btype] &&
        !server.blocked_clients_by_type[BLOCKED_MODULE]) {
        /* 没有客户端阻塞在此类型上。注意：阻塞的模块由
         * BLOCKED_MODULE 表示，即使意图是通过普通类型
         * （list、zset、stream）唤醒，因此在快速返回之前
         * 我们需要检查是否有阻塞的模块。 */
        return;
    }

    if (deleted) {
        /* 键已删除且没有客户端阻塞在此键上？无需排队。 */
        if (dictFind(db->blocking_keys_unblock_on_nokey,key) == NULL)
            return;
        /* 注意：如果我们到达这里，意味着该键也存在于 db->blocking_keys 中 */
    } else {
        /* 没有客户端阻塞在此键上？无需排队。 */
        if (dictFind(db->blocking_keys,key) == NULL)
            return;
    }

    dictEntry *de, *existing;
    de = dictAddRaw(db->ready_keys, key, &existing);
    if (de) {
        /* 我们将键添加到 db->ready_keys 字典中，
         * 以通过简单的 O(1) 检查避免将其多次添加到列表中。 */
        incrRefCount(key);
    } else {
        /* 键已被标记？无需再次排队。 */
        return;
    }

    /* 好的，我们需要将此键排入 server.ready_keys。 */
    rl = zmalloc(sizeof(*rl));
    rl->key = key;
    rl->db = db;
    incrRefCount(key);
    listAddNodeTail(server.ready_keys,rl);
}

/* 辅助函数，封装移除客户端阻塞键条目的逻辑。
 * 在这种情况下我们希望执行以下操作：
 * 1. 从全局 DB 锁定客户端列表中取消链接客户端
 * 2. 如果列表为空，从全局 db 阻塞列表中移除条目
 * 3. 如果全局列表为空，还从全局键字典中移除键，
 *    这应该触发键删除时的解除阻塞
 * 4. 从客户端阻塞键列表中移除键 - 注意，由于客户端可以阻塞
 *    在多个键上，但只在其中一个被触发时解除阻塞，我们希望
 *    避免逐个删除每个键，而是一次性清空字典。这就是提供
 *    remove_key 参数的原因，以支持 unblockClientWaitingData
 *    中的此逻辑
 */
static void releaseBlockedEntry(client *c, dictEntry *de, int remove_key) {
    list *l;
    listNode *pos;
    void *key;
    dictEntry *unblock_on_nokey_entry;

    key = dictGetKey(de);
    pos = dictGetVal(de);
    /* 从等待此键的客户端列表中移除此客户端。 */
    l = dictFetchValue(c->db->blocking_keys, key);
    serverAssertWithInfo(c,key,l != NULL);
    listUnlinkNode(l,pos);
    /* 如果列表为空，我们需要移除它以避免浪费内存。
     * 我们还将从 blocking_keys_unblock_on_nokey 字典中移除键（如果存在）。
     * 但是，如果列表不为空，我们仍将对 blocking_keys_unblock_on_nokey
     * 执行引用计数，并在引用为零时删除条目。
     * 为什么？因为可能有更多客户端阻塞在同一键上，但不要求在键删除时
     * 被触发，我们不希望这些客户端后来被 signalDeletedKeyAsReady 触发。 */
    if (listLength(l) == 0) {
        dictDelete(c->db->blocking_keys, key);
        dictDelete(c->db->blocking_keys_unblock_on_nokey,key);
    } else if (c->bstate.unblock_on_nokey) {
        unblock_on_nokey_entry = dictFind(c->db->blocking_keys_unblock_on_nokey,key);
        /* 不可能存在没有匹配条目的客户端阻塞在 nokey 上 */
        serverAssertWithInfo(c,key,unblock_on_nokey_entry != NULL);
        if (!dictIncrUnsignedIntegerVal(unblock_on_nokey_entry, -1)) {
            /* 如果计数为零，我们可以删除该条目 */
             dictDelete(c->db->blocking_keys_unblock_on_nokey,key);
        }
    }
    if (remove_key)
        dictDelete(c->bstate.keys, key);
}

void signalKeyAsReady(redisDb *db, robj *key, int type) {
    signalKeyAsReadyLogic(db, key, type, 0);
}

void signalDeletedKeyAsReady(redisDb *db, robj *key, int type) {
    signalKeyAsReadyLogic(db, key, type, 1);
}

/* handleClientsBlockedOnKeys() 的辅助函数。每当一个键就绪时
 * 调用此函数。我们遍历所有阻塞在此键上的客户端，
 * 并尝试重新执行命令（如果键仍然可用）。 */
static void handleClientsBlockedOnKey(readyList *rl) {

    /* 我们按客户端阻塞在此键上的相同顺序服务，
     * 从第一个阻塞到最后一个。 */
    dictEntry *de = dictFind(rl->db->blocking_keys,rl->key);

    if (de) {
        list *clients = dictGetVal(de);
        listNode *ln;
        listIter li;
        listRewind(clients,&li);

        /* 避免处理超过初始数量，以免在命令重新处理再次阻塞时
         * 陷入无限循环。 */
        long count = listLength(clients);
        while ((ln = listNext(&li)) && count--) {
            client *receiver = listNodeValue(ln);
            robj *o = lookupKeyReadWithFlags(rl->db, rl->key, LOOKUP_NOEFFECTS);
            /* 1. 如果新键被添加/触及，我们需要验证它满足阻塞类型，
             *    因为我们可能处理了错误的键类型。
             * 2. 我们希望服务阻塞在模块键上的客户端，
             *    无论对象类型如何：我们不知道模块当前
             *    试图完成什么。
             * 3. 在 XREADGROUP 调用的情况下，我们希望在对象类型
             *    任何变化或键被删除时解除阻塞，因为组不再有效。 */
            if ((o != NULL && (receiver->bstate.btype == getBlockedTypeByType(o->type))) ||
                (o != NULL && (receiver->bstate.btype == BLOCKED_MODULE)) ||
                (receiver->bstate.unblock_on_nokey))
            {
                if (receiver->bstate.btype != BLOCKED_MODULE)
                    unblockClientOnKey(receiver, rl->key);
                else
                    moduleUnblockClientOnKey(receiver, rl->key);
            }
        }
    }
}

/* 因 wait 命令阻塞客户端 */
void blockForReplication(client *c, mstime_t timeout, long long offset, long numreplicas) {
    c->bstate.timeout = timeout;
    c->bstate.reploffset = offset;
    c->bstate.numreplicas = numreplicas;
    listAddNodeHead(server.clients_waiting_acks,c);
    blockClient(c,BLOCKED_WAIT);
}

/* 因 waitaof 命令阻塞客户端 */
void blockForAofFsync(client *c, mstime_t timeout, long long offset, int numlocal, long numreplicas) {
    c->bstate.timeout = timeout;
    c->bstate.reploffset = offset;
    c->bstate.numreplicas = numreplicas;
    c->bstate.numlocal = numlocal;
    listAddNodeHead(server.clients_waiting_acks,c);
    blockClient(c,BLOCKED_WAITAOF);
}

/* 延迟客户端执行命令。例如服务器可能繁忙，
 * 请求避免处理客户端命令，这些命令将在服务器
 * 准备好接受时稍后处理。 */
void blockPostponeClient(client *c) {
    c->bstate.timeout = 0;
    blockClient(c,BLOCKED_POSTPONE);
    listAddNodeTail(server.postponed_clients, c);
    c->postponed_list_node = listLast(server.postponed_clients);
    /* 标记此客户端以执行其命令 */
    c->flags |= CLIENT_PENDING_COMMAND;
}

/* 因 shutdown 命令阻塞客户端 */
void blockClientShutdown(client *c) {
    blockClient(c, BLOCKED_SHUTDOWN);
}

/* 当特定键对客户端可用时解除其阻塞。
 * 此函数将从阻塞在此键上的客户端列表中移除客户端，
 * 并从客户端阻塞的键字典中移除该键。
 * 如果客户端有待处理的命令，将立即处理。 */
static void unblockClientOnKey(client *c, robj *key) {
    dictEntry *de;

    de = dictFind(c->bstate.keys, key);
    releaseBlockedEntry(c, de, 1);

    /* 仅在阻塞 API 调用的情况下，我们可能阻塞在多个键上。
       但我们应该强制解除所有阻塞键的阻塞 */
    serverAssert(c->bstate.btype == BLOCKED_STREAM ||
                c->bstate.btype == BLOCKED_LIST   ||
                c->bstate.btype == BLOCKED_ZSET);

    /* 我们需要在调用 processCommandAndResetClient 之前解除客户端阻塞，
     * 因为它会检查 CLIENT_BLOCKED 标志 */
    unblockClient(c, 0);
    /* 如果此客户端在命令期间被按键阻塞，
     * 我们需要重新处理该命令 */
    if (c->flags & CLIENT_PENDING_COMMAND) {
        c->flags &= ~CLIENT_PENDING_COMMAND;
        /* 我们希望命令处理和解除阻塞处理器（参见 RM_Call 'K' 选项）
         * 原子性地运行，这就是为什么我们必须在运行命令之前进入执行单元，
         * 并在调用解除阻塞处理器（如果存在）之后退出执行单元。
         * 注意我们还必须设置当前客户端，以便在尝试发送客户端缓存
         * 通知（在 'afterCommand' 中完成）时可用。 */
        client *old_client = server.current_client;
        server.current_client = c;
        enterExecutionUnit(1, 0);
        processCommandAndResetClient(c);
        if (!(c->flags & CLIENT_BLOCKED)) {
            if (c->flags & CLIENT_MODULE) {
                moduleCallCommandUnblockedHandler(c);
            } else {
                queueClientForReprocessing(c);
            }
        }
        exitExecutionUnit();
        afterCommand(c);
        server.current_client = old_client;
    }
}

/* 从模块上下文中解除阻塞在特定键上的客户端。
 * 此函数将尝试服务模块调用，如果成功，将客户端添加到
 * 模块解除阻塞的客户端列表中，该列表将在
 * moduleHandleBlockedClients 中处理。 */
static void moduleUnblockClientOnKey(client *c, robj *key) {
    long long prev_error_replies = server.stat_total_error_replies;
    client *old_client = server.current_client;
    server.current_client = c;
    monotime replyTimer;
    elapsedStart(&replyTimer);

    if (moduleTryServeClientBlockedOnKey(c, key)) {
        updateStatsOnUnblock(c, 0, elapsedUs(replyTimer), server.stat_total_error_replies != prev_error_replies);
        moduleUnblockClient(c);
    }
    /* 即使客户端未被解除阻塞，我们也需要调用 afterCommand，
     * 以传播在 moduleTryServeClientBlockedOnKey 内部
     * 可能做出的任何更改 */
    afterCommand(c);
    server.current_client = old_client;
}

/* 解除当前因超时而被阻塞的客户端。实现将首先以空回复
 * 响应被阻塞的客户端，或者对于模块阻塞的客户端，
 * 将使用超时回调。在这种情况下，由于我们可能有
 * 待处理的命令，我们希望移除待处理标志，
 * 以表示我们已用超时回复响应了该命令。 */
void unblockClientOnTimeout(client *c) {
    /* 客户端已被解锁（在 moduleUnblocked 列表中），尽快返回。 */
    if (c->bstate.btype == BLOCKED_MODULE && isModuleClientUnblocked(c)) return;

    replyToBlockedClientTimedOut(c);
    if (c->flags & CLIENT_PENDING_COMMAND)
        c->flags &= ~CLIENT_PENDING_COMMAND;
    unblockClient(c, 1);
}

/* 解除当前因错误而被阻塞的客户端。
 * 如果提供了 err_str，将用于回复被阻塞的客户端 */
void unblockClientOnError(client *c, const char *err_str) {
    if (err_str)
        addReplyError(c, err_str);
    updateStatsOnUnblock(c, 0, 0, 1);
    if (c->flags & CLIENT_PENDING_COMMAND)
        c->flags &= ~CLIENT_PENDING_COMMAND;
    unblockClient(c, 1);
}

void blockedBeforeSleep(void) {
    /* 处理被阻塞客户端的精确超时。 */
    handleBlockedClientsTimeout();

    /* 解除所有因 WAIT 或 WAITAOF 同步复制而阻塞的客户端。 */
    if (listLength(server.clients_waiting_acks))
        processClientsWaitingReplicas();

    /* 不定期尝试处理被阻塞的客户端。
     *
     * 示例：模块从定时器回调中调用 RM_SignalKeyAsReady
     * （所以我们根本不会访问 processCommand()）。
     *
     * 这可能会解除客户端阻塞，因此必须在
     * processUnblockedClients 之前完成 */
    handleClientsBlockedOnKeys();

    /* 检查是否有实现了阻塞命令的模块解除阻塞的客户端。 */
    if (moduleCount())
        moduleHandleBlockedClients();

    /* 尝试处理刚解除阻塞的客户端的待处理命令。 */
    if (listLength(server.unblocked_clients))
        processUnblockedClients();
}
