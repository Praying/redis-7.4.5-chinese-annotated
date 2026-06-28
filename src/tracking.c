/* tracking.c - 客户端缓存（Client Side Caching）：键追踪与失效通知
 *
 * Copyright (c) 2019-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"

/* 追踪表由一棵基数树（radix tree）构成，其中每个键对应一棵
 * 存储客户端 ID 的基数树，用于记录哪些客户端可能在本地缓存
 * 中持有某些键。
 *
 * 当客户端通过 "CLIENT TRACKING on" 启用追踪时，每个返回给
 * 该客户端的键都会被记录到该表中，建立键到客户端 ID 的映射。
 * 之后当某个键被修改时，所有可能持有该键本地副本的客户端
 * 都将收到一条失效消息。
 *
 * 客户端通常会将频繁请求的对象缓存在内存中，当收到失效消息
 * 时将其移除。 */
rax *TrackingTable = NULL;       /* 全局追踪表：键 -> 客户端 ID 集合 */
rax *PrefixTable = NULL;         /* 前缀表：用于广播模式（BCAST）的前缀匹配 */
uint64_t TrackingTableTotalItems = 0; /* 追踪表中存储的 ID 总数。
                                         可用于粗略估算服务端用于
                                         CSC 的总内存占用。 */
robj *TrackingChannelName;       /* BCAST 模式下使用的 PubSub 频道名：
                                     "__redis__:invalidate" */

/* 该结构体作为 PrefixTable 的值，表示在给定前缀下被修改的键列表
 * 以及需要被通知的客户端列表。 */
typedef struct bcastState {
    rax *keys;      /* 当前事件循环周期中被修改的键 */
    rax *clients;   /* 订阅了此前缀通知事件的客户端 */
} bcastState;

/* 移除客户端 'c' 的追踪状态。这里我们只需要递减处于追踪模式的
 * 客户端计数器，因为追踪表中只存储了客户端 ID，ID 引用会在后续
 * 被惰性移除。否则当一个在表中有大量条目的客户端被移除时，
 * 清理工作将耗费大量时间。 */
void disableTracking(client *c) {
    /* 如果该客户端处于广播模式，需要将其从所有已注册的前缀中取消订阅。 */
    if (c->flags & CLIENT_TRACKING_BCAST) {
        raxIterator ri;
        raxStart(&ri,c->client_tracking_prefixes);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            void *result;
            int found = raxFind(PrefixTable,ri.key,ri.key_len,&result);
            serverAssert(found);
            bcastState *bs = result;
            raxRemove(bs->clients,(unsigned char*)&c,sizeof(c),NULL);
            /* 如果这是最后一个客户端，则从表中移除该前缀。 */
            if (raxSize(bs->clients) == 0) {
                raxFree(bs->clients);
                raxFree(bs->keys);
                zfree(bs);
                raxRemove(PrefixTable,ri.key,ri.key_len,NULL);
            }
        }
        raxStop(&ri);
        raxFree(c->client_tracking_prefixes);
        c->client_tracking_prefixes = NULL;
    }

    /* 清除追踪相关标志并调整计数。 */
    if (c->flags & CLIENT_TRACKING) {
        server.tracking_clients--;
        c->flags &= ~(CLIENT_TRACKING|CLIENT_TRACKING_BROKEN_REDIR|
                      CLIENT_TRACKING_BCAST|CLIENT_TRACKING_OPTIN|
                      CLIENT_TRACKING_OPTOUT|CLIENT_TRACKING_CACHING|
                      CLIENT_TRACKING_NOLOOP);
    }
}

/* 检查两个字符串是否互为前缀（即其中一个是否是另一个的前缀）。
 * 用于检测前缀冲突。 */
static int stringCheckPrefix(unsigned char *s1, size_t s1_len, unsigned char *s2, size_t s2_len) {
    size_t min_length = s1_len < s2_len ? s1_len : s2_len;
    return memcmp(s1,s2,min_length) == 0;
}

/* 检查提供的前缀列表中是否存在互相冲突的情况，或者与客户端
 * 已有前缀冲突。冲突定义为：两个前缀会对同一个键触发失效通知。
 * 如果未发现前缀冲突，返回 1；否则返回 0 并向客户端发送错误消息。 */
int checkPrefixCollisionsOrReply(client *c, robj **prefixes, size_t numprefix) {
    for (size_t i = 0; i < numprefix; i++) {
        /* 检查输入列表是否与已有前缀重叠。 */
        if (c->client_tracking_prefixes) {
            raxIterator ri;
            raxStart(&ri,c->client_tracking_prefixes);
            raxSeek(&ri,"^",NULL,0);
            while(raxNext(&ri)) {
                if (stringCheckPrefix(ri.key,ri.key_len,
                    prefixes[i]->ptr,sdslen(prefixes[i]->ptr))) 
                {
                    sds collision = sdsnewlen(ri.key,ri.key_len);
                    addReplyErrorFormat(c,
                        "Prefix '%s' overlaps with an existing prefix '%s'. "
                        "Prefixes for a single client must not overlap.",
                        (unsigned char *)prefixes[i]->ptr,
                        (unsigned char *)collision);
                    sdsfree(collision);
                    raxStop(&ri);
                    return 0;
                }
            }
            raxStop(&ri);
        }
        /* 检查输入列表内部是否存在重叠。 */
        for (size_t j = i + 1; j < numprefix; j++) {
            if (stringCheckPrefix(prefixes[i]->ptr,sdslen(prefixes[i]->ptr),
                prefixes[j]->ptr,sdslen(prefixes[j]->ptr)))
            {
                addReplyErrorFormat(c,
                    "Prefix '%s' overlaps with another provided prefix '%s'. "
                    "Prefixes for a single client must not overlap.",
                    (unsigned char *)prefixes[i]->ptr,
                    (unsigned char *)prefixes[j]->ptr);
                return i;
            }
        }
    }
    return 1;
}

/* 为客户端 'c' 设置对前缀 'prefix' 的追踪。如果客户端 'c' 已经
 * 注册了该前缀，则不执行任何操作。 */
void enableBcastTrackingForPrefix(client *c, char *prefix, size_t plen) {
    void *result;
    bcastState *bs;
    /* 如果这是第一个订阅该前缀的客户端，则在表中创建该前缀条目。 */
    if (!raxFind(PrefixTable,(unsigned char*)prefix,plen,&result)) {
        bs = zmalloc(sizeof(*bs));
        bs->keys = raxNew();
        bs->clients = raxNew();
        raxInsert(PrefixTable,(unsigned char*)prefix,plen,bs,NULL);
    } else {
        bs = result;
    }
    if (raxTryInsert(bs->clients,(unsigned char*)&c,sizeof(c),NULL,NULL)) {
        if (c->client_tracking_prefixes == NULL)
            c->client_tracking_prefixes = raxNew();
        raxInsert(c->client_tracking_prefixes,
                  (unsigned char*)prefix,plen,NULL,NULL);
    }
}

/* 为客户端 'c' 启用追踪状态，同时按需分配追踪表。
 * 如果 'redirect_to' 参数非零，该客户端的失效消息将被发送到
 * 'redirect_to' 指定的客户端 ID。注意，如果重定向目标客户端
 * 最终被释放，我们会向原始客户端发送一条消息告知此情况。
 * 多个客户端可以将失效消息重定向到同一个客户端 ID。 */
void enableTracking(client *c, uint64_t redirect_to, uint64_t options, robj **prefix, size_t numprefix) {
    if (!(c->flags & CLIENT_TRACKING)) server.tracking_clients++;
    c->flags |= CLIENT_TRACKING;
    c->flags &= ~(CLIENT_TRACKING_BROKEN_REDIR|CLIENT_TRACKING_BCAST|
                  CLIENT_TRACKING_OPTIN|CLIENT_TRACKING_OPTOUT|
                  CLIENT_TRACKING_NOLOOP);
    c->client_tracking_redirection = redirect_to;

    /* 这可能是我们启用的第一个客户端，如果追踪表不存在则创建。 */
    if (TrackingTable == NULL) {
        TrackingTable = raxNew();
        PrefixTable = raxNew();
        TrackingChannelName = createStringObject("__redis__:invalidate",20);
    }

    /* 对于广播模式，在客户端中设置前缀列表。 */
    if (options & CLIENT_TRACKING_BCAST) {
        c->flags |= CLIENT_TRACKING_BCAST;
        if (numprefix == 0) enableBcastTrackingForPrefix(c,"",0);
        for (size_t j = 0; j < numprefix; j++) {
            sds sdsprefix = prefix[j]->ptr;
            enableBcastTrackingForPrefix(c,sdsprefix,sdslen(sdsprefix));
        }
    }

    /* 设置其余不需要特殊处理的标志位。 */
    c->flags |= options & (CLIENT_TRACKING_OPTIN|CLIENT_TRACKING_OPTOUT|
                           CLIENT_TRACKING_NOLOOP);
}

/* 该函数在只读命令执行后被调用，前提是客户端 'c' 启用了键追踪
 * 且追踪不在 BCAST 模式下。它会根据用户获取的键填充追踪失效表，
 * 以便 Redis 知道当某些键被修改时，哪些客户端应收到失效消息。 */
void trackingRememberKeys(client *tracking, client *executing) {
    /* 如果处于 optin/optout 模式，且 CACHING 命令的使用状态
     * 不满足条件，则直接返回。 */
    uint64_t optin = tracking->flags & CLIENT_TRACKING_OPTIN;
    uint64_t optout = tracking->flags & CLIENT_TRACKING_OPTOUT;
    uint64_t caching_given = tracking->flags & CLIENT_TRACKING_CACHING;
    if ((optin && !caching_given) || (optout && caching_given)) return;

    getKeysResult result = GETKEYS_RESULT_INIT;
    int numkeys = getKeysFromCommand(executing->cmd,executing->argv,executing->argc,&result);
    if (!numkeys) {
        getKeysFreeResult(&result);
        return;
    }
    /* 分片频道（Shard channels）被视为特殊键，供客户端库
     * 通过 `COMMAND` 命令发现需要连接的节点。
     * 这些频道不需要被追踪。 */
    if (executing->cmd->flags & CMD_PUBSUB) {
        return;
    }

    keyReference *keys = result.keys;

    for(int j = 0; j < numkeys; j++) {
        int idx = keys[j].pos;
        sds sdskey = executing->argv[idx]->ptr;
        void *result;
        rax *ids;
        if (!raxFind(TrackingTable,(unsigned char*)sdskey,sdslen(sdskey),&result)) {
            ids = raxNew();
            int inserted = raxTryInsert(TrackingTable,(unsigned char*)sdskey,
                                        sdslen(sdskey),ids, NULL);
            serverAssert(inserted == 1);
        } else {
            ids = result;
        }
        if (raxTryInsert(ids,(unsigned char*)&tracking->id,sizeof(tracking->id),NULL,NULL))
            TrackingTableTotalItems++;
    }
    getKeysFreeResult(&result);
}

/* 给定一个键名，该函数通过适当的通道（根据 RESP 版本：PubSub 或
 * Push 消息）向适当的客户端（在重定向情况下）发送失效消息，
 * 用于启用了追踪的客户端 'c'。
 *
 * 当 'proto' 参数非零时，函数将假设 'keyname' 指向一个长度为
 * 'keylen' 字节的缓冲区，其中已包含 Redis RESP 协议格式的数据。
 * 这用于以下场景：
 * - 在 BCAST 模式下，向所有适用客户端发送已失效键的数组
 * - 在 flush 命令之后，发送单个 RESP NULL 以表示所有键均已失效 */
void sendTrackingMessage(client *c, char *keyname, size_t keylen, int proto) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;

    int using_redirection = 0;
    if (c->client_tracking_redirection) {
        client *redir = lookupClientByID(c->client_tracking_redirection);
        if (!redir) {
            c->flags |= CLIENT_TRACKING_BROKEN_REDIR;
            /* 需要通知原始连接，由于重定向目标客户端已不存在，
             * 我们无法向其发送失效消息。 */
            if (c->resp > 2) {
                addReplyPushLen(c,2);
                addReplyBulkCBuffer(c,"tracking-redir-broken",21);
                addReplyLongLong(c,c->client_tracking_redirection);
            }
            if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
            return;
        }
        if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
        c = redir;
        using_redirection = 1;
        old_flags = c->flags;
        c->flags |= CLIENT_PUSHING;
    }

    /* 仅对 RESP3 及以上版本的客户端发送此类信息。但如果重定向
     * 处于激活状态，且重定向目标连接处于 Pub/Sub 模式，
     * 则也可以通过在 __redis__:invalidate 频道发送 Pub/Sub 消息
     * 来支持 RESP2。 */
    if (c->resp > 2) {
        addReplyPushLen(c,2);
        addReplyBulkCBuffer(c,"invalidate",10);
    } else if (using_redirection && c->flags & CLIENT_PUBSUB) {
        /* 使用静态对象来加速处理，前提是假设
         * addReplyPubsubMessage() 不会持有引用。 */
        addReplyPubsubMessage(c,TrackingChannelName,NULL,shared.messagebulk);
    } else {
        /* 如果执行到这里，说明客户端既不使用 RESP3，
         * 也没有重定向到另一个客户端。由于 RESP2 不支持
         * 在同一连接上发送 push 消息，因此无法向其发送任何内容。 */
        if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
        return;
    }

    /* 发送"值"部分，即键的数组。 */
    if (proto) {
        addReplyProto(c,keyname,keylen);
    } else {
        addReplyArrayLen(c,1);
        addReplyBulkCBuffer(c,keyname,keylen);
    }
    updateClientMemUsageAndBucket(c);
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* 当 Redis 中的键被修改且至少有一个客户端启用了 BCAST 模式时，
 * 该函数被调用。其目标是：如果该键匹配前缀表中的一个或多个前缀，
 * 则将其设置到对应的广播状态中。之后当返回事件循环时，
 * 会向订阅了各前缀的客户端发送失效消息。 */
void trackingRememberKeyToBroadcast(client *c, char *keyname, size_t keylen) {
    raxIterator ri;
    raxStart(&ri,PrefixTable);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        if (ri.key_len > keylen) continue;
        if (ri.key_len != 0 && memcmp(ri.key,keyname,ri.key_len) != 0)
            continue;
        bcastState *bs = ri.data;
        /* 将客户端指针作为关联值插入基数树中。这样我们可以知道
         * 最后修改该键的客户端是哪个，并在客户端处于 NOLOOP 模式
         * 时避免向其发送通知。 */
        raxInsert(bs->keys,(unsigned char*)keyname,keylen,c,NULL);
    }
    raxStop(&ri);
}

/* 该函数在 Redis 中键值发生变化时由 signalModifiedKey() 或其他
 * 地方调用。在键追踪的上下文中，我们的任务是向每个可能持有该缓存
 * 插槽中键的客户端发送通知。
 *
 * 注意 'c' 可能为 NULL，当操作在客户端修改数据库的上下文之外执行时
 * （例如因过期而删除键时）。
 *
 * 最后一个参数 'bcast' 告诉函数是否还应将该键调度为向 BCAST 模式
 * 客户端广播。当函数从 Redis 核心在键被修改后调用时为 true；
 * 但我们也会在内存压力下为了驱逐键表中的键而调用此函数：
 * 在这种情况下键并未真正改变，因此只需通知表中跟踪该键的客户端，
 * 否则它们将不知道我们已不再为其追踪该键。 */
void trackingInvalidateKey(client *c, robj *keyobj, int bcast) {
    if (TrackingTable == NULL) return;

    unsigned char *key = (unsigned char*)keyobj->ptr;
    size_t keylen = sdslen(keyobj->ptr);

    if (bcast && raxSize(PrefixTable) > 0)
        trackingRememberKeyToBroadcast(c,(char *)key,keylen);

    void *result;
    if (!raxFind(TrackingTable,key,keylen,&result)) return;
    rax *ids = result;

    raxIterator ri;
    raxStart(&ri,ids);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        uint64_t id;
        memcpy(&id,ri.key,sizeof(id));
        client *target = lookupClientByID(id);
        /* 注意，如果客户端处于 BCAST 模式，我们不想发送之前
         * 在非 BCAST 模式下积累的待处理失效消息。这种情况可能发生
         * 在客户端先以普通模式启用 TRACKING，然后切换到 BCAST 模式。 */
        if (target == NULL ||
            !(target->flags & CLIENT_TRACKING)||
            target->flags & CLIENT_TRACKING_BCAST)
        {
            continue;
        }

        /* 如果客户端启用了 NOLOOP 模式，则不发送关于该客户端
         * 自身修改的键的通知。 */
        if (target->flags & CLIENT_TRACKING_NOLOOP &&
            target == server.current_client)
        {
            continue;
        }

        /* 如果目标是当前正在执行命令的客户端，需要延迟键失效通知，
         * 因为失效消息不应与命令响应交错，应排在命令响应之后。 */
        if (target == server.current_client && (server.current_client->flags & CLIENT_EXECUTING_COMMAND)) {
            incrRefCount(keyobj);
            listAddNodeTail(server.tracking_pending_keys, keyobj);
        } else {
            sendTrackingMessage(target,(char *)keyobj->ptr,sdslen(keyobj->ptr),0);
        }
    }
    raxStop(&ri);

    /* 释放追踪表：如果该缓存插槽中有更多键被修改，
     * 会再次创建并填充基数树。 */
    TrackingTableTotalItems -= raxSize(ids);
    raxFree(ids);
    raxRemove(TrackingTable,(unsigned char*)key,keylen,NULL);
}

/* 处理待发送的键失效消息。这些消息在命令执行期间被延迟，
 * 在命令执行完毕且不在嵌套调用中时统一发送。 */
void trackingHandlePendingKeyInvalidations(void) {
    if (!listLength(server.tracking_pending_keys)) return;

    /* 仅在非嵌套调用时发送待处理的失效消息，
     * 避免消息与事务响应交错。 */
    if (server.execution_nesting) return;

    listNode *ln;
    listIter li;

    listRewind(server.tracking_pending_keys,&li);
    while ((ln = listNext(&li)) != NULL) {
        robj *key = listNodeValue(ln);
        /* current_client 可能已被释放，因此仅在
         * current_client 仍然存活时才发送失效消息 */
        if (server.current_client != NULL) {
            if (key != NULL) {
                sendTrackingMessage(server.current_client,(char *)key->ptr,sdslen(key->ptr),0);
            } else {
                sendTrackingMessage(server.current_client,shared.null[server.current_client->resp]->ptr,
                    sdslen(shared.null[server.current_client->resp]->ptr),1);
            }
        }
        if (key != NULL) decrRefCount(key);
    }
    listEmpty(server.tracking_pending_keys);
}

/* 当一个或所有 Redis 数据库被 flush 时调用此函数。缓存键不是
 * 针对每个 DB 的，而是全局的：目前的做法是向启用了追踪的客户端
 * 发送一条特殊通知（RESP NULL），表示"所有键均已失效"，
 * 以避免用大量失效消息淹没客户端。
 */
void freeTrackingRadixTreeCallback(void *rt) {
    raxFree(rt);
}

void freeTrackingRadixTree(rax *rt) {
    raxFreeWithCallback(rt,freeTrackingRadixTreeCallback);
}

/* 发送 RESP NULL 以表示所有键均已失效 */
void trackingInvalidateKeysOnFlush(int async) {
    if (server.tracking_clients) {
        listNode *ln;
        listIter li;
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = listNodeValue(ln);
            if (c->flags & CLIENT_TRACKING) {
                if (c == server.current_client) {
                    /* 使用特殊的 NULL 值标记需要在之后发送 null */
                    listAddNodeTail(server.tracking_pending_keys,NULL);
                } else {
                    sendTrackingMessage(c,shared.null[c->resp]->ptr,sdslen(shared.null[c->resp]->ptr),1);
                }
            }
        }
    }

    /* 在 FLUSHALL 的情况下，回收追踪所使用的所有内存。 */
    if (TrackingTable) {
        if (async) {
            freeTrackingRadixTreeAsync(TrackingTable);
        } else {
            freeTrackingRadixTree(TrackingTable);
        }
        TrackingTable = raxNew();
        TrackingTableTotalItems = 0;
    }
}

/* 追踪功能迫使 Redis 记住哪些客户端可能持有某些键。在读多写少
 * 的工作负载下，服务端需要记住的信息量可能非常大，且键的数量
 * 完全没有上限。
 *
 * 因此 Redis 允许用户为失效表配置最大键数。该函数确保我们不超过
 * 指定的填充率：如果超限，只需驱逐一个随机键的信息，并向客户端
 * 发送失效消息，就好像该键被修改了一样。 */
void trackingLimitUsedSlots(void) {
    static unsigned int timeout_counter = 0;
    if (TrackingTable == NULL) return;
    if (server.tracking_table_max_keys == 0) return; /* 未设置限制 */
    size_t max_keys = server.tracking_table_max_keys;
    if (raxSize(TrackingTable) <= max_keys) {
        timeout_counter = 0;
        return; /* 未达上限 */
    }

    /* 需要使一些键失效以回到限制以内。此处的工作量与我们进入
     * 此函数并发现仍超限的次数成正比。 */
    int effort = 100 * (timeout_counter+1);

    /* 通过随机游走逐个移除键。 */
    raxIterator ri;
    raxStart(&ri,TrackingTable);
    while(effort > 0) {
        effort--;
        raxSeek(&ri,"^",NULL,0);
        raxRandomWalk(&ri,0);
        if (raxEOF(&ri)) break;
        robj *keyobj = createStringObject((char*)ri.key,ri.key_len);
        trackingInvalidateKey(NULL,keyobj,0);
        decrRefCount(keyobj);
        if (raxSize(TrackingTable) <= max_keys) {
            timeout_counter = 0;
            raxStop(&ri);
            return; /* 尽快返回：已回到限制以内 */
        }
    }

    /* 如果执行到这里，说明在本次运行的最大努力下
     * 仍未能将键数降到配置限制以下。 */
    raxStop(&ri);
    timeout_counter++;
}

/* 为 'keys' 基数树中包含的所有键名生成 Redis 协议格式的数组。
 * 如果客户端不为 NULL，列表将不包含上次由该客户端修改的键，
 * 以实现 NOLOOP 选项。
 *
 * 如果结果数组为空，则返回 NULL。 */
sds trackingBuildBroadcastReply(client *c, rax *keys) {
    raxIterator ri;
    uint64_t count;

    if (c == NULL) {
        count = raxSize(keys);
    } else {
        count = 0;
        raxStart(&ri,keys);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            if (ri.data != c) count++;
        }
        raxStop(&ri);

        if (count == 0) return NULL;
    }

    /* 创建包含键列表的数组响应，然后发送给所有
     * 订阅了此前缀的客户端。 */
    char buf[32];
    size_t len = ll2string(buf,sizeof(buf),count);
    sds proto = sdsempty();
    proto = sdsMakeRoomFor(proto,count*15);
    proto = sdscatlen(proto,"*",1);
    proto = sdscatlen(proto,buf,len);
    proto = sdscatlen(proto,"\r\n",2);
    raxStart(&ri,keys);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        if (c && ri.data == c) continue;
        len = ll2string(buf,sizeof(buf),ri.key_len);
        proto = sdscatlen(proto,"$",1);
        proto = sdscatlen(proto,buf,len);
        proto = sdscatlen(proto,"\r\n",2);
        proto = sdscatlen(proto,ri.key,ri.key_len);
        proto = sdscatlen(proto,"\r\n",2);
    }
    raxStop(&ri);
    return proto;
}

/* 该函数遍历 BCAST 模式下客户端的前缀以及各前缀下被修改的键，
 * 然后向前缀下的每个客户端发送通知。 */
void trackingBroadcastInvalidationMessages(void) {
    raxIterator ri, ri2;

    /* 如果无事可做则尽快返回。 */
    if (TrackingTable == NULL || !server.tracking_clients) return;

    raxStart(&ri,PrefixTable);
    raxSeek(&ri,"^",NULL,0);

    /* 遍历每个前缀... */
    while(raxNext(&ri)) {
        bcastState *bs = ri.data;

        if (raxSize(bs->keys)) {
            /* 为所有未使用 NOLOOP 选项的客户端生成通用协议。 */
            sds proto = trackingBuildBroadcastReply(NULL,bs->keys);

            /* 将该键数组发送给列表中的每个客户端。 */
            raxStart(&ri2,bs->clients);
            raxSeek(&ri2,"^",NULL,0);
            while(raxNext(&ri2)) {
                client *c;
                memcpy(&c,ri2.key,sizeof(c));
                if (c->flags & CLIENT_TRACKING_NOLOOP) {
                    /* 该客户端可能需要排除某些键。 */
                    sds adhoc = trackingBuildBroadcastReply(c,bs->keys);
                    if (adhoc) {
                        sendTrackingMessage(c,adhoc,sdslen(adhoc),1);
                        sdsfree(adhoc);
                    }
                } else {
                    sendTrackingMessage(c,proto,sdslen(proto),1);
                }
            }
            raxStop(&ri2);

            /* 清理：可以清除此状态中的所有内容，因为我们只想
             * 追踪从现在开始累积的新键。 */
            sdsfree(proto);
        }
        raxFree(bs->keys);
        bs->keys = raxNew();
    }
    raxStop(&ri);
}

/* 用于获取追踪表中已使用的插槽数量。 */
uint64_t trackingGetTotalItems(void) {
    return TrackingTableTotalItems;
}

uint64_t trackingGetTotalKeys(void) {
    if (TrackingTable == NULL) return 0;
    return raxSize(TrackingTable);
}

uint64_t trackingGetTotalPrefixes(void) {
    if (PrefixTable == NULL) return 0;
    return raxSize(PrefixTable);
}
