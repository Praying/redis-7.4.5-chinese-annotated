/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "cluster.h"

/* 用于保存 Pub/Sub 相关元数据的结构体。
 * 目前同时用于全局 Pub/Sub 和分片 Pub/Sub 功能。 */
typedef struct pubsubtype {
    int shard;                          /* 是否为分片频道，1=是，0=否 */
    dict *(*clientPubSubChannels)(client*); /* 获取客户端订阅频道字典的函数指针 */
    int (*subscriptionCount)(client*);      /* 获取客户端订阅数量的函数指针 */
    kvstore **serverPubSubChannels;     /* 服务端级别频道 -> 客户端列表的 kvstore 指针 */
    robj **subscribeMsg;                /* 订阅响应消息（如 "subscribe"） */
    robj **unsubscribeMsg;              /* 取消订阅响应消息（如 "unsubscribe"） */
    robj **messageBulk;                 /* 消息类型标识（如 "message"） */
}pubsubtype;

/*
 * 获取客户端全局 Pub/Sub 频道的订阅数量。
 */
int clientSubscriptionsCount(client *c);

/*
 * 获取客户端分片级别 Pub/Sub 频道的订阅数量。
 */
int clientShardSubscriptionsCount(client *c);

/*
 * 获取客户端全局 Pub/Sub 频道字典。
 */
dict* getClientPubSubChannels(client *c);

/*
 * 获取客户端分片级别 Pub/Sub 频道字典。
 */
dict* getClientPubSubShardChannels(client *c);

/*
 * 获取客户端已订阅的频道列表。
 * 如果提供了模式参数，则只返回与该模式匹配的频道子集。
 */
void channelList(client *c, sds pat, kvstore *pubsub_channels);

/*
 * 全局频道的 Pub/Sub 类型实例。
 */
pubsubtype pubSubType = {
    .shard = 0,
    .clientPubSubChannels = getClientPubSubChannels,
    .subscriptionCount = clientSubscriptionsCount,
    .serverPubSubChannels = &server.pubsub_channels,
    .subscribeMsg = &shared.subscribebulk,
    .unsubscribeMsg = &shared.unsubscribebulk,
    .messageBulk = &shared.messagebulk,
};

/*
 * 分片级别频道的 Pub/Sub 类型实例，
 * 与特定哈希槽绑定。
 */
pubsubtype pubSubShardType = {
    .shard = 1,
    .clientPubSubChannels = getClientPubSubShardChannels,
    .subscriptionCount = clientShardSubscriptionsCount,
    .serverPubSubChannels = &server.pubsubshard_channels,
    .subscribeMsg = &shared.ssubscribebulk,
    .unsubscribeMsg = &shared.sunsubscribebulk,
    .messageBulk = &shared.smessagebulk,
};

/*-----------------------------------------------------------------------------
 * Pub/Sub 客户端响应 API
 *----------------------------------------------------------------------------*/

/* 向客户端发送类型为 "message" 的 Pub/Sub 消息。
 * 通常 'msg' 是包含要发送消息字符串的 Redis 对象。
 * 但如果调用者将 'msg' 设为 NULL，则可以通过
 * addReply*() 系列 API 发送特殊消息（例如数组类型）。 */
void addReplyPubsubMessage(client *c, robj *channel, robj *msg, robj *message_bulk) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,message_bulk);
    addReplyBulk(c,channel);
    if (msg) addReplyBulk(c,msg);
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* 向客户端发送类型为 "pmessage" 的 Pub/Sub 消息。
 * 与 addReplyPubsubMessage() 发送的 "message" 类型的区别是，
 * 此消息格式还包含匹配到消息的模式字符串。 */
void addReplyPubsubPatMessage(client *c, robj *pat, robj *channel, robj *msg) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[4]);
    else
        addReplyPushLen(c,4);
    addReply(c,shared.pmessagebulk);
    addReplyBulk(c,pat);
    addReplyBulk(c,channel);
    addReplyBulk(c,msg);
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* 向客户端发送 Pub/Sub 订阅确认通知。 */
void addReplyPubsubSubscribed(client *c, robj *channel, pubsubtype type) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,*type.subscribeMsg);
    addReplyBulk(c,channel);
    addReplyLongLong(c,type.subscriptionCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* 向客户端发送 Pub/Sub 取消订阅确认通知。
 * channel 可以为 NULL：当客户端执行批量取消订阅命令但没有
 * 可取消的频道时，仍然需要发送通知。 */
void addReplyPubsubUnsubscribed(client *c, robj *channel, pubsubtype type) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c, *type.unsubscribeMsg);
    if (channel)
        addReplyBulk(c,channel);
    else
        addReplyNull(c);
    addReplyLongLong(c,type.subscriptionCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* 向客户端发送模式订阅确认通知。 */
void addReplyPubsubPatSubscribed(client *c, robj *pattern) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.psubscribebulk);
    addReplyBulk(c,pattern);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* 向客户端发送模式取消订阅确认通知。
 * pattern 可以为 NULL：当客户端执行批量模式取消订阅命令
 * 但没有可取消的模式时，仍然需要发送通知。 */
void addReplyPubsubPatUnsubscribed(client *c, robj *pattern) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.punsubscribebulk);
    if (pattern)
        addReplyBulk(c,pattern);
    else
        addReplyNull(c);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/*-----------------------------------------------------------------------------
 * Pub/Sub 底层 API
 *----------------------------------------------------------------------------*/

/* 返回当前服务器处理的 Pub/Sub 频道 + 模式的总数量。 */
int serverPubsubSubscriptionCount(void) {
    return kvstoreSize(server.pubsub_channels) + dictSize(server.pubsub_patterns);
}

/* 返回当前服务器处理的分片级别 Pub/Sub 频道数量。 */
int serverPubsubShardSubscriptionCount(void) {
    return kvstoreSize(server.pubsubshard_channels);
}

/* 返回客户端订阅的频道 + 模式的总数量。 */
int clientSubscriptionsCount(client *c) {
    return dictSize(c->pubsub_channels) + dictSize(c->pubsub_patterns);
}

/* 返回客户端订阅的分片级别频道数量。 */
int clientShardSubscriptionsCount(client *c) {
    return dictSize(c->pubsubshard_channels);
}

dict* getClientPubSubChannels(client *c) {
    return c->pubsub_channels;
}

dict* getClientPubSubShardChannels(client *c) {
    return c->pubsubshard_channels;
}

/* 返回客户端订阅的全局 Pub/Sub + 分片级别频道的总数量。 */
int clientTotalPubSubSubscriptionCount(client *c) {
    return clientSubscriptionsCount(c) + clientShardSubscriptionsCount(c);
}

/* 将客户端标记为 Pub/Sub 模式，并增加服务器的 Pub/Sub 客户端计数。 */
void markClientAsPubSub(client *c) {
    if (!(c->flags & CLIENT_PUBSUB)) {
        c->flags |= CLIENT_PUBSUB;
        server.pubsub_clients++;
    }
}

/* 取消客户端的 Pub/Sub 模式标记，并减少服务器的 Pub/Sub 客户端计数。 */
void unmarkClientAsPubSub(client *c) {
    if (c->flags & CLIENT_PUBSUB) {
        c->flags &= ~CLIENT_PUBSUB;
        server.pubsub_clients--;
    }
}

/* 将客户端订阅到指定频道。操作成功返回 1，
 * 如果客户端已经订阅了该频道则返回 0。 */
int pubsubSubscribeChannel(client *c, robj *channel, pubsubtype type) {
    dictEntry *de, *existing;
    dict *clients = NULL;
    int retval = 0;
    unsigned int slot = 0;

    /* 将频道添加到客户端 -> 频道哈希表中 */
    void *position = dictFindPositionForInsert(type.clientPubSubChannels(c),channel,NULL);
    if (position) { /* 客户端尚未订阅此频道 */
        retval = 1;
        /* 将客户端添加到频道 -> 客户端列表哈希表中 */
        if (server.cluster_enabled && type.shard) {
            slot = getKeySlot(channel->ptr);
        }

        de = kvstoreDictAddRaw(*type.serverPubSubChannels, slot, channel, &existing);

        if (existing) {
            clients = dictGetVal(existing);
            channel = dictGetKey(existing);
        } else {
            clients = dictCreate(&clientDictType);
            kvstoreDictSetVal(*type.serverPubSubChannels, slot, de, clients);
            incrRefCount(channel);
        }

        serverAssert(dictAdd(clients, c, NULL) != DICT_ERR);
        serverAssert(dictInsertAtPosition(type.clientPubSubChannels(c), channel, position));
        incrRefCount(channel);
    }
    /* 通知客户端 */
    addReplyPubsubSubscribed(c,channel,type);
    return retval;
}

/* 取消客户端对指定频道的订阅。操作成功返回 1，
 * 如果客户端未订阅该频道则返回 0。 */
int pubsubUnsubscribeChannel(client *c, robj *channel, int notify, pubsubtype type) {
    dictEntry *de;
    dict *clients;
    int retval = 0;
    int slot = 0;

    /* 从客户端 -> 频道哈希表中移除该频道 */
    incrRefCount(channel); /* channel 可能只是指向哈希表中同一对象的指针，
                            此处增加引用计数以保护该对象... */
    if (dictDelete(type.clientPubSubChannels(c),channel) == DICT_OK) {
        retval = 1;
        /* 从频道 -> 客户端列表哈希表中移除该客户端 */
        if (server.cluster_enabled && type.shard) {
            slot = getKeySlot(channel->ptr);
        }
        de = kvstoreDictFind(*type.serverPubSubChannels, slot, channel);
        serverAssertWithInfo(c,NULL,de != NULL);
        clients = dictGetVal(de);
        serverAssertWithInfo(c, NULL, dictDelete(clients, c) == DICT_OK);
        if (dictSize(clients) == 0) {
            /* 如果这是最后一个客户端，则释放字典及关联的哈希表条目，
             * 这样可以防止滥用 Redis PUBSUB 命令创建海量频道。 */
            kvstoreDictDelete(*type.serverPubSubChannels, slot, channel);
        }
    }
    /* 通知客户端 */
    if (notify) {
        addReplyPubsubUnsubscribed(c,channel,type);
    }
    decrRefCount(channel); /* 此时可以安全释放引用计数 */
    return retval;
}

/* 取消指定槽位中所有分片频道的订阅。 */
void pubsubShardUnsubscribeAllChannelsInSlot(unsigned int slot) {
    if (!kvstoreDictSize(server.pubsubshard_channels, slot))
        return;

    kvstoreDictIterator *kvs_di = kvstoreGetDictSafeIterator(server.pubsubshard_channels, slot);
    dictEntry *de;
    while ((de = kvstoreDictIteratorNext(kvs_di)) != NULL) {
        robj *channel = dictGetKey(de);
        dict *clients = dictGetVal(de);
        /* 遍历订阅该频道的每个客户端，取消其订阅。 */
        dictIterator *iter = dictGetIterator(clients);
        dictEntry *entry;
        while ((entry = dictNext(iter)) != NULL) {
            client *c = dictGetKey(entry);
            int retval = dictDelete(c->pubsubshard_channels, channel);
            serverAssertWithInfo(c,channel,retval == DICT_OK);
            addReplyPubsubUnsubscribed(c, channel, pubSubShardType);
            /* 如果客户端没有其他 Pub/Sub 订阅，
             * 则退出 Pub/Sub 模式。 */
            if (clientTotalPubSubSubscriptionCount(c) == 0) {
                unmarkClientAsPubSub(c);
            }
        }
        dictReleaseIterator(iter);
        kvstoreDictDelete(server.pubsubshard_channels, slot, channel);
    }
    kvstoreReleaseDictIterator(kvs_di);
}

/* 将客户端订阅到指定模式。操作成功返回 1，
 * 如果客户端已经订阅了该模式则返回 0。 */
int pubsubSubscribePattern(client *c, robj *pattern) {
    dictEntry *de;
    dict *clients;
    int retval = 0;

    if (dictAdd(c->pubsub_patterns, pattern, NULL) == DICT_OK) {
        retval = 1;
        incrRefCount(pattern);
        /* 将客户端添加到模式 -> 客户端列表哈希表中 */
        de = dictFind(server.pubsub_patterns,pattern);
        if (de == NULL) {
            clients = dictCreate(&clientDictType);
            dictAdd(server.pubsub_patterns,pattern,clients);
            incrRefCount(pattern);
        } else {
            clients = dictGetVal(de);
        }
        serverAssert(dictAdd(clients, c, NULL) != DICT_ERR);
    }
    /* 通知客户端 */
    addReplyPubsubPatSubscribed(c,pattern);
    return retval;
}

/* 取消客户端对指定模式的订阅。操作成功返回 1，
 * 如果客户端未订阅该模式则返回 0。 */
int pubsubUnsubscribePattern(client *c, robj *pattern, int notify) {
    dictEntry *de;
    dict *clients;
    int retval = 0;

    incrRefCount(pattern); /* 保护对象，因为可能要移除的就是它本身 */
    if (dictDelete(c->pubsub_patterns, pattern) == DICT_OK) {
        retval = 1;
        /* 从模式 -> 客户端列表哈希表中移除该客户端 */
        de = dictFind(server.pubsub_patterns,pattern);
        serverAssertWithInfo(c,NULL,de != NULL);
        clients = dictGetVal(de);
        serverAssertWithInfo(c, NULL, dictDelete(clients, c) == DICT_OK);
        if (dictSize(clients) == 0) {
            /* 如果这是最后一个客户端，则释放字典及关联的哈希表条目。 */
            dictDelete(server.pubsub_patterns,pattern);
        }
    }
    /* 通知客户端 */
    if (notify) addReplyPubsubPatUnsubscribed(c,pattern);
    decrRefCount(pattern);
    return retval;
}

/* 取消客户端对所有频道的订阅。返回客户端之前订阅的频道数量。 */
int pubsubUnsubscribeAllChannelsInternal(client *c, int notify, pubsubtype type) {
    int count = 0;
    if (dictSize(type.clientPubSubChannels(c)) > 0) {
        dictIterator *di = dictGetSafeIterator(type.clientPubSubChannels(c));
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            robj *channel = dictGetKey(de);

            count += pubsubUnsubscribeChannel(c,channel,notify,type);
        }
        dictReleaseIterator(di);
    }
    /* 没有任何订阅？仍然需要回复客户端。 */
    if (notify && count == 0) {
        addReplyPubsubUnsubscribed(c,NULL,type);
    }
    return count;
}

/*
 * 取消客户端对所有全局频道的订阅。
 */
int pubsubUnsubscribeAllChannels(client *c, int notify) {
    int count = pubsubUnsubscribeAllChannelsInternal(c,notify,pubSubType);
    return count;
}

/*
 * 取消客户端对所有分片频道的订阅。
 */
int pubsubUnsubscribeShardAllChannels(client *c, int notify) {
    int count = pubsubUnsubscribeAllChannelsInternal(c, notify, pubSubShardType);
    return count;
}

/* 取消客户端对所有模式的订阅。返回客户端之前订阅的模式数量。 */
int pubsubUnsubscribeAllPatterns(client *c, int notify) {
    int count = 0;

    if (dictSize(c->pubsub_patterns) > 0) {
        dictIterator *di = dictGetSafeIterator(c->pubsub_patterns);
        dictEntry *de;

        while ((de = dictNext(di)) != NULL) {
            robj *pattern = dictGetKey(de);
            count += pubsubUnsubscribePattern(c, pattern, notify);
        }
        dictReleaseIterator(di);
    }

    /* 没有任何订阅？仍然需要回复客户端。 */
    if (notify && count == 0) addReplyPubsubPatUnsubscribed(c,NULL);
    return count;
}

/*
 * 向所有订阅者发布消息。
 */
int pubsubPublishMessageInternal(robj *channel, robj *message, pubsubtype type) {
    int receivers = 0;
    dictEntry *de;
    dictIterator *di;
    unsigned int slot = 0;

    /* 发送给正在监听该频道的客户端 */
    if (server.cluster_enabled && type.shard) {
        slot = keyHashSlot(channel->ptr, sdslen(channel->ptr));
    }
    de = kvstoreDictFind(*type.serverPubSubChannels, slot, channel);
    if (de) {
        dict *clients = dictGetVal(de);
        dictEntry *entry;
        dictIterator *iter = dictGetIterator(clients);
        while ((entry = dictNext(iter)) != NULL) {
            client *c = dictGetKey(entry);
            addReplyPubsubMessage(c,channel,message,*type.messageBulk);
            updateClientMemUsageAndBucket(c);
            receivers++;
        }
        dictReleaseIterator(iter);
    }

    if (type.shard) {
        /* 分片 Pub/Sub 不支持模式匹配。 */
        return receivers;
    }

    /* 发送给正在监听匹配频道的客户端 */
    di = dictGetIterator(server.pubsub_patterns);
    if (di) {
        channel = getDecodedObject(channel);
        while((de = dictNext(di)) != NULL) {
            robj *pattern = dictGetKey(de);
            dict *clients = dictGetVal(de);
            if (!stringmatchlen((char*)pattern->ptr,
                                sdslen(pattern->ptr),
                                (char*)channel->ptr,
                                sdslen(channel->ptr),0)) continue;

            dictEntry *entry;
            dictIterator *iter = dictGetIterator(clients);
            while ((entry = dictNext(iter)) != NULL) {
                client *c = dictGetKey(entry);
                addReplyPubsubPatMessage(c,pattern,channel,message);
                updateClientMemUsageAndBucket(c);
                receivers++;
            }
            dictReleaseIterator(iter);
        }
        decrRefCount(channel);
        dictReleaseIterator(di);
    }
    return receivers;
}

/* 向所有订阅者发布消息。 */
int pubsubPublishMessage(robj *channel, robj *message, int sharded) {
    return pubsubPublishMessageInternal(channel, message, sharded? pubSubShardType : pubSubType);
}

/*-----------------------------------------------------------------------------
 * Pub/Sub 命令实现
 *----------------------------------------------------------------------------*/

/* SUBSCRIBE channel [channel ...] */
void subscribeCommand(client *c) {
    int j;
    if ((c->flags & CLIENT_DENY_BLOCKING) && !(c->flags & CLIENT_MULTI)) {
        /**
         * 带有 CLIENT_DENY_BLOCKING 标志的客户端期望
         * 每条命令都有响应，因此不能执行 subscribe 操作。
         *
         * 注意：出于向后兼容的考虑，
         * MULTI 事务中有特殊处理。
         */
        addReplyError(c, "SUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }
    for (j = 1; j < c->argc; j++)
        pubsubSubscribeChannel(c,c->argv[j],pubSubType);
    markClientAsPubSub(c);
}

/* UNSUBSCRIBE [channel ...] */
void unsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllChannels(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribeChannel(c,c->argv[j],1,pubSubType);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

/* PSUBSCRIBE pattern [pattern ...] */
void psubscribeCommand(client *c) {
    int j;
    if ((c->flags & CLIENT_DENY_BLOCKING) && !(c->flags & CLIENT_MULTI)) {
        /**
         * 带有 CLIENT_DENY_BLOCKING 标志的客户端期望
         * 每条命令都有响应，因此不能执行 subscribe 操作。
         *
         * 注意：出于向后兼容的考虑，
         * MULTI 事务中有特殊处理。
         */
        addReplyError(c, "PSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }

    for (j = 1; j < c->argc; j++)
        pubsubSubscribePattern(c,c->argv[j]);
    markClientAsPubSub(c);
}

/* PUNSUBSCRIBE [pattern [pattern ...]] */
void punsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllPatterns(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribePattern(c,c->argv[j],1);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

/* 此函数封装了 pubsubPublishMessage，并将消息传播到集群。
 * 被 PUBLISH/SPUBLISH 命令及相应的模块 API 使用。 */
int pubsubPublishMessageAndPropagateToCluster(robj *channel, robj *message, int sharded) {
    int receivers = pubsubPublishMessage(channel, message, sharded);
    if (server.cluster_enabled)
        clusterPropagatePublish(channel, message, sharded);
    return receivers;
}

/* PUBLISH <channel> <message> */
void publishCommand(client *c) {
    if (server.sentinel_mode) {
        sentinelPublishCommand(c);
        return;
    }

    int receivers = pubsubPublishMessageAndPropagateToCluster(c->argv[1],c->argv[2],0);
    if (!server.cluster_enabled)
        forceCommandPropagation(c,PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}

/* PUBSUB 命令：用于 Pub/Sub 自省查询。 */
void pubsubCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"CHANNELS [<pattern>]",
"    Return the currently active channels matching a <pattern> (default: '*').",
"NUMPAT",
"    Return number of subscriptions to patterns.",
"NUMSUB [<channel> ...]",
"    Return the number of subscribers for the specified channels, excluding",
"    pattern subscriptions(default: no channels).",
"SHARDCHANNELS [<pattern>]",
"    Return the currently active shard level channels matching a <pattern> (default: '*').",
"SHARDNUMSUB [<shardchannel> ...]",
"    Return the number of subscribers for the specified shard level channel(s)",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"channels") &&
        (c->argc == 2 || c->argc == 3))
    {
        /* PUBSUB CHANNELS [<pattern>]：列出匹配模式的活跃频道 */
        sds pat = (c->argc == 2) ? NULL : c->argv[2]->ptr;
        channelList(c, pat, server.pubsub_channels);
    } else if (!strcasecmp(c->argv[1]->ptr,"numsub") && c->argc >= 2) {
        /* PUBSUB NUMSUB [Channel_1 ... Channel_N]：查询指定频道的订阅者数量 */
        int j;

        addReplyArrayLen(c,(c->argc-2)*2);
        for (j = 2; j < c->argc; j++) {
            dict *d = kvstoreDictFetchValue(server.pubsub_channels, 0, c->argv[j]);

            addReplyBulk(c,c->argv[j]);
            addReplyLongLong(c, d ? dictSize(d) : 0);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"numpat") && c->argc == 2) {
        /* PUBSUB NUMPAT：查询模式订阅的总数量 */
        addReplyLongLong(c,dictSize(server.pubsub_patterns));
    } else if (!strcasecmp(c->argv[1]->ptr,"shardchannels") &&
        (c->argc == 2 || c->argc == 3)) 
    {
        /* PUBSUB SHARDCHANNELS：列出匹配模式的分片频道 */
        sds pat = (c->argc == 2) ? NULL : c->argv[2]->ptr;
        channelList(c,pat,server.pubsubshard_channels);
    } else if (!strcasecmp(c->argv[1]->ptr,"shardnumsub") && c->argc >= 2) {
        /* PUBSUB SHARDNUMSUB [...]：查询分片频道的订阅者数量 */
        int j;
        addReplyArrayLen(c, (c->argc-2)*2);
        for (j = 2; j < c->argc; j++) {
            unsigned int slot = calculateKeySlot(c->argv[j]->ptr);
            dict *clients = kvstoreDictFetchValue(server.pubsubshard_channels, slot, c->argv[j]);

            addReplyBulk(c,c->argv[j]);
            addReplyLongLong(c, clients ? dictSize(clients) : 0);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* 返回客户端订阅的频道列表，可按模式过滤。 */
void channelList(client *c, sds pat, kvstore *pubsub_channels) {
    long mblen = 0;
    void *replylen;
    unsigned int slot_cnt = kvstoreNumDicts(pubsub_channels);

    replylen = addReplyDeferredLen(c);
    for (unsigned int i = 0; i < slot_cnt; i++) {
        if (!kvstoreDictSize(pubsub_channels, i))
            continue;
        kvstoreDictIterator *kvs_di = kvstoreGetDictIterator(pubsub_channels, i);
        dictEntry *de;
        while((de = kvstoreDictIteratorNext(kvs_di)) != NULL) {
            robj *cobj = dictGetKey(de);
            sds channel = cobj->ptr;

            if (!pat || stringmatchlen(pat, sdslen(pat),
                                    channel, sdslen(channel),0))
            {
                addReplyBulk(c,cobj);
                mblen++;
            }
        }
        kvstoreReleaseDictIterator(kvs_di);
    }
    setDeferredArrayLen(c,replylen,mblen);
}

/* SPUBLISH <shardchannel> <message>：向分片频道发布消息 */
void spublishCommand(client *c) {
    int receivers = pubsubPublishMessageAndPropagateToCluster(c->argv[1],c->argv[2],1);
    if (!server.cluster_enabled)
        forceCommandPropagation(c,PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}

/* SSUBSCRIBE shardchannel [shardchannel ...]：订阅分片频道 */
void ssubscribeCommand(client *c) {
    if (c->flags & CLIENT_DENY_BLOCKING) {
        /* 带有 CLIENT_DENY_BLOCKING 标志的客户端期望
         * 每条命令都有响应，因此不能执行 subscribe 操作。 */
        addReplyError(c, "SSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }

    for (int j = 1; j < c->argc; j++) {
        pubsubSubscribeChannel(c, c->argv[j], pubSubShardType);
    }
    markClientAsPubSub(c);
}

/* SUNSUBSCRIBE [shardchannel [shardchannel ...]]：取消订阅分片频道 */
void sunsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeShardAllChannels(c, 1);
    } else {
        for (int j = 1; j < c->argc; j++) {
            pubsubUnsubscribeChannel(c, c->argv[j], 1, pubSubShardType);
        }
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

/* 计算客户端 Pub/Sub 相关数据结构的内存开销。 */
size_t pubsubMemOverhead(client *c) {
    /* Pub/Sub 模式订阅的内存开销 */
    size_t mem = dictMemUsage(c->pubsub_patterns);
    /* 全局 Pub/Sub 频道的内存开销 */
    mem += dictMemUsage(c->pubsub_channels);
    /* 分片 Pub/Sub 频道的内存开销 */
    mem += dictMemUsage(c->pubsubshard_channels);
    return mem;
}

/* 返回服务器上所有 Pub/Sub 订阅的总数量（包括模式和分片频道）。 */
int pubsubTotalSubscriptions(void) {
    return dictSize(server.pubsub_patterns) +
           kvstoreSize(server.pubsub_channels) +
           kvstoreSize(server.pubsubshard_channels);
}
