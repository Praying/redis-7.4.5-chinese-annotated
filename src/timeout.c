/* Redis 客户端超时管理
 *
 * 本文件处理客户端连接超时和阻塞操作超时的检测与管理。
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "cluster.h"

#include <math.h>

/* ========================== 客户端超时处理 ============================= */

/* 检查阻塞客户端是否超时
 *
 * 若客户端当前未阻塞，则不执行任何操作，直接返回 0。
 * 若超时则发送回复、解除阻塞并返回 1。
 * 否则返回 0，不执行任何操作。 */
int checkBlockedClientTimeout(client *c, mstime_t now) {
    if (c->flags & CLIENT_BLOCKED &&
        c->bstate.timeout != 0
        && c->bstate.timeout < now)
    {
        /* 处理阻塞操作特有的超时 */
        unblockClientOnTimeout(c);
        return 1;
    } else {
        return 0;
    }
}

/* 检查客户端超时
 *
 * 若客户端被终止则返回非零值。
 * 由于此函数在循环中会被多次调用，因此接收当前时间（毫秒）作为参数，
 * 以避免每次迭代都调用 gettimeofday() 带来的性能开销。 */
int clientsCronHandleTimeout(client *c, mstime_t now_ms) {
    time_t now = now_ms/1000;

    if (server.maxidletime &&
        /* 处理空闲客户端连接超时（若已设置） */
        !(c->flags & CLIENT_SLAVE) &&   /* 从节点和监控客户端不超时 */
        !mustObeyClient(c) &&         /* 主节点和 AOF 客户端不超时 */
        !(c->flags & CLIENT_BLOCKED) && /* BLPOP 客户端不超时 */
        !(c->flags & CLIENT_PUBSUB) &&  /* Pub/Sub 客户端不超时 */
        (now - c->lastinteraction > server.maxidletime))
    {
        serverLog(LL_VERBOSE,"Closing idle client");
        freeClient(c);
        return 1;
    } else if (c->flags & CLIENT_BLOCKED) {
        /* Cluster 模式：处理被阻塞到不再由本服务器服务的键的客户端的解除阻塞和重定向 */
        if (server.cluster_enabled) {
            if (clusterRedirectBlockedClientIfNeeded(c))
                unblockClientOnError(c, NULL);
        }
    }
    return 0;
}

/* 阻塞客户端超时管理
 *
 * 我们使用 Radix 树（基数树）存储 128 位键，格式如下：
 *   [8 字节大端序过期时间] + [8 字节客户端 ID]
 *
 * Radix 树中不做任何清理：当已超时的客户端被处理时，
 * 若它们已不存在或不再以此超时阻塞，我们直接跳过。
 *
 * 每次客户端以超时设置阻塞时，我们将其添加到树中。
 * 在 beforeSleep() 中调用 handleBlockedClientsTimeout() 遍历树并解除客户端阻塞。 */

#define CLIENT_ST_KEYLEN 16    /* 8 字节 mstime + 8 字节 client ID */

/* 根据客户端 ID 和超时时间生成 Radix 树键
 *
 * 将 128 位键写入 buf 中 */
void encodeTimeoutKey(unsigned char *buf, uint64_t timeout, client *c) {
    timeout = htonu64(timeout);
    memcpy(buf,&timeout,sizeof(timeout));
    memcpy(buf+8,&c,sizeof(c));
    if (sizeof(c) == 4) memset(buf+12,0,4); /* 32 位系统补零填充 */
}

/* 解码由 encodeTimeoutKey() 编码的键
 *
 * 将超时时间写入 *toptr，客户端指针写入 *cptr */
void decodeTimeoutKey(unsigned char *buf, uint64_t *toptr, client **cptr) {
    memcpy(toptr,buf,sizeof(*toptr));
    *toptr = ntohu64(*toptr);
    memcpy(cptr,buf+8,sizeof(*cptr));
}

/* 将指定客户端 ID/超时时间添加到 Radix 树中
 *
 * 若超时时间为零（永久阻塞），则不添加该客户端 */
void addClientToTimeoutTable(client *c) {
    if (c->bstate.timeout == 0) return;
    uint64_t timeout = c->bstate.timeout;
    unsigned char buf[CLIENT_ST_KEYLEN];
    encodeTimeoutKey(buf,timeout,c);
    if (raxTryInsert(server.clients_timeout_table,buf,sizeof(buf),NULL,NULL))
        c->flags |= CLIENT_IN_TO_TABLE;
}

/* 当客户端因超时以外的原因被解除阻塞时，从表中移除该客户端 */
void removeClientFromTimeoutTable(client *c) {
    if (!(c->flags & CLIENT_IN_TO_TABLE)) return;
    c->flags &= ~CLIENT_IN_TO_TABLE;
    uint64_t timeout = c->bstate.timeout;
    unsigned char buf[CLIENT_ST_KEYLEN];
    encodeTimeoutKey(buf,timeout,c);
    raxRemove(server.clients_timeout_table,buf,sizeof(buf),NULL);
}

/* 在 beforeSleep() 中调用，解除等待超时阻塞的客户端 */
void handleBlockedClientsTimeout(void) {
    if (raxSize(server.clients_timeout_table) == 0) return;
    uint64_t now = mstime();
    raxIterator ri;
    raxStart(&ri,server.clients_timeout_table);
    raxSeek(&ri,"^",NULL,0);

    while(raxNext(&ri)) {
        uint64_t timeout;
        client *c;
        decodeTimeoutKey(ri.key,&timeout,&c);
        if (timeout >= now) break; /* 所有超时时间都在未来 */
        c->flags &= ~CLIENT_IN_TO_TABLE;
        checkBlockedClientTimeout(c,now);
        raxRemove(server.clients_timeout_table,ri.key,ri.key_len,NULL);
        raxSeek(&ri,"^",NULL,0);
    }
    raxStop(&ri);
}

/* 从对象中获取超时值并存储到 'timeout'
 *
 * 最终超时值始终以毫秒为单位存储（表示超时将在何时过期），
 * 但解析根据 'unit' 参数进行，可以是秒或毫秒。
 *
 * 注意：若超时为零（通常表示 API 中无超时），则存储到 'timeout' 的值也为零。 */
int getTimeoutFromObjectOrReply(client *c, robj *object, mstime_t *timeout, int unit) {
    long long tval;
    long double ftval;
    mstime_t now = commandTimeSnapshot();

    if (unit == UNIT_SECONDS) {
        if (getLongDoubleFromObjectOrReply(c,object,&ftval,
            "timeout is not a float or out of range") != C_OK)
            return C_ERR;

        ftval *= 1000.0;  /* 秒 => 毫秒 */
        if (ftval > LLONG_MAX) {
            addReplyError(c, "timeout is out of range");
            return C_ERR;
        }
        tval = (long long) ceill(ftval);
    } else {
        if (getLongLongFromObjectOrReply(c,object,&tval,
            "timeout is not an integer or out of range") != C_OK)
            return C_ERR;
    }

    if (tval < 0) {
        addReplyError(c,"timeout is negative");
        return C_ERR;
    }

    if (tval > 0) {
        if  (tval > LLONG_MAX - now) {
            addReplyError(c,"timeout is out of range"); /* 'tval+now' 会溢出 */
            return C_ERR;
        }
        tval += now;
    }
    *timeout = tval;

    return C_OK;
}
