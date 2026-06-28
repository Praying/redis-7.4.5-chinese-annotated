/*
 * Copyright (c) 2021-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

/*
 * 本文件实现了将客户端的请求和响应记录到文件的接口。
 * 此功能需要编译时定义 LOG_REQ_RES 宏，并通过
 * req-res-logfile 配置项开启。
 *
 * 日志格式示例：
 *
 * PING 命令：
 *
 * 4          <- 参数长度
 * ping       <- 参数内容
 * 12         <- 结束标记长度
 * __argv_end__  <- 请求参数结束标记
 * +PONG      <- RESP 格式的响应
 *
 * LRANGE 命令：
 *
 * 6
 * lrange
 * 4
 * list
 * 1
 * 0
 * 2
 * -1
 * 12
 * __argv_end__
 * *1
 * $3
 * ele
 *
 * 请求部分是从开头到 __argv_end__ 标记之间的所有内容。
 * 格式为：
 * <字符数>
 * <参数值>
 *
 * __argv_end__ 之后是响应部分，格式为 RESP
 * （2 或 3 版本，取决于客户端的配置）
 */

#include "server.h"
#include <ctype.h>

#ifdef LOG_REQ_RES

/* ----- 辅助函数 ----- */

/*
 * 判断是否应该记录该客户端的请求和响应。
 * 返回 1 表示应该记录，返回 0 表示忽略。
 */
static int reqresShouldLog(client *c) {
    /* 未配置日志文件时跳过 */
    if (!server.req_res_logfile)
        return 0;

    /* 忽略正在流式发送非标准响应的客户端
     * （发布/订阅、监控、从服务器） */
    if (c->flags & (CLIENT_PUBSUB|CLIENT_MONITOR|CLIENT_SLAVE))
        return 0;

    /* 仅对普通客户端生效，主服务器客户端不记录
     * （未实现 reqresAppendResponse 对共享从服务器
     * 缓冲区的支持） */
    if (getClientType(c) == CLIENT_TYPE_MASTER)
        return 0;

    return 1;
}

/*
 * 将指定数据追加到客户端的 reqres 缓冲区中。
 * 缓冲区空间不足时会自动扩容。
 * 返回追加的字节数。
 */
static size_t reqresAppendBuffer(client *c, void *buf, size_t len) {
    if (!c->reqres.buf) {
        /* 缓冲区尚未分配，初始容量取 len 和 1024 的较大值 */
        c->reqres.capacity = max(len, 1024);
        c->reqres.buf = zmalloc(c->reqres.capacity);
    } else if (c->reqres.capacity - c->reqres.used < len) {
        /* 剩余空间不足，扩展缓冲区容量 */
        c->reqres.capacity += len;
        c->reqres.buf = zrealloc(c->reqres.buf, c->reqres.capacity);
    }

    memcpy(c->reqres.buf + c->reqres.used, buf, len);
    c->reqres.used += len;
    return len;
}

/* ----- 请求相关函数 ----- */

/*
 * 将单个命令参数追加到 reqres 缓冲区。
 * 格式为：<参数长度>\r\n<参数内容>\r\n
 */
static size_t reqresAppendArg(client *c, char *arg, size_t arg_len) {
    char argv_len_buf[LONG_STR_SIZE];
    size_t argv_len_buf_len = ll2string(argv_len_buf,sizeof(argv_len_buf),(long)arg_len);
    size_t ret = reqresAppendBuffer(c, argv_len_buf, argv_len_buf_len);
    ret += reqresAppendBuffer(c, "\r\n", 2);
    ret += reqresAppendBuffer(c, arg, arg_len);
    ret += reqresAppendBuffer(c, "\r\n", 2);
    return ret;
}

/* ----- 对外 API ----- */


/*
 * 重置客户端内部的 clientReqResInfo 结构体，
 * 根据需要释放缓冲区。
 */
void reqresReset(client *c, int free_buf) {
    if (free_buf && c->reqres.buf)
        zfree(c->reqres.buf);
    memset(&c->reqres, 0, sizeof(c->reqres));
}

/*
 * 保存回复缓冲区（或回复链表）的偏移量。
 * 应在添加回复时调用（但由于 c->reqres.offset.saved
 * 标志，仅在首次调用时保存偏移量）。
 *
 * 核心流程：
 * 1. 客户端开始执行命令时，保存当前回复偏移量。
 * 2. 执行过程中，随着 addReply* 函数被调用，
 *    回复偏移量会增长。
 * 3. 命令执行完毕（commandProcessed）后，
 *    调用 reqresAppendResponse。
 * 4. reqresAppendResponse 追加当前偏移量与步骤 1 中
 *    保存的偏移量之间的差值（即本次命令的响应内容）。
 * 5. 在下一条命令之前重置客户端时，
 *    清除 c->reqres.offset.saved 标志，重新开始。
 *
 * 不能依赖 c->sentlen 来追踪，因为它受网络状态影响
 * （reqresAppendResponse 总是写入整个缓冲区，
 *  与 writeToClient 不同）。
 *
 * 理想情况下，这些代码可以放在 reqresAppendRequest
 * 内（由 processCommand 调用），但不能在 processCommand
 * 中保存回复偏移量，因为存在以下管道（pipeline）场景：
 *
 * set rd [redis_deferring_client]
 * set buf ""
 * append buf "SET key value\r\n"
 * append buf "BLPOP mylist 0\r\n"
 * $rd write $buf
 * $rd flush
 *
 * 假设我们在 processCommand 中保存回复偏移量：
 * 处理 BLPOP 时偏移量为 5（SET 返回的 +OK\r\n）
 * 随后 beforeSleep 被调用，+OK 写入网络，bufpos 变为 0
 * 当客户端最终解除阻塞时，缓存的偏移量是 5，但 bufpos
 * 已为 0，这样就会丢失响应的前 5 个字节。
 **/
void reqresSaveClientReplyOffset(client *c) {
    if (!reqresShouldLog(c))
        return;

    /* 仅在首次调用时保存偏移量 */
    if (c->reqres.offset.saved)
        return;

    c->reqres.offset.saved = 1;

    /* 记录静态回复缓冲区的当前写入位置 */
    c->reqres.offset.bufpos = c->bufpos;
    if (listLength(c->reply) && listNodeValue(listLast(c->reply))) {
        /* 记录回复链表中最后一个节点的索引和已用字节数 */
        c->reqres.offset.last_node.index = listLength(c->reply) - 1;
        c->reqres.offset.last_node.used = ((clientReplyBlock *)listNodeValue(listLast(c->reply)))->used;
    } else {
        /* 回复链表为空，偏移量归零 */
        c->reqres.offset.last_node.index = 0;
        c->reqres.offset.last_node.used = 0;
    }
}

/*
 * 将客户端当前命令的请求参数追加到 reqres 缓冲区。
 * 跳过部分会产生流式非标准响应的命令（如 DEBUG、
 * SUBSCRIBE 等）。
 * 返回追加的字节数。
 */
size_t reqresAppendRequest(client *c) {
    robj **argv = c->argv;
    int argc = c->argc;

    serverAssert(argc);

    if (!reqresShouldLog(c))
        return 0;

    /* 忽略会产生流式非标准响应的命令 */
    sds cmd = argv[0]->ptr;
    if (!strcasecmp(cmd,"debug") || /* DEBUG 会导致段错误 */
        !strcasecmp(cmd,"sync") ||
        !strcasecmp(cmd,"psync") ||
        !strcasecmp(cmd,"monitor") ||
        !strcasecmp(cmd,"subscribe") ||
        !strcasecmp(cmd,"unsubscribe") ||
        !strcasecmp(cmd,"ssubscribe") ||
        !strcasecmp(cmd,"sunsubscribe") ||
        !strcasecmp(cmd,"psubscribe") ||
        !strcasecmp(cmd,"punsubscribe"))
    {
        return 0;
    }

    c->reqres.argv_logged = 1; /* 标记请求参数已记录 */

    size_t ret = 0;
    /* 遍历所有参数，逐个追加到缓冲区 */
    for (int i = 0; i < argc; i++) {
        if (sdsEncodedObject(argv[i])) {
            ret += reqresAppendArg(c, argv[i]->ptr, sdslen(argv[i]->ptr));
        } else if (argv[i]->encoding == OBJ_ENCODING_INT) {
            char buf[LONG_STR_SIZE];
            size_t len = ll2string(buf,sizeof(buf),(long)argv[i]->ptr);
            ret += reqresAppendArg(c, buf, len);
        } else {
            serverPanic("Wrong encoding in reqresAppendRequest()");
        }
    }
    /* 追加请求参数结束标记 */
    return ret + reqresAppendArg(c, "__argv_end__", 12);
}

/*
 * 将客户端当前命令的响应内容追加到 reqres 缓冲区，
 * 然后将完整的请求和响应写入日志文件。
 * 返回追加的字节数。
 */
size_t reqresAppendResponse(client *c) {
    size_t ret = 0;

    if (!reqresShouldLog(c))
        return 0;

    if (!c->reqres.argv_logged) /* 例如：UNSUBSCRIBE */
        return 0;

    if (!c->reqres.offset.saved) /* 例如：模块客户端被阻塞 + CLIENT KILL */
        return 0;

    /* 首先追加静态回复缓冲区中新增的数据 */
    if (c->bufpos > c->reqres.offset.bufpos) {
        size_t written = reqresAppendBuffer(c, c->buf + c->reqres.offset.bufpos, c->bufpos - c->reqres.offset.bufpos);
        ret += written;
    }

    /* 获取回复链表的当前状态 */
    int curr_index = 0;
    size_t curr_used = 0;
    if (listLength(c->reply)) {
        curr_index = listLength(c->reply) - 1;
        curr_used = ((clientReplyBlock *)listNodeValue(listLast(c->reply)))->used;
    }

    /* 然后追加回复链表中新增的数据 */
    if (curr_index > c->reqres.offset.last_node.index ||
        curr_used > c->reqres.offset.last_node.used)
    {
        int i = 0;
        listIter iter;
        listNode *curr;
        clientReplyBlock *o;
        listRewind(c->reply, &iter);
        while ((curr = listNext(&iter)) != NULL) {
            size_t written;

            /* 跳过已经处理过的节点 */
            if (i < c->reqres.offset.last_node.index) {
                i++;
                continue;
            }
            o = listNodeValue(curr);
            if (o->used == 0) {
                i++;
                continue;
            }
            if (i == c->reqres.offset.last_node.index) {
                /* 处理可能不完整的节点——该节点中保存了
                 * 当前命令开始之前就已存在的数据，
                 * 只追加新增部分 */
                written = reqresAppendBuffer(c,
                                             o->buf + c->reqres.offset.last_node.used,
                                             o->used - c->reqres.offset.last_node.used);
            } else {
                /* 全新的节点，追加其全部数据 */
                written = reqresAppendBuffer(c, o->buf, o->used);
            }
            ret += written;
            i++;
        }
    }
    serverAssert(ret);

    /* 将请求和响应内容刷写到日志文件 */
    FILE *fp = fopen(server.req_res_logfile, "a");
    serverAssert(fp);
    fwrite(c->reqres.buf, c->reqres.used, 1, fp);
    fclose(fp);

    return ret;
}

#else /* #ifdef LOG_REQ_RES */

/* 未定义 LOG_REQ_RES 时，提供空实现以满足链接需求 */

void reqresReset(client *c, int free_buf) {
    UNUSED(c);
    UNUSED(free_buf);
}

inline void reqresSaveClientReplyOffset(client *c) {
    UNUSED(c);
}

inline size_t reqresAppendRequest(client *c) {
    UNUSED(c);
    return 0;
}

inline size_t reqresAppendResponse(client *c) {
    UNUSED(c);
    return 0;
}

#endif /* #ifdef LOG_REQ_RES */
