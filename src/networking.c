/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Copyright (c) 2024-present, Valkey contributors.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

#include "server.h"
#include "atomicvar.h"
#include "cluster.h"
#include "script.h"
#include "fpconv_dtoa.h"
#include "fmtargs.h"
#include <sys/socket.h>
#include <sys/uio.h>
#include <math.h>
#include <ctype.h>

static void setProtocolError(const char *errstr, client *c);
static void pauseClientsByClient(mstime_t end, int isPauseClientAll);
int postponeClientRead(client *c);
char *getClientSockname(client *c);
int ProcessingEventsWhileBlocked = 0; /* 参见 processEventsWhileBlocked() 函数。 */

/* 返回指定 SDS 字符串从内存分配器消耗的大小（包含内部碎片）。
 * 此函数用于计算客户端输出缓冲区的大小。 */
size_t sdsZmallocSize(sds s) {
    void *sh = sdsAllocPtr(s);
    return zmalloc_size(sh);
}

/* 返回指定 hfield（包含元数据 mstr）从内存分配器消耗的大小（包含内部碎片）。
 * 此函数用于计算客户端输出缓冲区的大小。 */
size_t hfieldZmallocSize(hfield s) {
    void *sh = hfieldGetAllocPtr(s);
    return zmalloc_size(sh);
}

/* 返回字符串对象中 object->ptr 指向的 SDS 字符串所使用的内存量（包含内部碎片）。 */
size_t getStringObjectSdsUsedMemory(robj *o) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    switch(o->encoding) {
    case OBJ_ENCODING_RAW: return sdsZmallocSize(o->ptr);
    case OBJ_ENCODING_EMBSTR: return zmalloc_size(o)-sizeof(robj);
    default: return 0; /* 目前只有整数编码。 */
    }
}

/* 返回字符串对象的长度。
 * 注意：此长度不包含内部碎片或 SDS 未使用的空间。 */
size_t getStringObjectLen(robj *o) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    switch(o->encoding) {
    case OBJ_ENCODING_RAW: return sdslen(o->ptr);
    case OBJ_ENCODING_EMBSTR: return sdslen(o->ptr);
    default: return 0; /* 目前只有整数编码。 */
    }
}

/* Client.reply 链表的复制和释放方法。 */
void *dupClientReplyValue(void *o) {
    clientReplyBlock *old = o;
    clientReplyBlock *buf = zmalloc(sizeof(clientReplyBlock) + old->size);
    memcpy(buf, o, sizeof(clientReplyBlock) + old->size);
    return buf;
}

void freeClientReplyValue(void *o) {
    zfree(o);
}

/* 将客户端链接到全局客户端链表中。
 * unlinkClient() 执行相反的操作（以及其他清理工作）。 */
void linkClient(client *c) {
    listAddNodeTail(server.clients,c);
    /* 注意：我们记住了客户端所在的链表节点，这样在 unlinkClient() 中
     * 移除客户端时不需要线性扫描，只需常数时间操作即可。 */
    c->client_list_node = listLast(server.clients);
    uint64_t id = htonu64(c->id);
    raxInsert(server.clients_index,(unsigned char*)&id,sizeof(id),c,NULL);
}

/* 初始化客户端认证状态。 */
static void clientSetDefaultAuth(client *c) {
    /* 如果默认用户不需要认证，则直接将该用户标记为已认证。 */
    c->user = DefaultUser;
    c->authenticated = (c->user->flags & USER_FLAG_NOPASS) &&
                       !(c->user->flags & USER_FLAG_DISABLED);
}

int authRequired(client *c) {
    /* 检查用户是否已认证。如果默认用户被标记为 "nopass" 且处于活动状态，
     * 则跳过此检查。 */
    int auth_required = (!(DefaultUser->flags & USER_FLAG_NOPASS) ||
                          (DefaultUser->flags & USER_FLAG_DISABLED)) &&
                        !c->authenticated;
    return auth_required;
}

client *createClient(connection *conn) {
    client *c = zmalloc(sizeof(client));

    /* 传入 NULL 作为 conn 参数可以创建一个未连接的客户端。
     * 这很有用，因为所有命令都需要在客户端上下文中执行。
     * 当命令在其他上下文中执行时（例如 Lua 脚本），我们需要一个未连接的客户端。 */
    if (conn) {
        connEnableTcpNoDelay(conn); /* 启用 TCP_NODELAY，禁用 Nagle 算法以减少延迟 */
        if (server.tcpkeepalive)
            connKeepAlive(conn,server.tcpkeepalive); /* 启用 TCP keepalive 探测 */
        connSetReadHandler(conn, readQueryFromClient); /* 设置读事件回调为 readQueryFromClient */
        connSetPrivateData(conn, c); /* 将客户端对象关联到连接的私有数据 */
    }
    c->buf = zmalloc_usable(PROTO_REPLY_CHUNK_BYTES, &c->buf_usable_size); /* 分配固定大小的回复缓冲区 */
    selectDb(c,0); /* 默认选择数据库 0 */
    uint64_t client_id;
    atomicGetIncr(server.next_client_id, client_id, 1); /* 原子递增地获取全局唯一的客户端 ID */
    c->id = client_id;
#ifdef LOG_REQ_RES
    reqresReset(c, 0);
    c->resp = server.client_default_resp;
#else
    c->resp = 2;
#endif
    c->conn = conn;                        /* 客户端对应的连接 */
    c->name = NULL;                        /* 客户端名称（可通过 CLIENT SETNAME 设置） */
    c->lib_name = NULL;                    /* 客户端使用的库名称 */
    c->lib_ver = NULL;                     /* 客户端使用的库版本 */
    c->bufpos = 0;                         /* 固定回复缓冲区中当前写入位置 */
    c->buf_peak = c->buf_usable_size;      /* 固定回复缓冲区的历史峰值 */
    c->buf_peak_last_reset_time = server.unixtime; /* 缓冲区峰值上次重置时间 */
    c->ref_repl_buf_node = NULL;           /* replica 引用的复制缓冲区节点 */
    c->ref_block_pos = 0;                  /* replica 引用的复制缓冲区块内偏移 */
    c->qb_pos = 0;                         /* 查询缓冲区中已解析的位置 */
    c->querybuf = sdsempty();              /* 查询缓冲区（SDS 字符串） */
    c->querybuf_peak = 0;                  /* 查询缓冲区的历史峰值 */
    c->reqtype = 0;                        /* 请求类型：PROTO_REQ_INLINE 或 PROTO_REQ_MULTIBULK */
    c->argc = 0;                           /* 当前命令的参数个数 */
    c->argv = NULL;                        /* 当前命令的参数数组 */
    c->argv_len = 0;                       /* argv 数组已分配的长度 */
    c->argv_len_sum = 0;                   /* argv 中所有参数的总长度 */
    c->original_argc = 0;                  /* 原始命令的参数个数（命令被重写前保存） */
    c->original_argv = NULL;               /* 原始命令的参数数组（命令被重写前保存） */
    c->cmd = c->lastcmd = c->realcmd = NULL; /* 当前命令、上一条命令、实际执行的命令 */
    c->cur_script = NULL;                  /* 当前正在执行的脚本 */
    c->multibulklen = 0;                   /* 多条批量请求中剩余待读取的参数个数 */
    c->bulklen = -1;                       /* 当前批量参数的长度（-1 表示尚未读取） */
    c->sentlen = 0;                        /* 当前回复已发送的字节数 */
    c->flags = 0;                          /* 客户端标志位 */
    c->slot = -1;                          /* 客户端正在操作的集群槽位 */
    c->ctime = c->lastinteraction = server.unixtime; /* 创建时间和最后交互时间 */
    c->duration = 0;                       /* 当前命令已执行的时长（微秒） */
    clientSetDefaultAuth(c);               /* 设置默认认证状态 */
    c->replstate = REPL_STATE_NONE;        /* 复制状态：未在复制中 */
    c->repl_start_cmd_stream_on_ack = 0;   /* 是否在收到 ACK 后开始发送命令流 */
    c->reploff = 0;                        /* 该 replica 已确认的复制偏移量 */
    c->read_reploff = 0;                   /* 该 replica 已读取的复制偏移量 */
    c->repl_applied = 0;                   /* 该 replica 已应用的复制偏移量 */
    c->repl_ack_off = 0;                   /* replica 上次 ACK 的偏移量 */
    c->repl_ack_time = 0;                  /* replica 上次 ACK 的时间 */
    c->repl_aof_off = 0;                   /* AOF 中已发送给该 replica 的偏移量 */
    c->repl_last_partial_write = 0;        /* 上次不完整写入的时间戳 */
    c->slave_listening_port = 0;           /* replica 监听端口 */
    c->slave_addr = NULL;                  /* replica 地址 */
    c->slave_capa = SLAVE_CAPA_NONE;       /* replica 能力标志 */
    c->slave_req = SLAVE_REQ_NONE;         /* replica 请求类型 */
    c->reply = listCreate();               /* 回复对象链表（动态缓冲区） */
    c->deferred_reply_errors = NULL;       /* 延迟发送的错误回复列表 */
    c->reply_bytes = 0;                    /* 回复链表中已使用的字节数 */
    c->obuf_soft_limit_reached_time = 0;   /* 输出缓冲区软限制达到的时间 */
    listSetFreeMethod(c->reply,freeClientReplyValue);   /* 设置回复链表的释放方法 */
    listSetDupMethod(c->reply,dupClientReplyValue);     /* 设置回复链表的复制方法 */
    initClientBlockingState(c);           /* 初始化客户端阻塞状态 */
    c->woff = 0;                          /* 最后一次写操作的 replication ID offset */
    c->watched_keys = listCreate();       /* WATCH 命令监视的键列表（事务用） */
    c->pubsub_channels = dictCreate(&objectKeyPointerValueDictType);    /* 订阅的频道（channel -> list of clients） */
    c->pubsub_patterns = dictCreate(&objectKeyPointerValueDictType);    /* 订阅的模式（pattern -> list of clients） */
    c->pubsubshard_channels = dictCreate(&objectKeyPointerValueDictType); /* 分片订阅的频道 */
    c->peerid = NULL;                     /* 客户端对端地址的字符串表示 */
    c->sockname = NULL;                   /* 客户端本端 socket 名称 */
    c->client_list_node = NULL;           /* 在 server.clients 链表中的节点 */
    c->postponed_list_node = NULL;        /* 延迟读取链表中的节点 */
    c->pending_read_list_node = NULL;     /* 待读取链表中的节点（IO 线程使用） */
    c->client_tracking_redirection = 0;   /* 客户端缓存追踪重定向的目标客户端 ID */
    c->client_tracking_prefixes = NULL;   /* 客户端缓存追踪的前缀列表 */
    c->last_memory_usage = 0;             /* 上次内存使用量统计 */
    c->last_memory_type = CLIENT_TYPE_NORMAL; /* 上次客户端类型（用于内存统计） */
    c->module_blocked_client = NULL;        /* 模块阻塞客户端上下文 */
    c->module_auth_ctx = NULL;             /* 模块认证上下文 */
    c->auth_callback = NULL;               /* 异步认证回调函数 */
    c->auth_callback_privdata = NULL;      /* 异步认证回调的私有数据 */
    c->auth_module = NULL;                 /* 处理认证的模块 */
    listInitNode(&c->clients_pending_write_node, c); /* 初始化待写入链表节点 */
    c->mem_usage_bucket = NULL;            /* 内存使用量桶 */
    c->mem_usage_bucket_node = NULL;       /* 在内存使用量桶链表中的节点 */
    if (conn) linkClient(c);               /* 有连接时将客户端加入全局链表 */
    initClientMultiState(c);              /* 初始化 MULTI/EXEC 事务状态 */
    return c;
}

void installClientWriteHandler(client *c) {
    int ae_barrier = 0;
    /* 对于 fsync=always 策略，我们希望同一个文件描述符不会在同一次事件循环迭代中
     * 同时进行读和写操作。这样在接收查询和向客户端发送回复之间，
     * beforeSleep() 会被调用以执行 AOF 的实际 fsync 操作到磁盘。
     * 写屏障（write barrier）确保了这一点。 */
    if (server.aof_state == AOF_ON &&
        server.aof_fsync == AOF_FSYNC_ALWAYS)
    {
        ae_barrier = 1;
    }
    if (connSetWriteHandlerWithBarrier(c->conn, sendReplyToClient, ae_barrier) == C_ERR) {
        freeClientAsync(c);
    }
}

/* 此函数将客户端加入待写入队列，等待将输出缓冲区的数据写入 socket。
 * 注意：此时并不会立即安装写事件处理器，而是先将客户端放入待写入队列中，
 * 在返回事件循环之前我们会尝试直接写入（参见 handleClientsWithPendingWrites() 函数）。
 * 如果写入失败且还有更多数据需要写入（超过 socket 缓冲区容量），
 * 才会真正安装写事件处理器。 */
void putClientInPendingWriteQueue(client *c) {
    /* 仅在尚未安排写入且（对于 replica）在当前阶段可以接收写入时，
     * 才将客户端加入写入队列。 */
    if (!(c->flags & CLIENT_PENDING_WRITE) &&
        (c->replstate == REPL_STATE_NONE ||
         (c->replstate == SLAVE_STATE_ONLINE && !c->repl_start_cmd_stream_on_ack)))
    {
        /* 这里不直接安装写事件处理器，而是标记客户端并将其放入待写入列表中。
         * 这样在重新进入事件循环之前，我们可以尝试直接写入客户端 socket，
         * 从而避免一次系统调用。只有在无法一次写入全部回复时，
         * 才会真正安装写事件处理器。 */
        c->flags |= CLIENT_PENDING_WRITE;
        listLinkNodeHead(server.clients_pending_write, &c->clients_pending_write_node);
    }
}

/* 每次向客户端发送新数据时都会调用此函数。行为如下：
 *
 * 如果客户端应该接收新数据（普通客户端会），函数返回 C_OK，
 * 并确保在事件循环中安装写事件处理器，以便 socket 可写时写入新数据。
 *
 * 如果客户端不应该接收新数据（例如用于加载 AOF 的伪客户端、master 客户端，
 * 或写事件处理器安装失败），函数返回 C_ERR。
 *
 * 在以下情况下函数可能返回 C_OK，但实际并未安装写事件处理器：
 * 1) 输出缓冲区中已有数据，事件处理器应该已经安装。
 * 2) 客户端是 replica 但尚未上线，此时只在缓冲区中累积写入，暂不实际发送。
 *
 * 通常在构建回复时调用此函数，在向客户端输出缓冲区添加更多数据之前。
 * 如果函数返回 C_ERR，则不应向输出缓冲区追加任何数据。 */
int prepareClientToWrite(client *c) {
    /* 如果是 Lua 脚本客户端，总是返回 C_OK，因为根本没有 socket。 */
    if (c->flags & (CLIENT_SCRIPT|CLIENT_MODULE)) return C_OK;

    /* 如果设置了 CLIENT_CLOSE_ASAP 标志，则无需写入任何数据。 */
    if (c->flags & CLIENT_CLOSE_ASAP) return C_ERR;

    /* CLIENT REPLY OFF / SKIP 处理：不发送回复。
     * CLIENT_PUSHING 处理：禁用回复静默标志。 */
    if ((c->flags & (CLIENT_REPLY_OFF|CLIENT_REPLY_SKIP)) &&
        !(c->flags & CLIENT_PUSHING)) return C_ERR;

    /* master 不接收回复，除非设置了 CLIENT_MASTER_FORCE_REPLY 标志。 */
    if ((c->flags & CLIENT_MASTER) &&
        !(c->flags & CLIENT_MASTER_FORCE_REPLY)) return C_ERR;

    if (!c->conn) return C_ERR; /* 用于 AOF 加载的伪客户端。 */

    /* 将客户端加入待写入队列，除非它已经有待写入的数据。
     *
     * 如果设置了 CLIENT_PENDING_READ，说明当前在 IO 线程中，
     * 不应将客户端加入待写入队列。此时会在返回后由
     * handleClientsWithPendingReadsUsingThreads() 处理。
     */
    if (!clientHasPendingReplies(c) && io_threads_op == IO_THREADS_OP_IDLE)
        putClientInPendingWriteQueue(c);

    /* 授权调用者向该客户端的输出缓冲区中追加数据。 */
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * 向输出缓冲区添加数据的底层函数。
 * -------------------------------------------------------------------------- */

/* 尝试将回复数据添加到客户端结构体中的固定缓冲区。
 * 返回实际添加到回复缓冲区的数据长度。
 *
 * Sanitizer 抑制说明：client->buf_usable_size 由 zmalloc_usable_size() 调用确定。
 * 写入超出 client->buf 边界的位置会使 sanitizer 产生误报的越界错误。 */
REDIS_NO_SANITIZE("bounds")
size_t _addReplyToBuffer(client *c, const char *s, size_t len) {
    size_t available = c->buf_usable_size - c->bufpos;

    /* 如果回复链表中已有数据，则不能再向固定缓冲区添加数据。
     * （固定缓冲区和回复链表不能混用，以保证回复顺序） */
    if (listLength(c->reply) > 0) return 0;

    size_t reply_len = len > available ? available : len;
    memcpy(c->buf+c->bufpos,s,reply_len);
    c->bufpos+=reply_len;
    /* 在向缓冲区追加回复后更新缓冲区峰值 */
    if(c->buf_peak < (size_t)c->bufpos)
        c->buf_peak = (size_t)c->bufpos;
    return reply_len;
}

/* 将回复数据添加到回复链表中。
 * 注意：此函数的部分修改需要同步到 AddReplyFromClient 函数。 */
void _addReplyProtoToList(client *c, list *reply_list, const char *s, size_t len) {
    listNode *ln = listLast(reply_list);
    clientReplyBlock *tail = ln? listNodeValue(ln): NULL;

    /* 注意：即使存在尾节点，'tail' 也可能为 NULL，因为当使用 addReplyDeferredLen() 时，
     * 它会设置一个值为 NULL 的占位节点，稍后在设置 bulk 长度时再填充。 */

    /* 尽可能追加到尾部节点 */
    if (tail) {
        /* 复制能放入尾部节点的部分，剩余部分留给新节点 */
        size_t avail = tail->size - tail->used;
        size_t copy = avail >= len? len: avail;
        memcpy(tail->buf + tail->used, s, copy);
        tail->used += copy;
        s += copy;
        len -= copy;
    }
    if (len) {
        /* 创建新节点，确保至少分配 PROTO_REPLY_CHUNK_BYTES 大小 */
        size_t usable_size;
        size_t size = len < PROTO_REPLY_CHUNK_BYTES? PROTO_REPLY_CHUNK_BYTES: len;
        tail = zmalloc_usable(size + sizeof(clientReplyBlock), &usable_size);
        /* 继承内存分配的内部碎片大小 */
        tail->size = usable_size - sizeof(clientReplyBlock);
        tail->used = len;
        memcpy(tail->buf, s, len);
        listAddNodeTail(reply_list, tail);
        c->reply_bytes += tail->size;

        closeClientOnOutputBufferLimitReached(c, 1);
    }
}

/* SUBSCRIBE / UNSUBSCRIBE 系列命令以 push 消息作为回复，
 * 换句话说，它们用一条或多条 push 消息来响应（取决于参数数量），
 * 而不是常规的回复。 */
int cmdHasPushAsReply(struct redisCommand *cmd) {
    if (!cmd) return 0;
    return cmd->proc == subscribeCommand  || cmd->proc == unsubscribeCommand ||
           cmd->proc == psubscribeCommand || cmd->proc == punsubscribeCommand ||
           cmd->proc == ssubscribeCommand || cmd->proc == sunsubscribeCommand;
}

void _addReplyToBufferOrList(client *c, const char *s, size_t len) {
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    /* replica 正常情况下不应向回复缓冲区写入数据。如果一个异常的 replica 在复制链路上
     * 发送了命令导致生成了回复，我们直接断开该连接。
     * 注意：这是检查命令是否添加了回复的最简单方式。复制链路用于写入数据而非接收回复，
     * 因此正常情况下不应该在 replica 客户端上执行到这里。 */
    if (getClientType(c) == CLIENT_TYPE_SLAVE) {
        sds cmdname = c->lastcmd ? c->lastcmd->fullname : NULL;
        logInvalidUseAndFreeClientAsync(c, "Replica generated a reply to command '%s'",
                                        cmdname ? cmdname : "<unknown>");
        return;
    }

    /* 在此处调用，因为此函数可能影响回复缓冲区偏移量（参见函数注释） */
    reqresSaveClientReplyOffset(c);

    /* 如果我们正在向当前客户端处理 push 消息（例如执行 PUBLISH 到自身订阅的频道），
     * 则希望将该消息延迟到命令回复之后添加（在 MULTI/EXEC 中尤其重要）。
     * 例外情况是 SUBSCRIBE 系列命令，它们（目前）使用 push 消息作为回复。
     * 检查 executing_client 也避免影响属于淘汰过程的 push 消息。
     * 先检查 CLIENT_PUSHING 以避免竞态条件，因为它在模块的伪客户端中不存在。 */
    if ((c->flags & CLIENT_PUSHING) && c == server.current_client &&
        server.executing_client && !cmdHasPushAsReply(server.executing_client->cmd))
    {
        _addReplyProtoToList(c,server.pending_push_messages,s,len);
        return;
    }

    size_t reply_len = _addReplyToBuffer(c,s,len);
    if (len > reply_len) _addReplyProtoToList(c,c->reply,s+reply_len,len-reply_len);
}

/* -----------------------------------------------------------------------------
 * 向客户端输出缓冲区添加数据的高层函数。
 * 以下函数是命令实现中实际调用的接口。
 * -------------------------------------------------------------------------- */

/* 将对象 'obj' 的字符串表示添加到客户端输出缓冲区。 */
void addReply(client *c, robj *obj) {
    if (prepareClientToWrite(c) != C_OK) return;

    if (sdsEncodedObject(obj)) {
        _addReplyToBufferOrList(c,obj->ptr,sdslen(obj->ptr));
    } else if (obj->encoding == OBJ_ENCODING_INT) {
        /* 对于整数编码的字符串，我们使用优化函数将其转换为字符串，
         * 然后将结果字符串附加到输出缓冲区。 */
        char buf[32];
        size_t len = ll2string(buf,sizeof(buf),(long)obj->ptr);
        _addReplyToBufferOrList(c,buf,len);
    } else {
        serverPanic("Wrong obj->encoding in addReply()");
    }
}

/* 将 SDS 字符串 's' 添加到客户端输出缓冲区，副作用是释放该 SDS 字符串。 */
void addReplySds(client *c, sds s) {
    if (prepareClientToWrite(c) != C_OK) {
        /* 调用者期望 sds 被释放。 */
        sdsfree(s);
        return;
    }
    _addReplyToBufferOrList(c,s,sdslen(s));
    sdsfree(s);
}

/* 此底层函数将任意协议数据添加到客户端缓冲区，优先尝试固定缓冲区，
 * 如果不行则使用回复对象链表。
 *
 * 该函数高效的原因是不需要时不会创建 SDS 对象或 Redis 对象。
 * 只有在无法扩展链表中现有尾部对象时，才会通过调用 _addReplyProtoToList()
 * 创建新对象。 */
void addReplyProto(client *c, const char *s, size_t len) {
    if (prepareClientToWrite(c) != C_OK) return;
    _addReplyToBufferOrList(c,s,len);
}

/* addReplyError...() 系列函数调用的底层函数。
 * 它生成以下格式的 Redis 错误协议：
 *
 * -ERRORCODE Error Message<CR><LF>
 *
 * 如果字符串 's' 中已包含错误码（以 '-' 开头），则使用提供的错误码，
 * 否则自动添加通用错误码前缀 "-ERR "。
 * 注意：'s' 不能以 \r\n 结尾。 */
void addReplyErrorLength(client *c, const char *s, size_t len) {
    /* 如果字符串已以 "-..." 开头，则错误码由调用者提供，否则使用 "-ERR"。 */
    if (!len || s[0] != '-') addReplyProto(c,"-ERR ",5);
    addReplyProto(c,s,len);
    addReplyProto(c,"\r\n",2);
}

/* 发送错误回复后执行一些操作（按需记录日志、更新统计信息等）。
 * 可用标志：
 * ERR_REPLY_FLAG_NO_STATS_UPDATE - 指示不更新任何错误统计信息。 */
void afterErrorReply(client *c, const char *s, size_t len, int flags) {
    /* 模块客户端分为两类：
     * 1. 通过 RM_Call 调用的，错误不会返回给客户端，因此不应计入统计。
     * 2. 通过模块线程安全上下文调用 RM_ReplyWithError 的，稍后会由主线程添加到真实客户端。 */
    if (c->flags & CLIENT_MODULE) {
        if (!c->deferred_reply_errors) {
            c->deferred_reply_errors = listCreate();
            listSetFreeMethod(c->deferred_reply_errors, (void (*)(void*))sdsfree);
        }
        listAddNodeTail(c->deferred_reply_errors, sdsnewlen(s, len));
        return;
    }

    if (!(flags & ERR_REPLY_FLAG_NO_STATS_UPDATE)) {
        /* 递增全局错误计数器 */
        server.stat_total_error_replies++;
        /* 递增错误统计。
         * 如果字符串已以 "-..." 开头，则错误前缀由调用者提供（搜索限制在 32 字符内），否则使用 "-ERR"。 */
        if (s[0] != '-') {
            incrementErrorCount("ERR", 3);
        } else {
            char *spaceloc = memchr(s, ' ', len < 32 ? len : 32);
            if (spaceloc) {
                const size_t errEndPos = (size_t)(spaceloc - s);
                incrementErrorCount(s+1, errEndPos-1);
            } else {
                /* 如果无法获取错误前缀，回退到 ERR */
                incrementErrorCount("ERR", 3);
            }
        }
    } else {
        /* stat_total_error_replies 不会更新，这意味着命令统计也不会更新。
         * 但我们仍希望将此命令计为失败，所以在这里更新。
         * 使用 c->realcmd 是因为 c->cmd 可能已被修改（如 GEOADD 中的情况）。 */
        c->realcmd->failed_calls++;
    }

    /* 有时 replica 向 master 回复错误是正常的，并且会调用此函数。
     * 实际上错误永远不会被发送，因为对 master 客户端调用 addReply*() 不会有任何效果。
     * 一个典型的例子是：
     *
     *    EVAL 'redis.call("incr",KEYS[1]); redis.call("nonexisting")' 1 x
     *
     * 其中 master 必须传播第一个更改，即使第二个会产生错误。
     * 不过记录这些事件仍然很有用，因为它们很少发生，可能暗示脚本中的错误或 Redis 的 bug。 */
    int ctype = getClientType(c);
    if (ctype == CLIENT_TYPE_MASTER || ctype == CLIENT_TYPE_SLAVE || c->id == CLIENT_ID_AOF) {
        char *to, *from;

        if (c->id == CLIENT_ID_AOF) {
            to = "AOF-loading-client";
            from = "server";
        } else if (ctype == CLIENT_TYPE_MASTER) {
            to = "master";
            from = "replica";
        } else {
            to = "replica";
            from = "master";
        }

        if (len > 4096) len = 4096;
        sds cmdname = c->lastcmd ? c->lastcmd->fullname : NULL;
        serverLog(LL_WARNING,"== CRITICAL == This %s is sending an error "
                             "to its %s: '%.*s' after processing the command "
                             "'%s'", from, to, (int)len, s, cmdname ? cmdname : "<unknown>");
        if (ctype == CLIENT_TYPE_MASTER && server.repl_backlog &&
            server.repl_backlog->histlen > 0)
        {
            showLatestBacklog();
        }
        server.stat_unexpected_error_replies++;

        /* 根据传播错误行为，检查是否需要在此处触发 panic。
         * 目前检查两种情况：
         * 1. 命令来自 master 且我们不是可写 replica。
         * 2. 我们正在从 AOF 文件读取。 */
        int panic_in_replicas = (ctype == CLIENT_TYPE_MASTER && server.repl_slave_ro)
            && (server.propagation_error_behavior == PROPAGATION_ERR_BEHAVIOR_PANIC ||
            server.propagation_error_behavior == PROPAGATION_ERR_BEHAVIOR_PANIC_ON_REPLICAS);
        int panic_in_aof = c->id == CLIENT_ID_AOF 
            && server.propagation_error_behavior == PROPAGATION_ERR_BEHAVIOR_PANIC;
        if (panic_in_replicas || panic_in_aof) {
            serverPanic("This %s panicked sending an error to its %s"
                " after processing the command '%s'",
                from, to, cmdname ? cmdname : "<unknown>");
        }
    }
}

/* 'err' 对象应以 -ERRORCODE 开头并以 \r\n 结尾。
 * 与依赖 addReplyErrorLength 的 addReplyErrorSds 等函数不同。 */
void addReplyErrorObject(client *c, robj *err) {
    addReply(c, err);
    afterErrorReply(c, err->ptr, sdslen(err->ptr)-2, 0); /* Ignore trailing \r\n */
}

/* 通过检查第一个字符来发送普通回复或错误回复。
 * 如果第一个字符是 '-'，则回复被视为错误。
 * 无论何种情况都会发送给定的回复，如果回复被识别为错误，
 * 还会执行一些后续操作，如记录日志和更新统计信息。 */
void addReplyOrErrorObject(client *c, robj *reply) {
    serverAssert(sdsEncodedObject(reply));
    sds rep = reply->ptr;
    if (sdslen(rep) > 1 && rep[0] == '-') {
        addReplyErrorObject(c, reply);
    } else {
        addReply(c, reply);
    }
}

/* 参见 addReplyErrorLength 了解输入字符串的格式要求。 */
void addReplyError(client *c, const char *err) {
    addReplyErrorLength(c,err,strlen(err));
    afterErrorReply(c,err,strlen(err),0);
}

/* 向指定客户端添加错误回复。
 * 支持的标志：
 * ERR_REPLY_FLAG_NO_STATS_UPDATE - 指示不执行任何错误统计更新 */
void addReplyErrorSdsEx(client *c, sds err, int flags) {
    addReplyErrorLength(c,err,sdslen(err));
    afterErrorReply(c,err,sdslen(err),flags);
    sdsfree(err);
}

/* 参见 addReplyErrorLength 了解输入字符串的格式要求。 */
/* 副作用是释放 SDS 字符串。 */
void addReplyErrorSds(client *c, sds err) {
    addReplyErrorSdsEx(c, err, 0);
}

/* 参见 addReplyErrorLength 了解输入字符串的格式要求。 */
/* 副作用是释放 SDS 字符串。 */
void addReplyErrorSdsSafe(client *c, sds err) {
    err = sdsmapchars(err, "\r\n", "  ",  2);
    addReplyErrorSdsEx(c, err, 0);
}

/* addReplyErrorFormat、addReplyErrorFormatEx 和 RM_ReplyWithErrorFormat 使用的内部函数。
 * 关于标志的更多信息请参考 afterErrorReply。 */
void addReplyErrorFormatInternal(client *c, int flags, const char *fmt, va_list ap) {
    va_list cpy;
    va_copy(cpy,ap);
    sds s = sdscatvprintf(sdsempty(),fmt,cpy);
    va_end(cpy);
    /* 修剪末尾的换行符（addReplyErrorLength 会添加换行符） */
    s = sdstrim(s, "\r\n");
    /* 确保字符串中间没有换行符，否则会产生无效的协议。 */
    s = sdsmapchars(s, "\r\n", "  ",  2);
    addReplyErrorLength(c,s,sdslen(s));
    afterErrorReply(c,s,sdslen(s),flags);
    sdsfree(s);
}

void addReplyErrorFormatEx(client *c, int flags, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    addReplyErrorFormatInternal(c, flags, fmt, ap);
    va_end(ap);
}

/* 参见 addReplyErrorLength 了解格式化字符串的要求。
 * 格式化字符串可以安全地在任意位置包含 \r 和 \n。 */
void addReplyErrorFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    addReplyErrorFormatInternal(c, 0, fmt, ap);
    va_end(ap);
}

void addReplyErrorArity(client *c) {
    addReplyErrorFormat(c, "wrong number of arguments for '%s' command",
                        c->cmd->fullname);
}

void addReplyErrorExpireTime(client *c) {
    addReplyErrorFormat(c, "invalid expire time in '%s' command",
                        c->cmd->fullname);
}

void addReplyStatusLength(client *c, const char *s, size_t len) {
    addReplyProto(c,"+",1);
    addReplyProto(c,s,len);
    addReplyProto(c,"\r\n",2);
}

void addReplyStatus(client *c, const char *status) {
    addReplyStatusLength(c,status,strlen(status));
}

void addReplyStatusFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    addReplyStatusLength(c,s,sdslen(s));
    sdsfree(s);
}

/* 有时我们被迫创建新的回复节点，无法追加到前一个节点。
 * 此时我们尝试修剪上一个回复节点末尾不再使用的空间。 */
void trimReplyUnusedTailSpace(client *c) {
    listNode *ln = listLast(c->reply);
    clientReplyBlock *tail = ln? listNodeValue(ln): NULL;

    /* 注意：即使存在尾节点，'tail' 也可能为 NULL（当使用 addReplyDeferredLen() 时） */
    if (!tail) return;

    /* 仅当未使用的空间相对较大（超过分配大小的 1/4）时才尝试修剪，
     * 否则 realloc 很可能会不执行任何操作（NOP）。
     * 另外，为避免 realloc 中发生的大量 memmove 操作，仅在已用部分较小时才执行。 */
    if (tail->size - tail->used > tail->size / 4 &&
        tail->used < PROTO_REPLY_CHUNK_BYTES)
    {
        size_t usable_size;
        size_t old_size = tail->size;
        tail = zrealloc_usable(tail, tail->used + sizeof(clientReplyBlock), &usable_size);
        /* 继承内存分配的内部碎片大小（至少用于内存使用量跟踪） */
        tail->size = usable_size - sizeof(clientReplyBlock);
        c->reply_bytes = c->reply_bytes + tail->size - old_size;
        listNodeValue(ln) = tail;
    }
}

/* 向回复链表添加一个空对象，用于稍后填充多条批量回复的长度（调用时长度未知）。 */
void *addReplyDeferredLen(client *c) {
    /* 注意：即使对象尚未准备好发送，我们也会在此安装写事件，
     * 因为我们确定在返回事件循环之前会调用 setDeferredAggregateLen()。 */
    if (prepareClientToWrite(c) != C_OK) return NULL;

    /* replica 正常情况下不应向回复缓冲区写入数据。如果一个异常的 replica 在复制链路上
     * 发送了命令导致生成了回复，我们直接断开该连接。
     * 注意：这是检查命令是否添加了回复的最简单方式。复制链路用于写入数据而非接收回复，
     * 因此正常情况下不应该在 replica 客户端上执行到这里。 */
    if (getClientType(c) == CLIENT_TYPE_SLAVE) {
        sds cmdname = c->lastcmd ? c->lastcmd->fullname : NULL;
        logInvalidUseAndFreeClientAsync(c, "Replica generated a reply to command '%s'",
                                        cmdname ? cmdname : "<unknown>");
        return NULL;
    }

    /* 在此处调用，因为此函数在概念上影响回复缓冲区偏移量（参见函数注释） */
    reqresSaveClientReplyOffset(c);

    trimReplyUnusedTailSpace(c);
    listAddNodeTail(c->reply,NULL); /* NULL is our placeholder. */
    return listLast(c->reply);
}

void setDeferredReply(client *c, void *node, const char *s, size_t length) {
    listNode *ln = (listNode*)node;
    clientReplyBlock *next, *prev;

    /* 当 *node 为 NULL 时中止：当客户端不应接受写入时，
     * addReplyDeferredLen() 返回 NULL */
    if (node == NULL) return;
    serverAssert(!listNodeValue(ln));

    /* 通常我们会用包含数组长度协议的新缓冲区结构填充 addReplyDeferredLen()
     * 添加的这个空 NULL 节点。但有时前一个/后一个节点中可能有剩余空间，
     * 我们可以删除这个 NULL 节点，将数据前缀/后缀到紧邻的节点中，
     * 以节省后续的 write(2) 系统调用。需要满足以下条件：
     *
     * - 前一个节点非 NULL 且有剩余空间，或者
     * - 后一个节点非 NULL，
     * - 已分配足够空间，
     * - 且不能太大（避免大量 memmove） */
    if (ln->prev != NULL && (prev = listNodeValue(ln->prev)) &&
        prev->size - prev->used > 0)
    {
        size_t len_to_copy = prev->size - prev->used;
        if (len_to_copy > length)
            len_to_copy = length;
        memcpy(prev->buf + prev->used, s, len_to_copy);
        prev->used += len_to_copy;
        length -= len_to_copy;
        if (length == 0) {
            listDelNode(c->reply, ln);
            return;
        }
        s += len_to_copy;
    }

    if (ln->next != NULL && (next = listNodeValue(ln->next)) &&
        next->size - next->used >= length &&
        next->used < PROTO_REPLY_CHUNK_BYTES * 4)
    {
        memmove(next->buf + length, next->buf, next->used);
        memcpy(next->buf, s, length);
        next->used += length;
        listDelNode(c->reply,ln);
    } else {
        /* Create a new node */
        size_t usable_size;
        clientReplyBlock *buf = zmalloc_usable(length + sizeof(clientReplyBlock), &usable_size);
        /* Take over the allocation's internal fragmentation */
        buf->size = usable_size - sizeof(clientReplyBlock);
        buf->used = length;
        memcpy(buf->buf, s, length);
        listNodeValue(ln) = buf;
        c->reply_bytes += buf->size;

        closeClientOnOutputBufferLimitReached(c, 1);
    }
}

/* Populate the length object and try gluing it to the next chunk. */
void setDeferredAggregateLen(client *c, void *node, long length, char prefix) {
    serverAssert(length >= 0);

    /* 当 *node 为 NULL 时中止：当客户端不应接受写入时，
     * addReplyDeferredLen() 返回 NULL */
    if (node == NULL) return;

    /* 像 *2\r\n、%3\r\n 或 ~4\r\n 这样的协议头非常常见，
     * 因此当整数较小时（大多数情况如此），我们使用预分配的共享对象。 */
    const size_t hdr_len = OBJ_SHARED_HDR_STRLEN(length);
    const int opt_hdr = length < OBJ_SHARED_BULKHDR_LEN;
    if (prefix == '*' && opt_hdr) {
        setDeferredReply(c, node, shared.mbulkhdr[length]->ptr, hdr_len);
        return;
    }
    if (prefix == '%' && opt_hdr) {
        setDeferredReply(c, node, shared.maphdr[length]->ptr, hdr_len);
        return;
    }
    if (prefix == '~' && opt_hdr) {
        setDeferredReply(c, node, shared.sethdr[length]->ptr, hdr_len);
        return;
    }

    char lenstr[128];
    size_t lenstr_len = snprintf(lenstr, sizeof(lenstr), "%c%ld\r\n", prefix, length);
    setDeferredReply(c, node, lenstr, lenstr_len);
}

void setDeferredArrayLen(client *c, void *node, long length) {
    setDeferredAggregateLen(c,node,length,'*');
}

void setDeferredMapLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    setDeferredAggregateLen(c,node,length,prefix);
}

void setDeferredSetLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '~';
    setDeferredAggregateLen(c,node,length,prefix);
}

void setDeferredAttributeLen(client *c, void *node, long length) {
    serverAssert(c->resp >= 3);
    setDeferredAggregateLen(c,node,length,'|');
}

void setDeferredPushLen(client *c, void *node, long length) {
    serverAssert(c->resp >= 3);
    setDeferredAggregateLen(c,node,length,'>');
}

/* 将 double 值作为 bulk 回复添加 */
void addReplyDouble(client *c, double d) {
    if (c->resp == 3) {
        char dbuf[MAX_D2STRING_CHARS+3];
        dbuf[0] = ',';
        const int dlen = d2string(dbuf+1,sizeof(dbuf)-1,d);
        dbuf[dlen+1] = '\r';
        dbuf[dlen+2] = '\n';
        dbuf[dlen+3] = '\0';
        addReplyProto(c,dbuf,dlen+3);
    } else {
        char dbuf[MAX_LONG_DOUBLE_CHARS+32];
        /* 为了在格式化数字前添加字符串长度前缀，同时避免额外的 memcpy 操作，
         * 我们预留最大头 `0000\r\n` 的空间，先打印 double 值，
         * 再在前面添加 RESP 头，最后以正确的 `start` 偏移量发送缓冲区。 */
        const int dlen = d2string(dbuf+7,sizeof(dbuf)-7,d);
        int digits = digits10(dlen);
        int start = 4 - digits;
        serverAssert(start >= 0);
        dbuf[start] = '$';

        /* 将 `dlen` 转换为字符串，将其数字放在 '$' 之后、格式化的 double 字符串之前。 */
        for(int i = digits, val = dlen; val && i > 0 ; --i, val /= 10) {
            dbuf[start + i] = "0123456789"[val % 10];
        }
        dbuf[5] = '\r';
        dbuf[6] = '\n';
        dbuf[dlen+7] = '\r';
        dbuf[dlen+8] = '\n';
        dbuf[dlen+9] = '\0';
        addReplyProto(c,dbuf+start,dlen+9-start);
    }
}

void addReplyBigNum(client *c, const char* num, size_t len) {
    if (c->resp == 2) {
        addReplyBulkCBuffer(c, num, len);
    } else {
        addReplyProto(c,"(",1);
        addReplyProto(c,num,len);
        addReplyProto(c,"\r\n",2);
    }
}

/* 将 long double 值作为 bulk 回复添加，但使用人类可读的格式化方式，
 * 而不是将 double 的原始行为暴露给用户。 */
void addReplyHumanLongDouble(client *c, long double d) {
    if (c->resp == 2) {
        robj *o = createStringObjectFromLongDouble(d,1);
        addReplyBulk(c,o);
        decrRefCount(o);
    } else {
        char buf[MAX_LONG_DOUBLE_CHARS];
        int len = ld2string(buf,sizeof(buf),d,LD_STR_HUMAN);
        addReplyProto(c,",",1);
        addReplyProto(c,buf,len);
        addReplyProto(c,"\r\n",2);
    }
}

/* 将 long long 值作为整数回复或 bulk 长度/多条批量计数添加。
 * 基本上用于输出 <prefix><long long><crlf> 格式。 */
static void _addReplyLongLongWithPrefix(client *c, long long ll, char prefix) {
    char buf[128];
    int len;

    /* 像 $3\r\n 或 *2\r\n 这样的协议头非常常见，
     * 因此当整数较小时（大多数情况如此），我们使用预分配的共享对象。 */
    const int opt_hdr = ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0;
    const size_t hdr_len = OBJ_SHARED_HDR_STRLEN(ll);
    if (prefix == '*' && opt_hdr) {
        _addReplyToBufferOrList(c, shared.mbulkhdr[ll]->ptr, hdr_len);
        return;
    } else if (prefix == '$' && opt_hdr) {
        _addReplyToBufferOrList(c, shared.bulkhdr[ll]->ptr, hdr_len);
        return;
    } else if (prefix == '%' && opt_hdr) {
        _addReplyToBufferOrList(c, shared.maphdr[ll]->ptr, hdr_len);
        return;
    } else if (prefix == '~' && opt_hdr) {
        _addReplyToBufferOrList(c, shared.sethdr[ll]->ptr, hdr_len);
        return;
    }

    buf[0] = prefix;
    len = ll2string(buf + 1, sizeof(buf) - 1, ll);
    buf[len + 1] = '\r';
    buf[len + 2] = '\n';
    _addReplyToBufferOrList(c, buf, len + 3);
}

void addReplyLongLong(client *c, long long ll) {
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c, shared.cone);
    else {
        if (prepareClientToWrite(c) != C_OK) return;
        _addReplyLongLongWithPrefix(c, ll, ':');
    }
}

void addReplyAggregateLen(client *c, long length, int prefix) {
    serverAssert(length >= 0);
    if (prepareClientToWrite(c) != C_OK) return;
    _addReplyLongLongWithPrefix(c, length, prefix);
}

void addReplyArrayLen(client *c, long length) {
    addReplyAggregateLen(c,length,'*');
}

void addReplyMapLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    addReplyAggregateLen(c,length,prefix);
}

void addReplySetLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '~';
    addReplyAggregateLen(c,length,prefix);
}

void addReplyAttributeLen(client *c, long length) {
    serverAssert(c->resp >= 3);
    addReplyAggregateLen(c,length,'|');
}

void addReplyPushLen(client *c, long length) {
    serverAssert(c->resp >= 3);
    serverAssertWithInfo(c, NULL, c->flags & CLIENT_PUSHING);
    addReplyAggregateLen(c,length,'>');
}

void addReplyNull(client *c) {
    if (c->resp == 2) {
        addReplyProto(c,"$-1\r\n",5);
    } else {
        addReplyProto(c,"_\r\n",3);
    }
}

void addReplyBool(client *c, int b) {
    if (c->resp == 2) {
        addReply(c, b ? shared.cone : shared.czero);
    } else {
        addReplyProto(c, b ? "#t\r\n" : "#f\r\n",4);
    }
}

/* 空数组（null array）是 RESP3 中不再存在的概念。但 RESP2 有这个概念，
 * 因此在 API 层面我们保留了此调用，它会生成正确的 RESP2 协议。
 * 对于 RESP3，回复始终是 Null 类型 "_\r\n"。 */
void addReplyNullArray(client *c) {
    if (c->resp == 2) {
        addReplyProto(c,"*-1\r\n",5);
    } else {
        addReplyProto(c,"_\r\n",3);
    }
}

/* 创建 bulk 回复的长度前缀，例如: $2234 */
void addReplyBulkLen(client *c, robj *obj) {
    size_t len = stringObjectLen(obj);
    if (prepareClientToWrite(c) != C_OK) return;
    _addReplyLongLongWithPrefix(c, len, '$');
}

/* 将 Redis 对象作为 bulk 回复添加 */
void addReplyBulk(client *c, robj *obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReplyProto(c,"\r\n",2);
}

/* 将 C 缓冲区作为 bulk 回复添加 */
void addReplyBulkCBuffer(client *c, const void *p, size_t len) {
    if (prepareClientToWrite(c) != C_OK) return;
    _addReplyLongLongWithPrefix(c, len, '$');
    _addReplyToBufferOrList(c, p, len);
    _addReplyToBufferOrList(c, "\r\n", 2);
}

/* 将 SDS 添加到回复中（获取 SDS 的所有权并释放它） */
void addReplyBulkSds(client *c, sds s) {
    if (prepareClientToWrite(c) != C_OK) {
        sdsfree(s);
        return;
    }
    _addReplyLongLongWithPrefix(c, sdslen(s), '$');
    _addReplyToBufferOrList(c, s, sdslen(s));
    sdsfree(s);
    _addReplyToBufferOrList(c, "\r\n", 2);
}

/* 将 SDS 设置为延迟回复（为与 addReplyBulkSds 对称，也会释放 SDS） */
void setDeferredReplyBulkSds(client *c, void *node, sds s) {
    sds reply = sdscatprintf(sdsempty(), "$%d\r\n%s\r\n", (unsigned)sdslen(s), s);
    setDeferredReply(c, node, reply, sdslen(reply));
    sdsfree(reply);
    sdsfree(s);
}

/* 将 C 空终止字符串作为 bulk 回复添加 */
void addReplyBulkCString(client *c, const char *s) {
    if (s == NULL) {
        addReplyNull(c);
    } else {
        addReplyBulkCBuffer(c,s,strlen(s));
    }
}

/* 将 long long 值作为 bulk 回复添加 */
void addReplyBulkLongLong(client *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}

/* 以指定扩展名的 verbatim 类型回复。
 *
 * 'ext' 是文件的"扩展名"，实际上只是一个三字符的类型标识，
 * 描述 verbatim 字符串的格式。例如 "txt" 表示接收方应将其解释为纯文本，
 * "md " 表示 Markdown 等。只使用扩展名的前三个字符，
 * 如果提供的扩展名短于三个字符，剩余部分用空格填充。 */
void addReplyVerbatim(client *c, const char *s, size_t len, const char *ext) {
    if (c->resp == 2) {
        addReplyBulkCBuffer(c,s,len);
    } else {
        char buf[32];
        size_t preflen = snprintf(buf,sizeof(buf),"=%zu\r\nxxx:",len+4);
        char *p = buf+preflen-4;
        for (int i = 0; i < 3; i++) {
            if (*ext == '\0') {
                p[i] = ' ';
            } else {
                p[i] = *ext++;
            }
        }
        addReplyProto(c,buf,preflen);
        addReplyProto(c,s,len);
        addReplyProto(c,"\r\n",2);
    }
}

/* 此函数类似于 addReplyHelp 函数，但增加了传入两个字符串数组的能力。
 * 某些命令基于 Redis 编译的特定功能实现有一些额外的子命令（目前仅限集群）。
 * 此函数允许在 `help` 中传入通用子命令，在 `extended_help` 中传入特定实现的子命令。
 */
void addExtendedReplyHelp(client *c, const char **help, const char **extended_help) {
    sds cmd = sdsnew((char*) c->argv[0]->ptr);
    void *blenp = addReplyDeferredLen(c);
    int blen = 0;
    int idx = 0;

    sdstoupper(cmd);
    addReplyStatusFormat(c,
        "%s <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",cmd);
    sdsfree(cmd);

    while (help[blen]) addReplyStatus(c,help[blen++]);
    if (extended_help) {
        while (extended_help[idx]) addReplyStatus(c,extended_help[idx++]);
    }
    blen += idx;

    addReplyStatus(c,"HELP");
    addReplyStatus(c,"    Print this help.");

    blen += 1;  /* Account for the header. */
    blen += 2;  /* Account for the footer. */
    setDeferredArrayLen(c,blenp,blen);
}

/* 添加一组 C 字符串数组作为状态回复（带标题）。
 * 此函数通常由支持子命令的命令在响应 'help' 子命令时调用。
 * help 数组以 NULL 哨兵值终止。 */
void addReplyHelp(client *c, const char **help) {
    addExtendedReplyHelp(c, help, NULL);
}

/* 添加一个提示性的错误回复。
 * 此函数通常由支持子命令的命令在遇到未知子命令或参数错误时调用。 */
void addReplySubcommandSyntaxError(client *c) {
    sds cmd = sdsnew((char*) c->argv[0]->ptr);
    sdstoupper(cmd);
    addReplyErrorFormat(c,
        "unknown subcommand or wrong number of arguments for '%.128s'. Try %s HELP.",
        (char*)c->argv[1]->ptr,cmd);
    sdsfree(cmd);
}

/* 将 'src' 客户端的输出缓冲区追加到 'dst' 客户端的输出缓冲区中。
 * 此函数会清空 'src' 的输出缓冲区。 */
void AddReplyFromClient(client *dst, client *src) {
    /* 如果源客户端因输出缓冲区限制而包含部分响应，则将该限制传播到目标客户端，
     * 而不是复制部分回复。我们不想冒险复制部分响应，
     * 以防输出限制因某种原因（比如限制已更改）做出不同的决定。 */
    if (src->flags & CLIENT_CLOSE_ASAP) {
        sds client = catClientInfoString(sdsempty(),dst);
        freeClientAsync(dst);
        serverLog(LL_WARNING,"Client %s scheduled to be closed ASAP for overcoming of output buffer limits.", client);
        sdsfree(client);
        return;
    }

    /* 首先添加固定缓冲区的数据（进入固定缓冲区或回复链表） */
    addReplyProto(dst,src->buf, src->bufpos);

    /* 需要在 addReplyProto 之后再次检查 prepareClientToWrite，
     * 因为 addReplyProto 可能改变了一些状态（如 CLIENT_CLOSE_ASAP） */
    if (prepareClientToWrite(dst) != C_OK)
        return;

    /* We're bypassing _addReplyProtoToList, so we need to add the pre/post
     * checks in it. */
    if (dst->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    /* Concatenate the reply list into the dest */
    if (listLength(src->reply))
        listJoin(dst->reply,src->reply);
    dst->reply_bytes += src->reply_bytes;
    src->reply_bytes = 0;
    src->bufpos = 0;

    if (src->deferred_reply_errors) {
        deferredAfterErrorReply(dst, src->deferred_reply_errors);
        listRelease(src->deferred_reply_errors);
        src->deferred_reply_errors = NULL;
    }

    /* Check output buffer limits */
    closeClientOnOutputBufferLimitReached(dst, 1);
}

/* Append the listed errors to the server error statistics. the input
 * list is not modified and remains the responsibility of the caller. */
void deferredAfterErrorReply(client *c, list *errors) {
    listIter li;
    listNode *ln;
    listRewind(errors,&li);
    while((ln = listNext(&li))) {
        sds err = ln->value;
        afterErrorReply(c, err, sdslen(err), 0);
    }
}

/* Logically copy 'src' replica client buffers info to 'dst' replica.
 * Basically increase referenced buffer block node reference count. */
void copyReplicaOutputBuffer(client *dst, client *src) {
    serverAssert(src->bufpos == 0 && listLength(src->reply) == 0);

    if (src->ref_repl_buf_node == NULL) return;
    dst->ref_repl_buf_node = src->ref_repl_buf_node;
    dst->ref_block_pos = src->ref_block_pos;
    ((replBufBlock *)listNodeValue(dst->ref_repl_buf_node))->refcount++;
}

/* Return true if the specified client has pending reply buffers to write to
 * the socket. */
int clientHasPendingReplies(client *c) {
    if (getClientType(c) == CLIENT_TYPE_SLAVE) {
        /* Replicas use global shared replication buffer instead of
         * private output buffer. */
        serverAssert(c->bufpos == 0 && listLength(c->reply) == 0);
        if (c->ref_repl_buf_node == NULL) return 0;

        /* If the last replication buffer block content is totally sent,
         * we have nothing to send. */
        listNode *ln = listLast(server.repl_buffer_blocks);
        replBufBlock *tail = listNodeValue(ln);
        if (ln == c->ref_repl_buf_node &&
            c->ref_block_pos == tail->used) return 0;

        return 1;
    } else {
        return c->bufpos || listLength(c->reply);
    }
}

void clientAcceptHandler(connection *conn) {
    client *c = connGetPrivateData(conn);

    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING,
                  "Error accepting a client connection: %s (addr=%s laddr=%s)",
                  connGetLastError(conn), getClientPeerId(c), getClientSockname(c));
        freeClientAsync(c);
        return;
    }

    /* If the server is running in protected mode (the default) and there
     * is no password set, nor a specific interface is bound, we don't accept
     * requests from non loopback interfaces. Instead we try to explain the
     * user what to do to fix it if needed. */
    if (server.protected_mode &&
        DefaultUser->flags & USER_FLAG_NOPASS)
    {
        if (connIsLocal(conn) != 1) {
            char *err =
                "-DENIED Redis is running in protected mode because protected "
                "mode is enabled and no password is set for the default user. "
                "In this mode connections are only accepted from the loopback interface. "
                "If you want to connect from external computers to Redis you "
                "may adopt one of the following solutions: "
                "1) Just disable protected mode sending the command "
                "'CONFIG SET protected-mode no' from the loopback interface "
                "by connecting to Redis from the same host the server is "
                "running, however MAKE SURE Redis is not publicly accessible "
                "from internet if you do so. Use CONFIG REWRITE to make this "
                "change permanent. "
                "2) Alternatively you can just disable the protected mode by "
                "editing the Redis configuration file, and setting the protected "
                "mode option to 'no', and then restarting the server. "
                "3) If you started the server manually just for testing, restart "
                "it with the '--protected-mode no' option. "
                "4) Set up an authentication password for the default user. "
                "NOTE: You only need to do one of the above things in order for "
                "the server to start accepting connections from the outside.\r\n";
            if (connWrite(c->conn,err,strlen(err)) == -1) {
                /* Nothing to do, Just to avoid the warning... */
            }
            server.stat_rejected_conn++;
            freeClientAsync(c);
            return;
        }
    }

    server.stat_numconnections++;
    moduleFireServerEvent(REDISMODULE_EVENT_CLIENT_CHANGE,
                          REDISMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED,
                          c);
}

void acceptCommonHandler(connection *conn, int flags, char *ip) {
    client *c;
    UNUSED(ip);

    if (connGetState(conn) != CONN_STATE_ACCEPTING) {
        char addr[NET_ADDR_STR_LEN] = {0};
        char laddr[NET_ADDR_STR_LEN] = {0};
        connFormatAddr(conn, addr, sizeof(addr), 1);
        connFormatAddr(conn, laddr, sizeof(addr), 0);
        serverLog(LL_VERBOSE,
                  "Accepted client connection in error state: %s (addr=%s laddr=%s)",
                  connGetLastError(conn), addr, laddr);
        connClose(conn);
        return;
    }

    /* Limit the number of connections we take at the same time.
     *
     * Admission control will happen before a client is created and connAccept()
     * called, because we don't want to even start transport-level negotiation
     * if rejected. */
    if (listLength(server.clients) + getClusterConnectionsCount()
        >= server.maxclients)
    {
        char *err;
        if (server.cluster_enabled)
            err = "-ERR max number of clients + cluster "
                  "connections reached\r\n";
        else
            err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors.
         * Note that for TLS connections, no handshake was done yet so nothing
         * is written and the connection will just drop. */
        if (connWrite(conn,err,strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        server.stat_rejected_conn++;
        connClose(conn);
        return;
    }

    /* Create connection and client */
    if ((c = createClient(conn)) == NULL) {
        char addr[NET_ADDR_STR_LEN] = {0};
        char laddr[NET_ADDR_STR_LEN] = {0};
        connFormatAddr(conn, addr, sizeof(addr), 1);
        connFormatAddr(conn, laddr, sizeof(addr), 0);
        serverLog(LL_WARNING,
                  "Error registering fd event for the new client connection: %s (addr=%s laddr=%s)",
                  connGetLastError(conn), addr, laddr);
        connClose(conn); /* May be already closed, just ignore errors */
        return;
    }

    /* Last chance to keep flags */
    c->flags |= flags;

    /* Initiate accept.
     *
     * Note that connAccept() is free to do two things here:
     * 1. Call clientAcceptHandler() immediately;
     * 2. Schedule a future call to clientAcceptHandler().
     *
     * Because of that, we must do nothing else afterwards.
     */
    if (connAccept(conn, clientAcceptHandler) == C_ERR) {
        if (connGetState(conn) == CONN_STATE_ERROR)
            serverLog(LL_WARNING,
                      "Error accepting a client connection: %s (addr=%s laddr=%s)",
                      connGetLastError(conn), getClientPeerId(c), getClientSockname(c));
        freeClient(connGetPrivateData(conn));
        return;
    }
}

void freeClientOriginalArgv(client *c) {
    /* We didn't rewrite this client */
    if (!c->original_argv) return;

    for (int j = 0; j < c->original_argc; j++)
        decrRefCount(c->original_argv[j]);
    zfree(c->original_argv);
    c->original_argv = NULL;
    c->original_argc = 0;
}

void freeClientArgv(client *c) {
    int j;
    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    c->argc = 0;
    c->cmd = NULL;
    c->argv_len_sum = 0;
    c->argv_len = 0;
    zfree(c->argv);
    c->argv = NULL;
}

/* Close all the slaves connections. This is useful in chained replication
 * when we resync with our own master and want to force all our slaves to
 * resync with us as well. */
void disconnectSlaves(void) {
    listIter li;
    listNode *ln;
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        freeClient((client*)ln->value);
    }
}

/* Check if there is any other slave waiting dumping RDB finished expect me.
 * This function is useful to judge current dumping RDB can be used for full
 * synchronization or not. */
int anyOtherSlaveWaitRdb(client *except_me) {
    listIter li;
    listNode *ln;

    listRewind(server.slaves, &li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        if (slave != except_me &&
            slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END)
        {
            return 1;
        }
    }
    return 0;
}

/* Remove the specified client from global lists where the client could
 * be referenced, not including the Pub/Sub channels.
 * This is used by freeClient() and replicationCacheMaster(). */
void unlinkClient(client *c) {
    listNode *ln;

    /* If this is marked as current client unset it. */
    if (c->conn && server.current_client == c) server.current_client = NULL;

    /* Certain operations must be done only if the client has an active connection.
     * If the client was already unlinked or if it's a "fake client" the
     * conn is already set to NULL. */
    if (c->conn) {
        /* Remove from the list of active clients. */
        if (c->client_list_node) {
            uint64_t id = htonu64(c->id);
            raxRemove(server.clients_index,(unsigned char*)&id,sizeof(id),NULL);
            listDelNode(server.clients,c->client_list_node);
            c->client_list_node = NULL;
        }

        /* Check if this is a replica waiting for diskless replication (rdb pipe),
         * in which case it needs to be cleaned from that list */
        if (c->flags & CLIENT_SLAVE &&
            c->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
            server.rdb_pipe_conns)
        {
            int i;
            for (i=0; i < server.rdb_pipe_numconns; i++) {
                if (server.rdb_pipe_conns[i] == c->conn) {
                    rdbPipeWriteHandlerConnRemoved(c->conn);
                    server.rdb_pipe_conns[i] = NULL;
                    break;
                }
            }
        }
        /* Only use shutdown when the fork is active and we are the parent. */
        if (server.child_type) connShutdown(c->conn);
        connClose(c->conn);
        c->conn = NULL;
    }

    /* Remove from the list of pending writes if needed. */
    if (c->flags & CLIENT_PENDING_WRITE) {
        serverAssert(&c->clients_pending_write_node.next != NULL || 
                     &c->clients_pending_write_node.prev != NULL);
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        c->flags &= ~CLIENT_PENDING_WRITE;
    }

    /* Remove from the list of pending reads if needed. */
    serverAssert(!c->conn || io_threads_op == IO_THREADS_OP_IDLE);
    if (c->pending_read_list_node != NULL) {
        listDelNode(server.clients_pending_read,c->pending_read_list_node);
        c->pending_read_list_node = NULL;
    }


    /* When client was just unblocked because of a blocking operation,
     * remove it from the list of unblocked clients. */
    if (c->flags & CLIENT_UNBLOCKED) {
        ln = listSearchKey(server.unblocked_clients,c);
        serverAssert(ln != NULL);
        listDelNode(server.unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;
    }

    /* Clear the tracking status. */
    if (c->flags & CLIENT_TRACKING) disableTracking(c);
}

/* Clear the client state to resemble a newly connected client. */
void clearClientConnectionState(client *c) {
    listNode *ln;

    /* MONITOR clients are also marked with CLIENT_SLAVE, we need to
     * distinguish between the two.
     */
    if (c->flags & CLIENT_MONITOR) {
        ln = listSearchKey(server.monitors,c);
        serverAssert(ln != NULL);
        listDelNode(server.monitors,ln);

        c->flags &= ~(CLIENT_MONITOR|CLIENT_SLAVE);
    }

    serverAssert(!(c->flags &(CLIENT_SLAVE|CLIENT_MASTER)));

    if (c->flags & CLIENT_TRACKING) disableTracking(c);
    selectDb(c,0);
#ifdef LOG_REQ_RES
    c->resp = server.client_default_resp;
#else
    c->resp = 2;
#endif

    clientSetDefaultAuth(c);
    moduleNotifyUserChanged(c);
    discardTransaction(c);

    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeShardAllChannels(c, 0);
    pubsubUnsubscribeAllPatterns(c,0);
    unmarkClientAsPubSub(c);

    if (c->name) {
        decrRefCount(c->name);
        c->name = NULL;
    }

    /* Note: lib_name and lib_ver are not reset since they still
     * represent the client library behind the connection. */
    
    /* Selectively clear state flags not covered above */
    c->flags &= ~(CLIENT_ASKING|CLIENT_READONLY|CLIENT_REPLY_OFF|
                  CLIENT_REPLY_SKIP_NEXT|CLIENT_NO_TOUCH|CLIENT_NO_EVICT);
}

void deauthenticateAndCloseClient(client *c) {
    c->user = DefaultUser;
    c->authenticated = 0;
    /* We will write replies to this client later, so we can't
     * close it directly even if async. */
    if (c == server.current_client) {
        c->flags |= CLIENT_CLOSE_AFTER_COMMAND;
    } else {
        freeClientAsync(c);
    }
}

void freeClient(client *c) {
    listNode *ln;

    /* If a client is protected, yet we need to free it right now, make sure
     * to at least use asynchronous freeing. */
    if (c->flags & CLIENT_PROTECTED) {
        freeClientAsync(c);
        return;
    }

    /* For connected clients, call the disconnection event of modules hooks. */
    if (c->conn) {
        moduleFireServerEvent(REDISMODULE_EVENT_CLIENT_CHANGE,
                              REDISMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED,
                              c);
    }

    /* Notify module system that this client auth status changed. */
    moduleNotifyUserChanged(c);

    /* Free the RedisModuleBlockedClient held onto for reprocessing if not already freed. */
    zfree(c->module_blocked_client);

    /* If this client was scheduled for async freeing we need to remove it
     * from the queue. Note that we need to do this here, because later
     * we may call replicationCacheMaster() and the client should already
     * be removed from the list of clients to free. */
    if (c->flags & CLIENT_CLOSE_ASAP) {
        ln = listSearchKey(server.clients_to_close,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_to_close,ln);
    }

    /* If it is our master that's being disconnected we should make sure
     * to cache the state to try a partial resynchronization later.
     *
     * Note that before doing this we make sure that the client is not in
     * some unexpected state, by checking its flags. */
    if (server.master && c->flags & CLIENT_MASTER) {
        serverLog(LL_NOTICE,"Connection with master lost.");
        if (!(c->flags & (CLIENT_PROTOCOL_ERROR|CLIENT_BLOCKED))) {
            c->flags &= ~(CLIENT_CLOSE_ASAP|CLIENT_CLOSE_AFTER_REPLY);
            replicationCacheMaster(c);
            return;
        }
    }

    /* Log link disconnection with slave */
    if (getClientType(c) == CLIENT_TYPE_SLAVE) {
        serverLog(LL_NOTICE,"Connection with replica %s lost.",
            replicationGetSlaveName(c));
    }

    /* Free the query buffer */
    sdsfree(c->querybuf);
    c->querybuf = NULL;

    /* Deallocate structures used to block on blocking ops. */
    /* If there is any in-flight command, we don't record their duration. */
    c->duration = 0;
    if (c->flags & CLIENT_BLOCKED) unblockClient(c, 1);
    dictRelease(c->bstate.keys);

    /* UNWATCH all the keys */
    unwatchAllKeys(c);
    listRelease(c->watched_keys);

    /* Unsubscribe from all the pubsub channels */
    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeShardAllChannels(c, 0);
    pubsubUnsubscribeAllPatterns(c,0);
    unmarkClientAsPubSub(c);
    dictRelease(c->pubsub_channels);
    dictRelease(c->pubsub_patterns);
    dictRelease(c->pubsubshard_channels);

    /* Free data structures. */
    listRelease(c->reply);
    zfree(c->buf);
    freeReplicaReferencedReplBuffer(c);
    freeClientArgv(c);
    freeClientOriginalArgv(c);
    if (c->deferred_reply_errors)
        listRelease(c->deferred_reply_errors);
#ifdef LOG_REQ_RES
    reqresReset(c, 1);
#endif

    /* Remove the contribution that this client gave to our
     * incrementally computed memory usage. */
    if (c->conn)
        server.stat_clients_type_memory[c->last_memory_type] -=
            c->last_memory_usage;

    /* Unlink the client: this will close the socket, remove the I/O
     * handlers, and remove references of the client from different
     * places where active clients may be referenced. */
    unlinkClient(c);

    /* Master/slave cleanup Case 1:
     * we lost the connection with a slave. */
    if (c->flags & CLIENT_SLAVE) {
        /* If there is no any other slave waiting dumping RDB finished, the
         * current child process need not continue to dump RDB, then we kill it.
         * So child process won't use more memory, and we also can fork a new
         * child process asap to dump rdb for next full synchronization or bgsave.
         * But we also need to check if users enable 'save' RDB, if enable, we
         * should not remove directly since that means RDB is important for users
         * to keep data safe and we may delay configured 'save' for full sync. */
        if (server.saveparamslen == 0 &&
            c->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
            server.child_type == CHILD_TYPE_RDB &&
            server.rdb_child_type == RDB_CHILD_TYPE_DISK &&
            anyOtherSlaveWaitRdb(c) == 0)
        {
            killRDBChild();
        }
        if (c->replstate == SLAVE_STATE_SEND_BULK) {
            if (c->repldbfd != -1) close(c->repldbfd);
            if (c->replpreamble) sdsfree(c->replpreamble);
        }
        list *l = (c->flags & CLIENT_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l,c);
        serverAssert(ln != NULL);
        listDelNode(l,ln);
        /* We need to remember the time when we started to have zero
         * attached slaves, as after some time we'll free the replication
         * backlog. */
        if (getClientType(c) == CLIENT_TYPE_SLAVE && listLength(server.slaves) == 0)
            server.repl_no_slaves_since = server.unixtime;
        refreshGoodSlavesCount();
        /* Fire the replica change modules event. */
        if (c->replstate == SLAVE_STATE_ONLINE)
            moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                                  REDISMODULE_SUBEVENT_REPLICA_CHANGE_OFFLINE,
                                  NULL);
    }

    /* Master/slave cleanup Case 2:
     * we lost the connection with the master. */
    if (c->flags & CLIENT_MASTER) replicationHandleMasterDisconnection();

    /* Remove client from memory usage buckets */
    if (c->mem_usage_bucket) {
        c->mem_usage_bucket->mem_usage_sum -= c->last_memory_usage;
        listDelNode(c->mem_usage_bucket->clients, c->mem_usage_bucket_node);
    }

    /* Release other dynamically allocated client structure fields,
     * and finally release the client structure itself. */
    if (c->name) decrRefCount(c->name);
    if (c->lib_name) decrRefCount(c->lib_name);
    if (c->lib_ver) decrRefCount(c->lib_ver);
    freeClientMultiState(c);
    sdsfree(c->peerid);
    sdsfree(c->sockname);
    sdsfree(c->slave_addr);
    zfree(c);
}

/* Schedule a client to free it at a safe time in the beforeSleep() function.
 * This function is useful when we need to terminate a client but we are in
 * a context where calling freeClient() is not possible, because the client
 * should be valid for the continuation of the flow of the program. */
void freeClientAsync(client *c) {
    /* We need to handle concurrent access to the server.clients_to_close list
     * only in the freeClientAsync() function, since it's the only function that
     * may access the list while Redis uses I/O threads. All the other accesses
     * are in the context of the main thread while the other threads are
     * idle. */
    if (c->flags & CLIENT_CLOSE_ASAP || c->flags & CLIENT_SCRIPT) return;
    c->flags |= CLIENT_CLOSE_ASAP;
    /* Replicas that was marked as CLIENT_CLOSE_ASAP should not keep the
     * replication backlog from been trimmed. */
    if (c->flags & CLIENT_SLAVE) freeReplicaReferencedReplBuffer(c);
    if (server.io_threads_num == 1) {
        /* no need to bother with locking if there's just one thread (the main thread) */
        listAddNodeTail(server.clients_to_close,c);
        return;
    }
    static pthread_mutex_t async_free_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&async_free_queue_mutex);
    listAddNodeTail(server.clients_to_close,c);
    pthread_mutex_unlock(&async_free_queue_mutex);
}

/* Log errors for invalid use and free the client in async way.
 * We will add additional information about the client to the message. */
void logInvalidUseAndFreeClientAsync(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    sds info = sdscatvprintf(sdsempty(), fmt, ap);
    va_end(ap);

    sds client = catClientInfoString(sdsempty(), c);
    serverLog(LL_WARNING, "%s, disconnecting it: %s", info, client);

    sdsfree(info);
    sdsfree(client);
    freeClientAsync(c);
}

/* Perform processing of the client before moving on to processing the next client
 * this is useful for performing operations that affect the global state but can't
 * wait until we're done with all clients. In other words can't wait until beforeSleep()
 * return C_ERR in case client is no longer valid after call.
 * The input client argument: c, may be NULL in case the previous client was
 * freed before the call. */
int beforeNextClient(client *c) {
    /* Notice, this code is also called from 'processUnblockedClients'.
     * But in case of a module blocked client (see RM_Call 'K' flag) we do not reach this code path.
     * So whenever we change the code here we need to consider if we need this change on module
     * blocked client as well */

    /* Skip the client processing if we're in an IO thread, in that case we'll perform
       this operation later (this function is called again) in the fan-in stage of the threading mechanism */
    if (io_threads_op != IO_THREADS_OP_IDLE)
        return C_OK;
    /* Handle async frees */
    /* Note: this doesn't make the server.clients_to_close list redundant because of
     * cases where we want an async free of a client other than myself. For example
     * in ACL modifications we disconnect clients authenticated to non-existent
     * users (see ACL LOAD). */
    if (c && (c->flags & CLIENT_CLOSE_ASAP)) {
        freeClient(c);
        return C_ERR;
    }
    return C_OK;
}

/* Free the clients marked as CLOSE_ASAP, return the number of clients
 * freed. */
int freeClientsInAsyncFreeQueue(void) {
    int freed = 0;
    listIter li;
    listNode *ln;

    listRewind(server.clients_to_close,&li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);

        if (c->flags & CLIENT_PROTECTED) continue;

        c->flags &= ~CLIENT_CLOSE_ASAP;
        freeClient(c);
        listDelNode(server.clients_to_close,ln);
        freed++;
    }
    return freed;
}

/* Return a client by ID, or NULL if the client ID is not in the set
 * of registered clients. Note that "fake clients", created with -1 as FD,
 * are not registered clients. */
client *lookupClientByID(uint64_t id) {
    id = htonu64(id);
    void *c = NULL;
    raxFind(server.clients_index,(unsigned char*)&id,sizeof(id),&c);
    return c;
}

/* This function should be called from _writeToClient when the reply list is not empty,
 * it gathers the scattered buffers from reply list and sends them away with connWritev.
 * If we write successfully, it returns C_OK, otherwise, C_ERR is returned,
 * and 'nwritten' is an output parameter, it means how many bytes server write
 * to client. */
static int _writevToClient(client *c, ssize_t *nwritten) {
    int iovcnt = 0;
    int iovmax = min(IOV_MAX, c->conn->iovcnt);
    struct iovec iov[iovmax];
    size_t iov_bytes_len = 0;
    /* If the static reply buffer is not empty, 
     * add it to the iov array for writev() as well. */
    if (c->bufpos > 0) {
        iov[iovcnt].iov_base = c->buf + c->sentlen;
        iov[iovcnt].iov_len = c->bufpos - c->sentlen;
        iov_bytes_len += iov[iovcnt++].iov_len;
    }
    /* The first node of reply list might be incomplete from the last call,
     * thus it needs to be calibrated to get the actual data address and length. */
    size_t offset = c->bufpos > 0 ? 0 : c->sentlen;
    listIter iter;
    listNode *next;
    clientReplyBlock *o;
    listRewind(c->reply, &iter);
    while ((next = listNext(&iter)) && iovcnt < iovmax && iov_bytes_len < NET_MAX_WRITES_PER_EVENT) {
        o = listNodeValue(next);
        if (o->used == 0) { /* empty node, just release it and skip. */
            c->reply_bytes -= o->size;
            listDelNode(c->reply, next);
            offset = 0;
            continue;
        }

        iov[iovcnt].iov_base = o->buf + offset;
        iov[iovcnt].iov_len = o->used - offset;
        iov_bytes_len += iov[iovcnt++].iov_len;
        offset = 0;
    }
    if (iovcnt == 0) return C_OK;
    *nwritten = connWritev(c->conn, iov, iovcnt);
    if (*nwritten <= 0) return C_ERR;

    /* Locate the new node which has leftover data and
     * release all nodes in front of it. */
    ssize_t remaining = *nwritten;
    if (c->bufpos > 0) { /* deal with static reply buffer first. */
        int buf_len = c->bufpos - c->sentlen;
        c->sentlen += remaining;
        /* If the buffer was sent, set bufpos to zero to continue with
         * the remainder of the reply. */
        if (remaining >= buf_len) {
            c->bufpos = 0;
            c->sentlen = 0;
        }
        remaining -= buf_len;
    }
    listRewind(c->reply, &iter);
    while (remaining > 0) {
        next = listNext(&iter);
        o = listNodeValue(next);
        if (remaining < (ssize_t)(o->used - c->sentlen)) {
            c->sentlen += remaining;
            break;
        }
        remaining -= (ssize_t)(o->used - c->sentlen);
        c->reply_bytes -= o->size;
        listDelNode(c->reply, next);
        c->sentlen = 0;
    }

    return C_OK;
}

/* This function does actual writing output buffers to different types of
 * clients, it is called by writeToClient.
 * If we write successfully, it returns C_OK, otherwise, C_ERR is returned,
 * and 'nwritten' is an output parameter, it means how many bytes server write
 * to client. */
int _writeToClient(client *c, ssize_t *nwritten) {
    *nwritten = 0;
    if (getClientType(c) == CLIENT_TYPE_SLAVE) {
        serverAssert(c->bufpos == 0 && listLength(c->reply) == 0);

        replBufBlock *o = listNodeValue(c->ref_repl_buf_node);
        serverAssert(o->used >= c->ref_block_pos);
        /* Send current block if it is not fully sent. */
        if (o->used > c->ref_block_pos) {
            *nwritten = connWrite(c->conn, o->buf+c->ref_block_pos,
                                  o->used-c->ref_block_pos);
            if (*nwritten <= 0) return C_ERR;
            c->ref_block_pos += *nwritten;
        }

        /* If we fully sent the object on head, go to the next one. */
        listNode *next = listNextNode(c->ref_repl_buf_node);
        if (next && c->ref_block_pos == o->used) {
            o->refcount--;
            ((replBufBlock *)(listNodeValue(next)))->refcount++;
            c->ref_repl_buf_node = next;
            c->ref_block_pos = 0;
            incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
        }
        return C_OK;
    }

    /* When the reply list is not empty, it's better to use writev to save us some
     * system calls and TCP packets. */
    if (listLength(c->reply) > 0) {
        int ret = _writevToClient(c, nwritten);
        if (ret != C_OK) return ret;

        /* If there are no longer objects in the list, we expect
         * the count of reply bytes to be exactly zero. */
        if (listLength(c->reply) == 0)
            serverAssert(c->reply_bytes == 0);
    } else if (c->bufpos > 0) {
        *nwritten = connWrite(c->conn, c->buf + c->sentlen, c->bufpos - c->sentlen);
        if (*nwritten <= 0) return C_ERR;
        c->sentlen += *nwritten;

        /* If the buffer was sent, set bufpos to zero to continue with
         * the remainder of the reply. */
        if ((int)c->sentlen == c->bufpos) {
            c->bufpos = 0;
            c->sentlen = 0;
        }
    } 

    return C_OK;
}

/* Write data in output buffers to client. Return C_OK if the client
 * is still valid after the call, C_ERR if it was freed because of some
 * error.  If handler_installed is set, it will attempt to clear the
 * write event.
 *
 * This function is called by threads, but always with handler_installed
 * set to 0. So when handler_installed is set to 0 the function must be
 * thread safe. */
int writeToClient(client *c, int handler_installed) {
    /* Update total number of writes on server */
    atomicIncr(server.stat_total_writes_processed, 1);

    ssize_t nwritten = 0, totwritten = 0;

    while(clientHasPendingReplies(c)) {
        int ret = _writeToClient(c, &nwritten);
        if (ret == C_ERR) break;
        totwritten += nwritten;
        /* Note that we avoid to send more than NET_MAX_WRITES_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interface).
         *
         * However if we are over the maxmemory limit we ignore that and
         * just deliver as much data as it is possible to deliver.
         *
         * Moreover, we also send as much as possible if the client is
         * a slave or a monitor (otherwise, on high-speed traffic, the
         * replication/output buffer will grow indefinitely) */
        if (totwritten > NET_MAX_WRITES_PER_EVENT &&
            (server.maxmemory == 0 ||
             zmalloc_used_memory() < server.maxmemory) &&
            !(c->flags & CLIENT_SLAVE)) break;
    }

    if (getClientType(c) == CLIENT_TYPE_SLAVE) {
        atomicIncr(server.stat_net_repl_output_bytes, totwritten);
    } else {
        atomicIncr(server.stat_net_output_bytes, totwritten);
    }

    if (nwritten == -1) {
        if (connGetState(c->conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_VERBOSE,
                "Error writing to client: %s", connGetLastError(c->conn));
            freeClientAsync(c);
            return C_ERR;
        }
    }
    if (totwritten > 0) {
        /* For clients representing masters we don't count sending data
         * as an interaction, since we always send REPLCONF ACK commands
         * that take some time to just fill the socket output buffer.
         * We just rely on data / pings received for timeout detection. */
        if (!(c->flags & CLIENT_MASTER)) c->lastinteraction = server.unixtime;
    }
    if (!clientHasPendingReplies(c)) {
        c->sentlen = 0;
        /* Note that writeToClient() is called in a threaded way, but
         * aeDeleteFileEvent() is not thread safe: however writeToClient()
         * is always called with handler_installed set to 0 from threads
         * so we are fine. */
        if (handler_installed) {
            serverAssert(io_threads_op == IO_THREADS_OP_IDLE);
            connSetWriteHandler(c->conn, NULL);
        }

        /* Close connection after entire reply has been sent. */
        if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
            freeClientAsync(c);
            return C_ERR;
        }
    }
    /* Update client's memory usage after writing.
     * Since this isn't thread safe we do this conditionally. In case of threaded writes this is done in
     * handleClientsWithPendingWritesUsingThreads(). */
    if (io_threads_op == IO_THREADS_OP_IDLE)
        updateClientMemUsageAndBucket(c);
    return C_OK;
}

/* Write event handler. Just send data to the client. */
void sendReplyToClient(connection *conn) {
    client *c = connGetPrivateData(conn);
    writeToClient(c,1);
}

/* This function is called just before entering the event loop, in the hope
 * we can just write the replies to the client output buffer without any
 * need to use a syscall in order to install the writable event handler,
 * get it called, and so forth. */
int handleClientsWithPendingWrites(void) {
    listIter li;
    listNode *ln;
    int processed = listLength(server.clients_pending_write);

    listRewind(server.clients_pending_write,&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
        listUnlinkNode(server.clients_pending_write,ln);

        /* If a client is protected, don't do anything,
         * that may trigger write error or recreate handler. */
        if (c->flags & CLIENT_PROTECTED) continue;

        /* Don't write to clients that are going to be closed anyway. */
        if (c->flags & CLIENT_CLOSE_ASAP) continue;

        /* Try to write buffers to the client socket. */
        if (writeToClient(c,0) == C_ERR) continue;

        /* If after the synchronous writes above we still have data to
         * output to the client, we need to install the writable handler. */
        if (clientHasPendingReplies(c)) {
            installClientWriteHandler(c);
        }
    }
    return processed;
}

/* resetClient prepare the client to process the next command */
void resetClient(client *c) {
    redisCommandProc *prevcmd = c->cmd ? c->cmd->proc : NULL;

    freeClientArgv(c);
    c->cur_script = NULL;
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->slot = -1;
    c->flags &= ~CLIENT_EXECUTING_COMMAND;

    /* Make sure the duration has been recorded to some command. */
    serverAssert(c->duration == 0);
#ifdef LOG_REQ_RES
    reqresReset(c, 1);
#endif

    if (c->deferred_reply_errors)
        listRelease(c->deferred_reply_errors);
    c->deferred_reply_errors = NULL;

    /* We clear the ASKING flag as well if we are not inside a MULTI, and
     * if what we just executed is not the ASKING command itself. */
    if (!(c->flags & CLIENT_MULTI) && prevcmd != askingCommand)
        c->flags &= ~CLIENT_ASKING;

    /* We do the same for the CACHING command as well. It also affects
     * the next command or transaction executed, in a way very similar
     * to ASKING. */
    if (!(c->flags & CLIENT_MULTI) && prevcmd != clientCommand)
        c->flags &= ~CLIENT_TRACKING_CACHING;

    /* Remove the CLIENT_REPLY_SKIP flag if any so that the reply
     * to the next command will be sent, but set the flag if the command
     * we just processed was "CLIENT REPLY SKIP". */
    c->flags &= ~CLIENT_REPLY_SKIP;
    if (c->flags & CLIENT_REPLY_SKIP_NEXT) {
        c->flags |= CLIENT_REPLY_SKIP;
        c->flags &= ~CLIENT_REPLY_SKIP_NEXT;
    }
}

/* This function is used when we want to re-enter the event loop but there
 * is the risk that the client we are dealing with will be freed in some
 * way. This happens for instance in:
 *
 * * DEBUG RELOAD and similar.
 * * When a Lua script is in -BUSY state.
 *
 * So the function will protect the client by doing two things:
 *
 * 1) It removes the file events. This way it is not possible that an
 *    error is signaled on the socket, freeing the client.
 * 2) Moreover it makes sure that if the client is freed in a different code
 *    path, it is not really released, but only marked for later release. */
void protectClient(client *c) {
    c->flags |= CLIENT_PROTECTED;
    if (c->conn) {
        connSetReadHandler(c->conn,NULL);
        connSetWriteHandler(c->conn,NULL);
    }
}

/* This will undo the client protection done by protectClient() */
void unprotectClient(client *c) {
    if (c->flags & CLIENT_PROTECTED) {
        c->flags &= ~CLIENT_PROTECTED;
        if (c->conn) {
            connSetReadHandler(c->conn,readQueryFromClient);
            if (clientHasPendingReplies(c)) putClientInPendingWriteQueue(c);
        }
    }
}

/* Like processMultibulkBuffer(), but for the inline protocol instead of RESP,
 * this function consumes the client query buffer and creates a command ready
 * to be executed inside the client structure. Returns C_OK if the command
 * is ready to be executed, or C_ERR if there is still protocol to read to
 * have a well formed command. The function also returns C_ERR when there is
 * a protocol error: in such a case the client structure is setup to reply
 * with the error and close the connection. */
int processInlineBuffer(client *c) {
    char *newline;
    int argc, j, linefeed_chars = 1;
    sds *argv, aux;
    size_t querylen;

    /* Search for end of line */
    newline = strchr(c->querybuf+c->qb_pos,'\n');

    /* Nothing to do without a \r\n */
    if (newline == NULL) {
        if (sdslen(c->querybuf)-c->qb_pos > PROTO_INLINE_MAX_SIZE) {
            addReplyError(c,"Protocol error: too big inline request");
            setProtocolError("too big inline request",c);
        }
        return C_ERR;
    }

    /* Handle the \r\n case. */
    if (newline != c->querybuf+c->qb_pos && *(newline-1) == '\r')
        newline--, linefeed_chars++;

    /* Split the input buffer up to the \r\n */
    querylen = newline-(c->querybuf+c->qb_pos);
    aux = sdsnewlen(c->querybuf+c->qb_pos,querylen);
    argv = sdssplitargs(aux,&argc);
    sdsfree(aux);
    if (argv == NULL) {
        addReplyError(c,"Protocol error: unbalanced quotes in request");
        setProtocolError("unbalanced quotes in inline request",c);
        return C_ERR;
    }

    /* Newline from slaves can be used to refresh the last ACK time.
     * This is useful for a slave to ping back while loading a big
     * RDB file. */
    if (querylen == 0 && getClientType(c) == CLIENT_TYPE_SLAVE)
        c->repl_ack_time = server.unixtime;

    /* Masters should never send us inline protocol to run actual
     * commands. If this happens, it is likely due to a bug in Redis where
     * we got some desynchronization in the protocol, for example
     * because of a PSYNC gone bad.
     *
     * However there is an exception: masters may send us just a newline
     * to keep the connection active. */
    if (querylen != 0 && c->flags & CLIENT_MASTER) {
        sdsfreesplitres(argv,argc);
        serverLog(LL_WARNING,"WARNING: Receiving inline protocol from master, master stream corruption? Closing the master connection and discarding the cached master.");
        setProtocolError("Master using the inline protocol. Desync?",c);
        return C_ERR;
    }

    /* Move querybuffer position to the next query in the buffer. */
    c->qb_pos += querylen+linefeed_chars;

    /* Setup argv array on client structure */
    if (argc) {
        if (c->argv) zfree(c->argv);
        c->argv_len = argc;
        c->argv = zmalloc(sizeof(robj*)*c->argv_len);
        c->argv_len_sum = 0;
    }

    /* Create redis objects for all arguments. */
    for (c->argc = 0, j = 0; j < argc; j++) {
        c->argv[c->argc] = createObject(OBJ_STRING,argv[j]);
        c->argc++;
        c->argv_len_sum += sdslen(argv[j]);
    }
    zfree(argv);
    return C_OK;
}

/* Helper function. Record protocol error details in server log,
 * and set the client as CLIENT_CLOSE_AFTER_REPLY and
 * CLIENT_PROTOCOL_ERROR. */
#define PROTO_DUMP_LEN 128
static void setProtocolError(const char *errstr, client *c) {
    if (server.verbosity <= LL_VERBOSE || c->flags & CLIENT_MASTER) {
        sds client = catClientInfoString(sdsempty(),c);

        /* Sample some protocol to given an idea about what was inside. */
        char buf[256];
        if (sdslen(c->querybuf)-c->qb_pos < PROTO_DUMP_LEN) {
            snprintf(buf,sizeof(buf),"Query buffer during protocol error: '%s'", c->querybuf+c->qb_pos);
        } else {
            snprintf(buf,sizeof(buf),"Query buffer during protocol error: '%.*s' (... more %zu bytes ...) '%.*s'", PROTO_DUMP_LEN/2, c->querybuf+c->qb_pos, sdslen(c->querybuf)-c->qb_pos-PROTO_DUMP_LEN, PROTO_DUMP_LEN/2, c->querybuf+sdslen(c->querybuf)-PROTO_DUMP_LEN/2);
        }

        /* Remove non printable chars. */
        char *p = buf;
        while (*p != '\0') {
            if (!isprint(*p)) *p = '.';
            p++;
        }

        /* Log all the client and protocol info. */
        int loglevel = (c->flags & CLIENT_MASTER) ? LL_WARNING :
                                                    LL_VERBOSE;
        serverLog(loglevel,
            "Protocol error (%s) from client: %s. %s", errstr, client, buf);
        sdsfree(client);
    }
    c->flags |= (CLIENT_CLOSE_AFTER_REPLY|CLIENT_PROTOCOL_ERROR);
}

/* Process the query buffer for client 'c', setting up the client argument
 * vector for command execution. Returns C_OK if after running the function
 * the client has a well-formed ready to be processed command, otherwise
 * C_ERR if there is still to read more buffer to get the full command.
 * The function also returns C_ERR when there is a protocol error: in such a
 * case the client structure is setup to reply with the error and close
 * the connection.
 *
 * This function is called if processInputBuffer() detects that the next
 * command is in RESP format, so the first byte in the command is found
 * to be '*'. Otherwise for inline commands processInlineBuffer() is called. */
int processMultibulkBuffer(client *c) {
    char *newline = NULL;
    int ok;
    long long ll;

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        serverAssertWithInfo(c,NULL,c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf+c->qb_pos,'\r');
        if (newline == NULL) {
            if (sdslen(c->querybuf)-c->qb_pos > PROTO_INLINE_MAX_SIZE) {
                addReplyError(c,"Protocol error: too big mbulk count string");
                setProtocolError("too big mbulk count string",c);
            }
            return C_ERR;
        }

        /* Buffer should also contain \n */
        if (newline-(c->querybuf+c->qb_pos) > (ssize_t)(sdslen(c->querybuf)-c->qb_pos-2))
            return C_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        serverAssertWithInfo(c,NULL,c->querybuf[c->qb_pos] == '*');
        ok = string2ll(c->querybuf+1+c->qb_pos,newline-(c->querybuf+1+c->qb_pos),&ll);
        if (!ok || ll > INT_MAX) {
            addReplyError(c,"Protocol error: invalid multibulk length");
            setProtocolError("invalid mbulk count",c);
            return C_ERR;
        } else if (ll > 10 && authRequired(c)) {
            addReplyError(c, "Protocol error: unauthenticated multibulk length");
            setProtocolError("unauth mbulk count", c);
            return C_ERR;
        }

        c->qb_pos = (newline-c->querybuf)+2;

        if (ll <= 0) return C_OK;

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        if (c->argv) zfree(c->argv);
        c->argv_len = min(c->multibulklen, 1024);
        c->argv = zmalloc(sizeof(robj*)*c->argv_len);
        c->argv_len_sum = 0;
    }

    serverAssertWithInfo(c,NULL,c->multibulklen > 0);
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf+c->qb_pos,'\r');
            if (newline == NULL) {
                if (sdslen(c->querybuf)-c->qb_pos > PROTO_INLINE_MAX_SIZE) {
                    addReplyError(c,
                        "Protocol error: too big bulk count string");
                    setProtocolError("too big bulk count string",c);
                    return C_ERR;
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(c->querybuf+c->qb_pos) > (ssize_t)(sdslen(c->querybuf)-c->qb_pos-2))
                break;

            if (c->querybuf[c->qb_pos] != '$') {
                addReplyErrorFormat(c,
                    "Protocol error: expected '$', got '%c'",
                    c->querybuf[c->qb_pos]);
                setProtocolError("expected $ but got something else",c);
                return C_ERR;
            }

            ok = string2ll(c->querybuf+c->qb_pos+1,newline-(c->querybuf+c->qb_pos+1),&ll);
            if (!ok || ll < 0 ||
                (!(c->flags & CLIENT_MASTER) && ll > server.proto_max_bulk_len)) {
                addReplyError(c,"Protocol error: invalid bulk length");
                setProtocolError("invalid bulk length",c);
                return C_ERR;
            } else if (ll > 16384 && authRequired(c)) {
                addReplyError(c, "Protocol error: unauthenticated bulk length");
                setProtocolError("unauth bulk length", c);
                return C_ERR;
            }

            c->qb_pos = newline-c->querybuf+2;
            if (!(c->flags & CLIENT_MASTER) && ll >= PROTO_MBULK_BIG_ARG) {
                /* When the client is not a master client (because master
                 * client's querybuf can only be trimmed after data applied
                 * and sent to replicas).
                 *
                 * If we are going to read a large object from network
                 * try to make it likely that it will start at c->querybuf
                 * boundary so that we can optimize object creation
                 * avoiding a large copy of data.
                 *
                 * But only when the data we have not parsed is less than
                 * or equal to ll+2. If the data length is greater than
                 * ll+2, trimming querybuf is just a waste of time, because
                 * at this time the querybuf contains not only our bulk. */
                if (sdslen(c->querybuf)-c->qb_pos <= (size_t)ll+2) {
                    sdsrange(c->querybuf,c->qb_pos,-1);
                    c->qb_pos = 0;
                    /* Hint the sds library about the amount of bytes this string is
                     * going to contain. */
                    c->querybuf = sdsMakeRoomForNonGreedy(c->querybuf,ll+2-sdslen(c->querybuf));
                    /* We later set the peak to the used portion of the buffer, but here we over
                     * allocated because we know what we need, make sure it'll not be shrunk before used. */
                    if (c->querybuf_peak < (size_t)ll + 2) c->querybuf_peak = ll + 2;
                }
            }
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-c->qb_pos < (size_t)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            /* Check if we have space in argv, grow if needed */
            if (c->argc >= c->argv_len) {
                c->argv_len = min(c->argv_len < INT_MAX/2 ? c->argv_len*2 : INT_MAX, c->argc+c->multibulklen);
                c->argv = zrealloc(c->argv, sizeof(robj*)*c->argv_len);
            }

            /* Optimization: if a non-master client's buffer contains JUST our bulk element
             * instead of creating a new object by *copying* the sds we
             * just use the current sds string. */
            if (!(c->flags & CLIENT_MASTER) &&
                c->qb_pos == 0 &&
                c->bulklen >= PROTO_MBULK_BIG_ARG &&
                sdslen(c->querybuf) == (size_t)(c->bulklen+2))
            {
                c->argv[c->argc++] = createObject(OBJ_STRING,c->querybuf);
                c->argv_len_sum += c->bulklen;
                sdsIncrLen(c->querybuf,-2); /* remove CRLF */
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                c->querybuf = sdsnewlen(SDS_NOINIT,c->bulklen+2);
                sdsclear(c->querybuf);
            } else {
                c->argv[c->argc++] =
                    createStringObject(c->querybuf+c->qb_pos,c->bulklen);
                c->argv_len_sum += c->bulklen;
                c->qb_pos += c->bulklen+2;
            }
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) return C_OK;

    /* Still not ready to process the command */
    return C_ERR;
}

/* Perform necessary tasks after a command was executed:
 *
 * 1. The client is reset unless there are reasons to avoid doing it.
 * 2. In the case of master clients, the replication offset is updated.
 * 3. Propagate commands we got from our master to replicas down the line. */
void commandProcessed(client *c) {
    /* If client is blocked(including paused), just return avoid reset and replicate.
     *
     * 1. Don't reset the client structure for blocked clients, so that the reply
     *    callback will still be able to access the client argv and argc fields.
     *    The client will be reset in unblockClient().
     * 2. Don't update replication offset or propagate commands to replicas,
     *    since we have not applied the command. */
    if (c->flags & CLIENT_BLOCKED) return;

    reqresAppendResponse(c);
    resetClient(c);

    long long prev_offset = c->reploff;
    if (c->flags & CLIENT_MASTER && !(c->flags & CLIENT_MULTI)) {
        /* Update the applied replication offset of our master. */
        c->reploff = c->read_reploff - sdslen(c->querybuf) + c->qb_pos;
    }

    /* If the client is a master we need to compute the difference
     * between the applied offset before and after processing the buffer,
     * to understand how much of the replication stream was actually
     * applied to the master state: this quantity, and its corresponding
     * part of the replication stream, will be propagated to the
     * sub-replicas and to the replication backlog. */
    if (c->flags & CLIENT_MASTER) {
        long long applied = c->reploff - prev_offset;
        if (applied) {
            replicationFeedStreamFromMasterStream(c->querybuf+c->repl_applied,applied);
            c->repl_applied += applied;
        }
    }
}

/* This function calls processCommand(), but also performs a few sub tasks
 * for the client that are useful in that context:
 *
 * 1. It sets the current client to the client 'c'.
 * 2. calls commandProcessed() if the command was handled.
 *
 * The function returns C_ERR in case the client was freed as a side effect
 * of processing the command, otherwise C_OK is returned. */
int processCommandAndResetClient(client *c) {
    int deadclient = 0;
    client *old_client = server.current_client;
    server.current_client = c;
    if (processCommand(c) == C_OK) {
        commandProcessed(c);
        /* Update the client's memory to include output buffer growth following the
         * processed command. */
        if (c->conn) updateClientMemUsageAndBucket(c);
    }

    if (server.current_client == NULL) deadclient = 1;
    /*
     * Restore the old client, this is needed because when a script
     * times out, we will get into this code from processEventsWhileBlocked.
     * Which will cause to set the server.current_client. If not restored
     * we will return 1 to our caller which will falsely indicate the client
     * is dead and will stop reading from its buffer.
     */
    server.current_client = old_client;
    /* performEvictions may flush slave output buffers. This may
     * result in a slave, that may be the active client, to be
     * freed. */
    return deadclient ? C_ERR : C_OK;
}


/* This function will execute any fully parsed commands pending on
 * the client. Returns C_ERR if the client is no longer valid after executing
 * the command, and C_OK for all other cases. */
int processPendingCommandAndInputBuffer(client *c) {
    /* Notice, this code is also called from 'processUnblockedClients'.
     * But in case of a module blocked client (see RM_Call 'K' flag) we do not reach this code path.
     * So whenever we change the code here we need to consider if we need this change on module
     * blocked client as well */
    if (c->flags & CLIENT_PENDING_COMMAND) {
        c->flags &= ~CLIENT_PENDING_COMMAND;
        if (processCommandAndResetClient(c) == C_ERR) {
            return C_ERR;
        }
    }

    /* Now process client if it has more data in it's buffer.
     *
     * Note: when a master client steps into this function,
     * it can always satisfy this condition, because its querybuf
     * contains data not applied. */
    if (c->querybuf && sdslen(c->querybuf) > 0) {
        return processInputBuffer(c);
    }
    return C_OK;
}

/* This function is called every time, in the client structure 'c', there is
 * more query buffer to process, because we read more data from the socket
 * or because a client was blocked and later reactivated, so there could be
 * pending query buffer, already representing a full command, to process.
 * return C_ERR in case the client was freed during the processing */
int processInputBuffer(client *c) {
    /* Keep processing while there is something in the input buffer */
    while(c->qb_pos < sdslen(c->querybuf)) {
        /* Immediately abort if the client is in the middle of something. */
        if (c->flags & CLIENT_BLOCKED) break;

        /* Don't process more buffers from clients that have already pending
         * commands to execute in c->argv. */
        if (c->flags & CLIENT_PENDING_COMMAND) break;

        /* Don't process input from the master while there is a busy script
         * condition on the slave. We want just to accumulate the replication
         * stream (instead of replying -BUSY like we do with other clients) and
         * later resume the processing. */
        if (isInsideYieldingLongCommand() && c->flags & CLIENT_MASTER) break;

        /* CLIENT_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands).
         *
         * The same applies for clients we want to terminate ASAP. */
        if (c->flags & (CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP)) break;

        /* Determine request type when unknown. */
        if (!c->reqtype) {
            if (c->querybuf[c->qb_pos] == '*') {
                c->reqtype = PROTO_REQ_MULTIBULK;
            } else {
                c->reqtype = PROTO_REQ_INLINE;
            }
        }

        if (c->reqtype == PROTO_REQ_INLINE) {
            if (processInlineBuffer(c) != C_OK) break;
        } else if (c->reqtype == PROTO_REQ_MULTIBULK) {
            if (processMultibulkBuffer(c) != C_OK) break;
        } else {
            serverPanic("Unknown request type");
        }

        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            resetClient(c);
        } else {
            /* If we are in the context of an I/O thread, we can't really
             * execute the command here. All we can do is to flag the client
             * as one that needs to process the command. */
            if (io_threads_op != IO_THREADS_OP_IDLE) {
                serverAssert(io_threads_op == IO_THREADS_OP_READ);
                c->flags |= CLIENT_PENDING_COMMAND;
                break;
            }

            /* We are finally ready to execute the command. */
            if (processCommandAndResetClient(c) == C_ERR) {
                /* If the client is no longer valid, we avoid exiting this
                 * loop and trimming the client buffer later. So we return
                 * ASAP in that case. */
                return C_ERR;
            }
        }
    }

    if (c->flags & CLIENT_MASTER) {
        /* If the client is a master, trim the querybuf to repl_applied,
         * since master client is very special, its querybuf not only
         * used to parse command, but also proxy to sub-replicas.
         *
         * Here are some scenarios we cannot trim to qb_pos:
         * 1. we don't receive complete command from master
         * 2. master client blocked cause of client pause
         * 3. io threads operate read, master client flagged with CLIENT_PENDING_COMMAND
         *
         * In these scenarios, qb_pos points to the part of the current command
         * or the beginning of next command, and the current command is not applied yet,
         * so the repl_applied is not equal to qb_pos. */
        if (c->repl_applied) {
            sdsrange(c->querybuf,c->repl_applied,-1);
            c->qb_pos -= c->repl_applied;
            c->repl_applied = 0;
        }
    } else if (c->qb_pos) {
        /* Trim to pos */
        sdsrange(c->querybuf,c->qb_pos,-1);
        c->qb_pos = 0;
    }

    /* Update client memory usage after processing the query buffer, this is
     * important in case the query buffer is big and wasn't drained during
     * the above loop (because of partially sent big commands). */
    if (io_threads_op == IO_THREADS_OP_IDLE)
        updateClientMemUsageAndBucket(c);

    return C_OK;
}

void readQueryFromClient(connection *conn) {
    client *c = connGetPrivateData(conn);
    int nread, big_arg = 0;
    size_t qblen, readlen;

    /* Check if we want to read from the client later when exiting from
     * the event loop. This is the case if threaded I/O is enabled. */
    if (postponeClientRead(c)) return;

    /* Update total number of reads on server */
    atomicIncr(server.stat_total_reads_processed, 1);

    readlen = PROTO_IOBUF_LEN;
    /* If this is a multi bulk request, and we are processing a bulk reply
     * that is large enough, try to maximize the probability that the query
     * buffer contains exactly the SDS string representing the object, even
     * at the risk of requiring more read(2) calls. This way the function
     * processMultiBulkBuffer() can avoid copying buffers to create the
     * Redis Object representing the argument. */
    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1
        && c->bulklen >= PROTO_MBULK_BIG_ARG)
    {
        ssize_t remaining = (size_t)(c->bulklen+2)-(sdslen(c->querybuf)-c->qb_pos);
        big_arg = 1;

        /* Note that the 'remaining' variable may be zero in some edge case,
         * for example once we resume a blocked client after CLIENT PAUSE. */
        if (remaining > 0) readlen = remaining;

        /* Master client needs expand the readlen when meet BIG_ARG(see #9100),
         * but doesn't need align to the next arg, we can read more data. */
        if (c->flags & CLIENT_MASTER && readlen < PROTO_IOBUF_LEN)
            readlen = PROTO_IOBUF_LEN;
    }

    qblen = sdslen(c->querybuf);
    if (!(c->flags & CLIENT_MASTER) && // master client's querybuf can grow greedy.
        (big_arg || sdsalloc(c->querybuf) < PROTO_IOBUF_LEN)) {
        /* When reading a BIG_ARG we won't be reading more than that one arg
         * into the query buffer, so we don't need to pre-allocate more than we
         * need, so using the non-greedy growing. For an initial allocation of
         * the query buffer, we also don't wanna use the greedy growth, in order
         * to avoid collision with the RESIZE_THRESHOLD mechanism. */
        c->querybuf = sdsMakeRoomForNonGreedy(c->querybuf, readlen);
        /* We later set the peak to the used portion of the buffer, but here we over
         * allocated because we know what we need, make sure it'll not be shrunk before used. */
        if (c->querybuf_peak < qblen + readlen) c->querybuf_peak = qblen + readlen;
    } else {
        c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);

        /* Read as much as possible from the socket to save read(2) system calls. */
        readlen = sdsavail(c->querybuf);
    }
    nread = connRead(c->conn, c->querybuf+qblen, readlen);
    if (nread == -1) {
        if (connGetState(conn) == CONN_STATE_CONNECTED) {
            return;
        } else {
            serverLog(LL_VERBOSE, "Reading from client: %s",connGetLastError(c->conn));
            freeClientAsync(c);
            goto done;
        }
    } else if (nread == 0) {
        if (server.verbosity <= LL_VERBOSE) {
            sds info = catClientInfoString(sdsempty(), c);
            serverLog(LL_VERBOSE, "Client closed connection %s", info);
            sdsfree(info);
        }
        freeClientAsync(c);
        goto done;
    }

    sdsIncrLen(c->querybuf,nread);
    qblen = sdslen(c->querybuf);
    if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;

    c->lastinteraction = server.unixtime;
    if (c->flags & CLIENT_MASTER) {
        c->read_reploff += nread;
        atomicIncr(server.stat_net_repl_input_bytes, nread);
    } else {
        atomicIncr(server.stat_net_input_bytes, nread);
    }

    if (!(c->flags & CLIENT_MASTER) &&
        /* The commands cached in the MULTI/EXEC queue have not been executed yet,
         * so they are also considered a part of the query buffer in a broader sense.
         *
         * For unauthenticated clients, the query buffer cannot exceed 1MB at most. */
        (c->mstate.argv_len_sums + sdslen(c->querybuf) > server.client_max_querybuf_len ||
         (c->mstate.argv_len_sums + sdslen(c->querybuf) > 1024*1024 && authRequired(c)))) {
        sds ci = catClientInfoString(sdsempty(),c), bytes = sdsempty();

        bytes = sdscatrepr(bytes,c->querybuf,64);
        serverLog(LL_WARNING,"Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci, bytes);
        sdsfree(ci);
        sdsfree(bytes);
        freeClientAsync(c);
        atomicIncr(server.stat_client_qbuf_limit_disconnections, 1);
        goto done;
    }

    /* There is more data in the client input buffer, continue parsing it
     * and check if there is a full command to execute. */
    if (processInputBuffer(c) == C_ERR)
         c = NULL;

done:
    beforeNextClient(c);
}

/* A Redis "Address String" is a colon separated ip:port pair.
 * For IPv4 it's in the form x.y.z.k:port, example: "127.0.0.1:1234".
 * For IPv6 addresses we use [] around the IP part, like in "[::1]:1234".
 * For Unix sockets we use path:0, like in "/tmp/redis:0".
 *
 * An Address String always fits inside a buffer of NET_ADDR_STR_LEN bytes,
 * including the null term.
 *
 * On failure the function still populates 'addr' with the "?:0" string in case
 * you want to relax error checking or need to display something anyway (see
 * anetFdToString implementation for more info). */
void genClientAddrString(client *client, char *addr,
                         size_t addr_len, int remote) {
    if (client->flags & CLIENT_UNIX_SOCKET) {
        /* Unix socket client. */
        snprintf(addr,addr_len,"%s:0",server.unixsocket);
    } else {
        /* TCP client. */
        connFormatAddr(client->conn,addr,addr_len,remote);
    }
}

/* This function returns the client peer id, by creating and caching it
 * if client->peerid is NULL, otherwise returning the cached value.
 * The Peer ID never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientPeerId(client *c) {
    char peerid[NET_ADDR_STR_LEN] = {0};

    if (c->peerid == NULL) {
        genClientAddrString(c,peerid,sizeof(peerid),1);
        c->peerid = sdsnew(peerid);
    }
    return c->peerid;
}

/* This function returns the client bound socket name, by creating and caching
 * it if client->sockname is NULL, otherwise returning the cached value.
 * The Socket Name never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientSockname(client *c) {
    char sockname[NET_ADDR_STR_LEN] = {0};

    if (c->sockname == NULL) {
        genClientAddrString(c,sockname,sizeof(sockname),0);
        c->sockname = sdsnew(sockname);
    }
    return c->sockname;
}

/* Concatenate a string representing the state of a client in a human
 * readable format, into the sds string 's'. */
sds catClientInfoString(sds s, client *client) {
    char flags[17], events[3], conninfo[CONN_INFO_LEN], *p;

    p = flags;
    if (client->flags & CLIENT_SLAVE) {
        if (client->flags & CLIENT_MONITOR)
            *p++ = 'O';
        else
            *p++ = 'S';
    }
    if (client->flags & CLIENT_MASTER) *p++ = 'M';
    if (client->flags & CLIENT_PUBSUB) *p++ = 'P';
    if (client->flags & CLIENT_MULTI) *p++ = 'x';
    if (client->flags & CLIENT_BLOCKED) *p++ = 'b';
    if (client->flags & CLIENT_TRACKING) *p++ = 't';
    if (client->flags & CLIENT_TRACKING_BROKEN_REDIR) *p++ = 'R';
    if (client->flags & CLIENT_TRACKING_BCAST) *p++ = 'B';
    if (client->flags & CLIENT_DIRTY_CAS) *p++ = 'd';
    if (client->flags & CLIENT_CLOSE_AFTER_REPLY) *p++ = 'c';
    if (client->flags & CLIENT_UNBLOCKED) *p++ = 'u';
    if (client->flags & CLIENT_CLOSE_ASAP) *p++ = 'A';
    if (client->flags & CLIENT_UNIX_SOCKET) *p++ = 'U';
    if (client->flags & CLIENT_READONLY) *p++ = 'r';
    if (client->flags & CLIENT_NO_EVICT) *p++ = 'e';
    if (client->flags & CLIENT_NO_TOUCH) *p++ = 'T';
    if (p == flags) *p++ = 'N';
    *p++ = '\0';

    p = events;
    if (client->conn) {
        if (connHasReadHandler(client->conn)) *p++ = 'r';
        if (connHasWriteHandler(client->conn)) *p++ = 'w';
    }
    *p = '\0';

    /* Compute the total memory consumed by this client. */
    size_t obufmem, total_mem = getClientMemoryUsage(client, &obufmem);

    size_t used_blocks_of_repl_buf = 0;
    if (client->ref_repl_buf_node) {
        replBufBlock *last = listNodeValue(listLast(server.repl_buffer_blocks));
        replBufBlock *cur = listNodeValue(client->ref_repl_buf_node);
        used_blocks_of_repl_buf = last->id - cur->id + 1;
    }

    sds ret = sdscatfmt(s, FMTARGS(
        "id=%U", (unsigned long long) client->id,
        " addr=%s", getClientPeerId(client),
        " laddr=%s", getClientSockname(client),
        " %s", connGetInfo(client->conn, conninfo, sizeof(conninfo)),
        " name=%s", client->name ? (char*)client->name->ptr : "",
        " age=%I", (long long)(commandTimeSnapshot() / 1000 - client->ctime),
        " idle=%I", (long long)(server.unixtime - client->lastinteraction),
        " flags=%s", flags,
        " db=%i", client->db->id,
        " sub=%i", (int) dictSize(client->pubsub_channels),
        " psub=%i", (int) dictSize(client->pubsub_patterns),
        " ssub=%i", (int) dictSize(client->pubsubshard_channels),
        " multi=%i", (client->flags & CLIENT_MULTI) ? client->mstate.count : -1,
        " watch=%i", (int) listLength(client->watched_keys),
        " qbuf=%U", (unsigned long long) sdslen(client->querybuf),
        " qbuf-free=%U", (unsigned long long) sdsavail(client->querybuf),
        " argv-mem=%U", (unsigned long long) client->argv_len_sum,
        " multi-mem=%U", (unsigned long long) client->mstate.argv_len_sums,
        " rbs=%U", (unsigned long long) client->buf_usable_size,
        " rbp=%U", (unsigned long long) client->buf_peak,
        " obl=%U", (unsigned long long) client->bufpos,
        " oll=%U", (unsigned long long) listLength(client->reply) + used_blocks_of_repl_buf,
        " omem=%U", (unsigned long long) obufmem, /* should not include client->buf since we want to see 0 for static clients. */
        " tot-mem=%U", (unsigned long long) total_mem,
        " events=%s", events,
        " cmd=%s", client->lastcmd ? client->lastcmd->fullname : "NULL",
        " user=%s", client->user ? client->user->name : "(superuser)",
        " redir=%I", (client->flags & CLIENT_TRACKING) ? (long long) client->client_tracking_redirection : -1,
        " resp=%i", client->resp,
        " lib-name=%s", client->lib_name ? (char*)client->lib_name->ptr : "",
        " lib-ver=%s", client->lib_ver ? (char*)client->lib_ver->ptr : ""));
    return ret;
}

sds getAllClientsInfoString(int type) {
    listNode *ln;
    listIter li;
    client *client;
    sds o = sdsnewlen(SDS_NOINIT,200*listLength(server.clients));
    sdsclear(o);
    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        client = listNodeValue(ln);
        if (type != -1 && getClientType(client) != type) continue;
        o = catClientInfoString(o,client);
        o = sdscatlen(o,"\n",1);
    }
    return o;
}

/* Check validity of an attribute that's gonna be shown in CLIENT LIST. */
int validateClientAttr(const char *val) {
    /* Check if the charset is ok. We need to do this otherwise
     * CLIENT LIST format will break. You should always be able to
     * split by space to get the different fields. */
    while (*val) {
        if (*val < '!' || *val > '~') { /* ASCII is assumed. */
            return C_ERR;
        }
        val++;
    }
    return C_OK;
}

/* Returns C_OK if the name is valid. Returns C_ERR & sets `err` (when provided) otherwise. */
int validateClientName(robj *name, const char **err) {
    const char *err_msg = "Client names cannot contain spaces, newlines or special characters.";
    int len = (name != NULL) ? sdslen(name->ptr) : 0;
    /* We allow setting the client name to an empty string. */
    if (len == 0)
        return C_OK;
    if (validateClientAttr(name->ptr) == C_ERR) {
        if (err) *err = err_msg;
        return C_ERR;
    }
    return C_OK;
}

/* Returns C_OK if the name has been set or C_ERR if the name is invalid. */
int clientSetName(client *c, robj *name, const char **err) {
    if (validateClientName(name, err) == C_ERR) {
        return C_ERR;
    }
    int len = (name != NULL) ? sdslen(name->ptr) : 0;
    /* Setting the client name to an empty string actually removes
     * the current name. */
    if (len == 0) {
        if (c->name) decrRefCount(c->name);
        c->name = NULL;
        return C_OK;
    }
    if (c->name) decrRefCount(c->name);
    c->name = name;
    incrRefCount(name);
    return C_OK;
}

/* This function implements CLIENT SETNAME, including replying to the
 * user with an error if the charset is wrong (in that case C_ERR is
 * returned). If the function succeeded C_OK is returned, and it's up
 * to the caller to send a reply if needed.
 *
 * Setting an empty string as name has the effect of unsetting the
 * currently set name: the client will remain unnamed.
 *
 * This function is also used to implement the HELLO SETNAME option. */
int clientSetNameOrReply(client *c, robj *name) {
    const char *err = NULL;
    int result = clientSetName(c, name, &err);
    if (result == C_ERR) {
        addReplyError(c, err);
    }
    return result;
}

/* Set client or connection related info */
void clientSetinfoCommand(client *c) {
    sds attr = c->argv[2]->ptr;
    robj *valob = c->argv[3];
    sds val = valob->ptr;
    robj **destvar = NULL;
    if (!strcasecmp(attr,"lib-name")) {
        destvar = &c->lib_name;
    } else if (!strcasecmp(attr,"lib-ver")) {
        destvar = &c->lib_ver;
    } else {
        addReplyErrorFormat(c,"Unrecognized option '%s'", attr);
        return;
    }

    if (validateClientAttr(val)==C_ERR) {
        addReplyErrorFormat(c,
            "%s cannot contain spaces, newlines or special characters.", attr);
        return;
    }
    if (*destvar) decrRefCount(*destvar);
    if (sdslen(val)) {
        *destvar = valob;
        incrRefCount(valob);
    } else
        *destvar = NULL;
    addReply(c,shared.ok);
}

/* Reset the client state to resemble a newly connected client.
 */
void resetCommand(client *c) {
    /* MONITOR clients are also marked with CLIENT_SLAVE, we need to
     * distinguish between the two.
     */
    uint64_t flags = c->flags;
    if (flags & CLIENT_MONITOR) flags &= ~(CLIENT_MONITOR|CLIENT_SLAVE);

    if (flags & (CLIENT_SLAVE|CLIENT_MASTER|CLIENT_MODULE)) {
        addReplyError(c,"can only reset normal client connections");
        return;
    }

    clearClientConnectionState(c);
    addReplyStatus(c,"RESET");
}

/* Disconnect the current client */
void quitCommand(client *c) {
    addReply(c,shared.ok);
    c->flags |= CLIENT_CLOSE_AFTER_REPLY;
}

void clientCommand(client *c) {
    listNode *ln;
    listIter li;

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"CACHING (YES|NO)",
"    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
"GETREDIR",
"    Return the client ID we are redirecting to when tracking is enabled.",
"GETNAME",
"    Return the name of the current connection.",
"ID",
"    Return the ID of the current connection.",
"INFO",
"    Return information about the current client connection.",
"KILL <ip:port>",
"    Kill connection made from <ip:port>.",
"KILL <option> <value> [<option> <value> [...]]",
"    Kill connections. Options are:",
"    * ADDR (<ip:port>|<unixsocket>:0)",
"      Kill connections made from the specified address",
"    * LADDR (<ip:port>|<unixsocket>:0)",
"      Kill connections made to specified local address",
"    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
"      Kill connections by type.",
"    * USER <username>",
"      Kill connections authenticated by <username>.",
"    * SKIPME (YES|NO)",
"      Skip killing current connection (default: yes).",
"    * ID <client-id>",
"      Kill connections by client id.",
"    * MAXAGE <maxage>",
"      Kill connections older than the specified age.",
"LIST [options ...]",
"    Return information about client connections. Options:",
"    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
"      Return clients of specified type.",
"UNPAUSE",
"    Stop the current client pause, resuming traffic.",
"PAUSE <timeout> [WRITE|ALL]",
"    Suspend all, or just write, clients for <timeout> milliseconds.",
"REPLY (ON|OFF|SKIP)",
"    Control the replies sent to the current connection.",
"SETNAME <name>",
"    Assign the name <name> to the current connection.",
"SETINFO <option> <value>",
"    Set client meta attr. Options are:",
"    * LIB-NAME: the client lib name.",
"    * LIB-VER: the client lib version.",
"UNBLOCK <clientid> [TIMEOUT|ERROR]",
"    Unblock the specified blocked client.",
"TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
"         [OPTIN] [OPTOUT] [NOLOOP]",
"    Control server assisted client side caching.",
"TRACKINGINFO",
"    Report tracking status for the current connection.",
"NO-EVICT (ON|OFF)",
"    Protect current client connection from eviction.",
"NO-TOUCH (ON|OFF)",
"    Will not touch LRU/LFU stats when this mode is on.",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"id") && c->argc == 2) {
        /* CLIENT ID */
        addReplyLongLong(c,c->id);
    } else if (!strcasecmp(c->argv[1]->ptr,"info") && c->argc == 2) {
        /* CLIENT INFO */
        sds o = catClientInfoString(sdsempty(), c);
        o = sdscatlen(o,"\n",1);
        addReplyVerbatim(c,o,sdslen(o),"txt");
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr,"list")) {
        /* CLIENT LIST */
        int type = -1;
        sds o = NULL;
        if (c->argc == 4 && !strcasecmp(c->argv[2]->ptr,"type")) {
            type = getClientTypeByName(c->argv[3]->ptr);
            if (type == -1) {
                addReplyErrorFormat(c,"Unknown client type '%s'",
                    (char*) c->argv[3]->ptr);
                return;
            }
        } else if (c->argc > 3 && !strcasecmp(c->argv[2]->ptr,"id")) {
            int j;
            o = sdsempty();
            for (j = 3; j < c->argc; j++) {
                long long cid;
                if (getLongLongFromObjectOrReply(c, c->argv[j], &cid,
                            "Invalid client ID")) {
                    sdsfree(o);
                    return;
                }
                client *cl = lookupClientByID(cid);
                if (cl) {
                    o = catClientInfoString(o, cl);
                    o = sdscatlen(o, "\n", 1);
                }
            }
        } else if (c->argc != 2) {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }

        if (!o)
            o = getAllClientsInfoString(type);
        addReplyVerbatim(c,o,sdslen(o),"txt");
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr,"reply") && c->argc == 3) {
        /* CLIENT REPLY ON|OFF|SKIP */
        if (!strcasecmp(c->argv[2]->ptr,"on")) {
            c->flags &= ~(CLIENT_REPLY_SKIP|CLIENT_REPLY_OFF);
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"off")) {
            c->flags |= CLIENT_REPLY_OFF;
        } else if (!strcasecmp(c->argv[2]->ptr,"skip")) {
            if (!(c->flags & CLIENT_REPLY_OFF))
                c->flags |= CLIENT_REPLY_SKIP_NEXT;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"no-evict") && c->argc == 3) {
        /* CLIENT NO-EVICT ON|OFF */
        if (!strcasecmp(c->argv[2]->ptr,"on")) {
            c->flags |= CLIENT_NO_EVICT;
            removeClientFromMemUsageBucket(c, 0);
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"off")) {
            c->flags &= ~CLIENT_NO_EVICT;
            updateClientMemUsageAndBucket(c);
            addReply(c,shared.ok);
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"kill")) {
        /* CLIENT KILL <ip:port>
         * CLIENT KILL <option> [value] ... <option> [value] */
        char *addr = NULL;
        char *laddr = NULL;
        user *user = NULL;
        int type = -1;
        uint64_t id = 0;
        long long max_age = 0;
        int skipme = 1;
        int killed = 0, close_this_client = 0;

        if (c->argc == 3) {
            /* Old style syntax: CLIENT KILL <addr> */
            addr = c->argv[2]->ptr;
            skipme = 0; /* With the old form, you can kill yourself. */
        } else if (c->argc > 3) {
            int i = 2; /* Next option index. */

            /* New style syntax: parse options. */
            while(i < c->argc) {
                int moreargs = c->argc > i+1;

                if (!strcasecmp(c->argv[i]->ptr,"id") && moreargs) {
                    long tmp;

                    if (getRangeLongFromObjectOrReply(c, c->argv[i+1], 1, LONG_MAX, &tmp,
                                                      "client-id should be greater than 0") != C_OK)
                        return;
                    id = tmp;
                } else if (!strcasecmp(c->argv[i]->ptr,"maxage") && moreargs) {
                    long long tmp;

                    if (getLongLongFromObjectOrReply(c, c->argv[i+1], &tmp,
                                                     "maxage is not an integer or out of range") != C_OK)
                        return;
                    if (tmp <= 0) {
                        addReplyError(c, "maxage should be greater than 0");
                        return;
                    }

                    max_age = tmp;
                } else if (!strcasecmp(c->argv[i]->ptr,"type") && moreargs) {
                    type = getClientTypeByName(c->argv[i+1]->ptr);
                    if (type == -1) {
                        addReplyErrorFormat(c,"Unknown client type '%s'",
                            (char*) c->argv[i+1]->ptr);
                        return;
                    }
                } else if (!strcasecmp(c->argv[i]->ptr,"addr") && moreargs) {
                    addr = c->argv[i+1]->ptr;
                } else if (!strcasecmp(c->argv[i]->ptr,"laddr") && moreargs) {
                    laddr = c->argv[i+1]->ptr;
                } else if (!strcasecmp(c->argv[i]->ptr,"user") && moreargs) {
                    user = ACLGetUserByName(c->argv[i+1]->ptr,
                                            sdslen(c->argv[i+1]->ptr));
                    if (user == NULL) {
                        addReplyErrorFormat(c,"No such user '%s'",
                            (char*) c->argv[i+1]->ptr);
                        return;
                    }
                } else if (!strcasecmp(c->argv[i]->ptr,"skipme") && moreargs) {
                    if (!strcasecmp(c->argv[i+1]->ptr,"yes")) {
                        skipme = 1;
                    } else if (!strcasecmp(c->argv[i+1]->ptr,"no")) {
                        skipme = 0;
                    } else {
                        addReplyErrorObject(c,shared.syntaxerr);
                        return;
                    }
                } else {
                    addReplyErrorObject(c,shared.syntaxerr);
                    return;
                }
                i += 2;
            }
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }

        /* Iterate clients killing all the matching clients. */
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            client *client = listNodeValue(ln);
            if (addr && strcmp(getClientPeerId(client),addr) != 0) continue;
            if (laddr && strcmp(getClientSockname(client),laddr) != 0) continue;
            if (type != -1 && getClientType(client) != type) continue;
            if (id != 0 && client->id != id) continue;
            if (user && client->user != user) continue;
            if (c == client && skipme) continue;
            if (max_age != 0 && (long long)(commandTimeSnapshot() / 1000 - client->ctime) < max_age) continue;

            /* Kill it. */
            if (c == client) {
                close_this_client = 1;
            } else {
                freeClient(client);
            }
            killed++;
        }

        /* Reply according to old/new format. */
        if (c->argc == 3) {
            if (killed == 0)
                addReplyError(c,"No such client");
            else
                addReply(c,shared.ok);
        } else {
            addReplyLongLong(c,killed);
        }

        /* If this client has to be closed, flag it as CLOSE_AFTER_REPLY
         * only after we queued the reply to its output buffers. */
        if (close_this_client) c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    } else if (!strcasecmp(c->argv[1]->ptr,"unblock") && (c->argc == 3 ||
                                                          c->argc == 4))
    {
        /* CLIENT UNBLOCK <id> [timeout|error] */
        long long id;
        int unblock_error = 0;

        if (c->argc == 4) {
            if (!strcasecmp(c->argv[3]->ptr,"timeout")) {
                unblock_error = 0;
            } else if (!strcasecmp(c->argv[3]->ptr,"error")) {
                unblock_error = 1;
            } else {
                addReplyError(c,
                    "CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
                return;
            }
        }
        if (getLongLongFromObjectOrReply(c,c->argv[2],&id,NULL)
            != C_OK) return;
        struct client *target = lookupClientByID(id);
        /* Note that we never try to unblock a client blocked on a module command, which
         * doesn't have a timeout callback (even in the case of UNBLOCK ERROR).
         * The reason is that we assume that if a command doesn't expect to be timedout,
         * it also doesn't expect to be unblocked by CLIENT UNBLOCK */
        if (target && target->flags & CLIENT_BLOCKED && moduleBlockedClientMayTimeout(target)) {
            if (unblock_error)
                unblockClientOnError(target,
                    "-UNBLOCKED client unblocked via CLIENT UNBLOCK");
            else
                unblockClientOnTimeout(target);

            addReply(c,shared.cone);
        } else {
            addReply(c,shared.czero);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"setname") && c->argc == 3) {
        /* CLIENT SETNAME */
        if (clientSetNameOrReply(c,c->argv[2]) == C_OK)
            addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"getname") && c->argc == 2) {
        /* CLIENT GETNAME */
        if (c->name)
            addReplyBulk(c,c->name);
        else
            addReplyNull(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"unpause") && c->argc == 2) {
        /* CLIENT UNPAUSE */
        unpauseActions(PAUSE_BY_CLIENT_COMMAND);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"pause") && (c->argc == 3 ||
                                                        c->argc == 4))
    {
        /* CLIENT PAUSE TIMEOUT [WRITE|ALL] */
        mstime_t end;
        int isPauseClientAll = 1;
        if (c->argc == 4) {
            if (!strcasecmp(c->argv[3]->ptr,"write")) {
                isPauseClientAll = 0;
            } else if (strcasecmp(c->argv[3]->ptr,"all")) {
                addReplyError(c,
                    "CLIENT PAUSE mode must be WRITE or ALL");  
                return;       
            }
        }

        if (getTimeoutFromObjectOrReply(c,c->argv[2],&end,
            UNIT_MILLISECONDS) != C_OK) return;
        pauseClientsByClient(end, isPauseClientAll);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"tracking") && c->argc >= 3) {
        /* CLIENT TRACKING (on|off) [REDIRECT <id>] [BCAST] [PREFIX first]
         *                          [PREFIX second] [OPTIN] [OPTOUT] [NOLOOP]... */
        long long redir = 0;
        uint64_t options = 0;
        robj **prefix = NULL;
        size_t numprefix = 0;

        /* Parse the options. */
        for (int j = 3; j < c->argc; j++) {
            int moreargs = (c->argc-1) - j;

            if (!strcasecmp(c->argv[j]->ptr,"redirect") && moreargs) {
                j++;
                if (redir != 0) {
                    addReplyError(c,"A client can only redirect to a single "
                                    "other client");
                    zfree(prefix);
                    return;
                }

                if (getLongLongFromObjectOrReply(c,c->argv[j],&redir,NULL) !=
                    C_OK)
                {
                    zfree(prefix);
                    return;
                }
                /* We will require the client with the specified ID to exist
                 * right now, even if it is possible that it gets disconnected
                 * later. Still a valid sanity check. */
                if (lookupClientByID(redir) == NULL) {
                    addReplyError(c,"The client ID you want redirect to "
                                    "does not exist");
                    zfree(prefix);
                    return;
                }
            } else if (!strcasecmp(c->argv[j]->ptr,"bcast")) {
                options |= CLIENT_TRACKING_BCAST;
            } else if (!strcasecmp(c->argv[j]->ptr,"optin")) {
                options |= CLIENT_TRACKING_OPTIN;
            } else if (!strcasecmp(c->argv[j]->ptr,"optout")) {
                options |= CLIENT_TRACKING_OPTOUT;
            } else if (!strcasecmp(c->argv[j]->ptr,"noloop")) {
                options |= CLIENT_TRACKING_NOLOOP;
            } else if (!strcasecmp(c->argv[j]->ptr,"prefix") && moreargs) {
                j++;
                prefix = zrealloc(prefix,sizeof(robj*)*(numprefix+1));
                prefix[numprefix++] = c->argv[j];
            } else {
                zfree(prefix);
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }

        /* Options are ok: enable or disable the tracking for this client. */
        if (!strcasecmp(c->argv[2]->ptr,"on")) {
            /* Before enabling tracking, make sure options are compatible
             * among each other and with the current state of the client. */
            if (!(options & CLIENT_TRACKING_BCAST) && numprefix) {
                addReplyError(c,
                    "PREFIX option requires BCAST mode to be enabled");
                zfree(prefix);
                return;
            }

            if (c->flags & CLIENT_TRACKING) {
                int oldbcast = !!(c->flags & CLIENT_TRACKING_BCAST);
                int newbcast = !!(options & CLIENT_TRACKING_BCAST);
                if (oldbcast != newbcast) {
                    addReplyError(c,
                    "You can't switch BCAST mode on/off before disabling "
                    "tracking for this client, and then re-enabling it with "
                    "a different mode.");
                    zfree(prefix);
                    return;
                }
            }

            if (options & CLIENT_TRACKING_BCAST &&
                options & (CLIENT_TRACKING_OPTIN|CLIENT_TRACKING_OPTOUT))
            {
                addReplyError(c,
                "OPTIN and OPTOUT are not compatible with BCAST");
                zfree(prefix);
                return;
            }

            if (options & CLIENT_TRACKING_OPTIN && options & CLIENT_TRACKING_OPTOUT)
            {
                addReplyError(c,
                "You can't specify both OPTIN mode and OPTOUT mode");
                zfree(prefix);
                return;
            }

            if ((options & CLIENT_TRACKING_OPTIN && c->flags & CLIENT_TRACKING_OPTOUT) ||
                (options & CLIENT_TRACKING_OPTOUT && c->flags & CLIENT_TRACKING_OPTIN))
            {
                addReplyError(c,
                "You can't switch OPTIN/OPTOUT mode before disabling "
                "tracking for this client, and then re-enabling it with "
                "a different mode.");
                zfree(prefix);
                return;
            }

            if (options & CLIENT_TRACKING_BCAST) {
                if (!checkPrefixCollisionsOrReply(c,prefix,numprefix)) {
                    zfree(prefix);
                    return;
                }
            }

            enableTracking(c,redir,options,prefix,numprefix);
        } else if (!strcasecmp(c->argv[2]->ptr,"off")) {
            disableTracking(c);
        } else {
            zfree(prefix);
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
        zfree(prefix);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"caching") && c->argc >= 3) {
        if (!(c->flags & CLIENT_TRACKING)) {
            addReplyError(c,"CLIENT CACHING can be called only when the "
                            "client is in tracking mode with OPTIN or "
                            "OPTOUT mode enabled");
            return;
        }

        char *opt = c->argv[2]->ptr;
        if (!strcasecmp(opt,"yes")) {
            if (c->flags & CLIENT_TRACKING_OPTIN) {
                c->flags |= CLIENT_TRACKING_CACHING;
            } else {
                addReplyError(c,"CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.");
                return;
            }
        } else if (!strcasecmp(opt,"no")) {
            if (c->flags & CLIENT_TRACKING_OPTOUT) {
                c->flags |= CLIENT_TRACKING_CACHING;
            } else {
                addReplyError(c,"CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.");
                return;
            }
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }

        /* Common reply for when we succeeded. */
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"getredir") && c->argc == 2) {
        /* CLIENT GETREDIR */
        if (c->flags & CLIENT_TRACKING) {
            addReplyLongLong(c,c->client_tracking_redirection);
        } else {
            addReplyLongLong(c,-1);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"trackinginfo") && c->argc == 2) {
        addReplyMapLen(c,3);

        /* Flags */
        addReplyBulkCString(c,"flags");
        void *arraylen_ptr = addReplyDeferredLen(c);
        int numflags = 0;
        addReplyBulkCString(c,c->flags & CLIENT_TRACKING ? "on" : "off");
        numflags++;
        if (c->flags & CLIENT_TRACKING_BCAST) {
            addReplyBulkCString(c,"bcast");
            numflags++;
        }
        if (c->flags & CLIENT_TRACKING_OPTIN) {
            addReplyBulkCString(c,"optin");
            numflags++;
            if (c->flags & CLIENT_TRACKING_CACHING) {
                addReplyBulkCString(c,"caching-yes");
                numflags++;        
            }
        }
        if (c->flags & CLIENT_TRACKING_OPTOUT) {
            addReplyBulkCString(c,"optout");
            numflags++;
            if (c->flags & CLIENT_TRACKING_CACHING) {
                addReplyBulkCString(c,"caching-no");
                numflags++;        
            }
        }
        if (c->flags & CLIENT_TRACKING_NOLOOP) {
            addReplyBulkCString(c,"noloop");
            numflags++;
        }
        if (c->flags & CLIENT_TRACKING_BROKEN_REDIR) {
            addReplyBulkCString(c,"broken_redirect");
            numflags++;
        }
        setDeferredSetLen(c,arraylen_ptr,numflags);

        /* Redirect */
        addReplyBulkCString(c,"redirect");
        if (c->flags & CLIENT_TRACKING) {
            addReplyLongLong(c,c->client_tracking_redirection);
        } else {
            addReplyLongLong(c,-1);
        }

        /* Prefixes */
        addReplyBulkCString(c,"prefixes");
        if (c->client_tracking_prefixes) {
            addReplyArrayLen(c,raxSize(c->client_tracking_prefixes));
            raxIterator ri;
            raxStart(&ri,c->client_tracking_prefixes);
            raxSeek(&ri,"^",NULL,0);
            while(raxNext(&ri)) {
                addReplyBulkCBuffer(c,ri.key,ri.key_len);
            }
            raxStop(&ri);
        } else {
            addReplyArrayLen(c,0);
        }
    } else if (!strcasecmp(c->argv[1]->ptr, "no-touch")) {
        /* CLIENT NO-TOUCH ON|OFF */
        if (!strcasecmp(c->argv[2]->ptr,"on")) {
            c->flags |= CLIENT_NO_TOUCH;
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"off")) {
            c->flags &= ~CLIENT_NO_TOUCH;
            addReply(c,shared.ok);
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* HELLO [<protocol-version> [AUTH <user> <password>] [SETNAME <name>] ] */
void helloCommand(client *c) {
    long long ver = 0;
    int next_arg = 1;

    if (c->argc >= 2) {
        if (getLongLongFromObjectOrReply(c, c->argv[next_arg++], &ver,
            "Protocol version is not an integer or out of range") != C_OK) {
            return;
        }

        if (ver < 2 || ver > 3) {
            addReplyError(c,"-NOPROTO unsupported protocol version");
            return;
        }
    }

    robj *username = NULL;
    robj *password = NULL;
    robj *clientname = NULL;
    for (int j = next_arg; j < c->argc; j++) {
        int moreargs = (c->argc-1) - j;
        const char *opt = c->argv[j]->ptr;
        if (!strcasecmp(opt,"AUTH") && moreargs >= 2) {
            redactClientCommandArgument(c, j+1);
            redactClientCommandArgument(c, j+2);
            username = c->argv[j+1];
            password = c->argv[j+2];
            j += 2;
        } else if (!strcasecmp(opt,"SETNAME") && moreargs) {
            clientname = c->argv[j+1];
            const char *err = NULL;
            if (validateClientName(clientname, &err) == C_ERR) {
                addReplyError(c, err);
                return;
            }
            j++;
        } else {
            addReplyErrorFormat(c,"Syntax error in HELLO option '%s'",opt);
            return;
        }
    }

    if (username && password) {
        robj *err = NULL;
        int auth_result = ACLAuthenticateUser(c, username, password, &err);
        if (auth_result == AUTH_ERR) {
            addAuthErrReply(c, err);
        }
        if (err) decrRefCount(err);
        /* In case of auth errors, return early since we already replied with an ERR.
         * In case of blocking module auth, we reply to the client/setname later upon unblocking. */
        if (auth_result == AUTH_ERR || auth_result == AUTH_BLOCKED) {
            return;
        }
    }

    /* At this point we need to be authenticated to continue. */
    if (!c->authenticated) {
        addReplyError(c,"-NOAUTH HELLO must be called with the client already "
                        "authenticated, otherwise the HELLO <proto> AUTH <user> <pass> "
                        "option can be used to authenticate the client and "
                        "select the RESP protocol version at the same time");
        return;
    }

    /* Now that we're authenticated, set the client name. */
    if (clientname) clientSetName(c, clientname, NULL);

    /* Let's switch to the specified RESP mode. */
    if (ver) c->resp = ver;
    addReplyMapLen(c,6 + !server.sentinel_mode);

    addReplyBulkCString(c,"server");
    addReplyBulkCString(c,"redis");

    addReplyBulkCString(c,"version");
    addReplyBulkCString(c,REDIS_VERSION);

    addReplyBulkCString(c,"proto");
    addReplyLongLong(c,c->resp);

    addReplyBulkCString(c,"id");
    addReplyLongLong(c,c->id);

    addReplyBulkCString(c,"mode");
    if (server.sentinel_mode) addReplyBulkCString(c,"sentinel");
    else if (server.cluster_enabled) addReplyBulkCString(c,"cluster");
    else addReplyBulkCString(c,"standalone");

    if (!server.sentinel_mode) {
        addReplyBulkCString(c,"role");
        addReplyBulkCString(c,server.masterhost ? "replica" : "master");
    }

    addReplyBulkCString(c,"modules");
    addReplyLoadedModules(c);
}

/* This callback is bound to POST and "Host:" command names. Those are not
 * really commands, but are used in security attacks in order to talk to
 * Redis instances via HTTP, with a technique called "cross protocol scripting"
 * which exploits the fact that services like Redis will discard invalid
 * HTTP headers and will process what follows.
 *
 * As a protection against this attack, Redis will terminate the connection
 * when a POST or "Host:" header is seen, and will log the event from
 * time to time (to avoid creating a DOS as a result of too many logs). */
void securityWarningCommand(client *c) {
    static time_t logged_time = 0;
    time_t now = time(NULL);

    if (llabs(now-logged_time) > 60) {
        char ip[NET_IP_STR_LEN];
        int port;
        if (connAddrPeerName(c->conn, ip, sizeof(ip), &port) == -1) {
            serverLog(LL_WARNING,"Possible SECURITY ATTACK detected. It looks like somebody is sending POST or Host: commands to Redis. This is likely due to an attacker attempting to use Cross Protocol Scripting to compromise your Redis instance. Connection aborted.");
        } else {
            serverLog(LL_WARNING,"Possible SECURITY ATTACK detected. It looks like somebody is sending POST or Host: commands to Redis. This is likely due to an attacker attempting to use Cross Protocol Scripting to compromise your Redis instance. Connection from %s:%d aborted.", ip, port);
        }
        logged_time = now;
    }
    freeClientAsync(c);
}

/* Keep track of the original command arguments so that we can generate
 * an accurate slowlog entry after the command has been executed. */
static void retainOriginalCommandVector(client *c) {
    /* We already rewrote this command, so don't rewrite it again */
    if (c->original_argv) return;
    c->original_argc = c->argc;
    c->original_argv = zmalloc(sizeof(robj*)*(c->argc));
    for (int j = 0; j < c->argc; j++) {
        c->original_argv[j] = c->argv[j];
        incrRefCount(c->argv[j]);
    }
}

/* Redact a given argument to prevent it from being shown
 * in the slowlog. This information is stored in the
 * original_argv array. */
void redactClientCommandArgument(client *c, int argc) {
    retainOriginalCommandVector(c);
    if (c->original_argv[argc] == shared.redacted) {
        /* This argument has already been redacted */
        return;
    }
    decrRefCount(c->original_argv[argc]);
    c->original_argv[argc] = shared.redacted;
}

/* Rewrite the command vector of the client. All the new objects ref count
 * is incremented. The old command vector is freed, and the old objects
 * ref count is decremented. */
void rewriteClientCommandVector(client *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = zmalloc(sizeof(robj*)*argc);
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        robj *a;

        a = va_arg(ap, robj*);
        argv[j] = a;
        incrRefCount(a);
    }
    replaceClientCommandVector(c, argc, argv);
    va_end(ap);
}

/* Completely replace the client command vector with the provided one. */
void replaceClientCommandVector(client *c, int argc, robj **argv) {
    int j;
    retainOriginalCommandVector(c);
    freeClientArgv(c);
    c->argv = argv;
    c->argc = argc;
    c->argv_len_sum = 0;
    for (j = 0; j < c->argc; j++)
        if (c->argv[j])
            c->argv_len_sum += getStringObjectLen(c->argv[j]);
    c->cmd = lookupCommandOrOriginal(c->argv,c->argc);
    serverAssertWithInfo(c,NULL,c->cmd != NULL);
}

/* Rewrite a single item in the command vector.
 * The new val ref count is incremented, and the old decremented.
 *
 * It is possible to specify an argument over the current size of the
 * argument vector: in this case the array of objects gets reallocated
 * and c->argc set to the max value. However it's up to the caller to
 *
 * 1. Make sure there are no "holes" and all the arguments are set.
 * 2. If the original argument vector was longer than the one we
 *    want to end with, it's up to the caller to set c->argc and
 *    free the no longer used objects on c->argv.
 * 3. To remove argument at i'th index, pass NULL as new value
 */
void rewriteClientCommandArgument(client *c, int i, robj *newval) {
    robj *oldval;
    retainOriginalCommandVector(c);

    /* We need to handle both extending beyond argc (just update it and
     * initialize the new element) or beyond argv_len (realloc is needed).
     */
    if (i >= c->argc) {
        if (i >= c->argv_len) {
            c->argv = zrealloc(c->argv,sizeof(robj*)*(i+1));
            c->argv_len = i+1;
        }
        c->argc = i+1;
        c->argv[i] = NULL;
    }
    oldval = c->argv[i];
    if (oldval) c->argv_len_sum -= getStringObjectLen(oldval);

    if (newval) {
        c->argv[i] = newval;
        incrRefCount(newval);
        c->argv_len_sum += getStringObjectLen(newval);
    } else {
        /* move the remaining arguments one step left */
        for (int j = i+1; j < c->argc; j++) {
            c->argv[j-1] = c->argv[j];
        }
        c->argv[--c->argc] = NULL;
    }
    if (oldval) decrRefCount(oldval);

    /* If this is the command name make sure to fix c->cmd. */
    if (i == 0) {
        c->cmd = lookupCommandOrOriginal(c->argv,c->argc);
        serverAssertWithInfo(c,NULL,c->cmd != NULL);
    }
}

/* This function returns the number of bytes that Redis is
 * using to store the reply still not read by the client.
 *
 * Note: this function is very fast so can be called as many time as
 * the caller wishes. The main usage of this function currently is
 * enforcing the client output length limits. */
size_t getClientOutputBufferMemoryUsage(client *c) {
    if (getClientType(c) == CLIENT_TYPE_SLAVE) {
        size_t repl_buf_size = 0;
        size_t repl_node_num = 0;
        size_t repl_node_size = sizeof(listNode) + sizeof(replBufBlock);
        if (c->ref_repl_buf_node) {
            replBufBlock *last = listNodeValue(listLast(server.repl_buffer_blocks));
            replBufBlock *cur = listNodeValue(c->ref_repl_buf_node);
            repl_buf_size = last->repl_offset + last->size - cur->repl_offset;
            repl_node_num = last->id - cur->id + 1;
        }
        return repl_buf_size + (repl_node_size*repl_node_num);
    } else { 
        size_t list_item_size = sizeof(listNode) + sizeof(clientReplyBlock);
        return c->reply_bytes + (list_item_size*listLength(c->reply));
    }
}

/* Returns the total client's memory usage.
 * Optionally, if output_buffer_mem_usage is not NULL, it fills it with
 * the client output buffer memory usage portion of the total. */
size_t getClientMemoryUsage(client *c, size_t *output_buffer_mem_usage) {
    size_t mem = getClientOutputBufferMemoryUsage(c);
    if (output_buffer_mem_usage != NULL)
        *output_buffer_mem_usage = mem;
    mem += sdsZmallocSize(c->querybuf);
    mem += zmalloc_size(c);
    mem += c->buf_usable_size;
    /* For efficiency (less work keeping track of the argv memory), it doesn't include the used memory
     * i.e. unused sds space and internal fragmentation, just the string length. but this is enough to
     * spot problematic clients. */
    mem += c->argv_len_sum + sizeof(robj*)*c->argc;
    mem += multiStateMemOverhead(c);

    /* Add memory overhead of pubsub channels and patterns. Note: this is just the overhead of the robj pointers
     * to the strings themselves because they aren't stored per client. */
    mem += pubsubMemOverhead(c);

    /* Add memory overhead of the tracking prefixes, this is an underestimation so we don't need to traverse the entire rax */
    if (c->client_tracking_prefixes)
        mem += c->client_tracking_prefixes->numnodes * (sizeof(raxNode) * sizeof(raxNode*));

    return mem;
}

/* Get the class of a client, used in order to enforce limits to different
 * classes of clients.
 *
 * The function will return one of the following:
 * CLIENT_TYPE_NORMAL -> Normal client, including MONITOR
 * CLIENT_TYPE_SLAVE  -> Slave
 * CLIENT_TYPE_PUBSUB -> Client subscribed to Pub/Sub channels
 * CLIENT_TYPE_MASTER -> The client representing our replication master.
 */
int getClientType(client *c) {
    if (c->flags & CLIENT_MASTER) return CLIENT_TYPE_MASTER;
    /* Even though MONITOR clients are marked as replicas, we
     * want the expose them as normal clients. */
    if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR))
        return CLIENT_TYPE_SLAVE;
    if (c->flags & CLIENT_PUBSUB) return CLIENT_TYPE_PUBSUB;
    return CLIENT_TYPE_NORMAL;
}

int getClientTypeByName(char *name) {
    if (!strcasecmp(name,"normal")) return CLIENT_TYPE_NORMAL;
    else if (!strcasecmp(name,"slave")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name,"replica")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name,"pubsub")) return CLIENT_TYPE_PUBSUB;
    else if (!strcasecmp(name,"master")) return CLIENT_TYPE_MASTER;
    else return -1;
}

char *getClientTypeName(int class) {
    switch(class) {
    case CLIENT_TYPE_NORMAL: return "normal";
    case CLIENT_TYPE_SLAVE:  return "slave";
    case CLIENT_TYPE_PUBSUB: return "pubsub";
    case CLIENT_TYPE_MASTER: return "master";
    default:                       return NULL;
    }
}

/* The function checks if the client reached output buffer soft or hard
 * limit, and also update the state needed to check the soft limit as
 * a side effect.
 *
 * Return value: non-zero if the client reached the soft or the hard limit.
 *               Otherwise zero is returned. */
int checkClientOutputBufferLimits(client *c) {
    int soft = 0, hard = 0, class;
    unsigned long used_mem = getClientOutputBufferMemoryUsage(c);

    /* For unauthenticated clients the output buffer is limited to prevent
     * them from abusing it by not reading the replies */
    if (used_mem > 1024 && authRequired(c))
        return 1;

    class = getClientType(c);
    /* For the purpose of output buffer limiting, masters are handled
     * like normal clients. */
    if (class == CLIENT_TYPE_MASTER) class = CLIENT_TYPE_NORMAL;

    /* Note that it doesn't make sense to set the replica clients output buffer
     * limit lower than the repl-backlog-size config (partial sync will succeed
     * and then replica will get disconnected).
     * Such a configuration is ignored (the size of repl-backlog-size will be used).
     * This doesn't have memory consumption implications since the replica client
     * will share the backlog buffers memory. */
    size_t hard_limit_bytes = server.client_obuf_limits[class].hard_limit_bytes;
    if (class == CLIENT_TYPE_SLAVE && hard_limit_bytes &&
        (long long)hard_limit_bytes < server.repl_backlog_size)
        hard_limit_bytes = server.repl_backlog_size;
    if (server.client_obuf_limits[class].hard_limit_bytes &&
        used_mem >= hard_limit_bytes)
        hard = 1;
    if (server.client_obuf_limits[class].soft_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].soft_limit_bytes)
        soft = 1;

    /* We need to check if the soft limit is reached continuously for the
     * specified amount of seconds. */
    if (soft) {
        if (c->obuf_soft_limit_reached_time == 0) {
            c->obuf_soft_limit_reached_time = server.unixtime;
            soft = 0; /* First time we see the soft limit reached */
        } else {
            time_t elapsed = server.unixtime - c->obuf_soft_limit_reached_time;

            if (elapsed <=
                server.client_obuf_limits[class].soft_limit_seconds) {
                soft = 0; /* The client still did not reached the max number of
                             seconds for the soft limit to be considered
                             reached. */
            }
        }
    } else {
        c->obuf_soft_limit_reached_time = 0;
    }
    return soft || hard;
}

/* Asynchronously close a client if soft or hard limit is reached on the
 * output buffer size. The caller can check if the client will be closed
 * checking if the client CLIENT_CLOSE_ASAP flag is set.
 *
 * Note: we need to close the client asynchronously because this function is
 * called from contexts where the client can't be freed safely, i.e. from the
 * lower level functions pushing data inside the client output buffers.
 * When `async` is set to 0, we close the client immediately, this is
 * useful when called from cron.
 *
 * Returns 1 if client was (flagged) closed. */
int closeClientOnOutputBufferLimitReached(client *c, int async) {
    if (!c->conn) return 0; /* It is unsafe to free fake clients. */
    serverAssert(c->reply_bytes < SIZE_MAX-(1024*64));
    /* Note that c->reply_bytes is irrelevant for replica clients
     * (they use the global repl buffers). */
    if ((c->reply_bytes == 0 && getClientType(c) != CLIENT_TYPE_SLAVE) ||
        c->flags & CLIENT_CLOSE_ASAP) return 0;
    if (checkClientOutputBufferLimits(c)) {
        sds client = catClientInfoString(sdsempty(),c);

        if (async) {
            freeClientAsync(c);
            serverLog(LL_WARNING,
                      "Client %s scheduled to be closed ASAP for overcoming of output buffer limits.",
                      client);
        } else {
            freeClient(c);
            serverLog(LL_WARNING,
                      "Client %s closed for overcoming of output buffer limits.",
                      client);
        }
        sdsfree(client);
        server.stat_client_outbuf_limit_disconnections++;
        return  1;
    }
    return 0;
}

/* Helper function used by performEvictions() in order to flush slaves
 * output buffers without returning control to the event loop.
 * This is also called by SHUTDOWN for a best-effort attempt to send
 * slaves the latest writes. */
void flushSlavesOutputBuffers(void) {
    listIter li;
    listNode *ln;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = listNodeValue(ln);
        int can_receive_writes = connHasWriteHandler(slave->conn) ||
                                 (slave->flags & CLIENT_PENDING_WRITE);

        /* We don't want to send the pending data to the replica in a few
         * cases:
         *
         * 1. For some reason there is neither the write handler installed
         *    nor the client is flagged as to have pending writes: for some
         *    reason this replica may not be set to receive data. This is
         *    just for the sake of defensive programming.
         *
         * 2. The put_online_on_ack flag is true. To know why we don't want
         *    to send data to the replica in this case, please grep for the
         *    flag for this flag.
         *
         * 3. Obviously if the slave is not ONLINE.
         */
        if (slave->replstate == SLAVE_STATE_ONLINE &&
            !(slave->flags & CLIENT_CLOSE_ASAP) &&
            can_receive_writes &&
            !slave->repl_start_cmd_stream_on_ack &&
            clientHasPendingReplies(slave))
        {
            writeToClient(slave,0);
        }
    }
}

/* Compute current paused actions and its end time, aggregated for
 * all pause purposes. */
void updatePausedActions(void) {
    uint32_t prev_paused_actions = server.paused_actions;
    server.paused_actions = 0;

    for (int i = 0; i < NUM_PAUSE_PURPOSES; i++) {
        pause_event *p = &(server.client_pause_per_purpose[i]);
        if (p->end > server.mstime)
            server.paused_actions |= p->paused_actions;
        else {
            p->paused_actions = 0;
            p->end = 0;
        }
    }

    /* If the pause type is less restrictive than before, we unblock all clients
     * so they are reprocessed (may get re-paused). */
    uint32_t mask_cli = (PAUSE_ACTION_CLIENT_WRITE|PAUSE_ACTION_CLIENT_ALL);
    if ((server.paused_actions & mask_cli) < (prev_paused_actions & mask_cli)) {
        unblockPostponedClients();
    }
}

/* Unblock all paused clients (ones that where blocked by BLOCKED_POSTPONE (possibly in processCommand).
 * This means they'll get re-processed in beforeSleep, and may get paused again if needed. */
void unblockPostponedClients(void) {
    listNode *ln;
    listIter li;
    listRewind(server.postponed_clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        unblockClient(c, 1);
    }
}

/* Set pause-client end-time and restricted action. If already paused, then:
 * 1. Keep higher end-time value between configured and the new one
 * 2. Keep most restrictive action between configured and the new one */
static void pauseClientsByClient(mstime_t endTime, int isPauseClientAll) {
    uint32_t actions;
    pause_event *p = &server.client_pause_per_purpose[PAUSE_BY_CLIENT_COMMAND];

    if (isPauseClientAll)
        actions = PAUSE_ACTIONS_CLIENT_ALL_SET;
    else {
        actions = PAUSE_ACTIONS_CLIENT_WRITE_SET;
        /* If currently configured most restrictive client pause, then keep it */
        if (p->paused_actions & PAUSE_ACTION_CLIENT_ALL)
            actions = PAUSE_ACTIONS_CLIENT_ALL_SET;
    }
    
    pauseActions(PAUSE_BY_CLIENT_COMMAND, endTime, actions);
}

/* Pause actions up to the specified unixtime (in ms) for a given type of
 * commands.
 *
 * A main use case of this function is to allow pausing replication traffic
 * so that a failover without data loss to occur. Replicas will continue to receive
 * traffic to facilitate this functionality.
 * 
 * This function is also internally used by Redis Cluster for the manual
 * failover procedure implemented by CLUSTER FAILOVER.
 *
 * The function always succeed, even if there is already a pause in progress.
 * The new paused_actions of a given 'purpose' will override the old ones and
 * end time will be updated if new end time is bigger than currently configured */
void pauseActions(pause_purpose purpose, mstime_t end, uint32_t actions) {
    /* Manage pause type and end time per pause purpose. */
    server.client_pause_per_purpose[purpose].paused_actions = actions;

    /* If currently configured end time bigger than new one, then keep it */
    if (server.client_pause_per_purpose[purpose].end < end)
        server.client_pause_per_purpose[purpose].end = end;

    updatePausedActions();

    /* We allow write commands that were queued
     * up before and after to execute. We need
     * to track this state so that we don't assert
     * in propagateNow(). */
    if (server.in_exec) {
        server.client_pause_in_transaction = 1;
    }
}

/* Unpause actions and queue them for reprocessing. */
void unpauseActions(pause_purpose purpose) {
    server.client_pause_per_purpose[purpose].end = 0;
    server.client_pause_per_purpose[purpose].paused_actions = 0;
    updatePausedActions();
}

/* Returns bitmask of paused actions */
uint32_t isPausedActions(uint32_t actions_bitmask) {
    return (server.paused_actions & actions_bitmask);
}

/* Returns bitmask of paused actions */
uint32_t isPausedActionsWithUpdate(uint32_t actions_bitmask) {
    if (!(server.paused_actions & actions_bitmask)) return 0;
    updatePausedActions();
    return (server.paused_actions & actions_bitmask);
}

/* This function is called by Redis in order to process a few events from
 * time to time while blocked into some not interruptible operation.
 * This allows to reply to clients with the -LOADING error while loading the
 * data set at startup or after a full resynchronization with the master
 * and so forth.
 *
 * It calls the event loop in order to process a few events. Specifically we
 * try to call the event loop 4 times as long as we receive acknowledge that
 * some event was processed, in order to go forward with the accept, read,
 * write, close sequence needed to serve a client.
 *
 * The function returns the total number of events processed. */
void processEventsWhileBlocked(void) {
    int iterations = 4; /* See the function top-comment. */

    /* Update our cached time since it is used to create and update the last
     * interaction time with clients and for other important things. */
    updateCachedTime(0);

    /* For the few commands that are allowed during busy scripts, we rather
     * provide a fresher time than the one from when the script started (they
     * still won't get it from the call due to execution_nesting. For commands
     * during loading this doesn't matter. */
    mstime_t prev_cmd_time_snapshot = server.cmd_time_snapshot;
    server.cmd_time_snapshot = server.mstime;

    /* Note: when we are processing events while blocked (for instance during
     * busy Lua scripts), we set a global flag. When such flag is set, we
     * avoid handling the read part of clients using threaded I/O.
     * See https://github.com/redis/redis/issues/6988 for more info.
     * Note that there could be cases of nested calls to this function,
     * specifically on a busy script during async_loading rdb, and scripts
     * that came from AOF. */
    ProcessingEventsWhileBlocked++;
    while (iterations--) {
        long long startval = server.events_processed_while_blocked;
        long long ae_events = aeProcessEvents(server.el,
            AE_FILE_EVENTS|AE_DONT_WAIT|
            AE_CALL_BEFORE_SLEEP|AE_CALL_AFTER_SLEEP);
        /* Note that server.events_processed_while_blocked will also get
         * incremented by callbacks called by the event loop handlers. */
        server.events_processed_while_blocked += ae_events;
        long long events = server.events_processed_while_blocked - startval;
        if (!events) break;
    }

    whileBlockedCron();

    ProcessingEventsWhileBlocked--;
    serverAssert(ProcessingEventsWhileBlocked >= 0);

    server.cmd_time_snapshot = prev_cmd_time_snapshot;
}

/* ==========================================================================
 * Threaded I/O
 * ========================================================================== */

#define IO_THREADS_MAX_NUM 128
#ifndef CACHE_LINE_SIZE
#if defined(__aarch64__) && defined(__APPLE__)
#define CACHE_LINE_SIZE 128
#else
#define CACHE_LINE_SIZE 64
#endif
#endif

typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) threads_pending {
    redisAtomic unsigned long value;
} threads_pending;

pthread_t io_threads[IO_THREADS_MAX_NUM];
pthread_mutex_t io_threads_mutex[IO_THREADS_MAX_NUM];
threads_pending io_threads_pending[IO_THREADS_MAX_NUM];
int io_threads_op;      /* IO_THREADS_OP_IDLE, IO_THREADS_OP_READ or IO_THREADS_OP_WRITE. */ // TODO: should access to this be atomic??!

/* This is the list of clients each thread will serve when threaded I/O is
 * used. We spawn io_threads_num-1 threads, since one is the main thread
 * itself. */
list *io_threads_list[IO_THREADS_MAX_NUM];

static inline unsigned long getIOPendingCount(int i) {
    unsigned long count = 0;
    atomicGetWithSync(io_threads_pending[i].value, count);
    return count;
}

static inline void setIOPendingCount(int i, unsigned long count) {
    atomicSetWithSync(io_threads_pending[i].value, count);
}

void *IOThreadMain(void *myid) {
    /* The ID is the thread number (from 0 to server.io_threads_num-1), and is
     * used by the thread to just manipulate a single sub-array of clients. */
    long id = (unsigned long)myid;
    char thdname[16];

    snprintf(thdname, sizeof(thdname), "io_thd_%ld", id);
    redis_set_thread_title(thdname);
    redisSetCpuAffinity(server.server_cpulist);
    makeThreadKillable();

    while(1) {
        /* Wait for start */
        for (int j = 0; j < 1000000; j++) {
            if (getIOPendingCount(id) != 0) break;
        }

        /* Give the main thread a chance to stop this thread. */
        if (getIOPendingCount(id) == 0) {
            pthread_mutex_lock(&io_threads_mutex[id]);
            pthread_mutex_unlock(&io_threads_mutex[id]);
            continue;
        }

        serverAssert(getIOPendingCount(id) != 0);

        /* Process: note that the main thread will never touch our list
         * before we drop the pending count to 0. */
        listIter li;
        listNode *ln;
        listRewind(io_threads_list[id],&li);
        while((ln = listNext(&li))) {
            client *c = listNodeValue(ln);
            if (io_threads_op == IO_THREADS_OP_WRITE) {
                writeToClient(c,0);
            } else if (io_threads_op == IO_THREADS_OP_READ) {
                readQueryFromClient(c->conn);
            } else {
                serverPanic("io_threads_op value is unknown");
            }
        }
        listEmpty(io_threads_list[id]);
        setIOPendingCount(id, 0);
    }
}

/* Initialize the data structures needed for threaded I/O. */
void initThreadedIO(void) {
    server.io_threads_active = 0; /* We start with threads not active. */

    /* Indicate that io-threads are currently idle */
    io_threads_op = IO_THREADS_OP_IDLE;

    /* Don't spawn any thread if the user selected a single thread:
     * we'll handle I/O directly from the main thread. */
    if (server.io_threads_num == 1) return;

    if (server.io_threads_num > IO_THREADS_MAX_NUM) {
        serverLog(LL_WARNING,"Fatal: too many I/O threads configured. "
                             "The maximum number is %d.", IO_THREADS_MAX_NUM);
        exit(1);
    }

    /* Spawn and initialize the I/O threads. */
    for (int i = 0; i < server.io_threads_num; i++) {
        /* Things we do for all the threads including the main thread. */
        io_threads_list[i] = listCreate();
        if (i == 0) continue; /* Thread 0 is the main thread. */

        /* Things we do only for the additional threads. */
        pthread_t tid;
        pthread_mutex_init(&io_threads_mutex[i],NULL);
        setIOPendingCount(i, 0);
        pthread_mutex_lock(&io_threads_mutex[i]); /* Thread will be stopped. */
        if (pthread_create(&tid,NULL,IOThreadMain,(void*)(long)i) != 0) {
            serverLog(LL_WARNING,"Fatal: Can't initialize IO thread.");
            exit(1);
        }
        io_threads[i] = tid;
    }
}

void killIOThreads(void) {
    int err, j;
    for (j = 0; j < server.io_threads_num; j++) {
        if (io_threads[j] == pthread_self()) continue;
        if (io_threads[j] && pthread_cancel(io_threads[j]) == 0) {
            if ((err = pthread_join(io_threads[j],NULL)) != 0) {
                serverLog(LL_WARNING,
                    "IO thread(tid:%lu) can not be joined: %s",
                        (unsigned long)io_threads[j], strerror(err));
            } else {
                serverLog(LL_WARNING,
                    "IO thread(tid:%lu) terminated",(unsigned long)io_threads[j]);
            }
        }
    }
}

void startThreadedIO(void) {
    serverAssert(server.io_threads_active == 0);
    for (int j = 1; j < server.io_threads_num; j++)
        pthread_mutex_unlock(&io_threads_mutex[j]);
    server.io_threads_active = 1;
}

void stopThreadedIO(void) {
    /* We may have still clients with pending reads when this function
     * is called: handle them before stopping the threads. */
    handleClientsWithPendingReadsUsingThreads();
    serverAssert(server.io_threads_active == 1);
    for (int j = 1; j < server.io_threads_num; j++)
        pthread_mutex_lock(&io_threads_mutex[j]);
    server.io_threads_active = 0;
}

/* This function checks if there are not enough pending clients to justify
 * taking the I/O threads active: in that case I/O threads are stopped if
 * currently active. We track the pending writes as a measure of clients
 * we need to handle in parallel, however the I/O threading is disabled
 * globally for reads as well if we have too little pending clients.
 *
 * The function returns 0 if the I/O threading should be used because there
 * are enough active threads, otherwise 1 is returned and the I/O threads
 * could be possibly stopped (if already active) as a side effect. */
int stopThreadedIOIfNeeded(void) {
    int pending = listLength(server.clients_pending_write);

    /* Return ASAP if IO threads are disabled (single threaded mode). */
    if (server.io_threads_num == 1) return 1;

    if (pending < (server.io_threads_num*2)) {
        if (server.io_threads_active) stopThreadedIO();
        return 1;
    } else {
        return 0;
    }
}

/* This function achieves thread safety using a fan-out -> fan-in paradigm:
 * Fan out: The main thread fans out work to the io-threads which block until
 * setIOPendingCount() is called with a value larger than 0 by the main thread.
 * Fan in: The main thread waits until getIOPendingCount() returns 0. Then
 * it can safely perform post-processing and return to normal synchronous
 * work. */
int handleClientsWithPendingWritesUsingThreads(void) {
    int processed = listLength(server.clients_pending_write);
    if (processed == 0) return 0; /* Return ASAP if there are no clients. */

    /* If I/O threads are disabled or we have few clients to serve, don't
     * use I/O threads, but the boring synchronous code. */
    if (server.io_threads_num == 1 || stopThreadedIOIfNeeded()) {
        return handleClientsWithPendingWrites();
    }

    /* Start threads if needed. */
    if (!server.io_threads_active) startThreadedIO();

    /* Distribute the clients across N different lists. */
    listIter li;
    listNode *ln;
    listRewind(server.clients_pending_write,&li);
    int item_id = 0;
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        c->flags &= ~CLIENT_PENDING_WRITE;

        /* Remove clients from the list of pending writes since
         * they are going to be closed ASAP. */
        if (c->flags & CLIENT_CLOSE_ASAP) {
            listUnlinkNode(server.clients_pending_write, ln);
            continue;
        }

        /* Since all replicas and replication backlog use global replication
         * buffer, to guarantee data accessing thread safe, we must put all
         * replicas client into io_threads_list[0] i.e. main thread handles
         * sending the output buffer of all replicas. */
        if (getClientType(c) == CLIENT_TYPE_SLAVE) {
            listAddNodeTail(io_threads_list[0],c);
            continue;
        }

        int target_id = item_id % server.io_threads_num;
        listAddNodeTail(io_threads_list[target_id],c);
        item_id++;
    }

    /* Give the start condition to the waiting threads, by setting the
     * start condition atomic var. */
    io_threads_op = IO_THREADS_OP_WRITE;
    for (int j = 1; j < server.io_threads_num; j++) {
        int count = listLength(io_threads_list[j]);
        setIOPendingCount(j, count);
    }

    /* Also use the main thread to process a slice of clients. */
    listRewind(io_threads_list[0],&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        writeToClient(c,0);
    }
    listEmpty(io_threads_list[0]);

    /* Wait for all the other threads to end their work. */
    while(1) {
        unsigned long pending = 0;
        for (int j = 1; j < server.io_threads_num; j++)
            pending += getIOPendingCount(j);
        if (pending == 0) break;
    }

    io_threads_op = IO_THREADS_OP_IDLE;

    /* Run the list of clients again to install the write handler where
     * needed. */
    listRewind(server.clients_pending_write,&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);

        /* Update the client in the mem usage after we're done processing it in the io-threads */
        updateClientMemUsageAndBucket(c);

        /* Install the write handler if there are pending writes in some
         * of the clients. */
        if (clientHasPendingReplies(c)) {
            installClientWriteHandler(c);
        }
    }
    while(listLength(server.clients_pending_write) > 0) {
        listUnlinkNode(server.clients_pending_write, server.clients_pending_write->head);
    }

    /* Update processed count on server */
    server.stat_io_writes_processed += processed;

    return processed;
}

/* Return 1 if we want to handle the client read later using threaded I/O.
 * This is called by the readable handler of the event loop.
 * As a side effect of calling this function the client is put in the
 * pending read clients and flagged as such. */
int postponeClientRead(client *c) {
    if (server.io_threads_active &&
        server.io_threads_do_reads &&
        !ProcessingEventsWhileBlocked &&
        !(c->flags & (CLIENT_MASTER|CLIENT_SLAVE|CLIENT_BLOCKED)) &&
        io_threads_op == IO_THREADS_OP_IDLE)
    {
        listAddNodeHead(server.clients_pending_read,c);
        c->pending_read_list_node = listFirst(server.clients_pending_read);
        return 1;
    } else {
        return 0;
    }
}

/* When threaded I/O is also enabled for the reading + parsing side, the
 * readable handler will just put normal clients into a queue of clients to
 * process (instead of serving them synchronously). This function runs
 * the queue using the I/O threads, and process them in order to accumulate
 * the reads in the buffers, and also parse the first command available
 * rendering it in the client structures.
 * This function achieves thread safety using a fan-out -> fan-in paradigm:
 * Fan out: The main thread fans out work to the io-threads which block until
 * setIOPendingCount() is called with a value larger than 0 by the main thread.
 * Fan in: The main thread waits until getIOPendingCount() returns 0. Then
 * it can safely perform post-processing and return to normal synchronous
 * work. */
int handleClientsWithPendingReadsUsingThreads(void) {
    if (!server.io_threads_active || !server.io_threads_do_reads) return 0;
    int processed = listLength(server.clients_pending_read);
    if (processed == 0) return 0;

    /* Distribute the clients across N different lists. */
    listIter li;
    listNode *ln;
    listRewind(server.clients_pending_read,&li);
    int item_id = 0;
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        int target_id = item_id % server.io_threads_num;
        listAddNodeTail(io_threads_list[target_id],c);
        item_id++;
    }

    /* Give the start condition to the waiting threads, by setting the
     * start condition atomic var. */
    io_threads_op = IO_THREADS_OP_READ;
    for (int j = 1; j < server.io_threads_num; j++) {
        int count = listLength(io_threads_list[j]);
        setIOPendingCount(j, count);
    }

    /* Also use the main thread to process a slice of clients. */
    listRewind(io_threads_list[0],&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        readQueryFromClient(c->conn);
    }
    listEmpty(io_threads_list[0]);

    /* Wait for all the other threads to end their work. */
    while(1) {
        unsigned long pending = 0;
        for (int j = 1; j < server.io_threads_num; j++)
            pending += getIOPendingCount(j);
        if (pending == 0) break;
    }

    io_threads_op = IO_THREADS_OP_IDLE;

    /* Run the list of clients again to process the new buffers. */
    while(listLength(server.clients_pending_read)) {
        ln = listFirst(server.clients_pending_read);
        client *c = listNodeValue(ln);
        listDelNode(server.clients_pending_read,ln);
        c->pending_read_list_node = NULL;

        serverAssert(!(c->flags & CLIENT_BLOCKED));

        if (beforeNextClient(c) == C_ERR) {
            /* If the client is no longer valid, we avoid
             * processing the client later. So we just go
             * to the next. */
            continue;
        }

        /* Once io-threads are idle we can update the client in the mem usage */
        updateClientMemUsageAndBucket(c);

        if (processPendingCommandAndInputBuffer(c) == C_ERR) {
            /* If the client is no longer valid, we avoid
             * processing the client later. So we just go
             * to the next. */
            continue;
        }

        /* We may have pending replies if a thread readQueryFromClient() produced
         * replies and did not put the client in pending write queue (it can't).
         */
        if (!(c->flags & CLIENT_PENDING_WRITE) && clientHasPendingReplies(c))
            putClientInPendingWriteQueue(c);
    }

    /* Update processed count on server */
    server.stat_io_reads_processed += processed;

    return processed;
}

/* Returns the actual client eviction limit based on current configuration or
 * 0 if no limit. */
size_t getClientEvictionLimit(void) {
    size_t maxmemory_clients_actual = SIZE_MAX;

    /* Handle percentage of maxmemory*/
    if (server.maxmemory_clients < 0 && server.maxmemory > 0) {
        unsigned long long maxmemory_clients_bytes = (unsigned long long)((double)server.maxmemory * -(double) server.maxmemory_clients / 100);
        if (maxmemory_clients_bytes <= SIZE_MAX)
            maxmemory_clients_actual = maxmemory_clients_bytes;
    }
    else if (server.maxmemory_clients > 0)
        maxmemory_clients_actual = server.maxmemory_clients;
    else
        return 0;

    /* Don't allow a too small maxmemory-clients to avoid cases where we can't communicate
     * at all with the server because of bad configuration */
    if (maxmemory_clients_actual < 1024*128)
        maxmemory_clients_actual = 1024*128;

    return maxmemory_clients_actual;
}

void evictClients(void) {
    if (!server.client_mem_usage_buckets)
        return;
    /* Start eviction from topmost bucket (largest clients) */
    int curr_bucket = CLIENT_MEM_USAGE_BUCKETS-1;
    listIter bucket_iter;
    listRewind(server.client_mem_usage_buckets[curr_bucket].clients, &bucket_iter);
    size_t client_eviction_limit = getClientEvictionLimit();
    if (client_eviction_limit == 0)
        return;
    while (server.stat_clients_type_memory[CLIENT_TYPE_NORMAL] +
           server.stat_clients_type_memory[CLIENT_TYPE_PUBSUB] >= client_eviction_limit) {
        listNode *ln = listNext(&bucket_iter);
        if (ln) {
            client *c = ln->value;
            sds ci = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE, "Evicting client: %s", ci);
            freeClient(c);
            sdsfree(ci);
            server.stat_evictedclients++;
        } else {
            curr_bucket--;
            if (curr_bucket < 0) {
                serverLog(LL_WARNING, "Over client maxmemory after evicting all evictable clients");
                break;
            }
            listRewind(server.client_mem_usage_buckets[curr_bucket].clients, &bucket_iter);
        }
    }
}
