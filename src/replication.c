/* 异步复制（主从复制）实现。
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */


#include "server.h"
#include "cluster.h"
#include "bio.h"
#include "functions.h"
#include "connection.h"

#include <memory.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>

/* 复制模块的前置声明（在本文件其它位置实现） */
void replicationDiscardCachedMaster(void);
void replicationResurrectCachedMaster(connection *conn);
void replicationSendAck(void);
int replicaPutOnline(client *slave);
void replicaStartCommandStream(client *slave);
int cancelReplicationHandshake(int reconnect);

/* 用于记录本实例是否因为复制目的而生成过 RDB 文件。
 * 当实例被配置为无持久化时，可以据此删除该 RDB 文件。 */
int RDBGeneratedByReplication = 0;

/* --------------------------- Utility functions ---------------------------- */
/* 根据是否启用了 TLS 复制，返回对应的连接类型。
 * 启用了 tls_replication 则使用 TLS，否则使用 TCP。 */
static ConnectionType *connTypeOfReplication(void) {
    if (server.tls_replication) {
        return connectionTypeTls();
    }

    return connectionTypeTcp();
}

/* 返回一个字符串指针，表示从节点的 IP:监听端口对。
 * 主要用于日志记录，我们希望通过 IP 地址和监听端口来记录一个从节点，
 * 这样对用户更清晰，例如："Closing connection with replica 10.1.2.3:6380"。 */
char *replicationGetSlaveName(client *c) {
    static char buf[NET_HOST_PORT_STR_LEN];
    char ip[NET_IP_STR_LEN];

    ip[0] = '\0';
    buf[0] = '\0';
    if (c->slave_addr ||
        connAddrPeerName(c->conn,ip,sizeof(ip),NULL) != -1)
    {
        char *addr = c->slave_addr ? c->slave_addr : ip;
        if (c->slave_listening_port)
            formatAddr(buf,sizeof(buf),addr,c->slave_listening_port);
        else
            snprintf(buf,sizeof(buf),"%s:<unknown-replica-port>",addr);
    } else {
        snprintf(buf,sizeof(buf),"client id #%llu",
            (unsigned long long) c->id);
    }
    return buf;
}

/* 普通的 unlink() 可能会阻塞较长时间，因为它需要实际地将文件从文件系统删除。
 * 此函数将文件删除操作放到后台线程中执行。
 * 我们在线程中实际只执行 close()，利用以下事实：
 * 如果同一文件还有另一个打开实例，前台的 unlink() 只会移除文件名，
 * 文件存储空间的释放只有在最后一个引用被释放时才会发生。 */
int bg_unlink(const char *filename) {
    int fd = open(filename,O_RDONLY|O_NONBLOCK);
    if (fd == -1) {
        /* 无法打开文件？回退到在主线程中执行 unlink。 */
        return unlink(filename);
    } else {
        /* 下面的 unlink() 移除了文件名，但由于仍有进程打开它，
         * 并不会释放文件内容。 */
        int retval = unlink(filename);
        if (retval == -1) {
            /* 如果 unlink 出错，我们只返回错误码，并关闭对文件的新引用。 */
            int old_errno = errno;
            close(fd);  /* 这会覆盖 errno，所以需要先保存。 */
            errno = old_errno;
            return -1;
        }
        bioCreateCloseJob(fd, 0, 0);
        return 0; /* 成功。 */
    }
}

/* ---------------------------------- MASTER -------------------------------- */

/* 创建复制积压缓冲区（replication backlog）。
 * 分配 replBacklog 结构并初始化相关字段。 */
void createReplicationBacklog(void) {
    serverAssert(server.repl_backlog == NULL);
    server.repl_backlog = zmalloc(sizeof(replBacklog));
    server.repl_backlog->ref_repl_buf_node = NULL;
    server.repl_backlog->unindexed_count = 0;
    server.repl_backlog->blocks_index = raxNew();
    server.repl_backlog->histlen = 0;
    /* 缓冲区中没有任何数据，但实际上我们拥有的第一个字节
     * 是复制流下一个将被生成的字节。 */
    server.repl_backlog->offset = server.master_repl_offset+1;
}

/* 当用户在运行时修改复制积压缓冲区大小时调用本函数。
 * 该函数负责重新调整缓冲区大小并进行相应的设置，
 * 使其包含与原来相同的数据（数据可能变少，但保留最近的数据；
 * 如果缓冲区被扩大，则保持相同数据并增加更多空闲空间）。 */
void resizeReplicationBacklog(void) {
    if (server.repl_backlog_size < CONFIG_REPL_BACKLOG_MIN_SIZE)
        server.repl_backlog_size = CONFIG_REPL_BACKLOG_MIN_SIZE;
    if (server.repl_backlog)
        incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
}

/* 释放复制积压缓冲区。调用前需要保证没有从节点连接。 */
void freeReplicationBacklog(void) {
    serverAssert(listLength(server.slaves) == 0);
    if (server.repl_backlog == NULL) return;

    /* 减少起始缓冲节点的引用计数。 */
    if (server.repl_backlog->ref_repl_buf_node) {
        replBufBlock *o = listNodeValue(
            server.repl_backlog->ref_repl_buf_node);
        serverAssert(o->refcount == 1); /* 最后一个引用。 */
        o->refcount--;
    }

    /* 释放积压缓冲区时，所有复制缓冲块会被完全释放，
     * 因为积压缓冲区仅在没有副本时才会被释放，
     * 而积压缓冲区保存着所有块的最后一个引用。 */
    freeReplicationBacklogRefMemAsync(server.repl_buffer_blocks,
                            server.repl_backlog->blocks_index);
    resetReplicationBuffer();
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

/* 为了在副本请求部分重同步时能够快速从复制缓冲区块中查找偏移量，
 * 我们每 REPL_BACKLOG_INDEX_PER_BLOCKS 个块就创建一个索引块。 */
void createReplicationBacklogIndex(listNode *ln) {
    server.repl_backlog->unindexed_count++;
    if (server.repl_backlog->unindexed_count >= REPL_BACKLOG_INDEX_PER_BLOCKS) {
        replBufBlock *o = listNodeValue(ln);
        uint64_t encoded_offset = htonu64(o->repl_offset);
        raxInsert(server.repl_backlog->blocks_index,
                  (unsigned char*)&encoded_offset, sizeof(uint64_t),
                  ln, NULL);
        server.repl_backlog->unindexed_count = 0;
    }
}

/* 重设复制缓冲块的偏移基准，因为主节点重启时初始偏移量从 0 开始。 */
void rebaseReplicationBuffer(long long base_repl_offset) {
    raxFree(server.repl_backlog->blocks_index);
    server.repl_backlog->blocks_index = raxNew();
    server.repl_backlog->unindexed_count = 0;

    listIter li;
    listNode *ln;
    listRewind(server.repl_buffer_blocks, &li);
    while ((ln = listNext(&li))) {
        replBufBlock *o = listNodeValue(ln);
        o->repl_offset += base_repl_offset;
        createReplicationBacklogIndex(ln);
    }
}

/* 重置复制缓冲区，重新创建缓冲块列表。 */
void resetReplicationBuffer(void) {
    server.repl_buffer_mem = 0;
    server.repl_buffer_blocks = listCreate();
    listSetFreeMethod(server.repl_buffer_blocks, (void (*)(void*))zfree);
}

/* 判断是否可以向指定的副本复制缓冲。
 * 返回 1 表示可以，0 表示不可以。 */
int canFeedReplicaReplBuffer(client *replica) {
    /* 不要向仅需要 RDB 的副本复制。 */
    if (replica->flags & CLIENT_REPL_RDBONLY) return 0;

    /* 不要向仍在等待 BGSAVE 启动的副本复制。 */
    if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_START) return 0;

    /* 不要向即将被关闭的副本复制。 */
    if (replica->flags & CLIENT_CLOSE_ASAP) return 0;

    return 1;
}

/* 与 prepareClientToWrite 类似，需要注意：必须在将复制流送入全局复制缓冲区之前
 * 调用本函数，因为 prepareClientToWrite 中的 clientHasPendingReplies 会
 * 访问全局复制缓冲区进行判断。 */
int prepareReplicasToWrite(void) {
    listIter li;
    listNode *ln;
    int prepared = 0;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        if (!canFeedReplicaReplBuffer(slave)) continue;
        if (prepareClientToWrite(slave) == C_ERR) continue;
        prepared++;
    }

    return prepared;
}

/* feedReplicationBuffer() 的包装函数，输入为 Redis 字符串对象。 */
void feedReplicationBufferWithObject(robj *o) {
    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;

    if (o->encoding == OBJ_ENCODING_INT) {
        len = ll2string(llstr,sizeof(llstr),(long)o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    feedReplicationBuffer(p,len);
}

/* 通常情况下，当复制积压缓冲区大小超过设置且没有副本引用时，
 * 我们只需要裁剪一个复制缓冲块。但如果副本客户端断开连接，
 * 我们需要释放许多被引用的复制缓冲块。如果有大量块需要释放，
 * 会耗费大量时间并冻结服务器，因此我们对积压缓冲区进行增量裁剪。 */
void incrementalTrimReplicationBacklog(size_t max_blocks) {
    serverAssert(server.repl_backlog != NULL);

    size_t trimmed_blocks = 0;
    while (server.repl_backlog->histlen > server.repl_backlog_size &&
           trimmed_blocks < max_blocks)
    {
        /* 我们永远不会把积压缓冲区裁剪到少于一个块。 */
        if (listLength(server.repl_buffer_blocks) <= 1) break;

        /* 副本会增加其引用的第一个复制缓冲块的引用计数，
         * 在这种情况下，即使 backlog_histlen 超过了 backlog_size，
         * 我们也不会裁剪积压缓冲区。这隐式地使积压缓冲区大于我们的设置，
         * 但可以让主节点尽可能多地接受部分重同步。
         * 因此积压缓冲区必须是复制缓冲块的最后一个引用。 */
        listNode *first = listFirst(server.repl_buffer_blocks);
        serverAssert(first == server.repl_backlog->ref_repl_buf_node);
        replBufBlock *fo = listNodeValue(first);
        if (fo->refcount != 1) break;

        /* 如果释放第一个复制缓冲块后，积压缓冲区的有效大小会小于设置的
         * 积压缓冲区大小，我们也不会尝试裁剪积压缓冲区。 */
        if (server.repl_backlog->histlen - (long long)fo->size <=
            server.repl_backlog_size) break;

        /* 减少引用计数，稍后再释放第一个块。 */
        fo->refcount--;
        trimmed_blocks++;
        server.repl_backlog->histlen -= fo->size;

        /* 转去使用下一个复制缓冲块节点。 */
        listNode *next = listNextNode(first);
        server.repl_backlog->ref_repl_buf_node = next;
        serverAssert(server.repl_backlog->ref_repl_buf_node != NULL);
        /* 增加引用计数以保持新头部节点。 */
        ((replBufBlock *)listNodeValue(next))->refcount++;

        /* 从记录的块中移除该节点。 */
        uint64_t encoded_offset = htonu64(fo->repl_offset);
        raxRemove(server.repl_backlog->blocks_index,
            (unsigned char*)&encoded_offset, sizeof(uint64_t), NULL);

        /* 从全局复制缓冲区中删除第一个节点。 */
        serverAssert(fo->refcount == 0 && fo->used == fo->size);
        server.repl_buffer_mem -= (fo->size +
            sizeof(listNode) + sizeof(replBufBlock));
        listDelNode(server.repl_buffer_blocks, first);
    }

    /* 设置积压缓冲区中第一个字节的偏移量。 */
    server.repl_backlog->offset = server.master_repl_offset -
                              server.repl_backlog->histlen + 1;
}

/* 释放该副本引用的复制缓冲块。 */
void freeReplicaReferencedReplBuffer(client *replica) {
    if (replica->ref_repl_buf_node != NULL) {
        /* 减少起始缓冲节点的引用计数。 */
        replBufBlock *o = listNodeValue(replica->ref_repl_buf_node);
        serverAssert(o->refcount > 0);
        o->refcount--;
        incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
    }
    replica->ref_repl_buf_node = NULL;
    replica->ref_block_pos = 0;
}

/* 将字节追加到全局复制缓冲区列表中。复制积压缓冲区和所有副本客户端
 * 共同使用复制缓冲区，本函数替代了用于副本和复制积压缓冲区的
 * 'addReply*'、'feedReplicationBacklog'。
 * 首先将缓冲区添加到全局复制缓冲块列表中，然后更新副本/复制积压缓冲区
 * 引用的节点和块位置。 */
void feedReplicationBuffer(char *s, size_t len) {
    static long long repl_block_id = 0;

    if (server.repl_backlog == NULL) return;

    while(len > 0) {
        size_t start_pos = 0; /* The position of referenced block to start sending. */
        listNode *start_node = NULL; /* Replica/backlog starts referenced node. */
        int add_new_block = 0; /* Create new block if current block is total used. */
        listNode *ln = listLast(server.repl_buffer_blocks);
        replBufBlock *tail = ln ? listNodeValue(ln) : NULL;

        /* Append to tail string when possible. */
        if (tail && tail->size > tail->used) {
            start_node = listLast(server.repl_buffer_blocks);
            start_pos = tail->used;
            /* Copy the part we can fit into the tail, and leave the rest for a
             * new node */
            size_t avail = tail->size - tail->used;
            size_t copy = (avail >= len) ? len : avail;
            memcpy(tail->buf + tail->used, s, copy);
            tail->used += copy;
            s += copy;
            len -= copy;
            server.master_repl_offset += copy;
            server.repl_backlog->histlen += copy;
        }
        if (len) {
            /* Create a new node, make sure it is allocated to at
             * least PROTO_REPLY_CHUNK_BYTES */
            size_t usable_size;
            /* Avoid creating nodes smaller than PROTO_REPLY_CHUNK_BYTES, so that we can append more data into them,
             * and also avoid creating nodes bigger than repl_backlog_size / 16, so that we won't have huge nodes that can't
             * trim when we only still need to hold a small portion from them. */
            size_t limit = max((size_t)server.repl_backlog_size / 16, (size_t)PROTO_REPLY_CHUNK_BYTES);
            size_t size = min(max(len, (size_t)PROTO_REPLY_CHUNK_BYTES), limit);
            tail = zmalloc_usable(size + sizeof(replBufBlock), &usable_size);
            /* Take over the allocation's internal fragmentation */
            tail->size = usable_size - sizeof(replBufBlock);
            size_t copy = (tail->size >= len) ? len : tail->size;
            tail->used = copy;
            tail->refcount = 0;
            tail->repl_offset = server.master_repl_offset + 1;
            tail->id = repl_block_id++;
            memcpy(tail->buf, s, copy);
            listAddNodeTail(server.repl_buffer_blocks, tail);
            /* We also count the list node memory into replication buffer memory. */
            server.repl_buffer_mem += (usable_size + sizeof(listNode));
            add_new_block = 1;
            if (start_node == NULL) {
                start_node = listLast(server.repl_buffer_blocks);
                start_pos = 0;
            }
            s += copy;
            len -= copy;
            server.master_repl_offset += copy;
            server.repl_backlog->histlen += copy;
        }

        /* 副本的输出缓冲区处理。 */
        listIter li;
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;
            if (!canFeedReplicaReplBuffer(slave)) continue;

            /* 更新共享复制缓冲区的起始位置。 */
            if (slave->ref_repl_buf_node == NULL) {
                slave->ref_repl_buf_node = start_node;
                slave->ref_block_pos = start_pos;
                /* 仅增加起始块的引用计数。 */
                ((replBufBlock *)listNodeValue(start_node))->refcount++;
            }

            /* 仅在添加新块时检查输出缓冲区限制。 */
            if (add_new_block) closeClientOnOutputBufferLimitReached(slave, 1);
        }

        /* 复制积压缓冲区处理 */
        if (server.repl_backlog->ref_repl_buf_node == NULL) {
            server.repl_backlog->ref_repl_buf_node = start_node;
            /* 仅增加起始块的引用计数。 */
            ((replBufBlock *)listNodeValue(start_node))->refcount++;

            /* 在将复制流添加到复制积压缓冲区之前，复制缓冲区必须为空。 */
            serverAssert(add_new_block == 1 && start_pos == 0);
        }
        if (add_new_block) {
            createReplicationBacklogIndex(listLast(server.repl_buffer_blocks));

            /* 在添加复制数据之后进行裁剪，以在常见情况下保持积压缓冲区大小
             * 接近 repl_backlog_size 是很重要的。我们等到添加新块时才进行裁剪，
             * 以避免在添加少量数据时重复进行不必要的裁剪尝试。
             * 关于复制积压缓冲区内存跟踪的详细信息，请参见
             * freeMemoryGetNotCountedMemory() 中的注释。 */
            incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
        }
    }
}

/* 将写命令传播到复制流。
 *
 * 如果本实例是主节点，使用从客户端接收到的命令来生成复制流。
 * 如果本实例是从节点且挂载了子副本，则使用
 * replicationFeedStreamFromMasterStream()。 */
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc) {
    int j, len;
    char llstr[LONG_STR_SIZE];

    /* 如果传播的是不涉及键的命令（如 PING、REPLCONF），
     * 我们传入 dbid=-1，表示无需复制 select 命令。 */
    serverAssert(dictid == -1 || (dictid >= 0 && dictid < server.dbnum));

    /* 如果本实例不是顶层主节点，立即返回：我们将代理从主节点接收到的数据流，
     * 以便传播*完全相同*的复制流。这样此从节点可以通告与主节点相同的复制 ID
     * （因为它共享主节点的复制历史并具有相同的积压缓冲区和偏移量）。 */
    if (server.masterhost != NULL) return;

    /* 如果没有从节点，并且也没有要填充的积压缓冲区，我们可以立即返回。 */
    if (server.repl_backlog == NULL && listLength(slaves) == 0) {
        /* 我们仍然增加 repl_offset，因为我们用它来跟踪 AOF fsync，
         * 即使没有复制激活也是如此。如果 AOF 也被禁用，则不会执行此代码。 */
        server.master_repl_offset += 1;
        return;
    }

    /* 我们不能同时挂载了从节点但又没有积压缓冲区。 */
    serverAssert(!(listLength(slaves) != 0 && server.repl_backlog == NULL));

    /* 在送入复制流之前，必须先为所有副本安装写处理器。 */
    prepareReplicasToWrite();

    /* 如果需要，向每个从节点发送 SELECT 命令。 */
    if (dictid != -1 && server.slaveseldb != dictid) {
        robj *selectcmd;

        /* 对于少数数据库，我们有预计算的 SELECT 命令。 */
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            dictid_len = ll2string(llstr,sizeof(llstr),dictid);
            selectcmd = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, llstr));
        }

        feedReplicationBufferWithObject(selectcmd);

        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);

        server.slaveseldb = dictid;
    }

    /* 如果有复制缓冲区，将命令写入复制缓冲区。 */
    char aux[LONG_STR_SIZE+3];

    /* 添加多批量回复长度。 */
    aux[0] = '*';
    len = ll2string(aux+1,sizeof(aux)-1,argc);
    aux[len+1] = '\r';
    aux[len+2] = '\n';
    feedReplicationBuffer(aux,len+3);

    for (j = 0; j < argc; j++) {
        long objlen = stringObjectLen(argv[j]);

        /* 我们需要以批量回复的形式将对象送入缓冲区，
         * 而不是作为普通字符串，所以创建 $..CRLF 载荷长度
         * 并添加最终的 CRLF。 */
        aux[0] = '$';
        len = ll2string(aux+1,sizeof(aux)-1,objlen);
        aux[len+1] = '\r';
        aux[len+2] = '\n';
        feedReplicationBuffer(aux,len+3);
        feedReplicationBufferWithObject(argv[j]);
        feedReplicationBuffer(aux+len+1,2);
    }
}

/* 这是一个调试函数，在检测到复制协议有问题时被调用：
 * 目的是窥视复制积压缓冲区并显示最后几个字节，以便更容易猜测可能是哪种 bug。 */
void showLatestBacklog(void) {
    if (server.repl_backlog == NULL) return;
    if (listLength(server.repl_buffer_blocks) == 0) return;
    if (server.hide_user_data_from_log) {
        serverLog(LL_NOTICE,"hide-user-data-from-log is on, skip logging backlog content to avoid spilling PII.");
        return;
    }

    size_t dumplen = 256;
    if (server.repl_backlog->histlen < (long long)dumplen)
        dumplen = server.repl_backlog->histlen;

    sds dump = sdsempty();
    listNode *node = listLast(server.repl_buffer_blocks);
    while(dumplen) {
        if (node == NULL) break;
        replBufBlock *o = listNodeValue(node);
        size_t thislen = o->used >= dumplen ? dumplen : o->used;
        sds head = sdscatrepr(sdsempty(), o->buf+o->used-thislen, thislen);
        sds tmp = sdscatsds(head, dump);
        sdsfree(dump);
        dump = tmp;
        dumplen -= thislen;
        node = listPrevNode(node);
    }

    /* 最后记录这些字节：这是了解发生了什么的重要调试信息。 */
    serverLog(LL_NOTICE,"Latest backlog is: '%s'", dump);
    sdsfree(dump);
}

/* 此函数用于将从主节点接收到的数据代理给子副本。 */
#include <ctype.h>
void replicationFeedStreamFromMasterStream(char *buf, size_t buflen) {
    /* 如果挂载了从节点，则必须存在复制积压缓冲区。 */
    if (listLength(server.slaves)) serverAssert(server.repl_backlog != NULL);
    if (server.repl_backlog) {
        /* 在送入复制流之前，必须先为所有副本安装写处理器。 */
        prepareReplicasToWrite();
        feedReplicationBuffer(buf,buflen);
    }
}

/* 将命令发送给所有 MONITOR 客户端。
 * c 为发起命令的客户端；monitors 为监控客户端列表；dictid 为当前数据库 id；
 * argv/argc 为命令参数。 */
void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc) {
    /* 快速路径：如果监控列表为空或服务器处于 loading 状态，立即返回。 */
    if (monitors == NULL || listLength(monitors) == 0 || server.loading) return;
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & CLIENT_SCRIPT) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d lua] ",dictid);
    } else if (c->flags & CLIENT_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d unix:%s] ",dictid,server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr,"[%d %s] ",dictid,getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(OBJ_STRING,cmdrepr);

    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor,cmdobj);
        updateClientMemUsageAndBucket(monitor);
    }
    decrRefCount(cmdobj);
}

/* 将复制积压缓冲区中的数据（从指定的 offset 开始到积压缓冲区末尾）
 * 发送给从节点 c。返回发送的字节数。 */
long long addReplyReplicationBacklog(client *c, long long offset) {
    long long skip;

    serverLog(LL_DEBUG, "[PSYNC] Replica request offset: %lld", offset);

    if (server.repl_backlog->histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }

    serverLog(LL_DEBUG, "[PSYNC] Backlog size: %lld",
             server.repl_backlog_size);
    serverLog(LL_DEBUG, "[PSYNC] First byte: %lld",
             server.repl_backlog->offset);
    serverLog(LL_DEBUG, "[PSYNC] History len: %lld",
             server.repl_backlog->histlen);

    /* 计算我们需要丢弃的字节数。 */
    skip = offset - server.repl_backlog->offset;
    serverLog(LL_DEBUG, "[PSYNC] Skipping: %lld", skip);

    /* 遍历记录的块，快速搜索近似节点。 */
    listNode *node = NULL;
    if (raxSize(server.repl_backlog->blocks_index) > 0) {
        uint64_t encoded_offset = htonu64(offset);
        raxIterator ri;
        raxStart(&ri, server.repl_backlog->blocks_index);
        raxSeek(&ri, ">", (unsigned char*)&encoded_offset, sizeof(uint64_t));
        if (raxEOF(&ri)) {
            /* 没找到，所以从最后一个记录的节点开始搜索。 */
            raxSeek(&ri, "$", NULL, 0);
            raxPrev(&ri);
            node = (listNode *)ri.data;
        } else {
            raxPrev(&ri); /* 跳过找到的节点。 */
            /* 我们应该从上一个节点开始搜索，因为当前找到节点的偏移量超过了搜索偏移量。 */
            if (raxPrev(&ri))
                node = (listNode *)ri.data;
            else
                node = server.repl_backlog->ref_repl_buf_node;
        }
        raxStop(&ri);
    } else {
        /* 没有记录的块，直接从起始节点开始搜索。 */
        node = server.repl_backlog->ref_repl_buf_node;
    }

    /* 搜索精确的节点。 */
    while (node != NULL) {
        replBufBlock *o = listNodeValue(node);
        if (o->repl_offset + (long long)o->used >= offset) break;
        node = listNextNode(node);
    }
    serverAssert(node != NULL);

    /* 首先安装写处理器。 */
    prepareClientToWrite(c);
    /* 设置副本的输出缓冲区。 */
    replBufBlock *o = listNodeValue(node);
    o->refcount++;
    c->ref_repl_buf_node = node;
    c->ref_block_pos = offset - o->repl_offset;

    return server.repl_backlog->histlen - skip;
}

/* 返回作为对从节点 PSYNC 命令响应的偏移量。
 * 返回的值仅在 BGSAVE 进程启动之后、执行任何其他客户端命令之前有效。 */
long long getPsyncInitialOffset(void) {
    return server.master_repl_offset;
}

/* 在完全重同步的具体情况下发送 FULLRESYNC 响应，作为副作用以不同方式
 * 设置从节点为完全同步状态：
 *
 * 1) 在从节点客户端结构中记住此处发送的复制偏移量，以便如果新的从节点稍后
 *    通过复制此客户端输出缓冲区附加到同一个后台 RDB 保存进程，
 *    我们可以从该从节点获取正确的偏移量。
 * 2) 将从节点的复制状态设置为 WAIT_BGSAVE_END，以便我们从此时开始累积差异。
 * 3) 强制复制流重新发出 SELECT 语句，以便新从节点的增量差异将开始
 *    选择正确的数据库编号。
 *
 * 通常，此函数应在成功启动用于复制的 BGSAVE 之后立即调用，
 * 或者在已经有一个 BGSAVE 进行中并且我们已经将从此节点附加到它时调用。 */
int replicationSetupSlaveForFullResync(client *slave, long long offset) {
    char buf[128];
    int buflen;

    slave->psync_initial_offset = offset;
    slave->replstate = SLAVE_STATE_WAIT_BGSAVE_END;
    /* 我们也将为该从节点累积增量更改。将 slaveseldb 设置为 -1，
     * 以便强制在复制流中重新发出 SELECT 语句。 */
    server.slaveseldb = -1;

    /* 不要向使用旧版 SYNC 命令连接我们的从节点发送此响应。 */
    if (!(slave->flags & CLIENT_PRE_PSYNC)) {
        buflen = snprintf(buf,sizeof(buf),"+FULLRESYNC %s %lld\r\n",
                          server.replid,offset);
        if (connWrite(slave->conn,buf,buflen) != buflen) {
            freeClientAsync(slave);
            return C_ERR;
        }
    }
    return C_OK;
}

/* 此函数从主节点接收部分重同步请求的角度处理 PSYNC 命令。
 *
 * 成功时返回 C_OK，否则返回 C_ERR 并继续执行通常的完全重同步。 */
int masterTryPartialResynchronization(client *c, long long psync_offset) {
    long long psync_len;
    char *master_replid = c->argv[1]->ptr;
    char buf[128];
    int buflen;

    /* 此主节点的复制 ID 与想要成为从节点的客户端通过 PSYNC 通告的 ID 相同吗？
     * 如果复制 ID 已更改，则此主节点具有不同的复制历史，无法继续。
     *
     * 请注意，有两个可能有效的复制 ID：ID1 和 ID2。
     * 但 ID2 仅在特定的偏移量之前有效。 */
    if (strcasecmp(master_replid, server.replid) &&
        (strcasecmp(master_replid, server.replid2) ||
         psync_offset > server.second_replid_offset))
    {
        /* "?" 的 replid 由希望强制进行完全重同步的从节点使用。 */
        if (master_replid[0] != '?') {
            if (strcasecmp(master_replid, server.replid) &&
                strcasecmp(master_replid, server.replid2))
            {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Replication ID mismatch (Replica asked for '%s', my "
                    "replication IDs are '%s' and '%s')",
                    master_replid, server.replid, server.replid2);
            } else {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Requested offset for second ID was %lld, but I can reply "
                    "up to %lld", psync_offset, server.second_replid_offset);
            }
        } else {
            serverLog(LL_NOTICE,"Full resync requested by replica %s",
                replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    /* 我们是否仍然拥有从节点请求的数据？ */
    if (!server.repl_backlog ||
        psync_offset < server.repl_backlog->offset ||
        psync_offset > (server.repl_backlog->offset + server.repl_backlog->histlen))
    {
        serverLog(LL_NOTICE,
            "Unable to partial resync with replica %s for lack of backlog (Replica request was: %lld).", replicationGetSlaveName(c), psync_offset);
        if (psync_offset > server.master_repl_offset) {
            serverLog(LL_WARNING,
                "Warning: replica %s tried to PSYNC with an offset that is greater than the master replication offset.", replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    /* 如果到达了此点，我们能够执行部分重同步：
     * 1) 设置客户端状态以使其成为从节点。
     * 2) 通过 +CONTINUE 通知客户端我们可以继续。
     * 3) 将积压缓冲区的数据（从偏移量到末尾）发送给从节点。 */
    c->flags |= CLIENT_SLAVE;
    c->replstate = SLAVE_STATE_ONLINE;
    c->repl_ack_time = server.unixtime;
    c->repl_start_cmd_stream_on_ack = 0;
    listAddNodeTail(server.slaves,c);
    /* 我们不能使用连接缓冲区，因为它们在此阶段用于累积新命令。
     * 但我们确信套接字发送缓冲区为空，因此此写入实际上永远不会失败。 */
    if (c->slave_capa & SLAVE_CAPA_PSYNC2) {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s\r\n", server.replid);
    } else {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE\r\n");
    }
    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return C_OK;
    }
    psync_len = addReplyReplicationBacklog(c,psync_offset);
    serverLog(LL_NOTICE,
        "Partial resynchronization request from %s accepted. Sending %lld bytes of backlog starting from offset %lld.",
            replicationGetSlaveName(c),
            psync_len, psync_offset);
    /* 请注意，我们不需要在 server.slaveseldb 处将选定的 DB 设置为 -1
     * 以强制主节点发出 SELECT，因为从节点已经从前一次与主节点的连接中
     * 获得了此状态。 */

    refreshGoodSlavesCount();

    /* 触发副本变更的模块事件。 */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);

    return C_OK; /* 调用者可以返回，无需完全重同步。 */

need_full_resync:
    /* 由于某些原因我们需要完全重同步... 请注意，如果需要完全 SYNC，
     * 我们现在无法响应 PSYNC。响应必须包含我们传输 RDB 文件时的主节点
     * 偏移量，因此我们需要将响应延迟到那一刻。 */
    return C_ERR;
}

/* 为复制目的启动 BGSAVE，即根据配置选择磁盘或套接字目标，
 * 并确保在启动之前刷新脚本缓存。
 *
 * mincapa 参数是等待此 BGSAVE 的所有从节点能力的按位与，
 * 表示所有从节点都支持的能力。可以通过 SLAVE_CAPA_* 宏进行测试。
 *
 * 除了启动 BGSAVE 之外的副作用：
 *
 * 1) 处理处于 WAIT_START 状态的从节点，如果 BGSAVE 成功启动，
 *    则为它们准备完全同步；否则向它们发送错误并从从节点列表中删除。
 *
 * 2) 如果 BGSAVE 实际已启动，则刷新 Lua 脚本缓存。
 *
 * 成功返回 C_OK，否则返回 C_ERR。 */
int startBgsaveForReplication(int mincapa, int req) {
    int retval;
    int socket_target = 0;
    listIter li;
    listNode *ln;

    /* 如果从节点能够处理 EOF 标记并且我们配置为无盘同步，则使用套接字目标。
     * 注意，在创建"过滤型"RDB（例如仅函数）时，我们也强制使用套接字复制，
     * 以避免过滤数据覆盖快照 RDB 文件。 */
    socket_target = (server.repl_diskless_sync || req & SLAVE_REQ_RDB_MASK) && (mincapa & SLAVE_CAPA_EOF);
    /* 如果不支持套接字并且需要过滤器，`SYNC` 应该已经返回错误，这里进行断言。 */
    serverAssert(socket_target || !(req & SLAVE_REQ_RDB_MASK));

    serverLog(LL_NOTICE,"Starting BGSAVE for SYNC with target: %s",
        socket_target ? "replicas sockets" : "disk");

    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    /* 仅当 rsiptr 不为 NULL 时才执行 rdbSave*，
     * 否则从节点将丢失 repl-stream-db。 */
    if (rsiptr) {
        if (socket_target)
            retval = rdbSaveToSlavesSockets(req,rsiptr);
        else {
            /* 保留页缓存，因为它很快就会被使用 */
            retval = rdbSaveBackground(req, server.rdb_filename, rsiptr, RDBFLAGS_REPLICATION | RDBFLAGS_KEEP_CACHE);
        }
    } else {
        serverLog(LL_WARNING,"BGSAVE for replication: replication information not available, can't generate the RDB file right now. Try later.");
        retval = C_ERR;
    }

    /* 如果我们成功启动了使用磁盘目标的 BGSAVE，让我们记住这个事实，
     * 以便稍后可以在需要时删除该文件。注意，如果该特性被禁用，
     * 我们不会将标志设置为 1，否则该标志将永远不会被清除：文件不会被删除。
     * 这样如果用户稍后通过 CONFIG SET 启用它，我们也是安全的。 */
    if (retval == C_OK && !socket_target && server.rdb_del_sync_files)
        RDBGeneratedByReplication = 1;

    /* 如果 BGSAVE 失败，从从节点列表中删除等待完全重同步的从节点，
     * 通知它们发生了什么错误，并尽快关闭连接。 */
    if (retval == C_ERR) {
        serverLog(LL_WARNING,"BGSAVE for replication failed");
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                slave->replstate = REPL_STATE_NONE;
                slave->flags &= ~CLIENT_SLAVE;
                listDelNode(server.slaves,ln);
                addReplyError(slave,
                    "BGSAVE failed, replication can't continue");
                slave->flags |= CLIENT_CLOSE_AFTER_REPLY;
            }
        }
        return retval;
    }

    /* 如果目标是套接字，rdbSaveToSlavesSockets() 已经为从节点
     * 设置了完全重同步。否则对于磁盘目标，现在进行设置。 */
    if (!socket_target) {
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                /* 检查从节点是否具有完全相同的需求 */
                if (slave->slave_req != req)
                    continue;
                replicationSetupSlaveForFullResync(slave, getPsyncInitialOffset());
            }
        }
    }

    return retval;
}

/* SYNC 和 PSYNC 命令实现。 */
void syncCommand(client *c) {
    /* 如果已经是 slave 或处于 monitor 模式，则忽略 SYNC */
    if (c->flags & CLIENT_SLAVE) return;

    /* 检查这是否是针对具有相同 replid 的副本的故障转移请求，
     * 如果是，则成为主节点。 */
    if (c->argc > 3 && !strcasecmp(c->argv[0]->ptr,"psync") &&
        !strcasecmp(c->argv[3]->ptr,"failover"))
    {
        serverLog(LL_NOTICE, "Failover request received for replid %s.",
            (unsigned char *)c->argv[1]->ptr);
        if (!server.masterhost) {
            addReplyError(c, "PSYNC FAILOVER can't be sent to a master.");
            return;
        }

        if (!strcasecmp(c->argv[1]->ptr,server.replid)) {
            if (server.cluster_enabled) {
                clusterPromoteSelfToMaster();
            } else {
                replicationUnsetMaster();
            }
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,
                "MASTER MODE enabled (failover request from '%s')",client);
            sdsfree(client);
        } else {
            addReplyError(c, "PSYNC FAILOVER replid must match my replid.");
            return;
        }
    }

    /* 在我们进行故障转移时，不允许副本与我们同步 */
    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c,"-NOMASTERLINK Can't SYNC while failing over");
        return;
    }

    /* 如果我们是 slave 但与主节点的连接不正常，则拒绝 SYNC 请求... */
    if (server.masterhost && server.repl_state != REPL_STATE_CONNECTED) {
        addReplyError(c,"-NOMASTERLINK Can't SYNC while not connected with my master");
        return;
    }

    /* 当服务器有待发送数据给客户端（关于已发出的命令）时，无法发出 SYNC。
     * 我们需要一个全新的回复缓冲区来记录 BGSAVE 与当前数据集之间的差异，
     * 以便在需要时可以复制到其他从节点。 */
    if (clientHasPendingReplies(c)) {
        addReplyError(c,"SYNC and PSYNC are invalid with pending output");
        return;
    }

    /* 如果从节点不支持 EOF 能力但需要过滤型 RDB，则同步失败。
     * 这是因为我们强制通过套接字而非文件生成过滤型 RDB，
     * 以避免与快照文件冲突。如果需要，强制使用套接字的处理在
     * `startBgsaveForReplication` 中。 */
    if (c->slave_req & SLAVE_REQ_RDB_MASK && !(c->slave_capa & SLAVE_CAPA_EOF)) {
        addReplyError(c,"Filtered replica requires EOF capability");
        return;
    }

    serverLog(LL_NOTICE,"Replica %s asks for synchronization",
        replicationGetSlaveName(c));

    /* 如果这是 PSYNC 命令，尝试部分重同步。
     * 如果失败，我们将继续通常的完全重同步，但是当这种情况发生时
     * replicationSetupSlaveForFullResync 将响应：
     *
     * +FULLRESYNC <replid> <offset>
     *
     * 这样从节点就知道新的 replid 和偏移量，以便在与主节点连接断开时
     * 稍后尝试 PSYNC。 */
    if (!strcasecmp(c->argv[0]->ptr,"psync")) {
        long long psync_offset;
        if (getLongLongFromObjectOrReply(c, c->argv[2], &psync_offset, NULL) != C_OK) {
            serverLog(LL_WARNING, "Replica %s asks for synchronization but with a wrong offset",
                      replicationGetSlaveName(c));
            return;
        }

        if (masterTryPartialResynchronization(c, psync_offset) == C_OK) {
            server.stat_sync_partial_ok++;
            return; /* 无需完全重同步，直接返回。 */
        } else {
            char *master_replid = c->argv[1]->ptr;

            /* 为失败的 PSYNC 增加统计信息，但仅当 replid 不是 "?" 时，
             * 因为 "?" 由从节点在无法部分重同步时主动用于强制完全重同步。 */
            if (master_replid[0] != '?') server.stat_sync_partial_err++;
        }
    } else {
        /* 如果从节点使用 SYNC，我们面对的是旧版本的复制协议实现
         * （如 redis-cli --slave）。标记该客户端，以便我们不期望收到
         * REPLCONF ACK 反馈。 */
        c->flags |= CLIENT_PRE_PSYNC;
    }

    /* 完全重同步。 */
    server.stat_sync_full++;

    /* 将从节点设置为等待 BGSAVE 开始。以下代码路径将根据我们对从节点
     * 的不同处理来更改状态。 */
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
    if (server.repl_disable_tcp_nodelay)
        connDisableTcpNoDelay(c->conn); /* 失败不致命。 */
    c->repldbfd = -1;
    c->flags |= CLIENT_SLAVE;
    listAddNodeTail(server.slaves,c);

    /* 如果需要，创建复制积压缓冲区。 */
    if (listLength(server.slaves) == 1 && server.repl_backlog == NULL) {
        /* 当我们从头创建积压缓冲区时，我们总是使用新的复制 ID
         * 并清除 ID2，因为没有有效的过去历史。 */
        changeReplicationId();
        clearReplicationId2();
        createReplicationBacklog();
        serverLog(LL_NOTICE,"Replication backlog created, my new "
                            "replication IDs are '%s' and '%s'",
                            server.replid, server.replid2);
    }

    /* 情况 1：BGSAVE 正在进行，目标为磁盘。 */
    if (server.child_type == CHILD_TYPE_RDB &&
        server.rdb_child_type == RDB_CHILD_TYPE_DISK)
    {
        /* 好的，后台保存正在进行。让我们检查它是否适合复制，
         * 即自服务器 fork 进行保存以来是否有另一个从节点正在注册差异。 */
        client *slave;
        listNode *ln;
        listIter li;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            /* 如果客户端需要命令缓冲区，我们不能使用没有复制缓冲区的副本。 */
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
                (!(slave->flags & CLIENT_REPL_RDBONLY) ||
                 (c->flags & CLIENT_REPL_RDBONLY)))
                break;
        }
        /* 要附加此从节点，我们检查它至少具有触发当前 BGSAVE 的从节点
         * 的所有能力以及其确切的要求。 */
        if (ln && ((c->slave_capa & slave->slave_capa) == slave->slave_capa) &&
            c->slave_req == slave->slave_req) {
            /* 完美，服务器已经在为另一个从节点注册差异。
             * 设置正确的状态，并复制缓冲区。如果客户端不需要，我们不复制缓冲区。 */
            if (!(c->flags & CLIENT_REPL_RDBONLY))
                copyReplicaOutputBuffer(c,slave);
            replicationSetupSlaveForFullResync(c,slave->psync_initial_offset);
            serverLog(LL_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* 没有办法，我们需要等待下一次 BGSAVE 以注册差异。 */
            serverLog(LL_NOTICE,"Can't attach the replica to the current BGSAVE. Waiting for next BGSAVE for SYNC");
        }

    /* 情况 2：BGSAVE 正在进行，目标为套接字。 */
    } else if (server.child_type == CHILD_TYPE_RDB &&
               server.rdb_child_type == RDB_CHILD_TYPE_SOCKET)
    {
        /* 存在 RDB 子进程，但它直接写入子套接字。我们需要等待下一次 BGSAVE
         * 才能同步。 */
        serverLog(LL_NOTICE,"Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC");

    /* 情况 3：没有 BGSAVE 在进行中。 */
    } else {
        if (server.repl_diskless_sync && (c->slave_capa & SLAVE_CAPA_EOF) &&
            server.repl_diskless_sync_delay)
        {
            /* 无盘复制的 RDB 子进程在 replicationCron() 内部创建，
             * 因为我们希望延迟几秒钟启动以等待更多从节点到达。 */
            serverLog(LL_NOTICE,"Delay next BGSAVE for diskless SYNC");
        } else {
            /* 我们没有 BGSAVE 在进行，让我们启动一个。无盘模式或基于磁盘的模式
             * 由副本的能力决定。 */
            if (!hasActiveChildProcess()) {
                startBgsaveForReplication(c->slave_capa, c->slave_req);
            } else {
                serverLog(LL_NOTICE,
                    "No BGSAVE in progress, but another BG operation is active. "
                    "BGSAVE for replication delayed");
            }
        }
    }
    return;
}

/* REPLCONF <option> <value> <option> <value> ...
 * 此命令由副本在通过 SYNC 命令启动复制之前用于配置复制过程。
 * 此命令也由主节点用于从副本获取复制偏移量。
 *
 * 目前我们支持以下选项：
 *
 * - listening-port <port>
 * - ip-address <ip>
 *   副本 Redis 实例的监听 IP 和端口，以便主节点可以在 INFO 输出中
 *   准确地列出副本及其监听端口。
 *
 * - capa <eof|psync2>
 *   此实例支持的能力。
 *   eof: 支持 EOF 风格的 RDB 传输以用于无盘复制。
 *   psync2: 支持 PSYNC v2，因此理解 +CONTINUE <new repl ID>。
 *
 * - ack <offset> [fack <aofofs>]
 *   副本通知主节点它目前已处理的复制流数量，以及可选的已 fsync 到 AOF 文件
 *   的复制偏移量。此特殊模式不会向调用者回复。
 *
 * - getack <dummy>
 *   与其他子命令不同，此命令由主节点用于从副本获取复制偏移量。
 *
 * - rdb-only <0|1>
 *   仅需要 RDB 快照，不需要复制缓冲区。
 *
 * - rdb-filter-only <include-filters>
 *   为 RDB 快照定义"包含"过滤器。目前我们仅支持单个包含过滤器："functions"。
 *   传递空字符串 "" 将生成空的 RDB。 */
void replconfCommand(client *c) {
    int j;

    if ((c->argc % 2) == 0) {
        /* 参数数量必须为奇数，以确保每个选项都有一个对应的值。 */
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* 处理每个选项-值对。 */
    for (j = 1; j < c->argc; j+=2) {
        if (!strcasecmp(c->argv[j]->ptr,"listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c,c->argv[j+1],
                    &port,NULL) != C_OK))
                return;
            c->slave_listening_port = port;
        } else if (!strcasecmp(c->argv[j]->ptr,"ip-address")) {
            sds addr = c->argv[j+1]->ptr;
            if (sdslen(addr) < NET_HOST_STR_LEN) {
                if (c->slave_addr) sdsfree(c->slave_addr);
                c->slave_addr = sdsdup(addr);
            } else {
                addReplyErrorFormat(c,"REPLCONF ip-address provided by "
                    "replica instance is too long: %zd bytes", sdslen(addr));
                return;
            }
        } else if (!strcasecmp(c->argv[j]->ptr,"capa")) {
            /* 忽略主节点不理解的 capability。 */
            if (!strcasecmp(c->argv[j+1]->ptr,"eof"))
                c->slave_capa |= SLAVE_CAPA_EOF;
            else if (!strcasecmp(c->argv[j+1]->ptr,"psync2"))
                c->slave_capa |= SLAVE_CAPA_PSYNC2;
        } else if (!strcasecmp(c->argv[j]->ptr,"ack")) {
            /* REPLCONF ACK 由从节点用于通知主节点它目前已处理的复制流数量。
             * 这是一个内部命令，普通客户端永远不应使用。 */
            long long offset;

            if (!(c->flags & CLIENT_SLAVE)) return;
            if ((getLongLongFromObject(c->argv[j+1], &offset) != C_OK))
                return;
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;
            if (c->argc > j+3 && !strcasecmp(c->argv[j+2]->ptr,"fack")) {
                if ((getLongLongFromObject(c->argv[j+3], &offset) != C_OK))
                    return;
                if (offset > c->repl_aof_off)
                    c->repl_aof_off = offset;
            }
            c->repl_ack_time = server.unixtime;
            /* 如果这是无盘复制，我们需要在收到第一个 ACK 时真正将
             * 从节点置于在线状态（这确认了从节点已在线并准备好接收更多数据）。
             * 这样在流式传输 RDB 文件时，可以更简单且更少占用 CPU 来检测 EOF。
             * 有可能在检测到 bgsave 完成之前 ACK 就到达了我们（因为这取决于 cron 滴答），
             * 所以先快速检查一下（而不是等待下一个 ACK）。 */
            if (server.child_type == CHILD_TYPE_RDB && c->replstate == SLAVE_STATE_WAIT_BGSAVE_END)
                checkChildrenDone();
            if (c->repl_start_cmd_stream_on_ack && c->replstate == SLAVE_STATE_ONLINE)
                replicaStartCommandStream(c);
            /* 注意：此命令不会回复任何内容！ */
            return;
        } else if (!strcasecmp(c->argv[j]->ptr,"getack")) {
            /* REPLCONF GETACK 用于尽快向从节点请求 ACK。 */
            if (server.masterhost && server.master) replicationSendAck();
            return;
        } else if (!strcasecmp(c->argv[j]->ptr,"rdb-only")) {
           /* REPLCONF RDB-ONLY 用于标识客户端只需要 RDB 快照而无需复制缓冲区。 */
            long rdb_only = 0;
            if (getRangeLongFromObjectOrReply(c,c->argv[j+1],
                    0,1,&rdb_only,NULL) != C_OK)
                return;
            if (rdb_only == 1) c->flags |= CLIENT_REPL_RDBONLY;
            else c->flags &= ~CLIENT_REPL_RDBONLY;
        } else if (!strcasecmp(c->argv[j]->ptr,"rdb-filter-only")) {
            /* REPLCONFG RDB-FILTER-ONLY 用于为 RDB 快照定义"包含"过滤器。
             * 目前我们仅支持单个包含过滤器："functions"。将来我们可能希望添加
             * 其他过滤器，如 key patterns、key types、non-volatile、module aux 字段等。
             * 我们可能希望添加互补的 "RDB-FILTER-EXCLUDE" 来过滤掉某些数据。 */
            int filter_count, i;
            sds *filters;
            if (!(filters = sdssplitargs(c->argv[j+1]->ptr, &filter_count))) {
                addReplyError(c, "Missing rdb-filter-only values");
                return;
            }
            /* 默认情况下过滤掉 RDB 的所有部分 */
            c->slave_req |= SLAVE_REQ_RDB_EXCLUDE_DATA;
            c->slave_req |= SLAVE_REQ_RDB_EXCLUDE_FUNCTIONS;
            for (i = 0; i < filter_count; i++) {
                if (!strcasecmp(filters[i], "functions"))
                    c->slave_req &= ~SLAVE_REQ_RDB_EXCLUDE_FUNCTIONS;
                else {
                    addReplyErrorFormat(c, "Unsupported rdb-filter-only option: %s", (char*)filters[i]);
                    sdsfreesplitres(filters, filter_count);
                    return;
                }
            }
            sdsfreesplitres(filters, filter_count);
        } else {
            addReplyErrorFormat(c,"Unrecognized REPLCONF option: %s",
                (char*)c->argv[j]->ptr);
            return;
        }
    }
    addReply(c,shared.ok);
}

/* 此函数将副本置于在线状态，应在副本接收到初始同步的 RDB 文件后立即调用。
 *
 * 它执行以下操作：
 * 1) 将从节点置于 ONLINE 状态。
 * 2) 更新"良好副本"的计数。
 * 3) 触发模块事件。
 *
 * 返回值指示副本是否应断开连接。
 * */
int replicaPutOnline(client *slave) {
    if (slave->flags & CLIENT_REPL_RDBONLY) {
        slave->replstate = SLAVE_STATE_RDB_TRANSMITTED;
        /* 客户端仅要求 RDB，因此我们应该尽快关闭它 */
        serverLog(LL_NOTICE,
                  "RDB transfer completed, rdb only replica (%s) should be disconnected asap",
                  replicationGetSlaveName(slave));
        return 0;
    }
    slave->replstate = SLAVE_STATE_ONLINE;
    slave->repl_ack_time = server.unixtime; /* 防止错误的超时。 */

    refreshGoodSlavesCount();
    /* 触发副本变更的模块事件。 */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);
    serverLog(LL_NOTICE,"Synchronization with replica %s succeeded",
        replicationGetSlaveName(slave));
    return 1;
}

/* 此函数应在副本接收到初始同步的 RDB 文件后调用，
 * 此时我们终于准备好发送增量命令流。
 *
 * 它执行以下操作：
 * 1) 如果副本不需要复制命令缓冲区流，异步关闭其连接，
 *    因为它实际上不是有效的副本。
 * 2) 确保重新安装可写事件，因为在调用 SYNC 命令时我们没有回复
 *    并且它被禁用了，然后我们可以累积输出缓冲区数据而无需发送给副本，
 *    这样就不会与 RDB 流混合。 */
void replicaStartCommandStream(client *slave) {
    serverAssert(!(slave->flags & CLIENT_REPL_RDBONLY));
    slave->repl_start_cmd_stream_on_ack = 0;

    putClientInPendingWriteQueue(slave);
}

/* 我们定期调用此函数以删除因复制而生成的 RDB 文件，
 * 用于那些没有其他持久化机制的实例。
 * 我们不希望没有持久化的实例携带 RDB 文件，
 * 因为这在某些环境中违反了某些策略。 */
void removeRDBUsedToSyncReplicas(void) {
    /* 如果该特性被禁用，立即返回但同时清除 RDBGeneratedByReplication 标志（如果已设置）。
     * 否则如果该特性之前被启用但后来通过 CONFIG SET 禁用了，
     * 该标志可能保持为 1：下次通过 CONFIG SET 重新启用该特性时，
     * 即使最近没有因为复制而生成 RDB，该标志也是设置的。 */
    if (!server.rdb_del_sync_files) {
        RDBGeneratedByReplication = 0;
        return;
    }

    if (allPersistenceDisabled() && RDBGeneratedByReplication) {
        client *slave;
        listNode *ln;
        listIter li;

        int delrdb = 1;
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
                slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END ||
                slave->replstate == SLAVE_STATE_SEND_BULK)
            {
                delrdb = 0;
                break; /* 无需检查其他副本。 */
            }
        }
        if (delrdb) {
            struct stat sb;
            if (lstat(server.rdb_filename,&sb) != -1) {
                RDBGeneratedByReplication = 0;
                serverLog(LL_NOTICE,
                    "Removing the RDB file used to feed replicas "
                    "in a persistence-less instance");
                bg_unlink(server.rdb_filename);
            }
        }
    }
}

/* 关闭 repldbfd 并在客户端持有对复制 DB 的最后一个引用时回收页缓存 */
void closeRepldbfd(client *myself) {
    listNode *ln;
    listIter li;
    int reclaim = 1;
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        if (slave != myself && slave->replstate == SLAVE_STATE_SEND_BULK) {
            reclaim = 0;
            break;
        }
    }

    if (reclaim) {
        bioCreateCloseJob(myself->repldbfd, 0, 1);
    } else {
        close(myself->repldbfd);
    }
    myself->repldbfd = -1;
}

/* 向从节点发送 RDB 批量数据。
 * conn 为对应的从节点连接。 */
void sendBulkToSlave(connection *conn) {
    client *slave = connGetPrivateData(conn);
    char buf[PROTO_IOBUF_LEN];
    ssize_t nwritten, buflen;

    /* 在发送 RDB 文件之前，我们发送由复制过程配置的前导字节。
     * 目前前导字节只是文件的批量计数，格式为 "$<length>\r\n"。 */
    if (slave->replpreamble) {
        nwritten = connWrite(conn,slave->replpreamble,sdslen(slave->replpreamble));
        if (nwritten == -1) {
            serverLog(LL_WARNING,
                "Write error sending RDB preamble to replica: %s",
                connGetLastError(conn));
            freeClient(slave);
            return;
        }
        atomicIncr(server.stat_net_repl_output_bytes, nwritten);
        sdsrange(slave->replpreamble,nwritten,-1);
        if (sdslen(slave->replpreamble) == 0) {
            sdsfree(slave->replpreamble);
            slave->replpreamble = NULL;
            /* 继续发送数据。 */
        } else {
            return;
        }
    }

    /* 如果前导字节已经传输完毕，则发送 RDB 批量数据。 */
    lseek(slave->repldbfd,slave->repldboff,SEEK_SET);
    buflen = read(slave->repldbfd,buf,PROTO_IOBUF_LEN);
    if (buflen <= 0) {
        serverLog(LL_WARNING,"Read error sending DB to replica: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }
    if ((nwritten = connWrite(conn,buf,buflen)) == -1) {
        if (connGetState(conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_WARNING,"Write error sending DB to replica: %s",
                connGetLastError(conn));
            freeClient(slave);
        }
        return;
    }
    slave->repldboff += nwritten;
    atomicIncr(server.stat_net_repl_output_bytes, nwritten);
    if (slave->repldboff == slave->repldbsize) {
        closeRepldbfd(slave);
        connSetWriteHandler(slave->conn,NULL);
        if (!replicaPutOnline(slave)) {
            freeClient(slave);
            return;
        }
        replicaStartCommandStream(slave);
    }
}

/* 在 rdb 管道传输期间，从等待可写的连接列表中移除一个写处理器。 */
void rdbPipeWriteHandlerConnRemoved(struct connection *conn) {
    if (!connHasWriteHandler(conn))
        return;
    connSetWriteHandler(conn, NULL);
    client *slave = connGetPrivateData(conn);
    slave->repl_last_partial_write = 0;
    server.rdb_pipe_numconns_writing--;
    /* 如果该连接当前没有更多待写入数据，或发生写入错误： */
    if (server.rdb_pipe_numconns_writing == 0) {
        if (aeCreateFileEvent(server.el, server.rdb_pipe_read, AE_READABLE, rdbPipeReadHandler,NULL) == AE_ERR) {
            serverPanic("Unrecoverable error creating server.rdb_pipe_read file event.");
        }
    }
}

/* 在无盘主节点传输 rdb 管道数据期间，当副本再次变为可写时调用。 */
void rdbPipeWriteHandler(struct connection *conn) {
    serverAssert(server.rdb_pipe_bufflen>0);
    client *slave = connGetPrivateData(conn);
    ssize_t nwritten;
    if ((nwritten = connWrite(conn, server.rdb_pipe_buff + slave->repldboff,
                              server.rdb_pipe_bufflen - slave->repldboff)) == -1)
    {
        if (connGetState(conn) == CONN_STATE_CONNECTED)
            return; /* 等同于 EAGAIN（稍后重试） */
        serverLog(LL_WARNING,"Write error sending DB to replica: %s",
            connGetLastError(conn));
        freeClient(slave);
        return;
    } else {
        slave->repldboff += nwritten;
        atomicIncr(server.stat_net_repl_output_bytes, nwritten);
        if (slave->repldboff < server.rdb_pipe_bufflen) {
            slave->repl_last_partial_write = server.unixtime;
            return; /* 还有更多数据需要写入 */
        }
    }
    rdbPipeWriteHandlerConnRemoved(conn);
}

/* 在无盘主节点中，当有数据可从子进程的 rdb 管道读取时调用 */
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    int i;
    if (!server.rdb_pipe_buff)
        server.rdb_pipe_buff = zmalloc(PROTO_IOBUF_LEN);
    serverAssert(server.rdb_pipe_numconns_writing==0);

    while (1) {
        server.rdb_pipe_bufflen = read(fd, server.rdb_pipe_buff, PROTO_IOBUF_LEN);
        if (server.rdb_pipe_bufflen < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            serverLog(LL_WARNING,"Diskless rdb transfer, read error sending DB to replicas: %s", strerror(errno));
            for (i=0; i < server.rdb_pipe_numconns; i++) {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn)
                    continue;
                client *slave = connGetPrivateData(conn);
                freeClient(slave);
                server.rdb_pipe_conns[i] = NULL;
            }
            killRDBChild();
            return;
        }

        if (server.rdb_pipe_bufflen == 0) {
            /* EOF - 写入端已关闭。 */
            int stillUp = 0;
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            for (i=0; i < server.rdb_pipe_numconns; i++)
            {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn)
                    continue;
                stillUp++;
            }
            serverLog(LL_NOTICE,"Diskless rdb transfer, done reading from pipe, %d replicas still up.", stillUp);
            /* 副本已完成读取，通知子进程可以安全退出。
             * 当服务器检测到子进程已退出后，即可将副本标记为在线，
             * 并开始流式传输复制缓冲区。 */
            close(server.rdb_child_exit_pipe);
            server.rdb_child_exit_pipe = -1;
            return;
        }

        int stillAlive = 0;
        for (i=0; i < server.rdb_pipe_numconns; i++)
        {
            ssize_t nwritten;
            connection *conn = server.rdb_pipe_conns[i];
            if (!conn)
                continue;

            client *slave = connGetPrivateData(conn);
            if ((nwritten = connWrite(conn, server.rdb_pipe_buff, server.rdb_pipe_bufflen)) == -1) {
                if (connGetState(conn) != CONN_STATE_CONNECTED) {
                    serverLog(LL_WARNING,"Diskless rdb transfer, write error sending DB to replica: %s",
                        connGetLastError(conn));
                    freeClient(slave);
                    server.rdb_pipe_conns[i] = NULL;
                    continue;
                }
                /* 发生错误但仍处于连接状态，等同于 EAGAIN */
                slave->repldboff = 0;
            } else {
                /* 注意：使用无盘复制时，'repldboff' 表示已发送的
                 * 'rdb_pipe_buff' 的偏移量，而非整个 RDB 的偏移量。 */
                slave->repldboff = nwritten;
                atomicIncr(server.stat_net_repl_output_bytes, nwritten);
            }
            /* 如果无法将所有数据写入某个副本，
             * 则设置写处理器（并在下方禁用管道读处理器） */
            if (nwritten != server.rdb_pipe_bufflen) {
                slave->repl_last_partial_write = server.unixtime;
                server.rdb_pipe_numconns_writing++;
                connSetWriteHandler(conn, rdbPipeWriteHandler);
            }
            stillAlive++;
        }

        if (stillAlive == 0) {
            serverLog(LL_WARNING,"Diskless rdb transfer, last replica dropped, killing fork child.");
            /* 避免在 killRDBChild 后删除事件，因为这可能触发其他副本的新 bgsave。 */
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE); 
            killRDBChild();
            break;
        }
        /* 如果至少设置了一个写处理器，则移除管道读处理器。 */
        else if (server.rdb_pipe_numconns_writing) {
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            break;
        }
    }
}

/* 此函数在每次后台保存结束时被调用。
 *
 * 参数 bgsaveerr 如果后台保存成功则为 C_OK，否则向函数传递 C_ERR。
 * 'type' 参数是终止的子进程类型（如果它有磁盘或套接字目标）。 */
void updateSlavesWaitingBgsave(int bgsaveerr, int type) {
    listNode *ln;
    listIter li;

    /* 注意：我们有可能从 REPLCONF ACK 命令中到达这里，
     * 因此必须避免使用 freeClient，否则我们将在返回路径上崩溃。 */

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        /* 我们可能通过 freeClient()->killRDBChild()->checkChildrenDone() 到达这里。
         * 跳过已断开的从节点。 */
        if (!slave->conn) continue;

        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            struct redis_stat buf;

            if (bgsaveerr != C_OK) {
                freeClientAsync(slave);
                serverLog(LL_WARNING,"SYNC failed. BGSAVE child returned an error");
                continue;
            }

            /* 如果这是 RDB 保存到磁盘，我们必须准备将磁盘上的 RDB 发送到从节点套接字。
             * 否则如果这已经是 RDB -> 从节点套接字的传输（用于无盘复制），
             * 我们的工作很简单，只需将从节点置于在线状态即可。 */
            if (type == RDB_CHILD_TYPE_SOCKET) {
                serverLog(LL_NOTICE,
                    "Streamed RDB transfer with replica %s succeeded (socket). Waiting for REPLCONF ACK from replica to enable streaming",
                        replicationGetSlaveName(slave));
                /* 注意：我们等待来自副本的 REPLCONF ACK 消息，
                 * 以便真正将其置于在线状态（安装写处理器，以便可以传输累积的数据）。
                 * 但是我们会尽快更改复制状态，因为我们的从节点在技术上现在已经在线。
                 *
                 * 所以事情是这样工作的：
                 *
                 * 1. 我们通过套接字结束 RDB 文件的传输。
                 * 2. 副本被置于 ONLINE，但未安装写处理器。
                 * 3. 然而副本实际上线，并通过 REPLCONF ACK 命令向我们回 ping。
                 * 4. 现在我们最终安装写处理器，并将迄今为止累积的缓冲区发送给副本。
                 *
                 * 但我们为什么要这样做？因为当我们通过套接字直接流式传输 RDB 时，
                 * 副本必须检测 RDB EOF（文件结束），它是 RDB 末尾的一个特殊随机字符串
                 * （对于流式 RDB，我们事先不知道长度）。如果在最终 EOF 之后不再发送数据，
                 * 则检测这样的最终 EOF 字符串要简单得多且 CPU 占用更少。
                 * 因此我们不想将 RDB 传输的结束与其他复制数据的开始粘合在一起。 */
                if (!replicaPutOnline(slave)) {
                    freeClientAsync(slave);
                    continue;
                }
                slave->repl_start_cmd_stream_on_ack = 1;
            } else {
                if ((slave->repldbfd = open(server.rdb_filename,O_RDONLY)) == -1 ||
                    redis_fstat(slave->repldbfd,&buf) == -1) {
                    freeClientAsync(slave);
                    serverLog(LL_WARNING,"SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                    continue;
                }
                slave->repldboff = 0;
                slave->repldbsize = buf.st_size;
                slave->replstate = SLAVE_STATE_SEND_BULK;
                slave->replpreamble = sdscatprintf(sdsempty(),"$%lld\r\n",
                    (unsigned long long) slave->repldbsize);

                connSetWriteHandler(slave->conn,NULL);
                if (connSetWriteHandler(slave->conn,sendBulkToSlave) == C_ERR) {
                    freeClientAsync(slave);
                    continue;
                }
            }
        }
    }
}

/* 将当前实例的复制 ID 更改为新的随机 ID。
 * 这将阻止此主节点与其他从节点之间成功的 PSYNC，
 * 因此应在发生改变数据集当前历史的事情时调用该命令。 */
void changeReplicationId(void) {
    getRandomHexChars(server.replid,CONFIG_RUN_ID_SIZE);
    server.replid[CONFIG_RUN_ID_SIZE] = '\0';
}

/* 清除（失效）辅助复制 ID。例如，在完全重同步之后，
 * 当我们开始新的复制历史时，会发生这种情况。 */
void clearReplicationId2(void) {
    memset(server.replid2,'0',sizeof(server.replid));
    server.replid2[CONFIG_RUN_ID_SIZE] = '\0';
    server.second_replid_offset = -1;
}

/* 使用当前的复制 ID / 偏移量作为辅助复制 ID，并更改当前的 ID 以开始新的历史。
 * 当实例从从节点切换为主节点时应使用此函数，
 * 以便它可以服务使用主节点复制 ID 执行的 PSYNC 请求。 */
void shiftReplicationId(void) {
    memcpy(server.replid2,server.replid,sizeof(server.replid));
    /* 我们将第二个 replid 偏移量设置为主节点偏移量 + 1，
     * 因为从节点将请求它尚未接收的第一个字节，所以我们需要将偏移量加一：
     * 例如，如果作为从节点，我们确定与主节点有 50 字节的相同历史，
     * 在我们被转变为主节点之后，我们可以接受偏移量为 51 的 PSYNC 请求，
     * 因为请求的从节点具有直到第 50 字节的相同历史，
     * 并且正在请求从偏移量 51 开始的新字节。 */
    server.second_replid_offset = server.master_repl_offset+1;
    changeReplicationId();
    serverLog(LL_NOTICE,"Setting secondary replication ID to %s, valid up to offset: %lld. New replication ID is %s", server.replid2, server.second_replid_offset, server.replid);
}

/* ----------------------------------- SLAVE -------------------------------- */

/* 如果给定的复制状态是握手状态则返回 1，否则返回 0。 */
int slaveIsInHandshakeState(void) {
    return server.repl_state >= REPL_STATE_RECEIVE_PING_REPLY &&
           server.repl_state <= REPL_STATE_RECEIVE_PSYNC_REPLY;
}

/* 避免主节点在初始同步时检测到从节点加载 RDB 文件超时。
 * 我们发送单个换行符，它是有效的协议，但由于字节是不可分割的，
 * 所以保证要么完全发送，要么不发送。
 *
 * 该函数在两种上下文中被调用：在我们使用 emptyData() 清空当前数据时，
 * 以及在我们加载从主节点接收为 RDB 文件的新数据时。 */
void replicationSendNewlineToMaster(void) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        /* 此阶段的回 ping 是尽力而为的。 */
        if (server.repl_transfer_s) connWrite(server.repl_transfer_s, "\n", 1);
    }
}

/* 在清空旧数据以加载从主节点接收到的新数据集时由 emptyData() 调用的回调，
 * 以及在加载成功或失败后由 discardTempDb() 调用。 */
void replicationEmptyDbCallback(dict *d) {
    UNUSED(d);
    if (server.repl_state == REPL_STATE_TRANSFER)
        replicationSendNewlineToMaster();
}

/* 一旦我们与主节点建立了链接并执行了同步，此函数将从指定的文件描述符
 * 实例化我们存储在 server.master 处的主节点客户端。 */
void replicationCreateMasterClient(connection *conn, int dbid) {
    server.master = createClient(conn);
    if (conn)
        connSetReadHandler(server.master->conn, readQueryFromClient);

    /**
     * 重要说明：
     * CLIENT_DENY_BLOCKING 标志不应（也不应）在此处设置。
     * 对于 BLPOP 之类的命令，阻塞主节点连接是没有意义的，
     * 这种阻塞尝试可能会导致死锁并破坏复制。我们认为这样的
     * 行为是一个 bug，因为像 BLPOP 这样的命令永远不应该在复制链路上发送。
     * 阻塞复制链路的一个可能用例是模块希望将执行传递给后台线程，
     * 并在执行完成后解除阻塞。这是我们允许阻塞复制连接的原因。 */
    server.master->flags |= CLIENT_MASTER;

    server.master->authenticated = 1;
    server.master->reploff = server.master_initial_offset;
    server.master->read_reploff = server.master->reploff;
    server.master->user = NULL; /* 此客户端可以执行任何操作。 */
    memcpy(server.master->replid, server.master_replid,
        sizeof(server.master_replid));
    /* 如果主节点偏移量设置为 -1，则此主节点是旧版本的，不具备 PSYNC 能力，
     * 因此我们相应地标记它。 */
    if (server.master->reploff == -1)
        server.master->flags |= CLIENT_PRE_PSYNC;
    if (dbid != -1) selectDb(server.master,dbid);
}

/* 此函数将在主从同步后尝试重新启用 AOF 文件：
 * 如果多次尝试后仍然失败，则该从节点不能被认为是可靠的，并以错误退出。 */
void restartAOFAfterSYNC(void) {
    unsigned int tries, max_tries = 10;
    for (tries = 0; tries < max_tries; ++tries) {
        if (startAppendOnly() == C_OK) break;
        serverLog(LL_WARNING,
            "Failed enabling the AOF after successful master synchronization! "
            "Trying it again in one second.");
        sleep(1);
    }
    if (tries == max_tries) {
        serverLog(LL_WARNING,
            "FATAL: this replica instance finished the synchronization with "
            "its master, but the AOF can't be turned on. Exiting now.");
        exit(1);
    }
}

/* 计算是否使用无盘加载（diskless load）的布尔决策 */
static int useDisklessLoad(void) {
    /* 计算是否使用无盘加载的布尔决策 */
    int enabled = server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB ||
           (server.repl_diskless_load == REPL_DISKLESS_LOAD_WHEN_DB_EMPTY && dbTotalServerKeyCount()==0);

    if (enabled) {
        /* 检查所有模块是否处理读错误，否则使用无盘加载是不安全的。 */
        if (!moduleAllDatatypesHandleErrors()) {
            serverLog(LL_NOTICE,
                "Skipping diskless-load because there are modules that don't handle read errors.");
            enabled = 0;
        }
        /* 检查所有模块是否处理异步复制，否则使用无盘加载是不安全的。 */
        else if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB && !moduleAllModulesHandleReplAsyncLoad()) {
            serverLog(LL_NOTICE,
                "Skipping diskless-load because there are modules that are not aware of async replication.");
            enabled = 0;
        }
    }
    return enabled;
}

/* readSyncBulkPayload() 的辅助函数，用于在从主节点 socket 加载新 db 之前
 * 初始化 tempDb。tempDb 稍后可能由 swapMainDbWithTempDb 填充，
 * 或由 disklessLoadDiscardTempDb 释放。 */
redisDb *disklessLoadInitTempDb(void) {
    return initTempDb();
}

/* readSyncBulkPayload() 的辅助函数，用于在加载成功或失败时丢弃我们的 tempDb。 */
void disklessLoadDiscardTempDb(redisDb *tempDb) {
    discardTempDb(tempDb, replicationEmptyDbCallback);
}

/* 如果我们知道从主节点获得了完全不同的数据集，那么之后我们就无法再向
 * 我们的副本增量推送数据。如果我们有任何子副本，我们希望它们也与我们
 * 重新同步。这在我们刚刚完成 db 传输的 readSyncBulkPayload 中有用。 */
void replicationAttachToNewMaster(void) {
    /* 副本开始应用来自新主节点的数据，我们必须丢弃缓存的主节点结构。 */
    serverAssert(server.master == NULL);
    replicationDiscardCachedMaster();

    disconnectSlaves(); /* 强制我们的副本也与我们重新同步。 */
    freeReplicationBacklog(); /* 不允许我们的链式副本进行 PSYNC。 */
}

/* 异步读取我们从主节点接收到的 SYNC 载荷 */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
void readSyncBulkPayload(connection *conn) {
    char buf[PROTO_IOBUF_LEN];
    ssize_t nread, readlen, nwritten;
    int use_diskless_load = useDisklessLoad();
    redisDb *diskless_load_tempDb = NULL;
    functionsLibCtx* temp_functions_lib_ctx = NULL;
    int empty_db_flags = server.repl_slave_lazy_flush ? EMPTYDB_ASYNC :
                                                        EMPTYDB_NO_FLAGS;
    off_t left;

    /* 用于保存 EOF 标记的静态变量，以及从服务器接收的最后几个字节：
     * 当它们匹配时，我们已到达传输的末尾。 */
    static char eofmark[CONFIG_RUN_ID_SIZE];
    static char lastbytes[CONFIG_RUN_ID_SIZE];
    static int usemark = 0;

    /* 如果 repl_transfer_size == -1，我们仍然需要从主节点的回复中读取批量长度。 */
    if (server.repl_transfer_size == -1) {
        nread = connSyncReadLine(conn,buf,1024,server.repl_syncio_timeout*1000);
        if (nread == -1) {
            serverLog(LL_WARNING,
                "I/O error reading bulk count from MASTER: %s",
                connGetLastError(conn));
            goto error;
        } else {
            /* 此处的 nread 由 connSyncReadLine() 返回，它调用 syncReadLine() 并
             * 将 "\r\n" 转换为 '\0'，因此丢失了 1 个字节。 */
            atomicIncr(server.stat_net_repl_input_bytes, nread+1);
        }

        if (buf[0] == '-') {
            serverLog(LL_WARNING,
                "MASTER aborted replication with an error: %s",
                buf+1);
            goto error;
        } else if (buf[0] == '\0') {
            /* 在此阶段，单个换行符充当 PING 以保持连接活动。
             * 因此我们刷新上次交互的时间戳。 */
            server.repl_transfer_lastio = server.unixtime;
            return;
        } else if (buf[0] != '$') {
            serverLog(LL_WARNING,"Bad protocol from MASTER, the first byte is not '$' (we received '%s'), are you sure the host and port are right?", buf);
            goto error;
        }

        /* 批量载荷有两种可能的形式。一种是通常的 $<count> 批量格式。
         * 另一种用于无盘传输，当主节点事先不知道要传输的文件大小时。
         * 在后一种情况下，使用以下格式：
         *
         * $EOF:<40 bytes delimiter>
         *
         * 在文件末尾传输所声明的分隔符。该分隔符足够长且随机，
         * 与实际文件内容冲突的概率可以忽略不计。 */
        if (strncmp(buf+1,"EOF:",4) == 0 && strlen(buf+5) >= CONFIG_RUN_ID_SIZE) {
            usemark = 1;
            memcpy(eofmark,buf+5,CONFIG_RUN_ID_SIZE);
            memset(lastbytes,0,CONFIG_RUN_ID_SIZE);
            /* 设置任意的 repl_transfer_size 以避免在下次调用时
             * 进入此代码路径。 */
            server.repl_transfer_size = 0;
            serverLog(LL_NOTICE,
                "MASTER <-> REPLICA sync: receiving streamed RDB from master with EOF %s",
                use_diskless_load? "to parser":"to disk");
        } else {
            usemark = 0;
            server.repl_transfer_size = strtol(buf+1,NULL,10);
            serverLog(LL_NOTICE,
                "MASTER <-> REPLICA sync: receiving %lld bytes from master %s",
                (long long) server.repl_transfer_size,
                use_diskless_load? "to parser":"to disk");
        }
        return;
    }

    if (!use_diskless_load) {
        /* 从 socket 读取数据，将其存储到文件并搜索 EOF。 */
        if (usemark) {
            readlen = sizeof(buf);
        } else {
            left = server.repl_transfer_size - server.repl_transfer_read;
            readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
        }

        nread = connRead(conn,buf,readlen);
        if (nread <= 0) {
            if (connGetState(conn) == CONN_STATE_CONNECTED) {
                /* 等同于 EAGAIN */
                return;
            }
            serverLog(LL_WARNING,"I/O error trying to sync with MASTER: %s",
                (nread == -1) ? connGetLastError(conn) : "connection lost");
            cancelReplicationHandshake(1);
            return;
        }
        atomicIncr(server.stat_net_repl_input_bytes, nread);

        /* 当使用标记时，我们希望尽快检测到 EOF，以避免将 EOF 标记写入文件... */
        int eof_reached = 0;

        if (usemark) {
            /* 更新最后几个字节数组，并检查它是否与我们的分隔符匹配。 */
            if (nread >= CONFIG_RUN_ID_SIZE) {
                memcpy(lastbytes,buf+nread-CONFIG_RUN_ID_SIZE,
                       CONFIG_RUN_ID_SIZE);
            } else {
                int rem = CONFIG_RUN_ID_SIZE-nread;
                memmove(lastbytes,lastbytes+nread,rem);
                memcpy(lastbytes+rem,buf,nread);
            }
            if (memcmp(lastbytes,eofmark,CONFIG_RUN_ID_SIZE) == 0)
                eof_reached = 1;
        }

        /* 更新复制传输的最后 I/O 时间（用于在复制过程中检测超时），
         * 并将从 socket 获取的内容写入磁盘上的转储文件。 */
        server.repl_transfer_lastio = server.unixtime;
        if ((nwritten = write(server.repl_transfer_fd,buf,nread)) != nread) {
            serverLog(LL_WARNING,
                "Write error or short write writing to the DB dump file "
                "needed for MASTER <-> REPLICA synchronization: %s",
                (nwritten == -1) ? strerror(errno) : "short write");
            goto error;
        }
        server.repl_transfer_read += nread;

        /* 如果达到 EOF，从文件中删除最后 40 个字节。 */
        if (usemark && eof_reached) {
            if (ftruncate(server.repl_transfer_fd,
                server.repl_transfer_read - CONFIG_RUN_ID_SIZE) == -1)
            {
                serverLog(LL_WARNING,
                    "Error truncating the RDB file received from the master "
                    "for SYNC: %s", strerror(errno));
                goto error;
            }
        }

        /* 不时在磁盘上同步数据，否则在传输结束时我们可能会遭受很大的延迟，
         * 因为内存缓冲区会被复制到实际磁盘中。 */
        if (server.repl_transfer_read >=
            server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
        {
            off_t sync_size = server.repl_transfer_read -
                              server.repl_transfer_last_fsync_off;
            rdb_fsync_range(server.repl_transfer_fd,
                server.repl_transfer_last_fsync_off, sync_size);
            server.repl_transfer_last_fsync_off += sync_size;
        }

        /* 检查传输是否现已完成 */
        if (!usemark) {
            if (server.repl_transfer_read == server.repl_transfer_size)
                eof_reached = 1;
        }

        /* 如果传输尚未完成，我们需要读取更多内容，因此尽快返回并等待
         * 处理器再次被调用。 */
        if (!eof_reached) return;
    }

    /* 我们在以下情况之一到达此点：
     *
     * 1. 副本正在使用无盘复制，即它直接从 socket 读取数据到 Redis 内存，
     *    而不使用磁盘上的临时 RDB 文件。在这种情况下，我们只需阻塞并
     *    从 socket 读取所有内容。
     *
     * 2. 或者当我们从 socket 读取到 RDB 文件完成时，在这种情况下
     *    我们只想将 RDB 文件读入内存。 */

    /* 我们需要在刷新和解析 RDB 之前停止任何 AOF 重写子进程，
     * 否则我们将造成 copy-on-write 灾难。 */
    if (server.aof_state != AOF_OFF) stopAppendOnly();
    /* 在刷新和解析 RDB 之前也尝试停止 RDB 保存子进程：
     * 1. 确保后台保存不会在加载后覆盖已同步的数据。
     * 2. 避免 copy-on-write 灾难。 */
    if (server.child_type == CHILD_TYPE_RDB) {
        if (!use_diskless_load) {
            serverLog(LL_NOTICE,
                "Replica is about to load the RDB file received from the "
                "master, but there is a pending RDB child running. "
                "Killing process %ld and removing its temp file to avoid "
                "any race",
                (long) server.child_pid);
        }
        killRDBChild();
    }

    if (use_diskless_load && server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
        /* 初始化空的 tempDb 字典。 */
        diskless_load_tempDb = disklessLoadInitTempDb();
        temp_functions_lib_ctx = functionsLibCtxCreate();

        moduleFireServerEvent(REDISMODULE_EVENT_REPL_ASYNC_LOAD,
                              REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_STARTED,
                              NULL);
    } else {
        replicationAttachToNewMaster();

        serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Flushing old data");
        emptyData(-1,empty_db_flags,replicationEmptyDbCallback);
    }

    /* 在将 DB 加载到内存之前，我们需要删除可读 handler，
     * 否则它将被递归调用，因为 rdbLoad() 会不时调用事件循环
     * 来处理非阻塞加载的事件。 */
    connSetReadHandler(conn, NULL);
    
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Loading DB in memory");
    rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
    if (use_diskless_load) {
        rio rdb;
        redisDb *dbarray;
        functionsLibCtx* functions_lib_ctx;
        int asyncLoading = 0;

        if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
            /* 异步加载意味着我们在完全重同步期间继续提供读命令，
             * 并且仅在加载完成时"交换"新 db 与旧 db。
             * 它仅在 SWAPDB 无盘复制且主节点复制 ID 未更改时启用，
             * 因为在这种状态下，db 的旧内容表示我们当前从主节点接收的
             * 同一数据集的不同时间点。 */
            if (memcmp(server.replid, server.master_replid, CONFIG_RUN_ID_SIZE) == 0) {
                asyncLoading = 1;
            }
            dbarray = diskless_load_tempDb;
            functions_lib_ctx = temp_functions_lib_ctx;
        } else {
            dbarray = server.db;
            functions_lib_ctx = functionsLibCtxGetCurrent();
            functionsLibCtxClear(functions_lib_ctx);
        }

        rioInitWithConn(&rdb,conn,server.repl_transfer_size);

        /* 将 socket 置于阻塞模式以简化 RDB 传输。
         * 我们将在收到 RDB 时恢复它。 */
        connBlock(conn);
        connRecvTimeout(conn, server.repl_timeout*1000);
        startLoading(server.repl_transfer_size, RDBFLAGS_REPLICATION, asyncLoading);

        int loadingFailed = 0;
        rdbLoadingCtx loadingCtx = { .dbarray = dbarray, .functions_lib_ctx = functions_lib_ctx };
        if (rdbLoadRioWithLoadingCtx(&rdb,RDBFLAGS_REPLICATION,&rsi,&loadingCtx) != C_OK) {
            /* RDB 加载失败。 */
            serverLog(LL_WARNING,
                      "Failed trying to load the MASTER synchronization DB "
                      "from socket, check server logs.");
            loadingFailed = 1;
        } else if (usemark) {
            /* 验证结束标记是否正确。 */
            if (!rioRead(&rdb, buf, CONFIG_RUN_ID_SIZE) ||
                memcmp(buf, eofmark, CONFIG_RUN_ID_SIZE) != 0)
            {
                serverLog(LL_WARNING, "Replication stream EOF marker is broken");
                loadingFailed = 1;
            }
        }

        if (loadingFailed) {
            stopLoading(0);
            cancelReplicationHandshake(1);
            rioFreeConn(&rdb, NULL);

            if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
                /* 丢弃可能部分加载的 tempDb。 */
                moduleFireServerEvent(REDISMODULE_EVENT_REPL_ASYNC_LOAD,
                                      REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_ABORTED,
                                      NULL);

                disklessLoadDiscardTempDb(diskless_load_tempDb);
                functionsLibCtxFree(temp_functions_lib_ctx);
                serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Discarding temporary DB in background");
            } else {
                /* 删除半加载的数据，以防我们从一个空副本开始。 */
                emptyData(-1,empty_db_flags,replicationEmptyDbCallback);
            }

            /* 注意，在 SYNC 失败时重新启动 AOF 没有意义，
             * 它将在同步成功或副本升级时重新启动。 */
            return;
        }

        /* 如果我们到达此点，则 RDB 加载成功。 */
        if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
            /* 我们将很快将主 db 与 tempDb 交换，并且副本将开始
             * 应用来自新主节点的数据，我们必须丢弃缓存的主节点结构
             * 并强制子副本重新同步。 */
            replicationAttachToNewMaster();

            serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Swapping active DB with loaded DB");
            swapMainDbWithTempDb(diskless_load_tempDb);

            /* 将现有的 functions ctx 与临时的 ctx 交换 */
            functionsLibCtxSwapWithCurrent(temp_functions_lib_ctx);

            moduleFireServerEvent(REDISMODULE_EVENT_REPL_ASYNC_LOAD,
                        REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_COMPLETED,
                        NULL);

            /* 删除旧的 db，因为它现在没用了。 */
            disklessLoadDiscardTempDb(diskless_load_tempDb);
            serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Discarding old DB in background");
        }

        /* 通知 db 更改，因为复制是无盘的并且没有导致保存。 */
        server.dirty++;

        stopLoading(1);

        /* 清理并将 socket 恢复到原始状态以继续正常的复制。 */
        rioFreeConn(&rdb, NULL);
        connNonBlock(conn);
        connRecvTimeout(conn,0);
    } else {

        /* 确保新文件（也用于持久化）已完全同步
         * （之前调用 rdb_fsync_range 未能覆盖的部分）。 */
        if (fsync(server.repl_transfer_fd) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to sync the temp DB to disk in "
                "MASTER <-> REPLICA synchronization: %s",
                strerror(errno));
            cancelReplicationHandshake(1);
            return;
        }

        /* 异步重命名 RDB 文件，方式与重命名 rewrite AOF 相同。 */
        int old_rdb_fd = open(server.rdb_filename,O_RDONLY|O_NONBLOCK);
        if (rename(server.repl_transfer_tmpfile,server.rdb_filename) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to rename the temp DB into %s in "
                "MASTER <-> REPLICA synchronization: %s",
                server.rdb_filename, strerror(errno));
            cancelReplicationHandshake(1);
            if (old_rdb_fd != -1) close(old_rdb_fd);
            return;
        }
        /* 异步关闭旧 RDB 文件描述符。 */
        if (old_rdb_fd != -1) bioCreateCloseJob(old_rdb_fd, 0, 0);

        /* 同步目录以确保重命名操作被持久化。 */
        if (fsyncFileDir(server.rdb_filename) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to sync DB directory %s in "
                "MASTER <-> REPLICA synchronization: %s",
                server.rdb_filename, strerror(errno));
            cancelReplicationHandshake(1);
            return;
        }

        if (rdbLoad(server.rdb_filename,&rsi,RDBFLAGS_REPLICATION) != RDB_OK) {
            serverLog(LL_WARNING,
                "Failed trying to load the MASTER synchronization "
                "DB from disk, check server logs.");
            cancelReplicationHandshake(1);
            if (server.rdb_del_sync_files && allPersistenceDisabled()) {
                serverLog(LL_NOTICE,"Removing the RDB file obtained from "
                                    "the master. This replica has persistence "
                                    "disabled");
                bg_unlink(server.rdb_filename);
            }

            /* 如果基于磁盘的 RDB 加载失败，移除已部分加载的数据集。 */
            emptyData(-1, empty_db_flags, replicationEmptyDbCallback);

            /* 注意：同步失败时重启 AOF 没有意义，
               它会在同步成功或副本被提升时被重启。 */
            return;
        }

        /* 清理资源。 */
        if (server.rdb_del_sync_files && allPersistenceDisabled()) {
            serverLog(LL_NOTICE,"Removing the RDB file obtained from "
                                "the master. This replica has persistence "
                                "disabled");
            bg_unlink(server.rdb_filename);
        }

        zfree(server.repl_transfer_tmpfile);
        close(server.repl_transfer_fd);
        server.repl_transfer_fd = -1;
        server.repl_transfer_tmpfile = NULL;
    }

    /* 最终设置已连接的从节点 <- 主节点链接 */
    replicationCreateMasterClient(server.repl_transfer_s,rsi.repl_stream_db);
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* 触发主节点链接模块事件。 */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* 完全重同步后，我们使用主节点的 replication ID
     * 和偏移量。由于我们正在开始一段新历史，
     * 副 ID / 偏移量会被清除。 */
    memcpy(server.replid,server.master->replid,sizeof(server.replid));
    server.master_repl_offset = server.master->reploff;
    clearReplicationId2();

    /* 如果需要，创建复制积压缓冲区。从节点需要积累
     * 积压缓冲区，无论它们是否有子从节点，以便在
     * 故障转移后被提升为主节点时能正确工作。 */
    if (server.repl_backlog == NULL) createReplicationBacklog();
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Finished with success");

    if (server.supervised_mode == SUPERVISED_SYSTEMD) {
        redisCommunicateSystemd("STATUS=MASTER <-> REPLICA sync: Finished with success. Ready to accept connections in read-write mode.\n");
    }

    /* 立即发送初始 ACK，使此副本进入在线状态。 */
    if (usemark) replicationSendAck();

    /* 同步完成后，重启 AOF 子系统。这将触发一次
     * AOF 重写，完成后将开始向新文件追加数据。 */
    if (server.aof_enabled) restartAOFAfterSYNC();
    return;

error:
    cancelReplicationHandshake(1);
    return;
}

char *receiveSynchronousResponse(connection *conn) {
    char buf[256];
    /* 从服务器读取回复。 */
    if (connSyncReadLine(conn,buf,sizeof(buf),server.repl_syncio_timeout*1000) == -1)
    {
        serverLog(LL_WARNING, "Failed to read response from the server: %s", connGetLastError(conn));
        return NULL;
    }
    server.repl_transfer_lastio = server.unixtime;
    return sdsnew(buf);
}

/* 将预先格式化的 multi-bulk 命令发送到连接。 */
char* sendCommandRaw(connection *conn, sds cmd) {
    if (connSyncWrite(conn,cmd,sdslen(cmd),server.repl_syncio_timeout*1000) == -1) {
        return sdscatprintf(sdsempty(),"-Writing to master: %s",
                connGetLastError(conn));
    }
    return NULL;
}

/* 组装 multi-bulk 命令并发送到连接。
 * 用于在开始复制之前向主节点发送 AUTH 和 REPLCONF 命令。
 *
 * 接受一个 char* 参数列表，以 NULL 参数结尾。
 *
 * 返回一个 sds 字符串表示操作结果。
 * 出错时第一个字节为 "-"。
 */
char *sendCommand(connection *conn, ...) {
    va_list ap;
    sds cmd = sdsempty();
    sds cmdargs = sdsempty();
    size_t argslen = 0;
    char *arg;

    /* 创建要发送给主节点的命令，使用 Redis 二进制协议
     * 以确保发送正确的参数。
     * 此函数对所有二进制数据并不安全。 */
    va_start(ap,conn);
    while(1) {
        arg = va_arg(ap, char*);
        if (arg == NULL) break;
        cmdargs = sdscatprintf(cmdargs,"$%zu\r\n%s\r\n",strlen(arg),arg);
        argslen++;
    }

    cmd = sdscatprintf(cmd,"*%zu\r\n",argslen);
    cmd = sdscatsds(cmd,cmdargs);
    sdsfree(cmdargs);

    va_end(ap);
    char* err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if(err)
        return err;
    return NULL;
}

/* 组装 multi-bulk 命令并发送到连接。
 * 用于在开始复制之前向主节点发送 AUTH 和 REPLCONF 命令。
 *
 * argv_lens 是可选的，为 NULL 时使用 strlen。
 *
 * 返回一个 sds 字符串表示操作结果。
 * 出错时第一个字节为 "-"。
 */
char *sendCommandArgv(connection *conn, int argc, char **argv, size_t *argv_lens) {
    sds cmd = sdsempty();
    char *arg;
    int i;

    /* 创建要发送给主节点的命令。 */
    cmd = sdscatfmt(cmd,"*%i\r\n",argc);
    for (i=0; i<argc; i++) {
        int len;
        arg = argv[i];
        len = argv_lens ? argv_lens[i] : strlen(arg);
        cmd = sdscatfmt(cmd,"$%i\r\n",len);
        cmd = sdscatlen(cmd,arg,len);
        cmd = sdscatlen(cmd,"\r\n",2);
    }
    char* err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if (err)
        return err;
    return NULL;
}

/* 尝试与主节点进行部分重同步（如果我们即将重新连接）。
 * 如果没有缓存的主节点结构，至少尝试发送 "PSYNC ? -1" 命令，
 * 以通过 PSYNC 命令触发完全重同步，从而获取主节点的 replid
 * 和全局复制偏移量。
 *
 * 此函数设计为从 syncWithMaster() 调用，因此做了以下假设：
 *
 * 1) 调用者传递给函数一个已连接的套接字 "fd"。
 * 2) 此函数不会关闭文件描述符 "fd"。但如果部分重同步成功，
 *    函数会将 'fd' 复用为 server.master 客户端结构的
 *    文件描述符。
 *
 * 函数分为两半：如果 read_reply 为 0，函数将 PSYNC 命令
 * 写入套接字，然后需要以 read_reply 设为 1 再次调用，
 * 以读取命令的回复。这样设计是为了支持非阻塞操作——
 * 先写入，返回事件循环，等有数据时再读取。
 *
 * 当 read_reply 为 0 时，函数在写入出错时返回 PSYNC_WRITE_ERROR，
 * 否则返回 PSYNC_WAIT_REPLY 表示需要再次调用（read_reply=1）。
 * 即使 read_reply 已设为 1，函数也可能再次返回 PSYNC_WAIT_REPLY，
 * 表示数据不足以完成读取。此时应重新进入事件循环等待。
 *
 * 函数返回值：
 *
 * PSYNC_CONTINUE: PSYNC 命令成功，可以继续。
 * PSYNC_FULLRESYNC: PSYNC 受支持但需要完全重同步。
 *                   此时会保存主节点 replid 和全局复制偏移量。
 * PSYNC_NOT_SUPPORTED: 服务器完全不理解 PSYNC，
 *                      调用者应回退到 SYNC。
 * PSYNC_WRITE_ERROR: 将命令写入套接字时出错。
 * PSYNC_WAIT_REPLY: 以 read_reply 设为 1 再次调用。
 * PSYNC_TRY_LATER: 主节点当前处于瞬态错误状态。
 *
 * 重要副作用：
 *
 * 1) 函数调用的副作用之一是移除 "fd" 上的可读事件处理器，
 *    除非返回值为 PSYNC_WAIT_REPLY。
 * 2) server.master_initial_offset 会根据主节点回复
 *    设置为正确的值。该值将用于填充 server.master 结构的
 *    复制偏移量。
 */

#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5
int slaveTryPartialResynchronization(connection *conn, int read_reply) {
    char *psync_replid;
    char psync_offset[32];
    sds reply;

    /* 写入部分 */
    if (!read_reply) {
        /* 先将 master_initial_offset 设为 -1，以标记当前主节点
         * 的 replid 和偏移量无效。稍后如果能通过 PSYNC 命令
         * 执行完全重同步，会将偏移量设置为正确的值，
         * 该信息将传播到表示主节点的 client 结构中。 */
        server.master_initial_offset = -1;

        if (server.cached_master) {
            psync_replid = server.cached_master->replid;
            snprintf(psync_offset,sizeof(psync_offset),"%lld", server.cached_master->reploff+1);
            serverLog(LL_NOTICE,"Trying a partial resynchronization (request %s:%s).", psync_replid, psync_offset);
        } else {
            serverLog(LL_NOTICE,"Partial resynchronization not possible (no cached master)");
            psync_replid = "?";
            memcpy(psync_offset,"-1",3);
        }

        /* 发送 PSYNC 命令；如果这是一个正在进行故障转移
         * 的主节点，则向副本发送 FAILOVER 参数，
         * 使其成为主节点。 */
        if (server.failover_state == FAILOVER_IN_PROGRESS) {
            reply = sendCommand(conn,"PSYNC",psync_replid,psync_offset,"FAILOVER",NULL);
        } else {
            reply = sendCommand(conn,"PSYNC",psync_replid,psync_offset,NULL);
        }

        if (reply != NULL) {
            serverLog(LL_WARNING,"Unable to send PSYNC to master: %s",reply);
            sdsfree(reply);
            connSetReadHandler(conn, NULL);
            return PSYNC_WRITE_ERROR;
        }
        return PSYNC_WAIT_REPLY;
    }

    /* 读取部分 */
    reply = receiveSynchronousResponse(conn);
    /* 主节点未回复 PSYNC */
    if (reply == NULL) {
        connSetReadHandler(conn, NULL);
        serverLog(LL_WARNING, "Master did not reply to PSYNC, will try later");
        return PSYNC_TRY_LATER;
    }

    if (sdslen(reply) == 0) {
        /* 主节点在收到 PSYNC 命令后、回复之前
         * 可能会发送空行，仅用于保持连接活跃。 */
        sdsfree(reply);
        return PSYNC_WAIT_REPLY;
    }

    connSetReadHandler(conn, NULL);

    if (!strncmp(reply,"+FULLRESYNC",11)) {
        char *replid = NULL, *offset = NULL;

        /* FULL RESYNC，解析回复以提取 replid
         * 和复制偏移量。 */
        replid = strchr(reply,' ');
        if (replid) {
            replid++;
            offset = strchr(replid,' ');
            if (offset) offset++;
        }
        if (!replid || !offset || (offset-replid-1) != CONFIG_RUN_ID_SIZE) {
            serverLog(LL_WARNING,
                "Master replied with wrong +FULLRESYNC syntax.");
            /* 这是一个意外情况：+FULLRESYNC 回复表示
             * 主节点支持 PSYNC，但回复格式似乎有误。
             * 为安全起见，我们将主节点 replid 置零，
             * 确保后续 PSYNC 调用会失败。 */
            memset(server.master_replid,0,CONFIG_RUN_ID_SIZE+1);
        } else {
            memcpy(server.master_replid, replid, offset-replid-1);
            server.master_replid[CONFIG_RUN_ID_SIZE] = '\0';
            server.master_initial_offset = strtoll(offset,NULL,10);
            serverLog(LL_NOTICE,"Full resync from master: %s:%lld",
                server.master_replid,
                server.master_initial_offset);
        }
        sdsfree(reply);
        return PSYNC_FULLRESYNC;
    }

    if (!strncmp(reply,"+CONTINUE",9)) {
        /* 部分重同步已被接受。 */
        serverLog(LL_NOTICE,
            "Successful partial resynchronization with master.");

        /* 检查主节点通告的新 replication ID。如果已变更，
         * 需要将新 ID 设为主 ID，并将旧主节点 ID 设为
         * 副 ID（截止到当前偏移量），以便我们的子从节点
         * 在断开连接后能与我们进行 PSYNC。 */
        char *start = reply+10;
        char *end = reply+9;
        while(end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
        if (end-start == CONFIG_RUN_ID_SIZE) {
            char new[CONFIG_RUN_ID_SIZE+1];
            memcpy(new,start,CONFIG_RUN_ID_SIZE);
            new[CONFIG_RUN_ID_SIZE] = '\0';

            if (strcmp(new,server.cached_master->replid)) {
                /* 主节点 ID 已变更。 */
                serverLog(LL_NOTICE,"Master replication ID changed to %s",new);

                /* 将旧 ID 设为我们的 ID2，截止到当前偏移量+1。 */
                memcpy(server.replid2,server.cached_master->replid,
                    sizeof(server.replid2));
                server.second_replid_offset = server.master_repl_offset+1;

                /* 将缓存的主节点 ID 和我们自身的主 ID
                 * 更新为新 ID。 */
                memcpy(server.replid,new,sizeof(server.replid));
                memcpy(server.cached_master->replid,new,sizeof(server.replid));

                /* 断开所有子从节点的连接：它们需要被通知。 */
                disconnectSlaves();
            }
        }

        /* 设置复制以继续工作。 */
        sdsfree(reply);
        replicationResurrectCachedMaster(conn);

        /* 如果此实例被重启，且我们从持久化文件中读取了
         * PSYNC 的元数据，那么复制积压缓冲区可能尚未
         * 初始化，需要创建它。 */
        if (server.repl_backlog == NULL) createReplicationBacklog();
        return PSYNC_CONTINUE;
    }

    /* 执行到这里，说明收到了错误（master 不支持 PSYNC，或者处于
     * 特殊状态无法处理请求），或者收到了 master 的意外回复。
     *
     * 对于无法理解的错误返回 PSYNC_NOT_SUPPORTED，
     * 如果认为是瞬态错误则返回 PSYNC_TRY_LATER。 */

    if (!strncmp(reply,"-NOMASTERLINK",13) ||
        !strncmp(reply,"-LOADING",8))
    {
        serverLog(LL_NOTICE,
            "Master is currently unable to PSYNC "
            "but should be in the future: %s", reply);
        sdsfree(reply);
        return PSYNC_TRY_LATER;
    }

    if (strncmp(reply,"-ERR",4)) {
        /* 如果不是 -ERR 错误，记录这个意外事件。 */
        serverLog(LL_WARNING,
            "Unexpected reply to PSYNC from master: %s", reply);
    } else {
        serverLog(LL_NOTICE,
            "Master does not support PSYNC or is in "
            "error state (reply: %s)", reply);
    }
    sdsfree(reply);
    return PSYNC_NOT_SUPPORTED;
}

/* 当非阻塞连接成功建立到 master 的连接时触发此处理器。 */
void syncWithMaster(connection *conn) {
    char tmpfile[256], *err = NULL;
    int dfd = -1, maxtries = 5;
    int psync_result;

    /* 如果此事件在用户通过 SLAVEOF NO ONE 将实例
     * 转为主节点之后被触发，我们必须立即返回。 */
    if (server.repl_state == REPL_STATE_NONE) {
        connClose(conn);
        return;
    }

    /* 检查 socket 中的错误：非阻塞 connect() 之后，
     * 可能发现 socket 处于错误状态。 */
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING,"Error condition on socket for SYNC: %s",
                connGetLastError(conn));
        goto error;
    }

    /* 发送 PING 以检查主节点能否无错误地回复。 */
    if (server.repl_state == REPL_STATE_CONNECTING) {
        serverLog(LL_NOTICE,"Non blocking connect for SYNC fired the event.");
        /* 删除可写事件，使可读事件保持注册状态，
         * 从而等待 PONG 回复。 */
        connSetReadHandler(conn, syncWithMaster);
        connSetWriteHandler(conn, NULL);
        server.repl_state = REPL_STATE_RECEIVE_PING_REPLY;
        /* 发送 PING，完全不检查错误，
         * 超时机制会负责处理。 */
        err = sendCommand(conn,"PING",NULL);
        if (err) goto write_error;
        return;
    }

    /* 接收 PONG 命令。 */
    if (server.repl_state == REPL_STATE_RECEIVE_PING_REPLY) {
        err = receiveSynchronousResponse(conn);

        /* 主节点未回复 */
        if (err == NULL) goto no_response_error;

        /* 我们只接受两种有效回复：肯定的 +PONG 回复
         * （只检查开头的 "+"）或认证错误。
         * 注意旧版 Redis 回复 "operation not permitted"
         * 而非使用正确的错误码，因此两种情况都需要测试。 */
        if (err[0] != '+' &&
            strncmp(err,"-NOAUTH",7) != 0 &&
            strncmp(err,"-NOPERM",7) != 0 &&
            strncmp(err,"-ERR operation not permitted",28) != 0)
        {
            serverLog(LL_WARNING,"Error reply to PING from master: '%s'",err);
            sdsfree(err);
            goto error;
        } else {
            serverLog(LL_NOTICE,
                "Master replied to PING, replication can continue...");
        }
        sdsfree(err);
        err = NULL;
        server.repl_state = REPL_STATE_SEND_HANDSHAKE;
    }

    if (server.repl_state == REPL_STATE_SEND_HANDSHAKE) {
        /* 如有需要，向主节点发送 AUTH 认证。 */
        if (server.masterauth) {
            char *args[3] = {"AUTH",NULL,NULL};
            size_t lens[3] = {4,0,0};
            int argc = 1;
            if (server.masteruser) {
                args[argc] = server.masteruser;
                lens[argc] = strlen(server.masteruser);
                argc++;
            }
            args[argc] = server.masterauth;
            lens[argc] = sdslen(server.masterauth);
            argc++;
            err = sendCommandArgv(conn, argc, args, lens);
            if (err) goto write_error;
        }

        /* 设置从节点端口，以便主节点的 INFO 命令
         * 能正确列出从节点的监听端口。 */
        {
            int port;
            if (server.slave_announce_port)
                port = server.slave_announce_port;
            else if (server.tls_replication && server.tls_port)
                port = server.tls_port;
            else
                port = server.port;
            sds portstr = sdsfromlonglong(port);
            err = sendCommand(conn,"REPLCONF",
                    "listening-port",portstr, NULL);
            sdsfree(portstr);
            if (err) goto write_error;
        }

        /* 设置从节点 IP，以便主节点的 INFO 命令
         * 在端口转发或 NAT 场景下能正确列出从节点地址。
         * 如果未设置 slave-announce-ip 则跳过 REPLCONF ip-address。 */
        if (server.slave_announce_ip) {
            err = sendCommand(conn,"REPLCONF",
                    "ip-address",server.slave_announce_ip, NULL);
            if (err) goto write_error;
        }

        /* 通知主节点本从节点支持的能力。
         *
         * EOF: 支持 EOF 模式的 RDB 传输，用于无盘复制。
         * PSYNC2: 支持 PSYNC v2，能理解 +CONTINUE <new repl ID>。
         *
         * 主节点会忽略其不理解的能力标识。 */
        err = sendCommand(conn,"REPLCONF",
                "capa","eof","capa","psync2",NULL);
        if (err) goto write_error;

        server.repl_state = REPL_STATE_RECEIVE_AUTH_REPLY;
        return;
    }

    if (server.repl_state == REPL_STATE_RECEIVE_AUTH_REPLY && !server.masterauth)
        server.repl_state = REPL_STATE_RECEIVE_PORT_REPLY;

    /* 接收 AUTH 回复。 */
    if (server.repl_state == REPL_STATE_RECEIVE_AUTH_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        if (err[0] == '-') {
            serverLog(LL_WARNING,"Unable to AUTH to MASTER: %s",err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
        err = NULL;
        server.repl_state = REPL_STATE_RECEIVE_PORT_REPLY;
        return;
    }

    /* 接收 REPLCONF listening-port 回复。 */
    if (server.repl_state == REPL_STATE_RECEIVE_PORT_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        /* 忽略错误（如有），并非所有 Redis 版本
         * 都支持 REPLCONF listening-port。 */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                "REPLCONF listening-port: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_IP_REPLY;
        return;
    }

    if (server.repl_state == REPL_STATE_RECEIVE_IP_REPLY && !server.slave_announce_ip)
        server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;

    /* 接收 REPLCONF ip-address 回复。 */
    if (server.repl_state == REPL_STATE_RECEIVE_IP_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        /* 忽略错误（如有），并非所有 Redis 版本
         * 都支持 REPLCONF ip-address。 */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                "REPLCONF ip-address: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;
        return;
    }

    /* 接收 CAPA 回复。 */
    if (server.repl_state == REPL_STATE_RECEIVE_CAPA_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        /* 忽略错误（如有），并非所有 Redis 版本
         * 都支持 REPLCONF capa。 */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                  "REPLCONF capa: %s", err);
        }
        sdsfree(err);
        err = NULL;
        server.repl_state = REPL_STATE_SEND_PSYNC;
    }

    /* 尝试部分重同步。如果没有缓存的主节点信息，
     * slaveTryPartialResynchronization() 至少会尝试使用 PSYNC
     * 发起全量重同步，以获取主节点的 replid 和全局偏移量，
     * 从而在下次重连时尝试部分重同步。 */
    if (server.repl_state == REPL_STATE_SEND_PSYNC) {
        if (slaveTryPartialResynchronization(conn,0) == PSYNC_WRITE_ERROR) {
            err = sdsnew("Write error sending the PSYNC command.");
            abortFailover("Write error to failover target");
            goto write_error;
        }
        server.repl_state = REPL_STATE_RECEIVE_PSYNC_REPLY;
        return;
    }

    /* 如果执行到这里，应该处于 REPL_STATE_RECEIVE_PSYNC_REPLY 状态。 */
    if (server.repl_state != REPL_STATE_RECEIVE_PSYNC_REPLY) {
        serverLog(LL_WARNING,"syncWithMaster(): state machine error, "
                             "state should be RECEIVE_PSYNC but is %d",
                             server.repl_state);
        goto error;
    }

    psync_result = slaveTryPartialResynchronization(conn,1);
    if (psync_result == PSYNC_WAIT_REPLY) return; /* 稍后重试... */

    /* 检查计划故障转移的状态。我们期望得到 PSYNC_CONTINUE，
     * 但在边界情况下发生全量重同步在技术上也没有问题。 */
    if (server.failover_state == FAILOVER_IN_PROGRESS) {
        if (psync_result == PSYNC_CONTINUE || psync_result == PSYNC_FULLRESYNC) {
            clearFailoverState();
        } else {
            abortFailover("Failover target rejected psync request");
            return;
        }
    }

    /* 如果主节点处于临时错误状态，稍后应尝试从头开始 PSYNC，
     * 因此进入错误处理路径。这种情况发生在服务器正在加载
     * 数据集或未连接到其主节点等场景。 */
    if (psync_result == PSYNC_TRY_LATER) goto error;

    /* 注意：如果 PSYNC 没有返回 WAIT_REPLY，它会负责
     * 从文件描述符上卸载读事件处理器。 */

    if (psync_result == PSYNC_CONTINUE) {
        serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Master accepted a Partial Resynchronization.");
        if (server.supervised_mode == SUPERVISED_SYSTEMD) {
            redisCommunicateSystemd("STATUS=MASTER <-> REPLICA sync: Partial Resynchronization accepted. Ready to accept connections in read-write mode.\n");
        }
        return;
    }

    /* 如果需要，回退到 SYNC。否则 psync_result == PSYNC_FULLRESYNC，
     * 且 server.master_replid 和 master_initial_offset 已经填充。 */
    if (psync_result == PSYNC_NOT_SUPPORTED) {
        serverLog(LL_NOTICE,"Retrying with SYNC...");
        if (connSyncWrite(conn,"SYNC\r\n",6,server.repl_syncio_timeout*1000) == -1) {
            serverLog(LL_WARNING,"I/O error writing to MASTER: %s",
                connGetLastError(conn));
            goto error;
        }
    }

    /* 为批量传输准备合适的临时文件 */
    if (!useDisklessLoad()) {
        while(maxtries--) {
            snprintf(tmpfile,256,
                "temp-%d.%ld.rdb",(int)server.unixtime,(long int)getpid());
            dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
            if (dfd != -1) break;
            sleep(1);
        }
        if (dfd == -1) {
            serverLog(LL_WARNING,"Opening the temp file needed for MASTER <-> REPLICA synchronization: %s",strerror(errno));
            goto error;
        }
        server.repl_transfer_tmpfile = zstrdup(tmpfile);
        server.repl_transfer_fd = dfd;
    }

    /* 设置批量文件的非阻塞下载。 */
    if (connSetReadHandler(conn, readSyncBulkPayload)
            == C_ERR)
    {
        char conninfo[CONN_INFO_LEN];
        serverLog(LL_WARNING,
            "Can't create readable event for SYNC: %s (%s)",
            strerror(errno), connGetInfo(conn, conninfo, sizeof(conninfo)));
        goto error;
    }

    server.repl_state = REPL_STATE_TRANSFER;
    server.repl_transfer_size = -1;
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_lastio = server.unixtime;
    return;

no_response_error: /* 处理主节点无响应时 receiveSynchronousResponse() 的错误 */
    serverLog(LL_WARNING, "Master did not respond to command during SYNC handshake");
    /* 继续执行常规错误处理 */

error:
    if (dfd != -1) close(dfd);
    connClose(conn);
    server.repl_transfer_s = NULL;
    if (server.repl_transfer_fd != -1)
        close(server.repl_transfer_fd);
    if (server.repl_transfer_tmpfile)
        zfree(server.repl_transfer_tmpfile);
    server.repl_transfer_tmpfile = NULL;
    server.repl_transfer_fd = -1;
    server.repl_state = REPL_STATE_CONNECT;
    return;

write_error: /* 处理 sendCommand() 错误。 */
    serverLog(LL_WARNING,"Sending command to master in replication handshake: %s", err);
    sdsfree(err);
    goto error;
}

int connectWithMaster(void) {
    server.repl_transfer_s = connCreate(connTypeOfReplication());
    if (connConnect(server.repl_transfer_s, server.masterhost, server.masterport,
                server.bind_source_addr, syncWithMaster) == C_ERR) {
        serverLog(LL_WARNING,"Unable to connect to MASTER: %s",
                connGetLastError(server.repl_transfer_s));
        connClose(server.repl_transfer_s);
        server.repl_transfer_s = NULL;
        return C_ERR;
    }


    server.repl_transfer_lastio = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTING;
    serverLog(LL_NOTICE,"MASTER <-> REPLICA sync started");
    return C_OK;
}

/* 当非阻塞连接正在进行时，可以调用此函数来撤销它。
 * 不要直接调用此函数，请使用 cancelReplicationHandshake() 代替。 */
void undoConnectWithMaster(void) {
    connClose(server.repl_transfer_s);
    server.repl_transfer_s = NULL;
}

/* 中止与主节点 SYNC 过程中的批量数据集异步下载。
 * 不要直接调用此函数，请使用 cancelReplicationHandshake() 代替。 */
void replicationAbortSyncTransfer(void) {
    serverAssert(server.repl_state == REPL_STATE_TRANSFER);
    undoConnectWithMaster();
    if (server.repl_transfer_fd!=-1) {
        close(server.repl_transfer_fd);
        bg_unlink(server.repl_transfer_tmpfile);
        zfree(server.repl_transfer_tmpfile);
        server.repl_transfer_tmpfile = NULL;
        server.repl_transfer_fd = -1;
    }
}

/* 如果正在进行非阻塞复制尝试，此函数通过取消非阻塞连接尝试
 * 或初始批量传输来中止它。
 *
 * 如果复制握手正在进行中，返回 1，并将复制状态
 * (server.repl_state) 设置为 REPL_STATE_CONNECT。
 *
 * 否则返回 0，不执行任何操作。 */
int cancelReplicationHandshake(int reconnect) {
    if (server.repl_state == REPL_STATE_TRANSFER) {
        replicationAbortSyncTransfer();
        server.repl_state = REPL_STATE_CONNECT;
    } else if (server.repl_state == REPL_STATE_CONNECTING ||
               slaveIsInHandshakeState())
    {
        undoConnectWithMaster();
        server.repl_state = REPL_STATE_CONNECT;
    } else {
        return 0;
    }

    if (!reconnect)
        return 1;

    /* 尝试立即重新连接，不等待 replicationCron，
     * 这是 "diskless loading short read" 测试所需要的。 */
    serverLog(LL_NOTICE,"Reconnecting to MASTER %s:%d after failure",
        server.masterhost, server.masterport);
    connectWithMaster();

    return 1;
}

/* 设置复制指向指定的主节点地址和端口。 */
void replicationSetMaster(char *ip, int port) {
    int was_master = server.masterhost == NULL;

    sdsfree(server.masterhost);
    server.masterhost = NULL;
    if (server.master) {
        freeClient(server.master);
    }
    disconnectAllBlockedClients(); /* 主节点中的阻塞客户端，现在变为从节点。 */

    /* 在调用 freeClient 之后才设置 masterhost，因为 freeClient 会调用
     * replicationHandleMasterDisconnection，该函数可能直接在调用
     * 内部触发重新连接。 */
    server.masterhost = sdsnew(ip);
    server.masterport = port;

    /* 更新 oom_score_adj */
    setOOMScoreAdj(-1);

    /* 这里我们不断开与副本的连接，因为它们可能仍能与我们进行
     * 部分重同步。我们会在与新主节点部分重同步时更换 replid，
     * 或者在与新主节点全量同步完成 RDB 传输并准备加载数据库时，
     * 才断开与副本的连接并强制它们重新同步。 */

    cancelReplicationHandshake(0);
    /* 在销毁主节点状态之前，使用我们自身的参数创建缓存的主节点，
     * 以便稍后与新主节点进行 PSYNC。 */
    if (was_master) {
        replicationDiscardCachedMaster();
        replicationCacheMasterUsingMyself();
    }

    /* 触发角色变更模块事件。 */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_REPLICA,
                          NULL);

    /* 触发主链接模块事件。 */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    server.repl_state = REPL_STATE_CONNECT;
    serverLog(LL_NOTICE,"Connecting to MASTER %s:%d",
        server.masterhost, server.masterport);
    connectWithMaster();
}

/* 取消复制，将当前实例设为主节点。 */
void replicationUnsetMaster(void) {
    if (server.masterhost == NULL) return; /* 无需操作。 */

    /* 触发主链接模块事件。 */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    /* 先清除 masterhost，因为 freeClient 会调用
     * replicationHandleMasterDisconnection，该函数可能尝试重连。 */
    sdsfree(server.masterhost);
    server.masterhost = NULL;
    if (server.master) freeClient(server.master);
    replicationDiscardCachedMaster();
    cancelReplicationHandshake(0);
    /* 当从节点升为主节点时，当前的 replication ID
     * （从同步时继承自主节点）将作为辅助 ID 保留至当前偏移量，
     * 同时创建新的 replication ID 以继续新的复制历史。 */
    shiftReplicationId();
    /* 需要断开所有从节点：我们需要通知从节点
     * replication ID 的变更（参见 shiftReplicationId() 调用）。
     * 但从节点可以部分重同步，因此重连会非常快速。 */
    disconnectSlaves();
    server.repl_state = REPL_STATE_NONE;

    /* 需要确保新主节点以 SELECT 语句开始复制流。
     * 全量重同步后会强制执行此操作，但在 PSYNC version 2 中，
     * 主节点切换后无需全量重同步。 */
    server.slaveseldb = -1;

    /* 更新 oom_score_adj */
    setOOMScoreAdj(-1);

    /* 从节点升为主节点后，将"无从节点起始时间"
     * （用于计算复制积压缓冲区的存活时间）设为当前时刻。
     * 否则在故障转移后，如果从节点未立即连接，
     * 积压缓冲区将被释放。 */
    server.repl_no_slaves_since = server.unixtime;
    
    /* 重置宕机时间，为再次变为从节点做好准备。 */
    server.repl_down_since = 0;

    /* 触发角色变更模块事件。 */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_MASTER,
                          NULL);

    /* 如果在作为从节点进行同步时关闭了 AOF 子系统，
     * 重新启动它。 */
    if (server.aof_enabled && server.aof_state == AOF_OFF) restartAOFAfterSYNC();
}

/* 当从节点与主节点意外断开连接时调用此函数。 */
void replicationHandleMasterDisconnection(void) {
    /* 触发主链接模块事件。 */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    server.master = NULL;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_down_since = server.unixtime;
    /* 与主节点断开连接后，暂不断开从节点，
     * 后续可能需要与主节点进行 PSYNC。
     * 只有在必须全量重同步时才断开从节点。 */

    /* 尝试立即重连，而不是等待 replicationCron，
     * 等待 1 秒可能会导致积压缓冲区被回收。 */
    if (server.masterhost) {
        serverLog(LL_NOTICE,"Reconnecting to MASTER %s:%d",
            server.masterhost, server.masterport);
        connectWithMaster();
    }
}

void replicaofCommand(client *c) {
    /* 在集群模式下不允许使用 SLAVEOF，
     * 因为复制会自动使用主节点的当前地址进行配置。 */
    if (server.cluster_enabled) {
        addReplyError(c,"REPLICAOF not allowed in cluster mode.");
        return;
    }

    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c,"REPLICAOF not allowed while failing over.");
        return;
    }

    /* 特殊的 host/port 组合 "NO" "ONE" 将实例转为主节点，
     * 否则设置新的主节点地址。 */
    if (!strcasecmp(c->argv[1]->ptr,"no") &&
        !strcasecmp(c->argv[2]->ptr,"one")) {
        if (server.masterhost) {
            replicationUnsetMaster();
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,"MASTER MODE enabled (user request from '%s')",
                client);
            sdsfree(client);
        }
    } else {
        long port;

        if (c->flags & CLIENT_SLAVE)
        {
            /* 如果客户端已经是从节点，则不能执行此命令，
             * 因为该操作会刷新所有从节点（包括此客户端）。 */
            addReplyError(c, "Command is not valid when client is a replica.");
            return;
        }

        if (getRangeLongFromObjectOrReply(c, c->argv[2], 0, 65535, &port,
                                          "Invalid master port") != C_OK)
            return;

        /* 检查是否已连接到指定的主节点 */
        if (server.masterhost && !strcasecmp(server.masterhost,c->argv[1]->ptr)
            && server.masterport == port) {
            serverLog(LL_NOTICE,"REPLICAOF would result into synchronization "
                                "with the master we are already connected "
                                "with. No operation performed.");
            addReplySds(c,sdsnew("+OK Already connected to specified "
                                 "master\r\n"));
            return;
        }
        /* 无先前的主节点，或用户指定了不同的主节点，继续执行。 */
        replicationSetMaster(c->argv[1]->ptr, port);
        sds client = catClientInfoString(sdsempty(),c);
        serverLog(LL_NOTICE,"REPLICAOF %s:%d enabled (user request from '%s')",
            server.masterhost, server.masterport, client);
        sdsfree(client);
    }
    addReply(c,shared.ok);
}

/* ROLE 命令：以易于处理的格式提供实例角色信息
 * （主节点或从节点）以及与复制相关的附加信息。 */
void roleCommand(client *c) {
    if (server.sentinel_mode) {
        sentinelRoleCommand(c);
        return;
    }

    if (server.masterhost == NULL) {
        listIter li;
        listNode *ln;
        void *mbcount;
        int slaves = 0;

        addReplyArrayLen(c,3);
        addReplyBulkCBuffer(c,"master",6);
        addReplyLongLong(c,server.master_repl_offset);
        mbcount = addReplyDeferredLen(c);
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;
            char ip[NET_IP_STR_LEN], *slaveaddr = slave->slave_addr;

            if (!slaveaddr) {
                if (connAddrPeerName(slave->conn,ip,sizeof(ip),NULL) == -1)
                    continue;
                slaveaddr = ip;
            }
            if (slave->replstate != SLAVE_STATE_ONLINE) continue;
            addReplyArrayLen(c,3);
            addReplyBulkCString(c,slaveaddr);
            addReplyBulkLongLong(c,slave->slave_listening_port);
            addReplyBulkLongLong(c,slave->repl_ack_off);
            slaves++;
        }
        setDeferredArrayLen(c,mbcount,slaves);
    } else {
        char *slavestate = NULL;

        addReplyArrayLen(c,5);
        addReplyBulkCBuffer(c,"slave",5);
        addReplyBulkCString(c,server.masterhost);
        addReplyLongLong(c,server.masterport);
        if (slaveIsInHandshakeState()) {
            slavestate = "handshake";
        } else {
            switch(server.repl_state) {
            case REPL_STATE_NONE: slavestate = "none"; break;
            case REPL_STATE_CONNECT: slavestate = "connect"; break;
            case REPL_STATE_CONNECTING: slavestate = "connecting"; break;
            case REPL_STATE_TRANSFER: slavestate = "sync"; break;
            case REPL_STATE_CONNECTED: slavestate = "connected"; break;
            default: slavestate = "unknown"; break;
            }
        }
        addReplyBulkCString(c,slavestate);
        addReplyLongLong(c,server.master ? server.master->reploff : -1);
    }
}

/* 向主节点发送 REPLCONF ACK 命令，通知当前已处理的偏移量。
 * 如果未与主节点连接，此命令无效。 */
void replicationSendAck(void) {
    client *c = server.master;

    if (c != NULL) {
        int send_fack = server.fsynced_reploff != -1;
        c->flags |= CLIENT_MASTER_FORCE_REPLY;
        addReplyArrayLen(c,send_fack ? 5 : 3);
        addReplyBulkCString(c,"REPLCONF");
        addReplyBulkCString(c,"ACK");
        addReplyBulkLongLong(c,c->reploff);
        if (send_fack) {
            addReplyBulkCString(c,"FACK");
            addReplyBulkLongLong(c,server.fsynced_reploff);
        }
        c->flags &= ~CLIENT_MASTER_FORCE_REPLY;
    }
}

/* ---------------------- PSYNC 主节点缓存 ----------------------------------- */

/* 为了实现部分重同步（PSYNC），需要在短暂断连后缓存
 * 主节点的客户端结构。
 * 缓存存储在 server.cached_master 中，并通过以下函数
 * 进行管理。 */

/* 此函数由 freeClient() 调用，用于缓存主节点客户端结构
 * 而非销毁它。freeClient() 在此函数返回后将立即返回，
 * 因此所有避免"已挂起"客户端出现问题的操作必须在此
 * 函数中完成。
 *
 * 处理缓存主节点的其他函数包括：
 *
 * replicationDiscardCachedMaster() —— 确保销毁该客户端，
 * 用于因某些原因不再需要使用缓存主节点的情况。
 *
 * replicationResurrectCachedMaster() —— 在成功完成 PSYNC
 * 握手后调用，用于重新激活缓存的主节点。
 */
void replicationCacheMaster(client *c) {
    serverAssert(server.master != NULL && server.cached_master == NULL);
    serverLog(LL_NOTICE,"Caching the disconnected master state.");

    /* 将客户端从服务器结构中解除链接。 */
    unlinkClient(c);

    /* 重置主节点客户端，使其准备好接受新命令：
     * 丢弃未处理的查询缓冲区和未处理的偏移量，
     * 包括待处理的事务、已填充的参数、
     * 以及待发送给主节点的输出。 */
    sdsclear(server.master->querybuf);
    server.master->qb_pos = 0;
    server.master->repl_applied = 0;
    server.master->read_reploff = server.master->reploff;
    if (c->flags & CLIENT_MULTI) discardTransaction(c);
    listEmpty(c->reply);
    c->sentlen = 0;
    c->reply_bytes = 0;
    c->bufpos = 0;
    resetClient(c);

    /* 保存主节点。server.master 稍后会被
     * replicationHandleMasterDisconnection() 设为 NULL。 */
    server.cached_master = server.master;

    /* 使对等节点 ID 缓存失效。 */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }
    /* 使套接字名称缓存失效。 */
    if (c->sockname) {
        sdsfree(c->sockname);
        c->sockname = NULL;
    }

    /* 缓存主节点代替了实际的 freeClient() 调用，
     * 因此需要确保调整复制状态。此函数还会将
     * server.master 设为 NULL。 */
    replicationHandleMasterDisconnection();
}

/* 当主节点转变为从节点时调用此函数，用于从头创建一个
 * 缓存的主节点客户端，使得在故障转移后能与被提升为
 * 新主节点的从节点进行 PSYNC。
 *
 * 假设本实例之前是新主节点的主实例，新主节点会接受其
 * 复制 ID；如果故障转移期间未丢失数据，还会接受当前
 * 偏移量。因此我们使用当前的复制 ID 和偏移量来
 * 合成一个缓存的主节点。 */
void replicationCacheMasterUsingMyself(void) {
    serverLog(LL_NOTICE,
        "Before turning into a replica, using my own master parameters "
        "to synthesize a cached master: I may be able to synchronize with "
        "the new master with just a partial transfer.");

    /* 此值将被 replicationCreateMasterClient() 用于填充
     * server.master->reploff 字段。稍后将创建的主节点设为
     * server.cached_master，这样副本在 PSYNC 时会使用
     * 该偏移量。 */
    server.master_initial_offset = server.master_repl_offset;

    /* 创建的主节点客户端可以设为任意 DBID，
     * 因为新主节点会以 SELECT 开始其复制流。 */
    replicationCreateMasterClient(NULL,-1);

    /* 使用我们自己的 ID 和偏移量。 */
    memcpy(server.master->replid, server.replid, sizeof(server.replid));

    /* 设为缓存的主节点。 */
    unlinkClient(server.master);
    server.cached_master = server.master;
    server.master = NULL;
}

/* 释放缓存的主节点，在重连时不再满足部分重同步
 * 条件时调用。 */
void replicationDiscardCachedMaster(void) {
    if (server.cached_master == NULL) return;

    serverLog(LL_NOTICE,"Discarding previously cached master state.");
    server.cached_master->flags &= ~CLIENT_MASTER;
    freeClient(server.cached_master);
    server.cached_master = NULL;
}

/* 将缓存的主节点恢复为当前主节点，使用传入的连接
 * 作为新主节点的套接字。
 *
 * 在成功建立部分重同步时调用此函数，这样接收到的
 * 数据流将从该主节点上次断开的位置继续。 */
void replicationResurrectCachedMaster(connection *conn) {
    server.master = server.cached_master;
    server.cached_master = NULL;
    server.master->conn = conn;
    connSetPrivateData(server.master->conn, server.master);
    server.master->flags &= ~(CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP);
    server.master->authenticated = 1;
    server.master->lastinteraction = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* 触发主链接模块事件。 */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* 重新添加到客户端列表中。 */
    linkClient(server.master);
    if (connSetReadHandler(server.master->conn, readQueryFromClient)) {
        serverLog(LL_WARNING,"Error resurrecting the cached master, impossible to add the readable handler: %s", strerror(errno));
        freeClientAsync(server.master); /* 尽快关闭。 */
    }

    /* 如果写缓冲区中有待发送的数据，还需要安装写事件处理器。 */
    if (clientHasPendingReplies(server.master)) {
        if (connSetWriteHandler(server.master->conn, sendReplyToClient)) {
            serverLog(LL_WARNING,"Error resurrecting the cached master, impossible to add the writable handler: %s", strerror(errno));
            freeClientAsync(server.master); /* 尽快关闭。 */
        }
    }
}

/* ---------------------- MIN-SLAVES-TO-WRITE（最少从节点写入要求）--------- */

/* 此函数统计延迟 <= min-slaves-max-lag 的从节点数量。
 * 如果启用了此选项，当没有足够数量的从节点满足
 * 指定延迟要求时，服务器将拒绝写入。 */
void refreshGoodSlavesCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    if (!server.repl_min_slaves_to_write ||
        !server.repl_min_slaves_max_lag) return;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        time_t lag = server.unixtime - slave->repl_ack_time;

        if (slave->replstate == SLAVE_STATE_ONLINE &&
            lag <= server.repl_min_slaves_max_lag) good++;
    }
    server.repl_good_slaves_count = good;
}

/* 如果健康副本状态正常则返回 true，否则返回 false */
int checkGoodReplicasStatus(void) {
    return server.masterhost || /* 非主节点状态也视为正常 */
           !server.repl_min_slaves_max_lag || /* 未配置 min-slaves-max-lag */
           !server.repl_min_slaves_to_write || /* 未配置 min-slaves-to-write */
           server.repl_good_slaves_count >= server.repl_min_slaves_to_write; /* 检查是否有足够的从节点 */
}

/* ----------------------- 同步复制 ------------------------------------------
 * Redis 同步复制设计可概括为以下几点：
 *
 * - Redis 主节点拥有全局复制偏移量，用于 PSYNC。
 * - 每次向从节点发送新命令时，主节点递增该偏移量。
 * - 从节点定期向主节点回传已处理的偏移量。
 *
 * 同步复制新增了 WAIT 命令，格式如下：
 *
 *   WAIT <num_replicas> <milliseconds_timeout>
 *
 * 当确认的副本数达到 num_replicas 或超时时，
 * 返回已处理该查询的副本数量。
 *
 * 该命令的实现方式如下：
 *
 * - 每次客户端执行命令后，记录发送给从节点后的
 *   复制偏移量。
 * - 调用 WAIT 时，要求从节点立即发送 ACK 确认。
 *   同时阻塞客户端（参见 blocked.c）。
 * - 当收到足够数量的指定偏移量 ACK 或超时后，
 *   WAIT 命令解除阻塞并将回复发送给客户端。
 */

/* 此函数仅设置一个标志，以便在 beforeSleep() 中向
 * 所有从节点广播 REPLCONF GETACK 命令。注意这种方式
 * 将同一事件循环迭代中所有等待同步复制的客户端
 * "分组"在一起，只发送一次 GETACK。 */
void replicationRequestAckFromSlaves(void) {
    server.get_ack_from_slaves = 1;
}

/* 返回已确认指定复制偏移量的从节点数量。 */
int replicationCountAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate != SLAVE_STATE_ONLINE) continue;
        if (slave->repl_ack_off >= offset) count++;
    }
    return count;
}

/* 返回已确认指定复制偏移量已完成 AOF fsync 的副本数量。 */
int replicationCountAOFAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate != SLAVE_STATE_ONLINE) continue;
        if (slave->repl_aof_off >= offset) count++;
    }
    return count;
}

/* WAIT 命令：等待 N 个副本确认已处理我们最新的写命令
 *（以及之前所有的写命令）。 */
void waitCommand(client *c) {
    mstime_t timeout;
    long numreplicas, ackreplicas;
    long long offset = c->woff;

    if (server.masterhost) {
        addReplyError(c,"WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
        return;
    }

    /* 参数解析。 */
    if (getLongFromObjectOrReply(c,c->argv[1],&numreplicas,NULL) != C_OK)
        return;
    if (getTimeoutFromObjectOrReply(c,c->argv[2],&timeout,UNIT_MILLISECONDS)
        != C_OK) return;

    /* 先尝试不阻塞地检查是否已满足条件。 */
    ackreplicas = replicationCountAcksByOffset(c->woff);
    if (ackreplicas >= numreplicas || c->flags & CLIENT_DENY_BLOCKING) {
        addReplyLongLong(c,ackreplicas);
        return;
    }

    /* 否则阻塞客户端，将其放入等待副本确认的客户端列表中。 */
    blockForReplication(c,timeout,offset,numreplicas);

    /* 确保服务器在返回事件循环之前，向所有副本发送 ACK 请求。 */
    replicationRequestAckFromSlaves();
}

/* WAITAOF 命令：等待 N 个副本和/或本地主节点确认
 * 我们最新的写命令已同步到磁盘。 */
void waitaofCommand(client *c) {
    mstime_t timeout;
    long numreplicas, numlocal, ackreplicas, acklocal;

    /* 参数解析。 */
    if (getRangeLongFromObjectOrReply(c,c->argv[1],0,1,&numlocal,NULL) != C_OK)
        return;
    if (getPositiveLongFromObjectOrReply(c,c->argv[2],&numreplicas,NULL) != C_OK)
        return;
    if (getTimeoutFromObjectOrReply(c,c->argv[3],&timeout,UNIT_MILLISECONDS) != C_OK)
        return;

    if (server.masterhost) {
        addReplyError(c,"WAITAOF cannot be used with replica instances. Please also note that writes to replicas are just local and are not propagated.");
        return;
    }
    if (numlocal && !server.aof_enabled) {
        addReplyError(c, "WAITAOF cannot be used when numlocal is set but appendonly is disabled.");
        return;
    }

    /* 先尝试不阻塞地检查是否已满足条件。 */
    ackreplicas = replicationCountAOFAcksByOffset(c->woff);
    acklocal = server.fsynced_reploff >= c->woff;
    if ((ackreplicas >= numreplicas && acklocal >= numlocal) || c->flags & CLIENT_DENY_BLOCKING) {
        addReplyArrayLen(c,2);
        addReplyLongLong(c,acklocal);
        addReplyLongLong(c,ackreplicas);
        return;
    }

    /* 否则阻塞客户端，将其放入等待副本确认的客户端列表中。 */
    blockForAofFsync(c,timeout,c->woff,numlocal,numreplicas);

    /* 确保服务器在返回事件循环之前，向所有副本发送 ACK 请求。 */
    replicationRequestAckFromSlaves();
}

/* 此函数由 unblockClient() 调用，用于执行特定于阻塞操作类型的
 * 清理工作。我们只需将客户端从等待副本确认的客户端列表中移除。
 * 不要直接调用此函数，应调用 unblockClient()。 */
void unblockClientWaitingReplicas(client *c) {
    listNode *ln = listSearchKey(server.clients_waiting_acks,c);
    serverAssert(ln != NULL);
    listDelNode(server.clients_waiting_acks,ln);
    updateStatsOnUnblock(c, 0, 0, 0);
}

/* 检查是否有阻塞在 WAIT 或 WAITAOF 的客户端可以被解除阻塞，
 * 因为我们已收到足够数量的副本 ACK。 */
void processClientsWaitingReplicas(void) {
    long long last_offset = 0;
    long long last_aof_offset = 0;
    int last_numreplicas = 0;
    int last_aof_numreplicas = 0;

    listIter li;
    listNode *ln;

    listRewind(server.clients_waiting_acks,&li);
    while((ln = listNext(&li))) {
        int numlocal = 0;
        int numreplicas = 0;

        client *c = ln->value;
        int is_wait_aof = c->bstate.btype == BLOCKED_WAITAOF;

        if (is_wait_aof && c->bstate.numlocal && !server.aof_enabled) {
            addReplyError(c, "WAITAOF cannot be used when numlocal is set but appendonly is disabled.");
            unblockClient(c, 1);
            continue;
        }

        /* 每次找到一个满足给定偏移量和副本数量条件的客户端时，
         * 我们记住该结果，以便下一个客户端如果请求的偏移量/副本数量
         * 等于或小于当前值，则无需再次调用 replicationCountAcksByOffset()
         * 或 replicationCountAOFAcksByOffset() 即可解除阻塞。 */
        if (!is_wait_aof && last_offset && last_offset >= c->bstate.reploffset &&
                           last_numreplicas >= c->bstate.numreplicas)
        {
            numreplicas = last_numreplicas;
        } else if (is_wait_aof && last_aof_offset && last_aof_offset >= c->bstate.reploffset &&
                    last_aof_numreplicas >= c->bstate.numreplicas)
        {
            numreplicas = last_aof_numreplicas;
        } else {
            numreplicas = is_wait_aof ?
                replicationCountAOFAcksByOffset(c->bstate.reploffset) :
                replicationCountAcksByOffset(c->bstate.reploffset);

            /* 检查副本数量是否已满足。 */
            if (numreplicas < c->bstate.numreplicas) continue;

            if (is_wait_aof) {
                last_aof_offset = c->bstate.reploffset;
                last_aof_numreplicas = numreplicas;
            } else {
                last_offset = c->bstate.reploffset;
                last_numreplicas = numreplicas;
            }
        }

        /* 检查 WAITAOF 的本地约束是否已满足 */
        if (is_wait_aof) {
            numlocal = server.fsynced_reploff >= c->bstate.reploffset;
            if (numlocal < c->bstate.numlocal) continue;
        }

        /* 在解除阻塞之前发送回复，因为 unblockClient 会调用 reqresAppendResponse */
        if (is_wait_aof) {
            /* WAITAOF 返回数组回复 */
            addReplyArrayLen(c, 2);
            addReplyLongLong(c, numlocal);
            addReplyLongLong(c, numreplicas);
        } else {
            addReplyLongLong(c, numreplicas);
        }

        unblockClient(c, 1);
    }
}

/* 返回本实例的从节点复制偏移量，即我们已处理的主节点复制流的偏移量。 */
long long replicationGetSlaveOffset(void) {
    long long offset = 0;

    if (server.masterhost != NULL) {
        if (server.master) {
            offset = server.master->reploff;
        } else if (server.cached_master) {
            offset = server.cached_master->reploff;
        }
    }
    /* 当主节点完全不支持偏移量时，offset 可能为 -1。
     * 但此函数旨在返回一个能表达主节点已处理数据量的偏移量，
     * 因此我们返回一个非负整数。 */
    if (offset < 0) offset = 0;
    return offset;
}

/* --------------------------- 复制定时任务  ---------------------------- */

/* 复制定时任务函数，每秒调用一次。 */
void replicationCron(void) {
    static long long replication_cron_loops = 0;

    /* 首先检查故障转移状态，判断是否需要开始处理故障转移。 */
    updateFailoverStatus();

    /* 非阻塞连接是否超时？ */
    if (server.masterhost &&
        (server.repl_state == REPL_STATE_CONNECTING ||
         slaveIsInHandshakeState()) &&
         (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"Timeout connecting to the MASTER...");
        cancelReplicationHandshake(1);
    }

    /* 批量传输 I/O 是否超时？ */
    if (server.masterhost && server.repl_state == REPL_STATE_TRANSFER &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"Timeout receiving bulk data from MASTER... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");
        cancelReplicationHandshake(1);
    }

    /* 已连接的从节点与主节点通信超时？ */
    if (server.masterhost && server.repl_state == REPL_STATE_CONNECTED &&
        (time(NULL)-server.master->lastinteraction) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"MASTER timeout: no data nor PING received...");
        freeClient(server.master);
    }

    /* 检查是否需要连接到主节点 */
    if (server.repl_state == REPL_STATE_CONNECT) {
        serverLog(LL_NOTICE,"Connecting to MASTER %s:%d",
            server.masterhost, server.masterport);
        connectWithMaster();
    }

    /* 定期向主节点发送 ACK。
     * 注意：对于不支持 PSYNC 和复制偏移量的主节点，
     * 不会发送周期性 ACK。 */
    if (server.masterhost && server.master &&
        !(server.master->flags & CLIENT_PRE_PSYNC))
        replicationSendAck();

    /* 如果有连接的从节点，定期向它们发送 PING。
     * 这样从节点可以实现对主节点的显式超时检测，
     * 即使 TCP 连接未实际断开也能感知连接中断。 */
    listIter li;
    listNode *ln;
    robj *ping_argv[1];

    /* 首先，按照 ping_slave_period 配置发送 PING。 */
    if ((replication_cron_loops % server.repl_ping_slave_period) == 0 &&
        listLength(server.slaves))
    {
        /* 注意：在 Redis Cluster 手动故障转移期间，如果客户端被暂停，
         * 则不发送 PING。否则发送的 PING 会改变主从节点的复制偏移量，
         * 导致与 'mf_master_offset' 状态中存储的值不匹配。 */
        int manual_failover_in_progress =
            ((server.cluster_enabled &&
              clusterManualFailoverTimeLimit()) ||
            server.failover_end_time) &&
            isPausedActionsWithUpdate(PAUSE_ACTION_REPLICA);

        if (!manual_failover_in_progress) {
            ping_argv[0] = shared.ping;
            replicationFeedSlaves(server.slaves, -1,
                ping_argv, 1);
        }
    }

    /* 其次，向所有处于预同步阶段的从节点发送换行符，
     * 即等待主节点创建 RDB 文件的从节点。
     *
     * 如果与主节点断开连接，也向所有级联从节点发送换行符，
     * 以保持从节点感知其主节点在线。这是因为子从节点仅从
     * 顶层主节点接收代理数据，没有显式 ping 机制以避免
     * 改变复制偏移量。这种特殊的带外 ping（换行符）可以
     * 发送，不会影响偏移量。
     *
     * 换行符会被从节点忽略，但会刷新最后交互计时器以防止
     * 超时。此时忽略 ping 周期，每秒刷新一次连接，因为某些
     * 超时设置仅为几秒（例如：PSYNC 响应）。 */
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        int is_presync =
            (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
            (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
             server.rdb_child_type != RDB_CHILD_TYPE_SOCKET));

        if (is_presync) {
            connWrite(slave->conn, "\n", 1);
        }
    }

    /* 断开超时的从节点连接。 */
    if (listLength(server.slaves)) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            if (slave->replstate == SLAVE_STATE_ONLINE) {
                if (slave->flags & CLIENT_PRE_PSYNC)
                    continue;
                if ((server.unixtime - slave->repl_ack_time) > server.repl_timeout) {
                    serverLog(LL_WARNING, "Disconnecting timedout replica (streaming sync): %s",
                          replicationGetSlaveName(slave));
                    freeClient(slave);
                    continue;
                }
            }
            /* 仅考虑断开无盘复制的副本，因为基于磁盘的副本不由
             * fork 子进程提供数据，所以即使基于磁盘的副本卡住，
             * 也不会阻止 fork 子进程终止。 */
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END && server.rdb_child_type == RDB_CHILD_TYPE_SOCKET) {
                if (slave->repl_last_partial_write != 0 &&
                    (server.unixtime - slave->repl_last_partial_write) > server.repl_timeout)
                {
                    serverLog(LL_WARNING, "Disconnecting timedout replica (full sync): %s",
                          replicationGetSlaveName(slave));
                    freeClient(slave);
                    continue;
                }
            }
        }
    }

    /* 如果当前是主节点且没有连接的从节点，但存在活跃的复制积压缓冲区，
     * 为回收内存，可在配置的时间后释放它。注意：从节点不能执行此操作，
     * 没有子从节点的从节点仍应在积压缓冲区中积累数据，以便在故障转移
     * 后成为主节点时能回复 PSYNC 查询。 */
    if (listLength(server.slaves) == 0 && server.repl_backlog_time_limit &&
        server.repl_backlog && server.masterhost == NULL)
    {
        time_t idle = server.unixtime - server.repl_no_slaves_since;

        if (idle > server.repl_backlog_time_limit) {
            /* 释放积压缓冲区时，始终使用新的复制 ID 并清除 ID2。
             * 这是因为当没有积压缓冲区时，master_repl_offset 不会更新，
             * 但仍会保留复制 ID，导致以下问题：
             *
             * 1. 当前是主节点实例。
             * 2. 从节点被提升为主节点，其 repl-id-2 与当前 repl-id 相同。
             * 3. 当前仍为主节点时收到一些更新，但不会递增
             *    master_repl_offset。
             * 4. 之后被转为从节点，连接到新主节点，新主节点通过第二个
             *    复制 ID 接受 PSYNC 请求，但由于之前收到过写入操作，
             *    会导致数据不一致。 */
            changeReplicationId();
            clearReplicationId2();
            freeReplicationBacklog();
            serverLog(LL_NOTICE,
                "Replication backlog freed after %d seconds "
                "without connected replicas.",
                (int) server.repl_backlog_time_limit);
        }
    }

    replicationStartPendingFork();

    /* 如果 Redis 未使用任何持久化方式，移除用于复制的 RDB 文件。 */
    removeRDBUsedToSyncReplicas();

    /* 复制缓冲区完整性检查：复制缓冲区块的第一个块必须被某些实体引用，
     * 因为未被引用时会被释放，否则服务器会发生 OOM。此外，其引用计数
     * 不得超过副本数 + 1（复制积压缓冲区）。 */
    if (listLength(server.repl_buffer_blocks) > 0) {
        replBufBlock *o = listNodeValue(listFirst(server.repl_buffer_blocks));
        serverAssert(o->refcount > 0 &&
            o->refcount <= (int)listLength(server.slaves)+1);
    }

    /* 刷新延迟 <= min-slaves-max-lag 的从节点数量。 */
    refreshGoodSlavesCount();
    replication_cron_loops++; // 以 1 HZ 频率递增
}

int shouldStartChildReplication(int *mincapa_out, int *req_out) {
    /* 如果有从节点处于 WAIT_BGSAVE_START 状态，应启动适合复制的
     * BGSAVE。
     *
     * 对于无盘复制，确保等待配置指定的秒数，以便其他从节点
     * 有时间在开始流式传输前到达。 */
    if (!hasActiveChildProcess()) {
        time_t idle, max_idle = 0;
        int slaves_waiting = 0;
        int mincapa;
        int req;
        int first = 1;
        listNode *ln;
        listIter li;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                if (first) {
                    // 获取第一个从节点的需求
                    req = slave->slave_req;
                } else if (req != slave->slave_req) {
                    // 跳过不匹配的从节点
                    continue;
                }
                idle = server.unixtime - slave->lastinteraction;
                if (idle > max_idle) max_idle = idle;
                slaves_waiting++;
                mincapa = first ? slave->slave_capa : (mincapa & slave->slave_capa);
                first = 0;
            }
        }

        if (slaves_waiting &&
            (!server.repl_diskless_sync ||
             (server.repl_diskless_sync_max_replicas > 0 &&
              slaves_waiting >= server.repl_diskless_sync_max_replicas) ||
             max_idle >= server.repl_diskless_sync_delay))
        {
            if (mincapa_out)
                *mincapa_out = mincapa;
            if (req_out)
                *req_out = req;
            return 1;
        }
    }

    return 0;
}

void replicationStartPendingFork(void) {
    int mincapa = -1;
    int req = -1;

    if (shouldStartChildReplication(&mincapa, &req)) {
        /* 启动 BGSAVE。调用的函数会根据配置以及从节点的
         * 能力和需求，启动以 socket 或磁盘为目标的 BGSAVE。 */
        startBgsaveForReplication(mincapa, req);
    }
}

/* 从副本列表中查找指定 IP:PORT 的副本 */
static client *findReplica(char *host, int port) {
    listIter li;
    listNode *ln;
    client *replica;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        replica = ln->value;
        char ip[NET_IP_STR_LEN], *replicaip = replica->slave_addr;

        if (!replicaip) {
            if (connAddrPeerName(replica->conn, ip, sizeof(ip), NULL) == -1)
                continue;
            replicaip = ip;
        }

        if (!strcasecmp(host, replicaip) &&
                (port == replica->slave_listening_port))
            return replica;
    }

    return NULL;
}

const char *getFailoverStateString(void) {
    switch(server.failover_state) {
        case NO_FAILOVER: return "no-failover";
        case FAILOVER_IN_PROGRESS: return "failover-in-progress";
        case FAILOVER_WAIT_FOR_SYNC: return "waiting-for-sync";
        default: return "unknown";
    }
}

/* 重置内部故障转移配置状态，需要在故障转移
 * 成功或失败后调用，因为它包含了客户端的
 * 恢复操作（取消暂停）。 */
void clearFailoverState(void) {
    server.failover_end_time = 0;
    server.force_failover = 0;
    zfree(server.target_replica_host);
    server.target_replica_host = NULL;
    server.target_replica_port = 0;
    server.failover_state = NO_FAILOVER;
    unpauseActions(PAUSE_DURING_FAILOVER);
}

/* 如果有正在进行的故障转移，则中止它。 */
void abortFailover(const char *err) {
    if (server.failover_state == NO_FAILOVER) return;

    if (server.target_replica_host) {
        serverLog(LL_NOTICE,"FAILOVER to %s:%d aborted: %s",
            server.target_replica_host,server.target_replica_port,err);  
    } else {
        serverLog(LL_NOTICE,"FAILOVER to any replica aborted: %s",err);  
    }
    if (server.failover_state == FAILOVER_IN_PROGRESS) {
        replicationUnsetMaster();
    }
    clearFailoverState();
}

/*
 * FAILOVER [TO <HOST> <PORT> [FORCE]] [ABORT] [TIMEOUT <timeout>]
 *
 * 此命令用于协调主节点与其某个副本之间的故障转移。
 * 正常流程包含以下步骤：
 * 1) 主节点发起客户端写暂停，以停止复制流量。
 * 2) 主节点周期性检查是否有副本通过 ACK 确认
 *    已消费完全部复制流数据。
 * 3) 一旦有副本追上进度，主节点将自身转变为副本。
 * 4) 主节点向目标副本发送 PSYNC FAILOVER 请求，
 *    如果被接受，副本将成为新主节点并启动同步。
 *
 * FAILOVER ABORT 是中止故障转移命令的唯一方式，
 * 因为 replicaof 将被禁用。当故障转移无法继续
 * 推进时可能需要使用此命令。
 *
 * 可选参数 [TO <HOST> <IP>] 允许指定要故障转移
 * 到的特定副本。
 *
 * FORCE 标志表示即使目标副本尚未追上进度，
 * 也强制执行故障转移。使用此标志时必须同时
 * 指定 TIMEOUT 和目标 HOST 与 IP。
 *
 * TIMEOUT <timeout> 指定主节点等待副本同步的
 * 最长时间，超时后将中止。如果未指定，
 * 故障转移将无限期尝试，必须手动中止。
 */
void failoverCommand(client *c) {
    if (!clusterAllowFailoverCmd(c)) {
        return;
    }

    /* 处理 ABORT 的特殊情况 */
    if ((c->argc == 2) && !strcasecmp(c->argv[1]->ptr,"abort")) {
        if (server.failover_state == NO_FAILOVER) {
            addReplyError(c, "No failover in progress.");
            return;
        }

        abortFailover("Failover manually aborted");
        addReply(c,shared.ok);
        return;
    }

    long timeout_in_ms = 0;
    int force_flag = 0;
    long port = 0;
    char *host = NULL;

    /* 解析命令语法和参数。 */
    for (int j = 1; j < c->argc; j++) {
        if (!strcasecmp(c->argv[j]->ptr,"timeout") && (j + 1 < c->argc) &&
            timeout_in_ms == 0)
        {
            if (getLongFromObjectOrReply(c,c->argv[j + 1],
                        &timeout_in_ms,NULL) != C_OK) return;
            if (timeout_in_ms <= 0) {
                addReplyError(c,"FAILOVER timeout must be greater than 0");
                return;
            }
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr,"to") && (j + 2 < c->argc) &&
            !host) 
        {
            if (getLongFromObjectOrReply(c,c->argv[j + 2],&port,NULL) != C_OK)
                return;
            host = c->argv[j + 1]->ptr;
            j += 2;
        } else if (!strcasecmp(c->argv[j]->ptr,"force") && !force_flag) {
            force_flag = 1;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c,"FAILOVER already in progress.");
        return;
    }

    if (server.masterhost) {
        addReplyError(c,"FAILOVER is not valid when server is a replica.");
        return;
    }

    if (listLength(server.slaves) == 0) {
        addReplyError(c,"FAILOVER requires connected replicas.");
        return; 
    }

    if (force_flag && (!timeout_in_ms || !host)) {
        addReplyError(c,"FAILOVER with force option requires both a timeout "
            "and target HOST and IP.");
        return;     
    }

    /* 如果提供了副本地址，验证其是否已连接。 */
    if (host) {
        client *replica = findReplica(host, port);

        if (replica == NULL) {
            addReplyError(c,"FAILOVER target HOST and PORT is not "
                            "a replica.");
            return;
        }

        /* 检查请求的副本是否在线 */
        if (replica->replstate != SLAVE_STATE_ONLINE) {
            addReplyError(c,"FAILOVER target replica is not online.");
            return;
        }

        server.target_replica_host = zstrdup(host);
        server.target_replica_port = port;
        serverLog(LL_NOTICE,"FAILOVER requested to %s:%ld.",host,port);
    } else {
        serverLog(LL_NOTICE,"FAILOVER requested to any replica.");
    }

    mstime_t now = commandTimeSnapshot();
    if (timeout_in_ms) {
        server.failover_end_time = now + timeout_in_ms;
    }
    
    server.force_failover = force_flag;
    server.failover_state = FAILOVER_WAIT_FOR_SYNC;
    /* 集群故障转移最终会恢复操作 */
    pauseActions(PAUSE_DURING_FAILOVER,
                 LLONG_MAX,
                 PAUSE_ACTIONS_CLIENT_WRITE_SET);
    addReply(c,shared.ok);
}

/* 故障转移定时函数，检查协调故障转移状态。
 *
 * 实现说明：当前实现调用 replicationSetMaster()
 * 来启动故障转移请求，如果故障转移失败，
 * 这会产生一些未预期的副作用，如被阻塞的
 * 客户端将被解阻，副本将被断开连接。
 * 这可以进一步优化。
 */
void updateFailoverStatus(void) {
    if (server.failover_state != FAILOVER_WAIT_FOR_SYNC) return;
    mstime_t now = server.mstime;

    /* 检查故障转移操作是否已超时 */
    if (server.failover_end_time && server.failover_end_time <= now) {
        if (server.force_failover) {
            serverLog(LL_NOTICE,
                "FAILOVER to %s:%d time out exceeded, failing over.",
                server.target_replica_host, server.target_replica_port);
            server.failover_state = FAILOVER_IN_PROGRESS;
            /* 超时后如果指定了强制故障转移，则执行之。 */
            replicationSetMaster(server.target_replica_host,
                server.target_replica_port);
            return;
        } else {
            /* 未请求强制故障转移，因此超时中止。 */
            abortFailover("Replica never caught up before timeout");
            return;
        }
    }

    /* 检查副本是否已追上进度，以启动故障转移 */
    client *replica = NULL;
    if (server.target_replica_host) {
        replica = findReplica(server.target_replica_host, 
            server.target_replica_port);
    } else {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        /* 查找任何 repl_offset 与我们匹配的副本 */
        while((ln = listNext(&li))) {
            replica = ln->value;
            if (replica->repl_ack_off == server.master_repl_offset) {
                char ip[NET_IP_STR_LEN], *replicaaddr = replica->slave_addr;

                if (!replicaaddr) {
                    if (connAddrPeerName(replica->conn,ip,sizeof(ip),NULL) == -1)
                        continue;
                    replicaaddr = ip;
                }

                /* 正在故障转移到此特定节点 */
                server.target_replica_host = zstrdup(replicaaddr);
                server.target_replica_port = replica->slave_listening_port;
                break;
            }
        }
    }

    /* 找到了一个已追上进度的副本 */
    if (replica && (replica->repl_ack_off == server.master_repl_offset)) {
        server.failover_state = FAILOVER_IN_PROGRESS;
        serverLog(LL_NOTICE,
                "Failover target %s:%d is synced, failing over.",
                server.target_replica_host, server.target_replica_port);
        /* 指定副本已追上进度，执行故障转移。 */
        replicationSetMaster(server.target_replica_host,
            server.target_replica_port);
    }
}
