/* Slowlog（慢查询日志）实现了一个能够记住最近 N 个
 * 执行时间超过 M 微秒的查询的系统。
 *
 * 被记录到慢查询日志中的执行时间阈值通过
 * 'slowlog-log-slower-than' 配置指令设置，该指令也可以
 * 通过 CONFIG SET/GET 命令进行读写。
 *
 * 慢查询日志实际上并不"记录"到 Redis 日志文件中，
 * 而是通过 SLOWLOG 命令来访问。
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */


#include "server.h"
#include "slowlog.h"

/* 创建一个新的慢查询日志条目。
 * 增加所有保留对象的引用计数由此函数负责。 */
slowlogEntry *slowlogCreateEntry(client *c, robj **argv, int argc, long long duration) {
    slowlogEntry *se = zmalloc(sizeof(*se));
    int j, slargc = argc;

    if (slargc > SLOWLOG_ENTRY_MAX_ARGC) slargc = SLOWLOG_ENTRY_MAX_ARGC;
    se->argc = slargc;
    se->argv = zmalloc(sizeof(robj*)*slargc);
    for (j = 0; j < slargc; j++) {
        /* 记录过多参数是无用的内存浪费，因此我们在
         * SLOWLOG_ENTRY_MAX_ARGC 处停止，但用最后一个参数
         * 说明原始命令中还有多少剩余参数。 */
        if (slargc != argc && j == slargc-1) {
            se->argv[j] = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),"... (%d more arguments)",
                argc-slargc+1));
        } else {
            /* 同时截断过长的字符串 */
            if (argv[j]->type == OBJ_STRING &&
                sdsEncodedObject(argv[j]) &&
                sdslen(argv[j]->ptr) > SLOWLOG_ENTRY_MAX_STRING)
            {
                sds s = sdsnewlen(argv[j]->ptr, SLOWLOG_ENTRY_MAX_STRING);

                s = sdscatprintf(s,"... (%lu more bytes)",
                    (unsigned long)
                    sdslen(argv[j]->ptr) - SLOWLOG_ENTRY_MAX_STRING);
                se->argv[j] = createObject(OBJ_STRING,s);
            } else if (argv[j]->refcount == OBJ_SHARED_REFCOUNT) {
                se->argv[j] = argv[j];
            } else {
                /* 这里需要复制构成命令参数向量的字符串对象，
                 * 因为它们可能与存储在键中的字符串对象共享。
                 * 在 Redis 的任何部分和存储数据的数据结构之间
                 * 共享对象是一个问题：FLUSHALL ASYNC 可能释放
                 * 共享的字符串对象并产生竞态条件。 */
                se->argv[j] = dupStringObject(argv[j]);
            }
        }
    }
    se->time = time(NULL);
    se->duration = duration;
    se->id = server.slowlog_entry_id++;
    se->peerid = sdsnew(getClientPeerId(c));
    se->cname = c->name ? sdsnew(c->name->ptr) : sdsempty();
    return se;
}

/* 释放一个慢查询日志条目。参数类型为 void，
 * 以匹配 adlist.c 中 'free' 方法的函数原型。
 *
 * 此函数负责释放所有保留的对象。 */
void slowlogFreeEntry(void *septr) {
    slowlogEntry *se = septr;
    int j;

    for (j = 0; j < se->argc; j++)
        decrRefCount(se->argv[j]);
    zfree(se->argv);
    sdsfree(se->peerid);
    sdsfree(se->cname);
    zfree(se);
}

/* 初始化慢查询日志。此函数应在服务器启动时调用一次。 */
void slowlogInit(void) {
    server.slowlog = listCreate();
    server.slowlog_entry_id = 0;
    listSetFreeMethod(server.slowlog,slowlogFreeEntry);
}

/* 向慢查询日志中推入一个新条目。
 * 此函数会确保根据配置的最大长度修剪慢查询日志。 */
void slowlogPushEntryIfNeeded(client *c, robj **argv, int argc, long long duration) {
    if (server.slowlog_log_slower_than < 0 || server.slowlog_max_len == 0) return; /* 慢查询日志已禁用 */
    if (duration >= server.slowlog_log_slower_than)
        listAddNodeHead(server.slowlog,
                        slowlogCreateEntry(c,argv,argc,duration));

    /* 如有需要，移除旧条目 */
    while (listLength(server.slowlog) > server.slowlog_max_len)
        listDelNode(server.slowlog,listLast(server.slowlog));
}

/* 移除当前慢查询日志中的所有条目。 */
void slowlogReset(void) {
    while (listLength(server.slowlog) > 0)
        listDelNode(server.slowlog,listLast(server.slowlog));
}

/* SLOWLOG 命令。实现处理 Redis 慢查询日志所需的所有子命令。 */
void slowlogCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"GET [<count>]",
"    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
"    Entries are made of:",
"    id, timestamp, time in microseconds, arguments array, client IP and port,",
"    client name",
"LEN",
"    Return the length of the slowlog.",
"RESET",
"    Reset the slowlog.",
NULL
        };
        addReplyHelp(c, help);
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"reset")) {
        slowlogReset();
        addReply(c,shared.ok);
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"len")) {
        addReplyLongLong(c,listLength(server.slowlog));
    } else if ((c->argc == 2 || c->argc == 3) &&
               !strcasecmp(c->argv[1]->ptr,"get"))
    {
        long count = 10;
        listIter li;
        listNode *ln;
        slowlogEntry *se;

        if (c->argc == 3) {
            /* 解析 count 参数 */
            if (getRangeLongFromObjectOrReply(c, c->argv[2], -1,
                    LONG_MAX, &count, "count should be greater than or equal to -1") != C_OK)
                return;

            if (count == -1) {
                /* 将 -1 视为特殊值，表示获取所有慢查询日志。
                 * 将 count 设置为 server.slowlog 的长度即可。*/
                count = listLength(server.slowlog);
            }
        }

        if (count > (long)listLength(server.slowlog)) {
            count = listLength(server.slowlog);
        }
        addReplyArrayLen(c, count);
        listRewind(server.slowlog, &li);
        while (count--) {
            int j;

            ln = listNext(&li);
            se = ln->value;
            addReplyArrayLen(c,6);
            addReplyLongLong(c,se->id);
            addReplyLongLong(c,se->time);
            addReplyLongLong(c,se->duration);
            addReplyArrayLen(c,se->argc);
            for (j = 0; j < se->argc; j++)
                addReplyBulk(c,se->argv[j]);
            addReplyBulkCBuffer(c,se->peerid,sdslen(se->peerid));
            addReplyBulkCBuffer(c,se->cname,sdslen(se->cname));
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}
