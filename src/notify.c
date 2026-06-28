/*
 * Copyright (c) 2013-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"

/* 此文件实现了通过 Pub/Sub 发送键空间事件通知的功能，
 * 详见 https://redis.io/topics/notifications。 */

/* 将表示通知类别的字符串转换为对应的标志位整数（各标志位按位或）。
 *
 * 如果输入包含无法映射到任何类别的字符，函数返回 -1。 */
int keyspaceEventsStringToFlags(char *classes) {
    char *p = classes;
    int c, flags = 0;

    while((c = *p++) != '\0') {
        switch(c) {
        case 'A': flags |= NOTIFY_ALL; break;       /* 所有事件 */
        case 'g': flags |= NOTIFY_GENERIC; break;    /* 通用命令事件（如 DEL、RENAME） */
        case '$': flags |= NOTIFY_STRING; break;     /* 字符串命令事件 */
        case 'l': flags |= NOTIFY_LIST; break;       /* 列表命令事件 */
        case 's': flags |= NOTIFY_SET; break;        /* 集合命令事件 */
        case 'h': flags |= NOTIFY_HASH; break;       /* 哈希命令事件 */
        case 'z': flags |= NOTIFY_ZSET; break;       /* 有序集合命令事件 */
        case 'x': flags |= NOTIFY_EXPIRED; break;    /* 键过期事件 */
        case 'e': flags |= NOTIFY_EVICTED; break;    /* 键被驱逐事件 */
        case 'K': flags |= NOTIFY_KEYSPACE; break;   /* 键空间通知（频道名含键名） */
        case 'E': flags |= NOTIFY_KEYEVENT; break;   /* 键事件通知（频道名含事件名） */
        case 't': flags |= NOTIFY_STREAM; break;     /* 流命令事件 */
        case 'm': flags |= NOTIFY_KEY_MISS; break;   /* 访问不存在的键事件 */
        case 'd': flags |= NOTIFY_MODULE; break;     /* 模块事件 */
        case 'n': flags |= NOTIFY_NEW; break;        /* 新键创建事件 */
        default: return -1;
        }
    }
    return flags;
}

/* 此函数执行上述函数的逆操作：接收包含标志位的整数作为输入，
 * 返回表示所选类别的字符串。返回的字符串是 sds 类型，
 * 需要调用 sdsfree() 释放。 */
sds keyspaceEventsFlagsToString(int flags) {
    sds res;

    res = sdsempty();
    if ((flags & NOTIFY_ALL) == NOTIFY_ALL) {
        res = sdscatlen(res,"A",1);
    } else {
        if (flags & NOTIFY_GENERIC) res = sdscatlen(res,"g",1);
        if (flags & NOTIFY_STRING) res = sdscatlen(res,"$",1);
        if (flags & NOTIFY_LIST) res = sdscatlen(res,"l",1);
        if (flags & NOTIFY_SET) res = sdscatlen(res,"s",1);
        if (flags & NOTIFY_HASH) res = sdscatlen(res,"h",1);
        if (flags & NOTIFY_ZSET) res = sdscatlen(res,"z",1);
        if (flags & NOTIFY_EXPIRED) res = sdscatlen(res,"x",1);
        if (flags & NOTIFY_EVICTED) res = sdscatlen(res,"e",1);
        if (flags & NOTIFY_STREAM) res = sdscatlen(res,"t",1);
        if (flags & NOTIFY_MODULE) res = sdscatlen(res,"d",1);
        if (flags & NOTIFY_NEW) res = sdscatlen(res,"n",1);
    }
    if (flags & NOTIFY_KEYSPACE) res = sdscatlen(res,"K",1);
    if (flags & NOTIFY_KEYEVENT) res = sdscatlen(res,"E",1);
    if (flags & NOTIFY_KEY_MISS) res = sdscatlen(res,"m",1);
    return res;
}

/* 提供给 Redis 核心其他部分的 API 是一个简单的函数：
 *
 * notifyKeyspaceEvent(int type, char *event, robj *key, int dbid);
 *
 * 'type' 是在 server.h 中定义的通知类别标志。
 * 'event' 是表示事件名称的 C 字符串。
 * 'key' 是表示键名的 Redis 对象。
 * 'dbid' 是键所在的数据库 ID。 */
void notifyKeyspaceEvent(int type, const char *event, robj *key, int dbid) {
    sds chan;
    robj *chanobj, *eventobj;
    int len = -1;
    char buf[24];

    /* 如果有模块订阅了事件，立即通知模块系统。
     * 这会绕过通知配置，但模块引擎只会
     * 在事件类型匹配时才调用事件订阅者。 */
     moduleNotifyKeyspaceEvent(type, event, key, dbid);

    /* 如果此类事件的通知已关闭，立即返回。 */
    if (!(server.notify_keyspace_events & type)) return;

    eventobj = createStringObject(event,strlen(event));

    /* __keyspace@<db>__:<key> <event> 形式的通知。
     * 以频道名为键名，事件名为消息内容。 */
    if (server.notify_keyspace_events & NOTIFY_KEYSPACE) {
        chan = sdsnewlen("__keyspace@",11);
        len = ll2string(buf,sizeof(buf),dbid);
        chan = sdscatlen(chan, buf, len);
        chan = sdscatlen(chan, "__:", 3);
        chan = sdscatsds(chan, key->ptr);
        chanobj = createObject(OBJ_STRING, chan);
        pubsubPublishMessage(chanobj, eventobj, 0);
        decrRefCount(chanobj);
    }

    /* __keyevent@<db>__:<event> <key> 形式的通知。
     * 以事件名为频道名，键名为消息内容。 */
    if (server.notify_keyspace_events & NOTIFY_KEYEVENT) {
        chan = sdsnewlen("__keyevent@",11);
        if (len == -1) len = ll2string(buf,sizeof(buf),dbid);
        chan = sdscatlen(chan, buf, len);
        chan = sdscatlen(chan, "__:", 3);
        chan = sdscatsds(chan, eventobj->ptr);
        chanobj = createObject(OBJ_STRING, chan);
        pubsubPublishMessage(chanobj, key, 0);
        decrRefCount(chanobj);
    }
    decrRefCount(eventobj);
}
