/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"

/*-----------------------------------------------------------------------------
 * List API（列表 API）
 *----------------------------------------------------------------------------*/

/* 检查将要添加到列表中的一系列对象的长度和大小，
 * 以判断是否需要将 listpack 转换为 quicklist。
 * 注意：我们只检查字符串编码的对象，因为字符串长度可以在 O(1) 时间内查询。
 *
 * 如果传入了回调函数，则在执行列表编码转换前先调用该回调，
 * 以便调用者做一些前置工作。
 */
static void listTypeTryConvertListpack(robj *o, robj **argv, int start, int end,
                                       beforeConvertCB fn, void *data)
{
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK);

    size_t add_bytes = 0;
    size_t add_length = 0;

    if (argv) {
        for (int i = start; i <= end; i++) {
            if (!sdsEncodedObject(argv[i]))
                continue;
            add_bytes += sdslen(argv[i]->ptr);
        }
        add_length = end - start + 1;
    }

    if (quicklistNodeExceedsLimit(server.list_max_listpack_size,
            lpBytes(o->ptr) + add_bytes, lpLength(o->ptr) + add_length))
    {
        /* 在转换前调用回调函数 */
        if (fn) fn(data);

        quicklist *ql = quicklistNew(server.list_max_listpack_size, server.list_compress_depth);

        /* 如果 listpack 非空则追加到 quicklist，否则直接释放 */
        if (lpLength(o->ptr))
            quicklistAppendListpack(ql, o->ptr);
        else
            lpFree(o->ptr);
        o->ptr = ql;
        o->encoding = OBJ_ENCODING_QUICKLIST;
    }
}

/* 检查 quicklist 的长度和大小，以判断是否需要将其转换为 listpack。
 *
 * 'shrinking' 为 1 表示此次转换是由列表收缩引起的。
 * 为避免因频繁插入与删除而反复在 quicklist 和 listpack 之间转换，
 * 只有当 quicklist 的长度或大小低于限制值一半时，才执行该转换。
 *
 * 如果传入了回调函数，则在执行列表编码转换前先调用该回调，
 * 以便调用者做一些前置工作。
 */
static void listTypeTryConvertQuicklist(robj *o, int shrinking, beforeConvertCB fn, void *data) {
    serverAssert(o->encoding == OBJ_ENCODING_QUICKLIST);

    size_t sz_limit;
    unsigned int count_limit;
    quicklist *ql = o->ptr;

    /* 只有当 quicklist 仅包含一个 packed 节点时，才能转换为 listpack */
    if (ql->len != 1 || ql->head->container != QUICKLIST_NODE_CONTAINER_PACKED)
        return;

    /* 检查 quicklist 的长度或大小是否低于限制 */
    quicklistNodeLimit(server.list_max_listpack_size, &sz_limit, &count_limit);
    if (shrinking) {
        sz_limit /= 2;
        count_limit /= 2;
    }
    if (ql->head->sz > sz_limit || ql->count > count_limit) return;

    /* 在转换前调用回调函数 */
    if (fn) fn(data);

    /* 取出唯一 quicklist 节点中的 listpack，然后重置节点并释放 quicklist */
    o->ptr = ql->head->entry;
    ql->head->entry = NULL;
    quicklistRelease(ql);
    o->encoding = OBJ_ENCODING_LISTPACK;
}

/* 检查列表是否因增长、收缩或其它情况需要转换为合适的编码。
 *
 * 'lct' 可以是以下取值之一：
 * LIST_CONV_AUTO      - 在构建新列表后使用，希望函数自行决定最佳编码。
 * LIST_CONV_GROWING   - 在向列表添加元素前或刚添加后使用，
 *                       此时很可能只需要考虑从 listpack 转换为 quicklist。
 *                       'argv' 仅在该情形下用于计算即将添加到列表的对象大小。
 * LIST_CONV_SHRINKING - 在从列表中移除元素后使用，此时需要考虑
 *                       从 quicklist 转换为 listpack。
 *                       当确定是收缩场景时，会采用更低（更严格）的阈值，
 *                       以避免每次列表变化都反复进行转换。
 */
static void listTypeTryConversionRaw(robj *o, list_conv_type lct,
                                     robj **argv, int start, int end,
                                     beforeConvertCB fn, void *data)
{
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        if (lct == LIST_CONV_GROWING) return; /* 增长场景与 quicklist 无关 */
        listTypeTryConvertQuicklist(o, lct == LIST_CONV_SHRINKING, fn, data);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        if (lct == LIST_CONV_SHRINKING) return; /* 收缩场景与 listpack 无关 */
        listTypeTryConvertListpack(o, argv, start, end, fn, data);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* listTypeTryConversionRaw() 的简单包装，
 * 允许在不需要传递 'argv' 的情况下尝试编码转换。 */
void listTypeTryConversion(robj *o, list_conv_type lct, beforeConvertCB fn, void *data) {
    listTypeTryConversionRaw(o, lct, NULL, 0, 0, fn, data);
}

/* listTypeTryConversionRaw() 的简单包装，
 * 允许在向列表添加元素之前尝试编码转换。 */
void listTypeTryConversionAppend(robj *o, robj **argv, int start, int end,
                                 beforeConvertCB fn, void *data)
{
    listTypeTryConversionRaw(o, LIST_CONV_GROWING, argv, start, end, fn, data);
}

/* 将一个元素推入指定的列表对象 'subject' 的头部或尾部位置，
 * 具体位置由 'where' 决定。
 *
 * 调用者无需为 'value' 增加引用计数，
 * 函数会在需要时自行处理引用计数。
 */
void listTypePush(robj *subject, robj *value, int where) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        int pos = (where == LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
        if (value->encoding == OBJ_ENCODING_INT) {
            char buf[32];
            ll2string(buf, 32, (long)value->ptr);
            quicklistPush(subject->ptr, buf, strlen(buf), pos);
        } else {
            quicklistPush(subject->ptr, value->ptr, sdslen(value->ptr), pos);
        }
    } else if (subject->encoding == OBJ_ENCODING_LISTPACK) {
        if (value->encoding == OBJ_ENCODING_INT) {
            subject->ptr = (where == LIST_HEAD) ?
                lpPrependInteger(subject->ptr, (long)value->ptr) :
                lpAppendInteger(subject->ptr, (long)value->ptr);
        } else {
            subject->ptr = (where == LIST_HEAD) ?
                lpPrepend(subject->ptr, value->ptr, sdslen(value->ptr)) :
                lpAppend(subject->ptr, value->ptr, sdslen(value->ptr));
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* quicklistPopCustom 的回调函数：将弹出的原始数据封装为字符串对象 */
void *listPopSaver(unsigned char *data, size_t sz) {
    return createStringObject((char*)data,sz);
}

/* 从列表的指定端弹出一个元素并返回封装后的 robj 对象 */
robj *listTypePop(robj *subject, int where) {
    robj *value = NULL;

    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        long long vlong;
        int ql_where = where == LIST_HEAD ? QUICKLIST_HEAD : QUICKLIST_TAIL;
        if (quicklistPopCustom(subject->ptr, ql_where, (unsigned char **)&value,
                               NULL, &vlong, listPopSaver)) {
            if (!value)
                value = createStringObjectFromLongLong(vlong);
        }
    } else if (subject->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *p;
        unsigned char *vstr;
        int64_t vlen;
        unsigned char intbuf[LP_INTBUF_SIZE];

        p = (where == LIST_HEAD) ? lpFirst(subject->ptr) : lpLast(subject->ptr);
        if (p) {
            vstr = lpGet(p, &vlen, intbuf);
            value = createStringObject((char*)vstr, vlen);
            subject->ptr = lpDelete(subject->ptr, p, NULL);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

/* 返回列表对象中的元素数量 */
unsigned long listTypeLength(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistCount(subject->ptr);
    } else if (subject->encoding == OBJ_ENCODING_LISTPACK) {
        return lpLength(subject->ptr);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* 在指定索引位置初始化一个列表迭代器。 */
listTypeIterator *listTypeInitIterator(robj *subject, long index,
                                       unsigned char direction) {
    listTypeIterator *li = zmalloc(sizeof(listTypeIterator));
    li->subject = subject;
    li->encoding = subject->encoding;
    li->direction = direction;
    li->iter = NULL;
    /* LIST_HEAD 表示从 TAIL 端开始并向 head 方向遍历。
     * LIST_TAIL 表示从 HEAD 端开始并向 tail 方向遍历。 */
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        int iter_direction = direction == LIST_HEAD ? AL_START_TAIL : AL_START_HEAD;
        li->iter = quicklistGetIteratorAtIdx(li->subject->ptr,
                                             iter_direction, index);
    } else if (li->encoding == OBJ_ENCODING_LISTPACK) {
        li->lpi = lpSeek(subject->ptr, index);
    } else {
        serverPanic("Unknown list encoding");
    }
    return li;
}

/* 设置迭代器的遍历方向。 */
void listTypeSetIteratorDirection(listTypeIterator *li, listTypeEntry *entry, unsigned char direction) {
    if (li->direction == direction) return;

    li->direction = direction;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        int dir = direction == LIST_HEAD ? AL_START_TAIL : AL_START_HEAD;
        quicklistSetDirection(li->iter, dir);
    } else if (li->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = li->subject->ptr;
        /* 注意：listpack 的迭代器始终指向当前条目的下一个位置，
         * 因此需要根据方向更新迭代器的位置。 */
        li->lpi = (direction == LIST_TAIL) ? lpNext(lp, entry->lpe) : lpPrev(lp, entry->lpe);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* 释放迭代器资源。 */
void listTypeReleaseIterator(listTypeIterator *li) {
    if (li->encoding == OBJ_ENCODING_QUICKLIST)
        quicklistReleaseIterator(li->iter);
    zfree(li);
}

/* 将当前条目的指针保存到传入的 entry 结构中，
 * 并推进迭代器的位置。
 * 当当前位置确实是一个条目时返回 1，否则返回 0。 */
int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
    /* 迭代过程中禁止修改编码 */
    serverAssert(li->subject->encoding == li->encoding);

    entry->li = li;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistNext(li->iter, &entry->entry);
    } else if (li->encoding == OBJ_ENCODING_LISTPACK) {
        entry->lpe = li->lpi;
        if (entry->lpe != NULL) {
            li->lpi = (li->direction == LIST_TAIL) ?
                lpNext(li->subject->ptr,li->lpi) : lpPrev(li->subject->ptr,li->lpi);
            return 1;
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return 0;
}

/* 获取迭代器当前位置条目的值。
 * 当函数返回 NULL 时，表示值是整数，并通过引用写入 'lval'；
 * 否则返回字符串指针，并将字符串长度写入 'vlen'。 */
unsigned char *listTypeGetValue(listTypeEntry *entry, size_t *vlen, long long *lval) {
    unsigned char *vstr = NULL;
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        if (entry->entry.value) {
            vstr = entry->entry.value;
            *vlen = entry->entry.sz;
        } else {
            *lval = entry->entry.longval;
        }
    } else if (entry->li->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned int slen;
        vstr = lpGetValue(entry->lpe, &slen, lval);
        *vlen = slen;
    } else {
        serverPanic("Unknown list encoding");
    }
    return vstr;
}

/* 将迭代器当前位置的条目封装为 robj 对象并返回。 */
robj *listTypeGet(listTypeEntry *entry) {
    unsigned char *vstr;
    size_t vlen;
    long long lval;

    vstr = listTypeGetValue(entry, &vlen, &lval);
    if (vstr)
        return createStringObject((char *)vstr, vlen);
    else
        return createStringObjectFromLongLong(lval);
}

/* 在迭代器当前位置的前方或后方插入一个元素（由 where 决定）。 */
void listTypeInsert(listTypeEntry *entry, robj *value, int where) {
    robj *subject = entry->li->subject;
    value = getDecodedObject(value);
    sds str = value->ptr;
    size_t len = sdslen(str);

    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        if (where == LIST_TAIL) {
            quicklistInsertAfter(entry->li->iter, &entry->entry, str, len);
        } else if (where == LIST_HEAD) {
            quicklistInsertBefore(entry->li->iter, &entry->entry, str, len);
        }
    } else if (entry->li->encoding == OBJ_ENCODING_LISTPACK) {
        int lpw = (where == LIST_TAIL) ? LP_AFTER : LP_BEFORE;
        subject->ptr = lpInsertString(subject->ptr, (unsigned char *)str,
                                      len, entry->lpe, lpw, &entry->lpe);
    } else {
        serverPanic("Unknown list encoding");
    }
    decrRefCount(value);
}

/* 替换迭代器当前位置的条目。 */
void listTypeReplace(listTypeEntry *entry, robj *value) {
    robj *subject = entry->li->subject;
    value = getDecodedObject(value);
    sds str = value->ptr;
    size_t len = sdslen(str);

    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistReplaceEntry(entry->li->iter, &entry->entry, str, len);
    } else if (entry->li->encoding == OBJ_ENCODING_LISTPACK) {
        subject->ptr = lpReplace(subject->ptr, &entry->lpe, (unsigned char *)str, len);
    } else {
        serverPanic("Unknown list encoding");
    }

    decrRefCount(value);
}

/* 将偏移量 'index' 处的条目替换为 'value'。
 *
 * 替换成功返回 1。
 * 替换失败且未发生任何修改时返回 0。
 */
int listTypeReplaceAtIndex(robj *o, int index, robj *value) {
    value = getDecodedObject(value);
    sds vstr = value->ptr;
    size_t vlen = sdslen(vstr);
    int replaced = 0;

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *ql = o->ptr;
        replaced = quicklistReplaceAtIndex(ql, index, vstr, vlen);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *p = lpSeek(o->ptr,index);
        if (p) {
            o->ptr = lpReplace(o->ptr, &p, (unsigned char *)vstr, vlen);
            replaced = 1;
        }
    } else {
        serverPanic("Unknown list encoding");
    }

    decrRefCount(value);
    return replaced;
}

/* 比较给定对象与迭代器当前位置的条目是否相等。 */
int listTypeEqual(listTypeEntry *entry, robj *o) {
    serverAssertWithInfo(NULL,o,sdsEncodedObject(o));
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistCompare(&entry->entry,o->ptr,sdslen(o->ptr));
    } else if (entry->li->encoding == OBJ_ENCODING_LISTPACK) {
        return lpCompare(entry->lpe,o->ptr,sdslen(o->ptr));
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* 删除迭代器当前指向的元素。 */
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistDelEntry(iter->iter, &entry->entry);
    } else if (entry->li->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *p = entry->lpe;
        iter->subject->ptr = lpDelete(iter->subject->ptr,p,&p);

        /* 根据迭代方向更新迭代器位置 */
        if (iter->direction == LIST_TAIL)
            iter->lpi = p;
        else {
            if (p) {
                iter->lpi = lpPrev(iter->subject->ptr,p);
            } else {
                /* 我们删除了最后一个元素，因此需要将
                 * 迭代器指向最后一个元素。 */
                iter->lpi = lpLast(iter->subject->ptr);
            }
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* COPY 命令的辅助函数。
 * 复制一个列表对象，并保证返回对象的编码与原对象相同。
 *
 * 返回对象的 refcount 始终被设置为 1
 */
robj *listTypeDup(robj *o) {
    robj *lobj;

    serverAssert(o->type == OBJ_LIST);

    switch (o->encoding) {
        case OBJ_ENCODING_LISTPACK:
            lobj = createObject(OBJ_LIST, lpDup(o->ptr));
            break;
        case OBJ_ENCODING_QUICKLIST:
            lobj = createObject(OBJ_LIST, quicklistDup(o->ptr));
            break;
        default:
            serverPanic("Unknown list encoding");
            break;
    }
    lobj->encoding = o->encoding;
    return lobj;
}

/* 从列表中删除指定范围的元素。 */
void listTypeDelRange(robj *subject, long start, long count) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistDelRange(subject->ptr, start, count);
    } else if (subject->encoding == OBJ_ENCODING_LISTPACK) {
        subject->ptr = lpDeleteRange(subject->ptr, start, count);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/*-----------------------------------------------------------------------------
 * List Commands（列表命令）
 *----------------------------------------------------------------------------*/

/* 实现 LPUSH/RPUSH/LPUSHX/RPUSHX 命令。
 * 'xx': 仅在键存在时执行推入操作。
 *
 * 时间复杂度：O(N)，N 为要推入的元素数量。
 */
void pushGenericCommand(client *c, int where, int xx) {
    int j;

    robj *lobj = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c,lobj,OBJ_LIST)) return;
    if (!lobj) {
        if (xx) {
            addReply(c, shared.czero);
            return;
        }

        lobj = createListListpackObject();
        dbAdd(c->db,c->argv[1],lobj);
    }

    listTypeTryConversionAppend(lobj,c->argv,2,c->argc-1,NULL,NULL);
    for (j = 2; j < c->argc; j++) {
        listTypePush(lobj,c->argv[j],where);
        server.dirty++;
    }

    addReplyLongLong(c, listTypeLength(lobj));

    char *event = (where == LIST_HEAD) ? "lpush" : "rpush";
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
}

/* LPUSH <key> <element> [<element> ...]
 * 将一个或多个元素从列表头部插入。
 * 时间复杂度：O(N)，N 为要推入的元素数量。
 */
void lpushCommand(client *c) {
    pushGenericCommand(c,LIST_HEAD,0);
}

/* RPUSH <key> <element> [<element> ...]
 * 将一个或多个元素从列表尾部插入。
 * 时间复杂度：O(N)，N 为要推入的元素数量。
 */
void rpushCommand(client *c) {
    pushGenericCommand(c,LIST_TAIL,0);
}

/* LPUSHX <key> <element> [<element> ...]
 * 仅当列表存在时，才从列表头部插入一个或多个元素。
 * 时间复杂度：O(N)，N 为要推入的元素数量。
 */
void lpushxCommand(client *c) {
    pushGenericCommand(c,LIST_HEAD,1);
}

/* RPUSHX <key> <element> [<element> ...]
 * 仅当列表存在时，才从列表尾部插入一个或多个元素。
 * 时间复杂度：O(N)，N 为要推入的元素数量。
 */
void rpushxCommand(client *c) {
    pushGenericCommand(c,LIST_TAIL,1);
}

/* LINSERT <key> (BEFORE|AFTER) <pivot> <element>
 * 在列表中 pivot 元素之前或之后插入一个新元素。
 * 时间复杂度：O(N)，N 为 pivot 距离列表头部的距离。
 */
void linsertCommand(client *c) {
    int where;
    robj *subject;
    listTypeIterator *iter;
    listTypeEntry entry;
    int inserted = 0;

    if (strcasecmp(c->argv[2]->ptr,"after") == 0) {
        where = LIST_TAIL;
    } else if (strcasecmp(c->argv[2]->ptr,"before") == 0) {
        where = LIST_HEAD;
    } else {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    if ((subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,subject,OBJ_LIST)) return;

    /* 此时尚不确定该值能否被插入，但我们不能在迭代器内部进行编码转换。
     * 也不希望遍历列表两次（一次判断是否能插入，一次执行实际插入），
     * 因此我们假设该值可以被插入，并在必要时将 listpack 转换为常规列表。 */
    listTypeTryConversionAppend(subject,c->argv,4,4,NULL,NULL);

    /* 从头到尾扫描以查找 pivot */
    iter = listTypeInitIterator(subject,0,LIST_TAIL);
    while (listTypeNext(iter,&entry)) {
        if (listTypeEqual(&entry,c->argv[3])) {
            listTypeInsert(&entry,c->argv[4],where);
            inserted = 1;
            break;
        }
    }
    listTypeReleaseIterator(iter);

    if (inserted) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,"linsert",
                            c->argv[1],c->db->id);
        server.dirty++;
    } else {
        /* 通知客户端插入失败 */
        addReplyLongLong(c,-1);
        return;
    }

    addReplyLongLong(c,listTypeLength(subject));
}

/* LLEN <key>
 * 返回列表的长度。
 * 时间复杂度：O(1)
 */
void llenCommand(client *c) {
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.czero);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    addReplyLongLong(c,listTypeLength(o));
}

/* LINDEX <key> <index>
 * 返回列表中指定索引的元素，支持负索引（-1 表示最后一个元素）。
 * 时间复杂度：O(N)，N 为到达索引需要遍历的元素数；头/尾元素为 O(1)。
 */
void lindexCommand(client *c) {
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    long index;

    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != C_OK))
        return;

    listTypeIterator *iter = listTypeInitIterator(o,index,LIST_TAIL);
    listTypeEntry entry;
    unsigned char *vstr;
    size_t vlen;
    long long lval;

    if (listTypeNext(iter,&entry)) {
        vstr = listTypeGetValue(&entry,&vlen,&lval);
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, lval);
        }
    } else {
        addReplyNull(c);
    }

    listTypeReleaseIterator(iter);
}

/* LSET <key> <index> <element>
 * 将列表中指定索引的元素设置为新值。
 * 时间复杂度：O(N)，N 为到达索引需要遍历的元素数；头/尾元素为 O(1)。
 */
void lsetCommand(client *c) {
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    long index;
    robj *value = c->argv[3];

    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != C_OK))
        return;

    listTypeTryConversionAppend(o,c->argv,3,3,NULL,NULL);
    if (listTypeReplaceAtIndex(o,index,value)) {
        /* 可能用大元素替换小元素，也可能反过来，但增长场景已在
         * 上方的 listTypeTryConversionAppend() 中处理，
         * 这里只需尝试收缩场景下的转换。 */
        listTypeTryConversion(o,LIST_CONV_SHRINKING,NULL,NULL);
        addReply(c,shared.ok);
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,"lset",c->argv[1],c->db->id);
        server.dirty++;
    } else {
        addReplyErrorObject(c,shared.outofrangeerr);
    }
}

/* 类似于 addListRangeReply 的辅助函数，更多细节见下文。
 * 不同之处在于这里返回的是嵌套数组，形式如下：
 * 1) keyname
 * 2) 1) element1
 *    2) element2
 *
 * 同时还会通过 listElementsRemoved 真正从列表中弹出这些元素。
 * server.dirty 和事件通知在该函数中维护。
 *
 * 'deleted' 是可选的输出参数，用于指示该键是否被本函数删除。
 */
void listPopRangeAndReplyWithKey(client *c, robj *o, robj *key, int where, long count, int signal, int *deleted) {
    long llen = listTypeLength(o);
    long rangelen = (count > llen) ? llen : count;
    long rangestart = (where == LIST_HEAD) ? 0 : -rangelen;
    long rangeend = (where == LIST_HEAD) ? rangelen - 1 : -1;
    int reverse = (where == LIST_HEAD) ? 0 : 1;

    /* 仅返回一次键名，并附带一个元素数组 */
    addReplyArrayLen(c, 2);
    addReplyBulk(c, key);
    addListRangeReply(c, o, rangestart, rangeend, reverse);

    /* 弹出这些元素 */
    listTypeDelRange(o, rangestart, rangelen);
    /* 维护事件通知与 dirty 计数 */
    listElementsRemoved(c, key, where, o, rangelen, signal, deleted);
}

/* 从 addListRangeReply() 中抽离出来的实现，用于对 quicklist 编码列表进行回复。
 * 之所以单独抽出来，是为了把方法体尽量写小，
 * 这样循环中的代码可以更好地被内联以提升性能。 */
void addListQuicklistRangeReply(client *c, robj *o, int from, int rangelen, int reverse) {
    /* 以 multi-bulk 回复形式返回结果 */
    addReplyArrayLen(c,rangelen);

    int direction = reverse ? AL_START_TAIL : AL_START_HEAD;
    quicklistIter *iter = quicklistGetIteratorAtIdx(o->ptr, direction, from);
    while(rangelen--) {
        quicklistEntry qe;
        serverAssert(quicklistNext(iter, &qe)); /* 数据损坏则失败 */
        if (qe.value) {
            addReplyBulkCBuffer(c,qe.value,qe.sz);
        } else {
            addReplyBulkLongLong(c,qe.longval);
        }
    }
    quicklistReleaseIterator(iter);
}

/* 从 addListRangeReply() 中抽离出来的实现，用于对 listpack 编码列表进行回复。
 * 之所以单独抽出来，是为了把方法体尽量写小，
 * 这样循环中的代码可以更好地被内联以提升性能。 */
void addListListpackRangeReply(client *c, robj *o, int from, int rangelen, int reverse) {
    unsigned char *p = lpSeek(o->ptr, from);
    unsigned char *vstr;
    unsigned int vlen;
    long long lval;

    /* 以 multi-bulk 回复形式返回结果 */
    addReplyArrayLen(c,rangelen);

    while(rangelen--) {
        serverAssert(p); /* 数据损坏则失败 */
        vstr = lpGetValue(p, &vlen, &lval);
        if (vstr) {
            addReplyBulkCBuffer(c,vstr,vlen);
        } else {
            addReplyBulkLongLong(c,lval);
        }
        p = reverse ? lpPrev(o->ptr,p) : lpNext(o->ptr,p);
    }
}

/* 将列表在包含起始、结束索引之间的区间以 multi-bulk 形式回复给客户端，
 * 支持负索引。注意：start 必须小于等于 end，否则会返回空数组。
 * 当 reverse 参数为非零时，回复会被反转，即按 end 到 start 顺序返回元素。
 */
void addListRangeReply(client *c, robj *o, long start, long end, int reverse) {
    long rangelen, llen = listTypeLength(o);

    /* 转换负索引 */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* 不变式：start >= 0，因此当 end < 0 时该判断为真。
     * 当 start > end 或 start >= 列表长度时，区间为空。 */
    if (start > end || start >= llen) {
        addReply(c,shared.emptyarray);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    int from = reverse ? end : start;
    if (o->encoding == OBJ_ENCODING_QUICKLIST)
        addListQuicklistRangeReply(c, o, from, rangelen, reverse);
    else if (o->encoding == OBJ_ENCODING_LISTPACK)
        addListListpackRangeReply(c, o, from, rangelen, reverse);
    else
        serverPanic("Unknown list encoding");
}

/* 列表元素弹出任务的辅助整理函数。
 *
 * 若 'signal' 为 0，则跳过调用 signalModifiedKey()。
 *
 * 'deleted' 是可选的输出参数，用于指示该键是否被本函数删除。
 */
void listElementsRemoved(client *c, robj *key, int where, robj *o, long count, int signal, int *deleted) {
    char *event = (where == LIST_HEAD) ? "lpop" : "rpop";

    notifyKeyspaceEvent(NOTIFY_LIST, event, key, c->db->id);
    if (listTypeLength(o) == 0) {
        if (deleted) *deleted = 1;

        dbDelete(c->db, key);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
    } else {
        listTypeTryConversion(o, LIST_CONV_SHRINKING, NULL, NULL);
        if (deleted) *deleted = 0;
    }
    if (signal) signalModifiedKey(c, c->db, key);
    server.dirty += count;
}

/* 实现 LPOP/RPOP 命令的通用弹出操作。
 * where 参数指定操作列表的哪一端。
 * 客户端命令的第三个参数（可选）为 count，表示一次弹出多个元素。
 */
void popGenericCommand(client *c, int where) {
    int hascount = (c->argc == 3);
    long count = 0;
    robj *value;

    if (c->argc > 3) {
        addReplyErrorArity(c);
        return;
    } else if (hascount) {
        /* 解析可选的 count 参数 */
        if (getPositiveLongFromObjectOrReply(c,c->argv[2],&count,NULL) != C_OK)
            return;
    }

    robj *o = lookupKeyWriteOrReply(c, c->argv[1], hascount ? shared.nullarray[c->resp]: shared.null[c->resp]);
    if (o == NULL || checkType(c, o, OBJ_LIST))
        return;

    if (hascount && !count) {
        /* 快速退出路径 */
        addReply(c,shared.emptyarray);
        return;
    }

    if (!count) {
        /* 弹出单个元素。这是 POP 最初的语义，以 bulk 字符串形式回复。 */
        value = listTypePop(o,where);
        serverAssert(value != NULL);
        addReplyBulk(c,value);
        decrRefCount(value);
        listElementsRemoved(c,c->argv[1],where,o,1,1,NULL);
    } else {
        /* 弹出一定范围的元素。这是 POP 命令新增的功能，
         *  以 multi-bulk 形式回复。 */
        long llen = listTypeLength(o);
        long rangelen = (count > llen) ? llen : count;
        long rangestart = (where == LIST_HEAD) ? 0 : -rangelen;
        long rangeend = (where == LIST_HEAD) ? rangelen - 1 : -1;
        int reverse = (where == LIST_HEAD) ? 0 : 1;

        addListRangeReply(c,o,rangestart,rangeend,reverse);
        listTypeDelRange(o,rangestart,rangelen);
        listElementsRemoved(c,c->argv[1],where,o,rangelen,1,NULL);
    }
}

/* 与 popGenericCommand 类似，但支持多个键。
 * 接收多个 key，最终只从其中一个 key 上弹出元素。
 *
 * 'numkeys' 键的数量。
 * 'count'   请求弹出的元素数量。
 *
 * 始终以数组形式回复。
 */
void mpopGenericCommand(client *c, robj **keys, int numkeys, int where, long count) {
    int j;
    robj *o;
    robj *key;

    for (j = 0; j < numkeys; j++) {
        key = keys[j];
        o = lookupKeyWrite(c->db, key);

        /* 键不存在，跳到下一个键 */
        if (o == NULL) continue;

        if (checkType(c, o, OBJ_LIST)) return;

        long llen = listTypeLength(o);
        /* 空列表，跳到下一个键 */
        if (llen == 0) continue;

        /* 以嵌套数组的形式弹出一定范围的元素 */
        listPopRangeAndReplyWithKey(c, o, key, where, count, 1, NULL);

        /* 在副本中改写为 [LR]POP COUNT */
        robj *count_obj = createStringObjectFromLongLong((count > llen) ? llen : count);
        rewriteClientCommandVector(c, 3,
                                   (where == LIST_HEAD) ? shared.lpop : shared.rpop,
                                   key, count_obj);
        decrRefCount(count_obj);
        return;
    }

    /* 看起来无法弹出任何元素 */
    addReplyNullArray(c);
}

/* LPOP <key> [count]
 * 移除并返回列表头部的元素。
 * 时间复杂度：O(N)，N 为弹出元素数量。
 */
void lpopCommand(client *c) {
    popGenericCommand(c,LIST_HEAD);
}

/* RPOP <key> [count]
 * 移除并返回列表尾部的元素。
 * 时间复杂度：O(N)，N 为弹出元素数量。
 */
void rpopCommand(client *c) {
    popGenericCommand(c,LIST_TAIL);
}

/* LRANGE <key> <start> <stop>
 * 返回列表中指定区间内的元素，支持负索引。
 * 时间复杂度：O(S+N)，S 为 start 偏移，N 为区间元素数量。
 */
void lrangeCommand(client *c) {
    robj *o;
    long start, end;

    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray)) == NULL
         || checkType(c,o,OBJ_LIST)) return;

    addListRangeReply(c,o,start,end,0);
}

/* LTRIM <key> <start> <stop>
 * 将列表只保留指定区间内的元素，其它元素被移除。
 * 时间复杂度：O(N)，N 为被删除的元素总数。
 */
void ltrimCommand(client *c) {
    robj *o;
    long start, end, llen, ltrim, rtrim;

    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.ok)) == NULL ||
        checkType(c,o,OBJ_LIST)) return;
    llen = listTypeLength(o);

    /* 转换负索引 */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* 不变式：start >= 0，因此当 end < 0 时该判断为真。
     * 当 start > end 或 start >= 列表长度时，区间为空。 */
    if (start > end || start >= llen) {
        /* start 越界或 start > end 时，列表将被清空 */
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* 删除列表元素以完成 trim 操作 */
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistDelRange(o->ptr,0,ltrim);
        quicklistDelRange(o->ptr,-rtrim,rtrim);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        o->ptr = lpDeleteRange(o->ptr,0,ltrim);
        o->ptr = lpDeleteRange(o->ptr,-rtrim,rtrim);
    } else {
        serverPanic("Unknown list encoding");
    }

    notifyKeyspaceEvent(NOTIFY_LIST,"ltrim",c->argv[1],c->db->id);
    if (listTypeLength(o) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    } else {
        listTypeTryConversion(o,LIST_CONV_SHRINKING,NULL,NULL);
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    server.dirty += (ltrim + rtrim);
    addReply(c,shared.ok);
}

/* LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
 *
 * "rank" 表示匹配项的位置，1 表示返回第一个匹配，2 表示返回第二个匹配，
 * 以此类推。默认值为 1。若为负数，含义相同，但搜索从列表尾部开始。
 *
 * 如果指定了 COUNT，则不再返回单个元素，而是返回最多 "num-matches" 个
 * 匹配元素的列表。COUNT 可以与 RANK 组合使用，以便仅返回从第 N 个匹配
 * 开始的元素。若 COUNT 为 0，则返回所有匹配元素。
 *
 * MAXLEN 指示命令最多扫描 len 个元素。若为 0（默认值），
 * 则会在必要时扫描列表中的所有元素。
 *
 * 返回的索引与 LINDEX 返回的语义一致：列表头部第一个元素索引为 0，依次类推。
 */
void lposCommand(client *c) {
    robj *o, *ele;
    ele = c->argv[2];
    int direction = LIST_TAIL;
    long rank = 1, count = -1, maxlen = 0; /* Count -1: option not given. */

    /* Parse the optional arguments. */
    for (int j = 3; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc-1)-j;

        if (!strcasecmp(opt,"RANK") && moreargs) {
            j++;
            if (getRangeLongFromObjectOrReply(c, c->argv[j], -LONG_MAX, LONG_MAX, &rank, NULL) != C_OK)
                return;
            if (rank == 0) {
                addReplyError(c,"RANK can't be zero: use 1 to start from "
                                "the first match, 2 from the second ... "
                                "or use negative to start from the end of the list");
                return;
            }
        } else if (!strcasecmp(opt,"COUNT") && moreargs) {
            j++;
            if (getPositiveLongFromObjectOrReply(c, c->argv[j], &count,
              "COUNT can't be negative") != C_OK)
                return;
        } else if (!strcasecmp(opt,"MAXLEN") && moreargs) {
            j++;
            if (getPositiveLongFromObjectOrReply(c, c->argv[j], &maxlen, 
              "MAXLEN can't be negative") != C_OK)
                return;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* A negative rank means start from the tail. */
    if (rank < 0) {
        rank = -rank;
        direction = LIST_HEAD;
    }

    /* We return NULL or an empty array if there is no such key (or
     * if we find no matches, depending on the presence of the COUNT option. */
    if ((o = lookupKeyRead(c->db,c->argv[1])) == NULL) {
        if (count != -1)
            addReply(c,shared.emptyarray);
        else
            addReply(c,shared.null[c->resp]);
        return;
    }
    if (checkType(c,o,OBJ_LIST)) return;

    /* If we got the COUNT option, prepare to emit an array. */
    void *arraylenptr = NULL;
    if (count != -1) arraylenptr = addReplyDeferredLen(c);

    /* Seek the element. */
    listTypeIterator *li;
    li = listTypeInitIterator(o,direction == LIST_HEAD ? -1 : 0,direction);
    listTypeEntry entry;
    long llen = listTypeLength(o);
    long index = 0, matches = 0, matchindex = -1, arraylen = 0;
    while (listTypeNext(li,&entry) && (maxlen == 0 || index < maxlen)) {
        if (listTypeEqual(&entry,ele)) {
            matches++;
            matchindex = (direction == LIST_TAIL) ? index : llen - index - 1;
            if (matches >= rank) {
                if (arraylenptr) {
                    arraylen++;
                    addReplyLongLong(c,matchindex);
                    if (count && matches-rank+1 >= count) break;
                } else {
                    break;
                }
            }
        }
        index++;
        matchindex = -1; /* Remember if we exit the loop without a match. */
    }
    listTypeReleaseIterator(li);

    /* Reply to the client. Note that arraylenptr is not NULL only if
     * the COUNT option was selected. */
    if (arraylenptr != NULL) {
        setDeferredArrayLen(c,arraylenptr,arraylen);
    } else {
        if (matchindex != -1)
            addReplyLongLong(c,matchindex);
        else
            addReply(c,shared.null[c->resp]);
    }
}

/* LREM <key> <count> <element> */
void lremCommand(client *c) {
    robj *subject, *obj;
    obj = c->argv[3];
    long toremove;
    long removed = 0;

    if (getRangeLongFromObjectOrReply(c, c->argv[2], -LONG_MAX, LONG_MAX, &toremove, NULL) != C_OK)
        return;

    subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero);
    if (subject == NULL || checkType(c,subject,OBJ_LIST)) return;

    listTypeIterator *li;
    if (toremove < 0) {
        toremove = -toremove;
        li = listTypeInitIterator(subject,-1,LIST_HEAD);
    } else {
        li = listTypeInitIterator(subject,0,LIST_TAIL);
    }

    listTypeEntry entry;
    while (listTypeNext(li,&entry)) {
        if (listTypeEqual(&entry,obj)) {
            listTypeDelete(li, &entry);
            server.dirty++;
            removed++;
            if (toremove && removed == toremove) break;
        }
    }
    listTypeReleaseIterator(li);

    if (removed) {
        notifyKeyspaceEvent(NOTIFY_LIST,"lrem",c->argv[1],c->db->id);
        if (listTypeLength(subject) == 0) {
            dbDelete(c->db,c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
        } else {
            listTypeTryConversion(subject,LIST_CONV_SHRINKING,NULL,NULL);
        }
        signalModifiedKey(c,c->db,c->argv[1]);
    }

    addReplyLongLong(c,removed);
}

void lmoveHandlePush(client *c, robj *dstkey, robj *dstobj, robj *value,
                     int where) {
    /* Create the list if the key does not exist */
    if (!dstobj) {
        dstobj = createListListpackObject();
        dbAdd(c->db,dstkey,dstobj);
    }
    listTypeTryConversionAppend(dstobj,&value,0,0,NULL,NULL);
    listTypePush(dstobj,value,where);
    signalModifiedKey(c,c->db,dstkey);
    notifyKeyspaceEvent(NOTIFY_LIST,
                        where == LIST_HEAD ? "lpush" : "rpush",
                        dstkey,
                        c->db->id);
    /* Always send the pushed value to the client. */
    addReplyBulk(c,value);
}

int getListPositionFromObjectOrReply(client *c, robj *arg, int *position) {
    if (strcasecmp(arg->ptr,"right") == 0) {
        *position = LIST_TAIL;
    } else if (strcasecmp(arg->ptr,"left") == 0) {
        *position = LIST_HEAD;
    } else {
        addReplyErrorObject(c,shared.syntaxerr);
        return C_ERR;
    }
    return C_OK;
}

robj *getStringObjectFromListPosition(int position) {
    if (position == LIST_HEAD) {
        return shared.left;
    } else {
        // LIST_TAIL
        return shared.right;
    }
}

void lmoveGenericCommand(client *c, int wherefrom, int whereto) {
    robj *sobj, *value;
    if ((sobj = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]))
        == NULL || checkType(c,sobj,OBJ_LIST)) return;

    if (listTypeLength(sobj) == 0) {
        /* This may only happen after loading very old RDB files. Recent
         * versions of Redis delete keys of empty lists. */
        addReplyNull(c);
    } else {
        robj *dobj = lookupKeyWrite(c->db,c->argv[2]);
        robj *touchedkey = c->argv[1];

        if (checkType(c,dobj,OBJ_LIST)) return;
        value = listTypePop(sobj,wherefrom);
        serverAssert(value); /* assertion for valgrind (avoid NPD) */
        lmoveHandlePush(c,c->argv[2],dobj,value,whereto);
        listElementsRemoved(c,touchedkey,wherefrom,sobj,1,1,NULL);

        /* listTypePop returns an object with its refcount incremented */
        decrRefCount(value);

        if (c->cmd->proc == blmoveCommand) {
            rewriteClientCommandVector(c,5,shared.lmove,
                                       c->argv[1],c->argv[2],c->argv[3],c->argv[4]);
        } else if (c->cmd->proc == brpoplpushCommand) {
            rewriteClientCommandVector(c,3,shared.rpoplpush,
                                       c->argv[1],c->argv[2]);
        }
    }
}

/* LMOVE <source> <destination> (LEFT|RIGHT) (LEFT|RIGHT) */
void lmoveCommand(client *c) {
    int wherefrom, whereto;
    if (getListPositionFromObjectOrReply(c,c->argv[3],&wherefrom)
        != C_OK) return;
    if (getListPositionFromObjectOrReply(c,c->argv[4],&whereto)
        != C_OK) return;
    lmoveGenericCommand(c, wherefrom, whereto);
}

/* This is the semantic of this command:
 *  RPOPLPUSH srclist dstlist:
 *    IF LLEN(srclist) > 0
 *      element = RPOP srclist
 *      LPUSH dstlist element
 *      RETURN element
 *    ELSE
 *      RETURN nil
 *    END
 *  END
 *
 * The idea is to be able to get an element from a list in a reliable way
 * since the element is not just returned but pushed against another list
 * as well. This command was originally proposed by Ezra Zygmuntowicz.
 */
void rpoplpushCommand(client *c) {
    lmoveGenericCommand(c, LIST_TAIL, LIST_HEAD);
}

/* Blocking RPOP/LPOP/LMPOP
 *
 * 'numkeys' is the number of keys.
 * 'timeout_idx' parameter position of block timeout.
 * 'where' LIST_HEAD for LEFT, LIST_TAIL for RIGHT.
 * 'count' is the number of elements requested to pop, or -1 for plain single pop.
 *
 * When count is -1, a reply of a single bulk-string will be used.
 * When count > 0, an array reply will be used. */
void blockingPopGenericCommand(client *c, robj **keys, int numkeys, int where, int timeout_idx, long count) {
    robj *o;
    robj *key;
    mstime_t timeout;
    int j;

    if (getTimeoutFromObjectOrReply(c,c->argv[timeout_idx],&timeout,UNIT_SECONDS)
        != C_OK) return;

    /* Traverse all input keys, we take action only based on one key. */
    for (j = 0; j < numkeys; j++) {
        key = keys[j];
        o = lookupKeyWrite(c->db, key);

        /* Non-existing key, move to next key. */
        if (o == NULL) continue;

        if (checkType(c, o, OBJ_LIST)) return;

        long llen = listTypeLength(o);
        /* Empty list, move to next key. */
        if (llen == 0) continue;

        if (count != -1) {
            /* BLMPOP, non empty list, like a normal [LR]POP with count option.
             * The difference here we pop a range of elements in a nested arrays way. */
            listPopRangeAndReplyWithKey(c, o, key, where, count, 1, NULL);

            /* Replicate it as [LR]POP COUNT. */
            robj *count_obj = createStringObjectFromLongLong((count > llen) ? llen : count);
            rewriteClientCommandVector(c, 3,
                                       (where == LIST_HEAD) ? shared.lpop : shared.rpop,
                                       key, count_obj);
            decrRefCount(count_obj);
            return;
        }

        /* Non empty list, this is like a normal [LR]POP. */
        robj *value = listTypePop(o,where);
        serverAssert(value != NULL);

        addReplyArrayLen(c,2);
        addReplyBulk(c,key);
        addReplyBulk(c,value);
        decrRefCount(value);
        listElementsRemoved(c,key,where,o,1,1,NULL);

        /* Replicate it as an [LR]POP instead of B[LR]POP. */
        rewriteClientCommandVector(c,2,
            (where == LIST_HEAD) ? shared.lpop : shared.rpop,
            key);
        return;
    }

    /* If we are not allowed to block the client, the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    if (c->flags & CLIENT_DENY_BLOCKING) {
        addReplyNullArray(c);
        return;
    }

    /* If the keys do not exist we must block */
    blockForKeys(c,BLOCKED_LIST,keys,numkeys,timeout,0);
}

/* BLPOP <key> [<key> ...] <timeout> */
void blpopCommand(client *c) {
    blockingPopGenericCommand(c,c->argv+1,c->argc-2,LIST_HEAD,c->argc-1,-1);
}

/* BRPOP <key> [<key> ...] <timeout> */
void brpopCommand(client *c) {
    blockingPopGenericCommand(c,c->argv+1,c->argc-2,LIST_TAIL,c->argc-1,-1);
}

void blmoveGenericCommand(client *c, int wherefrom, int whereto, mstime_t timeout) {
    robj *key = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c,key,OBJ_LIST)) return;

    if (key == NULL) {
        if (c->flags & CLIENT_DENY_BLOCKING) {
            /* Blocking against an empty list when blocking is not allowed
             * returns immediately. */
            addReplyNull(c);
        } else {
            /* The list is empty and the client blocks. */
            blockForKeys(c,BLOCKED_LIST,c->argv + 1,1,timeout,0);
        }
    } else {
        /* The list exists and has elements, so
         * the regular lmoveCommand is executed. */
        serverAssertWithInfo(c,key,listTypeLength(key) > 0);
        lmoveGenericCommand(c,wherefrom,whereto);
    }
}

/* BLMOVE <source> <destination> (LEFT|RIGHT) (LEFT|RIGHT) <timeout> */
void blmoveCommand(client *c) {
    mstime_t timeout;
    int wherefrom, whereto;
    if (getListPositionFromObjectOrReply(c,c->argv[3],&wherefrom)
        != C_OK) return;
    if (getListPositionFromObjectOrReply(c,c->argv[4],&whereto)
        != C_OK) return;
    if (getTimeoutFromObjectOrReply(c,c->argv[5],&timeout,UNIT_SECONDS)
        != C_OK) return;
    blmoveGenericCommand(c,wherefrom,whereto,timeout);
}

/* BRPOPLPUSH <source> <destination> <timeout> */
void brpoplpushCommand(client *c) {
    mstime_t timeout;
    if (getTimeoutFromObjectOrReply(c,c->argv[3],&timeout,UNIT_SECONDS)
        != C_OK) return;
    blmoveGenericCommand(c, LIST_TAIL, LIST_HEAD, timeout);
}

/* LMPOP/BLMPOP
 *
 * 'numkeys_idx' parameter position of key number.
 * 'is_block' this indicates whether it is a blocking variant. */
void lmpopGenericCommand(client *c, int numkeys_idx, int is_block) {
    long j;
    long numkeys = 0;      /* Number of keys. */
    int where = 0;         /* HEAD for LEFT, TAIL for RIGHT. */
    long count = -1;       /* Reply will consist of up to count elements, depending on the list's length. */

    /* Parse the numkeys. */
    if (getRangeLongFromObjectOrReply(c, c->argv[numkeys_idx], 1, LONG_MAX,
                                      &numkeys, "numkeys should be greater than 0") != C_OK)
        return;

    /* Parse the where. where_idx: the index of where in the c->argv. */
    long where_idx = numkeys_idx + numkeys + 1;
    if (where_idx >= c->argc) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
    if (getListPositionFromObjectOrReply(c, c->argv[where_idx], &where) != C_OK)
        return;

    /* Parse the optional arguments. */
    for (j = where_idx + 1; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc - 1) - j;

        if (count == -1 && !strcasecmp(opt, "COUNT") && moreargs) {
            j++;
            if (getRangeLongFromObjectOrReply(c, c->argv[j], 1, LONG_MAX,
                                              &count,"count should be greater than 0") != C_OK)
                return;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    if (count == -1) count = 1;

    if (is_block) {
        /* BLOCK. We will handle CLIENT_DENY_BLOCKING flag in blockingPopGenericCommand. */
        blockingPopGenericCommand(c, c->argv+numkeys_idx+1, numkeys, where, 1, count);
    } else {
        /* NON-BLOCK */
        mpopGenericCommand(c, c->argv+numkeys_idx+1, numkeys, where, count);
    }
}

/* LMPOP numkeys <key> [<key> ...] (LEFT|RIGHT) [COUNT count] */
void lmpopCommand(client *c) {
    lmpopGenericCommand(c, 1, 0);
}

/* BLMPOP timeout numkeys <key> [<key> ...] (LEFT|RIGHT) [COUNT count] */
void blmpopCommand(client *c) {
    lmpopGenericCommand(c, 2, 1);
}
