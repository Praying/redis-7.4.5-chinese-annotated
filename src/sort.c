/* SORT 命令及其辅助函数。
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */


#include "server.h"
#include "pqsort.h" /* 用于 SORT+LIMIT 的部分快速排序 */
#include <math.h> /* isnan() */
#include "cluster.h"

/* 从跳表中按排名获取元素的前置声明 */
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank);

/* 创建一个排序操作对象。
 * type: 操作类型（如 SORT_OP_GET）
 * pattern: 用于 GET/BY 的键模式字符串 */
redisSortOperation *createSortOperation(int type, robj *pattern) {
    redisSortOperation *so = zmalloc(sizeof(*so));
    so->type = type;
    so->pattern = pattern;
    return so;
}

/* 根据模式查找键的值，规则如下：
 *
 * 1) 将 pattern 中第一个 '*' 替换为 subst 的值。
 *
 * 2) 如果 pattern 包含 "->"，箭头左侧部分作为包含哈希的键名，
 *    箭头右侧部分作为哈希字段名，返回该字段的值。
 *
 * 3) 如果 pattern 等于 "#"，函数直接返回 subst 本身，
 *    从而支持 SORT key GET # 这种用法来直接获取集合/列表元素。
 *
 * 返回的对象（非 NULL 时）引用计数总是 +1。 */
robj *lookupKeyByPattern(redisDb *db, robj *pattern, robj *subst) {
    char *p, *f, *k;
    sds spat, ssub;
    robj *keyobj, *fieldobj = NULL, *o;
    int prefixlen, sublen, postfixlen, fieldlen;

    /* 如果 pattern 是 "#"，直接返回替换对象本身，
     * 以实现 "SORT ... GET #" 功能。 */
    spat = pattern->ptr;
    if (spat[0] == '#' && spat[1] == '\0') {
        incrRefCount(subst);
        return subst;
    }

    /* 替换对象可能有特殊编码，如果是则动态创建解码后的对象。
     * 否则 getDecodedObject 仅增加引用计数，后续会减少。 */
    subst = getDecodedObject(subst);
    ssub = subst->ptr;

    /* 如果 pattern 中没有 '*'，返回 NULL，
     * 因为 GET 一个固定的键名没有意义。 */
    p = strchr(spat,'*');
    if (!p) {
        decrRefCount(subst);
        return NULL;
    }

    /* 判断是否为哈希字段解引用（即 pattern 中包含 "->"）。 */
    if ((f = strstr(p+1, "->")) != NULL && *(f+2) != '\0') {
        fieldlen = sdslen(spat)-(f-spat)-2;
        fieldobj = createStringObject(f+2,fieldlen);
    } else {
        fieldlen = 0;
    }

    /* 执行 '*' 替换：拼接前缀 + 替换值 + 后缀 */
    prefixlen = p-spat;
    sublen = sdslen(ssub);
    postfixlen = sdslen(spat)-(prefixlen+1)-(fieldlen ? fieldlen+2 : 0);
    keyobj = createStringObject(NULL,prefixlen+sublen+postfixlen);
    k = keyobj->ptr;
    memcpy(k,spat,prefixlen);
    memcpy(k+prefixlen,ssub,sublen);
    memcpy(k+prefixlen+sublen,p+1,postfixlen);
    decrRefCount(subst); /* decodeObject() 增加了引用计数，此处减少 */

    /* 查找替换后的键 */
    o = lookupKeyRead(db, keyobj);
    if (o == NULL) goto noobj;

    if (fieldobj) {
        if (o->type != OBJ_HASH) goto noobj;

        /* 根据字段名从哈希中获取值。
         * 返回的对象是新对象，引用计数已增加。 */
        int isHashDeleted;
        o = hashTypeGetValueObject(db, o, fieldobj->ptr, HFE_LAZY_EXPIRE, &isHashDeleted);

        if (isHashDeleted)
            goto noobj;

    } else {
        if (o->type != OBJ_STRING) goto noobj;

        /* 本函数返回的所有对象都需要增加引用计数，
         * 由 sortCommand 负责减少。 */
        incrRefCount(o);
    }
    decrRefCount(keyobj);
    if (fieldobj) decrRefCount(fieldobj);
    return o;

noobj:
    decrRefCount(keyobj);
    if (fieldlen) decrRefCount(fieldobj);
    return NULL;
}

/* sortCompare() 被 sortCommand() 中的 qsort 调用。
 * 由于带额外参数的 qsort_r 不是标准函数（仅为 BSD 扩展），
 * 因此通过全局 server 结构体传递排序参数。 */
int sortCompare(const void *s1, const void *s2) {
    const redisSortObject *so1 = s1, *so2 = s2;
    int cmp;

    if (!server.sort_alpha) {
        /* 数值排序：直接比较预计算的分数值 */
        if (so1->u.score > so2->u.score) {
            cmp = 1;
        } else if (so1->u.score < so2->u.score) {
            cmp = -1;
        } else {
            /* 分数相同时，按字典序比较以保证结果确定性 */
            cmp = compareStringObjects(so1->obj,so2->obj);
        }
    } else {
        /* 字母排序 */
        if (server.sort_bypattern) {
            if (!so1->u.cmpobj || !so2->u.cmpobj) {
                /* 至少一个比较对象为 NULL */
                if (so1->u.cmpobj == so2->u.cmpobj)
                    cmp = 0;
                else if (so1->u.cmpobj == NULL)
                    cmp = -1;
                else
                    cmp = 1;
            } else {
                /* 两个对象都存在，进行比较 */
                if (server.sort_store) {
                    cmp = compareStringObjects(so1->u.cmpobj,so2->u.cmpobj);
                } else {
                    /* 可直接使用 strcoll()，因为对象已确认为解码后的字符串 */
                    cmp = strcoll(so1->u.cmpobj->ptr,so2->u.cmpobj->ptr);
                }
            }
        } else {
            /* 直接比较元素 */
            if (server.sort_store) {
                cmp = compareStringObjects(so1->obj,so2->obj);
            } else {
                cmp = collateStringObjects(so1->obj,so2->obj);
            }
        }
    }
    /* 如果是降序排列，反转比较结果 */
    return server.sort_desc ? -cmp : cmp;
}

/* The SORT command is the most complex command in Redis. Warning: this code
 * is optimized for speed and a bit less for readability */
void sortCommandGeneric(client *c, int readonly) {
    list *operations;
    unsigned int outputlen = 0;
    int desc = 0, alpha = 0;
    long limit_start = 0, limit_count = -1, start, end;
    int j, dontsort = 0, vectorlen;
    int getop = 0; /* GET operation counter */
    int int_conversion_error = 0;
    int syntax_error = 0;
    robj *sortval, *sortby = NULL, *storekey = NULL;
    redisSortObject *vector; /* Resulting vector to sort */
    int user_has_full_key_access = 0; /* ACL - used in order to verify 'get' and 'by' options can be used */
    /* Create a list of operations to perform for every sorted element.
     * Operations can be GET */
    operations = listCreate();
    listSetFreeMethod(operations,zfree);
    j = 2; /* options start at argv[2] */

    user_has_full_key_access = ACLUserCheckCmdWithUnrestrictedKeyAccess(c->user, c->cmd, c->argv, c->argc, CMD_KEY_ACCESS);

    /* The SORT command has an SQL-alike syntax, parse it */
    while(j < c->argc) {
        int leftargs = c->argc-j-1;
        if (!strcasecmp(c->argv[j]->ptr,"asc")) {
            desc = 0;
        } else if (!strcasecmp(c->argv[j]->ptr,"desc")) {
            desc = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"alpha")) {
            alpha = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"limit") && leftargs >= 2) {
            if ((getLongFromObjectOrReply(c, c->argv[j+1], &limit_start, NULL)
                 != C_OK) ||
                (getLongFromObjectOrReply(c, c->argv[j+2], &limit_count, NULL)
                 != C_OK))
            {
                syntax_error++;
                break;
            }
            j+=2;
        } else if (readonly == 0 && !strcasecmp(c->argv[j]->ptr,"store") && leftargs >= 1) {
            storekey = c->argv[j+1];
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr,"by") && leftargs >= 1) {
            sortby = c->argv[j+1];
            /* If the BY pattern does not contain '*', i.e. it is constant,
             * we don't need to sort nor to lookup the weight keys. */
            if (strchr(c->argv[j+1]->ptr,'*') == NULL) {
                dontsort = 1;
            } else {
                /* If BY is specified with a real pattern, we can't accept it in cluster mode,
                 * unless we can make sure the keys formed by the pattern are in the same slot 
                 * as the key to sort. */
                if (server.cluster_enabled && patternHashSlot(sortby->ptr, sdslen(sortby->ptr)) != getKeySlot(c->argv[1]->ptr)) {
                    addReplyError(c, "BY option of SORT denied in Cluster mode when "
                                 "keys formed by the pattern may be in different slots.");
                    syntax_error++;
                    break;
                }
                /* If BY is specified with a real pattern, we can't accept
                 * it if no full ACL key access is applied for this command. */
                if (!user_has_full_key_access) {
                    addReplyError(c,"BY option of SORT denied due to insufficient ACL permissions.");
                    syntax_error++;
                    break;
                }
            }
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr,"get") && leftargs >= 1) {
            /* If GET is specified with a real pattern, we can't accept it in cluster mode,
             * unless we can make sure the keys formed by the pattern are in the same slot 
             * as the key to sort. The pattern # represents the key itself, so just skip
             * pattern slot check. */
            if (server.cluster_enabled &&
                strcmp(c->argv[j+1]->ptr, "#") &&
                patternHashSlot(c->argv[j+1]->ptr, sdslen(c->argv[j+1]->ptr)) != getKeySlot(c->argv[1]->ptr))
            {
                addReplyError(c, "GET option of SORT denied in Cluster mode when "
                              "keys formed by the pattern may be in different slots.");
                syntax_error++;
                break;
            }
            if (!user_has_full_key_access) {
                addReplyError(c,"GET option of SORT denied due to insufficient ACL permissions.");
                syntax_error++;
                break;
            }
            listAddNodeTail(operations,createSortOperation(
                SORT_OP_GET,c->argv[j+1]));
            getop++;
            j++;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            syntax_error++;
            break;
        }
        j++;
    }

    /* Handle syntax errors set during options parsing. */
    if (syntax_error) {
        listRelease(operations);
        return;
    }

    /* Lookup the key to sort. It must be of the right types */
    sortval = lookupKeyRead(c->db, c->argv[1]);
    if (sortval && sortval->type != OBJ_SET &&
                   sortval->type != OBJ_LIST &&
                   sortval->type != OBJ_ZSET)
    {
        listRelease(operations);
        addReplyErrorObject(c,shared.wrongtypeerr);
        return;
    }

    /* Now we need to protect sortval incrementing its count, in the future
     * SORT may have options able to overwrite/delete keys during the sorting
     * and the sorted key itself may get destroyed */
    if (sortval)
        incrRefCount(sortval);
    else
        sortval = createQuicklistObject(server.list_max_listpack_size, server.list_compress_depth);

    /* When sorting a set with no sort specified, we must sort the output
     * so the result is consistent across scripting and replication.
     *
     * The other types (list, sorted set) will retain their native order
     * even if no sort order is requested, so they remain stable across
     * scripting and replication. */
    if (dontsort &&
        sortval->type == OBJ_SET &&
        (storekey || c->flags & CLIENT_SCRIPT))
    {
        /* Force ALPHA sorting */
        dontsort = 0;
        alpha = 1;
        sortby = NULL;
    }

    /* Destructively convert encoded sorted sets for SORT. */
    if (sortval->type == OBJ_ZSET)
        zsetConvert(sortval, OBJ_ENCODING_SKIPLIST);

    /* Obtain the length of the object to sort. */
    switch(sortval->type) {
    case OBJ_LIST: vectorlen = listTypeLength(sortval); break;
    case OBJ_SET: vectorlen =  setTypeSize(sortval); break;
    case OBJ_ZSET: vectorlen = dictSize(((zset*)sortval->ptr)->dict); break;
    default: vectorlen = 0; serverPanic("Bad SORT type"); /* Avoid GCC warning */
    }

    /* Perform LIMIT start,count sanity checking.
     * And avoid integer overflow by limiting inputs to object sizes. */
    start = min(max(limit_start, 0), vectorlen);
    limit_count = min(max(limit_count, -1), vectorlen);
    end = (limit_count < 0) ? vectorlen-1 : start+limit_count-1;
    if (start >= vectorlen) {
        start = vectorlen-1;
        end = vectorlen-2;
    }
    if (end >= vectorlen) end = vectorlen-1;

    /* Whenever possible, we load elements into the output array in a more
     * direct way. This is possible if:
     *
     * 1) The object to sort is a sorted set or a list (internally sorted).
     * 2) There is nothing to sort as dontsort is true (BY <constant string>).
     *
     * In this special case, if we have a LIMIT option that actually reduces
     * the number of elements to fetch, we also optimize to just load the
     * range we are interested in and allocating a vector that is big enough
     * for the selected range length. */
    if ((sortval->type == OBJ_ZSET || sortval->type == OBJ_LIST) &&
        dontsort &&
        (start != 0 || end != vectorlen-1))
    {
        vectorlen = end-start+1;
    }

    /* Load the sorting vector with all the objects to sort */
    vector = zmalloc(sizeof(redisSortObject)*vectorlen);
    j = 0;

    if (sortval->type == OBJ_LIST && dontsort) {
        /* Special handling for a list, if 'dontsort' is true.
         * This makes sure we return elements in the list original
         * ordering, accordingly to DESC / ASC options.
         *
         * Note that in this case we also handle LIMIT here in a direct
         * way, just getting the required range, as an optimization. */
        if (end >= start) {
            listTypeIterator *li;
            listTypeEntry entry;
            li = listTypeInitIterator(sortval,
                    desc ? (long)(listTypeLength(sortval) - start - 1) : start,
                    desc ? LIST_HEAD : LIST_TAIL);

            while(j < vectorlen && listTypeNext(li,&entry)) {
                vector[j].obj = listTypeGet(&entry);
                vector[j].u.score = 0;
                vector[j].u.cmpobj = NULL;
                j++;
            }
            listTypeReleaseIterator(li);
            /* Fix start/end: output code is not aware of this optimization. */
            end -= start;
            start = 0;
        }
    } else if (sortval->type == OBJ_LIST) {
        listTypeIterator *li = listTypeInitIterator(sortval,0,LIST_TAIL);
        listTypeEntry entry;
        while(listTypeNext(li,&entry)) {
            vector[j].obj = listTypeGet(&entry);
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        listTypeReleaseIterator(li);
    } else if (sortval->type == OBJ_SET) {
        setTypeIterator *si = setTypeInitIterator(sortval);
        sds sdsele;
        while((sdsele = setTypeNextObject(si)) != NULL) {
            vector[j].obj = createObject(OBJ_STRING,sdsele);
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        setTypeReleaseIterator(si);
    } else if (sortval->type == OBJ_ZSET && dontsort) {
        /* Special handling for a sorted set, if 'dontsort' is true.
         * This makes sure we return elements in the sorted set original
         * ordering, accordingly to DESC / ASC options.
         *
         * Note that in this case we also handle LIMIT here in a direct
         * way, just getting the required range, as an optimization. */

        zset *zs = sortval->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        sds sdsele;
        int rangelen = vectorlen;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        if (desc) {
            long zsetlen = dictSize(((zset*)sortval->ptr)->dict);

            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,zsetlen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

        while(rangelen--) {
            serverAssertWithInfo(c,sortval,ln != NULL);
            sdsele = ln->ele;
            vector[j].obj = createStringObject(sdsele,sdslen(sdsele));
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
            ln = desc ? ln->backward : ln->level[0].forward;
        }
        /* Fix start/end: output code is not aware of this optimization. */
        end -= start;
        start = 0;
    } else if (sortval->type == OBJ_ZSET) {
        dict *set = ((zset*)sortval->ptr)->dict;
        dictIterator *di;
        dictEntry *setele;
        sds sdsele;
        di = dictGetIterator(set);
        while((setele = dictNext(di)) != NULL) {
            sdsele =  dictGetKey(setele);
            vector[j].obj = createStringObject(sdsele,sdslen(sdsele));
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        dictReleaseIterator(di);
    } else {
        serverPanic("Unknown type");
    }
    serverAssertWithInfo(c,sortval,j == vectorlen);

    /* Now it's time to load the right scores in the sorting vector */
    if (!dontsort) {
        for (j = 0; j < vectorlen; j++) {
            robj *byval;
            if (sortby) {
                /* lookup value to sort by */
                byval = lookupKeyByPattern(c->db,sortby,vector[j].obj);
                if (!byval) continue;
            } else {
                /* use object itself to sort by */
                byval = vector[j].obj;
            }

            if (alpha) {
                if (sortby) vector[j].u.cmpobj = getDecodedObject(byval);
            } else {
                if (sdsEncodedObject(byval)) {
                    char *eptr;

                    vector[j].u.score = strtod(byval->ptr,&eptr);
                    if (eptr[0] != '\0' || errno == ERANGE ||
                        isnan(vector[j].u.score))
                    {
                        int_conversion_error = 1;
                    }
                } else if (byval->encoding == OBJ_ENCODING_INT) {
                    /* Don't need to decode the object if it's
                     * integer-encoded (the only encoding supported) so
                     * far. We can just cast it */
                    vector[j].u.score = (long)byval->ptr;
                } else {
                    serverAssertWithInfo(c,sortval,1 != 1);
                }
            }

            /* when the object was retrieved using lookupKeyByPattern,
             * its refcount needs to be decreased. */
            if (sortby) {
                decrRefCount(byval);
            }
        }

        server.sort_desc = desc;
        server.sort_alpha = alpha;
        server.sort_bypattern = sortby ? 1 : 0;
        server.sort_store = storekey ? 1 : 0;
        if (sortby && (start != 0 || end != vectorlen-1))
            pqsort(vector,vectorlen,sizeof(redisSortObject),sortCompare, start,end);
        else
            qsort(vector,vectorlen,sizeof(redisSortObject),sortCompare);
    }

    /* Send command output to the output buffer, performing the specified
     * GET/DEL/INCR/DECR operations if any. */
    outputlen = getop ? getop*(end-start+1) : end-start+1;
    if (int_conversion_error) {
        addReplyError(c,"One or more scores can't be converted into double");
    } else if (storekey == NULL) {
        /* STORE option not specified, sent the sorting result to client */
        addReplyArrayLen(c,outputlen);
        for (j = start; j <= end; j++) {
            listNode *ln;
            listIter li;

            if (!getop) addReplyBulk(c,vector[j].obj);
            listRewind(operations,&li);
            while((ln = listNext(&li))) {
                redisSortOperation *sop = ln->value;
                robj *val = lookupKeyByPattern(c->db,sop->pattern,
                                               vector[j].obj);

                if (sop->type == SORT_OP_GET) {
                    if (!val) {
                        addReplyNull(c);
                    } else {
                        addReplyBulk(c,val);
                        decrRefCount(val);
                    }
                } else {
                    /* Always fails */
                    serverAssertWithInfo(c,sortval,sop->type == SORT_OP_GET);
                }
            }
        }
    } else {
        /* We can't predict the size and encoding of the stored list, we
         * assume it's a large list and then convert it at the end if needed. */
        robj *sobj = createQuicklistObject(server.list_max_listpack_size, server.list_compress_depth);

        /* STORE option specified, set the sorting result as a List object */
        for (j = start; j <= end; j++) {
            listNode *ln;
            listIter li;

            if (!getop) {
                listTypePush(sobj,vector[j].obj,LIST_TAIL);
            } else {
                listRewind(operations,&li);
                while((ln = listNext(&li))) {
                    redisSortOperation *sop = ln->value;
                    robj *val = lookupKeyByPattern(c->db,sop->pattern,
                                                   vector[j].obj);

                    if (sop->type == SORT_OP_GET) {
                        if (!val) val = createStringObject("",0);

                        /* listTypePush does an incrRefCount, so we should take care
                         * care of the incremented refcount caused by either
                         * lookupKeyByPattern or createStringObject("",0) */
                        listTypePush(sobj,val,LIST_TAIL);
                        decrRefCount(val);
                    } else {
                        /* Always fails */
                        serverAssertWithInfo(c,sortval,sop->type == SORT_OP_GET);
                    }
                }
            }
        }
        if (outputlen) {
            listTypeTryConversion(sobj,LIST_CONV_AUTO,NULL,NULL);
            setKey(c,c->db,storekey,sobj,0);
            notifyKeyspaceEvent(NOTIFY_LIST,"sortstore",storekey,
                                c->db->id);
            server.dirty += outputlen;
        } else if (dbDelete(c->db,storekey)) {
            signalModifiedKey(c,c->db,storekey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",storekey,c->db->id);
            server.dirty++;
        }
        decrRefCount(sobj);
        addReplyLongLong(c,outputlen);
    }

    /* Cleanup */
    for (j = 0; j < vectorlen; j++)
        decrRefCount(vector[j].obj);

    decrRefCount(sortval);
    listRelease(operations);
    for (j = 0; j < vectorlen; j++) {
        if (alpha && vector[j].u.cmpobj)
            decrRefCount(vector[j].u.cmpobj);
    }
    zfree(vector);
}

/* SORT wrapper function for read-only mode. */
void sortroCommand(client *c) {
    sortCommandGeneric(c, 1);
}

void sortCommand(client *c) {
    sortCommandGeneric(c, 0);
}
