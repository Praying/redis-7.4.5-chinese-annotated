/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "intset.h"  /* 紧凑型整数集合结构 */

/*-----------------------------------------------------------------------------
 * 集合（Set）相关命令
 *----------------------------------------------------------------------------*/

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op);

/* Factory method to return a set that *can* hold "value". When the object has
 * an integer-encodable value, an intset will be returned. Otherwise a listpack
 * or a regular hash table.
 *
 * The size hint indicates approximately how many items will be added which is
 * used to determine the initial representation. */
/* 工厂方法：返回一个能够容纳 "value" 的集合对象。
 * 如果对象对应的值可以编码为整数，则返回 intset；否则返回 listpack 或哈希表。
 *
 * size_hint 表示将要添加元素的大致数量，用于决定集合的初始表示形式。
 *
 * 时间复杂度：O(1) */
robj *setTypeCreate(sds value, size_t size_hint) {
    if (isSdsRepresentableAsLongLong(value,NULL) == C_OK && size_hint <= server.set_max_intset_entries)
        return createIntsetObject();
    if (size_hint <= server.set_max_listpack_entries)
        return createSetListpackObject();

    /* We may oversize the set by using the hint if the hint is not accurate,
     * but we will assume this is acceptable to maximize performance. */
    robj *o = createSetObject();
    dictExpand(o->ptr, size_hint);
    return o;
}

/* Check if the existing set should be converted to another encoding based off the
 * the size hint. */
/* 根据 size_hint 检查是否需要将已有集合转换为其他编码。
 *
 * 时间复杂度：O(1)（多数情况下），最坏情况下为 O(N) */
void setTypeMaybeConvert(robj *set, size_t size_hint) {
    if ((set->encoding == OBJ_ENCODING_LISTPACK && size_hint > server.set_max_listpack_entries)
        || (set->encoding == OBJ_ENCODING_INTSET && size_hint > server.set_max_intset_entries))
    {
        setTypeConvertAndExpand(set, OBJ_ENCODING_HT, size_hint, 1);
    }
}

/* Return the maximum number of entries to store in an intset. */
/* 返回 intset 所能存储的最大条目数。 */
static size_t intsetMaxEntries(void) {
    size_t max_entries = server.set_max_intset_entries;
    /* 由于 intset 内部实现限制，最多只能存储 1G（约 10 亿）个条目。 */
    if (max_entries >= 1<<30) max_entries = 1<<30;
    return max_entries;
}

/* Converts intset to HT if it contains too many entries. */
/* 如果 intset 包含过多条目，则将其转换为哈希表（HT）。 */
static void maybeConvertIntset(robj *subject) {
    serverAssert(subject->encoding == OBJ_ENCODING_INTSET);
    if (intsetLen(subject->ptr) > intsetMaxEntries())
        setTypeConvert(subject,OBJ_ENCODING_HT);
}

/* When you know all set elements are integers, call this to convert the set to
 * an intset. No conversion happens if the set contains too many entries for an
 * intset. */
/* 当确认集合中的所有元素都是整数时，调用此函数将集合转换为 intset。
 * 如果集合包含的元素数量过多（超过 intset 的限制），则不会进行转换。 */
static void maybeConvertToIntset(robj *set) {
    if (set->encoding == OBJ_ENCODING_INTSET) return; /* 已经是 intset */
    if (setTypeSize(set) > intsetMaxEntries()) return; /* 元素过多，不能使用 intset */
    intset *is = intsetNew();
    char *str;
    size_t len;
    int64_t llval;
    setTypeIterator *si = setTypeInitIterator(set);
    while (setTypeNext(si, &str, &len, &llval) != -1) {
        if (str) {
            /* 如果元素以字符串形式返回，我们可以尝试将其转换为整数。
             * 这种情况发生在 OBJ_ENCODING_HT 编码的集合中。 */
            serverAssert(string2ll(str, len, (long long *)&llval));
        }
        uint8_t success = 0;
        is = intsetAdd(is, llval, &success);
        serverAssert(success);
    }
    setTypeReleaseIterator(si);
    freeSetObject(set); /* 释放内部数据结构，但不释放 robj 本身 */
    set->ptr = is;
    set->encoding = OBJ_ENCODING_INTSET;
}

/* Add the specified sds value into a set.
 *
 * If the value was already member of the set, nothing is done and 0 is
 * returned, otherwise the new element is added and 1 is returned. */
/* 将指定的 sds 值添加到集合中。
 *
 * 如果该值已经是集合的成员，则什么也不做并返回 0；
 * 否则将新元素添加到集合中并返回 1。
 *
 * 时间复杂度：O(1) */
int setTypeAdd(robj *subject, sds value) {
    return setTypeAddAux(subject, value, sdslen(value), 0, 1);
}

/* Add member. This function is optimized for the different encodings. The
 * value can be provided as an sds string (indicated by passing str_is_sds =
 * 1), as string and length (str_is_sds = 0) or as an integer in which case str
 * is set to NULL and llval is provided instead.
 *
 * Returns 1 if the value was added and 0 if it was already a member. */
/* 添加成员。该函数针对不同编码进行了优化。
 * value 可以以 sds 字符串形式传入（str_is_sds = 1），
 * 也可以以字符串和长度形式传入（str_is_sds = 0），
 * 还可以以整数形式传入，此时 str 设为 NULL 并由 llval 提供值。
 *
 * 如果成功添加返回 1，如果该值已经是成员则返回 0。
 *
 * 时间复杂度：O(1) */
int setTypeAddAux(robj *set, char *str, size_t len, int64_t llval, int str_is_sds) {
    char tmpbuf[LONG_STR_SIZE];
    if (!str) {
        if (set->encoding == OBJ_ENCODING_INTSET) {
            uint8_t success = 0;
            set->ptr = intsetAdd(set->ptr, llval, &success);
            if (success) maybeConvertIntset(set);
            return success;
        }
        /* 将整数转换为字符串。 */
        len = ll2string(tmpbuf, sizeof tmpbuf, llval);
        str = tmpbuf;
        str_is_sds = 0;
    }

    serverAssert(str);
    if (set->encoding == OBJ_ENCODING_HT) {
        /* 如果已经是 sds 字符串则避免重复拷贝。 */
        sds sdsval = str_is_sds ? (sds)str : sdsnewlen(str, len);
        dict *ht = set->ptr;
        void *position = dictFindPositionForInsert(ht, sdsval, NULL);
        if (position) {
            /* 集合中不存在该键。添加它，但需要复制键以保证归属。 */
            if (sdsval == str) sdsval = sdsdup(sdsval);
            dictInsertAtPosition(ht, sdsval, position);
        } else if (sdsval != str) {
            /* 该字符串已经是集合成员。释放临时 sds 副本。 */
            sdsfree(sdsval);
        }
        return (position != NULL);
    } else if (set->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = set->ptr;
        unsigned char *p = lpFirst(lp);
        if (p != NULL)
            p = lpFind(lp, p, (unsigned char*)str, len, 0);
        if (p == NULL) {
            /* 未找到。 */
            if (lpLength(lp) < server.set_max_listpack_entries &&
                len <= server.set_max_listpack_value &&
                lpSafeToAdd(lp, len))
            {
                if (str == tmpbuf) {
                    /* 该值以整数形式传入，因此可以避免再次解析。
                     * TODO: 创建并使用 lpFindInteger，不再经过字符串中转。 */
                    lp = lpAppendInteger(lp, llval);
                } else {
                    lp = lpAppend(lp, (unsigned char*)str, len);
                }
                set->ptr = lp;
            } else {
                /* 已达到大小限制。转换为哈希表再添加。 */
                setTypeConvertAndExpand(set, OBJ_ENCODING_HT, lpLength(lp) + 1, 1);
                serverAssert(dictAdd(set->ptr,sdsnewlen(str,len),NULL) == DICT_OK);
            }
            return 1;
        }
    } else if (set->encoding == OBJ_ENCODING_INTSET) {
        long long value;
        if (string2ll(str, len, &value)) {
            uint8_t success = 0;
            set->ptr = intsetAdd(set->ptr,value,&success);
            if (success) {
                maybeConvertIntset(set);
                return 1;
            }
        } else {
            /* 检查 listpack 编码是否安全（不会越过任何阈值）。 */
            size_t maxelelen = 0, totsize = 0;
            unsigned long n = intsetLen(set->ptr);
            if (n != 0) {
                size_t elelen1 = sdigits10(intsetMax(set->ptr));
                size_t elelen2 = sdigits10(intsetMin(set->ptr));
                maxelelen = max(elelen1, elelen2);
                size_t s1 = lpEstimateBytesRepeatedInteger(intsetMax(set->ptr), n);
                size_t s2 = lpEstimateBytesRepeatedInteger(intsetMin(set->ptr), n);
                totsize = max(s1, s2);
            }
            if (intsetLen((const intset*)set->ptr) < server.set_max_listpack_entries &&
                len <= server.set_max_listpack_value &&
                maxelelen <= server.set_max_listpack_value &&
                lpSafeToAdd(NULL, totsize + len))
            {
                /* 上述 "safe to add" 检查中，我们假设 intset 中的所有元素大小均为 maxelelen。
                 * 这只是一个上界。 */
                setTypeConvertAndExpand(set, OBJ_ENCODING_LISTPACK,
                                        intsetLen(set->ptr) + 1, 1);
                unsigned char *lp = set->ptr;
                lp = lpAppend(lp, (unsigned char *)str, len);
                lp = lpShrinkToFit(lp);
                set->ptr = lp;
                return 1;
            } else {
                setTypeConvertAndExpand(set, OBJ_ENCODING_HT,
                                        intsetLen(set->ptr) + 1, 1);
                /* 集合原本是 intset，且该值无法编码为整数，
                 * 因此 dictAdd 应该总是成功。 */
                serverAssert(dictAdd(set->ptr,sdsnewlen(str,len),NULL) == DICT_OK);
                return 1;
            }
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* Deletes a value provided as an sds string from the set. Returns 1 if the
 * value was deleted and 0 if it was not a member of the set. */
/* 从集合中删除一个以 sds 字符串形式提供的值。
 * 如果值被成功删除则返回 1；如果该值不是集合成员则返回 0。
 *
 * 时间复杂度：O(1) */
int setTypeRemove(robj *setobj, sds value) {
    return setTypeRemoveAux(setobj, value, sdslen(value), 0, 1);
}

/* Remove a member. This function is optimized for the different encodings. The
 * value can be provided as an sds string (indicated by passing str_is_sds =
 * 1), as string and length (str_is_sds = 0) or as an integer in which case str
 * is set to NULL and llval is provided instead.
 *
 * Returns 1 if the value was deleted and 0 if it was not a member of the set. */
/* 删除成员。该函数针对不同编码进行了优化。
 * value 可以以 sds 字符串形式传入（str_is_sds = 1），
 * 也可以以字符串和长度形式传入（str_is_sds = 0），
 * 还可以以整数形式传入，此时 str 设为 NULL 并由 llval 提供值。
 *
 * 如果成功删除返回 1；如果该值不是集合成员则返回 0。
 *
 * 时间复杂度：O(1) */
int setTypeRemoveAux(robj *setobj, char *str, size_t len, int64_t llval, int str_is_sds) {
    char tmpbuf[LONG_STR_SIZE];
    if (!str) {
        if (setobj->encoding == OBJ_ENCODING_INTSET) {
            int success;
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
            return success;
        }
        len = ll2string(tmpbuf, sizeof tmpbuf, llval);
        str = tmpbuf;
        str_is_sds = 0;
    }

    if (setobj->encoding == OBJ_ENCODING_HT) {
        sds sdsval = str_is_sds ? (sds)str : sdsnewlen(str, len);
        int deleted = (dictDelete(setobj->ptr, sdsval) == DICT_OK);
        if (sdsval != str) sdsfree(sdsval); /* 释放临时副本 */
        return deleted;
    } else if (setobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = setobj->ptr;
        unsigned char *p = lpFirst(lp);
        if (p == NULL) return 0;
        p = lpFind(lp, p, (unsigned char*)str, len, 0);
        if (p != NULL) {
            lp = lpDelete(lp, p, NULL);
            setobj->ptr = lp;
            return 1;
        }
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        long long llval;
        if (string2ll(str, len, &llval)) {
            int success;
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
            if (success) return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* Check if an sds string is a member of the set. Returns 1 if the value is a
 * member of the set and 0 if it isn't. */
/* 检查给定的 sds 字符串是否为集合的成员。
 * 如果是成员则返回 1，否则返回 0。
 *
 * 时间复杂度：O(1) */
int setTypeIsMember(robj *subject, sds value) {
    return setTypeIsMemberAux(subject, value, sdslen(value), 0, 1);
}

/* Membership checking optimized for the different encodings. The value can be
 * provided as an sds string (indicated by passing str_is_sds = 1), as string
 * and length (str_is_sds = 0) or as an integer in which case str is set to NULL
 * and llval is provided instead.
 *
 * Returns 1 if the value is a member of the set and 0 if it isn't. */
/* 成员检查，针对不同编码进行了优化。
 * value 可以以 sds 字符串形式传入（str_is_sds = 1），
 * 也可以以字符串和长度形式传入（str_is_sds = 0），
 * 还可以以整数形式传入，此时 str 设为 NULL 并由 llval 提供值。
 *
 * 如果是成员则返回 1，否则返回 0。
 *
 * 时间复杂度：O(1) */
int setTypeIsMemberAux(robj *set, char *str, size_t len, int64_t llval, int str_is_sds) {
    char tmpbuf[LONG_STR_SIZE];
    if (!str) {
        if (set->encoding == OBJ_ENCODING_INTSET)
            return intsetFind(set->ptr, llval);
        len = ll2string(tmpbuf, sizeof tmpbuf, llval);
        str = tmpbuf;
        str_is_sds = 0;
    }

    if (set->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = set->ptr;
        unsigned char *p = lpFirst(lp);
        return p && lpFind(lp, p, (unsigned char*)str, len, 0);
    } else if (set->encoding == OBJ_ENCODING_INTSET) {
        long long llval;
        return string2ll(str, len, &llval) && intsetFind(set->ptr, llval);
    } else if (set->encoding == OBJ_ENCODING_HT && str_is_sds) {
        return dictFind(set->ptr, (sds)str) != NULL;
    } else if (set->encoding == OBJ_ENCODING_HT) {
        sds sdsval = sdsnewlen(str, len);
        int result = dictFind(set->ptr, sdsval) != NULL;
        sdsfree(sdsval);
        return result;
    } else {
        serverPanic("Unknown set encoding");
    }
}

setTypeIterator *setTypeInitIterator(robj *subject) {
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
    si->subject = subject;
    si->encoding = subject->encoding;
    if (si->encoding == OBJ_ENCODING_HT) {
        si->di = dictGetIterator(subject->ptr);
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        si->ii = 0;
    } else if (si->encoding == OBJ_ENCODING_LISTPACK) {
        si->lpi = NULL;
    } else {
        serverPanic("Unknown set encoding");
    }
    return si;
}

void setTypeReleaseIterator(setTypeIterator *si) {
    if (si->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(si->di);
    zfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position, as a string or as an integer.
 *
 * Since set elements can be internally be stored as SDS strings, char buffers or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointers
 * (str and len) or (llele) depending on whether the value is stored as a string
 * or as an integer internally.
 *
 * If OBJ_ENCODING_HT is returned, then str points to an sds string and can be
 * used as such. If OBJ_ENCODING_INTSET, then llele is populated and str is
 * pointed to NULL. If OBJ_ENCODING_LISTPACK is returned, the value can be
 * either a string or an integer. If *str is not NULL, then str and len are
 * populated with the string content and length. Otherwise, llele populated with
 * an integer value.
 *
 * Note that str, len and llele pointers should all be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 *
 * When there are no more elements -1 is returned. */
/* 移动到集合中的下一个条目，并以字符串或整数形式返回当前位置的对象。
 *
 * 由于集合元素内部可能以 SDS 字符串、char 缓冲区或简单的整数数组存储，
 * setTypeNext 会返回正在迭代的集合对象的编码，并根据内部值是以字符串
 * 还是整数存储，分别填充合适的指针（str 和 len）或（llele）。
 *
 * 如果返回 OBJ_ENCODING_HT，则 str 指向一个 sds 字符串，可以直接使用。
 * 如果返回 OBJ_ENCODING_INTSET，则 llele 被填充，str 设为 NULL。
 * 如果返回 OBJ_ENCODING_LISTPACK，则值既可以是字符串也可以是整数。
 *   如果 *str 不为 NULL，则 str 和 len 会被填充为字符串内容和长度。
 *   否则，llele 会被填充为整数值。
 *
 * 注意：str、len 和 llele 这几个指针都必须传入，且不能为 NULL，
 * 因为函数会对未使用的字段防御性地填充容易识别的默认值。
 *
 * 当没有更多元素时返回 -1。
 *
 * 时间复杂度：O(1) */
int setTypeNext(setTypeIterator *si, char **str, size_t *len, int64_t *llele) {
    if (si->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictNext(si->di);
        if (de == NULL) return -1;
        *str = dictGetKey(de);
        *len = sdslen(*str);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
            return -1;
        *str = NULL; /* 不需要。防御性设置。 */
    } else if (si->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = si->subject->ptr;
        unsigned char *lpi = si->lpi;
        if (lpi == NULL) {
            lpi = lpFirst(lp);
        } else {
            lpi = lpNext(lp, lpi);
        }
        if (lpi == NULL) return -1;
        si->lpi = lpi;
        unsigned int l;
        *str = (char *)lpGetValue(lpi, &l, (long long *)llele);
        *len = (size_t)l;
    } else {
        serverPanic("Wrong set encoding in setTypeNext");
    }
    return si->encoding;
}

/* The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new SDS
 * strings. So if you don't retain a pointer to this object you should call
 * sdsfree() against it.
 *
 * This function is the way to go for write operations where COW is not
 * an issue. */
/* 这不是对 copy-on-write（COW）友好的版本，但属于 setTypeNext() 的易用版本，
 * 它返回一个新的 SDS 字符串。因此，如果不打算保留该对象的指针，
 * 则应当对它调用 sdsfree() 进行释放。
 *
 * 对于不涉及 COW 问题的写操作，这是首选函数。 */
sds setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    char *str;
    size_t len;

    if (setTypeNext(si, &str, &len, &intele) == -1) return NULL;
    if (str != NULL) return sdsnewlen(str, len);
    return sdsfromlonglong(intele);
}

/* Return random element from a non empty set.
 * The returned element can be an int64_t value if the set is encoded
 * as an "intset" blob of integers, or an string.
 *
 * The caller provides three pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and can be used by the caller to check if the
 * int64_t pointer or the str and len pointers were populated, as for
 * setTypeNext. If OBJ_ENCODING_HT is returned, str is pointed to a
 * string which is actually an sds string and it can be used as such.
 *
 * Note that both the str, len and llele pointers should be passed and cannot
 * be NULL. If str is set to NULL, the value is an integer stored in llele. */
/* 从一个非空集合中返回一个随机元素。
 * 如果集合以 "intset" 整数块编码，则返回的元素可以是 int64_t 值；否则是一个字符串。
 *
 * 调用方提供三个指针用于填充正确的对象。函数返回值是对象 object->encoding 字段，
 * 调用方可以借此判断 int64_t 指针还是 str 和 len 指针被填充，这与 setTypeNext 一致。
 * 如果返回 OBJ_ENCODING_HT，str 指向一个实际为 sds 字符串的字符串，可以直接使用。
 *
 * 注意：str、len 和 llele 这几个指针都必须传入，且不能为 NULL。
 * 如果 str 被设置为 NULL，则表示该值是存储在 llele 中的整数。 */
int setTypeRandomElement(robj *setobj, char **str, size_t *len, int64_t *llele) {
    if (setobj->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictGetFairRandomKey(setobj->ptr);
        *str = dictGetKey(de);
        *len = sdslen(*str);
        *llele = -123456789; /* 不需要。防御性赋值。 */
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        *llele = intsetRandom(setobj->ptr);
        *str = NULL; /* 不需要。防御性赋值。 */
    } else if (setobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = setobj->ptr;
        int r = rand() % lpLength(lp);
        unsigned char *p = lpSeek(lp, r);
        unsigned int l;
        *str = (char *)lpGetValue(p, &l, (long long *)llele);
        *len = (size_t)l;
    } else {
        serverPanic("Unknown set encoding");
    }
    return setobj->encoding;
}

/* Pops a random element and returns it as an object. */
/* 弹出一个随机元素并以对象形式返回。 */
robj *setTypePopRandom(robj *set) {
    robj *obj;
    if (set->encoding == OBJ_ENCODING_LISTPACK) {
        /* 随机查找并直接删除，无需重新定位 listpack。 */
        unsigned int i = 0;
        unsigned char *p = lpNextRandom(set->ptr, lpFirst(set->ptr), &i, 1, 1);
        unsigned int len = 0; /* 初始化以避免编译警告 */
        long long llele = 0; /* 初始化以避免编译警告 */
        char *str = (char *)lpGetValue(p, &len, &llele);
        if (str)
            obj = createStringObject(str, len);
        else
            obj = createStringObjectFromLongLong(llele);
        set->ptr = lpDelete(set->ptr, p, NULL);
    } else {
        char *str;
        size_t len = 0;
        int64_t llele = 0;
        int encoding = setTypeRandomElement(set, &str, &len, &llele);
        if (str)
            obj = createStringObject(str, len);
        else
            obj = createStringObjectFromLongLong(llele);
        setTypeRemoveAux(set, str, len, llele, encoding == OBJ_ENCODING_HT);
    }
    return obj;
}

unsigned long setTypeSize(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        return dictSize((const dict*)subject->ptr);
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        return intsetLen((const intset*)subject->ptr);
    } else if (subject->encoding == OBJ_ENCODING_LISTPACK) {
        return lpLength((unsigned char *)subject->ptr);
    } else {
        serverPanic("Unknown set encoding");
    }
}

/* Convert the set to specified encoding. The resulting dict (when converting
 * to a hash table) is presized to hold the number of elements in the original
 * set. */
/* 将集合转换为指定的编码。当转换为哈希表时，目标 dict 会预先调整大小，
 * 以容纳原集合中的所有元素。
 *
 * 时间复杂度：O(N)，其中 N 是集合中元素的数量 */
void setTypeConvert(robj *setobj, int enc) {
    setTypeConvertAndExpand(setobj, enc, setTypeSize(setobj), 1);
}

/* Converts a set to the specified encoding, pre-sizing it for 'cap' elements.
 * The 'panic' argument controls whether to panic on OOM (panic=1) or return
 * C_ERR on OOM (panic=0). If panic=1 is given, this function always returns
 * C_OK. */
/* 将集合转换为指定编码，并预先调整为可容纳 'cap' 个元素的容量。
 * 'panic' 参数控制 OOM 时是触发 panic（panic=1），还是返回 C_ERR（panic=0）。
 * 当指定 panic=1 时，函数总是返回 C_OK。 */
int setTypeConvertAndExpand(robj *setobj, int enc, unsigned long cap, int panic) {
    setTypeIterator *si;
    serverAssertWithInfo(NULL,setobj,setobj->type == OBJ_SET &&
                             setobj->encoding != enc);

    if (enc == OBJ_ENCODING_HT) {
        dict *d = dictCreate(&setDictType);
        sds element;

        /* 预先调整 dict 容量以避免再哈希 */
        if (panic) {
            dictExpand(d, cap);
        } else if (dictTryExpand(d, cap) != DICT_OK) {
            dictRelease(d);
            return C_ERR;
        }

        /* 为了添加元素，我们提取整数并创建 redis 对象 */
        si = setTypeInitIterator(setobj);
        while ((element = setTypeNextObject(si)) != NULL) {
            serverAssert(dictAdd(d,element,NULL) == DICT_OK);
        }
        setTypeReleaseIterator(si);

        freeSetObject(setobj); /* 释放内部数据结构，但不释放 setobj 本身 */
        setobj->encoding = OBJ_ENCODING_HT;
        setobj->ptr = d;
    } else if (enc == OBJ_ENCODING_LISTPACK) {
        /* 预先为每个元素分配至少两个字节（enc/value + backlen） */
        size_t estcap = cap * 2;
        if (setobj->encoding == OBJ_ENCODING_INTSET && setTypeSize(setobj) > 0) {
            /* 如果是从 intset 转换而来，我们可以做出更精确的预估。 */
            size_t s1 = lpEstimateBytesRepeatedInteger(intsetMin(setobj->ptr), cap);
            size_t s2 = lpEstimateBytesRepeatedInteger(intsetMax(setobj->ptr), cap);
            estcap = max(s1, s2);
        }
        unsigned char *lp = lpNew(estcap);
        char *str;
        size_t len;
        int64_t llele;
        si = setTypeInitIterator(setobj);
        while (setTypeNext(si, &str, &len, &llele) != -1) {
            if (str != NULL)
                lp = lpAppend(lp, (unsigned char *)str, len);
            else
                lp = lpAppendInteger(lp, llele);
        }
        setTypeReleaseIterator(si);

        freeSetObject(setobj); /* 释放内部数据结构，但不释放 setobj 本身 */
        setobj->encoding = OBJ_ENCODING_LISTPACK;
        setobj->ptr = lp;
    } else {
        serverPanic("Unsupported set conversion");
    }
    return C_OK;
}

/* This is a helper function for the COPY command.
 * Duplicate a set object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */
/* 这是 COPY 命令的辅助函数。
 * 复制一个集合对象，并保证返回的对象与原对象具有相同的编码。
 *
 * 返回对象的 refcount 始终为 1 */
robj *setTypeDup(robj *o) {
    robj *set;
    setTypeIterator *si;

    serverAssert(o->type == OBJ_SET);

    /* 创建一个与原对象编码相同的新集合对象 */
    if (o->encoding == OBJ_ENCODING_INTSET) {
        intset *is = o->ptr;
        size_t size = intsetBlobLen(is);
        intset *newis = zmalloc(size);
        memcpy(newis,is,size);
        set = createObject(OBJ_SET, newis);
        set->encoding = OBJ_ENCODING_INTSET;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = o->ptr;
        size_t sz = lpBytes(lp);
        unsigned char *new_lp = zmalloc(sz);
        memcpy(new_lp, lp, sz);
        set = createObject(OBJ_SET, new_lp);
        set->encoding = OBJ_ENCODING_LISTPACK;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        set = createSetObject();
        dict *d = o->ptr;
        dictExpand(set->ptr, dictSize(d));
        si = setTypeInitIterator(o);
        char *str;
        size_t len;
        int64_t intobj;
        while (setTypeNext(si, &str, &len, &intobj) != -1) {
            setTypeAdd(set, (sds)str);
        }
        setTypeReleaseIterator(si);
    } else {
        serverPanic("Unknown set encoding");
    }
    return set;
}

/* SADD 命令实现。
 * 向集合添加一个或多个成员。
 * 返回新成功添加的成员数量（已存在的成员不计入）。
 *
 * 时间复杂度：每添加一个元素为 O(1) */
void saddCommand(client *c) {
    robj *set;
    int j, added = 0;

    set = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,set,OBJ_SET)) return;

    if (set == NULL) {
        /* 集合不存在，创建一个新集合 */
        set = setTypeCreate(c->argv[2]->ptr, c->argc - 2);
        dbAdd(c->db,c->argv[1],set);
    } else {
        setTypeMaybeConvert(set, c->argc - 2);
    }

    for (j = 2; j < c->argc; j++) {
        if (setTypeAdd(set,c->argv[j]->ptr)) added++;
    }
    if (added) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[1],c->db->id);
    }
    server.dirty += added;
    addReplyLongLong(c,added);
}

/* SREM 命令实现。
 * 从集合中删除一个或多个成员。
 * 返回实际被删除的成员数量。
 *
 * 时间复杂度：每删除一个元素为 O(1) */
void sremCommand(client *c) {
    robj *set;
    int j, deleted = 0, keyremoved = 0;

    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    for (j = 2; j < c->argc; j++) {
        if (setTypeRemove(set,c->argv[j]->ptr)) {
            deleted++;
            if (setTypeSize(set) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    if (deleted) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

/* SMOVE 命令实现。
 * 将成员从源集合移动到目标集合。
 * 成功移动返回 1，否则返回 0。
 *
 * 时间复杂度：O(1) */
void smoveCommand(client *c) {
    robj *srcset, *dstset, *ele;
    srcset = lookupKeyWrite(c->db,c->argv[1]);
    dstset = lookupKeyWrite(c->db,c->argv[2]);
    ele = c->argv[3];

    /* If the source key does not exist return 0 */
    /* 如果源键不存在，则返回 0 */
    if (srcset == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    /* 如果源键类型错误，或者目标键是集合类型但类型错误，返回错误。 */
    if (checkType(c,srcset,OBJ_SET) ||
        checkType(c,dstset,OBJ_SET)) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    /* 如果源集合和目标集合相同，SMOVE 实际上是一个空操作 */
    if (srcset == dstset) {
        addReply(c,setTypeIsMember(srcset,ele->ptr) ?
            shared.cone : shared.czero);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    /* 如果无法从源集合中删除该元素，返回 0。 */
    if (!setTypeRemove(srcset,ele->ptr)) {
        addReply(c,shared.czero);
        return;
    }
    notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);

    /* Remove the src set from the database when empty */
    /* 当源集合为空时，从数据库中删除该键 */
    if (setTypeSize(srcset) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Create the destination set when it doesn't exist */
    /* 当目标集合不存在时创建它 */
    if (!dstset) {
        dstset = setTypeCreate(ele->ptr, 1);
        dbAdd(c->db,c->argv[2],dstset);
    }

    signalModifiedKey(c,c->db,c->argv[1]);
    server.dirty++;

    /* An extra key has changed when ele was successfully added to dstset */
    /* 当元素成功添加到目标集合时，会多改动一个键 */
    if (setTypeAdd(dstset,ele->ptr)) {
        server.dirty++;
        signalModifiedKey(c,c->db,c->argv[2]);
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[2],c->db->id);
    }
    addReply(c,shared.cone);
}

/* SISMEMBER 命令实现。
 * 判断给定成员是否为集合的成员。
 * 是则返回 1，否则返回 0。
 *
 * 时间复杂度：O(1) */
void sismemberCommand(client *c) {
    robj *set;

    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    if (setTypeIsMember(set,c->argv[2]->ptr))
        addReply(c,shared.cone);
    else
        addReply(c,shared.czero);
}

/* SMISMEMBER 命令实现。
 * 批量判断多个成员是否属于集合。
 * 返回与请求成员数对应的 1/0 数组。
 *
 * 时间复杂度：每个成员 O(1) */
void smismemberCommand(client *c) {
    robj *set;
    int j;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * sets, where SMISMEMBER should respond with a series of zeros. */
    /* 键不存在时不要中断。不存在的键视为空集，
     * SMISMEMBER 应返回一串 0。 */
    set = lookupKeyRead(c->db,c->argv[1]);
    if (set && checkType(c,set,OBJ_SET)) return;

    addReplyArrayLen(c,c->argc - 2);

    for (j = 2; j < c->argc; j++) {
        if (set && setTypeIsMember(set,c->argv[j]->ptr))
            addReply(c,shared.cone);
        else
            addReply(c,shared.czero);
    }
}

/* SCARD 命令实现。
 * 返回集合中元素的数量（基数）。
 *
 * 时间复杂度：O(1) */
void scardCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_SET)) return;

    addReplyLongLong(c,setTypeSize(o));
}

/* Handle the "SPOP key <count>" variant. The normal version of the
 * command is handled by the spopCommand() function itself. */
/* 处理 "SPOP key <count>" 变体形式。
 * 普通版本的命令由 spopCommand() 函数自身处理。 */

/* How many times bigger should be the set compared to the remaining size
 * for us to use the "create new set" strategy? Read later in the
 * implementation for more info. */
/* 触发 "create new set" 策略时，集合大小相比剩余元素数的倍数阈值。
 * 阅读实现中的相关注释获取更多信息。 */
#define SPOP_MOVE_STRATEGY_MUL 5

/* SPOP key <count> 变体形式的处理函数。
 * 从集合中随机弹出 count 个元素并返回它们。
 *
 * 时间复杂度：O(N)，其中 N 为要弹出的元素数 */
void spopWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    robj *set;

    /* Get the count argument */
    /* 获取 count 参数 */
    if (getPositiveLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
    count = (unsigned long) l;

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set. Otherwise, return nil */
    /* 确保给定名称的键存在，并且类型确实是集合。否则返回 nil。 */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.emptyset[c->resp]))
        == NULL || checkType(c,set,OBJ_SET)) return;

    /* If count is zero, serve an empty set ASAP to avoid special
     * cases later. */
    /* 如果 count 为 0，立即返回空集，避免后续特殊处理。 */
    if (count == 0) {
        addReply(c,shared.emptyset[c->resp]);
        return;
    }

    size = setTypeSize(set);

    /* Generate an SPOP keyspace notification */
    /* 发送 SPOP 键空间通知 */
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);
    server.dirty += (count >= size) ? size : count;

    /* CASE 1:
     * The number of requested elements is greater than or equal to
     * the number of elements inside the set: simply return the whole set. */
    /* 情形 1：
     * 请求元素数量大于等于集合中的元素数量：直接返回整个集合。 */
    if (count >= size) {
        /* We just return the entire set */
        /* 直接返回整个集合 */
        sunionDiffGenericCommand(c,c->argv+1,1,NULL,SET_OP_UNION);

        /* Delete the set as it is now empty */
        /* 集合现在已空，删除该键 */
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);

        /* todo: Move the spop notification to be executed after the command logic. */
        /* todo: 将 spop 通知移到命令逻辑执行完毕后再发送。 */

        /* Propagate this command as a DEL or UNLINK operation */
        /* 将该命令以 DEL 或 UNLINK 形式进行传播 */
        robj *aux = server.lazyfree_lazy_server_del ? shared.unlink : shared.del;
        rewriteClientCommandVector(c, 2, aux, c->argv[1]);
        signalModifiedKey(c,c->db,c->argv[1]);
        return;
    }

    /* Case 2 and 3 require to replicate SPOP as a set of SREM commands.
     * Prepare our replication argument vector. Also send the array length
     * which is common to both the code paths. */
    /* 情形 2 和 情形 3 需要将 SPOP 复制为一组 SREM 命令。
     * 准备好复制参数向量，同时返回两种代码路径共用的数组长度。 */
    unsigned long batchsize = count > 1024 ? 1024 : count;
    robj **propargv = zmalloc(sizeof(robj *) * (2 + batchsize));
    propargv[0] = shared.srem;
    propargv[1] = c->argv[1];
    unsigned long propindex = 2;
    addReplySetLen(c,count);

    /* Common iteration vars. */
    /* 通用迭代变量。 */
    char *str;
    size_t len;
    int64_t llele;
    unsigned long remaining = size-count; /* Elements left after SPOP. */
                                         /* SPOP 后剩余的元素数。 */

    /* If we are here, the number of requested elements is less than the
     * number of elements inside the set. Also we are sure that count < size.
     * Use two different strategies.
     *
     * CASE 2: The number of elements to return is small compared to the
     * set size. We can just extract random elements and return them to
     * the set. */
    /* 如果走到这里，说明请求元素数小于集合元素数，
     * 并且 count < size。采用两种不同策略：
     *
     * 情形 2：要返回的元素数相对集合大小较小。
     *         只需随机抽取元素并返回。 */
    if (remaining*SPOP_MOVE_STRATEGY_MUL > count &&
        set->encoding == OBJ_ENCODING_LISTPACK)
    {
        /* Specialized case for listpack. Traverse it only once. */
        /* listpack 特化路径。只遍历一次。 */
        unsigned char *lp = set->ptr;
        unsigned char *p = lpFirst(lp);
        unsigned int index = 0;
        unsigned char **ps = zmalloc(sizeof(char *) * count);
        for (unsigned long i = 0; i < count; i++) {
            p = lpNextRandom(lp, p, &index, count - i, 1);
            unsigned int len;
            str = (char *)lpGetValue(p, &len, (long long *)&llele);

            if (str) {
                addReplyBulkCBuffer(c, str, len);
                propargv[propindex++] = createStringObject(str, len);
            } else {
                addReplyBulkLongLong(c, llele);
                propargv[propindex++] = createStringObjectFromLongLong(llele);
            }
            /* Replicate/AOF this command as an SREM operation */
            /* 以 SREM 操作形式复制/AOF */
            if (propindex == 2 + batchsize) {
                alsoPropagate(c->db->id, propargv, propindex, PROPAGATE_AOF | PROPAGATE_REPL);
                for (unsigned long j = 2; j < propindex; j++) {
                    decrRefCount(propargv[j]);
                }
                propindex = 2;
            }

            /* Store pointer for later deletion and move to next. */
            /* 保存指针用于后续删除，并向后移动。 */
            ps[i] = p;
            p = lpNext(lp, p);
            index++;
        }
        lp = lpBatchDelete(lp, ps, count);
        zfree(ps);
        set->ptr = lp;
    } else if (remaining*SPOP_MOVE_STRATEGY_MUL > count) {
        for (unsigned long i = 0; i < count; i++) {
            propargv[propindex] = setTypePopRandom(set);
            addReplyBulk(c, propargv[propindex]);
            propindex++;
            /* Replicate/AOF this command as an SREM operation */
            /* 以 SREM 操作形式复制/AOF */
            if (propindex == 2 + batchsize) {
                alsoPropagate(c->db->id, propargv, propindex, PROPAGATE_AOF | PROPAGATE_REPL);
                for (unsigned long j = 2; j < propindex; j++) {
                    decrRefCount(propargv[j]);
                }
                propindex = 2;
            }
        }
    } else {
    /* CASE 3: The number of elements to return is very big, approaching
     * the size of the set itself. After some time extracting random elements
     * from such a set becomes computationally expensive, so we use
     * a different strategy, we extract random elements that we don't
     * want to return (the elements that will remain part of the set),
     * creating a new set as we do this (that will be stored as the original
     * set). Then we return the elements left in the original set and
     * release it. */
    /* 情形 3：要返回的元素数量非常大，接近集合本身大小。
     * 持续从这种集合中随机抽取元素会变得计算昂贵，因此采用另一种策略：
     * 抽取我们不希望返回的随机元素（这些元素将保留在集合中），
     * 在此过程中创建一个新集合（之后会作为原集合存储），
     * 然后将原集合中剩余的元素返回并释放原集合。 */
        robj *newset = NULL;

        /* Create a new set with just the remaining elements. */
        /* 创建一个只包含剩余元素的新集合。 */
        if (set->encoding == OBJ_ENCODING_LISTPACK) {
            /* Specialized case for listpack. Traverse it only once. */
            /* listpack 特化路径。只遍历一次。 */
            newset = createSetListpackObject();
            unsigned char *lp = set->ptr;
            unsigned char *p = lpFirst(lp);
            unsigned int index = 0;
            unsigned char **ps = zmalloc(sizeof(char *) * remaining);
            for (unsigned long i = 0; i < remaining; i++) {
                p = lpNextRandom(lp, p, &index, remaining - i, 1);
                unsigned int len;
                str = (char *)lpGetValue(p, &len, (long long *)&llele);
                setTypeAddAux(newset, str, len, llele, 0);
                ps[i] = p;
                p = lpNext(lp, p);
                index++;
            }
            lp = lpBatchDelete(lp, ps, remaining);
            zfree(ps);
            set->ptr = lp;
        } else {
            while(remaining--) {
                int encoding = setTypeRandomElement(set, &str, &len, &llele);
                if (!newset) {
                    newset = str ? createSetListpackObject() : createIntsetObject();
                }
                setTypeAddAux(newset, str, len, llele, encoding == OBJ_ENCODING_HT);
                setTypeRemoveAux(set, str, len, llele, encoding == OBJ_ENCODING_HT);
            }
        }

        /* Transfer the old set to the client. */
        /* 将旧集合的内容传送给客户端。 */
        setTypeIterator *si;
        si = setTypeInitIterator(set);
        while (setTypeNext(si, &str, &len, &llele) != -1) {
            if (str == NULL) {
                addReplyBulkLongLong(c,llele);
                propargv[propindex++] = createStringObjectFromLongLong(llele);
            } else {
                addReplyBulkCBuffer(c, str, len);
                propargv[propindex++] = createStringObject(str, len);
            }
            /* Replicate/AOF this command as an SREM operation */
            /* 以 SREM 操作形式复制/AOF */
            if (propindex == 2 + batchsize) {
                alsoPropagate(c->db->id, propargv, propindex, PROPAGATE_AOF | PROPAGATE_REPL);
                for (unsigned long i = 2; i < propindex; i++) {
                    decrRefCount(propargv[i]);
                }
                propindex = 2;
            }
        }
        setTypeReleaseIterator(si);

        /* Assign the new set as the key value. */
        /* 将新集合赋值给键。 */
        dbReplaceValue(c->db,c->argv[1],newset);
    }

    /* Replicate/AOF the remaining elements as an SREM operation */
    /* 将剩余的元素以 SREM 操作的形式复制/AOF */
    if (propindex != 2) {
        alsoPropagate(c->db->id, propargv, propindex, PROPAGATE_AOF | PROPAGATE_REPL);
        for (unsigned long i = 2; i < propindex; i++) {
            decrRefCount(propargv[i]);
        }
        propindex = 2;
    }
    zfree(propargv);

    /* Don't propagate the command itself even if we incremented the
     * dirty counter. We don't want to propagate an SPOP command since
     * we propagated the command as a set of SREMs operations using
     * the alsoPropagate() API. */
    /* 即使增加了 dirty 计数，也不要传播命令本身。
     * 因为我们已经通过 alsoPropagate() API 把命令作为一组 SREM 操作进行了传播，
     * 不希望再传播一个 SPOP 命令。 */
    preventCommandPropagation(c);
    signalModifiedKey(c,c->db,c->argv[1]);
}

/* SPOP 命令实现。
 * 从集合中随机弹出一个元素；如果提供 count，则调用 spopWithCountCommand。
 * 返回被弹出的元素。
 *
 * 时间复杂度：不带 count 时为 O(1)；带 count 时为 O(N) */
void spopCommand(client *c) {
    robj *set, *ele;

    if (c->argc == 3) {
        spopWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set */
    /* 确保给定名称的键存在，并且其类型确实是集合 */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]))
         == NULL || checkType(c,set,OBJ_SET)) return;

    /* Pop a random element from the set */
    /* 从集合中随机弹出一个元素 */
    ele = setTypePopRandom(set);

    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);

    /* Replicate/AOF this command as an SREM operation */
    /* 以 SREM 操作形式复制/AOF */
    rewriteClientCommandVector(c,3,shared.srem,c->argv[1],ele);

    /* Add the element to the reply */
    /* 将元素添加到回复中 */
    addReplyBulk(c, ele);
    decrRefCount(ele);

    /* Delete the set if it's empty */
    /* 集合为空时将其删除 */
    if (setTypeSize(set) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Set has been modified */
    /* 集合已被修改 */
    signalModifiedKey(c,c->db,c->argv[1]);
    server.dirty++;
}

/* handle the "SRANDMEMBER key <count>" variant. The normal version of the
 * command is handled by the srandmemberCommand() function itself. */
/* 处理 "SRANDMEMBER key <count>" 变体形式。
 * 普通版本的命令由 srandmemberCommand() 函数自身处理。 */

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info. */
/* 不采用 "remove elements" 策略时，集合大小相对请求大小的倍数阈值。
 * 阅读实现中的相关注释获取更多信息。 */
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

/* If client is trying to ask for a very large number of random elements,
 * queuing may consume an unlimited amount of memory, so we want to limit
 * the number of randoms per time. */
/* 如果客户端请求数量极大的随机元素，队列可能消耗无限内存，
 * 因此我们限制单次请求的随机元素数量。 */
#define SRANDFIELD_RANDOM_SAMPLE_LIMIT 1000

/* SRANDMEMBER key <count> 变体形式的处理函数。
 * 从集合中随机返回 count 个元素（可能重复）。
 *
 * 时间复杂度：O(N)，其中 N 为请求的元素数量 */
void srandmemberWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    int uniq = 1;
    robj *set;
    char *str;
    size_t len;
    int64_t llele;

    dict *d;

    if (getRangeLongFromObjectOrReply(c,c->argv[2],-LONG_MAX,LONG_MAX,&l,NULL) != C_OK) return;
    if (l >= 0) {
        count = (unsigned long) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
        /* 负数 count 表示：返回的元素允许重复
         * （即每次抽取后不从集合中移除该元素）。 */
        count = -l;
        uniq = 0;
    }

    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray))
        == NULL || checkType(c,set,OBJ_SET)) return;
    size = setTypeSize(set);

    /* If count is zero, serve it ASAP to avoid special cases later. */
    /* 如果 count 为 0，立即返回，避免后续特殊处理。 */
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */
    /* 情形 1：count 为负数，因此抽取方法为：
     * 每次从整个集合中采样 "返回 N 个随机元素"。
     * 这种情况最简单，无需辅助数据结构即可处理。
     * 也是唯一一种会以随机顺序返回元素的情形。 */
    if (!uniq || count == 1) {
        addReplyArrayLen(c,count);

        if (set->encoding == OBJ_ENCODING_LISTPACK && count > 1) {
            /* Specialized case for listpack, traversing it only once. */
            /* listpack 特化路径，只遍历一次。 */
            unsigned long limit, sample_count;
            limit = count > SRANDFIELD_RANDOM_SAMPLE_LIMIT ? SRANDFIELD_RANDOM_SAMPLE_LIMIT : count;
            listpackEntry *entries = zmalloc(limit * sizeof(listpackEntry));
            while (count) {
                sample_count = count > limit ? limit : count;
                count -= sample_count;
                lpRandomEntries(set->ptr, sample_count, entries);
                for (unsigned long i = 0; i < sample_count; i++) {
                    if (entries[i].sval)
                        addReplyBulkCBuffer(c, entries[i].sval, entries[i].slen);
                    else
                        addReplyBulkLongLong(c, entries[i].lval);
                }
                if (c->flags & CLIENT_CLOSE_ASAP)
                    break;
            }
            zfree(entries);
            return;
        }

        while(count--) {
            setTypeRandomElement(set, &str, &len, &llele);
            if (str == NULL) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulkCBuffer(c, str, len);
            }
            if (c->flags & CLIENT_CLOSE_ASAP)
                break;
        }
        return;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set. */
    /* 情形 2：请求元素数量大于等于集合中的元素数量：直接返回整个集合。 */
    if (count >= size) {
        setTypeIterator *si;
        addReplyArrayLen(c,size);
        si = setTypeInitIterator(set);
        while (setTypeNext(si, &str, &len, &llele) != -1) {
            if (str == NULL) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulkCBuffer(c, str, len);
            }
            size--;
        }
        setTypeReleaseIterator(si);
        serverAssert(size==0);
        return;
    }

    /* CASE 2.5 listpack only. Sampling unique elements, in non-random order.
     * Listpack encoded sets are meant to be relatively small, so
     * SRANDMEMBER_SUB_STRATEGY_MUL isn't necessary and we rather not make
     * copies of the entries. Instead, we emit them directly to the output
     * buffer.
     *
     * And it is inefficient to repeatedly pick one random element from a
     * listpack in CASE 4. So we use this instead. */
    /* 情形 2.5（仅 listpack）：以非随机顺序采样唯一元素。
     * 使用 listpack 编码的集合通常较小，因此不需要 SRANDMEMBER_SUB_STRATEGY_MUL，
     * 我们也不希望拷贝各个条目，而是直接将它们写入输出缓冲区。
     *
     * 同时，在情形 4 中反复从 listpack 中挑选单个随机元素效率很低，
     * 因此这里采用这种实现。 */
    if (set->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = set->ptr;
        unsigned char *p = lpFirst(lp);
        unsigned int i = 0;
        addReplyArrayLen(c, count);
        while (count) {
            p = lpNextRandom(lp, p, &i, count--, 1);
            unsigned int len;
            str = (char *)lpGetValue(p, &len, (long long *)&llele);
            if (str == NULL) {
                addReplyBulkLongLong(c, llele);
            } else {
                addReplyBulkCBuffer(c, str, len);
            }
            p = lpNext(lp, p);
            i++;
        }
        return;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary dictionary. */
    /* 情形 3 和情形 4 需要一个辅助字典。 */
    d = dictCreate(&sdsReplyDictType);

    /* CASE 3:
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 4 is highly inefficient. */
    /* 情形 3：集合内的元素数量不超过请求数量的 SRANDMEMBER_SUB_STRATEGY_MUL 倍。
     * 这种情况下，我们从零开始用所有元素构建一个集合，
     * 然后删除随机元素，直到剩下请求数量的元素。
     *
     * 这样做的原因是：当请求元素数仅略小于集合元素数时，
     * 情形 4 采用的自然做法效率极低。 */
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        /* 将所有元素添加到临时字典中。 */
        si = setTypeInitIterator(set);
        dictExpand(d, size);
        while (setTypeNext(si, &str, &len, &llele) != -1) {
            int retval = DICT_ERR;

            if (str == NULL) {
                retval = dictAdd(d,sdsfromlonglong(llele),NULL);
            } else {
                retval = dictAdd(d, sdsnewlen(str, len), NULL);
            }
            serverAssert(retval == DICT_OK);
        }
        setTypeReleaseIterator(si);
        serverAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        /* 删除随机元素以达到正确的数量。 */
        while (size > count) {
            dictEntry *de;
            de = dictGetFairRandomKey(d);
            dictUnlink(d,dictGetKey(de));
            sdsfree(dictGetKey(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    /* 情形 4：集合远大于请求数量。
     * 这种情况下只需从集合中取随机元素并加入临时集合，
     * 反复尝试直到获得足够多的唯一元素达到指定数量。 */
    else {
        unsigned long added = 0;
        sds sdsele;

        dictExpand(d, count);
        while (added < count) {
            setTypeRandomElement(set, &str, &len, &llele);
            if (str == NULL) {
                sdsele = sdsfromlonglong(llele);
            } else {
                sdsele = sdsnewlen(str, len);
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            /* 尝试将对象加入字典。如果已存在则释放它，
             * 否则增加结果字典中的对象数量。 */
            if (dictAdd(d,sdsele,NULL) == DICT_OK)
                added++;
            else
                sdsfree(sdsele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    /* 情形 3 与情形 4：将结果发送给用户。 */
    {
        dictIterator *di;
        dictEntry *de;

        addReplyArrayLen(c,count);
        di = dictGetIterator(d);
        while((de = dictNext(di)) != NULL)
            addReplyBulkSds(c,dictGetKey(de));
        dictReleaseIterator(di);
        dictRelease(d);
    }
}

/* SRANDMEMBER <key> [<count>] 命令实现。
 * 从集合中随机返回一个或多个元素（不删除）。
 *
 * 时间复杂度：不带 count 时为 O(1)；带 count 时为 O(N) */
void srandmemberCommand(client *c) {
    robj *set;
    char *str;
    size_t len;
    int64_t llele;

    if (c->argc == 3) {
        srandmemberWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    /* 处理不带 <count> 参数的变体。以简单 bulk 字符串形式回复。 */
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))
        == NULL || checkType(c,set,OBJ_SET)) return;

    setTypeRandomElement(set, &str, &len, &llele);
    if (str == NULL) {
        addReplyBulkLongLong(c,llele);
    } else {
        addReplyBulkCBuffer(c, str, len);
    }
}

/* qsort 比较函数：按集合基数（元素数量）升序排列。 */
int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    if (setTypeSize(*(robj**)s1) > setTypeSize(*(robj**)s2)) return 1;
    if (setTypeSize(*(robj**)s1) < setTypeSize(*(robj**)s2)) return -1;
    return 0;
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. */
/* qsort 比较函数：按集合基数降序排列。
 * 该函数用于 SDIFF，可能接收到 NULL，此时应视为空集处理。 */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;
    unsigned long first = o1 ? setTypeSize(o1) : 0;
    unsigned long second = o2 ? setTypeSize(o2) : 0;

    if (first < second) return 1;
    if (first > second) return -1;
    return 0;
}

/* SINTER / SMEMBERS / SINTERSTORE / SINTERCARD
 *
 * 'cardinality_only' work for SINTERCARD, only return the cardinality
 * with minimum processing and memory overheads.
 *
 * 'limit' work for SINTERCARD, stop searching after reaching the limit.
 * Passing a 0 means unlimited.
 */
/* SINTER / SMEMBERS / SINTERSTORE / SINTERCARD 的通用实现。
 *
 * 'cardinality_only' 用于 SINTERCARD：仅返回基数，
 * 以最小的处理和内存开销完成。
 *
 * 'limit' 用于 SINTERCARD：达到上限后停止搜索。
 * 传入 0 表示无限制。
 *
 * 时间复杂度：O(N*M)，其中 N 是最小集合的元素数，M 是集合数量 */
void sinterGenericCommand(client *c, robj **setkeys,
                          unsigned long setnum, robj *dstkey,
                          int cardinality_only, unsigned long limit) {
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    char *str;
    size_t len;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding, empty = 0;

    for (j = 0; j < setnum; j++) {
        robj *setobj = lookupKeyRead(c->db, setkeys[j]);
        if (!setobj) {
            /* A NULL is considered an empty set */
            /* NULL 被视为空集 */
            empty += 1;
            sets[j] = NULL;
            continue;
        }
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }

    /* Set intersection with an empty set always results in an empty set.
     * Return ASAP if there is an empty set. */
    /* 与空集的交集结果始终为空集。
     * 如果存在空集则尽快返回。 */
    if (empty > 0) {
        zfree(sets);
        if (dstkey) {
            if (dbDelete(c->db,dstkey)) {
                signalModifiedKey(c,c->db,dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
                server.dirty++;
            }
            addReply(c,shared.czero);
        } else if (cardinality_only) {
            addReplyLongLong(c,cardinality);
        } else {
            addReply(c,shared.emptyset[c->resp]);
        }
        return;
    }

    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    /* 将集合按基数从小到大排序，可以提高算法性能 */
    qsort(sets,setnum,sizeof(robj*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    /* 首先应该输出元素总数……因为这是一个 multi-bulk 写入，
     * 但此时我们还不知道交集的大小，因此使用一个小技巧：
     * 先向输出列表追加一个空对象并保存指针，稍后再用正确的长度修改它。 */
    if (dstkey) {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        /* 如果有目标键用于存放结果集合，则以一个空集合作为初始值创建该键 */
        if (sets[0]->encoding == OBJ_ENCODING_INTSET) {
            /* The first set is an intset, so the result is an intset too. The
             * elements are inserted in ascending order which is efficient in an
             * intset. */
            /* 第一个集合是 intset，因此结果也是 intset。
             * 元素按升序插入，对于 intset 而言效率很高。 */
            dstset = createIntsetObject();
        } else if (sets[0]->encoding == OBJ_ENCODING_LISTPACK) {
            /* To avoid many reallocs, we estimate that the result is a listpack
             * of approximately the same size as the first set. Then we shrink
             * it or possibly convert it to intset in the end. */
            /* 为避免频繁的 realloc，我们预估结果 listpack 与第一个集合大小相近。
             * 之后再进行收缩或可能转换为 intset。 */
            unsigned char *lp = lpNew(lpBytes(sets[0]->ptr));
            dstset = createObject(OBJ_SET, lp);
            dstset->encoding = OBJ_ENCODING_LISTPACK;
        } else {
            /* We start off with a listpack, since it's more efficient to append
             * to than an intset. Later we can convert it to intset or a
             * hashtable. */
            /* 我们从 listpack 开始，因为它的追加操作比 intset 更高效。
             * 之后再转换为 intset 或哈希表。 */
            dstset = createSetListpackObject();
        }
    } else if (!cardinality_only) {
        replylen = addReplyDeferredLen(c);
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    /* 遍历第一个（最小的）集合中的所有元素，并测试该元素是否在所有其他集合中存在，
     * 只要至少有一个集合不包含该元素就将其丢弃。 */
    int only_integers = 1;
    si = setTypeInitIterator(sets[0]);
    while((encoding = setTypeNext(si, &str, &len, &intobj)) != -1) {
        for (j = 1; j < setnum; j++) {
            if (sets[j] == sets[0]) continue;
            if (!setTypeIsMemberAux(sets[j], str, len, intobj,
                                    encoding == OBJ_ENCODING_HT))
                break;
        }

        /* Only take action when all sets contain the member */
        /* 只有当所有集合都包含该成员时才进行后续处理 */
        if (j == setnum) {
            if (cardinality_only) {
                cardinality++;

                /* We stop the searching after reaching the limit. */
                /* 达到 limit 时停止搜索。 */
                if (limit && cardinality >= limit)
                    break;
            } else if (!dstkey) {
                if (str != NULL)
                    addReplyBulkCBuffer(c, str, len);
                else
                    addReplyBulkLongLong(c,intobj);
                cardinality++;
            } else {
                if (str && only_integers) {
                    /* It may be an integer although we got it as a string. */
                    /* 尽管以字符串形式取得，它仍可能是整数。 */
                    if (encoding == OBJ_ENCODING_HT &&
                        string2ll(str, len, (long long *)&intobj))
                    {
                        if (dstset->encoding == OBJ_ENCODING_LISTPACK ||
                            dstset->encoding == OBJ_ENCODING_INTSET)
                        {
                            /* Adding it as an integer is more efficient. */
                            /* 以整数形式添加效率更高。 */
                            str = NULL;
                        }
                    } else {
                        /* It's not an integer */
                        /* 它不是整数 */
                        only_integers = 0;
                    }
                }
                setTypeAddAux(dstset, str, len, intobj, encoding == OBJ_ENCODING_HT);
            }
        }
    }
    setTypeReleaseIterator(si);

    if (cardinality_only) {
        addReplyLongLong(c,cardinality);
    } else if (dstkey) {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */
        /* 当交集非空时，将结果集合存入目标键。 */
        if (setTypeSize(dstset) > 0) {
            if (only_integers) maybeConvertToIntset(dstset);
            if (dstset->encoding == OBJ_ENCODING_LISTPACK) {
                /* We allocated too much memory when we created it to avoid
                 * frequent reallocs. Therefore, we shrink it now. */
                /* 创建时为了避免频繁 realloc 分配了过多内存，
                 * 因此现在进行收缩。 */
                dstset->ptr = lpShrinkToFit(dstset->ptr);
            }
            setKey(c,c->db,dstkey,dstset,0);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,"sinterstore",
                dstkey,c->db->id);
            server.dirty++;
        } else {
            addReply(c,shared.czero);
            if (dbDelete(c->db,dstkey)) {
                server.dirty++;
                signalModifiedKey(c,c->db,dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
            }
        }
        decrRefCount(dstset);
    } else {
        setDeferredSetLen(c,replylen,cardinality);
    }
    zfree(sets);
}

/* SINTER key [key ...] 命令实现。
 * 返回所有给定集合的交集。
 *
 * 时间复杂度：O(N*M)，N 为最小集合的元素数，M 为集合数量 */
/* SINTER key [key ...] */
void sinterCommand(client *c) {
    sinterGenericCommand(c, c->argv+1,  c->argc-1, NULL, 0, 0);
}

/* SINTERCARD numkeys key [key ...] [LIMIT limit] 命令实现。
 * 返回给定集合交集的基数（元素数量），可指定 limit 上限。
 *
 * 时间复杂度：最坏情况 O(N*M)，N 为最小集合元素数，M 为集合数量 */
/* SINTERCARD numkeys key [key ...] [LIMIT limit] */
void sinterCardCommand(client *c) {
    long j;
    long numkeys = 0; /* Number of keys. */
                       /* 键的数量。 */
    long limit = 0;   /* 0 means not limit. */
                       /* 0 表示没有限制。 */

    if (getRangeLongFromObjectOrReply(c, c->argv[1], 1, LONG_MAX,
                                      &numkeys, "numkeys should be greater than 0") != C_OK)
        return;
    if (numkeys > (c->argc - 2)) {
        addReplyError(c, "Number of keys can't be greater than number of args");
        return;
    }

    for (j = 2 + numkeys; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc - 1) - j;

        if (!strcasecmp(opt, "LIMIT") && moreargs) {
            j++;
            if (getPositiveLongFromObjectOrReply(c, c->argv[j], &limit,
                                                 "LIMIT can't be negative") != C_OK)
                return;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    sinterGenericCommand(c, c->argv+2, numkeys, NULL, 1, limit);
}

/* SINTERSTORE destination key [key ...] 命令实现。
 * 将多个集合的交集结果存储到目标键。
 *
 * 时间复杂度：O(N*M)，N 为最小集合元素数，M 为集合数量 */
/* SINTERSTORE destination key [key ...] */
void sinterstoreCommand(client *c) {
    sinterGenericCommand(c, c->argv+2, c->argc-2, c->argv[1], 0, 0);
}

/* SUNION 与 SDIFF 通用实现（同时支持它们的 STORE 变体）。
 *
 * op 取值：
 *   SET_OP_UNION：求并集
 *   SET_OP_DIFF：求差集
 *
 * 时间复杂度：
 *   UNION：O(N)，N 为所有集合的元素总数
 *   DIFF：算法 1 为 O(N*M)，算法 2 为 O(N) */
void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op) {
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    char *str;
    size_t len;
    int64_t llval;
    int encoding;
    int j, cardinality = 0;
    int diff_algo = 1;
    int sameset = 0;

    for (j = 0; j < setnum; j++) {
        robj *setobj = lookupKeyRead(c->db, setkeys[j]);
        if (!setobj) {
            sets[j] = NULL;
            continue;
        }
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
        if (j > 0 && sets[0] == sets[j]) {
            sameset = 1;
        }
    }

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * We compute what is the best bet with the current input here. */
    /* 选择使用哪种 DIFF 算法。
     *
     * 算法 1 时间复杂度为 O(N*M)，其中 N 是第一个集合的元素数，M 是集合总数。
     *
     * 算法 2 时间复杂度为 O(N)，其中 N 是所有集合中的元素总数。
     *
     * 我们在这里根据当前输入计算出最佳选择。 */
    if (op == SET_OP_DIFF && sets[0] && !sameset) {
        long long algo_one_work = 0, algo_two_work = 0;

        for (j = 0; j < setnum; j++) {
            if (sets[j] == NULL) continue;

            algo_one_work += setTypeSize(sets[0]);
            algo_two_work += setTypeSize(sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        /* 算法 1 的常数时间更小，且当存在共有元素时操作数更少。
         * 因此给予算法 1 一些优势。 */
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            /* 使用算法 1 时，最好按基数递减顺序排列要被减去的集合，
             * 这样可以更快找到重复元素。 */
            qsort(sets+1,setnum-1,sizeof(robj*),
                qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union/diff. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE/SDIFFSTORE operation) then
     * this set object will be the resulting object to set into the target key*/
    /* 我们需要一个临时集合对象来存储并集/差集的结果。
     * 如果 dstkey 不为 NULL（即处于 SUNIONSTORE/SDIFFSTORE 操作中），
     * 那么该集合对象将作为结果对象保存到目标键。 */
    dstset = createIntsetObject();

    if (op == SET_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        /* 并集很简单，只需将每个集合的所有元素都加入临时集合即可。 */
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */
                                    /* 不存在的键视为空集 */

            si = setTypeInitIterator(sets[j]);
            while ((encoding = setTypeNext(si, &str, &len, &llval)) != -1) {
                cardinality += setTypeAddAux(dstset, str, len, llval, encoding == OBJ_ENCODING_HT);
            }
            setTypeReleaseIterator(si);
        }
    } else if (op == SET_OP_DIFF && sameset) {
        /* At least one of the sets is the same one (same key) as the first one, result must be empty. */
        /* 至少有一个集合与第一个集合是相同的（同一键），结果必须为空。 */
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
        /* DIFF 算法 1：
         *
         * 遍历第一个集合中的所有元素，只有当该元素在所有其他集合中都不存在时，
         * 才将其加入目标集合。
         *
         * 这种方式最多进行 N*M 次操作，
         * 其中 N 是第一个集合的元素数，M 是集合数量。 */
        si = setTypeInitIterator(sets[0]);
        while ((encoding = setTypeNext(si, &str, &len, &llval)) != -1) {
            for (j = 1; j < setnum; j++) {
                if (!sets[j]) continue; /* no key is an empty set. */
                                         /* 没有该键时视为空集。 */
                if (sets[j] == sets[0]) break; /* same set! */
                                               /* 同一个集合！ */
                if (setTypeIsMemberAux(sets[j], str, len, llval,
                                       encoding == OBJ_ENCODING_HT))
                    break;
            }
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                /* 没有其他集合包含此元素，加入结果集合。 */
                cardinality += setTypeAddAux(dstset, str, len, llval, encoding == OBJ_ENCODING_HT);
            }
        }
        setTypeReleaseIterator(si);
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * This is O(N) where N is the sum of all the elements in every
         * set. */
        /* DIFF 算法 2：
         *
         * 将第一个集合的所有元素加入辅助集合，然后从辅助集合中
         * 移除所有后续集合的元素。
         *
         * 该算法时间复杂度为 O(N)，其中 N 是所有集合中的元素总数。 */
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */
                                    /* 不存在的键视为空集 */

            si = setTypeInitIterator(sets[j]);
            while((encoding = setTypeNext(si, &str, &len, &llval)) != -1) {
                if (j == 0) {
                    cardinality += setTypeAddAux(dstset, str, len, llval,
                                                 encoding == OBJ_ENCODING_HT);
                } else {
                    cardinality -= setTypeRemoveAux(dstset, str, len, llval,
                                                    encoding == OBJ_ENCODING_HT);
                }
            }
            setTypeReleaseIterator(si);

            /* Exit if result set is empty as any additional removal
             * of elements will have no effect. */
            /* 当结果集合为空时立即退出，因为进一步的删除操作不会再有效果。 */
            if (cardinality == 0) break;
        }
    }

    /* Output the content of the resulting set, if not in STORE mode */
    /* 如果不是 STORE 模式，则输出结果集合的内容 */
    if (!dstkey) {
        addReplySetLen(c,cardinality);
        si = setTypeInitIterator(dstset);
        while (setTypeNext(si, &str, &len, &llval) != -1) {
            if (str)
                addReplyBulkCBuffer(c, str, len);
            else
                addReplyBulkLongLong(c, llval);
        }
        setTypeReleaseIterator(si);
        server.lazyfree_lazy_server_del ? freeObjAsync(NULL, dstset, -1) :
                                          decrRefCount(dstset);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        /* 如果有目标键用于存放结果集合，则将结果集合保存到该键 */
        if (setTypeSize(dstset) > 0) {
            setKey(c,c->db,dstkey,dstset,0);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,
                op == SET_OP_UNION ? "sunionstore" : "sdiffstore",
                dstkey,c->db->id);
            server.dirty++;
        } else {
            addReply(c,shared.czero);
            if (dbDelete(c->db,dstkey)) {
                server.dirty++;
                signalModifiedKey(c,c->db,dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
            }
        }
        decrRefCount(dstset);
    }
    zfree(sets);
}

/* SUNION key [key ...] 命令实现。
 * 返回所有给定集合的并集。
 *
 * 时间复杂度：O(N)，N 为所有集合的元素总数 */
/* SUNION key [key ...] */
void sunionCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_UNION);
}

/* SUNIONSTORE destination key [key ...] 命令实现。
 * 将多个集合的并集结果存储到目标键。
 *
 * 时间复杂度：O(N)，N 为所有集合的元素总数 */
/* SUNIONSTORE destination key [key ...] */
void sunionstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_UNION);
}

/* SDIFF key [key ...] 命令实现。
 * 返回所有给定集合的差集（第一个集合减去其他集合）。
 *
 * 时间复杂度：O(N)，N 为所有集合的元素总数 */
/* SDIFF key [key ...] */
void sdiffCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_DIFF);
}

/* SDIFFSTORE destination key [key ...] 命令实现。
 * 将多个集合的差集结果存储到目标键。
 *
 * 时间复杂度：O(N)，N 为所有集合的元素总数 */
/* SDIFFSTORE destination key [key ...] */
void sdiffstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_DIFF);
}

/* SSCAN 命令实现。
 * 使用游标迭代集合中的元素。
 *
 * 时间复杂度：每次调用 O(1)，完整迭代 O(N) */
void sscanCommand(client *c) {
    robj *set;
    unsigned long long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,set,OBJ_SET)) return;
    scanGenericCommand(c,set,cursor);
}
