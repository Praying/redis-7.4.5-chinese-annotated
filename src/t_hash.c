/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "ebuckets.h"
#include <math.h>

/* HEXPIRE 和 HPERSIST 的阈值：判断是否值得更新全局 HFE 数据结构中
 * 哈希对象的过期时间。 */
#define HASH_NEW_EXPIRE_DIFF_THRESHOLD max(4000, 1<<EB_BUCKET_KEY_PRECISION)

/* 从哈希字段的过期时间中预留 2 位，以便将来可能实现的轻量级
 * 索引/字段分类。可以通过如下方式 hack HFE 实现：
 *
 *    HPEXPIREAT key [ 2^47 + USER_INDEX ] FIELDS numfields field [field …]
 *
 * Redis 还需要暴露类似 HEXPIRESCAN 和 HEXPIRECOUNT 的命令以支持该想法。
 * 仍有待进一步明确定义。
 *
 * HFE_MAX_ABS_TIME_MSEC 约束只能在 API 层强制执行。在内部，为了将来
 * 做好准备，过期时间最大可到 EB_EXPIRE_TIME_MAX。
 */
#define HFE_MAX_ABS_TIME_MSEC (EB_EXPIRE_TIME_MAX >> 2)

/* GetFieldRes 枚举：hashTypeGet* 值族函数返回的结果状态。 */
typedef enum GetFieldRes {
    /* common (Used by hashTypeGet* value family) */
    GETF_OK = 0,            /* 字段已找到。 */
    GETF_NOT_FOUND,         /* 字段未找到。 */
    GETF_EXPIRED,           /* 逻辑上已过期（可能尚未惰性删除） */
    GETF_EXPIRED_HASH,      /* 由于获取的字段已过期且该字段是哈希中的
                             * 最后一个字段，因此删除该哈希。 */
} GetFieldRes;

/* ActiveExpireCtx 传递给 hashTypeActiveExpire() 的上下文。 */
typedef struct ExpireCtx {
    uint32_t fieldsToExpireQuota;  /* 本次过期配额（最多过期多少个字段） */
    redisDb *db;                   /* 所属数据库 */
} ExpireCtx;

/* 将 listpackEntry 别名为 CommonEntry，使其可在 listpack 之外复用。 */
typedef listpackEntry CommonEntry; /* extend usage beyond lp */

/* 哈希字段过期（HFE）相关函数声明 */
static ExpireAction onFieldExpire(eItem item, void *ctx);
static ExpireMeta* hfieldGetExpireMeta(const eItem field);
static ExpireMeta *hashGetExpireMeta(const eItem hash);
static void hexpireGenericCommand(client *c, const char *cmd, long long basetime, int unit);
static ExpireAction hashTypeActiveExpire(eItem hashObj, void *ctx);
static uint64_t hashTypeExpire(robj *o, ExpireCtx *expireCtx, int updateGlobalHFE);
static void hfieldPersist(robj *hashObj, hfield field);
static void propagateHashFieldDeletion(redisDb *db, sds key, char *field, size_t fieldLen);

/* 哈希 dictType 相关函数声明 */
static int dictHfieldKeyCompare(dict *d, const void *key1, const void *key2);
static uint64_t dictMstrHash(const void *key);
static void dictHfieldDestructor(dict *d, void *field);
static size_t hashDictWithExpireMetadataBytes(dict *d);
static void hashDictWithExpireOnRelease(dict *d);
static robj* hashTypeLookupWriteOrCreate(client *c, robj *key);

/*-----------------------------------------------------------------------------
 * 定义哈希的 dictType
 *
 * - 将字段存储为 mstr 字符串，可选择附加元数据以挂接 TTL
 * - 注意：小型哈希使用 listpack 表示
 * - 一旦为字段设置了过期时间，对应的 dict 实例和 dictType 将被替换为
 *   包含哈希字段过期（HFE）元数据的 dict，并使用 dictType
 *   `mstrHashDictTypeWithHFE`
 *----------------------------------------------------------------------------*/
dictType mstrHashDictType = {
    dictSdsHash,                                /* 查找哈希函数 */
    NULL,                                       /* key dup */
    NULL,                                       /* val dup */
    dictSdsMstrKeyCompare,                      /* 查找 key compare */
    dictHfieldDestructor,                       /* 键析构函数 */
    dictSdsDestructor,                          /* 值析构函数 */
    .storedHashFunction = dictMstrHash,         /* 存储哈希函数 */
    .storedKeyCompare = dictHfieldKeyCompare,   /* 存储键比较函数 */
};

/* 定义支持哈希字段过期（HFE）的替代哈希 dictType */
dictType mstrHashDictTypeWithHFE = {
    dictSdsHash,                                /* 查找哈希函数 */
    NULL,                                       /* key dup */
    NULL,                                       /* val dup */
    dictSdsMstrKeyCompare,                      /* 查找 key compare */
    dictHfieldDestructor,                       /* 键析构函数 */
    dictSdsDestructor,                          /* 值析构函数 */
    .storedHashFunction = dictMstrHash,         /* 存储哈希函数 */
    .storedKeyCompare = dictHfieldKeyCompare,   /* 存储键比较函数 */
    .dictMetadataBytes = hashDictWithExpireMetadataBytes,
    .onDictRelease = hashDictWithExpireOnRelease,
};

/*-----------------------------------------------------------------------------
 * 哈希字段过期（HFE）特性
 *
 * 每个哈希实例都在其私有的 ebuckets 数据结构中维护自己的哈希字段过期集合。
 * 为了支持跨哈希实例的 HFE 主动过期循环，具有 HFE 的哈希还会被注册到
 * 全局 ebuckets 数据结构中，并使用反映其下一个最早过期时间的值作为
 * 过期时间。全局 HFE 主动过期将由 activeExpireCycle() 函数触发，
 * 并为每个具有已过期字段的哈希实例调用“局部” HFE 主动过期。
 *
 * hashExpireBucketsType - 在全局空间 (db->hexpires) 中使用的 ebuckets 类型，
 * 用于注册具有一个或多个带时间过期字段的哈希。这些哈希将以哈希中最早
 * 字段的过期时间进行注册。
 *----------------------------------------------------------------------------*/
EbucketsType hashExpireBucketsType = {
    .onDeleteItem = NULL,
    .getExpireMeta = hashGetExpireMeta,   /* 获取附加到每个哈希的 ExpireMeta */
    .itemsAddrAreOdd = 0,                 /* dict 的地址是偶数 */
};

/* dictExpireMetadata - 带有时间过期的哈希字段的 ebuckets 类型。
 * ebuckets 实例将被附加到至少有一个字段具有过期时间的哈希。 */
EbucketsType hashFieldExpireBucketsType = {
    .onDeleteItem = NULL,
    .getExpireMeta = hfieldGetExpireMeta, /* 获取附加到每个字段的 ExpireMeta */
    .itemsAddrAreOdd = 1,                 /* hfield (mstr) 的地址是奇数！！ */
};

/* OnFieldExpireCtx：传递给 OnFieldExpire() 的上下文。 */
typedef struct OnFieldExpireCtx {
    robj *hashObj;  /* 当前正在过期的哈希对象 */
    redisDb *db;    /* 所属数据库 */
} OnFieldExpireCtx;

/* 哈希的 dict 实现已从将字段存储为 sds 字符串改为存储 "mstr"（带元数据的
 * 不可变字符串），以便能够将 TTL (ExpireMeta) 附加到哈希字段。这种 mstr
 * 的使用为将来根据需要为字段附加其他元数据的功能打开了大门。
 *
 * 以下定义了新的 hfield 类型的 mstr */
typedef enum HfieldMetaFlags {
    HFIELD_META_EXPIRE = 0,  /* 字段过期元数据标记位 */
} HfieldMetaFlags;

/* 哈希字段使用的 mstr 类型注册。
 * 注意：所有 metaSize[*] 值都保证为偶数，确保所有 hfield 实例的地址为奇数。 */
mstrKind mstrFieldKind = {
    .name = "hField",

    /* 注意：保证所有 metaSize[*] 值为偶数，
     * 以确保所有 hfield 实例的地址为奇数。 */
    .metaSize[HFIELD_META_EXPIRE] = sizeof(ExpireMeta),
};
/* 编译期断言：ExpireMeta 大小必须为偶数，保证 hfield 地址为奇数。 */
static_assert(sizeof(struct ExpireMeta ) % 2 == 0, "must be even!");

/* hpersistCommand() 命令的返回值。 */
typedef enum SetPersistRes {
    HFE_PERSIST_NO_FIELD =     -2,   /* 不存在对应的哈希字段 */
    HFE_PERSIST_NO_TTL =       -1,   /* 字段未挂接 TTL */
    HFE_PERSIST_OK =            1    /* 成功清除字段的过期时间 */
} SetPersistRes;

/* 判断 dict 是否是带有 HFE 元数据的哈希字典。 */
static inline int isDictWithMetaHFE(dict *d) {
    return d->type == &mstrHashDictTypeWithHFE;
}

/*-----------------------------------------------------------------------------
 * setex* - 设置字段的过期时间
 *
 * 设置字段的过期时间可能既耗时又复杂，因为每次更新过期时间不仅要更新
 * 对应哈希的 `ebuckets`，还可能要更新全局 HFE 数据结构的 `ebuckets`。
 * 需要为给定的哈希组织一系列字段过期更新操作，使得只有全部完成后，
 * 全局 HFE 数据结构才会被更新。
 *
 * 具体步骤如下：
 * 1. 调用 hashTypeSetExInit() 初始化 HashTypeSetEx 结构体。
 * 2. 对每个字段/过期更新调用一次或多次 hashTypeSetEx()。
 * 3. 调用 hashTypeSetExDone() 进行通知并更新全局 HFE。
 *----------------------------------------------------------------------------*/

/* hashTypeSetEx() 的返回值枚举。 */
typedef enum SetExRes {
    HSETEX_OK =                1,   /* 过期时间按预期设置/更新 */
    HSETEX_NO_FIELD =         -2,   /* 不存在对应的哈希字段 */
    HSETEX_NO_CONDITION_MET =  0,   /* 指定的 NX | XX | GT | LT 条件不满足 */
    HSETEX_DELETED =           2,   /* 由于指定的时间已过期，字段被删除 */
} SetExRes;

/* httlGenericCommand() 使用的返回值枚举。 */
typedef enum GetExpireTimeRes {
    HFE_GET_NO_FIELD =          -2, /* 不存在对应的哈希字段 */
    HFE_GET_NO_TTL =            -1, /* 字段未挂接 TTL */
} GetExpireTimeRes;

/* 设置过期时间的条件枚举（NX/XX/GT/LT 位标志）。 */
typedef enum ExpireSetCond {
    HFE_NX = 1<<0,  /* 仅当字段没有过期时间时设置 */
    HFE_XX = 1<<1,  /* 仅当字段已有过期时间时设置 */
    HFE_GT = 1<<2,  /* 仅当新过期时间大于当前过期时间时设置 */
    HFE_LT = 1<<3   /* 仅当新过期时间小于当前过期时间时设置 */
} ExpireSetCond;

/* hashTypeSetEx() 设置字段或过期时间所用的结构体。 */
typedef struct HashTypeSetEx {

    /*** 配置 ***/
    ExpireSetCond expireSetCond;        /* [XX | NX | GT | LT] 条件 */

    /*** 元数据 ***/
    uint64_t minExpire;                 /* 未初始化时为 EB_EXPIRE_TIME_INVALID */
    redisDb *db;                        /* 所属数据库 */
    robj *key, *hashObj;                /* 对应的 key 与哈希对象 */
    uint64_t minExpireFields;           /* 追踪已更新字段的旧/新最小过期时间。
                                         * 如果记录的最小值大于哈希的
                                         * minExpire，则无需更新全局 HFE DS */
    int fieldDeleted;                   /* 已删除的字段数 */
    int fieldUpdated;                   /* 已更新的字段数 */

    /* 可选：提供客户端以触发通知 */
    client *c;
    const char *cmd;
} HashTypeSetEx;

int hashTypeSetExInit(robj *key, robj *o, client *c, redisDb *db, const char *cmd,
                      ExpireSetCond expireSetCond, HashTypeSetEx *ex);

SetExRes hashTypeSetEx(robj *o, sds field, uint64_t expireAt, HashTypeSetEx *exInfo);

void hashTypeSetExDone(HashTypeSetEx *e);

/*-----------------------------------------------------------------------------
 * 哈希 dictType 的访问器函数
 *----------------------------------------------------------------------------*/

/* 比较两个 hfield 键（基于存储键的 mstr 表示）是否相等。
 * 时间复杂度：O(L)，L 为字段长度。 */
static int dictHfieldKeyCompare(dict *d, const void *key1, const void *key2)
{
    int l1,l2;
    UNUSED(d);

    /* 先比较长度，再逐字节比较 */
    l1 = hfieldlen((hfield)key1);
    l2 = hfieldlen((hfield)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* 计算 mstr 字段的哈希值（用于 dict 内部存储）。 */
static uint64_t dictMstrHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, mstrlen((char*)key));
}

/* 哈希字段键的析构函数。当 dict 中删除 hfield 时由 dict 调用。 */
static void dictHfieldDestructor(dict *d, void *field) {

    /* 如果该字段挂接了 TTL，则需要从哈希的私有 ebuckets 中移除。 */
    if (hfieldGetExpireTime(field) != EB_EXPIRE_TIME_INVALID) {
        dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
        ebRemove(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, field);
    }

    /* 释放 hfield 自身 */
    hfieldFree(field);

    /* 无需更新全局 HFE DS。这是不必要的。
     * 实现该功能会引入显著的复杂性和开销，而该操作并非关键。
     * 在最坏的情况下，哈希稍后会被主动过期操作高效地更新，
     * 或者由哈希的 dbGenericDelete() 函数删除。 */
}

/* 返回带有过期元数据的哈希 dict 所需分配的元数据大小。 */
static size_t hashDictWithExpireMetadataBytes(dict *d) {
    UNUSED(d);
    /* 哈希的 expireMeta、对 ebuckets 的引用以及指向哈希 key 的指针 */
    return sizeof(dictExpireMetadata);
}

/* dict 释放时调用的回调，销毁哈希的私有 ebuckets。
 * 注：此函数只有在 dict 分配了元数据时才会被注册。 */
static void hashDictWithExpireOnRelease(dict *d) {
    /* 必然已分配了元数据。否则此函数不会被注册。 */
    dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
    ebDestroy(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, NULL);
}

/*-----------------------------------------------------------------------------
 * listpackEx 函数
 *----------------------------------------------------------------------------*/
/*
 * 如果首次对 listpack 编码的哈希对象调用任意哈希字段过期命令，
 * 我们会将其转换为 OBJ_ENCODING_LISTPACK_EX 编码。
 * 我们分配 "struct listpackEx"，它持有 listpack 指针和元数据，
 * 用于将 key 注册到全局数据结构中。
 * 在 listpack 中，我们为每个 field-value 对再追加一个 TTL 条目。
 * 从此，listpack 中将包含三元组：field-value-ttl。
 * 如果某个字段没有设置 TTL，则将 'zero' 作为 TTL 值存储。
 * 'zero' 在 listpack 中编码为两个字节。因此不存在的 TTL
 * 每个字段的内存开销为两个字节。
 *
 * listpack 中的字段将按 TTL 排序。最早过期的字段将位于第一项。
 * 没有 TTL 的字段将位于 listpack 的末尾。这样更容易/更快地
 * 找到过期的项。
 */

#define HASH_LP_NO_TTL 0

/* 创建一个 listpackEx 结构。返回值为新分配并初始化后的 listpackEx。 */
struct listpackEx *listpackExCreate(void) {
    listpackEx *lpt = zcalloc(sizeof(*lpt));
    lpt->meta.trash = 1;  /* 初始时标记为 trash */
    lpt->lp = NULL;
    lpt->key = NULL;
    return lpt;
}

/* 释放 listpackEx 及其内部 listpack。 */
static void listpackExFree(listpackEx *lpt) {
    lpFree(lpt->lp);
    zfree(lpt);
}

/* lpFindCb() 回调函数所用的参数结构。 */
struct lpFingArgs {
    uint64_t max_to_search; /* [in] 最多搜索的元组数 */
    uint64_t expire_time;   /* [in] 查找 TTL 大于该值的元组 */
    unsigned char *p;       /* [out] TTL 大于 expire_time 的元组的首项 */
    int expired;            /* [out] TTL 小于 expire_time 的元组数量 */
    int index;              /* 内部使用 */
    unsigned char *fptr;    /* 内部使用的临时指针 */
};

/* lpFindCb() 的回调。用于统计已过期字段数量（主动过期时），
 * 或在根据新字段的过期时间查找插入位置时使用。 */
static int cbFindInListpack(const unsigned char *lp, unsigned char *p,
                            void *user, unsigned char *s, long long slen)
{
    (void) lp;
    struct lpFingArgs *r = user;

    r->index++;

    if (r->max_to_search == 0)
        return 0; /* 中断循环并返回 */

    if (r->index % 3 == 1) {
        /* 元组的第 1 项：字段名 */
        r->fptr = p;  /* First item of the tuple. */
    } else if (r->index % 3 == 0) {
        serverAssert(!s);

        /* 元组的第 3 项是过期时间 */
        if (slen == HASH_LP_NO_TTL || (uint64_t) slen >= r->expire_time) {
            r->p = r->fptr;
            return 0; /* 中断循环并返回 */
        }
        r->expired++;
        r->max_to_search--;
    }

    return 1;
}

/* 返回已过期字段的数量（仅做扫描，不实际删除）。 */
static uint64_t listpackExExpireDryRun(const robj *o) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK_EX);

    listpackEx *lpt = o->ptr;

    struct lpFingArgs r = {
        .max_to_search = UINT64_MAX,
        .expire_time = commandTimeSnapshot(),
    };

    lpFindCb(lpt->lp, NULL, &r, cbFindInListpack, 0);
    return r.expired;
}

/* 返回最早过期项的过期时间。
 * 若无带 TTL 的字段则返回 EB_EXPIRE_TIME_INVALID。 */
static uint64_t listpackExGetMinExpire(robj *o) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK_EX);

    long long expireAt;
    unsigned char *fptr;
    listpackEx *lpt = o->ptr;

    /* 由于字段按过期时间排序，第一个字段的过期时间最小。
     * 第 3 个元素是第一个字段的过期时间。 */
    fptr = lpSeek(lpt->lp, 2);
    if (fptr != NULL) {
        serverAssert(lpGetIntegerValue(fptr, &expireAt));

        /* 检查是否为不带 TTL 的字段。 */
        if (expireAt != HASH_LP_NO_TTL)
            return expireAt;
    }

    return EB_EXPIRE_TIME_INVALID;
}

/* 遍历字段并删除已过期的字段。
 * 时间复杂度：O(N)，N 为过期字段数量（受 info->maxToExpire 限制）。 */
void listpackExExpire(redisDb *db, robj *o, ExpireInfo *info) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK_EX);
    uint64_t expired = 0, min = EB_EXPIRE_TIME_INVALID;
    unsigned char *ptr;
    listpackEx *lpt = o->ptr;

    ptr = lpFirst(lpt->lp);

    while (ptr != NULL && (info->itemsExpired < info->maxToExpire)) {
        long long val;
        int64_t flen;
        unsigned char intbuf[LP_INTBUF_SIZE], *fref;

        fref = lpGet(ptr, &flen, intbuf);

        ptr = lpNext(lpt->lp, ptr);
        serverAssert(ptr);
        ptr = lpNext(lpt->lp, ptr);
        serverAssert(ptr && lpGetIntegerValue(ptr, &val));

        /* 字段按过期时间排序。当遇到未过期或无 TTL 的字段时，
         * 说明后续的字段也尚未过期，可以提前终止遍历。 */
        if (val == HASH_LP_NO_TTL || (uint64_t) val > info->now)
            break;

        propagateHashFieldDeletion(db, ((listpackEx *) o->ptr)->key, (char *)((fref) ? fref : intbuf), flen);
        server.stat_expired_subkeys++;

        ptr = lpNext(lpt->lp, ptr);

        info->itemsExpired++;
        expired++;
    }

    /* 一次性删除所有已过期的元组 */
    if (expired)
        lpt->lp = lpDeleteRange(lpt->lp, 0, expired * 3);

    min = hashTypeGetMinExpire(o, 1 /*accurate*/);
    info->nextExpireTime = min;
}

/* 在 listpackEx 中插入一个三元组。 */
static void listpackExAddInternal(robj *o, listpackEntry ent[3]) {
    listpackEx *lpt = o->ptr;

    /* 优化：如果是非易失字段（无 TTL），直接追加到末尾。 */
    if (ent[2].lval == HASH_LP_NO_TTL) {
        lpt->lp = lpBatchAppend(lpt->lp, ent, 3);
        return;
    }

    struct lpFingArgs r = {
            .max_to_search = UINT64_MAX,
            .expire_time = ent[2].lval,
    };

    /* 查找是否存在比该 TTL 更大的字段。 */
    lpFindCb(lpt->lp, NULL, &r, cbFindInListpack, 0);

    /* 如果列表为空或没有比该 TTL 更大的字段，结果将为 NULL。
     * 否则，就在找到的项之前插入。*/
    if (r.p)
        lpt->lp = lpBatchInsert(lpt->lp, r.p, LP_BEFORE, ent, 3, NULL);
    else
        lpt->lp = lpBatchAppend(lpt->lp, ent, 3);
}

/* 按过期时间顺序添加新字段。 */
void listpackExAddNew(robj *o, char *field, size_t flen,
                      char *value, size_t vlen, uint64_t expireAt) {
    listpackEntry ent[3] = {
        {.sval = (unsigned char*) field, .slen = flen},
        {.sval = (unsigned char*) value, .slen = vlen},
        {.lval = expireAt}
    };

    listpackExAddInternal(o, ent);
}

/* 如果过期时间发生变化，本函数将字段重新放置到正确位置。
 * 首先删除原字段，然后按过期时间顺序重新插入到 listpack 中。 */
static void listpackExUpdateExpiry(robj *o, sds field,
                                   unsigned char *fptr,
                                   unsigned char *vptr,
                                   uint64_t expire_at) {
    unsigned int slen = 0;
    long long val = 0;
    unsigned char tmp[512] = {0};
    unsigned char *valstr;
    sds tmpval = NULL;
    listpackEx *lpt = o->ptr;

    /* 复制 value */
    valstr = lpGetValue(vptr, &slen, &val);
    if (valstr) {
        /* 通常 listpack 中的项长度受 'hash-max-listpack-value' 配置限制。
         * 但少数情况下可能超过 sizeof(tmp)。 */
        if (slen > sizeof(tmp))
            tmpval = sdsnewlen(valstr, slen);
        else
            memcpy(tmp, valstr, slen);
    }

    /* 删除字段名、值和过期时间三元组 */
    lpt->lp = lpDeleteRangeWithEntry(lpt->lp, &fptr, 3);

    listpackEntry ent[3] = {{0}};

    ent[0].sval = (unsigned char*) field;
    ent[0].slen = sdslen(field);

    if (valstr) {
        ent[1].sval = tmpval ? (unsigned char *) tmpval : tmp;
        ent[1].slen = slen;
    } else {
        ent[1].lval = val;
    }
    ent[2].lval = expire_at;

    listpackExAddInternal(o, ent);
    sdsfree(tmpval);
}

/* 更新字段的过期时间（listpack 编码版本）。 */
SetExRes hashTypeSetExpiryListpack(HashTypeSetEx *ex, sds field,
                                   unsigned char *fptr, unsigned char *vptr,
                                   unsigned char *tptr, uint64_t expireAt)
{
    long long expireTime;
    uint64_t prevExpire = EB_EXPIRE_TIME_INVALID;

    serverAssert(lpGetIntegerValue(tptr, &expireTime));

    if (expireTime != HASH_LP_NO_TTL) {
        prevExpire = (uint64_t) expireTime;
    }

    if (prevExpire == EB_EXPIRE_TIME_INVALID) {
        /* 对于没有过期的字段，LT 条件视为满足 */
        if (ex->expireSetCond & (HFE_XX | HFE_GT))
            return HSETEX_NO_CONDITION_MET;
    } else {
        if (((ex->expireSetCond == HFE_GT) && (prevExpire >= expireAt)) ||
            ((ex->expireSetCond == HFE_LT) && (prevExpire <= expireAt)) ||
            (ex->expireSetCond == HFE_NX) )
            return HSETEX_NO_CONDITION_MET;

        /* 跟踪最小过期时间（仅稍后更新全局 HFE DS） */
        if (ex->minExpireFields > prevExpire)
            ex->minExpireFields = prevExpire;
    }

    /* 如果已过期，则删除该字段并传播删除操作。
     * 如果是从节点（replica），则按字段有效继续处理。 */
    if (unlikely(checkAlreadyExpired(expireAt))) {
        propagateHashFieldDeletion(ex->db, ex->key->ptr, field, sdslen(field));
        hashTypeDelete(ex->hashObj, field, 1);
        server.stat_expired_subkeys++;
        ex->fieldDeleted++;
        return HSETEX_DELETED;
    }

    if (ex->minExpireFields > expireAt)
        ex->minExpireFields = expireAt;

    listpackExUpdateExpiry(ex->hashObj, field, fptr, vptr, expireAt);
    ex->fieldUpdated++;
    return HSETEX_OK;
}

/* 如果已过期则返回 1，否则返回 0。 */
int hashTypeIsExpired(const robj *o, uint64_t expireAt) {
    if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        if (expireAt == HASH_LP_NO_TTL)
            return 0;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (expireAt == EB_EXPIRE_TIME_INVALID)
            return 0;
    } else {
        serverPanic("Unknown encoding: %d", o->encoding);
    }

    return (mstime_t) expireAt < commandTimeSnapshot();
}

/* 返回对象的 listpack 指针（无论是 LISTPACK 还是 LISTPACK_EX 编码）。 */
unsigned char *hashTypeListpackGetLp(robj *o) {
    if (o->encoding == OBJ_ENCODING_LISTPACK)
        return o->ptr;
    else if (o->encoding == OBJ_ENCODING_LISTPACK_EX)
        return ((listpackEx*)o->ptr)->lp;

    serverPanic("Unknown encoding: %d", o->encoding);
}

/*-----------------------------------------------------------------------------
 * 哈希类型 API
 *----------------------------------------------------------------------------*/

/* 检查多个对象的长度，以判断是否需要将 listpack 转换为真正的哈希。
 * 注意：我们只检查字符串编码的对象，因为它们的长度可以在 O(1) 时间内获得。 */
void hashTypeTryConversion(redisDb *db, robj *o, robj **argv, int start, int end) {
    int i;
    size_t sum = 0;

    if (o->encoding != OBJ_ENCODING_LISTPACK && o->encoding != OBJ_ENCODING_LISTPACK_EX)
        return;

    /* 我们假设输入中的字段大部分是唯一的，因此如果有足够多的参数，
     * 就创建一个预分配大小的哈希，这样如果有重复可能会过度分配内存。 */
    size_t new_fields = (end - start + 1) / 2;
    if (new_fields > server.hash_max_listpack_entries) {
        hashTypeConvert(o, OBJ_ENCODING_HT, &db->hexpires);
        dictExpand(o->ptr, new_fields);
        return;
    }

    for (i = start; i <= end; i++) {
        if (!sdsEncodedObject(argv[i]))
            continue;
        size_t len = sdslen(argv[i]->ptr);
        if (len > server.hash_max_listpack_value) {
            hashTypeConvert(o, OBJ_ENCODING_HT, &db->hexpires);
            return;
        }
        sum += len;
    }
    if (!lpSafeToAdd(hashTypeListpackGetLp(o), sum))
        hashTypeConvert(o, OBJ_ENCODING_HT, &db->hexpires);
}

/* 从 listpack 编码的哈希中按字段获取值。
 * 时间复杂度：O(N)，N 为字段数量。 */
GetFieldRes hashTypeGetFromListpack(robj *o, sds field,
                            unsigned char **vstr,
                            unsigned int *vlen,
                            long long *vll,
                            uint64_t *expiredAt)
{
    *expiredAt = EB_EXPIRE_TIME_INVALID;
    unsigned char *zl, *fptr = NULL, *vptr = NULL;

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        zl = o->ptr;
        fptr = lpFirst(zl);
        if (fptr != NULL) {
            fptr = lpFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* 获取指向 value 的指针（fptr 指向 field） */
                vptr = lpNext(zl, fptr);
                serverAssert(vptr != NULL);
            }
        }
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        long long expire;
        unsigned char *h;
        listpackEx *lpt = o->ptr;

        fptr = lpFirst(lpt->lp);
        if (fptr != NULL) {
            fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);
            if (fptr != NULL) {
                /* 获取指向 value 的指针（fptr 指向 field） */
                vptr = lpNext(lpt->lp, fptr);
                serverAssert(vptr != NULL);

                h = lpNext(lpt->lp, vptr);
                serverAssert(h && lpGetIntegerValue(h, &expire));
                if (expire != HASH_LP_NO_TTL)
                    *expiredAt = expire;
            }
        }
    } else {
        serverPanic("Unknown hash encoding: %d", o->encoding);
    }

    if (vptr != NULL) {
        *vstr = lpGetValue(vptr, vlen, vll);
        return GETF_OK;
    }

    return GETF_NOT_FOUND;
}

/* 从哈希表（HT）编码的哈希中按字段获取值。
 * 如果未找到字段返回 NULL，否则返回 SDS 值。
 * 时间复杂度：O(1)。 */
GetFieldRes hashTypeGetFromHashTable(robj *o, sds field, sds *value, uint64_t *expiredAt) {
    dictEntry *de;

    *expiredAt = EB_EXPIRE_TIME_INVALID;

    serverAssert(o->encoding == OBJ_ENCODING_HT);

    de = dictFind(o->ptr, field);

    if (de == NULL)
        return GETF_NOT_FOUND;

    *expiredAt = hfieldGetExpireTime(dictGetKey(de));
    *value = (sds) dictGetVal(de);
    return GETF_OK;
}

/* hashTypeGet*() 的更高级版本，返回指定字段关联的哈希值。
 *
 * 参数：
 * hfeFlags      - HFE_LAZY_* 标志位，控制惰性过期行为
 *
 * 返回：
 * GetFieldRes   - 获取操作的结果
 * vstr, vlen    - 如果是字符串，则引用赋给 *vstr 和 *vlen
 * vll           - 如果是数字则存储在 *vll 中。
 *                如果 *vll 被赋值，则 *vstr 会被设为 NULL，
 *                调用者可通过返回值是否为 GETF_OK 以及 vll（或 vstr）
 *                是否为 NULL 来判断是否成功。
 */
GetFieldRes hashTypeGetValue(redisDb *db, robj *o, sds field, unsigned char **vstr,
                             unsigned int *vlen, long long *vll, int hfeFlags) {
    uint64_t expiredAt;
    sds key;
    GetFieldRes res;
    if (o->encoding == OBJ_ENCODING_LISTPACK ||
        o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        *vstr = NULL;
        res = hashTypeGetFromListpack(o, field, vstr, vlen, vll, &expiredAt);

        if (res == GETF_NOT_FOUND)
            return GETF_NOT_FOUND;

    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds value = NULL;
        res = hashTypeGetFromHashTable(o, field, &value, &expiredAt);

        if (res == GETF_NOT_FOUND)
            return GETF_NOT_FOUND;

        *vstr = (unsigned char*) value;
        *vlen = sdslen(value);
    } else {
        serverPanic("Unknown hash encoding");
    }

    if (expiredAt >= (uint64_t) commandTimeSnapshot())
        return GETF_OK;

    if (server.masterhost) {
        /* 如果当前客户端是主节点（CLIENT_MASTER），只要未被删除就视为有效。 */
        if (server.current_client && (server.current_client->flags & CLIENT_MASTER))
            return GETF_OK;

        /* 如果是用户客户端，则按已过期处理，但不删除！ */
        return GETF_EXPIRED;
    }

    if ((server.loading) ||
        (server.lazy_expire_disabled) ||
        (hfeFlags & HFE_LAZY_AVOID_FIELD_DEL) ||
        (isPausedActionsWithUpdate(PAUSE_ACTION_EXPIRE)))
        return GETF_EXPIRED;

    if (o->encoding == OBJ_ENCODING_LISTPACK_EX)
        key = ((listpackEx *) o->ptr)->key;
    else
        key = ((dictExpireMetadata *) dictMetadata((dict*)o->ptr))->key;

    /* 删除字段并传播删除事件 */
    serverAssert(hashTypeDelete(o, field, 1) == 1);
    propagateHashFieldDeletion(db, key, field, sdslen(field));
    server.stat_expired_subkeys++;

    /* 如果该字段是哈希中的最后一个字段，则哈希也会被删除 */
    res = GETF_EXPIRED;
    robj *keyObj = createStringObject(key, sdslen(key));
    if (!(hfeFlags & HFE_LAZY_NO_NOTIFICATION))
        notifyKeyspaceEvent(NOTIFY_HASH, "hexpired", keyObj, db->id);
    if ((hashTypeLength(o, 0) == 0) && (!(hfeFlags & HFE_LAZY_AVOID_HASH_DEL))) {
        if (!(hfeFlags & HFE_LAZY_NO_NOTIFICATION))
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", keyObj, db->id);
        dbDelete(db,keyObj);
        res = GETF_EXPIRED_HASH;
    }
    signalModifiedKey(NULL, db, keyObj);
    decrRefCount(keyObj);
    return res;
}

/* 与 hashTypeGetValue() 类似，但返回 Redis 对象，便于 t_hash.c 之外
 * 与哈希类型交互使用。
 * 如果哈希中未找到字段，函数返回 NULL。否则返回一个新分配的字符串对象。
 *
 * hfeFlags      - HFE_LAZY_* 标志位
 * isHashDeleted - 如果访问的是已过期字段且它是哈希中的最后一个字段，
 *                 则哈希也会被删除，此时 *isHashDeleted 会被置为 1。
 */
robj *hashTypeGetValueObject(redisDb *db, robj *o, sds field, int hfeFlags, int *isHashDeleted) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    if (isHashDeleted) *isHashDeleted = 0;
    GetFieldRes res = hashTypeGetValue(db,o,field,&vstr,&vlen,&vll, hfeFlags);

    if (res == GETF_OK) {
        if (vstr) return createStringObject((char*)vstr,vlen);
        else return createStringObjectFromLongLong(vll);
    }

    if ((res == GETF_EXPIRED_HASH) && (isHashDeleted))
        *isHashDeleted = 1;

    /* GETF_EXPIRED_HASH, GETF_EXPIRED, GETF_NOT_FOUND */
    return NULL;
}

/* 检查指定字段是否存在于给定哈希中。如果字段已过期（HFE），
 * 则会进行惰性删除。
 *
 * hfeFlags      - HFE_LAZY_* 标志位
 * isHashDeleted - 如果访问的是已过期字段且它是哈希中的最后一个字段，
 *                 则哈希也会被删除，此时 *isHashDeleted 会被置为 1。
 *
 * 返回：1 表示字段存在，0 表示不存在。
 */
int hashTypeExists(redisDb *db, robj *o, sds field, int hfeFlags, int *isHashDeleted) {
    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    GetFieldRes res = hashTypeGetValue(db, o, field, &vstr, &vlen, &vll, hfeFlags);
    if (isHashDeleted)
        *isHashDeleted = (res == GETF_EXPIRED_HASH) ? 1 : 0;
    return (res == GETF_OK) ? 1 : 0;
}

/* 添加一个新字段，如果字段已存在则用新值覆盖旧值。
 * 插入返回 0，更新返回 1。
 *
 * 默认情况下，如果需要会对 key 和 value 的 SDS 字符串进行复制，
 * 因此调用者保留所传入字符串的所有权。但可以通过传递适当的标志
 * （可以按位或组合）来改变这一行为：
 *
 * HASH_SET_TAKE_FIELD  -- 将 SDS 字段的所有权移交给本函数
 * HASH_SET_TAKE_VALUE  -- 将 SDS 值的所有权移交给本函数
 * HASH_SET_KEEP_TTL    -- 如果字段已存在，保留其原有的 TTL
 *
 * 当使用上述标志时，调用者无需释放所传入的 SDS 字符串。
 * 由本函数负责使用这些字符串创建新条目，或在返回前释放 SDS 字符串。
 *
 * HASH_SET_COPY 对应于不传任何标志，表示默认的按需复制语义。
 */
#define HASH_SET_TAKE_FIELD  (1<<0)  /* 接管字段的所有权 */
#define HASH_SET_TAKE_VALUE  (1<<1)  /* 接管值的所有权 */
#define HASH_SET_KEEP_TTL (1<<2)     /* 保留原字段的 TTL */
#define HASH_SET_COPY 0              /* 默认：按需复制 */
int hashTypeSet(redisDb *db, robj *o, sds field, sds value, int flags) {
    int update = 0;

    /* 检查字段或值是否过长而不适合 listpack，若过长则先转换。
     * 此检查主要用于 HINCRBY* 命令。其他命令通常在更早的
     * hashTypeTryConversion 中已经处理，此处相当于空操作。 */
    if (o->encoding == OBJ_ENCODING_LISTPACK  ||
        o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        if (sdslen(field) > server.hash_max_listpack_value || sdslen(value) > server.hash_max_listpack_value)
            hashTypeConvert(o, OBJ_ENCODING_HT, &db->hexpires);
    }

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl, *fptr, *vptr;

        zl = o->ptr;
        fptr = lpFirst(zl);
        if (fptr != NULL) {
            fptr = lpFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* 获取指向 value 的指针（fptr 指向 field） */
                vptr = lpNext(zl, fptr);
                serverAssert(vptr != NULL);

                /* 替换 value */
                zl = lpReplace(zl, &vptr, (unsigned char*)value, sdslen(value));
                update = 1;
            }
        }

        if (!update) {
            /* 将新 field/value 对追加到 listpack 尾部 */
            zl = lpAppend(zl, (unsigned char*)field, sdslen(field));
            zl = lpAppend(zl, (unsigned char*)value, sdslen(value));
        }
        o->ptr = zl;

        /* 检查是否需要将 listpack 转换为哈希表 */
        if (hashTypeLength(o, 0) > server.hash_max_listpack_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT, &db->hexpires);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        unsigned char *fptr = NULL, *vptr = NULL, *tptr = NULL;
        listpackEx *lpt = o->ptr;
        long long expireTime = HASH_LP_NO_TTL;

        fptr = lpFirst(lpt->lp);
        if (fptr != NULL) {
            fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);
            if (fptr != NULL) {
                /* 获取指向 value 的指针（fptr 指向 field） */
                vptr = lpNext(lpt->lp, fptr);
                serverAssert(vptr != NULL);

                /* 替换 value */
                lpt->lp = lpReplace(lpt->lp, &vptr, (unsigned char *) value, sdslen(value));
                update = 1;

                fptr = lpPrev(lpt->lp, vptr);
                serverAssert(fptr != NULL);

                tptr = lpNext(lpt->lp, vptr);
                serverAssert(tptr && lpGetIntegerValue(tptr, &expireTime));

                if (flags & HASH_SET_KEEP_TTL) {
                    /* 保留旧字段及其 TTL */
                } else if (expireTime != HASH_LP_NO_TTL) {
                    /* 重新插入字段并覆盖 TTL */
                    listpackExUpdateExpiry(o, field, fptr, vptr, HASH_LP_NO_TTL);
                }
            }
        }

        if (!update)
            listpackExAddNew(o, field, sdslen(field), value, sdslen(value),
                             HASH_LP_NO_TTL);

        /* 检查是否需要将 listpack 转换为哈希表 */
        if (hashTypeLength(o, 0) > server.hash_max_listpack_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT, &db->hexpires);

    } else if (o->encoding == OBJ_ENCODING_HT) {
        hfield newField = hfieldNew(field, sdslen(field), 0);
        dict *ht = o->ptr;
        dictEntry *de, *existing;

        /* 存储键与查找键不同 */
        dictUseStoredKeyApi(ht, 1);
        de = dictAddRaw(ht, newField, &existing);
        dictUseStoredKeyApi(ht, 0);

        /* 如果字段已存在，则更新 "field"。"Value" 稍后设置 */
        if (de == NULL) {
            if (flags & HASH_SET_KEEP_TTL) {
                /* 保留旧字段及其 TTL */
                hfieldFree(newField);
            } else {
                /* 如果旧字段挂接了 TTL，则从哈希的私有 ebuckets 中移除 */
                hfield oldField = dictGetKey(existing);
                hfieldPersist(o, oldField);
                hfieldFree(oldField);
                dictSetKey(ht, existing, newField);
            }
            sdsfree(dictGetVal(existing));
            update = 1;
            de = existing;
        }

        if (flags & HASH_SET_TAKE_VALUE) {
            dictSetVal(ht, de, value);
            flags &= ~HASH_SET_TAKE_VALUE;
        } else {
            dictSetVal(ht, de, sdsdup(value));
        }
    } else {
        serverPanic("Unknown hash encoding");
    }

    /* 释放未被引用的 SDS 字符串，如果标志表明本函数应负责释放。 */
    if (flags & HASH_SET_TAKE_FIELD && field) sdsfree(field);
    if (flags & HASH_SET_TAKE_VALUE && value) sdsfree(value);
    return update;
}

SetExRes hashTypeSetExpiryHT(HashTypeSetEx *exInfo, sds field, uint64_t expireAt) {
    dict *ht = exInfo->hashObj->ptr;
    dictEntry *existingEntry = NULL;

    /* 使用过期元数据构造新字段 */
    hfield hfNew = hfieldNew(field, sdslen(field), 1 /*withExpireMeta*/);

    if ((existingEntry = dictFind(ht, field)) == NULL) {
        hfieldFree(hfNew);
        return HSETEX_NO_FIELD;
    }

    hfield hfOld = dictGetKey(existingEntry);

    /* 如果字段没有挂接过期元数据 */
    if (!hfieldIsExpireAttached(hfOld)) {

        /* 对于没有过期的字段，LT 条件视为满足 */
        if (exInfo->expireSetCond & (HFE_XX | HFE_GT)) {
            hfieldFree(hfNew);
            return HSETEX_NO_CONDITION_MET;
        }

        /* 删除旧字段。稍后将通过 dictSetKey(..,hfNew) 替换。 */
        hfieldFree(hfOld);

    } else { /* 字段挂接了 ExpireMeta 结构 */

        /* 不再需要 hfNew（直接修改现有字段的过期时间） */
        hfieldFree(hfNew);

        uint64_t prevExpire = hfieldGetExpireTime(hfOld);

        /* 如果字段具有有效的过期时间，则检查 GT|LT|NX */
        if (prevExpire != EB_EXPIRE_TIME_INVALID) {
            if (((exInfo->expireSetCond == HFE_GT) && (prevExpire >= expireAt)) ||
                ((exInfo->expireSetCond == HFE_LT) && (prevExpire <= expireAt)) ||
                (exInfo->expireSetCond == HFE_NX) )
                return HSETEX_NO_CONDITION_MET;

            /* 从哈希的私有 ebuckets 中移除旧过期时间 */
            dictExpireMetadata *dm = (dictExpireMetadata *) dictMetadata(ht);
            ebRemove(&dm->hfe, &hashFieldExpireBucketsType, hfOld);

            /* 跟踪最小过期时间（仅稍后更新全局 HFE DS） */
            if (exInfo->minExpireFields > prevExpire)
                exInfo->minExpireFields = prevExpire;

        } else {
            /* 字段的过期时间无效。无需调用 ebRemove() */

            /* 检查 XX|LT|GT */
            if (exInfo->expireSetCond & (HFE_XX | HFE_GT))
                return HSETEX_NO_CONDITION_MET;
        }

        /* 复用 hfOld 作为 hfNew，并通过 ebAdd() 重写其过期时间 */
        hfNew = hfOld;
    }

    dictSetKey(ht, existingEntry, hfNew);


    /* 如果已过期，则删除该字段并传播删除事件。
     * 如果是从节点（replica），则按字段有效继续处理。 */
    if (unlikely(checkAlreadyExpired(expireAt))) {
        /* 从节点不应主动删除字段 */
        propagateHashFieldDeletion(exInfo->db, exInfo->key->ptr, field, sdslen(field));
        hashTypeDelete(exInfo->hashObj, field, 1);
        server.stat_expired_subkeys++;
        exInfo->fieldDeleted++;
        return HSETEX_DELETED;
    }

    if (exInfo->minExpireFields > expireAt)
        exInfo->minExpireFields = expireAt;

    dictExpireMetadata *dm = (dictExpireMetadata *) dictMetadata(ht);
    ebAdd(&dm->hfe, &hashFieldExpireBucketsType, hfNew, expireAt);
    exInfo->fieldUpdated++;
    return HSETEX_OK;
}

/*
 * 设置字段过期时间。
 *
 * 注意必须先调用 hashTypeSetExInit()，然后再调用本函数。
 * 最后调用 hashTypeSetExDone() 进行通知和更新全局 HFE DS。
 */
SetExRes hashTypeSetEx(robj *o, sds field, uint64_t expireAt, HashTypeSetEx *exInfo)
{
    if (o->encoding == OBJ_ENCODING_LISTPACK_EX)
    {
        unsigned char *fptr = NULL, *vptr = NULL, *tptr = NULL;

        listpackEx *lpt = o->ptr;
        long long expireTime = HASH_LP_NO_TTL;

        if ((fptr = lpFirst(lpt->lp)) == NULL)
            return HSETEX_NO_FIELD;

        fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);

        if (!fptr)
            return HSETEX_NO_FIELD;

        /* 获取指向 value 的指针（fptr 指向 field） */
        vptr = lpNext(lpt->lp, fptr);
        serverAssert(vptr != NULL);

        tptr = lpNext(lpt->lp, vptr);
        serverAssert(tptr && lpGetIntegerValue(tptr, &expireTime));

        /* 更新 TTL */
        return hashTypeSetExpiryListpack(exInfo, field, fptr, vptr, tptr, expireAt);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        /* 如果需要在设置字段的同时设置过期时间 */
        return hashTypeSetExpiryHT(exInfo, field, expireAt);
    } else {
        serverPanic("Unknown hash encoding");
    }

    return HSETEX_OK; /* 不可达 */
}

/* 初始化哈希字典的过期元数据。 */
void initDictExpireMetadata(sds key, robj *o) {
    dict *ht = o->ptr;

    dictExpireMetadata *m = (dictExpireMetadata *) dictMetadata(ht);
    m->key = key;
    m->hfe = ebCreate();     /* 分配 HFE DS */
    m->expireMeta.trash = 1; /* 标记为 trash（只要尚未 ebAdd()） */
}

/* 在调用 hashTypeSetEx() 之前初始化 HashTypeSetEx 结构。 */
int hashTypeSetExInit(robj *key, robj *o, client *c, redisDb *db, const char *cmd,
                      ExpireSetCond expireSetCond, HashTypeSetEx *ex)
{
    dict *ht = o->ptr;
    ex->expireSetCond = expireSetCond;
    ex->minExpire = EB_EXPIRE_TIME_INVALID;
    ex->c = c;
    ex->cmd = cmd;
    ex->db = db;
    ex->key = key;
    ex->hashObj = o;
    ex->fieldDeleted = 0;
    ex->fieldUpdated = 0;
    ex->minExpireFields = EB_EXPIRE_TIME_INVALID;

    /* 确保 HASH 支持过期功能 */
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        hashTypeConvert(o, OBJ_ENCODING_LISTPACK_EX, &c->db->hexpires);

        listpackEx *lpt = o->ptr;
        dictEntry *de = dbFind(c->db, key->ptr);
        serverAssert(de != NULL);
        lpt->key = dictGetKey(de);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = o->ptr;

        /* 如果该哈希之前有 HFE 但之后没有了，在执行 MOVE/COPY/RENAME/RESTORE
         * 操作后，哈希中的 key 引用（lpt->key）可能会过时。这些命令
         * 仅在 HFE 存在时才维护 key 引用。也就是说，只有在 key 引用不是
         * "trash" 时才能确保其有效。
         * （TODO: 可以避免使用 dbFind()。需要扩展 lookupKey*() 使其
         *  返回 dictEntry） */
        if (lpt->meta.trash) {
            dictEntry *de = dbFind(c->db, key->ptr);
            serverAssert(de != NULL);
            lpt->key = dictGetKey(de);
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        /* 确保 dict 带有 HFE 元数据 */
        if (!isDictWithMetaHFE(ht)) {
            /* 重新分配（仅 dict 头部）以携带哈希字段过期元数据 */
            dictTypeAddMeta(&ht, &mstrHashDictTypeWithHFE);
            dictExpireMetadata *m = (dictExpireMetadata *) dictMetadata(ht);
            o->ptr = ht;

            /* 在键空间中查找该 key。需要保留对该 key 的引用，
             * 以便进行通知或删除哈希。 */
            dictEntry *de = dbFind(db, key->ptr);
            serverAssert(de != NULL);

            /* 填充 dict HFE 元数据 */
            m->key = dictGetKey(de); /* 引用键空间中的 key */
            m->hfe = ebCreate();     /* 分配 HFE DS */
            m->expireMeta.trash = 1; /* 标记为 trash（只要尚未 ebAdd()） */
        } else {
            dictExpireMetadata *m = (dictExpireMetadata *) dictMetadata(ht);
            /* 如果该哈希之前有 HFE 但之后没有了，哈希中的 key 引用
             * (m->key) 在 MOVE/COPY/RENAME/RESTORE 操作后可能已过时。
             * 这些命令仅在 HFE 存在时才维护 key 引用。
             * 也就是说，只有在 key 引用不是 "trash" 时才能确保其有效。 */
            if (m->expireMeta.trash) {
                dictEntry *de = dbFind(db, key->ptr);
                serverAssert(de != NULL);
                m->key = dictGetKey(de); /* 引用键空间中的 key */
            }
        }
    }

    /* 从附加到哈希的 ExpireMeta 中读取 minExpire */
    ex->minExpire = hashTypeGetMinExpire(o, 0);
    return C_OK;
}

/*
 * 调用 hashTypeSetEx() 设置字段或过期时间后，调用本函数
 * 进行通知和更新全局 HFE DS。
 */
void hashTypeSetExDone(HashTypeSetEx *ex) {
    /* 通知键空间事件，更新 dirty 计数，并更新全局 HFE DS */
    if (ex->fieldDeleted + ex->fieldUpdated > 0) {

        server.dirty += ex->fieldDeleted + ex->fieldUpdated;
        if (ex->fieldDeleted && hashTypeLength(ex->hashObj, 0) == 0) {
            dbDelete(ex->db,ex->key);
            signalModifiedKey(ex->c, ex->db, ex->key);
            notifyKeyspaceEvent(NOTIFY_HASH, "hdel", ex->key, ex->db->id);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",ex->key, ex->db->id);
        } else {
            signalModifiedKey(ex->c, ex->db, ex->key);
            notifyKeyspaceEvent(NOTIFY_HASH, ex->fieldDeleted ? "hdel" : "hexpire",
                                ex->key, ex->db->id);

            /* 如果哈希的最小 HFE 小于命令中指定字段的过期时间，
             * 同时也小于等于命令中提供的过期时间，则该命令
             * 不会改变哈希的最小 HFE。 */
            if ((ex->minExpire < ex->minExpireFields))
                return;

            /* 获取新的过期时间。可能会发生变化。 */
            uint64_t newMinExpire = hashTypeGetMinExpire(ex->hashObj, 1 /*accurate*/);

            /* 计算旧 minExpire 和新 minExpire 之间的差值。
             * 如果只相差几秒，则无需更新全局 HFE DS。
             * 在最坏情况下，哈希中的字段也会在几秒后被主动过期。
             *
             * 在任何情况下，主动过期操作都比在这里为单个项
             * 更新全局 HFE DS 更高效。
             */
            uint64_t diff = (ex->minExpire > newMinExpire) ?
                                (ex->minExpire - newMinExpire) : (newMinExpire - ex->minExpire);
            if (diff < HASH_NEW_EXPIRE_DIFF_THRESHOLD) return;

            if (ex->minExpire != EB_EXPIRE_TIME_INVALID)
                ebRemove(&ex->db->hexpires, &hashExpireBucketsType, ex->hashObj);
            if (newMinExpire != EB_EXPIRE_TIME_INVALID)
                ebAdd(&ex->db->hexpires, &hashExpireBucketsType, ex->hashObj, newMinExpire);
        }
    }
}

/* 从哈希中删除一个元素。
 *
 * 返回 1 表示已删除，0 表示未找到。
 * isSdsField - 1 表示字段是 sds，0 表示字段是 hfield。
 * 时间复杂度：O(1)（哈希表）或 O(N)（listpack）。 */
int hashTypeDelete(robj *o, void *field, int isSdsField) {
    int deleted = 0;
    int fieldLen = (isSdsField) ? sdslen((sds)field) : hfieldlen((hfield)field);

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl, *fptr;

        zl = o->ptr;
        fptr = lpFirst(zl);
        if (fptr != NULL) {
            fptr = lpFind(zl, fptr, (unsigned char*)field, fieldLen, 1);
            if (fptr != NULL) {
                /* 同时删除 key 和 value。 */
                zl = lpDeleteRangeWithEntry(zl,&fptr,2);
                o->ptr = zl;
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        unsigned char *fptr;
        listpackEx *lpt = o->ptr;

        fptr = lpFirst(lpt->lp);
        if (fptr != NULL) {
            fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, fieldLen, 2);
            if (fptr != NULL) {
                /* 删除 field、value 和 ttl */
                lpt->lp = lpDeleteRangeWithEntry(lpt->lp, &fptr, 3);
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        /* dictDelete() 会调用 dictHfieldDestructor() */
        dictUseStoredKeyApi((dict*)o->ptr, isSdsField ? 0 : 1);
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
            deleted = 1;
        }
        dictUseStoredKeyApi((dict*)o->ptr, 0);

    } else {
        serverPanic("Unknown hash encoding");
    }
    return deleted;
}

/* 返回哈希中的元素数量。
 *
 * 注意：当 HFE 较多时，subtractExpiredFields=1 可能会比较耗时。
 * 时间复杂度：O(1) 或 O(已过期字段数)（当 subtractExpiredFields=1 时）。 */
unsigned long hashTypeLength(const robj *o, int subtractExpiredFields) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        length = lpLength(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = o->ptr;
        length = lpLength(lpt->lp) / 3;

        if (subtractExpiredFields && lpt->meta.trash == 0)
            length -= listpackExExpireDryRun(o);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        uint64_t expiredItems = 0;
        dict *d = (dict*)o->ptr;
        if (subtractExpiredFields && isDictWithMetaHFE(d)) {
            dictExpireMetadata *meta = (dictExpireMetadata *) dictMetadata(d);
            /* 如果 dict 已注册到全局 HFE DS */
            if (meta->expireMeta.trash == 0)
                expiredItems = ebExpireDryRun(meta->hfe,
                                              &hashFieldExpireBucketsType,
                                              commandTimeSnapshot());
        }
        length = dictSize(d) - expiredItems;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return length;
}

hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    hi->subject = subject;
    hi->encoding = subject->encoding;

    if (hi->encoding == OBJ_ENCODING_LISTPACK ||
        hi->encoding == OBJ_ENCODING_LISTPACK_EX)
    {
        hi->fptr = NULL;
        hi->vptr = NULL;
        hi->tptr = NULL;
        hi->expire_time = EB_EXPIRE_TIME_INVALID;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return hi;
}

void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(hi->di);
    zfree(hi);
}

/* 移动到哈希中的下一条目。当能找到下一条目时返回 C_OK，
 * 当迭代器到达末尾时返回 C_ERR。 */
int hashTypeNext(hashTypeIterator *hi, int skipExpiredFields) {
    hi->expire_time = EB_EXPIRE_TIME_INVALID;
    if (hi->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* 初始化游标 */
            serverAssert(vptr == NULL);
            fptr = lpFirst(zl);
        } else {
            /* 前进游标 */
            serverAssert(vptr != NULL);
            fptr = lpNext(zl, vptr);
        }
        if (fptr == NULL) return C_ERR;

        /* 获取指向 value 的指针（fptr 指向 field） */
        vptr = lpNext(zl, fptr);
        serverAssert(vptr != NULL);

        /* fptr, vptr 现在指向第一对或下一对 */
        hi->fptr = fptr;
        hi->vptr = vptr;
    } else if (hi->encoding == OBJ_ENCODING_LISTPACK_EX) {
        long long expire_time;
        unsigned char *zl = hashTypeListpackGetLp(hi->subject);
        unsigned char *fptr, *vptr, *tptr;

        fptr = hi->fptr;
        vptr = hi->vptr;
        tptr = hi->tptr;

        if (fptr == NULL) {
            /* 初始化游标 */
            serverAssert(vptr == NULL);
            fptr = lpFirst(zl);
        } else {
            /* 前进游标 */
            serverAssert(tptr != NULL);
            fptr = lpNext(zl, tptr);
        }
        if (fptr == NULL) return C_ERR;

        while (fptr != NULL) {
            /* 获取指向 value 的指针（fptr 指向 field） */
            vptr = lpNext(zl, fptr);
            serverAssert(vptr != NULL);

            tptr = lpNext(zl, vptr);
            serverAssert(tptr && lpGetIntegerValue(tptr, &expire_time));

            if (!skipExpiredFields || !hashTypeIsExpired(hi->subject, expire_time))
                break;

            fptr = lpNext(zl, tptr);
        }
        if (fptr == NULL) return C_ERR;

        /* fptr, vptr now point to the first or next pair */
        hi->fptr = fptr;
        hi->vptr = vptr;
        hi->tptr = tptr;
        hi->expire_time = (expire_time != HASH_LP_NO_TTL) ? (uint64_t) expire_time : EB_EXPIRE_TIME_INVALID;
    } else if (hi->encoding == OBJ_ENCODING_HT) {

        while ((hi->de = dictNext(hi->di)) != NULL) {
            hi->expire_time = hfieldGetExpireTime(dictGetKey(hi->de));
            /* this condition still valid if expire_time equals EB_EXPIRE_TIME_INVALID */
            if (skipExpiredFields && ((mstime_t)hi->expire_time < commandTimeSnapshot()))
                continue;
            return C_OK;
        }
        return C_ERR;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_OK;
}

/* 在迭代器光标处获取字段或值，迭代的对象是 listpack 编码的哈希。
 * 原型与 `hashTypeGetFromListpack` 类似。 */
void hashTypeCurrentFromListpack(hashTypeIterator *hi, int what,
                                 unsigned char **vstr,
                                 unsigned int *vlen,
                                 long long *vll,
                                 uint64_t *expireTime)
{
    serverAssert(hi->encoding == OBJ_ENCODING_LISTPACK ||
                 hi->encoding == OBJ_ENCODING_LISTPACK_EX);

    if (what & OBJ_HASH_KEY) {
        *vstr = lpGetValue(hi->fptr, vlen, vll);
    } else {
        *vstr = lpGetValue(hi->vptr, vlen, vll);
    }

    if (expireTime)
        *expireTime = hi->expire_time;
}

/* 在迭代器光标处获取字段或值，迭代的对象是哈希表（HT）编码的哈希。
 * 原型与 `hashTypeGetFromHashTable` 类似。
 *
 * expireTime - 如果参数非空，函数将返回该字段的过期时间。
 *              如果未设置过期，则返回 EB_EXPIRE_TIME_INVALID。
 */
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, char **str, size_t *len, uint64_t *expireTime) {
    serverAssert(hi->encoding == OBJ_ENCODING_HT);
    hfield key = NULL;

    if (what & OBJ_HASH_KEY) {
        key = dictGetKey(hi->de);
        *str = key;
        *len = hfieldlen(key);
    } else {
        sds val = dictGetVal(hi->de);
        *str = val;
        *len = sdslen(val);
    }

    if (expireTime)
        *expireTime = hi->expire_time;
}

/* hashTypeCurrent*() 的更高级版本，返回迭代器当前位置的哈希值。
 *
 * 返回的元素按引用方式返回：如果以字符串形式返回，则通过 *vstr 和 *vlen；
 * 如果以数字形式返回，则通过 *vll 存储。
 *
 * 如果 *vll 被赋值，则 *vstr 会被设为 NULL，
 * 因此调用者始终可以通过检查返回值以及 vstr 是否为 NULL 来判断。 */
void hashTypeCurrentObject(hashTypeIterator *hi,
                           int what,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll,
                           uint64_t *expireTime)
{
    if (hi->encoding == OBJ_ENCODING_LISTPACK ||
        hi->encoding == OBJ_ENCODING_LISTPACK_EX)
    {
        *vstr = NULL;
        hashTypeCurrentFromListpack(hi, what, vstr, vlen, vll, expireTime);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        char *ele;
        size_t eleLen;
        hashTypeCurrentFromHashTable(hi, what, &ele, &eleLen, expireTime);
        *vstr = (unsigned char*) ele;
        *vlen = eleLen;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* 将当前迭代器位置的 key 或 value 作为新的 SDS 字符串返回。 */
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    hashTypeCurrentObject(hi,what,&vstr,&vlen,&vll, NULL);
    if (vstr) return sdsnewlen(vstr,vlen);
    return sdsfromlonglong(vll);
}

/* 将当前迭代器位置的 key 作为新的 hfield 字符串返回。 */
hfield hashTypeCurrentObjectNewHfield(hashTypeIterator *hi) {
    char buf[LONG_STR_SIZE];
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;
    uint64_t expireTime;
    hfield hf;

    hashTypeCurrentObject(hi,OBJ_HASH_KEY,&vstr,&vlen,&vll, &expireTime);

    if (!vstr) {
        vlen = ll2string(buf, sizeof(buf), vll);
        vstr = (unsigned char *) buf;
    }

    hf = hfieldNew(vstr,vlen, expireTime != EB_EXPIRE_TIME_INVALID);
    return hf;
}

static robj *hashTypeLookupWriteOrCreate(client *c, robj *key) {
    robj *o = lookupKeyWrite(c->db,key);
    if (checkType(c,o,OBJ_HASH)) return NULL;

    if (o == NULL) {
        o = createHashObject();
        dbAdd(c->db,key,o);
    }
    return o;
}


/* 将 listpack 编码的哈希转换为另一种编码。 */
void hashTypeConvertListpack(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK);

    if (enc == OBJ_ENCODING_LISTPACK) {
        /* 无需操作... */

    } else if (enc == OBJ_ENCODING_LISTPACK_EX) {
        unsigned char *p;

        /* 为每个 field - value 对追加 HASH_LP_NO_TTL。 */
        p = lpFirst(o->ptr);
        while (p != NULL) {
            p = lpNext(o->ptr, p);
            serverAssert(p);

            o->ptr = lpInsertInteger(o->ptr, HASH_LP_NO_TTL, p, LP_AFTER, &p);
            p = lpNext(o->ptr, p);
        }

        listpackEx *lpt = listpackExCreate();
        lpt->lp = o->ptr;
        o->encoding = OBJ_ENCODING_LISTPACK_EX;
        o->ptr = lpt;
    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        hi = hashTypeInitIterator(o);
        dict = dictCreate(&mstrHashDictType);

        /* 预分配 dict 大小以避免 rehash */
        dictExpand(dict,hashTypeLength(o, 0));

        while (hashTypeNext(hi, 0) != C_ERR) {

            hfield key = hashTypeCurrentObjectNewHfield(hi);
            sds value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            dictUseStoredKeyApi(dict, 1);
            ret = dictAdd(dict, key, value);
            dictUseStoredKeyApi(dict, 0);
            if (ret != DICT_OK) {
                hfieldFree(key); sdsfree(value); /* gcc ASAN 需要 */
                hashTypeReleaseIterator(hi);  /* gcc ASAN 需要 */
                serverLogHexDump(LL_WARNING,"listpack with dup elements dump",
                    o->ptr,lpBytes(o->ptr));
                serverPanic("Listpack corruption detected");
            }
        }
        hashTypeReleaseIterator(hi);
        zfree(o->ptr);
        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

void hashTypeConvertListpackEx(robj *o, int enc, ebuckets *hexpires) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK_EX);

    if (enc == OBJ_ENCODING_LISTPACK_EX) {
        return;
    } else if (enc == OBJ_ENCODING_HT) {
        int ret;
        hashTypeIterator *hi;
        dict *dict;
        dictExpireMetadata *dictExpireMeta;
        listpackEx *lpt = o->ptr;
        uint64_t minExpire = hashTypeGetMinExpire(o, 0);

        if (hexpires && lpt->meta.trash != 1)
            ebRemove(hexpires, &hashExpireBucketsType, o);

        dict = dictCreate(&mstrHashDictTypeWithHFE);
        dictExpand(dict,hashTypeLength(o, 0));
        dictExpireMeta = (dictExpireMetadata *) dictMetadata(dict);

        /* 填充 dict HFE 元数据 */
        dictExpireMeta->key = lpt->key;       /* 引用键空间中的 key */
        dictExpireMeta->hfe = ebCreate();     /* 分配 HFE DS */
        dictExpireMeta->expireMeta.trash = 1; /* 标记为 trash（只要尚未 ebAdd()） */

        hi = hashTypeInitIterator(o);

        while (hashTypeNext(hi, 0) != C_ERR) {
            hfield key = hashTypeCurrentObjectNewHfield(hi);
            sds value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            dictUseStoredKeyApi(dict, 1);
            ret = dictAdd(dict, key, value);
            dictUseStoredKeyApi(dict, 0);
            if (ret != DICT_OK) {
                hfieldFree(key); sdsfree(value); /* gcc ASAN 需要 */
                hashTypeReleaseIterator(hi);  /* gcc ASAN 需要 */
                serverLogHexDump(LL_WARNING,"listpack with dup elements dump",
                                 lpt->lp,lpBytes(lpt->lp));
                serverPanic("Listpack corruption detected");
            }

            if (hi->expire_time != EB_EXPIRE_TIME_INVALID)
                ebAdd(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, key, hi->expire_time);
        }
        hashTypeReleaseIterator(hi);
        listpackExFree(lpt);

        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;

        if (hexpires && minExpire != EB_EXPIRE_TIME_INVALID)
            ebAdd(hexpires, &hashExpireBucketsType, o, minExpire);
    } else {
        serverPanic("Unknown hash encoding: %d", enc);
    }
}

/* NOTE: hexpires can be NULL (Won't register in global HFE DS) */
void hashTypeConvert(robj *o, int enc, ebuckets *hexpires) {
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        hashTypeConvertListpack(o, enc);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        hashTypeConvertListpackEx(o, enc, hexpires);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        serverPanic("Not implemented");
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* 这是 COPY 命令的辅助函数。
 * 复制一个哈希对象，并保证返回的对象与原始对象具有相同的编码。
 *
 * 结果对象的引用计数始终被设置为 1。 */
robj *hashTypeDup(robj *o, sds newkey, uint64_t *minHashExpire) {
    robj *hobj;
    hashTypeIterator *hi;

    serverAssert(o->type == OBJ_HASH);

    if(o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = o->ptr;
        size_t sz = lpBytes(zl);
        unsigned char *new_zl = zmalloc(sz);
        memcpy(new_zl, zl, sz);
        hobj = createObject(OBJ_HASH, new_zl);
        hobj->encoding = OBJ_ENCODING_LISTPACK;
    } else if(o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = o->ptr;

        if (lpt->meta.trash == 0)
            *minHashExpire = ebGetMetaExpTime(&lpt->meta);

        listpackEx *dup = listpackExCreate();
        dup->key = newkey;

        size_t sz = lpBytes(lpt->lp);
        dup->lp = lpNew(sz);
        memcpy(dup->lp, lpt->lp, sz);

        hobj = createObject(OBJ_HASH, dup);
        hobj->encoding = OBJ_ENCODING_LISTPACK_EX;
    } else if(o->encoding == OBJ_ENCODING_HT) {
        dictExpireMetadata *dictExpireMetaSrc, *dictExpireMetaDst = NULL;
        dict *d;

        /* 如果 dict 没有 HFE 元数据，则创建一个没有 HFE 元数据的新 dict */
        if (!isDictWithMetaHFE(o->ptr)) {
            d = dictCreate(&mstrHashDictType);
        } else {
            /* 创建一个带有 HFE 元数据的新 dict */
            d = dictCreate(&mstrHashDictTypeWithHFE);
            dictExpireMetaSrc = (dictExpireMetadata *) dictMetadata((dict *) o->ptr);
            dictExpireMetaDst = (dictExpireMetadata *) dictMetadata(d);
            dictExpireMetaDst->key = newkey;         /* 引用键空间中的 key */
            dictExpireMetaDst->hfe = ebCreate();     /* 分配 HFE DS */
            dictExpireMetaDst->expireMeta.trash = 1; /* 标记为 trash（只要尚未 ebAdd()） */

            /* 提取源哈希的最小过期时间（调用者将使用此值
             * 将新哈希注册到全局 ebuckets，即 db->hexpires） */
            if (dictExpireMetaSrc->expireMeta.trash == 0)
                *minHashExpire = ebGetMetaExpTime(&dictExpireMetaSrc->expireMeta);
        }
        dictExpand(d, dictSize((const dict*)o->ptr));

        hi = hashTypeInitIterator(o);
        while (hashTypeNext(hi, 0) != C_ERR) {
            uint64_t expireTime;
            sds newfield, newvalue;
            /* 从原始哈希对象中提取一个 field-value 对。*/
            char *field, *value;
            size_t fieldLen, valueLen;
            hashTypeCurrentFromHashTable(hi, OBJ_HASH_KEY, &field, &fieldLen, &expireTime);
            if (expireTime == EB_EXPIRE_TIME_INVALID) {
                newfield = hfieldNew(field, fieldLen, 0);
            } else {
                newfield = hfieldNew(field, fieldLen, 1);
                ebAdd(&dictExpireMetaDst->hfe, &hashFieldExpireBucketsType, newfield, expireTime);
            }

            hashTypeCurrentFromHashTable(hi, OBJ_HASH_VALUE, &value, &valueLen, NULL);
            newvalue = sdsnewlen(value, valueLen);

            /* 将 field-value 对添加到新哈希对象中。 */
            dictUseStoredKeyApi(d, 1);
            dictAdd(d,newfield,newvalue);
            dictUseStoredKeyApi(d, 0);
        }
        hashTypeReleaseIterator(hi);

        hobj = createObject(OBJ_HASH, d);
        hobj->encoding = OBJ_ENCODING_HT;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return hobj;
}

/* 从 listpack 条目创建一个新的 sds 字符串。 */
sds hashSdsFromListpackEntry(listpackEntry *e) {
    return e->sval ? sdsnewlen(e->sval, e->slen) : sdsfromlonglong(e->lval);
}

/* 从 listpack 条目以 bulk string 形式回复客户端。 */
void hashReplyFromListpackEntry(client *c, listpackEntry *e) {
    if (e->sval)
        addReplyBulkCBuffer(c, e->sval, e->slen);
    else
        addReplyBulkLongLong(c, e->lval);
}

/* 从非空哈希中返回一个随机元素。
 * 'key' 和 'val' 会被设置为该元素。
 * 它们指向的内存不应由调用者释放或修改。
 * 'val' 可以为 NULL，此时不会提取值。 */
void hashTypeRandomElement(robj *hashobj, unsigned long hashsize, CommonEntry *key, CommonEntry *val) {
    if (hashobj->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictGetFairRandomKey(hashobj->ptr);
        hfield field = dictGetKey(de);
        key->sval = (unsigned char*)field;
        key->slen = hfieldlen(field);
        if (val) {
            sds s = dictGetVal(de);
            val->sval = (unsigned char*)s;
            val->slen = sdslen(s);
        }
    } else if (hashobj->encoding == OBJ_ENCODING_LISTPACK) {
        lpRandomPair(hashobj->ptr, hashsize, (listpackEntry *) key, (listpackEntry *) val, 2);
    } else if (hashobj->encoding == OBJ_ENCODING_LISTPACK_EX) {
        lpRandomPair(hashTypeListpackGetLp(hashobj), hashsize, (listpackEntry *) key,
                     (listpackEntry *) val, 3);
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/*
 * 哈希字段的主动过期
 *
 * 由 hashTypeDbActiveExpire() 针对每个在 HFE DB (db->hexpires) 中注册，
 * 且过期时间小于或等于当前时间的哈希调用。
 *
 * 此回调对每个哈希执行以下操作：
 * - 通过调用 ebExpire(hash) 删除已过期字段
 * - 之后如果还有待过期字段，则返回 ACT_UPDATE_EXP_ITEM 将该哈希
 *   在 HFE DB 中的过期时间更新为下一个最早的字段过期时间
 * - 如果哈希没有更多待过期字段，则通过返回 ACT_REMOVE_EXP_ITEM
 *   将其从 HFE DB 中移除
 * - 之后如果哈希已无任何字段，则会从键空间中删除该哈希。
 */
static ExpireAction hashTypeActiveExpire(eItem item, void *ctx) {
    ExpireCtx *expireCtx = ctx;

    /* 如果此回调的配额已用尽，则停止 */
    if (expireCtx->fieldsToExpireQuota == 0)
        return ACT_STOP_ACTIVE_EXP;

    uint64_t nextExpTime = hashTypeExpire((robj *) item, expireCtx, 0);

    /* 如果哈希没有更多字段可过期或已被删除，
     * 通知调用方 ebExpire() 将其从 HFE DB 中移除。 */
    if (nextExpTime == EB_EXPIRE_TIME_INVALID || nextExpTime == 0) {
        return ACT_REMOVE_EXP_ITEM;
    } else {
        /* 哈希还有更多字段要过期。更新哈希的下一个过期时间，
         * 并通知调用方将其重新添加到全局 HFE DS */
        ebSetMetaExpTime(hashGetExpireMeta(item), nextExpTime);
        return ACT_UPDATE_EXP_ITEM;
    }
}

/* 删除哈希中所有已过期字段，并在哈希为空时删除该哈希。
 *
 * updateGlobalHFE - 如果在删除已过期字段后应当用新的过期时间
 *                   更新全局 HFE DS 中的该哈希，则置 1。
 *
 * 返回该哈希的下一个过期时间：
 * - 0 表示哈希已被删除
 * - EB_EXPIRE_TIME_INVALID 表示没有更多字段需要过期
 */
static uint64_t hashTypeExpire(robj *o, ExpireCtx *expireCtx, int updateGlobalHFE) {
    uint64_t noExpireLeftRes = EB_EXPIRE_TIME_INVALID;
    redisDb *db = expireCtx->db;
    sds keystr = NULL;
    ExpireInfo info = {0};

    if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        info = (ExpireInfo) {
                .maxToExpire = expireCtx->fieldsToExpireQuota,
                .now = commandTimeSnapshot(),
                .itemsExpired = 0};

        listpackExExpire(db, o, &info);
        keystr = ((listpackEx*)o->ptr)->key;
    } else {
        serverAssert(o->encoding == OBJ_ENCODING_HT);

        dict *d = o->ptr;
        dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);

        OnFieldExpireCtx onFieldExpireCtx = { .hashObj = o, .db = db };

        info = (ExpireInfo){
            .maxToExpire = expireCtx->fieldsToExpireQuota,
            .onExpireItem = onFieldExpire,
            .ctx = &onFieldExpireCtx,
            .now = commandTimeSnapshot()
        };

        ebExpire(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, &info);
        keystr = dictExpireMeta->key;
    }

    /* 更新剩余配额 */
    expireCtx->fieldsToExpireQuota -= info.itemsExpired;

    /* 在某些情况下，字段可能在未更新全局 DS 的情况下被删除。
     * 结果，主动过期可能不会过期任何字段，
     * 在这种情况下，我们无需为该 key 发送通知或执行其他操作。 */
    if (info.itemsExpired) {
        robj *key = createStringObject(keystr, sdslen(keystr));
        notifyKeyspaceEvent(NOTIFY_HASH, "hexpired", key, db->id);

        if (updateGlobalHFE)
            ebRemove(&db->hexpires, &hashExpireBucketsType, o);

        if (hashTypeLength(o, 0) == 0) {
            dbDelete(db, key);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, db->id);
            noExpireLeftRes = 0;
        } else {
            if ((updateGlobalHFE) && (info.nextExpireTime != EB_EXPIRE_TIME_INVALID))
                ebAdd(&db->hexpires, &hashExpireBucketsType, o, info.nextExpireTime);
        }

        signalModifiedKey(NULL, db, key);
        decrRefCount(key);
    }

    /* return 0 if hash got deleted, EB_EXPIRE_TIME_INVALID if no more fields
     * with expiration. Else return next expiration time */
    return (info.nextExpireTime == EB_EXPIRE_TIME_INVALID) ? noExpireLeftRes : info.nextExpireTime;
}

/* 如有必要，删除哈希中所有已过期字段（目前仅由 HRANDFIELD 使用）。
 *
 * 如果整个哈希被删除则返回 1，否则返回 0。
 * 如果存在大量已过期字段，本函数可能比较耗时。
 */
static int hashTypeExpireIfNeeded(redisDb *db, robj *o) {
    uint64_t nextExpireTime;
    uint64_t minExpire = hashTypeGetMinExpire(o, 1 /*accurate*/);

    /* 无需过期 */
    if ((mstime_t) minExpire >= commandTimeSnapshot())
        return 0;

    /* 参照 expireIfNeeded() 中不进行惰性过期的条件 */
    if ( (server.loading) ||
         (server.lazy_expire_disabled) ||
         (server.masterhost) ||  /* 主节点客户端或用户客户端，不删除 */
         (isPausedActionsWithUpdate(PAUSE_ACTION_EXPIRE)))
        return 0;

    /* 注意过期所有字段 */
    ExpireCtx expireCtx = { .db = db, .fieldsToExpireQuota = UINT32_MAX };
    nextExpireTime = hashTypeExpire(o, &expireCtx, 1);
    /* return 1 if the entire hash was deleted */
    return nextExpireTime == 0;
}

/* 返回哈希字段的下一个/最小过期时间。
 * accurate=1 - 通过查找对象数据结构返回精确时间。
 * accurate=0 - 返回 expireMeta 中维护的最小过期时间
 *              （使用前需确认其不是 trash），由于优化原因可能并不精确。
 *
 * 如果未找到，返回 EB_EXPIRE_TIME_INVALID。
 */
uint64_t hashTypeGetMinExpire(robj *o, int accurate) {
    ExpireMeta *expireMeta = NULL;

    if (!accurate) {
        if (o->encoding == OBJ_ENCODING_LISTPACK) {
            return EB_EXPIRE_TIME_INVALID;
        } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
            listpackEx *lpt = o->ptr;
            expireMeta = &lpt->meta;
        } else {
            serverAssert(o->encoding == OBJ_ENCODING_HT);

            dict *d = o->ptr;
            if (!isDictWithMetaHFE(d))
                return EB_EXPIRE_TIME_INVALID;

            expireMeta = &((dictExpireMetadata *) dictMetadata(d))->expireMeta;
        }

        /* 在更新 HFE DS 之前保留下一个哈希字段过期时间。
         * 验证其不是 trash。 */
        if (expireMeta->trash == 1)
            return EB_EXPIRE_TIME_INVALID;

        return ebGetMetaExpTime(expireMeta);
    }

    /* accurate == 1 */

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        return EB_EXPIRE_TIME_INVALID;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        return listpackExGetMinExpire(o);
    } else {
        serverAssert(o->encoding == OBJ_ENCODING_HT);

        dict *d = o->ptr;
        if (!isDictWithMetaHFE(d))
            return EB_EXPIRE_TIME_INVALID;

        dictExpireMetadata *expireMeta = (dictExpireMetadata *) dictMetadata(d);
        return ebGetNextTimeToExpire(expireMeta->hfe, &hashFieldExpireBucketsType);
    }
}

uint64_t hashTypeRemoveFromExpires(ebuckets *hexpires, robj *o) {
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        return EB_EXPIRE_TIME_INVALID;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        /* 如果 dict 没有携带 HFE 元数据 */
        if (!isDictWithMetaHFE(o->ptr))
            return EB_EXPIRE_TIME_INVALID;
    }

    uint64_t expireTime = ebGetExpireTime(&hashExpireBucketsType, o);

    /* 如果已注册到全局 HFE DS，则移除（不是 trash） */
    if (expireTime != EB_EXPIRE_TIME_INVALID)
        ebRemove(hexpires, &hashExpireBucketsType, o);

    return expireTime;
}

int hashTypeIsFieldsWithExpire(robj *o) {
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        return 0;
    } else if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        return EB_EXPIRE_TIME_INVALID != listpackExGetMinExpire(o);
    } else { /* o->encoding == OBJ_ENCODING_HT */
        dict *d = o->ptr;
        /* 如果 dict 没有携带 HFE 元数据 */
        if (!isDictWithMetaHFE(d))
            return 0;
        dictExpireMetadata *meta = (dictExpireMetadata *) dictMetadata(d);
        return ebGetTotalItems(meta->hfe, &hashFieldExpireBucketsType) != 0;
    }
}

/* 将哈希添加到全局 HFE DS，并更新通知所需的 key。
 *
 * key         - 必须是持久化在 db->dict 中的同一个 key 实例。
 * expireTime  - 过期时间（毫秒）。
 *               如果为 0，则哈希将以已经预先写入附加元数据
 *               （在未挂接到全局 HFE DS 之前被视为 trash）的最小过期时间
 *               被添加到全局 HFE DS。
 *
 * 前置条件：哈希是 listpackex 类型或带有 HFE 元数据的 HT 类型。
 */
void hashTypeAddToExpires(redisDb *db, sds key, robj *hashObj, uint64_t expireTime) {
    if (expireTime > EB_EXPIRE_TIME_MAX)
         return;

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = hashObj->ptr;
        lpt->key = key;
        expireTime = (expireTime) ? expireTime : ebGetMetaExpTime(&lpt->meta);
        ebAdd(&db->hexpires, &hashExpireBucketsType, hashObj, expireTime);
    } else if (hashObj->encoding == OBJ_ENCODING_HT) {
        dict *d = hashObj->ptr;
        if (isDictWithMetaHFE(d)) {
            dictExpireMetadata *meta = (dictExpireMetadata *) dictMetadata(d);
            expireTime = (expireTime) ? expireTime : ebGetMetaExpTime(&meta->expireMeta);
            meta->key = key;
            ebAdd(&db->hexpires, &hashExpireBucketsType, hashObj, expireTime);
        }
    }
}

/* 数据库级主动过期，并更新字段上具有时间过期的哈希。
 *
 * 对每个在 HFE DB (db->hexpires) 中注册且过期时间小于或等于当前时间的哈希，
 * 都会调用回调函数 hashTypeActiveExpire()。该回调对每个哈希执行以下操作：
 * - 如果哈希有一个或多个字段需要过期，则删除这些字段。
 * - 如果还有更多字段需要过期，则在 HFE DB 中用下一个过期时间更新该哈希。
 * - 如果哈希没有更多字段需要过期，则将其从 HFE DB 中移除。
 * - 如果哈希已无任何字段，则将其从主 DB 中删除。
 *
 * 返回主动过期的字段数量。
 */
uint64_t hashTypeDbActiveExpire(redisDb *db, uint32_t maxFieldsToExpire) {
    ExpireCtx ctx = { .db = db, .fieldsToExpireQuota = maxFieldsToExpire };
    ExpireInfo info = {
            .maxToExpire = UINT64_MAX, /* Only maxFieldsToExpire play a role */
            .onExpireItem = hashTypeActiveExpire,
            .ctx = &ctx,
            .now = commandTimeSnapshot(),
            .itemsExpired = 0};

    ebExpire(&db->hexpires, &hashExpireBucketsType, &info);

    /* 返回主动过期的字段数量 */
    return maxFieldsToExpire - ctx.fieldsToExpireQuota;
}

void hashTypeFree(robj *o) {
    switch (o->encoding) {
        case OBJ_ENCODING_HT:
            /* 验证哈希未注册到全局 HFE DS */
            if (isDictWithMetaHFE((dict*)o->ptr)) {
                dictExpireMetadata *m = (dictExpireMetadata *)dictMetadata((dict*)o->ptr);
                serverAssert(m->expireMeta.trash == 1);
            }
            dictRelease((dict*) o->ptr);
            break;
        case OBJ_ENCODING_LISTPACK:
            lpFree(o->ptr);
            break;
        case OBJ_ENCODING_LISTPACK_EX:
            /* 验证哈希未注册到全局 HFE DS */
            serverAssert(((listpackEx *) o->ptr)->meta.trash == 1);
            listpackExFree(o->ptr);
            break;
        default:
            serverPanic("Unknown hash encoding type");
            break;
    }
}

/* 尝试更新对新 key 的引用。目前仅在 defrag 中使用。 */
void hashTypeUpdateKeyRef(robj *o, sds newkey) {
    if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = o->ptr;
        lpt->key = newkey;
    } else if (o->encoding == OBJ_ENCODING_HT && isDictWithMetaHFE(o->ptr)) {
        dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *)dictMetadata((dict*)o->ptr);
        dictExpireMeta->key = newkey;
    } else {
        /* 无需操作。 */
    }
}

ebuckets *hashTypeGetDictMetaHFE(dict *d) {
    dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
    return &dictExpireMeta->hfe;
}

/*-----------------------------------------------------------------------------
 * 哈希类型命令
 *----------------------------------------------------------------------------*/

/* HSETNX 命令实现。
 * 仅当字段不存在时设置其值。
 * 设置成功返回 1，字段已存在返回 0。
 * 时间复杂度：O(1) */
void hsetnxCommand(client *c) {
    int isHashDeleted;
    robj *o;
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    if (hashTypeExists(c->db, o, c->argv[2]->ptr, HFE_LAZY_EXPIRE, &isHashDeleted)) {
        addReply(c, shared.czero);
        return;
    }

    /* 字段已过期进而导致哈希被删除。创建新哈希！ */
    if (isHashDeleted) {
        o = createHashObject();
        dbAdd(c->db,c->argv[1],o);
    }

    hashTypeTryConversion(c->db, o,c->argv,2,3);
    hashTypeSet(c->db, o,c->argv[2]->ptr,c->argv[3]->ptr,HASH_SET_COPY);
    addReply(c, shared.cone);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    server.dirty++;
}

/* HSET 命令实现。
 * 在哈希中设置一个或多个 field-value 对。
 * 返回新添加的字段数量（HSET）。
 * HMSET（已弃用）返回 OK 字符串。
 * 时间复杂度：O(N)，N 为要设置的字段数。 */
void hsetCommand(client *c) {
    int i, created = 0;
    robj *o;

    if ((c->argc % 2) == 1) {
        addReplyErrorArity(c);
        return;
    }

    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    hashTypeTryConversion(c->db,o,c->argv,2,c->argc-1);

    for (i = 2; i < c->argc; i += 2)
        created += !hashTypeSet(c->db, o,c->argv[i]->ptr,c->argv[i+1]->ptr,HASH_SET_COPY);

    /* HMSET (deprecated) and HSET return value is different. */
    char *cmdname = c->argv[0]->ptr;
    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
        addReplyLongLong(c, created);
    } else {
        /* HMSET */
        addReply(c, shared.ok);
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    server.dirty += (c->argc - 2)/2;
}

/* HINCRBY 命令实现。
 * 将哈希中指定字段的整数值增加指定增量。返回更新后的值。
 * 时间复杂度：O(1) */
void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    GetFieldRes res = hashTypeGetValue(c->db,o,c->argv[2]->ptr,&vstr,&vlen,&value,
                                       HFE_LAZY_EXPIRE);
    if (res == GETF_OK) {
        if (vstr) {
            if (string2ll((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not an integer");
                return;
            }
        } /* 否则 hashTypeGetValue() 已经将其存储到 &value 中 */
    } else if ((res == GETF_NOT_FOUND) || (res == GETF_EXPIRED)) {
        value = 0;
    } else {
        /* 字段已过期进而导致哈希被删除。创建新哈希！ */
        o = createHashObject();
        dbAdd(c->db,c->argv[1],o);
        value = 0;
    }

    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    value += incr;
    new = sdsfromlonglong(value);
    hashTypeSet(c->db, o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE | HASH_SET_KEEP_TTL);
    addReplyLongLong(c,value);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    server.dirty++;
}

/* HINCRBYFLOAT 命令实现。
 * 将哈希中指定字段的浮点数值增加指定增量。返回更新后的值。
 * 时间复杂度：O(1) */
void hincrbyfloatCommand(client *c) {
    long double value, incr;
    long long ll;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    if (isnan(incr) || isinf(incr)) {
        addReplyError(c,"value is NaN or Infinity");
        return;
    }
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    GetFieldRes res = hashTypeGetValue(c->db, o,c->argv[2]->ptr,&vstr,&vlen,&ll,
                                       HFE_LAZY_EXPIRE);
    if (res == GETF_OK) {
        if (vstr) {
            if (string2ld((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not a float");
                return;
            }
        } else {
            value = (long double)ll;
        }
    } else if ((res == GETF_NOT_FOUND) || (res == GETF_EXPIRED)) {
        value = 0;
    } else {
        /* 字段已过期进而导致哈希被删除。创建新哈希！ */
        o = createHashObject();
        dbAdd(c->db,c->argv[1],o);
        value = 0;
    }

    value += incr;
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }

    char buf[MAX_LONG_DOUBLE_CHARS];
    int len = ld2string(buf,sizeof(buf),value,LD_STR_HUMAN);
    new = sdsnewlen(buf,len);
    hashTypeSet(c->db, o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE | HASH_SET_KEEP_TTL);
    addReplyBulkCBuffer(c,buf,len);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    server.dirty++;

    /* 始终将 HINCRBYFLOAT 复制为带有最终值的 HSET 命令，
     * 以确保浮点精度或格式上的差异不会在从节点上
     * 或 AOF 重启后产生不一致。 */
    robj *newobj;
    newobj = createRawStringObject(buf,len);
    rewriteClientCommandArgument(c,0,shared.hset);
    rewriteClientCommandArgument(c,3,newobj);
    decrRefCount(newobj);
}

static GetFieldRes addHashFieldToReply(client *c, robj *o, sds field, int hfeFlags) {
    if (o == NULL) {
        addReplyNull(c);
        return GETF_NOT_FOUND;
    }

    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    GetFieldRes res = hashTypeGetValue(c->db, o, field, &vstr, &vlen, &vll, hfeFlags);
    if (res == GETF_OK) {
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, vll);
        }
    } else {
        addReplyNull(c);
    }
    return res;
}

/* HGET 命令实现。
 * 返回指定字段的值；字段不存在返回 nil。
 * 时间复杂度：O(1) */
void hgetCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    addHashFieldToReply(c, o, c->argv[2]->ptr, HFE_LAZY_EXPIRE);
}

/* HMGET 命令实现。
 * 一次返回多个字段的值；不存在的字段返回 nil。
 * 时间复杂度：O(N)，N 为要获取的字段数。 */
void hmgetCommand(client *c) {
    GetFieldRes res = GETF_OK;
    robj *o;
    int i;
    int expired = 0, deleted = 0;

    /* 当 key 找不到时不要中止。不存在的 key 等价于空哈希，
     * HMGET 应返回一系列 null bulk。 */
    o = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c,o,OBJ_HASH)) return;

    addReplyArrayLen(c, c->argc-2);
    for (i = 2; i < c->argc ; i++) {
        if (!deleted) {
            res = addHashFieldToReply(c, o, c->argv[i]->ptr, HFE_LAZY_NO_NOTIFICATION);
            expired += (res == GETF_EXPIRED);
            deleted += (res == GETF_EXPIRED_HASH);
        } else {
            /* 如果哈希因所有字段都过期而被惰性删除（o 已无效），
             * 则用 null 填充剩余响应并返回。 */
            addReplyNull(c);
        }
    }

    if (expired) {
        notifyKeyspaceEvent(NOTIFY_HASH, "hexpired", c->argv[1], c->db->id);
        if (deleted)
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id); 
    }
}

/* HDEL 命令实现。
 * 从哈希中删除一个或多个指定字段。返回被成功删除的字段数量。
 * 时间复杂度：O(N)，N 为要删除的字段数。 */
void hdelCommand(client *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    /* 哈希字段过期经过优化，以避免在每次字段删除时频繁更新全局 HFE DS。
     * 最终主动过期会运行并优雅地更新全局 HFE DS 或从中移除哈希。
     * 但是，如果这是最后一个带过期的字段，则 "subexpiry" 统计信息
     * 可能会向用户反映错误的 HFE 哈希数量。下面的逻辑检查这
     * 是否确实是最后一个带过期的字段，并将其从全局 HFE DS 中移除。 */
    int isHFE = hashTypeIsFieldsWithExpire(o);

    for (j = 2; j < c->argc; j++) {
        if (hashTypeDelete(o,c->argv[j]->ptr,1)) {
            deleted++;
            if (hashTypeLength(o, 0) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    if (deleted) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
        if (keyremoved) {
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        } else {
            if (isHFE && (hashTypeIsFieldsWithExpire(o) == 0)) /* is it last HFE */
                ebRemove(&c->db->hexpires, &hashExpireBucketsType, o);
        }

        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

/* HLEN 命令实现。
 * 返回哈希中包含的字段数量。
 * 时间复杂度：O(1) */
void hlenCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    addReplyLongLong(c,hashTypeLength(o, 0));
}

/* HSTRLEN 命令实现。
 * 返回哈希中指定字段值的长度。字段不存在返回 0。
 * 时间复杂度：O(1) */
void hstrlenCommand(client *c) {
    robj *o;
    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    GetFieldRes res = hashTypeGetValue(c->db, o, c->argv[2]->ptr, &vstr, &vlen, &vll,
                                       HFE_LAZY_EXPIRE);

    if (res == GETF_NOT_FOUND || res == GETF_EXPIRED || res == GETF_EXPIRED_HASH) {
        addReply(c, shared.czero);
        return;
    }

    size_t len = vstr ? vlen : sdigits10(vll);
    addReplyLongLong(c,len);
}

static void addHashIteratorCursorToReply(client *c, hashTypeIterator *hi, int what) {
    if (hi->encoding == OBJ_ENCODING_LISTPACK ||
        hi->encoding == OBJ_ENCODING_LISTPACK_EX)
    {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromListpack(hi, what, &vstr, &vlen, &vll, NULL);
        if (vstr)
            addReplyBulkCBuffer(c, vstr, vlen);
        else
            addReplyBulkLongLong(c, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        char *value;
        size_t len;
        hashTypeCurrentFromHashTable(hi, what, &value, &len, NULL);
        addReplyBulkCBuffer(c, value, len);
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* HGETALL/HKEYS/HVALS 命令的通用实现。
 * 根据 flags 决定返回 key、value 或两者。
 * 时间复杂度：O(N) */
void genericHgetallCommand(client *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int length, count = 0;

    robj *emptyResp = (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) ?
        shared.emptymap[c->resp] : shared.emptyarray;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],emptyResp))
        == NULL || checkType(c,o,OBJ_HASH)) return;

    /* 如果用户同时请求 key 和 value（如 HGETALL 命令），
     * 则以 map 形式返回；否则使用扁平数组更合适。 */
    length = hashTypeLength(o, 1 /*subtractExpiredFields*/);
    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) {
        addReplyMapLen(c, length);
    } else {
        addReplyArrayLen(c, length);
    }

    hi = hashTypeInitIterator(o);

    /* 如果哈希在全局 HFE DS 中设置了过期时间，则跳过已过期字段。
     * 这里也可以直接置为 1，但那样会为每个字段过期做一次额外查找。 */
    int skipExpiredFields = (EB_EXPIRE_TIME_INVALID == hashTypeGetMinExpire(o, 0)) ? 0 : 1;

    while (hashTypeNext(hi, skipExpiredFields) != C_ERR) {
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            count++;
        }
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
            count++;
        }
    }

    hashTypeReleaseIterator(hi);

    /* 确保返回的元素数量正确。 */
    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) count /= 2;
    serverAssert(count == length);
}

/* HKEYS 命令实现。
 * 返回哈希中所有字段名。
 * 时间复杂度：O(N) */
void hkeysCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY);
}

/* HVALS 命令实现。
 * 返回哈希中所有字段的值。
 * 时间复杂度：O(N) */
void hvalsCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_VALUE);
}

/* HGETALL 命令实现。
 * 返回哈希中所有字段及其值。
 * 时间复杂度：O(N) */
void hgetallCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

/* HEXISTS 命令实现。
 * 判断哈希中是否存在指定字段。存在返回 1，否则返回 0。
 * 时间复杂度：O(1) */
void hexistsCommand(client *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    addReply(c,hashTypeExists(c->db,o,c->argv[2]->ptr,HFE_LAZY_EXPIRE, NULL) ?
                                shared.cone : shared.czero);
}

/* HSCAN 命令实现。
 * 增量迭代哈希中的 field-value 对。
 * 时间复杂度：O(1) 每次调用（总体 O(N)）。 */
void hscanCommand(client *c) {
    robj *o;
    unsigned long long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    scanGenericCommand(c,o,cursor);
}

static void hrandfieldReplyWithListpack(client *c, unsigned int count, listpackEntry *keys, listpackEntry *vals) {
    for (unsigned long i = 0; i < count; i++) {
        if (vals && c->resp > 2)
            addReplyArrayLen(c,2);
        if (keys[i].sval)
            addReplyBulkCBuffer(c, keys[i].sval, keys[i].slen);
        else
            addReplyBulkLongLong(c, keys[i].lval);
        if (vals) {
            if (vals[i].sval)
                addReplyBulkCBuffer(c, vals[i].sval, vals[i].slen);
            else
                addReplyBulkLongLong(c, vals[i].lval);
        }
    }
}

/* 与请求大小相比，哈希需要多大才不会采用"删除元素"策略？
 * 请参考后续实现中的说明。 */
#define HRANDFIELD_SUB_STRATEGY_MUL 3

/* 如果客户端请求非常多的随机元素，队列可能会消耗无限多的内存，
 * 因此我们需要限制每次随机的数量。 */
#define HRANDFIELD_RANDOM_SAMPLE_LIMIT 1000

/* HRANDFIELD count [WITHVALUES] 命令的内部实现。
 * 详见 hrandfieldCommand() 中的说明。 */
void hrandfieldWithCountCommand(client *c, long l, int withvalues) {
    unsigned long count, size;
    int uniq = 1;
    robj *hash;

    if ((hash = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray))
        == NULL || checkType(c,hash,OBJ_HASH)) return;

    if(l >= 0) {
        count = (unsigned long) l;
    } else {
        count = -l;
        uniq = 0;
    }

    /* 删除所有已过期字段。如果整个哈希都被删除则返回空数组。 */
    if (hashTypeExpireIfNeeded(c->db, hash)) {
        addReply(c, shared.emptyarray);
        return;
    }

    /* 删除已过期字段 */
    size = hashTypeLength(hash, 0);

    /* 如果 count 为 0，立即响应以避免后续特殊处理。 */
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */
    if (!uniq || count == 1) {
        if (withvalues && c->resp == 2)
            addReplyArrayLen(c, count*2);
        else
            addReplyArrayLen(c, count);
        if (hash->encoding == OBJ_ENCODING_HT) {
            while (count--) {
                dictEntry *de = dictGetFairRandomKey(hash->ptr);
                hfield field = dictGetKey(de);
                sds value = dictGetVal(de);
                if (withvalues && c->resp > 2)
                    addReplyArrayLen(c,2);
                addReplyBulkCBuffer(c, field, hfieldlen(field));
                if (withvalues)
                    addReplyBulkCBuffer(c, value, sdslen(value));
                if (c->flags & CLIENT_CLOSE_ASAP)
                    break;
            }
        } else if (hash->encoding == OBJ_ENCODING_LISTPACK ||
                   hash->encoding == OBJ_ENCODING_LISTPACK_EX)
        {
            listpackEntry *keys, *vals = NULL;
            unsigned long limit, sample_count;
            unsigned char *lp = hashTypeListpackGetLp(hash);
            int tuple_len = hash->encoding == OBJ_ENCODING_LISTPACK ? 2 : 3;

            limit = count > HRANDFIELD_RANDOM_SAMPLE_LIMIT ? HRANDFIELD_RANDOM_SAMPLE_LIMIT : count;
            keys = zmalloc(sizeof(listpackEntry)*limit);
            if (withvalues)
                vals = zmalloc(sizeof(listpackEntry)*limit);
            while (count) {
                sample_count = count > limit ? limit : count;
                count -= sample_count;
                lpRandomPairs(lp, sample_count, keys, vals, tuple_len);
                hrandfieldReplyWithListpack(c, sample_count, keys, vals);
                if (c->flags & CLIENT_CLOSE_ASAP)
                    break;
            }
            zfree(keys);
            zfree(vals);
        }
        return;
    }

    /* 初始化回复数量：RESP3 返回嵌套数组，RESP2 返回扁平数组。 */
    long reply_size = count < size ? count : size;
    if (withvalues && c->resp == 2)
        addReplyArrayLen(c, reply_size*2);
    else
        addReplyArrayLen(c, reply_size);

    /* CASE 2:
    * The number of requested elements is greater than the number of
    * elements inside the hash: simply return the whole hash. */
    if(count >= size) {
        hashTypeIterator *hi = hashTypeInitIterator(hash);
        while (hashTypeNext(hi, 0) != C_ERR) {
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            if (withvalues)
                addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
        }
        hashTypeReleaseIterator(hi);
        return;
    }

    /* CASE 2.5 listpack only. Sampling unique elements, in non-random order.
     * Listpack encoded hashes are meant to be relatively small, so
     * HRANDFIELD_SUB_STRATEGY_MUL isn't necessary and we rather not make
     * copies of the entries. Instead, we emit them directly to the output
     * buffer.
     *
     * And it is inefficient to repeatedly pick one random element from a
     * listpack in CASE 4. So we use this instead. */
    if (hash->encoding == OBJ_ENCODING_LISTPACK ||
        hash->encoding == OBJ_ENCODING_LISTPACK_EX)
    {
        unsigned char *lp = hashTypeListpackGetLp(hash);
        int tuple_len = hash->encoding == OBJ_ENCODING_LISTPACK ? 2 : 3;
        listpackEntry *keys, *vals = NULL;
        keys = zmalloc(sizeof(listpackEntry)*count);
        if (withvalues)
            vals = zmalloc(sizeof(listpackEntry)*count);
        serverAssert(lpRandomPairsUnique(lp, count, keys, vals, tuple_len) == count);
        hrandfieldReplyWithListpack(c, count, keys, vals);
        zfree(keys);
        zfree(vals);
        return;
    }

    /* CASE 3:
     * The number of elements inside the hash of type dict is not greater than
     * HRANDFIELD_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create an array of dictEntry pointers from the original hash,
     * and subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the hash, the natural approach
     * used into CASE 4 is highly inefficient. */
    if (count*HRANDFIELD_SUB_STRATEGY_MUL > size) {
        /* 哈希表编码（通用实现） */
        dict *ht = hash->ptr;
        dictIterator *di;
        dictEntry *de;
        unsigned long idx = 0;

        /* 分配一个临时数组，存储 dict 中键值对的指针，
         * 并通过删除随机元素来达到所需数量。 */
        struct FieldValPair {
            hfield field;
            sds value;
        } *pairs = zmalloc(sizeof(struct FieldValPair) * size);

        /* 将所有元素加入临时数组。 */
        di = dictGetIterator(ht);
        while((de = dictNext(di)) != NULL)
              pairs[idx++] = (struct FieldValPair) {dictGetKey(de), dictGetVal(de)};
        dictReleaseIterator(di);

        /* 删除随机元素以达到所需数量。 */
        while (size > count) {
            unsigned long toDiscardIdx = rand() % size;
            pairs[toDiscardIdx] = pairs[--size];
        }

        /* 回复数组中的内容 */
        for (idx = 0; idx < size; idx++) {
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            addReplyBulkCBuffer(c, pairs[idx].field, hfieldlen(pairs[idx].field));
            if (withvalues)
                addReplyBulkCBuffer(c, pairs[idx].value, sdslen(pairs[idx].value));
        }

        zfree(pairs);
    }

    /* CASE 4: We have a big hash compared to the requested number of elements.
     * In this case we can simply get random elements from the hash and add
     * to the temporary hash, trying to eventually get enough unique elements
     * to reach the specified count. */
    else {
        /* 分配临时 dictUnique 以查找唯一元素。
         * 仅保留对原哈希中 key-value 的引用。
         * 此 dict 放宽了哈希函数，使其基于字段指针。 */
        dictType uniqueDictType = { .hashFunction =  dictPtrHash };
        dict *dictUnique = dictCreate(&uniqueDictType);
        dictExpand(dictUnique, count);

        /* 哈希表编码（通用实现） */
        unsigned long added = 0;

        while(added < count) {
            dictEntry *de = dictGetFairRandomKey(hash->ptr);
            serverAssert(de != NULL);
            hfield field = dictGetKey(de);
            sds value = dictGetVal(de);

            /* 尝试将对象加入字典。如果已存在则跳过，
            * 否则增加结果字典中的对象数量。 */
            if (dictAdd(dictUnique, field, value) != DICT_OK)
                continue;

            added++;

            /* 可以立即回复，这样就无需将值保存在 dict 中。 */
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);

            addReplyBulkCBuffer(c, field, hfieldlen(field));
            if (withvalues)
                addReplyBulkCBuffer(c, value, sdslen(value));
        }

        /* 释放内存 */
        dictRelease(dictUnique);
    }
}

/*
 * HRANDFIELD - Return a random field from the hash value stored at key.
 * CLI usage: HRANDFIELD key [<count> [WITHVALUES]]
 *
 * Considerations for the current imp of HRANDFIELD & HFE feature:
 *  HRANDFIELD might access any of the fields in the hash as some of them might
 *  be expired. And so the Implementation of HRANDFIELD along with HFEs
 *  might be one of the two options:
 *  1. Expire hash-fields before diving into handling HRANDFIELD.
 *  2. Refine HRANDFIELD cases to deal with expired fields.
 *
 *  Regarding the first option, as reference, the command RANDOMKEY also declares
 *  on O(1) complexity, yet might be stuck on a very long (but not infinite) loop
 *  trying to find non-expired keys. Furthermore RANDOMKEY also evicts expired keys
 *  along the way even though it is categorized as a read-only command. Note that
 *  the case of HRANDFIELD is more lightweight versus RANDOMKEY since HFEs have
 *  much more effective and aggressive active-expiration for fields behind.
 *
 *  The second option introduces additional implementation complexity to HRANDFIELD.
 *  We could further refine HRANDFIELD cases to differentiate between scenarios
 *  with many expired fields versus few expired fields, and adjust based on the
 *  percentage of expired fields. However, this approach could still lead to long
 *  loops or necessitate expiring fields before selecting them. For the “lightweight”
 *  cases it is also expected to have a lightweight expiration.
 *
 *  Considering the pros and cons, and the fact that HRANDFIELD is an infrequent
 *  command (particularly with HFEs) and the fact we have effective active-expiration
 *  behind for hash-fields, it is better to keep it simple and choose the option #1.
 */
/* HRANDFIELD 命令实现。
 * 不指定 count 时返回单个随机字段；指定 count 时返回多个
 * 随机字段（带 WITHVALUES 时同时返回值）。
 * 时间复杂度：O(N)，N 为返回的字段数量。 */
void hrandfieldCommand(client *c) {
    long l;
    int withvalues = 0;
    robj *hash;
    CommonEntry ele;

    if (c->argc >= 3) {
        if (getRangeLongFromObjectOrReply(c,c->argv[2],-LONG_MAX,LONG_MAX,&l,NULL) != C_OK) return;
        if (c->argc > 4 || (c->argc == 4 && strcasecmp(c->argv[3]->ptr,"withvalues"))) {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        } else if (c->argc == 4) {
            withvalues = 1;
            if (l < -LONG_MAX/2 || l > LONG_MAX/2) {
                addReplyError(c,"value is out of range");
                return;
            }
        }
        hrandfieldWithCountCommand(c, l, withvalues);
        return;
    }

    /* 处理不带 <count> 参数的变体。回复简单的 bulk string */
    if ((hash = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))== NULL ||
        checkType(c,hash,OBJ_HASH)) {
        return;
    }

    /* 删除所有已过期字段。如果整个哈希都被删除则返回 null。 */
    if (hashTypeExpireIfNeeded(c->db, hash)) {
        addReply(c,shared.null[c->resp]);
        return;
    }

    hashTypeRandomElement(hash,hashTypeLength(hash, 0),&ele,NULL);

    if (ele.sval)
        addReplyBulkCBuffer(c, ele.sval, ele.slen);
    else
        addReplyBulkLongLong(c, ele.lval);
}

/*-----------------------------------------------------------------------------
 * 带有可选过期的哈希字段（基于 mstr）
 *----------------------------------------------------------------------------*/
/* 内部实现：创建一个新的 hfield 字符串。 */
static hfield _hfieldNew(const void *field, size_t fieldlen, int withExpireMeta,
                         int trymalloc)
{
    if (!withExpireMeta)
        return mstrNew(field, fieldlen, trymalloc);

    hfield hf = mstrNewWithMeta(&mstrFieldKind, field, fieldlen,
                                (mstrFlags) 1 << HFIELD_META_EXPIRE, trymalloc);

    if (!hf) return NULL;

    ExpireMeta *expireMeta = mstrMetaRef(hf, &mstrFieldKind, HFIELD_META_EXPIRE);

    /* as long as it is not inside ebuckets, it is considered trash */
    expireMeta->trash = 1;
    return hf;
}

/* 如果 expireAt 为 0，则忽略 expireAt，且不附加元数据。 */
hfield hfieldNew(const void *field, size_t fieldlen, int withExpireMeta) {
    return _hfieldNew(field, fieldlen, withExpireMeta, 0);
}

hfield hfieldTryNew(const void *field, size_t fieldlen, int withExpireMeta) {
    return _hfieldNew(field, fieldlen, withExpireMeta, 1);
}

int hfieldIsExpireAttached(hfield field) {
    return mstrIsMetaAttached(field) && mstrGetFlag(field, (int) HFIELD_META_EXPIRE);
}

static ExpireMeta* hfieldGetExpireMeta(const eItem field) {
    /* extract the expireMeta from the field of type mstr */
    return mstrMetaRef(field, &mstrFieldKind, (int) HFIELD_META_EXPIRE);
}

/* 返回值是 Unix 时间（毫秒）。 */
uint64_t hfieldGetExpireTime(hfield field) {
    if (!hfieldIsExpireAttached(field))
        return EB_EXPIRE_TIME_INVALID;

    ExpireMeta *expireMeta = mstrMetaRef(field, &mstrFieldKind, (int) HFIELD_META_EXPIRE);
    if (expireMeta->trash)
        return EB_EXPIRE_TIME_INVALID;

    return ebGetMetaExpTime(expireMeta);
}

/* 从字段移除 TTL。假定 ExpireMeta 已挂接且具有有效值。 */
static void hfieldPersist(robj *hashObj, hfield field) {
    uint64_t fieldExpireTime = hfieldGetExpireTime(field);
    if (fieldExpireTime == EB_EXPIRE_TIME_INVALID)
        return;

    /* if field is set with expire, then dict must has HFE metadata attached */
    dict *d = hashObj->ptr;
    dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *)dictMetadata(d);

    /* 如果字段具有有效的过期时间，则 dict 必须也具有有效的元数据 */
    serverAssert(dictExpireMeta->expireMeta.trash == 0);

    /* 从私有 HFE DS 中移除该字段 */
    ebRemove(&dictExpireMeta->hfe, &hashFieldExpireBucketsType, field);

    /* 无需更新全局 HFE DS。这是不必要的。
     * 实现该功能会引入显著的复杂性和开销，而该操作并非关键。
     * 在最坏的情况下，哈希稍后会被主动过期操作高效地更新，
     * 或者由哈希的 dbGenericDelete() 函数删除。 */
}

int hfieldIsExpired(hfield field) {
    /* 即便 hfieldGetExpireTime() 返回 EB_EXPIRE_TIME_INVALID，
     * 该条件仍然有效，因为该常量等价于 (EB_EXPIRE_TIME_MAX + 1)。 */
    return ( (mstime_t)hfieldGetExpireTime(field) < commandTimeSnapshot());
}

/*-----------------------------------------------------------------------------
 * 哈希字段过期（HFE）
 *----------------------------------------------------------------------------*/
/* 既可由主动过期 cron 任务调用，也可由客户端的查询调用。 */
static void propagateHashFieldDeletion(redisDb *db, sds key, char *field, size_t fieldLen) {
    robj *argv[] = {
        shared.hdel,
        createStringObject((char*) key, sdslen(key)),
        createStringObject(field, fieldLen)
    };

    enterExecutionUnit(1, 0);
    int prev_replication_allowed = server.replication_allowed;
    server.replication_allowed = 1;
    alsoPropagate(db->id,argv, 3, PROPAGATE_AOF|PROPAGATE_REPL);
    server.replication_allowed = prev_replication_allowed;
    exitExecutionUnit();

    /* 传播 HDEL 命令 */
    postExecutionUnitOperations();

    decrRefCount(argv[1]);
    decrRefCount(argv[2]);
}

/* 在哈希字段的主动过期过程中调用。向从节点传播删除事件并执行删除。 */
static ExpireAction onFieldExpire(eItem item, void *ctx) {
    OnFieldExpireCtx *expCtx = ctx;
    hfield hf = item;
    dict *d = expCtx->hashObj->ptr;
    dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
    propagateHashFieldDeletion(expCtx->db, dictExpireMeta->key, hf, hfieldlen(hf));
    serverAssert(hashTypeDelete(expCtx->hashObj, hf, 0) == 1);
    server.stat_expired_subkeys++;
    return ACT_REMOVE_EXP_ITEM;
}

/* 获取与哈希关联的 ExpireMeta。
 * 调用者需负责确保 ExpireMeta 确实已挂接。 */
static ExpireMeta *hashGetExpireMeta(const eItem hash) {
    robj *hashObj = (robj *)hash;
    if (hashObj->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = hashObj->ptr;
        return &lpt->meta;
    } else if (hashObj->encoding == OBJ_ENCODING_HT) {
        dict *d = hashObj->ptr;
        dictExpireMetadata *dictExpireMeta = (dictExpireMetadata *) dictMetadata(d);
        return &dictExpireMeta->expireMeta;
    } else {
        serverPanic("Unknown encoding: %d", hashObj->encoding);
    }
}

/* HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME 通用实现。
 * 命令格式：HTTL key <FIELDS count field [field ...]>
 * 返回每个指定字段的剩余 TTL。 */
static void httlGenericCommand(client *c, const char *cmd, long long basetime, int unit) {
    UNUSED(cmd);
    robj *hashObj;
    long numFields = 0, numFieldsAt = 3;

    /* 读取哈希对象 */
    hashObj = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c, hashObj, OBJ_HASH))
        return;

    if (strcasecmp(c->argv[numFieldsAt-1]->ptr, "FIELDS")) {
        addReplyError(c, "Mandatory argument FIELDS is missing or not at the right position");
        return;
    }

    /* 读取字段数量 */
    if (getRangeLongFromObjectOrReply(c, c->argv[numFieldsAt], 1, LONG_MAX,
                                      &numFields, "Number of fields must be a positive integer") != C_OK)
        return;

    /* 校验 numFields 与参数数量一致 */
    if (numFields != (c->argc - numFieldsAt - 1)) {
        addReplyError(c, "The `numfields` parameter must match the number of arguments");
        return;
    }

    /* 不存在的 key 与空哈希是等价的。这也意味着
     * 命令中的字段在哈希 key 中不存在。 */
    if (!hashObj) {
        addReplyArrayLen(c, numFields);
        for (int i = 0; i < numFields; i++) {
            addReplyLongLong(c, HFE_GET_NO_FIELD);
        }
        return;
    }

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK) {
        void *lp = hashObj->ptr;

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt+1+i]->ptr;
            void *fptr = lpFirst(lp);
            if (fptr != NULL)
                fptr = lpFind(lp, fptr, (unsigned char *) field, sdslen(field), 1);

            if (!fptr)
                addReplyLongLong(c, HFE_GET_NO_FIELD);
            else
                addReplyLongLong(c, HFE_GET_NO_TTL);
        }
        return;
    } else if (hashObj->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = hashObj->ptr;

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            long long expire;
            sds field = c->argv[numFieldsAt+1+i]->ptr;
            void *fptr = lpFirst(lpt->lp);
            if (fptr != NULL)
                fptr = lpFind(lpt->lp, fptr, (unsigned char *) field, sdslen(field), 2);

            if (!fptr) {
                addReplyLongLong(c, HFE_GET_NO_FIELD);
                continue;
            }

            fptr = lpNext(lpt->lp, fptr);
            serverAssert(fptr);
            fptr = lpNext(lpt->lp, fptr);
            serverAssert(fptr && lpGetIntegerValue(fptr, &expire));

            if (expire == HASH_LP_NO_TTL) {
                addReplyLongLong(c, HFE_GET_NO_TTL);
                continue;
            }

            if (expire <= commandTimeSnapshot()) {
                addReplyLongLong(c, HFE_GET_NO_FIELD);
                continue;
            }

            if (unit == UNIT_SECONDS)
                addReplyLongLong(c, (expire + 999 - basetime) / 1000);
            else
                addReplyLongLong(c, (expire - basetime));
        }
        return;
    } else if (hashObj->encoding == OBJ_ENCODING_HT) {
        dict *d = hashObj->ptr;

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt+1+i]->ptr;
            dictEntry *de = dictFind(d, field);
            if (de == NULL) {
                addReplyLongLong(c, HFE_GET_NO_FIELD);
                continue;
            }

            hfield hf = dictGetKey(de);
            uint64_t expire = hfieldGetExpireTime(hf);
            if (expire == EB_EXPIRE_TIME_INVALID) {
                addReplyLongLong(c, HFE_GET_NO_TTL); /* no ttl */
                continue;
            }

            if ( (long long) expire < commandTimeSnapshot()) {
                addReplyLongLong(c, HFE_GET_NO_FIELD);
                continue;
            }

            if (unit == UNIT_SECONDS)
                addReplyLongLong(c, (expire + 999 - basetime) / 1000);
            else
                addReplyLongLong(c, (expire - basetime));
        }
        return;
    } else {
        serverPanic("Unknown encoding: %d", hashObj->encoding);
    }
}

/* 这是 HEXPIRE、HPEXPIRE、HEXPIREAT 和 HPEXPIREAT 的通用命令实现。
 * 由于命令的第二个参数可能是相对时间或绝对时间，
 * 因此使用 "basetime" 参数指明基准时间（对于 *AT 变体为 0，
 * 对于相对过期则为当前时间）。
 *
 * unit 可以是 UNIT_SECONDS 或 UNIT_MILLISECONDS，仅用于 argv[2] 参数。
 * basetime 始终以毫秒为单位。
 *
 * 向从节点传播：
 *   该命令会被翻译为 HPEXPIREAT，且过期时间会被转换为绝对时间（毫秒）。
 *
 *   由于需要将 H(P)EXPIRE(AT) 命令传播给从节点，命令中提到的每个
 *   字段应归类为以下四种情况之一：
 *   1. 字段的过期时间更新成功 —— 作为 HPEXPIREAT 命令的一部分传播给从节点。
 *   2. 由于时间为过去，字段已被删除 —— 同时传播 HDEL 命令以删除该字段，
 *      并从传播的 HPEXPIREAT 命令中移除该字段。
 *   3. 字段不满足条件 —— 从传播的 HPEXPIREAT 命令中移除该字段。
 *   4. 字段不存在 —— 从传播的 HPEXPIREAT 命令中移除该字段。
 *
 *   如果提供的字段没有匹配情况 #1，即命令提供的时间已是过去时，
 *   则避免向从节点传播 HPEXPIREAT 命令。
 *
 *   此方法与现有 EXPIRE 命令一致。如果给定的 key 已过期，
 *   则将传播 DEL 而不是 EXPIRE 命令。如果条件不满足，
 *   命令将被拒绝。否则，将为给定的 key 传播 EXPIRE 命令。
 */
static void hexpireGenericCommand(client *c, const char *cmd, long long basetime, int unit) {
    long numFields = 0, numFieldsAt = 4;
    long long expire; /* unix time in msec */
    int fieldAt, fieldsNotSet = 0, expireSetCond = 0;
    robj *hashObj, *keyArg = c->argv[1], *expireArg = c->argv[2];

    /* 读取哈希对象 */
    hashObj = lookupKeyWrite(c->db, keyArg);
    if (checkType(c, hashObj, OBJ_HASH))
        return;

    /* 从命令中读取过期时间 */
    if (getLongLongFromObjectOrReply(c, expireArg, &expire, NULL) != C_OK)
        return;

    if (expire < 0) {
        addReplyError(c,"invalid expire time, must be >= 0");
        return;
    }

    if (unit == UNIT_SECONDS) {
        if (expire > (long long) HFE_MAX_ABS_TIME_MSEC / 1000) {
            addReplyErrorExpireTime(c);
            return;
        }
        expire *= 1000;
    }

    /* 确保最终的绝对 Unix 时间戳不超过 EB_EXPIRE_TIME_MAX。 */
    if (expire > (long long) HFE_MAX_ABS_TIME_MSEC - basetime) {
        addReplyErrorExpireTime(c);
        return;
    }
    expire += basetime;

    /* 读取可选的 expireSetCond [NX|XX|GT|LT] */
    char *optArg = c->argv[3]->ptr;
    if (!strcasecmp(optArg, "nx")) {
        expireSetCond = HFE_NX; ++numFieldsAt;
    } else if (!strcasecmp(optArg, "xx")) {
        expireSetCond = HFE_XX; ++numFieldsAt;
    } else if (!strcasecmp(optArg, "gt")) {
        expireSetCond = HFE_GT; ++numFieldsAt;
    } else if (!strcasecmp(optArg, "lt")) {
        expireSetCond = HFE_LT; ++numFieldsAt;
    }

    if (strcasecmp(c->argv[numFieldsAt-1]->ptr, "FIELDS")) {
        addReplyError(c, "Mandatory argument FIELDS is missing or not at the right position");
        return;
    }

    /* 读取字段数量 */
    if (getRangeLongFromObjectOrReply(c, c->argv[numFieldsAt], 1, LONG_MAX,
                                      &numFields, "Parameter `numFields` should be greater than 0") != C_OK)
        return;

    /* 校验 numFields 与参数数量一致 */
    if (numFields != (c->argc - numFieldsAt - 1)) {
        addReplyError(c, "The `numfields` parameter must match the number of arguments");
        return;
    }

    /* 不存在的 key 与空哈希是等价的。这也意味着
     * 命令中的字段在哈希 key 中不存在。 */
    if (!hashObj) {
        addReplyArrayLen(c, numFields);
        for (int i = 0; i < numFields; i++) {
            addReplyLongLong(c, HSETEX_NO_FIELD);
        }
        return;
    }

    HashTypeSetEx exCtx;
    hashTypeSetExInit(keyArg, hashObj, c, c->db, cmd, expireSetCond, &exCtx);
    addReplyArrayLen(c, numFields);

    fieldAt = numFieldsAt + 1;
    while (fieldAt < c->argc) {
        sds field = c->argv[fieldAt]->ptr;
        SetExRes res = hashTypeSetEx(hashObj, field, expire, &exCtx);

        if (unlikely(res != HSETEX_OK)) {
            /* 如果该字段未设置成功，则阻止该字段的传播 */
            rewriteClientCommandArgument(c, fieldAt, NULL);
            fieldsNotSet = 1;
        } else {
            ++fieldAt;
        }

        addReplyLongLong(c,res);
    }

    hashTypeSetExDone(&exCtx);

    /* 如果没有任何字段被更新（要么因为时间已是过去并已发送对应的 HDEL，
     * 要么因为条件不满足），则不传播该命令；
     * 此时传播一个没有字段的命令是无用且无效的。 */
    if (exCtx.fieldUpdated == 0) {
        preventCommandPropagation(c);
        return;
    }

    /* 如果部分字段被丢弃，则重写字段数量 */
    if (fieldsNotSet) {
        robj *numFieldsObj = createStringObjectFromLongLong(exCtx.fieldUpdated);
        rewriteClientCommandArgument(c, numFieldsAt, numFieldsObj);
        decrRefCount(numFieldsObj);
    }

    /* 以 HPEXPIREAT 毫秒时间戳形式传播。仅当尚未是 HPEXPIREAT 时重写。 */
    if (c->cmd->proc != hpexpireatCommand) {
        rewriteClientCommandArgument(c,0,shared.hpexpireat);
    }

    /* 将过期时间重写为 Unix 时间（毫秒） */
    if (basetime != 0 || unit == UNIT_SECONDS) {
        robj *expireObj = createStringObjectFromLongLong(expire);
        rewriteClientCommandArgument(c, 2, expireObj);
        decrRefCount(expireObj);
    }
}

/* HPEXPIRE key milliseconds [ NX | XX | GT | LT] numfields <field [field ...]>
 * 设置哈希字段的过期时间（毫秒，相对时间）。 */
void hpexpireCommand(client *c) {
    hexpireGenericCommand(c,"hpexpire", commandTimeSnapshot(),UNIT_MILLISECONDS);
}

/* HEXPIRE key seconds [NX | XX | GT | LT] numfields <field [field ...]>
 * 设置哈希字段的过期时间（秒，相对时间）。 */
void hexpireCommand(client *c) {
    hexpireGenericCommand(c,"hexpire", commandTimeSnapshot(),UNIT_SECONDS);
}

/* HEXPIREAT key unix-time-seconds [NX | XX | GT | LT] numfields <field [field ...]>
 * 设置哈希字段的过期时间（绝对时间，秒）。 */
void hexpireatCommand(client *c) {
    hexpireGenericCommand(c,"hexpireat", 0,UNIT_SECONDS);
}

/* HPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] numfields <field [field ...]>
 * 设置哈希字段的过期时间（绝对时间，毫秒）。 */
void hpexpireatCommand(client *c) {
    hexpireGenericCommand(c,"hpexpireat", 0,UNIT_MILLISECONDS);
}

/* for each specified field: get the remaining time to live in seconds*/
/* HTTL key numfields <field [field ...]>
 * 获取指定字段的剩余 TTL（秒）。 */
void httlCommand(client *c) {
    httlGenericCommand(c, "httl", commandTimeSnapshot(), UNIT_SECONDS);
}

/* HPTTL key numfields <field [field ...]>
 * 获取指定字段的剩余 TTL（毫秒）。 */
void hpttlCommand(client *c) {
    httlGenericCommand(c, "hpttl", commandTimeSnapshot(), UNIT_MILLISECONDS);
}

/* HEXPIRETIME key numFields <field [field ...]>
 * 获取指定字段的绝对过期时间（秒）。 */
void hexpiretimeCommand(client *c) {
    httlGenericCommand(c, "hexpiretime", 0, UNIT_SECONDS);
}

/* HPEXPIRETIME key numFields <field [field ...]>
 * 获取指定字段的绝对过期时间（毫秒）。 */
void hpexpiretimeCommand(client *c) {
    httlGenericCommand(c, "hexpiretime", 0, UNIT_MILLISECONDS);
}

/* HPERSIST key <FIELDS count field [field ...]>
 * 移除指定字段上的过期时间。 */
void hpersistCommand(client *c) {
    robj *hashObj;
    long numFields = 0, numFieldsAt = 3;
    int changed = 0; /* Used to determine whether to send a notification. */

    /* 读取哈希对象 */
    hashObj = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, hashObj, OBJ_HASH))
        return;

    if (strcasecmp(c->argv[numFieldsAt-1]->ptr, "FIELDS")) {
        addReplyError(c, "Mandatory argument FIELDS is missing or not at the right position");
        return;
    }

    /* 读取字段数量 */
    if (getRangeLongFromObjectOrReply(c, c->argv[numFieldsAt], 1, LONG_MAX,
                                      &numFields, "Number of fields must be a positive integer") != C_OK)
        return;

    /* 校验 numFields 与参数数量一致 */
    if (numFields != (c->argc - numFieldsAt - 1)) {
        addReplyError(c, "The `numfields` parameter must match the number of arguments");
        return;
    }

    /* 不存在的 key 与空哈希是等价的。这也意味着
     * 命令中的字段在哈希 key 中不存在。 */
    if (!hashObj) {
        addReplyArrayLen(c, numFields);
        for (int i = 0; i < numFields; i++) {
            addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
        }
        return;
    }

    if (hashObj->encoding == OBJ_ENCODING_LISTPACK) {
        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt + 1 + i]->ptr;
            unsigned char *fptr, *zl = hashObj->ptr;

            fptr = lpFirst(zl);
            if (fptr != NULL)
                fptr = lpFind(zl, fptr, (unsigned char *) field, sdslen(field), 1);

            if (!fptr)
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
            else
                addReplyLongLong(c, HFE_PERSIST_NO_TTL);
        }
        return;
    } else if (hashObj->encoding == OBJ_ENCODING_LISTPACK_EX) {
        long long prevExpire;
        unsigned char *fptr, *vptr, *tptr;
        listpackEx *lpt = hashObj->ptr;

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt + 1 + i]->ptr;

            fptr = lpFirst(lpt->lp);
            if (fptr != NULL)
                fptr = lpFind(lpt->lp, fptr, (unsigned char*)field, sdslen(field), 2);

            if (!fptr) {
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
                continue;
            }

            vptr = lpNext(lpt->lp, fptr);
            serverAssert(vptr);
            tptr = lpNext(lpt->lp, vptr);
            serverAssert(tptr && lpGetIntegerValue(tptr, &prevExpire));

            if (prevExpire == HASH_LP_NO_TTL) {
                addReplyLongLong(c, HFE_PERSIST_NO_TTL);
                continue;
            }

            if (prevExpire < commandTimeSnapshot()) {
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
                continue;
            }

            listpackExUpdateExpiry(hashObj, field, fptr, vptr, HASH_LP_NO_TTL);
            addReplyLongLong(c, HFE_PERSIST_OK);
            changed = 1;
        }
    } else if (hashObj->encoding == OBJ_ENCODING_HT) {
        dict *d = hashObj->ptr;

        addReplyArrayLen(c, numFields);
        for (int i = 0 ; i < numFields ; i++) {
            sds field = c->argv[numFieldsAt + 1 + i]->ptr;
            dictEntry *de = dictFind(d, field);
            if (de == NULL) {
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
                continue;
            }

            hfield hf = dictGetKey(de);
            uint64_t expire = hfieldGetExpireTime(hf);
            if (expire == EB_EXPIRE_TIME_INVALID) {
                addReplyLongLong(c, HFE_PERSIST_NO_TTL);
                continue;
            }

            /* 已过期。假装不存在该字段 */
            if ( (long long) expire < commandTimeSnapshot()) {
                addReplyLongLong(c, HFE_PERSIST_NO_FIELD);
                continue;
            }

            hfieldPersist(hashObj, hf);
            addReplyLongLong(c, HFE_PERSIST_OK);
            changed = 1;
        }
    } else {
        serverPanic("Unknown encoding: %d", hashObj->encoding);
    }

    /* 当任何字段上的过期时间被成功删除时，发出 hpersist 事件。 */
    if (changed) {
        notifyKeyspaceEvent(NOTIFY_HASH, "hpersist", c->argv[1], c->db->id);
        signalModifiedKey(c, c->db, c->argv[1]);
        server.dirty++;
    }
}
