/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "call_reply.h"

/* 标志位：标识该 reply 是根节点（即由 callReplyCreate 创建的顶层对象） */
#define REPLY_FLAG_ROOT (1<<0)
/* 标志位：标识该 reply 已经被解析过 */
#define REPLY_FLAG_PARSED (1<<1)
/* 标志位：标识该 reply 使用 RESP3 协议格式 */
#define REPLY_FLAG_RESP3 (1<<2)

/* --------------------------------------------------------
 * 一个不透明的结构体，用于解析 RESP 协议回复并将其表示为
 * 内部对象。用于解析 RM_Call 或 Lua 脚本中的回复。
 * -------------------------------------------------------- */
struct CallReply {
    void *private_data;        /* 调用者传入的私有数据指针 */
    sds original_proto;        /* 原始协议缓冲区，仅根 reply 可用 */
    const char *proto;         /* 当前 reply 对应的协议数据指针 */
    size_t proto_len;          /* 协议数据长度 */
    int type;                  /* 回复类型：REPLY_... */
    int flags;                 /* 标志位：REPLY_FLAG... */
    size_t len;                /* 字符串长度，或数组中元素的数量 */
    union {
        const char *str;       /* 字符串和错误回复的指针。无需释放，
                                * 始终指向 reply 对象自身的 proto
                                * 缓冲区内部，或（对于数组元素）
                                * 指向父 reply 对象的 proto 缓冲区 */
        struct {
            const char *str;
            const char *format;
        } verbatim_str;        /* verbatim string 回复的值 */
        long long ll;          /* 整数回复的值 */
        double d;              /* 浮点数回复的值 */
        struct CallReply *array; /* 子回复元素数组，用于 set、array、
                                   map 和 attribute 类型 */
    } val;
    list *deferred_error_list;   /* sds 形式的错误列表，或 NULL */
    struct CallReply *attribute; /* attribute 回复，不存在时为 NULL */
};

/* 设置 CallReply 的公共字段：类型、协议指针、协议长度和额外标志 */
static void callReplySetSharedData(CallReply *rep, int type, const char *proto, size_t proto_len, int extra_flags) {
    rep->type = type;
    rep->proto = proto;
    rep->proto_len = proto_len;
    rep->flags |= extra_flags;
}

/* 回调：解析 RESP3 null 回复 */
static void callReplyNull(void *ctx, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_NULL, proto, proto_len, REPLY_FLAG_RESP3);
}

/* 回调：解析 RESP2 null bulk string 回复 */
static void callReplyNullBulkString(void *ctx, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_NULL, proto, proto_len, 0);
}

/* 回调：解析 RESP2 null array 回复 */
static void callReplyNullArray(void *ctx, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_NULL, proto, proto_len, 0);
}

/* 回调：解析 bulk string 回复 */
static void callReplyBulkString(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_STRING, proto, proto_len, 0);
    rep->len = len;
    rep->val.str = str;
}

/* 回调：解析 error 回复 */
static void callReplyError(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_ERROR, proto, proto_len, 0);
    rep->len = len;
    rep->val.str = str;
}

/* 回调：解析 simple string 回复 */
static void callReplySimpleStr(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_STRING, proto, proto_len, 0);
    rep->len = len;
    rep->val.str = str;
}

/* 回调：解析整数回复 */
static void callReplyLong(void *ctx, long long val, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_INTEGER, proto, proto_len, 0);
    rep->val.ll = val;
}

/* 回调：解析 RESP3 浮点数回复 */
static void callReplyDouble(void *ctx, double val, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_DOUBLE, proto, proto_len, REPLY_FLAG_RESP3);
    rep->val.d = val;
}

/* 回调：解析 RESP3 verbatim string 回复 */
static void callReplyVerbatimString(void *ctx, const char *format, const char *str, size_t len, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_VERBATIM_STRING, proto, proto_len, REPLY_FLAG_RESP3);
    rep->len = len;
    rep->val.verbatim_str.str = str;
    rep->val.verbatim_str.format = format;
}

/* 回调：解析 RESP3 大数回复 */
static void callReplyBigNumber(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_BIG_NUMBER, proto, proto_len, REPLY_FLAG_RESP3);
    rep->len = len;
    rep->val.str = str;
}

/* 回调：解析 RESP3 布尔值回复 */
static void callReplyBool(void *ctx, int val, const char *proto, size_t proto_len) {
    CallReply *rep = ctx;
    callReplySetSharedData(rep, REDISMODULE_REPLY_BOOL, proto, proto_len, REPLY_FLAG_RESP3);
    rep->val.ll = val;
}

/* 解析集合类型回复（数组、集合、映射等），为每个元素
 * 递归调用 parseReply。elements_per_entry 指定每个条目
 * 包含的元素数（如 map 为 2，其他为 1）。 */
static void callReplyParseCollection(ReplyParser *parser, CallReply *rep, size_t len, const char *proto, size_t elements_per_entry) {
    rep->len = len;
    rep->val.array = zcalloc(elements_per_entry * len * sizeof(CallReply));
    for (size_t i = 0; i < len * elements_per_entry; i += elements_per_entry) {
        for (size_t j = 0 ; j < elements_per_entry ; ++j) {
            rep->val.array[i + j].private_data = rep->private_data;
            parseReply(parser, rep->val.array + i + j);
            rep->val.array[i + j].flags |= REPLY_FLAG_PARSED;
            if (rep->val.array[i + j].flags & REPLY_FLAG_RESP3) {
                /* 如果某个子回复是 RESP3，则当前回复也标记为 RESP3 */
                rep->flags |= REPLY_FLAG_RESP3;
            }
        }
    }
    rep->proto = proto;
    rep->proto_len = parser->curr_location - proto;
}

/* 回调：解析 RESP3 attribute 回复。attribute 附加在
 * 主回复之上，需要先解析 attribute，再继续解析主回复。 */
static void callReplyAttribute(ReplyParser *parser, void *ctx, size_t len, const char *proto) {
    CallReply *rep = ctx;
    rep->attribute = zcalloc(sizeof(CallReply));

    /* 继续解析 attribute 回复 */
    rep->attribute->len = len;
    rep->attribute->type = REDISMODULE_REPLY_ATTRIBUTE;
    callReplyParseCollection(parser, rep->attribute, len, proto, 2);
    rep->attribute->flags |= REPLY_FLAG_PARSED | REPLY_FLAG_RESP3;
    rep->attribute->private_data = rep->private_data;

    /* Continue parsing the reply */
    parseReply(parser, rep);

    /* In this case we need to fix the proto address and len, it should start from the attribute */
    rep->proto = proto;
    rep->proto_len = parser->curr_location - proto;
    rep->flags |= REPLY_FLAG_RESP3;
}

static void callReplyArray(ReplyParser *parser, void *ctx, size_t len, const char *proto) {
    CallReply *rep = ctx;
    rep->type = REDISMODULE_REPLY_ARRAY;
    callReplyParseCollection(parser, rep, len, proto, 1);
}

static void callReplySet(ReplyParser *parser, void *ctx, size_t len, const char *proto) {
    CallReply *rep = ctx;
    rep->type = REDISMODULE_REPLY_SET;
    callReplyParseCollection(parser, rep, len, proto, 1);
    rep->flags |= REPLY_FLAG_RESP3;
}

static void callReplyMap(ReplyParser *parser, void *ctx, size_t len, const char *proto) {
    CallReply *rep = ctx;
    rep->type = REDISMODULE_REPLY_MAP;
    callReplyParseCollection(parser, rep, len, proto, 2);
    rep->flags |= REPLY_FLAG_RESP3;
}

static void callReplyParseError(void *ctx) {
    CallReply *rep = ctx;
    rep->type = REDISMODULE_REPLY_UNKNOWN;
}

/* Recursively free the current call reply and its sub-replies. */
static void freeCallReplyInternal(CallReply *rep) {
    if (rep->type == REDISMODULE_REPLY_ARRAY || rep->type == REDISMODULE_REPLY_SET) {
        for (size_t i = 0 ; i < rep->len ; ++i) {
            freeCallReplyInternal(rep->val.array + i);
        }
        zfree(rep->val.array);
    }

    if (rep->type == REDISMODULE_REPLY_MAP || rep->type == REDISMODULE_REPLY_ATTRIBUTE) {
        for (size_t i = 0 ; i < rep->len ; ++i) {
            freeCallReplyInternal(rep->val.array + i * 2);
            freeCallReplyInternal(rep->val.array + i * 2 + 1);
        }
        zfree(rep->val.array);
    }

    if (rep->attribute) {
        freeCallReplyInternal(rep->attribute);
        zfree(rep->attribute);
    }
}

/* Free the given call reply and its children (in case of nested reply) recursively.
 * If private data was set when the CallReply was created it will not be freed, as it's
 * the caller's responsibility to free it before calling freeCallReply(). */
void freeCallReply(CallReply *rep) {
    if (!(rep->flags & REPLY_FLAG_ROOT)) {
        return;
    }
    if (rep->flags & REPLY_FLAG_PARSED) {
        if (rep->type == REDISMODULE_REPLY_PROMISE) {
            zfree(rep);
            return;
        }
        freeCallReplyInternal(rep);
    }
    sdsfree(rep->original_proto);
    if (rep->deferred_error_list)
        listRelease(rep->deferred_error_list);
    zfree(rep);
}

CallReply *callReplyCreatePromise(void *private_data) {
    CallReply *res = zmalloc(sizeof(*res));
    res->type = REDISMODULE_REPLY_PROMISE;
    /* Mark the reply as parsed so there will be not attempt to parse
     * it when calling reply API such as freeCallReply.
     * Also mark the reply as root so freeCallReply will not ignore it. */
    res->flags |= REPLY_FLAG_PARSED | REPLY_FLAG_ROOT;
    res->private_data = private_data;
    return res;
}

static const ReplyParserCallbacks DefaultParserCallbacks = {
    .null_callback = callReplyNull,
    .bulk_string_callback = callReplyBulkString,
    .null_bulk_string_callback = callReplyNullBulkString,
    .null_array_callback = callReplyNullArray,
    .error_callback = callReplyError,
    .simple_str_callback = callReplySimpleStr,
    .long_callback = callReplyLong,
    .array_callback = callReplyArray,
    .set_callback = callReplySet,
    .map_callback = callReplyMap,
    .double_callback = callReplyDouble,
    .bool_callback = callReplyBool,
    .big_number_callback = callReplyBigNumber,
    .verbatim_string_callback = callReplyVerbatimString,
    .attribute_callback = callReplyAttribute,
    .error = callReplyParseError,
};

/* Parse the buffer located in rep->original_proto and update the CallReply
 * structure to represent its contents. */
static void callReplyParse(CallReply *rep) {
    if (rep->flags & REPLY_FLAG_PARSED) {
        return;
    }

    ReplyParser parser = {.curr_location = rep->proto, .callbacks = DefaultParserCallbacks};

    parseReply(&parser, rep);
    rep->flags |= REPLY_FLAG_PARSED;
}

/* Return the call reply type (REDISMODULE_REPLY_...). */
int callReplyType(CallReply *rep) {
    if (!rep) return REDISMODULE_REPLY_UNKNOWN;
    callReplyParse(rep);
    return rep->type;
}

/* Return reply string as buffer and len. Applicable to:
 * - REDISMODULE_REPLY_STRING
 * - REDISMODULE_REPLY_ERROR
 *
 * The return value is borrowed from CallReply, so it must not be freed
 * explicitly or used after CallReply itself is freed.
 *
 * The returned value is not NULL terminated and its length is returned by
 * reference through len, which must not be NULL.
 */
const char *callReplyGetString(CallReply *rep, size_t *len) {
    callReplyParse(rep);
    if (rep->type != REDISMODULE_REPLY_STRING &&
        rep->type != REDISMODULE_REPLY_ERROR) return NULL;
    if (len) *len = rep->len;
    return rep->val.str;
}

/* Return a long long reply value. Applicable to:
 * - REDISMODULE_REPLY_INTEGER
 */
long long callReplyGetLongLong(CallReply *rep) {
    callReplyParse(rep);
    if (rep->type != REDISMODULE_REPLY_INTEGER) return LLONG_MIN;
    return rep->val.ll;
}

/* Return a double reply value. Applicable to:
 * - REDISMODULE_REPLY_DOUBLE
 */
double callReplyGetDouble(CallReply *rep) {
    callReplyParse(rep);
    if (rep->type != REDISMODULE_REPLY_DOUBLE) return LLONG_MIN;
    return rep->val.d;
}

/* Return a reply Boolean value. Applicable to:
 * - REDISMODULE_REPLY_BOOL
 */
int callReplyGetBool(CallReply *rep) {
    callReplyParse(rep);
    if (rep->type != REDISMODULE_REPLY_BOOL) return INT_MIN;
    return rep->val.ll;
}

/* Return reply length. Applicable to:
 * - REDISMODULE_REPLY_STRING
 * - REDISMODULE_REPLY_ERROR
 * - REDISMODULE_REPLY_ARRAY
 * - REDISMODULE_REPLY_SET
 * - REDISMODULE_REPLY_MAP
 * - REDISMODULE_REPLY_ATTRIBUTE
 */
size_t callReplyGetLen(CallReply *rep) {
    callReplyParse(rep);
    switch(rep->type) {
        case REDISMODULE_REPLY_STRING:
        case REDISMODULE_REPLY_ERROR:
        case REDISMODULE_REPLY_ARRAY:
        case REDISMODULE_REPLY_SET:
        case REDISMODULE_REPLY_MAP:
        case REDISMODULE_REPLY_ATTRIBUTE:
            return rep->len;
        default:
            return 0;
    }
}

static CallReply *callReplyGetCollectionElement(CallReply *rep, size_t idx, int elements_per_entry) {
    if (idx >= rep->len * elements_per_entry) return NULL; // real len is rep->len * elements_per_entry
    return rep->val.array+idx;
}

/* Return a reply array element at a given index. Applicable to:
 * - REDISMODULE_REPLY_ARRAY
 *
 * The return value is borrowed from CallReply, so it must not be freed
 * explicitly or used after CallReply itself is freed.
 */
CallReply *callReplyGetArrayElement(CallReply *rep, size_t idx) {
    callReplyParse(rep);
    if (rep->type != REDISMODULE_REPLY_ARRAY) return NULL;
    return callReplyGetCollectionElement(rep, idx, 1);
}

/* Return a reply set element at a given index. Applicable to:
 * - REDISMODULE_REPLY_SET
 *
 * The return value is borrowed from CallReply, so it must not be freed
 * explicitly or used after CallReply itself is freed.
 */
CallReply *callReplyGetSetElement(CallReply *rep, size_t idx) {
    callReplyParse(rep);
    if (rep->type != REDISMODULE_REPLY_SET) return NULL;
    return callReplyGetCollectionElement(rep, idx, 1);
}

static int callReplyGetMapElementInternal(CallReply *rep, size_t idx, CallReply **key, CallReply **val, int type) {
    callReplyParse(rep);
    if (rep->type != type) return C_ERR;
    if (idx >= rep->len) return C_ERR;
    if (key) *key = callReplyGetCollectionElement(rep, idx * 2, 2);
    if (val) *val = callReplyGetCollectionElement(rep, idx * 2 + 1, 2);
    return C_OK;
}

/* Retrieve a map reply key and value at a given index. Applicable to:
 * - REDISMODULE_REPLY_MAP
 *
 * The key and value are returned by reference through key and val,
 * which may also be NULL if not needed.
 *
 * Returns C_OK on success or C_ERR if reply type mismatches, or if idx is out
 * of range.
 *
 * The returned values are borrowed from CallReply, so they must not be freed
 * explicitly or used after CallReply itself is freed.
 */
int callReplyGetMapElement(CallReply *rep, size_t idx, CallReply **key, CallReply **val) {
    return callReplyGetMapElementInternal(rep, idx, key, val, REDISMODULE_REPLY_MAP);
}

/* Return reply attribute, or NULL if it does not exist. Applicable to all replies.
 *
 * The returned values are borrowed from CallReply, so they must not be freed
 * explicitly or used after CallReply itself is freed.
 */
CallReply *callReplyGetAttribute(CallReply *rep) {
    return rep->attribute;
}

/* Retrieve attribute reply key and value at a given index. Applicable to:
 * - REDISMODULE_REPLY_ATTRIBUTE
 *
 * The key and value are returned by reference through key and val,
 * which may also be NULL if not needed.
 *
 * Returns C_OK on success or C_ERR if reply type mismatches, or if idx is out
 * of range.
 *
 * The returned values are borrowed from CallReply, so they must not be freed
 * explicitly or used after CallReply itself is freed.
 */
int callReplyGetAttributeElement(CallReply *rep, size_t idx, CallReply **key, CallReply **val) {
    return callReplyGetMapElementInternal(rep, idx, key, val, REDISMODULE_REPLY_MAP);
}

/* Return a big number reply value. Applicable to:
 * - REDISMODULE_REPLY_BIG_NUMBER
 *
 * The returned values are borrowed from CallReply, so they must not be freed
 * explicitly or used after CallReply itself is freed.
 *
 * The return value is guaranteed to be a big number, as described in the RESP3
 * protocol specifications.
 *
 * The returned value is not NULL terminated and its length is returned by
 * reference through len, which must not be NULL.
 */
const char *callReplyGetBigNumber(CallReply *rep, size_t *len) {
    callReplyParse(rep);
    if (rep->type != REDISMODULE_REPLY_BIG_NUMBER) return NULL;
    *len = rep->len;
    return rep->val.str;
}

/* Return a verbatim string reply value. Applicable to:
 * - REDISMODULE_REPLY_VERBATIM_STRING
 *
 * If format is non-NULL, the verbatim reply format is also returned by value.
 *
 * The optional output argument can be given to get a verbatim reply
 * format, or can be set NULL if not needed.
 *
 * The return value is borrowed from CallReply, so it must not be freed
 * explicitly or used after CallReply itself is freed.
 *
 * The returned value is not NULL terminated and its length is returned by
 * reference through len, which must not be NULL.
 */
const char *callReplyGetVerbatim(CallReply *rep, size_t *len, const char **format){
    callReplyParse(rep);
    if (rep->type != REDISMODULE_REPLY_VERBATIM_STRING) return NULL;
    *len = rep->len;
    if (format) *format = rep->val.verbatim_str.format;
    return rep->val.verbatim_str.str;
}

/* Return the current reply blob.
 *
 * The return value is borrowed from CallReply, so it must not be freed
 * explicitly or used after CallReply itself is freed.
 */
const char *callReplyGetProto(CallReply *rep, size_t *proto_len) {
    *proto_len = rep->proto_len;
    return rep->proto;
}

/* Return CallReply private data, as set by the caller on callReplyCreate().
 */
void *callReplyGetPrivateData(CallReply *rep) {
    return rep->private_data;
}

/* Return true if the reply or one of it sub-replies is RESP3 formatted. */
int callReplyIsResp3(CallReply *rep) {
    return rep->flags & REPLY_FLAG_RESP3;
}

/* Returns a list of errors in sds form, or NULL. */
list *callReplyDeferredErrorList(CallReply *rep) {
    return rep->deferred_error_list;
}

/* Create a new CallReply struct from the reply blob.
 *
 * The function will own the reply blob, so it must not be used or freed by
 * the caller after passing it to this function.
 *
 * The reply blob will be freed when the returned CallReply struct is later
 * freed using freeCallReply().
 *
 * The deferred_error_list is an optional list of errors that are present
 * in the reply blob, if given, this function will take ownership on it.
 *
 * The private_data is optional and can later be accessed using
 * callReplyGetPrivateData().
 *
 * NOTE: The parser used for parsing the reply and producing CallReply is
 * designed to handle valid replies created by Redis itself. IT IS NOT
 * DESIGNED TO HANDLE USER INPUT and using it to parse invalid replies is
 * unsafe.
 */
CallReply *callReplyCreate(sds reply, list *deferred_error_list, void *private_data) {
    CallReply *res = zmalloc(sizeof(*res));
    res->flags = REPLY_FLAG_ROOT;
    res->original_proto = reply;
    res->proto = reply;
    res->proto_len = sdslen(reply);
    res->private_data = private_data;
    res->attribute = NULL;
    res->deferred_error_list = deferred_error_list;
    return res;
}

/* Create a new CallReply struct from the reply blob representing an error message.
 * Automatically creating deferred_error_list and set a copy of the reply in it.
 * Refer to callReplyCreate for detailed explanation.
 * Reply string can come in one of two forms:
 * 1. A protocol reply starting with "-CODE" and ending with "\r\n"
 * 2. A plain string, in which case this function adds the protocol header and footer. */
CallReply *callReplyCreateError(sds reply, void *private_data) {
    sds err_buff = reply;
    if (err_buff[0] != '-') {
        err_buff = sdscatfmt(sdsempty(), "-ERR %S\r\n", reply);
        sdsfree(reply);
    }
    list *deferred_error_list = listCreate();
    listSetFreeMethod(deferred_error_list, (void (*)(void*))sdsfree);
    listAddNodeTail(deferred_error_list, sdsnew(err_buff));
    return callReplyCreate(err_buff, deferred_error_list, private_data);
}
