/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

/* ----------------------------------------------------------------------------------------
 * 用于解析 RM_Call 或 Lua 中 redis.call() 所返回回复的 RESP 解析器。
 *
 * 该解析器引入了若干由用户设置的回调函数。每个回调对应一种不同的回复类型。
 * 每个回调都会收到调用 parseReply 时传入的 p_ctx 参数。回调还会向调用方提供
 * 当前回复的协议内容（底层字节流）及其大小。
 *
 * 部分回调还会接收解析器对象本身，包括：
 * - array_callback
 * - set_callback
 * - map_callback
 *
 * 这些回调需要按照给定的元素个数继续调用 parseReply 进行解析。后续的
 * parseReply 调用可以使用不同的 p_ctx，该 p_ctx 将被用于嵌套的 CallReply 对象。
 *
 * 这些回调不会收到 proto_len，因为在解析时该值是未知的。调用方可以在解析完
 * 整个集合之后自行计算该值。
 *
 * 注意：该解析器仅用于处理 Redis 自身生成的回复，未执行很多必要的校验，
 * 因此不适合用于解析用户输入。
 * ----------------------------------------------------------------------------------------
 */

#include "resp_parser.h"
#include "server.h"

/* 解析 RESP 批量字符串（bulk string），类型前缀为 '$'。
 * 如果长度为 -1，则表示空批量字符串（nil），否则根据长度读取实际的字符串内容。
 * 解析完成后会调用对应的回调（null_bulk_string_callback 或 bulk_string_callback）。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseBulk(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    long long bulklen;
    parser->curr_location = p + 2; /* 跳过 \r\n 终止符 */

    string2ll(proto+1,p-proto-1,&bulklen);
    if (bulklen == -1) {
        // 批量字符串长度为 -1，表示空值（nil）
        parser->callbacks.null_bulk_string_callback(p_ctx, proto, parser->curr_location - proto);
    } else {
        const char *str = parser->curr_location;
        parser->curr_location += bulklen;
        parser->curr_location += 2; /* 跳过字符串末尾的 \r\n */
        parser->callbacks.bulk_string_callback(p_ctx, str, bulklen, proto, parser->curr_location - proto);
    }

    return C_OK;
}

/* 解析 RESP 简单字符串（simple string），类型前缀为 '+'。
 * 调用 simple_str_callback 回调将字符串内容和长度传出。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseSimpleString(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    parser->curr_location = p + 2; /* 跳过 \r\n 终止符 */
    parser->callbacks.simple_str_callback(p_ctx, proto+1, p-proto-1, proto, parser->curr_location - proto);
    return C_OK;
}

/* 解析 RESP 错误回复（error），类型前缀为 '-'。
 * 调用 error_callback 回调将错误信息传出。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseError(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    parser->curr_location = p + 2; // 跳过 \r\n 终止符
    parser->callbacks.error_callback(p_ctx, proto+1, p-proto-1, proto, parser->curr_location - proto);
    return C_OK;
}

/* 解析 RESP 整数（integer），类型前缀为 ':'。
 * 将字符串形式的整数解析为 long long，并调用 long_callback 回调传出。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseLong(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    parser->curr_location = p + 2; /* 跳过 \r\n 终止符 */
    long long val;
    string2ll(proto+1,p-proto-1,&val);
    parser->callbacks.long_callback(p_ctx, val, proto, parser->curr_location - proto);
    return C_OK;
}

/* 解析 RESP3 属性块（attributes），类型前缀为 '|'。
 * 属性是键值对形式，其内容需要继续通过 parseReply 递归解析。
 * 调用 attribute_callback 回调，由回调负责按 len 解析后续内容。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseAttributes(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    long long len;
    string2ll(proto+1,p-proto-1,&len);
    p += 2;
    parser->curr_location = p;
    parser->callbacks.attribute_callback(parser, p_ctx, len, proto);
    return C_OK;
}

/* 解析 RESP3 verbatim string，类型前缀为 '='。
 * 格式为 "格式:内容"（如 "txt:Some string"），前 4 字节为格式。
 * 调用 verbatim_string_callback 回调将格式、内容、长度传出。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseVerbatimString(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    long long bulklen;
    parser->curr_location = p + 2; /* 跳过 \r\n 终止符 */
    string2ll(proto+1,p-proto-1,&bulklen);
    const char *format = parser->curr_location;
    parser->curr_location += bulklen;
    parser->curr_location += 2; /* 跳过末尾的 \r\n */
    parser->callbacks.verbatim_string_callback(p_ctx, format, format + 4, bulklen - 4, proto, parser->curr_location - proto);
    return C_OK;
}

/* 解析 RESP3 大数（big number），类型前缀为 '('。
 * 大数无法用 long long 表示，此处按字符串形式原样传给回调。
 * 调用 big_number_callback 回调。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseBigNumber(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    parser->curr_location = p + 2; /* 跳过 \r\n 终止符 */
    parser->callbacks.big_number_callback(p_ctx, proto+1, p-proto-1, proto, parser->curr_location - proto);
    return C_OK;
}

/* 解析 RESP3 空值（null），类型前缀为 '_'。
 * 调用 null_callback 回调，协议内容（长度为 3：_\r\n）一并传出。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseNull(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    parser->curr_location = p + 2; /* 跳过 \r\n 终止符 */
    parser->callbacks.null_callback(p_ctx, proto, parser->curr_location - proto);
    return C_OK;
}

/* 解析 RESP3 双精度浮点数（double），类型前缀为 ','。
 * 使用 strtod 将字符串转换为 double；若长度超过 MAX_LONG_DOUBLE_CHARS 则
 * 将值置为 0。调用 double_callback 回调传出。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseDouble(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    parser->curr_location = p + 2; /* 跳过 \r\n 终止符 */
    char buf[MAX_LONG_DOUBLE_CHARS+1];
    size_t len = p-proto-1;
    double d;
    if (len <= MAX_LONG_DOUBLE_CHARS) {
        memcpy(buf,proto+1,len);
        buf[len] = '\0';
        d = strtod(buf,NULL); /* 期望是合法的浮点表示。 */
    } else {
        d = 0;
    }
    parser->callbacks.double_callback(p_ctx, d, proto, parser->curr_location - proto);
    return C_OK;
}

/* 解析 RESP3 布尔值（boolean），类型前缀为 '#'。
 * 若首字符为 't' 则为真，否则为假。调用 bool_callback 回调传出。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseBool(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    parser->curr_location = p + 2; /* 跳过 \r\n 终止符 */
    parser->callbacks.bool_callback(p_ctx, proto[1] == 't', proto, parser->curr_location - proto);
    return C_OK;
}

/* 解析 RESP 数组（array），类型前缀为 '*'。
 * 如果长度为 -1 则表示空数组（nil）；否则调用 array_callback 回调，
 * 回调内部需要按 len 递归调用 parseReply 解析后续元素。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseArray(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    long long len;
    string2ll(proto+1,p-proto-1,&len);
    p += 2;
    parser->curr_location = p;
    if (len == -1) {
        // 数组长度为 -1，表示空数组（nil）
        parser->callbacks.null_array_callback(p_ctx, proto, parser->curr_location - proto);
    } else {
        parser->callbacks.array_callback(parser, p_ctx, len, proto);
    }
    return C_OK;
}

/* 解析 RESP3 集合（set），类型前缀为 '~'。
 * 调用 set_callback 回调，回调内部需要按 len 递归调用 parseReply 解析元素。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseSet(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    long long len;
    string2ll(proto+1,p-proto-1,&len);
    p += 2;
    parser->curr_location = p;
    parser->callbacks.set_callback(parser, p_ctx, len, proto);
    return C_OK;
}

/* 解析 RESP3 字典（map），类型前缀为 '%'。
 * map 中的元素按 key/value 交替排列，总数为 len。
 * 调用 map_callback 回调，回调内部需要按 len*2 递归调用 parseReply 解析。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK。 */
static int parseMap(ReplyParser *parser, void *p_ctx) {
    const char *proto = parser->curr_location;
    char *p = strchr(proto+1,'\r');
    long long len;
    string2ll(proto+1,p-proto-1,&len);
    p += 2;
    parser->curr_location = p;
    parser->callbacks.map_callback(parser, p_ctx, len, proto);
    return C_OK;
}

/* 解析由 parser->curr_location 指向的回复。
 * 根据首个字节判断 RESP 类型（见 switch 分支），并分派给对应的解析函数。
 * 若首字节不匹配任何已知类型且设置了错误回调，则调用 error 回调。
 * 参数：
 *   parser - 解析器对象
 *   p_ctx  - 透传给回调的上下文
 * 返回值：成功返回 C_OK，失败或类型未知返回 C_ERR。 */
int parseReply(ReplyParser *parser, void *p_ctx) {
    switch (parser->curr_location[0]) {
        case '$': return parseBulk(parser, p_ctx);              /* 批量字符串 */
        case '+': return parseSimpleString(parser, p_ctx);      /* 简单字符串 */
        case '-': return parseError(parser, p_ctx);             /* 错误 */
        case ':': return parseLong(parser, p_ctx);              /* 整数 */
        case '*': return parseArray(parser, p_ctx);             /* 数组 */
        case '~': return parseSet(parser, p_ctx);               /* 集合 */
        case '%': return parseMap(parser, p_ctx);               /* 字典 */
        case '#': return parseBool(parser, p_ctx);              /* 布尔 */
        case ',': return parseDouble(parser, p_ctx);            /* 双精度浮点 */
        case '_': return parseNull(parser, p_ctx);              /* 空值 */
        case '(': return parseBigNumber(parser, p_ctx);         /* 大数 */
        case '=': return parseVerbatimString(parser, p_ctx);    /* verbatim 字符串 */
        case '|': return parseAttributes(parser, p_ctx);        /* 属性 */
        default: if (parser->callbacks.error) parser->callbacks.error(p_ctx);
    }
    return C_ERR;
}
