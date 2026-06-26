/* ==========================================================================
 * connection.c - connection layer framework
 * --------------------------------------------------------------------------
 * Copyright (C) 2022  zhenwei pi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the
 * following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 * USE OR OTHER DEALINGS IN THE SOFTWARE.
 * ==========================================================================
 */

#include "server.h"
#include "connection.h"

/* ==========================================================================
 * 本文件实现了 Redis 的连接抽象层（connection layer framework）。
 *
 * Redis 7.0 起引入 connection 抽象层，将底层传输（TCP、Unix 域套接字、
 * TLS 等）封装为统一的 ConnectionType 接口，从而让上层代码以一致的方式
 * 处理不同的连接实现。
 *
 * 本文件负责：
 *   1) ConnectionType 的注册与查询；
 *   2) 内置连接类型（Socket/Unix/TLS）的初始化；
 *   3) 跨所有连接类型的通用操作（清理、待处理数据遍历等）；
 *   4) 生成监听端口的 INFO 信息字符串。
 * ========================================================================== */

/* 已注册的 ConnectionType 全局表。
 * connection 抽象层（Redis 7.0+ 引入）通过该数组管理所有具体的
 * 连接实现（如 CT_Socket、CT_Unix、CT_TLS）。 */
static ConnectionType *connTypes[CONN_TYPE_MAX];

/* 注册一个新的 ConnectionType 实现。
 * 将具体的连接类型注册到 connTypes 数组中，并可选地调用其 init 钩子。
 *
 * 参数：
 *   ct - 指向 ConnectionType 结构体的指针，描述待注册的实现。
 *
 * 返回值：
 *   C_OK  - 注册成功；
 *   C_ERR - 注册失败（通常是同名类型已存在）。 */
int connTypeRegister(ConnectionType *ct) {
    const char *typename = ct->get_type(NULL);
    ConnectionType *tmpct;
    int type;

    /* 查找一个空槽位用于存放新的 connection type */
    for (type = 0; type < CONN_TYPE_MAX; type++) {
        tmpct = connTypes[type];
        if (!tmpct)
            break;

        /* 大小写无关比较，我们并不在意 "tls"/"TLS" 的差异 */
        if (!strcasecmp(typename, tmpct->get_type(NULL))) {
            serverLog(LL_WARNING, "Connection types %s already registered", typename);
            return C_ERR;
        }
    }

    serverLog(LL_VERBOSE, "Connection type %s registered", typename);
    connTypes[type] = ct;

    // 调用具体实现的初始化钩子（若存在）
    if (ct->init) {
        ct->init();
    }

    return C_OK;
}

/* 初始化所有内置的 ConnectionType。
 * 依次注册 Socket、Unix 两种必备类型，并尝试注册 TLS（可能因
 * 未开启 BUILD_TLS 而失败，但不视为致命错误）。
 *
 * 返回值：始终返回 C_OK。 */
int connTypeInitialize(void) {
    /* 当前 socket 连接类型是必需的 */
    serverAssert(RedisRegisterConnectionTypeSocket() == C_OK);

    /* 当前 unix socket 连接类型是必需的 */
    serverAssert(RedisRegisterConnectionTypeUnix() == C_OK);

    /* 若未启用 BUILD_TLS 编译选项，注册可能失败，但不视为致命错误 */
    RedisRegisterConnectionTypeTLS();

    return C_OK;
}

/* 根据类型名称查找已注册的 ConnectionType。
 * 在 connTypes 数组中按大小写无关的方式查找匹配 typename 的实现。
 *
 * 参数：
 *   typename - 类型名字符串（如 CONN_TYPE_SOCKET、CONN_TYPE_TLS）。
 *
 * 返回值：找到时返回对应 ConnectionType 指针；否则返回 NULL。 */
ConnectionType *connectionByType(const char *typename) {
    ConnectionType *ct;

    for (int type = 0; type < CONN_TYPE_MAX; type++) {
        ct = connTypes[type];
        if (!ct)
            break;

        if (!strcasecmp(typename, ct->get_type(NULL)))
            return ct;
    }

    serverLog(LL_WARNING, "Missing implement of connection type %s", typename);

    return NULL;
}

/* 获取 TCP (Socket) ConnectionType，结果会被静态缓存，避免重复按字符串查找。
 * 缓存查询结果（按字符串仅查一次）。 */
ConnectionType *connectionTypeTcp(void) {
    static ConnectionType *ct_tcp = NULL;

    if (ct_tcp != NULL)
        return ct_tcp;

    ct_tcp = connectionByType(CONN_TYPE_SOCKET);
    serverAssert(ct_tcp != NULL);

    return ct_tcp;
}

/* 获取 TLS ConnectionType，结果会被静态缓存。
 * 与 TCP/Unix 不同，TLS 连接可能未被注册，因此需要额外缓存标志位
 * 来正确处理 NULL 返回值。缓存查询结果（按字符串仅查一次）。 */
ConnectionType *connectionTypeTls(void) {
    static ConnectionType *ct_tls = NULL;
    static int cached = 0;

    /* 与 TCP 和 Unix 连接不同，TLS 可能不存在，
     * 因此需要缓存指针以正确处理 NULL 情况。 */
    if (!cached) {
        cached = 1;
        ct_tls = connectionByType(CONN_TYPE_TLS);
    }

    return ct_tls;
}

/* 获取 Unix ConnectionType，结果会被静态缓存，避免重复按字符串查找。
 * 缓存查询结果（按字符串仅查一次）。 */
ConnectionType *connectionTypeUnix(void) {
    static ConnectionType *ct_unix = NULL;

    if (ct_unix != NULL)
        return ct_unix;

    ct_unix = connectionByType(CONN_TYPE_UNIX);
    return ct_unix;
}

/* 根据类型名称查找其在 connTypes 数组中的索引。
 *
 * 参数：
 *   typename - 类型名字符串。
 *
 * 返回值：找到时返回索引；未找到时返回 -1。 */
int connectionIndexByType(const char *typename) {
    ConnectionType *ct;

    for (int type = 0; type < CONN_TYPE_MAX; type++) {
        ct = connTypes[type];
        if (!ct)
            break;

        if (!strcasecmp(typename, ct->get_type(NULL)))
            return type;
    }

    return -1;
}

/* 清理所有已注册的 ConnectionType。
 * 依次调用每个 connection type 的 cleanup 钩子（若存在）。 */
void connTypeCleanupAll(void) {
    ConnectionType *ct;
    int type;

    for (type = 0; type < CONN_TYPE_MAX; type++) {
        ct = connTypes[type];
        if (!ct)
            break;

        if (ct->cleanup)
            ct->cleanup();
    }
}

/* 遍历所有 connection type，直到找到存在待处理数据的类型为止。
 * 一旦发现任一类型有挂起数据，便立刻返回。 */
int connTypeHasPendingData(void) {
    ConnectionType *ct;
    int type;
    int ret = 0;

    for (type = 0; type < CONN_TYPE_MAX; type++) {
        ct = connTypes[type];
        if (ct && ct->has_pending_data && (ret = ct->has_pending_data())) {
            return ret;
        }
    }

    return ret;
}

/* 遍历所有 connection type，并对每个类型处理其待处理数据。
 *
 * 返回值：所有类型累计处理的连接数。 */
int connTypeProcessPendingData(void) {
    ConnectionType *ct;
    int type;
    int ret = 0;

    for (type = 0; type < CONN_TYPE_MAX; type++) {
        ct = connTypes[type];
        if (ct && ct->process_pending_data) {
            ret += ct->process_pending_data();
        }
    }

    return ret;
}

/* 生成服务器所有监听端口的 INFO 信息字符串。
 * 遍历 server.listeners 数组，将每个监听器的类型、绑定地址、端口等
 * 信息追加到 info 字符串末尾。
 *
 * 参数：
 *   info - 已有的 sds 字符串，用于追加监听信息。
 *
 * 返回值：追加后的 sds 字符串。 */
sds getListensInfoString(sds info) {
    for (int j = 0; j < CONN_TYPE_MAX; j++) {
        connListener *listener = &server.listeners[j];
        if (listener->ct == NULL)
            continue;

        // 输出监听器类型名
        info = sdscatfmt(info, "listener%i:name=%s", j, listener->ct->get_type(NULL));
        // 输出每个绑定地址
        for (int i = 0; i < listener->count; i++) {
            info = sdscatfmt(info, ",bind=%s", listener->bindaddr[i]);
        }

        // 若端口有效，则输出端口号
        if (listener->port)
            info = sdscatfmt(info, ",port=%i", listener->port);

        info = sdscatfmt(info, "\r\n");
    }

    return info;
}
