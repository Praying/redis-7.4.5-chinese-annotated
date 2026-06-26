/* ==========================================================================
 * unix.c - unix socket connection implementation
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

static ConnectionType CT_Unix;

/* 返回 unix socket 连接的类型标识 */
static const char *connUnixGetType(connection *conn) {
    UNUSED(conn);

    return CONN_TYPE_UNIX;
}

/* unix socket 连接的事件循环处理函数
 * 直接复用 TCP 连接类型的 ae_handler 实现 */
static void connUnixEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask) {
    connectionTypeTcp()->ae_handler(el, fd, clientData, mask);
}

/* 获取 unix socket 连接的地址信息 */
static int connUnixAddr(connection *conn, char *ip, size_t ip_len, int *port, int remote) {
    return connectionTypeTcp()->addr(conn, ip, ip_len, port, remote);
}

/* 判断 unix socket 连接是否为本地连接 */
static int connUnixIsLocal(connection *conn) {
    UNUSED(conn);

    return 1; /* unix socket 始终是本地连接 */
}

/* 启动 unix socket 监听 */
static int connUnixListen(connListener *listener) {
    int fd;
    mode_t *perm = (mode_t *)listener->priv;

    if (listener->bindaddr_count == 0)
        return C_OK;

    /* 当前 listener->bindaddr_count 始终为 1，
     * 这里仍然使用循环以防将来 Redis 支持多个 unix socket */
    for (int j = 0; j < listener->bindaddr_count; j++) {
        char *addr = listener->bindaddr[j];

        unlink(addr); /* 删除旧的 socket 文件，无论是否成功都继续 */
        fd = anetUnixServer(server.neterr, addr, *perm, server.tcp_backlog);
        if (fd == ANET_ERR) {
            serverLog(LL_WARNING, "Failed opening Unix socket: %s", server.neterr);
            exit(1);
        }
        // 设置为非阻塞模式
        anetNonBlock(NULL, fd);
        // 设置 close-on-exec 标志
        anetCloexec(fd);
        listener->fd[listener->count++] = fd;
    }

    return C_OK;
}

/* 创建一个新的 unix socket 连接对象 */
static connection *connCreateUnix(void) {
    connection *conn = zcalloc(sizeof(connection));
    conn->type = &CT_Unix;
    conn->fd = -1;
    conn->iovcnt = IOV_MAX;

    return conn;
}

/* 创建一个已与接受到的 unix socket 关联的连接对象 */
static connection *connCreateAcceptedUnix(int fd, void *priv) {
    UNUSED(priv);
    connection *conn = connCreateUnix();
    conn->fd = fd;
    conn->state = CONN_STATE_ACCEPTING;
    return conn;
}

/* 监听 unix socket 的 accept 事件处理函数 */
static void connUnixAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cfd;
    int max = server.max_new_conns_per_cycle;
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        cfd = anetUnixAccept(server.neterr, fd);
        if (cfd == ANET_ERR) {
            if (anetAcceptFailureNeedsRetry(errno))
                continue;
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        serverLog(LL_VERBOSE,"Accepted connection to %s", server.unixsocket);
        acceptCommonHandler(connCreateAcceptedUnix(cfd, NULL),CLIENT_UNIX_SOCKET,NULL);
    }
}

/* 关闭 unix socket 的读写通道 */
static void connUnixShutdown(connection *conn) {
    connectionTypeTcp()->shutdown(conn);
}

/* 关闭 unix socket 连接并释放资源 */
static void connUnixClose(connection *conn) {
    connectionTypeTcp()->close(conn);
}

/* 完成 unix socket 连接的 accept 阶段 */
static int connUnixAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    return connectionTypeTcp()->accept(conn, accept_handler);
}

/* 通过 unix socket 写入数据 */
static int connUnixWrite(connection *conn, const void *data, size_t data_len) {
    return connectionTypeTcp()->write(conn, data, data_len);
}

/* 通过 unix socket 的 writev 接口（向量写） */
static int connUnixWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    return connectionTypeTcp()->writev(conn, iov, iovcnt);
}

/* 通过 unix socket 读取数据 */
static int connUnixRead(connection *conn, void *buf, size_t buf_len) {
    return connectionTypeTcp()->read(conn, buf, buf_len);
}

/* 注册 unix socket 连接的写处理函数 */
static int connUnixSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
    return connectionTypeTcp()->set_write_handler(conn, func, barrier);
}

/* 注册 unix socket 连接的读处理函数 */
static int connUnixSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    return connectionTypeTcp()->set_read_handler(conn, func);
}

/* 获取 unix socket 连接最近一次的错误描述 */
static const char *connUnixGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

/* unix socket 上的同步写 */
static ssize_t connUnixSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncWrite(conn->fd, ptr, size, timeout);
}

/* unix socket 上的同步读 */
static ssize_t connUnixSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncRead(conn->fd, ptr, size, timeout);
}

/* unix socket 上的同步按行读 */
static ssize_t connUnixSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncReadLine(conn->fd, ptr, size, timeout);
}

/* unix domain socket 连接类型实现 */
static ConnectionType CT_Unix = {
    /* 连接类型 */
    .get_type = connUnixGetType,

    /* 连接类型的初始化 & 清理 & 配置 */
    .init = NULL,
    .cleanup = NULL,
    .configure = NULL,

    /* ae & accept & listen & error & 地址处理 */
    .ae_handler = connUnixEventHandler,
    .accept_handler = connUnixAcceptHandler,
    .addr = connUnixAddr,
    .is_local = connUnixIsLocal,
    .listen = connUnixListen,

    /* 创建/关闭/释放连接 */
    .conn_create = connCreateUnix,
    .conn_create_accepted = connCreateAcceptedUnix,
    .shutdown = connUnixShutdown,
    .close = connUnixClose,

    /* connect 与 accept（unix socket 不支持主动连接） */
    .connect = NULL,
    .blocking_connect = NULL,
    .accept = connUnixAccept,

    /* I/O */
    .write = connUnixWrite,
    .writev = connUnixWritev,
    .read = connUnixRead,
    .set_write_handler = connUnixSetWriteHandler,
    .set_read_handler = connUnixSetReadHandler,
    .get_last_error = connUnixGetLastError,
    .sync_write = connUnixSyncWrite,
    .sync_read = connUnixSyncRead,
    .sync_readline = connUnixSyncReadLine,

    /* 待处理数据 */
    .has_pending_data = NULL,
    .process_pending_data = NULL,
};

/* 向连接层注册 unix socket 连接类型 */
int RedisRegisterConnectionTypeUnix(void)
{
    return connTypeRegister(&CT_Unix);
}
