/*
 * Copyright (c) 2019-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "connhelpers.h"

/* connections 模块为网络连接提供一个轻量级的抽象，
 * 以避免在 Redis 代码库中直接管理 socket 和异步事件。
 *
 * 它并不提供类似库中常见的高级连接特性，
 * 例如完整的输入/输出缓冲区管理、限流等。
 * 这些功能仍保留在 networking.c 中。
 *
 * 主要目标是允许以透明的方式处理基于 TCP 和 TLS 的连接。
 * 为此，连接具有以下特性：
 *
 * 1. 一个连接可以在其对应的 socket 存在之前就存在。
 *    这允许在建立实际连接之前处理各种上下文和配置设置。
 * 2. 调用者可以注册/注销逻辑读/写处理函数，
 *    这些函数将在连接有数据可读/可以接受写入时被调用。
 *    这些逻辑处理函数可能对应实际的 AE 事件，也可能不对应，
 *    具体取决于实现（对于 TCP 它们是对应的；对于 TLS 则不是）。
 */

static ConnectionType CT_Socket;

/* 当创建一个连接时，我们必须已经知道其类型，但底层的 socket 可能存在也可能不存在：
 *
 * - 对于已接受的连接，socket 已经存在，因为我们不对 listen/accept 部分建模；
 *   所以调用者调用 connCreateSocket() 后再调用 connAccept()。
 * - 对于发起的连接，socket 由连接模块自身创建；
 *   所以调用者调用 connCreateSocket() 后再调用 connConnect()，
 *   后者会注册一个 connect 回调，在已连接/错误状态（或任何传输层握手完成后）触发。
 *
 * 注意：早期版本曾将连接作为其他结构体的一部分嵌入，而不是独立分配。
 * 这本可以带来进一步的优化，例如使用 container_of() 等。
 * 但出于以下原因，最终放弃了该方案：
 *
 * 1. 在某些情况下，连接的创建/处理发生在包含结构体的上下文之外，
 *    此时复制它们会显得有些笨拙。
 * 2. 未来的实现可能希望为连接分配任意数据。
 * 3. container_of() 方法无论如何都有风险，因为连接可能嵌入到不同的结构体中，
 *    而不仅仅是 client。
 */

static connection *connCreateSocket(void) {
    connection *conn = zcalloc(sizeof(connection));
    conn->type = &CT_Socket;
    conn->fd = -1;
    conn->iovcnt = IOV_MAX;

    return conn;
}

/* 创建一个已经关联到已接受连接的 socket 类型连接。
 *
 * 在调用 connAccept() 并触发连接级 accept 处理函数之前，
 * socket 还不能用于 I/O。
 *
 * 调用者应使用 connGetState() 检查并确认所创建的连接未处于错误状态
 * （对于 socket 连接这不可能发生，但其他协议可能会出现错误状态）。
 */
static connection *connCreateAcceptedSocket(int fd, void *priv) {
    UNUSED(priv);
    connection *conn = connCreateSocket();
    conn->fd = fd;
    conn->state = CONN_STATE_ACCEPTING;
    return conn;
}

/* 以 socket 方式发起连接（TCP） */
static int connSocketConnect(connection *conn, const char *addr, int port, const char *src_addr,
        ConnectionCallbackFunc connect_handler) {
    int fd = anetTcpNonBlockBestEffortBindConnect(NULL,addr,port,src_addr);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    conn->fd = fd;
    conn->state = CONN_STATE_CONNECTING;

    conn->conn_handler = connect_handler;
    aeCreateFileEvent(server.el, conn->fd, AE_WRITABLE,
            conn->type->ae_handler, conn);

    return C_OK;
}

/* ------ 纯 socket 连接 ------- */

/* 下面是一个非常不完整的、与实现相关的调用列表。
 * 随着我们实现更多的连接类型，上面很多代码都将迁移到这里。
 */

static void connSocketShutdown(connection *conn) {
    if (conn->fd == -1) return;

    // 关闭 socket 的读写通道
    shutdown(conn->fd, SHUT_RDWR);
}

/* 关闭连接并释放资源。 */
static void connSocketClose(connection *conn) {
    if (conn->fd != -1) {
        aeDeleteFileEvent(server.el,conn->fd, AE_READABLE | AE_WRITABLE);
        close(conn->fd);
        conn->fd = -1;
    }

    /* 如果是在某个处理函数内部被调用，则调度关闭动作，
     * 但要保持连接存在，直到该处理函数返回。
     */
    if (connHasRefs(conn)) {
        conn->flags |= CONN_FLAG_CLOSE_SCHEDULED;
        return;
    }

    zfree(conn);
}

/* 通过 socket 写入数据 */
static int connSocketWrite(connection *conn, const void *data, size_t data_len) {
    int ret = write(conn->fd, data, data_len);
    if (ret < 0 && errno != EAGAIN) {
        conn->last_errno = errno;

        /* 不要覆盖尚未处于 connected 状态的连接状态，
         * 以避免干扰处理函数回调。
         */
        if (errno != EINTR && conn->state == CONN_STATE_CONNECTED)
            conn->state = CONN_STATE_ERROR;
    }

    return ret;
}

/* 通过 socket 的 writev 接口（向量写） */
static int connSocketWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    int ret = writev(conn->fd, iov, iovcnt);
    if (ret < 0 && errno != EAGAIN) {
        conn->last_errno = errno;

        /* 不要覆盖尚未处于 connected 状态的连接状态，
         * 以避免干扰处理函数回调。
         */
        if (errno != EINTR && conn->state == CONN_STATE_CONNECTED)
            conn->state = CONN_STATE_ERROR;
    }

    return ret;
}

/* 通过 socket 读取数据 */
static int connSocketRead(connection *conn, void *buf, size_t buf_len) {
    int ret = read(conn->fd, buf, buf_len);
    if (!ret) {
        conn->state = CONN_STATE_CLOSED;
    } else if (ret < 0 && errno != EAGAIN) {
        conn->last_errno = errno;

        /* 不要覆盖尚未处于 connected 状态的连接状态，
         * 以避免干扰处理函数回调。
         */
        if (errno != EINTR && conn->state == CONN_STATE_CONNECTED)
            conn->state = CONN_STATE_ERROR;
    }

    return ret;
}

/* 完成 socket 连接的 accept 阶段 */
static int connSocketAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    int ret = C_OK;

    if (conn->state != CONN_STATE_ACCEPTING) return C_ERR;
    conn->state = CONN_STATE_CONNECTED;

    connIncrRefs(conn);
    if (!callHandler(conn, accept_handler)) ret = C_ERR;
    connDecrRefs(conn);

    return ret;
}

/* 注册一个写处理函数，将在连接可写时被调用。
 * 若为 NULL，则移除已有的处理函数。
 *
 * barrier 标志表示请求设置写屏障（write barrier），
 * 效果是设置 CONN_FLAG_WRITE_BARRIER。
 * 这将确保在同一个事件循环中，写处理函数总是在读处理函数之前被调用，
 * 而不是在之后调用。
 */
static int connSocketSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
    if (func == conn->write_handler) return C_OK;

    conn->write_handler = func;
    if (barrier)
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    else
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    if (!conn->write_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,AE_WRITABLE,
                    conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

/* 注册一个读处理函数，将在连接可读时被调用。
 * 若为 NULL，则移除已有的处理函数。
 */
static int connSocketSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    if (func == conn->read_handler) return C_OK;

    conn->read_handler = func;
    if (!conn->read_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_READABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,
                    AE_READABLE,conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

/* 获取连接上最近一次的错误描述 */
static const char *connSocketGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

/* socket 连接的事件循环处理函数 */
static void connSocketEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask)
{
    UNUSED(el);
    UNUSED(fd);
    connection *conn = clientData;

    if (conn->state == CONN_STATE_CONNECTING &&
            (mask & AE_WRITABLE) && conn->conn_handler) {

        int conn_error = anetGetError(conn->fd);
        if (conn_error) {
            conn->last_errno = conn_error;
            conn->state = CONN_STATE_ERROR;
        } else {
            conn->state = CONN_STATE_CONNECTED;
        }

        if (!conn->write_handler) aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);

        if (!callHandler(conn, conn->conn_handler)) return;
        conn->conn_handler = NULL;
    }

    /* 通常我们先执行可读事件，然后再执行可写事件。
     * 这很有用，因为我们有时可以在处理完查询后，
     * 立即向客户端返回该查询的回复。
     *
     * 但如果在 mask 中设置了 WRITE_BARRIER，则应用要求我们反向操作：
     * 永远不要在可读事件之后再触发可写事件。
     * 在这种情况下，我们反转两者的调用顺序。
     * 这在我们想要在 beforeSleep() 钩子中做一些事情时很有用，
     * 例如在向客户端应答之前先将文件 fsync 到磁盘。 */
    int invert = conn->flags & CONN_FLAG_WRITE_BARRIER;

    int call_write = (mask & AE_WRITABLE) && conn->write_handler;
    int call_read = (mask & AE_READABLE) && conn->read_handler;

    /* 处理常规 I/O 流程 */
    if (!invert && call_read) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
    /* 触发可写事件 */
    if (call_write) {
        if (!callHandler(conn, conn->write_handler)) return;
    }
    /* 如果需要反转调用顺序，则在可写事件之后
     * 再触发可读事件 */
    if (invert && call_read) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
}

/* 监听 socket 上的 accept 事件处理函数 */
static void connSocketAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    int max = server.max_new_conns_per_cycle;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (anetAcceptFailureNeedsRetry(errno))
                continue;
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        serverLog(LL_VERBOSE,"Accepted %s:%d", cip, cport);
        acceptCommonHandler(connCreateAcceptedSocket(cfd, NULL),0,cip);
    }
}

/* 获取连接的地址信息（IP/端口） */
static int connSocketAddr(connection *conn, char *ip, size_t ip_len, int *port, int remote) {
    if (anetFdToString(conn->fd, ip, ip_len, port, remote) == 0)
        return C_OK;

    conn->last_errno = errno;
    return C_ERR;
}

/* 判断 socket 连接是否为本地连接（127.0.0.1 或 ::1） */
static int connSocketIsLocal(connection *conn) {
    char cip[NET_IP_STR_LEN + 1] = { 0 };

    if (connSocketAddr(conn, cip, sizeof(cip) - 1, NULL, 1) == C_ERR)
        return -1;

    return !strncmp(cip, "127.", 4) || !strcmp(cip, "::1");
}

/* 启动 socket 监听 */
static int connSocketListen(connListener *listener) {
    return listenToPort(listener);
}

/* 以阻塞方式发起 TCP 连接，可设置超时 */
static int connSocketBlockingConnect(connection *conn, const char *addr, int port, long long timeout) {
    int fd = anetTcpNonBlockConnect(NULL,addr,port);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    if ((aeWait(fd, AE_WRITABLE, timeout) & AE_WRITABLE) == 0) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = ETIMEDOUT;
        return C_ERR;
    }

    conn->fd = fd;
    conn->state = CONN_STATE_CONNECTED;
    return C_OK;
}

/* 基于连接的 syncio.c 函数版本。
 * 注意：理想情况下应当重构为纯异步工作方式。
 */

static ssize_t connSocketSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncWrite(conn->fd, ptr, size, timeout);
}

static ssize_t connSocketSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncRead(conn->fd, ptr, size, timeout);
}

static ssize_t connSocketSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncReadLine(conn->fd, ptr, size, timeout);
}

/* 返回 socket 连接的类型标识 */
static const char *connSocketGetType(connection *conn) {
    (void) conn;

    return CONN_TYPE_SOCKET;
}

/* TCP socket 连接类型实现 */
static ConnectionType CT_Socket = {
    /* 连接类型 */
    .get_type = connSocketGetType,

    /* 连接类型的初始化 & 清理 & 配置 */
    .init = NULL,
    .cleanup = NULL,
    .configure = NULL,

    /* ae & accept & listen & error & 地址处理 */
    .ae_handler = connSocketEventHandler,
    .accept_handler = connSocketAcceptHandler,
    .addr = connSocketAddr,
    .is_local = connSocketIsLocal,
    .listen = connSocketListen,

    /* 创建/关闭/释放连接 */
    .conn_create = connCreateSocket,
    .conn_create_accepted = connCreateAcceptedSocket,
    .shutdown = connSocketShutdown,
    .close = connSocketClose,

    /* connect 与 accept */
    .connect = connSocketConnect,
    .blocking_connect = connSocketBlockingConnect,
    .accept = connSocketAccept,

    /* I/O */
    .write = connSocketWrite,
    .writev = connSocketWritev,
    .read = connSocketRead,
    .set_write_handler = connSocketSetWriteHandler,
    .set_read_handler = connSocketSetReadHandler,
    .get_last_error = connSocketGetLastError,
    .sync_write = connSocketSyncWrite,
    .sync_read = connSocketSyncRead,
    .sync_readline = connSocketSyncReadLine,

    /* 待处理数据 */
    .has_pending_data = NULL,
    .process_pending_data = NULL,
};

/* 将连接设置为阻塞模式 */
int connBlock(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetBlock(NULL, conn->fd);
}

/* 将连接设置为非阻塞模式 */
int connNonBlock(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetNonBlock(NULL, conn->fd);
}

/* 启用 TCP_NODELAY（禁用 Nagle 算法） */
int connEnableTcpNoDelay(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetEnableTcpNoDelay(NULL, conn->fd);
}

/* 关闭 TCP_NODELAY（启用 Nagle 算法） */
int connDisableTcpNoDelay(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetDisableTcpNoDelay(NULL, conn->fd);
}

/* 启用 TCP keep-alive，interval 为探测间隔 */
int connKeepAlive(connection *conn, int interval) {
    if (conn->fd == -1) return C_ERR;
    return anetKeepAlive(NULL, conn->fd, interval);
}

/* 设置发送超时（毫秒） */
int connSendTimeout(connection *conn, long long ms) {
    return anetSendTimeout(NULL, conn->fd, ms);
}

/* 设置接收超时（毫秒） */
int connRecvTimeout(connection *conn, long long ms) {
    return anetRecvTimeout(NULL, conn->fd, ms);
}

/* 向连接层注册 socket 连接类型 */
int RedisRegisterConnectionTypeSocket(void)
{
    return connTypeRegister(&CT_Socket);
}
