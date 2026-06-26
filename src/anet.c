/* anet.c -- 基础 TCP socket 工具，让事情变得不那么乏味
 *
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * 本文件实现了 Redis 的异步网络工具库 (anet)，提供了对 TCP、Unix 域
 * socket 以及管道等常见网络操作的封装，使 socket 编程更加简单易用。
 */

#include "fmacros.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <netdb.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>

#include "anet.h"
#include "config.h"
#include "util.h"

/* 抑制未使用参数警告的辅助宏。 */
#define UNUSED(x) (void)(x)

/* 将格式化后的错误信息写入 err 缓冲区。
 * 如果 err 为 NULL，则直接返回，不写入任何内容。
 * fmt 是 printf 风格的格式字符串。 */
static void anetSetError(char *err, const char *fmt, ...)
{
    va_list ap;

    if (!err) return;
    va_start(ap, fmt);
    vsnprintf(err, ANET_ERR_LEN, fmt, ap);
    va_end(ap);
}

/* 通过 SO_ERROR socket 选项获取 socket 上的错误码。
 * 返回 socket 上的错误码，若 getsockopt 调用失败则返回 errno。 */
int anetGetError(int fd) {
    int sockerr = 0;
    socklen_t errlen = sizeof(sockerr);

    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    return sockerr;
}

/* 设置 socket 的阻塞模式。
 * 当 non_block 非零时设置为非阻塞模式，为 0 则设置为阻塞模式。
 * 注意 fcntl(2) 的 F_GETFL 和 F_SETFL 不会被信号中断。 */
int anetSetBlock(char *err, int fd, int non_block) {
    int flags;

    /* 设置 socket 为阻塞（non_block 为 0 时）或非阻塞。
     * 注意 fcntl(2) 的 F_GETFL 和 F_SETFL 不会被信号中断。 */
    if ((flags = fcntl(fd, F_GETFL)) == -1) {
        anetSetError(err, "fcntl(F_GETFL): %s", strerror(errno));
        return ANET_ERR;
    }

    /* 检查该标志是否已经处于目标状态，如果是，
     * 则无需再调用 fcntl 来设置/清除该标志。 */
    if (!!(flags & O_NONBLOCK) == !!non_block)
        return ANET_OK;

    if (non_block)
        flags |= O_NONBLOCK;
    else
        flags &= ~O_NONBLOCK;

    if (fcntl(fd, F_SETFL, flags) == -1) {
        anetSetError(err, "fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

/* 将 socket 设置为非阻塞模式的便捷封装。 */
int anetNonBlock(char *err, int fd) {
    return anetSetBlock(err,fd,1);
}

/* 将 socket 设置为阻塞模式的便捷封装。 */
int anetBlock(char *err, int fd) {
    return anetSetBlock(err,fd,0);
}

/* 在给定的 fd 上启用 FD_CLOEXEC 标志，以避免 fd 泄漏。
 * 该函数应当在调用 fork + execve 系统调用的特定场景下被调用。 */
int anetCloexec(int fd) {
    int r;
    int flags;

    do {
        r = fcntl(fd, F_GETFD);
    } while (r == -1 && errno == EINTR);

    if (r == -1 || (r & FD_CLOEXEC))
        return r;

    flags = r | FD_CLOEXEC;

    do {
        r = fcntl(fd, F_SETFD, flags);
    } while (r == -1 && errno == EINTR);

    return r;
}

/* 启用 TCP keep-alive 机制以检测已经断开的对端，
 * 会相应设置 TCP_KEEPIDLE、TCP_KEEPINTVL 和 TCP_KEEPCNT。 */
int anetKeepAlive(char *err, int fd, int interval)
{
    int enabled = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enabled, sizeof(enabled)))
    {
        anetSetError(err, "setsockopt SO_KEEPALIVE: %s", strerror(errno));
        return ANET_ERR;
    }

    int idle;
    int intvl;
    int cnt;

    /* 部分平台预期支持完整的 TCP keep-alive 机制，
     * 我们希望编译器在预处理指令意外失效时输出未使用变量的警告，
     * 除这些平台外，如果发生此类警告则直接忽略。 */
#if !(defined(_AIX) || defined(__APPLE__) || defined(__DragonFly__) || \
    defined(__FreeBSD__) || defined(__illumos__) || defined(__linux__) || \
    defined(__NetBSD__) || defined(__sun))
    UNUSED(interval);
    UNUSED(idle);
    UNUSED(intvl);
    UNUSED(cnt);
#endif

#ifdef __sun
    /* 与其他类 Unix 系统相比，Solaris/SmartOS 上 TCP keep-alive 的实现有所不同。
     * 因此需要在 Solaris 上做专门处理。
     *
     * Solaris 上有两种 keep-alive 机制：
     * - 默认情况下，第一个 keep-alive 探测包会在 TCP 连接空闲两小时后发送。
     * 如果对端在八分钟内没有响应探测包，TCP 连接将被中止。
     * 你可以使用 TCP_KEEPALIVE_THRESHOLD（毫秒）或 TCP_KEEPIDLE（秒）
     * 这两个 socket 选项来修改发送首个探测包的间隔。
     * 系统默认值由 TCP ndd 参数 tcp_keepalive_interval 控制。最小值为 10 秒。
     * 最大值为 10 天，默认值为 2 小时。如果探测包没有收到响应，
     * 可以使用 TCP_KEEPALIVE_ABORT_THRESHOLD socket 选项来修改中止 TCP 连接的时间阈值。
     * 该选项值是无符号整数，单位是毫秒。值为零表示 TCP 在探测时永远不会超时
     * 并中止连接。系统默认值由 TCP ndd 参数 tcp_keepalive_abort_interval 控制。
     * 默认值为 8 分钟。
     *
     * - 如果设置了 socket 选项 TCP_KEEPINTVL 和/或 TCP_KEEPCNT，则会激活第二种实现。
     * 后续每个探测包之间的时间间隔由 TCP_KEEPINTVL 设置，单位是秒。
     * 最小值为 10 秒，最大值为 10 天，默认值为 2 小时。
     * 在未收到响应的情况下，TCP 连接将在由 TCP_KEEPCNT 设置的一定数量的探测之后中止。 */

    idle = interval;
    if (idle < 10) idle = 10; // 内核要求至少 10 秒
    if (idle > 10*24*60*60) idle = 10*24*60*60; // 内核要求最多 10 天

    /* `TCP_KEEPIDLE`、`TCP_KEEPINTVL` 和 `TCP_KEEPCNT` 在 Solaris 11.4 之前
     * 不可用，不过这里我们仍然尝试使用它们。 */
#if defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL) && defined(TCP_KEEPCNT)
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle))) {
        anetSetError(err, "setsockopt TCP_KEEPIDLE: %s\n", strerror(errno));
        return ANET_ERR;
    }

    intvl = idle/3;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl))) {
        anetSetError(err, "setsockopt TCP_KEEPINTVL: %s\n", strerror(errno));
        return ANET_ERR;
    }

    cnt = 3;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt))) {
        anetSetError(err, "setsockopt TCP_KEEPCNT: %s\n", strerror(errno));
        return ANET_ERR;
    }
#else
    /* 在较老的 Solaris 上回退到第一种 keep-alive 实现，
     * 通过 `TCP_KEEPALIVE_THRESHOLD` + `TCP_KEEPALIVE_ABORT_THRESHOLD`
     * 在其他平台上模拟 keep-alive 机制。 */
    idle *= 1000; // 内核要求单位为毫秒
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE_THRESHOLD, &idle, sizeof(idle))) {
        anetSetError(err, "setsockopt TCP_KEEPINTVL: %s\n", strerror(errno));
        return ANET_ERR;
    }

    /* 注意在 Solaris 上后续探测包的发送间隔并不相等，
     * 而是按照指数退避算法发送。 */
    intvl = idle/3;
    cnt = 3;
    int time_to_abort = intvl * cnt;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE_ABORT_THRESHOLD, &time_to_abort, sizeof(time_to_abort))) {
        anetSetError(err, "setsockopt TCP_KEEPCNT: %s\n", strerror(errno));
        return ANET_ERR;
    }
#endif

    return ANET_OK;

#endif

#ifdef TCP_KEEPIDLE
    /* 在 Linux 及其他类 Unix 系统上，默认的 keepalive 时间
     * 是 7200 秒，几乎没有任何用处。
     * 因此我们修改这些设置使该特性真正可用。 */

    /* 在 idle 秒后发送第一个探测包。 */
    idle = interval;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle))) {
        anetSetError(err, "setsockopt TCP_KEEPIDLE: %s\n", strerror(errno));
        return ANET_ERR;
    }
#elif defined(TCP_KEEPALIVE)
    /* Darwin/macOS 使用 TCP_KEEPALIVE 来代替 TCP_KEEPIDLE。 */
    idle = interval;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &idle, sizeof(idle))) {
        anetSetError(err, "setsockopt TCP_KEEPALIVE: %s\n", strerror(errno));
        return ANET_ERR;
    }
#endif

#ifdef TCP_KEEPINTVL
    /* 按指定间隔发送后续探测包。注意我们将延时设置为 interval / 3，
     * 因为我们在检测错误之前会发送三次探测包（参见下一个 setsockopt 调用）。 */
    intvl = interval/3;
    if (intvl == 0) intvl = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl))) {
        anetSetError(err, "setsockopt TCP_KEEPINTVL: %s\n", strerror(errno));
        return ANET_ERR;
    }
#endif

#ifdef TCP_KEEPCNT
    /* 在发送三个 ACK 探测包且没有收到响应之后，认为该 socket 出错。 */
    cnt = 3;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt))) {
        anetSetError(err, "setsockopt TCP_KEEPCNT: %s\n", strerror(errno));
        return ANET_ERR;
    }
#endif

    return ANET_OK;
}

/* 设置或清除 TCP_NODELAY socket 选项。
 * val 非零启用 Nagle 算法关闭，0 则恢复 Nagle 算法。 */
static int anetSetTcpNoDelay(char *err, int fd, int val)
{
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == -1)
    {
        anetSetError(err, "setsockopt TCP_NODELAY: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

/* 启用 TCP_NODELAY（即关闭 Nagle 算法）的便捷封装。 */
int anetEnableTcpNoDelay(char *err, int fd)
{
    return anetSetTcpNoDelay(err, fd, 1);
}

/* 禁用 TCP_NODELAY（即开启 Nagle 算法）的便捷封装。 */
int anetDisableTcpNoDelay(char *err, int fd)
{
    return anetSetTcpNoDelay(err, fd, 0);
}

/* 将 socket 的发送超时（SO_SNDTIMEO socket 选项）设置为指定的毫秒数，
 * 如果 ms 参数为 0，则禁用超时。 */
int anetSendTimeout(char *err, int fd, long long ms) {
    struct timeval tv;

    tv.tv_sec = ms/1000;
    tv.tv_usec = (ms%1000)*1000;
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == -1) {
        anetSetError(err, "setsockopt SO_SNDTIMEO: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

/* 将 socket 的接收超时（SO_RCVTIMEO socket 选项）设置为指定的毫秒数，
 * 如果 ms 参数为 0，则禁用超时。 */
int anetRecvTimeout(char *err, int fd, long long ms) {
    struct timeval tv;

    tv.tv_sec = ms/1000;
    tv.tv_usec = (ms%1000)*1000;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1) {
        anetSetError(err, "setsockopt SO_RCVTIMEO: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

/* 解析主机名 "host"，并将 IP 地址的字符串表示写入 ipbuf 所指向的缓冲区。
 *
 * 如果 flags 设置了 ANET_IP_ONLY，则该函数只会解析原本就是 IPv4 或 IPv6
 * 地址的主机名。这会让函数变成一个验证/规范化函数。
 *
 * 如果设置了 ANET_PREFER_IPV4 标志，则优先选择 IPv4。
 * 如果设置了 ANET_PREFER_IPV6 标志，则优先选择 IPv6。
 * */
int anetResolve(char *err, char *host, char *ipbuf, size_t ipbuf_len,
                       int flags)
{
    struct addrinfo hints, *info;
    int rv;

    memset(&hints,0,sizeof(hints));
    if (flags & ANET_IP_ONLY) hints.ai_flags = AI_NUMERICHOST;
    hints.ai_family = AF_UNSPEC;
    if (flags & ANET_PREFER_IPV4 && !(flags & ANET_PREFER_IPV6)) {
        hints.ai_family = AF_INET;
    } else if (flags & ANET_PREFER_IPV6 && !(flags & ANET_PREFER_IPV4)) {
        hints.ai_family = AF_INET6;
    }
    hints.ai_socktype = SOCK_STREAM;  /* 指定 socktype 以避免出现重复项 */

    rv = getaddrinfo(host, NULL, &hints, &info);
    if (rv != 0 && hints.ai_family != AF_UNSPEC) {
        /* 尝试使用另一种 IP 版本。 */
        hints.ai_family = (hints.ai_family == AF_INET) ? AF_INET6 : AF_INET;
        rv = getaddrinfo(host, NULL, &hints, &info);
    }
    if (rv != 0) {
        anetSetError(err, "%s", gai_strerror(rv));
        return ANET_ERR;
    }
    if (info->ai_family == AF_INET) {
        struct sockaddr_in *sa = (struct sockaddr_in *)info->ai_addr;
        inet_ntop(AF_INET, &(sa->sin_addr), ipbuf, ipbuf_len);
    } else {
        struct sockaddr_in6 *sa = (struct sockaddr_in6 *)info->ai_addr;
        inet_ntop(AF_INET6, &(sa->sin6_addr), ipbuf, ipbuf_len);
    }

    freeaddrinfo(info);
    return ANET_OK;
}

/* 在 fd 上启用 SO_REUSEADDR 选项。
 * 确保类似 redis benchmark 这种高频率建连的程序可以反复开关 socket。 */
static int anetSetReuseAddr(char *err, int fd) {
    int yes = 1;
    /* 确保像 redis benchmark 这类频繁建连的程序能够无数次地关闭/打开 socket */
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
        anetSetError(err, "setsockopt SO_REUSEADDR: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

/* 创建一个指定协议族的流式 socket，并默认启用 SO_REUSEADDR。
 * 成功返回 socket 文件描述符，失败返回 ANET_ERR。 */
static int anetCreateSocket(char *err, int domain) {
    int s;
    if ((s = socket(domain, SOCK_STREAM, 0)) == -1) {
        anetSetError(err, "creating socket: %s", strerror(errno));
        return ANET_ERR;
    }

    /* 确保类似 redis benchmark 这种高频率建连的程序能够反复开关 socket */
    if (anetSetReuseAddr(err,s) == ANET_ERR) {
        close(s);
        return ANET_ERR;
    }
    return s;
}

/* 不带任何特殊标志的连接。 */
#define ANET_CONNECT_NONE 0
/* 使用非阻塞模式的连接。 */
#define ANET_CONNECT_NONBLOCK 1
/* 尽力绑定（best effort binding）：如果绑定失败，则再以无绑定方式重试。 */
#define ANET_CONNECT_BE_BINDING 2 /* Best effort binding. */

/* 通用的 TCP 连接函数，支持阻塞/非阻塞以及可选的源地址绑定。
 * addr 为目标地址，port 为目标端口，source_addr 为可选的源地址（用于绑定）。 */
static int anetTcpGenericConnect(char *err, const char *addr, int port,
                                 const char *source_addr, int flags)
{
    int s = ANET_ERR, rv;
    char portstr[6];  /* strlen("65535") + 1; */
    struct addrinfo hints, *servinfo, *bservinfo, *p, *b;

    snprintf(portstr,sizeof(portstr),"%d",port);
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo(addr,portstr,&hints,&servinfo)) != 0) {
        anetSetError(err, "%s", gai_strerror(rv));
        return ANET_ERR;
    }
    for (p = servinfo; p != NULL; p = p->ai_next) {
        /* 尝试创建 socket 并连接到目标地址。
         * 如果 socket() 或 connect() 调用失败，则使用 servinfo 中的下一项重试。 */
        if ((s = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1)
            continue;
        if (anetSetReuseAddr(err,s) == ANET_ERR) goto error;
        if (flags & ANET_CONNECT_NONBLOCK && anetNonBlock(err,s) != ANET_OK)
            goto error;
        if (source_addr) {
            int bound = 0;
            /* 使用 getaddrinfo 让我们无需自行判断 IPv4 还是 IPv6 */
            if ((rv = getaddrinfo(source_addr, NULL, &hints, &bservinfo)) != 0)
            {
                anetSetError(err, "%s", gai_strerror(rv));
                goto error;
            }
            for (b = bservinfo; b != NULL; b = b->ai_next) {
                if (bind(s,b->ai_addr,b->ai_addrlen) != -1) {
                    bound = 1;
                    break;
                }
            }
            freeaddrinfo(bservinfo);
            if (!bound) {
                anetSetError(err, "bind: %s", strerror(errno));
                goto error;
            }
        }
        if (connect(s,p->ai_addr,p->ai_addrlen) == -1) {
            /* 如果 socket 是非阻塞的，则 connect() 在此处返回 EINPROGRESS 错误是正常的。 */
            if (errno == EINPROGRESS && flags & ANET_CONNECT_NONBLOCK)
                goto end;
            close(s);
            s = ANET_ERR;
            continue;
        }

        /* 如果我们在 for 循环中某次迭代顺利完成而没有出错，
         * 那么我们就有了一个已连接的 socket。返回给调用者。 */
        goto end;
    }
    if (p == NULL)
        anetSetError(err, "creating socket: %s", strerror(errno));

error:
    if (s != ANET_ERR) {
        close(s);
        s = ANET_ERR;
    }

end:
    freeaddrinfo(servinfo);

    /* 处理尽力绑定：如果指定了绑定地址但无法建立连接，
     * 则尝试不使用绑定地址再次连接。 */
    if (s == ANET_ERR && source_addr && (flags & ANET_CONNECT_BE_BINDING)) {
        return anetTcpGenericConnect(err,addr,port,NULL,flags);
    } else {
        return s;
    }
}

/* 以非阻塞模式建立 TCP 连接（不绑定源地址）。 */
int anetTcpNonBlockConnect(char *err, const char *addr, int port)
{
    return anetTcpGenericConnect(err,addr,port,NULL,ANET_CONNECT_NONBLOCK);
}

/* 以非阻塞模式建立 TCP 连接，并尝试将源地址绑定到 source_addr。
 * 如果绑定失败则回退到不带源地址的方式（best-effort 绑定）。 */
int anetTcpNonBlockBestEffortBindConnect(char *err, const char *addr, int port,
                                         const char *source_addr)
{
    return anetTcpGenericConnect(err,addr,port,source_addr,
            ANET_CONNECT_NONBLOCK|ANET_CONNECT_BE_BINDING);
}

/* 通用的 Unix 域 socket 连接。
 * 成功返回 socket 文件描述符，失败返回 ANET_ERR。 */
int anetUnixGenericConnect(char *err, const char *path, int flags)
{
    int s;
    struct sockaddr_un sa;

    if ((s = anetCreateSocket(err,AF_LOCAL)) == ANET_ERR)
        return ANET_ERR;

    sa.sun_family = AF_LOCAL;
    redis_strlcpy(sa.sun_path,path,sizeof(sa.sun_path));
    if (flags & ANET_CONNECT_NONBLOCK) {
        if (anetNonBlock(err,s) != ANET_OK) {
            close(s);
            return ANET_ERR;
        }
    }
    if (connect(s,(struct sockaddr*)&sa,sizeof(sa)) == -1) {
        if (errno == EINPROGRESS &&
            flags & ANET_CONNECT_NONBLOCK)
            return s;

        anetSetError(err, "connect: %s", strerror(errno));
        close(s);
        return ANET_ERR;
    }
    return s;
}

/* 将 socket 绑定到 sa 并开始监听。
 * 对于 Unix 域 socket，如果 perm 非零则修改其文件权限。
 * 成功返回 ANET_OK，失败返回 ANET_ERR。 */
static int anetListen(char *err, int s, struct sockaddr *sa, socklen_t len, int backlog, mode_t perm) {
    if (bind(s,sa,len) == -1) {
        anetSetError(err, "bind: %s", strerror(errno));
        close(s);
        return ANET_ERR;
    }

    if (sa->sa_family == AF_LOCAL && perm)
        chmod(((struct sockaddr_un *) sa)->sun_path, perm);

    if (listen(s, backlog) == -1) {
        anetSetError(err, "listen: %s", strerror(errno));
        close(s);
        return ANET_ERR;
    }
    return ANET_OK;
}

/* 在 IPv6 socket 上设置 IPV6_V6ONLY 标志，
 * 限制该 socket 仅接受 IPv6 连接。 */
static int anetV6Only(char *err, int s) {
    int yes = 1;
    if (setsockopt(s,IPPROTO_IPV6,IPV6_V6ONLY,&yes,sizeof(yes)) == -1) {
        anetSetError(err, "setsockopt: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

/* 通用的 TCP 服务器 socket 创建函数。
 * 解析 bindaddr 与 port 创建监听 socket；如果 bindaddr 为 "*"，则
 * 等同于 INADDR_ANY。如果 af 为 AF_INET6，则将 socket 限制为仅 IPv6。
 * 成功返回 socket 文件描述符，失败返回 ANET_ERR。 */
static int _anetTcpServer(char *err, int port, char *bindaddr, int af, int backlog)
{
    int s = -1, rv;
    char _port[6];  /* strlen("65535") */
    struct addrinfo hints, *servinfo, *p;

    snprintf(_port,6,"%d",port);
    memset(&hints,0,sizeof(hints));
    hints.ai_family = af;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;    /* 当 bindaddr != NULL 时该选项无效 */
    if (bindaddr && !strcmp("*", bindaddr))
        bindaddr = NULL;
    if (af == AF_INET6 && bindaddr && !strcmp("::*", bindaddr))
        bindaddr = NULL;

    if ((rv = getaddrinfo(bindaddr,_port,&hints,&servinfo)) != 0) {
        anetSetError(err, "%s", gai_strerror(rv));
        return ANET_ERR;
    }
    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((s = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1)
            continue;

        if (af == AF_INET6 && anetV6Only(err,s) == ANET_ERR) goto error;
        if (anetSetReuseAddr(err,s) == ANET_ERR) goto error;
        if (anetListen(err,s,p->ai_addr,p->ai_addrlen,backlog,0) == ANET_ERR) s = ANET_ERR;
        goto end;
    }
    if (p == NULL) {
        anetSetError(err, "unable to bind socket, errno: %d", errno);
        goto error;
    }

error:
    if (s != -1) close(s);
    s = ANET_ERR;
end:
    freeaddrinfo(servinfo);
    return s;
}

/* 创建一个 IPv4 TCP 监听 socket 并绑定到指定地址和端口。
 * bindaddr 可以是 NULL（INADDR_ANY）、具体的 IP 字符串，或 "*" 表示通配。
 * backlog 是 listen() 的 backlog 参数。
 * 成功返回 socket 文件描述符，失败返回 ANET_ERR。 */
int anetTcpServer(char *err, int port, char *bindaddr, int backlog)
{
    return _anetTcpServer(err, port, bindaddr, AF_INET, backlog);
}

/* 创建一个 IPv6 TCP 监听 socket 并绑定到指定地址和端口。
 * bindaddr 可以是 NULL（INADDR_ANY）、具体的 IP 字符串，或 "::*" 表示通配。
 * backlog 是 listen() 的 backlog 参数。
 * 成功返回 socket 文件描述符，失败返回 ANET_ERR。 */
int anetTcp6Server(char *err, int port, char *bindaddr, int backlog)
{
    return _anetTcpServer(err, port, bindaddr, AF_INET6, backlog);
}

/* 创建一个 Unix 域 socket 监听 socket，并绑定到 path 指定的路径。
 * perm 是创建出来的 socket 文件权限；backlog 是 listen() 的 backlog 参数。
 * 成功返回 socket 文件描述符，失败返回 ANET_ERR。 */
int anetUnixServer(char *err, char *path, mode_t perm, int backlog)
{
    int s;
    struct sockaddr_un sa;

    if (strlen(path) > sizeof(sa.sun_path)-1) {
        anetSetError(err,"unix socket path too long (%zu), must be under %zu", strlen(path), sizeof(sa.sun_path));
        return ANET_ERR;
    }
    if ((s = anetCreateSocket(err,AF_LOCAL)) == ANET_ERR)
        return ANET_ERR;

    memset(&sa,0,sizeof(sa));
    sa.sun_family = AF_LOCAL;
    redis_strlcpy(sa.sun_path,path,sizeof(sa.sun_path));
    if (anetListen(err,s,(struct sockaddr*)&sa,sizeof(sa),backlog,perm) == ANET_ERR)
        return ANET_ERR;
    return s;
}

/* 接受一个新连接，并确保返回的 socket 为非阻塞且设置了 CLOEXEC。
 * 成功返回新的 socket FD，失败返回 ANET_ERR。 */
static int anetGenericAccept(char *err, int s, struct sockaddr *sa, socklen_t *len) {
    int fd;
    do {
        /* 在 Linux 上使用 accept4() 调用以在接收连接的同时
         * 将 socket 设置为非阻塞。 */
#ifdef HAVE_ACCEPT4
        fd = accept4(s, sa, len,  SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
        fd = accept(s,sa,len);
#endif
    } while(fd == -1 && errno == EINTR);
    if (fd == -1) {
        anetSetError(err, "accept: %s", strerror(errno));
        return ANET_ERR;
    }
#ifndef HAVE_ACCEPT4
    if (anetCloexec(fd) == -1) {
        anetSetError(err, "anetCloexec: %s", strerror(errno));
        close(fd);
        return ANET_ERR;
    }
    if (anetNonBlock(err, fd) != ANET_OK) {
        close(fd);
        return ANET_ERR;
    }
#endif
    return fd;
}

/* 接受一个 TCP 连接，并确保 socket 为非阻塞且设置了 CLOEXEC。
 * 如果 ip 和 port 不为 NULL，则将客户端的 IP 地址和端口写入其中。
 * 成功返回新的 socket FD，失败返回 ANET_ERR。 */
int anetTcpAccept(char *err, int serversock, char *ip, size_t ip_len, int *port) {
    int fd;
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);
    if ((fd = anetGenericAccept(err,serversock,(struct sockaddr*)&sa,&salen)) == ANET_ERR)
        return ANET_ERR;

    if (sa.ss_family == AF_INET) {
        struct sockaddr_in *s = (struct sockaddr_in *)&sa;
        if (ip) inet_ntop(AF_INET,(void*)&(s->sin_addr),ip,ip_len);
        if (port) *port = ntohs(s->sin_port);
    } else {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&sa;
        if (ip) inet_ntop(AF_INET6,(void*)&(s->sin6_addr),ip,ip_len);
        if (port) *port = ntohs(s->sin6_port);
    }
    return fd;
}

/* 接受一个 Unix 域连接，并确保 socket 为非阻塞且设置了 CLOEXEC。
 * 成功返回新的 socket FD，失败返回 ANET_ERR。 */
int anetUnixAccept(char *err, int s) {
    int fd;
    struct sockaddr_un sa;
    socklen_t salen = sizeof(sa);
    if ((fd = anetGenericAccept(err,s,(struct sockaddr*)&sa,&salen)) == ANET_ERR)
        return ANET_ERR;

    return fd;
}

/* 将 fd 关联的地址信息格式化为字符串。
 * 如果 remote 为非零，则获取对端地址（getpeername）；
 * 否则获取本端地址（getsockname）。
 * IPv4/IPv6 地址写入 ip 缓冲区，端口写入 *port。
 * Unix 域 socket 写入字符串 "/unixsocket"。
 * 成功返回 0，失败返回 -1。 */
int anetFdToString(int fd, char *ip, size_t ip_len, int *port, int remote) {
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);

    if (remote) {
        if (getpeername(fd, (struct sockaddr *)&sa, &salen) == -1) goto error;
    } else {
        if (getsockname(fd, (struct sockaddr *)&sa, &salen) == -1) goto error;
    }

    if (sa.ss_family == AF_INET) {
        struct sockaddr_in *s = (struct sockaddr_in *)&sa;
        if (ip) {
            if (inet_ntop(AF_INET,(void*)&(s->sin_addr),ip,ip_len) == NULL)
                goto error;
        }
        if (port) *port = ntohs(s->sin_port);
    } else if (sa.ss_family == AF_INET6) {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&sa;
        if (ip) {
            if (inet_ntop(AF_INET6,(void*)&(s->sin6_addr),ip,ip_len) == NULL)
                goto error;
        }
        if (port) *port = ntohs(s->sin6_port);
    } else if (sa.ss_family == AF_UNIX) {
        if (ip) {
            int res = snprintf(ip, ip_len, "/unixsocket");
            if (res < 0 || (unsigned int) res >= ip_len) goto error;
        }
        if (port) *port = 0;
    } else {
        goto error;
    }
    return 0;

error:
    if (ip) {
        if (ip_len >= 2) {
            ip[0] = '?';
            ip[1] = '\0';
        } else if (ip_len == 1) {
            ip[0] = '\0';
        }
    }
    if (port) *port = 0;
    return -1;
}

/* 创建一个管道，并按指定标志设置其读端和写端。
 * 注意它支持 pipe2() 和 fcntl(F_SETFL) 中定义的文件标志，
 * 一个常见用例是 O_CLOEXEC|O_NONBLOCK。 */
int anetPipe(int fds[2], int read_flags, int write_flags) {
    int pipe_flags = 0;
#if defined(__linux__) || defined(__FreeBSD__)
    /* 如果可能，尝试利用 pipe2() 一次性设置两端都需要的标志。
     * 设置 O_CLOEXEC 以防止 fd 泄漏没有任何副作用。 */
    pipe_flags = O_CLOEXEC | (read_flags & write_flags);
    if (pipe2(fds, pipe_flags)) {
        /* 在真正失败时直接返回失败；如果 pipe2 不支持则回退到简单 pipe。 */
        if (errno != ENOSYS && errno != EINVAL)
            return -1;
        pipe_flags = 0;
    } else {
        /* 如果两端的标志完全相同，则无需再进行任何额外操作。 */
        if ((O_CLOEXEC | read_flags) == (O_CLOEXEC | write_flags))
            return 0;
        /* 清除已经通过 pipe2 设置过的标志。 */
        read_flags &= ~pipe_flags;
        write_flags &= ~pipe_flags;
    }
#endif

    /* 当到达此处时如果 pipe_flags 为 0，意味着 pipe2 调用失败（或未被尝试），
     * 此时我们尝试使用 pipe。否则我们将跳过 pipe 调用并继续在下面设置具体标志。 */
    if (pipe_flags == 0 && pipe(fds))
        return -1;

    /* 文件描述符标志。
     * 目前仅定义了一个这样的标志：FD_CLOEXEC，即 close-on-exec 标志。 */
    if (read_flags & O_CLOEXEC)
        if (fcntl(fds[0], F_SETFD, FD_CLOEXEC))
            goto error;
    if (write_flags & O_CLOEXEC)
        if (fcntl(fds[1], F_SETFD, FD_CLOEXEC))
            goto error;

    /* 在清除文件描述符标志 O_CLOEXEC 后，剩余的是文件状态标志。 */
    read_flags &= ~O_CLOEXEC;
    if (read_flags)
        if (fcntl(fds[0], F_SETFL, read_flags))
            goto error;
    write_flags &= ~O_CLOEXEC;
    if (write_flags)
        if (fcntl(fds[1], F_SETFL, write_flags))
            goto error;

    return 0;

error:
    close(fds[0]);
    close(fds[1]);
    return -1;
}

/* 为 socket 设置 SO_MARK（Linux 上的 socket 标记 ID），
 * 用于基于 firewall / routing 规则的流量标记。
 * 在不支持 SOCKOPTMARKID 的平台上返回 ANET_OK 但不进行任何操作。 */
int anetSetSockMarkId(char *err, int fd, uint32_t id) {
#ifdef HAVE_SOCKOPTMARKID
    if (setsockopt(fd, SOL_SOCKET, SOCKOPTMARKID, (void *)&id, sizeof(id)) == -1) {
        anetSetError(err, "setsockopt: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
#else
    UNUSED(fd);
    UNUSED(id);
    anetSetError(err,"anetSetSockMarkid unsupported on this platform");
    return ANET_OK;
#endif
}

/* 判断 filepath 指定的文件是否为命名管道（FIFO）。
 * 是则返回 1，否则返回 0（若文件不存在也返回 0）。 */
int anetIsFifo(char *filepath) {
    struct stat sb;
    if (stat(filepath, &sb) == -1) return 0;
    return S_ISFIFO(sb.st_mode);
}

/* 必须在 accept4() 失败后调用该函数。当 err 表示的是已接收连接自身出错，
 * 且可以通过再次调用 accept4() 来继续接受下一个连接时，函数返回 1。
 * 其他错误则要么表示编程错误（例如在已关闭的 fd 上调用 accept()），
 * 要么表示已达资源上限（例如 -EMFILE，达到了打开 fd 上限）。
 * 在后一种情况下，调用者可能需要等待资源可用。
 * 详细信息请参见 accept4() 的文档。 */
int anetAcceptFailureNeedsRetry(int err) {
    if (err == ECONNABORTED)
        return 1;

#if defined(__linux__)
    /* 详细信息请参见
     * https://man7.org/linux/man-pages/man2/accept.2.html 中的 "Error Handling" 部分 */
    if (err == ENETDOWN || err == EPROTO || err == ENOPROTOOPT ||
        err == EHOSTDOWN || err == ENONET || err == EHOSTUNREACH ||
        err == EOPNOTSUPP || err == ENETUNREACH)
    {
        return 1;
    }
#endif
    return 0;
}
