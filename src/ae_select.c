/* 基于 select() 的 ae.c 模块。
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * select 是 POSIX 标准的 I/O 多路复用接口，
 * 作为 ae 的兼容性后备方案（优先级低于 epoll/kqueue 等）。
 */

#include <sys/select.h>
#include <string.h>

/* select 事件循环状态结构体。
 * 维护读/写 fd_set 集合以及它们的临时副本。 */
typedef struct aeApiState {
    fd_set rfds, wfds;           // 读/写 fd_set 集合（aeApiAddEvent 中维护）
    /* 我们需要保留一份 fd_set 的副本，因为在 select() 调用之后
     * 复用 FD 集合是不安全的（select 会就地修改它们）。 */
    fd_set _rfds, _wfds;         // 临时副本（每次 poll 前复制，每次 poll 后读取）
} aeApiState;

/* 创建 select 事件循环状态。
 * 分配 aeApiState 内存并初始化 fd_set。
 *
 * 返回值：成功返回 0，失败返回 -1。 */
static int aeApiCreate(aeEventLoop *eventLoop) {
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;       // 内存分配失败
    FD_ZERO(&state->rfds);       // 初始化读 fd_set 为空
    FD_ZERO(&state->wfds);       // 初始化写 fd_set 为空
    eventLoop->apidata = state;  // 绑定到事件循环
    return 0;
}

/* 调整事件循环大小（select 实现中仅做容量检查）。
 *
 * 返回值：setsize 小于 FD_SETSIZE 返回 0，否则返回 -1。 */
static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    AE_NOTUSED(eventLoop);
    /* 只需确保 fd_set 类型有足够的空间。 */
    if (setsize >= FD_SETSIZE) return -1;
    return 0;
}

/* 释放 select 事件循环状态占用的内存。 */
static void aeApiFree(aeEventLoop *eventLoop) {
    zfree(eventLoop->apidata);
}

/* 向 select 监听集合中注册 fd 关注的事件。
 *
 * 参数：
 *   fd   —— 要注册的 socket 文件描述符。
 *   mask —— 事件掩码（AE_READABLE / AE_WRITABLE）。
 *
 * 返回值：始终返回 0。 */
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;

    if (mask & AE_READABLE) FD_SET(fd,&state->rfds);  // 关注可读
    if (mask & AE_WRITABLE) FD_SET(fd,&state->wfds);  // 关注可写
    return 0;
}

/* 从 select 监听集合中移除 fd 关注的事件。
 *
 * 参数：
 *   fd   —— 要取消注册的 socket 文件描述符。
 *   mask —— 事件掩码（AE_READABLE / AE_WRITABLE）。 */
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;

    if (mask & AE_READABLE) FD_CLR(fd,&state->rfds);  // 取消关注可读
    if (mask & AE_WRITABLE) FD_CLR(fd,&state->wfds);  // 取消关注可写
}

/* select 实现的事件循环 poll。
 * 阻塞等待 tvp 指定的时间，等待已注册 fd 上的事件就绪。
 * 就绪事件写入 eventLoop->fired[] 数组。
 *
 * 参数：
 *   tvp —— select 阻塞超时时间；NULL 表示无限等待。
 *
 * 返回值：就绪事件数量（>=0）；返回 0 表示超时；-1 表示错误。 */
static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    int retval, j, numevents = 0;

    /* 复制 fd_set，因为 select 调用会修改它们。
     * 必须复制到 _rfds/_wfds 而不是直接传入 rfds/wfds，
     * 否则会破坏 aeApiAddEvent/DelEvent 维护的原始集合。 */
    memcpy(&state->_rfds,&state->rfds,sizeof(fd_set));
    memcpy(&state->_wfds,&state->wfds,sizeof(fd_set));

    /* 调用 select 等待事件就绪；
     * maxfd+1 是 select 要求传入的 nfds（最大 fd + 1）。 */
    retval = select(eventLoop->maxfd+1,
                &state->_rfds,&state->_wfds,NULL,tvp);
    if (retval > 0) {
        /* 有事件就绪，遍历所有 fd 检查是哪些就绪了 */
        for (j = 0; j <= eventLoop->maxfd; j++) {
            int mask = 0;
            aeFileEvent *fe = &eventLoop->events[j];

            if (fe->mask == AE_NONE) continue;  // 该 fd 未注册任何事件
            // 检查读就绪：事件循环注册过 AE_READABLE 且 select 报告可读
            if (fe->mask & AE_READABLE && FD_ISSET(j,&state->_rfds))
                mask |= AE_READABLE;
            // 检查写就绪：事件循环注册过 AE_WRITABLE 且 select 报告可写
            if (fe->mask & AE_WRITABLE && FD_ISSET(j,&state->_wfds))
                mask |= AE_WRITABLE;
            eventLoop->fired[numevents].fd = j;     // 记录就绪的 fd
            eventLoop->fired[numevents].mask = mask; // 记录就绪事件掩码
            numevents++;
        }
    } else if (retval == -1 && errno != EINTR) {
        /* 出错且不是被信号中断，则 panic */
        panic("aeApiPoll: select, %s", strerror(errno));
    }

    return numevents;
}

/* 返回事件循环后端实现名称。 */
static char *aeApiName(void) {
    return "select";
}
