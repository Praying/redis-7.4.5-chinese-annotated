/* 基于 Linux epoll(2) 的 ae.c 事件循环模块
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */


#include <sys/epoll.h>

/* epoll 事件循环状态结构体
 * 封装了 epoll 实例句柄以及用于接收就绪事件的数组 */
typedef struct aeApiState {
    int epfd;                        // epoll 实例的文件描述符
    struct epoll_event *events;      // 用于存储 epoll_wait 返回的就绪事件数组
} aeApiState;

/* 创建 epoll 事件循环后端。
 * 分配 aeApiState 与事件数组,并通过 epoll_create 创建 epoll 实例。
 * eventLoop 为事件循环主结构,setsize 表示可监听的最大 fd 数。
 * 成功返回 0,失败返回 -1。 */
static int aeApiCreate(aeEventLoop *eventLoop) {
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;
    // 根据 setsize 预分配用于接收 epoll_wait 就绪事件的事件数组
    state->events = zmalloc(sizeof(struct epoll_event)*eventLoop->setsize);
    if (!state->events) {
        zfree(state);
        return -1;
    }
    state->epfd = epoll_create(1024); /* 1024 只是给内核的一个提示值 */
    if (state->epfd == -1) {
        zfree(state->events);
        zfree(state);
        return -1;
    }
    // 设置 epoll fd 在 exec 时自动关闭,避免泄露到子进程
    anetCloexec(state->epfd);
    eventLoop->apidata = state;
    return 0;
}

/* 调整事件循环中事件数组的大小以适配新的 setsize。
 * eventLoop 为事件循环,setsize 为新的最大 fd 数。
 * 始终返回 0。 */
static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    aeApiState *state = eventLoop->apidata;

    // 按新的大小重新分配就绪事件数组
    state->events = zrealloc(state->events, sizeof(struct epoll_event)*setsize);
    return 0;
}

/* 释放 epoll 事件循环后端所占用的资源,
 * 包括 epoll fd、事件数组以及状态结构体。 */
static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;

    close(state->epfd);      // 关闭 epoll 实例
    zfree(state->events);    // 释放事件数组
    zfree(state);            // 释放状态结构体
}

/* 向 epoll 中注册或修改一个 fd 的监听事件。
 * eventLoop 为事件循环,fd 为要监听的文件描述符,mask 为待监听的事件掩码。
 * 若该 fd 之前未注册,则进行 EPOLL_CTL_ADD;否则进行 EPOLL_CTL_MOD。
 * 成功返回 0,失败返回 -1。 */
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    struct epoll_event ee = {0}; /* 避免 valgrind 警告 */
    /* 如果 fd 已经在监听某些事件,则需要 MOD 操作;
     * 否则需要 ADD 操作。 */
    int op = eventLoop->events[fd].mask == AE_NONE ?
            EPOLL_CTL_ADD : EPOLL_CTL_MOD;

    ee.events = 0;
    mask |= eventLoop->events[fd].mask; /* 合并旧事件 */
    if (mask & AE_READABLE) ee.events |= EPOLLIN;   // 监听可读
    if (mask & AE_WRITABLE) ee.events |= EPOLLOUT;  // 监听可写
    ee.data.fd = fd;
    // 调用 epoll_ctl 完成实际的注册/修改
    if (epoll_ctl(state->epfd,op,fd,&ee) == -1) return -1;
    return 0;
}

/* 从 epoll 中删除 fd 上指定类型(由 delmask 标识)的监听事件。
 * 若删除后该 fd 没有任何事件需要监听,则从 epoll 中彻底移除;
 * 否则使用 EPOLL_CTL_MOD 更新剩余事件。 */
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int delmask) {
    aeApiState *state = eventLoop->apidata;
    struct epoll_event ee = {0}; /* 避免 valgrind 警告 */
    // 从现有 mask 中清除要删除的事件位
    int mask = eventLoop->events[fd].mask & (~delmask);

    ee.events = 0;
    if (mask & AE_READABLE) ee.events |= EPOLLIN;
    if (mask & AE_WRITABLE) ee.events |= EPOLLOUT;
    ee.data.fd = fd;
    if (mask != AE_NONE) {
        // 还有事件需要监听,使用 MOD 更新
        epoll_ctl(state->epfd,EPOLL_CTL_MOD,fd,&ee);
    } else {
        /* 注意,内核版本 < 2.6.9 在调用 EPOLL_CTL_DEL 时
         * 也要求传入一个非空的 event 指针。 */
        epoll_ctl(state->epfd,EPOLL_CTL_DEL,fd,&ee);
    }
}

/* 阻塞等待 epoll 上的事件就绪,并将结果填充到 eventLoop->fired 中。
 * eventLoop 为事件循环,tvp 指定最大等待时间(可为 NULL 表示无限等待)。
 * 返回就绪事件的数量。 */
static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;

    // 将 timeval 转换为毫秒;若 tvp 为 NULL 则 -1 表示无限等待
    retval = epoll_wait(state->epfd,state->events,eventLoop->setsize,
            tvp ? (tvp->tv_sec*1000 + (tvp->tv_usec + 999)/1000) : -1);
    if (retval > 0) {
        int j;

        numevents = retval;
        for (j = 0; j < numevents; j++) {
            int mask = 0;
            struct epoll_event *e = state->events+j;

            // 将 epoll 事件标志转换为 ae 事件循环的事件掩码
            if (e->events & EPOLLIN) mask |= AE_READABLE;
            if (e->events & EPOLLOUT) mask |= AE_WRITABLE;
            if (e->events & EPOLLERR) mask |= AE_WRITABLE|AE_READABLE;
            if (e->events & EPOLLHUP) mask |= AE_WRITABLE|AE_READABLE;
            eventLoop->fired[j].fd = e->data.fd;       // 记录触发的 fd
            eventLoop->fired[j].mask = mask;           // 记录触发的事件掩码
        }
    } else if (retval == -1 && errno != EINTR) {
        // 若不是被信号中断则报错;EINTR 是可接受的
        panic("aeApiPoll: epoll_wait, %s", strerror(errno));
    }

    return numevents;
}

/* 返回当前事件循环后端的名称,用于日志或调试输出。 */
static char *aeApiName(void) {
    return "epoll";
}
