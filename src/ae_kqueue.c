/* 基于 Kqueue(2) 的 ae.c 模块
 *
 * Copyright (C) 2009 Harish Mallipeddi - harish.mallipeddi@gmail.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>

/* kqueue 事件循环状态结构体。
 * 用于保存 ae 事件循环在 kqueue 后端下的私有数据。 */
typedef struct aeApiState {
    int kqfd;                // kqueue 文件描述符
    struct kevent *events;   // 由 kevent() 返回的就绪事件数组

    /* 用于合并读/写事件的事件位图。
     * 为了减少内存占用，我们使用 2 个比特位来存储一个事件的掩码，
     * 这样 1 个字节就可以存储 4 个事件对应的掩码。 */
    char *eventsMask;
} aeApiState;

/* 计算存储 setsize 个事件掩码所需的字节数（每字节存 4 个事件）。 */
#define EVENT_MASK_MALLOC_SIZE(sz) (((sz) + 3) / 4)
/* 计算 fd 对应的 2 位掩码在字节内的偏移位置（每个 fd 占 2 位）。 */
#define EVENT_MASK_OFFSET(fd) ((fd) % 4 * 2)
/* 将 mask 编码到 eventsMask 字节数组中 fd 对应的那 2 个比特位。 */
#define EVENT_MASK_ENCODE(fd, mask) (((mask) & 0x3) << EVENT_MASK_OFFSET(fd))

/* 从 eventsMask 中取出 fd 对应的事件掩码（2 个比特位）。 */
static inline int getEventMask(const char *eventsMask, int fd) {
    return (eventsMask[fd/4] >> EVENT_MASK_OFFSET(fd)) & 0x3;
}

/* 将 fd 对应的事件掩码按位写入 eventsMask（与已有位或运算）。 */
static inline void addEventMask(char *eventsMask, int fd, int mask) {
    eventsMask[fd/4] |= EVENT_MASK_ENCODE(fd, mask);
}

/* 清除 eventsMask 中 fd 对应的那 2 个比特位。 */
static inline void resetEventMask(char *eventsMask, int fd) {
    eventsMask[fd/4] &= ~EVENT_MASK_ENCODE(fd, 0x3);
}

/* 创建 kqueue 实例，并初始化 aeApiState。
 *
 * 分配事件数组、创建 kqueue 文件描述符、设置 FD_CLOEXEC，
 * 同时分配并清零 eventsMask 位图。
 *
 * 返回 0 表示成功，-1 表示失败。 */
static int aeApiCreate(aeEventLoop *eventLoop) {
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;
    // 为 events 预分配 setsize 个 kevent 的空间
    state->events = zmalloc(sizeof(struct kevent)*eventLoop->setsize);
    if (!state->events) {
        zfree(state);
        return -1;
    }
    // 创建 kqueue 实例
    state->kqfd = kqueue();
    if (state->kqfd == -1) {
        zfree(state->events);
        zfree(state);
        return -1;
    }
    // 设置 kqueue fd 在 exec 时自动关闭
    anetCloexec(state->kqfd);
    // 为事件位图分配空间并清零
    state->eventsMask = zmalloc(EVENT_MASK_MALLOC_SIZE(eventLoop->setsize));
    memset(state->eventsMask, 0, EVENT_MASK_MALLOC_SIZE(eventLoop->setsize));
    // 将私有状态挂载到事件循环上
    eventLoop->apidata = state;
    return 0;
}

/* 调整 events 数组和 eventsMask 位图的大小，以适配新的 setsize。 */
static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    aeApiState *state = eventLoop->apidata;

    // 重新分配 events 数组
    state->events = zrealloc(state->events, sizeof(struct kevent)*setsize);
    // 重新分配并清零 eventsMask 位图
    state->eventsMask = zrealloc(state->eventsMask, EVENT_MASK_MALLOC_SIZE(setsize));
    memset(state->eventsMask, 0, EVENT_MASK_MALLOC_SIZE(setsize));
    return 0;
}

/* 释放 aeApiState 占用的所有资源。 */
static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;

    // 关闭 kqueue 文件描述符
    close(state->kqfd);
    // 释放 events 数组
    zfree(state->events);
    // 释放 eventsMask 位图
    zfree(state->eventsMask);
    // 释放状态结构体本身
    zfree(state);
}

/* 向 kqueue 中注册 fd 上指定类型（读/写）的事件。
 *
 * 如果 mask 包含 AE_READABLE，则通过 EVFILT_READ 监听可读事件；
 * 如果 mask 包含 AE_WRITABLE，则通过 EVFILT_WRITE 监听可写事件。
 * 任一 kevent 调用失败立即返回 -1。 */
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    struct kevent ke;

    if (mask & AE_READABLE) {
        // 注册 fd 上的可读事件
        EV_SET(&ke, fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
        if (kevent(state->kqfd, &ke, 1, NULL, 0, NULL) == -1) return -1;
    }
    if (mask & AE_WRITABLE) {
        // 注册 fd 上的可写事件
        EV_SET(&ke, fd, EVFILT_WRITE, EV_ADD, 0, 0, NULL);
        if (kevent(state->kqfd, &ke, 1, NULL, 0, NULL) == -1) return -1;
    }
    return 0;
}

/* 从 kqueue 中删除 fd 上指定类型（读/写）的事件。 */
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    struct kevent ke;

    if (mask & AE_READABLE) {
        // 删除 fd 上的可读事件
        EV_SET(&ke, fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
        kevent(state->kqfd, &ke, 1, NULL, 0, NULL);
    }
    if (mask & AE_WRITABLE) {
        // 删除 fd 上的可写事件
        EV_SET(&ke, fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
        kevent(state->kqfd, &ke, 1, NULL, 0, NULL);
    }
}

/* 轮询事件循环，等待内核返回就绪事件。
 *
 * tvp 给出最大阻塞时间；为 NULL 表示无限等待。
 * 返回值是就绪事件数（已写入 eventLoop->fired 数组），
 * 0 表示超时，-1 通常意味着错误。 */
static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;

    if (tvp != NULL) {
        struct timespec timeout;
        // 把 struct timeval（微秒精度）转换为 struct timespec（纳秒精度）
        timeout.tv_sec = tvp->tv_sec;
        timeout.tv_nsec = tvp->tv_usec * 1000;
        // 带超时调用 kevent 等待事件
        retval = kevent(state->kqfd, NULL, 0, state->events, eventLoop->setsize,
                        &timeout);
    } else {
        // 无限等待事件
        retval = kevent(state->kqfd, NULL, 0, state->events, eventLoop->setsize,
                        NULL);
    }

    if (retval > 0) {
        int j;

        /* 通常情况下我们先处理读事件，再处理写事件；
         * 当 barrier 被设置时则反过来。
         *
         * 但在 kqueue 中，读和写事件是相互独立的事件，
         * 因此我们无法直接控制它们的处理顺序。
         * 所以这里先把收到的事件按 fd 累积到 eventsMask 里，
         * 稍后再合并同一个 fd 的读/写事件。 */
        for (j = 0; j < retval; j++) {
            struct kevent *e = state->events+j;
            int fd = e->ident;
            int mask = 0;

            // 根据 filter 类型推导出 ae 的事件掩码
            if (e->filter == EVFILT_READ) mask = AE_READABLE;
            else if (e->filter == EVFILT_WRITE) mask = AE_WRITABLE;
            // 将该 fd 的事件掩码累加到位图中
            addEventMask(state->eventsMask, fd, mask);
        }

        /* 再次遍历 events，把同一 fd 的读/写事件合并为一个 fired 事件，
         * 同时清掉 eventsMask 中该 fd 的掩码，避免再次处理时重复添加。 */
        numevents = 0;
        for (j = 0; j < retval; j++) {
            struct kevent *e = state->events+j;
            int fd = e->ident;
            int mask = getEventMask(state->eventsMask, fd);

            if (mask) {
                // 写入 fired 数组并重置位图掩码
                eventLoop->fired[numevents].fd = fd;
                eventLoop->fired[numevents].mask = mask;
                resetEventMask(state->eventsMask, fd);
                numevents++;
            }
        }
    } else if (retval == -1 && errno != EINTR) {
        // 若不是被信号中断，则触发 panic
        panic("aeApiPoll: kevent, %s", strerror(errno));
    }

    return numevents;
}

/* 返回当前事件后端的名称。 */
static char *aeApiName(void) {
    return "kqueue";
}
