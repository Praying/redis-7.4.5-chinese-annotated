/* ae.c 模块:illumos event ports(事件端口)后端实现。
 *
 * Copyright (c) 2012, Joyent, Inc. All rights reserved.
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


#include <errno.h>
#include <port.h>
#include <poll.h>

#include <sys/types.h>
#include <sys/time.h>

#include <stdio.h>

/* 调试开关:设为非零值可开启调试日志输出 */
static int evport_debug = 0;

/*
 * 本文件基于 event ports(事件端口)接口实现 ae API,该机制自 Solaris 10 起
 * 出现于 Solaris 系操作系统中。通过事件端口接口,我们将文件描述符与端口
 * 进行关联。每个关联同时包含消费者关心的 poll(2) 事件集合
 * (例如 POLLIN 与 POLLOUT)。
 *
 * 本实现存在一个棘手之处:当我们通过 aeApiPoll 返回事件时,对应的文件描述符
 * 会从端口中解除关联(dissociate)。这一步是必须的,因为 poll 事件属于电平
 * 触发(level-triggered),如果 fd 不解除关联,那么在底层状态尚未变化时它会
 * 立即再次触发事件。我们必须在确认调用方已经从 fd 读取之后,才能重新建立
 * 关联。ae API 不会告诉我们读发生的精确时点,但我们确定它在再次调用
 * aeApiPoll 之前一定会发生。我们的方案是记录 aeApiPoll 上一次返回的 fd,
 * 并在下一次调用 aeApiPoll 时重新建立关联。
 *
 * 简而言之,本模块中每个 fd 的关联要么:(a)仅由内核中的关联表示,
 * 要么 (b)由 pending_fds 与 pending_masks 表示。(b) 仅对我们最近一次
 * aeApiPoll 返回的 fd 成立,且仅持续到下次进入 aeApiPoll 为止(届时会恢复
 * 内核中的关联)。
 */
#define MAX_EVENT_BATCHSZ 512

/* Solaris event port 事件循环状态结构体 */
typedef struct aeApiState {
    int     portfd;                             /* event port 文件描述符 */
    uint_t  npending;                           /* 待处理 fd 数量 */
    int     pending_fds[MAX_EVENT_BATCHSZ];     /* 待处理 fd 数组 */
    int     pending_masks[MAX_EVENT_BATCHSZ];   /* 待处理 fd 的事件掩码 */
} aeApiState;

/*
 * 创建 event port 实例并初始化事件循环状态。
 * 分配 aeApiState 结构体,创建 event port 并将其存入 state->portfd,
 * 初始化待处理 fd 数组,最后把 state 挂载到 eventLoop->apidata。
 *
 * 返回值:成功返回 0,失败返回 -1。
 */
static int aeApiCreate(aeEventLoop *eventLoop) {
    int i;
    aeApiState *state = zmalloc(sizeof(aeApiState));
    if (!state) return -1;

    // 创建 Solaris event port
    state->portfd = port_create();
    if (state->portfd == -1) {
        zfree(state);
        return -1;
    }
    // 将 portfd 设为 close-on-exec,避免子进程继承
    anetCloexec(state->portfd);

    state->npending = 0;

    // 初始化待处理 fd 数组
    for (i = 0; i < MAX_EVENT_BATCHSZ; i++) {
        state->pending_fds[i] = -1;
        state->pending_masks[i] = AE_NONE;
    }

    eventLoop->apidata = state;
    return 0;
}

/*
 * 调整事件循环的集合大小。本后端没有需要调整的内部缓冲,
 * 因此该函数实际上是一个空操作。
 *
 * 返回值:始终返回 0。
 */
static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    (void) eventLoop;
    (void) setsize;
    /* 这里不需要调整大小。 */
    return 0;
}

/*
 * 释放 aeApiState 关联的资源。关闭 event port 文件描述符并释放 state。
 */
static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;

    close(state->portfd);
    zfree(state);
}

/*
 * 在待处理 fd 数组中查找给定的 fd。
 *
 * 返回值:找到则返回其在数组中的下标,否则返回 -1。
 */
static int aeApiLookupPending(aeApiState *state, int fd) {
    uint_t i;

    for (i = 0; i < state->npending; i++) {
        if (state->pending_fds[i] == fd)
            return (i);
    }

    return (-1);
}

/*
 * 辅助函数:为给定的 fd 和事件掩码调用 port_associate。
 * 将 ae 的事件掩码转换为 poll(2) 的事件标志,然后调用 port_associate。
 * 若启用 evport_debug,则输出调试日志。
 *
 * 返回值:port_associate 的返回值(成功为 0,失败为 -1)。
 */
static int aeApiAssociate(const char *where, int portfd, int fd, int mask) {
    int events = 0;
    int rv, err;

    if (mask & AE_READABLE)
        events |= POLLIN;
    if (mask & AE_WRITABLE)
        events |= POLLOUT;

    if (evport_debug)
        fprintf(stderr, "%s: port_associate(%d, 0x%x) = ", where, fd, events);

    rv = port_associate(portfd, PORT_SOURCE_FD, fd, events,
        (void *)(uintptr_t)mask);
    err = errno;

    if (evport_debug)
        fprintf(stderr, "%d (%s)\n", rv, rv == 0 ? "no error" : strerror(err));

    if (rv == -1) {
        fprintf(stderr, "%s: port_associate: %s\n", where, strerror(err));

        if (err == EAGAIN)
            fprintf(stderr, "aeApiAssociate: event port limit exceeded.");
    }

    return rv;
}

/*
 * 给指定 fd 添加事件。如果该 fd 在待处理数组中,则只更新其掩码,
 * 否则直接调用 aeApiAssociate 与 port 建立关联。
 *
 * 返回值:成功返回 0,失败返回 aeApiAssociate 的返回值。
 */
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    int fullmask, pfd;

    if (evport_debug)
        fprintf(stderr, "aeApiAddEvent: fd %d mask 0x%x\n", fd, mask);

    /*
     * 由于 port_associate 的 events 参数会替换已有的事件,
     * 我们再次调用 port_associate() 时必须包含 fd 当前已经关联的事件。
     */
    fullmask = mask | eventLoop->events[fd].mask;
    pfd = aeApiLookupPending(state, fd);

    if (pfd != -1) {
        /*
         * 该 fd 刚刚从 aeApiPoll 返回。可以合理假设消费者已经处理了
         * 那个 poll 事件,但出于安全考虑我们仅更新 pending_mask,
         * fd 将在下一次调用 aeApiPoll 时按正常流程重新建立关联。
         */
        if (evport_debug)
            fprintf(stderr, "aeApiAddEvent: adding to pending fd %d\n", fd);
        state->pending_masks[pfd] |= fullmask;
        return 0;
    }

    return (aeApiAssociate("aeApiAddEvent", state->portfd, fd, fullmask));
}

/*
 * 删除 fd 上指定的事件。如果 fd 处于待处理状态,则只更新其掩码;
 * 否则调用 port_dissociate 或重新调用 aeApiAssociate 调整关联。
 */
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    int fullmask, pfd;

    if (evport_debug)
        fprintf(stderr, "del fd %d mask 0x%x\n", fd, mask);

    pfd = aeApiLookupPending(state, fd);

    if (pfd != -1) {
        if (evport_debug)
            fprintf(stderr, "deleting event from pending fd %d\n", fd);

        /*
         * 该 fd 刚从 aeApiPoll 返回,因此当前未与端口关联。
         * 我们只需相应地更新 pending_mask。
         */
        state->pending_masks[pfd] &= ~mask;

        if (state->pending_masks[pfd] == AE_NONE)
            state->pending_fds[pfd] = -1;

        return;
    }

    /*
     * 该 fd 当前已与端口关联。同添加事件一样,在更新关联之前我们必须
     * 考虑该 fd 的完整事件掩码。我们没有更好的办法在不去查看 eventLoop
     * 状态的前提下得知它的事件,这里依赖调用方已事先更新了 eventLoop
     * 中的 mask 这一前提。
     */

    fullmask = eventLoop->events[fd].mask;
    if (fullmask == AE_NONE) {
        /*
         * 我们要移除全部事件,因此使用 port_dissociate 彻底解除关联。
         * 此处失败意味着存在 bug。
         */
        if (evport_debug)
            fprintf(stderr, "aeApiDelEvent: port_dissociate(%d)\n", fd);

        if (port_dissociate(state->portfd, PORT_SOURCE_FD, fd) != 0) {
            perror("aeApiDelEvent: port_dissociate");
            abort(); /* 不会返回 */
        }
    } else if (aeApiAssociate("aeApiDelEvent", state->portfd, fd,
        fullmask) != 0) {
        /*
         * ENOMEM 是潜在瞬时错误,但内核一般只在情况极差时才会返回它。
         * EAGAIN 表示已达到资源上限,(反直觉地)重试没有意义。
         * 其他错误都说明存在 bug。在这些情况下,我们所能做的就是中止。
         */
        abort(); /* 不会返回 */
    }
}

/*
 * 等待事件并返回就绪的 fd 数量。首先将上一轮返回的 fd 重新与端口建立关联,
 * 然后调用 port_getn 获取事件,最后把事件写入 eventLoop->fired 并把 fd
 * 记录到待处理数组中(等待下一轮重新关联)。
 *
 * 返回值:就绪事件的数量。
 */
static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    struct timespec timeout, *tsp;
    uint_t mask, i;
    uint_t nevents;
    port_event_t event[MAX_EVENT_BATCHSZ];

    /*
     * 如果我们之前返回过 fd 事件,那么在调用 port_get() 之前必须先把它们
     * 与端口重新建立关联。详见本文件顶部块注释中的解释。
     */
    for (i = 0; i < state->npending; i++) {
        if (state->pending_fds[i] == -1)
            /* 该 fd 此间已被删除 */
            continue;

        if (aeApiAssociate("aeApiPoll", state->portfd,
            state->pending_fds[i], state->pending_masks[i]) != 0) {
            /* 这种情况是致命的,原因详见 aeApiDelEvent */
            abort();
        }

        state->pending_masks[i] = AE_NONE;
        state->pending_fds[i] = -1;
    }

    state->npending = 0;

    if (tvp != NULL) {
        timeout.tv_sec = tvp->tv_sec;
        timeout.tv_nsec = tvp->tv_usec * 1000;
        tsp = &timeout;
    } else {
        tsp = NULL;
    }

    /*
     * port_getn 可能在 errno == ETIME 时仍然返回了一些事件 (!)。
     * 因此遇到 ETIME 时我们还要再检查 nevents。
     */
    nevents = 1;
    if (port_getn(state->portfd, event, MAX_EVENT_BATCHSZ, &nevents,
        tsp) == -1 && (errno != ETIME || nevents == 0)) {
        if (errno == ETIME || errno == EINTR)
            return 0;

        /* 任何其他错误都说明存在 bug */
        panic("aeApiPoll: port_getn, %s", strerror(errno));
    }

    state->npending = nevents;

    for (i = 0; i < nevents; i++) {
            mask = 0;
            if (event[i].portev_events & POLLIN)
                mask |= AE_READABLE;
            if (event[i].portev_events & POLLOUT)
                mask |= AE_WRITABLE;

            eventLoop->fired[i].fd = event[i].portev_object;
            eventLoop->fired[i].mask = mask;

            if (evport_debug)
                fprintf(stderr, "aeApiPoll: fd %d mask 0x%x\n",
                    (int)event[i].portev_object, mask);

            // 暂存 fd 与掩码,等待下次 aeApiPoll 时重新建立关联
            state->pending_fds[i] = event[i].portev_object;
            state->pending_masks[i] = (uintptr_t)event[i].portev_user;
    }

    return nevents;
}

/* 返回当前事件循环后端实现的名称 "evport" */
static char *aeApiName(void) {
    return "evport";
}
