/* 一个简单的事件驱动编程库。最初我为 Jim 的事件循环（Jim 是一个
 * Tcl 解释器）编写了这段代码，但后来将其转换为一个独立的库以便复用。
 *
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "ae.h"
#include "anet.h"
#include "redisassert.h"

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "zmalloc.h"
#include "config.h"

/* 引入本系统支持的最佳多路复用层。
 * 以下顺序应按性能从高到低排列。 */
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
    #ifdef HAVE_EPOLL
    #include "ae_epoll.c"
    #else
        #ifdef HAVE_KQUEUE
        #include "ae_kqueue.c"
        #else
        #include "ae_select.c"
        #endif
    #endif
#endif


/* 创建一个新的事件循环。
 * setsize 是事件循环可处理的最大文件描述符数量。
 * 成功返回指向新创建的 aeEventLoop 的指针，失败返回 NULL。
 * 时间复杂度：O(1) */
aeEventLoop *aeCreateEventLoop(int setsize) {
    aeEventLoop *eventLoop;
    int i;

    monotonicInit();    /* 以防调用方未初始化（防御性调用）*/

    if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err;
    // 为文件事件数组和已触发事件数组分配内存
    eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent)*setsize);
    if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;
    eventLoop->setsize = setsize;            // 记录最大文件描述符数量
    eventLoop->timeEventHead = NULL;         // 时间事件链表头初始化为空
    eventLoop->timeEventNextId = 0;          // 下一个时间事件 ID 从 0 开始
    eventLoop->stop = 0;                     // 事件循环运行标志
    eventLoop->maxfd = -1;                   // 当前最大文件描述符，-1 表示没有
    eventLoop->beforesleep = NULL;           // 睡眠前回调函数
    eventLoop->aftersleep = NULL;            // 睡眠后回调函数
    eventLoop->flags = 0;                    // 事件循环全局标志位
    if (aeApiCreate(eventLoop) == -1) goto err;
    /* mask == AE_NONE 表示该文件描述符未注册任何事件，
     * 因此用 AE_NONE 初始化整个向量。 */
    for (i = 0; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return eventLoop;

err:
    // 出错时清理已经分配的内存资源
    if (eventLoop) {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}

/* 返回事件循环当前的 set 大小（即支持的最大文件描述符数量）。 */
int aeGetSetSize(aeEventLoop *eventLoop) {
    return eventLoop->setsize;
}

/*
 * 通知事件处理尽快改变等待超时。
 *
 * 注意：这只是打开或关闭全局的 AE_DONT_WAIT 标志。
 * 标志置位后，aeApiPoll 调用将不再阻塞。
 */
void aeSetDontWait(aeEventLoop *eventLoop, int noWait) {
    if (noWait)
        eventLoop->flags |= AE_DONT_WAIT;
    else
        eventLoop->flags &= ~AE_DONT_WAIT;
}

/* 调整事件循环的最大 set 大小。
 * 如果请求的新 set 大小小于当前 set 大小，但已经有正在使用的
 * 文件描述符 >= (请求的 set 大小 - 1)，则返回 AE_ERR，且不做任何修改。
 *
 * 否则返回 AE_OK，表示调整成功。 */
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize) {
    int i;

    // 已是目标大小则无需调整
    if (setsize == eventLoop->setsize) return AE_OK;
    // 若已有 fd 超出新大小，拒绝缩容以避免越界
    if (eventLoop->maxfd >= setsize) return AE_ERR;
    // 先调用底层多路复用 API 完成扩容/缩容
    if (aeApiResize(eventLoop,setsize) == -1) return AE_ERR;

    // 重新分配文件事件和已触发事件数组
    eventLoop->events = zrealloc(eventLoop->events,sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zrealloc(eventLoop->fired,sizeof(aeFiredEvent)*setsize);
    eventLoop->setsize = setsize;

    /* 确保新增的槽位以 AE_NONE 掩码初始化。 */
    for (i = eventLoop->maxfd+1; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return AE_OK;
}

/* 销毁一个事件循环，释放其占用的所有资源。
 * 包括文件事件数组、已触发事件数组以及时间事件链表。 */
void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    aeApiFree(eventLoop);
    zfree(eventLoop->events);
    zfree(eventLoop->fired);

    /* 释放时间事件链表。 */
    aeTimeEvent *next_te, *te = eventLoop->timeEventHead;
    while (te) {
        next_te = te->next;
        // 如果有 finalizerProc，先调用它释放关联的资源
        if (te->finalizerProc)
            te->finalizerProc(eventLoop, te->clientData);
        zfree(te);
        te = next_te;
    }
    zfree(eventLoop);
}

/* 设置事件循环停止标志，下一次 aeProcessEvents 返回后将退出主循环。 */
void aeStop(aeEventLoop *eventLoop) {
    eventLoop->stop = 1;
}

/* 为指定文件描述符注册一个文件事件监听。
 * 参数：
 *   eventLoop  - 事件循环
 *   fd         - 要监听的文件描述符
 *   mask       - 事件类型掩码（AE_READABLE / AE_WRITABLE）
 *   proc       - 事件触发时的回调函数
 *   clientData - 传递给回调函数的客户端数据
 * 返回：AE_OK 表示成功，AE_ERR 表示失败（如 fd 越界）。 */
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData)
{
    // 校验 fd 是否在事件循环容量内
    if (fd >= eventLoop->setsize) {
        errno = ERANGE;
        return AE_ERR;
    }
    aeFileEvent *fe = &eventLoop->events[fd];

    // 在底层多路复用 API 中注册事件
    if (aeApiAddEvent(eventLoop, fd, mask) == -1)
        return AE_ERR;
    fe->mask |= mask;
    if (mask & AE_READABLE) fe->rfileProc = proc;
    if (mask & AE_WRITABLE) fe->wfileProc = proc;
    fe->clientData = clientData;
    // 更新当前最大文件描述符，便于 aeApiPoll 减少扫描范围
    if (fd > eventLoop->maxfd)
        eventLoop->maxfd = fd;
    return AE_OK;
}

/* 取消对指定文件描述符上指定事件类型的监听。
 * 若取消后该 fd 不再监听任何事件，且 fd 是当前的 maxfd，
 * 则向前回溯更新 maxfd。 */
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask)
{
    if (fd >= eventLoop->setsize) return;
    aeFileEvent *fe = &eventLoop->events[fd];
    if (fe->mask == AE_NONE) return;

    /* 当移除 AE_WRITABLE 时，如果设置了 AE_BARRIER，也一并移除。 */
    if (mask & AE_WRITABLE) mask |= AE_BARRIER;

    aeApiDelEvent(eventLoop, fd, mask);
    fe->mask = fe->mask & (~mask);
    if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
        /* 更新 maxfd */
        int j;

        // 从当前 maxfd 向前回溯，找到第一个仍注册事件的 fd
        for (j = eventLoop->maxfd-1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE) break;
        eventLoop->maxfd = j;
    }
}

/* 获取指定文件描述符上注册的客户端数据指针。
 * 若 fd 未注册任何事件，返回 NULL。 */
void *aeGetFileClientData(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return NULL;
    aeFileEvent *fe = &eventLoop->events[fd];
    if (fe->mask == AE_NONE) return NULL;

    return fe->clientData;
}

/* 获取指定文件描述符上注册的事件掩码。
 * 若 fd 越界则返回 0。 */
int aeGetFileEvents(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return 0;
    aeFileEvent *fe = &eventLoop->events[fd];

    return fe->mask;
}

/* 创建一个定时器事件，在指定毫秒数后触发。
 * 参数：
 *   eventLoop      - 事件循环
 *   milliseconds   - 距第一次触发的毫秒数
 *   proc           - 定时器回调函数，返回值决定下次触发的时间间隔
 *   clientData     - 传递给回调函数的客户端数据
 *   finalizerProc  - 事件被删除时调用的清理回调（可为 NULL）
 * 返回：新创建的时间事件 ID，失败返回 AE_ERR。 */
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc)
{
    long long id = eventLoop->timeEventNextId++;
    aeTimeEvent *te;

    te = zmalloc(sizeof(*te));
    if (te == NULL) return AE_ERR;
    te->id = id;
    // 将毫秒转换为微秒（getMonotonicUs 返回值单位为微秒）
    te->when = getMonotonicUs() + milliseconds * 1000;
    te->timeProc = proc;              // 定时器回调
    te->finalizerProc = finalizerProc;// 释放时的清理回调
    te->clientData = clientData;
    // 将新事件插入到时间事件链表的头部
    te->prev = NULL;
    te->next = eventLoop->timeEventHead;
    te->refcount = 0;                 // 引用计数，防止递归调用时被提前释放
    if (te->next)
        te->next->prev = te;
    eventLoop->timeEventHead = te;
    return id;
}

/* 根据 ID 标记删除一个时间事件。
 * 注意：实际删除会被推迟到下一次 processTimeEvents 时执行。
 * 返回：AE_OK 表示找到并标记，AE_ERR 表示未找到对应 ID。 */
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
    aeTimeEvent *te = eventLoop->timeEventHead;
    while(te) {
        if (te->id == id) {
            // 标记为已删除，延迟到 processTimeEvents 时统一清理
            te->id = AE_DELETED_EVENT_ID;
            return AE_OK;
        }
        te = te->next;
    }
    return AE_ERR; /* 未找到指定 ID 的事件 */
}

/* 计算距离最早定时器触发的微秒数。
 * 若没有任何定时器，返回 -1。
 *
 * 注意：因为时间事件链表未排序，本函数时间复杂度为 O(N)。
 * 可考虑的优化（目前 Redis 暂不需要）：
 * 1) 按触发时间顺序插入，使最近的事件总在头部。改进后插入/删除仍是 O(N)。
 * 2) 使用跳表，使查找 O(1)、插入 O(log(N))。 */
static int64_t usUntilEarliestTimer(aeEventLoop *eventLoop) {
    aeTimeEvent *te = eventLoop->timeEventHead;
    if (te == NULL) return -1;

    aeTimeEvent *earliest = NULL;
    while (te) {
        // 跳过已标记删除的事件，找出触发时间最早的未删除事件
        if ((!earliest || te->when < earliest->when) && te->id != AE_DELETED_EVENT_ID)
            earliest = te;
        te = te->next;
    }

    monotime now = getMonotonicUs();
    // 若已到触发时间则返回 0，否则返回剩余等待时间
    return (now >= earliest->when) ? 0 : earliest->when - now;
}

/* 处理所有到期的时间事件。
 * 包括执行到期回调、清理已标记删除的事件、处理定时器返回值。 */
static int processTimeEvents(aeEventLoop *eventLoop) {
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;

    te = eventLoop->timeEventHead;
    // 记录本次迭代开始时已分配的最大 ID，
    // 避免在本轮迭代中处理由本轮回调新创建的事件
    maxId = eventLoop->timeEventNextId-1;
    monotime now = getMonotonicUs();
    while(te) {
        long long id;

        /* 清理已标记删除的事件。 */
        if (te->id == AE_DELETED_EVENT_ID) {
            aeTimeEvent *next = te->next;
            /* 若该事件尚有引用（例如正在被递归回调），则暂时不释放。 */
            if (te->refcount) {
                te = next;
                continue;
            }
            // 从双向链表中摘除该节点
            if (te->prev)
                te->prev->next = te->next;
            else
                eventLoop->timeEventHead = te->next;
            if (te->next)
                te->next->prev = te->prev;
            // 调用 finalizerProc 释放关联资源，并刷新 now
            if (te->finalizerProc) {
                te->finalizerProc(eventLoop, te->clientData);
                now = getMonotonicUs();
            }
            zfree(te);
            te = next;
            continue;
        }

        /* 不处理在本轮迭代中由时间事件回调新创建的事件。
         * 当前实现总是把新事件插入到链表头，本检查暂时无效，
         * 但保留以防未来实现变更，起到防御性作用。 */
        if (te->id > maxId) {
            te = te->next;
            continue;
        }

        // 事件已到期，调用 timeProc 处理
        if (te->when <= now) {
            int retval;

            id = te->id;
            te->refcount++;     // 增加引用计数，防止在回调中事件被删除后被释放
            retval = te->timeProc(eventLoop, id, te->clientData);
            te->refcount--;
            processed++;
            now = getMonotonicUs();
            // 根据返回值决定是重新调度还是删除该定时器
            if (retval != AE_NOMORE) {
                te->when = now + (monotime)retval * 1000;
            } else {
                te->id = AE_DELETED_EVENT_ID;
            }
        }
        te = te->next;
    }
    return processed;
}

/* 处理所有待处理的文件事件，然后处理所有待处理的时间事件
 * （时间事件可能由刚处理完的文件事件回调注册）。
 * 若没有特殊标志，函数会一直阻塞，直到有文件事件触发或下一个时间事件到期。
 *
 * flags 取值说明：
 *   0                            - 函数直接返回，不做任何处理
 *   AE_ALL_EVENTS                - 处理所有类型的事件
 *   AE_FILE_EVENTS               - 仅处理文件事件
 *   AE_TIME_EVENTS               - 仅处理时间事件
 *   AE_DONT_WAIT                 - 处理完不需要等待的事件后立即返回
 *   AE_CALL_AFTER_SLEEP          - 在阻塞返回后调用 aftersleep 回调
 *   AE_CALL_BEFORE_SLEEP         - 在阻塞之前调用 beforesleep 回调
 *
 * 返回值：本轮处理的事件总数（文件事件 + 时间事件）。 */
int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
    int processed = 0, numevents;

    /* 无事可做？立即返回。 */
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    /* 即使当前没有文件事件要处理，只要我们需要处理时间事件，
     * 就仍然要调用 aeApiPoll()，以便阻塞到下一个时间事件到期。 */
    if (eventLoop->maxfd != -1 ||
        ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) {
        int j;
        struct timeval tv, *tvp = NULL; /* tvp 为 NULL 表示无限等待。 */
        int64_t usUntilTimer;

        // 调用 beforesleep 钩子（若启用）
        if (eventLoop->beforesleep != NULL && (flags & AE_CALL_BEFORE_SLEEP))
            eventLoop->beforesleep(eventLoop);

        /* eventLoop->flags 可能在 beforesleep 中被修改，
         * 因此需要在调用之后再检查它。同时参数 flags 的优先级始终最高：
         * 一旦参数 flags 设置了 AE_DONT_WAIT，无论 eventLoop->flags 为何值，
         * 都应忽略它。 */
        if ((flags & AE_DONT_WAIT) || (eventLoop->flags & AE_DONT_WAIT)) {
            // 不等待，立即返回
            tv.tv_sec = tv.tv_usec = 0;
            tvp = &tv;
        } else if (flags & AE_TIME_EVENTS) {
            // 计算距离下一个定时器触发的等待时间
            usUntilTimer = usUntilEarliestTimer(eventLoop);
            if (usUntilTimer >= 0) {
                tv.tv_sec = usUntilTimer / 1000000;
                tv.tv_usec = usUntilTimer % 1000000;
                tvp = &tv;
            }
        }
        /* 调用多路复用 API，只有超时或有事件触发时才会返回。 */
        numevents = aeApiPoll(eventLoop, tvp);

        /* 若调用方未要求处理文件事件，则忽略触发的文件事件。 */
        if (!(flags & AE_FILE_EVENTS)) {
            numevents = 0;
        }

        /* 睡眠结束后调用 aftersleep 回调。 */
        if (eventLoop->aftersleep != NULL && flags & AE_CALL_AFTER_SLEEP)
            eventLoop->aftersleep(eventLoop);

        // 遍历本次轮询中触发的文件事件
        for (j = 0; j < numevents; j++) {
            int fd = eventLoop->fired[j].fd;
            aeFileEvent *fe = &eventLoop->events[fd];
            int mask = eventLoop->fired[j].mask;
            int fired = 0; /* 当前 fd 已触发的事件数。 */

            /* 通常我们先执行读事件，再执行写事件。这有助于在某些场景下，
             * 可以在处理完一个查询后立即返回响应。
             *
             * 但若 mask 中设置了 AE_BARRIER，应用层希望反转调用顺序：
             * 永远先执行写事件，再执行读事件。这种情况适用于
             * 在 beforeSleep() 钩子中先做某些工作（例如将文件 fsync 到磁盘），
             * 再回复客户端。 */
            int invert = fe->mask & AE_BARRIER;

            /* 注意 "fe->mask & mask & ..." 这段判断：
             * 可能前一个事件回调已经移除了某个待处理的事件，
             * 因此需要再次确认该事件仍然有效。
             *
             * 若不需要反转，则先触发读事件。 */
            if (!invert && fe->mask & mask & AE_READABLE) {
                fe->rfileProc(eventLoop,fd,fe->clientData,mask);
                fired++;
                fe = &eventLoop->events[fd]; /* 重新获取指针，以防 resize。 */
            }

            /* 触发写事件。 */
            if (fe->mask & mask & AE_WRITABLE) {
                if (!fired || fe->wfileProc != fe->rfileProc) {
                    fe->wfileProc(eventLoop,fd,fe->clientData,mask);
                    fired++;
                }
            }

            /* 若需要反转调用顺序，则在写事件之后再触发读事件。 */
            if (invert) {
                fe = &eventLoop->events[fd]; /* 重新获取指针，以防 resize。 */
                if ((fe->mask & mask & AE_READABLE) &&
                    (!fired || fe->wfileProc != fe->rfileProc))
                {
                    fe->rfileProc(eventLoop,fd,fe->clientData,mask);
                    fired++;
                }
            }

            processed++;
        }
    }
    /* 检查并处理时间事件 */
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);

    return processed; /* 返回已处理的文件/时间事件数量 */
}

/* 等待最多 milliseconds 毫秒，直到给定 fd 变为可读、可写或出错。
 * 返回值：
 *   掩码表示哪些事件就绪；
 *   0 表示超时；
 *  -1 表示调用出错（errno 被设置）。 */
int aeWait(int fd, int mask, long long milliseconds) {
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

    if ((retval = poll(&pfd, 1, milliseconds))== 1) {
        // 将 poll 返回的事件转换为 ae 的事件掩码
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
        // 错误和挂起也视为可写，以便上层尽快处理
        if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        return retmask;
    } else {
        // 0 表示超时，-1 表示错误
        return retval;
    }
}

/* 主事件循环入口。持续处理事件直到 stop 标志被设置。
 * 处理过程中会同时调用 beforesleep 和 aftersleep 钩子。 */
void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    while (!eventLoop->stop) {
        aeProcessEvents(eventLoop, AE_ALL_EVENTS|
                                   AE_CALL_BEFORE_SLEEP|
                                   AE_CALL_AFTER_SLEEP);
    }
}

/* 返回当前正在使用的多路复用 API 名称（epoll/kqueue/evport/select）。 */
char *aeGetApiName(void) {
    return aeApiName();
}

/* 设置事件循环进入睡眠前调用的钩子函数。 */
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
    eventLoop->beforesleep = beforesleep;
}

/* 设置事件循环从睡眠中唤醒后调用的钩子函数。 */
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep) {
    eventLoop->aftersleep = aftersleep;
}
