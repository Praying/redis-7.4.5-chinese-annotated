/* 线程管理器
 *
 * 本模块用于在 Linux 系统上向指定线程发送信号并等待其完成。
 * 主要用于 Redis Module API 的 ThreadSafeContext 功能。
 *
 * Copyright (c) 2021-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "threads_mngr.h"
/* 用于消除未使用参数警告的宏 */
#define UNUSED(V) ((void) V)

#ifdef __linux__
#include "atomicvar.h"
#include "server.h"

#include <signal.h>
#include <time.h>
#include <sys/syscall.h>

#define IN_PROGRESS 1
/* 等待线程完成的最大超时时间（秒） */
static const clock_t RUN_ON_THREADS_TIMEOUT = 2;

/*================================= 全局变量 ================================= */

static run_on_thread_cb g_callback = NULL;          /* 全局回调函数指针 */
static volatile size_t g_tids_len = 0;             /* 目标线程数量 */
static redisAtomic size_t g_num_threads_done = 0;   /* 已完成线程计数 */

/* 标志位：表示 ThreadsManager_runOnThreads 正在执行中 */
static redisAtomic int g_in_progress = 0;

/*============================ 内部函数原型 ========================== */

static void invoke_callback(int sig);
/* 测试并设置运行状态，安全启动时返回 0，正在运行时返回 IN_PROGRESS */
static int test_and_start(void);
static void wait_threads(void);
/* 清理全局变量（假设在 g_in_progress 保护下运行，非线程安全） */
static void ThreadsManager_cleanups(void);

/*============================ API 函数实现 ========================== */

/* 初始化线程管理器
 *
 * 注册 SIGUSR2 信号处理器，用于接收线程完成通知 */
void ThreadsManager_init(void) {
    /* 注册信号处理器 */
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    /* 不设置 SA_RESTART 标志意味着：当系统调用或库函数被阻塞时，
     * 若信号处理器被调用，则该调用将以 EINTR 错误失败并使用默认行为 */
    act.sa_flags = 0;
    act.sa_handler = invoke_callback;
    sigaction(SIGUSR2, &act, NULL);
}

/* 在指定线程上运行回调函数
 *
 * tids: 目标线程 ID 数组
 * tids_len: 线程 ID 数量
 * callback: 要在每个线程上执行的回调函数
 *
 * 返回 1 表示成功，0 表示失败（已有运行中的操作） */
__attribute__ ((noinline))
int ThreadsManager_runOnThreads(pid_t *tids, size_t tids_len, run_on_thread_cb callback) {
    /* 检查是否可以安全启动，若正在运行中则直接返回 */
    if(test_and_start() == IN_PROGRESS) {
        return 0;
    }

    /* 更新全局回调函数指针 */
    g_callback = callback;

    /* 设置目标线程数量 */
    g_tids_len = tids_len;

    /* 重置已完成线程计数
     * 处理上次运行超时的情况：可能在调用 ThreadsManager_cleanups 时，
     * 部分线程还未完成并增加 g_num_threads_done（已被设为 0） */
    g_num_threads_done = 0;

    /* 向 tids 数组中的所有线程发送信号 */
    pid_t pid = getpid();
    for (size_t i = 0; i < tids_len ; ++i) {
        syscall(SYS_tgkill, pid, tids[i], THREADS_SIGNAL);
    }

    /* 等待所有线程写入输出数组，或直到超时 */
    wait_threads();

    /* 清理操作，以便下次执行 */
    ThreadsManager_cleanups();

    return 1;
}

/*============================ 内部函数实现 ========================== */


/* 测试并设置运行状态
 *
 * atomicFlagGetSet 将变量设为 1 并返回原值 */
static int test_and_start(void) {
    int prev_state;
    atomicFlagGetSet(g_in_progress, prev_state);

    /* 若 prev_state 为 1，表示已有运行中的操作 */
    return prev_state;
}

/* 信号处理函数：在收到信号时调用回调
 *
 * 注意：使用 noinline 属性确保函数有独立地址，
 * 以便正确作为信号处理器传递 */
__attribute__ ((noinline))
static void invoke_callback(int sig) {
    UNUSED(sig);
    run_on_thread_cb callback = g_callback;
    if (callback) {
        callback();
        atomicIncr(g_num_threads_done, 1);
    } else {
        serverLogFromHandler(LL_WARNING, "tid %ld: ThreadsManager g_callback is NULL", syscall(SYS_gettid));
    }
}

/* 等待所有线程完成或超时 */
static void wait_threads(void) {
    struct timespec timeout_time;
    clock_gettime(CLOCK_REALTIME, &timeout_time);

    /* 计算相对超时时间 */
    timeout_time.tv_sec += RUN_ON_THREADS_TIMEOUT;

    /* 等待所有线程完成调用回调，或直到超时 */
    size_t curr_done_count;
    struct timespec curr_time;

    do {
        struct timeval tv = {
            .tv_sec = 0,
            .tv_usec = 10};
        /* 短暂休眠以让出 CPU 给其他线程
         * usleep 不是信号安全函数，因此使用 select 代替 */
        select(0, NULL, NULL, NULL, &tv);
        atomicGet(g_num_threads_done, curr_done_count);
        clock_gettime(CLOCK_REALTIME, &curr_time);
    } while (curr_done_count < g_tids_len &&
             curr_time.tv_sec <= timeout_time.tv_sec);

    if (curr_time.tv_sec > timeout_time.tv_sec) {
        serverLogRawFromHandler(LL_WARNING, "wait_threads(): waiting threads timed out");
    }

}

/* 清理全局变量，重置为初始状态 */
static void ThreadsManager_cleanups(void) {
    g_callback = NULL;
    g_tids_len = 0;
    g_num_threads_done = 0;

    /* 最后关闭 g_in_progress 标志 */
    atomicSet(g_in_progress, 0);

}
#else

/* 非 Linux 平台的空实现 */
void ThreadsManager_init(void) {
    /* 不做任何操作 */
}

int ThreadsManager_runOnThreads(pid_t *tids, size_t tids_len, run_on_thread_cb callback) {
    /* 不做任何操作 */
    UNUSED(tids);
    UNUSED(tids_len);
    UNUSED(callback);
    return 1;
}

#endif /* __linux__ */
