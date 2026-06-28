/* 子进程信息管理
 *
 * 本文件处理 Redis 在执行 RDB / AOF 后台保存操作时，
 * 子进程与父进程之间的信息传递（如 Copy-On-Write 内存使用量）。
 *
 * Copyright (c) 2016-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include <unistd.h>
#include <fcntl.h>

/* 子进程信息数据结构体 */
typedef struct {
    size_t keys;              /* 已处理的键数量 */
    size_t cow;               /* Copy-On-Write 内存大小（字节） */
    monotime cow_updated;     /* CoW 数据更新时间 */
    double progress;          /* 保存进度（0.0 - 1.0） */
    childInfoType information_type; /* 信息类型 */
} child_info_data;

/* 打开父子进程通信管道
 *
 * 该管道用于在 RDB / AOF 保存过程中，将子进程的信息（如使用的 Copy-On-Write 内存量）
 * 传递给父进程。 */
void openChildInfoPipe(void) {
    if (anetPipe(server.child_info_pipe, O_NONBLOCK, 0) == -1) {
        /* 若发生错误，两个文件描述符应仍为 -1，
         * 但为安全起见仍调用 closeChildInfoPipe() 进行清理。 */
        closeChildInfoPipe();
    } else {
        server.child_info_nread = 0;
    }
}

/* 关闭由 openChildInfoPipe() 打开的管道 */
void closeChildInfoPipe(void) {
    if (server.child_info_pipe[0] != -1 ||
        server.child_info_pipe[1] != -1)
    {
        close(server.child_info_pipe[0]);
        close(server.child_info_pipe[1]);
        server.child_info_pipe[0] = -1;
        server.child_info_pipe[1] = -1;
        server.child_info_nread = 0;
    }
}

/* 向父进程发送保存数据
 *
 * info_type: 信息类型（当前信息/AOF CoW/RDB CoW/模块 CoW）
 * keys: 已处理的键数量
 * progress: 保存进度百分比
 * pname: 进程名称（如 "RDB" 或 "AOF"） */
void sendChildInfoGeneric(childInfoType info_type, size_t keys, double progress, char *pname) {
    if (server.child_info_pipe[1] == -1) return;

    static monotime cow_updated = 0;
    static uint64_t cow_update_cost = 0;
    static size_t cow = 0;
    static size_t peak_cow = 0;
    static size_t update_count = 0;
    static unsigned long long sum_cow = 0;

    /* 初始化所有字段为零（包括填充字节，以满足 valgrind 检测） */
    child_info_data data = {0};

    /* 当报告当前信息时，需要限制 CoW 更新的频率，因为该操作开销较大。
     * 为此，我们测量获取读数所需的时间，并调度下一次读取在 time*CHILD_COW_COST_FACTOR
     * 时间后发生。 */

    monotime now = getMonotonicUs();
    if (info_type != CHILD_INFO_TYPE_CURRENT_INFO ||
        !cow_updated ||
        now - cow_updated > cow_update_cost * CHILD_COW_DUTY_CYCLE)
    {
        cow = zmalloc_get_private_dirty(-1);
        cow_updated = getMonotonicUs();
        cow_update_cost = cow_updated - now;
        if (cow > peak_cow) peak_cow = cow;
        sum_cow += cow;
        update_count++;

        int cow_info = (info_type != CHILD_INFO_TYPE_CURRENT_INFO);
        if (cow || cow_info) {
            serverLog(cow_info ? LL_NOTICE : LL_VERBOSE,
                      "Fork CoW for %s: current %zu MB, peak %zu MB, average %llu MB",
                      pname, cow>>20, peak_cow>>20, (sum_cow/update_count)>>20);
        }
    }

    data.information_type = info_type;
    data.keys = keys;
    data.cow = cow;
    data.cow_updated = cow_updated;
    data.progress = progress;

    ssize_t wlen = sizeof(data);

    if (write(server.child_info_pipe[1], &data, wlen) != wlen) {
        /* 向父进程写入失败，父进程可能已被终止，直接退出子进程 */
        serverLog(LL_WARNING,"Child failed reporting info to parent, exiting. %s", strerror(errno));
        exitFromChild(1);
    }
}

/* 更新子进程信息
 *
 * 将从子进程接收到的信息更新到 server 结构体的统计变量中 */
void updateChildInfo(childInfoType information_type, size_t cow, monotime cow_updated, size_t keys, double progress) {
    if (cow > server.stat_current_cow_peak) server.stat_current_cow_peak = cow;

    if (information_type == CHILD_INFO_TYPE_CURRENT_INFO) {
        server.stat_current_cow_bytes = cow;
        server.stat_current_cow_updated = cow_updated;
        server.stat_current_save_keys_processed = keys;
        if (progress != -1) server.stat_module_progress = progress;
    } else if (information_type == CHILD_INFO_TYPE_AOF_COW_SIZE) {
        server.stat_aof_cow_bytes = server.stat_current_cow_peak;
    } else if (information_type == CHILD_INFO_TYPE_RDB_COW_SIZE) {
        server.stat_rdb_cow_bytes = server.stat_current_cow_peak;
    } else if (information_type == CHILD_INFO_TYPE_MODULE_COW_SIZE) {
        server.stat_module_cow_bytes = server.stat_current_cow_peak;
    }
}

/* 从管道读取子进程信息数据
 *
 * 若完整数据已读入缓冲区，数据存储到 *buffer 并返回 1。
 * 否则，部分数据留在缓冲区，等待下次读取，返回 0。 */
int readChildInfo(childInfoType *information_type, size_t *cow, monotime *cow_updated, size_t *keys, double* progress) {
    /* 使用静态缓冲区配合 server.child_info_nread 处理短读取（short reads） */
    static child_info_data buffer;
    ssize_t wlen = sizeof(buffer);

    /* 防止数据重叠：若已读取完整结构体，则重置计数器 */
    if (server.child_info_nread == wlen) server.child_info_nread = 0;

    int nread = read(server.child_info_pipe[0], (char *)&buffer + server.child_info_nread, wlen - server.child_info_nread);
    if (nread > 0) {
        server.child_info_nread += nread;
    }

    /* 完整数据已接收 */
    if (server.child_info_nread == wlen) {
        *information_type = buffer.information_type;
        *cow = buffer.cow;
        *cow_updated = buffer.cow_updated;
        *keys = buffer.keys;
        *progress = buffer.progress;
        return 1;
    } else {
        return 0;
    }
}

/* 接收来自子进程的信息数据
 *
 * 在 beforeSleep() 中调用，耗尽管道并更新子进程信息，以获取最终消息 */
void receiveChildInfo(void) {
    if (server.child_info_pipe[0] == -1) return;

    size_t cow;
    monotime cow_updated;
    size_t keys;
    double progress;
    childInfoType information_type;

    /* 循环读取管道中的所有消息，确保获取最后一条信息 */
    while (readChildInfo(&information_type, &cow, &cow_updated, &keys, &progress)) {
        updateChildInfo(information_type, cow, cow_updated, keys, progress);
    }
}
