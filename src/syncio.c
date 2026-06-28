/* 同步 Socket 和文件 I/O 操作，供核心模块使用。
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"

/* ----------------- 带超时的阻塞式 Socket I/O --------------------- */

/* Redis 大部分 I/O 以非阻塞方式进行，但有两个例外：
 * 1) SYNC 命令中，从节点以阻塞方式执行；
 * 2) MIGRATE 命令必须阻塞，以保证从两个实例的角度来看
 *   （一个迁移键，一个接收键）操作是原子的。
 * 因此需要以下阻塞式 I/O 函数。
 *
 * 所有函数的超时参数单位均为毫秒。 */

#define SYNCIO__RESOLUTION 10 /* 精度：毫秒 */

/* 将指定数据写入文件描述符 fd。
 * 如果在 timeout 毫秒内写完所有数据，则操作成功并返回 size。
 * 否则操作失败，返回 -1，且可能发生部分写入。 */
ssize_t syncWrite(int fd, char *ptr, ssize_t size, long long timeout) {
    ssize_t nwritten, ret = size;       /* nwritten: 本次写入字节数 */
    long long start = mstime();         /* 记录起始时间 */
    long long remaining = timeout;      /* 剩余超时时间 */

    while(1) {
        /* 每次等待至少保证 SYNCIO__RESOLUTION 毫秒 */
        long long wait = (remaining > SYNCIO__RESOLUTION) ?
                          remaining : SYNCIO__RESOLUTION;
        long long elapsed;

        /* 先乐观尝试写入，不必先检查 fd 是否可写。
         * 最坏情况会得到 EAGAIN 错误。 */
        nwritten = write(fd,ptr,size);
        if (nwritten == -1) {
            if (errno != EAGAIN) return -1;
        } else {
            ptr += nwritten;
            size -= nwritten;
        }
        if (size == 0) return ret; /* 全部写完 */

        /* 等待 fd 变为可写 */
        aeWait(fd,AE_WRITABLE,wait);
        elapsed = mstime() - start;
        if (elapsed >= timeout) {
            errno = ETIMEDOUT;  /* 超时 */
            return -1;
        }
        remaining = timeout - elapsed;
    }
}

/* 从文件描述符 fd 读取指定字节数的数据。
 * 如果在 timeout 毫秒内读完所有数据，则操作成功并返回 size。
 * 否则操作失败，返回 -1，且可能已读取了部分数据。 */
ssize_t syncRead(int fd, char *ptr, ssize_t size, long long timeout) {
    ssize_t nread, totread = 0;  /* nread: 本次读取字节数 */
    long long start = mstime();  /* 记录起始时间 */
    long long remaining = timeout; /* 剩余超时时间 */

    if (size == 0) return 0;
    while(1) {
        long long wait = (remaining > SYNCIO__RESOLUTION) ?
                          remaining : SYNCIO__RESOLUTION;
        long long elapsed;

        /* 先乐观尝试读取，不必先检查 fd 是否可读。
         * 最坏情况会得到 EAGAIN 错误。 */
        nread = read(fd,ptr,size);
        if (nread == 0) return -1; /* 对端关闭连接（短读） */
        if (nread == -1) {
            if (errno != EAGAIN) return -1;
        } else {
            ptr += nread;
            size -= nread;
            totread += nread;
        }
        if (size == 0) return totread; /* 全部读完 */

        /* 等待 fd 变为可读 */
        aeWait(fd,AE_READABLE,wait);
        elapsed = mstime() - start;
        if (elapsed >= timeout) {
            errno = ETIMEDOUT;  /* 超时 */
            return -1;
        }
        remaining = timeout - elapsed;
    }
}

/* 从 fd 读取一行数据，保证每个字符的读取不超过 timeout 毫秒。
 *
 * 成功时返回读取的字节数，失败返回 -1。
 * 成功时字符串总是以 '\0' 正确结尾。 */
ssize_t syncReadLine(int fd, char *ptr, ssize_t size, long long timeout) {
    ssize_t nread = 0; /* 已读取字节数 */

    size--; /* 保留一个字节用于 '\0' 终止符 */
    while(size) {
        char c;

        /* 每次只读一个字符 */
        if (syncRead(fd,&c,1,timeout) == -1) return -1;
        if (c == '\n') {
            *ptr = '\0';
            /* 将行尾的 CR (\r) 替换为 '\0'（处理 CRLF 行尾） */
            if (nread && *(ptr-1) == '\r') *(ptr-1) = '\0';
            return nread;
        } else {
            *ptr++ = c;
            *ptr = '\0'; /* 始终保持字符串以 '\0' 结尾 */
            nread++;
        }
        size--;
    }
    return nread;
}
