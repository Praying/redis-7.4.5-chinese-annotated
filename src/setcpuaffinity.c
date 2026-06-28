/* ==========================================================================
 * setcpuaffinity.c - CPU 亲和性设置工具
 * --------------------------------------------------------------------------
 * 功能: 设置进程/线程运行的 CPU 核心,实现 CPU 绑定的功能
 * 支持: Linux、FreeBSD、DragonFly、NetBSD
 *
 * 版权:
 *   Copyright (C) 2020  zhenwei pi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the
 * following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 * USE OR OTHER DEALINGS IN THE SOFTWARE.
 * ==========================================================================
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#ifdef __linux__
#include <sched.h>
#endif
#ifdef __FreeBSD__
#include <sys/param.h>
#include <sys/cpuset.h>
#endif
#ifdef __DragonFly__
#include <pthread.h>
#include <pthread_np.h>
#endif
#ifdef __NetBSD__
#include <pthread.h>
#include <sched.h>
#endif
#include "config.h"

#ifdef USE_SETCPUAFFINITY
static const char *next_token(const char *q,  int sep) {
    if (q)
        q = strchr(q, sep);
    if (q)
        q++;

    return q;
}

static int next_num(const char *str, char **end, int *result) {
    if (!str || *str == '\0' || !isdigit(*str))
        return -1;

    *result = strtoul(str, end, 10);
    if (str == *end)
        return -1;

    return 0;
}

/* setcpuaffinity - 设置当前线程的 CPU 亲和性
 *
 * 将当前线程绑定到指定的 CPU 核心集合.
 * 功能类似于 Linux 的 taskset 命令.
 *
 * cpulist 格式说明:
 *   "0,2,3"   - 逗号分隔: 使用 CPU 0, 2, 3
 *   "0,2-3"   - 连字符范围: 使用 CPU 0, 2, 3
 *   "0-20:2"  - 冒号步进: 使用 CPU 0, 2, 4, 6, ... 20
 *
 * 解析逻辑参考 util-linux 的 taskset 命令实现.
 *
 * 参数:
 *   cpulist - CPU 列表字符串,如 "0,2,3" 或 "0-3"
 */
void setcpuaffinity(const char *cpulist) {
    const char *p, *q;
    char *end = NULL;
#ifdef __linux__
    cpu_set_t cpuset;
#endif
#if defined (__FreeBSD__) || defined(__DragonFly__)
    cpuset_t cpuset;
#endif
#ifdef __NetBSD__
    cpuset_t *cpuset;
#endif

    if (!cpulist)
        return;

#ifndef __NetBSD__
    CPU_ZERO(&cpuset);
#else
    cpuset = cpuset_create();
#endif

    q = cpulist;
    while (p = q, q = next_token(q, ','), p) {
        int a, b, s;
        const char *c1, *c2;

        if (next_num(p, &end, &a) != 0)
            return;

        b = a;
        s = 1;
        p = end;

        c1 = next_token(p, '-');
        c2 = next_token(p, ',');

        if (c1 != NULL && (c2 == NULL || c1 < c2)) {
            if (next_num(c1, &end, &b) != 0)
                return;

            c1 = end && *end ? next_token(end, ':') : NULL;
            if (c1 != NULL && (c2 == NULL || c1 < c2)) {
                if (next_num(c1, &end, &s) != 0)
                    return;

                if (s == 0)
                    return;
            }
        }

        if ((a > b))
            return;

        while (a <= b) {
#ifndef __NetBSD__
            CPU_SET(a, &cpuset);
#else
            cpuset_set(a, cpuset);
#endif
            a += s;
        }
    }

    if (end && *end)
        return;

#ifdef __linux__
    sched_setaffinity(0, sizeof(cpuset), &cpuset);
#endif
#ifdef __FreeBSD__
    cpuset_setaffinity(CPU_LEVEL_WHICH, CPU_WHICH_TID, -1, sizeof(cpuset), &cpuset);
#endif
#ifdef __DragonFly__
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
#endif
#ifdef __NetBSD__
    pthread_setaffinity_np(pthread_self(), cpuset_size(cpuset), cpuset);
    cpuset_destroy(cpuset);
#endif
}

#endif /* USE_SETCPUAFFINITY */
