/* zmalloc - 能跟踪总已分配内存的 malloc() 封装版本
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "fmacros.h"
#include "config.h"
#include "solarisfixes.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>

#ifdef __linux__
#include <sys/mman.h>
#endif

/* 此函数提供对原始 libc free() 的访问。这对于释放由
 * backtrace_symbols() 返回的结果很有用。我们需要在包含
 * zmalloc.h 之前定义此函数，因为 zmalloc.h 可能会在使用
 * jemalloc 或其他非标准分配器时隐藏原始的 free 实现。 */
void zlibc_free(void *ptr) {
    free(ptr);
}

#include <string.h>
#include "zmalloc.h"
#include "atomicvar.h"

#define UNUSED(x) ((void)(x))

#ifdef HAVE_MALLOC_SIZE
#define PREFIX_SIZE (0)
#else
/* 在所有系统上至少使用 8 字节对齐。 */
#if SIZE_MAX < 0xffffffffffffffffull
#define PREFIX_SIZE 8
#else
#define PREFIX_SIZE (sizeof(size_t))
#endif
#endif

/* 使用 libc 分配器时，使用最小分配大小以匹配
 * jemalloc 在此情况下不返回 NULL 的行为。
 */
#define MALLOC_MIN_SIZE(x) ((x) > 0 ? (x) : sizeof(long))

/* 使用 tcmalloc 时显式覆盖 malloc/free 等函数。 */
#if defined(USE_TCMALLOC)
#define malloc(size) tc_malloc(size)
#define calloc(count,size) tc_calloc(count,size)
#define realloc(ptr,size) tc_realloc(ptr,size)
#define free(ptr) tc_free(ptr)
/* 使用 jemalloc 时显式覆盖 malloc/free 等函数。 */
#elif defined(USE_JEMALLOC)
#define malloc(size) je_malloc(size)
#define calloc(count,size) je_calloc(count,size)
#define realloc(ptr,size) je_realloc(ptr,size)
#define free(ptr) je_free(ptr)
#define mallocx(size,flags) je_mallocx(size,flags)
#define rallocx(ptr,size,flags) je_rallocx(ptr,size,flags)
#define dallocx(ptr,flags) je_dallocx(ptr,flags)
#endif

#define update_zmalloc_stat_alloc(__n) atomicIncr(used_memory,(__n))
#define update_zmalloc_stat_free(__n) atomicDecr(used_memory,(__n))

static redisAtomic size_t used_memory = 0;

/* 默认的 OOM 处理函数：输出错误信息并终止进程 */
static void zmalloc_default_oom(size_t size) {
    fprintf(stderr, "zmalloc: Out of memory trying to allocate %zu bytes\n",
        size);
    fflush(stderr);
    abort();
}

static void (*zmalloc_oom_handler)(size_t) = zmalloc_default_oom;

#ifdef HAVE_MALLOC_SIZE
/* 当分配器支持 malloc_size 时，将分配扩展为可用。
 * 某些分配器可能返回比请求更大的内存块。 */
void *extend_to_usable(void *ptr, size_t size) {
    UNUSED(size);
    return ptr;
}
#endif

/* 尝试分配内存，失败时返回 NULL。
 * 如果 usable 非 NULL，则将其设为可用大小。 */
static inline void *ztrymalloc_usable_internal(size_t size, size_t *usable) {
    /* 可能溢出，返回 NULL，让调用方可以选择 panic 或处理分配失败。 */
    if (size >= SIZE_MAX/2) return NULL;
    void *ptr = malloc(MALLOC_MIN_SIZE(size)+PREFIX_SIZE);

    if (!ptr) return NULL;
#ifdef HAVE_MALLOC_SIZE
    size = zmalloc_size(ptr);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return ptr;
#else
    size = MALLOC_MIN_SIZE(size);
    *((size_t*)ptr) = size;
    update_zmalloc_stat_alloc(size+PREFIX_SIZE);
    if (usable) *usable = size;
    return (char*)ptr+PREFIX_SIZE;
#endif
}

/* 尝试分配内存并返回可用大小，失败时返回 NULL */
void *ztrymalloc_usable(size_t size, size_t *usable) {
    size_t usable_size = 0;
    void *ptr = ztrymalloc_usable_internal(size, &usable_size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* 分配内存，失败时触发 OOM 处理（panic） */
void *zmalloc(size_t size) {
    void *ptr = ztrymalloc_usable_internal(size, NULL);
    if (!ptr) zmalloc_oom_handler(size);
    return ptr;
}

/* 尝试分配内存，失败时返回 NULL。 */
void *ztrymalloc(size_t size) {
    void *ptr = ztrymalloc_usable_internal(size, NULL);
    return ptr;
}

/* 分配内存，失败时触发 OOM 处理。
 * 如果 usable 非 NULL，则将其设为可用大小。 */
void *zmalloc_usable(size_t size, size_t *usable) {
    size_t usable_size = 0;
    void *ptr = ztrymalloc_usable_internal(size, &usable_size);
    if (!ptr) zmalloc_oom_handler(size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

#if defined(USE_JEMALLOC)
/* 使用指定 flags 进行内存分配（仅 jemalloc） */
void *zmalloc_with_flags(size_t size, int flags) {
    if (size >= SIZE_MAX/2) zmalloc_oom_handler(size);
    void *ptr = mallocx(size+PREFIX_SIZE, flags);
    if (!ptr) zmalloc_oom_handler(size);
    update_zmalloc_stat_alloc(zmalloc_size(ptr));
    return ptr;
}

/* 使用指定 flags 重新分配内存（仅 jemalloc） */
void *zrealloc_with_flags(void *ptr, size_t size, int flags) {
    /* 大小为 0，不分配任何东西，重定向到 free。 */
    if (size == 0 && ptr != NULL) {
        zfree_with_flags(ptr, flags);
        return NULL;
    }

    /* 指针为 NULL，不释放任何东西，重定向到 malloc。 */
    if (ptr == NULL)
        return zmalloc_with_flags(size, flags);

    /* 可能溢出，返回 NULL，让调用方可以选择 panic 或处理分配失败。 */
    if (size >= SIZE_MAX/2) {
        zfree_with_flags(ptr, flags);
        zmalloc_oom_handler(size);
        return NULL;
    }

    size_t oldsize = zmalloc_size(ptr);
    void *newptr = rallocx(ptr, size, flags);
    if (newptr == NULL) {
        zmalloc_oom_handler(size);
        return NULL;
    }

    update_zmalloc_stat_free(oldsize);
    size = zmalloc_size(newptr);
    update_zmalloc_stat_alloc(size);
    return newptr;
}

/* 使用指定 flags 释放内存（仅 jemalloc） */
void zfree_with_flags(void *ptr, int flags) {
    if (ptr == NULL) return;
    update_zmalloc_stat_free(zmalloc_size(ptr));
    dallocx(ptr, flags);
}
#endif

/* 绕过线程缓存（thread cache）直接使用分配器 arena bins
 * 的分配和释放函数。目前仅针对 jemalloc 实现，
 * 用于在线碎片整理（online defragmentation）。 */
#ifdef HAVE_DEFRAG
/* 不使用线程缓存的内存分配（用于碎片整理） */
void *zmalloc_no_tcache(size_t size) {
    if (size >= SIZE_MAX/2) zmalloc_oom_handler(size);
    void *ptr = mallocx(size+PREFIX_SIZE, MALLOCX_TCACHE_NONE);
    if (!ptr) zmalloc_oom_handler(size);
    update_zmalloc_stat_alloc(zmalloc_size(ptr));
    return ptr;
}

/* 不使用线程缓存的内存释放（用于碎片整理） */
void zfree_no_tcache(void *ptr) {
    if (ptr == NULL) return;
    update_zmalloc_stat_free(zmalloc_size(ptr));
    dallocx(ptr, MALLOCX_TCACHE_NONE);
}
#endif

/* 尝试分配并清零内存，失败时返回 NULL。
 * 如果 usable 非 NULL，则将其设为可用大小。 */
static inline void *ztrycalloc_usable_internal(size_t size, size_t *usable) {
    /* 可能溢出，返回 NULL，让调用方可以选择 panic 或处理分配失败。 */
    if (size >= SIZE_MAX/2) return NULL;
    void *ptr = calloc(1, MALLOC_MIN_SIZE(size)+PREFIX_SIZE);
    if (ptr == NULL) return NULL;

#ifdef HAVE_MALLOC_SIZE
    size = zmalloc_size(ptr);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return ptr;
#else
    size = MALLOC_MIN_SIZE(size);
    *((size_t*)ptr) = size;
    update_zmalloc_stat_alloc(size+PREFIX_SIZE);
    if (usable) *usable = size;
    return (char*)ptr+PREFIX_SIZE;
#endif
}

/* 尝试分配并清零内存，返回可用大小，失败时返回 NULL */
void *ztrycalloc_usable(size_t size, size_t *usable) {
    size_t usable_size = 0;
    void *ptr = ztrycalloc_usable_internal(size, &usable_size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* 分配并清零内存，失败时触发 OOM 处理。
 * 需要此包装函数以提供与 calloc 兼容的签名 */
void *zcalloc_num(size_t num, size_t size) {
    /* 确保 calloc() 的参数相乘时不会溢出。
     * 除法运算可能产生除零错误，因此也需要检查。 */
    if ((size == 0) || (num > SIZE_MAX/size)) {
        zmalloc_oom_handler(SIZE_MAX);
        return NULL;
    }
    void *ptr = ztrycalloc_usable_internal(num*size, NULL);
    if (!ptr) zmalloc_oom_handler(num*size);
    return ptr;
}

/* 分配并清零内存，失败时触发 OOM 处理 */
void *zcalloc(size_t size) {
    void *ptr = ztrycalloc_usable_internal(size, NULL);
    if (!ptr) zmalloc_oom_handler(size);
    return ptr;
}

/* 尝试分配并清零内存，失败时返回 NULL。 */
void *ztrycalloc(size_t size) {
    void *ptr = ztrycalloc_usable_internal(size, NULL);
    return ptr;
}

/* 分配并清零内存，失败时触发 OOM 处理。
 * 如果 usable 非 NULL，则将其设为可用大小。 */
void *zcalloc_usable(size_t size, size_t *usable) {
    size_t usable_size = 0;
    void *ptr = ztrycalloc_usable_internal(size, &usable_size);
    if (!ptr) zmalloc_oom_handler(size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* 尝试重新分配内存，失败时返回 NULL。
 * 如果 usable 非 NULL，则将其设为可用大小。 */
static inline void *ztryrealloc_usable_internal(void *ptr, size_t size, size_t *usable) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
#endif
    size_t oldsize;
    void *newptr;

    /* 大小为 0，不分配任何东西，重定向到 free。 */
    if (size == 0 && ptr != NULL) {
        zfree(ptr);
        if (usable) *usable = 0;
        return NULL;
    }
    /* 指针为 NULL，不释放任何东西，重定向到 malloc。 */
    if (ptr == NULL)
        return ztrymalloc_usable(size, usable);

    /* 可能溢出，返回 NULL，让调用方可以选择 panic 或处理分配失败。 */
    if (size >= SIZE_MAX/2) {
        zfree(ptr);
        if (usable) *usable = 0;
        return NULL;
    }

#ifdef HAVE_MALLOC_SIZE
    oldsize = zmalloc_size(ptr);
    newptr = realloc(ptr,size);
    if (newptr == NULL) {
        if (usable) *usable = 0;
        return NULL;
    }

    update_zmalloc_stat_free(oldsize);
    size = zmalloc_size(newptr);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return newptr;
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realptr);
    newptr = realloc(realptr,size+PREFIX_SIZE);
    if (newptr == NULL) {
        if (usable) *usable = 0;
        return NULL;
    }

    *((size_t*)newptr) = size;
    update_zmalloc_stat_free(oldsize);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return (char*)newptr+PREFIX_SIZE;
#endif
}

/* 尝试重新分配内存并返回可用大小，失败时返回 NULL */
void *ztryrealloc_usable(void *ptr, size_t size, size_t *usable) {
    size_t usable_size = 0;
    ptr = ztryrealloc_usable_internal(ptr, size, &usable_size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* 重新分配内存，失败时触发 OOM 处理 */
void *zrealloc(void *ptr, size_t size) {
    ptr = ztryrealloc_usable_internal(ptr, size, NULL);
    if (!ptr && size != 0) zmalloc_oom_handler(size);
    return ptr;
}

/* 尝试重新分配内存，失败时返回 NULL。 */
void *ztryrealloc(void *ptr, size_t size) {
    ptr = ztryrealloc_usable_internal(ptr, size, NULL);
    return ptr;
}

/* 重新分配内存，失败时触发 OOM 处理。
 * 如果 usable 非 NULL，则将其设为可用大小。 */
void *zrealloc_usable(void *ptr, size_t size, size_t *usable) {
    size_t usable_size = 0;
    ptr = ztryrealloc_usable(ptr, size, &usable_size);
    if (!ptr && size != 0) zmalloc_oom_handler(size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* 为 malloc 本身不提供 zmalloc_size() 功能的系统提供实现，
 * 在这种情况下我们在每次分配的头部存储大小信息。 */
#ifndef HAVE_MALLOC_SIZE
size_t zmalloc_size(void *ptr) {
    void *realptr = (char*)ptr-PREFIX_SIZE;
    size_t size = *((size_t*)realptr);
    return size+PREFIX_SIZE;
}
size_t zmalloc_usable_size(void *ptr) {
    return zmalloc_size(ptr)-PREFIX_SIZE;
}
#endif

void zfree(void *ptr) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
    size_t oldsize;
#endif

    if (ptr == NULL) return;
#ifdef HAVE_MALLOC_SIZE
    update_zmalloc_stat_free(zmalloc_size(ptr));
    free(ptr);
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realptr);
    update_zmalloc_stat_free(oldsize+PREFIX_SIZE);
    free(realptr);
#endif
}

/* 类似于 zfree，额外将 *usable 设为被释放的可用大小。 */
void zfree_usable(void *ptr, size_t *usable) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
    size_t oldsize;
#endif

    if (ptr == NULL) return;
#ifdef HAVE_MALLOC_SIZE
    update_zmalloc_stat_free(*usable = zmalloc_size(ptr));
    free(ptr);
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    *usable = oldsize = *((size_t*)realptr);
    update_zmalloc_stat_free(oldsize+PREFIX_SIZE);
    free(realptr);
#endif
}

/* 复制字符串，使用 zmalloc 分配内存 */
char *zstrdup(const char *s) {
    size_t l = strlen(s)+1;
    char *p = zmalloc(l);

    memcpy(p,s,l);
    return p;
}

/* 获取已使用的内存总量 */
size_t zmalloc_used_memory(void) {
    size_t um;
    atomicGet(used_memory,um);
    return um;
}

/* 设置自定义的 OOM（内存不足）处理函数 */
void zmalloc_set_oom_handler(void (*oom_handler)(size_t)) {
    zmalloc_oom_handler = oom_handler;
}

/* 使用 'MADV_DONTNEED' 快速将内存归还给操作系统。
 * 我们在 fork 子进程中执行此操作，以避免父进程修改
 * 这些共享页面时触发写时复制（CoW）。 */
void zmadvise_dontneed(void *ptr) {
#if defined(USE_JEMALLOC) && defined(__linux__)
    static size_t page_size = 0;
    if (page_size == 0) page_size = sysconf(_SC_PAGESIZE);
    size_t page_size_mask = page_size - 1;

    size_t real_size = zmalloc_size(ptr);
    if (real_size < page_size) return;

    /* 我们需要按页面大小向上对齐指针，因为内存地址是
     * 向上增长的，且只能按页面粒度释放内存。 */
    char *aligned_ptr = (char *)(((size_t)ptr+page_size_mask) & ~page_size_mask);
    real_size -= (aligned_ptr-(char*)ptr);
    if (real_size >= page_size) {
        madvise((void *)aligned_ptr, real_size&~page_size_mask, MADV_DONTNEED);
    }
#else
    (void)(ptr);
#endif
}

/* 以操作系统特定的方式获取 RSS（常驻内存集）信息。
 *
 * 警告：zmalloc_get_rss() 函数不是为速度设计的，
 * 不应在 Redis 尝试通过过期或换出对象来释放内存的
 * 繁忙循环中调用。
 *
 * 对于需要"快速 RSS 报告"的场景，应使用
 * RedisEstimateRSS() 函数，它是更快速（但精度较低）
 * 的版本。 */

#if defined(HAVE_PROC_STAT)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#endif

/* 从 "/proc/self/stat" 中获取第 i 个字段，
 * 注意 i 从 1 开始，与 proc man 页面中的编号一致 */
int get_proc_stat_ll(int i, long long *res) {
#if defined(HAVE_PROC_STAT)
    char buf[4096];
    int fd, l;
    char *p, *x;

    if ((fd = open("/proc/self/stat",O_RDONLY)) == -1) return 0;
    if ((l = read(fd,buf,sizeof(buf)-1)) <= 0) {
        close(fd);
        return 0;
    }
    close(fd);
    buf[l] = '\0';
    if (buf[l-1] == '\n') buf[l-1] = '\0';

    /* 跳过 pid 和进程名（被括号包围的部分） */
    p = strrchr(buf, ')');
    if (!p) return 0;
    p++;
    while (*p == ' ') p++;
    if (*p == '\0') return 0;
    i -= 3;
    if (i < 0) return 0;

    while (p && i--) {
        p = strchr(p, ' ');
        if (p) p++;
        else return 0;
    }
    x = strchr(p,' ');
    if (x) *x = '\0';

    *res = strtoll(p,&x,10);
    if (*x != '\0') return 0;
    return 1;
#else
    UNUSED(i);
    UNUSED(res);
    return 0;
#endif
}

#if defined(HAVE_PROC_STAT)
size_t zmalloc_get_rss(void) {
    int page = sysconf(_SC_PAGESIZE);
    long long rss;

    /* RSS 是 /proc/<pid>/stat 中的第 24 个字段 */
    if (!get_proc_stat_ll(24, &rss)) return 0;
    rss *= page;
    return rss;
}
#elif defined(HAVE_TASKINFO)
#include <sys/types.h>
#include <sys/sysctl.h>
#include <mach/task.h>
#include <mach/mach_init.h>

size_t zmalloc_get_rss(void) {
    task_t task = MACH_PORT_NULL;
    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

    if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS)
        return 0;
    task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);

    return t_info.resident_size;
}
#elif defined(__FreeBSD__) || defined(__DragonFly__)
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/user.h>

size_t zmalloc_get_rss(void) {
    struct kinfo_proc info;
    size_t infolen = sizeof(info);
    int mib[4];
    mib[0] = CTL_KERN;
    mib[1] = KERN_PROC;
    mib[2] = KERN_PROC_PID;
    mib[3] = getpid();

    if (sysctl(mib, 4, &info, &infolen, NULL, 0) == 0)
#if defined(__FreeBSD__)
        return (size_t)info.ki_rssize * getpagesize();
#else
        return (size_t)info.kp_vm_rssize * getpagesize();
#endif

    return 0L;
}
#elif defined(__NetBSD__) || defined(__OpenBSD__)
#include <sys/types.h>
#include <sys/sysctl.h>

#if defined(__OpenBSD__)
#define kinfo_proc2 kinfo_proc
#define KERN_PROC2 KERN_PROC
#define __arraycount(a) (sizeof(a) / sizeof(a[0]))
#endif

size_t zmalloc_get_rss(void) {
    struct kinfo_proc2 info;
    size_t infolen = sizeof(info);
    int mib[6];
    mib[0] = CTL_KERN;
    mib[1] = KERN_PROC2;
    mib[2] = KERN_PROC_PID;
    mib[3] = getpid();
    mib[4] = sizeof(info);
    mib[5] = 1;
    if (sysctl(mib, __arraycount(mib), &info, &infolen, NULL, 0) == 0)
        return (size_t)info.p_vm_rssize * getpagesize();

    return 0L;
}
#elif defined(__HAIKU__)
#include <OS.h>

size_t zmalloc_get_rss(void) {
    area_info info;
    thread_info th;
    size_t rss = 0;
    ssize_t cookie = 0;

    if (get_thread_info(find_thread(0), &th) != B_OK)
        return 0;

    while (get_next_area_info(th.team, &cookie, &info) == B_OK)
        rss += info.ram_size;

    return rss;
}
#elif defined(HAVE_PSINFO)
#include <unistd.h>
#include <sys/procfs.h>
#include <fcntl.h>

size_t zmalloc_get_rss(void) {
    struct prpsinfo info;
    char filename[256];
    int fd;

    snprintf(filename,256,"/proc/%ld/psinfo",(long) getpid());

    if ((fd = open(filename,O_RDONLY)) == -1) return 0;
    if (ioctl(fd, PIOCPSINFO, &info) == -1) {
        close(fd);
        return 0;
    }

    close(fd);
    return info.pr_rssize;
}
#else
size_t zmalloc_get_rss(void) {
    /* 如果无法通过操作系统特定方式获取此系统的 RSS，
     * 则直接返回 zmalloc() 估算的内存使用量。
     *
     * 碎片率将始终显示为 1（无碎片）
     * 当然这只是近似值... */
    return zmalloc_used_memory();
}
#endif

#if defined(USE_JEMALLOC)

#include "redisassert.h"

/* 计算 small arena bins 内部碎片浪费的总内存。
 * 通过对所有 small bins 的 slabs 中未使用的 regs
 * 内存求和来完成。
 *
 * 传入 arena 参数可获取指定 arena 的信息，
 * 传入 MALLCTL_ARENAS_ALL 可获取所有 arena 的信息。 */
size_t zmalloc_get_frag_smallbins_by_arena(unsigned int arena) {
    unsigned nbins;
    size_t sz, frag = 0;
    char buf[100];

    sz = sizeof(unsigned);
    assert(!je_mallctl("arenas.nbins", &nbins, &sz, NULL, 0));
    for (unsigned j = 0; j < nbins; j++) {
        size_t curregs, curslabs, reg_size;
        uint32_t nregs;

        /* 当前 bin 的大小 */
        snprintf(buf, sizeof(buf), "arenas.bin.%u.size", j);
        sz = sizeof(size_t);
        assert(!je_mallctl(buf, &reg_size, &sz, NULL, 0));

        /* bin 中已使用的 region 数量 */
        snprintf(buf, sizeof(buf), "stats.arenas.%u.bins.%u.curregs", arena, j);
        sz = sizeof(size_t);
        assert(!je_mallctl(buf, &curregs, &sz, NULL, 0));

        /* 每个 slab 的 region 数量 */
        snprintf(buf, sizeof(buf), "arenas.bin.%u.nregs", j);
        sz = sizeof(uint32_t);
        assert(!je_mallctl(buf, &nregs, &sz, NULL, 0));

        /* bin 中当前 slab 的数量 */
        snprintf(buf, sizeof(buf), "stats.arenas.%u.bins.%u.curslabs", arena, j);
        sz = sizeof(size_t);
        assert(!je_mallctl(buf, &curslabs, &sz, NULL, 0));

        /* 计算当前 bin 的碎片字节数并累加到总数中。 */
        frag += ((nregs * curslabs) - curregs) * reg_size;
    }

    return frag;
}

/* 计算所有 small arena bins 内部碎片浪费的总内存。
 * 通过对所有 small bins 的 slabs 中未使用的 regs
 * 内存求和来完成。 */
size_t zmalloc_get_frag_smallbins(void) {
    return zmalloc_get_frag_smallbins_by_arena(MALLCTL_ARENAS_ALL);
}

/* 从分配器获取内存分配信息。
 *
 * refresh_stats 指示是否刷新缓存的统计信息。
 * 其他参数的含义请参考函数实现和 redis-doc 中
 * INFO 的 allocator_* 部分。 */
int zmalloc_get_allocator_info(int refresh_stats, size_t *allocated, size_t *active, size_t *resident,
                               size_t *retained, size_t *muzzy, size_t *frag_smallbins_bytes)
{
    size_t sz;
    *allocated = *resident = *active = 0;

    /* 更新 mallctl 缓存的统计数据。 */
    if (refresh_stats) {
        uint64_t epoch = 1;
        sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
    }

    sz = sizeof(size_t);
    /* 与 RSS 不同，此处不包含共享库和其他非堆映射的 RSS。 */
    je_mallctl("stats.resident", resident, &sz, NULL, 0);
    /* 与 resident 不同，此处不包含 jemalloc 预留待重用的
     * 页面（purge 操作会清理这些页面）。 */
    je_mallctl("stats.active", active, &sz, NULL, 0);
    /* 与 zmalloc_used_memory 不同，此值通过计入进程的
     * 所有分配（不仅仅是 zmalloc）来匹配 stats.resident。 */
    je_mallctl("stats.allocated", allocated, &sz, NULL, 0);

    /* Retained 内存是通过 `madvised(..., MADV_DONTNEED)` 释放的内存，
     * 不属于 RSS 或映射内存，与操作系统中的物理内存没有强关联。
     * 它仍然是 VM-Size 的一部分，可能在后续分配中被再次使用。 */
    if (retained) {
        *retained = 0;
        je_mallctl("stats.retained", retained, &sz, NULL, 0);
    }

    /* 与 retained 不同，Muzzy 表示通过 `madvised(..., MADV_FREE)`
     * 释放的内存。这些页面在操作系统决定重用之前
     * 仍会显示为进程的 RSS。 */
    if (muzzy) {
        char buf[100];
        size_t pmuzzy, page;
        snprintf(buf, sizeof(buf), "stats.arenas.%u.pmuzzy", MALLCTL_ARENAS_ALL);
        assert(!je_mallctl(buf, &pmuzzy, &sz, NULL, 0));
        assert(!je_mallctl("arenas.page", &page, &sz, NULL, 0));
        *muzzy = pmuzzy * page;
    }

    /* small bins 中未使用 regs 的已消耗内存总量（即外部碎片）。 */
    *frag_smallbins_bytes = zmalloc_get_frag_smallbins();
    return 1;
}

/* 从分配器获取指定 arena 的内存分配信息。
 *
 * refresh_stats 指示是否刷新缓存的统计信息。
 * 其他参数的含义请参考函数实现和 redis-doc 中
 * INFO 的 allocator_* 部分。 */
int zmalloc_get_allocator_info_by_arena(unsigned int arena, int refresh_stats, size_t *allocated,
                                        size_t *active, size_t *resident, size_t *frag_smallbins_bytes)
{
    char buf[100];
    size_t sz;
    *allocated = *resident = *active = 0;

    /* 更新 mallctl 缓存的统计数据。 */
    if (refresh_stats) {
        uint64_t epoch = 1;
        sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
    }

    sz = sizeof(size_t);
    /* 与 RSS 不同，此处不包含共享库和其他非堆映射的 RSS。 */
    snprintf(buf, sizeof(buf), "stats.arenas.%u.small.resident", arena);
    je_mallctl(buf, resident, &sz, NULL, 0);
    /* 与 resident 不同，此处不包含 jemalloc 预留待重用的
     * 页面（purge 操作会清理这些页面）。 */
    size_t pactive, page;
    snprintf(buf, sizeof(buf), "stats.arenas.%u.pactive", arena);
    assert(!je_mallctl(buf, &pactive, &sz, NULL, 0));
    assert(!je_mallctl("arenas.page", &page, &sz, NULL, 0));
    *active = pactive * page;
    /* 与 zmalloc_used_memory 不同，此值通过计入进程的
     * 所有分配（不仅仅是 zmalloc）来匹配 stats.resident。 */
    size_t small_allcated, large_allacted;
    snprintf(buf, sizeof(buf), "stats.arenas.%u.small.allocated", arena);
    assert(!je_mallctl(buf, &small_allcated, &sz, NULL, 0));
    *allocated += small_allcated;
    snprintf(buf, sizeof(buf), "stats.arenas.%u.large.allocated", arena);
    assert(!je_mallctl(buf, &large_allacted, &sz, NULL, 0));
    *allocated += large_allacted;

    /* small bins 中未使用 regs 的已消耗内存总量（即外部碎片）。 */
    *frag_smallbins_bytes = zmalloc_get_frag_smallbins_by_arena(arena);
    return 1;
}


void set_jemalloc_bg_thread(int enable) {
    /* 让 jemalloc 异步执行清理（purging）操作，
     * 这在 flushdb 之后没有流量时是必需的 */
    char val = !!enable;
    je_mallctl("background_thread", NULL, 0, &val, 1);
}

int jemalloc_purge(void) {
    /* 将所有未使用的（已预留的）页面归还给操作系统 */
    char tmp[32];
    unsigned narenas = 0;
    size_t sz = sizeof(unsigned);
    if (!je_mallctl("arenas.narenas", &narenas, &sz, NULL, 0)) {
        snprintf(tmp, sizeof(tmp), "arena.%u.purge", narenas);
        if (!je_mallctl(tmp, NULL, 0, NULL, 0))
            return 0;
    }
    return -1;
}

#else

int zmalloc_get_allocator_info(int refresh_stats, size_t *allocated, size_t *active, size_t *resident,
                               size_t *retained, size_t *muzzy, size_t *frag_smallbins_bytes)
{
    UNUSED(refresh_stats);
    *allocated = *resident = *active = *frag_smallbins_bytes = 0;
    if (retained) *retained = 0;
    if (muzzy) *muzzy = 0;
    return 1;
}

int zmalloc_get_allocator_info_by_arena(unsigned int arena, int refresh_stats, size_t *allocated,
                                        size_t *active, size_t *resident, size_t *frag_smallbins_bytes)
{
    UNUSED(arena);
    UNUSED(refresh_stats);
    *allocated = *resident = *active = *frag_smallbins_bytes = 0;
    return 1;
}


void set_jemalloc_bg_thread(int enable) {
    ((void)(enable));
}

int jemalloc_purge(void) {
    return 0;
}

#endif

#if defined(__APPLE__)
/* 用于 zmalloc_get_smap_bytes_by_field() 中的 proc_pidinfo()。
 * 注意此头文件不能包含在 zmalloc.h 中，因为它包含了一个
 * Darwin queue.h 文件，其中定义了与 Redis 用户代码冲突的
 * "LIST_HEAD" 宏。 */
#include <libproc.h>
#endif

/* 获取 /proc/self/smaps 中指定字段的总和（从 kb 转换为字节）。
 * 字段名必须带尾部 ":"，与 smaps 输出中的格式一致。
 *
 * 如果指定了 pid，则提取该 pid 的信息；
 * 如果 pid 为 -1，则报告当前进程的信息。
 *
 * 示例：zmalloc_get_smap_bytes_by_field("Rss:",-1);
 */
#if defined(HAVE_PROC_SMAPS)
size_t zmalloc_get_smap_bytes_by_field(char *field, long pid) {
    char line[1024];
    size_t bytes = 0;
    int flen = strlen(field);
    FILE *fp;

    if (pid == -1) {
        fp = fopen("/proc/self/smaps","r");
    } else {
        char filename[128];
        snprintf(filename,sizeof(filename),"/proc/%ld/smaps",pid);
        fp = fopen(filename,"r");
    }

    if (!fp) return 0;
    while(fgets(line,sizeof(line),fp) != NULL) {
        if (strncmp(line,field,flen) == 0) {
            char *p = strchr(line,'k');
            if (p) {
                *p = '\0';
                bytes += strtol(line+flen,NULL,10) * 1024;
            }
        }
    }
    fclose(fp);
    return bytes;
}
#else
/* 通过 libproc API 调用获取指定字段的总和。
 * 由于返回的是每页的值，需要相应地进行转换。
 *
 * 注意 AnonHugePages 在此平台上不操作，
 * 因为 THP（透明大页）功能不受支持
 */
size_t zmalloc_get_smap_bytes_by_field(char *field, long pid) {
#if defined(__APPLE__)
    struct proc_regioninfo pri;
    if (pid == -1) pid = getpid();
    if (proc_pidinfo(pid, PROC_PIDREGIONINFO, 0, &pri,
                     PROC_PIDREGIONINFO_SIZE) == PROC_PIDREGIONINFO_SIZE)
    {
        int pagesize = getpagesize();
        if (!strcmp(field, "Private_Dirty:")) {
            return (size_t)pri.pri_pages_dirtied * pagesize;
        } else if (!strcmp(field, "Rss:")) {
            return (size_t)pri.pri_pages_resident * pagesize;
        } else if (!strcmp(field, "AnonHugePages:")) {
            return 0;
        }
    }
    return 0;
#endif
    ((void) field);
    ((void) pid);
    return 0;
}
#endif

/* 返回标记为 Private Dirty 的页面总字节数。
 *
 * 注意：根据平台和进程的内存占用情况，
 * 此调用可能很慢，超过 1000ms！
 */
size_t zmalloc_get_private_dirty(long pid) {
    return zmalloc_get_smap_bytes_by_field("Private_Dirty:",pid);
}

/* 返回物理内存（RAM）的大小，以字节为单位。
 * 虽然看起来不太优雅，但这是实现跨平台获取
 * 物理内存大小最简洁的方式。整理自：
 *
 * http://nadeausoftware.com/articles/2012/09/c_c_tip_how_get_physical_memory_size_system
 *
 * 注意此函数：
 * 1) 在以下 CC 署名许可下发布：
 *    http://creativecommons.org/licenses/by/3.0/deed.en_US
 * 2) 最初由 David Robert Nadeau 实现。
 * 3) 由 Matt Stancliff 为 Redis 进行了修改。
 * 4) 此注释是为了遵守原始许可要求。
 */
size_t zmalloc_get_memory_size(void) {
#if defined(__unix__) || defined(__unix) || defined(unix) || \
    (defined(__APPLE__) && defined(__MACH__))
#if defined(CTL_HW) && (defined(HW_MEMSIZE) || defined(HW_PHYSMEM64))
    int mib[2];
    mib[0] = CTL_HW;
#if defined(HW_MEMSIZE)
    mib[1] = HW_MEMSIZE;            /* OSX 系统。 ----------------- */
#elif defined(HW_PHYSMEM64)
    mib[1] = HW_PHYSMEM64;          /* NetBSD, OpenBSD 系统。 --- */
#endif
    int64_t size = 0;               /* 64 位 */
    size_t len = sizeof(size);
    if (sysctl( mib, 2, &size, &len, NULL, 0) == 0)
        return (size_t)size;
    return 0L;          /* 失败 */

#elif defined(_SC_PHYS_PAGES) && defined(_SC_PAGESIZE)
    /* FreeBSD, Linux, OpenBSD 和 Solaris 系统。 --------------- */
    return (size_t)sysconf(_SC_PHYS_PAGES) * (size_t)sysconf(_SC_PAGESIZE);

#elif defined(CTL_HW) && (defined(HW_PHYSMEM) || defined(HW_REALMEM))
    /* DragonFly BSD, FreeBSD, NetBSD, OpenBSD 和 OSX 系统。 --- */
    int mib[2];
    mib[0] = CTL_HW;
#if defined(HW_REALMEM)
    mib[1] = HW_REALMEM;        /* FreeBSD 系统。 ------------- */
#elif defined(HW_PHYSMEM)
    mib[1] = HW_PHYSMEM;        /* 其他系统。 ----------------- */
#endif
    unsigned int size = 0;      /* 32 位 */
    size_t len = sizeof(size);
    if (sysctl(mib, 2, &size, &len, NULL, 0) == 0)
        return (size_t)size;
    return 0L;          /* 失败 */
#else
    return 0L;          /* 获取数据的方法未知。 */
#endif
#else
    return 0L;          /* 未知操作系统。 */
#endif
}

#ifdef REDIS_TEST
#include "testhelp.h"
#include "redisassert.h"

#define TEST(name) printf("test — %s\n", name);

int zmalloc_test(int argc, char **argv, int flags) {
    void *ptr, *ptr2;

    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    printf("Malloc prefix size: %d\n", (int) PREFIX_SIZE);

    TEST("Initial used memory is 0") {
        assert(zmalloc_used_memory() == 0);
    }

    TEST("Allocated 123 bytes") {
        ptr = zmalloc(123);
        printf("Allocated 123 bytes; used: %zu\n", zmalloc_used_memory());
    }

    TEST("Reallocated to 456 bytes") {
        ptr = zrealloc(ptr, 456);
        printf("Reallocated to 456 bytes; used: %zu\n", zmalloc_used_memory());
    }

    TEST("Callocated 123 bytes") {
        ptr2 = zcalloc(123);
        printf("Callocated 123 bytes; used: %zu\n", zmalloc_used_memory());
    }

    TEST("Freed pointers") {
        zfree(ptr);
        zfree(ptr2);
        printf("Freed pointers; used: %zu\n", zmalloc_used_memory());
    }

    TEST("Allocated 0 bytes") {
        ptr = zmalloc(0);
        printf("Allocated 0 bytes; used: %zu\n", zmalloc_used_memory());
        zfree(ptr);
    }

    TEST("At the end used memory is 0") {
        assert(zmalloc_used_memory() == 0);
    }

    return 0;
}
#endif
