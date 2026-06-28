/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <termios.h>
#include <sys/ioctl.h>
#if defined(__sun)
#include <stropts.h>
#endif
#include "config.h"
#include "redisassert.h"

#if (ULONG_MAX == 4294967295UL)
#define MEMTEST_32BIT
#elif (ULONG_MAX == 18446744073709551615ULL)
#define MEMTEST_64BIT
#else
#error "ULONG_MAX value not supported."
#endif

#ifdef MEMTEST_32BIT
#define ULONG_ONEZERO 0xaaaaaaaaUL
#define ULONG_ZEROONE 0x55555555UL
#else
#define ULONG_ONEZERO 0xaaaaaaaaaaaaaaaaUL
#define ULONG_ZEROONE 0x5555555555555555UL
#endif

#if defined(__has_attribute)
#if __has_attribute(no_sanitize)
#define NO_SANITIZE(sanitizer) __attribute__((no_sanitize(sanitizer)))
#endif
#endif

#if !defined(NO_SANITIZE)
#define NO_SANITIZE(sanitizer)
#endif

static struct winsize ws;
size_t progress_printed; /* Printed chars in screen-wide progress bar. */
size_t progress_full; /* How many chars to write to fill the progress bar. */

void memtest_progress_start(char *title, int pass) {
    int j;

    printf("\x1b[H\x1b[2J");    /* Cursor home, clear screen. */
    /* Fill with dots. */
    for (j = 0; j < ws.ws_col*(ws.ws_row-2); j++) printf(".");
    printf("Please keep the test running several minutes per GB of memory.\n");
    printf("Also check http://www.memtest86.com/ and http://pyropus.ca/software/memtester/");
    printf("\x1b[H\x1b[2K");          /* Cursor home, clear current line.  */
    printf("%s [%d]\n", title, pass); /* Print title. */
    progress_printed = 0;
    progress_full = (size_t)ws.ws_col*(ws.ws_row-3);
    fflush(stdout);
}

void memtest_progress_end(void) {
    printf("\x1b[H\x1b[2J");    /* Cursor home, clear screen. */
}

void memtest_progress_step(size_t curr, size_t size, char c) {
    size_t chars = ((unsigned long long)curr*progress_full)/size, j;

    for (j = 0; j < chars-progress_printed; j++) printf("%c",c);
    progress_printed = chars;
    fflush(stdout);
}

/* memtest_addressing - 地址寻址测试
 *
 * 测试内存寻址是否正确.
 * 每个内存位置写入其自身的地址值,然后验证.
 *
 * 此测试非常快速,但能尽早检测出内存子系统的重大问题.
 *
 * 参数:
 *   l         - 内存缓冲区指针
 *   bytes     - 测试字节数
 *   interactive - 是否交互式显示进度
 *
 * 返回:
 *   0  无错误
 *   1  发现内存错误
 */
int memtest_addressing(unsigned long *l, size_t bytes, int interactive) {
    unsigned long words = bytes/sizeof(unsigned long);
    unsigned long j, *p;

    /* Fill */
    p = l;
    for (j = 0; j < words; j++) {
        *p = (unsigned long)p;
        p++;
        if ((j & 0xffff) == 0 && interactive)
            memtest_progress_step(j,words*2,'A');
    }
    /* Test */
    p = l;
    for (j = 0; j < words; j++) {
        if (*p != (unsigned long)p) {
            if (interactive) {
                printf("\n*** MEMORY ADDRESSING ERROR: %p contains %lu\n",
                    (void*) p, *p);
                exit(1);
            }
            return 1;
        }
        p++;
        if ((j & 0xffff) == 0 && interactive)
            memtest_progress_step(j+words,words*2,'A');
    }
    return 0;
}

/* memtest_fill_random - 随机填充测试
 *
 * 以页为单位跳跃写入,确保在最短时间内遍历所有内存页.
 * 这种模式减少了缓存的影响,使操作系统难以将页面换出到 swap.
 *
 * 注意: 此测试不能调用 rand(),因为系统可能无法处理库调用.
 * 因此使用自己的 PRNG(伪随机数生成器),这里采用 xorshift* 算法.
 *
 * xorshift64* 是一个高性能的 64 位伪随机数生成器,
 * 具有良好的统计性能和很长的周期.
 */
#define xorshift64star_next() do { \
        rseed ^= rseed >> 12; \
        rseed ^= rseed << 25; \
        rseed ^= rseed >> 27; \
        rout = rseed * UINT64_C(2685821657736338717); \
} while(0)

void memtest_fill_random(unsigned long *l, size_t bytes, int interactive) {
    unsigned long step = 4096/sizeof(unsigned long);
    unsigned long words = bytes/sizeof(unsigned long)/2;
    unsigned long iwords = words/step;  /* words per iteration */
    unsigned long off, w, *l1, *l2;
    uint64_t rseed = UINT64_C(0xd13133de9afdb566); /* Just a random seed. */
    uint64_t rout = 0;

    assert((bytes & 4095) == 0);
    for (off = 0; off < step; off++) {
        l1 = l+off;
        l2 = l1+words;
        for (w = 0; w < iwords; w++) {
            xorshift64star_next();
            *l1 = *l2 = (unsigned long) rout;
            l1 += step;
            l2 += step;
            if ((w & 0xffff) == 0 && interactive)
                memtest_progress_step(w+iwords*off,words,'R');
        }
    }
}

/* memtest_fill_value - 双值交替填充测试
 *
 * 类似 memtest_fill_random(), 但使用两个指定的值交替填充内存.
 * 填充模式: v1|v2|v1|v2|...
 *
 * 这种模式可以检测位翻转(bit flip)和相邻位之间的干扰.
 *
 * 参数:
 *   l         - 内存缓冲区指针
 *   bytes    - 测试字节数
 *   v1, v2   - 两个交替使用的填充值
 *   sym      - 进度显示符号
 *   interactive - 是否交互式显示进度
 */
void memtest_fill_value(unsigned long *l, size_t bytes, unsigned long v1,
                        unsigned long v2, char sym, int interactive)
{
    unsigned long step = 4096/sizeof(unsigned long);
    unsigned long words = bytes/sizeof(unsigned long)/2;
    unsigned long iwords = words/step;  /* words per iteration */
    unsigned long off, w, *l1, *l2, v;

    assert((bytes & 4095) == 0);
    for (off = 0; off < step; off++) {
        l1 = l+off;
        l2 = l1+words;
        v = (off & 1) ? v2 : v1;
        for (w = 0; w < iwords; w++) {
#ifdef MEMTEST_32BIT
            *l1 = *l2 = ((unsigned long)     v) |
                        (((unsigned long)    v) << 16);
#else
            *l1 = *l2 = ((unsigned long)     v) |
                        (((unsigned long)    v) << 16) |
                        (((unsigned long)    v) << 32) |
                        (((unsigned long)    v) << 48);
#endif
            l1 += step;
            l2 += step;
            if ((w & 0xffff) == 0 && interactive)
                memtest_progress_step(w+iwords*off,words,sym);
        }
    }
}

int memtest_compare(unsigned long *l, size_t bytes, int interactive) {
    unsigned long words = bytes/sizeof(unsigned long)/2;
    unsigned long w, *l1, *l2;

    assert((bytes & 4095) == 0);
    l1 = l;
    l2 = l1+words;
    for (w = 0; w < words; w++) {
        if (*l1 != *l2) {
            if (interactive) {
                printf("\n*** MEMORY ERROR DETECTED: %p != %p (%lu vs %lu)\n",
                    (void*)l1, (void*)l2, *l1, *l2);
                exit(1);
            }
            return 1;
        }
        l1 ++;
        l2 ++;
        if ((w & 0xffff) == 0 && interactive)
            memtest_progress_step(w,words,'=');
    }
    return 0;
}

int memtest_compare_times(unsigned long *m, size_t bytes, int pass, int times,
                          int interactive)
{
    int j;
    int errors = 0;

    for (j = 0; j < times; j++) {
        if (interactive) memtest_progress_start("Compare",pass);
        errors += memtest_compare(m,bytes,interactive);
        if (interactive) memtest_progress_end();
    }
    return errors;
}

/* memtest_test - 综合内存测试
 *
 * 对指定内存区域进行全面的内存测试.
 * 字节数必须是 4096 的倍数.
 *
 * 测试流程(每轮 pass):
 *   1. 地址寻址测试 (memtest_addressing)
 *   2. 随机填充测试 (memtest_fill_random) + 4次比较
 *   3. 全1填充测试 (memtest_fill_value with 0 and -1) + 4次比较
 *   4. 棋盘格填充测试 (memtest_fill_value with ULONG_ONEZERO and ULONG_ZEROONE) + 4次比较
 *
 * 参数:
 *   m         - 内存缓冲区指针
 *   bytes     - 测试字节数(必须为 4096 的倍数)
 *   passes    - 测试轮数
 *   interactive - true: 交互式显示进度; false: API调用模式
 *
 * 返回:
 *   0  无错误
 *   1  发现内存错误
 */
int memtest_test(unsigned long *m, size_t bytes, int passes, int interactive) {
    int pass = 0;
    int errors = 0;

    while (pass != passes) {
        pass++;

        if (interactive) memtest_progress_start("Addressing test",pass);
        errors += memtest_addressing(m,bytes,interactive);
        if (interactive) memtest_progress_end();

        if (interactive) memtest_progress_start("Random fill",pass);
        memtest_fill_random(m,bytes,interactive);
        if (interactive) memtest_progress_end();
        errors += memtest_compare_times(m,bytes,pass,4,interactive);

        if (interactive) memtest_progress_start("Solid fill",pass);
        memtest_fill_value(m,bytes,0,(unsigned long)-1,'S',interactive);
        if (interactive) memtest_progress_end();
        errors += memtest_compare_times(m,bytes,pass,4,interactive);

        if (interactive) memtest_progress_start("Checkerboard fill",pass);
        memtest_fill_value(m,bytes,ULONG_ONEZERO,ULONG_ZEROONE,'C',interactive);
        if (interactive) memtest_progress_end();
        errors += memtest_compare_times(m,bytes,pass,4,interactive);
    }
    return errors;
}

/* memtest_preserving_test - 保留内存内容的测试版本
 *
 * 此函数分小块测试内存,以便在退出时恢复内存内容.
 *
 * 问题:
 *   1. CPU 缓存可能导致测试不能真正访问内存
 *   2. 无法同时测试大块内存,因为需要将数据备份到栈上
 *      (分配器可能不可用或已处于内存不足状态)
 *
 * 解决方案:
 *   在填充和比较周期之间,通过无用的内存访问"刷掉"缓存,
 *   强制真正的内存访问.
 *
 * 测试以 1024*1024/sizeof(long) 个 unsigned long 为单位进行备份.
 */
#define MEMTEST_BACKUP_WORDS (1024*(1024/sizeof(long)))
/* 在填充和比较周期开始和结束时,访问 MEMTEST_DECACHE_SIZE 大小的内存
 * 区域,以刷掉 CPU 缓存,确保测试真正访问主内存而非缓存. */
#define MEMTEST_DECACHE_SIZE (1024*8)

NO_SANITIZE("undefined")
int memtest_preserving_test(unsigned long *m, size_t bytes, int passes) {
    unsigned long backup[MEMTEST_BACKUP_WORDS];
    unsigned long *p = m;
    unsigned long *end = (unsigned long*) (((unsigned char*)m)+(bytes-MEMTEST_DECACHE_SIZE));
    size_t left = bytes;
    int errors = 0;

    if (bytes & 4095) return 0; /* Can't test across 4k page boundaries. */
    if (bytes < 4096*2) return 0; /* Can't test a single page. */

    while(left) {
        /* If we have to test a single final page, go back a single page
         * so that we can test two pages, since the code can't test a single
         * page but at least two. */
        if (left == 4096) {
            left += 4096;
            p -= 4096/sizeof(unsigned long);
        }

        int pass = 0;
        size_t len = (left > sizeof(backup)) ? sizeof(backup) : left;

        /* Always test an even number of pages. */
        if (len/4096 % 2) len -= 4096;

        memcpy(backup,p,len); /* Backup. */
        while(pass != passes) {
            pass++;
            errors += memtest_addressing(p,len,0);
            memtest_fill_random(p,len,0);
            if (bytes >= MEMTEST_DECACHE_SIZE) {
                memtest_compare_times(m,MEMTEST_DECACHE_SIZE,pass,1,0);
                memtest_compare_times(end,MEMTEST_DECACHE_SIZE,pass,1,0);
            }
            errors += memtest_compare_times(p,len,pass,4,0);
            memtest_fill_value(p,len,0,(unsigned long)-1,'S',0);
            if (bytes >= MEMTEST_DECACHE_SIZE) {
                memtest_compare_times(m,MEMTEST_DECACHE_SIZE,pass,1,0);
                memtest_compare_times(end,MEMTEST_DECACHE_SIZE,pass,1,0);
            }
            errors += memtest_compare_times(p,len,pass,4,0);
            memtest_fill_value(p,len,ULONG_ONEZERO,ULONG_ZEROONE,'C',0);
            if (bytes >= MEMTEST_DECACHE_SIZE) {
                memtest_compare_times(m,MEMTEST_DECACHE_SIZE,pass,1,0);
                memtest_compare_times(end,MEMTEST_DECACHE_SIZE,pass,1,0);
            }
            errors += memtest_compare_times(p,len,pass,4,0);
        }
        memcpy(p,backup,len); /* Restore. */
        left -= len;
        p += len/sizeof(unsigned long);
    }
    return errors;
}

/* Perform an interactive test allocating the specified number of megabytes. */
void memtest_alloc_and_test(size_t megabytes, int passes) {
    size_t bytes = megabytes*1024*1024;
    unsigned long *m = malloc(bytes);

    if (m == NULL) {
        fprintf(stderr,"Unable to allocate %zu megabytes: %s",
            megabytes, strerror(errno));
        exit(1);
    }
    memtest_test(m,bytes,passes,1);
    free(m);
}

/* memtest - 交互式内存测试入口函数
 *
 * 分配指定大小的内存并进行全面测试.
 * 这是一个交互式程序,会显示进度并打印 ASCII 艺术.
 *
 * 测试完成后会显示推荐工具:
 *   - memtest86: http://www.memtest86.com/
 *   - memtester: http://pyropus.ca/software/memtester/
 *
 * 参数:
 *   megabytes - 要测试的内存大小(兆字节)
 *   passes    - 每轮测试的次数
 */
void memtest(size_t megabytes, int passes) {
#if !defined(__HAIKU__)
    if (ioctl(1, TIOCGWINSZ, &ws) == -1) {
        ws.ws_col = 80;
        ws.ws_row = 20;
    }
#else
    ws.ws_col = 80;
    ws.ws_row = 20;
#endif
    memtest_alloc_and_test(megabytes,passes);
    printf("\nYour memory passed this test.\n");
    printf("Please if you are still in doubt use the following two tools:\n");
    printf("1) memtest86: http://www.memtest86.com/\n");
    printf("2) memtester: http://pyropus.ca/software/memtester/\n");
    exit(0);
}
