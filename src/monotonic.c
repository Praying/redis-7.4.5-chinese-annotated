/* monotonic.c - 单调时钟实现
 *
 * 提供高精度的单调时间（monotonic time）获取，
 * 用于 Redis 内部计时，避免系统时间被调整（ntp、时钟跳跃）导致的问题。
 */

#include "monotonic.h"
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "redisassert.h"

/* 函数指针：指向当前平台的单调时间获取函数（微秒级） */
monotime (*getMonotonicUs)(void) = NULL;

static char monotonic_info_string[32];


/* 使用处理器时钟（如 x86 TSC）可提升性能，比 POSIX 的 clock_gettime 快得多。
 * 在现代系统上通常是安全的，但存在一些边界情况。
 * 如需启用，可取消注释此行或使用 CFLAGS="-DUSE_PROCESSOR_CLOCK" 编译。
#define USE_PROCESSOR_CLOCK
 */


#if defined(USE_PROCESSOR_CLOCK) && defined(__x86_64__) && defined(__linux__)
#include <regex.h>
#include <x86intrin.h>

static long mono_ticksPerMicrosecond = 0;

/* getMonotonicUs_x86 - 通过 TSC 计数器获取单调时间（微秒） */
static monotime getMonotonicUs_x86(void) {
    return __rdtsc() / mono_ticksPerMicrosecond;
}

/* 从 /proc/cpuinfo 读取 CPU 主频，计算每微秒的 TSC 周期数 */
static void monotonicInit_x86linux(void) {
    const int bufflen = 256;
    char buf[bufflen];
    regex_t cpuGhzRegex, constTscRegex;
    const size_t nmatch = 2;
    regmatch_t pmatch[nmatch];
    int constantTsc = 0;
    int rc;

    /* 从 /proc/cpuinfo 解析 CPU 主频（GHz） */
    rc = regcomp(&cpuGhzRegex, "^model name\\s+:.*@ ([0-9.]+)GHz", REG_EXTENDED);
    assert(rc == 0);

    /* 检查 CPU 是否支持 constant_tsc（确保 TSC 在频率变化时保持恒定） */
    rc = regcomp(&constTscRegex, "^flags\\s+:.* constant_tsc", REG_EXTENDED);
    assert(rc == 0);

    FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
        while (fgets(buf, bufflen, cpuinfo) != NULL) {
            if (regexec(&cpuGhzRegex, buf, nmatch, pmatch, 0) == 0) {
                buf[pmatch[1].rm_eo] = '\0';
                double ghz = atof(&buf[pmatch[1].rm_so]);
                mono_ticksPerMicrosecond = (long)(ghz * 1000);
                break;
            }
        }
        while (fgets(buf, bufflen, cpuinfo) != NULL) {
            if (regexec(&constTscRegex, buf, nmatch, pmatch, 0) == 0) {
                constantTsc = 1;
                break;
            }
        }

        fclose(cpuinfo);
    }
    regfree(&cpuGhzRegex);
    regfree(&constTscRegex);

    if (mono_ticksPerMicrosecond == 0) {
        fprintf(stderr, "monotonic: x86 linux, unable to determine clock rate");
        return;
    }
    if (!constantTsc) {
        fprintf(stderr, "monotonic: x86 linux, 'constant_tsc' flag not present");
        return;
    }

    snprintf(monotonic_info_string, sizeof(monotonic_info_string),
            "X86 TSC @ %ld ticks/us", mono_ticksPerMicrosecond);
    getMonotonicUs = getMonotonicUs_x86;
}
#endif


#if defined(USE_PROCESSOR_CLOCK) && defined(__aarch64__)
static long mono_ticksPerMicrosecond = 0;

/* 读取 ARM 虚拟计数器（CNTVCT_EL0） */
static inline uint64_t __cntvct(void) {
    uint64_t virtual_timer_value;
    __asm__ volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
    return virtual_timer_value;
}

/* 读取计数器频率（CNTFREQ_EL0，单位 Hz） */
static inline uint32_t cntfrq_hz(void) {
    uint64_t virtual_freq_value;
    __asm__ volatile("mrs %0, cntfrq_el0" : "=r"(virtual_freq_value));
    return (uint32_t)virtual_freq_value;    /* 仅为低 32 位 */
}

static monotime getMonotonicUs_aarch64(void) {
    return __cntvct() / mono_ticksPerMicrosecond;
}

static void monotonicInit_aarch64(void) {
    mono_ticksPerMicrosecond = (long)cntfrq_hz() / 1000L / 1000L;
    if (mono_ticksPerMicrosecond == 0) {
        fprintf(stderr, "monotonic: aarch64, unable to determine clock rate");
        return;
    }

    snprintf(monotonic_info_string, sizeof(monotonic_info_string),
            "ARM CNTVCT @ %ld ticks/us", mono_ticksPerMicrosecond);
    getMonotonicUs = getMonotonicUs_aarch64;
}
#endif


/* getMonotonicUs_posix - 通过 POSIX clock_gettime 获取单调时间（微秒） */
static monotime getMonotonicUs_posix(void) {
    /* CLOCK_MONOTONIC 在 POSIX.1b (1993) 中定义，大多数现代系统均支持 */
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec) * 1000000 + ts.tv_nsec / 1000;
}

/* 初始化 POSIX 单调时钟 */
static void monotonicInit_posix(void) {
    /* 确认 CLOCK_MONOTONIC 可用 */
    struct timespec ts;
    int rc = clock_gettime(CLOCK_MONOTONIC, &ts);
    assert(rc == 0);

    snprintf(monotonic_info_string, sizeof(monotonic_info_string),
            "POSIX clock_gettime");
    getMonotonicUs = getMonotonicUs_posix;
}



/* monotonicInit - 初始化单调时钟
 * 依次尝试：x86 TSC > ARM CNTVCT > POSIX clock_gettime */
const char * monotonicInit(void) {
    #if defined(USE_PROCESSOR_CLOCK) && defined(__x86_64__) && defined(__linux__)
    if (getMonotonicUs == NULL) monotonicInit_x86linux();
    #endif

    #if defined(USE_PROCESSOR_CLOCK) && defined(__aarch64__)
    if (getMonotonicUs == NULL) monotonicInit_aarch64();
    #endif

    if (getMonotonicUs == NULL) monotonicInit_posix();

    return monotonic_info_string;
}

/* 返回当前使用的时钟信息字符串 */
const char *monotonicInfoString(void) {
    return monotonic_info_string;
}

/* 返回当前时钟类型（POSIX 或硬件） */
monotonic_clock_type monotonicGetType(void) {
    if (getMonotonicUs == getMonotonicUs_posix)
        return MONOTONIC_CLOCK_POSIX;
    return MONOTONIC_CLOCK_HW;
}
