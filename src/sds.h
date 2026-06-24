/* SDSLib 2.0 -- A C dynamic strings library
 *
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

/*
 * SDS (Simple Dynamic String) 头文件
 *
 * SDS 是 Redis 自己实现的动态字符串库，具有以下核心特性：
 *
 * 1. 二进制安全 (Binary Safe)：可以存储任意二进制数据，不限于文本
 * 2. O(1) 复杂度获取字符串长度：通过 header 中的 len 字段直接获取，无需遍历
 * 3. 预分配 (Pre-allocation)：当字符串需要扩展时，会预分配额外空间以减少内存重分配次数
 * 4. 惰性释放 (Lazy Free)：缩短字符串时不会立即释放多余内存，而是记录为空闲空间供后续使用
 * 5. 兼容 C 字符串：以 '\0' 结尾，可直接传给标准 C 字符串函数
 *
 * SDS 内存布局：
 *
 *   +--------+--------+--------+----------+
 *   | flags  |  len   | alloc  |   buf[]  |
 *   +--------+--------+--------+----------+
 *                         ^
 *                         |
 *                    sds 指针指向这里
 *
 * SDS 根据字符串长度使用不同大小的 header 结构：
 *   - sdshdr5:  用于极短字符串 (< 32 字节)，只使用 flags 字节
 *   - sdshdr8:  用于短字符串 (< 256 字节)
 *   - sdshdr16: 用于中等字符串 (< 65536 字节)
 *   - sdshdr32: 用于较长字符串 (< 4GB)
 *   - sdshdr64: 用于超长字符串 (>= 4GB，仅64位系统)
 *
 * sds 类型 (typedef char *sds) 指向 buf 的起始位置，flags 字节位于 s[-1]。
 * 通过 flags 的低 3 位可以判断 header 类型，进而计算 header 的偏移量。
 */

#ifndef __SDS_H
#define __SDS_H

/* 预分配的最大字节数。当字符串增长时，如果新长度 < 1MB，则预分配 2 倍空间；
 * 如果新长度 >= 1MB，则额外预分配 1MB 空间。 */
#define SDS_MAX_PREALLOC (1024*1024)

/* 用于 sdsnewlen() 的特殊标记，表示不初始化缓冲区内容（保持未初始化状态以提升性能） */
extern const char *SDS_NOINIT;

#include <sys/types.h>
#include <stdarg.h>
#include <stdint.h>

/* SDS 字符串类型，实际上就是 char*，指向 header 之后的 buf 起始位置 */
typedef char *sds;

/*
 * SDS header 结构体定义
 *
 * 使用 __attribute__((__packed__)) 确保结构体不会因对齐而插入填充字节，
 * 这样 sds 指针总是紧邻 flags 字节，方便通过 s[-1] 访问 flags。
 *
 * 五种 header 类型对应不同大小的字符串，以最小化内存开销：
 *   - sdshdr5:  极短字符串，flags 同时存储类型和长度（低3位=类型，高5位=长度）
 *   - sdshdr8:  len/alloc 各 1 字节，最大 255 字节
 *   - sdshdr16: len/alloc 各 2 字节，最大 65535 字节
 *   - sdshdr32: len/alloc 各 4 字节，最大 ~4GB
 *   - sdshdr64: len/alloc 各 8 字节，理论最大 ~16EB（仅64位系统使用）
 */

/* 注意：sdshdr5 实际不会被直接使用（代码中直接访问 flags 字节），
 * 此处仅用于文档说明 SDS_TYPE_5 的内存布局。 */
struct __attribute__ ((__packed__)) sdshdr5 {
    unsigned char flags; /* 低3位存储类型标识，高5位存储字符串长度 */
    char buf[];          /* 柔性数组成员，实际字符串数据从这里开始 */
};
struct __attribute__ ((__packed__)) sdshdr8 {
    uint8_t len;         /* 字符串已使用的长度 */
    uint8_t alloc;       /* 字符串已分配的总空间（不包含 header 和 null 终止符） */
    unsigned char flags; /* 低3位存储类型标识，高5位未使用 */
    char buf[];
};
struct __attribute__ ((__packed__)) sdshdr16 {
    uint16_t len;        /* 字符串已使用的长度 */
    uint16_t alloc;      /* 字符串已分配的总空间（不包含 header 和 null 终止符） */
    unsigned char flags; /* 低3位存储类型标识，高5位未使用 */
    char buf[];
};
struct __attribute__ ((__packed__)) sdshdr32 {
    uint32_t len;        /* 字符串已使用的长度 */
    uint32_t alloc;      /* 字符串已分配的总空间（不包含 header 和 null 终止符） */
    unsigned char flags; /* 低3位存储类型标识，高5位未使用 */
    char buf[];
};
struct __attribute__ ((__packed__)) sdshdr64 {
    uint64_t len;        /* 字符串已使用的长度 */
    uint64_t alloc;      /* 字符串已分配的总空间（不包含 header 和 null 终止符） */
    unsigned char flags; /* 低3位存储类型标识，高5位未使用 */
    char buf[];
};

/* SDS 类型常量，存储在 flags 字节的低3位中 */
#define SDS_TYPE_5  0    /* 极短字符串，长度存储在 flags 高5位 */
#define SDS_TYPE_8  1    /* 短字符串，len/alloc 各 1 字节 */
#define SDS_TYPE_16 2    /* 中等字符串，len/alloc 各 2 字节 */
#define SDS_TYPE_32 3    /* 长字符串，len/alloc 各 4 字节 */
#define SDS_TYPE_64 4    /* 超长字符串，len/alloc 各 8 字节 */
#define SDS_TYPE_MASK 7  /* 类型掩码（二进制 111），用于从 flags 中提取类型 */
#define SDS_TYPE_BITS 3  /* 类型占用的位数 */

/* 根据 sds 指针 s 和类型 T，声明并初始化一个指向 header 的指针变量 sh */
#define SDS_HDR_VAR(T,s) struct sdshdr##T *sh = (void*)((s)-(sizeof(struct sdshdr##T)));

/* 根据 sds 指针 s 和类型 T，返回指向 header 的指针（不声明变量） */
#define SDS_HDR(T,s) ((struct sdshdr##T *)((s)-(sizeof(struct sdshdr##T))))

/* 从 flags 字节中提取 SDS_TYPE_5 的长度（flags 高5位存储长度） */
#define SDS_TYPE_5_LEN(f) ((f)>>SDS_TYPE_BITS)

/* 获取 SDS 字符串已使用的长度。
 * 通过检查 s[-1] 的低3位确定 header 类型，然后从对应 header 中读取 len。
 * 对于 SDS_TYPE_5，长度存储在 flags 的高5位中。
 * 时间复杂度：O(1) */
static inline size_t sdslen(const sds s) {
    unsigned char flags = s[-1];
    switch(flags&SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            return SDS_TYPE_5_LEN(flags);
        case SDS_TYPE_8:
            return SDS_HDR(8,s)->len;
        case SDS_TYPE_16:
            return SDS_HDR(16,s)->len;
        case SDS_TYPE_32:
            return SDS_HDR(32,s)->len;
        case SDS_TYPE_64:
            return SDS_HDR(64,s)->len;
    }
    return 0;
}

/* 获取 SDS 字符串中可用（未使用）的字节数。
 * 计算方式：alloc - len。
 * SDS_TYPE_5 没有 alloc 字段，始终返回 0（极短字符串没有预分配空间）。
 * 时间复杂度：O(1) */
static inline size_t sdsavail(const sds s) {
    unsigned char flags = s[-1];
    switch(flags&SDS_TYPE_MASK) {
        case SDS_TYPE_5: {
            return 0;
        }
        case SDS_TYPE_8: {
            SDS_HDR_VAR(8,s);
            return sh->alloc - sh->len;
        }
        case SDS_TYPE_16: {
            SDS_HDR_VAR(16,s);
            return sh->alloc - sh->len;
        }
        case SDS_TYPE_32: {
            SDS_HDR_VAR(32,s);
            return sh->alloc - sh->len;
        }
        case SDS_TYPE_64: {
            SDS_HDR_VAR(64,s);
            return sh->alloc - sh->len;
        }
    }
    return 0;
}

/* 设置 SDS 字符串的长度为 newlen。
 * 注意：调用者需要确保 newlen 不超过已分配的空间。
 * 这是一个底层操作，通常由 sds 库内部使用。 */
static inline void sdssetlen(sds s, size_t newlen) {
    unsigned char flags = s[-1];
    switch(flags&SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            {
                unsigned char *fp = ((unsigned char*)s)-1;
                *fp = SDS_TYPE_5 | (newlen << SDS_TYPE_BITS);
            }
            break;
        case SDS_TYPE_8:
            SDS_HDR(8,s)->len = newlen;
            break;
        case SDS_TYPE_16:
            SDS_HDR(16,s)->len = newlen;
            break;
        case SDS_TYPE_32:
            SDS_HDR(32,s)->len = newlen;
            break;
        case SDS_TYPE_64:
            SDS_HDR(64,s)->len = newlen;
            break;
    }
}

/* 将 SDS 字符串的长度增加 inc 字节。
 * 这是一个底层操作，通常在直接写入 buf 后调用来更新长度。
 * 调用者需要确保增长后的长度不超过已分配的空间。 */
static inline void sdsinclen(sds s, size_t inc) {
    unsigned char flags = s[-1];
    switch(flags&SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            {
                unsigned char *fp = ((unsigned char*)s)-1;
                unsigned char newlen = SDS_TYPE_5_LEN(flags)+inc;
                *fp = SDS_TYPE_5 | (newlen << SDS_TYPE_BITS);
            }
            break;
        case SDS_TYPE_8:
            SDS_HDR(8,s)->len += inc;
            break;
        case SDS_TYPE_16:
            SDS_HDR(16,s)->len += inc;
            break;
        case SDS_TYPE_32:
            SDS_HDR(32,s)->len += inc;
            break;
        case SDS_TYPE_64:
            SDS_HDR(64,s)->len += inc;
            break;
    }
}

/* 获取 SDS 字符串已分配的总空间（不包含 header 和 null 终止符）。
 * sdsalloc() = sdsavail() + sdslen()
 * SDS_TYPE_5 没有 alloc 字段，返回的值等于 len（即不支持预分配）。 */
static inline size_t sdsalloc(const sds s) {
    unsigned char flags = s[-1];
    switch(flags&SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            return SDS_TYPE_5_LEN(flags);
        case SDS_TYPE_8:
            return SDS_HDR(8,s)->alloc;
        case SDS_TYPE_16:
            return SDS_HDR(16,s)->alloc;
        case SDS_TYPE_32:
            return SDS_HDR(32,s)->alloc;
        case SDS_TYPE_64:
            return SDS_HDR(64,s)->alloc;
    }
    return 0;
}

/* 设置 SDS 字符串的已分配空间大小为 newlen。
 * 这是一个底层操作，通常由 sds 库内部在内存重新分配后调用。
 * SDS_TYPE_5 没有 alloc 字段，此操作为空操作。 */
static inline void sdssetalloc(sds s, size_t newlen) {
    unsigned char flags = s[-1];
    switch(flags&SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            /* Nothing to do, this type has no total allocation info. */
            break;
        case SDS_TYPE_8:
            SDS_HDR(8,s)->alloc = newlen;
            break;
        case SDS_TYPE_16:
            SDS_HDR(16,s)->alloc = newlen;
            break;
        case SDS_TYPE_32:
            SDS_HDR(32,s)->alloc = newlen;
            break;
        case SDS_TYPE_64:
            SDS_HDR(64,s)->alloc = newlen;
            break;
    }
}

/* ---- SDS 创建与销毁 ---- */
sds sdsnewlen(const void *init, size_t initlen);     /* 使用指定内容和长度创建新的 SDS 字符串 */
sds sdstrynewlen(const void *init, size_t initlen);   /* 同 sdsnewlen，但内存分配失败时返回 NULL 而非 panic */
sds sdsnew(const char *init);                         /* 从 C 字符串创建新的 SDS 字符串 */
sds sdsempty(void);                                   /* 创建一个空的 SDS 字符串（长度为0，但有隐含的 null 终止符） */
sds sdsdup(const sds s);                              /* 复制一个 SDS 字符串 */
void sdsfree(sds s);                                  /* 释放 SDS 字符串占用的内存，NULL 时安全跳过 */

/* ---- SDS 修改操作 ---- */
sds sdsgrowzero(sds s, size_t len);                   /* 将 SDS 扩展到指定长度，新增部分用零填充 */
sds sdscatlen(sds s, const void *t, size_t len);      /* 将指定长度的二进制安全数据追加到 SDS 末尾 */
sds sdscat(sds s, const char *t);                     /* 将 C 字符串追加到 SDS 末尾 */
sds sdscatsds(sds s, const sds t);                    /* 将另一个 SDS 字符串追加到当前 SDS 末尾 */
sds sdscpylen(sds s, const char *t, size_t len);      /* 用指定长度的数据替换 SDS 的内容 */
sds sdscpy(sds s, const char *t);                     /* 用 C 字符串替换 SDS 的内容 */

/* ---- SDS 格式化输出 ---- */
sds sdscatvprintf(sds s, const char *fmt, va_list ap);  /* 使用 va_list 进行格式化追加 */
#ifdef __GNUC__
sds sdscatprintf(sds s, const char *fmt, ...)           /* 使用 printf 风格的格式化追加 */
    __attribute__((format(printf, 2, 3)));
#else
sds sdscatprintf(sds s, const char *fmt, ...);
#endif

sds sdscatfmt(sds s, char const *fmt, ...);             /* 高性能格式化追加（不依赖 libc 的 sprintf） */

/* ---- SDS 字符串操作 ---- */
sds sdstrim(sds s, const char *cset);                   /* 从 SDS 两端移除在 cset 中出现的字符 */
void sdssubstr(sds s, size_t start, size_t len);        /* 将 SDS 修改为原字符串的子串（原地修改） */
void sdsrange(sds s, ssize_t start, ssize_t end);       /* 将 SDS 修改为指定范围的子串（支持负数索引） */
void sdsupdatelen(sds s);                               /* 根据 strlen() 的结果更新 SDS 的长度 */
void sdsclear(sds s);                                   /* 清空 SDS 内容，但保留已分配的内存空间 */
int sdscmp(const sds s1, const sds s2);                 /* 比较两个 SDS 字符串（类似 memcmp） */

/* ---- SDS 分割与合并 ---- */
sds *sdssplitlen(const char *s, ssize_t len, const char *sep, int seplen, int *count); /* 按分隔符分割字符串 */
void sdsfreesplitres(sds *tokens, int count);           /* 释放 sdssplitlen() 返回的结果数组 */
sds sdsjoin(char **argv, int argc, char *sep);          /* 使用分隔符合并 C 字符串数组 */
sds sdsjoinsds(sds *argv, int argc, const char *sep, size_t seplen); /* 使用分隔符合并 SDS 字符串数组 */

/* ---- SDS 转换与实用工具 ---- */
void sdstolower(sds s);                                 /* 将 SDS 中所有字符转为小写 */
void sdstoupper(sds s);                                 /* 将 SDS 中所有字符转为大写 */
sds sdsfromlonglong(long long value);                   /* 从 long long 值创建 SDS 字符串 */
sds sdscatrepr(sds s, const char *p, size_t len);       /* 将二进制数据转义为可打印表示并追加 */
sds *sdssplitargs(const char *line, int *argc);         /* 将命令行字符串解析为参数数组（支持引号和转义） */
sds sdsmapchars(sds s, const char *from, const char *to, size_t setlen); /* 字符映射替换 */
int sdsneedsrepr(const sds s);                          /* 检查字符串是否需要转义表示 */

/* SDS 模板替换的回调函数类型。
 * 当 sdstemplate() 遇到模板变量（如 {variable}）时调用此回调。
 * 参数 variable 为变量名，arg 为用户自定义参数。
 * 回调返回替换值（新创建的 SDS），返回 NULL 表示出错。 */
typedef sds (*sdstemplate_callback_t)(const sds variable, void *arg);
sds sdstemplate(const char *template, sdstemplate_callback_t cb_func, void *cb_arg); /* 模板字符串变量替换 */

/* ---- SDS 底层内存管理函数（暴露给用户 API） ---- */
sds sdsMakeRoomFor(sds s, size_t addlen);              /* 预分配空间（贪心策略：多分配以减少后续 realloc） */
sds sdsMakeRoomForNonGreedy(sds s, size_t addlen);     /* 预分配空间（非贪心：只分配刚好需要的空间） */
void sdsIncrLen(sds s, ssize_t incr);                  /* 手动调整字符串长度（正值增长，负值缩短） */
sds sdsRemoveFreeSpace(sds s, int would_regrow);       /* 释放 SDS 末尾的空闲空间 */
sds sdsResize(sds s, size_t size, int would_regrow);   /* 调整 SDS 的分配空间大小 */
size_t sdsAllocSize(sds s);                            /* 获取 SDS 总内存占用（header + buf + null 终止符） */
void *sdsAllocPtr(sds s);                              /* 获取 SDS 实际内存分配的起始指针 */

/* SDS 使用的内存分配器封装。
 * 这些函数将 SDS 内部使用的分配器暴露给链接 SDS 的程序，
 * 使得外部程序可以使用与 SDS 相同的分配器来分配/释放内存，
 * 避免在不同分配器之间混用导致的问题。 */
void *sds_malloc(size_t size);
void *sds_realloc(void *ptr, size_t size);
void sds_free(void *ptr);

#ifdef REDIS_TEST
int sdsTest(int argc, char *argv[], int flags);
#endif

#endif
