/* SDSLib 2.0 -- A C dynamic strings library
 *
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

/*
 * SDS (Simple Dynamic String) 实现文件
 *
 * 本文件实现了 Redis 的核心字符串库 SDS。SDS 在 C 字符串的基础上
 * 增加了以下能力：
 *
 * 1. O(1) 长度获取：通过 header 中的 len 字段，无需遍历整个字符串
 * 2. 二进制安全：可以存储任意二进制数据（包括 '\0'），不依赖 null 终止符判断长度
 * 3. 预分配策略：字符串增长时预分配额外空间，减少内存重分配次数
 * 4. 惰性释放：字符串缩短时不立即释放多余内存，为后续增长预留空间
 * 5. 兼容 C 字符串：始终以 '\0' 结尾，可直接传给标准 C 函数（如 printf）
 * 6. 多种 header 类型：根据字符串大小选择最小的 header，节省内存
 *
 * 主要函数分类：
 *   - 创建与销毁：sdsnewlen, sdsnew, sdsempty, sdsdup, sdsfree
 *   - 内存管理：sdsMakeRoomFor, sdsRemoveFreeSpace, sdsResize
 *   - 字符串操作：sdscatlen, sdscat, sdscpy, sdstrim, sdsrange 等
 *   - 格式化：sdscatprintf, sdscatfmt
 *   - 分割合并：sdssplitlen, sdsjoin, sdssplitargs
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include "redisassert.h"
#include "sds.h"
#include "sdsalloc.h"
#include "util.h"

/* SDS_NOINIT 用于 sdsnewlen()，表示不初始化缓冲区（保持未初始化状态以提升性能） */
const char *SDS_NOINIT = "SDS_NOINIT";

/* 根据 SDS 类型返回对应 header 结构体的大小（字节数）。
 * 不同类型的 header 包含不同大小的 len 和 alloc 字段。 */
static inline int sdsHdrSize(char type) {
    switch(type&SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            return sizeof(struct sdshdr5);
        case SDS_TYPE_8:
            return sizeof(struct sdshdr8);
        case SDS_TYPE_16:
            return sizeof(struct sdshdr16);
        case SDS_TYPE_32:
            return sizeof(struct sdshdr32);
        case SDS_TYPE_64:
            return sizeof(struct sdshdr64);
    }
    return 0;
}

/* 根据字符串长度选择最合适的 SDS header 类型。
 * 选择原则：使用能满足需求的最小 header 类型，以节省内存。
 *   - < 32 字节  → SDS_TYPE_5
 *   - < 256 字节 → SDS_TYPE_8
 *   - < 64KB     → SDS_TYPE_16
 *   - < 4GB      → SDS_TYPE_32（32位系统到此为止）
 *   - >= 4GB     → SDS_TYPE_64（仅64位系统） */
static inline char sdsReqType(size_t string_size) {
    if (string_size < 1<<5)
        return SDS_TYPE_5;
    if (string_size < 1<<8)
        return SDS_TYPE_8;
    if (string_size < 1<<16)
        return SDS_TYPE_16;
#if (LONG_MAX == LLONG_MAX)
    if (string_size < 1ll<<32)
        return SDS_TYPE_32;
    return SDS_TYPE_64;
#else
    return SDS_TYPE_32;
#endif
}

/* 返回指定 SDS 类型所能容纳的最大字符串长度。
 * 例如 SDS_TYPE_8 的 max = 2^8 - 1 = 255。
 * 注意：SDS_TYPE_5 的 max = 2^5 - 1 = 31。 */
static inline size_t sdsTypeMaxSize(char type) {
    if (type == SDS_TYPE_5)
        return (1<<5) - 1;
    if (type == SDS_TYPE_8)
        return (1<<8) - 1;
    if (type == SDS_TYPE_16)
        return (1<<16) - 1;
#if (LONG_MAX == LLONG_MAX)
    if (type == SDS_TYPE_32)
        return (1ll<<32) - 1;
#endif
    return -1; /* this is equivalent to the max SDS_TYPE_64 or SDS_TYPE_32 */
}

/* 创建一个新的 SDS 字符串，内容由 init 指针和 initlen 指定。
 *
 * 参数说明：
 *   - init: 初始内容指针。传 NULL 时用零初始化，传 SDS_NOINIT 时不初始化缓冲区
 *   - initlen: 初始内容长度
 *   - trymalloc: 为 1 时使用尝试性分配（失败返回 NULL），为 0 时分配失败会触发 assert
 *
 * 返回值：新创建的 SDS 字符串指针，内存不足时返回 NULL
 *
 * SDS 字符串始终以 '\0' 结尾（二进制安全：中间可以包含 '\0' 字符，
 * 长度通过 header 中的 len 字段获取，而非依赖 null 终止符）。
 *
 * 内存布局：
 *   [header][buf（字符串内容）][\0]
 *          ^
 *          |
 *     返回的 sds 指针指向这里
 *
 * 内部逻辑：
 *   1. 根据 initlen 选择合适的 header 类型
 *   2. 分配 header + initlen + 1（null 终止符）的内存
 *   3. 初始化 header（设置 len、alloc、flags）
 *   4. 如果 init 非 NULL 且非 SDS_NOINIT，复制初始内容
 *   5. 在末尾添加 '\0' 终止符 */
sds _sdsnewlen(const void *init, size_t initlen, int trymalloc) {
    void *sh;      /* 指向内存分配的起始位置（header 之前） */
    sds s;         /* 返回给调用者的 SDS 指针（指向 buf） */
    char type = sdsReqType(initlen);
    /* 空字符串通常是为了后续追加而创建的，使用 SDS_TYPE_8 而非 SDS_TYPE_5，
     * 因为 SDS_TYPE_5 不支持预分配空间（没有 alloc 字段）。 */
    if (type == SDS_TYPE_5 && initlen == 0) type = SDS_TYPE_8;
    int hdrlen = sdsHdrSize(type);
    unsigned char *fp; /* flags 指针，指向 s[-1]（即 flags 字节） */
    size_t usable;     /* 实际可用的缓冲区大小（可能因分配器对齐而大于请求大小） */

    /* 溢出检测：确保 initlen + hdrlen + 1 不会发生整数溢出 */
    assert(initlen + hdrlen + 1 > initlen);
    /* 分配内存：header 大小 + 字符串内容 + null 终止符 */
    sh = trymalloc?
        s_trymalloc_usable(hdrlen+initlen+1, &usable) :
        s_malloc_usable(hdrlen+initlen+1, &usable);
    if (sh == NULL) return NULL;
    if (init==SDS_NOINIT)
        init = NULL;          /* SDS_NOINIT：不初始化缓冲区内容 */
    else if (!init)
        memset(sh, 0, hdrlen+initlen+1);  /* init 为 NULL：用零填充 */
    s = (char*)sh+hdrlen;     /* s 指向 buf 的起始位置 */
    fp = ((unsigned char*)s)-1;  /* fp 指向 flags 字节（s 的前一个字节） */
    /* usable 为分配器实际提供的可用空间（减去 header 和 null 终止符），
     * 可能大于请求的 initlen，多出的部分作为预分配空间。 */
    usable = usable-hdrlen-1;
    if (usable > sdsTypeMaxSize(type))
        usable = sdsTypeMaxSize(type);  /* 不超过当前类型能表示的最大值 */
    /* 根据类型初始化 header 字段 */
    switch(type) {
        case SDS_TYPE_5: {
            /* SDS_TYPE_5 特殊处理：将类型和长度都编码到 flags 字节中
             * flags = 低3位(type) | 高5位(initlen) */
            *fp = type | (initlen << SDS_TYPE_BITS);
            break;
        }
        case SDS_TYPE_8: {
            SDS_HDR_VAR(8,s);
            sh->len = initlen;      /* 记录已使用长度 */
            sh->alloc = usable;     /* 记录已分配空间（可能 > initlen，即预分配） */
            *fp = type;             /* 设置类型标志 */
            break;
        }
        case SDS_TYPE_16: {
            SDS_HDR_VAR(16,s);
            sh->len = initlen;
            sh->alloc = usable;
            *fp = type;
            break;
        }
        case SDS_TYPE_32: {
            SDS_HDR_VAR(32,s);
            sh->len = initlen;
            sh->alloc = usable;
            *fp = type;
            break;
        }
        case SDS_TYPE_64: {
            SDS_HDR_VAR(64,s);
            sh->len = initlen;
            sh->alloc = usable;
            *fp = type;
            break;
        }
    }
    /* 复制初始内容到 buf（如果 init 非 NULL 且长度 > 0） */
    if (initlen && init)
        memcpy(s, init, initlen);
    /* 添加 null 终止符，保证与 C 字符串兼容 */
    s[initlen] = '\0';
    return s;
}

/* 创建新的 SDS 字符串（分配失败时触发 assert） */
sds sdsnewlen(const void *init, size_t initlen) {
    return _sdsnewlen(init, initlen, 0);
}

/* 尝试创建新的 SDS 字符串（分配失败时返回 NULL，不会 panic）。
 * 适用于内存紧张但可以优雅处理失败的场景。 */
sds sdstrynewlen(const void *init, size_t initlen) {
    return _sdsnewlen(init, initlen, 1);
}

/* 创建一个空的 SDS 字符串（长度为0）。
 * 虽然长度为0，但仍然有隐含的 '\0' 终止符。 */
sds sdsempty(void) {
    return sdsnewlen("",0);
}

/* 从 null 结尾的 C 字符串创建新的 SDS 字符串。
 * 如果 init 为 NULL，则创建长度为0的空字符串。 */
sds sdsnew(const char *init) {
    size_t initlen = (init == NULL) ? 0 : strlen(init);
    return sdsnewlen(init, initlen);
}

/* 复制一个 SDS 字符串，返回新创建的副本。 */
sds sdsdup(const sds s) {
    return sdsnewlen(s, sdslen(s));
}

/* 释放 SDS 字符串占用的内存。
 * 如果 s 为 NULL 则安全返回（不执行任何操作）。
 * 释放的是从 header 开始的整个内存块，因此需要先计算 header 的偏移量。 */
void sdsfree(sds s) {
    if (s == NULL) return;
    s_free((char*)s-sdsHdrSize(s[-1]));
}

/* 根据 strlen() 的结果更新 SDS 的长度为实际内容长度。
 *
 * 当直接修改 SDS buf 中的内容（如手动设置 '\0'）后，
 * header 中的 len 可能与实际内容不一致，此函数用于同步。
 *
 * 示例：
 *   s = sdsnew("foobar");  // len = 6
 *   s[2] = '\0';           // 手动截断，但 len 仍为 6
 *   sdsupdatelen(s);       // len 更新为 2（strlen("fo") 的结果）
 */
void sdsupdatelen(sds s) {
    size_t reallen = strlen(s);
    sdssetlen(s, reallen);
}

/* 清空 SDS 字符串内容（长度设为0），但不释放已分配的内存。
 * 已分配的空间保留为空闲区域，下次追加操作时可以直接使用，
 * 避免不必要的内存重分配。 */
void sdsclear(sds s) {
    sdssetlen(s, 0);
    s[0] = '\0';
}

/* 确保 SDS 字符串末尾有足够的空间供追加 addlen 字节的数据。
 *
 * 这是 SDS 预分配策略的核心函数。如果当前空闲空间不足，
 * 会重新分配内存并可能预分配额外空间以减少后续 realloc 次数。
 *
 * 参数：
 *   - s: SDS 字符串指针
 *   - addlen: 需要追加的字节数（不含 null 终止符）
 *   - greedy: 贪心模式标志
 *     - 1（贪心）：新长度 < 1MB 时预分配 2 倍空间，否则多分配 1MB
 *     - 0（非贪心）：只分配刚好满足 addlen 需要的空间
 *
 * 返回值：调整后的 SDS 指针（可能与输入相同，也可能因 realloc 而改变）。
 *         内存不足时返回 NULL。
 *
 * 重要：此函数不改变 sdslen() 返回的长度，只增加 sdsavail() 的值。
 *
 * 内部逻辑：
 *   1. 如果已有足够空间，直接返回
 *   2. 计算新长度（考虑贪心预分配）
 *   3. 根据新长度选择合适的 header 类型
 *   4. 如果类型不变，使用 realloc 扩展
 *   5. 如果类型改变（header 大小不同），分配新内存并复制数据 */
sds _sdsMakeRoomFor(sds s, size_t addlen, int greedy) {
    void *sh, *newsh;
    size_t avail = sdsavail(s);     /* 当前可用空间 */
    size_t len, newlen, reqlen;
    char type, oldtype = s[-1] & SDS_TYPE_MASK;  /* 当前和新的 header 类型 */
    int hdrlen;
    size_t usable;

    /* 快速返回：如果已有足够空间，无需任何操作 */
    if (avail >= addlen) return s;

    len = sdslen(s);
    sh = (char*)s-sdsHdrSize(oldtype);  /* 当前内存块的起始位置（header 之前） */
    reqlen = newlen = (len+addlen);     /* 最终需要的字符串长度 */
    assert(newlen > len);   /* 溢出检测 */
    /* 贪心预分配策略：
     * - 新长度 < 1MB：预分配 2 倍空间（如 100 → 200）
     * - 新长度 >= 1MB：额外预分配 1MB（如 1.5MB → 2.5MB）
     * 这种策略在字符串频繁追加时能显著减少 realloc 次数。 */
    if (greedy == 1) {
        if (newlen < SDS_MAX_PREALLOC)
            newlen *= 2;
        else
            newlen += SDS_MAX_PREALLOC;
    }

    /* 根据新长度选择 header 类型 */
    type = sdsReqType(newlen);

    /* 不使用 SDS_TYPE_5：用户正在追加字符串，而 SDS_TYPE_5 没有 alloc 字段，
     * 无法记录空闲空间，导致每次追加都需要调用 sdsMakeRoomFor()。 */
    if (type == SDS_TYPE_5) type = SDS_TYPE_8;

    hdrlen = sdsHdrSize(type);
    assert(hdrlen + newlen + 1 > reqlen);  /* 溢出检测 */
    if (oldtype==type) {
        /* 类型不变：直接使用 realloc 扩展内存（数据可能被移动） */
        newsh = s_realloc_usable(sh, hdrlen+newlen+1, &usable);
        if (newsh == NULL) return NULL;
        s = (char*)newsh+hdrlen;
    } else {
        /* 类型改变（header 大小不同）：不能使用 realloc，需要分配新内存并手动复制 */
        newsh = s_malloc_usable(hdrlen+newlen+1, &usable);
        if (newsh == NULL) return NULL;
        memcpy((char*)newsh+hdrlen, s, len+1);  /* 复制字符串内容 + null 终止符 */
        s_free(sh);  /* 释放旧内存 */
        s = (char*)newsh+hdrlen;
        s[-1] = type;         /* 更新类型标志 */
        sdssetlen(s, len);    /* 保持原有长度不变 */
    }
    /* 记录实际可用空间（可能因分配器对齐而大于请求值） */
    usable = usable-hdrlen-1;
    if (usable > sdsTypeMaxSize(type))
        usable = sdsTypeMaxSize(type);
    sdssetalloc(s, usable);
    return s;
}

/* 预分配空间（贪心模式）。
 * 比实际需要的多分配一些空间，适用于频繁追加的场景，
 * 可以减少 realloc 调用次数，提升性能。 */
sds sdsMakeRoomFor(sds s, size_t addlen) {
    return _sdsMakeRoomFor(s, addlen, 1);
}

/* 预分配空间（非贪心模式）。
 * 只分配刚好满足 addlen 需要的空间，适用于一次性追加或内存敏感的场景。 */
sds sdsMakeRoomForNonGreedy(sds s, size_t addlen) {
    return _sdsMakeRoomFor(s, addlen, 0);
}

/* 释放 SDS 末尾的空闲空间，将分配大小调整为恰好等于已使用长度。
 * 调用后，下次追加操作需要重新分配内存。
 * 返回的 SDS 指针可能与输入不同，调用者需更新所有引用。 */
sds sdsRemoveFreeSpace(sds s, int would_regrow) {
    return sdsResize(s, sdslen(s), would_regrow);
}

/* 调整 SDS 的分配空间大小。
 *
 * 此函数可以扩大或缩小分配空间。如果 size 小于当前已使用长度，
 * 字符串内容会被截断。
 *
 * 参数：
 *   - s: SDS 字符串指针
 *   - size: 期望的新分配大小
 *   - would_regrow: 如果为 1，表示字符串后续可能增长，
 *     此时避免使用 SDS_TYPE_5（因为 TYPE_5 没有 alloc 字段，
 *     无法记录空闲空间，会导致每次追加都需要 realloc）
 *
 * 注意：sdsalloc() 会被设置为请求的 size，而非实际分配大小，
 * 这样调用者可以检测到多余空间并避免重复调用此函数。 */
sds sdsResize(sds s, size_t size, int would_regrow) {
    void *sh, *newsh;
    char type, oldtype = s[-1] & SDS_TYPE_MASK;
    int hdrlen, oldhdrlen = sdsHdrSize(oldtype);
    size_t len = sdslen(s);
    sh = (char*)s-oldhdrlen;

    /* 快速返回：如果已分配空间正好等于请求大小 */
    if (sdsalloc(s) == size) return s;

    /* 如果请求大小小于当前长度，截断字符串 */
    if (size < len) len = size;

    /* 根据新大小选择最小的合适 header 类型 */
    type = sdsReqType(size);
    if (would_regrow) {
        /* 如果字符串后续可能增长，避免使用 SDS_TYPE_5
         * （TYPE_5 没有 alloc 字段，无法预分配空间） */
        if (type == SDS_TYPE_5) type = SDS_TYPE_8;
    }
    hdrlen = sdsHdrSize(type);

    /* 决定是否使用 realloc：
     * - 类型不变：使用 realloc（分配器可能原地扩展）
     * - 类型变小但 > SDS_TYPE_8：使用 realloc（开销较小）
     * - 其他情况（类型变化大）：分配新内存并手动复制 */
    int use_realloc = (oldtype==type || (type < oldtype && type > SDS_TYPE_8));
    size_t newlen = use_realloc ? oldhdrlen+size+1 : hdrlen+size+1;

    if (use_realloc) {
        int alloc_already_optimal = 0;
        #if defined(USE_JEMALLOC)
            /* je_nallocx 返回 newlen 对应的实际分配大小。
             * 如果 Jemalloc 的实际分配大小不变，跳过 realloc 调用，
             * 因为即使大小不变，realloc 也会有开销。 */
            alloc_already_optimal = (je_nallocx(newlen, 0) == zmalloc_size(sh));
        #endif
        if (!alloc_already_optimal) {
            newsh = s_realloc(sh, newlen);
            if (newsh == NULL) return NULL;
            s = (char*)newsh+oldhdrlen;
        }
    } else {
        /* 类型变化大，分配新内存并复制数据 */
        newsh = s_malloc(newlen);
        if (newsh == NULL) return NULL;
        memcpy((char*)newsh+hdrlen, s, len);
        s_free(sh);
        s = (char*)newsh+hdrlen;
        s[-1] = type;  /* 更新类型标志 */
    }
    s[len] = '\0';     /* 确保 null 终止符 */
    sdssetlen(s, len); /* 更新长度 */
    sdssetalloc(s, size); /* 更新已分配空间大小 */
    return s;
}

/* 返回 SDS 字符串的总内存占用大小，包括：
 * 1) header 结构体大小
 * 2) 字符串内容（已使用 + 空闲空间）
 * 3) null 终止符（1 字节）
 *
 * 此函数可用于精确统计 SDS 字符串的内存消耗。 */
size_t sdsAllocSize(sds s) {
    size_t alloc = sdsalloc(s);
    return sdsHdrSize(s[-1])+alloc+1;
}

/* 返回 SDS 实际内存分配的起始指针（header 的起始位置）。
 * SDS 通常通过 sds 指针（指向 buf）来引用，但内存块的实际起始位置在 header 之前。 */
void *sdsAllocPtr(sds s) {
    return (void*) (s-sdsHdrSize(s[-1]));
}

/* 手动调整 SDS 字符串的长度。
 *
 * 根据 incr 的值增加或减少字符串长度，并在新末尾设置 '\0' 终止符。
 * incr 可以为负值（用于从右侧截断字符串）。
 *
 * 典型使用场景：
 * 当调用 sdsMakeRoomFor() 预分配空间后，直接将数据写入 buf，
 * 然后调用此函数更新长度，避免了中间缓冲区的复制开销。
 *
 * 使用示例（零拷贝读取数据）：
 *
 *   oldlen = sdslen(s);
 *   s = sdsMakeRoomFor(s, BUFFER_SIZE);       // 预分配空间
 *   nread = read(fd, s+oldlen, BUFFER_SIZE);  // 直接写入 SDS buf
 *   ... 检查 nread 是否 <= 0 ...
 *   sdsIncrLen(s, nread);                     // 更新长度
 *
 * 内部逻辑：
 * 根据 SDS 类型，对 len 字段执行 += incr 操作，
 * 并进行边界检查（确保不溢出、不为负）。 */
void sdsIncrLen(sds s, ssize_t incr) {
    unsigned char flags = s[-1];
    size_t len;
    switch(flags&SDS_TYPE_MASK) {
        case SDS_TYPE_5: {
            unsigned char *fp = ((unsigned char*)s)-1;
            unsigned char oldlen = SDS_TYPE_5_LEN(flags);
            assert((incr > 0 && oldlen+incr < 32) || (incr < 0 && oldlen >= (unsigned int)(-incr)));
            *fp = SDS_TYPE_5 | ((oldlen+incr) << SDS_TYPE_BITS);
            len = oldlen+incr;
            break;
        }
        case SDS_TYPE_8: {
            SDS_HDR_VAR(8,s);
            assert((incr >= 0 && sh->alloc-sh->len >= incr) || (incr < 0 && sh->len >= (unsigned int)(-incr)));
            len = (sh->len += incr);
            break;
        }
        case SDS_TYPE_16: {
            SDS_HDR_VAR(16,s);
            assert((incr >= 0 && sh->alloc-sh->len >= incr) || (incr < 0 && sh->len >= (unsigned int)(-incr)));
            len = (sh->len += incr);
            break;
        }
        case SDS_TYPE_32: {
            SDS_HDR_VAR(32,s);
            assert((incr >= 0 && sh->alloc-sh->len >= (unsigned int)incr) || (incr < 0 && sh->len >= (unsigned int)(-incr)));
            len = (sh->len += incr);
            break;
        }
        case SDS_TYPE_64: {
            SDS_HDR_VAR(64,s);
            assert((incr >= 0 && sh->alloc-sh->len >= (uint64_t)incr) || (incr < 0 && sh->len >= (uint64_t)(-incr)));
            len = (sh->len += incr);
            break;
        }
        default: len = 0; /* Just to avoid compilation warnings. */
    }
    s[len] = '\0';
}

/* 将 SDS 字符串扩展到指定长度，新增的部分用零字节填充。
 * 如果指定长度小于等于当前长度，则不执行任何操作。
 * 返回调整后的 SDS 指针（可能因 realloc 而改变）。 */
sds sdsgrowzero(sds s, size_t len) {
    size_t curlen = sdslen(s);

    if (len <= curlen) return s;
    s = sdsMakeRoomFor(s,len-curlen);
    if (s == NULL) return NULL;

    /* Make sure added region doesn't contain garbage */
    memset(s+curlen,0,(len-curlen+1)); /* also set trailing \0 byte */
    sdssetlen(s, len);
    return s;
}

/* 将指定长度的二进制安全数据追加到 SDS 字符串末尾。
 *
 * 参数：
 *   - s: 目标 SDS 字符串
 *   - t: 要追加的数据指针
 *   - len: 要追加的字节数
 *
 * 返回值：追加后的 SDS 指针（可能因 realloc 而改变，调用者必须更新引用）。
 *
 * 此函数是二进制安全的，可以追加包含 '\0' 的数据。 */
sds sdscatlen(sds s, const void *t, size_t len) {
    size_t curlen = sdslen(s);

    s = sdsMakeRoomFor(s,len);
    if (s == NULL) return NULL;
    memcpy(s+curlen, t, len);
    sdssetlen(s, curlen+len);
    s[curlen+len] = '\0';
    return s;
}

/* Append the specified null terminated C string to the sds string 's'.
 *
 * After the call, the passed sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscat(sds s, const char *t) {
    return sdscatlen(s, t, strlen(t));
}

/* Append the specified sds 't' to the existing sds 's'.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscatsds(sds s, const sds t) {
    return sdscatlen(s, t, sdslen(t));
}

/* Destructively modify the sds string 's' to hold the specified binary
 * safe string pointed by 't' of length 'len' bytes. */
sds sdscpylen(sds s, const char *t, size_t len) {
    if (sdsalloc(s) < len) {
        s = sdsMakeRoomFor(s,len-sdslen(s));
        if (s == NULL) return NULL;
    }
    memcpy(s, t, len);
    s[len] = '\0';
    sdssetlen(s, len);
    return s;
}

/* Like sdscpylen() but 't' must be a null-terminated string so that the length
 * of the string is obtained with strlen(). */
sds sdscpy(sds s, const char *t) {
    return sdscpylen(s, t, strlen(t));
}

/* Create an sds string from a long long value. It is much faster than:
 *
 * sdscatprintf(sdsempty(),"%lld\n", value);
 */
sds sdsfromlonglong(long long value) {
    char buf[LONG_STR_SIZE];
    int len = ll2string(buf,sizeof(buf),value);

    return sdsnewlen(buf,len);
}

/* Like sdscatprintf() but gets va_list instead of being variadic. */
sds sdscatvprintf(sds s, const char *fmt, va_list ap) {
    va_list cpy;
    char staticbuf[1024], *buf = staticbuf, *t;
    size_t buflen = strlen(fmt)*2;
    int bufstrlen;

    /* We try to start using a static buffer for speed.
     * If not possible we revert to heap allocation. */
    if (buflen > sizeof(staticbuf)) {
        buf = s_malloc(buflen);
        if (buf == NULL) return NULL;
    } else {
        buflen = sizeof(staticbuf);
    }

    /* Alloc enough space for buffer and \0 after failing to
     * fit the string in the current buffer size. */
    while(1) {
        va_copy(cpy,ap);
        bufstrlen = vsnprintf(buf, buflen, fmt, cpy);
        va_end(cpy);
        if (bufstrlen < 0) {
            if (buf != staticbuf) s_free(buf);
            return NULL;
        }
        if (((size_t)bufstrlen) >= buflen) {
            if (buf != staticbuf) s_free(buf);
            buflen = ((size_t)bufstrlen) + 1;
            buf = s_malloc(buflen);
            if (buf == NULL) return NULL;
            continue;
        }
        break;
    }

    /* Finally concat the obtained string to the SDS string and return it. */
    t = sdscatlen(s, buf, bufstrlen);
    if (buf != staticbuf) s_free(buf);
    return t;
}

/* Append to the sds string 's' a string obtained using printf-alike format
 * specifier.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 *
 * Example:
 *
 * s = sdsnew("Sum is: ");
 * s = sdscatprintf(s,"%d+%d = %d",a,b,a+b).
 *
 * Often you need to create a string from scratch with the printf-alike
 * format. When this is the need, just use sdsempty() as the target string:
 *
 * s = sdscatprintf(sdsempty(), "... your format ...", args);
 */
sds sdscatprintf(sds s, const char *fmt, ...) {
    va_list ap;
    char *t;
    va_start(ap, fmt);
    t = sdscatvprintf(s,fmt,ap);
    va_end(ap);
    return t;
}

/* This function is similar to sdscatprintf, but much faster as it does
 * not rely on sprintf() family functions implemented by the libc that
 * are often very slow. Moreover directly handling the sds string as
 * new data is concatenated provides a performance improvement.
 *
 * However this function only handles an incompatible subset of printf-alike
 * format specifiers:
 *
 * %s - C String
 * %S - SDS string
 * %i - signed int
 * %I - 64 bit signed integer (long long, int64_t)
 * %u - unsigned int
 * %U - 64 bit unsigned integer (unsigned long long, uint64_t)
 * %% - Verbatim "%" character.
 */
sds sdscatfmt(sds s, char const *fmt, ...) {
    size_t initlen = sdslen(s);
    const char *f = fmt;
    long i;
    va_list ap;

    /* To avoid continuous reallocations, let's start with a buffer that
     * can hold at least two times the format string itself. It's not the
     * best heuristic but seems to work in practice. */
    s = sdsMakeRoomFor(s, strlen(fmt)*2);
    va_start(ap,fmt);
    f = fmt;    /* Next format specifier byte to process. */
    i = initlen; /* Position of the next byte to write to dest str. */
    while(*f) {
        char next, *str;
        size_t l;
        long long num;
        unsigned long long unum;

        /* Make sure there is always space for at least 1 char. */
        if (sdsavail(s)==0) {
            s = sdsMakeRoomFor(s,1);
        }

        switch(*f) {
        case '%':
            next = *(f+1);
            if (next == '\0') break;
            f++;
            switch(next) {
            case 's':
            case 'S':
                str = va_arg(ap,char*);
                l = (next == 's') ? strlen(str) : sdslen(str);
                if (sdsavail(s) < l) {
                    s = sdsMakeRoomFor(s,l);
                }
                memcpy(s+i,str,l);
                sdsinclen(s,l);
                i += l;
                break;
            case 'i':
            case 'I':
                if (next == 'i')
                    num = va_arg(ap,int);
                else
                    num = va_arg(ap,long long);
                {
                    char buf[LONG_STR_SIZE];
                    l = ll2string(buf,sizeof(buf),num);
                    if (sdsavail(s) < l) {
                        s = sdsMakeRoomFor(s,l);
                    }
                    memcpy(s+i,buf,l);
                    sdsinclen(s,l);
                    i += l;
                }
                break;
            case 'u':
            case 'U':
                if (next == 'u')
                    unum = va_arg(ap,unsigned int);
                else
                    unum = va_arg(ap,unsigned long long);
                {
                    char buf[LONG_STR_SIZE];
                    l = ull2string(buf,sizeof(buf),unum);
                    if (sdsavail(s) < l) {
                        s = sdsMakeRoomFor(s,l);
                    }
                    memcpy(s+i,buf,l);
                    sdsinclen(s,l);
                    i += l;
                }
                break;
            default: /* Handle %% and generally %<unknown>. */
                s[i++] = next;
                sdsinclen(s,1);
                break;
            }
            break;
        default:
            s[i++] = *f;
            sdsinclen(s,1);
            break;
        }
        f++;
    }
    va_end(ap);

    /* Add null-term */
    s[i] = '\0';
    return s;
}

/* Remove the part of the string from left and from right composed just of
 * contiguous characters found in 'cset', that is a null terminated C string.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 *
 * Example:
 *
 * s = sdsnew("AA...AA.a.aa.aHelloWorld     :::");
 * s = sdstrim(s,"Aa. :");
 * printf("%s\n", s);
 *
 * Output will be just "HelloWorld".
 */
sds sdstrim(sds s, const char *cset) {
    char *end, *sp, *ep;
    size_t len;

    sp = s;
    ep = end = s+sdslen(s)-1;
    while(sp <= end && strchr(cset, *sp)) sp++;
    while(ep > sp && strchr(cset, *ep)) ep--;
    len = (ep-sp)+1;
    if (s != sp) memmove(s, sp, len);
    s[len] = '\0';
    sdssetlen(s,len);
    return s;
}

/* Changes the input string to be a subset of the original.
 * It does not release the free space in the string, so a call to
 * sdsRemoveFreeSpace may be wise after. */
void sdssubstr(sds s, size_t start, size_t len) {
    /* Clamp out of range input */
    size_t oldlen = sdslen(s);
    if (start >= oldlen) start = len = 0;
    if (len > oldlen-start) len = oldlen-start;

    /* Move the data */
    if (len) memmove(s, s+start, len);
    s[len] = 0;
    sdssetlen(s,len);
}

/* Turn the string into a smaller (or equal) string containing only the
 * substring specified by the 'start' and 'end' indexes.
 *
 * start and end can be negative, where -1 means the last character of the
 * string, -2 the penultimate character, and so forth.
 *
 * The interval is inclusive, so the start and end characters will be part
 * of the resulting string.
 *
 * The string is modified in-place.
 *
 * NOTE: this function can be misleading and can have unexpected behaviour,
 * specifically when you want the length of the new string to be 0.
 * Having start==end will result in a string with one character.
 * please consider using sdssubstr instead.
 *
 * Example:
 *
 * s = sdsnew("Hello World");
 * sdsrange(s,1,-1); => "ello World"
 */
void sdsrange(sds s, ssize_t start, ssize_t end) {
    size_t newlen, len = sdslen(s);
    if (len == 0) return;
    if (start < 0)
        start = len + start;
    if (end < 0)
        end = len + end;
    newlen = (start > end) ? 0 : (end-start)+1;
    sdssubstr(s, start, newlen);
}

/* Apply tolower() to every character of the sds string 's'. */
void sdstolower(sds s) {
    size_t len = sdslen(s), j;

    for (j = 0; j < len; j++) s[j] = tolower(s[j]);
}

/* Apply toupper() to every character of the sds string 's'. */
void sdstoupper(sds s) {
    size_t len = sdslen(s), j;

    for (j = 0; j < len; j++) s[j] = toupper(s[j]);
}

/* Compare two sds strings s1 and s2 with memcmp().
 *
 * Return value:
 *
 *     positive if s1 > s2.
 *     negative if s1 < s2.
 *     0 if s1 and s2 are exactly the same binary string.
 *
 * If two strings share exactly the same prefix, but one of the two has
 * additional characters, the longer string is considered to be greater than
 * the smaller one. */
int sdscmp(const sds s1, const sds s2) {
    size_t l1, l2, minlen;
    int cmp;

    l1 = sdslen(s1);
    l2 = sdslen(s2);
    minlen = (l1 < l2) ? l1 : l2;
    cmp = memcmp(s1,s2,minlen);
    if (cmp == 0) return l1>l2? 1: (l1<l2? -1: 0);
    return cmp;
}

/* Split 's' with separator in 'sep'. An array
 * of sds strings is returned. *count will be set
 * by reference to the number of tokens returned.
 *
 * On out of memory, zero length string, zero length
 * separator, NULL is returned.
 *
 * Note that 'sep' is able to split a string using
 * a multi-character separator. For example
 * sdssplit("foo_-_bar","_-_"); will return two
 * elements "foo" and "bar".
 *
 * This version of the function is binary-safe but
 * requires length arguments. sdssplit() is just the
 * same function but for zero-terminated strings.
 */
sds *sdssplitlen(const char *s, ssize_t len, const char *sep, int seplen, int *count) {
    int elements = 0, slots = 5;
    long start = 0, j;
    sds *tokens;

    if (seplen < 1 || len <= 0) {
        *count = 0;
        return NULL;
    }
    tokens = s_malloc(sizeof(sds)*slots);
    if (tokens == NULL) return NULL;

    for (j = 0; j < (len-(seplen-1)); j++) {
        /* make sure there is room for the next element and the final one */
        if (slots < elements+2) {
            sds *newtokens;

            slots *= 2;
            newtokens = s_realloc(tokens,sizeof(sds)*slots);
            if (newtokens == NULL) goto cleanup;
            tokens = newtokens;
        }
        /* search the separator */
        if ((seplen == 1 && *(s+j) == sep[0]) || (memcmp(s+j,sep,seplen) == 0)) {
            tokens[elements] = sdsnewlen(s+start,j-start);
            if (tokens[elements] == NULL) goto cleanup;
            elements++;
            start = j+seplen;
            j = j+seplen-1; /* skip the separator */
        }
    }
    /* Add the final element. We are sure there is room in the tokens array. */
    tokens[elements] = sdsnewlen(s+start,len-start);
    if (tokens[elements] == NULL) goto cleanup;
    elements++;
    *count = elements;
    return tokens;

cleanup:
    {
        int i;
        for (i = 0; i < elements; i++) sdsfree(tokens[i]);
        s_free(tokens);
        *count = 0;
        return NULL;
    }
}

/* Free the result returned by sdssplitlen(), or do nothing if 'tokens' is NULL. */
void sdsfreesplitres(sds *tokens, int count) {
    if (!tokens) return;
    while(count--)
        sdsfree(tokens[count]);
    s_free(tokens);
}

/* Append to the sds string "s" an escaped string representation where
 * all the non-printable characters (tested with isprint()) are turned into
 * escapes in the form "\n\r\a...." or "\x<hex-number>".
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscatrepr(sds s, const char *p, size_t len) {
    s = sdsMakeRoomFor(s, len + 2);
    s = sdscatlen(s,"\"",1);
    while(len--) {
        switch(*p) {
        case '\\':
        case '"':
            s = sdscatprintf(s,"\\%c",*p);
            break;
        case '\n': s = sdscatlen(s,"\\n",2); break;
        case '\r': s = sdscatlen(s,"\\r",2); break;
        case '\t': s = sdscatlen(s,"\\t",2); break;
        case '\a': s = sdscatlen(s,"\\a",2); break;
        case '\b': s = sdscatlen(s,"\\b",2); break;
        default:
            if (isprint(*p))
                s = sdscatlen(s, p, 1);
            else
                s = sdscatprintf(s,"\\x%02x",(unsigned char)*p);
            break;
        }
        p++;
    }
    return sdscatlen(s,"\"",1);
}

/* Returns one if the string contains characters to be escaped
 * by sdscatrepr(), zero otherwise.
 *
 * Typically, this should be used to help protect aggregated strings in a way
 * that is compatible with sdssplitargs(). For this reason, also spaces will be
 * treated as needing an escape.
 */
int sdsneedsrepr(const sds s) {
    size_t len = sdslen(s);
    const char *p = s;

    while (len--) {
        if (*p == '\\' || *p == '"' || *p == '\n' || *p == '\r' ||
            *p == '\t' || *p == '\a' || *p == '\b' || !isprint(*p) || isspace(*p)) return 1;
        p++;
    }

    return 0;
}

/* Helper function for sdssplitargs() that returns non zero if 'c'
 * is a valid hex digit. */
int is_hex_digit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

/* Helper function for sdssplitargs() that converts a hex digit into an
 * integer from 0 to 15 */
int hex_digit_to_int(char c) {
    switch(c) {
    case '0': return 0;
    case '1': return 1;
    case '2': return 2;
    case '3': return 3;
    case '4': return 4;
    case '5': return 5;
    case '6': return 6;
    case '7': return 7;
    case '8': return 8;
    case '9': return 9;
    case 'a': case 'A': return 10;
    case 'b': case 'B': return 11;
    case 'c': case 'C': return 12;
    case 'd': case 'D': return 13;
    case 'e': case 'E': return 14;
    case 'f': case 'F': return 15;
    default: return 0;
    }
}

/* Split a line into arguments, where every argument can be in the
 * following programming-language REPL-alike form:
 *
 * foo bar "newline are supported\n" and "\xff\x00otherstuff"
 *
 * The number of arguments is stored into *argc, and an array
 * of sds is returned.
 *
 * The caller should free the resulting array of sds strings with
 * sdsfreesplitres().
 *
 * Note that sdscatrepr() is able to convert back a string into
 * a quoted string in the same format sdssplitargs() is able to parse.
 *
 * The function returns the allocated tokens on success, even when the
 * input string is empty, or NULL if the input contains unbalanced
 * quotes or closed quotes followed by non space characters
 * as in: "foo"bar or "foo'
 */
sds *sdssplitargs(const char *line, int *argc) {
    const char *p = line;
    char *current = NULL;
    char **vector = NULL;

    *argc = 0;
    while(1) {
        /* skip blanks */
        while(*p && isspace(*p)) p++;
        if (*p) {
            /* get a token */
            int inq=0;  /* set to 1 if we are in "quotes" */
            int insq=0; /* set to 1 if we are in 'single quotes' */
            int done=0;

            if (current == NULL) current = sdsempty();
            while(!done) {
                if (inq) {
                    if (*p == '\\' && *(p+1) == 'x' &&
                                             is_hex_digit(*(p+2)) &&
                                             is_hex_digit(*(p+3)))
                    {
                        unsigned char byte;

                        byte = (hex_digit_to_int(*(p+2))*16)+
                                hex_digit_to_int(*(p+3));
                        current = sdscatlen(current,(char*)&byte,1);
                        p += 3;
                    } else if (*p == '\\' && *(p+1)) {
                        char c;

                        p++;
                        switch(*p) {
                        case 'n': c = '\n'; break;
                        case 'r': c = '\r'; break;
                        case 't': c = '\t'; break;
                        case 'b': c = '\b'; break;
                        case 'a': c = '\a'; break;
                        default: c = *p; break;
                        }
                        current = sdscatlen(current,&c,1);
                    } else if (*p == '"') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done=1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current,p,1);
                    }
                } else if (insq) {
                    if (*p == '\\' && *(p+1) == '\'') {
                        p++;
                        current = sdscatlen(current,"'",1);
                    } else if (*p == '\'') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done=1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current,p,1);
                    }
                } else {
                    switch(*p) {
                    case ' ':
                    case '\n':
                    case '\r':
                    case '\t':
                    case '\0':
                        done=1;
                        break;
                    case '"':
                        inq=1;
                        break;
                    case '\'':
                        insq=1;
                        break;
                    default:
                        current = sdscatlen(current,p,1);
                        break;
                    }
                }
                if (*p) p++;
            }
            /* add the token to the vector */
            vector = s_realloc(vector,((*argc)+1)*sizeof(char*));
            vector[*argc] = current;
            (*argc)++;
            current = NULL;
        } else {
            /* Even on empty input string return something not NULL. */
            if (vector == NULL) vector = s_malloc(sizeof(void*));
            return vector;
        }
    }

err:
    while((*argc)--)
        sdsfree(vector[*argc]);
    s_free(vector);
    if (current) sdsfree(current);
    *argc = 0;
    return NULL;
}

/* Modify the string substituting all the occurrences of the set of
 * characters specified in the 'from' string to the corresponding character
 * in the 'to' array.
 *
 * For instance: sdsmapchars(mystring, "ho", "01", 2)
 * will have the effect of turning the string "hello" into "0ell1".
 *
 * The function returns the sds string pointer, that is always the same
 * as the input pointer since no resize is needed. */
sds sdsmapchars(sds s, const char *from, const char *to, size_t setlen) {
    size_t j, i, l = sdslen(s);

    for (j = 0; j < l; j++) {
        for (i = 0; i < setlen; i++) {
            if (s[j] == from[i]) {
                s[j] = to[i];
                break;
            }
        }
    }
    return s;
}

/* Join an array of C strings using the specified separator (also a C string).
 * Returns the result as an sds string. */
sds sdsjoin(char **argv, int argc, char *sep) {
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++) {
        join = sdscat(join, argv[j]);
        if (j != argc-1) join = sdscat(join,sep);
    }
    return join;
}

/* Like sdsjoin, but joins an array of SDS strings. */
sds sdsjoinsds(sds *argv, int argc, const char *sep, size_t seplen) {
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++) {
        join = sdscatsds(join, argv[j]);
        if (j != argc-1) join = sdscatlen(join,sep,seplen);
    }
    return join;
}

/* Wrappers to the allocators used by SDS. Note that SDS will actually
 * just use the macros defined into sdsalloc.h in order to avoid to pay
 * the overhead of function calls. Here we define these wrappers only for
 * the programs SDS is linked to, if they want to touch the SDS internals
 * even if they use a different allocator. */
void *sds_malloc(size_t size) { return s_malloc(size); }
void *sds_realloc(void *ptr, size_t size) { return s_realloc(ptr,size); }
void sds_free(void *ptr) { s_free(ptr); }

/* Perform expansion of a template string and return the result as a newly
 * allocated sds.
 *
 * Template variables are specified using curly brackets, e.g. {variable}.
 * An opening bracket can be quoted by repeating it twice.
 */
sds sdstemplate(const char *template, sdstemplate_callback_t cb_func, void *cb_arg)
{
    sds res = sdsempty();
    const char *p = template;

    while (*p) {
        /* Find next variable, copy everything until there */
        const char *sv = strchr(p, '{');
        if (!sv) {
            /* Not found: copy till rest of template and stop */
            res = sdscat(res, p);
            break;
        } else if (sv > p) {
            /* Found: copy anything up to the beginning of the variable */
            res = sdscatlen(res, p, sv - p);
        }

        /* Skip into variable name, handle premature end or quoting */
        sv++;
        if (!*sv) goto error;       /* Premature end of template */
        if (*sv == '{') {
            /* Quoted '{' */
            p = sv + 1;
            res = sdscat(res, "{");
            continue;
        }

        /* Find end of variable name, handle premature end of template */
        const char *ev = strchr(sv, '}');
        if (!ev) goto error;

        /* Pass variable name to callback and obtain value. If callback failed,
         * abort. */
        sds varname = sdsnewlen(sv, ev - sv);
        sds value = cb_func(varname, cb_arg);
        sdsfree(varname);
        if (!value) goto error;

        /* Append value to result and continue */
        res = sdscat(res, value);
        sdsfree(value);
        p = ev + 1;
    }

    return res;

error:
    sdsfree(res);
    return NULL;
}

#ifdef REDIS_TEST
#include <stdio.h>
#include <limits.h>
#include "testhelp.h"

#define UNUSED(x) (void)(x)

static sds sdsTestTemplateCallback(sds varname, void *arg) {
    UNUSED(arg);
    static const char *_var1 = "variable1";
    static const char *_var2 = "variable2";

    if (!strcmp(varname, _var1)) return sdsnew("value1");
    else if (!strcmp(varname, _var2)) return sdsnew("value2");
    else return NULL;
}

int sdsTest(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    {
        sds x = sdsnew("foo"), y;

        test_cond("Create a string and obtain the length",
            sdslen(x) == 3 && memcmp(x,"foo\0",4) == 0);

        sdsfree(x);
        x = sdsnewlen("foo",2);
        test_cond("Create a string with specified length",
            sdslen(x) == 2 && memcmp(x,"fo\0",3) == 0);

        x = sdscat(x,"bar");
        test_cond("Strings concatenation",
            sdslen(x) == 5 && memcmp(x,"fobar\0",6) == 0);

        x = sdscpy(x,"a");
        test_cond("sdscpy() against an originally longer string",
            sdslen(x) == 1 && memcmp(x,"a\0",2) == 0);

        x = sdscpy(x,"xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk");
        test_cond("sdscpy() against an originally shorter string",
            sdslen(x) == 33 &&
            memcmp(x,"xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk\0",33) == 0);

        sdsfree(x);
        x = sdscatprintf(sdsempty(),"%d",123);
        test_cond("sdscatprintf() seems working in the base case",
            sdslen(x) == 3 && memcmp(x,"123\0",4) == 0);

        sdsfree(x);
        x = sdscatprintf(sdsempty(),"a%cb",0);
        test_cond("sdscatprintf() seems working with \\0 inside of result",
            sdslen(x) == 3 && memcmp(x,"a\0""b\0",4) == 0);

        {
            sdsfree(x);
            char etalon[1024*1024];
            for (size_t i = 0; i < sizeof(etalon); i++) {
                etalon[i] = '0';
            }
            x = sdscatprintf(sdsempty(),"%0*d",(int)sizeof(etalon),0);
            test_cond("sdscatprintf() can print 1MB",
                sdslen(x) == sizeof(etalon) && memcmp(x,etalon,sizeof(etalon)) == 0);
        }

        sdsfree(x);
        x = sdsnew("--");
        x = sdscatfmt(x, "Hello %s World %I,%I--", "Hi!", LLONG_MIN,LLONG_MAX);
        test_cond("sdscatfmt() seems working in the base case",
            sdslen(x) == 60 &&
            memcmp(x,"--Hello Hi! World -9223372036854775808,"
                     "9223372036854775807--",60) == 0);
        printf("[%s]\n",x);

        sdsfree(x);
        x = sdsnew("--");
        x = sdscatfmt(x, "%u,%U--", UINT_MAX, ULLONG_MAX);
        test_cond("sdscatfmt() seems working with unsigned numbers",
            sdslen(x) == 35 &&
            memcmp(x,"--4294967295,18446744073709551615--",35) == 0);

        sdsfree(x);
        x = sdsnew(" x ");
        sdstrim(x," x");
        test_cond("sdstrim() works when all chars match",
            sdslen(x) == 0);

        sdsfree(x);
        x = sdsnew(" x ");
        sdstrim(x," ");
        test_cond("sdstrim() works when a single char remains",
            sdslen(x) == 1 && x[0] == 'x');

        sdsfree(x);
        x = sdsnew("xxciaoyyy");
        sdstrim(x,"xy");
        test_cond("sdstrim() correctly trims characters",
            sdslen(x) == 4 && memcmp(x,"ciao\0",5) == 0);

        y = sdsdup(x);
        sdsrange(y,1,1);
        test_cond("sdsrange(...,1,1)",
            sdslen(y) == 1 && memcmp(y,"i\0",2) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,1,-1);
        test_cond("sdsrange(...,1,-1)",
            sdslen(y) == 3 && memcmp(y,"iao\0",4) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,-2,-1);
        test_cond("sdsrange(...,-2,-1)",
            sdslen(y) == 2 && memcmp(y,"ao\0",3) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,2,1);
        test_cond("sdsrange(...,2,1)",
            sdslen(y) == 0 && memcmp(y,"\0",1) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,1,100);
        test_cond("sdsrange(...,1,100)",
            sdslen(y) == 3 && memcmp(y,"iao\0",4) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,100,100);
        test_cond("sdsrange(...,100,100)",
            sdslen(y) == 0 && memcmp(y,"\0",1) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,4,6);
        test_cond("sdsrange(...,4,6)",
            sdslen(y) == 0 && memcmp(y,"\0",1) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,3,6);
        test_cond("sdsrange(...,3,6)",
            sdslen(y) == 1 && memcmp(y,"o\0",2) == 0);

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("foo");
        y = sdsnew("foa");
        test_cond("sdscmp(foo,foa)", sdscmp(x,y) > 0);

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("bar");
        y = sdsnew("bar");
        test_cond("sdscmp(bar,bar)", sdscmp(x,y) == 0);

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("aar");
        y = sdsnew("bar");
        test_cond("sdscmp(bar,bar)", sdscmp(x,y) < 0);

        sdsfree(y);
        sdsfree(x);
        x = sdsnewlen("\a\n\0foo\r",7);
        y = sdscatrepr(sdsempty(),x,sdslen(x));
        test_cond("sdscatrepr(...data...)",
            memcmp(y,"\"\\a\\n\\x00foo\\r\"",15) == 0);

        {
            unsigned int oldfree;
            char *p;
            int i;
            size_t step = 10, j;

            sdsfree(x);
            sdsfree(y);
            x = sdsnew("0");
            test_cond("sdsnew() free/len buffers", sdslen(x) == 1 && sdsavail(x) == 0);

            /* Run the test a few times in order to hit the first two
             * SDS header types. */
            for (i = 0; i < 10; i++) {
                size_t oldlen = sdslen(x);
                x = sdsMakeRoomFor(x,step);
                int type = x[-1]&SDS_TYPE_MASK;

                test_cond("sdsMakeRoomFor() len", sdslen(x) == oldlen);
                if (type != SDS_TYPE_5) {
                    test_cond("sdsMakeRoomFor() free", sdsavail(x) >= step);
                    oldfree = sdsavail(x);
                    UNUSED(oldfree);
                }
                p = x+oldlen;
                for (j = 0; j < step; j++) {
                    p[j] = 'A'+j;
                }
                sdsIncrLen(x,step);
            }
            test_cond("sdsMakeRoomFor() content",
                memcmp("0ABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJ",x,101) == 0);
            test_cond("sdsMakeRoomFor() final length",sdslen(x)==101);

            sdsfree(x);
        }

        /* Simple template */
        x = sdstemplate("v1={variable1} v2={variable2}", sdsTestTemplateCallback, NULL);
        test_cond("sdstemplate() normal flow",
                  memcmp(x,"v1=value1 v2=value2",19) == 0);
        sdsfree(x);

        /* Template with callback error */
        x = sdstemplate("v1={variable1} v3={doesnotexist}", sdsTestTemplateCallback, NULL);
        test_cond("sdstemplate() with callback error", x == NULL);

        /* Template with empty var name */
        x = sdstemplate("v1={", sdsTestTemplateCallback, NULL);
        test_cond("sdstemplate() with empty var name", x == NULL);

        /* Template with truncated var name */
        x = sdstemplate("v1={start", sdsTestTemplateCallback, NULL);
        test_cond("sdstemplate() with truncated var name", x == NULL);

        /* Template with quoting */
        x = sdstemplate("v1={{{variable1}} {{} v2={variable2}", sdsTestTemplateCallback, NULL);
        test_cond("sdstemplate() with quoting",
                  memcmp(x,"v1={value1} {} v2=value2",24) == 0);
        sdsfree(x);

        /* Test sdsresize - extend */
        x = sdsnew("1234567890123456789012345678901234567890");
        x = sdsResize(x, 200, 1);
        test_cond("sdsrezie() expand len", sdslen(x) == 40);
        test_cond("sdsrezie() expand strlen", strlen(x) == 40);
        test_cond("sdsrezie() expand alloc", sdsalloc(x) == 200);
        /* Test sdsresize - trim free space */
        x = sdsResize(x, 80, 1);
        test_cond("sdsrezie() shrink len", sdslen(x) == 40);
        test_cond("sdsrezie() shrink strlen", strlen(x) == 40);
        test_cond("sdsrezie() shrink alloc", sdsalloc(x) == 80);
        /* Test sdsresize - crop used space */
        x = sdsResize(x, 30, 1);
        test_cond("sdsrezie() crop len", sdslen(x) == 30);
        test_cond("sdsrezie() crop strlen", strlen(x) == 30);
        test_cond("sdsrezie() crop alloc", sdsalloc(x) == 30);
        /* Test sdsresize - extend to different class */
        x = sdsResize(x, 400, 1);
        test_cond("sdsrezie() expand len", sdslen(x) == 30);
        test_cond("sdsrezie() expand strlen", strlen(x) == 30);
        test_cond("sdsrezie() expand alloc", sdsalloc(x) == 400);
        /* Test sdsresize - shrink to different class */
        x = sdsResize(x, 4, 1);
        test_cond("sdsrezie() crop len", sdslen(x) == 4);
        test_cond("sdsrezie() crop strlen", strlen(x) == 4);
        test_cond("sdsrezie() crop alloc", sdsalloc(x) == 4);
        sdsfree(x);
    }
    return 0;
}
#endif
