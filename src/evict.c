/* Maxmemory 指令处理（LRU 淘汰及其他策略）。
 *
 * 本文件实现了 Redis 的内存淘汰机制。当内存使用量超过 maxmemory
 * 配置上限时，根据不同的淘汰策略（LRU、LFU、TTL、随机等）
 * 选择并删除键，以释放内存空间。
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "script.h"
#include <math.h>

/* ----------------------------------------------------------------------------
 * 数据结构
 * --------------------------------------------------------------------------*/

/* 为了提高 LRU 近似算法的质量，我们在多次 performEvictions() 调用
 * 之间维护一组适合淘汰的候选键。
 *
 * 淘汰池中的条目按空闲时间排序，较大的空闲时间在右侧（升序排列）。
 *
 * 当使用 LFU 策略时，使用反向频率值代替空闲时间，这样我们仍然
 * 按较大值淘汰（较大的反向频率意味着淘汰访问频率最低的键）。
 *
 * 空条目的 key 指针设置为 NULL。 */
#define EVPOOL_SIZE 16              /* 淘汰池大小 */
#define EVPOOL_CACHED_SDS_SIZE 255  /* 缓存 SDS 字符串的最大长度 */
struct evictionPoolEntry {
    unsigned long long idle;    /* 对象空闲时间（LFU 时为反向频率） */
    sds key;                    /* 键名 */
    sds cached;                 /* 用于缓存键名的 SDS 对象 */
    int dbid;                   /* 键所在的数据库编号 */
    int slot;                   /* 哈希槽 */
};

/* 全局淘汰池，用于在多次淘汰调用之间缓存最佳候选键 */
static struct evictionPoolEntry *EvictionPoolLRU;

static struct evictionPoolEntry *EvictionPoolLRU;

/* ----------------------------------------------------------------------------
 * 淘汰、老化和 LRU 的实现
 * --------------------------------------------------------------------------*/

/* 返回 LRU 时钟值，基于时钟精度。
 * 返回值是精简位数格式的时间，可用于设置和检查
 * redisObject 结构体的 object->lru 字段。 */
unsigned int getLRUClock(void) {
    return (mstime()/LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* 获取当前 LRU 时钟值。
 * 如果当前刷新频率低于 LRU 时钟精度（生产服务器通常如此），
 * 则返回预计算的缓存值；否则需要调用系统调用获取实时值。 */
unsigned int LRU_CLOCK(void) {
    unsigned int lruclock;
    if (1000/server.hz <= LRU_CLOCK_RESOLUTION) {
        lruclock = server.lruclock;
    } else {
        lruclock = getLRUClock();
    }
    return lruclock;
}

/* 使用近似 LRU 算法，估算对象自上次被访问以来的最小空闲毫秒数。 */
unsigned long long estimateObjectIdleTime(robj *o) {
    unsigned long long lruclock = LRU_CLOCK();
    if (lruclock >= o->lru) {
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION;
    } else {
        /* LRU 时钟回绕的情况 */
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) *
                    LRU_CLOCK_RESOLUTION;
    }
}

/* LRU 近似算法
 *
 * Redis 使用一种在常量内存下运行的 LRU 近似算法。每次需要淘汰键时，
 * 我们随机采样 N 个键（N 通常很小，约为 5），并将它们填充到一个
 * 大小为 M 的最佳淘汰候选池中（池大小由 EVPOOL_SIZE 定义）。
 *
 * 采样的 N 个键如果比池中已有的某个键更优（访问时间更早），
 * 则会被添加到淘汰候选池中。
 *
 * 池填充完成后，池中最佳的键将被淘汰。但请注意，当键被删除时
 * 我们不会从池中移除对应条目，因此池中可能包含已不存在的键。
 *
 * 当我们尝试淘汰键时，如果池中所有条目都已不存在，我们会重新
 * 填充池。此时可以确保池中至少有一个可淘汰的键（前提是数据库中
 * 至少存在一个可淘汰的键）。 */

/* 创建并初始化淘汰池。 */
void evictionPoolAlloc(void) {
    struct evictionPoolEntry *ep;
    int j;

    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    EvictionPoolLRU = ep;
}

/* performEvictions() 的辅助函数。每次需要淘汰键时调用此函数，
 * 向淘汰池中填充若干条目。空闲时间大于池中已有条目的键会被添加。
 * 如果池中有空闲位置，键总是会被添加。
 *
 * 我们按升序插入键，使空闲时间较小的键在左侧，空闲时间较大的键
 * 在右侧。 */
int evictionPoolPopulate(redisDb *db, kvstore *samplekvs, struct evictionPoolEntry *pool) {
    int j, k, count;
    dictEntry *samples[server.maxmemory_samples];

    /* 从 kvstore 中获取一个公平随机的哈希槽索引 */
    int slot = kvstoreGetFairRandomDictIndex(samplekvs);
    /* 从指定槽中采样若干键 */
    count = kvstoreDictGetSomeKeys(samplekvs,slot,samples,server.maxmemory_samples);
    for (j = 0; j < count; j++) {
        unsigned long long idle;
        sds key;
        robj *o;
        dictEntry *de;

        de = samples[j];
        key = dictGetKey(de);

        /* 如果采样的字典不是主字典（而是过期字典），
         * 需要在主键字典中重新查找以获取值对象。 */
        if (server.maxmemory_policy != MAXMEMORY_VOLATILE_TTL) {
            if (samplekvs != db->keys)
                de = kvstoreDictFind(db->keys, slot, key);
            o = dictGetVal(de);
        }

        /* 根据策略计算空闲时间（或得分）。
         * 这里称为 idle 只是因为代码最初只处理 LRU，
         * 但实际上它是一个得分，得分越高表示越适合淘汰。 */
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LRU) {
            /* LRU 策略：使用对象的空闲时间 */
            idle = estimateObjectIdleTime(o);
        } else if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            /* LFU 策略：使用反向频率值。
             * LRU 策略按空闲时间排序，从最大的空闲时间开始淘汰。
             * 而 LFU 策略有频率估计，我们希望优先淘汰频率最低的键。
             * 因此在池中使用反向频率（255 减去实际频率），
             * 这样仍然按较大值淘汰。 */
            idle = 255-LFUDecrAndReturn(o);
        } else if (server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {
            /* TTL 策略：过期时间越早越好。
             * 用 ULLONG_MAX 减去 TTL 值，使更早过期的键得分更高。 */
            idle = ULLONG_MAX - (long)dictGetVal(de);
        } else {
            serverPanic("Unknown eviction policy in evictionPoolPopulate()");
        }

        /* 将元素插入淘汰池中。
         * 首先找到第一个空位或第一个空闲时间小于当前元素的已占用位置。 */
        k = 0;
        while (k < EVPOOL_SIZE &&
               pool[k].key &&
               pool[k].idle < idle) k++;
        if (k == 0 && pool[EVPOOL_SIZE-1].key != NULL) {
            /* 如果当前元素得分小于池中最差元素，且池已满，
             * 则无法插入，跳过此元素。 */
            continue;
        } else if (k < EVPOOL_SIZE && pool[k].key == NULL) {
            /* 插入到空位，无需移动元素。 */
        } else {
            /* 插入到中间位置。此时 k 指向第一个大于待插入元素的位置。 */
            if (pool[EVPOOL_SIZE-1].key == NULL) {
                /* 右侧有空位：在 k 处插入，将 k 到末尾的元素向右移动。 */

                /* 在覆盖前保存 SDS 对象 */
                sds cached = pool[EVPOOL_SIZE-1].cached;
                memmove(pool+k+1,pool+k,
                    sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                pool[k].cached = cached;
            } else {
                /* 右侧无空位：插入到 k-1 处 */
                k--;
                /* 将 k 左侧（含 k）的所有元素向左移动，
                 * 丢弃空闲时间最小的元素。 */
                sds cached = pool[0].cached; /* 在覆盖前保存 SDS 对象 */
                if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                memmove(pool,pool+1,sizeof(pool[0])*k);
                pool[k].cached = cached;
            }
        }

        /* 尝试复用池条目中预分配的缓存 SDS 字符串，
         * 因为频繁分配和释放 SDS 对象开销较大
         * （这是 profiler 测出的，不是主观臆断）。 */
        int klen = sdslen(key);
        if (klen > EVPOOL_CACHED_SDS_SIZE) {
            /* 键名超过缓存长度，单独分配 */
            pool[k].key = sdsdup(key);
        } else {
            /* 键名较短，直接复用缓存的 SDS 空间 */
            memcpy(pool[k].cached,key,klen+1);
            sdssetlen(pool[k].cached,klen);
            pool[k].key = pool[k].cached;
        }
        pool[k].idle = idle;
        pool[k].dbid = db->id;
        pool[k].slot = slot;
    }

    return count;
}

/* ----------------------------------------------------------------------------
 * LFU（最不经常使用）淘汰策略的实现。
 *
 * 每个对象有 24 位空间用于实现 LFU 淘汰策略，
 * 复用了 LRU 字段的空间。
 *
 * 我们将 24 位分为两个字段：
 *
 *            16 位          8 位
 *     +------------------+--------+
 *     | 上次访问时间(LDT)| LOG_C  |
 *     +------------------+--------+
 *
 * LOG_C 是一个对数计数器，用于表示访问频率。
 * 但该字段也必须递减，否则过去频繁访问的键将永远保持高排名，
 * 而我们希望算法能适应访问模式的变化。
 *
 * 剩余的 16 位用于存储"访问时间"，即精简精度的 Unix 时间
 * （取分钟级时间的低 16 位，溢出回绕不影响功能）。
 * LOG_C 计数器默认每分钟衰减一次（取决于 lfu-decay-time 配置）。
 *
 * 新键不从零开始，以便在被淘汰前能积累一些访问次数，
 * 因此初始值为 LFU_INIT_VAL。LOG_C 的对数递增机制会考虑
 * LFU_INIT_VAL：从 LFU_INIT_VAL（或更小值）开始的键在被访问时
 * 有很高的概率被递增（概率取决于计数器值和 lfu-log-factor）。
 *
 * 递减时，每当 lfu-decay-time 分钟过去，对数计数器值减一。
 * --------------------------------------------------------------------------*/

/* 返回当前时间（分钟），仅取低 16 位。
 * 返回值适合作为 LFU 实现中的 LDT（上次访问时间）存储。 */
unsigned long LFUGetTimeInMinutes(void) {
    return (server.unixtime/60) & 65535;
}

/* 根据对象的 ldt（上次访问时间），计算自上次访问以来经过的最小分钟数。
 * 处理溢出情况（ldt 大于当前 16 位分钟时间），
 * 假设时间刚好回绕一次。 */
unsigned long LFUTimeElapsed(unsigned long ldt) {
    unsigned long now = LFUGetTimeInMinutes();
    if (now >= ldt) return now-ldt;
    return 65535-ldt+now;
}

/* 对计数器进行对数递增。当前计数器值越大，实际被递增的概率越低。
 * 计数器在 255 处饱和（不再增长）。 */
uint8_t LFULogIncr(uint8_t counter) {
    if (counter == 255) return 255;
    double r = (double)rand()/RAND_MAX;
    double baseval = counter - LFU_INIT_VAL;
    if (baseval < 0) baseval = 0;
    double p = 1.0/(baseval*server.lfu_log_factor+1);
    if (r < p) counter++;
    return counter;
}

/* 获取对象的 LFU 计数器值，并根据时间衰减递减计数器。
 * 不会更新对象的 LFU 字段——访问时间和计数器仅在对象真正被访问时
 * 才显式更新。这里根据已过去的 lfu-decay-time 周期数来递减计数器。
 * 返回衰减后的对象频率计数器值。
 *
 * 此函数用于在数据集中扫描最佳淘汰候选对象时，
 * 递减地更新被扫描对象的计数器。 */
unsigned long LFUDecrAndReturn(robj *o) {
    /* 提取 LDT（高 16 位）和计数器（低 8 位） */
    unsigned long ldt = o->lru >> 8;
    unsigned long counter = o->lru & 255;
    /* 计算经过了多少个衰减周期 */
    unsigned long num_periods = server.lfu_decay_time ? LFUTimeElapsed(ldt) / server.lfu_decay_time : 0;
    if (num_periods)
        /* 按周期数递减计数器，不低于 0 */
        counter = (num_periods > counter) ? 0 : counter - num_periods;
    return counter;
}

/* 我们不希望将 AOF 缓冲区和从节点输出缓冲区计入已用内存：
 * 淘汰机制应主要基于数据大小，因为将 DEL 命令推入这些缓冲区
 * 会造成反馈循环——更多的 DEL 会使缓冲区变大，如果计入这些内存，
 * 就需要淘汰更多键，从而产生更多 DEL，可能导致大规模淘汰循环，
 * 甚至所有键都被淘汰。
 *
 * 此函数返回 AOF 缓冲区和复制缓冲区的总大小。 */
size_t freeMemoryGetNotCountedMemory(void) {
    size_t overhead = 0;

    /* 由于所有副本和复制积压缓冲区共享全局复制缓冲区，
     * 我们认为只有超出积压缓冲区大小的部分才是副本的额外独立消耗。
     *
     * 注意，虽然积压缓冲区初期也会逐步增长（推送 DEL 会消耗内存），
     * 但它最终会停止增长并保持恒定大小，因此即使其创建会引起一些
     * 淘汰，也是有限的，不会产生共振效应。
     *
     * 注意，由于我们在后台逐步修剪积压缓冲区，当引用大量复制缓冲区
     * 块的慢速副本断开连接时，积压缓冲区大小可能超过设定值。
     * 为避免大规模淘汰循环，即使没有副本，我们也不将延迟释放的
     * 复制积压缓冲区计入已用内存——仍将其视为副本占用的内存。 */
    if ((long long)server.repl_buffer_mem > server.repl_backlog_size) {
        /* 我们使用链表结构管理复制缓冲区块，积压缓冲区也会占用
         * 一些额外内存。由于无法得知确切的块数量，只能根据每块大小
         * 进行近似估算。 */
        size_t extra_approx_size =
            (server.repl_backlog_size/PROTO_REPLY_CHUNK_BYTES + 1) *
            (sizeof(replBufBlock)+sizeof(listNode));
        size_t counted_mem = server.repl_backlog_size + extra_approx_size;
        if (server.repl_buffer_mem > counted_mem) {
            overhead += (server.repl_buffer_mem - counted_mem);
        }
    }

    /* AOF 缓冲区：如果 AOF 开启，将其分配大小计入不计入内存 */
    if (server.aof_state != AOF_OFF) {
        overhead += sdsAllocSize(server.aof_buf);
    }
    return overhead;
}

/* 从 maxmemory 指令的角度获取内存状态：
 * 如果已用内存低于 maxmemory 设定值，返回 C_OK。
 * 否则如果超出内存限制，返回 C_ERR。
 *
 * 当对应参数指针不为 NULL 时，通过引用返回附加信息。
 * 某些字段仅在返回 C_ERR 时才填充：
 *
 *  'total'     已用内存总字节数
 *              （C_OK 和 C_ERR 时均填充）
 *
 *  'logical'   已用内存减去从节点/AOF 缓冲区后的值
 *              （仅 C_ERR 时填充）
 *
 *  'tofree'    为回到内存限制内需要释放的内存量
 *              （仅 C_ERR 时填充）
 *
 *  'level'     内存使用比例，通常在 0 到 1 之间，
 *              超出内存限制时可能大于 1
 *              （C_OK 和 C_ERR 时均填充）
 */
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level) {
    size_t mem_reported, mem_used, mem_tofree;

    /* 获取 zmalloc 报告的已用内存总量 */
    mem_reported = zmalloc_used_memory();
    if (total) *total = mem_reported;

    /* 如果未设置 maxmemory，无需计算，立即返回 */
    if (!server.maxmemory) {
        if (level) *level = 0;
        return C_OK;
    }
    /* 如果未超限且不需要计算 level，可立即返回 */
    if (mem_reported <= server.maxmemory && !level) return C_OK;

    /* 从已用内存中扣除从节点输出缓冲区和 AOF 缓冲区的大小 */
    mem_used = mem_reported;
    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used-overhead : 0;

    /* 计算内存使用比例 */
    if (level) *level = (float)mem_used / (float)server.maxmemory;

    /* 报告的总量未超限则返回 OK */
    if (mem_reported <= server.maxmemory) return C_OK;

    /* 检查扣除缓冲区后是否仍然超限 */
    if (mem_used <= server.maxmemory) return C_OK;

    /* 计算需要释放的内存量 */
    mem_tofree = mem_used - server.maxmemory;

    if (logical) *logical = mem_used;
    if (tofree) *tofree = mem_tofree;

    return C_ERR;
}

/* 判断分配 moremem 字节后已用内存是否会超过 maxmemory。
 * 返回 1 表示会超限，返回 0 表示不会。
 * 当 Redis 已用内存超过 maxmemory 时可能拒绝请求或淘汰键，
 * 尤其是一次性分配大量内存时。 */
int overMaxmemoryAfterAlloc(size_t moremem) {
    if (!server.maxmemory) return  0; /* 未设置内存限制 */

    /* 快速检查：未扣缓冲区前若已不超限，则无需进一步计算 */
    size_t mem_used = zmalloc_used_memory();
    if (mem_used + moremem <= server.maxmemory) return 0;

    /* 扣除不计入的缓冲区内存后再判断 */
    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used - overhead : 0;
    return mem_used + moremem > server.maxmemory;
}

/* evictionTimeProc 在 maxmemory 被突破且无法立即解决时启动。
 * 它会在事件循环中以短周期反复执行淘汰，直到 maxmemory 条件
 * 满足或没有可淘汰的项为止。 */
static int isEvictionProcRunning = 0;
static int evictionTimeProc(
        struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    if (performEvictions() == EVICT_RUNNING) return 0;  /* 继续淘汰 */

    /* EVICT_OK - 内存已恢复正常，无需继续淘汰。
     * EVICT_FAIL - 已没有可淘汰的项。 */
    isEvictionProcRunning = 0;
    return AE_NOMORE;
}

/* 启动淘汰定时器事件。如果尚未运行，则创建一个立即触发的
 * 时间事件来执行淘汰处理。 */
void startEvictionTimeProc(void) {
    if (!isEvictionProcRunning) {
        isEvictionProcRunning = 1;
        aeCreateTimeEvent(server.el, 0,
                evictionTimeProc, NULL, NULL);
    }
}

/* 检查执行淘汰操作是否安全。
 *   返回 1 表示可以执行淘汰
 *   返回 0 表示应跳过淘汰处理
 */
static int isSafeToPerformEvictions(void) {
    /* 不能在脚本超时条件下执行淘汰 */
    /* 也不能在数据加载过程中执行 */
    if (isInsideYieldingLongCommand() || server.loading) return 0;

    /* 默认情况下，从节点应忽略 maxmemory 设置，
     * 仅作为主节点的精确副本。 */
    if (server.masterhost && server.repl_slave_ignore_maxmemory) return 0;

    /* 如果 'evict' 操作因任何原因被暂停，则返回 false */
    if (isPausedActionsWithUpdate(PAUSE_ACTION_EVICT)) return 0;

    return 1;
}

/* 将淘汰韧性参数（tenacity，0-100）转换为时间限制（微秒）的算法。
 * tenacity 越高，允许淘汰花费的时间越长。 */
static unsigned long evictionTimeLimitUs(void) {
    serverAssert(server.maxmemory_eviction_tenacity >= 0);
    serverAssert(server.maxmemory_eviction_tenacity <= 100);

    if (server.maxmemory_eviction_tenacity <= 10) {
        /* 0-10 范围：线性增长，从 0 到 500 微秒 */
        return 50uL * server.maxmemory_eviction_tenacity;
    }

    if (server.maxmemory_eviction_tenacity < 100) {
        /* 10-99 范围：15% 几何级数增长，
         * 当 tenacity==99 时限制约为 2 分钟 */
        return (unsigned long)(500.0 * pow(1.15, server.maxmemory_eviction_tenacity - 10.0));
    }

    return ULONG_MAX;   /* 100：不限制淘汰时间 */
}

/* 检查内存使用是否在当前 maxmemory 限制内。如果超出限制，
 * 尝试通过淘汰数据来释放内存（前提是操作安全）。
 *
 * Redis 可能会突然大幅超出 maxmemory 设定值。这可能发生在
 * 大规模内存分配（如哈希表扩容）或手动调整 maxmemory 设置时。
 * 因此，需要在受控的时间段内执行淘汰——否则 Redis 在淘汰期间
 * 会变得无响应。
 *
 * 此函数的目标是改善内存状况，而非立即解决。如果淘汰了一些项
 * 但仍未达到 maxmemory 限制，将启动一个 aeTimeProc 定时事件，
 * 继续淘汰直到内存限制满足或没有可淘汰的项。
 *
 * 应在执行命令前调用此函数。如果返回 EVICT_FAIL，
 * 则应拒绝会导致内存使用增加的命令。
 *
 * 返回值：
 *   EVICT_OK       - 内存正常，或当前无法执行淘汰
 *   EVICT_RUNNING  - 内存超限，但淘汰仍在进行中
 *   EVICT_FAIL     - 内存超限，且没有可淘汰的项
 */
int performEvictions(void) {
    /* 注意：这里不跳转到 update_metrics，因为此检查跳过淘汰
     * 相当于淘汰未被触发，是一个"假的" EVICT_OK。 */
    if (!isSafeToPerformEvictions()) return EVICT_OK;

    int keys_freed = 0;
    size_t mem_reported, mem_tofree;
    long long mem_freed; /* May be negative */
    mstime_t latency, eviction_latency;
    long long delta;
    int slaves = listLength(server.slaves);
    int result = EVICT_FAIL;

    /* 获取内存状态，如果未超限则直接返回 */
    if (getMaxmemoryState(&mem_reported,NULL,&mem_tofree,NULL) == C_OK) {
        result = EVICT_OK;
        goto update_metrics;
    }

    /* 策略为 noeviction 时，需要释放内存但策略禁止淘汰 */
    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION) {
        result = EVICT_FAIL;
        goto update_metrics;
    }

    unsigned long eviction_time_limit_us = evictionTimeLimitUs();

    mem_freed = 0;

    latencyStartMonitor(latency);

    monotime evictionTimer;
    elapsedStart(&evictionTimer);

    /* 断言检查：server.also_propagate 此处应为空 */
    serverAssert(server.also_propagate.numops == 0);
    /* 淘汰操作针对随机键执行，与当前命令的槽位无关。 */

    while (mem_freed < (long long)mem_tofree) {
        int j, k, i;
        static unsigned int next_db = 0;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        dictEntry *de;

        if (server.maxmemory_policy & (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU) ||
            server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL)
        {
            struct evictionPoolEntry *pool = EvictionPoolLRU;
            while (bestkey == NULL) {
                unsigned long total_keys = 0;

                /* 淘汰键时不应局限于单个数据库，
                 * 因此从每个数据库中采样键来填充淘汰池。 */
                for (i = 0; i < server.dbnum; i++) {
                    db = server.db+i;
                    kvstore *kvs;
                    if (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) {
                        kvs = db->keys;
                    } else {
                        kvs = db->expires;
                    }
                    unsigned long sampled_keys = 0;
                    unsigned long current_db_keys = kvstoreSize(kvs);
                    if (current_db_keys == 0) continue;

                    total_keys += current_db_keys;
                    int l = kvstoreNumNonEmptyDicts(kvs);
                    /* 循环次数不超过非空槽的数量 */
                    while (l--) {
                        sampled_keys += evictionPoolPopulate(db, kvs, pool);
                        /* 在当前数据库中已采样足够数量的键，退出循环 */
                        if (sampled_keys >= (unsigned long) server.maxmemory_samples)
                            break;
                        /* 如果当前数据库中键数量不多，字典可能非常稀疏，
                         * 无需满足采样数量要求即可退出循环 */
                        if (current_db_keys < (unsigned long) server.maxmemory_samples*10)
                            break;
                    }
                }
                if (!total_keys) break; /* 没有可淘汰的键 */

                /* 从最佳（最右）到最差（最左）反向遍历淘汰池，
                 * 选择一个实际存在的键进行淘汰。 */
                for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                    if (pool[k].key == NULL) continue;
                    bestdbid = pool[k].dbid;

                    kvstore *kvs;
                    if (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) {
                        kvs = server.db[bestdbid].keys;
                    } else {
                        kvs = server.db[bestdbid].expires;
                    }
                    de = kvstoreDictFind(kvs, pool[k].slot, pool[k].key);

                    /* 从池中移除该条目 */
                    if (pool[k].key != pool[k].cached)
                        sdsfree(pool[k].key);
                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    /* 如果键存在，就选中它。否则它是一个"幽灵"键
                     * （已被删除但仍在池中），尝试下一个元素。 */
                    if (de) {
                        bestkey = dictGetKey(de);
                        break;
                    } else {
                        /* 幽灵键，继续遍历 */
                    }
                }
            }
        }

        /* volatile-random 和 allkeys-random 策略 */
        else if (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 server.maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM)
        {
            /* 随机淘汰时，尝试从每个数据库中淘汰一个键，
             * 使用静态变量 next_db 来依次遍历所有数据库。 */
            for (i = 0; i < server.dbnum; i++) {
                j = (++next_db) % server.dbnum;
                db = server.db+j;
                kvstore *kvs;
                if (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM) {
                    kvs = db->keys;
                } else {
                    kvs = db->expires;
                }
                int slot = kvstoreGetFairRandomDictIndex(kvs);
                de = kvstoreDictGetRandomKey(kvs, slot);
                if (de) {
                    bestkey = dictGetKey(de);
                    bestdbid = j;
                    break;
                }
            }
        }

        /* 最终删除选中的键 */
        if (bestkey) {
            db = server.db+bestdbid;
            robj *keyobj = createStringObject(bestkey,sdslen(bestkey));
            /* 我们仅计算 db*Delete() 本身释放的内存量。
             * 实际上，在 AOF 和复制链路中传播 DEL 命令所需的内存
             * 可能大于删除键所释放的内存，但我们无法将这部分计入，
             * 否则循环将永远无法退出。
             *
             * signalModifiedKey 生成的客户端缓存失效消息同理。
             *
             * AOF 和输出缓冲区的内存最终会被释放，
             * 因此我们只关心键空间使用的内存。 */
            enterExecutionUnit(1, 0);
            delta = (long long) zmalloc_used_memory();
            latencyStartMonitor(eviction_latency);
            dbGenericDelete(db,keyobj,server.lazyfree_lazy_eviction,DB_FLAG_KEY_EVICTED);
            latencyEndMonitor(eviction_latency);
            latencyAddSampleIfNeeded("eviction-del",eviction_latency);
            delta -= (long long) zmalloc_used_memory();
            mem_freed += delta;
            server.stat_evictedkeys++;
            signalModifiedKey(NULL,db,keyobj);
            notifyKeyspaceEvent(NOTIFY_EVICTED, "evicted",
                keyobj, db->id);
            propagateDeletion(db,keyobj,server.lazyfree_lazy_eviction);
            exitExecutionUnit();
            postExecutionUnitOperations();
            decrRefCount(keyobj);
            keys_freed++;

            if (keys_freed % 16 == 0) {
                /* 当需要释放的内存量较大时，可能在此花费过多时间，
                 * 导致无法及时向从节点发送数据，
                 * 因此在循环中强制刷新从节点输出缓冲区。 */
                if (slaves) flushSlavesOutputBuffers();

                /* 正常情况下，停止条件是释放了预计算的固定内存量。
                 * 但当使用后台线程异步删除对象时，最好不时检查
                 * 是否已达到目标内存，因为 mem_freed 仅在
                 * dbAsyncDelete() 调用期间计算，
                 * 而后台线程可以持续释放内存。 */
                if (server.lazyfree_lazy_eviction) {
                    if (getMaxmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                        break;
                    }
                }

                /* 经过一段时间后提前退出循环——即使内存限制尚未满足。
                 * 如果突然需要释放大量内存，不应在此花费过多时间。 */
                if (elapsedUs(evictionTimer) > eviction_time_limit_us) {
                    // 仍需释放内存——启动淘汰定时器事件继续处理
                    startEvictionTimeProc();
                    break;
                }
            }
        } else {
            goto cant_free; /* 没有可淘汰的键... */
        }
    }
    /* 到达此处表示内存已恢复正常，或已达到时间限制 */
    result = (isEvictionProcRunning) ? EVICT_RUNNING : EVICT_OK;

cant_free:
    if (result == EVICT_FAIL) {
        /* 到此处表示已用尽可淘汰的项。但后台 lazyfree 线程中
         * 可能仍有正在释放的项。如果存在此类作业，进行短暂等待，
         * 但不会等待太久。 */
        mstime_t lazyfree_latency;
        latencyStartMonitor(lazyfree_latency);
        while (bioPendingJobsOfType(BIO_LAZY_FREE) &&
              elapsedUs(evictionTimer) < eviction_time_limit_us) {
            if (getMaxmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                result = EVICT_OK;
                break;
            }
            usleep(eviction_time_limit_us < 1000 ? eviction_time_limit_us : 1000);
        }
        latencyEndMonitor(lazyfree_latency);
        latencyAddSampleIfNeeded("eviction-lazyfree",lazyfree_latency);
    }

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle",latency);

update_metrics:
    /* 更新内存超限持续时间的统计指标 */
    if (result == EVICT_RUNNING || result == EVICT_FAIL) {
        /* 开始计时（如果尚未开始） */
        if (server.stat_last_eviction_exceeded_time == 0)
            elapsedStart(&server.stat_last_eviction_exceeded_time);
    } else if (result == EVICT_OK) {
        /* 内存恢复正常，累计超限时间并重置 */
        if (server.stat_last_eviction_exceeded_time != 0) {
            server.stat_total_eviction_exceeded_time += elapsedUs(server.stat_last_eviction_exceeded_time);
            server.stat_last_eviction_exceeded_time = 0;
        }
    }
    return result;
}
