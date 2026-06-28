/* EXPIRE 的实现（带有固定生存时间的键）。
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

/*-----------------------------------------------------------------------------
 * 过期键的增量回收。
 *
 * 当键被访问时，会在访问时进行过期检查并删除。但我们仍然需要一种机制，
 * 来确保即使没有任何访问发生，过期的键最终也会被移除。
 *----------------------------------------------------------------------------*/

/* 常量表，从 pow(0.98, 1) 到 pow(0.98, 16)。
 * 用于辅助计算 db->avg_ttl（平均过期时间）。 */
static double avg_ttl_factor[16] = {0.98, 0.9604, 0.941192, 0.922368, 0.903921, 0.885842, 0.868126, 0.850763, 0.833748, 0.817073, 0.800731, 0.784717, 0.769022, 0.753642, 0.738569, 0.723798};

/* activeExpireCycle() 函数的辅助函数。
 * 该函数尝试过期存储在 Redis 数据库 'expires' 哈希表中
 * 条目 'de' 所对应的键。
 *
 * 如果该键已过期，则将其从数据库中移除并返回 1。
 * 否则不执行任何操作并返回 0。
 *
 * 当一个键被过期删除时，server.stat_expiredkeys 会递增。
 *
 * 参数 'now' 是当前时间（毫秒），传入此参数是为了避免
 * 过多的 gettimeofday() 系统调用。 */
int activeExpireCycleTryExpire(redisDb *db, dictEntry *de, long long now) {
    long long t = dictGetSignedIntegerVal(de);
    if (now > t) {
        enterExecutionUnit(1, 0);
        sds key = dictGetKey(de);
        robj *keyobj = createStringObject(key,sdslen(key));
        deleteExpiredKeyAndPropagate(db,keyobj);
        decrRefCount(keyobj);
        exitExecutionUnit();
        return 1;
    } else {
        return 0;
    }
}

/* 尝试过期一批已超时的键。所使用的算法是自适应的：
 * 如果过期键较少，则只消耗少量 CPU 周期；
 * 否则会变得更加积极，以避免可删除的键占用过多内存。
 *
 * 每次过期周期会检查多个数据库：下一次调用将从下一个数据库开始。
 * 每次迭代最多检查 CRON_DBS_PER_CALL 个数据库。
 *
 * 根据 "type" 参数，该函数可以执行不同程度的工作：
 * 它可以执行"快速周期"或"慢速周期"。
 * 慢速周期是回收过期键的主要方式，其频率为 server.hz（通常为 10 赫兹）。
 *
 * 然而，慢速周期可能因为超时而退出。
 * 因此，该函数也会在每次事件循环中通过 beforeSleep() 执行快速周期。
 * 快速周期执行的工作量更少，但调用频率更高。
 *
 * 以下是两种过期周期的详细信息及其停止条件：
 *
 * 如果 type 为 ACTIVE_EXPIRE_CYCLE_FAST，函数将尝试运行一个"快速"
 * 过期周期，该周期耗时不超过 ACTIVE_EXPIRE_CYCLE_FAST_DURATION 微秒，
 * 且在相同时间间隔内不会重复运行。如果上一次慢速周期不是因为超时
 * 而退出的，则该周期将拒绝运行。
 *
 * 如果 type 为 ACTIVE_EXPIRE_CYCLE_SLOW，则执行正常的过期周期，
 * 时间限制为 REDIS_HZ 周期的一定百分比，由
 * ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 定义指定。
 * 在快速周期中，一旦估计数据库中已过期键的比例低于给定阈值，
 * 对该数据库的检查就会中断，以避免做过多工作却只回收少量内存。
 *
 * 配置的 expire "effort"（过期力度）会修改基准参数，
 * 以便在快速和慢速过期周期中执行更多的工作。
 */

#define ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP 20 /* 每个数据库循环的键数。 */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000 /* 微秒。 */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25 /* 最大 CPU 使用百分比。 */
#define ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE 10 /* 超过此比例的陈旧键后
                                                   执行额外的回收工作。 */

#define HFE_DB_BASE_ACTIVE_EXPIRE_FIELDS_PER_SEC 10000
/* 每秒每数据库基础的哈希字段主动过期数量。 */

/* 过期字典扫描回调所使用的数据结构。 */
typedef struct {
    redisDb *db;            /* 目标数据库 */
    long long now;          /* 当前时间（毫秒） */
    unsigned long sampled;  /* 已检查的键数量 */
    unsigned long expired;  /* 已过期的键数量 */
    long long ttl_sum;     /* 尚未过期的键的 TTL 总和 */
    int ttl_samples;       /* 尚未过期的键的采样数 */
} expireScanData;

/* 过期字典扫描回调函数，由 kvstoreScan 调用。 */
void expireScanCallback(void *privdata, const dictEntry *const_de) {
    dictEntry *de = (dictEntry *)const_de;
    expireScanData *data = privdata;
    long long ttl  = dictGetSignedIntegerVal(de) - data->now;
    if (activeExpireCycleTryExpire(data->db, de, data->now)) {
        data->expired++;
        /* 传播 DEL 命令 */
        postExecutionUnitOperations();
    }
    if (ttl > 0) {
        /* 收集尚未过期的键的 TTL，用于计算平均值。 */
        data->ttl_sum += ttl;
        data->ttl_samples++;
    }
    data->sampled++;
}

/* 检查过期字典是否适合进行采样扫描。
 * 当桶的填充率低于 1% 时，采样效率很低，此时应跳过等待
 * 字典重新调整大小。 */
static inline int isExpiryDictValidForSamplingCb(dict *d) {
    long long numkeys = dictSize(d);
    unsigned long buckets = dictBuckets(d);
    /* 当已填充桶的比例低于 1% 时，采样键空间代价很高，
     * 因此在这里停止，等待更好的时机……
     * 字典会尽快进行重新调整。 */
    if (buckets > DICT_HT_INITIAL_SIZE && (numkeys * 100/buckets < 1)) {
        return C_ERR;
    }
    return C_OK;
}

/* 哈希字段的主动过期周期。
 *
 * 注意，释放字段比释放键更加可预测且更有收益，因为字段存储在
 * 针对主动过期优化的 `ebuckets` 数据结构中，而且字段的删除
 * 处理也更简单。 */
static inline void activeExpireHashFieldCycle(int type) {
    /* 跨调用记住当前数据库索引 */
    static unsigned int currentDb = 0;

    /* 跟踪当前数据库的字段主动过期计数。
     * 只要未能主动过期完 currentDb 的所有已过期字段，
     * 该计数就会持续累加，表明可能需要调整 maxToExpire 的值。 */
    static uint64_t activeExpirySequence = 0;
    /* 调整 maxToExpire 的阈值 */
    const uint32_t EXPIRED_FIELDS_TH = 1000000;

    redisDb *db = server.db + currentDb;

    /* 如果数据库为空，切换到下一个数据库并返回 */
    if (ebIsEmpty(db->hexpires)) {
        activeExpirySequence = 0;
        currentDb = (currentDb + 1) % server.dbnum;
        return;
    }

    /* 单次调用中主动过期的最大字段数 */
    uint32_t maxToExpire = HFE_DB_BASE_ACTIVE_EXPIRE_FIELDS_PER_SEC / server.hz;

    /* 如果运行了一段时间但仍未能主动过期完 currentDb 的所有已过期字段
     * （即 activeExpirySequence 变得很大），则调整 maxToExpire */
    if ((activeExpirySequence > EXPIRED_FIELDS_TH) && (type == ACTIVE_EXPIRE_CYCLE_SLOW)) {
        /* maxToExpire 乘以一个 1 到 32 的因子，该因子与
         * activeExpirySequence 超过 EXPIRED_FIELDS_TH 的次数成正比 */
        uint64_t factor = activeExpirySequence / EXPIRED_FIELDS_TH;
        maxToExpire *= (factor<32) ? factor : 32;
    }

    if (hashTypeDbActiveExpire(db, maxToExpire) == maxToExpire) {
        /* 主动过期达到了 maxToExpire 的上限 */
        activeExpirySequence += maxToExpire;
    } else {
        /* 已成功主动过期完 currentDb 的所有已过期字段 */
        activeExpirySequence = 0;
        currentDb = (currentDb + 1) % server.dbnum;
    }
}

void activeExpireCycle(int type) {
    /* 根据配置的过期力度（expire effort）调整运行参数。
     * 默认力度为 1，最大可配置力度为 10。 */
    unsigned long
    effort = server.active_expire_effort-1, /* 重新缩放到 0 到 9。 */
    config_keys_per_loop = ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP +
                           ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP/4*effort,
    config_cycle_fast_duration = ACTIVE_EXPIRE_CYCLE_FAST_DURATION +
                                 ACTIVE_EXPIRE_CYCLE_FAST_DURATION/4*effort,
    config_cycle_slow_time_perc = ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC +
                                  2*effort,
    config_cycle_acceptable_stale = ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE-
                                    effort;

    /* 该函数具有一些全局状态，以便在多次调用之间增量地继续工作。 */
    static unsigned int current_db = 0; /* 下一个待检查的数据库。 */
    static int timelimit_exit = 0;      /* 上次调用是否达到时间限制？ */
    static long long last_fast_cycle = 0; /* 上次快速周期的运行时间。 */

    int j, iteration = 0;
    int dbs_per_call = CRON_DBS_PER_CALL;
    int dbs_performed = 0;
    long long start = ustime(), timelimit, elapsed;

    /* 如果 'expire' 操作被暂停（无论出于何种原因），则不执行任何键的过期。
     * 通常在暂停结束时，我们会正确地过期该键，或者已经发生了故障转移，
     * 新的主节点会发送过期操作。 */
    if (isPausedActionsWithUpdate(PAUSE_ACTION_EXPIRE)) return;

    if (type == ACTIVE_EXPIRE_CYCLE_FAST) {
        /* 如果上一次周期不是因为超时而退出的，则不启动快速周期，
         * 除非估计的陈旧键比例过高。同时，在快速周期总时长的
         * 两倍时间内不会重复运行快速周期。 */
        if (!timelimit_exit &&
            server.stat_expired_stale_perc < config_cycle_acceptable_stale)
            return;

        if (start < last_fast_cycle + (long long)config_cycle_fast_duration*2)
            return;

        last_fast_cycle = start;
    }

    /* 通常每次迭代应该测试 CRON_DBS_PER_CALL 个数据库，
     * 但有两个例外：
     *
     * 1) 不要测试超过实际数量的数据库。
     * 2) 如果上次达到了时间限制，我们希望在本次迭代中扫描所有数据库，
     *    因为某些数据库中还有工作要做，我们不希望过期键占用内存太久。 */
    if (dbs_per_call > server.dbnum || timelimit_exit)
        dbs_per_call = server.dbnum;

    /* 每次迭代最多可以使用 config_cycle_slow_time_perc 百分比的 CPU 时间。
     * 由于该函数每秒被调用 server.hz 次，以下是我们可以在此函数中
     * 花费的最大微秒数。 */
    timelimit = config_cycle_slow_time_perc*1000000/server.hz/100;
    timelimit_exit = 0;
    if (timelimit <= 0) timelimit = 1;

    if (type == ACTIVE_EXPIRE_CYCLE_FAST)
        timelimit = config_cycle_fast_duration; /* 以微秒为单位。 */

    /* 在过期键的过程中累积一些全局统计数据，
     * 以了解已经逻辑过期但仍存在于数据库中的键数量。 */
    long total_sampled = 0;
    long total_expired = 0;

    /* 尝试发现 bug（此处 server.also_propagate 应为空） */
    serverAssert(server.also_propagate.numops == 0);

    /* 当满足以下任一条件时停止迭代：
     *
     * 1) 已检查了足够数量的带过期时间的数据库。
     * 2) 已超过时间限制。
     * 3) 所有数据库都已遍历完毕。 */
    for (j = 0; dbs_performed < dbs_per_call && timelimit_exit == 0 && j < server.dbnum; j++) {
        /* 扫描回调数据，包括每次迭代的过期和已检查计数。 */
        expireScanData data;
        data.ttl_sum = 0;
        data.ttl_samples = 0;

        redisDb *db = server.db+(current_db % server.dbnum);
        data.db = db;

        int db_done = 0; /* 当前数据库的扫描是否已完成？ */
        int update_avg_ttl_times = 0, repeat = 0;

        /* 立即递增数据库索引，这样如果在当前数据库中超时，
         * 下次将从下一个数据库重新开始。这样可以将时间均匀分配
         * 到各个数据库。 */
        current_db++;

        /* 将哈希字段过期与键过期交叉执行。最好在处理过期键之前调用，
         * 因为 HFE（哈希字段过期）数据结构针对主动过期进行了优化。 */
        activeExpireHashFieldCycle(type);

        if (kvstoreSize(db->expires))
            dbs_performed++;

        /* 如果在周期结束时，与已扫描的键数量相比仍有很大比例的键需要过期，
         * 则继续过期。该百分比存储在 config_cycle_acceptable_stale 中，
         * 不是固定的，而是取决于 Redis 配置的"过期力度"。 */
        do {
            unsigned long num;
            iteration++;

            /* 如果没有需要过期的键，尽快尝试下一个数据库。 */
            if ((num = kvstoreSize(db->expires)) == 0) {
                db->avg_ttl = 0;
                break;
            }
            data.now = mstime();

            /* 主要的回收周期。扫描设置了过期时间的键，
             * 检查其中已过期的键。 */
            data.sampled = 0;
            data.expired = 0;

            if (num > config_keys_per_loop)
                num = config_keys_per_loop;

            /* 为了速度考虑，这里访问哈希表的底层表示：
             * 这使得此代码与 dict.c 耦合，但该部分十年来几乎没有变化。
             *
             * 注意，哈希表的某些位置可能为空，因此我们也需要一个关于
             * 扫描桶数量的停止条件。然而扫描空桶非常快：我们在缓存行中
             * 顺序扫描 NULL 指针数组，因此可以在相同时间内扫描比键更多的桶。 */
            long max_buckets = num*20;
            long checked_buckets = 0;

            int origin_ttl_samples = data.ttl_samples;

            while (data.sampled < num && checked_buckets < max_buckets) {
                db->expires_cursor = kvstoreScan(db->expires, db->expires_cursor, -1, expireScanCallback, isExpiryDictValidForSamplingCb, &data);
                if (db->expires_cursor == 0) {
                    db_done = 1;
                    break;
                }
                checked_buckets++;
            }
            total_expired += data.expired;
            total_sampled += data.sampled;

            /* 如果发现 TTL 尚未过期的键，我们需要更新一次平均 TTL 统计。 */
            if (data.ttl_samples - origin_ttl_samples > 0) update_avg_ttl_times++;

            /* 如果当前数据库扫描完成，或者陈旧键（逻辑过期但尚未回收）
             * 的比例在可接受范围内，则不重复当前数据库的周期。 */
            repeat = db_done ? 0 : (data.sampled == 0 || (data.expired * 100 / data.sampled) > config_cycle_acceptable_stale);

            /* 即使有很多键需要过期，我们也不能在这里永远阻塞。
             * 因此在经过给定的微秒数后，返回调用者，等待下一次
             * 主动过期周期。 */
            if ((iteration & 0xf) == 0 || !repeat) { /* 每 16 次迭代或即将退出时更新平均 TTL 统计。 */
                /* 更新此数据库的平均 TTL 统计，
                 * 因为可能即将达到时间限制。 */
                if (data.ttl_samples) {
                    long long avg_ttl = data.ttl_sum / data.ttl_samples;

                    /* 使用少量样本进行简单的移动平均计算。
                     * 当前估计值权重为 2%，之前的估计值权重为 98%。 */
                    if (db->avg_ttl == 0) {
                        db->avg_ttl = avg_ttl;
                    } else {
                        /* 原始代码如下：
                         * for (int i = 0; i < update_avg_ttl_times; i++) {
                         *   db->avg_ttl = (db->avg_ttl/50)*49 + (avg_ttl/50);
                         * }
                         * 我们可以将循环转换为等比数列求和。
                         * db->avg_ttl = db->avg_ttl * pow(0.98, update_avg_ttl_times) +
                         *                  avg_ttl / 50 * (pow(0.98, update_avg_ttl_times - 1) + ... + 1)
                         *             = db->avg_ttl * pow(0.98, update_avg_ttl_times) +
                         *                  avg_ttl * (1 - pow(0.98, update_avg_ttl_times))
                         *             = avg_ttl + (db->avg_ttl - avg_ttl) * pow(0.98, update_avg_ttl_times)
                         * 注意 update_avg_ttl_times 的值在 1 到 16 之间，
                         * 我们使用常量表来加速 pow(0.98, update_avg_ttl_times) 的计算。 */
                        db->avg_ttl = avg_ttl + (db->avg_ttl - avg_ttl) * avg_ttl_factor[update_avg_ttl_times - 1] ;
                    }
                    update_avg_ttl_times = 0;
                    data.ttl_sum = 0;
                    data.ttl_samples = 0;
                }
                if ((iteration & 0xf) == 0) { /* 每 16 次迭代检查一次时间限制。 */
                    elapsed = ustime()-start;
                    if (elapsed > timelimit) {
                        timelimit_exit = 1;
                        server.stat_expired_time_cap_reached_count++;
                        break;
                    }
                }
            }
        } while (repeat);
    }

    elapsed = ustime()-start;
    server.stat_expire_cycle_time_used += elapsed;
    latencyAddSampleIfNeeded("expire-cycle",elapsed/1000);

    /* 更新我们对已存在但尚未过期的键的估计。
     * 使用移动平均，本次样本占 5% 权重。 */
    double current_perc;
    if (total_sampled) {
        current_perc = (double)total_expired/total_sampled;
    } else
        current_perc = 0;
    server.stat_expired_stale_perc = (current_perc*0.05)+
                                     (server.stat_expired_stale_perc*0.95);
}

/*-----------------------------------------------------------------------------
 * 在可写从节点上创建的键的过期处理
 *
 * 通常从节点不处理过期操作：它们等待主节点合成 DEL 操作以保持一致性。
 * 但可写从节点是一个例外：如果一个键是在从节点上创建的，并且给它设置了
 * 过期时间，我们需要一种方式来过期这个键，因为主节点对此键一无所知。
 *
 * 为此，我们跟踪在从节点上创建的带有过期时间的键，并不时地调用
 * expireSlaveKeys() 函数来回收已过期的键。
 *
 * 注意，我们在此试图覆盖的用例是一个常见场景：将从节点设置为可写模式，
 * 以在从节点上执行耗时操作，这些操作主要用于以更加工的方式读取数据。
 * 例如：在临时键中执行集合交集操作，并设置过期时间以便用作缓存，
 * 避免每次都重新计算交集。
 *
 * 此实现目前并不完美，但比 3.2 版本中的键泄漏实现要好得多。
 *----------------------------------------------------------------------------*/

/* 该字典用于记住我们可能希望从从节点过期的键名称和数据库 ID。
 * 由于此功能不常使用，我们甚至不在启动时初始化数据库。
 * 我们将在该功能首次使用时（即调用 rememberSlaveKeyWithExpire() 时）
 * 进行初始化。
 *
 * 该字典使用 SDS 字符串作为哈希表键来表示键名，
 * 值是一个 64 位无符号整数，其中键可能存在的数据库对应的位设置为 1。
 * 目前，DB id > 63 的键不会被过期，但一个简单的修复方法是：
 * 当我们知道存在 DB ID 大于 63 的键时，将位图设置为 64 位无符号整数
 * 的最大值，并在这种情况下检查所有已配置的数据库。 */
dict *slaveKeysWithExpire = NULL;

/* 检查由主节点创建的带有过期时间的键集合，
 * 以判断它们是否应该被驱逐。 */
void expireSlaveKeys(void) {
    if (slaveKeysWithExpire == NULL ||
        dictSize(slaveKeysWithExpire) == 0) return;

    int cycles = 0, noexpire = 0;
    mstime_t start = mstime();
    while(1) {
        dictEntry *de = dictGetRandomKey(slaveKeysWithExpire);
        sds keyname = dictGetKey(de);
        uint64_t dbids = dictGetUnsignedIntegerVal(de);
        uint64_t new_dbids = 0;

        /* 针对值位图中设置的每一位所对应的数据库检查该键。 */
        int dbid = 0;
        while(dbids && dbid < server.dbnum) {
            if ((dbids & 1) != 0) {
                redisDb *db = server.db+dbid;
                dictEntry *expire = dbFindExpires(db, keyname);
                int expired = 0;

                if (expire &&
                    activeExpireCycleTryExpire(server.db+dbid,expire,start))
                {
                    expired = 1;
                    /* 传播 DEL 命令（可写副本不会向其他副本传播任何内容，
                     * 但可能会传播到 AOF）并触发模块钩子。 */
                    postExecutionUnitOperations();
                }

                /* 如果该键在此数据库中未过期，我们需要在新的位图值中
                 * 设置对应的位。循环结束时如果位图为零，则表示我们
                 * 不再需要跟踪此键。 */
                if (expire && !expired) {
                    noexpire++;
                    new_dbids |= (uint64_t)1 << dbid;
                }
            }
            dbid++;
            dbids >>= 1;
        }

        /* 将新的位图设置为键的值，存储在可写从节点上直接设置过期时间
         * 的键字典中。如果位图为零，则不再需要跟踪它。 */
        if (new_dbids)
            dictSetUnsignedIntegerVal(de,new_dbids);
        else
            dictDelete(slaveKeysWithExpire,keyname);

        /* 停止条件：连续发现 3 个无法过期的键，
         * 或者达到了时间限制。 */
        cycles++;
        if (noexpire > 3) break;
        if ((cycles % 64) == 0 && mstime()-start > 1) break;
        if (dictSize(slaveKeysWithExpire) == 0) break;
    }
}

/* 跟踪在可写从节点上下文中接收到 EXPIRE 或类似命令的键。 */
void rememberSlaveKeyWithExpire(redisDb *db, robj *key) {
    if (slaveKeysWithExpire == NULL) {
        static dictType dt = {
            dictSdsHash,                /* 哈希函数 */
            NULL,                       /* 键复制 */
            NULL,                       /* 值复制 */
            dictSdsKeyCompare,          /* 键比较 */
            dictSdsDestructor,          /* 键析构函数 */
            NULL,                       /* 值析构函数 */
            NULL                        /* 允许扩展 */
        };
        slaveKeysWithExpire = dictCreate(&dt);
    }
    if (db->id > 63) return;

    dictEntry *de = dictAddOrFind(slaveKeysWithExpire,key->ptr);
    /* 如果条目刚创建，将其设置为代表该键的 SDS 字符串的副本：
     * 我们不希望需要将这些键与主数据库同步。
     * expireSlaveKeys() 在扫描时会移除这些键。 */
    if (dictGetKey(de) == key->ptr) {
        dictSetKey(slaveKeysWithExpire, de, sdsdup(key->ptr));
        dictSetUnsignedIntegerVal(de,0);
    }

    uint64_t dbids = dictGetUnsignedIntegerVal(de);
    dbids |= (uint64_t)1 << db->id;
    dictSetUnsignedIntegerVal(de,dbids);
}

/* 返回我们正在跟踪的键数量。 */
size_t getSlaveKeyWithExpireCount(void) {
    if (slaveKeysWithExpire == NULL) return 0;
    return dictSize(slaveKeysWithExpire);
}

/* 移除哈希表中的键。当数据从服务器刷新时需要执行此操作。
 * 我们可能会从主节点收到同名/同数据库的新键，
 * 此时再对旧键执行过期操作已不合适。
 *
 * 注意：从技术上讲，我们应该处理单个数据库被刷新的情况，
 * 但这不值得做，因为在可写从节点和其主节点中使用同一组键名
 * 导致的竞态条件反正也会产生不一致。这只是一个尽力而为的操作。 */
void flushSlaveKeysWithExpireList(void) {
    if (slaveKeysWithExpire) {
        dictRelease(slaveKeysWithExpire);
        slaveKeysWithExpire = NULL;
    }
}

int checkAlreadyExpired(long long when) {
    /* 带有负 TTL 的 EXPIRE，或者带有过去时间戳的 EXPIREAT，
     * 在加载 AOF 或在从节点实例上下文中时，绝不应该作为 DEL 执行。
     *
     * 相反，我们把已经过期的键添加到数据库中（带上可能已过去的过期时间），
     * 然后等待主节点发送显式的 DEL 命令。 */
    return (when <= commandTimeSnapshot() && !server.loading && !server.masterhost);
}

#define EXPIRE_NX (1<<0) /* 仅当键没有过期时间时设置 */
#define EXPIRE_XX (1<<1) /* 仅当键已有过期时间时设置 */
#define EXPIRE_GT (1<<2) /* 仅当新过期时间大于当前值时设置 */
#define EXPIRE_LT (1<<3) /* 仅当新过期时间小于当前值时设置 */

/* 解析过期命令的附加标志参数
 *
 * 支持的标志：
 * - NX：仅当键没有过期时间时才设置过期时间
 * - XX：仅当键已有过期时间时才设置过期时间
 * - GT：仅当新过期时间大于当前过期时间时才设置
 * - LT：仅当新过期时间小于当前过期时间时才设置 */
int parseExtendedExpireArgumentsOrReply(client *c, int *flags) {
    int nx = 0, xx = 0, gt = 0, lt = 0;

    int j = 3;
    while (j < c->argc) {
        char *opt = c->argv[j]->ptr;
        if (!strcasecmp(opt,"nx")) {
            *flags |= EXPIRE_NX;
            nx = 1;
        } else if (!strcasecmp(opt,"xx")) {
            *flags |= EXPIRE_XX;
            xx = 1;
        } else if (!strcasecmp(opt,"gt")) {
            *flags |= EXPIRE_GT;
            gt = 1;
        } else if (!strcasecmp(opt,"lt")) {
            *flags |= EXPIRE_LT;
            lt = 1;
        } else {
            addReplyErrorFormat(c, "Unsupported option %s", opt);
            return C_ERR;
        }
        j++;
    }

    if ((nx && xx) || (nx && gt) || (nx && lt)) {
        addReplyError(c, "NX and XX, GT or LT options at the same time are not compatible");
        return C_ERR;
    }

    if (gt && lt) {
        addReplyError(c, "GT and LT options at the same time are not compatible");
        return C_ERR;
    }

    return C_OK;
}

/*-----------------------------------------------------------------------------
 * 过期命令
 *----------------------------------------------------------------------------*/

/* 这是 EXPIRE、PEXPIRE、EXPIREAT 和 PEXPIREAT 的通用命令实现。
 * 由于命令的第二个参数可能是相对值或绝对值，"basetime" 参数用于
 * 表示基准时间是什么（对于 *AT 变体命令为 0，对于相对过期命令
 * 则为当前时间）。
 *
 * unit 可以是 UNIT_SECONDS 或 UNIT_MILLISECONDS，仅用于 argv[2] 参数。
 * basetime 始终以毫秒为单位指定。
 *
 * 附加标志通过 parseExtendedExpireArguments 进行解析。 */
void expireGenericCommand(client *c, long long basetime, int unit) {
    robj *key = c->argv[1], *param = c->argv[2];
    long long when; /* 键过期的 Unix 时间（毫秒）。 */
    long long current_expire = -1;
    int flag = 0;

    /* 检查可选标志 */
    if (parseExtendedExpireArgumentsOrReply(c, &flag) != C_OK) {
        return;
    }

    if (getLongLongFromObjectOrReply(c, param, &when, NULL) != C_OK)
        return;

    /* EXPIRE 允许负数，但我们至少可以检测到
     * 单位转换或基准时间加法导致的溢出。 */
    if (unit == UNIT_SECONDS) {
        if (when > LLONG_MAX / 1000 || when < LLONG_MIN / 1000) {
            addReplyErrorExpireTime(c);
            return;
        }
        when *= 1000;
    }

    if (when > LLONG_MAX - basetime) {
        addReplyErrorExpireTime(c);
        return;
    }
    when += basetime;

    /* 键不存在，返回零。 */
    if (lookupKeyWrite(c->db,key) == NULL) {
        addReply(c,shared.czero);
        return;
    }

    if (flag) {
        current_expire = getExpire(c->db, key);

        /* 设置了 NX 选项，检查当前过期时间 */
        if (flag & EXPIRE_NX) {
            if (current_expire != -1) {
                addReply(c,shared.czero);
                return;
            }
        }

        /* 设置了 XX 选项，检查当前过期时间 */
        if (flag & EXPIRE_XX) {
            if (current_expire == -1) {
                /* 当键没有过期时间时回复 0 */
                addReply(c,shared.czero);
                return;
            }
        }

        /* 设置了 GT 选项，检查当前过期时间 */
        if (flag & EXPIRE_GT) {
            /* 当 current_expire 为 -1 时，我们认为它是无限 TTL，
             * 因此带 gt 的 expire 命令总是无法通过 GT 检查。 */
            if (when <= current_expire || current_expire == -1) {
                /* 当新过期时间不大于当前值时回复 0 */
                addReply(c,shared.czero);
                return;
            }
        }

        /* 设置了 LT 选项，检查当前过期时间 */
        if (flag & EXPIRE_LT) {
            /* 当 current_expire 为 -1 时，我们认为它是无限 TTL，
             * 但此时 'when' 仍可能为负数，因此如果键有过期时间
             * 且新值不小于当前值，则无法通过 LT 检查。 */
            if (current_expire != -1 && when >= current_expire) {
                /* 当新过期时间不小于当前值时回复 0 */
                addReply(c,shared.czero);
                return;
            }
        }
    }

    if (checkAlreadyExpired(when)) {
        robj *aux;

        int deleted = dbGenericDelete(c->db,key,server.lazyfree_lazy_expire,DB_FLAG_KEY_EXPIRED);
        serverAssertWithInfo(c,key,deleted);
        server.dirty++;

        /* 将此操作作为显式的 DEL 或 UNLINK 命令传播到 AOF 和从节点。 */
        aux = server.lazyfree_lazy_expire ? shared.unlink : shared.del;
        rewriteClientCommandVector(c,2,aux,key);
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
        addReply(c, shared.cone);
        return;
    } else {
        setExpire(c,c->db,key,when);
        addReply(c,shared.cone);
        /* 作为 PEXPIREAT 毫秒时间戳进行传播
         * 仅在命令不是 PEXPIREAT 时才重写命令参数 */
        if (c->cmd->proc != pexpireatCommand) {
            rewriteClientCommandArgument(c,0,shared.pexpireat);
        }

        /* 当与 argv[2] 参数相同时，避免创建额外的字符串对象 */
        if (basetime != 0 || unit == UNIT_SECONDS) {
            robj *when_obj = createStringObjectFromLongLong(when);
            rewriteClientCommandArgument(c,2,when_obj);
            decrRefCount(when_obj);
        }

        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",key,c->db->id);
        server.dirty++;
        return;
    }
}

/* EXPIRE key seconds [ NX | XX | GT | LT]
 * 以秒为单位设置键的过期时间（相对值）。 */
void expireCommand(client *c) {
    expireGenericCommand(c,commandTimeSnapshot(),UNIT_SECONDS);
}

/* EXPIREAT key unix-time-seconds [ NX | XX | GT | LT]
 * 设置键的过期时间为 Unix 时间戳（秒，绝对值）。 */
void expireatCommand(client *c) {
    expireGenericCommand(c,0,UNIT_SECONDS);
}

/* PEXPIRE key milliseconds [ NX | XX | GT | LT]
 * 以毫秒为单位设置键的过期时间（相对值）。 */
void pexpireCommand(client *c) {
    expireGenericCommand(c,commandTimeSnapshot(),UNIT_MILLISECONDS);
}

/* PEXPIREAT key unix-time-milliseconds [ NX | XX | GT | LT]
 * 设置键的过期时间为 Unix 时间戳（毫秒，绝对值）。 */
void pexpireatCommand(client *c) {
    expireGenericCommand(c,0,UNIT_MILLISECONDS);
}

/* TTL、PTTL、EXPIRETIME 和 PEXPIRETIME 的通用实现。
 * output_ms：是否以毫秒为单位输出（1=毫秒，0=秒）。
 * output_abs：是否输出绝对过期时间（1=绝对时间，0=相对 TTL）。 */
void ttlGenericCommand(client *c, int output_ms, int output_abs) {
    long long expire, ttl = -1;

    /* 如果键完全不存在，返回 -2 */
    if (lookupKeyReadWithFlags(c->db,c->argv[1],LOOKUP_NOTOUCH) == NULL) {
        addReplyLongLong(c,-2);
        return;
    }

    /* 键存在。如果没有设置过期时间则返回 -1，
     * 否则返回实际的 TTL 值。 */
    expire = getExpire(c->db,c->argv[1]);
    if (expire != -1) {
        ttl = output_abs ? expire : expire-commandTimeSnapshot();
        if (ttl < 0) ttl = 0;
    }
    if (ttl == -1) {
        addReplyLongLong(c,-1);
    } else {
        addReplyLongLong(c,output_ms ? ttl : ((ttl+500)/1000));
    }
}

/* TTL key - 获取键的剩余生存时间（秒）。 */
void ttlCommand(client *c) {
    ttlGenericCommand(c, 0, 0);
}

/* PTTL key - 获取键的剩余生存时间（毫秒）。 */
void pttlCommand(client *c) {
    ttlGenericCommand(c, 1, 0);
}

/* EXPIRETIME key - 获取键的过期 Unix 时间戳（秒）。 */
void expiretimeCommand(client *c) {
    ttlGenericCommand(c, 0, 1);
}

/* PEXPIRETIME key - 获取键的过期 Unix 时间戳（毫秒）。 */
void pexpiretimeCommand(client *c) {
    ttlGenericCommand(c, 1, 1);
}

/* PERSIST key - 移除键的过期时间，使其持久化。 */
void persistCommand(client *c) {
    if (lookupKeyWrite(c->db,c->argv[1])) {
        if (removeExpire(c->db,c->argv[1])) {
            signalModifiedKey(c,c->db,c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"persist",c->argv[1],c->db->id);
            addReply(c,shared.cone);
            server.dirty++;
        } else {
            addReply(c,shared.czero);
        }
    } else {
        addReply(c,shared.czero);
    }
}

/* TOUCH key1 [key2 key3 ... keyN] - 触摸指定的键，返回存在的键数量。 */
void touchCommand(client *c) {
    int touched = 0;
    for (int j = 1; j < c->argc; j++)
        if (lookupKeyRead(c->db,c->argv[j]) != NULL) touched++;
    addReplyLongLong(c,touched);
}
