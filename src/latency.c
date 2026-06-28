/* 延迟监控器允许通过 LATENCY 命令轻松观察 Redis 实例中的延迟来源。
 * 不同的延迟来源会被监控，如磁盘 I/O、命令执行、fork 系统调用等。
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2014-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "hdr_histogram.h"

/* 延迟事件的字典类型定义 */
int dictStringKeyCompare(dict *d, const void *key1, const void *key2) {
    UNUSED(d);
    return strcmp(key1,key2) == 0;
}

uint64_t dictStringHash(const void *key) {
    return dictGenHashFunction(key, strlen(key));
}

void dictVanillaFree(dict *d, void *val);

dictType latencyTimeSeriesDictType = {
    dictStringHash,             /* 哈希函数 */
    NULL,                       /* 键复制 */
    NULL,                       /* 值复制 */
    dictStringKeyCompare,       /* 键比较 */
    dictVanillaFree,            /* 键析构函数 */
    dictVanillaFree,            /* 值析构函数 */
    NULL                        /* 允许扩展 */
};

/* ------------------------- 工具函数 --------------------------------------- */

/* 报告 smap 中 AnonHugePages 的大小（字节）。如果返回值非零，
 * 说明进程启用了 THP（透明大页）支持，可能会出现内存使用 /
 * 延迟问题。 */
int THPGetAnonHugePagesSize(void) {
    return zmalloc_get_smap_bytes_by_field("AnonHugePages:",-1);
}

/* ---------------------------- 延迟 API ------------------------------------ */

/* 延迟监控器初始化。只需要创建时间序列的字典，每个时间序列
 * 按需创建，以避免维护一个固定的列表。 */
void latencyMonitorInit(void) {
    server.latency_events = dictCreate(&latencyTimeSeriesDictType);
}

/* 将指定的采样值添加到指定的时间序列 "event" 中。
 * 此函数通常通过 latencyAddSampleIfNeeded() 调用，
 * 那是一个宏，仅在延迟高于
 * server.latency_monitor_threshold 时才添加采样。 */
void latencyAddSample(const char *event, mstime_t latency) {
    struct latencyTimeSeries *ts = dictFetchValue(server.latency_events,event);
    time_t now = time(NULL);
    int prev;

    /* 如果时间序列不存在则创建 */
    if (ts == NULL) {
        ts = zmalloc(sizeof(*ts));
        ts->idx = 0;
        ts->max = 0;
        memset(ts->samples,0,sizeof(ts->samples));
        dictAdd(server.latency_events,zstrdup(event),ts);
    }

    if (latency > ts->max) ts->max = latency;

    /* 如果上一个采样在同一秒内，当新延迟大于旧值时更新旧采样，
     * 否则直接返回。 */
    prev = (ts->idx + LATENCY_TS_LEN - 1) % LATENCY_TS_LEN;
    if (ts->samples[prev].time == now) {
        if (latency > ts->samples[prev].latency)
            ts->samples[prev].latency = latency;
        return;
    }

    ts->samples[ts->idx].time = now;
    ts->samples[ts->idx].latency = latency;

    ts->idx++;
    if (ts->idx == LATENCY_TS_LEN) ts->idx = 0;
}

/* 重置指定事件的数据，若 'event' 为 NULL 则重置所有事件数据。
 *
 * 注意：即使 event_to_reset 不为 NULL，复杂度也是 O(N)，
 * 因为这样代码更简单，而且事件的最大数量是固定的且很小。 */
int latencyResetEvent(char *event_to_reset) {
    dictIterator *di;
    dictEntry *de;
    int resets = 0;

    di = dictGetSafeIterator(server.latency_events);
    while((de = dictNext(di)) != NULL) {
        char *event = dictGetKey(de);

        if (event_to_reset == NULL || strcasecmp(event,event_to_reset) == 0) {
            dictDelete(server.latency_events, event);
            resets++;
        }
    }
    dictReleaseIterator(di);
    return resets;
}

/* ------------------------ 延迟报告（诊断）--------------------------------- */

/* 分析给定事件的可用采样，返回填充了不同指标（平均值、MAD、
 * 最小值、最大值等）的结构体。详情请查看 latency.h 中
 * struct latencyStats 的定义。
 * 如果指定事件没有元素，则结构体用零值填充。 */
void analyzeLatencyForEvent(char *event, struct latencyStats *ls) {
    struct latencyTimeSeries *ts = dictFetchValue(server.latency_events,event);
    int j;
    uint64_t sum;

    ls->all_time_high = ts ? ts->max : 0;
    ls->avg = 0;
    ls->min = 0;
    ls->max = 0;
    ls->mad = 0;
    ls->samples = 0;
    ls->period = 0;
    if (!ts) return;

    /* 第一遍遍历，填充除 MAD 之外的所有指标 */
    sum = 0;
    for (j = 0; j < LATENCY_TS_LEN; j++) {
        if (ts->samples[j].time == 0) continue;
        ls->samples++;
        if (ls->samples == 1) {
            ls->min = ls->max = ts->samples[j].latency;
        } else {
            if (ls->min > ts->samples[j].latency)
                ls->min = ts->samples[j].latency;
            if (ls->max < ts->samples[j].latency)
                ls->max = ts->samples[j].latency;
        }
        sum += ts->samples[j].latency;

        /* 追踪 ls->period 中最旧的事件时间 */
        if (ls->period == 0 || ts->samples[j].time < ls->period)
            ls->period = ts->samples[j].time;
    }

    /* 目前 avg 实际上是延迟总和，period 是最旧事件时间。
     * 我们需要将前者转为平均值，将后者转为秒数范围。 */
    if (ls->samples) {
        ls->avg = sum / ls->samples;
        ls->period = time(NULL) - ls->period;
        if (ls->period == 0) ls->period = 1;
    }

    /* 第二遍遍历，计算 MAD（平均绝对偏差） */
    sum = 0;
    for (j = 0; j < LATENCY_TS_LEN; j++) {
        int64_t delta;

        if (ts->samples[j].time == 0) continue;
        delta = (int64_t)ls->avg - ts->samples[j].latency;
        if (delta < 0) delta = -delta;
        sum += delta;
    }
    if (ls->samples) ls->mad = sum / ls->samples;
}

/* 为此 Redis 实例创建一份人类可读的延迟事件报告。 */
sds createLatencyReport(void) {
    sds report = sdsempty();
    int advise_better_vm = 0;       /* 使用更好的虚拟机 */
    int advise_slowlog_enabled = 0; /* 启用慢查询日志 */
    int advise_slowlog_tuning = 0;  /* 重新配置慢查询日志 */
    int advise_slowlog_inspect = 0; /* 检查慢查询日志 */
    int advise_disk_contention = 0; /* 尝试降低磁盘争用 */
    int advise_scheduler = 0;       /* 固有延迟 */
    int advise_data_writeback = 0;  /* data=writeback */
    int advise_no_appendfsync = 0;  /* 重写时不执行 fsync */
    int advise_local_disk = 0;      /* 避免使用远程磁盘 */
    int advise_ssd = 0;             /* 使用 SSD 磁盘 */
    int advise_write_load_info = 0; /* 打印 AOF 和写负载信息 */
    int advise_hz = 0;              /* 使用更高的 HZ 值 */
    int advise_large_objects = 0;   /* 大对象的删除 */
    int advise_mass_eviction = 0;   /* 避免大量键被驱逐 */
    int advise_relax_fsync_policy = 0; /* appendfsync always 太慢 */
    int advise_disable_thp = 0;     /* 检测到 AnonHugePages */
    int advices = 0;

    /* 如果延迟引擎已禁用且看起来从未启用过，则立即返回 */
    if (dictSize(server.latency_events) == 0 &&
        server.latency_monitor_threshold == 0)
    {
        report = sdscat(report,"I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this Redis instance. You may use \"CONFIG SET latency-monitor-threshold <milliseconds>.\" in order to enable it. If we weren't in a deep space mission I'd suggest to take a look at https://redis.io/topics/latency-monitor.\n");
        return report;
    }

    /* 显示所有事件统计信息，并根据值为每个事件添加相关评论 */
    dictIterator *di;
    dictEntry *de;
    int eventnum = 0;

    di = dictGetSafeIterator(server.latency_events);
    while((de = dictNext(di)) != NULL) {
        char *event = dictGetKey(de);
        struct latencyTimeSeries *ts = dictGetVal(de);
        struct latencyStats ls;

        if (ts == NULL) continue;
        eventnum++;
        if (eventnum == 1) {
            report = sdscat(report,"Dave, I have observed latency spikes in this Redis instance. You don't mind talking about it, do you Dave?\n\n");
        }
        analyzeLatencyForEvent(event,&ls);

        report = sdscatprintf(report,
            "%d. %s: %d latency spikes (average %lums, mean deviation %lums, period %.2f sec). Worst all time event %lums.",
            eventnum, event,
            ls.samples,
            (unsigned long) ls.avg,
            (unsigned long) ls.mad,
            (double) ls.period/ls.samples,
            (unsigned long) ts->max);

        /* Fork 事件 */
        if (!strcasecmp(event,"fork")) {
            char *fork_quality;
            if (server.stat_fork_rate < 10) {
                fork_quality = "terrible";
                advise_better_vm = 1;
                advices++;
            } else if (server.stat_fork_rate < 25) {
                fork_quality = "poor";
                advise_better_vm = 1;
                advices++;
            } else if (server.stat_fork_rate < 100) {
                fork_quality = "good";
            } else {
                fork_quality = "excellent";
            }
            report = sdscatprintf(report,
                " Fork rate is %.2f GB/sec (%s).", server.stat_fork_rate,
                fork_quality);
        }

        /* 命令相关事件 */
        if (!strcasecmp(event,"command")) {
            if (server.slowlog_log_slower_than < 0 || server.slowlog_max_len == 0) {
                advise_slowlog_enabled = 1;
                advices++;
            } else if (server.slowlog_log_slower_than/1000 >
                       server.latency_monitor_threshold)
            {
                advise_slowlog_tuning = 1;
                advices++;
            }
            advise_slowlog_inspect = 1;
            advise_large_objects = 1;
            advices += 2;
        }

        /* 快速命令事件 */
        if (!strcasecmp(event,"fast-command")) {
            advise_scheduler = 1;
            advices++;
        }

        /* AOF 和 I/O 相关事件 */
        if (!strcasecmp(event,"aof-write-pending-fsync")) {
            advise_local_disk = 1;
            advise_disk_contention = 1;
            advise_ssd = 1;
            advise_data_writeback = 1;
            advices += 4;
        }

        if (!strcasecmp(event,"aof-write-active-child")) {
            advise_no_appendfsync = 1;
            advise_data_writeback = 1;
            advise_ssd = 1;
            advices += 3;
        }

        if (!strcasecmp(event,"aof-write-alone")) {
            advise_local_disk = 1;
            advise_data_writeback = 1;
            advise_ssd = 1;
            advices += 3;
        }

        if (!strcasecmp(event,"aof-fsync-always")) {
            advise_relax_fsync_policy = 1;
            advices++;
        }

        if (!strcasecmp(event,"aof-fstat") ||
            !strcasecmp(event,"rdb-unlink-temp-file")) {
            advise_disk_contention = 1;
            advise_local_disk = 1;
            advices += 2;
        }

        if (!strcasecmp(event,"aof-rewrite-diff-write") ||
            !strcasecmp(event,"aof-rename")) {
            advise_write_load_info = 1;
            advise_data_writeback = 1;
            advise_ssd = 1;
            advise_local_disk = 1;
            advices += 4;
        }

        /* 过期周期事件 */
        if (!strcasecmp(event,"expire-cycle")) {
            advise_hz = 1;
            advise_large_objects = 1;
            advices += 2;
        }

        /* 驱逐周期事件 */
        if (!strcasecmp(event,"eviction-del")) {
            advise_large_objects = 1;
            advices++;
        }

        if (!strcasecmp(event,"eviction-cycle")) {
            advise_mass_eviction = 1;
            advices++;
        }

        report = sdscatlen(report,"\n",1);
    }
    dictReleaseIterator(di);

    /* 添加非事件相关的建议 */
    if (THPGetAnonHugePagesSize() > 0) {
        advise_disable_thp = 1;
        advices++;
    }

    if (eventnum == 0 && advices == 0) {
        report = sdscat(report,"Dave, no latency spike was observed during the lifetime of this Redis instance, not in the slightest bit. I honestly think you ought to sit down calmly, take a stress pill, and think things over.\n");
    } else if (eventnum > 0 && advices == 0) {
        report = sdscat(report,"\nWhile there are latency events logged, I'm not able to suggest any easy fix. Please use the Redis community to get some help, providing this report in your help request.\n");
    } else {
        /* 添加所有累积的建议 */

        /* 更好的虚拟机 */
        report = sdscat(report,"\nI have a few advices for you:\n\n");
        if (advise_better_vm) {
            report = sdscat(report,"- If you are using a virtual machine, consider upgrading it with a faster one using a hypervisior that provides less latency during fork() calls. Xen is known to have poor fork() performance. Even in the context of the same VM provider, certain kinds of instances can execute fork faster than others.\n");
        }

        /* 慢查询日志 */
        if (advise_slowlog_enabled) {
            report = sdscatprintf(report,"- There are latency issues with potentially slow commands you are using. Try to enable the Slow Log Redis feature using the command 'CONFIG SET slowlog-log-slower-than %llu'. If the Slow log is disabled Redis is not able to log slow commands execution for you.\n", (unsigned long long)server.latency_monitor_threshold*1000);
        }

        if (advise_slowlog_tuning) {
            report = sdscatprintf(report,"- Your current Slow Log configuration only logs events that are slower than your configured latency monitor threshold. Please use 'CONFIG SET slowlog-log-slower-than %llu'.\n", (unsigned long long)server.latency_monitor_threshold*1000);
        }

        if (advise_slowlog_inspect) {
            report = sdscat(report,"- Check your Slow Log to understand what are the commands you are running which are too slow to execute. Please check https://redis.io/commands/slowlog for more information.\n");
        }

        /* 固有延迟 */
        if (advise_scheduler) {
            report = sdscat(report,"- The system is slow to execute Redis code paths not containing system calls. This usually means the system does not provide Redis CPU time to run for long periods. You should try to:\n"
            "  1) Lower the system load.\n"
            "  2) Use a computer / VM just for Redis if you are running other software in the same system.\n"
            "  3) Check if you have a \"noisy neighbour\" problem.\n"
            "  4) Check with 'redis-cli --intrinsic-latency 100' what is the intrinsic latency in your system.\n"
            "  5) Check if the problem is allocator-related by recompiling Redis with MALLOC=libc, if you are using Jemalloc. However this may create fragmentation problems.\n");
        }

        /* AOF / 磁盘延迟 */
        if (advise_local_disk) {
            report = sdscat(report,"- It is strongly advised to use local disks for persistence, especially if you are using AOF. Remote disks provided by platform-as-a-service providers are known to be slow.\n");
        }

        if (advise_ssd) {
            report = sdscat(report,"- SSD disks are able to reduce fsync latency, and total time needed for snapshotting and AOF log rewriting (resulting in smaller memory usage). With extremely high write load SSD disks can be a good option. However Redis should perform reasonably with high load using normal disks. Use this advice as a last resort.\n");
        }

        if (advise_data_writeback) {
            report = sdscat(report,"- Mounting ext3/4 filesystems with data=writeback can provide a performance boost compared to data=ordered, however this mode of operation provides less guarantees, and sometimes it can happen that after a hard crash the AOF file will have a half-written command at the end and will require to be repaired before Redis restarts.\n");
        }

        if (advise_disk_contention) {
            report = sdscat(report,"- Try to lower the disk contention. This is often caused by other disk intensive processes running in the same computer (including other Redis instances).\n");
        }

        if (advise_no_appendfsync) {
            report = sdscat(report,"- Assuming from the point of view of data safety this is viable in your environment, you could try to enable the 'no-appendfsync-on-rewrite' option, so that fsync will not be performed while there is a child rewriting the AOF file or producing an RDB file (the moment where there is high disk contention).\n");
        }

        if (advise_relax_fsync_policy && server.aof_fsync == AOF_FSYNC_ALWAYS) {
            report = sdscat(report,"- Your fsync policy is set to 'always'. It is very hard to get good performances with such a setup, if possible try to relax the fsync policy to 'onesec'.\n");
        }

        if (advise_write_load_info) {
            report = sdscat(report,"- Latency during the AOF atomic rename operation or when the final difference is flushed to the AOF file at the end of the rewrite, sometimes is caused by very high write load, causing the AOF buffer to get very large. If possible try to send less commands to accomplish the same work, or use Lua scripts to group multiple operations into a single EVALSHA call.\n");
        }

        if (advise_hz && server.hz < 100) {
            report = sdscat(report,"- In order to make the Redis keys expiring process more incremental, try to set the 'hz' configuration parameter to 100 using 'CONFIG SET hz 100'.\n");
        }

        if (advise_large_objects) {
            report = sdscat(report,"- Deleting, expiring or evicting (because of maxmemory policy) large objects is a blocking operation. If you have very large objects that are often deleted, expired, or evicted, try to fragment those objects into multiple smaller objects.\n");
        }

        if (advise_mass_eviction) {
            report = sdscat(report,"- Sudden changes to the 'maxmemory' setting via 'CONFIG SET', or allocation of large objects via sets or sorted sets intersections, STORE option of SORT, Redis Cluster large keys migrations (RESTORE command), may create sudden memory pressure forcing the server to block trying to evict keys. \n");
        }

        if (advise_disable_thp) {
            report = sdscat(report,"- I detected a non zero amount of anonymous huge pages used by your process. This creates very serious latency events in different conditions, especially when Redis is persisting on disk. To disable THP support use the command 'echo never > /sys/kernel/mm/transparent_hugepage/enabled', make sure to also add it into /etc/rc.local so that the command will be executed again after a reboot. Note that even if you have already disabled THP, you still need to restart the Redis process to get rid of the huge pages already created.\n");
        }
    }

    return report;
}

/* ---------------------- 延迟命令实现 --------------------------------------- */

/* latencyCommand() 的辅助函数，用于生成一个时间桶映射，
 * 每个桶代表一个延迟范围，从 1 纳秒到大约 1 秒。
 * 每个桶覆盖前一个桶范围的两倍。
 * 空桶不会被打印。
 * 超过 1 秒的都视为 +Inf。
 * 最多会有 log2(1000000000)=30 个桶 */
void fillCommandCDF(client *c, struct hdr_histogram* histogram) {
    addReplyMapLen(c,2);
    addReplyBulkCString(c,"calls");
    addReplyLongLong(c,(long long) histogram->total_count);
    addReplyBulkCString(c,"histogram_usec");
    void *replylen = addReplyDeferredLen(c);
    int samples = 0;
    struct hdr_iter iter;
    hdr_iter_log_init(&iter,histogram,1024,2);
    int64_t previous_count = 0;
    while (hdr_iter_next(&iter)) {
        const int64_t micros = iter.highest_equivalent_value / 1000;
        const int64_t cumulative_count = iter.cumulative_count;
        if(cumulative_count > previous_count){
            addReplyLongLong(c,(long long) micros);
            addReplyLongLong(c,(long long) cumulative_count);
            samples++;
        }
        previous_count = cumulative_count;
    }
    setDeferredMapLen(c,replylen,samples);
}

/* latencyCommand() 的辅助函数，为所有命令生成
 * 每个命令的延迟累积分布。 */
void latencyAllCommandsFillCDF(client *c, dict *commands, int *command_with_data) {
    dictIterator *di = dictGetSafeIterator(commands);
    dictEntry *de;
    struct redisCommand *cmd;

    while((de = dictNext(di)) != NULL) {
        cmd = (struct redisCommand *) dictGetVal(de);
        if (cmd->latency_histogram) {
            addReplyBulkCBuffer(c, cmd->fullname, sdslen(cmd->fullname));
            fillCommandCDF(c, cmd->latency_histogram);
            (*command_with_data)++;
        }

        if (cmd->subcommands) {
            latencyAllCommandsFillCDF(c, cmd->subcommands_dict, command_with_data);
        }
    }
    dictReleaseIterator(di);
}

/* latencyCommand() 的辅助函数，为指定的命令集生成
 * 每个命令的延迟累积分布。 */
void latencySpecificCommandsFillCDF(client *c) {
    void *replylen = addReplyDeferredLen(c);
    int command_with_data = 0;
    for (int j = 2; j < c->argc; j++){
        struct redisCommand *cmd = lookupCommandBySds(c->argv[j]->ptr);
        /* 如果命令不存在则跳过该回复 */
        if (cmd == NULL) {
            continue;
        }

        if (cmd->latency_histogram) {
            addReplyBulkCBuffer(c, cmd->fullname, sdslen(cmd->fullname));
            fillCommandCDF(c, cmd->latency_histogram);
            command_with_data++;
        }

        if (cmd->subcommands_dict) {
            dictEntry *de;
            dictIterator *di = dictGetSafeIterator(cmd->subcommands_dict);

            while ((de = dictNext(di)) != NULL) {
                struct redisCommand *sub = dictGetVal(de);
                if (sub->latency_histogram) {
                    addReplyBulkCBuffer(c, sub->fullname, sdslen(sub->fullname));
                    fillCommandCDF(c, sub->latency_histogram);
                    command_with_data++;
                }
            }
            dictReleaseIterator(di);
        }
    }
    setDeferredMapLen(c,replylen,command_with_data);
}

/* latencyCommand() 的辅助函数，为指定时间序列中内存里
 * 的所有采样生成时间-延迟回复。 */
void latencyCommandReplyWithSamples(client *c, struct latencyTimeSeries *ts) {
    void *replylen = addReplyDeferredLen(c);
    int samples = 0, j;

    for (j = 0; j < LATENCY_TS_LEN; j++) {
        int i = (ts->idx + j) % LATENCY_TS_LEN;

        if (ts->samples[i].time == 0) continue;
        addReplyArrayLen(c,2);
        addReplyLongLong(c,ts->samples[i].time);
        addReplyLongLong(c,ts->samples[i].latency);
        samples++;
    }
    setDeferredArrayLen(c,replylen,samples);
}

/* latencyCommand() 的辅助函数，为 LATEST 子命令生成回复，
 * 列出迄今为止注册的每种事件类型的最新延迟采样。 */
void latencyCommandReplyWithLatestEvents(client *c) {
    dictIterator *di;
    dictEntry *de;

    addReplyArrayLen(c,dictSize(server.latency_events));
    di = dictGetIterator(server.latency_events);
    while((de = dictNext(di)) != NULL) {
        char *event = dictGetKey(de);
        struct latencyTimeSeries *ts = dictGetVal(de);
        int last = (ts->idx + LATENCY_TS_LEN - 1) % LATENCY_TS_LEN;

        addReplyArrayLen(c,4);
        addReplyBulkCString(c,event);
        addReplyLongLong(c,ts->samples[last].time);
        addReplyLongLong(c,ts->samples[last].latency);
        addReplyLongLong(c,ts->max);
    }
    dictReleaseIterator(di);
}

#define LATENCY_GRAPH_COLS 80
sds latencyCommandGenSparkeline(char *event, struct latencyTimeSeries *ts) {
    int j;
    struct sequence *seq = createSparklineSequence();
    sds graph = sdsempty();
    uint32_t min = 0, max = 0;

    for (j = 0; j < LATENCY_TS_LEN; j++) {
        int i = (ts->idx + j) % LATENCY_TS_LEN;
        int elapsed;
        char buf[64];

        if (ts->samples[i].time == 0) continue;
        /* 更新最小值和最大值 */
        if (seq->length == 0) {
            min = max = ts->samples[i].latency;
        } else {
            if (ts->samples[i].latency > max) max = ts->samples[i].latency;
            if (ts->samples[i].latency < min) min = ts->samples[i].latency;
        }
        /* 使用事件发生前的秒/分/小时/天数作为标签 */
        elapsed = time(NULL) - ts->samples[i].time;
        if (elapsed < 60)
            snprintf(buf,sizeof(buf),"%ds",elapsed);
        else if (elapsed < 3600)
            snprintf(buf,sizeof(buf),"%dm",elapsed/60);
        else if (elapsed < 3600*24)
            snprintf(buf,sizeof(buf),"%dh",elapsed/3600);
        else
            snprintf(buf,sizeof(buf),"%dd",elapsed/(3600*24));
        sparklineSequenceAddSample(seq,ts->samples[i].latency,buf);
    }

    graph = sdscatprintf(graph,
        "%s - high %lu ms, low %lu ms (all time high %lu ms)\n", event,
        (unsigned long) max, (unsigned long) min, (unsigned long) ts->max);
    for (j = 0; j < LATENCY_GRAPH_COLS; j++)
        graph = sdscatlen(graph,"-",1);
    graph = sdscatlen(graph,"\n",1);
    graph = sparklineRender(graph,seq,LATENCY_GRAPH_COLS,4,SPARKLINE_FILL);
    freeSparklineSequence(seq);
    return graph;
}

/* LATENCY 命令实现。
 *
 * LATENCY HISTORY: 返回指定事件的时间-延迟采样。
 * LATENCY LATEST: 返回所有事件类别的最新延迟。
 * LATENCY DOCTOR: 返回实例延迟的人类可读分析报告。
 * LATENCY GRAPH: 提供指定事件延迟的 ASCII 图形。
 * LATENCY RESET: 重置指定事件的数据，若未提供事件则重置所有数据。
 * LATENCY HISTOGRAM: 以直方图格式返回指定命令名称的延迟累积分布。
 */
void latencyCommand(client *c) {
    struct latencyTimeSeries *ts;

    if (!strcasecmp(c->argv[1]->ptr,"history") && c->argc == 3) {
        /* LATENCY HISTORY <event> - 返回指定事件的历史采样 */
        ts = dictFetchValue(server.latency_events,c->argv[2]->ptr);
        if (ts == NULL) {
            addReplyArrayLen(c,0);
        } else {
            latencyCommandReplyWithSamples(c,ts);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"graph") && c->argc == 3) {
        /* LATENCY GRAPH <event> - 返回指定事件的 ASCII 图形 */
        sds graph;
        dictEntry *de;
        char *event;

        de = dictFind(server.latency_events,c->argv[2]->ptr);
        if (de == NULL) goto nodataerr;
        ts = dictGetVal(de);
        event = dictGetKey(de);

        graph = latencyCommandGenSparkeline(event,ts);
        addReplyVerbatim(c,graph,sdslen(graph),"txt");
        sdsfree(graph);
    } else if (!strcasecmp(c->argv[1]->ptr,"latest") && c->argc == 2) {
        /* LATENCY LATEST - 返回最新延迟采样 */
        latencyCommandReplyWithLatestEvents(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"doctor") && c->argc == 2) {
        /* LATENCY DOCTOR - 生成延迟诊断报告 */
        sds report = createLatencyReport();

        addReplyVerbatim(c,report,sdslen(report),"txt");
        sdsfree(report);
    } else if (!strcasecmp(c->argv[1]->ptr,"reset") && c->argc >= 2) {
        /* LATENCY RESET - 重置延迟数据 */
        if (c->argc == 2) {
            addReplyLongLong(c,latencyResetEvent(NULL));
        } else {
            int j, resets = 0;

            for (j = 2; j < c->argc; j++)
                resets += latencyResetEvent(c->argv[j]->ptr);
            addReplyLongLong(c,resets);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"histogram") && c->argc >= 2) {
        /* LATENCY HISTOGRAM - 返回延迟直方图 */
        if (c->argc == 2) {
            int command_with_data = 0;
            void *replylen = addReplyDeferredLen(c);
            latencyAllCommandsFillCDF(c, server.commands, &command_with_data);
            setDeferredMapLen(c, replylen, command_with_data);
        } else {
            latencySpecificCommandsFillCDF(c);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"help") && c->argc == 2) {
        const char *help[] = {
"DOCTOR",
"    Return a human readable latency analysis report.",
"GRAPH <event>",
"    Return an ASCII latency graph for the <event> class.",
"HISTORY <event>",
"    Return time-latency samples for the <event> class.",
"LATEST",
"    Return the latest latency samples for all events.",
"RESET [<event> ...]",
"    Reset latency data of one or more <event> classes.",
"    (default: reset all data for all event classes)",
"HISTOGRAM [COMMAND ...]",
"    Return a cumulative distribution of latencies in the format of a histogram for the specified command names.",
"    If no commands are specified then all histograms are replied.",
NULL
        };
        addReplyHelp(c, help);
    } else {
        addReplySubcommandSyntaxError(c);
    }
    return;

nodataerr:
    /* 当用户请求的事件没有延迟信息时的常见错误 */
    addReplyErrorFormat(c,
        "No samples available for event '%s'", (char*) c->argv[2]->ptr);
}

/* 添加持续时间采样到指定类型的统计中 */
void durationAddSample(int type, monotime duration) {
    if (type >= EL_DURATION_TYPE_NUM) {
        return;
    }
    durationStats* ds = &server.duration_stats[type];
    ds->cnt++;
    ds->sum += duration;
    if (duration > ds->max) {
        ds->max = duration;
    }
}
