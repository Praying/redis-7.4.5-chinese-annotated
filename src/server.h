/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Copyright (c) 2024-present, Valkey contributors.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

/*
 * Redis 服务器核心头文件
 *
 * 本文件定义了 Redis 服务器的核心数据结构、常量、宏和函数原型。
 * 包含了服务器运行所需的所有主要组件声明：
 * - 服务器全局状态 (redisServer)
 * - 客户端连接状态 (client)
 * - Redis 对象系统 (redisObject)
 * - 数据库结构 (redisDb)
 * - 命令表和命令处理
 * - 复制 (replication) 相关结构
 * - 集群 (cluster) 相关结构
 * - 发布/订阅 (pub/sub) 相关结构
 * - 模块 (module) 系统接口
 * - 持久化 (RDB/AOF) 相关定义
 * - 内存管理和淘汰策略
 * - ACL (访问控制列表) 系统
 */

#ifndef __REDIS_H
#define __REDIS_H

#include "fmacros.h"        /* 功能测试宏定义 */
#include "config.h"         /* 编译配置 (autoconf 生成) */
#include "solarisfixes.h"   /* Solaris 平台兼容性修复 */
#include "rio.h"            /* Redis I/O 抽象层，统一文件和缓冲区读写 */
#include "atomicvar.h"      /* 原子变量操作封装 */
#include "commands.h"       /* 命令表定义 (自动生成) */

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <lua.h>
#include <signal.h>

#ifdef HAVE_LIBSYSTEMD
#include <systemd/sd-daemon.h>
#endif

#ifndef static_assert
#define static_assert(expr, lit) extern char __static_assert_failure[(expr) ? 1:-1]
#endif

typedef long long mstime_t; /* 毫秒时间类型 */
typedef long long ustime_t; /* 微秒时间类型 */

#include "ae.h"      /* 事件驱动编程库，Redis 事件循环的核心 */
#include "sds.h"     /* 动态安全字符串 (Simple Dynamic Strings) */
#include "mstr.h"    /* 不可变字符串，可附带元数据 (用于 hash field) */
#include "ebuckets.h" /* 过期时间数据结构，用于管理 key/field 的过期 */
#include "dict.h"    /* 哈希表实现 */
#include "kvstore.h" /* 基于 slot 的哈希表，用于集群模式下的 keyspace */
#include "adlist.h"  /* 双向链表 */
#include "zmalloc.h" /* 内存分配封装，可追踪总内存使用量 */
#include "anet.h"    /* 网络编程工具库，简化 socket 操作 */
#include "version.h" /* 版本宏定义 */
#include "util.h"    /* 通用工具函数集 */
#include "latency.h" /* 延迟监控 API */
#include "sparkline.h" /* ASCII 迷你图表 API，用于 latency 命令输出 */
#include "quicklist.h"  /* 快速列表：由多个 N 元素扁平数组组成的链表，
                           是 List 类型的底层编码 */
#include "rax.h"     /* 基数树 (Radix Tree)，用于前缀查找 */
#include "connection.h" /* 连接抽象层，支持 TCP/TLS/Unix socket */

#define REDISMODULE_CORE 1
typedef struct redisObject robj;
#include "redismodule.h"    /* Redis modules API defines. */

/* Following includes allow test functions to be called from Redis main() */
#include "zipmap.h"
#include "ziplist.h" /* Compact list data structure */
#include "sha1.h"
#include "endianconv.h"
#include "crc64.h"

struct hdr_histogram;

/* 辅助宏 */
#define numElements(x) (sizeof(x)/sizeof((x)[0]))  /* 计算数组元素个数 */

/* 最小值/最大值 */
#undef min
#undef max
#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))

/* 通过成员地址获取外层结构体指针 (container_of 模式) */
#define redis_member2struct(struct_name, member_name, member_addr) \
            ((struct_name *)((char*)member_addr - offsetof(struct_name, member_name)))

/* 通用返回值/错误码 */
#define C_OK                    0    /* 操作成功 */
#define C_ERR                   -1   /* 操作失败 */

/* ============================================================
 * 服务器静态配置常量
 * 这些是编译时确定的默认值，部分可在运行时通过配置文件覆盖
 * ============================================================ */

#define CONFIG_DEFAULT_HZ        10             /* serverCron() 默认每秒调用次数 */
#define CONFIG_MIN_HZ            1              /* 最小 Hz 值 */
#define CONFIG_MAX_HZ            500            /* 最大 Hz 值 */
#define MAX_CLIENTS_PER_CLOCK_TICK 200          /* 每个时钟周期最多处理的客户端数，Hz 会据此自适应调整 */
#define CRON_DBS_PER_CALL 16                    /* serverCron() 每次调用处理的数据库数量 */
#define CRON_DICTS_PER_DB 16                    /* 每个数据库中每轮处理的字典数量 */
#define NET_MAX_WRITES_PER_EVENT (1024*64)      /* 单次事件循环中每个客户端最大写入字节数 (64KB) */
#define PROTO_SHARED_SELECT_CMDS 10             /* 预生成的共享 SELECT 命令数量 (SELECT 0~9) */
#define OBJ_SHARED_INTEGERS 10000               /* 预分配的共享整数对象数量 (0~9999) */
#define OBJ_SHARED_BULKHDR_LEN 32               /* 预分配的共享批量回复头长度 */
#define OBJ_SHARED_HDR_STRLEN(_len_) (((_len_) < 10) ? 4 : 5) /* 计算 "$<len>\r\n" 或 "*<len>\r\n" 的长度 */
#define LOG_MAX_LEN    1024                     /* syslog 消息的最大长度 */
#define AOF_REWRITE_ITEMS_PER_CMD 64            /* AOF 重写时每条命令最多写入的元素数 */
#define AOF_ANNOTATION_LINE_MAX_LEN 1024        /* AOF 注释行最大长度 */
#define CONFIG_RUN_ID_SIZE 40                   /* Run ID 长度 (十六进制字符串) */
#define RDB_EOF_MARK_SIZE 40                    /* RDB 文件 EOF 标记长度 */
#define CONFIG_REPL_BACKLOG_MIN_SIZE (1024*16)  /* 复制积压缓冲区最小大小 (16KB) */
#define CONFIG_BGSAVE_RETRY_DELAY 5             /* BGSAVE 失败后重试等待秒数 */
#define CONFIG_DEFAULT_PID_FILE "/var/run/redis.pid"  /* 默认 PID 文件路径 */
#define CONFIG_DEFAULT_BINDADDR_COUNT 2         /* 默认绑定地址数量 */
#define CONFIG_DEFAULT_BINDADDR { "*", "-::*" } /* 默认绑定地址：IPv4 和 IPv6 */
#define NET_HOST_STR_LEN 256                    /* 主机名最大长度 */
#define NET_IP_STR_LEN 46                       /* IP 地址字符串最大长度 (INET6_ADDRSTRLEN=46) */
#define NET_ADDR_STR_LEN (NET_IP_STR_LEN+32)   /* ip:port 字符串最大长度 */
#define NET_HOST_PORT_STR_LEN (NET_HOST_STR_LEN+32) /* hostname:port 字符串最大长度 */
#define CONFIG_BINDADDR_MAX 16                  /* 最多绑定的地址数 */
#define CONFIG_MIN_RESERVED_FDS 32              /* 最小保留文件描述符数 */
#define CONFIG_DEFAULT_PROC_TITLE_TEMPLATE "{title} {listen-addr} {server-mode}" /* 进程标题模板 */
#define INCREMENTAL_REHASHING_THRESHOLD_US 1000 /* 增量 rehash 的时间阈值 (微秒) */

/* 客户端内存淘汰池的桶大小。
 * 每个桶存储内存使用量不超过其下方桶大小两倍的客户端。
 * 用于客户端内存淘汰时按内存使用量分组管理。 */
#define CLIENT_MEM_USAGE_BUCKET_MIN_LOG 15 /* 最小桶：32KB (2^15) */
#define CLIENT_MEM_USAGE_BUCKET_MAX_LOG 33 /* 最大桶：4GB+ (2^33) */
#define CLIENT_MEM_USAGE_BUCKETS (1+CLIENT_MEM_USAGE_BUCKET_MAX_LOG-CLIENT_MEM_USAGE_BUCKET_MIN_LOG) /* 桶总数 */

/* 活跃过期周期类型 */
#define ACTIVE_EXPIRE_CYCLE_SLOW 0  /* 慢速模式：完整扫描，CPU 占用较高 */
#define ACTIVE_EXPIRE_CYCLE_FAST 1  /* 快速模式：部分扫描，CPU 占用较低 */

/* 子进程正常退出状态码。
 * 用于标识子进程（RDB/AOF）无错误终止：
 * 当需要终止保存子进程时，使用 SIGUSR1 信号使其以此状态码退出，
 * 避免触发父进程因写入错误而启动的写保护机制。 */
#define SERVER_CHILD_NOERROR_RETVAL    255

/* COW (Copy-On-Write) 信息采集的占空比。
 * 读取 COW 信息有时开销较大，会拖慢持续上报的子进程。
 * 我们测量获取 COW 信息的成本，并据此控制额外读取的频率。 */
#define CHILD_COW_DUTY_CYCLE           100

/* 瞬时指标跟踪。
 * 用于计算 ops/sec、网络吞吐量等瞬时统计数据。 */
#define STATS_METRIC_SAMPLES 16                     /* 每个指标的采样窗口大小 */
#define STATS_METRIC_COMMAND 0                      /* 指标类型：已执行命令数 */
#define STATS_METRIC_NET_INPUT 1                    /* 指标类型：网络读取字节数 */
#define STATS_METRIC_NET_OUTPUT 2                   /* 指标类型：网络写入字节数 */
#define STATS_METRIC_NET_INPUT_REPLICATION 3        /* 指标类型：复制期间网络读取字节数 */
#define STATS_METRIC_NET_OUTPUT_REPLICATION 4       /* 指标类型：复制期间网络写入字节数 */
#define STATS_METRIC_EL_CYCLE 5                     /* 指标类型：事件循环次数 */
#define STATS_METRIC_EL_DURATION 6                  /* 指标类型：事件循环持续时间 */
#define STATS_METRIC_COUNT 7                        /* 指标总数 */

/* 协议和 I/O 相关常量 */
#define PROTO_IOBUF_LEN         (1024*16)   /* 通用 I/O 缓冲区大小 (16KB) */
#define PROTO_REPLY_CHUNK_BYTES (16*1024)   /* 输出缓冲区块大小 (16KB) */
#define PROTO_INLINE_MAX_SIZE   (1024*64)   /* 内联命令最大长度 (64KB) */
#define PROTO_MBULK_BIG_ARG     (1024*32)   /* 多批量参数被视为"大参数"的阈值 (32KB) */
#define PROTO_RESIZE_THRESHOLD  (1024*32)   /* 查询缓冲区 resize 阈值 (32KB) */
#define PROTO_REPLY_MIN_BYTES   (1024)      /* 回复缓冲区下限 (1KB) */
#define REDIS_AUTOSYNC_BYTES (1024*1024*4)  /* 每写入 4MB 自动同步文件 */

#define REPLY_BUFFER_DEFAULT_PEAK_RESET_TIME 5000 /* 回复缓冲区峰值重置时间 (5秒) */

/* 配置服务器事件循环时，文件描述符总数 = server.maxclients + RESERVED_FDS + 安全余量。
 * RESERVED_FDS 默认为 32，加上 96 的安全余量，确保不超过 128 个 fd。 */
#define CONFIG_FDSET_INCR (CONFIG_MIN_RESERVED_FDS+96)

/* OOM (Out-Of-Memory) 评分调整类别。
 * 用于 Linux 内核的 OOM Killer 优先级管理。 */
#define CONFIG_OOM_MASTER 0     /* 主进程 OOM 评分 */
#define CONFIG_OOM_REPLICA 1    /* 复制子进程 OOM 评分 */
#define CONFIG_OOM_BGCHILD 2    /* 后台子进程 (RDB/AOF) OOM 评分 */
#define CONFIG_OOM_COUNT 3      /* OOM 类别总数 */

extern int configOOMScoreAdjValuesDefaults[CONFIG_OOM_COUNT]; /* OOM 评分默认值 */

/* 哈希表参数 */
#define HASHTABLE_MAX_LOAD_FACTOR 1.618   /* 哈希表最大负载因子 (黄金比例) */

/* 命令标志位。
 * 每个标志描述命令的某种属性，详见 struct redisCommand 的定义。 */
#define CMD_WRITE (1ULL<<0)                /* 写命令，可能修改 keyspace */
#define CMD_READONLY (1ULL<<1)             /* 只读命令，不修改数据 */
#define CMD_DENYOOM (1ULL<<2)              /* 内存不足时拒绝执行 (可能增加内存使用) */
#define CMD_MODULE (1ULL<<3)               /* 模块导出的命令 */
#define CMD_ADMIN (1ULL<<4)                /* 管理命令 (如 SAVE, SHUTDOWN) */
#define CMD_PUBSUB (1ULL<<5)               /* Pub/Sub 相关命令 */
#define CMD_NOSCRIPT (1ULL<<6)             /* 不允许在 Lua 脚本中执行 */
#define CMD_BLOCKING (1ULL<<8)             /* 可能阻塞客户端的命令 */
#define CMD_LOADING (1ULL<<9)              /* 数据库加载期间允许执行 */
#define CMD_STALE (1ULL<<10)               /* 从节点数据过期时允许执行 */
#define CMD_SKIP_MONITOR (1ULL<<11)        /* 不传播到 MONITOR */
#define CMD_SKIP_SLOWLOG (1ULL<<12)        /* 不记录到慢查询日志 */
#define CMD_ASKING (1ULL<<13)              /* 执行前隐式发送 ASKING (集群模式) */
#define CMD_FAST (1ULL<<14)                /* 快速命令：O(1) 或 O(logN)，不会长时间阻塞 */
#define CMD_NO_AUTH (1ULL<<15)             /* 不需要认证即可执行 */
#define CMD_MAY_REPLICATE (1ULL<<16)       /* 可能产生复制流量 (如 PUBLISH, EVAL) */
#define CMD_SENTINEL (1ULL<<17)            /* Sentinel 模式下存在 */
#define CMD_ONLY_SENTINEL (1ULL<<18)       /* 仅在 Sentinel 模式下存在 */
#define CMD_NO_MANDATORY_KEYS (1ULL<<19)   /* key 参数是可选的 */
#define CMD_PROTECTED (1ULL<<20)           /* 受保护的命令，需要特殊权限 */
#define CMD_MODULE_GETKEYS (1ULL<<21)      /* 使用模块的 getkeys 接口获取 key 参数 */
#define CMD_MODULE_NO_CLUSTER (1ULL<<22)   /* 在 Redis Cluster 中拒绝执行 */
#define CMD_NO_ASYNC_LOADING (1ULL<<23)    /* 异步加载期间拒绝执行 */
#define CMD_NO_MULTI (1ULL<<24)            /* 不允许在事务 (MULTI) 中执行 */
#define CMD_MOVABLE_KEYS (1ULL<<25)        /* 传统 key 范围规范无法覆盖所有 key，
                                            * 由 populateCommandLegacyRangeSpec 填充 */
#define CMD_ALLOW_BUSY ((1ULL<<26))        /* 允许在其他命令长时间运行时执行 */
#define CMD_MODULE_GETCHANNELS (1ULL<<27)  /* 使用模块的 getchannels 接口获取 channel 参数 */
#define CMD_TOUCHES_ARBITRARY_KEYS (1ULL<<28) /* 可能访问任意 key (非 argv 中指定的) */

/* ACL 类别标志位。
 * 用于将命令归类到不同的 ACL 权限类别中。 */
#define ACL_CATEGORY_KEYSPACE (1ULL<<0)     /* @keyspace - keyspace 相关命令 */
#define ACL_CATEGORY_READ (1ULL<<1)         /* @read - 读命令 */
#define ACL_CATEGORY_WRITE (1ULL<<2)        /* @write - 写命令 */
#define ACL_CATEGORY_SET (1ULL<<3)          /* @set - Set 数据类型命令 */
#define ACL_CATEGORY_SORTEDSET (1ULL<<4)    /* @sortedset - Sorted Set 数据类型命令 */
#define ACL_CATEGORY_LIST (1ULL<<5)         /* @list - List 数据类型命令 */
#define ACL_CATEGORY_HASH (1ULL<<6)         /* @hash - Hash 数据类型命令 */
#define ACL_CATEGORY_STRING (1ULL<<7)       /* @string - String 数据类型命令 */
#define ACL_CATEGORY_BITMAP (1ULL<<8)       /* @bitmap - Bitmap 数据类型命令 */
#define ACL_CATEGORY_HYPERLOGLOG (1ULL<<9)  /* @hyperloglog - HyperLogLog 数据类型命令 */
#define ACL_CATEGORY_GEO (1ULL<<10)         /* @geo - Geo 数据类型命令 */
#define ACL_CATEGORY_STREAM (1ULL<<11)      /* @stream - Stream 数据类型命令 */
#define ACL_CATEGORY_PUBSUB (1ULL<<12)      /* @pubsub - Pub/Sub 相关命令 */
#define ACL_CATEGORY_ADMIN (1ULL<<13)       /* @admin - 管理命令 */
#define ACL_CATEGORY_FAST (1ULL<<14)        /* @fast - 快速命令 (O(1) 或 O(logN)) */
#define ACL_CATEGORY_SLOW (1ULL<<15)        /* @slow - 慢命令 */
#define ACL_CATEGORY_BLOCKING (1ULL<<16)    /* @blocking - 阻塞命令 */
#define ACL_CATEGORY_DANGEROUS (1ULL<<17)   /* @dangerous - 危险命令 */
#define ACL_CATEGORY_CONNECTION (1ULL<<18)  /* @connection - 连接相关命令 */
#define ACL_CATEGORY_TRANSACTION (1ULL<<19) /* @transaction - 事务相关命令 */
#define ACL_CATEGORY_SCRIPTING (1ULL<<20)   /* @scripting - 脚本相关命令 */

/* Key-spec 标志位
 * -------------- */
/* 以下标志描述命令对 key 的值或元数据实际执行的操作，
 * 而非用户数据本身或其影响方式。
 * 每个 key-spec 必须恰好包含以下其中一个。
 * 不属于删除、覆写或只读的操作标记为 RW。 */
#define CMD_KEY_RO (1ULL<<0)     /* 只读 - 读取 key 的值，但不一定返回它 */
#define CMD_KEY_RW (1ULL<<1)     /* 读写 - 修改 key 的值或其元数据 */
#define CMD_KEY_OW (1ULL<<2)     /* 覆写 - 覆写 key 中存储的数据 */
#define CMD_KEY_RM (1ULL<<3)     /* 删除 - 删除该 key */

/* 以下标志描述对 key 值中用户数据的操作 (不包括 LRU、type、基数等元数据)。
 * 指的是对用户数据 (实际输入字符串/TTL) 的逻辑操作。
 * 不涉及元数据的修改或返回。
 * 任何非 INSERT/DELETE 的写操作都属于 UPDATE。
 * 每个 key-spec 可以包含一个写标志 (带或不带 access)，或都不包含： */
#define CMD_KEY_ACCESS (1ULL<<4) /* 返回、复制或使用 key 值中的用户数据 */
#define CMD_KEY_UPDATE (1ULL<<5) /* 更新值中的数据，新值可能依赖于旧值 */
#define CMD_KEY_INSERT (1ULL<<6) /* 向值中添加数据，不会修改或删除已有数据 */
#define CMD_KEY_DELETE (1ULL<<7) /* 显式删除 key 值中的部分内容 */

/* 其他标志： */
#define CMD_KEY_NOT_KEY (1ULL<<8)     /* "伪" key - 在集群模式下像 key 一样路由，
                                       * 但被排除在其他 key 检查之外 */
#define CMD_KEY_INCOMPLETE (1ULL<<9)  /* keyspec 可能未覆盖它应该包含的所有 key */
#define CMD_KEY_VARIABLE_FLAGS (1ULL<<10)  /* 某些 key 的标志可能因参数不同而变化 */

/* 访问类型未知时的 key 标志 */
#define CMD_KEY_FULL_ACCESS (CMD_KEY_RW | CMD_KEY_ACCESS | CMD_KEY_UPDATE)

/* key 被移除的方式标志 */
#define DB_FLAG_KEY_NONE 0              /* 无标志 */
#define DB_FLAG_KEY_DELETED (1ULL<<0)   /* 被显式删除 */
#define DB_FLAG_KEY_EXPIRED (1ULL<<1)   /* 因过期而删除 */
#define DB_FLAG_KEY_EVICTED (1ULL<<2)   /* 因内存淘汰而删除 */
#define DB_FLAG_KEY_OVERWRITE (1ULL<<3) /* 因覆写而删除 */

/* Channel 标志 (与 key 标志共享同一标志空间) */
#define CMD_CHANNEL_PATTERN (1ULL<<11)     /* 参数是 channel 模式 (模式匹配) */
#define CMD_CHANNEL_SUBSCRIBE (1ULL<<12)   /* 命令订阅 channel */
#define CMD_CHANNEL_UNSUBSCRIBE (1ULL<<13) /* 命令取消订阅 channel */
#define CMD_CHANNEL_PUBLISH (1ULL<<14)     /* 命令向 channel 发布消息 */

/* AOF (Append Only File) 状态 */
#define AOF_OFF 0             /* AOF 关闭 */
#define AOF_ON 1              /* AOF 开启，正在追加写入 */
#define AOF_WAIT_REWRITE 2    /* AOF 等待重写完成后开始追加 */

/* AOF 加载返回值 (loadAppendOnlyFiles / loadSingleAppendOnlyFile) */
#define AOF_OK 0              /* 加载成功 */
#define AOF_NOT_EXIST 1       /* AOF 文件不存在 */
#define AOF_EMPTY 2           /* AOF 文件为空 */
#define AOF_OPEN_ERR 3        /* 打开 AOF 文件失败 */
#define AOF_FAILED 4          /* 加载 AOF 文件失败 */
#define AOF_TRUNCATED 5       /* AOF 文件被截断 (不完整) */

/* RDB 加载返回值 */
#define RDB_OK 0              /* 加载成功 */
#define RDB_NOT_EXIST 1       /* RDB 文件不存在 */
#define RDB_FAILED 2          /* 加载 RDB 文件失败 */

/* 命令文档标志 */
#define CMD_DOC_NONE 0
#define CMD_DOC_DEPRECATED (1<<0) /* 命令已废弃 */
#define CMD_DOC_SYSCMD (1<<1)     /* 系统 (内部) 命令 */

/* ============================================================
 * 客户端标志位
 * 每个 client 结构体都有一个 flags 字段，用这些标志位表示客户端状态
 * ============================================================ */
#define CLIENT_SLAVE (1<<0)   /* 该客户端是从节点 (replica) */
#define CLIENT_MASTER (1<<1)  /* 该客户端是主节点 (master) */
#define CLIENT_MONITOR (1<<2) /* 该客户端是 MONITOR 连接 */
#define CLIENT_MULTI (1<<3)   /* 该客户端处于 MULTI 事务上下文中 */
#define CLIENT_BLOCKED (1<<4) /* 客户端正在阻塞操作中等待 */
#define CLIENT_DIRTY_CAS (1<<5) /* WATCH 的 key 已被修改，EXEC 将失败 */
#define CLIENT_CLOSE_AFTER_REPLY (1<<6) /* 写完所有回复后关闭连接 */
#define CLIENT_UNBLOCKED (1<<7) /* 客户端已被解除阻塞，存储在 server.unblocked_clients 中 */
#define CLIENT_SCRIPT (1<<8) /* 非连接客户端，由 Lua 脚本使用 */
#define CLIENT_ASKING (1<<9)     /* 客户端已发送 ASKING 命令 (集群模式) */
#define CLIENT_CLOSE_ASAP (1<<10)/* 尽快关闭此客户端 */
#define CLIENT_UNIX_SOCKET (1<<11) /* 客户端通过 Unix domain socket 连接 */
#define CLIENT_DIRTY_EXEC (1<<12)  /* 命令入队时出错，EXEC 将失败 */
#define CLIENT_MASTER_FORCE_REPLY (1<<13)  /* 即使是主节点也强制排队回复 */
#define CLIENT_FORCE_AOF (1<<14)   /* 强制将当前命令传播到 AOF */
#define CLIENT_FORCE_REPL (1<<15)  /* 强制将当前命令复制到从节点 */
#define CLIENT_PRE_PSYNC (1<<16)   /* 实例不支持 PSYNC 协议 (旧版本) */
#define CLIENT_READONLY (1<<17)    /* 集群客户端处于只读状态 */
#define CLIENT_PUBSUB (1<<18)      /* 客户端处于 Pub/Sub 模式 */
#define CLIENT_PREVENT_AOF_PROP (1<<19)  /* 不传播到 AOF */
#define CLIENT_PREVENT_REPL_PROP (1<<20)  /* 不复制到从节点 */
#define CLIENT_PREVENT_PROP (CLIENT_PREVENT_AOF_PROP|CLIENT_PREVENT_REPL_PROP) /* 不传播到任何地方 */
#define CLIENT_PENDING_WRITE (1<<21) /* 客户端有输出待发送，但写处理器尚未安装 */
#define CLIENT_REPLY_OFF (1<<22)   /* 不向客户端发送回复 */
#define CLIENT_REPLY_SKIP_NEXT (1<<23)  /* 下一条命令设置 CLIENT_REPLY_SKIP */
#define CLIENT_REPLY_SKIP (1<<24)  /* 跳过本次回复 */
#define CLIENT_LUA_DEBUG (1<<25)  /* 以调试模式运行 EVAL */
#define CLIENT_LUA_DEBUG_SYNC (1<<26)  /* EVAL 调试模式，不使用 fork() */
#define CLIENT_MODULE (1<<27) /* 非连接客户端，由模块使用 */
#define CLIENT_PROTECTED (1<<28) /* 客户端当前不应被释放 */
#define CLIENT_EXECUTING_COMMAND (1<<29) /* 客户端正在处理命令。
                                          * 通常仅在 call() 期间设置，
                                          * 但阻塞客户端可能保持此标志直到重新处理命令。 */

#define CLIENT_PENDING_COMMAND (1<<30) /* 客户端已完整解析命令，准备执行 */
#define CLIENT_TRACKING (1ULL<<31) /* 客户端启用了 key tracking (客户端缓存) */
#define CLIENT_TRACKING_BROKEN_REDIR (1ULL<<32) /* tracking 重定向目标客户端无效 */
#define CLIENT_TRACKING_BCAST (1ULL<<33) /* tracking 处于广播 (BCAST) 模式 */
#define CLIENT_TRACKING_OPTIN (1ULL<<34)  /* tracking 处于 opt-in 模式 */
#define CLIENT_TRACKING_OPTOUT (1ULL<<35) /* tracking 处于 opt-out 模式 */
#define CLIENT_TRACKING_CACHING (1ULL<<36) /* 根据 optin/optout 模式设置了 CACHING yes/no */
#define CLIENT_TRACKING_NOLOOP (1ULL<<37) /* 不发送关于自身写操作的失效消息 */
#define CLIENT_IN_TO_TABLE (1ULL<<38) /* 客户端在超时表中 */
#define CLIENT_PROTOCOL_ERROR (1ULL<<39) /* 协议错误 */
#define CLIENT_CLOSE_AFTER_COMMAND (1ULL<<40) /* 执行命令并写完回复后关闭连接 */
#define CLIENT_DENY_BLOCKING (1ULL<<41) /* 指示客户端不应被阻塞。
                                         * 在 MULTI、Lua、RM_Call 和 AOF 客户端中启用 */
#define CLIENT_REPL_RDBONLY (1ULL<<42) /* 该副本只需要 RDB 数据，不需要复制缓冲区 */
#define CLIENT_NO_EVICT (1ULL<<43) /* 该客户端受保护，不会被内存淘汰 */
#define CLIENT_ALLOW_OOM (1ULL<<44) /* RM_Call 使用的客户端允许在 OOM 时完整执行脚本 */
#define CLIENT_NO_TOUCH (1ULL<<45) /* 该客户端不会更新 LFU/LRU 统计 */
#define CLIENT_PUSHING (1ULL<<46) /* 该客户端正在推送通知 */
#define CLIENT_MODULE_AUTH_HAS_RESULT (1ULL<<47) /* 模块认证中的客户端已从模块获得认证结果 */
#define CLIENT_MODULE_PREVENT_AOF_PROP (1ULL<<48) /* 模块客户端不希望传播到 AOF */
#define CLIENT_MODULE_PREVENT_REPL_PROP (1ULL<<49) /* 模块客户端不希望复制到副本 */
#define CLIENT_REPROCESSING_COMMAND (1ULL<<50) /* 客户端正在重新处理命令 */

/* 不允许优化 FLUSH SYNC 以阻塞客户端异步方式运行的标志组合 */
#define CLIENT_AVOID_BLOCKING_ASYNC_FLUSH (CLIENT_DENY_BLOCKING|CLIENT_MULTI|CLIENT_LUA_DEBUG|CLIENT_LUA_DEBUG_SYNC|CLIENT_MODULE)

/* 客户端阻塞类型 (client 结构体的 btype 字段)。
 * 当 CLIENT_BLOCKED 标志被设置时，此字段标识阻塞原因。 */
typedef enum blocking_type {
    BLOCKED_NONE,    /* 未阻塞 */
    BLOCKED_LIST,    /* BLPOP 等列表阻塞操作 */
    BLOCKED_WAIT,    /* WAIT 同步复制等待 */
    BLOCKED_WAITAOF, /* WAITAOF 等待 AOF 文件 fsync */
    BLOCKED_MODULE,  /* 被可加载模块阻塞 */
    BLOCKED_STREAM,  /* XREAD 流读取阻塞 */
    BLOCKED_ZSET,    /* BZPOP 等有序集合阻塞操作 */
    BLOCKED_POSTPONE, /* 被 processCommand 阻塞，稍后重试处理 */
    BLOCKED_SHUTDOWN, /* SHUTDOWN 阻塞 */
    BLOCKED_LAZYFREE, /* LAZYFREE 惰性释放阻塞 */
    BLOCKED_NUM,      /* 阻塞状态总数 */
    BLOCKED_END       /* 枚举结束标记 */
} blocking_type;

/* 客户端请求协议类型 */
#define PROTO_REQ_INLINE 1     /* 内联命令格式 (如: PING\r\n) */
#define PROTO_REQ_MULTIBULK 2  /* 多批量命令格式 (RESP 协议, 如: *1\r\n$4\r\nPING\r\n) */

/* 客户端类别，用于客户端输出缓冲区限制。
 * 目前仅用于 max-client-output-buffer 配置的实现。 */
#define CLIENT_TYPE_NORMAL 0 /* 普通请求-回复客户端 + MONITOR 连接 */
#define CLIENT_TYPE_SLAVE 1  /* 从节点 (replica) */
#define CLIENT_TYPE_PUBSUB 2 /* 订阅 Pub/Sub channel 的客户端 */
#define CLIENT_TYPE_MASTER 3 /* 主节点连接 */
#define CLIENT_TYPE_COUNT 4  /* 客户端类型总数 */
#define CLIENT_TYPE_OBUF_COUNT 3 /* 受输出缓冲区配置约束的客户端类型数 (前三种) */

/* 从节点复制状态枚举。
 * 用于 server.repl_state，记录从节点当前复制阶段。 */
typedef enum {
    REPL_STATE_NONE = 0,            /* 无活跃复制 */
    REPL_STATE_CONNECT,             /* 需要连接到主节点 */
    REPL_STATE_CONNECTING,          /* 正在连接主节点 */
    /* --- 握手状态，必须保持有序 --- */
    REPL_STATE_RECEIVE_PING_REPLY,  /* 等待 PING 回复 */
    REPL_STATE_SEND_HANDSHAKE,      /* 向主节点发送握手序列 */
    REPL_STATE_RECEIVE_AUTH_REPLY,  /* 等待 AUTH 回复 */
    REPL_STATE_RECEIVE_PORT_REPLY,  /* 等待 REPLCONF (listening-port) 回复 */
    REPL_STATE_RECEIVE_IP_REPLY,    /* 等待 REPLCONF (ip-address) 回复 */
    REPL_STATE_RECEIVE_CAPA_REPLY,  /* 等待 REPLCONF (capa) 回复 */
    REPL_STATE_SEND_PSYNC,          /* 发送 PSYNC 命令 */
    REPL_STATE_RECEIVE_PSYNC_REPLY, /* 等待 PSYNC 回复 */
    /* --- 握手状态结束 --- */
    REPL_STATE_TRANSFER,        /* 正在从主节点接收 RDB 文件 */
    REPL_STATE_CONNECTED,       /* 已连接到主节点 */
} repl_state;

/* 协调故障转移状态 */
typedef enum {
    NO_FAILOVER = 0,        /* 无故障转移进行中 */
    FAILOVER_WAIT_FOR_SYNC, /* 等待目标副本追上数据 */
    FAILOVER_IN_PROGRESS    /* 等待目标副本接受 PSYNC FAILOVER 请求 */
} failover_state;

/* 从主节点视角看的从节点复制状态。
 * 用于 client->replstate 字段。
 * SEND_BULK 和 ONLINE 状态下，从节点在其输出队列中接收新更新。
 * WAIT_BGSAVE 状态下，服务器等待启动下一次后台保存。 */
#define SLAVE_STATE_WAIT_BGSAVE_START 6   /* 需要生成新的 RDB 文件 */
#define SLAVE_STATE_WAIT_BGSAVE_END 7     /* 等待 RDB 文件创建完成 */
#define SLAVE_STATE_SEND_BULK 8           /* 正在向从节点发送 RDB 文件 */
#define SLAVE_STATE_ONLINE 9              /* RDB 文件已传输，仅发送增量更新 */
#define SLAVE_STATE_RDB_TRANSMITTED 10    /* RDB 文件已传输 - 仅用于只需要 RDB 不需要复制缓冲区的副本 */

/* 从节点能力标志 */
#define SLAVE_CAPA_NONE 0
#define SLAVE_CAPA_EOF (1<<0)    /* 能解析 RDB EOF 流式格式 */
#define SLAVE_CAPA_PSYNC2 (1<<1) /* 支持 PSYNC2 协议 */

/* 从节点需求标志 */
#define SLAVE_REQ_NONE 0
#define SLAVE_REQ_RDB_EXCLUDE_DATA (1 << 0)      /* RDB 中排除数据 */
#define SLAVE_REQ_RDB_EXCLUDE_FUNCTIONS (1 << 1)  /* RDB 中排除函数 */
/* 从节点需求位域中表示非标准 (过滤) RDB 需求的掩码 */
#define SLAVE_REQ_RDB_MASK (SLAVE_REQ_RDB_EXCLUDE_DATA | SLAVE_REQ_RDB_EXCLUDE_FUNCTIONS)

/* 同步读取超时 (从节点侧) */
#define CONFIG_REPL_SYNCIO_TIMEOUT 5

/* 每次调用默认修剪的复制积压缓冲区块数 */
#define REPL_BACKLOG_TRIM_BLOCKS_PER_CALL 64

/* 为了快速定位 PSYNC 请求的偏移量，
 * 我们将复制缓冲区链表中的某些节点索引到 rax 树中。 */
#define REPL_BACKLOG_INDEX_PER_BLOCKS 64

/* 列表操作方向 */
#define LIST_HEAD 0  /* 从头部操作 */
#define LIST_TAIL 1  /* 从尾部操作 */

/* 有序集合范围边界 */
#define ZSET_MIN 0   /* 最小值端 */
#define ZSET_MAX 1   /* 最大值端 */

/* SORT 命令操作类型 */
#define SORT_OP_GET 0

/* 日志级别 */
#define LL_DEBUG 0     /* 调试信息 */
#define LL_VERBOSE 1   /* 详细信息 */
#define LL_NOTICE 2    /* 通知 (默认级别) */
#define LL_WARNING 3   /* 警告 */
#define LL_NOTHING 4   /* 不记录任何日志 */
#define LL_RAW (1<<10) /* 修饰符：不添加时间戳 */

/* 进程管理选项 */
#define SUPERVISED_NONE 0          /* 无管理 */
#define SUPERVISED_AUTODETECT 1    /* 自动检测管理方式 */
#define SUPERVISED_SYSTEMD 2       /* 由 systemd 管理 */
#define SUPERVISED_UPSTART 3       /* 由 upstart 管理 */

/* 消除未使用变量警告的宏 */
#define UNUSED(V) ((void) V)

/* 跳表 (Skip List) 参数 */
#define ZSKIPLIST_MAXLEVEL 32 /* 最大层数，足够支持 2^64 个元素 */
#define ZSKIPLIST_P 0.25      /* 跳表概率参数 P = 1/4 */
#define ZSKIPLIST_MAX_SEARCH 10 /* 最大搜索次数 */

/* AOF fsync 策略 */
#define AOF_FSYNC_NO 0         /* 不主动 fsync，由操作系统决定 */
#define AOF_FSYNC_ALWAYS 1     /* 每次写入后 fsync (最安全，最慢) */
#define AOF_FSYNC_EVERYSEC 2   /* 每秒 fsync 一次 (折中方案) */

/* 复制无盘加载模式 */
#define REPL_DISKLESS_LOAD_DISABLED 0      /* 禁用无盘加载 */
#define REPL_DISKLESS_LOAD_WHEN_DB_EMPTY 1 /* 仅当数据库为空时使用无盘加载 */
#define REPL_DISKLESS_LOAD_SWAPDB 2        /* 使用 swapdb 方式无盘加载 */

/* TLS 客户端认证模式 */
#define TLS_CLIENT_AUTH_NO 0         /* 不要求客户端证书 */
#define TLS_CLIENT_AUTH_YES 1        /* 要求客户端证书 */
#define TLS_CLIENT_AUTH_OPTIONAL 2   /* 可选客户端证书 */

/* RDB/RESTORE 载荷清理模式 */
#define SANITIZE_DUMP_NO 0           /* 不清理 */
#define SANITIZE_DUMP_YES 1          /* 深度清理 ziplist 和 listpack */
#define SANITIZE_DUMP_CLIENTS 2      /* 仅对客户端操作清理 */

/* 受保护配置/命令的启用模式 */
#define PROTECTED_ACTION_ALLOWED_NO 0    /* 禁止 */
#define PROTECTED_ACTION_ALLOWED_YES 1   /* 允许 */
#define PROTECTED_ACTION_ALLOWED_LOCAL 2 /* 仅本地连接允许 */

/* 集合操作类型 */
#define SET_OP_UNION 0   /* 并集 */
#define SET_OP_DIFF 1    /* 差集 */
#define SET_OP_INTER 2   /* 交集 */

/* OOM 评分调整模式 */
#define OOM_SCORE_ADJ_NO 0         /* 不调整 */
#define OOM_SCORE_RELATIVE 1       /* 相对调整 */
#define OOM_SCORE_ADJ_ABSOLUTE 2   /* 绝对调整 */

/* Redis maxmemory (最大内存) 淘汰策略。
 * 使用标志位组合而非简单递增数字，便于快速测试多个策略的共同属性。 */
#define MAXMEMORY_FLAG_LRU (1<<0)     /* 使用 LRU 算法 */
#define MAXMEMORY_FLAG_LFU (1<<1)     /* 使用 LFU 算法 */
#define MAXMEMORY_FLAG_ALLKEYS (1<<2) /* 在所有 key 中淘汰 (而非仅过期 key) */
#define MAXMEMORY_FLAG_NO_SHARED_INTEGERS \
    (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU) /* LRU/LFU 模式不使用共享整数 */

#define MAXMEMORY_VOLATILE_LRU ((0<<8)|MAXMEMORY_FLAG_LRU)       /* 在过期 key 中使用 LRU 淘汰 */
#define MAXMEMORY_VOLATILE_LFU ((1<<8)|MAXMEMORY_FLAG_LFU)       /* 在过期 key 中使用 LFU 淘汰 */
#define MAXMEMORY_VOLATILE_TTL (2<<8)                             /* 淘汰 TTL 最短的 key */
#define MAXMEMORY_VOLATILE_RANDOM (3<<8)                          /* 随机淘汰过期 key */
#define MAXMEMORY_ALLKEYS_LRU ((4<<8)|MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_ALLKEYS)   /* 在所有 key 中使用 LRU 淘汰 */
#define MAXMEMORY_ALLKEYS_LFU ((5<<8)|MAXMEMORY_FLAG_LFU|MAXMEMORY_FLAG_ALLKEYS)   /* 在所有 key 中使用 LFU 淘汰 */
#define MAXMEMORY_ALLKEYS_RANDOM ((6<<8)|MAXMEMORY_FLAG_ALLKEYS)  /* 在所有 key 中随机淘汰 */
#define MAXMEMORY_NO_EVICTION (7<<8)                              /* 不淘汰，内存不足时拒绝写入 */

/* 时间单位 */
#define UNIT_SECONDS 0        /* 秒 */
#define UNIT_MILLISECONDS 1   /* 毫秒 */

/* SHUTDOWN 命令标志 */
#define SHUTDOWN_NOFLAGS 0      /* 无标志 */
#define SHUTDOWN_SAVE 1         /* 强制在关闭前执行 SAVE (即使未配置 save 点) */
#define SHUTDOWN_NOSAVE 2       /* 关闭时不执行 SAVE */
#define SHUTDOWN_NOW 4          /* 不等待副本追上数据 */
#define SHUTDOWN_FORCE 8        /* 不让错误阻止关闭 */

/* 命令调用标志，见 call() 函数 */
#define CMD_CALL_NONE 0                                    /* 无标志 */
#define CMD_CALL_PROPAGATE_AOF (1<<0)                      /* 传播到 AOF */
#define CMD_CALL_PROPAGATE_REPL (1<<1)                     /* 复制到从节点 */
#define CMD_CALL_REPROCESSING (1<<2)                       /* 正在重新处理命令 */
#define CMD_CALL_FROM_MODULE (1<<3)                        /* 来自 RM_Call 调用 */
#define CMD_CALL_PROPAGATE (CMD_CALL_PROPAGATE_AOF|CMD_CALL_PROPAGATE_REPL) /* 传播到 AOF 和从节点 */
#define CMD_CALL_FULL (CMD_CALL_PROPAGATE)                 /* 完整传播 */

/* 命令传播标志，见 propagateNow() 函数 */
#define PROPAGATE_NONE 0   /* 不传播 */
#define PROPAGATE_AOF 1    /* 传播到 AOF */
#define PROPAGATE_REPL 2   /* 复制到从节点 */

/* 暂停操作类型 (位掩码) */
#define PAUSE_ACTION_CLIENT_WRITE     (1<<0)  /* 暂停客户端写操作 */
#define PAUSE_ACTION_CLIENT_ALL       (1<<1)  /* 暂停所有客户端操作 (必须大于 CLIENT_WRITE) */
#define PAUSE_ACTION_EXPIRE           (1<<2)  /* 暂停 key 过期处理 */
#define PAUSE_ACTION_EVICT            (1<<3)  /* 暂停内存淘汰 */
#define PAUSE_ACTION_REPLICA          (1<<4)  /* 暂停副本流量 */

/* 常用的暂停/恢复操作组合 */
#define PAUSE_ACTIONS_CLIENT_WRITE_SET (PAUSE_ACTION_CLIENT_WRITE|\
                                        PAUSE_ACTION_EXPIRE|\
                                        PAUSE_ACTION_EVICT|\
                                        PAUSE_ACTION_REPLICA)
#define PAUSE_ACTIONS_CLIENT_ALL_SET   (PAUSE_ACTION_CLIENT_ALL|\
                                        PAUSE_ACTION_EXPIRE|\
                                        PAUSE_ACTION_EVICT|\
                                        PAUSE_ACTION_REPLICA)

/* 客户端暂停目的。每个目的有独立的结束时间和暂停类型。 */
typedef enum {
    PAUSE_BY_CLIENT_COMMAND = 0,  /* 由 CLIENT PAUSE 命令触发 */
    PAUSE_DURING_SHUTDOWN,        /* 关闭期间暂停 */
    PAUSE_DURING_FAILOVER,        /* 故障转移期间暂停 */
    NUM_PAUSE_PURPOSES            /* 暂停目的总数 */
} pause_purpose;

typedef struct {
    uint32_t paused_actions; /* 暂停的操作位掩码 */
    mstime_t end;            /* 暂停结束时间 */
} pause_event;

/* 集群端点描述方式 */
typedef enum {
    CLUSTER_ENDPOINT_TYPE_IP = 0,          /* 显示 IP 地址 */
    CLUSTER_ENDPOINT_TYPE_HOSTNAME,        /* 显示主机名 */
    CLUSTER_ENDPOINT_TYPE_UNKNOWN_ENDPOINT /* 显示 NULL 或空 */
} cluster_endpoint_type;

/* RDB 后台子进程保存类型 */
#define RDB_CHILD_TYPE_NONE 0
#define RDB_CHILD_TYPE_DISK 1     /* RDB 写入磁盘 */
#define RDB_CHILD_TYPE_SOCKET 2   /* RDB 写入从节点 socket (无盘复制) */

/* Keyspace 通知类别。
 * 每个类别关联一个字符，用于 notify-keyspace-events 配置。 */
#define NOTIFY_KEYSPACE (1<<0)    /* K - keyspace 通知 (key 级别) */
#define NOTIFY_KEYEVENT (1<<1)    /* E - keyevent 通知 (事件级别) */
#define NOTIFY_GENERIC (1<<2)     /* g - 通用命令 (DEL, EXPIRE, RENAME 等) */
#define NOTIFY_STRING (1<<3)      /* $ - String 命令 */
#define NOTIFY_LIST (1<<4)        /* l - List 命令 */
#define NOTIFY_SET (1<<5)         /* s - Set 命令 */
#define NOTIFY_HASH (1<<6)        /* h - Hash 命令 */
#define NOTIFY_ZSET (1<<7)        /* z - Sorted Set 命令 */
#define NOTIFY_EXPIRED (1<<8)     /* x - key 过期事件 */
#define NOTIFY_EVICTED (1<<9)     /* e - key 被淘汰事件 */
#define NOTIFY_STREAM (1<<10)     /* t - Stream 命令 */
#define NOTIFY_KEY_MISS (1<<11)   /* m - key 未命中 (注意：故意排除在 NOTIFY_ALL 之外) */
#define NOTIFY_LOADED (1<<12)     /* 模块专用：从 RDB 加载 key 的通知 */
#define NOTIFY_MODULE (1<<13)     /* d - 模块 keyspace 通知 */
#define NOTIFY_NEW (1<<14)        /* n - 新 key 创建通知 */
#define NOTIFY_ALL (NOTIFY_GENERIC | NOTIFY_STRING | NOTIFY_LIST | NOTIFY_SET | NOTIFY_HASH | NOTIFY_ZSET | NOTIFY_EXPIRED | NOTIFY_EVICTED | NOTIFY_STREAM | NOTIFY_MODULE) /* 所有通知类型的组合 */

/* 使用此宏可以在 serverCron() 中按指定周期 (毫秒) 运行代码。
 * 实际分辨率取决于 server.hz。 */
#define run_with_period(_ms_) if (((_ms_) <= 1000/server.hz) || !(server.cronloops%((_ms_)/(1000/server.hz))))

/* 断言宏 - 失败时打印堆栈跟踪 */
#define serverAssertWithInfo(_c,_o,_e) (likely(_e)?(void)0 : (_serverAssertWithInfo(_c,_o,#_e,__FILE__,__LINE__),redis_unreachable()))
#define serverAssert(_e) (likely(_e)?(void)0 : (_serverAssert(#_e,__FILE__,__LINE__),redis_unreachable()))
#define serverPanic(...) _serverPanic(__FILE__,__LINE__,__VA_ARGS__),redis_unreachable()

/* 调试断言宏 - 仅在 DEBUG_ASSERTIONS 构建中执行。
 * 用于添加计算开销大或在正常运行中不安全的断言。 */
#ifdef DEBUG_ASSERTIONS
#define debugServerAssertWithInfo(...) serverAssertWithInfo(__VA_ARGS__)
#else
#define debugServerAssertWithInfo(...)
#endif

/* 每命令延迟直方图初始化设置 */
#define LATENCY_HISTOGRAM_MIN_VALUE 1L           /* 最小值：>= 1 纳秒 */
#define LATENCY_HISTOGRAM_MAX_VALUE 1000000000L  /* 最大值：<= 1 秒 */
#define LATENCY_HISTOGRAM_PRECISION 2  /* 在 MIN_VALUE 到 MAX_VALUE 范围内保持 2 位有效数字精度。
                                        * 范围内的值量化精度不超过任何值的 1/100 (即 1%)。
                                        * 每个直方图总大小约 40 KiB。 */

/* 模块繁忙标志，见 busy_module_yield_flags */
#define BUSY_MODULE_YIELD_NONE (0)           /* 无 */
#define BUSY_MODULE_YIELD_EVENTS (1<<0)      /* 让出事件处理 */
#define BUSY_MODULE_YIELD_CLIENTS (1<<1)     /* 让出客户端处理 */

/*-----------------------------------------------------------------------------
 * 数据类型定义
 *----------------------------------------------------------------------------*/

/* Redis 对象系统
 * Redis 使用统一的 redisObject 结构体来封装所有数据类型，
 * 通过 type 字段区分类型，encoding 字段区分底层编码。 */

/* Redis 对象类型 */
#define OBJ_STRING 0    /* String 字符串对象 */
#define OBJ_LIST 1      /* List 列表对象 */
#define OBJ_SET 2       /* Set 集合对象 */
#define OBJ_ZSET 3      /* Sorted Set 有序集合对象 */
#define OBJ_HASH 4      /* Hash 哈希对象 */

/* "module" 对象类型是特殊类型，表示对象由 Redis 模块直接管理。
 * 此时 value 指向 moduleValue 结构体，包含：
 * - 模块管理的对象值 (仅由模块自身处理)
 * - RedisModuleType 结构体，列出序列化/反序列化/AOF重写/释放的函数指针
 *
 * 在 RDB 文件中，模块类型编码为 OBJ_MODULE 后跟 64 位模块类型 ID：
 * - 高 54 位：模块特定签名 (用于分派加载到正确的模块)
 * - 低 10 位：编码版本 */
#define OBJ_MODULE 5    /* Module 模块对象 */
#define OBJ_STREAM 6    /* Stream 流对象 */
#define OBJ_TYPE_MAX 7  /* 对象类型最大值 */

/* 从模块类型 ID 中提取编码版本和签名 */
#define REDISMODULE_TYPE_ENCVER_BITS 10
#define REDISMODULE_TYPE_ENCVER_MASK ((1<<REDISMODULE_TYPE_ENCVER_BITS)-1)
#define REDISMODULE_TYPE_ENCVER(id) ((id) & REDISMODULE_TYPE_ENCVER_MASK)           /* 提取低 10 位编码版本 */
#define REDISMODULE_TYPE_SIGN(id) (((id) & ~((uint64_t)REDISMODULE_TYPE_ENCVER_MASK)) >>REDISMODULE_TYPE_ENCVER_BITS) /* 提取高 54 位签名 */

/* moduleTypeAuxSaveFunc 的位标志 */
#define REDISMODULE_AUX_BEFORE_RDB (1<<0)  /* 在 RDB 数据之前保存辅助数据 */
#define REDISMODULE_AUX_AFTER_RDB (1<<1)   /* 在 RDB 数据之后保存辅助数据 */

struct RedisModule;
struct RedisModuleIO;
struct RedisModuleDigest;
struct RedisModuleCtx;
struct moduleLoadQueueEntry;
struct RedisModuleKeyOptCtx;
struct RedisModuleCommand;
struct clusterState;

/* 模块类型实现必须导出一组方法，用于：
 * - 在 RDB 文件中序列化/反序列化值
 * - 重写 AOF 日志
 * - 为 "DEBUG DIGEST" 创建摘要
 * - key 被删除时释放值 */
typedef void *(*moduleTypeLoadFunc)(struct RedisModuleIO *io, int encver);              /* 从 RDB 加载 */
typedef void (*moduleTypeSaveFunc)(struct RedisModuleIO *io, void *value);              /* 保存到 RDB */
typedef int (*moduleTypeAuxLoadFunc)(struct RedisModuleIO *rdb, int encver, int when);  /* 加载辅助数据 */
typedef void (*moduleTypeAuxSaveFunc)(struct RedisModuleIO *rdb, int when);             /* 保存辅助数据 */
typedef void (*moduleTypeRewriteFunc)(struct RedisModuleIO *io, struct redisObject *key, void *value); /* AOF 重写 */
typedef void (*moduleTypeDigestFunc)(struct RedisModuleDigest *digest, void *value);    /* DEBUG DIGEST */
typedef size_t (*moduleTypeMemUsageFunc)(const void *value);                            /* 内存使用量 */
typedef void (*moduleTypeFreeFunc)(void *value);                                        /* 释放值 */
typedef size_t (*moduleTypeFreeEffortFunc)(struct redisObject *key, const void *value); /* 释放开销估计 */
typedef void (*moduleTypeUnlinkFunc)(struct redisObject *key, void *value);             /* key 被删除时的回调 */
typedef void *(*moduleTypeCopyFunc)(struct redisObject *fromkey, struct redisObject *tokey, const void *value); /* 复制值 */
typedef int (*moduleTypeDefragFunc)(struct RedisModuleDefragCtx *ctx, struct redisObject *key, void **value);   /* 内存碎片整理 */
typedef size_t (*moduleTypeMemUsageFunc2)(struct RedisModuleKeyOptCtx *ctx, const void *value, size_t sample_size); /* 内存使用量 v2 */
typedef void (*moduleTypeFreeFunc2)(struct RedisModuleKeyOptCtx *ctx, void *value);            /* 释放值 v2 */
typedef size_t (*moduleTypeFreeEffortFunc2)(struct RedisModuleKeyOptCtx *ctx, const void *value); /* 释放开销 v2 */
typedef void (*moduleTypeUnlinkFunc2)(struct RedisModuleKeyOptCtx *ctx, void *value);           /* unlink 回调 v2 */
typedef void *(*moduleTypeCopyFunc2)(struct RedisModuleKeyOptCtx *ctx, const void *value);     /* 复制值 v2 */
typedef int (*moduleTypeAuthCallback)(struct RedisModuleCtx *ctx, void *username, void *password, const char **err); /* 认证回调 */


/* 模块类型结构体。
 * 每个给定类型的值都引用此结构体，定义了该类型的方法和导出模块的链接。 */
typedef struct RedisModuleType {
    uint64_t id; /* 类型 ID：高 54 位签名 + 低 10 位编码版本 */
    struct RedisModule *module;           /* 所属模块 */
    moduleTypeLoadFunc rdb_load;          /* RDB 加载函数 */
    moduleTypeSaveFunc rdb_save;          /* RDB 保存函数 */
    moduleTypeRewriteFunc aof_rewrite;    /* AOF 重写函数 */
    moduleTypeMemUsageFunc mem_usage;     /* 内存使用量计算函数 */
    moduleTypeDigestFunc digest;          /* DEBUG DIGEST 函数 */
    moduleTypeFreeFunc free;              /* 值释放函数 */
    moduleTypeFreeEffortFunc free_effort; /* 释放开销估计函数 */
    moduleTypeUnlinkFunc unlink;          /* key 删除回调 */
    moduleTypeCopyFunc copy;              /* 值复制函数 */
    moduleTypeDefragFunc defrag;          /* 内存碎片整理函数 */
    moduleTypeAuxLoadFunc aux_load;       /* 辅助数据加载函数 */
    moduleTypeAuxSaveFunc aux_save;       /* 辅助数据保存函数 */
    moduleTypeMemUsageFunc2 mem_usage2;   /* 内存使用量计算 v2 */
    moduleTypeFreeEffortFunc2 free_effort2; /* 释放开销估计 v2 */
    moduleTypeUnlinkFunc2 unlink2;        /* key 删除回调 v2 */
    moduleTypeCopyFunc2 copy2;            /* 值复制函数 v2 */
    moduleTypeAuxSaveFunc aux_save2;      /* 辅助数据保存 v2 */
    int aux_save_triggers;                /* 辅助数据保存触发条件 */
    char name[10]; /* 模块类型名称：9 字节 + null 终止符。字符集：A-Z a-z 0-9 _- */
} moduleType;

/* 在 OBJ_MODULE 类型的 redisObject (robj) 中，value 指针指向此结构体。
 * 它引用 moduleType 结构体以操作值，同时提供模块命令创建的原始值指针。
 *
 * 释放示例：
 *  if (robj->type == OBJ_MODULE) {
 *      moduleValue *mt = robj->ptr;
 *      mt->type->free(mt->value);  // 调用模块的释放函数
 *      zfree(mt);                  // 释放中间结构体
 *  }
 */
typedef struct moduleValue {
    moduleType *type;   /* 模块类型 (包含操作函数) */
    void *value;        /* 模块管理的实际值 */
} moduleValue;

/* This structure represents a module inside the system. */
struct RedisModule {
    void *handle;   /* Module dlopen() handle. */
    char *name;     /* Module name. */
    int ver;        /* Module version. We use just progressive integers. */
    int apiver;     /* Module API version as requested during initialization.*/
    list *types;    /* Module data types. */
    list *usedby;   /* List of modules using APIs from this one. */
    list *using;    /* List of modules we use some APIs of. */
    list *filters;  /* List of filters the module has registered. */
    list *module_configs; /* List of configurations the module has registered */
    int configs_initialized; /* Have the module configurations been initialized? */
    int in_call;    /* RM_Call() nesting level */
    int in_hook;    /* Hooks callback nesting level for this module (0 or 1). */
    int options;    /* Module options and capabilities. */
    int blocked_clients;         /* Count of RedisModuleBlockedClient in this module. */
    RedisModuleInfoFunc info_cb; /* Callback for module to add INFO fields. */
    RedisModuleDefragFunc defrag_cb;    /* Callback for global data defrag. */
    struct moduleLoadQueueEntry *loadmod; /* Module load arguments for config rewrite. */
    int num_commands_with_acl_categories; /* Number of commands in this module included in acl categories */
    int onload;     /* Flag to identify if the call is being made from Onload (0 or 1) */
    size_t num_acl_categories_added; /* Number of acl categories added by this module. */
};
typedef struct RedisModule RedisModule;

/* This is a wrapper for the 'rio' streams used inside rdb.c in Redis, so that
 * the user does not have to take the total count of the written bytes nor
 * to care about error conditions. */
struct RedisModuleIO {
    size_t bytes;       /* Bytes read / written so far. */
    rio *rio;           /* Rio stream. */
    moduleType *type;   /* Module type doing the operation. */
    int error;          /* True if error condition happened. */
    struct RedisModuleCtx *ctx; /* Optional context, see RM_GetContextFromIO()*/
    struct redisObject *key;    /* Optional name of key processed */
    int dbid;            /* The dbid of the key being processed, -1 when unknown. */
    sds pre_flush_buffer; /* A buffer that should be flushed before next write operation
                           * See rdbSaveSingleModuleAux for more details */
};

/* Macro to initialize an IO context. Note that the 'ver' field is populated
 * inside rdb.c according to the version of the value to load. */
#define moduleInitIOContext(iovar,mtype,rioptr,keyptr,db) do { \
    iovar.rio = rioptr; \
    iovar.type = mtype; \
    iovar.bytes = 0; \
    iovar.error = 0; \
    iovar.key = keyptr; \
    iovar.dbid = db; \
    iovar.ctx = NULL; \
    iovar.pre_flush_buffer = NULL; \
} while(0)

/* This is a structure used to export DEBUG DIGEST capabilities to Redis
 * modules. We want to capture both the ordered and unordered elements of
 * a data structure, so that a digest can be created in a way that correctly
 * reflects the values. See the DEBUG DIGEST command implementation for more
 * background. */
struct RedisModuleDigest {
    unsigned char o[20];    /* Ordered elements. */
    unsigned char x[20];    /* Xored elements. */
    struct redisObject *key; /* Optional name of key processed */
    int dbid;                /* The dbid of the key being processed */
};

/* Just start with a digest composed of all zero bytes. */
#define moduleInitDigestContext(mdvar) do { \
    memset(mdvar.o,0,sizeof(mdvar.o)); \
    memset(mdvar.x,0,sizeof(mdvar.x)); \
} while(0)

/* Macro to check if the client is in the middle of module based authentication. */
#define clientHasModuleAuthInProgress(c) ((c)->module_auth_ctx != NULL)

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
#define OBJ_ENCODING_RAW 0     /* Raw representation */
#define OBJ_ENCODING_INT 1     /* Encoded as integer */
#define OBJ_ENCODING_HT 2      /* Encoded as hash table */
#define OBJ_ENCODING_ZIPMAP 3  /* No longer used: old hash encoding. */
#define OBJ_ENCODING_LINKEDLIST 4 /* No longer used: old list encoding. */
#define OBJ_ENCODING_ZIPLIST 5 /* No longer used: old list/hash/zset encoding. */
#define OBJ_ENCODING_INTSET 6  /* Encoded as intset */
#define OBJ_ENCODING_SKIPLIST 7  /* Encoded as skiplist */
#define OBJ_ENCODING_EMBSTR 8  /* Embedded sds string encoding */
#define OBJ_ENCODING_QUICKLIST 9 /* Encoded as linked list of listpacks */
#define OBJ_ENCODING_STREAM 10 /* Encoded as a radix tree of listpacks */
#define OBJ_ENCODING_LISTPACK 11 /* Encoded as a listpack */
#define OBJ_ENCODING_LISTPACK_EX 12 /* Encoded as listpack, extended with metadata */

#define LRU_BITS 24
#define LRU_CLOCK_MAX ((1<<LRU_BITS)-1) /* Max value of obj->lru */
#define LRU_CLOCK_RESOLUTION 1000 /* LRU clock resolution in ms */

#define OBJ_SHARED_REFCOUNT INT_MAX     /* Global object never destroyed. */
#define OBJ_STATIC_REFCOUNT (INT_MAX-1) /* Object allocated in the stack. */
#define OBJ_FIRST_SPECIAL_REFCOUNT OBJ_STATIC_REFCOUNT
struct redisObject {
    unsigned type:4;
    unsigned encoding:4;
    unsigned lru:LRU_BITS; /* LRU time (relative to global lru_clock) or
                            * LFU data (least significant 8 bits frequency
                            * and most significant 16 bits access time). */
    int refcount;
    void *ptr;
};

/* The string name for an object's type as listed above
 * Native types are checked against the OBJ_STRING, OBJ_LIST, OBJ_* defines,
 * and Module types have their registered name returned. */
char *getObjectTypeName(robj*);

/* Macro used to initialize a Redis object allocated on the stack.
 * Note that this macro is taken near the structure definition to make sure
 * we'll update it when the structure is changed, to avoid bugs like
 * bug #85 introduced exactly in this way. */
#define initStaticStringObject(_var,_ptr) do { \
    _var.refcount = OBJ_STATIC_REFCOUNT; \
    _var.type = OBJ_STRING; \
    _var.encoding = OBJ_ENCODING_RAW; \
    _var.ptr = _ptr; \
} while(0)

struct evictionPoolEntry; /* Defined in evict.c */

/* This structure is used in order to represent the output buffer of a client,
 * which is actually a linked list of blocks like that, that is: client->reply. */
typedef struct clientReplyBlock {
    size_t size, used;
    char buf[];
} clientReplyBlock;

/* Replication buffer blocks is the list of replBufBlock.
 *
 * +--------------+       +--------------+       +--------------+
 * | refcount = 1 |  ...  | refcount = 0 |  ...  | refcount = 2 |
 * +--------------+       +--------------+       +--------------+
 *      |                                            /       \
 *      |                                           /         \
 *      |                                          /           \
 *  Repl Backlog                               Replica_A    Replica_B
 *
 * Each replica or replication backlog increments only the refcount of the
 * 'ref_repl_buf_node' which it points to. So when replica walks to the next
 * node, it should first increase the next node's refcount, and when we trim
 * the replication buffer nodes, we remove node always from the head node which
 * refcount is 0. If the refcount of the head node is not 0, we must stop
 * trimming and never iterate the next node. */

/* Similar with 'clientReplyBlock', it is used for shared buffers between
 * all replica clients and replication backlog. */
typedef struct replBufBlock {
    int refcount;           /* Number of replicas or repl backlog using. */
    long long id;           /* The unique incremental number. */
    long long repl_offset;  /* Start replication offset of the block. */
    size_t size, used;
    char buf[];
} replBufBlock;

/* Redis database representation. There are multiple databases identified
 * by integers from 0 (the default database) up to the max configured
 * database. The database number is the 'id' field in the structure. */
typedef struct redisDb {
    kvstore *keys;              /* The keyspace for this DB */
    kvstore *expires;           /* Timeout of keys with a timeout set */
    ebuckets hexpires;          /* Hash expiration DS. Single TTL per hash (of next min field to expire) */
    dict *blocking_keys;        /* Keys with clients waiting for data (BLPOP)*/
    dict *blocking_keys_unblock_on_nokey;   /* Keys with clients waiting for
                                             * data, and should be unblocked if key is deleted (XREADEDGROUP).
                                             * This is a subset of blocking_keys*/
    dict *ready_keys;           /* Blocked keys that received a PUSH */
    dict *watched_keys;         /* WATCHED keys for MULTI/EXEC CAS */
    int id;                     /* Database ID */
    long long avg_ttl;          /* Average TTL, just for stats */
    unsigned long expires_cursor; /* Cursor of the active expire cycle. */
    list *defrag_later;         /* List of key names to attempt to defrag one by one, gradually. */
} redisDb;

/* forward declaration for functions ctx */
typedef struct functionsLibCtx functionsLibCtx;

/* Holding object that need to be populated during
 * rdb loading. On loading end it is possible to decide
 * whether not to set those objects on their rightful place.
 * For example: dbarray need to be set as main database on
 *              successful loading and dropped on failure. */
typedef struct rdbLoadingCtx {
    redisDb* dbarray;
    functionsLibCtx* functions_lib_ctx;
}rdbLoadingCtx;

/* Client MULTI/EXEC state */
typedef struct multiCmd {
    robj **argv;
    int argv_len;
    int argc;
    struct redisCommand *cmd;
} multiCmd;

typedef struct multiState {
    multiCmd *commands;     /* Array of MULTI commands */
    int count;              /* Total number of MULTI commands */
    int cmd_flags;          /* The accumulated command flags OR-ed together.
                               So if at least a command has a given flag, it
                               will be set in this field. */
    int cmd_inv_flags;      /* Same as cmd_flags, OR-ing the ~flags. so that it
                               is possible to know if all the commands have a
                               certain flag. */
    size_t argv_len_sums;    /* mem used by all commands arguments */
    int alloc_count;         /* total number of multiCmd struct memory reserved. */
} multiState;

/* This structure holds the blocking operation state for a client.
 * The fields used depend on client->btype. */
typedef struct blockingState {
    /* Generic fields. */
    blocking_type btype;                  /* Type of blocking op if CLIENT_BLOCKED. */
    mstime_t timeout;           /* Blocking operation timeout. If UNIX current time
                                 * is > timeout then the operation timed out. */
    int unblock_on_nokey;       /* Whether to unblock the client when at least one of the keys
                                   is deleted or does not exist anymore */
    /* BLOCKED_LIST, BLOCKED_ZSET and BLOCKED_STREAM or any other Keys related blocking */
    dict *keys;                 /* The keys we are blocked on */

    /* BLOCKED_WAIT and BLOCKED_WAITAOF */
    int numreplicas;        /* Number of replicas we are waiting for ACK. */
    int numlocal;           /* Indication if WAITAOF is waiting for local fsync. */
    long long reploffset;   /* Replication offset to reach. */

    /* BLOCKED_MODULE */
    void *module_blocked_handle; /* RedisModuleBlockedClient structure.
                                    which is opaque for the Redis core, only
                                    handled in module.c. */

    void *async_rm_call_handle; /* RedisModuleAsyncRMCallPromise structure.
                                   which is opaque for the Redis core, only
                                   handled in module.c. */

    /* BLOCKED_LAZYFREE */
    monotime lazyfreeStartTime;
} blockingState;

/* The following structure represents a node in the server.ready_keys list,
 * where we accumulate all the keys that had clients blocked with a blocking
 * operation such as B[LR]POP, but received new data in the context of the
 * last executed command.
 *
 * After the execution of every command or script, we iterate over this list to check
 * if as a result we should serve data to clients blocked, unblocking them.
 * Note that server.ready_keys will not have duplicates as there dictionary
 * also called ready_keys in every structure representing a Redis database,
 * where we make sure to remember if a given key was already added in the
 * server.ready_keys list. */
typedef struct readyList {
    redisDb *db;
    robj *key;
} readyList;

/* This structure represents a Redis user. This is useful for ACLs, the
 * user is associated to the connection after the connection is authenticated.
 * If there is no associated user, the connection uses the default user. */
#define USER_COMMAND_BITS_COUNT 1024    /* The total number of command bits
                                           in the user structure. The last valid
                                           command ID we can set in the user
                                           is USER_COMMAND_BITS_COUNT-1. */
#define USER_FLAG_ENABLED (1<<0)        /* The user is active. */
#define USER_FLAG_DISABLED (1<<1)       /* The user is disabled. */
#define USER_FLAG_NOPASS (1<<2)         /* The user requires no password, any
                                           provided password will work. For the
                                           default user, this also means that
                                           no AUTH is needed, and every
                                           connection is immediately
                                           authenticated. */
#define USER_FLAG_SANITIZE_PAYLOAD (1<<3)       /* The user require a deep RESTORE
                                                 * payload sanitization. */
#define USER_FLAG_SANITIZE_PAYLOAD_SKIP (1<<4)  /* The user should skip the
                                                 * deep sanitization of RESTORE
                                                 * payload. */

#define SELECTOR_FLAG_ROOT (1<<0)           /* This is the root user permission
                                             * selector. */
#define SELECTOR_FLAG_ALLKEYS (1<<1)        /* The user can mention any key. */
#define SELECTOR_FLAG_ALLCOMMANDS (1<<2)    /* The user can run all commands. */
#define SELECTOR_FLAG_ALLCHANNELS (1<<3)    /* The user can mention any Pub/Sub
                                               channel. */

typedef struct {
    sds name;       /* The username as an SDS string. */
    uint32_t flags; /* See USER_FLAG_* */
    list *passwords; /* A list of SDS valid passwords for this user. */
    list *selectors; /* A list of selectors this user validates commands
                        against. This list will always contain at least
                        one selector for backwards compatibility. */
    robj *acl_string; /* cached string represent of ACLs */
} user;

/* With multiplexing we need to take per-client state.
 * Clients are taken in a linked list. */

#define CLIENT_ID_AOF (UINT64_MAX) /* Reserved ID for the AOF client. If you
                                      need more reserved IDs use UINT64_MAX-1,
                                      -2, ... and so forth. */

/* Replication backlog is not a separate memory, it just is one consumer of
 * the global replication buffer. This structure records the reference of
 * replication buffers. Since the replication buffer block list may be very long,
 * it would cost much time to search replication offset on partial resync, so
 * we use one rax tree to index some blocks every REPL_BACKLOG_INDEX_PER_BLOCKS
 * to make searching offset from replication buffer blocks list faster. */
typedef struct replBacklog {
    listNode *ref_repl_buf_node; /* Referenced node of replication buffer blocks,
                                  * see the definition of replBufBlock. */
    size_t unindexed_count;      /* The count from last creating index block. */
    rax *blocks_index;           /* The index of recorded blocks of replication
                                  * buffer for quickly searching replication
                                  * offset on partial resynchronization. */
    long long histlen;           /* Backlog actual data length */
    long long offset;            /* Replication "master offset" of first
                                  * byte in the replication backlog buffer.*/
} replBacklog;

typedef struct {
    list *clients;
    size_t mem_usage_sum;
} clientMemUsageBucket;

#ifdef LOG_REQ_RES
/* Structure used to log client's requests and their
 * responses (see logreqres.c) */
typedef struct {
    /* General */
    int argv_logged; /* 1 if the command was logged */
    /* Vars for log buffer */
    unsigned char *buf; /* Buffer holding the data (request and response) */
    size_t used;
    size_t capacity;
    /* Vars for offsets within the client's reply */
    struct {
        /* General */
        int saved; /* 1 if we already saved the offset (first time we call addReply*) */
        /* Offset within the static reply buffer */
        int bufpos;
        /* Offset within the reply block list */
        struct {
            int index;
            size_t used;
        } last_node;
    } offset;
} clientReqResInfo;
#endif

typedef struct client {
    uint64_t id;            /* Client incremental unique ID. */
    uint64_t flags;         /* Client flags: CLIENT_* macros. */
    connection *conn;
    int resp;               /* RESP protocol version. Can be 2 or 3. */
    redisDb *db;            /* Pointer to currently SELECTed DB. */
    robj *name;             /* As set by CLIENT SETNAME. */
    robj *lib_name;         /* The client library name as set by CLIENT SETINFO. */
    robj *lib_ver;          /* The client library version as set by CLIENT SETINFO. */
    sds querybuf;           /* Buffer we use to accumulate client queries. */
    size_t qb_pos;          /* The position we have read in querybuf. */
    size_t querybuf_peak;   /* Recent (100ms or more) peak of querybuf size. */
    int argc;               /* Num of arguments of current command. */
    robj **argv;            /* Arguments of current command. */
    int argv_len;           /* Size of argv array (may be more than argc) */
    int original_argc;      /* Num of arguments of original command if arguments were rewritten. */
    robj **original_argv;   /* Arguments of original command if arguments were rewritten. */
    size_t argv_len_sum;    /* Sum of lengths of objects in argv list. */
    struct redisCommand *cmd, *lastcmd;  /* Last command executed. */
    struct redisCommand *realcmd; /* The original command that was executed by the client,
                                     Used to update error stats in case the c->cmd was modified
                                     during the command invocation (like on GEOADD for example). */
    user *user;             /* User associated with this connection. If the
                               user is set to NULL the connection can do
                               anything (admin). */
    int reqtype;            /* Request protocol type: PROTO_REQ_* */
    int multibulklen;       /* Number of multi bulk arguments left to read. */
    long bulklen;           /* Length of bulk argument in multi bulk request. */
    list *reply;            /* List of reply objects to send to the client. */
    unsigned long long reply_bytes; /* Tot bytes of objects in reply list. */
    list *deferred_reply_errors;    /* Used for module thread safe contexts. */
    size_t sentlen;         /* Amount of bytes already sent in the current
                               buffer or object being sent. */
    time_t ctime;           /* Client creation time. */
    long duration;          /* Current command duration. Used for measuring latency of blocking/non-blocking cmds */
    int slot;               /* The slot the client is executing against. Set to -1 if no slot is being used */
    dictEntry *cur_script;  /* Cached pointer to the dictEntry of the script being executed. */
    time_t lastinteraction; /* Time of the last interaction, used for timeout */
    time_t obuf_soft_limit_reached_time;
    int authenticated;      /* Needed when the default user requires auth. */
    int replstate;          /* Replication state if this is a slave. */
    int repl_start_cmd_stream_on_ack; /* Install slave write handler on first ACK. */
    int repldbfd;           /* Replication DB file descriptor. */
    off_t repldboff;        /* Replication DB file offset. */
    off_t repldbsize;       /* Replication DB file size. */
    sds replpreamble;       /* Replication DB preamble. */
    long long read_reploff; /* Read replication offset if this is a master. */
    long long reploff;      /* Applied replication offset if this is a master. */
    long long repl_applied; /* Applied replication data count in querybuf, if this is a replica. */
    long long repl_ack_off; /* Replication ack offset, if this is a slave. */
    long long repl_aof_off; /* Replication AOF fsync ack offset, if this is a slave. */
    long long repl_ack_time;/* Replication ack time, if this is a slave. */
    long long repl_last_partial_write; /* The last time the server did a partial write from the RDB child pipe to this replica  */
    long long psync_initial_offset; /* FULLRESYNC reply offset other slaves
                                       copying this slave output buffer
                                       should use. */
    char replid[CONFIG_RUN_ID_SIZE+1]; /* Master replication ID (if master). */
    int slave_listening_port; /* As configured with: REPLCONF listening-port */
    char *slave_addr;       /* Optionally given by REPLCONF ip-address */
    int slave_capa;         /* Slave capabilities: SLAVE_CAPA_* bitwise OR. */
    int slave_req;          /* Slave requirements: SLAVE_REQ_* */
    multiState mstate;      /* MULTI/EXEC state */
    blockingState bstate;     /* blocking state */
    long long woff;         /* Last write global replication offset. */
    list *watched_keys;     /* Keys WATCHED for MULTI/EXEC CAS */
    dict *pubsub_channels;  /* channels a client is interested in (SUBSCRIBE) */
    dict *pubsub_patterns;  /* patterns a client is interested in (PSUBSCRIBE) */
    dict *pubsubshard_channels;  /* shard level channels a client is interested in (SSUBSCRIBE) */
    sds peerid;             /* Cached peer ID. */
    sds sockname;           /* Cached connection target address. */
    listNode *client_list_node; /* list node in client list */
    listNode *postponed_list_node; /* list node within the postponed list */
    listNode *pending_read_list_node; /* list node in clients pending read list */
    void *module_blocked_client; /* Pointer to the RedisModuleBlockedClient associated with this
                                  * client. This is set in case of module authentication before the
                                  * unblocked client is reprocessed to handle reply callbacks. */
    void *module_auth_ctx; /* Ongoing / attempted module based auth callback's ctx.
                            * This is only tracked within the context of the command attempting
                            * authentication. If not NULL, it means module auth is in progress. */
    RedisModuleUserChangedFunc auth_callback; /* Module callback to execute
                                               * when the authenticated user
                                               * changes. */
    void *auth_callback_privdata; /* Private data that is passed when the auth
                                   * changed callback is executed. Opaque for
                                   * Redis Core. */
    void *auth_module;      /* The module that owns the callback, which is used
                             * to disconnect the client if the module is
                             * unloaded for cleanup. Opaque for Redis Core.*/

    /* If this client is in tracking mode and this field is non zero,
     * invalidation messages for keys fetched by this client will be sent to
     * the specified client ID. */
    uint64_t client_tracking_redirection;
    rax *client_tracking_prefixes; /* A dictionary of prefixes we are already
                                      subscribed to in BCAST mode, in the
                                      context of client side caching. */
    /* In updateClientMemoryUsage() we track the memory usage of
     * each client and add it to the sum of all the clients of a given type,
     * however we need to remember what was the old contribution of each
     * client, and in which category the client was, in order to remove it
     * before adding it the new value. */
    size_t last_memory_usage;
    int last_memory_type;

    listNode *mem_usage_bucket_node;
    clientMemUsageBucket *mem_usage_bucket;

    listNode *ref_repl_buf_node; /* Referenced node of replication buffer blocks,
                                  * see the definition of replBufBlock. */
    size_t ref_block_pos;        /* Access position of referenced buffer block,
                                  * i.e. the next offset to send. */

    /* list node in clients_pending_write list */
    listNode clients_pending_write_node;
    /* Response buffer */
    size_t buf_peak; /* Peak used size of buffer in last 5 sec interval. */
    mstime_t buf_peak_last_reset_time; /* keeps the last time the buffer peak value was reset */
    int bufpos;
    size_t buf_usable_size; /* Usable size of buffer. */
    char *buf;
#ifdef LOG_REQ_RES
    clientReqResInfo reqres;
#endif
} client;

/* ACL information */
typedef struct aclInfo {
    long long user_auth_failures; /* Auth failure counts on user level */
    long long invalid_cmd_accesses; /* Invalid command accesses that user doesn't have permission to */
    long long invalid_key_accesses; /* Invalid key accesses that user doesn't have permission to */
    long long invalid_channel_accesses; /* Invalid channel accesses that user doesn't have permission to */
} aclInfo;

struct saveparam {
    time_t seconds;
    int changes;
};

struct moduleLoadQueueEntry {
    sds path;
    int argc;
    robj **argv;
};

struct sentinelLoadQueueEntry {
    int argc;
    sds *argv;
    int linenum;
    sds line;
};

struct sentinelConfig {
    list *pre_monitor_cfg;
    list *monitor_cfg;
    list *post_monitor_cfg;
};

struct sharedObjectsStruct {
    robj *ok, *err, *emptybulk, *czero, *cone, *pong, *space,
    *queued, *null[4], *nullarray[4], *emptymap[4], *emptyset[4],
    *emptyarray, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
    *outofrangeerr, *noscripterr, *loadingerr,
    *slowevalerr, *slowscripterr, *slowmoduleerr, *bgsaveerr,
    *masterdownerr, *roslaveerr, *execaborterr, *noautherr, *noreplicaserr,
    *busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk,
    *unsubscribebulk, *psubscribebulk, *punsubscribebulk, *del, *unlink,
    *rpop, *lpop, *lpush, *rpoplpush, *lmove, *blmove, *zpopmin, *zpopmax,
    *emptyscan, *multi, *exec, *left, *right, *hset, *srem, *xgroup, *xclaim,
    *script, *replconf, *eval, *persist, *set, *pexpireat, *pexpire,
    *hdel, *hpexpireat,
    *time, *pxat, *absttl, *retrycount, *force, *justid, *entriesread,
    *lastid, *ping, *setid, *keepttl, *load, *createconsumer,
    *getack, *special_asterick, *special_equals, *default_username, *redacted,
    *ssubscribebulk,*sunsubscribebulk, *smessagebulk,
    *select[PROTO_SHARED_SELECT_CMDS],
    *integers[OBJ_SHARED_INTEGERS],
    *mbulkhdr[OBJ_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
    *bulkhdr[OBJ_SHARED_BULKHDR_LEN],  /* "$<value>\r\n" */
    *maphdr[OBJ_SHARED_BULKHDR_LEN],   /* "%<value>\r\n" */
    *sethdr[OBJ_SHARED_BULKHDR_LEN];   /* "~<value>\r\n" */
    sds minstring, maxstring;
};

/* ZSETs use a specialized version of Skiplists */
typedef struct zskiplistNode {
    sds ele;
    double score;
    struct zskiplistNode *backward;
    struct zskiplistLevel {
        struct zskiplistNode *forward;
        unsigned long span;
    } level[];
} zskiplistNode;

typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct zset {
    dict *dict;
    zskiplist *zsl;
} zset;

typedef struct clientBufferLimitsConfig {
    unsigned long long hard_limit_bytes;
    unsigned long long soft_limit_bytes;
    time_t soft_limit_seconds;
} clientBufferLimitsConfig;

extern clientBufferLimitsConfig clientBufferLimitsDefaults[CLIENT_TYPE_OBUF_COUNT];

/* The redisOp structure defines a Redis Operation, that is an instance of
 * a command with an argument vector, database ID, propagation target
 * (PROPAGATE_*), and command pointer.
 *
 * Currently only used to additionally propagate more commands to AOF/Replication
 * after the propagation of the executed command. */
typedef struct redisOp {
    robj **argv;
    int argc, dbid, target;
} redisOp;

/* Defines an array of Redis operations. There is an API to add to this
 * structure in an easy way.
 *
 * int redisOpArrayAppend(redisOpArray *oa, int dbid, robj **argv, int argc, int target);
 * void redisOpArrayFree(redisOpArray *oa);
 */
typedef struct redisOpArray {
    redisOp *ops;
    int numops;
    int capacity;
} redisOpArray;

/* This structure is returned by the getMemoryOverheadData() function in
 * order to return memory overhead information. */
struct redisMemOverhead {
    size_t peak_allocated;
    size_t total_allocated;
    size_t startup_allocated;
    size_t repl_backlog;
    size_t clients_slaves;
    size_t clients_normal;
    size_t cluster_links;
    size_t aof_buffer;
    size_t lua_caches;
    size_t functions_caches;
    size_t overhead_total;
    size_t dataset;
    size_t total_keys;
    size_t bytes_per_key;
    float dataset_perc;
    float peak_perc;
    float total_frag;
    ssize_t total_frag_bytes;
    float allocator_frag;
    ssize_t allocator_frag_bytes;
    float allocator_rss;
    ssize_t allocator_rss_bytes;
    float rss_extra;
    size_t rss_extra_bytes;
    size_t num_dbs;
    size_t overhead_db_hashtable_lut;
    size_t overhead_db_hashtable_rehashing;
    unsigned long db_dict_rehashing_count;
    struct {
        size_t dbid;
        size_t overhead_ht_main;
        size_t overhead_ht_expires;
    } *db;
};

/* Replication error behavior determines the replica behavior
 * when it receives an error over the replication stream. In
 * either case the error is logged. */
typedef enum {
    PROPAGATION_ERR_BEHAVIOR_IGNORE = 0,
    PROPAGATION_ERR_BEHAVIOR_PANIC,
    PROPAGATION_ERR_BEHAVIOR_PANIC_ON_REPLICAS
} replicationErrorBehavior;

/* This structure can be optionally passed to RDB save/load functions in
 * order to implement additional functionalities, by storing and loading
 * metadata to the RDB file.
 *
 * For example, to use select a DB at load time, useful in
 * replication in order to make sure that chained slaves (slaves of slaves)
 * select the correct DB and are able to accept the stream coming from the
 * top-level master. */
typedef struct rdbSaveInfo {
    /* Used saving and loading. */
    int repl_stream_db;  /* DB to select in server.master client. */

    /* Used only loading. */
    int repl_id_is_set;  /* True if repl_id field is set. */
    char repl_id[CONFIG_RUN_ID_SIZE+1];     /* Replication ID. */
    long long repl_offset;                  /* Replication offset. */
} rdbSaveInfo;

#define RDB_SAVE_INFO_INIT {-1,0,"0000000000000000000000000000000000000000",-1}

struct malloc_stats {
    size_t zmalloc_used;
    size_t process_rss;
    size_t allocator_allocated;
    size_t allocator_active;
    size_t allocator_resident;
    size_t allocator_muzzy;
    size_t allocator_frag_smallbins_bytes;
    size_t lua_allocator_allocated;
    size_t lua_allocator_active;
    size_t lua_allocator_resident;
    size_t lua_allocator_frag_smallbins_bytes;
};

/*-----------------------------------------------------------------------------
 * TLS Context Configuration
 *----------------------------------------------------------------------------*/

typedef struct redisTLSContextConfig {
    char *cert_file;                /* Server side and optionally client side cert file name */
    char *key_file;                 /* Private key filename for cert_file */
    char *key_file_pass;            /* Optional password for key_file */
    char *client_cert_file;         /* Certificate to use as a client; if none, use cert_file */
    char *client_key_file;          /* Private key filename for client_cert_file */
    char *client_key_file_pass;     /* Optional password for client_key_file */
    char *dh_params_file;
    char *ca_cert_file;
    char *ca_cert_dir;
    char *protocols;
    char *ciphers;
    char *ciphersuites;
    int prefer_server_ciphers;
    int session_caching;
    int session_cache_size;
    int session_cache_timeout;
} redisTLSContextConfig;

/*-----------------------------------------------------------------------------
 * AOF manifest definition
 *----------------------------------------------------------------------------*/
typedef enum {
    AOF_FILE_TYPE_BASE  = 'b', /* BASE file */
    AOF_FILE_TYPE_HIST  = 'h', /* HISTORY file */
    AOF_FILE_TYPE_INCR  = 'i', /* INCR file */
} aof_file_type;

typedef struct {
    sds           file_name;  /* file name */
    long long     file_seq;   /* file sequence */
    aof_file_type file_type;  /* file type */
} aofInfo;

typedef struct {
    aofInfo     *base_aof_info;       /* BASE file information. NULL if there is no BASE file. */
    list        *incr_aof_list;       /* INCR AOFs list. We may have multiple INCR AOF when rewrite fails. */
    list        *history_aof_list;    /* HISTORY AOF list. When the AOFRW success, The aofInfo contained in
                                         `base_aof_info` and `incr_aof_list` will be moved to this list. We
                                         will delete these AOF files when AOFRW finish. */
    long long   curr_base_file_seq;   /* The sequence number used by the current BASE file. */
    long long   curr_incr_file_seq;   /* The sequence number used by the current INCR file. */
    int         dirty;                /* 1 Indicates that the aofManifest in the memory is inconsistent with
                                         disk, we need to persist it immediately. */
} aofManifest;

/*-----------------------------------------------------------------------------
 * Global server state
 *----------------------------------------------------------------------------*/

/* AIX defines hz to __hz, we don't use this define and in order to allow
 * Redis build on AIX we need to undef it. */
#ifdef _AIX
#undef hz
#endif

#define CHILD_TYPE_NONE 0
#define CHILD_TYPE_RDB 1
#define CHILD_TYPE_AOF 2
#define CHILD_TYPE_LDB 3
#define CHILD_TYPE_MODULE 4

typedef enum childInfoType {
    CHILD_INFO_TYPE_CURRENT_INFO,
    CHILD_INFO_TYPE_AOF_COW_SIZE,
    CHILD_INFO_TYPE_RDB_COW_SIZE,
    CHILD_INFO_TYPE_MODULE_COW_SIZE
} childInfoType;

struct redisServer {
    /* General */
    pid_t pid;                  /* Main process pid. */
    pthread_t main_thread_id;         /* Main thread id */
    char *configfile;           /* Absolute config file path, or NULL */
    char *executable;           /* Absolute executable file path. */
    char **exec_argv;           /* Executable argv vector (copy). */
    int dynamic_hz;             /* Change hz value depending on # of clients. */
    int config_hz;              /* Configured HZ value. May be different than
                                   the actual 'hz' field value if dynamic-hz
                                   is enabled. */
    mode_t umask;               /* The umask value of the process on startup */
    int hz;                     /* serverCron() calls frequency in hertz */
    int in_fork_child;          /* indication that this is a fork child */
    redisDb *db;
    dict *commands;             /* Command table */
    dict *orig_commands;        /* Command table before command renaming. */
    aeEventLoop *el;
    rax *errors;                /* Errors table */
    int errors_enabled;         /* If true, errorstats is enabled, and we will add new errors. */
    unsigned int lruclock; /* Clock for LRU eviction */
    volatile sig_atomic_t shutdown_asap; /* Shutdown ordered by signal handler. */
    mstime_t shutdown_mstime;   /* Timestamp to limit graceful shutdown. */
    int last_sig_received;      /* Indicates the last SIGNAL received, if any (e.g., SIGINT or SIGTERM). */
    int shutdown_flags;         /* Flags passed to prepareForShutdown(). */
    int activerehashing;        /* Incremental rehash in serverCron() */
    int active_defrag_running;  /* Active defragmentation running (holds current scan aggressiveness) */
    char *pidfile;              /* PID file path */
    int arch_bits;              /* 32 or 64 depending on sizeof(long) */
    int cronloops;              /* Number of times the cron function run */
    char runid[CONFIG_RUN_ID_SIZE+1];  /* ID always different at every exec. */
    int sentinel_mode;          /* True if this instance is a Sentinel. */
    size_t initial_memory_usage; /* Bytes used after initialization. */
    int always_show_logo;       /* Show logo even for non-stdout logging. */
    int in_exec;                /* Are we inside EXEC? */
    int busy_module_yield_flags;         /* Are we inside a busy module? (triggered by RM_Yield). see BUSY_MODULE_YIELD_ flags. */
    const char *busy_module_yield_reply; /* When non-null, we are inside RM_Yield. */
    char *ignore_warnings;      /* Config: warnings that should be ignored. */
    int client_pause_in_transaction; /* Was a client pause executed during this Exec? */
    int thp_enabled;                 /* If true, THP is enabled. */
    size_t page_size;                /* The page size of OS. */
    /* Modules */
    dict *moduleapi;            /* Exported core APIs dictionary for modules. */
    dict *sharedapi;            /* Like moduleapi but containing the APIs that
                                   modules share with each other. */
    dict *module_configs_queue; /* Dict that stores module configurations from .conf file until after modules are loaded during startup or arguments to loadex. */
    list *loadmodule_queue;     /* List of modules to load at startup. */
    int module_pipe[2];         /* Pipe used to awake the event loop by module threads. */
    pid_t child_pid;            /* PID of current child */
    int child_type;             /* Type of current child */
    redisAtomic int module_gil_acquring; /* Indicates whether the GIL is being acquiring by the main thread. */
    /* Networking */
    int port;                   /* TCP listening port */
    int tls_port;               /* TLS listening port */
    int tcp_backlog;            /* TCP listen() backlog */
    char *bindaddr[CONFIG_BINDADDR_MAX]; /* Addresses we should bind to */
    int bindaddr_count;         /* Number of addresses in server.bindaddr[] */
    char *bind_source_addr;     /* Source address to bind on for outgoing connections */
    char *unixsocket;           /* UNIX socket path */
    unsigned int unixsocketperm; /* UNIX socket permission (see mode_t) */
    connListener listeners[CONN_TYPE_MAX]; /* TCP/Unix/TLS even more types */
    uint32_t socket_mark_id;    /* ID for listen socket marking */
    connListener clistener;     /* Cluster bus listener */
    list *clients;              /* List of active clients */
    list *clients_to_close;     /* Clients to close asynchronously */
    list *clients_pending_write; /* There is to write or install handler. */
    list *clients_pending_read;  /* Client has pending read socket buffers. */
    list *slaves, *monitors;    /* List of slaves and MONITORs */
    client *current_client;     /* The client that triggered the command execution (External or AOF). */
    client *executing_client;   /* The client executing the current command (possibly script or module). */

#ifdef LOG_REQ_RES
    char *req_res_logfile; /* Path of log file for logging all requests and their replies. If NULL, no logging will be performed */
    unsigned int client_default_resp;
#endif

    /* Stuff for client mem eviction */
    clientMemUsageBucket* client_mem_usage_buckets;

    rax *clients_timeout_table; /* Radix tree for blocked clients timeouts. */
    int execution_nesting;      /* Execution nesting level.
                                 * e.g. call(), async module stuff (timers, events, etc.),
                                 * cron stuff (active expire, eviction) */
    rax *clients_index;         /* Active clients dictionary by client ID. */
    uint32_t paused_actions;   /* Bitmask of actions that are currently paused */
    list *postponed_clients;       /* List of postponed clients */
    pause_event client_pause_per_purpose[NUM_PAUSE_PURPOSES];
    char neterr[ANET_ERR_LEN];   /* Error buffer for anet.c */
    dict *migrate_cached_sockets;/* MIGRATE cached sockets */
    redisAtomic uint64_t next_client_id; /* Next client unique ID. Incremental. */
    int protected_mode;         /* Don't accept external connections. */
    int io_threads_num;         /* Number of IO threads to use. */
    int io_threads_do_reads;    /* Read and parse from IO threads? */
    int io_threads_active;      /* Is IO threads currently active? */
    long long events_processed_while_blocked; /* processEventsWhileBlocked() */
    int enable_protected_configs;    /* Enable the modification of protected configs, see PROTECTED_ACTION_ALLOWED_* */
    int enable_debug_cmd;            /* Enable DEBUG commands, see PROTECTED_ACTION_ALLOWED_* */
    int enable_module_cmd;           /* Enable MODULE commands, see PROTECTED_ACTION_ALLOWED_* */

    /* RDB / AOF loading information */
    volatile sig_atomic_t loading; /* We are loading data from disk if true */
    volatile sig_atomic_t async_loading; /* We are loading data without blocking the db being served */
    off_t loading_total_bytes;
    off_t loading_rdb_used_mem;
    off_t loading_loaded_bytes;
    time_t loading_start_time;
    off_t loading_process_events_interval_bytes;
    /* Fields used only for stats */
    time_t stat_starttime;          /* Server start time */
    long long stat_numcommands;     /* Number of processed commands */
    long long stat_numconnections;  /* Number of connections received */
    long long stat_expiredkeys;     /* Number of expired keys */
    long long stat_expired_subkeys; /* Number of expired subkeys (Currently only hash-fields) */
    double stat_expired_stale_perc; /* Percentage of keys probably expired */
    long long stat_expired_time_cap_reached_count; /* Early expire cycle stops.*/
    long long stat_expire_cycle_time_used; /* Cumulative microseconds used. */
    long long stat_evictedkeys;     /* Number of evicted keys (maxmemory) */
    long long stat_evictedclients;  /* Number of evicted clients */
    long long stat_evictedscripts;  /* Number of evicted lua scripts. */
    long long stat_total_eviction_exceeded_time;  /* Total time over the memory limit, unit us */
    monotime stat_last_eviction_exceeded_time;  /* Timestamp of current eviction start, unit us */
    long long stat_keyspace_hits;   /* Number of successful lookups of keys */
    long long stat_keyspace_misses; /* Number of failed lookups of keys */
    long long stat_active_defrag_hits;      /* number of allocations moved */
    long long stat_active_defrag_misses;    /* number of allocations scanned but not moved */
    long long stat_active_defrag_key_hits;  /* number of keys with moved allocations */
    long long stat_active_defrag_key_misses;/* number of keys scanned and not moved */
    long long stat_active_defrag_scanned;   /* number of dictEntries scanned */
    long long stat_total_active_defrag_time; /* Total time memory fragmentation over the limit, unit us */
    monotime stat_last_active_defrag_time; /* Timestamp of current active defrag start */
    size_t stat_peak_memory;        /* Max used memory record */
    long long stat_aof_rewrites;    /* number of aof file rewrites performed */
    long long stat_aofrw_consecutive_failures; /* The number of consecutive failures of aofrw */
    long long stat_rdb_saves;       /* number of rdb saves performed */
    long long stat_fork_time;       /* Time needed to perform latest fork() */
    double stat_fork_rate;          /* Fork rate in GB/sec. */
    long long stat_total_forks;     /* Total count of fork. */
    long long stat_rejected_conn;   /* Clients rejected because of maxclients */
    long long stat_sync_full;       /* Number of full resyncs with slaves. */
    long long stat_sync_partial_ok; /* Number of accepted PSYNC requests. */
    long long stat_sync_partial_err;/* Number of unaccepted PSYNC requests. */
    list *slowlog;                  /* SLOWLOG list of commands */
    long long slowlog_entry_id;     /* SLOWLOG current entry ID */
    long long slowlog_log_slower_than; /* SLOWLOG time limit (to get logged) */
    unsigned long slowlog_max_len;     /* SLOWLOG max number of items logged */
    struct malloc_stats cron_malloc_stats; /* sampled in serverCron(). */
    redisAtomic long long stat_net_input_bytes; /* Bytes read from network. */
    redisAtomic long long stat_net_output_bytes; /* Bytes written to network. */
    redisAtomic long long stat_net_repl_input_bytes; /* Bytes read during replication, added to stat_net_input_bytes in 'info'. */
    redisAtomic long long stat_net_repl_output_bytes; /* Bytes written during replication, added to stat_net_output_bytes in 'info'. */
    size_t stat_current_cow_peak;   /* Peak size of copy on write bytes. */
    size_t stat_current_cow_bytes;  /* Copy on write bytes while child is active. */
    monotime stat_current_cow_updated;  /* Last update time of stat_current_cow_bytes */
    size_t stat_current_save_keys_processed;  /* Processed keys while child is active. */
    size_t stat_current_save_keys_total;  /* Number of keys when child started. */
    size_t stat_rdb_cow_bytes;      /* Copy on write bytes during RDB saving. */
    size_t stat_aof_cow_bytes;      /* Copy on write bytes during AOF rewrite. */
    size_t stat_module_cow_bytes;   /* Copy on write bytes during module fork. */
    double stat_module_progress;   /* Module save progress. */
    size_t stat_clients_type_memory[CLIENT_TYPE_COUNT];/* Mem usage by type */
    size_t stat_cluster_links_memory; /* Mem usage by cluster links */
    long long stat_unexpected_error_replies; /* Number of unexpected (aof-loading, replica to master, etc.) error replies */
    long long stat_total_error_replies; /* Total number of issued error replies ( command + rejected errors ) */
    long long stat_dump_payload_sanitizations; /* Number deep dump payloads integrity validations. */
    long long stat_io_reads_processed; /* Number of read events processed by IO / Main threads */
    long long stat_io_writes_processed; /* Number of write events processed by IO / Main threads */
    redisAtomic long long stat_total_reads_processed; /* Total number of read events processed */
    redisAtomic long long stat_total_writes_processed; /* Total number of write events processed */
    redisAtomic long long stat_client_qbuf_limit_disconnections;  /* Total number of clients reached query buf length limit */
    long long stat_client_outbuf_limit_disconnections;  /* Total number of clients reached output buf length limit */
    /* The following two are used to track instantaneous metrics, like
     * number of operations per second, network traffic. */
    struct {
        long long last_sample_base;  /* The divisor of last sample window */
        long long last_sample_value; /* The dividend of last sample window */
        long long samples[STATS_METRIC_SAMPLES];
        int idx;
    } inst_metric[STATS_METRIC_COUNT];
    long long stat_reply_buffer_shrinks; /* Total number of output buffer shrinks */
    long long stat_reply_buffer_expands; /* Total number of output buffer expands */
    monotime el_start;
    /* The following two are used to record the max number of commands executed in one eventloop.
     * Note that commands in transactions are also counted. */
    long long el_cmd_cnt_start;
    long long el_cmd_cnt_max;
    /* The sum of active-expire, active-defrag and all other tasks done by cron and beforeSleep,
       but excluding read, write and AOF, which are counted by other sets of metrics. */
    monotime el_cron_duration;
    durationStats duration_stats[EL_DURATION_TYPE_NUM];

    /* Configuration */
    int verbosity;                  /* Loglevel in redis.conf */
    int hide_user_data_from_log;    /* In the event of an assertion failure, hide command arguments from the operator */
    int maxidletime;                /* Client timeout in seconds */
    int tcpkeepalive;               /* Set SO_KEEPALIVE if non-zero. */
    int active_expire_enabled;      /* Can be disabled for testing purposes. */
    int active_expire_effort;       /* From 1 (default) to 10, active effort. */
    int lazy_expire_disabled;       /* If > 0, don't trigger lazy expire */
    int active_defrag_enabled;
    int sanitize_dump_payload;      /* Enables deep sanitization for ziplist and listpack in RDB and RESTORE. */
    int skip_checksum_validation;   /* Disable checksum validation for RDB and RESTORE payload. */
    int jemalloc_bg_thread;         /* Enable jemalloc background thread */
    int active_defrag_configuration_changed; /* defrag configuration has been changed and need to reconsider
                                              * active_defrag_running in computeDefragCycles. */
    size_t active_defrag_ignore_bytes; /* minimum amount of fragmentation waste to start active defrag */
    int active_defrag_threshold_lower; /* minimum percentage of fragmentation to start active defrag */
    int active_defrag_threshold_upper; /* maximum percentage of fragmentation at which we use maximum effort */
    int active_defrag_cycle_min;       /* minimal effort for defrag in CPU percentage */
    int active_defrag_cycle_max;       /* maximal effort for defrag in CPU percentage */
    unsigned long active_defrag_max_scan_fields; /* maximum number of fields of set/hash/zset/list to process from within the main dict scan */
    size_t client_max_querybuf_len; /* Limit for client query buffer length */
    int dbnum;                      /* Total number of configured DBs */
    int supervised;                 /* 1 if supervised, 0 otherwise. */
    int supervised_mode;            /* See SUPERVISED_* */
    int daemonize;                  /* True if running as a daemon */
    int set_proc_title;             /* True if change proc title */
    char *proc_title_template;      /* Process title template format */
    clientBufferLimitsConfig client_obuf_limits[CLIENT_TYPE_OBUF_COUNT];
    int pause_cron;                 /* Don't run cron tasks (debug) */
    int dict_resizing;              /* Whether to allow main dict and expired dict to be resized (debug) */
    int latency_tracking_enabled;   /* 1 if extended latency tracking is enabled, 0 otherwise. */
    double *latency_tracking_info_percentiles; /* Extended latency tracking info output percentile list configuration. */
    int latency_tracking_info_percentiles_len;
    unsigned int max_new_tls_conns_per_cycle; /* The maximum number of tls connections that will be accepted during each invocation of the event loop. */
    unsigned int max_new_conns_per_cycle; /* The maximum number of tcp connections that will be accepted during each invocation of the event loop. */
    /* AOF persistence */
    int aof_enabled;                /* AOF configuration */
    int aof_state;                  /* AOF_(ON|OFF|WAIT_REWRITE) */
    int aof_fsync;                  /* Kind of fsync() policy */
    char *aof_filename;             /* Basename of the AOF file and manifest file */
    char *aof_dirname;              /* Name of the AOF directory */
    int aof_no_fsync_on_rewrite;    /* Don't fsync if a rewrite is in prog. */
    int aof_rewrite_perc;           /* Rewrite AOF if % growth is > M and... */
    off_t aof_rewrite_min_size;     /* the AOF file is at least N bytes. */
    off_t aof_rewrite_base_size;    /* AOF size on latest startup or rewrite. */
    off_t aof_current_size;         /* AOF current size (Including BASE + INCRs). */
    off_t aof_last_incr_size;       /* The size of the latest incr AOF. */
    off_t aof_last_incr_fsync_offset; /* AOF offset which is already requested to be synced to disk.
                                       * Compare with the aof_last_incr_size. */
    int aof_flush_sleep;            /* Micros to sleep before flush. (used by tests) */
    int aof_rewrite_scheduled;      /* Rewrite once BGSAVE terminates. */
    sds aof_buf;      /* AOF buffer, written before entering the event loop */
    int aof_fd;       /* File descriptor of currently selected AOF file */
    int aof_selected_db; /* Currently selected DB in AOF */
    mstime_t aof_flush_postponed_start; /* mstime of postponed AOF flush */
    mstime_t aof_last_fsync;            /* mstime of last fsync() */
    time_t aof_rewrite_time_last;   /* Time used by last AOF rewrite run. */
    time_t aof_rewrite_time_start;  /* Current AOF rewrite start time. */
    time_t aof_cur_timestamp;       /* Current record timestamp in AOF */
    int aof_timestamp_enabled;      /* Enable record timestamp in AOF */
    int aof_lastbgrewrite_status;   /* C_OK or C_ERR */
    unsigned long aof_delayed_fsync;  /* delayed AOF fsync() counter */
    int aof_rewrite_incremental_fsync;/* fsync incrementally while aof rewriting? */
    int rdb_save_incremental_fsync;   /* fsync incrementally while rdb saving? */
    int aof_last_write_status;      /* C_OK or C_ERR */
    int aof_last_write_errno;       /* Valid if aof write/fsync status is ERR */
    int aof_load_truncated;         /* Don't stop on unexpected AOF EOF. */
    int aof_use_rdb_preamble;       /* Specify base AOF to use RDB encoding on AOF rewrites. */
    redisAtomic int aof_bio_fsync_status; /* Status of AOF fsync in bio job. */
    redisAtomic int aof_bio_fsync_errno;  /* Errno of AOF fsync in bio job. */
    aofManifest *aof_manifest;       /* Used to track AOFs. */
    int aof_disable_auto_gc;         /* If disable automatically deleting HISTORY type AOFs?
                                        default no. (for testings). */

    /* RDB persistence */
    long long dirty;                /* Changes to DB from the last save */
    long long dirty_before_bgsave;  /* Used to restore dirty on failed BGSAVE */
    long long rdb_last_load_keys_expired;  /* number of expired keys when loading RDB */
    long long rdb_last_load_keys_loaded;   /* number of loaded keys when loading RDB */
    int bgsave_aborted;             /* Set when killing a child, to treat it as aborted even if it succeeds. */
    struct saveparam *saveparams;   /* Save points array for RDB */
    int saveparamslen;              /* Number of saving points */
    char *rdb_filename;             /* Name of RDB file */
    int rdb_compression;            /* Use compression in RDB? */
    int rdb_checksum;               /* Use RDB checksum? */
    int rdb_del_sync_files;         /* Remove RDB files used only for SYNC if
                                       the instance does not use persistence. */
    time_t lastsave;                /* Unix time of last successful save */
    time_t lastbgsave_try;          /* Unix time of last attempted bgsave */
    time_t rdb_save_time_last;      /* Time used by last RDB save run. */
    time_t rdb_save_time_start;     /* Current RDB save start time. */
    int rdb_bgsave_scheduled;       /* BGSAVE when possible if true. */
    int rdb_child_type;             /* Type of save by active child. */
    int lastbgsave_status;          /* C_OK or C_ERR */
    int stop_writes_on_bgsave_err;  /* Don't allow writes if can't BGSAVE */
    int rdb_pipe_read;              /* RDB pipe used to transfer the rdb data */
                                    /* to the parent process in diskless repl. */
    int rdb_child_exit_pipe;        /* Used by the diskless parent allow child exit. */
    connection **rdb_pipe_conns;    /* Connections which are currently the */
    int rdb_pipe_numconns;          /* target of diskless rdb fork child. */
    int rdb_pipe_numconns_writing;  /* Number of rdb conns with pending writes. */
    char *rdb_pipe_buff;            /* In diskless replication, this buffer holds data */
    int rdb_pipe_bufflen;           /* that was read from the rdb pipe. */
    int rdb_key_save_delay;         /* Delay in microseconds between keys while
                                     * writing aof or rdb. (for testings). negative
                                     * value means fractions of microseconds (on average). */
    int key_load_delay;             /* Delay in microseconds between keys while
                                     * loading aof or rdb. (for testings). negative
                                     * value means fractions of microseconds (on average). */
    /* Pipe and data structures for child -> parent info sharing. */
    int child_info_pipe[2];         /* Pipe used to write the child_info_data. */
    int child_info_nread;           /* Num of bytes of the last read from pipe */
    /* Propagation of commands in AOF / replication */
    redisOpArray also_propagate;    /* Additional command to propagate. */
    int replication_allowed;        /* Are we allowed to replicate? */
    /* Logging */
    char *logfile;                  /* Path of log file */
    int syslog_enabled;             /* Is syslog enabled? */
    char *syslog_ident;             /* Syslog ident */
    int syslog_facility;            /* Syslog facility */
    int crashlog_enabled;           /* Enable signal handler for crashlog.
                                     * disable for clean core dumps. */
    int memcheck_enabled;           /* Enable memory check on crash. */
    int use_exit_on_panic;          /* Use exit() on panic and assert rather than
                                     * abort(). useful for Valgrind. */
    /* Shutdown */
    int shutdown_timeout;           /* Graceful shutdown time limit in seconds. */
    int shutdown_on_sigint;         /* Shutdown flags configured for SIGINT. */
    int shutdown_on_sigterm;        /* Shutdown flags configured for SIGTERM. */

    /* Replication (master) */
    char replid[CONFIG_RUN_ID_SIZE+1];  /* My current replication ID. */
    char replid2[CONFIG_RUN_ID_SIZE+1]; /* replid inherited from master*/
    long long master_repl_offset;   /* My current replication offset */
    long long second_replid_offset; /* Accept offsets up to this for replid2. */
    redisAtomic long long fsynced_reploff_pending;/* Largest replication offset to
                                     * potentially have been fsynced, applied to
                                       fsynced_reploff only when AOF state is AOF_ON
                                       (not during the initial rewrite) */
    long long fsynced_reploff;      /* Largest replication offset that has been confirmed to be fsynced */
    int slaveseldb;                 /* Last SELECTed DB in replication output */
    int repl_ping_slave_period;     /* Master pings the slave every N seconds */
    replBacklog *repl_backlog;      /* Replication backlog for partial syncs */
    long long repl_backlog_size;    /* Backlog circular buffer size */
    time_t repl_backlog_time_limit; /* Time without slaves after the backlog
                                       gets released. */
    time_t repl_no_slaves_since;    /* We have no slaves since that time.
                                       Only valid if server.slaves len is 0. */
    int repl_min_slaves_to_write;   /* Min number of slaves to write. */
    int repl_min_slaves_max_lag;    /* Max lag of <count> slaves to write. */
    int repl_good_slaves_count;     /* Number of slaves with lag <= max_lag. */
    int repl_diskless_sync;         /* Master send RDB to slaves sockets directly. */
    int repl_diskless_load;         /* Slave parse RDB directly from the socket.
                                     * see REPL_DISKLESS_LOAD_* enum */
    int repl_diskless_sync_delay;   /* Delay to start a diskless repl BGSAVE. */
    int repl_diskless_sync_max_replicas;/* Max replicas for diskless repl BGSAVE
                                         * delay (start sooner if they all connect). */
    size_t repl_buffer_mem;         /* The memory of replication buffer. */
    list *repl_buffer_blocks;       /* Replication buffers blocks list
                                     * (serving replica clients and repl backlog) */
    /* Replication (slave) */
    char *masteruser;               /* AUTH with this user and masterauth with master */
    sds masterauth;                 /* AUTH with this password with master */
    char *masterhost;               /* Hostname of master */
    int masterport;                 /* Port of master */
    int repl_timeout;               /* Timeout after N seconds of master idle */
    client *master;     /* Client that is master for this slave */
    client *cached_master; /* Cached master to be reused for PSYNC. */
    int repl_syncio_timeout; /* Timeout for synchronous I/O calls */
    int repl_state;          /* Replication status if the instance is a slave */
    off_t repl_transfer_size; /* Size of RDB to read from master during sync. */
    off_t repl_transfer_read; /* Amount of RDB read from master during sync. */
    off_t repl_transfer_last_fsync_off; /* Offset when we fsync-ed last time. */
    connection *repl_transfer_s;     /* Slave -> Master SYNC connection */
    int repl_transfer_fd;    /* Slave -> Master SYNC temp file descriptor */
    char *repl_transfer_tmpfile; /* Slave-> master SYNC temp file name */
    time_t repl_transfer_lastio; /* Unix time of the latest read, for timeout */
    int repl_serve_stale_data; /* Serve stale data when link is down? */
    int repl_slave_ro;          /* Slave is read only? */
    int repl_slave_ignore_maxmemory;    /* If true slaves do not evict. */
    time_t repl_down_since; /* Unix time at which link with master went down */
    int repl_disable_tcp_nodelay;   /* Disable TCP_NODELAY after SYNC? */
    int slave_priority;             /* Reported in INFO and used by Sentinel. */
    int replica_announced;          /* If true, replica is announced by Sentinel */
    int slave_announce_port;        /* Give the master this listening port. */
    char *slave_announce_ip;        /* Give the master this ip address. */
    int propagation_error_behavior; /* Configures the behavior of the replica
                                     * when it receives an error on the replication stream */
    int repl_ignore_disk_write_error;   /* Configures whether replicas panic when unable to
                                         * persist writes to AOF. */
    /* The following two fields is where we store master PSYNC replid/offset
     * while the PSYNC is in progress. At the end we'll copy the fields into
     * the server->master client structure. */
    char master_replid[CONFIG_RUN_ID_SIZE+1];  /* Master PSYNC runid. */
    long long master_initial_offset;           /* Master PSYNC offset. */
    int repl_slave_lazy_flush;          /* Lazy FLUSHALL before loading DB? */
    /* Synchronous replication. */
    list *clients_waiting_acks;         /* Clients waiting in WAIT or WAITAOF. */
    int get_ack_from_slaves;            /* If true we send REPLCONF GETACK. */
    /* Limits */
    unsigned int maxclients;            /* Max number of simultaneous clients */
    unsigned long long maxmemory;   /* Max number of memory bytes to use */
    ssize_t maxmemory_clients;       /* Memory limit for total client buffers */
    int maxmemory_policy;           /* Policy for key eviction */
    int maxmemory_samples;          /* Precision of random sampling */
    int maxmemory_eviction_tenacity;/* Aggressiveness of eviction processing */
    int lfu_log_factor;             /* LFU logarithmic counter factor. */
    int lfu_decay_time;             /* LFU counter decay factor. */
    long long proto_max_bulk_len;   /* Protocol bulk length maximum size. */
    int oom_score_adj_values[CONFIG_OOM_COUNT];   /* Linux oom_score_adj configuration */
    int oom_score_adj;                            /* If true, oom_score_adj is managed */
    int disable_thp;                              /* If true, disable THP by syscall */
    /* Blocked clients */
    unsigned int blocked_clients;   /* # of clients executing a blocking cmd.*/
    unsigned int blocked_clients_by_type[BLOCKED_NUM];
    list *unblocked_clients; /* list of clients to unblock before next loop */
    list *ready_keys;        /* List of readyList structures for BLPOP & co */
    /* Client side caching. */
    unsigned int tracking_clients;  /* # of clients with tracking enabled.*/
    size_t tracking_table_max_keys; /* Max number of keys in tracking table. */
    list *tracking_pending_keys; /* tracking invalidation keys pending to flush */
    list *pending_push_messages; /* pending publish or other push messages to flush */
    /* Sort parameters - qsort_r() is only available under BSD so we
     * have to take this state global, in order to pass it to sortCompare() */
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
    int sort_store;
    /* Zip structure config, see redis.conf for more information  */
    size_t hash_max_listpack_entries;
    size_t hash_max_listpack_value;
    size_t set_max_intset_entries;
    size_t set_max_listpack_entries;
    size_t set_max_listpack_value;
    size_t zset_max_listpack_entries;
    size_t zset_max_listpack_value;
    size_t hll_sparse_max_bytes;
    size_t stream_node_max_bytes;
    long long stream_node_max_entries;
    /* List parameters */
    int list_max_listpack_size;
    int list_compress_depth;
    /* time cache */
    redisAtomic time_t unixtime; /* Unix time sampled every cron cycle. */
    time_t timezone;            /* Cached timezone. As set by tzset(). */
    redisAtomic int daylight_active; /* Currently in daylight saving time. */
    mstime_t mstime;            /* 'unixtime' in milliseconds. */
    ustime_t ustime;            /* 'unixtime' in microseconds. */
    mstime_t cmd_time_snapshot; /* Time snapshot of the root execution nesting. */
    size_t blocking_op_nesting; /* Nesting level of blocking operation, used to reset blocked_last_cron. */
    long long blocked_last_cron; /* Indicate the mstime of the last time we did cron jobs from a blocking operation */
    /* Pubsub */
    kvstore *pubsub_channels;  /* Map channels to list of subscribed clients */
    dict *pubsub_patterns;  /* A dict of pubsub_patterns */
    int notify_keyspace_events; /* Events to propagate via Pub/Sub. This is an
                                   xor of NOTIFY_... flags. */
    kvstore *pubsubshard_channels;  /* Map shard channels in every slot to list of subscribed clients */
    unsigned int pubsub_clients; /* # of clients in Pub/Sub mode */
    unsigned int watching_clients; /* # of clients are wathcing keys */
    /* Cluster */
    int cluster_enabled;      /* Is cluster enabled? */
    int cluster_port;         /* Set the cluster port for a node. */
    mstime_t cluster_node_timeout; /* Cluster node timeout. */
    mstime_t cluster_ping_interval;    /* A debug configuration for setting how often cluster nodes send ping messages. */
    char *cluster_configfile; /* Cluster auto-generated config file name. */
    struct clusterState *cluster;  /* State of the cluster */
    int cluster_migration_barrier; /* Cluster replicas migration barrier. */
    int cluster_allow_replica_migration; /* Automatic replica migrations to orphaned masters and from empty masters */
    int cluster_slave_validity_factor; /* Slave max data age for failover. */
    int cluster_require_full_coverage; /* If true, put the cluster down if
                                          there is at least an uncovered slot.*/
    int cluster_slave_no_failover;  /* Prevent slave from starting a failover
                                       if the master is in failure state. */
    char *cluster_announce_ip;  /* IP address to announce on cluster bus. */
    char *cluster_announce_hostname;  /* hostname to announce on cluster bus. */
    char *cluster_announce_human_nodename;  /* Human readable node name assigned to a node. */
    int cluster_preferred_endpoint_type; /* Use the announced hostname when available. */
    int cluster_announce_port;     /* base port to announce on cluster bus. */
    int cluster_announce_tls_port; /* TLS port to announce on cluster bus. */
    int cluster_announce_bus_port; /* bus port to announce on cluster bus. */
    int cluster_module_flags;      /* Set of flags that Redis modules are able
                                      to set in order to suppress certain
                                      native Redis Cluster features. Check the
                                      REDISMODULE_CLUSTER_FLAG_*. */
    int cluster_allow_reads_when_down; /* Are reads allowed when the cluster
                                        is down? */
    int cluster_config_file_lock_fd;   /* cluster config fd, will be flocked. */
    unsigned long long cluster_link_msg_queue_limit_bytes;  /* Memory usage limit on individual link msg queue */
    int cluster_drop_packet_filter; /* Debug config that allows tactically
                                   * dropping packets of a specific type */
    /* Scripting */
    unsigned int lua_arena;         /* eval lua arena used in jemalloc. */
    mstime_t busy_reply_threshold;  /* Script / module timeout in milliseconds */
    int pre_command_oom_state;         /* OOM before command (script?) was started */
    int script_disable_deny_script;    /* Allow running commands marked "noscript" inside a script. */
    /* Lazy free */
    int lazyfree_lazy_eviction;
    int lazyfree_lazy_expire;
    int lazyfree_lazy_server_del;
    int lazyfree_lazy_user_del;
    int lazyfree_lazy_user_flush;
    /* Latency monitor */
    long long latency_monitor_threshold;
    dict *latency_events;
    /* ACLs */
    char *acl_filename;           /* ACL Users file. NULL if not configured. */
    unsigned long acllog_max_len; /* Maximum length of the ACL LOG list. */
    sds requirepass;              /* Remember the cleartext password set with
                                     the old "requirepass" directive for
                                     backward compatibility with Redis <= 5. */
    int acl_pubsub_default;      /* Default ACL pub/sub channels flag */
    aclInfo acl_info; /* ACL info */
    /* Assert & bug reporting */
    int watchdog_period;  /* Software watchdog period in ms. 0 = off */
    /* System hardware info */
    size_t system_memory_size;  /* Total memory in system as reported by OS */
    /* TLS Configuration */
    int tls_cluster;
    int tls_replication;
    int tls_auth_clients;
    redisTLSContextConfig tls_ctx_config;
    /* cpu affinity */
    char *server_cpulist; /* cpu affinity list of redis server main/io thread. */
    char *bio_cpulist; /* cpu affinity list of bio thread. */
    char *aof_rewrite_cpulist; /* cpu affinity list of aof rewrite process. */
    char *bgsave_cpulist; /* cpu affinity list of bgsave process. */
    /* Sentinel config */
    struct sentinelConfig *sentinel_config; /* sentinel config to load at startup time. */
    /* Coordinate failover info */
    mstime_t failover_end_time; /* Deadline for failover command. */
    int force_failover; /* If true then failover will be forced at the
                         * deadline, otherwise failover is aborted. */
    char *target_replica_host; /* Failover target host. If null during a
                                * failover then any replica can be used. */
    int target_replica_port; /* Failover target port */
    int failover_state; /* Failover state */
    int cluster_allow_pubsubshard_when_down; /* Is pubsubshard allowed when the cluster
                                                is down, doesn't affect pubsub global. */
    long reply_buffer_peak_reset_time; /* The amount of time (in milliseconds) to wait between reply buffer peak resets */
    int reply_buffer_resizing_enabled; /* Is reply buffer resizing enabled (1 by default) */
    /* Local environment */
    char *locale_collate;
};

/* we use 6 so that all getKeyResult fits a cacheline */
#define MAX_KEYS_BUFFER 6

typedef struct {
    int pos; /* The position of the key within the client array */
    int flags; /* The flags associated with the key access, see
                  CMD_KEY_* for more information */
} keyReference;

/* A result structure for the various getkeys function calls. It lists the
 * keys as indices to the provided argv. This functionality is also re-used
 * for returning channel information.
 */
typedef struct {
    int numkeys;                                 /* Number of key indices return */
    int size;                                    /* Available array size */
    keyReference keysbuf[MAX_KEYS_BUFFER];       /* Pre-allocated buffer, to save heap allocations */
    keyReference *keys;                          /* Key indices array, points to keysbuf or heap */
} getKeysResult;
#define GETKEYS_RESULT_INIT { 0, MAX_KEYS_BUFFER, {{0}}, NULL }

/* Key specs definitions.
 *
 * Brief: This is a scheme that tries to describe the location
 * of key arguments better than the old [first,last,step] scheme
 * which is limited and doesn't fit many commands.
 *
 * There are two steps:
 * 1. begin_search (BS): in which index should we start searching for keys?
 * 2. find_keys (FK): relative to the output of BS, how can we will which args are keys?
 *
 * There are two types of BS:
 * 1. index: key args start at a constant index
 * 2. keyword: key args start just after a specific keyword
 *
 * There are two kinds of FK:
 * 1. range: keys end at a specific index (or relative to the last argument)
 * 2. keynum: there's an arg that contains the number of key args somewhere before the keys themselves
 */

/* WARNING! Must be synced with generate-command-code.py and RedisModuleKeySpecBeginSearchType */
typedef enum {
    KSPEC_BS_INVALID = 0, /* Must be 0 */
    KSPEC_BS_UNKNOWN,
    KSPEC_BS_INDEX,
    KSPEC_BS_KEYWORD
} kspec_bs_type;

/* WARNING! Must be synced with generate-command-code.py and RedisModuleKeySpecFindKeysType */
typedef enum {
    KSPEC_FK_INVALID = 0, /* Must be 0 */
    KSPEC_FK_UNKNOWN,
    KSPEC_FK_RANGE,
    KSPEC_FK_KEYNUM
} kspec_fk_type;

/* WARNING! This struct must match RedisModuleCommandKeySpec */
typedef struct {
    /* Declarative data */
    const char *notes;
    uint64_t flags;
    kspec_bs_type begin_search_type;
    union {
        struct {
            /* The index from which we start the search for keys */
            int pos;
        } index;
        struct {
            /* The keyword that indicates the beginning of key args */
            const char *keyword;
            /* An index in argv from which to start searching.
             * Can be negative, which means start search from the end, in reverse
             * (Example: -2 means to start in reverse from the penultimate arg) */
            int startfrom;
        } keyword;
    } bs;
    kspec_fk_type find_keys_type;
    union {
        /* NOTE: Indices in this struct are relative to the result of the begin_search step!
         * These are: range.lastkey, keynum.keynumidx, keynum.firstkey */
        struct {
            /* Index of the last key.
             * Can be negative, in which case it's not relative. -1 indicating till the last argument,
             * -2 one before the last and so on. */
            int lastkey;
            /* How many args should we skip after finding a key, in order to find the next one. */
            int keystep;
            /* If lastkey is -1, we use limit to stop the search by a factor. 0 and 1 mean no limit.
             * 2 means 1/2 of the remaining args, 3 means 1/3, and so on. */
            int limit;
        } range;
        struct {
            /* Index of the argument containing the number of keys to come */
            int keynumidx;
            /* Index of the fist key (Usually it's just after keynumidx, in
             * which case it should be set to keynumidx+1). */
            int firstkey;
            /* How many args should we skip after finding a key, in order to find the next one. */
            int keystep;
        } keynum;
    } fk;
} keySpec;

#ifdef LOG_REQ_RES

/* Must be synced with generate-command-code.py */
typedef enum {
    JSON_TYPE_STRING,
    JSON_TYPE_INTEGER,
    JSON_TYPE_BOOLEAN,
    JSON_TYPE_OBJECT,
    JSON_TYPE_ARRAY,
} jsonType;

typedef struct jsonObjectElement {
    jsonType type;
    const char *key;
    union {
        const char *string;
        long long integer;
        int boolean;
        struct jsonObject *object;
        struct {
            struct jsonObject **objects;
            int length;
        } array;
    } value;
} jsonObjectElement;

typedef struct jsonObject {
    struct jsonObjectElement *elements;
    int length;
} jsonObject;

#endif

/* WARNING! This struct must match RedisModuleCommandHistoryEntry */
typedef struct {
    const char *since;
    const char *changes;
} commandHistory;

/* Must be synced with COMMAND_GROUP_STR and generate-command-code.py */
typedef enum {
    COMMAND_GROUP_GENERIC,
    COMMAND_GROUP_STRING,
    COMMAND_GROUP_LIST,
    COMMAND_GROUP_SET,
    COMMAND_GROUP_SORTED_SET,
    COMMAND_GROUP_HASH,
    COMMAND_GROUP_PUBSUB,
    COMMAND_GROUP_TRANSACTIONS,
    COMMAND_GROUP_CONNECTION,
    COMMAND_GROUP_SERVER,
    COMMAND_GROUP_SCRIPTING,
    COMMAND_GROUP_HYPERLOGLOG,
    COMMAND_GROUP_CLUSTER,
    COMMAND_GROUP_SENTINEL,
    COMMAND_GROUP_GEO,
    COMMAND_GROUP_STREAM,
    COMMAND_GROUP_BITMAP,
    COMMAND_GROUP_MODULE,
} redisCommandGroup;

typedef void redisCommandProc(client *c);
typedef int redisGetKeysProc(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);

/* Redis command structure.
 *
 * Note that the command table is in commands.c and it is auto-generated.
 *
 * This is the meaning of the flags:
 *
 * CMD_WRITE:       Write command (may modify the key space).
 *
 * CMD_READONLY:    Commands just reading from keys without changing the content.
 *                  Note that commands that don't read from the keyspace such as
 *                  TIME, SELECT, INFO, administrative commands, and connection
 *                  or transaction related commands (multi, exec, discard, ...)
 *                  are not flagged as read-only commands, since they affect the
 *                  server or the connection in other ways.
 *
 * CMD_DENYOOM:     May increase memory usage once called. Don't allow if out
 *                  of memory.
 *
 * CMD_ADMIN:       Administrative command, like SAVE or SHUTDOWN.
 *
 * CMD_PUBSUB:      Pub/Sub related command.
 *
 * CMD_NOSCRIPT:    Command not allowed in scripts.
 *
 * CMD_BLOCKING:    The command has the potential to block the client.
 *
 * CMD_LOADING:     Allow the command while loading the database.
 *
 * CMD_NO_ASYNC_LOADING: Deny during async loading (when a replica uses diskless
 *                       sync swapdb, and allows access to the old dataset)
 *
 * CMD_STALE:       Allow the command while a slave has stale data but is not
 *                  allowed to serve this data. Normally no command is accepted
 *                  in this condition but just a few.
 *
 * CMD_SKIP_MONITOR:  Do not automatically propagate the command on MONITOR.
 *
 * CMD_SKIP_SLOWLOG:  Do not automatically propagate the command to the slowlog.
 *
 * CMD_ASKING:      Perform an implicit ASKING for this command, so the
 *                  command will be accepted in cluster mode if the slot is marked
 *                  as 'importing'.
 *
 * CMD_FAST:        Fast command: O(1) or O(log(N)) command that should never
 *                  delay its execution as long as the kernel scheduler is giving
 *                  us time. Note that commands that may trigger a DEL as a side
 *                  effect (like SET) are not fast commands.
 *
 * CMD_NO_AUTH:     Command doesn't require authentication
 *
 * CMD_MAY_REPLICATE:   Command may produce replication traffic, but should be
 *                      allowed under circumstances where write commands are disallowed.
 *                      Examples include PUBLISH, which replicates pubsub messages,and
 *                      EVAL, which may execute write commands, which are replicated,
 *                      or may just execute read commands. A command can not be marked
 *                      both CMD_WRITE and CMD_MAY_REPLICATE
 *
 * CMD_SENTINEL:    This command is present in sentinel mode.
 *
 * CMD_ONLY_SENTINEL: This command is present only when in sentinel mode.
 *                    And should be removed from redis.
 *
 * CMD_NO_MANDATORY_KEYS: This key arguments for this command are optional.
 *
 * CMD_NO_MULTI: The command is not allowed inside a transaction
 *
 * CMD_ALLOW_BUSY: The command can run while another command is running for
 *                 a long time (timedout script, module command that yields)
 *
 * CMD_TOUCHES_ARBITRARY_KEYS: The command may touch (and cause lazy-expire)
 *                             arbitrary key (i.e not provided in argv)
 *
 * The following additional flags are only used in order to put commands
 * in a specific ACL category. Commands can have multiple ACL categories.
 * See redis.conf for the exact meaning of each.
 *
 * @keyspace, @read, @write, @set, @sortedset, @list, @hash, @string, @bitmap,
 * @hyperloglog, @stream, @admin, @fast, @slow, @pubsub, @blocking, @dangerous,
 * @connection, @transaction, @scripting, @geo.
 *
 * Note that:
 *
 * 1) The read-only flag implies the @read ACL category.
 * 2) The write flag implies the @write ACL category.
 * 3) The fast flag implies the @fast ACL category.
 * 4) The admin flag implies the @admin and @dangerous ACL category.
 * 5) The pub-sub flag implies the @pubsub ACL category.
 * 6) The lack of fast flag implies the @slow ACL category.
 * 7) The non obvious "keyspace" category includes the commands
 *    that interact with keys without having anything to do with
 *    specific data structures, such as: DEL, RENAME, MOVE, SELECT,
 *    TYPE, EXPIRE*, PEXPIRE*, TTL, PTTL, ...
 */
struct redisCommand {
    /* Declarative data */
    const char *declared_name; /* A string representing the command declared_name.
                                * It is a const char * for native commands and SDS for module commands. */
    const char *summary; /* Summary of the command (optional). */
    const char *complexity; /* Complexity description (optional). */
    const char *since; /* Debut version of the command (optional). */
    int doc_flags; /* Flags for documentation (see CMD_DOC_*). */
    const char *replaced_by; /* In case the command is deprecated, this is the successor command. */
    const char *deprecated_since; /* In case the command is deprecated, when did it happen? */
    redisCommandGroup group; /* Command group */
    commandHistory *history; /* History of the command */
    int num_history;
    const char **tips; /* An array of strings that are meant to be tips for clients/proxies regarding this command */
    int num_tips;
    redisCommandProc *proc; /* Command implementation */
    int arity; /* Number of arguments, it is possible to use -N to say >= N */
    uint64_t flags; /* Command flags, see CMD_*. */
    uint64_t acl_categories; /* ACl categories, see ACL_CATEGORY_*. */
    keySpec *key_specs;
    int key_specs_num;
    /* Use a function to determine keys arguments in a command line.
     * Used for Redis Cluster redirect (may be NULL) */
    redisGetKeysProc *getkeys_proc;
    int num_args; /* Length of args array. */
    /* Array of subcommands (may be NULL) */
    struct redisCommand *subcommands;
    /* Array of arguments (may be NULL) */
    struct redisCommandArg *args;
#ifdef LOG_REQ_RES
    /* Reply schema */
    struct jsonObject *reply_schema;
#endif

    /* Runtime populated data */
    long long microseconds, calls, rejected_calls, failed_calls;
    int id;     /* Command ID. This is a progressive ID starting from 0 that
                   is assigned at runtime, and is used in order to check
                   ACLs. A connection is able to execute a given command if
                   the user associated to the connection has this command
                   bit set in the bitmap of allowed commands. */
    sds fullname; /* A SDS string representing the command fullname. */
    struct hdr_histogram* latency_histogram; /*points to the command latency command histogram (unit of time nanosecond) */
    keySpec legacy_range_key_spec; /* The legacy (first,last,step) key spec is
                                     * still maintained (if applicable) so that
                                     * we can still support the reply format of
                                     * COMMAND INFO and COMMAND GETKEYS */
    dict *subcommands_dict; /* A dictionary that holds the subcommands, the key is the subcommand sds name
                             * (not the fullname), and the value is the redisCommand structure pointer. */
    struct redisCommand *parent;
    struct RedisModuleCommand *module_cmd; /* A pointer to the module command data (NULL if native command) */
};

struct redisError {
    long long count;
};

struct redisFunctionSym {
    char *name;
    unsigned long pointer;
};

typedef struct _redisSortObject {
    robj *obj;
    union {
        double score;
        robj *cmpobj;
    } u;
} redisSortObject;

typedef struct _redisSortOperation {
    int type;
    robj *pattern;
} redisSortOperation;

/* Structure to hold list iteration abstraction. */
typedef struct {
    robj *subject;
    unsigned char encoding;
    unsigned char direction; /* Iteration direction */

    unsigned char *lpi; /* listpack iterator */
    quicklistIter *iter; /* quicklist iterator */
} listTypeIterator;

/* Structure for an entry while iterating over a list. */
typedef struct {
    listTypeIterator *li;
    unsigned char *lpe; /* Entry in listpack */
    quicklistEntry entry; /* Entry in quicklist */
} listTypeEntry;

/* Structure to hold set iteration abstraction. */
typedef struct {
    robj *subject;
    int encoding;
    int ii; /* intset iterator */
    dictIterator *di;
    unsigned char *lpi; /* listpack iterator */
} setTypeIterator;

/* Structure to hold hash iteration abstraction. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
typedef struct {
    robj *subject;
    int encoding;

    unsigned char *fptr, *vptr, *tptr;
    uint64_t expire_time; /* Only used with OBJ_ENCODING_LISTPACK_EX */

    dictIterator *di;
    dictEntry *de;
} hashTypeIterator;

#include "stream.h"  /* Stream data type header file. */

#define OBJ_HASH_KEY 1
#define OBJ_HASH_VALUE 2

#define IO_THREADS_OP_IDLE 0
#define IO_THREADS_OP_READ 1
#define IO_THREADS_OP_WRITE 2
extern int io_threads_op;

/* Hash-field data type (of t_hash.c) */
typedef mstr hfield;
extern  mstrKind mstrFieldKind;

/*-----------------------------------------------------------------------------
 * Extern declarations
 *----------------------------------------------------------------------------*/

extern struct redisServer server;
extern struct sharedObjectsStruct shared;
extern dictType objectKeyPointerValueDictType;
extern dictType objectKeyHeapPointerValueDictType;
extern dictType setDictType;
extern dictType BenchmarkDictType;
extern dictType zsetDictType;
extern dictType dbDictType;
extern double R_Zero, R_PosInf, R_NegInf, R_Nan;
extern dictType hashDictType;
extern dictType mstrHashDictType;
extern dictType mstrHashDictTypeWithHFE;
extern dictType stringSetDictType;
extern dictType externalStringType;
extern dictType sdsHashDictType;
extern dictType clientDictType;
extern dictType objToDictDictType;
extern dictType dbExpiresDictType;
extern dictType modulesDictType;
extern dictType sdsReplyDictType;
extern dictType keylistDictType;
extern dict *modules;

extern EbucketsType hashExpireBucketsType;  /* global expires */
extern EbucketsType hashFieldExpireBucketsType; /* local per hash */

/*-----------------------------------------------------------------------------
 * Functions prototypes
 *----------------------------------------------------------------------------*/

/* Command metadata */
void populateCommandLegacyRangeSpec(struct redisCommand *c);

/* Modules */
void moduleInitModulesSystem(void);
void moduleInitModulesSystemLast(void);
void modulesCron(void);
int moduleLoad(const char *path, void **argv, int argc, int is_loadex);
int moduleUnload(sds name, const char **errmsg, int forced_unload);
void moduleLoadFromQueue(void);
int moduleGetCommandKeysViaAPI(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int moduleGetCommandChannelsViaAPI(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
moduleType *moduleTypeLookupModuleByID(uint64_t id);
moduleType *moduleTypeLookupModuleByName(const char *name);
moduleType *moduleTypeLookupModuleByNameIgnoreCase(const char *name);
void moduleTypeNameByID(char *name, uint64_t moduleid);
const char *moduleTypeModuleName(moduleType *mt);
const char *moduleNameFromCommand(struct redisCommand *cmd);
void moduleFreeContext(struct RedisModuleCtx *ctx);
void moduleCallCommandUnblockedHandler(client *c);
int isModuleClientUnblocked(client *c);
void unblockClientFromModule(client *c);
void moduleHandleBlockedClients(void);
void moduleBlockedClientTimedOut(client *c);
void modulePipeReadable(aeEventLoop *el, int fd, void *privdata, int mask);
size_t moduleCount(void);
void moduleAcquireGIL(void);
int moduleTryAcquireGIL(void);
void moduleReleaseGIL(void);
void moduleNotifyKeyspaceEvent(int type, const char *event, robj *key, int dbid);
void firePostExecutionUnitJobs(void);
void moduleCallCommandFilters(client *c);
void modulePostExecutionUnitOperations(void);
void ModuleForkDoneHandler(int exitcode, int bysignal);
int TerminateModuleForkChild(int child_pid, int wait);
ssize_t rdbSaveModulesAux(rio *rdb, int when);
int moduleAllDatatypesHandleErrors(void);
int moduleAllModulesHandleReplAsyncLoad(void);
sds modulesCollectInfo(sds info, dict *sections_dict, int for_crash_report, int sections);
void moduleFireServerEvent(uint64_t eid, int subid, void *data);
void processModuleLoadingProgressEvent(int is_aof);
int moduleTryServeClientBlockedOnKey(client *c, robj *key);
void moduleUnblockClient(client *c);
int moduleBlockedClientMayTimeout(client *c);
int moduleClientIsBlockedOnKeys(client *c);
void moduleNotifyUserChanged(client *c);
void moduleNotifyKeyUnlink(robj *key, robj *val, int dbid, int flags);
size_t moduleGetFreeEffort(robj *key, robj *val, int dbid);
size_t moduleGetMemUsage(robj *key, robj *val, size_t sample_size, int dbid);
robj *moduleTypeDupOrReply(client *c, robj *fromkey, robj *tokey, int todb, robj *value);
int moduleDefragValue(robj *key, robj *obj, int dbid);
int moduleLateDefrag(robj *key, robj *value, unsigned long *cursor, long long endtime, int dbid);
void moduleDefragGlobals(void);
void *moduleGetHandleByName(char *modulename);
int moduleIsModuleCommand(void *module_handle, struct redisCommand *cmd);

/* Utils */
long long ustime(void);
mstime_t mstime(void);
mstime_t commandTimeSnapshot(void);
void getRandomHexChars(char *p, size_t len);
void getRandomBytes(unsigned char *p, size_t len);
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);
void exitFromChild(int retcode);
long long redisPopcount(void *s, long count);
int redisSetProcTitle(char *title);
int validateProcTitleTemplate(const char *template);
int redisCommunicateSystemd(const char *sd_notify_msg);
void redisSetCpuAffinity(const char *cpulist);

/* afterErrorReply flags */
#define ERR_REPLY_FLAG_NO_STATS_UPDATE (1ULL<<0) /* Indicating that we should not update
                                                    error stats after sending error reply */
/* networking.c -- Networking and Client related operations */
client *createClient(connection *conn);
void freeClient(client *c);
void freeClientAsync(client *c);
void deauthenticateAndCloseClient(client *c);
void logInvalidUseAndFreeClientAsync(client *c, const char *fmt, ...);
int beforeNextClient(client *c);
void clearClientConnectionState(client *c);
void resetClient(client *c);
void freeClientOriginalArgv(client *c);
void freeClientArgv(client *c);
void sendReplyToClient(connection *conn);
void *addReplyDeferredLen(client *c);
void setDeferredArrayLen(client *c, void *node, long length);
void setDeferredMapLen(client *c, void *node, long length);
void setDeferredSetLen(client *c, void *node, long length);
void setDeferredAttributeLen(client *c, void *node, long length);
void setDeferredPushLen(client *c, void *node, long length);
int processInputBuffer(client *c);
void acceptCommonHandler(connection *conn, int flags, char *ip);
void readQueryFromClient(connection *conn);
int prepareClientToWrite(client *c);
void addReplyNull(client *c);
void addReplyNullArray(client *c);
void addReplyBool(client *c, int b);
void addReplyVerbatim(client *c, const char *s, size_t len, const char *ext);
void addReplyProto(client *c, const char *s, size_t len);
void AddReplyFromClient(client *c, client *src);
void addReplyBulk(client *c, robj *obj);
void addReplyBulkCString(client *c, const char *s);
void addReplyBulkCBuffer(client *c, const void *p, size_t len);
void addReplyBulkLongLong(client *c, long long ll);
void addReply(client *c, robj *obj);
void addReplyStatusLength(client *c, const char *s, size_t len);
void addReplySds(client *c, sds s);
void addReplyBulkSds(client *c, sds s);
void setDeferredReplyBulkSds(client *c, void *node, sds s);
void addReplyErrorObject(client *c, robj *err);
void addReplyOrErrorObject(client *c, robj *reply);
void afterErrorReply(client *c, const char *s, size_t len, int flags);
void addReplyErrorFormatInternal(client *c, int flags, const char *fmt, va_list ap);
void addReplyErrorSdsEx(client *c, sds err, int flags);
void addReplyErrorSds(client *c, sds err);
void addReplyErrorSdsSafe(client *c, sds err);
void addReplyError(client *c, const char *err);
void addReplyErrorArity(client *c);
void addReplyErrorExpireTime(client *c);
void addReplyStatus(client *c, const char *status);
void addReplyDouble(client *c, double d);
void addReplyBigNum(client *c, const char *num, size_t len);
void addReplyHumanLongDouble(client *c, long double d);
void addReplyLongLong(client *c, long long ll);
void addReplyArrayLen(client *c, long length);
void addReplyMapLen(client *c, long length);
void addReplySetLen(client *c, long length);
void addReplyAttributeLen(client *c, long length);
void addReplyPushLen(client *c, long length);
void addReplyHelp(client *c, const char **help);
void addExtendedReplyHelp(client *c, const char **help, const char **extended_help);
void addReplySubcommandSyntaxError(client *c);
void addReplyLoadedModules(client *c);
void copyReplicaOutputBuffer(client *dst, client *src);
void addListRangeReply(client *c, robj *o, long start, long end, int reverse);
void deferredAfterErrorReply(client *c, list *errors);
size_t sdsZmallocSize(sds s);
size_t hfieldZmallocSize(hfield s);
size_t getStringObjectSdsUsedMemory(robj *o);
void freeClientReplyValue(void *o);
void *dupClientReplyValue(void *o);
char *getClientPeerId(client *client);
char *getClientSockName(client *client);
sds catClientInfoString(sds s, client *client);
sds getAllClientsInfoString(int type);
int clientSetName(client *c, robj *name, const char **err);
void rewriteClientCommandVector(client *c, int argc, ...);
void rewriteClientCommandArgument(client *c, int i, robj *newval);
void replaceClientCommandVector(client *c, int argc, robj **argv);
void redactClientCommandArgument(client *c, int argc);
size_t getClientOutputBufferMemoryUsage(client *c);
size_t getClientMemoryUsage(client *c, size_t *output_buffer_mem_usage);
int freeClientsInAsyncFreeQueue(void);
int closeClientOnOutputBufferLimitReached(client *c, int async);
int getClientType(client *c);
int getClientTypeByName(char *name);
char *getClientTypeName(int class);
void flushSlavesOutputBuffers(void);
void disconnectSlaves(void);
void evictClients(void);
int listenToPort(connListener *fds);
void pauseActions(pause_purpose purpose, mstime_t end, uint32_t actions_bitmask);
void unpauseActions(pause_purpose purpose);
uint32_t isPausedActions(uint32_t action_bitmask);
uint32_t isPausedActionsWithUpdate(uint32_t action_bitmask);
void updatePausedActions(void);
void unblockPostponedClients(void);
void processEventsWhileBlocked(void);
void whileBlockedCron(void);
void blockingOperationStarts(void);
void blockingOperationEnds(void);
int handleClientsWithPendingWrites(void);
int handleClientsWithPendingWritesUsingThreads(void);
int handleClientsWithPendingReadsUsingThreads(void);
int stopThreadedIOIfNeeded(void);
int clientHasPendingReplies(client *c);
int updateClientMemUsageAndBucket(client *c);
void removeClientFromMemUsageBucket(client *c, int allow_eviction);
void unlinkClient(client *c);
int writeToClient(client *c, int handler_installed);
void linkClient(client *c);
void protectClient(client *c);
void unprotectClient(client *c);
void initThreadedIO(void);
client *lookupClientByID(uint64_t id);
int authRequired(client *c);
void putClientInPendingWriteQueue(client *c);

/* logreqres.c - logging of requests and responses */
void reqresReset(client *c, int free_buf);
void reqresSaveClientReplyOffset(client *c);
size_t reqresAppendRequest(client *c);
size_t reqresAppendResponse(client *c);

#ifdef __GNUC__
void addReplyErrorFormatEx(client *c, int flags, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));
void addReplyErrorFormat(client *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
void addReplyStatusFormat(client *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void addReplyErrorFormatEx(client *c, int flags, const char *fmt, ...);
void addReplyErrorFormat(client *c, const char *fmt, ...);
void addReplyStatusFormat(client *c, const char *fmt, ...);
#endif

/* Client side caching (tracking mode) */
void enableTracking(client *c, uint64_t redirect_to, uint64_t options, robj **prefix, size_t numprefix);
void disableTracking(client *c);
void trackingRememberKeys(client *tracking, client *executing);
void trackingInvalidateKey(client *c, robj *keyobj, int bcast);
void trackingScheduleKeyInvalidation(uint64_t client_id, robj *keyobj);
void trackingHandlePendingKeyInvalidations(void);
void trackingInvalidateKeysOnFlush(int async);
void freeTrackingRadixTree(rax *rt);
void freeTrackingRadixTreeAsync(rax *rt);
void freeErrorsRadixTreeAsync(rax *errors);
void trackingLimitUsedSlots(void);
uint64_t trackingGetTotalItems(void);
uint64_t trackingGetTotalKeys(void);
uint64_t trackingGetTotalPrefixes(void);
void trackingBroadcastInvalidationMessages(void);
int checkPrefixCollisionsOrReply(client *c, robj **prefix, size_t numprefix);

/* List data type */
void listTypePush(robj *subject, robj *value, int where);
robj *listTypePop(robj *subject, int where);
unsigned long listTypeLength(const robj *subject);
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction);
void listTypeReleaseIterator(listTypeIterator *li);
void listTypeSetIteratorDirection(listTypeIterator *li, listTypeEntry *entry, unsigned char direction);
int listTypeNext(listTypeIterator *li, listTypeEntry *entry);
robj *listTypeGet(listTypeEntry *entry);
unsigned char *listTypeGetValue(listTypeEntry *entry, size_t *vlen, long long *lval);
void listTypeInsert(listTypeEntry *entry, robj *value, int where);
void listTypeReplace(listTypeEntry *entry, robj *value);
int listTypeEqual(listTypeEntry *entry, robj *o);
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry);
robj *listTypeDup(robj *o);
void listTypeDelRange(robj *o, long start, long stop);
void popGenericCommand(client *c, int where);
void listElementsRemoved(client *c, robj *key, int where, robj *o, long count, int signal, int *deleted);
typedef enum {
    LIST_CONV_AUTO,
    LIST_CONV_GROWING,
    LIST_CONV_SHRINKING,
} list_conv_type;
typedef void (*beforeConvertCB)(void *data);
void listTypeTryConversion(robj *o, list_conv_type lct, beforeConvertCB fn, void *data);
void listTypeTryConversionAppend(robj *o, robj **argv, int start, int end, beforeConvertCB fn, void *data);

/* MULTI/EXEC/WATCH... */
void unwatchAllKeys(client *c);
void initClientMultiState(client *c);
void freeClientMultiState(client *c);
void queueMultiCommand(client *c, uint64_t cmd_flags);
size_t multiStateMemOverhead(client *c);
void touchWatchedKey(redisDb *db, robj *key);
int isWatchedKeyExpired(client *c);
void touchAllWatchedKeysInDb(redisDb *emptied, redisDb *replaced_with);
void discardTransaction(client *c);
void flagTransaction(client *c);
void execCommandAbort(client *c, sds error);

/* Redis object implementation */
void decrRefCount(robj *o);
void decrRefCountVoid(void *o);
void incrRefCount(robj *o);
robj *makeObjectShared(robj *o);
void freeStringObject(robj *o);
void freeListObject(robj *o);
void freeSetObject(robj *o);
void freeZsetObject(robj *o);
void freeHashObject(robj *o);
void dismissObject(robj *o, size_t dump_size);
robj *createObject(int type, void *ptr);
void initObjectLRUOrLFU(robj *o);
robj *createStringObject(const char *ptr, size_t len);
robj *createRawStringObject(const char *ptr, size_t len);
robj *createEmbeddedStringObject(const char *ptr, size_t len);
robj *tryCreateRawStringObject(const char *ptr, size_t len);
robj *tryCreateStringObject(const char *ptr, size_t len);
robj *dupStringObject(const robj *o);
int isSdsRepresentableAsLongLong(sds s, long long *llval);
int isObjectRepresentableAsLongLong(robj *o, long long *llongval);
robj *tryObjectEncoding(robj *o);
robj *tryObjectEncodingEx(robj *o, int try_trim);
robj *getDecodedObject(robj *o);
size_t stringObjectLen(robj *o);
robj *createStringObjectFromLongLong(long long value);
robj *createStringObjectFromLongLongForValue(long long value);
robj *createStringObjectFromLongLongWithSds(long long value);
robj *createStringObjectFromLongDouble(long double value, int humanfriendly);
robj *createQuicklistObject(int fill, int compress);
robj *createListListpackObject(void);
robj *createSetObject(void);
robj *createIntsetObject(void);
robj *createSetListpackObject(void);
robj *createHashObject(void);
robj *createZsetObject(void);
robj *createZsetListpackObject(void);
robj *createStreamObject(void);
robj *createModuleObject(moduleType *mt, void *value);
int getLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg);
int getPositiveLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg);
int getRangeLongFromObjectOrReply(client *c, robj *o, long min, long max, long *target, const char *msg);
int checkType(client *c, robj *o, int type);
int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg);
int getDoubleFromObjectOrReply(client *c, robj *o, double *target, const char *msg);
int getDoubleFromObject(const robj *o, double *target);
int getLongLongFromObject(robj *o, long long *target);
int getLongDoubleFromObject(robj *o, long double *target);
int getLongDoubleFromObjectOrReply(client *c, robj *o, long double *target, const char *msg);
int getIntFromObjectOrReply(client *c, robj *o, int *target, const char *msg);
char *strEncoding(int encoding);
int compareStringObjects(const robj *a, const robj *b);
int collateStringObjects(const robj *a, const robj *b);
int equalStringObjects(robj *a, robj *b);
unsigned long long estimateObjectIdleTime(robj *o);
void trimStringObjectIfNeeded(robj *o, int trim_small_values);
#define sdsEncodedObject(objptr) (objptr->encoding == OBJ_ENCODING_RAW || objptr->encoding == OBJ_ENCODING_EMBSTR)

/* Synchronous I/O with timeout */
ssize_t syncWrite(int fd, char *ptr, ssize_t size, long long timeout);
ssize_t syncRead(int fd, char *ptr, ssize_t size, long long timeout);
ssize_t syncReadLine(int fd, char *ptr, ssize_t size, long long timeout);

/* Replication */
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc);
void replicationFeedStreamFromMasterStream(char *buf, size_t buflen);
void resetReplicationBuffer(void);
void feedReplicationBuffer(char *buf, size_t len);
void freeReplicaReferencedReplBuffer(client *replica);
void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc);
void updateSlavesWaitingBgsave(int bgsaveerr, int type);
void replicationCron(void);
void replicationStartPendingFork(void);
void replicationHandleMasterDisconnection(void);
void replicationCacheMaster(client *c);
void resizeReplicationBacklog(void);
void replicationSetMaster(char *ip, int port);
void replicationUnsetMaster(void);
void refreshGoodSlavesCount(void);
int checkGoodReplicasStatus(void);
void processClientsWaitingReplicas(void);
void unblockClientWaitingReplicas(client *c);
int replicationCountAcksByOffset(long long offset);
int replicationCountAOFAcksByOffset(long long offset);
void replicationSendNewlineToMaster(void);
long long replicationGetSlaveOffset(void);
char *replicationGetSlaveName(client *c);
long long getPsyncInitialOffset(void);
int replicationSetupSlaveForFullResync(client *slave, long long offset);
void changeReplicationId(void);
void clearReplicationId2(void);
void createReplicationBacklog(void);
void freeReplicationBacklog(void);
void replicationCacheMasterUsingMyself(void);
void feedReplicationBacklog(void *ptr, size_t len);
void incrementalTrimReplicationBacklog(size_t blocks);
int canFeedReplicaReplBuffer(client *replica);
void rebaseReplicationBuffer(long long base_repl_offset);
void showLatestBacklog(void);
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
void rdbPipeWriteHandlerConnRemoved(struct connection *conn);
void clearFailoverState(void);
void updateFailoverStatus(void);
void abortFailover(const char *err);
const char *getFailoverStateString(void);

/* Generic persistence functions */
void startLoadingFile(size_t size, char* filename, int rdbflags);
void startLoading(size_t size, int rdbflags, int async);
void loadingAbsProgress(off_t pos);
void loadingIncrProgress(off_t size);
void stopLoading(int success);
void updateLoadingFileName(char* filename);
void startSaving(int rdbflags);
void stopSaving(int success);
int allPersistenceDisabled(void);

#define DISK_ERROR_TYPE_AOF 1       /* Don't accept writes: AOF errors. */
#define DISK_ERROR_TYPE_RDB 2       /* Don't accept writes: RDB errors. */
#define DISK_ERROR_TYPE_NONE 0      /* No problems, we can accept writes. */
int writeCommandsDeniedByDiskError(void);
sds writeCommandsGetDiskErrorMessage(int);

/* RDB persistence */
#include "rdb.h"
void killRDBChild(void);
int bg_unlink(const char *filename);

/* AOF persistence */
void flushAppendOnlyFile(int force);
void feedAppendOnlyFile(int dictid, robj **argv, int argc);
void aofRemoveTempFile(pid_t childpid);
int rewriteAppendOnlyFileBackground(void);
int loadAppendOnlyFiles(aofManifest *am);
void stopAppendOnly(void);
int startAppendOnly(void);
void backgroundRewriteDoneHandler(int exitcode, int bysignal);
void killAppendOnlyChild(void);
void restartAOFAfterSYNC(void);
void aofLoadManifestFromDisk(void);
void aofOpenIfNeededOnServerStart(void);
void aofManifestFree(aofManifest *am);
int aofDelHistoryFiles(void);
int aofRewriteLimited(void);

/* Child info */
void openChildInfoPipe(void);
void closeChildInfoPipe(void);
void sendChildInfoGeneric(childInfoType info_type, size_t keys, double progress, char *pname);
void sendChildCowInfo(childInfoType info_type, char *pname);
void sendChildInfo(childInfoType info_type, size_t keys, char *pname);
void receiveChildInfo(void);

/* Fork helpers */
int redisFork(int purpose);
int hasActiveChildProcess(void);
void resetChildState(void);
int isMutuallyExclusiveChildType(int type);

/* acl.c -- Authentication related prototypes. */
extern rax *Users;
extern user *DefaultUser;
void ACLInit(void);
/* Return values for ACLCheckAllPerm(). */
#define ACL_OK 0
#define ACL_DENIED_CMD 1
#define ACL_DENIED_KEY 2
#define ACL_DENIED_AUTH 3 /* Only used for ACL LOG entries. */
#define ACL_DENIED_CHANNEL 4 /* Only used for pub/sub commands */

/* Context values for addACLLogEntry(). */
#define ACL_LOG_CTX_TOPLEVEL 0
#define ACL_LOG_CTX_LUA 1
#define ACL_LOG_CTX_MULTI 2
#define ACL_LOG_CTX_MODULE 3

/* ACL key permission types */
#define ACL_READ_PERMISSION (1<<0)
#define ACL_WRITE_PERMISSION (1<<1)
#define ACL_ALL_PERMISSION (ACL_READ_PERMISSION|ACL_WRITE_PERMISSION)

/* Return codes for Authentication functions to indicate the result. */
typedef enum {
    AUTH_OK = 0,
    AUTH_ERR,
    AUTH_NOT_HANDLED,
    AUTH_BLOCKED
} AuthResult;

int ACLCheckUserCredentials(robj *username, robj *password);
int ACLAuthenticateUser(client *c, robj *username, robj *password, robj **err);
int checkModuleAuthentication(client *c, robj *username, robj *password, robj **err);
void addAuthErrReply(client *c, robj *err);
unsigned long ACLGetCommandID(sds cmdname);
void ACLClearCommandID(void);
user *ACLGetUserByName(const char *name, size_t namelen);
int ACLUserCheckKeyPerm(user *u, const char *key, int keylen, int flags);
int ACLUserCheckChannelPerm(user *u, sds channel, int literal);
int ACLCheckAllUserCommandPerm(user *u, struct redisCommand *cmd, robj **argv, int argc, int *idxptr);
int ACLUserCheckCmdWithUnrestrictedKeyAccess(user *u, struct redisCommand *cmd, robj **argv, int argc, int flags);
int ACLCheckAllPerm(client *c, int *idxptr);
int ACLSetUser(user *u, const char *op, ssize_t oplen);
sds ACLStringSetUser(user *u, sds username, sds *argv, int argc);
uint64_t ACLGetCommandCategoryFlagByName(const char *name);
int ACLAddCommandCategory(const char *name, uint64_t flag);
void ACLCleanupCategoriesOnFailure(size_t num_acl_categories_added);
int ACLAppendUserForLoading(sds *argv, int argc, int *argc_err);
const char *ACLSetUserStringError(void);
int ACLLoadConfiguredUsers(void);
robj *ACLDescribeUser(user *u);
void ACLLoadUsersAtStartup(void);
void addReplyCommandCategories(client *c, struct redisCommand *cmd);
user *ACLCreateUnlinkedUser(void);
void ACLFreeUserAndKillClients(user *u);
void addACLLogEntry(client *c, int reason, int context, int argpos, sds username, sds object);
sds getAclErrorMessage(int acl_res, user *user, struct redisCommand *cmd, sds errored_val, int verbose);
void ACLUpdateDefaultUserPassword(sds password);
sds genRedisInfoStringACLStats(sds info);
void ACLRecomputeCommandBitsFromCommandRulesAllUsers(void);

/* Sorted sets data type */

/* Input flags. */
#define ZADD_IN_NONE 0
#define ZADD_IN_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_IN_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_IN_XX (1<<2)      /* Only touch elements already existing. */
#define ZADD_IN_GT (1<<3)      /* Only update existing when new scores are higher. */
#define ZADD_IN_LT (1<<4)      /* Only update existing when new scores are lower. */

/* Output flags. */
#define ZADD_OUT_NOP (1<<0)     /* Operation not performed because of conditionals.*/
#define ZADD_OUT_NAN (1<<1)     /* Only touch elements already existing. */
#define ZADD_OUT_ADDED (1<<2)   /* The element was new and was added. */
#define ZADD_OUT_UPDATED (1<<3) /* The element already existed, score updated. */

/* Struct to hold an inclusive/exclusive range spec by score comparison. */
typedef struct {
    double min, max;
    int minex, maxex; /* are min or max exclusive? */
} zrangespec;

/* Struct to hold an inclusive/exclusive range spec by lexicographic comparison. */
typedef struct {
    sds min, max;     /* May be set to shared.(minstring|maxstring) */
    int minex, maxex; /* are min or max exclusive? */
} zlexrangespec;

/* flags for incrCommandFailedCalls */
#define ERROR_COMMAND_REJECTED (1<<0) /* Indicate to update the command rejected stats */
#define ERROR_COMMAND_FAILED (1<<1) /* Indicate to update the command failed stats */

zskiplist *zslCreate(void);
void zslFree(zskiplist *zsl);
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele);
unsigned char *zzlInsert(unsigned char *zl, sds ele, double score);
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node);
zskiplistNode *zslNthInRange(zskiplist *zsl, zrangespec *range, long n);
double zzlGetScore(unsigned char *sptr);
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range);
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range);
unsigned long zsetLength(const robj *zobj);
void zsetConvert(robj *zobj, int encoding);
void zsetConvertToListpackIfNeeded(robj *zobj, size_t maxelelen, size_t totelelen);
int zsetScore(robj *zobj, sds member, double *score);
unsigned long zslGetRank(zskiplist *zsl, double score, sds o);
int zsetAdd(robj *zobj, double score, sds ele, int in_flags, int *out_flags, double *newscore);
long zsetRank(robj *zobj, sds ele, int reverse, double *score);
int zsetDel(robj *zobj, sds ele);
robj *zsetDup(robj *o);
void genericZpopCommand(client *c, robj **keyv, int keyc, int where, int emitkey, long count, int use_nested_array, int reply_nil_when_empty, int *deleted);
sds lpGetObject(unsigned char *sptr);
int zslValueGteMin(double value, zrangespec *spec);
int zslValueLteMax(double value, zrangespec *spec);
void zslFreeLexRange(zlexrangespec *spec);
int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec);
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range);
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range);
zskiplistNode *zslNthInLexRange(zskiplist *zsl, zlexrangespec *range, long n);
int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec);
int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec);
int zslLexValueGteMin(sds value, zlexrangespec *spec);
int zslLexValueLteMax(sds value, zlexrangespec *spec);

/* Core functions */
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level);
size_t freeMemoryGetNotCountedMemory(void);
int overMaxmemoryAfterAlloc(size_t moremem);
uint64_t getCommandFlags(client *c);
int processCommand(client *c);
void commandProcessed(client *c);
int processPendingCommandAndInputBuffer(client *c);
int processCommandAndResetClient(client *c);
void setupSignalHandlers(void);
int createSocketAcceptHandler(connListener *sfd, aeFileProc *accept_handler);
connListener *listenerByType(const char *typename);
int changeListener(connListener *listener);
void closeListener(connListener *listener);
struct redisCommand *lookupSubcommand(struct redisCommand *container, sds sub_name);
struct redisCommand *lookupCommand(robj **argv, int argc);
struct redisCommand *lookupCommandBySdsLogic(dict *commands, sds s);
struct redisCommand *lookupCommandBySds(sds s);
struct redisCommand *lookupCommandByCStringLogic(dict *commands, const char *s);
struct redisCommand *lookupCommandByCString(const char *s);
struct redisCommand *lookupCommandOrOriginal(robj **argv, int argc);
int commandCheckExistence(client *c, sds *err);
int commandCheckArity(client *c, sds *err);
void startCommandExecution(void);
int incrCommandStatsOnError(struct redisCommand *cmd, int flags);
void call(client *c, int flags);
void alsoPropagate(int dbid, robj **argv, int argc, int target);
void postExecutionUnitOperations(void);
void redisOpArrayFree(redisOpArray *oa);
void forceCommandPropagation(client *c, int flags);
void preventCommandPropagation(client *c);
void preventCommandAOF(client *c);
void preventCommandReplication(client *c);
void slowlogPushCurrentCommand(client *c, struct redisCommand *cmd, ustime_t duration);
void updateCommandLatencyHistogram(struct hdr_histogram** latency_histogram, int64_t duration_hist);
int prepareForShutdown(int flags);
void replyToClientsBlockedOnShutdown(void);
int abortShutdown(void);
void afterCommand(client *c);
int mustObeyClient(client *c);
#ifdef __GNUC__
void _serverLog(int level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
void serverLogFromHandler(int level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void serverLogFromHandler(int level, const char *fmt, ...);
void _serverLog(int level, const char *fmt, ...);
#endif
void serverLogRaw(int level, const char *msg);
void serverLogRawFromHandler(int level, const char *msg);
void usage(void);
void updateDictResizePolicy(void);
void populateCommandTable(void);
void resetCommandTableStats(dict* commands);
void resetErrorTableStats(void);
void adjustOpenFilesLimit(void);
void incrementErrorCount(const char *fullerr, size_t namelen);
void closeListeningSockets(int unlink_unix_socket);
void updateCachedTime(int update_daylight_info);
void enterExecutionUnit(int update_cached_time, long long us);
void exitExecutionUnit(void);
void resetServerStats(void);
void activeDefragCycle(void);
unsigned int getLRUClock(void);
unsigned int LRU_CLOCK(void);
const char *evictPolicyToString(void);
struct redisMemOverhead *getMemoryOverheadData(void);
void freeMemoryOverheadData(struct redisMemOverhead *mh);
void checkChildrenDone(void);
int setOOMScoreAdj(int process_class);
void rejectCommandFormat(client *c, const char *fmt, ...);
void *activeDefragAlloc(void *ptr);
robj *activeDefragStringOb(robj* ob);
void dismissSds(sds s);
void dismissMemory(void* ptr, size_t size_hint);
void dismissMemoryInChild(void);

#define RESTART_SERVER_NONE 0
#define RESTART_SERVER_GRACEFULLY (1<<0)     /* Do proper shutdown. */
#define RESTART_SERVER_CONFIG_REWRITE (1<<1) /* CONFIG REWRITE before restart.*/
int restartServer(int flags, mstime_t delay);
int getKeySlot(sds key);
int calculateKeySlot(sds key);

/* kvstore wrappers */
int dbExpand(redisDb *db, uint64_t db_size, int try_expand);
int dbExpandExpires(redisDb *db, uint64_t db_size, int try_expand);
dictEntry *dbFind(redisDb *db, void *key);
dictEntry *dbFindExpires(redisDb *db, void *key);
unsigned long long dbSize(redisDb *db);
unsigned long long dbScan(redisDb *db, unsigned long long cursor, dictScanFunction *scan_cb, void *privdata);

/* Set data type */
robj *setTypeCreate(sds value, size_t size_hint);
int setTypeAdd(robj *subject, sds value);
int setTypeAddAux(robj *set, char *str, size_t len, int64_t llval, int str_is_sds);
int setTypeRemove(robj *subject, sds value);
int setTypeRemoveAux(robj *set, char *str, size_t len, int64_t llval, int str_is_sds);
int setTypeIsMember(robj *subject, sds value);
int setTypeIsMemberAux(robj *set, char *str, size_t len, int64_t llval, int str_is_sds);
setTypeIterator *setTypeInitIterator(robj *subject);
void setTypeReleaseIterator(setTypeIterator *si);
int setTypeNext(setTypeIterator *si, char **str, size_t *len, int64_t *llele);
sds setTypeNextObject(setTypeIterator *si);
int setTypeRandomElement(robj *setobj, char **str, size_t *len, int64_t *llele);
unsigned long setTypeSize(const robj *subject);
void setTypeConvert(robj *subject, int enc);
int setTypeConvertAndExpand(robj *setobj, int enc, unsigned long cap, int panic);
robj *setTypeDup(robj *o);

/* Data structure for OBJ_ENCODING_LISTPACK_EX for hash. It contains listpack
 * and metadata fields for hash field expiration.*/
typedef struct listpackEx {
    ExpireMeta meta;  /* To be used in order to register the hash in the
                         global ebuckets (i.e. db->hexpires) with next,
                         minimum, hash-field to expire. TTL value might be
                         inaccurate up-to few seconds due to optimization
                         consideration.  */
    sds key;          /* reference to the key, same one that stored in
                         db->dict. Will be used from active-expiration flow
                         for notification and deletion of the object, if
                         needed. */
    void *lp;         /* listpack that contains 'key-value-ttl' tuples which
                         are ordered by ttl. */
} listpackEx;

/* Each dict of hash object that has fields with time-Expiration will have the
 * following metadata attached to dict header */
typedef struct dictExpireMetadata {
    ExpireMeta expireMeta;   /* embedded ExpireMeta in dict.
                                To be used in order to register the hash in the
                                global ebuckets (i.e db->hexpires) with next,
                                minimum, hash-field to expire. TTL value might be
                                inaccurate up-to few seconds due to optimization
                                consideration. */
    ebuckets hfe;            /* DS of Hash Fields Expiration, associated to each hash */
    sds key;                 /* reference to the key, same one that stored in
                               db->dict. Will be used from active-expiration flow
                               for notification and deletion of the object, if
                               needed. */
} dictExpireMetadata;

/* Hash data type */
#define HASH_SET_TAKE_FIELD (1<<0)
#define HASH_SET_TAKE_VALUE (1<<1)
#define HASH_SET_COPY 0

/* Hash field lazy expiration flags. Used by core hashTypeGetValue() and its callers */
#define HFE_LAZY_EXPIRE           (0) /* Delete expired field, and if last field also the hash */
#define HFE_LAZY_AVOID_FIELD_DEL  (1<<0) /* Avoid deleting expired field */
#define HFE_LAZY_AVOID_HASH_DEL   (1<<1) /* Avoid deleting hash if the field is the last one */
#define HFE_LAZY_NO_NOTIFICATION  (1<<2) /* Do not send notification, used when multiple fields
                                          * may expire and only one notification is desired. */

void hashTypeConvert(robj *o, int enc, ebuckets *hexpires);
void hashTypeTryConversion(redisDb *db, robj *subject, robj **argv, int start, int end);
int hashTypeExists(redisDb *db, robj *o, sds key, int hfeFlags, int *isHashDeleted);
int hashTypeDelete(robj *o, void *key, int isSdsField);
unsigned long hashTypeLength(const robj *o, int subtractExpiredFields);
hashTypeIterator *hashTypeInitIterator(robj *subject);
void hashTypeReleaseIterator(hashTypeIterator *hi);
int hashTypeNext(hashTypeIterator *hi, int skipExpiredFields);
void hashTypeCurrentFromListpack(hashTypeIterator *hi, int what,
                                 unsigned char **vstr,
                                 unsigned int *vlen,
                                 long long *vll,
                                 uint64_t *expireTime);
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, char **str,
                                  size_t *len, uint64_t *expireTime);
void hashTypeCurrentObject(hashTypeIterator *hi, int what, unsigned char **vstr,
                           unsigned int *vlen, long long *vll, uint64_t *expireTime);
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what);
hfield hashTypeCurrentObjectNewHfield(hashTypeIterator *hi);
robj *hashTypeGetValueObject(redisDb *db, robj *o, sds field, int hfeFlags, int *isHashDeleted);
int hashTypeSet(redisDb *db, robj *o, sds field, sds value, int flags);
robj *hashTypeDup(robj *o, sds newkey, uint64_t *minHashExpire);
uint64_t hashTypeRemoveFromExpires(ebuckets *hexpires, robj *o);
void hashTypeAddToExpires(redisDb *db, sds key, robj *hashObj, uint64_t expireTime);
void hashTypeFree(robj *o);
int hashTypeIsExpired(const robj *o, uint64_t expireAt);
unsigned char *hashTypeListpackGetLp(robj *o);
uint64_t hashTypeGetMinExpire(robj *o, int accurate);
void hashTypeUpdateKeyRef(robj *o, sds newkey);
ebuckets *hashTypeGetDictMetaHFE(dict *d);
void initDictExpireMetadata(sds key, robj *o);
struct listpackEx *listpackExCreate(void);
void listpackExAddNew(robj *o, char *field, size_t flen,
                      char *value, size_t vlen, uint64_t expireAt);

/* Hash-Field data type (of t_hash.c) */
hfield hfieldNew(const void *field, size_t fieldlen, int withExpireMeta);
hfield hfieldTryNew(const void *field, size_t fieldlen, int withExpireMeta);
int hfieldIsExpireAttached(hfield field);
int hfieldIsExpired(hfield field);
uint64_t hfieldGetExpireTime(hfield field);
static inline void hfieldFree(hfield field) { mstrFree(&mstrFieldKind, field); }
static inline void *hfieldGetAllocPtr(hfield field) { return mstrGetAllocPtr(&mstrFieldKind, field); }
static inline size_t hfieldlen(hfield field) { return mstrlen(field);}
uint64_t hfieldGetExpireTime(hfield field);

/* Pub / Sub */
int pubsubUnsubscribeAllChannels(client *c, int notify);
int pubsubUnsubscribeShardAllChannels(client *c, int notify);
void pubsubShardUnsubscribeAllChannelsInSlot(unsigned int slot);
int pubsubUnsubscribeAllPatterns(client *c, int notify);
int pubsubPublishMessage(robj *channel, robj *message, int sharded);
int pubsubPublishMessageAndPropagateToCluster(robj *channel, robj *message, int sharded);
void addReplyPubsubMessage(client *c, robj *channel, robj *msg, robj *message_bulk);
int serverPubsubSubscriptionCount(void);
int serverPubsubShardSubscriptionCount(void);
size_t pubsubMemOverhead(client *c);
void unmarkClientAsPubSub(client *c);
int pubsubTotalSubscriptions(void);
dict *getClientPubSubChannels(client *c);
dict *getClientPubSubShardChannels(client *c);

/* Keyspace events notification */
void notifyKeyspaceEvent(int type, const char *event, robj *key, int dbid);
int keyspaceEventsStringToFlags(char *classes);
sds keyspaceEventsFlagsToString(int flags);

/* Configuration */
/* Configuration Flags */
#define MODIFIABLE_CONFIG 0 /* This is the implied default for a standard 
                             * config, which is mutable. */
#define IMMUTABLE_CONFIG (1ULL<<0) /* Can this value only be set at startup? */
#define SENSITIVE_CONFIG (1ULL<<1) /* Does this value contain sensitive information */
#define DEBUG_CONFIG (1ULL<<2) /* Values that are useful for debugging. */
#define MULTI_ARG_CONFIG (1ULL<<3) /* This config receives multiple arguments. */
#define HIDDEN_CONFIG (1ULL<<4) /* This config is hidden in `config get <pattern>` (used for tests/debugging) */
#define PROTECTED_CONFIG (1ULL<<5) /* Becomes immutable if enable-protected-configs is enabled. */
#define DENY_LOADING_CONFIG (1ULL<<6) /* This config is forbidden during loading. */
#define ALIAS_CONFIG (1ULL<<7) /* For configs with multiple names, this flag is set on the alias. */
#define MODULE_CONFIG (1ULL<<8) /* This config is a module config */
#define VOLATILE_CONFIG (1ULL<<9) /* The config is a reference to the config data and not the config data itself (ex.
                                   * a file name containing more configuration like a tls key). In this case we want
                                   * to apply the configuration change even if the new config value is the same as
                                   * the old. */

#define INTEGER_CONFIG 0 /* No flags means a simple integer configuration */
#define MEMORY_CONFIG (1<<0) /* Indicates if this value can be loaded as a memory value */
#define PERCENT_CONFIG (1<<1) /* Indicates if this value can be loaded as a percent (and stored as a negative int) */
#define OCTAL_CONFIG (1<<2) /* This value uses octal representation */

/* Enum Configs contain an array of configEnum objects that match a string with an integer. */
typedef struct configEnum {
    char *name;
    int val;
} configEnum;

/* Type of configuration. */
typedef enum {
    BOOL_CONFIG,
    NUMERIC_CONFIG,
    STRING_CONFIG,
    SDS_CONFIG,
    ENUM_CONFIG,
    SPECIAL_CONFIG,
} configType;

void loadServerConfig(char *filename, char config_from_stdin, char *options);
void appendServerSaveParams(time_t seconds, int changes);
void resetServerSaveParams(void);
struct rewriteConfigState; /* Forward declaration to export API. */
int rewriteConfigRewriteLine(struct rewriteConfigState *state, const char *option, sds line, int force);
void rewriteConfigMarkAsProcessed(struct rewriteConfigState *state, const char *option);
int rewriteConfig(char *path, int force_write);
void initConfigValues(void);
void removeConfig(sds name);
sds getConfigDebugInfo(void);
int allowProtectedAction(int config, client *c);
void initServerClientMemUsageBuckets(void);
void freeServerClientMemUsageBuckets(void);

/* Module Configuration */
typedef struct ModuleConfig ModuleConfig;
int performModuleConfigSetFromName(sds name, sds value, const char **err);
int performModuleConfigSetDefaultFromName(sds name, const char **err);
void addModuleBoolConfig(const char *module_name, const char *name, int flags, void *privdata, int default_val);
void addModuleStringConfig(const char *module_name, const char *name, int flags, void *privdata, sds default_val);
void addModuleEnumConfig(const char *module_name, const char *name, int flags, void *privdata, int default_val, configEnum *enum_vals);
void addModuleNumericConfig(const char *module_name, const char *name, int flags, void *privdata, long long default_val, int conf_flags, long long lower, long long upper);
void addModuleConfigApply(list *module_configs, ModuleConfig *module_config);
int moduleConfigApplyConfig(list *module_configs, const char **err, const char **err_arg_name);
int getModuleBoolConfig(ModuleConfig *module_config);
int setModuleBoolConfig(ModuleConfig *config, int val, const char **err);
sds getModuleStringConfig(ModuleConfig *module_config);
int setModuleStringConfig(ModuleConfig *config, sds strval, const char **err);
int getModuleEnumConfig(ModuleConfig *module_config);
int setModuleEnumConfig(ModuleConfig *config, int val, const char **err);
long long getModuleNumericConfig(ModuleConfig *module_config);
int setModuleNumericConfig(ModuleConfig *config, long long val, const char **err);

/* db.c -- Keyspace access API */
int removeExpire(redisDb *db, robj *key);
void deleteExpiredKeyAndPropagate(redisDb *db, robj *keyobj);
void propagateDeletion(redisDb *db, robj *key, int lazy);
int keyIsExpired(redisDb *db, robj *key);
long long getExpire(redisDb *db, robj *key);
void setExpire(client *c, redisDb *db, robj *key, long long when);
int checkAlreadyExpired(long long when);
int parseExtendedExpireArgumentsOrReply(client *c, int *flags);
robj *lookupKeyRead(redisDb *db, robj *key);
robj *lookupKeyWrite(redisDb *db, robj *key);
robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply);
robj *lookupKeyWriteOrReply(client *c, robj *key, robj *reply);
robj *lookupKeyReadWithFlags(redisDb *db, robj *key, int flags);
robj *lookupKeyWriteWithFlags(redisDb *db, robj *key, int flags);
robj *objectCommandLookup(client *c, robj *key);
robj *objectCommandLookupOrReply(client *c, robj *key, robj *reply);
int objectSetLRUOrLFU(robj *val, long long lfu_freq, long long lru_idle,
                       long long lru_clock, int lru_multiplier);
#define LOOKUP_NONE 0
#define LOOKUP_NOTOUCH (1<<0)  /* Don't update LRU. */
#define LOOKUP_NONOTIFY (1<<1) /* Don't trigger keyspace event on key misses. */
#define LOOKUP_NOSTATS (1<<2)  /* Don't update keyspace hits/misses counters. */
#define LOOKUP_WRITE (1<<3)    /* Delete expired keys even in replicas. */
#define LOOKUP_NOEXPIRE (1<<4) /* Avoid deleting lazy expired keys. */
#define LOOKUP_NOEFFECTS (LOOKUP_NONOTIFY | LOOKUP_NOSTATS | LOOKUP_NOTOUCH | LOOKUP_NOEXPIRE) /* Avoid any effects from fetching the key */

dictEntry *dbAdd(redisDb *db, robj *key, robj *val);
int dbAddRDBLoad(redisDb *db, sds key, robj *val);
void dbReplaceValue(redisDb *db, robj *key, robj *val);

#define SETKEY_KEEPTTL 1
#define SETKEY_NO_SIGNAL 2
#define SETKEY_ALREADY_EXIST 4
#define SETKEY_DOESNT_EXIST 8
#define SETKEY_ADD_OR_UPDATE 16 /* Key most likely doesn't exists */
void setKey(client *c, redisDb *db, robj *key, robj *val, int flags);
robj *dbRandomKey(redisDb *db);
int dbGenericDelete(redisDb *db, robj *key, int async, int flags);
int dbSyncDelete(redisDb *db, robj *key);
int dbDelete(redisDb *db, robj *key);
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o);

#define EMPTYDB_NO_FLAGS 0      /* No flags. */
#define EMPTYDB_ASYNC (1<<0)    /* Reclaim memory in another thread. */
#define EMPTYDB_NOFUNCTIONS (1<<1) /* Indicate not to flush the functions. */
long long emptyData(int dbnum, int flags, void(callback)(dict*));
long long emptyDbStructure(redisDb *dbarray, int dbnum, int async, void(callback)(dict*));
void flushAllDataAndResetRDB(int flags);
long long dbTotalServerKeyCount(void);
redisDb *initTempDb(void);
void discardTempDb(redisDb *tempDb, void(callback)(dict*));


int selectDb(client *c, int id);
void signalModifiedKey(client *c, redisDb *db, robj *key);
void signalFlushedDb(int dbid, int async);
void scanGenericCommand(client *c, robj *o, unsigned long long cursor);
int parseScanCursorOrReply(client *c, robj *o, unsigned long long *cursor);
int dbAsyncDelete(redisDb *db, robj *key);
void emptyDbAsync(redisDb *db);
size_t lazyfreeGetPendingObjectsCount(void);
size_t lazyfreeGetFreedObjectsCount(void);
void lazyfreeResetStats(void);
void freeObjAsync(robj *key, robj *obj, int dbid);
void freeReplicationBacklogRefMemAsync(list *blocks, rax *index);

/* API to get key arguments from commands */
#define GET_KEYSPEC_DEFAULT 0
#define GET_KEYSPEC_INCLUDE_NOT_KEYS (1<<0) /* Consider 'fake' keys as keys */
#define GET_KEYSPEC_RETURN_PARTIAL (1<<1) /* Return all keys that can be found */

int getKeysFromCommandWithSpecs(struct redisCommand *cmd, robj **argv, int argc, int search_flags, getKeysResult *result);
keyReference *getKeysPrepareResult(getKeysResult *result, int numkeys);
int getKeysFromCommand(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int doesCommandHaveKeys(struct redisCommand *cmd);
int getChannelsFromCommand(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int doesCommandHaveChannelsWithFlags(struct redisCommand *cmd, int flags);
void getKeysFreeResult(getKeysResult *result);
int sintercardGetKeys(struct redisCommand *cmd,robj **argv, int argc, getKeysResult *result);
int zunionInterDiffGetKeys(struct redisCommand *cmd,robj **argv, int argc, getKeysResult *result);
int zunionInterDiffStoreGetKeys(struct redisCommand *cmd,robj **argv, int argc, getKeysResult *result);
int evalGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int functionGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int sortGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int sortROGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int migrateGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int georadiusGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int xreadGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int lmpopGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int blmpopGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int zmpopGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int bzmpopGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int setGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int bitfieldGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);

unsigned short crc16(const char *buf, int len);

/* Sentinel */
void initSentinelConfig(void);
void initSentinel(void);
void sentinelTimer(void);
const char *sentinelHandleConfiguration(char **argv, int argc);
void queueSentinelConfig(sds *argv, int argc, int linenum, sds line);
void loadSentinelConfigFromQueue(void);
void sentinelIsRunning(void);
void sentinelCheckConfigFile(void);
void sentinelCommand(client *c);
void sentinelInfoCommand(client *c);
void sentinelPublishCommand(client *c);
void sentinelRoleCommand(client *c);

/* redis-check-rdb & aof */
int redis_check_rdb(char *rdbfilename, FILE *fp);
int redis_check_rdb_main(int argc, char **argv, FILE *fp);
int redis_check_aof_main(int argc, char **argv);

/* Scripting */
void scriptingInit(int setup);
int ldbRemoveChild(pid_t pid);
void ldbKillForkedSessions(void);
int ldbPendingChildren(void);
void luaLdbLineHook(lua_State *lua, lua_Debug *ar);
void freeLuaScriptsSync(dict *lua_scripts, list *lua_scripts_lru_list, lua_State *lua);
void freeLuaScriptsAsync(dict *lua_scripts, list *lua_scripts_lru_list, lua_State *lua);
void freeFunctionsAsync(functionsLibCtx *functions_lib_ctx, dict *engines);
int ldbIsEnabled(void);
void ldbLog(sds entry);
void ldbLogRedisReply(char *reply);
void sha1hex(char *digest, char *script, size_t len);
unsigned long evalMemory(void);
dict* evalScriptsDict(void);
unsigned long evalScriptsMemory(void);
uint64_t evalGetCommandFlags(client *c, uint64_t orig_flags);
uint64_t fcallGetCommandFlags(client *c, uint64_t orig_flags);
int isInsideYieldingLongCommand(void);

typedef struct luaScript {
    uint64_t flags;
    robj *body;
    listNode *node;  /* list node in lua_scripts_lru_list list. */
} luaScript;
/* Cache of recently used small arguments to avoid malloc calls. */
#define LUA_CMD_OBJCACHE_SIZE 32
#define LUA_CMD_OBJCACHE_MAX_LEN 64

/* Blocked clients API */
void processUnblockedClients(void);
void initClientBlockingState(client *c);
void blockClient(client *c, int btype);
void unblockClient(client *c, int queue_for_reprocessing);
void unblockClientOnTimeout(client *c);
void unblockClientOnError(client *c, const char *err_str);
void queueClientForReprocessing(client *c);
void replyToBlockedClientTimedOut(client *c);
int getTimeoutFromObjectOrReply(client *c, robj *object, mstime_t *timeout, int unit);
void disconnectAllBlockedClients(void);
void handleClientsBlockedOnKeys(void);
void signalKeyAsReady(redisDb *db, robj *key, int type);
void blockForKeys(client *c, int btype, robj **keys, int numkeys, mstime_t timeout, int unblock_on_nokey);
void blockClientShutdown(client *c);
void blockPostponeClient(client *c);
void blockForReplication(client *c, mstime_t timeout, long long offset, long numreplicas);
void blockForAofFsync(client *c, mstime_t timeout, long long offset, int numlocal, long numreplicas);
void signalDeletedKeyAsReady(redisDb *db, robj *key, int type);
void updateStatsOnUnblock(client *c, long blocked_us, long reply_us, int had_errors);
void scanDatabaseForDeletedKeys(redisDb *emptied, redisDb *replaced_with);
void totalNumberOfStatefulKeys(unsigned long *blocking_keys, unsigned long *bloking_keys_on_nokey, unsigned long *watched_keys);
void blockedBeforeSleep(void);

/* timeout.c -- Blocked clients timeout and connections timeout. */
void addClientToTimeoutTable(client *c);
void removeClientFromTimeoutTable(client *c);
void handleBlockedClientsTimeout(void);
int clientsCronHandleTimeout(client *c, mstime_t now_ms);

/* expire.c -- Handling of expired keys */
void activeExpireCycle(int type);
void expireSlaveKeys(void);
void rememberSlaveKeyWithExpire(redisDb *db, robj *key);
void flushSlaveKeysWithExpireList(void);
size_t getSlaveKeyWithExpireCount(void);
uint64_t hashTypeDbActiveExpire(redisDb *db, uint32_t maxFieldsToExpire);

/* evict.c -- maxmemory handling and LRU eviction. */
void evictionPoolAlloc(void);
#define LFU_INIT_VAL 5
unsigned long LFUGetTimeInMinutes(void);
uint8_t LFULogIncr(uint8_t value);
unsigned long LFUDecrAndReturn(robj *o);
#define EVICT_OK 0
#define EVICT_RUNNING 1
#define EVICT_FAIL 2
int performEvictions(void);
void startEvictionTimeProc(void);

/* Keys hashing / comparison functions for dict.c hash tables. */
uint64_t dictSdsHash(const void *key);
uint64_t dictPtrHash(const void *key);
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCompare(dict *d, const void *key1, const void *key2);
int dictSdsMstrKeyCompare(dict *d, const void *sdsLookup, const void *mstrStored);
int dictSdsKeyCaseCompare(dict *d, const void *key1, const void *key2);
void dictSdsDestructor(dict *d, void *val);
void dictListDestructor(dict *d, void *val);
void *dictSdsDup(dict *d, const void *key);

/* Git SHA1 */
char *redisGitSHA1(void);
char *redisGitDirty(void);
uint64_t redisBuildId(void);
const char *redisBuildIdRaw(void);
char *redisBuildIdString(void);

/* Commands prototypes */
void authCommand(client *c);
void pingCommand(client *c);
void echoCommand(client *c);
void commandCommand(client *c);
void commandCountCommand(client *c);
void commandListCommand(client *c);
void commandInfoCommand(client *c);
void commandGetKeysCommand(client *c);
void commandGetKeysAndFlagsCommand(client *c);
void commandHelpCommand(client *c);
void commandDocsCommand(client *c);
void setCommand(client *c);
void setnxCommand(client *c);
void setexCommand(client *c);
void psetexCommand(client *c);
void getCommand(client *c);
void getexCommand(client *c);
void getdelCommand(client *c);
void delCommand(client *c);
void unlinkCommand(client *c);
void existsCommand(client *c);
void setbitCommand(client *c);
void getbitCommand(client *c);
void bitfieldCommand(client *c);
void bitfieldroCommand(client *c);
void setrangeCommand(client *c);
void getrangeCommand(client *c);
void incrCommand(client *c);
void decrCommand(client *c);
void incrbyCommand(client *c);
void decrbyCommand(client *c);
void incrbyfloatCommand(client *c);
void selectCommand(client *c);
void swapdbCommand(client *c);
void randomkeyCommand(client *c);
void keysCommand(client *c);
void scanCommand(client *c);
void dbsizeCommand(client *c);
void lastsaveCommand(client *c);
void saveCommand(client *c);
void bgsaveCommand(client *c);
void bgrewriteaofCommand(client *c);
void shutdownCommand(client *c);
void slowlogCommand(client *c);
void moveCommand(client *c);
void copyCommand(client *c);
void renameCommand(client *c);
void renamenxCommand(client *c);
void lpushCommand(client *c);
void rpushCommand(client *c);
void lpushxCommand(client *c);
void rpushxCommand(client *c);
void linsertCommand(client *c);
void lpopCommand(client *c);
void rpopCommand(client *c);
void lmpopCommand(client *c);
void llenCommand(client *c);
void lindexCommand(client *c);
void lrangeCommand(client *c);
void ltrimCommand(client *c);
void typeCommand(client *c);
void lsetCommand(client *c);
void saddCommand(client *c);
void sremCommand(client *c);
void smoveCommand(client *c);
void sismemberCommand(client *c);
void smismemberCommand(client *c);
void scardCommand(client *c);
void spopCommand(client *c);
void srandmemberCommand(client *c);
void sinterCommand(client *c);
void sinterCardCommand(client *c);
void sinterstoreCommand(client *c);
void sunionCommand(client *c);
void sunionstoreCommand(client *c);
void sdiffCommand(client *c);
void sdiffstoreCommand(client *c);
void sscanCommand(client *c);
void syncCommand(client *c);
void flushdbCommand(client *c);
void flushallCommand(client *c);
void sortCommand(client *c);
void sortroCommand(client *c);
void lremCommand(client *c);
void lposCommand(client *c);
void rpoplpushCommand(client *c);
void lmoveCommand(client *c);
void infoCommand(client *c);
void mgetCommand(client *c);
void monitorCommand(client *c);
void expireCommand(client *c);
void expireatCommand(client *c);
void pexpireCommand(client *c);
void pexpireatCommand(client *c);
void getsetCommand(client *c);
void ttlCommand(client *c);
void touchCommand(client *c);
void pttlCommand(client *c);
void expiretimeCommand(client *c);
void pexpiretimeCommand(client *c);
void persistCommand(client *c);
void replicaofCommand(client *c);
void roleCommand(client *c);
void debugCommand(client *c);
void msetCommand(client *c);
void msetnxCommand(client *c);
void zaddCommand(client *c);
void zincrbyCommand(client *c);
void zrangeCommand(client *c);
void zrangebyscoreCommand(client *c);
void zrevrangebyscoreCommand(client *c);
void zrangebylexCommand(client *c);
void zrevrangebylexCommand(client *c);
void zcountCommand(client *c);
void zlexcountCommand(client *c);
void zrevrangeCommand(client *c);
void zcardCommand(client *c);
void zremCommand(client *c);
void zscoreCommand(client *c);
void zmscoreCommand(client *c);
void zremrangebyscoreCommand(client *c);
void zremrangebylexCommand(client *c);
void zpopminCommand(client *c);
void zpopmaxCommand(client *c);
void zmpopCommand(client *c);
void bzpopminCommand(client *c);
void bzpopmaxCommand(client *c);
void bzmpopCommand(client *c);
void zrandmemberCommand(client *c);
void multiCommand(client *c);
void execCommand(client *c);
void discardCommand(client *c);
void blpopCommand(client *c);
void brpopCommand(client *c);
void blmpopCommand(client *c);
void brpoplpushCommand(client *c);
void blmoveCommand(client *c);
void appendCommand(client *c);
void strlenCommand(client *c);
void zrankCommand(client *c);
void zrevrankCommand(client *c);
void hsetCommand(client *c);
void hpexpireCommand(client *c);
void hexpireCommand(client *c);
void hpexpireatCommand(client *c);
void hexpireatCommand(client *c);
void httlCommand(client *c);
void hpttlCommand(client *c);
void hexpiretimeCommand(client *c);
void hpexpiretimeCommand(client *c);
void hpersistCommand(client *c);
void hsetnxCommand(client *c);
void hgetCommand(client *c);
void hmgetCommand(client *c);
void hdelCommand(client *c);
void hlenCommand(client *c);
void hstrlenCommand(client *c);
void zremrangebyrankCommand(client *c);
void zunionstoreCommand(client *c);
void zinterstoreCommand(client *c);
void zdiffstoreCommand(client *c);
void zunionCommand(client *c);
void zinterCommand(client *c);
void zinterCardCommand(client *c);
void zrangestoreCommand(client *c);
void zdiffCommand(client *c);
void zscanCommand(client *c);
void hkeysCommand(client *c);
void hvalsCommand(client *c);
void hgetallCommand(client *c);
void hexistsCommand(client *c);
void hscanCommand(client *c);
void hrandfieldCommand(client *c);
void configSetCommand(client *c);
void configGetCommand(client *c);
void configResetStatCommand(client *c);
void configRewriteCommand(client *c);
void configHelpCommand(client *c);
void hincrbyCommand(client *c);
void hincrbyfloatCommand(client *c);
void subscribeCommand(client *c);
void unsubscribeCommand(client *c);
void psubscribeCommand(client *c);
void punsubscribeCommand(client *c);
void publishCommand(client *c);
void pubsubCommand(client *c);
void spublishCommand(client *c);
void ssubscribeCommand(client *c);
void sunsubscribeCommand(client *c);
void watchCommand(client *c);
void unwatchCommand(client *c);
void clusterCommand(client *c);
void restoreCommand(client *c);
void migrateCommand(client *c);
void askingCommand(client *c);
void readonlyCommand(client *c);
void readwriteCommand(client *c);
int verifyDumpPayload(unsigned char *p, size_t len, uint16_t *rdbver_ptr);
void dumpCommand(client *c);
void objectCommand(client *c);
void memoryCommand(client *c);
void clientCommand(client *c);
void helloCommand(client *c);
void clientSetinfoCommand(client *c);
void evalCommand(client *c);
void evalRoCommand(client *c);
void evalShaCommand(client *c);
void evalShaRoCommand(client *c);
void scriptCommand(client *c);
void fcallCommand(client *c);
void fcallroCommand(client *c);
void functionLoadCommand(client *c);
void functionDeleteCommand(client *c);
void functionKillCommand(client *c);
void functionStatsCommand(client *c);
void functionListCommand(client *c);
void functionHelpCommand(client *c);
void functionFlushCommand(client *c);
void functionRestoreCommand(client *c);
void functionDumpCommand(client *c);
void timeCommand(client *c);
void bitopCommand(client *c);
void bitcountCommand(client *c);
void bitposCommand(client *c);
void replconfCommand(client *c);
void waitCommand(client *c);
void waitaofCommand(client *c);
void georadiusbymemberCommand(client *c);
void georadiusbymemberroCommand(client *c);
void georadiusCommand(client *c);
void georadiusroCommand(client *c);
void geoaddCommand(client *c);
void geohashCommand(client *c);
void geoposCommand(client *c);
void geodistCommand(client *c);
void geosearchCommand(client *c);
void geosearchstoreCommand(client *c);
void pfselftestCommand(client *c);
void pfaddCommand(client *c);
void pfcountCommand(client *c);
void pfmergeCommand(client *c);
void pfdebugCommand(client *c);
void latencyCommand(client *c);
void moduleCommand(client *c);
void securityWarningCommand(client *c);
void xaddCommand(client *c);
void xrangeCommand(client *c);
void xrevrangeCommand(client *c);
void xlenCommand(client *c);
void xreadCommand(client *c);
void xgroupCommand(client *c);
void xsetidCommand(client *c);
void xackCommand(client *c);
void xpendingCommand(client *c);
void xclaimCommand(client *c);
void xautoclaimCommand(client *c);
void xinfoCommand(client *c);
void xdelCommand(client *c);
void xtrimCommand(client *c);
void lolwutCommand(client *c);
void aclCommand(client *c);
void lcsCommand(client *c);
void quitCommand(client *c);
void resetCommand(client *c);
void failoverCommand(client *c);

#if defined(__GNUC__)
void *calloc(size_t count, size_t size) __attribute__ ((deprecated));
void free(void *ptr) __attribute__ ((deprecated));
void *malloc(size_t size) __attribute__ ((deprecated));
void *realloc(void *ptr, size_t size) __attribute__ ((deprecated));
#endif

/* Debugging stuff */
void _serverAssertWithInfo(const client *c, const robj *o, const char *estr, const char *file, int line);
void _serverAssert(const char *estr, const char *file, int line);
#ifdef __GNUC__
void _serverPanic(const char *file, int line, const char *msg, ...)
    __attribute__ ((format (printf, 3, 4)));
#else
void _serverPanic(const char *file, int line, const char *msg, ...);
#endif
void serverLogObjectDebugInfo(const robj *o);
void setupDebugSigHandlers(void);
void setupSigSegvHandler(void);
void removeSigSegvHandlers(void);
const char *getSafeInfoString(const char *s, size_t len, char **tmp);
dict *genInfoSectionDict(robj **argv, int argc, char **defaults, int *out_all, int *out_everything);
void releaseInfoSectionDict(dict *sec);
sds genRedisInfoString(dict *section_dict, int all_sections, int everything);
sds genModulesInfoString(sds info);
void applyWatchdogPeriod(void);
void watchdogScheduleSignal(int period);
void serverLogHexDump(int level, char *descr, void *value, size_t len);
int memtest_preserving_test(unsigned long *m, size_t bytes, int passes);
void mixDigest(unsigned char *digest, const void *ptr, size_t len);
void xorDigest(unsigned char *digest, const void *ptr, size_t len);
sds catSubCommandFullname(const char *parent_name, const char *sub_name);
void commandAddSubcommand(struct redisCommand *parent, struct redisCommand *subcommand, const char *declared_name);
void debugDelay(int usec);
void killIOThreads(void);
void killThreads(void);
void makeThreadKillable(void);
void swapMainDbWithTempDb(redisDb *tempDb);
sds getVersion(void);

/* Use macro for checking log level to avoid evaluating arguments in cases log
 * should be ignored due to low level. */
#define serverLog(level, ...) do {\
        if (((level)&0xff) < server.verbosity) break;\
        _serverLog(level, __VA_ARGS__);\
    } while(0)

#define redisDebug(fmt, ...) \
    printf("DEBUG %s:%d > " fmt "\n", __FILE__, __LINE__, __VA_ARGS__)
#define redisDebugMark() \
    printf("-- MARK %s:%d --\n", __FILE__, __LINE__)

int iAmMaster(void);

#define STRINGIFY_(x) #x
#define STRINGIFY(x) STRINGIFY_(x)

#endif
