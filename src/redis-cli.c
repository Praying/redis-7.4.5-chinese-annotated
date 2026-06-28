/* Redis CLI (command line interface) — Redis 命令行接口
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * 本文件实现 redis-cli 客户端命令行工具，提供了与 Redis 服务器交互、
 * 执行命令、管理集群、监控数据、查看延迟等功能。
 */

#include "fmacros.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <assert.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <termios.h>

#include <hiredis.h>             /* Redis 客户端 C 库 hiredis */
#ifdef USE_OPENSSL              /* 如果启用了 OpenSSL 则包含 TLS 相关头文件 */
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <hiredis_ssl.h>
#endif
#include <sdscompat.h> /* 引入 hiredis 的 sds 兼容头，将 sds 调用映射到 hi_ 系列 */
/* 使用 hiredis 提供的 sds.h，确保二进制中只有一套 sds 函数 */
#include <sds.h>
#include "dict.h"                /* 哈希表数据结构 */
#include "adlist.h"              /* 双向链表 */
#include "zmalloc.h"             /* 内存分配封装 */
#include "linenoise.h"           /* 简易命令行编辑库 */
#include "anet.h"                /* 网络编程封装 */
#include "ae.h"                  /* 事件循环库 */
#include "connection.h"          /* 连接抽象层 */
#include "cli_common.h"          /* CLI 公共函数 */
#include "mt19937-64.h"          /* 64 位梅森旋转随机数发生器 */
#include "cli_commands.h"        /* CLI 内置命令表 */
#include "hdr_histogram.h"       /* 高动态范围直方图，用于延迟统计 */

/* 抑制未使用参数的编译器警告 */
#define UNUSED(V) ((void) V)

/* 输出模式常量 */
#define OUTPUT_STANDARD 0        /* 标准人类可读输出 */
#define OUTPUT_RAW 1             /* 原始字节输出 */
#define OUTPUT_CSV 2             /* CSV 格式输出 */
#define OUTPUT_JSON 3            /* JSON 格式输出 */
#define OUTPUT_QUOTED_JSON 4     /* 带引号的 JSON 格式输出 */

/* Redis CLI 心跳间隔，单位为秒 */
#define REDIS_CLI_KEEPALIVE_INTERVAL 15
/* 管道模式默认超时时间，单位为秒 */
#define REDIS_CLI_DEFAULT_PIPE_TIMEOUT 30
/* 历史文件路径环境变量 */
#define REDIS_CLI_HISTFILE_ENV "REDISCLI_HISTFILE"
/* 历史文件默认路径（家目录下） */
#define REDIS_CLI_HISTFILE_DEFAULT ".rediscli_history"
/* 配置文件路径环境变量 */
#define REDIS_CLI_RCFILE_ENV "REDISCLI_RCFILE"
/* 配置文件默认路径 */
#define REDIS_CLI_RCFILE_DEFAULT ".redisclirc"
/* 认证密码环境变量 */
#define REDIS_CLI_AUTH_ENV "REDISCLI_AUTH"
/* 跳过集群交互确认的环境变量 */
#define REDIS_CLI_CLUSTER_YES_ENV "REDISCLI_CLUSTER_YES"

/* 集群管理器相关常量 */
#define CLUSTER_MANAGER_SLOTS               16384
#define CLUSTER_MANAGER_PORT_INCR           10000 /* 与 CLUSTER_PORT_INCR 相同 */
#define CLUSTER_MANAGER_MIGRATE_TIMEOUT     60000 /* 迁移超时（毫秒） */
#define CLUSTER_MANAGER_MIGRATE_PIPELINE    10    /* 迁移流水线大小 */
#define CLUSTER_MANAGER_REBALANCE_THRESHOLD 2     /* 再平衡阈值 */

/* 无效主机参数错误消息 */
#define CLUSTER_MANAGER_INVALID_HOST_ARG \
    "[ERR] Invalid arguments: you need to pass either a valid " \
    "address (ie. 120.0.0.1:7000) or space separated IP " \
    "and port (ie. 120.0.0.1 7000)\n"
/* 判断当前是否处于集群管理模式 */
#define CLUSTER_MANAGER_MODE() (config.cluster_manager_command.name != NULL)
/* 根据节点总数和副本数计算主节点数 */
#define CLUSTER_MANAGER_MASTERS_COUNT(nodes, replicas) ((nodes)/((replicas) + 1))
/* 在指定节点上执行 Redis 命令的快捷宏 */
#define CLUSTER_MANAGER_COMMAND(n,...) \
        (redisCommand((n)->context, __VA_ARGS__))

/* 释放集群节点数组分配内存 */
#define CLUSTER_MANAGER_NODE_ARRAY_FREE(array) zfree((array)->alloc)

/* 打印节点返回的错误信息 */
#define CLUSTER_MANAGER_PRINT_REPLY_ERROR(n, err) \
    clusterManagerLogErr("Node %s:%d replied with error:\n%s\n", \
                         (n)->ip, (n)->port, (err));

/* 不同日志级别的日志宏封装 */
#define clusterManagerLogInfo(...) \
    clusterManagerLog(CLUSTER_MANAGER_LOG_LVL_INFO,__VA_ARGS__)

#define clusterManagerLogErr(...) \
    clusterManagerLog(CLUSTER_MANAGER_LOG_LVL_ERR,__VA_ARGS__)

#define clusterManagerLogWarn(...) \
    clusterManagerLog(CLUSTER_MANAGER_LOG_LVL_WARN,__VA_ARGS__)

#define clusterManagerLogOk(...) \
    clusterManagerLog(CLUSTER_MANAGER_LOG_LVL_SUCCESS,__VA_ARGS__)

/* 集群节点状态标志位 */
#define CLUSTER_MANAGER_FLAG_MYSELF     1 << 0  /* 自身节点 */
#define CLUSTER_MANAGER_FLAG_SLAVE      1 << 1  /* 从节点 */
#define CLUSTER_MANAGER_FLAG_FRIEND     1 << 2  /* 已知节点 */
#define CLUSTER_MANAGER_FLAG_NOADDR     1 << 3  /* 缺少地址信息 */
#define CLUSTER_MANAGER_FLAG_DISCONNECT 1 << 4  /* 已断开连接 */
#define CLUSTER_MANAGER_FLAG_FAIL       1 << 5  /* 故障节点 */

/* 集群管理命令选项标志 */
#define CLUSTER_MANAGER_CMD_FLAG_FIX            1 << 0   /* 修复集群 */
#define CLUSTER_MANAGER_CMD_FLAG_SLAVE          1 << 1   /* 包含从节点 */
#define CLUSTER_MANAGER_CMD_FLAG_YES            1 << 2   /* 自动确认 */
#define CLUSTER_MANAGER_CMD_FLAG_AUTOWEIGHTS    1 << 3   /* 自动权重 */
#define CLUSTER_MANAGER_CMD_FLAG_EMPTYMASTER    1 << 4   /* 允许空主节点 */
#define CLUSTER_MANAGER_CMD_FLAG_SIMULATE       1 << 5   /* 模拟执行 */
#define CLUSTER_MANAGER_CMD_FLAG_REPLACE        1 << 6   /* 替换节点 */
#define CLUSTER_MANAGER_CMD_FLAG_COPY           1 << 7   /* 复制模式 */
#define CLUSTER_MANAGER_CMD_FLAG_COLOR          1 << 8   /* 彩色输出 */
#define CLUSTER_MANAGER_CMD_FLAG_CHECK_OWNERS   1 << 9   /* 检查所有权 */
#define CLUSTER_MANAGER_CMD_FLAG_FIX_WITH_UNREACHABLE_MASTERS 1 << 10
#define CLUSTER_MANAGER_CMD_FLAG_MASTERS_ONLY   1 << 11  /* 仅主节点 */
#define CLUSTER_MANAGER_CMD_FLAG_SLAVES_ONLY    1 << 12  /* 仅从节点 */

/* 集群管理操作选项 */
#define CLUSTER_MANAGER_OPT_GETFRIENDS  1 << 0  /* 获取友好节点 */
#define CLUSTER_MANAGER_OPT_COLD        1 << 1  /* 冷数据迁移 */
#define CLUSTER_MANAGER_OPT_UPDATE      1 << 2  /* 更新配置 */
#define CLUSTER_MANAGER_OPT_QUIET       1 << 6  /* 静默模式 */
#define CLUSTER_MANAGER_OPT_VERBOSE     1 << 7  /* 详细模式 */

/* 集群管理器日志级别 */
#define CLUSTER_MANAGER_LOG_LVL_INFO    1   /* 信息 */
#define CLUSTER_MANAGER_LOG_LVL_WARN    2   /* 警告 */
#define CLUSTER_MANAGER_LOG_LVL_ERR     3   /* 错误 */
#define CLUSTER_MANAGER_LOG_LVL_SUCCESS 4   /* 成功 */

/* 加入集群后等待检查的秒数 */
#define CLUSTER_JOIN_CHECK_AFTER        20

/* ANSI 终端颜色转义序列 */
#define LOG_COLOR_BOLD      "29;1m"
#define LOG_COLOR_RED       "31;1m"
#define LOG_COLOR_GREEN     "32;1m"
#define LOG_COLOR_YELLOW    "33;1m"
#define LOG_COLOR_RESET     "0m"

/* cliConnect() 的标志位 */
#define CC_FORCE (1<<0)         /* 已连接时强制重连 */
#define CC_QUIET (1<<1)         /* 不打印连接错误 */

/* DNS 解析 */
#define NET_IP_STR_LEN 46       /* INET6_ADDRSTRLEN 为 46 字节 */

/* 刷新间隔，单位毫秒 */
#define REFRESH_INTERVAL 300

/* 判断当前是否在 TTY 或伪 TTY 环境中（含 FAKETTY 环境变量） */
#define IS_TTY_OR_FAKETTY() (isatty(STDOUT_FILENO) || getenv("FAKETTY"))

/* --latency-dist 选项使用的调色板（颜色版） */
int spectrum_palette_color_size = 19;
int spectrum_palette_color[] = {0,233,234,235,237,239,241,243,245,247,144,143,142,184,226,214,208,202,196};

/* 灰度版调色板 */
int spectrum_palette_mono_size = 13;
int spectrum_palette_mono[] = {0,233,234,235,237,239,241,243,245,247,249,251,253};

/* 当前正在使用的调色板 */
int *spectrum_palette;
int spectrum_palette_size;

/* 终端原始模式保存状态 */
static int orig_termios_saved = 0;
static struct termios orig_termios; /* 用于退出时恢复终端状态 */

/* 字典辅助函数 */
static uint64_t dictSdsHash(const void *key);
static int dictSdsKeyCompare(dict *d, const void *key1,
    const void *key2);
static void dictSdsDestructor(dict *d, void *val);
static void dictListDestructor(dict *d, void *val);

/* 集群管理器命令信息结构 */
typedef struct clusterManagerCommand {
    char *name;          /* 命令名 */
    int argc;            /* 参数数量 */
    char **argv;         /* 参数数组 */
    sds stdin_arg;       /* 来自标准输入的参数（-X 选项） */
    int flags;           /* 命令标志位 */
    int replicas;        /* 副本数 */
    char *from;          /* 源地址 */
    char *to;            /* 目标地址 */
    char **weight;       /* 权重数组 */
    int weight_argc;     /* 权重参数数量 */
    char *master_id;     /* 主节点 ID */
    int slots;           /* 槽位数 */
    int timeout;         /* 超时时间 */
    int pipeline;        /* 流水线大小 */
    float threshold;     /* 阈值 */
    char *backup_dir;    /* 备份目录 */
    char *from_user;     /* 源用户名 */
    char *from_pass;     /* 源密码 */
    int from_askpass;    /* 是否需要交互输入源密码 */
} clusterManagerCommand;

static int createClusterManagerCommand(char *cmdname, int argc, char **argv);


/* 当前 Redis 连接上下文 */
static redisContext *context;

/* redis-cli 的全局配置结构体，保存命令行参数和运行时状态 */
static struct config {
    cliConnInfo conn_info;                  /* 连接信息（主机/端口等） */
    struct timeval connect_timeout;          /* 连接超时时间 */
    char *hostsocket;                        /* Unix 套接字路径 */
    int tls;                                 /* 是否启用 TLS */
    cliSSLconfig sslconfig;                  /* SSL/TLS 配置 */
    long repeat;                             /* 重复执行次数 */
    long interval;                           /* 重复执行间隔 */
    int dbnum; /* 当前选中的数据库编号 */
    int interactive;                         /* 是否为交互模式 */
    int shutdown;                            /* 是否关闭服务器 */
    int monitor_mode;                        /* 是否处于 MONITOR 模式 */
    int pubsub_mode;                         /* 是否处于发布订阅模式 */
    int blocking_state_aborted; /* 用于中止 monitor_mode 和 pubsub_mode */
    int latency_mode;                        /* 延迟测试模式 */
    int latency_dist_mode;                   /* 延迟分布模式 */
    int latency_history;                     /* 延迟历史模式 */
    int lru_test_mode;                       /* LRU 测试模式 */
    long long lru_test_sample_size;          /* LRU 测试采样大小 */
    int cluster_mode;                        /* 集群模式 */
    int cluster_reissue_command;             /* 集群模式下重新发送命令 */
    int cluster_send_asking;                 /* 集群模式下发送 ASKING */
    int slave_mode;                          /* 复制模式 */
    int pipe_mode;                           /* 管道模式 */
    int pipe_timeout;                        /* 管道超时 */
    int getrdb_mode;                         /* 获取 RDB 模式 */
    int get_functions_rdb_mode;              /* 获取函数库 RDB 模式 */
    int stat_mode;                           /* 统计模式 */
    int scan_mode;                           /* SCAN 模式 */
    int count;                               /* COUNT 参数 */
    int intrinsic_latency_mode;              /* 系统固有延迟测量模式 */
    int intrinsic_latency_duration;          /* 固有延迟测量时长 */
    sds pattern;                             /* 模式匹配字符串 */
    char *rdb_filename;                      /* RDB 文件名 */
    int bigkeys;                             /* --bigkeys 模式 */
    int memkeys;                             /* --memkeys 模式 */
    long long memkeys_samples;               /* --memkeys 采样数 */
    int hotkeys;                             /* --hotkeys 模式 */
    int keystats;                            /* --keystats 模式 */
    unsigned long long cursor;               /* SCAN 游标 */
    unsigned long top_sizes_limit;           /* top 列表大小限制 */
    int stdin_lastarg; /* 从标准输入读取最后一个参数（-x 选项） */
    int stdin_tag_arg; /* 从标准输入读取 <tag> 参数（-X 选项） */
    char *stdin_tag_name; /* 用户输入的占位符标签名 */
    int askpass;                             /* 交互式输入密码 */
    int quoted_input;   /* 强制输入参数视为带引号字符串 */
    int output; /* 输出模式，参见 OUTPUT_* 宏定义 */
    int push_output; /* 是否显示自发的 PUSH 响应 */
    sds mb_delim;                            /* 多块分隔符 */
    sds cmd_delim;                           /* 命令分隔符 */
    char prompt[128];                        /* 交互模式提示符 */
    char *eval;                              /* EVAL 命令字符串 */
    int eval_ldb;                            /* 启用 Lua 调试器 */
    int eval_ldb_sync;  /* 请求 Lua 调试器进入同步模式 */
    int eval_ldb_end;   /* Lua 调试会话已结束 */
    int enable_ldb_on_eval; /* 处理手动 SCRIPT DEBUG + EVAL 命令 */
    int last_cmd_type;                       /* 上一个命令的类型 */
    redisReply *last_reply;                  /* 上一次回复对象 */
    int verbose;                             /* 详细输出模式 */
    int set_errcode;                         /* 是否设置错误返回码 */
    clusterManagerCommand cluster_manager_command; /* 集群管理命令 */
    int no_auth_warning;                     /* 不打印 AUTH 警告 */
    int resp2; /* 1：显式通过 -2 选项指定 */
    int resp3; /* 1：显式指定；2：隐式（如 --json 选项） */
    int current_resp3; /* 当前连接是否启用了 RESP3 协议 */
    int in_multi;                            /* 是否处于 MULTI/EXEC 事务中 */
    int pre_multi_dbnum;                     /* 进入 MULTI 前的数据库编号 */
    char *server_version;                    /* 服务器版本 */
    char *test_hint;                         /* 测试提示 */
    char *test_hint_file;                    /* 测试提示文件 */
    int prefer_ipv4; /* DNS 解析时优先使用 IPv4 */
    int prefer_ipv6; /* DNS 解析时优先使用 IPv6 */
} config;

/* 用户偏好设置 */
static struct pref {
    int hints;                              /* 是否启用命令提示 */
} pref;

/* 用于在信号处理函数中通知主循环强制取消 */
static volatile sig_atomic_t force_cancel_loop = 0;

/* 函数前置声明 */
static void usage(int err);                 /* 打印帮助信息 */
static void slaveMode(int send_sync);       /* 进入 SLAVEOF 模式 */
static int cliConnect(int flags);           /* 建立到 Redis 的连接 */

/* 从 INFO 输出中获取字段值的辅助函数 */
static char *getInfoField(char *info, char *field);
static long getLongInfoField(char *info, char *field);

/*------------------------------------------------------------------------------
 * Utility functions — 通用工具函数
 *--------------------------------------------------------------------------- */
size_t redis_strlcpy(char *dst, const char *src, size_t dsize); /* 字符串安全拷贝 */

static void cliPushHandler(void *, void *); /* 处理服务器主动推送的消息 */

uint16_t crc16(const char *buf, int len);   /* CRC-16 校验 */

/* 获取当前时间，单位为微秒 */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* 获取当前时间，单位为毫秒 */
static long long mstime(void) {
    return ustime()/1000;
}

/* 刷新交互模式下的命令行提示符 */
static void cliRefreshPrompt(void) {
    if (config.eval_ldb) return;  /* Lua 调试模式下不刷新提示符 */

    sds prompt = sdsempty();
    if (config.hostsocket != NULL) {
        /* 使用 Unix 套接字连接 */
        prompt = sdscatfmt(prompt,"redis %s",config.hostsocket);
    } else {
        /* 使用 TCP 连接，格式化主机地址 */
        char addr[256];
        formatAddr(addr, sizeof(addr), config.conn_info.hostip, config.conn_info.hostport);
        prompt = sdscatlen(prompt,addr,strlen(addr));
    }

    /* 如有必要添加 [dbnum] */
    if (config.dbnum != 0)
        prompt = sdscatfmt(prompt,"[%i]",config.dbnum);

    /* 如果处于事务状态则添加 TX 标识 */
    if (config.in_multi)
        prompt = sdscatlen(prompt,"(TX)",4);

    if (config.pubsub_mode)
        prompt = sdscatfmt(prompt,"(subscribed mode)");

    /* 复制提示符到静态缓冲区，附加 "> " */
    prompt = sdscatlen(prompt,"> ",2);
    snprintf(config.prompt,sizeof(config.prompt),"%s",prompt);
    sdsfree(prompt);
}

/* 获取指定 'dotfilename' 的完整路径。
 *
 * 通常只是简单地将用户的 $HOME 与 'dotfilename' 拼接。
 * 但是如果环境变量 'envoverride' 被设置，则使用其值作为路径。
 *
 * 返回值：若文件是 /dev/null 或因错误无法获取则返回 NULL；
 * 否则返回一个 SDS 字符串，调用者需要负责释放。 */
static sds getDotfilePath(char *envoverride, char *dotfilename) {
    char *path = NULL;
    sds dotPath = NULL;

    /* 检查环境变量是否覆盖了点文件路径 */
    path = getenv(envoverride);
    if (path != NULL && *path != '\0') {
        if (!strcmp("/dev/null", path)) {
            return NULL;  /* 显式禁用配置文件 */
        }

        /* 如果环境变量已设置，直接返回其值 */
        dotPath = sdsnew(path);
    } else {
        char *home = getenv("HOME");
        if (home != NULL && *home != '\0') {
            /* 未设置覆盖则使用 $HOME/<dotfilename> */
            dotPath = sdscatprintf(sdsempty(), "%s/%s", home, dotfilename);
        }
    }
    return dotPath;
}

/* SDS 字典键的哈希函数 */
static uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

/* SDS 字典键的比较函数 */
static int dictSdsKeyCompare(dict *d, const void *key1, const void *key2)
{
    int l1,l2;
    UNUSED(d);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;       /* 长度不同则不相等 */
    return memcmp(key1, key2, l1) == 0;
}

/* SDS 字典值的析构函数：释放 sds 字符串 */
static void dictSdsDestructor(dict *d, void *val)
{
    UNUSED(d);
    sdsfree(val);
}

/* 链表字典值的析构函数：释放 adlist 链表 */
void dictListDestructor(dict *d, void *val)
{
    UNUSED(d);
    listRelease((list*)val);
}

/* 打印前先清除当前行（在 TTY 中），返回打印的行数 */
int cleanPrintfln(char *fmt, ...) {
    va_list args;
    char buf[1024]; /* 输出长度限制 */
    int char_count, line_count = 0;

    /* 如果处于 TTY 模式则先清除当前行 */
    if (IS_TTY_OR_FAKETTY()) {
        printf("\033[2K\r");
    }

    va_start(args, fmt);
    char_count = vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);

    if (char_count >= (int)sizeof(buf)) {
        fprintf(stderr, "Warning: String was trimmed in cleanPrintln\n");
    }

    /* 逐行输出，并统计行数 */
    char *position, *string = buf;
    while ((position = strchr(string, '\n')) != NULL) {
        int line_length = (int)(position - string);
        printf("%.*s\n", line_length, string);
        string = position + 1;
        line_count++;
    }

    printf("%s\n", string);
    return line_count + 1;
}

/*------------------------------------------------------------------------------
 * Help functions — 帮助信息相关函数
 *--------------------------------------------------------------------------- */

#define CLI_HELP_COMMAND 1
#define CLI_HELP_GROUP 2

typedef struct {
    int type;
    int argc;
    sds *argv;
    sds full;

    /* Only used for help on commands */
    struct commandDocs docs;
} helpEntry;

static helpEntry *helpEntries = NULL;
static int helpEntriesLen = 0;

/* 兼容 7.0 之前服务器的帮助信息整合函数。
 * cliLegacyInitHelp() 使用 commands.c 中的命令和分组名称初始化
 * helpEntries 数组。但是我们连接的 Redis 实例可能支持更多命令，
 * 所以此函数将旧的条目与使用新版 Redis 的 COMMAND 命令获取的
 * 额外条目进行整合。 */
static void cliLegacyIntegrateHelp(void) {
    if (cliConnect(CC_QUIET) == REDIS_ERR) return;

    redisReply *reply = redisCommand(context, "COMMAND");
    if(reply == NULL || reply->type != REDIS_REPLY_ARRAY) return;

    /* 遍历 COMMAND 返回的数组，只填充尚不存在的条目 */
    for (size_t j = 0; j < reply->elements; j++) {
        redisReply *entry = reply->element[j];
        if (entry->type != REDIS_REPLY_ARRAY || entry->elements < 4 ||
            entry->element[0]->type != REDIS_REPLY_STRING ||
            entry->element[1]->type != REDIS_REPLY_INTEGER ||
            entry->element[3]->type != REDIS_REPLY_INTEGER) return;
        char *cmdname = entry->element[0]->str;
        int i;

        for (i = 0; i < helpEntriesLen; i++) {
            helpEntry *he = helpEntries+i;
            if (!strcasecmp(he->argv[0],cmdname))
                break;
        }
        if (i != helpEntriesLen) continue;

        helpEntriesLen++;
        helpEntries = zrealloc(helpEntries,sizeof(helpEntry)*helpEntriesLen);
        helpEntry *new = helpEntries+(helpEntriesLen-1);

        new->argc = 1;
        new->argv = zmalloc(sizeof(sds));
        new->argv[0] = sdsnew(cmdname);
        new->full = new->argv[0];
        new->type = CLI_HELP_COMMAND;
        sdstoupper(new->argv[0]);

        new->docs.name = new->argv[0];
        new->docs.args = NULL;
        new->docs.numargs = 0;
        new->docs.params = sdsempty();
        int args = llabs(entry->element[1]->integer);
        args--; /* Remove the command name itself. */
        if (entry->element[3]->integer == 1) {
            new->docs.params = sdscat(new->docs.params,"key ");
            args--;
        }
        while(args-- > 0) new->docs.params = sdscat(new->docs.params,"arg ");
        if (entry->element[1]->integer < 0)
            new->docs.params = sdscat(new->docs.params,"...options...");
        new->docs.summary = "Help not available";  /* 无可用帮助摘要 */
        new->docs.since = "Not known";           /* 引入版本未知 */
        new->docs.group = "generic";             /* 通用分组 */
    }
    freeReplyObject(reply);
}

/* 将字符串拼接到 sds 末尾，若字符串为空则改为拼接双引号 */
static sds sdscat_orempty(sds params, const char *value) {
    if (value[0] == '\0') {
        return sdscat(params, "\"\"");
    }
    return sdscat(params, value);
}

static sds makeHint(char **inputargv, int inputargc, int cmdlen, struct commandDocs docs);

static void cliAddCommandDocArg(cliCommandArg *cmdArg, redisReply *argMap);

/* 递归构建命令参数文档数组 */
static void cliMakeCommandDocArgs(redisReply *arguments, cliCommandArg *result) {
    for (size_t j = 0; j < arguments->elements; j++) {
        cliAddCommandDocArg(&result[j], arguments->element[j]);
    }
}

/* 解析单个命令参数的元数据（name/type/flags 等） */
static void cliAddCommandDocArg(cliCommandArg *cmdArg, redisReply *argMap) {
    if (argMap->type != REDIS_REPLY_MAP && argMap->type != REDIS_REPLY_ARRAY) {
        return;
    }

    for (size_t i = 0; i < argMap->elements; i += 2) {
        assert(argMap->element[i]->type == REDIS_REPLY_STRING);
        char *key = argMap->element[i]->str;
        if (!strcmp(key, "name")) {
            /* 参数名 */
            assert(argMap->element[i + 1]->type == REDIS_REPLY_STRING);
            cmdArg->name = sdsnew(argMap->element[i + 1]->str);
        } else if (!strcmp(key, "display_text")) {
            /* 显示文本 */
            assert(argMap->element[i + 1]->type == REDIS_REPLY_STRING);
            cmdArg->display_text = sdsnew(argMap->element[i + 1]->str);
        } else if (!strcmp(key, "token")) {
            /* 参数标记 */
            assert(argMap->element[i + 1]->type == REDIS_REPLY_STRING);
            cmdArg->token = sdsnew(argMap->element[i + 1]->str);
        } else if (!strcmp(key, "type")) {
            /* 参数类型 */
            assert(argMap->element[i + 1]->type == REDIS_REPLY_STRING);
            char *type = argMap->element[i + 1]->str;
            if (!strcmp(type, "string")) {
                cmdArg->type = ARG_TYPE_STRING;
            } else if (!strcmp(type, "integer")) {
                cmdArg->type = ARG_TYPE_INTEGER;
            } else if (!strcmp(type, "double")) {
                cmdArg->type = ARG_TYPE_DOUBLE;
            } else if (!strcmp(type, "key")) {
                cmdArg->type = ARG_TYPE_KEY;
            } else if (!strcmp(type, "pattern")) {
                cmdArg->type = ARG_TYPE_PATTERN;
            } else if (!strcmp(type, "unix-time")) {
                cmdArg->type = ARG_TYPE_UNIX_TIME;
            } else if (!strcmp(type, "pure-token")) {
                cmdArg->type = ARG_TYPE_PURE_TOKEN;
            } else if (!strcmp(type, "oneof")) {
                cmdArg->type = ARG_TYPE_ONEOF;
            } else if (!strcmp(type, "block")) {
                cmdArg->type = ARG_TYPE_BLOCK;
            }
        } else if (!strcmp(key, "arguments")) {
            /* 子参数列表 */
            redisReply *arguments = argMap->element[i + 1];
            cmdArg->subargs = zcalloc(arguments->elements * sizeof(cliCommandArg));
            cmdArg->numsubargs = arguments->elements;
            cliMakeCommandDocArgs(arguments, cmdArg->subargs);
        } else if (!strcmp(key, "flags")) {
            /* 参数标志：optional/multiple 等 */
            redisReply *flags = argMap->element[i + 1];
            assert(flags->type == REDIS_REPLY_SET || flags->type == REDIS_REPLY_ARRAY);
            for (size_t j = 0; j < flags->elements; j++) {
                assert(flags->element[j]->type == REDIS_REPLY_STATUS);
                char *flag = flags->element[j]->str;
                if (!strcmp(flag, "optional")) {
                    cmdArg->flags |= CMD_ARG_OPTIONAL;        /* 可选参数 */
                } else if (!strcmp(flag, "multiple")) {
                    cmdArg->flags |= CMD_ARG_MULTIPLE;        /* 可重复 */
                } else if (!strcmp(flag, "multiple_token")) {
                    cmdArg->flags |= CMD_ARG_MULTIPLE_TOKEN;  /* 可重复标记 */
                }
            }
        }
    }
}

/* 填充命令/子命令名称对应的帮助条目字段 */
static void cliFillInCommandHelpEntry(helpEntry *help, char *cmdname, char *subcommandname) {
    help->argc = subcommandname ? 2 : 1;
    help->argv = zmalloc(sizeof(sds) * help->argc);
    help->argv[0] = sdsnew(cmdname);
    sdstoupper(help->argv[0]);  /* 命令名转大写 */
    if (subcommandname) {
        /* 子命令名称可能由竖线分隔的两个单词组成 */
        char *pipe = strchr(subcommandname, '|');
        if (pipe != NULL) {
            help->argv[1] = sdsnew(pipe + 1);
        } else {
            help->argv[1] = sdsnew(subcommandname);
        }
        sdstoupper(help->argv[1]);
    }
    sds fullname = sdsnew(help->argv[0]);
    if (subcommandname) {
        fullname = sdscat(fullname, " ");
        fullname = sdscat(fullname, help->argv[1]);
    }
    help->full = fullname;
    help->type = CLI_HELP_COMMAND;

    help->docs.name = help->full;
    help->docs.params = NULL;
    help->docs.args = NULL;
    help->docs.numargs = 0;
    help->docs.since = NULL;
}

/* 为 'specs' 所描述的命令/子命令初始化帮助条目。
 * 'next' 指向下一个待填充的帮助条目。
 * 'groups' 是要填充的命令分组名集合。
 * 返回帮助条目表中的下一个可用位置指针。
 * 如果命令有子命令，则会递归调用自身处理子命令。 */
static helpEntry *cliInitCommandHelpEntry(char *cmdname, char *subcommandname,
                                          helpEntry *next, redisReply *specs,
                                          dict *groups) {
    helpEntry *help = next++;
    cliFillInCommandHelpEntry(help, cmdname, subcommandname);

    assert(specs->type == REDIS_REPLY_MAP || specs->type == REDIS_REPLY_ARRAY);
    for (size_t j = 0; j < specs->elements; j += 2) {
        assert(specs->element[j]->type == REDIS_REPLY_STRING);
        char *key = specs->element[j]->str;
        if (!strcmp(key, "summary")) {
            /* 命令摘要 */
            redisReply *reply = specs->element[j + 1];
            assert(reply->type == REDIS_REPLY_STRING);
            help->docs.summary = sdsnew(reply->str);
        } else if (!strcmp(key, "since")) {
            /* 引入版本 */
            redisReply *reply = specs->element[j + 1];
            assert(reply->type == REDIS_REPLY_STRING);
            help->docs.since = sdsnew(reply->str);
        } else if (!strcmp(key, "group")) {
            /* 命令分组 */
            redisReply *reply = specs->element[j + 1];
            assert(reply->type == REDIS_REPLY_STRING);
            help->docs.group = sdsnew(reply->str);
            sds group = sdsdup(help->docs.group);
            if (dictAdd(groups, group, NULL) != DICT_OK) {
                sdsfree(group);
            }
        } else if (!strcmp(key, "arguments")) {
            /* 参数列表 */
            redisReply *arguments = specs->element[j + 1];
            assert(arguments->type == REDIS_REPLY_ARRAY);
            help->docs.args = zcalloc(arguments->elements * sizeof(cliCommandArg));
            help->docs.numargs = arguments->elements;
            cliMakeCommandDocArgs(arguments, help->docs.args);
            help->docs.params = makeHint(NULL, 0, 0, help->docs);
        } else if (!strcmp(key, "subcommands")) {
            /* 子命令列表，递归处理 */
            redisReply *subcommands = specs->element[j + 1];
            assert(subcommands->type == REDIS_REPLY_MAP || subcommands->type == REDIS_REPLY_ARRAY);
            for (size_t i = 0; i < subcommands->elements; i += 2) {
                assert(subcommands->element[i]->type == REDIS_REPLY_STRING);
                char *subcommandname = subcommands->element[i]->str;
                redisReply *subcommand = subcommands->element[i + 1];
                assert(subcommand->type == REDIS_REPLY_MAP || subcommand->type == REDIS_REPLY_ARRAY);
                next = cliInitCommandHelpEntry(cmdname, subcommandname, next, subcommand, groups);
            }
        }
    }
    return next;
}

/* 返回命令文档表中所有命令及子命令的总数 */
static size_t cliCountCommands(redisReply* commandTable) {
    size_t numCommands = commandTable->elements / 2;

    /* 命令文档表将命令名映射到其规格说明 */
    for (size_t i = 0; i < commandTable->elements; i += 2) {
        assert(commandTable->element[i]->type == REDIS_REPLY_STRING);  /* 命令名 */
        assert(commandTable->element[i + 1]->type == REDIS_REPLY_MAP ||
               commandTable->element[i + 1]->type == REDIS_REPLY_ARRAY);
        redisReply *map = commandTable->element[i + 1];
        for (size_t j = 0; j < map->elements; j += 2) {
            assert(map->element[j]->type == REDIS_REPLY_STRING);
            char *key = map->element[j]->str;
            if (!strcmp(key, "subcommands")) {
                redisReply *subcommands = map->element[j + 1];
                assert(subcommands->type == REDIS_REPLY_MAP || subcommands->type == REDIS_REPLY_ARRAY);
                numCommands += subcommands->elements / 2;
            }
        }
    }
    return numCommands;
}

/* 帮助条目排序的比较函数：按完整名称字典序比较 */
int helpEntryCompare(const void *entry1, const void *entry2) {
    helpEntry *i1 = (helpEntry *)entry1;
    helpEntry *i2 = (helpEntry *)entry2;
    return strcmp(i1->full, i2->full);
}

/* 为命令分组初始化帮助条目。
 * 在命令帮助条目填充完毕后调用，用于扩展帮助表添加分组条目。 */
void cliInitGroupHelpEntries(dict *groups) {
    dictIterator *iter = dictGetIterator(groups);
    dictEntry *entry;
    helpEntry tmp;

    int numGroups = dictSize(groups);
    int pos = helpEntriesLen;
    helpEntriesLen += numGroups;
    helpEntries = zrealloc(helpEntries, sizeof(helpEntry)*helpEntriesLen);

    for (entry = dictNext(iter); entry != NULL; entry = dictNext(iter)) {
        tmp.argc = 1;
        tmp.argv = zmalloc(sizeof(sds));
        tmp.argv[0] = sdscatprintf(sdsempty(),"@%s",(char *)dictGetKey(entry));
        tmp.full = tmp.argv[0];
        tmp.type = CLI_HELP_GROUP;
        tmp.docs.name = NULL;
        tmp.docs.params = NULL;
        tmp.docs.args = NULL;
        tmp.docs.numargs = 0;
        tmp.docs.summary = NULL;
        tmp.docs.since = NULL;
        tmp.docs.group = NULL;
        helpEntries[pos++] = tmp;
    }
    dictReleaseIterator(iter);
}

/* 为 COMMAND DOCS 回复中所有命令初始化帮助条目 */
void cliInitCommandHelpEntries(redisReply *commandTable, dict *groups) {
    helpEntry *next = helpEntries;
    for (size_t i = 0; i < commandTable->elements; i += 2) {
        assert(commandTable->element[i]->type == REDIS_REPLY_STRING);
        char *cmdname = commandTable->element[i]->str;

        assert(commandTable->element[i + 1]->type == REDIS_REPLY_MAP ||
               commandTable->element[i + 1]->type == REDIS_REPLY_ARRAY);
        redisReply *cmdspecs = commandTable->element[i + 1];
        next = cliInitCommandHelpEntry(cmdname, NULL, next, cmdspecs, groups);
    }
}

/* 判断服务器版本是否支持自某版本起才可用的命令/参数。
 * 支持则返回 1，"since" 版本比 "version" 新则返回 0。 */
static int versionIsSupported(sds version, sds since) {
    int i;
    char *versionPos = version;
    char *sincePos = since;
    if (!since) {
        return 1;  /* 没有 since 信息默认支持 */
    }

    /* 比较主版本、次版本、修订号 */
    for (i = 0; i != 3; i++) {
        int versionPart = atoi(versionPos);
        int sincePart = atoi(sincePos);
        if (versionPart > sincePart) {
            return 1;
        } else if (sincePart > versionPart) {
            return 0;
        }
        versionPos = strchr(versionPos, '.');
        sincePos = strchr(sincePos, '.');

        /* 同时解析完两个版本号说明二者相等 */
        if (!versionPos && !sincePos) return 1;

        /* 位数不同视为不支持 */
        if (!versionPos || !sincePos) return 0;

        versionPos++;
        sincePos++;
    }
    return 0;
}

/* 移除当前服务器版本不支持的参数项 */
static void removeUnsupportedArgs(struct cliCommandArg *args, int *numargs, sds version) {
    int i = 0, j;
    while (i != *numargs) {
        if (versionIsSupported(version, args[i].since)) {
            if (args[i].subargs) {
                removeUnsupportedArgs(args[i].subargs, &args[i].numsubargs, version);
            }
            i++;
            continue;
        }
        for (j = i; j != *numargs - 1; j++) {
            args[j] = args[j + 1];
        }
        (*numargs)--;
    }
}

/* 旧版帮助条目初始化：使用静态 commandDocs 数据 */
static helpEntry *cliLegacyInitCommandHelpEntry(char *cmdname, char *subcommandname,
                                                helpEntry *next, struct commandDocs *command,
                                                dict *groups, sds version) {
    helpEntry *help = next++;
    cliFillInCommandHelpEntry(help, cmdname, subcommandname);

    help->docs.summary = sdsnew(command->summary);
    help->docs.since = sdsnew(command->since);
    help->docs.group = sdsnew(command->group);
    sds group = sdsdup(help->docs.group);
    if (dictAdd(groups, group, NULL) != DICT_OK) {
        sdsfree(group);
    }

    if (command->args != NULL) {
        help->docs.args = command->args;
        help->docs.numargs = command->numargs;
        /* 移除当前服务器版本不支持的参数 */
        if (version)
            removeUnsupportedArgs(help->docs.args, &help->docs.numargs, version);
        help->docs.params = makeHint(NULL, 0, 0, help->docs);
    }

    if (command->subcommands != NULL) {
        for (size_t i = 0; command->subcommands[i].name != NULL; i++) {
            if (!version || versionIsSupported(version, command->subcommands[i].since)) {
                char *subcommandname = command->subcommands[i].name;
                next = cliLegacyInitCommandHelpEntry(
                    cmdname, subcommandname, next, &command->subcommands[i], groups, version);
            }
        }
    }
    return next;
}

int cliLegacyInitCommandHelpEntries(struct commandDocs *commands, dict *groups, sds version) {
    helpEntry *next = helpEntries;
    for (size_t i = 0; commands[i].name != NULL; i++) {
        if (!version || versionIsSupported(version, commands[i].since)) {
            next = cliLegacyInitCommandHelpEntry(commands[i].name, NULL, next, &commands[i], groups, version);
        }
    }
    return next - helpEntries;
}

/* 返回命令文档表中所有命令及子命令的总数（可选按服务器版本过滤） */
static size_t cliLegacyCountCommands(struct commandDocs *commands, sds version) {
    int numCommands = 0;
    for (size_t i = 0; commands[i].name != NULL; i++) {
        if (version && !versionIsSupported(version, commands[i].since)) {
            continue;
        }
        numCommands++;
        if (commands[i].subcommands != NULL) {
            numCommands += cliLegacyCountCommands(commands[i].subcommands, version);
        }
    }
    return numCommands;
}

/* 通过调用 INFO SERVER 获取服务器版本字符串。
 * 结果存储在 config.server_version 中。
 * 未连接或无法获取时返回 NULL。 */
static sds cliGetServerVersion(void) {
    static const char *key = "\nredis_version:";
    redisReply *serverInfo = NULL;
    char *pos;

    if (config.server_version != NULL) {
        return config.server_version;  /* 缓存命中 */
    }

    if (!context) return NULL;
    serverInfo = redisCommand(context, "INFO SERVER");
    if (serverInfo == NULL || serverInfo->type == REDIS_REPLY_ERROR) {
        freeReplyObject(serverInfo);
        return sdsempty();
    }

    assert(serverInfo->type == REDIS_REPLY_STRING || serverInfo->type == REDIS_REPLY_VERB);
    sds info = serverInfo->str;

    /* 在 INFO SERVER 输出中找到 "redis_version" 的首次出现位置 */
    pos = strstr(info, key);
    if (pos) {
        pos += strlen(key);
        char *end = strchr(pos, '\r');
        if (end) {
            sds version = sdsnewlen(pos, end - pos);
            freeReplyObject(serverInfo);
            config.server_version = version;
            return version;
        }
    }
    freeReplyObject(serverInfo);
    return NULL;
}

/* 使用静态 cli_commands.c 数据初始化帮助信息（兼容旧版本） */
static void cliLegacyInitHelp(dict *groups) {
    sds serverVersion = cliGetServerVersion();

    /* 遍历 commandDocs 数组并填充条目 */
    helpEntriesLen = cliLegacyCountCommands(redisCommandTable, serverVersion);
    helpEntries = zmalloc(sizeof(helpEntry)*helpEntriesLen);

    helpEntriesLen = cliLegacyInitCommandHelpEntries(redisCommandTable, groups, serverVersion);
    cliInitGroupHelpEntries(groups);

    qsort(helpEntries, helpEntriesLen, sizeof(helpEntry), helpEntryCompare);
    dictRelease(groups);
}

/* cliInitHelp() 通过 COMMAND DOCS 命令获取命令与分组名称
 * 以及命令描述，并据此设置 helpEntries 数组。 */
static void cliInitHelp(void) {
    /* 字符串集合的字典类型，用于收集命令分组名 */
    dictType groupsdt = {
        dictSdsHash,                /* 哈希函数 */
        NULL,                       /* 键复制函数 */
        NULL,                       /* 值复制函数 */
        dictSdsKeyCompare,          /* 键比较函数 */
        dictSdsDestructor,          /* 键析构函数 */
        NULL,                       /* 值析构函数 */
        NULL                        /* 允许扩展回调 */
    };
    redisReply *commandTable;
    dict *groups;

    if (cliConnect(CC_QUIET) == REDIS_ERR) {
        /* 无法连接服务器，但仍然希望提供帮助信息，
         * 改为从静态 cli_commands.c 数据生成。 */
        groups = dictCreate(&groupsdt);
        cliLegacyInitHelp(groups);
        return;
    }
    commandTable = redisCommand(context, "COMMAND DOCS");
    if (commandTable == NULL || commandTable->type == REDIS_REPLY_ERROR) {
        /* 不支持新的 COMMAND DOCS 子命令，改用静态数据生成 */
        freeReplyObject(commandTable);

        groups = dictCreate(&groupsdt);
        cliLegacyInitHelp(groups);
        cliLegacyIntegrateHelp();
        return;
    };
    if (commandTable->type != REDIS_REPLY_MAP && commandTable->type != REDIS_REPLY_ARRAY) return;

    /* 遍历 COMMAND DOCS 返回的数组并填充条目 */
    helpEntriesLen = cliCountCommands(commandTable);
    helpEntries = zmalloc(sizeof(helpEntry)*helpEntriesLen);

    groups = dictCreate(&groupsdt);
    cliInitCommandHelpEntries(commandTable, groups);
    cliInitGroupHelpEntries(groups);

    qsort(helpEntries, helpEntriesLen, sizeof(helpEntry), helpEntryCompare);
    freeReplyObject(commandTable);
    dictRelease(groups);
}

/* 将命令帮助信息输出到标准输出 */
static void cliOutputCommandHelp(struct commandDocs *help, int group) {
    printf("\r\n  \x1b[1m%s\x1b[0m \x1b[90m%s\x1b[0m\r\n", help->name, help->params);
    printf("  \x1b[33msummary:\x1b[0m %s\r\n", help->summary);
    if (help->since != NULL) {
        printf("  \x1b[33msince:\x1b[0m %s\r\n", help->since);
    }
    if (group) {
        printf("  \x1b[33mgroup:\x1b[0m %s\r\n", help->group);
    }
}

/* 打印通用帮助信息 */
static void cliOutputGenericHelp(void) {
    sds version = cliVersion();
    printf(
        "redis-cli %s\n"
        "To get help about Redis commands type:\n"
        "      \"help @<group>\" to get a list of commands in <group>\n"
        "      \"help <command>\" for help on <command>\n"
        "      \"help <tab>\" to get a list of possible help topics\n"
        "      \"quit\" to exit\n"
        "\n"
        "To set redis-cli preferences:\n"
        "      \":set hints\" enable online hints\n"
        "      \":set nohints\" disable online hints\n"
        "Set your preferences in ~/.redisclirc\n",
        version
    );
    sdsfree(version);
}

/* 输出命令帮助，按分组或命令名过滤 */
static void cliOutputHelp(int argc, char **argv) {
    int i, j;
    char *group = NULL;
    helpEntry *entry;
    struct commandDocs *help;

    if (argc == 0) {
        cliOutputGenericHelp();
        return;
    } else if (argc > 0 && argv[0][0] == '@') {
        group = argv[0]+1;
    }

    if (helpEntries == NULL) {
        /* 使用 COMMAND 命令的结果初始化帮助信息。
         * 当使用 redis-cli help XXX 时需要先初始化。 */
        cliInitHelp();
    }

    assert(argc > 0);
    for (i = 0; i < helpEntriesLen; i++) {
        entry = &helpEntries[i];
        if (entry->type != CLI_HELP_COMMAND) continue;

        help = &entry->docs;
        if (group == NULL) {
            /* 比较所有参数 */
            if (argc <= entry->argc) {
                for (j = 0; j < argc; j++) {
                    if (strcasecmp(argv[j],entry->argv[j]) != 0) break;
                }
                if (j == argc) {
                    cliOutputCommandHelp(help,1);
                }
            }
        } else if (strcasecmp(group, help->group) == 0) {
            cliOutputCommandHelp(help,0);
        }
    }
    printf("\r\n");
}

/* linenoise 库的 Tab 补全回调函数 */
static void completionCallback(const char *buf, linenoiseCompletions *lc) {
    size_t startpos = 0;
    int mask;
    int i;
    size_t matchlen;
    sds tmp;

    if (strncasecmp(buf,"help ",5) == 0) {
        /* "help " 命令的补全 */
        startpos = 5;
        while (isspace(buf[startpos])) startpos++;
        mask = CLI_HELP_COMMAND | CLI_HELP_GROUP;
    } else {
        /* 普通命令补全 */
        mask = CLI_HELP_COMMAND;
    }

    for (i = 0; i < helpEntriesLen; i++) {
        if (!(helpEntries[i].type & mask)) continue;

        matchlen = strlen(buf+startpos);
        if (strncasecmp(buf+startpos,helpEntries[i].full,matchlen) == 0) {
            tmp = sdsnewlen(buf,startpos);
            tmp = sdscat(tmp,helpEntries[i].full);
            linenoiseAddCompletion(lc,tmp);
            sdsfree(tmp);
        }
    }
}

static sds addHintForArgument(sds hint, cliCommandArg *arg);

/* 在构造字符串时给词与词之间添加分隔符。
 * 当字符串长度大于其先前记录的长度 (*len) 且不是最后一个词时
 * 添加分隔符，随后更新长度记录。 */
static sds addSeparator(sds str, size_t *len, char *separator, int is_last) {
    if (sdslen(str) > *len && !is_last) {
        str = sdscat(str, separator);
        *len = sdslen(str);
    }
    return str;
}

/* 递归地将所有参数的 matched* 字段清零 */
static void clearMatchedArgs(cliCommandArg *args, int numargs) {
    for (int i = 0; i != numargs; ++i) {
        args[i].matched = 0;
        args[i].matched_token = 0;
        args[i].matched_name = 0;
        args[i].matched_all = 0;
        if (args[i].subargs) {
            clearMatchedArgs(args[i].subargs, args[i].numsubargs);
        }
    }
}

/* 构造描述参数的补全提示字符串，跳过已经匹配的部分。
 * 所有参数的提示被添加到输入的 'hint' 字符串中，使用 'separator' 分隔。 */
static sds addHintForArguments(sds hint, cliCommandArg *args, int numargs, char *separator) {
    int i, j, incomplete;
    size_t len=sdslen(hint);
    for (i = 0; i < numargs; i++) {
        if (!(args[i].flags & CMD_ARG_OPTIONAL)) {
            hint = addHintForArgument(hint, &args[i]);
            hint = addSeparator(hint, &len, separator, i == numargs-1);
            continue;
        }

        /* 规则：连续的"可选"参数可以以任意顺序出现。
         * 但如果它们后面跟着必需参数，则这些可选参数之后不再出现可选参数。
         *
         * 本段代码将所有连续可选参数作为整体处理，
         * 便于优先显示当前未完成的可选参数补全。 */
        for (j = i, incomplete = -1; j < numargs; j++) {
            if (!(args[j].flags & CMD_ARG_OPTIONAL)) break;
            if (args[j].matched != 0 && args[j].matched_all == 0) {
                /* 用户已开始输入该参数；优先显示其补全 */
                hint = addHintForArgument(hint, &args[j]);
                hint = addSeparator(hint, &len, separator, i == numargs-1);
                incomplete = j;
            }
        }

        /* 如果后续非可选参数尚未匹配，则为本组剩余可选参数也添加提示 */
        if (j == numargs || args[j].matched == 0) {
            for (; i < j; i++) {
                if (incomplete != i) {
                    hint = addHintForArgument(hint, &args[i]);
                    hint = addSeparator(hint, &len, separator, i == numargs-1);
                }
            }
        }

        i = j - 1;
    }
    return hint;
}

/* 为可重复（multiple）类型的参数添加提示字符串中的"重复"片段：[ABC def ...]
 * 重复片段是固定单元，不过滤已匹配元素。 */
static sds addHintForRepeatedArgument(sds hint, cliCommandArg *arg) {
    if (!(arg->flags & CMD_ARG_MULTIPLE)) {
        return hint;
    }

    /* 重复片段总是显示在参数提示末尾，因此输出前
     * 可以安全地清空其匹配标志。 */
    clearMatchedArgs(arg, 1);

    if (hint[0] != '\0') {
        hint = sdscat(hint, " ");
    }
    hint = sdscat(hint, "[");

    if (arg->flags & CMD_ARG_MULTIPLE_TOKEN) {
        hint = sdscat_orempty(hint, arg->token);
        if (arg->type != ARG_TYPE_PURE_TOKEN) {
            hint = sdscat(hint, " ");
        }
    }

    switch (arg->type) {
     case ARG_TYPE_ONEOF:
        hint = addHintForArguments(hint, arg->subargs, arg->numsubargs, "|");
        break;

    case ARG_TYPE_BLOCK:
        hint = addHintForArguments(hint, arg->subargs, arg->numsubargs, " ");
        break;

    case ARG_TYPE_PURE_TOKEN:
        break;

    default:
        hint = sdscat_orempty(hint, arg->display_text ? arg->display_text : arg->name);
        break;
    }

    hint = sdscat(hint, " ...]");
    return hint;
}

/* 若参数尚未完全匹配，则为单个参数添加提示字符串 */
static sds addHintForArgument(sds hint, cliCommandArg *arg) {
    if (arg->matched_all) {
        return hint;
    }

    /* 可选参数用方括号包围，除非它已被部分匹配 */
    if ((arg->flags & CMD_ARG_OPTIONAL) && !arg->matched) {
        hint = sdscat(hint, "[");
    }

    /* 如果有标记 token 且尚未匹配，则以 token 开头 */
    if (arg->token != NULL && !arg->matched_token) {
        hint = sdscat_orempty(hint, arg->token);
        if (arg->type != ARG_TYPE_PURE_TOKEN) {
            hint = sdscat(hint, " ");
        }
    }

    /* 添加参数语法字符串的主体部分 */
    switch (arg->type) {
     case ARG_TYPE_ONEOF:
        if (arg->matched == 0) {
            hint = addHintForArguments(hint, arg->subargs, arg->numsubargs, "|");
        } else {
            int i;
            for (i = 0; i < arg->numsubargs; i++) {
                if (arg->subargs[i].matched != 0) {
                    hint = addHintForArgument(hint, &arg->subargs[i]);
                }
            }
        }
        break;

    case ARG_TYPE_BLOCK:
        hint = addHintForArguments(hint, arg->subargs, arg->numsubargs, " ");
        break;

    case ARG_TYPE_PURE_TOKEN:
        break;

    default:
        if (!arg->matched_name) {
            hint = sdscat_orempty(hint, arg->display_text ? arg->display_text : arg->name);
        }
        break;
    }

    hint = addHintForRepeatedArgument(hint, arg);

    if ((arg->flags & CMD_ARG_OPTIONAL) && !arg->matched) {
        hint = sdscat(hint, "]");
    }

    return hint;
}

static int matchArg(char **nextword, int numwords, cliCommandArg *arg);
static int matchArgs(char **words, int numwords, cliCommandArg *args, int numargs);

/* 尝试将输入的下一组单词与某个参数进行匹配 */
static int matchNoTokenArg(char **nextword, int numwords, cliCommandArg *arg) {
    int i;
    switch (arg->type) {
    case ARG_TYPE_BLOCK: {
        arg->matched += matchArgs(nextword, numwords, arg->subargs, arg->numsubargs);

        /* 所有子参数都必须匹配，块才算匹配 */
        arg->matched_all = 1;
        for (i = 0; i < arg->numsubargs; i++) {
            if (arg->subargs[i].matched_all == 0) {
                arg->matched_all = 0;
            }
        }
        break;
    }
    case ARG_TYPE_ONEOF: {
        /* 二选一/多选一：任意子参数匹配即可 */
        for (i = 0; i < arg->numsubargs; i++) {
            if (matchArg(nextword, numwords, &arg->subargs[i])) {
                arg->matched += arg->subargs[i].matched;
                arg->matched_all = arg->subargs[i].matched_all;
                break;
            }
        }
        break;
    }

    case ARG_TYPE_INTEGER:
    case ARG_TYPE_UNIX_TIME: {
        long long value;
        if (sscanf(*nextword, "%lld", &value) == 1) {
            arg->matched += 1;
            arg->matched_name = 1;
            arg->matched_all = 1;
        } else {
            /* 类型不正确导致匹配失败 */
            arg->matched = 0;
            arg->matched_name = 0;
        }
        break;
    }

    case ARG_TYPE_DOUBLE: {
        double value;
        if (sscanf(*nextword, "%lf", &value) == 1) {
            arg->matched += 1;
            arg->matched_name = 1;
            arg->matched_all = 1;
        } else {
            /* 类型不正确导致匹配失败 */
            arg->matched = 0;
            arg->matched_name = 0;
        }
        break;
    }

    default:
        arg->matched += 1;
        arg->matched_name = 1;
        arg->matched_all = 1;
        break;
    }
    return arg->matched;
}

/* 尝试将输入的下一个单词与 token 字面量进行匹配 */
static int matchToken(char **nextword, cliCommandArg *arg) {
    if (strcasecmp(arg->token, nextword[0]) != 0) {
        return 0;
    }
    arg->matched_token = 1;
    arg->matched = 1;
    return 1;
}

/* 尝试将输入的下一组单词与下一个参数进行匹配。
 * 若参数标记为 multiple（可重复），仅匹配一次。
 * 若下一个输入单词无法匹配则返回 0 表示失败。 */
static int matchArgOnce(char **nextword, int numwords, cliCommandArg *arg) {
    /* 首先匹配 token（若存在） */
    if (arg->token != NULL) {
        if (!matchToken(nextword, arg)) {
            return 0;
        }
        if (arg->type == ARG_TYPE_PURE_TOKEN) {
            arg->matched_all = 1;
            return 1;
        }
        if (numwords == 1) {
            return 1;
        }
        nextword++;
        numwords--;
    }

    /* 然后匹配参数剩余部分 */
    if (!matchNoTokenArg(nextword, numwords, arg)) {
        return 0;
    }
    return arg->matched;
}

/* 尝试将输入的下一组单词与下一个参数进行匹配。
 * 若参数标记为 multiple（可重复），尽可能多次匹配。 */
static int matchArg(char **nextword, int numwords, cliCommandArg *arg) {
    int matchedWords = 0;
    int matchedOnce = matchArgOnce(nextword, numwords, arg);
    if (!(arg->flags & CMD_ARG_MULTIPLE)) {
        return matchedOnce;
    }

    /* 已成功匹配一次；尽可能多次匹配 multiple 参数 */
    matchedWords += matchedOnce;
    while (arg->matched_all && matchedWords < numwords) {
        clearMatchedArgs(arg, 1);
        if (arg->token != NULL && !(arg->flags & CMD_ARG_MULTIPLE_TOKEN)) {
            /* token 仅在首次出现，后续匹配假装 token 已出现
             * 以便不再提示。 */
            matchedOnce = matchNoTokenArg(nextword + matchedWords, numwords - matchedWords, arg);
            if (arg->matched) {
                arg->matched_token = 1;
            }
        } else {
            matchedOnce = matchArgOnce(nextword + matchedWords, numwords - matchedWords, arg);
        }
        matchedWords += matchedOnce;
    }
    arg->matched_all = 0;  /* 因为还可能有更多次重复 */
    return matchedWords;
}

/* 尝试将输入的下一组单词与某一组连续可选参数中的任一项匹配 */
static int matchOneOptionalArg(char **words, int numwords, cliCommandArg *args, int numargs, int *matchedarg) {
    for (int nextword = 0, nextarg = 0; nextword != numwords && nextarg != numargs; ++nextarg) {
        if (args[nextarg].matched) {
            /* 已经匹配过该参数 */
            continue;
        }

        int matchedWords = matchArg(&words[nextword], numwords - nextword, &args[nextarg]);
        if (matchedWords != 0) {
            *matchedarg = nextarg;
            return matchedWords;
        }
    }
    return 0;
}

/* 在一组连续可选参数中尽可能多地匹配输入单词 */
static int matchOptionalArgs(char **words, int numwords, cliCommandArg *args, int numargs) {
    int nextword = 0;
    int matchedarg = -1, lastmatchedarg = -1;
    while (nextword != numwords) {
        int matchedWords = matchOneOptionalArg(&words[nextword], numwords - nextword, args, numargs, &matchedarg);
        if (matchedWords == 0) {
            break;
        }
        /* 成功匹配一个可选参数；将前一个匹配标记为已完成，
         * 避免出现部分提示。 */
        if (lastmatchedarg != -1) {
            args[lastmatchedarg].matched_all = 1;
        }
        lastmatchedarg = matchedarg;
        nextword += matchedWords;
    }
    return nextword;
}

/* 尽可能多地将输入单词与命令参数匹配 */
static int matchArgs(char **words, int numwords, cliCommandArg *args, int numargs) {
    int nextword, nextarg, matchedWords;
    for (nextword = 0, nextarg = 0; nextword != numwords && nextarg != numargs; ++nextarg) {
        /* Optional args can occur in any order. Collect a range of consecutive optional args
         * and try to match them as a group against the next input words.
         */
        if (args[nextarg].flags & CMD_ARG_OPTIONAL) {
            int lastoptional;
            for (lastoptional = nextarg; lastoptional < numargs; lastoptional++) {
                if (!(args[lastoptional].flags & CMD_ARG_OPTIONAL)) break;
            }
            matchedWords = matchOptionalArgs(&words[nextword], numwords - nextword, &args[nextarg], lastoptional - nextarg);
            nextarg = lastoptional - 1;
        } else {
            matchedWords = matchArg(&words[nextword], numwords - nextword, &args[nextarg]);
            if (matchedWords == 0) {
                /* Couldn't match a required word - matching fails! */
                return 0;
            }
        }

        nextword += matchedWords;
    }
    return nextword;
}

/* Compute the linenoise hint for the input prefix in inputargv/inputargc.
 * cmdlen is the number of words from the start of the input that make up the command.
 * If docs.args exists, dynamically creates a hint string by matching the arg specs
 * against the input words.
 */
static sds makeHint(char **inputargv, int inputargc, int cmdlen, struct commandDocs docs) {
    sds hint;

    if (docs.args) {
        /* 清空返回提示中的已匹配参数，仅显示用户尚未输入的部分 */
        clearMatchedArgs(docs.args, docs.numargs);
        hint = sdsempty();
        int matchedWords = 0;
        if (inputargv && inputargc)
            matchedWords = matchArgs(inputargv + cmdlen, inputargc - cmdlen, docs.args, docs.numargs);
        if (matchedWords == inputargc - cmdlen) {
            hint = addHintForArguments(hint, docs.args, docs.numargs, " ");
        }
        return hint;
    }

    /* 若无参数规格，在用户开始输入前直接显示提示字符串 */
    if (inputargc <= cmdlen) {
        hint = sdsnew(docs.params);
    } else {
        hint = sdsempty();
    }
    return hint;
}

/* 搜索匹配输入单词最长前缀的命令 */
static helpEntry* findHelpEntry(int argc, char **argv) {
    helpEntry *entry = NULL;
    int i, rawargc, matchlen = 0;
    sds *rawargv;

    for (i = 0; i < helpEntriesLen; i++) {
        if (!(helpEntries[i].type & CLI_HELP_COMMAND)) continue;

        rawargv = helpEntries[i].argv;
        rawargc = helpEntries[i].argc;
        if (rawargc <= argc) {
            int j;
            for (j = 0; j < rawargc; j++) {
                if (strcasecmp(rawargv[j],argv[j])) {
                    break;
                }
            }
            if (j == rawargc && rawargc > matchlen) {
                matchlen = rawargc;
                entry = &helpEntries[i];
            }
        }
    }
    return entry;
}

/* 返回给定部分输入的命令行提示字符串 */
static sds getHintForInput(const char *charinput) {
    sds hint = NULL;
    int inputargc, inputlen = strlen(charinput);
    sds *inputargv = sdssplitargs(charinput, &inputargc);
    int endspace = inputlen && isspace(charinput[inputlen-1]);

    /* 在用户输入空格之前不匹配最后一个单词 */
    int matchargc = endspace ? inputargc : inputargc - 1;

    helpEntry *entry = findHelpEntry(matchargc, inputargv);
    if (entry) {
       hint = makeHint(inputargv, matchargc, entry->argc, entry->docs);
    }
    sdsfreesplitres(inputargv, inputargc);
    return hint;
}

/* linenoise 提示回调函数 */
static char *hintsCallback(const char *buf, int *color, int *bold) {
    if (!pref.hints) return NULL;  /* 用户关闭了提示功能 */

    sds hint = getHintForInput(buf);
    if (hint == NULL) {
        return NULL;
    }

    *color = 90;   /* 灰色 */
    *bold = 0;

    /* 必要时在开头添加空格 */
    int len = strlen(buf);
    int endspace = len && isspace(buf[len-1]);
    if (!endspace) {
        sds newhint = sdsnewlen(" ",1);
        newhint = sdscatsds(newhint,hint);
        sdsfree(hint);
        hint = newhint;
    }

    return hint;
}

static void freeHintsCallback(void *ptr) {
    sdsfree(ptr);
}

/*------------------------------------------------------------------------------
 * TTY manipulation — 终端控制
 *--------------------------------------------------------------------------- */

/* 若已修改终端属性，则恢复 */
void cliRestoreTTY(void) {
    if (orig_termios_saved)
        tcsetattr(STDIN_FILENO, TCSANOW, &orig_termios);
}

/* 将终端切换到"按任意键继续"模式（关闭回显与规范模式） */
static void cliPressAnyKeyTTY(void) {
    if (!isatty(STDIN_FILENO)) return;
    if (!orig_termios_saved) {
        if (tcgetattr(STDIN_FILENO, &orig_termios) == -1) return;
        atexit(cliRestoreTTY);
        orig_termios_saved = 1;
    }
    struct termios mode = orig_termios;
    mode.c_lflag &= ~(ECHO | ICANON); /* 关闭回显、关闭规范模式 */
    tcsetattr(STDIN_FILENO, TCSANOW, &mode);
}

/*------------------------------------------------------------------------------
 * Networking / parsing — 网络与解析
 *--------------------------------------------------------------------------- */

/* 向服务器发送 AUTH 命令进行身份认证 */
static int cliAuth(redisContext *ctx, char *user, char *auth) {
    redisReply *reply;
    if (auth == NULL) return REDIS_OK;  /* 无密码直接返回成功 */

    if (user == NULL)
        reply = redisCommand(ctx,"AUTH %s",auth);
    else
        reply = redisCommand(ctx,"AUTH %s %s",user,auth);

    if (reply == NULL) {
        fprintf(stderr, "\nI/O error\n");
        return REDIS_ERR;
    }

    int result = REDIS_OK;
    if (reply->type == REDIS_REPLY_ERROR) {
        result = REDIS_ERR;
        fprintf(stderr, "AUTH failed: %s\n", reply->str);
    }
    freeReplyObject(reply);
    return result;
}

/* 向服务器发送 SELECT input_dbnum 切换数据库 */
static int cliSelect(void) {
    redisReply *reply;
    if (config.conn_info.input_dbnum == config.dbnum) return REDIS_OK;

    reply = redisCommand(context,"SELECT %d",config.conn_info.input_dbnum);
    if (reply == NULL) {
        fprintf(stderr, "\nI/O error\n");
        return REDIS_ERR;
    }

    int result = REDIS_OK;
    if (reply->type == REDIS_REPLY_ERROR) {
        result = REDIS_ERR;
        fprintf(stderr,"SELECT %d failed: %s\n",config.conn_info.input_dbnum,reply->str);
    } else {
        config.dbnum = config.conn_info.input_dbnum;
        cliRefreshPrompt();
    }
    freeReplyObject(reply);
    return result;
}

/* 若 redis-cli 启动时指定了 -3 选项，则切换到 RESP3 协议 */
static int cliSwitchProto(void) {
    redisReply *reply;
    if (!config.resp3 || config.resp2) return REDIS_OK;

    reply = redisCommand(context,"HELLO 3");
    if (reply == NULL) {
        fprintf(stderr, "\nI/O error\n");
        return REDIS_ERR;
    }

    int result = REDIS_OK;
    if (reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr,"HELLO 3 failed: %s\n",reply->str);
        if (config.resp3 == 1) {
            result = REDIS_ERR;   /* 显式 -3，要求必须成功 */
        } else if (config.resp3 == 2) {
            result = REDIS_OK;    /* 隐式（如 --json），允许失败 */
        }
    }

    /* 获取服务器版本字符串以备后用 */
    for (size_t i = 0; i < reply->elements; i += 2) {
        assert(reply->element[i]->type == REDIS_REPLY_STRING);
        char *key = reply->element[i]->str;
        if (!strcmp(key, "version")) {
            assert(reply->element[i + 1]->type == REDIS_REPLY_STRING);
            config.server_version = sdsnew(reply->element[i + 1]->str);
        }
    }
    freeReplyObject(reply);
    config.current_resp3 = 1;
    return result;
}

/* 连接到 Redis 服务器。可通过以下标志控制行为：
 *      CC_FORCE : 即使已存在连接也强制重连。
 *      CC_QUIET : 连接失败时不打印错误信息。 */
static int cliConnect(int flags) {
    if (context == NULL || flags & CC_FORCE) {
        if (context != NULL) {
            redisFree(context);
            config.dbnum = 0;
            config.in_multi = 0;
            config.pubsub_mode = 0;
            cliRefreshPrompt();
        }

        /* 在集群模式下被重定向后不再使用 Unix 套接字 */
        if (config.hostsocket == NULL ||
            (config.cluster_mode && config.cluster_reissue_command)) {
            context = redisConnectWrapper(config.conn_info.hostip, config.conn_info.hostport,
                                          config.connect_timeout);
        } else {
            context = redisConnectUnixWrapper(config.hostsocket, config.connect_timeout);
        }

        /* 启用 TLS 时进行安全握手 */
        if (!context->err && config.tls) {
            const char *err = NULL;
            if (cliSecureConnection(context, config.sslconfig, &err) == REDIS_ERR && err) {
                fprintf(stderr, "Could not negotiate a TLS connection: %s\n", err);
                redisFree(context);
                context = NULL;
                return REDIS_ERR;
            }
        }

        if (context->err) {
            if (!(flags & CC_QUIET)) {
                fprintf(stderr,"Could not connect to Redis at ");
                if (config.hostsocket == NULL ||
                    (config.cluster_mode && config.cluster_reissue_command))
                {
                    fprintf(stderr, "%s:%d: %s\n",
                        config.conn_info.hostip,config.conn_info.hostport,context->errstr);
                } else {
                    fprintf(stderr,"%s: %s\n",
                        config.hostsocket,context->errstr);
                }
            }
            redisFree(context);
            context = NULL;
            return REDIS_ERR;
        }


        /* 在 Redis 上下文的套接字上启用激进的 KEEP_ALIVE 选项，
         * 以避免执行长命令时出现超时，同时也有助于更及时发现真实错误。 */
        anetKeepAlive(NULL, context->fd, REDIS_CLI_KEEPALIVE_INTERVAL);

        /* 当前连接状态 */
        config.current_resp3 = 0;

        /* 执行 AUTH、选择数据库、视需要切换到 RESP3 协议 */
        if (cliAuth(context, config.conn_info.user, config.conn_info.auth) != REDIS_OK)
            return REDIS_ERR;
        if (cliSelect() != REDIS_OK)
            return REDIS_ERR;
        if (cliSwitchProto() != REDIS_OK)
            return REDIS_ERR;
    }

    /* 若需要则注册 PUSH 消息回调 */
    if (config.push_output) {
        redisSetPushCallback(context, cliPushHandler);
    }

    return REDIS_OK;
}

/* 在集群模式下，若服务器返回 ASK 错误需要重定向到另一节点，
 * 在发送真实命令前需要先发送 ASKING 命令。 */
static int cliSendAsking(void) {
    redisReply *reply;

    config.cluster_send_asking = 0;
    if (context == NULL) {
        return REDIS_ERR;
    }
    reply = redisCommand(context,"ASKING");
    if (reply == NULL) {
        fprintf(stderr, "\nI/O error\n");
        return REDIS_ERR;
    }
    int result = REDIS_OK;
    if (reply->type == REDIS_REPLY_ERROR) {
        result = REDIS_ERR;
        fprintf(stderr,"ASKING failed: %s\n",reply->str);
    }
    freeReplyObject(reply);
    return result;
}

/* 打印 Redis 上下文的错误信息 */
static void cliPrintContextError(void) {
    if (context == NULL) return;
    fprintf(stderr,"Error: %s\n",context->errstr);
}

/* 判断 reply 是否为 RESP3 的 invalidate 推送消息 */
static int isInvalidateReply(redisReply *reply) {
    return reply->type == REDIS_REPLY_PUSH && reply->elements == 2 &&
        reply->element[0]->type == REDIS_REPLY_STRING &&
        !strncmp(reply->element[0]->str, "invalidate", 10) &&
        reply->element[1]->type == REDIS_REPLY_ARRAY;
}

/* RESP3 'invalidate' 消息的专用显示处理器。
 * 本函数不对 reply 做合法性校验，因此调用前应已经确认其正确性。 */
static sds cliFormatInvalidateTTY(redisReply *r) {
    sds out = sdsnew("-> invalidate: ");

    for (size_t i = 0; i < r->element[1]->elements; i++) {
        redisReply *key = r->element[1]->element[i];
        assert(key->type == REDIS_REPLY_STRING);

        out = sdscatfmt(out, "'%s'", key->str, key->len);
        if (i < r->element[1]->elements - 1)
            out = sdscatlen(out, ", ", 2);
    }

    return sdscatlen(out, "\n", 1);
}

/* 当 cliFormatReplyTTY 以多行形式渲染 reply 时返回非零值 */
static int cliIsMultilineValueTTY(redisReply *r) {
    switch (r->type) {
    case REDIS_REPLY_ARRAY:
    case REDIS_REPLY_SET:
    case REDIS_REPLY_PUSH:
        if (r->elements == 0) return 0;
        if (r->elements > 1) return 1;
        return cliIsMultilineValueTTY(r->element[0]);
    case REDIS_REPLY_MAP:
        if (r->elements == 0) return 0;
        if (r->elements > 2) return 1;
        return cliIsMultilineValueTTY(r->element[1]);
    default:
        return 0;
    }
}

/* 将 reply 格式化为人类可读的字符串（TTY 友好的多行输出） */
static sds cliFormatReplyTTY(redisReply *r, char *prefix) {
    sds out = sdsempty();
    switch (r->type) {
    case REDIS_REPLY_ERROR:
        /* 错误响应 */
        out = sdscatprintf(out,"(error) %s\n", r->str);
    break;
    case REDIS_REPLY_STATUS:
        /* 状态响应 */
        out = sdscat(out,r->str);
        out = sdscat(out,"\n");
    break;
    case REDIS_REPLY_INTEGER:
        /* 整数响应 */
        out = sdscatprintf(out,"(integer) %lld\n",r->integer);
    break;
    case REDIS_REPLY_DOUBLE:
        /* 双精度浮点响应 */
        out = sdscatprintf(out,"(double) %s\n",r->str);
    break;
    case REDIS_REPLY_STRING:
    case REDIS_REPLY_VERB:
        /* 为标准输出生成内容时希望带有引号等丰富展示，
         * verbatim（逐字）字符串类型除外。 */
        if (r->type == REDIS_REPLY_STRING) {
            out = sdscatrepr(out,r->str,r->len);
            out = sdscat(out,"\n");
        } else {
            out = sdscatlen(out,r->str,r->len);
            out = sdscat(out,"\n");
        }
    break;
    case REDIS_REPLY_NIL:
        /* 空响应 */
        out = sdscat(out,"(nil)\n");
    break;
    case REDIS_REPLY_BOOL:
        /* 布尔响应 */
        out = sdscat(out,r->integer ? "(true)\n" : "(false)\n");
    break;
    case REDIS_REPLY_ARRAY:
    case REDIS_REPLY_MAP:
    case REDIS_REPLY_SET:
    case REDIS_REPLY_PUSH:
        /* 聚合类型：数组/映射/集合/推送 */
        if (r->elements == 0) {
            if (r->type == REDIS_REPLY_ARRAY)
                out = sdscat(out,"(empty array)\n");
            else if (r->type == REDIS_REPLY_MAP)
                out = sdscat(out,"(empty hash)\n");
            else if (r->type == REDIS_REPLY_SET)
                out = sdscat(out,"(empty set)\n");
            else if (r->type == REDIS_REPLY_PUSH)
                out = sdscat(out,"(empty push)\n");
            else
                out = sdscat(out,"(empty aggregate type)\n");
        } else {
            unsigned int i, idxlen = 0;
            char _prefixlen[16];
            char _prefixfmt[16];
            sds _prefix;
            sds tmp;

            /* 计算表示最大索引所需的字符数 */
            i = r->elements;
            if (r->type == REDIS_REPLY_MAP) i /= 2;
            do {
                idxlen++;
                i /= 10;
            } while(i);

            /* 嵌套多层 bulk 的前缀应随 idxlen+2 个空格增长 */
            memset(_prefixlen,' ',idxlen+2);
            _prefixlen[idxlen+2] = '\0';
            _prefix = sdscat(sdsnew(prefix),_prefixlen);

            /* 设置每条条目的前缀格式 */
            char numsep;
            if (r->type == REDIS_REPLY_SET) numsep = '~';
            else if (r->type == REDIS_REPLY_MAP) numsep = '#';
            /* TODO: 此处将是脚本的破坏性变更，需在主版本中处理 */
            /* else if (r->type == REDIS_REPLY_PUSH) numsep = '>'; */
            else numsep = ')';
            snprintf(_prefixfmt,sizeof(_prefixfmt),"%%s%%%ud%c ",idxlen,numsep);

            for (i = 0; i < r->elements; i++) {
                unsigned int human_idx = (r->type == REDIS_REPLY_MAP) ?
                                         i/2 : i;
                human_idx++; /* 改为 1-based 索引 */

                /* 第一个元素不使用前缀，因为父调用方已添加索引号 */
                out = sdscatprintf(out,_prefixfmt,i == 0 ? "" : prefix,human_idx);

                /* 格式化 multi bulk 条目 */
                tmp = cliFormatReplyTTY(r->element[i],_prefix);
                out = sdscatlen(out,tmp,sdslen(tmp));
                sdsfree(tmp);

                /* map 类型还要格式化 value */
                if (r->type == REDIS_REPLY_MAP) {
                    i++;
                    sdsrange(out,0,-2);
                    out = sdscat(out," => ");
                    if (cliIsMultilineValueTTY(r->element[i])) {
                        /* 多行 value 之前换行以对齐 */
                        out = sdscat(out, "\n");
                        out = sdscat(out, _prefix);
                    }
                    tmp = cliFormatReplyTTY(r->element[i],_prefix);
                    out = sdscatlen(out,tmp,sdslen(tmp));
                    sdsfree(tmp);
                }
            }
            sdsfree(_prefix);
        }
    break;
    default:
        fprintf(stderr,"Unknown reply type: %d\n", r->type);
        exit(1);
    }
    return out;
}

/* 若 reply 是 pubsub 推送消息则返回 1 */
int isPubsubPush(redisReply *r) {
    if (r == NULL ||
        r->type != (config.current_resp3 ? REDIS_REPLY_PUSH : REDIS_REPLY_ARRAY) ||
        r->elements < 3 ||
        r->element[0]->type != REDIS_REPLY_STRING)
    {
        return 0;
    }
    char *str = r->element[0]->str;
    size_t len = r->element[0]->len;
    /* 判断是否为 [p|s][un]subscribe 或 [p|s]message，
     * 但更简单的方式是判断它是否以 "message" 或 "subscribe" 结尾。 */
    return ((len >= strlen("message") &&
             !strcmp(str + len - strlen("message"), "message")) ||
            (len >= strlen("subscribe") &&
             !strcmp(str + len - strlen("subscribe"), "subscribe")));
}

/* 判断当前终端是否支持 ANSI 颜色 */
int isColorTerm(void) {
    char *t = getenv("TERM");
    return t != NULL && strstr(t,"xterm") != NULL;
}

/* sdsCatColorizedLdbReply() 的辅助函数：给 SDS 字符串追加带颜色片段 */
sds sdscatcolor(sds o, char *s, size_t len, char *color) {
    if (!isColorTerm()) return sdscatlen(o,s,len);  /* 不支持颜色则原样输出 */

    int bold = strstr(color,"bold") != NULL;
    int ccode = 37; /* 默认白色 */
    if (strstr(color,"red")) ccode = 31;
    else if (strstr(color,"green")) ccode = 32;
    else if (strstr(color,"yellow")) ccode = 33;
    else if (strstr(color,"blue")) ccode = 34;
    else if (strstr(color,"magenta")) ccode = 35;
    else if (strstr(color,"cyan")) ccode = 36;
    else if (strstr(color,"white")) ccode = 37;

    o = sdscatfmt(o,"\033[%i;%i;49m",bold,ccode);
    o = sdscatlen(o,s,len);
    o = sdscat(o,"\033[0m");
    return o;
}

/* 根据 Lua 调试器回复的前缀为其上色 */
sds sdsCatColorizedLdbReply(sds o, char *s, size_t len) {
    char *color = "white";

    if (strstr(s,"<debug>")) color = "bold";
    if (strstr(s,"<redis>")) color = "green";
    if (strstr(s,"<reply>")) color = "cyan";
    if (strstr(s,"<error>")) color = "red";
    if (strstr(s,"<hint>")) color = "bold";
    if (strstr(s,"<value>") || strstr(s,"<retval>")) color = "magenta";
    if (len > 4 && isdigit(s[3])) {
        if (s[1] == '>') color = "yellow"; /* 当前行 */
        else if (s[2] == '#') color = "bold"; /* 断点 */
    }
    return sdscatcolor(o,s,len,color);
}

/* 将 reply 格式化为最简原始字符串 */
static sds cliFormatReplyRaw(redisReply *r) {
    sds out = sdsempty(), tmp;
    size_t i;

    switch (r->type) {
    case REDIS_REPLY_NIL:
        /* 空响应：无输出 */
        break;
    case REDIS_REPLY_ERROR:
        out = sdscatlen(out,r->str,r->len);
        out = sdscatlen(out,"\n",1);
        break;
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_STRING:
    case REDIS_REPLY_VERB:
        if (r->type == REDIS_REPLY_STATUS && config.eval_ldb) {
            /* Lua 调试器以简单 status 字符串数组回复，
             * 调试会话中为其染色以增强可读性。 */

            /* 检测调试会话是否结束 */
            if (strstr(r->str,"<endsession>") == r->str) {
                config.enable_ldb_on_eval = 0;
                config.eval_ldb = 0;
                config.eval_ldb_end = 1; /* 通知调用方会话结束 */
                config.output = OUTPUT_STANDARD;
                cliRefreshPrompt();
            } else {
                out = sdsCatColorizedLdbReply(out,r->str,r->len);
            }
        } else {
            out = sdscatlen(out,r->str,r->len);
        }
        break;
    case REDIS_REPLY_BOOL:
        out = sdscat(out,r->integer ? "(true)" : "(false)");
    break;
    case REDIS_REPLY_INTEGER:
        out = sdscatprintf(out,"%lld",r->integer);
        break;
    case REDIS_REPLY_DOUBLE:
        out = sdscatprintf(out,"%s",r->str);
        break;
    case REDIS_REPLY_SET:
    case REDIS_REPLY_ARRAY:
    case REDIS_REPLY_PUSH:
        for (i = 0; i < r->elements; i++) {
            if (i > 0) out = sdscat(out,config.mb_delim);
            tmp = cliFormatReplyRaw(r->element[i]);
            out = sdscatlen(out,tmp,sdslen(tmp));
            sdsfree(tmp);
        }
        break;
    case REDIS_REPLY_MAP:
        for (i = 0; i < r->elements; i += 2) {
            if (i > 0) out = sdscat(out,config.mb_delim);
            tmp = cliFormatReplyRaw(r->element[i]);
            out = sdscatlen(out,tmp,sdslen(tmp));
            sdsfree(tmp);

            out = sdscatlen(out," ",1);
            tmp = cliFormatReplyRaw(r->element[i+1]);
            out = sdscatlen(out,tmp,sdslen(tmp));
            sdsfree(tmp);
        }
        break;
    default:
        fprintf(stderr,"Unknown reply type: %d\n", r->type);
        exit(1);
    }
    return out;
}

/* 将 reply 格式化为 CSV 字符串 */
static sds cliFormatReplyCSV(redisReply *r) {
    unsigned int i;

    sds out = sdsempty();
    switch (r->type) {
    case REDIS_REPLY_ERROR:
        out = sdscat(out,"ERROR,");
        out = sdscatrepr(out,r->str,strlen(r->str));
    break;
    case REDIS_REPLY_STATUS:
        out = sdscatrepr(out,r->str,r->len);
    break;
    case REDIS_REPLY_INTEGER:
        out = sdscatprintf(out,"%lld",r->integer);
    break;
    case REDIS_REPLY_DOUBLE:
        out = sdscatprintf(out,"%s",r->str);
        break;
    case REDIS_REPLY_STRING:
    case REDIS_REPLY_VERB:
        out = sdscatrepr(out,r->str,r->len);
    break;
    case REDIS_REPLY_NIL:
        out = sdscat(out,"NULL");
    break;
    case REDIS_REPLY_BOOL:
        out = sdscat(out,r->integer ? "true" : "false");
    break;
    case REDIS_REPLY_ARRAY:
    case REDIS_REPLY_SET:
    case REDIS_REPLY_PUSH:
    case REDIS_REPLY_MAP: /* CSV 无 map 类型，按扁平列表输出 */
        for (i = 0; i < r->elements; i++) {
            sds tmp = cliFormatReplyCSV(r->element[i]);
            out = sdscatlen(out,tmp,sdslen(tmp));
            if (i != r->elements-1) out = sdscat(out,",");
            sdsfree(tmp);
        }
    break;
    default:
        fprintf(stderr,"Unknown reply type: %d\n", r->type);
        exit(1);
    }
    return out;
}

/* 将指定缓冲区按所需 JSON 输出模式追加到 out 并返回 */
static sds jsonStringOutput(sds out, const char *p, int len, int mode) {
    if (mode == OUTPUT_JSON) {
        return escapeJsonString(out, p, len);
    } else if (mode == OUTPUT_QUOTED_JSON) {
        /* 需要对反斜杠进行双重转义 */
        sds tmp = sdscatrepr(sdsempty(), p, len);
        int tmplen = sdslen(tmp);
        char *n = tmp;
        while (tmplen--) {
            if (*n == '\\') out = sdscatlen(out, "\\\\", 2);
            else out = sdscatlen(out, n, 1);
            n++;
        }

        sdsfree(tmp);
        return out;
    } else {
        assert(0);
    }
}

/* 将 reply 格式化为 JSON 字符串 */
static sds cliFormatReplyJson(sds out, redisReply *r, int mode) {
    unsigned int i;

    switch (r->type) {
    case REDIS_REPLY_ERROR:
        out = sdscat(out,"error:");
        out = jsonStringOutput(out,r->str,strlen(r->str),mode);
        break;
    case REDIS_REPLY_STATUS:
        out = jsonStringOutput(out,r->str,r->len,mode);
        break;
    case REDIS_REPLY_INTEGER:
        out = sdscatprintf(out,"%lld",r->integer);
        break;
    case REDIS_REPLY_DOUBLE:
        out = sdscatprintf(out,"%s",r->str);
        break;
    case REDIS_REPLY_STRING:
    case REDIS_REPLY_VERB:
        out = jsonStringOutput(out,r->str,r->len,mode);
        break;
    case REDIS_REPLY_NIL:
        out = sdscat(out,"null");
        break;
    case REDIS_REPLY_BOOL:
        out = sdscat(out,r->integer ? "true" : "false");
        break;
    case REDIS_REPLY_ARRAY:
    case REDIS_REPLY_SET:
    case REDIS_REPLY_PUSH:
        out = sdscat(out,"[");
        for (i = 0; i < r->elements; i++ ) {
            out = cliFormatReplyJson(out,r->element[i],mode);
            if (i != r->elements-1) out = sdscat(out,",");
        }
        out = sdscat(out,"]");
        break;
    case REDIS_REPLY_MAP:
        out = sdscat(out,"{");
        for (i = 0; i < r->elements; i += 2) {
            redisReply *key = r->element[i];
            if (key->type == REDIS_REPLY_ERROR ||
                key->type == REDIS_REPLY_STATUS ||
                key->type == REDIS_REPLY_STRING ||
                key->type == REDIS_REPLY_VERB)
            {
                out = cliFormatReplyJson(out,key,mode);
            } else {
                /* 根据 JSON 规范，map 的 key 必须是字符串，
                 * 但 RESP3 中 key 可以是其他类型。
                 * 第一个 cliFormatReplyJson 调用将非字符串类型转为字符串，
                 * 第二个 escapeJsonString 调用对转换后的字符串进行转义。 */
                sds keystr = cliFormatReplyJson(sdsempty(),key,mode);
                if (keystr[0] == '"') out = sdscatsds(out,keystr);
                else out = sdscatfmt(out,"\"%S\"",keystr);
                sdsfree(keystr);
            }
            out = sdscat(out,":");

            out = cliFormatReplyJson(out,r->element[i+1],mode);
            if (i != r->elements-2) out = sdscat(out,",");
        }
        out = sdscat(out,"}");
        break;
    default:
        fprintf(stderr,"Unknown reply type: %d\n", r->type);
        exit(1);
    }
    return out;
}

/* 根据不同输出模式生成回复字符串 */
static sds cliFormatReply(redisReply *reply, int mode, int verbatim) {
    sds out;

    if (verbatim) {
        out = cliFormatReplyRaw(reply);
    }  else if (mode == OUTPUT_STANDARD) {
        out = cliFormatReplyTTY(reply, "");
    } else if (mode == OUTPUT_RAW) {
        out = cliFormatReplyRaw(reply);
        out = sdscatsds(out, config.cmd_delim);
    } else if (mode == OUTPUT_CSV) {
        out = cliFormatReplyCSV(reply);
        out = sdscatlen(out, "\n", 1);
    } else if (mode == OUTPUT_JSON || mode == OUTPUT_QUOTED_JSON) {
        out = cliFormatReplyJson(sdsempty(), reply, mode);
        out = sdscatlen(out, "\n", 1);
    } else {
        fprintf(stderr, "Error:  Unknown output encoding %d\n", mode);
        exit(1);
    }

    return out;
}

/* 输出任何接收到的自发性 PUSH 回复 */
static void cliPushHandler(void *privdata, void *reply) {
    UNUSED(privdata);
    sds out;

    if (config.output == OUTPUT_STANDARD && isInvalidateReply(reply)) {
        out = cliFormatInvalidateTTY(reply);
    } else {
        out = cliFormatReply(reply, config.output, 0);
    }

    fwrite(out, sdslen(out), 1, stdout);

    freeReplyObject(reply);
    sdsfree(out);
}

/* 读取服务器回复，并按当前输出模式打印 */
static int cliReadReply(int output_raw_strings) {
    void *_reply;
    redisReply *reply;
    sds out = NULL;
    int output = 1;

    if (config.last_reply) {
        freeReplyObject(config.last_reply);
        config.last_reply = NULL;
    }

    if (redisGetReply(context,&_reply) != REDIS_OK) {
        if (config.blocking_state_aborted) {
            /* Ctrl-C 中断了阻塞状态 */
            config.blocking_state_aborted = 0;
            config.monitor_mode = 0;
            config.pubsub_mode = 0;
            return cliConnect(CC_FORCE);
        }

        if (config.shutdown) {
            redisFree(context);
            context = NULL;
            return REDIS_OK;
        }
        if (config.interactive) {
            /* 过滤需要重连的情况 */
            if (context->err == REDIS_ERR_IO &&
                (errno == ECONNRESET || errno == EPIPE))
                return REDIS_ERR;
            if (context->err == REDIS_ERR_EOF)
                return REDIS_ERR;
        }
        cliPrintContextError();
        exit(1);
        return REDIS_ERR; /* 避免编译器警告 */
    }

    config.last_reply = reply = (redisReply*)_reply;

    config.last_cmd_type = reply->type;

    /* 检查是否需要连接到其他节点并重新提交请求 */
    if (config.cluster_mode && reply->type == REDIS_REPLY_ERROR &&
        (!strncmp(reply->str,"MOVED ",6) || !strncmp(reply->str,"ASK ",4)))
    {
        char *p = reply->str, *s;
        int slot;

        output = 0;
        /* 注释以指针位置标识：
         *
         * [S] 表示指针 s 的位置
         * [P] 表示指针 p 的位置
         */
        s = strchr(p,' ');      /* MOVED[S]3999 127.0.0.1:6381 */
        p = strchr(s+1,' ');    /* MOVED[S]3999[P]127.0.0.1:6381 */
        *p = '\0';
        slot = atoi(s+1);
        s = strrchr(p+1,':');    /* MOVED 3999[P]127.0.0.1[S]6381 */
        *s = '\0';
        if (p+1 != s) {
            /* 主机名可能为空（如端点类型未知时的 'MOVED 3999 :6381'），
             * 仅在主机名非空时更新。 */
            sdsfree(config.conn_info.hostip);
            config.conn_info.hostip = sdsnew(p+1);
        }
        config.conn_info.hostport = atoi(s+1);
        if (config.interactive)
            printf("-> Redirected to slot [%d] located at %s:%d\n",
                slot, config.conn_info.hostip, config.conn_info.hostport);
        config.cluster_reissue_command = 1;
        if (!strncmp(reply->str,"ASK ",4)) {
            config.cluster_send_asking = 1;
        }
        cliRefreshPrompt();
    } else if (!config.interactive && config.set_errcode &&
        reply->type == REDIS_REPLY_ERROR)
    {
        fprintf(stderr,"%s\n",reply->str);
        exit(1);
        return REDIS_ERR; /* 避免编译器警告 */
    }

    if (output) {
        out = cliFormatReply(reply, config.output, output_raw_strings);
        fwrite(out,sdslen(out),1,stdout);
        fflush(stdout);
        sdsfree(out);
    }
    return REDIS_OK;
}

/* 同时等待 Redis pubsub 消息和标准输入 */
static void cliWaitForMessagesOrStdin(void) {
    int show_info = config.output != OUTPUT_RAW && (isatty(STDOUT_FILENO) ||
                                                    getenv("FAKETTY"));
    int use_color = show_info && isColorTerm();
    cliPressAnyKeyTTY();
    while (config.pubsub_mode) {
        /* 首先检查是否有已缓冲的回复 */
        redisReply *reply;
        do {
            if (redisGetReplyFromReader(context, (void **)&reply) != REDIS_OK) {
                cliPrintContextError();
                exit(1);
            }
            if (reply) {
                sds out = cliFormatReply(reply, config.output, 0);
                fwrite(out,sdslen(out),1,stdout);
                fflush(stdout);
                sdsfree(out);
            }
        } while(reply);

        /* 等待来自 Redis 套接字或标准输入的输入 */
        struct timeval tv;
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(context->fd, &readfds);
        FD_SET(STDIN_FILENO, &readfds);
        tv.tv_sec = 5;
        tv.tv_usec = 0;
        if (show_info) {
            if (use_color) printf("\033[1;90m"); /* 加粗、亮色 */
            printf("Reading messages... (press Ctrl-C to quit or any key to type command)\r");
            if (use_color) printf("\033[0m"); /* 复位颜色 */
            fflush(stdout);
        }
        select(context->fd + 1, &readfds, NULL, NULL, &tv);
        if (show_info) {
            printf("\033[K"); /* 清除当前行 */
            fflush(stdout);
        }
        if (config.blocking_state_aborted) {
            /* 用户按下了 Ctrl-C */
            config.blocking_state_aborted = 0;
            config.pubsub_mode = 0;
            if (cliConnect(CC_FORCE) != REDIS_OK) {
                cliPrintContextError();
                exit(1);
            }
            break;
        } else if (FD_ISSET(context->fd, &readfds)) {
            /* 来自 Redis 的消息 */
            if (cliReadReply(0) != REDIS_OK) {
                cliPrintContextError();
                exit(1);
            }
            fflush(stdout);
        } else if (FD_ISSET(STDIN_FILENO, &readfds)) {
            /* 用户按下了任意键 */
            break;
        }
    }
    cliRestoreTTY();
}

/* 向服务器发送 Redis 命令。repeat 参数指定重复执行的次数。 */
static int cliSendCommand(int argc, char **argv, long repeat) {
    char *command = argv[0];
    size_t *argvlen;
    int j, output_raw;

    if (context == NULL) return REDIS_ERR;

    /* 识别输出格式应为原始格式的命令 */
    output_raw = 0;
    if (!strcasecmp(command,"info") ||
        !strcasecmp(command,"lolwut") ||
        (argc >= 2 && !strcasecmp(command,"debug") &&
                       !strcasecmp(argv[1],"htstats")) ||
        (argc >= 2 && !strcasecmp(command,"debug") &&
                       !strcasecmp(argv[1],"htstats-key")) ||
        (argc >= 2 && !strcasecmp(command,"debug") &&
                       !strcasecmp(argv[1],"client-eviction")) ||
        (argc >= 2 && !strcasecmp(command,"memory") &&
                      (!strcasecmp(argv[1],"malloc-stats") ||
                       !strcasecmp(argv[1],"doctor"))) ||
        (argc == 2 && !strcasecmp(command,"cluster") &&
                      (!strcasecmp(argv[1],"nodes") ||
                       !strcasecmp(argv[1],"info"))) ||
        (argc >= 2 && !strcasecmp(command,"client") &&
                       (!strcasecmp(argv[1],"list") ||
                        !strcasecmp(argv[1],"info"))) ||
        (argc == 3 && !strcasecmp(command,"latency") &&
                       !strcasecmp(argv[1],"graph")) ||
        (argc == 2 && !strcasecmp(command,"latency") &&
                       !strcasecmp(argv[1],"doctor")) ||
        /* 为 Redis Cluster Proxy 格式化 PROXY INFO 命令：
         * https://github.com/artix75/redis-cluster-proxy */
        (argc >= 2 && !strcasecmp(command,"proxy") &&
                       !strcasecmp(argv[1],"info")))
    {
        output_raw = 1;
    }

    /* 根据命令名设置相应的运行模式 */
    if (!strcasecmp(command,"shutdown")) config.shutdown = 1;
    if (!strcasecmp(command,"monitor")) config.monitor_mode = 1;
    int is_subscribe = (!strcasecmp(command, "subscribe") ||
                        !strcasecmp(command, "psubscribe") ||
                        !strcasecmp(command, "ssubscribe"));
    int is_unsubscribe = (!strcasecmp(command, "unsubscribe") ||
                          !strcasecmp(command, "punsubscribe") ||
                          !strcasecmp(command, "sunsubscribe"));
    if (!strcasecmp(command,"sync") ||
        !strcasecmp(command,"psync")) config.slave_mode = 1;

    /* 当用户手动调用 SCRIPT DEBUG 时，按需在下一次 EVAL
     * 启用调试模式。 */
    if (argc == 3 && !strcasecmp(argv[0],"script") &&
                     !strcasecmp(argv[1],"debug"))
    {
        if (!strcasecmp(argv[2],"yes") || !strcasecmp(argv[2],"sync")) {
            config.enable_ldb_on_eval = 1;
        } else {
            config.enable_ldb_on_eval = 0;
        }
    }

    /* 如有需要则在 EVAL 上真正激活 LDB */
    if (!strcasecmp(command,"eval") && config.enable_ldb_on_eval) {
        config.eval_ldb = 1;
        config.output = OUTPUT_RAW;
    }

    /* 设置各参数长度 */
    argvlen = zmalloc(argc*sizeof(size_t));
    for (j = 0; j < argc; j++)
        argvlen[j] = sdslen(argv[j]);

    /* 允许负的 repeat，表示无限循环，
     * 与 -i 选项配合工作良好。 */
    while(repeat < 0 || repeat-- > 0) {
        redisAppendCommandArgv(context,argc,(const char**)argv,argvlen);

        if (config.monitor_mode) {
            do {
                if (cliReadReply(output_raw) != REDIS_OK) {
                    cliPrintContextError();
                    exit(1);
                }
                fflush(stdout);

                /* 当 MONITOR 命令返回错误时退出循环 */
                if (config.last_cmd_type == REDIS_REPLY_ERROR)
                    config.monitor_mode = 0;
            } while(config.monitor_mode);
            zfree(argvlen);
            return REDIS_OK;
        }

        int num_expected_pubsub_push = 0;
        if (is_subscribe || is_unsubscribe) {
            /* 当设置了 push 回调时，redisGetReply（hiredis）会循环读取
             * 直到收到 in-band 消息；但这些命令仅通过 push 回复确认。
             * 若指定了 channel，每个 channel 有一个 push 回复；
             * 否则至少有一个。 */
            num_expected_pubsub_push = argc > 1 ? argc - 1 : 1;
            /* 临时取消默认 PUSH 回调以兼容 RESP2/RESP3 */
            redisSetPushCallback(context, NULL);
        }

        if (config.slave_mode) {
            printf("Entering replica output mode...  (press Ctrl-C to quit)\n");
            slaveMode(0);
            config.slave_mode = 0;
            zfree(argvlen);
            return REDIS_ERR;  /* 返回错误表示 slaveMode 与主节点断开 */
        }

        /* 读取回复，必要时跳过 pubsub/push 消息 */
        while (1) {
            if (cliReadReply(output_raw) != REDIS_OK) {
                zfree(argvlen);
                return REDIS_ERR;
            }
            fflush(stdout);
            if (config.pubsub_mode || num_expected_pubsub_push > 0) {
                if (isPubsubPush(config.last_reply)) {
                    if (num_expected_pubsub_push > 0 &&
                        !strcasecmp(config.last_reply->element[0]->str, command))
                    {
                        /* 此推送消息确认了 [p|s][un]subscribe 命令 */
                        if (is_subscribe && !config.pubsub_mode) {
                            config.pubsub_mode = 1;
                            cliRefreshPrompt();
                        }
                        if (--num_expected_pubsub_push > 0) {
                            continue; /* 还需要更多确认消息 */
                        }
                    } else {
                        continue; /* 跳过该 pubsub 消息 */
                    }
                } else if (config.last_reply->type == REDIS_REPLY_PUSH) {
                    continue; /* 跳过其他推送消息 */
                }
            }

            /* SELECT 成功执行时记录新的数据库编号 */
            if (!strcasecmp(command,"select") && argc == 2 &&
                config.last_cmd_type != REDIS_REPLY_ERROR)
            {
                config.conn_info.input_dbnum = config.dbnum = atoi(argv[1]);
                cliRefreshPrompt();
            } else if (!strcasecmp(command,"auth") && (argc == 2 || argc == 3)) {
                cliSelect();
            } else if (!strcasecmp(command,"multi") && argc == 1 &&
                config.last_cmd_type != REDIS_REPLY_ERROR)
            {
                /* 进入 MULTI 事务状态 */
                config.in_multi = 1;
                config.pre_multi_dbnum = config.dbnum;
                cliRefreshPrompt();
            } else if (!strcasecmp(command,"exec") && argc == 1 && config.in_multi) {
                /* 结束事务 */
                config.in_multi = 0;
                if (config.last_cmd_type == REDIS_REPLY_ERROR ||
                    config.last_cmd_type == REDIS_REPLY_NIL)
                {
                    /* 事务被丢弃，恢复事务前的数据库 */
                    config.conn_info.input_dbnum = config.dbnum = config.pre_multi_dbnum;
                }
                cliRefreshPrompt();
            } else if (!strcasecmp(command,"discard") && argc == 1 &&
                config.last_cmd_type != REDIS_REPLY_ERROR)
            {
                /* 显式 DISCARD */
                config.in_multi = 0;
                config.conn_info.input_dbnum = config.dbnum = config.pre_multi_dbnum;
                cliRefreshPrompt();
            } else if (!strcasecmp(command,"reset") && argc == 1 &&
                                     config.last_cmd_type != REDIS_REPLY_ERROR) {
                /* RESET 命令会重置连接状态 */
                config.in_multi = 0;
                config.dbnum = 0;
                config.conn_info.input_dbnum = 0;
                config.current_resp3 = 0;
                if (config.pubsub_mode && config.push_output) {
                    redisSetPushCallback(context, cliPushHandler);
                }
                config.pubsub_mode = 0;
                cliRefreshPrompt();
            } else if (!strcasecmp(command,"hello")) {
                /* HELLO 命令可能在 RESP2 与 RESP3 之间切换 */
                if (config.last_cmd_type == REDIS_REPLY_MAP) {
                    config.current_resp3 = 1;
                } else if (config.last_cmd_type == REDIS_REPLY_ARRAY) {
                    config.current_resp3 = 0;
                }
            } else if ((is_subscribe || is_unsubscribe) && !config.pubsub_mode) {
                /* 未进入 pubsub 模式，恢复 push 回调 */
                if (config.push_output)
                    redisSetPushCallback(context, cliPushHandler);
            }

            break;
        }
        if (config.cluster_reissue_command){
            /* 若需要重发命令则跳出循环，
             * 避免更多 repeat 次数的空操作。 */
            break;
        }
        if (config.interval) usleep(config.interval);
        fflush(stdout); /* 便于 grep 处理输出 */
    }

    zfree(argvlen);
    return REDIS_OK;
}

/* 发送命令，若连接断开则自动重连 */
static redisReply *reconnectingRedisCommand(redisContext *c, const char *fmt, ...) {
    redisReply *reply = NULL;
    int tries = 0;
    va_list ap;

    assert(!c->err);
    while(reply == NULL) {
        while (c->err & (REDIS_ERR_IO | REDIS_ERR_EOF)) {
            printf("\r\x1b[0K"); /* 光标回到行首并清除该行 */
            printf("Reconnecting... %d\r", ++tries);
            fflush(stdout);

            redisFree(c);
            c = redisConnectWrapper(config.conn_info.hostip, config.conn_info.hostport,
                                    config.connect_timeout);
            if (!c->err && config.tls) {
                const char *err = NULL;
                if (cliSecureConnection(c, config.sslconfig, &err) == REDIS_ERR && err) {
                    fprintf(stderr, "TLS Error: %s\n", err);
                    exit(1);
                }
            }
            usleep(1000000);
        }

        va_start(ap,fmt);
        reply = redisvCommand(c,fmt,ap);
        va_end(ap);

        if (c->err && !(c->err & (REDIS_ERR_IO | REDIS_ERR_EOF))) {
            fprintf(stderr, "Error: %s\n", c->errstr);
            exit(1);
        } else if (tries > 0) {
            printf("\r\x1b[0K"); /* 光标回到行首并清除该行 */
        }
    }

    context = c;
    return reply;
}

/*------------------------------------------------------------------------------
 * User interface — 用户界面与命令行解析
 *--------------------------------------------------------------------------- */

/* 解析命令行选项，填充 config 结构体 */
static int parseOptions(int argc, char **argv) {
    int i;

    for (i = 1; i < argc; i++) {
        int lastarg = i==argc-1;

        if (!strcmp(argv[i],"-h") && !lastarg) {
            sdsfree(config.conn_info.hostip);
            config.conn_info.hostip = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"-h") && lastarg) {
            usage(0);
        } else if (!strcmp(argv[i],"--help")) {
            usage(0);
        } else if (!strcmp(argv[i],"-x")) {
            config.stdin_lastarg = 1;
        } else if (!strcmp(argv[i], "-X") && !lastarg) {
            config.stdin_tag_arg = 1;
            config.stdin_tag_name = argv[++i];
        } else if (!strcmp(argv[i],"-p") && !lastarg) {
            config.conn_info.hostport = atoi(argv[++i]);
            if (config.conn_info.hostport < 0 || config.conn_info.hostport > 65535) {
                fprintf(stderr, "Invalid server port.\n");
                exit(1);
            }
        } else if (!strcmp(argv[i],"-t") && !lastarg) {
            char *eptr;
            double seconds = strtod(argv[++i], &eptr);
            if (eptr[0] != '\0' || isnan(seconds) || seconds < 0.0) {
                fprintf(stderr, "Invalid connection timeout for -t.\n");
                exit(1);
            }
            config.connect_timeout.tv_sec = (long long)seconds;
            config.connect_timeout.tv_usec = ((long long)(seconds * 1000000)) % 1000000;
        } else if (!strcmp(argv[i],"-s") && !lastarg) {
            config.hostsocket = argv[++i];
        } else if (!strcmp(argv[i],"-r") && !lastarg) {
            config.repeat = strtoll(argv[++i],NULL,10);
        } else if (!strcmp(argv[i],"-i") && !lastarg) {
            double seconds = atof(argv[++i]);
            config.interval = seconds*1000000;
        } else if (!strcmp(argv[i],"-n") && !lastarg) {
            config.conn_info.input_dbnum = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--no-auth-warning")) {
            config.no_auth_warning = 1;
        } else if (!strcmp(argv[i], "--askpass")) {
            config.askpass = 1;
        } else if ((!strcmp(argv[i],"-a") || !strcmp(argv[i],"--pass"))
                   && !lastarg)
        {
            config.conn_info.auth = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"--user") && !lastarg) {
            config.conn_info.user = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"-u") && !lastarg) {
            parseRedisUri(argv[++i],"redis-cli",&config.conn_info,&config.tls);
            if (config.conn_info.hostport < 0 || config.conn_info.hostport > 65535) {
                fprintf(stderr, "Invalid server port.\n");
                exit(1);
            }
        } else if (!strcmp(argv[i],"--raw")) {
            config.output = OUTPUT_RAW;
        } else if (!strcmp(argv[i],"--no-raw")) {
            config.output = OUTPUT_STANDARD;
        } else if (!strcmp(argv[i],"--quoted-input")) {
            config.quoted_input = 1;
        } else if (!strcmp(argv[i],"--csv")) {
            config.output = OUTPUT_CSV;
        } else if (!strcmp(argv[i],"--json")) {
            /* Not overwrite explicit value by -3 */
            if (config.resp3 == 0) {
                config.resp3 = 2;
            }
            config.output = OUTPUT_JSON;
        } else if (!strcmp(argv[i],"--quoted-json")) {
            /* Not overwrite explicit value by -3*/
            if (config.resp3 == 0) {
                config.resp3 = 2;
            }
            config.output = OUTPUT_QUOTED_JSON;
        } else if (!strcmp(argv[i],"--latency")) {
            config.latency_mode = 1;
        } else if (!strcmp(argv[i],"--latency-dist")) {
            config.latency_dist_mode = 1;
        } else if (!strcmp(argv[i],"--mono")) {
            spectrum_palette = spectrum_palette_mono;
            spectrum_palette_size = spectrum_palette_mono_size;
        } else if (!strcmp(argv[i],"--latency-history")) {
            config.latency_mode = 1;
            config.latency_history = 1;
        } else if (!strcmp(argv[i],"--lru-test") && !lastarg) {
            config.lru_test_mode = 1;
            config.lru_test_sample_size = strtoll(argv[++i],NULL,10);
        } else if (!strcmp(argv[i],"--slave")) {
            config.slave_mode = 1;
        } else if (!strcmp(argv[i],"--replica")) {
            config.slave_mode = 1;
        } else if (!strcmp(argv[i],"--stat")) {
            config.stat_mode = 1;
        } else if (!strcmp(argv[i],"--scan")) {
            config.scan_mode = 1;
        } else if (!strcmp(argv[i],"--pattern") && !lastarg) {
            sdsfree(config.pattern);
            config.pattern = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"--count") && !lastarg) {
            config.count = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"--quoted-pattern") && !lastarg) {
            sdsfree(config.pattern);
            config.pattern = unquoteCString(argv[++i]);
            if (!config.pattern) {
                fprintf(stderr,"Invalid quoted string specified for --quoted-pattern.\n");
                exit(1);
            }
        } else if (!strcmp(argv[i],"--intrinsic-latency") && !lastarg) {
            config.intrinsic_latency_mode = 1;
            config.intrinsic_latency_duration = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"--rdb") && !lastarg) {
            config.getrdb_mode = 1;
            config.rdb_filename = argv[++i];
        } else if (!strcmp(argv[i],"--functions-rdb") && !lastarg) {
            config.get_functions_rdb_mode = 1;
            config.rdb_filename = argv[++i];
        } else if (!strcmp(argv[i],"--pipe")) {
            config.pipe_mode = 1;
        } else if (!strcmp(argv[i],"--pipe-timeout") && !lastarg) {
            config.pipe_timeout = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"--bigkeys")) {
            config.bigkeys = 1;
        } else if (!strcmp(argv[i],"--memkeys")) {
            config.memkeys = 1;
            config.memkeys_samples = -1; /* use redis default */
        } else if (!strcmp(argv[i],"--memkeys-samples") && !lastarg) {
            char *endptr;
            config.memkeys = 1;
            config.keystats = 1;
            config.memkeys_samples = strtoll(argv[++i], &endptr, 10);
            if (*endptr) {
                fprintf(stderr, "--memkeys-samples conversion error.\n");
                exit(1);
            }
            if (config.memkeys_samples < 0) {
               fprintf(stderr, "--memkeys-samples value should be positive.\n");
               exit(1);
            }
        } else if (!strcmp(argv[i],"--hotkeys")) {
            config.hotkeys = 1;
        } else if (!strcmp(argv[i], "--keystats")) {
            config.keystats = 1;
            config.memkeys_samples = -1; /* use redis default */
        } else if (!strcmp(argv[i],"--keystats-samples") && !lastarg) {
            char *endptr;
            config.keystats = 1;
            config.memkeys_samples = strtoll(argv[++i], &endptr, 10);
            if (*endptr) {
                fprintf(stderr, "--keystats-samples conversion error.\n");
                exit(1);
            }
            if (config.memkeys_samples < 0) {
               fprintf(stderr, "--keystats-samples value should be positive.\n");
               exit(1);
            }
        } else if (!strcmp(argv[i],"--cursor") && !lastarg) {
            i++;
            char sign = *argv[i];
            char *endptr;
            config.cursor = strtoull(argv[i], &endptr, 10);
            if (*endptr) {
               fprintf(stderr, "--cursor conversion error.\n");
               exit(1);
            }
            if (sign == '-' && config.cursor != 0) {
                fprintf(stderr, "--cursor should be followed by a positive integer.\n");
                exit(1);
            }
        } else if (!strcmp(argv[i],"--top") && !lastarg) {
            i++;
            char sign = *argv[i];
            char *endptr;
            config.top_sizes_limit = strtoull(argv[i], &endptr, 10);
            if (*endptr) {
               fprintf(stderr, "--top conversion error.\n");
               exit(1);
            }
            if (sign == '-' && config.top_sizes_limit != 0) {
                fprintf(stderr, "--top should be followed by a positive integer.\n");
                exit(1);
            }
        } else if (!strcmp(argv[i],"--eval") && !lastarg) {
            config.eval = argv[++i];
        } else if (!strcmp(argv[i],"--ldb")) {
            config.eval_ldb = 1;
            config.output = OUTPUT_RAW;
        } else if (!strcmp(argv[i],"--ldb-sync-mode")) {
            config.eval_ldb = 1;
            config.eval_ldb_sync = 1;
            config.output = OUTPUT_RAW;
        } else if (!strcmp(argv[i],"-c")) {
            config.cluster_mode = 1;
        } else if (!strcmp(argv[i],"-d") && !lastarg) {
            sdsfree(config.mb_delim);
            config.mb_delim = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"-D") && !lastarg) {
            sdsfree(config.cmd_delim);
            config.cmd_delim = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"-e")) {
            config.set_errcode = 1;
        } else if (!strcmp(argv[i],"--verbose")) {
            config.verbose = 1;
        } else if (!strcmp(argv[i],"-4")) {
            config.prefer_ipv4 = 1;
        } else if (!strcmp(argv[i],"-6")) {
            config.prefer_ipv6 = 1;
        } else if (!strcmp(argv[i],"--cluster") && !lastarg) {
            if (CLUSTER_MANAGER_MODE()) usage(1);
            char *cmd = argv[++i];
            int j = i;
            while (j < argc && argv[j][0] != '-') j++;
            if (j > i) j--;
            int err = createClusterManagerCommand(cmd, j - i, argv + i + 1);
            if (err) exit(err);
            i = j;
        } else if (!strcmp(argv[i],"--cluster") && lastarg) {
            usage(1);
        } else if ((!strcmp(argv[i],"--cluster-only-masters"))) {
            config.cluster_manager_command.flags |=
                    CLUSTER_MANAGER_CMD_FLAG_MASTERS_ONLY;
        } else if ((!strcmp(argv[i],"--cluster-only-replicas"))) {
            config.cluster_manager_command.flags |=
                    CLUSTER_MANAGER_CMD_FLAG_SLAVES_ONLY;
        } else if (!strcmp(argv[i],"--cluster-replicas") && !lastarg) {
            config.cluster_manager_command.replicas = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"--cluster-master-id") && !lastarg) {
            config.cluster_manager_command.master_id = argv[++i];
        } else if (!strcmp(argv[i],"--cluster-from") && !lastarg) {
            config.cluster_manager_command.from = argv[++i];
        } else if (!strcmp(argv[i],"--cluster-to") && !lastarg) {
            config.cluster_manager_command.to = argv[++i];
        } else if (!strcmp(argv[i],"--cluster-from-user") && !lastarg) {
            config.cluster_manager_command.from_user = argv[++i];
        } else if (!strcmp(argv[i],"--cluster-from-pass") && !lastarg) {
            config.cluster_manager_command.from_pass = argv[++i];
        } else if (!strcmp(argv[i], "--cluster-from-askpass")) {
            config.cluster_manager_command.from_askpass = 1;
        } else if (!strcmp(argv[i],"--cluster-weight") && !lastarg) {
            if (config.cluster_manager_command.weight != NULL) {
                fprintf(stderr, "WARNING: you cannot use --cluster-weight "
                                "more than once.\n"
                                "You can set more weights by adding them "
                                "as a space-separated list, ie:\n"
                                "--cluster-weight n1=w n2=w\n");
                exit(1);
            }
            int widx = i + 1;
            char **weight = argv + widx;
            int wargc = 0;
            for (; widx < argc; widx++) {
                if (strstr(argv[widx], "--") == argv[widx]) break;
                if (strchr(argv[widx], '=') == NULL) break;
                wargc++;
            }
            if (wargc > 0) {
                config.cluster_manager_command.weight = weight;
                config.cluster_manager_command.weight_argc = wargc;
                i += wargc;
            }
        } else if (!strcmp(argv[i],"--cluster-slots") && !lastarg) {
            config.cluster_manager_command.slots = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"--cluster-timeout") && !lastarg) {
            config.cluster_manager_command.timeout = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"--cluster-pipeline") && !lastarg) {
            config.cluster_manager_command.pipeline = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"--cluster-threshold") && !lastarg) {
            config.cluster_manager_command.threshold = atof(argv[++i]);
        } else if (!strcmp(argv[i],"--cluster-yes")) {
            config.cluster_manager_command.flags |=
                CLUSTER_MANAGER_CMD_FLAG_YES;
        } else if (!strcmp(argv[i],"--cluster-simulate")) {
            config.cluster_manager_command.flags |=
                CLUSTER_MANAGER_CMD_FLAG_SIMULATE;
        } else if (!strcmp(argv[i],"--cluster-replace")) {
            config.cluster_manager_command.flags |=
                CLUSTER_MANAGER_CMD_FLAG_REPLACE;
        } else if (!strcmp(argv[i],"--cluster-copy")) {
            config.cluster_manager_command.flags |=
                CLUSTER_MANAGER_CMD_FLAG_COPY;
        } else if (!strcmp(argv[i],"--cluster-slave")) {
            config.cluster_manager_command.flags |=
                CLUSTER_MANAGER_CMD_FLAG_SLAVE;
        } else if (!strcmp(argv[i],"--cluster-use-empty-masters")) {
            config.cluster_manager_command.flags |=
                CLUSTER_MANAGER_CMD_FLAG_EMPTYMASTER;
        } else if (!strcmp(argv[i],"--cluster-search-multiple-owners")) {
            config.cluster_manager_command.flags |=
                CLUSTER_MANAGER_CMD_FLAG_CHECK_OWNERS;
        } else if (!strcmp(argv[i],"--cluster-fix-with-unreachable-masters")) {
            /* 修复集群时允许包含不可达的主节点 */
            config.cluster_manager_command.flags |=
                CLUSTER_MANAGER_CMD_FLAG_FIX_WITH_UNREACHABLE_MASTERS;
        } else if (!strcmp(argv[i],"--test_hint") && !lastarg) {
            config.test_hint = argv[++i];
        } else if (!strcmp(argv[i],"--test_hint_file") && !lastarg) {
            config.test_hint_file = argv[++i];
#ifdef USE_OPENSSL
        } else if (!strcmp(argv[i],"--tls")) {
            /* 启用 TLS 安全连接 */
            config.tls = 1;
        } else if (!strcmp(argv[i],"--sni") && !lastarg) {
            /* TLS SNI（服务器名称指示） */
            config.sslconfig.sni = argv[++i];
        } else if (!strcmp(argv[i],"--cacertdir") && !lastarg) {
            /* CA 证书目录 */
            config.sslconfig.cacertdir = argv[++i];
        } else if (!strcmp(argv[i],"--cacert") && !lastarg) {
            /* CA 证书文件 */
            config.sslconfig.cacert = argv[++i];
        } else if (!strcmp(argv[i],"--cert") && !lastarg) {
            /* 客户端证书文件 */
            config.sslconfig.cert = argv[++i];
        } else if (!strcmp(argv[i],"--key") && !lastarg) {
            /* 客户端私钥文件 */
            config.sslconfig.key = argv[++i];
        } else if (!strcmp(argv[i],"--tls-ciphers") && !lastarg) {
            /* TLSv1.2 及以下版本的密码套件列表 */
            config.sslconfig.ciphers = argv[++i];
        } else if (!strcmp(argv[i],"--insecure")) {
            /* 跳过证书校验，允许不安全连接 */
            config.sslconfig.skip_cert_verify = 1;
        #ifdef TLS1_3_VERSION
        } else if (!strcmp(argv[i],"--tls-ciphersuites") && !lastarg) {
            /* TLSv1.3 版本的密码套件列表 */
            config.sslconfig.ciphersuites = argv[++i];
        #endif
#endif
        } else if (!strcmp(argv[i],"-v") || !strcmp(argv[i], "--version")) {
            sds version = cliVersion();
            printf("redis-cli %s\n", version);
            sdsfree(version);
            exit(0);
        } else if (!strcmp(argv[i],"-2")) {
            /* 显式使用 RESP2 协议 */
            config.resp2 = 1;
        } else if (!strcmp(argv[i],"-3")) {
            /* 显式使用 RESP3 协议 */
            config.resp3 = 1;
        } else if (!strcmp(argv[i],"--show-pushes") && !lastarg) {
            /* 是否打印 RESP3 PUSH 消息 */
            char *argval = argv[++i];
            if (!strncasecmp(argval, "n", 1)) {
                config.push_output = 0;
            } else if (!strncasecmp(argval, "y", 1)) {
                config.push_output = 1;
            } else {
                fprintf(stderr, "Unknown --show-pushes value '%s' "
                        "(valid: '[y]es', '[n]o')\n", argval);
            }
        } else if (CLUSTER_MANAGER_MODE() && argv[i][0] != '-') {
            /* 收集集群管理子命令的参数 */
            if (config.cluster_manager_command.argc == 0) {
                int j = i + 1;
                while (j < argc && argv[j][0] != '-') j++;
                int cmd_argc = j - i;
                config.cluster_manager_command.argc = cmd_argc;
                config.cluster_manager_command.argv = argv + i;
                if (cmd_argc > 1) i = j - 1;
            }
        } else {
            if (argv[i][0] == '-') {
                fprintf(stderr,
                    "Unrecognized option or bad number of args for: '%s'\n",
                    argv[i]);
                exit(1);
            } else {
                /* 看起来是命令名，停止解析选项 */
                break;
            }
        }
    }

    /* 校验互斥选项组合 */
    if (config.hostsocket && config.cluster_mode) {
        fprintf(stderr,"Options -c and -s are mutually exclusive.\n");
        exit(1);
    }

    if (config.resp2 && config.resp3 == 1) {
        fprintf(stderr,"Options -2 and -3 are mutually exclusive.\n");
        exit(1);
    }

    /* --ldb 需要配合 --eval 使用 */
    if (config.eval_ldb && config.eval == NULL) {
        fprintf(stderr,"Options --ldb and --ldb-sync-mode require --eval.\n");
        fprintf(stderr,"Try %s --help for more information.\n", argv[0]);
        exit(1);
    }

    /* 若未禁用则打印密码安全警告 */
    if (!config.no_auth_warning && config.conn_info.auth != NULL) {
        fputs("Warning: Using a password with '-a' or '-u' option on the command"
              " line interface may not be safe.\n", stderr);
    }

    if (config.get_functions_rdb_mode && config.getrdb_mode) {
        fprintf(stderr,"Option --functions-rdb and --rdb are mutually exclusive.\n");
        exit(1);
    }

    if (config.stdin_lastarg && config.stdin_tag_arg) {
        fprintf(stderr, "Options -x and -X are mutually exclusive.\n");
        exit(1);
    }

    if (config.prefer_ipv4 && config.prefer_ipv6) {
        fprintf(stderr, "Options -4 and -6 are mutually exclusive.\n");
        exit(1);
    }

    return i;
}

/* 解析环境变量设置 */
static void parseEnv(void) {
    /* 从环境变量读取密码，但不覆盖 CLI 参数 */
    char *auth = getenv(REDIS_CLI_AUTH_ENV);
    if (auth != NULL && config.conn_info.auth == NULL) {
        config.conn_info.auth = auth;
    }

    char *cluster_yes = getenv(REDIS_CLI_CLUSTER_YES_ENV);
    if (cluster_yes != NULL && !strcmp(cluster_yes, "1")) {
        config.cluster_manager_command.flags |= CLUSTER_MANAGER_CMD_FLAG_YES;
    }
}

/* 打印 redis-cli 的帮助信息；err 非零时输出到 stderr 并以非零码退出 */
static void usage(int err) {
    sds version = cliVersion();
    FILE *target = err ? stderr: stdout;
    /* TLS 相关使用说明（仅在启用 OpenSSL 时编译进二进制） */
    const char *tls_usage =
#ifdef USE_OPENSSL
"  --tls              Establish a secure TLS connection.\n"
"  --sni <host>       Server name indication for TLS.\n"
"  --cacert <file>    CA Certificate file to verify with.\n"
"  --cacertdir <dir>  Directory where trusted CA certificates are stored.\n"
"                     If neither cacert nor cacertdir are specified, the default\n"
"                     system-wide trusted root certs configuration will apply.\n"
"  --insecure         Allow insecure TLS connection by skipping cert validation.\n"
"  --cert <file>      Client certificate to authenticate with.\n"
"  --key <file>       Private key file to authenticate with.\n"
"  --tls-ciphers <list> Sets the list of preferred ciphers (TLSv1.2 and below)\n"
"                     in order of preference from highest to lowest separated by colon (\":\").\n"
"                     See the ciphers(1ssl) manpage for more information about the syntax of this string.\n"
#ifdef TLS1_3_VERSION
"  --tls-ciphersuites <list> Sets the list of preferred ciphersuites (TLSv1.3)\n"
"                     in order of preference from highest to lowest separated by colon (\":\").\n"
"                     See the ciphers(1ssl) manpage for more information about the syntax of this string,\n"
"                     and specifically for TLSv1.3 ciphersuites.\n"
#endif
#endif
"";

    fprintf(target,
"redis-cli %s\n"
"\n"
"Usage: redis-cli [OPTIONS] [cmd [arg [arg ...]]]\n"
"  -h <hostname>      Server hostname (default: 127.0.0.1).\n"
"  -p <port>          Server port (default: 6379).\n"
"  -t <timeout>       Server connection timeout in seconds (decimals allowed).\n"
"                     Default timeout is 0, meaning no limit, depending on the OS.\n"
"  -s <socket>        Server socket (overrides hostname and port).\n"
"  -a <password>      Password to use when connecting to the server.\n"
"                     You can also use the " REDIS_CLI_AUTH_ENV " environment\n"
"                     variable to pass this password more safely\n"
"                     (if both are used, this argument takes precedence).\n"
"  --user <username>  Used to send ACL style 'AUTH username pass'. Needs -a.\n"
"  --pass <password>  Alias of -a for consistency with the new --user option.\n"
"  --askpass          Force user to input password with mask from STDIN.\n"
"                     If this argument is used, '-a' and " REDIS_CLI_AUTH_ENV "\n"
"                     environment variable will be ignored.\n"
"  -u <uri>           Server URI on format redis://user:password@host:port/dbnum\n"
"                     User, password and dbnum are optional. For authentication\n"
"                     without a username, use username 'default'. For TLS, use\n"
"                     the scheme 'rediss'.\n"
"  -r <repeat>        Execute specified command N times.\n"
"  -i <interval>      When -r is used, waits <interval> seconds per command.\n"
"                     It is possible to specify sub-second times like -i 0.1.\n"
"                     This interval is also used in --scan and --stat per cycle.\n"
"                     and in --bigkeys, --memkeys, and --hotkeys per 100 cycles.\n"
"  -n <db>            Database number.\n"
"  -2                 Start session in RESP2 protocol mode.\n"
"  -3                 Start session in RESP3 protocol mode.\n"
"  -x                 Read last argument from STDIN (see example below).\n"
"  -X                 Read <tag> argument from STDIN (see example below).\n"
"  -d <delimiter>     Delimiter between response bulks for raw formatting (default: \\n).\n"
"  -D <delimiter>     Delimiter between responses for raw formatting (default: \\n).\n"
"  -c                 Enable cluster mode (follow -ASK and -MOVED redirections).\n"
"  -e                 Return exit error code when command execution fails.\n"
"  -4                 Prefer IPv4 over IPv6 on DNS lookup.\n"
"  -6                 Prefer IPv6 over IPv4 on DNS lookup.\n"
"%s"
"  --raw              Use raw formatting for replies (default when STDOUT is\n"
"                     not a tty).\n"
"  --no-raw           Force formatted output even when STDOUT is not a tty.\n"
"  --quoted-input     Force input to be handled as quoted strings.\n"
"  --csv              Output in CSV format.\n"
"  --json             Output in JSON format (default RESP3, use -2 if you want to use with RESP2).\n"
"  --quoted-json      Same as --json, but produce ASCII-safe quoted strings, not Unicode.\n"
"  --show-pushes <yn> Whether to print RESP3 PUSH messages.  Enabled by default when\n"
"                     STDOUT is a tty but can be overridden with --show-pushes no.\n"
"  --stat             Print rolling stats about server: mem, clients, ...\n",
version,tls_usage);

    fprintf(target,
"  --latency          Enter a special mode continuously sampling latency.\n"
"                     If you use this mode in an interactive session it runs\n"
"                     forever displaying real-time stats. Otherwise if --raw or\n"
"                     --csv is specified, or if you redirect the output to a non\n"
"                     TTY, it samples the latency for 1 second (you can use\n"
"                     -i to change the interval), then produces a single output\n"
"                     and exits.\n"
"  --latency-history  Like --latency but tracking latency changes over time.\n"
"                     Default time interval is 15 sec. Change it using -i.\n"
"  --latency-dist     Shows latency as a spectrum, requires xterm 256 colors.\n"
"                     Default time interval is 1 sec. Change it using -i.\n"
"  --lru-test <keys>  Simulate a cache workload with an 80-20 distribution.\n"
"  --replica          Simulate a replica showing commands received from the master.\n"
"  --rdb <filename>   Transfer an RDB dump from remote server to local file.\n"
"                     Use filename of \"-\" to write to stdout.\n"
"  --functions-rdb <filename> Like --rdb but only get the functions (not the keys)\n"
"                     when getting the RDB dump file.\n"
"  --pipe             Transfer raw Redis protocol from stdin to server.\n"
"  --pipe-timeout <n> In --pipe mode, abort with error if after sending all data.\n"
"                     no reply is received within <n> seconds.\n"
"                     Default timeout: %d. Use 0 to wait forever.\n",
    REDIS_CLI_DEFAULT_PIPE_TIMEOUT);
    fprintf(target,
"  --bigkeys          Sample Redis keys looking for keys with many elements (complexity).\n"
"  --memkeys          Sample Redis keys looking for keys consuming a lot of memory.\n"
"  --memkeys-samples <n> Sample Redis keys looking for keys consuming a lot of memory.\n"
"                     And define number of key elements to sample\n"
"  --keystats         Sample Redis keys looking for keys memory size and length (combine bigkeys and memkeys).\n"
"  --keystats-samples <n> Sample Redis keys looking for keys memory size and length.\n"
"                     And define number of key elements to sample (only for memory usage).\n"
"  --cursor <n>       Start the scan at the cursor <n> (usually after a Ctrl-C).\n"
"                     Optionally used with --keystats and --keystats-samples.\n"
"  --top <n>          To display <n> top key sizes (default: 10).\n"
"                     Optionally used with --keystats and --keystats-samples.\n"
"  --hotkeys          Sample Redis keys looking for hot keys.\n"
"                     only works when maxmemory-policy is *lfu.\n"
"  --scan             List all keys using the SCAN command.\n"
"  --pattern <pat>    Keys pattern when using the --scan, --bigkeys or --hotkeys\n"
"                     options (default: *).\n"
"  --count <count>    Count option when using the --scan, --bigkeys or --hotkeys (default: 10).\n"
"  --quoted-pattern <pat> Same as --pattern, but the specified string can be\n"
"                         quoted, in order to pass an otherwise non binary-safe string.\n"
"  --intrinsic-latency <sec> Run a test to measure intrinsic system latency.\n"
"                     The test will run for the specified amount of seconds.\n"
"  --eval <file>      Send an EVAL command using the Lua script at <file>.\n"
"  --ldb              Used with --eval enable the Redis Lua debugger.\n"
"  --ldb-sync-mode    Like --ldb but uses the synchronous Lua debugger, in\n"
"                     this mode the server is blocked and script changes are\n"
"                     not rolled back from the server memory.\n"
"  --cluster <command> [args...] [opts...]\n"
"                     Cluster Manager command and arguments (see below).\n"
"  --verbose          Verbose mode.\n"
"  --no-auth-warning  Don't show warning message when using password on command\n"
"                     line interface.\n"
"  --help             Output this help and exit.\n"
"  --version          Output version and exit.\n"
"\n");
    /* Using another fprintf call to avoid -Woverlength-strings compile warning */
    fprintf(target,
"Cluster Manager Commands:\n"
"  Use --cluster help to list all available cluster manager commands.\n"
"\n"
"Examples:\n"
"  redis-cli -u redis://default:PASSWORD@localhost:6379/0\n"
"  cat /etc/passwd | redis-cli -x set mypasswd\n"
"  redis-cli -D \"\" --raw dump key > key.dump && redis-cli -X dump_tag restore key2 0 dump_tag replace < key.dump\n"
"  redis-cli -r 100 lpush mylist x\n"
"  redis-cli -r 100 -i 1 info | grep used_memory_human:\n"
"  redis-cli --quoted-input set '\"null-\\x00-separated\"' value\n"
"  redis-cli --eval myscript.lua key1 key2 , arg1 arg2 arg3\n"
"  redis-cli --scan --pattern '*:12345*'\n"
"  redis-cli --scan --pattern '*:12345*' --count 100\n"
"\n"
"  (Note: when using --eval the comma separates KEYS[] from ARGV[] items)\n"
"\n"
"When no command is given, redis-cli starts in interactive mode.\n"
"Type \"help\" in interactive mode for information on available commands\n"
"and settings.\n"
"\n");
    sdsfree(version);
    exit(err);
}

static int confirmWithYes(char *msg, int ignore_force) {
    /* 若设置了 --cluster-yes 且未忽略强制标志则直接确认 */
    if (!ignore_force &&
        (config.cluster_manager_command.flags & CLUSTER_MANAGER_CMD_FLAG_YES)) {
        return 1;
    }

    printf("%s (type 'yes' to accept): ", msg);
    fflush(stdout);
    char buf[4];
    int nread = read(fileno(stdin),buf,4);
    buf[3] = '\0';
    return (nread != 0 && !strcmp("yes", buf));
}

/* 发送命令，必要时进行 repeat 重复。处理重定向和重连。 */
static int issueCommandRepeat(int argc, char **argv, long repeat) {
    /* Lua 调试模式下希望把 "help" 透传给 Redis，由其自身处理 HELP 消息，
     * 而非由 CLI 处理（详见 ldbRepl）。
     *
     * 对于普通 Redis HELP，无需连接即可处理。 */
    if (!config.eval_ldb &&
        (!strcasecmp(argv[0],"help") || !strcasecmp(argv[0],"?")))
    {
        cliOutputHelp(--argc, ++argv);
        return REDIS_OK;
    }

    while (1) {
        if (config.cluster_reissue_command || context == NULL ||
            context->err == REDIS_ERR_IO || context->err == REDIS_ERR_EOF)
        {
            if (cliConnect(CC_FORCE) != REDIS_OK) {
                cliPrintContextError();
                config.cluster_reissue_command = 0;
                return REDIS_ERR;
            }
        }
        config.cluster_reissue_command = 0;
        if (config.cluster_send_asking) {
            if (cliSendAsking() != REDIS_OK) {
                cliPrintContextError();
                return REDIS_ERR;
            }
        }
        if (cliSendCommand(argc,argv,repeat) != REDIS_OK) {
            cliPrintContextError();
            redisFree(context);
            context = NULL;
            return REDIS_ERR;
        }

        /* 集群模式下若被重定向则再次发送命令 */
        if (config.cluster_mode && config.cluster_reissue_command) {
            continue;
        }
        break;
    }
    return REDIS_OK;
}

/* 使用配置中的 repeat 次数发送命令 */
static int issueCommand(int argc, char **argv) {
    return issueCommandRepeat(argc, argv, config.repeat);
}

/* 将用户输入的命令拆分为多个 SDS 参数。
 * 通常使用 sds.c 中的 sdssplitargs()，它支持引号字符串、转义等。
 * 但在 Lua 调试模式下使用 "eval" 命令时，希望把 "e " 或 "eval "
 * 后面的整个 Lua 脚本原样作为一个大参数传递。 */
static sds *cliSplitArgs(char *line, int *argc) {
    if (config.eval_ldb && (strstr(line,"eval ") == line ||
                            strstr(line,"e ") == line))
    {
        sds *argv = sds_malloc(sizeof(sds)*2);
        *argc = 2;
        int len = strlen(line);
        int elen = line[1] == ' ' ? 2 : 5; /* "e " 还是 "eval "？ */
        argv[0] = sdsnewlen(line,elen-1);
        argv[1] = sdsnewlen(line+elen,len-elen);
        return argv;
    } else {
        return sdssplitargs(line,argc);
    }
}

/* 设置 CLI 偏好。该函数在交互模式下调用 ":command" 或读取 ~/.redisclirc 时执行。 */
void cliSetPreferences(char **argv, int argc, int interactive) {
    if (!strcasecmp(argv[0],":set") && argc >= 2) {
        if (!strcasecmp(argv[1],"hints")) pref.hints = 1;
        else if (!strcasecmp(argv[1],"nohints")) pref.hints = 0;
        else {
            printf("%sunknown redis-cli preference '%s'\n",
                interactive ? "" : ".redisclirc: ",
                argv[1]);
        }
    } else {
        printf("%sunknown redis-cli internal command '%s'\n",
            interactive ? "" : ".redisclirc: ",
            argv[0]);
    }
}

/* 加载 ~/.redisclirc 文件（若存在） */
void cliLoadPreferences(void) {
    sds rcfile = getDotfilePath(REDIS_CLI_RCFILE_ENV,REDIS_CLI_RCFILE_DEFAULT);
    if (rcfile == NULL) return;
    FILE *fp = fopen(rcfile,"r");
    char buf[1024];

    if (fp) {
        while(fgets(buf,sizeof(buf),fp) != NULL) {
            sds *argv;
            int argc;

            argv = sdssplitargs(buf,&argc);
            if (argc > 0) cliSetPreferences(argv,argc,0);
            sdsfreesplitres(argv,argc);
        }
        fclose(fp);
    }
    sdsfree(rcfile);
}

/* 部分命令可能包含敏感信息，不应写入历史文件。当前包含：
 * - AUTH
 * - ACL DELUSER、ACL SETUSER、ACL GETUSER
 * - CONFIG SET masterauth/masteruser/tls-key-file-pass/tls-client-key-file-pass/requirepass
 * - HELLO 携带 [AUTH username password]
 * - MIGRATE 携带 [AUTH password] 或 [AUTH2 username password]
 * - SENTINEL CONFIG SET sentinel-pass password、SENTINEL CONFIG SET sentinel-user username
 * - SENTINEL SET <mastername> auth-pass password、SENTINEL SET <mastername> auth-user username */
static int isSensitiveCommand(int argc, char **argv) {
    if (!strcasecmp(argv[0],"auth")) {
        return 1;
    } else if (argc > 1 &&
        !strcasecmp(argv[0],"acl") && (
            !strcasecmp(argv[1],"deluser") ||
            !strcasecmp(argv[1],"setuser") ||
            !strcasecmp(argv[1],"getuser")))
    {
        return 1;
    } else if (argc > 2 &&
        !strcasecmp(argv[0],"config") &&
        !strcasecmp(argv[1],"set")) {
            for (int j = 2; j < argc; j = j+2) {
                if (!strcasecmp(argv[j],"masterauth") ||
                    !strcasecmp(argv[j],"masteruser") ||
                    !strcasecmp(argv[j],"tls-key-file-pass") ||
                    !strcasecmp(argv[j],"tls-client-key-file-pass") ||
                    !strcasecmp(argv[j],"requirepass")) {
                    return 1;
                }
            }
            return 0;
    /* HELLO [protover [AUTH username password] [SETNAME clientname]] */
    } else if (argc > 4 && !strcasecmp(argv[0],"hello")) {
        for (int j = 2; j < argc; j++) {
            int moreargs = argc - 1 - j;
            if (!strcasecmp(argv[j],"AUTH") && moreargs >= 2) {
                return 1;
            } else if (!strcasecmp(argv[j],"SETNAME") && moreargs) {
                j++;
            } else {
                return 0;
            }
        }
    /* MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE]
     * [AUTH password] [AUTH2 username password] [KEYS key [key ...]] */
    } else if (argc > 7 && !strcasecmp(argv[0], "migrate")) {
        for (int j = 6; j < argc; j++) {
            int moreargs = argc - 1 - j;
            if (!strcasecmp(argv[j],"auth") && moreargs) {
                return 1;
            } else if (!strcasecmp(argv[j],"auth2") && moreargs >= 2) {
                return 1;
            } else if (!strcasecmp(argv[j],"keys") && moreargs) {
                return 0;
            }
        }
    } else if (argc > 4 && !strcasecmp(argv[0], "sentinel")) {
        /* SENTINEL CONFIG SET sentinel-pass password
         * SENTINEL CONFIG SET sentinel-user username */
        if (!strcasecmp(argv[1], "config") && 
            !strcasecmp(argv[2], "set") &&
            (!strcasecmp(argv[3], "sentinel-pass") ||
             !strcasecmp(argv[3], "sentinel-user"))) 
        {
            return 1;
        }
        /* SENTINEL SET <mastername> auth-pass password 
         * SENTINEL SET <mastername> auth-user username */
        if (!strcasecmp(argv[1], "set") &&
            (!strcasecmp(argv[3], "auth-pass") || 
             !strcasecmp(argv[3], "auth-user"))) 
        {
            return 1;
        }
    }
    return 0;
}

/* redis-cli 的交互式命令行主循环（REPL） */
static void repl(void) {
    sds historyfile = NULL;
    int history = 0;
    char *line;
    int argc;
    sds *argv;

    /* Lua 调试模式下不需要初始化 redis HELP。
     * 它有自己的一套 HELP 和命令（COMMAND/COMMAND DOCS 会失败且无返回）。
     * 我们会在 Lua 调试会话结束后再初始化 redis HELP。 */
    if ((!config.eval_ldb) && isatty(fileno(stdin))) {
        /* 使用 COMMAND 命令的结果初始化帮助信息 */
        cliInitHelp();
    }

    config.interactive = 1;
    linenoiseSetMultiLine(1);
    linenoiseSetCompletionCallback(completionCallback);
    linenoiseSetHintsCallback(hintsCallback);
    linenoiseSetFreeHintsCallback(freeHintsCallback);

    /* 仅当 stdin 是 TTY 时才使用历史记录和加载 rc 文件 */
    if (getenv("FAKETTY_WITH_PROMPT") != NULL || isatty(fileno(stdin))) {
        historyfile = getDotfilePath(REDIS_CLI_HISTFILE_ENV,REDIS_CLI_HISTFILE_DEFAULT);
        // 始终保留内存中的历史，无论历史文件路径是否能确定
        history = 1;
        if (historyfile != NULL) {
            linenoiseHistoryLoad(historyfile);
        }
        cliLoadPreferences();
    }

    cliRefreshPrompt();
    while(1) {
        line = linenoise(context ? config.prompt : "not connected> ");
        if (line == NULL) {
            /* ^C、^D 或类似中断 */
            if (config.pubsub_mode) {
                config.pubsub_mode = 0;
                if (cliConnect(CC_FORCE) == REDIS_OK)
                    continue;
            }
            break;
        } else if (line[0] != '\0') {
            long repeat = 1;
            int skipargs = 0;
            char *endptr = NULL;

            argv = cliSplitArgs(line,&argc);
            if (argv == NULL) {
                printf("Invalid argument(s)\n");
                fflush(stdout);
                if (history) linenoiseHistoryAdd(line, 0);
                if (historyfile) linenoiseHistorySave(historyfile);
                linenoiseFree(line);
                continue;
            } else if (argc == 0) {
                sdsfreesplitres(argv,argc);
                linenoiseFree(line);
                continue;
            }

            /* 检查是否为 repeat 命令选项（数字前缀），若是则需跳过该参数 */
            errno = 0;
            repeat = strtol(argv[0], &endptr, 10);
            if (argc > 1 && *endptr == '\0') {
                if (errno == ERANGE || errno == EINVAL || repeat <= 0) {
                    fputs("Invalid redis-cli repeat command option value.\n", stdout);
                    sdsfreesplitres(argv, argc);
                    linenoiseFree(line);
                    continue;
                }
                skipargs = 1;
            } else {
                repeat = 1;
            }

            /* 始终保留内存中的历史记录。但对于包含敏感信息的命令，
             * 避免写入历史文件。 */
            int is_sensitive = isSensitiveCommand(argc - skipargs, argv + skipargs);
            if (history) linenoiseHistoryAdd(line, is_sensitive);
            if (!is_sensitive && historyfile) linenoiseHistorySave(historyfile);

            if (strcasecmp(argv[0],"quit") == 0 ||
                strcasecmp(argv[0],"exit") == 0)
            {
                /* 用户请求退出 */
                exit(0);
            } else if (argv[0][0] == ':') {
                /* 以 ':' 开头的命令用于设置偏好 */
                cliSetPreferences(argv,argc,1);
                sdsfreesplitres(argv,argc);
                linenoiseFree(line);
                continue;
            } else if (strcasecmp(argv[0],"restart") == 0) {
                if (config.eval) {
                    /* Lua 调试模式下重启会话 */
                    config.eval_ldb = 1;
                    config.output = OUTPUT_RAW;
                    sdsfreesplitres(argv,argc);
                    linenoiseFree(line);
                    return; /* 返回 evalMode 重启会话 */
                } else {
                    printf("Use 'restart' only in Lua debugging mode.\n");
                    fflush(stdout);
                }
            } else if (argc == 3 && !strcasecmp(argv[0],"connect")) {
                /* 切换连接 */
                sdsfree(config.conn_info.hostip);
                config.conn_info.hostip = sdsnew(argv[1]);
                config.conn_info.hostport = atoi(argv[2]);
                cliRefreshPrompt();
                cliConnect(CC_FORCE);
            } else if (argc == 1 && !strcasecmp(argv[0],"clear")) {
                /* 清屏 */
                linenoiseClearScreen();
            } else {
                long long start_time = mstime(), elapsed;

                issueCommandRepeat(argc-skipargs, argv+skipargs, repeat);

                /* 调试会话结束时显示 EVAL 最终回复 */
                if (config.eval_ldb_end) {
                    config.eval_ldb_end = 0;
                    cliReadReply(0);
                    printf("\n(Lua debugging session ended%s)\n\n",
                        config.eval_ldb_sync ? "" :
                        " -- dataset changes rolled back");
                    cliInitHelp();
                }

                elapsed = mstime()-start_time;
                if (elapsed >= 500 &&
                    config.output == OUTPUT_STANDARD)
                {
                    /* 命令耗时较长时打印耗时 */
                    printf("(%.2fs)\n",(double)elapsed/1000);
                }
            }
            /* 释放参数数组 */
            sdsfreesplitres(argv,argc);
        }

        if (config.pubsub_mode) {
            cliWaitForMessagesOrStdin();
        }

        /* linenoise() 返回的是 malloc 分配的字符串，类似于 readline() */
        linenoiseFree(line);
    }
    exit(0);
}

/* 非交互模式：根据命令行参数直接执行命令 */
static int noninteractive(int argc, char **argv) {
    int retval = 0;
    sds *sds_args = getSdsArrayFromArgv(argc, argv, config.quoted_input);

    if (!sds_args) {
        printf("Invalid quoted string\n");
        return 1;
    }

    if (config.stdin_lastarg) {
        /* -x：从标准输入读取最后一个参数 */
        sds_args = sds_realloc(sds_args, (argc + 1) * sizeof(sds));
        sds_args[argc] = readArgFromStdin();
        argc++;
    } else if (config.stdin_tag_arg) {
        /* -X：替换参数中匹配的标签 */
        int i = 0, tag_match = 0;

        for (; i < argc; i++) {
            if (strcmp(config.stdin_tag_name, sds_args[i]) != 0) continue;

            tag_match = 1;
            sdsfree(sds_args[i]);
            sds_args[i] = readArgFromStdin();
            break;
        }

        if (!tag_match) {
            sdsfreesplitres(sds_args, argc);
            fprintf(stderr, "Using -X option but stdin tag not match.\n");
            return 1;
        }
    }

    retval = issueCommand(argc, sds_args);
    sdsfreesplitres(sds_args, argc);
    while (config.pubsub_mode) {
        if (cliReadReply(0) != REDIS_OK) {
            cliPrintContextError();
            exit(1);
        }
        fflush(stdout);
    }
    return retval == REDIS_OK ? 0 : 1;
}

/*------------------------------------------------------------------------------
 * Eval mode — EVAL 模式（执行 Lua 脚本）
 *--------------------------------------------------------------------------- */

/* EVAL 模式主入口：加载脚本、调用 EVAL，必要时进入调试 REPL */
static int evalMode(int argc, char **argv) {
    sds script = NULL;
    FILE *fp;
    char buf[1024];
    size_t nread;
    char **argv2;
    int j, got_comma, keys;
    int retval = REDIS_OK;

    while(1) {
        if (config.eval_ldb) {
            printf(
            "Lua debugging session started, please use:\n"
            "quit    -- End the session.\n"
            "restart -- Restart the script in debug mode again.\n"
            "help    -- Show Lua script debugging commands.\n\n"
            );
        }

        sdsfree(script);
        script = sdsempty();
        got_comma = 0;
        keys = 0;

        /* 从文件加载脚本内容为 sds 字符串 */
        fp = fopen(config.eval,"r");
        if (!fp) {
            fprintf(stderr,
                "Can't open file '%s': %s\n", config.eval, strerror(errno));
            exit(1);
        }
        while((nread = fread(buf,1,sizeof(buf),fp)) != 0) {
            script = sdscatlen(script,buf,nread);
        }
        fclose(fp);

        /* 若处于调试模式则启用 Lua 调试器 */
        if (config.eval_ldb) {
            redisReply *reply = redisCommand(context,
                    config.eval_ldb_sync ?
                    "SCRIPT DEBUG sync": "SCRIPT DEBUG yes");
            if (reply) freeReplyObject(reply);
        }

        /* 构造 EVAL 命令的参数数组：
         * EVAL <script> <numkeys> <key1> ... <keyN> <arg1> ... <argN> */
        argv2 = zmalloc(sizeof(sds)*(argc+3));
        argv2[0] = sdsnew("EVAL");
        argv2[1] = script;
        for (j = 0; j < argc; j++) {
            if (!got_comma && argv[j][0] == ',' && argv[j][1] == 0) {
                /* ',' 分隔 KEYS 和 ARGV */
                got_comma = 1;
                continue;
            }
            argv2[j+3-got_comma] = sdsnew(argv[j]);
            if (!got_comma) keys++;
        }
        argv2[2] = sdscatprintf(sdsempty(),"%d",keys);

        /* 执行 EVAL */
        int eval_ldb = config.eval_ldb; /* 保存当前调试状态，可能被改写 */
        retval = issueCommand(argc+3-got_comma, argv2);
        if (eval_ldb) {
            if (!config.eval_ldb) {
                /* 调试会话立即结束说明脚本编译出错，
                 * 直接显示错误并不进入 REPL。 */
                printf("Eval debugging session can't start:\n");
                cliReadReply(0);
                break; /* 返回调用方 */
            } else {
                strncpy(config.prompt,"lua debugger> ",sizeof(config.prompt));
                repl();
                /* 若 repl() 返回则重启会话 */
                cliConnect(CC_FORCE);
                printf("\n");
            }
        } else {
            break; /* 返回调用方 */
        }
    }
    return retval == REDIS_OK ? 0 : 1;
}

/*------------------------------------------------------------------------------
 * Cluster Manager — 集群管理子系统
 *--------------------------------------------------------------------------- */

/* 集群管理器全局状态 */
static struct clusterManager {
    list *nodes;    /* 配置中的节点列表 */
    list *errors;
    int unreachable_masters;    /* 不可达的主节点数 */
} cluster_manager;

/* clusterManagerFixSlotsCoverage 使用的全局变量 */
dict *clusterManagerUncoveredSlots = NULL;

/* 集群管理节点结构 */
typedef struct clusterManagerNode {
    redisContext *context;
    sds name;
    char *ip;
    int port;
    int bus_port; /* 集群总线端口 */
    uint64_t current_epoch;
    time_t ping_sent;
    time_t ping_recv;
    int flags;
    list *flags_str; /* 标志的字符串形式 */
    sds replicate;  /* 若为从节点则为主节点 ID */
    int dirty;      /* 节点有尚未刷新的修改 */
    uint8_t slots[CLUSTER_MANAGER_SLOTS];
    int slots_count;
    int replicas_count;
    list *friends;
    sds *migrating; /* sds 数组，偶数下标是槽位、奇数下标是目标节点 ID */
    sds *importing; /* sds 数组，偶数下标是槽位、奇数下标是源节点 ID */
    int migrating_count; /* migrating 数组长度（迁移槽位数 × 2） */
    int importing_count; /* importing 数组长度（导入槽位数 × 2） */
    float weight;   /* rebalance 使用的权重 */
    int balance;    /* rebalance 内部使用 */
} clusterManagerNode;

/* Data structure used to represent a sequence of cluster nodes. */
typedef struct clusterManagerNodeArray {
    clusterManagerNode **nodes; /* Actual nodes array */
    clusterManagerNode **alloc; /* Pointer to the allocated memory */
    int len;                    /* Actual length of the array */
    int count;                  /* Non-NULL nodes count */
} clusterManagerNodeArray;

/* Used for the reshard table. */
typedef struct clusterManagerReshardTableItem {
    clusterManagerNode *source;
    int slot;
} clusterManagerReshardTableItem;

/* Info about a cluster internal link. */

typedef struct clusterManagerLink {
    sds node_name;
    sds node_addr;
    int connected;
    int handshaking;
} clusterManagerLink;

static dictType clusterManagerDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    dictSdsDestructor,         /* val destructor */
    NULL                       /* allow to expand */
};

static dictType clusterManagerLinkDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    dictListDestructor,        /* val destructor */
    NULL                       /* allow to expand */
};

typedef int clusterManagerCommandProc(int argc, char **argv);
typedef int (*clusterManagerOnReplyError)(redisReply *reply,
    clusterManagerNode *n, int bulk_idx);

/* Cluster Manager helper functions */

static clusterManagerNode *clusterManagerNewNode(char *ip, int port, int bus_port);
static clusterManagerNode *clusterManagerNodeByName(const char *name);
static clusterManagerNode *clusterManagerNodeByAbbreviatedName(const char *n);
static void clusterManagerNodeResetSlots(clusterManagerNode *node);
static int clusterManagerNodeIsCluster(clusterManagerNode *node, char **err);
static void clusterManagerPrintNotClusterNodeError(clusterManagerNode *node,
                                                   char *err);
static int clusterManagerNodeLoadInfo(clusterManagerNode *node, int opts,
                                      char **err);
static int clusterManagerLoadInfoFromNode(clusterManagerNode *node);
static int clusterManagerNodeIsEmpty(clusterManagerNode *node, char **err);
static int clusterManagerGetAntiAffinityScore(clusterManagerNodeArray *ipnodes,
    int ip_count, clusterManagerNode ***offending, int *offending_len);
static void clusterManagerOptimizeAntiAffinity(clusterManagerNodeArray *ipnodes,
    int ip_count);
static sds clusterManagerNodeInfo(clusterManagerNode *node, int indent);
static void clusterManagerShowNodes(void);
static void clusterManagerShowClusterInfo(void);
static int clusterManagerFlushNodeConfig(clusterManagerNode *node, char **err);
static void clusterManagerWaitForClusterJoin(void);
static int clusterManagerCheckCluster(int quiet);
static void clusterManagerLog(int level, const char* fmt, ...);
static int clusterManagerIsConfigConsistent(void);
static dict *clusterManagerGetLinkStatus(void);
static void clusterManagerOnError(sds err);
static void clusterManagerNodeArrayInit(clusterManagerNodeArray *array,
                                        int len);
static void clusterManagerNodeArrayReset(clusterManagerNodeArray *array);
static void clusterManagerNodeArrayShift(clusterManagerNodeArray *array,
                                         clusterManagerNode **nodeptr);
static void clusterManagerNodeArrayAdd(clusterManagerNodeArray *array,
                                       clusterManagerNode *node);

/* Cluster Manager commands. */

static int clusterManagerCommandCreate(int argc, char **argv);
static int clusterManagerCommandAddNode(int argc, char **argv);
static int clusterManagerCommandDeleteNode(int argc, char **argv);
static int clusterManagerCommandInfo(int argc, char **argv);
static int clusterManagerCommandCheck(int argc, char **argv);
static int clusterManagerCommandFix(int argc, char **argv);
static int clusterManagerCommandReshard(int argc, char **argv);
static int clusterManagerCommandRebalance(int argc, char **argv);
static int clusterManagerCommandSetTimeout(int argc, char **argv);
static int clusterManagerCommandImport(int argc, char **argv);
static int clusterManagerCommandCall(int argc, char **argv);
static int clusterManagerCommandHelp(int argc, char **argv);
static int clusterManagerCommandBackup(int argc, char **argv);

typedef struct clusterManagerCommandDef {
    char *name;
    clusterManagerCommandProc *proc;
    int arity;
    char *args;
    char *options;
} clusterManagerCommandDef;

clusterManagerCommandDef clusterManagerCommands[] = {
    {"create", clusterManagerCommandCreate, -1, "host1:port1 ... hostN:portN",
     "replicas <arg>"},
    {"check", clusterManagerCommandCheck, -1, "<host:port> or <host> <port> - separated by either colon or space",
     "search-multiple-owners"},
    {"info", clusterManagerCommandInfo, -1, "<host:port> or <host> <port> - separated by either colon or space", NULL},
    {"fix", clusterManagerCommandFix, -1, "<host:port> or <host> <port> - separated by either colon or space",
     "search-multiple-owners,fix-with-unreachable-masters"},
    {"reshard", clusterManagerCommandReshard, -1, "<host:port> or <host> <port> - separated by either colon or space",
     "from <arg>,to <arg>,slots <arg>,yes,timeout <arg>,pipeline <arg>,"
     "replace"},
    {"rebalance", clusterManagerCommandRebalance, -1, "<host:port> or <host> <port> - separated by either colon or space",
     "weight <node1=w1...nodeN=wN>,use-empty-masters,"
     "timeout <arg>,simulate,pipeline <arg>,threshold <arg>,replace"},
    {"add-node", clusterManagerCommandAddNode, 2,
     "new_host:new_port existing_host:existing_port", "slave,master-id <arg>"},
    {"del-node", clusterManagerCommandDeleteNode, 2, "host:port node_id",NULL},
    {"call", clusterManagerCommandCall, -2,
        "host:port command arg arg .. arg", "only-masters,only-replicas"},
    {"set-timeout", clusterManagerCommandSetTimeout, 2,
     "host:port milliseconds", NULL},
    {"import", clusterManagerCommandImport, 1, "host:port",
     "from <arg>,from-user <arg>,from-pass <arg>,from-askpass,copy,replace"},
    {"backup", clusterManagerCommandBackup, 2,  "host:port backup_directory",
     NULL},
    {"help", clusterManagerCommandHelp, 0, NULL, NULL}
};

typedef struct clusterManagerOptionDef {
    char *name;
    char *desc;
} clusterManagerOptionDef;

clusterManagerOptionDef clusterManagerOptions[] = {
    {"--cluster-yes", "Automatic yes to cluster commands prompts"}
};

static void getRDB(clusterManagerNode *node);

static int createClusterManagerCommand(char *cmdname, int argc, char **argv) {
    clusterManagerCommand *cmd = &config.cluster_manager_command;
    cmd->name = cmdname;
    cmd->argc = argc;
    cmd->argv = argc ? argv : NULL;
    if (isColorTerm()) cmd->flags |= CLUSTER_MANAGER_CMD_FLAG_COLOR;

    if (config.stdin_lastarg) {
        char **new_argv = zmalloc(sizeof(char*) * (cmd->argc+1));
        memcpy(new_argv, cmd->argv, sizeof(char*) * cmd->argc);

        cmd->stdin_arg = readArgFromStdin();
        new_argv[cmd->argc++] = cmd->stdin_arg;
        cmd->argv = new_argv;
    } else if (config.stdin_tag_arg) {
        int i = 0, tag_match = 0;
        cmd->stdin_arg = readArgFromStdin();

        for (; i < argc; i++) {
            if (strcmp(argv[i], config.stdin_tag_name) != 0) continue;

            tag_match = 1;
            cmd->argv[i] = (char *)cmd->stdin_arg;
            break;
        }

        if (!tag_match) {
            sdsfree(cmd->stdin_arg);
            fprintf(stderr, "Using -X option but stdin tag not match.\n");
            return 1;
        }
    }

    return 0;
}

static clusterManagerCommandProc *validateClusterManagerCommand(void) {
    int i, commands_count = sizeof(clusterManagerCommands) /
                            sizeof(clusterManagerCommandDef);
    clusterManagerCommandProc *proc = NULL;
    char *cmdname = config.cluster_manager_command.name;
    int argc = config.cluster_manager_command.argc;
    for (i = 0; i < commands_count; i++) {
        clusterManagerCommandDef cmddef = clusterManagerCommands[i];
        if (!strcmp(cmddef.name, cmdname)) {
            if ((cmddef.arity > 0 && argc != cmddef.arity) ||
                (cmddef.arity < 0 && argc < (cmddef.arity * -1))) {
                fprintf(stderr, "[ERR] Wrong number of arguments for "
                                "specified --cluster sub command\n");
                return NULL;
            }
            proc = cmddef.proc;
        }
    }
    if (!proc) fprintf(stderr, "Unknown --cluster subcommand\n");
    return proc;
}

/* 解析形如 ip:port[@bus_port] 的集群节点地址 */
static int parseClusterNodeAddress(char *addr, char **ip_ptr, int *port_ptr,
                                   int *bus_port_ptr)
{
    /* ip:port[@bus_port] */
    char *c = strrchr(addr, '@');
    if (c != NULL) {
        *c = '\0';
        if (bus_port_ptr != NULL)
            *bus_port_ptr = atoi(c + 1);
    }
    c = strrchr(addr, ':');
    if (c != NULL) {
        *c = '\0';
        *ip_ptr = addr;
        *port_ptr = atoi(++c);
    } else return 0;
    return 1;
}

/* 从命令行参数中解析主机 IP 和端口。
 * 若仅提供一个参数，则必须是 ip:port 格式；
 * 否则第一个参数为 IP，第二个参数为端口。
 * 解析成功返回 1 并通过 ip_ptr/port_ptr 输出；失败返回 0。 */
static int getClusterHostFromCmdArgs(int argc, char **argv,
                                     char **ip_ptr, int *port_ptr) {
    int port = 0;
    char *ip = NULL;
    if (argc == 1) {
        char *addr = argv[0];
        if (!parseClusterNodeAddress(addr, &ip, &port, NULL)) return 0;
    } else {
        ip = argv[0];
        port = atoi(argv[1]);
    }
    if (!ip || !port) return 0;
    else {
        *ip_ptr = ip;
        *port_ptr = port;
    }
    return 1;
}

/* 释放节点 flags 链表 */
static void freeClusterManagerNodeFlags(list *flags) {
    listIter li;
    listNode *ln;
    listRewind(flags, &li);
    while ((ln = listNext(&li)) != NULL) {
        sds flag = ln->value;
        sdsfree(flag);
    }
    listRelease(flags);
}

/* 递归释放集群管理节点及其 friends 子树 */
static void freeClusterManagerNode(clusterManagerNode *node) {
    if (node->context != NULL) redisFree(node->context);
    if (node->friends != NULL) {
        listIter li;
        listNode *ln;
        listRewind(node->friends,&li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *fn = ln->value;
            freeClusterManagerNode(fn);
        }
        listRelease(node->friends);
        node->friends = NULL;
    }
    if (node->name != NULL) sdsfree(node->name);
    if (node->replicate != NULL) sdsfree(node->replicate);
    if ((node->flags & CLUSTER_MANAGER_FLAG_FRIEND) && node->ip)
        sdsfree(node->ip);
    int i;
    if (node->migrating != NULL) {
        for (i = 0; i < node->migrating_count; i++) sdsfree(node->migrating[i]);
        zfree(node->migrating);
    }
    if (node->importing != NULL) {
        for (i = 0; i < node->importing_count; i++) sdsfree(node->importing[i]);
        zfree(node->importing);
    }
    if (node->flags_str != NULL) {
        freeClusterManagerNodeFlags(node->flags_str);
        node->flags_str = NULL;
    }
    zfree(node);
}

/* 释放整个集群管理器全局状态 */
static void freeClusterManager(void) {
    listIter li;
    listNode *ln;
    if (cluster_manager.nodes != NULL) {
        listRewind(cluster_manager.nodes,&li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = ln->value;
            freeClusterManagerNode(n);
        }
        listRelease(cluster_manager.nodes);
        cluster_manager.nodes = NULL;
    }
    if (cluster_manager.errors != NULL) {
        listRewind(cluster_manager.errors,&li);
        while ((ln = listNext(&li)) != NULL) {
            sds err = ln->value;
            sdsfree(err);
        }
        listRelease(cluster_manager.errors);
        cluster_manager.errors = NULL;
    }
    if (clusterManagerUncoveredSlots != NULL)
        dictRelease(clusterManagerUncoveredSlots);
}

/* 创建一个新的集群节点对象（默认值） */
static clusterManagerNode *clusterManagerNewNode(char *ip, int port, int bus_port) {
    clusterManagerNode *node = zmalloc(sizeof(*node));
    node->context = NULL;
    node->name = NULL;
    node->ip = ip;
    node->port = port;
    /* 此时 bus_port 可能不正确，若被使用会在 clusterManagerLoadInfoFromNode
     * 中被纠正。 */
    node->bus_port = bus_port ? bus_port : port + CLUSTER_MANAGER_PORT_INCR;
    node->current_epoch = 0;
    node->ping_sent = 0;
    node->ping_recv = 0;
    node->flags = 0;
    node->flags_str = NULL;
    node->replicate = NULL;
    node->dirty = 0;
    node->friends = NULL;
    node->migrating = NULL;
    node->importing = NULL;
    node->migrating_count = 0;
    node->importing_count = 0;
    node->replicas_count = 0;
    node->weight = 1.0f;
    node->balance = 0;
    clusterManagerNodeResetSlots(node);
    return node;
}

/* 构造集群节点 RDB 备份文件的完整路径 */
static sds clusterManagerGetNodeRDBFilename(clusterManagerNode *node) {
    assert(config.cluster_manager_command.backup_dir);
    sds filename = sdsnew(config.cluster_manager_command.backup_dir);
    if (filename[sdslen(filename) - 1] != '/')
        filename = sdscat(filename, "/");
    filename = sdscatprintf(filename, "redis-node-%s-%d-%s.rdb", node->ip,
                            node->port, node->name);
    return filename;
}

/* 检查 reply 是否为 NULL 或 REDIS_REPLY_ERROR。
 * 若为错误且 err 不为 NULL，则将错误字符串复制到 err（由调用方释放）；
 * 否则直接打印错误。 */
static int clusterManagerCheckRedisReply(clusterManagerNode *n,
                                         redisReply *r, char **err)
{
    int is_err = 0;
    if (!r || (is_err = (r->type == REDIS_REPLY_ERROR))) {
        if (is_err) {
            if (err != NULL) {
                *err = zmalloc((r->len + 1) * sizeof(char));
                redis_strlcpy(*err, r->str,(r->len + 1));
            } else CLUSTER_MANAGER_PRINT_REPLY_ERROR(n, r->str);
        }
        return 0;
    }
    return 1;
}

/* 在指定集群节点上执行 MULTI 命令，开启事务 */
static int clusterManagerStartTransaction(clusterManagerNode *node) {
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node, "MULTI");
    int success = clusterManagerCheckRedisReply(node, reply, NULL);
    if (reply) freeReplyObject(reply);
    return success;
}

/* 在指定集群节点上执行 EXEC 命令，提交事务 */
static int clusterManagerExecTransaction(clusterManagerNode *node,
                                         clusterManagerOnReplyError onerror)
{
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node, "EXEC");
    int success = clusterManagerCheckRedisReply(node, reply, NULL);
    if (success) {
        if (reply->type != REDIS_REPLY_ARRAY) {
            success = 0;
            goto cleanup;
        }
        size_t i;
        for (i = 0; i < reply->elements; i++) {
            redisReply *r = reply->element[i];
            char *err = NULL;
            success = clusterManagerCheckRedisReply(node, r, &err);
            if (!success && onerror) success = onerror(r, node, i);
            if (err) {
                if (!success)
                    CLUSTER_MANAGER_PRINT_REPLY_ERROR(node, err);
                zfree(err);
            }
            if (!success) break;
        }
    }
cleanup:
    if (reply) freeReplyObject(reply);
    return success;
}

/* 建立到指定集群节点的 Redis 连接（含 TLS 与 AUTH） */
static int clusterManagerNodeConnect(clusterManagerNode *node) {
    if (node->context) redisFree(node->context);
    node->context = redisConnectWrapper(node->ip, node->port, config.connect_timeout);
    if (!node->context->err && config.tls) {
        const char *err = NULL;
        if (cliSecureConnection(node->context, config.sslconfig, &err) == REDIS_ERR && err) {
            fprintf(stderr,"TLS Error: %s\n", err);
            redisFree(node->context);
            node->context = NULL;
            return 0;
        }
    }
    if (node->context->err) {
        fprintf(stderr,"Could not connect to Redis at ");
        fprintf(stderr,"%s:%d: %s\n", node->ip, node->port,
                node->context->errstr);
        redisFree(node->context);
        node->context = NULL;
        return 0;
    }
    /* 在 Redis 上下文套接字上设置激进的 KEEP_ALIVE 选项，
     * 以避免长命令执行超时，也有助于及时发现真实错误。 */
    anetKeepAlive(NULL, node->context->fd, REDIS_CLI_KEEPALIVE_INTERVAL);
    if (config.conn_info.auth) {
        redisReply *reply;
        if (config.conn_info.user == NULL)
            reply = redisCommand(node->context,"AUTH %s", config.conn_info.auth);
        else
            reply = redisCommand(node->context,"AUTH %s %s",
                                 config.conn_info.user,config.conn_info.auth);
        int ok = clusterManagerCheckRedisReply(node, reply, NULL);
        if (reply != NULL) freeReplyObject(reply);
        if (!ok) return 0;
    }
    return 1;
}

/* 从链表中移除指定节点 */
static void clusterManagerRemoveNodeFromList(list *nodelist,
                                             clusterManagerNode *node) {
    listIter li;
    listNode *ln;
    listRewind(nodelist, &li);
    while ((ln = listNext(&li)) != NULL) {
        if (node == ln->value) {
            listDelNode(nodelist, ln);
            break;
        }
    }
}

/* 按完整名称（节点 ID）查找节点；未找到返回 NULL */
static clusterManagerNode *clusterManagerNodeByName(const char *name) {
    if (cluster_manager.nodes == NULL) return NULL;
    clusterManagerNode *found = NULL;
    sds lcname = sdsempty();
    lcname = sdscpy(lcname, name);
    sdstolower(lcname);
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->name && !sdscmp(n->name, lcname)) {
            found = n;
            break;
        }
    }
    sdsfree(lcname);
    return found;
}

/* 类似 clusterManagerNodeByName，但允许使用节点 ID 的前缀，
 * 只要该前缀在集群中唯一。 */
static clusterManagerNode *clusterManagerNodeByAbbreviatedName(const char*name)
{
    if (cluster_manager.nodes == NULL) return NULL;
    clusterManagerNode *found = NULL;
    sds lcname = sdsempty();
    lcname = sdscpy(lcname, name);
    sdstolower(lcname);
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->name &&
            strstr(n->name, lcname) == n->name) {
            found = n;
            break;
        }
    }
    sdsfree(lcname);
    return found;
}

/* 重置节点的槽位数组与计数 */
static void clusterManagerNodeResetSlots(clusterManagerNode *node) {
    memset(node->slots, 0, sizeof(node->slots));
    node->slots_count = 0;
}

/* 在指定节点上执行 INFO 命令并返回 reply */
static redisReply *clusterManagerGetNodeRedisInfo(clusterManagerNode *node,
                                                  char **err)
{
    redisReply *info = CLUSTER_MANAGER_COMMAND(node, "INFO");
    if (err != NULL) *err = NULL;
    if (info == NULL) return NULL;
    if (info->type == REDIS_REPLY_ERROR) {
        if (err != NULL) {
            *err = zmalloc((info->len + 1) * sizeof(char));
            redis_strlcpy(*err, info->str,(info->len + 1));
        }
        freeReplyObject(info);
        return  NULL;
    }
    return info;
}

/* 判断指定节点是否启用了集群模式 */
static int clusterManagerNodeIsCluster(clusterManagerNode *node, char **err) {
    redisReply *info = clusterManagerGetNodeRedisInfo(node, err);
    if (info == NULL) return 0;
    int is_cluster = (int) getLongInfoField(info->str, "cluster_enabled");
    freeReplyObject(info);
    return is_cluster;
}

/* 检查节点是否为空：节点有任何 key 或已知其他节点都视为非空 */
static int clusterManagerNodeIsEmpty(clusterManagerNode *node, char **err) {
    redisReply *info = clusterManagerGetNodeRedisInfo(node, err);
    int is_empty = 1;
    if (info == NULL) return 0;
    if (strstr(info->str, "db0:") != NULL) {
        is_empty = 0;
        goto result;
    }
    freeReplyObject(info);
    info = CLUSTER_MANAGER_COMMAND(node, "CLUSTER INFO");
    if (err != NULL) *err = NULL;
    if (!clusterManagerCheckRedisReply(node, info, err)) {
        is_empty = 0;
        goto result;
    }
    long known_nodes = getLongInfoField(info->str, "cluster_known_nodes");
    is_empty = (known_nodes == 1);
result:
    freeReplyObject(info);
    return is_empty;
}

/* Return the anti-affinity score, which is a measure of the amount of
 * violations of anti-affinity in the current cluster layout, that is, how
 * badly the masters and slaves are distributed in the different IP
 * addresses so that slaves of the same master are not in the master
 * host and are also in different hosts.
 *
 * The score is calculated as follows:
 *
 * SAME_AS_MASTER = 10000 * each slave in the same IP of its master.
 * SAME_AS_SLAVE  = 1 * each slave having the same IP as another slave
                      of the same master.
 * FINAL_SCORE = SAME_AS_MASTER + SAME_AS_SLAVE
 *
 * So a greater score means a worse anti-affinity level, while zero
 * means perfect anti-affinity.
 *
 * The anti affinity optimization will try to get a score as low as
 * possible. Since we do not want to sacrifice the fact that slaves should
 * not be in the same host as the master, we assign 10000 times the score
 * to this violation, so that we'll optimize for the second factor only
 * if it does not impact the first one.
 *
 * The ipnodes argument is an array of clusterManagerNodeArray, one for
 * each IP, while ip_count is the total number of IPs in the configuration.
 *
 * The function returns the above score, and the list of
 * offending slaves can be stored into the 'offending' argument,
 * so that the optimizer can try changing the configuration of the
 * slaves violating the anti-affinity goals. */
static int clusterManagerGetAntiAffinityScore(clusterManagerNodeArray *ipnodes,
    int ip_count, clusterManagerNode ***offending, int *offending_len)
{
    int score = 0, i, j;
    int node_len = cluster_manager.nodes->len;
    clusterManagerNode **offending_p = NULL;
    if (offending != NULL) {
        *offending = zcalloc(node_len * sizeof(clusterManagerNode*));
        offending_p = *offending;
    }
    /* 对同一主机内的每组节点，按相关节点（互相复制的
     * 主从节点）分组计算。 */
    for (i = 0; i < ip_count; i++) {
        clusterManagerNodeArray *node_array = &(ipnodes[i]);
        dict *related = dictCreate(&clusterManagerDictType);
        char *ip = NULL;
        for (j = 0; j < node_array->len; j++) {
            clusterManagerNode *node = node_array->nodes[j];
            if (node == NULL) continue;
            if (!ip) ip = node->ip;
            sds types;
            /* 始终使用主节点 ID 作为 key */
            sds key = (!node->replicate ? node->name : node->replicate);
            assert(key != NULL);
            dictEntry *entry = dictFind(related, key);
            if (entry) types = sdsdup((sds) dictGetVal(entry));
            else types = sdsempty();
            /* 主节点类型 'm' 总是作为 types 字符串的第一个字符 */
            if (node->replicate) types = sdscat(types, "s");
            else {
                sds s = sdscatsds(sdsnew("m"), types);
                sdsfree(types);
                types = s;
            }
            dictReplace(related, key, types);
        }
        /* 现在很容易为同一主机的每个相关组计算本地得分 */
        dictIterator *iter = dictGetIterator(related);
        dictEntry *entry;
        while ((entry = dictNext(iter)) != NULL) {
            sds types = (sds) dictGetVal(entry);
            sds name = (sds) dictGetKey(entry);
            int typeslen = sdslen(types);
            if (typeslen < 2) continue;
            if (types[0] == 'm') score += (10000 * (typeslen - 1));
            else score += (1 * typeslen);
            if (offending == NULL) continue;
            /* 填充违规节点列表 */
            listIter li;
            listNode *ln;
            listRewind(cluster_manager.nodes, &li);
            while ((ln = listNext(&li)) != NULL) {
                clusterManagerNode *n = ln->value;
                if (n->replicate == NULL) continue;
                if (!strcmp(n->replicate, name) && !strcmp(n->ip, ip)) {
                    *(offending_p++) = n;
                    if (offending_len != NULL) (*offending_len)++;
                    break;
                }
            }
        }
        //if (offending_len != NULL) *offending_len = offending_p - *offending;
        dictReleaseIterator(iter);
        dictRelease(related);
    }
    return score;
}

/* 通过随机交换主从关系优化 anti-affinity 评分 */
static void clusterManagerOptimizeAntiAffinity(clusterManagerNodeArray *ipnodes,
    int ip_count)
{
    clusterManagerNode **offenders = NULL;
    int score = clusterManagerGetAntiAffinityScore(ipnodes, ip_count,
                                                   NULL, NULL);
    if (score == 0) goto cleanup;
    clusterManagerLogInfo(">>> Trying to optimize slaves allocation "
                          "for anti-affinity\n");
    int node_len = cluster_manager.nodes->len;
    int maxiter = 500 * node_len; // 迭代次数与集群规模成正比
    srand(time(NULL));
    while (maxiter > 0) {
        int offending_len = 0;
        if (offenders != NULL) {
            zfree(offenders);
            offenders = NULL;
        }
        score = clusterManagerGetAntiAffinityScore(ipnodes,
                                                   ip_count,
                                                   &offenders,
                                                   &offending_len);
        if (score == 0 || offending_len == 0) break; // 已达到最佳 anti-affinity
        /* 尝试随机交换一个造成亲和性问题的从节点的主节点，
         * 看看是否能改善亲和性。 */
        int rand_idx = rand() % offending_len;
        clusterManagerNode *first = offenders[rand_idx],
                           *second = NULL;
        clusterManagerNode **other_replicas = zcalloc((node_len - 1) *
                                                      sizeof(*other_replicas));
        int other_replicas_count = 0;
        listIter li;
        listNode *ln;
        listRewind(cluster_manager.nodes, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = ln->value;
            if (n != first && n->replicate != NULL)
                other_replicas[other_replicas_count++] = n;
        }
        if (other_replicas_count == 0) {
            zfree(other_replicas);
            break;
        }
        rand_idx = rand() % other_replicas_count;
        second = other_replicas[rand_idx];
        char *first_master = first->replicate,
             *second_master = second->replicate;
        first->replicate = second_master, first->dirty = 1;
        second->replicate = first_master, second->dirty = 1;
        int new_score = clusterManagerGetAntiAffinityScore(ipnodes,
                                                           ip_count,
                                                           NULL, NULL);
        /* 若变更使情况变差则回滚；否则保留，因为最优解
         * 可能需要多次组合交换。 */
        if (new_score > score) {
            first->replicate = first_master;
            second->replicate = second_master;
        }
        zfree(other_replicas);
        maxiter--;
    }
    score = clusterManagerGetAntiAffinityScore(ipnodes, ip_count, NULL, NULL);
    char *msg;
    int perfect = (score == 0);
    int log_level = (perfect ? CLUSTER_MANAGER_LOG_LVL_SUCCESS :
                               CLUSTER_MANAGER_LOG_LVL_WARN);
    if (perfect) msg = "[OK] Perfect anti-affinity obtained!";
    else if (score >= 10000)
        msg = ("[WARNING] Some slaves are in the same host as their master");
    else
        msg=("[WARNING] Some slaves of the same master are in the same host");
    clusterManagerLog(log_level, "%s\n", msg);
cleanup:
    zfree(offenders);
}

/* 返回节点 flags 的字符串表示 */
static sds clusterManagerNodeFlagString(clusterManagerNode *node) {
    sds flags = sdsempty();
    if (!node->flags_str) return flags;
    int empty = 1;
    listIter li;
    listNode *ln;
    listRewind(node->flags_str, &li);
    while ((ln = listNext(&li)) != NULL) {
        sds flag = ln->value;
        if (strcmp(flag, "myself") == 0) continue;
        if (!empty) flags = sdscat(flags, ",");
        flags = sdscatfmt(flags, "%S", flag);
        empty = 0;
    }
    return flags;
}

/* 返回节点槽位的字符串表示（如 [0-3,5,7-10]） */
static sds clusterManagerNodeSlotsString(clusterManagerNode *node) {
    sds slots = sdsempty();
    int first_range_idx = -1, last_slot_idx = -1, i;
    for (i = 0; i < CLUSTER_MANAGER_SLOTS; i++) {
        int has_slot = node->slots[i];
        if (has_slot) {
            if (first_range_idx == -1) {
                if (sdslen(slots)) slots = sdscat(slots, ",");
                first_range_idx = i;
                slots = sdscatfmt(slots, "[%u", i);
            }
            last_slot_idx = i;
        } else {
            if (last_slot_idx >= 0) {
                if (first_range_idx == last_slot_idx)
                    slots = sdscat(slots, "]");
                else slots = sdscatfmt(slots, "-%u]", last_slot_idx);
            }
            last_slot_idx = -1;
            first_range_idx = -1;
        }
    }
    if (last_slot_idx >= 0) {
        if (first_range_idx == last_slot_idx) slots = sdscat(slots, "]");
        else slots = sdscatfmt(slots, "-%u]", last_slot_idx);
    }
    return slots;
}

static sds clusterManagerNodeGetJSON(clusterManagerNode *node,
                                     unsigned long error_count)
{
    sds json = sdsempty();
    sds replicate = sdsempty();
    if (node->replicate)
        replicate = sdscatprintf(replicate, "\"%s\"", node->replicate);
    else
        replicate = sdscat(replicate, "null");
    sds slots = clusterManagerNodeSlotsString(node);
    sds flags = clusterManagerNodeFlagString(node);
    char *p = slots;
    while ((p = strchr(p, '-')) != NULL)
        *(p++) = ',';
    json = sdscatprintf(json,
        "  {\n"
        "    \"name\": \"%s\",\n"
        "    \"host\": \"%s\",\n"
        "    \"port\": %d,\n"
        "    \"replicate\": %s,\n"
        "    \"slots\": [%s],\n"
        "    \"slots_count\": %d,\n"
        "    \"flags\": \"%s\",\n"
        "    \"current_epoch\": %llu",
        node->name,
        node->ip,
        node->port,
        replicate,
        slots,
        node->slots_count,
        flags,
        (unsigned long long)node->current_epoch
    );
    if (error_count > 0) {
        json = sdscatprintf(json, ",\n    \"cluster_errors\": %lu",
                            error_count);
    }
    if (node->migrating_count > 0 && node->migrating != NULL) {
        int i = 0;
        sds migrating = sdsempty();
        for (; i < node->migrating_count; i += 2) {
            sds slot = node->migrating[i];
            sds dest = node->migrating[i + 1];
            if (slot && dest) {
                if (sdslen(migrating) > 0) migrating = sdscat(migrating, ",");
                migrating = sdscatfmt(migrating, "\"%S\": \"%S\"", slot, dest);
            }
        }
        if (sdslen(migrating) > 0)
            json = sdscatfmt(json, ",\n    \"migrating\": {%S}", migrating);
        sdsfree(migrating);
    }
    if (node->importing_count > 0 && node->importing != NULL) {
        int i = 0;
        sds importing = sdsempty();
        for (; i < node->importing_count; i += 2) {
            sds slot = node->importing[i];
            sds from = node->importing[i + 1];
            if (slot && from) {
                if (sdslen(importing) > 0) importing = sdscat(importing, ",");
                importing = sdscatfmt(importing, "\"%S\": \"%S\"", slot, from);
            }
        }
        if (sdslen(importing) > 0)
            json = sdscatfmt(json, ",\n    \"importing\": {%S}", importing);
        sdsfree(importing);
    }
    json = sdscat(json, "\n  }");
    sdsfree(replicate);
    sdsfree(slots);
    sdsfree(flags);
    return json;
}


/* -----------------------------------------------------------------------------
 * Key space handling — 键空间处理（CRC16 与 hash slot 计算）
 * -------------------------------------------------------------------------- */

/* 共有 16384 个哈希槽。key 的 hash slot 由 key 的
 * crc16 的低 14 位计算得到。
 *
 * 若 key 包含 {...} 模式，则只对 {} 之间的部分计算哈希。
 * 该规则未来可用于强制某些 key 位于同一节点
 * （前提是没有 reshard 进行中）。 */
static unsigned int clusterManagerKeyHashSlot(char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key,keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s+1) return crc16(key,keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return crc16(key+s+1,e-s-1) & 0x3FFF;
}

/* 返回集群节点的字符串表示 */
static sds clusterManagerNodeInfo(clusterManagerNode *node, int indent) {
    sds info = sdsempty();
    sds spaces = sdsempty();
    int i;
    for (i = 0; i < indent; i++) spaces = sdscat(spaces, " ");
    if (indent) info = sdscat(info, spaces);
    int is_master = !(node->flags & CLUSTER_MANAGER_FLAG_SLAVE);
    char *role = (is_master ? "M" : "S");
    sds slots = NULL;
    if (node->dirty && node->replicate != NULL)
        info = sdscatfmt(info, "S: %S %s:%u", node->name, node->ip, node->port);
    else {
        slots = clusterManagerNodeSlotsString(node);
        sds flags = clusterManagerNodeFlagString(node);
        info = sdscatfmt(info, "%s: %S %s:%u\n"
                               "%s   slots:%S (%u slots) "
                               "%S",
                               role, node->name, node->ip, node->port, spaces,
                               slots, node->slots_count, flags);
        sdsfree(slots);
        sdsfree(flags);
    }
    if (node->replicate != NULL)
        info = sdscatfmt(info, "\n%s   replicates %S", spaces, node->replicate);
    else if (node->replicas_count)
        info = sdscatfmt(info, "\n%s   %U additional replica(s)",
                         spaces, node->replicas_count);
    sdsfree(spaces);
    return info;
}

static void clusterManagerShowNodes(void) {
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *node = ln->value;
        sds info = clusterManagerNodeInfo(node, 0);
        printf("%s\n", (char *) info);
        sdsfree(info);
    }
}

static void clusterManagerShowClusterInfo(void) {
    int masters = 0;
    long long keys = 0;
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *node = ln->value;
        if (!(node->flags & CLUSTER_MANAGER_FLAG_SLAVE)) {
            if (!node->name) continue;
            int replicas = 0;
            long long dbsize = -1;
            char name[9];
            memcpy(name, node->name, 8);
            name[8] = '\0';
            listIter ri;
            listNode *rn;
            listRewind(cluster_manager.nodes, &ri);
            while ((rn = listNext(&ri)) != NULL) {
                clusterManagerNode *n = rn->value;
                if (n == node || !(n->flags & CLUSTER_MANAGER_FLAG_SLAVE))
                    continue;
                if (n->replicate && !strcmp(n->replicate, node->name))
                    replicas++;
            }
            redisReply *reply = CLUSTER_MANAGER_COMMAND(node, "DBSIZE");
            if (reply != NULL && reply->type == REDIS_REPLY_INTEGER)
                dbsize = reply->integer;
            if (dbsize < 0) {
                char *err = "";
                if (reply != NULL && reply->type == REDIS_REPLY_ERROR)
                    err = reply->str;
                CLUSTER_MANAGER_PRINT_REPLY_ERROR(node, err);
                if (reply != NULL) freeReplyObject(reply);
                return;
            };
            if (reply != NULL) freeReplyObject(reply);
            printf("%s:%d (%s...) -> %lld keys | %d slots | %d slaves.\n",
                   node->ip, node->port, name, dbsize,
                   node->slots_count, replicas);
            masters++;
            keys += dbsize;
        }
    }
    clusterManagerLogOk("[OK] %lld keys in %d masters.\n", keys, masters);
    float keys_per_slot = keys / (float) CLUSTER_MANAGER_SLOTS;
    printf("%.2f keys per slot on average.\n", keys_per_slot);
}

/* 通过执行 CLUSTER ADDSLOTS 将节点的脏槽位配置刷到服务器 */
static int clusterManagerAddSlots(clusterManagerNode *node, char**err)
{
    redisReply *reply = NULL;
    void *_reply = NULL;
    int success = 1;
    /* 前两个参数为命令本身 */
    int argc = node->slots_count + 2;
    sds *argv = zmalloc(argc * sizeof(*argv));
    size_t *argvlen = zmalloc(argc * sizeof(*argvlen));
    argv[0] = "CLUSTER";
    argv[1] = "ADDSLOTS";
    argvlen[0] = 7;
    argvlen[1] = 8;
    *err = NULL;
    int i, argv_idx = 2;
    for (i = 0; i < CLUSTER_MANAGER_SLOTS; i++) {
        if (argv_idx >= argc) break;
        if (node->slots[i]) {
            argv[argv_idx] = sdsfromlonglong((long long) i);
            argvlen[argv_idx] = sdslen(argv[argv_idx]);
            argv_idx++;
        }
    }
    if (argv_idx == 2) {
        success = 0;
        goto cleanup;
    }
    redisAppendCommandArgv(node->context,argc,(const char**)argv,argvlen);
    if (redisGetReply(node->context, &_reply) != REDIS_OK) {
        success = 0;
        goto cleanup;
    }
    reply = (redisReply*) _reply;
    success = clusterManagerCheckRedisReply(node, reply, err);
cleanup:
    zfree(argvlen);
    if (argv != NULL) {
        for (i = 2; i < argc; i++) sdsfree(argv[i]);
        zfree(argv);
    }
    if (reply != NULL) freeReplyObject(reply);
    return success;
}

/* 从节点 *n 的视角获取槽位的拥有者节点。
 * 若槽位未分配或回复为错误则返回 NULL。
 * 可通过 **err 区分槽位未分配与回复错误。 */
static clusterManagerNode *clusterManagerGetSlotOwner(clusterManagerNode *n,
                                                      int slot, char **err)
{
    assert(slot >= 0 && slot < CLUSTER_MANAGER_SLOTS);
    clusterManagerNode *owner = NULL;
    redisReply *reply = CLUSTER_MANAGER_COMMAND(n, "CLUSTER SLOTS");
    if (clusterManagerCheckRedisReply(n, reply, err)) {
        assert(reply->type == REDIS_REPLY_ARRAY);
        size_t i;
        for (i = 0; i < reply->elements; i++) {
            redisReply *r = reply->element[i];
            assert(r->type == REDIS_REPLY_ARRAY && r->elements >= 3);
            int from, to;
            from = r->element[0]->integer;
            to = r->element[1]->integer;
            if (slot < from || slot > to) continue;
            redisReply *nr =  r->element[2];
            assert(nr->type == REDIS_REPLY_ARRAY && nr->elements >= 2);
            char *name = NULL;
            if (nr->elements >= 3)
                name =  nr->element[2]->str;
            if (name != NULL)
                owner = clusterManagerNodeByName(name);
            else {
                char *ip = nr->element[0]->str;
                assert(ip != NULL);
                int port = (int) nr->element[1]->integer;
                listIter li;
                listNode *ln;
                listRewind(cluster_manager.nodes, &li);
                while ((ln = listNext(&li)) != NULL) {
                    clusterManagerNode *nd = ln->value;
                    if (strcmp(nd->ip, ip) == 0 && port == nd->port) {
                        owner = nd;
                        break;
                    }
                }
            }
            if (owner) break;
        }
    }
    if (reply) freeReplyObject(reply);
    return owner;
}

/* 设置槽位状态为 "importing" 或 "migrating" */
static int clusterManagerSetSlot(clusterManagerNode *node1,
                                 clusterManagerNode *node2,
                                 int slot, const char *status, char **err) {
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node1, "CLUSTER "
                                                "SETSLOT %d %s %s",
                                                slot, status,
                                                (char *) node2->name);
    if (err != NULL) *err = NULL;
    if (!reply) {
        if (err) *err = zstrdup("CLUSTER SETSLOT failed to run");
        return 0;
    }
    int success = 1;
    if (reply->type == REDIS_REPLY_ERROR) {
        success = 0;
        if (err != NULL) {
            *err = zmalloc((reply->len + 1) * sizeof(char));
            redis_strlcpy(*err, reply->str,(reply->len + 1));
        } else CLUSTER_MANAGER_PRINT_REPLY_ERROR(node1, reply->str);
        goto cleanup;
    }
cleanup:
    freeReplyObject(reply);
    return success;
}

/* 将槽位状态清除为 STABLE */
static int clusterManagerClearSlotStatus(clusterManagerNode *node, int slot) {
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node,
        "CLUSTER SETSLOT %d %s", slot, "STABLE");
    int success = clusterManagerCheckRedisReply(node, reply, NULL);
    if (reply) freeReplyObject(reply);
    return success;
}

/* 删除节点上的指定槽位分配；ignore_unassigned_err 为 1 时忽略未分配错误 */
static int clusterManagerDelSlot(clusterManagerNode *node, int slot,
                                 int ignore_unassigned_err)
{
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node,
        "CLUSTER DELSLOTS %d", slot);
    char *err = NULL;
    int success = clusterManagerCheckRedisReply(node, reply, &err);
    if (!success && reply && reply->type == REDIS_REPLY_ERROR &&
        ignore_unassigned_err)
    {
        char *get_owner_err = NULL;
        clusterManagerNode *assigned_to =
            clusterManagerGetSlotOwner(node, slot, &get_owner_err);
        if (!assigned_to) {
            if (get_owner_err == NULL) success = 1;
            else {
                CLUSTER_MANAGER_PRINT_REPLY_ERROR(node, get_owner_err);
                zfree(get_owner_err);
            }
        }
    }
    if (!success && err != NULL) {
        CLUSTER_MANAGER_PRINT_REPLY_ERROR(node, err);
        zfree(err);
    }
    if (reply) freeReplyObject(reply);
    return success;
}

/* 在节点上添加单个槽位 */
static int clusterManagerAddSlot(clusterManagerNode *node, int slot) {
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node,
        "CLUSTER ADDSLOTS %d", slot);
    int success = clusterManagerCheckRedisReply(node, reply, NULL);
    if (reply) freeReplyObject(reply);
    return success;
}

/* 查询节点指定槽位中的 key 数量 */
static signed int clusterManagerCountKeysInSlot(clusterManagerNode *node,
                                                int slot)
{
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node,
        "CLUSTER COUNTKEYSINSLOT %d", slot);
    int count = -1;
    int success = clusterManagerCheckRedisReply(node, reply, NULL);
    if (success && reply->type == REDIS_REPLY_INTEGER) count = reply->integer;
    if (reply) freeReplyObject(reply);
    return count;
}

/* 在节点上执行 CLUSTER BUMPEPOCH，提升集群 epoch */
static int clusterManagerBumpEpoch(clusterManagerNode *node) {
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node, "CLUSTER BUMPEPOCH");
    int success = clusterManagerCheckRedisReply(node, reply, NULL);
    if (reply) freeReplyObject(reply);
    return success;
}

/* clusterManagerSetSlotOwner 事务使用的回调：仅忽略
 * 非 ADDSLOTS 错误。返回 1 表示应忽略该错误。 */
static int clusterManagerOnSetOwnerErr(redisReply *reply,
    clusterManagerNode *n, int bulk_idx)
{
    UNUSED(reply);
    UNUSED(n);
    /* 仅当 ADDSLOTS 失败（bulk_idx == 1）时上报错误 */
    return (bulk_idx != 1);
}

/* 在事务中把槽位所有权赋予指定 owner */
static int clusterManagerSetSlotOwner(clusterManagerNode *owner,
                                      int slot,
                                      int do_clear)
{
    int success = clusterManagerStartTransaction(owner);
    if (!success) return 0;
    /* 确保槽位当前未被分配 */
    clusterManagerDelSlot(owner, slot, 1);
    /* 添加槽位并 bump epoch */
    clusterManagerAddSlot(owner, slot);
    if (do_clear) clusterManagerClearSlotStatus(owner, slot);
    clusterManagerBumpEpoch(owner);
    success = clusterManagerExecTransaction(owner, clusterManagerOnSetOwnerErr);
    return success;
}

/* 在两个节点 n1、n2 上通过 DEBUG DIGEST-VALUE 命令对 keys_reply 中
 * 指定 key 的值进行哈希计算。两个节点上同名但哈希不同的 key
 * 会被加入 *diffs 列表。reply 出错时返回 0。 */
static int clusterManagerCompareKeysValues(clusterManagerNode *n1,
                                          clusterManagerNode *n2,
                                          redisReply *keys_reply,
                                          list *diffs)
{
    size_t i, argc = keys_reply->elements + 2;
    static const char *hash_zero = "0000000000000000000000000000000000000000";
    char **argv = zcalloc(argc * sizeof(char *));
    size_t  *argv_len = zcalloc(argc * sizeof(size_t));
    argv[0] = "DEBUG";
    argv_len[0] = 5;
    argv[1] = "DIGEST-VALUE";
    argv_len[1] = 12;
    for (i = 0; i < keys_reply->elements; i++) {
        redisReply *entry = keys_reply->element[i];
        int idx = i + 2;
        argv[idx] = entry->str;
        argv_len[idx] = entry->len;
    }
    int success = 0;
    void *_reply1 = NULL, *_reply2 = NULL;
    redisReply *r1 = NULL, *r2 = NULL;
    redisAppendCommandArgv(n1->context,argc, (const char**)argv,argv_len);
    success = (redisGetReply(n1->context, &_reply1) == REDIS_OK);
    if (!success) goto cleanup;
    r1 = (redisReply *) _reply1;
    redisAppendCommandArgv(n2->context,argc, (const char**)argv,argv_len);
    success = (redisGetReply(n2->context, &_reply2) == REDIS_OK);
    if (!success) goto cleanup;
    r2 = (redisReply *) _reply2;
    success = (r1->type != REDIS_REPLY_ERROR && r2->type != REDIS_REPLY_ERROR);
    if (r1->type == REDIS_REPLY_ERROR) {
        CLUSTER_MANAGER_PRINT_REPLY_ERROR(n1, r1->str);
        success = 0;
    }
    if (r2->type == REDIS_REPLY_ERROR) {
        CLUSTER_MANAGER_PRINT_REPLY_ERROR(n2, r2->str);
        success = 0;
    }
    if (!success) goto cleanup;
    assert(keys_reply->elements == r1->elements &&
           keys_reply->elements == r2->elements);
    for (i = 0; i < keys_reply->elements; i++) {
        char *key = keys_reply->element[i]->str;
        char *hash1 = r1->element[i]->str;
        char *hash2 = r2->element[i]->str;
        /* Ignore keys that don't exist in both nodes. */
        if (strcmp(hash1, hash_zero) == 0 || strcmp(hash2, hash_zero) == 0)
            continue;
        if (strcmp(hash1, hash2) != 0) listAddNodeTail(diffs, key);
    }
cleanup:
    if (r1) freeReplyObject(r1);
    if (r2) freeReplyObject(r2);
    zfree(argv);
    zfree(argv_len);
    return success;
}

/* 迁移 reply->elements 中的所有 key。返回 MIGRATE 命令的 reply，
 * 出错则返回 NULL。若 'dots' 不为 NULL，则每迁移一个 key 打印一个点。 */
static redisReply *clusterManagerMigrateKeysInReply(clusterManagerNode *source,
                                                    clusterManagerNode *target,
                                                    redisReply *reply,
                                                    int replace, int timeout,
                                                    char *dots)
{
    redisReply *migrate_reply = NULL;
    char **argv = NULL;
    size_t *argv_len = NULL;
    int c = (replace ? 8 : 7);
    if (config.conn_info.auth) c += 2;
    if (config.conn_info.user) c += 1;
    size_t argc = c + reply->elements;
    size_t i, offset = 6; // Keys Offset
    argv = zcalloc(argc * sizeof(char *));
    argv_len = zcalloc(argc * sizeof(size_t));
    char portstr[255];
    char timeoutstr[255];
    snprintf(portstr, 10, "%d", target->port);
    snprintf(timeoutstr, 10, "%d", timeout);
    argv[0] = "MIGRATE";
    argv_len[0] = 7;
    argv[1] = target->ip;
    argv_len[1] = strlen(target->ip);
    argv[2] = portstr;
    argv_len[2] = strlen(portstr);
    argv[3] = "";
    argv_len[3] = 0;
    argv[4] = "0";
    argv_len[4] = 1;
    argv[5] = timeoutstr;
    argv_len[5] = strlen(timeoutstr);
    if (replace) {
        argv[offset] = "REPLACE";
        argv_len[offset] = 7;
        offset++;
    }
    if (config.conn_info.auth) {
        if (config.conn_info.user) {
            argv[offset] = "AUTH2";
            argv_len[offset] = 5;
            offset++;
            argv[offset] = config.conn_info.user;
            argv_len[offset] = strlen(config.conn_info.user);
            offset++;
            argv[offset] = config.conn_info.auth;
            argv_len[offset] = strlen(config.conn_info.auth);
            offset++;
        } else {
            argv[offset] = "AUTH";
            argv_len[offset] = 4;
            offset++;
            argv[offset] = config.conn_info.auth;
            argv_len[offset] = strlen(config.conn_info.auth);
            offset++;
        }
    }
    argv[offset] = "KEYS";
    argv_len[offset] = 4;
    offset++;
    for (i = 0; i < reply->elements; i++) {
        redisReply *entry = reply->element[i];
        size_t idx = i + offset;
        assert(entry->type == REDIS_REPLY_STRING);
        argv[idx] = (char *) sdsnewlen(entry->str, entry->len);
        argv_len[idx] = entry->len;
        if (dots) dots[i] = '.';
    }
    if (dots) dots[reply->elements] = '\0';
    void *_reply = NULL;
    redisAppendCommandArgv(source->context,argc,
                           (const char**)argv,argv_len);
    int success = (redisGetReply(source->context, &_reply) == REDIS_OK);
    for (i = 0; i < reply->elements; i++) sdsfree(argv[i + offset]);
    if (!success) goto cleanup;
    migrate_reply = (redisReply *) _reply;
cleanup:
    zfree(argv);
    zfree(argv_len);
    return migrate_reply;
}

/* 将指定槽位内的所有 key 从 source 迁移到 target */
static int clusterManagerMigrateKeysInSlot(clusterManagerNode *source,
                                           clusterManagerNode *target,
                                           int slot, int timeout,
                                           int pipeline, int verbose,
                                           char **err)
{
    int success = 1;
    int do_fix = config.cluster_manager_command.flags &
                 CLUSTER_MANAGER_CMD_FLAG_FIX;
    int do_replace = config.cluster_manager_command.flags &
                     CLUSTER_MANAGER_CMD_FLAG_REPLACE;
    while (1) {
        char *dots = NULL;
        redisReply *reply = NULL, *migrate_reply = NULL;
        reply = CLUSTER_MANAGER_COMMAND(source, "CLUSTER "
                                        "GETKEYSINSLOT %d %d", slot,
                                        pipeline);
        success = (reply != NULL);
        if (!success) return 0;
        if (reply->type == REDIS_REPLY_ERROR) {
            success = 0;
            if (err != NULL) {
                *err = zmalloc((reply->len + 1) * sizeof(char));
                redis_strlcpy(*err, reply->str,(reply->len + 1));
                CLUSTER_MANAGER_PRINT_REPLY_ERROR(source, *err);
            }
            goto next;
        }
        assert(reply->type == REDIS_REPLY_ARRAY);
        size_t count = reply->elements;
        if (count == 0) {
            freeReplyObject(reply);
            break;
        }
        if (verbose) dots = zmalloc((count+1) * sizeof(char));
        /* Calling MIGRATE command. */
        migrate_reply = clusterManagerMigrateKeysInReply(source, target,
                                                         reply, 0, timeout,
                                                         dots);
        if (migrate_reply == NULL) goto next;
        if (migrate_reply->type == REDIS_REPLY_ERROR) {
            int is_busy = strstr(migrate_reply->str, "BUSYKEY") != NULL;
            int not_served = 0;
            if (!is_busy) {
                /* Check if the slot is unassigned (not served) in the
                 * source node's configuration. */
                char *get_owner_err = NULL;
                clusterManagerNode *served_by =
                    clusterManagerGetSlotOwner(source, slot, &get_owner_err);
                if (!served_by) {
                    if (get_owner_err == NULL) not_served = 1;
                    else {
                        CLUSTER_MANAGER_PRINT_REPLY_ERROR(source,
                                                          get_owner_err);
                        zfree(get_owner_err);
                    }
                }
            }
            /* Try to handle errors. */
            if (is_busy || not_served) {
                /* If the key's slot is not served, try to assign slot
                 * to the target node. */
                if (do_fix && not_served) {
                    clusterManagerLogWarn("*** Slot was not served, setting "
                                          "owner to node %s:%d.\n",
                                          target->ip, target->port);
                    clusterManagerSetSlot(source, target, slot, "node", NULL);
                }
                /* If the key already exists in the target node (BUSYKEY),
                 * check whether its value is the same in both nodes.
                 * In case of equal values, retry migration with the
                 * REPLACE option.
                 * In case of different values:
                 *  - If the migration is requested by the fix command, stop
                 *    and warn the user.
                 *  - In other cases (ie. reshard), proceed only if the user
                 *    launched the command with the --cluster-replace option.*/
                if (is_busy) {
                    clusterManagerLogWarn("\n*** Target key exists\n");
                    if (!do_replace) {
                        clusterManagerLogWarn("*** Checking key values on "
                                              "both nodes...\n");
                        list *diffs = listCreate();
                        success = clusterManagerCompareKeysValues(source,
                            target, reply, diffs);
                        if (!success) {
                            clusterManagerLogErr("*** Value check failed!\n");
                            listRelease(diffs);
                            goto next;
                        }
                        if (listLength(diffs) > 0) {
                            success = 0;
                            clusterManagerLogErr(
                                "*** Found %d key(s) in both source node and "
                                "target node having different values.\n"
                                "    Source node: %s:%d\n"
                                "    Target node: %s:%d\n"
                                "    Keys(s):\n",
                                listLength(diffs),
                                source->ip, source->port,
                                target->ip, target->port);
                            listIter dli;
                            listNode *dln;
                            listRewind(diffs, &dli);
                            while((dln = listNext(&dli)) != NULL) {
                                char *k = dln->value;
                                clusterManagerLogErr("    - %s\n", k);
                            }
                            clusterManagerLogErr("Please fix the above key(s) "
                                                 "manually and try again "
                                                 "or relaunch the command \n"
                                                 "with --cluster-replace "
                                                 "option to force key "
                                                 "overriding.\n");
                            listRelease(diffs);
                            goto next;
                        }
                        listRelease(diffs);
                    }
                    clusterManagerLogWarn("*** Replacing target keys...\n");
                }
                freeReplyObject(migrate_reply);
                migrate_reply = clusterManagerMigrateKeysInReply(source,
                                                                 target,
                                                                 reply,
                                                                 is_busy,
                                                                 timeout,
                                                                 NULL);
                success = (migrate_reply != NULL &&
                           migrate_reply->type != REDIS_REPLY_ERROR);
            } else success = 0;
            if (!success) {
                if (migrate_reply != NULL) {
                    if (err) {
                        *err = zmalloc((migrate_reply->len + 1) * sizeof(char));
                        redis_strlcpy(*err, migrate_reply->str, (migrate_reply->len + 1));
                    }
                    printf("\n");
                    CLUSTER_MANAGER_PRINT_REPLY_ERROR(source,
                                                      migrate_reply->str);
                }
                goto next;
            }
        }
        if (verbose) {
            printf("%s", dots);
            fflush(stdout);
        }
next:
        if (reply != NULL) freeReplyObject(reply);
        if (migrate_reply != NULL) freeReplyObject(migrate_reply);
        if (dots) zfree(dots);
        if (!success) break;
    }
    return success;
}

/* 通过 MIGRATE 在 source 和 target 之间迁移槽位。
 *
 * 选项：
 * CLUSTER_MANAGER_OPT_VERBOSE -- 每迁移一个 key 打印一个点。
 * CLUSTER_MANAGER_OPT_COLD    -- 迁移 key 但不打开槽位 / 不重新配置节点。
 * CLUSTER_MANAGER_OPT_UPDATE  -- 更新 source/target 节点的 slots。
 * CLUSTER_MANAGER_OPT_QUIET   -- 不打印信息消息。
 */
static int clusterManagerMoveSlot(clusterManagerNode *source,
                                  clusterManagerNode *target,
                                  int slot, int opts,  char**err)
{
    if (!(opts & CLUSTER_MANAGER_OPT_QUIET)) {
        printf("Moving slot %d from %s:%d to %s:%d: ", slot, source->ip,
               source->port, target->ip, target->port);
        fflush(stdout);
    }
    if (err != NULL) *err = NULL;
    int pipeline = config.cluster_manager_command.pipeline,
        timeout = config.cluster_manager_command.timeout,
        print_dots = (opts & CLUSTER_MANAGER_OPT_VERBOSE),
        option_cold = (opts & CLUSTER_MANAGER_OPT_COLD),
        success = 1;
    if (!option_cold) {
        success = clusterManagerSetSlot(target, source, slot,
                                        "importing", err);
        if (!success) return 0;
        success = clusterManagerSetSlot(source, target, slot,
                                        "migrating", err);
        if (!success) return 0;
    }
    success = clusterManagerMigrateKeysInSlot(source, target, slot, timeout,
                                              pipeline, print_dots, err);
    if (!(opts & CLUSTER_MANAGER_OPT_QUIET)) printf("\n");
    if (!success) return 0;
    if (!option_cold) {
        /* Set the new node as the owner of the slot in all the known nodes.
         *
         * We inform the target node first. It will propagate the information to
         * the rest of the cluster.
         *
         * If we inform any other node first, it can happen that the target node
         * crashes before it is set as the new owner and then the slot is left
         * without an owner which results in redirect loops. See issue #7116. */
        success = clusterManagerSetSlot(target, target, slot, "node", err);
        if (!success) return 0;

        /* Inform the source node. If the source node has just lost its last
         * slot and the target node has already informed the source node, the
         * source node has turned itself into a replica. This is not an error in
         * this scenario so we ignore it. See issue #9223. */
        success = clusterManagerSetSlot(source, target, slot, "node", err);
        const char *acceptable = "ERR Please use SETSLOT only with masters.";
        if (!success && err && !strncmp(*err, acceptable, strlen(acceptable))) {
            zfree(*err);
            *err = NULL;
        } else if (!success && err) {
            return 0;
        }

        /* We also inform the other nodes to avoid redirects in case the target
         * node is slow to propagate the change to the entire cluster. */
        listIter li;
        listNode *ln;
        listRewind(cluster_manager.nodes, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = ln->value;
            if (n == target || n == source) continue; /* already done */
            if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
            success = clusterManagerSetSlot(n, target, slot, "node", err);
            if (!success) return 0;
        }
    }
    /* Update the node logical config */
    if (opts & CLUSTER_MANAGER_OPT_UPDATE) {
        source->slots[slot] = 0;
        target->slots[slot] = 1;
    }
    return 1;
}

/* 通过 REPLICATE 或 ADDSLOTS 把节点的脏配置刷到服务器 */
static int clusterManagerFlushNodeConfig(clusterManagerNode *node, char **err) {
    if (!node->dirty) return 0;
    redisReply *reply = NULL;
    int is_err = 0, success = 1;
    if (err != NULL) *err = NULL;
    if (node->replicate != NULL) {
        reply = CLUSTER_MANAGER_COMMAND(node, "CLUSTER REPLICATE %s",
                                        node->replicate);
        if (reply == NULL || (is_err = (reply->type == REDIS_REPLY_ERROR))) {
            if (is_err && err != NULL) {
                *err = zmalloc((reply->len + 1) * sizeof(char));
                redis_strlcpy(*err, reply->str, (reply->len + 1));
            }
            success = 0;
            /* If the cluster did not already joined it is possible that
             * the slave does not know the master node yet. So on errors
             * we return ASAP leaving the dirty flag set, to flush the
             * config later. */
            goto cleanup;
        }
    } else {
        int added = clusterManagerAddSlots(node, err);
        if (!added || *err != NULL) success = 0;
    }
    node->dirty = 0;
cleanup:
    if (reply != NULL) freeReplyObject(reply);
    return success;
}

/* 阻塞等待集群配置达到一致状态 */
static void clusterManagerWaitForClusterJoin(void) {
    printf("Waiting for the cluster to join\n");
    int counter = 0,
        check_after = CLUSTER_JOIN_CHECK_AFTER +
                      (int)(listLength(cluster_manager.nodes) * 0.15f);
    while(!clusterManagerIsConfigConsistent()) {
        printf(".");
        fflush(stdout);
        sleep(1);
        if (++counter > check_after) {
            dict *status = clusterManagerGetLinkStatus();
            dictIterator *iter = NULL;
            if (status != NULL && dictSize(status) > 0) {
                printf("\n");
                clusterManagerLogErr("Warning: %d node(s) may "
                                     "be unreachable\n", dictSize(status));
                iter = dictGetIterator(status);
                dictEntry *entry;
                while ((entry = dictNext(iter)) != NULL) {
                    sds nodeaddr = (sds) dictGetKey(entry);
                    char *node_ip = NULL;
                    int node_port = 0, node_bus_port = 0;
                    list *from = (list *) dictGetVal(entry);
                    if (parseClusterNodeAddress(nodeaddr, &node_ip,
                        &node_port, &node_bus_port) && node_bus_port) {
                        clusterManagerLogErr(" - The port %d of node %s may "
                                             "be unreachable from:\n",
                                             node_bus_port, node_ip);
                    } else {
                        clusterManagerLogErr(" - Node %s may be unreachable "
                                             "from:\n", nodeaddr);
                    }
                    listIter li;
                    listNode *ln;
                    listRewind(from, &li);
                    while ((ln = listNext(&li)) != NULL) {
                        sds from_addr = ln->value;
                        clusterManagerLogErr("   %s\n", from_addr);
                        sdsfree(from_addr);
                    }
                    clusterManagerLogErr("Cluster bus ports must be reachable "
                                         "by every node.\nRemember that "
                                         "cluster bus ports are different "
                                         "from standard instance ports.\n");
                    listEmpty(from);
                }
            }
            if (iter != NULL) dictReleaseIterator(iter);
            if (status != NULL) dictRelease(status);
            counter = 0;
        }
    }
    printf("\n");
}

/* 通过执行 CLUSTER NODES 命令加载节点的集群配置，并据此更新
 * node 的 name、replicate、slots 等信息。
 * 若 'opts' 设置了 CLUSTER_MANAGER_OPT_GETFRIENDS，且节点已知
 * 其他节点，则会把其他节点信息填充到 node 的 friends 列表。 */
static int clusterManagerNodeLoadInfo(clusterManagerNode *node, int opts,
                                      char **err)
{
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node, "CLUSTER NODES");
    int success = 1;
    *err = NULL;
    if (!clusterManagerCheckRedisReply(node, reply, err)) {
        success = 0;
        goto cleanup;
    }
    int getfriends = (opts & CLUSTER_MANAGER_OPT_GETFRIENDS);
    char *lines = reply->str, *p, *line;
    while ((p = strstr(lines, "\n")) != NULL) {
        *p = '\0';
        line = lines;
        lines = p + 1;
        char *name = NULL, *addr = NULL, *flags = NULL, *master_id = NULL,
             *ping_sent = NULL, *ping_recv = NULL, *config_epoch = NULL,
             *link_status = NULL;
        UNUSED(link_status);
        int i = 0;
        while ((p = strchr(line, ' ')) != NULL) {
            *p = '\0';
            char *token = line;
            line = p + 1;
            switch(i++){
            case 0: name = token; break;
            case 1: addr = token; break;
            case 2: flags = token; break;
            case 3: master_id = token; break;
            case 4: ping_sent = token; break;
            case 5: ping_recv = token; break;
            case 6: config_epoch = token; break;
            case 7: link_status = token; break;
            }
            if (i == 8) break; // Slots
        }
        if (!flags) {
            success = 0;
            goto cleanup;
        }

        char *ip = NULL;
        int port = 0, bus_port = 0;
        if (addr == NULL || !parseClusterNodeAddress(addr, &ip, &port, &bus_port)) {
            fprintf(stderr, "Error: invalid CLUSTER NODES reply\n");
            success = 0;
            goto cleanup;
        }

        int myself = (strstr(flags, "myself") != NULL);
        clusterManagerNode *currentNode = NULL;
        if (myself) {
            /* bus-port could be wrong, correct it here, see clusterManagerNewNode. */
            node->bus_port = bus_port;
            node->flags |= CLUSTER_MANAGER_FLAG_MYSELF;
            currentNode = node;
            clusterManagerNodeResetSlots(node);
            if (i == 8) {
                int remaining = strlen(line);
                while (remaining > 0) {
                    p = strchr(line, ' ');
                    if (p == NULL) p = line + remaining;
                    remaining -= (p - line);

                    char *slotsdef = line;
                    *p = '\0';
                    if (remaining) {
                        line = p + 1;
                        remaining--;
                    } else line = p;
                    char *dash = NULL;
                    if (slotsdef[0] == '[') {
                        slotsdef++;
                        if ((p = strstr(slotsdef, "->-"))) { // Migrating
                            *p = '\0';
                            p += 3;
                            char *closing_bracket = strchr(p, ']');
                            if (closing_bracket) *closing_bracket = '\0';
                            sds slot = sdsnew(slotsdef);
                            sds dst = sdsnew(p);
                            node->migrating_count += 2;
                            node->migrating = zrealloc(node->migrating,
                                (node->migrating_count * sizeof(sds)));
                            node->migrating[node->migrating_count - 2] =
                                slot;
                            node->migrating[node->migrating_count - 1] =
                                dst;
                        }  else if ((p = strstr(slotsdef, "-<-"))) {//Importing
                            *p = '\0';
                            p += 3;
                            char *closing_bracket = strchr(p, ']');
                            if (closing_bracket) *closing_bracket = '\0';
                            sds slot = sdsnew(slotsdef);
                            sds src = sdsnew(p);
                            node->importing_count += 2;
                            node->importing = zrealloc(node->importing,
                                (node->importing_count * sizeof(sds)));
                            node->importing[node->importing_count - 2] =
                                slot;
                            node->importing[node->importing_count - 1] =
                                src;
                        }
                    } else if ((dash = strchr(slotsdef, '-')) != NULL) {
                        p = dash;
                        int start, stop;
                        *p = '\0';
                        start = atoi(slotsdef);
                        stop = atoi(p + 1);
                        node->slots_count += (stop - (start - 1));
                        while (start <= stop) node->slots[start++] = 1;
                    } else if (p > slotsdef) {
                        node->slots[atoi(slotsdef)] = 1;
                        node->slots_count++;
                    }
                }
            }
            node->dirty = 0;
        } else if (!getfriends) {
            if (!(node->flags & CLUSTER_MANAGER_FLAG_MYSELF)) continue;
            else break;
        } else {
            currentNode = clusterManagerNewNode(sdsnew(ip), port, bus_port);
            currentNode->flags |= CLUSTER_MANAGER_FLAG_FRIEND;
            if (node->friends == NULL) node->friends = listCreate();
            listAddNodeTail(node->friends, currentNode);
        }
        if (name != NULL) {
            if (currentNode->name) sdsfree(currentNode->name);
            currentNode->name = sdsnew(name);
        }
        if (currentNode->flags_str != NULL)
            freeClusterManagerNodeFlags(currentNode->flags_str);
        currentNode->flags_str = listCreate();
        int flag_len;
        while ((flag_len = strlen(flags)) > 0) {
            sds flag = NULL;
            char *fp = strchr(flags, ',');
            if (fp) {
                *fp = '\0';
                flag = sdsnew(flags);
                flags = fp + 1;
            } else {
                flag = sdsnew(flags);
                flags += flag_len;
            }
            if (strcmp(flag, "noaddr") == 0)
                currentNode->flags |= CLUSTER_MANAGER_FLAG_NOADDR;
            else if (strcmp(flag, "disconnected") == 0)
                currentNode->flags |= CLUSTER_MANAGER_FLAG_DISCONNECT;
            else if (strcmp(flag, "fail") == 0)
                currentNode->flags |= CLUSTER_MANAGER_FLAG_FAIL;
            else if (strcmp(flag, "slave") == 0) {
                currentNode->flags |= CLUSTER_MANAGER_FLAG_SLAVE;
                if (master_id != NULL) {
                    if (currentNode->replicate) sdsfree(currentNode->replicate);
                    currentNode->replicate = sdsnew(master_id);
                }
            }
            listAddNodeTail(currentNode->flags_str, flag);
        }
        if (config_epoch != NULL)
            currentNode->current_epoch = atoll(config_epoch);
        if (ping_sent != NULL) currentNode->ping_sent = atoll(ping_sent);
        if (ping_recv != NULL) currentNode->ping_recv = atoll(ping_recv);
        if (!getfriends && myself) break;
    }
cleanup:
    if (reply) freeReplyObject(reply);
    return success;
}

/* 以 'node' 为起点加载集群信息，所有节点会被加入
 * cluster_manager.nodes 列表。
 * 注意：若出错，会在返回 0 前释放起始节点。 */
static int clusterManagerLoadInfoFromNode(clusterManagerNode *node) {
    if (node->context == NULL && !clusterManagerNodeConnect(node)) {
        freeClusterManagerNode(node);
        return 0;
    }
    char *e = NULL;
    if (!clusterManagerNodeIsCluster(node, &e)) {
        clusterManagerPrintNotClusterNodeError(node, e);
        if (e) zfree(e);
        freeClusterManagerNode(node);
        return 0;
    }
    e = NULL;
    if (!clusterManagerNodeLoadInfo(node, CLUSTER_MANAGER_OPT_GETFRIENDS, &e)) {
        if (e) {
            CLUSTER_MANAGER_PRINT_REPLY_ERROR(node, e);
            zfree(e);
        }
        freeClusterManagerNode(node);
        return 0;
    }
    listIter li;
    listNode *ln;
    if (cluster_manager.nodes != NULL) {
        listRewind(cluster_manager.nodes, &li);
        while ((ln = listNext(&li)) != NULL)
            freeClusterManagerNode((clusterManagerNode *) ln->value);
        listRelease(cluster_manager.nodes);
    }
    cluster_manager.nodes = listCreate();
    listAddNodeTail(cluster_manager.nodes, node);
    if (node->friends != NULL) {
        listRewind(node->friends, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *friend = ln->value;
            if (!friend->ip || !friend->port) goto invalid_friend;
            if (!friend->context && !clusterManagerNodeConnect(friend))
                goto invalid_friend;
            e = NULL;
            if (clusterManagerNodeLoadInfo(friend, 0, &e)) {
                if (friend->flags & (CLUSTER_MANAGER_FLAG_NOADDR |
                                     CLUSTER_MANAGER_FLAG_DISCONNECT |
                                     CLUSTER_MANAGER_FLAG_FAIL))
                {
                    goto invalid_friend;
                }
                listAddNodeTail(cluster_manager.nodes, friend);
            } else {
                clusterManagerLogErr("[ERR] Unable to load info for "
                                     "node %s:%d\n",
                                     friend->ip, friend->port);
                goto invalid_friend;
            }
            continue;
invalid_friend:
            if (!(friend->flags & CLUSTER_MANAGER_FLAG_SLAVE))
                cluster_manager.unreachable_masters++;
            freeClusterManagerNode(friend);
        }
        listRelease(node->friends);
        node->friends = NULL;
    }
    // Count replicas for each node
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->replicate != NULL) {
            clusterManagerNode *master = clusterManagerNodeByName(n->replicate);
            if (master == NULL) {
                clusterManagerLogWarn("*** WARNING: %s:%d claims to be "
                                      "slave of unknown node ID %s.\n",
                                      n->ip, n->port, n->replicate);
            } else master->replicas_count++;
        }
    }
    return 1;
}

/* 各种排序操作所用的比较函数 */
int clusterManagerSlotCompare(const void *slot1, const void *slot2) {
    const char **i1 = (const char **)slot1;
    const char **i2 = (const char **)slot2;
    return strcmp(*i1, *i2);
}

int clusterManagerSlotCountCompareDesc(const void *n1, const void *n2) {
    clusterManagerNode *node1 = *((clusterManagerNode **) n1);
    clusterManagerNode *node2 = *((clusterManagerNode **) n2);
    return node2->slots_count - node1->slots_count;
}

int clusterManagerCompareNodeBalance(const void *n1, const void *n2) {
    clusterManagerNode *node1 = *((clusterManagerNode **) n1);
    clusterManagerNode *node2 = *((clusterManagerNode **) n2);
    return node1->balance - node2->balance;
}

static sds clusterManagerGetConfigSignature(clusterManagerNode *node) {
    sds signature = NULL;
    int node_count = 0, i = 0, name_len = 0;
    char **node_configs = NULL;
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node, "CLUSTER NODES");
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR)
        goto cleanup;
    char *lines = reply->str, *p, *line;
    while ((p = strstr(lines, "\n")) != NULL) {
        i = 0;
        *p = '\0';
        line = lines;
        lines = p + 1;
        char *nodename = NULL;
        int tot_size = 0;
        while ((p = strchr(line, ' ')) != NULL) {
            *p = '\0';
            char *token = line;
            line = p + 1;
            if (i == 0) {
                nodename = token;
                tot_size = (p - token);
                name_len = tot_size++; // Make room for ':' in tot_size
            }
            if (++i == 8) break;
        }
        if (i != 8) continue;
        if (nodename == NULL) continue;
        int remaining = strlen(line);
        if (remaining == 0) continue;
        char **slots = NULL;
        int c = 0;
        while (remaining > 0) {
            p = strchr(line, ' ');
            if (p == NULL) p = line + remaining;
            int size = (p - line);
            remaining -= size;
            tot_size += size;
            char *slotsdef = line;
            *p = '\0';
            if (remaining) {
                line = p + 1;
                remaining--;
            } else line = p;
            if (slotsdef[0] != '[') {
                c++;
                slots = zrealloc(slots, (c * sizeof(char *)));
                slots[c - 1] = slotsdef;
            }
        }
        if (c > 0) {
            if (c > 1)
                qsort(slots, c, sizeof(char *), clusterManagerSlotCompare);
            node_count++;
            node_configs =
                zrealloc(node_configs, (node_count * sizeof(char *)));
            /* Make room for '|' separators. */
            tot_size += (sizeof(char) * (c - 1));
            char *cfg = zmalloc((sizeof(char) * tot_size) + 1);
            memcpy(cfg, nodename, name_len);
            char *sp = cfg + name_len;
            *(sp++) = ':';
            for (i = 0; i < c; i++) {
                if (i > 0) *(sp++) = ',';
                int slen = strlen(slots[i]);
                memcpy(sp, slots[i], slen);
                sp += slen;
            }
            *(sp++) = '\0';
            node_configs[node_count - 1] = cfg;
        }
        zfree(slots);
    }
    if (node_count > 0) {
        if (node_count > 1) {
            qsort(node_configs, node_count, sizeof(char *),
                  clusterManagerSlotCompare);
        }
        signature = sdsempty();
        for (i = 0; i < node_count; i++) {
            if (i > 0) signature = sdscatprintf(signature, "%c", '|');
            signature = sdscatfmt(signature, "%s", node_configs[i]);
        }
    }
cleanup:
    if (reply != NULL) freeReplyObject(reply);
    if (node_configs != NULL) {
        for (i = 0; i < node_count; i++) zfree(node_configs[i]);
        zfree(node_configs);
    }
    return signature;
}

static int clusterManagerIsConfigConsistent(void) {
    if (cluster_manager.nodes == NULL) return 0;
    int consistent = (listLength(cluster_manager.nodes) <= 1);
    // If the Cluster has only one node, it's always consistent
    if (consistent) return 1;
    sds first_cfg = NULL;
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *node = ln->value;
        sds cfg = clusterManagerGetConfigSignature(node);
        if (cfg == NULL) {
            consistent = 0;
            break;
        }
        if (first_cfg == NULL) first_cfg = cfg;
        else {
            consistent = !sdscmp(first_cfg, cfg);
            sdsfree(cfg);
            if (!consistent) break;
        }
    }
    if (first_cfg != NULL) sdsfree(first_cfg);
    return consistent;
}

static list *clusterManagerGetDisconnectedLinks(clusterManagerNode *node) {
    list *links = NULL;
    redisReply *reply = CLUSTER_MANAGER_COMMAND(node, "CLUSTER NODES");
    if (!clusterManagerCheckRedisReply(node, reply, NULL)) goto cleanup;
    links = listCreate();
    char *lines = reply->str, *p, *line;
    while ((p = strstr(lines, "\n")) != NULL) {
        int i = 0;
        *p = '\0';
        line = lines;
        lines = p + 1;
        char *nodename = NULL, *addr = NULL, *flags = NULL, *link_status = NULL;
        while ((p = strchr(line, ' ')) != NULL) {
            *p = '\0';
            char *token = line;
            line = p + 1;
            if (i == 0) nodename = token;
            else if (i == 1) addr = token;
            else if (i == 2) flags = token;
            else if (i == 7) link_status = token;
            else if (i == 8) break;
            i++;
        }
        if (i == 7) link_status = line;
        if (nodename == NULL || addr == NULL || flags == NULL ||
            link_status == NULL) continue;
        if (strstr(flags, "myself") != NULL) continue;
        int disconnected = ((strstr(flags, "disconnected") != NULL) ||
                            (strstr(link_status, "disconnected")));
        int handshaking = (strstr(flags, "handshake") != NULL);
        if (disconnected || handshaking) {
            clusterManagerLink *link = zmalloc(sizeof(*link));
            link->node_name = sdsnew(nodename);
            link->node_addr = sdsnew(addr);
            link->connected = 0;
            link->handshaking = handshaking;
            listAddNodeTail(links, link);
        }
    }
cleanup:
    if (reply != NULL) freeReplyObject(reply);
    return links;
}

/* 检查集群中断开的链路。返回的字典 key 是不可达节点地址，
 * value 是无法到达该节点的其他节点地址列表。 */
static dict *clusterManagerGetLinkStatus(void) {
    if (cluster_manager.nodes == NULL) return NULL;
    dict *status = dictCreate(&clusterManagerLinkDictType);
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *node = ln->value;
        list *links = clusterManagerGetDisconnectedLinks(node);
        if (links) {
            listIter lli;
            listNode *lln;
            listRewind(links, &lli);
            while ((lln = listNext(&lli)) != NULL) {
                clusterManagerLink *link = lln->value;
                list *from = NULL;
                dictEntry *entry = dictFind(status, link->node_addr);
                if (entry) from = dictGetVal(entry);
                else {
                    from = listCreate();
                    dictAdd(status, sdsdup(link->node_addr), from);
                }
                sds myaddr = sdsempty();
                myaddr = sdscatfmt(myaddr, "%s:%u", node->ip, node->port);
                listAddNodeTail(from, myaddr);
                sdsfree(link->node_name);
                sdsfree(link->node_addr);
                zfree(link);
            }
            listRelease(links);
        }
    }
    return status;
}

/* 将错误信息添加到 cluster_manager.errors 并打印 */
static void clusterManagerOnError(sds err) {
    if (cluster_manager.errors == NULL)
        cluster_manager.errors = listCreate();
    listAddNodeTail(cluster_manager.errors, err);
    clusterManagerLogErr("%s\n", (char *) err);
}

/* 检查集群的槽位覆盖情况。'all_slots' 必须是长度为 16384 的数组。
 * 已覆盖的槽位会被置 1，函数返回已覆盖槽位的总数。 */
static int clusterManagerGetCoveredSlots(char *all_slots) {
    if (cluster_manager.nodes == NULL) return 0;
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    int totslots = 0, i;
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *node = ln->value;
        for (i = 0; i < CLUSTER_MANAGER_SLOTS; i++) {
            if (node->slots[i] && !all_slots[i]) {
                all_slots[i] = 1;
                totslots++;
            }
        }
    }
    return totslots;
}

static void clusterManagerPrintSlotsList(list *slots) {
    clusterManagerNode n = {0};
    listIter li;
    listNode *ln;
    listRewind(slots, &li);
    while ((ln = listNext(&li)) != NULL) {
        int slot = atoi(ln->value);
        if (slot >= 0 && slot < CLUSTER_MANAGER_SLOTS)
            n.slots[slot] = 1;
    }
    sds nodeslist = clusterManagerNodeSlotsString(&n);
    printf("%s\n", nodeslist);
    sdsfree(nodeslist);
}

/* 在 'nodes' 列表中返回指定槽位内 key 数最多的节点 */
static clusterManagerNode * clusterManagerGetNodeWithMostKeysInSlot(list *nodes,
                                                                    int slot,
                                                                    char **err)
{
    clusterManagerNode *node = NULL;
    int numkeys = 0;
    listIter li;
    listNode *ln;
    listRewind(nodes, &li);
    if (err) *err = NULL;
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE || n->replicate)
            continue;
        redisReply *r =
            CLUSTER_MANAGER_COMMAND(n, "CLUSTER COUNTKEYSINSLOT %d", slot);
        int success = clusterManagerCheckRedisReply(n, r, err);
        if (success) {
            if (r->integer > numkeys || node == NULL) {
                numkeys = r->integer;
                node = n;
            }
        }
        if (r != NULL) freeReplyObject(r);
        /* If the reply contains errors */
        if (!success) {
            if (err != NULL && *err != NULL)
                CLUSTER_MANAGER_PRINT_REPLY_ERROR(n, err);
            node = NULL;
            break;
        }
    }
    return node;
}

/* 返回集群中副本数最少的主节点。
 * 若有多个主节点副本数相同则随机返回一个。 */

static clusterManagerNode *clusterManagerNodeWithLeastReplicas(void) {
    clusterManagerNode *node = NULL;
    int lowest_count = 0;
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
        if (node == NULL || n->replicas_count < lowest_count) {
            node = n;
            lowest_count = n->replicas_count;
        }
    }
    return node;
}

/* 随机返回一个主节点；若不存在则返回 NULL */

static clusterManagerNode *clusterManagerNodeMasterRandom(void) {
    int master_count = 0;
    int idx;
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
        master_count++;
    }

    assert(master_count > 0);
    srand(time(NULL));
    idx = rand() % master_count;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
        if (!idx--) {
            return n;
        }
    }
    /* Can not be reached */
    assert(0);
}

static int clusterManagerFixSlotsCoverage(char *all_slots) {
    int force_fix = config.cluster_manager_command.flags &
                    CLUSTER_MANAGER_CMD_FLAG_FIX_WITH_UNREACHABLE_MASTERS;

    if (cluster_manager.unreachable_masters > 0 && !force_fix) {
        clusterManagerLogWarn("*** Fixing slots coverage with %d unreachable masters is dangerous: redis-cli will assume that slots about masters that are not reachable are not covered, and will try to reassign them to the reachable nodes. This can cause data loss and is rarely what you want to do. If you really want to proceed use the --cluster-fix-with-unreachable-masters option.\n", cluster_manager.unreachable_masters);
        exit(1);
    }

    int i, fixed = 0;
    list *none = NULL, *single = NULL, *multi = NULL;
    clusterManagerLogInfo(">>> Fixing slots coverage...\n");
    for (i = 0; i < CLUSTER_MANAGER_SLOTS; i++) {
        int covered = all_slots[i];
        if (!covered) {
            sds slot = sdsfromlonglong((long long) i);
            list *slot_nodes = listCreate();
            sds slot_nodes_str = sdsempty();
            listIter li;
            listNode *ln;
            listRewind(cluster_manager.nodes, &li);
            while ((ln = listNext(&li)) != NULL) {
                clusterManagerNode *n = ln->value;
                if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE || n->replicate)
                    continue;
                redisReply *reply = CLUSTER_MANAGER_COMMAND(n,
                    "CLUSTER GETKEYSINSLOT %d %d", i, 1);
                if (!clusterManagerCheckRedisReply(n, reply, NULL)) {
                    fixed = -1;
                    if (reply) freeReplyObject(reply);
                    goto cleanup;
                }
                assert(reply->type == REDIS_REPLY_ARRAY);
                if (reply->elements > 0) {
                    listAddNodeTail(slot_nodes, n);
                    if (listLength(slot_nodes) > 1)
                        slot_nodes_str = sdscat(slot_nodes_str, ", ");
                    slot_nodes_str = sdscatfmt(slot_nodes_str,
                                               "%s:%u", n->ip, n->port);
                }
                freeReplyObject(reply);
            }
            sdsfree(slot_nodes_str);
            dictAdd(clusterManagerUncoveredSlots, slot, slot_nodes);
        }
    }

    /* For every slot, take action depending on the actual condition:
     * 1) No node has keys for this slot.
     * 2) A single node has keys for this slot.
     * 3) Multiple nodes have keys for this slot. */
    none = listCreate();
    single = listCreate();
    multi = listCreate();
    dictIterator *iter = dictGetIterator(clusterManagerUncoveredSlots);
    dictEntry *entry;
    while ((entry = dictNext(iter)) != NULL) {
        sds slot = (sds) dictGetKey(entry);
        list *nodes = (list *) dictGetVal(entry);
        switch (listLength(nodes)){
        case 0: listAddNodeTail(none, slot); break;
        case 1: listAddNodeTail(single, slot); break;
        default: listAddNodeTail(multi, slot); break;
        }
    }
    dictReleaseIterator(iter);

    /* we want explicit manual confirmation from users for all the fix cases */
    int ignore_force = 1;

    /*  Handle case "1": keys in no node. */
    if (listLength(none) > 0) {
        printf("The following uncovered slots have no keys "
               "across the cluster:\n");
        clusterManagerPrintSlotsList(none);
        if (confirmWithYes("Fix these slots by covering with a random node?",
                           ignore_force)) {
            listIter li;
            listNode *ln;
            listRewind(none, &li);
            while ((ln = listNext(&li)) != NULL) {
                sds slot = ln->value;
                int s = atoi(slot);
                clusterManagerNode *n = clusterManagerNodeMasterRandom();
                clusterManagerLogInfo(">>> Covering slot %s with %s:%d\n",
                                      slot, n->ip, n->port);
                if (!clusterManagerSetSlotOwner(n, s, 0)) {
                    fixed = -1;
                    goto cleanup;
                }
                /* Since CLUSTER ADDSLOTS succeeded, we also update the slot
                 * info into the node struct, in order to keep it synced */
                n->slots[s] = 1;
                fixed++;
            }
        }
    }

    /*  Handle case "2": keys only in one node. */
    if (listLength(single) > 0) {
        printf("The following uncovered slots have keys in just one node:\n");
        clusterManagerPrintSlotsList(single);
        if (confirmWithYes("Fix these slots by covering with those nodes?",
                           ignore_force)) {
            listIter li;
            listNode *ln;
            listRewind(single, &li);
            while ((ln = listNext(&li)) != NULL) {
                sds slot = ln->value;
                int s = atoi(slot);
                dictEntry *entry = dictFind(clusterManagerUncoveredSlots, slot);
                assert(entry != NULL);
                list *nodes = (list *) dictGetVal(entry);
                listNode *fn = listFirst(nodes);
                assert(fn != NULL);
                clusterManagerNode *n = fn->value;
                clusterManagerLogInfo(">>> Covering slot %s with %s:%d\n",
                                      slot, n->ip, n->port);
                if (!clusterManagerSetSlotOwner(n, s, 0)) {
                    fixed = -1;
                    goto cleanup;
                }
                /* Since CLUSTER ADDSLOTS succeeded, we also update the slot
                 * info into the node struct, in order to keep it synced */
                n->slots[atoi(slot)] = 1;
                fixed++;
            }
        }
    }

    /* Handle case "3": keys in multiple nodes. */
    if (listLength(multi) > 0) {
        printf("The following uncovered slots have keys in multiple nodes:\n");
        clusterManagerPrintSlotsList(multi);
        if (confirmWithYes("Fix these slots by moving keys "
                           "into a single node?", ignore_force)) {
            listIter li;
            listNode *ln;
            listRewind(multi, &li);
            while ((ln = listNext(&li)) != NULL) {
                sds slot = ln->value;
                dictEntry *entry = dictFind(clusterManagerUncoveredSlots, slot);
                assert(entry != NULL);
                list *nodes = (list *) dictGetVal(entry);
                int s = atoi(slot);
                clusterManagerNode *target =
                    clusterManagerGetNodeWithMostKeysInSlot(nodes, s, NULL);
                if (target == NULL) {
                    fixed = -1;
                    goto cleanup;
                }
                clusterManagerLogInfo(">>> Covering slot %s moving keys "
                                      "to %s:%d\n", slot,
                                      target->ip, target->port);
                if (!clusterManagerSetSlotOwner(target, s, 1)) {
                    fixed = -1;
                    goto cleanup;
                }
                /* Since CLUSTER ADDSLOTS succeeded, we also update the slot
                 * info into the node struct, in order to keep it synced */
                target->slots[atoi(slot)] = 1;
                listIter nli;
                listNode *nln;
                listRewind(nodes, &nli);
                while ((nln = listNext(&nli)) != NULL) {
                    clusterManagerNode *src = nln->value;
                    if (src == target) continue;
                    /* Assign the slot to target node in the source node. */
                    if (!clusterManagerSetSlot(src, target, s, "NODE", NULL))
                        fixed = -1;
                    if (fixed < 0) goto cleanup;
                    /* Set the source node in 'importing' state
                     * (even if we will actually migrate keys away)
                     * in order to avoid receiving redirections
                     * for MIGRATE. */
                    if (!clusterManagerSetSlot(src, target, s,
                                               "IMPORTING", NULL)) fixed = -1;
                    if (fixed < 0) goto cleanup;
                    int opts = CLUSTER_MANAGER_OPT_VERBOSE |
                               CLUSTER_MANAGER_OPT_COLD;
                    if (!clusterManagerMoveSlot(src, target, s, opts, NULL)) {
                        fixed = -1;
                        goto cleanup;
                    }
                    if (!clusterManagerClearSlotStatus(src, s))
                        fixed = -1;
                    if (fixed < 0) goto cleanup;
                }
                fixed++;
            }
        }
    }
cleanup:
    if (none) listRelease(none);
    if (single) listRelease(single);
    if (multi) listRelease(multi);
    return fixed;
}

/* 槽位 'slot' 在某些节点上被发现处于 importing 或 migrating 状态。
 * 该函数通过在合理位置迁移 key 来修复此状态。 */
static int clusterManagerFixOpenSlot(int slot) {
    int force_fix = config.cluster_manager_command.flags &
                    CLUSTER_MANAGER_CMD_FLAG_FIX_WITH_UNREACHABLE_MASTERS;

    if (cluster_manager.unreachable_masters > 0 && !force_fix) {
        clusterManagerLogWarn("*** Fixing open slots with %d unreachable masters is dangerous: redis-cli will assume that slots about masters that are not reachable are not covered, and will try to reassign them to the reachable nodes. This can cause data loss and is rarely what you want to do. If you really want to proceed use the --cluster-fix-with-unreachable-masters option.\n", cluster_manager.unreachable_masters);
        exit(1);
    }

    clusterManagerLogInfo(">>> Fixing open slot %d\n", slot);
    /* Try to obtain the current slot owner, according to the current
     * nodes configuration. */
    int success = 1;
    list *owners = listCreate();    /* List of nodes claiming some ownership.
                                       it could be stating in the configuration
                                       to have the node ownership, or just
                                       holding keys for such slot. */
    list *migrating = listCreate();
    list *importing = listCreate();
    sds migrating_str = sdsempty();
    sds importing_str = sdsempty();
    clusterManagerNode *owner = NULL; /* The obvious slot owner if any. */

    /* Iterate all the nodes, looking for potential owners of this slot. */
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
        if (n->slots[slot]) {
            listAddNodeTail(owners, n);
        } else {
            redisReply *r = CLUSTER_MANAGER_COMMAND(n,
                "CLUSTER COUNTKEYSINSLOT %d", slot);
            success = clusterManagerCheckRedisReply(n, r, NULL);
            if (success && r->integer > 0) {
                clusterManagerLogWarn("*** Found keys about slot %d "
                                      "in non-owner node %s:%d!\n", slot,
                                      n->ip, n->port);
                listAddNodeTail(owners, n);
            }
            if (r) freeReplyObject(r);
            if (!success) goto cleanup;
        }
    }

    /* If we have only a single potential owner for this slot,
     * set it as "owner". */
    if (listLength(owners) == 1) owner = listFirst(owners)->value;

    /* Scan the list of nodes again, in order to populate the
     * list of nodes in importing or migrating state for
     * this slot. */
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
        int is_migrating = 0, is_importing = 0;
        if (n->migrating) {
            for (int i = 0; i < n->migrating_count; i += 2) {
                sds migrating_slot = n->migrating[i];
                if (atoi(migrating_slot) == slot) {
                    char *sep = (listLength(migrating) == 0 ? "" : ",");
                    migrating_str = sdscatfmt(migrating_str, "%s%s:%u",
                                              sep, n->ip, n->port);
                    listAddNodeTail(migrating, n);
                    is_migrating = 1;
                    break;
                }
            }
        }
        if (!is_migrating && n->importing) {
            for (int i = 0; i < n->importing_count; i += 2) {
                sds importing_slot = n->importing[i];
                if (atoi(importing_slot) == slot) {
                    char *sep = (listLength(importing) == 0 ? "" : ",");
                    importing_str = sdscatfmt(importing_str, "%s%s:%u",
                                              sep, n->ip, n->port);
                    listAddNodeTail(importing, n);
                    is_importing = 1;
                    break;
                }
            }
        }

        /* If the node is neither migrating nor importing and it's not
         * the owner, then is added to the importing list in case
         * it has keys in the slot. */
        if (!is_migrating && !is_importing && n != owner) {
            redisReply *r = CLUSTER_MANAGER_COMMAND(n,
                "CLUSTER COUNTKEYSINSLOT %d", slot);
            success = clusterManagerCheckRedisReply(n, r, NULL);
            if (success && r->integer > 0) {
                clusterManagerLogWarn("*** Found keys about slot %d "
                                      "in node %s:%d!\n", slot, n->ip,
                                      n->port);
                char *sep = (listLength(importing) == 0 ? "" : ",");
                importing_str = sdscatfmt(importing_str, "%s%s:%u",
                                          sep, n->ip, n->port);
                listAddNodeTail(importing, n);
            }
            if (r) freeReplyObject(r);
            if (!success) goto cleanup;
        }
    }
    if (sdslen(migrating_str) > 0)
        printf("Set as migrating in: %s\n", migrating_str);
    if (sdslen(importing_str) > 0)
        printf("Set as importing in: %s\n", importing_str);

    /* If there is no slot owner, set as owner the node with the biggest
     * number of keys, among the set of migrating / importing nodes. */
    if (owner == NULL) {
        clusterManagerLogInfo(">>> No single clear owner for the slot, "
                              "selecting an owner by # of keys...\n");
        owner = clusterManagerGetNodeWithMostKeysInSlot(cluster_manager.nodes,
                                                        slot, NULL);
        // If we still don't have an owner, we can't fix it.
        if (owner == NULL) {
            clusterManagerLogErr("[ERR] Can't select a slot owner. "
                                 "Impossible to fix.\n");
            success = 0;
            goto cleanup;
        }

        // Use ADDSLOTS to assign the slot.
        clusterManagerLogWarn("*** Configuring %s:%d as the slot owner\n",
                              owner->ip, owner->port);
        success = clusterManagerClearSlotStatus(owner, slot);
        if (!success) goto cleanup;
        success = clusterManagerSetSlotOwner(owner, slot, 0);
        if (!success) goto cleanup;
        /* Since CLUSTER ADDSLOTS succeeded, we also update the slot
         * info into the node struct, in order to keep it synced */
        owner->slots[slot] = 1;
        /* Remove the owner from the list of migrating/importing
         * nodes. */
        clusterManagerRemoveNodeFromList(migrating, owner);
        clusterManagerRemoveNodeFromList(importing, owner);
    }

    /* If there are multiple owners of the slot, we need to fix it
     * so that a single node is the owner and all the other nodes
     * are in importing state. Later the fix can be handled by one
     * of the base cases above.
     *
     * Note that this case also covers multiple nodes having the slot
     * in migrating state, since migrating is a valid state only for
     * slot owners. */
    if (listLength(owners) > 1) {
        /* Owner cannot be NULL at this point, since if there are more owners,
         * the owner has been set in the previous condition (owner == NULL). */
        assert(owner != NULL);
        listRewind(owners, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = ln->value;
            if (n == owner) continue;
            success = clusterManagerDelSlot(n, slot, 1);
            if (!success) goto cleanup;
            n->slots[slot] = 0;
            /* Assign the slot to the owner in the node 'n' configuration.' */
            success = clusterManagerSetSlot(n, owner, slot, "node", NULL);
            if (!success) goto cleanup;
            success = clusterManagerSetSlot(n, owner, slot, "importing", NULL);
            if (!success) goto cleanup;
            /* Avoid duplicates. */
            clusterManagerRemoveNodeFromList(importing, n);
            listAddNodeTail(importing, n);
            /* Ensure that the node is not in the migrating list. */
            clusterManagerRemoveNodeFromList(migrating, n);
        }
    }
    int move_opts = CLUSTER_MANAGER_OPT_VERBOSE;

    /* Case 1: The slot is in migrating state in one node, and in
     *         importing state in 1 node. That's trivial to address. */
    if (listLength(migrating) == 1 && listLength(importing) == 1) {
        clusterManagerNode *src = listFirst(migrating)->value;
        clusterManagerNode *dst = listFirst(importing)->value;
        clusterManagerLogInfo(">>> Case 1: Moving slot %d from "
                              "%s:%d to %s:%d\n", slot,
                              src->ip, src->port, dst->ip, dst->port);
        move_opts |= CLUSTER_MANAGER_OPT_UPDATE;
        success = clusterManagerMoveSlot(src, dst, slot, move_opts, NULL);
    }

    /* Case 2: There are multiple nodes that claim the slot as importing,
     * they probably got keys about the slot after a restart so opened
     * the slot. In this case we just move all the keys to the owner
     * according to the configuration. */
    else if (listLength(migrating) == 0 && listLength(importing) > 0) {
        clusterManagerLogInfo(">>> Case 2: Moving all the %d slot keys to its "
                              "owner %s:%d\n", slot, owner->ip, owner->port);
        move_opts |= CLUSTER_MANAGER_OPT_COLD;
        listRewind(importing, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = ln->value;
            if (n == owner) continue;
            success = clusterManagerMoveSlot(n, owner, slot, move_opts, NULL);
            if (!success) goto cleanup;
            clusterManagerLogInfo(">>> Setting %d as STABLE in "
                                  "%s:%d\n", slot, n->ip, n->port);
            success = clusterManagerClearSlotStatus(n, slot);
            if (!success) goto cleanup;
        }
        /* Since the slot has been moved in "cold" mode, ensure that all the
         * other nodes update their own configuration about the slot itself. */
        listRewind(cluster_manager.nodes, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = ln->value;
            if (n == owner) continue;
            if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
            success = clusterManagerSetSlot(n, owner, slot, "NODE", NULL);
            if (!success) goto cleanup;
        }
    }

    /* Case 3: The slot is in migrating state in one node but multiple
     * other nodes claim to be in importing state and don't have any key in
     * the slot. We search for the importing node having the same ID as
     * the destination node of the migrating node.
     * In that case we move the slot from the migrating node to this node and
     * we close the importing states on all the other importing nodes.
     * If no importing node has the same ID as the destination node of the
     * migrating node, the slot's state is closed on both the migrating node
     * and the importing nodes. */
    else if (listLength(migrating) == 1 && listLength(importing) > 1) {
        int try_to_fix = 1;
        clusterManagerNode *src = listFirst(migrating)->value;
        clusterManagerNode *dst = NULL;
        sds target_id = NULL;
        for (int i = 0; i < src->migrating_count; i += 2) {
            sds migrating_slot = src->migrating[i];
            if (atoi(migrating_slot) == slot) {
                target_id = src->migrating[i + 1];
                break;
            }
        }
        assert(target_id != NULL);
        listIter li;
        listNode *ln;
        listRewind(importing, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = ln->value;
            int count = clusterManagerCountKeysInSlot(n, slot);
            if (count > 0) {
                try_to_fix = 0;
                break;
            }
            if (strcmp(n->name, target_id) == 0) dst = n;
        }
        if (!try_to_fix) goto unhandled_case;
        if (dst != NULL) {
            clusterManagerLogInfo(">>> Case 3: Moving slot %d from %s:%d to "
                                  "%s:%d and closing it on all the other "
                                  "importing nodes.\n",
                                  slot, src->ip, src->port,
                                  dst->ip, dst->port);
            /* Move the slot to the destination node. */
            success = clusterManagerMoveSlot(src, dst, slot, move_opts, NULL);
            if (!success) goto cleanup;
            /* Close slot on all the other importing nodes. */
            listRewind(importing, &li);
            while ((ln = listNext(&li)) != NULL) {
                clusterManagerNode *n = ln->value;
                if (dst == n) continue;
                success = clusterManagerClearSlotStatus(n, slot);
                if (!success) goto cleanup;
            }
        } else {
            clusterManagerLogInfo(">>> Case 3: Closing slot %d on both "
                                  "migrating and importing nodes.\n", slot);
            /* Close the slot on both the migrating node and the importing
             * nodes. */
            success = clusterManagerClearSlotStatus(src, slot);
            if (!success) goto cleanup;
            listRewind(importing, &li);
            while ((ln = listNext(&li)) != NULL) {
                clusterManagerNode *n = ln->value;
                success = clusterManagerClearSlotStatus(n, slot);
                if (!success) goto cleanup;
            }
        }
    } else {
        int try_to_close_slot = (listLength(importing) == 0 &&
                                 listLength(migrating) == 1);
        if (try_to_close_slot) {
            clusterManagerNode *n = listFirst(migrating)->value;
            if (!owner || owner != n) {
                redisReply *r = CLUSTER_MANAGER_COMMAND(n,
                    "CLUSTER GETKEYSINSLOT %d %d", slot, 10);
                success = clusterManagerCheckRedisReply(n, r, NULL);
                if (r) {
                    if (success) try_to_close_slot = (r->elements == 0);
                    freeReplyObject(r);
                }
                if (!success) goto cleanup;
            }
        }
        /* Case 4: There are no slots claiming to be in importing state, but
         * there is a migrating node that actually don't have any key or is the
         * slot owner. We can just close the slot, probably a reshard
         * interrupted in the middle. */
        if (try_to_close_slot) {
            clusterManagerNode *n = listFirst(migrating)->value;
            clusterManagerLogInfo(">>> Case 4: Closing slot %d on %s:%d\n",
                                  slot, n->ip, n->port);
            redisReply *r = CLUSTER_MANAGER_COMMAND(n, "CLUSTER SETSLOT %d %s",
                                                    slot, "STABLE");
            success = clusterManagerCheckRedisReply(n, r, NULL);
            if (r) freeReplyObject(r);
            if (!success) goto cleanup;
        } else {
unhandled_case:
            success = 0;
            clusterManagerLogErr("[ERR] Sorry, redis-cli can't fix this slot "
                                 "yet (work in progress). Slot is set as "
                                 "migrating in %s, as importing in %s, "
                                 "owner is %s:%d\n", migrating_str,
                                 importing_str, owner->ip, owner->port);
        }
    }
cleanup:
    listRelease(owners);
    listRelease(migrating);
    listRelease(importing);
    sdsfree(migrating_str);
    sdsfree(importing_str);
    return success;
}

/* 修复指定槽位存在多个 owner 的情况 */
static int clusterManagerFixMultipleSlotOwners(int slot, list *owners) {
    clusterManagerLogInfo(">>> Fixing multiple owners for slot %d...\n", slot);
    int success = 0;
    assert(listLength(owners) > 1);
    clusterManagerNode *owner = clusterManagerGetNodeWithMostKeysInSlot(owners,
                                                                        slot,
                                                                        NULL);
    if (!owner) owner = listFirst(owners)->value;
    clusterManagerLogInfo(">>> Setting slot %d owner: %s:%d\n",
                          slot, owner->ip, owner->port);
    /* 设置槽位 owner */
    if (!clusterManagerSetSlotOwner(owner, slot, 0)) return 0;
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    /* 更新其他主节点上的配置：将槽位分配给新 owner，
     * 若节点拥有该槽位的 key，则迁移这些 key。 */
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n == owner) continue;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
        int count = clusterManagerCountKeysInSlot(n, slot);
        success = (count >= 0);
        if (!success) break;
        clusterManagerDelSlot(n, slot, 1);
        if (!clusterManagerSetSlot(n, owner, slot, "node", NULL)) return 0;
        if (count > 0) {
            int opts = CLUSTER_MANAGER_OPT_VERBOSE |
                       CLUSTER_MANAGER_OPT_COLD;
            success = clusterManagerMoveSlot(n, owner, slot, opts, NULL);
            if (!success) break;
        }
    }
    return success;
}

/* 检查集群一致性：节点视图、开放槽位、覆盖情况、多 owner 等 */
static int clusterManagerCheckCluster(int quiet) {
    listNode *ln = listFirst(cluster_manager.nodes);
    if (!ln) return 0;
    clusterManagerNode *node = ln->value;
    clusterManagerLogInfo(">>> Performing Cluster Check (using node %s:%d)\n",
                          node->ip, node->port);
    int result = 1, consistent = 0;
    int do_fix = config.cluster_manager_command.flags &
                 CLUSTER_MANAGER_CMD_FLAG_FIX;
    if (!quiet) clusterManagerShowNodes();
    consistent = clusterManagerIsConfigConsistent();
    if (!consistent) {
        sds err = sdsnew("[ERR] Nodes don't agree about configuration!");
        clusterManagerOnError(err);
        result = 0;
    } else {
        clusterManagerLogOk("[OK] All nodes agree about slots "
                            "configuration.\n");
    }
    /* Check open slots */
    clusterManagerLogInfo(">>> Check for open slots...\n");
    listIter li;
    listRewind(cluster_manager.nodes, &li);
    int i;
    dict *open_slots = NULL;
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->migrating != NULL) {
            if (open_slots == NULL)
                open_slots = dictCreate(&clusterManagerDictType);
            sds errstr = sdsempty();
            errstr = sdscatprintf(errstr,
                                "[WARNING] Node %s:%d has slots in "
                                "migrating state ",
                                n->ip,
                                n->port);
            for (i = 0; i < n->migrating_count; i += 2) {
                sds slot = n->migrating[i];
                dictReplace(open_slots, slot, sdsdup(n->migrating[i + 1]));
                char *fmt = (i > 0 ? ",%S" : "%S");
                errstr = sdscatfmt(errstr, fmt, slot);
            }
            errstr = sdscat(errstr, ".");
            clusterManagerOnError(errstr);
        }
        if (n->importing != NULL) {
            if (open_slots == NULL)
                open_slots = dictCreate(&clusterManagerDictType);
            sds errstr = sdsempty();
            errstr = sdscatprintf(errstr,
                                "[WARNING] Node %s:%d has slots in "
                                "importing state ",
                                n->ip,
                                n->port);
            for (i = 0; i < n->importing_count; i += 2) {
                sds slot = n->importing[i];
                dictReplace(open_slots, slot, sdsdup(n->importing[i + 1]));
                char *fmt = (i > 0 ? ",%S" : "%S");
                errstr = sdscatfmt(errstr, fmt, slot);
            }
            errstr = sdscat(errstr, ".");
            clusterManagerOnError(errstr);
        }
    }
    if (open_slots != NULL) {
        result = 0;
        dictIterator *iter = dictGetIterator(open_slots);
        dictEntry *entry;
        sds errstr = sdsnew("[WARNING] The following slots are open: ");
        i = 0;
        while ((entry = dictNext(iter)) != NULL) {
            sds slot = (sds) dictGetKey(entry);
            char *fmt = (i++ > 0 ? ",%S" : "%S");
            errstr = sdscatfmt(errstr, fmt, slot);
        }
        clusterManagerLogErr("%s.\n", (char *) errstr);
        sdsfree(errstr);
        if (do_fix) {
            /* Fix open slots. */
            dictReleaseIterator(iter);
            iter = dictGetIterator(open_slots);
            while ((entry = dictNext(iter)) != NULL) {
                sds slot = (sds) dictGetKey(entry);
                result = clusterManagerFixOpenSlot(atoi(slot));
                if (!result) break;
            }
        }
        dictReleaseIterator(iter);
        dictRelease(open_slots);
    }
    clusterManagerLogInfo(">>> Check slots coverage...\n");
    char slots[CLUSTER_MANAGER_SLOTS];
    memset(slots, 0, CLUSTER_MANAGER_SLOTS);
    int coverage = clusterManagerGetCoveredSlots(slots);
    if (coverage == CLUSTER_MANAGER_SLOTS) {
        clusterManagerLogOk("[OK] All %d slots covered.\n",
                            CLUSTER_MANAGER_SLOTS);
    } else {
        sds err = sdsempty();
        err = sdscatprintf(err, "[ERR] Not all %d slots are "
                                "covered by nodes.\n",
                                CLUSTER_MANAGER_SLOTS);
        clusterManagerOnError(err);
        result = 0;
        if (do_fix/* && result*/) {
            dictType dtype = clusterManagerDictType;
            dtype.keyDestructor = dictSdsDestructor;
            dtype.valDestructor = dictListDestructor;
            clusterManagerUncoveredSlots = dictCreate(&dtype);
            int fixed = clusterManagerFixSlotsCoverage(slots);
            if (fixed > 0) result = 1;
        }
    }
    int search_multiple_owners = config.cluster_manager_command.flags &
                                 CLUSTER_MANAGER_CMD_FLAG_CHECK_OWNERS;
    if (search_multiple_owners) {
        /* Check whether there are multiple owners, even when slots are
         * fully covered and there are no open slots. */
        clusterManagerLogInfo(">>> Check for multiple slot owners...\n");
        int slot = 0, slots_with_multiple_owners = 0;
        for (; slot < CLUSTER_MANAGER_SLOTS; slot++) {
            listIter li;
            listNode *ln;
            listRewind(cluster_manager.nodes, &li);
            list *owners = listCreate();
            while ((ln = listNext(&li)) != NULL) {
                clusterManagerNode *n = ln->value;
                if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
                if (n->slots[slot]) listAddNodeTail(owners, n);
                else {
                    /* Nodes having keys for the slot will be considered
                     * owners too. */
                    int count = clusterManagerCountKeysInSlot(n, slot);
                    if (count > 0) listAddNodeTail(owners, n);
                }
            }
            if (listLength(owners) > 1) {
                result = 0;
                clusterManagerLogErr("[WARNING] Slot %d has %d owners:\n",
                                     slot, listLength(owners));
                listRewind(owners, &li);
                while ((ln = listNext(&li)) != NULL) {
                    clusterManagerNode *n = ln->value;
                    clusterManagerLogErr("    %s:%d\n", n->ip, n->port);
                }
                slots_with_multiple_owners++;
                if (do_fix) {
                    result = clusterManagerFixMultipleSlotOwners(slot, owners);
                    if (!result) {
                        clusterManagerLogErr("Failed to fix multiple owners "
                                             "for slot %d\n", slot);
                        listRelease(owners);
                        break;
                    } else slots_with_multiple_owners--;
                }
            }
            listRelease(owners);
        }
        if (slots_with_multiple_owners == 0)
            clusterManagerLogOk("[OK] No multiple owners found.\n");
    }
    return result;
}

/* 根据 ID 获取 reshard 源/目标节点，并校验其有效性 */
static clusterManagerNode *clusterNodeForResharding(char *id,
                                                    clusterManagerNode *target,
                                                    int *raise_err)
{
    clusterManagerNode *node = NULL;
    const char *invalid_node_msg = "*** The specified node (%s) is not known "
                                   "or not a master, please retry.\n";
    node = clusterManagerNodeByName(id);
    *raise_err = 0;
    if (!node || node->flags & CLUSTER_MANAGER_FLAG_SLAVE) {
        clusterManagerLogErr(invalid_node_msg, id);
        *raise_err = 1;
        return NULL;
    } else if (target != NULL) {
        if (!strcmp(node->name, target->name)) {
            clusterManagerLogErr( "*** It is not possible to use "
                                  "the target node as "
                                  "source node.\n");
            return NULL;
        }
    }
    return node;
}

/* 计算 reshard 表：根据源节点列表和要迁移的槽位数生成迁移条目 */
static list *clusterManagerComputeReshardTable(list *sources, int numslots) {
    list *moved = listCreate();
    int src_count = listLength(sources), i = 0, tot_slots = 0, j;
    clusterManagerNode **sorted = zmalloc(src_count * sizeof(*sorted));
    listIter li;
    listNode *ln;
    listRewind(sources, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *node = ln->value;
        tot_slots += node->slots_count;
        sorted[i++] = node;
    }
    qsort(sorted, src_count, sizeof(clusterManagerNode *),
          clusterManagerSlotCountCompareDesc);
    for (i = 0; i < src_count; i++) {
        clusterManagerNode *node = sorted[i];
        float n = ((float) numslots / tot_slots * node->slots_count);
        if (i == 0) n = ceil(n);
        else n = floor(n);
        int max = (int) n, count = 0;
        for (j = 0; j < CLUSTER_MANAGER_SLOTS; j++) {
            int slot = node->slots[j];
            if (!slot) continue;
            if (count >= max || (int)listLength(moved) >= numslots) break;
            clusterManagerReshardTableItem *item = zmalloc(sizeof(*item));
            item->source = node;
            item->slot = j;
            listAddNodeTail(moved, item);
            count++;
        }
    }
    zfree(sorted);
    return moved;
}

/* 显示 reshard 表的内容 */
static void clusterManagerShowReshardTable(list *table) {
    listIter li;
    listNode *ln;
    listRewind(table, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerReshardTableItem *item = ln->value;
        clusterManagerNode *n = item->source;
        printf("    Moving slot %d from %s\n", item->slot, (char *) n->name);
    }
}

/* 释放 reshard 表 */
static void clusterManagerReleaseReshardTable(list *table) {
    if (table != NULL) {
        listIter li;
        listNode *ln;
        listRewind(table, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerReshardTableItem *item = ln->value;
            zfree(item);
        }
        listRelease(table);
    }
}

/* 集群管理器日志输出函数，根据 level 与颜色标志输出带颜色的日志 */
static void clusterManagerLog(int level, const char* fmt, ...) {
    int use_colors =
        (config.cluster_manager_command.flags & CLUSTER_MANAGER_CMD_FLAG_COLOR);
    if (use_colors) {
        printf("\033[");
        switch (level) {
        case CLUSTER_MANAGER_LOG_LVL_INFO: printf(LOG_COLOR_BOLD); break;
        case CLUSTER_MANAGER_LOG_LVL_WARN: printf(LOG_COLOR_YELLOW); break;
        case CLUSTER_MANAGER_LOG_LVL_ERR: printf(LOG_COLOR_RED); break;
        case CLUSTER_MANAGER_LOG_LVL_SUCCESS: printf(LOG_COLOR_GREEN); break;
        default: printf(LOG_COLOR_RESET); break;
        }
    }
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    if (use_colors) printf("\033[" LOG_COLOR_RESET);
}

static void clusterManagerNodeArrayInit(clusterManagerNodeArray *array,
                                        int alloc_len)
{
    array->nodes = zcalloc(alloc_len * sizeof(clusterManagerNode*));
    array->alloc = array->nodes;
    array->len = alloc_len;
    array->count = 0;
}

/* Reset array->nodes to the original array allocation and re-count non-NULL
 * nodes. */
static void clusterManagerNodeArrayReset(clusterManagerNodeArray *array) {
    if (array->nodes > array->alloc) {
        array->len = array->nodes - array->alloc;
        array->nodes = array->alloc;
        array->count = 0;
        int i = 0;
        for(; i < array->len; i++) {
            if (array->nodes[i] != NULL) array->count++;
        }
    }
}

/* Shift array->nodes and store the shifted node into 'nodeptr'. */
static void clusterManagerNodeArrayShift(clusterManagerNodeArray *array,
                                         clusterManagerNode **nodeptr)
{
    assert(array->len > 0);
    /* If the first node to be shifted is not NULL, decrement count. */
    if (*array->nodes != NULL) array->count--;
    /* Store the first node to be shifted into 'nodeptr'. */
    *nodeptr = *array->nodes;
    /* Shift the nodes array and decrement length. */
    array->nodes++;
    array->len--;
}

static void clusterManagerNodeArrayAdd(clusterManagerNodeArray *array,
                                       clusterManagerNode *node)
{
    assert(array->len > 0);
    assert(node != NULL);
    assert(array->count < array->len);
    array->nodes[array->count++] = node;
}

static void clusterManagerPrintNotEmptyNodeError(clusterManagerNode *node,
                                                 char *err)
{
    char *msg;
    if (err) msg = err;
    else {
        msg = "is not empty. Either the node already knows other "
              "nodes (check with CLUSTER NODES) or contains some "
              "key in database 0.";
    }
    clusterManagerLogErr("[ERR] Node %s:%d %s\n", node->ip, node->port, msg);
}

static void clusterManagerPrintNotClusterNodeError(clusterManagerNode *node,
                                                   char *err)
{
    char *msg = (err ? err : "is not configured as a cluster node.");
    clusterManagerLogErr("[ERR] Node %s:%d %s\n", node->ip, node->port, msg);
}

/* 以 Cluster Manager 模式执行 redis-cli */
static void clusterManagerMode(clusterManagerCommandProc *proc) {
    int argc = config.cluster_manager_command.argc;
    char **argv = config.cluster_manager_command.argv;
    cluster_manager.nodes = NULL;
    int success = proc(argc, argv);

    /* Initialized in createClusterManagerCommand. */
    if (config.stdin_lastarg) {
        zfree(config.cluster_manager_command.argv);
        sdsfree(config.cluster_manager_command.stdin_arg);
    } else if (config.stdin_tag_arg) {
        sdsfree(config.cluster_manager_command.stdin_arg);
    }
    freeClusterManager();

    exit(success ? 0 : 1);
}

/* 集群管理命令集合 */

/* 创建集群（--cluster create） */
static int clusterManagerCommandCreate(int argc, char **argv) {
    int i, j, success = 1;
    cluster_manager.nodes = listCreate();
    for (i = 0; i < argc; i++) {
        char *addr = argv[i];
        char *ip = NULL;
        int port = 0;
        if (!parseClusterNodeAddress(addr, &ip, &port, NULL)) {
            fprintf(stderr, "Invalid address format: %s\n", addr);
            return 0;
        }

        clusterManagerNode *node = clusterManagerNewNode(ip, port, 0);
        if (!clusterManagerNodeConnect(node)) {
            freeClusterManagerNode(node);
            return 0;
        }
        char *err = NULL;
        if (!clusterManagerNodeIsCluster(node, &err)) {
            clusterManagerPrintNotClusterNodeError(node, err);
            if (err) zfree(err);
            freeClusterManagerNode(node);
            return 0;
        }
        err = NULL;
        if (!clusterManagerNodeLoadInfo(node, 0, &err)) {
            if (err) {
                CLUSTER_MANAGER_PRINT_REPLY_ERROR(node, err);
                zfree(err);
            }
            freeClusterManagerNode(node);
            return 0;
        }
        err = NULL;
        if (!clusterManagerNodeIsEmpty(node, &err)) {
            clusterManagerPrintNotEmptyNodeError(node, err);
            if (err) zfree(err);
            freeClusterManagerNode(node);
            return 0;
        }
        listAddNodeTail(cluster_manager.nodes, node);
    }
    int node_len = cluster_manager.nodes->len;
    int replicas = config.cluster_manager_command.replicas;
    int masters_count = CLUSTER_MANAGER_MASTERS_COUNT(node_len, replicas);
    if (masters_count < 3) {
        clusterManagerLogErr(
            "*** ERROR: Invalid configuration for cluster creation.\n"
            "*** Redis Cluster requires at least 3 master nodes.\n"
            "*** This is not possible with %d nodes and %d replicas per node.",
            node_len, replicas);
        clusterManagerLogErr("\n*** At least %d nodes are required.\n",
                             3 * (replicas + 1));
        return 0;
    }
    clusterManagerLogInfo(">>> Performing hash slots allocation "
                          "on %d nodes...\n", node_len);
    int interleaved_len = 0, ip_count = 0;
    clusterManagerNode **interleaved = zcalloc(node_len*sizeof(**interleaved));
    char **ips = zcalloc(node_len * sizeof(char*));
    clusterManagerNodeArray *ip_nodes = zcalloc(node_len * sizeof(*ip_nodes));
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        int found = 0;
        for (i = 0; i < ip_count; i++) {
            char *ip = ips[i];
            if (!strcmp(ip, n->ip)) {
                found = 1;
                break;
            }
        }
        if (!found) {
            ips[ip_count++] = n->ip;
        }
        clusterManagerNodeArray *node_array = &(ip_nodes[i]);
        if (node_array->nodes == NULL)
            clusterManagerNodeArrayInit(node_array, node_len);
        clusterManagerNodeArrayAdd(node_array, n);
    }
    while (interleaved_len < node_len) {
        for (i = 0; i < ip_count; i++) {
            clusterManagerNodeArray *node_array = &(ip_nodes[i]);
            if (node_array->count > 0) {
                clusterManagerNode *n = NULL;
                clusterManagerNodeArrayShift(node_array, &n);
                interleaved[interleaved_len++] = n;
            }
        }
    }
    clusterManagerNode **masters = interleaved;
    interleaved += masters_count;
    interleaved_len -= masters_count;
    float slots_per_node = CLUSTER_MANAGER_SLOTS / (float) masters_count;
    long first = 0;
    float cursor = 0.0f;
    for (i = 0; i < masters_count; i++) {
        clusterManagerNode *master = masters[i];
        long last = lround(cursor + slots_per_node - 1);
        if (last > CLUSTER_MANAGER_SLOTS || i == (masters_count - 1))
            last = CLUSTER_MANAGER_SLOTS - 1;
        if (last < first) last = first;
        printf("Master[%d] -> Slots %ld - %ld\n", i, first, last);
        master->slots_count = 0;
        for (j = first; j <= last; j++) {
            master->slots[j] = 1;
            master->slots_count++;
        }
        master->dirty = 1;
        first = last + 1;
        cursor += slots_per_node;
    }

    /* Rotating the list sometimes helps to get better initial
     * anti-affinity before the optimizer runs. */
    clusterManagerNode *first_node = interleaved[0];
    for (i = 0; i < (interleaved_len - 1); i++)
        interleaved[i] = interleaved[i + 1];
    interleaved[interleaved_len - 1] = first_node;
    int assign_unused = 0, available_count = interleaved_len;
assign_replicas:
    for (i = 0; i < masters_count; i++) {
        clusterManagerNode *master = masters[i];
        int assigned_replicas = 0;
        while (assigned_replicas < replicas) {
            if (available_count == 0) break;
            clusterManagerNode *found = NULL, *slave = NULL;
            int firstNodeIdx = -1;
            for (j = 0; j < interleaved_len; j++) {
                clusterManagerNode *n = interleaved[j];
                if (n == NULL) continue;
                if (strcmp(n->ip, master->ip)) {
                    found = n;
                    interleaved[j] = NULL;
                    break;
                }
                if (firstNodeIdx < 0) firstNodeIdx = j;
            }
            if (found) slave = found;
            else if (firstNodeIdx >= 0) {
                slave = interleaved[firstNodeIdx];
                interleaved_len -= (firstNodeIdx + 1);
                interleaved += (firstNodeIdx + 1);
            }
            if (slave != NULL) {
                assigned_replicas++;
                available_count--;
                if (slave->replicate) sdsfree(slave->replicate);
                slave->replicate = sdsnew(master->name);
                slave->dirty = 1;
            } else break;
            printf("Adding replica %s:%d to %s:%d\n", slave->ip, slave->port,
                   master->ip, master->port);
            if (assign_unused) break;
        }
    }
    if (!assign_unused && available_count > 0) {
        assign_unused = 1;
        printf("Adding extra replicas...\n");
        goto assign_replicas;
    }
    for (i = 0; i < ip_count; i++) {
        clusterManagerNodeArray *node_array = ip_nodes + i;
        clusterManagerNodeArrayReset(node_array);
    }
    clusterManagerOptimizeAntiAffinity(ip_nodes, ip_count);
    clusterManagerShowNodes();
    int ignore_force = 0;
    if (confirmWithYes("Can I set the above configuration?", ignore_force)) {
        listRewind(cluster_manager.nodes, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *node = ln->value;
            char *err = NULL;
            int flushed = clusterManagerFlushNodeConfig(node, &err);
            if (!flushed && node->dirty && !node->replicate) {
                if (err != NULL) {
                    CLUSTER_MANAGER_PRINT_REPLY_ERROR(node, err);
                    zfree(err);
                }
                success = 0;
                goto cleanup;
            } else if (err != NULL) zfree(err);
        }
        clusterManagerLogInfo(">>> Nodes configuration updated\n");
        clusterManagerLogInfo(">>> Assign a different config epoch to "
                              "each node\n");
        int config_epoch = 1;
        listRewind(cluster_manager.nodes, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *node = ln->value;
            redisReply *reply = NULL;
            reply = CLUSTER_MANAGER_COMMAND(node,
                                            "cluster set-config-epoch %d",
                                            config_epoch++);
            if (reply != NULL) freeReplyObject(reply);
        }
        clusterManagerLogInfo(">>> Sending CLUSTER MEET messages to join "
                              "the cluster\n");
        clusterManagerNode *first = NULL;
        char first_ip[NET_IP_STR_LEN]; /* first->ip may be a hostname */
        listRewind(cluster_manager.nodes, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *node = ln->value;
            if (first == NULL) {
                first = node;
                /* Although hiredis supports connecting to a hostname, CLUSTER
                 * MEET requires an IP address, so we do a DNS lookup here. */
                int anet_flags = ANET_NONE;
                if (config.prefer_ipv4) anet_flags |= ANET_PREFER_IPV4;
                if (config.prefer_ipv6) anet_flags |= ANET_PREFER_IPV6;
                if (anetResolve(NULL, first->ip, first_ip, sizeof(first_ip), anet_flags)
                    == ANET_ERR)
                {
                    fprintf(stderr, "Invalid IP address or hostname specified: %s\n", first->ip);
                    success = 0;
                    goto cleanup;
                }
                continue;
            }
            redisReply *reply = NULL;
            if (first->bus_port == 0 || (first->bus_port == first->port + CLUSTER_MANAGER_PORT_INCR)) {
                /* CLUSTER MEET bus-port parameter was added in 4.0.
                 * So if (bus_port == 0) or (bus_port == port + CLUSTER_MANAGER_PORT_INCR),
                 * we just call CLUSTER MEET with 2 arguments, using the old form. */
                reply = CLUSTER_MANAGER_COMMAND(node, "cluster meet %s %d",
                                                first_ip, first->port);
            } else {
                reply = CLUSTER_MANAGER_COMMAND(node, "cluster meet %s %d %d",
                                                first_ip, first->port, first->bus_port);
            }
            int is_err = 0;
            if (reply != NULL) {
                if ((is_err = reply->type == REDIS_REPLY_ERROR))
                    CLUSTER_MANAGER_PRINT_REPLY_ERROR(node, reply->str);
                freeReplyObject(reply);
            } else {
                is_err = 1;
                fprintf(stderr, "Failed to send CLUSTER MEET command.\n");
            }
            if (is_err) {
                success = 0;
                goto cleanup;
            }
        }
        /* Give one second for the join to start, in order to avoid that
         * waiting for cluster join will find all the nodes agree about
         * the config as they are still empty with unassigned slots. */
        sleep(1);
        clusterManagerWaitForClusterJoin();
        /* Useful for the replicas */
        listRewind(cluster_manager.nodes, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *node = ln->value;
            if (!node->dirty) continue;
            char *err = NULL;
            int flushed = clusterManagerFlushNodeConfig(node, &err);
            if (!flushed && !node->replicate) {
                if (err != NULL) {
                    CLUSTER_MANAGER_PRINT_REPLY_ERROR(node, err);
                    zfree(err);
                }
                success = 0;
                goto cleanup;
            } else if (err != NULL) {
                zfree(err);
            }
        }
        // Reset Nodes
        listRewind(cluster_manager.nodes, &li);
        clusterManagerNode *first_node = NULL;
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *node = ln->value;
            if (!first_node) first_node = node;
            else freeClusterManagerNode(node);
        }
        listEmpty(cluster_manager.nodes);
        if (!clusterManagerLoadInfoFromNode(first_node)) {
            success = 0;
            goto cleanup;
        }
        clusterManagerCheckCluster(0);
    }
cleanup:
    /* Free everything */
    zfree(masters);
    zfree(ips);
    for (i = 0; i < node_len; i++) {
        clusterManagerNodeArray *node_array = ip_nodes + i;
        CLUSTER_MANAGER_NODE_ARRAY_FREE(node_array);
    }
    zfree(ip_nodes);
    return success;
}

/* 添加节点（--cluster add-node） */
static int clusterManagerCommandAddNode(int argc, char **argv) {
    int success = 1;
    redisReply *reply = NULL;
    redisReply *function_restore_reply = NULL;
    redisReply *function_list_reply = NULL;
    char *ref_ip = NULL, *ip = NULL;
    int ref_port = 0, port = 0;
    if (!getClusterHostFromCmdArgs(argc - 1, argv + 1, &ref_ip, &ref_port))
        goto invalid_args;
    if (!getClusterHostFromCmdArgs(1, argv, &ip, &port))
        goto invalid_args;
    clusterManagerLogInfo(">>> Adding node %s:%d to cluster %s:%d\n", ip, port,
                          ref_ip, ref_port);
    // Check the existing cluster
    clusterManagerNode *refnode = clusterManagerNewNode(ref_ip, ref_port, 0);
    if (!clusterManagerLoadInfoFromNode(refnode)) return 0;
    if (!clusterManagerCheckCluster(0)) return 0;

    /* If --cluster-master-id was specified, try to resolve it now so that we
     * abort before starting with the node configuration. */
    clusterManagerNode *master_node = NULL;
    if (config.cluster_manager_command.flags & CLUSTER_MANAGER_CMD_FLAG_SLAVE) {
        char *master_id = config.cluster_manager_command.master_id;
        if (master_id != NULL) {
            master_node = clusterManagerNodeByName(master_id);
            if (master_node == NULL) {
                clusterManagerLogErr("[ERR] No such master ID %s\n", master_id);
                return 0;
            }
        } else {
            master_node = clusterManagerNodeWithLeastReplicas();
            assert(master_node != NULL);
            printf("Automatically selected master %s:%d\n", master_node->ip,
                   master_node->port);
        }
    }

    // Add the new node
    clusterManagerNode *new_node = clusterManagerNewNode(ip, port, 0);
    int added = 0;
    if (!clusterManagerNodeConnect(new_node)) {
        clusterManagerLogErr("[ERR] Sorry, can't connect to node %s:%d\n",
                             ip, port);
        success = 0;
        goto cleanup;
    }
    char *err = NULL;
    if (!(success = clusterManagerNodeIsCluster(new_node, &err))) {
        clusterManagerPrintNotClusterNodeError(new_node, err);
        if (err) zfree(err);
        goto cleanup;
    }
    if (!clusterManagerNodeLoadInfo(new_node, 0, &err)) {
        if (err) {
            CLUSTER_MANAGER_PRINT_REPLY_ERROR(new_node, err);
            zfree(err);
        }
        success = 0;
        goto cleanup;
    }
    if (!(success = clusterManagerNodeIsEmpty(new_node, &err))) {
        clusterManagerPrintNotEmptyNodeError(new_node, err);
        if (err) zfree(err);
        goto cleanup;
    }
    clusterManagerNode *first = listFirst(cluster_manager.nodes)->value;
    listAddNodeTail(cluster_manager.nodes, new_node);
    added = 1;

    if (!master_node) {
        /* Send functions to the new node, if new node is a replica it will get the functions from its primary. */
        clusterManagerLogInfo(">>> Getting functions from cluster\n");
        reply = CLUSTER_MANAGER_COMMAND(refnode, "FUNCTION DUMP");
        if (!clusterManagerCheckRedisReply(refnode, reply, &err)) {
            clusterManagerLogInfo(">>> Failed retrieving Functions from the cluster, "
                    "skip this step as Redis version do not support function command (error = '%s')\n", err? err : "NULL reply");
            if (err) zfree(err);
        } else {
            assert(reply->type == REDIS_REPLY_STRING);
            clusterManagerLogInfo(">>> Send FUNCTION LIST to %s:%d to verify there is no functions in it\n", ip, port);
            function_list_reply = CLUSTER_MANAGER_COMMAND(new_node, "FUNCTION LIST");
            if (!clusterManagerCheckRedisReply(new_node, function_list_reply, &err)) {
                clusterManagerLogErr(">>> Failed on CLUSTER LIST (error = '%s')\r\n", err? err : "NULL reply");
                if (err) zfree(err);
                success = 0;
                goto cleanup;
            }
            assert(function_list_reply->type == REDIS_REPLY_ARRAY);
            if (function_list_reply->elements > 0) {
                clusterManagerLogErr(">>> New node already contains functions and can not be added to the cluster. Use FUNCTION FLUSH and try again.\r\n");
                success = 0;
                goto cleanup;
            }
            clusterManagerLogInfo(">>> Send FUNCTION RESTORE to %s:%d\n", ip, port);
            function_restore_reply = CLUSTER_MANAGER_COMMAND(new_node, "FUNCTION RESTORE %b", reply->str, reply->len);
            if (!clusterManagerCheckRedisReply(new_node, function_restore_reply, &err)) {
                clusterManagerLogErr(">>> Failed loading functions to the new node (error = '%s')\r\n", err? err : "NULL reply");
                if (err) zfree(err);
                success = 0;
                goto cleanup;
            }
        }
    }

    if (reply) freeReplyObject(reply);

    // Send CLUSTER MEET command to the new node
    clusterManagerLogInfo(">>> Send CLUSTER MEET to node %s:%d to make it "
                          "join the cluster.\n", ip, port);
    /* CLUSTER MEET requires an IP address, so we do a DNS lookup here. */
    char first_ip[NET_IP_STR_LEN];
    int anet_flags = ANET_NONE;
    if (config.prefer_ipv4) anet_flags |= ANET_PREFER_IPV4;
    if (config.prefer_ipv6) anet_flags |= ANET_PREFER_IPV6;
    if (anetResolve(NULL, first->ip, first_ip, sizeof(first_ip), anet_flags) == ANET_ERR) {
        fprintf(stderr, "Invalid IP address or hostname specified: %s\n", first->ip);
        success = 0;
        goto cleanup;
    }

    if (first->bus_port == 0 || (first->bus_port == first->port + CLUSTER_MANAGER_PORT_INCR)) {
        /* CLUSTER MEET bus-port parameter was added in 4.0.
         * So if (bus_port == 0) or (bus_port == port + CLUSTER_MANAGER_PORT_INCR),
         * we just call CLUSTER MEET with 2 arguments, using the old form. */
        reply = CLUSTER_MANAGER_COMMAND(new_node, "CLUSTER MEET %s %d",
                                        first_ip, first->port);
    } else {
        reply = CLUSTER_MANAGER_COMMAND(new_node, "CLUSTER MEET %s %d %d",
                                        first_ip, first->port, first->bus_port);
    }

    if (!(success = clusterManagerCheckRedisReply(new_node, reply, NULL)))
        goto cleanup;

    /* Additional configuration is needed if the node is added as a slave. */
    if (master_node) {
        sleep(1);
        clusterManagerWaitForClusterJoin();
        clusterManagerLogInfo(">>> Configure node as replica of %s:%d.\n",
                              master_node->ip, master_node->port);
        freeReplyObject(reply);
        reply = CLUSTER_MANAGER_COMMAND(new_node, "CLUSTER REPLICATE %s",
                                        master_node->name);
        if (!(success = clusterManagerCheckRedisReply(new_node, reply, NULL)))
            goto cleanup;
    }
    clusterManagerLogOk("[OK] New node added correctly.\n");
cleanup:
    if (!added && new_node) freeClusterManagerNode(new_node);
    if (reply) freeReplyObject(reply);
    if (function_restore_reply) freeReplyObject(function_restore_reply);
    if (function_list_reply) freeReplyObject(function_list_reply);
    return success;
invalid_args:
    fprintf(stderr, CLUSTER_MANAGER_INVALID_HOST_ARG);
    return 0;
}

/* 删除节点（--cluster del-node） */
static int clusterManagerCommandDeleteNode(int argc, char **argv) {
    UNUSED(argc);
    int success = 1;
    int port = 0;
    char *ip = NULL;
    if (!getClusterHostFromCmdArgs(1, argv, &ip, &port)) goto invalid_args;
    char *node_id = argv[1];
    clusterManagerLogInfo(">>> Removing node %s from cluster %s:%d\n",
                          node_id, ip, port);
    clusterManagerNode *ref_node = clusterManagerNewNode(ip, port, 0);
    clusterManagerNode *node = NULL;

    // Load cluster information
    if (!clusterManagerLoadInfoFromNode(ref_node)) return 0;

    // Check if the node exists and is not empty
    node = clusterManagerNodeByName(node_id);
    if (node == NULL) {
        clusterManagerLogErr("[ERR] No such node ID %s\n", node_id);
        return 0;
    }
    if (node->slots_count != 0) {
        clusterManagerLogErr("[ERR] Node %s:%d is not empty! Reshard data "
                             "away and try again.\n", node->ip, node->port);
        return 0;
    }

    // Send CLUSTER FORGET to all the nodes but the node to remove
    clusterManagerLogInfo(">>> Sending CLUSTER FORGET messages to the "
                          "cluster...\n");
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n == node) continue;
        if (n->replicate && !strcasecmp(n->replicate, node_id)) {
            // Reconfigure the slave to replicate with some other node
            clusterManagerNode *master = clusterManagerNodeWithLeastReplicas();
            assert(master != NULL);
            clusterManagerLogInfo(">>> %s:%d as replica of %s:%d\n",
                                  n->ip, n->port, master->ip, master->port);
            redisReply *r = CLUSTER_MANAGER_COMMAND(n, "CLUSTER REPLICATE %s",
                                                    master->name);
            success = clusterManagerCheckRedisReply(n, r, NULL);
            if (r) freeReplyObject(r);
            if (!success) return 0;
        }
        redisReply *r = CLUSTER_MANAGER_COMMAND(n, "CLUSTER FORGET %s",
                                                node_id);
        success = clusterManagerCheckRedisReply(n, r, NULL);
        if (r) freeReplyObject(r);
        if (!success) return 0;
    }

    /* Finally send CLUSTER RESET to the node. */
    clusterManagerLogInfo(">>> Sending CLUSTER RESET SOFT to the "
                          "deleted node.\n");
    redisReply *r = redisCommand(node->context, "CLUSTER RESET %s", "SOFT");
    success = clusterManagerCheckRedisReply(node, r, NULL);
    if (r) freeReplyObject(r);
    return success;
invalid_args:
    fprintf(stderr, CLUSTER_MANAGER_INVALID_HOST_ARG);
    return 0;
}

/* 显示集群简要信息（--cluster info） */
static int clusterManagerCommandInfo(int argc, char **argv) {
    int port = 0;
    char *ip = NULL;
    if (!getClusterHostFromCmdArgs(argc, argv, &ip, &port)) goto invalid_args;
    clusterManagerNode *node = clusterManagerNewNode(ip, port, 0);
    if (!clusterManagerLoadInfoFromNode(node)) return 0;
    clusterManagerShowClusterInfo();
    return 1;
invalid_args:
    fprintf(stderr, CLUSTER_MANAGER_INVALID_HOST_ARG);
    return 0;
}

/* 检查集群（--cluster check） */
static int clusterManagerCommandCheck(int argc, char **argv) {
    int port = 0;
    char *ip = NULL;
    if (!getClusterHostFromCmdArgs(argc, argv, &ip, &port)) goto invalid_args;
    clusterManagerNode *node = clusterManagerNewNode(ip, port, 0);
    if (!clusterManagerLoadInfoFromNode(node)) return 0;
    clusterManagerShowClusterInfo();
    return clusterManagerCheckCluster(0);
invalid_args:
    fprintf(stderr, CLUSTER_MANAGER_INVALID_HOST_ARG);
    return 0;
}

/* 修复集群（--cluster fix） */
static int clusterManagerCommandFix(int argc, char **argv) {
    config.cluster_manager_command.flags |= CLUSTER_MANAGER_CMD_FLAG_FIX;
    return clusterManagerCommandCheck(argc, argv);
}

/* 槽位重新分片（--cluster reshard） */
static int clusterManagerCommandReshard(int argc, char **argv) {
    int port = 0;
    char *ip = NULL;
    if (!getClusterHostFromCmdArgs(argc, argv, &ip, &port)) goto invalid_args;
    clusterManagerNode *node = clusterManagerNewNode(ip, port, 0);
    if (!clusterManagerLoadInfoFromNode(node)) return 0;
    clusterManagerCheckCluster(0);
    if (cluster_manager.errors && listLength(cluster_manager.errors) > 0) {
        fflush(stdout);
        fprintf(stderr,
                "*** Please fix your cluster problems before resharding\n");
        return 0;
    }
    int slots = config.cluster_manager_command.slots;
    if (!slots) {
        while (slots <= 0 || slots > CLUSTER_MANAGER_SLOTS) {
            printf("How many slots do you want to move (from 1 to %d)? ",
                   CLUSTER_MANAGER_SLOTS);
            fflush(stdout);
            char buf[6];
            int nread = read(fileno(stdin),buf,6);
            if (nread <= 0) continue;
            int last_idx = nread - 1;
            if (buf[last_idx] != '\n') {
                int ch;
                while ((ch = getchar()) != '\n' && ch != EOF) {}
            }
            buf[last_idx] = '\0';
            slots = atoi(buf);
        }
    }
    char buf[255];
    char *to = config.cluster_manager_command.to,
         *from = config.cluster_manager_command.from;
    while (to == NULL) {
        printf("What is the receiving node ID? ");
        fflush(stdout);
        int nread = read(fileno(stdin),buf,255);
        if (nread <= 0) continue;
        int last_idx = nread - 1;
        if (buf[last_idx] != '\n') {
            int ch;
            while ((ch = getchar()) != '\n' && ch != EOF) {}
        }
        buf[last_idx] = '\0';
        if (strlen(buf) > 0) to = buf;
    }
    int raise_err = 0;
    clusterManagerNode *target = clusterNodeForResharding(to, NULL, &raise_err);
    if (target == NULL) return 0;
    list *sources = listCreate();
    list *table = NULL;
    int all = 0, result = 1;
    if (from == NULL) {
        printf("Please enter all the source node IDs.\n");
        printf("  Type 'all' to use all the nodes as source nodes for "
               "the hash slots.\n");
        printf("  Type 'done' once you entered all the source nodes IDs.\n");
        while (1) {
            printf("Source node #%lu: ", listLength(sources) + 1);
            fflush(stdout);
            int nread = read(fileno(stdin),buf,255);
            if (nread <= 0) continue;
            int last_idx = nread - 1;
            if (buf[last_idx] != '\n') {
                int ch;
                while ((ch = getchar()) != '\n' && ch != EOF) {}
            }
            buf[last_idx] = '\0';
            if (!strcmp(buf, "done")) break;
            else if (!strcmp(buf, "all")) {
                all = 1;
                break;
            } else {
                clusterManagerNode *src =
                    clusterNodeForResharding(buf, target, &raise_err);
                if (src != NULL) listAddNodeTail(sources, src);
                else if (raise_err) {
                    result = 0;
                    goto cleanup;
                }
            }
        }
    } else {
        char *p;
        while((p = strchr(from, ',')) != NULL) {
            *p = '\0';
            if (!strcmp(from, "all")) {
                all = 1;
                break;
            } else {
                clusterManagerNode *src =
                    clusterNodeForResharding(from, target, &raise_err);
                if (src != NULL) listAddNodeTail(sources, src);
                else if (raise_err) {
                    result = 0;
                    goto cleanup;
                }
            }
            from = p + 1;
        }
        /* Check if there's still another source to process. */
        if (!all && strlen(from) > 0) {
            if (!strcmp(from, "all")) all = 1;
            if (!all) {
                clusterManagerNode *src =
                    clusterNodeForResharding(from, target, &raise_err);
                if (src != NULL) listAddNodeTail(sources, src);
                else if (raise_err) {
                    result = 0;
                    goto cleanup;
                }
            }
        }
    }
    listIter li;
    listNode *ln;
    if (all) {
        listEmpty(sources);
        listRewind(cluster_manager.nodes, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = ln->value;
            if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE || n->replicate)
                continue;
            if (!sdscmp(n->name, target->name)) continue;
            listAddNodeTail(sources, n);
        }
    }
    if (listLength(sources) == 0) {
        fprintf(stderr, "*** No source nodes given, operation aborted.\n");
        result = 0;
        goto cleanup;
    }
    printf("\nReady to move %d slots.\n", slots);
    printf("  Source nodes:\n");
    listRewind(sources, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *src = ln->value;
        sds info = clusterManagerNodeInfo(src, 4);
        printf("%s\n", info);
        sdsfree(info);
    }
    printf("  Destination node:\n");
    sds info = clusterManagerNodeInfo(target, 4);
    printf("%s\n", info);
    sdsfree(info);
    table = clusterManagerComputeReshardTable(sources, slots);
    printf("  Resharding plan:\n");
    clusterManagerShowReshardTable(table);
    if (!(config.cluster_manager_command.flags &
          CLUSTER_MANAGER_CMD_FLAG_YES))
    {
        printf("Do you want to proceed with the proposed "
               "reshard plan (yes/no)? ");
        fflush(stdout);
        char buf[4];
        int nread = read(fileno(stdin),buf,4);
        buf[3] = '\0';
        if (nread <= 0 || strcmp("yes", buf) != 0) {
            result = 0;
            goto cleanup;
        }
    }
    int opts = CLUSTER_MANAGER_OPT_VERBOSE;
    listRewind(table, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerReshardTableItem *item = ln->value;
        char *err = NULL;
        result = clusterManagerMoveSlot(item->source, target, item->slot,
                                        opts, &err);
        if (!result) {
            if (err != NULL) {
                clusterManagerLogErr("clusterManagerMoveSlot failed: %s\n", err);
                zfree(err);
            }
            goto cleanup;
        }
    }
cleanup:
    listRelease(sources);
    clusterManagerReleaseReshardTable(table);
    return result;
invalid_args:
    fprintf(stderr, CLUSTER_MANAGER_INVALID_HOST_ARG);
    return 0;
}

/* 集群槽位再平衡（--cluster rebalance） */
static int clusterManagerCommandRebalance(int argc, char **argv) {
    int port = 0;
    char *ip = NULL;
    clusterManagerNode **weightedNodes = NULL;
    list *involved = NULL;
    if (!getClusterHostFromCmdArgs(argc, argv, &ip, &port)) goto invalid_args;
    clusterManagerNode *node = clusterManagerNewNode(ip, port, 0);
    if (!clusterManagerLoadInfoFromNode(node)) return 0;
    int result = 1, i;
    if (config.cluster_manager_command.weight != NULL) {
        for (i = 0; i < config.cluster_manager_command.weight_argc; i++) {
            char *name = config.cluster_manager_command.weight[i];
            char *p = strchr(name, '=');
            if (p == NULL) {
                clusterManagerLogErr("*** invalid input %s\n", name);
                result = 0;
                goto cleanup;
            }
            *p = '\0';
            float w = atof(++p);
            clusterManagerNode *n = clusterManagerNodeByAbbreviatedName(name);
            if (n == NULL) {
                clusterManagerLogErr("*** No such master node %s\n", name);
                result = 0;
                goto cleanup;
            }
            n->weight = w;
        }
    }
    float total_weight = 0;
    int nodes_involved = 0;
    int use_empty = config.cluster_manager_command.flags &
                    CLUSTER_MANAGER_CMD_FLAG_EMPTYMASTER;
    involved = listCreate();
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    /* Compute the total cluster weight. */
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE || n->replicate)
            continue;
        if (!use_empty && n->slots_count == 0) {
            n->weight = 0;
            continue;
        }
        total_weight += n->weight;
        nodes_involved++;
        listAddNodeTail(involved, n);
    }
    weightedNodes = zmalloc(nodes_involved * sizeof(clusterManagerNode *));
    if (weightedNodes == NULL) goto cleanup;
    /* Check cluster, only proceed if it looks sane. */
    clusterManagerCheckCluster(1);
    if (cluster_manager.errors && listLength(cluster_manager.errors) > 0) {
        clusterManagerLogErr("*** Please fix your cluster problems "
                             "before rebalancing\n");
        result = 0;
        goto cleanup;
    }
    /* Calculate the slots balance for each node. It's the number of
     * slots the node should lose (if positive) or gain (if negative)
     * in order to be balanced. */
    int threshold_reached = 0, total_balance = 0;
    float threshold = config.cluster_manager_command.threshold;
    i = 0;
    listRewind(involved, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        weightedNodes[i++] = n;
        int expected = (int) (((float)CLUSTER_MANAGER_SLOTS / total_weight) *
                        n->weight);
        n->balance = n->slots_count - expected;
        total_balance += n->balance;
        /* Compute the percentage of difference between the
         * expected number of slots and the real one, to see
         * if it's over the threshold specified by the user. */
        int over_threshold = 0;
        if (threshold > 0) {
            if (n->slots_count > 0) {
                float err_perc = fabs((100-(100.0*expected/n->slots_count)));
                if (err_perc > threshold) over_threshold = 1;
            } else if (expected > 1) {
                over_threshold = 1;
            }
        }
        if (over_threshold) threshold_reached = 1;
    }
    if (!threshold_reached) {
        clusterManagerLogWarn("*** No rebalancing needed! "
                             "All nodes are within the %.2f%% threshold.\n",
                             config.cluster_manager_command.threshold);
        goto cleanup;
    }
    /* Because of rounding, it is possible that the balance of all nodes
     * summed does not give 0. Make sure that nodes that have to provide
     * slots are always matched by nodes receiving slots. */
    while (total_balance > 0) {
        listRewind(involved, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = ln->value;
            if (n->balance <= 0 && total_balance > 0) {
                n->balance--;
                total_balance--;
            }
        }
    }
    /* Sort nodes by their slots balance. */
    qsort(weightedNodes, nodes_involved, sizeof(clusterManagerNode *),
          clusterManagerCompareNodeBalance);
    clusterManagerLogInfo(">>> Rebalancing across %d nodes. "
                          "Total weight = %.2f\n",
                          nodes_involved, total_weight);
    if (config.verbose) {
        for (i = 0; i < nodes_involved; i++) {
            clusterManagerNode *n = weightedNodes[i];
            printf("%s:%d balance is %d slots\n", n->ip, n->port, n->balance);
        }
    }
    /* Now we have at the start of the 'sn' array nodes that should get
     * slots, at the end nodes that must give slots.
     * We take two indexes, one at the start, and one at the end,
     * incrementing or decrementing the indexes accordingly til we
     * find nodes that need to get/provide slots. */
    int dst_idx = 0;
    int src_idx = nodes_involved - 1;
    int simulate = config.cluster_manager_command.flags &
                   CLUSTER_MANAGER_CMD_FLAG_SIMULATE;
    while (dst_idx < src_idx) {
        clusterManagerNode *dst = weightedNodes[dst_idx];
        clusterManagerNode *src = weightedNodes[src_idx];
        int db = abs(dst->balance);
        int sb = abs(src->balance);
        int numslots = (db < sb ? db : sb);
        if (numslots > 0) {
            printf("Moving %d slots from %s:%d to %s:%d\n", numslots,
                                                            src->ip,
                                                            src->port,
                                                            dst->ip,
                                                            dst->port);
            /* Actually move the slots. */
            list *lsrc = listCreate(), *table = NULL;
            listAddNodeTail(lsrc, src);
            table = clusterManagerComputeReshardTable(lsrc, numslots);
            listRelease(lsrc);
            int table_len = (int) listLength(table);
            if (!table || table_len != numslots) {
                clusterManagerLogErr("*** Assertion failed: Reshard table "
                                     "!= number of slots");
                result = 0;
                goto end_move;
            }
            if (simulate) {
                for (i = 0; i < table_len; i++) printf("#");
            } else {
                int opts = CLUSTER_MANAGER_OPT_QUIET |
                           CLUSTER_MANAGER_OPT_UPDATE;
                listRewind(table, &li);
                while ((ln = listNext(&li)) != NULL) {
                    clusterManagerReshardTableItem *item = ln->value;
                    char *err;
                    result = clusterManagerMoveSlot(item->source,
                                                    dst,
                                                    item->slot,
                                                    opts, &err);
                    if (!result) {
                        clusterManagerLogErr("*** clusterManagerMoveSlot: %s\n", err);
                        zfree(err);
                        goto end_move;
                    }
                    printf("#");
                    fflush(stdout);
                }

            }
            printf("\n");
end_move:
            clusterManagerReleaseReshardTable(table);
            if (!result) goto cleanup;
        }
        /* Update nodes balance. */
        dst->balance += numslots;
        src->balance -= numslots;
        if (dst->balance == 0) dst_idx++;
        if (src->balance == 0) src_idx --;
    }
cleanup:
    if (involved != NULL) listRelease(involved);
    if (weightedNodes != NULL) zfree(weightedNodes);
    return result;
invalid_args:
    fprintf(stderr, CLUSTER_MANAGER_INVALID_HOST_ARG);
    return 0;
}

/* 设置集群节点超时（--cluster set-timeout） */
static int clusterManagerCommandSetTimeout(int argc, char **argv) {
    UNUSED(argc);
    int port = 0;
    char *ip = NULL;
    if (!getClusterHostFromCmdArgs(1, argv, &ip, &port)) goto invalid_args;
    int timeout = atoi(argv[1]);
    if (timeout < 100) {
        fprintf(stderr, "Setting a node timeout of less than 100 "
                "milliseconds is a bad idea.\n");
        return 0;
    }
    // Load cluster information
    clusterManagerNode *node = clusterManagerNewNode(ip, port, 0);
    if (!clusterManagerLoadInfoFromNode(node)) return 0;
    int ok_count = 0, err_count = 0;

    clusterManagerLogInfo(">>> Reconfiguring node timeout in every "
                          "cluster node...\n");
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        char *err = NULL;
        redisReply *reply = CLUSTER_MANAGER_COMMAND(n, "CONFIG %s %s %d",
                                                    "SET",
                                                    "cluster-node-timeout",
                                                    timeout);
        if (reply == NULL) goto reply_err;
        int ok = clusterManagerCheckRedisReply(n, reply, &err);
        freeReplyObject(reply);
        if (!ok) goto reply_err;
        reply = CLUSTER_MANAGER_COMMAND(n, "CONFIG %s", "REWRITE");
        if (reply == NULL) goto reply_err;
        ok = clusterManagerCheckRedisReply(n, reply, &err);
        freeReplyObject(reply);
        if (!ok) goto reply_err;
        clusterManagerLogWarn("*** New timeout set for %s:%d\n", n->ip,
                              n->port);
        ok_count++;
        continue;
reply_err:;
        int need_free = 0;
        if (err == NULL) err = "";
        else need_free = 1;
        clusterManagerLogErr("ERR setting node-timeout for %s:%d: %s\n", n->ip,
                             n->port, err);
        if (need_free) zfree(err);
        err_count++;
    }
    clusterManagerLogInfo(">>> New node timeout set. %d OK, %d ERR.\n",
                          ok_count, err_count);
    return 1;
invalid_args:
    fprintf(stderr, CLUSTER_MANAGER_INVALID_HOST_ARG);
    return 0;
}

/* 从外部 Redis 实例导入数据（--cluster import） */
static int clusterManagerCommandImport(int argc, char **argv) {
    int success = 1;
    int port = 0, src_port = 0;
    char *ip = NULL, *src_ip = NULL;
    char *invalid_args_msg = NULL;
    sds cmdfmt = NULL;
    if (!getClusterHostFromCmdArgs(argc, argv, &ip, &port)) {
        invalid_args_msg = CLUSTER_MANAGER_INVALID_HOST_ARG;
        goto invalid_args;
    }
    if (config.cluster_manager_command.from == NULL) {
        invalid_args_msg = "[ERR] Option '--cluster-from' is required for "
                           "subcommand 'import'.\n";
        goto invalid_args;
    }
    char *src_host[] = {config.cluster_manager_command.from};
    if (!getClusterHostFromCmdArgs(1, src_host, &src_ip, &src_port)) {
        invalid_args_msg = "[ERR] Invalid --cluster-from host. You need to "
                           "pass a valid address (ie. 120.0.0.1:7000).\n";
        goto invalid_args;
    }
    clusterManagerLogInfo(">>> Importing data from %s:%d to cluster %s:%d\n",
                          src_ip, src_port, ip, port);

    clusterManagerNode *refnode = clusterManagerNewNode(ip, port, 0);
    if (!clusterManagerLoadInfoFromNode(refnode)) return 0;
    if (!clusterManagerCheckCluster(0)) return 0;
    char *reply_err = NULL;
    redisReply *src_reply = NULL;
    // Connect to the source node.
    redisContext *src_ctx = redisConnectWrapper(src_ip, src_port, config.connect_timeout);
    if (src_ctx->err) {
        success = 0;
        fprintf(stderr,"Could not connect to Redis at %s:%d: %s.\n", src_ip,
                src_port, src_ctx->errstr);
        goto cleanup;
    }
    // Auth for the source node. 
    char *from_user = config.cluster_manager_command.from_user;
    char *from_pass = config.cluster_manager_command.from_pass;
    if (cliAuth(src_ctx, from_user, from_pass) == REDIS_ERR) {
        success = 0;
        goto cleanup;
    }

    src_reply = reconnectingRedisCommand(src_ctx, "INFO");
    if (!src_reply || src_reply->type == REDIS_REPLY_ERROR) {
        if (src_reply && src_reply->str) reply_err = src_reply->str;
        success = 0;
        goto cleanup;
    }
    if (getLongInfoField(src_reply->str, "cluster_enabled")) {
        clusterManagerLogErr("[ERR] The source node should not be a "
                             "cluster node.\n");
        success = 0;
        goto cleanup;
    }
    freeReplyObject(src_reply);
    src_reply = reconnectingRedisCommand(src_ctx, "DBSIZE");
    if (!src_reply || src_reply->type == REDIS_REPLY_ERROR) {
        if (src_reply && src_reply->str) reply_err = src_reply->str;
        success = 0;
        goto cleanup;
    }
    int size = src_reply->integer, i;
    clusterManagerLogWarn("*** Importing %d keys from DB 0\n", size);

    // Build a slot -> node map
    clusterManagerNode  *slots_map[CLUSTER_MANAGER_SLOTS];
    memset(slots_map, 0, sizeof(slots_map));
    listIter li;
    listNode *ln;
    for (i = 0; i < CLUSTER_MANAGER_SLOTS; i++) {
        listRewind(cluster_manager.nodes, &li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = ln->value;
            if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
            if (n->slots_count == 0) continue;
            if (n->slots[i]) {
                slots_map[i] = n;
                break;
            }
        }
    }
    cmdfmt = sdsnew("MIGRATE %s %d %s %d %d");
    if (config.conn_info.auth) {
        if (config.conn_info.user) {
            cmdfmt = sdscatfmt(cmdfmt," AUTH2 %s %s", config.conn_info.user, config.conn_info.auth); 
        } else {
            cmdfmt = sdscatfmt(cmdfmt," AUTH %s", config.conn_info.auth);
        }
    }

    if (config.cluster_manager_command.flags & CLUSTER_MANAGER_CMD_FLAG_COPY)
        cmdfmt = sdscat(cmdfmt," COPY");
    if (config.cluster_manager_command.flags & CLUSTER_MANAGER_CMD_FLAG_REPLACE)
        cmdfmt = sdscat(cmdfmt," REPLACE");

    /* Use SCAN to iterate over the keys, migrating to the
     * right node as needed. */
    int cursor = -999, timeout = config.cluster_manager_command.timeout;
    while (cursor != 0) {
        if (cursor < 0) cursor = 0;
        freeReplyObject(src_reply);
        src_reply = reconnectingRedisCommand(src_ctx, "SCAN %d COUNT %d",
                                             cursor, 1000);
        if (!src_reply || src_reply->type == REDIS_REPLY_ERROR) {
            if (src_reply && src_reply->str) reply_err = src_reply->str;
            success = 0;
            goto cleanup;
        }
        assert(src_reply->type == REDIS_REPLY_ARRAY);
        assert(src_reply->elements >= 2);
        assert(src_reply->element[1]->type == REDIS_REPLY_ARRAY);
        if (src_reply->element[0]->type == REDIS_REPLY_STRING)
            cursor = atoi(src_reply->element[0]->str);
        else if (src_reply->element[0]->type == REDIS_REPLY_INTEGER)
            cursor = src_reply->element[0]->integer;
        int keycount = src_reply->element[1]->elements;
        for (i = 0; i < keycount; i++) {
            redisReply *kr = src_reply->element[1]->element[i];
            assert(kr->type == REDIS_REPLY_STRING);
            char *key = kr->str;
            uint16_t slot = clusterManagerKeyHashSlot(key, kr->len);
            clusterManagerNode *target = slots_map[slot];
            printf("Migrating %s to %s:%d: ", key, target->ip, target->port);
            redisReply *r = reconnectingRedisCommand(src_ctx, cmdfmt,
                                                     target->ip, target->port,
                                                     key, 0, timeout);
            if (!r || r->type == REDIS_REPLY_ERROR) {
                if (r && r->str) {
                    clusterManagerLogErr("Source %s:%d replied with "
                                         "error:\n%s\n", src_ip, src_port,
                                         r->str);
                }
                success = 0;
            }
            freeReplyObject(r);
            if (!success) goto cleanup;
            clusterManagerLogOk("OK\n");
        }
    }
cleanup:
    if (reply_err)
        clusterManagerLogErr("Source %s:%d replied with error:\n%s\n",
                             src_ip, src_port, reply_err);
    if (src_ctx) redisFree(src_ctx);
    if (src_reply) freeReplyObject(src_reply);
    if (cmdfmt) sdsfree(cmdfmt);
    return success;
invalid_args:
    fprintf(stderr, "%s", invalid_args_msg);
    return 0;
}

/* 在所有集群节点上执行同一条命令（--cluster call） */
static int clusterManagerCommandCall(int argc, char **argv) {
    int port = 0, i;
    char *ip = NULL;
    if (!getClusterHostFromCmdArgs(1, argv, &ip, &port)) goto invalid_args;
    clusterManagerNode *refnode = clusterManagerNewNode(ip, port, 0);
    if (!clusterManagerLoadInfoFromNode(refnode)) return 0;
    argc--;
    argv++;
    size_t *argvlen = zmalloc(argc*sizeof(size_t));
    clusterManagerLogInfo(">>> Calling");
    for (i = 0; i < argc; i++) {
        argvlen[i] = strlen(argv[i]);
        printf(" %s", argv[i]);
    }
    printf("\n");
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if ((config.cluster_manager_command.flags & CLUSTER_MANAGER_CMD_FLAG_MASTERS_ONLY)
              && (n->replicate != NULL)) continue;  // continue if node is slave
        if ((config.cluster_manager_command.flags & CLUSTER_MANAGER_CMD_FLAG_SLAVES_ONLY)
              && (n->replicate == NULL)) continue;   // continue if node is master
        if (!n->context && !clusterManagerNodeConnect(n)) continue;
        redisReply *reply = NULL;
        redisAppendCommandArgv(n->context, argc, (const char **) argv, argvlen);
        int status = redisGetReply(n->context, (void **)(&reply));
        if (status != REDIS_OK || reply == NULL )
            printf("%s:%d: Failed!\n", n->ip, n->port);
        else {
            sds formatted_reply = cliFormatReplyRaw(reply);
            printf("%s:%d: %s\n", n->ip, n->port, (char *) formatted_reply);
            sdsfree(formatted_reply);
        }
        if (reply != NULL) freeReplyObject(reply);
    }
    zfree(argvlen);
    return 1;
invalid_args:
    fprintf(stderr, CLUSTER_MANAGER_INVALID_HOST_ARG);
    return 0;
}

/* 备份集群所有节点的 RDB 与拓扑信息（--cluster backup） */
static int clusterManagerCommandBackup(int argc, char **argv) {
    UNUSED(argc);
    int success = 1, port = 0;
    char *ip = NULL;
    if (!getClusterHostFromCmdArgs(1, argv, &ip, &port)) goto invalid_args;
    clusterManagerNode *refnode = clusterManagerNewNode(ip, port, 0);
    if (!clusterManagerLoadInfoFromNode(refnode)) return 0;
    int no_issues = clusterManagerCheckCluster(0);
    int cluster_errors_count = (no_issues ? 0 :
                                listLength(cluster_manager.errors));
    config.cluster_manager_command.backup_dir = argv[1];
    /* TODO: check if backup_dir is a valid directory. */
    sds json = sdsnew("[\n");
    int first_node = 0;
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        if (!first_node) first_node = 1;
        else json = sdscat(json, ",\n");
        clusterManagerNode *node = ln->value;
        sds node_json = clusterManagerNodeGetJSON(node, cluster_errors_count);
        json = sdscat(json, node_json);
        sdsfree(node_json);
        if (node->replicate)
            continue;
        clusterManagerLogInfo(">>> Node %s:%d -> Saving RDB...\n",
                              node->ip, node->port);
        fflush(stdout);
        getRDB(node);
    }
    json = sdscat(json, "\n]");
    sds jsonpath = sdsnew(config.cluster_manager_command.backup_dir);
    if (jsonpath[sdslen(jsonpath) - 1] != '/')
        jsonpath = sdscat(jsonpath, "/");
    jsonpath = sdscat(jsonpath, "nodes.json");
    fflush(stdout);
    clusterManagerLogInfo("Saving cluster configuration to: %s\n", jsonpath);
    FILE *out = fopen(jsonpath, "w+");
    if (!out) {
        clusterManagerLogErr("Could not save nodes to: %s\n", jsonpath);
        success = 0;
        goto cleanup;
    }
    fputs(json, out);
    fclose(out);
cleanup:
    sdsfree(json);
    sdsfree(jsonpath);
    if (success) {
        if (!no_issues) {
            clusterManagerLogWarn("*** Cluster seems to have some problems, "
                                  "please be aware of it if you're going "
                                  "to restore this backup.\n");
        }
        clusterManagerLogOk("[OK] Backup created into: %s\n",
                            config.cluster_manager_command.backup_dir);
    } else clusterManagerLogOk("[ERR] Failed to back cluster!\n");
    return success;
invalid_args:
    fprintf(stderr, CLUSTER_MANAGER_INVALID_HOST_ARG);
    return 0;
}

/* 打印集群管理命令帮助信息（--cluster help） */
static int clusterManagerCommandHelp(int argc, char **argv) {
    UNUSED(argc);
    UNUSED(argv);
    int commands_count = sizeof(clusterManagerCommands) /
                         sizeof(clusterManagerCommandDef);
    int i = 0, j;
    fprintf(stdout, "Cluster Manager Commands:\n");
    int padding = 15;
    for (; i < commands_count; i++) {
        clusterManagerCommandDef *def = &(clusterManagerCommands[i]);
        int namelen = strlen(def->name), padlen = padding - namelen;
        fprintf(stdout, "  %s", def->name);
        for (j = 0; j < padlen; j++) fprintf(stdout, " ");
        fprintf(stdout, "%s\n", (def->args ? def->args : ""));
        if (def->options != NULL) {
            int optslen = strlen(def->options);
            char *p = def->options, *eos = p + optslen;
            char *comma = NULL;
            while ((comma = strchr(p, ',')) != NULL) {
                int deflen = (int)(comma - p);
                char buf[255];
                memcpy(buf, p, deflen);
                buf[deflen] = '\0';
                for (j = 0; j < padding; j++) fprintf(stdout, " ");
                fprintf(stdout, "  --cluster-%s\n", buf);
                p = comma + 1;
                if (p >= eos) break;
            }
            if (p < eos) {
                for (j = 0; j < padding; j++) fprintf(stdout, " ");
                fprintf(stdout, "  --cluster-%s\n", p);
            }
        }
    }
    fprintf(stdout, "\nFor check, fix, reshard, del-node, set-timeout, "
                    "info, rebalance, call, import, backup you "
                    "can specify the host and port of any working node in "
                    "the cluster.\n");

    int options_count = sizeof(clusterManagerOptions) /
                        sizeof(clusterManagerOptionDef);
    i = 0;
    fprintf(stdout, "\nCluster Manager Options:\n");
    for (; i < options_count; i++) {
        clusterManagerOptionDef *def = &(clusterManagerOptions[i]);
        int namelen = strlen(def->name), padlen = padding - namelen;
        fprintf(stdout, "  %s", def->name);
        for (j = 0; j < padlen; j++) fprintf(stdout, " ");
        fprintf(stdout, "%s\n", def->desc);
    }

    fprintf(stdout, "\n");
    return 0;
}

/*------------------------------------------------------------------------------
 * Latency and latency history modes — 延迟测量与历史模式
 *--------------------------------------------------------------------------- */

/* 按当前输出模式打印延迟统计信息 */
static void latencyModePrint(long long min, long long max, double avg, long long count) {
    if (config.output == OUTPUT_STANDARD) {
        printf("min: %lld, max: %lld, avg: %.2f (%lld samples)",
                min, max, avg, count);
        fflush(stdout);
    } else if (config.output == OUTPUT_CSV) {
        printf("%lld,%lld,%.2f,%lld\n", min, max, avg, count);
    } else if (config.output == OUTPUT_RAW) {
        printf("%lld %lld %.2f %lld\n", min, max, avg, count);
    } else if (config.output == OUTPUT_JSON) {
        printf("{\"min\": %lld, \"max\": %lld, \"avg\": %.2f, \"count\": %lld}\n", min, max, avg, count);
    }
}

/* 延迟采样间隔，单位毫秒 */
#define LATENCY_SAMPLE_RATE 10
/* 延迟历史模式默认间隔，单位毫秒 */
#define LATENCY_HISTORY_DEFAULT_INTERVAL 15000

/* --latency 与 --latency-history 模式主循环 */
static void latencyMode(void) {
    redisReply *reply;
    long long start, latency, min = 0, max = 0, tot = 0, count = 0;
    long long history_interval =
        config.interval ? config.interval/1000 :
                          LATENCY_HISTORY_DEFAULT_INTERVAL;
    double avg;
    long long history_start = mstime();

    /* Set a default for the interval in case of --latency option
     * with --raw, --csv or when it is redirected to non tty. */
    if (config.interval == 0) {
        config.interval = 1000;
    } else {
        config.interval /= 1000; /* We need to convert to milliseconds. */
    }

    if (!context) exit(1);
    while(1) {
        start = mstime();
        reply = reconnectingRedisCommand(context,"PING");
        if (reply == NULL) {
            fprintf(stderr,"\nI/O error\n");
            exit(1);
        }
        latency = mstime()-start;
        freeReplyObject(reply);
        count++;
        if (count == 1) {
            min = max = tot = latency;
            avg = (double) latency;
        } else {
            if (latency < min) min = latency;
            if (latency > max) max = latency;
            tot += latency;
            avg = (double) tot/count;
        }

        if (config.output == OUTPUT_STANDARD) {
            printf("\x1b[0G\x1b[2K"); /* Clear the line. */
            latencyModePrint(min,max,avg,count);
        } else {
            if (config.latency_history) {
                latencyModePrint(min,max,avg,count);
            } else if (mstime()-history_start > config.interval) {
                latencyModePrint(min,max,avg,count);
                exit(0);
            }
        }

        if (config.latency_history && mstime()-history_start > history_interval)
        {
            printf(" -- %.2f seconds range\n", (float)(mstime()-history_start)/1000);
            history_start = mstime();
            min = max = tot = count = 0;
        }
        usleep(LATENCY_SAMPLE_RATE * 1000);
    }
}

/*------------------------------------------------------------------------------
 * Latency distribution mode -- 需要支持 256 色的 xterm
 *--------------------------------------------------------------------------- */

/* 延迟分布模式默认采样间隔，单位毫秒 */
#define LATENCY_DIST_DEFAULT_INTERVAL 1000

/* 用于存储采样分布的结构 */
struct distsamples {
    long long max;   /* 本区间所能容纳的最大延迟（微秒） */
    long long count; /* 落入本区间的样本数 */
    int character;   /* 可视化时对应的字符 */
};

/* latencyDistMode() 的辅助函数：以 256 色 xterm 终端可视化
 * 已收集的样本。
 *
 * 接受一个按 max 值从小到大排列的 distsamples 数组。
 * 最后一项的 max 必须为 0，表示它承接所有大于前一项的样本，
 * 同时也作为循环终止哨兵。
 *
 * 'tot' 是各桶样本数总和，即从 i=0 累加到 max 哨兵之前的
 * SUM(samples[i].count)。
 *
 * 该函数会将所有桶的 count 重置为 0（副作用）。 */
void showLatencyDistSamples(struct distsamples *samples, long long tot) {
    int j;

     /* We convert samples into an index inside the palette
     * proportional to the percentage a given bucket represents.
     * This way intensity of the different parts of the spectrum
     * don't change relative to the number of requests, which avoids to
     * pollute the visualization with non-latency related info. */
    printf("\033[38;5;0m"); /* Set foreground color to black. */
    for (j = 0; ; j++) {
        int coloridx =
            ceil((double) samples[j].count / tot * (spectrum_palette_size-1));
        int color = spectrum_palette[coloridx];
        printf("\033[48;5;%dm%c", (int)color, samples[j].character);
        samples[j].count = 0;
        if (samples[j].max == 0) break; /* Last sample. */
    }
    printf("\033[0m\n");
    fflush(stdout);
}

/* 显示图例：不同桶的值与颜色含义，便于阅读频谱 */
void showLatencyDistLegend(void) {
    int j;

    printf("---------------------------------------------\n");
    printf(". - * #          .01 .125 .25 .5 milliseconds\n");
    printf("1,2,3,...,9      from 1 to 9     milliseconds\n");
    printf("A,B,C,D,E        10,20,30,40,50  milliseconds\n");
    printf("F,G,H,I,J        .1,.2,.3,.4,.5       seconds\n");
    printf("K,L,M,N,O,P,Q,?  1,2,4,8,16,30,60,>60 seconds\n");
    printf("From 0 to 100%%: ");
    for (j = 0; j < spectrum_palette_size; j++) {
        printf("\033[48;5;%dm ", spectrum_palette[j]);
    }
    printf("\033[0m\n");
    printf("---------------------------------------------\n");
}

/* --latency-dist 模式主循环 */
static void latencyDistMode(void) {
    redisReply *reply;
    long long start, latency, count = 0;
    long long history_interval =
        config.interval ? config.interval/1000 :
                          LATENCY_DIST_DEFAULT_INTERVAL;
    long long history_start = ustime();
    int j, outputs = 0;

    struct distsamples samples[] = {
        /* We use a mostly logarithmic scale, with certain linear intervals
         * which are more interesting than others, like 1-10 milliseconds
         * range. */
        {10,0,'.'},         /* 0.01 ms */
        {125,0,'-'},        /* 0.125 ms */
        {250,0,'*'},        /* 0.25 ms */
        {500,0,'#'},        /* 0.5 ms */
        {1000,0,'1'},       /* 1 ms */
        {2000,0,'2'},       /* 2 ms */
        {3000,0,'3'},       /* 3 ms */
        {4000,0,'4'},       /* 4 ms */
        {5000,0,'5'},       /* 5 ms */
        {6000,0,'6'},       /* 6 ms */
        {7000,0,'7'},       /* 7 ms */
        {8000,0,'8'},       /* 8 ms */
        {9000,0,'9'},       /* 9 ms */
        {10000,0,'A'},      /* 10 ms */
        {20000,0,'B'},      /* 20 ms */
        {30000,0,'C'},      /* 30 ms */
        {40000,0,'D'},      /* 40 ms */
        {50000,0,'E'},      /* 50 ms */
        {100000,0,'F'},     /* 0.1 s */
        {200000,0,'G'},     /* 0.2 s */
        {300000,0,'H'},     /* 0.3 s */
        {400000,0,'I'},     /* 0.4 s */
        {500000,0,'J'},     /* 0.5 s */
        {1000000,0,'K'},    /* 1 s */
        {2000000,0,'L'},    /* 2 s */
        {4000000,0,'M'},    /* 4 s */
        {8000000,0,'N'},    /* 8 s */
        {16000000,0,'O'},   /* 16 s */
        {30000000,0,'P'},   /* 30 s */
        {60000000,0,'Q'},   /* 1 minute */
        {0,0,'?'},          /* > 1 minute */
    };

    if (!context) exit(1);
    while(1) {
        start = ustime();
        reply = reconnectingRedisCommand(context,"PING");
        if (reply == NULL) {
            fprintf(stderr,"\nI/O error\n");
            exit(1);
        }
        latency = ustime()-start;
        freeReplyObject(reply);
        count++;

        /* Populate the relevant bucket. */
        for (j = 0; ; j++) {
            if (samples[j].max == 0 || latency <= samples[j].max) {
                samples[j].count++;
                break;
            }
        }

        /* From time to time show the spectrum. */
        if (count && (ustime()-history_start)/1000 > history_interval) {
            if ((outputs++ % 20) == 0)
                showLatencyDistLegend();
            showLatencyDistSamples(samples,count);
            history_start = ustime();
            count = 0;
        }
        usleep(LATENCY_SAMPLE_RATE * 1000);
    }
}

/*------------------------------------------------------------------------------
 * Slave mode — SLAVE/REPLICAOF 模式
 *--------------------------------------------------------------------------- */

/* RDB 流的 EOF 标记字节数 */
#define RDB_EOF_MARK_SIZE 40

/* 向服务器发送 REPLCONF 命令并处理响应 */
int sendReplconf(const char* arg1, const char* arg2) {
    int res = 1;
    fprintf(stderr, "sending REPLCONF %s %s\n", arg1, arg2);
    redisReply *reply = redisCommand(context, "REPLCONF %s %s", arg1, arg2);

    /* Handle any error conditions */
    if(reply == NULL) {
        fprintf(stderr, "\nI/O error\n");
        exit(1);
    } else if(reply->type == REDIS_REPLY_ERROR) {
        /* non fatal, old versions may not support it */
        fprintf(stderr, "REPLCONF %s error: %s\n", arg1, reply->str);
        res = 0;
    }
    freeReplyObject(reply);
    return res;
}

void sendCapa(void) {
    sendReplconf("capa", "eof");
}

void sendRdbOnly(void) {
    sendReplconf("rdb-only", "1");
}

/* 通过 redisContext 读取原始字节。
 * 读操作是非贪婪的，不一定会填满整个缓冲区。 */
static ssize_t readConn(redisContext *c, char *buf, size_t len)
{
    return c->funcs->read(c, buf, len);
}

/* 发送 SYNC 命令并读取 payload 大小。slaveMode() 与 getRDB() 都会使用。
 *
 * send_sync 为 1 表示显式发送 SYNC 命令；为 0 表示不再发送 SYNC，
 * 而是把 c->obuf 中已有的命令发送出去。
 *
 * 返回要读取的 RDB payload 大小；若使用 EOF 标记且大小未知则返回 0；
 * 若收到 PSYNC +CONTINUE（无 RDB payload）也返回 0。
 *
 * out_full_mode 为 1 表示本次为 full sync；为 0 表示 partial mode。 */
unsigned long long sendSync(redisContext *c, int send_sync, char *out_eof, int *out_full_mode) {
    /* To start we need to send the SYNC command and return the payload.
     * The hiredis client lib does not understand this part of the protocol
     * and we don't want to mess with its buffers, so everything is performed
     * using direct low-level I/O. */
    char buf[4096], *p;
    ssize_t nread;

    if (out_full_mode) *out_full_mode = 1;

    if (send_sync) {
        /* Send the SYNC command. */
        if (cliWriteConn(c, "SYNC\r\n", 6) != 6) {
            fprintf(stderr,"Error writing to master\n");
            exit(1);
        }
    } else {
        /* We have written the command into c->obuf before. */
        if (cliWriteConn(c, "", 0) != 0) {
            fprintf(stderr,"Error writing to master\n");
            exit(1);
        }
    }

    /* Read $<payload>\r\n, making sure to read just up to "\n" */
    p = buf;
    while(1) {
        nread = readConn(c,p,1);
        if (nread <= 0) {
            fprintf(stderr,"Error reading bulk length while SYNCing\n");
            exit(1);
        }
        if (*p == '\n' && p != buf) break;
        if (*p != '\n') p++;
        if (p >= buf + sizeof(buf) - 1) break; /* Go back one more char for null-term. */
    }
    *p = '\0';
    if (buf[0] == '-') {
        fprintf(stderr, "SYNC with master failed: %s\n", buf);
        exit(1);
    }

    /* Handling PSYNC responses.
     * Read +FULLRESYNC <replid> <offset>\r\n, after that is the $<payload> or the $EOF:<40 bytes delimiter>
     * Read +CONTINUE <replid>\r\n or +CONTINUE\r\n, after that is the command stream */
    if (!strncmp(buf, "+FULLRESYNC", 11) ||
        !strncmp(buf, "+CONTINUE", 9))
    {
        int sync_partial = !strncmp(buf, "+CONTINUE", 9);
        fprintf(stderr, "PSYNC replied %s\n", buf);
        p = buf;
        while(1) {
            nread = readConn(c,p,1);
            if (nread <= 0) {
                fprintf(stderr,"Error reading bulk length while PSYNCing\n");
                exit(1);
            }
            if (*p == '\n' && p != buf) break;
            if (*p != '\n') p++;
            if (p >= buf + sizeof(buf) - 1) break; /* Go back one more char for null-term. */
        }
        *p = '\0';

        if (sync_partial) {
            if (out_full_mode) *out_full_mode = 0;
            return 0;
        }
    }

    if (strncmp(buf+1,"EOF:",4) == 0 && strlen(buf+5) >= RDB_EOF_MARK_SIZE) {
        memcpy(out_eof, buf+5, RDB_EOF_MARK_SIZE);
        return 0;
    }
    return strtoull(buf+1,NULL,10);
}

/* --slave / --replica 模式：模拟从节点接收并打印主节点的命令流 */
static void slaveMode(int send_sync) {
    static char eofmark[RDB_EOF_MARK_SIZE];
    static char lastbytes[RDB_EOF_MARK_SIZE];
    static int usemark = 0;
    static int out_full_mode;
    unsigned long long payload = sendSync(context, send_sync, eofmark, &out_full_mode);
    char buf[1024];
    int original_output = config.output;
    char *info = out_full_mode ? "Full resync" : "Partial resync";

    if (out_full_mode == 1 && payload == 0) {
        /* SYNC with EOF marker or PSYNC +FULLRESYNC with EOF marker. */
        payload = ULLONG_MAX;
        memset(lastbytes,0,RDB_EOF_MARK_SIZE);
        usemark = 1;
        fprintf(stderr, "%s with master, discarding "
                        "bytes of bulk transfer until EOF marker...\n", info);
    } else if (out_full_mode == 1 && payload != 0) {
        /* SYNC without EOF marker or PSYNC +FULLRESYNC. */
        fprintf(stderr, "%s with master, discarding %llu "
                        "bytes of bulk transfer...\n", info, payload);
    } else if (out_full_mode == 0 && payload == 0) {
        /* PSYNC +CONTINUE (no RDB payload). */
        fprintf(stderr, "%s with master...\n", info);
    }

    /* Discard the payload. */
    while(payload) {
        ssize_t nread;

        nread = readConn(context,buf,(payload > sizeof(buf)) ? sizeof(buf) : payload);
        if (nread <= 0) {
            fprintf(stderr,"Error reading RDB payload while %sing\n", info);
            exit(1);
        }
        payload -= nread;

        if (usemark) {
            /* Update the last bytes array, and check if it matches our delimiter.*/
            if (nread >= RDB_EOF_MARK_SIZE) {
                memcpy(lastbytes,buf+nread-RDB_EOF_MARK_SIZE,RDB_EOF_MARK_SIZE);
            } else {
                int rem = RDB_EOF_MARK_SIZE-nread;
                memmove(lastbytes,lastbytes+nread,rem);
                memcpy(lastbytes+rem,buf,nread);
            }
            if (memcmp(lastbytes,eofmark,RDB_EOF_MARK_SIZE) == 0)
                break;
        }
    }

    if (usemark) {
        unsigned long long offset = ULLONG_MAX - payload;
        fprintf(stderr,"%s done after %llu bytes. Logging commands from master.\n", info, offset);
        /* put the slave online */
        sleep(1);
        sendReplconf("ACK", "0");
    } else
        fprintf(stderr,"%s done. Logging commands from master.\n", info);

    /* Now we can use hiredis to read the incoming protocol. */
    config.output = OUTPUT_CSV;
    while (cliReadReply(0) == REDIS_OK);
    config.output = original_output;
}

/*------------------------------------------------------------------------------
 * RDB transfer mode — RDB 转储模式
 *--------------------------------------------------------------------------- */

/* 实现 --rdb 选项：通过复制协议从远程服务器拉取 RDB 文件 */
static void getRDB(clusterManagerNode *node) {
    int fd;
    redisContext *s;
    char *filename;
    if (node != NULL) {
        assert(node->context);
        s = node->context;
        filename = clusterManagerGetNodeRDBFilename(node);
    } else {
        s = context;
        filename = config.rdb_filename;
    }
    static char eofmark[RDB_EOF_MARK_SIZE];
    static char lastbytes[RDB_EOF_MARK_SIZE];
    static int usemark = 0;
    unsigned long long payload = sendSync(s, 1, eofmark, NULL);
    char buf[4096];

    if (payload == 0) {
        payload = ULLONG_MAX;
        memset(lastbytes,0,RDB_EOF_MARK_SIZE);
        usemark = 1;
        fprintf(stderr,"SYNC sent to master, writing bytes of bulk transfer "
                "until EOF marker to '%s'\n", filename);
    } else {
        fprintf(stderr,"SYNC sent to master, writing %llu bytes to '%s'\n",
            payload, filename);
    }

    int write_to_stdout = !strcmp(filename,"-");
    /* Write to file. */
    if (write_to_stdout) {
        fd = STDOUT_FILENO;
    } else {
        fd = open(filename, O_CREAT|O_WRONLY, 0644);
        if (fd == -1) {
            fprintf(stderr, "Error opening '%s': %s\n", filename,
                strerror(errno));
            exit(1);
        }
    }

    while(payload) {
        ssize_t nread, nwritten;

        nread = readConn(s,buf,(payload > sizeof(buf)) ? sizeof(buf) : payload);
        if (nread <= 0) {
            fprintf(stderr,"I/O Error reading RDB payload from socket\n");
            exit(1);
        }
        nwritten = write(fd, buf, nread);
        if (nwritten != nread) {
            fprintf(stderr,"Error writing data to file: %s\n",
                (nwritten == -1) ? strerror(errno) : "short write");
            exit(1);
        }
        payload -= nread;

        if (usemark) {
            /* Update the last bytes array, and check if it matches our delimiter.*/
            if (nread >= RDB_EOF_MARK_SIZE) {
                memcpy(lastbytes,buf+nread-RDB_EOF_MARK_SIZE,RDB_EOF_MARK_SIZE);
            } else {
                int rem = RDB_EOF_MARK_SIZE-nread;
                memmove(lastbytes,lastbytes+nread,rem);
                memcpy(lastbytes+rem,buf,nread);
            }
            if (memcmp(lastbytes,eofmark,RDB_EOF_MARK_SIZE) == 0)
                break;
        }
    }
    if (usemark) {
        payload = ULLONG_MAX - payload - RDB_EOF_MARK_SIZE;
        if (!write_to_stdout && ftruncate(fd, payload) == -1)
            fprintf(stderr,"ftruncate failed: %s.\n", strerror(errno));
        fprintf(stderr,"Transfer finished with success after %llu bytes\n", payload);
    } else {
        fprintf(stderr,"Transfer finished with success.\n");
    }
    redisFree(s); /* Close the connection ASAP as fsync() may take time. */
    if (node)
        node->context = NULL;
    if (!write_to_stdout && fsync(fd) == -1) {
        fprintf(stderr,"Fail to fsync '%s': %s\n", filename, strerror(errno));
        exit(1);
    }
    close(fd);
    if (node) {
        sdsfree(filename);
        return;
    }
    exit(0);
}

/*------------------------------------------------------------------------------
 * Bulk import (pipe) mode — 管道批量导入模式
 *--------------------------------------------------------------------------- */

/* 管道模式单次写循环允许的最大字节数 */
#define PIPEMODE_WRITE_LOOP_MAX_BYTES (128*1024)

/* --pipe 模式：从 stdin 读取 RESP 协议并批量发送给服务器 */
static void pipeMode(void) {
    long long errors = 0, replies = 0, obuf_len = 0, obuf_pos = 0;
    char obuf[1024*16]; /* Output buffer */
    char aneterr[ANET_ERR_LEN];
    redisReply *reply;
    int eof = 0; /* True once we consumed all the standard input. */
    int done = 0;
    char magic[20]; /* Special reply we recognize. */
    time_t last_read_time = time(NULL);

    srand(time(NULL));

    /* Use non blocking I/O. */
    if (anetNonBlock(aneterr,context->fd) == ANET_ERR) {
        fprintf(stderr, "Can't set the socket in non blocking mode: %s\n",
            aneterr);
        exit(1);
    }

    context->flags &= ~REDIS_BLOCK;

    /* Transfer raw protocol and read replies from the server at the same
     * time. */
    while(!done) {
        int mask = AE_READABLE;

        if (!eof || obuf_len != 0) mask |= AE_WRITABLE;
        mask = aeWait(context->fd,mask,1000);

        /* Handle the readable state: we can read replies from the server. */
        if (mask & AE_READABLE) {
            int read_error = 0;

            do {
                if (!read_error && redisBufferRead(context) == REDIS_ERR) {
                    read_error = 1;
                }

                reply = NULL;
                if (redisGetReply(context, (void **) &reply) == REDIS_ERR) {
                    fprintf(stderr, "Error reading replies from server\n");
                    exit(1);
                }
                if (reply) {
                    last_read_time = time(NULL);
                    if (reply->type == REDIS_REPLY_ERROR) {
                        fprintf(stderr,"%s\n", reply->str);
                        errors++;
                    } else if (eof && reply->type == REDIS_REPLY_STRING &&
                                      reply->len == 20) {
                        /* Check if this is the reply to our final ECHO
                         * command. If so everything was received
                         * from the server. */
                        if (memcmp(reply->str,magic,20) == 0) {
                            printf("Last reply received from server.\n");
                            done = 1;
                            replies--;
                        }
                    }
                    replies++;
                    freeReplyObject(reply);
                }
            } while(reply);

            /* Abort on read errors. We abort here because it is important
             * to consume replies even after a read error: this way we can
             * show a potential problem to the user. */
            if (read_error) exit(1);
        }

        /* Handle the writable state: we can send protocol to the server. */
        if (mask & AE_WRITABLE) {
            ssize_t loop_nwritten = 0;

            while(1) {
                /* Transfer current buffer to server. */
                if (obuf_len != 0) {
                    ssize_t nwritten = cliWriteConn(context,obuf+obuf_pos,obuf_len);

                    if (nwritten == -1) {
                        if (errno != EAGAIN && errno != EINTR) {
                            fprintf(stderr, "Error writing to the server: %s\n",
                                strerror(errno));
                            exit(1);
                        } else {
                            nwritten = 0;
                        }
                    }
                    obuf_len -= nwritten;
                    obuf_pos += nwritten;
                    loop_nwritten += nwritten;
                    if (obuf_len != 0) break; /* Can't accept more data. */
                }
                if (context->err) {
                    fprintf(stderr, "Server I/O Error: %s\n", context->errstr);
                    exit(1);
                }
                /* If buffer is empty, load from stdin. */
                if (obuf_len == 0 && !eof) {
                    ssize_t nread = read(STDIN_FILENO,obuf,sizeof(obuf));

                    if (nread == 0) {
                        /* The ECHO sequence starts with a "\r\n" so that if there
                         * is garbage in the protocol we read from stdin, the ECHO
                         * will likely still be properly formatted.
                         * CRLF is ignored by Redis, so it has no effects. */
                        char echo[] =
                        "\r\n*2\r\n$4\r\nECHO\r\n$20\r\n01234567890123456789\r\n";
                        int j;

                        eof = 1;
                        /* Everything transferred, so we queue a special
                         * ECHO command that we can match in the replies
                         * to make sure everything was read from the server. */
                        for (j = 0; j < 20; j++)
                            magic[j] = rand() & 0xff;
                        memcpy(echo+21,magic,20);
                        memcpy(obuf,echo,sizeof(echo)-1);
                        obuf_len = sizeof(echo)-1;
                        obuf_pos = 0;
                        printf("All data transferred. Waiting for the last reply...\n");
                    } else if (nread == -1) {
                        fprintf(stderr, "Error reading from stdin: %s\n",
                            strerror(errno));
                        exit(1);
                    } else {
                        obuf_len = nread;
                        obuf_pos = 0;
                    }
                }
                if ((obuf_len == 0 && eof) ||
                    loop_nwritten > PIPEMODE_WRITE_LOOP_MAX_BYTES) break;
            }
        }

        /* Handle timeout, that is, we reached EOF, and we are not getting
         * replies from the server for a few seconds, nor the final ECHO is
         * received. */
        if (eof && config.pipe_timeout > 0 &&
            time(NULL)-last_read_time > config.pipe_timeout)
        {
            fprintf(stderr,"No replies for %d seconds: exiting.\n",
                config.pipe_timeout);
            errors++;
            break;
        }
    }
    printf("errors: %lld, replies: %lld\n", errors, replies);
    if (errors)
        exit(1);
    else
        exit(0);
}

/*------------------------------------------------------------------------------
 * Find big keys — 查找大 key / 内存占用大的 key
 *--------------------------------------------------------------------------- */

/* 发送 SCAN 命令并返回 reply；it 是用于迭代的游标 */
static redisReply *sendScan(unsigned long long *it) {
    redisReply *reply;

    if (config.pattern)
        reply = redisCommand(context, "SCAN %llu MATCH %b COUNT %d",
            *it, config.pattern, sdslen(config.pattern), config.count);
    else
        reply = redisCommand(context, "SCAN %llu COUNT %d",
            *it, config.count);

    /* Handle any error conditions */
    if(reply == NULL) {
        fprintf(stderr, "\nI/O error\n");
        exit(1);
    } else if(reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "SCAN error: %s\n", reply->str);
        exit(1);
    } else if(reply->type != REDIS_REPLY_ARRAY) {
        fprintf(stderr, "Non ARRAY response from SCAN!\n");
        exit(1);
    } else if(reply->elements != 2) {
        fprintf(stderr, "Invalid element count from SCAN!\n");
        exit(1);
    }

    /* Validate our types are correct */
    assert(reply->element[0]->type == REDIS_REPLY_STRING);
    assert(reply->element[1]->type == REDIS_REPLY_ARRAY);

    /* Update iterator */
    *it = strtoull(reply->element[0]->str, NULL, 10);

    return reply;
}

/* 通过 DBSIZE 获取当前数据库 key 数量 */
static int getDbSize(void) {
    redisReply *reply;
    int size;

    reply = redisCommand(context, "DBSIZE");

    if (reply == NULL) {
        fprintf(stderr, "\nI/O error\n");
        exit(1);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "Couldn't determine DBSIZE: %s\n", reply->str);
        exit(1);
    } else if (reply->type != REDIS_REPLY_INTEGER) {
        fprintf(stderr, "Non INTEGER response from DBSIZE!\n");
        exit(1);
    }

    /* Grab the number of keys and free our reply */
    size = reply->integer;
    freeReplyObject(reply);

    return size;
}

/* 通过 CONFIG GET databases 获取数据库数量 */
static int getDatabases(void) {
    redisReply *reply;
    int dbnum;

    reply = redisCommand(context, "CONFIG GET databases");

    if (reply == NULL) {
        fprintf(stderr, "\nI/O error\n");
        exit(1);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        dbnum = 16;
        fprintf(stderr, "CONFIG GET databases fails: %s, use default value 16 instead\n", reply->str);
    } else {
        assert(reply->type == (config.current_resp3 ? REDIS_REPLY_MAP : REDIS_REPLY_ARRAY));
        assert(reply->elements == 2);
        dbnum = atoi(reply->element[1]->str);
    }

    freeReplyObject(reply);
    return dbnum;
}

typedef struct {
    char *name;
    char *sizecmd;
    char *sizeunit;
    unsigned long long biggest;
    unsigned long long count;
    unsigned long long totalsize;
    sds biggest_key;
} typeinfo;

typeinfo type_string = { "string", "STRLEN", "bytes" };
typeinfo type_list = { "list", "LLEN", "items" };
typeinfo type_set = { "set", "SCARD", "members" };
typeinfo type_hash = { "hash", "HLEN", "fields" };
typeinfo type_zset = { "zset", "ZCARD", "members" };
typeinfo type_stream = { "stream", "XLEN", "entries" };
typeinfo type_other = { "other", NULL, "?" };

static typeinfo* typeinfo_add(dict *types, char* name, typeinfo* type_template) {
    typeinfo *info = zmalloc(sizeof(typeinfo));
    *info = *type_template;
    info->name = sdsnew(name);
    dictAdd(types, info->name, info);
    return info;
}

void type_free(dict *d, void* val) {
    typeinfo *info = val;
    UNUSED(d);
    if (info->biggest_key)
        sdsfree(info->biggest_key);
    sdsfree(info->name);
    zfree(info);
}

static dictType typeinfoDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor (owned by the value)*/
    type_free,                 /* val destructor */
    NULL                       /* allow to expand */
};

static void getKeyTypes(dict *types_dict, redisReply *keys, typeinfo **types) {
    redisReply *reply;
    unsigned int i;

    /* Pipeline TYPE commands */
    for(i=0;i<keys->elements;i++) {
        const char* argv[] = {"TYPE", keys->element[i]->str};
        size_t lens[] = {4, keys->element[i]->len};
        redisAppendCommandArgv(context, 2, argv, lens);
    }

    /* Retrieve types */
    for(i=0;i<keys->elements;i++) {
        if(redisGetReply(context, (void**)&reply)!=REDIS_OK) {
            fprintf(stderr, "Error getting type for key '%s' (%d: %s)\n",
                keys->element[i]->str, context->err, context->errstr);
            exit(1);
        } else if(reply->type != REDIS_REPLY_STATUS) {
            if(reply->type == REDIS_REPLY_ERROR) {
                fprintf(stderr, "TYPE returned an error: %s\n", reply->str);
            } else {
                fprintf(stderr,
                    "Invalid reply type (%d) for TYPE on key '%s'!\n",
                    reply->type, keys->element[i]->str);
            }
            exit(1);
        }

        sds typereply = sdsnew(reply->str);
        dictEntry *de = dictFind(types_dict, typereply);
        sdsfree(typereply);
        typeinfo *type = NULL;
        if (de)
            type = dictGetVal(de);
        else if (strcmp(reply->str, "none")) /* create new types for modules, (but not for deleted keys) */
            type = typeinfo_add(types_dict, reply->str, &type_other);
        types[i] = type;
        freeReplyObject(reply);
    }
}

static void getKeySizes(redisReply *keys, typeinfo **types,
                        unsigned long long *sizes, int memkeys,
                        long long memkeys_samples)
{
    redisReply *reply;
    unsigned int i;

    /* Pipeline size commands */
    for(i=0;i<keys->elements;i++) {
        /* Skip keys that disappeared between SCAN and TYPE (or unknown types when not in memkeys mode) */
        if(!types[i] || (!types[i]->sizecmd && !memkeys))
            continue;

        if (!memkeys) {
            const char* argv[] = {types[i]->sizecmd, keys->element[i]->str};
            size_t lens[] = {strlen(types[i]->sizecmd), keys->element[i]->len};
            redisAppendCommandArgv(context, 2, argv, lens);
        } else if (memkeys_samples == -1) {
            const char* argv[] = {"MEMORY", "USAGE", keys->element[i]->str};
            size_t lens[] = {6, 5, keys->element[i]->len};
            redisAppendCommandArgv(context, 3, argv, lens);
        } else {
            sds samplesstr = sdsfromlonglong(memkeys_samples);
            const char* argv[] = {"MEMORY", "USAGE", keys->element[i]->str, "SAMPLES", samplesstr};
            size_t lens[] = {6, 5, keys->element[i]->len, 7, sdslen(samplesstr)};
            redisAppendCommandArgv(context, 5, argv, lens);
            sdsfree(samplesstr);
        }
    }

    /* Retrieve sizes */
    for(i=0;i<keys->elements;i++) {
        /* Skip keys that disappeared between SCAN and TYPE (or unknown types when not in memkeys mode) */
        if(!types[i] || (!types[i]->sizecmd && !memkeys)) {
            sizes[i] = 0;
            continue;
        }

        /* Retrieve size */
        if(redisGetReply(context, (void**)&reply)!=REDIS_OK) {
            fprintf(stderr, "Error getting size for key '%s' (%d: %s)\n",
                keys->element[i]->str, context->err, context->errstr);
            exit(1);
        } else if(reply->type != REDIS_REPLY_INTEGER) {
            /* Theoretically the key could have been removed and
             * added as a different type between TYPE and SIZE */
            fprintf(stderr,
                "Warning:  %s on '%s' failed (may have changed type)\n",
                !memkeys? types[i]->sizecmd: "MEMORY USAGE",
                keys->element[i]->str);
            sizes[i] = 0;
        } else {
            sizes[i] = reply->integer;
        }

        freeReplyObject(reply);
    }
}

/* 长时统计循环的 SIGINT 处理函数：通知循环优雅退出 */
static void longStatLoopModeStop(int s) {
    UNUSED(s);
    force_cancel_loop = 1;
}

/* 集群模式下需要先发送 READONLY 才能访问从节点。
 * 若服务器未启用集群则忽略相关错误。 */
static void sendReadOnly(void) {
    redisReply *read_reply;
    read_reply = redisCommand(context, "READONLY");
    if (read_reply == NULL){
        fprintf(stderr, "\nI/O error\n");
        exit(1);
    } else if (read_reply->type == REDIS_REPLY_ERROR && 
               strcmp(read_reply->str, "ERR This instance has cluster support disabled") != 0 &&
               strncmp(read_reply->str, "ERR unknown command", 19) != 0) {
        fprintf(stderr, "Error: %s\n", read_reply->str);
        exit(1);
    }
    freeReplyObject(read_reply);
}

/* 进度条显示函数声明 */
static int displayKeyStatsProgressbar(unsigned long long sampled,
                                      unsigned long long total_keys);

/* --bigkeys / --memkeys / --hotkeys 共用的统计扫描主循环 */
static void findBigKeys(int memkeys, long long memkeys_samples) {
    unsigned long long sampled = 0, total_keys, totlen=0, *sizes=NULL, it=0, scan_loops = 0;
    redisReply *reply, *keys;
    unsigned int arrsize=0, i;
    dictIterator *di;
    dictEntry *de;
    typeinfo **types = NULL;
    double pct;
    long long refresh_time = mstime();

    dict *types_dict = dictCreate(&typeinfoDictType);
    typeinfo_add(types_dict, "string", &type_string);
    typeinfo_add(types_dict, "list", &type_list);
    typeinfo_add(types_dict, "set", &type_set);
    typeinfo_add(types_dict, "hash", &type_hash);
    typeinfo_add(types_dict, "zset", &type_zset);
    typeinfo_add(types_dict, "stream", &type_stream);

    signal(SIGINT, longStatLoopModeStop);
    /* Total keys pre scanning */
    total_keys = getDbSize();

    /* Status message */
    printf("\n# Scanning the entire keyspace to find biggest keys as well as\n");
    printf("# average sizes per key type.  You can use -i 0.1 to sleep 0.1 sec\n");
    printf("# per 100 SCAN commands (not usually needed).\n\n");
    
    /* Use readonly in cluster */
    sendReadOnly();

    /* SCAN loop */
    do {
        /* Calculate approximate percentage completion */
        pct = 100 * (double)sampled/total_keys;

        /* Grab some keys and point to the keys array */
        reply = sendScan(&it);
        scan_loops++;
        keys  = reply->element[1];

        /* Reallocate our type and size array if we need to */
        if(keys->elements > arrsize) {
            types = zrealloc(types, sizeof(typeinfo*)*keys->elements);
            sizes = zrealloc(sizes, sizeof(unsigned long long)*keys->elements);

            if(!types || !sizes) {
                fprintf(stderr, "Failed to allocate storage for keys!\n");
                exit(1);
            }

            arrsize = keys->elements;
        }

        /* Retrieve types and then sizes */
        getKeyTypes(types_dict, keys, types);
        getKeySizes(keys, types, sizes, memkeys, memkeys_samples);

        /* Now update our stats */
        for(i=0;i<keys->elements;i++) {
            typeinfo *type = types[i];
            /* Skip keys that disappeared between SCAN and TYPE */
            if(!type)
                continue;

            type->totalsize += sizes[i];
            type->count++;
            totlen += keys->element[i]->len;
            sampled++;

            if(type->biggest<sizes[i]) {
                /* Keep track of biggest key name for this type */
                if (type->biggest_key)
                    sdsfree(type->biggest_key);
                type->biggest_key = sdscatrepr(sdsempty(), keys->element[i]->str, keys->element[i]->len);
                if(!type->biggest_key) {
                    fprintf(stderr, "Failed to allocate memory for key!\n");
                    exit(1);
                }

                /* We only show the original progress output when writing to a file */
                if (!IS_TTY_OR_FAKETTY()) {
                    printf("[%05.2f%%] Biggest %-6s found so far %s with %llu %s\n",
                        pct, type->name, type->biggest_key, sizes[i],
                        !memkeys? type->sizeunit: "bytes");
                }

                /* Keep track of the biggest size for this type */
                type->biggest = sizes[i];
            }

            /* Update overall progress
             * We only show the original progress output when writing to a file */
            if (sampled % 1000000 == 0 && !IS_TTY_OR_FAKETTY()) {
                printf("[%05.2f%%] Sampled %llu keys so far\n", pct, sampled);
            }

            /* Show the progress bar in TTY */
            if (mstime() > refresh_time + REFRESH_INTERVAL && IS_TTY_OR_FAKETTY()) {
                int line_count = 0;
                refresh_time = mstime();

                line_count = displayKeyStatsProgressbar(sampled, total_keys);
                line_count += cleanPrintfln("");

                di = dictGetIterator(types_dict);
                while ((de = dictNext(di))) {
                    typeinfo *current_type = dictGetVal(de);
                    if (current_type->biggest > 0) {
                        line_count += cleanPrintfln("Biggest %-9s found so far %s with %llu %s",
                            current_type->name, current_type->biggest_key, current_type->biggest,
                            !memkeys? current_type->sizeunit: "bytes");
                    }
                }
                dictReleaseIterator(di);

                printf("\033[%dA\r", line_count);
            }
        }

        /* Sleep if we've been directed to do so */
        if (config.interval && (scan_loops % 100) == 0) {
            usleep(config.interval);
        }

        freeReplyObject(reply);
    } while(force_cancel_loop == 0 && it != 0);

    /* Final progress bar if TTY */
    if (IS_TTY_OR_FAKETTY()) {
        displayKeyStatsProgressbar(sampled, total_keys);

        /* Clean the types info shown during the progress bar */
        int line_count = 0;
        di = dictGetIterator(types_dict);
        while ((de = dictNext(di)))
            line_count += cleanPrintfln("");
        dictReleaseIterator(di);
        printf("\033[%dA\r", line_count);
    }

    if(types) zfree(types);
    if(sizes) zfree(sizes);

    /* We're done */
    printf("\n-------- summary -------\n\n");

    /* Show percentage and sampled output when writing to a file */
    if (!IS_TTY_OR_FAKETTY()) {
        if (force_cancel_loop) printf("[%05.2f%%] ", pct);
        printf("Sampled %llu keys in the keyspace!\n", sampled);
    }

    printf("Total key length in bytes is %llu (avg len %.2f)\n\n",
       totlen, totlen ? (double)totlen/sampled : 0);

    /* Output the biggest keys we found, for types we did find */
    di = dictGetIterator(types_dict);
    while ((de = dictNext(di))) {
        typeinfo *type = dictGetVal(de);
        if(type->biggest_key) {
            printf("Biggest %6s found %s has %llu %s\n", type->name, type->biggest_key,
               type->biggest, !memkeys? type->sizeunit: "bytes");
        }
    }
    dictReleaseIterator(di);

    printf("\n");

    di = dictGetIterator(types_dict);
    while ((de = dictNext(di))) {
        typeinfo *type = dictGetVal(de);
        printf("%llu %ss with %llu %s (%05.2f%% of keys, avg size %.2f)\n",
           type->count, type->name, type->totalsize, !memkeys? type->sizeunit: "bytes",
           sampled ? 100 * (double)type->count/sampled : 0,
           type->count ? (double)type->totalsize/type->count : 0);
    }
    dictReleaseIterator(di);

    dictRelease(types_dict);

    /* Success! */
    exit(0);
}

static void getKeyFreqs(redisReply *keys, unsigned long long *freqs) {
    redisReply *reply;
    unsigned int i;

    /* Pipeline OBJECT freq commands */
    for(i=0;i<keys->elements;i++) {
        const char* argv[] = {"OBJECT", "FREQ", keys->element[i]->str};
        size_t lens[] = {6, 4, keys->element[i]->len};
        redisAppendCommandArgv(context, 3, argv, lens);
    }

    /* Retrieve freqs */
    for(i=0;i<keys->elements;i++) {
        if(redisGetReply(context, (void**)&reply)!=REDIS_OK) {
            sds keyname = sdscatrepr(sdsempty(), keys->element[i]->str, keys->element[i]->len);
            fprintf(stderr, "Error getting freq for key '%s' (%d: %s)\n",
                keyname, context->err, context->errstr);
            sdsfree(keyname);
            exit(1);
        } else if(reply->type != REDIS_REPLY_INTEGER) {
            if(reply->type == REDIS_REPLY_ERROR) {
                fprintf(stderr, "Error: %s\n", reply->str);
                exit(1);
            } else {
                sds keyname = sdscatrepr(sdsempty(), keys->element[i]->str, keys->element[i]->len);
                fprintf(stderr, "Warning: OBJECT freq on '%s' failed (may have been deleted)\n", keyname);
                sdsfree(keyname);
                freqs[i] = 0;
            }
        } else {
            freqs[i] = reply->integer;
        }
        freeReplyObject(reply);
    }
}

/* 热门 key 采样保留个数 */
#define HOTKEYS_SAMPLE 16

/* --hotkeys 模式：找出访问频率最高的 key（需要 maxmemory-policy *lfu） */
static void findHotKeys(void) {
    redisReply *keys, *reply;
    unsigned long long counters[HOTKEYS_SAMPLE] = {0};
    sds hotkeys[HOTKEYS_SAMPLE] = {NULL};
    unsigned long long sampled = 0, total_keys, *freqs = NULL, it = 0, scan_loops = 0;
    unsigned int arrsize = 0, i;
    int k;
    double pct;
    long long refresh_time = mstime();

    signal(SIGINT, longStatLoopModeStop);
    /* Total keys pre scanning */
    total_keys = getDbSize();

    /* Status message */
    printf("\n# Scanning the entire keyspace to find hot keys as well as\n");
    printf("# average sizes per key type.  You can use -i 0.1 to sleep 0.1 sec\n");
    printf("# per 100 SCAN commands (not usually needed).\n\n");

    /* Use readonly in cluster */
    sendReadOnly();
    
    /* SCAN loop */
    do {
        /* Calculate approximate percentage completion */
        pct = 100 * (double)sampled/total_keys;

        /* Grab some keys and point to the keys array */
        reply = sendScan(&it);
        scan_loops++;
        keys  = reply->element[1];

        /* Reallocate our freqs array if we need to */
        if(keys->elements > arrsize) {
            freqs = zrealloc(freqs, sizeof(unsigned long long)*keys->elements);

            if(!freqs) {
                fprintf(stderr, "Failed to allocate storage for keys!\n");
                exit(1);
            }

            arrsize = keys->elements;
        }

        getKeyFreqs(keys, freqs);

        /* Now update our stats */
        for(i=0;i<keys->elements;i++) {
            sampled++;

            /* Update overall progress.
             * Only show the original progress output when writing to a file */
            if (sampled % 1000000 == 0 && !IS_TTY_OR_FAKETTY()) {
                printf("[%05.2f%%] Sampled %llu keys so far\n", pct, sampled);
            }

            /* Use eviction pool here */
            k = 0;
            while (k < HOTKEYS_SAMPLE && freqs[i] > counters[k]) k++;
            if (k == 0) continue;
            k--;
            if (k == 0 || counters[k] == 0) {
                sdsfree(hotkeys[k]);
            } else {
                sdsfree(hotkeys[0]);
                memmove(counters,counters+1,sizeof(counters[0])*k);
                memmove(hotkeys,hotkeys+1,sizeof(hotkeys[0])*k);
            }
            counters[k] = freqs[i];
            hotkeys[k] = sdscatrepr(sdsempty(), keys->element[i]->str, keys->element[i]->len);

            /* Only show the original progress output when writing to a file */
            if (!IS_TTY_OR_FAKETTY()) {
                printf("[%05.2f%%] Hot key %s found so far with counter %llu\n",
                    pct, hotkeys[k], freqs[i]);
            }
        }

        /* Show the progress bar in TTY */
        if (mstime() > refresh_time + REFRESH_INTERVAL && IS_TTY_OR_FAKETTY()) {
            int line_count = 0;
            refresh_time = mstime();

            line_count = displayKeyStatsProgressbar(sampled, total_keys);
            line_count += cleanPrintfln("");

            for (k = HOTKEYS_SAMPLE - 1; k >= 0; k--) {
                if (counters[k] > 0) {
                    line_count += cleanPrintfln("hot key found with counter: %llu\tkeyname: %s", 
                        counters[k], hotkeys[k]);
                }
            }

            printf("\033[%dA\r", line_count);
        }

        /* Sleep if we've been directed to do so */
        if (config.interval && (scan_loops % 100) == 0) {
            usleep(config.interval);
        }

        freeReplyObject(reply);
    } while(force_cancel_loop ==0 && it != 0);

    /* Final progress bar in TTY */
    if (IS_TTY_OR_FAKETTY()) {
        displayKeyStatsProgressbar(sampled, total_keys);

        /* clean the types info shown during the progress bar */
        int line_count = 0;
        for (k = 0; k <= HOTKEYS_SAMPLE; k++)
            line_count += cleanPrintfln("");
        printf("\033[%dA\r", line_count);
    }

    if (freqs) zfree(freqs);

    /* We're done */
    printf("\n-------- summary -------\n\n");

    /* Show the original output when writing to a file */
    if (!IS_TTY_OR_FAKETTY()) {
        if(force_cancel_loop) printf("[%05.2f%%] ",pct);
        printf("Sampled %llu keys in the keyspace!\n", sampled);
    }

    for (k = HOTKEYS_SAMPLE - 1; k >= 0; k--) {
        if (counters[k] > 0) {
            printf("hot key found with counter: %llu\tkeyname: %s\n", counters[k], hotkeys[k]);
            sdsfree(hotkeys[k]);
        }
    }

    exit(0);
}

/*------------------------------------------------------------------------------
 * Stats mode — 服务器统计模式
 *--------------------------------------------------------------------------- */

/* 从 INFO 命令输出 'info' 中取回指定字段值。
 * 返回的缓冲区需要由调用者释放；若字段不存在则返回 NULL。 */
static char *getInfoField(char *info, char *field) {
    char *p = strstr(info,field);
    char *n1, *n2;
    char *result;

    if (!p) return NULL;
    p += strlen(field)+1;
    n1 = strchr(p,'\r');
    n2 = strchr(p,',');
    if (n2 && n2 < n1) n1 = n2;
    result = zmalloc(sizeof(char)*(n1-p)+1);
    memcpy(result,p,(n1-p));
    result[n1-p] = '\0';
    return result;
}

/* 与 getInfoField 类似，但自动转换为 long。出错（字段缺失）时返回 LONG_MIN。 */
static long getLongInfoField(char *info, char *field) {
    char *value = getInfoField(info,field);
    long l;

    if (!value) return LONG_MIN;
    l = strtol(value,NULL,10);
    zfree(value);
    return l;
}

/* 将字节数转换为人类可读字符串，形式为：
 * 1003B, 4.03K, 100.00M, 2.32G, 3.01T。
 * 返回参数 's'，其中保存转换结果。 */
char *bytesToHuman(char *s, size_t size, long long n) {
    double d;
    char *r = s;

    if (n < 0) {
        *s = '-';
        s++;
        n = -n;
    }
    if (n < 1024) {
        /* Bytes */
        snprintf(s,size,"%lldB",n);
    } else if (n < (1024*1024)) {
        d = (double)n/(1024);
        snprintf(s,size,"%.2fK",d);
    } else if (n < (1024LL*1024*1024)) {
        d = (double)n/(1024*1024);
        snprintf(s,size,"%.2fM",d);
    } else if (n < (1024LL*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024);
        snprintf(s,size,"%.2fG",d);
    } else if (n < (1024LL*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024);
        snprintf(s,size,"%.2fT",d);
    }

    return r;
}

/* --stat 模式：周期性打印服务器统计信息 */
static void statMode(void) {
    redisReply *reply;
    long aux, requests = 0;
    int dbnum = getDatabases();
    int i = 0;

    while(1) {
        char buf[64];
        int j;

        reply = reconnectingRedisCommand(context,"INFO");
        if (reply == NULL) {
            fprintf(stderr, "\nI/O error\n");
            exit(1);
        } else if (reply->type == REDIS_REPLY_ERROR) {
            fprintf(stderr, "ERROR: %s\n", reply->str);
            exit(1);
        }

        if ((i++ % 20) == 0) {
            printf(
"------- data ------ --------------------- load -------------------- - child -\n"
"keys       mem      clients blocked requests            connections          \n");
        }

        /* Keys */
        aux = 0;
        for (j = 0; j < dbnum; j++) {
            long k;

            snprintf(buf,sizeof(buf),"db%d:keys",j);
            k = getLongInfoField(reply->str,buf);
            if (k == LONG_MIN) continue;
            aux += k;
        }
        snprintf(buf,sizeof(buf),"%ld",aux);
        printf("%-11s",buf);

        /* Used memory */
        aux = getLongInfoField(reply->str,"used_memory");
        bytesToHuman(buf,sizeof(buf),aux);
        printf("%-8s",buf);

        /* Clients */
        aux = getLongInfoField(reply->str,"connected_clients");
        snprintf(buf,sizeof(buf),"%ld",aux);
        printf(" %-8s",buf);

        /* Blocked (BLPOPPING) Clients */
        aux = getLongInfoField(reply->str,"blocked_clients");
        snprintf(buf,sizeof(buf),"%ld",aux);
        printf("%-8s",buf);

        /* Requests */
        aux = getLongInfoField(reply->str,"total_commands_processed");
        snprintf(buf,sizeof(buf),"%ld (+%ld)",aux,requests == 0 ? 0 : aux-requests);
        printf("%-19s",buf);
        requests = aux;

        /* Connections */
        aux = getLongInfoField(reply->str,"total_connections_received");
        snprintf(buf,sizeof(buf),"%ld",aux);
        printf(" %-12s",buf);

        /* Children */
        aux = getLongInfoField(reply->str,"bgsave_in_progress");
        aux |= getLongInfoField(reply->str,"aof_rewrite_in_progress") << 1;
        aux |= getLongInfoField(reply->str,"loading") << 2;
        switch(aux) {
        case 0: break;
        case 1:
            printf("SAVE");
            break;
        case 2:
            printf("AOF");
            break;
        case 3:
            printf("SAVE+AOF");
            break;
        case 4:
            printf("LOAD");
            break;
        }

        printf("\n");
        freeReplyObject(reply);
        usleep(config.interval);
    }
}

/*------------------------------------------------------------------------------
 * Scan mode — SCAN 模式
 *--------------------------------------------------------------------------- */

/* --scan 模式：通过 SCAN 遍历并输出所有匹配 key */
static void scanMode(void) {
    redisReply *reply;
    unsigned long long cur = 0;
    signal(SIGINT, longStatLoopModeStop);
    do {
        reply = sendScan(&cur);
        for (unsigned int j = 0; j < reply->element[1]->elements; j++) {
            if (config.output == OUTPUT_STANDARD) {
                sds out = sdscatrepr(sdsempty(), reply->element[1]->element[j]->str,
                                     reply->element[1]->element[j]->len);
                printf("%s\n", out);
                sdsfree(out);
            } else {
                printf("%s\n", reply->element[1]->element[j]->str);
            }
        }
        freeReplyObject(reply);
        if (config.interval) usleep(config.interval);
    } while(force_cancel_loop == 0 && cur != 0);

    exit(0);
}

/*------------------------------------------------------------------------------
 * LRU test mode — LRU 缓存模拟测试
 *--------------------------------------------------------------------------- */

/* 返回一个 [min, max] 区间内（含两端）的整数，服从幂律分布。
 * alpha 越大越偏向较小值；alpha = 6.2 时近似 80-20 分布，
 * 即 20% 的返回值占总频次的 80%。 */
long long powerLawRand(long long min, long long max, double alpha) {
    double pl, r;

    max += 1;
    r = ((double)rand()) / RAND_MAX;
    pl = pow(
        ((pow(max,alpha+1) - pow(min,alpha+1))*r + pow(min,alpha+1)),
        (1.0/(alpha+1)));
    return (max-1-(long long)pl)+min;
}

/* 在 lru_test_sample_size 个 key 中按 80-20 分布生成一个 key 名 */
void LRUTestGenKey(char *buf, size_t buflen) {
    snprintf(buf, buflen, "lru:%lld",
        powerLawRand(1, config.lru_test_sample_size, 6.2));
}

/* LRU 测试每个周期长度（毫秒） */
#define LRU_CYCLE_PERIOD 1000
/* LRU 测试每个周期内的流水线命令数 */
#define LRU_CYCLE_PIPELINE_SIZE 250

/* --lru-test 模式：模拟缓存访问负载并报告命中率 */
static void LRUTestMode(void) {
    redisReply *reply;
    char key[128];
    long long start_cycle;
    int j;

    srand(time(NULL)^getpid());
    while(1) {
        /* Perform cycles of 1 second with 50% writes and 50% reads.
         * We use pipelining batching writes / reads N times per cycle in order
         * to fill the target instance easily. */
        start_cycle = mstime();
        long long hits = 0, misses = 0;
        while(mstime() - start_cycle < LRU_CYCLE_PERIOD) {
            /* Write cycle. */
            for (j = 0; j < LRU_CYCLE_PIPELINE_SIZE; j++) {
                char val[6];
                val[5] = '\0';
                for (int i = 0; i < 5; i++) val[i] = 'A'+rand()%('z'-'A');
                LRUTestGenKey(key,sizeof(key));
                redisAppendCommand(context, "SET %s %s",key,val);
            }
            for (j = 0; j < LRU_CYCLE_PIPELINE_SIZE; j++)
                redisGetReply(context, (void**)&reply);

            /* Read cycle. */
            for (j = 0; j < LRU_CYCLE_PIPELINE_SIZE; j++) {
                LRUTestGenKey(key,sizeof(key));
                redisAppendCommand(context, "GET %s",key);
            }
            for (j = 0; j < LRU_CYCLE_PIPELINE_SIZE; j++) {
                if (redisGetReply(context, (void**)&reply) == REDIS_OK) {
                    switch(reply->type) {
                        case REDIS_REPLY_ERROR:
                            fprintf(stderr, "%s\n", reply->str);
                            break;
                        case REDIS_REPLY_NIL:
                            misses++;
                            break;
                        default:
                            hits++;
                            break;
                    }
                }
            }

            if (context->err) {
                fprintf(stderr,"I/O error during LRU test\n");
                exit(1);
            }
        }
        /* Print stats. */
        printf(
            "%lld Gets/sec | Hits: %lld (%.2f%%) | Misses: %lld (%.2f%%)\n",
            hits+misses,
            hits, (double)hits/(hits+misses)*100,
            misses, (double)misses/(hits+misses)*100);
    }
    exit(0);
}

/*------------------------------------------------------------------------------
 * Intrinsic latency mode — 系统固有延迟测量
 *
 * 测量进程自身的最大延迟（不涉及系统调用）。
 * 用来估算内核在没有调度到本进程时消耗的时间。
 *--------------------------------------------------------------------------- */

/* 一段不会被编译器优化掉的计算逻辑。
 * 在非常慢的硬件上也应在 100-200 微秒内完成；现代硬件一般 < 10 微秒。 */
unsigned long compute_something_fast(void) {
    unsigned char s[256], i, j, t;
    int count = 1000, k;
    unsigned long output = 0;

    for (k = 0; k < 256; k++) s[k] = k;

    i = 0;
    j = 0;
    while(count--) {
        i++;
        j = j + s[i];
        t = s[i];
        s[i] = s[j];
        s[j] = t;
        output += s[(s[i]+s[j])&255];
    }
    return output;
}

/* SIGINT 处理：用于中断 MONITOR 或 pubsub 阻塞状态 */
static void sigIntHandler(int s) {
    UNUSED(s);

    if (config.monitor_mode || config.pubsub_mode) {
        close(context->fd);
        context->fd = REDIS_INVALID_FD;
        config.blocking_state_aborted = 1;
    } else {
        exit(1);
    }
}

/* --intrinsic-latency 模式：测量进程自身的固有延迟 */
static void intrinsicLatencyMode(void) {
    long long test_end, run_time, max_latency = 0, runs = 0;

    run_time = (long long)config.intrinsic_latency_duration * 1000000;
    test_end = ustime() + run_time;
    signal(SIGINT, longStatLoopModeStop);

    while(1) {
        long long start, end, latency;

        start = ustime();
        compute_something_fast();
        end = ustime();
        latency = end-start;
        runs++;
        if (latency <= 0) continue;

        /* Reporting */
        if (latency > max_latency) {
            max_latency = latency;
            printf("Max latency so far: %lld microseconds.\n", max_latency);
        }

        double avg_us = (double)run_time/runs;
        double avg_ns = avg_us * 1e3;
        if (force_cancel_loop || end > test_end) {
            printf("\n%lld total runs "
                "(avg latency: "
                "%.4f microseconds / %.2f nanoseconds per run).\n",
                runs, avg_us, avg_ns);
            printf("Worst run took %.0fx longer than the average latency.\n",
                max_latency / avg_us);
            exit(0);
        }
    }
}

/* 在终端上以隐藏输入方式提示输入密码 */
static sds askPassword(const char *msg) {
    linenoiseMaskModeEnable();
    sds auth = linenoise(msg);
    linenoiseMaskModeDisable();
    return auth;
}

/* 输出给定输入前缀的提示补全字符串 */
void testHint(const char *input) {
    cliInitHelp();

    sds hint = getHintForInput(input);
    printf("%s\n", hint);
    exit(0);
}

sds readHintSuiteLine(char buf[], size_t size, FILE *fp) {
    while (fgets(buf, size, fp) != NULL) {
        if (buf[0] != '#') {
            sds input = sdsnew(buf);

            /* Strip newline. */
            input = sdstrim(input, "\n");
            return input;
        }
    }
    return NULL;
}

/* 运行文件中的提示补全测试套件 */
void testHintSuite(char *filename) {
    FILE *fp;
    char buf[256];
    sds line, input, expected, hint;
    int pass=0, fail=0;
    int argc;
    char **argv;

    fp = fopen(filename, "r");
    if (!fp) {
        fprintf(stderr,
            "Can't open file '%s': %s\n", filename, strerror(errno));
        exit(-1);
    }

    cliInitHelp();

    while (1) {
        line = readHintSuiteLine(buf, sizeof(buf), fp);
        if (line == NULL) break;
        argv = sdssplitargs(line, &argc);
        sdsfree(line);
        if (argc == 0) {
            sdsfreesplitres(argv, argc);
            continue;
        }

        if (argc == 1) {
            fprintf(stderr,
                "Missing expected hint for input '%s'\n", argv[0]);
            exit(-1);
        }
        input = argv[0];
        expected = argv[1];
        hint = getHintForInput(input);
        if (config.verbose) {
            printf("Input: '%s', Expected: '%s', Hint: '%s'\n", input, expected, hint);
        }

        /* Strip trailing spaces from hint - they don't matter. */
        while (hint != NULL && sdslen(hint) > 0 && hint[sdslen(hint) - 1] == ' ') {            
            sdssetlen(hint, sdslen(hint) - 1);
            hint[sdslen(hint)] = '\0';
        }

        if (hint == NULL || strcmp(hint, expected) != 0) {
            fprintf(stderr, "Test case '%s' FAILED: expected '%s', got '%s'\n", input, expected, hint);
            ++fail;
        }
        else {
            ++pass;
        }
        sdsfreesplitres(argv, argc);
        sdsfree(hint);
    }
    fclose(fp);
    
    printf("%s: %d/%d passed\n", fail == 0 ? "SUCCESS" : "FAILURE", pass, pass + fail);
    exit(fail);
}

/*------------------------------------------------------------------------------
 * Keystats — 键统计信息展示
 *--------------------------------------------------------------------------- */

/* Key 名长度分布相关结构 */

/* 单个长度区间的统计条目 */
typedef struct size_dist_entry {
    unsigned long long size;        /* Key 名长度（字节） */
    unsigned long long count;       /* 长度小于等于 size 的 key 数量 */
} size_dist_entry;

/* Key 名长度分布统计 */
typedef struct size_dist {
    unsigned long long total_count; /* key 总数 */
    unsigned long long total_size;  /* 所有 key 名长度总和（字节） */
    unsigned long long max_size;    /* 最大的 key 名长度 */
    size_dist_entry *size_dist;     /* 各区间的 size/count 数组 */
} size_dist;

/* 初始化 size_dist。distribution 是以 {0, 0} 结尾的 size_dist_entry 数组，
 * 例如：size_dist_entry distribution[] = { {32, 0}, {256, 0}, {0, 0} }; */
static void sizeDistInit(size_dist *dist, size_dist_entry *distribution) {
    dist->max_size = 0;
    dist->total_count = 0;
    dist->total_size = 0;
    dist->size_dist = distribution;
}

static void addSizeDist(size_dist *dist, unsigned long long size) {
    dist->total_count++;
    dist->total_size += size;

    if (size > dist->max_size)
        dist->max_size = size;

    int j;
    for (j=0; dist->size_dist[j].size && size > dist->size_dist[j].size; j++);
    dist->size_dist[j].count++;
}

static int displayKeyStatsLengthDist(size_dist *dist) {
    int line_count = 0;
    unsigned long long total_keys = 0, size;
    char buf[2][256];

    line_count += cleanPrintfln("Key name length Percentile Total keys");
    line_count += cleanPrintfln("--------------- ---------- -----------");

    for (int i=0; dist->size_dist[i].size; i++) {
        if (dist->size_dist[i].count) {
            if (dist->max_size < dist->size_dist[i].size) {
                size = dist->max_size;
            } else {
                size = dist->size_dist[i].size;
            }
            total_keys += dist->size_dist[i].count;
            line_count += cleanPrintfln("%15s %9.4f%% %11llu",
                bytesToHuman(buf[1], sizeof(buf[1]), size),
                (double)100 * total_keys / dist->total_count,
                total_keys);
        }
    }

    if (total_keys < dist->total_count) {
        line_count += cleanPrintfln("           inf %9.4f%% %11llu", 100.0, dist->total_count);
    }

    line_count += cleanPrintfln("Total key length is %s (%s avg)",
        bytesToHuman(buf[0], sizeof(buf[0]), dist->total_size),
        dist->total_count ? bytesToHuman(buf[1], sizeof(buf[1]), dist->total_size/dist->total_count) : "0");

    return line_count;
}

/* 进度条字符宽度 */
#define PROGRESSBAR_WIDTH 60

/* 显示 bigkeys/memkeys/keystats 等扫描的进度条 */
static int displayKeyStatsProgressbar(unsigned long long sampled,
                                      unsigned long long total_keys)
{
    int line_count = 0;
    char progressbar[512];
    char buf[2][128];

    /* We can go over 100% if keys are added in the middle of the scans.
     * Cap at 100% or the progressbar memset will overflow. */
    double completion_pct = total_keys ? sampled < total_keys ? (double) sampled/total_keys : 1 : 0;

    /* If we are not redirecting to a file, build the progress bar */
    if (IS_TTY_OR_FAKETTY()) {
        int completed_width = (int)round(PROGRESSBAR_WIDTH * completion_pct);
        memset(buf[0], '|', completed_width);
        buf[0][completed_width]= '\0';

        int uncompleted_width = PROGRESSBAR_WIDTH - completed_width;
        memset(buf[1], '-', uncompleted_width);
        buf[1][uncompleted_width]= '\0';

        char red[] = "\033[31m";
        char green[] = "\033[32m";
        char default_color[] = "\033[39m";
        snprintf(progressbar, sizeof(progressbar), "%s%s%s%s%s",
            green, buf[0], red, buf[1], default_color);
    } else {
        snprintf(progressbar, sizeof(progressbar), "%s", "keys scanned");
    }

    line_count += cleanPrintfln("%6.2f%% %s", completion_pct * 100, progressbar);
    line_count += cleanPrintfln("Keys sampled: %llu", sampled);

    return line_count;
}

static int displayKeyStatsSizeType(dict *memkeys_types_dict) {
    dictIterator *di;
    dictEntry *de;
    int line_count = 0;
    char buf[256];

    line_count += cleanPrintfln("--- Top size per type ---");
    di = dictGetIterator(memkeys_types_dict);
    while ((de = dictNext(di))) {
        typeinfo *type = dictGetVal(de);
        if (type->biggest_key) {
            line_count += cleanPrintfln("%-10s %s is %s",
                type->name, type->biggest_key,
                bytesToHuman(buf, sizeof(buf),type->biggest));
        }
    }
    dictReleaseIterator(di);

    return line_count;
}

static int displayKeyStatsLengthType(dict *bigkeys_types_dict) {
    dictIterator *di;
    dictEntry *de;
    int line_count = 0;
    char buf[256];

    line_count += cleanPrintfln("--- Top length and cardinality per type ---");
    di = dictGetIterator(bigkeys_types_dict);
    while ((de = dictNext(di))) {
        typeinfo *type = dictGetVal(de);
        if (type->biggest_key) {
            if (!strcmp(type->sizeunit, "bytes")) {
                bytesToHuman(buf, sizeof(buf), type->biggest);
            } else {
                snprintf(buf, sizeof(buf), "%llu %s", type->biggest, type->sizeunit);
            }
            line_count += cleanPrintfln("%-10s %s has %s", type->name, type->biggest_key, buf);
        }
    }
    dictReleaseIterator(di);

    return line_count;
}

static int displayKeyStatsSizeDist(struct hdr_histogram *keysize_histogram) {
    int line_count = 0;
    double percentile;
    char size[32], mean[32], stddev[32];
    struct hdr_iter iter;
    int64_t last_displayed_cumulative_count = 0;

    hdr_iter_percentile_init(&iter, keysize_histogram, 1);

    line_count += cleanPrintfln("Key size Percentile Total keys");
    line_count += cleanPrintfln("-------- ---------- -----------");

    while (hdr_iter_next(&iter)) {
        /* Skip repeat in hdr_histogram cumulative_count, and set the last line
         * to 100% when total_count is reached. For instance:
         * 140.68K    99.9969%        50013
         * 140.68K    99.9977%        50013
         *   2.04G    99.9985%        50014
         *   2.04G   100.0000%        50014
         * Will display:
         * 140.68K    99.9969%        50013
         *   2.04G   100.0000%        50014                                   */

        if (iter.cumulative_count != last_displayed_cumulative_count) {
            if (iter.cumulative_count == iter.h->total_count) {
                percentile = 100;
            } else {
                percentile = iter.specifics.percentiles.percentile;
            }

            line_count += cleanPrintfln("%8s %9.4f%% %11lld",
                bytesToHuman(size, sizeof(size), iter.highest_equivalent_value),
                percentile,
                iter.cumulative_count);

            last_displayed_cumulative_count = iter.cumulative_count;
        }
    }

    bytesToHuman(mean, sizeof(mean),hdr_mean(keysize_histogram));
    bytesToHuman(stddev, sizeof(stddev),hdr_stddev(keysize_histogram));
    line_count += cleanPrintfln("Note: 0.01%% size precision, Mean: %s, StdDeviation: %s", mean, stddev);

    return line_count;
}

static int displayKeyStatsType(unsigned long long sampled,
                               dict *memkeys_types_dict,
                               dict *bigkeys_types_dict)
{
    dictIterator *di;
    dictEntry *de;
    int line_count = 0;
    char total_size[64], size_avg[64], total_length[64], length_avg[64];

    line_count += cleanPrintfln("Type        Total keys  Keys %% Tot size Avg size  Total length/card Avg ln/card");
    line_count += cleanPrintfln("--------- ------------ ------- -------- -------- ------------------ -----------");

    di = dictGetIterator(memkeys_types_dict);
    while ((de = dictNext(di))) {
        typeinfo *memkey_type = dictGetVal(de);
        if (memkey_type->count) {
            /* Key count, percentage, memkeys info */
            bytesToHuman(total_size, sizeof(total_size), memkey_type->totalsize);
            bytesToHuman(size_avg, sizeof(size_avg), memkey_type->totalsize/memkey_type->count);

            strncpy(total_length, " - ", sizeof(total_length));
            strncpy(length_avg, " - ", sizeof(length_avg));

            /* bigkeys info */
            dictEntry *bk_de = dictFind(bigkeys_types_dict, memkey_type->name);
            if (bk_de) { /* If we have it in memkeys it should be in bigkeys */
                typeinfo *bigkey_type = dictGetVal(bk_de);
                if (bigkey_type->sizecmd && bigkey_type->count) {
                    double avg = (double)bigkey_type->totalsize/bigkey_type->count;
                    if (!strcmp(bigkey_type->sizeunit, "bytes")) {
                        bytesToHuman(total_length, sizeof(total_length), bigkey_type->totalsize);
                        bytesToHuman(length_avg, sizeof(length_avg), (long long)round(avg)); /* better than truncating */
                    } else {
                        snprintf(total_length, sizeof(total_length), "%llu %s", bigkey_type->totalsize, bigkey_type->sizeunit);
                        snprintf(length_avg, sizeof(length_avg), "%.2f", avg);
                    }
                }
            }
            /* Print the line for the given Redis type */
            line_count += cleanPrintfln("%-10s %11llu %6.2f%% %8s %8s %18s %11s",
                memkey_type->name, memkey_type->count,
                sampled ? 100 * (double)memkey_type->count/sampled : 0,
                total_size, size_avg, total_length, length_avg);
        }
    }
    dictReleaseIterator(di);

    return line_count;
}

typedef struct key_info {
    unsigned long long size;
    char type_name[10]; /* Key type name seems to be 9 char max + \0 */
    sds key_name;
} key_info;

static int displayKeyStatsTopSizes(list *top_key_sizes, unsigned long top_sizes_limit) {
    int line_count = 0, i = 0;

    line_count += cleanPrintfln("--- Top %llu key sizes ---", top_sizes_limit);
    char buffer[32];
    listIter *iter = listGetIterator(top_key_sizes, AL_START_HEAD);
    listNode *node;
    while ((node = listNext(iter)) != NULL) {
        key_info *key = (key_info*) listNodeValue(node);
        line_count += cleanPrintfln("%3d %8s %-10s %s", ++i, bytesToHuman(buffer, sizeof(buffer), key->size),
                                    key->type_name, key->key_name);
    }
    listReleaseIterator(iter);

    return line_count;
}

static key_info *createKeySizeInfo(char *key_name, size_t key_name_len, char *key_type, unsigned long long size) {
    key_info *key = zmalloc(sizeof(key_info));
    key->size = size;
    snprintf(key->type_name, sizeof(key->type_name), "%s", key_type);
    key->key_name = sdscatrepr(sdsempty(), key_name, key_name_len);
    if (!key->key_name) {
        fprintf(stderr, "Failed to allocate memory for key name.\n");
        exit(1);
    }
    return key;
}

/* Insert key info in topkeys sorted by size (from high to low size).
 * Keep a maximum of config.top_sizes_limit items in topkeys list.
 * key_name and type_name are copied.
 * Return: 0 size was not added (too small), 1 size was inserted.  */
static int updateTopSizes(char *key_name, size_t key_name_len, unsigned long long key_size,
                          char *type_name, list *topkeys, unsigned long top_sizes_limit)
{
    listNode *node;
    listIter *iter;
    key_info *new_node;

    /* Check if we do not need to add to the list */
    if (top_sizes_limit != 0 &&
        topkeys->len == top_sizes_limit &&
        key_size <= ((key_info*)topkeys->tail->value)->size){
        return 0;
    }

    /* Find where to insert the new key size */
    iter = listGetIterator(topkeys, AL_START_HEAD);
    do {
        node = listNext(iter);
    } while (node != NULL && key_size <= ((key_info*)node->value)->size);
    listReleaseIterator(iter);

    new_node = createKeySizeInfo(key_name, key_name_len, type_name, key_size);
    if (node) {
        /* Insert before the node */
        listInsertNode(topkeys, node, new_node, 0);
    } else {
        /* Insert as the last node */
        listAddNodeTail(topkeys, new_node);
    }

    /* Trim to stay within the limit */
    if (topkeys->len == top_sizes_limit + 1) {
        sdsfree(((key_info*)topkeys->tail->value)->key_name);
        listDelNode(topkeys, topkeys->tail); /* list->free is set */
    }

    return 1;
}

static void displayKeyStats(unsigned long long sampled, unsigned long long total_keys,
                            unsigned long long total_size, dict *memkeys_types_dict,
                            dict *bigkeys_types_dict, list *top_key_sizes,
                            unsigned long top_sizes_limit, int move_cursor_up)
{
    int line_count = 0;
    char buf[256];

    line_count += displayKeyStatsProgressbar(sampled, total_keys);
    line_count += cleanPrintfln("Keys size:    %s", bytesToHuman(buf, sizeof(buf), total_size));
    line_count += cleanPrintfln("");
    line_count += displayKeyStatsTopSizes(top_key_sizes, top_sizes_limit);
    line_count += cleanPrintfln("");
    line_count += displayKeyStatsSizeType(memkeys_types_dict);
    line_count += cleanPrintfln("");
    line_count += displayKeyStatsLengthType(bigkeys_types_dict);

    /* If we need to refresh the stats */
    if (move_cursor_up) {
        printf("\033[%dA\r", line_count);
    }

    fflush(stdout);
}

static void updateKeyType(redisReply *element, unsigned long long size, typeinfo *type) {
    type->totalsize += size;
    type->count++;

    if (type->biggest<size) {
        /* Keep track of biggest key name for this type */
        if (type->biggest_key)
            sdsfree(type->biggest_key);
        type->biggest_key = sdsnewlen(element->str, element->len);
        if (!type->biggest_key) {
            fprintf(stderr, "Failed to allocate memory for key!\n");
            exit(1);
        }
        /* Keep track of the biggest size for this type */
        type->biggest = size;
    }
}

static void keyStats(long long memkeys_samples, unsigned long long cursor, unsigned long top_sizes_limit) {
    unsigned long long sampled = 0, total_keys, total_size = 0, it = 0, scan_loops = 0;
    unsigned long long *memkeys_sizes = NULL, *bigkeys_sizes = NULL;
    redisReply *reply, *keys;
    unsigned int array_size = 0, i;
    typeinfo **memkeys_types = NULL, **bigkeys_types = NULL;
    list *top_sizes;
    long long refresh_time = mstime();

    if (cursor != 0) {
        it = cursor;
    }

    if ((top_sizes = listCreate()) == NULL) {
        fprintf(stderr, "top_sizes list creation failed.\n");
        exit(1);
    }
    top_sizes->free = zfree;

    dict *memkeys_types_dict = dictCreate(&typeinfoDictType);
    typeinfo_add(memkeys_types_dict, "string", &type_string);
    typeinfo_add(memkeys_types_dict, "list", &type_list);
    typeinfo_add(memkeys_types_dict, "set", &type_set);
    typeinfo_add(memkeys_types_dict, "hash", &type_hash);
    typeinfo_add(memkeys_types_dict, "zset", &type_zset);
    typeinfo_add(memkeys_types_dict, "stream", &type_stream);

    /* We could use only one typeinfo dictionary if we add new fields to save
     * both memkey and bigkey info. Not sure it would make sense in findBigKeys(). */
    dict *bigkeys_types_dict = dictCreate(&typeinfoDictType);
    typeinfo_add(bigkeys_types_dict, "string", &type_string);
    typeinfo_add(bigkeys_types_dict, "list", &type_list);
    typeinfo_add(bigkeys_types_dict, "set", &type_set);
    typeinfo_add(bigkeys_types_dict, "hash", &type_hash);
    typeinfo_add(bigkeys_types_dict, "zset", &type_zset);
    typeinfo_add(bigkeys_types_dict, "stream", &type_stream);

    size_dist key_length_dist;
    size_dist_entry distribution[] = {
        {1<<5, 0},                 /*  32 B  (sds)                                            */
        {1<<8, 0},                 /* 256 B  (sds)                                            */
        {1<<16, 0},                /*  64 KB (sds and Redis Enterprise key name max length)   */
        {1024*1024, 0},            /*   1 MB                                                  */
        {16*1024*1024, 0},         /*  16 MB                                                  */
        {128*1024*1024, 0},        /* 128 MB                                                  */
        {512*1024*1024, 0},        /* 512 MB (max String size)                                */
        {0, 0},                    /* Sizes above the last entry                              */
    };
    sizeDistInit(&key_length_dist, distribution);

    struct hdr_histogram *keysize_histogram;
    /* Record max of 1TB for a key size should cover all keys.
     * significant_figures == 4 (0.01% precision on key size)  */
    if (hdr_init(1, 1ULL*1024*1024*1024*1024, 4, &keysize_histogram)) {
        fprintf(stderr, "Keystats hdr init error\n");
        exit(1);
    }

    signal(SIGINT, longStatLoopModeStop);

    /* Total keys pre scanning */
    total_keys = getDbSize();

    /* Status message */
    printf("\n# Scanning the entire keyspace to find the biggest keys and distribution information.\n");
    printf("# Use -i 0.1 to sleep 0.1 sec per 100 SCAN commands (not usually needed).\n");
    printf("# Use --cursor <n> to start the scan at the cursor <n> (usually after a Ctrl-C).\n");
    printf("# Use --top <n> to display <n> top key sizes (default is 10).\n");
    printf("# Ctrl-C to stop the scan.\n\n");

    /* Use readonly in cluster */
    sendReadOnly();

    /* SCAN loop */
    do {
        /* Grab some keys and point to the keys array */
        reply = sendScan(&it);
        scan_loops++;
        keys = reply->element[1];

        /* Reallocate our type and size array if we need to */
        if (keys->elements > array_size) {
            memkeys_types = zrealloc(memkeys_types, sizeof(typeinfo*)*keys->elements);
            memkeys_sizes = zrealloc(memkeys_sizes, sizeof(unsigned long long)*keys->elements);

            bigkeys_types = zrealloc(bigkeys_types, sizeof(typeinfo*)*keys->elements);
            bigkeys_sizes = zrealloc(bigkeys_sizes, sizeof(unsigned long long)*keys->elements);

            if (!memkeys_types || !memkeys_sizes || !bigkeys_types || !bigkeys_sizes) {
                fprintf(stderr, "Failed to allocate storage for keys!\n");
                exit(1);
            }

            array_size = keys->elements;
        }

        /* Retrieve types and sizes for memkeys */
        getKeyTypes(memkeys_types_dict, keys, memkeys_types);
        getKeySizes(keys, memkeys_types, memkeys_sizes, 1, memkeys_samples);

        /* Retrieve types and sizes for bigkeys */
        getKeyTypes(bigkeys_types_dict, keys, bigkeys_types);
        getKeySizes(keys, bigkeys_types, bigkeys_sizes, 0, memkeys_samples);

        for (i=0; i<keys->elements; i++) {
            /* Skip keys that disappeared between SCAN and TYPE */
            if (!memkeys_types[i] || !bigkeys_types[i]) {
                continue;
            }

            total_size += memkeys_sizes[i];
            sampled++;

            updateTopSizes(keys->element[i]->str, keys->element[i]->len, memkeys_sizes[i],
                           memkeys_types[i]->name, top_sizes, top_sizes_limit);
            updateKeyType(keys->element[i], memkeys_sizes[i], memkeys_types[i]);
            updateKeyType(keys->element[i], bigkeys_sizes[i], bigkeys_types[i]);

            /* Key Size distribution */
            if (hdr_record_value(keysize_histogram, memkeys_sizes[i]) == 0) {
                fprintf(stderr, "Value %llu not added in the hdr histogram.\n", memkeys_sizes[i]);
            }

            /* Key length distribution */
            addSizeDist(&key_length_dist, keys->element[i]->len);
        }

        /* Refresh keystats info on regular basis */
        if (mstime() > refresh_time + REFRESH_INTERVAL && IS_TTY_OR_FAKETTY()) {
            displayKeyStats(sampled, total_keys, total_size, memkeys_types_dict, bigkeys_types_dict,
                top_sizes, top_sizes_limit, 1);
            refresh_time = mstime();
        }

        /* Sleep if we've been directed to do so */
        if (config.interval && (scan_loops % 100) == 0) {
            usleep(config.interval);
        }

        freeReplyObject(reply);
    } while(force_cancel_loop == 0 && it != 0);

    displayKeyStats(sampled, total_keys, total_size, memkeys_types_dict, bigkeys_types_dict, top_sizes,
                    top_sizes_limit, 0);

    /* Additional data at the end of the SCAN loop.
     * Using cleanPrintfln in case we want to print during the SCAN loop. */
    cleanPrintfln("");
    displayKeyStatsSizeDist(keysize_histogram);
    cleanPrintfln("");
    displayKeyStatsLengthDist(&key_length_dist);
    cleanPrintfln("");
    displayKeyStatsType(sampled, memkeys_types_dict, bigkeys_types_dict);

    if (it != 0) {
        printf("\n");
        printf("Scan interrupted:\n");
        printf("Use 'redis-cli --keystats --cursor %llu' to restart from the last cursor.\n", it);
    }

    if (memkeys_types) zfree(memkeys_types);
    if (bigkeys_types) zfree(bigkeys_types);
    if (memkeys_sizes) zfree(memkeys_sizes);
    if (bigkeys_sizes) zfree(bigkeys_sizes);
    dictRelease(memkeys_types_dict);
    dictRelease(bigkeys_types_dict);
    hdr_close(keysize_histogram);

    /* sdsfree before listRelease */
    listIter *iter = listGetIterator(top_sizes, AL_START_HEAD);
    listNode *node;
    while ((node = listNext(iter)) != NULL) {
        key_info *key = (key_info*) listNodeValue(node);
        sdsfree(key->key_name);
    }
    listReleaseIterator(iter);
    listRelease(top_sizes); /* list->free is set */

    exit(0);
}

/*------------------------------------------------------------------------------
 * Program main() — 程序入口
 *--------------------------------------------------------------------------- */

/* redis-cli 主入口：解析参数、按所选模式运行 */
int main(int argc, char **argv) {
    int firstarg;
    struct timeval tv;

    memset(&config.sslconfig, 0, sizeof(config.sslconfig));
    config.conn_info.hostip = sdsnew("127.0.0.1");
    config.conn_info.hostport = 6379;
    config.connect_timeout.tv_sec = 0;
    config.connect_timeout.tv_usec = 0;
    config.hostsocket = NULL;
    config.repeat = 1;
    config.interval = 0;
    config.dbnum = 0;
    config.conn_info.input_dbnum = 0;
    config.interactive = 0;
    config.shutdown = 0;
    config.monitor_mode = 0;
    config.pubsub_mode = 0;
    config.blocking_state_aborted = 0;
    config.latency_mode = 0;
    config.latency_dist_mode = 0;
    config.latency_history = 0;
    config.lru_test_mode = 0;
    config.lru_test_sample_size = 0;
    config.cluster_mode = 0;
    config.cluster_send_asking = 0;
    config.slave_mode = 0;
    config.getrdb_mode = 0;
    config.get_functions_rdb_mode = 0;
    config.stat_mode = 0;
    config.scan_mode = 0;
    config.count = 10;
    config.intrinsic_latency_mode = 0;
    config.pattern = NULL;
    config.rdb_filename = NULL;
    config.pipe_mode = 0;
    config.pipe_timeout = REDIS_CLI_DEFAULT_PIPE_TIMEOUT;
    config.bigkeys = 0;
    config.memkeys = 0;
    config.keystats = 0;
    config.cursor = 0;
    config.top_sizes_limit = 10;
    config.hotkeys = 0;
    config.stdin_lastarg = 0;
    config.stdin_tag_arg = 0;
    config.stdin_tag_name = NULL;
    config.conn_info.auth = NULL;
    config.askpass = 0;
    config.conn_info.user = NULL;
    config.eval = NULL;
    config.eval_ldb = 0;
    config.eval_ldb_end = 0;
    config.eval_ldb_sync = 0;
    config.enable_ldb_on_eval = 0;
    config.last_cmd_type = -1;
    config.last_reply = NULL;
    config.verbose = 0;
    config.set_errcode = 0;
    config.no_auth_warning = 0;
    config.in_multi = 0;
    config.server_version = NULL;
    config.prefer_ipv4 = 0;
    config.prefer_ipv6 = 0;
    config.cluster_manager_command.name = NULL;
    config.cluster_manager_command.argc = 0;
    config.cluster_manager_command.argv = NULL;
    config.cluster_manager_command.stdin_arg = NULL;
    config.cluster_manager_command.flags = 0;
    config.cluster_manager_command.replicas = 0;
    config.cluster_manager_command.from = NULL;
    config.cluster_manager_command.to = NULL;
    config.cluster_manager_command.from_user = NULL;
    config.cluster_manager_command.from_pass = NULL;
    config.cluster_manager_command.from_askpass = 0;
    config.cluster_manager_command.weight = NULL;
    config.cluster_manager_command.weight_argc = 0;
    config.cluster_manager_command.slots = 0;
    config.cluster_manager_command.timeout = CLUSTER_MANAGER_MIGRATE_TIMEOUT;
    config.cluster_manager_command.pipeline = CLUSTER_MANAGER_MIGRATE_PIPELINE;
    config.cluster_manager_command.threshold =
        CLUSTER_MANAGER_REBALANCE_THRESHOLD;
    config.cluster_manager_command.backup_dir = NULL;
    pref.hints = 1;

    spectrum_palette = spectrum_palette_color;
    spectrum_palette_size = spectrum_palette_color_size;

    if (!isatty(fileno(stdout)) && (getenv("FAKETTY") == NULL)) {
        config.output = OUTPUT_RAW;
        config.push_output = 0;
    } else {
        config.output = OUTPUT_STANDARD;
        config.push_output = 1;
    }
    config.mb_delim = sdsnew("\n");
    config.cmd_delim = sdsnew("\n");

    firstarg = parseOptions(argc,argv);
    argc -= firstarg;
    argv += firstarg;

    parseEnv();

    if (config.askpass) {
        config.conn_info.auth = askPassword("Please input password: ");
    }

    if (config.cluster_manager_command.from_askpass) {
        config.cluster_manager_command.from_pass = askPassword(
            "Please input import source node password: ");
    }

#ifdef USE_OPENSSL
    if (config.tls) {
        cliSecureInit();
    }
#endif

    gettimeofday(&tv, NULL);
    init_genrand64(((long long) tv.tv_sec * 1000000 + tv.tv_usec) ^ getpid());

    /* Cluster Manager mode */
    if (CLUSTER_MANAGER_MODE()) {
        clusterManagerCommandProc *proc = validateClusterManagerCommand();
        if (!proc) {
            exit(1);
        }
        clusterManagerMode(proc);
    }

    /* Latency mode */
    if (config.latency_mode) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        latencyMode();
    }

    /* Latency distribution mode */
    if (config.latency_dist_mode) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        latencyDistMode();
    }

    /* Slave mode */
    if (config.slave_mode) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        sendCapa();
        sendReplconf("rdb-filter-only", "");
        slaveMode(1);
    }

    /* Get RDB/functions mode. */
    if (config.getrdb_mode || config.get_functions_rdb_mode) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        sendCapa();
        sendRdbOnly();
        if (config.get_functions_rdb_mode && !sendReplconf("rdb-filter-only", "functions")) {
            fprintf(stderr, "Failed requesting functions only RDB from server, aborting\n");
            exit(1);
        }
        getRDB(NULL);
    }

    /* Pipe mode */
    if (config.pipe_mode) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        pipeMode();
    }

    /* Find big keys */
    if (config.bigkeys) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        findBigKeys(0, 0);
    }

    /* Find large keys */
    if (config.memkeys) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        findBigKeys(1, config.memkeys_samples);
    }

    /* Find big and large keys */
    if (config.keystats) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        keyStats(config.memkeys_samples, config.cursor, config.top_sizes_limit);
    }

    /* Find hot keys */
    if (config.hotkeys) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        findHotKeys();
    }

    /* Stat mode */
    if (config.stat_mode) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        if (config.interval == 0) config.interval = 1000000;
        statMode();
    }

    /* Scan mode */
    if (config.scan_mode) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        scanMode();
    }

    /* LRU test mode */
    if (config.lru_test_mode) {
        if (cliConnect(0) == REDIS_ERR) exit(1);
        LRUTestMode();
    }

    /* Intrinsic latency mode */
    if (config.intrinsic_latency_mode) intrinsicLatencyMode();

    /* 输出给定输入前缀的命令行提示 */
    if (config.test_hint) {
        testHint(config.test_hint);
    }
    /* 运行命令行提示测试套件 */
    if (config.test_hint_file) {
        testHintSuite(config.test_hint_file);
    }

    /* 未提供命令时进入交互模式 */
    if (argc == 0 && !config.eval) {
        /* 交互模式下忽略 SIGPIPE，以便断线时强制重连 */
        signal(SIGPIPE, SIG_IGN);
        signal(SIGINT, sigIntHandler);

        /* repl 模式下连接失败不直接退出，
         * 每次发送命令时都会尝试重连。 */
        cliConnect(0);
        repl();
    }

    /* 否则按提供的参数执行命令 */
    if (config.eval) {
        if (cliConnect(0) != REDIS_OK) exit(1);
        return evalMode(argc,argv);
    } else {
        cliConnect(CC_QUIET);
        return noninteractive(argc,argv);
    }
}
