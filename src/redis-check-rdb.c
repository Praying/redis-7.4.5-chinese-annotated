/*
 * Copyright (c) 2016-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

/*
 * redis-check-rdb 工具实现
 *
 * 该文件实现了 redis-check-rdb 命令行工具，用于检查 RDB
 * (Redis Database) 文件的完整性与正确性。它同时被 server.c
 * 调用以在 RDB 加载阶段对 RDB 前导（preamble）进行校验。
 */

#include "mt19937-64.h"
#include "server.h"
#include "rdb.h"

#include <stdarg.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>

/* 共享对象的创建函数（由 server.c 提供实现） */
void createSharedObjects(void);
/* RDB 加载进度回调 */
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len);
/* 标记当前是否处于 RDB 校验模式 */
int rdbCheckMode = 0;

/* 全局 rdbstate 结构：在 RDB 校验过程中跟踪当前状态与统计信息 */
struct {
    rio *rio;                       /* 当前用于读取的 RIO 对象 */
    robj *key;                      /* 当前正在读取的 key */
    int key_type;                   /* 当前 key 的类型（-1 表示无） */
    unsigned long keys;             /* 已处理的 key 数量 */
    unsigned long expires;          /* 带有过期时间的 key 数量 */
    unsigned long already_expired;  /* 已过期的 key 数量 */
    unsigned long subexpires;       /* 带有子过期时间的 key 数量 */
    int doing;                      /* 读取 RDB 时所处的状态 */
    int error_set;                  /* 错误信息是否已设置 */
    char error[1024];               /* 错误信息缓冲 */
} rdbstate;

/* 在每一步加载过程中记录当前正在执行的操作，
 * 以便在发生错误时能够记录这些上下文信息。 */
#define RDB_CHECK_DOING_START 0              /* 起始状态 */
#define RDB_CHECK_DOING_READ_TYPE 1          /* 正在读取类型 */
#define RDB_CHECK_DOING_READ_EXPIRE 2        /* 正在读取过期时间 */
#define RDB_CHECK_DOING_READ_KEY 3           /* 正在读取 key */
#define RDB_CHECK_DOING_READ_OBJECT_VALUE 4  /* 正在读取对象值 */
#define RDB_CHECK_DOING_CHECK_SUM 5          /* 正在校验校验和 */
#define RDB_CHECK_DOING_READ_LEN 6           /* 正在读取长度 */
#define RDB_CHECK_DOING_READ_AUX 7           /* 正在读取 AUX 字段 */
#define RDB_CHECK_DOING_READ_MODULE_AUX 8    /* 正在读取模块 AUX */
#define RDB_CHECK_DOING_READ_FUNCTIONS 9     /* 正在读取函数库 */

char *rdb_check_doing_string[] = {
    "start",                /* 起始状态 */
    "read-type",            /* 正在读取类型 */
    "read-expire",          /* 正在读取过期时间 */
    "read-key",             /* 正在读取 key */
    "read-object-value",    /* 正在读取对象值 */
    "check-sum",            /* 正在校验校验和 */
    "read-len",             /* 正在读取长度 */
    "read-aux",             /* 正在读取 AUX 字段 */
    "read-module-aux",      /* 正在读取模块 AUX */
    "read-functions"        /* 正在读取函数库 */
};

char *rdb_type_string[] = {
    "string",                       /* 字符串 */
    "list-linked",                  /* 链表实现的 list */
    "set-hashtable",                /* 哈希表实现的 set */
    "zset-v1",                      /* v1 版 zset */
    "hash-hashtable",               /* 哈希表实现的 hash */
    "zset-v2",                      /* v2 版 zset（带 listpack） */
    "module-pre-release",           /* 预发布版 module */
    "module-value",                 /* module 值 */
    "",                             /* 保留项 */
    "hash-zipmap",                  /* zipmap 实现的 hash */
    "list-ziplist",                 /* ziplist 实现的 list */
    "set-intset",                   /* intset 实现的 set */
    "zset-ziplist",                 /* ziplist 实现的 zset */
    "hash-ziplist",                 /* ziplist 实现的 hash */
    "quicklist",                    /* quicklist 实现的 list */
    "stream",                       /* stream */
    "hash-listpack",                /* listpack 实现的 hash */
    "zset-listpack",                /* listpack 实现的 zset */
    "quicklist-v2",                 /* v2 版 quicklist */
    "stream-v2",                    /* v2 版 stream */
    "set-listpack",                 /* listpack 实现的 set */
    "stream-v3",                    /* v3 版 stream */
    "hash-hashtable-md-pre-release",/* 预发布版哈希+元数据 */
    "hash-listpack-md-pre-release", /* 预发布版 listpack+元数据 */
    "hash-hashtable-md",            /* 哈希+元数据 */
    "hash-listpack-md",             /* listpack+元数据 */
};

/* 显示 rdbstate 中收集到的若干统计信息 */
void rdbShowGenericInfo(void) {
    printf("[info] %lu keys read\n", rdbstate.keys);
    printf("[info] %lu expires\n", rdbstate.expires);
    printf("[info] %lu already expired\n", rdbstate.already_expired);
    printf("[info] %lu subexpires\n", rdbstate.subexpires);
}

/* 当发生 RDB 错误时被调用，提供 RDB 的相关信息以及
 * 检测到错误时的读取偏移量。 */
void rdbCheckError(const char *fmt, ...) {
    char msg[1024];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    printf("--- RDB ERROR DETECTED ---\n");
    printf("[offset %llu] %s\n",
        (unsigned long long) (rdbstate.rio ?
            rdbstate.rio->processed_bytes : 0), msg);
    printf("[additional info] While doing: %s\n",
        rdb_check_doing_string[rdbstate.doing]);
    if (rdbstate.key)
        printf("[additional info] Reading key '%s'\n",
            (char*)rdbstate.key->ptr);
    if (rdbstate.key_type != -1)
        printf("[additional info] Reading type %d (%s)\n",
            rdbstate.key_type,
            ((unsigned)rdbstate.key_type <
             sizeof(rdb_type_string)/sizeof(char*)) ?
                rdb_type_string[rdbstate.key_type] : "unknown");
    rdbShowGenericInfo();
}

/* 在 RDB 校验过程中打印信息性日志。 */
void rdbCheckInfo(const char *fmt, ...) {
    char msg[1024];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    printf("[offset %llu] %s\n",
        (unsigned long long) (rdbstate.rio ?
            rdbstate.rio->processed_bytes : 0), msg);
}

/* 在 rdb.c 内部使用，用于记录 RDB 加载逻辑中的特定错误。 */
void rdbCheckSetError(const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(rdbstate.error, sizeof(rdbstate.error), fmt, ap);
    va_end(ap);
    rdbstate.error_set = 1;
}

/* 在 RDB 校验过程中为内存越界等异常信号设置专门的信号处理函数，
 * 以便在因内容损坏而崩溃时能够记录 RDB 中出错的位置。 */
void rdbCheckHandleCrash(int sig, siginfo_t *info, void *secret) {
    UNUSED(sig);
    UNUSED(info);
    UNUSED(secret);

    rdbCheckError("Server crash checking the specified RDB file!");
    exit(1);
}

/* 注册 RDB 校验期间的崩溃信号处理函数。 */
void rdbCheckSetupSignals(void) {
    struct sigaction act;

    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = rdbCheckHandleCrash;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
    sigaction(SIGABRT, &act, NULL);
}

/* 校验指定的 RDB 文件。若 RDB 看起来正常则返回 0，否则返回 1。
 * 当 'fp' 为 NULL 时，文件由 'rdbfilename' 指定；
 * 否则校验已经打开的 'fp' 文件。 */
int redis_check_rdb(char *rdbfilename, FILE *fp) {
    uint64_t dbid;
    int selected_dbid = -1;
    int type, rdbver;
    char buf[1024];
    long long expiretime, now = mstime();
    static rio rdb; /* Pointed by global struct riostate. */
    struct stat sb;

    int closefile = (fp == NULL);
    if (fp == NULL && (fp = fopen(rdbfilename,"r")) == NULL) return 1;

    if (fstat(fileno(fp), &sb) == -1)
        sb.st_size = 0;

    startLoadingFile(sb.st_size, rdbfilename, RDBFLAGS_NONE);
    rioInitWithFile(&rdb,fp);
    rdbstate.rio = &rdb;
    rdb.update_cksum = rdbLoadProgressCallback;
    if (rioRead(&rdb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        rdbCheckError("Wrong signature trying to load DB from file");
        goto err;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > RDB_VERSION) {
        rdbCheckError("Can't handle RDB format version %d",rdbver);
        goto err;
    }

    expiretime = -1;
    while(1) {
        robj *key, *val;

        /* 读取类型。 */
        rdbstate.doing = RDB_CHECK_DOING_READ_TYPE;
        if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;

        /* 处理特殊类型。 */
        if (type == RDB_OPCODE_EXPIRETIME) {
            rdbstate.doing = RDB_CHECK_DOING_READ_EXPIRE;
            /* EXPIRETIME：加载与下一个要加载的 key 相关联的过期时间。
             * 注意：加载过期时间之后还需要加载实际的类型，然后继续。 */
            expiretime = rdbLoadTime(&rdb);
            expiretime *= 1000;
            if (rioGetReadError(&rdb)) goto eoferr;
            continue; /* 读取下一个操作码。 */
        } else if (type == RDB_OPCODE_EXPIRETIME_MS) {
            /* EXPIRETIME_MS：自 RDB v3 引入的毫秒级过期时间。
             * 与 EXPIRETIME 类似但精度更高。 */
            rdbstate.doing = RDB_CHECK_DOING_READ_EXPIRE;
            expiretime = rdbLoadMillisecondTime(&rdb, rdbver);
            if (rioGetReadError(&rdb)) goto eoferr;
            continue; /* 读取下一个操作码。 */
        } else if (type == RDB_OPCODE_FREQ) {
            /* FREQ：LFU 淘汰策略中的访问频率。 */
            uint8_t byte;
            if (rioRead(&rdb,&byte,1) == 0) goto eoferr;
            continue; /* 读取下一个操作码。 */
        } else if (type == RDB_OPCODE_IDLE) {
            /* IDLE：LRU 淘汰策略中的空闲时间。 */
            if (rdbLoadLen(&rdb,NULL) == RDB_LENERR) goto eoferr;
            continue; /* 读取下一个操作码。 */
        } else if (type == RDB_OPCODE_EOF) {
            /* EOF：文件结束，退出主循环。 */
            break;
        } else if (type == RDB_OPCODE_SELECTDB) {
            /* SELECTDB：选择指定的数据库。 */
            rdbstate.doing = RDB_CHECK_DOING_READ_LEN;
            if ((dbid = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            rdbCheckInfo("Selecting DB ID %llu", (unsigned long long)dbid);
            selected_dbid = dbid;
            continue; /* 再次读取类型。 */
        } else if (type == RDB_OPCODE_RESIZEDB) {
            /* RESIZEDB：提示当前所选数据库中 key 的大小，
             * 以避免无谓的 rehash。 */
            uint64_t db_size, expires_size;
            rdbstate.doing = RDB_CHECK_DOING_READ_LEN;
            if ((db_size = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((expires_size = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            continue; /* 再次读取类型。 */
        } else if (type == RDB_OPCODE_SLOT_INFO) {
            uint64_t slot_id, slot_size, expires_slot_size;
            if ((slot_id = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((slot_size = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((expires_slot_size = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            continue; /* 再次读取类型。 */
        } else if (type == RDB_OPCODE_AUX) {
            /* AUX：通用的字符串-字符串字段。用于以向后兼容的方式
             * 向 RDB 中添加状态信息。RDB 加载实现必须能够跳过
             * 其无法识别的 AUX 字段。
             *
             * 一个 AUX 字段由两个字符串组成：key 和 value。 */
            robj *auxkey, *auxval;
            rdbstate.doing = RDB_CHECK_DOING_READ_AUX;
            if ((auxkey = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;
            if ((auxval = rdbLoadStringObject(&rdb)) == NULL) {
                decrRefCount(auxkey);
                goto eoferr;
            }

            rdbCheckInfo("AUX FIELD %s = '%s'",
                (char*)auxkey->ptr, (char*)auxval->ptr);
            decrRefCount(auxkey);
            decrRefCount(auxval);
            continue; /* 再次读取类型。 */
        } else if (type == RDB_OPCODE_MODULE_AUX) {
            /* AUX：模块的辅助数据。 */
            uint64_t moduleid, when_opcode, when;
            rdbstate.doing = RDB_CHECK_DOING_READ_MODULE_AUX;
            if ((moduleid = rdbLoadLen(&rdb,NULL)) == RDB_LENERR) goto eoferr;
            if ((when_opcode = rdbLoadLen(&rdb,NULL)) == RDB_LENERR) goto eoferr;
            if ((when = rdbLoadLen(&rdb,NULL)) == RDB_LENERR) goto eoferr;
            if (when_opcode != RDB_MODULE_OPCODE_UINT) {
                rdbCheckError("bad when_opcode");
                goto err;
            }

            char name[10];
            moduleTypeNameByID(name,moduleid);
            rdbCheckInfo("MODULE AUX for: %s", name);

            robj *o = rdbLoadCheckModuleValue(&rdb,name);
            decrRefCount(o);
            continue; /* 再次读取类型。 */
        } else if (type == RDB_OPCODE_FUNCTION_PRE_GA) {
            rdbCheckError("Pre-release function format not supported %d",rdbver);
            goto err;
        } else if (type == RDB_OPCODE_FUNCTION2) {
            sds err = NULL;
            rdbstate.doing = RDB_CHECK_DOING_READ_FUNCTIONS;
            if (rdbFunctionLoad(&rdb, rdbver, NULL, 0, &err) != C_OK) {
                rdbCheckError("Failed loading library, %s", err);
                sdsfree(err);
                goto err;
            }
            continue;
        } else {
            if (!rdbIsObjectType(type)) {
                rdbCheckError("Invalid object type: %d", type);
                goto err;
            }
            rdbstate.key_type = type;
        }

        /* 读取 key */
        rdbstate.doing = RDB_CHECK_DOING_READ_KEY;
        if ((key = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;
        rdbstate.key = key;
        rdbstate.keys++;
        /* 读取 value */
        rdbstate.doing = RDB_CHECK_DOING_READ_OBJECT_VALUE;
        if ((val = rdbLoadObject(type,&rdb,key->ptr,selected_dbid,NULL)) == NULL)
            goto eoferr;
        /* 检查该 key 是否已经过期。 */
        if (expiretime != -1 && expiretime < now)
            rdbstate.already_expired++;
        if (expiretime != -1) rdbstate.expires++;
        /* 如果是带有 HFE（字段级过期）的 hash，则需要单独计数。 */
        if ((val->type == OBJ_HASH) && (hashTypeGetMinExpire(val, 1) != EB_EXPIRE_TIME_INVALID))
            rdbstate.subexpires++;

        rdbstate.key = NULL;
        decrRefCount(key);
        decrRefCount(val);
        rdbstate.key_type = -1;
        expiretime = -1;
    }
    /* 当 RDB 版本 >= 5 时校验校验和 */
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = rdb.cksum;

        rdbstate.doing = RDB_CHECK_DOING_CHECK_SUM;
        if (rioRead(&rdb,&cksum,8) == 0) goto eoferr;
        memrev64ifbe(&cksum);
        if (cksum == 0) {
            rdbCheckInfo("RDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            rdbCheckError("RDB CRC error");
            goto err;
        } else {
            rdbCheckInfo("Checksum OK");
        }
    }

    if (closefile) fclose(fp);
    stopLoading(1);
    return 0;

eoferr: /* 在此处以致命退出处理意外的 EOF */
    if (rdbstate.error_set) {
        rdbCheckError(rdbstate.error);
    } else {
        rdbCheckError("Unexpected EOF reading RDB file");
    }
err:
    if (closefile) fclose(fp);
    stopLoading(0);
    return 1;
}

/* RDB check main：当 Redis 通过 redis-check-rdb 别名执行时由 server.c
 * 调用，或在 RDB 加载出错时被调用。
 *
 * 该函数有两种工作方式：可以以 argc/argv 作为独立可执行程序被调用，
 * 也可以在已经打开了待校验文件时通过非空的 'fp' 参数被调用。
 * 后一种情况发生在需要校验 AOF 文件内嵌的 RDB 前导时。
 *
 * 当以 fp = NULL 调用时，函数不会返回，而是根据校验结果
 * （RDB 正常或损坏）以对应的退出码退出。
 * 否则，当以非空的 fp 调用时，函数根据成功或失败返回 C_OK 或 C_ERR。 */
int redis_check_rdb_main(int argc, char **argv, FILE *fp) {
    struct timeval tv;

    if (argc != 2 && fp == NULL) {
        fprintf(stderr, "Usage: %s <rdb-file-name>\n", argv[0]);
        exit(1);
    } else if (!strcmp(argv[1],"-v") || !strcmp(argv[1], "--version")) {
        sds version = getVersion();
        printf("redis-check-rdb %s\n", version);
        sdsfree(version);
        exit(0);
    }

    gettimeofday(&tv, NULL);
    init_genrand64(((long long) tv.tv_sec * 1000000 + tv.tv_usec) ^ getpid());

    /* 为了调用加载函数，需要创建共享的整数对象。
     * 不过由于该函数可能被已经初始化的 Redis 实例调用，
     * 因此需要先判断是否确实需要创建。 */
    if (shared.integers[0] == NULL)
        createSharedObjects();
    server.loading_process_events_interval_bytes = 0;
    server.sanitize_dump_payload = SANITIZE_DUMP_YES;
    rdbCheckMode = 1;
    rdbCheckInfo("Checking RDB file %s", argv[1]);
    rdbCheckSetupSignals();
    int retval = redis_check_rdb(argv[1],fp);
    if (retval == 0) {
        rdbCheckInfo("\\o/ RDB looks OK! \\o/");
        rdbShowGenericInfo();
    }
    if (fp) return (retval == 0) ? C_OK : C_ERR;
    exit(retval);
}
