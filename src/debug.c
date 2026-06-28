/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

/*
 * debug.c - Redis 调试与崩溃报告模块
 *
 * 本文件实现了 Redis 的调试子系统，主要包括以下功能：
 * 1. DEBUG 命令处理（debugCommand）
 *    - 提供 segfault、panic、reload、digest 等调试子命令
 * 2. 数据集摘要计算（digest）
 *    - 使用 SHA1 + XOR 运算为数据库内容生成唯一摘要
 * 3. 断言与崩溃处理
 *    - _serverAssert / _serverPanic 等断言失败处理函数
 *    - 信号处理器（SIGSEGV、SIGBUS 等）
 * 4. 崩溃报告生成
 *    - 记录堆栈跟踪、寄存器状态、服务器信息、客户端信息等
 * 5. 软件看门狗（watchdog）
 *    - 通过 SIGALRM 定时输出堆栈跟踪
 * 6. 内存测试
 *    - 在崩溃时进行快速内存检测
 */

#include "server.h"
#include "util.h"
#include "sha1.h"   /* SHA1 用于 DEBUG DIGEST 摘要计算 */
#include "crc64.h"
#include "bio.h"
#include "quicklist.h"
#include "fpconv_dtoa.h"
#include "cluster.h"
#include "threads_mngr.h"
#include "script.h"

#include <arpa/inet.h>
#include <signal.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#ifdef HAVE_BACKTRACE
#include <execinfo.h>     /* backtrace() 和 backtrace_symbols_fd() */
#ifndef __OpenBSD__
#include <ucontext.h>     /* 获取信号处理器中的寄存器上下文 */
#else
typedef ucontext_t sigcontext_t;
#endif
#endif /* HAVE_BACKTRACE */

#ifdef __CYGWIN__
#ifndef SA_ONSTACK
#define SA_ONSTACK 0x08000000
#endif
#endif

#if defined(__APPLE__) && defined(__arm64__)
#include <mach/mach.h>    /* macOS ARM64 上用于获取线程状态 */
#endif

/* =============================== 全局变量 =============================== */
static int bug_report_start = 0;
/* 标志位：bug 报告头是否已输出 */
static pthread_mutex_t bug_report_start_mutex = PTHREAD_MUTEX_INITIALIZER;
/* 互斥锁：防止两个线程同时崩溃时并发写入报告 */
static pthread_mutex_t signal_handler_lock;
static pthread_mutexattr_t signal_handler_lock_attr;
static volatile int signal_handler_lock_initialized = 0;
/* 前向声明 */
int bugReportStart(void);
void printCrashReport(void);
void bugReportEnd(int killViaSignal, int sig);
void logStackTrace(void *eip, int uplevel, int current_thread);
void sigalrmSignalHandler(int sig, siginfo_t *info, void *secret);

/* ================================ 调试功能 ================================ */

/*
 * 计算长度为 len 的数据 ptr 的 SHA1 值，
 * 然后将结果与 digest 进行 XOR 运算。
 *
 * 由于 XOR 满足交换律，此操作可用于聚合无序元素的摘要，
 * 因此 digest(a,b,c,d) 与 digest(b,a,c,d) 的结果相同。
 */
void xorDigest(unsigned char *digest, const void *ptr, size_t len) {
    SHA1_CTX ctx;
    unsigned char hash[20];
    int j;

    SHA1Init(&ctx);
    SHA1Update(&ctx,ptr,len);
    SHA1Final(hash,&ctx);

    for (j = 0; j < 20; j++)
        digest[j] ^= hash[j];
}

/* xorStringObjectDigest - 对字符串对象计算 XOR 摘要 */
void xorStringObjectDigest(unsigned char *digest, robj *o) {
    o = getDecodedObject(o);
    xorDigest(digest,o->ptr,sdslen(o->ptr));
    decrRefCount(o);
}

/*
 * 与 xorDigest 不同，此函数不仅计算 SHA1 并与 digest 进行 XOR，
 * 还会对 XOR 后的 digest 本身再次计算 SHA1，并用新值替换旧值。
 *
 * 最终公式为：
 *   digest = SHA1(digest xor SHA1(data))
 *
 * 此函数用于需要保持顺序的场景，
 * 因此 digest(a,b,c,d) 与 digest(b,c,d,a) 的结果不同。
 *
 * 注意：mixDigest("foo") 后再 mixDigest("bar")
 * 与 mixDigest("fo") 后再 mixDigest("obar") 的结果不同。
 */
void mixDigest(unsigned char *digest, const void *ptr, size_t len) {
    SHA1_CTX ctx;

    xorDigest(digest,ptr,len);
    SHA1Init(&ctx);
    SHA1Update(&ctx,digest,20);
    SHA1Final(digest,&ctx);
}

/* mixStringObjectDigest - 对字符串对象计算有序混合摘要 */
void mixStringObjectDigest(unsigned char *digest, robj *o) {
    o = getDecodedObject(o);
    mixDigest(digest,o->ptr,sdslen(o->ptr));
    decrRefCount(o);
}

/*
 * 计算存储在对象 o 中的数据结构的摘要。
 * 此函数是 DEBUG DIGEST 命令的核心：计算整个数据集的摘要时，
 * 对每个键值对计算摘要，然后将所有摘要通过 XOR 合并。
 *
 * 注意：此函数不会重置传入的 digest 初始值，
 * 而是将对象的摘要混合到已有的摘要中。
 */
void xorObjectDigest(redisDb *db, robj *keyobj, unsigned char *digest, robj *o) {
    uint32_t aux = htonl(o->type);
    mixDigest(digest,&aux,sizeof(aux));
    long long expiretime = getExpire(db,keyobj);
    char buf[128];

    /* 根据对象类型，对键和关联的值计算摘要 */
    if (o->type == OBJ_STRING) {
        mixStringObjectDigest(digest,o);
    } else if (o->type == OBJ_LIST) {
        listTypeIterator *li = listTypeInitIterator(o,0,LIST_TAIL);
        listTypeEntry entry;
        while(listTypeNext(li,&entry)) {
            robj *eleobj = listTypeGet(&entry);
            mixStringObjectDigest(digest,eleobj);
            decrRefCount(eleobj);
        }
        listTypeReleaseIterator(li);
    } else if (o->type == OBJ_SET) {
        setTypeIterator *si = setTypeInitIterator(o);
        sds sdsele;
        while((sdsele = setTypeNextObject(si)) != NULL) {
            xorDigest(digest,sdsele,sdslen(sdsele));
            sdsfree(sdsele);
        }
        setTypeReleaseIterator(si);
    } else if (o->type == OBJ_ZSET) {
        unsigned char eledigest[20];

        if (o->encoding == OBJ_ENCODING_LISTPACK) {
            unsigned char *zl = o->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vll;
            double score;

            eptr = lpSeek(zl,0);
            serverAssert(eptr != NULL);
            sptr = lpNext(zl,eptr);
            serverAssert(sptr != NULL);

            while (eptr != NULL) {
                vstr = lpGetValue(eptr,&vlen,&vll);
                score = zzlGetScore(sptr);

                memset(eledigest,0,20);
                if (vstr != NULL) {
                    mixDigest(eledigest,vstr,vlen);
                } else {
                    ll2string(buf,sizeof(buf),vll);
                    mixDigest(eledigest,buf,strlen(buf));
                }
                const int len = fpconv_dtoa(score, buf);
                buf[len] = '\0';
                mixDigest(eledigest,buf,strlen(buf));
                xorDigest(digest,eledigest,20);
                zzlNext(zl,&eptr,&sptr);
            }
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            dictIterator *di = dictGetIterator(zs->dict);
            dictEntry *de;

            while((de = dictNext(di)) != NULL) {
                sds sdsele = dictGetKey(de);
                double *score = dictGetVal(de);
                const int len = fpconv_dtoa(*score, buf);
                buf[len] = '\0';
                memset(eledigest,0,20);
                mixDigest(eledigest,sdsele,sdslen(sdsele));
                mixDigest(eledigest,buf,strlen(buf));
                xorDigest(digest,eledigest,20);
            }
            dictReleaseIterator(di);
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        hashTypeIterator *hi = hashTypeInitIterator(o);
        while (hashTypeNext(hi, 0) != C_ERR) {
            unsigned char eledigest[20];
            sds sdsele;

            /* 计算字段名的摘要 */
            memset(eledigest,0,20);
            sdsele = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            mixDigest(eledigest,sdsele,sdslen(sdsele));
            sdsfree(sdsele);
            /* 计算字段值的摘要 */
            sdsele = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            mixDigest(eledigest,sdsele,sdslen(sdsele));
            sdsfree(sdsele);
            /* 哈希字段过期时间（HFE） */
            if (hi->expire_time != EB_EXPIRE_TIME_INVALID)
                xorDigest(eledigest,"!!hexpire!!",11);
            xorDigest(digest,eledigest,20);
        }
        hashTypeReleaseIterator(hi);
    } else if (o->type == OBJ_STREAM) {
        streamIterator si;
        streamIteratorStart(&si,o->ptr,NULL,NULL,0);
        streamID id;
        int64_t numfields;

        while(streamIteratorGetID(&si,&id,&numfields)) {
            sds itemid = sdscatfmt(sdsempty(),"%U.%U",id.ms,id.seq);
            mixDigest(digest,itemid,sdslen(itemid));
            sdsfree(itemid);

            while(numfields--) {
                unsigned char *field, *value;
                int64_t field_len, value_len;
                streamIteratorGetField(&si,&field,&value,
                                           &field_len,&value_len);
                mixDigest(digest,field,field_len);
                mixDigest(digest,value,value_len);
            }
        }
        streamIteratorStop(&si);
    } else if (o->type == OBJ_MODULE) {
        RedisModuleDigest md = {{0},{0},keyobj,db->id};
        moduleValue *mv = o->ptr;
        moduleType *mt = mv->type;
        moduleInitDigestContext(md);
        if (mt->digest) {
            mt->digest(&md,mv->value);
            xorDigest(digest,md.x,sizeof(md.x));
        }
    } else {
        serverPanic("Unknown object type");
    }
    /* 如果键设置了过期时间，将其加入摘要混合 */
    if (expiretime != -1) xorDigest(digest,"!!expire!!",10);
}

/*
 * 计算整个数据集的摘要。
 *
 * 由于键、集合元素、哈希元素是无序的，我们使用一个技巧：
 * 每个聚合摘要都是其元素摘要的 XOR 结果，
 * 这样顺序就不会影响结果。
 * 对于列表（list），我们使用反馈机制，将输出摘要作为输入，
 * 以确保不同顺序的列表会产生不同的摘要。
 */
void computeDatasetDigest(unsigned char *final) {
    unsigned char digest[20];
    dictEntry *de;
    int j;
    uint32_t aux;

    memset(final,0,20); /* 以全零值作为初始摘要 */

    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        if (kvstoreSize(db->keys) == 0)
            continue;
        kvstoreIterator *kvs_it = kvstoreIteratorInit(db->keys);

        /* 将 DB ID 加入摘要，使得相同数据集在不同 DB 中产生不同摘要 */
        aux = htonl(j);
        mixDigest(final,&aux,sizeof(aux));

        /* 遍历当前 DB 的所有键值对 */
        while((de = kvstoreIteratorNext(kvs_it)) != NULL) {
            sds key;
            robj *keyobj, *o;

            memset(digest,0,20); /* 当前键值对的摘要 */
            key = dictGetKey(de);
            keyobj = createStringObject(key,sdslen(key));

            mixDigest(digest,key,sdslen(key));

            o = dictGetVal(de);
            xorObjectDigest(db,keyobj,digest,o);

            /* 将当前键值对的摘要 XOR 到最终摘要中 */
            xorDigest(final,digest,20);
            decrRefCount(keyobj);
        }
        kvstoreIteratorRelease(kvs_it);
    }
}

#ifdef USE_JEMALLOC
/*
 * mallctl_int - 通过 je_mallctl 获取或设置 jemalloc 整型配置项。
 *
 * 首先尝试 int64 大小，如果失败则依次尝试更小的类型
 * （int32、bool），以兼容不同配置项的数据类型。
 */
void mallctl_int(client *c, robj **argv, int argc) {
    int ret;
    /* 从最大的 int64 开始尝试，失败则尝试更小的类型 */
    int64_t old = 0, val;
    if (argc > 1) {
        long long ll;
        if (getLongLongFromObjectOrReply(c, argv[1], &ll, NULL) != C_OK)
            return;
        val = ll;
    }
    size_t sz = sizeof(old);
    while (sz > 0) {
        size_t zz = sz;
        if ((ret=je_mallctl(argv[0]->ptr, &old, &zz, argc > 1? &val: NULL, argc > 1?sz: 0))) {
            if (ret == EPERM && argc > 1) {
                /* 如果此选项是只写的，尝试仅写入 */
                if (!(ret=je_mallctl(argv[0]->ptr, NULL, 0, &val, sz))) {
                    addReply(c, shared.ok);
                    return;
                }
            }
            if (ret==EINVAL) {
                /* 大小可能不正确，尝试更小的类型 */
                sz /= 2;
#if BYTE_ORDER == BIG_ENDIAN
                val <<= 8*sz;
#endif
                continue;
            }
            addReplyErrorFormat(c,"%s", strerror(ret));
            return;
        } else {
#if BYTE_ORDER == BIG_ENDIAN
            old >>= 64 - 8*sz;
#endif
            addReplyLongLong(c, old);
            return;
        }
    }
    addReplyErrorFormat(c,"%s", strerror(EINVAL));
}

/*
 * mallctl_string - 通过 je_mallctl 获取或设置 jemalloc 字符串配置项。
 * 对于字符串类型，需要先读取旧值再进行覆盖。
 */
void mallctl_string(client *c, robj **argv, int argc) {
    int rret, wret;
    char *old;
    size_t sz = sizeof(old);
    /* 对于字符串类型，需要先获取旧值，再进行覆盖写入 */
    if ((rret=je_mallctl(argv[0]->ptr, &old, &sz, NULL, 0))) {
        /* 除非此选项是只写的，否则返回错误 */
        if (!(rret == EPERM && argc > 1)) {
            addReplyErrorFormat(c,"%s", strerror(rret));
            return;
        }
    }
    if(argc > 1) {
        char *val = argv[1]->ptr;
        char **valref = &val;
        if ((!strcmp(val,"VOID")))
            valref = NULL, sz = 0;
        wret = je_mallctl(argv[0]->ptr, NULL, 0, valref, sz);
    }
    if (!rret)
        addReplyBulkCString(c, old);
    else if (wret)
        addReplyErrorFormat(c,"%s", strerror(wret));
    else
        addReply(c, shared.ok);
}
#endif

/*
 * debugCommand - 处理 DEBUG 命令的入口函数。
 *
 * DEBUG 是一个多功能调试命令，包含众多子命令，主要用于
 * 开发和测试环境。以下为常用子命令：
 *
 * - SEGFAULT / PANIC: 触发崩溃（用于测试崩溃报告）
 * - RELOAD: 重新加载 RDB 文件
 * - LOADAOF: 重新加载 AOF 文件
 * - DIGEST: 计算当前数据集的摘要
 * - OBJECT: 显示键的底层对象信息
 * - POPULATE: 批量创建测试数据
 * - SLEEP: 让服务器休眠指定时间
 * - PROTOCOL: 测试各种 RESP 协议回复类型
 * - RESTART / CRASH-AND-RECOVER: 重启服务器
 * - OOM: 模拟内存耗尽
 */
void debugCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"AOF-FLUSH-SLEEP <microsec>",
"    Server will sleep before flushing the AOF, this is used for testing.",
"ASSERT",
"    Crash by assertion failed.",
"CHANGE-REPL-ID",
"    Change the replication IDs of the instance.",
"    Dangerous: should be used only for testing the replication subsystem.",
"CONFIG-REWRITE-FORCE-ALL",
"    Like CONFIG REWRITE but writes all configuration options, including",
"    keywords not listed in original configuration file or default values.",
"CRASH-AND-RECOVER [<milliseconds>]",
"    Hard crash and restart after a <milliseconds> delay (default 0).",
"DIGEST",
"    Output a hex signature representing the current DB content.",
"DIGEST-VALUE <key> [<key> ...]",
"    Output a hex signature of the values of all the specified keys.",
"ERROR <string>",
"    Return a Redis protocol error with <string> as message. Useful for clients",
"    unit tests to simulate Redis errors.",
"LEAK <string>",
"    Create a memory leak of the input string.",
"LOG <message>",
"    Write <message> to the server log.",
"HTSTATS <dbid> [full]",
"    Return hash table statistics of the specified Redis database.",
"HTSTATS-KEY <key> [full]",
"    Like HTSTATS but for the hash table stored at <key>'s value.",
"LOADAOF",
"    Flush the AOF buffers on disk and reload the AOF in memory.",
"REPLICATE <string>",
"    Replicates the provided string to replicas, allowing data divergence.",
#ifdef USE_JEMALLOC
"MALLCTL <key> [<val>]",
"    Get or set a malloc tuning integer.",
"MALLCTL-STR <key> [<val>]",
"    Get or set a malloc tuning string.",
#endif
"OBJECT <key>",
"    Show low level info about `key` and associated value.",
"DROP-CLUSTER-PACKET-FILTER <packet-type>",
"    Drop all packets that match the filtered type. Set to -1 allow all packets.",
"OOM",
"    Crash the server simulating an out-of-memory error.",
"PANIC",
"    Crash the server simulating a panic.",
"POPULATE <count> [<prefix>] [<size>]",
"    Create <count> string keys named key:<num>. If <prefix> is specified then",
"    it is used instead of the 'key' prefix. These are not propagated to",
"    replicas. Cluster slots are not respected so keys not belonging to the",
"    current node can be created in cluster mode.",
"PROTOCOL <type>",
"    Reply with a test value of the specified type. <type> can be: string,",
"    integer, double, bignum, null, array, set, map, attrib, push, verbatim,",
"    true, false.",
"RELOAD [option ...]",
"    Save the RDB on disk and reload it back to memory. Valid <option> values:",
"    * MERGE: conflicting keys will be loaded from RDB.",
"    * NOFLUSH: the existing database will not be removed before load, but",
"      conflicting keys will generate an exception and kill the server.",
"    * NOSAVE: the database will be loaded from an existing RDB file.",
"    Examples:",
"    * DEBUG RELOAD: verify that the server is able to persist, flush and reload",
"      the database.",
"    * DEBUG RELOAD NOSAVE: replace the current database with the contents of an",
"      existing RDB file.",
"    * DEBUG RELOAD NOSAVE NOFLUSH MERGE: add the contents of an existing RDB",
"      file to the database.",
"RESTART [<milliseconds>]",
"    Graceful restart: save config, db, restart after a <milliseconds> delay (default 0).",
"SDSLEN <key>",
"    Show low level SDS string info representing `key` and value.",
"SEGFAULT",
"    Crash the server with sigsegv.",
"SET-ACTIVE-EXPIRE <0|1>",
"    Setting it to 0 disables expiring keys (and hash-fields) in background ",
"    when they are not accessed (otherwise the Redis behavior). Setting it",
"    to 1 reenables back the default.",
"QUICKLIST-PACKED-THRESHOLD <size>",
"    Sets the threshold for elements to be inserted as plain vs packed nodes",
"    Default value is 1GB, allows values up to 4GB. Setting to 0 restores to default.",
"SET-SKIP-CHECKSUM-VALIDATION <0|1>",
"    Enables or disables checksum checks for RDB files and RESTORE's payload.",
"SLEEP <seconds>",
"    Stop the server for <seconds>. Decimals allowed.",
"STRINGMATCH-TEST",
"    Run a fuzz tester against the stringmatchlen() function.",
"STRUCTSIZE",
"    Return the size of different Redis core C structures.",
"LISTPACK <key>",
"    Show low level info about the listpack encoding of <key>.",
"QUICKLIST <key> [<0|1>]",
"    Show low level info about the quicklist encoding of <key>.",
"    The optional argument (0 by default) sets the level of detail",
"CLIENT-EVICTION",
"    Show low level client eviction pools info (maxmemory-clients).",
"PAUSE-CRON <0|1>",
"    Stop periodic cron job processing.",
"REPLYBUFFER PEAK-RESET-TIME <NEVER||RESET|time>",
"    Sets the time (in milliseconds) to wait between client reply buffer peak resets.",
"    In case NEVER is provided the last observed peak will never be reset",
"    In case RESET is provided the peak reset time will be restored to the default value",
"REPLYBUFFER RESIZING <0|1>",
"    Enable or disable the reply buffer resize cron job",
"DICT-RESIZING <0|1>",
"    Enable or disable the main dict and expire dict resizing.",
"SCRIPT <LIST|<sha>>",
"    Output SHA and content of all scripts or of a specific script with its SHA.",
NULL
        };
        addExtendedReplyHelp(c, help, clusterDebugCommandExtendedHelp());
    } else if (!strcasecmp(c->argv[1]->ptr,"segfault")) {
        /* 向随机地址写入（如 "*((char*)-1) = 'x'"）会触发编译器警告。
         * 作为替代方案，我们映射一个只读内存区域，
         * 然后尝试写入该区域来触发段错误（SIGSEGV）。 */
        char* p = mmap(NULL, 4096, PROT_READ, MAP_PRIVATE | MAP_ANON, -1, 0);
        *p = 'x';
    } else if (!strcasecmp(c->argv[1]->ptr,"panic")) {
        serverPanic("DEBUG PANIC called at Unix time %lld", (long long)time(NULL));
    } else if (!strcasecmp(c->argv[1]->ptr,"restart") ||
               !strcasecmp(c->argv[1]->ptr,"crash-and-recover"))
    {
        long long delay = 0;
        if (c->argc >= 3) {
            if (getLongLongFromObjectOrReply(c, c->argv[2], &delay, NULL)
                != C_OK) return;
            if (delay < 0) delay = 0;
        }
        int flags = !strcasecmp(c->argv[1]->ptr,"restart") ?
            (RESTART_SERVER_GRACEFULLY|RESTART_SERVER_CONFIG_REWRITE) :
             RESTART_SERVER_NONE;
        restartServer(flags,delay);
        addReplyError(c,"failed to restart the server. Check server logs.");
    } else if (!strcasecmp(c->argv[1]->ptr,"oom")) {
        void *ptr = zmalloc(SIZE_MAX/2); /* 应该会触发内存耗尽 */
        zfree(ptr);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"assert")) {
        serverAssertWithInfo(c,c->argv[0],1 == 2);
    } else if (!strcasecmp(c->argv[1]->ptr,"log") && c->argc == 3) {
        serverLog(LL_WARNING, "DEBUG LOG: %s", (char*)c->argv[2]->ptr);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"leak") && c->argc == 3) {
        sdsdup(c->argv[2]->ptr);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"reload")) {
        int flush = 1, save = 1;
        int flags = RDBFLAGS_NONE;

        /* 解析修改 RELOAD 行为的附加选项 */
        for (int j = 2; j < c->argc; j++) {
            char *opt = c->argv[j]->ptr;
            if (!strcasecmp(opt,"MERGE")) {
                flags |= RDBFLAGS_ALLOW_DUP;
            } else if (!strcasecmp(opt,"NOFLUSH")) {
                flush = 0;
            } else if (!strcasecmp(opt,"NOSAVE")) {
                save = 0;
            } else {
                addReplyError(c,"DEBUG RELOAD only supports the "
                                "MERGE, NOFLUSH and NOSAVE options.");
                return;
            }
        }

        /* 默认行为是先保存 RDB 文件，然后再加载回来 */
        if (save) {
            rdbSaveInfo rsi, *rsiptr;
            rsiptr = rdbPopulateSaveInfo(&rsi);
            if (rdbSave(SLAVE_REQ_NONE,server.rdb_filename,rsiptr,RDBFLAGS_NONE) != C_OK) {
                addReplyErrorObject(c,shared.err);
                return;
            }
        }

        /* 默认行为是在加载 RDB 文件前清除当前数据集。
         * 但当 MERGE 和 NOFLUSH 选项同时使用时，
         * 可以将两个数据集合并。 */
        if (flush) emptyData(-1,EMPTYDB_NO_FLAGS,NULL);

        protectClient(c);
        int ret = rdbLoad(server.rdb_filename,NULL,flags);
        unprotectClient(c);
        if (ret != RDB_OK) {
            addReplyError(c,"Error trying to load the RDB dump, check server logs.");
            return;
        }
        serverLog(LL_NOTICE,"DB reloaded by DEBUG RELOAD");
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"loadaof")) {
        if (server.aof_state != AOF_OFF) flushAppendOnlyFile(1);
        emptyData(-1,EMPTYDB_NO_FLAGS,NULL);
        protectClient(c);
        if (server.aof_manifest) aofManifestFree(server.aof_manifest);
        aofLoadManifestFromDisk();
        aofDelHistoryFiles();
        int ret = loadAppendOnlyFiles(server.aof_manifest);
        unprotectClient(c);
        if (ret != AOF_OK && ret != AOF_EMPTY) {
            addReplyError(c, "Error trying to load the AOF files, check server logs.");
            return;
        }
        server.dirty = 0; /* 防止触发 AOF 写入或复制 */
        serverLog(LL_NOTICE,"Append Only File loaded by DEBUG LOADAOF");
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"drop-cluster-packet-filter") && c->argc == 3) {
        long packet_type;
        if (getLongFromObjectOrReply(c, c->argv[2], &packet_type, NULL) != C_OK)
            return;
        server.cluster_drop_packet_filter = packet_type;
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"object") && c->argc == 3) {
        dictEntry *de;
        robj *val;
        char *strenc;

        if ((de = dbFind(c->db, c->argv[2]->ptr)) == NULL) {
            addReplyErrorObject(c,shared.nokeyerr);
            return;
        }
        val = dictGetVal(de);
        strenc = strEncoding(val->encoding);

        char extra[138] = {0};
        if (val->encoding == OBJ_ENCODING_QUICKLIST) {
            char *nextra = extra;
            int remaining = sizeof(extra);
            quicklist *ql = val->ptr;
            /* 添加 quicklist 节点数量 */
            int used = snprintf(nextra, remaining, " ql_nodes:%lu", ql->len);
            nextra += used;
            remaining -= used;
            /* 添加 quicklist 平均填充因子 */
            double avg = (double)ql->count/ql->len;
            used = snprintf(nextra, remaining, " ql_avg_node:%.2f", avg);
            nextra += used;
            remaining -= used;
            /* 添加 quicklist 填充级别 / 最大 listpack 大小 */
            used = snprintf(nextra, remaining, " ql_listpack_max:%d", ql->fill);
            nextra += used;
            remaining -= used;
            /* 是否已压缩 */
            int compressed = ql->compress != 0;
            used = snprintf(nextra, remaining, " ql_compressed:%d", compressed);
            nextra += used;
            remaining -= used;
            /* 添加未压缩总大小 */
            unsigned long sz = 0;
            for (quicklistNode *node = ql->head; node; node = node->next) {
                sz += node->sz;
            }
            used = snprintf(nextra, remaining, " ql_uncompressed_size:%lu", sz);
            nextra += used;
            remaining -= used;
        }

        addReplyStatusFormat(c,
            "Value at:%p refcount:%d "
            "encoding:%s serializedlength:%zu "
            "lru:%d lru_seconds_idle:%llu%s",
            (void*)val, val->refcount,
            strenc, rdbSavedObjectLen(val, c->argv[2], c->db->id),
            val->lru, estimateObjectIdleTime(val)/1000, extra);
    } else if (!strcasecmp(c->argv[1]->ptr,"sdslen") && c->argc == 3) {
        dictEntry *de;
        robj *val;
        sds key;

        if ((de = dbFind(c->db, c->argv[2]->ptr)) == NULL) {
            addReplyErrorObject(c,shared.nokeyerr);
            return;
        }
        val = dictGetVal(de);
        key = dictGetKey(de);

        if (val->type != OBJ_STRING || !sdsEncodedObject(val)) {
            addReplyError(c,"Not an sds encoded string.");
        } else {
            addReplyStatusFormat(c,
                "key_sds_len:%lld, key_sds_avail:%lld, key_zmalloc: %lld, "
                "val_sds_len:%lld, val_sds_avail:%lld, val_zmalloc: %lld",
                (long long) sdslen(key),
                (long long) sdsavail(key),
                (long long) sdsZmallocSize(key),
                (long long) sdslen(val->ptr),
                (long long) sdsavail(val->ptr),
                (long long) getStringObjectSdsUsedMemory(val));
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"listpack") && c->argc == 3) {
        robj *o;

        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nokeyerr))
                == NULL) return;

        if (o->encoding != OBJ_ENCODING_LISTPACK && o->encoding != OBJ_ENCODING_LISTPACK_EX) {
            addReplyError(c,"Not a listpack encoded object.");
        } else {
            if (o->encoding == OBJ_ENCODING_LISTPACK)
                lpRepr(o->ptr);
            else if (o->encoding == OBJ_ENCODING_LISTPACK_EX)
                lpRepr(((listpackEx*)o->ptr)->lp);

            addReplyStatus(c,"Listpack structure printed on stdout");
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"quicklist") && (c->argc == 3 || c->argc == 4)) {
        robj *o;

        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nokeyerr))
            == NULL) return;

        int full = 0;
        if (c->argc == 4)
            full = atoi(c->argv[3]->ptr);
        if (o->encoding != OBJ_ENCODING_QUICKLIST) {
            addReplyError(c,"Not a quicklist encoded object.");
        } else {
            quicklistRepr(o->ptr, full);
            addReplyStatus(c,"Quicklist structure printed on stdout");
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"populate") &&
               c->argc >= 3 && c->argc <= 5) {
        long keys, j;
        robj *key, *val;
        char buf[128];

        if (getPositiveLongFromObjectOrReply(c, c->argv[2], &keys, NULL) != C_OK)
            return;

        if (server.loading || server.async_loading) {
            addReplyErrorObject(c, shared.loadingerr);
            return;
        }

        if (dbExpand(c->db, keys, 1) == C_ERR) {
            addReplyError(c, "OOM in dictTryExpand");
            return;
        }
        long valsize = 0;
        if ( c->argc == 5 && getPositiveLongFromObjectOrReply(c, c->argv[4], &valsize, NULL) != C_OK ) 
            return;

        for (j = 0; j < keys; j++) {
            snprintf(buf,sizeof(buf),"%s:%lu",
                (c->argc == 3) ? "key" : (char*)c->argv[3]->ptr, j);
            key = createStringObject(buf,strlen(buf));
            if (lookupKeyWrite(c->db,key) != NULL) {
                decrRefCount(key);
                continue;
            }
            snprintf(buf,sizeof(buf),"value:%lu",j);
            if (valsize==0)
                val = createStringObject(buf,strlen(buf));
            else {
                int buflen = strlen(buf);
                val = createStringObject(NULL,valsize);
                memcpy(val->ptr, buf, valsize<=buflen? valsize: buflen);
            }
            dbAdd(c->db,key,val);
            signalModifiedKey(c,c->db,key);
            decrRefCount(key);
        }
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"digest") && c->argc == 2) {
        /* DEBUG DIGEST（不指定键的形式，计算整个数据集的摘要） */
        unsigned char digest[20];
        sds d = sdsempty();

        computeDatasetDigest(digest);
        for (int i = 0; i < 20; i++) d = sdscatprintf(d, "%02x",digest[i]);
        addReplyStatus(c,d);
        sdsfree(d);
    } else if (!strcasecmp(c->argv[1]->ptr,"digest-value") && c->argc >= 2) {
        /* DEBUG DIGEST-VALUE key key key ... key（计算指定键的值摘要） */
        addReplyArrayLen(c,c->argc-2);
        for (int j = 2; j < c->argc; j++) {
            unsigned char digest[20];
            memset(digest,0,20); /* 以全零作为初始摘要 */

            /* 不使用 lookupKey，因为调试命令应该能操作
             * 逻辑上已过期的键 */
            dictEntry *de;
            robj *o = ((de = dbFind(c->db, c->argv[j]->ptr)) == NULL) ? NULL : dictGetVal(de);
            if (o) xorObjectDigest(c->db,c->argv[j],digest,o);

            sds d = sdsempty();
            for (int i = 0; i < 20; i++) d = sdscatprintf(d, "%02x",digest[i]);
            addReplyStatus(c,d);
            sdsfree(d);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"protocol") && c->argc == 3) {
        /* DEBUG PROTOCOL [string|integer|double|bignum|null|array|set|map|
         *                 attrib|push|verbatim|true|false] */
        char *name = c->argv[2]->ptr;
        if (!strcasecmp(name,"string")) {
            addReplyBulkCString(c,"Hello World");
        } else if (!strcasecmp(name,"integer")) {
            addReplyLongLong(c,12345);
        } else if (!strcasecmp(name,"double")) {
            addReplyDouble(c,3.141);
        } else if (!strcasecmp(name,"bignum")) {
            addReplyBigNum(c,"1234567999999999999999999999999999999",37);
        } else if (!strcasecmp(name,"null")) {
            addReplyNull(c);
        } else if (!strcasecmp(name,"array")) {
            addReplyArrayLen(c,3);
            for (int j = 0; j < 3; j++) addReplyLongLong(c,j);
        } else if (!strcasecmp(name,"set")) {
            addReplySetLen(c,3);
            for (int j = 0; j < 3; j++) addReplyLongLong(c,j);
        } else if (!strcasecmp(name,"map")) {
            addReplyMapLen(c,3);
            for (int j = 0; j < 3; j++) {
                addReplyLongLong(c,j);
                addReplyBool(c, j == 1);
            }
        } else if (!strcasecmp(name,"attrib")) {
            if (c->resp >= 3) {
                addReplyAttributeLen(c,1);
                addReplyBulkCString(c,"key-popularity");
                addReplyArrayLen(c,2);
                addReplyBulkCString(c,"key:123");
                addReplyLongLong(c,90);
            }
            /* 属性（Attribute）不是真正的回复，因此格式正确的回复
             * 应在属性之后还有普通的回复类型。 */
            addReplyBulkCString(c,"Some real reply following the attribute");
        } else if (!strcasecmp(name,"push")) {
            if (c->resp < 3) {
                addReplyError(c,"RESP2 is not supported by this command");
                return;
	    }
            uint64_t old_flags = c->flags;
            c->flags |= CLIENT_PUSHING;
            addReplyPushLen(c,2);
            addReplyBulkCString(c,"server-cpu-usage");
            addReplyLongLong(c,42);
            if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
            /* Push 回复不是同步回复，因此还需要发送一个普通回复，
             * 以便丢弃 push 回复的阻塞客户端能够消费此回复并继续执行。 */
            addReplyBulkCString(c,"Some real reply following the push reply");
        } else if (!strcasecmp(name,"true")) {
            addReplyBool(c,1);
        } else if (!strcasecmp(name,"false")) {
            addReplyBool(c,0);
        } else if (!strcasecmp(name,"verbatim")) {
            addReplyVerbatim(c,"This is a verbatim\nstring",25,"txt");
        } else {
            addReplyError(c,"Wrong protocol type name. Please use one of the following: string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false");
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"sleep") && c->argc == 3) {
        double dtime = strtod(c->argv[2]->ptr,NULL);
        long long utime = dtime*1000000;
        struct timespec tv;

        tv.tv_sec = utime / 1000000;
        tv.tv_nsec = (utime % 1000000) * 1000;
        nanosleep(&tv, NULL);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"set-active-expire") &&
               c->argc == 3)
    {
        server.active_expire_enabled = atoi(c->argv[2]->ptr);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"quicklist-packed-threshold") &&
               c->argc == 3)
    {
        int memerr;
        unsigned long long sz = memtoull((const char *)c->argv[2]->ptr, &memerr);
        if (memerr || !quicklistSetPackedThreshold(sz)) {
            addReplyError(c, "argument must be a memory value bigger than 1 and smaller than 4gb");
        } else {
            addReply(c,shared.ok);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"set-skip-checksum-validation") &&
               c->argc == 3)
    {
        server.skip_checksum_validation = atoi(c->argv[2]->ptr);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"aof-flush-sleep") &&
               c->argc == 3)
    {
        server.aof_flush_sleep = atoi(c->argv[2]->ptr);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"replicate") && c->argc >= 3) {
        replicationFeedSlaves(server.slaves, -1,
                c->argv + 2, c->argc - 2);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"error") && c->argc == 3) {
        sds errstr = sdsnewlen("-",1);

        errstr = sdscatsds(errstr,c->argv[2]->ptr);
        errstr = sdsmapchars(errstr,"\n\r","  ",2); /* 错误消息中不允许换行 */
        errstr = sdscatlen(errstr,"\r\n",2);
        addReplySds(c,errstr);
    } else if (!strcasecmp(c->argv[1]->ptr,"structsize") && c->argc == 2) {
        sds sizes = sdsempty();
        sizes = sdscatprintf(sizes,"bits:%d ",(sizeof(void*) == 8)?64:32);
        sizes = sdscatprintf(sizes,"robj:%d ",(int)sizeof(robj));
        sizes = sdscatprintf(sizes,"dictentry:%d ",(int)dictEntryMemUsage());
        sizes = sdscatprintf(sizes,"sdshdr5:%d ",(int)sizeof(struct sdshdr5));
        sizes = sdscatprintf(sizes,"sdshdr8:%d ",(int)sizeof(struct sdshdr8));
        sizes = sdscatprintf(sizes,"sdshdr16:%d ",(int)sizeof(struct sdshdr16));
        sizes = sdscatprintf(sizes,"sdshdr32:%d ",(int)sizeof(struct sdshdr32));
        sizes = sdscatprintf(sizes,"sdshdr64:%d ",(int)sizeof(struct sdshdr64));
        addReplyBulkSds(c,sizes);
    } else if (!strcasecmp(c->argv[1]->ptr,"htstats") && c->argc >= 3) {
        long dbid;
        sds stats = sdsempty();
        char buf[4096];
        int full = 0;

        if (getLongFromObjectOrReply(c, c->argv[2], &dbid, NULL) != C_OK) {
            sdsfree(stats);
            return;
        }
        if (dbid < 0 || dbid >= server.dbnum) {
            sdsfree(stats);
            addReplyError(c,"Out of range database");
            return;
        }
        if (c->argc >= 4 && !strcasecmp(c->argv[3]->ptr,"full"))
            full = 1;

        stats = sdscatprintf(stats,"[Dictionary HT]\n");
        kvstoreGetStats(server.db[dbid].keys, buf, sizeof(buf), full);
        stats = sdscat(stats,buf);

        stats = sdscatprintf(stats,"[Expires HT]\n");
        kvstoreGetStats(server.db[dbid].expires, buf, sizeof(buf), full);
        stats = sdscat(stats,buf);

        addReplyVerbatim(c,stats,sdslen(stats),"txt");
        sdsfree(stats);
    } else if (!strcasecmp(c->argv[1]->ptr,"htstats-key") && c->argc >= 3) {
        robj *o;
        dict *ht = NULL;
        int full = 0;

        if (c->argc >= 4 && !strcasecmp(c->argv[3]->ptr,"full"))
            full = 1;

        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nokeyerr))
                == NULL) return;

        /* 尝试从对象中获取哈希表引用 */
        switch (o->encoding) {
        case OBJ_ENCODING_SKIPLIST:
            {
                zset *zs = o->ptr;
                ht = zs->dict;
            }
            break;
        case OBJ_ENCODING_HT:
            ht = o->ptr;
            break;
        }

        if (ht == NULL) {
            addReplyError(c,"The value stored at the specified key is not "
                            "represented using an hash table");
        } else {
            char buf[4096];
            dictGetStats(buf,sizeof(buf),ht,full);
            addReplyVerbatim(c,buf,strlen(buf),"txt");
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"change-repl-id") && c->argc == 2) {
        serverLog(LL_NOTICE,"Changing replication IDs after receiving DEBUG change-repl-id");
        changeReplicationId();
        clearReplicationId2();
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"stringmatch-test") && c->argc == 2)
    {
        stringmatchlen_fuzz_test();
        addReplyStatus(c,"Apparently Redis did not crash: test passed");
    } else if (!strcasecmp(c->argv[1]->ptr,"set-disable-deny-scripts") && c->argc == 3)
    {
        server.script_disable_deny_script = atoi(c->argv[2]->ptr);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"config-rewrite-force-all") && c->argc == 2)
    {
        if (rewriteConfig(server.configfile, 1) == -1)
            addReplyErrorFormat(c, "CONFIG-REWRITE-FORCE-ALL failed: %s", strerror(errno));
        else
            addReply(c, shared.ok);
    } else if(!strcasecmp(c->argv[1]->ptr,"client-eviction") && c->argc == 2) {
        if (!server.client_mem_usage_buckets) {
            addReplyError(c,"maxmemory-clients is disabled.");
            return;
        }
        sds bucket_info = sdsempty();
        for (int j = 0; j < CLIENT_MEM_USAGE_BUCKETS; j++) {
            if (j == 0)
                bucket_info = sdscatprintf(bucket_info, "bucket          0");
            else
                bucket_info = sdscatprintf(bucket_info, "bucket %10zu", (size_t)1<<(j-1+CLIENT_MEM_USAGE_BUCKET_MIN_LOG));
            if (j == CLIENT_MEM_USAGE_BUCKETS-1)
                bucket_info = sdscatprintf(bucket_info, "+            : ");
            else
                bucket_info = sdscatprintf(bucket_info, " - %10zu: ", ((size_t)1<<(j+CLIENT_MEM_USAGE_BUCKET_MIN_LOG))-1);
            bucket_info = sdscatprintf(bucket_info, "tot-mem: %10zu, clients: %lu\n",
                server.client_mem_usage_buckets[j].mem_usage_sum,
                server.client_mem_usage_buckets[j].clients->len);
        }
        addReplyVerbatim(c,bucket_info,sdslen(bucket_info),"txt");
        sdsfree(bucket_info);
#ifdef USE_JEMALLOC
    } else if(!strcasecmp(c->argv[1]->ptr,"mallctl") && c->argc >= 3) {
        mallctl_int(c, c->argv+2, c->argc-2);
        return;
    } else if(!strcasecmp(c->argv[1]->ptr,"mallctl-str") && c->argc >= 3) {
        mallctl_string(c, c->argv+2, c->argc-2);
        return;
#endif
    } else if (!strcasecmp(c->argv[1]->ptr,"pause-cron") && c->argc == 3)
    {
        server.pause_cron = atoi(c->argv[2]->ptr);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"replybuffer") && c->argc == 4 ) {
        if(!strcasecmp(c->argv[2]->ptr, "peak-reset-time")) {
            if (!strcasecmp(c->argv[3]->ptr, "never")) {
                server.reply_buffer_peak_reset_time = -1;
            } else if(!strcasecmp(c->argv[3]->ptr, "reset")) {
                server.reply_buffer_peak_reset_time = REPLY_BUFFER_DEFAULT_PEAK_RESET_TIME;
            } else {
                if (getLongFromObjectOrReply(c, c->argv[3], &server.reply_buffer_peak_reset_time, NULL) != C_OK)
                    return;
            }
        } else if(!strcasecmp(c->argv[2]->ptr,"resizing")) {
            server.reply_buffer_resizing_enabled = atoi(c->argv[3]->ptr);
        } else {
            addReplySubcommandSyntaxError(c);
            return;
        }
        addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr, "dict-resizing") && c->argc == 3) {
        server.dict_resizing = atoi(c->argv[2]->ptr);
        addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"script") && c->argc == 3) {
        if (!strcasecmp(c->argv[2]->ptr,"list")) {
            dictIterator *di = dictGetIterator(evalScriptsDict());
            dictEntry *de;
            while ((de = dictNext(di)) != NULL) {
                luaScript *script = dictGetVal(de);
                sds *sha = dictGetKey(de);
                serverLog(LL_WARNING, "SCRIPT SHA: %s\n%s", (char*)sha, (char*)script->body->ptr);
            }
            dictReleaseIterator(di);
        } else if (sdslen(c->argv[2]->ptr) == 40) {
            dictEntry *de;
            if ((de = dictFind(evalScriptsDict(), c->argv[2]->ptr)) == NULL) {
                addReplyErrorObject(c, shared.noscripterr);
                return;
            }
            luaScript *script = dictGetVal(de);
            serverLog(LL_WARNING, "SCRIPT SHA: %s\n%s", (char*)c->argv[2]->ptr, (char*)script->body->ptr);
        } else {
            addReplySubcommandSyntaxError(c);
            return;
        }
        addReply(c,shared.ok);
    } else if(!handleDebugClusterCommand(c)) {
        addReplySubcommandSyntaxError(c);
        return;
    }
}

/* ============================= 崩溃处理 ================================ */

/*
 * _serverAssert - 处理服务器断言失败。
 *
 * 当 serverAssert() 宏的条件不满足时调用此函数。
 * 它会启动 bug 报告、记录断言失败信息、输出堆栈跟踪
 * 以及完整的崩溃报告，然后终止进程。
 *
 * 参数：
 *   estr: 断言表达式的字符串形式
 *   file: 触发断言的源文件名
 *   line: 触发断言的行号
 */
__attribute__ ((noinline))
void _serverAssert(const char *estr, const char *file, int line) {
    int new_report = bugReportStart();
    serverLog(LL_WARNING,"=== %sASSERTION FAILED ===", new_report ? "" : "RECURSIVE ");
    serverLog(LL_WARNING,"==> %s:%d '%s' is not true",file,line,estr);

    if (server.crashlog_enabled) {
#ifdef HAVE_BACKTRACE
        logStackTrace(NULL, 1, 0);
#endif
        /* 如果是递归断言（即在 printCrashReport 中再次触发），
         * 则不再重复输出崩溃报告。 */
        if (new_report) printCrashReport();
    }

    // 移除信号处理器，这样在 abort() 时会输出崩溃报告
    removeSigSegvHandlers();
    bugReportEnd(0, 0);
}

/* 返回允许记录的客户端命令参数数量 */
int clientArgsToLog(const client *c) {
    return server.hide_user_data_from_log ? 1 : c->argc;
}

/* 输出断言失败时的客户端上下文信息 */
void _serverAssertPrintClientInfo(const client *c) {
    int j;
    char conninfo[CONN_INFO_LEN];

    bugReportStart();
    serverLog(LL_WARNING,"=== ASSERTION FAILED CLIENT CONTEXT ===");
    serverLog(LL_WARNING,"client->flags = %llu", (unsigned long long) c->flags);
    serverLog(LL_WARNING,"client->conn = %s", connGetInfo(c->conn, conninfo, sizeof(conninfo)));
    serverLog(LL_WARNING,"client->argc = %d", c->argc);
    for (j=0; j < c->argc; j++) {
        if (j >= clientArgsToLog(c)) {
            serverLog(LL_WARNING,"client->argv[%d] = *redacted*",j);
            continue;
        }
        char buf[128];
        char *arg;

        if (c->argv[j]->type == OBJ_STRING && sdsEncodedObject(c->argv[j])) {
            arg = (char*) c->argv[j]->ptr;
        } else {
            snprintf(buf,sizeof(buf),"Object type: %u, encoding: %u",
                c->argv[j]->type, c->argv[j]->encoding);
            arg = buf;
        }
        serverLog(LL_WARNING,"client->argv[%d] = \"%s\" (refcount: %d)",
            j, arg, c->argv[j]->refcount);
    }
}

/*
 * serverLogObjectDebugInfo - 记录 Redis 对象的调试信息。
 *
 * 输出对象的类型、编码和引用计数。
 * 如果启用了 UNSAFE_CRASH_REPORT，还会输出
 * 对象的具体内容（字符串值、列表长度等）。
 */
void serverLogObjectDebugInfo(const robj *o) {
    serverLog(LL_WARNING,"Object type: %u", o->type);
    serverLog(LL_WARNING,"Object encoding: %u", o->encoding);
    serverLog(LL_WARNING,"Object refcount: %d", o->refcount);
#if UNSAFE_CRASH_REPORT
    /* 此代码目前已被禁用。o->ptr 可能不可靠：
     * 在某些情况下，ziplist 可能已被 realloc 释放，
     * 但 o->ptr 尚未更新。
     * 在另一些情况下，调用 ziplistLen 可能需要遍历
     * 列表中的所有元素（并可能再次崩溃）。
     * 虽然再次崩溃在某些场景下可以接受，
     * 但无效内存访问会干扰 valgrind，
     * 还可能导致随机内存内容"泄露"到日志文件中。 */
    if (o->type == OBJ_STRING && sdsEncodedObject(o)) {
        serverLog(LL_WARNING,"Object raw string len: %zu", sdslen(o->ptr));
        if (sdslen(o->ptr) < 4096) {
            sds repr = sdscatrepr(sdsempty(),o->ptr,sdslen(o->ptr));
            serverLog(LL_WARNING,"Object raw string content: %s", repr);
            sdsfree(repr);
        }
    } else if (o->type == OBJ_LIST) {
        serverLog(LL_WARNING,"List length: %d", (int) listTypeLength(o));
    } else if (o->type == OBJ_SET) {
        serverLog(LL_WARNING,"Set size: %d", (int) setTypeSize(o));
    } else if (o->type == OBJ_HASH) {
        serverLog(LL_WARNING,"Hash size: %d", (int) hashTypeLength(o, 0));
    } else if (o->type == OBJ_ZSET) {
        serverLog(LL_WARNING,"Sorted set size: %d", (int) zsetLength(o));
        if (o->encoding == OBJ_ENCODING_SKIPLIST)
            serverLog(LL_WARNING,"Skiplist level: %d", (int) ((const zset*)o->ptr)->zsl->level);
    } else if (o->type == OBJ_STREAM) {
        serverLog(LL_WARNING,"Stream size: %d", (int) streamLength(o));
    }
#endif
}

void _serverAssertPrintObject(const robj *o) {
    bugReportStart();
    serverLog(LL_WARNING,"=== ASSERTION FAILED OBJECT CONTEXT ===");
    serverLogObjectDebugInfo(o);
}

void _serverAssertWithInfo(const client *c, const robj *o, const char *estr, const char *file, int line) {
    if (c) _serverAssertPrintClientInfo(c);
    if (o) _serverAssertPrintObject(o);
    _serverAssert(estr,file,line);
}

/*
 * _serverPanic - 处理服务器 panic（严重错误）。
 *
 * 当 serverPanic() 宏被调用时触发此函数。
 * 记录 "Guru Meditation" 错误信息（致敬 Amiga 崩溃提示），
 * 输出堆栈跟踪和崩溃报告后终止进程。
 */
__attribute__ ((noinline))
void _serverPanic(const char *file, int line, const char *msg, ...) {
    va_list ap;
    va_start(ap,msg);
    char fmtmsg[256];
    vsnprintf(fmtmsg,sizeof(fmtmsg),msg,ap);
    va_end(ap);

    int new_report = bugReportStart();
    serverLog(LL_WARNING,"------------------------------------------------");
    serverLog(LL_WARNING,"!!! Software Failure. Press left mouse button to continue");
    serverLog(LL_WARNING,"Guru Meditation: %s #%s:%d",fmtmsg,file,line);

    if (server.crashlog_enabled) {
#ifdef HAVE_BACKTRACE
        logStackTrace(NULL, 1, 0);
#endif
        /* 如果是递归 panic（即在 printCrashReport 中再次触发），
         * 则不再重复输出崩溃报告。 */
        if (new_report) printCrashReport();
    }

    // 移除信号处理器，这样在 abort() 时会输出崩溃报告
    removeSigSegvHandlers();
    bugReportEnd(0, 0);
}

/*
 * bugReportStart - 开始输出 bug 报告。
 *
 * 首次调用时输出报告头并返回 1，
 * 后续调用返回 0（避免重复输出头部）。
 */
int bugReportStart(void) {
    pthread_mutex_lock(&bug_report_start_mutex);
    if (bug_report_start == 0) {
        serverLogRaw(LL_WARNING|LL_RAW,
        "\n\n=== REDIS BUG REPORT START: Cut & paste starting from here ===\n");
        bug_report_start = 1;
        pthread_mutex_unlock(&bug_report_start_mutex);
        return 1;
    }
    pthread_mutex_unlock(&bug_report_start_mutex);
    return 0;
}

#ifdef HAVE_BACKTRACE

/*
 * getAndSetMcontextEip - 获取当前 EIP（指令指针），
 * 并可选地将其设置为新值。
 *
 * 此函数根据不同的操作系统和 CPU 架构，
 * 从 ucontext_t 结构中读取/设置指令指针寄存器。
 *
 * 参数：
 *   uc: 信号处理器的上下文
 *   eip: 新的指令指针值，为 NULL 则仅读取
 * 返回：旧的指令指针值
 */
static void* getAndSetMcontextEip(ucontext_t *uc, void *eip) {
/* 不支持的平台：返回 NULL */
#define NOT_SUPPORTED() do {\
    UNUSED(uc);\
    UNUSED(eip);\
    return NULL;\
} while(0)
/* 获取旧值，如果 new_val 非空则设置新值，返回旧值 */
#define GET_SET_RETURN(target_var, new_val) do {\
    void *old_val = (void*)target_var; \
    if (new_val) { \
        void **temp = (void**)&target_var; \
        *temp = new_val; \
    } \
    return old_val; \
} while(0)
#if defined(__APPLE__) && !defined(MAC_OS_10_6_DETECTED)
    /* macOS < 10.6 */
    #if defined(__x86_64__)
    GET_SET_RETURN(uc->uc_mcontext->__ss.__rip, eip);
    #elif defined(__i386__)
    GET_SET_RETURN(uc->uc_mcontext->__ss.__eip, eip);
    #else
    GET_SET_RETURN(uc->uc_mcontext->__ss.__srr0, eip);
    #endif
#elif defined(__APPLE__) && defined(MAC_OS_10_6_DETECTED)
    /* macOS >= 10.6 */
    #if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    GET_SET_RETURN(uc->uc_mcontext->__ss.__rip, eip);
    #elif defined(__i386__)
    GET_SET_RETURN(uc->uc_mcontext->__ss.__eip, eip);
    #else
    /* OSX ARM64 */
    void *old_val = (void*)arm_thread_state64_get_pc(uc->uc_mcontext->__ss);
    if (eip) {
        arm_thread_state64_set_pc_fptr(uc->uc_mcontext->__ss, eip);
    }
    return old_val;
    #endif
#elif defined(__linux__)
    /* Linux */
    #if defined(__i386__) || ((defined(__X86_64__) || defined(__x86_64__)) && defined(__ILP32__))
    GET_SET_RETURN(uc->uc_mcontext.gregs[14], eip);
    #elif defined(__X86_64__) || defined(__x86_64__)
    GET_SET_RETURN(uc->uc_mcontext.gregs[16], eip);
    #elif defined(__ia64__) /* Linux IA64 */
    GET_SET_RETURN(uc->uc_mcontext.sc_ip, eip);
    #elif defined(__riscv) /* Linux RISC-V */
    GET_SET_RETURN(uc->uc_mcontext.__gregs[REG_PC], eip);
    #elif defined(__arm__) /* Linux ARM */
    GET_SET_RETURN(uc->uc_mcontext.arm_pc, eip);
    #elif defined(__aarch64__) /* Linux AArch64 */
    GET_SET_RETURN(uc->uc_mcontext.pc, eip);
    #else
    NOT_SUPPORTED();
    #endif
#elif defined(__FreeBSD__)
    /* FreeBSD */
    #if defined(__i386__)
    GET_SET_RETURN(uc->uc_mcontext.mc_eip, eip);
    #elif defined(__x86_64__)
    GET_SET_RETURN(uc->uc_mcontext.mc_rip, eip);
    #else
    NOT_SUPPORTED();
    #endif
#elif defined(__OpenBSD__)
    /* OpenBSD */
    #if defined(__i386__)
    GET_SET_RETURN(uc->sc_eip, eip);
    #elif defined(__x86_64__)
    GET_SET_RETURN(uc->sc_rip, eip);
    #else
    NOT_SUPPORTED();
    #endif
#elif defined(__NetBSD__)
    #if defined(__i386__)
    GET_SET_RETURN(uc->uc_mcontext.__gregs[_REG_EIP], eip);
    #elif defined(__x86_64__)
    GET_SET_RETURN(uc->uc_mcontext.__gregs[_REG_RIP], eip);
    #else
    NOT_SUPPORTED();
    #endif
#elif defined(__DragonFly__)
    GET_SET_RETURN(uc->uc_mcontext.mc_rip, eip);
#elif defined(__sun) && defined(__x86_64__)
    GET_SET_RETURN(uc->uc_mcontext.gregs[REG_RIP], eip);
#else
    NOT_SUPPORTED();
#endif
#undef NOT_SUPPORTED
}

/*
 * logStackContent - 输出栈内存内容。
 *
 * 从栈指针 sp 开始，向高地址方向打印 16 个栈帧的内容。
 * 用于崩溃诊断时查看栈上的数据。
 */
REDIS_NO_SANITIZE("address")
void logStackContent(void **sp) {
    if (server.hide_user_data_from_log) {
        /* 如果启用了隐藏用户数据，则跳过栈内容输出以避免泄露个人信息 */
        serverLog(LL_NOTICE,"hide-user-data-from-log is on, skip logging stack content to avoid spilling PII.");
        return;
    }
    int i;
    for (i = 15; i >= 0; i--) {
        unsigned long addr = (unsigned long) sp+i;
        unsigned long val = (unsigned long) sp[i];

        if (sizeof(long) == 4)
            serverLog(LL_WARNING, "(%08lx) -> %08lx", addr, val);
        else
            serverLog(LL_WARNING, "(%016lx) -> %016lx", addr, val);
    }
}

/*
 * logRegisters - 输出处理器寄存器状态。
 *
 * 根据不同平台（macOS、Linux、FreeBSD、OpenBSD 等）
 * 和架构（x86、x86_64、ARM、AArch64、RISC-V 等）
 * 输出寄存器的当前值，用于崩溃诊断。
 */
void logRegisters(ucontext_t *uc) {
    serverLog(LL_WARNING|LL_RAW, "\n------ REGISTERS ------\n");
/* 不支持的平台：输出提示信息 */
#define NOT_SUPPORTED() do {\
    UNUSED(uc);\
    serverLog(LL_WARNING,\
              "  Dumping of registers not supported for this OS/arch");\
} while(0)

/* macOS */
#if defined(__APPLE__) && defined(MAC_OS_10_6_DETECTED)
  /* OSX AMD64 */
    #if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCS :%016lx FS:%016lx  GS:%016lx",
        (unsigned long) uc->uc_mcontext->__ss.__rax,
        (unsigned long) uc->uc_mcontext->__ss.__rbx,
        (unsigned long) uc->uc_mcontext->__ss.__rcx,
        (unsigned long) uc->uc_mcontext->__ss.__rdx,
        (unsigned long) uc->uc_mcontext->__ss.__rdi,
        (unsigned long) uc->uc_mcontext->__ss.__rsi,
        (unsigned long) uc->uc_mcontext->__ss.__rbp,
        (unsigned long) uc->uc_mcontext->__ss.__rsp,
        (unsigned long) uc->uc_mcontext->__ss.__r8,
        (unsigned long) uc->uc_mcontext->__ss.__r9,
        (unsigned long) uc->uc_mcontext->__ss.__r10,
        (unsigned long) uc->uc_mcontext->__ss.__r11,
        (unsigned long) uc->uc_mcontext->__ss.__r12,
        (unsigned long) uc->uc_mcontext->__ss.__r13,
        (unsigned long) uc->uc_mcontext->__ss.__r14,
        (unsigned long) uc->uc_mcontext->__ss.__r15,
        (unsigned long) uc->uc_mcontext->__ss.__rip,
        (unsigned long) uc->uc_mcontext->__ss.__rflags,
        (unsigned long) uc->uc_mcontext->__ss.__cs,
        (unsigned long) uc->uc_mcontext->__ss.__fs,
        (unsigned long) uc->uc_mcontext->__ss.__gs
    );
    logStackContent((void**)uc->uc_mcontext->__ss.__rsp);
    #elif defined(__i386__)
    /* OSX x86 */
    serverLog(LL_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS:%08lx  EFL:%08lx EIP:%08lx CS :%08lx\n"
    "DS:%08lx  ES:%08lx  FS :%08lx GS :%08lx",
        (unsigned long) uc->uc_mcontext->__ss.__eax,
        (unsigned long) uc->uc_mcontext->__ss.__ebx,
        (unsigned long) uc->uc_mcontext->__ss.__ecx,
        (unsigned long) uc->uc_mcontext->__ss.__edx,
        (unsigned long) uc->uc_mcontext->__ss.__edi,
        (unsigned long) uc->uc_mcontext->__ss.__esi,
        (unsigned long) uc->uc_mcontext->__ss.__ebp,
        (unsigned long) uc->uc_mcontext->__ss.__esp,
        (unsigned long) uc->uc_mcontext->__ss.__ss,
        (unsigned long) uc->uc_mcontext->__ss.__eflags,
        (unsigned long) uc->uc_mcontext->__ss.__eip,
        (unsigned long) uc->uc_mcontext->__ss.__cs,
        (unsigned long) uc->uc_mcontext->__ss.__ds,
        (unsigned long) uc->uc_mcontext->__ss.__es,
        (unsigned long) uc->uc_mcontext->__ss.__fs,
        (unsigned long) uc->uc_mcontext->__ss.__gs
    );
    logStackContent((void**)uc->uc_mcontext->__ss.__esp);
    #else
    /* OSX ARM64 */
    serverLog(LL_WARNING,
    "\n"
    "x0:%016lx x1:%016lx x2:%016lx x3:%016lx\n"
    "x4:%016lx x5:%016lx x6:%016lx x7:%016lx\n"
    "x8:%016lx x9:%016lx x10:%016lx x11:%016lx\n"
    "x12:%016lx x13:%016lx x14:%016lx x15:%016lx\n"
    "x16:%016lx x17:%016lx x18:%016lx x19:%016lx\n"
    "x20:%016lx x21:%016lx x22:%016lx x23:%016lx\n"
    "x24:%016lx x25:%016lx x26:%016lx x27:%016lx\n"
    "x28:%016lx fp:%016lx lr:%016lx\n"
    "sp:%016lx pc:%016lx cpsr:%08lx\n",
        (unsigned long) uc->uc_mcontext->__ss.__x[0],
        (unsigned long) uc->uc_mcontext->__ss.__x[1],
        (unsigned long) uc->uc_mcontext->__ss.__x[2],
        (unsigned long) uc->uc_mcontext->__ss.__x[3],
        (unsigned long) uc->uc_mcontext->__ss.__x[4],
        (unsigned long) uc->uc_mcontext->__ss.__x[5],
        (unsigned long) uc->uc_mcontext->__ss.__x[6],
        (unsigned long) uc->uc_mcontext->__ss.__x[7],
        (unsigned long) uc->uc_mcontext->__ss.__x[8],
        (unsigned long) uc->uc_mcontext->__ss.__x[9],
        (unsigned long) uc->uc_mcontext->__ss.__x[10],
        (unsigned long) uc->uc_mcontext->__ss.__x[11],
        (unsigned long) uc->uc_mcontext->__ss.__x[12],
        (unsigned long) uc->uc_mcontext->__ss.__x[13],
        (unsigned long) uc->uc_mcontext->__ss.__x[14],
        (unsigned long) uc->uc_mcontext->__ss.__x[15],
        (unsigned long) uc->uc_mcontext->__ss.__x[16],
        (unsigned long) uc->uc_mcontext->__ss.__x[17],
        (unsigned long) uc->uc_mcontext->__ss.__x[18],
        (unsigned long) uc->uc_mcontext->__ss.__x[19],
        (unsigned long) uc->uc_mcontext->__ss.__x[20],
        (unsigned long) uc->uc_mcontext->__ss.__x[21],
        (unsigned long) uc->uc_mcontext->__ss.__x[22],
        (unsigned long) uc->uc_mcontext->__ss.__x[23],
        (unsigned long) uc->uc_mcontext->__ss.__x[24],
        (unsigned long) uc->uc_mcontext->__ss.__x[25],
        (unsigned long) uc->uc_mcontext->__ss.__x[26],
        (unsigned long) uc->uc_mcontext->__ss.__x[27],
        (unsigned long) uc->uc_mcontext->__ss.__x[28],
        (unsigned long) arm_thread_state64_get_fp(uc->uc_mcontext->__ss),
        (unsigned long) arm_thread_state64_get_lr(uc->uc_mcontext->__ss),
        (unsigned long) arm_thread_state64_get_sp(uc->uc_mcontext->__ss),
        (unsigned long) arm_thread_state64_get_pc(uc->uc_mcontext->__ss),
        (unsigned long) uc->uc_mcontext->__ss.__cpsr
    );
    logStackContent((void**) arm_thread_state64_get_sp(uc->uc_mcontext->__ss));
    #endif
/* Linux */
#elif defined(__linux__)
    /* Linux x86 */
    #if defined(__i386__) || ((defined(__X86_64__) || defined(__x86_64__)) && defined(__ILP32__))
    serverLog(LL_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS :%08lx EFL:%08lx EIP:%08lx CS:%08lx\n"
    "DS :%08lx ES :%08lx FS :%08lx GS:%08lx",
        (unsigned long) uc->uc_mcontext.gregs[11],
        (unsigned long) uc->uc_mcontext.gregs[8],
        (unsigned long) uc->uc_mcontext.gregs[10],
        (unsigned long) uc->uc_mcontext.gregs[9],
        (unsigned long) uc->uc_mcontext.gregs[4],
        (unsigned long) uc->uc_mcontext.gregs[5],
        (unsigned long) uc->uc_mcontext.gregs[6],
        (unsigned long) uc->uc_mcontext.gregs[7],
        (unsigned long) uc->uc_mcontext.gregs[18],
        (unsigned long) uc->uc_mcontext.gregs[17],
        (unsigned long) uc->uc_mcontext.gregs[14],
        (unsigned long) uc->uc_mcontext.gregs[15],
        (unsigned long) uc->uc_mcontext.gregs[3],
        (unsigned long) uc->uc_mcontext.gregs[2],
        (unsigned long) uc->uc_mcontext.gregs[1],
        (unsigned long) uc->uc_mcontext.gregs[0]
    );
    logStackContent((void**)uc->uc_mcontext.gregs[7]);
    #elif defined(__X86_64__) || defined(__x86_64__)
    /* Linux AMD64 */
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->uc_mcontext.gregs[13],
        (unsigned long) uc->uc_mcontext.gregs[11],
        (unsigned long) uc->uc_mcontext.gregs[14],
        (unsigned long) uc->uc_mcontext.gregs[12],
        (unsigned long) uc->uc_mcontext.gregs[8],
        (unsigned long) uc->uc_mcontext.gregs[9],
        (unsigned long) uc->uc_mcontext.gregs[10],
        (unsigned long) uc->uc_mcontext.gregs[15],
        (unsigned long) uc->uc_mcontext.gregs[0],
        (unsigned long) uc->uc_mcontext.gregs[1],
        (unsigned long) uc->uc_mcontext.gregs[2],
        (unsigned long) uc->uc_mcontext.gregs[3],
        (unsigned long) uc->uc_mcontext.gregs[4],
        (unsigned long) uc->uc_mcontext.gregs[5],
        (unsigned long) uc->uc_mcontext.gregs[6],
        (unsigned long) uc->uc_mcontext.gregs[7],
        (unsigned long) uc->uc_mcontext.gregs[16],
        (unsigned long) uc->uc_mcontext.gregs[17],
        (unsigned long) uc->uc_mcontext.gregs[18]
    );
    logStackContent((void**)uc->uc_mcontext.gregs[15]);
    #elif defined(__riscv) /* Linux RISC-V */
    serverLog(LL_WARNING,
	"\n"
    "ra:%016lx gp:%016lx\ntp:%016lx t0:%016lx\n"
    "t1:%016lx t2:%016lx\ns0:%016lx s1:%016lx\n"
    "a0:%016lx a1:%016lx\na2:%016lx a3:%016lx\n"
    "a4:%016lx a5:%016lx\na6:%016lx a7:%016lx\n"
    "s2:%016lx s3:%016lx\ns4:%016lx s5:%016lx\n"
    "s6:%016lx s7:%016lx\ns8:%016lx s9:%016lx\n"
    "s10:%016lx s11:%016lx\nt3:%016lx t4:%016lx\n"
    "t5:%016lx t6:%016lx\n",
        (unsigned long) uc->uc_mcontext.__gregs[1],
        (unsigned long) uc->uc_mcontext.__gregs[3],
        (unsigned long) uc->uc_mcontext.__gregs[4],
        (unsigned long) uc->uc_mcontext.__gregs[5],
        (unsigned long) uc->uc_mcontext.__gregs[6],
        (unsigned long) uc->uc_mcontext.__gregs[7],
        (unsigned long) uc->uc_mcontext.__gregs[8],
        (unsigned long) uc->uc_mcontext.__gregs[9],
        (unsigned long) uc->uc_mcontext.__gregs[10],
        (unsigned long) uc->uc_mcontext.__gregs[11],
        (unsigned long) uc->uc_mcontext.__gregs[12],
        (unsigned long) uc->uc_mcontext.__gregs[13],
        (unsigned long) uc->uc_mcontext.__gregs[14],
        (unsigned long) uc->uc_mcontext.__gregs[15],
        (unsigned long) uc->uc_mcontext.__gregs[16],
        (unsigned long) uc->uc_mcontext.__gregs[17],
        (unsigned long) uc->uc_mcontext.__gregs[18],
        (unsigned long) uc->uc_mcontext.__gregs[19],
        (unsigned long) uc->uc_mcontext.__gregs[20],
        (unsigned long) uc->uc_mcontext.__gregs[21],
        (unsigned long) uc->uc_mcontext.__gregs[22],
        (unsigned long) uc->uc_mcontext.__gregs[23],
        (unsigned long) uc->uc_mcontext.__gregs[24],
        (unsigned long) uc->uc_mcontext.__gregs[25],
        (unsigned long) uc->uc_mcontext.__gregs[26],
        (unsigned long) uc->uc_mcontext.__gregs[27],
        (unsigned long) uc->uc_mcontext.__gregs[28],
        (unsigned long) uc->uc_mcontext.__gregs[29],
        (unsigned long) uc->uc_mcontext.__gregs[30],
        (unsigned long) uc->uc_mcontext.__gregs[31]
    );
    logStackContent((void**)uc->uc_mcontext.__gregs[REG_SP]);
    #elif defined(__aarch64__) /* Linux AArch64 */
    serverLog(LL_WARNING,
	      "\n"
	      "X18:%016lx X19:%016lx\nX20:%016lx X21:%016lx\n"
	      "X22:%016lx X23:%016lx\nX24:%016lx X25:%016lx\n"
	      "X26:%016lx X27:%016lx\nX28:%016lx X29:%016lx\n"
	      "X30:%016lx\n"
	      "pc:%016lx sp:%016lx\npstate:%016lx fault_address:%016lx\n",
	      (unsigned long) uc->uc_mcontext.regs[18],
	      (unsigned long) uc->uc_mcontext.regs[19],
	      (unsigned long) uc->uc_mcontext.regs[20],
	      (unsigned long) uc->uc_mcontext.regs[21],
	      (unsigned long) uc->uc_mcontext.regs[22],
	      (unsigned long) uc->uc_mcontext.regs[23],
	      (unsigned long) uc->uc_mcontext.regs[24],
	      (unsigned long) uc->uc_mcontext.regs[25],
	      (unsigned long) uc->uc_mcontext.regs[26],
	      (unsigned long) uc->uc_mcontext.regs[27],
	      (unsigned long) uc->uc_mcontext.regs[28],
	      (unsigned long) uc->uc_mcontext.regs[29],
	      (unsigned long) uc->uc_mcontext.regs[30],
	      (unsigned long) uc->uc_mcontext.pc,
	      (unsigned long) uc->uc_mcontext.sp,
	      (unsigned long) uc->uc_mcontext.pstate,
	      (unsigned long) uc->uc_mcontext.fault_address
		      );
	      logStackContent((void**)uc->uc_mcontext.sp);
    #elif defined(__arm__) /* Linux ARM */
    serverLog(LL_WARNING,
	      "\n"
	      "R10:%016lx R9 :%016lx\nR8 :%016lx R7 :%016lx\n"
	      "R6 :%016lx R5 :%016lx\nR4 :%016lx R3 :%016lx\n"
	      "R2 :%016lx R1 :%016lx\nR0 :%016lx EC :%016lx\n"
	      "fp: %016lx ip:%016lx\n"
	      "pc:%016lx sp:%016lx\ncpsr:%016lx fault_address:%016lx\n",
	      (unsigned long) uc->uc_mcontext.arm_r10,
	      (unsigned long) uc->uc_mcontext.arm_r9,
	      (unsigned long) uc->uc_mcontext.arm_r8,
	      (unsigned long) uc->uc_mcontext.arm_r7,
	      (unsigned long) uc->uc_mcontext.arm_r6,
	      (unsigned long) uc->uc_mcontext.arm_r5,
	      (unsigned long) uc->uc_mcontext.arm_r4,
	      (unsigned long) uc->uc_mcontext.arm_r3,
	      (unsigned long) uc->uc_mcontext.arm_r2,
	      (unsigned long) uc->uc_mcontext.arm_r1,
	      (unsigned long) uc->uc_mcontext.arm_r0,
	      (unsigned long) uc->uc_mcontext.error_code,
	      (unsigned long) uc->uc_mcontext.arm_fp,
	      (unsigned long) uc->uc_mcontext.arm_ip,
	      (unsigned long) uc->uc_mcontext.arm_pc,
	      (unsigned long) uc->uc_mcontext.arm_sp,
	      (unsigned long) uc->uc_mcontext.arm_cpsr,
	      (unsigned long) uc->uc_mcontext.fault_address
		      );
	      logStackContent((void**)uc->uc_mcontext.arm_sp);
    #else
	NOT_SUPPORTED();
    #endif
#elif defined(__FreeBSD__)
    #if defined(__x86_64__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->uc_mcontext.mc_rax,
        (unsigned long) uc->uc_mcontext.mc_rbx,
        (unsigned long) uc->uc_mcontext.mc_rcx,
        (unsigned long) uc->uc_mcontext.mc_rdx,
        (unsigned long) uc->uc_mcontext.mc_rdi,
        (unsigned long) uc->uc_mcontext.mc_rsi,
        (unsigned long) uc->uc_mcontext.mc_rbp,
        (unsigned long) uc->uc_mcontext.mc_rsp,
        (unsigned long) uc->uc_mcontext.mc_r8,
        (unsigned long) uc->uc_mcontext.mc_r9,
        (unsigned long) uc->uc_mcontext.mc_r10,
        (unsigned long) uc->uc_mcontext.mc_r11,
        (unsigned long) uc->uc_mcontext.mc_r12,
        (unsigned long) uc->uc_mcontext.mc_r13,
        (unsigned long) uc->uc_mcontext.mc_r14,
        (unsigned long) uc->uc_mcontext.mc_r15,
        (unsigned long) uc->uc_mcontext.mc_rip,
        (unsigned long) uc->uc_mcontext.mc_rflags,
        (unsigned long) uc->uc_mcontext.mc_cs
    );
    logStackContent((void**)uc->uc_mcontext.mc_rsp);
    #elif defined(__i386__)
    serverLog(LL_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS :%08lx EFL:%08lx EIP:%08lx CS:%08lx\n"
    "DS :%08lx ES :%08lx FS :%08lx GS:%08lx",
        (unsigned long) uc->uc_mcontext.mc_eax,
        (unsigned long) uc->uc_mcontext.mc_ebx,
        (unsigned long) uc->uc_mcontext.mc_ebx,
        (unsigned long) uc->uc_mcontext.mc_edx,
        (unsigned long) uc->uc_mcontext.mc_edi,
        (unsigned long) uc->uc_mcontext.mc_esi,
        (unsigned long) uc->uc_mcontext.mc_ebp,
        (unsigned long) uc->uc_mcontext.mc_esp,
        (unsigned long) uc->uc_mcontext.mc_ss,
        (unsigned long) uc->uc_mcontext.mc_eflags,
        (unsigned long) uc->uc_mcontext.mc_eip,
        (unsigned long) uc->uc_mcontext.mc_cs,
        (unsigned long) uc->uc_mcontext.mc_es,
        (unsigned long) uc->uc_mcontext.mc_fs,
        (unsigned long) uc->uc_mcontext.mc_gs
    );
    logStackContent((void**)uc->uc_mcontext.mc_esp);
    #else
    NOT_SUPPORTED();
    #endif
#elif defined(__OpenBSD__)
    #if defined(__x86_64__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->sc_rax,
        (unsigned long) uc->sc_rbx,
        (unsigned long) uc->sc_rcx,
        (unsigned long) uc->sc_rdx,
        (unsigned long) uc->sc_rdi,
        (unsigned long) uc->sc_rsi,
        (unsigned long) uc->sc_rbp,
        (unsigned long) uc->sc_rsp,
        (unsigned long) uc->sc_r8,
        (unsigned long) uc->sc_r9,
        (unsigned long) uc->sc_r10,
        (unsigned long) uc->sc_r11,
        (unsigned long) uc->sc_r12,
        (unsigned long) uc->sc_r13,
        (unsigned long) uc->sc_r14,
        (unsigned long) uc->sc_r15,
        (unsigned long) uc->sc_rip,
        (unsigned long) uc->sc_rflags,
        (unsigned long) uc->sc_cs
    );
    logStackContent((void**)uc->sc_rsp);
    #elif defined(__i386__)
    serverLog(LL_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS :%08lx EFL:%08lx EIP:%08lx CS:%08lx\n"
    "DS :%08lx ES :%08lx FS :%08lx GS:%08lx",
        (unsigned long) uc->sc_eax,
        (unsigned long) uc->sc_ebx,
        (unsigned long) uc->sc_ebx,
        (unsigned long) uc->sc_edx,
        (unsigned long) uc->sc_edi,
        (unsigned long) uc->sc_esi,
        (unsigned long) uc->sc_ebp,
        (unsigned long) uc->sc_esp,
        (unsigned long) uc->sc_ss,
        (unsigned long) uc->sc_eflags,
        (unsigned long) uc->sc_eip,
        (unsigned long) uc->sc_cs,
        (unsigned long) uc->sc_es,
        (unsigned long) uc->sc_fs,
        (unsigned long) uc->sc_gs
    );
    logStackContent((void**)uc->sc_esp);
    #else
    NOT_SUPPORTED();
    #endif
#elif defined(__NetBSD__)
    #if defined(__x86_64__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RAX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RBX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RCX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RDX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RDI],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RSI],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RBP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RSP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R8],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R9],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R10],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R11],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R12],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R13],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R14],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R15],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RIP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RFLAGS],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_CS]
    );
    logStackContent((void**)uc->uc_mcontext.__gregs[_REG_RSP]);
    #elif defined(__i386__)
    serverLog(LL_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS :%08lx EFL:%08lx EIP:%08lx CS:%08lx\n"
    "DS :%08lx ES :%08lx FS :%08lx GS:%08lx",
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EAX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EBX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EDX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EDI],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_ESI],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EBP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_ESP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_SS],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EFLAGS],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EIP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_CS],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_ES],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_FS],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_GS]
    );
    #else
    NOT_SUPPORTED();
    #endif
#elif defined(__DragonFly__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->uc_mcontext.mc_rax,
        (unsigned long) uc->uc_mcontext.mc_rbx,
        (unsigned long) uc->uc_mcontext.mc_rcx,
        (unsigned long) uc->uc_mcontext.mc_rdx,
        (unsigned long) uc->uc_mcontext.mc_rdi,
        (unsigned long) uc->uc_mcontext.mc_rsi,
        (unsigned long) uc->uc_mcontext.mc_rbp,
        (unsigned long) uc->uc_mcontext.mc_rsp,
        (unsigned long) uc->uc_mcontext.mc_r8,
        (unsigned long) uc->uc_mcontext.mc_r9,
        (unsigned long) uc->uc_mcontext.mc_r10,
        (unsigned long) uc->uc_mcontext.mc_r11,
        (unsigned long) uc->uc_mcontext.mc_r12,
        (unsigned long) uc->uc_mcontext.mc_r13,
        (unsigned long) uc->uc_mcontext.mc_r14,
        (unsigned long) uc->uc_mcontext.mc_r15,
        (unsigned long) uc->uc_mcontext.mc_rip,
        (unsigned long) uc->uc_mcontext.mc_rflags,
        (unsigned long) uc->uc_mcontext.mc_cs
    );
    logStackContent((void**)uc->uc_mcontext.mc_rsp);
#elif defined(__sun)
    #if defined(__x86_64__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->uc_mcontext.gregs[REG_RAX],
        (unsigned long) uc->uc_mcontext.gregs[REG_RBX],
        (unsigned long) uc->uc_mcontext.gregs[REG_RCX],
        (unsigned long) uc->uc_mcontext.gregs[REG_RDX],
        (unsigned long) uc->uc_mcontext.gregs[REG_RDI],
        (unsigned long) uc->uc_mcontext.gregs[REG_RSI],
        (unsigned long) uc->uc_mcontext.gregs[REG_RBP],
        (unsigned long) uc->uc_mcontext.gregs[REG_RSP],
        (unsigned long) uc->uc_mcontext.gregs[REG_R8],
        (unsigned long) uc->uc_mcontext.gregs[REG_R9],
        (unsigned long) uc->uc_mcontext.gregs[REG_R10],
        (unsigned long) uc->uc_mcontext.gregs[REG_R11],
        (unsigned long) uc->uc_mcontext.gregs[REG_R12],
        (unsigned long) uc->uc_mcontext.gregs[REG_R13],
        (unsigned long) uc->uc_mcontext.gregs[REG_R14],
        (unsigned long) uc->uc_mcontext.gregs[REG_R15],
        (unsigned long) uc->uc_mcontext.gregs[REG_RIP],
        (unsigned long) uc->uc_mcontext.gregs[REG_RFL],
        (unsigned long) uc->uc_mcontext.gregs[REG_CS]
    );
    logStackContent((void**)uc->uc_mcontext.gregs[REG_RSP]);
    #endif
#else
    NOT_SUPPORTED();
#endif
#undef NOT_SUPPORTED
}

#endif /* HAVE_BACKTRACE */

/*
 * openDirectLogFiledes - 打开一个直接写入 Redis 日志的文件描述符。
 *
 * 使用 write(2) 系统调用直接写入，适用于代码中的临界区
 * （例如内存测试期间），此时不能信任 Redis 的其余部分，
 * 或者当 API 调用需要原始文件描述符时。
 *
 * 使用 closeDirectLogFiledes() 关闭。
 */
int openDirectLogFiledes(void) {
    int log_to_stdout = server.logfile[0] == '\0';
    int fd = log_to_stdout ?
        STDOUT_FILENO :
        open(server.logfile, O_APPEND|O_CREAT|O_WRONLY, 0644);
    return fd;
}

/* 关闭 openDirectLogFiledes() 返回的文件描述符 */
void closeDirectLogFiledes(int fd) {
    int log_to_stdout = server.logfile[0] == '\0';
    if (!log_to_stdout) close(fd);
}

/* Linux 平台下的多线程堆栈跟踪支持 */
#if defined(HAVE_BACKTRACE) && defined(__linux__)
/* 用于在信号处理器和主线程之间传递堆栈跟踪数据的管道 */
static int stacktrace_pipe[2] = {0};
static void setupStacktracePipe(void) {
    if (-1 == anetPipe(stacktrace_pipe, O_CLOEXEC | O_NONBLOCK, O_CLOEXEC | O_NONBLOCK)) {
        serverLog(LL_WARNING, "setupStacktracePipe failed: %s", strerror(errno));
    }
}
#else
static void setupStacktracePipe(void) {/* we don't need a pipe to write the stacktraces */}
#endif
#ifdef HAVE_BACKTRACE
#define BACKTRACE_MAX_SIZE 100

#ifdef __linux__
#if !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif
#include <sys/prctl.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <dirent.h>

#define TIDS_MAX_SIZE 50
static size_t get_ready_to_signal_threads_tids(int sig_num, pid_t tids[TIDS_MAX_SIZE]);

/* 堆栈跟踪数据结构，用于在信号处理器中收集线程信息 */
typedef struct {
    char thread_name[16];              /* 线程名称 */
    int trace_size;                    /* 堆栈帧数量 */
    pid_t tid;                         /* 线程 ID */
    void *trace[BACKTRACE_MAX_SIZE];   /* 堆栈帧地址数组 */
} stacktrace_data;

/*
 * collect_stacktrace_data - 收集当前线程的堆栈跟踪数据。
 *
 * 由 ThreadsManager_runOnThreads() 在各线程中调用，
 * 收集后通过 stacktrace_pipe 发送给主线程。
 * 此函数标记为 noinline 以确保 backtrace() 能正确跳过它。
 */
__attribute__ ((noinline)) static void collect_stacktrace_data(void) {
    stacktrace_data trace_data = {{0}};

    /* 首先获取堆栈跟踪！ */
    trace_data.trace_size = backtrace(trace_data.trace, BACKTRACE_MAX_SIZE);

    /* 获取线程名称 */
    prctl(PR_GET_NAME, trace_data.thread_name);

    /* 获取线程 ID */
    trace_data.tid = syscall(SYS_gettid);

    /* 将数据发送给主进程 */
    if (write(stacktrace_pipe[1], &trace_data, sizeof(trace_data)) == -1) {/* 避免警告 */};
}

/*
 * writeStacktraces - 收集并写入所有线程的堆栈跟踪。
 *
 * 此函数通过信号机制触发每个线程收集堆栈跟踪数据，
 * 然后从 stacktrace_pipe 中读取数据并写入日志文件。
 * 仅在 Linux 平台可用。
 */
__attribute__ ((noinline))
static void writeStacktraces(int fd, int uplevel) {
    /* 获取所有不阻塞或忽略 THREADS_SIGNAL 的线程列表 */
    pid_t tids[TIDS_MAX_SIZE];
    size_t len_tids = get_ready_to_signal_threads_tids(THREADS_SIGNAL, tids);
    if (!len_tids) {
        serverLogRawFromHandler(LL_WARNING, "writeStacktraces(): Failed to get the process's threads.");
    }

    char buff[PIPE_BUF];
    /* 清空堆栈跟踪管道中的旧数据 */
    while (read(stacktrace_pipe[0], &buff, sizeof(buff)) > 0) {}

    /* ThreadsManager_runOnThreads 返回 0 表示已在运行中 */
    if (!ThreadsManager_runOnThreads(tids, len_tids, collect_stacktrace_data)) return;

    size_t collected = 0;

    pid_t calling_tid = syscall(SYS_gettid);

    /* 从 stacktrace_pipe 中读取数据直到为空 */
    stacktrace_data curr_stacktrace_data = {{0}};
    while (read(stacktrace_pipe[0], &curr_stacktrace_data, sizeof(curr_stacktrace_data)) > 0) {
        /* 堆栈跟踪头部：线程 ID 和线程名称 */
        snprintf_async_signal_safe(buff, sizeof(buff), "\n%d %s", curr_stacktrace_data.tid, curr_stacktrace_data.thread_name);
        if (write(fd,buff,strlen(buff)) == -1) {/* 避免警告 */};

        /* 跳过内核信号处理调用、信号处理器和回调函数的地址 */
        int curr_uplevel = 3;

        if (curr_stacktrace_data.tid == calling_tid) {
            /* 跳过 signal 系统调用和 ThreadsManager_runOnThreads */
            curr_uplevel += uplevel + 2;
            /* 在当前处理日志的线程头部添加标记 */
            if (write(fd," *\n",strlen(" *\n")) == -1) {/* 避免警告 */};
        } else {
            /* 仅添加换行 */
            if (write(fd,"\n",strlen("\n")) == -1) {/* 避免警告 */};
        }

        /* 输出堆栈跟踪符号 */
        backtrace_symbols_fd(curr_stacktrace_data.trace+curr_uplevel, curr_stacktrace_data.trace_size-curr_uplevel, fd);

        ++collected;
    }

    snprintf_async_signal_safe(buff, sizeof(buff), "\n%lu/%lu expected stacktraces.\n", (long unsigned)(collected), (long unsigned)len_tids);
    if (write(fd,buff,strlen(buff)) == -1) {/* 避免编译器警告 */};

}

#endif /* __linux__ */

/* writeCurrentThreadsStackTrace - 仅写入当前线程的堆栈跟踪 */
__attribute__ ((noinline))
static void writeCurrentThreadsStackTrace(int fd, int uplevel) {
    void *trace[BACKTRACE_MAX_SIZE];

    int trace_size = backtrace(trace, BACKTRACE_MAX_SIZE);

    char *msg = "\nBacktrace:\n";
    if (write(fd,msg,strlen(msg)) == -1) {/* 避免编译器警告 */};
    backtrace_symbols_fd(trace+uplevel, trace_size-uplevel, fd);
}

/*
 * logStackTrace - 使用 backtrace() 记录堆栈跟踪。
 *
 * 此函数设计为可从信号处理器中安全调用。
 *
 * 参数：
 *   eip: 指令指针（可选，可以为 NULL）
 *   uplevel: 需要跳过的调用函数层数
 *   current_thread: 非零值表示仅记录当前线程；
 *                   在 Linux 上，0 表示记录所有线程
 *
 * 注意：参与 uplevel 计数的函数应使用
 * __attribute__ ((noinline)) 声明，
 * 以确保编译器不会将其内联。
 */
__attribute__ ((noinline))
void logStackTrace(void *eip, int uplevel, int current_thread) {
    int fd = openDirectLogFiledes();
    char *msg;
    uplevel++; /* 跳过本函数自身 */

    if (fd == -1) return; /* 无法记录日志则直接返回 */

    msg = "\n------ STACK TRACE ------\n";
    if (write(fd,msg,strlen(msg)) == -1) {/* 避免编译器警告 */};

    if (eip) {
        /* 将 EIP（指令指针）写入日志文件 */
        msg = "EIP:\n";
        if (write(fd,msg,strlen(msg)) == -1) {/* 避免警告 */};
        backtrace_symbols_fd(&eip, 1, fd);
    }

    /* 将堆栈符号写入日志文件 */
    ++uplevel;
#ifdef __linux__
    if (current_thread) {
        writeCurrentThreadsStackTrace(fd, uplevel);
    } else {
        writeStacktraces(fd, uplevel);
    }
#else
    /* 在非 Linux 平台上，仅支持写入当前线程的堆栈跟踪 */
    UNUSED(current_thread);
    writeCurrentThreadsStackTrace(fd, uplevel);
#endif
    msg = "\n------ STACK TRACE DONE ------\n";
    if (write(fd,msg,strlen(msg)) == -1) {/* 避免编译器警告 */};


    /* 清理：关闭文件描述符 */
    closeDirectLogFiledes(fd);
}

#endif /* HAVE_BACKTRACE */

/* genClusterDebugString - 生成集群调试信息字符串，包含集群信息和节点描述 */
sds genClusterDebugString(sds infostring) {
    sds cluster_info = genClusterInfoString();
    sds cluster_nodes = clusterGenNodesDescription(NULL, 0, 0);

    infostring = sdscatprintf(infostring, "\r\n# Cluster info\r\n");
    infostring = sdscatsds(infostring, cluster_info);
    infostring = sdscatprintf(infostring, "\n------ CLUSTER NODES OUTPUT ------\n");
    infostring = sdscatsds(infostring, cluster_nodes);

    sdsfree(cluster_info);
    sdsfree(cluster_nodes);

    return infostring;
}

/* logServerInfo - 记录全局服务器信息（INFO 输出和客户端列表） */
void logServerInfo(void) {
    sds infostring, clients;
    serverLogRaw(LL_WARNING|LL_RAW, "\n------ INFO OUTPUT ------\n");
    int all = 0, everything = 0;
    robj *argv[1];
    argv[0] = createStringObject("all", strlen("all"));
    dict *section_dict = genInfoSectionDict(argv, 1, NULL, &all, &everything);
    infostring = genRedisInfoString(section_dict, all, everything);
    if (server.cluster_enabled){
        infostring = genClusterDebugString(infostring);
    }
    serverLogRaw(LL_WARNING|LL_RAW, infostring);
    serverLogRaw(LL_WARNING|LL_RAW, "\n------ CLIENT LIST OUTPUT ------\n");
    clients = getAllClientsInfoString(-1);
    serverLogRaw(LL_WARNING|LL_RAW, clients);
    sdsfree(infostring);
    sdsfree(clients);
    releaseInfoSectionDict(section_dict);
    decrRefCount(argv[0]);
}

/* logConfigDebugInfo - 记录配置调试信息 */
void logConfigDebugInfo(void) {
    sds configstring;
    configstring = getConfigDebugInfo();
    serverLogRaw(LL_WARNING|LL_RAW, "\n------ CONFIG DEBUG OUTPUT ------\n");
    serverLogRaw(LL_WARNING|LL_RAW, configstring);
    sdsfree(configstring);
}

/* logModulesInfo - 记录模块信息。放在最后执行，因为它可能崩溃。 */
void logModulesInfo(void) {
    serverLogRaw(LL_WARNING|LL_RAW, "\n------ MODULES INFO OUTPUT ------\n");
    sds infostring = modulesCollectInfo(sdsempty(), NULL, 1, 0);
    serverLogRaw(LL_WARNING|LL_RAW, infostring);
    sdsfree(infostring);
}

/*
 * logCurrentClient - 记录"当前"客户端的信息。
 *
 * "当前"客户端是指 Redis 正在服务的客户端。
 * 如果 Redis 当前没有在服务客户端，则 cc 为 NULL。
 */
void logCurrentClient(client *cc, const char *title) {
    if (cc == NULL) return;

    sds client;
    int j;

    serverLog(LL_WARNING|LL_RAW, "\n------ %s CLIENT INFO ------\n", title);
    client = catClientInfoString(sdsempty(),cc);
    serverLog(LL_WARNING|LL_RAW,"%s\n", client);
    sdsfree(client);
    serverLog(LL_WARNING|LL_RAW,"argc: '%d'\n", cc->argc);
    for (j = 0; j < cc->argc; j++) {
        if (j >= clientArgsToLog(cc)) {
            serverLog(LL_WARNING|LL_RAW,"argv[%d]: *redacted*\n",j);
            continue;
        }
        robj *decoded;
        decoded = getDecodedObject(cc->argv[j]);
        sds repr = sdscatrepr(sdsempty(),decoded->ptr, min(sdslen(decoded->ptr), 1024));
        serverLog(LL_WARNING|LL_RAW,"argv[%d]: '%s'\n", j, (char*)repr);
        if (!strcasecmp(decoded->ptr, "auth") || !strcasecmp(decoded->ptr, "auth2")) {
            sdsfree(repr);
            decrRefCount(decoded);
            break;
        }
        sdsfree(repr);
        decrRefCount(decoded);
    }
    /* 检查第一个参数（通常是键）是否存在于当前数据库中，
     * 如果存在则输出关联对象的信息。 */
    if (cc->argc > 1) {
        robj *val, *key;
        dictEntry *de;

        key = getDecodedObject(cc->argv[1]);
        de = dbFind(cc->db, key->ptr);
        if (de) {
            val = dictGetVal(de);
            serverLog(LL_WARNING,"key '%s' found in DB containing the following object:", (char*)key->ptr);
            serverLogObjectDebugInfo(val);
        }
        decrRefCount(key);
    }
}

#if defined(HAVE_PROC_MAPS)

#define MEMTEST_MAX_REGIONS 128

/*
 * memtest_test_linux_anonymous_maps - 在段错误期间执行的非破坏性内存测试。
 *
 * 解析 /proc/self/maps 获取可写的匿名内存映射区域，
 * 然后对每个区域进行 memtest_preserving_test 测试。
 * 注意：必须在关闭文件描述符之前完成测试，
 * 因为关闭操作可能导致某些被测试的内存区域被取消映射。
 */
int memtest_test_linux_anonymous_maps(void) {
    FILE *fp;
    char line[1024];
    char logbuf[1024];
    size_t start_addr, end_addr, size;
    size_t start_vect[MEMTEST_MAX_REGIONS];
    size_t size_vect[MEMTEST_MAX_REGIONS];
    int regions = 0, j;

    int fd = openDirectLogFiledes();
    if (fd == -1) return 0;

    fp = fopen("/proc/self/maps","r");
    if (!fp) {
        closeDirectLogFiledes(fd);
        return 0;
    }
    while(fgets(line,sizeof(line),fp) != NULL) {
        char *start, *end, *p = line;

        start = p;
        p = strchr(p,'-');
        if (!p) continue;
        *p++ = '\0';
        end = p;
        p = strchr(p,' ');
        if (!p) continue;
        *p++ = '\0';
        if (strstr(p,"stack") ||
            strstr(p,"vdso") ||
            strstr(p,"vsyscall")) continue;
        if (!strstr(p,"00:00")) continue;
        if (!strstr(p,"rw")) continue;

        start_addr = strtoul(start,NULL,16);
        end_addr = strtoul(end,NULL,16);
        size = end_addr-start_addr;

        start_vect[regions] = start_addr;
        size_vect[regions] = size;
        snprintf(logbuf,sizeof(logbuf),
            "*** Preparing to test memory region %lx (%lu bytes)\n",
                (unsigned long) start_vect[regions],
                (unsigned long) size_vect[regions]);
        if (write(fd,logbuf,strlen(logbuf)) == -1) { /* 无需处理 */ }
        regions++;
    }

    int errors = 0;
    for (j = 0; j < regions; j++) {
        if (write(fd,".",1) == -1) { /* 无需处理 */ }
        errors += memtest_preserving_test((void*)start_vect[j],size_vect[j],1);
        if (write(fd, errors ? "E" : "O",1) == -1) { /* 无需处理 */ }
    }
    if (write(fd,"\n",1) == -1) { /* 无需处理 */ }

    /* 注意：必须在此处才关闭文件描述符，
     * 因为提前关闭可能导致被测试的内存区域被取消映射。 */
    fclose(fp);
    closeDirectLogFiledes(fd);
    return errors;
}
#endif /* HAVE_PROC_MAPS */

/* killMainThread - 终止主线程（如果当前不是主线程） */
static void killMainThread(void) {
    int err;
    if (pthread_self() != server.main_thread_id && pthread_cancel(server.main_thread_id) == 0) {
        if ((err = pthread_join(server.main_thread_id,NULL)) != 0) {
            serverLog(LL_WARNING, "main thread can not be joined: %s", strerror(err));
        } else {
            serverLog(LL_WARNING, "main thread terminated");
        }
    }
}

/*
 * killThreads - 以非正常方式终止运行中的线程（当前线程除外）。
 *
 * 此函数仅在必须停止线程的紧急情况下使用。
 * 目前 Redis 仅在崩溃时（如 SIGSEGV）调用此函数，
 * 以便在没有其他线程干扰内存的情况下执行快速内存检查。
 */
void killThreads(void) {
    killMainThread();
    bioKillThreads();
    killIOThreads();
}

/* doFastMemoryTest - 执行快速内存测试 */
void doFastMemoryTest(void) {
#if defined(HAVE_PROC_MAPS)
    if (server.memcheck_enabled) {
        /* 测试内存 */
        serverLogRaw(LL_WARNING|LL_RAW, "\n------ FAST MEMORY TEST ------\n");
        killThreads();
        if (memtest_test_linux_anonymous_maps()) {
            serverLogRaw(LL_WARNING|LL_RAW,
                "!!! MEMORY ERROR DETECTED! Check your memory ASAP !!!\n");
        } else {
            serverLogRaw(LL_WARNING|LL_RAW,
                "Fast memory test PASSED, however your memory can still be broken. Please run a memory test for several hours if possible.\n");
        }
    }
#endif /* HAVE_PROC_MAPS */
}

/*
 * dumpX86Calls - 扫描 x86 机器码中的函数调用。
 *
 * 从地址 addr 开始扫描最多 len 字节，
 * 搜索 E8（callq）操作码，并输出有效的调用目标符号。
 */
void dumpX86Calls(void *addr, size_t len) {
    size_t j;
    unsigned char *p = addr;
    Dl_info info;
    /* 哈希表用于尽力避免重复输出相同的符号 */
    unsigned long ht[256] = {0};

    if (len < 5) return;
    for (j = 0; j < len-4; j++) {
        if (p[j] != 0xE8) continue; /* 不是 E8 CALL 操作码 */
        unsigned long target = (unsigned long)addr+j+5;
        uint32_t tmp;
        memcpy(&tmp, p+j+1, sizeof(tmp));
        target += tmp;
        if (dladdr((void*)target, &info) != 0 && info.dli_sname != NULL) {
            if (ht[target&0xff] != target) {
                printf("Function at 0x%lx is %s\n",target,info.dli_sname);
                ht[target&0xff] = target;
            }
            j += 4; /* 跳过 32 位立即数 */
        }
    }
}

/* dumpCodeAroundEIP - 转储 EIP 指令指针周围的机器码 */
void dumpCodeAroundEIP(void *eip) {
    Dl_info info;
    if (dladdr(eip, &info) != 0) {
        serverLog(LL_WARNING|LL_RAW,
            "\n------ DUMPING CODE AROUND EIP ------\n"
            "Symbol: %s (base: %p)\n"
            "Module: %s (base %p)\n"
            "$ xxd -r -p /tmp/dump.hex /tmp/dump.bin\n"
            "$ objdump --adjust-vma=%p -D -b binary -m i386:x86-64 /tmp/dump.bin\n"
            "------\n",
            info.dli_sname, info.dli_saddr, info.dli_fname, info.dli_fbase,
            info.dli_saddr);
        size_t len = (long)eip - (long)info.dli_saddr;
        unsigned long sz = sysconf(_SC_PAGESIZE);
        if (len < 1<<13) { /* we don't have functions over 8k (verified) */
            /* 查找下一个页面地址作为转储的"安全"限制。
             * 然后尝试转储 EIP 之后的 128 字节（如果空间允许），否则提前停止。 */
            void *base = (void *)info.dli_saddr;
            unsigned long next = ((unsigned long)eip + sz) & ~(sz-1);
            unsigned long end = (unsigned long)eip + 128;
            if (end > next) end = next;
            len = end - (unsigned long)base;
            serverLogHexDump(LL_WARNING, "dump of function",
                base, len);
            dumpX86Calls(base, len);
        }
    }
}

/* 用于替换无效函数指针调用的目标函数 */
void invalidFunctionWasCalled(void) {}

typedef void (*invalidFunctionWasCalledType)(void);

/*
 * sigsegvHandler - 段错误（SIGSEGV）等信号的处理器。
 *
 * 处理 SIGSEGV、SIGBUS、SIGFPE、SIGILL 和 SIGABRT 信号。
 * 记录崩溃信息、寄存器状态、堆栈跟踪，并生成完整的崩溃报告。
 * 使用互斥锁防止多线程同时处理崩溃时产生死锁。
 */
__attribute__ ((noinline))
static void sigsegvHandler(int sig, siginfo_t *info, void *secret) {
    UNUSED(secret);
    UNUSED(info);
    int print_full_crash_info = 1;
    /* 检查是否可以安全进入信号处理器。两个线程同时崩溃会导致死锁。 */
    if(pthread_mutex_lock(&signal_handler_lock) == EDEADLK) {
        /* 如果当前线程已持有锁（即在处理信号期间再次崩溃），
         * 则切换为输出精简的崩溃信息。 */
        serverLogRawFromHandler(LL_WARNING,
            "Crashed running signal handler. Providing reduced version of recursive crash report.");
        print_full_crash_info = 0;
    }

    bugReportStart();
    serverLog(LL_WARNING,
        "Redis %s crashed by signal: %d, si_code: %d", REDIS_VERSION, sig, info->si_code);
    if (sig == SIGSEGV || sig == SIGBUS) {
        serverLog(LL_WARNING,
        "Accessing address: %p", (void*)info->si_addr);
    }
    if (info->si_code == SI_USER && info->si_pid != -1) {
        serverLog(LL_WARNING, "Killed by PID: %ld, UID: %d", (long) info->si_pid, info->si_uid);
    }

#ifdef HAVE_BACKTRACE
    ucontext_t *uc = (ucontext_t*) secret;
    void *eip = getAndSetMcontextEip(uc, NULL);
    if (eip != NULL) {
        serverLog(LL_WARNING,
        "Crashed running the instruction at: %p", eip);
    }

    if (eip == info->si_addr) {
        /* 当 eip 与错误地址匹配时，说明是在调用未映射的函数指针时崩溃。
         * 此时 backtrace() 会尝试访问该地址而再次崩溃，
         * 导致无法记录崩溃报告。
         * 将 eip 设置为一个有效的地址来避免此问题。 */

        /* 此技巧用于避免编译器警告 */
        void *ptr;
        invalidFunctionWasCalledType *ptr_ptr = (invalidFunctionWasCalledType*)&ptr;
        *ptr_ptr = invalidFunctionWasCalled;
        getAndSetMcontextEip(uc, ptr);
    }

    /* 输出精简崩溃信息时，仅打印当前线程的堆栈跟踪，
     * 以避免与多线程堆栈收集器产生竞态条件。 */
    logStackTrace(eip, 1, !print_full_crash_info);

    if (eip == info->si_addr) {
        /* Restore old eip */
        getAndSetMcontextEip(uc, eip);
    }

    logRegisters(uc);
#endif

    if (print_full_crash_info) printCrashReport();

#ifdef HAVE_BACKTRACE
    if (eip != NULL)
        dumpCodeAroundEIP(eip);
#endif

    bugReportEnd(1, sig);
}

/* setupDebugSigHandlers - 设置调试信号处理器（SIGSEGV 等和 SIGALRM） */
void setupDebugSigHandlers(void) {
    setupStacktracePipe();

    setupSigSegvHandler();

    struct sigaction act;

    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_SIGINFO;
    act.sa_sigaction = sigalrmSignalHandler;
    sigaction(SIGALRM, &act, NULL);
}

/* setupSigSegvHandler - 设置段错误信号处理器 */
void setupSigSegvHandler(void) {
    /* 初始化信号处理器锁。
     * 尝试初始化已初始化的 mutex 或 mutexattr 会导致未定义行为。 */
    if (!signal_handler_lock_initialized) {
        /* 设置信号处理器的错误检查属性。同一线程中重复加锁将报错。 */
        pthread_mutexattr_init(&signal_handler_lock_attr);
        pthread_mutexattr_settype(&signal_handler_lock_attr, PTHREAD_MUTEX_ERRORCHECK);
        pthread_mutex_init(&signal_handler_lock, &signal_handler_lock_attr);
        signal_handler_lock_initialized = 1;
    }

    struct sigaction act;

    sigemptyset(&act.sa_mask);
    /* SA_NODEFER: 禁止在进入信号处理器时将该信号添加到调用进程的信号掩码中，
     * 除非该信号包含在 sa_mask 字段中。 */
    /* SA_SIGINFO: 使用 sa_sigaction 中定义的函数作为处理器，
     * 而不是使用 sa_handler。 */
    act.sa_flags = SA_NODEFER | SA_SIGINFO;
    act.sa_sigaction = sigsegvHandler;
    if(server.crashlog_enabled) {
        sigaction(SIGSEGV, &act, NULL);
        sigaction(SIGBUS, &act, NULL);
        sigaction(SIGFPE, &act, NULL);
        sigaction(SIGILL, &act, NULL);
        sigaction(SIGABRT, &act, NULL);
    }
}

/* removeSigSegvHandlers - 移除段错误等信号处理器，恢复为默认行为 */
void removeSigSegvHandlers(void) {
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND;
    act.sa_handler = SIG_DFL;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
    sigaction(SIGABRT, &act, NULL);
}

/*
 * printCrashReport - 输出完整的崩溃报告。
 *
 * 依次记录以下信息：
 * 1. 服务器 INFO 和客户端列表
 * 2. 当前正在服务的客户端信息
 * 3. 模块信息
 * 4. 配置调试信息
 * 5. 快速内存测试结果
 */
void printCrashReport(void) {
    /* 记录 INFO 和 CLIENT LIST */
    logServerInfo();

    /* Log the current client */
    logCurrentClient(server.current_client, "CURRENT");
    logCurrentClient(server.executing_client, "EXECUTING");

    /* 记录模块信息。放在最后执行，因为它可能崩溃。 */
    logModulesInfo();

    /* 记录调试配置信息，这些值对调试崩溃可能有帮助 */
    logConfigDebugInfo();

    /* 运行内存测试，以防崩溃是由内存损坏引起的 */
    doFastMemoryTest();
}

/*
 * bugReportEnd - 结束 bug 报告并终止进程。
 *
 * 输出报告结束提示和 issue 上报链接。
 * 根据 killViaSignal 参数决定终止方式：
 * - 0: 使用 abort() 或 _exit()
 * - 1: 恢复信号默认处理器后发送信号（允许生成 core dump）
 */
void bugReportEnd(int killViaSignal, int sig) {
    struct sigaction act;

    serverLogRawFromHandler(LL_WARNING|LL_RAW,
"\n=== REDIS BUG REPORT END. Make sure to include from START to END. ===\n\n"
"       Please report the crash by opening an issue on github:\n\n"
"           http://github.com/redis/redis/issues\n\n"
"  If a Redis module was involved, please open in the module's repo instead.\n\n"
"  Suspect RAM error? Use redis-server --test-memory to verify it.\n\n"
"  Some other issues could be detected by redis-server --check-system\n"
);

    /* 不调用 free(messages)，因为内存可能已损坏 */
    if (server.daemonize && server.supervised == 0 && server.pidfile) unlink(server.pidfile);

    if (!killViaSignal) {
        /* 为避免 valgrind 问题，可能需要直接退出而不是生成信号 */
        if (server.use_exit_on_panic) {
             /* 使用 _exit 以绕过 gcc ASAN 的误报泄漏报告 */
             fflush(stdout);
            _exit(1);
        }
        abort();
    }

    /* 确保最终以正确的信号退出，这样如果启用了 core dump，会生成核心转储文件。 */
    sigemptyset (&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = SIG_DFL;
    sigaction (sig, &act, NULL);
    kill(getpid(),sig);
}

/* ======================== 调试用日志函数 =============================== */

/*
 * serverLogHexDump - 以十六进制格式输出内存内容。
 *
 * 将 value 指向的 len 字节数据以十六进制字符串形式记录到日志中。
 */
void serverLogHexDump(int level, char *descr, void *value, size_t len) {
    char buf[65], *b;
    unsigned char *v = value;
    char charset[] = "0123456789abcdef";

    serverLog(level,"%s (hexdump of %zu bytes):", descr, len);
    b = buf;
    while(len) {
        b[0] = charset[(*v)>>4];
        b[1] = charset[(*v)&0xf];
        b[2] = '\0';
        b += 2;
        len--;
        v++;
        if (b-buf == 64 || len == 0) {
            serverLogRaw(level|LL_RAW,buf);
            b = buf;
        }
    }
    serverLogRaw(level|LL_RAW,"\n");
}

/* ========================= 软件看门狗（Watchdog）========================= */
#include <sys/time.h>

/*
 * sigalrmSignalHandler - SIGALRM 信号处理器。
 *
 * 在软件看门狗超时时被调用，输出当前堆栈跟踪。
 * 也可通过 kill() 显式发送 SIGALRM 来获取堆栈跟踪。
 */
void sigalrmSignalHandler(int sig, siginfo_t *info, void *secret) {
#ifdef HAVE_BACKTRACE
    ucontext_t *uc = (ucontext_t*) secret;
#else
    (void)secret;
#endif
    UNUSED(sig);

    /* SIGALRM 可以通过 kill() 显式发送给进程以获取堆栈跟踪，
     * 也可以由看门狗定期发送。后一种情况下 si_pid 未设置。 */
    if(info->si_pid == 0) {
        serverLogRawFromHandler(LL_WARNING,"\n--- WATCHDOG TIMER EXPIRED ---");
    } else {
        serverLogRawFromHandler(LL_WARNING, "\nReceived SIGALRM");
    }
#ifdef HAVE_BACKTRACE
    logStackTrace(getAndSetMcontextEip(uc, NULL), 1, 0);
#else
    serverLogRawFromHandler(LL_WARNING,"Sorry: no support for backtrace().");
#endif
    serverLogRawFromHandler(LL_WARNING,"--------\n");
}

/*
 * watchdogScheduleSignal - 调度 SIGALRM 信号的发送。
 *
 * 在指定的毫秒数后发送 SIGALRM 信号。
 * 如果已有定时器，会重新调度到新的时间。
 * 如果 period 为 0，则禁用当前定时器。
 */
void watchdogScheduleSignal(int period) {
    struct itimerval it;

    /* 当 period 为 0 时会停止定时器 */
    it.it_value.tv_sec = period/1000;
    it.it_value.tv_usec = (period%1000)*1000;
    /* 不自动重复触发 */
    it.it_interval.tv_sec = 0;
    it.it_interval.tv_usec = 0;
    setitimer(ITIMER_REAL, &it, NULL);
}
/* applyWatchdogPeriod - 应用看门狗周期配置 */
void applyWatchdogPeriod(void) {
    /* 当周期为 0 时禁用看门狗 */
    if (server.watchdog_period == 0) {
        watchdogScheduleSignal(0); /* Stop the current timer. */
    } else {
        /* 如果配置的周期小于定时器周期的两倍，
         * 则对于软件看门狗来说太短了，无法可靠工作。
         * 如有必要，在此处修正。 */
        int min_period = (1000/server.hz)*2;
        if (server.watchdog_period < min_period) server.watchdog_period = min_period;
        watchdogScheduleSignal(server.watchdog_period); /* Adjust the current timer. */
    }
}

/*
 * debugDelay - 调试用延迟函数。
 *
 * 正数表示休眠时间（微秒）。
 * 负数表示微秒的分数，即 -10 表示 100 纳秒。
 *
 * 由于即使最短的休眠也会导致上下文切换和系统调用，
 * 实现短延迟的方式是通过概率性地减少休眠频率。
 */
void debugDelay(int usec) {
    /* 由于即使最短的休眠也会导致上下文切换和系统调用，
     * 通过概率性休眠来实现亚微秒级的延迟效果 */
    if (usec < 0) usec = (rand() % -usec) == 0 ? 1: 0;
    if (usec) usleep(usec);
}

#ifdef HAVE_BACKTRACE
#ifdef __linux__

/* ========================= 堆栈跟踪工具函数 ============================== */



/*
 * is_thread_ready_to_signal - 检查线程是否能接收指定信号。
 *
 * 如果线程既不阻塞也不忽略该信号，返回 1（线程可以处理信号）。
 * 如果线程阻塞或忽略了 sig_num，返回 0。
 * 如果发生错误，也返回 0 并记录警告日志。
 */
static int is_thread_ready_to_signal(const char *proc_pid_task_path, const char *tid, int sig_num) {
    /* 打开线程状态文件路径 /proc/<pid>/task/<tid>/status */
    char path_buff[PATH_MAX];
    snprintf_async_signal_safe(path_buff, PATH_MAX, "%s/%s/status", proc_pid_task_path, tid);

    int thread_status_file = open(path_buff, O_RDONLY);
    char buff[PATH_MAX];
    if (thread_status_file == -1) {
        serverLogFromHandler(LL_WARNING, "tid:%s: failed to open %s file", tid, path_buff);
        return 0;
    }

    int ret = 1;
    size_t field_name_len = strlen("SigBlk:\t"); /* SigIgn 的长度相同 */
    char *line = NULL;
    size_t fields_count = 2;
    while ((line = fgets_async_signal_safe(buff, PATH_MAX, thread_status_file)) && fields_count) {
        /* 遍历文件直到找到 SigBlk 或 SigIgn 字段行 */
        if (!strncmp(buff, "SigBlk:\t", field_name_len) ||  !strncmp(buff, "SigIgn:\t", field_name_len)) {
            line = buff + field_name_len;
            unsigned long sig_mask;
            if (-1 == string2ul_base16_async_signal_safe(line, sizeof(buff), &sig_mask)) {
                serverLogRawFromHandler(LL_WARNING, "Can't convert signal mask to an unsigned long due to an overflow");
                ret = 0;
                break;
            }

            /* 信号掩码中的位位置与信号编号对齐。由于信号编号从 1 开始，
             * 需要将信号编号减 1 以与零基索引正确对齐 */
            if (sig_mask & (1L << (sig_num - 1))) { /* 如果信号被阻塞或忽略则返回 0 */
                ret = 0;
                break;
            }
            --fields_count;
        }
    }

    close(thread_status_file);

    /* 如果到达 EOF，说明未找到 SigBlk 或/和 SigIgn 字段，说明出了问题 */
    if (line == NULL)  {
        ret = 0;
        serverLogFromHandler(LL_WARNING, "tid:%s: failed to find SigBlk or/and SigIgn field(s) in %s/%s/status file", tid, proc_pid_task_path, tid);
    }
    return ret;
}

/*
 * 使用 syscall(SYS_getdents64) 读取目录，与 opendir() 不同，
 * 该系统调用被认为是异步信号安全的（async-signal-safe）。
 *
 * glibc 2.30 开始支持 getdents64() 包装函数。
 * 为兼容更早版本的 glibc，直接使用 syscall(SYS_getdents64)，
 * 因此需要自行定义 linux_dirent64 结构体。
 * 此结构非常古老且稳定，除非内核选择破坏与所有已有二进制文件的
 * 兼容性（极不可能），否则不会改变。
 */
struct linux_dirent64 {
   unsigned long long d_ino;        /* inode 号 */
   long long d_off;                 /* 到下一个 dirent 的偏移 */
   unsigned short d_reclen;         /* 此 linux_dirent 的长度 */
   unsigned char  d_type;           /* 文件类型 */
   char           d_name[256];      /* 文件名（以 null 结尾） */
};

/*
 * get_ready_to_signal_threads_tids - 获取可以接收指定信号的线程 ID 列表。
 *
 * 将这些线程的 tid 写入 tids 数组。
 * 返回可接收信号的线程数量，失败时返回 0。
 */
static size_t get_ready_to_signal_threads_tids(int sig_num, pid_t tids[TIDS_MAX_SIZE]) {
    /* 打开 /proc/<pid>/task 目录 */
    char path_buff[PATH_MAX];
    snprintf_async_signal_safe(path_buff, PATH_MAX, "/proc/%d/task", getpid());

    int dir;
    if (-1 == (dir = open(path_buff,  O_RDONLY | O_DIRECTORY))) return 0;

    size_t tids_count = 0;
    pid_t calling_tid = syscall(SYS_gettid);
    int current_thread_index = -1;
    long nread;
    char buff[PATH_MAX];

    /* readdir() 不是异步信号安全的（AS-safe）。
     * 因此使用 SYS_getdents64 读取目录，该系统调用被认为是 AS 安全的 */
    while ((nread = syscall(SYS_getdents64, dir, buff, PATH_MAX))) {
        if (nread == -1) {
            close(dir);
            serverLogRawFromHandler(LL_WARNING, "get_ready_to_signal_threads_tids(): Failed to read the process's task directory");
            return 0;
        }
        /* 每个线程由一个目录表示 */
        for (long pos = 0; pos < nread;) {
            struct linux_dirent64 *entry = (struct linux_dirent64 *)(buff + pos);
            pos += entry->d_reclen;
            /* 跳过无关目录 */
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;

            /* 线程的目录名等同于其 tid */
           long tid;
           string2l(entry->d_name, strlen(entry->d_name), &tid);

            if(!is_thread_ready_to_signal(path_buff, entry->d_name, sig_num)) continue;

            if(tid == calling_tid) {
                current_thread_index = tids_count;
            }

            /* 保存线程 ID */
            tids[tids_count++] = tid;
            
            /* 达到最大线程数时停止 */
            if(tids_count == TIDS_MAX_SIZE) {
                serverLogRawFromHandler(LL_WARNING, "get_ready_to_signal_threads_tids(): Reached the limit of the tids buffer.");
                break;
            }
        }

        if(tids_count == TIDS_MAX_SIZE) break;
    }

    /* 将当前线程的 tid 交换到数组末尾（使其最后被处理） */
    if(current_thread_index != -1) {
        pid_t last_tid = tids[tids_count - 1];

        tids[tids_count - 1] = calling_tid;
        tids[current_thread_index] = last_tid;
    }

    close(dir);

    return tids_count;
}
#endif /* __linux__ */
#endif /* HAVE_BACKTRACE */
