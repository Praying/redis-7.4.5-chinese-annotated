/*
 * Copyright (c) 2011-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

/*
 * 本文件实现了 Redis Functions 子系统。
 * 主要功能包括：
 * - 函数库（library）和引擎（engine）的注册与管理
 * - 函数库上下文（functionsLibCtx）的生命周期管理
 * - 实现所有 FUNCTION 子命令：
 *   FUNCTION LOAD    - 加载函数库
 *   FUNCTION DELETE  - 删除函数库
 *   FUNCTION LIST    - 列出函数库信息
 *   FUNCTION STATS   - 查看运行中的函数统计
 *   FUNCTION FLUSH   - 清空所有函数库
 *   FUNCTION DUMP    - 序列化所有函数库
 *   FUNCTION RESTORE - 从序列化数据恢复函数库
 *   FUNCTION KILL    - 终止正在运行的函数
 *   FUNCTION HELP    - 显示帮助信息
 *   FCALL / FCALL_RO - 调用函数
 */

#include "functions.h"
#include "sds.h"
#include "dict.h"
#include "adlist.h"
#include "atomicvar.h"

/* 函数加载超时时间（毫秒） */
#define LOAD_TIMEOUT_MS 500

/* 恢复策略枚举 */
typedef enum {
    restorePolicy_Flush,    /* 清空已有库后再恢复 */
    restorePolicy_Append,   /* 追加模式，冲突则中止 */
    restorePolicy_Replace   /* 追加模式，冲突则替换 */
} restorePolicy;

/* 引擎缓存的内存开销（结构体、字典等元数据） */
static size_t engine_cache_memory = 0;

/* 前向声明 */
static void engineFunctionDispose(dict *d, void *obj);
static void engineStatsDispose(dict *d, void *obj);
static void engineLibraryDispose(dict *d, void *obj);
static void engineDispose(dict *d, void *obj);
static int functionsVerifyName(sds name);

/* 每个引擎的统计信息 */
typedef struct functionsLibEngineStats {
    size_t n_lib;        /* 该引擎下的函数库数量 */
    size_t n_functions;  /* 该引擎下的函数数量 */
} functionsLibEngineStats;

/* 函数库上下文，管理所有已注册的函数库 */
struct functionsLibCtx {
    dict *libraries;     /* 函数库名 -> 函数库对象 */
    dict *functions;     /* 函数名 -> 函数对象（可用于执行） */
    size_t cache_memory; /* 所有函数的额外内存开销（结构体、字典等） */
    dict *engines_stats; /* 每个引擎的统计信息 */
};

/* 函数库元数据（从代码 shebang 行解析） */
typedef struct functionsLibMataData {
    sds engine;  /* 引擎名称 */
    sds name;    /* 函数库名称 */
    sds code;    /* 函数库代码（去除 shebang 行后） */
} functionsLibMataData;

/* 引擎字典类型（key 不区分大小写） */
dictType engineDictType = {
        dictSdsCaseHash,       /* 哈希函数 */
        dictSdsDup,            /* key 复制 */
        NULL,                  /* val 复制 */
        dictSdsKeyCaseCompare, /* key 比较（不区分大小写） */
        dictSdsDestructor,     /* key 析构 */
        engineDispose,         /* val 析构 */
        NULL                   /* 允许扩展 */
};

/* 函数字典类型（key 不区分大小写） */
dictType functionDictType = {
        dictSdsCaseHash,      /* 哈希函数 */
        dictSdsDup,           /* key 复制 */
        NULL,                 /* val 复制 */
        dictSdsKeyCaseCompare,/* key 比较（不区分大小写） */
        dictSdsDestructor,    /* key 析构 */
        NULL,                 /* val 析构 */
        NULL                  /* 允许扩展 */
};

/* 引擎统计字典类型（key 不区分大小写） */
dictType engineStatsDictType = {
        dictSdsCaseHash,      /* 哈希函数 */
        dictSdsDup,           /* key 复制 */
        NULL,                 /* val 复制 */
        dictSdsKeyCaseCompare,/* key 比较（不区分大小写） */
        dictSdsDestructor,    /* key 析构 */
        engineStatsDispose,   /* val 析构 */
        NULL                  /* 允许扩展 */
};

/* 库内函数字典类型（key 区分大小写） */
dictType libraryFunctionDictType = {
        dictSdsHash,          /* 哈希函数 */
        dictSdsDup,           /* key 复制 */
        NULL,                 /* val 复制 */
        dictSdsKeyCompare,    /* key 比较（区分大小写） */
        dictSdsDestructor,    /* key 析构 */
        engineFunctionDispose,/* val 析构 */
        NULL                  /* 允许扩展 */
};

/* 函数库字典类型（key 区分大小写） */
dictType librariesDictType = {
        dictSdsHash,          /* 哈希函数 */
        dictSdsDup,           /* key 复制 */
        NULL,                 /* val 复制 */
        dictSdsKeyCompare,    /* key 比较（区分大小写） */
        dictSdsDestructor,    /* key 析构 */
        engineLibraryDispose, /* val 析构 */
        NULL                  /* 允许扩展 */
};

/* 引擎字典：engine_name -> engineInfo */
static dict *engines = NULL;

/* 当前函数库上下文 */
static functionsLibCtx *curr_functions_lib_ctx = NULL;

/* 计算函数对象的内存占用 */
static size_t functionMallocSize(functionInfo *fi) {
    return zmalloc_size(fi) + sdsZmallocSize(fi->name)
            + (fi->desc ? sdsZmallocSize(fi->desc) : 0)
            + fi->li->ei->engine->get_function_memory_overhead(fi->function);
}

/* 计算函数库对象的内存占用 */
static size_t libraryMallocSize(functionLibInfo *li) {
    return zmalloc_size(li) + sdsZmallocSize(li->name)
            + sdsZmallocSize(li->code);
}

/* 释放引擎统计信息 */
static void engineStatsDispose(dict *d, void *obj) {
    UNUSED(d);
    functionsLibEngineStats *stats = obj;
    zfree(stats);
}

/* 释放函数内存 */
static void engineFunctionDispose(dict *d, void *obj) {
    UNUSED(d);
    if (!obj) {
        return;
    }
    functionInfo *fi = obj;
    sdsfree(fi->name);
    if (fi->desc) {
        sdsfree(fi->desc);
    }
    engine *engine = fi->li->ei->engine;
    engine->free_function(engine->engine_ctx, fi->function);
    zfree(fi);
}

/* 释放函数库及其所有资源 */
static void engineLibraryFree(functionLibInfo* li) {
    if (!li) {
        return;
    }
    dictRelease(li->functions);
    sdsfree(li->name);
    sdsfree(li->code);
    zfree(li);
}

/* 字典释放回调，释放函数库 */
static void engineLibraryDispose(dict *d, void *obj) {
    UNUSED(d);
    engineLibraryFree(obj);
}

/* 字典释放回调，释放引擎及其关联资源 */
static void engineDispose(dict *d, void *obj) {
    UNUSED(d);
    engineInfo *ei = obj;
    freeClient(ei->c);
    sdsfree(ei->name);
    ei->engine->free_ctx(ei->engine->engine_ctx);
    zfree(ei->engine);
    zfree(ei);
}

/* 清空给定函数库上下文中的所有函数和库 */
void functionsLibCtxClear(functionsLibCtx *lib_ctx) {
    dictEmpty(lib_ctx->functions, NULL);
    dictEmpty(lib_ctx->libraries, NULL);
    dictIterator *iter = dictGetIterator(lib_ctx->engines_stats);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionsLibEngineStats *stats = dictGetVal(entry);
        stats->n_functions = 0;
        stats->n_lib = 0;
    }
    dictReleaseIterator(iter);
    lib_ctx->cache_memory = 0;
}

/* 清除当前函数库上下文（支持同步/异步） */
void functionsLibCtxClearCurrent(int async) {
    if (async) {
        functionsLibCtx *old_l_ctx = curr_functions_lib_ctx;
        dict *old_engines = engines;
        freeFunctionsAsync(old_l_ctx, old_engines);
    } else {
        functionsLibCtxFree(curr_functions_lib_ctx);
        dictRelease(engines);
    }
    functionsInit();
}

/* 释放给定的函数库上下文及其所有资源 */
void functionsLibCtxFree(functionsLibCtx *functions_lib_ctx) {
    functionsLibCtxClear(functions_lib_ctx);
    dictRelease(functions_lib_ctx->functions);
    dictRelease(functions_lib_ctx->libraries);
    dictRelease(functions_lib_ctx->engines_stats);
    zfree(functions_lib_ctx);
}

/* 将当前函数库上下文替换为给定的新上下文，并释放旧上下文 */
void functionsLibCtxSwapWithCurrent(functionsLibCtx *new_lib_ctx) {
    functionsLibCtxFree(curr_functions_lib_ctx);
    curr_functions_lib_ctx = new_lib_ctx;
}

/* 返回当前函数库上下文 */
functionsLibCtx* functionsLibCtxGetCurrent(void) {
    return curr_functions_lib_ctx;
}

/* 创建新的函数库上下文 */
functionsLibCtx* functionsLibCtxCreate(void) {
    functionsLibCtx *ret = zmalloc(sizeof(functionsLibCtx));
    ret->libraries = dictCreate(&librariesDictType);
    ret->functions = dictCreate(&functionDictType);
    ret->engines_stats = dictCreate(&engineStatsDictType);
    dictIterator *iter = dictGetIterator(engines);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        engineInfo *ei = dictGetVal(entry);
        functionsLibEngineStats *stats = zcalloc(sizeof(*stats));
        dictAdd(ret->engines_stats, ei->name, stats);
    }
    dictReleaseIterator(iter);
    ret->cache_memory = 0;
    return ret;
}

/*
 * 在给定的函数库中创建一个函数。
 * 成功返回 C_OK。
 * 失败返回 C_ERR，并通过 err 输出参数设置相关错误信息。
 *
 * 注意：代码假设 'name' 以 NULL 结尾，但不要求二进制安全。
 *       函数会验证名称是否符合命名格式，不符合则返回错误。
 */
int functionLibCreateFunction(sds name, void *function, functionLibInfo *li, sds desc, uint64_t f_flags, sds *err) {
    if (functionsVerifyName(name) != C_OK) {
        *err = sdsnew("Library names can only contain letters, numbers, or underscores(_) and must be at least one character long");
        return C_ERR;
    }

    if (dictFetchValue(li->functions, name)) {
        *err = sdsnew("Function already exists in the library");
        return C_ERR;
    }

    functionInfo *fi = zmalloc(sizeof(*fi));
    *fi = (functionInfo) {
        .name = name,
        .function = function,
        .li = li,
        .desc = desc,
        .f_flags = f_flags,
    };

    int res = dictAdd(li->functions, fi->name, fi);
    serverAssert(res == DICT_OK);

    return C_OK;
}

/* 创建新的函数库信息对象 */
static functionLibInfo* engineLibraryCreate(sds name, engineInfo *ei, sds code) {
    functionLibInfo *li = zmalloc(sizeof(*li));
    *li = (functionLibInfo) {
        .name = sdsdup(name),
        .functions = dictCreate(&libraryFunctionDictType),
        .ei = ei,
        .code = sdsdup(code),
    };
    return li;
}

/* 从上下文中取消链接函数库，更新统计信息 */
static void libraryUnlink(functionsLibCtx *lib_ctx, functionLibInfo* li) {
    dictIterator *iter = dictGetIterator(li->functions);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionInfo *fi = dictGetVal(entry);
        int ret = dictDelete(lib_ctx->functions, fi->name);
        serverAssert(ret == DICT_OK);
        lib_ctx->cache_memory -= functionMallocSize(fi);
    }
    dictReleaseIterator(iter);
    entry = dictUnlink(lib_ctx->libraries, li->name);
    dictSetVal(lib_ctx->libraries, entry, NULL);
    dictFreeUnlinkedEntry(lib_ctx->libraries, entry);
    lib_ctx->cache_memory -= libraryMallocSize(li);

    /* 更新引擎统计 */
    functionsLibEngineStats *stats = dictFetchValue(lib_ctx->engines_stats, li->ei->name);
    serverAssert(stats);
    stats->n_lib--;
    stats->n_functions -= dictSize(li->functions);
}

/* 将函数库链接到上下文中，更新统计信息 */
static void libraryLink(functionsLibCtx *lib_ctx, functionLibInfo* li) {
    dictIterator *iter = dictGetIterator(li->functions);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionInfo *fi = dictGetVal(entry);
        dictAdd(lib_ctx->functions, fi->name, fi);
        lib_ctx->cache_memory += functionMallocSize(fi);
    }
    dictReleaseIterator(iter);

    dictAdd(lib_ctx->libraries, li->name, li);
    lib_ctx->cache_memory += libraryMallocSize(li);

    /* 更新引擎统计 */
    functionsLibEngineStats *stats = dictFetchValue(lib_ctx->engines_stats, li->ei->name);
    serverAssert(stats);
    stats->n_lib++;
    stats->n_functions += dictSize(li->functions);
}

/* 将 lib_ctx_src 中的所有函数库合并到 lib_ctx_dst 中。
 * 若发生冲突，当 'replace' 参数为 true 时，用新库替换旧库；
 * 否则中止操作，lib_ctx_dst 和 lib_ctx_src 保持不变。
 * 成功返回 C_OK，中止返回 C_ERR 并通过 'err' 参数设置错误信息。
 */
static int libraryJoin(functionsLibCtx *functions_lib_ctx_dst, functionsLibCtx *functions_lib_ctx_src, int replace, sds *err) {
    int ret = C_ERR;
    dictIterator *iter = NULL;
    /* 存储需要替换的旧库（用于回滚）。
     * 仅在需要时初始化 */
    list *old_libraries_list = NULL;
    dictEntry *entry = NULL;
    iter = dictGetIterator(functions_lib_ctx_src->libraries);
    while ((entry = dictNext(iter))) {
        functionLibInfo *li = dictGetVal(entry);
        functionLibInfo *old_li = dictFetchValue(functions_lib_ctx_dst->libraries, li->name);
        if (old_li) {
            if (!replace) {
                /* 函数库已存在，恢复失败 */
                *err = sdscatfmt(sdsempty(), "Library %s already exists", li->name);
                goto done;
            } else {
                if (!old_libraries_list) {
                    old_libraries_list = listCreate();
                    listSetFreeMethod(old_libraries_list, (void (*)(void*))engineLibraryFree);
                }
                libraryUnlink(functions_lib_ctx_dst, old_li);
                listAddNodeTail(old_libraries_list, old_li);
            }
        }
    }
    dictReleaseIterator(iter);
    iter = NULL;

    /* 确保没有函数名冲突 */
    iter = dictGetIterator(functions_lib_ctx_src->functions);
    while ((entry = dictNext(iter))) {
        functionInfo *fi = dictGetVal(entry);
        if (dictFetchValue(functions_lib_ctx_dst->functions, fi->name)) {
            *err = sdscatfmt(sdsempty(), "Function %s already exists", fi->name);
            goto done;
        }
    }
    dictReleaseIterator(iter);
    iter = NULL;

    /* 无冲突，安全链接所有新函数库 */
    iter = dictGetIterator(functions_lib_ctx_src->libraries);
    while ((entry = dictNext(iter))) {
        functionLibInfo *li = dictGetVal(entry);
        libraryLink(functions_lib_ctx_dst, li);
        dictSetVal(functions_lib_ctx_src->libraries, entry, NULL);
    }
    dictReleaseIterator(iter);
    iter = NULL;

    functionsLibCtxClear(functions_lib_ctx_src);
    if (old_libraries_list) {
        listRelease(old_libraries_list);
        old_libraries_list = NULL;
    }
    ret = C_OK;

done:
    if (iter) dictReleaseIterator(iter);
    if (old_libraries_list) {
        /* 回滚：重新链接所有旧函数库 */
        while (listLength(old_libraries_list) > 0) {
            listNode *head = listFirst(old_libraries_list);
            functionLibInfo *li = listNodeValue(head);
            listNodeValue(head) = NULL;
            libraryLink(functions_lib_ctx_dst, li);
            listDelNode(old_libraries_list, head);
        }
        listRelease(old_libraries_list);
    }
    return ret;
}

/* 注册引擎，应在引擎启动时调用一次，参数如下：
 *
 * - engine_name - 要注册的引擎名称
 * - engine_ctx  - Redis 用于与引擎交互的引擎上下文 */
int functionsRegisterEngine(const char *engine_name, engine *engine) {
    sds engine_name_sds = sdsnew(engine_name);
    if (dictFetchValue(engines, engine_name_sds)) {
        serverLog(LL_WARNING, "Same engine was registered twice");
        sdsfree(engine_name_sds);
        return C_ERR;
    }

    client *c = createClient(NULL);
    c->flags |= (CLIENT_DENY_BLOCKING | CLIENT_SCRIPT);
    engineInfo *ei = zmalloc(sizeof(*ei));
    *ei = (engineInfo ) { .name = engine_name_sds, .engine = engine, .c = c,};

    dictAdd(engines, engine_name_sds, ei);

    engine_cache_memory += zmalloc_size(ei) + sdsZmallocSize(ei->name) +
            zmalloc_size(engine) +
            engine->get_engine_memory_overhead(engine->engine_ctx);

    return C_OK;
}

/*
 * FUNCTION STATS 命令实现
 * 返回当前正在运行的函数信息及各引擎的统计信息
 */
void functionStatsCommand(client *c) {
    if (scriptIsRunning() && scriptIsEval()) {
        addReplyErrorObject(c, shared.slowevalerr);
        return;
    }

    addReplyMapLen(c, 2);

    addReplyBulkCString(c, "running_script");
    if (!scriptIsRunning()) {
        addReplyNull(c);
    } else {
        addReplyMapLen(c, 3);
        addReplyBulkCString(c, "name");
        addReplyBulkCString(c, scriptCurrFunction());
        addReplyBulkCString(c, "command");
        client *script_client = scriptGetCaller();
        addReplyArrayLen(c, script_client->argc);
        for (int i = 0 ; i < script_client->argc ; ++i) {
            addReplyBulkCBuffer(c, script_client->argv[i]->ptr, sdslen(script_client->argv[i]->ptr));
        }
        addReplyBulkCString(c, "duration_ms");
        addReplyLongLong(c, scriptRunDuration());
    }

    addReplyBulkCString(c, "engines");
    addReplyMapLen(c, dictSize(engines));
    dictIterator *iter = dictGetIterator(engines);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        engineInfo *ei = dictGetVal(entry);
        addReplyBulkCString(c, ei->name);
        addReplyMapLen(c, 2);
        functionsLibEngineStats *e_stats = dictFetchValue(curr_functions_lib_ctx->engines_stats, ei->name);
        addReplyBulkCString(c, "libraries_count");
        addReplyLongLong(c, e_stats->n_lib);
        addReplyBulkCString(c, "functions_count");
        addReplyLongLong(c, e_stats->n_functions);
    }
    dictReleaseIterator(iter);
}

/* 构建函数标志的回复 */
static void functionListReplyFlags(client *c, functionInfo *fi) {
    /* 首先计算标志数量 */
    int flagcount = 0;
    for (scriptFlag *flag = scripts_flags_def; flag->str ; ++flag) {
        if (fi->f_flags & flag->flag) {
            ++flagcount;
        }
    }

    addReplySetLen(c, flagcount);

    for (scriptFlag *flag = scripts_flags_def; flag->str ; ++flag) {
        if (fi->f_flags & flag->flag) {
            addReplyStatus(c, flag->str);
        }
    }
}

/*
 * FUNCTION LIST [LIBRARYNAME PATTERN] [WITHCODE]
 *
 * 返回所有函数库的概要信息：
 * * 函数库名称
 * * 运行函数库所使用的引擎
 * * 函数列表
 * * 函数库代码（如果指定了 WITHCODE）
 *
 * 也可通过 LIBRARYNAME 参数指定函数库名称模式，
 * 仅返回匹配该模式的函数库。
 */
void functionListCommand(client *c) {
    int with_code = 0;
    sds library_name = NULL;
    for (int i = 2 ; i < c->argc ; ++i) {
        robj *next_arg = c->argv[i];
        if (!with_code && !strcasecmp(next_arg->ptr, "withcode")) {
            with_code = 1;
            continue;
        }
        if (!library_name && !strcasecmp(next_arg->ptr, "libraryname")) {
            if (i >= c->argc - 1) {
                addReplyError(c, "library name argument was not given");
                return;
            }
            library_name = c->argv[++i]->ptr;
            continue;
        }
        addReplyErrorSds(c, sdscatfmt(sdsempty(), "Unknown argument %s", next_arg->ptr));
        return;
    }
    size_t reply_len = 0;
    void *len_ptr = NULL;
    if (library_name) {
        len_ptr = addReplyDeferredLen(c);
    } else {
        /* 未指定模式时，已知回复长度，直接设置 */
        addReplyArrayLen(c, dictSize(curr_functions_lib_ctx->libraries));
    }
    dictIterator *iter = dictGetIterator(curr_functions_lib_ctx->libraries);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionLibInfo *li = dictGetVal(entry);
        if (library_name) {
            if (!stringmatchlen(library_name, sdslen(library_name), li->name, sdslen(li->name), 1)) {
                continue;
            }
        }
        ++reply_len;
        addReplyMapLen(c, with_code? 4 : 3);
        addReplyBulkCString(c, "library_name");
        addReplyBulkCBuffer(c, li->name, sdslen(li->name));
        addReplyBulkCString(c, "engine");
        addReplyBulkCBuffer(c, li->ei->name, sdslen(li->ei->name));

        addReplyBulkCString(c, "functions");
        addReplyArrayLen(c, dictSize(li->functions));
        dictIterator *functions_iter = dictGetIterator(li->functions);
        dictEntry *function_entry = NULL;
        while ((function_entry = dictNext(functions_iter))) {
            functionInfo *fi = dictGetVal(function_entry);
            addReplyMapLen(c, 3);
            addReplyBulkCString(c, "name");
            addReplyBulkCBuffer(c, fi->name, sdslen(fi->name));
            addReplyBulkCString(c, "description");
            if (fi->desc) {
                addReplyBulkCBuffer(c, fi->desc, sdslen(fi->desc));
            } else {
                addReplyNull(c);
            }
            addReplyBulkCString(c, "flags");
            functionListReplyFlags(c, fi);
        }
        dictReleaseIterator(functions_iter);

        if (with_code) {
            addReplyBulkCString(c, "library_code");
            addReplyBulkCBuffer(c, li->code, sdslen(li->code));
        }
    }
    dictReleaseIterator(iter);
    if (len_ptr) {
        setDeferredArrayLen(c, len_ptr, reply_len);
    }
}

/*
 * FUNCTION DELETE <LIBRARY NAME>
 * 删除指定的函数库
 */
void functionDeleteCommand(client *c) {
    robj *function_name = c->argv[2];
    functionLibInfo *li = dictFetchValue(curr_functions_lib_ctx->libraries, function_name->ptr);
    if (!li) {
        addReplyError(c, "Library not found");
        return;
    }

    libraryUnlink(curr_functions_lib_ctx, li);
    engineLibraryFree(li);
    /* 标记数据已变更，用于复制和持久化 */
    server.dirty++;
    addReply(c, shared.ok);
}

/* FUNCTION KILL - 终止正在运行的函数 */
void functionKillCommand(client *c) {
    scriptKill(c, 0);
}

/* 尝试提取命令标志，返回修改后的标志。
 * 注意：不保证命令参数的正确性。 */
uint64_t fcallGetCommandFlags(client *c, uint64_t cmd_flags) {
    robj *function_name = c->argv[1];
    c->cur_script = dictFind(curr_functions_lib_ctx->functions, function_name->ptr);
    if (!c->cur_script)
        return cmd_flags;
    functionInfo *fi = dictGetVal(c->cur_script);
    uint64_t script_flags = fi->f_flags;
    return scriptFlagsToCmdFlags(cmd_flags, script_flags);
}

/* FCALL 通用实现，ro 为 1 时表示只读模式 */
static void fcallCommandGeneric(client *c, int ro) {
    /* 函数需要在其执行的命令之前发送给监控器 */
    replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);

    robj *function_name = c->argv[1];
    dictEntry *de = c->cur_script;
    if (!de)
        de = dictFind(curr_functions_lib_ctx->functions, function_name->ptr);
    if (!de) {
        addReplyError(c, "Function not found");
        return;
    }
    functionInfo *fi = dictGetVal(de);
    engine *engine = fi->li->ei->engine;

    long long numkeys;
    /* 获取作为 key 的参数数量 */
    if (getLongLongFromObject(c->argv[2], &numkeys) != C_OK) {
        addReplyError(c, "Bad number of keys provided");
        return;
    }
    if (numkeys > (c->argc - 3)) {
        addReplyError(c, "Number of keys can't be greater than number of args");
        return;
    } else if (numkeys < 0) {
        addReplyError(c, "Number of keys can't be negative");
        return;
    }

    scriptRunCtx run_ctx;

    if (scriptPrepareForRun(&run_ctx, fi->li->ei->c, c, fi->name, fi->f_flags, ro) != C_OK)
        return;

    engine->call(&run_ctx, engine->engine_ctx, fi->function, c->argv + 3, numkeys,
                 c->argv + 3 + numkeys, c->argc - 3 - numkeys);
    scriptResetRun(&run_ctx);
}

/*
 * FCALL <FUNCTION NAME> nkeys <key1 .. keyn> <arg1 .. argn>
 * 调用函数（可读写）
 */
void fcallCommand(client *c) {
    fcallCommandGeneric(c, 0);
}

/*
 * FCALL_RO <FUNCTION NAME> nkeys <key1 .. keyn> <arg1 .. argn>
 * 调用函数（只读模式）
 */
void fcallroCommand(client *c) {
    fcallCommandGeneric(c, 1);
}

/*
 * FUNCTION DUMP
 *
 * 返回表示所有函数库的二进制载荷，可通过 FUNCTION RESTORE 加载。
 *
 * 载荷结构与 RDB 格式相同，每个函数库单独保存，包含以下信息：
 * * 函数库名称
 * * 引擎名称
 * * 函数库代码
 * 每个函数库前保存 RDB_OPCODE_FUNCTION2 标识。
 * 载荷末尾保存 RDB 版本号和 crc64 校验和：
 * - RDB 版本号用于向后兼容
 * - crc64 用于验证载荷内容完整性
 */
void functionDumpCommand(client *c) {
    unsigned char buf[2];
    uint64_t crc;
    rio payload;
    rioInitWithBuffer(&payload, sdsempty());

    rdbSaveFunctions(&payload);

    /* RDB 版本号 */
    buf[0] = RDB_VERSION & 0xff;
    buf[1] = (RDB_VERSION >> 8) & 0xff;
    payload.io.buffer.ptr = sdscatlen(payload.io.buffer.ptr, buf, 2);

    /* CRC64 校验和 */
    crc = crc64(0, (unsigned char*) payload.io.buffer.ptr,
                sdslen(payload.io.buffer.ptr));
    memrev64ifbe(&crc);
    payload.io.buffer.ptr = sdscatlen(payload.io.buffer.ptr, &crc, 8);

    addReplyBulkSds(c, payload.io.buffer.ptr);
}

/*
 * FUNCTION RESTORE <payload> [FLUSH|APPEND|REPLACE]
 *
 * 从给定的载荷恢复函数库。
 * 恢复策略用于控制如何处理已有函数库（默认 APPEND）：
 * * FLUSH:    删除所有已有函数库后恢复。
 * * APPEND:   将恢复的函数库追加到已有库中，冲突则中止。
 * * REPLACE:  将恢复的函数库追加到已有库中，冲突则替换旧库。
 */
void functionRestoreCommand(client *c) {
    if (c->argc > 4) {
        addReplySubcommandSyntaxError(c);
        return;
    }

    restorePolicy restore_replicy = restorePolicy_Append; /* 默认策略：APPEND */
    sds data = c->argv[2]->ptr;
    size_t data_len = sdslen(data);
    rio payload;
    sds err = NULL;

    if (c->argc == 4) {
        const char *restore_policy_str = c->argv[3]->ptr;
        if (!strcasecmp(restore_policy_str, "append")) {
            restore_replicy = restorePolicy_Append;
        } else if (!strcasecmp(restore_policy_str, "replace")) {
            restore_replicy = restorePolicy_Replace;
        } else if (!strcasecmp(restore_policy_str, "flush")) {
            restore_replicy = restorePolicy_Flush;
        } else {
            addReplyError(c, "Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.");
            return;
        }
    }

    uint16_t rdbver;
    if (verifyDumpPayload((unsigned char*)data, data_len, &rdbver) != C_OK) {
        addReplyError(c, "DUMP payload version or checksum are wrong");
        return;
    }

    functionsLibCtx *functions_lib_ctx = functionsLibCtxCreate();
    rioInitWithBuffer(&payload, data);

    /* 读取直到最后 10 字节（包含 RDB 版本号和校验和） */
    while (data_len - payload.io.buffer.pos > 10) {
        int type;
        if ((type = rdbLoadType(&payload)) == -1) {
            err = sdsnew("can not read data type");
            goto load_error;
        }
        if (type == RDB_OPCODE_FUNCTION_PRE_GA) {
            err = sdsnew("Pre-GA function format not supported");
            goto load_error;
        }
        if (type != RDB_OPCODE_FUNCTION2) {
            err = sdsnew("given type is not a function");
            goto load_error;
        }
        if (rdbFunctionLoad(&payload, rdbver, functions_lib_ctx, RDBFLAGS_NONE, &err) != C_OK) {
            if (!err) {
                err = sdsnew("failed loading the given functions payload");
            }
            goto load_error;
        }
    }

    if (restore_replicy == restorePolicy_Flush) {
        functionsLibCtxSwapWithCurrent(functions_lib_ctx);
        functions_lib_ctx = NULL; /* 避免在最后释放上下文 */
    } else {
        if (libraryJoin(curr_functions_lib_ctx, functions_lib_ctx, restore_replicy == restorePolicy_Replace, &err) != C_OK) {
            goto load_error;
        }
    }

    /* 标记数据已变更，用于复制和持久化 */
    server.dirty++;

load_error:
    if (err) {
        addReplyErrorSds(c, err);
    } else {
        addReply(c, shared.ok);
    }
    if (functions_lib_ctx) {
        functionsLibCtxFree(functions_lib_ctx);
    }
}

/* FUNCTION FLUSH [ASYNC | SYNC] 命令实现 */
void functionFlushCommand(client *c) {
    if (c->argc > 3) {
        addReplySubcommandSyntaxError(c);
        return;
    }
    int async = 0;
    if (c->argc == 3 && !strcasecmp(c->argv[2]->ptr,"sync")) {
        async = 0;
    } else if (c->argc == 3 && !strcasecmp(c->argv[2]->ptr,"async")) {
        async = 1;
    } else if (c->argc == 2) {
        async = server.lazyfree_lazy_user_flush ? 1 : 0;
    } else {
        addReplyError(c,"FUNCTION FLUSH only supports SYNC|ASYNC option");
        return;
    }

    functionsLibCtxClearCurrent(async);

    /* 标记数据已变更，用于复制和持久化 */
    server.dirty++;
    addReply(c,shared.ok);
}

/* FUNCTION HELP - 显示 FUNCTION 子命令的帮助信息 */
void functionHelpCommand(client *c) {
    const char *help[] = {
"LOAD [REPLACE] <FUNCTION CODE>",
"    Create a new library with the given library name and code.",
"DELETE <LIBRARY NAME>",
"    Delete the given library.",
"LIST [LIBRARYNAME PATTERN] [WITHCODE]",
"    Return general information on all the libraries:",
"    * Library name",
"    * The engine used to run the Library",
"    * Functions list",
"    * Library code (if WITHCODE is given)",
"    It also possible to get only function that matches a pattern using LIBRARYNAME argument.",
"STATS",
"    Return information about the current function running:",
"    * Function name",
"    * Command used to run the function",
"    * Duration in MS that the function is running",
"    If no function is running, return nil",
"    In addition, returns a list of available engines.",
"KILL",
"    Kill the current running function.",
"FLUSH [ASYNC|SYNC]",
"    Delete all the libraries.",
"    When called without the optional mode argument, the behavior is determined by the",
"    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
"    * ASYNC: Asynchronously flush the libraries.",
"    * SYNC: Synchronously flush the libraries.",
"DUMP",
"    Return a serialized payload representing the current libraries, can be restored using FUNCTION RESTORE command",
"RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]",
"    Restore the libraries represented by the given payload, it is possible to give a restore policy to",
"    control how to handle existing libraries (default APPEND):",
"    * FLUSH: delete all existing libraries.",
"    * APPEND: appends the restored libraries to the existing libraries. On collision, abort.",
"    * REPLACE: appends the restored libraries to the existing libraries, On collision, replace the old",
"      libraries with the new libraries (notice that even on this option there is a chance of failure",
"      in case of functions name collision with another library).",
NULL };
    addReplyHelp(c, help);
}

/* 验证函数名称格式：仅允许 [a-zA-Z0-9_] 且至少一个字符 */
static int functionsVerifyName(sds name) {
    if (sdslen(name) == 0) {
        return C_ERR;
    }
    for (size_t i = 0 ; i < sdslen(name) ; ++i) {
        char curr_char = name[i];
        if ((curr_char >= 'a' && curr_char <= 'z') ||
            (curr_char >= 'A' && curr_char <= 'Z') ||
            (curr_char >= '0' && curr_char <= '9') ||
            (curr_char == '_'))
        {
            continue;
        }
        return C_ERR;
    }
    return C_OK;
}

/*
 * 从函数库代码负载中提取元数据（shebang 行）。
 * 解析 "#!<engine> name=<libname>" 格式的元数据行，
 * 并将剩余代码存入 md->code。
 * 成功返回 C_OK，失败返回 C_ERR 并设置 err。
 */
int functionExtractLibMetaData(sds payload, functionsLibMataData *md, sds *err) {
    sds name = NULL;
    sds engine = NULL;
    if (strncmp(payload, "#!", 2) != 0) {
        *err = sdsnew("Missing library metadata");
        return C_ERR;
    }
    char *shebang_end = strchr(payload, '\n');
    if (shebang_end == NULL) {
        *err = sdsnew("Invalid library metadata");
        return C_ERR;
    }
    size_t shebang_len = shebang_end - payload;
    sds shebang = sdsnewlen(payload, shebang_len);
    int numparts;
    sds *parts = sdssplitargs(shebang, &numparts);
    sdsfree(shebang);
    if (!parts || numparts == 0) {
        *err = sdsnew("Invalid library metadata");
        sdsfreesplitres(parts, numparts);
        return C_ERR;
    }
    engine = sdsdup(parts[0]);
    sdsrange(engine, 2, -1);
    for (int i = 1 ; i < numparts ; ++i) {
        sds part = parts[i];
        if (strncasecmp(part, "name=", 5) == 0) {
            if (name) {
                *err = sdscatfmt(sdsempty(), "Invalid metadata value, name argument was given multiple times");
                goto error;
            }
            name = sdsdup(part);
            sdsrange(name, 5, -1);
            continue;
        }
        *err = sdscatfmt(sdsempty(), "Invalid metadata value given: %s", part);
        goto error;
    }

    if (!name) {
        *err = sdsnew("Library name was not given");
        goto error;
    }

    sdsfreesplitres(parts, numparts);

    md->name = name;
    md->code = sdsnewlen(shebang_end, sdslen(payload) - shebang_len);
    md->engine = engine;

    return C_OK;

error:
    if (name) sdsfree(name);
    if (engine) sdsfree(engine);
    sdsfreesplitres(parts, numparts);
    return C_ERR;
}

/* 释放函数库元数据结构体中的所有字符串字段 */
void functionFreeLibMetaData(functionsLibMataData *md) {
    if (md->code) sdsfree(md->code);
    if (md->name) sdsfree(md->name);
    if (md->engine) sdsfree(md->engine);
}

/* 编译并保存给定的函数库。
 * 成功返回加载的函数库名称，失败返回 NULL 并通过 err 参数设置错误信息。 */
sds functionsCreateWithLibraryCtx(sds code, int replace, sds* err, functionsLibCtx *lib_ctx, size_t timeout) {
    dictIterator *iter = NULL;
    dictEntry *entry = NULL;
    functionLibInfo *new_li = NULL;
    functionLibInfo *old_li = NULL;
    functionsLibMataData md = {0};
    if (functionExtractLibMetaData(code, &md, err) != C_OK) {
        return NULL;
    }

    if (functionsVerifyName(md.name)) {
        *err = sdsnew("Library names can only contain letters, numbers, or underscores(_) and must be at least one character long");
        goto error;
    }

    engineInfo *ei = dictFetchValue(engines, md.engine);
    if (!ei) {
        *err = sdscatfmt(sdsempty(), "Engine '%S' not found", md.engine);
        goto error;
    }
    engine *engine = ei->engine;

    old_li = dictFetchValue(lib_ctx->libraries, md.name);
    if (old_li && !replace) {
        old_li = NULL;
        *err = sdscatfmt(sdsempty(), "Library '%S' already exists", md.name);
        goto error;
    }

    if (old_li) {
        libraryUnlink(lib_ctx, old_li);
    }

    new_li = engineLibraryCreate(md.name, ei, code);
    if (engine->create(engine->engine_ctx, new_li, md.code, timeout, err) != C_OK) {
        goto error;
    }

    if (dictSize(new_li->functions) == 0) {
        *err = sdsnew("No functions registered");
        goto error;
    }

    /* 验证没有重复的函数名 */
    iter = dictGetIterator(new_li->functions);
    while ((entry = dictNext(iter))) {
        functionInfo *fi = dictGetVal(entry);
        if (dictFetchValue(lib_ctx->functions, fi->name)) {
            /* 函数名冲突，中止 */
            *err = sdscatfmt(sdsempty(), "Function %s already exists", fi->name);
            goto error;
        }
    }
    dictReleaseIterator(iter);
    iter = NULL;

    libraryLink(lib_ctx, new_li);

    if (old_li) {
        engineLibraryFree(old_li);
    }

    sds loaded_lib_name = md.name;
    md.name = NULL;
    functionFreeLibMetaData(&md);

    return loaded_lib_name;

error:
    if (iter) dictReleaseIterator(iter);
    if (new_li) engineLibraryFree(new_li);
    if (old_li) libraryLink(lib_ctx, old_li);
    functionFreeLibMetaData(&md);
    return NULL;
}

/*
 * FUNCTION LOAD [REPLACE] <LIBRARY CODE>
 * REPLACE      - 可选，替换已存在的函数库
 * LIBRARY CODE - 传递给引擎的函数库代码
 */
void functionLoadCommand(client *c) {
    int replace = 0;
    int argc_pos = 2;
    while (argc_pos < c->argc - 1) {
        robj *next_arg = c->argv[argc_pos++];
        if (!strcasecmp(next_arg->ptr, "replace")) {
            replace = 1;
            continue;
        }
        addReplyErrorFormat(c, "Unknown option given: %s", (char*)next_arg->ptr);
        return;
    }

    if (argc_pos >= c->argc) {
        addReplyError(c, "Function code is missing");
        return;
    }

    robj *code = c->argv[argc_pos];
    sds err = NULL;
    sds library_name = NULL;
    size_t timeout = LOAD_TIMEOUT_MS;
    if (mustObeyClient(c)) {
        timeout = 0;
    }
    if (!(library_name = functionsCreateWithLibraryCtx(code->ptr, replace, &err, curr_functions_lib_ctx, timeout)))
    {
        addReplyErrorSds(c, err);
        return;
    }
    /* 标记数据已变更，用于复制和持久化 */
    server.dirty++;
    addReplyBulkSds(c, library_name);
}

/* 返回所有引擎的内存使用量之和 */
unsigned long functionsMemory(void) {
    dictIterator *iter = dictGetIterator(engines);
    dictEntry *entry = NULL;
    size_t engines_memory = 0;
    while ((entry = dictNext(iter))) {
        engineInfo *ei = dictGetVal(entry);
        engine *engine = ei->engine;
        engines_memory += engine->get_used_memory(engine->engine_ctx);
    }
    dictReleaseIterator(iter);

    return engines_memory;
}

/* 返回所有引擎的额外内存开销之和 */
unsigned long functionsMemoryOverhead(void) {
    size_t memory_overhead = dictMemUsage(engines);
    memory_overhead += dictMemUsage(curr_functions_lib_ctx->functions);
    memory_overhead += sizeof(functionsLibCtx);
    memory_overhead += curr_functions_lib_ctx->cache_memory;
    memory_overhead += engine_cache_memory;

    return memory_overhead;
}

/* 返回已注册的函数数量 */
unsigned long functionsNum(void) {
    return dictSize(curr_functions_lib_ctx->functions);
}

/* 返回已注册的函数库总数 */
unsigned long functionsLibNum(void) {
    return dictSize(curr_functions_lib_ctx->libraries);
}

/* 返回当前函数库上下文的 libraries 字典 */
dict* functionsLibGet(void) {
    return curr_functions_lib_ctx->libraries;
}

/* 返回给定上下文中的函数数量 */
size_t functionsLibCtxFunctionsLen(functionsLibCtx *functions_ctx) {
    return dictSize(functions_ctx->functions);
}

/*
 * 初始化引擎数据结构。
 * 应在服务器启动时调用一次。
 */
int functionsInit(void) {
    engines = dictCreate(&engineDictType);

    if (luaEngineInitEngine() != C_OK) {
        return C_ERR;
    }

    /* 必须在引擎初始化之后创建函数库上下文 */
    curr_functions_lib_ctx = functionsLibCtxCreate();

    return C_OK;
}
