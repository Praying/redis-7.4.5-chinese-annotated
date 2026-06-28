/*
 * Copyright (c) 2011-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"          // Redis 服务器核心头文件
#include "sha1.h"            // SHA1 哈希算法实现
#include "rand.h"            // 随机数生成器
#include "cluster.h"         // 集群相关定义
#include "monotonic.h"       // 单调时钟计时工具
#include "resp_parser.h"     // RESP 协议解析器
#include "script_lua.h"      // Lua 脚本与 Redis API 桥接层

#include <lua.h>             // Lua 核心 API
#include <lauxlib.h>         // Lua 辅助库
#include <lualib.h>          // Lua 标准库
#if defined(USE_JEMALLOC)
#include <lstate.h>          // Lua 内部状态结构（用于 jemalloc tcache 管理）
#endif
#include <ctype.h>           // 字符处理函数
#include <math.h>            // 数学函数

// GC 请求计数器，每次 GC 执行后重置
static int gc_count = 0;

/* 前向声明：Lua 调试器相关函数 */
void ldbInit(void);                          // 初始化调试器数据结构
void ldbDisable(client *c);                  // 禁用客户端的调试模式
void ldbEnable(client *c);                   // 启用客户端的调试模式
void evalGenericCommandWithDebugging(client *c, int evalsha);  // 带调试的 EVAL/EVALSHA 执行
sds ldbCatStackValue(sds s, lua_State *lua, int idx);  // 将 Lua 栈值转为可读字符串
listNode *luaScriptsLRUAdd(client *c, sds sha, int evalsha);  // 将脚本加入 LRU 淘汰列表

/* luaScript 字典值的析构函数：释放脚本体引用和内存 */
static void dictLuaScriptDestructor(dict *d, void *val) {
    UNUSED(d);
    if (val == NULL) return; /* 延迟释放会将值设为 NULL */
    decrRefCount(((luaScript*)val)->body);  // 减少脚本体 robj 引用计数
    zfree(val);                              // 释放 luaScript 结构体
}

/* 不区分大小写的字符串哈希函数，用于 SHA1 键的哈希 */
static uint64_t dictStrCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, strlen((char*)key));
}

/* lctx.lua_scripts 字典类型定义：
 * 键为 SHA1 字符串（sds），值为 luaScript 结构体指针。
 * 用于缓存已加载的 Lua 脚本。 */
dictType shaScriptObjectDictType = {
        dictStrCaseHash,            /* 不区分大小写的哈希函数 */
        NULL,                       /* 键复制（不使用） */
        NULL,                       /* 值复制（不使用） */
        dictSdsKeyCaseCompare,      /* 不区分大小写的键比较函数 */
        dictSdsDestructor,          /* 键析构函数：释放 sds 字符串 */
        dictLuaScriptDestructor,    /* 值析构函数：释放 luaScript */
        NULL                        /* 允许扩展（使用默认行为） */
};

/* Lua 上下文结构体：保存所有 Lua 脚本相关的全局状态 */
struct luaCtx {
    lua_State *lua;            /* Lua 解释器实例，所有客户端共享一个 */
    client *lua_client;        /* 伪客户端，用于从 Lua 中执行 Redis 命令 */
    dict *lua_scripts;         /* SHA1 -> luaScript 的字典缓存 */
    list *lua_scripts_lru_list; /* SHA1 的 LRU 淘汰链表（先进先出） */
    unsigned long long lua_scripts_mem;  /* 已缓存脚本的内存占用（含开销） */
} lctx;

/* Lua 调试器（LDB）共享状态，存储在全局结构体 ldb 中 */
#define LDB_BREAKPOINTS_MAX 64  /* 最大断点数量 */
#define LDB_MAX_LEN_DEFAULT 256 /* 回复/变量转储的默认长度限制 */
struct ldbState {
    connection *conn; /* 调试客户端的连接 */
    int active;       /* 当前是否正在调试 EVAL */
    int forked;       /* 是否为 fork() 出来的调试会话 */
    list *logs;       /* 待发送给客户端的消息列表 */
    list *traces;     /* 上次停止以来执行的 Redis 命令消息 */
    list *children;   /* 所有 fork 出的调试会话的 pid 列表 */
    int bp[LDB_BREAKPOINTS_MAX]; /* 断点行号数组 */
    int bpcount;      /* bp 数组中有效断点的数量 */
    int step;         /* 单步模式标志：在下一行停止（忽略断点） */
    int luabp;        /* 因调用 redis.breakpoint() 而在下一行停止 */
    sds *src;         /* Lua 脚本源代码按行分割后的数组 */
    int lines;        /* src 数组中的总行数 */
    int currentline;  /* 当前执行的行号 */
    sds cbuf;         /* 调试客户端的命令缓冲区 */
    size_t maxlen;    /* 变量转储/回复的最大长度 */
    int maxlen_hint_sent; /* 是否已提示过用户关于 "set maxlen" 命令 */
} ldb;

/* ---------------------------------------------------------------------------
 * 工具函数
 * ------------------------------------------------------------------------- */

/* 对输入字符串执行 SHA1 哈希。用于：
 * 1. 对脚本体进行哈希以生成 Lua 函数名
 * 2. 实现 redis.sha1() 命令
 *
 * 'digest' 应指向 41 字节的缓冲区：40 字节存放十六进制 SHA1 值，
 * 加 1 字节的 null 终止符。 */
void sha1hex(char *digest, char *script, size_t len) {
    SHA1_CTX ctx;
    unsigned char hash[20];
    char *cset = "0123456789abcdef";
    int j;

    SHA1Init(&ctx);                                    // 初始化 SHA1 上下文
    SHA1Update(&ctx,(unsigned char*)script,len);       // 喂入脚本数据
    SHA1Final(hash,&ctx);                              // 完成哈希计算

    // 将 20 字节的二进制哈希转为 40 字节的十六进制字符串
    for (j = 0; j < 20; j++) {
        digest[j*2] = cset[((hash[j]&0xF0)>>4)];      // 高 4 位
        digest[j*2+1] = cset[(hash[j]&0xF)];           // 低 4 位
    }
    digest[40] = '\0';
}

/* redis.breakpoint()
 *
 * 允许在调试会话中从 Lua 代码内部中断执行，
 * 效果等同于在该函数调用之后的代码行设置了断点。 */
int luaRedisBreakpointCommand(lua_State *lua) {
    if (ldb.active) {
        ldb.luabp = 1;              // 设置 Lua 断点标志
        lua_pushboolean(lua,1);
    } else {
        lua_pushboolean(lua,0);     // 非调试模式下不起作用
    }
    return 1;
}

/* redis.debug()
 *
 * 将字符串消息记录到调试输出控制台。
 * 可接受多个参数，参数之间用逗号分隔。
 * 不返回任何值给调用者。 */
int luaRedisDebugCommand(lua_State *lua) {
    if (!ldb.active) return 0;     // 非调试模式下忽略
    int argc = lua_gettop(lua);
    // 构造日志前缀，包含当前行号
    sds log = sdscatprintf(sdsempty(),"<debug> line %d: ", ldb.currentline);
    while(argc--) {
        log = ldbCatStackValue(log,lua,-1 - argc);  // 追加参数的可读表示
        if (argc != 0) log = sdscatlen(log,", ",2);  // 参数间加逗号分隔
    }
    ldbLog(log);
    return 0;
}

/* redis.replicate_commands()
 *
 * 已废弃：现在不做任何操作，始终返回 true。
 * 原功能：如果脚本尚未调用写命令，则开启单命令复制模式并返回 true；
 * 否则返回 false 并保持默认的整脚本复制模式。 */
int luaRedisReplicateCommandsCommand(lua_State *lua) {
    lua_pushboolean(lua,1);        // 始终返回 true（已废弃，无实际效果）
    return 1;
}

/* 初始化 Lua 脚本环境。
 *
 * 服务器启动时首次调用此函数时 'setup' 参数设为 1。
 * 在 Redis 进程生命周期内可多次调用（'setup' 设为 0），
 * 在 scriptingRelease() 之后调用以重置 Lua 脚本环境。
 *
 * 更简便的方式是直接调用 scriptingReset()。 */
void scriptingInit(int setup) {
    if (setup) {
        lctx.lua_client = NULL;                  // 首次初始化，清空伪客户端
        server.script_disable_deny_script = 0;   // 重置脚本权限标志
        ldbInit();                               // 初始化 Lua 调试器
    }

    lua_State *lua = createLuaState();           // 创建新的 Lua 虚拟机
    if (lua == NULL) {
        serverLog(LL_WARNING, "Failed creating the lua VM.");
        exit(1);
    }

    /* 初始化 SHA1 -> 脚本 的映射字典。
     * 初始化用于脚本淘汰的链表，与字典共享 SHA 键，
     * 因此不设置释放函数。 */
    lctx.lua_scripts = dictCreate(&shaScriptObjectDictType);
    lctx.lua_scripts_lru_list = listCreate();
    lctx.lua_scripts_mem = 0;

    luaRegisterRedisAPI(lua);                    // 注册 Redis Lua API（redis.call 等）

    /* 注册调试命令到 redis 全局表 */
    lua_getglobal(lua,"redis");

    /* redis.breakpoint */
    lua_pushstring(lua,"breakpoint");
    lua_pushcfunction(lua,luaRedisBreakpointCommand);
    lua_settable(lua,-3);

    /* redis.debug */
    lua_pushstring(lua,"debug");
    lua_pushcfunction(lua,luaRedisDebugCommand);
    lua_settable(lua,-3);

    /* redis.replicate_commands（已废弃） */
    lua_pushstring(lua, "replicate_commands");
    lua_pushcfunction(lua, luaRedisReplicateCommandsCommand);
    lua_settable(lua, -3);

    lua_setglobal(lua,"redis");

    /* 添加 pcall 错误报告的辅助函数。
     * 注意：当错误发生在 C 函数中时，我们报告调用者的信息，
     * 因为从用户调试脚本的角度来看这更有意义。 */
    {
        char *errh_func =       "local dbg = debug\n"
                                "debug = nil\n"
                                "function __redis__err__handler(err)\n"
                                "  local i = dbg.getinfo(2,'nSl')\n"
                                "  if i and i.what == 'C' then\n"
                                "    i = dbg.getinfo(3,'nSl')\n"
                                "  end\n"
                                "  if type(err) ~= 'table' then\n"
                                "    err = {err='ERR ' .. tostring(err)}"
                                "  end"
                                "  if i then\n"
                                "    err['source'] = i.source\n"
                                "    err['line'] = i.currentline\n"
                                "  end"
                                "  return err\n"
                                "end\n";
        luaL_loadbuffer(lua,errh_func,strlen(errh_func),"@err_handler_def");
        lua_pcall(lua,0,0,0);
    }

    /* 创建用于在 Lua 解释器中执行 Redis 命令的伪客户端（不连接任何 socket）。
     * 注意：当 scriptingReset() 调用本函数时无需重新创建。 */
    if (lctx.lua_client == NULL) {
        lctx.lua_client = createClient(NULL);
        lctx.lua_client->flags |= CLIENT_SCRIPT;         // 标记为脚本客户端

        /* 不允许在 Lua 脚本中执行阻塞命令 */
        lctx.lua_client->flags |= CLIENT_DENY_BLOCKING;
    }

    /* 锁定全局表，防止脚本修改 */
    lua_pushvalue(lua, LUA_GLOBALSINDEX);
    luaSetErrorMetatable(lua);
    /* 递归锁定全局表可访问到的所有子表 */
    luaSetTableProtectionRecursively(lua);
    lua_pop(lua, 1);

    lctx.lua = lua;                                      // 保存 Lua 虚拟机引用
}

/* 同步释放 lua_scripts 字典并关闭 Lua 解释器。 */
void freeLuaScriptsSync(dict *lua_scripts, list *lua_scripts_lru_list, lua_State *lua) {
    dictRelease(lua_scripts);                   // 释放脚本字典
    listRelease(lua_scripts_lru_list);           // 释放 LRU 链表

#if defined(USE_JEMALLOC)
    /* Lua 关闭前，获取之前使用的私有 tcache 的 ID，稍后销毁 */
    void *ud = (global_State*)G(lua)->ud;
    unsigned int lua_tcache = (unsigned int)(uintptr_t)ud;
#endif

    lua_gc(lua, LUA_GCCOLLECT, 0);              // 触发 Lua GC
    lua_close(lua);                              // 关闭 Lua 虚拟机

#if defined(USE_JEMALLOC)
    // 销毁 Lua 使用的 jemalloc 私有 tcache
    je_mallctl("tcache.destroy", NULL, NULL, (void *)&lua_tcache, sizeof(unsigned int));
#endif
}

/* 释放与 Lua 脚本相关的资源。
 * 此函数用于重置脚本环境。 */
void scriptingRelease(int async) {
    if (async)
        freeLuaScriptsAsync(lctx.lua_scripts, lctx.lua_scripts_lru_list, lctx.lua);
    else
        freeLuaScriptsSync(lctx.lua_scripts, lctx.lua_scripts_lru_list, lctx.lua);
}

/* 重置脚本环境：释放旧资源并重新初始化 */
void scriptingReset(int async) {
    scriptingRelease(async);   // 释放旧的 Lua 环境
    scriptingInit(0);          // 重新初始化（非首次启动模式）
}

/* ---------------------------------------------------------------------------
 * EVAL 和 SCRIPT 命令实现
 * ------------------------------------------------------------------------- */

/* 根据 EVAL 或 EVALSHA 调用计算 Lua 函数名。
 * 函数名格式为 "f_" + 40 字符的十六进制 SHA1。
 * 结果写入 out_funcname（需至少 43 字节缓冲区）。 */
static void evalCalcFunctionName(int evalsha, sds script, char *out_funcname) {
    /* 获取脚本 SHA1，然后检查该函数是否已在 Lua 状态中定义 */
    out_funcname[0] = 'f';       // 函数名前缀 "f_"
    out_funcname[1] = '_';
    if (!evalsha) {
        /* EVAL 调用：需要对脚本代码计算 SHA1 哈希 */
        sha1hex(out_funcname+2,script,sdslen(script));
    } else {
        /* EVALSHA 调用：已经拥有 SHA1，直接使用 */
        int j;
        char *sha = script;

        /* 转换为小写。不使用 tolower() 是因为该函数在 profiler 输出中
         * 总是占用不少时间。手动转换更高效。 */
        for (j = 0; j < 40; j++)
            out_funcname[j+2] = (sha[j] >= 'A' && sha[j] <= 'Z') ?
                sha[j]+('a'-'A') : sha[j];
        out_funcname[42] = '\0';
    }
}

/* 尝试从脚本体中提取 shebang 标志。
 * 如果没有找到 shebang，则以成功状态返回，标志为 EVAL 兼容模式。
 * err 参数可选，用于获取详细错误字符串。
 * out_shebang_len 参数可选，用于从脚本中裁剪 shebang 行。
 * 成功返回 C_OK，失败返回 C_ERR。 */
int evalExtractShebangFlags(sds body, uint64_t *out_flags, ssize_t *out_shebang_len, sds *err) {
    ssize_t shebang_len = 0;
    uint64_t script_flags = SCRIPT_FLAG_EVAL_COMPAT_MODE;  // 默认为 EVAL 兼容模式
    if (!strncmp(body, "#!", 2)) {
        int numparts,j;
        char *shebang_end = strchr(body, '\n');    // 查找 shebang 行结尾
        if (shebang_end == NULL) {
            if (err)
                *err = sdsnew("Invalid script shebang");
            return C_ERR;
        }
        shebang_len = shebang_end - body;           // 计算 shebang 行长度
        sds shebang = sdsnewlen(body, shebang_len);
        sds *parts = sdssplitargs(shebang, &numparts);  // 按空格分割 shebang
        sdsfree(shebang);
        if (!parts || numparts == 0) {
            if (err)
                *err = sdsnew("Invalid engine in script shebang");
            sdsfreesplitres(parts, numparts);
            return C_ERR;
        }
        /* 验证指定的解释器是否为 lua */
        if (strcmp(parts[0], "#!lua")) {
            if (err)
                *err = sdscatfmt(sdsempty(), "Unexpected engine in script shebang: %s", parts[0]);
            sdsfreesplitres(parts, numparts);
            return C_ERR;
        }
        script_flags &= ~SCRIPT_FLAG_EVAL_COMPAT_MODE;  // 有 shebang 则退出兼容模式
        for (j = 1; j < numparts; j++) {
            if (!strncmp(parts[j], "flags=", 6)) {
                sdsrange(parts[j], 6, -1);          // 提取 "flags=" 之后的部分
                int numflags, jj;
                sds *flags = sdssplitlen(parts[j], sdslen(parts[j]), ",", 1, &numflags);
                for (jj = 0; jj < numflags; jj++) {
                    scriptFlag *sf;
                    for (sf = scripts_flags_def; sf->flag; sf++) {
                        if (!strcmp(flags[jj], sf->str)) break;  // 查找已知标志
                    }
                    if (!sf->flag) {
                        if (err)
                            *err = sdscatfmt(sdsempty(), "Unexpected flag in script shebang: %s", flags[jj]);
                        sdsfreesplitres(flags, numflags);
                        sdsfreesplitres(parts, numparts);
                        return C_ERR;
                    }
                    script_flags |= sf->flag;       // 设置对应的脚本标志位
                }
                sdsfreesplitres(flags, numflags);
            } else {
                /* Lua 脚本仅支持 flags 选项 */
                if (err)
                    *err = sdscatfmt(sdsempty(), "Unknown lua shebang option: %s", parts[j]);
                sdsfreesplitres(parts, numparts);
                return C_ERR;
            }
        }
        sdsfreesplitres(parts, numparts);
    }
    if (out_shebang_len)
        *out_shebang_len = shebang_len;
    *out_flags = script_flags;
    return C_OK;
}

/* 尝试提取脚本的命令标志并返回修改后的标志。
 * 注意：此函数不保证命令参数的正确性。 */
uint64_t evalGetCommandFlags(client *c, uint64_t cmd_flags) {
    char funcname[43];
    int evalsha = c->cmd->proc == evalShaCommand || c->cmd->proc == evalShaRoCommand;
    if (evalsha && sdslen(c->argv[1]->ptr) != 40)
        return cmd_flags;                       // SHA 长度不对，直接返回原标志
    uint64_t script_flags;
    evalCalcFunctionName(evalsha, c->argv[1]->ptr, funcname);
    char *lua_cur_script = funcname + 2;
    c->cur_script = dictFind(lctx.lua_scripts, lua_cur_script);  // 查找已缓存的脚本
    if (!c->cur_script) {
        if (evalsha)
            return cmd_flags;                   // EVALSHA 且脚本未缓存，无法提取标志
        // EVAL 调用：从脚本体中提取 shebang 标志
        if (evalExtractShebangFlags(c->argv[1]->ptr, &script_flags, NULL, NULL) == C_ERR)
            return cmd_flags;
    } else {
        luaScript *l = dictGetVal(c->cur_script);
        script_flags = l->flags;                // 使用缓存的脚本标志
    }
    if (script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE)
        return cmd_flags;                       // 兼容模式下不修改命令标志
    return scriptFlagsToCmdFlags(cmd_flags, script_flags);  // 将脚本标志转换为命令标志
}

/* 使用指定的脚本体定义一个 Lua 函数。
 * 函数名格式为：
 *
 *   f_<十六进制 sha1>
 *
 * 成功调用时会增加 'body' 对象的引用计数。
 *
 * 成功时返回指向新添加函数 SHA1 的 SDS 字符串指针
 * （在下次 scriptingReset() 调用之前有效），失败返回 NULL。
 *
 * 如果脚本已存在，函数行为与成功情况相同（幂等）。
 *
 * 如果 'c' 不为 NULL，出错时会向客户端发送错误信息，
 * 包含问题描述和 Lua 解释器错误。
 *
 * 'evalsha' 表示 Lua 函数是从 EVAL 还是 SCRIPT LOAD 上下文创建的。 */
sds luaCreateFunction(client *c, robj *body, int evalsha) {
    char funcname[43];
    dictEntry *de;
    uint64_t script_flags;

    funcname[0] = 'f';
    funcname[1] = '_';
    sha1hex(funcname+2,body->ptr,sdslen(body->ptr));  // 计算脚本体的 SHA1

    // 检查脚本是否已存在于缓存中
    if ((de = dictFind(lctx.lua_scripts,funcname+2)) != NULL) {
        return dictGetKey(de);              // 已存在，直接返回 SHA1 键
    }

    /* 处理脚本代码中的 shebang 头 */
    ssize_t shebang_len = 0;
    sds err = NULL;
    if (evalExtractShebangFlags(body->ptr, &script_flags, &shebang_len, &err) == C_ERR) {
        if (c != NULL) {
            addReplyErrorSds(c, err);
        }
        return NULL;
    }

    /* 遇到 shebang 行时跳过它，但保留换行符以保持用户代码的行号不变 */
    if (luaL_loadbuffer(lctx.lua,(char*)body->ptr + shebang_len,sdslen(body->ptr) - shebang_len,"@user_script")) {
        if (c != NULL) {
            addReplyErrorFormat(c,
                "Error compiling script (new function): %s",
                lua_tostring(lctx.lua,-1));
        }
        lua_pop(lctx.lua,1);
        luaGC(lctx.lua, &gc_count);
        return NULL;
    }

    serverAssert(lua_isfunction(lctx.lua, -1));  // 确认栈顶是函数

    lua_setfield(lctx.lua, LUA_REGISTRYINDEX, funcname);  // 将函数注册到 Lua 注册表

    /* 同时保存 SHA1 -> 原始脚本的映射到字典中，
     * 以便在复制/AOF 写入时将 EVALSHA 命令转为 EVAL + 原始脚本。 */
    luaScript *l = zcalloc(sizeof(luaScript));
    l->body = body;
    l->flags = script_flags;
    sds sha = sdsnewlen(funcname+2,40);
    l->node = luaScriptsLRUAdd(c, sha, evalsha);  // 加入 LRU 淘汰列表
    int retval = dictAdd(lctx.lua_scripts,sha,l);
    serverAssertWithInfo(c ? c : lctx.lua_client,NULL,retval == DICT_OK);
    // 更新脚本缓存的内存使用量
    lctx.lua_scripts_mem += sdsZmallocSize(sha) + getStringObjectSdsUsedMemory(body);
    incrRefCount(body);                       // 增加脚本体的引用计数

    /* 创建脚本并加入 LRU 列表后执行 GC，
     * 因为添加过程中可能会淘汰旧脚本。 */
    luaGC(lctx.lua, &gc_count);

    return sha;
}

/* 删除指定 SHA1 的 Lua 函数。
 *
 * 从 Lua 解释器中删除该函数，并从服务器的脚本缓存中移除。 */
void luaDeleteFunction(client *c, sds sha) {
    /* 从 Lua 解释器中删除脚本函数 */
    char funcname[43];
    funcname[0] = 'f';
    funcname[1] = '_';
    memcpy(funcname+2, sha, 40);
    funcname[42] = '\0';
    lua_pushnil(lctx.lua);                              // 将 nil 压栈
    lua_setfield(lctx.lua, LUA_REGISTRYINDEX, funcname); // 用 nil 覆盖注册表中的函数

    /* 从服务器脚本缓存中删除脚本 */
    dictEntry *de = dictUnlink(lctx.lua_scripts, sha);
    serverAssertWithInfo(c ? c : lctx.lua_client, NULL, de);
    luaScript *l = dictGetVal(de);
    /* 只删除通过 EVAL 加载的脚本，它们必定存在于 LRU 列表中 */
    serverAssert(l->node);
    listDelNode(lctx.lua_scripts_lru_list, l->node);    // 从 LRU 列表中移除
    // 更新脚本缓存的内存使用量
    lctx.lua_scripts_mem -= sdsZmallocSize(sha) + getStringObjectSdsUsedMemory(l->body);
    dictFreeUnlinkedEntry(lctx.lua_scripts, de);        // 释放字典条目
}

/* 滥用 EVAL 的用户每次调用都会生成新的 Lua 脚本，可能随时间消耗大量内存。
 * 由于 EVAL 是 Lua 缓存的主要滥用者，且不会出现 pipeline 问题
 * （脚本不会在 EVALSHA 需要时消失导致失败），因此仅为 EVAL 脚本实现淘汰机制
 * （不包括通过 SCRIPT LOAD 加载的脚本）。
 * 由于脚本数量不多，与键空间不同，不需要维护真正排序的 LRU 链表。
 *
 * 'evalsha' 表示 Lua 函数是从 EVAL 还是 SCRIPT LOAD 上下文添加的。
 *
 * 返回添加的链表节点，保存在 luaScript 中以便脚本每次被使用时
 * 能快速移除并重新插入到 LRU 列表尾部。 */
#define LRU_LIST_LENGTH 500    // LRU 列表最大长度
listNode *luaScriptsLRUAdd(client *c, sds sha, int evalsha) {
    /* 脚本淘汰仅适用于 EVAL，不适用于 SCRIPT LOAD */
    if (evalsha) return NULL;

    /* 淘汰最旧的脚本 */
    while (listLength(lctx.lua_scripts_lru_list) >= LRU_LIST_LENGTH) {
        listNode *ln = listFirst(lctx.lua_scripts_lru_list);
        sds oldest = listNodeValue(ln);
        luaDeleteFunction(c, oldest);          // 删除最旧的脚本
        server.stat_evictedscripts++;           // 更新淘汰脚本计数
    }

    /* 将当前脚本添加到列表尾部 */
    listAddNodeTail(lctx.lua_scripts_lru_list, sha);
    return listLast(lctx.lua_scripts_lru_list);
}

/* EVAL/EVALSHA 命令的核心执行函数。
 * evalsha 参数为 1 时表示 EVALSHA，为 0 时表示 EVAL。 */
void evalGenericCommand(client *c, int evalsha) {
    lua_State *lua = lctx.lua;
    char funcname[43];
    long long numkeys;

    /* 获取 key 参数的数量 */
    if (getLongLongFromObjectOrReply(c,c->argv[2],&numkeys,NULL) != C_OK)
        return;
    if (numkeys > (c->argc - 3)) {
        addReplyError(c,"Number of keys can't be greater than number of args");
        return;
    } else if (numkeys < 0) {
        addReplyError(c,"Number of keys can't be negative");
        return;
    }

    /* 如果在 evalGetCommandFlags 中已查找到脚本，直接使用缓存的 SHA */
    if (c->cur_script) {
        funcname[0] = 'f', funcname[1] = '_';
        memcpy(funcname+2, dictGetKey(c->cur_script), 40);
        funcname[42] = '\0';
    } else
        evalCalcFunctionName(evalsha, c->argv[1]->ptr, funcname);

    /* 将 pcall 错误处理函数压入 Lua 栈 */
    lua_getglobal(lua, "__redis__err__handler");

    /* 尝试查找 Lua 函数 */
    lua_getfield(lua, LUA_REGISTRYINDEX, funcname);
    if (lua_isnil(lua,-1)) {
        lua_pop(lua,1); /* 移除栈顶的 nil */
        /* 函数未定义：如果有脚本体则定义它。
         * 如果是 EVALSHA 调用，直接返回错误。 */
        if (evalsha) {
            lua_pop(lua,1); /* 移除错误处理函数 */
            addReplyErrorObject(c, shared.noscripterr);
            return;
        }
        if (luaCreateFunction(c, c->argv[1], evalsha) == NULL) {
            lua_pop(lua,1); /* 移除错误处理函数 */
            /* luaCreateFunction() 返回 NULL 时已向客户端发送了错误 */
            return;
        }
        /* 现在以下查找保证返回非 nil */
        lua_getfield(lua, LUA_REGISTRYINDEX, funcname);
        serverAssert(!lua_isnil(lua,-1));
    }

    char *lua_cur_script = funcname + 2;
    dictEntry *de = c->cur_script;
    if (!de)
        de = dictFind(lctx.lua_scripts, lua_cur_script);
    luaScript *l = dictGetVal(de);
    int ro = c->cmd->proc == evalRoCommand || c->cmd->proc == evalShaRoCommand;

    /* 准备脚本执行上下文 */
    scriptRunCtx rctx;
    if (scriptPrepareForRun(&rctx, lctx.lua_client, c, lua_cur_script, l->flags, ro) != C_OK) {
        lua_pop(lua,2); /* 移除函数和错误处理函数 */
        return;
    }
    rctx.flags |= SCRIPT_EVAL_MODE; /* 标记为 EVAL 模式（区别于 FCALL），
                                      * 以获得适当的错误消息和日志 */

    /* 执行 Lua 函数 */
    luaCallFunction(&rctx, lua, c->argv+3, numkeys, c->argv+3+numkeys, c->argc-3-numkeys, ldb.active);
    lua_pop(lua,1); /* 移除错误处理函数 */
    scriptResetRun(&rctx);
    luaGC(lua, &gc_count);          // 执行 GC

    if (l->node) {
        /* 脚本调用后快速移除并重新插入到 LRU 列表尾部，
         * 以维护 LRU 顺序。 */
        listUnlinkNode(lctx.lua_scripts_lru_list, l->node);
        listLinkNodeTail(lctx.lua_scripts_lru_list, l->node);
    }
}

/* EVAL 命令处理函数 */
void evalCommand(client *c) {
    /* 显式向 monitor 发送命令，使 Lua 内部命令出现在其脚本命令之后 */
    replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
    if (!(c->flags & CLIENT_LUA_DEBUG))
        evalGenericCommand(c,0);                    // 正常执行 EVAL
    else
        evalGenericCommandWithDebugging(c,0);       // 调试模式执行
}

/* EVAL_RO 只读命令处理函数（委托给 evalCommand） */
void evalRoCommand(client *c) {
    evalCommand(c);
}

/* EVALSHA 命令处理函数 */
void evalShaCommand(client *c) {
    /* 显式向 monitor 发送命令，使 Lua 内部命令出现在其脚本命令之后 */
    replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
    if (sdslen(c->argv[1]->ptr) != 40) {
        /* SHA 长度不是 40 则必定不匹配，尽早返回错误。
         * 这样 evalGenericCommand() 内部无需再做长度校验。 */
        addReplyErrorObject(c, shared.noscripterr);
        return;
    }
    if (!(c->flags & CLIENT_LUA_DEBUG))
        evalGenericCommand(c,1);                    // 正常执行 EVALSHA
    else {
        addReplyError(c,"Please use EVAL instead of EVALSHA for debugging");
        return;
    }
}

/* EVALSHA_RO 只读命令处理函数（委托给 evalShaCommand） */
void evalShaRoCommand(client *c) {
    evalShaCommand(c);
}

/* SCRIPT 命令处理函数：处理 SCRIPT HELP/FLUSH/EXISTS/LOAD/KILL/DEBUG 子命令 */
void scriptCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"DEBUG (YES|SYNC|NO)",
"    Set the debug mode for subsequent scripts executed.",
"EXISTS <sha1> [<sha1> ...]",
"    Return information about the existence of the scripts in the script cache.",
"FLUSH [ASYNC|SYNC]",
"    Flush the Lua scripts cache. Very dangerous on replicas.",
"    When called without the optional mode argument, the behavior is determined by the",
"    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
"    * ASYNC: Asynchronously flush the scripts cache.",
"    * SYNC: Synchronously flush the scripts cache.",
"KILL",
"    Kill the currently executing Lua script.",
"LOAD <script>",
"    Load a script into the scripts cache without executing it.",
NULL
        };
        addReplyHelp(c, help);
    } else if (c->argc >= 2 && !strcasecmp(c->argv[1]->ptr,"flush")) {
        /* SCRIPT FLUSH [ASYNC|SYNC]：清除 Lua 脚本缓存 */
        int async = 0;
        if (c->argc == 3 && !strcasecmp(c->argv[2]->ptr,"sync")) {
            async = 0;
        } else if (c->argc == 3 && !strcasecmp(c->argv[2]->ptr,"async")) {
            async = 1;
        } else if (c->argc == 2) {
            async = server.lazyfree_lazy_user_flush ? 1 : 0;
        } else {
            addReplyError(c,"SCRIPT FLUSH only support SYNC|ASYNC option");
            return;
        }
        scriptingReset(async);
        addReply(c,shared.ok);
    } else if (c->argc >= 2 && !strcasecmp(c->argv[1]->ptr,"exists")) {
        /* SCRIPT EXISTS <sha1> [...]：检查脚本是否在缓存中 */
        int j;

        addReplyArrayLen(c, c->argc-2);
        for (j = 2; j < c->argc; j++) {
            if (dictFind(lctx.lua_scripts,c->argv[j]->ptr))
                addReply(c,shared.cone);            // 存在
            else
                addReply(c,shared.czero);           // 不存在
        }
    } else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr,"load")) {
        /* SCRIPT LOAD <script>：加载脚本到缓存但不执行 */
        sds sha = luaCreateFunction(c, c->argv[2], 1);
        if (sha == NULL) return; /* luaCreateFunction() 已发送错误信息 */
        addReplyBulkCBuffer(c,sha,40);              // 返回脚本 SHA1
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"kill")) {
        /* SCRIPT KILL：终止当前正在执行的 Lua 脚本 */
        scriptKill(c, 1);
    } else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr,"debug")) {
        /* SCRIPT DEBUG YES|SYNC|NO：设置脚本调试模式 */
        if (clientHasPendingReplies(c)) {
            addReplyError(c,"SCRIPT DEBUG must be called outside a pipeline");
            return;
        }
        if (!strcasecmp(c->argv[2]->ptr,"no")) {
            ldbDisable(c);                          // 关闭调试
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"yes")) {
            ldbEnable(c);                           // 开启异步调试
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"sync")) {
            ldbEnable(c);                           // 开启同步调试
            addReply(c,shared.ok);
            c->flags |= CLIENT_LUA_DEBUG_SYNC;
        } else {
            addReplyError(c,"Use SCRIPT DEBUG YES/SYNC/NO");
            return;
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* 返回 Lua 解释器的内存使用量 */
unsigned long evalMemory(void) {
    return luaMemory(lctx.lua);
}

/* 返回脚本缓存字典（用于 INFO 命令等） */
dict* evalScriptsDict(void) {
    return lctx.lua_scripts;
}

/* 返回脚本缓存的总内存使用量（包含脚本体、字典开销、luaScript 结构体和 LRU 链表节点） */
unsigned long evalScriptsMemory(void) {
    return lctx.lua_scripts_mem +
            dictMemUsage(lctx.lua_scripts) +
            dictSize(lctx.lua_scripts) * sizeof(luaScript) +
            listLength(lctx.lua_scripts_lru_list) * sizeof(listNode);
}

/* ---------------------------------------------------------------------------
 * LDB: Redis Lua 调试器设施
 * ------------------------------------------------------------------------- */

/* 初始化 Lua 调试器数据结构 */
void ldbInit(void) {
    ldb.conn = NULL;
    ldb.active = 0;
    ldb.logs = listCreate();
    listSetFreeMethod(ldb.logs,(void (*)(void*))sdsfree);  // 日志条目用 sdsfree 释放
    ldb.children = listCreate();
    ldb.src = NULL;
    ldb.lines = 0;
    ldb.cbuf = sdsempty();
}

/* 清空指定列表中的所有待处理消息 */
void ldbFlushLog(list *log) {
    listNode *ln;

    while((ln = listFirst(log)) != NULL)
        listDelNode(log,ln);
}

/* 检查调试器是否处于活跃的单步状态 */
int ldbIsEnabled(void){
    return ldb.active && ldb.step;
}

/* 为指定客户端启用 Lua 脚本调试模式 */
void ldbEnable(client *c) {
    c->flags |= CLIENT_LUA_DEBUG;
    ldbFlushLog(ldb.logs);              // 清空之前的日志
    ldb.conn = c->conn;                 // 绑定调试客户端连接
    ldb.step = 1;                       // 启用单步模式
    ldb.bpcount = 0;                    // 清空断点计数
    ldb.luabp = 0;                      // 清空 Lua 断点标志
    sdsfree(ldb.cbuf);
    ldb.cbuf = sdsempty();              // 清空命令缓冲区
    ldb.maxlen = LDB_MAX_LEN_DEFAULT;   // 设置默认截断长度
    ldb.maxlen_hint_sent = 0;
}

/* 从客户端视角退出调试模式。此函数不足以完全关闭调试会话，
 * 完整关闭请参见 ldbEndSession()。 */
void ldbDisable(client *c) {
    c->flags &= ~(CLIENT_LUA_DEBUG|CLIENT_LUA_DEBUG_SYNC);
}

/* 将日志条目追加到 LDB 日志列表 */
void ldbLog(sds entry) {
    listAddNodeTail(ldb.logs,entry);
}

/* ldbLog() 的长度限制版本：防止日志超过 ldb.maxlen。
 * 首次触发截断时会生成提示，告知用户可使用 "maxlen" 命令禁用截断。 */
void ldbLogWithMaxLen(sds entry) {
    int trimmed = 0;
    if (ldb.maxlen && sdslen(entry) > ldb.maxlen) {
        sdsrange(entry,0,ldb.maxlen-1);         // 截断到 maxlen 长度
        entry = sdscatlen(entry," ...",4);      // 追加省略号
        trimmed = 1;
    }
    ldbLog(entry);
    if (trimmed && ldb.maxlen_hint_sent == 0) {
        ldb.maxlen_hint_sent = 1;
        ldbLog(sdsnew(
        "<hint> The above reply was trimmed. Use 'maxlen 0' to disable trimming."));
    }
}

/* 将 ldb.logs 以 multi-bulk 简单字符串回复的形式发送给调试客户端。
 * 包含换行符的日志条目会将换行替换为空格。发送后的条目会被消费（移除）。 */
void ldbSendLogs(void) {
    sds proto = sdsempty();
    proto = sdscatfmt(proto,"*%i\r\n", (int)listLength(ldb.logs));
    while(listLength(ldb.logs)) {
        listNode *ln = listFirst(ldb.logs);
        proto = sdscatlen(proto,"+",1);             // RESP 简单字符串前缀
        sdsmapchars(ln->value,"\r\n","  ",2);       // 换行替换为空格
        proto = sdscatsds(proto,ln->value);
        proto = sdscatlen(proto,"\r\n",2);
        listDelNode(ldb.logs,ln);
    }
    if (connWrite(ldb.conn,proto,sdslen(proto)) == -1) {
        /* 不检查 write() 返回值以避免警告，
         * 下一次 read() 会捕获 I/O 错误并关闭调试会话。 */
    }
    sdsfree(proto);
}

/* 在调用 EVAL 实现之前启动调试会话。
 * 技术手段：捕获客户端的 socket 文件描述符，
 * 以便在 Lua 钩子中直接进行 I/O，无需重新进入 Redis 事件循环。
 *
 * 返回 1 表示调用者应继续执行 EVAL；
 * 返回 0 表示应中止操作（fork 会话中的父进程，或 fork 失败时）。
 *
 * 调用者仅在 ldbStartSession() 返回 1 时才应调用 ldbEndSession()。 */
int ldbStartSession(client *c) {
    ldb.forked = (c->flags & CLIENT_LUA_DEBUG_SYNC) == 0;  // 判断是否为异步（fork）模式
    if (ldb.forked) {
        pid_t cp = redisFork(CHILD_TYPE_LDB);      // fork 子进程用于调试
        if (cp == -1) {
            addReplyErrorFormat(c,"Fork() failed: can't run EVAL in debugging mode: %s", strerror(errno));
            return 0;
        } else if (cp == 0) {
            /* 子进程：忽略父进程处理的重要信号 */
            struct sigaction act;
            sigemptyset(&act.sa_mask);
            act.sa_flags = 0;
            act.sa_handler = SIG_IGN;
            sigaction(SIGTERM, &act, NULL);
            sigaction(SIGINT, &act, NULL);

            /* 记录子进程创建日志；关闭监听 socket 以确保
             * 父进程崩溃时客户端会收到连接重置。 */
            serverLog(LL_NOTICE,"Redis forked for debugging eval");
        } else {
            /* 父进程：记录子进程 pid 并关闭客户端连接 */
            listAddNodeTail(ldb.children,(void*)(unsigned long)cp);
            freeClientAsync(c); /* 在父进程侧关闭客户端 */
            return 0;
        }
    } else {
        serverLog(LL_NOTICE,
            "Redis synchronous debugging eval session started");
    }

    /* 设置调试会话环境 */
    connBlock(ldb.conn);                    // 设为阻塞模式
    connSendTimeout(ldb.conn,5000);         // 设置 5 秒发送超时
    ldb.active = 1;                         // 标记调试器为活跃状态

    /* EVAL 的第一个参数是脚本体，将其按行分割，
     * 调试器通过行号访问源代码。 */
    sds srcstring = sdsdup(c->argv[1]->ptr);
    size_t srclen = sdslen(srcstring);
    // 去除尾部换行符
    while(srclen && (srcstring[srclen-1] == '\n' ||
                     srcstring[srclen-1] == '\r'))
    {
        srcstring[--srclen] = '\0';
    }
    sdssetlen(srcstring,srclen);
    ldb.src = sdssplitlen(srcstring,sdslen(srcstring),"\n",1,&ldb.lines);
    sdsfree(srcstring);
    return 1;
}

/* 在 EVAL 调用（带调试模式）返回后结束调试会话。 */
void ldbEndSession(client *c) {
    /* 发送剩余日志和 <endsession> 标记 */
    ldbLog(sdsnew("<endsession>"));
    ldbSendLogs();

    /* 如果是 fork 出来的会话，子进程直接退出 */
    if (ldb.forked) {
        writeToClient(c,0);
        serverLog(LL_NOTICE,"Lua debugging session child exiting");
        exitFromChild(0);
    } else {
        serverLog(LL_NOTICE,
            "Redis synchronous debugging eval session ended");
    }

    /* 同步模式下恢复客户端状态 */
    connNonBlock(ldb.conn);                     // 恢复非阻塞模式
    connSendTimeout(ldb.conn,0);                // 取消发送超时

    /* 发送最终 EVAL 回复后关闭客户端连接，表示调试会话结束 */
    c->flags |= CLIENT_CLOSE_AFTER_REPLY;

    /* 清理调试器资源 */
    sdsfreesplitres(ldb.src,ldb.lines);
    ldb.lines = 0;
    ldb.active = 0;
}

/* 如果指定的 pid 在 fork 调试会话的子进程列表中，则将其移除。
 * 找到并移除返回非零值，未找到返回 0。 */
int ldbRemoveChild(pid_t pid) {
    listNode *ln = listSearchKey(ldb.children,(void*)(unsigned long)pid);
    if (ln) {
        listDelNode(ldb.children,ln);
        return 1;
    }
    return 0;
}

/* 返回父进程中尚未通过 wait() 收到终止确认的子进程数量 */
int ldbPendingChildren(void) {
    return listLength(ldb.children);
}

/* 终止所有 fork 出的调试会话 */
void ldbKillForkedSessions(void) {
    listIter li;
    listNode *ln;

    listRewind(ldb.children,&li);
    while((ln = listNext(&li))) {
        pid_t pid = (unsigned long) ln->value;
        serverLog(LL_NOTICE,"Killing debugging session %ld",(long)pid);
        kill(pid,SIGKILL);                      // 强制终止子进程
    }
    listRelease(ldb.children);
    ldb.children = listCreate();                // 重建空的子进程列表
}

/* EVAL / EVALSHA 的调试包装函数：启用调试并确保
 * 无论发生什么，EVAL 返回后都会结束调试会话。 */
void evalGenericCommandWithDebugging(client *c, int evalsha) {
    if (ldbStartSession(c)) {
        evalGenericCommand(c,evalsha);
        ldbEndSession(c);
    } else {
        ldbDisable(c);
    }
}

/* 返回 ldb.src 中指定行的源代码指针（行号从 1 开始）。
 * 越界行号返回特殊字符串。 */
char *ldbGetSourceLine(int line) {
    int idx = line-1;
    if (idx < 0 || idx >= ldb.lines) return "<out of range source code line>";
    return ldb.src[idx];
}

/* 检查指定行是否设置了断点 */
int ldbIsBreakpoint(int line) {
    int j;

    for (j = 0; j < ldb.bpcount; j++)
        if (ldb.bp[j] == line) return 1;
    return 0;
}

/* 添加断点到指定行。已达到最大断点数时忽略。
 * 返回 1 表示添加成功（或已存在），0 表示空间不足或行号无效。 */
int ldbAddBreakpoint(int line) {
    if (line <= 0 || line > ldb.lines) return 0;
    if (!ldbIsBreakpoint(line) && ldb.bpcount != LDB_BREAKPOINTS_MAX) {
        ldb.bp[ldb.bpcount++] = line;
        return 1;
    }
    return 0;
}

/* 移除指定行的断点。返回 1 表示已移除，0 表示该行无断点。 */
int ldbDelBreakpoint(int line) {
    int j;

    for (j = 0; j < ldb.bpcount; j++) {
        if (ldb.bp[j] == line) {
            ldb.bpcount--;
            memmove(ldb.bp+j,ldb.bp+j+1,ldb.bpcount-j);  // 前移后续元素
            return 1;
        }
    }
    return 0;
}

/* 从调试客户端查询缓冲区中解析有效的 multi-bulk 命令。
 * 成功时返回 SDS 字符串数组，失败返回 NULL 表示需要读取更多数据。 */
sds *ldbReplParseCommand(int *argcp, char** err) {
    static char* protocol_error = "protocol error";
    sds *argv = NULL;
    int argc = 0;
    if (sdslen(ldb.cbuf) == 0) return NULL;

    /* 在副本上操作更简单，可以自由修改以便解析 */
    sds copy = sdsdup(ldb.cbuf);
    char *p = copy;

    /* 简化的 Redis 协议解析器，仅适用于此上下文，
     * 对不完整协议有较好的容错性。 */

    /* 解析 *<count>\r\n（multi-bulk 长度） */
    p = strchr(p,'*'); if (!p) goto protoerr;
    char *plen = p+1; /* multi-bulk 长度指针 */
    p = strstr(p,"\r\n"); if (!p) goto keep_reading;
    *p = '\0'; p += 2;
    *argcp = atoi(plen);
    if (*argcp <= 0 || *argcp > 1024) goto protoerr;

    /* 解析每个参数 */
    argv = zmalloc(sizeof(sds)*(*argcp));
    argc = 0;
    while(argc < *argcp) {
        /* 到达末尾但应有更多数据需要读取 */
        if (*p == '\0') goto keep_reading;

        if (*p != '$') goto protoerr;
        plen = p+1; /* bulk 字符串长度指针 */
        p = strstr(p,"\r\n"); if (!p) goto keep_reading;
        *p = '\0'; p += 2;
        int slen = atoi(plen); /* 当前参数的长度 */
        if (slen <= 0 || slen > 1024) goto protoerr;
        if ((size_t)(p + slen + 2 - copy) > sdslen(copy) ) goto keep_reading;
        argv[argc++] = sdsnewlen(p,slen);
        p += slen; /* 跳过已解析的参数 */
        if (p[0] != '\r' || p[1] != '\n') goto protoerr;
        p += 2; /* 跳过 \r\n */
    }
    sdsfree(copy);
    return argv;

protoerr:
    *err = protocol_error;
keep_reading:
    sdsfreesplitres(argv,argc);
    sdsfree(copy);
    return NULL;
}

/* 在 Lua 调试器输出中记录指定行的源代码。
 * 前缀标记：-> 当前行，# 断点，-># 两者兼具 */
void ldbLogSourceLine(int lnum) {
    char *line = ldbGetSourceLine(lnum);
    char *prefix;
    int bp = ldbIsBreakpoint(lnum);
    int current = ldb.currentline == lnum;

    if (current && bp)
        prefix = "->#";             // 当前行且有断点
    else if (current)
        prefix = "-> ";             // 当前行
    else if (bp)
        prefix = "  #";             // 有断点
    else
        prefix = "   ";             // 普通行
    sds thisline = sdscatprintf(sdsempty(),"%s%-3d %s", prefix, lnum, line);
    ldbLog(thisline);
}

/* 实现 Lua 调试器的 "list" 命令。
 * around 为 0 时列出整个文件；否则只显示指定行附近的代码。
 * context 参数指定前后显示的行数。 */
void ldbList(int around, int context) {
    int j;

    for (j = 1; j <= ldb.lines; j++) {
        if (around != 0 && abs(around-j) > context) continue;
        ldbLogSourceLine(j);
    }
}

/* 将 Lua 栈上 'idx' 位置的值的人类可读表示追加到 SDS 字符串。
 * 返回追加后的新 SDS 字符串。用于实现 ldbLogStackValue()。
 *
 * 元素不会从栈中移除，也不会被类型转换。 */
#define LDB_MAX_VALUES_DEPTH (LUA_MINSTACK/2)  // 最大递归深度
sds ldbCatStackValueRec(sds s, lua_State *lua, int idx, int level) {
    int t = lua_type(lua,idx);

    if (level++ == LDB_MAX_VALUES_DEPTH)
        return sdscat(s,"<max recursion level reached! Nested table?>");

    switch(t) {
    case LUA_TSTRING:
        {
        size_t strl;
        char *strp = (char*)lua_tolstring(lua,idx,&strl);
        s = sdscatrepr(s,strp,strl);           // 用引号包裹的字符串表示
        }
        break;
    case LUA_TBOOLEAN:
        s = sdscat(s,lua_toboolean(lua,idx) ? "true" : "false");
        break;
    case LUA_TNUMBER:
        s = sdscatprintf(s,"%g",(double)lua_tonumber(lua,idx));
        break;
    case LUA_TNIL:
        s = sdscatlen(s,"nil",3);
        break;
    case LUA_TTABLE:
        {
        int expected_index = 1;    // 数组的预期起始索引
        int is_array = 1;          // 检查失败时会被设为 0
        /* 同时构建两种表示：一种假设是数组，一种不是。
         * 最终根据检查结果选择正确的一种。 */
        sds repr1 = sdsempty();    // 数组表示
        sds repr2 = sdsempty();    // 完整键值对表示
        lua_pushnil(lua);          // 以 nil 作为第一个键开始迭代
        while (lua_next(lua,idx-1)) {
            /* 检查到目前为止是否仍像数组 */
            if (is_array &&
                (lua_type(lua,-2) != LUA_TNUMBER ||
                 lua_tonumber(lua,-2) != expected_index)) is_array = 0;
            /* 栈状态：table, key, value */
            /* 数组表示 */
            repr1 = ldbCatStackValueRec(repr1,lua,-1,level);
            repr1 = sdscatlen(repr1,"; ",2);
            /* 完整键值对表示 */
            repr2 = sdscatlen(repr2,"[",1);
            repr2 = ldbCatStackValueRec(repr2,lua,-2,level);
            repr2 = sdscatlen(repr2,"]=",2);
            repr2 = ldbCatStackValueRec(repr2,lua,-1,level);
            repr2 = sdscatlen(repr2,"; ",2);
            lua_pop(lua,1);        // 弹出 value，栈：table, key
            expected_index++;
        }
        /* 去除末尾的 " ;" */
        if (sdslen(repr1)) sdsrange(repr1,0,-3);
        if (sdslen(repr2)) sdsrange(repr2,0,-3);
        /* 选择正确的表示并释放另一个 */
        s = sdscatlen(s,"{",1);
        s = sdscatsds(s,is_array ? repr1 : repr2);
        s = sdscatlen(s,"}",1);
        sdsfree(repr1);
        sdsfree(repr2);
        }
        break;
    case LUA_TFUNCTION:
    case LUA_TUSERDATA:
    case LUA_TTHREAD:
    case LUA_TLIGHTUSERDATA:
        {
        const void *p = lua_topointer(lua,idx);
        char *typename = "unknown";
        if (t == LUA_TFUNCTION) typename = "function";
        else if (t == LUA_TUSERDATA) typename = "userdata";
        else if (t == LUA_TTHREAD) typename = "thread";
        else if (t == LUA_TLIGHTUSERDATA) typename = "light-userdata";
        s = sdscatprintf(s,"\"%s@%p\"",typename,p);  // 输出类型名和内存地址
        }
        break;
    default:
        s = sdscat(s,"\"<unknown-lua-type>\"");
        break;
    }
    return s;
}

/* ldbCatStackValueRec() 的高层包装，递归层级从 0 开始 */
sds ldbCatStackValue(sds s, lua_State *lua, int idx) {
    return ldbCatStackValueRec(s,lua,idx,0);
}

/* 生成调试器日志条目，表示 Lua 栈顶对象的值。
 * 元素不会从栈中弹出或修改。
 * 具体实现参见 ldbCatStackValue()。 */
void ldbLogStackValue(lua_State *lua, char *prefix) {
    sds s = sdsnew(prefix);
    s = ldbCatStackValue(s,lua,-1);
    ldbLogWithMaxLen(s);
}

/* 前向声明：将各种 RESP 类型转换为人类可读格式的辅助函数 */
char *ldbRedisProtocolToHuman_Int(sds *o, char *reply);       // 整数类型
char *ldbRedisProtocolToHuman_Bulk(sds *o, char *reply);      // 批量字符串
char *ldbRedisProtocolToHuman_Status(sds *o, char *reply);    // 状态/错误回复
char *ldbRedisProtocolToHuman_MultiBulk(sds *o, char *reply); // 数组
char *ldbRedisProtocolToHuman_Set(sds *o, char *reply);       // 集合
char *ldbRedisProtocolToHuman_Map(sds *o, char *reply);       // 映射
char *ldbRedisProtocolToHuman_Null(sds *o, char *reply);       // 空值类型
char *ldbRedisProtocolToHuman_Bool(sds *o, char *reply);      // 布尔类型
char *ldbRedisProtocolToHuman_Double(sds *o, char *reply);    // 双精度浮点

/* 将 Redis 协议回复转换为人类可读格式并追加到 SDS 字符串 'o'。
 *
 * SDS 字符串通过引用传递（二级指针），以便返回修改后的指针，
 * 符合 SDS 语义。 */
char *ldbRedisProtocolToHuman(sds *o, char *reply) {
    char *p = reply;
    switch(*p) {
    case ':': p = ldbRedisProtocolToHuman_Int(o,reply); break;        // : 整数
    case '$': p = ldbRedisProtocolToHuman_Bulk(o,reply); break;       // $ 批量字符串
    case '+': p = ldbRedisProtocolToHuman_Status(o,reply); break;     // + 状态回复
    case '-': p = ldbRedisProtocolToHuman_Status(o,reply); break;     // - 错误回复
    case '*': p = ldbRedisProtocolToHuman_MultiBulk(o,reply); break;  // * 数组
    case '~': p = ldbRedisProtocolToHuman_Set(o,reply); break;        // ~ 集合
    case '%': p = ldbRedisProtocolToHuman_Map(o,reply); break;        // % 映射
    case '_': p = ldbRedisProtocolToHuman_Null(o,reply); break;       // _ 空值
    case '#': p = ldbRedisProtocolToHuman_Bool(o,reply); break;       // # 布尔
    case ',': p = ldbRedisProtocolToHuman_Double(o,reply); break;     // , 双精度浮点
    }
    return p;
}

/* 以下函数是 ldbRedisProtocolToHuman() 的辅助函数，
 * 分别处理各种 Redis 返回类型。 */

/* 将 RESP 整数回复（:xxx\r\n）转为可读格式 */
char *ldbRedisProtocolToHuman_Int(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    *o = sdscatlen(*o,reply+1,p-reply-1);
    return p+2;
}

/* 将 RESP 批量字符串回复（$len\r\n...）转为可读格式 */
char *ldbRedisProtocolToHuman_Bulk(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    long long bulklen;

    string2ll(reply+1,p-reply-1,&bulklen);
    if (bulklen == -1) {
        *o = sdscatlen(*o,"NULL",4);                // 空批量字符串
        return p+2;
    } else {
        *o = sdscatrepr(*o,p+2,bulklen);            // 带引号的字符串表示
        return p+2+bulklen+2;
    }
}

/* 将 RESP 状态/错误回复（+/-xxx\r\n）转为可读格式 */
char *ldbRedisProtocolToHuman_Status(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');

    *o = sdscatrepr(*o,reply,p-reply);
    return p+2;
}

/* 将 RESP 数组回复（*count\r\n...）转为可读格式 */
char *ldbRedisProtocolToHuman_MultiBulk(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    long long mbulklen;
    int j = 0;

    string2ll(reply+1,p-reply-1,&mbulklen);
    p += 2;
    if (mbulklen == -1) {
        *o = sdscatlen(*o,"NULL",4);                // 空数组
        return p;
    }
    *o = sdscatlen(*o,"[",1);
    for (j = 0; j < mbulklen; j++) {
        p = ldbRedisProtocolToHuman(o,p);           // 递归解析每个元素
        if (j != mbulklen-1) *o = sdscatlen(*o,",",1);
    }
    *o = sdscatlen(*o,"]",1);
    return p;
}

/* 将 RESP 集合回复（~count\r\n...）转为可读格式 */
char *ldbRedisProtocolToHuman_Set(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    long long mbulklen;
    int j = 0;

    string2ll(reply+1,p-reply-1,&mbulklen);
    p += 2;
    *o = sdscatlen(*o,"~(",2);
    for (j = 0; j < mbulklen; j++) {
        p = ldbRedisProtocolToHuman(o,p);
        if (j != mbulklen-1) *o = sdscatlen(*o,",",1);
    }
    *o = sdscatlen(*o,")",1);
    return p;
}

/* 将 RESP 映射回复（%count\r\n...）转为可读格式 */
char *ldbRedisProtocolToHuman_Map(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    long long mbulklen;
    int j = 0;

    string2ll(reply+1,p-reply-1,&mbulklen);
    p += 2;
    *o = sdscatlen(*o,"{",1);
    for (j = 0; j < mbulklen; j++) {
        p = ldbRedisProtocolToHuman(o,p);           // 解析键
        *o = sdscatlen(*o," => ",4);
        p = ldbRedisProtocolToHuman(o,p);           // 解析值
        if (j != mbulklen-1) *o = sdscatlen(*o,",",1);
    }
    *o = sdscatlen(*o,"}",1);
    return p;
}

/* 将 RESP 空值回复（_\r\n）转为可读格式 */
char *ldbRedisProtocolToHuman_Null(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    *o = sdscatlen(*o,"(null)",6);
    return p+2;
}

/* 将 RESP 布尔回复（#t/f\r\n）转为可读格式 */
char *ldbRedisProtocolToHuman_Bool(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    if (reply[1] == 't')
        *o = sdscatlen(*o,"#true",5);
    else
        *o = sdscatlen(*o,"#false",6);
    return p+2;
}

/* 将 RESP 双精度浮点回复（,xxx\r\n）转为可读格式 */
char *ldbRedisProtocolToHuman_Double(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    *o = sdscatlen(*o,"(double) ",9);
    *o = sdscatlen(*o,reply+1,p-reply-1);
    return p+2;
}

/* 将 Redis 回复以人类可读格式记录为调试器输出。
 * 超过长度限制的字符串会被截断。 */
void ldbLogRedisReply(char *reply) {
    sds log = sdsnew("<reply> ");
    ldbRedisProtocolToHuman(&log,reply);
    ldbLogWithMaxLen(log);
}

/* 实现 Lua 调试器的 "print <var>" 命令。
 * 从当前栈帧向上扫描，查找名为 varname 的 Lua 变量，
 * 找到的第一个匹配变量会被打印。 */
void ldbPrint(lua_State *lua, char *varname) {
    lua_Debug ar;

    int l = 0; /* 栈层级 */
    while (lua_getstack(lua,l,&ar) != 0) {
        l++;
        const char *name;
        int i = 1; /* 变量索引 */
        while((name = lua_getlocal(lua,&ar,i)) != NULL) {
            i++;
            if (strcmp(varname,name) == 0) {
                ldbLogStackValue(lua,"<value> ");   // 找到变量，打印其值
                lua_pop(lua,1);
                return;
            } else {
                lua_pop(lua,1); /* 弹出不匹配的变量名 */
            }
        }
    }

    /* 特殊情况：尝试查找全局变量 ARGV 和 KEYS */
    if (!strcmp(varname,"ARGV") || !strcmp(varname,"KEYS")) {
        lua_getglobal(lua, varname);
        ldbLogStackValue(lua,"<value> ");
        lua_pop(lua,1);
    } else {
        ldbLog(sdsnew("No such variable."));
    }
}

/* 实现 Lua 调试器的 "print" 命令（无参数）。
 * 打印当前栈帧中的所有局部变量。 */
void ldbPrintAll(lua_State *lua) {
    lua_Debug ar;
    int vars = 0;

    if (lua_getstack(lua,0,&ar) != 0) {
        const char *name;
        int i = 1; /* 变量索引 */
        while((name = lua_getlocal(lua,&ar,i)) != NULL) {
            i++;
            if (!strstr(name,"(*temporary)")) {     // 跳过临时变量
                sds prefix = sdscatprintf(sdsempty(),"<value> %s = ",name);
                ldbLogStackValue(lua,prefix);
                sdsfree(prefix);
                vars++;
            }
            lua_pop(lua,1);
        }
    }

    if (vars == 0) {
        ldbLog(sdsnew("No local variables in the current context."));
    }
}

/* 实现 break 命令：列出、添加和移除断点 */
void ldbBreak(sds *argv, int argc) {
    if (argc == 1) {
        if (ldb.bpcount == 0) {
            ldbLog(sdsnew("No breakpoints set. Use 'b <line>' to add one."));
            return;
        } else {
            ldbLog(sdscatfmt(sdsempty(),"%i breakpoints set:",ldb.bpcount));
            int j;
            for (j = 0; j < ldb.bpcount; j++)
                ldbLogSourceLine(ldb.bp[j]);
        }
    } else {
        int j;
        for (j = 1; j < argc; j++) {
            char *arg = argv[j];
            long line;
            if (!string2l(arg,sdslen(arg),&line)) {
                ldbLog(sdscatfmt(sdsempty(),"Invalid argument:'%s'",arg));
            } else {
                if (line == 0) {
                    ldb.bpcount = 0;
                    ldbLog(sdsnew("All breakpoints removed."));
                } else if (line > 0) {
                    if (ldb.bpcount == LDB_BREAKPOINTS_MAX) {
                        ldbLog(sdsnew("Too many breakpoints set."));
                    } else if (ldbAddBreakpoint(line)) {
                        ldbList(line,1);
                    } else {
                        ldbLog(sdsnew("Wrong line number."));
                    }
                } else if (line < 0) {
                    if (ldbDelBreakpoint(-line))
                        ldbLog(sdsnew("Breakpoint removed."));
                    else
                        ldbLog(sdsnew("No breakpoint in the specified line."));
                }
            }
        }
    }
}

/* 实现 Lua 调试器的 "eval" 命令。
 * 编译用户传入的代码片段并执行，显示栈上剩余的结果。 */
void ldbEval(lua_State *lua, sds *argv, int argc) {
    /* 将多个参数拼接为一个代码字符串 */
    sds code = sdsjoinsds(argv+1,argc-1," ",1);
    sds expr = sdscatsds(sdsnew("return "),code);

    /* 先尝试作为表达式编译（加上 "return " 前缀） */
    if (luaL_loadbuffer(lua,expr,sdslen(expr),"@ldb_eval")) {
        lua_pop(lua,1);
        /* 失败则尝试作为语句编译 */
        if (luaL_loadbuffer(lua,code,sdslen(code),"@ldb_eval")) {
            ldbLog(sdscatfmt(sdsempty(),"<error> %s",lua_tostring(lua,-1)));
            lua_pop(lua,1);
            sdsfree(code);
            sdsfree(expr);
            return;
        }
    }

    /* 执行编译后的代码 */
    sdsfree(code);
    sdsfree(expr);
    if (lua_pcall(lua,0,1,0)) {
        ldbLog(sdscatfmt(sdsempty(),"<error> %s",lua_tostring(lua,-1)));
        lua_pop(lua,1);
        return;
    }
    ldbLogStackValue(lua,"<retval> ");          // 打印返回值
    lua_pop(lua,1);
}

/* 实现调试器的 "redis" 命令。
 * 技巧：直接调用 Lua 的 redis.call() 实现，并启用 ldb.step，
 * 这样 Redis 命令及其回复会作为副作用被记录到调试日志中。 */
void ldbRedis(lua_State *lua, sds *argv, int argc) {
    int j;

    if (!lua_checkstack(lua, argc + 1)) {
        /* 需要时扩展 Lua 栈空间以确保能压入 argc + 1 个元素。
         * 最坏情况：1(redis 表) + 1(redis.call 函数) + (argc-1)(用户参数) = argc + 1 */
        ldbLogRedisReply("max lua stack reached");
        return;
    }

    lua_getglobal(lua,"redis");             // 获取 redis 全局表
    lua_pushstring(lua,"call");
    lua_gettable(lua,-2);                   // 栈：redis, redis.call
    for (j = 1; j < argc; j++)
        lua_pushlstring(lua,argv[j],sdslen(argv[j]));
    ldb.step = 1;                           // 强制 redis.call() 记录日志
    lua_pcall(lua,argc-1,1,0);              // 执行 redis.call()，栈：redis, result
    ldb.step = 0;                           // 禁用日志记录
    lua_pop(lua,2);                         // 弹出结果并清理栈
}

/* 实现 Lua 调试器的 "trace" 命令。
 * 查询 Lua 调用栈并打印从当前帧到最外层的回溯信息。 */
void ldbTrace(lua_State *lua) {
    lua_Debug ar;
    int level = 0;

    while(lua_getstack(lua,level,&ar)) {
        lua_getinfo(lua,"Snl",&ar);
        if(strstr(ar.short_src,"user_script") != NULL) {
            ldbLog(sdscatprintf(sdsempty(),"%s %s:",
                (level == 0) ? "In" : "From",
                ar.name ? ar.name : "top level"));
            ldbLogSourceLine(ar.currentline);
        }
        level++;
    }
    if (level == 0) {
        ldbLog(sdsnew("<error> Can't retrieve Lua stack."));
    }
}

/* 实现调试器的 "maxlen" 命令：查询或设置 ldb.maxlen 变量，
 * 控制回复和变量转储的最大长度。 */
void ldbMaxlen(sds *argv, int argc) {
    if (argc == 2) {
        int newval = atoi(argv[1]);
        ldb.maxlen_hint_sent = 1; /* 用户已知晓此命令，不再提示 */
        if (newval != 0 && newval <= 60) newval = 60;  // 最小有效值为 60
        ldb.maxlen = newval;
    }
    if (ldb.maxlen) {
        ldbLog(sdscatprintf(sdsempty(),"<value> replies are truncated at %d bytes.",(int)ldb.maxlen));
    } else {
        ldbLog(sdscatprintf(sdsempty(),"<value> replies are unlimited."));
    }
}

/* 从调试客户端读取并执行调试命令。
 * 返回 C_OK 表示调试会话继续，C_ERR 表示客户端断开或超时。 */
int ldbRepl(lua_State *lua) {
    sds *argv;
    int argc;
    char* err = NULL;

    /* 持续处理命令，直到遇到需要返回 Lua 解释器的命令（如 step/continue） */
    while(1) {
        while((argv = ldbReplParseCommand(&argc, &err)) == NULL) {
            char buf[1024];
            if (err) {
                luaPushError(lua, err);
                luaError(lua);
            }
            int nread = connRead(ldb.conn,buf,sizeof(buf));
            if (nread <= 0) {
                /* 客户端已断开，确保脚本在无用户输入情况下继续运行 */
                ldb.step = 0;
                ldb.bpcount = 0;
                return C_ERR;
            }
            ldb.cbuf = sdscatlen(ldb.cbuf,buf,nread);
            /* 缓冲区超过 1MB 时退出，防止客户端耗尽内存 */
            if (sdslen(ldb.cbuf) > 1<<20) {
                sdsfree(ldb.cbuf);
                ldb.cbuf = sdsempty();
                luaPushError(lua, "max client buffer reached");
                luaError(lua);
            }
        }

        /* 清空旧缓冲区 */
        sdsfree(ldb.cbuf);
        ldb.cbuf = sdsempty();

        /* 执行调试命令 */
        if (!strcasecmp(argv[0],"h") || !strcasecmp(argv[0],"help")) {
ldbLog(sdsnew("Redis Lua debugger help:"));
ldbLog(sdsnew("[h]elp               Show this help."));
ldbLog(sdsnew("[s]tep               Run current line and stop again."));
ldbLog(sdsnew("[n]ext               Alias for step."));
ldbLog(sdsnew("[c]ontinue           Run till next breakpoint."));
ldbLog(sdsnew("[l]ist               List source code around current line."));
ldbLog(sdsnew("[l]ist [line]        List source code around [line]."));
ldbLog(sdsnew("                     line = 0 means: current position."));
ldbLog(sdsnew("[l]ist [line] [ctx]  In this form [ctx] specifies how many lines"));
ldbLog(sdsnew("                     to show before/after [line]."));
ldbLog(sdsnew("[w]hole              List all source code. Alias for 'list 1 1000000'."));
ldbLog(sdsnew("[p]rint              Show all the local variables."));
ldbLog(sdsnew("[p]rint <var>        Show the value of the specified variable."));
ldbLog(sdsnew("                     Can also show global vars KEYS and ARGV."));
ldbLog(sdsnew("[b]reak              Show all breakpoints."));
ldbLog(sdsnew("[b]reak <line>       Add a breakpoint to the specified line."));
ldbLog(sdsnew("[b]reak -<line>      Remove breakpoint from the specified line."));
ldbLog(sdsnew("[b]reak 0            Remove all breakpoints."));
ldbLog(sdsnew("[t]race              Show a backtrace."));
ldbLog(sdsnew("[e]val <code>        Execute some Lua code (in a different callframe)."));
ldbLog(sdsnew("[r]edis <cmd>        Execute a Redis command."));
ldbLog(sdsnew("[m]axlen [len]       Trim logged Redis replies and Lua var dumps to len."));
ldbLog(sdsnew("                     Specifying zero as <len> means unlimited."));
ldbLog(sdsnew("[a]bort              Stop the execution of the script. In sync"));
ldbLog(sdsnew("                     mode dataset changes will be retained."));
ldbLog(sdsnew(""));
ldbLog(sdsnew("Debugger functions you can call from Lua scripts:"));
ldbLog(sdsnew("redis.debug()        Produce logs in the debugger console."));
ldbLog(sdsnew("redis.breakpoint()   Stop execution like if there was a breakpoint in the"));
ldbLog(sdsnew("                     next line of code."));
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"s") || !strcasecmp(argv[0],"step") ||
                   !strcasecmp(argv[0],"n") || !strcasecmp(argv[0],"next")) {
            ldb.step = 1;
            break;
        } else if (!strcasecmp(argv[0],"c") || !strcasecmp(argv[0],"continue")){
            break;
        } else if (!strcasecmp(argv[0],"t") || !strcasecmp(argv[0],"trace")) {
            ldbTrace(lua);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"m") || !strcasecmp(argv[0],"maxlen")) {
            ldbMaxlen(argv,argc);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"b") || !strcasecmp(argv[0],"break")) {
            ldbBreak(argv,argc);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"e") || !strcasecmp(argv[0],"eval")) {
            ldbEval(lua,argv,argc);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"a") || !strcasecmp(argv[0],"abort")) {
            luaPushError(lua, "script aborted for user request");
            luaError(lua);
        } else if (argc > 1 &&
                   (!strcasecmp(argv[0],"r") || !strcasecmp(argv[0],"redis"))) {
            ldbRedis(lua,argv,argc);
            ldbSendLogs();
        } else if ((!strcasecmp(argv[0],"p") || !strcasecmp(argv[0],"print"))) {
            if (argc == 2)
                ldbPrint(lua,argv[1]);
            else
                ldbPrintAll(lua);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"l") || !strcasecmp(argv[0],"list")){
            int around = ldb.currentline, ctx = 5;
            if (argc > 1) {
                int num = atoi(argv[1]);
                if (num > 0) around = num;
            }
            if (argc > 2) ctx = atoi(argv[2]);
            ldbList(around,ctx);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"w") || !strcasecmp(argv[0],"whole")){
            ldbList(1,1000000);
            ldbSendLogs();
        } else {
            ldbLog(sdsnew("<error> Unknown Redis Lua debugger command or "
                          "wrong number of arguments."));
            ldbSendLogs();
        }

        /* 释放命令参数数组 */
        sdsfreesplitres(argv,argc);
    }

    /* 如果在 while 循环内 break，释放当前命令 argv */
    sdsfreesplitres(argv,argc);
    return C_OK;
}

/* Lua 调试器的核心：Lua 每次即将执行新行时调用此钩子函数。 */
void luaLdbLineHook(lua_State *lua, lua_Debug *ar) {
    scriptRunCtx* rctx = luaGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    serverAssert(rctx); // 仅在脚本调用内部有效
    lua_getstack(lua,0,ar);
    lua_getinfo(lua,"Sl",ar);
    ldb.currentline = ar->currentline;

    int bp = ldbIsBreakpoint(ldb.currentline) || ldb.luabp;  // 检查是否有断点
    int timeout = 0;

    /* 忽略用户脚本之外的事件 */
    if(strstr(ar->short_src,"user_script") == NULL) return;

    /* 检查是否超时（通过 LUA_HOOKCOUNT 事件） */
    if (ar->event == LUA_HOOKCOUNT && ldb.step == 0 && bp == 0) {
        mstime_t elapsed = elapsedMs(rctx->start_time);
        mstime_t timelimit = server.busy_reply_threshold ?
                             server.busy_reply_threshold : 5000;
        if (elapsed >= timelimit) {
            timeout = 1;
            ldb.step = 1;                       // 超时时强制单步停止
        } else {
            return; /* 未超时，忽略 COUNT 事件 */
        }
    }

    if (ldb.step || bp) {
        /* 确定停止原因 */
        char *reason = "step over";
        if (bp) reason = ldb.luabp ? "redis.breakpoint() called" :
                                     "break point";
        else if (timeout) reason = "timeout reached, infinite loop?";
        ldb.step = 0;
        ldb.luabp = 0;
        ldbLog(sdscatprintf(sdsempty(),
            "* Stopped at %d, stop reason = %s",
            ldb.currentline, reason));
        ldbLogSourceLine(ldb.currentline);
        ldbSendLogs();
        if (ldbRepl(lua) == C_ERR && timeout) {
            /* 客户端断开且发生超时：终止脚本，否则进程将无限阻塞 */
            luaPushError(lua, "timeout during Lua debugging with client closing connection");
            luaError(lua);
        }
        rctx->start_time = getMonotonicUs();    // 重置计时起点
    }
}
