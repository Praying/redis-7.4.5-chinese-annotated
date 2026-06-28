/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

/* 脚本执行引擎核心实现
 * 提供 Lua/Function 脚本的生命周期管理、命令校验、
 * 超时中断、集群状态验证及内存分配等功能 */

#include "server.h"    /* 服务器核心头文件，包含全局状态与通用工具 */
#include "script.h"    /* 脚本子系统头文件，定义脚本标志与运行上下文 */
#include "cluster.h"   /* 集群子系统头文件，用于集群状态与槽位校验 */

#include <lua.h>       /* Lua C API 核心头文件 */
#include <lauxlib.h>   /* Lua 辅助库头文件 */

/* 脚本标志定义数组
 * 将脚本内部标志位映射到可读字符串名称，
 * 用于 Function 注册时的标志声明与解析。
 * 以 {0, NULL} 作为数组终止哨兵 */
scriptFlag scripts_flags_def[] = {
    {.flag = SCRIPT_FLAG_NO_WRITES, .str = "no-writes"},       /* 禁止写操作 */
    {.flag = SCRIPT_FLAG_ALLOW_OOM, .str = "allow-oom"},       /* 允许在 OOM 时执行 */
    {.flag = SCRIPT_FLAG_ALLOW_STALE, .str = "allow-stale"},   /* 允许在过期副本上执行 */
    {.flag = SCRIPT_FLAG_NO_CLUSTER, .str = "no-cluster"},     /* 禁止在集群模式下执行 */
    {.flag = SCRIPT_FLAG_ALLOW_CROSS_SLOT, .str = "allow-cross-slot-keys"}, /* 允许跨槽访问 */
    {.flag = 0, .str = NULL}, /* 标志数组结束 */
};

/* 脚本执行时，保存当前运行上下文的全局指针。
 * 同一时刻最多只有一个脚本在执行。 */
static scriptRunCtx *curr_run_ctx = NULL;

/* 退出脚本超时模式
 * 清除超时标志，结束阻塞操作状态，
 * 并将主节点连接重新加入事件处理队列 */
static void exitScriptTimedoutMode(scriptRunCtx *run_ctx) {
    serverAssert(run_ctx == curr_run_ctx);
    serverAssert(scriptIsTimedout());
    run_ctx->flags &= ~SCRIPT_TIMEDOUT;   /* 清除超时标志 */
    blockingOperationEnds();               /* 结束阻塞操作状态 */
    /* 如果当前是副本且有活跃的主节点连接，将其重新加入处理队列 */
    if (server.masterhost && server.master) queueClientForReprocessing(server.master);
}

/* 进入脚本超时模式
 * 标记脚本已超时，并通知服务器进入阻塞操作状态 */
static void enterScriptTimedoutMode(scriptRunCtx *run_ctx) {
    serverAssert(run_ctx == curr_run_ctx);
    serverAssert(!scriptIsTimedout());
    run_ctx->flags |= SCRIPT_TIMEDOUT;    /* 标记脚本已超时 */
    blockingOperationStarts();             /* 通知服务器开始阻塞操作 */
}

#if defined(USE_JEMALLOC)
/* Lua 使用 jemalloc 时的自定义内存分配器
 * 作为 lua_newstate 的参数传入，将 Lua VM 的内存
 * 分配限定在专用 arena 和 tcache 中，
 * 避免与主服务器内存分配互相干扰 */
static void *luaAlloc(void *ud, void *ptr, size_t osize, size_t nsize) {
    UNUSED(osize);

    /* ud 携带的是 tcache ID，通过指针转换得到 */
    unsigned int tcache = (unsigned int)(uintptr_t)ud;
    if (nsize == 0) {
        /* 释放内存：使用专用 arena 和 tcache */
        zfree_with_flags(ptr, MALLOCX_ARENA(server.lua_arena) | MALLOCX_TCACHE(tcache));
        return NULL;
    } else {
        /* 分配或重新分配内存 */
        return zrealloc_with_flags(ptr, nsize, MALLOCX_ARENA(server.lua_arena) | MALLOCX_TCACHE(tcache));
    }
}

/* 创建 Lua 解释器，使用 jemalloc 作为 Lua 内存分配器 */
lua_State *createLuaState(void) {
    /* 每次创建 Lua VM 时，都会创建一个新的私有 tcache。
     * 该私有 tcache 在 Lua VM 关闭后销毁。 */
    unsigned int tcache;
    size_t sz = sizeof(unsigned int);
    int err = je_mallctl("tcache.create", (void *)&tcache, &sz, NULL, 0);
    if (err) {
        serverLog(LL_WARNING, "Failed creating the lua jemalloc tcache (err=%d).", err);
        exit(1);
    }

    /* 将 tcache 作为 ud 传入，使其不与服务器全局状态绑定 */
    return lua_newstate(luaAlloc, (void *)(uintptr_t)tcache);
}

/* 使用 jemalloc 时，需要为 Lua 创建专用的 arena，
 * 以避免 Lua 的内存分配阻塞内存碎片整理器（defragger） */
void luaEnvInit(void) {
    unsigned int arena;
    size_t sz = sizeof(unsigned int);
    /* 通过 jemalloc mallctl 接口创建新 arena */
    int err = je_mallctl("arenas.create", (void *)&arena, &sz, NULL, 0);
    if (err) {
        serverLog(LL_WARNING, "Failed creating the lua jemalloc arena (err=%d).", err);
        exit(1);
    }
    server.lua_arena = arena;  /* 保存 arena 编号供后续使用 */
}

#else

/* 创建 Lua 解释器，使用 glibc 默认分配器 */
lua_State *createLuaState(void) {
    return lua_open();   /* 使用 Lua 默认的分配器创建 VM */
}

/* glibc 模式下无需额外初始化，
 * 将 lua_arena 设为无效值 UINT_MAX 表示不使用专用 arena */
void luaEnvInit(void) {
    server.lua_arena = UINT_MAX;
}

#endif

/* 判断当前脚本是否已进入超时状态 */
int scriptIsTimedout(void) {
    return scriptIsRunning() && (curr_run_ctx->flags & SCRIPT_TIMEDOUT);
}

/* 获取脚本执行所使用的伪客户端（用于实际执行命令） */
client* scriptGetClient(void) {
    serverAssert(scriptIsRunning());
    return curr_run_ctx->c;
}

/* 获取触发脚本执行的原始客户端（即发送 EVAL/FCALL 的客户端） */
client* scriptGetCaller(void) {
    serverAssert(scriptIsRunning());
    return curr_run_ctx->original_client;
}

/* 脚本中断函数
 * 应当在脚本执行期间定期调用，用于：
 * 1. 响应特殊命令（如 PING）
 * 2. 检查脚本是否超时或被杀死
 * 返回 SCRIPT_CONTINUE 继续执行，或 SCRIPT_KILL 终止脚本 */
int scriptInterrupt(scriptRunCtx *run_ctx) {
    if (run_ctx->flags & SCRIPT_TIMEDOUT) {
        /* 脚本已超时，只需处理挂起的事件后返回 */
        processEventsWhileBlocked();
        return (run_ctx->flags & SCRIPT_KILLED) ? SCRIPT_KILL : SCRIPT_CONTINUE;
    }

    /* 检查脚本执行时间是否超过 busy-reply 阈值 */
    long long elapsed = elapsedMs(run_ctx->start_time);
    if (elapsed < server.busy_reply_threshold) {
        return SCRIPT_CONTINUE;  /* 未超时，继续执行 */
    }

    /* 输出慢脚本警告日志 */
    serverLog(LL_WARNING,
            "Slow script detected: still in execution after %lld milliseconds. "
                    "You can try killing the script using the %s command. Script name is: %s.",
            elapsed, (run_ctx->flags & SCRIPT_EVAL_MODE) ? "SCRIPT KILL" : "FUNCTION KILL", run_ctx->funcname);

    /* 进入超时模式 */
    enterScriptTimedoutMode(run_ctx);
    /* 脚本超时后会重新进入事件循环以允许其他命令执行。
     * 因此需要保护正在执行脚本的原始客户端，防止其在事件循环中
     * 被断开连接，导致 EVAL 命令返回时客户端已不存在。 */
    protectClient(run_ctx->original_client);

    /* 在阻塞期间处理待处理的事件（如 PING 等） */
    processEventsWhileBlocked();

    return (run_ctx->flags & SCRIPT_KILLED) ? SCRIPT_KILL : SCRIPT_CONTINUE;
}

/* 将脚本标志转换为命令标志
 * 脚本声明的标志会覆盖从命令本身继承的标志，
 * 从而精确控制脚本内命令的执行权限 */
uint64_t scriptFlagsToCmdFlags(uint64_t cmd_flags, uint64_t script_flags) {
    /* 如果脚本声明了标志，清除命令原有的相关标志，改用脚本声明的 */
    cmd_flags &= ~(CMD_STALE | CMD_DENYOOM | CMD_WRITE);

    /* NO_WRITES 隐含 ALLOW_OOM：只读脚本不会增加内存使用 */
    if (!(script_flags & (SCRIPT_FLAG_ALLOW_OOM | SCRIPT_FLAG_NO_WRITES)))
        cmd_flags |= CMD_DENYOOM;   /* 非 allow-oom 且非 no-writes 则拒绝 OOM */
    if (!(script_flags & SCRIPT_FLAG_NO_WRITES))
        cmd_flags |= CMD_WRITE;     /* 非 no-writes 则标记为写命令 */
    if (script_flags & SCRIPT_FLAG_ALLOW_STALE)
        cmd_flags |= CMD_STALE;     /* 允许在过期副本上执行 */

    /* 清除 MAY_REPLICATE 标志，因为脚本标志已明确是否允许写操作 */
    cmd_flags &= ~CMD_MAY_REPLICATE;

    return cmd_flags;
}

/* 准备脚本运行上下文
 * 在脚本执行前进行各种前置校验（集群、只读、OOM 等），
 * 初始化运行上下文字段。成功返回 C_OK，失败返回 C_ERR
 * 并通过 caller 回复错误信息。 */
int scriptPrepareForRun(scriptRunCtx *run_ctx, client *engine_client,
                        client *caller, const char *funcname,
                        uint64_t script_flags, int ro)
{
    serverAssert(!curr_run_ctx);  /* 确保当前没有脚本在运行 */
    int client_allow_oom = !!(caller->flags & CLIENT_ALLOW_OOM);

    /* 判断是否处于"过期"状态：作为副本但与主节点断开连接
     * 且不允许服务过期数据 */
    int running_stale = server.masterhost &&
            server.repl_state != REPL_STATE_CONNECTED &&
            server.repl_serve_stale_data == 0;
    /* obey_client：主节点发来的命令或 AOF 加载时需要服从 */
    int obey_client = mustObeyClient(caller);

    /* --- 非 EVAL 兼容模式（即 Function 模式）的校验 --- */
    if (!(script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE)) {
        /* 声明了 no-cluster 标志的脚本不允许在集群模式下执行 */
        if ((script_flags & SCRIPT_FLAG_NO_CLUSTER) && server.cluster_enabled) {
            addReplyError(caller, "Can not run script on cluster, 'no-cluster' flag is set.");
            return C_ERR;
        }

        /* 副本处于过期状态且脚本未声明 allow-stale，拒绝执行 */
        if (running_stale && !(script_flags & SCRIPT_FLAG_ALLOW_STALE)) {
            addReplyError(caller, "-MASTERDOWN Link with MASTER is down, "
                             "replica-serve-stale-data is set to 'no' "
                             "and 'allow-stale' flag is not set on the script.");
            return C_ERR;
        }

        /* 以下校验针对可能执行写操作的脚本 */
        if (!(script_flags & SCRIPT_FLAG_NO_WRITES)) {
            /* 脚本可能执行写操作，需要验证：
             * 1. 当前不是只读副本
             * 2. 没有磁盘错误
             * 3. 不是通过 *_ro 命令调用 */
            if (server.masterhost && server.repl_slave_ro && !obey_client) {
                addReplyError(caller, "-READONLY Can not run script with write flag on readonly replica");
                return C_ERR;
            }

            /* 如果无法持久化，拒绝写操作 */
            int deny_write_type = writeCommandsDeniedByDiskError();
            if (deny_write_type != DISK_ERROR_TYPE_NONE && !obey_client) {
                if (deny_write_type == DISK_ERROR_TYPE_RDB)
                    addReplyError(caller, "-MISCONF Redis is configured to save RDB snapshots, "
                                     "but it's currently unable to persist to disk. "
                                     "Writable scripts are blocked. Use 'no-writes' flag for read only scripts.");
                else
                    addReplyErrorFormat(caller, "-MISCONF Redis is configured to persist data to AOF, "
                                           "but it's currently unable to persist to disk. "
                                           "Writable scripts are blocked. Use 'no-writes' flag for read only scripts. "
                                           "AOF error: %s", strerror(server.aof_last_write_errno));
                return C_ERR;
            }

            /* 不允许通过 *_ro 命令执行带写标志的脚本 */
            if (ro) {
                addReplyError(caller, "Can not execute a script with write flag using *_ro command.");
                return C_ERR;
            }

            /* 如果配置了 min-slaves-to-write 且健康副本数量不足，拒绝写操作 */
            if (!checkGoodReplicasStatus()) {
                addReplyErrorObject(caller, shared.noreplicaserr);
                return C_ERR;
            }
        }

        /* 检查 OOM 状态。no-writes 标志隐含 allow-oom。
         * 该检查在写操作校验之后，因此错误回复中无需提及 no-writes */
        if (!client_allow_oom && server.pre_command_oom_state && server.maxmemory &&
            !(script_flags & (SCRIPT_FLAG_ALLOW_OOM|SCRIPT_FLAG_NO_WRITES)))
        {
            addReplyError(caller, "-OOM allow-oom flag is not set on the script, "
                                  "can not run it when used memory > 'maxmemory'");
            return C_ERR;
        }

    } else {
        /* EVAL 兼容模式（无 shebang 的 eval/evalsha）的特殊处理 */
        if (running_stale) {
            addReplyErrorObject(caller, shared.masterdownerr);
            return C_ERR;
        }
    }

    /* --- 校验通过，初始化运行上下文 --- */
    run_ctx->c = engine_client;            /* 脚本执行用的伪客户端 */
    run_ctx->original_client = caller;     /* 触发脚本的原始客户端 */
    run_ctx->funcname = funcname;          /* 脚本/函数名称 */
    run_ctx->slot = caller->slot;          /* 客户端当前访问的槽位 */

    client *script_client = run_ctx->c;
    client *curr_client = run_ctx->original_client;

    /* 在 Lua 客户端上下文中选择与原始客户端相同的数据库 */
    selectDb(script_client, curr_client->db->id);
    script_client->resp = 2; /* 默认使用 RESP2，脚本可以切换到 RESP3 */

    /* 如果原始客户端处于 MULTI 事务上下文，同样标记 Lua 客户端 */
    if (curr_client->flags & CLIENT_MULTI) {
        script_client->flags |= CLIENT_MULTI;
    }

    run_ctx->start_time = getMonotonicUs();  /* 记录脚本开始执行的时间 */

    run_ctx->flags = 0;
    /* 默认将脚本产生的写命令同时传播到 AOF 和副本 */
    run_ctx->repl_flags = PROPAGATE_AOF | PROPAGATE_REPL;

    /* fcall_ro 或声明了 no-writes 标志的函数，禁止执行写命令 */
    if (ro || (!(script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) && (script_flags & SCRIPT_FLAG_NO_WRITES))) {
        run_ctx->flags |= SCRIPT_READ_ONLY;
    }
    /* 允许在 OOM 状态下执行：客户端显式允许或脚本声明了 allow-oom。
     * 注意：无需检查 no-writes，因为只有写命令才受 OOM 限制。 */
    if (client_allow_oom || (!(script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) && (script_flags & SCRIPT_FLAG_ALLOW_OOM))) {
        run_ctx->flags |= SCRIPT_ALLOW_OOM;
    }

    /* EVAL 兼容模式或声明了 allow-cross-slot 的脚本允许跨槽访问 */
    if ((script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) || (script_flags & SCRIPT_FLAG_ALLOW_CROSS_SLOT)) {
        run_ctx->flags |= SCRIPT_ALLOW_CROSS_SLOT;
    }

    /* 设置全局运行上下文指针，用于后续中断/杀死脚本 */
    curr_run_ctx = run_ctx;

    return C_OK;
}

/* 脚本执行完毕后重置运行上下文 */
void scriptResetRun(scriptRunCtx *run_ctx) {
    serverAssert(curr_run_ctx);

    /* 脚本执行完毕，移除伪客户端的 MULTI 事务状态 */
    run_ctx->c->flags &= ~CLIENT_MULTI;

    if (scriptIsTimedout()) {
        exitScriptTimedoutMode(run_ctx);
        /* 恢复脚本超时时被保护的原始客户端 */
        unprotectClient(run_ctx->original_client);
    }

    run_ctx->slot = -1;   /* 清除槽位标记 */

    /* 阻止原始客户端的命令传播（脚本内的命令已自行传播） */
    preventCommandPropagation(run_ctx->original_client);

    /* 清除全局运行上下文，表示当前没有脚本在执行 */
    curr_run_ctx = NULL;
}

/* 判断是否有脚本正在运行 */
int scriptIsRunning(void) {
    return curr_run_ctx != NULL;
}

/* 获取当前正在运行的脚本/函数名称 */
const char* scriptCurrFunction(void) {
    serverAssert(scriptIsRunning());
    return curr_run_ctx->funcname;
}

/* 判断当前脚本是否为 EVAL 模式（而非 Function 模式） */
int scriptIsEval(void) {
    serverAssert(scriptIsRunning());
    return curr_run_ctx->flags & SCRIPT_EVAL_MODE;
}

/* 杀死当前正在运行的脚本
 * is_eval 为 true 表示通过 SCRIPT KILL 命令调用，
 * 为 false 表示通过 FUNCTION KILL 命令调用 */
void scriptKill(client *c, int is_eval) {
    if (!curr_run_ctx) {
        addReplyError(c, "-NOTBUSY No scripts in execution right now.");
        return;
    }
    /* 由主节点发起的脚本不可被杀死 */
    if (mustObeyClient(curr_run_ctx->original_client)) {
        addReplyError(c,
                "-UNKILLABLE The busy script was sent by a master instance in the context of replication and cannot be killed.");
        return;
    }
    /* 脚本已执行过写命令，杀死会导致数据不一致，不可杀死 */
    if (curr_run_ctx->flags & SCRIPT_WRITE_DIRTY) {
        addReplyError(c,
                "-UNKILLABLE Sorry the script already executed write "
                        "commands against the dataset. You can either wait the "
                        "script termination or kill the server in a hard way "
                        "using the SHUTDOWN NOSAVE command.");
        return;
    }
    /* 不允许用 SCRIPT KILL 杀死 Function */
    if (is_eval && !(curr_run_ctx->flags & SCRIPT_EVAL_MODE)) {
        addReplyErrorObject(c, shared.slowscripterr);
        return;
    }
    /* 不允许用 FUNCTION KILL 杀死 EVAL 脚本 */
    if (!is_eval && (curr_run_ctx->flags & SCRIPT_EVAL_MODE)) {
        addReplyErrorObject(c, shared.slowevalerr);
        return;
    }
    /* 设置 SCRIPT_KILLED 标志，脚本将在下一次 scriptInterrupt 调用时终止 */
    curr_run_ctx->flags |= SCRIPT_KILLED;
    addReply(c, shared.ok);
}

/* 校验脚本中调用的命令参数数量是否正确 */
static int scriptVerifyCommandArity(struct redisCommand *cmd, int argc, sds *err) {
    if (!cmd || ((cmd->arity > 0 && cmd->arity != argc) || (argc < -cmd->arity))) {
        if (cmd)
            *err = sdsnew("Wrong number of args calling Redis command from script");
        else
            *err = sdsnew("Unknown Redis command called from script");
        return C_ERR;
    }
    return C_OK;
}

/* 校验脚本中的命令是否符合 ACL 权限控制 */
static int scriptVerifyACL(client *c, sds *err) {
    /* 检查 ACL 权限 */
    int acl_errpos;
    int acl_retval = ACLCheckAllPerm(c, &acl_errpos);
    if (acl_retval != ACL_OK) {
        addACLLogEntry(c,acl_retval,ACL_LOG_CTX_LUA,acl_errpos,NULL,NULL);
        sds msg = getAclErrorMessage(acl_retval, c->user, c->cmd, c->argv[acl_errpos]->ptr, 0);
        *err = sdscatsds(sdsnew("ACL failure in script: "), msg);
        sdsfree(msg);
        return C_ERR;
    }
    return C_OK;
}

/* 校验脚本中的写命令是否允许执行 */
static int scriptVerifyWriteCommandAllow(scriptRunCtx *run_ctx, char **err) {

    /* 在只读命令或只读脚本中，立即拒绝写命令。
     * 注意：在脚本中，may-replicate 命令也被视为写命令。
     * 这也使得只读脚本可以在 CLIENT PAUSE WRITE 期间运行。 */
    if (run_ctx->flags & SCRIPT_READ_ONLY &&
        (run_ctx->c->cmd->flags & (CMD_WRITE|CMD_MAY_REPLICATE)))
    {
        *err = sdsnew("Write commands are not allowed from read-only scripts.");
        return C_ERR;
    }

    /* 以下检查基于服务器状态，仅对写命令有意义，
     * 非写命令直接返回 */
    if (!(run_ctx->c->cmd->flags & CMD_WRITE))
        return C_OK;

    /* 如果脚本已经修改过数据集，则不能因不确定的错误状态而拒绝执行，
     * 否则会导致部分写入的不一致状态 */
    if ((run_ctx->flags & SCRIPT_WRITE_DIRTY))
        return C_OK;

    /* 在只读副本上禁止写命令，或者在脚本中已调用过
     * 非确定性命令时也应拒绝写操作 */
    int deny_write_type = writeCommandsDeniedByDiskError();

    if (server.masterhost && server.repl_slave_ro &&
        !mustObeyClient(run_ctx->original_client))
    {
        *err = sdsdup(shared.roslaveerr->ptr);
        return C_ERR;
    }

    if (deny_write_type != DISK_ERROR_TYPE_NONE) {
        *err = writeCommandsGetDiskErrorMessage(deny_write_type);
        return C_ERR;
    }

    /* 如果配置了 min-slaves-to-write 且健康副本数量不足，拒绝写操作。
     * 注意：此检查仅对未声明标志的 EVAL 脚本可达，
     * Function 模式的类似检查在 scriptPrepareForRun 中完成 */
    if (!checkGoodReplicasStatus()) {
        *err = sdsdup(shared.noreplicaserr->ptr);
        return C_ERR;
    }

    return C_OK;
}

/* 校验脚本执行时是否发生 OOM（内存超限） */
static int scriptVerifyOOM(scriptRunCtx *run_ctx, char **err) {
    if (run_ctx->flags & SCRIPT_ALLOW_OOM) {
        /* 脚本声明允许 OOM，允许执行任何命令 */
        return C_OK;
    }

    /* 如果已达到 maxmemory 配置的内存上限，则不允许执行可能增加内存的命令。
     * 但仅在脚本尚未产生副作用（第一次写操作）时才拒绝，
     * 因为中途停止会导致部分写入的不一致。 */

    if (server.maxmemory &&                            /* maxmemory 已启用 */
        !mustObeyClient(run_ctx->original_client) &&   /* 副本或 AOF 加载不检查内存 */
        !(run_ctx->flags & SCRIPT_WRITE_DIRTY) &&      /* 脚本尚未产生副作用 */
        server.pre_command_oom_state &&                /* 脚本启动前已检测到 OOM */
        (run_ctx->c->cmd->flags & CMD_DENYOOM))        /* 当前命令受 OOM 限制 */
    {
        *err = sdsdup(shared.oomerr->ptr);
        return C_ERR;
    }

    return C_OK;
}

/* 校验脚本在集群模式下的键访问是否合法
 * 确保脚本不访问非本地键，不跨槽访问（除非允许） */
static int scriptVerifyClusterState(scriptRunCtx *run_ctx, client *c,
                                    client *original_c, sds *err)
{
    /* 非集群模式或来自主节点/AOF 加载的命令无需检查 */
    if (!server.cluster_enabled || mustObeyClient(original_c)) {
        return C_OK;
    }
    /* 在集群节点上，需要确保脚本不尝试访问非本地键，
     * 来自主节点的命令或 AOF 回放除外。 */
    int error_code;
    /* 将原始客户端的相关标志复制到脚本客户端 */
    c->flags &= ~(CLIENT_READONLY | CLIENT_ASKING);
    c->flags |= original_c->flags & (CLIENT_READONLY | CLIENT_ASKING);
    const uint64_t cmd_flags = getCommandFlags(c);
    int hashslot = -1;
    /* 查询命令涉及的键所属节点，如果不是本节点则报错 */
    if (getNodeByQuery(c, c->cmd, c->argv, c->argc, &hashslot, cmd_flags, &error_code) != getMyClusterNode()) {
        if (error_code == CLUSTER_REDIR_DOWN_RO_STATE) {
            *err = sdsnew(
                    "Script attempted to execute a write command while the "
                            "cluster is down and readonly");
        } else if (error_code == CLUSTER_REDIR_DOWN_STATE) {
            *err = sdsnew("Script attempted to execute a command while the "
                    "cluster is down");
        } else if (error_code == CLUSTER_REDIR_CROSS_SLOT) {
            *err = sdscatfmt(sdsempty(), 
                             "Command '%S' in script attempted to access keys that don't hash to the same slot",
                             c->cmd->fullname);
        } else if (error_code == CLUSTER_REDIR_UNSTABLE) {
            /* 请求涉及同一槽位的多个键，但该槽位当前处于
             * 迁移或导入过程中，状态不稳定 */
            *err = sdscatfmt(sdsempty(),
                             "Unable to execute command '%S' in script "
                             "because undeclared keys were accessed during rehashing of the slot",
                             c->cmd->fullname); 
        } else if (error_code == CLUSTER_REDIR_DOWN_UNBOUND) {
            *err = sdsnew("Script attempted to access a slot not served"); 
        } else {
            /* MOVED 或 ASK 重定向：键不属于本节点 */
            *err = sdsnew("Script attempted to access a non local key in a "
                    "cluster node");
        }
        return C_ERR;
    }

    /* 如果脚本预先声明了键，跨槽错误已经在此之前抛出。
     * 此处仅检查未预先声明的键是否跨槽访问。 */
    if (hashslot != -1 && !(run_ctx->flags & SCRIPT_ALLOW_CROSS_SLOT)) {
        if (run_ctx->slot == -1) {
            run_ctx->slot = hashslot;
        } else if (run_ctx->slot != hashslot) {
            *err = sdsnew("Script attempted to access keys that do not hash to "
                    "the same slot");
            return C_ERR;
        }
    }

    /* 记录当前访问的槽位，用于后续跨槽检测 */
    c->slot = hashslot;
    original_c->slot = hashslot;

    return C_OK;
}

/* 设置脚本客户端的 RESP 协议版本（仅支持 2 或 3） */
int scriptSetResp(scriptRunCtx *run_ctx, int resp) {
    if (resp != 2 && resp != 3) {
        return C_ERR;
    }

    run_ctx->c->resp = resp;
    return C_OK;
}

/* 设置脚本的复制传播标志
 * 可选值为 PROPAGATE_AOF 和/或 PROPAGATE_REPL 的组合 */
int scriptSetRepl(scriptRunCtx *run_ctx, int repl) {
    if ((repl & ~(PROPAGATE_AOF | PROPAGATE_REPL)) != 0) {
        return C_ERR;
    }
    run_ctx->repl_flags = repl;
    return C_OK;
}

/* 校验在副本过期（stale）状态下是否允许执行该命令 */
static int scriptVerifyAllowStale(client *c, sds *err) {
    if (!server.masterhost) {
        /* 非副本节点，过期状态不相关 */
        return C_OK;
    }

    if (server.repl_state == REPL_STATE_CONNECTED) {
        /* 已连接到主节点，不存在过期问题 */
        return C_OK;
    }

    if (server.repl_serve_stale_data == 1) {
        /* 虽与主节点断开，但配置允许服务过期数据 */
        return C_OK;
    }

    if (c->cmd->flags & CMD_STALE) {
        /* 该命令本身允许在过期状态下执行 */
        return C_OK;
    }

    /* 副本处于过期状态，不允许执行此命令 */
    *err = sdsnew("Can not execute the command on a stale replica");
    return C_ERR;
}

/* 在脚本中调用一条 Redis 命令
 * 命令的回复写入 run_ctx 的伪客户端，由脚本引擎读取和解析。
 * err 输出参数仅在发生错误时被设置，描述错误原因。
 * 如果发生错误，不会向客户端写入回复。 */
void scriptCall(scriptRunCtx *run_ctx, sds *err) {
    client *c = run_ctx->c;

    /* 设置伪客户端的用户权限，与原始客户端一致 */
    c->user = run_ctx->original_client->user;

    /* 调用模块钩子，允许模块过滤/修改命令 */
    moduleCallCommandFilters(c);

    /* 查找命令并校验 */
    struct redisCommand *cmd = lookupCommand(c->argv, c->argc);
    c->cmd = c->lastcmd = c->realcmd = cmd;
    if (scriptVerifyCommandArity(cmd, c->argc, err) != C_OK) {
        goto error;
    }

    /* 某些命令不允许在脚本中执行 */
    if (!server.script_disable_deny_script && (cmd->flags & CMD_NOSCRIPT)) {
        *err = sdsnew("This Redis command is not allowed from script");
        goto error;
    }

    if (scriptVerifyAllowStale(c, err) != C_OK) {
        goto error;
    }

    if (scriptVerifyACL(c, err) != C_OK) {
        goto error;
    }

    if (scriptVerifyWriteCommandAllow(run_ctx, err) != C_OK) {
        goto error;
    }

    if (scriptVerifyOOM(run_ctx, err) != C_OK) {
        goto error;
    }

    /* 如果是写命令，标记脚本已产生数据变更 */
    if (cmd->flags & CMD_WRITE) {
        run_ctx->flags |= SCRIPT_WRITE_DIRTY;
    }

    /* 校验集群状态下的键访问合法性 */
    if (scriptVerifyClusterState(run_ctx, c, run_ctx->original_client, err) != C_OK) {
        goto error;
    }

    /* 根据脚本的复制标志设置命令调用标志 */
    int call_flags = CMD_CALL_NONE;
    if (run_ctx->repl_flags & PROPAGATE_AOF) {
        call_flags |= CMD_CALL_PROPAGATE_AOF;
    }
    if (run_ctx->repl_flags & PROPAGATE_REPL) {
        call_flags |= CMD_CALL_PROPAGATE_REPL;
    }
    /* 执行命令 */
    call(c, call_flags);
    /* 脚本中的命令不应阻塞客户端 */
    serverAssert((c->flags & CLIENT_BLOCKED) == 0);
    return;

error:
    /* 命令校验失败，记录错误回复并更新错误统计 */
    afterErrorReply(c, *err, sdslen(*err), 0);
    incrCommandStatsOnError(cmd, ERROR_COMMAND_REJECTED);
}

/* 获取当前脚本已运行的时长（毫秒） */
long long scriptRunDuration(void) {
    serverAssert(scriptIsRunning());
    return elapsedMs(curr_run_ctx->start_time);
}
