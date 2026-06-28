/*
 * Copyright (c) 2021-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

/*
 * function_lua.c 提供 Lua 引擎功能。
 * 包括注册引擎和实现引擎回调：
 * * 从代码文本（blob）创建函数
 * * 调用函数
 * * 释放函数内存
 * * 获取内存使用量
 *
 * 使用 script_lua.c 来运行 Lua 代码。
 */

#include "functions.h"
#include "script_lua.h"
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#if defined(USE_JEMALLOC)
#include <lstate.h>
#endif

#define LUA_ENGINE_NAME "LUA"                       /* Lua 引擎名称 */
#define REGISTRY_ENGINE_CTX_NAME "__ENGINE_CTX__"   /* Lua 注册表中引擎上下文的键名 */
#define REGISTRY_ERROR_HANDLER_NAME "__ERROR_HANDLER__" /* Lua 注册表中错误处理器的键名 */
#define REGISTRY_LOAD_CTX_NAME "__LIBRARY_CTX__"    /* Lua 注册表中加载上下文的键名 */
#define LIBRARY_API_NAME "__LIBRARY_API__"          /* 函数库 API 表名 */
#define GLOBALS_API_NAME "__GLOBALS_API__"          /* 全局 API 表名 */

static int gc_count = 0; /* GC 请求计数器，每次 GC 执行后重置 */

/* Lua 引擎上下文，包含 Lua 状态机 */
typedef struct luaEngineCtx {
    lua_State *lua;
} luaEngineCtx;

/* Lua 函数上下文，通过引用从注册表获取函数对象 */
typedef struct luaFunctionCtx {
    /* 特殊 ID，用于从 Lua 注册表中获取 Lua 函数对象 */
    int lua_function_ref;
} luaFunctionCtx;

/* FUNCTION LOAD 执行期间的上下文信息 */
typedef struct loadCtx {
    functionLibInfo *li;
    monotime start_time;
    size_t timeout;
} loadCtx;

/* 注册函数时的参数集合 */
typedef struct registerFunctionArgs {
    sds name;
    sds desc;
    luaFunctionCtx *lua_f_ctx;
    uint64_t f_flags;
} registerFunctionArgs;

/*
 * Lua 钩子函数，用于在 FUNCTION LOAD 执行超时时取消执行。
 * 当执行超时（500ms）时取消执行。
 * 此执行应该很快，只注册函数，
 * 因此 500ms 应该绰绰有余。
 */
static void luaEngineLoadHook(lua_State *lua, lua_Debug *ar) {
    UNUSED(ar);
    loadCtx *load_ctx = luaGetFromRegistry(lua, REGISTRY_LOAD_CTX_NAME);
    serverAssert(load_ctx); /* 仅在脚本调用上下文中有效 */
    uint64_t duration = elapsedMs(load_ctx->start_time);
    if (load_ctx->timeout > 0 && duration > load_ctx->timeout) {
        lua_sethook(lua, luaEngineLoadHook, LUA_MASKLINE, 0);

        luaPushError(lua,"FUNCTION LOAD timeout");
        luaError(lua);
    }
}

/*
 * 编译给定代码文本（blob）并保存到 Lua 注册表中。
 * 返回一个函数上下文，包含可用于后续从注册表获取函数的 Lua 引用。
 *
 * 编译出错时返回 NULL，并将错误信息设置到 err 变量中。
 */
static int luaEngineCreate(void *engine_ctx, functionLibInfo *li, sds blob, size_t timeout, sds *err) {
    int ret = C_ERR;
    luaEngineCtx *lua_engine_ctx = engine_ctx;
    lua_State *lua = lua_engine_ctx->lua;

    /* 设置加载库的全局变量 */
    lua_getmetatable(lua, LUA_GLOBALSINDEX);
    lua_enablereadonlytable(lua, -1, 0); /* 禁用全局保护 */
    lua_getfield(lua, LUA_REGISTRYINDEX, LIBRARY_API_NAME);
    lua_setfield(lua, -2, "__index");
    lua_enablereadonlytable(lua, LUA_GLOBALSINDEX, 1); /* 启用全局表保护 */
    lua_pop(lua, 1); /* 弹出元表 */

    /* 编译代码 */
    if (luaL_loadbuffer(lua, blob, sdslen(blob), "@user_function")) {
        *err = sdscatprintf(sdsempty(), "Error compiling function: %s", lua_tostring(lua, -1));
        lua_pop(lua, 1); /* 弹出错误信息 */
        goto done;
    }
    serverAssert(lua_isfunction(lua, -1));

    loadCtx load_ctx = {
        .li = li,
        .start_time = getMonotonicUs(),
        .timeout = timeout,
    };
    luaSaveOnRegistry(lua, REGISTRY_LOAD_CTX_NAME, &load_ctx);

    lua_sethook(lua,luaEngineLoadHook,LUA_MASKCOUNT,100000);
    /* 运行编译后的代码，使其注册函数 */
    if (lua_pcall(lua,0,0,0)) {
        errorInfo err_info = {0};
        luaExtractErrorInformation(lua, &err_info);
        *err = sdscatprintf(sdsempty(), "Error registering functions: %s", err_info.msg);
        lua_pop(lua, 1); /* 弹出错误信息 */
        luaErrorInformationDiscard(&err_info);
        goto done;
    }

    ret = C_OK;

done:
    /* 恢复原始全局变量 */
    lua_getmetatable(lua, LUA_GLOBALSINDEX);
    lua_enablereadonlytable(lua, -1, 0); /* 禁用全局保护 */
    lua_getfield(lua, LUA_REGISTRYINDEX, GLOBALS_API_NAME);
    lua_setfield(lua, -2, "__index");
    lua_enablereadonlytable(lua, LUA_GLOBALSINDEX, 1); /* 启用全局表保护 */
    lua_pop(lua, 1); /* 弹出元表 */

    lua_sethook(lua,NULL,0,0); /* 禁用钩子 */
    luaSaveOnRegistry(lua, REGISTRY_LOAD_CTX_NAME, NULL);
    luaGC(lua, &gc_count);
    return ret;
}

/*
 * 调用已编译的 Lua 函数，传入给定的 keys 和 args 参数。
 */
static void luaEngineCall(scriptRunCtx *run_ctx,
                          void *engine_ctx,
                          void *compiled_function,
                          robj **keys,
                          size_t nkeys,
                          robj **args,
                          size_t nargs)
{
    luaEngineCtx *lua_engine_ctx = engine_ctx;
    lua_State *lua = lua_engine_ctx->lua;
    luaFunctionCtx *f_ctx = compiled_function;

    /* 压入错误处理器 */
    lua_pushstring(lua, REGISTRY_ERROR_HANDLER_NAME);
    lua_gettable(lua, LUA_REGISTRYINDEX);

    lua_rawgeti(lua, LUA_REGISTRYINDEX, f_ctx->lua_function_ref);

    serverAssert(lua_isfunction(lua, -1));

    luaCallFunction(run_ctx, lua, keys, nkeys, args, nargs, 0);
    lua_pop(lua, 1); /* 弹出错误处理器 */
    luaGC(lua, &gc_count);
}

/* 获取 Lua 引擎已使用的内存 */
static size_t luaEngineGetUsedMemoy(void *engine_ctx) {
    luaEngineCtx *lua_engine_ctx = engine_ctx;
    return luaMemory(lua_engine_ctx->lua);
}

/* 获取单个函数的内存开销 */
static size_t luaEngineFunctionMemoryOverhead(void *compiled_function) {
    return zmalloc_size(compiled_function);
}

/* 获取 Lua 引擎的内存开销 */
static size_t luaEngineMemoryOverhead(void *engine_ctx) {
    luaEngineCtx *lua_engine_ctx = engine_ctx;
    return zmalloc_size(lua_engine_ctx);
}

/* 释放已编译的 Lua 函数内存 */
static void luaEngineFreeFunction(void *engine_ctx, void *compiled_function) {
    luaEngineCtx *lua_engine_ctx = engine_ctx;
    lua_State *lua = lua_engine_ctx->lua;
    luaFunctionCtx *f_ctx = compiled_function;
    lua_unref(lua, f_ctx->lua_function_ref);
    zfree(f_ctx);
}

/* 释放 Lua 引擎上下文，关闭 Lua 状态机 */
static void luaEngineFreeCtx(void *engine_ctx) {
    luaEngineCtx *lua_engine_ctx = engine_ctx;
#if defined(USE_JEMALLOC)
    /* 当 Lua 关闭时，销毁之前使用的私有 tcache。 */
    void *ud = (global_State*)G(lua_engine_ctx->lua)->ud;
    unsigned int lua_tcache = (unsigned int)(uintptr_t)ud;
#endif

    lua_gc(lua_engine_ctx->lua, LUA_GCCOLLECT, 0);
    lua_close(lua_engine_ctx->lua);
    zfree(lua_engine_ctx);

#if defined(USE_JEMALLOC)
    je_mallctl("tcache.destroy", NULL, NULL, (void *)&lua_tcache, sizeof(unsigned int));
#endif
}

/* 初始化注册函数参数结构体 */
static void luaRegisterFunctionArgsInitialize(registerFunctionArgs *register_f_args,
    sds name,
    sds desc,
    luaFunctionCtx *lua_f_ctx,
    uint64_t flags)
{
    *register_f_args = (registerFunctionArgs){
        .name = name,
        .desc = desc,
        .lua_f_ctx = lua_f_ctx,
        .f_flags = flags,
    };
}

/* 清理注册函数参数，释放相关资源 */
static void luaRegisterFunctionArgsDispose(lua_State *lua, registerFunctionArgs *register_f_args) {
    sdsfree(register_f_args->name);
    if (register_f_args->desc) sdsfree(register_f_args->desc);
    lua_unref(lua, register_f_args->lua_f_ctx->lua_function_ref);
    zfree(register_f_args->lua_f_ctx);
}

/*
 * 从 Lua 栈顶读取函数标志。
 * 成功时返回 C_OK 并将标志设置到 flags 输出参数中。
 * 遇到未知标志时返回 C_ERR。
 */
static int luaRegisterFunctionReadFlags(lua_State *lua, uint64_t *flags) {
    int j = 1;
    int ret = C_ERR;
    int f_flags = 0;
    while(1) {
        lua_pushnumber(lua,j++);
        lua_gettable(lua,-2);
        int t = lua_type(lua,-1);
        if (t == LUA_TNIL) {
            lua_pop(lua,1);
            break;
        }
        if (!lua_isstring(lua, -1)) {
            lua_pop(lua,1);
            goto done;
        }

        const char *flag_str = lua_tostring(lua, -1);
        int found = 0;
        for (scriptFlag *flag = scripts_flags_def; flag->str ; ++flag) {
            if (!strcasecmp(flag->str, flag_str)) {
                f_flags |= flag->flag;
                found = 1;
                break;
            }
        }
        /* 弹出值以继续迭代 */
        lua_pop(lua,1);
        if (!found) {
            /* 未找到匹配的标志 */
            goto done;
        }
    }

    *flags = f_flags;
    ret = C_OK;

done:
    return ret;
}

/* 从 Lua 表中读取命名参数形式的注册函数参数 */
static int luaRegisterFunctionReadNamedArgs(lua_State *lua, registerFunctionArgs *register_f_args) {
    char *err = NULL;
    sds name = NULL;
    sds desc = NULL;
    luaFunctionCtx *lua_f_ctx = NULL;
    uint64_t flags = 0;
    if (!lua_istable(lua, 1)) {
        err = "calling redis.register_function with a single argument is only applicable to Lua table (representing named arguments).";
        goto error;
    }

    /* 遍历所有命名参数 */
    lua_pushnil(lua);
    while (lua_next(lua, -2)) {
        /* 当前栈状态：table, key, value */
        if (!lua_isstring(lua, -2)) {
            err = "named argument key given to redis.register_function is not a string";
            goto error;
        }
        const char *key = lua_tostring(lua, -2);
        if (!strcasecmp(key, "function_name")) {
            if (!(name = luaGetStringSds(lua, -1))) {
                err = "function_name argument given to redis.register_function must be a string";
                goto error;
            }
        } else if (!strcasecmp(key, "description")) {
            if (!(desc = luaGetStringSds(lua, -1))) {
                err = "description argument given to redis.register_function must be a string";
                goto error;
            }
        } else if (!strcasecmp(key, "callback")) {
            if (!lua_isfunction(lua, -1)) {
                err = "callback argument given to redis.register_function must be a function";
                goto error;
            }
            int lua_function_ref = luaL_ref(lua, LUA_REGISTRYINDEX);

            lua_f_ctx = zmalloc(sizeof(*lua_f_ctx));
            lua_f_ctx->lua_function_ref = lua_function_ref;
            continue; /* 值已被弹出，无需再次弹出 */
        } else if (!strcasecmp(key, "flags")) {
            if (!lua_istable(lua, -1)) {
                err = "flags argument to redis.register_function must be a table representing function flags";
                goto error;
            }
            if (luaRegisterFunctionReadFlags(lua, &flags) != C_OK) {
                err = "unknown flag given";
                goto error;
            }
        } else {
            /* 传入了未知参数，抛出错误 */
            err = "unknown argument given to redis.register_function";
            goto error;
        }
        lua_pop(lua, 1); /* 弹出值以继续迭代 */
    }

    if (!name) {
        err = "redis.register_function must get a function name argument";
        goto error;
    }

    if (!lua_f_ctx) {
        err = "redis.register_function must get a callback argument";
        goto error;
    }

    luaRegisterFunctionArgsInitialize(register_f_args, name, desc, lua_f_ctx, flags);

    return C_OK;

error:
    if (name) sdsfree(name);
    if (desc) sdsfree(desc);
    if (lua_f_ctx) {
        lua_unref(lua, lua_f_ctx->lua_function_ref);
        zfree(lua_f_ctx);
    }
    luaPushError(lua, err);
    return C_ERR;
}

/* 读取位置参数形式的注册函数参数 */
static int luaRegisterFunctionReadPositionalArgs(lua_State *lua, registerFunctionArgs *register_f_args) {
    char *err = NULL;
    sds name = NULL;
    sds desc = NULL;
    luaFunctionCtx *lua_f_ctx = NULL;
    if (!(name = luaGetStringSds(lua, 1))) {
        err = "first argument to redis.register_function must be a string";
        goto error;
    }

    if (!lua_isfunction(lua, 2)) {
        err = "second argument to redis.register_function must be a function";
        goto error;
    }

    int lua_function_ref = luaL_ref(lua, LUA_REGISTRYINDEX);

    lua_f_ctx = zmalloc(sizeof(*lua_f_ctx));
    lua_f_ctx->lua_function_ref = lua_function_ref;

    luaRegisterFunctionArgsInitialize(register_f_args, name, NULL, lua_f_ctx, 0);

    return C_OK;

error:
    if (name) sdsfree(name);
    if (desc) sdsfree(desc);
    luaPushError(lua, err);
    return C_ERR;
}

/* 读取 redis.register_function 的参数（自动判断命名/位置参数） */
static int luaRegisterFunctionReadArgs(lua_State *lua, registerFunctionArgs *register_f_args) {
    int argc = lua_gettop(lua);
    if (argc < 1 || argc > 2) {
        luaPushError(lua, "wrong number of arguments to redis.register_function");
        return C_ERR;
    }

    if (argc == 1) {
        return luaRegisterFunctionReadNamedArgs(lua, register_f_args);
    } else {
        return luaRegisterFunctionReadPositionalArgs(lua, register_f_args);
    }
}

/* redis.register_function 的 C 实现，将 Lua 函数注册到函数库中 */
static int luaRegisterFunction(lua_State *lua) {
    registerFunctionArgs register_f_args = {0};

    loadCtx *load_ctx = luaGetFromRegistry(lua, REGISTRY_LOAD_CTX_NAME);
    if (!load_ctx) {
        luaPushError(lua, "redis.register_function can only be called on FUNCTION LOAD command");
        return luaError(lua);
    }

    if (luaRegisterFunctionReadArgs(lua, &register_f_args) != C_OK) {
        return luaError(lua);
    }

    sds err = NULL;
    if (functionLibCreateFunction(register_f_args.name, register_f_args.lua_f_ctx, load_ctx->li, register_f_args.desc, register_f_args.f_flags, &err) != C_OK) {
        luaRegisterFunctionArgsDispose(lua, &register_f_args);
        luaPushError(lua, err);
        sdsfree(err);
        return luaError(lua);
    }

    return 0;
}

/* 初始化 Lua 引擎，应在启动时调用一次 */
int luaEngineInitEngine(void) {
    luaEngineCtx *lua_engine_ctx = zmalloc(sizeof(*lua_engine_ctx));
    lua_engine_ctx->lua = createLuaState();

    luaRegisterRedisAPI(lua_engine_ctx->lua);

    /* 注册函数库命令表和字段，并保存到注册表 */
    lua_newtable(lua_engine_ctx->lua); /* 创建函数库全局表 */
    lua_newtable(lua_engine_ctx->lua); /* 创建函数库 redis 表 */

    lua_pushstring(lua_engine_ctx->lua, "register_function");
    lua_pushcfunction(lua_engine_ctx->lua, luaRegisterFunction);
    lua_settable(lua_engine_ctx->lua, -3);

    luaRegisterLogFunction(lua_engine_ctx->lua);
    luaRegisterVersion(lua_engine_ctx->lua);

    luaSetErrorMetatable(lua_engine_ctx->lua);
    lua_setfield(lua_engine_ctx->lua, -2, REDIS_API_NAME);

    luaSetErrorMetatable(lua_engine_ctx->lua);
    luaSetTableProtectionRecursively(lua_engine_ctx->lua); /* 保护函数库全局表 */
    lua_setfield(lua_engine_ctx->lua, LUA_REGISTRYINDEX, LIBRARY_API_NAME);

    /* 将错误处理器保存到注册表 */
    lua_pushstring(lua_engine_ctx->lua, REGISTRY_ERROR_HANDLER_NAME);
    char *errh_func =       "local dbg = debug\n"
                            "debug = nil\n"
                            "local error_handler = function (err)\n"
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
                            "end\n"
                            "return error_handler";
    luaL_loadbuffer(lua_engine_ctx->lua, errh_func, strlen(errh_func), "@err_handler_def");
    lua_pcall(lua_engine_ctx->lua,0,1,0);
    lua_settable(lua_engine_ctx->lua, LUA_REGISTRYINDEX);

    lua_pushvalue(lua_engine_ctx->lua, LUA_GLOBALSINDEX);
    luaSetErrorMetatable(lua_engine_ctx->lua);
    luaSetTableProtectionRecursively(lua_engine_ctx->lua); /* 保护全局表 */
    lua_pop(lua_engine_ctx->lua, 1);

    /* 将默认全局变量保存到注册表 */
    lua_pushvalue(lua_engine_ctx->lua, LUA_GLOBALSINDEX);
    lua_setfield(lua_engine_ctx->lua, LUA_REGISTRYINDEX, GLOBALS_API_NAME);

    /* 将 engine_ctx 保存到注册表，以便从 Lua 解释器中获取 */
    luaSaveOnRegistry(lua_engine_ctx->lua, REGISTRY_ENGINE_CTX_NAME, lua_engine_ctx);

    /* 创建新的空表作为新的全局变量表，通过元表控制真正的全局变量 */
    lua_newtable(lua_engine_ctx->lua); /* 新的全局表 */
    lua_newtable(lua_engine_ctx->lua); /* 新全局表的元表 */
    lua_pushvalue(lua_engine_ctx->lua, LUA_GLOBALSINDEX);
    lua_setfield(lua_engine_ctx->lua, -2, "__index");
    lua_enablereadonlytable(lua_engine_ctx->lua, -1, 1); /* 保护元表 */
    lua_setmetatable(lua_engine_ctx->lua, -2);
    lua_enablereadonlytable(lua_engine_ctx->lua, -1, 1); /* 保护新全局表 */
    lua_replace(lua_engine_ctx->lua, LUA_GLOBALSINDEX); /* 将新全局表设置为全局变量 */


    engine *lua_engine = zmalloc(sizeof(*lua_engine));
    *lua_engine = (engine) {
        .engine_ctx = lua_engine_ctx,
        .create = luaEngineCreate,
        .call = luaEngineCall,
        .get_used_memory = luaEngineGetUsedMemoy,
        .get_function_memory_overhead = luaEngineFunctionMemoryOverhead,
        .get_engine_memory_overhead = luaEngineMemoryOverhead,
        .free_function = luaEngineFreeFunction,
        .free_ctx = luaEngineFreeCtx,
    };
    return functionsRegisterEngine(LUA_ENGINE_NAME, lua_engine);
}
