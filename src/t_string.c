/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include <math.h> /* isnan(), isinf() 用于浮点数检查 */

/* 前向声明 */
int getGenericCommand(client *c);

/*-----------------------------------------------------------------------------
 * 字符串命令
 *----------------------------------------------------------------------------*/

/* 检查字符串长度是否超出允许的最大值。
 * 用于 SETRANGE/APPEND 等可能改变字符串长度的命令。
 * 若客户端可绕过限制（mustObeyClient），直接通过；否则校验 size+append
 * 是否超过 server.proto_max_bulk_len，并防止整数溢出。
 *
 * 参数：
 *   c      - 当前客户端
 *   size   - 字符串当前长度
 *   append - 待追加的长度
 * 返回值：C_OK 表示通过校验，C_ERR 表示超出限制（已向客户端返回错误）。 */
static int checkStringLength(client *c, long long size, long long append) {
    if (mustObeyClient(c))
        return C_OK;
    /* 将 size 转为 uint64_t 仅是为了防止溢出时的未定义行为 */
    long long total = (uint64_t)size + append;
    /* 测试配置的 max-bulk-len（限制最大字符串对象大小），同时检测溢出。 */
    if (total > server.proto_max_bulk_len || total < size || total < append) {
        addReplyError(c,"string exceeds maximum allowed size (proto-max-bulk-len)");
        return C_ERR;
    }
    return C_OK;
}

/* setGenericCommand() 函数以不同的选项和变体实现 SET 操作。
 * 该函数被调用以实现以下命令：SET、SETEX、PSETEX、SETNX、GETSET。
 *
 * 'flags' 改变命令的行为（NX、XX 或 GET，见下文）。
 *
 * 'expire' 表示用户传入的过期时间（Redis 对象形式），
 *          将根据指定的 'unit' 进行解释。
 *
 * 'ok_reply' 和 'abort_reply' 是当操作执行成功，
 * 或因 NX/XX 标志未执行时，函数将回复给客户端的内容。
 *
 * 若 ok_reply 为 NULL，则使用 "+OK"。
 * 若 abort_reply 为 NULL，则使用 "$-1"。 */

/* SET 选项标志位定义 */
#define OBJ_NO_FLAGS 0
#define OBJ_SET_NX (1<<0)          /* 仅当 key 不存在时设置 */
#define OBJ_SET_XX (1<<1)          /* 仅当 key 存在时设置 */
#define OBJ_EX (1<<2)              /* 给定的是秒级过期时间 */
#define OBJ_PX (1<<3)              /* 给定的是毫秒级过期时间 */
#define OBJ_KEEPTTL (1<<4)         /* 设置并保留已有 TTL */
#define OBJ_SET_GET (1<<5)         /* 在设置前获取旧值 */
#define OBJ_EXAT (1<<6)            /* 给定的是秒级绝对时间戳 */
#define OBJ_PXAT (1<<7)            /* 给定的是毫秒级绝对时间戳 */
#define OBJ_PERSIST (1<<8)         /* 需要移除 TTL */

/* 前向声明 */
static int getExpireMillisecondsOrReply(client *c, robj *expire, int flags, int unit, long long *milliseconds);

/* SET 命令的通用实现。
 * 根据 flags 支持 NX/XX/EX/PX/EXAT/PXAT/KEEPTTL/GET 等选项，
 * 同时被 SET/SETEX/PSETEX/SETNX/GETSET 等命令复用。
 * 时间复杂度：O(1) */
void setGenericCommand(client *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {
    long long milliseconds = 0; /* 初始化以避免无害的编译告警 */
    int found = 0;
    int setkey_flags = 0;

    // 解析过期参数为绝对毫秒时间戳
    if (expire && getExpireMillisecondsOrReply(c, expire, flags, unit, &milliseconds) != C_OK) {
        return;
    }

    // 如果带有 GET 选项，先返回旧值
    if (flags & OBJ_SET_GET) {
        if (getGenericCommand(c) == C_ERR) return;
    }

    // 查找 key 当前是否存在
    found = (lookupKeyWrite(c->db,key) != NULL);

    // NX 要求 key 不存在，XX 要求 key 存在；条件不符则中止
    if ((flags & OBJ_SET_NX && found) ||
        (flags & OBJ_SET_XX && !found))
    {
        if (!(flags & OBJ_SET_GET)) {
            addReply(c, abort_reply ? abort_reply : shared.null[c->resp]);
        }
        return;
    }

    /* 当 expire 不为 NULL 时，避免删除 TTL，以便后续更新 TTL
     * 而非删除后再重新创建。 */
    setkey_flags |= ((flags & OBJ_KEEPTTL) || expire) ? SETKEY_KEEPTTL : 0;
    setkey_flags |= found ? SETKEY_ALREADY_EXIST : SETKEY_DOESNT_EXIST;

    // 写入数据库并触发相关事件
    setKey(c,c->db,key,val,setkey_flags);
    server.dirty++;
    notifyKeyspaceEvent(NOTIFY_STRING,"set",key,c->db->id);

    // 设置过期时间（如果有）
    if (expire) {
        setExpire(c,c->db,key,milliseconds);
        /* 若有 EX/PX/EXAT 标志，则以 SET Key Value PXAT <毫秒时间戳> 形式传播。 */
        if (!(flags & OBJ_PXAT)) {
            robj *milliseconds_obj = createStringObjectFromLongLong(milliseconds);
            rewriteClientCommandVector(c, 5, shared.set, key, val, shared.pxat, milliseconds_obj);
            decrRefCount(milliseconds_obj);
        }
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",key,c->db->id);
    }

    // 返回成功响应（若非 GET 模式）
    if (!(flags & OBJ_SET_GET)) {
        addReply(c, ok_reply ? ok_reply : shared.ok);
    }

    /* 传播命令时去掉 GET 参数（若带 expire 则无需此操作，
     * 因为那时已经完整重写了命令 argv） */
    if ((flags & OBJ_SET_GET) && !expire) {
        int argc = 0;
        int j;
        robj **argv = zmalloc((c->argc-1)*sizeof(robj*));
        for (j=0; j < c->argc; j++) {
            char *a = c->argv[j]->ptr;
            /* 跳过 GET 参数（它可能出现多次）。 */
            if (j >= 3 &&
                (a[0] == 'g' || a[0] == 'G') &&
                (a[1] == 'e' || a[1] == 'E') &&
                (a[2] == 't' || a[2] == 'T') && a[3] == '\0')
                continue;
            argv[argc++] = c->argv[j];
            incrRefCount(c->argv[j]);
        }
        replaceClientCommandVector(c, argc, argv);
    }
}

/*
 * 提取给定 GET/SET 命令的 `expire` 参数，并转换为以毫秒为单位的绝对时间戳。
 *
 * "client"      - 发送 `expire` 参数的客户端。
 * "expire"      - 待提取的 `expire` 参数对象。
 * "flags"       - 命令的行为标志（如 PX 或 EX）。
 * "unit"        - `expire` 参数的原始单位（如 UNIT_SECONDS）。
 * "milliseconds" - 输出参数。
 *
 * 若返回 C_OK，则 "milliseconds" 输出参数将被设置为最终的绝对时间戳。
 * 若返回 C_ERR，则已向给定客户端返回了错误响应。
 */
static int getExpireMillisecondsOrReply(client *c, robj *expire, int flags, int unit, long long *milliseconds) {
    int ret = getLongLongFromObjectOrReply(c, expire, milliseconds, NULL);
    if (ret != C_OK) {
        return ret;
    }

    if (*milliseconds <= 0 || (unit == UNIT_SECONDS && *milliseconds > LLONG_MAX / 1000)) {
        /* 给定为负数，或者乘法将发生溢出。 */
        addReplyErrorExpireTime(c);
        return C_ERR;
    }

    // 秒转换为毫秒
    if (unit == UNIT_SECONDS) *milliseconds *= 1000;

    // PX/EX 表示相对时间，需加上当前时间得到绝对时间戳
    if ((flags & OBJ_PX) || (flags & OBJ_EX)) {
        *milliseconds += commandTimeSnapshot();
    }

    if (*milliseconds <= 0) {
        /* 检测到溢出。 */
        addReplyErrorExpireTime(c);
        return C_ERR;
    }

    return C_OK;
}

/* 命令类型：GET 系列（GETEX/GETDEL）或 SET 系列（SET/SETEX/PSETEX/SETNX/GETSET） */
#define COMMAND_GET 0
#define COMMAND_SET 1
/*
 * parseExtendedStringArgumentsOrReply() 函数对 SET 和 GET 命令中使用的扩展
 * 字符串参数进行通用校验。
 *
 * GET 专属命令 - PERSIST
 * SET 专属命令 - XX/NX/GET
 * 通用命令    - EX/EXAT/PX/PXAT/KEEPTTL
 *
 * 函数接受 client 指针、flags 指针、unit 指针、expire 对象的指针的指针
 * （如需确定）以及 command_type（COMMAND_GET 或 COMMAND_SET）。
 *
 * 若存在语法错误则返回 C_ERR，否则返回 C_OK。
 *
 * 解析参数后会更新输入的 flags。若存在 EX/EXAT/PX/PXAT 参数，
 * 则更新 unit 和 expire。设置 PX/PXAT 时 unit 将更新为毫秒。
 */
int parseExtendedStringArgumentsOrReply(client *c, int *flags, int *unit, robj **expire, int command_type) {

    // 从第 3（SET）或第 2（GET）个参数开始解析
    int j = command_type == COMMAND_GET ? 2 : 3;
    for (; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        // 取下一个参数（用于带值的选项如 EX/PX）
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];

        // NX：仅当 key 不存在时设置
        if ((opt[0] == 'n' || opt[0] == 'N') &&
            (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
            !(*flags & OBJ_SET_XX) && (command_type == COMMAND_SET))
        {
            *flags |= OBJ_SET_NX;
        // XX：仅当 key 存在时设置
        } else if ((opt[0] == 'x' || opt[0] == 'X') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_SET_NX) && (command_type == COMMAND_SET))
        {
            *flags |= OBJ_SET_XX;
        // GET：设置前返回旧值
        } else if ((opt[0] == 'g' || opt[0] == 'G') &&
                   (opt[1] == 'e' || opt[1] == 'E') &&
                   (opt[2] == 't' || opt[2] == 'T') && opt[3] == '\0' &&
                   (command_type == COMMAND_SET))
        {
            *flags |= OBJ_SET_GET;
        // KEEPTTL：保留已有 TTL
        } else if (!strcasecmp(opt, "KEEPTTL") && !(*flags & OBJ_PERSIST) &&
            !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
            !(*flags & OBJ_PX) && !(*flags & OBJ_PXAT) && (command_type == COMMAND_SET))
        {
            *flags |= OBJ_KEEPTTL;
        // PERSIST：移除 TTL（仅 GET 系列命令）
        } else if (!strcasecmp(opt,"PERSIST") && (command_type == COMMAND_GET) &&
               !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
               !(*flags & OBJ_PX) && !(*flags & OBJ_PXAT) &&
               !(*flags & OBJ_KEEPTTL))
        {
            *flags |= OBJ_PERSIST;
        // EX seconds：秒级相对过期时间
        } else if ((opt[0] == 'e' || opt[0] == 'E') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EXAT) && !(*flags & OBJ_PX) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            *flags |= OBJ_EX;
            *expire = next;
            j++;
        // PX milliseconds：毫秒级相对过期时间
        } else if ((opt[0] == 'p' || opt[0] == 'P') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            *flags |= OBJ_PX;
            *unit = UNIT_MILLISECONDS;
            *expire = next;
            j++;
        // EXAT seconds-timestamp：秒级绝对过期时间戳
        } else if ((opt[0] == 'e' || opt[0] == 'E') &&
                   (opt[1] == 'x' || opt[1] == 'X') &&
                   (opt[2] == 'a' || opt[2] == 'A') &&
                   (opt[3] == 't' || opt[3] == 'T') && opt[4] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_PX) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            *flags |= OBJ_EXAT;
            *expire = next;
            j++;
        // PXAT milliseconds-timestamp：毫秒级绝对过期时间戳
        } else if ((opt[0] == 'p' || opt[0] == 'P') &&
                   (opt[1] == 'x' || opt[1] == 'X') &&
                   (opt[2] == 'a' || opt[2] == 'A') &&
                   (opt[3] == 't' || opt[3] == 'T') && opt[4] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
                   !(*flags & OBJ_PX) && next)
        {
            *flags |= OBJ_PXAT;
            *unit = UNIT_MILLISECONDS;
            *expire = next;
            j++;
        } else {
            // 未知选项：语法错误
            addReplyErrorObject(c,shared.syntaxerr);
            return C_ERR;
        }
    }
    return C_OK;
}

/* SET key value [NX] [XX] [KEEPTTL] [GET] [EX <seconds>] [PX <milliseconds>]
 *     [EXAT <seconds-timestamp>][PXAT <milliseconds-timestamp>]
 * SET 命令入口；解析扩展选项并调用 setGenericCommand。
 * 时间复杂度：O(1) */
void setCommand(client *c) {
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = OBJ_NO_FLAGS;

    if (parseExtendedStringArgumentsOrReply(c,&flags,&unit,&expire,COMMAND_SET) != C_OK) {
        return;
    }

    // 尝试对 value 进行更紧凑的编码（如整型编码）
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,flags,c->argv[1],c->argv[2],expire,unit,NULL,NULL);
}

/* SETNX key value — 仅当 key 不存在时设置。
 * 成功返回 1，已存在返回 0。
 * 时间复杂度：O(1) */
void setnxCommand(client *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,OBJ_SET_NX,c->argv[1],c->argv[2],NULL,0,shared.cone,shared.czero);
}

/* SETEX key seconds value — 设置字符串并指定秒级过期时间。
 * 时间复杂度：O(1) */
void setexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_EX,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS,NULL,NULL);
}

/* PSETEX key milliseconds value — 设置字符串并指定毫秒级过期时间。
 * 时间复杂度：O(1) */
void psetexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_PX,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS,NULL,NULL);
}

/* GET 命令的通用实现。
 * 返回字符串值；键不存在返回 nil；类型错误返回 C_ERR。
 * 时间复杂度：O(1) */
int getGenericCommand(client *c) {
    robj *o;

    // 读取键，键不存在直接回复 nil
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return C_OK;

    // 类型校验：必须为字符串类型
    if (checkType(c,o,OBJ_STRING)) {
        return C_ERR;
    }

    // 返回 bulk 字符串
    addReplyBulk(c,o);
    return C_OK;
}

/* GET 命令入口，委托给 getGenericCommand。
 * 时间复杂度：O(1) */
void getCommand(client *c) {
    getGenericCommand(c);
}

/*
 * GETEX <key> [PERSIST][EX seconds][PX milliseconds][EXAT seconds-timestamp][PXAT milliseconds-timestamp]
 *
 * getexCommand() 函数实现 GET 命令的扩展选项与变体。
 * 与 GET 不同，该命令并非只读。
 *
 * 未指定任何选项时，默认行为与 GET 相同，不修改 TTL。
 *
 * 同一时间只能使用以下选项之一：
 *
 * 1. PERSIST 移除与该 key 关联的 TTL。
 * 2. EX      设置秒级过期 TTL。
 * 3. PX      设置毫秒级过期 TTL。
 * 4. EXAT    类似 EX，但接受绝对 Unix 时间戳（秒）作为过期时间。
 * 5. PXAT    类似 PX，但接受绝对 Unix 时间戳（毫秒）作为过期时间。
 *
 * 命令可能返回 bulk 字符串、错误响应或 nil。
 */
/* GETEX 命令：读取字符串值并可选择修改其 TTL。
 * 时间复杂度：O(1) */
void getexCommand(client *c) {
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = OBJ_NO_FLAGS;

    if (parseExtendedStringArgumentsOrReply(c,&flags,&unit,&expire,COMMAND_GET) != C_OK) {
        return;
    }

    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return;

    if (checkType(c,o,OBJ_STRING)) {
        return;
    }

    /* 先校验过期时间值 */
    long long milliseconds = 0;
    if (expire && getExpireMillisecondsOrReply(c, expire, flags, unit, &milliseconds) != C_OK) {
        return;
    }

    /* 在过期或删除 key 之前，需要先返回值 */
    addReplyBulk(c,o);

    /* 该命令不会原样传播，而是以 PEXPIRE[AT]、DEL、UNLINK 或 PERSIST
     * 的形式传播。因此在 feedAppendOnlyFile 中无需特殊处理
     * 来将相对过期时间转换为绝对过期时间。 */
    if (((flags & OBJ_PXAT) || (flags & OBJ_EXAT)) && checkAlreadyExpired(milliseconds)) {
        /* 当指定 PXAT/EXAT 绝对时间戳时，可能时间戳已经过期，此时删除 key。 */
        int deleted = dbGenericDelete(c->db, c->argv[1], server.lazyfree_lazy_expire, DB_FLAG_KEY_EXPIRED);
        serverAssert(deleted);
        // 根据配置选择传播为 DEL 还是 UNLINK
        robj *aux = server.lazyfree_lazy_expire ? shared.unlink : shared.del;
        rewriteClientCommandVector(c,2,aux,c->argv[1]);
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        server.dirty++;
    } else if (expire) {
        // 设置新的过期时间
        setExpire(c,c->db,c->argv[1],milliseconds);
        /* 当存在 EX/PX/EXAT/PXAT 标志且 key 未过期时，
         * 以 PEXPIREAT <毫秒时间戳> 的形式传播。 */
        robj *milliseconds_obj = createStringObjectFromLongLong(milliseconds);
        rewriteClientCommandVector(c,3,shared.pexpireat,c->argv[1],milliseconds_obj);
        decrRefCount(milliseconds_obj);
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",c->argv[1],c->db->id);
        server.dirty++;
    } else if (flags & OBJ_PERSIST) {
        // 移除 TTL
        if (removeExpire(c->db, c->argv[1])) {
            signalModifiedKey(c, c->db, c->argv[1]);
            rewriteClientCommandVector(c, 2, shared.persist, c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"persist",c->argv[1],c->db->id);
            server.dirty++;
        }
    }
}

/* GETDEL key — 读取字符串值后立即删除该 key。
 * 不存在返回 nil；类型错误返回错误。
 * 时间复杂度：O(1) */
void getdelCommand(client *c) {
    if (getGenericCommand(c) == C_ERR) return;
    if (dbSyncDelete(c->db, c->argv[1])) {
        /* 以 DEL 命令的形式传播 */
        rewriteClientCommandVector(c,2,shared.del,c->argv[1]);
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        server.dirty++;
    }
}

/* GETSET key value — 设置新值并返回旧值（原子操作）。
 * 已废弃，建议使用 SET ... GET 替代。
 * 时间复杂度：O(1) */
void getsetCommand(client *c) {
    if (getGenericCommand(c) == C_ERR) return;
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setKey(c,c->db,c->argv[1],c->argv[2],0);
    notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[1],c->db->id);
    server.dirty++;

    /* 以 SET 命令的形式传播 */
    rewriteClientCommandArgument(c,0,shared.set);
}

/* SETRANGE key offset value — 从 offset 开始用 value 覆盖部分字符串。
 * key 不存在时自动创建并以零字节填充。
 * 返回设置后字符串的总长度。
 * 时间复杂度：O(1)（不计拷贝时间），实际通常为 O(M)，M 为 value 长度。 */
void setrangeCommand(client *c) {
    robj *o;
    long offset;
    sds value = c->argv[3]->ptr;

    // 解析 offset 参数
    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != C_OK)
        return;

    if (offset < 0) {
        addReplyError(c,"offset is out of range");
        return;
    }

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* 当 key 不存在且 value 为空时返回 0 */
        if (sdslen(value) == 0) {
            addReply(c,shared.czero);
            return;
        }

        /* 当结果字符串超过允许大小时返回 */
        if (checkStringLength(c,offset,sdslen(value)) != C_OK)
            return;

        // 创建并填充零字节的新字符串对象
        o = createObject(OBJ_STRING,sdsnewlen(NULL, offset+sdslen(value)));
        dbAdd(c->db,c->argv[1],o);
    } else {
        size_t olen;

        /* key 存在，检查类型 */
        if (checkType(c,o,OBJ_STRING))
            return;

        /* 当 value 为空时返回已有字符串长度 */
        olen = stringObjectLen(o);
        if (sdslen(value) == 0) {
            addReplyLongLong(c,olen);
            return;
        }

        /* 当结果字符串超过允许大小时返回 */
        if (checkStringLength(c,offset,sdslen(value)) != C_OK)
            return;

        /* 当对象被共享或以特殊编码存储时，复制出一份独立副本。 */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    // 写入新内容并发出通知
    if (sdslen(value) > 0) {
        o->ptr = sdsgrowzero(o->ptr,offset+sdslen(value));
        memcpy((char*)o->ptr+offset,value,sdslen(value));
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,
            "setrange",c->argv[1],c->db->id);
        server.dirty++;
    }
    addReplyLongLong(c,sdslen(o->ptr));
}

/* GETRANGE key start end — 返回字符串的子串（start/end 支持负数索引）。
 * 越界或 start > end 返回空字符串。
 * 时间复杂度：O(N)，N 为返回子串的长度。 */
void getrangeCommand(client *c) {
    robj *o;
    long long start, end;
    char *str, llbuf[32];
    size_t strlen;

    // 解析 start 与 end 索引
    if (getLongLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK)
        return;
    if (getLongLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK)
        return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptybulk)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;

    // 整型编码需要先转换为字符串
    if (o->encoding == OBJ_ENCODING_INT) {
        str = llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        str = o->ptr;
        strlen = sdslen(str);
    }

    /* 转换负数索引 */
    if (start < 0 && end < 0 && start > end) {
        addReply(c,shared.emptybulk);
        return;
    }
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned long long)end >= strlen) end = strlen-1;

    /* 前置条件：end >= 0 且 end < strlen，
     * 因此唯一不会返回内容的条件是：start > end。 */
    if (start > end || strlen == 0) {
        addReply(c,shared.emptybulk);
    } else {
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
}

/* MGET key [key ...] — 原子地获取多个 key 的值。
 * 不存在或类型错误对应位置返回 nil。
 * 时间复杂度：O(N)，N 为 key 数量。 */
void mgetCommand(client *c) {
    int j;

    // 先回复数组长度
    addReplyArrayLen(c,c->argc-1);
    for (j = 1; j < c->argc; j++) {
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) {
            addReplyNull(c);
        } else {
            if (o->type != OBJ_STRING) {
                addReplyNull(c);
            } else {
                addReplyBulk(c,o);
            }
        }
    }
}

/* MSET/MSETNX 的通用实现。
 * nx=1 表示 MSETNX 语义：若任一 key 已存在，则全部不设置并返回 0。
 * nx=0 表示 MSET 语义：无条件设置全部 key，返回 OK。
 * 时间复杂度：O(N)，N 为 key 数量。 */
void msetGenericCommand(client *c, int nx) {
    int j;

    // 校验参数个数必须为奇数（命令名 + 偶数个 key/value）
    if ((c->argc % 2) == 0) {
        addReplyErrorArity(c);
        return;
    }

    /* 处理 NX 标志。MSETNX 的语义是：
     * 只要有一个 key 已经存在，就返回 0 且不设置任何 key。 */
    if (nx) {
        for (j = 1; j < c->argc; j += 2) {
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {
                addReply(c, shared.czero);
                return;
            }
        }
    }

    // NX 模式下，首个 key 必须不存在，后续则可能更新已存在的相同 key
    int setkey_flags = nx ? SETKEY_DOESNT_EXIST : 0;
    for (j = 1; j < c->argc; j += 2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
        setKey(c, c->db, c->argv[j], c->argv[j + 1], setkey_flags);
        notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[j],c->db->id);
        /* 在 MSETNX 中可能覆盖相同的 key，无法保证其不存在。 */
        if (nx)
            setkey_flags = SETKEY_ADD_OR_UPDATE;
    }
    server.dirty += (c->argc-1)/2;
    addReply(c, nx ? shared.cone : shared.ok);
}

/* MSET key value [key value ...] — 原子地设置多个 key 的值。
 * 时间复杂度：O(N)，N 为 key 数量。 */
void msetCommand(client *c) {
    msetGenericCommand(c,0);
}

/* MSETNX key value [key value ...] — 仅当所有 key 都不存在时设置。
 * 时间复杂度：O(N)，N 为 key 数量。 */
void msetnxCommand(client *c) {
    msetGenericCommand(c,1);
}

/* 通用整数增减命令实现。
 * incr > 0 表示加，incr < 0 表示减；不存在的 key 按 0 处理。
 * 自动检测溢出并返回错误；原地修改以避免分配。
 * 时间复杂度：O(1) */
void incrDecrCommand(client *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,o,OBJ_STRING)) return;
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != C_OK) return;

    oldvalue = value;
    // 检测加/减操作是否会导致 long long 溢出
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    value += incr;

    // 原地修改：仅在对象独占、整型编码且值仍在共享整数范围内时
    if (o && o->refcount == 1 && o->encoding == OBJ_ENCODING_INT &&
        (value < 0 || value >= OBJ_SHARED_INTEGERS) &&
        value >= LONG_MIN && value <= LONG_MAX)
    {
        new = o;
        o->ptr = (void*)((long)value);
    } else {
        // 创建新对象并替换/新增
        new = createStringObjectFromLongLongForValue(value);
        if (o) {
            dbReplaceValue(c->db,c->argv[1],new);
        } else {
            dbAdd(c->db,c->argv[1],new);
        }
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrby",c->argv[1],c->db->id);
    server.dirty++;
    addReplyLongLong(c, value);
}

/* INCR key — 将存储的整数值加 1。
 * 不存在则初始化为 0 再加 1。
 * 时间复杂度：O(1) */
void incrCommand(client *c) {
    incrDecrCommand(c,1);
}

/* DECR key — 将存储的整数值减 1。
 * 不存在则初始化为 0 再减 1。
 * 时间复杂度：O(1) */
void decrCommand(client *c) {
    incrDecrCommand(c,-1);
}

/* INCRBY key increment — 将存储的整数值加上指定的增量。
 * 溢出返回错误。
 * 时间复杂度：O(1) */
void incrbyCommand(client *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) return;
    incrDecrCommand(c,incr);
}

/* DECRBY key decrement — 将存储的整数值减去指定的减量。
 * 溢出（含 LLONG_MIN 取负）返回错误。
 * 时间复杂度：O(1) */
void decrbyCommand(client *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) return;
    /* 溢出检查：对 LLONG_MIN 取负会导致溢出 */
    if (incr == LLONG_MIN) {
        addReplyError(c, "decrement would overflow");
        return;
    }
    incrDecrCommand(c,-incr);
}

/* INCRBYFLOAT key increment — 将存储的浮点值加上指定的增量。
 * 结果不能为 NaN 或 Infinity。
 * 时间复杂度：O(1) */
void incrbyfloatCommand(client *c) {
    long double incr, value;
    robj *o, *new;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,o,OBJ_STRING)) return;
    if (getLongDoubleFromObjectOrReply(c,o,&value,NULL) != C_OK ||
        getLongDoubleFromObjectOrReply(c,c->argv[2],&incr,NULL) != C_OK)
        return;

    value += incr;
    // 禁止产生 NaN 或 Infinity
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }
    new = createStringObjectFromLongDouble(value,1);
    if (o)
        dbReplaceValue(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrbyfloat",c->argv[1],c->db->id);
    server.dirty++;
    addReplyBulk(c,new);

    /* 总是以 SET 命令加最终值的形式复制 INCRBYFLOAT，
     * 以确保浮点精度或格式差异不会在副本或 AOF 重启后产生分歧。 */
    rewriteClientCommandArgument(c,0,shared.set);
    rewriteClientCommandArgument(c,2,new);
    rewriteClientCommandArgument(c,3,shared.keepttl);
}

/* APPEND key value — 将 value 追加到已有字符串末尾。
 * key 不存在时等价于 SET。
 * 返回追加后字符串的总长度。
 * 时间复杂度：O(1)（摊销复杂度，假设追加值较小）。 */
void appendCommand(client *c) {
    size_t totlen;
    robj *o, *append;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* key 不存在，直接创建 */
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[2]);
        totlen = stringObjectLen(c->argv[2]);
    } else {
        /* key 存在，检查类型 */
        if (checkType(c,o,OBJ_STRING))
            return;

        /* append 参数始终是 sds 类型 */
        append = c->argv[2];
        if (checkStringLength(c,stringObjectLen(o),sdslen(append->ptr)) != C_OK)
            return;

        /* 追加 value */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        totlen = sdslen(o->ptr);
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"append",c->argv[1],c->db->id);
    server.dirty++;
    addReplyLongLong(c,totlen);
}

/* STRLEN key — 返回字符串值的长度。
 * 不存在返回 0，类型错误返回错误。
 * 时间复杂度：O(1) */
void strlenCommand(client *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;
    addReplyLongLong(c,stringObjectLen(o));
}

/* LCS key1 key2 [LEN] [IDX] [MINMATCHLEN <len>] [WITHMATCHLEN]
 * 计算两字符串的最长公共子序列（LCS）。
 * 选项：
 *   LEN           - 仅返回 LCS 长度
 *   IDX           - 返回匹配区间数组
 *   MINMATCHLEN   - 过滤短于该长度的匹配
 *   WITHMATCHLEN  - 在 IDX 模式下额外返回每段匹配长度
 * 时间复杂度：O(N*M)，N 与 M 分别为两字符串长度。 */
void lcsCommand(client *c) {
    uint32_t i, j;
    long long minmatchlen = 0;
    sds a = NULL, b = NULL;
    int getlen = 0, getidx = 0, withmatchlen = 0;
    robj *obja = NULL, *objb = NULL;

    // 读取两个 key 的字符串对象
    obja = lookupKeyRead(c->db,c->argv[1]);
    objb = lookupKeyRead(c->db,c->argv[2]);
    if ((obja && obja->type != OBJ_STRING) ||
        (objb && objb->type != OBJ_STRING))
    {
        addReplyError(c,
            "The specified keys must contain string values");
        /* 不要在此清理对象，需要在调用 getDecodedObject() 之后才能清理。 */
        obja = NULL;
        objb = NULL;
        goto cleanup;
    }
    // 解码为统一字符串对象（空 key 视为空串）
    obja = obja ? getDecodedObject(obja) : createStringObject("",0);
    objb = objb ? getDecodedObject(objb) : createStringObject("",0);
    a = obja->ptr;
    b = objb->ptr;

    // 解析选项参数
    for (j = 3; j < (uint32_t)c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc-1) - j;

        if (!strcasecmp(opt,"IDX")) {
            getidx = 1;
        } else if (!strcasecmp(opt,"LEN")) {
            getlen = 1;
        } else if (!strcasecmp(opt,"WITHMATCHLEN")) {
            withmatchlen = 1;
        } else if (!strcasecmp(opt,"MINMATCHLEN") && moreargs) {
            if (getLongLongFromObjectOrReply(c,c->argv[j+1],&minmatchlen,NULL)
                != C_OK) goto cleanup;
            if (minmatchlen < 0) minmatchlen = 0;
            j++;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            goto cleanup;
        }
    }

    /* 当用户传入含混不清的参数时给出错误。 */
    if (getlen && getidx) {
        addReplyError(c,
            "If you want both the length and indexes, please just use IDX.");
        goto cleanup;
    }

    /* 检测字符串长度是否过大致后续溢出。 */
    if (sdslen(a) >= UINT32_MAX-1 || sdslen(b) >= UINT32_MAX-1) {
        addReplyError(c, "String too long for LCS");
        goto cleanup;
    }

    /* 使用经典的动态规划算法构建 LCS(x,y) 子串表。 */
    uint32_t alen = sdslen(a);
    uint32_t blen = sdslen(b);

    /* 建立 uint32_t 数组 LCS[i,j] 存储 A0..i-1 与 B0..j-1 的 LCS 长度。
     * 注意这里使用一维数组，因此按下标 LCS[j+(blen+1)*i] 访问。 */
    #define LCS(A,B) lcs[(B)+((A)*(blen+1))]

    /* 尝试分配 LCS 表，溢出或内存不足时中止。 */
    unsigned long long lcssize = (unsigned long long)(alen+1)*(blen+1); /* 由前述长度限制保证不会溢出。 */
    unsigned long long lcsalloc = lcssize * sizeof(uint32_t);
    uint32_t *lcs = NULL;
    if (lcsalloc < SIZE_MAX && lcsalloc / lcssize == sizeof(uint32_t)) {
        if (lcsalloc > (size_t)server.proto_max_bulk_len) {
            addReplyError(c, "Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len");
            goto cleanup;
        }
        lcs = ztrymalloc(lcsalloc);
    }
    if (!lcs) {
        addReplyError(c, "Insufficient memory, failed allocating transient memory for LCS");
        goto cleanup;
    }

    /* 开始构建 LCS 表。 */
    for (uint32_t i = 0; i <= alen; i++) {
        for (uint32_t j = 0; j <= blen; j++) {
            if (i == 0 || j == 0) {
                /* 若其中一个子串长度为 0，则 LCS 长度为 0。 */
                LCS(i,j) = 0;
            } else if (a[i-1] == b[j-1]) {
                /* 末尾字符相同的两个序列的 LCS（以及 LCS 本身）
                 * 等于去掉末字符的 LCS 再加上该末字符。 */
                LCS(i,j) = LCS(i-1,j-1)+1;
            } else {
                /* 末尾字符不同时，取两者 LCS 中较长者：
                 * 第一字符串与去掉末字符的第二字符串的 LCS，
                 * 以及对应的反向情形。 */
                uint32_t lcs1 = LCS(i-1,j);
                uint32_t lcs2 = LCS(i,j-1);
                LCS(i,j) = lcs1 > lcs2 ? lcs1 : lcs2;
            }
        }
    }

    /* 如有需要，把实际的 LCS 字符串存入 "result"。
     * 这里按反向构造，长度已知并保存在 idx 中。 */
    uint32_t idx = LCS(alen,blen);
    sds result = NULL;        /* 最终的 LCS 字符串。 */
    void *arraylenptr = NULL; /* IDX 模式下数组长度的延迟写入指针。 */
    uint32_t arange_start = alen, /* alen 作为哨兵，表示尚未设置。 */
             arange_end = 0,
             brange_start = 0,
             brange_end = 0;

    /* 是否需要计算实际的 LCS 字符串？需要时预分配 result。 */
    int computelcs = getidx || !getlen;
    if (computelcs) result = sdsnewlen(SDS_NOINIT,idx);

    /* 如果要输出匹配区间，先开启一个延迟写入的数组。 */
    uint32_t arraylen = 0;  /* 已输出区间数量。 */
    if (getidx) {
        addReplyMapLen(c,2);
        addReplyBulkCString(c,"matches");
        arraylenptr = addReplyDeferredLen(c);
    }

    // 反向回溯构造 LCS
    i = alen, j = blen;
    while (computelcs && i > 0 && j > 0) {
        int emit_range = 0;
        if (a[i-1] == b[j-1]) {
            /* 若字符匹配，记录该字符并将索引前移以寻找新的匹配。 */
            result[idx-1] = a[i-1];

            /* 跟踪当前匹配区间。 */
            if (arange_start == alen) {
                arange_start = i-1;
                arange_end = i-1;
                brange_start = j-1;
                brange_end = j-1;
            } else {
                /* 若区间连续，可尝试向前扩展。 */
                if (arange_start == i && brange_start == j) {
                    arange_start--;
                    brange_start--;
                } else {
                    emit_range = 1;
                }
            }
            /* 当已匹配到任一字符串的首字节时，立即输出当前区间并尽快退出循环。 */
            if (arange_start == 0 || brange_start == 0) emit_range = 1;
            idx--; i--; j--;
        } else {
            /* 否则根据 LCS 较大方向回退 i 和 j，以确定下一步前进方向。 */
            uint32_t lcs1 = LCS(i-1,j);
            uint32_t lcs2 = LCS(i,j-1);
            if (lcs1 > lcs2)
                i--;
            else
                j--;
            if (arange_start != alen) emit_range = 1;
        }

        /* 按需输出当前区间。 */
        uint32_t match_len = arange_end - arange_start + 1;
        if (emit_range) {
            if (minmatchlen == 0 || match_len >= minmatchlen) {
                if (arraylenptr) {
                    // 每段：[a_start,a_end],[b_start,b_end](,match_len)
                    addReplyArrayLen(c,2+withmatchlen);
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,arange_start);
                    addReplyLongLong(c,arange_end);
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,brange_start);
                    addReplyLongLong(c,brange_end);
                    if (withmatchlen) addReplyLongLong(c,match_len);
                    arraylen++;
                }
            }
            arange_start = alen; /* 重置，下一轮重新开始记录。 */
        }
    }

    /* 触发键空间通知、脏计数递增等。 */

    /* 根据给定选项返回响应。 */
    if (arraylenptr) {
        // IDX 模式：返回 { matches: [...], len: <长度> }
        addReplyBulkCString(c,"len");
        addReplyLongLong(c,LCS(alen,blen));
        setDeferredArrayLen(c,arraylenptr,arraylen);
    } else if (getlen) {
        addReplyLongLong(c,LCS(alen,blen));
    } else {
        addReplyBulkSds(c,result);
        result = NULL;
    }

    /* 清理资源。 */
    sdsfree(result);
    zfree(lcs);

cleanup:
    if (obja) decrRefCount(obja);
    if (objb) decrRefCount(objb);
    return;
}

